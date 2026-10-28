# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_strict_commit_partial_runner - RecoverLand validation runtime.

Invariant: I-STRICT-01 (un rewind strict est atomique ou dit exactement ce
qui a ete ecrit) et I-RESTORE-01 (aucun succes annonce que le fichier
contredit, aucune ecriture reelle sans trace au journal).

Le mecanisme
------------
`StrictRestoreRunner` ecrit toutes les compensations d'une couche dans un
seul tampon et appelle `commitChanges()` une fois. Sur OGR, ce commit
n'est PAS atomique : QGIS ecrit les etapes l'une apres l'autre
(geometries, attributs, suppressions, ajouts) et une etape refusee
n'annule pas les precedentes. Avant correctif, un `False` declenchait
`rollBack()` et TOUTES les actions etaient comptees en echec, alors que
les etapes deja ecrites restaient dans le fichier, sans trace au journal,
hors d'atteinte d'une annulation.

Cas
---
Couche A, contrainte UNIQUE(code). Apres T : C002 est renomme (journalise),
C001 est supprime (journalise), puis un autre outil insere une ligne de
code C001 directement dans le GeoPackage (non journalise). Rewind a T :
la remise du nom de C002 passe (etape attributs), la recreation de C001
heurte UNIQUE (etape ajouts). Le commit est partiel.

Deux variantes :
  A  la compensation des etapes ecrites reussit ;
  B  la compensation est refusee (un declencheur SQLite, pose apres
     l'edition de l'utilisateur, interdit de remettre la valeur d'avant le
     run) : la remise du nom reste dans le fichier. C'est le chemin normal
     des fournisseurs a FID positionnels (Shapefile, GeoJSON), ou rien
     n'est defait.

Ce qui est exige dans les deux, quelle que soit la strategie :
  P1  le rewind n'annonce aucun succes que le fichier contredit ;
  P2  toute compensation que le fichier montre appliquee a sa trace au
      journal, et aucune trace n'existe pour une compensation absente ;
  P3  le rapport compte au moins un echec (la recreation refusee) ;
  P4  la capture n'est plus suspendue apres le run ; la ligne externe est
      intacte ; aucune ligne C001 en double.

Verdict attendu avant correctif : FAIL (P1/P2 : C002 remis a T sur le
disque, rapport "tout en echec", aucune trace). Apres le premier
correctif, B echouait encore sur P1 : les ecritures restees dans le
fichier etaient tracees mais comptees en echec.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_strict_commit_partial_runner"
INVARIANT = "I-STRICT-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _state(path):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    out = []
    for f in lyr.getFeatures():
        out.append((f["code"], f["nom"]))
    return sorted(out, key=lambda t: (str(t[0]), str(t[1])))


def _traces(journal):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT restored_from_event_id, operation_type, attributes_json "
            "FROM audit_event WHERE restored_from_event_id IS NOT NULL").fetchall()
    finally:
        conn.close()
    return [{"from": r[0], "op": r[1], "attrs": (r[2] or "")[:200]} for r in rows]


def _events(journal):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT event_id, operation_type, attributes_json FROM audit_event "
            "WHERE restored_from_event_id IS NULL ORDER BY event_id").fetchall()
    finally:
        conn.close()
    return [{"eid": r[0], "op": r[1], "attrs": (r[2] or "")[:200]} for r in rows]


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_partial_")
    flog(f"rw_strict_commit_partial_runner setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    for variant, block in (("A", False), ("B", True)):
        sub = os.path.join(d["tmpdir"], variant)
        os.makedirs(sub, exist_ok=True)
        d[variant] = {}
        _one(ctx, sub, d[variant], block)
    flog(f"rw_strict_commit_partial_runner run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k in ('A', 'B')}, default=str)[:4000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def _one(ctx, tmp, d, block_compensation):
    from qgis.core import QgsProject
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from scripts.validation.plugin_harness import pump
    from scripts.validation.scenarios.rw_strict_runner_end_to_end import (
        _gpkg, _run_rewind, _fid_of, _commit,
    )

    path, lyr = _gpkg(tmp, "a", [("C001", "UN", 1.0, 1.0), ("C002", "DEUX", 2.0, 2.0),
                                 ("C003", "TROIS", 3.0, 3.0)])
    # UNIQUE(code), created before QGIS opens the file for editing.
    lyr = None
    conn = sqlite3.connect(path)
    conn.execute("CREATE UNIQUE INDEX ux_pts_code ON pts(code)")
    conn.commit()
    conn.close()
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "a", "ogr")
    QgsProject.instance().addMapLayer(lyr)
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()
    tracker.connect_layer(lyr)
    fp = compute_datasource_fingerprint(lyr)
    d["journal"] = jm.path
    try:
        d["t0"] = _state(path)
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        c2 = _fid_of(lyr, "C002")
        c1 = _fid_of(lyr, "C001")
        idx_nom = lyr.fields().indexFromName("nom")
        d["commit_rename"] = _commit(lyr, lambda: lyr.changeAttributeValue(c2, idx_nom, "DEUX_APRES"))
        d["commit_delete"] = _commit(lyr, lambda: lyr.deleteFeature(c1))
        pump(0.3)
        # Another tool writes C001 into the file: not journaled. A second,
        # untracked layer writes through its provider (the GeoPackage R-tree
        # triggers need the spatial SQL functions plain sqlite3 lacks).
        from qgis.core import QgsFeature
        other = QgsVectorLayer(f"{path}|layername=pts", "outil_externe", "ogr")
        ext = QgsFeature(other.fields())
        ext.setAttribute("code", "C001")
        ext.setAttribute("nom", "EXTERNE")
        d["external_insert"] = bool(other.dataProvider().addFeatures([ext])[0])
        del other
        if block_compensation:
            # Refuse putting the pre-run value back: the compensation of
            # the rewind's own write fails, the write stays in the file.
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TRIGGER rl_block_undo BEFORE UPDATE OF nom ON pts "
                "WHEN OLD.nom = 'DEUX' AND NEW.nom = 'DEUX_APRES' "
                "BEGIN SELECT RAISE(ABORT, 'compensation bloquee'); END")
            conn.commit()
            conn.close()
        lyr.dataProvider().reloadData()
        lyr.reload()
        d["before_rewind"] = _state(path)
        d["events"] = _events(jm.path)
        d["rewind"] = _run_rewind(ctx, jm, wq, tracker, {fp: lyr}, fp,
                                  RestoreCutoff(CutoffType.BY_DATE, t0))
        d["after_rewind"] = _state(path)
        d["traces"] = _traces(jm.path)
        d["suppressed_after"] = bool(getattr(tracker, "is_suppressed", False)) or bool(
            getattr(tracker, "_suppressed_layers", None))
        d["layer_editable_after"] = bool(lyr.isEditable())
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        try:
            if lyr is not None and lyr.isEditable():
                lyr.rollBack()
        except Exception:  # noqa: BLE001
            pass
        tracker.deactivate()
        tracker.disconnect_all()
        wq.stop()
        jm.close()
        QgsProject.instance().clear()


def _variant_assertions(d, variant):
    out = []
    before = d.get("before_rewind") or []
    after = d.get("after_rewind") or []
    rw = d.get("rewind") or {}
    traces = d.get("traces") or []
    events = d.get("events") or []
    rename_eid = next((e["eid"] for e in events if e["op"] == "UPDATE"), None)
    delete_eid = next((e["eid"] for e in events if e["op"] == "DELETE"), None)
    out.append((
        f"{variant}_fixture_partial_commit_is_set_up",
        d.get("commit_rename") is True and d.get("commit_delete") is True
        and ("C001", "EXTERNE") in before and ("C002", "DEUX_APRES") in before
        and rename_eid is not None and delete_eid is not None,
        f"avant rewind={before} evenements={events} crash={d.get('crash')} "
        f"{d.get('trace', '')[-700:]}",
    ))
    rename_on_disk = ("C002", "DEUX") in after
    rename_traced = any(t["from"] == rename_eid for t in traces)
    recreate_on_disk = sum(1 for c, _n in after if c == "C001") > 1 or ("C001", "UN") in after
    recreate_traced = any(t["from"] == delete_eid for t in traces)
    out.append((
        f"{variant}_p1_no_success_the_file_contradicts",
        rw.get("finished") is True
        and (rw.get("total_ok") or 0) == int(rename_on_disk) + int(recreate_on_disk),
        f"succes annonces={rw.get('total_ok')} ; sur le disque : renommage "
        f"compense={rename_on_disk}, recreation={recreate_on_disk} ; apres="
        f"{after} rapport={rw}",
    ))
    out.append((
        f"{variant}_p2_every_write_on_disk_is_traced_and_only_those",
        rename_on_disk == rename_traced and recreate_on_disk == recreate_traced,
        f"renommage : disque={rename_on_disk} trace={rename_traced} ; recreation : "
        f"disque={recreate_on_disk} trace={recreate_traced} ; traces={traces}. Une "
        f"compensation ecrite sans trace ne peut plus etre annulee ni vue par le "
        f"rewind suivant ; une trace sans ecriture ment sur l'etat du fichier",
    ))
    out.append((
        f"{variant}_p3_the_refused_recreation_is_reported",
        (rw.get("total_fail") or 0) >= 1 and bool(rw.get("errors")),
        f"echecs={rw.get('total_fail')} erreurs={rw.get('errors')} : la recreation "
        f"refusee par UNIQUE(code) doit etre dite",
    ))
    out.append((
        f"{variant}_p4_capture_released_external_row_intact",
        d.get("suppressed_after") is False and d.get("layer_editable_after") is False
        and ("C001", "EXTERNE") in after
        and sum(1 for c, _n in after if c == "C001") == 1,
        f"suspendue apres={d.get('suppressed_after')} couche en edition="
        f"{d.get('layer_editable_after')} apres={after}",
    ))
    return out


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    for variant in ("A", "B"):
        out.extend(_variant_assertions(ctx.data.get(variant) or {}, variant))
    out.append(assert_log_contains(
        ctx.records, rf"rw_strict_commit_partial_runner.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
