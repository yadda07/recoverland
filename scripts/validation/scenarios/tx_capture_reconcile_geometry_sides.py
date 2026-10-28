# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_reconcile_geometry_sides - RecoverLand validation runtime.

Invariant: I-CRASH-01 (apres un plantage pendant un commit, le
rapprochement dit exactement ce que la source a recu, ni plus ni moins).

Le mecanisme
------------
Quand QGIS s'arrete pendant un commit, l'etat "prepare" (outbox) garde
l'etat d'avant de chaque entite modifiee ; au redemarrage le rapprochement
(core/crash_reconcile.py) compare cet etat a la source et reconstruit les
UPDATE qui ont atterri. Il ne suivait pas le contrat v7 de la capture
directe sur la geometrie :

* une modification d'attributs seule est lue SANS geometrie (NO_GEOMETRY) :
  son etat "prepare" porte ``geometry_captured=False`` et une geometrie
  None qui veut dire "pas lue". Le rapprochement la lisait "pas de
  geometrie". Plantage AVANT l'ecriture : l'entite, qui a une geometrie,
  differait toujours de cet etat, le renommage jamais ecrit etait compte
  "reellement ecrit" et journalise (l'utilisateur lit dans l'historique une
  modification que ses donnees n'ont jamais recue). Plantage APRES : le
  renommage portait une geometrie apparue qui n'a jamais change ;
* une geometrie effacee par le commit etait ecrite None cote NEW ; en v7
  None se lit "geometrie inchangee" : la restauration refuse faute d'etat
  d'apres alors que le journal tient la geometrie d'avant ;
* un etat "prepare" qui ne dit pas s'il a lu la geometrie (sans
  ``geometry_captured``, ecrit par une version anterieure) et dont la
  geometrie d'avant est None, sur une entite qui en a une : "pas lue" et
  "ajoutee par le commit" sont possibles l'une et l'autre. Le rapprochement
  en faisait une geometrie ajoutee : une annulation l'effacerait.

Cas
---
G1  plantage apres l'etat "prepare", avant toute ecriture (processus
    enfant) : renommage de C001 (attribut seul), deplacement de C002,
    geometrie de C003 effacee, geometrie donnee a C004. Rien n'a atterri :
    attendu landed=0 not_applied=4, aucun evenement, fichier intact.
G2  meme commit, plantage apres l'ecriture : C001 sans transition de
    geometrie, C003 cote NEW = marqueur, C004 cote OLD = marqueur, C002
    ancien et nouveau point. Restaurer chacun de ces UPDATE remet
    reellement l'etat d'avant (C001 garde sa geometrie, C003 retrouve la
    sienne, C004 la perd).
G3  registre ecrit par une version sans ``geometry_captured`` (lignes
    INTERRUPTED construites a la main) : C001 (geometrie presente, attributs
    inchanges, geometrie d'avant None) est indetermine, sans evenement ;
    C004 (sans geometrie, code change) donne un UPDATE d'attribut sans
    geometrie.

Verdict attendu avant correctif : FAIL (G1 landed=1, G2 C003 None cote NEW
et C001 geometrie fantome, G3 C001 journalise).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_reconcile_geometry_sides"
INVARIANT = "I-CRASH-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(path: str) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    for i in range(1, 6):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("code", f"C00{i}")
        feat.SetField("nom", f"nom {i}")
        if i != 4:      # C004 starts without geometry
            feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(feat)
    ds = None


def _layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


def _pump(seconds: float = 0.0) -> None:
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + seconds
    while True:
        QCoreApplication.processEvents()
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def _file_state(path: str) -> dict:
    """code -> (nom, WKT or None), read from the file."""
    out = {}
    for f in _layer(path, "g_state").getFeatures():
        g = f.geometry()
        out[f["code"]] = (f["nom"], None if g is None or g.isNull() else g.asWkt())
    return out


def _rows(journal_path: str) -> list:
    from recoverland.core.constants import is_null_geometry_marker
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT event_id, operation_type, event_schema_version, geometry_wkb, "
            "new_geometry_wkb, feature_identity_json, attributes_json FROM audit_event "
            "WHERE restored_from_event_id IS NULL ORDER BY event_id").fetchall()
        try:
            ledger = [(s, d) for s, d in conn.execute(
                "SELECT state, detail FROM capture_commit ORDER BY recorded_at")]
        except sqlite3.Error:
            ledger = []
    finally:
        conn.close()
    out = []
    for eid, op, version, old, new, ident, attrs in rows:
        try:
            fid = json.loads(ident or "{}").get("fid")
        except ValueError:
            fid = None
        out.append({"eid": eid, "op": op, "version": version, "fid": fid,
                    "old_len": None if old is None else len(old),
                    "new_len": None if new is None else len(new),
                    "marker_old": is_null_geometry_marker(old, version),
                    "marker_new": is_null_geometry_marker(new, version),
                    "attrs": attrs})
    return {"events": out, "ledger": ledger}


def _restart(workdir: str, gpkg: str) -> dict:
    """What QGIS does at the next start: open, replay, connect the layer."""
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    jm = JournalManager()
    jm.open_for_project(os.path.join(workdir, "p.qgz"), workdir)
    wq = WriteQueue()
    wq.start(jm.path)
    statuses = []
    tr = EditSessionTracker(wq, jm)
    tr.set_capture_status_callback(lambda k, d: statuses.append((k, dict(d))))
    tr.activate()
    tr.connect_layer(_layer(gpkg, "g_restart"))
    _pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()
    path = jm.path
    jm.close()
    rec = [{k: d.get(k) for k in ("landed", "not_applied", "unresolved")}
           for k, d in statuses if k == "reconciled"]
    out = {"journal": path, "reconciled": rec}
    out.update(_rows(path))
    return out


_CHILD = r'''
from qgis.core import QgsVectorLayer, QgsGeometry, QgsPointXY
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "g.gpkg")
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
wq = WriteQueue(); wq.start(jm.path)
tr = EditSessionTracker(wq, jm); tr.activate()
layer = QgsVectorLayer(gpkg + "|layername=pts", "pts", "ogr")
tr.connect_layer(layer)
fids = {{f["code"]: f.id() for f in layer.getFeatures()}}
idx = layer.fields().indexFromName("code")
layer.startEditing()
layer.changeAttributeValue(fids["C001"], idx, "C001_CRASH")
layer.changeGeometry(fids["C002"], QgsGeometry.fromPointXY(QgsPointXY(22.0, 22.0)))
layer.changeGeometry(fids["C003"], QgsGeometry())
layer.changeGeometry(fids["C004"], QgsGeometry.fromPointXY(QgsPointXY(40.0, 40.0)))
print("COMMIT", layer.commitChanges(), flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _crash_case(ctx, key: str, fault: str) -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], key)
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "g.gpkg")
    _make_gpkg(gpkg)
    fids = {f["code"]: f.id() for f in _layer(gpkg, f"{key}_fix").getFeatures()}
    rc, _so, se = run_child(_CHILD.format(workdir=work), fault=fault,
                            timeout=180, workdir=work)
    res = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE, "fids": fids,
           "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else "",
           "file_after_crash": _file_state(gpkg)}
    res.update(_restart(work, gpkg))
    res["file_after_restart"] = _file_state(gpkg)
    return res


def _case_g1(ctx) -> dict:
    return _crash_case(ctx, "g1", "capture_after_prepare")


def _case_g2(ctx) -> dict:
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_updated_feature
    res = _crash_case(ctx, "g2", "capture_after_provider_commit")
    gpkg = os.path.join(ctx.data["tmpdir"], "g2", "g.gpkg")
    res["restores"] = {}
    for code in ("C001", "C002", "C003", "C004"):
        row = next((r for r in res["events"]
                    if r["op"] == "UPDATE" and r["fid"] == res["fids"].get(code)), None)
        if row is None:
            continue
        conn = sqlite3.connect(f"file:{Path(res['journal']).as_posix()}?mode=ro", uri=True)
        try:
            event = get_event_by_id(conn, row["eid"])
        finally:
            conn.close()
        out = restore_updated_feature(_layer(gpkg, f"g2_restore_{code}"), event)
        res["restores"][code] = {k: out.get(k) for k in (
            "success", "identity_level", "reason_code", "message")}
    res["file_after_restores"] = _file_state(gpkg)
    return res


def _case_g3(ctx) -> dict:
    """Ledger rows of a build that did not say whether it read the geometry."""
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.journal_writes import ensure_v7_schema, record_capture_commit
    from recoverland.core.identity import (
        compute_datasource_fingerprint, compute_feature_identity,
    )
    from recoverland.core.serialization import serialize_attributes
    work = os.path.join(ctx.data["tmpdir"], "g3")
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "g.gpkg")
    _make_gpkg(gpkg)
    open(os.path.join(work, "p.qgz"), "w").close()
    layer = _layer(gpkg, "g3_fix")
    names = [f.name() for f in layer.dataProvider().fields()]
    feats = {f["code"]: f for f in layer.getFeatures()}
    snaps = []
    for code, attrs in (("C001", None), ("C004", {"code": "C004_AVANT"})):
        feat = feats[code]
        before = serialize_attributes(feat, names)
        before.update(attrs or {})
        snaps.append({"kind": "UPDATE", "fid": feat.id(),
                      "identity": compute_feature_identity(layer, feat),
                      "attributes": before, "changed_fields": ["code"],
                      "new_attributes": None, "geometry_wkb": None,
                      "new_geometry_wkb": None})
    jm = JournalManager()
    jm.open_for_project(os.path.join(work, "p.qgz"), work)
    path = jm.path
    jm.close()
    conn = sqlite3.connect(path)
    try:
        with conn:
            ensure_v7_schema(conn)
            record_capture_commit(
                conn, "g3legacycommit", "INTERRUPTED", layer_id=layer.id(),
                layer_name="pts", datasource_fingerprint=compute_datasource_fingerprint(layer),
                session_id="g3", prepared_at="2026-09-27T10:00:00+00:00",
                finalized_at=None, n_snapshots=len(snaps), n_events=0,
                detail="scenario: legacy prepare without geometry_captured",
                snapshots_json=json.dumps(snaps))
    finally:
        conn.close()
    del layer
    res = {"fids": {c: f.id() for c, f in feats.items()}}
    res.update(_restart(work, gpkg))
    res["file_after_restart"] = _file_state(gpkg)
    return res


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_c_gsides_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("g1", _case_g1), ("g2", _case_g2), ("g3", _case_g3)):
        try:
            ctx.data[key] = fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-900:]}
        flog(f"{SCENARIO_ID} case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:2500]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _update_of(res: dict, code: str) -> dict:
    fid = (res.get("fids") or {}).get(code)
    return next((r for r in res.get("events") or []
                 if r["op"] == "UPDATE" and r["fid"] == fid), None) or {}


def _totals(res: dict) -> dict:
    rec = res.get("reconciled") or []
    return {k: sum(int(r.get(k) or 0) for r in rec)
            for k in ("landed", "not_applied", "unresolved")}


_INITIAL = {"C001": ("nom 1", "Point (1 1)"), "C002": ("nom 2", "Point (2 2)"),
            "C003": ("nom 3", "Point (3 3)"), "C004": ("nom 4", None),
            "C005": ("nom 5", "Point (5 5)")}


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    g1, g2, g3 = ctx.data.get("g1", {}), ctx.data.get("g2", {}), ctx.data.get("g3", {})

    # ===== G1: crash before the provider wrote anything =================
    out.append((
        "g1_fixture_crash_before_the_provider_wrote",
        g1.get("fault_reached") is True and g1.get("file_after_crash") == _INITIAL,
        f"rc={g1.get('rc')} fichier={g1.get('file_after_crash')} stderr="
        f"{g1.get('stderr')} crash={g1.get('crash')} {g1.get('trace', '')}",
    ))
    out.append((
        "g1_nothing_written_is_reported_not_applied",
        len(g1.get("reconciled") or []) == 1
        and _totals(g1) == {"landed": 0, "not_applied": 4, "unresolved": 0}
        and not g1.get("events") and g1.get("file_after_restart") == _INITIAL,
        f"rapprochement={g1.get('reconciled')} attendu landed=0 not_applied=4 "
        f"unresolved=0 ; evenements={g1.get('events')} ; registre={g1.get('ledger')}. "
        f"Le renommage de C001 n'a jamais atteint le fichier : son etat 'prepare' "
        f"n'a pas lu la geometrie, la lire 'pas de geometrie' le fait compter "
        f"'reellement ecrit' et l'historique montre une modification que les "
        f"donnees n'ont jamais recue",
    ))

    # ===== G2: crash after the provider wrote ==========================
    after2 = g2.get("file_after_crash") or {}
    out.append((
        "g2_fixture_crash_after_the_provider_wrote",
        g2.get("fault_reached") is True
        and after2.get("C001_CRASH") == ("nom 1", "Point (1 1)")
        and after2.get("C002") == ("nom 2", "Point (22 22)")
        and after2.get("C003") == ("nom 3", None)
        and after2.get("C004") == ("nom 4", "Point (40 40)"),
        f"rc={g2.get('rc')} fichier={after2} stderr={g2.get('stderr')} "
        f"crash={g2.get('crash')} {g2.get('trace', '')}",
    ))
    c1, c2 = _update_of(g2, "C001"), _update_of(g2, "C002")
    c3, c4 = _update_of(g2, "C003"), _update_of(g2, "C004")
    out.append((
        "g2_reconciled_updates_follow_the_v7_geometry_contract",
        _totals(g2) == {"landed": 4, "not_applied": 0, "unresolved": 0}
        and c1.get("old_len") is None and c1.get("new_len") is None
        and "C001_CRASH" in (c1.get("attrs") or "")
        and bool(c2.get("old_len")) and bool(c2.get("new_len"))
        and bool(c3.get("old_len")) and c3.get("marker_new") is True
        and c4.get("marker_old") is True and bool(c4.get("new_len")),
        f"rapprochement={g2.get('reconciled')} ; C001={c1 or None} (attendu sans "
        f"geometrie) ; C002={c2 or None} ; C003={c3 or None} (attendu NEW=marqueur) ; "
        f"C004={c4 or None} (attendu OLD=marqueur). Un cote NULL ecrit None se "
        f"lit 'geometrie inchangee', une geometrie non lue lue 'absente' fabrique "
        f"une geometrie apparue",
    ))
    rest2 = g2.get("restores") or {}
    final2 = g2.get("file_after_restores") or {}
    out.append((
        "g2_restoring_each_reconciled_update_restores_the_file",
        all((rest2.get(c) or {}).get("success") is True
            for c in ("C001", "C002", "C003", "C004"))
        and final2 == _INITIAL,
        f"restaurations={rest2} ; fichier apres={final2} attendu {_INITIAL}. Une "
        f"restauration refusee ou qui annonce un succes sans remettre la geometrie "
        f"laisse l'utilisateur sans retour arriere apres le plantage",
    ))

    # ===== G3: legacy prepare without geometry_captured =================
    l1, l4 = _update_of(g3, "C001"), _update_of(g3, "C004")
    out.append((
        "g3_legacy_unknown_geometry_read_is_unresolved_not_invented",
        _totals(g3) == {"landed": 1, "not_applied": 0, "unresolved": 1}
        and not l1 and bool(l4) and l4.get("old_len") is None
        and l4.get("new_len") is None
        and g3.get("file_after_restart") == _INITIAL,
        f"rapprochement={g3.get('reconciled')} attendu landed=1 unresolved=1 ; "
        f"UPDATE C001={l1 or None} (attendu aucun) ; UPDATE C004={l4 or None} "
        f"(attendu attribut seul) ; evenements={g3.get('events')} crash="
        f"{g3.get('crash')} {g3.get('trace', '')}. Une geometrie d'avant None "
        f"dont on ne sait pas si elle a ete lue ne prouve pas que le commit a "
        f"ajoute la geometrie : l'annuler effacerait celle de l'utilisateur",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=3))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
