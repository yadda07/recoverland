# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_journal_long_path - RecoverLand validation runtime.

Invariant: I-JRN-01 (un projet a un journal, le meme a chaque ouverture)
et I-CAPTURE-01 (une edition acceptee finit au journal).

Le mecanisme
------------
Windows limite un chemin a 260 caracteres (MAX_PATH) pour les appels
Win32 classiques, sauf sous la forme etendue `\\\\?\\C:\\...`. Le journal vit
a cote du projet (`<dossier du projet>/.recoverland/...sqlite`) ou dans le
profil QGIS pour un projet non enregistre, et ses fichiers compagnons
ajoutent des suffixes (`-wal`, `-shm`, `.rlwriter`, `.outbox/seg-*.jsonl`).
Mesure sur ce poste : un profil dont le chemin fait 269 caracteres jusqu'au
verrou -> "unable to open database file", backend non initialise, capture
coupee pour toute la session. Et pour un projet ENREGISTRE dans un dossier
profond, `os.path.isfile(projet)` repond False : le projet est pris pour un
projet non enregistre et son journal est range dans la zone des projets
vierges du PROFIL (mesure avant correctif), pas a cote du projet. Cette
zone est nettoyee par `cleanup_orphan_journals` : tout journal non modifie
depuis 30 jours y est SUPPRIME a l'ouverture d'un autre projet. Un projet
profond laisse un mois de cote perd tout son historique, et copier le
dossier du projet sur un autre poste ne l'emporte pas.

Cas
---
L1  projet enregistre sous un chemin de ~280 caracteres : deux ouvertures
    successives donnent le MEME journal, et il est a cote du projet.
L2  projet non enregistre, profil profond : le journal s'ouvre.
L3  capture reelle (tracker + WriteQueue) d'un commit : la ligne arrive au
    journal ; une connexion en lecture seule la relit.
L4  apres arret et redemarrage, l'evenement est toujours la et l'outbox est
    vide.

Verdict attendu avant correctif : FAIL (L1 a L4).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_long_path"
INVARIANT = "I-JRN-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PFX = "\\\\?\\"


def _ext(path: str) -> str:
    p = os.path.abspath(path)
    return p if p.startswith(_PFX) else _PFX + p


def _deep(base: str, total: int) -> str:
    deep = base
    while len(deep) < total:
        deep = os.path.join(deep, "dossier_de_projet_au_nom_tres_long")
    os.makedirs(_ext(deep), exist_ok=True)
    return deep


def _gpkg(tmpdir):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(20)",
                         "m", "memory")
    mem.startEditing()
    f = QgsFeature(mem.fields())
    f.setAttributes(["L001", "AVANT"])
    f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0, 1.0)))
    mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, "court.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _count(journal: str, needle: str) -> int:
    from recoverland.core.journal_manager import sqlite_readonly_uri
    conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
            (f"%{needle}%",)).fetchone()[0])
    finally:
        conn.close()


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_lp_")
    flog(f"tx_journal_long_path setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump

    d = ctx.data
    tmp = d["tmpdir"]
    try:
        # ---- L1: saved project in a deep folder ------------------------
        proj_dir = _deep(os.path.join(tmp, "projets"), 250)
        project = os.path.join(proj_dir, "mon_projet.qgz")
        with open(_ext(project), "w") as fh:
            fh.write("x")
        d["project_len"] = len(project)
        jm = JournalManager()
        try:
            p1 = jm.open_for_project(project, os.path.join(tmp, "profil"))
            jm.close()
            p2 = jm.open_for_project(project, os.path.join(tmp, "profil"))
            d["l1_same_journal"] = (p1 == p2)
            d["l1_next_to_project"] = os.path.normcase(
                os.path.abspath(p1)).replace(_PFX.lower(), "").startswith(
                os.path.normcase(os.path.abspath(proj_dir)))
            d["l1_journal"] = p1
        except Exception as exc:  # noqa: BLE001
            d["l1_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            jm.close()

        # ---- L2: unsaved project, deep profile --------------------------
        profile = _deep(os.path.join(tmp, "profil_profond"), 240)
        jm2 = JournalManager()
        try:
            d["l2_journal"] = jm2.open_for_project("", profile)
            d["l2_open"] = bool(jm2.is_open)
        except Exception as exc:  # noqa: BLE001
            d["l2_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            jm2.close()

        # ---- L3: real capture into the deep journal ---------------------
        gpkg = _gpkg(tmp)
        lyr = QgsVectorLayer(f"{gpkg}|layername=pts", "court", "ogr")
        QgsProject.instance().addMapLayer(lyr)
        jm3 = JournalManager()
        wq = WriteQueue()
        tracker = None
        try:
            journal = jm3.open_for_project(project, os.path.join(tmp, "profil"))
            d["l3_journal_len"] = len(journal)
            wq.start(journal)
            tracker = EditSessionTracker(wq, jm3)
            tracker.activate()
            tracker.connect_layer(lyr)
            fid = next(lyr.getFeatures()).id()
            lyr.startEditing()
            lyr.changeAttributeValue(fid, lyr.fields().indexFromName("nom"), "APRES_LP")
            d["l3_commit"] = bool(lyr.commitChanges())
            pump(0.3)
            if hasattr(wq, "wait_until_drained"):
                wq.wait_until_drained(5.0)
            else:
                time.sleep(1.5)
            d["l3_rows"] = _count(journal, "APRES_LP")
        except Exception as exc:  # noqa: BLE001
            import traceback
            d["l3_error"] = f"{type(exc).__name__}: {exc}"
            d["trace"] = traceback.format_exc()[-1200:]
        finally:
            if tracker is not None:
                tracker.deactivate()
                tracker.disconnect_all()
            wq.stop()
            jm3.close()
            QgsProject.instance().clear()

        # ---- L4: restart --------------------------------------------------
        jm4 = JournalManager()
        wq4 = WriteQueue()
        try:
            journal = jm4.open_for_project(project, os.path.join(tmp, "profil"))
            wq4.start(journal)
            d["l4_rows"] = _count(journal, "APRES_LP")
            outbox = journal + ".outbox"
            try:
                segs = [n for n in os.listdir(_ext(outbox)) if n.startswith("seg-")]
            except OSError:
                segs = []
            d["l4_outbox_segments"] = len(segs)
        except Exception as exc:  # noqa: BLE001
            d["l4_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            wq4.stop()
            jm4.close()
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    flog(f"tx_journal_long_path run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:2500]}",
         "INFO")
    shutil.rmtree(_ext(tmp), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    out.append((
        "l1_saved_deep_project_keeps_one_journal",
        d.get("l1_same_journal") is True and d.get("l1_next_to_project") is True,
        f"chemin du projet={d.get('project_len')} car. ; meme journal aux deux "
        f"ouvertures={d.get('l1_same_journal')} ; a cote du projet="
        f"{d.get('l1_next_to_project')} ; journal={d.get('l1_journal')} ; erreur="
        f"{d.get('l1_error')} crash={d.get('crash')} : un projet enregistre pris "
        f"pour un projet vierge a son journal dans le profil, supprime apres 30 "
        f"jours sans ouverture",
    ))
    out.append((
        "l2_deep_profile_journal_opens",
        d.get("l2_open") is True,
        f"ouvert={d.get('l2_open')} erreur={d.get('l2_error')} journal="
        f"{d.get('l2_journal')}",
    ))
    out.append((
        "l3_capture_reaches_the_deep_journal",
        d.get("l3_commit") is True and d.get("l3_rows") == 1,
        f"commit={d.get('l3_commit')} lignes={d.get('l3_rows')} longueur du "
        f"journal={d.get('l3_journal_len')} erreur={d.get('l3_error')} "
        f"{d.get('trace', '')[-500:]}",
    ))
    out.append((
        "l4_event_survives_a_restart",
        d.get("l4_rows") == 1 and d.get("l4_outbox_segments") == 0,
        f"lignes apres redemarrage={d.get('l4_rows')} segments d'outbox="
        f"{d.get('l4_outbox_segments')} erreur={d.get('l4_error')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_journal_long_path.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
