"""tx_geom_null_transitions - RecoverLand validation runtime.

Invariant: I-8 (le rewind rend la geometrie capturee, y compris son
absence) et I-RESTORE-01 (APPLIED seulement si le fichier le confirme).

Le mecanisme
------------
Dans le journal, `geometry_wkb IS NULL` voulait dire a la fois "la
geometrie n'a pas change" et "il n'y avait pas de geometrie". Quand une
geometrie est AJOUTEE a une entite qui n'en avait pas (import de lignes
sans geometrie puis numerisation, cas courant de geocodage), l'UPDATE
portait geometry_wkb=NULL cote avant : les moteurs de restauration ne
touchaient pas la geometrie, et annoncaient pourtant la compensation
APPLIQUEE. Dans l'autre sens (geometrie supprimee), new_geometry_wkb=NULL
rendait l'etat d'apres invisible a la preuve de cible et a Review.

Cas
---
G1  une entite sans geometrie en recoit une apres T ; rewind a T : le
    fichier doit rendre une geometrie NULLE (pas le point numerise).
G2  une entite perd sa geometrie apres T ; rewind a T : le fichier doit
    rendre le point d'origine.
G3  une entite a geometrie VIDE (POINT EMPTY, type conserve, zero
    coordonnee) en recoit une apres T ; rewind a T : elle redevient VIDE.
    Le moteur refusait toute geometrie vide ("Geometry rebuilt empty")
    alors que `rebuild_geometry` ecarte deja les blobs illisibles.

Verdict attendu avant correctif : FAIL (G1 : point laisse en place et
rewind annonce applique).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_geom_null_transitions"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _gpkg(tmpdir):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(20)",
                         "m", "memory")
    mem.startEditing()
    for code, nom in (("G1", "SANS_GEOM"), ("G2", "AVEC_GEOM"), ("G3", "GEOM_VIDE")):
        f = QgsFeature(mem.fields())
        f.setAttributes([code, nom])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.0, 2.0)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, "g.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    # The writer turns a feature without geometry into POINT EMPTY: set the
    # NULL and EMPTY states explicitly through the provider instead.
    lyr = QgsVectorLayer(f"{path}|layername=pts", "g", "ogr")
    assert lyr.isValid()
    by_code = {f["code"]: f.id() for f in lyr.getFeatures()}
    lyr.dataProvider().changeGeometryValues({
        by_code["G1"]: QgsGeometry(),
        by_code["G3"]: QgsGeometry.fromWkt("Point EMPTY"),
    })
    lyr.reload()
    return path, lyr


def _state(path):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    out = {}
    for f in lyr.getFeatures():
        g = f.geometry()
        out[f["code"]] = None if (g is None or g.isNull()) else g.asWkt(3)
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_tx_geomnull_")
    flog(f"tx_geom_null_transitions setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsGeometry, QgsPointXY
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump
    from scripts.validation.scenarios.rw_strict_runner_end_to_end import _run_rewind, _fid_of

    d = ctx.data
    tmp = d["tmpdir"]
    path, lyr = _gpkg(tmp)
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
    try:
        d["t0"] = _state(path)
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        g1 = _fid_of(lyr, "G1")
        g2 = _fid_of(lyr, "G2")
        g3 = _fid_of(lyr, "G3")
        lyr.startEditing()
        lyr.changeGeometry(g1, QgsGeometry.fromPointXY(QgsPointXY(7.0, 7.0)))
        lyr.changeGeometry(g2, QgsGeometry())
        lyr.changeGeometry(g3, QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)))
        d["commit"] = bool(lyr.commitChanges())
        pump(0.3)
        d["after_edit"] = _state(path)
        d["rewind"] = _run_rewind(ctx, jm, wq, tracker, {fp: lyr}, fp,
                                  RestoreCutoff(CutoffType.BY_DATE, t0))
        d["after_rewind"] = _state(path)
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    finally:
        tracker.deactivate()
        tracker.disconnect_all()
        wq.stop()
        jm.close()
        QgsProject.instance().clear()
    flog(f"tx_geom_null_transitions run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:2500]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    t0 = d.get("t0") or {}
    edited = d.get("after_edit") or {}
    back = d.get("after_rewind") or {}
    rw = d.get("rewind") or {}
    out.append((
        "fixture_transitions_happened",
        d.get("commit") is True and t0.get("G1") is None and edited.get("G1") is not None
        and t0.get("G2") is not None and edited.get("G2") is None
        and t0.get("G3") == "Point EMPTY" and edited.get("G3") not in (None, "Point EMPTY"),
        f"t0={t0} apres edition={edited} crash={d.get('crash')} {d.get('trace', '')[-600:]}",
    ))
    out.append((
        "g1_added_geometry_is_removed_by_the_rewind",
        back.get("G1") is None,
        f"G1 avant={t0.get('G1')} apres rewind={back.get('G1')} rapport={rw} : la "
        f"geometrie numerisee apres T doit disparaitre ; annoncer la "
        f"compensation appliquee en la laissant est un faux succes",
    ))
    out.append((
        "g2_removed_geometry_comes_back",
        back.get("G2") == t0.get("G2") and t0.get("G2") is not None,
        f"G2 avant={t0.get('G2')} apres rewind={back.get('G2')} rapport={rw}",
    ))
    out.append((
        "g3_empty_geometry_comes_back_empty",
        back.get("G3") == "Point EMPTY",
        f"G3 avant={t0.get('G3')} apres rewind={back.get('G3')} rapport={rw} : une "
        f"geometrie vide capturee est un etat, pas une corruption",
    ))
    out.append((
        "rewind_reports_no_false_success",
        rw.get("finished") is True and rw.get("total_fail") == 0 and back == t0,
        f"rapport={rw} etat final={back} attendu={t0}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_geom_null_transitions.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
