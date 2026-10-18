"""rvw_restore_preview_witnesses - RecoverLand validation runtime.

Invariant: I-3 (un etat passe est montre la ou le Rewind le remet, ou pas
du tout) et I-8 (un refus dit sa vraie raison), pour l'APERCU de geometrie
du dialogue Restore.

Le mecanisme
------------
Le journal garde les coordonnees BRUTES ; le SCR de capture n'est qu'une
etiquette. Apres un changement d'etiquette (Definir le SCR de la couche),
le Rewind place l'ancienne geometrie d'un UPDATE grace aux TEMOINS de
l'evenement : l'entite porte encore exactement sa nouvelle geometrie
brute, donc les nombres n'ont pas ete convertis (relabel), et l'ancienne
geometrie est ecrite brute. L'apercu (recover_dialog._update_geometry_preview
-> GeometryPreviewManager.show) ne passe que la geometrie affichee, sans
les temoins : seule, elle ne departage rien, et l'apercu refuse. Le texte
du refus disait alors que "les preuves disponibles" ne permettent pas de
trancher, ce qui est faux : les preuves existent, l'apercu ne les a pas
recues.

L'echec concret pour l'utilisateur
----------------------------------
Sa couche Lambert-93 etait declaree par erreur en CC46 (EPSG:3946) ; il
deplace la parcelle B, enregistre, puis corrige le SCR de la couche en
EPSG:2154. Dans Restore, il clique la ligne de l'UPDATE : l'apercu ne
dessine rien et affirme que rien ne permet de placer la geometrie, alors
que le Rewind de cet evenement la remet exactement en (651000, 6861000).
Il croit l'evenement irrecuperable, ou croit l'apercu et le Rewind en
desaccord sur des preuves identiques.

Ce que le scenario pilote : le vrai plugin, un vrai GeoPackage, une vraie
edition capturee, un vrai changement de SCR de la couche, la recherche du
dialogue (search_events sans BLOB, comme LocalSearchThread) et la methode
du dialogue elle-meme (_update_geometry_preview, sur une ligne selectionnee)
sur son vrai GeometryPreviewManager et le vrai canevas.

Deux verdicts distincts :
  * le texte du refus (partie de ce correctif, core/geometry_preview.py) :
    FAIL avant, PASS apres ;
  * l'accord de l'apercu avec le Rewind : il depend de l'appelant
    (_update_geometry_preview, code du mode Restore, hors du perimetre
    Review) qui doit passer les temoins de l'evenement. Il reste FAIL tant
    que l'appelant ne les passe pas : c'est voulu, l'ecart est reel.
"""
from __future__ import annotations

import json
import math
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rvw_restore_preview_witnesses"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_OLD = (651000.0, 6861000.0)
_NEW = (652000.0, 6862000.0)


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _gpkg_cc46(tmpdir) -> str:
    """Lambert-93 numbers in a file that declares CC46: the wrong declaration."""
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsVectorLayer,
    )
    h = _h()
    uri = "Point?crs=EPSG:3946&" + "&".join(f"field={n}:{t}" for n, t in h.FIELDS)
    mem = QgsVectorLayer(uri, "seed", "memory")
    feats = []
    for code, xy in (("B", _OLD), ("C", (660000.0, 6870000.0))):
        f = QgsFeature(mem.fields())
        f.setAttributes([code, code, 1.0])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*xy)))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, "cadastre.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_prvw_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _gpkg_cc46(tmpdir)
    flog(f"rvw_restore_preview_witnesses setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _dialog_events(jpath, fingerprint):
    """The rows of the Restore table, fetched as LocalSearchThread fetches them."""
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events
    conn = sqlite3.connect(jpath)
    try:
        crit = SearchCriteria(datasource_fingerprint=fingerprint, layer_name=None,
                              operation_type=None, user_name=None,
                              start_date=None, end_date=None, page=1,
                              page_size=100)
        return list(search_events(conn, crit, exclude_blobs=True).events)
    finally:
        conn.close()


def run(ctx):
    from qgis.core import QgsCoordinateReferenceSystem
    from recoverland.core.geometry_utils import geometry_for_layer_crs, rebuild_geometry
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from recoverland.core.search_service import get_event_by_id
    import qgis.utils
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    # The preview pushes its refusal through qgis.utils.iface, as in QGIS.
    saved_iface = qgis.utils.iface
    qgis.utils.iface = iface
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        d["crs_at_capture"] = layer.crs().authid()
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)
        d["commit"] = h.user_commit(layer, lambda lyr: h.move(lyr, "B", *_NEW))
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 1) - rows0
        # The user corrects the declaration: Set Layer CRS (a relabel).
        layer.setCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
        d["crs_now"] = layer.crs().authid()
        h.set_view(iface, 640000.0, 6850000.0, 670000.0, 6880000.0)

        dlg = h.open_dialog(plugin, [layer])
        events = _dialog_events(jpath, compute_datasource_fingerprint(layer))
        upd = [i for i, e in enumerate(events) if e.operation_type == "UPDATE"]
        d["n_events"] = len(events)
        d["n_update"] = len(upd)
        if upd:
            n_msgs = len(iface.bar.messages)
            dlg._search_events = events
            dlg.selected_rows = [upd[0]]
            # The dialog's own method, as on_selection_changed calls it.
            dlg._update_geometry_preview()
            mgr = dlg._geom_preview
            box = mgr._last_bbox if mgr._band is not None else None
            d["preview"] = {
                "drawn": mgr._band is not None,
                "at": [box.center().x(), box.center().y()] if box is not None else None,
                "refusal": getattr(mgr, "last_refusal", None),
                "messages": [f"{t} | {x}" for t, x, _l, _dd in iface.bar.messages[n_msgs:]],
            }
            mgr.clear()
            # What the Rewind of this very event writes: its witnesses.
            conn = sqlite3.connect(jpath)
            try:
                full = get_event_by_id(conn, events[upd[0]].event_id)
            finally:
                conn.close()
            geom, status = geometry_for_layer_crs(
                rebuild_geometry(full.geometry_wkb), full.crs_authid, layer,
                "rvw_prvw", (full.geometry_wkb, full.new_geometry_wkb))
            d["event_crs"] = full.crs_authid
            d["rewind"] = {
                "status": status,
                "at": ([geom.asPoint().x(), geom.asPoint().y()]
                       if geom is not None and not geom.isNull() else None),
            }
        dlg.close()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        qgis.utils.iface = saved_iface
        h.shutdown(plugin)
    flog(f"rvw_restore_preview_witnesses run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _near(a, b, tol) -> bool:
    return (a is not None and b is not None
            and math.hypot(a[0] - b[0], a[1] - b[1]) <= tol)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    pv = d.get("preview") or {}
    rw = d.get("rewind") or {}
    out = []
    out.append((
        "setup_update_captured_in_cc46_then_relabelled",
        d.get("commit") is True and d.get("n_update") == 1
        and d.get("event_crs") == "EPSG:3946" and d.get("crs_now") == "EPSG:2154"
        and rw.get("status") == "relabeled" and _near(rw.get("at"), _OLD, 1e-3),
        f"commit={d.get('commit')} evenements={d.get('n_events')} "
        f"UPDATE={d.get('n_update')} SCR capture={d.get('event_crs')} "
        f"SCR actuel={d.get('crs_now')} Rewind={rw} (attendu relabeled en "
        f"{_OLD}, sur le temoin de l'evenement){crash}",
    ))
    out.append((
        "restore_preview_never_draws_where_the_rewind_would_not",
        not pv.get("drawn") or _near(pv.get("at"), _OLD, 1.0),
        f"apercu={pv} Rewind={rw} : un apercu dessine ailleurs que la ou le "
        f"Rewind remet l'entite montrerait un etat passe qui n'a pas "
        f"existe{crash}",
    ))
    refusal = pv.get("refusal") or ""
    out.append((
        "restore_preview_refusal_names_the_missing_witnesses",
        pv.get("drawn") is True
        or ("witness" in refusal and "from the evidence available" not in refusal
            and any("witness" in m for m in pv.get("messages") or [])),
        f"refus de l'apercu={refusal!r} messages={pv.get('messages')} : "
        f"l'apercu n'a recu que la geometrie affichee ; dire que les preuves "
        f"disponibles ne tranchent pas est faux (le Rewind tranche sur les "
        f"temoins de l'evenement){crash}",
    ))
    # Red until the Restore-mode caller passes the event witnesses
    # (recover_dialog._update_geometry_preview, outside the Review scope).
    out.append((
        "restore_preview_of_an_update_agrees_with_the_rewind",
        pv.get("drawn") is True and _near(pv.get("at"), _OLD, 1.0),
        f"apercu={pv} Rewind={rw} : l'apercu de l'UPDATE doit etre dessine la "
        f"ou le Rewind le remet ; OUVERT tant que "
        f"recover_dialog._update_geometry_preview (mode Restore) ne passe pas "
        f"witnesses=(geometry_wkb, new_geometry_wkb) a "
        f"GeometryPreviewManager.show{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_restore_preview_witnesses.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
