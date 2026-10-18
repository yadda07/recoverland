"""rw_restore_preview_witness_unresolved - RecoverLand validation runtime.

Invariant: I-8 (un refus dit sa vraie raison), pour l'apercu de geometrie
du dialogue Restore quand un temoin ne peut pas etre relu.

Le mecanisme
------------
Les lignes du tableau Restore sont LEGERES : geometry_wkb et
new_geometry_wkb n'y sont qu'un marqueur d'un octet (b'\\x00'). L'apercu
(recover_dialog._update_geometry_preview) relit l'evenement entier pour
obtenir la geometrie affichee ET l'autre geometrie capturee, les temoins
que le Rewind utilise pour trancher entre changement d'etiquette et
reprojection (rvw_restore_preview_witnesses). Si l'autre geometrie reste
le marqueur (evenement non relu : connexion de lecture indisponible), le
passer comme temoin donnerait au verdict un octet qui n'est pas une
geometrie, et le refus dirait que "les preuves disponibles" ne tranchent
pas : faux, un temoin n'a pas ete relu. Le correctif ne passe alors aucun
temoin, pour que le refus dise que les temoins n'ont pas ete fournis, et
le journalise (witness_unresolved).

L'echec concret pour l'utilisateur
----------------------------------
Il clique une ligne d'UPDATE apres un changement de SCR de la couche
alors que le journal ne se relit plus : l'apercu ne dessine rien. Le
message doit dire pourquoi (temoins non fournis), pas que les preuves de
l'evenement sont contradictoires ou muettes.

Ce que le scenario pilote : le vrai plugin, la vraie GeoPackage de
rvw_restore_preview_witnesses (Lambert-93 declare CC46, puis Definir le
SCR en EPSG:2154), la vraie edition capturee, la methode du dialogue
elle-meme sur une ligne dont la geometrie affichee est relue mais dont
l'autre geometrie reste le marqueur, sans connexion de lecture.

Garde du correctif de l'item 1 (et non defaut preexistant) : avant le
correctif, l'apercu ne passait aucun temoin du tout, ce cas passait donc
deja ; il echouerait si le correctif passait le marqueur comme temoin.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_restore_preview_witness_unresolved"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _w():
    from scripts.validation.scenarios import rvw_restore_preview_witnesses as w
    return w


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rw_pwu_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _w()._gpkg_cc46(tmpdir)
    flog(f"rw_restore_preview_witness_unresolved setup: trace_id={ctx.trace_id}")


def run(ctx):
    from qgis.core import QgsCoordinateReferenceSystem
    import qgis.utils
    import recoverland.core.geometry_preview as gp
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from recoverland.core.search_service import _BLOB_MARKER, get_event_by_id
    w = _w()
    h = w._h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    saved_iface = qgis.utils.iface
    qgis.utils.iface = iface
    real_verdict = gp.display_verdict
    seen = []

    def _recording_verdict(geom, crs, layer, trace_id="", witnesses=(),
                           stories=None):
        seen.append([bytes(x or b"")[:2].hex() if isinstance(x, (bytes, bytearray))
                     else type(x).__name__ for x in (witnesses or ())])
        return real_verdict(geom, crs, layer, trace_id, witnesses, stories)

    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)
        d["commit"] = h.user_commit(layer, lambda lyr: h.move(lyr, "B", *w._NEW))
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 1) - rows0
        layer.setCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
        h.set_view(iface, 640000.0, 6850000.0, 670000.0, 6880000.0)
        dlg = h.open_dialog(plugin, [layer])
        events = w._dialog_events(jpath, compute_datasource_fingerprint(layer))
        upd = [i for i, e in enumerate(events) if e.operation_type == "UPDATE"]
        d["n_update"] = len(upd)
        if upd:
            light = events[upd[0]]
            conn = sqlite3.connect(jpath)
            try:
                full = get_event_by_id(conn, light.event_id)
            finally:
                conn.close()
            # The displayed geometry read back, the witness left a marker.
            row = light._replace(geometry_wkb=full.geometry_wkb,
                                 new_geometry_wkb=_BLOB_MARKER)
            d["row_markers"] = [bytes(row.geometry_wkb)[:2].hex(),
                                bytes(row.new_geometry_wkb).hex()]
            dlg._search_events = [row]
            dlg.selected_rows = [0]
            dlg._close_dialog_read_conn()
            dlg._get_dialog_read_conn = lambda: None
            n_msgs = len(iface.bar.messages)
            gp.display_verdict = _recording_verdict
            try:
                dlg._update_geometry_preview()
            finally:
                gp.display_verdict = real_verdict
            mgr = dlg._geom_preview
            d["preview"] = {
                "drawn": mgr._band is not None,
                "refusal": getattr(mgr, "last_refusal", None),
                "messages": [f"{t} | {x}" for t, x, _l, _dd
                             in iface.bar.messages[n_msgs:]],
                "witnesses_seen": seen,
            }
            mgr.clear()
        dlg.close()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        gp.display_verdict = real_verdict
        qgis.utils.iface = saved_iface
        h.shutdown(plugin)
    flog(f"rw_restore_preview_witness_unresolved run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-600:]}" if d.get("crash") else ""
    pv = d.get("preview") or {}
    seen = pv.get("witnesses_seen") or []
    refusal = pv.get("refusal") or ""
    out = [("no_crash", "crash" not in d, crash)]
    out.append((
        "setup_update_row_with_unresolved_witness",
        d.get("commit") is True and d.get("n_update") == 1
        and (d.get("row_markers") or ["", ""])[1] == "00"
        and (d.get("row_markers") or ["00", ""])[0] != "00",
        f"commit={d.get('commit')} UPDATE={d.get('n_update')} "
        f"geometries de la ligne={d.get('row_markers')}{crash}",
    ))
    out.append((
        "marker_never_handed_as_a_witness",
        len(seen) == 1 and "00" not in seen[0],
        f"temoins recus par le verdict={seen} : un marqueur d'un octet n'est "
        f"pas une geometrie ; le passer ferait dire au refus que les preuves "
        f"de l'evenement ne tranchent pas{crash}",
    ))
    out.append((
        "refusal_says_the_witnesses_were_not_provided",
        pv.get("drawn") is False and "were not provided" in refusal
        and "from the evidence available" not in refusal
        and any("were not provided" in m for m in pv.get("messages") or []),
        f"apercu={pv} : sans temoin relu, l'apercu ne dessine rien et dit "
        f"que les temoins n'ont pas ete fournis{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, r"_update_geometry_preview: witness_unresolved",
        name="witness_unresolved_logged", min_count=1))
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_restore_preview_witness_unresolved run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
