"""rw_buffer_insert_geom_guards - RecoverLand validation runtime.

Invariant: I-8 (ne jamais ecrire une geometrie qu'on n'a pas pu decoder ni
aligner; pas de faux succes).
Constat resolu: buffer-insert-undo-geometry-guards (HIGH), volet rewind
(le volet annulation est prouve par rs_undo_target_proof V4-V7).

Le mecanisme
------------
Le rewind annule un DELETE en recreant l'entite dans le tampon d'edition
(`restore_executor._buffer_insert`). `rebuild_geometry` rend None pour un
WKB tronque ou corrompu, et le code sautait alors simplement
``setGeometry``: l'entite etait recreee SANS geometrie et comptee
"Inserted via buffer". Le chemin evenement par evenement refuse ce meme
evenement (corrupt_wkb). Aucun controle non plus du type de geometrie:
une LineString partait dans une couche de points.

Ce que l'utilisateur voit
-------------------------
Une ligne du journal abimee (ecriture interrompue, copie partielle): la
restauration d'evenement la refuse, mais un rewind recree l'entite sans
geometrie -- invisible sur la carte -- et la compte "restauree". Un
evenement d'un autre type de geometrie rend la couche heterogene ou fait
echouer le commit avec un message OGR brut, apres une action annoncee
reussie.

Cas
---
    G1  WKB tronque: refus corrupt_wkb, fichier intact
    G2  LineString dans une couche de points: refus
        geometry_type_mismatch, fichier intact
    G3  temoin: WKB sain, l'entite revient avec sa geometrie

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_buffer_insert_geom_guards"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _delete_event(eid, nom, wkb, geometry_type="Point"):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="DELETE",
        feature_identity_json=json.dumps({"fid": 40 + eid}),
        entity_fingerprint=f"fid:{40 + eid}",
        attributes_json=json.dumps({"all_attributes": {
            "nom": nom, "code": f"K{eid}"}}),
        geometry_wkb=wkb, geometry_type=geometry_type,
        field_schema_json=fx.schema(("nom", "string"), ("code", "string")))


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmpdir = tempfile.mkdtemp(prefix="rl_rw_bigg_")
    ctx.data["tmpdir"] = tmpdir
    fields = [("nom", ogr.OFTString), ("code", ogr.OFTString)]
    for key in ("g1", "g2", "g3"):
        ctx.data[key] = fx.make_gpkg(tmpdir, key, ogr.wkbPoint, fields,
                                     [(1, ["RESTE", "K0"], "POINT (1 1)")])
    ctx.data["before"] = fx.file_rows(ctx.data["g1"], "g1")
    flog(f"rw_buffer_insert_geom_guards setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tid = ctx.trace_id
    res = {}
    flog(f"rw_buffer_insert_geom_guards run start: trace_id={tid}", "INFO")

    truncated = fx.wkb_wkt("POINT (5 5)")[:12]
    res["g1"] = fx.buffer_apply(fx.open_layer(ctx.data["g1"], "g1"), "INSERT",
                                _delete_event(1, "TRONQUE", truncated), tid)
    line = fx.wkb_wkt("LINESTRING (0 0, 5 5)")
    res["g2"] = fx.buffer_apply(fx.open_layer(ctx.data["g2"], "g2"), "INSERT",
                                _delete_event(2, "LIGNE", line, "LineString"),
                                tid)
    res["g3"] = fx.buffer_apply(fx.open_layer(ctx.data["g3"], "g3"), "INSERT",
                                _delete_event(3, "SAIN",
                                              fx.wkb_wkt("POINT (5 5)")), tid)
    ctx.data["res"] = res
    ctx.data["after"] = {key: fx.file_rows(ctx.data[key], key)
                         for key in ("g1", "g2", "g3")}
    summary = {key: (value.get("success"), value.get("reason_code"))
               for key, value in res.items()}
    flog(f"rw_buffer_insert_geom_guards run end: trace_id={tid} "
         f"results={summary}", "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    res = ctx.data.get("res") or {}
    after = ctx.data.get("after") or {}
    before = ctx.data.get("before") or []

    g1 = res.get("g1") or {}
    out.append((
        "g1_truncated_wkb_refused",
        g1.get("success") is False and g1.get("reason_code") == "corrupt_wkb",
        f"resultat={g1}: le WKB du journal est tronque; le rewind recree "
        f"l'entite SANS geometrie et annonce 'Inserted via buffer'",
    ))
    out.append((
        "g1_no_geometry_less_feature",
        after.get("g1") == before,
        f"apres={after.get('g1')} avant={before}: une entite invisible "
        f"(geometrie NULL) a ete ajoutee a la couche",
    ))
    g2 = res.get("g2") or {}
    out.append((
        "g2_type_mismatch_refused",
        g2.get("success") is False
        and g2.get("reason_code") == "geometry_type_mismatch",
        f"resultat={g2}: une LineString ne doit pas entrer dans une couche "
        f"de points; le chemin evenement refuse deja ce cas",
    ))
    out.append((
        "g2_layer_untouched",
        after.get("g2") == before,
        f"apres={after.get('g2')} avant={before}",
    ))
    g3 = res.get("g3") or {}
    out.append((
        "control_sound_wkb_reinserted",
        g3.get("success") is True
        and any(r[1] == "SAIN" and r[-1] == "Point (5 5)"
                for r in after.get("g3") or []),
        f"temoin: resultat={g3} apres={after.get('g3')}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_buffer_insert_geom_guards.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
