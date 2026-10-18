"""rs_undo_target_proof - RecoverLand validation runtime.

Invariant: I-3 (identite prouvee avant d'ecrire) applique a l'ANNULATION.
Constats resolus: undo-update-unproven-stale-target (CRITICAL),
undo-path-bypasses-guards (HIGH), et le volet annulation de
buffer-insert-undo-geometry-guards (HIGH).

Le mecanisme
------------
Annuler une restauration d'UPDATE, c'est remettre l'etat NEW. La fonction
`_undo_update_restore` resolvait sa cible depuis l'identite ORIGINALE de
l'evenement (FID historique ou cle) et ecrivait NEW sans aucune preuve:
ni que la cible portait encore ce que la restauration avait ecrit, ni que
c'etait la meme entite. Le rewind avait pu relocaliser l'ecriture (FID
historique occupe par une autre entite apres un repack): l'annulation
ecrivait sur l'occupant. Une modification faite apres la restauration
etait ecrasee sans evenement (le suivi est suspendu pendant une
annulation). La geometrie NEW etait ecrite brute, dans le SCR de capture,
et une geometrie illisible etait sautee en silence. `_undo_insert_restore`
ignorait le verdict du pre-controle (derive de schema), n'alignait pas le
SCR, et reinserait une entite SANS geometrie quand le WKB etait tronque.

Ce que l'utilisateur voit
-------------------------
Il ferme la boite de dialogue, la rouvre plus tard et lance un autre
rewind: l'annulation automatique ecrit les attributs et la geometrie de P
sur Q, compte "annulee", et P reste a l'ancien etat. Ou bien la valeur
qu'il a saisie apres la restauration disparait sans trace. Sur une couche
reprojetee, l'entite part a des milliers de kilometres; avec un journal
abime, elle revient invisible (sans geometrie). Chaque fois: "Undo
reverted".

Cas
---
    V1  shapefile, cible relocalisee par le rewind (FID historique = Q)
    V2  GeoPackage, valeur modifiee apres la restauration (c)
    V3  couche en EPSG:2154, evenement capture en EPSG:4326
    V4  geometrie NEW illisible: rien d'hybride
    V5  annulation d'un INSERT restaure: WKB tronque
    V6  annulation d'un INSERT restaure: derive de schema
    V7  annulation d'un INSERT restaure: SCR de capture different

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import math
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_undo_target_proof"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_WGS_OLD = (2.35, 48.85)
_WGS_NEW = (2.36, 48.86)


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _gpkg_fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def _gpkg_schema():
    return _fx().schema(("fid", "integer"), ("nom", "string"),
                        ("code", "string"))


def _update_event(eid, identity, changed, old_wkb=None, new_wkb=None,
                  crs="EPSG:4326", schema=None):
    fx = _fx()
    fp = (f"pk:fid={identity['pk_value']}" if "pk_value" in identity
          else f"fid:{identity['fid']}")
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE", crs_authid=crs,
        feature_identity_json=json.dumps(identity), entity_fingerprint=fp,
        attributes_json=json.dumps({"changed_only": {
            k: {"old": old, "new": new} for k, (old, new) in changed.items()}}),
        geometry_wkb=old_wkb, new_geometry_wkb=new_wkb,
        field_schema_json=schema or _gpkg_schema())


def _l93(xy):
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject,
        QgsPointXY,
    )
    xform = QgsCoordinateTransform(
        QgsCoordinateReferenceSystem("EPSG:4326"),
        QgsCoordinateReferenceSystem("EPSG:2154"), QgsProject.instance())
    pt = xform.transform(QgsPointXY(*xy))
    return (pt.x(), pt.y())


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmpdir = tempfile.mkdtemp(prefix="rl_rs_utp_")
    ctx.data["tmpdir"] = tmpdir

    # V1: after a repack, FID 1 holds the stranger Q; P (the entity the
    # rewind restored, relocated) sits at FID 2, at its OLD state.
    ctx.data["shp_v1"] = fx.make_shapefile(
        tmpdir, "v1", "Point", ["nom:string(20)", "val:integer"], [
            (["A", 1], "Point (1 1)"),
            (["Q", 5], "Point (2 2)"),
            (["P_OLD", 8], "Point (9 9)"),
        ])
    # V2, V4: one parcel, fid 1 = primary key.
    for key in ("v2", "v4"):
        ctx.data[f"gpkg_{key}"] = fx.make_gpkg(
            tmpdir, key, ogr.wkbPoint, _gpkg_fields(),
            [(1, ["b", "K1"], "POINT (1 1)"), (2, ["AUTRE", "K2"], "POINT (2 2)")])
    # V3: metric layer; the feature sits at the OLD state the restore
    # wrote, i.e. the capture point reprojected.
    from recoverland.core.geometry_utils import geometry_for_layer_crs
    ctx.data["gpkg_v3"] = fx.make_gpkg(
        tmpdir, "v3", ogr.wkbPoint, _gpkg_fields(),
        [(1, ["RESEAU_OLD", "K1"], "POINT (0 0)")], epsg=2154)
    layer = fx.open_layer(ctx.data["gpkg_v3"], "v3")
    from qgis.core import QgsGeometry, QgsPointXY
    old_l93, _status = geometry_for_layer_crs(
        QgsGeometry.fromPointXY(QgsPointXY(*_WGS_OLD)), "EPSG:4326", layer)
    layer.dataProvider().changeGeometryValues({1: old_l93})
    del layer
    ctx.data["expected_new_l93"] = _l93(_WGS_NEW)
    # V5, V6, V7: the INSERT restore deleted the feature; the layer has
    # lost the column "ancien" for V6, and is metric for V7.
    for key in ("v5", "v6"):
        ctx.data[f"gpkg_{key}"] = fx.make_gpkg(
            tmpdir, key, ogr.wkbPoint, _gpkg_fields(),
            [(1, ["RESTE", "K1"], "POINT (1 1)")])
    ctx.data["gpkg_v7"] = fx.make_gpkg(
        tmpdir, "v7", ogr.wkbPoint, _gpkg_fields(),
        [(1, ["RESTE", "K1"], "POINT (652500 6862500)")], epsg=2154)

    ctx.data["before"] = {
        "v1": fx.file_rows(ctx.data["shp_v1"]),
        **{key: fx.file_rows(ctx.data[f"gpkg_{key}"], key)
           for key in ("v2", "v3", "v4", "v5", "v6", "v7")},
    }
    flog(f"rs_undo_target_proof setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _undo(layer, event) -> dict:
    from recoverland.core.restore_service import undo_prepare, undo_apply_one
    return dict(undo_apply_one(layer, event, undo_prepare(layer, [event])))


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import restore_updated_feature
    fx = _fx()
    tid = ctx.trace_id
    res = {}
    flog(f"rs_undo_target_proof run start: trace_id={tid}", "INFO")

    # V1 --------------------------------------------------------------
    layer = fx.open_layer(ctx.data["shp_v1"])
    q_fid = next(f.id() for f in layer.getFeatures() if f["nom"] == "Q")
    ev_v1 = _update_event(
        401, {"fid": q_fid}, {"nom": ("P_OLD", "P_NEW")},
        old_wkb=fx.wkb_wkt("Point (9 9)"), new_wkb=fx.wkb_wkt("Point (7 7)"),
        schema=fx.schema(("nom", "string"), ("val", "integer")))
    res["v1"] = _undo(layer, ev_v1)

    # V2: restore u1 (b -> a), the user then types c, then the undo -----
    ident = {"fid": 1, "pk_field": "fid", "pk_value": 1}
    ev_v2 = _update_event(402, ident, {"nom": ("a", "b")})
    layer = fx.open_layer(ctx.data["gpkg_v2"], "v2")
    res["v2_restore"] = dict(restore_updated_feature(layer, ev_v2))
    layer.dataProvider().changeAttributeValues(
        {1: {layer.fields().indexOf("nom"): "c"}})
    del layer
    ctx.data["v2_after_edit"] = fx.file_rows(ctx.data["gpkg_v2"], "v2")
    res["v2"] = _undo(fx.open_layer(ctx.data["gpkg_v2"], "v2"), ev_v2)

    # V3: the NEW geometry was captured in EPSG:4326 ------------------
    ev_v3 = _update_event(
        403, ident, {"nom": ("RESEAU_OLD", "RESEAU_NEW")},
        old_wkb=fx.wkb_point(*_WGS_OLD), new_wkb=fx.wkb_point(*_WGS_NEW))
    res["v3"] = _undo(fx.open_layer(ctx.data["gpkg_v3"], "v3"), ev_v3)

    # V4: the NEW geometry is truncated in the journal ----------------
    layer = fx.open_layer(ctx.data["gpkg_v4"], "v4")
    layer.dataProvider().changeAttributeValues(
        {1: {layer.fields().indexOf("nom"): "a"}})
    del layer
    ctx.data["before"]["v4"] = fx.file_rows(ctx.data["gpkg_v4"], "v4")
    ev_v4 = _update_event(404, ident, {"nom": ("a", "b")},
                          old_wkb=fx.wkb_wkt("POINT (1 1)"),
                          new_wkb=fx.wkb_wkt("POINT (5 5)")[:9])
    res["v4"] = _undo(fx.open_layer(ctx.data["gpkg_v4"], "v4"), ev_v4)

    # V5 / V6 / V7: undo of an INSERT restore (re-insert) --------------
    def _ins(eid, attrs, wkb, crs="EPSG:4326", schema=None):
        return fx.audit_event(
            event_id=eid, operation_type="INSERT", crs_authid=crs,
            feature_identity_json=json.dumps({"fid": 9}),
            entity_fingerprint="fid:9",
            attributes_json=json.dumps({"all_attributes": attrs}),
            geometry_wkb=wkb, field_schema_json=schema or fx.schema(
                ("nom", "string"), ("code", "string")))

    res["v5"] = _undo(fx.open_layer(ctx.data["gpkg_v5"], "v5"),
                      _ins(405, {"nom": "BORNE", "code": "K9"},
                           fx.wkb_wkt("POINT (5 5)")[:12]))
    res["v6"] = _undo(fx.open_layer(ctx.data["gpkg_v6"], "v6"),
                      _ins(406, {"nom": "BORNE", "code": "K9",
                                 "ancien": "valeur perdue"},
                           fx.wkb_wkt("POINT (5 5)"),
                           schema=fx.schema(("nom", "string"),
                                            ("code", "string"),
                                            ("ancien", "string"))))
    res["v7"] = _undo(fx.open_layer(ctx.data["gpkg_v7"], "v7"),
                      _ins(407, {"nom": "BORNE", "code": "K9"},
                           fx.wkb_point(*_WGS_NEW)))

    ctx.data["res"] = res
    ctx.data["after"] = {
        "v1": fx.file_rows(ctx.data["shp_v1"]),
        **{key: fx.file_rows(ctx.data[f"gpkg_{key}"], key)
           for key in ("v2", "v3", "v4", "v5", "v6", "v7")},
    }
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"))
               for key, value in res.items()}
    flog(f"rs_undo_target_proof run end: trace_id={tid} results={summary}",
         "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _xy(wkt):
    if not wkt:
        return None
    from qgis.core import QgsGeometry
    pt = QgsGeometry.fromWkt(wkt).asPoint()
    return (pt.x(), pt.y())


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    res = ctx.data.get("res") or {}
    before = ctx.data.get("before") or {}
    after = ctx.data.get("after") or {}

    # V1 ---------------------------------------------------------------
    v1_after = {r[0]: r for r in after.get("v1") or []}
    v1_before = {r[0]: r for r in before.get("v1") or []}
    out.append((
        "v1_historical_fid_occupant_untouched",
        v1_after.get("Q") == v1_before.get("Q") and "Q" in v1_after,
        f"Q avant={v1_before.get('Q')} apres={v1_after.get('Q')} "
        f"couche={after.get('v1')}: l'annulation a ecrit les attributs et "
        f"la geometrie de P sur Q, l'occupant du FID historique.",
    ))
    v1 = res.get("v1") or {}
    p_new = v1_after.get("P_NEW")
    out.append((
        "v1_undo_lands_on_p_or_is_refused",
        (v1.get("success") is True and p_new is not None
         and _xy(p_new[-1]) == (7.0, 7.0))
        or (v1.get("success") is False and "P_OLD" in v1_after),
        f"resultat={v1} couche={after.get('v1')}: soit l'annulation "
        f"retrouve P (qui porte l'etat ecrit par la restauration) et lui "
        f"rend NEW, soit elle refuse et le dit.",
    ))

    # V2 ---------------------------------------------------------------
    v2 = res.get("v2") or {}
    names_v2 = [r[1] for r in after.get("v2") or []]
    out.append((
        "v2_restore_happened",
        (res.get("v2_restore") or {}).get("success") is True,
        f"restauration prealable={res.get('v2_restore')}",
    ))
    out.append((
        "v2_later_edit_survives",
        "c" in names_v2 and "b" not in names_v2,
        f"apres annulation={after.get('v2')} (apres la saisie: "
        f"{ctx.data.get('v2_after_edit')}): la valeur 'c' saisie APRES la "
        f"restauration est ecrasee par 'b', sans evenement: le suivi est "
        f"suspendu pendant l'annulation.",
    ))
    out.append((
        "v2_is_a_named_refusal",
        v2.get("success") is False and bool(v2.get("reason_code")),
        f"resultat={v2}: l'annulation doit etre refusee et nommee",
    ))

    # V3 ---------------------------------------------------------------
    v3_rows = after.get("v3") or []
    v3_xy = _xy(v3_rows[0][-1]) if v3_rows else None
    exp = ctx.data.get("expected_new_l93")
    dist = (math.hypot(v3_xy[0] - exp[0], v3_xy[1] - exp[1])
            if v3_xy and exp else None)
    v3 = res.get("v3") or {}
    out.append((
        "v3_undo_never_writes_raw_degrees",
        v3_xy is not None and not (abs(v3_xy[0]) < 180 and abs(v3_xy[1]) < 90),
        f"apres={v3_xy}: les degres EPSG:4326 du journal ont ete ecrits "
        f"tels quels dans une couche en metres (l'entite part a ~6900 km).",
    ))
    out.append((
        "v3_undo_reprojected_or_refused",
        (v3.get("success") is True and dist is not None and dist < 1.0)
        or (v3.get("success") is False and v3_rows == before.get("v3")),
        f"resultat={v3} position={v3_xy} attendu={exp} ecart={dist}",
    ))

    # V4 ---------------------------------------------------------------
    v4 = res.get("v4") or {}
    out.append((
        "v4_unreadable_geometry_is_refused",
        v4.get("success") is False,
        f"resultat={v4}: la geometrie NEW est illisible; l'annulation a "
        f"ecrit les attributs, saute la geometrie et annonce 'Undo "
        f"reverted'.",
    ))
    out.append((
        "v4_feature_not_hybrid",
        after.get("v4") == before.get("v4"),
        f"apres={after.get('v4')} avant={before.get('v4')}: attributs NEW, "
        f"geometrie OLD: un etat que l'entite n'a jamais eu.",
    ))

    # V5 / V6 / V7 -----------------------------------------------------
    v5 = res.get("v5") or {}
    out.append((
        "v5_truncated_wkb_not_reinserted_without_geometry",
        v5.get("success") is False and after.get("v5") == before.get("v5"),
        f"resultat={v5} apres={after.get('v5')}: l'entite est reinseree "
        f"SANS geometrie (invisible sur la carte) et l'annulation est "
        f"annoncee reussie.",
    ))
    v6 = res.get("v6") or {}
    out.append((
        "v6_schema_drift_honoured",
        v6.get("success") is False and after.get("v6") == before.get("v6"),
        f"resultat={v6} apres={after.get('v6')}: la colonne 'ancien' a "
        f"disparu; le pre-controle le dit, l'annulation l'ignorait et "
        f"reinserait une entite amputee.",
    ))
    v7_rows = [r for r in after.get("v7") or [] if r[1] == "BORNE"]
    v7_xy = _xy(v7_rows[0][-1]) if v7_rows else None
    v7 = res.get("v7") or {}
    out.append((
        "v7_reinsert_reprojected_or_refused",
        (v7.get("success") is True and v7_xy is not None
         and math.hypot(v7_xy[0] - exp[0], v7_xy[1] - exp[1]) < 1.0)
        or (v7.get("success") is False and after.get("v7") == before.get("v7")),
        f"resultat={v7} position={v7_xy} attendu={exp}: les degres du "
        f"journal ne doivent jamais etre reinjectes bruts dans une couche "
        f"metrique.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_undo_target_proof.*trace_id={ctx.trace_id}",
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
