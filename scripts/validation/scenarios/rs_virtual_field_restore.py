"""rs_virtual_field_restore - RecoverLand validation runtime.

Invariant: fail-loud (un echec nomme sa cause) et I-RESTORE-01 (une
restauration faisable n'echoue pas pour une raison etrangere aux donnees).
Constat resolu (revue du tour precedent): MAJOR `_provider_delete` lisait
l'image avant suppression, venue du FOURNISSEUR, avec les noms de champs
de la COUCHE.

Le mecanisme
------------
Avant de supprimer par le fournisseur, `_provider_delete` photographie
l'entite (pour compter, apres coup, les entites qui portent encore cet
etat sur un shapefile qui renumerote). La photographie est lue par
``dataProvider().getFeatures()``, qui ne connait que les colonnes
stockees, mais indexee par ``layer.fields()``, qui contient aussi les
champs virtuels (expression) et les champs de jointure. ``pre['nom_maj']``
leve KeyError avant toute suppression. Le message remonte tel quel: "'nom_maj'".

Ce que l'utilisateur voit
-------------------------
Sa couche porte un champ calcule (upper("nom")) ou une jointure vers une
table de reference, ce qui est courant. Toute annulation d'INSERT, et
toute annulation d'une restauration de DELETE, echoue avec pour seul
message "'nom_maj'". Rien n'est supprime, et rien ne dit pourquoi.

Cas
---
    V1  GeoPackage + champ virtuel: restauration de l'INSERT, supprime
    V2  shapefile + champ virtuel: idem (lecture de controle par contenu)
    V3  GeoPackage + jointure vers une table de reference: idem
    V4  restore_batch sur V1: succes, aucune erreur
    V5  GeoPackage + champ virtuel: restauration d'un DELETE (reinsertion)
    V6  GeoPackage + champ virtuel: restauration d'un UPDATE

Verdict avant correctif (code du tour precedent): FAIL, 4/8 (KeyError
'nom_maj' / 'ref_fid'). Sur le code d'origine: PASS 8/8, c'est une
regression du tour precedent. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_virtual_field_restore"
INVARIANT = "fail-loud"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS = [(1, ["A", "K1"], "POINT (1 1)"), (2, ["NEW", "K2"], "POINT (2 2)")]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def _string_type():
    try:
        from qgis.PyQt.QtCore import QMetaType
        return QMetaType.Type.QString
    except (ImportError, AttributeError):
        from qgis.PyQt.QtCore import QVariant
        return QVariant.String


def _with_virtual(layer):
    from qgis.core import QgsField
    layer.addExpressionField('upper("nom")', QgsField("nom_maj", _string_type()))
    return layer


def _with_join(layer, ref_path):
    from qgis.core import QgsProject, QgsVectorLayerJoinInfo
    ref = _fx().open_layer(ref_path, "ref")
    QgsProject.instance().addMapLayer(ref)
    join = QgsVectorLayerJoinInfo()
    join.setJoinLayer(ref)
    join.setJoinFieldName("code")
    join.setTargetFieldName("code")
    join.setUsingMemoryCache(True)
    join.setPrefix("ref_")
    layer.addJoin(join)
    return layer, ref


def _gpkg_event(eid, op, attrs_json, fid=2, old_wkt=None, new_wkt=None):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type=op,
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        entity_fingerprint=f"pk:fid={fid}",
        field_schema_json=fx.schema(("fid", "integer"), ("nom", "string"),
                                    ("code", "string")),
        attributes_json=json.dumps(attrs_json),
        geometry_wkb=fx.wkb_wkt(old_wkt) if old_wkt else None,
        new_geometry_wkb=fx.wkb_wkt(new_wkt) if new_wkt else None,
        event_schema_version=7)


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmp = tempfile.mkdtemp(prefix="rl_rs_vfr_")
    ctx.data["tmpdir"] = tmp
    paths = {}
    for key in ("v1", "v3", "v4", "v5", "v6"):
        paths[key] = fx.make_gpkg(tmp, key, ogr.wkbPoint, _fields(), _ROWS)
    paths["v2"] = fx.make_shapefile(tmp, "v2", "Point",
                                    ["nom:string(20)", "code:string(20)"],
                                    [(["A", "K1"], "Point (1 1)"),
                                     (["NEW", "K2"], "Point (2 2)")])
    paths["ref"] = fx.make_gpkg(tmp, "ref", ogr.wkbNone,
                                [("code", ogr.OFTString),
                                 ("libelle", ogr.OFTString)],
                                [(1, ["K1", "un"], None),
                                 (2, ["K2", "deux"], None)])
    ctx.data["paths"] = paths
    flog(f"rs_virtual_field_restore setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmp}", "INFO")


def _rows(key, path):
    fx = _fx()
    if path.endswith(".gpkg"):
        return fx.file_rows(path, key)
    return fx.file_rows(path)


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_inserted_feature, restore_deleted_feature,
        restore_updated_feature, restore_batch,
    )
    fx = _fx()
    d = ctx.data
    paths = d["paths"]
    tid = ctx.trace_id
    res = {}
    fields_seen = {}
    flog(f"rs_virtual_field_restore run start: trace_id={tid}", "INFO")

    def _safe(fn, *args, **kwargs):
        try:
            return dict(fn(*args, **kwargs))
        except Exception as exc:  # noqa: BLE001 - un crash est un resultat
            return {"success": None, "crash": f"{type(exc).__name__}: {exc}"}

    ins_gpkg = _gpkg_event(801, "INSERT", {"all_attributes": {
        "fid": 2, "nom": "NEW", "code": "K2"}}, old_wkt="POINT (2 2)")
    try:
        layer = _with_virtual(fx.open_layer(paths["v1"], "v1"))
        fields_seen["v1"] = [f.name() for f in layer.fields()]
        res["v1"] = _safe(restore_inserted_feature, layer, ins_gpkg)

        layer = _with_virtual(fx.open_layer(paths["v2"]))
        fid_new = next(f.id() for f in layer.getFeatures() if f["nom"] == "NEW")
        ins_shp = fx.audit_event(
            event_id=802, operation_type="INSERT",
            feature_identity_json=json.dumps({"fid": fid_new}),
            entity_fingerprint=f"fid:{fid_new}",
            field_schema_json=fx.schema(("nom", "string"), ("code", "string")),
            attributes_json=json.dumps({"all_attributes": {
                "nom": "NEW", "code": "K2"}}),
            geometry_wkb=fx.wkb_wkt("Point (2 2)"), event_schema_version=7)
        res["v2"] = _safe(restore_inserted_feature, layer, ins_shp)

        layer, _ref = _with_join(fx.open_layer(paths["v3"], "v3"),
                                 paths["ref"])
        fields_seen["v3"] = [f.name() for f in layer.fields()]
        res["v3"] = _safe(restore_inserted_feature, layer, ins_gpkg)

        layer = _with_virtual(fx.open_layer(paths["v4"], "v4"))
        try:
            report = restore_batch(layer, [ins_gpkg], trace_id=tid)
            res["v4"] = {"succeeded": list(report.succeeded),
                         "failed": {int(k): str(v)[:200]
                                    for k, v in report.failed.items()}}
        except Exception as exc:  # noqa: BLE001
            res["v4"] = {"crash": f"{type(exc).__name__}: {exc}"}

        layer = _with_virtual(fx.open_layer(paths["v5"], "v5"))
        res["v5"] = _safe(restore_deleted_feature, layer, _gpkg_event(
            803, "DELETE", {"all_attributes": {
                "fid": 7, "nom": "BORNE", "code": "K7"}},
            fid=7, old_wkt="POINT (7 7)"))

        layer = _with_virtual(fx.open_layer(paths["v6"], "v6"))
        res["v6"] = _safe(restore_updated_feature, layer, _gpkg_event(
            804, "UPDATE", {"changed_only": {
                "nom": {"old": "ANCIEN", "new": "NEW"}}}))
    finally:
        QgsProject.instance().clear()

    d["res"] = res
    d["fields_seen"] = fields_seen
    d["after"] = {key: _rows(key, path) for key, path in paths.items()
                  if key != "ref"}
    flog(f"rs_virtual_field_restore run end: trace_id={tid} res={res}",
         "INFO")
    shutil.rmtree(d.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    res = d.get("res") or {}
    after = d.get("after") or {}
    seen = d.get("fields_seen") or {}

    out.append((
        "fixture_has_virtual_and_joined_fields",
        "nom_maj" in (seen.get("v1") or [])
        and any(name.startswith("ref_") for name in seen.get("v3") or []),
        f"champs de la couche: v1={seen.get('v1')} v3={seen.get('v3')}",
    ))
    labels = {
        "v1": "GeoPackage + champ virtuel",
        "v2": "shapefile + champ virtuel",
        "v3": "GeoPackage + jointure",
    }
    for key, label in labels.items():
        result = res.get(key) or {}
        out.append((
            f"{key}_insert_undone",
            result.get("success") is True and not result.get("skipped")
            and not any("NEW" in repr(r) for r in after.get(key) or []),
            f"{label}: annulation de l'INSERT: resultat={result} fichier "
            f"apres={after.get(key)}. L'entite inseree est la et prouvee; un "
            f"champ calcule ou joint n'est pas une colonne du fichier, il ne "
            f"doit pas faire echouer la suppression (message nu \"'nom_maj'\").",
        ))
    v4 = res.get("v4") or {}
    out.append((
        "v4_batch_reports_success",
        v4.get("succeeded") == [801] and not v4.get("failed")
        and not v4.get("crash"),
        f"restore_batch avec un champ virtuel: {v4}",
    ))
    v5 = res.get("v5") or {}
    out.append((
        "v5_delete_restored",
        v5.get("success") is True and not v5.get("skipped")
        and any("BORNE" in repr(r) for r in after.get("v5") or []),
        f"restauration d'un DELETE avec un champ virtuel: resultat={v5} "
        f"fichier apres={after.get('v5')}",
    ))
    v6 = res.get("v6") or {}
    out.append((
        "v6_update_restored",
        v6.get("success") is True
        and any("ANCIEN" in repr(r) for r in after.get("v6") or []),
        f"restauration d'un UPDATE avec un champ virtuel: resultat={v6} "
        f"fichier apres={after.get('v6')}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_virtual_field_restore.*trace_id={ctx.trace_id}",
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
