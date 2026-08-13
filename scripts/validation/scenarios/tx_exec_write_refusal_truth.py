"""tx_exec_write_refusal_truth - RecoverLand validation runtime.

Invariant: BL-04 (un refus d'ecriture doit etre propre, explicite, et
ne rien laisser derriere lui).

Le mecanisme
------------
Trois facons de faire echouer une ecriture pour de vrai, sans simulacre :

1. Le provider n'a PAS la capacite. Un shapefile dont les .shp/.shx/.dbf
   sont en lecture seule sur le disque : OGR l'ouvre en lecture seule et
   `provider.capabilities()` perd AddFeatures / ChangeAttributeValues /
   DeleteFeatures. `restore_service._can_write` doit alors bloquer les
   trois operations AVANT la moindre tentative.

2. La contrainte du fichier refuse la valeur restauree. Un GPKG dont
   `code` est NOT NULL : restaurer un etat ancien ou `code` etait vide
   fait echouer `provider.changeAttributeValues`. Le provider produit un
   diagnostic exact -- "NOT NULL constraint failed: zones.code" -- que
   `restore_updated_feature` jette : il renvoie "Attribute update
   failed", sans le champ, sans la contrainte. `restore_deleted_feature`,
   lui, remonte bien `provider.errors()`. Deux chemins voisins, deux
   qualites de diagnostic.

3. La geometrie restauree n'a pas le type de la couche. Un evenement
   capture sur une couche Point est rejoue sur une couche devenue
   LineString (ou l'inverse) : GDAL/GPKG accepte l'ecriture avec un
   simple avertissement. L'evenement porte pourtant `geometry_type`, et
   personne ne le compare au type de la couche cible avant d'ecrire.

Le scenario d'echec concret
---------------------------
Un fichier pose sur un partage reseau en lecture seule, ou verrouille
par un collegue : l'utilisateur lance une restauration, doit obtenir un
refus net, et retrouver son fichier a l'octet pres. Puis une couche
durcie depuis la capture (colonne passee obligatoire) : le refus doit
lui dire QUEL champ bloque, sinon il n'a aucune action possible. Enfin
une couche dont le type geometrique a change : ecrire quand meme y
melange des types et produit un GeoPackage non conforme, en annoncant
"Reverted".

Ce que le scenario verifie apres coup : la couche est TOUJOURS relue
depuis le disque.

Verdict attendu : PASS sur les refus de capacite (le garde-fou tient),
FAIL sur la qualite du diagnostic NOT NULL et sur l'absence de controle
du type geometrique.
"""
from __future__ import annotations

import json
import os
import shutil
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_exec_write_refusal_truth"
INVARIANT = "BL-04"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_ewrt_datasource"
_PROJECT_FP = "tx_ewrt_project"
_GPKG_LAYER = "zones"

_SHP_ROWS = [("PONT", "C001"), ("GARE", "C002")]
_SCHEMA_SHP = [
    {"name": "nom", "type": "String", "length": 40, "precision": 0},
    {"name": "code", "type": "String", "length": 10, "precision": 0},
]
_SCHEMA_GPKG = _SCHEMA_SHP + [
    {"name": "pop", "type": "Integer", "length": 10, "precision": 0},
]


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _wkb_line() -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPolylineXY(
        [QgsPointXY(0.0, 0.0), QgsPointXY(5.0, 5.0)]).asWkb())


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------

def _make_readonly_shapefile(tmpdir: str) -> str:
    from osgeo import ogr

    folder = Path(tmpdir) / "ro"
    folder.mkdir(parents=True, exist_ok=True)
    path = str(folder / "ro.shp")
    datasource = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(path)
    layer = datasource.CreateLayer("ro", geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    for index, (nom, code) in enumerate(_SHP_ROWS):
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("nom", nom)
        feature.SetField("code", code)
        feature.SetGeometry(
            ogr.CreateGeometryFromWkt(f"POINT({index + 1} {index + 1})"))
        layer.CreateFeature(feature)
        feature = None
    datasource.FlushCache()
    datasource = None

    for suffix in (".shp", ".shx", ".dbf", ".prj"):
        sidecar = folder / f"ro{suffix}"
        if sidecar.exists():
            os.chmod(str(sidecar), stat.S_IREAD)
    return path


def _make_notnull_gpkg(tmpdir: str) -> str:
    from osgeo import ogr

    path = str(Path(tmpdir) / "notnull.gpkg")
    datasource = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    layer = datasource.CreateLayer(_GPKG_LAYER, geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    mandatory = ogr.FieldDefn("code", ogr.OFTString)
    mandatory.SetNullable(0)
    layer.CreateField(mandatory)
    layer.CreateField(ogr.FieldDefn("pop", ogr.OFTInteger))
    feature = ogr.Feature(layer.GetLayerDefn())
    feature.SetField("nom", "PONT")
    feature.SetField("code", "C001")
    feature.SetField("pop", 100)
    feature.SetGeometry(ogr.CreateGeometryFromWkt("POINT(1 1)"))
    layer.CreateFeature(feature)
    feature = None
    datasource.FlushCache()
    datasource = None
    return path


def _open_shp(path: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(path, "ro", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"shapefile invalide: {path}")
    return layer


def _open_gpkg(path: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername={_GPKG_LAYER}", _GPKG_LAYER, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"GPKG invalide: {path}")
    return layer


def _rows(layer) -> list:
    rows = []
    for feature in layer.getFeatures():
        geom = feature.geometry()
        rows.append({
            "nom": feature["nom"],
            "code": feature["code"],
            "wkt": geom.asWkt(3) if geom is not None and not geom.isNull() else None,
        })
    rows.sort(key=lambda r: str(r["nom"]))
    return rows


def _snapshot_shp(path: str) -> list:
    layer = _open_shp(path)
    rows = _rows(layer)
    del layer
    return rows


def _snapshot_gpkg(path: str) -> list:
    layer = _open_gpkg(path)
    rows = []
    for feature in layer.getFeatures():
        geom = feature.geometry()
        rows.append({
            "nom": feature["nom"],
            "code": feature["code"],
            "pop": feature["pop"],
            "wkt": geom.asWkt(3) if geom is not None and not geom.isNull() else None,
        })
    del layer
    return rows


# ---------------------------------------------------------------------
# Evenements
# ---------------------------------------------------------------------

def _base_event(**overrides):
    from recoverland.core.audit_backend import AuditEvent

    payload = dict(
        event_id=1,
        project_fingerprint=_PROJECT_FP,
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_ewrt",
        layer_name_snapshot="ro",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": 0}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": "PONT_AVANT", "new": "PONT"}}}),
        geometry_wkb=_wkb_point(1.0, 1.0),
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps(_SCHEMA_SHP),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint="fid:0",
        event_schema_version=2,
        new_geometry_wkb=_wkb_point(1.0, 1.0),
        invalidated_at=None,
    )
    payload.update(overrides)
    return AuditEvent(**payload)


def _readonly_events(live_fid: int) -> list:
    identity = json.dumps({"fid": live_fid})
    update = _base_event(
        event_id=1, feature_identity_json=identity,
        entity_fingerprint=f"fid:{live_fid}")
    delete = _base_event(
        event_id=2,
        operation_type="DELETE",
        feature_identity_json=json.dumps({"fid": 91}),
        entity_fingerprint="fid:91",
        attributes_json=json.dumps({"all_attributes": {
            "nom": "DISPARU", "code": "C009"}}),
        geometry_wkb=_wkb_point(9.0, 9.0),
        new_geometry_wkb=None,
    )
    insert = _base_event(
        event_id=3,
        operation_type="INSERT",
        feature_identity_json=identity,
        entity_fingerprint=f"fid:{live_fid}",
        attributes_json=json.dumps({"all_attributes": {
            "nom": _SHP_ROWS[0][0], "code": _SHP_ROWS[0][1]}}),
        geometry_wkb=_wkb_point(1.0, 1.0),
        new_geometry_wkb=None,
    )
    return [update, delete, insert]


def _notnull_event(fid: int):
    """L'etat d'avant portait un `code` vide; la colonne est desormais
    obligatoire."""
    return _base_event(
        event_id=20,
        layer_name_snapshot=_GPKG_LAYER,
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        entity_fingerprint=f"fid:{fid}",
        field_schema_json=json.dumps(_SCHEMA_GPKG),
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": "PONT_AVANT", "new": "PONT"},
            "code": {"old": None, "new": "C001"},
            "pop": {"old": 55, "new": 100},
        }}),
    )


def _geomtype_event(fid: int):
    """L'evenement decrit une LineString; la couche cible est Point."""
    return _base_event(
        event_id=21,
        layer_name_snapshot=_GPKG_LAYER,
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        entity_fingerprint=f"fid:{fid}",
        field_schema_json=json.dumps(_SCHEMA_GPKG),
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": "PONT_AVANT", "new": "PONT"}}}),
        geometry_wkb=_wkb_line(),
        geometry_type="LineString",
        new_geometry_wkb=None,
    )


# ---------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.compat import QgisCompat

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_ewrt_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["shp"] = _make_readonly_shapefile(tmpdir)
    ctx.data["gpkg"] = _make_notnull_gpkg(tmpdir)

    layer = _open_shp(ctx.data["shp"])
    caps = layer.dataProvider().capabilities()
    ctx.data["ro_caps"] = {
        "add": bool(caps & QgisCompat.CAP_ADD_FEATURES),
        "change_attr": bool(caps & QgisCompat.CAP_CHANGE_ATTRIBUTE_VALUES),
        "delete": bool(caps & QgisCompat.CAP_DELETE_FEATURES),
    }
    ctx.data["ro_fid"] = next(layer.getFeatures()).id()
    ctx.data["before_shp"] = _rows(layer)
    del layer

    ctx.data["before_gpkg"] = _snapshot_gpkg(ctx.data["gpkg"])

    flog(
        f"tx_exec_write_refusal_truth setup: trace_id={ctx.trace_id} "
        f"ro_caps={ctx.data['ro_caps']} before_shp={ctx.data['before_shp']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_deleted_feature, restore_inserted_feature,
        restore_updated_feature, restore_batch,
    )
    from recoverland.core.restore_executor import preflight_layer_check
    from recoverland.core.restore_planner import plan_event_restore

    flog(f"tx_exec_write_refusal_truth run: trace_id={ctx.trace_id}", "INFO")

    # --- volet 1 : provider sans capacite d'ecriture --------------------
    update, delete, insert = _readonly_events(ctx.data["ro_fid"])
    layer = _open_shp(ctx.data["shp"])
    ctx.data["ro_update"] = dict(restore_updated_feature(layer, update))
    ctx.data["ro_delete"] = dict(restore_deleted_feature(layer, delete))
    ctx.data["ro_insert"] = dict(restore_inserted_feature(layer, insert))

    plan = plan_event_restore([update, delete, insert], _DS_FP, "ro")
    ctx.data["ro_preflight"] = list(preflight_layer_check(plan, layer))

    report = restore_batch(layer, [update, delete, insert],
                           trace_id=ctx.trace_id)
    ctx.data["ro_batch"] = {
        "succeeded": list(report.succeeded),
        "failed": {int(k): str(v)[:120] for k, v in report.failed.items()},
        "traces": len(report.trace_events or ()),
        "target_absent": list(report.failed_target_absent),
        "skipped_idempotent": list(report.skipped_idempotent),
    }
    del layer
    ctx.data["after_shp"] = _snapshot_shp(ctx.data["shp"])

    # --- volet 2 : contrainte NOT NULL violee ---------------------------
    layer = _open_gpkg(ctx.data["gpkg"])
    gpkg_fid = next(layer.getFeatures()).id()
    ctx.data["notnull_result"] = dict(
        restore_updated_feature(layer, _notnull_event(gpkg_fid)))
    ctx.data["provider_errors"] = [str(e)[:160]
                                   for e in layer.dataProvider().errors()]
    del layer
    ctx.data["after_notnull"] = _snapshot_gpkg(ctx.data["gpkg"])

    # --- volet 3 : type geometrique incompatible ------------------------
    layer = _open_gpkg(ctx.data["gpkg"])
    ctx.data["layer_wkb_type"] = str(layer.wkbType())
    ctx.data["geomtype_result"] = dict(
        restore_updated_feature(layer, _geomtype_event(gpkg_fid)))
    del layer
    ctx.data["after_geomtype"] = _snapshot_gpkg(ctx.data["gpkg"])

    flog(
        f"tx_exec_write_refusal_truth done: trace_id={ctx.trace_id} "
        f"ro_update={ctx.data['ro_update']} ro_batch={ctx.data['ro_batch']} "
        f"notnull={ctx.data['notnull_result']} "
        f"geomtype={ctx.data['geomtype_result']} "
        f"after_geomtype={ctx.data['after_geomtype']}",
        "INFO",
    )

    _force_remove(ctx.data.get("tmpdir", ""))


def _force_remove(folder: str) -> None:
    """rmtree sur un dossier dont des fichiers sont en lecture seule."""
    def _unlock(func, path, _exc):
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except OSError:
            pass

    if not folder:
        return
    try:
        shutil.rmtree(folder, onerror=_unlock)
    except (OSError, TypeError):
        shutil.rmtree(folder, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    caps = ctx.data.get("ro_caps") or {}
    ro_update = ctx.data.get("ro_update") or {}
    ro_delete = ctx.data.get("ro_delete") or {}
    ro_insert = ctx.data.get("ro_insert") or {}
    batch = ctx.data.get("ro_batch") or {}
    notnull = ctx.data.get("notnull_result") or {}
    geomtype = ctx.data.get("geomtype_result") or {}
    after_geom = ctx.data.get("after_geomtype") or []

    # ===== Amorcage =====================================================
    out.append((
        "fixture_is_really_read_only",
        caps.get("add") is False and caps.get("change_attr") is False
        and caps.get("delete") is False,
        f"capacites du provider={caps}: le fichier doit etre reellement "
        f"ouvert en lecture seule, sinon le scenario ne teste rien",
    ))

    # ===== Volet 1 : refus de capacite ==================================
    for name, result in (("update", ro_update), ("delete", ro_delete),
                         ("insert", ro_insert)):
        out.append((
            f"readonly_{name}_is_refused",
            result.get("success") is False,
            f"resultat={result}: sur une couche non inscriptible, la "
            f"restauration d'un {name.upper()} doit etre refusee, pas "
            f"comptee comme reussie",
        ))
        out.append((
            f"readonly_{name}_says_why",
            "capabilit" in str(result.get("message", "")).lower(),
            f"message={result.get('message')!r}: l'utilisateur doit lire que "
            f"la couche n'accepte pas l'ecriture, sinon il croira a un "
            f"probleme de donnees et cherchera au mauvais endroit",
        ))

    out.append((
        "preflight_blocks_before_any_write",
        len(ctx.data.get("ro_preflight") or []) >= 1,
        f"preflight_layer_check={ctx.data.get('ro_preflight')}: le controle "
        f"de couche doit rendre au moins un motif bloquant avant que le "
        f"moindre octet ne soit tente",
    ))
    out.append((
        "batch_reports_every_event_failed",
        batch.get("succeeded") == [] and len(batch.get("failed") or {}) == 3,
        f"lot={batch}: aucun evenement ne peut aboutir sur une couche en "
        f"lecture seule; le rapport doit le dire pour chacun",
    ))
    out.append((
        "batch_does_not_soften_a_write_refusal",
        batch.get("target_absent") == []
        and batch.get("skipped_idempotent") == [],
        f"target_absent={batch.get('target_absent')} "
        f"skipped_idempotent={batch.get('skipped_idempotent')}: un refus "
        f"d'ecriture reclasse en \"entite absente\" ou en \"deja fait\" "
        f"ferait passer une couche verrouillee pour une couche deja a jour",
    ))
    out.append((
        "batch_journals_nothing",
        (batch.get("traces") or 0) == 0,
        f"traces={batch.get('traces')}: aucune trace de restauration ne doit "
        f"entrer au journal quand rien n'a ete ecrit, sinon le dedup du "
        f"rewind croira ces evenements deja compenses",
    ))
    out.append((
        "readonly_file_is_untouched",
        ctx.data.get("after_shp") == ctx.data.get("before_shp"),
        f"avant={ctx.data.get('before_shp')} apres={ctx.data.get('after_shp')}",
    ))

    # ===== Volet 2 : NOT NULL ===========================================
    out.append((
        "notnull_violation_is_refused",
        notnull.get("success") is False,
        f"resultat={notnull}: ecrire NULL dans une colonne devenue "
        f"obligatoire est refuse par le fichier; la restauration doit "
        f"remonter cet echec",
    ))
    out.append((
        "notnull_data_is_untouched",
        ctx.data.get("after_notnull") == ctx.data.get("before_gpkg"),
        f"avant={ctx.data.get('before_gpkg')} "
        f"apres={ctx.data.get('after_notnull')}: un lot d'attributs refuse "
        f"ne doit en ecrire aucun, meme partiellement",
    ))
    out.append((
        "notnull_message_names_the_blocking_field",
        "code" in str(notnull.get("message", "")).lower(),
        f"message={notnull.get('message')!r} alors que le provider avait "
        f"produit {ctx.data.get('provider_errors')}: restore_updated_feature "
        f"jette provider.errors() et rend \"Attribute update failed\". "
        f"L'utilisateur ne sait pas quel champ bloque, donc ne peut rien "
        f"faire. restore_deleted_feature, lui, remonte bien ce diagnostic.",
    ))

    # ===== Volet 3 : type geometrique ===================================
    stored_wkt = str((after_geom[0] if after_geom else {}).get("wkt") or "")
    out.append((
        "geometry_type_mismatch_is_caught",
        geomtype.get("success") is False
        or not stored_wkt.lower().startswith("linestring"),
        f"resultat={geomtype} geometrie ecrite={stored_wkt[:60]!r} sur une "
        f"couche {ctx.data.get('layer_wkb_type')}: l'evenement porte "
        f"geometry_type='LineString', la couche est ponctuelle, et le "
        f"plugin ecrit quand meme en annoncant une restauration reussie. "
        f"Le GeoPackage devient non conforme et la couche melange les "
        f"types sans que personne ne soit averti.",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_exec_write_refusal_truth.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Garde de source ==============================================
    src = (_PLUGIN_ROOT / "core" / "restore_service.py").read_text(
        encoding="utf-8", errors="replace")
    start = src.find("def restore_updated_feature(")
    end = src.find("def validate_restore_layer_state(", start)
    body = src[start:end] if start >= 0 and end > start else ""
    out.append((
        "update_path_surfaces_provider_errors",
        "provider.errors()" in body,
        "restore_updated_feature renvoie \"Attribute update failed\" sans "
        "jamais lire provider.errors(), alors que restore_deleted_feature "
        "le fait deux lignes plus haut dans le meme fichier",
    ))
    out.append((
        "update_path_checks_geometry_type",
        "geometry_type" in body or "wkbType" in body,
        "restore_updated_feature ecrit event.geometry_wkb sans jamais "
        "comparer event.geometry_type au type de la couche cible: rien "
        "n'empeche une geometrie d'un autre type d'entrer dans la couche",
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
