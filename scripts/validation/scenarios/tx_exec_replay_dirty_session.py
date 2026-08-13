"""tx_exec_replay_dirty_session - RecoverLand validation runtime.

Invariant: I-8 (rejouer une restauration deja appliquee ne doit rien
ajouter, et une restauration annoncee doit survivre a la couche).

Le mecanisme
------------
Deux failles voisines, toutes deux dans le chemin d'ecriture direct
(`restore_service`, sans tampon d'edition).

A. Re-restaurer un DELETE duplique l'entite.
   `restore_deleted_feature` possede bien une garde d'idempotence :

       if pk_field and pk_value is not None:
           existing_fid = _find_target_feature(layer, identity)
           if existing_fid is not None:
               return {"success": True, "message": "Already exists (skip)"}

   Elle est inatteignable en pratique. Sur shapefile l'identite est
   FID-seule (`pk_field` absent) : la garde est sautee. Sur GPKG le
   `pk_field` est `fid`, mais la re-insertion passe par
   `provider.addFeatures()` qui attribue un NOUVEAU fid : la valeur
   historique cherchee par la garde ne revient jamais. Dans les deux
   cas, chaque rejeu ajoute une entite de plus, et chaque rejeu repond
   "Restored".

   Rejouer arrive tout le temps : double-clic, relance apres un rewind
   partiel, deux utilisateurs sur le meme GPKG, reprise apres plantage.

B. Restaurer sous une session d'edition non validee.
   `validate_restore_layer_state` sait dire "commit or rollback before
   restore", mais n'est appelee que par `restore_batch`. Le chemin
   evenement par evenement ne la consulte pas : il ecrit directement
   par le provider, SOUS le tampon d'edition de l'utilisateur. Le
   commit suivant de l'utilisateur repasse par dessus les attributs
   qu'il avait en tampon.

   Resultat pour l'utilisateur : le plugin confirme "Reverted", puis
   son propre Enregistrer efface la restauration -- sur les attributs
   seulement, car la geometrie, elle, n'etait pas dans son tampon.
   L'entite finit hybride : geometrie d'hier, attributs d'aujourd'hui.
   Et le journal, lui, a enregistre une restauration reussie.

Ce que le scenario verifie apres coup : la couche est TOUJOURS relue
depuis le disque, apres fermeture de toute reference QGIS.

Verdict attendu : FAIL sur la duplication au rejeu et sur la
restauration effacee par le commit utilisateur.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_exec_replay_dirty_session"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_erds_datasource"
_PROJECT_FP = "tx_erds_project"
_GPKG_LAYER = "zones"

_SURVIVOR = ("GARE", "C002")
_GONE = ("PONT_DISPARU", "C001")

_USER_EDIT = "SAISIE_UTILISATEUR_NON_VALIDEE"
_RESTORED_NOM = "PONT_AVANT"
_RESTORED_XY = (11.0, 11.0)
_LIVE_XY = (1.0, 1.0)

_SCHEMA = [
    {"name": "nom", "type": "String", "length": 40, "precision": 0},
    {"name": "code", "type": "String", "length": 10, "precision": 0},
]


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------

def _make_shapefile(tmpdir: str, stem: str, rows) -> str:
    from osgeo import ogr

    folder = Path(tmpdir) / stem
    folder.mkdir(parents=True, exist_ok=True)
    path = str(folder / f"{stem}.shp")
    datasource = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(path)
    layer = datasource.CreateLayer(stem, geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    for index, (nom, code) in enumerate(rows):
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("nom", nom)
        feature.SetField("code", code)
        feature.SetGeometry(
            ogr.CreateGeometryFromWkt(f"POINT({index + 1} {index + 1})"))
        layer.CreateFeature(feature)
        feature = None
    datasource.FlushCache()
    datasource = None
    return path


def _make_gpkg(tmpdir: str, stem: str, rows) -> str:
    from osgeo import ogr

    path = str(Path(tmpdir) / f"{stem}.gpkg")
    datasource = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    layer = datasource.CreateLayer(_GPKG_LAYER, geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    for index, (nom, code) in enumerate(rows):
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("nom", nom)
        feature.SetField("code", code)
        feature.SetGeometry(
            ogr.CreateGeometryFromWkt(f"POINT({index + 1} {index + 1})"))
        layer.CreateFeature(feature)
        feature = None
    datasource.FlushCache()
    datasource = None
    return path


def _open(path: str):
    from qgis.core import QgsVectorLayer
    uri = f"{path}|layername={_GPKG_LAYER}" if path.endswith(".gpkg") else path
    layer = QgsVectorLayer(uri, "cible", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche invalide: {path}")
    return layer


def _rows(path: str) -> list:
    layer = _open(path)
    rows = []
    for feature in layer.getFeatures():
        geom = feature.geometry()
        point = geom.asPoint() if geom is not None and not geom.isNull() else None
        rows.append({
            "nom": feature["nom"],
            "code": feature["code"],
            "xy": (round(point.x(), 3), round(point.y(), 3)) if point else None,
        })
    rows.sort(key=lambda r: (str(r["nom"]), str(r["xy"])))
    del layer
    return rows


# ---------------------------------------------------------------------
# Evenements
# ---------------------------------------------------------------------

def _event(**overrides):
    from recoverland.core.audit_backend import AuditEvent

    payload = dict(
        event_id=1,
        project_fingerprint=_PROJECT_FP,
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_erds",
        layer_name_snapshot="cible",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": 0}),
        operation_type="DELETE",
        attributes_json=json.dumps({"all_attributes": {
            "nom": _GONE[0], "code": _GONE[1]}}),
        geometry_wkb=_wkb_point(7.0, 7.0),
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps(_SCHEMA),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint="fid:0",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )
    payload.update(overrides)
    return AuditEvent(**payload)


# ---------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_erds_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["shp_del"] = _make_shapefile(tmpdir, "shpdel", [_SURVIVOR])
    ctx.data["gpkg_del"] = _make_gpkg(tmpdir, "gpkgdel", [_SURVIVOR])
    ctx.data["shp_ins"] = _make_shapefile(
        tmpdir, "shpins", [_SURVIVOR, ("PONT", "C001")])
    ctx.data["gpkg_dirty"] = _make_gpkg(tmpdir, "gpkgdirty", [("PONT", "C001")])

    ctx.data["before_shp_del"] = _rows(ctx.data["shp_del"])
    ctx.data["before_gpkg_del"] = _rows(ctx.data["gpkg_del"])
    ctx.data["before_shp_ins"] = _rows(ctx.data["shp_ins"])
    ctx.data["before_dirty"] = _rows(ctx.data["gpkg_dirty"])

    flog(
        f"tx_exec_replay_dirty_session setup: trace_id={ctx.trace_id} "
        f"shp_del={ctx.data['before_shp_del']} "
        f"dirty={ctx.data['before_dirty']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_deleted_feature, restore_inserted_feature,
        restore_updated_feature, restore_batch, validate_restore_layer_state,
    )

    flog(f"tx_exec_replay_dirty_session run: trace_id={ctx.trace_id}", "INFO")

    _replay_delete_shapefile(ctx, restore_deleted_feature)
    _replay_delete_gpkg(ctx, restore_deleted_feature)
    _replay_undo_insert(ctx, restore_inserted_feature)
    _dirty_session(ctx, restore_updated_feature, restore_batch,
                   validate_restore_layer_state)

    flog(
        f"tx_exec_replay_dirty_session done: trace_id={ctx.trace_id} "
        f"shp_del_after={ctx.data.get('shp_del_after2')} "
        f"gpkg_del_after={ctx.data.get('gpkg_del_after2')} "
        f"dirty_after={ctx.data.get('dirty_after_commit')}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _replay_delete_shapefile(ctx, restore_deleted_feature) -> None:
    """Shapefile, identite FID-seule: la garde d'idempotence est sautee."""
    event = _event(event_id=10)
    layer = _open(ctx.data["shp_del"])
    ctx.data["shp_del_r1"] = dict(restore_deleted_feature(layer, event))
    del layer
    ctx.data["shp_del_after1"] = _rows(ctx.data["shp_del"])

    layer = _open(ctx.data["shp_del"])
    ctx.data["shp_del_r2"] = dict(restore_deleted_feature(layer, event))
    del layer
    ctx.data["shp_del_after2"] = _rows(ctx.data["shp_del"])


def _replay_delete_gpkg(ctx, restore_deleted_feature) -> None:
    """GPKG, identite forte (pk=fid): la garde ne se declenche pas non plus,
    car la re-insertion ne rend jamais son fid historique a l'entite."""
    event = _event(
        event_id=11,
        feature_identity_json=json.dumps(
            {"fid": 77, "pk_field": "fid", "pk_value": 77}),
        entity_fingerprint="fid:77",
    )
    layer = _open(ctx.data["gpkg_del"])
    ctx.data["gpkg_del_r1"] = dict(restore_deleted_feature(layer, event))
    del layer
    ctx.data["gpkg_del_after1"] = _rows(ctx.data["gpkg_del"])

    layer = _open(ctx.data["gpkg_del"])
    ctx.data["gpkg_del_r2"] = dict(restore_deleted_feature(layer, event))
    del layer
    ctx.data["gpkg_del_after2"] = _rows(ctx.data["gpkg_del"])


def _replay_undo_insert(ctx, restore_inserted_feature) -> None:
    """Annuler deux fois un INSERT: la seconde doit etre un non-evenement."""
    layer = _open(ctx.data["shp_ins"])
    target = None
    for feature in layer.getFeatures():
        if feature["nom"] == "PONT":
            target = feature
            break
    geom = target.geometry()
    point = geom.asPoint()
    event = _event(
        event_id=12,
        operation_type="INSERT",
        feature_identity_json=json.dumps({"fid": target.id()}),
        entity_fingerprint=f"fid:{target.id()}",
        attributes_json=json.dumps({"all_attributes": {
            "nom": "PONT", "code": "C001"}}),
        geometry_wkb=_wkb_point(point.x(), point.y()),
    )
    ctx.data["ins_r1"] = dict(restore_inserted_feature(layer, event))
    del layer
    ctx.data["ins_after1"] = _rows(ctx.data["shp_ins"])

    layer = _open(ctx.data["shp_ins"])
    ctx.data["ins_r2"] = dict(restore_inserted_feature(layer, event))
    del layer
    ctx.data["ins_after2"] = _rows(ctx.data["shp_ins"])


def _dirty_session(ctx, restore_updated_feature, restore_batch,
                   validate_restore_layer_state) -> None:
    """Restauration pendant une session d'edition non validee."""
    layer = _open(ctx.data["gpkg_dirty"])
    fid = next(layer.getFeatures()).id()
    event = _event(
        event_id=13,
        operation_type="UPDATE",
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        entity_fingerprint=f"fid:{fid}",
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": _RESTORED_NOM, "new": "PONT"}}}),
        geometry_wkb=_wkb_point(*_RESTORED_XY),
        new_geometry_wkb=_wkb_point(*_LIVE_XY),
    )

    layer.startEditing()
    index = layer.fields().indexOf("nom")
    layer.changeAttributeValue(fid, index, _USER_EDIT)
    ctx.data["dirty_is_modified"] = bool(layer.isModified())
    ctx.data["dirty_guard_says"] = validate_restore_layer_state(layer)

    ctx.data["dirty_batch"] = _batch_summary(
        restore_batch(layer, [event], trace_id=ctx.trace_id))
    ctx.data["dirty_result"] = dict(restore_updated_feature(layer, event))
    ctx.data["dirty_commit"] = bool(layer.commitChanges())
    del layer
    ctx.data["dirty_after_commit"] = _rows(ctx.data["gpkg_dirty"])


def _batch_summary(report) -> dict:
    return {
        "succeeded": list(report.succeeded),
        "failed": {int(k): str(v)[:120] for k, v in report.failed.items()},
        "traces": len(report.trace_events or ()),
    }


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    shp_after1 = ctx.data.get("shp_del_after1") or []
    shp_after2 = ctx.data.get("shp_del_after2") or []
    gpkg_after1 = ctx.data.get("gpkg_del_after1") or []
    gpkg_after2 = ctx.data.get("gpkg_del_after2") or []
    ins_after1 = ctx.data.get("ins_after1") or []
    ins_after2 = ctx.data.get("ins_after2") or []
    dirty_after = ctx.data.get("dirty_after_commit") or []
    dirty_row = dirty_after[0] if dirty_after else {}

    # ===== Volet A1 : rejeu d'un DELETE, shapefile ======================
    out.append((
        "delete_restore_actually_works_once",
        len(shp_after1) == len(ctx.data.get("before_shp_del") or []) + 1
        and (ctx.data.get("shp_del_r1") or {}).get("success") is True,
        f"apres 1 restauration={shp_after1} resultat="
        f"{ctx.data.get('shp_del_r1')}: le scenario a besoin d'une premiere "
        f"restauration reussie avant de tester le rejeu",
    ))
    out.append((
        "shapefile_replay_adds_no_duplicate",
        shp_after2 == shp_after1,
        f"apres 1 rejeu={shp_after1} apres 2 rejeux={shp_after2}: la meme "
        f"restauration lancee deux fois cree une seconde copie de l'entite. "
        f"Sur une couche FID-seule la garde \"Already exists (skip)\" ne "
        f"s'execute jamais (elle exige un pk_field que le shapefile n'a "
        f"pas), donc chaque double-clic ajoute un doublon silencieux.",
    ))
    out.append((
        "shapefile_replay_is_reported_as_a_skip",
        (ctx.data.get("shp_del_r2") or {}).get("skipped") is True
        or "skip" in str((ctx.data.get("shp_del_r2") or {}).get("message", "")).lower(),
        f"resultat du 2e rejeu={ctx.data.get('shp_del_r2')}: le second appel "
        f"repond exactement comme le premier (\"Restored\"), donc rien dans "
        f"le compte rendu ne permet de reperer le doublon",
    ))

    # ===== Volet A2 : rejeu d'un DELETE, GPKG (identite forte) ==========
    out.append((
        "gpkg_replay_adds_no_duplicate",
        gpkg_after2 == gpkg_after1,
        f"apres 1 rejeu={gpkg_after1} apres 2 rejeux={gpkg_after2}: meme sur "
        f"une couche a identite forte la duplication a lieu. La garde "
        f"cherche l'entite par son fid historique (77) mais addFeatures lui "
        f"a donne un fid neuf: la valeur cherchee ne peut jamais revenir, "
        f"donc la garde est structurellement morte.",
    ))

    # ===== Volet B : annuler deux fois un INSERT ========================
    out.append((
        "undo_insert_removes_the_feature_once",
        len(ins_after1) == len(ctx.data.get("before_shp_ins") or []) - 1,
        f"apres annulation={ins_after1}: l'annulation d'un INSERT doit "
        f"supprimer l'entite inseree",
    ))
    out.append((
        "undo_insert_replay_is_a_non_event",
        ins_after2 == ins_after1
        and (ctx.data.get("ins_r2") or {}).get("skipped") is True,
        f"apres rejeu={ins_after2} resultat={ctx.data.get('ins_r2')}: "
        f"rejouer l'annulation d'un INSERT ne doit rien supprimer d'autre "
        f"et doit se declarer comme un non-evenement",
    ))

    # ===== Volet C : session d'edition non validee ======================
    out.append((
        "guard_knows_the_session_is_dirty",
        bool(ctx.data.get("dirty_guard_says")),
        f"validate_restore_layer_state={ctx.data.get('dirty_guard_says')!r}: "
        f"le garde-fou existe et sait repondre; toute la question est de "
        f"savoir qui l'appelle",
    ))
    out.append((
        "batch_path_refuses_a_dirty_layer",
        (ctx.data.get("dirty_batch") or {}).get("succeeded") == []
        and bool((ctx.data.get("dirty_batch") or {}).get("failed")),
        f"restore_batch={ctx.data.get('dirty_batch')}: le chemin par lot "
        f"consulte bien le garde-fou et refuse",
    ))
    out.append((
        "event_path_refuses_a_dirty_layer_too",
        (ctx.data.get("dirty_result") or {}).get("success") is False,
        f"restore_updated_feature={ctx.data.get('dirty_result')} alors que "
        f"restore_batch avait refuse la MEME couche et le MEME evenement: "
        f"la restauration evenement par evenement ecrit directement par le "
        f"provider, sous le tampon d'edition de l'utilisateur, sans jamais "
        f"appeler validate_restore_layer_state",
    ))
    out.append((
        "announced_restore_survives_the_user_commit",
        (ctx.data.get("dirty_result") or {}).get("success") is not True
        or dirty_row.get("nom") == _RESTORED_NOM,
        f"le plugin a confirme {(ctx.data.get('dirty_result') or {}).get('message')!r} "
        f"puis l'enregistrement de l'utilisateur a remis "
        f"nom={dirty_row.get('nom')!r}: la restauration confirmee a ete "
        f"effacee par le commit suivant, et le journal a pourtant enregistre "
        f"une restauration reussie",
    ))
    out.append((
        "feature_is_not_left_hybrid",
        (dirty_row.get("nom") == _RESTORED_NOM)
        == (dirty_row.get("xy") == _RESTORED_XY),
        f"entite finale={dirty_row}: la geometrie est revenue a "
        f"{_RESTORED_XY} (elle n'etait pas dans le tampon de l'utilisateur) "
        f"tandis que l'attribut est reste a la saisie non validee. L'entite "
        f"n'a jamais existe sous cette forme: ni etat d'hier, ni etat "
        f"d'aujourd'hui.",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_exec_replay_dirty_session.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Garde de source ==============================================
    src = (_PLUGIN_ROOT / "core" / "restore_service.py").read_text(
        encoding="utf-8", errors="replace")
    for func, following in (
        ("def restore_updated_feature(", "def validate_restore_layer_state("),
        ("def restore_deleted_feature(", "def restore_inserted_feature("),
    ):
        start = src.find(func)
        end = src.find(following, start)
        body = src[start:end] if start >= 0 and end > start else ""
        name = func[4:-1]
        out.append((
            f"{name}_checks_the_edit_session",
            "validate_restore_layer_state" in body or "isEditable" in body,
            f"{name} ecrit par le provider sans verifier si une session "
            f"d'edition non validee est ouverte; seul restore_batch consulte "
            f"validate_restore_layer_state",
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
