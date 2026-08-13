"""tx_exec_schema_drift_silence - RecoverLand validation runtime.

Invariant: RLU-053 (une restauration partielle ne doit jamais etre
annoncee comme une restauration complete).

Le mecanisme
------------
Entre la capture d'un evenement et sa restauration, le schema de la
couche bouge : on renomme un champ, on change son type, on en supprime
un, on en ajoute un. `core.schema_drift` sait decrire tout cela
(`DriftReport` porte `missing_in_current`, `type_changed`,
`added_in_current`, et `format_drift_message` sait le formuler). Mais
les deux chemins d'ecriture n'en font pas le meme usage :

1. `restore_service.pre_check_restore` ne bloque QUE sur
   `missing_in_current`. Une derive de TYPE seule (`pop` entier devenu
   texte) laisse passer le pre-check avec la raison litterale "OK".
2. `schema_drift.build_field_mapping` ne retient que `drift.matched` :
   les champs manquants ET les champs de type change sont retires du
   mapping. `serialization.iter_mapped_attributes` les saute en
   silence (un simple flog WARNING dans le fichier de log).
3. `restore_executor._buffer_update` -- le chemin du REWIND -- ne passe
   jamais par `pre_check_restore`. Il recalcule le mapping localement
   via `_safe_field_mapping` et renvoie
   `{"status": "APPLIED", "message": "Reverted via buffer"}` quel que
   soit le nombre de champs abandonnes en route.

Le scenario d'echec concret
---------------------------
Un evenement a capture 4 champs (nom, code, pop, alt). Depuis, `code`
a ete renomme `code_insee`, `pop` est passe entier -> texte, `alt` a
ete supprime, `zone` a ete ajoute. L'utilisateur lance un REWIND. Un
seul champ sur quatre est reellement reecrit, et le rapport affiche
APPLIED. L'utilisateur croit son entite revenue a son etat d'avant ;
elle est en realite un hybride : `nom` d'hier, `code_insee` et `pop`
d'aujourd'hui. Rien, ni dans le resultat ni dans le journal, ne le
signale.

Troisieme volet : `_types_compatible` ne connait pas "Integer64".
Exporter un GPKG (`pop` = Integer) en shapefile (`pop` = Integer64) --
une manipulation banale -- suffit a faire passer un entier pour
incompatible avec un entier, donc a le faire sauter en silence.

Ce que le scenario verifie apres coup : la couche est TOUJOURS relue
depuis le disque, champ par champ, pour comparer ce qui a ete annonce
avec ce qui a ete ecrit.

Verdict attendu : FAIL tant que les champs abandonnes ne remontent pas
dans le resultat de la restauration.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_exec_schema_drift_silence"
INVARIANT = "RLU-053"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_esds_datasource"
_PROJECT_FP = "tx_esds_project"
_LAYER = "zones"

# Etat vivant de l'entite au moment ou la restauration est demandee.
_LIVE = {"nom": "PONT", "code": "C001", "pop": 100, "alt": 12}
# Etat d'avant l'edition, celui que la restauration doit reecrire.
_OLD = {"nom": "PONT_AVANT", "code": "C000", "pop": 55, "alt": 7}

_SCHEMA = [
    {"name": "nom", "type": "String", "length": 40, "precision": 0},
    {"name": "code", "type": "String", "length": 10, "precision": 0},
    {"name": "pop", "type": "Integer", "length": 10, "precision": 0},
    {"name": "alt", "type": "Integer", "length": 10, "precision": 0},
]


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _string_field(name: str):
    from qgis.core import QgsField
    try:
        from qgis.PyQt.QtCore import QMetaType
        return QgsField(name, QMetaType.Type.QString)
    except Exception:  # noqa: BLE001 - QGIS < 3.38 fallback
        from qgis.PyQt.QtCore import QVariant
        return QgsField(name, QVariant.String)


def _make_gpkg(tmpdir: str, stem: str) -> str:
    """GPKG a une entite portant l'etat vivant _LIVE."""
    from osgeo import ogr

    path = str(Path(tmpdir) / f"{stem}.gpkg")
    datasource = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    layer = datasource.CreateLayer(_LAYER, geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("pop", ogr.OFTInteger))
    layer.CreateField(ogr.FieldDefn("alt", ogr.OFTInteger))
    feature = ogr.Feature(layer.GetLayerDefn())
    for key, value in _LIVE.items():
        feature.SetField(key, value)
    feature.SetGeometry(ogr.CreateGeometryFromWkt("POINT(1 1)"))
    layer.CreateFeature(feature)
    feature = None
    datasource.FlushCache()
    datasource = None
    return path


def _open(path: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername={_LAYER}", _LAYER, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path}")
    return layer


def _disk_row(path: str) -> dict:
    """Relit le FICHIER dans une couche neuve; aucun cache provider."""
    layer = _open(path)
    row = {}
    feature = next(layer.getFeatures(), None)
    if feature is not None:
        for field in layer.fields():
            if field.name() == "fid":
                continue
            value = feature[field.name()]
            row[field.name()] = None if _is_null(value) else value
    del layer
    return row


def _is_null(value) -> bool:
    if value is None:
        return True
    try:
        return bool(hasattr(value, "isNull") and value.isNull())
    except (TypeError, RuntimeError):
        return False


def _event(fid: int, fields: tuple) -> "object":
    """UPDATE portant l'ancien etat de chaque champ de *fields*."""
    from recoverland.core.audit_backend import AuditEvent

    changed = {
        name: {"old": _OLD[name], "new": _LIVE[name]} for name in fields
    }
    schema = [entry for entry in _SCHEMA if entry["name"] in fields]
    return AuditEvent(
        event_id=42,
        project_fingerprint=_PROJECT_FP,
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_esds",
        layer_name_snapshot=_LAYER,
        provider_type="ogr",
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": changed}),
        geometry_wkb=_wkb_point(1.0, 1.0),
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps(schema),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=_wkb_point(1.0, 1.0),
        invalidated_at=None,
    )


def _drift_full(path: str) -> None:
    """Renomme, retype, supprime, ajoute -- dans cet ordre, en vrai."""
    layer = _open(path)
    provider = layer.dataProvider()

    provider.renameAttributes(
        {layer.fields().indexOf("code"): "code_insee"})
    layer.updateFields()

    provider.deleteAttributes([layer.fields().indexOf("pop")])
    layer.updateFields()
    provider.addAttributes([_string_field("pop")])
    layer.updateFields()

    provider.deleteAttributes([layer.fields().indexOf("alt")])
    layer.updateFields()

    provider.addAttributes([_string_field("zone")])
    layer.updateFields()
    del layer


def _drift_type_only(path: str) -> None:
    """Seul `pop` change de type : entier -> texte."""
    layer = _open(path)
    provider = layer.dataProvider()
    provider.deleteAttributes([layer.fields().indexOf("pop")])
    layer.updateFields()
    provider.addAttributes([_string_field("pop")])
    layer.updateFields()
    del layer


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_esds_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["path_full"] = _make_gpkg(tmpdir, "drift_full")
    ctx.data["path_type"] = _make_gpkg(tmpdir, "drift_type")

    layer = _open(ctx.data["path_full"])
    ctx.data["fid"] = next(layer.getFeatures()).id()
    del layer

    _drift_full(ctx.data["path_full"])
    _drift_type_only(ctx.data["path_type"])

    ctx.data["schema_full"] = list(_disk_row(ctx.data["path_full"]).keys())
    ctx.data["schema_type"] = list(_disk_row(ctx.data["path_type"]).keys())
    ctx.data["before_full"] = _disk_row(ctx.data["path_full"])
    ctx.data["before_type"] = _disk_row(ctx.data["path_type"])

    flog(
        f"tx_exec_schema_drift_silence setup: trace_id={ctx.trace_id} "
        f"schema_full={ctx.data['schema_full']} "
        f"schema_type={ctx.data['schema_type']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        pre_check_restore, restore_updated_feature,
    )
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.schema_drift import (
        compare_schemas, extract_current_schema, parse_field_schema,
        format_drift_message,
    )

    flog(f"tx_exec_schema_drift_silence run: trace_id={ctx.trace_id}", "INFO")

    fid = ctx.data["fid"]
    full_event = _event(fid, ("nom", "code", "pop", "alt"))
    type_event = _event(fid, ("nom", "code", "pop"))

    # --- volet 1 : derive complete, chemin RESTORE evenement par evenement
    layer = _open(ctx.data["path_full"])
    check = pre_check_restore(layer, full_event)
    ctx.data["full_precheck"] = {
        "can_restore": bool(check.can_restore),
        "reason": str(check.reason),
        "missing": list(check.drift.missing_in_current) if check.drift else [],
        "type_changed": dict(check.drift.type_changed) if check.drift else {},
        "added": list(check.drift.added_in_current) if check.drift else [],
    }
    ctx.data["full_restore_result"] = dict(
        restore_updated_feature(layer, full_event))
    del layer
    ctx.data["after_full_restore"] = _disk_row(ctx.data["path_full"])

    # --- volet 2 : derive complete, chemin REWIND (tampon, sans pre-check)
    layer = _open(ctx.data["path_full"])
    layer.startEditing()
    ctx.data["full_buffer_result"] = dict(
        _apply_via_buffer(layer, "UPDATE", full_event, trace_id=ctx.trace_id))
    ctx.data["full_buffer_commit"] = bool(layer.commitChanges())
    del layer
    ctx.data["after_full_buffer"] = _disk_row(ctx.data["path_full"])

    # --- volet 3 : derive de TYPE seule, chemin RESTORE
    layer = _open(ctx.data["path_type"])
    check_t = pre_check_restore(layer, type_event)
    ctx.data["type_precheck"] = {
        "can_restore": bool(check_t.can_restore),
        "reason": str(check_t.reason),
        "type_changed": dict(check_t.drift.type_changed) if check_t.drift else {},
        "drift_message": format_drift_message(check_t.drift) if check_t.drift else "",
    }
    ctx.data["type_restore_result"] = dict(
        restore_updated_feature(layer, type_event))
    del layer
    ctx.data["after_type"] = _disk_row(ctx.data["path_type"])

    # --- volet 4 : Integer (GPKG) vs Integer64 (shapefile), meme donnee
    shp_layer = _export_shapefile(ctx.data["tmpdir"], ctx.data["path_type"])
    hist = parse_field_schema(json.dumps(_SCHEMA))
    drift_shp = compare_schemas(hist, extract_current_schema(shp_layer))
    ctx.data["shp_types"] = {
        f.name(): f.typeName() for f in shp_layer.fields()
    }
    ctx.data["shp_type_changed"] = dict(drift_shp.type_changed)
    ctx.data["shp_matched"] = list(drift_shp.matched)
    del shp_layer

    flog(
        f"tx_exec_schema_drift_silence done: trace_id={ctx.trace_id} "
        f"full_restore={ctx.data['full_restore_result']} "
        f"full_buffer={ctx.data['full_buffer_result']} "
        f"type_restore={ctx.data['type_restore_result']} "
        f"after_type={ctx.data['after_type']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _export_shapefile(tmpdir: str, gpkg_path: str):
    """Meme couche exportee en shapefile : les entiers deviennent Integer64."""
    from qgis.core import (
        QgsVectorFileWriter, QgsCoordinateTransformContext, QgsVectorLayer,
    )

    source = _open(gpkg_path)
    target = str(Path(tmpdir) / "export.shp")
    options = QgsVectorFileWriter.SaveVectorOptions()
    options.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        source, target, QgsCoordinateTransformContext(), options)
    del source
    layer = QgsVectorLayer(target, "export", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"export shapefile invalide: {target}")
    return layer


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    precheck = ctx.data.get("full_precheck") or {}
    restore = ctx.data.get("full_restore_result") or {}
    buffer_res = ctx.data.get("full_buffer_result") or {}
    after_restore = ctx.data.get("after_full_restore") or {}
    after_buffer = ctx.data.get("after_full_buffer") or {}
    t_precheck = ctx.data.get("type_precheck") or {}
    t_restore = ctx.data.get("type_restore_result") or {}
    after_type = ctx.data.get("after_type") or {}

    # ===== Amorcage =====================================================
    out.append((
        "drift_really_applied",
        ctx.data.get("schema_full") == ["nom", "code_insee", "pop", "zone"],
        f"schema apres derive={ctx.data.get('schema_full')}: le scenario a "
        f"besoin d'un champ renomme, d'un champ retype, d'un champ supprime "
        f"et d'un champ ajoute",
    ))
    out.append((
        "drift_detected",
        sorted(precheck.get("missing") or []) == ["alt", "code"]
        and "pop" in (precheck.get("type_changed") or {}),
        f"drift={precheck}: le detecteur doit voir code et alt disparus et "
        f"pop retype, sinon rien de ce qui suit n'a de sens",
    ))

    # ===== Chemin RESTORE : le refus est-il complet ? ====================
    out.append((
        "restore_refuses_when_fields_are_gone",
        restore.get("success") is False,
        f"resultat={restore}: reecrire un etat ancien alors que deux des "
        f"quatre champs captures n'existent plus produirait une entite "
        f"hybride; le refus est la bonne reponse",
    ))
    out.append((
        "refusal_message_lists_every_lost_field",
        all(token in str(restore.get("message", ""))
            for token in ("code", "alt", "pop")),
        f"message={restore.get('message')!r}: la raison ne nomme que les "
        f"champs disparus et tait le champ retype (pop). L'utilisateur qui "
        f"recree 'code' et 'alt' relancera la restauration en croyant le "
        f"probleme regle, et perdra pop sans avertissement.",
    ))
    out.append((
        "refused_restore_touches_nothing",
        after_restore == (ctx.data.get("before_full") or {}),
        f"avant={ctx.data.get('before_full')} apres={after_restore}: un refus "
        f"doit laisser la donnee strictement intacte",
    ))

    # ===== Chemin REWIND : APPLIED en silence ? ==========================
    dropped = ("code", "alt", "pop")
    reported = json.dumps(buffer_res, ensure_ascii=False, default=str)
    out.append((
        "buffer_update_reports_dropped_fields",
        all(name in reported for name in dropped),
        f"resultat du rewind={buffer_res}: 3 des 4 champs captures ont ete "
        f"abandonnes (code renomme, alt supprime, pop retype) et le resultat "
        f"annonce APPLIED sans en citer un seul. L'utilisateur croit son "
        f"entite revenue a son etat d'avant; elle est un hybride.",
    ))
    out.append((
        "buffer_update_partial_is_not_a_plain_success",
        buffer_res.get("status") != "APPLIED"
        or buffer_res.get("reason_code") != "applied",
        f"status={buffer_res.get('status')} reason_code="
        f"{buffer_res.get('reason_code')}: une restauration amputee de 3 "
        f"champs sur 4 est comptee dans la meme categorie qu'une "
        f"restauration complete, donc invisible dans le decompte final",
    ))
    out.append((
        "rewind_ignores_the_precheck_entirely",
        ctx.data.get("full_buffer_commit") is True
        and after_buffer.get("nom") == _OLD["nom"],
        f"commit={ctx.data.get('full_buffer_commit')} disque={after_buffer}: "
        f"le chemin REWIND ecrit la ou le chemin RESTORE avait refuse "
        f"d'ecrire, sur exactement le meme evenement et la meme couche",
    ))

    # ===== Aucune valeur ne doit atterrir dans le mauvais champ ==========
    out.append((
        "no_value_lands_in_a_renamed_field",
        after_buffer.get("code_insee") == _LIVE["code"],
        f"code_insee={after_buffer.get('code_insee')!r} attendu "
        f"{_LIVE['code']!r}: l'ancienne valeur de 'code' ne doit jamais "
        f"suivre le renommage toute seule, sinon la restauration ecrit une "
        f"donnee d'hier dans un champ qui ne la designe plus",
    ))
    out.append((
        "no_value_lands_in_a_new_field",
        after_buffer.get("zone") is None,
        f"zone={after_buffer.get('zone')!r}: le champ ajoute apres la capture "
        f"doit rester vide; toute valeur qui y atterrit vient d'un decalage "
        f"d'indice et corrompt la couche",
    ))

    # ===== Derive de TYPE seule : le pre-check dit "OK" ==================
    out.append((
        "type_drift_is_not_announced_as_ok",
        t_precheck.get("reason") != "OK",
        f"pre_check raison={t_precheck.get('reason')!r} alors que "
        f"type_changed={t_precheck.get('type_changed')}: le pre-vol repond "
        f"litteralement \"OK\" sur une couche ou un champ ne peut pas etre "
        f"restaure. format_drift_message sait le dire "
        f"({t_precheck.get('drift_message')!r}) mais n'est pas appele ici.",
    ))
    out.append((
        "type_drift_restore_reports_the_skip",
        t_restore.get("success") is not True
        or "pop" in json.dumps(t_restore, ensure_ascii=False, default=str),
        f"resultat={t_restore}: la restauration renvoie un succes nu "
        f"(\"Reverted\") sans mentionner que pop n'a pas ete restaure",
    ))
    out.append((
        "type_drift_value_actually_restored_or_refused",
        (t_restore.get("success") is not True)
        or str(after_type.get("pop")) == str(_OLD["pop"]),
        f"le plugin annonce {t_restore.get('message')!r} mais le disque "
        f"contient pop={after_type.get('pop')!r} au lieu de "
        f"{_OLD['pop']!r}: l'utilisateur a une confirmation de restauration "
        f"pour une valeur qui n'a jamais ete ecrite",
    ))

    # ===== Integer vs Integer64 =========================================
    out.append((
        "integer64_is_a_compatible_integer",
        "alt" in (ctx.data.get("shp_matched") or []),
        f"types shapefile={ctx.data.get('shp_types')} "
        f"type_changed={ctx.data.get('shp_type_changed')}: exporter un GPKG "
        f"en shapefile transforme Integer en Integer64, un type absent du "
        f"groupe de compatibilite de _types_compatible. Tous les champs "
        f"entiers deviennent alors 'incompatibles' et sont sautes en silence "
        f"a chaque restauration sur shapefile, alors qu'un entier se "
        f"restaure parfaitement dans un entier.",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"iter_mapped_attributes: \d+ field\(s\) skipped",
        name="dropped_fields_leave_at_least_a_log_trace",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_exec_schema_drift_silence.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Garde de source ==============================================
    drift_src = (_PLUGIN_ROOT / "core" / "schema_drift.py").read_text(
        encoding="utf-8", errors="replace")
    start = drift_src.find("def build_field_mapping(")
    end = drift_src.find("def format_drift_message(", start)
    body = drift_src[start:end] if start >= 0 and end > start else ""
    out.append((
        "mapping_api_can_report_what_it_dropped",
        "dropped" in body,
        "build_field_mapping / safe_field_mapping ne rendent qu'un dict "
        "nom->nom: les champs manquants et les champs retypes disparaissent "
        "du resultat sans que l'appelant puisse savoir lesquels. Tant que "
        "l'API ne rend pas la liste des abandons, aucun appelant ne PEUT "
        "prevenir l'utilisateur.",
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
