"""tx_geom_crs_restore_mismatch - RecoverLand validation runtime.

Invariant: I-3 (l'ecriture doit viser la bonne entite, au bon endroit).

Le mecanisme
------------
Chaque evenement du journal porte `crs_authid`, le CRS de la couche AU
MOMENT DE LA CAPTURE (`geometry_utils.extract_crs_authid`). Le WKB est
stocke octet a octet, donc en coordonnees brutes de ce CRS-la.

Cote lecture, le plugin sait reprojeter: `geometry_utils.
reproject_geometry_for_render` transforme la geometrie de l'evenement vers
le CRS du canevas avant de la peindre (Time Lens), et `geometry_preview`
fait de meme. Cote ECRITURE, rien:

    core/restore_service.py:restore_deleted_feature
        geom = rebuild_geometry(event.geometry_wkb)
        ...
        new_feature.setGeometry(geom)          # tel quel

    core/restore_service.py:restore_updated_feature
        geom = rebuild_geometry(event.geometry_wkb)
        provider.changeGeometryValues({target_fid: geom})   # tel quel

    core/restore_executor.py:_buffer_update
        geom = rebuild_geometry(event.geometry_wkb)
        layer.changeGeometry(target_fid, geom)              # tel quel

`event.crs_authid` n'est jamais compare a `layer.crs()`. Aucune
reprojection, aucun refus, aucun avertissement.

Le scenario d'echec concret
---------------------------
Un utilisateur travaille sur un reseau en EPSG:4326 (couche telechargee en
degres). Le plugin journalise ses modifications. Plus tard il reprojette le
jeu de donnees en Lambert-93 (EPSG:2154) au meme emplacement, ou il corrige
un CRS mal declare. Le journal, lui, contient toujours des degres.

  - Il restaure un evenement DELETE: la feature est re-inseree a
    (2.35, 48.85) dans une couche metrique, soit a ~6 900 km du bon
    endroit, quelque part au large du golfe de Guinee. L'operation est
    rapportee "Restored", et la feature est invisible sur le canevas.
  - Il restaure un evenement UPDATE attributaire (edit_tracker ne pose
    `new_geometry_wkb` que si la geometrie a change, ligne 969): la preuve
    de cible passe par les attributs, puis `has_geom = event.geometry_wkb
    is not None` declenche quand meme une reecriture de geometrie. La
    feature existante est TELEPORTEE alors que l'utilisateur ne demandait
    que le retour d'un attribut.

Le meme mecanisme frappe le cas inverse (journal metrique, couche en
degres): les coordonnees Lambert sortent de la plage valide du WGS84.

Plan du scenario (vrai GeoPackage en EPSG:2154, vrai provider OGR):
    setup : une couche "reseau" EPSG:2154, une feature PYLONE a
            (652000, 6862000) - Paris en Lambert-93.
    cas A : UPDATE attributaire capture en EPSG:4326, geometrie
            (2.35 48.85). Doit etre reprojete ou refuse.
    cas B : DELETE capture en EPSG:4326, meme geometrie. Re-insertion:
            doit atterrir sur Paris, pas sur l'equateur.
    cas C : temoin positif - DELETE capture en EPSG:2154. Doit passer et
            atterrir exactement aux coordonnees capturees (le garde-fou
            eventuel ne doit pas casser une restauration legitime).

Verdict attendu si le produit est sain: PASS.
"""
from __future__ import annotations

import json
import math
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_geom_crs_restore_mismatch"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LAYER_CRS = "EPSG:2154"
_EVENT_CRS = "EPSG:4326"

# Paris, exprime dans les deux CRS.
_WGS84_XY = (2.35, 48.85)
_L93_XY = (652000.0, 6862000.0)
# Temoin positif: une position Lambert-93 voisine, capturee dans le bon CRS.
_L93_CONTROL_XY = (652500.0, 6862500.0)

_FIELDS = [
    {"name": "nom", "type": "String", "length": 32, "precision": 0},
    {"name": "code", "type": "String", "length": 32, "precision": 0},
]
_LIVE_NAME = "PYLONE"
_LIVE_CODE = "P042"
_OLD_NAME = "PYLONE_AVANT"

# Tolerance de position apres restauration, en metres (CRS metrique).
_POS_TOL_M = 1.0


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _make_gpkg(tmpdir: str):
    """Ecrit un GeoPackage mono-feature en EPSG:2154 et renvoie la couche."""
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        f"Point?crs={_LAYER_CRS}&field=nom:string(32)&field=code:string(32)",
        "seed", "memory",
    )
    mem.startEditing()
    feat = QgsFeature(mem.fields())
    feat.setAttributes([_LIVE_NAME, _LIVE_CODE])
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*_L93_XY)))
    mem.addFeature(feat)
    mem.commitChanges()

    path = str(Path(tmpdir) / "reseau.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "reseau"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)

    layer = QgsVectorLayer(f"{path}|layername=reseau", "reseau", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path}")
    return layer


def _event(operation, geom_wkb, crs, fid, attrs_json, new_geom_wkb=None,
           event_id=1):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=event_id,
        project_fingerprint="tx_crs_project",
        datasource_fingerprint="tx_crs_datasource",
        layer_id_snapshot="lyr_tx_crs",
        layer_name_snapshot="reseau",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type=operation,
        attributes_json=attrs_json,
        geometry_wkb=geom_wkb,
        geometry_type="Point",
        crs_authid=crs,
        field_schema_json=json.dumps(_FIELDS),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=new_geom_wkb,
        invalidated_at=None,
    )


def _feature_xy(layer, fid):
    """Coordonnees persistees de la feature *fid*, relues depuis le provider."""
    from qgis.core import QgsFeatureRequest
    feat = next(layer.dataProvider().getFeatures(QgsFeatureRequest(int(fid))),
                None)
    if feat is None:
        return None
    geom = feat.geometry()
    if geom is None or geom.isNull() or geom.isEmpty():
        return None
    pt = geom.asPoint()
    return (round(pt.x(), 4), round(pt.y(), 4))


def _all_xy(layer):
    out = {}
    for feat in layer.dataProvider().getFeatures():
        geom = feat.geometry()
        if geom is None or geom.isNull() or geom.isEmpty():
            out[feat.id()] = None
            continue
        pt = geom.asPoint()
        out[feat.id()] = (round(pt.x(), 4), round(pt.y(), 4))
    return out


def _dist(a, b):
    if a is None or b is None:
        return float("inf")
    return math.hypot(a[0] - b[0], a[1] - b[1])


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject,
        QgsPointXY,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_crs_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_gpkg(tmpdir)
    ctx.data["layer"] = layer
    ctx.data["layer_crs"] = layer.crs().authid()

    live = next(layer.getFeatures(), None)
    ctx.data["live_fid"] = live.id() if live is not None else None
    ctx.data["live_xy"] = _feature_xy(layer, ctx.data["live_fid"])

    # Position VRAIE de (2.35, 48.85) une fois reprojetee en Lambert-93:
    # c'est la ou une restauration correcte doit deposer la geometrie.
    xform = QgsCoordinateTransform(
        QgsCoordinateReferenceSystem(_EVENT_CRS),
        QgsCoordinateReferenceSystem(_LAYER_CRS),
        QgsProject.instance(),
    )
    target = xform.transform(QgsPointXY(*_WGS84_XY))
    ctx.data["expected_l93"] = (round(target.x(), 4), round(target.y(), 4))

    from recoverland.core.identity import get_identity_strength_for_layer
    ctx.data["strength"] = str(get_identity_strength_for_layer(layer))

    flog(
        f"tx_geom_crs_restore_mismatch setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} layer_crs={ctx.data['layer_crs']} "
        f"live_fid={ctx.data['live_fid']} live_xy={ctx.data['live_xy']} "
        f"expected_l93={ctx.data['expected_l93']} "
        f"strength={ctx.data['strength']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.restore_service import (
        restore_deleted_feature, restore_updated_feature,
    )
    from recoverland.core.logger import flog

    layer = ctx.data["layer"]
    fid = ctx.data["live_fid"]

    flog(f"tx_geom_crs_restore_mismatch run start: trace_id={ctx.trace_id}",
         "INFO")

    # --- cas A: UPDATE attributaire capture en EPSG:4326 -----------------
    # edit_tracker ne remplit new_geometry_wkb que si la geometrie a change;
    # une edition d'attribut pur laisse donc geometry_wkb rempli (etat OLD)
    # et new_geometry_wkb a None. La preuve de cible passe par les attributs,
    # puis la geometrie est reecrite quand meme.
    upd = _event(
        "UPDATE",
        _wkb_point(*_WGS84_XY),
        _EVENT_CRS,
        fid,
        json.dumps({"changed_only": {"nom": {"old": _OLD_NAME,
                                             "new": _LIVE_NAME}}}),
        new_geom_wkb=None,
        event_id=101,
    )
    ctx.data["upd_result"] = restore_updated_feature(layer, upd)
    layer.reload()
    ctx.data["after_upd_xy"] = _feature_xy(layer, fid)
    ctx.data["after_upd_dist_expected"] = _dist(
        ctx.data["after_upd_xy"], ctx.data["expected_l93"])
    ctx.data["after_upd_dist_origin"] = _dist(
        ctx.data["after_upd_xy"], ctx.data["live_xy"])
    ctx.data["upd_wrote_raw_degrees"] = (
        ctx.data["after_upd_xy"] is not None
        and _dist(ctx.data["after_upd_xy"], _WGS84_XY) < 1e-3
    )

    # --- cas B: DELETE capture en EPSG:4326, re-insertion ----------------
    before_b = set(_all_xy(layer).keys())
    dele = _event(
        "DELETE",
        _wkb_point(*_WGS84_XY),
        _EVENT_CRS,
        9999,  # feature disparue: aucune correspondance vivante
        json.dumps({"all_attributes": {"nom": "POSTE_4326", "code": "D001"}}),
        event_id=102,
    )
    ctx.data["del_result"] = restore_deleted_feature(layer, dele)
    layer.reload()
    after_b = _all_xy(layer)
    new_fids_b = sorted(set(after_b.keys()) - before_b)
    ctx.data["del_new_fids"] = new_fids_b
    ctx.data["del_new_xy"] = after_b.get(new_fids_b[0]) if new_fids_b else None
    ctx.data["del_dist_expected"] = _dist(
        ctx.data["del_new_xy"], ctx.data["expected_l93"])
    ctx.data["del_wrote_raw_degrees"] = (
        ctx.data["del_new_xy"] is not None
        and _dist(ctx.data["del_new_xy"], _WGS84_XY) < 1e-3
    )

    # --- cas C: temoin positif, DELETE capture dans le CRS de la couche --
    before_c = set(_all_xy(layer).keys())
    ctrl = _event(
        "DELETE",
        _wkb_point(*_L93_CONTROL_XY),
        _LAYER_CRS,
        9998,
        json.dumps({"all_attributes": {"nom": "POSTE_2154", "code": "D002"}}),
        event_id=103,
    )
    ctx.data["ctrl_result"] = restore_deleted_feature(layer, ctrl)
    layer.reload()
    after_c = _all_xy(layer)
    new_fids_c = sorted(set(after_c.keys()) - before_c)
    ctx.data["ctrl_new_xy"] = after_c.get(new_fids_c[0]) if new_fids_c else None
    ctx.data["ctrl_dist"] = _dist(ctx.data["ctrl_new_xy"], _L93_CONTROL_XY)

    flog(
        f"tx_geom_crs_restore_mismatch run end: trace_id={ctx.trace_id} "
        f"upd={ctx.data['upd_result']} after_upd_xy={ctx.data['after_upd_xy']} "
        f"del={ctx.data['del_result']} del_xy={ctx.data['del_new_xy']} "
        f"ctrl={ctx.data['ctrl_result']} ctrl_xy={ctx.data['ctrl_new_xy']}",
        "INFO",
    )

    ctx.data["layer"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    live_xy = ctx.data.get("live_xy")
    expected = ctx.data.get("expected_l93")
    upd_res = ctx.data.get("upd_result") or {}
    del_res = ctx.data.get("del_result") or {}
    ctrl_res = ctx.data.get("ctrl_result") or {}

    # ===== Sanity: le decor est bien celui qu'on croit ===================
    out.append((
        "fixture_layer_is_lambert93",
        ctx.data.get("layer_crs") == _LAYER_CRS,
        f"layer_crs={ctx.data.get('layer_crs')} attendu={_LAYER_CRS}: sans "
        f"couche metrique, le scenario ne prouve rien",
    ))
    out.append((
        "fixture_seeded_on_paris",
        live_xy is not None and _dist(live_xy, _L93_XY) < _POS_TOL_M,
        f"live_xy={live_xy} attendu={_L93_XY}",
    ))
    out.append((
        "fixture_reference_point_is_far",
        expected is not None and _dist(expected, _WGS84_XY) > 1e6,
        f"expected_l93={expected}: la reprojection de {_WGS84_XY} doit "
        f"produire un point tres eloigne des degres bruts, sinon la mesure "
        f"d'erreur n'a pas de sens",
    ))

    # ===== Cas A: UPDATE attributaire, evenement en 4326 =================
    upd_xy = ctx.data.get("after_upd_xy")
    d_exp = ctx.data.get("after_upd_dist_expected")
    upd_ok = (
        upd_res.get("success") is False
        or (isinstance(d_exp, float) and d_exp < _POS_TOL_M)
        # tolerable aussi: la geometrie n'a pas ete touchee du tout
        or (upd_xy is not None and _dist(upd_xy, live_xy) < _POS_TOL_M)
    )
    out.append((
        "update_does_not_teleport_the_feature",
        upd_ok,
        f"la feature est passee de {live_xy} a {upd_xy} (couche "
        f"{ctx.data.get('layer_crs')}) alors que l'evenement etait capture en "
        f"{_EVENT_CRS}. Ecart avec la position correcte "
        f"({expected}): {d_exp} unites de CRS, soit ~{(d_exp or 0)/1000:.0f} km. "
        f"L'utilisateur ne demandait que le retour d'un attribut: sa feature "
        f"a disparu du canevas. result={upd_res}",
    ))
    out.append((
        "update_never_writes_raw_degrees",
        not ctx.data.get("upd_wrote_raw_degrees"),
        f"after_upd_xy={upd_xy}: les degres du journal ont ete ecrits tels "
        f"quels dans une couche en metres. Aucune conversion, aucun refus, "
        f"aucun avertissement.",
    ))

    # ===== Cas B: DELETE restaure = re-insertion ========================
    del_xy = ctx.data.get("del_new_xy")
    d_del = ctx.data.get("del_dist_expected")
    del_ok = (
        del_res.get("success") is False
        or (isinstance(d_del, float) and d_del < _POS_TOL_M)
    )
    out.append((
        "deleted_feature_is_reinserted_at_the_right_place",
        del_ok,
        f"la feature recreee est a {del_xy}, la bonne position etant "
        f"{expected}: erreur de {d_del} unites de CRS "
        f"(~{(d_del or 0)/1000:.0f} km). La restauration est rapportee "
        f"{del_res.get('message')!r} alors que la donnee est inexploitable. "
        f"result={del_res}",
    ))
    out.append((
        "deleted_restore_never_writes_raw_degrees",
        not ctx.data.get("del_wrote_raw_degrees"),
        f"del_new_xy={del_xy} = les coordonnees WGS84 brutes du journal. "
        f"Le WKB est reinjecte sans regarder event.crs_authid="
        f"{_EVENT_CRS!r} ni layer.crs()={ctx.data.get('layer_crs')!r}.",
    ))

    # ===== Cas C: temoin positif ========================================
    out.append((
        "same_crs_restore_still_works",
        ctrl_res.get("success") is True
        and isinstance(ctx.data.get("ctrl_dist"), float)
        and ctx.data["ctrl_dist"] < _POS_TOL_M,
        f"temoin: un evenement capture dans le CRS de la couche doit etre "
        f"restaure a l'identique. result={ctrl_res} xy="
        f"{ctx.data.get('ctrl_new_xy')} attendu={_L93_CONTROL_XY} "
        f"ecart={ctx.data.get('ctrl_dist')}",
    ))

    # ===== Tracabilite: l'incoherence doit au minimum se voir ===========
    out.append(assert_log_contains(
        ctx.records,
        r"(?i)crs[_ ]?mismatch|crs.*(differ|incoherent|reprojec)",
        name="crs_mismatch_is_logged",
        min_count=1,
    ))

    # ===== Garde-fou de source ==========================================
    src = (_PLUGIN_ROOT / "core" / "restore_service.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "restore_service_reads_event_crs_before_writing",
        "event.crs_authid" in src.split("def build_restore_trace_event")[0],
        "restore_service ne consulte jamais event.crs_authid avant "
        "d'ecrire une geometrie (la seule occurrence du champ est la recopie "
        "dans la trace de restauration). Le CRS de capture est stocke depuis "
        "l'origine mais n'est utilise que pour l'affichage.",
    ))

    exe = (_PLUGIN_ROOT / "core" / "restore_executor.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "rewind_executor_reads_event_crs",
        "crs_authid" in exe,
        "core/restore_executor.py (chemin REWIND) n'evoque jamais le CRS: "
        "un rewind sur une couche reprojetee depuis la capture repositionne "
        "toutes les features au mauvais endroit, en masse.",
    ))

    # ===== Propagation du trace_id ======================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_geom_crs_restore_mismatch.*trace_id={ctx.trace_id}",
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
