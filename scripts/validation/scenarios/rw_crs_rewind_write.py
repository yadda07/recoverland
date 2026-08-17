"""rw_crs_rewind_write - RecoverLand validation runtime.

Invariant: I-3 (l'ecriture doit viser la bonne entite, au bon endroit).

Pourquoi ce scenario existe en plus de tx_geom_crs_restore_mismatch
------------------------------------------------------------------
`tx_geom_crs_restore_mismatch` mesure le CHEMIN UNITAIRE
(`restore_service.restore_deleted_feature` / `restore_updated_feature`)
avec de vraies coordonnees, et se contente d'un GARDE DE SOURCE pour le
chemin REWIND:

    "crs_authid" in core/restore_executor.py

Cette chaine est satisfaite par un simple commentaire. Le chemin REWIND
(`restore_executor._apply_via_buffer` -> `_buffer_update` /
`_buffer_insert`) est pourtant celui qui ecrit EN MASSE: un rewind sur une
couche reprojetee depuis la capture deplace toutes les entites d'un coup.
Ce scenario mesure ce chemin-la sur des coordonnees persistees, pour que
la garantie tienne a autre chose qu'a une chaine de caracteres.

Le mecanisme
------------
Le journal stocke le WKB octet a octet, donc en coordonnees BRUTES de
`event.crs_authid` - le CRS de la couche AU MOMENT DE LA CAPTURE. Quand la
couche a ete reprojetee depuis (ou quand un CRS mal declare a ete
corrige), reinjecter ces octets tels quels teleporte l'entite: (2.35,
48.85) degres ecrits dans une couche EPSG:2154 atterrissent a ~6 900 km,
au large du golfe de Guinee, et le rewind annonce "Reverted"/"Inserted".

Le correctif attendu: `_buffer_update` et `_buffer_insert` passent la
geometrie par `geometry_utils.geometry_for_layer_crs` (le meme helper que
`restore_service._geometry_for_write`) avant de toucher le tampon
d'edition. Reprojection quand c'est possible, REFUS
(`reason_code=crs_mismatch`, rien d'ecrit) quand le CRS de capture est
inexploitable.

Plan (vrai GeoPackage EPSG:2154, vrai provider OGR, vrai tampon d'edition,
commit reel puis relecture par le provider)
    cas A : UPDATE attributaire capture en EPSG:4326. La geometrie doit
            atterrir sur la position reprojetee, jamais sur les degres.
    cas B : INSERT compensatoire (undo d'un DELETE) capture en EPSG:4326.
            Meme exigence sur l'entite recreee.
    cas C : temoin positif - INSERT compensatoire capture en EPSG:2154.
            Doit rester au micron: le garde-fou ne doit rien couter a une
            restauration legitime.
    cas D : refus - INSERT compensatoire dont le CRS de capture ne se
            resout pas. Aucune entite ne doit apparaitre, et le resultat
            doit le dire (reason_code=crs_mismatch).

Verdict attendu si le produit est sain: PASS.
"""
from __future__ import annotations

import json
import math
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_crs_rewind_write"
INVARIANT = "I-3"

# Expected log signature documented for grep-ability:
EXPECTED_SIGNATURE = r"BUF_CRS .*crs_mismatch .*action=(reprojected|transform_failed)"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LAYER_CRS = "EPSG:2154"
_EVENT_CRS = "EPSG:4326"
# Un code d'autorite qui ne se resout pas: QgsCoordinateReferenceSystem le
# declare invalide, donc aucune transformation n'est constructible.
_BROKEN_CRS = "EPSG:999999"

# Paris, exprime dans les deux CRS.
_WGS84_XY = (2.35, 48.85)
_L93_XY = (652000.0, 6862000.0)
# Temoin positif: une position Lambert-93 voisine, capturee dans le bon CRS.
_L93_CONTROL_XY = (652500.0, 6862500.0)

_FIELDS = [{"name": "nom", "type": "String", "length": 32, "precision": 0}]
_LIVE_NAME = "PYLONE"
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
        f"Point?crs={_LAYER_CRS}&field=nom:string(32)", "seed", "memory")
    mem.startEditing()
    feat = QgsFeature(mem.fields())
    feat.setAttributes([_LIVE_NAME])
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


def _event(operation, geom_wkb, crs, fid, attrs_json, event_id):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=event_id,
        project_fingerprint="rw_crs_project",
        datasource_fingerprint="rw_crs_datasource",
        layer_id_snapshot="lyr_rw_crs",
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
        created_at=datetime(2026, 8, 17, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _persisted_xy(layer, fid):
    """Coordonnees relues depuis le provider, apres commit."""
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


def _fids(layer):
    return {feat.id() for feat in layer.dataProvider().getFeatures()}


def _dist(a, b):
    if a is None or b is None:
        return float("inf")
    return math.hypot(a[0] - b[0], a[1] - b[1])


def _apply(ctx, comp_op, event):
    """Passe *event* par le chemin REWIND, commit, relit le provider."""
    from recoverland.core.restore_executor import _apply_via_buffer

    layer = ctx.data["layer"]
    layer.startEditing()
    layer.beginEditCommand("rw_crs_rewind_write")
    result = _apply_via_buffer(layer, comp_op, event, {},
                               trace_id=ctx.trace_id)
    layer.endEditCommand()
    committed = layer.commitChanges()
    if not committed:
        layer.rollBack()
    layer.reload()
    return {
        "success": bool(result.get("success")),
        "skipped": bool(result.get("skipped")),
        "status": result.get("status"),
        "reason_code": result.get("reason_code"),
        "message": str(result.get("message", ""))[:140],
        "committed": bool(committed),
    }


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject,
        QgsPointXY,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_rw_crs_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_gpkg(tmpdir)
    ctx.data["layer"] = layer
    ctx.data["layer_crs"] = layer.crs().authid()

    live = next(layer.getFeatures(), None)
    ctx.data["live_fid"] = live.id() if live is not None else None
    ctx.data["live_xy"] = _persisted_xy(layer, ctx.data["live_fid"])

    # Position VRAIE de (2.35, 48.85) une fois reprojetee en Lambert-93:
    # c'est la ou une ecriture correcte doit deposer la geometrie.
    xform = QgsCoordinateTransform(
        QgsCoordinateReferenceSystem(_EVENT_CRS),
        QgsCoordinateReferenceSystem(_LAYER_CRS),
        QgsProject.instance(),
    )
    target = xform.transform(QgsPointXY(*_WGS84_XY))
    ctx.data["expected_l93"] = (round(target.x(), 4), round(target.y(), 4))
    ctx.data["broken_crs_is_invalid"] = not QgsCoordinateReferenceSystem(
        _BROKEN_CRS).isValid()

    flog(
        f"rw_crs_rewind_write setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} layer_crs={ctx.data['layer_crs']} "
        f"live_fid={ctx.data['live_fid']} live_xy={ctx.data['live_xy']} "
        f"expected_l93={ctx.data['expected_l93']} "
        f"broken_crs_invalid={ctx.data['broken_crs_is_invalid']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    layer = ctx.data["layer"]
    fid = ctx.data["live_fid"]
    expected = ctx.data["expected_l93"]

    flog(f"rw_crs_rewind_write run start: trace_id={ctx.trace_id}", "INFO")

    # --- cas A: UPDATE attributaire capture en EPSG:4326 -----------------
    # edit_tracker ne remplit new_geometry_wkb que si la geometrie a change;
    # une edition d'attribut pur laisse geometry_wkb rempli (etat OLD) et
    # declenche quand meme une reecriture de geometrie cote rewind.
    upd = _event(
        "UPDATE", _wkb_point(*_WGS84_XY), _EVENT_CRS, fid,
        json.dumps({"changed_only": {"nom": {"old": _OLD_NAME,
                                             "new": _LIVE_NAME}}}),
        event_id=201,
    )
    ctx.data["upd_result"] = _apply(ctx, "UPDATE", upd)
    ctx.data["upd_xy"] = _persisted_xy(layer, fid)
    ctx.data["upd_dist_expected"] = _dist(ctx.data["upd_xy"], expected)
    ctx.data["upd_dist_raw_degrees"] = _dist(ctx.data["upd_xy"], _WGS84_XY)

    # --- cas B: INSERT compensatoire capture en EPSG:4326 ----------------
    before_b = _fids(layer)
    ins = _event(
        "DELETE", _wkb_point(*_WGS84_XY), _EVENT_CRS, 9999,
        json.dumps({"all_attributes": {"nom": "POSTE_4326"}}),
        event_id=202,
    )
    ctx.data["ins_result"] = _apply(ctx, "INSERT", ins)
    new_b = sorted(_fids(layer) - before_b)
    ctx.data["ins_new_fids"] = new_b
    ctx.data["ins_xy"] = _persisted_xy(layer, new_b[0]) if new_b else None
    ctx.data["ins_dist_expected"] = _dist(ctx.data["ins_xy"], expected)
    ctx.data["ins_dist_raw_degrees"] = _dist(ctx.data["ins_xy"], _WGS84_XY)

    # --- cas C: temoin positif, capture dans le CRS de la couche ---------
    before_c = _fids(layer)
    ctrl = _event(
        "DELETE", _wkb_point(*_L93_CONTROL_XY), _LAYER_CRS, 9998,
        json.dumps({"all_attributes": {"nom": "POSTE_2154"}}),
        event_id=203,
    )
    ctx.data["ctrl_result"] = _apply(ctx, "INSERT", ctrl)
    new_c = sorted(_fids(layer) - before_c)
    ctx.data["ctrl_xy"] = _persisted_xy(layer, new_c[0]) if new_c else None
    ctx.data["ctrl_dist"] = _dist(ctx.data["ctrl_xy"], _L93_CONTROL_XY)

    # --- cas D: refus, CRS de capture inexploitable ----------------------
    before_d = _fids(layer)
    broken = _event(
        "DELETE", _wkb_point(*_WGS84_XY), _BROKEN_CRS, 9997,
        json.dumps({"all_attributes": {"nom": "POSTE_BROKEN"}}),
        event_id=204,
    )
    ctx.data["broken_result"] = _apply(ctx, "INSERT", broken)
    ctx.data["broken_new_fids"] = sorted(_fids(layer) - before_d)

    flog(
        f"rw_crs_rewind_write run end: trace_id={ctx.trace_id} "
        f"upd={ctx.data['upd_result']} upd_xy={ctx.data['upd_xy']} "
        f"ins={ctx.data['ins_result']} ins_xy={ctx.data['ins_xy']} "
        f"ctrl={ctx.data['ctrl_result']} ctrl_xy={ctx.data['ctrl_xy']} "
        f"broken={ctx.data['broken_result']} "
        f"broken_new_fids={ctx.data['broken_new_fids']}",
        "INFO",
    )

    ctx.data["layer"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    live_xy = ctx.data.get("live_xy")
    expected = ctx.data.get("expected_l93")
    upd = ctx.data.get("upd_result") or {}
    ins = ctx.data.get("ins_result") or {}
    ctrl = ctx.data.get("ctrl_result") or {}
    broken = ctx.data.get("broken_result") or {}

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
    out.append((
        "fixture_broken_crs_really_unresolvable",
        ctx.data.get("broken_crs_is_invalid") is True,
        f"{_BROKEN_CRS} doit etre invalide pour QGIS, sinon le cas D ne "
        f"teste aucun refus",
    ))

    # ===== Cas A: UPDATE attributaire, evenement en 4326 =================
    d_upd = ctx.data.get("upd_dist_expected")
    out.append((
        "rewind_update_lands_on_the_reprojected_position",
        upd.get("success") is False
        or (isinstance(d_upd, float) and d_upd < _POS_TOL_M),
        f"la feature est passee de {live_xy} a {ctx.data.get('upd_xy')} "
        f"(couche {ctx.data.get('layer_crs')}) alors que l'evenement etait "
        f"capture en {_EVENT_CRS}. Ecart avec la position correcte "
        f"({expected}): {d_upd} unites de CRS, soit "
        f"~{(d_upd or 0) / 1000:.0f} km. result={upd}",
    ))
    out.append((
        "rewind_update_never_writes_raw_degrees",
        not (isinstance(ctx.data.get("upd_dist_raw_degrees"), float)
             and ctx.data["upd_dist_raw_degrees"] < 1e-3),
        f"upd_xy={ctx.data.get('upd_xy')} = les degres du journal ecrits "
        f"tels quels dans une couche en metres, par le chemin REWIND.",
    ))

    # ===== Cas B: INSERT compensatoire, evenement en 4326 ================
    d_ins = ctx.data.get("ins_dist_expected")
    out.append((
        "rewind_insert_lands_on_the_reprojected_position",
        ins.get("success") is False
        or (isinstance(d_ins, float) and d_ins < _POS_TOL_M),
        f"l'entite recreee par le rewind est a {ctx.data.get('ins_xy')}, la "
        f"bonne position etant {expected}: erreur de {d_ins} unites de CRS "
        f"(~{(d_ins or 0) / 1000:.0f} km), rapportee "
        f"{ins.get('message')!r}. result={ins}",
    ))
    out.append((
        "rewind_insert_never_writes_raw_degrees",
        not (isinstance(ctx.data.get("ins_dist_raw_degrees"), float)
             and ctx.data["ins_dist_raw_degrees"] < 1e-3),
        f"ins_xy={ctx.data.get('ins_xy')} = les coordonnees WGS84 brutes du "
        f"journal. Le WKB est reinjecte sans regarder event.crs_authid.",
    ))

    # ===== Cas C: temoin positif ========================================
    out.append((
        "same_crs_rewind_is_untouched",
        ctrl.get("success") is True
        and isinstance(ctx.data.get("ctrl_dist"), float)
        and ctx.data["ctrl_dist"] < 1e-6,
        f"temoin: une capture faite dans le CRS de la couche doit etre "
        f"reecrite a l'identique, sans le moindre deplacement. result={ctrl} "
        f"xy={ctx.data.get('ctrl_xy')} attendu={_L93_CONTROL_XY} "
        f"ecart={ctx.data.get('ctrl_dist')}",
    ))

    # ===== Cas D: refus plutot que mauvaise position =====================
    out.append((
        "unresolvable_crs_is_refused",
        broken.get("success") is False
        and broken.get("reason_code") == "crs_mismatch",
        f"un CRS de capture inexploitable doit produire un echec explicite "
        f"(reason_code=crs_mismatch), pas une ecriture au hasard. "
        f"result={broken}",
    ))
    out.append((
        "unresolvable_crs_writes_nothing",
        ctx.data.get("broken_new_fids") == [],
        f"aucune entite ne doit apparaitre apres un refus CRS: "
        f"new_fids={ctx.data.get('broken_new_fids')}",
    ))

    # ===== Tracabilite ==================================================
    out.append(assert_log_contains(
        ctx.records,
        r"BUF_CRS .*crs_mismatch",
        name="crs_mismatch_is_logged_by_the_rewind",
        min_count=1,
    ))

    # ===== Garde de source: le helper partage, pas une copie ============
    exe = (_PLUGIN_ROOT / "core" / "restore_executor.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "executor_uses_the_shared_crs_helper",
        "geometry_for_layer_crs" in exe,
        "restore_executor doit passer par "
        "geometry_utils.geometry_for_layer_crs, le meme helper que "
        "restore_service._geometry_for_write: deux definitions de "
        "\"la geometrie est-elle a sa place\" derivent tot ou tard",
    ))

    # ===== Propagation du trace_id ======================================
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_crs_rewind_write.*trace_id={ctx.trace_id}",
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
