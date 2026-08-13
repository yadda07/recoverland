"""tx_geom_corrupt_and_typemismatch - RecoverLand validation runtime.

Invariant: I-8 (ne jamais ecrire une geometrie qu'on n'a pas su relire).

Le mecanisme
------------
`geometry_utils.rebuild_geometry` ne signale JAMAIS un echec de decodage:

    def rebuild_geometry(wkb_data):
        if wkb_data is None or len(wkb_data) == 0:
            return None
        geom = QgsGeometry()
        geom.fromWkb(wkb_data)      # renvoie False si le WKB est illisible
        return geom                  # ... et on rend l'objet quand meme

Le booleen de `fromWkb` est ignore. Sur un WKB tronque ou corrompu, l'appel
echoue et l'objet rendu est une QgsGeometry NULLE - qui n'est pas `None`.
Les deux chemins de restauration evenement-par-evenement testent
exactement `is not None`:

    restore_service.restore_updated_feature
        geom = rebuild_geometry(event.geometry_wkb)
        if geom is not None:
            provider.changeGeometryValues({target_fid: geom})

    restore_service.restore_deleted_feature
        geom = rebuild_geometry(event.geometry_wkb)
        if geom is not None:
            new_feature.setGeometry(geom)

Le chemin REWIND, lui, est protege: `restore_executor._buffer_update`
teste `geom.isEmpty() or geom.isNull()` et rend
`reason_code=rebuilt_empty`. La protection existe donc dans le produit,
mais seulement d'un cote.

Le scenario d'echec concret
---------------------------
Un journal SQLite peut rendre un WKB tronque: coupure pendant l'ecriture de
la file (write_queue), disque plein (disk_monitor existe justement pour ca),
copie partielle du .db, bit rot. L'utilisateur ouvre RecoverLand pour
reparer une erreur de saisie et restaure l'evenement. Selon le chemin:

  - UPDATE : la geometrie NULLE est ecrite sur la feature VIVANTE. La
    feature perd sa geometrie, disparait du canevas, et le plugin rapporte
    "Reverted". L'outil de recuperation vient de detruire la donnee qu'il
    devait sauver.
  - DELETE : la feature est recreee SANS geometrie, avec ses attributs.
    L'utilisateur voit "Restored", la table contient bien une ligne de
    plus, mais rien ne s'affiche.

Le scenario teste aussi deux limites voisines:
  - blob de longueur zero (rebuild_geometry rend None): l'ecriture de
    geometrie est silencieusement sautee et la restauration est quand meme
    rapportee comme reussie;
  - incompatibilite de type: restaurer un POINT dans une couche declaree
    POLYGON;
  - couche NoGeometry: les chemins geometriques doivent rester inertes.

Plan du scenario (vrais GeoPackage en EPSG:2154, vrai provider OGR):
    couche A "poteaux" (Point, DEUX features)  : WKB tronque sur la
        premiere, blob de longueur zero sur la seconde - une feature par
        cas, sinon le premier cas detruit la geometrie que le second doit
        trouver intacte et sa preuve de cible echoue pour la mauvaise
        raison.
    couche B "parcelles" (Polygon): cas type incompatible.
    couche C memoire NoGeometry   : cas inertie.

Verdict attendu si le produit est sain: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_geom_corrupt_and_typemismatch"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_CRS = "EPSG:2154"
_PT_XY = (652000.0, 6862000.0)
_PT2_XY = (653000.0, 6863000.0)
_POLY_WKT = ("POLYGON((652000 6862000, 652100 6862000, "
             "652100 6862100, 652000 6862100, 652000 6862000))")
_FIELDS = [
    {"name": "nom", "type": "String", "length": 32, "precision": 0},
    {"name": "code", "type": "String", "length": 32, "precision": 0},
]
_LIVE_NAME = "POTEAU"
_LIVE_CODE = "A001"
_OLD_NAME = "POTEAU_AVANT"


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _wkb_from_wkt(wkt: str) -> bytes:
    from qgis.core import QgsGeometry
    return bytes(QgsGeometry.fromWkt(wkt).asWkb())


def _make_gpkg(tmpdir, name, geom_spec, feature_wkts):
    """Ecrit un GeoPackage et renvoie la couche chargee.

    *feature_wkts* est une liste: chaque cas de torture travaille sur SA
    propre feature, sinon le premier cas mutile la geometrie que le
    suivant est cense trouver intacte et le second test ne prouve plus
    rien (piege rencontre a la premiere execution).
    """
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        f"{geom_spec}?crs={_CRS}&field=nom:string(32)&field=code:string(32)",
        "seed", "memory",
    )
    mem.startEditing()
    for i, wkt in enumerate(feature_wkts):
        feat = QgsFeature(mem.fields())
        feat.setAttributes([_LIVE_NAME, f"{_LIVE_CODE}{i}"])
        feat.setGeometry(QgsGeometry.fromWkt(wkt))
        mem.addFeature(feat)
    mem.commitChanges()

    path = str(Path(tmpdir) / f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = name
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)

    layer = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path}")
    return layer


def _event(operation, geom_wkb, fid, attrs_json, new_geom_wkb=None,
           event_id=1, geometry_type="Point"):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=event_id,
        project_fingerprint="tx_cor_project",
        datasource_fingerprint="tx_cor_datasource",
        layer_id_snapshot="lyr_tx_cor",
        layer_name_snapshot="poteaux",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type=operation,
        attributes_json=attrs_json,
        geometry_wkb=geom_wkb,
        geometry_type=geometry_type,
        crs_authid=_CRS,
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


def _geom_state(layer, fid):
    """(present, wkt_court) de la geometrie persistee, ou (False, 'absent')."""
    from qgis.core import QgsFeatureRequest
    feat = next(layer.dataProvider().getFeatures(QgsFeatureRequest(int(fid))),
                None)
    if feat is None:
        return (False, "absent")
    geom = feat.geometry()
    if geom is None or geom.isNull() or geom.isEmpty():
        return (False, "null_or_empty")
    return (True, geom.asWkt(3)[:80])


def _inventory(layer):
    out = {}
    for feat in layer.dataProvider().getFeatures():
        geom = feat.geometry()
        present = not (geom is None or geom.isNull() or geom.isEmpty())
        out[feat.id()] = (present, geom.asWkt(3)[:60] if present else "-")
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsVectorLayer, QgsFeature

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_cor_")
    ctx.data["tmpdir"] = tmpdir

    layer_pt = _make_gpkg(tmpdir, "poteaux", "Point",
                          [f"POINT({_PT_XY[0]} {_PT_XY[1]})",
                           f"POINT({_PT2_XY[0]} {_PT2_XY[1]})"])
    layer_poly = _make_gpkg(tmpdir, "parcelles", "Polygon", [_POLY_WKT])

    nogeom = QgsVectorLayer(
        "None?field=nom:string(32)&field=code:string(32)", "annexes", "memory")
    nogeom.startEditing()
    f = QgsFeature(nogeom.fields())
    f.setAttributes([_LIVE_NAME, _LIVE_CODE])
    nogeom.addFeature(f)
    nogeom.commitChanges()

    ctx.data["layer_pt"] = layer_pt
    ctx.data["layer_poly"] = layer_poly
    ctx.data["layer_nogeom"] = nogeom

    pt_fids = sorted(f.id() for f in layer_pt.getFeatures())
    poly_feat = next(layer_poly.getFeatures(), None)
    ng_feat = next(nogeom.getFeatures(), None)
    ctx.data["pt_fid"] = pt_fids[0] if pt_fids else None
    ctx.data["pt_fid2"] = pt_fids[1] if len(pt_fids) > 1 else None
    ctx.data["poly_fid"] = poly_feat.id() if poly_feat else None
    ctx.data["ng_fid"] = ng_feat.id() if ng_feat else None
    ctx.data["pt_before"] = _geom_state(layer_pt, ctx.data["pt_fid"])
    ctx.data["pt2_before"] = _geom_state(layer_pt, ctx.data["pt_fid2"])
    ctx.data["poly_before"] = _geom_state(layer_poly, ctx.data["poly_fid"])

    from recoverland.core.geometry_utils import (
        extract_geometry_type, extract_crs_authid,
    )
    ctx.data["nogeom_type"] = extract_geometry_type(nogeom)
    ctx.data["nogeom_crs"] = extract_crs_authid(nogeom)

    flog(
        f"tx_geom_corrupt_and_typemismatch setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} pt_fid={ctx.data['pt_fid']} "
        f"pt_before={ctx.data['pt_before']} poly_fid={ctx.data['poly_fid']} "
        f"nogeom_type={ctx.data['nogeom_type']} "
        f"nogeom_crs={ctx.data['nogeom_crs']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.restore_service import (
        restore_deleted_feature, restore_updated_feature,
    )
    from recoverland.core.geometry_utils import rebuild_geometry
    from recoverland.core.logger import flog

    layer_pt = ctx.data["layer_pt"]
    layer_poly = ctx.data["layer_poly"]
    nogeom = ctx.data["layer_nogeom"]
    pt_fid = ctx.data["pt_fid"]
    live_wkb = _wkb_point(*_PT_XY)
    truncated = live_wkb[:11]      # en-tete + un demi-double
    garbage = b"\x01\x2a\x00\x00\x00" + b"\xde\xad\xbe\xef" * 3

    flog(f"tx_geom_corrupt_and_typemismatch run start: "
         f"trace_id={ctx.trace_id}", "INFO")

    # --- sonde directe: que rend rebuild_geometry sur du WKB casse ? -----
    probe = {}
    for name, blob in (("truncated", truncated), ("garbage", garbage),
                       ("zero_length", b""), ("valid", live_wkb)):
        geom = rebuild_geometry(blob)
        probe[name] = {
            "is_none": geom is None,
            "is_null": None if geom is None else bool(geom.isNull()),
            "is_empty": None if geom is None else bool(geom.isEmpty()),
        }
    ctx.data["rebuild_probe"] = probe

    # --- cas 1: UPDATE dont le WKB OLD est tronque -----------------------
    attrs = json.dumps({"changed_only": {"nom": {"old": _OLD_NAME,
                                                 "new": _LIVE_NAME}}})
    ev = _event("UPDATE", truncated, pt_fid, attrs,
                new_geom_wkb=live_wkb, event_id=201)
    ctx.data["trunc_result"] = restore_updated_feature(layer_pt, ev)
    layer_pt.reload()
    ctx.data["pt_after_trunc"] = _geom_state(layer_pt, pt_fid)

    # --- cas 2: UPDATE dont le WKB OLD est un blob de longueur zero ------
    # Sur la SECONDE feature, restee intacte: sinon la preuve de cible
    # echoue a cause des degats du cas 1 et le cas 2 ne teste plus rien.
    pt_fid2 = ctx.data["pt_fid2"]
    ev = _event("UPDATE", b"", pt_fid2, attrs,
                new_geom_wkb=_wkb_point(*_PT2_XY), event_id=202)
    ctx.data["zero_result"] = restore_updated_feature(layer_pt, ev)
    layer_pt.reload()
    ctx.data["pt_after_zero"] = _geom_state(layer_pt, pt_fid2)

    # --- cas 3: DELETE dont le WKB est tronque -> re-insertion -----------
    before = set(_inventory(layer_pt).keys())
    ev = _event("DELETE", truncated, 9999,
                json.dumps({"all_attributes": {"nom": "FANTOME",
                                               "code": "Z999"}}),
                event_id=203)
    ctx.data["del_trunc_result"] = restore_deleted_feature(layer_pt, ev)
    layer_pt.reload()
    inv = _inventory(layer_pt)
    new_fids = sorted(set(inv.keys()) - before)
    ctx.data["del_trunc_new"] = [inv[f] for f in new_fids]

    # --- cas 4: type incompatible - un POINT dans une couche POLYGON -----
    before_poly = set(_inventory(layer_poly).keys())
    ev = _event("DELETE", _wkb_point(*_PT_XY), 9999,
                json.dumps({"all_attributes": {"nom": "INTRUS",
                                               "code": "Y888"}}),
                event_id=204)
    ctx.data["poly_ins_result"] = restore_deleted_feature(layer_poly, ev)
    layer_poly.reload()
    inv_poly = _inventory(layer_poly)
    new_poly = sorted(set(inv_poly.keys()) - before_poly)
    ctx.data["poly_ins_new"] = [inv_poly[f] for f in new_poly]

    # ... et le meme point ecrase-t-il la geometrie d'une parcelle ?
    poly_fid = ctx.data["poly_fid"]
    ev = _event("UPDATE", _wkb_point(*_PT_XY), poly_fid, attrs,
                new_geom_wkb=_wkb_from_wkt(_POLY_WKT), event_id=205)
    ctx.data["poly_upd_result"] = restore_updated_feature(layer_poly, ev)
    layer_poly.reload()
    ctx.data["poly_after_upd"] = _geom_state(layer_poly, poly_fid)

    # --- cas 5: couche NoGeometry, evenement porteur d'une geometrie -----
    ng_before = len(list(nogeom.dataProvider().getFeatures()))
    ev = _event("DELETE", _wkb_point(*_PT_XY), 9999,
                json.dumps({"all_attributes": {"nom": "SANS_GEOM",
                                               "code": "N001"}}),
                event_id=206, geometry_type="NoGeometry")
    try:
        ctx.data["ng_result"] = restore_deleted_feature(nogeom, ev)
        ctx.data["ng_error"] = None
    except Exception as exc:  # noqa: BLE001
        ctx.data["ng_result"] = None
        ctx.data["ng_error"] = repr(exc)
    ng_after = list(nogeom.dataProvider().getFeatures())
    ctx.data["ng_added"] = len(ng_after) - ng_before
    ctx.data["ng_names"] = sorted(str(f["nom"]) for f in ng_after)

    flog(
        f"tx_geom_corrupt_and_typemismatch run end: trace_id={ctx.trace_id} "
        f"probe={probe} trunc={ctx.data['trunc_result']} "
        f"pt_after_trunc={ctx.data['pt_after_trunc']} "
        f"zero={ctx.data['zero_result']} "
        f"del_trunc={ctx.data['del_trunc_result']} "
        f"new={ctx.data['del_trunc_new']} "
        f"poly_ins={ctx.data['poly_ins_result']} "
        f"poly_new={ctx.data['poly_ins_new']} "
        f"poly_upd={ctx.data['poly_upd_result']} "
        f"poly_after={ctx.data['poly_after_upd']} "
        f"ng={ctx.data['ng_result']} ng_added={ctx.data['ng_added']}",
        "INFO",
    )

    ctx.data["layer_pt"] = None
    ctx.data["layer_poly"] = None
    ctx.data["layer_nogeom"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    probe = ctx.data.get("rebuild_probe") or {}
    trunc_res = ctx.data.get("trunc_result") or {}
    zero_res = ctx.data.get("zero_result") or {}
    del_res = ctx.data.get("del_trunc_result") or {}
    poly_ins = ctx.data.get("poly_ins_result") or {}
    poly_upd = ctx.data.get("poly_upd_result") or {}

    # ===== Sanity =======================================================
    out.append((
        "fixture_seeded",
        ctx.data.get("pt_before", (False,))[0] is True
        and ctx.data.get("pt2_before", (False,))[0] is True
        and ctx.data.get("poly_before", (False,))[0] is True,
        f"pt_before={ctx.data.get('pt_before')} "
        f"pt2_before={ctx.data.get('pt2_before')} "
        f"poly_before={ctx.data.get('poly_before')}",
    ))
    out.append((
        "corrupt_wkb_is_really_undecodable",
        probe.get("truncated", {}).get("is_null") is not False
        or probe.get("garbage", {}).get("is_null") is not False,
        f"probe={probe}: le scenario a besoin d'un WKB que QGIS refuse de "
        f"decoder, sinon il ne prouve rien",
    ))

    # ===== Le trou: rebuild_geometry ne signale pas l'echec ==============
    out.append((
        "rebuild_geometry_signals_a_decode_failure",
        probe.get("truncated", {}).get("is_none") is True
        or probe.get("truncated", {}).get("is_null") is False,
        f"rebuild_geometry(WKB tronque) -> {probe.get('truncated')}: le "
        f"booleen rendu par QgsGeometry.fromWkb est ignore, la fonction rend "
        f"une geometrie NULLE qui n'est pas None. Tout appelant qui teste "
        f"`is not None` croit tenir une geometrie valide.",
    ))

    # ===== UPDATE: la feature vivante ne doit pas perdre sa geometrie ====
    after_trunc = ctx.data.get("pt_after_trunc") or (None, "?")
    out.append((
        "update_with_corrupt_wkb_does_not_destroy_the_live_geometry",
        after_trunc[0] is True,
        f"apres restauration d'un evenement au WKB tronque, la feature "
        f"vivante est passee de {ctx.data.get('pt_before')} a {after_trunc}: "
        f"elle a perdu sa geometrie et disparait du canevas. L'outil de "
        f"recuperation a detruit la donnee qu'il devait sauver. "
        f"result={trunc_res}",
    ))
    out.append((
        "update_with_corrupt_wkb_is_reported_as_a_failure",
        trunc_res.get("success") is False,
        f"result={trunc_res}: un WKB illisible doit produire un echec "
        f"explicite (le chemin REWIND rend deja reason_code=rebuilt_empty). "
        f"Rapporter 'Reverted' fait croire a l'utilisateur que sa donnee est "
        f"revenue.",
    ))

    # ===== UPDATE: blob de longueur zero ================================
    after_zero = ctx.data.get("pt_after_zero") or (None, "?")
    out.append((
        "zero_length_wkb_is_not_silently_ignored",
        zero_res.get("success") is False
        or "geom" in str(zero_res.get("message", "")).lower(),
        f"result={zero_res} etat_geom={after_zero}: l'evenement annonce une "
        f"geometrie (geometry_wkb non NULL) mais le blob est vide; "
        f"rebuild_geometry rend None, l'ecriture de geometrie est sautee "
        f"sans un mot, et la restauration est rapportee reussie. "
        f"L'utilisateur croit son etat precedent entierement retabli alors "
        f"que seuls les attributs sont revenus.",
    ))

    # ===== DELETE: pas de fantome sans geometrie ========================
    new_geoms = ctx.data.get("del_trunc_new") or []
    ghost = [g for g in new_geoms if g[0] is False]
    out.append((
        "delete_restore_with_corrupt_wkb_creates_no_ghost_feature",
        not ghost and (del_res.get("success") is not True or not new_geoms),
        f"la restauration a insere {len(new_geoms)} feature(s) dont "
        f"{len(ghost)} sans geometrie ({new_geoms}), en rapportant "
        f"{del_res.get('message')!r}. Une ligne apparait dans la table, rien "
        f"n'apparait sur la carte, et l'utilisateur doit la retrouver a la "
        f"main pour la supprimer. result={del_res}",
    ))

    # ===== Type incompatible ============================================
    poly_new = ctx.data.get("poly_ins_new") or []
    intrus = [g for g in poly_new if g[0] and g[1].lower().startswith("point")]
    out.append((
        "point_is_not_inserted_into_a_polygon_layer",
        poly_ins.get("success") is False or not intrus,
        f"un POINT a ete insere dans une couche declaree Polygon "
        f"({poly_new}), result={poly_ins}. Le type de la couche n'est jamais "
        f"compare a event.geometry_type; selon le pilote, la couche devient "
        f"heterogene ou l'ecriture echoue avec un message brut d'OGR.",
    ))
    after_poly = ctx.data.get("poly_after_upd") or (None, "?")
    out.append((
        "polygon_feature_is_not_overwritten_by_a_point",
        poly_upd.get("success") is False
        or not str(after_poly[1]).lower().startswith("point"),
        f"la parcelle est passee de {ctx.data.get('poly_before')} a "
        f"{after_poly}: une geometrie de type incompatible a remplace la "
        f"surface, result={poly_upd}",
    ))

    # ===== Couche NoGeometry: inertie ===================================
    out.append((
        "nogeometry_layer_is_detected",
        ctx.data.get("nogeom_type") == "NoGeometry"
        and ctx.data.get("nogeom_crs") is None,
        f"extract_geometry_type={ctx.data.get('nogeom_type')!r} "
        f"extract_crs_authid={ctx.data.get('nogeom_crs')!r} attendu "
        f"('NoGeometry', None)",
    ))
    out.append((
        "nogeometry_restore_does_not_raise",
        ctx.data.get("ng_error") is None,
        f"restore_deleted_feature a leve {ctx.data.get('ng_error')} sur une "
        f"couche non spatiale: les chemins geometriques doivent rester "
        f"inertes, pas exploser",
    ))
    out.append((
        "nogeometry_restore_restores_the_row",
        ctx.data.get("ng_added") == 1
        and "SANS_GEOM" in (ctx.data.get("ng_names") or []),
        f"ng_added={ctx.data.get('ng_added')} noms={ctx.data.get('ng_names')}: "
        f"sur une table attributaire, la restauration d'un DELETE doit "
        f"recreer la ligne",
    ))

    # ===== Le produit sait deja refuser: cote REWIND =====================
    exe = (_PLUGIN_ROOT / "core" / "restore_executor.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "rewind_path_still_guards_rebuilt_empty",
        "rebuilt_empty" in exe and "corrupt_wkb" in exe,
        "le garde-fou du chemin REWIND (reason_code=rebuilt_empty / "
        "corrupt_wkb) a disparu de restore_executor: plus rien ne protege la "
        "restauration en masse",
    ))
    svc = (_PLUGIN_ROOT / "core" / "restore_service.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "event_by_event_path_has_the_same_guard",
        "rebuilt_empty" in svc or "corrupt_wkb" in svc,
        "restore_service (restauration evenement par evenement) n'a aucun "
        "equivalent de rebuilt_empty/corrupt_wkb: la meme donnee corrompue "
        "est refusee par le rewind et acceptee par la restauration unitaire",
    ))

    # ===== Propagation du trace_id ======================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_geom_corrupt_and_typemismatch.*trace_id={ctx.trace_id}",
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
