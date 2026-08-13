"""tx_geom_makevalid_shape_swap - RecoverLand validation runtime.

Invariant: I-8 (le rewind rend la geometrie capturee, ou ne rend rien).

Le mecanisme
------------
`restore_executor._buffer_update` reconstruit la geometrie OLD de
l'evenement puis, si elle est GEOS-invalide, la repare:

    if not geom.isGeosValid():
        repaired = geom.makeValid()
        _, _, drift_units = _compute_makevalid_drift(geom, repaired)
        if drift_units > MAKEVALID_DRIFT_TOLERANCE:   # 1e-6
            return status=SKIPPED_GEOMETRY_DRIFT
        geom = repaired                                # applique

Le garde-fou existe, mais sa mesure est une distance de Chebyshev entre
les deux BOITES ENGLOBANTES (`geometry_utils._compute_makevalid_drift`).
Sa propre docstring le dit: "drift=0 does not imply identical WKB".

Or `makeValid()` est precisement une operation qui reorganise l'interieur
d'une forme en gardant ses sommets extremes:
  - un noeud papillon devient deux triangles disjoints (aire 0 -> 50);
  - deux parties qui se recouvrent sont fusionnees (l'aire du recouvrement
    est perdue);
  - un arc circulaire est LINEARISE (la courbe devient une polyligne).
Dans les trois cas, les sommets extremes sont conserves, donc la boite
englobante est identique au dixieme de micron pres, donc `drift_units`
vaut exactement 0, donc le garde-fou laisse passer.

Le scenario d'echec concret
---------------------------
Un plan de charge est saisi sous forme de multipolygone dont deux parties
se chevauchent (cas courant sur des parcelles numerisees a la main). Le
plugin capture cette geometrie telle quelle - c'est son travail: rendre
EXACTEMENT ce qui existait. L'utilisateur fait un REWIND. Le rewind lui
rend une geometrie fusionnee: 25 unites d'aire ont disparu, la table des
surfaces ne correspond plus, et le journal indique "APPLIED" avec
`drift_units=0.000000000`. Rien, ni dans l'interface ni dans le log, ne
signale que la forme rendue n'est pas la forme capturee.

Regle appliquee par ce scenario, pour chaque geometrie invalide:
le rewind doit SOIT appliquer une geometrie de meme type et de meme
mesure que celle capturee, SOIT refuser avec SKIPPED_GEOMETRY_DRIFT.
Appliquer silencieusement une autre forme est le defaut.

Plan du scenario (vrai GeoPackage EPSG:2154, vrai buffer d'edition QGIS):
    setup : une couche "zones" avec une feature carree par cas, pour que
            chaque cas travaille sur sa propre cible.
    run   : pour chaque cas, un evenement UPDATE dont la geometrie OLD est
            la geometrie tordue et dont la geometrie NEW est la geometrie
            vivante (preuve de cible), passe dans `_buffer_update` sur une
            couche en edition, puis commit et relecture cote provider.

Verdict attendu si le produit est sain: PASS.
"""
from __future__ import annotations

import json
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_geom_makevalid_shape_swap"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_CRS = "EPSG:2154"
_FIELDS = [
    {"name": "nom", "type": "String", "length": 32, "precision": 0},
    {"name": "code", "type": "String", "length": 32, "precision": 0},
]
_LIVE_NAME = "ZONE"
_OLD_NAME = "ZONE_AVANT"

# (cle, WKT de la geometrie OLD capturee, doit-elle etre GEOS-invalide ?)
_CASES = [
    # Noeud papillon: deux triangles qui se croisent. makeValid rend un
    # multipolygone de deux triangles: meme boite, aire 0 -> 50.
    ("bowtie", "POLYGON((0 0, 10 10, 10 0, 0 10, 0 0))", True),
    # Deux carres qui se recouvrent sur 5x5. makeValid fusionne: meme
    # boite (0 0 15 15), aire 200 -> 175.
    ("overlap",
     "MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),"
     "((5 5,15 5,15 15,5 15,5 5)))", True),
    # Trou entierement hors de l'enveloppe exterieure.
    ("hole_outside",
     "POLYGON((0 0,10 0,10 10,0 10,0 0),(20 20,21 20,21 21,20 21,20 20))",
     True),
    # Temoin: geometrie parfaitement valide, le garde-fou ne doit pas s'en
    # meler et la restauration doit passer intacte.
    ("valid_control", "POLYGON((0 0,8 0,8 8,0 8,0 0))", False),
]

# Emplacement vivant de chaque feature cible (carres disjoints).
_LIVE_SQUARES = {
    key: (
        "POLYGON(({x0} {y0},{x1} {y0},{x1} {y1},{x0} {y1},{x0} {y0}))"
    ).format(x0=1000 * i, y0=0, x1=1000 * i + 100, y1=100)
    for i, (key, _wkt, _inv) in enumerate(_CASES)
}


def _wkb(wkt: str) -> bytes:
    from qgis.core import QgsGeometry
    return bytes(QgsGeometry.fromWkt(wkt).asWkb())


def _measure(geom) -> dict:
    """Type + mesures d'une QgsGeometry, sous une forme comparable."""
    from qgis.core import QgsWkbTypes
    if geom is None:
        return {"present": False}
    try:
        if geom.isNull() or geom.isEmpty():
            return {"present": False}
    except Exception:  # noqa: BLE001
        return {"present": False}
    return {
        "present": True,
        "type": QgsWkbTypes.displayString(geom.wkbType()),
        "area": round(geom.area(), 6),
        "length": round(geom.length(), 6),
        "wkt": geom.asWkt(2)[:100],
    }


def _make_layer(tmpdir):
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        f"Polygon?crs={_CRS}&field=nom:string(32)&field=code:string(32)",
        "seed", "memory",
    )
    mem.startEditing()
    for key, _wkt, _inv in _CASES:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([_LIVE_NAME, key])
        feat.setGeometry(QgsGeometry.fromWkt(_LIVE_SQUARES[key]))
        mem.addFeature(feat)
    mem.commitChanges()

    path = str(Path(tmpdir) / "zones.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "zones"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)

    layer = QgsVectorLayer(f"{path}|layername=zones", "zones", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path}")
    return layer


def _event(fid, old_wkb, new_wkb, event_id):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=event_id,
        project_fingerprint="tx_mvs_project",
        datasource_fingerprint="tx_mvs_datasource",
        layer_id_snapshot="lyr_tx_mvs",
        layer_name_snapshot="zones",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": _OLD_NAME, "new": _LIVE_NAME}}}),
        geometry_wkb=old_wkb,
        geometry_type="Polygon",
        crs_authid=_CRS,
        field_schema_json=json.dumps(_FIELDS),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=new_wkb,
        invalidated_at=None,
    )


def _read_geom(layer, fid):
    from qgis.core import QgsFeatureRequest
    feat = next(layer.dataProvider().getFeatures(QgsFeatureRequest(int(fid))),
                None)
    return None if feat is None else feat.geometry()


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsGeometry

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_mvs_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_layer(tmpdir)
    ctx.data["layer"] = layer

    # Une feature par cas, dans l'ordre d'insertion.
    fids = sorted(f.id() for f in layer.getFeatures())
    ctx.data["fids"] = {key: fids[i] for i, (key, _w, _i) in enumerate(_CASES)}

    # Ce que le journal a capture, et ce que makeValid en ferait: les deux
    # mesures sont calculees ici pour que le message d'assertion puisse
    # nommer exactement ce qui est perdu.
    captured = {}
    for key, wkt, must_be_invalid in _CASES:
        geom = QgsGeometry.fromWkt(wkt)
        entry = {
            "wkt": wkt,
            "geos_valid": bool(geom.isGeosValid()),
            "must_be_invalid": must_be_invalid,
            "measure": _measure(geom),
        }
        if not entry["geos_valid"]:
            repaired = geom.makeValid()
            entry["repaired"] = _measure(repaired)
            bb_a, bb_b = geom.boundingBox(), repaired.boundingBox()
            entry["bbox_identical"] = (
                bb_a.xMinimum() == bb_b.xMinimum()
                and bb_a.yMinimum() == bb_b.yMinimum()
                and bb_a.xMaximum() == bb_b.xMaximum()
                and bb_a.yMaximum() == bb_b.yMaximum()
            )
        else:
            entry["repaired"] = None
            entry["bbox_identical"] = None
        captured[key] = entry
    ctx.data["captured"] = captured

    digest = {
        k: (v["geos_valid"], (v["measure"] or {}).get("area"),
            (v["repaired"] or {}).get("area"), v["bbox_identical"])
        for k, v in captured.items()
    }
    flog(
        f"tx_geom_makevalid_shape_swap setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} fids={ctx.data['fids']} captured={digest}",
        "INFO",
    )


def _geos_equal(a, b) -> bool:
    """Egalite geometrique tolerante a l'encodage, jamais levee."""
    if a is None or b is None:
        return False
    try:
        return bool(a.equals(b))
    except Exception:  # noqa: BLE001 - GEOS refuse parfois les invalides
        return False


def run(ctx):
    from recoverland.core.restore_executor import _buffer_update
    from recoverland.core.logger import flog
    from qgis.core import QgsGeometry

    layer = ctx.data["layer"]
    fids = ctx.data["fids"]

    flog(f"tx_geom_makevalid_shape_swap run start: trace_id={ctx.trace_id}",
         "INFO")

    outcomes = {}
    for i, (key, wkt, _inv) in enumerate(_CASES):
        fid = fids[key]
        captured_geom = QgsGeometry.fromWkt(wkt)
        # Geometrie que _buffer_update a DECIDE d'ecrire: l'originale si
        # elle est valide, sa reparation sinon. C'est elle qui doit se
        # retrouver sur le disque apres commit.
        if captured_geom.isGeosValid():
            intended = captured_geom
        else:
            intended = captured_geom.makeValid()

        live_geom = _read_geom(layer, fid)
        live_wkb = bytes(live_geom.asWkb()) if live_geom is not None else None
        event = _event(fid, _wkb(wkt), live_wkb, event_id=300 + i)

        layer.startEditing()
        result = _buffer_update(layer, event, trace_id=ctx.trace_id)
        committed = layer.commitChanges()
        layer.reload()

        after = _read_geom(layer, fid)
        outcomes[key] = {
            "eid": 300 + i,
            "result": dict(result) if isinstance(result, dict) else result,
            "committed": bool(committed),
            "after": _measure(after),
            "intended": _measure(intended),
            "after_equals_captured_wkb": (
                after is not None and bytes(after.asWkb()) == _wkb(wkt)
            ),
            "after_equals_captured_geos": _geos_equal(after, captured_geom),
            "after_equals_intended_geos": _geos_equal(after, intended),
        }
    ctx.data["outcomes"] = outcomes

    flog(
        f"tx_geom_makevalid_shape_swap run end: trace_id={ctx.trace_id} "
        f"outcomes={outcomes}",
        "INFO",
    )

    ctx.data["layer"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


_DRIFT_RE = re.compile(
    r"makevalid_drift: eid=(\d+).*?drift_units=([0-9.eE+-]+).*?status=(\w+)")


def _drift_log(records):
    """{eid: (drift_units, status)} extrait des lignes makevalid_drift."""
    found = {}
    for rec in records:
        m = _DRIFT_RE.search(rec.raw)
        if m:
            found[int(m.group(1))] = (m.group(2), m.group(3))
    return found


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    captured = ctx.data.get("captured") or {}
    outcomes = ctx.data.get("outcomes") or {}
    drift = _drift_log(ctx.records)

    # ===== Sanity: les geometries tordues le sont vraiment ==============
    for key, _wkt, must_be_invalid in _CASES:
        info = captured.get(key, {})
        out.append((
            f"{key}__fixture_validity_is_as_expected",
            info.get("geos_valid") is (not must_be_invalid),
            f"{key}: isGeosValid={info.get('geos_valid')} attendu "
            f"{not must_be_invalid}; sans cette condition le scenario "
            f"n'emprunte pas la branche makeValid",
        ))

    # ===== Le trou: la mesure de derive ne voit pas le changement =======
    for key, _wkt, must_be_invalid in _CASES:
        if not must_be_invalid:
            continue
        info = captured.get(key, {})
        if not info.get("bbox_identical"):
            continue
        rep = info.get("repaired") or {}
        mes = info.get("measure") or {}
        eid = (outcomes.get(key) or {}).get("eid")
        logged = drift.get(eid)
        out.append((
            f"{key}__drift_metric_sees_the_shape_change",
            logged is not None and logged[1] == "SKIPPED_GEOMETRY_DRIFT",
            f"{key}: makeValid change la forme (aire {mes.get('area')} -> "
            f"{rep.get('area')}, type {mes.get('type')} -> {rep.get('type')}) "
            f"en conservant la boite englobante au bit pres. Le journal "
            f"enregistre drift_units={logged[0] if logged else '?'} "
            f"status={logged[1] if logged else 'aucun log'}: la mesure de "
            f"derive ne compare que des boites, elle ne peut pas voir une "
            f"reorganisation interne.",
        ))

    # ===== La regle: forme rendue = forme capturee, ou refus ============
    for key, _wkt, must_be_invalid in _CASES:
        info = captured.get(key, {})
        res = (outcomes.get(key) or {}).get("result") or {}
        after = (outcomes.get(key) or {}).get("after") or {}
        mes = info.get("measure") or {}
        rep = info.get("repaired") or {}
        oc = outcomes.get(key) or {}
        skipped = res.get("status") == "SKIPPED_GEOMETRY_DRIFT"
        # Egalite geometrique, pas metrique: un noeud papillon a une aire
        # signee nulle et un perimetre que n'importe quelle degenerescence
        # de meme contour partage (piege rencontre a la premiere execution,
        # ou l'assertion passait sur une forme qui n'etait pas la bonne).
        same_shape = (
            oc.get("after_equals_captured_wkb") is True
            or oc.get("after_equals_captured_geos") is True
        )
        out.append((
            f"{key}__restored_shape_is_the_captured_one_or_refused",
            skipped or same_shape,
            f"{key}: le rewind a ecrit {after.get('type')} aire="
            f"{after.get('area')} perim={after.get('length')} "
            f"[{after.get('wkt')}] la ou le journal avait capture "
            f"{mes.get('type')} aire={mes.get('area')} "
            f"perim={mes.get('length')} (makeValid aurait donne "
            f"{rep.get('type')} aire={rep.get('area')} "
            f"perim={rep.get('length')}). status={res.get('status')} "
            f"message={res.get('message')!r} "
            f"wkb_identique={(outcomes.get(key) or {}).get('after_equals_captured_wkb')}. "
            f"L'utilisateur recupere une autre geometrie que la sienne, sans "
            f"aucun signal: ni refus, ni avertissement, ni difference de "
            f"boite englobante.",
        ))

    # ===== Ce qui est ecrit doit etre ce qui a ete decide ===============
    # Deuxieme mutation, independante de makeValid: la couche GeoPackage est
    # declaree Polygon. Quand la reparation produit un MultiPolygon, le
    # provider le ramene a une seule partie au commit et la deuxieme partie
    # devient un TROU. Personne ne relit la geometrie apres le commit.
    for key, _wkt, must_be_invalid in _CASES:
        oc = outcomes.get(key) or {}
        res = oc.get("result") or {}
        if res.get("status") != "APPLIED":
            continue
        after = oc.get("after") or {}
        intended = oc.get("intended") or {}
        out.append((
            f"{key}__applied_geometry_survives_the_commit",
            oc.get("after_equals_intended_geos") is True,
            f"{key}: _buffer_update a decide d'ecrire {intended.get('type')} "
            f"aire={intended.get('area')}, le disque contient "
            f"{after.get('type')} aire={after.get('area')} "
            f"[{after.get('wkt')}]. La couche est declaree monopart: la "
            f"partie surnumeraire de la geometrie reparee est devenue un "
            f"trou au commit. Le rewind annonce 'Reverted via buffer' sans "
            f"jamais relire ce qui a ete reellement persiste.",
        ))

    # ===== Temoin: une geometrie valide passe intacte ===================
    ctrl = (outcomes.get("valid_control") or {})
    out.append((
        "valid_geometry_is_restored_byte_for_byte",
        ctrl.get("after_equals_captured_wkb") is True
        and (ctrl.get("result") or {}).get("success") is True,
        f"temoin: une geometrie valide doit revenir a l'octet pres. "
        f"result={ctrl.get('result')} after={ctrl.get('after')} "
        f"wkb_identique={ctrl.get('after_equals_captured_wkb')}",
    ))

    # ===== La reparation doit au moins etre tracee ======================
    out.append(assert_log_contains(
        ctx.records,
        r"makevalid_drift: eid=\d+",
        name="repair_is_logged",
        min_count=1,
    ))

    # ===== Garde-fou de source ==========================================
    src = (_PLUGIN_ROOT / "core" / "geometry_utils.py").read_text(
        encoding="utf-8", errors="replace")
    start = src.find("def _compute_makevalid_drift(")
    end = src.find("def geometry_to_wkb(", start)
    body = src[start:end] if start >= 0 and end > start else ""
    out.append((
        "drift_metric_compares_more_than_bounding_boxes",
        "area" in body or "hausdorff" in body.lower() or "equals" in body,
        "core/geometry_utils._compute_makevalid_drift ne lit que "
        "boundingBox(): aucune mesure d'aire, de longueur, de type ni de "
        "distance de Hausdorff. Deux formes differentes de meme emprise sont "
        "declarees identiques, et le seul garde-fou du rewind repose "
        "entierement sur cette mesure.",
    ))

    # ===== Propagation du trace_id ======================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_geom_makevalid_shape_swap.*trace_id={ctx.trace_id}",
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
