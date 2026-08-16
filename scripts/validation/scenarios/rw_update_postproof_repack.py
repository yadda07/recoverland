"""rw_update_postproof_repack - RecoverLand validation runtime.

Invariant: BL-RW-P5-28 (a rewind must find its target when the target is
still there, must skip when there is nothing to do, and must keep refusing
when the target is genuinely unidentifiable).

What this proves, and where the numbers come from
-------------------------------------------------
Measured on the user's own run (trace c435ca0d, 2026-08-14 16:42, journal
`...\\Projet QGIS\\.recoverland\\recoverland_audit.sqlite`, 208 MB):

    finish status=partial ok=8 fail=59 applied=0 failed_target_absent=59

Zero features restored. The 59 refusals were opened one by one against the
live layer and classified:

    16  the NEW shape IS in the layer (15 at a shifted FID, 1 at the
        historical one) but `feature_matches_geometry` rejects it
    16  the feature is ALREADY at the OLD state -- the action is a no-op
     6  only an attribute key could locate the target; that path is not
        wired for UPDATE
    21  neither NEW nor OLD anywhere, or ambiguous -- refusal is correct

`zone_mkt_rip` is an ESRI shapefile: no primary key, positional FIDs, and
OGR rewrites ring encoding when it repacks the file. Its polygons are
GEOS-invalid at the source (108 self-intersections measured on one), which
is why `QgsGeometry.equals()` returns False on two encodings of the SAME
shape -- the comparison is only reliable after `makeValid()` on both sides.

The five cases below are those four buckets, reduced to their mechanism.
Case E is the reason the guard exists at all and must NEVER regress.

Cases
-----
    A  ring re-encoded (same vertices, rotated start), FID unchanged
       -> must be APPLIED                          (reproduces eid=62765)
    B  ring re-encoded AND FIDs shifted by a deletion
       -> must be APPLIED at the relocated FID      (reproduces eid=62774)
    C  attribute-only edit, FID shifted, NEW value unique in the layer
       -> must be APPLIED at the relocated FID      (reproduces eid=62742)
    D  the feature is already back at its OLD state
       -> must be SKIPPED, not counted as a failure (reproduces eid=62350)
    E  the historical FID holds an unrelated feature
       -> must be REFUSED and left byte-identical   (reproduces eid=62690)

Verdicts
--------
Pre-patch: 2/8 -- A, B, C and D all refused, the predicate rejects the
re-encoded shape, and the refusals are reported as `target_absent`.

Current: 6/8. C (attribute relocation), D (idempotence) and the predicate
itself are fixed. A and B still FAIL, and deliberately so: both now FIND
their target, but the write is stopped by the makeValid drift guard
(`SKIPPED_GEOMETRY_DRIFT`, tolerance 1e-6 = one micron in EPSG:2154).

Removing that guard was tried and REVERTED: writing the captured bytes
as-is assumes the provider refuses what it cannot store, and it does not
-- a single-part layer accepts a multi-part geometry, `changeGeometry`
returns True, and OGR drops the extra parts at COMMIT, persisting an
amputated feature under a "Reverted via buffer" success. That is
`tx_geom_makevalid_shape_swap`, which caught the attempt.

So A and B are the standing proof of the remaining work: a post-COMMIT
read-back that turns a mangled write into a reported failure. Until it
exists they must stay red -- a green suite would claim a rewind that
restores geometry, and it does not.
"""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_update_postproof_repack"
INVARIANT = "BL-RW-P5-28"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FIELDS = [{"name": "code", "type": "string"}, {"name": "val", "type": "integer"}]

# Lambert-93-like metric coordinates, so a drift reads in metres exactly as
# it does on the user's data.
_X0, _Y0 = 700000.0, 6500000.0


def _bowtie(dx: float, dy: float, rotate: int = 0):
    """A self-intersecting quadrilateral -- GEOS-INVALID by construction.

    The user's polygons are invalid at the source (they come out of a
    third-party export). That is not incidental: on a VALID polygon
    `equals()` already ignores the starting vertex, so a valid fixture
    would not reproduce the defect at all.

    *rotate* shifts the starting vertex of the ring. Same vertex multiset,
    same bounding box, DIFFERENT WKB bytes -- exactly what OGR produces
    when it repacks a shapefile.
    """
    from qgis.core import QgsGeometry, QgsPointXY

    ring = [
        (_X0 + dx, _Y0 + dy),
        (_X0 + dx + 100.0, _Y0 + dy + 100.0),
        (_X0 + dx + 100.0, _Y0 + dy),
        (_X0 + dx, _Y0 + dy + 100.0),
    ]
    ring = ring[rotate:] + ring[:rotate]
    ring = ring + [ring[0]]
    return QgsGeometry.fromPolygonXY([[QgsPointXY(x, y) for x, y in ring]])


def _wkb(geom) -> bytes:
    return bytes(geom.asWkb())


def _make_shapefile(tmpdir: str, rows):
    """Write a polygon shapefile from ``rows`` = [(code, val, geom), ...]."""
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsVectorFileWriter,
        QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        "Polygon?crs=EPSG:2154&field=code:string(20)&field=val:integer",
        "seed", "memory",
    )
    mem.startEditing()
    for code, val, geom in rows:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([code, val])
        feat.setGeometry(geom)
        mem.addFeature(feat)
    mem.commitChanges()

    path = str(Path(tmpdir) / "repack.shp")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)

    layer = QgsVectorLayer(path, "repack", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"shapefile layer invalid: {path}")
    return layer


def _event(eid: int, fid: int, old_attrs: dict, new_attrs: dict,
           old_geom=None, new_geom=None):
    from recoverland.core.audit_backend import AuditEvent

    changed = {
        k: {"old": old_attrs[k], "new": new_attrs[k]} for k in new_attrs
    }
    return AuditEvent(
        event_id=eid,
        project_fingerprint="rw_upr_project",
        datasource_fingerprint="rw_upr_datasource",
        layer_id_snapshot="lyr_upr",
        layer_name_snapshot="repack",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": changed}),
        geometry_wkb=_wkb(old_geom) if old_geom is not None else None,
        geometry_type="Polygon",
        crs_authid="EPSG:2154",
        field_schema_json=json.dumps(_FIELDS),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 14, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=_wkb(new_geom) if new_geom is not None else None,
        invalidated_at=None,
    )


def _snapshot(layer) -> dict:
    out = {}
    for f in layer.getFeatures():
        g = f.geometry()
        out[f.id()] = (f["code"], f["val"],
                       _wkb(g).hex() if g and not g.isNull() else None)
    return out


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_rw_upr_")
    ctx.data["tmpdir"] = tmpdir

    # Geometries. `*_old` is what the event captured as the pre-edit state,
    # `*_new` the post-edit state. `*_enc` is the SAME shape as `*_new`
    # with a rotated ring: what the repacked file actually holds.
    a_old, a_new = _bowtie(0, 0), _bowtie(0, 500)
    b_old, b_new = _bowtie(1000, 0), _bowtie(1000, 500)
    a_enc, b_enc = _bowtie(0, 500, rotate=2), _bowtie(1000, 500, rotate=3)
    c_geom = _bowtie(2000, 0)
    d_old = _bowtie(3000, 0)
    e_geom = _bowtie(4000, 0)

    ctx.data["invalid_fixture"] = not a_new.isGeosValid()
    ctx.data["enc_differs"] = _wkb(a_enc) != _wkb(a_new)
    ctx.data["same_bbox"] = a_enc.boundingBox() == a_new.boundingBox()

    # The live layer, AFTER the third-party repack:
    #   idx 0  the decoy that shifts every FID below it (case B/C rely on it)
    #   idx 1  case A, re-encoded NEW, at the FID the event recorded (1)
    #   idx 2  case B, re-encoded NEW, event recorded FID 9 (stale)
    #   idx 3  case C, attribute-only, event recorded FID 9 (stale)
    #   idx 4  case D, already back at OLD
    #   idx 5  case E, unrelated feature sitting at the event's FID 5
    rows = [
        ("FILLER", 0, _bowtie(5000, 0)),
        ("A", 2, a_enc),
        ("B", 2, b_enc),
        ("C", 2, c_geom),
        ("D", 1, d_old),
        ("DECOY", 99, e_geom),
    ]
    layer = _make_shapefile(tmpdir, rows)
    ctx.data["layer"] = layer
    ctx.data["before"] = _snapshot(layer)

    fids = sorted(ctx.data["before"])
    ctx.data["fid_a"], ctx.data["fid_b"] = fids[1], fids[2]
    ctx.data["fid_c"], ctx.data["fid_d"] = fids[3], fids[4]
    ctx.data["fid_e"] = fids[5]

    ctx.data["events"] = [
        # A: historical FID still correct, only the encoding changed.
        _event(101, ctx.data["fid_a"], {"val": 1}, {"val": 2},
               old_geom=a_old, new_geom=a_new),
        # B: historical FID is stale (records shifted), encoding changed.
        _event(102, 9, {"val": 1}, {"val": 2},
               old_geom=b_old, new_geom=b_new),
        # C: attribute-only edit (no geometry captured at all), stale FID.
        # TWO changed fields on purpose: `_find_by_attrs_only` refuses to
        # relocate on a single attribute (min_attrs=2) because one field is
        # not discriminant enough to bet a write on. That refusal is
        # correct and must not be weakened -- so the case that PROVES the
        # relocation has to be one where the evidence is sufficient.
        _event(103, 9, {"val": 1, "code": "C_OLD"}, {"val": 2, "code": "C"}),
        # D: the feature is already back at OLD -- nothing to do.
        _event(104, ctx.data["fid_d"], {"val": 1}, {"val": 2},
               old_geom=d_old, new_geom=_bowtie(3000, 500)),
        # E: the historical FID holds a stranger. Must stay untouched.
        _event(105, ctx.data["fid_e"], {"val": 1}, {"val": 2},
               old_geom=_bowtie(9000, 0), new_geom=_bowtie(9000, 500)),
    ]
    ctx.data["expect_fid"] = {
        101: ctx.data["fid_a"], 102: ctx.data["fid_b"], 103: ctx.data["fid_c"],
    }

    flog(
        f"rw_update_postproof_repack setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} n_features={len(rows)} "
        f"fixture_geos_invalid={ctx.data['invalid_fixture']} "
        f"encoding_differs={ctx.data['enc_differs']} "
        f"same_bbox={ctx.data['same_bbox']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_executor import _apply_via_buffer

    layer = ctx.data["layer"]
    results = {}

    # The predicate, measured on its own: this is the mechanism under test,
    # isolated from every other moving part of the rewind.
    from recoverland.core.geometry_utils import feature_matches_geometry
    probe_feat = layer.getFeature(ctx.data["fid_a"])
    expected = _bowtie(0, 500)
    ctx.data["predicate_says"] = bool(
        feature_matches_geometry(probe_feat, expected))

    layer.startEditing()
    layer.beginEditCommand("rw_update_postproof_repack")
    for event in ctx.data["events"]:
        res = _apply_via_buffer(layer, "UPDATE", event, {},
                                trace_id=ctx.trace_id)
        results[event.event_id] = {
            "success": bool(res.get("success")),
            "skipped": bool(res.get("skipped")),
            "status": res.get("status"),
            "reason_code": res.get("reason_code"),
            "message": str(res.get("message", ""))[:120],
        }
        flog(f"rw_upr: eid={event.event_id} -> {results[event.event_id]}")
    layer.endEditCommand()
    committed = layer.commitChanges()
    if not committed:
        layer.rollBack()

    ctx.data["results"] = results
    ctx.data["committed"] = bool(committed)
    ctx.data["after"] = _snapshot(layer)
    flog(
        f"rw_update_postproof_repack run: trace_id={ctx.trace_id} "
        f"committed={committed} predicate_says={ctx.data['predicate_says']} "
        f"results={results}",
        "INFO",
    )


def _val_at(snap, fid):
    row = snap.get(fid)
    return row[1] if row else None


def assertions(ctx):
    out = []
    res = ctx.data.get("results", {})
    before = ctx.data.get("before", {})
    after = ctx.data.get("after", {})

    # The fixture must really reproduce the conditions, or the rest is noise.
    out.append((
        "fixture_reproduces_the_conditions",
        bool(ctx.data.get("invalid_fixture")) and bool(ctx.data.get("enc_differs"))
        and bool(ctx.data.get("same_bbox")),
        f"GEOS-invalid={ctx.data.get('invalid_fixture')} "
        f"encoding_differs={ctx.data.get('enc_differs')} "
        f"same_bbox={ctx.data.get('same_bbox')} "
        f"(all three required: a valid polygon would match anyway)",
    ))

    # THE mechanism, isolated.
    out.append((
        "predicate_matches_reencoded_shape",
        bool(ctx.data.get("predicate_says")),
        f"feature_matches_geometry(live, expected)="
        f"{ctx.data.get('predicate_says')} on two encodings of the SAME "
        f"shape (same vertices, rotated ring, identical bbox)",
    ))

    # A -- applied at the historical FID.
    a = res.get(101, {})
    out.append((
        "reencoded_geometry_applied",
        a.get("success") and not a.get("skipped")
        and _val_at(after, ctx.data["fid_a"]) == 1,
        f"A: {a.get('status')} reason={a.get('reason_code')} "
        f"val {_val_at(before, ctx.data['fid_a'])} -> "
        f"{_val_at(after, ctx.data['fid_a'])} (expected 1)",
    ))

    # B -- relocated to the shifted FID.
    b = res.get(102, {})
    out.append((
        "repacked_fid_relocated",
        b.get("success") and not b.get("skipped")
        and _val_at(after, ctx.data["fid_b"]) == 1,
        f"B: {b.get('status')} reason={b.get('reason_code')} "
        f"val at relocated fid {_val_at(after, ctx.data['fid_b'])} "
        f"(expected 1); event carried the stale fid 9",
    ))

    # C -- attribute-only relocation.
    c = res.get(103, {})
    out.append((
        "attr_only_relocated",
        c.get("success") and not c.get("skipped")
        and _val_at(after, ctx.data["fid_c"]) == 1,
        f"C: {c.get('status')} reason={c.get('reason_code')} "
        f"val at relocated fid {_val_at(after, ctx.data['fid_c'])} "
        f"(expected 1); no geometry captured, only an attribute key",
    ))

    # D -- already at OLD: a skip, not a failure, and nothing written.
    d = res.get(104, {})
    out.append((
        "already_restored_is_skipped",
        d.get("success") and d.get("skipped")
        and before.get(ctx.data["fid_d"]) == after.get(ctx.data["fid_d"]),
        f"D: {d.get('status')} reason={d.get('reason_code')} "
        f"unchanged={before.get(ctx.data['fid_d']) == after.get(ctx.data['fid_d'])} "
        f"(writing OLD onto a feature already at OLD is a no-op, "
        f"not a failure)",
    ))

    # E -- THE regression that must never be introduced.
    e = res.get(105, {})
    out.append((
        "decoy_untouched",
        (not e.get("success") or e.get("skipped"))
        and before.get(ctx.data["fid_e"]) == after.get(ctx.data["fid_e"]),
        f"E: {e.get('status')} reason={e.get('reason_code')} "
        f"decoy byte-identical="
        f"{before.get(ctx.data['fid_e']) == after.get(ctx.data['fid_e'])} "
        f"(refusing here is the whole point of the guard)",
    ))

    # The per-action result must name the real cause. The aggregated
    # CYCLE_SUMMARY counter still folds this into failed_target_absent --
    # splitting the six-way breakdown is its own change, pinned by
    # rw_apply_perf::unverifiable_is_soft -- but the reason_code carried
    # by the result is what a reader can act on.
    refused = [r for r in res.values()
               if not r.get("success") and not r.get("skipped")]
    out.append((
        "refusal_reason_is_not_target_absent",
        all(r.get("reason_code") != "target_absent" for r in refused),
        f"refusal reason codes={[r.get('reason_code') for r in refused]} "
        f"('target_absent' would claim the feature is gone when it is "
        f"present but unverifiable)",
    ))

    return out
