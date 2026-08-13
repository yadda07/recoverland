"""rs_update_target_proof - RecoverLand validation runtime.

Invariant: I-3 (identity strength) at the point of WRITE.
Root cause: `restore_service.restore_updated_feature` wrote the event's OLD
state onto whatever feature occupied the historical FID, with no proof that
it was the right one.

The hole this scenario proves
-----------------------------
On a shapefile or a GeoJSON the FID is a record number, not an identity:
OGR renumbers records after a deletion or a re-export. The rewind executor
knows this -- `_buffer_update` refuses (or re-locates the target) when the
feature at the historical FID no longer carries the captured NEW state. The
event-by-event RESTORE path did none of that:

    target_fid = _find_target_feature(layer, identity, fid_cache)
    ...
    provider.changeAttributeValues(attr_changes)
    provider.changeGeometryValues({target_fid: geom})

So restoring one event from the history could overwrite the attributes AND
the geometry of a completely unrelated feature, report success, and journal
the damage as a legitimate restore. For a recovery tool this is the worst
possible outcome, and nothing in the UI hinted at it.

Proof required before writing, weakest identity first:
  - a real primary key resolved the target -> the FID never mattered;
  - the event captured a NEW geometry -> the feature must still carry it;
  - attribute-only edit -> the feature must still carry the NEW values.
Nothing comparable at all -> refuse. Refusing loses one restore; applying
destroys a feature that was never part of the operation.

Scenario layout (real shapefile in a temp dir, real QGIS provider):
    setup: a .shp holding ONE feature, "PONT" at (5 5)
    case 1: an event describing a past edit of fid=1 whose NEW geometry was
            (1 1) -- i.e. the FID has been reused since. Must be REFUSED and
            the live feature must come out untouched.
    case 2: the same event with a NEW geometry that does match the live
            feature. Must be APPLIED -- the guard must not block a
            legitimate restore.

Pre-patch verdict: FAIL (case 1 overwrites the wrong feature).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rs_update_target_proof"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = [{"name": "nom", "type": "string"}, {"name": "code", "type": "string"}]
_LIVE_NAME = "PONT"
_LIVE_CODE = "C999"
_LIVE_XY = (5.0, 5.0)
_STALE_XY = (1.0, 1.0)


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _make_shapefile(tmpdir: str):
    """Write a one-feature shapefile and return the loaded layer."""
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=nom:string(20)&field=code:string(20)",
        "seed", "memory",
    )
    mem.startEditing()
    feat = QgsFeature(mem.fields())
    feat.setAttributes([_LIVE_NAME, _LIVE_CODE])
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*_LIVE_XY)))
    mem.addFeature(feat)
    mem.commitChanges()

    path = str(Path(tmpdir) / "targets.shp")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)

    layer = QgsVectorLayer(path, "targets", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"shapefile layer invalid: {path}")
    return layer


def _event(new_wkb: bytes, fid: int):
    """An UPDATE event on *fid*: 'nom' went from PILE to PONT, geom moved.

    *fid* is read from the live layer rather than hard-coded: OGR numbers
    shapefile records from 0, GPKG from 1.
    """
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=1,
        project_fingerprint="rs_utp_project",
        datasource_fingerprint="rs_utp_datasource",
        layer_id_snapshot="lyr_utp",
        layer_name_snapshot="targets",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": "PILE", "new": _LIVE_NAME},
        }}),
        geometry_wkb=_wkb_point(0.0, 0.0),
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps(_FIELDS),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint="fid:1",
        event_schema_version=2,
        new_geometry_wkb=new_wkb,
        invalidated_at=None,
    )


def _snapshot(layer) -> dict:
    feat = next(layer.getFeatures(), None)
    if feat is None:
        return {}
    geom = feat.geometry()
    point = geom.asPoint() if not geom.isNull() else None
    return {
        "fid": feat.id(),
        "nom": feat["nom"],
        "code": feat["code"],
        "xy": (round(point.x(), 6), round(point.y(), 6)) if point else None,
    }


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_rs_utp_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_shapefile(tmpdir)
    ctx.data["layer"] = layer
    ctx.data["before"] = _snapshot(layer)

    from recoverland.core.identity import get_identity_strength_for_layer
    ctx.data["strength"] = str(get_identity_strength_for_layer(layer))

    flog(
        f"rs_update_target_proof setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} before={ctx.data['before']} "
        f"strength={ctx.data['strength']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.restore_service import restore_updated_feature
    from recoverland.core.logger import flog

    layer = ctx.data["layer"]

    flog(f"rs_update_target_proof run start: trace_id={ctx.trace_id}", "INFO")

    # --- case 1: the FID now points at an unrelated feature --------------
    live_fid = ctx.data["before"]["fid"]
    stale = _event(_wkb_point(*_STALE_XY), live_fid)
    ctx.data["stale_result"] = restore_updated_feature(layer, stale)
    layer.reload()
    ctx.data["after_stale"] = _snapshot(layer)

    # --- case 2: the FID still holds the captured NEW state --------------
    matching = _event(_wkb_point(*_LIVE_XY), live_fid)
    ctx.data["match_result"] = restore_updated_feature(layer, matching)
    layer.reload()
    ctx.data["after_match"] = _snapshot(layer)

    flog(
        f"rs_update_target_proof run end: trace_id={ctx.trace_id} "
        f"stale={ctx.data['stale_result']} after_stale={ctx.data['after_stale']} "
        f"match={ctx.data['match_result']} after_match={ctx.data['after_match']}",
        "INFO",
    )

    # Everything the assertions need is already in ctx.data. The runner has no
    # teardown hook, so drop the fixture here; OGR may still hold the file open
    # on Windows, hence ignore_errors.
    ctx.data["layer"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    before = ctx.data.get("before") or {}
    stale_res = ctx.data.get("stale_result") or {}
    after_stale = ctx.data.get("after_stale") or {}
    match_res = ctx.data.get("match_result") or {}
    after_match = ctx.data.get("after_match") or {}

    # ===== Sanity ========================================================
    out.append((
        "fixture_is_fid_only",
        ctx.data.get("strength") not in (None, "IdentityStrength.NONE"),
        f"identity strength={ctx.data.get('strength')}: the scenario needs a "
        f"layer the restore path actually accepts, otherwise it short-circuits "
        f"before reaching the guard",
    ))
    out.append((
        "fixture_seeded",
        before.get("nom") == _LIVE_NAME and before.get("xy") == _LIVE_XY,
        f"before={before} expected nom={_LIVE_NAME} xy={_LIVE_XY}",
    ))

    # ===== The hole ======================================================
    out.append((
        "stale_fid_restore_is_refused",
        stale_res.get("success") is False,
        f"result={stale_res}: the event's NEW geometry is {_STALE_XY} while the "
        f"feature at fid=1 sits at {_LIVE_XY}. The FID has been reused; writing "
        f"the OLD state here destroys an unrelated feature.",
    ))
    out.append((
        "refusal_is_explicit",
        stale_res.get("reason_code") == "target_unverifiable",
        f"reason_code={stale_res.get('reason_code')!r} expected "
        f"'target_unverifiable' (the caller must be able to tell an unprovable "
        f"target from an ordinary failure)",
    ))
    out.append((
        "unrelated_feature_untouched",
        after_stale == before,
        f"after={after_stale} before={before}: neither attributes nor geometry "
        f"of the live feature may change when the target is unproven",
    ))

    # ===== The guard must not block a legitimate restore =================
    out.append((
        "matching_target_is_applied",
        match_res.get("success") is True,
        f"result={match_res}: the feature at fid=1 does carry the captured NEW "
        f"state, so the restore is legitimate and must go through",
    ))
    out.append((
        "matching_restore_reverted_the_value",
        after_match.get("nom") == "PILE",
        f"after={after_match} expected nom='PILE' (the OLD value of the delta)",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"restore_updated\[1\]:.*status=refused",
        name="refusal_logged",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rs_update_target_proof.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Source guard ==================================================
    src = (_PLUGIN_ROOT / "core" / "restore_service.py").read_text(
        encoding="utf-8", errors="replace")
    start = src.find("def restore_updated_feature(")
    write = src.find("provider.changeAttributeValues", start)
    body = src[start:write] if start >= 0 and write > start else ""
    out.append((
        "guard_runs_before_any_write",
        "_verify_update_target" in body,
        "restore_updated_feature must verify the target BEFORE calling "
        "changeAttributeValues; a check placed after the write proves nothing",
    ))
    out.append((
        "guard_accepts_a_real_primary_key",
        bool(re.search(r"def _verify_update_target[\s\S]{0,2500}?pk_field", src)),
        "a PK-resolved target needs no FID proof; the guard must say so "
        "explicitly or it will refuse valid PostGIS/GPKG restores",
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
