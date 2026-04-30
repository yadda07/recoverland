"""Restore executor for RecoverLand (BL-04).

Applies a RestorePlan to a QGIS layer. Supports strict_atomic
(editing buffer + rollback) and best_effort (per-entity isolation).

Must run on main thread only.
"""
from typing import Dict, List, Optional

from .audit_backend import AuditEvent, RestoreReport
from .restore_contracts import (
    RestorePlan, AtomicityPolicy, PreflightVerdict,
    RestoreSession,
)
from .restore_planner import preflight_check
from .restore_service import restore_batch, build_restore_trace_event
from .search_service import reconstruct_attributes
from .schema_drift import safe_field_mapping
from .geometry_utils import (
    rebuild_geometry, is_geometry_present,
    feature_matches_geometry, get_feature_source,
    wkb_short_repr, feature_geom_short_repr,
)
from .serialization import iter_mapped_attributes
from .logger import flog


def preflight_layer_check(plan: RestorePlan, layer) -> List[str]:
    """Check layer-level prerequisites that require QGIS objects.

    Returns list of blocking reasons (empty = OK).
    """
    from ..compat import QgisCompat

    blocking: List[str] = []
    if layer is None:
        blocking.append("Layer is None")
        return blocking
    if not hasattr(layer, 'dataProvider') or layer.dataProvider() is None:
        blocking.append("Layer has no data provider")
        return blocking

    caps = layer.dataProvider().capabilities()
    needed_ops = {a.compensatory_op for a in plan.actions}
    cap_map = {
        "INSERT": (QgisCompat.CAP_ADD_FEATURES, "AddFeatures"),
        "DELETE": (QgisCompat.CAP_DELETE_FEATURES, "DeleteFeatures"),
        "UPDATE": (QgisCompat.CAP_CHANGE_ATTRIBUTE_VALUES, "ChangeAttributeValues"),
    }
    for op in needed_ops:
        entry = cap_map.get(op)
        if entry and not (caps & entry[0]):
            blocking.append(f"Provider missing capability: {entry[1]}")

    has_geom_update = any(
        a.has_geometry and a.compensatory_op == "UPDATE" for a in plan.actions
    )
    if has_geom_update and not (caps & QgisCompat.CAP_CHANGE_GEOMETRIES):
        blocking.append("Provider missing capability: ChangeGeometries")

    return blocking


def execute_restore_plan(
    plan: RestorePlan,
    events_by_id: Dict[int, AuditEvent],
    layer,
    trace_id: str = "",
) -> RestoreReport:
    """Execute a restore plan against a single layer.

    Runs pure preflight, then layer preflight, then dispatches.
    """
    report = preflight_check(plan)
    if report.verdict == PreflightVerdict.BLOCKED:
        msg = "; ".join(report.blocking_reasons[:3])
        return RestoreReport([], {0: f"Preflight blocked: {msg}"}, plan.event_count)

    layer_issues = preflight_layer_check(plan, layer)
    if layer_issues:
        msg = "; ".join(layer_issues[:3])
        return RestoreReport([], {0: f"Layer check failed: {msg}"}, plan.event_count)

    if plan.atomicity == AtomicityPolicy.STRICT:
        return _execute_strict(plan, events_by_id, layer, trace_id=trace_id)
    return _execute_best_effort(plan, events_by_id, layer)


def build_restore_session(
    plan: RestorePlan, report: RestoreReport,
    started_at: str, finished_at: str,
) -> RestoreSession:
    """Build a RestoreSession record from execution results."""
    import uuid
    s_count = len(report.succeeded)
    f_count = len(report.failed)
    if f_count == 0 and s_count > 0:
        status = "completed"
    elif s_count > 0 and f_count > 0:
        status = "partial"
    elif s_count == 0 and f_count > 0:
        status = "failed"
    else:
        status = "cancelled"
    return RestoreSession(
        session_id=str(uuid.uuid4()),
        mode=plan.mode,
        scope=plan.scope,
        cutoff=plan.cutoff,
        datasource_fingerprint=plan.datasource_fingerprint,
        layer_name=plan.layer_name,
        started_at=started_at,
        finished_at=finished_at,
        succeeded_count=s_count,
        failed_count=f_count,
        total_requested=report.total_requested,
        status=status,
    )


def _execute_best_effort(
    plan: RestorePlan,
    events_by_id: Dict[int, AuditEvent],
    layer,
) -> RestoreReport:
    """Per-entity isolation via existing restore_batch."""
    ordered = [events_by_id[a.event_id] for a in plan.actions if a.event_id in events_by_id]
    return restore_batch(layer, ordered)


def _execute_strict(
    plan: RestorePlan,
    events_by_id: Dict[int, AuditEvent],
    layer,
    trace_id: str = "",
) -> RestoreReport:
    """All-or-nothing via QGIS editing buffer."""
    prefix = f"[{trace_id}] " if trace_id else ""
    layer_name = layer.name() if hasattr(layer, 'name') else "?"
    flog(f"{prefix}strict_execute: start layer={layer_name} "
         f"actions={len(plan.actions)}")

    was_editing = layer.isEditable()
    if not was_editing:
        if not layer.startEditing():
            flog(f"{prefix}strict_execute: startEditing failed", "ERROR")
            return RestoreReport([], {0: "Cannot start editing on layer"}, plan.event_count)

    layer.beginEditCommand("RecoverLand: temporal restore")
    succeeded: List[int] = []
    failed: Dict[int, str] = {}

    for action in plan.actions:
        event = events_by_id.get(action.event_id)
        if event is None:
            failed[action.event_id] = "Event data not found"
            break

        result = _apply_via_buffer(layer, action.compensatory_op, event)
        if result["success"]:
            succeeded.append(action.event_id)
        else:
            failed[action.event_id] = result["message"]
            break

    if failed:
        flog(f"{prefix}strict_execute: rollback_started "
             f"applied={len(succeeded)} failed={len(failed)}", "WARNING")
        layer.destroyEditCommand()
        if not was_editing:
            layer.rollBack()
        flog(f"{prefix}strict_execute: rollback_done", "WARNING")
        return RestoreReport([], failed, plan.event_count)

    layer.endEditCommand()
    if not was_editing:
        flog(f"{prefix}strict_execute: commit_started")
        if not layer.commitChanges():
            errors = layer.commitErrors()
            msg = "; ".join(errors) if errors else "Commit failed"
            flog(f"{prefix}strict_execute: commit_failed msg={msg}", "ERROR")
            layer.rollBack()
            return RestoreReport([], {0: msg}, plan.event_count)
        flog(f"{prefix}strict_execute: commit_done")

    traces = _build_traces_for_succeeded(succeeded, events_by_id, layer)
    flog(f"{prefix}strict_execute: completed ok={len(succeeded)} "
         f"traces={len(traces)}")
    return RestoreReport(succeeded, {}, plan.event_count, trace_events=tuple(traces))


def _build_traces_for_succeeded(
    succeeded: List[int],
    events_by_id: Dict[int, AuditEvent],
    layer,
) -> list:
    """Build trace events for successfully restored events."""
    traces = []
    for eid in succeeded:
        event = events_by_id.get(eid)
        if event is None:
            continue
        trace = build_restore_trace_event(event, layer)
        if trace is not None:
            traces.append(trace)
    return traces


def _apply_via_buffer(layer, compensatory_op: str, event: AuditEvent,
                      fid_remap: Optional[Dict] = None) -> dict:
    """Apply a single compensatory action via the layer editing buffer.

    fid_remap: shared dict {entity_fingerprint -> new_fid} maintained across
    actions of the same atomic restore. INSERT compensatory writes into it;
    UPDATE/DELETE consult it first. This is required for FID-only identity
    layers where re-inserted features get fresh negative FIDs in the buffer
    that no longer match the historical FID stored in the event.
    """
    if compensatory_op == "INSERT":
        return _buffer_insert(layer, event, fid_remap)
    if compensatory_op == "DELETE":
        return _buffer_delete(layer, event, fid_remap)
    if compensatory_op == "UPDATE":
        return _buffer_update(layer, event, fid_remap)
    return {"success": False, "message": f"Unknown operation: {compensatory_op}"}


def _buffer_insert(layer, event: AuditEvent,
                   fid_remap: Optional[Dict] = None) -> dict:
    """Re-insert a deleted feature via editing buffer.

    Plain addFeature. No geometry-based idempotence: scanning the layer
    for an existing feature matching the snapshot caused cross-feature
    corruption (matching a freshly inserted buffer feature or an
    unrelated neighbour, then overwriting its attrs+geom with the wrong
    event's snapshot). If rewinds are repeated, users must rollback
    compensatory inserts explicitly via the strict runner, not rely on
    silent merging here.
    """
    from qgis.core import QgsFeature

    attrs = reconstruct_attributes(event)
    geom = rebuild_geometry(event.geometry_wkb)
    mapping = _safe_field_mapping(layer, event)

    feature = QgsFeature(layer.fields())
    fields = layer.fields()
    for idx, value in iter_mapped_attributes(mapping, attrs, fields):
        feature.setAttribute(idx, value)

    if geom is not None:
        feature.setGeometry(geom)

    if not layer.addFeature(feature):
        return {"success": False, "message": "Buffer addFeature failed"}
    _record_remap(fid_remap, event, feature.id())
    return {"success": True, "message": "Inserted via buffer", "fid": feature.id()}


def _record_remap(fid_remap: Optional[Dict], event: AuditEvent,
                  new_fid: Optional[int]) -> None:
    if fid_remap is None or new_fid is None:
        return
    key = event.entity_fingerprint
    if key:
        fid_remap[key] = new_fid


def _resolve_target_fid(layer, event: AuditEvent,
                        fid_remap: Optional[Dict]) -> Optional[int]:
    """Return remapped FID for entity if known, else None.

    The remap is built within a single restore session: when a Phase 0
    compensatory INSERT re-creates a feature, it stores the resulting
    buffer FID in fid_remap[entity_fingerprint]. Subsequent Phase 1
    UPDATE / Phase 2 DELETE actions on the same entity must consult the
    remap to reach the freshly-allocated FID, otherwise they look up the
    historical FID, miss, and silently skip.

    Both 'pk:field=value' and 'fid:N' keys are honoured. The dict is
    built fresh for each per-layer restore session in StrictRestoreRunner
    (see _strict_fid_remap reset in begin_strict_for_layer), so stale
    cross-session entries are not a concern. Within a session, two
    events sharing the same entity_fingerprint are by definition the
    same logical entity per the auditing model, so applying the remap
    is correct.
    """
    if fid_remap is None:
        return None
    key = event.entity_fingerprint
    if not key:
        return None
    return fid_remap.get(key)


def _buffer_delete(layer, event: AuditEvent,
                   fid_remap: Optional[Dict] = None) -> dict:
    """Delete an inserted feature via editing buffer.

    Safety: when identity is FID-only (no PK captured), FIDs can be reused
    across commits by some providers (GPKG/OGR). Before deleting, verify
    that the target feature still matches the snapshot captured at INSERT
    time. On mismatch, refuse rather than risk destroying unrelated data.
    """
    from .restore_service import (
        _parse_identity, _find_target_feature, _find_by_snapshot,
        _diagnose_snapshot_miss,
    )

    identity = _parse_identity(event.feature_identity_json)
    remapped = _resolve_target_fid(layer, event, fid_remap)
    target_fid = remapped if remapped is not None \
        else _find_target_feature(layer, identity)
    if target_fid is None:
        snapshot_fid = _find_by_snapshot(layer, event, resolve_ambiguity=True)
        if snapshot_fid is not None:
            flog(f"_buffer_delete: identity miss, recovered by snapshot scan "
                 f"eid={event.event_id} identity_fid={identity.get('fid')} "
                 f"recovered_fid={snapshot_fid}", "WARNING")
            target_fid = snapshot_fid
        else:
            flog(f"_buffer_delete: target already absent eid={event.event_id} "
                 f"identity_fid={identity.get('fid')} "
                 f"pk_field={identity.get('pk_field')!r} "
                 f"pk_value={identity.get('pk_value')!r}")
            _diagnose_snapshot_miss(layer, event)
            return {"success": True, "message": "Already absent"}

    has_pk = bool(identity.get("pk_field")) and identity.get("pk_value") is not None
    trusted = remapped is not None or has_pk
    if not trusted and not _target_matches_insert_snapshot(layer, target_fid, event):
        fallback_fid = _find_by_snapshot(layer, event, resolve_ambiguity=True)
        if fallback_fid is None:
            flog(f"_buffer_delete: identity mismatch, skipping delete "
                 f"eid={event.event_id} fid={target_fid}", "WARNING")
            return {"success": True,
                    "skipped": True,
                    "message": "Skipped: identity mismatch (FID-only)"}
        flog(f"_buffer_delete: FID mismatch, recovered by snapshot scan "
             f"eid={event.event_id} identity_fid={target_fid} "
             f"recovered_fid={fallback_fid}")
        target_fid = fallback_fid

    if not layer.deleteFeature(target_fid):
        return {"success": False, "message": "Buffer deleteFeature failed"}
    return {"success": True, "message": "Deleted via buffer"}


def _target_matches_insert_snapshot(layer, fid: int, event: AuditEvent) -> bool:
    """Check that feature fid matches the INSERT-time snapshot of event."""
    attrs = reconstruct_attributes(event)
    geom = rebuild_geometry(event.geometry_wkb) if event.geometry_wkb else None
    mapping = _safe_field_mapping(layer, event)
    try:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(fid)
        source = get_feature_source(layer)
        for feature in source(request):
            if geom is not None and not _feature_geometry_matches(feature, geom):
                return False
            fields = layer.fields()
            for idx, value in iter_mapped_attributes(mapping, attrs, fields):
                if feature[idx] != value:
                    return False
            return True
    except Exception as exc:
        flog(f"_buffer_delete: snapshot verification failed "
             f"fid={fid}: {exc}", "WARNING")
        return False
    return False


def _buffer_update(layer, event: AuditEvent,
                   fid_remap: Optional[Dict] = None) -> dict:
    """Revert attributes and geometry via editing buffer.

    Safety: when identity is FID-only (no PK captured) and the captured NEW
    geometry does not match what is currently at the historical FID, the
    shapefile has reorganised its FIDs since capture. Falling through would
    rewrite an unrelated feature with the OLD geometry of the original.
    Resolution: locate the genuine target by scanning for the captured NEW
    geometry (post-edit state). On failure, refuse rather than mutate the
    wrong feature.
    """
    from .restore_service import _parse_identity, _find_target_feature

    identity = _parse_identity(event.feature_identity_json)
    remapped = _resolve_target_fid(layer, event, fid_remap)
    target_fid = remapped if remapped is not None \
        else _find_target_feature(layer, identity)

    has_pk = bool(identity.get("pk_field")) and identity.get("pk_value") is not None
    trusted = remapped is not None or has_pk

    if target_fid is not None and not trusted:
        if not _target_matches_update_post_state(layer, target_fid, event):
            fallback_fid = _find_update_target_by_post_state(layer, event)
            if fallback_fid is None:
                flog(f"_buffer_update: post-state mismatch and no snapshot "
                     f"match, skipping eid={event.event_id} "
                     f"historical_fid={target_fid}", "WARNING")
                return {"success": True,
                        "skipped": True,
                        "message": "Skipped: post-state mismatch (FID-only)"}
            flog(f"_buffer_update: FID mismatch, recovered by post-state "
                 f"scan eid={event.event_id} historical_fid={target_fid} "
                 f"recovered_fid={fallback_fid}")
            target_fid = fallback_fid

    if target_fid is None:
        fallback_fid = _find_update_target_by_post_state(layer, event)
        if fallback_fid is not None:
            flog(f"_buffer_update: target located via post-state scan "
                 f"eid={event.event_id} fid={fallback_fid}")
            target_fid = fallback_fid
        else:
            flog(f"_buffer_update: target absent, skipping "
                 f"eid={event.event_id} "
                 f"identity_keys={list(identity.keys())}", "WARNING")
            return {"success": True,
                    "skipped": True,
                    "message": "Skipped: target feature absent"}
    if remapped is not None:
        flog(f"_buffer_update: using fid_remap "
             f"eid={event.event_id} fid={target_fid}")

    old_attrs = reconstruct_attributes(event)
    mapping = _safe_field_mapping(layer, event)
    fields = layer.fields()

    attr_ok = 0
    attr_fail = 0
    for idx, value in iter_mapped_attributes(mapping, old_attrs, fields):
        if layer.changeAttributeValue(target_fid, idx, value):
            attr_ok += 1
        else:
            attr_fail += 1

    geom_status = "no_geom_in_event"
    if event.geometry_wkb is not None:
        wkb_len = len(event.geometry_wkb)
        geom = rebuild_geometry(event.geometry_wkb)
        if geom is None:
            geom_status = f"rebuild_failed wkb_len={wkb_len}"
            flog(f"_buffer_update: rebuild_geometry returned None "
                 f"eid={event.event_id} fid={target_fid} wkb_len={wkb_len}",
                 "ERROR")
            return {"success": False,
                    "message": "Geometry rebuild failed (corrupt WKB)"}
        if geom.isEmpty() or geom.isNull():
            geom_status = "rebuilt_empty"
            flog(f"_buffer_update: rebuilt geometry empty/null "
                 f"eid={event.event_id} fid={target_fid}", "ERROR")
            return {"success": False,
                    "message": "Geometry rebuilt empty"}

        before_apply = feature_geom_short_repr(layer, target_fid)
        old_target = wkb_short_repr(event.geometry_wkb)

        identical = _current_geom_matches(layer, target_fid, geom)

        change_ok = layer.changeGeometry(target_fid, geom)
        after_apply = feature_geom_short_repr(layer, target_fid)

        flog(f"TRACE_RESTORE: eid={event.event_id} fid={target_fid} "
             f"before_apply={before_apply} "
             f"target_OLD={old_target} "
             f"changeGeometry={change_ok} "
             f"after_apply={after_apply} "
             f"identical={identical}")

        if not change_ok:
            geom_status = "changeGeometry_rejected"
            flog(f"_buffer_update: changeGeometry REJECTED "
                 f"eid={event.event_id} fid={target_fid} wkb_len={wkb_len}",
                 "ERROR")
            return {"success": False,
                    "message": "changeGeometry rejected by buffer"}

        if identical:
            flog(f"_buffer_update: NO-OP geom identical "
                 f"eid={event.event_id} fid={target_fid}", "WARNING")
            geom_status = f"noop wkb_len={wkb_len}"
        else:
            flog(f"_buffer_update: geom changed "
                 f"eid={event.event_id} fid={target_fid}")
            geom_status = f"applied wkb_len={wkb_len}"

    if attr_fail > 0 and attr_ok == 0:
        flog(f"_buffer_update: all attr changes REJECTED "
             f"eid={event.event_id} fid={target_fid} attempted={attr_fail}",
             "ERROR")
        return {"success": False,
                "message": "All attribute changes rejected"}

    flog(f"_buffer_update: applied eid={event.event_id} fid={target_fid} "
         f"attr_ok={attr_ok} attr_fail={attr_fail} geom={geom_status}")
    return {"success": True, "message": "Reverted via buffer"}


def _safe_field_mapping(layer, event: AuditEvent) -> dict:
    """Build field mapping handling schema drift.

    Delegates to schema_drift.safe_field_mapping (DUP-04 consolidation):
    same algorithm as restore_service.pre_check_restore plus drift,
    but recomputed locally because buffer ops do not run pre_check
    upfront.
    """
    return safe_field_mapping(event, layer=layer)


def _apply_insert_if_already_present(layer, event, attrs, geom, mapping):
    if geom is None:
        return None
    fid = _find_existing_insert_target(layer, event, geom)
    if fid is None:
        return None
    result = _apply_snapshot_to_existing(layer, fid, attrs, geom, mapping)
    if result["success"]:
        flog(f"_buffer_insert: already present, snapshot applied "
             f"eid={event.event_id} fid={fid}")
    return result


def _find_existing_insert_target(layer, event, geom):
    identity_fid = _find_existing_by_identity(layer, event)
    if identity_fid is not None:
        return identity_fid
    return _find_existing_by_snapshot(layer, geom)


def _find_existing_by_identity(layer, event):
    from .restore_service import _parse_identity, _find_target_feature

    identity = _parse_identity(event.feature_identity_json)
    has_pk = bool(identity.get("pk_field")) and identity.get("pk_value") is not None
    if not has_pk:
        return None
    try:
        return _find_target_feature(layer, identity)
    except Exception as exc:
        flog(f"_buffer_insert: identity lookup failed "
             f"eid={event.event_id}: {exc}", "WARNING")
        return None


def _find_existing_by_snapshot(layer, geom):
    """Find a feature already present whose geometry matches.

    Used for INSERT idempotence and UPDATE post-state recovery. Geometry
    match is the primary signal: spatial duplicates within the same bbox
    with identical geometry are highly likely to be the same logical
    feature re-inserted by a previous restore. Strict attribute equality
    on top tends to cause false negatives (QVariant NULL vs None,
    trigger-altered fields, datetime serialization), which then leads to
    duplicate inserts on repeated rewinds.
    """
    try:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest()
        if not hasattr(request, "setFilterRect") or not hasattr(geom, "boundingBox"):
            return None
        request.setFilterRect(geom.boundingBox())
        source = get_feature_source(layer)
        for feature in source(request):
            if _feature_geometry_matches(feature, geom):
                return feature.id()
    except Exception as exc:
        flog(f"_buffer_insert: snapshot lookup failed: {exc}", "WARNING")
    return None


def _find_update_target_by_post_state(layer, event):
    new_wkb = getattr(event, "new_geometry_wkb", None)
    if new_wkb is None:
        return None
    geom = rebuild_geometry(new_wkb)
    if not is_geometry_present(geom):
        return None
    return _find_existing_by_snapshot(layer, geom)


def _target_matches_update_post_state(layer, fid: int, event) -> bool:
    """Return True when feature *fid* still carries the event's NEW geometry.

    For UPDATE events captured with a post-edit geometry, the feature at
    *fid* should currently hold that NEW geometry. A mismatch indicates
    the FID has been reassigned by the provider (typical on shapefiles
    after compaction) and points at an unrelated feature.

    Backward compatibility: when no NEW was captured (legacy event with
    new_geometry_wkb=None), the check is skipped (returns True) so the
    historical behaviour is preserved.
    """
    new_wkb = getattr(event, "new_geometry_wkb", None)
    if new_wkb is None:
        return True
    geom = rebuild_geometry(new_wkb)
    if not is_geometry_present(geom):
        return True
    return _current_geom_matches(layer, fid, geom)


def _feature_geometry_matches(feature, expected_geom) -> bool:
    """Local alias kept for in-module call sites; delegates to
    geometry_utils.feature_matches_geometry (DUP-10 consolidation)."""
    return feature_matches_geometry(feature, expected_geom)


def _apply_snapshot_to_existing(layer, fid, attrs, geom, mapping) -> dict:
    fields = layer.fields()
    attr_fail = 0
    for idx, value in iter_mapped_attributes(mapping, attrs, fields):
        if not layer.changeAttributeValue(fid, idx, value):
            attr_fail += 1
    if not layer.changeGeometry(fid, geom):
        return {"success": False, "message": "Existing feature geometry update failed"}
    if attr_fail > 0:
        return {"success": False, "message": "Existing feature attribute update failed"}
    return {"success": True, "message": "Already present", "fid": fid}


def _current_geom_matches(layer, fid: int, expected_geom) -> bool:
    try:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(fid)
        source = get_feature_source(layer)
        for feature in source(request):
            return _feature_geometry_matches(feature, expected_geom)
    except Exception as exc:
        flog(f"_buffer_update: current geometry lookup failed "
             f"fid={fid}: {exc}", "WARNING")
    return False
