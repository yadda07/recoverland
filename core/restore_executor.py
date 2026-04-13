"""Restore executor for RecoverLand (BL-04).

Applies a RestorePlan to a QGIS layer. Supports strict_atomic
(editing buffer + rollback) and best_effort (per-entity isolation).

Must run on main thread only.
"""
from typing import Dict, List

from .audit_backend import AuditEvent, RestoreReport
from .restore_contracts import (
    RestorePlan, AtomicityPolicy, PreflightVerdict, COMPENSATORY_OPS,
    RestoreSession,
)
from .restore_planner import preflight_check
from .restore_service import restore_batch
from .search_service import reconstruct_attributes
from .schema_drift import (
    parse_field_schema, extract_current_schema,
    compare_schemas, build_field_mapping,
)
from .geometry_utils import rebuild_geometry
from .serialization import is_layer_audit_field
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
        return _execute_strict(plan, events_by_id, layer)
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
) -> RestoreReport:
    """All-or-nothing via QGIS editing buffer."""
    was_editing = layer.isEditable()
    if not was_editing:
        if not layer.startEditing():
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
        layer.destroyEditCommand()
        if not was_editing:
            layer.rollBack()
        flog(f"restore_executor: strict rollback, {len(failed)} failures", "WARNING")
        return RestoreReport([], failed, plan.event_count)

    layer.endEditCommand()
    if not was_editing:
        if not layer.commitChanges():
            errors = layer.commitErrors()
            msg = "; ".join(errors) if errors else "Commit failed"
            layer.rollBack()
            return RestoreReport([], {0: msg}, plan.event_count)

    return RestoreReport(succeeded, {}, plan.event_count)


def _apply_via_buffer(layer, compensatory_op: str, event: AuditEvent) -> dict:
    """Apply a single compensatory action via the layer editing buffer."""
    if compensatory_op == "INSERT":
        return _buffer_insert(layer, event)
    if compensatory_op == "DELETE":
        return _buffer_delete(layer, event)
    if compensatory_op == "UPDATE":
        return _buffer_update(layer, event)
    return {"success": False, "message": f"Unknown operation: {compensatory_op}"}


def _buffer_insert(layer, event: AuditEvent) -> dict:
    """Re-insert a deleted feature via editing buffer."""
    from qgis.core import QgsFeature

    attrs = reconstruct_attributes(event)
    geom = rebuild_geometry(event.geometry_wkb)
    mapping = _safe_field_mapping(layer, event)

    feature = QgsFeature(layer.fields())
    fields = layer.fields()
    for hist_name, curr_name in mapping.items():
        if hist_name not in attrs or is_layer_audit_field(hist_name):
            continue
        idx = fields.indexOf(curr_name)
        if idx >= 0:
            feature.setAttribute(idx, attrs[hist_name])

    if geom is not None:
        feature.setGeometry(geom)

    if not layer.addFeature(feature):
        return {"success": False, "message": "Buffer addFeature failed"}
    return {"success": True, "message": "Inserted via buffer"}


def _buffer_delete(layer, event: AuditEvent) -> dict:
    """Delete an inserted feature via editing buffer."""
    from .restore_service import _parse_identity, _find_target_feature

    identity = _parse_identity(event.feature_identity_json)
    target_fid = _find_target_feature(layer, identity)
    if target_fid is None:
        return {"success": False, "message": "Target feature not found"}

    if not layer.deleteFeature(target_fid):
        return {"success": False, "message": "Buffer deleteFeature failed"}
    return {"success": True, "message": "Deleted via buffer"}


def _buffer_update(layer, event: AuditEvent) -> dict:
    """Revert attributes and geometry via editing buffer."""
    from .restore_service import _parse_identity, _find_target_feature

    identity = _parse_identity(event.feature_identity_json)
    target_fid = _find_target_feature(layer, identity)
    if target_fid is None:
        return {"success": False, "message": "Target feature not found"}

    old_attrs = reconstruct_attributes(event)
    mapping = _safe_field_mapping(layer, event)
    fields = layer.fields()

    for hist_name, curr_name in mapping.items():
        if hist_name not in old_attrs or is_layer_audit_field(hist_name):
            continue
        idx = fields.indexOf(curr_name)
        if idx >= 0:
            layer.changeAttributeValue(target_fid, idx, old_attrs[hist_name])

    if event.geometry_wkb is not None:
        geom = rebuild_geometry(event.geometry_wkb)
        if geom is not None:
            layer.changeGeometry(target_fid, geom)

    return {"success": True, "message": "Reverted via buffer"}


def _safe_field_mapping(layer, event: AuditEvent) -> dict:
    """Build field mapping handling schema drift."""
    hist_schema = parse_field_schema(event.field_schema_json)
    curr_schema = extract_current_schema(layer)
    drift = compare_schemas(hist_schema, curr_schema)
    return build_field_mapping(drift, hist_schema)
