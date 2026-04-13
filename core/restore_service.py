"""Local restore service for RecoverLand (RLU-040 to RLU-043).

Restores audited features back into their source layers.
Handles DELETE restore (re-insert), UPDATE restore (revert attributes/geometry),
conflict detection, and batch operations with per-entity isolation.
"""
import json
from typing import List, Dict, Optional, NamedTuple, Any

from .audit_backend import AuditEvent, RestoreReport
from .schema_drift import (
    parse_field_schema, extract_current_schema,
    compare_schemas, build_field_mapping, DriftReport,
)
from .search_service import reconstruct_attributes, reconstruct_new_attributes
from .geometry_utils import rebuild_geometry
from .identity import get_identity_strength_for_layer
from .support_policy import IdentityStrength
from .serialization import deserialize_value, is_layer_audit_field
from .logger import flog
from ..compat import QgisCompat


class PreCheckResult(NamedTuple):
    can_restore: bool
    reason: str
    drift: Optional[DriftReport]


def pre_check_restore(layer, event: AuditEvent) -> PreCheckResult:
    """Validate that a restore operation is feasible on the target layer."""
    if layer is None:
        return PreCheckResult(False, "Target layer not found", None)

    provider = layer.dataProvider()
    if provider is None:
        return PreCheckResult(False, "No data provider", None)

    if not _can_write(provider, event.operation_type):
        return PreCheckResult(False, "Provider lacks write capability", None)

    hist_schema = parse_field_schema(event.field_schema_json)
    curr_schema = extract_current_schema(layer)
    drift = compare_schemas(hist_schema, curr_schema)

    if not drift.is_compatible and drift.missing_in_current:
        reason = f"Schema drift: missing fields {drift.missing_in_current}"
        return PreCheckResult(False, reason, drift)

    return PreCheckResult(True, "OK", drift)


def restore_deleted_feature(layer, event: AuditEvent) -> Dict[str, Any]:
    """Re-insert a deleted feature into the target layer.

    Returns dict with 'success', 'message', and optionally 'fid'.
    """
    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    attrs = reconstruct_attributes(event)
    geom = rebuild_geometry(event.geometry_wkb)
    field_mapping = _build_safe_mapping(check.drift, event)

    from qgis.core import QgsFeature, QgsFields
    new_feature = QgsFeature(layer.fields())
    _apply_attributes(new_feature, layer.fields(), attrs, field_mapping)

    if geom is not None:
        new_feature.setGeometry(geom)

    provider = layer.dataProvider()
    success, added = provider.addFeatures([new_feature])
    if not success:
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Insert failed"
        return {"success": False, "message": msg}

    new_fid = added[0].id() if added else None
    return {"success": True, "message": "Restored", "fid": new_fid}


def restore_inserted_feature(layer, event: AuditEvent,
                              fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Undo an INSERT by deleting the inserted feature from the target layer."""
    if layer is None:
        return {"success": False, "message": "Target layer not found"}

    provider = layer.dataProvider()
    if provider is None:
        return {"success": False, "message": "No data provider"}

    if not bool(provider.capabilities() & QgisCompat.CAP_DELETE_FEATURES):
        return {"success": False, "message": "Provider lacks delete capability"}

    identity = _parse_identity(event.feature_identity_json)
    strength = get_identity_strength_for_layer(layer)

    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity for restore"}

    target_fid = _find_target_feature(layer, identity, fid_cache)
    if target_fid is None:
        return {"success": False, "message": "Target feature not found"}

    if not provider.deleteFeatures([target_fid]):
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Delete failed"
        return {"success": False, "message": msg}

    return {"success": True, "message": "Deleted (undo insert)"}


def restore_updated_feature(layer, event: AuditEvent,
                             fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Revert a modified feature to its pre-update state.

    Uses the old values from the delta to update the current feature.
    """
    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    identity = _parse_identity(event.feature_identity_json)
    strength = get_identity_strength_for_layer(layer)

    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity for restore"}

    target_fid = _find_target_feature(layer, identity, fid_cache)
    if target_fid is None:
        return {"success": False, "message": "Target feature not found"}

    old_attrs = reconstruct_attributes(event)
    field_mapping = _build_safe_mapping(check.drift, event)
    attr_changes = _build_attribute_changes(layer, target_fid, old_attrs, field_mapping)

    provider = layer.dataProvider()
    if attr_changes:
        success = provider.changeAttributeValues(attr_changes)
        if not success:
            return {"success": False, "message": "Attribute update failed"}

    if event.geometry_wkb is not None:
        if not bool(provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
            return {"success": False, "message": "Provider lacks geometry change capability"}
        geom = rebuild_geometry(event.geometry_wkb)
        if geom is not None:
            geom_changes = {target_fid: geom}
            if not provider.changeGeometryValues(geom_changes):
                return {"success": False, "message": "Geometry update failed"}

    return {"success": True, "message": "Reverted"}


def validate_restore_layer_state(layer) -> Optional[str]:
    if layer is None:
        return "Target layer not found"
    if hasattr(layer, 'isEditable') and layer.isEditable():
        return "Target layer has uncommitted edits; commit or rollback before restore"
    return None


def build_fid_cache(layer, events: List[AuditEvent]) -> Dict:
    """Pre-resolve PK-based identities to FIDs in one batched query per PK field.

    Returns dict mapping (pk_field, pk_value_str) -> fid.
    Events without PK identity are skipped (fallback to per-event lookup).
    """
    from qgis.core import QgsFeatureRequest, QgsExpression
    from collections import defaultdict

    cache: Dict = {}
    pk_groups: Dict[str, list] = defaultdict(list)

    for event in events:
        if event.operation_type == "DELETE":
            continue
        identity = _parse_identity(event.feature_identity_json)
        pk_field = identity.get("pk_field")
        pk_value = identity.get("pk_value")
        if pk_field and pk_value is not None:
            pk_groups[pk_field].append(pk_value)

    provider = layer.dataProvider()
    for pk_field, pk_values in pk_groups.items():
        unique_values = list(set(str(v) for v in pk_values))
        if not unique_values:
            continue
        escaped_field = QgsExpression.quotedColumnRef(str(pk_field))
        value_list = ", ".join(QgsExpression.quotedValue(v) for v in unique_values)
        expr = QgsExpression(f"{escaped_field} IN ({value_list})")
        if expr.hasParserError():
            flog(f"build_fid_cache: parse error for {pk_field}: {expr.parserErrorString()}", "WARNING")
            continue
        field_idx = layer.fields().indexOf(pk_field)
        request = QgsFeatureRequest(expr)
        if field_idx >= 0:
            request.setSubsetOfAttributes([field_idx])
        request.setFlags(QgsFeatureRequest.NoGeometry)
        for feat in provider.getFeatures(request):
            cache[(pk_field, str(feat[pk_field]))] = feat.id()

    return cache


def restore_batch(layer, events: List[AuditEvent],
                   fid_cache: Optional[Dict] = None) -> RestoreReport:
    """Restore a batch of events with per-entity error isolation."""
    layer_error = validate_restore_layer_state(layer)
    if layer_error:
        failed = {(e.event_id or 0): layer_error for e in events}
        return RestoreReport([], failed, len(events), ())
    if fid_cache is None:
        fid_cache = build_fid_cache(layer, events)
    succeeded = []
    failed = {}
    traces = []

    for event in events:
        eid = event.event_id or 0
        try:
            if event.operation_type == "DELETE":
                result = restore_deleted_feature(layer, event)
            elif event.operation_type == "UPDATE":
                result = restore_updated_feature(layer, event, fid_cache)
            elif event.operation_type == "INSERT":
                result = restore_inserted_feature(layer, event, fid_cache)
            else:
                result = {"success": False, "message": f"Unsupported: {event.operation_type}"}

            if result["success"]:
                succeeded.append(eid)
                trace = build_restore_trace_event(event, layer)
                if trace is not None:
                    traces.append(trace)
            else:
                failed[eid] = result["message"]
        except Exception as e:
            failed[eid] = str(e)
            flog(f"restore_batch: error on event {eid}: {e}", "ERROR")

    return RestoreReport(
        succeeded=succeeded,
        failed=failed,
        total_requested=len(events),
        trace_events=tuple(traces),
    )


def undo_restore_batch(layer, events: List[AuditEvent]) -> RestoreReport:
    """Undo a previous restore by reversing each operation.

    UPDATE  -> re-apply post-edit ('new') attribute values + new_geometry_wkb
    DELETE  -> delete the feature that was re-inserted by the restore
    INSERT  -> re-insert the feature that was deleted by the restore
    """
    layer_error = validate_restore_layer_state(layer)
    if layer_error:
        failed = {(e.event_id or 0): layer_error for e in events}
        return RestoreReport([], failed, len(events), ())
    succeeded = []
    failed = {}

    for event in events:
        eid = event.event_id or 0
        try:
            if event.operation_type == "UPDATE":
                result = _undo_update_restore(layer, event)
            elif event.operation_type == "DELETE":
                result = restore_inserted_feature(layer, event)
            elif event.operation_type == "INSERT":
                result = _undo_insert_restore(layer, event)
            else:
                result = {"success": False, "message": f"Unsupported: {event.operation_type}"}

            if result["success"]:
                succeeded.append(eid)
            else:
                failed[eid] = result["message"]
        except Exception as e:
            failed[eid] = str(e)
            flog(f"undo_restore_batch: error on event {eid}: {e}", "ERROR")

    return RestoreReport(
        succeeded=succeeded,
        failed=failed,
        total_requested=len(events),
        trace_events=(),
    )


def _undo_update_restore(layer, event: AuditEvent,
                          fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Re-apply post-edit values to reverse a previous UPDATE restore."""
    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    identity = _parse_identity(event.feature_identity_json)
    strength = get_identity_strength_for_layer(layer)
    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity"}

    target_fid = _find_target_feature(layer, identity, fid_cache)
    if target_fid is None:
        return {"success": False, "message": "Target feature not found"}

    new_attrs = reconstruct_new_attributes(event)
    field_mapping = _build_safe_mapping(check.drift, event)
    attr_changes = _build_attribute_changes(layer, target_fid, new_attrs, field_mapping)

    provider = layer.dataProvider()
    if attr_changes:
        if not provider.changeAttributeValues(attr_changes):
            return {"success": False, "message": "Attribute update failed"}

    new_geom_wkb = getattr(event, 'new_geometry_wkb', None)
    if new_geom_wkb is not None:
        if not bool(provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
            return {"success": False, "message": "Provider lacks geometry change capability"}
        geom = rebuild_geometry(new_geom_wkb)
        if geom is not None:
            if not provider.changeGeometryValues({target_fid: geom}):
                return {"success": False, "message": "Geometry update failed"}

    return {"success": True, "message": "Undo reverted"}


def _undo_insert_restore(layer, event: AuditEvent) -> Dict[str, Any]:
    """Re-insert a feature that was deleted by a previous INSERT restore."""
    provider = layer.dataProvider()
    if provider is None:
        return {"success": False, "message": "No data provider"}
    if not bool(provider.capabilities() & QgisCompat.CAP_ADD_FEATURES):
        return {"success": False, "message": "Provider lacks add capability"}

    attrs = reconstruct_attributes(event)
    geom_wkb = getattr(event, 'new_geometry_wkb', None) or event.geometry_wkb
    geom = rebuild_geometry(geom_wkb)
    check = pre_check_restore(layer, event)
    field_mapping = _build_safe_mapping(check.drift if check else None, event)

    from qgis.core import QgsFeature
    new_feature = QgsFeature(layer.fields())
    _apply_attributes(new_feature, layer.fields(), attrs, field_mapping)
    if geom is not None:
        new_feature.setGeometry(geom)

    success, _ = provider.addFeatures([new_feature])
    if not success:
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Insert failed"
        return {"success": False, "message": msg}

    return {"success": True, "message": "Re-inserted (undo delete)"}


def build_restore_trace_event(source_event: AuditEvent, layer) -> Optional[AuditEvent]:
    """Build an audit event that traces a restore operation (RLU-044).

    The trace event records that a restore was performed, referencing
    the original event via restored_from_event_id.
    """
    from datetime import datetime, timezone
    from .identity import (
        compute_datasource_fingerprint, compute_project_fingerprint,
        extract_layer_name, compute_entity_fingerprint,
    )
    from .user_identity import resolve_user_name
    from .sqlite_schema import CURRENT_SCHEMA_VERSION

    if source_event.event_id is None:
        return None

    now = datetime.now(timezone.utc).isoformat()
    _UNDO_OP = {"DELETE": "INSERT", "UPDATE": "UPDATE", "INSERT": "DELETE"}
    restore_op = _UNDO_OP.get(source_event.operation_type, "UPDATE")
    ref_json = json.dumps({"_restore_ref": source_event.event_id})

    return AuditEvent(
        event_id=None,
        project_fingerprint=compute_project_fingerprint(),
        datasource_fingerprint=compute_datasource_fingerprint(layer),
        layer_id_snapshot=layer.id(),
        layer_name_snapshot=extract_layer_name(layer),
        provider_type=layer.dataProvider().name(),
        feature_identity_json=source_event.feature_identity_json,
        operation_type=restore_op,
        attributes_json=ref_json,
        geometry_wkb=None,
        geometry_type=source_event.geometry_type,
        crs_authid=source_event.crs_authid,
        field_schema_json=None,
        user_name=resolve_user_name(),
        session_id=None,
        created_at=now,
        restored_from_event_id=source_event.event_id,
        entity_fingerprint=compute_entity_fingerprint(source_event.feature_identity_json),
        event_schema_version=CURRENT_SCHEMA_VERSION,
    )


def _can_write(provider, operation_type: str) -> bool:
    caps = provider.capabilities()
    if operation_type == "DELETE":
        return bool(caps & QgisCompat.CAP_ADD_FEATURES)
    if operation_type == "UPDATE":
        return bool(caps & QgisCompat.CAP_CHANGE_ATTRIBUTE_VALUES)
    if operation_type == "INSERT":
        return bool(caps & QgisCompat.CAP_DELETE_FEATURES)
    return False


def _parse_identity(identity_json: str) -> Dict[str, Any]:
    try:
        return json.loads(identity_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _find_target_feature(layer, identity: Dict,
                          fid_cache: Optional[Dict] = None) -> Optional[int]:
    """Find the FID of the target feature using identity info.

    If fid_cache is provided, checks it first to avoid redundant provider queries.
    """
    from qgis.core import QgsFeatureRequest, QgsExpression

    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")

    if fid_cache is not None and pk_field and pk_value is not None:
        cached = fid_cache.get((pk_field, str(pk_value)))
        if cached is not None:
            return cached

    provider = layer.dataProvider()
    if pk_field and pk_value is not None:
        escaped_field = QgsExpression.quotedColumnRef(str(pk_field))
        escaped_value = QgsExpression.quotedValue(pk_value)
        expr = QgsExpression(f"{escaped_field} = {escaped_value}")
        if expr.hasParserError():
            flog(f"restore: invalid PK expression for field={pk_field} "
                 f"value={pk_value!r}: {expr.parserErrorString()}", "WARNING")
        else:
            request = QgsFeatureRequest(expr).setLimit(1)
            for feat in provider.getFeatures(request):
                return feat.id()

    fid = identity.get("fid")
    if fid is not None:
        request = QgsFeatureRequest(fid)
        for feat in provider.getFeatures(request):
            return feat.id()

    return None


def _build_safe_mapping(drift: Optional[DriftReport], event: AuditEvent) -> Dict[str, str]:
    if drift is None:
        hist = parse_field_schema(event.field_schema_json)
        return {f.name: f.name for f in hist}
    return build_field_mapping(drift, parse_field_schema(event.field_schema_json))


def _apply_attributes(feature, fields, attrs: Dict, mapping: Dict) -> None:
    """Apply restored attributes to a QgsFeature using the field mapping."""
    for hist_name, curr_name in mapping.items():
        if hist_name not in attrs:
            continue
        if is_layer_audit_field(hist_name):
            continue
        idx = fields.indexOf(curr_name)
        if idx < 0:
            continue
        feature.setAttribute(idx, attrs[hist_name])


def _build_attribute_changes(layer, fid: int, old_attrs: Dict,
                              mapping: Dict) -> Dict[int, Dict[int, Any]]:
    """Build the change dict for provider.changeAttributeValues()."""
    fields = layer.fields()
    changes = {}
    for hist_name, curr_name in mapping.items():
        if hist_name not in old_attrs:
            continue
        if is_layer_audit_field(hist_name):
            continue
        idx = fields.indexOf(curr_name)
        if idx < 0:
            continue
        changes[idx] = old_attrs[hist_name]

    if not changes:
        return {}
    return {fid: changes}
