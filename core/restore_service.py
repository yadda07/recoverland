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
    compare_schemas, DriftReport, safe_field_mapping,
)
from .search_service import reconstruct_attributes, reconstruct_new_attributes
from .geometry_utils import (
    rebuild_geometry, is_geometry_present,
    feature_matches_geometry, get_feature_source,
)
from .identity import get_identity_strength_for_layer
from .support_policy import IdentityStrength
from .serialization import is_layer_audit_field, iter_mapped_attributes
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
    eid = event.event_id or 0
    check = pre_check_restore(layer, event)
    if not check.can_restore:
        flog(f"restore_deleted[{eid}]: pre_check failed: {check.reason}", "WARNING")
        return {"success": False, "message": check.reason}

    identity = _parse_identity(event.feature_identity_json)
    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")
    if pk_field and pk_value is not None:
        existing_fid = _find_target_feature(layer, identity)
        if existing_fid is not None:
            flog(f"restore_deleted[{eid}]: skip, {pk_field}={pk_value} already exists fid={existing_fid}")
            return {"success": True, "message": "Already exists (skip)", "fid": existing_fid}

    attrs = reconstruct_attributes(event)
    geom = rebuild_geometry(event.geometry_wkb)
    field_mapping = _build_safe_mapping(check.drift, event)
    geom_str = 'yes' if geom else 'no'
    flog(
        f"restore_deleted[{eid}]: attrs={len(attrs)} keys,"
        f" geom={geom_str}, mapping={len(field_mapping)} fields"
    )

    from qgis.core import QgsFeature
    new_feature = QgsFeature(layer.fields())
    _apply_attributes(new_feature, layer.fields(), attrs, field_mapping)

    if geom is not None:
        new_feature.setGeometry(geom)

    provider = layer.dataProvider()
    success, added = provider.addFeatures([new_feature])
    if not success:
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Insert failed"
        flog(f"restore_deleted[{eid}]: addFeatures failed: {msg}", "ERROR")
        return {"success": False, "message": msg}

    new_fid = added[0].id() if added else None
    flog(f"restore_deleted[{eid}]: OK new_fid={new_fid}")
    return {"success": True, "message": "Restored", "fid": new_fid}


def restore_inserted_feature(layer, event: AuditEvent,
                             fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Undo an INSERT by deleting the inserted feature from the target layer.

    Lookup strategy for layers without a stable PK:
      1. Try the FID stored in the identity. Verify it matches the captured
         snapshot (geom + attrs). If yes, delete it.
      2. Otherwise, scan the layer for a feature that exactly matches the
         snapshot. This is needed when the provider has reused the original
         FID for another feature (shapefile recycles), or when the
         re-inserted feature received a fresh FID at commit time.
      3. Refuse only when neither path yields a confidently-identified
         target. This avoids the silent feature accumulation observed
         when repeated rewinds re-create the same feature each time.
    """
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

    has_pk = bool(identity.get("pk_field")) and identity.get("pk_value") is not None
    target_fid = _find_target_feature(layer, identity, fid_cache)

    if has_pk:
        if target_fid is None:
            return {"success": False, "message": "Target feature not found"}
        return _delete_target_or_fail(provider, target_fid, event)

    if target_fid is not None and _verify_insert_snapshot(
            layer, target_fid, event):
        flog(f"restore_inserted_feature: matched by FID and snapshot "
             f"eid={event.event_id} fid={target_fid}")
        return _delete_target_or_fail(provider, target_fid, event)

    fallback_fid = _find_by_snapshot(layer, event, resolve_ambiguity=True)
    if fallback_fid is not None:
        flog(f"restore_inserted_feature: FID mismatch, recovered by "
             f"snapshot scan eid={event.event_id} "
             f"identity_fid={identity.get('fid')} "
             f"recovered_fid={fallback_fid}")
        return _delete_target_or_fail(provider, fallback_fid, event)

    flog(f"restore_inserted_feature: target absent (FID lookup miss and "
         f"no snapshot match) eid={event.event_id} "
         f"identity_fid={identity.get('fid')}", "WARNING")
    _diagnose_snapshot_miss(layer, event)
    return {"success": True, "message": "Already absent (no snapshot match)",
            "skipped": True}


def _delete_target_or_fail(provider, target_fid: int,
                           event: AuditEvent) -> Dict[str, Any]:
    if not provider.deleteFeatures([target_fid]):
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Delete failed"
        flog(f"restore_inserted_feature: deleteFeatures failed "
             f"eid={event.event_id} fid={target_fid} msg={msg}", "ERROR")
        return {"success": False, "message": msg}
    return {"success": True, "message": "Deleted (undo insert)",
            "fid": target_fid}


def _find_by_snapshot(
    layer, event: AuditEvent,
    resolve_ambiguity: bool = False,
) -> Optional[int]:
    """Scan the layer for the single feature that matches event's snapshot.

    When *resolve_ambiguity* is False (default): returns the FID of the
    unique match, or None when no feature matches or several do (ambiguous:
    refuse to act blindly).

    When *resolve_ambiguity* is True: in case of multiple strict matches
    (geometry + every captured attribute identical), returns the highest
    FID, which is the most recently inserted feature and therefore the
    duplicate created by a previous rewind. The risk of destroying
    unrelated data is bounded because matches are strictly identical on
    geometry AND every attribute, so the kept feature carries the same
    semantics as the deleted one.

    Strict matching is used because callers may use the result for a
    *destructive* operation (undo of a previous insert).
    """
    from qgis.core import QgsFeatureRequest

    expected_geom = rebuild_geometry(event.geometry_wkb) \
        if event.geometry_wkb is not None else None
    expected_attrs = reconstruct_attributes(event)
    fields = layer.fields()
    relevant = [
        (hist_name, fields.indexOf(hist_name))
        for hist_name in expected_attrs.keys()
        if not is_layer_audit_field(hist_name)
        and fields.indexOf(hist_name) >= 0
    ]

    matches: List[int] = []
    request = QgsFeatureRequest()
    if expected_geom is not None and hasattr(request, "setFilterRect") \
            and hasattr(expected_geom, "boundingBox"):
        request.setFilterRect(expected_geom.boundingBox())
    source = get_feature_source(layer)
    try:
        for feature in source(request):
            if expected_geom is not None and not feature_matches_geometry(
                    feature, expected_geom):
                continue
            attrs_ok = True
            for hist_name, idx in relevant:
                if feature[idx] != expected_attrs.get(hist_name):
                    attrs_ok = False
                    break
            if attrs_ok:
                matches.append(feature.id())
                if not resolve_ambiguity and len(matches) > 1:
                    break
    except Exception as exc:
        flog(f"_find_by_snapshot: scan failed "
             f"eid={event.event_id}: {exc}", "WARNING")
        return None

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        if resolve_ambiguity:
            chosen = max(matches)
            flog(f"_find_by_snapshot: ambiguity resolved by max-fid heuristic "
                 f"eid={event.event_id} candidates={matches[:5]} chosen={chosen} "
                 f"identity_fid={_identity_fid(event)}", "WARNING")
            return chosen
        flog(f"_find_by_snapshot: ambiguous, {len(matches)} matches found "
             f"eid={event.event_id} fids={matches[:5]}", "WARNING")
    return None


def _diagnose_snapshot_miss(layer, event: AuditEvent) -> None:
    """Log why _find_by_snapshot found 0 strict matches.

    Re-scans the layer using *geometry only* (same bbox prefilter) and,
    for every geom-only candidate, lists the historical attribute keys
    whose persisted value diverges from the captured snapshot. The first
    five candidates and the first five diverging attributes per candidate
    are reported. Side-effect free: only writes WARNING-level log lines.

    Used immediately after a snapshot lookup miss to expose the cause
    (typical: trigger-altered fields, NULL vs None, datetime serialization)
    without changing resolution logic.
    """
    from qgis.core import QgsFeatureRequest

    expected_geom = rebuild_geometry(event.geometry_wkb) \
        if event.geometry_wkb is not None else None
    if not is_geometry_present(expected_geom):
        flog(f"_diagnose_snapshot_miss: eid={event.event_id} no expected "
             f"geom, cannot diagnose", "WARNING")
        return

    expected_attrs = reconstruct_attributes(event)
    fields = layer.fields()
    relevant = [
        (hist_name, fields.indexOf(hist_name))
        for hist_name in expected_attrs.keys()
        if not is_layer_audit_field(hist_name)
        and fields.indexOf(hist_name) >= 0
    ]

    request = QgsFeatureRequest()
    if hasattr(request, "setFilterRect") \
            and hasattr(expected_geom, "boundingBox"):
        request.setFilterRect(expected_geom.boundingBox())
    source = get_feature_source(layer)

    _DIAG_SCAN_CAP = 100
    candidates: List[tuple] = []
    scanned = 0
    try:
        for feature in source(request):
            scanned += 1
            if scanned > _DIAG_SCAN_CAP:
                break
            if not feature_matches_geometry(feature, expected_geom):
                continue
            diffs = []
            for hist_name, idx in relevant:
                actual = feature[idx]
                expected = expected_attrs.get(hist_name)
                if actual != expected:
                    diffs.append(
                        f"{hist_name}: actual={actual!r} expected={expected!r}"
                    )
            candidates.append((feature.id(), diffs))
    except Exception as exc:
        flog(f"_diagnose_snapshot_miss: scan failed "
             f"eid={event.event_id}: {exc}", "WARNING")
        return

    if not candidates:
        flog(f"_diagnose_snapshot_miss: eid={event.event_id} "
             f"0 geom-only candidates in bbox "
             f"identity_fid={_identity_fid(event)}", "WARNING")
        return

    flog(f"_diagnose_snapshot_miss: eid={event.event_id} "
         f"{len(candidates)} geom-only candidate(s) "
         f"identity_fid={_identity_fid(event)}", "WARNING")
    for fid, diffs in candidates[:5]:
        if not diffs:
            flog(f"  fid={fid} attrs all match (audit-field exclusion?)",
                 "WARNING")
        else:
            flog(f"  fid={fid} attr_diffs={diffs[:5]}", "WARNING")


def _identity_fid(event: AuditEvent) -> Optional[int]:
    """Return the historical FID stored in feature_identity_json, or None."""
    try:
        identity = json.loads(event.feature_identity_json or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(identity, dict):
        return None
    fid = identity.get("fid")
    return fid if isinstance(fid, int) else None


def _verify_insert_snapshot(layer, fid: int, event: AuditEvent) -> bool:
    """Return True when feature fid matches the event's INSERT snapshot."""
    from qgis.core import QgsFeatureRequest

    try:
        request = QgsFeatureRequest(fid)
        source = get_feature_source(layer)
        feature = next(iter(source(request)), None)
        if feature is None:
            return False
        if event.geometry_wkb is not None:
            expected = rebuild_geometry(event.geometry_wkb)
            if not feature_matches_geometry(feature, expected):
                return False
        attrs = reconstruct_attributes(event)
        fields = layer.fields()
        for hist_name, value in attrs.items():
            if is_layer_audit_field(hist_name):
                continue
            idx = fields.indexOf(hist_name)
            if idx < 0:
                continue
            if feature[hist_name] != value:
                return False
        return True
    except Exception as exc:
        flog(f"_verify_insert_snapshot failed fid={fid}: {exc}", "WARNING")
        return False


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
    has_geom = event.geometry_wkb is not None
    if has_geom and not bool(provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
        return {"success": False, "message": "Provider lacks geometry change capability"}

    if attr_changes:
        success = provider.changeAttributeValues(attr_changes)
        if not success:
            return {"success": False, "message": "Attribute update failed"}

    if has_geom:
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
        if hasattr(layer, 'isModified') and layer.isModified():
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
        request.setFlags(QgisCompat.NO_GEOMETRY)
        for feat in provider.getFeatures(request):
            cache[(pk_field, str(feat[pk_field]))] = feat.id()

    return cache


# -----------------------------------------------------------------------
# Dispatch tables (BLK-02). Declared lazily via accessor functions because
# the UNDO table references _undo_update_restore / _undo_insert_restore
# which are defined further down. Module-level eager construction would
# work but keeping the lookup behind helper functions also makes the
# intent explicit: all 3 operations must stay covered in both directions.
# -----------------------------------------------------------------------

def _restore_dispatch():
    """Return {operation_type: handler(layer, event, fid_cache) -> dict}."""
    return {
        "DELETE": lambda layer, event, fid_cache:
            restore_deleted_feature(layer, event),
        "UPDATE": restore_updated_feature,
        "INSERT": restore_inserted_feature,
    }


def _undo_dispatch():
    """Return {operation_type: handler(layer, event) -> dict}."""
    return {
        "UPDATE": _undo_update_restore,
        "DELETE": lambda layer, event: restore_inserted_feature(layer, event),
        "INSERT": _undo_insert_restore,
    }


def _unsupported_result(operation_type: str) -> Dict[str, Any]:
    return {"success": False, "message": f"Unsupported: {operation_type}"}


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
    dispatch = _restore_dispatch()

    for event in events:
        eid = event.event_id or 0
        try:
            handler = dispatch.get(event.operation_type)
            if handler is None:
                result = _unsupported_result(event.operation_type)
            else:
                result = handler(layer, event, fid_cache)

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
    dispatch = _undo_dispatch()

    for event in events:
        eid = event.event_id or 0
        try:
            handler = dispatch.get(event.operation_type)
            if handler is None:
                result = _unsupported_result(event.operation_type)
            else:
                result = handler(layer, event)

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

    Lookup priority:
      1. fid_cache (batched PK resolution).
      2. PK-based search (pk_field + pk_value).
      3. Raw FID fallback — only when no PK was captured for this event.

    When a PK was captured at capture time, the FID fallback is intentionally
    refused: FIDs are not stable after commits on many providers (ogr,
    postgres without oid), so a stale FID could address a different feature.
    """
    from qgis.core import QgsFeatureRequest, QgsExpression

    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")
    has_pk = bool(pk_field) and pk_value is not None

    if fid_cache is not None and has_pk:
        cached = fid_cache.get((pk_field, str(pk_value)))
        if cached is not None:
            return cached

    provider = layer.dataProvider()
    if has_pk:
        escaped_field = QgsExpression.quotedColumnRef(str(pk_field))
        escaped_value = QgsExpression.quotedValue(pk_value)
        expr = QgsExpression(f"{escaped_field} = {escaped_value}")
        if expr.hasParserError():
            flog(f"restore: invalid PK expression for field={pk_field} "
                 f"value={pk_value!r}: {expr.parserErrorString()}", "WARNING")
            return None
        request = QgsFeatureRequest(expr).setLimit(1)
        for feat in provider.getFeatures(request):
            return feat.id()
        # PK was expected but not found: do not fall back to FID, which is
        # unstable and could point at an unrelated feature.
        flog(f"restore: PK {pk_field}={pk_value!r} not found; "
             f"refusing FID fallback (safety)", "DEBUG")
        return None

    fid = identity.get("fid")
    if fid is not None:
        request = QgsFeatureRequest(fid)
        for feat in provider.getFeatures(request):
            return feat.id()

    return None


def _build_safe_mapping(drift: Optional[DriftReport], event: AuditEvent) -> Dict[str, str]:
    """Thin shim kept for in-module call sites that already hold a drift.

    Delegates to schema_drift.safe_field_mapping so the mapping logic
    lives in one place across restore_service and restore_executor.
    """
    return safe_field_mapping(event, drift=drift)


def _apply_attributes(feature, fields, attrs: Dict, mapping: Dict) -> None:
    """Apply restored attributes to a QgsFeature using the field mapping."""
    for idx, value in iter_mapped_attributes(mapping, attrs, fields):
        feature.setAttribute(idx, value)


def _build_attribute_changes(layer, fid: int, old_attrs: Dict,
                             mapping: Dict) -> Dict[int, Dict[int, Any]]:
    """Build the change dict for provider.changeAttributeValues()."""
    fields = layer.fields()
    changes = {
        idx: value
        for idx, value in iter_mapped_attributes(mapping, old_attrs, fields)
    }
    if not changes:
        return {}
    return {fid: changes}
