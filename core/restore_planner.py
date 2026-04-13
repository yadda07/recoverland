"""Restore planner for RecoverLand time-travel restore (BL-03).

Builds RestorePlan from events using contracts defined in
restore_contracts.py. Supports event-based (Mode A) and
temporal (Mode B) restoration.

No QGIS dependency. All data comes via AuditEvent.
"""
from typing import List, Optional

from .audit_backend import AuditEvent
from .restore_contracts import (
    RestoreMode, RestoreScope, CutoffType, RestoreCutoff, ConflictPolicy,
    AtomicityPolicy, PlannedAction, Conflict, RestorePlan,
    PreflightReport, PreflightVerdict,
    COMPENSATORY_OPS, check_volume_limits, validate_cutoff,
)


def plan_event_restore(
    events: List[AuditEvent],
    datasource_fp: str,
    layer_name: Optional[str],
    conflict_policy: ConflictPolicy = ConflictPolicy.SKIP,
) -> RestorePlan:
    """Build a restore plan for explicitly selected events (Mode A)."""
    actions: List[PlannedAction] = []
    conflicts: List[Conflict] = []
    entities: set = set()

    for event in events:
        action, conflict = _build_action(event, require_fingerprint=False)
        if conflict is not None:
            conflicts.append(conflict)
        if action is not None:
            entities.add(event.entity_fingerprint or f"eid_{event.event_id}")
            actions.append(action)

    return RestorePlan(
        mode=RestoreMode.EVENT,
        scope=RestoreScope.SELECTION,
        cutoff=None,
        atomicity=AtomicityPolicy.BEST_EFFORT,
        conflict_policy=conflict_policy,
        actions=actions,
        conflicts=conflicts,
        entity_count=len(entities),
        event_count=len(actions),
        datasource_fingerprint=datasource_fp,
        layer_name=layer_name,
    )


def plan_temporal_restore(
    events_after_cutoff: List[AuditEvent],
    datasource_fp: str,
    layer_name: Optional[str],
    cutoff: RestoreCutoff,
    scope: RestoreScope = RestoreScope.LAYER,
    conflict_policy: ConflictPolicy = ConflictPolicy.ABORT,
) -> RestorePlan:
    """Build a reverse-replay plan from events after cutoff (Mode B).

    Events must be ordered by event_id DESC (most recent first).
    """
    actions: List[PlannedAction] = []
    conflicts: List[Conflict] = []
    entities: set = set()

    for event in events_after_cutoff:
        action, conflict = _build_action(event, require_fingerprint=True)
        if conflict is not None:
            conflicts.append(conflict)
            if conflict.severity == "blocking":
                continue
        if action is not None:
            entities.add(event.entity_fingerprint)
            actions.append(action)

    return RestorePlan(
        mode=RestoreMode.TEMPORAL,
        scope=scope,
        cutoff=cutoff,
        atomicity=AtomicityPolicy.STRICT,
        conflict_policy=conflict_policy,
        actions=actions,
        conflicts=conflicts,
        entity_count=len(entities),
        event_count=len(actions),
        datasource_fingerprint=datasource_fp,
        layer_name=layer_name,
    )


def check_retention_coverage(
    cutoff: RestoreCutoff, oldest_event_date: Optional[str],
) -> Optional[str]:
    """Check if the cutoff falls within the retained history.

    Returns a blocking reason string if history was purged before cutoff,
    or None if OK. Only meaningful for BY_DATE cutoffs.
    """
    if cutoff.cutoff_type != CutoffType.BY_DATE:
        return None
    if oldest_event_date is None:
        return "No events found for this datasource"
    if isinstance(cutoff.value, str) and cutoff.value < oldest_event_date:
        return (
            f"History purged before this date. "
            f"Oldest available: {oldest_event_date}, requested: {cutoff.value}"
        )
    return None


def preflight_check(plan: RestorePlan) -> PreflightReport:
    """Validate a restore plan and produce a preflight report."""
    blocking: List[str] = []
    warnings: List[str] = []

    vol_ok, vol_warnings, vol_blocking = check_volume_limits(
        plan.event_count, plan.entity_count,
    )
    warnings.extend(vol_warnings)
    blocking.extend(vol_blocking)

    if plan.cutoff is not None:
        cutoff_err = validate_cutoff(plan.cutoff)
        if cutoff_err:
            blocking.append(f"Invalid cutoff: {cutoff_err}")

    for conflict in plan.conflicts:
        if conflict.severity == "blocking":
            blocking.append(f"Event {conflict.event_id}: {conflict.reason}")
        else:
            warnings.append(f"Event {conflict.event_id}: {conflict.reason}")

    if not plan.actions:
        blocking.append("No actions in plan")

    if plan.mode == RestoreMode.TEMPORAL:
        if plan.atomicity != AtomicityPolicy.STRICT:
            blocking.append("Temporal mode requires STRICT atomicity")

    verdict = PreflightVerdict.BLOCKED if blocking else (
        PreflightVerdict.GO_WITH_WARNINGS if warnings else PreflightVerdict.GO
    )

    return PreflightReport(
        verdict=verdict,
        plan=plan,
        blocking_reasons=blocking,
        warnings=warnings,
        estimated_duration_ms=None,
    )


def _build_action(
    event: AuditEvent, require_fingerprint: bool,
) -> tuple:
    """Build a PlannedAction from an AuditEvent.

    Returns (action_or_None, conflict_or_None).
    """
    eid = event.event_id or 0
    comp_op = COMPENSATORY_OPS.get(event.operation_type)
    if comp_op is None:
        return None, Conflict(
            event_id=eid,
            reason=f"unsupported_operation: {event.operation_type}",
            severity="blocking",
            details=None,
        )

    conflict = None
    if not event.entity_fingerprint:
        severity = "blocking" if require_fingerprint else "warning"
        details = (
            "Temporal restore requires stable identity"
            if require_fingerprint
            else "FID-based fallback will be used"
        )
        conflict = Conflict(
            event_id=eid,
            reason="missing_entity_fingerprint",
            severity=severity,
            details=details,
        )
        if require_fingerprint:
            return None, conflict

    action = PlannedAction(
        event_id=eid,
        operation_type=event.operation_type,
        compensatory_op=comp_op,
        entity_fingerprint=event.entity_fingerprint,
        datasource_fingerprint=event.datasource_fingerprint,
        layer_name=event.layer_name_snapshot,
        has_geometry=event.geometry_wkb is not None,
        has_attribute_changes=True,
    )
    return action, conflict
