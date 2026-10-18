"""Restore planner for RecoverLand time-travel restore (BL-03).

Builds RestorePlan from events using contracts defined in
restore_contracts.py. Supports event-based (Mode A) and
temporal (Mode B) restoration.

No QGIS dependency. All data comes via AuditEvent.
"""
from typing import List, Optional

from .audit_backend import AuditEvent
from .logger import flog
from .restore_contracts import (
    RestoreMode, RestoreScope, CutoffType, RestoreCutoff, ConflictPolicy,
    AtomicityPolicy, PlannedAction, Conflict, RestorePlan,
    PreflightReport, PreflightVerdict,
    COMPENSATORY_OPS, check_volume_limits, validate_cutoff,
)
from .rewind_dedup import (
    PLAN_MARK_REFUSED, chronological_key, plan_item_kind, refusal_reason,
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

    Events are the collapsed rewind window (rewind_dedup). Their order in
    the list does not matter: the plan orders them itself, on the
    chronology the dedup uses (``rewind_dedup.chronological_key``:
    created_at, then event_id).
    """
    actions: List[PlannedAction] = []
    conflicts: List[Conflict] = []
    entities: set = set()
    chronology: dict = {}

    for event in events_after_cutoff:
        action, conflict = _build_action(event, require_fingerprint=True)
        if conflict is not None:
            conflicts.append(conflict)
            if conflict.severity == "blocking":
                continue
        if action is not None:
            entities.add(event.entity_fingerprint)
            actions.append(action)
            chronology[action.event_id] = chronological_key(event)

    ranks = {eid: rank for rank, eid in enumerate(
        sorted(chronology, key=chronology.get))}
    actions.sort(key=lambda a: _temporal_action_order(a, ranks))

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


def _purged_horizon_reason(plan: RestorePlan,
                           conn=None) -> Optional[str]:
    """Blocking reason when a purge removed the date the plan asks for.

    Second half of the coverage guard, for the callers that cannot pass
    `oldest_event_date` because they hold no connection. `retention`
    records, PER DATASOURCE, what each purge deleted; a cutoff it covers
    names a state the journal cannot serve any more. Per datasource is the
    whole point: a horizon shared by every layer turned one purge into a
    refusal of every older rewind, on layers the purge had not touched at
    all. And per INTERVAL for the logical purges (`retention.coverage_refusal`):
    a purged INSERT/DELETE pair or undone compensation only loses the
    dates between its two ends, and refusing everything before its end
    disabled most of a layer's history after one maintenance click.
    """
    cutoff = plan.cutoff
    if cutoff is None or cutoff.cutoff_type != CutoffType.BY_DATE:
        return None
    if not isinstance(cutoff.value, str):
        return None
    # Local import: retention imports this module for its own coverage
    # helper, so the dependency only exists at call time.
    from .retention import coverage_refusal

    return coverage_refusal(conn, plan.datasource_fingerprint, cutoff.value)


def preflight_check(
    plan: RestorePlan, oldest_event_date: Optional[str] = None,
    conn=None,
) -> PreflightReport:
    """Validate a restore plan and produce a preflight report.

    *oldest_event_date* is the created_at of the oldest event still held by
    the journal for `plan.datasource_fingerprint`
    (event_stream_repository.get_oldest_event_date, or
    retention.check_journal_coverage which wraps both). When it is supplied
    and the requested date predates it, the plan is BLOCKED: replaying only
    what survived a purge produces a state the layer never had, announced
    as a success.

    The planner has no journal access of its own, so a caller that omits
    the argument used to get no coverage guard at all -- and the automatic
    purge that runs at QGIS startup is exactly the caller-free path that
    creates the hole. `_purged_horizon_reason` closes it from the other
    end, with what the purge itself recorded for THIS layer.
    """
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
        coverage_err = None
        if oldest_event_date:
            coverage_err = check_retention_coverage(
                plan.cutoff, oldest_event_date)
        if coverage_err is None:
            # With a journal connection the horizon is the one the journal
            # persisted (survives restarts); without, session memory only.
            coverage_err = _purged_horizon_reason(plan, conn)
        if coverage_err:
            flog(f"restore_planner: retention coverage refused "
                 f"datasource={plan.datasource_fingerprint} "
                 f"{coverage_err}", "WARNING")
            blocking.append(f"Retention coverage: {coverage_err}")

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

    if getattr(event, 'restored_from_event_id', None) is not None:
        flog(f"restore_planner: trace event leaked into planner "
             f"eid={eid} restored_from={event.restored_from_event_id}", "ERROR")
        return None, Conflict(
            event_id=eid,
            reason="trace_event_leaked",
            severity="blocking",
            details=None,
        )

    # A refusal the rewind window named: the state at the cutoff needs
    # what the journal no longer holds. Blocking, with its reason in the
    # text preflight shows, so the whole layer is refused out loud instead
    # of being rewound to a state it never had.
    if plan_item_kind(event) == PLAN_MARK_REFUSED:
        reason = refusal_reason(event)
        flog(f"restore_planner: rewind refused eid={eid} "
             f"datasource={event.datasource_fingerprint} reason={reason}",
             "ERROR")
        return None, Conflict(
            event_id=eid,
            reason=f"unrebuildable_state: {reason}",
            severity="blocking",
            details=reason,
        )

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
        details = (
            "Temporal restore requires stable identity: event skipped"
            if require_fingerprint
            else "FID-based fallback will be used"
        )
        # Severity is a WARNING even in temporal mode, although the event is
        # still dropped from the plan. Compensating a row we cannot name
        # would edit whichever feature happens to carry that fid today, so
        # the row does not get an action. But `preflight_check` turns any
        # blocking conflict into a refusal of the WHOLE group: one legacy
        # row -- a line written before the entity_fingerprint backfill --
        # cancelled the rewind of every perfectly identified entity of the
        # layer, and the user read "Preflight blocked" without recovering
        # anything. Naming the row in the warnings and restoring the rest is
        # strictly more of the user's data back.
        # Fail-closed is preserved at the other end: a plan where NO event
        # has an identity ends up with zero action and preflight still
        # blocks it ("No actions in plan").
        conflict = Conflict(
            event_id=eid,
            reason="missing_entity_fingerprint",
            severity="warning",
            details=details,
        )
        if require_fingerprint:
            flog(f"restore_planner: event skipped, no stable identity "
                 f"eid={eid} op={event.operation_type} "
                 f"datasource={event.datasource_fingerprint}", "WARNING")
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


def _temporal_action_order(action: PlannedAction,
                           ranks: Optional[dict] = None) -> tuple:
    """Order of compensatory actions for atomic temporal restore.

    Phase 0: INSERT compensatory (re-insert deleted features) so later
             UPDATE/DELETE actions can target them via fid_remap.
    Phase 1: UPDATE compensatory, newest first (undo most recent edit
             first; otherwise reverting the oldest UPDATE leaves the
             feature in an intermediate state and breaks the post-state
             fallback for subsequent UPDATEs).
    Phase 2: DELETE compensatory, oldest first (delete inserted
             features last so any prior UPDATE can still target them).

    Inside a phase the order is CHRONOLOGICAL: *ranks* maps each event_id
    to its rank in (created_at, event_id), the order the fetch and the
    dedup use. Sorting on the raw event_id disagreed with them for an
    event re-inserted from the pending file after a crash: it keeps its
    old created_at but receives a NEW, larger event_id. Two UPDATEs of the
    same field, the old one re-imported, were then unwound in the wrong
    order and the field ended on the intermediate value, both actions
    reported applied (finding planner-eid-order-vs-timeline). Without
    *ranks* (legacy callers) the event_id stands in for the rank.
    """
    eid = action.event_id or 0
    rank = ranks.get(action.event_id, eid) if ranks else eid
    op = action.compensatory_op
    if op == "INSERT":
        return (0, rank, 0)
    if op == "UPDATE":
        return (1, -rank, 0)
    if op == "DELETE":
        return (2, rank, 0)
    return (99, rank, 0)
