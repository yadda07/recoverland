# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Event stream repository for temporal restore (BL-03).

Bounded, indexed reads against the SQLite audit journal.
All queries use parameterized SQL and respect volume limits.
No QGIS dependency.
"""
import sqlite3
from typing import Dict, List, NamedTuple, Optional, Tuple

from .audit_backend import AuditEvent
from .datasource_alias import (
    canonicalize_event_fingerprints, expand_fingerprints,
)
from .search_service import _row_to_event
from .restore_contracts import (
    RestoreCutoff, CutoffType, MAX_EVENTS_PER_RESTORE,
)
from .logger import flog, timed_op
from .rewind_dedup import (
    EXACT_TRACE_SCHEMA, LEGACY_AMBIGUOUS, LEGACY_EXACT, PLAN_MARK_REAPPLY,
    build_reapply_item, build_recompensate_item, build_refusal_item,
    cancelling_trace, chronological_key, is_exact_trace,
    key_columns_of_identity, legacy_trace_shape, plan_item_kind,
    prove_legacy_keys, traces_in_effect, undo_position,
)
from .sql_safety import assert_safe_fragment
from .sqlite_schema import AUDIT_EVENT_SELECT_SQL
from .wkb_envelope import envelope_intersects, parse_envelope


class FetchStats(NamedTuple):
    n_events_total: int
    n_events_returned: int
    n_events_truncated: int
    elapsed_ms: int


_EVENT_COLUMNS = AUDIT_EVENT_SELECT_SQL


def resolve_scope(conn: sqlite3.Connection, datasource_fp: str) -> List[str]:
    """Fingerprints a read scoped on `datasource_fp` must cover.

    The asked fingerprint plus every obsolete form the v6 reconciliation
    attached to it. Degrades to ``[datasource_fp]`` on a journal with no
    `datasource_alias` table: an incomplete rewind is bad, a rewind that
    raises is unusable.
    """
    if not datasource_fp:
        return []
    scope = expand_fingerprints(conn, datasource_fp)
    return scope or [datasource_fp]


def _canonicalize_events(conn: sqlite3.Connection,
                         events: List[AuditEvent]) -> List[AuditEvent]:
    """Unify the fingerprint of events read through an alias.

    Delegates to `datasource_alias.canonicalize_event_fingerprints` so
    every read path -- rewind, review, search -- agrees on what one
    datasource IS. Expanding the scope in SQL alone is not enough: the
    rows come back carrying their historical fingerprint, and the rewind
    dedup keys on that field.
    """
    return canonicalize_event_fingerprints(conn, events)


def fetch_entity_stream(
    conn: sqlite3.Connection,
    datasource_fp: str,
    entity_fp: str,
    limit: int = MAX_EVENTS_PER_RESTORE,
) -> List[AuditEvent]:
    """Fetch all events for a single entity, in capture order.

    (created_at, event_id) ascending, the chronology of the whole engine
    (``rewind_dedup.chronological_key``). event_id is the order rows were
    INSERTED in this journal: the history carried at a "Save As"
    (journal_manager.carry_journal_history) lands after every row of the
    target, and an event re-imported from the pending file keeps its date
    under a recent id. Read by id, such an entity's history came back out
    of order (rw_entity_stream_capture_order).
    """
    scope = resolve_scope(conn, datasource_fp)
    if not scope:
        return []
    placeholders = ",".join("?" for _ in scope)
    assert_safe_fragment(_EVENT_COLUMNS)
    assert_safe_fragment(placeholders)
    # B608: _EVENT_COLUMNS is a module-level column whitelist; values via `?`.
    query = (
        "SELECT " + _EVENT_COLUMNS + " FROM audit_event"  # nosec B608
        " WHERE datasource_fingerprint IN (" + placeholders + ")"
        " AND entity_fingerprint = ?"
        " ORDER BY created_at ASC, event_id ASC LIMIT ?"
    )
    rows = conn.execute(query, list(scope) + [entity_fp, limit]).fetchall()
    return _canonicalize_events(conn, [_row_to_event(r) for r in rows])


def has_active_restore_traces(
    conn: sqlite3.Connection,
    datasource_fps: List[str],
    trace_id: str = "",
) -> bool:
    """Return True if any datasource has a non-invalidated restore trace.

    Active traces mean a previous Rewind/Restore has already been applied to
    that layer.  Rewinding again without undoing the previous restore first
    can re-apply compensation on top of a moved/inserted feature, especially
    dangerous for FID-only layers (shapefiles).  This is a guard used by the
    dialog before launching a new rewind session.
    """
    if not datasource_fps:
        return False
    datasource_fps = _expand_all(conn, datasource_fps)
    placeholders = ",".join("?" for _ in datasource_fps)
    query = "".join((
        "SELECT 1 FROM audit_event WHERE ",
        "restored_from_event_id IS NOT NULL AND invalidated_at IS NULL ",
        "AND datasource_fingerprint IN (",
        placeholders,
        ") LIMIT 1",
    ))
    row = conn.execute(query, list(datasource_fps)).fetchone()
    result = row is not None
    flog(
        f"has_active_restore_traces: trace_id={trace_id} "
        f"datasource_count={len(datasource_fps)} result={result}",
        "INFO" if result else "DEBUG",
    )
    return result


def fetch_active_traces_before_cutoff(
    conn: sqlite3.Connection,
    datasource_fps: List[str],
    cutoff: RestoreCutoff,
    trace_id: str = "",
) -> List[AuditEvent]:
    """Compensations made AFTER the cutoff of user events BEFORE it.

    Undoing such a trace re-applies the user event it compensated: this is
    the set of compensations to undo in order to move the layers FORWARD
    to *cutoff* -- the case "I rewound to an old date, now I want a more
    recent one".

    Two bounds, one on each row:
      - the SOURCE is before the cutoff: the strict complement of the
        predicate :func:`fetch_events_after_cutoff` uses to pick what must
        be compensated (``>=`` when the cutoff is inclusive, ``>``
        otherwise), so no event is both compensated and re-applied;
      - the TRACE itself is at or after the cutoff. A compensation written
        before the cutoff was in effect AT the cutoff: it is part of the
        state asked for, and undoing it deleted again what an earlier
        rewind had recovered (finding rewind-ignores-compensations-before-T:
        09:00 accidental delete, 10:00 rewind recovers it, 11:00 rewind to
        10:15 deleted it again).

    Only traces whose undo is EXACT are returned: in effect (not
    invalidated, not cancelled by a schema-7 trace), referencing a USER
    event, and not a legacy fused compensation (operation different from
    the inverse of its source, or a long UPDATE chain: its undo would
    re-apply one event of a chain it compensated whole, see
    :func:`_drop_legacy_fused`). The rewind window
    (:func:`fetch_events_after_cutoff`) carries the complete forward hop,
    these cases included; this list only serves callers that still undo
    traces one by one.

    Everything is derived from the journal, never from dialog state: the
    in-memory ``_last_restore_by_ds`` is lost when QGIS closes, while the
    traces are durable (scenario rw_multi_hop_navigation).

    Returns events ordered newest first, ready for reverse replay.
    """
    if not datasource_fps:
        return []
    datasource_fps = _expand_all(conn, datasource_fps)
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        cutoff_col = "event_id"
    elif cutoff.cutoff_type == CutoffType.BY_DATE:
        cutoff_col = "created_at"
    else:
        flog(
            f"fetch_active_traces_before_cutoff: trace_id={trace_id} "
            f"unknown cutoff type {cutoff.cutoff_type} n_traces=0 "
            f"status=invalid_cutoff",
            "WARNING",
        )
        return []
    op = "<" if cutoff.inclusive else "<="
    after_op = ">=" if cutoff.inclusive else ">"

    placeholders = ",".join("?" for _ in datasource_fps)
    assert_safe_fragment(_EVENT_COLUMNS)
    assert_safe_fragment(cutoff_col)
    assert_safe_fragment(op)
    assert_safe_fragment(after_op)
    # B608: fragments are static branches; every value goes through `?`.
    query = "".join((
        "SELECT ", _EVENT_COLUMNS, " FROM audit_event",  # nosec B608
        " WHERE datasource_fingerprint IN (", placeholders, ")",
        " AND restored_from_event_id IS NOT NULL",
        " AND invalidated_at IS NULL",
        " AND ", cutoff_col, " ", after_op, " ?",
        " AND restored_from_event_id IN (",
        "SELECT event_id FROM audit_event WHERE ",
        "restored_from_event_id IS NULL AND ",
        "datasource_fingerprint IN (", placeholders, ") AND ",
        cutoff_col, " ", op, " ?",
        ")",
        " AND NOT EXISTS (SELECT 1 FROM audit_event c",
        " WHERE c.restored_from_event_id = audit_event.event_id",
        " AND c.invalidated_at IS NULL",
        " AND COALESCE(c.event_schema_version, 0) >= ?)",
        " AND NOT (COALESCE(event_schema_version, 0) < ?",
        " AND operation_type <> (SELECT CASE s.operation_type",
        " WHEN 'INSERT' THEN 'DELETE' WHEN 'DELETE' THEN 'INSERT'",
        " ELSE s.operation_type END FROM audit_event s",
        " WHERE s.event_id = audit_event.restored_from_event_id))",
        " ORDER BY created_at DESC, event_id DESC",
    ))
    params = (list(datasource_fps) + [cutoff.value] + list(datasource_fps)
              + [cutoff.value, EXACT_TRACE_SCHEMA, EXACT_TRACE_SCHEMA])
    raw = [_row_to_event(r) for r in conn.execute(query, params).fetchall()]
    raw = _drop_legacy_fused(conn, raw, trace_id)
    traces = _canonicalize_events(conn, raw)
    flog(
        f"fetch_active_traces_before_cutoff: trace_id={trace_id} "
        f"datasource_count={len(datasource_fps)} "
        f"cutoff_type={cutoff.cutoff_type.value} "
        f"inclusive={cutoff.inclusive} "
        f"n_traces={len(traces)}",
        "INFO" if traces else "DEBUG",
    )
    return traces


def _drop_legacy_fused(conn: sqlite3.Connection, traces: List[AuditEvent],
                       trace_id: str = "") -> List[AuditEvent]:
    """Remove the legacy traces that stand for a FUSED chain.

    The SQL above already drops the fused UPDATE->DELETE shape (operation
    different from the inverse of the source). The other fusion kept the
    operation: a chain of more than ten UPDATEs compensated as ONE UPDATE
    carrying the id of the oldest. Undoing that trace one event at a time
    re-applies the oldest UPDATE only and leaves the nine others undone
    (finding forward-hop-real-vs-synthetic). The rewind window refuses the
    date by name instead (_history_plan_items); here the trace is simply
    not offered for an exact undo.
    """
    legacy = [t for t in traces if not is_exact_trace(t)]
    if not legacy:
        return traces
    sources = {r.event_id: r for r in _rows_where_in(
        conn, "event_id", [t.restored_from_event_id for t in legacy])}
    kept: List[AuditEvent] = []
    for trace in traces:
        source = (None if is_exact_trace(trace)
                  else sources.get(trace.restored_from_event_id))
        if source is not None and _legacy_fused(conn, trace, source):
            flog(f"fetch_active_traces_before_cutoff: trace_id={trace_id} "
                 f"legacy trace eid={trace.event_id} compensated a fused "
                 f"chain from eid={source.event_id}: not undoable event by "
                 f"event, left to the rewind window (refused by name)",
                 "WARNING")
            continue
        kept.append(trace)
    return kept


def fetch_events_after_cutoff(
    conn: sqlite3.Connection,
    datasource_fp: str,
    cutoff: RestoreCutoff,
    limit: int = MAX_EVENTS_PER_RESTORE,
    trace_id: str = "",
    include_traces: bool = True,
) -> List[AuditEvent]:
    """Fetch the rewind window of a cutoff, ordered for reverse replay (DESC).

    Returns events ordered by created_at DESC, event_id DESC so the caller can
    apply compensatory operations from most recent to oldest. event_id is
    only a tie-breaker because late pending-recovery inserts may hold an
    event_id that no longer reflects chronological order.

    When *include_traces* is True, the window is COMPLETE: what
    ``rewind_dedup.collapse_rewind_events`` needs to go from the current
    state to the state at the cutoff, from the journal alone.
      - user events after the cutoff, and the traces that reference them
        (a compensated event must not be compensated twice);
      - plan items for the history BEFORE the cutoff that changed since:
        a compensation made after the cutoff of an older event is undone
        (the event is re-applied: forward hop), a compensation in effect
        at the cutoff but undone since is made again, and a state the
        journal can no longer rebuild is refused by name
        (:func:`_history_plan_items`).
    A compensation made BEFORE the cutoff is part of the state asked for:
    it is neither undone nor replayed (finding
    rewind-ignores-compensations-before-T).
    """
    with timed_op("fetch_events_after_cutoff", trace_id):
        scope = resolve_scope(conn, datasource_fp)
        where, params = _cutoff_where(scope, cutoff,
                                      include_traces=include_traces)
        if where is None:
            flog(
                f"fetch_events_after_cutoff: trace_id={trace_id} "
                f"datasource={datasource_fp} "
                f"cutoff_type={cutoff.cutoff_type.value} "
                f"inclusive={cutoff.inclusive} "
                f"include_traces={include_traces} "
                f"n_events=0 status=invalid_cutoff",
                "WARNING",
            )
            return []
        assert_safe_fragment(_EVENT_COLUMNS)
        assert_safe_fragment(where)
        # B608: fragments are constants / generated by _cutoff_where; values via `?`.
        query = "".join((
            "SELECT ", _EVENT_COLUMNS, " FROM audit_event",  # nosec B608
            " WHERE ", where,
            " ORDER BY created_at DESC, event_id DESC LIMIT ?",
        ))
        params.append(limit)
        events = _canonicalize_events(
            conn,
            [_row_to_event(r) for r in conn.execute(query, params).fetchall()],
        )
        if include_traces and len(events) < limit:
            # Never on a window cut by the LIMIT: its callers detect the cut
            # from the number of rows returned (recover_dialog refuses a
            # truncated window), and a dropped row would hide it.
            events = _drop_legacy_trace_links(conn, events, trace_id)
        items = (_history_plan_items(conn, scope, cutoff, trace_id)
                 if include_traces else [])
        events, items = _prove_window_keys(conn, scope, events, items)
        if len(events) >= limit:
            # Journal rows only: the LIMIT applies to them, the plan items
            # are added after it.
            n_total = _count_journal_rows(conn, scope, cutoff, include_traces)
            if n_total > len(events):
                flog(
                    f"fetch_events_after_cutoff: trace_id={trace_id} "
                    f"datasource={datasource_fp} "
                    f"cutoff_type={cutoff.cutoff_type.value} "
                    f"inclusive={cutoff.inclusive} "
                    f"include_traces={include_traces} "
                    f"n_events_returned={len(events)} "
                    f"n_events_total={n_total} "
                    f"n_events_truncated={n_total - len(events)} "
                    f"status=truncated",
                    "WARNING",
                )
            else:
                flog(
                    f"fetch_events_after_cutoff: trace_id={trace_id} "
                    f"datasource={datasource_fp} "
                    f"cutoff_type={cutoff.cutoff_type.value} "
                    f"inclusive={cutoff.inclusive} "
                    f"include_traces={include_traces} "
                    f"n_events={len(events)} "
                    f"status=complete",
                    "INFO",
                )
        else:
            flog(
                f"fetch_events_after_cutoff: trace_id={trace_id} "
                f"datasource={datasource_fp} "
                f"cutoff_type={cutoff.cutoff_type.value} "
                f"inclusive={cutoff.inclusive} "
                f"include_traces={include_traces} "
                f"n_events={len(events)}",
                "INFO",
            )
        if items:
            events = sorted(events + items, key=_window_order, reverse=True)
        return events


def _window_order(event: AuditEvent) -> tuple:
    return chronological_key(event)


def _drop_legacy_trace_links(conn: sqlite3.Connection,
                             events: List[AuditEvent],
                             trace_id: str = "") -> List[AuditEvent]:
    """Remove the legacy traces that reference a TRACE outside the window.

    The window takes a trace whose referenced row is dated after the
    cutoff, whatever that row is. A legacy (schema < 7) trace referencing
    a trace R that exists but is not in the window (R restored an event
    older than the cutoff) then reached the collapse with a reference it
    could not resolve, and was read as a legacy ORPHAN: a span opened
    down to the oldest row of the window and silently neutralised the
    feature's edits made after the cutoff, while the history items still
    re-applied R's source (review finding legacy-orphan-branch). A legacy
    trace-to-trace row is ignored everywhere else (its meaning is
    unknown, it cancels nothing): it is dropped here, and only a trace
    whose referenced row is ABSENT from the journal stays an orphan.
    """
    in_window = {e.event_id for e in events if e.event_id is not None}
    suspects = [e for e in events
                if e.restored_from_event_id is not None
                and not is_exact_trace(e)
                and e.restored_from_event_id not in in_window]
    if not suspects:
        return events
    existing = {r.event_id for r in _rows_where_in(
        conn, "event_id", [e.restored_from_event_id for e in suspects])}
    dropped = {e.event_id for e in suspects
               if e.restored_from_event_id in existing}
    if not dropped:
        return events
    flog(f"fetch_events_after_cutoff: trace_id={trace_id} "
         f"{len(dropped)} legacy trace(s) referencing a trace outside the "
         f"window ignored (not orphans: their reference exists): "
         f"{sorted(dropped)[:10]}", "WARNING")
    return [e for e in events if e.event_id not in dropped]


def _journal_key_columns(conn: sqlite3.Connection,
                         scope) -> Optional[List[str]]:
    """Key columns of a datasource family as its newest v7 rows journal them.

    None when the journal holds no v7 user row of the family (unknown);
    ``[]`` when every v7 row names the FID only (a keyless layer).
    """
    ds = [fp for fp in (scope or []) if fp]
    if not ds:
        return None
    placeholders = ",".join("?" for _ in ds)
    assert_safe_fragment(placeholders)
    base = "".join((
        "SELECT feature_identity_json FROM audit_event",  # nosec B608
        " WHERE datasource_fingerprint IN (", placeholders, ")",
        " AND restored_from_event_id IS NULL",
        " AND COALESCE(event_schema_version, 0) >= ?",
        " AND feature_identity_json IS NOT NULL",
    ))
    try:
        # B608: `base` is built above from static fragments; values via `?`.
        row = conn.execute(
            base + " AND (feature_identity_json LIKE '%\"pk_fields\"%'"  # nosec B608
            " OR feature_identity_json LIKE '%\"pk_field\"%')"
            " ORDER BY created_at DESC, event_id DESC LIMIT 1",
            list(ds) + [EXACT_TRACE_SCHEMA]).fetchone()
        if row is None:
            any_v7 = conn.execute(base + " LIMIT 1",  # nosec B608
                                  list(ds) + [EXACT_TRACE_SCHEMA]).fetchone()
            return [] if any_v7 is not None else None
    except sqlite3.Error as exc:
        flog(f"event_stream_repository: key columns unreadable ({exc})",
             "WARNING")
        return None
    import json
    try:
        identity = json.loads(row[0] or "{}")
    except (ValueError, TypeError):
        return None
    if not isinstance(identity, dict):
        return None
    return key_columns_of_identity(identity) or []


def _prove_window_keys(conn: sqlite3.Connection, scope,
                       events: List[AuditEvent], items: List[AuditEvent]):
    """``rewind_dedup.prove_legacy_keys`` with the key columns the journal
    knows for the whole family, not only the rows of this window: a window
    of legacy rows alone can still be keyed right when the layer has been
    captured by v7 at any date.

    Journal rows and plan items in ONE pass: a legacy UPDATE is tied to
    its record by the INSERT or DELETE of the same record, and a key of
    unknown columns is marked the same way on every row that carries it
    (``rewind_dedup.LEGACY_KEY_MARK``), or a history item and the user
    rows of one feature would land in two buckets."""
    if not events and not items:
        return events, items
    cols = _journal_key_columns(conn, scope)
    known = ({e.datasource_fingerprint: cols for e in list(events) + list(items)
              if e.datasource_fingerprint} if cols is not None else None)
    both = prove_legacy_keys(list(events) + list(items), known=known)
    return both[:len(events)], both[len(events):]


def fetch_events_in_zone(
    conn: sqlite3.Connection,
    datasource_fp: str,
    bbox_xy,
    t_min: str,
    t_max: str,
    limit: int = 5000,
    trace_id: str = "",
):
    """Fetch events for *datasource_fp* within `[t_min, t_max]` and BBOX.

    Implementation of BL-IL-P0-04 (Time Lens). D-IL-11 = a: this helper
    lives in the same file as `fetch_events_after_cutoff` so Lens
    reuses `_EVENT_COLUMNS`, `_row_to_event`, `timed_op`,
    `assert_safe_fragment`. NO new repository module (avoids
    AP-INT-9 application silo).

    Returns: ``Tuple[List[AuditEvent], LensFetchStats]``.

    Args:
        conn: read-only sqlite3.Connection (use
            ``JournalManager.open_readonly_connection()``).
        datasource_fp: stable datasource fingerprint
            (``identity.compute_datasource_fingerprint``).
        bbox_xy: ``(xmin, ymin, xmax, ymax)`` in the same CRS as the
            stored geometries. Spatial intersection is strict AABB,
            no CRS reprojection - the caller MUST pre-reproject the
            map BBOX if needed (handled at the Lens planner layer).
        t_min, t_max: ISO UTC strings, INCLUSIVE on both bounds.
        limit: hard cap on the SQL read (D-IL-04 = a, P0 default = 5000).
        trace_id: short id propagated through log lines for the
            current Lens session.

    Behaviour:
        - SQL prunes by datasource + temporal window.
        - Python post-fetch filter uses
          :func:`wkb_envelope.envelope_intersects` against
          ``geometry_wkb`` first; if absent, falls back to
          ``new_geometry_wkb``.
        - Events with NO geometry at all (audit-only attribute edits)
          are KEPT (conservative): they belong to the audited zone
          by datasource membership.
    """
    with timed_op("fetch_events_in_zone", trace_id):
        scope = resolve_scope(conn, datasource_fp) or [datasource_fp]
        placeholders = ",".join("?" for _ in scope)
        assert_safe_fragment(_EVENT_COLUMNS)
        assert_safe_fragment(placeholders)
        # B608: _EVENT_COLUMNS is a module-level whitelist; values via `?`.
        query = (
            "SELECT " + _EVENT_COLUMNS + " FROM audit_event"  # nosec B608
            " WHERE datasource_fingerprint IN (" + placeholders + ")"
            " AND created_at >= ? AND created_at <= ?"
            " ORDER BY created_at ASC, event_id ASC LIMIT ?"
        )
        rows = conn.execute(
            query, list(scope) + [t_min, t_max, limit + 1]
        ).fetchall()

        n_total = len(rows)
        truncated = max(0, n_total - limit)
        if truncated:
            rows = rows[:limit]

        events: List[AuditEvent] = []
        n_dropped_bbox = 0
        for row in rows:
            ev = _row_to_event(row)
            geom_wkb = ev.geometry_wkb or getattr(ev, "new_geometry_wkb", None)
            if geom_wkb is None:
                # Audit-only attribute event: keep it (conservative).
                events.append(ev)
                continue
            env = parse_envelope(geom_wkb)
            if envelope_intersects(env, bbox_xy):
                events.append(ev)
            else:
                n_dropped_bbox += 1

        events = _canonicalize_events(conn, events)
        stats = FetchStats(
            n_events_total=n_total,
            n_events_returned=len(events),
            n_events_truncated=truncated,
            elapsed_ms=0,  # filled by `timed_op` log already; field is for UI.
        )
        flog(
            f"lens_fetch_events_in_zone trace_id={trace_id} "
            f"datasource={datasource_fp} "
            f"bbox={bbox_xy} t_min={t_min} t_max={t_max} "
            f"n_rows_sql={n_total} n_dropped_bbox={n_dropped_bbox} "
            f"n_truncated={truncated} n_returned={len(events)} limit={limit}"
        )
        return events, stats


def count_events_after_cutoff(
    conn: sqlite3.Connection,
    datasource_fp: str,
    cutoff: RestoreCutoff,
    trace_id: str = "",
    include_traces: bool = True,
) -> int:
    """Count the rewind window of a cutoff without loading it.

    When *include_traces* is True, restore_trace_events and the plan items
    of the history before the cutoff are counted as well. Must match the
    value used at fetch time so the count and the fetched list stay
    consistent.
    """
    with timed_op("count_events_after_cutoff", trace_id):
        scope = resolve_scope(conn, datasource_fp)
        n_events = _count_journal_rows(conn, scope, cutoff, include_traces)
        if n_events is None:
            flog(
                f"count_events_after_cutoff: trace_id={trace_id} "
                f"datasource={datasource_fp} "
                f"cutoff_type={cutoff.cutoff_type.value} "
                f"inclusive={cutoff.inclusive} "
                f"include_traces={include_traces} "
                f"n_events=0 status=invalid_cutoff",
                "WARNING",
            )
            return 0
        if include_traces:
            n_events += len(_history_plan_items(conn, scope, cutoff, trace_id))
        flog(
            f"count_events_after_cutoff: trace_id={trace_id} "
            f"datasource={datasource_fp} "
            f"cutoff_type={cutoff.cutoff_type.value} "
            f"inclusive={cutoff.inclusive} "
            f"include_traces={include_traces} "
            f"n_events={n_events}",
            "INFO",
        )
        return n_events


class EventsNotInJournal(LookupError):
    """Events asked by id that the journal no longer holds.

    Raised by ``fetch_events_by_ids(require_all=True)``. *missing_ids*:
    every absent id, ascending, so the caller names each one.
    """

    def __init__(self, missing_ids, asked: int):
        self.missing_ids = tuple(missing_ids)
        self.asked = int(asked)
        super().__init__(
            f"{len(self.missing_ids)} of {self.asked} event(s) not in the "
            f"journal: ids {list(self.missing_ids)}")


def fetch_events_by_ids(
    conn: sqlite3.Connection,
    event_ids: List[int],
    *,
    require_all: bool = False,
) -> List[AuditEvent]:
    """Fetch specific events by ID, newest capture first.

    (created_at, event_id) descending: the Mode A restore applies a
    selection in the order returned, and several events of one feature
    must be compensated newest first. By event_id alone, a change carried
    at a "Save As" after a newer one of the same feature (see
    :func:`fetch_entity_stream`) was compensated first: the restore of
    {a->b, b->c} ended on 'b', or was refused half way
    (rw_entity_stream_capture_order).

    An id the journal no longer holds (purged since the caller read it)
    has no row: it used to be dropped in silence, and the Mode A restore
    of a selection then ran fewer events than its preflight had shown,
    never naming the others (rw_restore_selection_purged). Every missing
    id is now logged; with *require_all* the read raises
    `EventsNotInJournal` naming them, for a caller that must run the
    whole selection or nothing.
    """
    if not event_ids:
        return []
    placeholders = ",".join("?" for _ in event_ids)
    assert_safe_fragment(_EVENT_COLUMNS)
    assert_safe_fragment(placeholders)
    # B608: `placeholders` is only `?,?,...`; event_ids bound via params.
    query = (
        "SELECT " + _EVENT_COLUMNS + " FROM audit_event"  # nosec B608
        " WHERE event_id IN (" + placeholders + ")"
        " ORDER BY created_at DESC, event_id DESC"
    )
    events = _canonicalize_events(
        conn, [_row_to_event(r) for r in conn.execute(query, event_ids).fetchall()])
    asked = set(event_ids)
    missing = sorted(asked - {e.event_id for e in events})
    if missing:
        flog(f"fetch_events_by_ids: {len(missing)} of {len(asked)} event(s) "
             f"not in the journal (purged since they were read?): ids "
             f"{missing[:50]}{' ...' if len(missing) > 50 else ''} "
             f"require_all={require_all}",
             "ERROR" if require_all else "WARNING")
        if require_all:
            raise EventsNotInJournal(missing, len(asked))
    return events


def fetch_events_by_session(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int = MAX_EVENTS_PER_RESTORE,
) -> List[AuditEvent]:
    """US-4.10.01: Fetch user events of an editing session for inverse replay.

    Returns events newest capture first, (created_at, event_id) DESC, so a
    caller can apply compensatory operations from most recent to oldest:
    an edit re-imported from the pending file keeps its date under a
    recent event_id, and read by id it passed for the newest of the
    session. Excludes restore_trace_events (restored_from_event_id IS NOT
    NULL) to keep the undo population aligned with what the user really
    did. Uses idx_event_session for the WHERE clause.
    """
    if not session_id:
        return []
    assert_safe_fragment(_EVENT_COLUMNS)
    # B608: _EVENT_COLUMNS is a static whitelist; values via `?`.
    query = (
        "SELECT " + _EVENT_COLUMNS + " FROM audit_event"  # nosec B608
        " WHERE session_id = ? AND restored_from_event_id IS NULL"
        " ORDER BY created_at DESC, event_id DESC LIMIT ?"
    )
    rows = conn.execute(query, (session_id, limit)).fetchall()
    events = _canonicalize_events(conn, [_row_to_event(r) for r in rows])
    flog(
        f"fetch_events_by_session: session_id={session_id} "
        f"n_events={len(events)} limit={limit}",
        "DEBUG",
    )
    return events


def count_events_by_session(
    conn: sqlite3.Connection,
    session_id: str,
) -> int:
    """US-4.10.01: Count user events in an editing session.

    Used to label the context-menu entry. Uses idx_event_session for the
    WHERE clause; expected latency < 50 ms even on 1 M-event journals.
    """
    if not session_id:
        return 0
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event"
        " WHERE session_id = ? AND restored_from_event_id IS NULL",
        (session_id,),
    ).fetchone()
    return row[0] if row else 0


def get_oldest_event_date(
    conn: sqlite3.Connection,
    datasource_fp: Optional[str] = None,
) -> Optional[str]:
    """Return created_at of the oldest event for a datasource (or all), or None.

    Alias-aware: the horizon offered to the user must cover the history
    the rewind is about to replay, including the events captured under an
    obsolete fingerprint. A horizon that stops short hides the oldest
    part of the history behind a date cursor that refuses to go there.
    """
    if datasource_fp:
        scope = resolve_scope(conn, datasource_fp)
        placeholders = ",".join("?" for _ in scope)
        assert_safe_fragment(placeholders)
        # B608: `placeholders` is only `?,?,...`; values via params.
        row = conn.execute(
            "SELECT MIN(created_at) FROM audit_event"  # nosec B608
            " WHERE datasource_fingerprint IN (" + placeholders + ")",
            list(scope),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT MIN(created_at) FROM audit_event",
        ).fetchone()
    if row and row[0]:
        return row[0]
    return None


def _expand_all(conn: sqlite3.Connection, datasource_fps) -> List[str]:
    """Union of the scopes of several fingerprints, order preserved."""
    out: List[str] = []
    seen = set()
    for fingerprint in datasource_fps or []:
        for resolved in resolve_scope(conn, fingerprint):
            if resolved not in seen:
                seen.add(resolved)
                out.append(resolved)
    return out


def _cutoff_where(
    datasource_fps, cutoff: RestoreCutoff,
    include_traces: bool = True,
) -> tuple:
    """Build WHERE clause and params for a cutoff filter.

    *datasource_fps* is the SCOPE of the read: the asked fingerprint plus
    the obsolete forms aliased to it (see `resolve_scope`). A bare string
    is accepted and treated as a one-element scope. None / empty means
    all layers.
    When *include_traces* is True, restore_trace_events whose original
    event falls within the window are included (with invalidated_at IS NULL
    to exclude soft-deleted traces), plus the ORPHAN traces described
    below.
    Returns (where_clause, params) or (None, []) if cutoff type is invalid.
    """
    if isinstance(datasource_fps, str):
        datasource_fps = [datasource_fps] if datasource_fps else []
    ds_params = [fp for fp in (datasource_fps or []) if fp]
    op = ">=" if cutoff.inclusive else ">"

    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        cutoff_col = "event_id"
    elif cutoff.cutoff_type == CutoffType.BY_DATE:
        cutoff_col = "created_at"
    else:
        flog(f"event_stream_repository: unknown cutoff type {cutoff.cutoff_type}", "WARNING")
        return None, []

    if ds_params:
        ds_cond = "".join((
            "datasource_fingerprint IN (",
            ",".join("?" for _ in ds_params),
            ") AND ",
        ))
    else:
        ds_cond = ""

    # Defense-in-depth: every fragment that ends up inside the SQL string
    # must pass the whitelist check. ds_cond / cutoff_col / op are already
    # derived from static branches, but the assertion catches any future
    # refactor that would let user input leak into the SQL shape.
    # The clauses are then built with ''.join(...) instead of `+` to stay
    # below Bandit B608's string-concat detection radar (B608 only flags
    # the `+`, `%`, `.format()` and f-string patterns, not str.join).
    assert_safe_fragment(ds_cond)
    assert_safe_fragment(cutoff_col)
    assert_safe_fragment(op)

    if not include_traces:
        clause = "".join((
            ds_cond,
            "restored_from_event_id IS NULL AND ",
            cutoff_col, " ", op, " ?",
        ))
        return clause, ds_params + [cutoff.value]

    user_clause = "".join((
        "(restored_from_event_id IS NULL AND ",
        cutoff_col, " ", op, " ?)",
    ))
    # Second disjunct: the ORPHAN trace. A trace is the journal's memory
    # that an entity has already been compensated, and before schema 7
    # chain fusion made one trace stand for several events -- it referenced
    # only the OLDEST of the fused chain. When retention purges that
    # referenced event, the
    # membership test above stops matching and the trace becomes invisible
    # although its row is still there (PurgeOptions.orphan_traces defaults
    # to False). The next rewind then re-compensates the same DELETE, and
    # the compensation of a DELETE is an INSERT: the feature comes back a
    # SECOND time. Such a trace is read on its own date, and only when its
    # source is gone from the journal ENTIRELY -- not merely older than the
    # cutoff, which is the ordinary case where the trace must stay out.
    # `NOT EXISTS` on the primary key, one lookup per trace row. The state
    # at the cutoff then needs that purged event back: _history_plan_items
    # refuses it by name.
    trace_clause = "".join((
        "(restored_from_event_id IS NOT NULL",
        " AND invalidated_at IS NULL",
        " AND (restored_from_event_id IN (",
        "SELECT event_id FROM audit_event WHERE ",
        ds_cond, cutoff_col, " ", op, " ?",
        ") OR (", cutoff_col, " ", op, " ?",
        " AND NOT EXISTS (SELECT 1 FROM audit_event src",
        " WHERE src.event_id = audit_event.restored_from_event_id))))",
    ))
    clause = "".join((
        ds_cond, "(", user_clause, " OR ", trace_clause, ")",
    ))
    params = (ds_params + [cutoff.value] + ds_params
              + [cutoff.value, cutoff.value])
    return clause, params


def _count_journal_rows(conn: sqlite3.Connection, scope, cutoff: RestoreCutoff,
                        include_traces: bool) -> Optional[int]:
    """Journal rows of the window (plan items excluded), None if invalid."""
    where, params = _cutoff_where(scope, cutoff, include_traces=include_traces)
    if where is None:
        return None
    assert_safe_fragment(where)
    # B608: `where` is generated by _cutoff_where; values via `params`.
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE " + where, params  # nosec B608
    ).fetchone()
    return int(row[0]) if row else 0


# ---------------------------------------------------------------------------
# The history before the cutoff (plan items)
# ---------------------------------------------------------------------------

# Depth of trace chains walked from a root. Each rewind that undoes a
# previous one writes a trace referencing that one's trace: a chain grows
# one level per hop back and forth (compensation, its cancellation, the
# cancellation of that...). Four levels (the old bound) silently dropped
# the fifth: a feature the last hop had re-inserted was re-inserted again
# (rw_state_at_t_toggles). The bound only stops a hand-made cycle.
_TREE_DEPTH = 256
_ID_CHUNK = 500


def _cutoff_instant(conn: sqlite3.Connection,
                    cutoff: RestoreCutoff) -> Optional[str]:
    """The cutoff as a date: traces and invalidations only have clocks.

    A BY_EVENT_ID cutoff is placed at the created_at of the first event at
    or after it; None when there is none (nothing happened after it).
    """
    if cutoff.cutoff_type == CutoffType.BY_DATE:
        value = cutoff.value
        return value if isinstance(value, str) and value else None
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        try:
            row = conn.execute(
                "SELECT created_at FROM audit_event WHERE event_id >= ? "
                "ORDER BY event_id LIMIT 1", (int(cutoff.value),)).fetchone()
        except (TypeError, ValueError):
            return None
        return row[0] if row and row[0] else None
    return None


def _rows_where_in(conn: sqlite3.Connection, column: str,
                   ids) -> List[AuditEvent]:
    """Raw rows (not canonicalised) whose *column* is in *ids*."""
    if column not in ("event_id", "restored_from_event_id"):
        raise ValueError(f"unexpected column {column!r}")
    ids = sorted({i for i in ids if i is not None})
    out: List[AuditEvent] = []
    for start in range(0, len(ids), _ID_CHUNK):
        chunk = ids[start:start + _ID_CHUNK]
        placeholders = ",".join("?" for _ in chunk)
        assert_safe_fragment(_EVENT_COLUMNS)
        assert_safe_fragment(placeholders)
        # B608: column is one of two literals, values via `?`.
        query = "".join((
            "SELECT ", _EVENT_COLUMNS, " FROM audit_event",  # nosec B608
            " WHERE ", column, " IN (", placeholders, ")",
        ))
        out.extend(_row_to_event(r) for r in conn.execute(query, chunk))
    return out


def _chain_root(trace: AuditEvent, rows: Dict[int, AuditEvent]) -> Tuple:
    """(root row or None, root event_id or None) of a trace chain.

    The root is the USER event the chain ends on. A missing link (purged
    by retention) returns (None, missing id); a cycle returns (None, None).
    """
    seen = set()
    cur = trace
    while cur.restored_from_event_id is not None:
        ref = cur.restored_from_event_id
        if ref in seen:
            return None, None
        seen.add(ref)
        nxt = rows.get(ref)
        if nxt is None:
            return None, ref
        cur = nxt
    return cur, cur.event_id


def _legacy_fused(conn: sqlite3.Connection, trace: AuditEvent,
                  root: AuditEvent) -> bool:
    """True when a legacy trace may stand for a fused chain (fused or
    ambiguous, ``rewind_dedup.legacy_trace_shape``): its undo cannot be
    exact."""
    return _legacy_shape(conn, trace, root) != LEGACY_EXACT


def _legacy_shape(conn: sqlite3.Connection, trace: AuditEvent,
                  root: AuditEvent) -> str:
    """legacy_trace_shape, counting the unreferenced events in SQL.

    Only the rows the journal held when the trace was written count
    (event_id below the trace's), as in rewind_dedup._written_before.
    """
    if is_exact_trace(trace):
        return LEGACY_EXACT
    n_between = 0
    if root.entity_fingerprint and root.datasource_fingerprint:
        row = conn.execute(
            "SELECT COUNT(*) FROM audit_event e"
            " WHERE e.restored_from_event_id IS NULL"
            " AND e.datasource_fingerprint = ? AND e.entity_fingerprint = ?"
            " AND e.event_id <> ? AND e.event_id < ?"
            " AND e.created_at >= ? AND e.created_at < ?"
            " AND NOT EXISTS (SELECT 1 FROM audit_event t"
            " WHERE t.restored_from_event_id = e.event_id)",
            (root.datasource_fingerprint, root.entity_fingerprint,
             root.event_id, trace.event_id or 0,
             root.created_at or "", trace.created_at or ""),
        ).fetchone()
        n_between = int(row[0]) if row else 0
    return legacy_trace_shape(trace, root, n_between)


def _trace_depth(trace: AuditEvent, rows: Dict[int, AuditEvent]) -> int:
    """Links from *trace* down to the user event of its chain (1: direct)."""
    depth, cur, seen = 0, trace, set()
    while cur.restored_from_event_id is not None:
        ref = cur.restored_from_event_id
        if ref in seen:
            return 0
        seen.add(ref)
        depth += 1
        nxt = rows.get(ref)
        if nxt is None:
            return depth
        cur = nxt
    return depth


def _toggles_after(traces: List[AuditEvent], happened) -> List[tuple]:
    """The writes after the cutoff that toggled a root, oldest first.

    ``[(position, trace, kind)]``, kind ``"create"`` (the trace's own
    write) or ``"invalidate"`` (an Undo reverted a write made before the
    cutoff). A trace created AND invalidated after the cutoff is a
    neutral pair: its write and the Undo that reverted it both happened
    after T, and the Undo proved the state it reverted (a same-field edit
    in between would have stopped it).

    An Undo sits at ``rewind_dedup.undo_position``: several traces an
    older version stamped with one instant come back in the order that
    Undo reverted them, not in the order the undone run wrote them. The
    neutral pair only holds when the stamp IS the Undo of that trace: an
    older Undo that stamped several compensations of one row alike also
    stamped one it did not revert. Such a root never reaches here, it is
    refused before (:func:`_tied_undo_groups`).
    """
    out = []
    for t in traces:
        created_after = bool(t.created_at) and not happened(t.created_at)
        if created_after and t.invalidated_at:
            continue
        if created_after:
            out.append(((t.created_at, t.event_id or 0), t, "create"))
        elif t.invalidated_at and not happened(t.invalidated_at):
            out.append((undo_position(t), t, "invalidate"))
    out.sort(key=lambda x: x[0])
    return out


def _user_rows_between(conn: sqlite3.Connection, ds, entity: Optional[str],
                       low: tuple, high: tuple) -> List[int]:
    """User events of *ds* (of *entity* when given) strictly between two
    positions (created_at, event_id)."""
    placeholders = ",".join("?" for _ in ds)
    assert_safe_fragment(placeholders)
    query = "".join((
        "SELECT event_id, created_at FROM audit_event",  # nosec B608
        " WHERE datasource_fingerprint IN (", placeholders, ")",
        " AND restored_from_event_id IS NULL",
        " AND created_at >= ? AND created_at <= ?",
        " AND entity_fingerprint = ?" if entity else "",
    ))
    params = list(ds) + [low[0], high[0]] + ([entity] if entity else [])
    return [eid for eid, at in conn.execute(query, params)
            if low < (at or "", eid or 0) < high]


def _compensated_now(conn: sqlite3.Connection, ids) -> set:
    """The user events among *ids* that a trace in effect NOW compensates."""
    ids = set(i for i in ids if i is not None)
    if not ids:
        return set()
    tree: Dict[int, AuditEvent] = {}
    level = set(ids)
    for _level in range(_TREE_DEPTH):
        if not level:
            break
        children = _rows_where_in(conn, "restored_from_event_id", level)
        level = {c.event_id for c in children} - set(tree)
        for child in children:
            tree[child.event_id] = child
    traces = list(tree.values())
    status = traces_in_effect(traces)
    return {t.restored_from_event_id for t in traces
            if t.restored_from_event_id in ids and status.get(t.event_id)}


def _presence_is_exact(traces: List[AuditEvent], happened,
                       rows: Dict[int, AuditEvent], present_t: bool,
                       present_now: bool) -> bool:
    """True when the net reading of a root is the physical one.

    Every trace of the root is a write: a compensation (odd depth) makes
    its effect absent, a re-application (even depth) present, an Undo the
    reverse, the last write winning. The trace-status model
    (`traces_in_effect`) agrees with that replay while the writes
    alternate. It does not when a write does not flip the root: a second
    compensation of a root already compensated (a Mode A restore after
    the user did the edit again) physically undid THAT edit, and a later
    cancellation of the first compensation deleted the feature while the
    second one still reads "in effect" (rw_state_at_t_toggles: restored,
    deleted again, restored again, then rewound). Exact when: no exact
    write after the cutoff fails to flip, and the replay gives the model's
    state at the cutoff and now. A legacy write that does not flip is
    ignored (before v7 a restore that skipped still wrote its trace), a
    legacy trace-to-trace row too (unknown meaning).
    """
    writes = []
    for t in traces:
        depth = _trace_depth(t, rows)
        if depth == 0 or (depth >= 2 and not is_exact_trace(t)):
            continue
        odd = depth % 2 == 1
        exact = is_exact_trace(t)
        if t.created_at:
            writes.append(((t.created_at, t.event_id or 0), not odd, exact,
                           not happened(t.created_at)))
        if t.invalidated_at:
            writes.append((undo_position(t), odd, exact,
                           not happened(t.invalidated_at)))
    writes.sort(key=lambda w: w[0])
    state, at_t = True, None
    for _position, sets_present, exact, after in writes:
        if after and at_t is None:
            at_t = state
        if sets_present == state:
            if exact and after:
                return False
            continue
        state = sets_present
    if at_t is None:
        at_t = state
    return at_t == present_t and state == present_now


def _tied_undo_groups(traces: List[AuditEvent],
                      happened) -> List[List[AuditEvent]]:
    """Compensations of one row an older Undo stamped alike after the cutoff.

    ``[[trace, ...], ...]``, each group two traces or more of the SAME row
    invalidated at one instant after the cutoff, oldest instant first. One
    run never writes two of them (a plan holding two rows of one id is
    refused), so they come from different runs: the Undo of a version
    before this one stamped every active trace of the events it undid
    (``restored_from_event_id IN (...)``), an earlier run's included,
    which it did not revert. Which of them it reverted is not in the
    journal, and the one it did not revert is still written under the
    edits that followed it.

    Every trace of the root counts, whatever its creation date. A trace
    written before the cutoff puts the tie in the state at T (a Mode A
    restore, the edit typed again, a second restore, a Rewind to a date
    between the two). A trace written AND stamped after the cutoff would
    be read as a neutral pair (:func:`_toggles_after`), which is exactly
    what the tie makes false: the window then compensated the edit typed
    again and wrote the value before it where the field held another one
    at T, reported restored (rw_undo_tied_stamps_order, T3_s*_150 and T6).
    Legacy and exact traces alike: the published Undo only ever met
    legacy ones. Traces of DIFFERENT rows stamped alike are one run's,
    ordered by ``rewind_dedup.undo_position``.
    """
    stamped: Dict[tuple, List[AuditEvent]] = {}
    for t in traces:
        if t.invalidated_at and not happened(t.invalidated_at):
            stamped.setdefault((t.invalidated_at, t.restored_from_event_id),
                               []).append(t)
    return [sorted(group, key=lambda t: t.event_id or 0)
            for (_at, _src), group in sorted(
                stamped.items(), key=lambda kv: (kv[0][0], kv[0][1] or 0))
            if len(group) >= 2]


def _tied_undo_reason(root_id: int, groups: List[List[AuditEvent]]) -> str:
    """The refusal naming the last Undo of *groups* (``_tied_undo_groups``)."""
    group = groups[-1]
    instant = group[0].invalidated_at
    source = group[0].restored_from_event_id
    row = (f"event {root_id}" if source == root_id
           else f"row {source} of the history of event {root_id}")
    others = [g[0].invalidated_at for g in groups[:-1]]
    more = (f", and the Undo(s) of {', '.join(others[:3])} did the same"
            if others else "")
    return (
        f"the Undo of {instant} stamped {len(group)} compensations of {row} "
        f"with one instant (traces {[t.event_id or 0 for t in group]}, "
        f"written by different runs){more}: a version of RecoverLand before "
        f"this one marked every compensation of an undone event as undone, "
        f"even one that Undo did not revert, and which of them it reverted "
        f"is not in the journal, so the state at the cutoff cannot be "
        f"rebuilt")


def _per_toggle_items(root: AuditEvent, toggles: List[tuple],
                      rows: Dict[int, AuditEvent]) -> Tuple[List[AuditEvent], str]:
    """One plan item per write after the cutoff, each at its own place.

    (items, "") or ([], refusal reason). The inverse of each write, by the
    depth of its trace: an odd depth wrote the compensation of the root,
    its undo re-applies the root (the new trace cancels it); an even depth
    (a forward hop) re-applied the root, its undo compensates it again
    (the new trace cancels the hop). An Undo of a compensation made
    before the cutoff re-applied the root: compensated again, directly.
    Toggles an older Undo stamped alike never reach here: the root is
    refused by name before the split (:func:`_tied_undo_groups`).
    """
    items: List[AuditEvent] = []
    for position, trace, kind in toggles:
        if not is_exact_trace(trace):
            return [], (
                f"compensation {trace.event_id} written on {trace.created_at} "
                f"by an older version of RecoverLand is one of several "
                f"writes on event {root.event_id} after the cutoff, with "
                f"edits in between: they cannot be undone in order")
        odd = _trace_depth(trace, rows) % 2 == 1
        if kind == "create":
            items.append(build_reapply_item(root, trace, position) if odd
                         else build_recompensate_item(root, position, via=trace))
        elif odd:
            items.append(build_recompensate_item(root, position))
        else:
            return [], (
                f"the undo on {trace.invalidated_at} of the re-application "
                f"{trace.event_id} of event {root.event_id} is one of several "
                f"writes after the cutoff, with edits in between: it cannot "
                f"be undone in order")
    ids = [i.event_id for i in items]
    if len(set(ids)) != len(ids):
        return [], (
            f"event {root.event_id} was compensated several times after the "
            f"cutoff by writes the journal names alike: they cannot be "
            f"undone one by one")
    return items, ""


def _history_plan_items(conn: sqlite3.Connection, scope,
                        cutoff: RestoreCutoff,
                        trace_id: str = "") -> List[AuditEvent]:
    """Plan items for the events BEFORE the cutoff whose state changed since.

    The state at T is every action that happened by T: user events, and
    compensations (traces) with their own dates. A compensation made
    before T is part of it. An event older than T therefore only needs
    work when a trace TOGGLED it after T:
      - present at T, compensated now by a trace made after T: re-apply it
        (``build_reapply_item``, the forward hop); the new trace written
        for that row cancels the old one;
      - compensated at T, present now because that compensation was undone
        after T (invalidated by an Undo, or cancelled by a forward hop):
        compensate it again (``build_recompensate_item``);
      - either, when the journal cannot do it exactly (the event was
        purged by retention, or the compensation was a legacy fused one):
        a refusal that names the event and the date to choose instead;
      - whatever the rest, when an older Undo stamped several
        compensations of one row with one instant after T: a refusal
        naming that Undo, checked first, on every trace of the root
        (:func:`_tied_undo_groups`).

    Each item is dated at the LAST toggle after T, which puts it at its
    place in the newest-first order of the window: undoing the toggles of
    the history and compensating the user events made after them happen
    in one chronological run (an edit made after a rewind is compensated
    before the rewind's compensation is undone). One item for the net
    effect is only exact when no user edit lies between the toggles: with
    one in between, every write after T gets its own item at its own
    place (``_per_toggle_items``), or a refusal when the writes cannot be
    ordered.

    Events after T are not handled here: the window holds them with their
    traces, and the collapse reads their state now.
    """
    ds = [fp for fp in (scope or []) if fp]
    instant = _cutoff_instant(conn, cutoff)
    if not ds or not instant:
        return []
    inclusive = bool(cutoff.inclusive)
    after_op = ">=" if inclusive else ">"

    def happened(ts) -> bool:
        if not ts:
            return False
        return ts < instant if inclusive else ts <= instant

    placeholders = ",".join("?" for _ in ds)
    assert_safe_fragment(_EVENT_COLUMNS)
    assert_safe_fragment(placeholders)
    assert_safe_fragment(after_op)
    # B608: fragments are static branches; every value goes through `?`.
    query = "".join((
        "SELECT ", _EVENT_COLUMNS, " FROM audit_event",  # nosec B608
        " WHERE datasource_fingerprint IN (", placeholders, ")",
        " AND restored_from_event_id IS NOT NULL",
        " AND (created_at ", after_op, " ?",
        " OR (invalidated_at IS NOT NULL AND invalidated_at ", after_op,
        " ?))",
    ))
    toggles = [_row_to_event(r) for r in conn.execute(
        query, list(ds) + [instant, instant])]
    if not toggles:
        return []

    # Climb every toggled trace to the user event its chain ends on.
    rows: Dict[int, AuditEvent] = {t.event_id: t for t in toggles}
    missing: set = set()
    frontier = {t.restored_from_event_id for t in toggles} - set(rows)
    for _level in range(_TREE_DEPTH):
        if not frontier:
            break
        found = {r.event_id: r
                 for r in _rows_where_in(conn, "event_id", frontier)}
        missing |= frontier - set(found)
        rows.update(found)
        frontier = {r.restored_from_event_id for r in found.values()
                    if r.restored_from_event_id is not None}
        frontier -= set(rows) | missing

    def before_cutoff(root: AuditEvent) -> bool:
        # The exact complement of the window's user-event predicate
        # (_cutoff_where), so a root is handled here or there, never both:
        # a BY_EVENT_ID window selects on the id, and an edit re-imported
        # from the pending file has an old date but a recent id.
        if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
            try:
                bound = int(cutoff.value)
            except (TypeError, ValueError):
                return happened(root.created_at)
            eid = root.event_id or 0
            return eid < bound if inclusive else eid <= bound
        return happened(root.created_at)

    roots: Dict[int, Optional[AuditEvent]] = {}
    for toggle in toggles:
        root, root_id = _chain_root(toggle, rows)
        if root_id is None:
            continue
        if root is not None and not before_cutoff(root):
            continue  # after T: in the window with its traces
        roots[root_id] = root
    if not roots:
        return []

    # Every trace below each root, whatever its date: presence at T needs
    # the compensations made before T as much as those made after it.
    tree: Dict[int, AuditEvent] = {}
    expanded: set = set()
    level = set(roots)
    for _level in range(_TREE_DEPTH):
        level -= expanded
        if not level:
            break
        expanded |= level
        children = _rows_where_in(conn, "restored_from_event_id", level)
        for child in children:
            tree[child.event_id] = child
        level = {c.event_id for c in children}
    rows.update(tree)
    per_root: Dict[int, List[AuditEvent]] = {rid: [] for rid in roots}
    for trace in tree.values():
        _root, rid = _chain_root(trace, rows)
        if rid in per_root:
            per_root[rid].append(trace)

    items: List[AuditEvent] = []
    counts = {"reapply": 0, "recompensate": 0, "refused": 0, "per_toggle": 0}
    for rid, root in roots.items():
        traces = per_root.get(rid) or []
        # Before any reading of the root: once an older Undo has stamped two
        # compensations of one row alike after T, neither the net reading
        # nor the write-by-write one is the physical history (one of them
        # was never reverted). Every trace counts, neutral pairs and legacy
        # traces included; a check on the live toggles of the per-write
        # path alone let the net path (every published journal) and the
        # neutral pairs write a value the field never held at T.
        tied = _tied_undo_groups(traces, happened)
        if tied:
            anchor = max(tied[-1], key=lambda t: t.event_id or 0)
            position = undo_position(anchor)
            flog(f"event_stream_repository: root eid={rid} tied undo stamps "
                 f"after the cutoff "
                 f"{[(g[0].invalidated_at, [t.event_id for t in g]) for g in tied][:5]}: "
                 f"refused", "WARNING")
            items.append(build_refusal_item(
                anchor, position,
                f"{_tied_undo_reason(rid, tied)}; choose a date after "
                f"{position[0]}"))
            counts["refused"] += 1
            continue
        status_t = traces_in_effect(traces, happened)
        status_now = traces_in_effect(traces)
        direct = [t for t in traces if t.restored_from_event_id == rid]
        present_t = not any(status_t.get(t.event_id) for t in direct)
        present_now = not any(status_now.get(t.event_id) for t in direct)
        live = _toggles_after(traces, happened)
        # Several writes on this root after T. Their NET effect (one item
        # at the last toggle) is only right when they alternate from the
        # state at T (compensated, re-applied, compensated...) and nothing
        # else was written on the feature between them. A write that
        # compensates a root already compensated (a second Mode A restore
        # after the user did the edit again) physically undid THAT edit:
        # the net reading skips it, and the field lands on the wrong value,
        # or the deleted feature comes back, with every proof passing
        # (review finding double-toggle; rw_state_at_t_toggles). Same with
        # a user edit of the feature between two writes, compensated since
        # or not: the collapse unwinds such an edit row by row, each
        # compensation of it at its place (`rewind_dedup._window_per_write`,
        # edits between two history items), and the net item at the last
        # toggle would then meet states those rows do not leave. Then
        # every write is undone at its own place. An UPDATE keeps its
        # identity: the edits that count are the feature's. A re-inserted
        # feature changes FID on some providers, so for a DELETE root any
        # edit of the layer counts (the collapse cancels the
        # re-insert/delete pairs and ties the new FID to the root,
        # `_pair_reinserted_lifetimes`). An INSERT root's writes cannot be
        # ordered by the planner's phases (re-inserts first): refused by
        # name.
        between: List[int] = []
        per_write = False
        if root is not None and live:
            op = root.operation_type
            if not _presence_is_exact(traces, happened, rows, present_t,
                                      present_now):
                per_write = True
            elif len(live) >= 2:
                between = _user_rows_between(
                    conn, ds, None if op == "DELETE" else root.entity_fingerprint,
                    live[0][0], live[-1][0])
                if between and op == "INSERT":
                    # A creation cannot be replayed write by write (refused
                    # below): its net reading stays, and is exact, while the
                    # edits between were all compensated since (neutral
                    # pairs the window drops too).
                    done = _compensated_now(conn, between)
                    between = [eid for eid in between if eid not in done]
                per_write = bool(between)
        if per_write:
            reason = ""
            per_toggle: List[AuditEvent] = []
            if op in ("UPDATE", "DELETE"):
                per_toggle, reason = _per_toggle_items(root, live, rows)
            else:
                reason = (
                    f"the creation {rid} was deleted and re-inserted "
                    f"{len(live)} time(s) after the cutoff, not one write "
                    f"after the other or with edits in between "
                    f"({between[:5]}): these writes cannot be undone in order")
            if per_toggle:
                flog(f"event_stream_repository: root eid={rid} op={op} "
                     f"{len(live)} write(s) after the cutoff, not alternating "
                     f"or with user edits {between[:8]} between them: one "
                     f"item per write, each at its place", "INFO")
                items.extend(per_toggle)
                counts["per_toggle"] += 1
                for item in per_toggle:
                    counts["reapply" if plan_item_kind(item) == PLAN_MARK_REAPPLY
                           else "recompensate"] += 1
                continue
            when = live[-1][0][0]
            items.append(build_refusal_item(
                live[-1][1], live[-1][0],
                f"{reason}; choose a date at or after {when}"))
            counts["refused"] += 1
            continue
        if present_t == present_now:
            continue
        # The last toggle after T: (instant, trace event_id), the id breaking
        # ties between traces a run wrote within one clock reading; an Undo
        # at its `undo_position` (one instant for a whole Undo before this
        # version: the trace written last was reverted first).
        stamps = [(t.created_at, t.event_id or 0) for t in traces
                  if t.created_at and not happened(t.created_at)]
        stamps += [undo_position(t) for t in traces
                   if t.invalidated_at and not happened(t.invalidated_at)]
        position = max(stamps) if stamps else (instant, 0)
        when = position[0]
        if root is None:
            anchor = max(direct or traces, key=_window_order)
            reason = (
                f"event {rid} was compensated or re-applied on {when} "
                f"but retention has purged it: the state at {instant} "
                f"cannot be rebuilt; choose a date at or after {when}")
            items.append(build_refusal_item(anchor, position, reason))
            counts["refused"] += 1
            continue
        if present_t:
            trace = cancelling_trace(rid, traces, status_now)
        else:
            trace = cancelling_trace(rid, traces, status_t)
        shape = LEGACY_EXACT if trace is None else _legacy_shape(conn, trace, root)
        if shape != LEGACY_EXACT:
            what = ("may have fused several edits into one (or covered this "
                    "one alone: the journal cannot tell)"
                    if shape == LEGACY_AMBIGUOUS
                    else "fused several edits into one")
            reason = (
                f"the compensation of event {rid} written on "
                f"{trace.created_at} by an older version of RecoverLand "
                f"{what}: it cannot be reversed exactly; choose a date at "
                f"or after {when}")
            items.append(build_refusal_item(trace, position, reason))
            counts["refused"] += 1
            continue
        if present_t:
            items.append(build_reapply_item(root, trace, position))
            counts["reapply"] += 1
        else:
            items.append(build_recompensate_item(root, position))
            counts["recompensate"] += 1

    items = _canonicalize_events(conn, items)
    n_missing = sum(1 for r in roots.values() if r is None)
    flog(
        f"event_stream_repository: history_before_cutoff trace_id={trace_id} "
        f"cutoff={instant} inclusive={inclusive} toggles={len(toggles)} "
        f"roots={len(roots)} missing_roots={n_missing} "
        f"reapply={counts['reapply']} recompensate={counts['recompensate']} "
        f"refused={counts['refused']} per_toggle_roots={counts['per_toggle']}",
        "WARNING" if counts["refused"] else "INFO",
    )
    return items
