"""Event stream repository for temporal restore (BL-03).

Bounded, indexed reads against the SQLite audit journal.
All queries use parameterized SQL and respect volume limits.
No QGIS dependency.
"""
import sqlite3
from typing import List, NamedTuple, Optional

from .audit_backend import AuditEvent
from .datasource_alias import (
    canonicalize_event_fingerprints, expand_fingerprints,
)
from .search_service import _row_to_event
from .restore_contracts import (
    RestoreCutoff, CutoffType, MAX_EVENTS_PER_RESTORE,
)
from .logger import flog, timed_op
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
    """Fetch all events for a single entity, ordered by event_id ASC."""
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
        " ORDER BY event_id ASC LIMIT ?"
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
    """Active traces whose compensated event lies BEFORE the cutoff.

    Undoing a trace re-applies the user event it compensated, so this is
    exactly the set of compensations to undo in order to move the layers
    FORWARD to *cutoff* -- the case "I rewound to an old date, now I want a
    more recent one".

    The predicate is the strict complement of the one
    :func:`fetch_events_after_cutoff` uses to pick what must be compensated
    (``>=`` when the cutoff is inclusive, ``>`` otherwise). Complement, so
    every event of the history falls in exactly one of the two sets: no
    event can be both compensated and re-applied, none can be forgotten.

    Everything is derived from the journal, never from dialog state: the
    in-memory ``_last_restore_by_ds`` is lost when QGIS closes, while the
    traces are durable. Without this, forward navigation after a restart
    silently did nothing (scenario rw_multi_hop_navigation).

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

    placeholders = ",".join("?" for _ in datasource_fps)
    assert_safe_fragment(_EVENT_COLUMNS)
    assert_safe_fragment(cutoff_col)
    assert_safe_fragment(op)
    # B608: fragments are static branches; every value goes through `?`.
    query = "".join((
        "SELECT ", _EVENT_COLUMNS, " FROM audit_event",  # nosec B608
        " WHERE datasource_fingerprint IN (", placeholders, ")",
        " AND restored_from_event_id IS NOT NULL",
        " AND invalidated_at IS NULL",
        " AND restored_from_event_id IN (",
        "SELECT event_id FROM audit_event WHERE ",
        "datasource_fingerprint IN (", placeholders, ") AND ",
        cutoff_col, " ", op, " ?",
        ")",
        " ORDER BY created_at DESC, event_id DESC",
    ))
    params = list(datasource_fps) + list(datasource_fps) + [cutoff.value]
    traces = _canonicalize_events(
        conn, [_row_to_event(r) for r in conn.execute(query, params).fetchall()])
    flog(
        f"fetch_active_traces_before_cutoff: trace_id={trace_id} "
        f"datasource_count={len(datasource_fps)} "
        f"cutoff_type={cutoff.cutoff_type.value} "
        f"inclusive={cutoff.inclusive} "
        f"n_traces={len(traces)}",
        "INFO" if traces else "DEBUG",
    )
    return traces


def fetch_events_after_cutoff(
    conn: sqlite3.Connection,
    datasource_fp: str,
    cutoff: RestoreCutoff,
    limit: int = MAX_EVENTS_PER_RESTORE,
    trace_id: str = "",
    include_traces: bool = True,
) -> List[AuditEvent]:
    """Fetch events after a cutoff, ordered for reverse replay (DESC).

    Returns events ordered by created_at DESC, event_id DESC so the caller can
    apply compensatory operations from most recent to oldest. event_id is
    only a tie-breaker because late pending-recovery inserts may hold an
    event_id that no longer reflects chronological order.

    When *include_traces* is True, restore_trace_events are included in the
    result. The rewind planner needs them to detect that an entity has
    already been restored by a previous rewind and to avoid re-applying
    a compensatory action (which causes feature accumulation).
    """
    with timed_op("fetch_events_after_cutoff", trace_id):
        where, params = _cutoff_where(resolve_scope(conn, datasource_fp), cutoff,
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
        if len(events) >= limit:
            n_total = count_events_after_cutoff(
                conn, datasource_fp, cutoff, trace_id=trace_id,
                include_traces=include_traces)
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
        return events


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
    """Count events after a cutoff without loading them.

    When *include_traces* is True, restore_trace_events are counted as well.
    Must match the value used at fetch time so the count and the fetched
    list stay consistent.
    """
    with timed_op("count_events_after_cutoff", trace_id):
        where, params = _cutoff_where(resolve_scope(conn, datasource_fp), cutoff,
                                      include_traces=include_traces)
        if where is None:
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
        assert_safe_fragment(where)
        # B608: `where` is generated by _cutoff_where; values via `params`.
        row = conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE " + where, params  # nosec B608
        ).fetchone()
        n_events = row[0] if row else 0
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


def fetch_events_by_ids(
    conn: sqlite3.Connection,
    event_ids: List[int],
) -> List[AuditEvent]:
    """Fetch specific events by ID, ordered by event_id DESC."""
    if not event_ids:
        return []
    placeholders = ",".join("?" for _ in event_ids)
    assert_safe_fragment(_EVENT_COLUMNS)
    assert_safe_fragment(placeholders)
    # B608: `placeholders` is only `?,?,...`; event_ids bound via params.
    query = (
        "SELECT " + _EVENT_COLUMNS + " FROM audit_event"  # nosec B608
        " WHERE event_id IN (" + placeholders + ")"
        " ORDER BY event_id DESC"
    )
    return _canonicalize_events(
        conn, [_row_to_event(r) for r in conn.execute(query, event_ids).fetchall()])


def fetch_events_by_session(
    conn: sqlite3.Connection,
    session_id: str,
    limit: int = MAX_EVENTS_PER_RESTORE,
) -> List[AuditEvent]:
    """US-4.10.01: Fetch user events of an editing session for inverse replay.

    Returns events ordered by event_id DESC (latest first) so UndoRunner can
    apply compensatory operations from most recent to oldest. Excludes
    restore_trace_events (restored_from_event_id IS NOT NULL) to keep the
    undo population aligned with what the user really did.
    Uses idx_event_session for the WHERE clause.
    """
    if not session_id:
        return []
    assert_safe_fragment(_EVENT_COLUMNS)
    # B608: _EVENT_COLUMNS is a static whitelist; values via `?`.
    query = (
        "SELECT " + _EVENT_COLUMNS + " FROM audit_event"  # nosec B608
        " WHERE session_id = ? AND restored_from_event_id IS NULL"
        " ORDER BY event_id DESC LIMIT ?"
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
    # that an entity has already been compensated, and chain fusion makes
    # one trace stand for several events -- it references only the OLDEST
    # of the fused chain. When retention purges that referenced event, the
    # membership test above stops matching and the trace becomes invisible
    # although its row is still there (PurgeOptions.orphan_traces defaults
    # to False). The next rewind then re-compensates the same DELETE, and
    # the compensation of a DELETE is an INSERT: the feature comes back a
    # SECOND time. Such a trace is read on its own date, and only when its
    # source is gone from the journal ENTIRELY -- not merely older than the
    # cutoff, which is the ordinary case where the trace must stay out.
    # `NOT EXISTS` on the primary key, one lookup per trace row.
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
