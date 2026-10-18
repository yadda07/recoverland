"""Retention, purge and compaction for RecoverLand (RLU-013).

Manages journal size by deleting old events and running VACUUM.
All purge operations are explicit and confirmed; never implicit.
"""
import json
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, NamedTuple, Optional, Callable, Tuple

from .logger import flog, timed_op

_vacuum_lock = threading.Lock()
_PURGE_BATCH_SIZE = 5000
# Distinct timestamps (i.e. whole commits) deleted per retention batch.
_PURGE_TS_BATCH = 500

# Newest date a purge of this session removed, PER DATASOURCE.
# Per datasource and not per journal: a single global horizon made one
# purge refuse every rewind older than the oldest surviving event of ANY
# layer, including layers the purge had never touched.
_purge_horizon_lock = threading.Lock()
_purge_horizon: Dict[str, str] = {}
# Intervals a logical purge of this session made unrebuildable, PER
# DATASOURCE: [(first date, last date)], both ends refused.
_purge_gaps: Dict[str, List[Tuple[str, str]]] = {}

# The intervals, durably. A logical purge (INSERT/DELETE pair, invalidated
# trace) removes the history of ONE interval, not everything before it: a
# pair (t1, t2) only rebuilt the dates between its two events, a trace in
# effect from c to i only the dates between those. Recording its end as a
# floor refused every date of the layer before it, for ever, after one
# routine maintenance click (review finding purge-horizon-overreach).
# Created here, on first use, like the rows it holds.
_GAP_DDL = (
    "CREATE TABLE IF NOT EXISTS journal_coverage_gap ("
    "datasource_fingerprint TEXT NOT NULL, "
    "gap_from TEXT NOT NULL, "
    "gap_through TEXT NOT NULL, "
    "kind TEXT, "
    "recorded_at TEXT NOT NULL, "
    "PRIMARY KEY (datasource_fingerprint, gap_from, gap_through))"
)


class PurgeResult(NamedTuple):
    deleted_count: int
    vacuum_done: bool
    error: str
    retention_deleted: int = 0
    excess_deleted: int = 0
    invalidated_deleted: int = 0
    pair_deleted: int = 0
    orphan_trace_deleted: int = 0


class RetentionPolicy(NamedTuple):
    retention_days: int
    max_events: int


class PurgeOptions(NamedTuple):
    """Explicit scope for a purge operation. No boolean parameters."""

    retention: bool = True
    invalidated: bool = False
    insert_delete_pairs: bool = False
    orphan_traces: bool = False


class LogicalGarbageCount(NamedTuple):
    """Breakdown of purgeable logical garbage in the journal."""

    old_events: int
    invalidated_events: int
    insert_delete_pairs: int
    orphan_traces: int
    total: int


DEFAULT_POLICY = RetentionPolicy(retention_days=365, max_events=1_000_000)


def count_purgeable_events(conn: sqlite3.Connection, policy: RetentionPolicy) -> int:
    """Count events older than the retention period."""
    cutoff = _compute_cutoff(policy.retention_days)
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE created_at < ?", (cutoff,)
    ).fetchone()
    return row[0] if row else 0


def count_logical_garbage_events(conn: sqlite3.Connection) -> LogicalGarbageCount:
    """Count logical garbage: invalidated events, orphan traces, INSERT/DELETE pairs."""
    invalidated = _count_invalidated_events(conn)
    pairs = _count_insert_delete_pairs(conn)
    orphan = _count_orphan_traces(conn)
    total = invalidated + (pairs * 2) + orphan
    return LogicalGarbageCount(
        old_events=0,
        invalidated_events=invalidated,
        insert_delete_pairs=pairs,
        orphan_traces=orphan,
        total=total,
    )


def _count_invalidated_events(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE invalidated_at IS NOT NULL"
    ).fetchone()
    return int(row[0] if row else 0)


# The INSERT/DELETE pairs a logical purge may treat as noise: an INSERT
# immediately followed by the DELETE of the same entity, in one edit
# session of ONE layer, among USER events only, and neither event
# referenced by a trace. The partition used to be (entity_fingerprint,
# session_id) with no datasource and no filter on traces. Every trace has
# session_id NULL, so the traces of 'fid:5' of EVERY layer fell in one
# partition, and an INSERT trace of layer A followed by a DELETE trace of
# layer B was deleted as a "pair": the next rewind to the same date
# re-inserted A's feature a second time and failed on B (finding
# logical-purge-deletes-active-traces). Chronological order, like the
# rewind (an event re-imported from the pending file keeps its date).
_PAIRS_CTE = (
    "WITH ordered AS ("
    "  SELECT event_id, operation_type, datasource_fingerprint, created_at,"
    "    LEAD(event_id) OVER ("
    "      PARTITION BY datasource_fingerprint, entity_fingerprint, session_id"
    "      ORDER BY created_at, event_id"
    "    ) AS next_id,"
    "    LEAD(operation_type) OVER ("
    "      PARTITION BY datasource_fingerprint, entity_fingerprint, session_id"
    "      ORDER BY created_at, event_id"
    "    ) AS next_op,"
    "    LEAD(created_at) OVER ("
    "      PARTITION BY datasource_fingerprint, entity_fingerprint, session_id"
    "      ORDER BY created_at, event_id"
    "    ) AS next_at"
    "  FROM audit_event"
    "  WHERE restored_from_event_id IS NULL AND invalidated_at IS NULL"
    "    AND entity_fingerprint IS NOT NULL AND session_id IS NOT NULL"
    "),"
    "pairs AS ("
    "  SELECT event_id, next_id, datasource_fingerprint, created_at, next_at"
    "  FROM ordered"
    "  WHERE operation_type = 'INSERT' AND next_op = 'DELETE'"
    "    AND NOT EXISTS (SELECT 1 FROM audit_event t"
    "      WHERE t.restored_from_event_id IN (ordered.event_id, ordered.next_id))"
    ")"
)

# A trace whose referenced row is gone from the journal ENTIRELY. A trace
# that references another trace is not an orphan: since schema 7 it is how
# a rewind re-applies what a previous rewind compensated (it cancels that
# trace). The old test, "not a user event", deleted it and silently put
# the cancelled compensation back in effect.
_ORPHAN_TRACE_WHERE = (
    "restored_from_event_id IS NOT NULL AND NOT EXISTS ("
    "SELECT 1 FROM audit_event src"
    " WHERE src.event_id = audit_event.restored_from_event_id)"
)


class LogicalPurgeImpact(NamedTuple):
    """What each logical purge would make unreachable, before it runs.

    ``invalidated`` and ``insert_delete_pairs``: ``[(datasource, first
    date, last date)]``, the intervals a rewind will refuse afterwards.
    ``orphan_traces``: ``[(datasource, date)]``, every date at or before
    it will be refused on that layer.
    """

    invalidated: List[Tuple[str, str, str]]
    insert_delete_pairs: List[Tuple[str, str, str]]
    orphan_traces: List[Tuple[str, str]]


def logical_purge_impact(conn: sqlite3.Connection) -> LogicalPurgeImpact:
    """The dates each logical purge would take away from the rewind.

    The same selections the purges make (`_purge_invalidated_events`,
    `_purge_insert_delete_pairs`, `_purge_orphan_traces`), read without
    deleting anything, so the maintenance dialog can say it BEFORE the
    user confirms.
    """
    try:
        invalidated = [tuple(r) for r in conn.execute(
            "SELECT datasource_fingerprint, created_at,"
            " MAX(created_at, COALESCE(invalidated_at, ''))"
            " FROM audit_event WHERE invalidated_at IS NOT NULL")]
        pairs = [tuple(r) for r in conn.execute(
            _PAIRS_CTE + " SELECT datasource_fingerprint, created_at, next_at"  # nosec B608
            " FROM pairs")]
        orphans = [tuple(r) for r in conn.execute(
            "SELECT datasource_fingerprint,"  # nosec B608
            " MAX(created_at, COALESCE(invalidated_at, ''))"
            " FROM audit_event WHERE " + _ORPHAN_TRACE_WHERE)]
    except sqlite3.Error as exc:
        flog(f"retention: purge impact unreadable ({exc})", "WARNING")
        return LogicalPurgeImpact([], [], [])
    return LogicalPurgeImpact(invalidated, pairs, orphans)


def _count_insert_delete_pairs(conn: sqlite3.Connection) -> int:
    """Count the pairs `_purge_insert_delete_pairs` would delete."""
    row = conn.execute(
        _PAIRS_CTE + " SELECT COUNT(*) FROM pairs"  # nosec B608
    ).fetchone()
    return int(row[0] if row else 0)


def _count_orphan_traces(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE " + _ORPHAN_TRACE_WHERE  # nosec B608
    ).fetchone()
    return int(row[0] if row else 0)


def _family(conn: sqlite3.Connection, fingerprint: str) -> set:
    """The alias family of *fingerprint* (itself included)."""
    try:
        from .datasource_alias import expand_fingerprints
        family = set(expand_fingerprints(conn, fingerprint))
    except Exception:  # noqa: BLE001 - alias table absent on old journals
        family = set()
    family.add(fingerprint)
    return family


def _remember_deleted_history(conn: sqlite3.Connection, deleted) -> None:
    """Horizon of a logical purge whose loss reaches back to the start.

    *deleted* is ``[(datasource_fingerprint, newest instant the row
    mattered to)]``. Used for the orphan traces: such a trace is what says
    that every date before it needs an event retention already purged, so
    everything up to it is lost. Without a horizon, a rewind to such a
    date replays what is left and announces a state that never existed;
    with it, the preflight refuses that date by name (`coverage_refusal`,
    `restore_planner.preflight_check`). The logical purges that only lose
    an INTERVAL record it with `_remember_gaps`. Recorded for the whole
    alias family, in the caller's transaction.
    """
    horizons: Dict[str, str] = {}
    counts: Dict[str, int] = {}
    for fingerprint, stamp in deleted:
        if not fingerprint or not stamp:
            continue
        for member in _family(conn, fingerprint):
            if stamp > horizons.get(member, ""):
                horizons[member] = stamp
            counts[member] = counts.get(member, 0) + 1
    if horizons:
        _remember_horizons(conn, horizons, counts)


def _remember_gaps(conn: sqlite3.Connection, gaps, kind: str) -> None:
    """Intervals a logical purge made unrebuildable, per datasource family.

    *gaps* is ``[(datasource_fingerprint, first instant, last instant)]``:
    an INSERT/DELETE pair rebuilt the dates between its two events, an
    invalidated trace the dates it was in effect. Only those dates are
    refused afterwards (`coverage_refusal`), both ends included: an
    inclusive and an exclusive cutoff at an end see different sides of
    the event. Overlapping intervals of one datasource are merged. Session
    memory and journal rows, in the caller's transaction.
    """
    per_member: Dict[str, List[Tuple[str, str]]] = {}
    for fingerprint, low, high in gaps:
        if not fingerprint or not low or not high:
            continue
        if high < low:
            low, high = high, low
        for member in _family(conn, fingerprint):
            per_member.setdefault(member, []).append((low, high))
    if not per_member:
        return
    merged = {m: _merge_intervals(v) for m, v in per_member.items()}
    with _purge_horizon_lock:
        for member, intervals in merged.items():
            _purge_gaps[member] = _merge_intervals(
                _purge_gaps.get(member, []) + intervals)
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(_GAP_DDL)
        conn.executemany(
            "INSERT OR IGNORE INTO journal_coverage_gap (datasource_fingerprint, "
            "gap_from, gap_through, kind, recorded_at) VALUES (?,?,?,?,?)",
            [(member, low, high, kind, now)
             for member, intervals in merged.items() for low, high in intervals])
    except sqlite3.Error as exc:
        flog(f"retention: purged intervals NOT persisted ({exc}): after a "
             f"restart a rewind inside them would not be refused", "ERROR")
    flog(f"retention: {sum(len(v) for v in merged.values())} unrebuildable "
         f"interval(s) recorded ({kind}) for {len(merged)} datasource "
         f"fingerprint(s): rewinds inside them are refused, dates outside "
         f"them stay reachable")


def _merge_intervals(intervals) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for low, high in sorted(intervals):
        if out and low <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], high))
        else:
            out.append((low, high))
    return out


def purged_gaps(datasource_fp: str,
                conn: Optional[sqlite3.Connection] = None) -> List[Tuple[str, str]]:
    """Intervals a logical purge made unrebuildable for *datasource_fp*.

    Session memory, plus the journal's rows for the whole alias family
    when *conn* is given. Sorted and merged.
    """
    if not datasource_fp:
        return []
    family = _family(conn, datasource_fp) if conn is not None else {datasource_fp}
    with _purge_horizon_lock:
        found = [g for m in family for g in _purge_gaps.get(m, [])]
    if conn is not None:
        placeholders = ",".join("?" for _ in family)
        try:
            found += [tuple(r) for r in conn.execute(
                "SELECT gap_from, gap_through FROM journal_coverage_gap "  # nosec B608
                "WHERE datasource_fingerprint IN (" + placeholders + ")",
                list(family))]
        except sqlite3.Error:
            pass  # no table: no logical purge ever recorded an interval
    return _merge_intervals(found)


def purged_horizon(datasource_fp: str,
                   conn: Optional[sqlite3.Connection] = None) -> Optional[str]:
    """Newest date a purge removed for *datasource_fp*, or None.

    A purge is the only thing that makes a past state unreachable, and it
    is the only thing that knows it: once the rows are deleted, nothing in
    the journal says a date used to be covered. This horizon is what the
    rewind preflight (`restore_planner.preflight_check`) reads to refuse a
    date the journal cannot serve any more instead of replaying what
    survived and reporting success.

    Session memory, not journal memory: it only holds what THIS process
    purged. The durable guard is the oldest surviving event of the layer
    (`check_journal_coverage`), which callers pass to `preflight_check` as
    `oldest_event_date`; this one covers the case the plugin creates
    itself, the automatic purge at QGIS startup followed by a rewind in
    the same session.

    A single date for callers that can only hold one (Review's floor): it
    includes the end of every purged INTERVAL, so it refuses more than was
    lost, never less. The rewind asks `coverage_refusal`, which refuses
    only the intervals themselves.
    """
    if not datasource_fp:
        return None
    floor = purge_floor(datasource_fp, conn)
    gaps = purged_gaps(datasource_fp, conn)
    candidates = [h for h in (floor, gaps[-1][1] if gaps else None) if h]
    return max(candidates) if candidates else None


def purge_floor(datasource_fp: str,
                conn: Optional[sqlite3.Connection] = None) -> Optional[str]:
    """Newest date before which EVERYTHING was purged for *datasource_fp*.

    Retention, the excess purge and the orphan-trace purge remove history
    from the start: every date at or before this one is unrebuildable.
    Session memory and the journal's ``journal_coverage`` rows. A journal
    older than v7 with no row for the layer falls back to the historical
    guard in `coverage_refusal` (`_legacy_floor_refusal`), never to "no
    limit".
    """
    if not datasource_fp:
        return None
    with _purge_horizon_lock:
        session = _purge_horizon.get(datasource_fp)
    durable = durable_purge_horizon(conn, datasource_fp) if conn is not None else None
    candidates = [h for h in (session, durable) if h]
    return max(candidates) if candidates else None


def _inferred_v7_creation(conn: sqlite3.Connection) -> Optional[str]:
    """When v7 created this journal, deduced from schema_version, or None.

    The inference for a journal that holds no coverage stamp (made v7
    before journal_writes.coverage_origin existed). The migration stamps
    each version when it runs: a journal created by v7 has all its
    versions stamped within the same minute (its oldest stamp is its
    creation), a migrated one carries older stamps below 7. Unreadable,
    or no v7 stamp at all: None, assumed older (the guard errs on the
    side of refusing). A clock that was wrong when an older release
    created the file fools this inference; the stamp does not.
    """
    try:
        rows = conn.execute(
            "SELECT version_number, applied_at FROM schema_version").fetchall()
        stamps = [(int(v), str(at)) for v, at in rows if v is not None and at]
    except (sqlite3.Error, TypeError, ValueError):
        return None
    v7 = [at for v, at in stamps if v >= 7]
    if not v7:
        return None
    try:
        limit = (datetime.fromisoformat(min(v7)) - timedelta(seconds=60)).isoformat()
    except ValueError:
        return None
    if any(v < 7 and at < limit for v, at in stamps):
        return None
    return min(at for _v, at in stamps)


def _journal_predates_v7(conn: sqlite3.Connection) -> bool:
    """True when the journal lived under a release older than schema 7.

    Such a release purged at startup and kept the horizon in process
    memory only: nothing below the oldest surviving event of a layer is
    vouched for. The recorded stamp answers first
    (journal_writes.coverage_origin): "created" -> False; "migrated", or
    "" (the opener could not say) -> True. No stamp: the schema_version
    inference (_inferred_v7_creation). It used to be the inference alone,
    which a clock ahead when an older release created the file turns into
    "created by v7": a rewind below a silent purge was then accepted
    (tx_journal_coverage_origin_stamp, case C), and a legacy journal that
    never held a row refused the first edit after the upgrade (case I).
    """
    from .journal_writes import ORIGIN_CREATED, coverage_origin
    stamp = coverage_origin(conn)
    if stamp is not None:
        return stamp["origin"] != ORIGIN_CREATED
    return _inferred_v7_creation(conn) is None


_UNVOUCHED_FLOOR_KEY = "coverage_unvouched_floor_fps"


def has_unvouched_floor(conn: Optional[sqlite3.Connection],
                        datasource_fp: str) -> bool:
    """True when carried history of *datasource_fp* cannot vouch before its oldest row."""
    if conn is None or not datasource_fp:
        return False
    try:
        row = conn.execute(
            "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
            (_UNVOUCHED_FLOOR_KEY,),
        ).fetchone()
        if not row or not row[0]:
            return False
        stored = set(json.loads(row[0]))
    except (sqlite3.Error, ValueError, TypeError):
        return False
    if not stored:
        return False
    if datasource_fp in stored:
        return True
    return bool(_family(conn, datasource_fp) & stored)


def record_unvouched_floor(conn: sqlite3.Connection, fingerprints) -> None:
    """Record datasources whose history before their oldest row here is unvouched."""
    fps = {fp for fp in (fingerprints or ()) if fp}
    if not fps:
        return
    expanded = set()
    for fp in fps:
        expanded.update(_family(conn, fp))
    try:
        row = conn.execute(
            "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
            (_UNVOUCHED_FLOOR_KEY,),
        ).fetchone()
        if row and row[0]:
            expanded.update(json.loads(row[0]))
    except (sqlite3.Error, ValueError, TypeError):
        pass
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO backend_settings "
        "(setting_key, setting_value, updated_at) VALUES (?, ?, ?)",
        (_UNVOUCHED_FLOOR_KEY, json.dumps(sorted(expanded)), now),
    )


def _legacy_floor_refusal(conn: sqlite3.Connection, datasource_fp: str,
                          cutoff_value: str) -> Optional[str]:
    """The historical guard, for a datasource no purge record covers."""
    if durable_purge_horizon(conn, datasource_fp):
        return None
    predates = _journal_predates_v7(conn)
    unvouched = has_unvouched_floor(conn, datasource_fp)
    inferred_created = None
    if not predates and not unvouched:
        from .journal_writes import coverage_origin
        if coverage_origin(conn) is None:
            inferred_created = _inferred_v7_creation(conn)
        if inferred_created is None:
            return None
    try:
        from .event_stream_repository import get_oldest_event_date
        oldest = get_oldest_event_date(conn, datasource_fp)
    except sqlite3.Error as exc:
        flog(f"retention: oldest event unreadable ({exc}): the date cannot "
             f"be vouched for", "WARNING")
        return (f"History before this date cannot be vouched for (journal "
                f"older than v7, oldest event unreadable), requested: "
                f"{cutoff_value}")
    if inferred_created is not None and (not oldest or oldest >= inferred_created):
        return None
    if oldest and cutoff_value < oldest:
        return (f"History purged before this date by an older version of "
                f"RecoverLand, which did not record it. Oldest available: "
                f"{oldest}, requested: {cutoff_value}")
    return None


def durable_purge_horizon(conn: Optional[sqlite3.Connection],
                          datasource_fp: str) -> Optional[str]:
    """The purge horizon the JOURNAL remembers for *datasource_fp* (any alias).

    Survives restarts, unlike the session memory above. Alias-aware: the
    purge recorded the whole fingerprint family, the lookup accepts any
    member of it.
    """
    if conn is None or not datasource_fp:
        return None
    try:
        from .datasource_alias import expand_fingerprints
        family = set(expand_fingerprints(conn, datasource_fp))
    except Exception:  # noqa: BLE001 - alias table absent on old journals
        family = set()
    family.add(datasource_fp)
    placeholders = ",".join("?" for _ in family)
    try:
        row = conn.execute(
            "SELECT MAX(purged_through) FROM journal_coverage "  # nosec B608
            "WHERE datasource_fingerprint IN (" + placeholders + ")",
            list(family)).fetchone()
    except sqlite3.Error:
        return None
    return row[0] if row and row[0] else None


def journal_coverage(conn: sqlite3.Connection,
                     datasource_fp: Optional[str] = None) -> dict:
    """What the journal can really reconstruct, in the user's terms.

    ``recoverable_from``: the oldest date a rewind may target and get a
    complete answer (strictly after a purge horizon, not before the first
    captured event). ``purged_through``: newest date a purge removed from
    the start of the history. ``gaps``: the intervals a logical purge made
    unrebuildable, refused on their own (dates on both sides stay
    reachable). ``coverage_gap``: True when a purge removed history of
    this scope.
    """
    from .event_stream_repository import get_oldest_event_date
    oldest = newest = None
    try:
        oldest = get_oldest_event_date(conn, datasource_fp)
        if datasource_fp:
            from .event_stream_repository import resolve_scope
            scope = list(resolve_scope(conn, datasource_fp))
            ph = ",".join("?" for _ in scope)
            row = conn.execute(
                "SELECT MAX(created_at) FROM audit_event "  # nosec B608
                "WHERE datasource_fingerprint IN (" + ph + ")", scope).fetchone()
        else:
            row = conn.execute("SELECT MAX(created_at) FROM audit_event").fetchone()
        newest = row[0] if row else None
    except sqlite3.Error as exc:
        flog(f"retention: coverage lookup failed: {exc}", "WARNING")
    gaps: List[Tuple[str, str]] = []
    if datasource_fp:
        purged = purge_floor(datasource_fp, conn)
        gaps = purged_gaps(datasource_fp, conn)
    else:
        try:
            row = conn.execute(
                "SELECT MAX(purged_through) FROM journal_coverage").fetchone()
            purged = row[0] if row and row[0] else None
        except sqlite3.Error:
            purged = None
    recoverable_from = oldest
    if purged and (recoverable_from is None or purged >= recoverable_from):
        recoverable_from = purged
    return {
        "journal_oldest_event": oldest,
        "journal_newest_time": newest,
        "purged_through": purged,
        "recoverable_from": recoverable_from,
        "gaps": gaps,
        "coverage_gap": bool(purged or gaps),
    }


def coverage_refusal(conn: Optional[sqlite3.Connection], datasource_fp: str,
                     cutoff_value: str) -> Optional[str]:
    """Blocking reason when *cutoff_value* is not fully reconstructible.

    Three causes, each named: a purge that removed everything up to a
    date (retention, excess, orphan traces: the cutoff is at or before
    it); a logical purge that removed ONE interval (the cutoff falls
    inside it; dates before and after it stay reachable); a history this
    journal cannot vouch for below the oldest surviving event of the
    layer (the cutoff is before it): the journal lived under a release
    older than v7, which purged without any record
    (`_legacy_floor_refusal`, which also names the known limit of carried
    history). Without *conn*, the purges of this session only.
    """
    if not cutoff_value:
        return None
    floor = purge_floor(datasource_fp, conn)
    if floor and cutoff_value <= floor:
        return (f"History purged for this layer up to {floor}, "
                f"requested: {cutoff_value}")
    for low, high in purged_gaps(datasource_fp, conn):
        if low <= cutoff_value <= high:
            return (f"History of this layer purged between {low} and "
                    f"{high} (maintenance purge of logical garbage), "
                    f"requested: {cutoff_value}; a date before {low} or "
                    f"after {high} can be rebuilt")
    if conn is not None:
        return _legacy_floor_refusal(conn, datasource_fp, cutoff_value)
    return None


def _record_purge_horizon(conn: sqlite3.Connection, before_date: str) -> None:
    """Record, per datasource, the newest date about to be deleted.

    Must run BEFORE the DELETE, while the rows are still readable.
    """
    try:
        rows = conn.execute(
            "SELECT datasource_fingerprint, MAX(created_at) FROM audit_event"
            " WHERE created_at < ? GROUP BY datasource_fingerprint",
            (before_date,),
        ).fetchall()
    except sqlite3.Error as e:
        flog(f"retention: purge horizon capture failed: {e}", "WARNING")
        return
    if not rows:
        return
    from .datasource_alias import expand_fingerprints

    horizons: Dict[str, str] = {}
    for fingerprint, newest in rows:
        if not fingerprint or not newest:
            continue
        # The rewind names a layer by its canonical fingerprint while the
        # purged rows may carry an obsolete one. Recording the horizon for
        # the whole alias family is what keeps the guard from missing the
        # layer it is meant to protect.
        family = set(expand_fingerprints(conn, fingerprint))
        family.add(fingerprint)
        for member in family:
            if newest > horizons.get(member, ""):
                horizons[member] = newest
    if not horizons:
        return
    _remember_horizons(conn, horizons)
    flog(f"retention: purge horizon recorded before {before_date} for "
         f"{len(horizons)} datasource fingerprint(s)")


def _remember_horizons(conn: sqlite3.Connection, horizons: Dict[str, str],
                       counts: Optional[Dict[str, int]] = None) -> None:
    """Session memory + journal row, in the caller's transaction."""
    with _purge_horizon_lock:
        for fingerprint, newest in horizons.items():
            if newest > _purge_horizon.get(fingerprint, ""):
                _purge_horizon[fingerprint] = newest
    now = datetime.now(timezone.utc).isoformat()
    try:
        from .journal_writes import ensure_v7_schema
        ensure_v7_schema(conn)
        for fingerprint, newest in horizons.items():
            conn.execute(
                "INSERT INTO journal_coverage (datasource_fingerprint, "
                "purged_through, purged_events, updated_at) VALUES (?,?,?,?) "
                "ON CONFLICT(datasource_fingerprint) DO UPDATE SET "
                "purged_through = MAX(journal_coverage.purged_through, "
                "excluded.purged_through), "
                "purged_events = journal_coverage.purged_events + excluded.purged_events, "
                "updated_at = excluded.updated_at",
                (fingerprint, newest, int((counts or {}).get(fingerprint, 0)), now))
    except sqlite3.Error as exc:
        # The session memory still guards this session; say that the
        # journal will not remember it after a restart.
        flog(f"retention: purge horizon NOT persisted ({exc}): after a "
             f"restart a rewind before it would not be refused", "ERROR")


def purge_old_events(conn: sqlite3.Connection,
                     policy: RetentionPolicy,
                     trace_id: str = "") -> PurgeResult:
    """Delete events older than the retention period and enforce max_events.

    Convenience wrapper that purges only by retention/excess (no logical garbage).
    Does NOT run VACUUM inline; use vacuum_async() to reclaim space.
    """
    return purge_old_events_with_options(conn, policy, PurgeOptions(), trace_id=trace_id)


def purge_old_events_with_options(conn: sqlite3.Connection,
                                  policy: RetentionPolicy,
                                  options: PurgeOptions,
                                  trace_id: str = "") -> PurgeResult:
    """Delete events according to an explicit PurgeOptions scope.

    Deletes in batches of _PURGE_BATCH_SIZE to avoid holding the write
    lock for too long. Runs ANALYZE + WAL checkpoint after purge.
    Does NOT run VACUUM inline; use vacuum_async() to reclaim space.
    """
    retention_deleted = 0
    excess_deleted = 0
    invalidated_deleted = 0
    pair_deleted = 0
    orphan_trace_deleted = 0

    try:
        with timed_op("purge_old_events_with_options", trace_id):
            if options.retention:
                cutoff = _compute_cutoff(policy.retention_days)
                with conn:
                    _record_purge_horizon(conn, cutoff)
                # Batches of whole timestamps, oldest first: every event of
                # a commit shares one created_at, so no batch (and no crash
                # between two batches) can leave half a commit behind.
                while True:
                    with conn:
                        cursor = conn.execute(
                            "DELETE FROM audit_event WHERE created_at IN "
                            "(SELECT DISTINCT created_at FROM audit_event "
                            "WHERE created_at < ? ORDER BY created_at LIMIT ?)",
                            (cutoff, _PURGE_TS_BATCH),
                        )
                        batch_deleted = cursor.rowcount
                    retention_deleted += batch_deleted
                    if batch_deleted <= 0:
                        break
                if retention_deleted:
                    flog(f"retention: purged {retention_deleted} events older than {cutoff}")

                excess_deleted = _purge_excess(conn, policy.max_events)

            if options.invalidated:
                invalidated_deleted = _purge_invalidated_events(conn)

            if options.insert_delete_pairs:
                pair_deleted = _purge_insert_delete_pairs(conn)

            if options.orphan_traces:
                orphan_trace_deleted = _purge_orphan_traces(conn)

            total_deleted = (
                retention_deleted + excess_deleted + invalidated_deleted + pair_deleted + orphan_trace_deleted
            )

            if total_deleted > 0:
                _post_purge_maintenance(conn)
                from .datasource_registry import purge_orphan_datasources
                purge_orphan_datasources(conn)

            return PurgeResult(
                deleted_count=total_deleted,
                vacuum_done=False,
                error="",
                retention_deleted=retention_deleted,
                excess_deleted=excess_deleted,
                invalidated_deleted=invalidated_deleted,
                pair_deleted=pair_deleted,
                orphan_trace_deleted=orphan_trace_deleted,
            )

    except sqlite3.Error as e:
        flog(f"retention: purge error: {e}", "ERROR")
        total = (
            retention_deleted + excess_deleted + invalidated_deleted + pair_deleted + orphan_trace_deleted
        )
        return PurgeResult(
            deleted_count=total,
            vacuum_done=False,
            error=str(e),
            retention_deleted=retention_deleted,
            excess_deleted=excess_deleted,
            invalidated_deleted=invalidated_deleted,
            pair_deleted=pair_deleted,
            orphan_trace_deleted=orphan_trace_deleted,
        )


def purge_excess_events(conn: sqlite3.Connection, max_events: int) -> int:
    """Delete oldest events to enforce the max_events cap. Returns deleted count."""
    return _purge_excess(conn, max_events)


def _excess_cut_boundary(conn: sqlite3.Connection, to_delete: int) -> Optional[str]:
    """Return the created_at of the oldest event that must be kept.

    `to_delete` is a row count; this turns it into a timestamp so the cut
    can be aligned on a commit boundary.
    """
    row = conn.execute(
        "SELECT created_at FROM audit_event"
        " ORDER BY created_at ASC, event_id ASC LIMIT 1 OFFSET ?",
        (to_delete,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _next_commit_boundary(conn: sqlite3.Connection,
                          boundary: str) -> Optional[str]:
    """created_at of the first commit strictly after *boundary*, or None."""
    row = conn.execute(
        "SELECT MIN(created_at) FROM audit_event WHERE created_at > ?",
        (boundary,),
    ).fetchone()
    return row[0] if row and row[0] else None


def _commit_groups(conn: sqlite3.Connection) -> list:
    """Commits of the journal, oldest first: (key, is_legacy, n, newest_ts).

    A commit is identified by commit_id (schema v7) and, for rows written
    before v7, by their shared created_at. The timestamp is a clock
    reading, not an identity, which is why it is only the fallback.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_event)")}
    if "commit_id" in cols:
        rows = conn.execute(
            "SELECT COALESCE(commit_id, 'ts:' || created_at) AS k, "
            "commit_id IS NULL, COUNT(*), MIN(created_at), MAX(created_at), "
            "MIN(event_id) FROM audit_event GROUP BY k "
            "ORDER BY MIN(created_at), MIN(event_id)").fetchall()
    else:
        rows = conn.execute(
            "SELECT 'ts:' || created_at, 1, COUNT(*), MIN(created_at), "
            "MAX(created_at), MIN(event_id) FROM audit_event "
            "GROUP BY created_at ORDER BY MIN(created_at), MIN(event_id)").fetchall()
    return [(k, bool(legacy), int(n), t1) for k, legacy, n, _t0, t1, _e in rows]


def _purge_excess(conn: sqlite3.Connection, max_events: int) -> int:
    """Enforce max_events by deleting whole commits, oldest first.

    The cap is a promise to the user, and so is the integrity of a commit.
    Both hold: whole commits are deleted from the oldest until the journal
    is at or under the cap (the last commit removed may take it a little
    under). A commit is never cut in two: a later rewind would compensate
    half of its entities and report success.

    One limit, stated instead of hidden: the NEWEST commit is never
    deleted to honour the cap. If it alone exceeds the cap, the journal is
    left above it and a WARNING says so.

    Everything happens in ONE transaction, horizon included: an error
    cannot leave half a commit deleted, and the journal never forgets that
    a date stopped being covered.
    """
    if max_events <= 0:
        return 0
    total = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
    if total <= max_events:
        return 0
    to_delete = total - max_events
    try:
        groups = _commit_groups(conn)
        chosen, acc = [], 0
        for group in groups[:-1]:
            if acc >= to_delete:
                break
            chosen.append(group)
            acc += group[2]
        if not chosen:
            flog(f"retention: excess purge kept the whole journal: its "
                 f"{total} events form a single commit and the cap of "
                 f"{max_events} cannot be honoured without splitting it",
                 "WARNING")
            return 0
        with conn:
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS _rl_purge_keys "
                         "(k TEXT PRIMARY KEY, legacy INTEGER)")
            conn.execute("DELETE FROM _rl_purge_keys")
            conn.executemany(
                "INSERT OR IGNORE INTO _rl_purge_keys (k, legacy) VALUES (?, ?)",
                [(g[0][3:] if g[1] else g[0], 1 if g[1] else 0) for g in chosen])
            where = ("((commit_id IS NULL AND created_at IN "
                     "(SELECT k FROM _rl_purge_keys WHERE legacy = 1)) "
                     "OR commit_id IN (SELECT k FROM _rl_purge_keys WHERE legacy = 0))")
            cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_event)")}
            if "commit_id" not in cols:
                where = "created_at IN (SELECT k FROM _rl_purge_keys)"
            horizon_rows = conn.execute(
                "SELECT datasource_fingerprint, MAX(created_at), COUNT(*) "  # nosec B608
                "FROM audit_event WHERE " + where +
                " GROUP BY datasource_fingerprint").fetchall()
            horizons, counts = {}, {}
            from .datasource_alias import expand_fingerprints
            for fingerprint, newest, n in horizon_rows:
                if not fingerprint or not newest:
                    continue
                family = set(expand_fingerprints(conn, fingerprint))
                family.add(fingerprint)
                for member in family:
                    if newest > horizons.get(member, ""):
                        horizons[member] = newest
                    counts[member] = counts.get(member, 0) + int(n)
            _remember_horizons(conn, horizons, counts)
            deleted = conn.execute(
                "DELETE FROM audit_event WHERE " + where).rowcount  # nosec B608
            conn.execute("DELETE FROM _rl_purge_keys")
        remaining = total - deleted
        flog(f"retention: purged {deleted} excess events in {len(chosen)} "
             f"whole commit(s) (total was {total}, max={max_events}, "
             f"now {remaining})")
        if remaining > max_events:
            flog(f"retention: excess purge left {remaining} events for a cap "
                 f"of {max_events}: the newest commit alone is larger than "
                 f"the cap and is never deleted", "WARNING")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: excess purge error (nothing deleted, the "
             f"transaction was rolled back): {e}", "ERROR")
        return 0


def purge_by_session(conn: sqlite3.Connection, session_id: str) -> int:
    """Delete all events for a specific session. Returns deleted count."""
    try:
        with conn:
            cursor = conn.execute(
                "DELETE FROM audit_event WHERE session_id = ?", (session_id,)
            )
            return cursor.rowcount
    except sqlite3.Error as e:
        flog(f"retention: session purge error: {e}", "ERROR")
        return 0


def get_journal_stats(conn: sqlite3.Connection) -> dict:
    """Return basic journal statistics for display.

    Uses idx_event_active_created (partial) for active events
    and idx_event_restored for trace count.
    """
    try:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM audit_event "
            "INDEXED BY idx_event_active_created "
            "WHERE restored_from_event_id IS NULL"
        ).fetchone()
        lo = conn.execute(
            "SELECT MIN(created_at) FROM audit_event "
            "INDEXED BY idx_event_active_created "
            "WHERE restored_from_event_id IS NULL"
        ).fetchone()
        hi = conn.execute(
            "SELECT MAX(created_at) FROM audit_event "
            "INDEXED BY idx_event_active_created "
            "WHERE restored_from_event_id IS NULL"
        ).fetchone()
        active = (cnt[0] if cnt else 0, lo[0] if lo else None, hi[0] if hi else None)
    except sqlite3.OperationalError:
        active = conn.execute(
            "SELECT COUNT(*), MIN(created_at), MAX(created_at)"
            " FROM audit_event WHERE restored_from_event_id IS NULL"
        ).fetchone() or (0, None, None)
    trace = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE restored_from_event_id IS NOT NULL"
    ).fetchone()
    return {
        "total_events": int(active[0] or 0),
        "oldest_event": active[1],
        "newest_event": active[2],
        "trace_events": int(trace[0] or 0) if trace else 0,
    }


def check_journal_coverage(conn: sqlite3.Connection,
                           cutoff,
                           datasource_fp: Optional[str] = None) -> Optional[str]:
    """Return a blocking reason if the journal no longer covers *cutoff*.

    Retention is what removes the oldest events, so retention is what can
    answer whether a rewind date is still backed by the journal. A date
    older than the oldest surviving event is not reachable any more: the
    rewind would replay what is left and report success on a state that
    never existed.

    *cutoff* is a restore_contracts.RestoreCutoff. Returns None when the
    date is covered (or when the cutoff is not date-based). Callers of the
    rewind pipeline pass the result to restore_planner.preflight_check via
    its `oldest_event_date` argument, or surface it directly.
    """
    from .event_stream_repository import get_oldest_event_date
    from .restore_planner import check_retention_coverage

    try:
        oldest = get_oldest_event_date(conn, datasource_fp)
    except sqlite3.Error as e:
        flog(f"retention: journal coverage lookup failed: {e}", "WARNING")
        return None
    reason = check_retention_coverage(cutoff, oldest)
    if reason:
        flog(f"retention: journal coverage refused: {reason}", "WARNING")
    return reason


def retention_cutoff(policy: RetentionPolicy) -> str:
    """The date before which the retention purge of *policy* deletes."""
    return _compute_cutoff(policy.retention_days)


def _compute_cutoff(retention_days: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    return cutoff.isoformat()


def vacuum_async(db_path: str,
                 callback: Optional[Callable[[bool], None]] = None) -> None:
    """Run VACUUM in a dedicated thread to avoid blocking the UI.

    Uses a lock to prevent concurrent VACUUM operations.
    The callback (if provided) is called with True on success, False on error.
    """
    def _run():
        if not _vacuum_lock.acquire(blocking=False):
            flog("retention: VACUUM skipped, another VACUUM is already running", "WARNING")
            if callback is not None:
                callback(False)
            return
        success = False
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                flog("retention: WAL checkpoint (TRUNCATE) completed before VACUUM")
            except sqlite3.Error as ce:
                flog(f"retention: WAL checkpoint failed: {ce}", "WARNING")
            conn.execute("VACUUM")
            # In WAL mode the rebuilt pages sit in the -wal file until a
            # checkpoint: without it the journal file keeps its size while
            # other connections are open and the dialog reports 0 bytes
            # reclaimed after a successful compaction.
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except sqlite3.Error as ce:
                flog(f"retention: WAL checkpoint after VACUUM failed: {ce}", "WARNING")
            flog("retention: VACUUM completed (async)")
            success = True
        except sqlite3.Error as e:
            flog(f"retention: VACUUM failed (async): {e}", "WARNING")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            _vacuum_lock.release()
        if callback is not None:
            try:
                callback(success)
            except Exception as cb_err:  # noqa: BLE001 - a notification
                flog(f"retention: VACUUM completion callback failed: {cb_err}", "ERROR")

    t = threading.Thread(target=_run, name="RecoverLand-Vacuum", daemon=True)
    t.start()


def _purge_orphan_traces(conn: sqlite3.Connection) -> int:
    """Delete trace events whose referenced row no longer exists (RW-18).

    Such a trace is still the only record that a compensation happened:
    a rewind to a date before it needs the purged event back and refuses
    by name. Deleting it would turn that refusal into a silent wrong
    state, so the purge records the horizon it creates first (the trace's
    newest instant, per datasource).

    Returns number of orphan traces deleted.
    """
    try:
        with conn:
            rows = conn.execute(
                "SELECT event_id, datasource_fingerprint,"  # nosec B608
                " MAX(created_at, COALESCE(invalidated_at, ''))"
                " FROM audit_event WHERE " + _ORPHAN_TRACE_WHERE
            ).fetchall()
            _remember_deleted_history(conn, [(r[1], r[2]) for r in rows])
            conn.executemany("DELETE FROM audit_event WHERE event_id = ?",
                             [(r[0],) for r in rows])
            deleted = len(rows)
        if deleted:
            flog(f"retention: purged {deleted} orphan trace event(s), "
                 f"rewinds before them are now refused")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: orphan trace purge error: {e}", "WARNING")
        return 0


def _purge_invalidated_events(conn: sqlite3.Connection) -> int:
    """Delete events marked as invalidated (invalidated_at IS NOT NULL).

    An invalidated trace is a compensation that was in effect from its
    created_at until its invalidated_at (an Undo): the state at any date
    in between contains it. The interval recorded first makes a rewind to
    those dates a refusal instead of a state without it; a date before
    the compensation or after its Undo never needed the row and stays
    reachable.
    """
    try:
        with conn:
            rows = conn.execute(
                "SELECT datasource_fingerprint, created_at,"
                " MAX(created_at, COALESCE(invalidated_at, ''))"
                " FROM audit_event WHERE invalidated_at IS NOT NULL"
            ).fetchall()
            _remember_gaps(conn, rows, "invalidated")
            cursor = conn.execute(
                "DELETE FROM audit_event WHERE invalidated_at IS NOT NULL"
            )
            deleted = cursor.rowcount
        if deleted:
            flog(f"retention: purged {deleted} invalidated event(s)")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: invalidated purge error: {e}", "WARNING")
        return 0


def _purge_insert_delete_pairs(conn: sqlite3.Connection) -> int:
    """Delete consecutive INSERT/DELETE pairs of one entity, one session.

    A pair is annulled only when no UPDATE exists between the INSERT and the
    DELETE, preserving the rewind chain for real edits; see `_PAIRS_CTE`
    for what else it must hold (user events of ONE layer, none referenced
    by a trace). The dates between the INSERT and the DELETE lose the
    feature: that interval is recorded before the rows go. Before the
    INSERT and after the DELETE the pair cancels out and nothing is lost.
    """
    deleted = 0
    try:
        while True:
            with conn:
                rows = conn.execute(
                    _PAIRS_CTE + " SELECT event_id, next_id,"  # nosec B608
                    " datasource_fingerprint, created_at, next_at FROM pairs"
                    " LIMIT ?",
                    (_PURGE_BATCH_SIZE,),
                ).fetchall()
                if rows:
                    _remember_gaps(conn, [(r[2], r[3], r[4]) for r in rows],
                                   "insert_delete_pair")
                    conn.executemany(
                        "DELETE FROM audit_event WHERE event_id = ?",
                        [(eid,) for r in rows for eid in (r[0], r[1])])
            deleted += 2 * len(rows)
            if len(rows) < _PURGE_BATCH_SIZE:
                break
        if deleted:
            flog(f"retention: purged {deleted} event(s) from INSERT/DELETE pairs")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: insert/delete pair purge error: {e}", "WARNING")
        return deleted


def _post_purge_maintenance(conn: sqlite3.Connection) -> None:
    """Refresh query planner stats and checkpoint WAL after a purge.

    Orphan traces are NOT collected here. A restore trace is the journal's
    memory that an entity has already been compensated; deleting it because
    retention removed the event it points to makes that compensation
    invisible and the next rewind replays it, duplicating the entity.
    Traces are only removed when the caller asks for it explicitly
    (PurgeOptions.orphan_traces).
    """
    try:
        conn.execute("ANALYZE audit_event")
        flog("retention: post-purge ANALYZE completed")
    except sqlite3.Error as e:
        flog(f"retention: post-purge ANALYZE failed: {e}", "WARNING")
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.Error as e:
        flog(f"retention: post-purge WAL checkpoint failed: {e}", "WARNING")
