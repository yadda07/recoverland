"""Retention, purge and compaction for RecoverLand (RLU-013).

Manages journal size by deleting old events and running VACUUM.
All purge operations are explicit and confirmed; never implicit.
"""
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import NamedTuple, Optional, Callable

from .logger import flog, timed_op

_vacuum_lock = threading.Lock()
_PURGE_BATCH_SIZE = 5000


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


def _count_insert_delete_pairs(conn: sqlite3.Connection) -> int:
    """Count consecutive INSERT/DELETE pairs in the same session (no UPDATE in between)."""
    row = conn.execute(
        "WITH ordered AS ("
        "  SELECT event_id, operation_type,"
        "    LEAD(operation_type) OVER ("
        "      PARTITION BY entity_fingerprint, session_id ORDER BY event_id"
        "    ) AS next_op,"
        "    LEAD(event_id) OVER ("
        "      PARTITION BY entity_fingerprint, session_id ORDER BY event_id"
        "    ) AS next_id"
        "  FROM audit_event"
        "  WHERE invalidated_at IS NULL AND entity_fingerprint IS NOT NULL"
        ")"
        "SELECT COUNT(*) FROM ordered"
        " WHERE operation_type = 'INSERT' AND next_op = 'DELETE'"
    ).fetchone()
    return int(row[0] if row else 0)


def _count_orphan_traces(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event"
        " WHERE restored_from_event_id IS NOT NULL"
        "   AND restored_from_event_id NOT IN"
        "       (SELECT event_id FROM audit_event WHERE restored_from_event_id IS NULL)"
    ).fetchone()
    return int(row[0] if row else 0)


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
                while True:
                    with conn:
                        cursor = conn.execute(
                            "DELETE FROM audit_event WHERE event_id IN "
                            "(SELECT event_id FROM audit_event WHERE created_at < ? LIMIT ?)",
                            (cutoff, _PURGE_BATCH_SIZE),
                        )
                        batch_deleted = cursor.rowcount
                    retention_deleted += batch_deleted
                    if batch_deleted < _PURGE_BATCH_SIZE:
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


def _purge_excess(conn: sqlite3.Connection, max_events: int) -> int:
    """Internal: delete oldest events that exceed max_events (batched)."""
    if max_events <= 0:
        return 0
    total = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
    if total <= max_events:
        return 0
    to_delete = total - max_events
    deleted = 0
    try:
        while deleted < to_delete:
            chunk = min(_PURGE_BATCH_SIZE, to_delete - deleted)
            with conn:
                cursor = conn.execute(
                    "DELETE FROM audit_event WHERE event_id IN "
                    "(SELECT event_id FROM audit_event "
                    "ORDER BY created_at ASC, event_id ASC LIMIT ?)",
                    (chunk,),
                )
                batch = cursor.rowcount
            deleted += batch
            if batch < chunk:
                break
        if deleted:
            flog(f"retention: purged {deleted} excess events (total was {total}, max={max_events})")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: excess purge error: {e}", "ERROR")
        return deleted


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
        try:
            conn = sqlite3.connect(db_path)
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                flog("retention: WAL checkpoint (TRUNCATE) completed before VACUUM")
            except sqlite3.Error as ce:
                flog(f"retention: WAL checkpoint failed: {ce}", "WARNING")
            conn.execute("VACUUM")
            conn.close()
            flog("retention: VACUUM completed (async)")
            success = True
        except sqlite3.Error as e:
            flog(f"retention: VACUUM failed (async): {e}", "WARNING")
        finally:
            _vacuum_lock.release()
        if callback is not None:
            callback(success)

    t = threading.Thread(target=_run, name="RecoverLand-Vacuum", daemon=True)
    t.start()


def _purge_orphan_traces(conn: sqlite3.Connection) -> int:
    """Delete trace events whose referenced user event no longer exists (RW-18).

    Returns number of orphan traces deleted.
    """
    try:
        with conn:
            cursor = conn.execute(
                "DELETE FROM audit_event WHERE restored_from_event_id IS NOT NULL "
                "AND restored_from_event_id NOT IN "
                "(SELECT event_id FROM audit_event WHERE restored_from_event_id IS NULL)"
            )
            deleted = cursor.rowcount
        if deleted:
            flog(f"retention: purged {deleted} orphan trace event(s)")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: orphan trace purge error: {e}", "WARNING")
        return 0


def _purge_invalidated_events(conn: sqlite3.Connection) -> int:
    """Delete events marked as invalidated (invalidated_at IS NOT NULL)."""
    try:
        with conn:
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
    """Delete consecutive INSERT/DELETE pairs in the same session.

    A pair is annulled only when no UPDATE exists between the INSERT and the
    DELETE, preserving the rewind chain for real edits.
    """
    deleted = 0
    try:
        while True:
            with conn:
                conn.execute(
                    "WITH ordered AS ("
                    "  SELECT event_id, operation_type,"
                    "    LEAD(event_id) OVER ("
                    "      PARTITION BY entity_fingerprint, session_id ORDER BY event_id"
                    "    ) AS next_id,"
                    "    LEAD(operation_type) OVER ("
                    "      PARTITION BY entity_fingerprint, session_id ORDER BY event_id"
                    "    ) AS next_op"
                    "  FROM audit_event"
                    "  WHERE invalidated_at IS NULL AND entity_fingerprint IS NOT NULL"
                    "),"
                    "pairs AS ("
                    "  SELECT event_id, next_id FROM ordered"
                    "  WHERE operation_type = 'INSERT' AND next_op = 'DELETE'"
                    "  LIMIT ?"
                    ")"
                    "DELETE FROM audit_event WHERE event_id IN (SELECT event_id FROM pairs)"
                    "   OR event_id IN (SELECT next_id FROM pairs)",
                    (_PURGE_BATCH_SIZE,),
                )
                batch = conn.execute("SELECT changes()").fetchone()[0]
            deleted += batch
            if batch < _PURGE_BATCH_SIZE:
                break
        if deleted:
            flog(f"retention: purged {deleted} event(s) from INSERT/DELETE pairs")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: insert/delete pair purge error: {e}", "WARNING")
        return deleted


def _post_purge_maintenance(conn: sqlite3.Connection) -> None:
    """Refresh query planner stats and checkpoint WAL after a purge."""
    _purge_orphan_traces(conn)
    try:
        conn.execute("ANALYZE audit_event")
        flog("retention: post-purge ANALYZE completed")
    except sqlite3.Error as e:
        flog(f"retention: post-purge ANALYZE failed: {e}", "WARNING")
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.Error as e:
        flog(f"retention: post-purge WAL checkpoint failed: {e}", "WARNING")
