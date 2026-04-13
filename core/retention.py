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


class RetentionPolicy(NamedTuple):
    retention_days: int
    max_events: int


DEFAULT_POLICY = RetentionPolicy(retention_days=365, max_events=1_000_000)


def count_purgeable_events(conn: sqlite3.Connection, policy: RetentionPolicy) -> int:
    """Count events older than the retention period."""
    cutoff = _compute_cutoff(policy.retention_days)
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE created_at < ?", (cutoff,)
    ).fetchone()
    return row[0] if row else 0


def purge_old_events(conn: sqlite3.Connection,
                     policy: RetentionPolicy,
                     trace_id: str = "") -> PurgeResult:
    """Delete events older than the retention period and enforce max_events.

    Deletes in batches of _PURGE_BATCH_SIZE to avoid holding the write
    lock for too long. Runs ANALYZE + WAL checkpoint after purge.
    Does NOT run VACUUM inline; use vacuum_async() to reclaim space.
    """
    total_deleted = 0
    try:
        with timed_op("purge_old_events", trace_id):
            cutoff = _compute_cutoff(policy.retention_days)
            while True:
                with conn:
                    cursor = conn.execute(
                        "DELETE FROM audit_event WHERE event_id IN "
                        "(SELECT event_id FROM audit_event WHERE created_at < ? LIMIT ?)",
                        (cutoff, _PURGE_BATCH_SIZE),
                    )
                    batch_deleted = cursor.rowcount
                total_deleted += batch_deleted
                if batch_deleted < _PURGE_BATCH_SIZE:
                    break
            if total_deleted:
                flog(f"retention: purged {total_deleted} events older than {cutoff}")

            excess = _purge_excess(conn, policy.max_events)
            total_deleted += excess

            if total_deleted > 0:
                _post_purge_maintenance(conn)

            return PurgeResult(deleted_count=total_deleted, vacuum_done=False, error="")

    except sqlite3.Error as e:
        flog(f"retention: purge error: {e}", "ERROR")
        return PurgeResult(deleted_count=total_deleted, vacuum_done=False, error=str(e))


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
                    "(SELECT event_id FROM audit_event ORDER BY created_at ASC LIMIT ?)",
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


def _post_purge_maintenance(conn: sqlite3.Connection) -> None:
    """Refresh query planner stats and checkpoint WAL after a purge."""
    try:
        conn.execute("ANALYZE audit_event")
        flog("retention: post-purge ANALYZE completed")
    except sqlite3.Error as e:
        flog(f"retention: post-purge ANALYZE failed: {e}", "WARNING")
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.Error as e:
        flog(f"retention: post-purge WAL checkpoint failed: {e}", "WARNING")
