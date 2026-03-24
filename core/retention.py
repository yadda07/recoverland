"""Retention, purge and compaction for RecoverLand (RLU-013).

Manages journal size by deleting old events and running VACUUM.
All purge operations are explicit and confirmed; never implicit.
"""
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from typing import NamedTuple, Optional, Callable

from .logger import flog


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


def purge_old_events(conn: sqlite3.Connection, policy: RetentionPolicy) -> PurgeResult:
    """Delete events older than the retention period and enforce max_events.

    Returns PurgeResult with count. Does NOT run VACUUM inline;
    use vacuum_async() to reclaim space without blocking the UI.
    """
    total_deleted = 0
    try:
        cutoff = _compute_cutoff(policy.retention_days)
        with conn:
            cursor = conn.execute(
                "DELETE FROM audit_event WHERE created_at < ?", (cutoff,)
            )
            total_deleted += cursor.rowcount
        if total_deleted:
            flog(f"retention: purged {total_deleted} events older than {cutoff}")

        excess = _purge_excess(conn, policy.max_events)
        total_deleted += excess

        return PurgeResult(deleted_count=total_deleted, vacuum_done=False, error="")

    except sqlite3.Error as e:
        flog(f"retention: purge error: {e}", "ERROR")
        return PurgeResult(deleted_count=total_deleted, vacuum_done=False, error=str(e))


def purge_excess_events(conn: sqlite3.Connection, max_events: int) -> int:
    """Delete oldest events to enforce the max_events cap. Returns deleted count."""
    return _purge_excess(conn, max_events)


def _purge_excess(conn: sqlite3.Connection, max_events: int) -> int:
    """Internal: delete oldest events that exceed max_events."""
    if max_events <= 0:
        return 0
    total = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
    if total <= max_events:
        return 0
    to_delete = total - max_events
    try:
        with conn:
            cursor = conn.execute(
                "DELETE FROM audit_event WHERE event_id IN "
                "(SELECT event_id FROM audit_event ORDER BY created_at ASC LIMIT ?)",
                (to_delete,),
            )
            deleted = cursor.rowcount
        flog(f"retention: purged {deleted} excess events (total was {total}, max={max_events})")
        return deleted
    except sqlite3.Error as e:
        flog(f"retention: excess purge error: {e}", "ERROR")
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
    """Return basic journal statistics for display."""
    total = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
    oldest = conn.execute(
        "SELECT MIN(created_at) FROM audit_event"
    ).fetchone()[0]
    newest = conn.execute(
        "SELECT MAX(created_at) FROM audit_event"
    ).fetchone()[0]
    return {
        "total_events": total,
        "oldest_event": oldest,
        "newest_event": newest,
    }


def _compute_cutoff(retention_days: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    return cutoff.isoformat()


def vacuum_async(db_path: str,
                 callback: Optional[Callable[[bool], None]] = None) -> None:
    """Run VACUUM in a dedicated thread to avoid blocking the UI.

    The callback (if provided) is called with True on success, False on error.
    """
    def _run():
        success = False
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("VACUUM")
            conn.close()
            flog("retention: VACUUM completed (async)")
            success = True
        except sqlite3.Error as e:
            flog(f"retention: VACUUM failed (async): {e}", "WARNING")
        if callback is not None:
            callback(success)

    t = threading.Thread(target=_run, name="RecoverLand-Vacuum", daemon=True)
    t.start()


def _try_vacuum(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("VACUUM")
        flog("retention: VACUUM completed")
        return True
    except sqlite3.Error as e:
        flog(f"retention: VACUUM failed: {e}", "WARNING")
        return False
