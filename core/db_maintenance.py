"""Database maintenance for RecoverLand SQLite journals.

Provides ANALYZE, integrity checks, and WAL management.
All operations are bounded, safe for concurrent WAL readers,
and designed to be called after purge or periodically.
"""
import sqlite3
from typing import NamedTuple

from .logger import flog


class MaintenanceResult(NamedTuple):
    analyze_ok: bool
    integrity_ok: bool
    wal_pages: int
    wal_checkpointed: bool
    error: str


def run_analyze(conn: sqlite3.Connection) -> bool:
    """Run ANALYZE on audit_event to refresh query planner statistics."""
    try:
        conn.execute("ANALYZE audit_event")
        flog("maintenance: ANALYZE audit_event completed")
        return True
    except sqlite3.Error as e:
        flog(f"maintenance: ANALYZE failed: {e}", "WARNING")
        return False


def check_integrity_quick(conn: sqlite3.Connection) -> bool:
    """Run quick integrity check. Returns True if database is healthy."""
    try:
        row = conn.execute("PRAGMA quick_check(1)").fetchone()
        ok = row is not None and row[0] == "ok"
        if not ok:
            flog(f"maintenance: integrity check failed: {row}", "ERROR")
        return ok
    except sqlite3.Error as e:
        flog(f"maintenance: integrity check error: {e}", "ERROR")
        return False


def wal_checkpoint(conn: sqlite3.Connection,
                   mode: str = "PASSIVE") -> int:
    """Run WAL checkpoint. Returns pages checkpointed, or -1 on error.

    Modes: PASSIVE (non-blocking), FULL (waits for readers),
    TRUNCATE (resets WAL file to zero).
    """
    if mode not in ("PASSIVE", "FULL", "TRUNCATE", "RESTART"):
        flog(f"maintenance: invalid checkpoint mode {mode!r}", "ERROR")
        return -1
    try:
        row = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
        pages = row[2] if row and len(row) > 2 else 0
        flog(f"maintenance: WAL checkpoint({mode}) checkpointed {pages} pages")
        return pages
    except sqlite3.Error as e:
        flog(f"maintenance: WAL checkpoint failed: {e}", "WARNING")
        return -1


def run_maintenance(conn: sqlite3.Connection) -> MaintenanceResult:
    """Run full maintenance sequence: integrity, ANALYZE, WAL checkpoint."""
    integrity_ok = check_integrity_quick(conn)
    analyze_ok = run_analyze(conn)
    try:
        row = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        wal_pages = row[1] if row and len(row) > 1 else 0
        checkpointed = row[2] if row and len(row) > 2 else 0
        flog(f"maintenance: WAL {wal_pages} total pages, {checkpointed} checkpointed")
    except sqlite3.Error as e:
        flog(f"maintenance: WAL checkpoint failed: {e}", "WARNING")
        return MaintenanceResult(
            analyze_ok=analyze_ok, integrity_ok=integrity_ok,
            wal_pages=0, wal_checkpointed=False, error=str(e),
        )
    return MaintenanceResult(
        analyze_ok=analyze_ok, integrity_ok=integrity_ok,
        wal_pages=wal_pages, wal_checkpointed=checkpointed > 0,
        error="",
    )
