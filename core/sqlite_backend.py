"""SQLite local audit backend for RecoverLand (RLU-050).

Implements AuditBackend using the local SQLite journal.
Delegates actual queries to search_service and write_queue.
"""
import sqlite3
from typing import List, Dict, Optional

from .audit_backend import AuditBackend, AuditEvent, SearchCriteria, SearchResult
from .search_service import (
    search_events, count_events, get_event_by_id,
    get_distinct_layers, get_distinct_users,
)
from .journal_manager import JournalManager
from .write_queue import WriteQueue
from .logger import flog  # noqa: F401 used in error paths


class SQLiteAuditBackend(AuditBackend):
    """Local SQLite audit backend."""

    def __init__(self, journal: JournalManager, write_queue: WriteQueue):
        self._journal = journal
        self._write_queue = write_queue
        self._read_conn: Optional[sqlite3.Connection] = None

    def write_events(self, events: List[AuditEvent]) -> int:
        accepted = self._write_queue.enqueue(events)
        return len(events) if accepted else 0

    def search(self, criteria: SearchCriteria) -> SearchResult:
        conn = self._get_read_conn()
        return search_events(conn, criteria)

    def count(self, criteria: SearchCriteria) -> int:
        conn = self._get_read_conn()
        return count_events(conn, criteria)

    def get_event(self, event_id: int) -> Optional[AuditEvent]:
        conn = self._get_read_conn()
        return get_event_by_id(conn, event_id)

    def get_distinct_layers(self) -> List[Dict[str, str]]:
        conn = self._get_read_conn()
        return get_distinct_layers(conn)

    def get_distinct_users(self) -> List[str]:
        conn = self._get_read_conn()
        return get_distinct_users(conn)

    def is_available(self) -> bool:
        return self._journal.is_open

    def invalidate_read_cache(self) -> None:
        """Close cached read connection so the next query sees fresh WAL data."""
        if self._read_conn is not None:
            try:
                self._read_conn.close()
            except sqlite3.Error as exc:
                flog(f"SQLiteAuditBackend.invalidate_read_cache: close failed: {exc}", "DEBUG")
            self._read_conn = None

    def close(self) -> None:
        self.invalidate_read_cache()

    def _get_read_conn(self) -> sqlite3.Connection:
        if self._read_conn is None:
            self._read_conn = self._journal.create_read_connection()
        return self._read_conn
