# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""SQLite local audit backend for RecoverLand (RLU-050).

Implements AuditBackend using the local SQLite journal.
Delegates actual queries to search_service and write_queue.
"""
import sqlite3
import threading
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
        self._conn_lock = threading.Lock()

    def write_events(self, events: List[AuditEvent]) -> int:
        accepted = self._write_queue.enqueue(events)
        return len(events) if accepted else 0

    def search(self, criteria: SearchCriteria) -> SearchResult:
        with self._conn_lock:
            return search_events(self._get_read_conn_locked(), criteria)

    def count(self, criteria: SearchCriteria) -> int:
        with self._conn_lock:
            return count_events(self._get_read_conn_locked(), criteria)

    def get_event(self, event_id: int) -> Optional[AuditEvent]:
        with self._conn_lock:
            return get_event_by_id(self._get_read_conn_locked(), event_id)

    def get_distinct_layers(self) -> List[Dict[str, str]]:
        with self._conn_lock:
            return get_distinct_layers(self._get_read_conn_locked())

    def get_distinct_users(self) -> List[str]:
        with self._conn_lock:
            return get_distinct_users(self._get_read_conn_locked())

    def is_available(self) -> bool:
        return self._journal.is_open

    def invalidate_read_cache(self) -> None:
        """Close cached read connection so the next query sees fresh WAL data."""
        with self._conn_lock:
            if self._read_conn is not None:
                try:
                    self._read_conn.close()
                except sqlite3.Error as exc:
                    flog(f"SQLiteAuditBackend.invalidate_read_cache: close failed: {exc}", "DEBUG")
                self._read_conn = None

    def close(self) -> None:
        self.invalidate_read_cache()

    def _get_read_conn_locked(self) -> sqlite3.Connection:
        if self._read_conn is None:
            self._read_conn = self._journal.create_read_connection()
        return self._read_conn

    def _get_read_conn(self) -> sqlite3.Connection:
        with self._conn_lock:
            return self._get_read_conn_locked()
