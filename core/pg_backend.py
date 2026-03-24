"""PostgreSQL legacy audit backend adapter for RecoverLand (RLU-050).

Wraps the existing PostgreSQL-based recover/restore logic behind the
common AuditBackend interface. Read-only for write_events (server triggers
handle writes). Search and restore delegate to the existing thread-based flow.
"""
from typing import List, Dict, Optional

from .audit_backend import (
    ReadOnlyBackend, AuditEvent, SearchCriteria, SearchResult,
)
from .constants import HAS_PSYCOPG2, SCHEMA_AUDIT_MAPPING
from .logger import flog


class PostgreSQLAuditBackend(ReadOnlyBackend):
    """Legacy PostgreSQL backend. Reads from server-side audit tables."""

    def __init__(self, db_params: Dict[str, str]):
        self._db_params = db_params
        self._available = HAS_PSYCOPG2 and bool(db_params)

    def search(self, criteria: SearchCriteria) -> SearchResult:
        """Not implemented for local search; legacy uses RecoverThread."""
        return SearchResult(events=[], total_count=0, page=1, page_size=100)

    def count(self, criteria: SearchCriteria) -> int:
        return 0

    def get_event(self, event_id: int) -> Optional[AuditEvent]:
        return None

    def get_distinct_layers(self) -> List[Dict[str, str]]:
        """Return known schema mappings as layer entries."""
        return [
            {"fingerprint": f"postgres::{schema}", "name": schema, "provider": "postgres"}
            for schema in SCHEMA_AUDIT_MAPPING.keys()
        ]

    def get_distinct_users(self) -> List[str]:
        return []

    def is_available(self) -> bool:
        return self._available

    def close(self) -> None:
        pass

    @property
    def db_params(self) -> Dict[str, str]:
        return dict(self._db_params)

    def has_audit_schema(self, schema: str) -> bool:
        """Check if a schema has a known audit table mapping."""
        return schema in SCHEMA_AUDIT_MAPPING
