"""Abstract audit backend contract for RecoverLand (RLU-002).

Defines the interface that both PostgreSQL and SQLite backends implement.
The UI and business logic depend only on this contract.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, NamedTuple


class AuditEvent(NamedTuple):
    event_id: Optional[int]
    project_fingerprint: str
    datasource_fingerprint: str
    layer_id_snapshot: str
    layer_name_snapshot: str
    provider_type: str
    feature_identity_json: str
    operation_type: str
    attributes_json: str
    geometry_wkb: Optional[bytes]
    geometry_type: str
    crs_authid: Optional[str]
    field_schema_json: str
    user_name: str
    session_id: Optional[str]
    created_at: str
    restored_from_event_id: Optional[int]
    entity_fingerprint: Optional[str] = None
    event_schema_version: Optional[int] = None
    new_geometry_wkb: Optional[bytes] = None


class SearchCriteria(NamedTuple):
    datasource_fingerprint: Optional[str]
    layer_name: Optional[str]
    operation_type: Optional[str]
    user_name: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    page: int
    page_size: int


class SearchResult(NamedTuple):
    events: List[AuditEvent]
    total_count: int
    page: int
    page_size: int


class RestoreRequest(NamedTuple):
    event_ids: List[int]
    target_layer_id: str


class RestoreReport(NamedTuple):
    succeeded: List[int]
    failed: Dict[int, str]
    total_requested: int
    trace_events: tuple = ()


class AuditBackend(ABC):
    """Contract for audit backends. One backend per responsibility."""

    @abstractmethod
    def write_events(self, events: List[AuditEvent]) -> int:
        """Write audit events. Returns count of events written."""

    @abstractmethod
    def search(self, criteria: SearchCriteria) -> SearchResult:
        """Search audit history with pagination."""

    @abstractmethod
    def count(self, criteria: SearchCriteria) -> int:
        """Count matching events without loading them."""

    @abstractmethod
    def get_event(self, event_id: int) -> Optional[AuditEvent]:
        """Retrieve a single event by ID."""

    @abstractmethod
    def get_distinct_layers(self) -> List[Dict[str, str]]:
        """List distinct audited layers (fingerprint + display name)."""

    @abstractmethod
    def get_distinct_users(self) -> List[str]:
        """List distinct user names in the journal."""

    @abstractmethod
    def is_available(self) -> bool:
        """Check if backend is operational."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""

    @property
    def supports_search(self) -> bool:
        """True if search/count/get_event return real data.

        Backends that delegate search to an external mechanism (e.g.
        legacy PG via RecoverThread) should return False so the UI
        does not call search() expecting results.
        """
        return True


class ReadOnlyBackend(AuditBackend):
    """Backend that only supports reading (e.g. legacy PG via server triggers).

    search/count/get_event are stubs that return empty results.
    The legacy PG flow uses RecoverThread/RestoreThread directly.
    """

    def write_events(self, events: List[AuditEvent]) -> int:
        raise NotImplementedError("This backend does not support local writes")

    @property
    def supports_search(self) -> bool:
        return False
