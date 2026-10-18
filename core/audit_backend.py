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
    invalidated_at: Optional[str] = None
    # Schema v7. event_uid: identity of this event, assigned at capture, so
    # the writer and an outbox replay cannot both insert it. commit_id: the
    # QGIS commit attempt it belongs to (created_at is a clock reading, not
    # a transaction identity).
    event_uid: Optional[str] = None
    commit_id: Optional[str] = None


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
    # BL-RW-P3-18: per-category breakdown for the runner.finished signal.
    # All defaults are empty so legacy call sites stay valid.
    # `succeeded` already includes idempotent skips; the breakdown
    # below decomposes the populations into mutually-exclusive sets:
    #   applied = succeeded \ (skipped_idempotent
    #                          \u222a failed_target_absent
    #                          \u222a failed_geometry_drift)
    #   failed_other = failed \ (failed_target_absent
    #                            \u222a failed_geometry_drift)
    # Two traps for whoever counts from this report:
    #   - the three lists classify by REASON, independently of success:
    #     an id can be in `failed` and in one of them. Derive `applied`
    #     and `failed_other` by subtraction as above, never by summing
    #     the lists, or the same action is counted twice.
    #   - a write that had to abandon captured fields (RLU-053,
    #     reason_code `applied_partial`) lands in `succeeded` with no
    #     list of its own, so this contract cannot distinguish a complete
    #     restore from a mutilated one. Only the buffer-level path
    #     (`_apply_via_buffer` -> StrictRestoreRunner) can.
    skipped_idempotent: List[int] = []
    failed_target_absent: List[int] = []
    failed_geometry_drift: List[int] = []
    # v7: events in `failed` whose write nevertheless reached the file (the
    # read-back contradicts it, a refused write could not be reverted, a
    # commit left part of the batch on disk). The data changed: the restore
    # paths journal a trace for each (the undo journals none, as for its
    # successes). A subset of `failed`, never of `succeeded`; empty for
    # producers that write nothing or do not know.
    landed: List[int] = []


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
