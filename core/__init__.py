"""Core modules for RecoverLand plugin."""
from .constants import PLUGIN_NAME
from .logger import flog, qlog, LoggerMixin, generate_trace_id, timed_op

from .support_policy import (
    IdentityStrength, SupportLevel, ProviderPolicy,
    evaluate_layer_support, is_capture_supported, is_restore_supported,
    format_support_message,
)
from .audit_backend import (
    AuditEvent, SearchCriteria, SearchResult, RestoreRequest, RestoreReport,
    AuditBackend,
)
from .user_identity import resolve_user_name
from .serialization import (
    serialize_value, deserialize_value, serialize_attributes,
    serialize_field_schema, compute_update_delta, build_full_snapshot,
)
from .geometry_utils import (
    extract_geometry_wkb, extract_geometry_type, extract_crs_authid,
    rebuild_geometry, capture_geometry_info,
)
from .identity import (
    compute_datasource_fingerprint, compute_feature_identity,
    compute_project_fingerprint, extract_layer_name,
    get_identity_strength_for_layer,
)
from .sqlite_schema import (
    initialize_schema, apply_pragmas, get_schema_version,
    CURRENT_SCHEMA_VERSION,
)
from .journal_manager import JournalManager, get_journal_size_bytes, format_journal_size, cleanup_orphan_journals
from .write_queue import WriteQueue
from .edit_buffer import EditSessionBuffer, FeatureSnapshot, create_snapshot_from_feature
from .edit_tracker import EditSessionTracker
from .search_service import (
    search_events, count_events, reconstruct_attributes,
    get_distinct_layers, get_distinct_users,
    summarize_scope, JournalScopeSummary,
    is_geometry_only_update, get_event_by_id,
    _BLOB_MARKER,
)
from .schema_drift import (
    compare_schemas, parse_field_schema, extract_current_schema,
    format_drift_message, DriftReport,
)
from .restore_service import (
    pre_check_restore, restore_deleted_feature, restore_inserted_feature,
    restore_updated_feature, restore_batch, PreCheckResult,
)
from .sqlite_backend import SQLiteAuditBackend
from .retention import (
    purge_old_events, count_purgeable_events, get_journal_stats,
    purge_excess_events, vacuum_async,
    RetentionPolicy, DEFAULT_POLICY,
)
from .integrity import check_journal_integrity, save_pending_events
from .local_settings import LocalSettings
from .audit_field_policy import is_layer_audit_field
from .datasource_registry import (
    register_datasource, lookup_datasource, create_layer_from_registry,
    purge_orphan_datasources, DatasourceInfo,
)
from .datasource_alias import (
    add_alias, remove_alias, list_aliases, resolve_fingerprints,
)
from .journal_audit import (
    build_journal_audit_report, JournalAuditReport,
    UserActivity, LayerActivity,
)
from .restore_service import build_restore_trace_event
from .restore_contracts import (
    RestoreMode, RestoreScope, CutoffType, ConflictPolicy,
    AtomicityPolicy, PreflightVerdict,
    RestoreCutoff, PlannedAction, Conflict, RestorePlan, PreflightReport,
    COMPENSATORY_OPS, MAX_EVENTS_PER_RESTORE, MAX_ENTITIES_PER_RESTORE,
    is_restore_allowed, validate_cutoff, check_volume_limits,
    default_atomicity, scope_requires_confirmation,
    RestoreSession,
)
from .identity import compute_entity_fingerprint
from .event_stream_repository import (
    fetch_entity_stream, fetch_events_after_cutoff,
    count_events_after_cutoff, fetch_events_by_ids,
    get_oldest_event_date,
)
from .restore_planner import (
    plan_event_restore, plan_temporal_restore, preflight_check,
    check_retention_coverage,
)
from .restore_executor import execute_restore_plan, preflight_layer_check, build_restore_session
from .restore_service import undo_restore_batch
from .search_service import reconstruct_new_attributes
from .restore_preview import (
    format_plan_summary, format_preflight_report, format_dry_run_message,
)
from .health_monitor import (
    evaluate_journal_health, check_disk_space, format_integrity_message,
    format_user_error, HealthLevel, JournalHealthStatus, DiskSpaceStatus,
)
from .time_format import (
    format_relative_time, format_short_absolute, format_full_timestamp,
    compute_history_span,
)
from .disk_monitor import check_disk_for_path, format_disk_message, DiskStatus
from .db_maintenance import (
    run_analyze, check_integrity_quick, wal_checkpoint,
    run_maintenance, MaintenanceResult,
)
from .geometry_preview import GeometryPreviewManager
from .layer_stats_cache import LayerStatsCache, LayerStats
from .workflow_service import (
    execute_grouped_restore, execute_grouped_undo,
    find_target_layer, GroupedRestoreResult,
)
from .rewind_dedup import collapse_rewind_events

__all__ = (
    "PLUGIN_NAME",
    "flog", "qlog", "LoggerMixin", "generate_trace_id", "timed_op",
    "IdentityStrength", "SupportLevel", "ProviderPolicy",
    "evaluate_layer_support", "is_capture_supported", "is_restore_supported",
    "format_support_message",
    "AuditEvent", "SearchCriteria", "SearchResult", "RestoreRequest", "RestoreReport",
    "AuditBackend",
    "resolve_user_name",
    "serialize_value", "deserialize_value", "serialize_attributes",
    "serialize_field_schema", "compute_update_delta", "build_full_snapshot",
    "extract_geometry_wkb", "extract_geometry_type", "extract_crs_authid",
    "rebuild_geometry", "capture_geometry_info",
    "compute_datasource_fingerprint", "compute_feature_identity",
    "compute_project_fingerprint", "extract_layer_name",
    "get_identity_strength_for_layer", "compute_entity_fingerprint",
    "initialize_schema", "apply_pragmas", "get_schema_version",
    "CURRENT_SCHEMA_VERSION",
    "JournalManager", "get_journal_size_bytes", "format_journal_size", "cleanup_orphan_journals",
    "WriteQueue",
    "EditSessionBuffer", "FeatureSnapshot", "create_snapshot_from_feature",
    "EditSessionTracker",
    "search_events", "count_events", "reconstruct_attributes",
    "get_distinct_layers", "get_distinct_users",
    "summarize_scope", "JournalScopeSummary",
    "is_geometry_only_update", "get_event_by_id",
    "_BLOB_MARKER", "reconstruct_new_attributes",
    "compare_schemas", "parse_field_schema", "extract_current_schema",
    "format_drift_message", "DriftReport",
    "pre_check_restore", "restore_deleted_feature", "restore_inserted_feature",
    "restore_updated_feature", "restore_batch", "PreCheckResult",
    "build_restore_trace_event", "undo_restore_batch",
    "SQLiteAuditBackend",
    "purge_old_events", "count_purgeable_events", "get_journal_stats",
    "purge_excess_events", "vacuum_async",
    "RetentionPolicy", "DEFAULT_POLICY",
    "check_journal_integrity", "save_pending_events",
    "LocalSettings",
    "is_layer_audit_field",
    "register_datasource", "lookup_datasource", "create_layer_from_registry",
    "purge_orphan_datasources", "DatasourceInfo",
    "add_alias", "remove_alias", "list_aliases", "resolve_fingerprints",
    "build_journal_audit_report", "JournalAuditReport",
    "UserActivity", "LayerActivity",
    "RestoreMode", "RestoreScope", "CutoffType", "ConflictPolicy",
    "AtomicityPolicy", "PreflightVerdict",
    "RestoreCutoff", "PlannedAction", "Conflict", "RestorePlan", "PreflightReport",
    "COMPENSATORY_OPS", "MAX_EVENTS_PER_RESTORE", "MAX_ENTITIES_PER_RESTORE",
    "is_restore_allowed", "validate_cutoff", "check_volume_limits",
    "default_atomicity", "scope_requires_confirmation",
    "RestoreSession",
    "fetch_entity_stream", "fetch_events_after_cutoff",
    "count_events_after_cutoff", "fetch_events_by_ids",
    "get_oldest_event_date",
    "plan_event_restore", "plan_temporal_restore", "preflight_check",
    "check_retention_coverage",
    "execute_restore_plan", "preflight_layer_check", "build_restore_session",
    "format_plan_summary", "format_preflight_report", "format_dry_run_message",
    "evaluate_journal_health", "check_disk_space", "format_integrity_message",
    "format_user_error", "HealthLevel", "JournalHealthStatus", "DiskSpaceStatus",
    "format_relative_time", "format_short_absolute", "format_full_timestamp",
    "compute_history_span",
    "check_disk_for_path", "format_disk_message", "DiskStatus",
    "run_analyze", "check_integrity_quick", "wal_checkpoint",
    "run_maintenance", "MaintenanceResult",
    "GeometryPreviewManager",
    "LayerStatsCache", "LayerStats",
    "execute_grouped_restore", "execute_grouped_undo",
    "find_target_layer", "GroupedRestoreResult",
    "collapse_rewind_events",
)
