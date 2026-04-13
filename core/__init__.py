"""Core modules for RecoverLand plugin."""
from .constants import (
    DB_CONNECT_TIMEOUT, DB_STATEMENT_TIMEOUT, THREAD_STOP_TIMEOUT,
    PLUGIN_NAME, SCHEMA_AUDIT_MAPPING, AVAILABLE_SCHEMAS, HAS_PSYCOPG2, psycopg2
)
from .logger import flog, qlog, LoggerMixin, generate_trace_id, timed_op
from .database import DatabaseMixin
from .threads import RecoverThread, RestoreThread

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
    is_geometry_only_update,
)
from .schema_drift import (
    compare_schemas, parse_field_schema, extract_current_schema,
    format_drift_message, DriftReport,
)
from .restore_service import (
    pre_check_restore, restore_deleted_feature, restore_inserted_feature,
    restore_updated_feature, restore_batch, PreCheckResult,
)
from .pg_backend import PostgreSQLAuditBackend
from .sqlite_backend import SQLiteAuditBackend
from .backend_router import BackendRouter, BackendMode, format_mode_display
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
    DatasourceInfo,
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
