"""SQLite schema definition and initialization for RecoverLand (RLU-010, RLU-012).

Provides DDL statements and idempotent schema creation.
All PRAGMAs are applied at every connection open.
"""
import sqlite3
from typing import List, Tuple

from .logger import flog

CURRENT_SCHEMA_VERSION = 4

# Authoritative column order for the audit_event table. Every SELECT/INSERT
# site (search_service, event_stream_repository, write_queue, integrity)
# MUST consume these constants instead of redeclaring the column list, so
# that adding/removing a column stays a one-line change.
AUDIT_EVENT_COLUMNS = (
    "event_id", "project_fingerprint", "datasource_fingerprint",
    "layer_id_snapshot", "layer_name_snapshot", "provider_type",
    "feature_identity_json", "operation_type", "attributes_json",
    "geometry_wkb", "geometry_type", "crs_authid", "field_schema_json",
    "user_name", "session_id", "created_at", "restored_from_event_id",
    "entity_fingerprint", "event_schema_version", "new_geometry_wkb",
)
AUDIT_EVENT_INSERT_COLUMNS = AUDIT_EVENT_COLUMNS[1:]  # event_id is autoincrement
AUDIT_EVENT_SELECT_SQL = ", ".join(AUDIT_EVENT_COLUMNS)
AUDIT_EVENT_INSERT_SQL = ", ".join(AUDIT_EVENT_INSERT_COLUMNS)
AUDIT_EVENT_INSERT_PLACEHOLDERS = ",".join(["?"] * len(AUDIT_EVENT_INSERT_COLUMNS))


def build_lightweight_select_sql() -> str:
    """SELECT fragment with geometry BLOBs replaced by NULL/NOT-NULL booleans.

    Used by the search service when callers (search list, paginated UI)
    do not need the raw WKB bytes. The lightweight projection keeps the
    column order identical so _row_to_event can decode either projection
    with the same indices.
    """
    cols = list(AUDIT_EVENT_COLUMNS)
    cols[cols.index("geometry_wkb")] = "(geometry_wkb IS NOT NULL)"
    cols[cols.index("new_geometry_wkb")] = "(new_geometry_wkb IS NOT NULL)"
    return ", ".join(cols)


_PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    # WAL + synchronous=NORMAL: performance/durability trade-off.
    # An OS crash may lose the last committed events.
    # Acceptable for a local audit journal (source data remains intact).
    "PRAGMA synchronous=NORMAL",
    "PRAGMA busy_timeout=5000",
    "PRAGMA cache_size=-8000",
    # page_size only takes effect on a new database; ignored on existing files.
    "PRAGMA page_size=4096",
    "PRAGMA foreign_keys=OFF",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA journal_size_limit=67108864",
    "PRAGMA mmap_size=268435456",
    "PRAGMA analysis_limit=1000",
]

_TABLE_DDL = [
    """CREATE TABLE IF NOT EXISTS schema_version (
        version_number INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL,
        description TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS backend_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS audit_session (
        session_id TEXT PRIMARY KEY,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        committed_at TEXT,
        rolled_back_at TEXT,
        qgis_user_context_json TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS datasource_registry (
        datasource_fingerprint TEXT PRIMARY KEY,
        provider_type TEXT NOT NULL,
        source_uri TEXT NOT NULL,
        layer_name TEXT,
        authcfg TEXT,
        crs_authid TEXT,
        geometry_type TEXT DEFAULT 'NoGeometry',
        last_seen_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS audit_event (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        layer_id_snapshot TEXT,
        layer_name_snapshot TEXT,
        provider_type TEXT NOT NULL,
        feature_identity_json TEXT,
        operation_type TEXT NOT NULL CHECK(operation_type IN ('INSERT','UPDATE','DELETE')),
        attributes_json TEXT NOT NULL,
        geometry_wkb BLOB,
        geometry_type TEXT DEFAULT 'NoGeometry',
        crs_authid TEXT,
        field_schema_json TEXT,
        user_name TEXT NOT NULL,
        session_id TEXT,
        created_at TEXT NOT NULL,
        restored_from_event_id INTEGER,
        entity_fingerprint TEXT,
        event_schema_version INTEGER,
        new_geometry_wkb BLOB
    )""",
    """CREATE TABLE IF NOT EXISTS datasource_alias (
        alias_fingerprint TEXT PRIMARY KEY,
        target_fingerprint TEXT NOT NULL,
        created_at TEXT NOT NULL,
        note TEXT,
        CHECK(alias_fingerprint != target_fingerprint)
    )""",
]

_INDEX_DDL = [
    """CREATE INDEX IF NOT EXISTS idx_event_op_date
       ON audit_event(operation_type, created_at)""",
    """CREATE INDEX IF NOT EXISTS idx_event_user_date
       ON audit_event(user_name, created_at)""",
    """CREATE INDEX IF NOT EXISTS idx_event_restored
       ON audit_event(restored_from_event_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_session
       ON audit_event(session_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_created
       ON audit_event(created_at)""",
    """CREATE INDEX IF NOT EXISTS idx_event_entity_stream
       ON audit_event(datasource_fingerprint, entity_fingerprint, event_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_temporal
       ON audit_event(datasource_fingerprint, created_at, event_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_active
       ON audit_event(datasource_fingerprint, layer_name_snapshot, created_at)
       WHERE restored_from_event_id IS NULL""",
    """CREATE INDEX IF NOT EXISTS idx_event_active_created
       ON audit_event(created_at)
       WHERE restored_from_event_id IS NULL""",
    """CREATE INDEX IF NOT EXISTS idx_datasource_alias_target
       ON datasource_alias(target_fingerprint)""",
]


def apply_pragmas(conn: sqlite3.Connection) -> None:
    """Apply all required PRAGMAs to an open connection."""
    for pragma in _PRAGMAS:
        conn.execute(pragma)


def initialize_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't exist. Idempotent."""
    apply_pragmas(conn)
    with conn:
        for ddl in _TABLE_DDL:
            conn.execute(ddl)
        current = get_schema_version(conn)
        if current < CURRENT_SCHEMA_VERSION:
            _run_migrations(conn, current)
        for ddl in _INDEX_DDL:
            conn.execute(ddl)
        _record_schema_version(conn)


def _record_schema_version(conn: sqlite3.Connection) -> None:
    """Record current schema version if not already present."""
    row = conn.execute(
        "SELECT version_number FROM schema_version WHERE version_number = ?",
        (CURRENT_SCHEMA_VERSION,)
    ).fetchone()
    if row is None:
        from datetime import datetime, timezone
        conn.execute(
            "INSERT INTO schema_version (version_number, applied_at, description) VALUES (?, ?, ?)",
            (CURRENT_SCHEMA_VERSION, datetime.now(timezone.utc).isoformat(), "Initial schema")
        )


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Read current schema version. Returns 0 if no version recorded."""
    try:
        row = conn.execute(
            "SELECT MAX(version_number) FROM schema_version"
        ).fetchone()
        return row[0] if row and row[0] is not None else 0
    except sqlite3.OperationalError:
        return 0


def get_all_ddl() -> List[str]:
    """Return all DDL statements for external inspection."""
    return list(_TABLE_DDL) + list(_INDEX_DDL)


def get_migration_plan(current_version: int) -> List[Tuple[int, str, str]]:
    """Return ordered list of (version, description, sql) migrations to apply."""
    migrations: List[Tuple[int, str, str]] = []
    if current_version < 1:
        migrations.append((1, "Initial schema", ""))
    if current_version < 2:
        migrations.append((2, "Add entity_fingerprint, event_schema_version, new_geometry_wkb",
                           _V2_MIGRATION_SQL))
    if current_version < 3:
        migrations.append((3, "Add partial indexes and performance PRAGMAs", ""))
    if current_version < 4:
        migrations.append((4, "Add datasource_alias table", _V4_MIGRATION_SQL))
    return migrations


_V2_MIGRATION_SQL = ";".join([
    "ALTER TABLE audit_event ADD COLUMN entity_fingerprint TEXT",
    "ALTER TABLE audit_event ADD COLUMN event_schema_version INTEGER",
    "ALTER TABLE audit_event ADD COLUMN new_geometry_wkb BLOB",
])

_V4_MIGRATION_SQL = (
    "CREATE TABLE IF NOT EXISTS datasource_alias ("
    "alias_fingerprint TEXT PRIMARY KEY, "
    "target_fingerprint TEXT NOT NULL, "
    "created_at TEXT NOT NULL, "
    "note TEXT, "
    "CHECK(alias_fingerprint != target_fingerprint))"
)


def _run_migrations(conn: sqlite3.Connection, current_version: int) -> None:
    """Execute pending migrations from current_version to CURRENT_SCHEMA_VERSION."""
    from datetime import datetime, timezone
    plan = get_migration_plan(current_version)
    for version, description, sql in plan:
        if version <= current_version:
            continue
        if sql:
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    try:
                        conn.execute(statement)
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            raise
        for ddl in _INDEX_DDL:
            try:
                conn.execute(ddl)
            except sqlite3.OperationalError as exc:
                # Index may already exist or column may be missing on older
                # schema. Log at DEBUG so upgrade issues stay traceable.
                flog(f"sqlite_schema: index DDL skipped: {exc} ({ddl[:60]}...)", "DEBUG")
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (version, datetime.now(timezone.utc).isoformat(), description)
        )
    if current_version < 2:
        _backfill_entity_fingerprint(conn)


_BACKFILL_BATCH = 50000


def _backfill_entity_fingerprint(conn: sqlite3.Connection) -> int:
    """Best-effort backfill of entity_fingerprint from feature_identity_json.

    Reuses identity.compute_entity_fingerprint as the single source of
    truth for the fingerprint format. Lazy import to avoid any chance
    of import cycle during early schema initialisation.
    """
    from .identity import compute_entity_fingerprint
    total = 0
    while True:
        rows = conn.execute(
            "SELECT event_id, feature_identity_json FROM audit_event "
            "WHERE entity_fingerprint IS NULL "
            "AND feature_identity_json IS NOT NULL "
            "LIMIT ?",
            (_BACKFILL_BATCH,)
        ).fetchall()
        if not rows:
            break
        updates = []
        for event_id, identity_json in rows:
            fp = compute_entity_fingerprint(identity_json)
            if fp is not None:
                updates.append((fp, event_id))
        if updates:
            conn.executemany(
                "UPDATE audit_event SET entity_fingerprint = ? WHERE event_id = ?",
                updates
            )
        total += len(updates)
        if len(rows) < _BACKFILL_BATCH:
            break
    return total
