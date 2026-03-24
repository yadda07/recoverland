"""SQLite schema definition and initialization for RecoverLand (RLU-010, RLU-012).

Provides DDL statements and idempotent schema creation.
All PRAGMAs are applied at every connection open.
"""
import sqlite3
from typing import List, Tuple

CURRENT_SCHEMA_VERSION = 1

_PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA busy_timeout=5000",
    "PRAGMA cache_size=-8000",
    "PRAGMA page_size=4096",
    "PRAGMA foreign_keys=OFF",
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
        restored_from_event_id INTEGER
    )""",
]

_INDEX_DDL = [
    """CREATE INDEX IF NOT EXISTS idx_event_main
       ON audit_event(datasource_fingerprint, layer_name_snapshot, operation_type, created_at)""",
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
    """Return ordered list of (version, description, sql) migrations to apply.

    Currently only version 1 exists. Future migrations append here.
    """
    migrations: List[Tuple[int, str, str]] = []
    if current_version < 1:
        migrations.append((1, "Initial schema", ""))
    return migrations
