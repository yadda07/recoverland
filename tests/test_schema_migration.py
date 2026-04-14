"""Tests for schema migration v1->v2 (BL-02) and entity fingerprint backfill."""
import sqlite3
import json

from recoverland.core.sqlite_schema import (
    initialize_schema, get_schema_version, apply_pragmas,
    CURRENT_SCHEMA_VERSION, _backfill_entity_fingerprint, _extract_entity_fp,
)


def _create_v1_db():
    """Create an in-memory DB with v1 schema (no new columns)."""
    conn = sqlite3.connect(":memory:")
    apply_pragmas(conn)
    conn.execute("""CREATE TABLE schema_version (
        version_number INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL,
        description TEXT
    )""")
    conn.execute(
        "INSERT INTO schema_version VALUES (1, '2025-01-01T00:00:00Z', 'Initial')"
    )
    conn.execute("""CREATE TABLE backend_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT,
        updated_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE audit_session (
        session_id TEXT PRIMARY KEY,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        committed_at TEXT,
        rolled_back_at TEXT,
        qgis_user_context_json TEXT
    )""")
    conn.execute("""CREATE TABLE datasource_registry (
        datasource_fingerprint TEXT PRIMARY KEY,
        provider_type TEXT NOT NULL,
        source_uri TEXT NOT NULL,
        layer_name TEXT,
        authcfg TEXT,
        crs_authid TEXT,
        geometry_type TEXT DEFAULT 'NoGeometry',
        last_seen_at TEXT NOT NULL
    )""")
    conn.execute("""CREATE TABLE audit_event (
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
    )""")
    conn.commit()
    return conn


def _insert_v1_event(conn, event_id_hint, identity_json):
    conn.execute(
        "INSERT INTO audit_event "
        "(project_fingerprint, datasource_fingerprint, provider_type, "
        "feature_identity_json, operation_type, attributes_json, "
        "user_name, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("proj", "ds", "ogr", identity_json, "INSERT", "{}", "user", "2025-01-01"),
    )
    conn.commit()


class TestExtractEntityFp:
    def test_pk_identity(self):
        assert _extract_entity_fp('{"fid": 1, "pk_field": "gid", "pk_value": 42}') == "pk:gid=42"

    def test_fid_only(self):
        assert _extract_entity_fp('{"fid": 7}') == "fid:7"

    def test_empty_json(self):
        assert _extract_entity_fp("{}") is None

    def test_invalid_json(self):
        assert _extract_entity_fp("not json") is None


class TestMigrationV1ToV2:
    def test_fresh_install_is_v2(self):
        conn = sqlite3.connect(":memory:")
        initialize_schema(conn)
        assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION

    def test_fresh_install_has_new_columns(self):
        conn = sqlite3.connect(":memory:")
        initialize_schema(conn)
        row = conn.execute(
            "SELECT entity_fingerprint, event_schema_version, new_geometry_wkb "
            "FROM audit_event LIMIT 1"
        ).fetchone()
        assert row is None

    def test_migrate_v1_adds_columns(self):
        conn = _create_v1_db()
        assert get_schema_version(conn) == 1
        initialize_schema(conn)
        assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION
        cols = [r[1] for r in conn.execute("PRAGMA table_info(audit_event)").fetchall()]
        assert "entity_fingerprint" in cols
        assert "event_schema_version" in cols
        assert "new_geometry_wkb" in cols

    def test_migrate_v1_creates_indexes(self):
        conn = _create_v1_db()
        initialize_schema(conn)
        indexes = [r[1] for r in conn.execute(
            "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='audit_event'"
        ).fetchall()]
        assert "idx_event_entity_stream" in indexes
        assert "idx_event_temporal" in indexes

    def test_migrate_idempotent(self):
        conn = _create_v1_db()
        initialize_schema(conn)
        initialize_schema(conn)
        assert get_schema_version(conn) == CURRENT_SCHEMA_VERSION

    def test_backfill_entity_fingerprint(self):
        conn = _create_v1_db()
        _insert_v1_event(conn, 1, '{"fid": 1, "pk_field": "id", "pk_value": 42}')
        _insert_v1_event(conn, 2, '{"fid": 2}')
        _insert_v1_event(conn, 3, '{}')
        initialize_schema(conn)
        rows = conn.execute(
            "SELECT event_id, entity_fingerprint FROM audit_event ORDER BY event_id"
        ).fetchall()
        assert rows[0][1] == "pk:id=42"
        assert rows[1][1] == "fid:2"
        assert rows[2][1] is None
