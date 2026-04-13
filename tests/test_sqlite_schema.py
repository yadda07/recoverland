"""Tests for core.sqlite_schema module (RLU-010, RLU-012)."""
import sqlite3
import tempfile
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import (
    initialize_schema, get_schema_version, apply_pragmas,
    CURRENT_SCHEMA_VERSION, get_all_ddl, get_migration_plan,
)


class TestSQLiteSchema(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")

    def tearDown(self):
        self.conn.close()

    def test_initialize_creates_tables(self):
        initialize_schema(self.conn)
        tables = self._get_tables()
        self.assertIn("audit_event", tables)
        self.assertIn("audit_session", tables)
        self.assertIn("backend_settings", tables)
        self.assertIn("schema_version", tables)

    def test_initialize_creates_indexes(self):
        initialize_schema(self.conn)
        indexes = self._get_indexes()
        self.assertIn("idx_event_main", indexes)
        self.assertIn("idx_event_op_date", indexes)
        self.assertIn("idx_event_user_date", indexes)
        self.assertIn("idx_event_restored", indexes)
        self.assertIn("idx_event_session", indexes)
        self.assertIn("idx_event_active", indexes)
        self.assertIn("idx_event_active_created", indexes)

    def test_initialize_is_idempotent(self):
        initialize_schema(self.conn)
        initialize_schema(self.conn)
        tables = self._get_tables()
        self.assertIn("audit_event", tables)

    def test_schema_version_recorded(self):
        initialize_schema(self.conn)
        version = get_schema_version(self.conn)
        self.assertEqual(version, CURRENT_SCHEMA_VERSION)

    def test_schema_version_zero_before_init(self):
        version = get_schema_version(self.conn)
        self.assertEqual(version, 0)

    def test_pragmas_applied(self):
        # WAL not supported on :memory:, use temp file
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            file_conn = sqlite3.connect(tmp_path)
            apply_pragmas(file_conn)
            journal = file_conn.execute("PRAGMA journal_mode").fetchone()[0]
            self.assertEqual(journal, "wal")
            file_conn.close()
        finally:
            os.unlink(tmp_path)

    def test_audit_event_columns(self):
        initialize_schema(self.conn)
        cols = self._get_columns("audit_event")
        expected = {
            "event_id", "project_fingerprint", "datasource_fingerprint",
            "layer_id_snapshot", "layer_name_snapshot", "provider_type",
            "feature_identity_json", "operation_type", "attributes_json",
            "geometry_wkb", "geometry_type", "crs_authid", "field_schema_json",
            "user_name", "session_id", "created_at", "restored_from_event_id",
            "entity_fingerprint", "event_schema_version", "new_geometry_wkb",
        }
        self.assertEqual(cols, expected)

    def test_operation_type_check_constraint(self):
        initialize_schema(self.conn)
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """INSERT INTO audit_event (
                    project_fingerprint, datasource_fingerprint, provider_type,
                    operation_type, attributes_json, user_name, created_at
                ) VALUES ('p', 'd', 'ogr', 'INVALID', '{}', 'user', '2025-01-01')"""
            )

    def test_valid_operation_types(self):
        initialize_schema(self.conn)
        for op in ("INSERT", "UPDATE", "DELETE"):
            self.conn.execute(
                """INSERT INTO audit_event (
                    project_fingerprint, datasource_fingerprint, provider_type,
                    operation_type, attributes_json, user_name, created_at
                ) VALUES ('p', 'd', 'ogr', ?, '{}', 'user', '2025-01-01')""",
                (op,)
            )
        count = self.conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        self.assertEqual(count, 3)

    def test_get_all_ddl_returns_list(self):
        ddl = get_all_ddl()
        self.assertIsInstance(ddl, list)
        self.assertGreater(len(ddl), 5)

    def test_migration_plan_from_zero(self):
        plan = get_migration_plan(0)
        self.assertGreater(len(plan), 0)
        self.assertEqual(plan[0][0], 1)

    def test_migration_plan_from_current(self):
        plan = get_migration_plan(CURRENT_SCHEMA_VERSION)
        self.assertEqual(len(plan), 0)

    def test_migration_plan_from_future_version(self):
        plan = get_migration_plan(999)
        self.assertEqual(len(plan), 0)

    def test_concurrent_initialize_no_error(self):
        conn1 = sqlite3.connect(":memory:")
        conn2 = sqlite3.connect(":memory:")
        initialize_schema(conn1)
        initialize_schema(conn2)
        v1 = get_schema_version(conn1)
        v2 = get_schema_version(conn2)
        self.assertEqual(v1, CURRENT_SCHEMA_VERSION)
        self.assertEqual(v2, CURRENT_SCHEMA_VERSION)
        conn1.close()
        conn2.close()

    def test_schema_version_is_positive_integer(self):
        self.assertIsInstance(CURRENT_SCHEMA_VERSION, int)
        self.assertGreater(CURRENT_SCHEMA_VERSION, 0)

    def test_insert_all_operation_types_constraint(self):
        initialize_schema(self.conn)
        for invalid_op in ("MERGE", "TRUNCATE", "UPSERT", "", "delete"):
            with self.assertRaises(sqlite3.IntegrityError,
                                   msg=f"Should reject op={invalid_op!r}"):
                self.conn.execute(
                    """INSERT INTO audit_event (
                        project_fingerprint, datasource_fingerprint, provider_type,
                        operation_type, attributes_json, user_name, created_at
                    ) VALUES ('p', 'd', 'ogr', ?, '{}', 'user', '2025-01-01')""",
                    (invalid_op,)
                )
                self.conn.rollback()

    def test_event_id_autoincrement(self):
        initialize_schema(self.conn)
        self.conn.execute(
            """INSERT INTO audit_event (
                project_fingerprint, datasource_fingerprint, provider_type,
                operation_type, attributes_json, user_name, created_at
            ) VALUES ('p', 'd', 'ogr', 'INSERT', '{}', 'u', '2025-01-01')""")
        self.conn.execute(
            """INSERT INTO audit_event (
                project_fingerprint, datasource_fingerprint, provider_type,
                operation_type, attributes_json, user_name, created_at
            ) VALUES ('p', 'd', 'ogr', 'DELETE', '{}', 'u', '2025-01-02')""")
        rows = self.conn.execute(
            "SELECT event_id FROM audit_event ORDER BY event_id").fetchall()
        self.assertEqual(rows[0][0], 1)
        self.assertEqual(rows[1][0], 2)

    def test_null_not_allowed_for_required_fields(self):
        initialize_schema(self.conn)
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """INSERT INTO audit_event (
                    project_fingerprint, datasource_fingerprint, provider_type,
                    operation_type, attributes_json, user_name, created_at
                ) VALUES (NULL, 'd', 'ogr', 'DELETE', '{}', 'u', '2025-01-01')""")

    def _get_tables(self):
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        return {r[0] for r in rows}

    def _get_indexes(self):
        rows = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
        return {r[0] for r in rows}

    def _get_columns(self, table_name):
        rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {r[1] for r in rows}


if __name__ == '__main__':
    unittest.main()
