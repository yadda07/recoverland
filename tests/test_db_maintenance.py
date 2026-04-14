"""Tests for core.db_maintenance module."""
import sqlite3
import tempfile
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.db_maintenance import (
    run_analyze, check_integrity_quick, wal_checkpoint,
    run_maintenance, MaintenanceResult,
)


def _insert_events(conn, count):
    for i in range(count):
        conn.execute("""
            INSERT INTO audit_event (
                project_fingerprint, datasource_fingerprint, provider_type,
                operation_type, attributes_json, user_name, created_at
            ) VALUES ('p', 'd', 'ogr', 'INSERT', '{}', 'u', ?)
        """, (f"2025-01-{(i % 28) + 1:02d}T00:00:00",))
    conn.commit()


class TestRunAnalyze(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_analyze_on_empty_table(self):
        self.assertTrue(run_analyze(self.conn))

    def test_analyze_on_populated_table(self):
        _insert_events(self.conn, 100)
        self.assertTrue(run_analyze(self.conn))

    def test_analyze_returns_false_on_closed_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.close()
        self.assertFalse(run_analyze(conn))


class TestCheckIntegrityQuick(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_healthy_database(self):
        _insert_events(self.conn, 50)
        self.assertTrue(check_integrity_quick(self.conn))

    def test_empty_database(self):
        self.assertTrue(check_integrity_quick(self.conn))


class TestWalCheckpoint(unittest.TestCase):

    def test_checkpoint_on_file_db(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            _insert_events(conn, 20)
            pages = wal_checkpoint(conn, "PASSIVE")
            self.assertGreaterEqual(pages, 0)
            conn.close()
        finally:
            for p in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_invalid_mode_returns_minus_one(self):
        conn = sqlite3.connect(":memory:")
        initialize_schema(conn)
        self.assertEqual(wal_checkpoint(conn, "BOGUS"), -1)
        conn.close()


class TestRunMaintenance(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_maintenance_result_shape(self):
        result = run_maintenance(self.conn)
        self.assertIsInstance(result, MaintenanceResult)
        self.assertTrue(result.analyze_ok)
        self.assertTrue(result.integrity_ok)
        self.assertEqual(result.error, "")

    def test_maintenance_on_populated_db(self):
        _insert_events(self.conn, 200)
        result = run_maintenance(self.conn)
        self.assertTrue(result.analyze_ok)
        self.assertTrue(result.integrity_ok)

    def test_maintenance_result_fields(self):
        r = MaintenanceResult(
            analyze_ok=True, integrity_ok=True,
            wal_pages=10, wal_checkpointed=True, error="",
        )
        self.assertEqual(r.wal_pages, 10)
        self.assertTrue(r.wal_checkpointed)


class TestPartialIndexesUsed(unittest.TestCase):
    """Verify that partial indexes exist and the query planner can see them."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)
        _insert_events(self.conn, 50)

    def tearDown(self):
        self.conn.close()

    def test_partial_index_active_exists(self):
        indexes = {
            r[0] for r in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        self.assertIn("idx_event_active", indexes)
        self.assertIn("idx_event_active_created", indexes)

    def test_active_count_query_uses_index(self):
        plan = self.conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT COUNT(*), MIN(created_at), MAX(created_at) "
            "FROM audit_event WHERE restored_from_event_id IS NULL"
        ).fetchall()
        plan_text = " ".join(str(row) for row in plan)
        self.assertIn("USING INDEX", plan_text)


if __name__ == '__main__':
    unittest.main()
