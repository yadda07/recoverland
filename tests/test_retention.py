"""Tests for core.retention module (RLU-013)."""
import sqlite3
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.retention import (
    count_purgeable_events, purge_old_events, purge_by_session,
    get_journal_stats, RetentionPolicy, PurgeResult,
)


def _insert_event(conn, created_at, session_id="sess1"):
    conn.execute("""
        INSERT INTO audit_event (
            project_fingerprint, datasource_fingerprint, provider_type,
            operation_type, attributes_json, user_name, session_id, created_at
        ) VALUES ('p', 'd', 'ogr', 'DELETE', '{}', 'user', ?, ?)
    """, (session_id, created_at))
    conn.commit()


class TestRetention(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_count_purgeable_none(self):
        _insert_event(self.conn, "2026-03-23T10:00:00")
        policy = RetentionPolicy(retention_days=365, max_events=1_000_000)
        count = count_purgeable_events(self.conn, policy)
        self.assertEqual(count, 0)

    def test_count_purgeable_old_events(self):
        _insert_event(self.conn, "2020-01-01T00:00:00")
        _insert_event(self.conn, "2025-03-15T10:00:00")
        policy = RetentionPolicy(retention_days=30, max_events=1_000_000)
        count = count_purgeable_events(self.conn, policy)
        self.assertGreaterEqual(count, 1)

    def test_purge_old_events(self):
        _insert_event(self.conn, "2020-01-01T00:00:00")
        _insert_event(self.conn, "2020-02-01T00:00:00")
        _insert_event(self.conn, "2025-03-15T10:00:00")
        policy = RetentionPolicy(retention_days=30, max_events=1_000_000)
        result = purge_old_events(self.conn, policy)
        self.assertGreaterEqual(result.deleted_count, 2)
        self.assertEqual(result.error, "")

    def test_purge_by_session(self):
        _insert_event(self.conn, "2025-03-15T10:00:00", session_id="sess_A")
        _insert_event(self.conn, "2025-03-15T11:00:00", session_id="sess_A")
        _insert_event(self.conn, "2025-03-15T12:00:00", session_id="sess_B")
        deleted = purge_by_session(self.conn, "sess_A")
        self.assertEqual(deleted, 2)
        remaining = self.conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        self.assertEqual(remaining, 1)

    def test_journal_stats(self):
        _insert_event(self.conn, "2025-01-01T00:00:00")
        _insert_event(self.conn, "2025-03-15T10:00:00")
        stats = get_journal_stats(self.conn)
        self.assertEqual(stats["total_events"], 2)
        self.assertEqual(stats["oldest_event"], "2025-01-01T00:00:00")
        self.assertEqual(stats["newest_event"], "2025-03-15T10:00:00")

    def test_journal_stats_empty(self):
        stats = get_journal_stats(self.conn)
        self.assertEqual(stats["total_events"], 0)
        self.assertIsNone(stats["oldest_event"])


class TestRetentionMaxEventsCap(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def _insert(self, created_at, session_id="s"):
        self.conn.execute("""
            INSERT INTO audit_event (
                project_fingerprint, datasource_fingerprint, provider_type,
                operation_type, attributes_json, user_name, session_id, created_at
            ) VALUES ('p', 'd', 'ogr', 'DELETE', '{}', 'u', ?, ?)
        """, (session_id, created_at))
        self.conn.commit()

    def test_purge_excess_deletes_oldest(self):
        for i in range(15):
            self._insert(f"2025-01-{i+1:02d}T00:00:00")
        policy = RetentionPolicy(retention_days=36500, max_events=10)
        result = purge_old_events(self.conn, policy)
        self.assertEqual(result.deleted_count, 5)
        remaining = self.conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        self.assertEqual(remaining, 10)

    def test_purge_excess_preserves_newest(self):
        for i in range(20):
            self._insert(f"2025-01-{i+1:02d}T00:00:00")
        policy = RetentionPolicy(retention_days=36500, max_events=5)
        purge_old_events(self.conn, policy)
        oldest = self.conn.execute(
            "SELECT MIN(created_at) FROM audit_event"
        ).fetchone()[0]
        self.assertEqual(oldest, "2025-01-16T00:00:00")

    def test_purge_excess_no_delete_when_under_cap(self):
        for i in range(5):
            self._insert(f"2025-01-{i+1:02d}T00:00:00")
        policy = RetentionPolicy(retention_days=36500, max_events=100)
        result = purge_old_events(self.conn, policy)
        self.assertEqual(result.deleted_count, 0)

    def test_purge_excess_exact_at_cap_no_delete(self):
        for i in range(10):
            self._insert(f"2025-01-{i+1:02d}T00:00:00")
        policy = RetentionPolicy(retention_days=36500, max_events=10)
        result = purge_old_events(self.conn, policy)
        self.assertEqual(result.deleted_count, 0)

    def test_purge_combines_date_and_cap(self):
        self._insert("2020-01-01T00:00:00")
        self._insert("2020-06-01T00:00:00")
        for i in range(8):
            self._insert(f"2026-03-{i+1:02d}T00:00:00")
        policy = RetentionPolicy(retention_days=30, max_events=5)
        result = purge_old_events(self.conn, policy)
        self.assertGreaterEqual(result.deleted_count, 5)
        remaining = self.conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        self.assertLessEqual(remaining, 5)

    def test_purge_zero_max_events_no_delete(self):
        self._insert("2026-01-01T00:00:00")
        policy = RetentionPolicy(retention_days=36500, max_events=0)
        result = purge_old_events(self.conn, policy)
        self.assertEqual(result.deleted_count, 0)


class TestRetentionTraceEventHandling(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def _insert(self, created_at, restored_from=None):
        self.conn.execute("""
            INSERT INTO audit_event (
                project_fingerprint, datasource_fingerprint, provider_type,
                operation_type, attributes_json, user_name, created_at,
                restored_from_event_id
            ) VALUES ('p', 'd', 'ogr', 'DELETE', '{}', 'u', ?, ?)
        """, (created_at, restored_from))
        self.conn.commit()

    def test_journal_stats_separates_traces(self):
        self._insert("2025-01-01T00:00:00")
        self._insert("2025-02-01T00:00:00")
        self._insert("2025-03-01T00:00:00", restored_from=1)
        stats = get_journal_stats(self.conn)
        self.assertEqual(stats["total_events"], 2)
        self.assertEqual(stats["trace_events"], 1)

    def test_journal_stats_only_traces_returns_zero_total(self):
        self._insert("2025-01-01T00:00:00", restored_from=999)
        stats = get_journal_stats(self.conn)
        self.assertEqual(stats["total_events"], 0)
        self.assertEqual(stats["trace_events"], 1)

    def test_purge_also_removes_old_traces(self):
        self._insert("2020-01-01T00:00:00")
        self._insert("2020-02-01T00:00:00", restored_from=1)
        self._insert("2099-01-01T00:00:00")
        policy = RetentionPolicy(retention_days=30, max_events=1_000_000)
        result = purge_old_events(self.conn, policy)
        self.assertGreaterEqual(result.deleted_count, 2)
        remaining = self.conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        self.assertEqual(remaining, 1)


class TestPurgeBySession(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_nonexistent_session_returns_zero(self):
        deleted = purge_by_session(self.conn, "ghost_session")
        self.assertEqual(deleted, 0)

    def test_empty_session_id(self):
        deleted = purge_by_session(self.conn, "")
        self.assertEqual(deleted, 0)

    def test_sql_injection_session_id(self):
        self.conn.execute("""
            INSERT INTO audit_event (
                project_fingerprint, datasource_fingerprint, provider_type,
                operation_type, attributes_json, user_name, session_id, created_at
            ) VALUES ('p', 'd', 'ogr', 'DELETE', '{}', 'u', 'safe', '2025-01-01')
        """)
        self.conn.commit()
        toxic = "'; DROP TABLE audit_event; --"
        deleted = purge_by_session(self.conn, toxic)
        self.assertEqual(deleted, 0)
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_event'"
        ).fetchone()
        self.assertIsNotNone(tables)


class TestVacuumAsync(unittest.TestCase):

    def test_vacuum_callback_called_on_success(self):
        import tempfile
        import threading
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.close()
            result = []
            event = threading.Event()

            def on_done(success):
                result.append(success)
                event.set()

            from recoverland.core.retention import vacuum_async
            vacuum_async(tmp_path, on_done)
            event.wait(timeout=10)
            self.assertEqual(result, [True])
        finally:
            for p in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
                try:
                    os.unlink(p)
                except OSError:
                    pass

    def test_vacuum_callback_false_on_bad_path(self):
        import threading
        result = []
        event = threading.Event()

        def on_done(success):
            result.append(success)
            event.set()

        from recoverland.core.retention import vacuum_async
        vacuum_async("/nonexistent/path/audit.sqlite", on_done)
        event.wait(timeout=10)
        self.assertEqual(result, [False])

    def test_vacuum_no_callback_no_crash(self):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.close()
            from recoverland.core.retention import vacuum_async
            vacuum_async(tmp_path, None)
            import time
            time.sleep(1)
        finally:
            for p in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
                try:
                    os.unlink(p)
                except OSError:
                    pass


class TestPurgeErrorResult(unittest.TestCase):

    def test_purge_returns_error_string_on_failure(self):
        conn = sqlite3.connect(":memory:")
        policy = RetentionPolicy(retention_days=30, max_events=100)
        result = purge_old_events(conn, policy)
        self.assertIn("error", result._fields)
        conn.close()

    def test_purge_result_tuple_shape(self):
        r = PurgeResult(deleted_count=5, vacuum_done=False, error="")
        self.assertEqual(r.deleted_count, 5)
        self.assertFalse(r.vacuum_done)
        self.assertEqual(r.error, "")

    def test_default_policy_values(self):
        from recoverland.core.retention import DEFAULT_POLICY
        self.assertEqual(DEFAULT_POLICY.retention_days, 365)
        self.assertEqual(DEFAULT_POLICY.max_events, 1_000_000)


if __name__ == '__main__':
    unittest.main()
