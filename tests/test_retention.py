"""Tests for core.retention module (RLU-013)."""
import sqlite3
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.retention import (
    count_purgeable_events, purge_old_events, purge_by_session,
    get_journal_stats, RetentionPolicy,
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


if __name__ == '__main__':
    unittest.main()
