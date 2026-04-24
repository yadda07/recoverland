"""Tests for FEAT-07 journal audit service."""
import sqlite3
import unittest

from recoverland.core.journal_audit import (
    build_journal_audit_report, UserActivity, LayerActivity,
)
from recoverland.core.sqlite_schema import initialize_schema


def _db():
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _insert(conn, eid, fp, op, user, date, layer="lname", trace_from=None):
    conn.execute(
        "INSERT INTO audit_event (event_id, project_fingerprint, "
        "datasource_fingerprint, layer_id_snapshot, layer_name_snapshot, "
        "provider_type, feature_identity_json, operation_type, "
        "attributes_json, geometry_wkb, geometry_type, crs_authid, "
        "field_schema_json, user_name, session_id, created_at, "
        "restored_from_event_id, entity_fingerprint, event_schema_version) "
        "VALUES (?, 'p', ?, 'l', ?, 'ogr', '{}', ?, '{}', NULL, "
        "'Point', 'EPSG:4326', '[]', ?, 's', ?, ?, 'e', 2)",
        (eid, fp, layer, op, user, date, trace_from),
    )


class TestEmptyJournal(unittest.TestCase):
    def test_empty_report_has_zero_counts(self):
        conn = _db()
        report = build_journal_audit_report(conn)
        self.assertEqual(report.total_events, 0)
        self.assertEqual(report.active_events, 0)
        self.assertEqual(report.trace_events, 0)
        self.assertEqual(report.insert_count, 0)
        self.assertEqual(report.update_count, 0)
        self.assertEqual(report.delete_count, 0)
        self.assertEqual(report.distinct_users, 0)
        self.assertEqual(report.distinct_layers, 0)
        self.assertIsNone(report.oldest_event)
        self.assertIsNone(report.newest_event)
        self.assertEqual(report.top_users, ())
        self.assertEqual(report.top_layers, ())


class TestOperationCounts(unittest.TestCase):
    def test_counts_split_by_operation(self):
        conn = _db()
        _insert(conn, 1, "fp1", "INSERT", "alice", "2025-01-01T10:00:00Z")
        _insert(conn, 2, "fp1", "UPDATE", "alice", "2025-01-02T10:00:00Z")
        _insert(conn, 3, "fp1", "UPDATE", "bob", "2025-01-03T10:00:00Z")
        _insert(conn, 4, "fp1", "DELETE", "bob", "2025-01-04T10:00:00Z")
        conn.commit()
        report = build_journal_audit_report(conn)
        self.assertEqual(report.total_events, 4)
        self.assertEqual(report.active_events, 4)
        self.assertEqual(report.trace_events, 0)
        self.assertEqual(report.insert_count, 1)
        self.assertEqual(report.update_count, 2)
        self.assertEqual(report.delete_count, 1)


class TestTraceEventsSeparated(unittest.TestCase):
    def test_trace_events_not_counted_in_operations(self):
        conn = _db()
        _insert(conn, 1, "fp1", "UPDATE", "alice", "2025-01-01T10:00:00Z")
        # Trace event (restore audit): restored_from_event_id = 1
        _insert(conn, 2, "fp1", "UPDATE", "alice", "2025-01-02T10:00:00Z",
                trace_from=1)
        conn.commit()
        report = build_journal_audit_report(conn)
        self.assertEqual(report.total_events, 2)
        self.assertEqual(report.active_events, 1)
        self.assertEqual(report.trace_events, 1)
        self.assertEqual(report.update_count, 1,
                         "trace events must not inflate operation counts")


class TestTopUsers(unittest.TestCase):
    def test_top_users_ordered_by_count(self):
        conn = _db()
        for i in range(5):
            _insert(conn, 100 + i, "fp1", "UPDATE", "alice",
                    f"2025-01-0{i+1}T10:00:00Z")
        for i in range(2):
            _insert(conn, 200 + i, "fp1", "UPDATE", "bob",
                    f"2025-02-0{i+1}T10:00:00Z")
        conn.commit()
        report = build_journal_audit_report(conn)
        self.assertEqual(len(report.top_users), 2)
        self.assertEqual(report.top_users[0].user_name, "alice")
        self.assertEqual(report.top_users[0].event_count, 5)
        self.assertEqual(report.top_users[1].user_name, "bob")
        self.assertEqual(report.top_users[1].event_count, 2)

    def test_top_n_clamps_to_max(self):
        conn = _db()
        # top_n=1000 must clamp to 100; ensure no crash on the limit.
        report = build_journal_audit_report(conn, top_n=1000)
        self.assertIsNotNone(report)

    def test_top_n_clamps_to_min(self):
        conn = _db()
        _insert(conn, 1, "fp1", "UPDATE", "alice", "2025-01-01T10:00:00Z")
        conn.commit()
        report = build_journal_audit_report(conn, top_n=0)
        self.assertEqual(len(report.top_users), 1,
                         "top_n=0 must clamp to 1, not return empty")


class TestTopLayers(unittest.TestCase):
    def test_top_layers_ordered_by_count(self):
        conn = _db()
        for i in range(3):
            _insert(conn, 100 + i, "fp_busy", "UPDATE", "alice",
                    f"2025-01-0{i+1}T10:00:00Z", layer="busy_layer")
        _insert(conn, 200, "fp_small", "UPDATE", "alice",
                "2025-02-01T10:00:00Z", layer="small_layer")
        conn.commit()
        report = build_journal_audit_report(conn)
        self.assertEqual(len(report.top_layers), 2)
        self.assertEqual(report.top_layers[0].datasource_fingerprint, "fp_busy")
        self.assertEqual(report.top_layers[0].event_count, 3)
        self.assertEqual(report.top_layers[0].layer_name, "busy_layer")


class TestTimeRange(unittest.TestCase):
    def test_oldest_and_newest_reflect_content(self):
        conn = _db()
        _insert(conn, 1, "fp", "UPDATE", "u", "2025-06-05T10:00:00Z")
        _insert(conn, 2, "fp", "UPDATE", "u", "2025-01-01T10:00:00Z")
        _insert(conn, 3, "fp", "UPDATE", "u", "2025-12-25T10:00:00Z")
        conn.commit()
        report = build_journal_audit_report(conn)
        self.assertEqual(report.oldest_event, "2025-01-01T10:00:00Z")
        self.assertEqual(report.newest_event, "2025-12-25T10:00:00Z")


if __name__ == "__main__":
    unittest.main()
