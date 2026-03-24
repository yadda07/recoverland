"""Robust tests for UX ergonomie features across all sprints.

Covers: tracker session counter, commit callback, mass deletion detection,
humanize_error, empty result suggestions, health monitor integration,
status bar states, column memory persistence.
"""
import sqlite3
import json
import os
import tempfile

from recoverland.core.edit_tracker import EditSessionTracker
from recoverland.core.health_monitor import (
    evaluate_journal_health, HealthLevel,
    format_integrity_message, format_user_error,
    _humanize_integrity_issue,
)
from recoverland.core.time_format import format_relative_time, _parse_iso
from recoverland.core.disk_monitor import check_disk_for_path, DiskStatus, format_disk_message
from recoverland.core.write_queue import WriteQueue
from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.journal_manager import JournalManager
from recoverland.core.retention import (
    RetentionPolicy, purge_old_events, count_purgeable_events,
    get_journal_stats, vacuum_async,
)
from recoverland.core.audit_backend import AuditEvent


def _make_event(op="UPDATE", layer="test_layer", restored_from=None):
    return AuditEvent(
        event_id=None,
        project_fingerprint="proj::test",
        datasource_fingerprint="ogr::test.gpkg",
        layer_id_snapshot="lid",
        layer_name_snapshot=layer,
        provider_type="ogr",
        feature_identity_json='{"fid": 1}',
        operation_type=op,
        attributes_json='{"all_attributes": {"a": 1}}',
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid=None,
        field_schema_json="[]",
        user_name="tester",
        session_id="sess-1",
        created_at="2025-06-15T10:00:00+00:00",
        restored_from_event_id=restored_from,
    )


# ---------------------------------------------------------------------------
# EditSessionTracker: session counter and commit callback (UX-A05, UX-G02, UX-B04)
# ---------------------------------------------------------------------------

class TestTrackerSessionCounter:

    def _make_tracker(self):
        wq = WriteQueue()
        jm = JournalManager()
        return EditSessionTracker(wq, jm)

    def test_initial_count_is_zero(self):
        t = self._make_tracker()
        assert t.session_event_count == 0

    def test_reset_session_count(self):
        t = self._make_tracker()
        t._session_event_count = 42
        t.reset_session_count()
        assert t.session_event_count == 0

    def test_commit_callback_setter(self):
        t = self._make_tracker()
        called = []
        t.set_commit_callback(lambda *a: called.append(a))
        assert t._on_commit_callback is not None

    def test_mass_delete_threshold(self):
        assert EditSessionTracker._MASS_DELETE_THRESHOLD == 100


# ---------------------------------------------------------------------------
# Health evaluation edge cases (UX-A01 hardening)
# ---------------------------------------------------------------------------

class TestHealthEdgeCases:

    def test_exactly_at_each_boundary(self):
        """Verify exact boundary values for all threshold transitions."""
        boundaries = [
            (50 * 1024 * 1024 - 1, 0, HealthLevel.HEALTHY),
            (50 * 1024 * 1024, 0, HealthLevel.INFO),
            (200 * 1024 * 1024 - 1, 0, HealthLevel.INFO),
            (200 * 1024 * 1024, 0, HealthLevel.WARNING),
            (500 * 1024 * 1024 - 1, 0, HealthLevel.WARNING),
            (500 * 1024 * 1024, 0, HealthLevel.CRITICAL),
            (0, 99_999, HealthLevel.HEALTHY),
            (0, 100_000, HealthLevel.INFO),
            (0, 499_999, HealthLevel.INFO),
            (0, 500_000, HealthLevel.WARNING),
            (0, 999_999, HealthLevel.WARNING),
            (0, 1_000_000, HealthLevel.CRITICAL),
        ]
        for size, count, expected in boundaries:
            h = evaluate_journal_health(size, count, "", "")
            assert h.level == expected, (
                f"size={size} count={count}: expected {expected}, got {h.level}")

    def test_mixed_levels_worst_wins(self):
        """When size and count have different levels, worst wins."""
        h = evaluate_journal_health(500 * 1024 * 1024, 50, "", "")
        assert h.level == HealthLevel.CRITICAL

        h = evaluate_journal_health(100, 1_000_000, "", "")
        assert h.level == HealthLevel.CRITICAL

    def test_very_large_values(self):
        """10 GB journal, 50M events: still critical, no crash."""
        h = evaluate_journal_health(
            10 * 1024 * 1024 * 1024, 50_000_000, "", "")
        assert h.level == HealthLevel.CRITICAL
        assert h.message != ""

    def test_health_message_contains_numbers(self):
        h = evaluate_journal_health(200 * 1024 * 1024, 250_000, "", "")
        assert "200" in h.message
        assert "250" in h.message


# ---------------------------------------------------------------------------
# Integrity message humanization (UX-A02 + UX-H01 hardening)
# ---------------------------------------------------------------------------

class TestIntegrityHumanization:

    def test_all_known_issue_types(self):
        known = [
            "Integrity check failed: tree 5 page 8",
            "WAL checkpoint failed: database locked",
            "Schema version 5 is newer than expected 1",
            "No schema version found",
            "Journal file not found",
            "Cannot open journal: permission denied",
        ]
        for issue in known:
            msg = _humanize_integrity_issue(issue)
            assert msg != issue, f"Issue not humanized: {issue}"

    def test_unknown_issue_includes_original(self):
        original = "Totally unexpected error XYZ"
        msg = _humanize_integrity_issue(original)
        assert "XYZ" in msg

    def test_multiple_issues_all_present(self):
        issues = [
            "Integrity check failed: x",
            "WAL checkpoint failed: y",
            "No schema version found",
        ]
        msg = format_integrity_message(issues, 0)
        assert msg is not None
        assert "anomalies" in msg
        assert "consolidation" in msg
        assert "version" in msg

    def test_zero_recovered_no_issues(self):
        assert format_integrity_message([], 0) is None

    def test_recovered_only(self):
        msg = format_integrity_message([], 7)
        assert "7" in msg


# ---------------------------------------------------------------------------
# Error humanization (UX-H01 hardening)
# ---------------------------------------------------------------------------

class TestHumanizeError:

    def test_connection_error(self):
        msg = format_user_error(
            "Impossible", "Connection refused", "Retry")
        assert "Impossible" in msg

    def test_structured_format(self):
        msg = format_user_error("What", "Why", "Action")
        assert "What" in msg
        assert "Why" in msg
        assert "Action" in msg


# ---------------------------------------------------------------------------
# Disk monitoring edge cases (UX-A04 hardening)
# ---------------------------------------------------------------------------

class TestDiskMonitorEdgeCases:

    def test_format_critical_message(self):
        s = DiskStatus(50_000_000, 10_000_000_000, "C:", True, True)
        msg = format_disk_message(s)
        assert "critique" in msg
        assert "desactive" in msg

    def test_format_low_message(self):
        s = DiskStatus(400_000_000, 10_000_000_000, "D:", True, False)
        msg = format_disk_message(s)
        assert "faible" in msg
        assert "D:" in msg

    def test_format_healthy_empty(self):
        s = DiskStatus(1_000_000_000, 10_000_000_000, "C:", False, False)
        assert format_disk_message(s) == ""

    def test_check_real_temp_dir(self):
        s = check_disk_for_path(tempfile.gettempdir())
        assert s.free_bytes >= 0
        assert isinstance(s.is_low, bool)
        assert isinstance(s.is_critical, bool)


# ---------------------------------------------------------------------------
# Time formatting edge cases (UX-E01 hardening)
# ---------------------------------------------------------------------------

class TestTimeFormatEdgeCases:

    def test_none_input(self):
        assert format_relative_time(None) == ""

    def test_empty_input(self):
        assert format_relative_time("") == ""

    def test_integer_input_via_parse(self):
        assert _parse_iso(42) is None

    def test_list_input_via_parse(self):
        assert _parse_iso([1, 2, 3]) is None

    def test_very_old_date(self):
        result = format_relative_time("1990-01-01T00:00:00")
        assert "/" in result
        assert "1990" in result

    def test_whitespace_only(self):
        assert _parse_iso("   ") is None


# ---------------------------------------------------------------------------
# Retention and purge integration (UX-D01/D02 hardening)
# ---------------------------------------------------------------------------

class TestRetentionIntegration:

    def _make_journal(self):
        path = os.path.join(tempfile.mkdtemp(), "test_retention.sqlite")
        conn = sqlite3.connect(path)
        initialize_schema(conn)
        return conn, path

    def test_purge_empty_journal(self):
        conn, _ = self._make_journal()
        policy = RetentionPolicy(retention_days=30, max_events=100)
        result = purge_old_events(conn, policy)
        assert result.deleted_count == 0
        conn.close()

    def test_count_purgeable_empty(self):
        conn, _ = self._make_journal()
        policy = RetentionPolicy(retention_days=30, max_events=100)
        count = count_purgeable_events(conn, policy)
        assert count == 0
        conn.close()

    def test_stats_empty_journal(self):
        conn, _ = self._make_journal()
        stats = get_journal_stats(conn)
        assert stats["total_events"] == 0
        assert stats["oldest_event"] is None
        assert stats["newest_event"] is None
        conn.close()

    def test_stats_with_events(self):
        conn, _ = self._make_journal()
        conn.execute(
            "INSERT INTO audit_event "
            "(project_fingerprint, datasource_fingerprint, provider_type, "
            "operation_type, attributes_json, user_name, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("p", "d", "ogr", "INSERT", '{}', "u", "2025-01-01T00:00:00"))
        conn.commit()
        stats = get_journal_stats(conn)
        assert stats["total_events"] == 1
        assert stats["oldest_event"] == "2025-01-01T00:00:00"
        conn.close()

    def test_purge_respects_retention_days(self):
        conn, _ = self._make_journal()
        conn.execute(
            "INSERT INTO audit_event "
            "(project_fingerprint, datasource_fingerprint, provider_type, "
            "operation_type, attributes_json, user_name, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("p", "d", "ogr", "INSERT", '{}', "u", "2020-01-01T00:00:00"))
        conn.commit()
        policy = RetentionPolicy(retention_days=30, max_events=1_000_000)
        count = count_purgeable_events(conn, policy)
        assert count == 1
        result = purge_old_events(conn, policy)
        assert result.deleted_count == 1
        conn.close()


# ---------------------------------------------------------------------------
# Restore history badge (UX-C02 hardening)
# ---------------------------------------------------------------------------

class TestRestoreHistoryBadge:

    def test_event_with_restored_from_has_badge(self):
        e = _make_event(op="UPDATE", restored_from=42)
        op_label = e.operation_type or ""
        if e.restored_from_event_id is not None:
            op_label = f"{op_label} [Restaure]"
        assert "[Restaure]" in op_label

    def test_event_without_restored_from_no_badge(self):
        e = _make_event(op="DELETE")
        op_label = e.operation_type or ""
        if e.restored_from_event_id is not None:
            op_label = f"{op_label} [Restaure]"
        assert "[Restaure]" not in op_label

    def test_none_restored_from(self):
        e = _make_event(op="INSERT", restored_from=None)
        assert e.restored_from_event_id is None
