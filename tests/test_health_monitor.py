"""Robust tests for health_monitor.py (UX-A01, UX-A04, UX-H01).

Covers: all threshold boundaries, edge cases, disk space checks,
integrity message humanization, structured error formatting.
"""
import tempfile

from recoverland.core.health_monitor import (
    evaluate_journal_health,
    check_disk_space,
    format_integrity_message,
    format_user_error,
    HealthLevel,
    JournalHealthStatus,
    DiskSpaceStatus,
    _classify_size,
    _classify_count,
    _worst_level,
    _humanize_integrity_issue,
    _format_size,
)


# ---------------------------------------------------------------------------
# evaluate_journal_health: threshold boundaries
# ---------------------------------------------------------------------------

class TestEvaluateJournalHealth:

    def test_healthy_small_journal(self):
        h = evaluate_journal_health(1024, 10, "2025-01-01", "2025-01-02")
        assert h.level == HealthLevel.HEALTHY
        assert h.message == ""
        assert h.suggestion == ""

    def test_info_threshold_size_exact(self):
        h = evaluate_journal_health(50 * 1024 * 1024, 50, "", "")
        assert h.level == HealthLevel.INFO
        assert "50.0 Mo" in h.message

    def test_info_threshold_size_just_below(self):
        h = evaluate_journal_health(50 * 1024 * 1024 - 1, 50, "", "")
        assert h.level == HealthLevel.HEALTHY

    def test_warning_threshold_size(self):
        h = evaluate_journal_health(200 * 1024 * 1024, 50, "", "")
        assert h.level == HealthLevel.WARNING
        assert "croissance" in h.message
        assert "retention" in h.suggestion

    def test_critical_threshold_size(self):
        h = evaluate_journal_health(500 * 1024 * 1024, 50, "", "")
        assert h.level == HealthLevel.CRITICAL
        assert "volumineux" in h.message.lower() or "Purge" in h.message
        assert "purger" in h.suggestion.lower()

    def test_critical_threshold_size_exact(self):
        h = evaluate_journal_health(500 * 1024 * 1024, 0, "", "")
        assert h.level == HealthLevel.CRITICAL

    def test_info_threshold_count_exact(self):
        h = evaluate_journal_health(0, 100_000, "", "")
        assert h.level == HealthLevel.INFO

    def test_info_threshold_count_just_below(self):
        h = evaluate_journal_health(0, 99_999, "", "")
        assert h.level == HealthLevel.HEALTHY

    def test_warning_threshold_count(self):
        h = evaluate_journal_health(0, 500_000, "", "")
        assert h.level == HealthLevel.WARNING

    def test_critical_threshold_count(self):
        h = evaluate_journal_health(0, 1_000_000, "", "")
        assert h.level == HealthLevel.CRITICAL

    def test_worst_of_size_and_count(self):
        """Size is healthy but count is critical: result is critical."""
        h = evaluate_journal_health(1024, 1_000_000, "", "")
        assert h.level == HealthLevel.CRITICAL

    def test_worst_of_count_and_size(self):
        """Count is healthy but size is critical: result is critical."""
        h = evaluate_journal_health(600 * 1024 * 1024, 10, "", "")
        assert h.level == HealthLevel.CRITICAL

    def test_zero_values(self):
        h = evaluate_journal_health(0, 0, "", "")
        assert h.level == HealthLevel.HEALTHY
        assert h.message == ""

    def test_negative_size_treated_as_healthy(self):
        h = evaluate_journal_health(-1, 0, "", "")
        assert h.level == HealthLevel.HEALTHY

    def test_event_count_formatting_with_spaces(self):
        h = evaluate_journal_health(200 * 1024 * 1024, 123456, "", "")
        assert "123 456" in h.message

    def test_preserves_oldest_newest(self):
        h = evaluate_journal_health(0, 0, "2024-01-01", "2025-06-15")
        assert h.oldest_event == "2024-01-01"
        assert h.newest_event == "2025-06-15"

    def test_returns_named_tuple(self):
        h = evaluate_journal_health(0, 0, "", "")
        assert isinstance(h, JournalHealthStatus)
        assert hasattr(h, 'level')
        assert hasattr(h, 'size_bytes')
        assert hasattr(h, 'event_count')


# ---------------------------------------------------------------------------
# check_disk_space
# ---------------------------------------------------------------------------

class TestCheckDiskSpace:

    def test_empty_path_returns_healthy(self):
        result = check_disk_space("")
        assert result.level == HealthLevel.HEALTHY
        assert not result.should_disable_tracking

    def test_valid_path_returns_result(self):
        result = check_disk_space(tempfile.gettempdir())
        assert isinstance(result, DiskSpaceStatus)
        assert result.free_bytes >= 0

    def test_nonexistent_path_no_crash(self):
        result = check_disk_space("/nonexistent/path/journal.sqlite")
        assert isinstance(result, DiskSpaceStatus)

    def test_none_path_handled(self):
        result = check_disk_space("")
        assert result.level == HealthLevel.HEALTHY


# ---------------------------------------------------------------------------
# format_integrity_message
# ---------------------------------------------------------------------------

class TestFormatIntegrityMessage:

    def test_healthy_no_recovery_returns_none(self):
        assert format_integrity_message([], 0) is None

    def test_recovered_events_only(self):
        msg = format_integrity_message([], 5)
        assert msg is not None
        assert "5" in msg
        assert "recupere" in msg

    def test_single_issue(self):
        msg = format_integrity_message(["Integrity check failed: some error"], 0)
        assert msg is not None
        assert "anomalies" in msg
        assert "incompletes" in msg

    def test_multiple_issues(self):
        msg = format_integrity_message(
            ["WAL checkpoint failed: locked", "No schema version found"], 0)
        assert msg is not None
        assert "consolidation" in msg
        assert "version" in msg

    def test_recovered_plus_issues(self):
        msg = format_integrity_message(["Integrity check failed: x"], 3)
        assert "3" in msg
        assert "anomalies" in msg

    def test_unknown_issue_preserved(self):
        msg = format_integrity_message(["Something completely unexpected"], 0)
        assert "Something completely unexpected" in msg

    def test_schema_newer_issue(self):
        msg = format_integrity_message(
            ["Schema version 5 is newer than expected 1"], 0)
        assert "recente" in msg

    def test_not_found_issue(self):
        msg = format_integrity_message(["Journal file not found"], 0)
        assert "introuvable" in msg

    def test_cannot_open_issue(self):
        msg = format_integrity_message(["Cannot open journal: locked"], 0)
        assert "ouvrir" in msg


# ---------------------------------------------------------------------------
# format_user_error
# ---------------------------------------------------------------------------

class TestFormatUserError:

    def test_basic_format(self):
        msg = format_user_error(
            "Impossible de restaurer",
            "La couche n'est pas ouverte",
            "Ouvrez la couche puis relancez")
        assert "Impossible de restaurer" in msg
        assert "ouverte" in msg
        assert "relancez" in msg

    def test_empty_parts(self):
        msg = format_user_error("", "", "")
        assert " : " in msg


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class TestClassifySize:

    def test_below_all(self):
        assert _classify_size(0) == HealthLevel.HEALTHY

    def test_at_info(self):
        assert _classify_size(50 * 1024 * 1024) == HealthLevel.INFO

    def test_at_warning(self):
        assert _classify_size(200 * 1024 * 1024) == HealthLevel.WARNING

    def test_at_critical(self):
        assert _classify_size(500 * 1024 * 1024) == HealthLevel.CRITICAL

    def test_very_large(self):
        assert _classify_size(10 * 1024 * 1024 * 1024) == HealthLevel.CRITICAL


class TestClassifyCount:

    def test_below_all(self):
        assert _classify_count(0) == HealthLevel.HEALTHY

    def test_at_info(self):
        assert _classify_count(100_000) == HealthLevel.INFO

    def test_at_warning(self):
        assert _classify_count(500_000) == HealthLevel.WARNING

    def test_at_critical(self):
        assert _classify_count(1_000_000) == HealthLevel.CRITICAL


class TestWorstLevel:

    def test_same_levels(self):
        assert _worst_level(HealthLevel.HEALTHY, HealthLevel.HEALTHY) == HealthLevel.HEALTHY

    def test_one_critical(self):
        assert _worst_level(HealthLevel.HEALTHY, HealthLevel.CRITICAL) == HealthLevel.CRITICAL

    def test_warning_vs_info(self):
        assert _worst_level(HealthLevel.WARNING, HealthLevel.INFO) == HealthLevel.WARNING

    def test_unknown_level_treated_as_zero(self):
        result = _worst_level("unknown", HealthLevel.INFO)
        assert result == HealthLevel.INFO


class TestFormatSize:

    def test_bytes(self):
        assert _format_size(500) == "500 o"

    def test_kilobytes(self):
        result = _format_size(1536)
        assert "Ko" in result

    def test_megabytes(self):
        result = _format_size(50 * 1024 * 1024)
        assert "Mo" in result
        assert "50" in result

    def test_gigabytes(self):
        result = _format_size(2 * 1024 * 1024 * 1024)
        assert "Go" in result

    def test_zero(self):
        assert _format_size(0) == "0 o"


class TestHumanizeIntegrityIssue:

    def test_integrity_check_failed(self):
        msg = _humanize_integrity_issue("Integrity check failed: tree 5 page 8")
        assert "anomalies" in msg

    def test_wal_checkpoint_failed(self):
        msg = _humanize_integrity_issue("WAL checkpoint failed: database locked")
        assert "consolidation" in msg

    def test_schema_newer(self):
        msg = _humanize_integrity_issue("Schema version 3 is newer than expected 1")
        assert "recente" in msg

    def test_no_schema_version(self):
        msg = _humanize_integrity_issue("No schema version found")
        assert "version" in msg

    def test_generic_issue(self):
        msg = _humanize_integrity_issue("Totally new error type")
        assert "Totally new error type" in msg
