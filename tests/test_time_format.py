"""Robust tests for time_format.py (UX-E01).

Covers: relative time formatting, absolute formatting, full timestamps,
history span computation, ISO parsing edge cases, timezone handling.
"""
from datetime import datetime, timezone, timedelta

from recoverland.core.time_format import (
    format_relative_time,
    format_short_absolute,
    format_full_timestamp,
    compute_history_span,
    _parse_iso,
)


def _iso_ago(seconds: int) -> str:
    """Build an ISO timestamp N seconds in the past."""
    dt = datetime.now(timezone.utc) - timedelta(seconds=seconds)
    return dt.isoformat()


class TestFormatRelativeTime:

    def test_just_now(self):
        ts = _iso_ago(3)
        result = format_relative_time(ts)
        assert "instant" in result

    def test_seconds(self):
        ts = _iso_ago(45)
        result = format_relative_time(ts)
        assert "45s" in result

    def test_minutes(self):
        ts = _iso_ago(300)
        result = format_relative_time(ts)
        assert "5 min" in result

    def test_one_minute(self):
        ts = _iso_ago(60)
        result = format_relative_time(ts)
        assert "1 min" in result

    def test_59_minutes(self):
        ts = _iso_ago(59 * 60)
        result = format_relative_time(ts)
        assert "59 min" in result

    def test_hours(self):
        ts = _iso_ago(7200)
        result = format_relative_time(ts)
        assert "2h" in result

    def test_23_hours(self):
        ts = _iso_ago(23 * 3600)
        result = format_relative_time(ts)
        assert "23h" in result

    def test_yesterday(self):
        ts = _iso_ago(30 * 3600)
        result = format_relative_time(ts)
        assert "hier" in result

    def test_days(self):
        ts = _iso_ago(4 * 86400)
        result = format_relative_time(ts)
        assert "4j" in result

    def test_6_days(self):
        ts = _iso_ago(6 * 86400)
        result = format_relative_time(ts)
        assert "6j" in result

    def test_7_days_switches_to_absolute(self):
        ts = _iso_ago(7 * 86400)
        result = format_relative_time(ts)
        assert "/" in result

    def test_old_date_absolute_format(self):
        result = format_relative_time("2020-01-15T10:30:00")
        assert "15/01" in result
        assert "2020" in result

    def test_empty_string(self):
        assert format_relative_time("") == ""

    def test_none_returns_empty(self):
        assert format_relative_time(None) == ""

    def test_garbage_string_returns_raw(self):
        result = format_relative_time("not a date")
        assert "not a date" in result

    def test_future_timestamp_falls_back(self):
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        result = format_relative_time(future)
        assert "/" in result

    def test_iso_with_timezone(self):
        ts = _iso_ago(120)
        result = format_relative_time(ts)
        assert "2 min" in result

    def test_iso_without_timezone(self):
        dt = datetime.now(timezone.utc) - timedelta(minutes=10)
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S")
        result = format_relative_time(ts)
        assert "10 min" in result

    def test_iso_with_microseconds(self):
        dt = datetime.now(timezone.utc) - timedelta(seconds=30)
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        result = format_relative_time(ts)
        assert "30s" in result

    def test_space_separated_datetime(self):
        dt = datetime.now(timezone.utc) - timedelta(minutes=5)
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
        result = format_relative_time(ts)
        assert "5 min" in result


class TestFormatShortAbsolute:

    def test_same_year(self):
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y-%m-%dT%H:%M:%S")
        result = format_short_absolute(ts)
        assert "/" in result
        assert str(now.year) not in result

    def test_different_year(self):
        result = format_short_absolute("2020-06-15T14:32:00")
        assert "2020" in result
        assert "15/06" in result

    def test_empty(self):
        assert format_short_absolute("") == ""

    def test_none(self):
        assert format_short_absolute(None) == ""


class TestFormatFullTimestamp:

    def test_valid_timestamp(self):
        result = format_full_timestamp("2025-06-15T14:32:05+00:00")
        assert "2025-06-15" in result
        assert "14:32:05" in result
        assert "UTC" in result

    def test_without_tz(self):
        result = format_full_timestamp("2025-06-15T14:32:05")
        assert "UTC" in result

    def test_empty(self):
        assert format_full_timestamp("") == ""

    def test_none(self):
        assert format_full_timestamp(None) == ""

    def test_garbage(self):
        result = format_full_timestamp("garbage")
        assert result == "garbage"


class TestComputeHistorySpan:

    def test_same_day(self):
        result = compute_history_span(
            "2025-06-15T10:00:00", "2025-06-15T14:00:00")
        assert "4 heure" in result

    def test_less_than_hour(self):
        result = compute_history_span(
            "2025-06-15T10:00:00", "2025-06-15T10:30:00")
        assert "< 1 heure" in result

    def test_few_days(self):
        result = compute_history_span(
            "2025-06-10T10:00:00", "2025-06-15T10:00:00")
        assert "5 jour" in result

    def test_months(self):
        result = compute_history_span(
            "2025-01-01T00:00:00", "2025-04-01T00:00:00")
        assert "mois" in result

    def test_years(self):
        result = compute_history_span(
            "2023-01-01T00:00:00", "2025-06-01T00:00:00")
        assert "an" in result

    def test_empty_oldest(self):
        assert compute_history_span("", "2025-06-15T10:00:00") == ""

    def test_empty_newest(self):
        assert compute_history_span("2025-06-15T10:00:00", "") == ""

    def test_both_empty(self):
        assert compute_history_span("", "") == ""

    def test_invalid_dates(self):
        assert compute_history_span("garbage", "also garbage") == ""


class TestParseIso:

    def test_full_iso_with_tz(self):
        dt = _parse_iso("2025-06-15T14:32:05+00:00")
        assert dt is not None
        assert dt.year == 2025
        assert dt.tzinfo is not None

    def test_iso_without_tz(self):
        dt = _parse_iso("2025-06-15T14:32:05")
        assert dt is not None
        assert dt.tzinfo == timezone.utc

    def test_iso_with_microseconds(self):
        dt = _parse_iso("2025-06-15T14:32:05.123456")
        assert dt is not None

    def test_space_separated(self):
        dt = _parse_iso("2025-06-15 14:32:05")
        assert dt is not None

    def test_space_separated_with_microseconds(self):
        dt = _parse_iso("2025-06-15 14:32:05.123456")
        assert dt is not None

    def test_empty_string(self):
        assert _parse_iso("") is None

    def test_none(self):
        assert _parse_iso(None) is None

    def test_whitespace_only(self):
        assert _parse_iso("   ") is None

    def test_garbage(self):
        assert _parse_iso("not-a-date") is None

    def test_integer_input(self):
        assert _parse_iso(12345) is None

    def test_partial_date(self):
        assert _parse_iso("2025-06") is None

    def test_date_only(self):
        assert _parse_iso("2025-06-15") is None

    def test_iso_with_z_suffix(self):
        dt = _parse_iso("2025-06-15T14:32:05+00:00")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_leading_trailing_whitespace(self):
        dt = _parse_iso("  2025-06-15T14:32:05  ")
        assert dt is not None
