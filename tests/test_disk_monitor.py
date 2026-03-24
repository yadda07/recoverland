"""Robust tests for disk_monitor.py (UX-A04).

Covers: disk space detection, message formatting, edge cases,
nonexistent paths, volume extraction.
"""
import os
import tempfile

from recoverland.core.disk_monitor import (
    check_disk_for_path,
    format_disk_message,
    DiskStatus,
    _find_existing_parent,
    _extract_volume,
    _fmt,
)


class TestCheckDiskForPath:

    def test_empty_path(self):
        s = check_disk_for_path("")
        assert isinstance(s, DiskStatus)
        assert not s.is_low
        assert not s.is_critical

    def test_temp_dir(self):
        s = check_disk_for_path(tempfile.gettempdir())
        assert s.free_bytes >= 0
        assert s.total_bytes >= 0
        assert isinstance(s.volume_path, str)

    def test_file_in_temp(self):
        path = os.path.join(tempfile.gettempdir(), "fake_journal.sqlite")
        s = check_disk_for_path(path)
        assert s.free_bytes >= 0

    def test_deeply_nonexistent_path(self):
        s = check_disk_for_path("/nonexistent/deep/path/file.db")
        assert isinstance(s, DiskStatus)

    def test_none_path_via_empty(self):
        s = check_disk_for_path("")
        assert not s.is_low


class TestFormatDiskMessage:

    def test_healthy_empty_message(self):
        s = DiskStatus(1_000_000_000, 10_000_000_000, "C:", False, False)
        assert format_disk_message(s) == ""

    def test_low_space(self):
        s = DiskStatus(400_000_000, 10_000_000_000, "C:", True, False)
        msg = format_disk_message(s)
        assert "faible" in msg
        assert "C:" in msg

    def test_critical_space(self):
        s = DiskStatus(50_000_000, 10_000_000_000, "D:", False, True)
        msg = format_disk_message(s)
        assert "critique" in msg
        assert "D:" in msg
        assert "desactive" in msg

    def test_both_flags_critical_wins(self):
        s = DiskStatus(50_000_000, 10_000_000_000, "C:", True, True)
        msg = format_disk_message(s)
        assert "critique" in msg


class TestFindExistingParent:

    def test_existing_dir(self):
        result = _find_existing_parent(tempfile.gettempdir())
        assert os.path.exists(result)

    def test_nonexistent_deep(self):
        path = os.path.join(tempfile.gettempdir(), "a", "b", "c", "d")
        result = _find_existing_parent(path)
        assert os.path.exists(result)

    def test_root_returns_something(self):
        if os.name == 'nt':
            result = _find_existing_parent("C:\\nonexistent")
            assert result == "C:\\"
        else:
            result = _find_existing_parent("/nonexistent")
            assert result == "/"


class TestExtractVolume:

    def test_windows_drive(self):
        if os.name == 'nt':
            assert _extract_volume("C:\\Users\\test") == "C:"

    def test_unix_root(self):
        if os.name != 'nt':
            assert _extract_volume("/home/user") == "/"

    def test_relative_path(self):
        result = _extract_volume("relative/path")
        assert isinstance(result, str)


class TestFmt:

    def test_bytes(self):
        assert _fmt(500) == "500 o"

    def test_zero(self):
        assert _fmt(0) == "0 o"

    def test_kilobytes(self):
        result = _fmt(2048)
        assert "Ko" in result

    def test_megabytes(self):
        result = _fmt(100 * 1024 * 1024)
        assert "Mo" in result

    def test_gigabytes(self):
        result = _fmt(5 * 1024 * 1024 * 1024)
        assert "Go" in result

    def test_exact_1kb(self):
        result = _fmt(1024)
        assert "Ko" in result

    def test_exact_1mb(self):
        result = _fmt(1024 * 1024)
        assert "Mo" in result

    def test_exact_1gb(self):
        result = _fmt(1024 * 1024 * 1024)
        assert "Go" in result
