"""Tests for BLK-06 multi-writer lock protection.

Tests target the pure helpers (_is_pid_alive, _read_lock_file,
_acquire_writer_lock, _release_writer_lock) to avoid the heavy SQLite
open path and its schema migration.
"""
import os
import socket
import tempfile
import unittest

from recoverland.core.journal_manager import (
    JournalManager, JournalLockError,
    _is_pid_alive, _read_lock_file, _LOCK_SUFFIX,
)


# ---------------------------------------------------------------------------
# _is_pid_alive
# ---------------------------------------------------------------------------

class TestIsPidAlive(unittest.TestCase):
    def test_current_pid_is_alive(self):
        self.assertTrue(_is_pid_alive(os.getpid()))

    def test_pid_zero_is_not_alive(self):
        self.assertFalse(_is_pid_alive(0))

    def test_negative_pid_is_not_alive(self):
        self.assertFalse(_is_pid_alive(-1))


# ---------------------------------------------------------------------------
# _read_lock_file
# ---------------------------------------------------------------------------

class TestReadLockFile(unittest.TestCase):
    def _write_tmp(self, content: str) -> str:
        fd, path = tempfile.mkstemp(suffix=_LOCK_SUFFIX)
        with os.fdopen(fd, 'w', encoding='utf-8') as fh:
            fh.write(content)
        return path

    def test_missing_file_returns_none(self):
        self.assertIsNone(_read_lock_file("/nonexistent/path/lock.rlwriter"))

    def test_valid_content_parses(self):
        path = self._write_tmp("1234|hostname|1700000000\n")
        try:
            info = _read_lock_file(path)
            self.assertEqual(info, (1234, "hostname", 1700000000))
        finally:
            os.unlink(path)

    def test_malformed_pid_returns_none(self):
        path = self._write_tmp("not_a_pid|host|1")
        try:
            self.assertIsNone(_read_lock_file(path))
        finally:
            os.unlink(path)

    def test_too_few_fields_returns_none(self):
        path = self._write_tmp("1234|hostname")
        try:
            self.assertIsNone(_read_lock_file(path))
        finally:
            os.unlink(path)

    def test_empty_file_returns_none(self):
        path = self._write_tmp("")
        try:
            self.assertIsNone(_read_lock_file(path))
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _acquire_writer_lock / _release_writer_lock
# ---------------------------------------------------------------------------

class TestAcquireReleaseLock(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.journal_path = os.path.join(self.tmpdir, "audit.sqlite")
        self.lock_path = self.journal_path + _LOCK_SUFFIX

    def tearDown(self):
        for name in os.listdir(self.tmpdir):
            try:
                os.unlink(os.path.join(self.tmpdir, name))
            except OSError:
                pass
        try:
            os.rmdir(self.tmpdir)
        except OSError:
            pass

    def test_first_acquire_creates_lock(self):
        JournalManager._acquire_writer_lock(self.journal_path)
        self.assertTrue(os.path.isfile(self.lock_path))
        info = _read_lock_file(self.lock_path)
        self.assertIsNotNone(info)
        self.assertEqual(info[0], os.getpid())
        self.assertEqual(info[1], socket.gethostname())

    def test_release_removes_lock(self):
        JournalManager._acquire_writer_lock(self.journal_path)
        JournalManager._release_writer_lock(self.journal_path)
        self.assertFalse(os.path.isfile(self.lock_path))

    def test_reacquire_same_process_succeeds(self):
        JournalManager._acquire_writer_lock(self.journal_path)
        # Same process, same PID: second acquire must not raise.
        JournalManager._acquire_writer_lock(self.journal_path)
        JournalManager._release_writer_lock(self.journal_path)

    def test_stale_dead_pid_is_reclaimed(self):
        # Pre-create a lock with a PID that does not exist.
        with open(self.lock_path, 'w', encoding='utf-8') as fh:
            fh.write(f"9999999|{socket.gethostname()}|1\n")
        # Acquire must succeed by reclaiming the stale lock.
        JournalManager._acquire_writer_lock(self.journal_path)
        info = _read_lock_file(self.lock_path)
        self.assertEqual(info[0], os.getpid())
        JournalManager._release_writer_lock(self.journal_path)

    def test_live_foreign_pid_same_host_raises(self):
        # Find any live PID that is not us, on our host.
        alive_other_pid = None
        for candidate in (1, 4, os.getppid()):
            if candidate and candidate != os.getpid() and _is_pid_alive(candidate):
                alive_other_pid = candidate
                break
        if alive_other_pid is None:
            self.skipTest("No live foreign PID available on this platform")

        with open(self.lock_path, 'w', encoding='utf-8') as fh:
            fh.write(f"{alive_other_pid}|{socket.gethostname()}|1\n")

        with self.assertRaises(JournalLockError):
            JournalManager._acquire_writer_lock(self.journal_path)

        # Lock-file must be preserved (do not steal from the live owner).
        info = _read_lock_file(self.lock_path)
        self.assertEqual(info[0], alive_other_pid)

    def test_different_host_lock_is_reclaimed(self):
        # Another host's lock must be reclaimed (can't check their PIDs).
        with open(self.lock_path, 'w', encoding='utf-8') as fh:
            fh.write(f"{os.getpid()}|some-other-host-xyz|1\n")
        JournalManager._acquire_writer_lock(self.journal_path)
        info = _read_lock_file(self.lock_path)
        self.assertEqual(info[1], socket.gethostname())
        JournalManager._release_writer_lock(self.journal_path)

    def test_release_does_not_steal_foreign_lock(self):
        # Simulate a foreign lock-file and ensure release leaves it alone.
        with open(self.lock_path, 'w', encoding='utf-8') as fh:
            fh.write("12345|foreign-host|1\n")
        JournalManager._release_writer_lock(self.journal_path)
        self.assertTrue(os.path.isfile(self.lock_path),
                        "release must not remove a lock we do not own")


if __name__ == "__main__":
    unittest.main()
