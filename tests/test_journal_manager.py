"""Tests for core.journal_manager module (RLU-003)."""
import tempfile
import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.journal_manager import (
    JournalManager, _resolve_journal_path,
    _journal_for_saved_project, _journal_for_unsaved_project,
    get_journal_size_bytes, format_journal_size,
)


class TestResolveJournalPath(unittest.TestCase):

    def test_saved_project(self):
        with tempfile.NamedTemporaryFile(suffix='.qgz', delete=False) as tmp:
            project_path = tmp.name
        try:
            result = _resolve_journal_path(project_path, "/fake/profile")
            self.assertIn(".recoverland", result)
            self.assertTrue(result.endswith("recoverland_audit.sqlite"))
        finally:
            os.unlink(project_path)

    def test_unsaved_project(self):
        result = _resolve_journal_path("", "/fake/profile")
        self.assertIn("recoverland", result)
        self.assertIn("audit_", result)
        self.assertTrue(result.endswith(".sqlite"))

    def test_unsaved_project_same_token_same_path(self):
        a = _resolve_journal_path("", "/fake/profile", "token-a")
        b = _resolve_journal_path("", "/fake/profile", "token-a")
        self.assertEqual(a, b)

    def test_unsaved_project_different_tokens_different_paths(self):
        a = _resolve_journal_path("", "/fake/profile", "token-a")
        b = _resolve_journal_path("", "/fake/profile", "token-b")
        self.assertNotEqual(a, b)


class TestJournalForSavedProject(unittest.TestCase):

    def test_path_structure(self):
        result = _journal_for_saved_project("C:/projects/myproject.qgz")
        self.assertIn(".recoverland", result)
        self.assertIn("C:", result)


class TestJournalForUnsavedProject(unittest.TestCase):

    def test_hash_in_filename(self):
        result = _journal_for_unsaved_project("/profile", "hint1")
        self.assertIn("audit_", result)
        self.assertTrue(result.endswith(".sqlite"))

    def test_different_hints_different_paths(self):
        a = _journal_for_unsaved_project("/profile", "hint1")
        b = _journal_for_unsaved_project("/profile", "hint2")
        self.assertNotEqual(a, b)

    def test_empty_hint_uses_session_token(self):
        result = _journal_for_unsaved_project("/profile", "", "session-a")
        import hashlib
        expected_hint = "session-a"
        expected_fp = hashlib.sha256(expected_hint.encode("utf-8")).hexdigest()[:16]
        self.assertIn(expected_fp, result)

    def test_empty_hints_different_tokens_differ(self):
        import hashlib
        fp_a = hashlib.sha256("session_a".encode()).hexdigest()[:16]
        fp_b = hashlib.sha256("session_b".encode()).hexdigest()[:16]
        self.assertNotEqual(fp_a, fp_b)


class TestJournalManager(unittest.TestCase):

    def test_open_creates_db(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_file = os.path.join(tmpdir, "test.qgz")
            with open(project_file, "w") as f:
                f.write("fake")
            jm = JournalManager()
            try:
                path = jm.open_for_project(project_file, tmpdir)
                self.assertTrue(os.path.isfile(path))
                self.assertTrue(jm.is_open)
                self.assertIsNotNone(jm.path)
            finally:
                jm.close()

    def test_close(self):
        jm = JournalManager()
        self.assertFalse(jm.is_open)
        jm.close()

    def test_get_connection_raises_when_closed(self):
        jm = JournalManager()
        with self.assertRaises(RuntimeError):
            jm.get_connection()

    def test_create_read_connection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_file = os.path.join(tmpdir, "test.qgz")
            with open(project_file, "w") as f:
                f.write("fake")
            jm = JournalManager()
            try:
                jm.open_for_project(project_file, tmpdir)
                read_conn = jm.create_read_connection()
                self.assertIsNotNone(read_conn)
                read_conn.close()
            finally:
                jm.close()

    def test_create_write_connection(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            project_file = os.path.join(tmpdir, "test.qgz")
            with open(project_file, "w") as f:
                f.write("fake")
            jm = JournalManager()
            try:
                jm.open_for_project(project_file, tmpdir)
                write_conn = jm.create_write_connection()
                self.assertIsNotNone(write_conn)
                write_conn.close()
            finally:
                jm.close()

    def test_unsaved_reopen_uses_new_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            jm = JournalManager()
            try:
                first = jm.open_for_project("", tmpdir)
                jm.close()
                second = jm.open_for_project("", tmpdir)
                self.assertNotEqual(first, second)
            finally:
                jm.close()


class TestJournalSizeUtils(unittest.TestCase):

    def test_size_nonexistent(self):
        self.assertEqual(get_journal_size_bytes("/nonexistent"), 0)

    def test_format_bytes(self):
        self.assertEqual(format_journal_size(500), "500 B")

    def test_format_kb(self):
        result = format_journal_size(5000)
        self.assertIn("KB", result)

    def test_format_mb(self):
        result = format_journal_size(5_000_000)
        self.assertIn("MB", result)

    def test_format_gb(self):
        result = format_journal_size(5_000_000_000)
        self.assertIn("GB", result)


if __name__ == '__main__':
    unittest.main()
