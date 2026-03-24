"""Tests for core.integrity module (RLU-064)."""
import sqlite3
import tempfile
import json
import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.integrity import (
    check_journal_integrity, save_pending_events, _get_pending_path,
)


class TestCheckJournalIntegrity(unittest.TestCase):

    def test_healthy_journal(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.close()
            result = check_journal_integrity(tmp_path)
            self.assertTrue(result.is_healthy)
            self.assertEqual(len(result.issues), 0)
        finally:
            self._cleanup(tmp_path)

    def test_missing_file(self):
        result = check_journal_integrity("/nonexistent/path/audit.sqlite")
        self.assertFalse(result.is_healthy)
        self.assertIn("not found", result.issues[0].lower())

    def test_no_schema_version(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            conn.execute("CREATE TABLE dummy (id INTEGER)")
            conn.commit()
            conn.close()
            result = check_journal_integrity(tmp_path)
            self.assertFalse(result.is_healthy)
        finally:
            self._cleanup(tmp_path)

    def test_pending_events_recovered(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.close()

            pending_path = _get_pending_path(tmp_path)
            events = [{
                "project_fingerprint": "p",
                "datasource_fingerprint": "d",
                "provider_type": "ogr",
                "operation_type": "DELETE",
                "attributes_json": "{}",
                "user_name": "test",
                "created_at": "2025-01-01T00:00:00",
            }]
            with open(pending_path, "w") as f:
                json.dump(events, f)

            result = check_journal_integrity(tmp_path)
            self.assertEqual(result.recovered_events, 1)
            self.assertFalse(os.path.exists(pending_path))

            conn = sqlite3.connect(tmp_path)
            count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
            conn.close()
            self.assertEqual(count, 1)
        finally:
            self._cleanup(tmp_path)

    @staticmethod
    def _cleanup(path):
        for p in (path, path + "-wal", path + "-shm"):
            try:
                os.unlink(p)
            except OSError:
                pass


class TestSavePendingEvents(unittest.TestCase):

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "audit.sqlite")
            events = [{"operation_type": "DELETE", "user_name": "alice"}]
            save_pending_events(db_path, events)

            pending_path = _get_pending_path(db_path)
            self.assertTrue(os.path.exists(pending_path))

            with open(pending_path, "r") as f:
                loaded = json.load(f)
            self.assertEqual(len(loaded), 1)
            self.assertEqual(loaded[0]["user_name"], "alice")


if __name__ == '__main__':
    unittest.main()
