"""Tests for core.local_settings module (RLU-060)."""
import sqlite3
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.local_settings import LocalSettings


class TestLocalSettings(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)
        self.settings = LocalSettings(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_default_local_mode_inactive(self):
        self.assertFalse(self.settings.is_local_active)

    def test_activate_local_mode(self):
        self.settings.activate_local_mode()
        self.assertTrue(self.settings.is_local_active)

    def test_deactivate_local_mode(self):
        self.settings.activate_local_mode()
        self.settings.deactivate_local_mode()
        self.assertFalse(self.settings.is_local_active)

    def test_get_default(self):
        self.assertEqual(self.settings.get("retention_days"), "365")

    def test_set_and_get(self):
        self.settings.set("retention_days", "90")
        self.assertEqual(self.settings.get("retention_days"), "90")

    def test_user_name_override_empty(self):
        self.assertIsNone(self.settings.user_name_override)

    def test_user_name_override_set(self):
        self.settings.set_user_name_override("alice")
        self.assertEqual(self.settings.user_name_override, "alice")

    def test_retention_days_property(self):
        self.assertEqual(self.settings.retention_days, 365)
        self.settings.set_retention_days(90)
        self.assertEqual(self.settings.retention_days, 90)

    def test_retention_days_validation(self):
        with self.assertRaises(ValueError):
            self.settings.set_retention_days(0)

    def test_max_events_property(self):
        self.assertEqual(self.settings.max_events, 1_000_000)

    def test_capture_inserts_default(self):
        self.assertTrue(self.settings.capture_inserts)

    def test_to_dict(self):
        d = self.settings.to_dict()
        self.assertIn("local_mode_active", d)
        self.assertIn("retention_days", d)

    def test_persistence_across_reload(self):
        self.settings.set("retention_days", "60")
        reloaded = LocalSettings(self.conn)
        self.assertEqual(reloaded.get("retention_days"), "60")

    def test_set_persists_in_db(self):
        self.settings.set("local_mode_active", "1")
        row = self.conn.execute(
            "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
            ("local_mode_active",)
        ).fetchone()
        self.assertEqual(row[0], "1")


if __name__ == '__main__':
    unittest.main()
