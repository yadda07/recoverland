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


class TestLocalSettingsEdgeCases(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)
        self.settings = LocalSettings(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_retention_days_negative_raises(self):
        with self.assertRaises(ValueError):
            self.settings.set_retention_days(-1)

    def test_retention_days_one_accepted(self):
        self.settings.set_retention_days(1)
        self.assertEqual(self.settings.retention_days, 1)

    def test_retention_days_very_large(self):
        self.settings.set_retention_days(36500)
        self.assertEqual(self.settings.retention_days, 36500)

    def test_retention_days_invalid_string_in_db_falls_back(self):
        self.conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, setting_value, updated_at) "
            "VALUES ('retention_days', 'not_a_number', '2025-01-01')"
        )
        self.conn.commit()
        reloaded = LocalSettings(self.conn)
        self.assertEqual(reloaded.retention_days, 365)

    def test_max_events_invalid_string_falls_back(self):
        self.conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, setting_value, updated_at) "
            "VALUES ('max_events', 'garbage', '2025-01-01')"
        )
        self.conn.commit()
        reloaded = LocalSettings(self.conn)
        self.assertEqual(reloaded.max_events, 1_000_000)

    def test_capture_inserts_toggle(self):
        self.assertTrue(self.settings.capture_inserts)
        self.settings.set_capture_inserts("0")
        self.assertFalse(self.settings.capture_inserts)
        self.settings.set_capture_inserts("1")
        self.assertTrue(self.settings.capture_inserts)

    def test_unknown_key_returns_empty_string(self):
        self.assertEqual(self.settings.get("nonexistent_key"), "")

    def test_user_name_override_whitespace_stripped(self):
        self.settings.set_user_name_override("  alice  ")
        self.assertEqual(self.settings.user_name_override, "alice")

    def test_user_name_override_empty_returns_none(self):
        self.settings.set_user_name_override("")
        self.assertIsNone(self.settings.user_name_override)

    def test_activate_deactivate_idempotent(self):
        self.settings.activate_local_mode()
        self.settings.activate_local_mode()
        self.assertTrue(self.settings.is_local_active)
        self.settings.deactivate_local_mode()
        self.settings.deactivate_local_mode()
        self.assertFalse(self.settings.is_local_active)

    def test_to_dict_contains_all_defaults(self):
        d = self.settings.to_dict()
        self.assertIn("local_mode_active", d)
        self.assertIn("retention_days", d)
        self.assertIn("max_events", d)
        self.assertIn("capture_inserts", d)
        self.assertIn("user_name_override", d)

    def test_to_dict_reflects_set_values(self):
        self.settings.set_retention_days(60)
        self.settings.activate_local_mode()
        d = self.settings.to_dict()
        self.assertEqual(d["retention_days"], "60")
        self.assertEqual(d["local_mode_active"], "1")

    def test_sql_injection_in_key(self):
        toxic = "'; DROP TABLE backend_settings; --"
        self.settings.set(toxic, "value")
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='backend_settings'"
        ).fetchone()
        self.assertIsNotNone(tables)

    def test_sql_injection_in_value(self):
        toxic = "'); DROP TABLE backend_settings; --"
        self.settings.set("test_key", toxic)
        retrieved = self.settings.get("test_key")
        self.assertEqual(retrieved, toxic)

    def test_unicode_values(self):
        self.settings.set_user_name_override("jean-pierre")
        self.assertEqual(self.settings.user_name_override, "jean-pierre")

    def test_very_long_value(self):
        long_val = "x" * 10_000
        self.settings.set("test_long", long_val)
        self.assertEqual(self.settings.get("test_long"), long_val)


if __name__ == '__main__':
    unittest.main()
