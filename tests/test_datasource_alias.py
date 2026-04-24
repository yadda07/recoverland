"""Tests for datasource_alias (BLK-07)."""
import sqlite3
import unittest

from recoverland.core.datasource_alias import (
    add_alias, remove_alias, list_aliases, resolve_fingerprints,
)
from recoverland.core.sqlite_schema import initialize_schema


def _db():
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


class TestAliasSchema(unittest.TestCase):
    def test_schema_creates_datasource_alias_table(self):
        conn = _db()
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='datasource_alias'"
        ).fetchone()
        self.assertIsNotNone(row, "datasource_alias table must exist (v4)")

    def test_schema_version_is_4(self):
        from recoverland.core.sqlite_schema import (
            CURRENT_SCHEMA_VERSION, get_schema_version,
        )
        self.assertEqual(CURRENT_SCHEMA_VERSION, 4)
        conn = _db()
        self.assertEqual(get_schema_version(conn), 4)


class TestAddAlias(unittest.TestCase):
    def test_add_simple(self):
        conn = _db()
        self.assertTrue(add_alias(conn, "fp_new", "fp_old", "path change"))
        rows = list_aliases(conn)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "fp_new")
        self.assertEqual(rows[0][1], "fp_old")
        self.assertEqual(rows[0][3], "path change")

    def test_refuse_self_alias(self):
        conn = _db()
        self.assertFalse(add_alias(conn, "fp_x", "fp_x"))

    def test_refuse_empty(self):
        conn = _db()
        self.assertFalse(add_alias(conn, "", "fp_x"))
        self.assertFalse(add_alias(conn, "fp_x", ""))

    def test_replace_existing(self):
        conn = _db()
        add_alias(conn, "fp_a", "fp_b")
        add_alias(conn, "fp_a", "fp_c")  # replace
        rows = list_aliases(conn)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "fp_c")

    def test_refuse_direct_cycle(self):
        # fp_a -> fp_b, then fp_b -> fp_a must be refused
        conn = _db()
        add_alias(conn, "fp_a", "fp_b")
        self.assertFalse(add_alias(conn, "fp_b", "fp_a"))

    def test_refuse_transitive_cycle(self):
        # fp_a -> fp_b, fp_b -> fp_c, then fp_c -> fp_a must be refused
        conn = _db()
        add_alias(conn, "fp_a", "fp_b")
        add_alias(conn, "fp_b", "fp_c")
        self.assertFalse(add_alias(conn, "fp_c", "fp_a"))


class TestResolve(unittest.TestCase):
    def test_resolve_no_alias(self):
        conn = _db()
        self.assertEqual(resolve_fingerprints(conn, "fp_x"), ["fp_x"])

    def test_resolve_empty_returns_empty(self):
        conn = _db()
        self.assertEqual(resolve_fingerprints(conn, ""), [])

    def test_resolve_from_target_gathers_aliases(self):
        conn = _db()
        add_alias(conn, "fp_new1", "fp_target")
        add_alias(conn, "fp_new2", "fp_target")
        result = resolve_fingerprints(conn, "fp_target")
        self.assertIn("fp_target", result)
        self.assertIn("fp_new1", result)
        self.assertIn("fp_new2", result)

    def test_resolve_from_alias_gathers_target(self):
        conn = _db()
        add_alias(conn, "fp_new", "fp_target")
        result = resolve_fingerprints(conn, "fp_new")
        self.assertIn("fp_new", result)
        self.assertIn("fp_target", result)

    def test_resolve_transitive(self):
        # Chain: fp_a -> fp_b -> fp_c. Query fp_c must return all three.
        conn = _db()
        add_alias(conn, "fp_a", "fp_b")
        add_alias(conn, "fp_b", "fp_c")
        result = resolve_fingerprints(conn, "fp_c")
        self.assertCountEqual(result, ["fp_a", "fp_b", "fp_c"])


class TestRemove(unittest.TestCase):
    def test_remove_existing(self):
        conn = _db()
        add_alias(conn, "fp_a", "fp_b")
        self.assertEqual(remove_alias(conn, "fp_a"), 1)
        self.assertEqual(list_aliases(conn), [])

    def test_remove_missing(self):
        conn = _db()
        self.assertEqual(remove_alias(conn, "fp_ghost"), 0)


if __name__ == "__main__":
    unittest.main()
