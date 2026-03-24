"""Tests for core.user_identity module."""
import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.user_identity import (
    resolve_user_name,
    invalidate_cache,
    _get_os_login,
)


class TestResolveUserName(unittest.TestCase):
    def setUp(self):
        invalidate_cache()

    def tearDown(self):
        invalidate_cache()

    def test_explicit_config_overrides(self):
        result = resolve_user_name("admin_user")
        self.assertEqual(result, "admin_user")

    def test_explicit_config_strips(self):
        result = resolve_user_name("  spaced  ")
        self.assertEqual(result, "spaced")

    def test_empty_config_falls_back(self):
        result = resolve_user_name("")
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_none_config_falls_back(self):
        result = resolve_user_name(None)
        self.assertIsInstance(result, str)
        self.assertTrue(len(result) > 0)

    def test_never_returns_empty(self):
        result = resolve_user_name()
        self.assertIsNotNone(result)
        self.assertNotEqual(result, "")

    def test_cache_persists(self):
        first = resolve_user_name()
        second = resolve_user_name()
        self.assertEqual(first, second)

    def test_invalidate_clears_cache(self):
        resolve_user_name("user_a")
        invalidate_cache()
        result = resolve_user_name("user_b")
        self.assertEqual(result, "user_b")

    def test_env_var_override(self):
        invalidate_cache()
        os.environ["RECOVERLAND_USER"] = "env_test_user"
        try:
            result = resolve_user_name()
            self.assertEqual(result, "env_test_user")
        finally:
            del os.environ["RECOVERLAND_USER"]
            invalidate_cache()


class TestGetOsLogin(unittest.TestCase):
    def test_returns_string_or_none(self):
        result = _get_os_login()
        self.assertTrue(result is None or isinstance(result, str))


if __name__ == '__main__':
    unittest.main()
