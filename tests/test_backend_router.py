"""Tests for core.backend_router module."""
import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.backend_router import BackendRouter, BackendMode, format_mode_display


class _FakeProvider:
    def __init__(self, name="ogr"):
        self._name = name
    def name(self):
        return self._name
    def capabilities(self):
        return 15


class _FakeLayer:
    def __init__(self, lid, provider_name="ogr", source="test.gpkg"):
        self._id = lid
        self._provider = _FakeProvider(provider_name)
        self._source = source
    def id(self):
        return self._id
    def name(self):
        return "test_layer"
    def source(self):
        return self._source
    def dataProvider(self):
        return self._provider


class _FakeBackend:
    def __init__(self, available=True):
        self._available = available
    def is_available(self):
        return self._available


class TestBackendRouter(unittest.TestCase):
    def test_no_backend_returns_none(self):
        router = BackendRouter()
        layer = _FakeLayer("l1")
        self.assertIsNone(router.resolve_backend(layer))

    def test_sqlite_backend_when_local_active(self):
        router = BackendRouter()
        sqlite = _FakeBackend(True)
        router.set_sqlite_backend(sqlite)
        router.activate_local_mode()
        layer = _FakeLayer("l1")
        self.assertIs(router.resolve_backend(layer), sqlite)

    def test_sqlite_backend_cached(self):
        router = BackendRouter()
        sqlite = _FakeBackend(True)
        router.set_sqlite_backend(sqlite)
        router.activate_local_mode()
        layer = _FakeLayer("l1")
        router.resolve_backend(layer)
        self.assertIs(router.resolve_backend(layer), sqlite)

    def test_invalidate_layer(self):
        router = BackendRouter()
        sqlite = _FakeBackend(True)
        router.set_sqlite_backend(sqlite)
        router.activate_local_mode()
        layer = _FakeLayer("l1")
        router.resolve_backend(layer)
        router.invalidate_layer("l1")
        self.assertIs(router.resolve_backend(layer), sqlite)

    def test_clear_cache(self):
        router = BackendRouter()
        sqlite = _FakeBackend(True)
        router.set_sqlite_backend(sqlite)
        router.activate_local_mode()
        layer = _FakeLayer("l1")
        router.resolve_backend(layer)
        router.clear_cache()
        router.deactivate_local_mode()
        self.assertIsNone(router.resolve_backend(layer))

    def test_resolve_mode_none_when_no_backend(self):
        router = BackendRouter()
        layer = _FakeLayer("l1")
        self.assertEqual(router.resolve_mode(layer), BackendMode.NONE)

    def test_is_local_active(self):
        router = BackendRouter()
        self.assertFalse(router.is_local_active)
        router.activate_local_mode()
        self.assertTrue(router.is_local_active)
        router.deactivate_local_mode()
        self.assertFalse(router.is_local_active)


class TestFormatModeDisplay(unittest.TestCase):
    def test_postgres(self):
        self.assertIn("PostgreSQL", format_mode_display(BackendMode.POSTGRES_LEGACY))

    def test_sqlite(self):
        self.assertIn("Local", format_mode_display(BackendMode.LOCAL_SQLITE))

    def test_none(self):
        self.assertIn("Not audited", format_mode_display(BackendMode.NONE))


if __name__ == '__main__':
    unittest.main()
