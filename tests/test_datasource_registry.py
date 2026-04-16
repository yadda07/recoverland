"""Tests for core.datasource_registry - hard edge cases, security, toxic inputs."""
import sqlite3
import tempfile
import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.datasource_registry import (
    register_datasource, lookup_datasource, DatasourceInfo,
    _extract_authcfg, _strip_password_from_uri,
)


def _cleanup(path):
    for p in (path, path + "-wal", path + "-shm"):
        try:
            os.unlink(p)
        except OSError:
            pass


class _FakeCrs:
    def __init__(self, valid=True, authid="EPSG:4326"):
        self._valid = valid
        self._authid = authid

    def isValid(self):
        return self._valid

    def authid(self):
        return self._authid


class _FakeProvider:
    def __init__(self, name="ogr"):
        self._name = name

    def name(self):
        return self._name

    def capabilities(self):
        return 15


class _FakeLayer:
    def __init__(self, name="test", provider="ogr", source="test.gpkg",
                 crs=None, wkb_type=1):
        self._name = name
        self._provider = _FakeProvider(provider)
        self._source = source
        self._crs = crs or _FakeCrs()
        self._wkb_type = wkb_type
        self._id = f"layer_{id(self)}"

    def id(self):
        return self._id

    def name(self):
        return self._name

    def source(self):
        return self._source

    def dataProvider(self):
        return self._provider

    def crs(self):
        return self._crs

    def wkbType(self):
        return self._wkb_type

    def fields(self):
        return []


# ---------------------------------------------------------------------------
# Password stripping (SECURITY CRITICAL)
# ---------------------------------------------------------------------------
class TestStripPassword(unittest.TestCase):
    """Passwords must NEVER be stored in the registry."""

    def test_single_quoted_password_stripped(self):
        uri = "host='h' password='s3cret' dbname='d'"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("s3cret", result)
        self.assertIn("password=***", result)

    def test_double_quoted_password_stripped(self):
        uri = 'host="h" password="s3cret" dbname="d"'
        result = _strip_password_from_uri(uri)
        self.assertNotIn("s3cret", result)
        self.assertIn("password=***", result)

    def test_unquoted_password_stripped(self):
        uri = "host=h password=s3cret dbname=d"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("s3cret", result)
        self.assertIn("password=***", result)

    def test_no_password_unchanged(self):
        uri = "host='h' dbname='d' authcfg=abc123"
        result = _strip_password_from_uri(uri)
        self.assertEqual(result, uri)

    def test_empty_password_stripped(self):
        uri = "host='h' password='' dbname='d'"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("password=''", result)

    def test_password_with_special_chars(self):
        uri = "host='h' password='p@ss!w0rd#$%' dbname='d'"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("p@ss!w0rd#$%", result)

    def test_multiple_passwords_all_stripped(self):
        uri = "password='a' host='h' password='b'"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("'a'", result)
        self.assertNotIn("'b'", result)


# ---------------------------------------------------------------------------
# authcfg extraction
# ---------------------------------------------------------------------------
class TestExtractAuthcfg(unittest.TestCase):
    def test_authcfg_present(self):
        self.assertEqual(_extract_authcfg("host='h' authcfg=abc123 dbname='d'"), "abc123")

    def test_authcfg_absent(self):
        self.assertEqual(_extract_authcfg("host='h' dbname='d'"), "")

    def test_authcfg_in_gpkg_uri(self):
        self.assertEqual(_extract_authcfg("/data/test.gpkg|authcfg=XYZ99"), "XYZ99")

    def test_empty_uri(self):
        self.assertEqual(_extract_authcfg(""), "")


# ---------------------------------------------------------------------------
# Register + lookup round-trip
# ---------------------------------------------------------------------------
class TestRegisterAndLookup(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        self.conn = sqlite3.connect(self.tmp_path)
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        _cleanup(self.tmp_path)

    def test_register_then_lookup(self):
        layer = _FakeLayer("parcelles", "ogr", "/data/parcelles.gpkg")
        register_datasource(self.conn, layer)
        from recoverland.core.identity import compute_datasource_fingerprint
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIsNotNone(info)
        self.assertEqual(info.provider_type, "ogr")
        self.assertIn("parcelles.gpkg", info.source_uri)
        self.assertEqual(info.layer_name, "parcelles")

    def test_lookup_nonexistent_returns_none(self):
        info = lookup_datasource(self.conn, "nonexistent::fingerprint")
        self.assertIsNone(info)

    def test_register_updates_on_second_call(self):
        layer1 = _FakeLayer("v1", "ogr", "/data/test.gpkg")
        register_datasource(self.conn, layer1)
        layer2 = _FakeLayer("v2", "ogr", "/data/test.gpkg")
        register_datasource(self.conn, layer2)
        from recoverland.core.identity import compute_datasource_fingerprint
        fp = compute_datasource_fingerprint(layer2)
        info = lookup_datasource(self.conn, fp)
        self.assertEqual(info.layer_name, "v2")

    def test_password_never_stored(self):
        layer = _FakeLayer(
            "pg_layer", "postgres",
            "host='h' password='SECRET' dbname='d' table='t'",
        )
        register_datasource(self.conn, layer)
        from recoverland.core.identity import compute_datasource_fingerprint
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertNotIn("SECRET", info.source_uri)

    def test_authcfg_stored(self):
        layer = _FakeLayer(
            "pg_layer", "postgres",
            "host='h' authcfg=ABC123 dbname='d' table='t'",
        )
        register_datasource(self.conn, layer)
        from recoverland.core.identity import compute_datasource_fingerprint
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertEqual(info.authcfg, "ABC123")

    def test_sql_injection_in_layer_name(self):
        toxic = "'; DROP TABLE datasource_registry; --"
        layer = _FakeLayer(toxic, "ogr", "/data/test.gpkg")
        register_datasource(self.conn, layer)
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='datasource_registry'"
        ).fetchone()
        self.assertIsNotNone(tables)

    def test_unicode_source_uri(self):
        layer = _FakeLayer("donnees", "ogr", "/donnees/fichier.gpkg")
        register_datasource(self.conn, layer)
        from recoverland.core.identity import compute_datasource_fingerprint
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIn("fichier.gpkg", info.source_uri)

    def test_very_long_uri(self):
        long_path = "/data/" + "a" * 5000 + ".gpkg"
        layer = _FakeLayer("long", "ogr", long_path)
        register_datasource(self.conn, layer)
        from recoverland.core.identity import compute_datasource_fingerprint
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIn("a" * 100, info.source_uri)

    def test_multiple_providers(self):
        ogr = _FakeLayer("f1", "ogr", "/data/a.gpkg")
        pg = _FakeLayer("f2", "postgres", "host='h' dbname='d' table='t'")
        spa = _FakeLayer("f3", "spatialite", "/data/b.db")
        for lyr in (ogr, pg, spa):
            register_datasource(self.conn, lyr)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM datasource_registry"
        ).fetchone()[0]
        self.assertEqual(count, 3)


# ---------------------------------------------------------------------------
# DatasourceInfo NamedTuple
# ---------------------------------------------------------------------------
class TestDatasourceInfo(unittest.TestCase):
    def test_fields(self):
        info = DatasourceInfo(
            fingerprint="ogr::test.gpkg",
            provider_type="ogr",
            source_uri="/data/test.gpkg",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        self.assertEqual(info.fingerprint, "ogr::test.gpkg")
        self.assertEqual(info.provider_type, "ogr")
        self.assertEqual(info.geometry_type, "Point")


if __name__ == '__main__':
    unittest.main()
