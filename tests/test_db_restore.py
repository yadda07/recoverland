"""Tests for DB-backed layer restore: identity, registry, credential resolution."""
import sqlite3
import tempfile
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.identity import (
    compute_datasource_fingerprint,
    _normalize_pg_source,
    _normalize_mssql_source,
    _normalize_oracle_source,
    get_identity_strength_for_layer,
)
from recoverland.core.support_policy import IdentityStrength
from recoverland.core.datasource_registry import (
    register_datasource, lookup_datasource, DatasourceInfo,
    _enrich_db_uri, _find_matching_saved_connection,
    _default_port, _DB_PROVIDERS, _SETTINGS_PREFIX,
    _strip_password_from_uri, create_layer_from_registry,
)


def _cleanup(path):
    for p in (path, path + "-wal", path + "-shm"):
        try:
            os.unlink(p)
        except OSError:
            pass


class _FakeProvider:
    def __init__(self, name="postgres", pk_indexes=None):
        self._name = name
        self._pk_indexes = pk_indexes or [0]

    def name(self):
        return self._name

    def capabilities(self):
        return 15

    def pkAttributeIndexes(self):
        return self._pk_indexes


class _FakeLayer:
    def __init__(self, name="test", provider="postgres", source="",
                 wkb_type=1, pk_indexes=None):
        self._name = name
        self._provider = _FakeProvider(provider, pk_indexes)
        self._source = source
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
        c = MagicMock()
        c.isValid.return_value = True
        c.authid.return_value = "EPSG:4326"
        return c

    def wkbType(self):
        return self._wkb_type

    def fields(self):
        return []


# ---------------------------------------------------------------------------
# PostgreSQL URI normalization
# ---------------------------------------------------------------------------
class TestNormalizePgSource(unittest.TestCase):
    def test_single_quoted(self):
        raw = "host='myhost' port='5433' dbname='mydb' schema='public' table='parcelles'"
        result = _normalize_pg_source(raw)
        self.assertEqual(result, "host=myhost port=5433 dbname=mydb schema=public table=parcelles")

    def test_double_quoted(self):
        raw = 'host="myhost" port="5432" dbname="mydb" schema="geo" table="points"'
        result = _normalize_pg_source(raw)
        self.assertEqual(result, "host=myhost port=5432 dbname=mydb schema=geo table=points")

    def test_unquoted(self):
        raw = "host=myhost dbname=mydb table=parcelles"
        result = _normalize_pg_source(raw)
        self.assertEqual(result, "host=myhost port=5432 dbname=mydb schema=public table=parcelles")

    def test_password_ignored(self):
        raw = "host='h' password='secret' dbname='d' table='t'"
        result = _normalize_pg_source(raw)
        self.assertNotIn("secret", result)
        self.assertIn("host=h", result)

    def test_extra_keys_ignored(self):
        raw = "host='h' dbname='d' table='t' sslmode=require authcfg=ABC123"
        result = _normalize_pg_source(raw)
        self.assertNotIn("sslmode", result)
        self.assertNotIn("authcfg", result)


# ---------------------------------------------------------------------------
# MSSQL URI normalization
# ---------------------------------------------------------------------------
class TestNormalizeMssqlSource(unittest.TestCase):
    def test_basic(self):
        raw = "host='sqlserver' port='1433' dbname='mydb' schema='dbo' table='features'"
        result = _normalize_mssql_source(raw)
        self.assertEqual(result, "host=sqlserver port=1433 dbname=mydb schema=dbo table=features")

    def test_defaults(self):
        raw = "host='sqlserver' dbname='mydb' table='features'"
        result = _normalize_mssql_source(raw)
        self.assertIn("port=1433", result)
        self.assertIn("schema=dbo", result)


# ---------------------------------------------------------------------------
# Oracle URI normalization
# ---------------------------------------------------------------------------
class TestNormalizeOracleSource(unittest.TestCase):
    def test_basic(self):
        raw = "host='oraserver' port='1521' dbname='ORCL' table='parcelles'"
        result = _normalize_oracle_source(raw)
        self.assertEqual(result, "host=oraserver port=1521 dbname=ORCL table=parcelles")

    def test_defaults(self):
        raw = "host='oraserver' dbname='ORCL' table='t'"
        result = _normalize_oracle_source(raw)
        self.assertIn("port=1521", result)


# ---------------------------------------------------------------------------
# Fingerprint stability across DB providers
# ---------------------------------------------------------------------------
class TestFingerprintDbProviders(unittest.TestCase):
    def test_pg_fingerprint_stable(self):
        src = "host='h' port='5432' dbname='d' schema='public' table='t' password='secret'"
        layer = _FakeLayer("test", "postgres", src)
        fp1 = compute_datasource_fingerprint(layer)
        fp2 = compute_datasource_fingerprint(layer)
        self.assertEqual(fp1, fp2)
        self.assertTrue(fp1.startswith("postgres::"))
        self.assertNotIn("secret", fp1)

    def test_mssql_fingerprint_stable(self):
        src = "host='h' port='1433' dbname='d' schema='dbo' table='t'"
        layer = _FakeLayer("test", "mssql", src)
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("mssql::"))

    def test_oracle_fingerprint_stable(self):
        src = "host='h' port='1521' dbname='ORCL' table='t'"
        layer = _FakeLayer("test", "oracle", src)
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("oracle::"))


# ---------------------------------------------------------------------------
# Identity strength for DB providers
# ---------------------------------------------------------------------------
class TestIdentityStrengthDb(unittest.TestCase):
    def test_postgres_strong(self):
        layer = _FakeLayer(provider="postgres")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.STRONG)

    def test_mssql_strong(self):
        layer = _FakeLayer(provider="mssql")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.STRONG)

    def test_oracle_strong(self):
        layer = _FakeLayer(provider="oracle")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.STRONG)


# ---------------------------------------------------------------------------
# _DB_PROVIDERS and _default_port
# ---------------------------------------------------------------------------
class TestDbProviderConstants(unittest.TestCase):
    def test_db_providers_set(self):
        self.assertIn("postgres", _DB_PROVIDERS)
        self.assertIn("mssql", _DB_PROVIDERS)
        self.assertIn("oracle", _DB_PROVIDERS)
        self.assertNotIn("ogr", _DB_PROVIDERS)

    def test_default_port_postgres(self):
        self.assertEqual(_default_port("postgres"), "5432")

    def test_default_port_mssql(self):
        self.assertEqual(_default_port("mssql"), "1433")

    def test_default_port_oracle(self):
        self.assertEqual(_default_port("oracle"), "1521")


# ---------------------------------------------------------------------------
# Register + lookup round-trip for DB layers
# ---------------------------------------------------------------------------
class TestRegisterDbLayers(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        self.conn = sqlite3.connect(self.tmp_path)
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()
        _cleanup(self.tmp_path)

    def test_pg_layer_registered_with_stripped_password(self):
        layer = _FakeLayer(
            "parcelles", "postgres",
            "host='pghost' password='secret123' dbname='mydb' table='parcelles' authcfg=ABC",
        )
        register_datasource(self.conn, layer)
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIsNotNone(info)
        self.assertEqual(info.provider_type, "postgres")
        self.assertNotIn("secret123", info.source_uri)
        self.assertEqual(info.authcfg, "ABC")

    def test_pg_layer_without_authcfg(self):
        layer = _FakeLayer(
            "parcelles", "postgres",
            "host='pghost' password='secret' dbname='mydb' table='parcelles'",
        )
        register_datasource(self.conn, layer)
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIsNotNone(info)
        self.assertEqual(info.authcfg, "")

    def test_mssql_layer_registered(self):
        layer = _FakeLayer(
            "roads", "mssql",
            "host='sqlserver' dbname='geodb' schema='dbo' table='roads'",
        )
        register_datasource(self.conn, layer)
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIsNotNone(info)
        self.assertEqual(info.provider_type, "mssql")

    def test_oracle_layer_registered(self):
        layer = _FakeLayer(
            "parcels", "oracle",
            "host='orahost' dbname='ORCL' table='parcels'",
        )
        register_datasource(self.conn, layer)
        fp = compute_datasource_fingerprint(layer)
        info = lookup_datasource(self.conn, fp)
        self.assertIsNotNone(info)
        self.assertEqual(info.provider_type, "oracle")


# ---------------------------------------------------------------------------
# _enrich_db_uri
# ---------------------------------------------------------------------------
def _make_mock_uri_factory():
    """Create a mock QgsDataSourceUri class that tracks calls."""
    def factory(source_uri=""):
        obj = MagicMock()
        obj.host.return_value = ""
        obj.port.return_value = ""
        obj.database.return_value = ""
        obj.uri.return_value = source_uri + " authcfg=INJECTED"
        return obj
    return factory


class TestEnrichDbUri(unittest.TestCase):
    def test_with_authcfg(self):
        """When authcfg is present, URI is enriched directly."""
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='h' dbname='d' table='t'",
            layer_name="test",
            authcfg="ABC123",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        import qgis.core as qc
        orig = qc.QgsDataSourceUri
        qc.QgsDataSourceUri = _make_mock_uri_factory()
        try:
            result = _enrich_db_uri(info)
            self.assertIsNotNone(result)
        finally:
            qc.QgsDataSourceUri = orig

    def test_without_authcfg_no_saved_connection(self):
        """When no authcfg and no saved connection, returns None."""
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='h' dbname='d' table='t'",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        import qgis.core as qc
        orig = qc.QgsDataSourceUri
        qc.QgsDataSourceUri = _make_mock_uri_factory()
        try:
            with patch.object(
                sys.modules['recoverland.core.datasource_registry'],
                '_find_matching_saved_connection', return_value=None,
            ):
                result = _enrich_db_uri(info)
                self.assertIsNone(result)
        finally:
            qc.QgsDataSourceUri = orig


# ---------------------------------------------------------------------------
# _find_matching_saved_connection
# ---------------------------------------------------------------------------
class TestFindMatchingSavedConnection(unittest.TestCase):
    def test_non_db_provider_returns_none(self):
        info = DatasourceInfo(
            fingerprint="ogr::test",
            provider_type="ogr",
            source_uri="/data/test.gpkg",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        result = _find_matching_saved_connection(info)
        self.assertIsNone(result)

    def _patch_qgis(self, mock_uri_attrs, settings_data):
        """Helper: patch QgsDataSourceUri + QgsSettings on qgis.core."""
        import qgis.core as qc

        def uri_factory(source_uri=""):
            obj = MagicMock()
            for attr, val in mock_uri_attrs.items():
                getattr(obj, attr).return_value = val
            return obj

        mock_settings = MagicMock()
        mock_settings.childGroups.return_value = list(settings_data.keys())
        def value_fn(key, default=""):
            for conn_name, kv in settings_data.items():
                for k, v in kv.items():
                    full_key = f"{_SETTINGS_PREFIX.get('postgres', '')}/{conn_name}/{k}"
                    if key == full_key:
                        return v
            return default
        mock_settings.value.side_effect = value_fn

        originals = (qc.QgsDataSourceUri, qc.QgsSettings)
        qc.QgsDataSourceUri = uri_factory
        qc.QgsSettings = lambda: mock_settings
        return originals

    def _unpatch_qgis(self, originals):
        import qgis.core as qc
        qc.QgsDataSourceUri, qc.QgsSettings = originals

    def test_matching_pg_connection_with_authcfg(self):
        """QGIS saved connection with authcfg is found."""
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='myhost' port='5432' dbname='mydb' table='t'",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        orig = self._patch_qgis(
            {"host": "myhost", "port": "5432", "database": "mydb"},
            {"Production DB": {
                "host": "myhost", "port": "5432", "database": "mydb",
                "authcfg": "XYZ789", "username": "", "password": "",
            }},
        )
        try:
            result = _find_matching_saved_connection(info)
            self.assertIsNotNone(result)
            self.assertEqual(result["authcfg"], "XYZ789")
        finally:
            self._unpatch_qgis(orig)

    def test_matching_pg_connection_with_username(self):
        """QGIS saved connection with username/password is found."""
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='pghost' port='5432' dbname='geodb' table='t'",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        orig = self._patch_qgis(
            {"host": "pghost", "port": "5432", "database": "geodb"},
            {"MyConn": {
                "host": "pghost", "port": "5432", "database": "geodb",
                "authcfg": "", "username": "gis_user", "password": "gis_pass",
            }},
        )
        try:
            result = _find_matching_saved_connection(info)
            self.assertIsNotNone(result)
            self.assertEqual(result["username"], "gis_user")
            self.assertEqual(result["password"], "gis_pass")
        finally:
            self._unpatch_qgis(orig)

    def test_no_match_different_host(self):
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='hostA' port='5432' dbname='db' table='t'",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        orig = self._patch_qgis(
            {"host": "hostA", "port": "5432", "database": "db"},
            {"OtherConn": {
                "host": "hostB", "port": "5432", "database": "db",
            }},
        )
        try:
            result = _find_matching_saved_connection(info)
            self.assertIsNone(result)
        finally:
            self._unpatch_qgis(orig)

    def test_case_insensitive_host_match(self):
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='MyHost' dbname='db' table='t'",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        orig = self._patch_qgis(
            {"host": "MyHost", "port": "5432", "database": "db"},
            {"Conn1": {
                "host": "myhost", "port": "5432", "database": "db",
                "authcfg": "ABC",
            }},
        )
        try:
            result = _find_matching_saved_connection(info)
            self.assertIsNotNone(result)
            self.assertEqual(result["authcfg"], "ABC")
        finally:
            self._unpatch_qgis(orig)


# ---------------------------------------------------------------------------
# create_layer_from_registry for DB layer (integration mock)
# ---------------------------------------------------------------------------
class TestCreateLayerFromRegistryDb(unittest.TestCase):
    def test_db_layer_no_credentials_returns_none(self):
        info = DatasourceInfo(
            fingerprint="postgres::test",
            provider_type="postgres",
            source_uri="host='h' dbname='d' table='t' password=***",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        import qgis.core as qc
        orig = qc.QgsDataSourceUri
        qc.QgsDataSourceUri = _make_mock_uri_factory()
        try:
            with patch.object(
                sys.modules['recoverland.core.datasource_registry'],
                '_find_matching_saved_connection', return_value=None,
            ):
                result = create_layer_from_registry(info)
                self.assertIsNone(result)
        finally:
            qc.QgsDataSourceUri = orig

    def test_ogr_layer_passes_uri_directly(self):
        info = DatasourceInfo(
            fingerprint="ogr::test.gpkg",
            provider_type="ogr",
            source_uri="/data/test.gpkg",
            layer_name="test",
            authcfg="",
            crs_authid="EPSG:4326",
            geometry_type="Point",
        )
        mock_layer = MagicMock()
        mock_layer.isValid.return_value = True

        import qgis.core as qc
        orig = qc.QgsVectorLayer
        qc.QgsVectorLayer = MagicMock(return_value=mock_layer)
        try:
            result = create_layer_from_registry(info)
            qc.QgsVectorLayer.assert_called_once_with(
                "/data/test.gpkg", "test (restore)", "ogr",
            )
            self.assertIsNotNone(result)
        finally:
            qc.QgsVectorLayer = orig


# ---------------------------------------------------------------------------
# Password stripping for DB URIs
# ---------------------------------------------------------------------------
class TestPasswordStrippingDb(unittest.TestCase):
    def test_mssql_password_stripped(self):
        uri = "host='h' password='s3cret' dbname='d' table='t'"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("s3cret", result)

    def test_oracle_password_stripped(self):
        uri = "host='h' password='orapass' dbname='d' table='t'"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("orapass", result)


if __name__ == '__main__':
    unittest.main()
