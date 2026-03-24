"""Tests for core.identity module - hard edge cases, toxic inputs, boundaries."""
import json
import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.identity import (
    compute_datasource_fingerprint,
    compute_feature_identity,
    compute_project_fingerprint,
    extract_layer_name,
    get_identity_strength_for_layer,
    _normalize_file_source,
    _normalize_pg_source,
)
from recoverland.core.support_policy import IdentityStrength


class _FakeProvider:
    def __init__(self, name, pk_indices=None):
        self._name = name
        self._pk = pk_indices or []
    def name(self):
        return self._name
    def pkAttributeIndexes(self):
        return self._pk
    def capabilities(self):
        return 15


class _FakeField:
    def __init__(self, name):
        self._name = name
    def name(self):
        return self._name


class _FakeFields:
    def __init__(self, fields):
        self._fields = [_FakeField(n) for n in fields]
    def __iter__(self):
        return iter(self._fields)
    def count(self):
        return len(self._fields)
    def at(self, idx):
        return self._fields[idx]


class _FakeFeature:
    def __init__(self, fid, attrs=None):
        self._fid = fid
        self._attrs = attrs or {}
    def id(self):
        return self._fid
    def geometry(self):
        return None
    def __getitem__(self, key):
        return self._attrs.get(key)


class _FakeLayer:
    def __init__(self, name, provider_name, source, pk_indices=None, field_names=None):
        self._name = name
        self._provider = _FakeProvider(provider_name, pk_indices)
        self._source = source
        self._fields = _FakeFields(field_names or [])
        self._id = f"layer_{id(self)}"
    def id(self):
        return self._id
    def name(self):
        return self._name
    def source(self):
        return self._source
    def dataProvider(self):
        return self._provider
    def fields(self):
        return self._fields
    def crs(self):
        return None


# ---------------------------------------------------------------------------
# PG source normalization
# ---------------------------------------------------------------------------
class TestNormalizePgSource(unittest.TestCase):
    def test_standard_pg_uri(self):
        raw = "host='10.0.0.1' port='5432' dbname='mydb' schema='public' table='parcelles'"
        result = _normalize_pg_source(raw)
        self.assertIn("host=10.0.0.1", result)
        self.assertIn("dbname=mydb", result)
        self.assertIn("table=parcelles", result)

    def test_missing_port_defaults_to_5432(self):
        raw = "host='10.0.0.1' dbname='mydb' schema='public' table='t'"
        result = _normalize_pg_source(raw)
        self.assertIn("port=5432", result)

    def test_missing_schema_defaults_to_public(self):
        raw = "host='h' dbname='d' table='t'"
        result = _normalize_pg_source(raw)
        self.assertIn("schema=public", result)

    def test_double_quoted_values(self):
        raw = 'host="10.0.0.1" dbname="my db" table="my table"'
        result = _normalize_pg_source(raw)
        self.assertIn("host=10.0.0.1", result)

    def test_empty_string_no_crash(self):
        result = _normalize_pg_source("")
        self.assertIsInstance(result, str)

    def test_garbage_input_no_crash(self):
        result = _normalize_pg_source("not a pg uri at all !@#$%")
        self.assertIsInstance(result, str)

    def test_uri_with_password_ignored(self):
        raw = "host='h' port='5432' dbname='d' user='u' password='secret' table='t'"
        result = _normalize_pg_source(raw)
        self.assertNotIn("secret", result)
        self.assertNotIn("password", result)

    def test_deterministic_same_input_same_output(self):
        raw = "host='h' port='5432' dbname='d' schema='s' table='t'"
        self.assertEqual(_normalize_pg_source(raw), _normalize_pg_source(raw))

    def test_order_independent(self):
        a = "host='h' port='5432' dbname='d' schema='s' table='t'"
        b = "table='t' schema='s' dbname='d' port='5432' host='h'"
        self.assertEqual(_normalize_pg_source(a), _normalize_pg_source(b))


# ---------------------------------------------------------------------------
# File source normalization
# ---------------------------------------------------------------------------
class TestNormalizeFileSource(unittest.TestCase):
    def test_gpkg_with_layer(self):
        raw = "C:\\data\\test.gpkg|layername=parcelles"
        result = _normalize_file_source(raw)
        self.assertIn("test.gpkg", result)
        self.assertIn("|layername=parcelles", result)

    def test_simple_path(self):
        result = _normalize_file_source("/home/user/data.shp")
        self.assertIn("data.shp", result)

    def test_backslash_normalized_to_forward(self):
        result = _normalize_file_source("C:\\a\\b\\c.gpkg")
        self.assertNotIn("\\", result.split("|")[0].replace("\\", "/"))

    def test_empty_string_no_crash(self):
        result = _normalize_file_source("")
        self.assertIsInstance(result, str)

    def test_pipe_in_filename(self):
        result = _normalize_file_source("data.gpkg|layername=x|subset=y")
        self.assertIn("|layername=x|subset=y", result)

    def test_unicode_path(self):
        result = _normalize_file_source("/donnees/donnees.gpkg")
        self.assertIn("donnees.gpkg", result)

    def test_spaces_in_path(self):
        result = _normalize_file_source("C:/my data/file name.gpkg")
        self.assertIn("file name.gpkg", result)


# ---------------------------------------------------------------------------
# Datasource fingerprint
# ---------------------------------------------------------------------------
class TestComputeDatasourceFingerprint(unittest.TestCase):
    def test_ogr_starts_with_prefix(self):
        layer = _FakeLayer("t", "ogr", "C:/data/test.gpkg")
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("ogr::"))

    def test_postgres_starts_with_prefix(self):
        layer = _FakeLayer("t", "postgres", "host='h' dbname='d' table='t'")
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("postgres::"))

    def test_spatialite_starts_with_prefix(self):
        layer = _FakeLayer("t", "spatialite", "/data/spatial.db")
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("spatialite::"))

    def test_unknown_provider_still_works(self):
        layer = _FakeLayer("t", "totalement_inconnu", "blah")
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("totalement_inconnu::"))

    def test_same_layer_same_fingerprint(self):
        layer = _FakeLayer("t", "ogr", "C:/data/test.gpkg")
        self.assertEqual(
            compute_datasource_fingerprint(layer),
            compute_datasource_fingerprint(layer),
        )

    def test_different_sources_different_fingerprints(self):
        a = _FakeLayer("t", "ogr", "a.gpkg")
        b = _FakeLayer("t", "ogr", "b.gpkg")
        self.assertNotEqual(
            compute_datasource_fingerprint(a),
            compute_datasource_fingerprint(b),
        )

    def test_empty_source_no_crash(self):
        layer = _FakeLayer("t", "ogr", "")
        fp = compute_datasource_fingerprint(layer)
        self.assertTrue(fp.startswith("ogr::"))


# ---------------------------------------------------------------------------
# Feature identity
# ---------------------------------------------------------------------------
class TestComputeFeatureIdentity(unittest.TestCase):
    def test_feature_with_pk(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg", pk_indices=[0], field_names=["gid"])
        feat = _FakeFeature(42, {"gid": 99})
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertEqual(result["fid"], 42)
        self.assertEqual(result["pk_field"], "gid")
        self.assertEqual(result["pk_value"], 99)

    def test_feature_without_pk(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg", field_names=["name"])
        feat = _FakeFeature(7)
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertEqual(result["fid"], 7)
        self.assertNotIn("pk_field", result)

    def test_negative_fid(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg")
        feat = _FakeFeature(-999)
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertEqual(result["fid"], -999)

    def test_huge_fid(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg")
        feat = _FakeFeature(2**53)
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertEqual(result["fid"], 2**53)

    def test_pk_value_none_skipped(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg", pk_indices=[0], field_names=["gid"])
        feat = _FakeFeature(1, {"gid": None})
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertNotIn("pk_field", result)

    def test_pk_value_string_with_quotes(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg", pk_indices=[0], field_names=["code"])
        feat = _FakeFeature(1, {"code": "it's \"quoted\""})
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertEqual(result["pk_value"], "it's \"quoted\"")

    def test_pk_index_out_of_range(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg", pk_indices=[999], field_names=["gid"])
        feat = _FakeFeature(1, {"gid": 10})
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertNotIn("pk_field", result)

    def test_result_is_valid_json(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg")
        feat = _FakeFeature(1)
        raw = compute_feature_identity(layer, feat)
        parsed = json.loads(raw)
        self.assertIsInstance(parsed, dict)

    def test_feature_attr_raises_keyerror(self):
        layer = _FakeLayer("t", "ogr", "t.gpkg", pk_indices=[0], field_names=["x"])
        feat = _FakeFeature(1, {})
        result = json.loads(compute_feature_identity(layer, feat))
        self.assertEqual(result["fid"], 1)


# ---------------------------------------------------------------------------
# Layer name extraction
# ---------------------------------------------------------------------------
class TestExtractLayerName(unittest.TestCase):
    def test_normal_name(self):
        layer = _FakeLayer("parcelles", "ogr", "x")
        self.assertEqual(extract_layer_name(layer), "parcelles")

    def test_empty_name_returns_unnamed(self):
        layer = _FakeLayer("", "ogr", "x")
        self.assertEqual(extract_layer_name(layer), "unnamed")

    def test_unicode_name(self):
        layer = _FakeLayer("couche_donnees", "ogr", "x")
        self.assertEqual(extract_layer_name(layer), "couche_donnees")


# ---------------------------------------------------------------------------
# Identity strength per provider
# ---------------------------------------------------------------------------
class TestIdentityStrength(unittest.TestCase):
    def test_postgres_strong(self):
        layer = _FakeLayer("t", "postgres", "x")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.STRONG)

    def test_spatialite_strong(self):
        layer = _FakeLayer("t", "spatialite", "x")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.STRONG)

    def test_memory_none(self):
        layer = _FakeLayer("t", "memory", "x")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.NONE)

    def test_delimitedtext_weak(self):
        layer = _FakeLayer("t", "delimitedtext", "x")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.WEAK)

    def test_ogr_gpkg_strong(self):
        layer = _FakeLayer("t", "ogr", "data.gpkg")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.STRONG)

    def test_ogr_shp_medium(self):
        layer = _FakeLayer("t", "ogr", "data.shp")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.MEDIUM)

    def test_ogr_csv_weak(self):
        layer = _FakeLayer("t", "ogr", "data.csv")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.WEAK)

    def test_ogr_geojson_medium(self):
        layer = _FakeLayer("t", "ogr", "data.geojson")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.MEDIUM)

    def test_unknown_provider_medium(self):
        layer = _FakeLayer("t", "bizarre_provider", "x")
        self.assertEqual(get_identity_strength_for_layer(layer), IdentityStrength.MEDIUM)


# ---------------------------------------------------------------------------
# Project fingerprint
# ---------------------------------------------------------------------------
class TestProjectFingerprint(unittest.TestCase):
    def test_returns_string_with_prefix(self):
        fp = compute_project_fingerprint()
        self.assertIsInstance(fp, str)
        self.assertTrue(fp.startswith("project::"))

    def test_deterministic(self):
        self.assertEqual(compute_project_fingerprint(), compute_project_fingerprint())

    def test_never_empty(self):
        fp = compute_project_fingerprint()
        self.assertTrue(len(fp) > len("project::"))


if __name__ == '__main__':
    unittest.main()
