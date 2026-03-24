"""Tests for core.support_policy module (RLU-001)."""
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.support_policy import (
    get_provider_policy, refine_ogr_identity,
    IdentityStrength, SupportLevel, format_support_message,
)


class TestGetProviderPolicy(unittest.TestCase):

    def test_postgres_fully_supported(self):
        p = get_provider_policy("postgres")
        self.assertEqual(p.support_level, SupportLevel.FULL)
        self.assertTrue(p.capture)
        self.assertTrue(p.restore)

    def test_ogr_fully_supported(self):
        p = get_provider_policy("ogr")
        self.assertEqual(p.support_level, SupportLevel.FULL)

    def test_spatialite_fully_supported(self):
        p = get_provider_policy("spatialite")
        self.assertEqual(p.support_level, SupportLevel.FULL)
        self.assertEqual(p.identity_strength, IdentityStrength.STRONG)

    def test_memory_informational(self):
        p = get_provider_policy("memory")
        self.assertEqual(p.support_level, SupportLevel.INFORMATIONAL)
        self.assertTrue(p.capture)
        self.assertFalse(p.restore)

    def test_virtual_refused(self):
        p = get_provider_policy("virtual")
        self.assertEqual(p.support_level, SupportLevel.REFUSED)
        self.assertFalse(p.capture)
        self.assertFalse(p.restore)

    def test_unknown_refused(self):
        p = get_provider_policy("nonexistent_provider")
        self.assertEqual(p.support_level, SupportLevel.REFUSED)

    def test_wfs_partial(self):
        p = get_provider_policy("wfs")
        self.assertEqual(p.support_level, SupportLevel.PARTIAL)


class TestRefineOgrIdentity(unittest.TestCase):

    def test_gpkg_strong(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/mydata.gpkg|layername=parcelles"),
            IdentityStrength.STRONG,
        )

    def test_shapefile_medium(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/parcelles.shp"),
            IdentityStrength.MEDIUM,
        )

    def test_csv_weak(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/import.csv"),
            IdentityStrength.WEAK,
        )

    def test_xlsx_weak(self):
        self.assertEqual(
            refine_ogr_identity("/tmp/data.xlsx"),
            IdentityStrength.WEAK,
        )

    def test_geojson_medium(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/zones.geojson"),
            IdentityStrength.MEDIUM,
        )

    def test_flatgeobuf_strong(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/lines.fgb"),
            IdentityStrength.STRONG,
        )

    def test_sqlite_strong(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/local.sqlite"),
            IdentityStrength.STRONG,
        )

    def test_kml_weak(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/markers.kml"),
            IdentityStrength.WEAK,
        )

    def test_dbf_medium(self):
        self.assertEqual(
            refine_ogr_identity("C:/data/attributes.dbf"),
            IdentityStrength.MEDIUM,
        )


class TestFormatSupportMessage(unittest.TestCase):

    def test_full_message(self):
        p = get_provider_policy("postgres")
        msg = format_support_message(p)
        self.assertIn("Fully supported", msg)

    def test_refused_message(self):
        p = get_provider_policy("virtual")
        msg = format_support_message(p)
        self.assertIn("Not supported", msg)

    def test_partial_message(self):
        p = get_provider_policy("wfs")
        msg = format_support_message(p)
        self.assertIn("Partially supported", msg)


if __name__ == '__main__':
    unittest.main()
