"""Tests for core.geometry_utils module."""
import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.geometry_utils import (
    geometries_equal,
    extract_geometry_type,
    extract_crs_authid,
)


class TestGeometriesEqual(unittest.TestCase):
    def test_both_none(self):
        self.assertTrue(geometries_equal(None, None))

    def test_one_none(self):
        self.assertFalse(geometries_equal(b'\x01', None))
        self.assertFalse(geometries_equal(None, b'\x01'))

    def test_equal_bytes(self):
        wkb = b'\x01\x02\x03'
        self.assertTrue(geometries_equal(wkb, wkb))

    def test_different_bytes(self):
        self.assertFalse(geometries_equal(b'\x01', b'\x02'))


class _FakeWkbTypes:
    NoGeometry = 100
    @staticmethod
    def displayString(wkb_type):
        if wkb_type == 100:
            return "NoGeometry"
        return "Point"


class _FakeCrs:
    def __init__(self, valid=True, authid="EPSG:4326"):
        self._valid = valid
        self._authid = authid
    def isValid(self):
        return self._valid
    def authid(self):
        return self._authid


class _FakeLayer:
    def __init__(self, wkb_type=1, crs=None):
        self._wkb_type = wkb_type
        self._crs = crs or _FakeCrs()
    def wkbType(self):
        return self._wkb_type
    def crs(self):
        return self._crs


class TestExtractGeometryType(unittest.TestCase):
    def test_no_geometry(self):
        layer = _FakeLayer(wkb_type=100)
        self.assertEqual(extract_geometry_type(layer), "NoGeometry")

    def test_point_geometry(self):
        layer = _FakeLayer(wkb_type=1)
        result = extract_geometry_type(layer)
        self.assertIsInstance(result, str)


class TestExtractCrsAuthid(unittest.TestCase):
    def test_no_geometry_returns_none(self):
        layer = _FakeLayer(wkb_type=100)
        self.assertIsNone(extract_crs_authid(layer))

    def test_valid_crs(self):
        layer = _FakeLayer(wkb_type=1, crs=_FakeCrs(True, "EPSG:2154"))
        result = extract_crs_authid(layer)
        self.assertEqual(result, "EPSG:2154")

    def test_invalid_crs_returns_none(self):
        layer = _FakeLayer(wkb_type=1, crs=_FakeCrs(False, ""))
        self.assertIsNone(extract_crs_authid(layer))

    def test_none_crs_returns_none(self):
        class _NoCrsLayer(_FakeLayer):
            def crs(self):
                return None
        layer = _NoCrsLayer(wkb_type=1)
        self.assertIsNone(extract_crs_authid(layer))


class TestRebuildGeometry(unittest.TestCase):
    def test_none_input(self):
        from recoverland.core.geometry_utils import rebuild_geometry
        self.assertIsNone(rebuild_geometry(None))

    def test_empty_bytes_input(self):
        from recoverland.core.geometry_utils import rebuild_geometry
        self.assertIsNone(rebuild_geometry(b''))

    def test_valid_wkb_returns_geometry(self):
        from recoverland.core.geometry_utils import rebuild_geometry
        wkb = b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@'
        result = rebuild_geometry(wkb)
        self.assertIsNotNone(result)

    def test_single_byte_wkb(self):
        from recoverland.core.geometry_utils import rebuild_geometry
        result = rebuild_geometry(b'\x01')
        self.assertIsNotNone(result)


class TestExtractGeometryWkb(unittest.TestCase):
    def test_null_geometry_returns_none(self):
        from recoverland.core.geometry_utils import extract_geometry_wkb

        class _FeatNullGeom:
            def geometry(self):
                class _Geom:
                    def isNull(self):
                        return True
                    def isEmpty(self):
                        return False
                return _Geom()

        self.assertIsNone(extract_geometry_wkb(_FeatNullGeom()))

    def test_none_geometry_returns_none(self):
        from recoverland.core.geometry_utils import extract_geometry_wkb

        class _FeatNoGeom:
            def geometry(self):
                return None

        self.assertIsNone(extract_geometry_wkb(_FeatNoGeom()))

    def test_empty_geometry_returns_none(self):
        from recoverland.core.geometry_utils import extract_geometry_wkb

        class _FeatEmptyGeom:
            def geometry(self):
                class _Geom:
                    def isNull(self):
                        return False
                    def isEmpty(self):
                        return True
                return _Geom()

        self.assertIsNone(extract_geometry_wkb(_FeatEmptyGeom()))

    def test_valid_geometry_returns_bytes(self):
        from recoverland.core.geometry_utils import extract_geometry_wkb

        class _FeatWithGeom:
            def geometry(self):
                class _Geom:
                    def isNull(self):
                        return False
                    def isEmpty(self):
                        return False
                    def asWkb(self):
                        return b'\x01\x02\x03'
                return _Geom()

        result = extract_geometry_wkb(_FeatWithGeom())
        self.assertEqual(result, b'\x01\x02\x03')


class TestGeometriesEqualEdgeCases(unittest.TestCase):
    def test_empty_bytes_are_equal(self):
        self.assertTrue(geometries_equal(b'', b''))

    def test_empty_vs_none_not_equal(self):
        self.assertFalse(geometries_equal(b'', None))

    def test_large_identical_wkb(self):
        wkb = b'\xAB' * 100_000
        self.assertTrue(geometries_equal(wkb, wkb))

    def test_large_different_wkb(self):
        a = b'\xAB' * 100_000
        b = b'\xAB' * 99_999 + b'\xCD'
        self.assertFalse(geometries_equal(a, b))

    def test_same_reference_is_equal(self):
        wkb = b'\x01\x02'
        self.assertTrue(geometries_equal(wkb, wkb))


class TestCaptureGeometryInfo(unittest.TestCase):
    def test_no_geometry_layer(self):
        from recoverland.core.geometry_utils import capture_geometry_info

        class _FeatNoGeom:
            def geometry(self):
                return None

        layer = _FakeLayer(wkb_type=100)
        wkb, gtype, crs = capture_geometry_info(layer, _FeatNoGeom())
        self.assertIsNone(wkb)
        self.assertEqual(gtype, "NoGeometry")
        self.assertIsNone(crs)

    def test_spatial_layer_with_geometry(self):
        from recoverland.core.geometry_utils import capture_geometry_info

        class _FeatWithGeom:
            def geometry(self):
                class _Geom:
                    def isNull(self):
                        return False
                    def isEmpty(self):
                        return False
                    def asWkb(self):
                        return b'\x01\x02\x03'
                return _Geom()

        layer = _FakeLayer(wkb_type=1, crs=_FakeCrs(True, "EPSG:4326"))
        wkb, gtype, crs = capture_geometry_info(layer, _FeatWithGeom())
        self.assertEqual(wkb, b'\x01\x02\x03')
        self.assertIsInstance(gtype, str)
        self.assertEqual(crs, "EPSG:4326")


if __name__ == '__main__':
    unittest.main()
