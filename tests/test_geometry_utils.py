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


if __name__ == '__main__':
    unittest.main()
