"""Geometry conversion utilities for RecoverLand (RLU-028).

Handles WKB serialization/deserialization of QgsGeometry objects.
Stores geometry as binary WKB in SQLite, with CRS tracked separately.
Tables without geometry get geometry_wkb=NULL, geometry_type='NoGeometry'.
"""
from typing import Optional, Tuple


def extract_geometry_wkb(feature) -> Optional[bytes]:
    """Extract WKB bytes from a QgsFeature's geometry.

    Returns None if the feature has no geometry or geometry is empty/null.
    """
    geom = feature.geometry()
    if geom is None or geom.isNull() or geom.isEmpty():
        return None
    return bytes(geom.asWkb())


def extract_geometry_type(layer) -> str:
    """Get the geometry type string for a layer.

    Returns 'NoGeometry' for non-spatial layers.
    """
    try:
        from qgis.core import QgsWkbTypes
        wkb_type = layer.wkbType()
        if wkb_type == QgsWkbTypes.NoGeometry:
            return "NoGeometry"
        return QgsWkbTypes.displayString(wkb_type)
    except Exception:
        return "Unknown"


def extract_crs_authid(layer) -> Optional[str]:
    """Get CRS auth ID (e.g. 'EPSG:4326') for a layer.

    Returns None for non-spatial layers.
    """
    if extract_geometry_type(layer) == "NoGeometry":
        return None
    crs = layer.crs()
    if crs is None or not crs.isValid():
        return None
    return crs.authid()


def rebuild_geometry(wkb_data: Optional[bytes]):
    """Reconstruct a QgsGeometry from WKB bytes.

    Returns None if wkb_data is None or empty.
    """
    if wkb_data is None or len(wkb_data) == 0:
        return None
    from qgis.core import QgsGeometry
    geom = QgsGeometry()
    geom.fromWkb(wkb_data)
    return geom


def geometries_equal(wkb_a: Optional[bytes], wkb_b: Optional[bytes]) -> bool:
    """Compare two WKB geometries for equality."""
    if wkb_a is None and wkb_b is None:
        return True
    if wkb_a is None or wkb_b is None:
        return False
    return wkb_a == wkb_b


def capture_geometry_info(layer, feature) -> Tuple[Optional[bytes], str, Optional[str]]:
    """Capture geometry WKB, type, and CRS from a layer+feature.

    Returns (wkb_bytes, geometry_type_str, crs_authid).
    For non-spatial layers: (None, 'NoGeometry', None).
    """
    geom_type = extract_geometry_type(layer)
    if geom_type == "NoGeometry":
        return None, "NoGeometry", None
    wkb = extract_geometry_wkb(feature)
    crs = extract_crs_authid(layer)
    return wkb, geom_type, crs
