"""Geometry conversion utilities for RecoverLand (RLU-028).

Handles WKB serialization/deserialization of QgsGeometry objects.
Stores geometry as binary WKB in SQLite, with CRS tracked separately.
Tables without geometry get geometry_wkb=NULL, geometry_type='NoGeometry'.
"""
from typing import Optional, Tuple


def geometry_to_wkb(geom) -> Optional[bytes]:
    """Convert a QgsGeometry to WKB bytes, or None when absent/empty.

    Centralises the ``geom is not None and not geom.isNull() and
    not geom.isEmpty()`` guard followed by ``bytes(geom.asWkb())``
    that recurs in edit_tracker every time the buffer is mirrored to
    an audit event (DUP-08).
    """
    if not is_geometry_present(geom):
        return None
    return bytes(geom.asWkb())


def extract_geometry_wkb(feature) -> Optional[bytes]:
    """Extract WKB bytes from a QgsFeature's geometry.

    Returns None if the feature has no geometry or geometry is empty/null.
    """
    return geometry_to_wkb(feature.geometry())


def extract_geometry_type(layer) -> str:
    """Get the geometry type string for a layer.

    Returns 'NoGeometry' for non-spatial layers across all QGIS versions:
    - QGIS 3.30+ : Qgis.WkbType.NoGeometry (scoped enum)
    - QGIS 3.22 - 3.28 : QgsWkbTypes.NoGeometry (short form)
    - Display string via Qgis.WkbType(...).name (3.30+) or QgsWkbTypes.displayString (3.x).
    """
    try:
        from qgis.core import Qgis
        wkb_type = layer.wkbType()

        if hasattr(Qgis, 'WkbType'):
            if wkb_type == Qgis.WkbType.NoGeometry:
                return "NoGeometry"
            try:
                return Qgis.WkbType(wkb_type).name if isinstance(wkb_type, int) else str(wkb_type)
            except (ValueError, AttributeError):
                pass

        try:
            from qgis.core import QgsWkbTypes
            if hasattr(QgsWkbTypes, 'NoGeometry') and wkb_type == QgsWkbTypes.NoGeometry:
                return "NoGeometry"
            if hasattr(QgsWkbTypes, 'displayString'):
                return QgsWkbTypes.displayString(wkb_type)
        except ImportError:
            pass

        if hasattr(Qgis, 'displayString'):
            return Qgis.displayString(wkb_type)
        return str(wkb_type)
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


def is_geometry_present(geom) -> bool:
    """Return True iff a QgsGeometry is present, non-null and non-empty.

    Mirrors the ``geom is None or geom.isNull() or geom.isEmpty()``
    check that recurs throughout edit_tracker, restore_service and
    restore_executor (DUP-08). Centralised here so the predicate
    stays consistent if QGIS introduces a fourth invalid state.
    """
    if geom is None:
        return False
    try:
        if geom.isNull() or geom.isEmpty():
            return False
    except (AttributeError, RuntimeError):
        # geom does not implement the QgsGeometry interface (mock/stub).
        return False
    return True


def feature_matches_geometry(feature, expected_geom) -> bool:
    """Return True when *feature*'s geometry matches *expected_geom*.

    Two-step comparison shared between the snapshot scanners in
    restore_service and the buffer ops in restore_executor (DUP-10):
      1. Byte-for-byte WKB equality (fast path; the audit pipeline
         re-serialises geometries the same way QGIS does).
      2. ``QgsGeometry.equals`` fallback for the cases where two valid
         WKB encodings represent the same shape (e.g. ring orientation,
         redundant Z coordinate).
    """
    if not is_geometry_present(expected_geom):
        return False
    current = feature.geometry()
    if not is_geometry_present(current):
        return False
    if geometries_equal(bytes(current.asWkb()), bytes(expected_geom.asWkb())):
        return True
    if hasattr(current, "equals"):
        return bool(current.equals(expected_geom))
    return False


def get_feature_source(layer):
    """Return a callable producing features for *layer*.

    Some buffer-aware code paths need ``layer.getFeatures`` so the
    in-progress edit buffer is honoured; others use the data provider
    directly when only the persisted state is relevant. This helper
    centralises the historical defensive lookup
    (``layer.getFeatures if hasattr(layer, 'getFeatures') else
    layer.dataProvider().getFeatures``) used in 6+ sites (DUP-09).

    Defensive against test mocks that expose only ``dataProvider``.
    """
    if hasattr(layer, "getFeatures"):
        return layer.getFeatures
    return layer.dataProvider().getFeatures


def wkb_short_repr(wkb_data: Optional[bytes]) -> str:
    """Return a short human-readable representation of a WKB blob.

    Used for diagnostic logging. Output looks like:
      None                 if wkb is None
      'empty'              if wkb is empty/invalid
      'POINT(x.xxx y.yyy)' for points
      'CENTROID(x.xxx y.yyy)+nbpts=N len=L' for other geometries
    """
    if wkb_data is None:
        return "None"
    if len(wkb_data) == 0:
        return "empty"
    try:
        geom = rebuild_geometry(wkb_data)
        if not is_geometry_present(geom):
            return f"empty(len={len(wkb_data)})"
        try:
            if geom.type() == 0:  # Point
                p = geom.asPoint()
                return f"POINT({p.x():.3f} {p.y():.3f})"
        except Exception:
            pass
        try:
            c = geom.centroid().asPoint()
            return (f"CENTROID({c.x():.3f} {c.y():.3f})+"
                    f"len={len(wkb_data)}")
        except Exception:
            return f"WKB(len={len(wkb_data)})"
    except Exception as exc:
        return f"unparseable(len={len(wkb_data)},err={exc})"


def feature_geom_short_repr(layer, fid: int) -> str:
    """Read the current geometry of a feature directly from the provider
    and return a short human-readable representation.

    Returns 'absent' if the feature is not present (already deleted).
    Used to verify *what is actually persisted* after a commit, instead
    of trusting the buffer or post-commit signals.
    """
    try:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(int(fid))
        provider = layer.dataProvider()
        feature = next(provider.getFeatures(request), None)
        if feature is None:
            return "absent"
        geom = feature.geometry()
        if not is_geometry_present(geom):
            return "no_geom"
        return wkb_short_repr(bytes(geom.asWkb()))
    except Exception as exc:
        return f"lookup_err({exc})"


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
