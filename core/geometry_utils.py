"""Geometry conversion utilities for RecoverLand (RLU-028).

Handles WKB serialization/deserialization of QgsGeometry objects.
Stores geometry as binary WKB in SQLite, with CRS tracked separately.
Tables without geometry get geometry_wkb=NULL, geometry_type='NoGeometry'.
"""
import hashlib
from typing import Optional, Tuple


def _compute_makevalid_drift(geom_before, geom_after) -> Tuple[str, str, float]:
    """Quantify the change between two QgsGeometry objects.

    Returns a tuple `(wkb_hash_before, wkb_hash_after, drift_units)` where:
      - `wkb_hash_*` are 8-hex-char SHA-256 prefixes of `bytes(geom.asWkb())`.
        They equal each other if and only if the WKB byte sequences are
        identical.
      - `drift_units` is the L_infinity (Chebyshev) distance between the
        two bounding boxes expressed in the geometry CRS units:
            max(|xmin_a - xmin_b|, |ymin_a - ymin_b|,
                |xmax_a - xmax_b|, |ymax_a - ymax_b|)
        A drift of 0 means the bboxes coincide exactly. Identical inputs
        always produce drift=0, but drift=0 does not imply identical WKB
        (two distinct shapes can share a bbox). Treat as a fast,
        conservative coarse-grained drift metric, not a full geometric
        equality test.

    Used by `core/restore_executor.py:_buffer_update` (BL-RW-P1-08,
    CR-8) to decide whether the result of `QgsGeometry.makeValid()` is
    close enough to the original to be safely applied, or whether the
    drift exceeds `MAKEVALID_DRIFT_TOLERANCE` and the apply must be
    skipped with status `SKIPPED_GEOMETRY_DRIFT`.

    Defensive: if either input is None / empty / lacks asWkb(), the
    function falls back to placeholder hashes and an infinite drift so
    that callers treat the comparison as "definitely drifted".
    """
    def _wkb_hash(g):
        try:
            if g is None or g.isNull() or g.isEmpty():
                return "00000000"
            return hashlib.sha256(bytes(g.asWkb())).hexdigest()[:8]
        except Exception:
            return "00000000"

    hash_before = _wkb_hash(geom_before)
    hash_after = _wkb_hash(geom_after)

    try:
        bb_b = geom_before.boundingBox()
        bb_a = geom_after.boundingBox()
        drift = max(
            abs(bb_a.xMinimum() - bb_b.xMinimum()),
            abs(bb_a.yMinimum() - bb_b.yMinimum()),
            abs(bb_a.xMaximum() - bb_b.xMaximum()),
            abs(bb_a.yMaximum() - bb_b.yMaximum()),
        )
    except Exception:
        drift = float("inf")

    return hash_before, hash_after, drift


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


def repair_geometry_for_render(wkb_data: Optional[bytes], trace_id: str = ""):
    """Reconstruct a QgsGeometry from WKB and repair if GEOS-invalid.

    Used by Time Lens (BL-IL-P0-07, cause racine CR-IL-4) before adding
    audit geometries to a memory overlay layer. Audit events may store
    geometries that were valid at capture time but are GEOS-invalid
    today (mixed providers, makeValid() not yet applied, manual file
    edits). Rendering such geometries either fails silently or paints
    self-intersecting noise on the canvas.

    Pattern: rebuild_geometry then makeValid if needed. Same antecedent
    as `restore_executor._buffer_update` (RW-19a / BL-RW-P1-08) which
    repairs on the write side; this helper repairs on the render side.

    Returns:
        QgsGeometry: a non-empty geometry safe to render. Either the
            original (when isGeosValid()) or its makeValid() result.
        None: when *wkb_data* is None/empty/corrupted, when makeValid()
            yields an empty result, or when makeValid() raises.

    Emits log signatures (structured key=value) for traceability:
        flog: lens_geom_repair event=repaired trace_id=<id> drift_units=<f>
        flog: lens_geom_repair event=repair_yielded_empty trace_id=<id>
        flog: lens_geom_repair event=repair_exception trace_id=<id> type=<X>
    """
    geom = rebuild_geometry(wkb_data)
    if not is_geometry_present(geom):
        return None

    try:
        if geom.isGeosValid():
            return geom
    except (AttributeError, RuntimeError):
        # geom does not implement the predicate (mock/stub). Fall through.
        pass

    # Lazy import to keep the module importable without QGIS for static
    # checks (cf. il7 structural assertion).
    from .logger import flog  # noqa: PLC0415

    try:
        repaired = geom.makeValid()
    except Exception as exc:  # noqa: BLE001 - QGIS raises various types
        flog(
            f"lens_geom_repair event=repair_exception trace_id={trace_id} "
            f"type={type(exc).__name__}",
            "WARNING",
        )
        return None

    if not is_geometry_present(repaired):
        flog(
            f"lens_geom_repair event=repair_yielded_empty trace_id={trace_id}",
            "WARNING",
        )
        return None

    try:
        _hash_b, _hash_a, drift = _compute_makevalid_drift(geom, repaired)
    except Exception:  # noqa: BLE001 - drift is diagnostic only
        drift = float("nan")
    flog(
        f"lens_geom_repair event=repaired trace_id={trace_id} "
        f"drift_units={drift:.6f}",
        "INFO",
    )
    return repaired


def reproject_geometry_for_render(
    geom,
    src_crs_authid: Optional[str],
    dst_crs_authid: str,
    transform_cache: dict,
    trace_id: str = "",
):
    """Reproject a QgsGeometry from src CRS to dst CRS for rendering.

    Used by Time Lens (BL-IL-P0-06, cause racine CR-IL-3) before adding
    audit geometries to the canvas overlay. Audit events are captured
    in the layer source CRS, but the canvas may render in a different
    CRS. Without on-the-fly reprojection, an event captured in EPSG:2154
    and shown on an EPSG:3857 canvas appears thousands of kilometres
    off-target.

    Args:
        geom: QgsGeometry to reproject (assumed non-None, non-empty;
            callers should pre-filter via is_geometry_present).
        src_crs_authid: CRS authority id of *geom* (e.g. "EPSG:2154").
            None or invalid triggers a skip with warning.
        dst_crs_authid: CRS authority id of the target canvas (e.g.
            "EPSG:3857"). Must be valid; fail-fast on None/invalid.
        transform_cache: dict keyed by ``(src_authid, dst_authid)``,
            holding QgsCoordinateTransform instances. Mutated in place:
            entries are added on cache miss. The caller owns the cache
            lifecycle (invalidate on QgsProject.crsChanged).
        trace_id: opaque correlation id propagated in log signatures.

    Returns:
        QgsGeometry: a reprojected copy of *geom* if the transform
            succeeds.
        None: when *src_crs_authid* is None/invalid, when the
            destination CRS cannot be resolved, when the transform
            instantiation fails (CRS not installed locally), or when
            the actual reprojection raises a QGIS exception.

    Emits log signatures (structured key=value) for traceability:
        flog: lens_geom_reproject event=reprojected trace_id=<id>
              src_crs=<a> dst_crs=<b> cache_hit=<bool>
        flog: lens_geom_reproject event=skipped trace_id=<id>
              reason=src_crs_none|src_crs_invalid|dst_crs_invalid|transform_failed
              src_crs=<a> dst_crs=<b>
    """
    from .logger import flog  # noqa: PLC0415

    if src_crs_authid is None or not src_crs_authid:
        flog(
            f"lens_geom_reproject event=skipped trace_id={trace_id} "
            f"reason=src_crs_none src_crs=None dst_crs={dst_crs_authid}",
            "WARNING",
        )
        return None

    from qgis.core import (  # noqa: PLC0415
        QgsCoordinateReferenceSystem,
        QgsCoordinateTransform,
        QgsProject,
    )

    cache_key = (src_crs_authid, dst_crs_authid)
    transform = transform_cache.get(cache_key)
    cache_hit = transform is not None

    if transform is None:
        src_crs = QgsCoordinateReferenceSystem(src_crs_authid)
        if not src_crs.isValid():
            flog(
                f"lens_geom_reproject event=skipped trace_id={trace_id} "
                f"reason=src_crs_invalid src_crs={src_crs_authid} "
                f"dst_crs={dst_crs_authid}",
                "WARNING",
            )
            return None
        dst_crs = QgsCoordinateReferenceSystem(dst_crs_authid)
        if not dst_crs.isValid():
            flog(
                f"lens_geom_reproject event=skipped trace_id={trace_id} "
                f"reason=dst_crs_invalid src_crs={src_crs_authid} "
                f"dst_crs={dst_crs_authid}",
                "WARNING",
            )
            return None
        try:
            transform = QgsCoordinateTransform(
                src_crs, dst_crs, QgsProject.instance(),
            )
        except Exception as exc:  # noqa: BLE001 - QGIS raises various types
            flog(
                f"lens_geom_reproject event=skipped trace_id={trace_id} "
                f"reason=transform_failed src_crs={src_crs_authid} "
                f"dst_crs={dst_crs_authid} type={type(exc).__name__}",
                "WARNING",
            )
            return None
        transform_cache[cache_key] = transform

    try:
        # transform() returns int status code (0=ok); we need a copy.
        repro = type(geom)(geom)
        status = repro.transform(transform)
    except Exception as exc:  # noqa: BLE001 - QGIS raises various types
        flog(
            f"lens_geom_reproject event=skipped trace_id={trace_id} "
            f"reason=transform_failed src_crs={src_crs_authid} "
            f"dst_crs={dst_crs_authid} type={type(exc).__name__}",
            "WARNING",
        )
        return None

    if status != 0:
        flog(
            f"lens_geom_reproject event=skipped trace_id={trace_id} "
            f"reason=transform_failed src_crs={src_crs_authid} "
            f"dst_crs={dst_crs_authid} status={status}",
            "WARNING",
        )
        return None

    flog(
        f"lens_geom_reproject event=reprojected trace_id={trace_id} "
        f"src_crs={src_crs_authid} dst_crs={dst_crs_authid} "
        f"cache_hit={str(cache_hit).lower()}",
        "INFO",
    )
    return repro


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
