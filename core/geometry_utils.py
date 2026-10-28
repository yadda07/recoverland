# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Geometry conversion utilities for RecoverLand (RLU-028).

Handles WKB serialization/deserialization of QgsGeometry objects.
Stores geometry as binary WKB in SQLite, with CRS tracked separately.
Tables without geometry get geometry_wkb=NULL, geometry_type='NoGeometry'.
"""
import hashlib
import math
import os
import time
from typing import NamedTuple, Optional, Tuple


# Heavy forensic diagnostic: per-event provider.getFeatures() calls that
# snapshot geometry before/after each restore action. Useful to triage a
# corrupted rewind, but costs one provider I/O round-trip per event, which
# freezes the UI thread on rewinds of ~1000 events (Windows reports
# "(Ne repond pas)"). Off by default; the user opts in by setting the
# env var to a truthy value before starting QGIS.
HEAVY_DIAG_ENV = "RECOVERLAND_HEAVY_DIAG"
_HEAVY_DIAG_DISABLED_TOKENS = ("", "0", "false", "no", "off")
_HEAVY_DIAG = (
    os.environ.get(HEAVY_DIAG_ENV, "0").strip().lower()
    not in _HEAVY_DIAG_DISABLED_TOKENS
)


def heavy_diag_enabled() -> bool:
    """Return True when the RECOVERLAND_HEAVY_DIAG opt-in is active.

    Call sites use this to short-circuit per-event diagnostic loops
    (EDIT_START, TRACE_POST_COMMIT, RELOAD_CHECK) when the user has
    not asked for forensic logging.
    """
    return _HEAVY_DIAG


def _compute_makevalid_drift(geom_before, geom_after) -> Tuple[str, str, float]:
    """Quantify the change between two QgsGeometry objects.

    Returns a tuple `(wkb_hash_before, wkb_hash_after, drift_units)` where:
      - `wkb_hash_*` are 8-hex-char SHA-256 prefixes of `bytes(geom.asWkb())`.
        They equal each other if and only if the WKB byte sequences are
        identical.
      - `drift_units` is a distance in the geometry CRS units, the largest
        of three terms:
          1. the L_infinity (Chebyshev) distance between the two bounding
             boxes:
                 max(|xmin_a - xmin_b|, |ymin_a - ymin_b|,
                     |xmax_a - xmax_b|, |ymax_a - ymax_b|)
          2. the difference of perimeters/lengths |len_a - len_b|;
          3. the difference of areas brought back to a length, |area_a -
             area_b| divided by the longest boundary (the mean width of
             the band that changed hands). When neither geometry has a
             boundary the square root of the area delta is used instead.
        Identical inputs always produce drift=0.

    Terms 2 and 3 exist because the bbox alone is blind to exactly what
    `makeValid()` does (I-8): it reorganises the inside of a shape while
    keeping its extreme vertices. A bowtie becomes two disjoint triangles
    (area 0 -> 50), two overlapping parts get merged (area 200 -> 150),
    an arc is linearised (same endpoints, shorter length). In each case
    the bounding box is identical to the bit, so a bbox-only metric
    reported drift=0 and the rewind applied a shape the user never had.
    Comparing lengths and areas as well catches those three families
    while staying a single number the caller compares to one tolerance.

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
        len_b = float(geom_before.length())
        len_a = float(geom_after.length())
        area_b = float(geom_before.area())
        area_a = float(geom_after.area())
    except Exception:
        return hash_before, hash_after, float("inf")

    drift = max(drift, abs(len_a - len_b))

    delta_area = abs(area_a - area_b)
    boundary = max(len_b, len_a)
    if boundary > 0.0:
        drift = max(drift, delta_area / boundary)
    elif delta_area > 0.0:
        drift = max(drift, math.sqrt(delta_area))

    return hash_before, hash_after, drift


def geometry_to_wkb(geom) -> Optional[bytes]:
    """Convert a QgsGeometry to WKB bytes, or None when the geometry is absent.

    "Absent" means None or a NULL QgsGeometry: the state a provider
    reports for a feature that carries no geometry at all, and the state
    the journal represents with ``geometry_wkb IS NULL``.

    An EMPTY geometry (``POLYGON EMPTY``) is a DIFFERENT state: it keeps
    its type and has a real 9-byte WKB. Folding it into NULL made the two
    indistinguishable in the journal, so restoring an event whose
    geometry was empty skipped the geometry write entirely and still
    reported success (I-8). Empty geometries are therefore serialised
    like any other.
    """
    if geom is None:
        return None
    try:
        if geom.isNull():
            return None
    except (AttributeError, RuntimeError):
        # geom does not implement the QgsGeometry interface (mock/stub).
        return None
    wkb = bytes(geom.asWkb())
    return wkb or None


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
            # QgsWkbTypes.Type.NoGeometry, scoped: measured == 100 on Qt5 and
            # Qt6 alike, so the hasattr guard on the short name bought nothing.
            if wkb_type == QgsWkbTypes.Type.NoGeometry:
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
    """Reconstruct a QgsGeometry from WKB bytes, or None if undecodable.

    Returns None when *wkb_data* is None, zero-length, or when QGIS
    cannot decode it.

    A failed decode used to be invisible (I-8). ``QgsGeometry.fromWkb``
    reports it by leaving the geometry NULL - its return value cannot be
    trusted, the PyQt binding of QGIS 4.x returns None whatever the
    outcome - so a truncated or corrupted blob (interrupted write, full
    disk, partial copy of the journal) produced a NULL QgsGeometry that
    was NOT ``None``. Every caller testing ``is not None`` then wrote
    that NULL geometry onto a live feature and destroyed the very data
    the plugin exists to save. Callers must keep treating None as "no
    geometry to write" AND, when the event announced one, as a refusal.

    An EMPTY but typed geometry (``POLYGON EMPTY``) decodes to a
    non-null geometry and is returned as-is: it is a legitimate captured
    state, not a decode failure.
    """
    if wkb_data is None or len(wkb_data) == 0:
        return None
    from qgis.core import QgsGeometry
    geom = QgsGeometry()
    geom.fromWkb(wkb_data)
    try:
        if geom.isNull():
            return None
    except (AttributeError, RuntimeError):
        return None
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
        return None

    from qgis.core import (  # noqa: PLC0415
        QgsCoordinateReferenceSystem,
        QgsCoordinateTransform,
        QgsProject,
    )

    cache_key = (src_crs_authid, dst_crs_authid)
    transform = transform_cache.get(cache_key)

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

    return repro


# Transforms reused across restores, keyed by (src_authid, layer CRS key).
# A restore batch on a reprojected layer hits the same pair for every
# event; building one QgsCoordinateTransform per event is pure waste.
_WRITE_TRANSFORM_CACHE: dict = {}
# Valid area of a layer CRS, expressed in that CRS, keyed like the
# transforms. None when the CRS publishes no area of use, or when the area
# cannot be expressed in the CRS itself.
_CRS_AREA_CACHE: dict = {}
# The story -- relabel or in-place reprojection -- settled for one layer and
# one capture CRS (`_story_key`), as ``(decision, evidence)``. A relabel or a
# reprojection happens to the layer, not to one geometry: once evidence has
# settled it, every geometry captured in that CRS follows it, the identity
# proofs and the writes of an operation alike, whatever order the events
# come in. Evidence that contradicts it turns it into ``"conflict"``, and
# every later geometry of the pair is refused. Deciding per geometry let one
# UPDATE be proven under one story and written under the other, and one
# rewind apply both stories to the same layer. Kept for the session: a story
# settled by evidence does not stop being true between two operations, and a
# new CRS on the layer, or a new source behind it, is a new key.
# The table the WRITES read and record; a display passes a `ReadOnlyStories`
# to crs_verdict instead (``stories``), which reads it and records nothing.
_CRS_STORIES: dict = {}

# crs_decision lines already written, per (story key, decision, evidence,
# trace_id): how many geometries were decided alike since. A relabel or a
# reprojection is decided for a layer, not for one geometry: one line per
# story and per operation (its trace_id) keeps the evidence in the log,
# where one WARNING line per geometry buried it (2000 lines for one Review
# date of a mass-edited layer) and cost as much as the decision. A call
# without a trace_id (an event restored on its own) is logged every time, as
# before: nothing else would isolate it in the log.
_DECISIONS_LOGGED: dict = {}
_DECISIONS_LOGGED_MAX = 4096

# `CaptureCarriers`: how many committed features one pass may read per
# reading of its group (at least `_PASS_MIN`), and at most, whatever that
# gives. One pass is one provider read in ONE unit of work
# (`CaptureCarriers.load_steps`), on the GUI thread for a display: ~2000
# small features take ~10 ms. 16 per reading stays under the cost of the
# query the pass replaces (one per reading, ~0.3 ms on a local file, a
# network round trip on a database).
_PASS_PER_READING = 16
_PASS_MIN = 256
_PASS_MAX = 2000
# How long one pass may read: past it the pass stops (it covers nothing)
# and the cap above follows the measured cost of a feature, never below
# `_PASS_CAP_MIN` -- heavy polygons or a slow provider make smaller passes,
# not longer ones.
_PASS_TARGET_MS = 30.0
_PASS_CAP_MIN = 64
# What the passes of one load may read in unions that held more than their
# limit (they cover nothing, their readings go to the per-reading query):
# `_PASS_WASTE_PER_READING` features per reading to look up, never less
# than one full pass nor more than `_PASS_WASTE_PASSES` of them, plus half
# of what the passes that did cover their readings read. Past it a pass may
# still read `_PASS_PROBE` features: a group whose union is (nearly) empty
# -- the readings no feature carries -- still costs one request, not one
# per reading.
_PASS_WASTE_PER_READING = 2
_PASS_WASTE_PASSES = 3
_PASS_PROBE = 32
# Entries a `CarrierMemo` keeps, every scope together, before it starts over.
_MEMO_MAX = 100000

# How far outside the valid area of the layer CRS a position must lie to be
# ruled out, as a fraction of the larger side of that area. An area of use
# is a recommendation, not a mathematical domain: real data spills over it
# (a national grid carrying a border strip, a UTM layer beyond its zone). A
# hypothesis is therefore accepted only when its position lies INSIDE the
# area and the other one lies BEYOND this margin; anything in between
# decides nothing. The two hypotheses differ by orders of magnitude when
# they differ at all (degrees against metres, one false origin against
# another), so the margin never hides a real answer.
_AREA_MARGIN_RATIO = 0.25
# Beyond this many times the larger side of the area, the rejected position
# is one no data in this CRS can plausibly hold (degrees read as metres
# land millions of metres off): the area verdict is STRONG and stands as
# evidence. Between the margin and this bound it is WEAK -- data genuinely
# stored outside its CRS's area of use (Lyon in the British grid) produces
# exactly that -- and it only counts when something else corroborates it.
_AREA_STRONG_RATIO = 1.0
# Relative gap under which both hypotheses give the same position (two
# authority ids for one definition, a null datum shift): nothing to decide.
_EQUIVALENT_RTOL = 1e-9

# Statuses of `geometry_for_layer_crs` that mean "the capture CRS is not the
# layer CRS", whatever was decided. Callers log them as a crs_mismatch.
CRS_MISMATCH_STATUSES = ("reprojected", "relabeled", "equivalent_crs",
                         "crs_ambiguous", "transform_failed")
# Statuses after which nothing may be written, whatever geometry comes with
# them (`geometry_for_layer_crs` hands a comparison reading with
# "crs_ambiguous", see there).
CRS_REFUSED_STATUSES = ("crs_ambiguous", "transform_failed")


class CrsVerdict(NamedTuple):
    """What `crs_verdict` decided for one captured geometry.

    ``status``: same_crs, unknown_crs, empty, equivalent_crs, relabeled,
    reprojected, crs_ambiguous or transform_failed. ``geometry``: what may
    be written in the layer CRS, None when nothing may be (crs_ambiguous,
    transform_failed). ``raw`` and ``reprojected`` are the two readings of
    the captured coordinates, in the layer CRS: untouched (the relabel
    story) and through the transform (the reprojection story, None when it
    cannot be computed). ``evidence``: what the decision rests on, as
    logged in the ``crs_decision`` line.
    """

    status: str
    geometry: object
    raw: object
    reprojected: object
    evidence: str


def _finite_box(geom) -> Optional[Tuple[float, float, float, float]]:
    """(xmin, ymin, xmax, ymax) of *geom*, or None if it has no finite box.

    Plain floats on purpose: ``QgsRectangle.isNull()`` changed meaning across
    QGIS versions (a box at (0, 0, 0, 0) is "null" in 3.x), and a point at
    the origin must not vanish from the evidence.
    """
    if geom is None:
        return None
    try:
        if geom.isNull() or geom.isEmpty():
            return None
        rect = geom.boundingBox()
    except (AttributeError, RuntimeError):
        return None
    return _finite_rect(rect)


def _finite_rect(rect) -> Optional[Tuple[float, float, float, float]]:
    try:
        box = (rect.xMinimum(), rect.yMinimum(),
               rect.xMaximum(), rect.yMaximum())
    except (AttributeError, RuntimeError):
        return None
    # Spelled out: called several times per geometry a verdict decides, a
    # generator here was a visible part of a Review date's cost.
    isfinite = math.isfinite
    if not (isfinite(box[0]) and isfinite(box[1])
            and isfinite(box[2]) and isfinite(box[3])):
        return None
    if box[0] > box[2] or box[1] > box[3]:
        return None
    return box


def _larger_side(box) -> float:
    return max(box[2] - box[0], box[3] - box[1])


def _grown(box, margin: float):
    return (box[0] - margin, box[1] - margin, box[2] + margin, box[3] + margin)


def _overlaps(area, box) -> bool:
    return (area is not None and box is not None
            and box[0] <= area[2] and area[0] <= box[2]
            and box[1] <= area[3] and area[1] <= box[3])


def _same_box(a, b) -> bool:
    scale = max(1.0, max(abs(v) for v in a + b))
    return all(abs(x - y) <= _EQUIVALENT_RTOL * scale for x, y in zip(a, b))


def _crs_key(crs) -> str:
    return crs.authid() or crs.toWkt()


def _crs_label(crs) -> str:
    """Authority id, or a readable name for a CRS that has none."""
    try:
        return crs.authid() or f"custom CRS '{crs.description()}'"
    except (AttributeError, RuntimeError):
        return "unknown CRS"


def _write_transform(src_crs_authid: str, dst_crs):
    """Transform capture CRS -> layer CRS, or None when it cannot be built."""
    key = (src_crs_authid, _crs_key(dst_crs))
    cached = _WRITE_TRANSFORM_CACHE.get(key)
    if cached is not None:
        return cached
    from qgis.core import (  # noqa: PLC0415
        QgsCoordinateReferenceSystem,
        QgsCoordinateTransform,
        QgsProject,
    )
    src_crs = QgsCoordinateReferenceSystem(src_crs_authid)
    if not src_crs.isValid():
        return None
    try:
        transform = QgsCoordinateTransform(src_crs, dst_crs,
                                           QgsProject.instance())
    except Exception:  # noqa: BLE001 - QGIS raises various types
        return None
    if not transform.isValid():
        return None
    _WRITE_TRANSFORM_CACHE[key] = transform
    return transform


def _reprojected(geom, transform):
    """Copy of *geom* through *transform*, None when it cannot go through.

    A failure here is not an infrastructure problem (the transform exists):
    the raw coordinates are outside the domain of the capture CRS, or their
    image is outside the domain of the layer CRS. Either way the file cannot
    hold this geometry reprojected, and that is evidence, not an error.
    """
    try:
        repro = type(geom)(geom)
        status = repro.transform(transform)
    except Exception:  # noqa: BLE001 - QgsCsException, out of domain
        return None
    if int(getattr(status, "value", status)) != 0:
        return None
    if _finite_box(repro) is None:
        return None
    return repro


def _crs_valid_area(dst_crs):
    """Valid area of *dst_crs* in its own coordinates, None if unknown.

    ``QgsCoordinateReferenceSystem.bounds()`` is the area of use in WGS84
    degrees; it is carried into the CRS itself so that it can be compared
    with raw provider coordinates.
    """
    key = _crs_key(dst_crs)
    if key in _CRS_AREA_CACHE:
        return _CRS_AREA_CACHE[key]
    area = None
    try:
        from qgis.core import (  # noqa: PLC0415
            QgsCoordinateReferenceSystem,
            QgsCoordinateTransform,
            QgsProject,
        )
        bounds = dst_crs.bounds()
        if bounds is not None and bounds.width() > 0 and bounds.height() > 0:
            to_layer = QgsCoordinateTransform(
                QgsCoordinateReferenceSystem("EPSG:4326"), dst_crs,
                QgsProject.instance())
            area = _finite_rect(to_layer.transformBoundingBox(bounds))
            if area is not None and _larger_side(area) <= 0:
                area = None
    except Exception:  # noqa: BLE001 - QgsCsException on world-wide areas
        area = None
    _CRS_AREA_CACHE[key] = area
    return area


def _witness_list(witnesses) -> list:
    """The `crs_verdict` witnesses as geometries, decoded here and only here.

    A write path hands the OTHER captured geometries of its event, as the
    journal holds them (WKB blobs): the NEW geometry of an UPDATE is the
    one its target still carries, and whether that target holds it raw or
    reprojected tells the story for the OLD one about to be written. They
    are decoded only once a decision is needed -- a layer still in its
    capture CRS never pays for it. Skips what has no coordinate to look for
    (None, the NULL marker -- a zero-length blob --, an undecodable blob,
    an EMPTY geometry).
    """
    out = []
    for item in witnesses or ():
        if isinstance(item, (bytes, bytearray, memoryview)):
            item = rebuild_geometry(bytes(item)) if len(item) else None
        if is_geometry_present(item):
            out.append(item)
    return out


def _committed_carriers(layer, geom) -> Optional[list]:
    """FIDs of the committed features whose geometry IS *geom*; None if unread.

    The provider, not the layer: during a rewind the edit buffer holds what
    this very batch wrote, placed by this very decision -- not evidence.
    The comparison is `feature_matches_geometry`, the one the identity
    proofs use: a feature they would match on this reading is a feature
    seen here. A read that failed is reported (None), never taken for "no
    such feature": that silence would leave another piece of evidence
    unchallenged.
    """
    try:
        from qgis.core import QgsFeatureRequest  # noqa: PLC0415
        provider = layer.dataProvider()
        request = QgsFeatureRequest().setFilterRect(geom.boundingBox())
        request.setNoAttributes()
        errors_before = len(provider.errors())
        iterator = provider.getFeatures(request)
        if hasattr(iterator, "isValid") and not iterator.isValid():
            return None
        expected_wkb = bytes(geom.asWkb())
        fids = [feature.id() for feature in iterator
                if feature_matches_geometry(feature, geom,
                                            expected_wkb=expected_wkb)]
        if len(provider.errors()) > errors_before:
            return None
    except Exception:  # noqa: BLE001 - any failure voids the read
        return None
    return fids


def _union_box(boxes) -> Tuple[float, float, float, float]:
    return (min(b[0] for b in boxes), min(b[1] for b in boxes),
            max(b[2] for b in boxes), max(b[3] for b in boxes))


def _halves(boxes: list) -> Tuple[list, list]:
    """*boxes* cut in two at the median of their centres, along the longer
    side of their union."""
    union = _union_box(boxes)
    axis = 0 if union[2] - union[0] >= union[3] - union[1] else 1
    ordered = sorted(boxes, key=lambda b: b[axis] + b[axis + 2])
    middle = len(ordered) // 2
    return ordered[:middle], ordered[middle:]


def _area_per_reading(boxes: list) -> float:
    union = _union_box(boxes)
    return (union[2] - union[0]) * (union[3] - union[1]) / len(boxes)


def _reading_key(data: bytes) -> bytes:
    """What a `CarrierMemo` knows a reading by: a digest of its exact WKB."""
    return hashlib.blake2b(data, digest_size=16).digest()


class CarrierMemo:
    """What `CaptureCarriers` proved, kept from one batch to the next.

    Per layer scope (id, source, CRS) and per reading (a digest of its
    exact WKB): the fids of the committed features carrying it, as a
    complete read found them. A Review date proved all of it again at
    every date: up to four provider queries per edited geometry, date
    after date, about features nobody had touched meanwhile.

    Valid while the committed features stay as they were read. The owner
    calls `forget` on every change it can see (the Review: a commit, a
    reload, a CRS or source change on any layer of the project, Review
    started or stopped -- snapshot_overlay_session._CarrierWatch).
    ``epoch`` counts the forgets: a batch records only what it read in the
    epoch it still is in, so a read that straddles a change is never kept,
    and one read before it is never served after it.
    """

    def __init__(self, max_entries: int = _MEMO_MAX):
        self.epoch = 0
        self._max = max_entries
        self._scopes: dict = {}
        self._n = 0

    def __len__(self) -> int:
        return self._n

    def forget(self) -> None:
        self.epoch += 1
        self._scopes = {}
        self._n = 0

    def get(self, scope, key) -> Optional[tuple]:
        """The fids known to carry the reading *key*, None if not known."""
        entries = self._scopes.get(scope)
        return None if entries is None else entries.get(key)

    def put(self, scope, key, fids, epoch: int) -> None:
        """Keep *fids* for *key*, read during *epoch*; nothing if the memo
        has forgotten since."""
        if epoch != self.epoch or scope is None:
            return
        if self._n >= self._max:
            # Bounded: it starts over (a forget), nothing stale is kept.
            self.forget()
            return
        entries = self._scopes.setdefault(scope, {})
        if key not in entries:
            self._n += 1
        entries[key] = tuple(fids)


class CaptureCarriers:
    """The committed features of one layer that carry a captured position.

    `_committed_carriers` answers for ONE reading with one provider query,
    and `crs_verdict` asks it for up to four readings per geometry (raw and
    reprojected, the geometry and the other side of its event), even once
    the story of the layer is settled: its own evidence must still agree
    with the story. A caller that decides many geometries of one layer at
    once paid that for each of them: a Review date of 2000 edited entities
    held QGIS seconds on the GUI thread, and again at every date.

    This lookup answers the same question for a batch. The readings are
    registered first (`add_capture`, the very readings crs_verdict will
    ask for), read in bounded provider passes (`load_steps`, one pass per
    unit of work), and `carriers` answers from memory. The display (Review)
    and a write path that passes it (crs_verdict ``carriers``) rest on the
    same evidence as the per-reading query, feature for feature:
      * a feature `feature_matches_geometry` accepts carries the reading's
        very coordinates, hence its very bounding box: it is kept under
        that box and compared by the same function;
      * a pass covers its readings only when it read EVERY committed
        feature of their union (below its limit, in time, no provider
        error); a reading no pass covered, or not registered, is asked to
        the provider on its own, exactly as before. A pass that failed is
        never taken for "no carrier": it covers nothing.

    The passes, bounded one by one AND together. The edited features of a
    real layer sit among many others: the union of their readings then
    holds far more features than the readings, and halving it level after
    level (as this lookup did) read 32 features per geometry in one
    blocking go before handing almost every reading back to the
    per-reading query, at every date. Now:
      1. one pass over the union of every reading: on an edited area that
         holds the edited features and few others, the only one;
      2. if it held more than one pass may read, the readings are cut,
         WITHOUT reading, into groups one pass can hold at
         `_PASS_PER_READING` features per reading, read smallest union per
         reading first (the readings nothing carries -- a relabel's
         reprojected ones, a reprojection's raw ones -- often share one
         empty corner);
      3. a group whose union holds more than that is not cut again: its
         features are spread among others more densely than the budget,
         and its halves would be as dense. Its readings go to the
         per-reading query;
      4. what passes may read in unions that covered nothing is bounded
         (`_PASS_WASTE_PER_READING`, `_PASS_WASTE_PASSES`, half of what
         the covering passes read); past it, a pass reads at most
         `_PASS_PROBE` features;
      5. one pass reads at most `_PASS_MAX` features and stops after
         `_PASS_TARGET_MS`; the cap follows the measured cost per feature.
    On a dense layer the batch costs at most that bound on top of the old
    queries; on the usual edited area it replaces them.

    *memo*: a `CarrierMemo` the caller keeps across batches (the Review,
    across dates). A reading it knows is neither registered nor read; what
    a pass or a query proves is added to it, unless it forgot meanwhile.
    It is also how a load run across event-loop turns sees a change of the
    committed data between two of its passes: they straddle it, so they
    cover nothing, every reading goes to the query.

    Without a memo, a snapshot of the committed state at `load`: valid for
    one batch. A commit during the batch must end it (the Review cancels
    the completion of a date when a commit lands). `serves` refuses
    another layer, or this one under another source or CRS.
    """

    def __init__(self, layer, memo: Optional[CarrierMemo] = None):
        self._layer = layer
        self._scope = self._scope_of(layer)
        self._memo = memo
        self._pending: set = set()
        self._covered: set = set()
        self._index: dict = {}
        self._cap = _PASS_MAX
        self._load_epoch = None
        self._waste_max = 0
        self._useful = 0
        self.n_passes = 0
        self.n_read = 0
        self.n_wasted = 0
        self.n_direct = 0
        self.n_known = 0
        self.n_memo = 0
        self.n_uncovered = 0
        self.n_failed_passes = 0
        self.stopped = ""

    @staticmethod
    def _scope_of(layer):
        try:
            return (layer.id(), layer.dataProvider().dataSourceUri(),
                    _crs_key(layer.crs()))
        except (AttributeError, RuntimeError):
            return None

    @property
    def cap(self) -> int:
        """Features one pass may read now (it follows the measured cost)."""
        return self._cap

    def serves(self, layer) -> bool:
        """True when *layer* is the layer, source and CRS this was built on."""
        return self._scope is not None and self._scope_of(layer) == self._scope

    def _known(self, reading) -> bool:
        if self._memo is None:
            return False
        try:
            key = _reading_key(bytes(reading.asWkb()))
        except (AttributeError, RuntimeError, TypeError):
            return False
        return self._memo.get(self._scope, key) is not None

    def add_capture(self, geom, src_crs_authid: Optional[str],
                    witnesses=()) -> None:
        """Register the readings `crs_verdict` asks for about *geom*.

        Nothing when no decision will be needed (same CRS, no capture
        CRS, no transform): crs_verdict reads no witness then. A reading
        the memo knows is counted (``n_known``), not registered.
        """
        if geom is None or not src_crs_authid or self._scope is None:
            return
        try:
            dst_crs = self._layer.crs()
            if not dst_crs.isValid() or dst_crs.authid() == src_crs_authid:
                return
        except (AttributeError, RuntimeError):
            return
        transform = _write_transform(src_crs_authid, dst_crs)
        if transform is None:
            return
        seen = set()
        for witness in [geom] + _witness_list(witnesses):
            try:
                data = bytes(witness.asWkb())
            except (AttributeError, RuntimeError, TypeError):
                continue
            if data in seen:
                continue  # the geometry is one of its own event's sides
            seen.add(data)
            for reading in (witness, _reprojected(witness, transform)):
                box = _finite_box(reading)
                if box is None or box in self._covered or box in self._pending:
                    continue
                if self._known(reading):
                    self.n_known += 1
                    continue
                self._pending.add(box)

    def load(self) -> None:
        """`load_steps` in one go (a caller with no event loop to give back)."""
        for _unit in self.load_steps():
            pass

    def load_steps(self):
        """Read the registered readings' carriers, one pass per ``yield``.

        The order and the bounds are the class's (1 to 5). A unit is one
        pass, or one cut of a group too large for one pass (a sort, no
        read). A caller that gives the event loop back between two units
        passes a memo: its epoch is how a change of the committed data in
        between is seen (`load` runs them in one go).
        """
        boxes = list(self._pending)
        self._pending = set()
        memo = self._memo
        self._load_epoch = memo.epoch if memo is not None else None
        new_cover: set = set()
        if len(boxes) >= 2:
            self._waste_max = min(
                _PASS_WASTE_PASSES * _PASS_MAX,
                max(_PASS_MAX, _PASS_WASTE_PER_READING * len(boxes)))
            self._useful = 0
            outcome = self._try(boxes, new_cover)
            yield
            if outcome in ("over", "slow") and self._natural(boxes) > self._cap:
                yield from self._leaves_steps(boxes, new_cover)
            elif outcome != "ok":
                self.stopped = {"over": "dense",
                                "failed": "provider_failed"}.get(outcome, outcome)
        if memo is not None and memo.epoch != self._load_epoch and new_cover:
            # The committed data changed between two passes (a commit, a
            # reload...): what they read straddles it. Each reading goes
            # to the query, after the change.
            self._covered.difference_update(new_cover)
            for box in new_cover:
                self._index.pop(box, None)
            self.stopped = "data_changed"
        self.n_uncovered += sum(1 for box in boxes if box not in self._covered)

    def _leaves_steps(self, boxes: list, new_cover: set):
        """Steps 2 to 5: cut *boxes* into groups one pass can hold, then
        read them, the smallest union per reading first."""
        leaves = []
        work = [boxes]
        while work:
            group = work.pop()
            if len(group) > 1 and self._natural(group) > self._cap:
                work.extend(_halves(group))
                yield
            elif len(group) > 1:
                leaves.append(group)
        leaves.sort(key=_area_per_reading, reverse=True)
        while leaves:
            group = leaves.pop()
            if len(group) > 1 and self._natural(group) > self._cap:
                # The cap came down since it was cut (a slow pass).
                leaves.extend(_halves(group))
                yield
                continue
            if not self.serves(self._layer):
                # Another source or CRS behind the layer, or the layer
                # gone: nothing more is read for this scope.
                self.stopped = "layer_changed"
                return
            outcome = self._try(group, new_cover)
            yield
            if outcome == "failed":
                self.stopped = "provider_failed"
                return
            if outcome == "slow" and len(group) > 1:
                leaves.extend(_halves(group))
            elif outcome == "over":
                self.stopped = self.stopped or "dense"

    @staticmethod
    def _natural(group: list) -> int:
        return max(_PASS_MIN, _PASS_PER_READING * len(group))

    def _try(self, group: list, new_cover: set) -> str:
        """One pass over *group*, within the cap and what is left of the
        waste allowance. Its outcome: ok, over, slow or failed."""
        allowed = self._waste_max + self._useful // 2 - self.n_wasted
        limit = min(self._cap, self._natural(group), max(_PASS_PROBE, allowed))
        found, n_read, outcome = self._pass(group, limit)
        if outcome != "ok":
            self.n_wasted += n_read
            return outcome
        self._useful += n_read
        for box, features in found.items():
            self._index.setdefault(box, []).extend(features)
        self._covered.update(group)
        new_cover.update(group)
        return "ok"

    def _pass(self, group: list, limit: int):
        """(found, n read, outcome) of one provider read over the union of
        *group*: ``found`` is {box: [feature]} when the outcome is "ok"; it
        is None when more than *limit* features lie there ("over"), when
        reading them took longer than `_PASS_TARGET_MS` ("slow"), or when
        the read failed ("failed", counted in ``n_failed_passes``)."""
        wanted = set(group)
        union = _union_box(group)
        # A union of points on one line has no width: widened by a hair so
        # no provider takes it for an empty (or null) rectangle. The extra
        # features it may bring are dropped (not a wanted box).
        pad = 1e-9 * max(1.0, max(abs(v) for v in union))
        self.n_passes += 1
        n_read = 0
        t_first = None
        found: dict = {}
        outcome = "ok"
        try:
            from qgis.core import QgsFeatureRequest, QgsRectangle  # noqa: PLC0415
            provider = self._layer.dataProvider()
            request = QgsFeatureRequest().setFilterRect(QgsRectangle(
                union[0] - pad, union[1] - pad, union[2] + pad, union[3] + pad))
            request.setNoAttributes()
            request.setLimit(limit + 1)
            errors_before = len(provider.errors())
            iterator = provider.getFeatures(request)
            if hasattr(iterator, "isValid") and not iterator.isValid():
                outcome = "failed"
            else:
                for feature in iterator:
                    n_read += 1
                    if t_first is None:
                        # From the first feature: the request's own latency
                        # (a network round trip) is not a cost per feature.
                        t_first = time.perf_counter()
                    if n_read > limit:
                        outcome = "over"
                        break
                    if n_read % 64 == 0 and (time.perf_counter() - t_first) \
                            * 1000.0 >= _PASS_TARGET_MS:
                        outcome = "slow"
                        break
                    box = _finite_box(feature.geometry())
                    if box in wanted:
                        found.setdefault(box, []).append(feature)
                if outcome != "ok":
                    iterator.close()
                elif len(provider.errors()) > errors_before:
                    outcome = "failed"
        except Exception:  # noqa: BLE001 - any failure voids the pass
            outcome = "failed"
        self.n_read += n_read
        if outcome == "failed":
            self.n_failed_passes += 1
        if t_first is not None and n_read >= 64:
            per_feature_ms = (time.perf_counter() - t_first) * 1000.0 / n_read
            if per_feature_ms > 0:
                self._cap = max(_PASS_CAP_MIN, min(
                    _PASS_MAX, int(_PASS_TARGET_MS / per_feature_ms)))
        return (found if outcome == "ok" else None), n_read, outcome

    def carriers(self, reading) -> Optional[list]:
        """`_committed_carriers` of *reading*: from the memo when it knows
        it, from memory when a pass covered it, else one query."""
        memo = self._memo
        try:
            data = bytes(reading.asWkb())
        except (AttributeError, RuntimeError, TypeError):
            data = None
        key = _reading_key(data) if data is not None else None
        if memo is not None and key is not None:
            known = memo.get(self._scope, key)
            if known is not None:
                self.n_memo += 1
                return list(known)
        box = _finite_box(reading)
        if box is None or box not in self._covered:
            self.n_direct += 1
            epoch = memo.epoch if memo is not None else None
            fids = _committed_carriers(self._layer, reading)
            if fids is not None and memo is not None and key is not None:
                memo.put(self._scope, key, fids, epoch)
            return fids
        fids = []
        for feature in self._index.get(box, ()):
            if feature.id() not in fids and feature_matches_geometry(
                    feature, reading, expected_wkb=data):
                fids.append(feature.id())
        if memo is not None and key is not None:
            memo.put(self._scope, key, fids, self._load_epoch)
        return fids


class ReadOnlyStories:
    """The CRS stories as a display reads them (crs_verdict ``stories``).

    Every story the writes settled (`_CRS_STORIES`, as it is when asked) is
    read; what this view's own evidence settles is kept in ``own`` (a dict
    the caller owns and may keep across calls), never in the table the
    writes read: what the user merely looked at must not decide what a
    restore writes. When its own story and a settled one disagree, the
    answer is the conflict a write would reach.

    Explicit, where the display used to swap the module table for a view
    around each verdict (geometry_preview.reading_stories): nothing is
    installed anywhere, so no other caller, on any thread, can go through
    it by mistake.
    """

    read_only = True

    def __init__(self, own: Optional[dict] = None):
        self.own = own if own is not None else {}

    def get(self, key, default=None):
        settled = _CRS_STORIES.get(key)
        own = self.own.get(key)
        if own is None or (settled is not None and settled[0] == own[0]):
            return settled if settled is not None else default
        if settled is None:
            return own
        return ("conflict", f"{settled[0]}({settled[1]})_vs_display_"
                            f"{own[0]}({own[1]})")

    def __setitem__(self, key, value) -> None:
        if self.own.get(key) != value:
            from .logger import flog  # noqa: PLC0415
            scope = (f"layer={key[0]} src_crs={key[2]}"
                     if isinstance(key, tuple) and len(key) > 2 else f"key={key!r}")
            flog(f"display: crs_story kept to the display, not recorded "
                 f"{scope} story={value!r} (a write follows only what a "
                 f"write proved)", "INFO")
        self.own[key] = value

    def forget_stale(self, layer) -> int:
        """Drop what this view settled for *layer* under another source or
        CRS than the one it has now. Returns how many stories were dropped.

        The key already carries both (a new CRS or source is a new key); a
        story kept for the old pair would come back if the layer went back
        to it, whatever was done to the file meanwhile.
        """
        try:
            layer_id = layer.id()
            now = (layer.dataProvider().dataSourceUri(), _crs_key(layer.crs()))
        except (AttributeError, RuntimeError):
            return 0
        stale = [key for key in self.own
                 if isinstance(key, tuple) and len(key) == 4
                 and key[0] == layer_id and (key[1], key[3]) != now]
        for key in stale:
            del self.own[key]
        return len(stale)


def _witness_verdict(layer, witnesses, transform, facts,
                     carriers=None) -> Tuple[Optional[str], str]:
    """(story or None, evidence) from features that still carry a capture.

    A committed feature carrying EXACTLY the raw reading of a captured
    geometry saw its numbers kept (relabel); one carrying exactly the
    reprojected reading saw them converted (reprojection). Exact equality
    of the coordinates is what makes it evidence, unlike being near other
    features: under the other story the feature would sit at the other
    reading, and a stranger landing on the very same coordinates is not a
    coincidence to bet a write on. Both readings carried is a conflict, so
    is a read that failed. A witness whose two readings coincide tells
    nothing and is skipped.

    *carriers*: a `CaptureCarriers` of *layer* answers from its memo and
    its passes (same evidence); None, or one built on another layer,
    source or CRS: one provider query per reading.
    """
    lookup = (carriers.carriers
              if carriers is not None and carriers.serves(layer)
              else (lambda reading: _committed_carriers(layer, reading)))
    at_raw, at_repro = set(), set()
    seen = set()
    for witness in witnesses:
        try:
            data = bytes(witness.asWkb())
        except (AttributeError, RuntimeError, TypeError):
            continue
        if data in seen:
            continue
        seen.add(data)
        raw_box = _finite_box(witness)
        repro = _reprojected(witness, transform)
        repro_box = _finite_box(repro)
        if raw_box is None:
            continue
        if repro_box is not None and _same_box(raw_box, repro_box):
            continue
        for reading, bucket in ((witness, at_raw), (repro, at_repro)):
            if reading is None:
                continue
            fids = lookup(reading)
            if fids is None:
                facts["witness"] = "unreadable"
                return "conflict", "witness_read_failed"
            bucket.update(fids)
    facts["witnesses"] = len(seen)
    if at_raw:
        facts["witness_raw"] = sorted(at_raw)[:5]
    if at_repro:
        facts["witness_repro"] = sorted(at_repro)[:5]
    if at_raw and at_repro:
        return "conflict", "witness_conflict"
    if at_raw:
        return "relabel", "witness"
    if at_repro:
        return "reprojection", "witness"
    return None, ""


def _area_verdict(raw_box, repro_box, dst_crs, facts) -> Tuple[Optional[str], bool]:
    """(story or None, strong) from the valid area of the layer CRS.

    A hypothesis wins when its position lies inside the area and the other
    lies beyond the margin (or does not exist: the reprojection could not
    be computed). STRONG when the rejected position is absent or lies
    beyond `_AREA_STRONG_RATIO` times the area, WEAK otherwise.
    """
    area = _crs_valid_area(dst_crs)
    if area is None:
        facts["area"] = "unknown"
        return None, False
    side = _larger_side(area)
    far = _grown(area, _AREA_MARGIN_RATIO * side)
    beyond = _grown(area, _AREA_STRONG_RATIO * side)
    raw_in, repro_in = _overlaps(area, raw_box), _overlaps(area, repro_box)
    raw_far, repro_far = (not _overlaps(far, raw_box),
                          not _overlaps(far, repro_box))
    facts.update(raw_in_area=raw_in, repro_in_area=repro_in,
                 raw_far=raw_far, repro_far=repro_far)
    by_area, strong = None, False
    if raw_in and repro_far:
        by_area, strong = "relabel", not _overlaps(beyond, repro_box)
    elif repro_in and raw_far:
        by_area, strong = "reprojection", not _overlaps(beyond, raw_box)
    if by_area:
        facts["area_verdict"] = "strong" if strong else "weak"
    return by_area, strong


def _story_key(layer, src_crs_authid: str, dst_crs) -> tuple:
    """(layer, source, capture CRS, layer CRS): the scope of one story."""
    try:
        layer_id = layer.id()
    except (AttributeError, RuntimeError):
        layer_id = f"object-{id(layer)}"
    try:
        source = layer.dataProvider().dataSourceUri()
    except (AttributeError, RuntimeError):
        source = ""
    return (layer_id, source, src_crs_authid, _crs_key(dst_crs))


def _settle_story(key, own: Optional[str], own_evidence: str,
                  weak: Optional[str], facts: dict,
                  table=None) -> Tuple[str, str]:
    """Confront this geometry's evidence with the story of its layer.

    Own evidence (a witness, a strong area verdict, an impossible
    reprojection) settles the story when there is none yet and must agree
    with it otherwise: a contradiction turns the story into a conflict,
    refused from then on. A geometry without evidence of its own follows
    the story, unless its weak area verdict points the other way. Without a
    story, a weak verdict alone is not trusted, and nothing at all decides
    nothing.

    *table*: where the story is read and recorded, `_CRS_STORIES` (looked
    up at call time) when None, or a `ReadOnlyStories`.
    """
    if table is None:
        table = _CRS_STORIES
    story = table.get(key)
    if story is not None:
        facts["story"] = f"{story[0]}:{story[1]}"
    if story is not None and story[0] == "conflict":
        return "ambiguous", "story_conflict"
    if own:
        if story is not None and story[0] != own:
            table[key] = (
                "conflict", f"{story[0]}({story[1]})_vs_{own}({own_evidence})")
            from .logger import flog  # noqa: PLC0415
            scope = (" (display only: not recorded for the writes)"
                     if getattr(table, "read_only", False) else "")
            flog(f"crs_story conflict src_crs={key[2]} layer={key[0]} "
                 f"settled={story[0]}({story[1]}) contradicted_by={own}"
                 f"({own_evidence}): every later geometry of this layer "
                 f"captured in {key[2]} is refused (crs_ambiguous){scope}",
                 "ERROR")
            return "ambiguous", "contradicts_story"
        if story is None:
            table[key] = (own, own_evidence)
        return own, own_evidence
    if story is not None:
        if weak and weak != story[0]:
            return "ambiguous", "weak_area_disagrees_with_story"
        return story[0], "story"
    if weak:
        return "ambiguous", "weak_area_uncorroborated"
    return "ambiguous", "no_evidence"


def _decide_crs_story(geom, repro, dst_crs, layer, transform, witnesses,
                      key, table=None, carriers=None) -> Tuple[str, str, dict]:
    """Relabel or reprojection, from evidence. Returns (decision, evidence, facts).

    Evidence, each piece naming one story:
      * a witness (`_witness_verdict`): a committed feature carries exactly
        one reading of *geom* or of another geometry of its event;
      * a STRONG area verdict (`_area_verdict`);
      * an impossible reprojection: the file cannot have been rewritten
        through a transform these coordinates do not go through.
    A WEAK area verdict is not evidence on its own. Pieces that disagree
    are a conflict. Then the story of the layer (`_settle_story`).

    Not evidence: the layer's data extent. Being near the OTHER features
    says nothing of what happened to the coordinates (Lambert-93 and Web
    Mercator overlap numerically over France): taking it for evidence
    re-created a Toulouse feature 968 km away, near Verdun, with success.
    """
    facts = {}
    raw_box, repro_box = _finite_box(geom), _finite_box(repro)
    firm = []
    if repro_box is None:
        firm.append(("relabel", "reprojection_impossible"))
    by_area, strong = _area_verdict(raw_box, repro_box, dst_crs, facts)
    weak = None
    if by_area and strong:
        firm.append((by_area, "area_strong"))
    elif by_area:
        weak = by_area
    seen, seen_evidence = _witness_verdict(
        layer, [geom] + _witness_list(witnesses), transform, facts, carriers)
    if seen == "conflict":
        return "ambiguous", seen_evidence, facts
    if seen:
        firm.append((seen, seen_evidence))
    decisions = {decision for decision, _evidence in firm}
    if len(decisions) > 1:
        facts["firm"] = ",".join(f"{d}:{e}" for d, e in firm)
        return "ambiguous", "evidence_conflict", facts
    own = decisions.pop() if decisions else None
    own_evidence = "+".join(evidence for _decision, evidence in firm)
    if own and weak and weak != own:
        facts["weak_area_overruled"] = weak
    decision, evidence = _settle_story(key, own, own_evidence, weak, facts,
                                       table)
    return decision, evidence, facts


def _box_center(box) -> str:
    if box is None:
        return "none"
    return f"({(box[0] + box[2]) / 2:.6g},{(box[1] + box[3]) / 2:.6g})"


def _log_decision(signature, text: str, level: str) -> None:
    """Write one crs_decision line, once per *signature* (see
    `_DECISIONS_LOGGED`); every time when *signature* is None."""
    from .logger import flog  # noqa: PLC0415
    if signature is None:
        flog(text, level)
        return
    seen = _DECISIONS_LOGGED.get(signature)
    if seen is not None:
        _DECISIONS_LOGGED[signature] = seen + 1
        return
    if len(_DECISIONS_LOGGED) >= _DECISIONS_LOGGED_MAX:
        _DECISIONS_LOGGED.clear()
    _DECISIONS_LOGGED[signature] = 1
    flog(text, level)


def crs_verdict(geom, src_crs_authid: Optional[str], layer,
                trace_id: str = "", witnesses=(), stories=None,
                carriers=None) -> CrsVerdict:
    """Decide how *geom*, captured in *src_crs_authid*, goes into *layer*.

    The journal stores the WKB verbatim: the RAW coordinates the provider
    returned at capture time. ``event.crs_authid`` is only the CRS the
    layer DECLARED then. When it differs from the CRS the layer declares
    now, two stories fit and they call for opposite writes:

      * a relabel -- "Set Layer CRS", a corrected .prj, a wrongly declared
        CRS fixed. The provider still returns the same numbers; only their
        reading changed. The raw coordinates must be written as they are:
        reprojecting them moves every restored feature (hundreds or
        thousands of km) while the restore reports success (I-3).
      * a reprojection in place -- the file rewritten in another CRS at the
        same path. The provider's numbers changed; writing the raw ones
        drops a degree position into a metric layer ~6900 km away.

    Deciding by default is wrong one way or the other, so the decision is
    taken from evidence (`_decide_crs_story`), once per layer and capture
    CRS (`_CRS_STORIES`); when there is none, or when it contradicts
    itself, nothing may be written (status ``crs_ambiguous``). Every
    decision is logged (``crs_decision``) with the evidence it rests on:
    under a *trace_id*, once per layer, capture CRS, decision and evidence
    (`_DECISIONS_LOGGED`), the geometries decided alike after it are not
    logged one by one; without one, every time.

    *witnesses*: the other captured geometries of the event, as WKB blobs
    or geometries (`_witness_list`); *geom* itself is always one.

    *stories*: None for the writes (read and record `_CRS_STORIES`), or a
    `ReadOnlyStories` for a display (reads it, records nothing in it).
    Nothing else: a plain dict would not read what the writes settled.

    *carriers*: a `CaptureCarriers` loaded for a batch of geometries of
    *layer*, which answers the witness reads from its memo and its passes
    (same evidence, see there); None: one provider query per reading. The
    decision is the same either way.
    """
    if stories is not None and not isinstance(stories, ReadOnlyStories):
        raise TypeError(f"crs_verdict: stories must be None (the writes' "
                        f"table) or a ReadOnlyStories, not "
                        f"{type(stories).__name__}")
    if geom is None:
        return CrsVerdict("unknown_crs", None, None, None, "no_geometry")
    if not src_crs_authid:
        return CrsVerdict("unknown_crs", geom, geom, None, "no_capture_crs")

    dst_crs = None
    try:
        crs = layer.crs()
        if crs is not None and crs.isValid():
            dst_crs = crs
    except (AttributeError, RuntimeError):
        dst_crs = None
    if dst_crs is None:
        return CrsVerdict("unknown_crs", geom, geom, None, "no_layer_crs")

    if dst_crs.authid() == src_crs_authid:
        return CrsVerdict("same_crs", geom, geom, geom, "same_crs")
    try:
        if geom.isEmpty():
            return CrsVerdict("empty", geom, geom, geom, "no_coordinate")
    except (AttributeError, RuntimeError):
        pass

    layer_label = _crs_label(dst_crs).replace(" ", "_")
    read_only = stories is not None
    tail = ((" scope=display" if read_only else "")
            + (f" trace_id={trace_id}" if trace_id else " trace_id=-"))
    key = _story_key(layer, src_crs_authid, dst_crs)

    def head(decision: str) -> str:
        return (f"crs_decision decision={decision} src_crs={src_crs_authid} "
                f"layer_crs={layer_label}")

    def signature(decision: str, evidence: str):
        return ((key, decision, evidence, trace_id, read_only)
                if trace_id else None)

    transform = _write_transform(src_crs_authid, dst_crs)
    if transform is None:
        _log_decision(signature("refused", "transform_unavailable"),
                      f"{head('refused')} evidence=transform_unavailable "
                      f"(capture CRS unknown to this QGIS: the reprojection "
                      f"story cannot be tested){tail}", "WARNING")
        return CrsVerdict("transform_failed", None, geom, None,
                          "transform_unavailable")

    repro = _reprojected(geom, transform)
    raw_box = _finite_box(geom)
    repro_box = _finite_box(repro)
    if raw_box is not None and repro_box is not None \
            and _same_box(raw_box, repro_box):
        _log_decision(signature("equivalent", "same_position"),
                      f"{head('equivalent')} evidence=same_position "
                      f"raw={_box_center(raw_box)}{tail}", "INFO")
        return CrsVerdict("equivalent_crs", geom, geom, repro, "same_position")

    decision, evidence, facts = _decide_crs_story(
        geom, repro, dst_crs, layer, transform, witnesses, key, stories,
        carriers)
    facts_txt = " ".join(f"{k}={v}" for k, v in facts.items())
    _log_decision(signature(decision, evidence),
                  f"{head(decision)} evidence={evidence} {facts_txt} "
                  f"raw={_box_center(raw_box)} repro={_box_center(repro_box)}"
                  f"{tail}", "INFO" if evidence == "story" else "WARNING")
    if decision == "relabel":
        return CrsVerdict("relabeled", geom, geom, repro, evidence)
    if decision == "reprojection":
        return CrsVerdict("reprojected", repro, geom, repro, evidence)
    return CrsVerdict("crs_ambiguous", None, geom, repro, evidence)


def geometry_for_layer_crs(geom, src_crs_authid: Optional[str], layer,
                           trace_id: str = "", witnesses=(),
                           carriers=None) -> Tuple[object, str]:
    """Express *geom* in the CRS of *layer*: `crs_verdict` as a pair.

    Used before a geometry is written back (the rewind's
    `restore_executor._geometry_in_layer_crs`, the event restore's
    `restore_service._geometry_for_write`) AND before it is compared with
    live features (`restore_service._expected_geometry`: identity proofs,
    duplicate scans). Both go through the same story, so a feature proven
    under one story is never written under the other.

    Returns ``(geometry, status)``:
        (geom, "same_crs")      both CRS agree; *geom* is untouched, so a
                                capture made in the layer CRS costs nothing.
        (geom, "unknown_crs")   the event or the layer has no usable CRS
                                (legacy event, non-spatial layer):
                                nothing can be decided, geometry as-is.
        (geom, "empty")         no coordinate a CRS could misplace.
        (geom, "equivalent_crs")  both stories give the same position (two
                                ids for one definition): *geom* untouched.
        (geom, "relabeled")     the evidence says relabel: raw coordinates,
                                untouched.
        (reprojected, "reprojected")  the evidence says reprojection.
        (reading, "crs_ambiguous")  the evidence cannot tell a relabel from
                                a reprojection. NOTHING MAY BE WRITTEN: a
                                caller that writes refuses on the status
                                (reason_code ``crs_ambiguous``,
                                `CRS_REFUSED_STATUSES`). *reading* is the
                                reprojected one (None when it cannot be
                                computed), for comparisons only -- the
                                reading every version before the story
                                existed used. It cannot match a committed
                                feature exactly (such a feature would have
                                been a witness and settled the story), so a
                                proof resting on the geometry fails instead
                                of being skipped: an "ambiguous" answer
                                used to be None, "no geometry to compare",
                                and the undo of an INSERT then deleted, on
                                its attributes alone, an unrelated feature
                                3 km away.
        (None, "transform_failed")  the capture CRS is not usable here (not
                                installed locally, unknown id): the
                                reprojection story cannot even be tested,
                                the caller MUST refuse to write.

    *carriers*: a `CaptureCarriers` a batch of writes on *layer* loaded
    once (the evidence the Review reads, from the same passes); None: one
    provider query per witness reading. The stories are always the
    writes' own (`_CRS_STORIES`).
    """
    verdict = crs_verdict(geom, src_crs_authid, layer, trace_id, witnesses,
                          carriers=carriers)
    if verdict.status == "crs_ambiguous":
        return verdict.reprojected, verdict.status
    return verdict.geometry, verdict.status


def crs_refusal(status: str, src_crs_authid: Optional[str], layer) -> Tuple[str, str]:
    """(reason_code, message) for a geometry `geometry_for_layer_crs` refused.

    One wording for both engines (rewind and event-by-event restore), so a
    refusal reads the same whichever button the user pressed.
    """
    try:
        dst_label = _crs_label(layer.crs())
    except (AttributeError, RuntimeError):
        dst_label = "unknown CRS"
    if status == "crs_ambiguous":
        return "crs_ambiguous", (
            f"Geometry captured in {src_crs_authid}, layer now in "
            f"{dst_label}: cannot tell a relabel (same coordinates, CRS "
            f"declaration corrected) from a reprojection (coordinates "
            f"converted) from the evidence available (features still carrying "
            f"a captured position, CRS area of use); nothing written"
        )
    return "crs_mismatch", (
        f"Geometry captured in {src_crs_authid} cannot be converted to "
        f"{dst_label}; nothing written"
    )


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


def _cheap_shape_signature(geom):
    """(vertex count, area, length) -- or None when unavailable.

    A REJECT filter in front of `_canonical_rings`, which is the only
    honest comparison but materialises and sorts every vertex IN PYTHON.
    On a 200-feature layer of 500 KB multipolygons that is 6.3 million
    point tuples per scan: measured 48 s for ONE event on zone_mkt_rip,
    against 2.6 s before the canonical comparison existed. The three
    quantities here are computed by QGIS in C++ and are invariant to ring
    orientation, starting vertex and part order -- exactly what the
    canonical comparison exists to see through -- so a mismatch is a
    proof of difference and costs nothing.

    Fails OPEN (returns None) so an uncomparable geometry falls through to
    the expensive path rather than being wrongly rejected.
    """
    try:
        n = geom.constGet().nCoordinates()
        return (int(n), float(geom.area()), float(geom.length()))
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None


def _signatures_differ(a, b, rel_tol: float = 1e-9) -> bool:
    """True when two cheap signatures prove the shapes differ.

    Vertex counts must match exactly. Area and length are compared with a
    relative tolerance: rotating a ring reorders the terms of the sums, so
    the same shape can differ in the last bits of a double.
    """
    if a is None or b is None:
        return False
    if a[0] != b[0]:
        return True
    for x, y in ((a[1], b[1]), (a[2], b[2])):
        scale = max(abs(x), abs(y), 1.0)
        if abs(x - y) > scale * rel_tol:
            return True
    return False


def _canonical_ring_cycle(pts: list) -> tuple:
    """Dihedral canonical cycle of a closed polygon ring (without duplicate end)."""
    if len(pts) <= 2:
        return tuple(sorted(pts))
    min_pt = min(pts)
    candidates = []
    for seq in (pts, pts[::-1]):
        for idx, pt in enumerate(seq):
            if pt == min_pt:
                candidates.append(tuple(seq[idx:] + seq[:idx]))
    return min(candidates)


def _canonical_rings(geom):
    """Canonical, GEOS-free shape signature, or None when unavailable.

    Rebuilds polygons as sorted parts of dihedral-canonical rings, dropping
    each ring's closing vertex. That makes the signature invariant to the
    three things OGR rewrites when it repacks a shapefile (ring orientation,
    starting vertex, part order) while rejecting vertex permutations and
    preserving the direction of linear geometries.
    """
    try:
        if geom.isMultipart():
            parts = geom.asMultiPolygon()
        else:
            poly = geom.asPolygon()
            parts = [poly] if poly else []
    except (AttributeError, RuntimeError, TypeError):
        parts = []

    if parts:
        out = []
        for rings in parts:
            canon = []
            for ring in rings:
                pts = [(p.x(), p.y()) for p in ring]
                if len(pts) > 1 and pts[0] == pts[-1]:
                    pts = pts[:-1]
                canon.append(_canonical_ring_cycle(pts))
            out.append(tuple(sorted(canon)))
        return tuple(sorted(out)) if out else None

    try:
        lines = geom.asMultiPolyline() if geom.isMultipart() else (
            [geom.asPolyline()] if geom.asPolyline() else [])
    except (AttributeError, RuntimeError, TypeError):
        lines = []
    if lines:
        out_lines = [tuple((p.x(), p.y()) for p in line)
                     for line in lines if line]
        return tuple(sorted(out_lines)) if out_lines else None

    try:
        pts = [(v.x(), v.y()) for v in geom.vertices()]
    except (AttributeError, RuntimeError, TypeError):
        return None
    return tuple(sorted(pts)) if pts else None


def feature_matches_geometry(feature, expected_geom, expected_wkb=None,
                             allow_repair: bool = False) -> bool:
    """Return True when *feature*'s geometry matches *expected_geom*.

    Comparison shared between the snapshot scanners in restore_service
    and the buffer ops in restore_executor (DUP-10). Ordered by cost:

      0. Bounding-box fast reject (BL-RW-P1-26): two geometries with
         different bounding boxes cannot be equal, and the bbox compare
         avoids both the WKB byte copy and the GEOS construction that
         dominated the rewind apply phase on heavy polygons (measured
         55s/59s on zone_mkt_rip, 2026-07-05). Tolerant, see _bbox_close.
      1. Byte-for-byte WKB equality (fast path; the audit pipeline
         re-serialises geometries the same way QGIS does).
      2. Canonical ring signature (BL-RW-P5-28). Byte equality and
         ``equals()`` are BOTH sensitive to ring orientation and starting
         vertex, which OGR rewrites whenever it repacks a shapefile. On
         the user's run of 2026-08-14 that cost 16 refusals on features
         that were sitting in the layer, unchanged, under a different
         encoding of the same vertices.
      3. ``makeValid`` on both sides then ``isGeosEqual`` -- last resort,
         only when *allow_repair* is set. It costs ~900 ms per pair on a
         500 KB multipolygon, which is affordable to verify ONE candidate
         and ruinous inside a 200-feature scan; hence the opt-in rather
         than a default. ``equals()`` alone is not a substitute: these
         geometries are GEOS-invalid at the source and it answers False
         on two encodings of the same shape.

    *expected_wkb* lets per-event scan loops serialise the expected
    geometry ONCE instead of once per scanned feature.
    """
    if not is_geometry_present(expected_geom):
        return False
    current = feature.geometry()
    if not is_geometry_present(current):
        return False
    try:
        if current.boundingBox() != expected_geom.boundingBox():
            return False
    except (AttributeError, RuntimeError, TypeError):
        pass
    if expected_wkb is None:
        expected_wkb = bytes(expected_geom.asWkb())
    if geometries_equal(bytes(current.asWkb()), expected_wkb):
        return True

    if not _signatures_differ(_cheap_shape_signature(current),
                              _cheap_shape_signature(expected_geom)):
        cur_rings = _canonical_rings(current)
        if cur_rings is not None and cur_rings == _canonical_rings(expected_geom):
            return True

    if hasattr(current, "equals") and bool(current.equals(expected_geom)):
        return True

    if allow_repair:
        try:
            a, b = current.makeValid(), expected_geom.makeValid()
            if (is_geometry_present(a) and is_geometry_present(b)
                    and a.isGeosEqual(b)):
                return True
        except (AttributeError, RuntimeError, TypeError):
            pass
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
        except Exception:  # nosec B110 # justification below
            # Deliberately silent, and it must stay that way:
            #  1. this module keeps no module-level logger import so it
            #     stays importable without QGIS (every qgis/logger import
            #     here is function-local), and importing flog from inside
            #     this handler would raise on the very environments the
            #     handler exists to survive;
            #  2. this function is a log-message formatter whose contract
            #     is to never raise -- emitting a log line while a caller
            #     is building a log line inverts the record order.
            # Nothing is lost: the caller logs our return value, and the
            # fallthrough is visible in it (CENTROID(...)/WKB(len=...)
            # instead of POINT(...)).
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


def feature_geom_short_repr_diag(layer, fid: int) -> str:
    """Diag-gated variant of ``feature_geom_short_repr``.

    When ``RECOVERLAND_HEAVY_DIAG`` is off (default), returns ``"diag_off"``
    without touching the provider, so the per-event log placeholder is
    preserved but the provider.getFeatures() round-trip is skipped.
    Cuts the UI thread blocking time on large rewinds.

    Use this wrapper for log-only call sites (BUF_INS, TRACE_RESTORE,
    UNDO_UPD). Keep the original ``feature_geom_short_repr`` for the
    rare functional sites where the persisted geometry must be read
    regardless of the diag flag.
    """
    if not _HEAVY_DIAG:
        return "diag_off"
    return feature_geom_short_repr(layer, fid)


def reproject_bbox(
    bbox_xy: Tuple[float, ...], src_crs: str, dst_crs: str,
) -> Tuple[float, ...]:
    """Reproject a bounding box tuple (xmin, ymin, xmax, ymax) between CRS.

    Returns the original bbox on error or if CRS are identical.
    Consolidated from review session and render worker (BL-DIAG-P1-10).
    """
    if not dst_crs or dst_crs == src_crs:
        return bbox_xy
    try:
        from qgis.core import (  # noqa: PLC0415
            QgsCoordinateReferenceSystem,
            QgsCoordinateTransform,
            QgsGeometry,
            QgsProject,
            QgsRectangle,
        )
        rect = QgsRectangle(bbox_xy[0], bbox_xy[1], bbox_xy[2], bbox_xy[3])
        geom = QgsGeometry.fromRect(rect)
        xform = QgsCoordinateTransform(
            QgsCoordinateReferenceSystem(src_crs),
            QgsCoordinateReferenceSystem(dst_crs),
            QgsProject.instance(),
        )
        geom.transform(xform)
        b = geom.boundingBox()
        return (b.xMinimum(), b.yMinimum(), b.xMaximum(), b.yMaximum())
    except Exception:  # noqa: BLE001
        return bbox_xy


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
