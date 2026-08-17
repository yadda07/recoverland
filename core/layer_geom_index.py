"""Per-layer geometry index for rewind snapshot scans (BL-RW-P1-27).

Problem (measured 2026-07-06, trace 506233ab): every compensatory action
that needs a geometry scan (_find_by_snapshot, _find_existing_by_snapshot)
iterated the WHOLE layer and materialised every heavy geometry AGAIN.
On zone_mkt_rip (132 actions x 193 heavy multipolygons) the apply phase
alone took 95s of the 122s total.

Design:
  - One lazy pass per layer materialises each geometry ONCE and stores a
    compact digest per feature: (md5(wkb), bbox tuple). ~60 bytes/feature.
  - candidate_fids(geom) returns fids whose digest md5 matches (byte-equal
    WKB fast path) OR whose bbox matches exactly (encoding variants of the
    same shape share vertices hence bbox). Callers MUST re-verify each
    candidate live (fetch by fid + feature_matches_geometry): the index is
    a pre-filter, never a proof.
  - The editing buffer mutates during a batch: callers notify mutations
    via evict(fid) / reindex(fid, geom) so stale digests are never served.

The index intentionally lives OUTSIDE QgsSpatialIndex: exact equality
lookup needs digests, not intersection, and QgsSpatialIndex would still
require materialising candidate geometries for the final compare.
"""
import hashlib
import time
from typing import Dict, List, Optional, Tuple

from .logger import flog
from .geometry_utils import is_geometry_present, get_feature_source

_BBOX_PRECISION = 9


def _md5_digest(payload: bytes) -> bytes:
    """MD5 of `payload` as a CONTENT fingerprint -- never a secret.

    `usedforsecurity=False` states that intent to hashlib: no security
    decision rests on this digest, it only answers "are these two WKB
    blobs the same bytes?". It also lets the index work on a FIPS-enabled
    OpenSSL, where the bare md5 constructor refuses to run at all.

    The keyword reached hashlib in Python 3.9, which is the floor QGIS
    3.40 still ships on some Linux packages, so the plain call is kept as
    a fallback for anything older. Both branches return the SAME bytes:
    the flag selects an implementation policy, not the algorithm. That
    equality is load-bearing -- digests computed off-thread by
    layer_index_thread are compared against digests computed here.
    """
    try:
        return hashlib.md5(payload, usedforsecurity=False).digest()
    except TypeError:
        # Python < 3.9 has no such keyword. A TypeError caused instead by
        # a payload of the wrong type re-raises identically below, so the
        # caller sees the same exception it always did.
        # B324 is suppressed on the next line only: same fingerprint role
        # as above, and the keyword that would express it is unavailable
        # precisely on the interpreters this branch exists for.
        return hashlib.md5(payload).digest()  # nosec B324


def _geom_digest(geom) -> Optional[Tuple[bytes, Tuple]]:
    """Return (md5_of_wkb, rounded_bbox_tuple) or None if geom is absent."""
    if not is_geometry_present(geom):
        return None
    try:
        wkb = bytes(geom.asWkb())
        rect = geom.boundingBox()
        bbox = (
            round(rect.xMinimum(), _BBOX_PRECISION),
            round(rect.yMinimum(), _BBOX_PRECISION),
            round(rect.xMaximum(), _BBOX_PRECISION),
            round(rect.yMaximum(), _BBOX_PRECISION),
        )
    except (AttributeError, RuntimeError, TypeError) as exc:
        flog(f"layer_geom_index: digest_failed type={type(exc).__name__} "
             f"msg={exc}", "WARNING")
        return None
    return _md5_digest(wkb), bbox


class LayerGeomIndex:
    """Lazy exact-match geometry pre-filter for one layer's batch.

    Thread affinity: main thread only (same constraint as the editing
    buffer it mirrors). One instance per (layer, batch): never reuse
    across batches or after commit.
    """

    def __init__(self, layer, label: str = ""):
        self._layer = layer
        self._label = label or getattr(layer, "name", lambda: "?")()
        self._by_fid: Dict[int, Tuple[bytes, Tuple]] = {}
        self._built = False
        self._build_failed = False

    @property
    def built(self) -> bool:
        return self._built

    def adopt(self, by_fid: Dict[int, Tuple[bytes, Tuple]]) -> None:
        """Install digests computed elsewhere (BL-RW-P5-29).

        The single full pass costs ~800 ms of pure OGR/GEOS work on the
        user's layer. Done on the GUI thread it is an indivisible stall;
        done in a worker it is invisible, because sip releases the GIL
        around every C++ call (measured: animation frame interval 16.0 ms
        during the worker read, against 43.2 ms with the current
        chunked-on-GUI-thread apply). This is the door for the worker to
        hand its result over; the digests are plain tuples, so nothing
        Qt-owned crosses the thread boundary.
        """
        self._by_fid = dict(by_fid)
        self._built = True
        self._build_failed = False
        flog(f"layer_geom_index: adopted layer={self._label!r} "
             f"n_indexed={len(self._by_fid)} source=worker")

    def _build(self) -> None:
        """Single full pass, geometries materialised once.

        Synchronous fallback for callers with no worker (validation
        scenarios, the event-by-event path). Prefer `adopt`.
        """
        t0 = time.monotonic()
        n_feats = 0
        n_indexed = 0
        try:
            from qgis.core import QgsFeatureRequest
            request = QgsFeatureRequest()
            try:
                request.setSubsetOfAttributes([])
            except (AttributeError, TypeError):
                pass
            source = get_feature_source(self._layer)
            for feature in source(request):
                n_feats += 1
                digest = _geom_digest(feature.geometry())
                if digest is not None:
                    self._by_fid[feature.id()] = digest
                    n_indexed += 1
        except Exception as exc:
            # Fail open: callers fall back to the live full scan.
            self._build_failed = True
            flog(f"layer_geom_index: build_failed layer={self._label!r} "
                 f"type={type(exc).__name__} msg={exc}", "WARNING")
            return
        finally:
            self._built = True
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        flog(f"layer_geom_index: built layer={self._label!r} "
             f"n_features={n_feats} n_indexed={n_indexed} "
             f"elapsed_ms={elapsed_ms:.0f}")

    @property
    def usable(self) -> bool:
        """False when the build failed: callers must use the live scan."""
        if not self._built:
            self._build()
        return not self._build_failed

    def candidate_fids(self, expected_geom,
                       expected_wkb: Optional[bytes] = None
                       ) -> Optional[List[int]]:
        """Fids plausibly equal to expected_geom, BEST FIRST.

        Returns ``None`` when the index cannot answer (build failed, or the
        expected geometry has no digest) and a LIST when it can -- possibly
        empty, meaning "I looked at every feature and none matches".
        BL-RW-P5-29: the two used to be the same value, and an appelant
        reading ``[]`` as "absent" turned a failed build into a silent
        SKIPPED_IDEMPOTENT. `None` means "I do not know, go and scan".

        Order matters as much as content. md5 candidates come FIRST: on the
        user's layer 194 of 210 features share one bounding box, so a
        bbox-matching list is the whole layer and re-verifying it costs the
        scan it was meant to replace (measured 1076 ms). Serving the byte
        -identical candidate first brings that to 14.4 ms.

        Callers MUST verify each candidate live before acting: digests
        pre-filter, they do not prove equality.
        """
        if not self.usable:
            return None
        if expected_wkb is not None and is_geometry_present(expected_geom):
            try:
                exp_md5 = _md5_digest(expected_wkb)
                rect = expected_geom.boundingBox()
                exp_bbox = (
                    round(rect.xMinimum(), _BBOX_PRECISION),
                    round(rect.yMinimum(), _BBOX_PRECISION),
                    round(rect.xMaximum(), _BBOX_PRECISION),
                    round(rect.yMaximum(), _BBOX_PRECISION),
                )
            except (AttributeError, RuntimeError, TypeError):
                return None
        else:
            digest = _geom_digest(expected_geom)
            if digest is None:
                return None
            exp_md5, exp_bbox = digest
        exact, by_bbox = [], []
        for fid, (md5, bbox) in self._by_fid.items():
            if md5 == exp_md5:
                exact.append(fid)
            elif bbox == exp_bbox:
                by_bbox.append(fid)
        return exact + by_bbox

    def drop(self) -> None:
        """Make the index permanently unusable for this batch.

        Called at every commit and rollback. commitChanges renumbers the
        provider FIDs AND moves their content (measured: old fid 4 becomes
        new fid 3), so a surviving index knows the right geometries under
        the wrong fids. The live re-verification then rejects the proposed
        fid and the caller concludes "absent" -- the renumbering turns
        harmless false positives into SILENT FALSE NEGATIVES, which is the
        one failure mode re-verification cannot catch. QGIS publishes no
        old->new mapping for untouched rows, so this is not repairable:
        the only safe answer is to stop answering.
        """
        self._by_fid.clear()
        self._built = True
        self._build_failed = True

    def evict(self, fid: int) -> None:
        """Drop a mutated/deleted fid: it will never be served stale."""
        self._by_fid.pop(fid, None)

    def reindex(self, fid: int, geom) -> None:
        """Re-digest a fid after changeGeometry/addFeature succeeded."""
        digest = _geom_digest(geom)
        if digest is None:
            self._by_fid.pop(fid, None)
            return
        self._by_fid[fid] = digest

    def __len__(self) -> int:
        return len(self._by_fid)
