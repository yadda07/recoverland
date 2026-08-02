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
    return hashlib.md5(wkb).digest(), bbox


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

    def _build(self) -> None:
        """Single full pass, geometries materialised once."""
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
                       expected_wkb: Optional[bytes] = None) -> List[int]:
        """Fids plausibly equal to expected_geom (md5 OR exact bbox match).

        Callers MUST verify each candidate live before acting: digests
        pre-filter, they do not prove equality.
        """
        if not self.usable:
            return []
        if expected_wkb is not None and is_geometry_present(expected_geom):
            try:
                exp_md5 = hashlib.md5(expected_wkb).digest()
                rect = expected_geom.boundingBox()
                exp_bbox = (
                    round(rect.xMinimum(), _BBOX_PRECISION),
                    round(rect.yMinimum(), _BBOX_PRECISION),
                    round(rect.xMaximum(), _BBOX_PRECISION),
                    round(rect.yMaximum(), _BBOX_PRECISION),
                )
            except (AttributeError, RuntimeError, TypeError):
                return []
        else:
            digest = _geom_digest(expected_geom)
            if digest is None:
                return []
            exp_md5, exp_bbox = digest
        return [
            fid for fid, (md5, bbox) in self._by_fid.items()
            if md5 == exp_md5 or bbox == exp_bbox
        ]

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
