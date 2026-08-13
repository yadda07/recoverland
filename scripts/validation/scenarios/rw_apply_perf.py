"""Scenario BL-RW-P1-26: rewind apply must not freeze the UI nor roll back
layers on unverifiable targets.

Offline proof on the REAL modules (qgis shim):
  1. _classify_restore_result: target_unverifiable is SOFT (target_absent),
     buffer_refused stays HARD (antithesis: soft set must not widen).
  2. _chunk_should_yield: progress guaranteed (never yields at n_done=0),
     yields at budget or max_actions.
  3. feature_matches_geometry: bbox fast-reject skips WKB + GEOS equals
     on non-matches; encoding variants (same bbox, different WKB) still
     reach equals(); precomputed expected_wkb is honoured.
  4. _find_by_attrs_only requests NoGeometry + attribute subset and still
     rescues the drifted feature.
"""
import sys
from pathlib import Path

SCENARIO_ID = "rw_apply_perf"
INVARIANT = "BL-RW-P1-26"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS_PARENT = _PLUGIN_ROOT.parent
if str(_PLUGINS_PARENT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_PARENT))

from scripts.validation.scenarios.rw_trace_lifecycle import _install_qgis_shim  # noqa: E402
from scripts.validation.scenarios.rw_insert_comp_rescue import (  # noqa: E402
    FakeLayer, _mk_event, _FIELDS, FakeFeature,
)


class SpyGeom:
    def __init__(self, bbox, wkb, equals_result=False):
        self._bbox = bbox
        self._wkb = wkb
        self._equals_result = equals_result
        self.aswkb_calls = 0
        self.equals_calls = 0

    def isNull(self):
        return False

    def isEmpty(self):
        return False

    def boundingBox(self):
        return self._bbox

    def asWkb(self):
        self.aswkb_calls += 1
        return self._wkb

    def equals(self, other):
        self.equals_calls += 1
        return self._equals_result


class SpyFeature:
    def __init__(self, geom):
        self._geom = geom

    def geometry(self):
        return self._geom


class RecordingRequest:
    instances = []

    def __init__(self, *args):
        self.flags = None
        self.subset = None
        RecordingRequest.instances.append(self)

    def setFlags(self, flags):
        self.flags = flags

    def setSubsetOfAttributes(self, idx_list):
        self.subset = list(idx_list)

    def setFilterRect(self, rect):
        pass


def setup(ctx):
    import importlib
    try:
        import qgis  # noqa: F401
    except ImportError:
        _install_qgis_shim()
    import recoverland.core.geometry_utils as gu
    import recoverland.core.restore_service as rs
    import recoverland.restore_runner as rr
    importlib.reload(gu)
    importlib.reload(rs)
    importlib.reload(rr)
    ctx.data["gu"] = gu
    ctx.data["rs"] = rs
    ctx.data["rr"] = rr
    ctx.data["rr_source"] = (
        _PLUGIN_ROOT / "restore_runner.py").read_text(encoding="utf-8")


def run(ctx):
    rs = ctx.data["rs"]
    gu = ctx.data["gu"]
    rr = ctx.data["rr"]

    # 1. Classification FIX-A.
    def mk(code):
        return {"success": False, "status": "FAILED",
                "reason_code": code, "message": "x"}

    ctx.data["cls_unverifiable"] = rs._classify_restore_result(
        mk("target_unverifiable"))
    ctx.data["cls_buffer_refused"] = rs._classify_restore_result(
        mk("buffer_refused"))
    ctx.data["cls_skip"] = rs._classify_restore_result(
        {"success": True, "skipped": True, "status": "SKIPPED_IDEMPOTENT",
         "reason_code": "absent_vs_insert_comp", "message": "x"})

    # 2. Chunk yield policy.
    ctx.data["yield_zero_done"] = rr._chunk_should_yield(10_000.0, 0)
    ctx.data["yield_under_budget"] = rr._chunk_should_yield(39.0, 5)
    ctx.data["yield_at_budget"] = rr._chunk_should_yield(40.0, 1)
    ctx.data["yield_max_actions"] = rr._chunk_should_yield(0.0, 50)

    # 3. Geometry fast-reject.
    heavy = b"\x01" * 64
    exp = SpyGeom(bbox=(0, 0, 10, 10), wkb=heavy)
    cur_far = SpyGeom(bbox=(90, 90, 99, 99), wkb=b"\x02" * 64)
    r_reject = gu.feature_matches_geometry(SpyFeature(cur_far), exp)
    ctx.data["reject_result"] = r_reject
    ctx.data["reject_no_geos"] = (cur_far.equals_calls == 0
                                  and cur_far.aswkb_calls == 0)

    cur_same = SpyGeom(bbox=(0, 0, 10, 10), wkb=heavy)
    ctx.data["match_wkb"] = gu.feature_matches_geometry(
        SpyFeature(cur_same), exp, expected_wkb=bytes(heavy))
    ctx.data["match_wkb_no_expected_serialize"] = exp.aswkb_calls == 0

    exp2 = SpyGeom(bbox=(0, 0, 10, 10), wkb=heavy)
    cur_variant = SpyGeom(bbox=(0, 0, 10, 10), wkb=b"\x03" * 64,
                          equals_result=True)
    ctx.data["variant_match"] = gu.feature_matches_geometry(
        SpyFeature(cur_variant), exp2)
    ctx.data["variant_used_geos"] = cur_variant.equals_calls == 1

    # 4. attr_rescue request shape + behaviour preserved. The recording
    # class replaces qgis.core.QgsFeatureRequest ONLY around this call
    # (try/finally restore: a live QGIS session must never keep the fake).
    RecordingRequest.instances.clear()
    stress_attrs = {"sro": "stress_fddf4652", "zs_refpm": "stress_5bf53cfc",
                    "commentair": None, "exe": 3608}
    layer = FakeLayer(_FIELDS, [
        FakeFeature(11, ["63398/A2D", "PM12", None, 100]),
        FakeFeature(12, ["stress_fddf4652", "stress_5bf53cfc", None, 3608]),
    ])
    qcore = sys.modules["qgis.core"]
    original_request = getattr(qcore, "QgsFeatureRequest", None)
    qcore.QgsFeatureRequest = RecordingRequest
    try:
        ctx.data["rescue_fid"] = rs._find_by_attrs_only(
            layer, _mk_event(stress_attrs))
    finally:
        if original_request is not None:
            qcore.QgsFeatureRequest = original_request
    req = RecordingRequest.instances[-1] if RecordingRequest.instances else None
    from recoverland.compat import QgisCompat
    ctx.data["req_no_geometry"] = (
        req is not None and req.flags is QgisCompat.NO_GEOMETRY)
    ctx.data["req_subset"] = req.subset if req is not None else None


def assertions(ctx):
    src = ctx.data["rr_source"]
    strict_body = src.split("def _process_strict_chunk_inner", 1)[-1]
    return [
        ("unverifiable_is_soft",
         ctx.data["cls_unverifiable"] == "target_absent",
         f"cls={ctx.data['cls_unverifiable']} expected target_absent "
         "(refusal without buffer mutation must not roll back the layer)"),
        ("neg_buffer_refused_stays_hard",
         ctx.data["cls_buffer_refused"] == "failed",
         f"cls={ctx.data['cls_buffer_refused']} expected failed"),
        ("neg_idempotent_skip_unchanged",
         ctx.data["cls_skip"] == "skipped_idempotent",
         f"cls={ctx.data['cls_skip']}"),
        ("neg_chunk_never_yields_before_progress",
         ctx.data["yield_zero_done"] is False,
         f"yield(10s, n_done=0)={ctx.data['yield_zero_done']} expected False"),
        ("chunk_respects_budget",
         ctx.data["yield_under_budget"] is False
         and ctx.data["yield_at_budget"] is True,
         f"under={ctx.data['yield_under_budget']} at={ctx.data['yield_at_budget']}"),
        ("chunk_caps_actions",
         ctx.data["yield_max_actions"] is True,
         f"max_actions={ctx.data['yield_max_actions']}"),
        ("bbox_fast_reject_skips_geos",
         ctx.data["reject_result"] is False and ctx.data["reject_no_geos"],
         f"result={ctx.data['reject_result']} "
         f"no_geos={ctx.data['reject_no_geos']} (bbox differ must cost 0 "
         "WKB copy and 0 GEOS equals)"),
        ("expected_wkb_hoisted",
         ctx.data["match_wkb"] is True
         and ctx.data["match_wkb_no_expected_serialize"],
         f"match={ctx.data['match_wkb']} "
         f"no_serialize={ctx.data['match_wkb_no_expected_serialize']}"),
        ("neg_encoding_variant_still_matches",
         ctx.data["variant_match"] is True and ctx.data["variant_used_geos"],
         f"match={ctx.data['variant_match']} "
         f"geos={ctx.data['variant_used_geos']} (same bbox, different WKB "
         "must still reach equals())"),
        ("attr_rescue_requests_no_geometry",
         ctx.data["req_no_geometry"] and ctx.data["req_subset"] == [0, 1, 3],
         f"no_geom={ctx.data['req_no_geometry']} "
         f"subset={ctx.data['req_subset']} expected [0, 1, 3]"),
        ("attr_rescue_behaviour_preserved",
         ctx.data["rescue_fid"] == 12,
         f"rescue_fid={ctx.data['rescue_fid']} expected 12"),
        ("strict_runner_budget_wired",
         "_chunk_should_yield(" in strict_body
         and "while i < len(actions):" in strict_body
         and "end = min(start + _CHUNK_SIZE, len(actions))" not in strict_body,
         "StrictRestoreRunner must loop with the time-budget yield, not a "
         "fixed-size slice"),
    ]
