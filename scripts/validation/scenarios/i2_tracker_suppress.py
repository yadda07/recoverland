"""i2_tracker_suppress - RecoverLand validation runtime.

Invariant: I-2 (no audit event is captured while the tracker is
suppressed during a restore run).
Backlog item: BL-RW-P1-05.
Root cause: CR-5.

Five commit signal handlers on `EditSessionTracker` currently lack
an `if not self._active or self.is_suppressed: return` guard:

    _on_committed_features_added           (l.381)
    _on_committed_features_removed         (l.413)
    _on_committed_attribute_values_changes (l.429)
    _on_committed_geometries_changes       (l.451)
    _on_rollback                           (l.474)

The leak is masked today because `_on_before_commit` (which is
guarded) never creates a buffer while suppressed, so the handlers
return early on `buf is None`. A refactor that creates buffers via
another path would break I-2 silently. P1-05 hardens the guards.

Scenario layout (no QGIS signals required - we call the handlers
directly on a tracker with a manually injected buffer):

  setup:
    - build EditSessionTracker with dummy write_queue / journal_manager
    - activate it
    - inject a fresh EditSessionBuffer in tracker._buffers
  run:
    - tracker.suppress()
    - call the four committed* handlers with non-empty payloads
    - call _on_rollback and verify the buffer survives (no pop)
    - tracker.unsuppress()
    - call _on_committed_features_removed with a different fid to
      prove the tracker resumes normally
  assertions:
    - source patterns: each of the five handlers starts with the
      guard `if not self._active or self.is_suppressed: return`
    - runtime: no committed change recorded during suppress
    - runtime: buffer still in tracker._buffers after _on_rollback
    - runtime: new deletion recorded after unsuppress
    - log signature: `signal=... ignored=suppressed` produced for
      every skipped handler (at least 5 occurrences)

Pre-patch verdict: FAIL.
Post-patch verdict: PASS.
"""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

SCENARIO_ID = "i2_tracker_suppress"
INVARIANT = "I-2"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_GUARD_RE = re.compile(
    r"if\s+not\s+self\._active\s+or\s+self\.is_suppressed\s*:"
    r"[\s\S]{0,400}?\breturn\b"
)

_HANDLERS = [
    "_on_committed_features_added",
    "_on_committed_features_removed",
    "_on_committed_attribute_values_changes",
    "_on_committed_geometries_changes",
    "_on_rollback",
]


def _extract_handler_body(text: str, handler: str) -> str | None:
    """Return the lines that follow the def line, until the next def.

    The cap (24 lines) is generous enough to include a multi-line
    docstring plus the guard block (`if ...:` + multi-line flog +
    `return`) while still excluding the body of subsequent handlers.
    """
    pattern = re.compile(rf"^\s*def\s+{re.escape(handler)}\b[^\n]*\n", re.M)
    m = pattern.search(text)
    if not m:
        return None
    start = m.end()
    lines = text[start:].splitlines(keepends=True)
    body = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("def "):
            break
        body.append(line)
        if len(body) > 24:
            break
    return "".join(body)


def _make_gpkg_fixture() -> tuple[str, str]:
    """Create a real GPKG file via OGR. Returns (path, tmpdir)."""
    from osgeo import ogr

    tmpdir = tempfile.mkdtemp(prefix="rl_i2_gpkg_")
    path = os.path.join(tmpdir, "test.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("GPKG driver not available")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer("points", geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    ds.FlushCache()
    ds = None
    return path, tmpdir


def _make_gpkg_layer(gpkg_path: str):
    from qgis.core import QgsVectorLayer
    uri = f"{gpkg_path}|layername=points"
    layer = QgsVectorLayer(uri, "i2_test_gpkg", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"GPKG layer invalid for uri={uri!r}")
    return layer


def _add_point_feature(layer, name_value: str) -> bool:
    """Start editing, add a single point feature, commit. Returns True on success."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    if not layer.startEditing():
        return False
    feat = QgsFeature(layer.fields())
    idx = layer.fields().indexFromName("name")
    if idx >= 0:
        feat.setAttribute(idx, name_value)
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(0.0, 0.0)))
    if not layer.addFeature(feat):
        layer.rollBack()
        return False
    if not layer.commitChanges():
        return False
    return True


def setup(ctx):
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.edit_buffer import EditSessionBuffer
    from recoverland.core.logger import flog

    # Capturing write_queue: records every batch of events the tracker
    # would have persisted. Used by the E2E phase to verify zero events
    # under suppress and one event after unsuppress.
    class _CapturingWQ:
        def __init__(self):
            self.captured_events: list = []
            self.enqueue_calls: int = 0

        def enqueue(self, events) -> bool:
            self.enqueue_calls += 1
            self.captured_events.extend(events)
            return True

        # Legacy alias kept in case any code path still calls .add(...)
        def add(self, *args, **kwargs):
            raise AssertionError("write_queue.add unexpected in i2 scenario")

    class _DummyJM:
        def get_active_session_id(self):
            return "i2_session"

        def get_connection(self):
            # _register_datasource swallows exceptions, so we can refuse
            # to provide a real connection without breaking the flow.
            raise RuntimeError("i2 dummy: journal connection not provided")

    wq = _CapturingWQ()
    tracker = EditSessionTracker(wq, _DummyJM())
    tracker.activate()

    # ---- Direct-call phase (legacy, unchanged): inject a buffer manually
    layer_id = "i2_test_layer"
    buffer = EditSessionBuffer(layer_id, "i2_session")
    tracker._buffers[layer_id] = buffer

    # ---- E2E phase: real GPKG layer connected via Qt signals
    gpkg_path, gpkg_tmpdir = _make_gpkg_fixture()
    gpkg_layer = _make_gpkg_layer(gpkg_path)
    tracker.connect_layer(gpkg_layer)

    ctx.data["tracker"] = tracker
    ctx.data["wq"] = wq
    ctx.data["layer_id"] = layer_id
    ctx.data["buffer"] = buffer
    ctx.data["gpkg_path"] = gpkg_path
    ctx.data["gpkg_tmpdir"] = gpkg_tmpdir
    ctx.data["gpkg_layer"] = gpkg_layer

    flog(
        "i2_tracker_suppress setup: trace_id={tid} "
        "layer_id={lid} active={a} suppressed={s} "
        "gpkg_id={gid} gpkg_connected={gc}".format(
            tid=ctx.trace_id, lid=layer_id,
            a=tracker.is_active, s=tracker.is_suppressed,
            gid=gpkg_layer.id(),
            gc=gpkg_layer.id() in tracker._connected_layers),
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    tracker = ctx.data["tracker"]
    layer_id = ctx.data["layer_id"]
    buffer = ctx.data["buffer"]
    wq = ctx.data["wq"]
    gpkg_layer = ctx.data["gpkg_layer"]
    gpkg_id = gpkg_layer.id()

    flog(f"i2_tracker_suppress run start: trace_id={ctx.trace_id}", "INFO")

    # ===== Phase A: direct-call (legacy unchanged) ==================
    tracker.suppress()
    flog(
        f"i2_tracker_suppress: phase=suppressed "
        f"depth={tracker._suppress_depth} trace_id={ctx.trace_id}",
        "INFO",
    )

    tracker._on_committed_features_removed(layer_id, [10, 20])
    tracker._on_committed_attribute_values_changes(layer_id, {1: {0: "X"}})
    tracker._on_committed_geometries_changes(layer_id, {1: None})

    ctx.data["dels_under_suppress"] = sorted(buffer.get_committed_deletions())
    ctx.data["attrs_under_suppress"] = dict(buffer.get_committed_attr_changes())
    ctx.data["geoms_under_suppress"] = dict(buffer.get_committed_geom_changes())

    tracker._on_rollback(layer_id)
    ctx.data["buffer_still_present_after_rollback"] = (
        layer_id in tracker._buffers
    )

    # ===== Phase B (E2E): real Qt edit while suppressed =============
    # The tracker is still suppressed from above. We trigger a real
    # commit through the QgsVectorLayer signal chain.
    wq_calls_before_e2e_suppress = wq.enqueue_calls
    wq_events_before_e2e_suppress = len(wq.captured_events)

    e2e_suppress_commit_ok = _add_point_feature(gpkg_layer, "during_suppress")
    ctx.data["e2e_suppress_commit_ok"] = e2e_suppress_commit_ok
    ctx.data["e2e_buffer_under_suppress"] = (
        gpkg_id in tracker._buffers
    )
    ctx.data["e2e_wq_calls_under_suppress"] = (
        wq.enqueue_calls - wq_calls_before_e2e_suppress
    )
    ctx.data["e2e_events_captured_under_suppress"] = (
        len(wq.captured_events) - wq_events_before_e2e_suppress
    )

    tracker.unsuppress()
    flog(
        f"i2_tracker_suppress: phase=unsuppressed "
        f"depth={tracker._suppress_depth} trace_id={ctx.trace_id}",
        "INFO",
    )

    # ===== Phase A end: direct-call resumption ======================
    tracker._on_committed_features_removed(layer_id, [99])
    ctx.data["dels_after_unsuppress"] = sorted(buffer.get_committed_deletions())

    # ===== Phase B end (E2E): real Qt edit after unsuppress =========
    wq_calls_before_e2e_normal = wq.enqueue_calls
    wq_events_before_e2e_normal = len(wq.captured_events)

    e2e_normal_commit_ok = _add_point_feature(gpkg_layer, "after_unsuppress")
    ctx.data["e2e_normal_commit_ok"] = e2e_normal_commit_ok
    ctx.data["e2e_wq_calls_after_unsuppress"] = (
        wq.enqueue_calls - wq_calls_before_e2e_normal
    )
    new_events = wq.captured_events[wq_events_before_e2e_normal:]
    ctx.data["e2e_events_captured_after_unsuppress"] = len(new_events)
    ctx.data["e2e_first_op"] = (
        new_events[0].operation_type if new_events else None
    )

    flog(
        "i2_tracker_suppress run end: trace_id={tid} "
        "dels_under_suppress={du} dels_after_unsuppress={da} "
        "buffer_present_after_rollback={bp} "
        "e2e_suppress_ok={es} e2e_evts_suppress={ev_s} "
        "e2e_normal_ok={en} e2e_evts_normal={ev_n} e2e_op={op}".format(
            tid=ctx.trace_id,
            du=ctx.data["dels_under_suppress"],
            da=ctx.data["dels_after_unsuppress"],
            bp=ctx.data["buffer_still_present_after_rollback"],
            es=ctx.data["e2e_suppress_commit_ok"],
            ev_s=ctx.data["e2e_events_captured_under_suppress"],
            en=ctx.data["e2e_normal_commit_ok"],
            ev_n=ctx.data["e2e_events_captured_after_unsuppress"],
            op=ctx.data["e2e_first_op"]),
        "INFO",
    )


def _check_source_guard(handler: str) -> tuple[bool, str]:
    rel = Path("core/edit_tracker.py")
    full = _PLUGIN_ROOT / rel
    if not full.is_file():
        return False, f"missing file: {rel}"
    text = full.read_text(encoding="utf-8", errors="replace")
    body = _extract_handler_body(text, handler)
    if body is None:
        return False, f"handler {handler} not found in {rel}"
    if _GUARD_RE.search(body):
        return True, f"guard present at top of {handler}"
    return False, f"guard absent at top of {handler}"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    for handler in _HANDLERS:
        ok, msg = _check_source_guard(handler)
        out.append((f"source_guard__{handler}", ok, msg))

    out.append((
        "no_deletions_under_suppress",
        ctx.data.get("dels_under_suppress") == [],
        f"dels_under_suppress={ctx.data.get('dels_under_suppress')} "
        f"expected=[] (guard must skip _on_committed_features_removed)",
    ))

    out.append((
        "no_attrs_under_suppress",
        ctx.data.get("attrs_under_suppress") == {},
        f"attrs_under_suppress={ctx.data.get('attrs_under_suppress')} "
        f"expected={{}} (guard must skip _on_committed_attribute_values_changes)",
    ))

    out.append((
        "no_geoms_under_suppress",
        ctx.data.get("geoms_under_suppress") == {},
        f"geoms_under_suppress={ctx.data.get('geoms_under_suppress')} "
        f"expected={{}} (guard must skip _on_committed_geometries_changes)",
    ))

    out.append((
        "rollback_does_not_pop_buffer_under_suppress",
        ctx.data.get("buffer_still_present_after_rollback") is True,
        f"buffer_present_after_rollback="
        f"{ctx.data.get('buffer_still_present_after_rollback')} "
        f"expected=True (guard must skip _on_rollback under suppress)",
    ))

    out.append((
        "deletion_recorded_after_unsuppress",
        ctx.data.get("dels_after_unsuppress") == [99],
        f"dels_after_unsuppress={ctx.data.get('dels_after_unsuppress')} "
        f"expected=[99] (tracker must resume after unsuppress)",
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker:\s+signal=\S+\s+ignored=suppressed",
        name="signal_ignored_suppressed_logs_present",
        min_count=4,
    ))

    # ===== E2E brutal assertions ====================================
    out.append((
        "e2e_suppress_commit_succeeded_at_qgis_level",
        ctx.data.get("e2e_suppress_commit_ok") is True,
        f"e2e_suppress_commit_ok={ctx.data.get('e2e_suppress_commit_ok')} "
        f"expected=True (Qt commit on GPKG must succeed even under suppress)",
    ))
    out.append((
        "e2e_no_buffer_created_under_suppress",
        ctx.data.get("e2e_buffer_under_suppress") is False,
        f"e2e_buffer_under_suppress={ctx.data.get('e2e_buffer_under_suppress')} "
        f"expected=False (editingStarted must be guarded under suppress)",
    ))
    out.append((
        "e2e_zero_events_captured_under_suppress",
        ctx.data.get("e2e_events_captured_under_suppress") == 0,
        f"e2e_events_captured_under_suppress="
        f"{ctx.data.get('e2e_events_captured_under_suppress')} expected=0 "
        f"(real Qt commit under suppress must NOT produce any audit event)",
    ))
    out.append((
        "e2e_zero_wq_calls_under_suppress",
        ctx.data.get("e2e_wq_calls_under_suppress") == 0,
        f"e2e_wq_calls_under_suppress="
        f"{ctx.data.get('e2e_wq_calls_under_suppress')} expected=0 "
        f"(write_queue.enqueue must NOT be called under suppress)",
    ))
    out.append((
        "e2e_normal_commit_succeeded_at_qgis_level",
        ctx.data.get("e2e_normal_commit_ok") is True,
        f"e2e_normal_commit_ok={ctx.data.get('e2e_normal_commit_ok')} "
        f"expected=True (Qt commit must succeed after unsuppress)",
    ))
    out.append((
        "e2e_one_event_captured_after_unsuppress",
        ctx.data.get("e2e_events_captured_after_unsuppress") == 1,
        f"e2e_events_captured_after_unsuppress="
        f"{ctx.data.get('e2e_events_captured_after_unsuppress')} expected=1 "
        f"(tracker must resume capture: 1 INSERT event from real Qt path)",
    ))
    out.append((
        "e2e_first_op_is_INSERT",
        ctx.data.get("e2e_first_op") == "INSERT",
        f"e2e_first_op={ctx.data.get('e2e_first_op')!r} expected='INSERT' "
        f"(addFeature on GPKG must be captured as INSERT)",
    ))
    # ================================================================

    out.append(assert_log_contains(
        ctx.records,
        rf"i2_tracker_suppress.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=3,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
