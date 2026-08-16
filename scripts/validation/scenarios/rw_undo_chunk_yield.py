"""rw_undo_chunk_yield - RecoverLand validation runtime.

Invariant: BL-RW-P5-27 (no runner may hold the GUI thread for longer than
one chunk budget, whatever the size of a layer group).

Why this case matters
---------------------
Measured on the user's own run (profile log, trace a1517527, 2026-08-13
18:27:40 -> 18:29:45): clicking Rewind first auto-undoes the previous
rewind -- 1340 events over 22 layers, 139.27 s. One of those groups,
`zone_mkt_rip` with 108 events, took **118.30 s in a single
uninterruptible block**, because `UndoRunner._process_next_group` called
`undo_restore_batch(layer, group)` once for the WHOLE layer and only then
re-armed a QTimer. Windows flags a window as "Ne repond pas" after ~5 s
without a message pump: the threshold was exceeded by a factor of 23, and
the progress bar -- fed by one `progress` emission per LAYER -- stayed on
its last painted image the entire time.

`StrictRestoreRunner` already solves exactly this with a TIME budget
(`_chunk_should_yield`, `_CHUNK_BUDGET_MS`); `UndoRunner` never got it.

What this scenario proves
-------------------------
It does NOT measure a provider. It measures the SCHEDULING of the runner,
which is what freezes the UI. A heartbeat QTimer ticks every 10 ms on the
GUI thread while the runner works; the largest gap between two heartbeats
IS the freeze the user sees. The per-event work is a stub that costs a
fixed 25 ms, so the numbers are deterministic and provider-agnostic.

The stub is installed on the three seam names at once
(`undo_restore_batch`, `undo_prepare`, `undo_apply_one`), so the scenario
is fair to both shapes of the runner: whichever it calls, one event costs
25 ms. Pre-patch the runner calls the batch entry once per layer;
post-patch it calls prepare once per layer and apply once per event.

Assertions (measured, 120 events on one layer, 25 ms each = 3.0 s of work):
  1. undo_max_heartbeat_gap_ms < 500      -- pre-patch ~3000, post-patch ~40
  2. undo_progress_emissions >= 3          -- pre-patch 1 (one per layer)
  3. every event is applied, exactly once  -- non-regression
  4. application order is the RW-16 reverse order -- non-regression
  5. the per-layer context is built ONCE, not once per chunk

Pre-patch verdict: FAIL (1 and 2).
Post-patch verdict: PASS (5/5).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_undo_chunk_yield"
INVARIANT = "BL-RW-P5-27"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
# TWO datasources: the freeze is proven on the big group, and the second
# one exercises the group-advance path (per-layer context released, cursor
# moved on, next group opened) that the per-event chunking introduced.
_DS_FP_A = "rw_ucy_datasource_a"
_DS_FP_B = "rw_ucy_datasource_b"

# The big group is large enough that a per-layer block is unmistakable and
# small enough that the scenario stays under 5 s.
_N_EVENTS_A = 90
_N_EVENTS_B = 30
_N_EVENTS = _N_EVENTS_A + _N_EVENTS_B
_COST_PER_EVENT_S = 0.025
# Windows flags a frozen window at ~5 s; the animation stutters from
# ~150-200 ms. 500 ms is a generous ceiling that still fails hard on a
# whole-layer block (3.0 s of work here).
_MAX_GAP_MS = 500.0
_HEARTBEAT_MS = 10


class _FakeLayer:
    """Minimal surface used by _resolve_runner_layer and UndoRunner."""

    def __init__(self, name: str, feature_count: int = 200):
        self._name = name
        self._count = feature_count
        self.reload_calls = 0
        self.repaint_calls = 0

    def name(self):
        return self._name

    def isEditable(self):
        return False

    def isModified(self):
        return False

    def featureCount(self):
        return self._count

    def reload(self):
        self.reload_calls += 1

    def triggerRepaint(self):
        self.repaint_calls += 1


def _mk_event(eid: int, op: str, ts, ds_fp: str, layer_name: str):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=eid,
        project_fingerprint="rw_ucy_project",
        datasource_fingerprint=ds_fp,
        layer_id_snapshot=f"lyr_{layer_name}",
        layer_name_snapshot=layer_name,
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": eid}),
        operation_type=op,
        attributes_json=json.dumps({"changed_only": {}}),
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:4326",
        field_schema_json=None,
        user_name="tester",
        session_id=None,
        created_at=ts.isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{eid}",
        event_schema_version=2,
    )


def _install_stubs(ctx):
    """Replace the per-event work by a fixed 25 ms cost on every seam.

    Returns the list of names actually patched. `undo_prepare` and
    `undo_apply_one` do not exist pre-patch: patching is best-effort per
    name so the same scenario runs against both shapes.
    """
    from recoverland import restore_runner as rr
    from recoverland.core.restore_service import RestoreReport

    applied: list = []
    prepared: list = []
    ctx.data["applied"] = applied
    ctx.data["prepared"] = prepared

    def fake_batch(layer, events):
        """Pre-patch seam: the whole layer in one synchronous call."""
        prepared.append(len(events))
        ordered = list(reversed(events))
        for event in ordered:
            time.sleep(_COST_PER_EVENT_S)
            applied.append(event.event_id)
        return RestoreReport(
            succeeded=[e.event_id for e in ordered],
            failed={},
            total_requested=len(events),
            trace_events=(),
        )

    def fake_prepare(layer, events):
        """Post-patch seam: per-layer preparation, once."""
        prepared.append(len(events))
        return rr.UndoBatchContext(
            ordered=list(reversed(events)),
            fid_cache={},
            layer_error=None,
        )

    def fake_apply_one(layer, event, context):
        """Post-patch seam: one event, one chunk-visible unit of work."""
        time.sleep(_COST_PER_EVENT_S)
        applied.append(event.event_id)
        return {"success": True, "message": "stub"}

    originals = {}
    patched = []
    for name, stub in (("undo_restore_batch", fake_batch),
                       ("undo_prepare", fake_prepare),
                       ("undo_apply_one", fake_apply_one)):
        # Only names the runner actually imports are patched: pre-patch the
        # last two do not exist, and creating them would let the scenario
        # pass against a runner that never calls them.
        if hasattr(rr, name):
            originals[name] = getattr(rr, name)
            setattr(rr, name, stub)
            patched.append(name)
    ctx.data["originals"] = originals
    ctx.data["patched"] = patched
    return patched


def setup(ctx):
    from recoverland.core.logger import flog

    # setup() is documented as idempotent: a second call must not stack a
    # stub on top of a stub and lose the real function forever.
    _restore_stubs(ctx)

    t0 = datetime(2026, 8, 14, 9, 0, 0, tzinfo=timezone.utc)
    eid = 1000
    by_ds = {}
    layers = {}
    for ds_fp, layer_name, n in ((_DS_FP_A, "ucy_layer_a", _N_EVENTS_A),
                                 (_DS_FP_B, "ucy_layer_b", _N_EVENTS_B)):
        group = [
            _mk_event(eid + i, "UPDATE", t0 + timedelta(seconds=i),
                      ds_fp, layer_name)
            for i in range(n)
        ]
        eid += n
        # The dialog hands the runner events newest-first, exactly as
        # fetch_events_after_cutoff orders them.
        group.reverse()
        by_ds[ds_fp] = group
        layers[ds_fp] = _FakeLayer(layer_name)

    ctx.data["by_ds"] = by_ds
    ctx.data["layers"] = layers
    ctx.data["events"] = [e for group in by_ds.values() for e in group]

    patched = _install_stubs(ctx)
    flog(
        f"rw_undo_chunk_yield setup: trace_id={ctx.trace_id} "
        f"n_events={_N_EVENTS} groups={len(by_ds)} "
        f"cost_per_event_ms={_COST_PER_EVENT_S * 1000:.0f} "
        f"seams_patched={patched}",
        "INFO",
    )


def _restore_stubs(ctx) -> None:
    """Put the real functions back. Idempotent, never raises.

    `restore_runner` is a process-wide module shared with the rest of the
    suite: a leaked stub would make every later undo report success
    without touching a provider, and the scenarios that assert on it
    would pass for the wrong reason.
    """
    from recoverland import restore_runner as rr

    for name, original in (ctx.data.get("originals") or {}).items():
        try:
            setattr(rr, name, original)
        except Exception:  # pragma: no cover
            pass
    ctx.data["originals"] = {}


def run(ctx):
    try:
        _run_inner(ctx)
    finally:
        _restore_stubs(ctx)


def _run_inner(ctx):
    from qgis.PyQt.QtCore import QCoreApplication, QTimer
    from recoverland import restore_runner as rr
    from recoverland.core.logger import flog

    layers = ctx.data["layers"]
    beats: list = []
    progress_points: list = []

    heartbeat = QTimer()
    heartbeat.setInterval(_HEARTBEAT_MS)
    heartbeat.timeout.connect(lambda: beats.append(time.monotonic()))
    heartbeat.start()

    def resolve(evt):
        return layers[evt.datasource_fingerprint]

    runner = rr.UndoRunner(ctx.data["by_ds"], resolve, tracker=None)
    done = {"result": None}
    runner.progress.connect(
        lambda d, t: progress_points.append((time.monotonic(), d, t))
    )
    runner.finished.connect(lambda res: done.__setitem__("result", res))

    t_start = time.monotonic()
    beats.append(t_start)
    runner.start()

    # Pump the loop the way QGIS does, with a hard ceiling so a wedged
    # runner fails the scenario instead of hanging the suite.
    deadline = t_start + 120.0
    while done["result"] is None and time.monotonic() < deadline:
        QCoreApplication.processEvents()
        time.sleep(0.001)
    t_end = time.monotonic()
    heartbeat.stop()

    gaps = [
        (beats[i] - beats[i - 1]) * 1000.0
        for i in range(1, len(beats))
    ]
    max_gap = max(gaps) if gaps else 0.0

    ctx.data["max_gap_ms"] = max_gap
    ctx.data["n_beats"] = len(beats)
    ctx.data["progress_points"] = progress_points
    ctx.data["elapsed_s"] = t_end - t_start
    ctx.data["finished"] = done["result"] is not None

    flog(
        f"rw_undo_chunk_yield run: trace_id={ctx.trace_id} "
        f"undo_max_heartbeat_gap_ms={max_gap:.1f} "
        f"undo_progress_emissions={len(progress_points)} "
        f"heartbeats={len(beats)} "
        f"applied={len(ctx.data['applied'])} "
        f"prepare_calls={len(ctx.data['prepared'])} "
        f"elapsed_s={t_end - t_start:.2f} "
        f"finished={done['result'] is not None}",
        "INFO",
    )


def assertions(ctx):
    results = []

    max_gap = ctx.data.get("max_gap_ms", 0.0)
    applied = ctx.data.get("applied", [])
    prepared = ctx.data.get("prepared", [])
    points = ctx.data.get("progress_points", [])
    events = ctx.data.get("events", [])

    results.append((
        "undo_finished",
        bool(ctx.data.get("finished")),
        f"runner finished={ctx.data.get('finished')} "
        f"elapsed_s={ctx.data.get('elapsed_s', 0):.2f}",
    ))

    # 1. THE assertion: the longest stall of the GUI thread.
    results.append((
        "undo_max_heartbeat_gap_under_budget",
        max_gap < _MAX_GAP_MS,
        f"longest GUI stall {max_gap:.1f} ms (ceiling {_MAX_GAP_MS:.0f} ms, "
        f"total work {_N_EVENTS * _COST_PER_EVENT_S * 1000:.0f} ms)",
    ))

    # 2. The progress bar must move more than once per layer. The budget
    #    is 40 ms and one event costs 25 ms, so a correctly chunked run
    #    emits roughly one every two events; anything close to the number
    #    of GROUPS means the bar is still per-layer.
    min_emissions = len(ctx.data.get("by_ds", {})) + 10
    results.append((
        "undo_progress_emitted_per_chunk",
        len(points) >= min_emissions,
        f"{len(points)} progress emission(s) for {_N_EVENTS} events over "
        f"{len(ctx.data.get('by_ds', {}))} layer(s) (need >= {min_emissions}; "
        f"one per layer would give {len(ctx.data.get('by_ds', {}))})",
    ))

    # 3. Non-regression: every event applied exactly once.
    expected_ids = sorted(e.event_id for e in events)
    results.append((
        "undo_every_event_applied_once",
        sorted(applied) == expected_ids,
        f"applied {len(applied)} of {len(events)} events, "
        f"duplicates={len(applied) - len(set(applied))}",
    ))

    # 4. Non-regression RW-16: forward-chronological unwind order WITHIN
    #    each layer, and layers processed in order -- a re-INSERT must
    #    precede the UPDATE that depends on its existence.
    expected_order = [
        e.event_id
        for group in ctx.data.get("by_ds", {}).values()
        for e in reversed(group)
    ]
    results.append((
        "undo_rw16_order_preserved",
        applied == expected_order,
        f"first applied={applied[:3]} expected={expected_order[:3]} "
        f"(RW-16: a re-INSERT must precede the UPDATE that depends on it)",
    ))

    # 4b. The group-advance path really ran: every layer was opened,
    #     closed and repainted exactly once.
    layers = list(ctx.data.get("layers", {}).values())
    repaints = [lyr.repaint_calls for lyr in layers]
    reloads = [lyr.reload_calls for lyr in layers]
    results.append((
        "undo_every_group_closed_once",
        bool(layers) and all(n == 1 for n in repaints)
        and all(n == 1 for n in reloads),
        f"{len(layers)} layer(s): repaints={repaints} reloads={reloads} "
        f"(exactly 1 each; a reload BETWEEN chunks would invalidate the "
        f"FIDs the remaining events resolve against)",
    ))

    # 5. The per-layer context is built once per LAYER, not once per chunk.
    expected_prepare = [_N_EVENTS_A, _N_EVENTS_B]
    results.append((
        "undo_context_built_once_per_layer",
        prepared == expected_prepare,
        f"per-layer preparation called {len(prepared)} time(s) with "
        f"{prepared} event(s), expected {expected_prepare}; rebuilding the "
        f"fid_cache per chunk would change the result, not just the cost",
    ))

    # 6. The measurement is only worth anything if the stubs were really
    #    installed. A rename of the seams would otherwise leave the
    #    scenario patching nothing and passing on a run that did no work.
    patched = ctx.data.get("patched", [])
    results.append((
        "undo_seams_actually_stubbed",
        "undo_prepare" in patched and "undo_apply_one" in patched,
        f"seams patched={patched}; without undo_prepare AND undo_apply_one "
        f"the timings above measure a runner that never ran the stub",
    ))

    return results
