"""Non-blocking restore runner for RecoverLand.

Processes restore operations in small batches on the main thread,
yielding to the Qt event loop between batches so the UI stays responsive.

QGIS layer operations (provider.addFeatures, etc.) must run on the
main thread; this runner respects that constraint while avoiding freeze.
"""
from collections import defaultdict
import json
import time
from typing import Any, List, Dict, Optional, Callable

from qgis.PyQt.QtCore import QObject, QTimer, pyqtSignal

from .core.audit_backend import AuditEvent
from .core.restore_service import (
    restore_batch,
    undo_prepare,
    undo_apply_one,
    UndoBatchContext,
    build_fid_cache,
    build_restore_trace_event,
)
from .core.restore_executor import _apply_via_buffer
from .core.workflow_service import GroupedRestoreResult
from .core.logger import flog
from .core.observability import assert_invariant, log_cycle_summary
from .core.geometry_utils import (
    feature_geom_short_repr, heavy_diag_enabled, wkb_short_repr,
)

# Chunk size for the QTimer-driven restore loop. Each chunk is processed
# synchronously on the UI thread; smaller chunks = more frequent yields
# = the logo animation and progress smoother stay responsive. The total
# rewind throughput is barely affected because the per-chunk overhead
# (slice + emit + singleShot) is negligible compared to the actual
# layer.changeGeometry / layer.addFeatures calls inside restore_batch.
_CHUNK_SIZE = 5

# BL-RW-P1-26: the strict runner bounds each event-loop tick by TIME,
# not by a fixed action count. A fixed count freezes the UI when single
# actions are expensive (measured 2026-07-05: 5 actions x ~5s of heavy
# polygon scans = 25.5s single chunk on zone_mkt_rip). 40ms keeps the
# progress animation fluid; the max-actions cap bounds per-tick work
# when actions are cheap.
_CHUNK_BUDGET_MS = 40.0
_CHUNK_MAX_ACTIONS = 50

# Threshold above which a single chunk is suspected of freezing the UI
# thread (Windows reports "(Ne repond pas)" around 5 s, but the animation
# visibly stutters from ~150-200 ms onwards). Shared by the strict runner
# and the undo runner: both bound their ticks the same way, so both must
# name the layer that blew the budget the same way.
_CHUNK_SLOW_MS = 200


def _chunk_should_yield(elapsed_ms: float, n_done: int,
                        budget_ms: float = _CHUNK_BUDGET_MS,
                        max_actions: int = _CHUNK_MAX_ACTIONS) -> bool:
    """Decide whether the strict apply loop must yield to the event loop.

    Progress guarantee: never yields before at least one action ran,
    even when a single action blows the budget (it already happened,
    yielding first would deadlock the plan). Yields once the time
    budget is exhausted or max_actions were processed.
    """
    if n_done <= 0:
        return False
    return elapsed_ms >= budget_ms or n_done >= max_actions


_RUNNER_TO_CYCLE: Dict[str, str] = {
    "RestoreRunner": "event_restore",
    "StrictRestoreRunner": "rewind",
    "UndoRunner": "undo",
}


def _cycle_for_runner(runner_name: str) -> str:
    """Map a runner class to its CYCLE_SUMMARY cycle name."""
    return _RUNNER_TO_CYCLE.get(runner_name, runner_name.lower())


# The mutually-exclusive outcomes a planned action can end in. Every
# action increments exactly one of them; that is what makes the
# conservation invariant below checkable at all. `cancelled` is kept
# apart: those actions were never attempted, so they belong to no
# outcome and must never be folded into a failure.
_OUTCOME_BUCKETS = (
    "applied", "applied_partial", "skipped_idempotent",
    "failed", "failed_target_absent", "failed_geometry_drift",
)


def _apply_counters(breakdown: Dict[str, int]) -> Dict[str, int]:
    """Derive the counters CYCLE_SUMMARY publishes from the outcome buckets.

    Measured on a real project (2026-08-12), the line said:

        plan_actions=73 apply_ok=14 apply_skipped=67 apply_fail=59
        applied=6 failed=0 failed_target_absent=59 skipped_idempotent=3

    73 planned actions, 140 counted results, and a rewind that failed on
    59 of them announcing `failed=0`. Three different populations were
    printed side by side as if they partitioned the plan:

      * `apply_skipped` was ``events - events_that_fully_succeeded``, an
        EVENT-level number (73 - 6 = 67) covering every failure and every
        skip a second time, on top of the ACTION-level `apply_ok` and
        `apply_fail`. That is the double count: 14 + 59 already totalled
        the 73 actions before `apply_skipped` was added.
      * `apply_ok` counted everything the provider did not reject
        (6 applied + 3 idempotent skips + 5 drift refusals), so it
        contradicted `applied=6` on the same line.
      * `failed` published the residual "no specific category" bucket
        under the name a reader takes for the failure TOTAL.

    Here the three counters are computed from the buckets and from
    nothing else, so they partition the plan by construction:

        apply_ok      = applied + applied_partial
        apply_skipped = skipped_idempotent
        apply_fail    = failed + failed_target_absent
                        + failed_geometry_drift

    `failed` then publishes that failure total (never 0 while an action
    failed) and the residual keeps the explicit name `failed_other`.

    Returns the buckets plus the derived counters and ``outcome_sum``,
    the total of the six outcome buckets.
    """
    out: Dict[str, int] = {
        key: int(breakdown.get(key, 0) or 0) for key in _OUTCOME_BUCKETS
    }
    out["cancelled"] = int(breakdown.get("cancelled", 0) or 0)
    out["outcome_sum"] = sum(out[key] for key in _OUTCOME_BUCKETS)
    out["apply_ok"] = out["applied"] + out["applied_partial"]
    out["apply_skipped"] = out["skipped_idempotent"]
    out["apply_fail"] = (out["failed"] + out["failed_target_absent"]
                         + out["failed_geometry_drift"])
    return out


def _assert_bucket_conservation(
    runner_name: str, trace_id: str, counters: Dict[str, int],
    total_ok: int, total_fail: int, plan_actions: Optional[int],
) -> bool:
    """Check the buckets against BOTH accounting bases, loudly.

    * ``total_ok + total_fail`` is what the runner counted while
      applying. This base was already checked.
    * ``plan_actions`` is what CYCLE_SUMMARY publishes, i.e. the number
      the reader adds the apply counters against. It was never checked,
      which is how the 2026-08-12 line could pass the invariant
      (6+0+3+0+59+5 == 14+59 == 73, silence) while publishing 140
      results for 73 actions: the counters on the line were simply not
      the buckets the invariant was verifying.

    Returns True when both bases agree. Never raises: the data changes
    already landed on disk, it is the metrics that are suspect, and a
    rewind must not be reported as crashed because its accounting drifted.
    """
    detail: Dict[str, Any] = {
        "runner": runner_name,
        "applied": counters["applied"],
        "applied_partial": counters["applied_partial"],
        "skipped_idempotent": counters["skipped_idempotent"],
        "failed_other": counters["failed"],
        "failed_target_absent": counters["failed_target_absent"],
        "failed_geometry_drift": counters["failed_geometry_drift"],
        "cancelled": counters["cancelled"],
    }
    if trace_id:
        detail["trace"] = trace_id

    outcome_sum = counters["outcome_sum"]
    expected = int(total_ok) + int(total_fail)
    ok_totals = assert_invariant(
        outcome_sum == expected,
        "apply_buckets_vs_totals",
        base="total_ok+total_fail", expected=expected,
        bucket_sum=outcome_sum, delta=outcome_sum - expected,
        **detail,
    )

    ok_plan = True
    if plan_actions is not None:
        published = outcome_sum + counters["cancelled"]
        planned = int(plan_actions)
        ok_plan = assert_invariant(
            published == planned,
            "apply_buckets_vs_plan_actions",
            base="plan_actions", expected=planned,
            bucket_sum=published, delta=published - planned,
            apply_ok=counters["apply_ok"],
            apply_skipped=counters["apply_skipped"],
            apply_fail=counters["apply_fail"],
            **detail,
        )
    return bool(ok_totals and ok_plan)


def _finish_runner(
    runner,
    *,
    runner_name: str,
    by_ds: Dict[str, list],
    total_ok: int,
    total_fail: int,
    errors: List[str],
    traces: list,
    write_queue,
    started_at: float,
    tracker,
    trace_id: str = "",
    status_label: Optional[str] = None,
    failed_eids: Optional[List[int]] = None,
    cycle: Optional[str] = None,
    extra_stats: Optional[Dict[str, int]] = None,
    # BL-RW-P3-18: per-category breakdown propagated to runner.finished.
    breakdown: Optional[Dict[str, int]] = None,
    # RLU-054: the number of actions CYCLE_SUMMARY is accounted against.
    # Supplied by the runner that owns a plan; None on the paths that
    # have no per-action classification (undo).
    plan_actions: Optional[int] = None,
) -> None:
    """Common end-of-run pipeline for the three QObject runners.

    Steps reproduced from the previous per-runner implementations:
      1. Enqueue trace events to the write queue when present (logged at
         ERROR if the queue refuses them so the user knows the journal
         is degraded but the data changes still landed).
      2. Build the GroupedRestoreResult expected by the dialog code.
      3. Log a single ``finish`` line with elapsed_ms and either an
         ok/fail summary (RestoreRunner) or a status_label
         (StrictRestoreRunner).
      4. Emit ``runner.finished`` so the dialog drives the next step.
      5. Always unsuppress the edit tracker on the way out, even when
         the emit raises, otherwise tracking stays paused indefinitely.

    RLU-054: the CYCLE_SUMMARY apply counters are derived from the
    outcome buckets here (see `_apply_counters`) instead of being
    collected from three different populations, and the conservation
    invariant is checked against the base the line publishes as well as
    against the runner's own totals.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    counters = _apply_counters(breakdown or {})
    applied = counters["applied"]
    # RLU-053: a write that had to abandon captured fields is a success
    # for the provider and a mutilation for the user. It leaves `applied`
    # so that "restored" never counts a hybrid feature.
    applied_partial = counters["applied_partial"]
    skipped_idempotent = counters["skipped_idempotent"]
    failed_other = counters["failed"]
    failed_target_absent = counters["failed_target_absent"]
    failed_geometry_drift = counters["failed_geometry_drift"]
    cancelled = counters["cancelled"]

    conserved = True
    if breakdown is not None:
        conserved = _assert_bucket_conservation(
            runner_name, trace_id, counters,
            total_ok, total_fail, plan_actions,
        )
    try:
        if traces and write_queue is not None:
            accepted = write_queue.enqueue(list(traces))
            if not accepted:
                msg = (
                    "Journal trace write failed; restore data changes"
                    " succeeded but trace events were saved"
                    " for pending recovery"
                )
                errors.append(msg)
                flog(f"{runner_name}: {msg}", "ERROR")

        result = GroupedRestoreResult(
            total_ok=total_ok,
            total_fail=total_fail,
            errors=errors,
            by_ds=dict(by_ds),
            trace_events=traces,
            failed_eids=failed_eids or [],
            applied=applied,
            skipped_idempotent=skipped_idempotent,
            failed=failed_other,
            failed_target_absent=failed_target_absent,
            failed_geometry_drift=failed_geometry_drift,
        )
        elapsed_ms = (
            int((time.monotonic() - started_at) * 1000)
            if started_at else 0
        )
        # `failed_other` is named in full here too: the short `failed=`
        # on this line was read as the failure total by anyone grepping
        # a run, exactly like on CYCLE_SUMMARY.
        status_part = (
            f"status={status_label} " if status_label is not None else ""
        )
        flog(f"{prefix}{runner_name}: finish {status_part}"
             f"ok={total_ok} fail={total_fail} elapsed_ms={elapsed_ms} "
             f"applied={applied} applied_partial={applied_partial} "
             f"skipped_idempotent={skipped_idempotent} "
             f"failed_other={failed_other} "
             f"failed_target_absent={failed_target_absent} "
             f"failed_geometry_drift={failed_geometry_drift} "
             f"cancelled={cancelled}")

        cycle_name = cycle or _cycle_for_runner(runner_name)
        cycle_stats: Dict[str, Any] = dict(extra_stats or {})
        cycle_stats["traces_written"] = len(traces) if traces else 0
        if plan_actions is not None:
            cycle_stats["plan_actions"] = int(plan_actions)
        if breakdown is None:
            # No per-action classification on this path (undo). Publish
            # the provider-level totals and NOT a row of zeroed buckets:
            # `failed=0` printed next to a non-zero `apply_fail` reads as
            # "no failure", which is the opposite of what happened.
            cycle_stats["apply_ok"] = total_ok
            cycle_stats["apply_fail"] = total_fail
        else:
            cycle_stats.update({
                # Disjoint by construction: ok + skipped + fail
                # (+ cancelled) == plan_actions.
                "apply_ok": counters["apply_ok"],
                "apply_skipped": counters["apply_skipped"],
                "apply_fail": counters["apply_fail"],
                "applied": applied,
                "applied_partial": applied_partial,
                "skipped_idempotent": skipped_idempotent,
                # `failed` is the failure TOTAL, `failed_other` the
                # residual bucket. The reverse convention is what
                # announced failed=0 on a rewind that failed 59 times.
                "failed": counters["apply_fail"],
                "failed_other": failed_other,
                "failed_target_absent": failed_target_absent,
                "failed_geometry_drift": failed_geometry_drift,
            })
            if cancelled:
                cycle_stats["cancelled"] = cancelled
            if not conserved:
                # The counters did not add up; say so on the line the
                # user re-reads rather than only in the CRITICAL above.
                cycle_stats["bucket_conservation"] = "violated"
        log_cycle_summary(trace_id, cycle_name, cycle_stats, elapsed_ms)

        runner.finished.emit(result)
    finally:
        if tracker is not None:
            tracker.unsuppress()


def _abort_started_run(runner, runner_name: str, exc: BaseException,
                       trace_id: str = "") -> None:
    """Close a run whose start() died before the async chain could begin.

    The three runners suppress the edit tracker in start() and release it
    in _finish(), at the end of a chain driven by QTimer. When the very
    first step raises (layer removed from the project, destroyed C++
    object, SQLite failure), that chain never runs: the tracker used to
    stay suppressed for the rest of the session, so the plugin silently
    stopped journalling the user's edits right after a failed restore -
    exactly when he starts repairing by hand. Finishing the runner here
    releases the lock and tells the dialog the run is over; the caller
    still sees the original exception.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    flog(f"{prefix}{runner_name}: start aborted, releasing capture lock "
         f"({type(exc).__name__}: {exc})", "ERROR")
    runner._errors.append(f"Restauration interrompue: {exc}")
    runner._finish()


def _resolve_runner_layer(
    find_layer_fn: Callable[[AuditEvent], object],
    group: list,
    fp: str,
    runner_name: str,
    uncommitted_msg: str,
    trace_id: str = "",
):
    """Resolve and validate the target layer for a runner group.

    Common preflight reused by RestoreRunner, StrictRestoreRunner and
    UndoRunner: the lookup fails when the layer is missing from the
    project, refuses to act when the user has uncommitted edits, and
    auto-commits a clean edit session so the runner can take ownership
    of the next provider transaction.

    Returns ``(layer, errors)``. ``errors`` is empty when preflight
    passed (the caller proceeds with the resolved layer). Otherwise the
    caller must record each error message against the whole group and
    advance.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    name = group[0].layer_name_snapshot or fp
    layer = find_layer_fn(group[0])

    if layer is None:
        flog(f"{prefix}{runner_name}: layer NOT FOUND "
             f"name='{name}' fp={fp} events={len(group)}", "ERROR")
        return None, [f"Couche '{name}' non trouvee dans le projet."]

    if hasattr(layer, 'isEditable') and layer.isEditable():
        if hasattr(layer, 'isModified') and layer.isModified():
            flog(f"{prefix}{runner_name}: layer HAS UNSAVED EDITS "
                 f"name='{name}' fp={fp}", "ERROR")
            return None, [
                f"Evt {event.event_id or 0}: {uncommitted_msg}"
                for event in group
            ]
        flog(f"{prefix}{runner_name}: auto-closing edit session "
             f"name='{name}' fp={fp}")
        layer.commitChanges()

    return layer, []


class RestoreRunner(QObject):
    """Stepped restore executor that never blocks the UI.

    Usage:
        runner = RestoreRunner(events, resolver, write_queue)
        runner.progress.connect(on_progress)
        runner.finished.connect(on_done)
        runner.start()
    """

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(object)

    def __init__(
        self,
        events: List[AuditEvent],
        find_layer_fn: Callable[[AuditEvent], object],
        write_queue=None,
        tracker=None,
        trace_id: str = "",
        parent: Optional[QObject] = None,
    ):
        super().__init__(parent)
        self._events = events
        self._find_layer_fn = find_layer_fn
        self._write_queue = write_queue
        self._tracker = tracker
        self._trace_id = trace_id
        self._cancelled = False

        self._by_ds: Dict[str, list] = defaultdict(list)
        for event in events:
            self._by_ds[event.datasource_fingerprint].append(event)

        self._groups = list(self._by_ds.items())
        self._group_idx = 0
        self._event_idx = 0

        self._total_ok = 0
        self._total_fail = 0
        self._errors: List[str] = []
        self._traces: list = []
        self._processed = 0
        self._started_at = 0.0

        self._current_layer = None
        self._current_fp = ""
        self._group_ok = 0
        self._group_fid_cache: Dict = {}

        # BL-RW-P3-18: per-category breakdown accumulator.
        self._applied = 0
        self._skipped_idempotent = 0
        self._failed_other = 0
        self._failed_target_absent = 0
        self._failed_geometry_drift = 0

    def start(self) -> None:
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        self._started_at = time.monotonic()
        flog(f"{prefix}RestoreRunner: start events={len(self._events)} groups={len(self._groups)}")
        if self._tracker is not None:
            self._tracker.suppress()
        try:
            self._advance_group()
        except BaseException as exc:
            _abort_started_run(self, "RestoreRunner", exc, self._trace_id)
            raise

    def cancel(self) -> None:
        self._cancelled = True

    def _advance_group(self) -> None:
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return

        fp, group = self._groups[self._group_idx]
        self._current_fp = fp
        self._event_idx = 0
        self._group_ok = 0

        layer, errors = _resolve_runner_layer(
            self._find_layer_fn, group, fp,
            runner_name="RestoreRunner",
            uncommitted_msg="Target layer has uncommitted edits; "
                            "commit or rollback before restore",
            trace_id=self._trace_id,
        )
        if layer is None:
            self._errors.extend(errors)
            # BL-RW-P4-21: layer cannot be resolved -> entire group is
            # accounted as failed_other so the breakdown invariant holds.
            self._failed_other += len(group)
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, len(self._events))
            self._group_idx += 1
            QTimer.singleShot(0, self._advance_group)
            return

        self._current_layer = layer
        self._group_fid_cache = build_fid_cache(layer, group)
        QTimer.singleShot(0, self._process_chunk)

    def _process_chunk(self) -> None:
        if self._cancelled:
            self._finish()
            return

        _fp, group = self._groups[self._group_idx]
        layer = self._current_layer
        start = self._event_idx
        end = min(start + _CHUNK_SIZE, len(group))
        chunk = group[start:end]

        report = restore_batch(
            layer, chunk,
            fid_cache=self._group_fid_cache,
            trace_id=self._trace_id,
        )
        self._total_ok += len(report.succeeded)
        self._group_ok += len(report.succeeded)
        self._total_fail += len(report.failed)
        self._traces.extend(report.trace_events)
        for eid, msg in report.failed.items():
            self._errors.append(f"Evt {eid}: {msg}")

        # BL-RW-P3-18: per-category accumulation.
        target_absent_set = set(report.failed_target_absent or [])
        drift_set = set(report.failed_geometry_drift or [])
        skipped_set = set(report.skipped_idempotent or [])
        self._skipped_idempotent += len(skipped_set)
        self._failed_target_absent += len(target_absent_set)
        self._failed_geometry_drift += len(drift_set)
        for eid in report.succeeded:
            if eid in skipped_set or eid in target_absent_set or eid in drift_set:
                continue
            self._applied += 1
        for eid in report.failed.keys():
            if eid in target_absent_set or eid in drift_set:
                continue
            self._failed_other += 1

        self._event_idx = end
        self._processed += len(chunk)
        self.progress.emit(self._processed, len(self._events))

        if self._event_idx >= len(group):
            if self._group_ok > 0:
                provider = layer.dataProvider()
                if provider is not None and hasattr(provider, 'reloadData'):
                    provider.reloadData()
                if hasattr(layer, 'updateExtents'):
                    layer.updateExtents()
                layer.reload()
            layer.triggerRepaint()
            self._group_idx += 1
            QTimer.singleShot(0, self._advance_group)
        else:
            QTimer.singleShot(0, self._process_chunk)

    def _finish(self) -> None:
        # RLU-054: one event yields exactly one result here, so the plan
        # base is the event count and whatever a cancellation left
        # unprocessed is `cancelled` -- never silently a failure.
        cancelled = max(len(self._events) - self._processed, 0)
        _finish_runner(
            self,
            runner_name="RestoreRunner",
            by_ds=self._by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=self._traces,
            write_queue=self._write_queue,
            started_at=self._started_at,
            tracker=self._tracker,
            trace_id=self._trace_id,
            plan_actions=len(self._events),
            breakdown={
                "applied": self._applied,
                "skipped_idempotent": self._skipped_idempotent,
                "failed": self._failed_other,
                "failed_target_absent": self._failed_target_absent,
                "failed_geometry_drift": self._failed_geometry_drift,
                "cancelled": cancelled,
            },
        )


class StrictRestoreRunner(QObject):
    """Atomic per-layer restore executor for temporal mode.

    Uses QGIS editing buffer + rollback: if any event in a layer
    group fails, all changes for that layer are rolled back.
    Non-blocking: one layer per QTimer tick.

    Usage:
        runner = StrictRestoreRunner(events, resolver, cutoff, ...)
        runner.progress.connect(on_progress)
        runner.finished.connect(on_done)
        runner.start()
    """

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(object)

    def __init__(
        self,
        events: List[AuditEvent],
        find_layer_fn: Callable[[AuditEvent], object],
        cutoff,
        write_queue=None,
        tracker=None,
        trace_id: str = "",
        parent: Optional[QObject] = None,
        extra_stats: Optional[Dict[str, int]] = None,
    ):
        super().__init__(parent)
        self._events = events
        self._find_layer_fn = find_layer_fn
        self._cutoff = cutoff
        self._write_queue = write_queue
        self._tracker = tracker
        self._trace_id = trace_id
        self._extra_stats = dict(extra_stats) if extra_stats else {}
        self._cancelled = False

        self._by_ds: Dict[str, list] = defaultdict(list)
        for event in events:
            self._by_ds[event.datasource_fingerprint].append(event)

        self._groups = list(self._by_ds.items())
        self._group_idx = 0

        self._total_ok = 0
        self._total_fail = 0
        self._errors: List[str] = []
        self._traces: list = []
        self._processed = 0
        self._started_at = 0.0

        self._strict_plan = None
        self._strict_events_by_id: Dict = {}
        self._strict_layer = None
        self._strict_layer_name = ""
        self._strict_action_idx = 0
        self._strict_succeeded: List[int] = []
        self._strict_was_editing = False
        self._strict_apply_start = 0.0
        self._strict_count_before = -1
        self._strict_fid_remap: Dict = {}
        self._strict_skipped: List[int] = []
        self._all_succeeded_ids: set = set()

        # RLU-054: the base CYCLE_SUMMARY is accounted against. Counted
        # as the run goes, layer by layer, because that is the only place
        # that knows how many actions a plan really carried:
        #   _plan_actions — actions the planner emitted, plus the events
        #     of a group refused whole (no layer, preflight blocked, no
        #     edit session) since those never reached the planner's
        #     verdict and are accounted as one failure each.
        #   _plan_skipped — events the planner dropped (no stable
        #     identity): never applied, never failed, and until now
        #     invisible on the line.
        self._plan_actions = 0
        self._plan_skipped = 0

        # BL-RW-P3-18: per-category breakdown accumulator (mirror of
        # RestoreRunner). Per-layer counters are kept separate so a
        # rollback or a commit failure can credit the whole layer as
        # _failed_other instead of leaving accumulated buckets that
        # never made it to the user-visible state (BL-RW-P4-21).
        self._applied = 0
        self._skipped_idempotent = 0
        self._failed_other = 0
        self._failed_target_absent = 0
        self._failed_geometry_drift = 0
        # RLU-053: features written without every captured field, and the
        # names of the fields left behind. `_apply_via_buffer` says so
        # (status=APPLIED_PARTIAL + dropped_fields); until now nobody
        # read it, so a mutilated rewind was announced as a full one.
        self._applied_partial = 0
        self._partial_dropped: List[str] = []
        # Per-group accumulators reset at every _begin_strict_for_layer
        # and merged into the globals only on commit success.
        self._strict_group_applied = 0
        self._strict_group_applied_partial = 0
        self._strict_group_dropped: List[str] = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0

    def start(self) -> None:
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        self._started_at = time.monotonic()
        flog(f"{prefix}StrictRestoreRunner: start events={len(self._events)} "
             f"layers={len(self._groups)} strategy=STRICT")
        if self._tracker is not None:
            self._tracker.suppress()
        try:
            self._advance_group()
        except BaseException as exc:
            _abort_started_run(self, "StrictRestoreRunner", exc, self._trace_id)
            raise

    def cancel(self) -> None:
        self._cancelled = True
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        flog(f"{prefix}StrictRestoreRunner: cancel_requested "
             f"applied={self._total_ok}")

    def partial_restore_report(self) -> tuple:
        """RLU-053: ``(n_features, field_names)`` restored incompletely.

        The count and the abandoned column names of every feature the
        buffer wrote as APPLIED_PARTIAL, once the layers that carried
        them have committed (a rolled-back layer contributes nothing).
        The dialog reads this at the end of the run: it is what turns
        "restauree" into "restauree, sans ces champs". ``(0, ())`` when
        every captured field made it back.

        Kept on the runner rather than in the emitted
        ``GroupedRestoreResult``: that contract lives in
        core/workflow_service.py and is shared with the two other
        runners; extending it belongs to the module that owns it.
        """
        return (self._applied_partial, tuple(self._partial_dropped))

    @staticmethod
    def _merge_dropped(target: List[str], names) -> None:
        """Append the unseen names of *names* to *target*, order kept.

        The same column is dropped on every feature of a drifted layer;
        the user must read it once, not once per feature.
        """
        for name in names or ():
            text = str(name)
            if text and text not in target:
                target.append(text)

    def _advance_group(self) -> None:
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return

        fp, group = self._groups[self._group_idx]
        self._group_idx += 1
        prefix = f"[{self._trace_id}] " if self._trace_id else ""

        layer, errors = _resolve_runner_layer(
            self._find_layer_fn, group, fp,
            runner_name="StrictRestoreRunner",
            uncommitted_msg="Target layer has uncommitted edits; "
                            "commit or rollback before restore",
            trace_id=self._trace_id,
        )
        if layer is None:
            self._errors.extend(errors)
            # BL-RW-P4-21: layer cannot be resolved -> entire group is
            # accounted as failed_other so the breakdown invariant holds.
            # RLU-054: and as many planned actions, so the failures it
            # adds stay inside the base the summary publishes.
            self._plan_actions += len(group)
            self._failed_other += len(group)
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, len(self._events))
            QTimer.singleShot(0, self._advance_group)
            return

        self._begin_strict_for_layer(fp, group, layer, prefix)

    def _begin_strict_for_layer(
        self, fp: str, group: list, layer, prefix: str,
    ) -> None:
        """Prepare chunked strict restore for one layer (BL-PERF-009)."""
        from .core.restore_planner import plan_temporal_restore
        from .core.restore_executor import preflight_layer_check
        from .core.restore_contracts import PreflightVerdict
        from .core.restore_planner import preflight_check

        layer_name = group[0].layer_name_snapshot or fp
        plan = plan_temporal_restore(
            group, fp, layer_name, self._cutoff,
        )

        report = preflight_check(plan)
        if report.verdict == PreflightVerdict.BLOCKED:
            msg = "; ".join(report.blocking_reasons[:3])
            self._errors.append(f"Preflight blocked: {msg}")
            # BL-RW-P4-21: preflight blocked the whole group -> count
            # every action as failed_other.
            self._plan_actions += len(group)
            self._failed_other += len(group)
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, len(self._events))
            QTimer.singleShot(0, self._advance_group)
            return

        layer_issues = preflight_layer_check(plan, layer)
        if layer_issues:
            msg = "; ".join(layer_issues[:3])
            self._errors.append(f"Layer check failed: {msg}")
            # BL-RW-P4-21: layer-level preflight refused the layer ->
            # whole group is failed_other.
            self._plan_actions += len(group)
            self._failed_other += len(group)
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, len(self._events))
            QTimer.singleShot(0, self._advance_group)
            return

        events_by_id = {e.event_id: e for e in group}

        self._strict_plan = plan
        self._strict_events_by_id = events_by_id
        self._strict_layer = layer
        self._strict_layer_name = layer_name
        self._strict_action_idx = 0
        self._strict_succeeded = []
        self._strict_skipped = []
        self._strict_was_editing = layer.isEditable()
        self._strict_apply_start = time.monotonic()
        self._strict_fid_remap = {}
        # BL-RW-P4-21: reset per-group bucket accumulators.
        self._strict_group_applied = 0
        self._strict_group_applied_partial = 0
        self._strict_group_dropped = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0

        if not self._strict_was_editing:
            if not layer.startEditing():
                flog(f"{prefix}StrictRestoreRunner: startEditing failed", "ERROR")
                self._errors.append("Cannot start editing on layer")
                # BL-RW-P4-21: cannot open the layer for editing ->
                # whole group is failed_other.
                self._plan_actions += len(group)
                self._failed_other += len(group)
                self._total_fail += len(group)
                self._processed += len(group)
                self.progress.emit(self._processed, len(self._events))
                QTimer.singleShot(0, self._advance_group)
                return

        # RLU-054: from here the layer enters the apply phase, so every
        # action of ITS plan will land in exactly one outcome bucket. The
        # events the planner refused (no stable identity) are counted
        # apart: they are neither applied, nor skipped at runtime, nor
        # failed, and folding them into any of those was part of what made
        # the summary add up to twice the plan.
        n_actions = len(plan.actions)
        self._plan_actions += n_actions
        self._plan_skipped += max(len(group) - n_actions, 0)

        count_before = layer.featureCount()
        self._strict_count_before = count_before

        # Pre-fetch geom snapshots for the forensic EDIT_START log. This is
        # the single biggest cause of the UI freeze during a rewind: it
        # does one provider.getFeatures() per UPDATE event before the first
        # chunk runs. Gate it on the RECOVERLAND_HEAVY_DIAG opt-in.
        n_update_events = sum(
            1 for a in group
            if getattr(a, 'new_geometry_wkb', None)
        )
        if heavy_diag_enabled():
            upd_geom_state = {}
            for a in group:
                wkb = getattr(a, 'new_geometry_wkb', None)
                if not wkb:
                    continue
                try:
                    fid = json.loads(a.feature_identity_json).get('fid')
                    if fid is not None:
                        upd_geom_state[fid] = feature_geom_short_repr(layer, fid)
                except Exception as exc:
                    flog(f"EDIT_START err eid={getattr(a, 'event_id', '?')} exc={exc}", "WARNING")
            flog(f"EDIT_START layer={layer_name} n_geom_upd={len(upd_geom_state)} geom_at_start={upd_geom_state}")
        else:
            flog(f"EDIT_START layer={layer_name} n_geom_upd={n_update_events} geom_at_start=diag_off")

        layer.beginEditCommand("RecoverLand: temporal restore")

        ops = {}
        for a in plan.actions:
            ops[a.compensatory_op] = ops.get(a.compensatory_op, 0) + 1
        flog(f"{prefix}StrictRestoreRunner: begin_strict "
             f"layer={layer_name} actions={plan.event_count} "
             f"compensatory={ops} feat_count_before={count_before}")
        QTimer.singleShot(0, self._process_strict_chunk)

    def _process_strict_chunk(self) -> None:
        """Process a chunk of strict restore actions (BL-PERF-009)."""
        try:
            self._process_strict_chunk_inner()
        except Exception as exc:
            prefix = f"[{self._trace_id}] " if self._trace_id else ""
            flog(f"{prefix}StrictRestoreRunner: CRASH in chunk: {exc}", "CRITICAL")
            self._errors.append(f"Internal error: {exc}")
            self._rollback_strict(prefix, "internal_crash")

    def _process_strict_chunk_inner(self) -> None:
        """Inner chunk logic, called by _process_strict_chunk with crash guard."""
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        plan = self._strict_plan
        events_by_id = self._strict_events_by_id
        layer = self._strict_layer
        actions = plan.actions
        start = self._strict_action_idx
        chunk_t0 = time.monotonic()

        if self._cancelled:
            self._rollback_strict(prefix, "cancel_requested")
            return

        i = start
        while i < len(actions):
            elapsed_ms = (time.monotonic() - chunk_t0) * 1000.0
            if _chunk_should_yield(elapsed_ms, i - start):
                break
            action = actions[i]
            i += 1
            event = events_by_id.get(action.event_id)
            if event is None:
                self._errors.append(f"Evt {action.event_id}: Event data not found")
                self._rollback_strict(prefix, "event_not_found")
                return

            result = _apply_via_buffer(
                layer, action.compensatory_op, event,
                self._strict_fid_remap,
                trace_id=self._trace_id,
            )
            # BL-RW-P3-18: classify the action result for the breakdown.
            from .core.restore_service import _classify_restore_result
            reason = _classify_restore_result(result)

            if result["success"]:
                skipped = result.get("skipped", False)
                flog(f"{prefix}StrictRestoreRunner: action_ok "
                     f"eid={action.event_id} "
                     f"comp_op={action.compensatory_op} "
                     f"orig_op={action.operation_type} "
                     f"skipped={skipped} reason={reason} "
                     f"fid={result.get('fid', '?')}")
                if skipped:
                    self._strict_skipped.append(action.event_id)
                else:
                    self._strict_succeeded.append(action.event_id)
                    self._all_succeeded_ids.add(action.event_id)
                # BL-RW-P4-21: route bucket increments through the
                # per-group accumulators so a later rollback can
                # discard them cleanly.
                if reason == "target_absent":
                    self._strict_group_failed_target_absent += 1
                elif reason == "geometry_drift":
                    self._strict_group_failed_geometry_drift += 1
                elif reason == "skipped_idempotent":
                    self._strict_group_skipped_idempotent += 1
                elif reason == "applied_partial":
                    # RLU-053: the buffer wrote the feature, but not every
                    # captured field. Counting it with the complete ones
                    # is what let a hybrid entity be announced as
                    # "restored"; it gets its own bucket and its fields
                    # are named to the user at the end of the run.
                    self._strict_group_applied_partial += 1
                    self._merge_dropped(self._strict_group_dropped,
                                        result.get("dropped_fields"))
                    flog(f"{prefix}StrictRestoreRunner: applied_partial "
                         f"eid={action.event_id} "
                         f"dropped={list(result.get('dropped_fields') or ())} "
                         f"msg={result['message']}", "WARNING")
                else:
                    self._strict_group_applied += 1
            else:
                # BL-RW-P4-22: distinguish soft failures (target_absent,
                # geometry_drift) from hard failures. Soft failures are
                # bounded to a single action that left the buffer state
                # untouched, so the batch can keep processing the
                # remaining actions instead of triggering a layer-wide
                # rollback that would discard already-applied valid
                # compensations.
                soft_fail = reason in ("target_absent", "geometry_drift")
                log_level = "WARNING" if soft_fail else "ERROR"
                flog(f"{prefix}StrictRestoreRunner: action_failed "
                     f"eid={action.event_id} comp_op={action.compensatory_op} "
                     f"orig_op={action.operation_type} reason={reason} "
                     f"soft={soft_fail} msg={result['message']}",
                     log_level)
                self._errors.append(
                    f"Evt {action.event_id}: {result['message']}")
                if reason == "target_absent":
                    self._strict_group_failed_target_absent += 1
                elif reason == "geometry_drift":
                    self._strict_group_failed_geometry_drift += 1
                else:
                    self._strict_group_failed_other += 1
                if soft_fail:
                    # Skip the offending action; the buffer is untouched
                    # because _buffer_update/_buffer_delete return early
                    # before any layer mutation when target is absent or
                    # geometry drift is detected. The breakdown counter
                    # already recorded the failure reason; the commit
                    # path will fold it into _total_fail.
                    continue
                self._rollback_strict(prefix, "apply_failed")
                return

        end = i
        self._strict_action_idx = end
        self._processed += (end - start)
        self.progress.emit(self._processed, len(self._events))

        chunk_elapsed_ms = (time.monotonic() - chunk_t0) * 1000.0
        if chunk_elapsed_ms > _CHUNK_SLOW_MS:
            flog(f"{prefix}CHUNK_SLOW layer={self._strict_layer_name!r} "
                 f"actions={start}..{end} n={end - start} "
                 f"elapsed_ms={chunk_elapsed_ms:.0f}", "WARNING")

        if self._strict_action_idx >= len(actions):
            self._commit_strict(prefix)
        else:
            QTimer.singleShot(0, self._process_strict_chunk)

    def _rollback_strict(self, prefix: str, reason: str) -> None:
        """Rollback current strict restore layer."""
        layer = self._strict_layer
        succeeded_count = len(self._strict_succeeded)
        total_actions = len(self._strict_plan.actions)
        failed_count = total_actions - succeeded_count

        flog(f"{prefix}StrictRestoreRunner: rollback reason={reason} "
             f"applied={succeeded_count} remaining={failed_count}", "WARNING")

        layer.destroyEditCommand()
        if not self._strict_was_editing:
            layer.rollBack()

        for eid in self._strict_succeeded:
            self._all_succeeded_ids.discard(eid)

        # BL-RW-P4-21: the layer rolled back, so discard the per-group
        # bucket accumulators (applied/skipped were rolled back at the
        # provider level) and credit the whole layer as failed_other,
        # except for the action that actually triggered the rollback
        # whose specific reason (target_absent / geometry_drift) is
        # preserved so the breakdown still tells the operator why.
        preserved_target_absent = self._strict_group_failed_target_absent
        preserved_geometry_drift = self._strict_group_failed_geometry_drift
        self._failed_target_absent += preserved_target_absent
        self._failed_geometry_drift += preserved_geometry_drift
        remainder_other = (
            total_actions - preserved_target_absent - preserved_geometry_drift
        )
        self._failed_other += max(remainder_other, 0)
        self._strict_group_applied = 0
        # RLU-053: the layer rolled back, so nothing was written
        # incompletely either; the field names must not survive into the
        # end-of-run report of a layer the user never got.
        self._strict_group_applied_partial = 0
        self._strict_group_dropped = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0

        self._total_fail += total_actions
        remaining = total_actions - self._strict_action_idx
        self._processed += remaining
        self.progress.emit(self._processed, len(self._events))
        layer.triggerRepaint()
        QTimer.singleShot(0, self._advance_group)

    def _log_post_commit_state(self, layer, layer_name: str,
                               prefix: str) -> None:
        """Log target geometry vs persisted geometry for each succeeded UPDATE.

        For every UPDATE compensatory action that returned success during
        the apply phase, read the feature back from the provider AFTER the
        commit and compare the persisted geometry with the OLD WKB written
        in the audit event. This is the ground-truth check: if the
        persisted geometry does not match the target, the commit silently
        re-applied the buffer state or the provider rejected the geom
        update without flagging an error.

        Gated on RECOVERLAND_HEAVY_DIAG: each iteration does one
        provider.getFeatures() and we run the loop on the UI thread.
        Skipped by default to keep the rewind responsive.
        """
        if not heavy_diag_enabled():
            flog(f"{prefix}TRACE_POST_COMMIT: layer={layer_name} skipped "
                 f"(set RECOVERLAND_HEAVY_DIAG=1 to enable per-event check)")
            return
        try:
            import json
        except ImportError:
            return
        succeeded = set(self._strict_succeeded)
        if not succeeded:
            return
        plan = getattr(self, '_strict_plan', None)
        events_by_id = getattr(self, '_strict_events_by_id', {})
        if plan is None:
            return
        check_count = 0
        mismatch_count = 0
        for action in plan.actions:
            if action.compensatory_op != "UPDATE":
                continue
            if action.event_id not in succeeded:
                continue
            event = events_by_id.get(action.event_id)
            if event is None or event.geometry_wkb is None:
                continue
            try:
                identity = json.loads(event.feature_identity_json)
            except (json.JSONDecodeError, TypeError, ValueError):
                identity = {}
            fid = identity.get("fid")
            if fid is None:
                continue
            persisted = feature_geom_short_repr(layer, int(fid))
            target = wkb_short_repr(event.geometry_wkb)
            match = persisted == target
            if not match:
                mismatch_count += 1
            flog(f"{prefix}TRACE_POST_COMMIT: layer={layer_name} "
                 f"eid={event.event_id} fid={fid} "
                 f"target_OLD={target} "
                 f"persisted={persisted} "
                 f"match={match}")
            check_count += 1
        if check_count:
            flog(f"{prefix}TRACE_POST_COMMIT: layer={layer_name} "
                 f"checked={check_count} mismatches={mismatch_count}")

    def _commit_strict(self, prefix: str) -> None:
        """Commit current strict restore layer."""
        layer = self._strict_layer
        layer_name = self._strict_layer_name
        apply_ms = int((time.monotonic() - self._strict_apply_start) * 1000)

        layer.endEditCommand()

        if not self._strict_was_editing:
            commit_start = time.monotonic()
            eb = layer.editBuffer()
            buf_add = len(eb.addedFeatures()) if eb else -1
            buf_del = len(eb.deletedFeatureIds()) if eb else -1
            buf_attr = len(eb.changedAttributeValues()) if eb else -1
            buf_geom = len(eb.changedGeometries()) if eb else -1
            flog(f"{prefix}StrictRestoreRunner: commit_started "
                 f"layer={layer_name} "
                 f"buf_add={buf_add} buf_del={buf_del} "
                 f"buf_attr={buf_attr} buf_geom={buf_geom}")
            if not layer.commitChanges():
                errors = layer.commitErrors()
                msg = "; ".join(errors) if errors else "Commit failed"
                flog(f"{prefix}StrictRestoreRunner: commit_failed "
                     f"msg={msg}", "ERROR")
                layer.rollBack()
                for eid in self._strict_succeeded:
                    self._all_succeeded_ids.discard(eid)
                self._errors.append(f"Commit failed: {msg}")
                # BL-RW-P4-21: commit failure rolls back every action
                # in the layer; per-group bucket accumulators no longer
                # represent reality. Credit the whole layer as
                # failed_other and discard the per-group counters.
                total_actions = len(self._strict_plan.actions)
                self._failed_other += total_actions
                self._strict_group_applied = 0
                self._strict_group_applied_partial = 0
                self._strict_group_dropped = []
                self._strict_group_skipped_idempotent = 0
                self._strict_group_failed_other = 0
                self._strict_group_failed_target_absent = 0
                self._strict_group_failed_geometry_drift = 0
                self._total_fail += total_actions
                layer.triggerRepaint()
                QTimer.singleShot(0, self._advance_group)
                return
            commit_ms = int((time.monotonic() - commit_start) * 1000)
            flog(f"{prefix}StrictRestoreRunner: commit_done "
                 f"layer={layer_name} commit_elapsed_ms={commit_ms}")

        ok_count = len(self._strict_succeeded)
        skipped_count = len(self._strict_skipped)
        # BL-RW-P4-21: skipped-idempotent events are successes per the
        # audit_backend contract (`succeeded includes idempotent skips`)
        # and must contribute to total_ok so the breakdown invariant
        # bucket_sum == total_ok + total_fail holds.
        self._total_ok += ok_count + skipped_count
        # BL-RW-P4-22: soft failures (target_absent, geometry_drift,
        # failed_other) accumulated during this layer batch must be
        # credited to _total_fail so the invariant
        # bucket_sum == total_ok + total_fail holds even when the layer
        # commits with partial soft-fail isolation enabled. We compute
        # the delta from total_actions - (ok + skipped) to avoid the
        # double-count that would happen if we naively summed the
        # bucket accumulators: _buffer_update can return success=True,
        # skipped=True, reason_code=geometry_drift (drift skipped case),
        # which legitimately increments BOTH the skipped accumulator
        # (via _strict_skipped) and the failed_geometry_drift bucket
        # (for the breakdown). Counting that event as both ok and fail
        # would break the invariant; treating it as ok (already in
        # skipped_count) is the right call here.
        total_actions = len(self._strict_plan.actions)
        soft_fail_count = max(0, total_actions - ok_count - skipped_count)
        self._total_fail += soft_fail_count
        # BL-RW-P4-21: commit succeeded -> the per-group bucket
        # accumulators reflect the final state and can be merged into
        # the global breakdown counters.
        self._applied += self._strict_group_applied
        # RLU-053: the incomplete writes are on disk now, so their count
        # and their abandoned columns become part of the run report.
        self._applied_partial += self._strict_group_applied_partial
        self._merge_dropped(self._partial_dropped, self._strict_group_dropped)
        self._skipped_idempotent += self._strict_group_skipped_idempotent
        self._failed_other += self._strict_group_failed_other
        self._failed_target_absent += self._strict_group_failed_target_absent
        self._failed_geometry_drift += self._strict_group_failed_geometry_drift
        flog(f"{prefix}StrictRestoreRunner: commit_breakdown "
             f"layer={layer_name} ok={ok_count} skipped={skipped_count} "
             f"soft_fail={soft_fail_count} "
             f"applied_partial={self._strict_group_applied_partial} "
             f"dropped_fields={self._strict_group_dropped} "
             f"failed_target_absent={self._strict_group_failed_target_absent} "
             f"failed_geometry_drift={self._strict_group_failed_geometry_drift} "
             f"failed_other={self._strict_group_failed_other}")
        self._strict_group_applied = 0
        self._strict_group_applied_partial = 0
        self._strict_group_dropped = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0

        for eid in self._strict_succeeded:
            event = self._strict_events_by_id.get(eid)
            if event is not None:
                trace = build_restore_trace_event(event, layer)
                if trace is not None:
                    self._traces.append(trace)

        self._log_post_commit_state(layer, layer_name, prefix)

        count_after = layer.featureCount()
        count_before = getattr(self, '_strict_count_before', -1)
        if ok_count > 0:
            layer.reload()
            count_after_reload = layer.featureCount()
        else:
            count_after_reload = count_after
        layer.triggerRepaint()

        if skipped_count > 0:
            flog(f"{prefix}StrictRestoreRunner: skipped "
                 f"layer={layer_name} count={skipped_count} "
                 f"reason=target_absent_or_identity_mismatch", "WARNING")

        flog(f"{prefix}StrictRestoreRunner: layer={layer_name} "
             f"ok={ok_count} apply_elapsed_ms={apply_ms} "
             f"feat_before={count_before} feat_after_commit={count_after} "
             f"feat_after_reload={count_after_reload}")
        QTimer.singleShot(0, self._advance_group)

    def _finish(self) -> None:
        status = "cancelled" if self._cancelled else (
            "completed" if self._total_fail == 0 else "partial")
        succeeded_by_ds: Dict[str, list] = {}
        for fp, events in self._by_ds.items():
            ok_events = [e for e in events
                         if e.event_id in self._all_succeeded_ids]
            if ok_events:
                succeeded_by_ds[fp] = ok_events
        excluded_from_undo = sum(len(v) for v in self._by_ds.values()) - sum(
            len(v) for v in succeeded_by_ds.values())
        if excluded_from_undo > 0:
            flog(f"StrictRestoreRunner: by_ds filtered: "
                 f"{excluded_from_undo} event(s) excluded from undo scope")
        # RLU-054: `excluded_from_undo` used to be published as
        # `apply_skipped`. It is an EVENT-level number (every event that
        # did not fully succeed, whatever the reason) and it was printed
        # next to the ACTION-level apply_ok/apply_fail, which already
        # totalled the plan: 73 actions came out as 14+67+59 = 140
        # results. It stays a log line about the undo scope, which is
        # what it measures; the summary now publishes the buckets.
        cancelled = sum(
            len(group) for _fp, group in self._groups[self._group_idx:]
        )
        if cancelled:
            flog(f"StrictRestoreRunner: cancelled "
                 f"actions_never_attempted={cancelled} "
                 f"groups_left={len(self._groups) - self._group_idx}",
                 "WARNING")
        merged_extra: Dict[str, int] = dict(self._extra_stats)
        merged_extra["plan_skipped"] = self._plan_skipped
        if self._applied_partial:
            # RLU-053: named once per run, before the dialog reads them,
            # so the log tells the same story as the message the user
            # gets even when the dialog is closed mid-run.
            flog(f"StrictRestoreRunner: partial_restore "
                 f"features={self._applied_partial} "
                 f"dropped_fields={self._partial_dropped}", "WARNING")
        _finish_runner(
            self,
            runner_name="StrictRestoreRunner",
            by_ds=succeeded_by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=self._traces,
            write_queue=self._write_queue,
            started_at=self._started_at,
            tracker=self._tracker,
            trace_id=self._trace_id,
            status_label=status,
            extra_stats=merged_extra,
            plan_actions=self._plan_actions + cancelled,
            breakdown={
                "applied": self._applied,
                "applied_partial": self._applied_partial,
                "skipped_idempotent": self._skipped_idempotent,
                "failed": self._failed_other,
                "failed_target_absent": self._failed_target_absent,
                "failed_geometry_drift": self._failed_geometry_drift,
                "cancelled": cancelled,
            },
        )


class UndoRunner(QObject):
    """Stepped undo executor (same pattern as RestoreRunner)."""

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(object)

    def __init__(
        self,
        by_ds: Dict[str, list],
        find_layer_fn: Callable[[AuditEvent], object],
        tracker=None,
        parent: Optional[QObject] = None,
    ):
        super().__init__(parent)
        self._by_ds = by_ds
        self._find_layer_fn = find_layer_fn
        self._tracker = tracker
        self._cancelled = False

        self._groups = list(by_ds.items())
        self._group_idx = 0
        self._total_events = sum(len(g) for g in by_ds.values())

        self._total_ok = 0
        self._total_fail = 0
        self._errors: List[str] = []
        self._failed_eids: List[int] = []
        self._processed = 0

        # Per-layer chunk state (BL-RW-P5-27), reset at every
        # _process_next_group and consumed by _process_undo_chunk.
        self._undo_layer = None
        self._undo_layer_name = ""
        self._undo_group: List[AuditEvent] = []
        self._undo_context: Optional[UndoBatchContext] = None
        self._undo_action_idx = 0
        self._undo_ok = 0
        self._undo_fail = 0
        self._undo_count_before = -1

    def start(self) -> None:
        total_events = sum(len(g) for g in self._by_ds.values())
        flog(f"UndoRunner: start layers={len(self._groups)} "
             f"total_events={total_events}")
        if self._tracker is not None:
            self._tracker.suppress()
        try:
            self._process_next_group()
        except BaseException as exc:
            _abort_started_run(self, "UndoRunner", exc)
            raise

    def cancel(self) -> None:
        self._cancelled = True

    def _process_next_group(self) -> None:
        """Open the next layer group, then hand over to the chunk loop."""
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return

        fp, group = self._groups[self._group_idx]
        layer_name_hint = group[0].layer_name_snapshot if group else '?'
        flog(f"UndoRunner: process_group idx={self._group_idx} "
             f"layer={layer_name_hint!r} n_events={len(group)}")
        # BL-RW-P5-27: this dump cost 130 ms of frozen UI for 1340 events
        # BEFORE any useful work (measured, trace a1517527). The aggregated
        # line above says how many; the detail is worth an opt-in, not a
        # freeze.
        if heavy_diag_enabled():
            for e in group:
                flog(f"  UndoRunner event: op={e.operation_type} "
                     f"eid={e.event_id} "
                     f"identity={(e.feature_identity_json or '')[:80]}")
        layer, errors = _resolve_runner_layer(
            self._find_layer_fn, group, fp,
            runner_name="UndoRunner",
            uncommitted_msg="Target layer has uncommitted edits",
        )
        if layer is None:
            self._errors.extend(errors)
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, self._total_events)
            self._group_idx += 1
            QTimer.singleShot(0, self._process_next_group)
            return

        context = undo_prepare(layer, group)
        if context.layer_error:
            flog(f"UndoRunner: layer refused layer={layer_name_hint!r} "
                 f"reason={context.layer_error}", "ERROR")
            for event in group:
                eid = event.event_id or 0
                self._errors.append(f"Evt {eid}: {context.layer_error}")
                self._failed_eids.append(eid)
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, self._total_events)
            self._group_idx += 1
            QTimer.singleShot(0, self._process_next_group)
            return

        # Per-layer state, built once and carried across every tick of this
        # group: rebuilding `ordered` or `fid_cache` mid-group would change
        # the result, not just the cost (see UndoBatchContext).
        self._undo_layer = layer
        self._undo_layer_name = layer_name_hint
        self._undo_group = group
        self._undo_context = context
        self._undo_action_idx = 0
        self._undo_ok = 0
        self._undo_fail = 0
        self._undo_count_before = layer.featureCount()
        QTimer.singleShot(0, self._process_undo_chunk)

    def _process_undo_chunk(self) -> None:
        """Apply a time-bounded slice of the current layer group."""
        try:
            self._process_undo_chunk_inner()
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            flog(f"UndoRunner: CRASH in chunk: {exc}", "CRITICAL")
            self._errors.append(f"Internal error: {exc}")
            if self._undo_context is not None:
                self._finish_undo_group()
            else:
                # The group closed itself before raising, and its `finally`
                # already advanced the cursor and armed the next step.
                # Closing again would skip a layer and put two chains on
                # the same runner.
                flog("UndoRunner: crash after group close, chain already armed",
                     "WARNING")

    def _process_undo_chunk_inner(self) -> None:
        layer = self._undo_layer
        context = self._undo_context
        ordered = context.ordered
        start = self._undo_action_idx
        chunk_t0 = time.monotonic()

        if self._cancelled:
            # Stop where we are: what is already written stays written, and
            # the untouched tail is reported as unprocessed by _finish.
            flog(f"UndoRunner: cancel_requested "
                 f"layer={self._undo_layer_name!r} "
                 f"applied={self._undo_ok} of {len(ordered)}")
            self._finish_undo_group()
            return

        # The layer was cleared by `_resolve_runner_layer` and
        # `undo_prepare` before the FIRST tick only. Chunking is what makes
        # QGIS answer during the run, so the user can now open an edit
        # session on this very layer between two ticks -- and the writes
        # below go straight to the provider, under his buffer: his next
        # Save would replay his values over what we just reverted, on a
        # layer this runner reports as fully undone.
        if hasattr(layer, 'isEditable') and layer.isEditable():
            msg = ("Couche passee en edition pendant l'annulation ; "
                   "evenements restants abandonnes")
            flog(f"UndoRunner: layer became editable mid-group "
                 f"layer={self._undo_layer_name!r} "
                 f"applied={self._undo_ok} of {len(ordered)}", "ERROR")
            for event in ordered[self._undo_action_idx:]:
                eid = event.event_id or 0
                self._undo_fail += 1
                self._errors.append(f"Evt {eid}: {msg}")
                self._failed_eids.append(eid)
            self._undo_action_idx = len(ordered)
            self._finish_undo_group()
            return

        i = start
        while i < len(ordered):
            elapsed_ms = (time.monotonic() - chunk_t0) * 1000.0
            if _chunk_should_yield(elapsed_ms, i - start):
                break
            event = ordered[i]
            i += 1
            result = undo_apply_one(layer, event, context)
            eid = event.event_id or 0
            if result["success"]:
                self._undo_ok += 1
            else:
                self._undo_fail += 1
                self._errors.append(
                    f"Evt {eid}: {result.get('message', 'echec non decrit')}")
                self._failed_eids.append(eid)

        n_done = i - start
        self._undo_action_idx = i
        chunk_ms = (time.monotonic() - chunk_t0) * 1000.0
        if chunk_ms >= _CHUNK_SLOW_MS:
            # One action alone can blow the budget (the budget is only
            # tested BETWEEN actions); saying so names the layer that needs
            # an index rather than leaving a silent stall.
            flog(f"UndoRunner: CHUNK_SLOW layer={self._undo_layer_name!r} "
                 f"actions={start}..{i} n={n_done} "
                 f"elapsed_ms={int(chunk_ms)} "
                 f"feat_count={self._undo_count_before}", "WARNING")

        # Progress is emitted per CHUNK, not per layer: one emission per
        # layer left the bar frozen for 118 s on the user's run.
        self.progress.emit(self._processed + self._undo_action_idx,
                           self._total_events)

        if self._undo_action_idx >= len(ordered):
            self._finish_undo_group()
            return
        QTimer.singleShot(0, self._process_undo_chunk)

    def _finish_undo_group(self) -> None:
        """Close the current layer: reload, repaint, account, advance.

        Two hard rules, both learned the hard way.

        1. The group state is CLAIMED on entry, before the first call that
           can raise, so this method is idempotent. It runs inside the
           ``try`` of `_process_undo_chunk`, whose crash guard calls it
           again: a second pass would credit the same events twice and
           raise a second time, this time out of a QTimer slot. The chain
           would die there -- and with it `_finish()`, whose ``finally``
           is the only thing that releases the edit tracker.
        2. Every QGIS call is guarded and the advance sits in a
           ``finally``, so no provider failure can stop the run before
           `_finish()`. This mirrors `_rollback_strict`, which always ends
           on `QTimer.singleShot(0, self._advance_group)`.

        `layer.reload()` lands HERE and nowhere else: dropped between two
        chunks it would invalidate the provider FIDs the remaining events
        of this same group still resolve against.
        """
        layer = self._undo_layer
        layer_name_hint = self._undo_layer_name
        group = self._undo_group
        context = self._undo_context
        n_ok, n_fail = self._undo_ok, self._undo_fail
        # Claimed before anything fallible runs.
        self._undo_layer = None
        self._undo_context = None
        self._undo_group = []
        self._undo_ok = 0
        self._undo_fail = 0

        try:
            if context is None or layer is None:
                flog("UndoRunner: finish_group called with no open group, "
                     "advancing", "WARNING")
                return
            ordered = context.ordered

            self._total_ok += n_ok
            self._total_fail += n_fail

            count_after = -1
            try:
                if n_ok:
                    layer.reload()
                    if heavy_diag_enabled():
                        import json as _json
                        geom_check = {}
                        for ev in group:
                            if getattr(ev, 'new_geometry_wkb', None) is not None:
                                try:
                                    fid = _json.loads(
                                        ev.feature_identity_json).get('fid')
                                    if fid is not None:
                                        geom_check[fid] = feature_geom_short_repr(
                                            layer, fid)
                                except Exception:
                                    pass
                        flog(f"RELOAD_CHECK layer={layer_name_hint!r} "
                             f"geom_after_reload={geom_check}")
                    else:
                        flog(f"RELOAD_CHECK layer={layer_name_hint!r} "
                             f"geom_after_reload=diag_off "
                             f"(set RECOVERLAND_HEAVY_DIAG=1 to enable)")
                count_after = layer.featureCount()
                layer.triggerRepaint()
            except Exception as exc:  # noqa: BLE001 - layer may be gone
                # The writes already landed; only the refresh failed. Say
                # so and keep going -- the alternative is a dead chain.
                flog(f"UndoRunner: post-apply refresh failed "
                     f"layer={layer_name_hint!r}: {exc}", "ERROR")
                self._errors.append(
                    f"Couche '{layer_name_hint}' non rafraichie: {exc}")

            flog(f"UndoRunner: group_done layer={layer_name_hint!r} "
                 f"ok={n_ok} fail={n_fail} "
                 f"processed={self._undo_action_idx}/{len(ordered)} "
                 f"feat_before={self._undo_count_before} "
                 f"feat_after={count_after}")

            # A cancelled group leaves a tail nobody applied; it must still
            # be accounted, otherwise _processed never reaches
            # _total_events and the bar stops short of the end.
            self._processed += len(group)
            self.progress.emit(self._processed, self._total_events)
        finally:
            self._group_idx += 1
            QTimer.singleShot(0, self._process_next_group)

    def _finish(self) -> None:
        _finish_runner(
            self,
            runner_name="UndoRunner",
            by_ds=self._by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=[],
            write_queue=None,
            started_at=0.0,
            tracker=self._tracker,
            failed_eids=self._failed_eids,
        )
