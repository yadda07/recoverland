"""Non-blocking restore runner for RecoverLand.

Processes restore operations in small batches on the main thread,
yielding to the Qt event loop between batches so the UI stays responsive.

QGIS layer operations (provider.addFeatures, etc.) must run on the
main thread; this runner respects that constraint while avoiding freeze.
"""
from collections import defaultdict
import json
import time
from typing import List, Dict, Optional, Callable

from qgis.PyQt.QtCore import QObject, QTimer, pyqtSignal

from .core.audit_backend import AuditEvent
from .core.restore_service import (
    restore_batch,
    undo_restore_batch,
    build_fid_cache,
    build_restore_trace_event,
)
from .core.restore_executor import _apply_via_buffer
from .core.workflow_service import GroupedRestoreResult
from .core.logger import flog
from .core.observability import log_cycle_summary
from .core.geometry_utils import (
    feature_geom_short_repr, wkb_short_repr,
)

_CHUNK_SIZE = 20

_RUNNER_TO_CYCLE: Dict[str, str] = {
    "RestoreRunner": "event_restore",
    "StrictRestoreRunner": "rewind",
    "UndoRunner": "undo",
}


def _cycle_for_runner(runner_name: str) -> str:
    """Map a runner class to its CYCLE_SUMMARY cycle name."""
    return _RUNNER_TO_CYCLE.get(runner_name, runner_name.lower())


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
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    bd = breakdown or {}
    applied = int(bd.get("applied", 0))
    skipped_idempotent = int(bd.get("skipped_idempotent", 0))
    failed_other = int(bd.get("failed", 0))
    failed_target_absent = int(bd.get("failed_target_absent", 0))
    failed_geometry_drift = int(bd.get("failed_geometry_drift", 0))

    # BL-RW-P3-18 antithesis: conservation invariant.
    # Any drift between total_ok+total_fail and the 5-bucket sum exposes
    # a classification bug. We log a WARNING but do not raise; the user
    # gets correct restore results, just without trustworthy metrics.
    bucket_sum = (applied + skipped_idempotent + failed_other
                  + failed_target_absent + failed_geometry_drift)
    expected = total_ok + total_fail
    if breakdown is not None and bucket_sum != expected:
        flog(
            f"{prefix}{runner_name}: BREAKDOWN_INVARIANT_VIOLATION "
            f"bucket_sum={bucket_sum} total_ok+total_fail={expected} "
            f"delta={bucket_sum - expected} "
            f"applied={applied} skipped_idempotent={skipped_idempotent} "
            f"failed={failed_other} target_absent={failed_target_absent} "
            f"geometry_drift={failed_geometry_drift}",
            "WARNING",
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
        if status_label is not None:
            flog(f"{prefix}{runner_name}: finish status={status_label} "
                 f"ok={total_ok} fail={total_fail} elapsed_ms={elapsed_ms} "
                 f"applied={applied} skipped_idempotent={skipped_idempotent} "
                 f"failed={failed_other} "
                 f"failed_target_absent={failed_target_absent} "
                 f"failed_geometry_drift={failed_geometry_drift}")
        else:
            flog(f"{prefix}{runner_name}: finish ok={total_ok} "
                 f"fail={total_fail} elapsed_ms={elapsed_ms} "
                 f"applied={applied} skipped_idempotent={skipped_idempotent} "
                 f"failed={failed_other} "
                 f"failed_target_absent={failed_target_absent} "
                 f"failed_geometry_drift={failed_geometry_drift}")

        cycle_name = cycle or _cycle_for_runner(runner_name)
        cycle_stats: Dict[str, int] = {
            "apply_ok": total_ok,
            "apply_fail": total_fail,
            "traces_written": len(traces) if traces else 0,
            "applied": applied,
            "skipped_idempotent": skipped_idempotent,
            "failed": failed_other,
            "failed_target_absent": failed_target_absent,
            "failed_geometry_drift": failed_geometry_drift,
        }
        if extra_stats:
            cycle_stats.update(extra_stats)
        log_cycle_summary(trace_id, cycle_name, cycle_stats, elapsed_ms)

        runner.finished.emit(result)
    finally:
        if tracker is not None:
            tracker.unsuppress()


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
        self._advance_group()

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
            breakdown={
                "applied": self._applied,
                "skipped_idempotent": self._skipped_idempotent,
                "failed": self._failed_other,
                "failed_target_absent": self._failed_target_absent,
                "failed_geometry_drift": self._failed_geometry_drift,
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
        # Per-group accumulators reset at every _begin_strict_for_layer
        # and merged into the globals only on commit success.
        self._strict_group_applied = 0
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
        self._advance_group()

    def cancel(self) -> None:
        self._cancelled = True
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        flog(f"{prefix}StrictRestoreRunner: cancel_requested "
             f"applied={self._total_ok}")

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
                self._failed_other += len(group)
                self._total_fail += len(group)
                self._processed += len(group)
                self.progress.emit(self._processed, len(self._events))
                QTimer.singleShot(0, self._advance_group)
                return

        count_before = layer.featureCount()
        self._strict_count_before = count_before

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
                flog(f"EDIT_START err eid={getattr(a,'event_id','?')} exc={exc}", "WARNING")
        flog(f"EDIT_START layer={layer_name} n_geom_upd={len(upd_geom_state)} geom_at_start={upd_geom_state}")

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
        end = min(start + _CHUNK_SIZE, len(actions))

        if self._cancelled:
            self._rollback_strict(prefix, "cancel_requested")
            return

        for i in range(start, end):
            action = actions[i]
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
                else:
                    self._strict_group_applied += 1
            else:
                flog(f"{prefix}StrictRestoreRunner: action_failed "
                     f"eid={action.event_id} comp_op={action.compensatory_op} "
                     f"orig_op={action.operation_type} reason={reason} "
                     f"msg={result['message']}",
                     "ERROR")
                self._errors.append(
                    f"Evt {action.event_id}: {result['message']}")
                if reason == "target_absent":
                    self._strict_group_failed_target_absent += 1
                elif reason == "geometry_drift":
                    self._strict_group_failed_geometry_drift += 1
                else:
                    self._strict_group_failed_other += 1
                self._rollback_strict(prefix, "apply_failed")
                return

        self._strict_action_idx = end
        self._processed += (end - start)
        self.progress.emit(self._processed, len(self._events))

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
        """
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
        # BL-RW-P4-21: commit succeeded -> the per-group bucket
        # accumulators reflect the final state and can be merged into
        # the global breakdown counters.
        self._applied += self._strict_group_applied
        self._skipped_idempotent += self._strict_group_skipped_idempotent
        self._failed_other += self._strict_group_failed_other
        self._failed_target_absent += self._strict_group_failed_target_absent
        self._failed_geometry_drift += self._strict_group_failed_geometry_drift
        self._strict_group_applied = 0
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
        skipped_total = sum(len(v) for v in self._by_ds.values()) - sum(
            len(v) for v in succeeded_by_ds.values())
        if skipped_total > 0:
            flog(f"StrictRestoreRunner: by_ds filtered: "
                 f"{skipped_total} skipped event(s) excluded from undo scope")
        plan_actions_total = sum(len(v) for v in self._by_ds.values())
        merged_extra: Dict[str, int] = dict(self._extra_stats)
        merged_extra.update({
            "plan_actions": plan_actions_total,
            "apply_skipped": skipped_total,
        })
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
            breakdown={
                "applied": self._applied,
                "skipped_idempotent": self._skipped_idempotent,
                "failed": self._failed_other,
                "failed_target_absent": self._failed_target_absent,
                "failed_geometry_drift": self._failed_geometry_drift,
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

    def start(self) -> None:
        total_events = sum(len(g) for g in self._by_ds.values())
        flog(f"UndoRunner: start layers={len(self._groups)} "
             f"total_events={total_events}")
        if self._tracker is not None:
            self._tracker.suppress()
        self._process_next_group()

    def cancel(self) -> None:
        self._cancelled = True

    def _process_next_group(self) -> None:
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return

        fp, group = self._groups[self._group_idx]
        layer_name_hint = group[0].layer_name_snapshot if group else '?'
        flog(f"UndoRunner: process_group idx={self._group_idx} "
             f"layer={layer_name_hint!r} n_events={len(group)}")
        for e in group:
            flog(f"  UndoRunner event: op={e.operation_type} eid={e.event_id} "
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

        count_before = layer.featureCount()
        report = undo_restore_batch(layer, group)
        self._total_ok += len(report.succeeded)
        self._total_fail += len(report.failed)
        for eid, msg in report.failed.items():
            self._errors.append(f"Evt {eid}: {msg}")
            self._failed_eids.append(eid)
        if report.succeeded:
            layer.reload()
            import json as _json
            geom_check = {}
            for ev in group:
                if getattr(ev, 'new_geometry_wkb', None) is not None:
                    try:
                        fid = _json.loads(ev.feature_identity_json).get('fid')
                        if fid is not None:
                            geom_check[fid] = feature_geom_short_repr(layer, fid)
                    except Exception:
                        pass
            flog(f"RELOAD_CHECK layer={layer_name_hint!r} "
                 f"geom_after_reload={geom_check}")
        count_after = layer.featureCount()
        flog(f"UndoRunner: group_done layer={layer_name_hint!r} "
             f"ok={len(report.succeeded)} fail={len(report.failed)} "
             f"feat_before={count_before} feat_after={count_after}")
        layer.triggerRepaint()

        self._processed += len(group)
        self.progress.emit(self._processed, self._total_events)
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
