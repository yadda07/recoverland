"""Non-blocking restore runner for RecoverLand.

Processes restore operations in small batches on the main thread,
yielding to the Qt event loop between batches so the UI stays responsive.

QGIS layer operations (provider.addFeatures, etc.) must run on the
main thread; this runner respects that constraint while avoiding freeze.
"""
from collections import defaultdict
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
from .core.geometry_utils import (
    feature_geom_short_repr, wkb_short_repr,
)

_CHUNK_SIZE = 20


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
        )
        elapsed_ms = (
            int((time.monotonic() - started_at) * 1000)
            if started_at else 0
        )
        if status_label is not None:
            flog(f"{prefix}{runner_name}: finish status={status_label} "
                 f"ok={total_ok} fail={total_fail} elapsed_ms={elapsed_ms}")
        else:
            flog(f"{prefix}{runner_name}: finish ok={total_ok} "
                 f"fail={total_fail} elapsed_ms={elapsed_ms}")
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

        report = restore_batch(layer, chunk, fid_cache=self._group_fid_cache)
        self._total_ok += len(report.succeeded)
        self._group_ok += len(report.succeeded)
        self._total_fail += len(report.failed)
        self._traces.extend(report.trace_events)
        for eid, msg in report.failed.items():
            self._errors.append(f"Evt {eid}: {msg}")

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
    ):
        super().__init__(parent)
        self._events = events
        self._find_layer_fn = find_layer_fn
        self._cutoff = cutoff
        self._write_queue = write_queue
        self._tracker = tracker
        self._trace_id = trace_id
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
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, len(self._events))
            QTimer.singleShot(0, self._advance_group)
            return

        layer_issues = preflight_layer_check(plan, layer)
        if layer_issues:
            msg = "; ".join(layer_issues[:3])
            self._errors.append(f"Layer check failed: {msg}")
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

        if not self._strict_was_editing:
            if not layer.startEditing():
                flog(f"{prefix}StrictRestoreRunner: startEditing failed", "ERROR")
                self._errors.append("Cannot start editing on layer")
                self._total_fail += len(group)
                self._processed += len(group)
                self.progress.emit(self._processed, len(self._events))
                QTimer.singleShot(0, self._advance_group)
                return

        count_before = layer.featureCount()
        self._strict_count_before = count_before

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
            )
            if result["success"]:
                if result.get("skipped"):
                    self._strict_skipped.append(action.event_id)
                else:
                    self._strict_succeeded.append(action.event_id)
            else:
                flog(f"{prefix}StrictRestoreRunner: action_failed "
                     f"eid={action.event_id} comp_op={action.compensatory_op} "
                     f"orig_op={action.operation_type} msg={result['message']}",
                     "ERROR")
                self._errors.append(
                    f"Evt {action.event_id}: {result['message']}")
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
                self._errors.append(f"Commit failed: {msg}")
                self._total_fail += len(self._strict_plan.actions)
                layer.triggerRepaint()
                QTimer.singleShot(0, self._advance_group)
                return
            commit_ms = int((time.monotonic() - commit_start) * 1000)
            flog(f"{prefix}StrictRestoreRunner: commit_done "
                 f"layer={layer_name} commit_elapsed_ms={commit_ms}")

        ok_count = len(self._strict_succeeded)
        skipped_count = len(self._strict_skipped)
        self._total_ok += ok_count

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
        _finish_runner(
            self,
            runner_name="StrictRestoreRunner",
            by_ds=self._by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=self._traces,
            write_queue=self._write_queue,
            started_at=self._started_at,
            tracker=self._tracker,
            trace_id=self._trace_id,
            status_label=status,
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
        self._processed = 0

    def start(self) -> None:
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

        report = undo_restore_batch(layer, group)
        self._total_ok += len(report.succeeded)
        self._total_fail += len(report.failed)
        for eid, msg in report.failed.items():
            self._errors.append(f"Evt {eid}: {msg}")
        if report.succeeded:
            layer.reload()
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
        )
