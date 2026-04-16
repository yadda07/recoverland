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
)
from .core.workflow_service import GroupedRestoreResult
from .core.logger import flog

_CHUNK_SIZE = 20


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

        layer = self._find_layer_fn(group[0])
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        if layer is None:
            name = group[0].layer_name_snapshot or fp
            flog(f"{prefix}RestoreRunner: layer NOT FOUND name='{name}' fp={fp} events={len(group)}", "ERROR")
            self._errors.append(f"Couche '{name}' non trouvee dans le projet.")
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, len(self._events))
            self._group_idx += 1
            QTimer.singleShot(0, self._advance_group)
            return

        if hasattr(layer, 'isEditable') and layer.isEditable():
            name = group[0].layer_name_snapshot or fp
            if hasattr(layer, 'isModified') and layer.isModified():
                flog(f"{prefix}RestoreRunner: layer HAS UNSAVED EDITS name='{name}' fp={fp}", "ERROR")
                msg = "Target layer has uncommitted edits; commit or rollback before restore"
                for event in group:
                    self._errors.append(f"Evt {event.event_id or 0}: {msg}")
                self._total_fail += len(group)
                self._processed += len(group)
                self.progress.emit(self._processed, len(self._events))
                self._group_idx += 1
                QTimer.singleShot(0, self._advance_group)
                return
            flog(f"{prefix}RestoreRunner: auto-closing edit session name='{name}' fp={fp}")
            layer.commitChanges()

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
                layer.reload()
            layer.triggerRepaint()
            self._group_idx += 1
            QTimer.singleShot(0, self._advance_group)
        else:
            QTimer.singleShot(0, self._process_chunk)

    def _finish(self) -> None:
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        try:
            if self._traces and self._write_queue is not None:
                accepted = self._write_queue.enqueue(list(self._traces))
                if not accepted:
                    msg = (
                        "Journal trace write failed; restore data changes"
                        " succeeded but trace events were saved"
                        " for pending recovery"
                    )
                    self._errors.append(msg)
                    flog(f"RestoreRunner: {msg}", "ERROR")

            result = GroupedRestoreResult(
                total_ok=self._total_ok,
                total_fail=self._total_fail,
                errors=self._errors,
                by_ds=dict(self._by_ds),
                trace_events=self._traces,
            )
            elapsed_ms = int((time.monotonic() - self._started_at) * 1000) if self._started_at else 0
            flog(f"{prefix}RestoreRunner: finish ok={self._total_ok} fail={self._total_fail} elapsed_ms={elapsed_ms}")
            self.finished.emit(result)
        finally:
            if self._tracker is not None:
                self._tracker.unsuppress()


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
        layer = self._find_layer_fn(group[0])
        if layer is None:
            name = group[0].layer_name_snapshot or fp
            flog(f"UndoRunner: layer NOT FOUND name='{name}' fp={fp} events={len(group)}", "ERROR")
            self._errors.append(f"Couche '{name}' non trouvee dans le projet.")
            self._total_fail += len(group)
            self._processed += len(group)
            self.progress.emit(self._processed, self._total_events)
            self._group_idx += 1
            QTimer.singleShot(0, self._process_next_group)
            return

        if hasattr(layer, 'isEditable') and layer.isEditable():
            name = group[0].layer_name_snapshot or fp
            if hasattr(layer, 'isModified') and layer.isModified():
                flog(f"UndoRunner: layer HAS UNSAVED EDITS name='{name}' fp={fp}", "ERROR")
                msg = "Target layer has uncommitted edits"
                for event in group:
                    self._errors.append(f"Evt {event.event_id or 0}: {msg}")
                self._total_fail += len(group)
                self._processed += len(group)
                self.progress.emit(self._processed, self._total_events)
                self._group_idx += 1
                QTimer.singleShot(0, self._process_next_group)
                return
            flog(f"UndoRunner: auto-closing edit session name='{name}' fp={fp}")
            layer.commitChanges()

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
        try:
            result = GroupedRestoreResult(
                total_ok=self._total_ok,
                total_fail=self._total_fail,
                errors=self._errors,
                by_ds=dict(self._by_ds),
                trace_events=[],
            )
            self.finished.emit(result)
        finally:
            if self._tracker is not None:
                self._tracker.unsuppress()
