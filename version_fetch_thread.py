"""Background thread for fetching temporal restore events from SQLite.

Moves the synchronous count + fetch queries out of the main thread
to prevent UI freezing on large journals (CRT-01).
"""
from qgis.PyQt.QtCore import pyqtSignal

from .core.event_stream_repository import (
    fetch_events_after_cutoff, count_events_after_cutoff,
)
from .core.logger import flog
from .qgs_task_support import TaskEnabledThread, trace_prefix


def _run_fetch(journal, fingerprints, cutoff, trace_id, count_callback, is_cancelled):
    conn = None
    prefix = trace_prefix(trace_id)
    try:
        conn = journal.create_read_connection()
        total = 0
        for fp in fingerprints:
            if is_cancelled():
                return None
            total += count_events_after_cutoff(conn, fp, cutoff, trace_id=trace_id)
        if is_cancelled():
            return None
        count_callback(total)
        if total == 0:
            return []
        events = []
        for fp in fingerprints:
            if is_cancelled():
                return None
            events.extend(fetch_events_after_cutoff(conn, fp, cutoff, trace_id=trace_id))
        events.sort(key=lambda e: (e.created_at or "", e.event_id or 0), reverse=True)
        flog(f"{prefix}VersionFetchThread: fetched {len(events)} events")
        if is_cancelled():
            return None
        return events
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                flog(f"{prefix}VersionFetchThread: close error: {e}", "WARNING")


def _run_fetch_task(task, journal, fingerprints, cutoff, trace_id, count_callback):
    return _run_fetch(journal, fingerprints, cutoff, trace_id, count_callback, task.isCanceled)


class VersionFetchThread(TaskEnabledThread):
    """Fetch events after a temporal cutoff in a background thread."""

    count_ready = pyqtSignal(int)
    events_ready = pyqtSignal(list)
    error_occurred = pyqtSignal(str)

    def __init__(self, journal, fingerprints, cutoff, trace_id: str = ""):
        super().__init__(trace_id=trace_id)
        self._journal = journal
        self._fingerprints = list(fingerprints)
        self._cutoff = cutoff

    def run(self):
        try:
            events = _run_fetch(
                self._journal,
                self._fingerprints,
                self._cutoff,
                self._trace_id,
                self.count_ready.emit,
                lambda: self._stopped,
            )
            if not self._stopped and events is not None:
                self.events_ready.emit(events)

        except Exception as e:
            flog(f"{trace_prefix(self._trace_id)}VersionFetchThread: error: {e}", "ERROR")
            if not self._stopped:
                self.error_occurred.emit(str(e))
        finally:
            self._clear_task()

    def _start_task(self) -> None:
        prefix = trace_prefix(self._trace_id)
        self._submit_task(
            "RecoverLand temporal fetch",
            _run_fetch_task,
            on_finished=self._on_task_finished,
            journal=self._journal,
            fingerprints=self._fingerprints,
            cutoff=self._cutoff,
            trace_id=self._trace_id,
            count_callback=self.count_ready.emit,
        )
        flog(f"{prefix}VersionFetchThread: submitted to QgsTaskManager")

    def _on_task_finished(self, exception, result=None) -> None:
        self._handle_task_finished(
            exception, result, self.events_ready, "VersionFetchThread"
        )
