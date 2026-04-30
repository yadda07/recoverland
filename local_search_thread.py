"""Background thread for searching the local SQLite audit journal."""
from qgis.PyQt.QtCore import pyqtSignal

from .core import flog, search_events
from .qgs_task_support import TaskEnabledThread, trace_prefix


def _run_search(journal, criteria, trace_id, phase_callback, is_cancelled):
    conn = None
    prefix = trace_prefix(trace_id)
    try:
        phase_callback("Connexion au journal local...")
        if is_cancelled():
            return None
        conn = journal.create_read_connection()
        phase_callback("Recherche en cours...")
        flog(
            f"{prefix}LocalSearchThread: criteria fp={criteria.datasource_fingerprint}"
            f" op={criteria.operation_type} start={criteria.start_date}"
            f" end={criteria.end_date}"
        )
        result = search_events(conn, criteria, trace_id=trace_id,
                               exclude_blobs=True)
        flog(f"{prefix}LocalSearchThread: found {result.total_count} events, {len(result.events)} returned")
        if is_cancelled():
            return None
        return result
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                flog(f"{prefix}LocalSearchThread: close error: {e}", "WARNING")


def _run_search_task(task, journal, criteria, trace_id, phase_callback):
    return _run_search(journal, criteria, trace_id, phase_callback, task.isCanceled)


class LocalSearchThread(TaskEnabledThread):
    """Search the SQLite audit journal in a background thread."""

    results_ready = pyqtSignal(object)
    phase_changed = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, journal, criteria, trace_id: str = ""):
        super().__init__(trace_id=trace_id)
        self._journal = journal
        self._criteria = criteria

    def run(self):
        try:
            result = _run_search(
                self._journal,
                self._criteria,
                self._trace_id,
                self.phase_changed.emit,
                lambda: self._stopped,
            )
            if not self._stopped:
                self.results_ready.emit(result)
        except Exception as e:
            flog(f"{trace_prefix(self._trace_id)}LocalSearchThread: error: {e}", "ERROR")
            if not self._stopped:
                self.error_occurred.emit(str(e))
        finally:
            self._clear_task()

    def _start_task(self) -> None:
        prefix = trace_prefix(self._trace_id)
        self._submit_task(
            "RecoverLand local search",
            _run_search_task,
            on_finished=self._on_task_finished,
            journal=self._journal,
            criteria=self._criteria,
            trace_id=self._trace_id,
            phase_callback=self.phase_changed.emit,
        )
        flog(f"{prefix}LocalSearchThread: submitted to QgsTaskManager")

    def _on_task_finished(self, exception, result=None) -> None:
        self._handle_task_finished(
            exception, result, self.results_ready, "LocalSearchThread"
        )
