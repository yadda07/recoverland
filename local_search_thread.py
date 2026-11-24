# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Background thread for searching the local SQLite audit journal."""
from qgis.PyQt.QtCore import pyqtSignal

from .core import flog, search_events
from .qgs_task_support import (
    TaskEnabledThread, open_worker_read_connection, trace_prefix,
)


def _run_search(journal, criteria, trace_id, phase_callback, is_cancelled,
                journal_path=None, register_conn=None, unregister_conn=None):
    conn = None
    prefix = trace_prefix(trace_id)
    try:
        phase_callback("Connexion au journal local...")
        if is_cancelled():
            return None
        conn = open_worker_read_connection(journal, journal_path)
        if register_conn is not None:
            register_conn(conn)
        if is_cancelled():
            return None
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
        if unregister_conn is not None:
            unregister_conn()
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                flog(f"{prefix}LocalSearchThread: close error: {e}", "WARNING")


def _run_search_task(task, journal, criteria, trace_id, phase_callback,
                     journal_path=None, register_conn=None, unregister_conn=None):
    return _run_search(
        journal, criteria, trace_id, phase_callback, task.isCanceled,
        journal_path=journal_path,
        register_conn=register_conn,
        unregister_conn=unregister_conn,
    )


class LocalSearchThread(TaskEnabledThread):
    """Search the SQLite audit journal in a background thread."""

    results_ready = pyqtSignal(object)
    phase_changed = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, journal, criteria, trace_id: str = ""):
        super().__init__(trace_id=trace_id)
        self._journal = journal
        self._journal_path = getattr(journal, "path", None)
        self._criteria = criteria

    def run(self):
        try:
            result = _run_search(
                self._journal,
                self._criteria,
                self._trace_id,
                self.phase_changed.emit,
                lambda: self._stopped,
                journal_path=self._journal_path,
                register_conn=self._register_conn,
                unregister_conn=self._unregister_conn,
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
            journal_path=self._journal_path,
            register_conn=self._register_conn,
            unregister_conn=self._unregister_conn,
        )
        flog(f"{prefix}LocalSearchThread: submitted to QgsTaskManager")

    def _on_task_finished(self, exception, result=None) -> None:
        self._handle_task_finished(
            exception, result, self.results_ready, "LocalSearchThread"
        )
