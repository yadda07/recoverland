"""Background thread for searching the local SQLite audit journal."""
from qgis.PyQt.QtCore import QThread, pyqtSignal

from .core import flog, search_events


class LocalSearchThread(QThread):
    """Search the SQLite audit journal in a background thread."""

    results_ready = pyqtSignal(object)
    phase_changed = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, journal, criteria):
        super().__init__()
        self._journal = journal
        self._criteria = criteria
        self._stopped = False

    def stop(self):
        self._stopped = True

    def run(self):
        conn = None
        try:
            self.phase_changed.emit("Connexion au journal local...")
            conn = self._journal.create_read_connection()
            self.phase_changed.emit("Recherche en cours...")
            flog(f"LocalSearchThread: criteria fp={self._criteria.datasource_fingerprint} "
                 f"op={self._criteria.operation_type} "
                 f"start={self._criteria.start_date} end={self._criteria.end_date}")
            result = search_events(conn, self._criteria)
            flog(f"LocalSearchThread: found {result.total_count} events, "
                 f"{len(result.events)} returned")
            if not self._stopped:
                self.results_ready.emit(result)
        except Exception as e:
            flog(f"LocalSearchThread: error: {e}", "ERROR")
            if not self._stopped:
                self.error_occurred.emit(str(e))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
