# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Background thread for journal statistics (BL-PERF-001).

Offloads stats cache rebuild, summarize_scope, distinct layers,
and health evaluation from the UI thread to a background worker.
Uses TaskEnabledThread for QGIS 3.40+ / 4.x compatibility.
"""
from qgis.PyQt.QtCore import pyqtSignal

from .core import (
    flog,
    LayerStatsCache,
    summarize_scope,
    get_distinct_layers,
    get_journal_size_bytes, format_journal_size,
    evaluate_journal_health,
    get_journal_stats,
)
from .qgs_task_support import (
    TaskEnabledThread, open_worker_read_connection, trace_prefix,
)


class StatsResult:
    """Bundle of stats computed in background."""
    __slots__ = (
        'stats_cache', 'summary', 'layers', 'size_str',
        'health', 'trace_id',
    )

    def __init__(self):
        self.stats_cache = None
        self.summary = None
        self.layers = []
        self.size_str = ""
        self.health = None
        self.trace_id = ""


def _run_stats(journal, criteria, trace_id, is_cancelled,
               journal_path=None, register_conn=None, unregister_conn=None):
    """Execute stats queries on a short-lived read connection."""
    prefix = trace_prefix(trace_id)
    conn = None
    result = StatsResult()
    result.trace_id = trace_id

    try:
        path = journal_path or (getattr(journal, "path", None) if journal is not None else None)
        if not path:
            if journal is None or not getattr(journal, "is_open", False):
                return result
            path = journal.path
        if not path:
            return result

        result.size_str = format_journal_size(get_journal_size_bytes(path))

        if is_cancelled():
            return result

        conn = open_worker_read_connection(journal, path)
        if register_conn is not None:
            register_conn(conn)
        if is_cancelled():
            return result

        cache = LayerStatsCache()
        cache.build(conn)
        result.stats_cache = cache

        if is_cancelled():
            return result

        result.layers = get_distinct_layers(conn)

        if is_cancelled():
            return result

        if criteria is not None:
            result.summary = summarize_scope(conn, criteria)

        if is_cancelled():
            return result

        size_bytes = get_journal_size_bytes(path)
        try:
            stats = get_journal_stats(conn)
            result.health = evaluate_journal_health(
                size_bytes,
                stats["total_events"],
                stats.get("oldest_event", ""),
                stats.get("newest_event", ""),
            )
        except Exception as e:
            flog(f"{prefix}stats_thread: health error: {e}", "WARNING")
            result.health = evaluate_journal_health(size_bytes, 0, "", "")

        return result
    except Exception as e:
        if is_cancelled():
            return result
        flog(f"{prefix}stats_thread: error: {e}", "ERROR")
        raise
    finally:
        if unregister_conn is not None:
            unregister_conn()
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001
                # This finally also runs while the `raise` above propagates.
                # flog is proven usable on that path (it just logged the
                # error) and logging swallows handler failures, so the
                # pending exception still reaches the caller untouched.
                flog(f"{prefix}stats_thread: read connection close failed "
                     f"({exc!r})", "DEBUG")


def _run_stats_task(task, journal, criteria, trace_id,
                    journal_path=None, register_conn=None, unregister_conn=None):
    return _run_stats(
        journal, criteria, trace_id, task.isCanceled,
        journal_path=journal_path,
        register_conn=register_conn,
        unregister_conn=unregister_conn,
    )


class JournalStatsThread(TaskEnabledThread):
    """Compute journal stats in background thread."""

    stats_ready = pyqtSignal(object)
    error_occurred = pyqtSignal(str)

    def __init__(self, journal, criteria=None, trace_id=""):
        super().__init__(trace_id=trace_id)
        self._journal = journal
        self._journal_path = getattr(journal, "path", None)
        self._criteria = criteria

    def run(self):
        try:
            result = _run_stats(
                self._journal,
                self._criteria,
                self._trace_id,
                lambda: self._stopped,
                journal_path=self._journal_path,
                register_conn=self._register_conn,
                unregister_conn=self._unregister_conn,
            )
            if not self._stopped:
                self.stats_ready.emit(result)
        except Exception as e:
            flog(f"{trace_prefix(self._trace_id)}StatsThread: error: {e}", "ERROR")
            if not self._stopped:
                self.error_occurred.emit(str(e))
        finally:
            self._clear_task()

    def _start_task(self):
        self._submit_task(
            "RecoverLand journal stats",
            _run_stats_task,
            on_finished=self._on_task_finished,
            journal=self._journal,
            criteria=self._criteria,
            trace_id=self._trace_id,
            journal_path=self._journal_path,
            register_conn=self._register_conn,
            unregister_conn=self._unregister_conn,
        )

    def _on_task_finished(self, exception, result=None):
        self._handle_task_finished(
            exception, result, self.stats_ready, "StatsThread"
        )
