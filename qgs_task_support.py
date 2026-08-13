import time

from qgis.PyQt.QtCore import QCoreApplication, QThread

try:
    from qgis.core import QgsApplication, QgsTask
except ImportError:
    QgsApplication = None
    QgsTask = None


# Upper bound for joining a cancelled job. Cancelling a QgsTask is only a
# request: it is honoured at the worker's next checkpoint, and a worker
# reading a journal on a network share can take seconds to reach it. The
# bound keeps a wedged worker a slow shutdown instead of a frozen QGIS.
_CANCEL_JOIN_LIMIT_MS = 30000

# Pause between two pumps of the event loop while waiting.
_JOIN_POLL_S = 0.005


def trace_prefix(trace_id: str) -> str:
    return f"[{trace_id}] " if trace_id else ""


def supports_qgs_task() -> bool:
    return (
        QgsTask is not None and QgsApplication is not None and hasattr(QgsApplication, "taskManager")
    )


class TaskEnabledThread(QThread):
    def __init__(self, trace_id: str = ""):
        super().__init__()
        self._stopped = False
        self._running = False
        self._task = None
        self._restart_pending = False
        self._trace_id = trace_id

    def stop(self):
        self._stopped = True
        # A cancellation cancels the restart the caller may have queued:
        # the caller is giving up, not asking for a fresh run.
        self._restart_pending = False
        if self._task is not None and hasattr(self._task, "cancel"):
            self._task.cancel()

    def start(self):
        """Submit the background job. Returns True when it was accepted.

        A start refused while a CANCELLED job is still unwinding would
        vanish: that job emits nothing, so the caller waits forever on a
        result nobody will produce. It is remembered and run once the job
        releases. A start refused while a live job runs is a genuine
        duplicate (two clicks on Search) and stays refused: its result is
        already on its way, and running the query twice would only add a
        concurrent read of the journal.
        """
        if self._running:
            from .core.logger import flog
            self._restart_pending = self._stopped
            flog(
                f"{trace_prefix(self._trace_id)}{type(self).__name__}: start "
                f"refused, previous job still running "
                f"(cancelled={self._stopped} deferred={self._restart_pending})",
                "WARNING",
            )
            return False
        self._restart_pending = False
        self._stopped = False
        if supports_qgs_task():
            self._start_task()
            return True
        self._running = True
        super().start()
        return True

    def isRunning(self):
        if self._task is not None:
            return self._running
        return QThread.isRunning(self)

    def wait(self, msecs=0):
        """Block until the background job is really finished.

        On the QgsTask backend the job does not run in this QThread, so
        QThread.wait() has nothing to synchronise on. Callers write
        ``stop(); wait(500)`` as a barrier before closing the journal and
        destroying the widget, so returning early is exactly what leaves
        an orphan worker reading a closed journal.

        The job signals completion through the Qt event loop, so waiting
        means pumping that loop; sleeping alone would never observe it.
        A cancelled job is joined for at least _CANCEL_JOIN_LIMIT_MS
        whatever the caller asked for, because the caller is about to
        free the resources the worker still holds.

        Returns True once the job is finished, False on timeout, like
        QThread.wait().
        """
        if self._task is None:
            return QThread.wait(self, msecs)
        limit_ms = int(msecs) if msecs and int(msecs) > 0 else _CANCEL_JOIN_LIMIT_MS
        if self._stopped:
            limit_ms = max(limit_ms, _CANCEL_JOIN_LIMIT_MS)
        deadline = time.monotonic() + limit_ms / 1000.0
        while self._running:
            QCoreApplication.processEvents()
            if not self._running or time.monotonic() >= deadline:
                break
            time.sleep(_JOIN_POLL_S)
        if self._running:
            # Giving up: the caller will free resources the worker still
            # uses, so leave a trace of the only moment that can happen.
            from .core.logger import flog
            flog(
                f"{trace_prefix(self._trace_id)}{type(self).__name__}: "
                f"wait({msecs}) gave up after {limit_ms} ms, job still "
                f"running (cancelled={self._stopped})",
                "WARNING",
            )
        return not self._running

    def _submit_task(self, description: str, function, on_finished, **kwargs) -> None:
        self._running = True
        self._task = QgsTask.fromFunction(
            description,
            function,
            on_finished=on_finished,
            **kwargs,
        )
        QgsApplication.taskManager().addTask(self._task)

    def _clear_task(self) -> None:
        self._running = False
        self._task = None

    def _honour_pending_restart(self, label: str) -> None:
        """Run the start() that was refused while the job was in flight."""
        if not self._restart_pending:
            return
        from .core.logger import flog
        self._restart_pending = False
        flog(f"{trace_prefix(self._trace_id)}{label}: running the deferred "
             f"restart requested during the previous job")
        self.start()

    def _handle_task_finished(self, exception, result, result_signal, label):
        from .core.logger import flog
        prefix = trace_prefix(self._trace_id)
        cancelled = self._stopped
        self._clear_task()
        try:
            if cancelled:
                return
            if exception is not None:
                flog(f"{prefix}{label}: error: {exception}", "ERROR")
                self.error_occurred.emit(str(exception))
                return
            if result is not None:
                result_signal.emit(result)
        finally:
            # After the outcome of the finished job has been delivered, so
            # a deferred restart never overwrites it.
            self._honour_pending_restart(label)
