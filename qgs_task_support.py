from qgis.PyQt.QtCore import QThread

try:
    from qgis.core import QgsApplication, QgsTask
except ImportError:
    QgsApplication = None
    QgsTask = None


def trace_prefix(trace_id: str) -> str:
    return f"[{trace_id}] " if trace_id else ""


def supports_qgs_task() -> bool:
    return (
        QgsTask is not None
        and QgsApplication is not None
        and hasattr(QgsApplication, "taskManager")
    )


class TaskEnabledThread(QThread):
    def __init__(self, trace_id: str = ""):
        super().__init__()
        self._stopped = False
        self._running = False
        self._task = None
        self._trace_id = trace_id

    def stop(self):
        self._stopped = True
        if self._task is not None and hasattr(self._task, "cancel"):
            self._task.cancel()

    def start(self):
        self._stopped = False
        if supports_qgs_task():
            self._start_task()
            return
        self._running = True
        super().start()

    def isRunning(self):
        if self._task is not None:
            return self._running
        return QThread.isRunning(self)

    def wait(self, msecs=0):
        if self._task is not None:
            return not self._running
        return QThread.wait(self, msecs)

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

    def _handle_task_finished(self, exception, result, result_signal, label):
        from .core.logger import flog
        prefix = trace_prefix(self._trace_id)
        self._clear_task()
        if self._stopped:
            return
        if exception is not None:
            flog(f"{prefix}{label}: error: {exception}", "ERROR")
            self.error_occurred.emit(str(exception))
            return
        if result is not None:
            result_signal.emit(result)
