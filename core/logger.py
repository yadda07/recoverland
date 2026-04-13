"""Logging infrastructure for RecoverLand plugin."""
import os
import logging
import threading
import time
import uuid
from contextlib import contextmanager

from qgis.core import QgsMessageLog

from .constants import PLUGIN_NAME
from ..compat import QgisCompat, get_environment_info

# --- File Logger Setup (writes to plugin directory) ---
_PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_LOG_FILE = os.path.join(_PLUGIN_DIR, "recoverland_debug.log")

_file_logger = logging.getLogger("RecoverLand.FileDebug")
_file_logger.setLevel(logging.DEBUG)
_file_logger.propagate = False
if not _file_logger.handlers:
    _fh = logging.FileHandler(_LOG_FILE, mode='w', encoding='utf-8')
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter(
        '%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] %(message)s',
        datefmt='%H:%M:%S'
    ))
    _file_logger.addHandler(_fh)

_file_logger.info("=" * 80)
_file_logger.info(f"RecoverLand module loaded - PID={os.getpid()} Thread={threading.current_thread().name}")
_file_logger.info(f"Plugin dir: {_PLUGIN_DIR}")
_file_logger.info(f"Log file: {_LOG_FILE}")
try:
    from .constants import HAS_PSYCOPG2, psycopg2
    _file_logger.info(f"psycopg2: {'v' + psycopg2.__version__ if HAS_PSYCOPG2 else 'NOT INSTALLED'}")
    _file_logger.info(get_environment_info())
except Exception:
    pass
_file_logger.info("=" * 80)


def flog(message: str, level: str = "INFO") -> None:
    """Write to debug file log (thread-safe, no Qt dependency)."""
    if level == "DEBUG":
        _file_logger.debug(message)
    elif level == "WARNING":
        _file_logger.warning(message)
    elif level == "ERROR":
        _file_logger.error(message)
    else:
        _file_logger.info(message)


_QLOG_LEVELS = {
    "INFO": QgisCompat.MSG_INFO,
    "WARNING": QgisCompat.MSG_WARNING,
    "ERROR": QgisCompat.MSG_CRITICAL,
}


def qlog(message: str, level: str = "INFO") -> None:
    """Write to debug file log AND QGIS Message Log panel (RecoverLand tab).

    Use this for user-facing operational messages that should appear
    in the RecoverLand tab of the QGIS log panel.
    """
    flog(message, level)
    try:
        qgis_level = _QLOG_LEVELS.get(level, QgisCompat.MSG_INFO)
        QgsMessageLog.logMessage(message, PLUGIN_NAME, qgis_level)
    except Exception:
        pass


def generate_trace_id() -> str:
    """Generate a short trace ID for correlating related log entries."""
    return uuid.uuid4().hex[:8]


@contextmanager
def timed_op(operation: str, trace_id: str = ""):
    """Context manager that logs operation duration.

    Usage:
        with timed_op("search", tid) as ctx:
            result = do_search()
    Logs: "[abc12345] search completed in 123ms"
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    flog(f"{prefix}{operation}: start")
    t0 = time.monotonic()
    try:
        yield
    finally:
        elapsed_ms = (time.monotonic() - t0) * 1000
        flog(f"{prefix}{operation}: completed in {elapsed_ms:.0f}ms")


class LoggerMixin:
    """Mixin for centralized logging (QGIS log panel + file)."""
    
    def log_info(self, message: str) -> None:
        flog(message, "INFO")
        QgsMessageLog.logMessage(message, PLUGIN_NAME, QgisCompat.MSG_INFO)
    
    def log_warning(self, message: str) -> None:
        flog(message, "WARNING")
        QgsMessageLog.logMessage(message, PLUGIN_NAME, QgisCompat.MSG_WARNING)
    
    def log_error(self, message: str) -> None:
        flog(message, "ERROR")
        QgsMessageLog.logMessage(message, PLUGIN_NAME, QgisCompat.MSG_CRITICAL)
