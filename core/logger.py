"""Logging infrastructure for RecoverLand plugin."""
import os
import logging
import logging.handlers
import threading
import time
import uuid
from contextlib import contextmanager

from qgis.core import QgsMessageLog

from .constants import PLUGIN_NAME
from ..compat import QgisCompat, get_environment_info

_LOG_MAX_BYTES = 5 * 1024 * 1024
_LOG_BACKUP_COUNT = 5

# --- File Logger Setup (writes to QGIS profile directory, not plugin dir) ---
_PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _resolve_log_path() -> str:
    """Determine log file path: profile dir preferred, plugin dir fallback."""
    try:
        from qgis.core import QgsApplication
        profile_dir = QgsApplication.qgisSettingsDirPath()
        if profile_dir and os.path.isdir(profile_dir):
            log_dir = os.path.join(profile_dir, "recoverland")
            os.makedirs(log_dir, exist_ok=True)
            return os.path.join(log_dir, "recoverland_debug.log")
    except Exception as exc:  # pragma: no cover - defensive fallback
        import sys
        print(
            f"[RecoverLand] log path fallback to plugin dir: {exc}",
            file=sys.stderr,
        )
    return os.path.join(_PLUGIN_DIR, "recoverland_debug.log")


_LOG_FILE = _resolve_log_path()

_file_logger = logging.getLogger("RecoverLand.FileDebug")
_file_logger.setLevel(logging.DEBUG)
_file_logger.propagate = False
if not _file_logger.handlers:
    _fh = logging.handlers.RotatingFileHandler(
        _LOG_FILE,
        mode='a',
        maxBytes=_LOG_MAX_BYTES,
        backupCount=_LOG_BACKUP_COUNT,
        encoding='utf-8',
    )
    _fh.setLevel(logging.DEBUG)
    _fh.setFormatter(logging.Formatter(
        '%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S'
    ))
    _file_logger.addHandler(_fh)

_file_logger.info("=" * 80)
_file_logger.info(f"RecoverLand module loaded - PID={os.getpid()} Thread={threading.current_thread().name}")
_file_logger.info(f"Plugin dir: {_PLUGIN_DIR}")
_file_logger.info(f"Log file: {_LOG_FILE}")
try:
    _file_logger.info(get_environment_info())
except Exception as exc:
    _file_logger.warning("get_environment_info failed: %s", exc)
_file_logger.info("=" * 80)


def flog(message: str, level: str = "INFO") -> None:
    """Write to debug file log (thread-safe, no Qt dependency)."""
    if level == "DEBUG":
        _file_logger.debug(message)
    elif level == "WARNING":
        _file_logger.warning(message)
    elif level == "ERROR":
        _file_logger.error(message)
    elif level == "CRITICAL":
        _file_logger.critical(message)
    else:
        _file_logger.info(message)


_KV_QUOTE_TRIGGERS = (" ", "=", '"', "'", "\n", "\t")


def _format_kv_value(value) -> str:
    """Render a single value for the key=value log format.

    Bare token for simple values. Values containing space, '=', quote,
    newline or tab are wrapped in matching quotes so the parser
    (`scripts.validation.parse_log._KV_RE`) can recover the original
    string by stripping the outermost quotes.

    Quote selection:
        - no special character        -> bare token
        - contains double quote only  -> wrapped in single quotes
        - contains single quote only  -> wrapped in double quotes
        - contains both quote types   -> wrapped in double quotes with
          backslash escape (unambiguous but requires a tolerant parser)
        - other special character     -> wrapped in double quotes
    """
    s = str(value)
    if s == "":
        return '""'
    if not any(c in s for c in _KV_QUOTE_TRIGGERS):
        return s
    has_double = '"' in s
    has_single = "'" in s
    if has_double and not has_single:
        return f"'{s}'"
    if has_double and has_single:
        s = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{s}"'
    return f'"{s}"'


def flog_kv(level: str, event: str, *, module: str, **fields) -> None:
    """Emit a structured `level=... module=... event=... k=v ...` log line.

    The output format is grep-able and parseable by
    `scripts.validation.parse_log.parse_line`. Field order follows the
    insertion order of `fields` (Python 3.7+ dict ordering).

    Args:
        level: log severity (INFO, WARNING, ERROR, DEBUG, CRITICAL).
        event: short uppercase event identifier (e.g. BUF_INS).
        module: source module name (e.g. restore_executor).
        **fields: additional key=value pairs.

    Example:
        flog_kv("INFO", "BUF_INS", module="restore_executor",
                eid=12, fp="fid:42", buf_fid=43)
        # -> level=INFO module=restore_executor event=BUF_INS eid=12 fp=fid:42 buf_fid=43
    """
    parts = [
        f"level={level}",
        f"module={module}",
        f"event={event}",
    ]
    for k, v in fields.items():
        parts.append(f"{k}={_format_kv_value(v)}")
    flog(" ".join(parts), level)


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
    except Exception as exc:  # QGIS message log unavailable (shutdown, no UI)
        _file_logger.debug("QgsMessageLog unavailable: %s", exc)


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
