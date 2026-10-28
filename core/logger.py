# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Logging infrastructure for RecoverLand plugin."""
import os
import logging
import logging.handlers
import threading
import time
import uuid
from contextlib import contextmanager
from typing import Optional

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


class _SharedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """RotatingFileHandler that never drops a record it cannot rotate.

    Several QGIS instances can share one profile, hence one debug log.
    The rollover then renames a file a sibling process still holds open,
    which fails on Windows, and the stock handler answers by discarding
    the record. The debug log is the only forensic material left after an
    incident, so keep appending to the current file instead: an oversized
    log is recoverable, a missing line is not. A failed rotation is
    retried later rather than on every record, so a permanently held file
    does not cost a failed rename per line.
    """

    _RETRY_DELAY_S = 60.0

    def __init__(self, *args, **kwargs):
        self._rollover_retry_at = 0.0
        super().__init__(*args, **kwargs)

    def _open(self):
        """Open with Win32 FILE_APPEND_DATA so multi-process appends never collide."""
        if os.name == "nt" and "a" in (self.mode or "a"):
            try:
                import ctypes
                import msvcrt
                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel32.CreateFileW.argtypes = [
                    ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                    ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32,
                    ctypes.c_void_p,
                ]
                kernel32.CreateFileW.restype = ctypes.c_void_p
                handle = kernel32.CreateFileW(
                    self.baseFilename,
                    0x0004 | 0x00100000,  # FILE_APPEND_DATA | SYNCHRONIZE
                    0x0001 | 0x0002,      # FILE_SHARE_READ | FILE_SHARE_WRITE
                    None,
                    4,                    # OPEN_ALWAYS
                    0x80,                 # FILE_ATTRIBUTE_NORMAL
                    None,
                )
                invalid = ctypes.c_void_p(-1).value
                if handle is not None and handle != invalid:
                    fd = msvcrt.open_osfhandle(handle, os.O_WRONLY | os.O_TEXT)
                    return os.fdopen(
                        fd, "w", encoding=self.encoding or "utf-8",
                        errors=self.errors)
            except Exception:  # noqa: BLE001 # nosec B110 - fall back to stdlib open
                pass
        return super()._open()

    def doRollover(self):
        """Rotate only once the current file is known to be movable.

        The stock rollover shifts the backups (.4 -> .5, deleting the
        oldest) BEFORE renaming the live file. When a sibling QGIS holds
        that file, the rename fails after the shift: every failed attempt
        destroyed one backup, 4 of the 5 forensic files within minutes.
        Here the live file is moved aside FIRST; the backups are only
        shifted once that move succeeded.

        A shift can still fail: on Windows a backup opened by a reader that
        does not share deletion (antivirus, indexer, a `tail`, another
        QGIS) cannot be renamed. The generation just moved aside then
        stays in `.rolling`, newest after the live file, where log readers
        look for it (parse_log._chain_paths). It is never deleted: the
        next rollover moves it into the chain first, and postpones itself
        while it cannot. Each failure writes a named `#` line in the live
        file instead of being swallowed.
        """
        if time.monotonic() < self._rollover_retry_at:
            return
        if self.stream:
            self.stream.close()
            self.stream = None
        aside = self.baseFilename + ".rolling"
        if os.path.exists(aside):
            try:
                self._shift_into_chain(aside)
            except OSError as exc:
                self._postpone(
                    f"# rollover postponed (backup shift: {exc}); "
                    f"{os.path.basename(aside)} kept, still appending here\n")
                return
        try:
            os.replace(self.baseFilename, aside)
        except OSError as exc:
            self._postpone(
                f"# rollover postponed ({exc}); still appending here\n")
            return
        self._rollover_retry_at = 0.0
        self.stream = self._open()
        try:
            self._shift_into_chain(aside)
        except OSError as exc:
            self.stream.write(
                f"# backup shift postponed ({exc}); "
                f"{os.path.basename(aside)} kept\n")

    def _postpone(self, line: str) -> None:
        """Keep appending to the live file, retry the rollover later."""
        self._rollover_retry_at = time.monotonic() + self._RETRY_DELAY_S
        self.stream = self._open()
        self.stream.write(line)

    def _shift_into_chain(self, aside: str) -> None:
        """Move *aside* to `.1`, pushing the backups down to the first free slot.

        Only a full chain loses its oldest generation (the retention of
        `backupCount` copies). A hole left by an earlier failed shift is
        filled instead: a generation still within the retention is never
        deleted to make room. Renames go from the oldest end, so a failure
        halfway leaves every generation on disk, in order, around one hole.
        """
        names = [self.rotation_filename(f"{self.baseFilename}.{i}")
                 for i in range(1, max(1, self.backupCount) + 1)]
        free = next((i for i, name in enumerate(names)
                     if not os.path.exists(name)), None)
        if free is None:
            os.remove(names[-1])
            free = len(names) - 1
        for i in range(free, 0, -1):
            os.rename(names[i - 1], names[i])
        os.rename(aside, names[0])


_file_logger = logging.getLogger("RecoverLand.FileDebug")
_file_logger.setLevel(logging.DEBUG)
_file_logger.propagate = False
if not _file_logger.handlers:
    _fh = _SharedRotatingFileHandler(
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

# Optional project-scoped handler (populated when a journal is opened).
_project_log_handler: Optional[logging.handlers.RotatingFileHandler] = None


def set_project_log_path(journal_path: str) -> None:
    """Add or replace a project-scoped log handler beside the journal.

    The project log is written to ``<project_dir>/.recoverland/recoverland_debug.log``
    so users can retrieve diagnostics directly from the project directory.
    """
    global _project_log_handler
    if _project_log_handler is not None:
        try:
            _file_logger.removeHandler(_project_log_handler)
            _project_log_handler.close()
        except Exception as exc:  # noqa: BLE001
            _file_logger.debug("log: failed to close previous project handler: %s", exc)
        _project_log_handler = None

    if not journal_path:
        return
    project_dir = os.path.dirname(os.path.dirname(journal_path))
    if not project_dir or not os.path.isdir(project_dir):
        return
    log_dir = os.path.join(project_dir, ".recoverland")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except OSError as exc:
        _file_logger.warning("log: cannot create project log dir %s: %s", log_dir, exc)
        return
    log_path = os.path.join(log_dir, "recoverland_debug.log")
    try:
        handler = _SharedRotatingFileHandler(
            log_path,
            mode='a',
            maxBytes=_LOG_MAX_BYTES,
            backupCount=_LOG_BACKUP_COUNT,
            encoding='utf-8',
        )
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(threadName)-15s] %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S'
        ))
        _file_logger.addHandler(handler)
        _project_log_handler = handler
        _file_logger.info("log: project_log_ready path=%s", log_path)
    except Exception as exc:  # noqa: BLE001
        _file_logger.warning("log: cannot create project log %s: %s", log_path, exc)


def remove_project_log_handler() -> None:
    """Remove the project-scoped log handler (project switch / shutdown)."""
    global _project_log_handler
    if _project_log_handler is None:
        return
    try:
        _file_logger.removeHandler(_project_log_handler)
        _project_log_handler.close()
        _file_logger.info("log: project_log_removed")
    except Exception as exc:  # noqa: BLE001
        _file_logger.debug("log: failed to remove project handler: %s", exc)
    _project_log_handler = None


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

# Line breaks are the only characters that destroy a record: the handler
# writes them literally, the file then holds two physical lines, and the
# second one has no timestamp so every reader drops it while the first is
# left with an unclosed quote. Substituted, never emitted raw. Backslashes
# are deliberately left alone: doubling them would rewrite every Windows
# path already present in the log for no gain.
_KV_LINE_BREAKS = (("\r\n", "\\n"), ("\n", "\\n"), ("\r", "\\r"))

# A value carrying BOTH quote characters cannot be wrapped as it is: the
# reference parser (`scripts/validation/parse_log._KV_RE`) closes its
# capture on the first raw delimiter found inside the value, and putting a
# backslash in front of that delimiter does not help since the parser
# never unescapes. It then cuts the value short AND reads the leftover
# tail as key=value pairs, so a layer name quoting an `eid=999` rewrites
# the real `eid` of the line. Emitting the double quote as its unicode
# escape leaves no quote character inside the wrapper, so only the writer
# can close it: the value comes back whole and the other pairs of the
# line keep their real values.
_KV_DQUOTE_ESCAPE = "\\u0022"


def _escape_line_breaks(s: str) -> str:
    for raw, escaped in _KV_LINE_BREAKS:
        if raw in s:
            s = s.replace(raw, escaped)
    return s


def _format_kv_value(value) -> str:
    """Render a single value for the key=value log format.

    Bare token for simple values. Values containing space, '=', quote or
    tab are wrapped in matching quotes so the parser
    (`scripts.validation.parse_log._KV_RE`) can recover the original
    string by stripping the outermost quotes. Line breaks are replaced by
    their escape sequence so the record stays on one physical line.

    Quote selection:
        - no special character        -> bare token
        - contains double quote only  -> wrapped in single quotes
        - contains single quote only  -> wrapped in double quotes
        - contains both quote types   -> wrapped in double quotes, inner
          double quotes emitted as \\u0022 so nothing inside the value can
          close the wrapper
        - other special character     -> wrapped in double quotes
    """
    s = str(value)
    if s == "":
        return '""'
    if not any(c in s for c in _KV_QUOTE_TRIGGERS + ("\r",)):
        # Bare token: a Windows path without spaces stays exactly as is.
        return s
    # Anything that needs quoting is written as a JSON string: quotes,
    # backslashes, tabs and line breaks all escaped by one standard, so
    # the reader (parse_log) decodes the value EXACTLY with json.loads.
    # The previous scheme left backslashes raw: "\n" in a record could be
    # an escaped line break or the start of a path, and the round trip
    # lost multi-line values and values with both quote characters.
    import json as _json
    return _json.dumps(s, ensure_ascii=False)


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
    # A CRITICAL message used to fall through to Info in the log panel.
    "CRITICAL": QgisCompat.MSG_CRITICAL,
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


# ----------------------------------------------------------------------
# JSON Lines forensic sink: one self-describing JSON object per line,
# readable by a machine after an incident whatever the values contain.
# ----------------------------------------------------------------------

_EVENTS_FILE = os.path.join(os.path.dirname(_LOG_FILE), "recoverland_events.jsonl")
_events_logger = logging.getLogger("RecoverLand.Events")
_events_logger.setLevel(logging.DEBUG)
_events_logger.propagate = False
if not _events_logger.handlers:
    try:
        _eh = _SharedRotatingFileHandler(
            _EVENTS_FILE, mode='a', maxBytes=_LOG_MAX_BYTES,
            backupCount=_LOG_BACKUP_COUNT, encoding='utf-8')
        _eh.setLevel(logging.DEBUG)
        _eh.setFormatter(logging.Formatter('%(message)s'))
        _events_logger.addHandler(_eh)
    except Exception as exc:  # noqa: BLE001 - the text log still works
        _file_logger.warning("log: cannot open JSON Lines sink %s: %s",
                             _EVENTS_FILE, exc)


def jlog(event: str, level: str = "INFO", component: str = "",
         trace_id: str = "", **fields) -> None:
    """Record a technical event as ONE JSON line (and mirror it in the text log).

    Keys always present: ts, level, component, trace_id, event, pid,
    thread. Everything else is caller data (project, layer, commit_id,
    event_count, queue_depth, state_before, state_after, duration_ms,
    error_code, error_detail, ...). json.dumps escapes every character that
    could split or confuse a line.
    """
    import json as _json
    from datetime import datetime as _dt, timezone as _tz
    record = {
        "ts": _dt.now(_tz.utc).isoformat(),
        "level": level,
        "component": component,
        "trace_id": trace_id,
        "event": event,
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
    }
    record.update(fields)
    try:
        line = _json.dumps(record, ensure_ascii=False, default=str)
    except (TypeError, ValueError) as exc:
        line = _json.dumps({"ts": record["ts"], "event": event,
                            "error": f"unserialisable fields: {exc}"})
    try:
        _events_logger.info(line)
    except Exception:  # noqa: BLE001 # nosec B110 - logging must never raise
        pass
    flog_kv(level, event, module=component or "recoverland",
            trace_id=trace_id, **fields)


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
