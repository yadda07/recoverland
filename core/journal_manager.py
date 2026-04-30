"""Journal location and lifecycle management for RecoverLand (RLU-003).

Determines where the SQLite audit journal lives, opens/creates it,
and manages its lifecycle (open, close, relocate).

Rules:
- Project saved: .recoverland/recoverland_audit.sqlite next to .qgz/.qgs
- Project unsaved: %APPDATA%/QGIS/QGIS3/profiles/<profile>/recoverland/audit/<hash>.sqlite
- Never returns an uninitialized database.
"""
import os
import hashlib
import socket
import sqlite3
import time
import uuid
from typing import Optional

from .sqlite_schema import initialize_schema, apply_pragmas, get_schema_version
from .logger import flog

_JOURNAL_FILENAME = "recoverland_audit.sqlite"
_JOURNAL_SUBDIR = ".recoverland"
_UNSAVED_SUBDIR = "recoverland"
_AUDIT_SUBDIR = "audit"

_LOCK_SUFFIX = ".rlwriter"


class JournalLockError(RuntimeError):
    """Raised when another live QGIS instance already holds the writer lock."""


def _is_pid_alive(pid: int) -> bool:
    """Return True if the given PID is a running process on this machine.

    Platform split is mandatory: on Windows, os.kill(pid, 0) is NOT a
    benign signal check; it calls TerminateProcess() and would kill the
    target (including ourselves when pid == getpid()). We use OpenProcess
    via ctypes there. On POSIX, os.kill(pid, 0) is the canonical probe.
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        return _is_pid_alive_windows(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we cannot signal it; still alive.
        return True
    except OSError:
        return False
    return True


def _is_pid_alive_windows(pid: int) -> bool:
    """Windows-only PID liveness check using OpenProcess + GetExitCodeProcess."""
    import ctypes
    from ctypes import wintypes

    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    STILL_ACTIVE = 259

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
    if not handle:
        # ERROR_INVALID_PARAMETER (87) = no such process;
        # ERROR_ACCESS_DENIED (5) = process exists but locked down.
        return ctypes.get_last_error() == 5
    try:
        exit_code = wintypes.DWORD()
        ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
        if not ok:
            return False
        return exit_code.value == STILL_ACTIVE
    finally:
        kernel32.CloseHandle(handle)


def _read_lock_file(lock_path: str) -> Optional[tuple]:
    """Parse a lock-file. Returns (pid:int, host:str, ts:int) or None."""
    try:
        with open(lock_path, "r", encoding="utf-8") as fh:
            content = fh.read().strip()
    except OSError:
        return None
    if not content:
        return None
    parts = content.split("|")
    if len(parts) < 3:
        return None
    try:
        pid = int(parts[0])
        host = parts[1]
        ts = int(parts[2])
    except (ValueError, IndexError):
        return None
    return (pid, host, ts)


class JournalManager:
    """Manages the SQLite audit journal file and connections."""

    def __init__(self):
        self._conn: Optional[sqlite3.Connection] = None
        self._path: Optional[str] = None
        self._unsaved_session_token: Optional[str] = None
        self._lock_acquired: bool = False

    @property
    def path(self) -> Optional[str]:
        return self._path

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    @property
    def is_lock_degraded(self) -> bool:
        """True if the journal is open but the writer lock could not be acquired."""
        return self.is_open and not self._lock_acquired

    def open_for_project(self, project_path: str, profile_path: str) -> str:
        """Open or create the journal for the given project context.

        Returns the journal file path.
        Raises OSError if the directory cannot be created.
        Raises sqlite3.Error if the database cannot be opened.
        Raises JournalLockError if another live QGIS instance holds the
        writer lock for this journal (multi-writer protection).
        """
        self.close()
        unsaved_token = ""
        if not (project_path and os.path.isfile(project_path)):
            self._unsaved_session_token = uuid.uuid4().hex[:16]
            unsaved_token = self._unsaved_session_token
        journal_path = _resolve_journal_path(project_path, profile_path, unsaved_token)
        self._ensure_directory(journal_path)
        self._acquire_writer_lock(journal_path)
        try:
            self._open_connection(journal_path)
        except Exception:
            self._release_writer_lock(journal_path)
            raise
        self._path = journal_path
        flog(f"JournalManager: opened {journal_path}")
        return journal_path

    def get_connection(self) -> sqlite3.Connection:
        """Return the active connection. Raises if not open."""
        if self._conn is None:
            raise RuntimeError("Journal is not open")
        return self._conn

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.close()
            flog(f"JournalManager: closed {self._path}")
        except sqlite3.Error as e:
            flog(f"JournalManager: close error: {e}", "WARNING")
        finally:
            if self._path is not None:
                self._release_writer_lock(self._path)
            self._conn = None
            self._path = None
            self._unsaved_session_token = None
            self._lock_acquired = False

    def create_read_connection(self) -> sqlite3.Connection:
        """Create a separate read-only connection for search threads."""
        if self._path is None:
            raise RuntimeError("Journal is not open")
        conn = sqlite3.connect(
            f"file:{self._path}?mode=ro",
            uri=True,
            check_same_thread=False,
        )
        apply_pragmas(conn)
        return conn

    def create_write_connection(self) -> sqlite3.Connection:
        """Create a separate write connection for the write queue thread."""
        if self._path is None:
            raise RuntimeError("Journal is not open")
        conn = sqlite3.connect(self._path, check_same_thread=False)
        apply_pragmas(conn)
        return conn

    def _open_connection(self, path: str) -> None:
        conn = sqlite3.connect(path, check_same_thread=False)
        try:
            initialize_schema(conn)
            version = get_schema_version(conn)
            flog(f"JournalManager: schema version={version}")
            self._refresh_planner_stats(conn)
        except Exception:
            conn.close()
            raise
        self._conn = conn

    @staticmethod
    def _refresh_planner_stats(conn: sqlite3.Connection) -> None:
        """Run ANALYZE at startup so the query planner has fresh statistics.

        analysis_limit=1000 (set in PRAGMAs) caps the sample size,
        keeping this under 50ms even on journals with 10^6+ events.
        """
        try:
            conn.execute("ANALYZE")
            flog("JournalManager: startup ANALYZE completed")
        except sqlite3.Error as exc:
            flog(f"JournalManager: startup ANALYZE failed: {exc}", "WARNING")

    @staticmethod
    def _ensure_directory(path: str) -> None:
        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)

    def _acquire_writer_lock(self, journal_path: str) -> None:
        """Write a PID lock-file next to the journal.

        If a lock-file already exists and its PID is alive on this host,
        refuse to open: another live QGIS instance is already writing.
        Stale locks (dead PID or different host) are reclaimed.
        """
        lock_path = journal_path + _LOCK_SUFFIX
        me_pid = os.getpid()
        me_host = socket.gethostname()

        if os.path.isfile(lock_path):
            info = _read_lock_file(lock_path)
            if info is not None:
                other_pid, other_host, _ = info
                if other_host == me_host and other_pid != me_pid and _is_pid_alive(other_pid):
                    raise JournalLockError(
                        f"Another live QGIS instance (pid={other_pid}) is "
                        f"already writing to {journal_path}. Close it before "
                        f"opening the project in this instance."
                    )
                # Stale: dead PID or different host. Log and reclaim.
                flog(
                    f"JournalManager: stale writer lock reclaimed "
                    f"(pid={other_pid}, host={other_host}) on {journal_path}",
                    "WARNING",
                )

        payload = f"{me_pid}|{me_host}|{int(time.time())}\n"
        try:
            with open(lock_path, "w", encoding="utf-8") as fh:
                fh.write(payload)
            self._lock_acquired = True
        except OSError as exc:
            self._lock_acquired = False
            flog(f"JournalManager: cannot write lock-file {lock_path}: {exc}. "
                 f"Multi-writer protection disabled for this session.", "ERROR")

    @staticmethod
    def _release_writer_lock(journal_path: str) -> None:
        """Remove the lock-file when closing the journal.

        Only removes the lock if it still matches this process; otherwise
        leaves it alone so a concurrent owner is not evicted.
        """
        lock_path = journal_path + _LOCK_SUFFIX
        if not os.path.isfile(lock_path):
            return
        info = _read_lock_file(lock_path)
        if info is not None:
            pid, host, _ = info
            if pid != os.getpid() or host != socket.gethostname():
                flog(
                    f"JournalManager: lock-file not owned by us "
                    f"(pid={pid}); skipping remove",
                    "DEBUG",
                )
                return
        try:
            os.remove(lock_path)
        except OSError as exc:
            flog(f"JournalManager: cannot remove lock-file {lock_path}: {exc}", "DEBUG")


def _resolve_journal_path(project_path: str,
                          profile_path: str,
                          unsaved_token: str = "") -> str:
    """Determine the journal file path based on project save state."""
    if project_path and os.path.isfile(project_path):
        return _journal_for_saved_project(project_path)
    return _journal_for_unsaved_project(profile_path, project_path, unsaved_token)


def _journal_for_saved_project(project_path: str) -> str:
    project_dir = os.path.dirname(os.path.abspath(project_path))
    journal_dir = os.path.join(project_dir, _JOURNAL_SUBDIR)
    return os.path.join(journal_dir, _JOURNAL_FILENAME)


def _journal_for_unsaved_project(profile_path: str,
                                 hint: str,
                                 session_token: str = "") -> str:
    base_dir = os.path.join(profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR)
    if not hint:
        hint = session_token or uuid.uuid4().hex
    fingerprint = hashlib.sha256(hint.encode("utf-8")).hexdigest()[:16]
    filename = f"audit_{fingerprint}.sqlite"
    return os.path.join(base_dir, filename)


_ORPHAN_MAX_AGE_DAYS = 30


def cleanup_orphan_journals(profile_path: str,
                            max_age_days: int = _ORPHAN_MAX_AGE_DAYS,
                            current_path: str = "") -> int:
    """Remove orphan journal files from unsaved-project audit directory.

    Only removes files older than max_age_days that are not the current journal.
    Returns the number of files removed.
    """
    import time as _time
    audit_dir = os.path.join(profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR)
    if not os.path.isdir(audit_dir):
        return 0
    cutoff = _time.time() - (max_age_days * 86400)
    removed = 0
    current_norm = os.path.normcase(os.path.abspath(current_path)) if current_path else ""
    for filename in os.listdir(audit_dir):
        if not filename.endswith(".sqlite"):
            continue
        full = os.path.join(audit_dir, filename)
        if current_norm and os.path.normcase(os.path.abspath(full)) == current_norm:
            continue
        try:
            mtime = os.path.getmtime(full)
            if mtime < cutoff:
                os.remove(full)
                for suffix in ("-wal", "-shm"):
                    sidecar = full + suffix
                    if os.path.exists(sidecar):
                        os.remove(sidecar)
                removed += 1
                flog(f"JournalManager: removed orphan {filename}")
        except OSError as e:
            flog(f"JournalManager: cannot remove orphan {filename}: {e}", "WARNING")
    if removed:
        flog(f"JournalManager: cleaned {removed} orphan journal(s) from {audit_dir}")
    return removed


def get_journal_size_bytes(path: str) -> int:
    """Return the file size of the journal in bytes, or 0 if not found."""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def format_journal_size(size_bytes: int) -> str:
    """Human-readable journal size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
