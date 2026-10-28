# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Journal location and lifecycle management for RecoverLand (RLU-003).

Determines where the SQLite audit journal lives, opens/creates it,
and manages its lifecycle (open, close, relocate).

Rules:
- Project saved: .recoverland/recoverland_audit.sqlite next to .qgz/.qgs
- Project kept in a project storage (GeoPackage, PostgreSQL): its
  absoluteFilePath() is empty; the journal is keyed on the storage URI,
  <profile>/recoverland/audit/storage_<hash>.sqlite, the same at every open.
- Project unsaved: %APPDATA%/QGIS/QGIS3/profiles/<profile>/recoverland/audit/<hash>.sqlite
- When the project is saved, the history follows it: planned inside the
  save (JournalManager.plan_history_carry, recorded in the profile),
  carried out when the session journal closes, or at the next open of the
  project's journal if QGIS died first (carry_journal_history).
- One writer per journal: see the writer lock section.
- A journal path too long for Win32 (MAX_PATH) is used in its extended-
  length form, every other path exactly as before (core/long_paths.py).
- Never returns an uninitialized database.
"""
import os
import hashlib
import json
import re
import socket
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .sqlite_schema import (
    initialize_schema, apply_pragmas, get_schema_version,
    NetworkJournalBusyError,
)
from .long_paths import when_needed, plain, path_key
from .logger import flog, qlog

_JOURNAL_FILENAME = "recoverland_audit.sqlite"
_JOURNAL_SUBDIR = ".recoverland"
_UNSAVED_SUBDIR = "recoverland"
_AUDIT_SUBDIR = "audit"
_STORAGE_PREFIX = "storage_"

_LOCK_SUFFIX = ".rlwriter"

# Writer lock liveness. The owner rewrites its heartbeat every
# _LOCK_HEARTBEAT_SEC from a daemon thread (not a Qt timer: a GUI thread
# busy for minutes in a long processing job is still a live writer). A lock
# whose heartbeat is older than _LOCK_TTL_SEC belongs to a dead instance.
# The TTL is compared with the local clock: it assumes the clocks of two
# machines sharing a project differ by less than TTL - heartbeat (domain
# members are kept within 5 minutes by Kerberos).
_LOCK_HEARTBEAT_SEC = 30.0
_LOCK_TTL_SEC = 600
# One byte locked far past the content of the lock file, for the whole
# session: the operating system (and the SMB server for a share) enforces
# it, and releases it when the process dies. Content stays readable.
_OS_LOCK_OFFSET = 0x7FFF0000


class JournalLockError(RuntimeError):
    """Raised when another live QGIS instance already holds the writer lock."""

    def __init__(self, message: str, journal_path: str = "",
                 holder_pid: Optional[int] = None, holder_host: str = "",
                 heartbeat_age_s: Optional[float] = None, reason: str = ""):
        super().__init__(message)
        self.journal_path = journal_path
        self.holder_pid = holder_pid
        self.holder_host = holder_host
        self.heartbeat_age_s = heartbeat_age_s
        self.reason = reason

    def user_message(self) -> str:
        """French, user-facing: who holds the journal and what it means here."""
        who = []
        if self.holder_host:
            who.append(f"poste {self.holder_host}")
        if self.holder_pid is not None:
            who.append(f"processus {self.holder_pid}")
        holder = f" ({', '.join(who)})" if who else ""
        hint = ""
        if self.reason == _WAL_HELD_REASON:
            hint = (" Un autre poste l'a ouvert en mode WAL, probablement avec "
                    "une version plus ancienne de RecoverLand : mettez-la a "
                    "jour sur ce poste.")
        return (
            f"Le journal RecoverLand de ce projet est tenu par une autre "
            f"instance de QGIS{holder}. L'enregistrement est INACTIF dans "
            f"cette instance pour ce projet.{hint} Une fois l'autre instance "
            f"fermee, enregistrez ou rouvrez le projet pour le reactiver."
        )


# JournalLockError.reason when a network journal cannot leave WAL mode
# because another client keeps it open (see sqlite_schema).
_WAL_HELD_REASON = "network journal kept in WAL mode by another client"


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


def _process_image_name(pid: int) -> str:
    """Executable name of a live process, '' when it cannot be read."""
    if pid <= 0:
        return ""
    if os.name != "nt":
        try:
            with open(f"/proc/{pid}/comm", "r", encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            return ""
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.QueryFullProcessImageNameW.argtypes = (
            wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR,
            ctypes.POINTER(wintypes.DWORD))
        kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return ""
        try:
            size = wintypes.DWORD(1024)
            buf = ctypes.create_unicode_buffer(size.value)
            if not kernel32.QueryFullProcessImageNameW(handle, 0, buf, ctypes.byref(size)):
                return ""
            return os.path.basename(buf.value)
        finally:
            kernel32.CloseHandle(handle)
    except (AttributeError, OSError, ValueError):
        return ""


def _looks_like_qgis(image: str) -> bool:
    """A process that may be a QGIS instance (QGIS itself or its python)."""
    name = (image or "").lower()
    return "qgis" in name or "python" in name


def _read_lock_file(lock_path: str) -> Optional[tuple]:
    """Parse a lock-file. Returns (pid:int, host:str, ts:int) or None."""
    try:
        with open(lock_path, "r", encoding="utf-8") as fh:
            content = fh.read().strip()
    except OSError:
        return None
    return _parse_lock(content)


def _parse_lock(content: str) -> Optional[tuple]:
    content = (content or "").strip()
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


def sqlite_readonly_uri(path: str) -> str:
    """Build a SQLite `file:` URI opening `path` read-only.

    A filesystem path is NOT a URI. SQLite splits `file:<path>?<query>#<frag>`
    and percent-decodes the path, so interpolating a raw path silently breaks
    on characters users routinely put in folder names:

      * `#` (as in "Lot #12") ends the path: everything after it -- including
        the `?mode=ro` -- becomes a fragment. SQLite then opens a DIFFERENT,
        truncated path in read-write mode and CREATES it if missing. Capture
        keeps writing to the real journal (raw path, non-URI connection)
        while every read sees an empty database, so rewind reports "nothing
        to restore" on a full journal and drops a ghost file next to it.
      * `%41` is decoded to `A`, pointing the read at another directory.

    Only the three characters SQLite gives URI meaning to are escaped. The
    rest of the path is left untouched on purpose: `Path.as_uri()` would
    rewrite a UNC path `\\\\server\\share\\x` into the authority form
    `file://server/share/x`, which SQLite rejects, and network shares are a
    normal place to keep a project.
    """
    # Order matters: '%' first, or the escapes introduced below get escaped.
    return "file:" + (
        path.replace("%", "%25").replace("#", "%23").replace("?", "%3F")
    ) + "?mode=ro"


# ----------------------------------------------------------------------
# Writer lock
# ----------------------------------------------------------------------
#
# `<journal>.rlwriter` holds "pid|host|heartbeat". Two protections, because
# either alone has a hole:
#
# * the CONTENT, refreshed every _LOCK_HEARTBEAT_SEC, says who writes and
#   whether it is still alive. A lock from another machine used to be
#   declared stale whatever its age: on a network share two technicians
#   ended up writing one WAL journal. It is now live until its heartbeat
#   is older than _LOCK_TTL_SEC.
# * an OS byte-range lock held for the session makes the take-over atomic.
#   "Read the file, then write ours" let two QGIS started together both
#   see no lock and both write; the OS lock lets exactly one through, and
#   it is released by the OS itself when the holder dies.
#
# The lock is per journal and per PROCESS: two JournalManager of the same
# process opening the same journal share it (reference count), as they
# always could.

class _HeldLock:
    def __init__(self, lock_path: str, fd: int, os_locked: bool):
        self.lock_path = lock_path
        self.fd: Optional[int] = fd
        self.os_locked = os_locked
        self.refs = 1
        self.lost = False
        self.lost_reason = ""
        self.stop = threading.Event()
        self.io_lock = threading.Lock()
        self.thread: Optional[threading.Thread] = None


_HELD_LOCKS: Dict[str, _HeldLock] = {}
_HELD_GUARD = threading.Lock()


def _lock_key(lock_path: str) -> str:
    # Both spellings of a long path (plain, extended) are the same lock.
    return path_key(lock_path)


def _lock_path_of(journal_path: str) -> str:
    """The .rlwriter file of *journal_path*, usable past MAX_PATH."""
    return when_needed(journal_path + _LOCK_SUFFIX)


def _me() -> Tuple[int, str]:
    return os.getpid(), socket.gethostname()


def _lock_payload() -> bytes:
    pid, host = _me()
    return f"{pid}|{host}|{int(time.time())}\n".encode("utf-8")


def _os_lock(fd: int) -> str:
    """'locked', 'busy' (another handle holds it) or 'unsupported: ...'."""
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, _OS_LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, _OS_LOCK_OFFSET, 0)
        return "locked"
    except OSError as exc:
        # EACCES / EAGAIN / EDEADLOCK: another handle holds the byte.
        if exc.errno in (13, 11, 35, 36):
            return "busy"
        return f"unsupported: {exc}"
    except ImportError as exc:
        return f"unsupported: {exc}"
    finally:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
        except OSError:
            pass


def _os_unlock(fd: int) -> None:
    try:
        if os.name == "nt":
            import msvcrt
            os.lseek(fd, _OS_LOCK_OFFSET, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.lockf(fd, fcntl.LOCK_UN, 1, _OS_LOCK_OFFSET, 0)
    except (OSError, ImportError) as exc:
        flog(f"JournalManager: OS unlock failed: {exc}", "DEBUG")


def _read_fd(fd: int) -> Optional[tuple]:
    try:
        os.lseek(fd, 0, os.SEEK_SET)
        data = os.read(fd, 512)
    except OSError:
        return None
    return _parse_lock(data.decode("utf-8", errors="replace"))


def _write_fd(fd: int) -> None:
    data = _lock_payload()
    os.lseek(fd, 0, os.SEEK_SET)
    os.write(fd, data)
    os.ftruncate(fd, len(data))
    os.fsync(fd)


def _heartbeat_age(info: tuple, lock_path: str) -> float:
    """Seconds since the holder last proved it was alive (content or mtime)."""
    ts = float(info[2])
    try:
        ts = max(ts, os.path.getmtime(lock_path))
    except OSError:
        pass
    return time.time() - ts


def _judge_lock(info: tuple, lock_path: str) -> Tuple[bool, str, float]:
    """(live, reason, heartbeat_age_s) for the content of a lock file."""
    pid, host, _ts = info
    me_pid, me_host = _me()
    age = _heartbeat_age(info, lock_path)
    if host == me_host:
        if pid == me_pid:
            return False, "left by this process id", age
        if not _is_pid_alive(pid):
            return False, "dead pid", age
        if age <= _LOCK_TTL_SEC:
            return True, "live pid, fresh heartbeat", age
        image = _process_image_name(pid)
        if image and not _looks_like_qgis(image):
            return False, f"pid recycled by {image}", age
        # A QGIS-like process that does not refresh: an older plugin version
        # or a frozen instance. Writing next to it is the one thing to avoid.
        return True, f"live pid ({image or 'image unknown'}), stale heartbeat", age
    if age <= _LOCK_TTL_SEC:
        return True, "other host, fresh heartbeat", age
    return False, "other host, heartbeat expired", age


def _lock_error(journal_path: str, info: Optional[tuple], reason: str,
                age: Optional[float]) -> JournalLockError:
    pid = info[0] if info else None
    host = info[1] if info else ""
    age_txt = f"{age:.0f}s" if age is not None else "unknown"
    return JournalLockError(
        f"{journal_path} is held by host={host or '?'} pid={pid if pid is not None else '?'} "
        f"(last heartbeat {age_txt} ago, {reason}; a lock is stale after "
        f"{_LOCK_TTL_SEC}s without heartbeat). Capture is INACTIVE in "
        f"this instance for this project; it resumes when the project is "
        f"saved or reopened after the other instance has released it.",
        journal_path=journal_path, holder_pid=pid, holder_host=host,
        heartbeat_age_s=age, reason=reason)


def writer_lock_holder(journal_path: str) -> Optional[dict]:
    """Who holds the writer lock of *journal_path*, None when it is free.

    Read-only probe (no lock is taken or left behind), for callers that
    wait for a journal to come free and for the orphan cleanup.
    """
    lock_path = _lock_path_of(journal_path)
    with _HELD_GUARD:
        held = _HELD_LOCKS.get(_lock_key(lock_path))
        if held is not None and not held.lost:
            pid, host = _me()
            return {"pid": pid, "host": host, "heartbeat_age_s": 0.0,
                    "reason": "held by this process"}
    if not os.path.isfile(lock_path):
        return None
    info = _read_lock_file(lock_path)
    try:
        fd = os.open(lock_path, os.O_RDWR)
    except OSError:
        fd = None
    try:
        if fd is not None:
            state = _os_lock(fd)
            if state == "busy":
                return {"pid": info[0] if info else None,
                        "host": info[1] if info else "",
                        "heartbeat_age_s": _heartbeat_age(info, lock_path) if info else None,
                        "reason": "OS lock held"}
            if state == "locked":
                _os_unlock(fd)
    finally:
        if fd is not None:
            os.close(fd)
    if info is None:
        return None
    live, reason, age = _judge_lock(info, lock_path)
    if not live:
        return None
    return {"pid": info[0], "host": info[1], "heartbeat_age_s": age,
            "reason": reason}


def _acquire_lock(journal_path: str) -> Optional[_HeldLock]:
    """Take the writer lock of *journal_path* for this process.

    Returns the held lock, or None when no lock file can be written at all
    (read-only directory): the caller then runs without multi-writer
    protection and says so. Raises JournalLockError when another live
    instance holds it.
    """
    lock_path = _lock_path_of(journal_path)
    key = _lock_key(lock_path)
    with _HELD_GUARD:
        held = _HELD_LOCKS.get(key)
        if held is not None and not held.lost:
            held.refs += 1
            return held
    try:
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    except OSError as exc:
        info = _read_lock_file(lock_path)
        if info is not None:
            live, reason, age = _judge_lock(info, lock_path)
            if live:
                raise _lock_error(journal_path, info, reason, age) from exc
        flog(f"JournalManager: cannot write lock-file {lock_path}: {exc}. "
             f"Multi-writer protection disabled for this session.", "ERROR")
        return None
    state = _os_lock(fd)
    info = _read_fd(fd)
    if state == "busy":
        os.close(fd)
        age = _heartbeat_age(info, lock_path) if info else None
        raise _lock_error(journal_path, info, "OS lock held by another instance", age)
    if info is not None:
        live, reason, age = _judge_lock(info, lock_path)
        if live:
            if state == "locked":
                _os_unlock(fd)
            os.close(fd)
            raise _lock_error(journal_path, info, reason, age)
        flog(
            f"JournalManager: stale writer lock reclaimed "
            f"(pid={info[0]}, host={info[1]}, heartbeat_age_s={age:.0f}, "
            f"reason={reason}) on {journal_path}",
            "WARNING",
        )
    if state != "locked":
        flog(f"JournalManager: OS lock unavailable on {lock_path} ({state}); "
             f"the heartbeat alone protects this journal", "WARNING")
    try:
        _write_fd(fd)
    except OSError as exc:
        if state == "locked":
            _os_unlock(fd)
        os.close(fd)
        flog(f"JournalManager: cannot write lock-file {lock_path}: {exc}. "
             f"Multi-writer protection disabled for this session.", "ERROR")
        return None
    held = _HeldLock(lock_path, fd, state == "locked")
    held.thread = threading.Thread(
        target=_heartbeat_loop, args=(held, journal_path),
        name="RecoverLand-LockHeartbeat", daemon=True)
    with _HELD_GUARD:
        _HELD_LOCKS[key] = held
    held.thread.start()
    return held


def _heartbeat_loop(held: _HeldLock, journal_path: str) -> None:
    me = _me()
    failures = 0
    while not held.stop.wait(_LOCK_HEARTBEAT_SEC):
        with held.io_lock:
            if held.fd is None:
                return
            info = _read_fd(held.fd)
            if info is None or (info[0], info[1]) != me:
                held.lost = True
                held.lost_reason = (
                    f"host={info[1]} pid={info[0]}" if info else "lock emptied")
                flog(
                    f"JournalManager: writer lock of {journal_path} taken over "
                    f"({held.lost_reason}): another instance may be writing "
                    f"this journal. The heartbeat stops; reopen the project "
                    f"once the other instance is closed.",
                    "CRITICAL",
                )
                return
            try:
                _write_fd(held.fd)
                failures = 0
            except OSError as exc:
                failures += 1
                if failures == 1 or failures % 10 == 0:
                    flog(f"JournalManager: heartbeat of {held.lock_path} "
                         f"failed ({failures}x): {exc}", "WARNING")


def _release_lock(held: _HeldLock) -> None:
    key = _lock_key(held.lock_path)
    with _HELD_GUARD:
        held.refs -= 1
        if held.refs > 0:
            return
        if _HELD_LOCKS.get(key) is held:
            del _HELD_LOCKS[key]
    held.stop.set()
    if held.thread is not None and held.thread is not threading.current_thread():
        held.thread.join(timeout=2.0)
    with held.io_lock:
        fd, held.fd = held.fd, None
    if fd is None:
        return
    info = _read_fd(fd)
    ours = info is not None and (info[0], info[1]) == _me()
    if not ours:
        flog(f"JournalManager: lock-file not owned by us "
             f"(pid={info[0] if info else '?'}); skipping remove", "DEBUG")
    if ours and os.name != "nt":
        # POSIX: unlink while the OS lock is still ours, so nobody can take
        # the file between the unlock and the removal.
        _remove_quietly(held.lock_path)
    if held.os_locked:
        _os_unlock(fd)
    try:
        os.close(fd)
    except OSError:
        pass
    if ours and os.name == "nt":
        # Windows: a file we hold open cannot be deleted. A racer who opened
        # it in between also holds it open, and the removal then fails.
        _remove_quietly(held.lock_path)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError as exc:
        flog(f"JournalManager: cannot remove lock-file {path}: {exc}", "DEBUG")


def _read_lock_raw(lock_path: str) -> Optional[Tuple[bytes, Tuple[int, int]]]:
    """Exact bytes and (atime_ns, mtime_ns) of a lock file, to put it back."""
    try:
        with open(lock_path, "rb") as fh:
            data = fh.read(512)
        st = os.stat(lock_path)
    except OSError:
        return None
    return data, (st.st_atime_ns, st.st_mtime_ns)


def _put_lock_content_back(held: Optional[_HeldLock], raw) -> bool:
    """Write the previous holder's lock content back through our handle.

    Once the file names somebody else, _release_lock leaves it in place
    instead of deleting it as ours.
    """
    if held is None or raw is None:
        return False
    data = raw[0]
    with held.io_lock:
        fd = held.fd
        if fd is None:
            return False
        mode = None
        try:
            if os.name == "nt":
                # os.open gives a text-mode descriptor on Windows: the bytes
                # must go back exactly as they were, line ends included.
                import msvcrt
                mode = msvcrt.setmode(fd, os.O_BINARY)
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, data)
            os.ftruncate(fd, len(data))
            os.fsync(fd)
        except (OSError, ImportError) as exc:
            flog(f"JournalManager: cannot put back the lock of the previous "
                 f"holder in {held.lock_path}: {exc}", "WARNING")
            return False
        finally:
            if mode is not None:
                try:
                    import msvcrt
                    msvcrt.setmode(fd, mode)
                except (OSError, ImportError):
                    pass
    return True


def _restore_lock_times(lock_path: str, raw) -> None:
    """Give a lock put back its own heartbeat time (the file mtime counts)."""
    if raw is None:
        return
    try:
        os.utime(lock_path, ns=raw[1])
    except OSError as exc:
        flog(f"JournalManager: cannot restore the time of {lock_path}: {exc}",
             "DEBUG")


# ----------------------------------------------------------------------
# Journal targets
# ----------------------------------------------------------------------

class _JournalTarget:
    """Where a project's journal lives and why (kind)."""

    __slots__ = ("path", "kind", "session_key")

    def __init__(self, path: str, kind: str, session_key: str = ""):
        self.path = path
        # saved:   next to a project file
        # storage: keyed on a project storage URI (GeoPackage, PostgreSQL)
        # hint:    keyed on the would-be path of a project not on disk
        # session: random key, one per unsaved session
        self.kind = kind
        self.session_key = session_key


def _same_file(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return False
    return path_key(a) == path_key(b)


def _isfile(path: str) -> bool:
    """os.path.isfile, right past MAX_PATH too.

    A plain path of 260 characters or more answers False for a file that
    exists (measured): a project saved in a deep folder was taken for an
    unsaved one and its journal went to the profile, where the orphan
    cleanup deletes what is not touched for 30 days.
    """
    return bool(path) and os.path.isfile(when_needed(path))


def _isdir(path: str) -> bool:
    return bool(path) and os.path.isdir(when_needed(path))


def _project_storage_key() -> str:
    """Storage URI of the current project, '' unless it lives in a storage.

    A project kept in a GeoPackage or PostgreSQL project storage has an
    empty absoluteFilePath() (QGIS API); it used to be taken for an unsaved
    project and got a brand new journal at every open. Its fileName() is
    the storage URI, stable across opens.
    """
    try:
        from qgis.core import QgsProject
        project = QgsProject.instance()
        if project.projectStorage() is None:
            return ""
        return project.fileName() or ""
    except Exception as exc:  # noqa: BLE001 - no QGIS: no storage
        flog(f"JournalManager: project storage unknown ({exc})", "DEBUG")
        return ""


def _normalize_storage_key(uri: str) -> str:
    """Drop the credentials a storage URI may carry (user:password@).

    The same project opened by two users must find the same journal, and
    the key is hashed anyway, never logged.
    """
    return re.sub(r"//[^/@?]*@", "//", uri.strip())


def _being_saved_to(project_path: str) -> bool:
    """A project file QGIS is writing right now: its folder exists.

    projectSaved is emitted inside QgsProject.write(), before a .qgz
    archive is on disk (QGIS 4.2), so the file itself cannot be the test.
    """
    if not project_path:
        return False
    return _isdir(os.path.dirname(os.path.abspath(project_path)))


def _resolve_target(project_path: str, profile_path: str,
                    being_written: bool = False) -> _JournalTarget:
    if project_path and (_isfile(project_path)
                         or (being_written and _being_saved_to(project_path))):
        return _JournalTarget(_journal_for_saved_project(project_path), "saved")
    storage = "" if project_path else _project_storage_key()
    if storage:
        return _JournalTarget(
            _journal_for_storage_project(profile_path, storage), "storage")
    if project_path:
        return _JournalTarget(
            _journal_for_unsaved_project(profile_path, project_path), "hint")
    key = uuid.uuid4().hex[:16]
    return _JournalTarget(
        _journal_for_unsaved_project(profile_path, "", key), "session", key)


class HistoryCarry:
    """What must follow a project that has just been saved elsewhere.

    Planned by JournalManager.plan_history_carry while the source journal
    is still open (inside QgsProject.write()), recorded durably in the
    profile (see _remember_owed), and carried out when the source journal
    closes, whoever closes it. If QGIS dies first, the next open of the
    target journal does it (JournalManager._settle_owed_carries).
    """

    _FIELDS = ("source", "source_kind", "source_session_key", "target",
               "target_kind", "since_event_id", "opened_at", "mark_source",
               "project_file", "profile_path", "planned_at", "project_stamp",
               "until_event_id", "until_at", "until_uids")

    def __init__(self, source: str, source_kind: str, source_session_key: str,
                 target: str, target_kind: str, since_event_id: int,
                 opened_at: str, mark_source: bool, project_file: str = "",
                 profile_path: str = "", planned_at: str = "",
                 project_stamp: Optional[list] = None,
                 until_event_id: Optional[int] = None, until_at: str = "",
                 until_uids: Optional[List[str]] = None):
        self.source = source
        self.source_kind = source_kind
        self.source_session_key = source_session_key
        self.target = target
        self.target_kind = target_kind
        self.since_event_id = since_event_id
        self.opened_at = opened_at
        self.mark_source = mark_source
        # The project file the save writes ('' for a project storage): a
        # carry into the journal of a file that was never written is void.
        self.project_file = project_file
        # (mtime_ns, size, file id) of the ARCHIVE (.qgz) the save must
        # replace, taken in projectSaved, which fires before an archive is
        # written and even when writing it then fails (measured, QGIS
        # 4.2.1): unchanged afterwards, the save failed. None when there was
        # no file, and always None for a .qgs: QGIS writes it BEFORE
        # emitting projectSaved (a failed .qgs save emits nothing), so its
        # stamp at that point is already the new file's (see _writes_archive).
        self.project_stamp = project_stamp
        self.profile_path = profile_path
        self.planned_at = planned_at or _utc_now()
        # Upper bound of what the source holds for this project (None: not
        # bounded yet). A saved source is the journal of a whole directory:
        # once its session is over, later rows belong to other projects.
        # until_uids: the events of that session still in the source's
        # outbox when the bound was set (a session that died); replayed
        # later, they get event_ids above the bound and still belong here.
        # until_at: when the bound was set.
        self.until_event_id = until_event_id
        self.until_at = until_at
        self.until_uids = until_uids

    def as_dict(self) -> dict:
        return {name: getattr(self, name) for name in self._FIELDS}

    @classmethod
    def from_dict(cls, data: dict) -> "HistoryCarry":
        stamp = data.get("project_stamp")
        until = data.get("until_event_id")
        uids = data.get("until_uids")
        return cls(
            source=str(data.get("source") or ""),
            source_kind=str(data.get("source_kind") or ""),
            source_session_key=str(data.get("source_session_key") or ""),
            target=str(data.get("target") or ""),
            target_kind=str(data.get("target_kind") or ""),
            since_event_id=int(data.get("since_event_id") or 0),
            opened_at=str(data.get("opened_at") or ""),
            mark_source=bool(data.get("mark_source")),
            project_file=str(data.get("project_file") or ""),
            profile_path=str(data.get("profile_path") or ""),
            planned_at=str(data.get("planned_at") or ""),
            project_stamp=list(stamp) if isinstance(stamp, list) else None,
            until_event_id=int(until) if until is not None else None,
            until_at=str(data.get("until_at") or ""),
            until_uids=[str(u) for u in uids] if isinstance(uids, list) else None)


class HistoryCarryReport:
    """What a carry did, in the terms the user needs."""

    def __init__(self, source: str, target: str, since_event_id: int = 0):
        self.source = source
        self.target = target
        self.since_event_id = since_event_id
        self.events_seen = 0
        self.events_inserted = 0
        self.events_already_present = 0
        self.legacy_identified = 0
        self.traces_remapped = 0
        self.orphan_traces = 0
        self.commits_copied = 0
        self.alias_conflicts = 0
        self.outbox: Optional[dict] = None
        self.outbox_segments_left = 0
        self.source_marked = False
        # The source journal could not be written (archived project, share
        # mounted read-only): it was only read, nothing was written to it.
        self.source_read_only = False
        # The source journal is provably gone (a listable folder without
        # it): nothing could be carried, and the user is told.
        self.source_missing = False
        # Outbox lines the source's journal did not take (see
        # _session_outbox): this session's events carried from there, this
        # session's events the journal refuses (left there), other sessions'
        # events (left there, not ours), and lines that cannot be parsed
        # (whose they are is unknown).
        self.session_outbox_carried = 0
        self.session_outbox_left = 0
        self.session_outbox_left_why = ""
        self.outbox_foreign_events = 0
        self.outbox_unreadable_lines = 0
        # Highest event_id of the source this carry looked at (None: the
        # source was not read, never taken for 0).
        self.through_event_id: Optional[int] = None
        # Entities the target journal had already changed AFTER the first
        # carried change of the same entity (another instance wrote them in
        # between): their history is merged out of order, and said so.
        self.out_of_order_entities = 0
        self.ok = False
        self.fell_back = False
        # Not carried, but still recorded as owed: the next open of the
        # project's journal tries again.
        self.owed = False
        self.error = ""
        # Where the carry failed: 'source' (the journal holding the history),
        # 'target' (the project's journal), 'project_file' (the save did not
        # write its file), '' (unknown or none).
        self.failure_side = ""
        # With failure_side 'project_file': 'missing' / 'unchanged' (the
        # save failed) or 'unknown' (its folder cannot be read right now).
        self.project_file_state = ""
        # The journal capture goes to after this carry, and whether a real
        # write went through there (JournalManager.open_for_project).
        self.capture_journal = ""
        self.capture_write_ok = False
        self.warnings: List[str] = []

    def as_dict(self) -> dict:
        return dict(self.__dict__)

    def user_message(self) -> Tuple[str, str]:
        """(level, French text) for the message bar; text '' = nothing to say.

        Never says where recording goes on unless a write there was proven
        (capture_write_ok): telling the user "no loss" over a journal that
        refuses every write is how edits went missing after a "Save As" out
        of a read-only journal.
        """
        source, target = plain(self.source), plain(self.target)
        if self.fell_back and self.capture_write_ok:
            return "CRITICAL", (
                f"Projet enregistre, mais le journal du projet ({target}) "
                f"n'a pas pu recevoir l'historique de la session ({self.error}). "
                f"L'enregistrement continue dans {source}, sans perte ; le "
                f"rattachement sera retente au prochain enregistrement du projet "
                f"ou a sa prochaine ouverture.")
        if not self.ok:
            retry = (" Le rattachement sera retente a la prochaine ouverture du "
                     "projet." if self.owed else "")
            where = ""
            if self.capture_journal and self.capture_write_ok:
                where = (f" L'enregistrement continue dans "
                         f"{plain(self.capture_journal)}.")
            if self.failure_side == "project_file":
                if self.project_file_state == "unknown":
                    return "WARNING", (
                        f"L'enregistrement du projet n'a pas pu etre verifie "
                        f"({self.error}) : l'historique capture reste dans "
                        f"{source}.{retry}{where}")
                return "CRITICAL", (
                    f"Le projet n'a pas ete enregistre ({self.error}) : "
                    f"l'historique capture reste dans {source}.{retry}{where}")
            side = {"source": "journal d'origine",
                    "target": "journal du projet"}.get(self.failure_side, "")
            detail = f"{side} : {self.error}" if side else self.error
            return "CRITICAL", (
                f"L'historique capture avant l'enregistrement du projet n'a pas "
                f"pu etre rattache a son journal ({detail}). Il reste intact "
                f"dans {source}.{retry}{where}")
        if self.source_missing:
            return "WARNING", (
                f"Le journal d'origine ({source}) n'existe plus : l'historique "
                f"capture avant l'enregistrement du projet n'a pas pu etre "
                f"rattache a son journal ({target}).")
        notes = self.notes()
        others = ""
        if self.outbox_foreign_events and self.source_read_only:
            others = (
                f" L'outbox de {source} (journal en lecture seule) garde "
                f"{self.outbox_foreign_events} modification(s) d'autres "
                f"sessions, qui n'appartiennent pas a celle-ci ; elles y restent "
                f"conservees.")
        if notes:
            return "WARNING", (
                f"{self.events_inserted} modification(s) rattachee(s) au journal "
                f"du projet ({target}), mais " + " ; ".join(notes) + "."
                + others)
        if self.events_inserted:
            return "INFO", (
                f"Projet enregistre : {self.events_inserted} modification(s) "
                f"capturee(s) avant l'enregistrement ont ete rattachee(s) au "
                f"journal du projet ({target})." + others)
        return "INFO", others.strip()

    def notes(self) -> List[str]:
        """What a carry that went through leaves out or cannot vouch for.

        French, one clause each; shared by the message of a carry at the
        save and by the one of an owed carry done at a later open.
        """
        source = plain(self.source)
        notes = []
        if self.session_outbox_left:
            notes.append(
                f"{self.session_outbox_left} modification(s) de cette session, "
                f"en attente dans l'outbox de {source}, sont refusee(s) par le "
                f"journal ({self.session_outbox_left_why}) : l'historique du "
                f"projet ne les contient pas ; elles restent conservees dans "
                f"cette outbox")
        if self.outbox_unreadable_lines:
            notes.append(
                f"{self.outbox_unreadable_lines} ligne(s) illisible(s) de "
                f"l'outbox de {source} n'ont pu etre attribuees a aucune "
                f"session ; elles y restent conservees")
        if self.outbox_segments_left and not self.source_read_only:
            carried = ""
            if self.session_outbox_carried:
                carried = (f" (les {self.session_outbox_carried} modification(s) "
                           f"de cette session qu'ils contiennent ont ete "
                           f"rattachees au projet)")
            notes.append(
                f"{self.outbox_segments_left} lot(s) en attente dans l'outbox "
                f"de {source} n'ont pas pu etre integres a ce journal ; ils y "
                f"restent conserves{carried}")
        if self.out_of_order_entities:
            notes.append(
                f"{self.out_of_order_entities} entite(s) avaient deja ete "
                f"modifiee(s) plus tard dans le journal du projet (une autre "
                f"instance) : l'ordre de leur historique peut etre inexact, "
                f"verifiez-les avant un Rewind")
        return notes


class JournalManager:
    """Manages the SQLite audit journal file and connections."""

    def __init__(self):
        self._conn: Optional[sqlite3.Connection] = None
        self._path: Optional[str] = None
        # Not a credential: a per-session discriminant that gives an
        # unsaved project its own journal filename (see
        # _journal_for_unsaved_project).
        self._unsaved_session_key: Optional[str] = None
        self._lock_acquired: bool = False
        self._held_lock: Optional[_HeldLock] = None
        self._reconciliation = None
        self._kind = ""
        self._event_id_at_open = 0
        self._opened_at = ""
        # Planned at the save, carried out by close() (see HistoryCarry).
        self._pending_carry: Optional[HistoryCarry] = None
        # A carry close() could not complete: the next open_for_project of
        # its target keeps capturing into the source instead (nothing split)
        # when the project's journal is what refused it and the source takes
        # writes (see _fallback_refusal).
        self._owed_carry: Optional[HistoryCarry] = None
        self.last_carry_report: Optional[HistoryCarryReport] = None
        self.last_lock_error: Optional[JournalLockError] = None

    @property
    def path(self) -> Optional[str]:
        return self._path

    @property
    def pending_carry(self) -> Optional[HistoryCarry]:
        """The history carry planned at the last save, not yet carried out.

        Read once the save call has returned (inside QgsProject.write() a
        .qgz is not on disk yet). projectSaved also fires for a .qgz save
        that FAILED (measured, QGIS 4.2.1: a folder that does not exist, a
        read-only file left untouched): a plan whose project file was
        provably not written is dropped here and None is returned, so the
        journal stays where the history is. A file that cannot be checked
        (folder unreachable) also answers None, and the plan is kept: the
        close decides again. A .qgs is written before projectSaved, and a
        failed .qgs save emits nothing: its file being there is the proof.
        """
        carry = self._pending_carry
        if carry is None:
            return None
        state = _project_file_state(carry)
        if state in ("missing", "unchanged"):
            self.cancel_history_carry(
                f"the save did not write {plain(carry.project_file)} ({state})")
            return None
        if state == "unknown":
            flog(f"JournalManager: history carry {carry.source} -> "
                 f"{carry.target} on hold: the project file "
                 f"{carry.project_file} cannot be checked", "WARNING")
            return None
        return carry

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    @property
    def kind(self) -> str:
        """saved / storage / hint / session ('' when closed)."""
        return self._kind

    @property
    def is_lock_degraded(self) -> bool:
        """True if the journal is open but the writer lock could not be acquired."""
        return self.is_open and not self._lock_acquired

    @property
    def is_lock_lost(self) -> bool:
        """True when another instance took this journal's lock over.

        Detected by the heartbeat. From then on two instances may write the
        journal: the interface must say so.
        """
        held = self._held_lock
        return bool(self.is_open and held is not None and held.lost)

    @property
    def is_reconciliation_degraded(self) -> bool:
        """True when part of the history is not attached to its source.

        The v6 reconciliation refuses to guess: an ambiguous source, a
        fingerprint with no registered address, or an interrupted pass
        leaves events readable under their old identity but invisible to
        a rewind scoped on the current one. The interface must be able
        to say so BEFORE proposing a rewind.
        """
        report = self._reconciliation
        return bool(report is not None and report.is_degraded)

    def reconciliation_warning(self) -> str:
        """French, user-facing summary of the degradation ('' if none)."""
        if self._conn is None:
            return ""
        try:
            from .datasource_alias import describe_reconciliation
            return describe_reconciliation(self._conn)
        except Exception as exc:  # noqa: BLE001 - a warning never blocks
            flog(f"JournalManager: cannot describe reconciliation: {exc}",
                 "WARNING")
            return ""

    def open_for_project(self, project_path: str, profile_path: str) -> str:
        """Open or create the journal for the given project context.

        Returns the journal file path.
        Raises OSError if the directory cannot be created.
        Raises sqlite3.Error if the database cannot be opened.
        Raises JournalLockError if another live QGIS instance holds the
        writer lock for this journal (multi-writer protection).

        The current journal is closed first, which carries out a history
        carry planned at the last save (see close). If that carry could not
        complete and this project resolves to its target, the SOURCE journal
        is opened instead only when the project's journal is what refused
        the carry (held by another instance, not writable) AND a real write
        to the source goes through: capture then goes on where the history
        is, nothing is split, and the next save retries. In every other case
        (the source is read-only, or is what failed) the project's journal
        is opened as usual and the carry stays owed. last_carry_report says
        what happened, which side failed and where capture goes.

        Once the journal is open, whatever earlier sessions still owe it
        (a save followed by a crash of QGIS) is carried into it.
        """
        self.close()
        owed, self._owed_carry = self._owed_carry, None
        self.last_lock_error = None
        target = _resolve_target(project_path, profile_path)
        report = None
        if owed is not None and _same_file(owed.target, target.path):
            report = self._report_for(owed)
            refusal = self._fallback_refusal(owed, report)
            if not refusal:
                return self._fall_back_to_source(owed, target, report)
            flog(f"JournalManager: capture not kept on {owed.source}: "
                 f"{refusal}. The project's journal {target.path} takes it; "
                 f"the history carry stays owed.", "WARNING")
            self.last_carry_report = report
        path = self._open_target_or_record(target)
        if report is not None:
            report.capture_journal = path
            probe = _write_probe(self._conn)
            report.capture_write_ok = not probe
            if probe:
                flog(f"JournalManager: {path} refuses writes ({probe})", "ERROR")
        self._freeze_owed_sources(profile_path)
        self._settle_owed_carries(profile_path)
        return path

    def _report_for(self, carry: HistoryCarry) -> HistoryCarryReport:
        report = self.last_carry_report
        if report is None or not _same_file(report.target, carry.target):
            report = HistoryCarryReport(carry.source, carry.target,
                                        carry.since_event_id)
            report.error = "the history carry did not complete"
        return report

    @staticmethod
    def _fallback_refusal(carry: HistoryCarry, report: HistoryCarryReport) -> str:
        """'' when capture may stay on the source of *carry*, else why not.

        Staying on the source only makes sense when the project's journal is
        what refused the carry. When the SOURCE is what failed (read-only
        archive, unreadable, held elsewhere), keeping capture there sent
        every later edit to a journal that could not take it, while the
        project's own journal was writable (measured: "Save As" out of a
        read-only journal, the next edit found in no journal at all).
        """
        if report.failure_side != "target":
            return (f"the carry failed on the {report.failure_side or 'unknown'} "
                    f"side ({report.error})")
        if not _journal_file_writable(carry.source):
            return f"the source journal {carry.source} does not take writes"
        return ""

    def _open_target_or_record(self, target: _JournalTarget) -> str:
        try:
            return self._open_target(target)
        except JournalLockError as exc:
            self.last_lock_error = exc
            flog(f"JournalManager: journal refused, capture inactive for this "
                 f"project: {exc}", "WARNING")
            raise

    def _fall_back_to_source(self, carry: HistoryCarry, target: _JournalTarget,
                             report: HistoryCarryReport) -> str:
        """Keep capturing into the journal that holds the history.

        Only once a real write to it went through (_write_probe): a journal
        that opens but refuses writes is served read-only by
        initialize_schema, and telling the user "recording continues there,
        no loss" over it was false.
        """
        source = _JournalTarget(carry.source, carry.source_kind,
                                carry.source_session_key or "")
        try:
            self._open_target(source)
            probe = _write_probe(self._conn)
            if probe:
                raise sqlite3.OperationalError(f"write probe refused: {probe}")
        except Exception as exc:  # noqa: BLE001
            self.close()
            flog(f"JournalManager: cannot keep capturing into the source "
                 f"journal {carry.source} ({type(exc).__name__}: {exc}); the "
                 f"history stays there and its carry stays owed", "ERROR")
            # Capture goes to the project's journal (or is refused by its
            # lock, loudly); the history stays in the source, kept by the
            # orphan cleanup and carried at a later open of the target.
            self.last_carry_report = report
            path = self._open_target_or_record(target)
            report.capture_journal = path
            report.capture_write_ok = not _write_probe(self._conn)
            self._freeze_owed_sources(carry.profile_path)
            self._settle_owed_carries(carry.profile_path)
            return path
        # The session window of the source is the one planned, not "now".
        self._event_id_at_open = carry.since_event_id
        self._opened_at = carry.opened_at
        report.fell_back = True
        report.capture_journal = carry.source
        report.capture_write_ok = True
        self.last_carry_report = report
        # Capture goes on in the source on behalf of the project: whatever
        # it holds from now on is owed to it, the bound set at close is off.
        _update_owed(carry.profile_path, carry.source, carry.target,
                     until_event_id=None, until_at="", until_uids=None)
        # The carry is still owed: the next save plans it again, and the
        # durable record makes the next open of the target try as well.
        flog(f"JournalManager: journal kept on {carry.source}: {report.error}. "
             f"The history will be carried to {carry.target} at the next save.",
             "WARNING")
        return self._path

    def _open_target(self, target: _JournalTarget) -> str:
        self._ensure_directory(target.path)
        # Who held it before us: an older client's lock looks stale (it is
        # never refreshed) and is reclaimed below, yet that client may still
        # be writing the journal in WAL mode on a share.
        lock_path = _lock_path_of(target.path)
        previous = _read_lock_file(lock_path)
        previous_raw = _read_lock_raw(lock_path) if previous is not None else None
        self._acquire_writer_lock(target.path)
        try:
            self._open_connection(target.path)
        except NetworkJournalBusyError as exc:
            if previous is not None and (previous[0], previous[1]) == _me():
                previous = None
            # That client is live (it keeps the journal in WAL): its lock
            # file, reclaimed above as stale, is put back as it was instead
            # of being deleted with ours. It is the only trace of the holder.
            if previous is not None and previous_raw is not None:
                _put_lock_content_back(self._held_lock, previous_raw)
            self._release_writer_lock(target.path)
            if previous is not None and previous_raw is not None:
                _restore_lock_times(lock_path, previous_raw)
            raise JournalLockError(
                f"{target.path} cannot be written from this machine: {exc}. "
                f"Capture is INACTIVE in this instance for this project; it "
                f"resumes when the project is saved or reopened after the "
                f"other client has closed it.",
                journal_path=target.path,
                holder_pid=previous[0] if previous else None,
                holder_host=previous[1] if previous else "",
                reason=_WAL_HELD_REASON) from exc
        except Exception:
            self._release_writer_lock(target.path)
            raise
        self._path = target.path
        self._kind = target.kind
        self._unsaved_session_key = target.session_key or None
        # Unreadable right after the open (it just initialized): 0, as ever.
        self._event_id_at_open = _max_event_id(self._conn) or 0
        self._opened_at = _utc_now()
        flog(f"JournalManager: opened {target.path} kind={target.kind}")
        return target.path

    def plan_history_carry(self, project_path: str, profile_path: str,
                           being_written: bool = False) -> Optional[HistoryCarry]:
        """Plan the carry of this session's history to the project's journal.

        Called when the project is saved (projectSaved), while the current
        journal is still open. Returns None when the project already reads
        the current journal (Ctrl+S, "Save As" into the same directory) or
        is still unsaved.

        `being_written`: called from inside QgsProject.write(), where a .qgz
        is not on disk yet; the project's folder is enough to name its
        journal. It MUST be planned there and then: "New", "Open" and
        "Quit" save a dirty project and clear it (or unload the plugins)
        in the same call, with no event-loop turn in between.

        The plan is recorded durably in the profile, and carried out by
        close() when the current journal closes, whoever closes it (the
        switch to the project's journal, File > New, another project,
        unload). If QGIS dies before that, the next open of the project's
        journal carries it (_settle_owed_carries).

        What follows the project: everything, when the current journal is
        the session journal of an unsaved project (it only ever held this
        session); otherwise what this QGIS session journalled since the
        journal was opened (a "Save As" of a saved project into another
        directory must not drag the other projects of the old directory).

        Never into a profile journal keyed on a would-be path ('hint'): the
        project file is not on disk and its folder does not exist, which
        inside write() means the save is failing (measured, QGIS 4.2.1:
        projectSaved fires for a .qgz written into a missing folder, and
        write() returns False). Carrying there moved the history into a
        journal no project opens, under a "Projet enregistre" message.
        """
        self.last_carry_report = None
        if not self.is_open:
            return None
        target = _resolve_target(project_path, profile_path, being_written)
        if target.kind == "hint":
            flog(f"JournalManager: no history carry for {project_path!r}: "
                 f"neither the file nor its folder exists (a save that fails "
                 f"still emits projectSaved); the history stays in "
                 f"{self._path}", "WARNING")
            return None
        if target.kind == "session" or _same_file(target.path, self._path):
            return None
        since = 0 if self._kind == "session" else self._event_id_at_open
        project_file = (os.path.abspath(project_path)
                        if target.kind == "saved" else "")
        # Only an archive is written after projectSaved: its stamp now is
        # the file the save must replace. A .qgs is already written here,
        # and a stamp taken now made every successful .qgs save look like
        # one that left its file untouched (measured, QGIS 4.2.1).
        stamp = (_file_stamp(project_file)
                 if project_file and _writes_archive(project_file) else None)
        carry = HistoryCarry(
            source=self._path, source_kind=self._kind,
            source_session_key=self._unsaved_session_key or "",
            target=target.path, target_kind=target.kind,
            since_event_id=since, opened_at=self._opened_at,
            mark_source=(self._kind == "session"),
            project_file=project_file,
            profile_path=profile_path,
            project_stamp=stamp)
        self._pending_carry = carry
        self._owed_carry = None
        durable = _remember_owed(carry)
        flog(f"JournalManager: history carry planned {carry.source} -> "
             f"{carry.target} since_event_id={since} "
             f"source_kind={self._kind} target_kind={target.kind} "
             f"durable={durable}")
        return carry

    def cancel_history_carry(self, reason: str) -> None:
        """Drop the planned carry (the save did not produce its file)."""
        carry, self._pending_carry = self._pending_carry, None
        if carry is None:
            return
        _forget_owed(carry.profile_path, carry.source, carry.target)
        flog(f"JournalManager: history carry {carry.source} -> {carry.target} "
             f"cancelled: {reason}. The history stays in {carry.source}.",
             "WARNING")

    def get_connection(self) -> sqlite3.Connection:
        """Return the active connection. Raises if not open."""
        if self._conn is None:
            raise RuntimeError("Journal is not open")
        return self._conn

    def close(self) -> None:
        """Close the journal; carry out the history carry planned for it.

        The caller has stopped the writer first (_shutdown_local_backend),
        so everything accepted is in the journal or its outbox, and the
        carry (which replays that outbox) sees the whole session. It runs
        before the writer lock is released: nobody can take the source in
        between.
        """
        if self._conn is None:
            return
        carry, self._pending_carry = self._pending_carry, None
        if carry is not None and not _same_file(carry.source, self._path):
            carry = None
        try:
            self._conn.close()
            flog(f"JournalManager: closed {self._path}")
        except sqlite3.Error as e:
            flog(f"JournalManager: close error: {e}", "WARNING")
        finally:
            if carry is not None:
                self._settle_carry(carry)
            if self._path is not None:
                self._release_writer_lock(self._path)
            self._conn = None
            self._path = None
            self._unsaved_session_key = None
            self._lock_acquired = False
            self._reconciliation = None
            self._kind = ""
            self._event_id_at_open = 0
            self._opened_at = ""

    def _settle_carry(self, carry: HistoryCarry) -> None:
        """Carry out the plan of the last save as its source closes. Never raises."""
        report = HistoryCarryReport(carry.source, carry.target,
                                    carry.since_event_id)
        try:
            state = _project_file_state(carry)
            if state in ("missing", "unchanged", "unknown"):
                report.failure_side = "project_file"
                report.project_file_state = state
                report.error = _project_file_error(carry, state)
                if state == "unknown":
                    # Not provably unwritten: kept for the next open of the
                    # project's journal, which checks again.
                    report.owed = _is_owed(carry)
                    flog(f"JournalManager: history carry {carry.source} -> "
                         f"{carry.target} on hold: {report.error}; it stays "
                         f"owed", "WARNING")
                else:
                    # The save did not produce its file: the project still
                    # reads the source journal, the carry is void.
                    _forget_owed(carry.profile_path, carry.source, carry.target)
                    flog(f"JournalManager: history carry {carry.source} -> "
                         f"{carry.target} dropped: {report.error}", "WARNING")
                self.last_carry_report = report
                return
            report = _carry_into_journal(carry)
        except Exception as exc:  # noqa: BLE001 - close() must complete
            report.error = f"{type(exc).__name__}: {exc}"
        self.last_carry_report = report
        if report.ok:
            _forget_owed(carry.profile_path, carry.source, carry.target)
            return
        if carry.source_kind != "session" and carry.until_event_id is None:
            # The session of a shared (directory) journal ends here: bound
            # what is owed, so a later retry does not drag what other
            # projects of that directory write next. Only with a bound that
            # was READ: a source out of reach gave 0, and every later retry
            # carried "nothing above 0" and forgot the obligation. Left
            # unbounded, it is bounded at the next open of that journal
            # (_freeze_owed_sources), before anything else is captured there.
            bound = report.through_event_id
            if bound is None:
                bound = _journal_max_event_id(carry.source)
            uids = _outbox_uids_since(carry.source, carry.opened_at)
            if bound is None or uids is None:
                flog(f"JournalManager: owed carry {carry.source} -> "
                     f"{carry.target} left unbounded: the source cannot be read "
                     f"now (max event_id {'unknown' if bound is None else bound}, "
                     f"outbox {'unreadable' if uids is None else 'read'}); it is "
                     f"bounded at the next open of that journal", "WARNING")
            else:
                carry.until_event_id = bound
                carry.until_at = _utc_now()
                carry.until_uids = uids
                _update_owed(carry.profile_path, carry.source, carry.target,
                             until_event_id=carry.until_event_id,
                             until_at=carry.until_at, until_uids=carry.until_uids)
        report.owed = _is_owed(carry)
        self._owed_carry = carry
        flog(f"JournalManager: history carry {carry.source} -> {carry.target} "
             f"not done at close ({report.failure_side or 'unknown'} side: "
             f"{report.error}); the history stays in the source and the carry "
             f"stays owed (until_event_id={carry.until_event_id})", "WARNING")

    def _settle_owed_carries(self, profile_path: str) -> None:
        """Carry what earlier sessions still owe to the journal just opened.

        A save records durably which journal holds the history the saved
        project must show. The carry normally runs when that journal
        closes; when QGIS died first (crash, killed, power cut) the record
        is still there, and this open of the project's journal does it.
        Never raises: a carry that fails stays owed, the source is kept.
        """
        if self._conn is None or not profile_path:
            return
        try:
            with _owed_lock(profile_path):
                entries = [HistoryCarry.from_dict(e) for e in _load_owed(profile_path)]
        except Exception as exc:  # noqa: BLE001 - never block the open
            flog(f"JournalManager: owed history carries unreadable: {exc}", "WARNING")
            return
        for carry in entries:
            if (not _same_file(carry.target, self._path)
                    or _same_file(carry.source, self._path)):
                continue
            state = _project_file_state(carry)
            if state in ("missing", "unchanged"):
                self._abandon_owed(
                    profile_path, carry, _project_file_error(carry, state),
                    f"le fichier de projet {plain(carry.project_file)} n'a pas "
                    f"ete ecrit par l'enregistrement")
                continue
            if state == "unknown":
                flog(f"JournalManager: owed carry {carry.source} -> "
                     f"{carry.target} kept for a later open: "
                     f"{_project_file_error(carry, state)}", "WARNING")
                continue
            if not _isfile(carry.source):
                if _provably_absent(carry.source):
                    self._abandon_owed(
                        profile_path, carry, "the source journal no longer exists",
                        "le journal qui contenait cet historique n'existe plus")
                else:
                    # A share that is not mounted right now is not a deleted
                    # journal: the obligation waits for it.
                    flog(f"JournalManager: owed carry {carry.source} -> "
                         f"{carry.target} kept for a later open: the source "
                         f"journal is unreachable", "WARNING")
                continue
            report = carry_journal_history(
                carry.source, self._conn, self._path,
                since_event_id=carry.since_event_id, opened_at=carry.opened_at,
                mark_source=carry.mark_source,
                until_event_id=carry.until_event_id, until_at=carry.until_at,
                until_uids=carry.until_uids)
            if not report.ok:
                flog(f"JournalManager: owed carry {carry.source} -> "
                     f"{carry.target} failed again ({report.error}); it stays "
                     f"owed, the source is kept", "WARNING")
                continue
            _forget_owed(profile_path, carry.source, carry.target)
            flog(f"JournalManager: event=owed_history_carried source="
                 f"{carry.source} target={carry.target} planned_at="
                 f"{carry.planned_at} inserted={report.events_inserted}",
                 "WARNING")
            notes = report.notes()
            if report.events_inserted or notes:
                # What was left out (refused, unreadable, out of order) is
                # said here too: this is the only message of an owed carry.
                caveat = (" Attention : " + " ; ".join(notes) + ".") if notes else ""
                qlog(f"RecoverLand : {report.events_inserted} modification(s) "
                     f"capturee(s) avant l'enregistrement de ce projet, lors "
                     f"d'une session precedente (QGIS arrete avant la fin du "
                     f"rattachement, ou journal du projet alors indisponible), "
                     f"ont ete rattachee(s) a son journal ({plain(self._path)})."
                     f"{caveat}", "WARNING")

    @staticmethod
    def _abandon_owed(profile_path: str, carry: HistoryCarry, why: str,
                      why_user: str) -> None:
        """Drop an owed carry that can provably never be done, and say so."""
        _forget_owed(profile_path, carry.source, carry.target)
        flog(f"JournalManager: owed carry {carry.source} -> {carry.target} "
             f"abandoned: {why}", "WARNING")
        qlog(f"RecoverLand : un rattachement d'historique en attente vers le "
             f"journal {plain(carry.target)} est abandonne : {why_user}. "
             f"Ce qui n'a pas ete rattache reste dans {plain(carry.source)} "
             f"si ce journal existe encore.", "WARNING")

    def _freeze_owed_sources(self, profile_path: str) -> None:
        """Bound what the journal just opened still owes to other projects.

        A save followed by a crash leaves an owed carry whose source is the
        journal of a whole directory. Opening that journal again (for this
        project or another one of the directory) starts new sessions that
        belong to it, not to the project saved elsewhere: before any
        capture, the owed entries get their upper bound: the last event_id
        now, plus the events of the session that died still waiting in the
        outbox (replayed after this open, above the bound). Never raises.
        """
        if self._conn is None or not profile_path:
            return
        bound = _max_event_id(self._conn)
        now = _utc_now()
        try:
            with _owed_lock(profile_path):
                entries = _read_owed(profile_path)
                changed = False
                for entry in entries:
                    if (_same_file(entry.get("source"), self._path)
                            and entry.get("until_event_id") is None):
                        uids = _outbox_uids_since(
                            self._path, str(entry.get("opened_at") or ""))
                        if bound is None or uids is None:
                            # Never a guessed bound (see _settle_carry): the
                            # next open of this journal tries again.
                            flog(f"JournalManager: owed carry {entry.get('source')} "
                                 f"-> {entry.get('target')} not bounded: this "
                                 f"journal or its outbox cannot be read", "WARNING")
                            continue
                        entry["until_event_id"] = bound
                        entry["until_at"] = now
                        entry["until_uids"] = uids
                        changed = True
                if changed:
                    _store_owed(profile_path, entries)
                    flog(f"JournalManager: owed carries from {self._path} "
                         f"bounded at event_id={bound} until_at={now}", "INFO")
        except OSError as exc:
            flog(f"JournalManager: owed carries from {self._path} not "
                 f"bounded ({exc}): a later carry may take rows of other "
                 f"projects of this directory", "WARNING")

    def open_readonly_connection(self) -> sqlite3.Connection:
        """Open a read-only connection dedicated to Time Lens (IL-I1).

        Returns a fresh sqlite3.Connection with TWO independent layers of
        write protection:

            1. URI mode=ro  - filesystem-level (SQLITE_OPEN_READONLY).
            2. PRAGMA query_only=ON  - session-level (rejects any DML).

        Defence-in-depth: any caller that bypasses one layer still hits
        the other. Lens never shares `JournalManager._conn` (the
        read-write connection used by the WriteQueue).

        Logs `flog: journal_manager event=ro_connection_opened mode=ro
        query_only=1` to make IL-I1 traceable in `recoverland_debug.log`.
        """
        if self._path is None:
            raise RuntimeError("Journal is not open")
        conn = sqlite3.connect(
            sqlite_readonly_uri(self._path),
            uri=True,
            check_same_thread=False,
        )
        try:
            # Belt-and-suspenders: even if a caller drops the URI mode=ro
            # parameter, query_only blocks any DML at the session layer.
            conn.execute("PRAGMA query_only=ON")
            apply_pragmas(conn, read_only=True)
            # apply_pragmas may reset query_only; re-apply to be safe.
            conn.execute("PRAGMA query_only=ON")
        except sqlite3.Error:
            conn.close()
            raise
        flog(
            f"journal_manager event=ro_connection_opened "
            f"path={self._path} mode=ro query_only=1"
        )
        return conn

    def create_read_connection(self) -> sqlite3.Connection:
        """Create a separate read-only connection for search threads."""
        if self._path is None:
            raise RuntimeError("Journal is not open")
        conn = sqlite3.connect(
            sqlite_readonly_uri(self._path),
            uri=True,
            check_same_thread=False,
        )
        try:
            apply_pragmas(conn, read_only=True)
        except sqlite3.Error:
            conn.close()
            raise
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
            _initialize_journal(conn)
            version = get_schema_version(conn)
            flog(f"JournalManager: schema version={version}")
            self._refresh_planner_stats(conn)
        except Exception:
            conn.close()
            raise
        self._conn = conn
        self._load_reconciliation_state(conn)

    def _load_reconciliation_state(self, conn: sqlite3.Connection) -> None:
        """Read back what the v6 reconciliation could and could not do."""
        self._reconciliation = None
        try:
            from .datasource_alias import load_reconciliation
            report = load_reconciliation(conn)
        except Exception as exc:  # noqa: BLE001 - never block the open
            flog(f"JournalManager: cannot read the reconciliation report: "
                 f"{exc}", "WARNING")
            return
        self._reconciliation = report
        if report is None:
            return
        if report.is_degraded:
            flog(
                f"JournalManager: datasource reconciliation degraded "
                f"status={report.status} ambiguous={len(report.ambiguous)} "
                f"without_source_uri={len(report.without_source_uri)} "
                f"aliases_created={report.aliases_created} "
                f"scanned={report.scanned}",
                "WARNING",
            )
        else:
            flog(
                f"JournalManager: datasource reconciliation ok "
                f"scanned={report.scanned} "
                f"aliases={report.aliases_created}",
                "DEBUG",
            )

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
        """Take the writer lock of the journal (see the writer lock section).

        Raises JournalLockError when another live instance holds it: same
        host with a live process, or another host whose heartbeat is
        younger than _LOCK_TTL_SEC, or anyone holding the OS lock.
        """
        held = _acquire_lock(journal_path)
        self._held_lock = held
        self._lock_acquired = held is not None

    def _release_writer_lock(self, journal_path: str) -> None:
        """Release this manager's share of the lock; the last one removes it.

        The file is only removed if it still names this process: a lock
        taken over by another instance is left to its owner.
        """
        held, self._held_lock = self._held_lock, None
        if held is not None:
            _release_lock(held)

    @staticmethod
    def writer_lock_holder(journal_path: str) -> Optional[dict]:
        """Who holds the writer lock of *journal_path*, None when free."""
        return writer_lock_holder(journal_path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _initialize_journal(conn: sqlite3.Connection) -> None:
    """initialize_schema, then say what the file was before it.

    The schema transaction stamps "created" a file whose audit_event never
    held a row; only this open sees the file before the schema lands, so
    it adds "migrated" for a journal that held events below the schema
    that records every purge, and the schema it saw
    (journal_writes.coverage_origin). The stamp is a recorded
    fact for the floor readers and protects nothing on its own: the rewind
    (retention._journal_predates_v7) and Review
    (snapshot_rebuild_worker.recording_journal_creation) have to consult it
    before their schema_version inference, which a clock that was wrong
    when an older release created the file fools
    (tx_journal_coverage_origin_stamp, case C). Raises what
    initialize_schema raises; the stamp itself never fails the open.
    """
    from .journal_writes import schema_before_init, record_coverage_origin
    before = schema_before_init(conn)
    initialize_schema(conn)
    record_coverage_origin(conn, before)


def _max_event_id(conn: Optional[sqlite3.Connection]) -> Optional[int]:
    """MAX(event_id) (0 for an empty journal), None when it cannot be read.

    None is never 0: a bound read as 0 from a journal out of reach made an
    owed carry take "nothing above 0" and forget the obligation.
    """
    if conn is None:
        return None
    try:
        row = conn.execute("SELECT COALESCE(MAX(event_id), 0) FROM audit_event").fetchone()
        return int(row[0] or 0)
    except sqlite3.Error:
        return None


def _journal_max_event_id(path: str) -> Optional[int]:
    """MAX(event_id) of a journal file, read-only (None when unreadable)."""
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(path), uri=True)
    except sqlite3.Error:
        return None
    try:
        return _max_event_id(conn)
    finally:
        conn.close()


def _is_readonly_error(exc: BaseException) -> bool:
    name = str(getattr(exc, "sqlite_errorname", "") or "")
    return name.startswith("SQLITE_READONLY") or "readonly database" in str(exc)


def _journal_file_writable(path: str) -> bool:
    """The journal file and its folder can be written (not a proof, a filter).

    The proof is _write_probe on an open connection: a share mounted
    read-only by its ACL alone passes this test and still refuses writes.
    """
    try:
        full = when_needed(path)
        return (os.path.isfile(full) and os.access(full, os.W_OK)
                and os.access(os.path.dirname(full), os.W_OK))
    except OSError:
        return False


def _write_probe(conn: Optional[sqlite3.Connection]) -> str:
    """'' when a write to this journal really goes through, else why not.

    A row is written inside a transaction that is rolled back: nothing
    reaches the file. `BEGIN IMMEDIATE` alone proves nothing (it succeeds
    on a read-only WAL journal); the INSERT is refused there.
    """
    if conn is None:
        return "no connection"
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as exc:
        return f"{type(exc).__name__}: {exc}"
    try:
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES ('rl_write_probe', '1', '')")
        return ""
    except sqlite3.Error as exc:
        return f"{type(exc).__name__}: {exc}"
    finally:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass


def _writes_archive(project_file: str) -> bool:
    """True when QGIS saves *project_file* as a zip archive (.qgz).

    The same test as QgsProject::write (QgsZipUtils::isZipFile: the suffix,
    any case). It decides the order between the file and projectSaved
    (measured, QGIS 4.2.1): an archive is written AFTER the signal, which
    fires even when the archive then cannot be written; a .qgs is written
    BEFORE it, and a .qgs save that fails emits no signal at all.
    """
    try:
        from qgis.core import QgsZipUtils
        return bool(QgsZipUtils.isZipFile(project_file))
    except Exception:  # noqa: BLE001 - no QGIS: the suffix QGIS tests
        return os.path.splitext(project_file)[1].lower() == ".qgz"


def _file_stamp(path: str) -> Optional[list]:
    """[mtime_ns, size, file id] of *path*, None when it does not exist.

    A successful .qgz save replaces the archive (new file id, new mtime;
    measured, QGIS 4.2.1); a failed one leaves all three as they were.
    """
    try:
        st = os.stat(when_needed(path))
    except OSError:
        return None
    return [int(st.st_mtime_ns), int(st.st_size), int(st.st_ino)]


def _provably_absent(path: str) -> bool:
    """True only when a reachable folder is listed and *path* is not in it.

    FileNotFoundError alone is no proof: Windows answers it for a share
    that is not reachable right now too. The nearest ancestor that can be
    listed decides; none (drive or share gone) means "unknown", not absent.
    """
    child = os.path.abspath(plain(path))
    parent = os.path.dirname(child)
    while parent and parent != child:
        try:
            names = os.listdir(when_needed(parent))
        except OSError:
            child, parent = parent, os.path.dirname(parent)
            continue
        wanted = os.path.normcase(os.path.basename(child))
        return all(os.path.normcase(name) != wanted for name in names)
    return False


def _project_file_state(carry: HistoryCarry) -> str:
    """Did the save behind *carry* write its project file?

    'none' (no file: project storage), 'written', 'missing' (provably not
    on disk), 'unchanged' (the archive the save should have replaced is
    untouched) or 'unknown' (cannot be checked now). A stamp only means
    something for an archive (see HistoryCarry.project_stamp): one recorded
    for a .qgs is the file already written, never a failed save.
    """
    if not carry.project_file:
        return "none"
    stamp = _file_stamp(carry.project_file)
    if stamp is None:
        return "missing" if _provably_absent(carry.project_file) else "unknown"
    if (carry.project_stamp and _writes_archive(carry.project_file)
            and list(carry.project_stamp) == stamp):
        return "unchanged"
    return "written"


def _project_file_error(carry: HistoryCarry, state: str) -> str:
    name = plain(carry.project_file)
    if state == "missing":
        return f"the project file {name} was not written"
    if state == "unchanged":
        return f"the save left {name} untouched"
    return f"the project file {name} cannot be checked (its folder is unreachable)"


def journal_shares_directory(journal_path: str) -> bool:
    """True for a journal of the profile's shared audit directory.

    Every unsaved-project (and project-storage) journal of a profile lives
    in <profile>/recoverland/audit/, so a file named after that directory
    alone (the legacy pending file) belongs to none of them in particular.
    A saved project's journal is alone in <project>/.recoverland/.
    """
    folder = os.path.dirname(os.path.abspath(plain(journal_path)))
    return (os.path.basename(folder).lower() == _AUDIT_SUBDIR
            and os.path.basename(os.path.dirname(folder)).lower() == _UNSAVED_SUBDIR)


def _resolve_journal_path(project_path: str,
                          profile_path: str,
                          unsaved_key: str = "") -> str:
    """Determine the journal file path based on project save state.

    `unsaved_key` is a filename discriminant, not a credential: it only
    separates the journals of two unsaved projects in the same profile.
    """
    if project_path and _isfile(project_path):
        return _journal_for_saved_project(project_path)
    return _journal_for_unsaved_project(profile_path, project_path, unsaved_key)


def _journal_for_saved_project(project_path: str) -> str:
    project_dir = os.path.dirname(os.path.abspath(project_path))
    journal_dir = os.path.join(project_dir, _JOURNAL_SUBDIR)
    return when_needed(os.path.join(journal_dir, _JOURNAL_FILENAME))


def _journal_for_unsaved_project(profile_path: str,
                                 hint: str,
                                 session_key: str = "") -> str:
    """Build the journal path of an unsaved project.

    `hint` (the project's would-be path) wins; `session_key` is the
    per-session fallback discriminant, and a fresh uuid the last resort.
    Neither is secret: both are hashed only to get a stable, filesystem
    -safe filename out of arbitrary text.
    """
    base_dir = os.path.join(profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR)
    if not hint:
        hint = session_key or uuid.uuid4().hex
    fingerprint = hashlib.sha256(hint.encode("utf-8")).hexdigest()[:16]
    filename = f"audit_{fingerprint}.sqlite"
    return when_needed(os.path.join(base_dir, filename))


def _journal_for_storage_project(profile_path: str, storage_uri: str) -> str:
    """Stable journal of a project kept in a project storage.

    Named apart from the session journals (storage_ prefix): it is the
    journal of a project that exists, never an orphan to clean up.
    """
    base_dir = os.path.join(profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR)
    key = _normalize_storage_key(storage_uri)
    fingerprint = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return when_needed(os.path.join(base_dir, f"{_STORAGE_PREFIX}{fingerprint}.sqlite"))


# ----------------------------------------------------------------------
# History carry
# ----------------------------------------------------------------------

_CARRY_BATCH = 2000
_IN_CHUNK = 500
_MARK_INTO = "carried_into"
_MARK_THROUGH = "carried_through_event_id"
_MARK_AT = "carried_at"

# Carries planned at a save and not carried out yet, per profile. Written
# inside QgsProject.write() (a few hundred bytes), removed once carried:
# what survives a crash between the save and the close of the session
# journal, so the saved project still gets its history at its next open.
# Every QGIS of the profile reads and rewrites it: see _owed_lock.
_OWED_FILENAME = "carries_owed.json"
_OWED_GUARD = threading.Lock()
_OWED_LOCK_SUFFIX = ".lock"
# How long an access waits for another QGIS of the profile. What it waits
# for is the read and rewrite of a few hundred bytes.
_OWED_LOCK_WAIT_SEC = 5.0


def _owed_path(profile_path: str) -> str:
    return when_needed(os.path.join(
        profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR, _OWED_FILENAME))


def _owed_key(source: str, target: str) -> Tuple[str, str]:
    return (path_key(source), path_key(target))


@contextmanager
def _owed_lock(profile_path: str):
    """Hold the owed carries of *profile_path* for one access, across processes.

    The thread lock alone let two QGIS of one profile read the same
    version of the file, and each write it back with its own change only:
    the obligation recorded by the first vanished without a word (and on
    Windows the rename of one failed while the other was reading). Under
    this lock every access re-reads the file and changes its own entry
    only (add, remove or update ONE key): a write never replaces what
    another process wrote in between. The lock is an OS lock on one byte
    of a side file, released by the OS when its process dies, so a crash
    never leaves the file locked. Raises TimeoutError (an OSError) when
    another process keeps it past _OWED_LOCK_WAIT_SEC. Where the file
    system offers no such lock, the thread lock alone applies, as before,
    and that is logged.
    """
    with _OWED_GUARD:
        lock_path = when_needed(_owed_path(profile_path) + _OWED_LOCK_SUFFIX)
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_BINARY", 0),
                     0o600)
        state = ""
        try:
            deadline = time.monotonic() + _OWED_LOCK_WAIT_SEC
            state = _os_lock(fd)
            while state == "busy" and time.monotonic() < deadline:
                time.sleep(0.01)
                state = _os_lock(fd)
            if state == "busy":
                raise TimeoutError(
                    f"{plain(lock_path)} held by another process for more "
                    f"than {_OWED_LOCK_WAIT_SEC:.0f} s")
            if state != "locked":
                flog(f"JournalManager: owed history carries not locked across "
                     f"processes ({state}): two QGIS of this profile writing "
                     f"at the same moment could lose one entry", "WARNING")
            yield
        finally:
            if state == "locked":
                _os_unlock(fd)
            os.close(fd)


# A file just written is often held for a moment by an antivirus or the
# search indexer: Windows then refuses to open or replace it (WinError 5;
# measured, 2 writes in 160 by four QGIS of one profile). Such a refusal
# is retried for about a second before it counts.
_OWED_SHARE_TRIES = 40 if os.name == "nt" else 1
_OWED_SHARE_PAUSE_SEC = 0.025


def _share_retry(action):
    for attempt in range(_OWED_SHARE_TRIES):
        try:
            return action()
        except PermissionError:
            if attempt + 1 >= _OWED_SHARE_TRIES:
                raise
            time.sleep(_OWED_SHARE_PAUSE_SEC)
    return None


def _read_owed(profile_path: str) -> List[dict]:
    """The owed carries, for an access under _owed_lock.

    [] only when there is no file. A file that cannot be opened raises
    (OSError): read as empty, it was written back by the next writer with
    that writer's entry alone, every other obligation gone. A file that is
    not a list of carries is set aside under its own name, and the user is
    told (_set_aside_owed); the registry then starts afresh.
    """
    path = _owed_path(profile_path)

    def read():
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    try:
        data = _share_retry(read)
    except FileNotFoundError:
        return []
    except ValueError as exc:
        _set_aside_owed(path, f"{type(exc).__name__}: {exc}")
        return []
    if not isinstance(data, list):
        _set_aside_owed(path, f"JSON root is {type(data).__name__}, not a list")
        return []
    return [e for e in data if isinstance(e, dict) and e.get("source") and e.get("target")]


def _load_owed(profile_path: str) -> List[dict]:
    """_read_owed for a reader: [] when the file cannot be read (logged)."""
    try:
        return _read_owed(profile_path)
    except OSError as exc:
        flog(f"JournalManager: owed history carries unreadable "
             f"({_owed_path(profile_path)}): {exc}", "WARNING")
        return []


def _set_aside_owed(path: str, why: str) -> None:
    """Keep an unreadable registry under another name, and say so.

    Raises OSError when it cannot be moved: the access that found it then
    fails instead of writing over it.
    """
    aside = when_needed(
        f"{plain(path)}.unreadable-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}")
    _share_retry(lambda: os.replace(path, aside))
    flog(f"JournalManager: owed history carries unreadable ({why}); kept as "
         f"{aside}, a new registry starts", "ERROR")
    qlog(f"RecoverLand : le registre des rattachements d'historique en attente "
         f"etait illisible ({why}). Il est conserve sous {plain(aside)} ; les "
         f"rattachements qu'il contenait ne seront pas relances "
         f"automatiquement.", "WARNING")


def _store_owed(profile_path: str, entries: List[dict]) -> None:
    path = _owed_path(profile_path)
    if not entries:
        try:
            _share_retry(lambda: os.remove(path))
        except FileNotFoundError:
            pass
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = when_needed(f"{path}.{os.getpid()}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(entries, fh, indent=1)
            fh.flush()
            os.fsync(fh.fileno())
        _share_retry(lambda: os.replace(tmp, path))
    except OSError:
        # The registry is untouched; its would-be replacement is not left
        # next to it.
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise


def _remember_owed(carry: HistoryCarry) -> bool:
    """Record *carry* durably; False (and a log line) when it cannot be."""
    if not carry.profile_path:
        return False
    key = _owed_key(carry.source, carry.target)
    try:
        with _owed_lock(carry.profile_path):
            entries = [e for e in _read_owed(carry.profile_path)
                       if _owed_key(e["source"], e["target"]) != key]
            entries.append(carry.as_dict())
            _store_owed(carry.profile_path, entries)
            return True
    except OSError as exc:
        flog(f"JournalManager: cannot record the history carry "
             f"{carry.source} -> {carry.target} ({exc}); it runs when the "
             f"journal closes, but a crash before that would leave the "
             f"history in the source only", "WARNING")
        return False


def _is_owed(carry: HistoryCarry) -> bool:
    """True when *carry* is recorded durably (a later open will retry it)."""
    if not carry.profile_path:
        return False
    key = _owed_key(carry.source, carry.target)
    try:
        with _owed_lock(carry.profile_path):
            return any(_owed_key(e["source"], e["target"]) == key
                       for e in _load_owed(carry.profile_path))
    except OSError as exc:
        # Unknown is reported as not recorded: the caller then says the
        # history may stay in the source, never that a retry is certain.
        flog(f"JournalManager: cannot read the owed history carries ({exc}); "
             f"{carry.source} -> {carry.target} reported as not recorded",
             "WARNING")
        return False


def _forget_owed(profile_path: str, source: str, target: str) -> None:
    if not profile_path:
        return
    key = _owed_key(source, target)
    try:
        with _owed_lock(profile_path):
            entries = _read_owed(profile_path)
            kept = [e for e in entries if _owed_key(e["source"], e["target"]) != key]
            if len(kept) != len(entries):
                _store_owed(profile_path, kept)
    except OSError as exc:
        # Harmless: a carry done twice inserts nothing (event_uid).
        flog(f"JournalManager: cannot clear the owed carry {source} -> "
             f"{target}: {exc}", "WARNING")


def _update_owed(profile_path: str, source: str, target: str, **fields) -> bool:
    """Change fields of one owed entry in place (False when not recorded)."""
    if not profile_path:
        return False
    key = _owed_key(source, target)
    try:
        with _owed_lock(profile_path):
            entries = _read_owed(profile_path)
            found = False
            for entry in entries:
                if _owed_key(entry["source"], entry["target"]) == key:
                    entry.update(fields)
                    found = True
            if found:
                _store_owed(profile_path, entries)
            return found
    except OSError as exc:
        flog(f"JournalManager: cannot update the owed carry {source} -> "
             f"{target} ({fields}): {exc}", "WARNING")
        return False


def _carry_into_journal(carry: HistoryCarry) -> HistoryCarryReport:
    """Carry *carry* into its target journal, opened for the time of it.

    The target is taken under its writer lock like any open: a project
    journal another instance is writing is not touched (the carry then
    stays owed). Every failure up to the carry itself is the target's.
    """
    report = HistoryCarryReport(carry.source, carry.target, carry.since_event_id)
    report.failure_side = "target"
    try:
        os.makedirs(os.path.dirname(carry.target), exist_ok=True)
        held = _acquire_lock(carry.target)
    except JournalLockError as exc:
        report.error = f"project journal held by another instance: {exc}"
        flog(f"JournalManager: history carry into {carry.target} refused: "
             f"{report.error}", "WARNING")
        return report
    except OSError as exc:
        report.error = f"{type(exc).__name__}: {exc}"
        return report
    conn = None
    try:
        conn = sqlite3.connect(carry.target, check_same_thread=False)
        _initialize_journal(conn)
    except Exception as exc:  # noqa: BLE001 - reported, the source is kept
        report.error = f"{type(exc).__name__}: {exc}"
        flog(f"JournalManager: history carry into {carry.target} failed: "
             f"{report.error}", "ERROR")
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        if held is not None:
            _release_lock(held)
        return report
    try:
        return carry_journal_history(
            carry.source, conn, carry.target,
            since_event_id=carry.since_event_id, opened_at=carry.opened_at,
            mark_source=carry.mark_source,
            until_event_id=carry.until_event_id, until_at=carry.until_at,
            until_uids=carry.until_uids)
    except Exception as exc:  # noqa: BLE001 - reported, the source is kept
        report.failure_side = ""
        report.error = f"{type(exc).__name__}: {exc}"
        flog(f"JournalManager: history carry into {carry.target} failed: "
             f"{report.error}", "ERROR")
        return report
    finally:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        if held is not None:
            _release_lock(held)


def _columns(conn: sqlite3.Connection, table: str) -> List[str]:
    try:
        return [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]  # nosec B608
    except sqlite3.Error:
        return []


def _chunks(values: list, size: int = _IN_CHUNK):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _copy_side_rows(src: sqlite3.Connection, dst: sqlite3.Connection,
                    table: str, key_col: str, keys: Optional[list],
                    conflict: str = "INSERT OR IGNORE") -> int:
    """Copy rows of *table* (all when keys is None) on the common columns."""
    src_cols = _columns(src, table)
    dst_cols = set(_columns(dst, table))
    cols = [c for c in src_cols if c in dst_cols]
    if not cols or key_col not in cols:
        return 0
    col_sql = ", ".join(cols)
    insert = (f"{conflict} INTO {table} ({col_sql}) "  # nosec B608: schema names
              f"VALUES ({','.join(['?'] * len(cols))})")
    before = dst.total_changes
    if keys is None:
        rows = src.execute(f"SELECT {col_sql} FROM {table}").fetchall()  # nosec B608
        dst.executemany(insert, rows)
    else:
        for chunk in _chunks(sorted(set(k for k in keys if k is not None))):
            rows = src.execute(
                f"SELECT {col_sql} FROM {table} WHERE {key_col} IN "  # nosec B608
                f"({','.join(['?'] * len(chunk))})", chunk).fetchall()
            dst.executemany(insert, rows)
    return dst.total_changes - before


def _select_carry_rows(src: sqlite3.Connection, select: str, cols: List[str],
                       since: int, until_event_id: Optional[int],
                       until_uids: Optional[List[str]],
                       extra_ids: Optional[List[int]] = None) -> list:
    """The rows a carry takes, in event_id order.

    Everything above *since*; with a bound, up to until_event_id plus the
    listed events of that session replayed later (until_uids); and the user
    events the traces among them compensate, wherever they are, as well as
    those *extra_ids* (compensated by traces carried from elsewhere: the
    outbox of a read-only source).
    """
    i_id = cols.index("event_id")
    i_ref = cols.index("restored_from_event_id")
    base = f"SELECT {select} FROM audit_event WHERE "  # nosec B608
    if until_event_id is None:
        rows = src.execute(base + "event_id > ?", (since,)).fetchall()
    else:
        rows = src.execute(base + "event_id > ? AND event_id <= ?",
                           (since, int(until_event_id))).fetchall()
        if until_uids and "event_uid" in set(_columns(src, "audit_event")):
            for chunk in _chunks(sorted(set(until_uids))):
                rows.extend(src.execute(
                    base + "event_id > ? AND event_uid IN "
                    f"({','.join(['?'] * len(chunk))})",
                    [int(until_event_id)] + chunk).fetchall())
    have = {r[i_id] for r in rows}
    refs = sorted(({r[i_ref] for r in rows if r[i_ref] is not None}
                   | {int(i) for i in extra_ids or ()}) - have)
    for chunk in _chunks(refs):
        rows.extend(src.execute(
            base + f"event_id IN ({','.join(['?'] * len(chunk))})", chunk).fetchall())
    rows.sort(key=lambda r: r[i_id])
    return rows


def _outbox_uids_since(journal_path: str,
                       opened_at: str) -> Optional[List[str]]:
    """event_uids waiting in the outbox of *journal_path*, captured since
    *opened_at* (the session that owes them). Read only.

    None when the outbox cannot be read: an outbox folder "not found" is
    only proof there is none where its parent folder can be listed (a share
    out of reach answers "not found" for everything it holds).
    """
    from .capture_outbox import list_segments, _parse_segment, outbox_dir_for
    if not opened_at:
        return []
    outbox = outbox_dir_for(journal_path)
    try:
        os.listdir(outbox)
    except FileNotFoundError:
        if _provably_absent(outbox):
            return []
        flog(f"JournalManager: outbox of {journal_path} out of reach while "
             f"bounding an owed carry", "WARNING")
        return None
    except OSError as exc:
        flog(f"JournalManager: outbox of {journal_path} unreadable while "
             f"bounding an owed carry ({exc})", "WARNING")
        return None
    uids = []
    try:
        for _seq, path in list_segments(outbox):
            lines, _torn = _parse_segment(path)
            for line in lines:
                rec = line.get("e") if line.get("t") == "ev" else None
                if (isinstance(rec, dict) and rec.get("event_uid")
                        and str(rec.get("created_at") or "") >= opened_at):
                    uids.append(str(rec["event_uid"]))
    except OSError as exc:
        flog(f"JournalManager: outbox of {journal_path} unreadable while "
             f"bounding an owed carry ({exc})", "WARNING")
        return None
    return uids


def _open_source(source_path: str, writable: bool) -> sqlite3.Connection:
    """The carry's connection to its source: read-only unless it takes writes."""
    if writable:
        conn = sqlite3.connect(source_path, check_same_thread=False)
        try:
            apply_pragmas(conn)
        except Exception:
            conn.close()
            raise
        return conn
    conn = sqlite3.connect(sqlite_readonly_uri(source_path), uri=True,
                           check_same_thread=False)
    try:
        apply_pragmas(conn, read_only=True)
    except sqlite3.Error as exc:
        # Settings of a connection that only reads: never worth the carry.
        flog(f"JournalManager: pragmas of the read-only source {source_path} "
             f"not applied: {exc}", "DEBUG")
    return conn


def _in_session(at: str, opened_at: str, until_at: str) -> bool:
    return bool(at and opened_at) and at >= opened_at and (not until_at or at < until_at)


class _SessionOutbox:
    """THIS session's lines in the outbox of a source (read only)."""

    def __init__(self):
        self.segments = 0
        # 'ev' records of this session, not in the quarantine.
        self.records: List[dict] = []
        # Their finalize lines, and this session's prepare lines; every
        # prepare of the outbox (a finalize takes its details from it) and
        # every commit id finalized there.
        self.finalizes: List[dict] = []
        self.prepares: Dict[str, dict] = {}
        self.all_prepares: Dict[str, dict] = {}
        self.finalized: set = set()
        # This session's batches whose end line is missing (torn write).
        self.incomplete: List[dict] = []
        self.quarantined = 0
        self.foreign_events = 0
        self.unreadable_lines = 0
        self.interrupted: List[str] = []

    def as_dict(self) -> dict:
        return {"segments": self.segments, "session_events": len(self.records),
                "session_finalized": len(self.finalizes),
                "session_prepares": len(self.prepares),
                "incomplete_batches": len(self.incomplete),
                "interrupted_commits": len(self.interrupted),
                "already_quarantined": self.quarantined,
                "foreign_events": self.foreign_events,
                "unreadable_lines": self.unreadable_lines}


def _session_outbox(outbox_dir: str, opened_at: str,
                    until_at: str) -> _SessionOutbox:
    """Read the outbox of a source; keep THIS session's lines.

    Called for what the source's journal did not take (read-only, or it
    refused the replay): what this session captured there (edits made while
    the archived project was open) belongs to the project saved from it and
    goes to ITS journal. Line by line: an event is this
    session's when its created_at lies in [opened_at, until_at); the
    finalize and end lines of its batch come with it, a prepare by its own
    time. The outbox used to be refused WHOLE as soon as one line came from
    another session, and the carry still ended "done": this session's edits
    never reached the project, and nothing retried.

    Nothing is moved or deleted: every line stays in the outbox, to be
    replayed into its own journal when that one takes writes again (the
    lines of this session end up in both journals, as when the source is
    writable). Raises OSError when a segment cannot be read: this session's
    edits may be in it, so the carry fails on the source side and stays owed.
    """
    from .capture_outbox import list_segments, _parse_segment, quarantined_uids
    box = _SessionOutbox()
    parsed = []
    for _seq, path in list_segments(outbox_dir):
        try:
            lines, torn = _parse_segment(path)
        except OSError as exc:
            raise OSError(f"outbox segment {plain(path)} unreadable, it may "
                          f"hold edits of this session ({exc})") from exc
        parsed.append(lines)
        box.unreadable_lines += len(torn)
    box.segments = len(parsed)
    in_quarantine = quarantined_uids(outbox_dir)
    batches: Dict[str, dict] = {}
    for lines in parsed:
        for line in lines:
            kind = line.get("t")
            if kind == "ev" and isinstance(line.get("e"), dict):
                rec = line["e"]
                if not _in_session(str(rec.get("created_at") or ""), opened_at, until_at):
                    box.foreign_events += 1
                    continue
                info = batches.setdefault(line.get("b"), {"events": 0, "end": None,
                                                          "commits": set()})
                info["events"] += 1
                if rec.get("commit_id"):
                    info["commits"].add(rec["commit_id"])
                if rec.get("event_uid") in in_quarantine:
                    box.quarantined += 1
                    continue
                box.records.append(rec)
            elif kind == "prepare":
                box.all_prepares[line.get("c", "")] = line
                if _in_session(str(line.get("at") or ""), opened_at, until_at):
                    box.prepares[line.get("c", "")] = line
            elif kind == "finalize":
                box.finalized.add(line.get("c", ""))
    for lines in parsed:
        for line in lines:
            kind = line.get("t")
            if kind == "finalize" and (
                    line.get("b") in batches
                    or _in_session(str(line.get("at") or ""), opened_at, until_at)):
                box.finalizes.append(line)
            elif kind == "end" and line.get("b") in batches:
                batches[line.get("b")]["end"] = line.get("n")
    box.incomplete = [{"batch": b, "events": info["events"],
                       "commits": sorted(info["commits"])}
                      for b, info in batches.items()
                      if info["end"] is None and info["events"]]
    return box


def _session_outbox_rows(box: _SessionOutbox,
                         cols: List[str]) -> Tuple[list, List[str]]:
    """This session's outbox events as carry rows (no source event_id), and
    why the journal refuses the others (the checks replay_outbox applies)."""
    from .capture_outbox import record_to_row_values
    from .journal_writes import EVENT_WRITE_COLUMNS, _row_tuple, validate_event_row
    rows, refused, seen = [], [], set()
    for rec in box.records:
        values = record_to_row_values(rec)
        why = validate_event_row(values)
        if not why and not values.get("event_uid"):
            why = "no event_uid"
        if why:
            refused.append(why)
            continue
        if values["event_uid"] in seen:
            continue
        seen.add(values["event_uid"])
        full = dict(zip(EVENT_WRITE_COLUMNS, _row_tuple(values)))
        rows.append(tuple(full.get(c) for c in cols))
    return rows, refused


def _record_session_commits(target_conn: sqlite3.Connection,
                            src: sqlite3.Connection, box: _SessionOutbox) -> None:
    """The commit ledger of this session's outbox lines, in the target.

    The same records replay_outbox writes for them: how each commit ended
    (finalize), the batches a torn write left without their end
    (INCOMPLETE), and the prepared commits nothing resolved (INTERRUPTED,
    their pre-edit state kept). Runs in the carry's transaction.
    """
    from .capture_outbox import _commit_recorded, _preserved_state
    from .journal_writes import record_capture_commit
    for line in box.finalizes:
        cid = line.get("c", "")
        prep = box.all_prepares.get(cid, {})
        meta = line.get("meta") or {}
        record_capture_commit(
            target_conn, cid, line.get("state", "COMMITTED"),
            layer_id=prep.get("layer_id"), layer_name=prep.get("layer_name"),
            datasource_fingerprint=prep.get("ds"),
            session_id=prep.get("session_id"), prepared_at=prep.get("at"),
            finalized_at=line.get("at"),
            n_snapshots=len(prep.get("snapshots") or []),
            n_events=meta.get("n_events"),
            detail=meta.get("detail") or "carried from the outbox of a read-only journal",
            snapshots_json=(json.dumps(_preserved_state(prep), ensure_ascii=True)
                            if line.get("state") == "GENERATION_FAILED" else None))
    for inc in box.incomplete:
        for cid in inc["commits"]:
            if cid in box.all_prepares:
                continue
            record_capture_commit(
                target_conn, cid, "INCOMPLETE", finalized_at=None,
                n_events=inc["events"],
                detail=(f"torn write: {inc['events']} event(s) of the batch "
                        f"recovered, its end marker is missing; the commit may "
                        f"lack events"))
    for cid, prep in box.prepares.items():
        if (cid in box.finalized or _commit_recorded(src, cid)
                or _commit_recorded(target_conn, cid)):
            continue
        record_capture_commit(
            target_conn, cid, "INTERRUPTED",
            layer_id=prep.get("layer_id"), layer_name=prep.get("layer_name"),
            datasource_fingerprint=prep.get("ds"),
            session_id=prep.get("session_id"), prepared_at=prep.get("at"),
            finalized_at=None, n_snapshots=len(prep.get("snapshots") or []),
            n_events=0,
            detail=("QGIS stopped between the pre-commit capture and the end of "
                    "the provider commit: the pre-edit state of these features "
                    "is kept, whether the provider applied the edit is unknown"),
            snapshots_json=json.dumps(_preserved_state(prep), ensure_ascii=True))
        box.interrupted.append(cid)


def _journal_stamp(conn: sqlite3.Connection) -> str:
    """Creation time of a journal (its first schema record), '' if unknown."""
    try:
        row = conn.execute("SELECT MIN(applied_at) FROM schema_version").fetchone()
        return str(row[0] or "") if row else ""
    except sqlite3.Error:
        return ""


_LEGACY_UID_PARTS = ("event_id", "created_at", "datasource_fingerprint",
                     "feature_identity_json", "operation_type", "attributes_json")


def _legacy_uid(stamp: str, row: tuple, cols: List[str]) -> str:
    """Identity of a row written before event_uid existed.

    Deterministic (journal creation time and the row's own content), so a
    read-only source gives the same identity at every carry without being
    written, and a writable one records that same identity: carried twice,
    from either state, the row is found and not added again.
    """
    parts = [stamp] + [repr(row[cols.index(c)]) for c in _LEGACY_UID_PARTS]
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return "carry-" + digest[:32]


def _count_out_of_order(target_conn: sqlite3.Connection, rows: list,
                        cols: List[str]) -> int:
    """Entities whose carried changes predate a change the target already has.

    Carried rows get event_ids after every row of the target, and the
    history of an entity is read in event_id order: a carried change older
    than one the target holds (another instance wrote the same entity in
    between) would be replayed after it. Counted and reported, never
    merged in silence.
    """
    i_ds = cols.index("datasource_fingerprint")
    i_ent = cols.index("entity_fingerprint")
    i_at = cols.index("created_at")
    first: Dict[Tuple[str, str], str] = {}
    for r in rows:
        if not r[i_ent]:
            continue
        key = (r[i_ds], r[i_ent])
        at = str(r[i_at] or "")
        if key not in first or at < first[key]:
            first[key] = at
    if not first or target_conn.execute(
            "SELECT 1 FROM audit_event LIMIT 1").fetchone() is None:
        return 0
    # One set-based query, not one per entity: a session can touch tens of
    # thousands of entities and the carry runs as its journal closes.
    target_conn.execute("CREATE TEMP TABLE IF NOT EXISTS rl_carry_first "
                        "(ds TEXT, ent TEXT, at TEXT)")
    try:
        target_conn.execute("DELETE FROM rl_carry_first")
        target_conn.executemany(
            "INSERT INTO rl_carry_first (ds, ent, at) VALUES (?, ?, ?)",
            [(ds, ent, at) for (ds, ent), at in first.items()])
        row = target_conn.execute(
            "SELECT COUNT(*) FROM rl_carry_first f WHERE EXISTS ("
            "SELECT 1 FROM audit_event e WHERE e.datasource_fingerprint = f.ds "
            "AND e.entity_fingerprint = f.ent AND e.created_at > f.at)").fetchone()
        return int(row[0] or 0)
    finally:
        target_conn.execute("DELETE FROM rl_carry_first")


def carry_journal_history(source_path: str, target_conn: sqlite3.Connection,
                          target_path: str, since_event_id: int = 0,
                          opened_at: str = "", mark_source: bool = True,
                          trace_id: str = "",
                          until_event_id: Optional[int] = None,
                          until_at: str = "",
                          until_uids: Optional[List[str]] = None) -> HistoryCarryReport:
    """Copy the history of *source_path* into the journal of *target_conn*.

    Idempotent, whatever the source holds:
      * v7 rows are keyed by their event_uid (ON CONFLICT DO NOTHING);
      * legacy rows (no event_uid) get a deterministic one (_legacy_uid),
        recorded IN THE SOURCE when it takes writes, so a second carry finds
        them under the same identity instead of adding them again;
      * restored_from_event_id is rewritten to the target's event_id of the
        same event (by event_uid): copied as is it would designate an
        unrelated event of the target, and the next rewind would compensate
        a compensation. A trace whose source event exists nowhere stays an
        orphan (negative reference, matching no row).
    Rows written since *since_event_id* are carried (up to until_event_id
    and the listed until_uids, when the carry is bounded), plus the user
    events those traces compensate. The source's outbox leftovers are replayed into
    the source first, so nothing accepted is left behind. Side tables follow
    for the carried events: commit ledger, datasource registry and aliases,
    sessions, the purge horizon (merged on the latest date: the target
    must not claim a completeness the source lost to a purge) and the
    intervals of the logical purges, for the whole alias family of the
    carried sources (_carry_coverage).

    A source that does not take writes (archived project, share mounted
    read-only) is only READ: nothing is written to it, and the carry still
    goes through. It used to write to the source before reading anything,
    so it failed even with nothing to carry, and every later retry failed
    the same way. The events THIS session left in the source's outbox (its
    journal is read-only, or refused them) are carried from there, line by
    line (_session_outbox); every line stays in that outbox. A segment that
    cannot be read fails the carry on the source side: it stays owed.

    A source that is not found is "nothing to carry" only when it is
    provably gone (_provably_absent); a share out of reach answers "not
    found" too, and the carry then fails on the source side and stays owed.

    All target writes are one transaction, verified before commit. The
    source is only marked (backend_settings) when everything was carried,
    which is what lets the orphan cleanup remove it later. report.failure_side
    says which journal a failure came from.
    """
    from .journal_writes import ensure_v7_schema
    from .capture_outbox import replay_outbox, outbox_dir_for, list_segments
    from .sqlite_schema import AUDIT_EVENT_COLUMNS

    prefix = f"[{trace_id}] " if trace_id else ""
    since = int(since_event_id or 0)
    report = HistoryCarryReport(source_path, target_path, since)
    if _same_file(source_path, target_path):
        report.ok = True
        return report
    if not _isfile(source_path):
        if _provably_absent(source_path):
            report.ok = True
            report.source_missing = True
            report.warnings.append("source journal not found, nothing to carry")
            flog(f"{prefix}JournalManager: carry from {source_path}: source "
                 f"journal no longer exists, nothing to carry", "WARNING")
            return report
        # Not found is not gone: a share that dropped answers the same. The
        # history may be intact there; the obligation waits for it.
        report.failure_side = "source"
        report.error = "source journal unreachable"
        flog(f"{prefix}JournalManager: carry from {source_path} not possible "
             f"now: the source journal is unreachable (not provably gone); "
             f"the history carry stays owed", "ERROR")
        return report
    try:
        src_held = _acquire_lock(source_path)
    except JournalLockError as exc:
        report.failure_side = "source"
        report.error = f"source journal held by another instance: {exc}"
        flog(f"{prefix}JournalManager: carry refused: {report.error}", "ERROR")
        return report
    src = None
    in_target_tx = False
    side = "source"
    try:
        writable = _journal_file_writable(source_path)
        src = _open_source(source_path, writable)
        if writable:
            try:
                with src:
                    ensure_v7_schema(src)
            except sqlite3.Error as exc:
                if not _is_readonly_error(exc):
                    raise
                writable = False
                src.close()
                src = _open_source(source_path, False)
        report.source_read_only = not writable
        outbox_dir = outbox_dir_for(source_path)
        src_cols = set(_columns(src, "audit_event"))
        cols = list(AUDIT_EVENT_COLUMNS) + ["event_uid", "commit_id"]
        i_id = cols.index("event_id")
        i_ref = cols.index("restored_from_event_id")
        i_uid = cols.index("event_uid")
        i_cid = cols.index("commit_id")
        i_ds = cols.index("datasource_fingerprint")
        i_sess = cols.index("session_id")
        box = None
        box_rows: list = []
        if writable:
            replay = replay_outbox(source_path, src, trace_id=trace_id)
            report.outbox = replay.as_dict()
        if list_segments(outbox_dir):
            # What the source's own journal did not take (a read-only file,
            # a share read-only by its ACL alone, a write it refused): this
            # session's lines are carried from the outbox itself.
            box = _session_outbox(outbox_dir, opened_at, until_at)
            report.outbox = dict(report.outbox or {}, session_outbox=box.as_dict())
            report.outbox_foreign_events = box.foreign_events
            report.outbox_unreadable_lines = box.unreadable_lines
            box_rows, refused = _session_outbox_rows(box, cols)
            if refused:
                report.session_outbox_left = len(refused)
                report.session_outbox_left_why = ", ".join(sorted(set(refused)))
                report.warnings.append(
                    f"{len(refused)} event(s) of this session in the outbox of "
                    f"the source refused ({report.session_outbox_left_why}): "
                    f"left there, not in the project's history")
            if box.foreign_events or box.unreadable_lines:
                report.warnings.append(
                    f"outbox of the source: {box.foreign_events} event(s) of "
                    f"other sessions and {box.unreadable_lines} unreadable "
                    f"line(s) left in place")
        report.outbox_segments_left = len(list_segments(outbox_dir))
        if report.outbox_segments_left and not writable:
            report.warnings.append(
                f"source journal read-only: {report.outbox_segments_left} "
                f"outbox segment(s) left in {plain(outbox_dir)}")
        report.through_event_id = _max_event_id(src)

        select = ", ".join(c if c in src_cols else f"NULL AS {c}" for c in cols)
        rows = _select_carry_rows(
            src, select, cols, since, until_event_id, until_uids,
            extra_ids=[r[i_ref] for r in box_rows if r[i_ref] is not None])
        report.events_seen = len(rows) + len(box_rows)

        legacy = [r for r in rows if r[i_uid] is None]
        if legacy:
            stamp = _journal_stamp(src)
            derived = {r[i_id]: _legacy_uid(stamp, r, cols) for r in legacy}
            if writable and "event_uid" in src_cols:
                try:
                    with src:
                        src.executemany(
                            "UPDATE audit_event SET event_uid = ? "
                            "WHERE event_id = ? AND event_uid IS NULL",
                            [(uid, eid) for eid, uid in derived.items()])
                except sqlite3.Error as exc:
                    if not _is_readonly_error(exc):
                        raise
                    writable = False
                    report.source_read_only = True
            rows = [r if r[i_uid] is not None else
                    r[:i_uid] + (derived[r[i_id]],) + r[i_uid + 1:]
                    for r in rows]
            report.legacy_identified = len(legacy)
        uid_by_src_id = {r[i_id]: r[i_uid] for r in rows}
        # This session's outbox events come after the journal's rows (as a
        # replay into the source would put them); one that also reached the
        # journal is carried once, from there. Their restore traces refer
        # to source event_ids like any row, and are remapped below.
        in_rows = set(uid_by_src_id.values())
        box_rows = [r for r in box_rows if r[i_uid] not in in_rows]
        report.session_outbox_carried = len(box_rows)
        rows = rows + box_rows
        uids = [r[i_uid] for r in rows]

        insert_cols = [c for c in cols if c != "event_id"]
        insert_sql = (
            f"INSERT INTO audit_event ({', '.join(insert_cols)}) "  # nosec B608
            f"VALUES ({','.join(['?'] * len(insert_cols))}) "
            f"ON CONFLICT(event_uid) DO NOTHING")

        side = "target"
        target_conn.execute("BEGIN IMMEDIATE")
        in_target_tx = True
        ensure_v7_schema(target_conn)
        already = set()
        for chunk in _chunks(uids):
            already.update(r[0] for r in target_conn.execute(
                "SELECT event_uid FROM audit_event WHERE event_uid IN "  # nosec B608
                f"({','.join(['?'] * len(chunk))})", chunk))
        new_rows = [r for r in rows if r[i_uid] not in already]
        report.out_of_order_entities = _count_out_of_order(
            target_conn, new_rows, cols)
        for start in range(0, len(new_rows), _CARRY_BATCH):
            batch = []
            for r in new_rows[start:start + _CARRY_BATCH]:
                values = list(r)
                if values[i_ref] is not None:
                    # Orphan until proven otherwise: matches no event_id.
                    values[i_ref] = -abs(int(values[i_ref]))
                del values[i_id]
                batch.append(values)
            target_conn.executemany(insert_sql, batch)
        report.events_inserted = len(new_rows)
        report.events_already_present = len(rows) - len(new_rows)

        dst_id_by_uid = {}
        for chunk in _chunks(uids):
            for uid, eid in target_conn.execute(
                    "SELECT event_uid, event_id FROM audit_event WHERE "  # nosec B608
                    f"event_uid IN ({','.join(['?'] * len(chunk))})", chunk):
                dst_id_by_uid[uid] = eid
        if len(dst_id_by_uid) != len(set(uids)):
            raise RuntimeError(
                f"verification failed: {len(set(uids)) - len(dst_id_by_uid)} "
                f"carried event(s) missing from the target")
        fixes = []
        for r in new_rows:
            ref = r[i_ref]
            if ref is None:
                continue
            dst_ref = dst_id_by_uid.get(uid_by_src_id.get(ref))
            if dst_ref is None:
                report.orphan_traces += 1
                continue
            fixes.append((dst_ref, r[i_uid]))
        target_conn.executemany(
            "UPDATE audit_event SET restored_from_event_id = ? WHERE event_uid = ?",
            fixes)
        report.traces_remapped = len(fixes)
        if box is not None:
            _record_session_commits(target_conn, src, box)
            if box.interrupted:
                report.warnings.append(
                    f"{len(box.interrupted)} interrupted commit(s) of this "
                    f"session recorded from the outbox of the read-only source")

        full = since == 0 and until_event_id is None
        fps = None if full else [r[i_ds] for r in rows]
        commit_ids = [r[i_cid] for r in rows if r[i_cid]]
        if full:
            report.commits_copied = _copy_side_rows(
                src, target_conn, "capture_commit", "commit_id", None)
        else:
            report.commits_copied = _copy_side_rows(
                src, target_conn, "capture_commit", "commit_id", commit_ids)
            if opened_at and _columns(src, "capture_commit"):
                extra_sql = "SELECT commit_id FROM capture_commit WHERE recorded_at >= ?"
                extra_params = [opened_at]
                if until_at:
                    extra_sql += " AND recorded_at < ?"
                    extra_params.append(until_at)
                extra = [r[0] for r in src.execute(extra_sql, extra_params)]
                report.commits_copied += _copy_side_rows(
                    src, target_conn, "capture_commit", "commit_id", extra)
        _copy_side_rows(src, target_conn, "datasource_registry",
                        "datasource_fingerprint", fps)
        _copy_side_rows(src, target_conn, "audit_session", "session_id",
                        None if full else [r[i_sess] for r in rows])
        family = _alias_family_scope(src, fps)
        report.alias_conflicts = _carry_aliases(src, target_conn, family)
        carried_fps = sorted({r[i_ds] for r in rows if r[i_ds]})
        _carry_coverage(src, target_conn, family, carried_fps=carried_fps,
                        since_event_id=since)
        target_conn.commit()
        in_target_tx = False
        report.ok = True
        side = "source"

        if mark_source and not report.outbox_segments_left:
            if not writable:
                report.warnings.append("source journal read-only: not marked "
                                       "as carried (the orphan cleanup keeps it)")
            else:
                through = max((r[i_id] for r in rows if r[i_id] is not None),
                              default=0)
                try:
                    with src:
                        for key, value in ((_MARK_INTO, target_path),
                                           (_MARK_THROUGH, str(through)),
                                           (_MARK_AT, _utc_now())):
                            src.execute(
                                "INSERT OR REPLACE INTO backend_settings "
                                "(setting_key, setting_value, updated_at) "
                                "VALUES (?, ?, ?)",
                                (key, value, _utc_now()))
                    report.source_marked = True
                except sqlite3.Error as exc:
                    # Everything is in the target; an unmarked source is
                    # only kept (and reported) by the orphan cleanup.
                    report.warnings.append(f"source not marked as carried: {exc}")
                    flog(f"{prefix}JournalManager: {source_path} not marked as "
                         f"carried ({exc}); the orphan cleanup keeps it",
                         "WARNING")
    except Exception as exc:  # noqa: BLE001 - a carry never loses the source
        if in_target_tx:
            try:
                target_conn.rollback()
            except sqlite3.Error:
                pass
        report.ok = False
        report.failure_side = side
        report.error = f"{type(exc).__name__}: {exc}"
        flog(f"{prefix}JournalManager: history carry {source_path} -> "
             f"{target_path} failed ({side} side), nothing written to the "
             f"target: {report.error}", "ERROR")
    finally:
        if src is not None:
            try:
                src.close()
            except sqlite3.Error:
                pass
        if src_held is not None:
            _release_lock(src_held)
    if report.ok:
        flog(f"{prefix}JournalManager: event=history_carried source={source_path} "
             f"target={target_path} since_event_id={since} "
             f"until_event_id={until_event_id} "
             f"until_uids={len(until_uids) if until_uids else 0} "
             f"seen={report.events_seen} inserted={report.events_inserted} "
             f"already={report.events_already_present} "
             f"legacy_identified={report.legacy_identified} "
             f"traces_remapped={report.traces_remapped} "
             f"orphan_traces={report.orphan_traces} "
             f"commits={report.commits_copied} "
             f"outbox_left={report.outbox_segments_left} "
             f"alias_conflicts={report.alias_conflicts} "
             f"out_of_order_entities={report.out_of_order_entities} "
             f"source_read_only={report.source_read_only} "
             f"source_marked={report.source_marked}",
             "WARNING" if (report.outbox_segments_left or report.alias_conflicts
                           or report.out_of_order_entities)
             else "INFO")
    return report


def _carry_aliases(src: sqlite3.Connection, dst: sqlite3.Connection,
                   fps: Optional[list]) -> int:
    """Copy the aliases the carried events rely on; count the conflicts."""
    cols = [c for c in _columns(src, "datasource_alias")
            if c in set(_columns(dst, "datasource_alias"))]
    if "alias_fingerprint" not in cols or "target_fingerprint" not in cols:
        return 0
    col_sql = ", ".join(cols)
    if fps is None:
        rows = src.execute(f"SELECT {col_sql} FROM datasource_alias").fetchall()  # nosec B608
    else:
        rows = []
        for chunk in _chunks(sorted(set(f for f in fps if f))):
            marks = ",".join(["?"] * len(chunk))
            rows.extend(src.execute(
                f"SELECT {col_sql} FROM datasource_alias WHERE "  # nosec B608
                f"alias_fingerprint IN ({marks}) OR target_fingerprint IN ({marks})",
                chunk + chunk).fetchall())
    i_alias = cols.index("alias_fingerprint")
    i_target = cols.index("target_fingerprint")
    conflicts = 0
    for row in rows:
        existing = dst.execute(
            "SELECT target_fingerprint FROM datasource_alias WHERE alias_fingerprint = ?",
            (row[i_alias],)).fetchone()
        if existing is not None:
            if existing[0] != row[i_target]:
                conflicts += 1
                flog(f"JournalManager: alias {row[i_alias][:60]} points to "
                     f"another source in the target journal; the target's "
                     f"alias is kept", "WARNING")
            continue
        dst.execute(
            f"INSERT OR IGNORE INTO datasource_alias ({col_sql}) "  # nosec B608
            f"VALUES ({','.join(['?'] * len(cols))})", row)
    return conflicts


def _alias_family_scope(src: sqlite3.Connection,
                        fps: Optional[list]) -> Optional[list]:
    """The carried fingerprints and their whole alias family in the source.

    None (a full carry) stays None: everything follows. A purge records
    its horizon and its intervals under each member its family had THEN:
    a record written before an alias existed is held by the older
    fingerprint alone, and a carry filtered on the fingerprints of the
    carried events left it behind (the target then accepted a rewind into
    that purged history). The alias rows of the family follow too, so the
    target resolves the same family (retention.purged_gaps expands it).
    """
    if fps is None:
        return None
    scope = set(f for f in fps if f)
    if not scope or not _columns(src, "datasource_alias"):
        return sorted(scope)
    from .datasource_alias import expand_fingerprints
    for fingerprint in list(scope):
        scope.update(expand_fingerprints(src, fingerprint))
    return sorted(scope)


def _rows_of_scope(src: sqlite3.Connection, select: str,
                   fps: Optional[list]) -> list:
    """Rows of *select* (no WHERE) for the datasource fingerprints *fps*."""
    if fps is None:
        return src.execute(select).fetchall()
    rows = []
    for chunk in _chunks(sorted(set(f for f in fps if f))):
        rows.extend(src.execute(
            f"{select} WHERE datasource_fingerprint IN "  # nosec B608
            f"({','.join(['?'] * len(chunk))})", chunk).fetchall())
    return rows


def _carry_coverage(src: sqlite3.Connection, dst: sqlite3.Connection,
                    fps: Optional[list],
                    carried_fps: Optional[list] = None,
                    since_event_id: int = 0) -> None:
    """Carry what the source's purges made unrebuildable. In the caller's tx."""
    from .retention import (
        _GAP_DDL, _journal_predates_v7, has_unvouched_floor,
        record_unvouched_floor,
    )
    if _columns(src, "journal_coverage") and _columns(dst, "journal_coverage"):
        rows = _rows_of_scope(
            src, "SELECT datasource_fingerprint, purged_through, purged_events, "
                 "updated_at FROM journal_coverage", fps)
        dst.executemany(
            "INSERT INTO journal_coverage (datasource_fingerprint, purged_through, "
            "purged_events, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(datasource_fingerprint) DO UPDATE SET "
            "purged_through = MAX(journal_coverage.purged_through, excluded.purged_through), "
            "purged_events = MAX(journal_coverage.purged_events, excluded.purged_events), "
            "updated_at = excluded.updated_at", rows)
    if _columns(src, "journal_coverage_gap"):
        gaps = _rows_of_scope(
            src, "SELECT datasource_fingerprint, gap_from, gap_through, kind, "
                 "recorded_at FROM journal_coverage_gap", fps)
        if gaps:
            dst.execute(_GAP_DDL)
            before = dst.total_changes
            dst.executemany(
                "INSERT OR IGNORE INTO journal_coverage_gap (datasource_fingerprint, "
                "gap_from, gap_through, kind, recorded_at) VALUES (?, ?, ?, ?, ?)", gaps)
            added = dst.total_changes - before
            flog(f"JournalManager: purged intervals carried: {added} added, "
                 f"{len(gaps) - added} already present, for "
                 f"{len(set(g[0] for g in gaps))} datasource fingerprint(s)")
    candidates = carried_fps if carried_fps is not None else (fps or [])
    if not candidates:
        return
    src_predates = _journal_predates_v7(src)
    unvouched = []
    for fp in candidates:
        if src_predates or has_unvouched_floor(src, fp):
            unvouched.append(fp)
            continue
        if since_event_id > 0:
            fam = _alias_family_scope(src, [fp]) or [fp]
            marks = ",".join("?" for _ in fam)
            left_behind = src.execute(
                f"SELECT 1 FROM audit_event WHERE event_id <= ? "  # nosec B608
                f"AND datasource_fingerprint IN ({marks}) LIMIT 1",
                [int(since_event_id)] + list(fam),
            ).fetchone()
            if left_behind is not None:
                unvouched.append(fp)
    if unvouched:
        record_unvouched_floor(dst, unvouched)


# ----------------------------------------------------------------------
# Orphan journals
# ----------------------------------------------------------------------

_ORPHAN_MAX_AGE_DAYS = 30

# Orphans already announced in this QGIS session (one message per file,
# not one per project opened).
_ORPHANS_REPORTED: set = set()


def _orphan_content(path: str) -> dict:
    """What an orphan journal still holds (never raises)."""
    out = {"events": -1, "commits": 0, "max_event_id": 0, "carried_through": None,
           "carried_into": "", "outbox_files": 0, "pending_file": False,
           "error": ""}
    try:
        from .capture_outbox import outbox_dir_for
        outbox = outbox_dir_for(path)
        if os.path.isdir(outbox):
            out["outbox_files"] = sum(len(files) for _d, _s, files in os.walk(outbox))
    except Exception as exc:  # noqa: BLE001 - an unknown outbox is kept
        out["outbox_files"] = 1
        out["error"] = f"outbox: {exc}"
    try:
        # The legacy pending file of this journal (named after it in the
        # shared audit directory): the last copy of edits, never deleted.
        from .integrity import _get_pending_path
        out["pending_file"] = _isfile(_get_pending_path(path))
    except Exception as exc:  # noqa: BLE001 - an unknown pending file is kept
        out["pending_file"] = True
        out["error"] = f"pending: {exc}"
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(path), uri=True)
        try:
            row = conn.execute(
                "SELECT COUNT(*), COALESCE(MAX(event_id), 0) FROM audit_event").fetchone()
            out["events"] = int(row[0])
            out["max_event_id"] = int(row[1])
            if _columns(conn, "capture_commit"):
                out["commits"] = int(conn.execute(
                    "SELECT COUNT(*) FROM capture_commit").fetchone()[0])
            if _columns(conn, "backend_settings"):
                marks = dict(conn.execute(
                    "SELECT setting_key, setting_value FROM backend_settings "
                    "WHERE setting_key IN (?, ?)", (_MARK_INTO, _MARK_THROUGH)))
                if marks.get(_MARK_THROUGH) is not None:
                    out["carried_through"] = int(marks[_MARK_THROUGH])
                out["carried_into"] = marks.get(_MARK_INTO) or ""
        finally:
            conn.close()
    except (sqlite3.Error, ValueError) as exc:
        out["events"] = -1
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _orphan_verdict(content: dict) -> str:
    """'empty', 'carried' (safe to remove) or 'holds' (keep and report)."""
    if (content["events"] < 0 or content["outbox_files"]
            or content.get("pending_file")):
        return "holds"
    if content["events"] == 0 and content["commits"] == 0:
        return "empty"
    through = content["carried_through"]
    if (through is not None and content["max_event_id"] <= through
            and content["carried_into"] and _isfile(content["carried_into"])):
        return "carried"
    return "holds"


def _remove_journal_files(full: str) -> None:
    os.remove(full)
    for suffix in ("-wal", "-shm", "-journal", _LOCK_SUFFIX):
        sidecar = full + suffix
        if os.path.exists(sidecar):
            os.remove(sidecar)
    # An outbox directory is only ever removed EMPTY (checked by the
    # verdict): rmdir refuses anything else.
    try:
        from .capture_outbox import outbox_dir_for, QUARANTINE_DIRNAME
        outbox = outbox_dir_for(full)
        for directory in (os.path.join(outbox, QUARANTINE_DIRNAME), outbox):
            if os.path.isdir(directory):
                os.rmdir(directory)
    except (OSError, ImportError) as exc:
        flog(f"JournalManager: empty outbox of {full} left in place: {exc}",
             "DEBUG")


def cleanup_orphan_journals(profile_path: str,
                            max_age_days: int = _ORPHAN_MAX_AGE_DAYS,
                            current_path: str = "") -> int:
    """Remove orphan journals of the unsaved-project audit directory.

    Only journals older than max_age_days that are not the current one,
    not held by a live instance, and whose content is proven to be
    unneeded: empty (no event, no commit, nothing in the outbox), or
    entirely carried into a project journal that still exists. A journal
    that still holds events -- or cannot be read to prove otherwise -- is
    KEPT, and the user is told where it is and what it holds: it is the
    only copy of what a session of an unsaved project captured. Journals
    of stored projects (storage_ prefix) are never orphans.
    Returns the number of files removed.
    """
    audit_dir = when_needed(os.path.join(profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR))
    if not os.path.isdir(audit_dir):
        return 0
    cutoff = time.time() - (max_age_days * 86400)
    removed = 0
    kept = []
    current_norm = path_key(current_path) if current_path else ""
    for filename in sorted(os.listdir(audit_dir)):
        if not filename.endswith(".sqlite") or filename.startswith(_STORAGE_PREFIX):
            continue
        full = os.path.join(audit_dir, filename)
        if current_norm and path_key(full) == current_norm:
            continue
        try:
            if os.path.getmtime(full) >= cutoff:
                continue
        except OSError as e:
            flog(f"JournalManager: cannot stat orphan {filename}: {e}", "WARNING")
            continue
        if writer_lock_holder(full) is not None:
            continue
        content = _orphan_content(full)
        verdict = _orphan_verdict(content)
        if verdict == "holds":
            kept.append((filename, content))
            flog(f"JournalManager: orphan journal kept (history no project "
                 f"opens) path={full} events={content['events']} "
                 f"commits={content['commits']} "
                 f"outbox_files={content['outbox_files']} "
                 f"pending_file={content['pending_file']} "
                 f"error={content['error']!r}", "WARNING")
            continue
        try:
            _remove_journal_files(full)
            removed += 1
            detail = (f", its events live in {content['carried_into']}"
                      if verdict == "carried" else "")
            flog(f"JournalManager: removed orphan {filename} ({verdict}{detail})")
        except OSError as e:
            flog(f"JournalManager: cannot remove orphan {filename}: {e}", "WARNING")
    if removed:
        flog(f"JournalManager: cleaned {removed} orphan journal(s) from {audit_dir}")
    _report_kept_orphans(audit_dir, kept)
    return removed


def _report_kept_orphans(audit_dir: str, kept: list) -> None:
    fresh = [(name, c) for name, c in kept
             if os.path.join(audit_dir, name) not in _ORPHANS_REPORTED]
    if not fresh:
        return
    parts = []
    for name, c in fresh:
        _ORPHANS_REPORTED.add(os.path.join(audit_dir, name))
        if c["events"] < 0:
            what = "illisible"
        else:
            what = f"{c['events']} evenement(s)"
        if c["outbox_files"]:
            what += f", {c['outbox_files']} fichier(s) en attente dans l'outbox"
        if c.get("pending_file"):
            what += ", un fichier d'evenements en attente"
        parts.append(f"{name} ({what})")
    qlog(
        f"RecoverLand : {len(fresh)} journal(aux) de projet(s) jamais "
        f"enregistre(s) contiennent encore un historique qu'aucun projet "
        f"n'ouvre : {'; '.join(parts)}. Ils sont conserves dans "
        f"{plain(audit_dir)} et ne seront pas supprimes automatiquement.",
        "WARNING",
    )


def get_journal_size_bytes(path: str) -> int:
    """Return the total disk footprint (main + WAL + SHM) in bytes."""
    total = 0
    for suffix in ("", "-wal", "-shm"):
        try:
            total += os.path.getsize(when_needed(path + suffix))
        except OSError:
            pass
    return total


def format_journal_size(size_bytes: int) -> str:
    """Human-readable journal size."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
