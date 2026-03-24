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
import sqlite3
from typing import Optional

from .sqlite_schema import initialize_schema, apply_pragmas, get_schema_version
from .logger import flog

_JOURNAL_FILENAME = "recoverland_audit.sqlite"
_JOURNAL_SUBDIR = ".recoverland"
_UNSAVED_SUBDIR = "recoverland"
_AUDIT_SUBDIR = "audit"


class JournalManager:
    """Manages the SQLite audit journal file and connections."""

    def __init__(self):
        self._conn: Optional[sqlite3.Connection] = None
        self._path: Optional[str] = None

    @property
    def path(self) -> Optional[str]:
        return self._path

    @property
    def is_open(self) -> bool:
        return self._conn is not None

    def open_for_project(self, project_path: str, profile_path: str) -> str:
        """Open or create the journal for the given project context.

        Returns the journal file path.
        Raises OSError if the directory cannot be created.
        Raises sqlite3.Error if the database cannot be opened.
        """
        self.close()
        journal_path = _resolve_journal_path(project_path, profile_path)
        self._ensure_directory(journal_path)
        self._open_connection(journal_path)
        self._path = journal_path
        flog(f"JournalManager: opened {journal_path}")
        return journal_path

    def get_connection(self) -> sqlite3.Connection:
        """Return the active connection. Raises if not open."""
        if self._conn is None:
            raise RuntimeError("Journal is not open")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
                flog(f"JournalManager: closed {self._path}")
            except sqlite3.Error as e:
                flog(f"JournalManager: close error: {e}", "WARNING")
            finally:
                self._conn = None
                self._path = None

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
        except Exception:
            conn.close()
            raise
        self._conn = conn

    @staticmethod
    def _ensure_directory(path: str) -> None:
        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)


def _resolve_journal_path(project_path: str, profile_path: str) -> str:
    """Determine the journal file path based on project save state."""
    if project_path and os.path.isfile(project_path):
        return _journal_for_saved_project(project_path)
    return _journal_for_unsaved_project(profile_path, project_path)


def _journal_for_saved_project(project_path: str) -> str:
    project_dir = os.path.dirname(os.path.abspath(project_path))
    journal_dir = os.path.join(project_dir, _JOURNAL_SUBDIR)
    return os.path.join(journal_dir, _JOURNAL_FILENAME)


def _journal_for_unsaved_project(profile_path: str, hint: str) -> str:
    base_dir = os.path.join(profile_path, _UNSAVED_SUBDIR, _AUDIT_SUBDIR)
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
