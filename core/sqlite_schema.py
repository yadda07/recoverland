# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""SQLite schema definition and initialization for RecoverLand (RLU-010, RLU-012).

Provides DDL statements and idempotent schema creation.
All PRAGMAs are applied at every connection open.
"""
import os
import sqlite3
import threading
from typing import List, Optional, Tuple

from .logger import flog
from .long_paths import path_key, when_needed

CURRENT_SCHEMA_VERSION = 7

# Authoritative column order for the audit_event table. Every SELECT/INSERT
# site (search_service, event_stream_repository, write_queue, integrity)
# MUST consume these constants instead of redeclaring the column list, so
# that adding/removing a column stays a one-line change.
AUDIT_EVENT_COLUMNS = (
    "event_id", "project_fingerprint", "datasource_fingerprint",
    "layer_id_snapshot", "layer_name_snapshot", "provider_type",
    "feature_identity_json", "operation_type", "attributes_json",
    "geometry_wkb", "geometry_type", "crs_authid", "field_schema_json",
    "user_name", "session_id", "created_at", "restored_from_event_id",
    "entity_fingerprint", "event_schema_version", "new_geometry_wkb",
    "invalidated_at",
)
AUDIT_EVENT_INSERT_COLUMNS = AUDIT_EVENT_COLUMNS[1:]  # event_id is autoincrement
AUDIT_EVENT_SELECT_SQL = ", ".join(AUDIT_EVENT_COLUMNS)
AUDIT_EVENT_INSERT_SQL = ", ".join(AUDIT_EVENT_INSERT_COLUMNS)
AUDIT_EVENT_INSERT_PLACEHOLDERS = ",".join(["?"] * len(AUDIT_EVENT_INSERT_COLUMNS))


def build_lightweight_select_sql() -> str:
    """SELECT fragment with geometry BLOBs replaced by NULL/NOT-NULL booleans.

    Used by the search service when callers (search list, paginated UI)
    do not need the raw WKB bytes. The lightweight projection keeps the
    column order identical so _row_to_event can decode either projection
    with the same indices.
    """
    cols = list(AUDIT_EVENT_COLUMNS)
    cols[cols.index("geometry_wkb")] = "(geometry_wkb IS NOT NULL)"
    cols[cols.index("new_geometry_wkb")] = "(new_geometry_wkb IS NOT NULL)"
    return ", ".join(cols)


# No memory-mapped I/O for a local journal on Windows, measured on this
# code base:
#   * while one connection maps the file (the JournalManager's, open for
#     the whole session), VACUUM + wal_checkpoint(TRUNCATE) never shrinks
#     it: 5 140 480 bytes before and after, still after every connection
#     closed; without mmap it shrinks to 8 192 bytes. "Compacter" reclaimed
#     nothing while the plugin ran, and said so as a success;
#   * an I/O error on a mapped page (disk removed, sector unreadable) is a
#     hardware exception, not an SQLite error: it terminates QGIS.
# The read cost is small: indexed queries stay within tens of ms either
# way, the 100 000-row rewind fetch took 1370 ms without mmap and 1565 ms
# with it. Elsewhere a mapping follows a truncation and is kept.
_LOCAL_MMAP_SIZE = 0 if os.name == "nt" else 268435456

_PRAGMAS = [
    "PRAGMA busy_timeout=5000",
    # page_size only takes effect on a new database before journal_mode is set.
    "PRAGMA page_size=4096",
    "PRAGMA journal_mode=WAL",
    # WAL + synchronous=NORMAL: performance/durability trade-off.
    # An OS crash may lose the last committed events.
    # Acceptable for a local audit journal (source data remains intact).
    "PRAGMA synchronous=NORMAL",
    "PRAGMA cache_size=-8000",
    "PRAGMA foreign_keys=OFF",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA journal_size_limit=67108864",
    f"PRAGMA mmap_size={_LOCAL_MMAP_SIZE}",
    "PRAGMA analysis_limit=1000",
]

_READ_ONLY_LOCAL_PRAGMAS = [
    "PRAGMA busy_timeout=5000",
    "PRAGMA cache_size=-8000",
    "PRAGMA foreign_keys=OFF",
    "PRAGMA temp_store=MEMORY",
    f"PRAGMA mmap_size={_LOCAL_MMAP_SIZE}",
    "PRAGMA analysis_limit=1000",
]

_READ_ONLY_NETWORK_PRAGMAS = [
    "PRAGMA busy_timeout=5000",
    "PRAGMA cache_size=-8000",
    "PRAGMA foreign_keys=OFF",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA mmap_size=0",
    "PRAGMA analysis_limit=1000",
]

# A journal lives next to its project, and a project on a network share
# (UNC path, mapped network drive) is a normal place to keep one. Two of
# the local settings are unsafe there, as SQLite documents:
#   * WAL keeps its index in shared memory (the -shm file, mapped), which
#     is not coherent between two clients of a share: each machine sees
#     its own view of the WAL, commits are lost or the file ends up
#     "malformed";
#   * an I/O error on a memory-mapped page is not an SQLite error but a
#     hardware exception (EXCEPTION_IN_PAGE_ERROR on Windows): a two
#     second share outage during a read terminates QGIS.
# So a network journal uses the rollback journal (DELETE, readers and the
# writer then take turns, busy_timeout absorbs it), FULL sync (NORMAL is
# only safe with WAL), and no mmap. busy_timeout comes first: switching an
# existing WAL journal back to DELETE needs a lock.
#
# Every journal an older version left on a share IS in WAL mode, and the
# mode is written in the file. Leaving it can fail, and how it fails says
# what to do (see _leave_wal_on_share):
#   * SQLITE_BUSY: another machine keeps it open in WAL mode (an older
#     RecoverLand). Writing next to it is the corruption to avoid: refused;
#   * the file is read-only (share mounted read-only, archived project),
#     or the connection only reads: the history is served as it is, in
#     WAL, and nothing is written to it.
_NETWORK_JOURNAL_MODE = "PRAGMA journal_mode=DELETE"
_NETWORK_PRAGMAS = [
    "PRAGMA busy_timeout=5000",
    _NETWORK_JOURNAL_MODE,
    "PRAGMA synchronous=FULL",
    "PRAGMA cache_size=-8000",
    "PRAGMA page_size=4096",
    "PRAGMA foreign_keys=OFF",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA journal_size_limit=67108864",
    "PRAGMA mmap_size=0",
    "PRAGMA analysis_limit=1000",
]

# Filesystems whose files other machines may open concurrently (POSIX).
_NETWORK_FS_TYPES = frozenset({
    "nfs", "nfs4", "cifs", "smb", "smbfs", "smb3", "afs", "ncpfs",
    "fuse.sshfs", "9p", "ceph", "glusterfs", "fuse.glusterfs", "lustre",
    "gpfs", "davfs", "fuse.davfs2", "fuse.rclone",
})

_NETWORK_LOG_LOCK = threading.Lock()
_NETWORK_JOURNALS_LOGGED: set = set()
_NETWORK_WAL_READERS_LOGGED: set = set()
_WIN_KERNEL32 = None


def _warn_once(seen: set, path: str) -> bool:
    key = path_key(path)
    with _NETWORK_LOG_LOCK:
        if key in seen:
            return False
        seen.add(key)
        return True


class NetworkJournalBusyError(sqlite3.OperationalError):
    """A network journal still in WAL mode that it is not safe to write."""


class NewerSchemaError(sqlite3.DatabaseError):
    """Raised when opening a journal created by a newer plugin schema version."""


_TABLE_DDL = [
    """CREATE TABLE IF NOT EXISTS schema_version (
        version_number INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL,
        description TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS backend_settings (
        setting_key TEXT PRIMARY KEY,
        setting_value TEXT,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS audit_session (
        session_id TEXT PRIMARY KEY,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        committed_at TEXT,
        rolled_back_at TEXT,
        qgis_user_context_json TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS datasource_registry (
        datasource_fingerprint TEXT PRIMARY KEY,
        provider_type TEXT NOT NULL,
        source_uri TEXT NOT NULL,
        layer_name TEXT,
        authcfg TEXT,
        crs_authid TEXT,
        geometry_type TEXT DEFAULT 'NoGeometry',
        last_seen_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS audit_event (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        layer_id_snapshot TEXT,
        layer_name_snapshot TEXT,
        provider_type TEXT NOT NULL,
        feature_identity_json TEXT,
        operation_type TEXT NOT NULL CHECK(operation_type IN ('INSERT','UPDATE','DELETE')),
        attributes_json TEXT NOT NULL,
        geometry_wkb BLOB,
        geometry_type TEXT DEFAULT 'NoGeometry',
        crs_authid TEXT,
        field_schema_json TEXT,
        user_name TEXT NOT NULL,
        session_id TEXT,
        created_at TEXT NOT NULL,
        restored_from_event_id INTEGER,
        entity_fingerprint TEXT,
        event_schema_version INTEGER,
        new_geometry_wkb BLOB,
        invalidated_at TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS datasource_alias (
        alias_fingerprint TEXT PRIMARY KEY,
        target_fingerprint TEXT NOT NULL,
        created_at TEXT NOT NULL,
        note TEXT,
        CHECK(alias_fingerprint != target_fingerprint)
    )""",
]

_INDEX_DDL = [
    """CREATE INDEX IF NOT EXISTS idx_event_op_date
       ON audit_event(operation_type, created_at)""",
    """CREATE INDEX IF NOT EXISTS idx_event_user_date
       ON audit_event(user_name, created_at)""",
    """CREATE INDEX IF NOT EXISTS idx_event_restored
       ON audit_event(restored_from_event_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_session
       ON audit_event(session_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_created
       ON audit_event(created_at)""",
    """CREATE INDEX IF NOT EXISTS idx_event_entity_stream
       ON audit_event(datasource_fingerprint, entity_fingerprint, event_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_temporal
       ON audit_event(datasource_fingerprint, created_at, event_id)""",
    """CREATE INDEX IF NOT EXISTS idx_event_active
       ON audit_event(datasource_fingerprint, layer_name_snapshot, created_at)
       WHERE restored_from_event_id IS NULL""",
    """CREATE INDEX IF NOT EXISTS idx_event_active_created
       ON audit_event(created_at)
       WHERE restored_from_event_id IS NULL""",
    """CREATE INDEX IF NOT EXISTS idx_event_snap_lookup
       ON audit_event(datasource_fingerprint, entity_fingerprint, created_at)
       WHERE invalidated_at IS NULL""",
    """CREATE INDEX IF NOT EXISTS idx_event_invalidated
       ON audit_event(invalidated_at)
       WHERE invalidated_at IS NOT NULL""",
    """CREATE INDEX IF NOT EXISTS idx_datasource_alias_target
       ON datasource_alias(target_fingerprint)""",
]


def apply_pragmas(conn: sqlite3.Connection, read_only: bool = False) -> None:
    """Apply all required PRAGMAs to an open connection.

    When *read_only* is True (or the underlying file is not writable), only
    read-safe PRAGMAs are executed so DELETE-mode or read-only local/network
    journals never fail with SQLITE_READONLY on ``PRAGMA journal_mode=...``.
    """
    path = _main_db_path(conn)
    network = bool(path) and is_network_path(path)
    if not network:
        pragmas = _READ_ONLY_LOCAL_PRAGMAS if read_only else _PRAGMAS
        for pragma in pragmas:
            conn.execute(pragma)
        return
    if read_only:
        for pragma in _READ_ONLY_NETWORK_PRAGMAS:
            conn.execute(pragma)
        _leave_wal_on_share(conn, path, read_only=True)
        _log_network_journal(conn, path)
        return
    for pragma in _NETWORK_PRAGMAS:
        if pragma == _NETWORK_JOURNAL_MODE:
            _leave_wal_on_share(conn, path, read_only)
        else:
            conn.execute(pragma)
    _log_network_journal(conn, path)


def _leave_wal_on_share(conn: sqlite3.Connection, path: str,
                        read_only: bool) -> None:
    """Put a network journal in DELETE mode, or say why it stays in WAL."""
    try:
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    except sqlite3.Error as exc:
        flog(f"sqlite_schema: journal mode of {path} unreadable ({exc})", "WARNING")
        mode = ""
    if mode == "delete":
        return
    if read_only:
        if mode == "wal" and _warn_once(_NETWORK_WAL_READERS_LOGGED, path):
            flog(f"sqlite_schema: journal on a network share ({path}) is "
                 f"still in WAL mode for a read-only connection: it is read "
                 f"as it is; only a writing connection may change its mode.",
                 "WARNING")
        return
    error: Optional[sqlite3.Error] = None
    try:
        row = conn.execute(_NETWORK_JOURNAL_MODE).fetchone()
        if row is not None and str(row[0]).lower() == "delete":
            if mode == "wal":
                flog(f"sqlite_schema: journal on a network share ({path}) "
                     f"converted from WAL to DELETE", "INFO")
            return
    except sqlite3.Error as exc:
        error = exc
    reason = (f"{getattr(error, 'sqlite_errorname', '') or type(error).__name__}: "
              f"{error}" if error is not None else "the mode did not change")
    busy = str(getattr(error, "sqlite_errorname", "") or "").startswith(
        ("SQLITE_BUSY", "SQLITE_LOCKED"))
    shown = (mode or "unknown").upper()
    readonly_err = str(getattr(error, "sqlite_errorname", "") or "").startswith(
        "SQLITE_READONLY")
    if not busy and (readonly_err or not _is_writable(conn)):
        flog(f"sqlite_schema: read-only journal on a network share ({path}) "
             f"stays in {shown} mode ({reason}): it is served read-only as "
             f"it is, nothing is written to it from this connection.",
             "WARNING")
        return
    if busy:
        why = (f"another machine keeps it open in {shown} mode, most likely "
               f"an older RecoverLand version")
    else:
        why = "its mode cannot be changed from this machine"
    flog(f"sqlite_schema: journal on a network share ({path}) is in {shown} "
         f"mode and cannot leave it ({reason}): {why}. Writing it from here "
         f"as well would corrupt it; this journal is refused.", "CRITICAL")
    raise NetworkJournalBusyError(
        f"network journal kept in {shown} mode ({why}; {reason})")


def _log_network_journal(conn: sqlite3.Connection, path: str) -> None:
    if not _warn_once(_NETWORK_JOURNALS_LOGGED, path):
        return
    try:
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).upper()
    except sqlite3.Error as exc:
        mode = f"unknown ({exc})"
    level = "INFO" if mode == "DELETE" else "WARNING"
    flog(f"sqlite_schema: journal on a network share ({path}): "
         f"journal_mode=DELETE requested (effective={mode}), synchronous=FULL, "
         f"mmap disabled. WAL shared memory is not coherent between machines "
         f"and a share outage on a mapped page would terminate QGIS.", level)


def is_network_path(path: str) -> bool:
    """True when *path* is on a filesystem other machines can open too.

    Windows: a UNC path (``\\\\server\\share``, also in its extended-length
    form) or a drive letter whose type is DRIVE_REMOTE (mapped network
    drive). POSIX: the longest mount point of /proc/mounts that contains
    the path has a network filesystem type. Any doubt (no /proc/mounts,
    API failure) answers False: the local settings are what every journal
    had before this check existed.
    """
    if not path:
        return False
    if os.name == "nt":
        return _is_network_path_windows(path)
    return _is_network_path_posix(path)


def _win_kernel32():
    global _WIN_KERNEL32
    if _WIN_KERNEL32 is None:
        import ctypes
        from ctypes import wintypes
        k32 = ctypes.WinDLL("kernel32", use_last_error=True)
        k32.GetDriveTypeW.argtypes = (wintypes.LPCWSTR,)
        k32.GetDriveTypeW.restype = wintypes.UINT
        _WIN_KERNEL32 = k32
    return _WIN_KERNEL32


def _is_network_path_windows(path: str) -> bool:
    try:
        resolved = os.path.realpath(path)
    except OSError:
        resolved = path
    for candidate in (path, resolved):
        p = candidate.replace("/", "\\")
        if p.upper().startswith("\\\\?\\UNC\\"):
            return True
        if p.startswith("\\\\?\\") or p.startswith("\\\\.\\"):
            p = p[4:]
        elif p.startswith("\\\\"):
            return True
        drive = os.path.splitdrive(p)[0]
        if len(drive) == 2 and drive[1] == ":":
            try:
                drive_remote = 4
                if _win_kernel32().GetDriveTypeW(drive + "\\") == drive_remote:
                    return True
            except (AttributeError, OSError) as exc:
                flog(f"sqlite_schema: drive type of {drive} unknown ({exc})", "DEBUG")
    return False


def _is_network_path_posix(path: str) -> bool:
    try:
        with open("/proc/mounts", "r", encoding="utf-8") as fh:
            mounts = [line.split() for line in fh]
    except OSError:
        return False
    target = os.path.realpath(path)
    best, best_type = "", ""
    for parts in mounts:
        if len(parts) < 3:
            continue
        point = parts[1].replace("\\040", " ")
        inside = target == point or target.startswith(point.rstrip("/") + "/")
        if inside and len(point) > len(best):
            best, best_type = point, parts[2]
    return best_type in _NETWORK_FS_TYPES


def initialize_schema(conn: sqlite3.Connection) -> None:
    """Create all tables and indexes if they don't exist. Idempotent and atomic.

    Read-only journals are detected BEFORE applying write PRAGMAs or DDL and
    served as they are. Journals created by a newer plugin schema version
    (version > CURRENT_SCHEMA_VERSION) are refused with NewerSchemaError
    rather than mutated.
    """
    writable = _is_writable(conn)
    apply_pragmas(conn, read_only=not writable)
    if not writable:
        flog(
            f"sqlite_schema: journal is read-only "
            f"(path={_main_db_path(conn)!r}); schema left at version "
            f"{get_schema_version(conn)}, migrations and datasource "
            f"reconciliation deferred. The journal stays readable.",
            "WARNING",
        )
        return
    existing_ver = get_schema_version(conn)
    if existing_ver > CURRENT_SCHEMA_VERSION:
        raise NewerSchemaError(
            f"Journal schema version {existing_ver} is newer than supported "
            f"version {CURRENT_SCHEMA_VERSION}; upgrade RecoverLand to open "
            f"this journal for writing."
        )
    # Explicit BEGIN IMMEDIATE: Python's default isolation_level="" runs DDL
    # outside any transaction under `with conn:`, which broke atomicity
    # between CREATE TABLE journal_coverage and stamping coverage_origin.
    started_tx = not conn.in_transaction
    if started_tx:
        conn.execute("BEGIN IMMEDIATE")
    try:
        for ddl in _TABLE_DDL:
            conn.execute(ddl)
        current = get_schema_version(conn)
        if current > CURRENT_SCHEMA_VERSION:
            raise NewerSchemaError(
                f"Journal schema version {current} is newer than supported "
                f"version {CURRENT_SCHEMA_VERSION}."
            )
        if current < CURRENT_SCHEMA_VERSION:
            _run_migrations(conn, current)
        for ddl in _INDEX_DDL:
            conn.execute(ddl)
        from .journal_writes import ensure_v7_schema
        ensure_v7_schema(conn)
        _record_schema_version(conn)
        _reconcile_datasources(conn)
        if started_tx:
            conn.commit()
    except Exception:
        if started_tx and conn.in_transaction:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
        raise


def _main_db_path(conn: sqlite3.Connection) -> str:
    """Filesystem path of the `main` database, '' for memory/temp DBs."""
    try:
        for row in conn.execute("PRAGMA database_list").fetchall():
            if len(row) >= 3 and row[1] == "main":
                return row[2] or ""
    except sqlite3.Error as exc:
        flog(f"sqlite_schema: cannot read database_list: {exc}", "DEBUG")
    return ""


def _is_writable(conn: sqlite3.Connection) -> bool:
    """True when the journal file can actually be written to.

    Probes real OS write permission via open(..., 'r+b') because Windows
    os.access(path, os.W_OK) ignores NTFS/SMB ACLs and only checks the
    legacy DOS read-only attribute.
    """
    path = _main_db_path(conn)
    if not path:
        return True
    fs_path = when_needed(path)
    try:
        if not os.path.isfile(fs_path):
            return True
        if os.name == "nt":
            from .long_paths import win_fs_retry
            try:
                fh = win_fs_retry(open, fs_path, "r+b", attempts=4, base_delay=0.02)
                fh.close()
                return True
            except PermissionError:
                return False
            except OSError:
                return os.access(fs_path, os.W_OK)
        return os.access(fs_path, os.W_OK)
    except OSError as exc:
        flog(f"sqlite_schema: writability check failed on {path!r}: {exc}",
             "DEBUG")
        return True


def _reconcile_datasources(conn: sqlite3.Connection) -> None:
    """Run the v6 datasource reconciliation inside the schema transaction.

    Lives here so that a journal is either fully migrated or not migrated
    at all: the alias rows and the schema version commit together. Never
    raises -- a reconciliation that fails is retried at the next open,
    while a plugin that refuses to open is useless.
    """
    try:
        from .datasource_alias import reconcile_legacy_fingerprints
        reconcile_legacy_fingerprints(conn)
    except Exception as exc:  # noqa: BLE001 - opening the journal wins
        flog(f"sqlite_schema: datasource reconciliation skipped "
             f"({type(exc).__name__}: {exc})", "WARNING")


def _record_schema_version(conn: sqlite3.Connection) -> None:
    """Record current schema version if not already present."""
    row = conn.execute(
        "SELECT version_number FROM schema_version WHERE version_number = ?",
        (CURRENT_SCHEMA_VERSION,)
    ).fetchone()
    if row is None:
        from datetime import datetime, timezone
        conn.execute(
            "INSERT INTO schema_version (version_number, applied_at, description) VALUES (?, ?, ?)",
            (CURRENT_SCHEMA_VERSION, datetime.now(timezone.utc).isoformat(), "Initial schema")
        )


def get_schema_version(conn: sqlite3.Connection) -> int:
    """Read current schema version. Returns 0 if no version recorded."""
    try:
        row = conn.execute(
            "SELECT MAX(version_number) FROM schema_version"
        ).fetchone()
        return row[0] if row and row[0] is not None else 0
    except sqlite3.OperationalError:
        return 0


def get_all_ddl() -> List[str]:
    """Return all DDL statements for external inspection."""
    return list(_TABLE_DDL) + list(_INDEX_DDL)


def get_migration_plan(current_version: int) -> List[Tuple[int, str, str]]:
    """Return ordered list of (version, description, sql) migrations to apply."""
    migrations: List[Tuple[int, str, str]] = []
    if current_version < 1:
        migrations.append((1, "Initial schema", ""))
    if current_version < 2:
        migrations.append((2, "Add entity_fingerprint, event_schema_version, new_geometry_wkb",
                           _V2_MIGRATION_SQL))
    if current_version < 3:
        migrations.append((3, "Add partial indexes and performance PRAGMAs", ""))
    if current_version < 4:
        migrations.append((4, "Add datasource_alias table", _V4_MIGRATION_SQL))
    if current_version < 5:
        migrations.append((5, "Add invalidated_at for soft-delete traces", _V5_MIGRATION_SQL))
    if current_version < 6:
        migrations.append((6, "Reconcile legacy datasource fingerprints", _V6_MIGRATION_SQL))
    if current_version < 7:
        migrations.append((7, "event_uid, commit_id and the capture_commit ledger", ""))
    return migrations


_V2_MIGRATION_SQL = ";".join([
    "ALTER TABLE audit_event ADD COLUMN entity_fingerprint TEXT",
    "ALTER TABLE audit_event ADD COLUMN event_schema_version INTEGER",
    "ALTER TABLE audit_event ADD COLUMN new_geometry_wkb BLOB",
])

_V4_MIGRATION_SQL = (
    "CREATE TABLE IF NOT EXISTS datasource_alias ("
    "alias_fingerprint TEXT PRIMARY KEY, "
    "target_fingerprint TEXT NOT NULL, "
    "created_at TEXT NOT NULL, "
    "note TEXT, "
    "CHECK(alias_fingerprint != target_fingerprint))"
)

_V5_MIGRATION_SQL = "ALTER TABLE audit_event ADD COLUMN invalidated_at TEXT"

# v6 changes no column and no table: `datasource_alias` has existed since
# v4. What it changes is the way a fingerprint is computed, and the
# repair is a set of ALIAS rows written by
# `datasource_alias.reconcile_legacy_fingerprints` -- run at every open,
# inside this same transaction, so an interrupted migration retries
# instead of leaving a half-reconciled journal. Not one row of
# `audit_event` is touched.
_V6_MIGRATION_SQL = ""


def _run_migrations(conn: sqlite3.Connection, current_version: int) -> None:
    """Execute pending migrations from current_version to CURRENT_SCHEMA_VERSION."""
    from datetime import datetime, timezone
    plan = get_migration_plan(current_version)
    for version, description, sql in plan:
        if version <= current_version:
            continue
        if sql:
            for statement in sql.split(";"):
                statement = statement.strip()
                if statement:
                    try:
                        conn.execute(statement)
                    except sqlite3.OperationalError as e:
                        if "duplicate column" not in str(e).lower():
                            raise
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (version, datetime.now(timezone.utc).isoformat(), description)
        )
    if current_version < 2:
        _backfill_entity_fingerprint(conn)
    if current_version < 7:
        from .journal_writes import ensure_v7_schema
        ensure_v7_schema(conn)


_BACKFILL_BATCH = 50000


def _backfill_entity_fingerprint(conn: sqlite3.Connection) -> int:
    """Best-effort backfill of entity_fingerprint from feature_identity_json.

    Uses a monotonic event_id cursor so rows whose identity JSON cannot yield
    a fingerprint never cause an infinite loop.
    """
    from .identity import compute_entity_fingerprint
    total = 0
    last_id = 0
    while True:
        rows = conn.execute(
            "SELECT event_id, feature_identity_json FROM audit_event "
            "WHERE entity_fingerprint IS NULL "
            "AND feature_identity_json IS NOT NULL "
            "AND event_id > ? "
            "ORDER BY event_id "
            "LIMIT ?",
            (last_id, _BACKFILL_BATCH)
        ).fetchall()
        if not rows:
            break
        last_id = rows[-1][0]
        updates = []
        for event_id, identity_json in rows:
            fp = compute_entity_fingerprint(identity_json)
            if fp is not None:
                updates.append((fp, event_id))
        if updates:
            conn.executemany(
                "UPDATE audit_event SET entity_fingerprint = ? WHERE event_id = ?",
                updates
            )
        total += len(updates)
        if len(rows) < _BACKFILL_BATCH:
            break
    return total
