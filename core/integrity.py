# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Journal integrity and recovery for RecoverLand (RLU-064).

Verifies SQLite journal health at startup. Handles corruption detection,
WAL checkpoint, pending event recovery, and migration state validation.
"""
import os
import json
import base64
import sqlite3
from typing import NamedTuple, List

from .sqlite_schema import (
    apply_pragmas, get_schema_version, CURRENT_SCHEMA_VERSION,
    AUDIT_EVENT_INSERT_COLUMNS,
)
from .journal_manager import sqlite_readonly_uri, journal_shares_directory
from .long_paths import when_needed, plain
from .logger import flog, timed_op

# Legacy name, in the journal's directory: kept for a journal that owns its
# directory (<project>/.recoverland/). In the profile's shared audit
# directory the pending file is named after its journal (_PENDING_SUFFIX).
_PENDING_FILENAME = "recoverland_pending.json"
_PENDING_SUFFIX = ".pending.json"

_VALID_OP_TYPES = frozenset({"INSERT", "UPDATE", "DELETE"})

# Authoritative key set for pending events: derived from the schema column
# list so a column rename/addition propagates without manual updates here.
# The v7 columns (event_uid, commit_id) are part of AuditEvent._asdict():
# leaving them out rejected EVERY pending event written since v7.
from .journal_writes import EVENT_EXT_COLUMNS, EVENT_WRITE_COLUMNS  # noqa: E402
_KNOWN_EVENT_KEYS = frozenset(AUDIT_EVENT_INSERT_COLUMNS) | frozenset(EVENT_EXT_COLUMNS)

_REQUIRED_EVENT_KEYS = frozenset({
    "operation_type", "created_at", "user_name",
})


# NOT NULL columns of audit_event: a pending payload that lost one of them
# still has to produce a valid row rather than fail the whole recovery.
_PENDING_DEFAULTS = {
    "project_fingerprint": "",
    "datasource_fingerprint": "",
    "provider_type": "",
    "operation_type": "",
    "attributes_json": "{}",
    "geometry_type": "NoGeometry",
    "user_name": "unknown",
    "created_at": "",
}


def _validate_pending_event(evt: dict) -> str:
    """Return empty string if valid, or a reason string if rejected."""
    if not isinstance(evt, dict):
        return "not a dict"
    missing = _REQUIRED_EVENT_KEYS - evt.keys()
    if missing:
        return f"missing keys: {missing}"
    # event_id is tolerated, never inserted: save_pending_events serialises
    # AuditEvent._asdict(), whose first field is event_id, while
    # AUDIT_EVENT_INSERT_COLUMNS drops it (the column is AUTOINCREMENT).
    # Rejecting on it made EVERY pending event unrecoverable -- the crash
    # safety net announced to the user had never once put an event back in
    # the journal. Filtering at read time rather than at write time also
    # repairs the pending files already sitting on users' disks.
    unknown = evt.keys() - _KNOWN_EVENT_KEYS - {"event_id"}
    if unknown:
        return f"unknown keys: {unknown}"
    if evt.get("operation_type") not in _VALID_OP_TYPES:
        return f"invalid operation_type: {evt.get('operation_type')!r}"
    if not evt.get("created_at"):
        return "empty created_at"
    return ""


class IntegrityResult(NamedTuple):
    is_healthy: bool
    issues: List[str]
    recovered_events: int
    pending_rejected: int = 0


def check_journal_integrity(db_path: str, trace_id: str = "") -> IntegrityResult:
    """Run all integrity checks on a journal file.

    Returns IntegrityResult with health status and any issues found.
    """
    issues: List[str] = []
    recovered = 0
    rejected = 0

    with timed_op("check_journal_integrity", trace_id):
        db_path = when_needed(db_path)
        if not os.path.isfile(db_path):
            return IntegrityResult(False, ["Journal file not found"], 0)

        conn = None
        try:
            conn = sqlite3.connect(db_path)
            apply_pragmas(conn)

            _check_sqlite_integrity(conn, issues)
            corrupt = any(i.startswith("Integrity check failed") for i in issues)
            _check_wal_state(conn, issues)
            _check_schema_version(conn, issues)
            _check_logical_integrity(conn, issues)
            if corrupt:
                # Importing the last copy of accepted edits into a file that
                # is about to be renamed aside as corrupt buries them where
                # nothing reads them. Keep the pending file for the journal
                # that will actually be used.
                issues.append("Pending events file kept: the journal failed "
                              "its integrity check")
            else:
                recovered, rejected = _recover_pending_events(db_path, conn, issues)

        except sqlite3.Error as e:
            issues.append(f"Cannot open journal: {e}")
        finally:
            if conn:
                conn.close()

    is_healthy = len(issues) == 0
    return IntegrityResult(is_healthy, issues, recovered, rejected)


def _check_sqlite_integrity(conn: sqlite3.Connection, issues: List[str]) -> None:
    try:
        result = conn.execute("PRAGMA integrity_check").fetchone()
        if result and result[0] != "ok":
            issues.append(f"Integrity check failed: {result[0]}")
    except sqlite3.Error as e:
        issues.append(f"Integrity check error: {e}")


def _check_wal_state(conn: sqlite3.Connection, issues: List[str]) -> None:
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.Error as e:
        issues.append(f"WAL checkpoint failed: {e}")


def _check_schema_version(conn: sqlite3.Connection, issues: List[str]) -> None:
    try:
        version = get_schema_version(conn)
        if version == 0:
            issues.append("No schema version found")
        elif version > CURRENT_SCHEMA_VERSION:
            issues.append(
                f"Schema version {version} is newer than expected {CURRENT_SCHEMA_VERSION}"
            )
    except sqlite3.Error as e:
        issues.append(f"Schema version check error: {e}")


def _check_logical_integrity(conn: sqlite3.Connection, issues: List[str]) -> None:
    """Check semantic integrity: orphan traces, sessions, datasources."""
    _check_orphan_restored_events(conn, issues)
    _check_orphan_sessions(conn, issues)
    _check_orphan_datasources(conn, issues)


def _check_orphan_restored_events(conn: sqlite3.Connection, issues: List[str]) -> None:
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM audit_event"
            " WHERE restored_from_event_id IS NOT NULL"
            "   AND restored_from_event_id NOT IN"
            "       (SELECT event_id FROM audit_event WHERE restored_from_event_id IS NULL)"
        ).fetchone()
        count = int(row[0] if row else 0)
        if count:
            issues.append(f"{count} trace event(s) reference a missing source event")
    except sqlite3.Error as e:
        issues.append(f"Orphan trace check error: {e}")


def _check_orphan_sessions(conn: sqlite3.Connection, issues: List[str]) -> None:
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM audit_session s"
            " WHERE NOT EXISTS (SELECT 1 FROM audit_event e WHERE e.session_id = s.session_id)"
        ).fetchone()
        count = int(row[0] if row else 0)
        if count:
            issues.append(f"{count} session(s) with no events")
    except sqlite3.Error as e:
        issues.append(f"Orphan session check error: {e}")


def _check_orphan_datasources(conn: sqlite3.Connection, issues: List[str]) -> None:
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM datasource_registry r"
            " WHERE NOT EXISTS ("
            "   SELECT 1 FROM audit_event e"
            "   WHERE e.datasource_fingerprint = r.datasource_fingerprint"
            " )"
        ).fetchone()
        count = int(row[0] if row else 0)
        if count:
            issues.append(f"{count} datasource registry entry(ies) with no events")
    except sqlite3.Error as e:
        issues.append(f"Orphan datasource check error: {e}")


def _recover_pending_events(db_path: str, conn: sqlite3.Connection,
                            issues: List[str]) -> tuple:
    """Re-integrate events from the pending recovery file.

    This file is written by a process that is already dying, so it is by
    construction the one most likely to be truncated, empty or half-written.
    It is also the LAST copy of edits the user was told were saved: a
    pending entry may only disappear once it is written in the journal.
    Every branch that keeps the file back therefore also reports an issue,
    so the startup message stops saying "healthy" over an unread file.

    Only the journal's OWN pending file is imported. A journal of the
    profile's shared audit directory used to import the directory's legacy
    file too, shared by every unsaved-project journal: the events of one
    session went into the next unsaved journal opened, another project's
    history. That file is kept and reported instead (_report_shared_pending).

    Returns (recovered_count, rejected_count).
    """
    result = _recover_pending_file(_get_pending_path(db_path), conn, issues)
    if journal_shares_directory(db_path):
        _report_shared_pending(db_path, issues)
    return result


def _report_shared_pending(db_path: str, issues: List[str]) -> None:
    """Say that the shared legacy pending file exists and why it stays."""
    legacy = when_needed(os.path.join(os.path.dirname(db_path), _PENDING_FILENAME))
    if not os.path.isfile(legacy):
        return
    try:
        with open(legacy, "r", encoding="utf-8") as f:
            data = json.load(f)
        what = (f"{len(data)} event(s)" if isinstance(data, list)
                else f"JSON root {type(data).__name__}")
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        what = f"unreadable: {exc}"
    flog(f"integrity: shared legacy pending file {legacy} ({what}) kept, not "
         f"imported into {db_path}: its journal cannot be proven", "WARNING")
    issues.append(
        f"Pending events file kept unrecovered ({plain(legacy)}, {what}): it "
        f"is shared by every unsaved-project journal of the profile, so the "
        f"journal its events belong to cannot be proven; it is never imported "
        f"into another project's journal")


def pending_files_for(db_path: str) -> List[str]:
    """Every pending file whose events may belong to the journal *db_path*.

    Its own file, and for a journal of the shared audit directory the
    legacy file of that directory (never imported, see
    _recover_pending_events). For callers that must not act on a journal
    while edits wait outside it.
    """
    paths = [_get_pending_path(db_path)]
    if journal_shares_directory(db_path):
        paths.append(when_needed(os.path.join(os.path.dirname(db_path),
                                              _PENDING_FILENAME)))
    return paths


def _recover_pending_file(pending_path: str, conn: sqlite3.Connection,
                          issues: List[str]) -> tuple:
    """Import one pending file into *conn* (see _recover_pending_events)."""
    if not os.path.isfile(pending_path):
        return 0, 0

    _MAX_PENDING_SIZE = 50 * 1024 * 1024  # 50 MB
    try:
        file_size = os.path.getsize(pending_path)
        if file_size > _MAX_PENDING_SIZE:
            flog(
                f"integrity: pending file too large size_bytes={file_size} "
                f"max={_MAX_PENDING_SIZE}, skipping auto-recovery",
                "ERROR",
            )
            issues.append(
                f"Pending events file kept unrecovered "
                f"(too large: {file_size} bytes)"
            )
            return 0, 0
        with open(pending_path, "r", encoding="utf-8") as f:
            events = json.load(f)

        if not isinstance(events, list):
            # The destructive branch this replaces: the file was removed as
            # soon as the JSON root was not a list. A perfectly valid file of
            # another shape -- an older {"events": [...]} layout, a manual
            # merge, a partially rewritten root -- was deleted without one
            # entry being read, and startup still announced a healthy
            # journal. An unread file is a lead; a deleted one is data loss.
            flog(
                f"integrity: pending file root is {type(events).__name__}, "
                f"not a list; kept untouched at {pending_path}",
                "ERROR",
            )
            issues.append(
                f"Pending events file kept unrecovered "
                f"(unexpected JSON root: {type(events).__name__})"
            )
            return 0, 0

        if len(events) == 0:
            # An empty list is the one shape that provably holds nothing to
            # recover, so removing it is safe and avoids a useless read at
            # every startup.
            os.remove(pending_path)
            return 0, 0

        try:
            count, remaining = _insert_pending_events(conn, events)
        except sqlite3.Error as exc:
            # A COMMIT that fails (disk full, busy share) is not corruption:
            # letting it escape turned it into "Cannot open journal", which
            # the plugin offers to fix by renaming a HEALTHY journal aside.
            flog(f"integrity: pending import failed, file kept: {exc}", "ERROR")
            issues.append(f"Pending events file kept unrecovered "
                          f"(journal write failed: {exc})")
            return 0, 0
        rejected = len(remaining)
        if remaining:
            _rewrite_pending_events(pending_path, remaining)
            flog(
                f"integrity: recovered {count} pending events, "
                f"rejected {rejected} (kept in pending file)",
                "WARNING",
            )
            return count, rejected

        os.remove(pending_path)
        flog(f"integrity: recovered {count} pending events")
        return count, 0

    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as e:
        # Unreadable bytes, zero-length file, unreadable encoding: the write
        # was cut in flight. Keep the file and say so -- deleting it closes
        # the only remaining lead on those edits.
        flog(f"integrity: pending recovery failed: {e}", "ERROR")
        issues.append(f"Pending events file kept unrecovered ({e})")
        return 0, 0


def _insert_pending_events(conn: sqlite3.Connection, events: list) -> tuple:
    """Insert recovered events into audit_event table.

    Uses the shared AUDIT_EVENT_INSERT_* constants so the column list
    stays in lockstep with WriteQueue and the schema definition.
    """
    from .journal_writes import ensure_v7_schema
    # v7: keyed by event_uid, so a pending file replayed twice (a kill
    # between the COMMIT and the file removal) cannot duplicate an edit.
    sql = "".join((
        "INSERT INTO audit_event (",  # nosec B608: column list is a module constant
        ", ".join(EVENT_WRITE_COLUMNS),
        ") VALUES (",
        ",".join(["?"] * len(EVENT_WRITE_COLUMNS)),
        ") ON CONFLICT(event_uid) DO NOTHING",
    ))
    count = 0
    remaining = []
    with conn:
        ensure_v7_schema(conn)
        for evt in events:
            reason = _validate_pending_event(evt)
            if reason:
                flog(f"integrity: reject pending event: {reason}", "WARNING")
                remaining.append(evt)
                continue
            try:
                restored = _restore_event_from_json(dict(evt))
                # Built from the column list, not hand-written: the previous
                # literal tuple carried 19 values for 20 columns (it predated
                # invalidated_at), so every insert raised and was swallowed as
                # "skip pending event". Driving it off
                # AUDIT_EVENT_INSERT_COLUMNS keeps it in lockstep with the
                # schema the next time a column is added.
                conn.execute(sql, tuple(
                    restored.get(col, _PENDING_DEFAULTS.get(col))
                    for col in EVENT_WRITE_COLUMNS
                ))
                count += 1
            except sqlite3.Error as e:
                flog(f"integrity: skip pending event: {e}", "WARNING")
                remaining.append(evt)
    return count, remaining


def _atomic_write_json(path: str, payload) -> None:
    """Write *payload* so that a crash leaves either the old or the new file.

    The previous code opened the pending file in "w" mode and rewrote it:
    a crash mid-write left a truncated JSON, i.e. every event it held
    unreadable, including the ones saved by earlier sessions.
    """
    tmp = when_needed(f"{path}.tmp-{os.getpid()}")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _rewrite_pending_events(pending_path: str, events: list) -> None:
    _atomic_write_json(pending_path, events)


def save_pending_events(db_path: str, events: list) -> None:
    """Save unwritten events to a recovery file for next startup.

    Checks available disk space before writing to avoid silent failure.
    """
    pending_path = _get_pending_path(db_path)
    try:
        # No free-space guard any more: refusing below 10 MB meant refusing
        # exactly when SQLite fails with SQLITE_FULL, i.e. dropping the batch
        # in the one situation the fallback exists for. Try, and report.
        existing = []
        if os.path.isfile(pending_path):
            try:
                with open(pending_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                if not isinstance(existing, list):
                    raise ValueError(f"root is {type(existing).__name__}")
                flog(f"integrity: appending to existing pending n_existing={len(existing)}")
            except (ValueError, OSError) as read_err:
                # Never overwrite a file we cannot read: it may be the last
                # copy of older edits. Set it aside, then start a new one.
                aside = when_needed(
                    f"{pending_path}.unreadable-{os.getpid()}-{len(events)}")
                try:
                    os.replace(pending_path, aside)
                    flog(f"integrity: unreadable pending file set aside as "
                         f"{aside} ({read_err})", "ERROR")
                except OSError as mv_err:
                    flog(f"integrity: unreadable pending file cannot be set "
                         f"aside ({mv_err}); new events NOT saved over it",
                         "CRITICAL")
                    return
                existing = []
        serializable = list(existing)
        for evt in events:
            if hasattr(evt, '_asdict'):
                serializable.append(_prepare_event_for_json(evt._asdict()))
            elif isinstance(evt, dict):
                serializable.append(_prepare_event_for_json(evt))
        _atomic_write_json(pending_path, serializable)
        flog(f"integrity: saved {len(serializable)} pending events "
             f"(new={len(serializable) - len(existing)})")
    except (OSError, TypeError) as e:
        flog(f"integrity: cannot save pending events: {e}", "ERROR")


class JournalHealthReport(NamedTuple):
    db_size_bytes: int
    wal_size_bytes: int
    total_events: int
    oldest_event: str
    newest_event: str
    schema_version: int


def get_journal_health_report(db_path: str) -> JournalHealthReport:
    """Collect monitoring metrics for a journal file without blocking."""
    db_size = 0
    wal_size = 0
    total_events = 0
    oldest = ""
    newest = ""
    version = 0

    db_path = when_needed(db_path)
    try:
        db_size = os.path.getsize(db_path)
    except OSError:
        pass
    wal_path = db_path + "-wal"
    try:
        wal_size = os.path.getsize(wal_path)
    except OSError:
        pass

    conn = None
    try:
        conn = sqlite3.connect(
            sqlite_readonly_uri(db_path), uri=True, check_same_thread=False
        )
        apply_pragmas(conn)
        version = get_schema_version(conn)
        row = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()
        total_events = row[0] if row else 0
        row = conn.execute("SELECT MIN(created_at) FROM audit_event").fetchone()
        oldest = row[0] or "" if row else ""
        row = conn.execute("SELECT MAX(created_at) FROM audit_event").fetchone()
        newest = row[0] or "" if row else ""
    except sqlite3.Error as e:
        flog(f"integrity: health report error: {e}", "WARNING")
    finally:
        if conn:
            conn.close()

    return JournalHealthReport(
        db_size_bytes=db_size,
        wal_size_bytes=wal_size,
        total_events=total_events,
        oldest_event=oldest,
        newest_event=newest,
        schema_version=version,
    )


def _prepare_event_for_json(d: dict) -> dict:
    """Convert bytes fields to base64 strings for JSON serialization."""
    out = dict(d)
    for key in ("geometry_wkb", "new_geometry_wkb"):
        wkb = out.get(key)
        if isinstance(wkb, (bytes, bytearray)):
            out[key] = "b64:" + base64.b64encode(bytes(wkb)).decode("ascii")
    return out


def _restore_event_from_json(d: dict) -> dict:
    """Convert base64-encoded fields back to bytes after JSON deserialization."""
    for key in ("geometry_wkb", "new_geometry_wkb"):
        wkb = d.get(key)
        if isinstance(wkb, str) and wkb.startswith("b64:"):
            d[key] = base64.b64decode(wkb[4:])
    return d


def _get_pending_path(db_path: str) -> str:
    """The pending-recovery file of the journal *db_path*.

    A journal that owns its directory (<project>/.recoverland/) keeps the
    legacy name next to it. In the profile's shared audit directory every
    unsaved-project journal used to share that one name, so the file is
    named after its journal there, as the capture outbox is.
    """
    if journal_shares_directory(db_path):
        return when_needed(db_path + _PENDING_SUFFIX)
    return when_needed(os.path.join(os.path.dirname(db_path), _PENDING_FILENAME))
