"""Journal integrity and recovery for RecoverLand (RLU-064).

Verifies SQLite journal health at startup. Handles corruption detection,
WAL checkpoint, pending event recovery, and migration state validation.
"""
import os
import json
import base64
import sqlite3
from typing import NamedTuple, List

from .sqlite_schema import apply_pragmas, get_schema_version, CURRENT_SCHEMA_VERSION
from .logger import flog, timed_op

_PENDING_FILENAME = "recoverland_pending.json"


class IntegrityResult(NamedTuple):
    is_healthy: bool
    issues: List[str]
    recovered_events: int


def check_journal_integrity(db_path: str, trace_id: str = "") -> IntegrityResult:
    """Run all integrity checks on a journal file.

    Returns IntegrityResult with health status and any issues found.
    """
    issues: List[str] = []
    recovered = 0

    with timed_op("check_journal_integrity", trace_id):
        if not os.path.isfile(db_path):
            return IntegrityResult(False, ["Journal file not found"], 0)

        conn = None
        try:
            conn = sqlite3.connect(db_path)
            apply_pragmas(conn)

            _check_sqlite_integrity(conn, issues)
            _check_wal_state(conn, issues)
            _check_schema_version(conn, issues)
            recovered = _recover_pending_events(db_path, conn)

        except sqlite3.Error as e:
            issues.append(f"Cannot open journal: {e}")
        finally:
            if conn:
                conn.close()

    is_healthy = len(issues) == 0
    return IntegrityResult(is_healthy, issues, recovered)


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


def _recover_pending_events(db_path: str, conn: sqlite3.Connection) -> int:
    """Re-integrate events from the pending recovery file."""
    pending_path = _get_pending_path(db_path)
    if not os.path.isfile(pending_path):
        return 0

    try:
        with open(pending_path, "r", encoding="utf-8") as f:
            events = json.load(f)

        if not isinstance(events, list) or len(events) == 0:
            os.remove(pending_path)
            return 0

        count, remaining = _insert_pending_events(conn, events)
        if remaining:
            _rewrite_pending_events(pending_path, remaining)
            flog(
                f"integrity: recovered {count} pending events, kept {len(remaining)} unrecovered",
                "WARNING",
            )
            return count

        os.remove(pending_path)
        flog(f"integrity: recovered {count} pending events")
        return count

    except (json.JSONDecodeError, OSError) as e:
        flog(f"integrity: pending recovery failed: {e}", "ERROR")
        return 0


def _insert_pending_events(conn: sqlite3.Connection, events: list) -> tuple:
    """Insert recovered events into audit_event table (v2 schema)."""
    sql = """
        INSERT INTO audit_event (
            project_fingerprint, datasource_fingerprint, layer_id_snapshot,
            layer_name_snapshot, provider_type, feature_identity_json,
            operation_type, attributes_json, geometry_wkb, geometry_type,
            crs_authid, field_schema_json, user_name, session_id,
            created_at, restored_from_event_id,
            entity_fingerprint, event_schema_version, new_geometry_wkb
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    count = 0
    remaining = []
    with conn:
        for evt in events:
            if not isinstance(evt, dict):
                flog("integrity: skip pending event with invalid payload", "WARNING")
                remaining.append(evt)
                continue
            try:
                restored = _restore_event_from_json(dict(evt))
                conn.execute(sql, (
                    restored.get("project_fingerprint", ""),
                    restored.get("datasource_fingerprint", ""),
                    restored.get("layer_id_snapshot"),
                    restored.get("layer_name_snapshot"),
                    restored.get("provider_type", ""),
                    restored.get("feature_identity_json"),
                    restored.get("operation_type", ""),
                    restored.get("attributes_json", "{}"),
                    restored.get("geometry_wkb"),
                    evt.get("geometry_type", "NoGeometry"),
                    evt.get("crs_authid"),
                    evt.get("field_schema_json"),
                    evt.get("user_name", "unknown"),
                    evt.get("session_id"),
                    evt.get("created_at", ""),
                    evt.get("restored_from_event_id"),
                    evt.get("entity_fingerprint"),
                    evt.get("event_schema_version"),
                    restored.get("new_geometry_wkb"),
                ))
                count += 1
            except sqlite3.Error as e:
                flog(f"integrity: skip pending event: {e}", "WARNING")
                remaining.append(evt)
    return count, remaining


def _rewrite_pending_events(pending_path: str, events: list) -> None:
    with open(pending_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False)


def save_pending_events(db_path: str, events: list) -> None:
    """Save unwritten events to a recovery file for next startup.

    Checks available disk space before writing to avoid silent failure.
    """
    pending_path = _get_pending_path(db_path)
    try:
        import shutil
        parent = os.path.dirname(pending_path)
        if os.path.exists(parent):
            usage = shutil.disk_usage(parent)
            if usage.free < 10 * 1024 * 1024:
                flog(f"integrity: skip pending save, disk free={usage.free} < 10 MB", "ERROR")
                return
        serializable = []
        for evt in events:
            if hasattr(evt, '_asdict'):
                serializable.append(_prepare_event_for_json(evt._asdict()))
            elif isinstance(evt, dict):
                serializable.append(_prepare_event_for_json(evt))
        with open(pending_path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False)
        flog(f"integrity: saved {len(serializable)} pending events")
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
            f"file:{db_path}?mode=ro", uri=True, check_same_thread=False
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
    return os.path.join(os.path.dirname(db_path), _PENDING_FILENAME)
