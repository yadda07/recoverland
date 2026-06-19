"""Scenario BL-RW-P1-23-A2b: active trace guard blocks unsafe cascade rewind.

Standalone proof that has_active_restore_traces detects a non-invalidated
restore trace in the journal.  The dialog uses this to prevent a new rewind
from replaying compensation on top of a previous restore, which is the root
cause of FID-only collision in shapefile rewinds after a prior rewind.
"""
import sqlite3
from datetime import datetime, timezone
from typing import NamedTuple

SCENARIO_ID = "rw_rewind_active_trace_guard"
INVARIANT = "BL-RW-P1-23"

_FP = "datasource_shapefile_fp"


class RestoreCutoff(NamedTuple):
    cutoff_type: str
    value: str
    inclusive: bool = True


class CutoffType:
    BY_DATE = "BY_DATE"
    BY_EVENT_ID = "BY_EVENT_ID"


class AuditEvent(NamedTuple):
    event_id: int
    project_fingerprint: str
    datasource_fingerprint: str
    layer_id_snapshot: str
    layer_name_snapshot: str
    provider_type: str
    feature_identity_json: str
    operation_type: str
    attributes_json: str
    geometry_wkb: bytes
    geometry_type: str
    crs_authid: str
    field_schema_json: str
    user_name: str
    session_id: str
    created_at: str
    restored_from_event_id: int
    entity_fingerprint: str
    event_schema_version: int
    new_geometry_wkb: bytes
    invalidated_at: str


_EVENT_COLUMNS = (
    "event_id", "project_fingerprint", "datasource_fingerprint",
    "layer_id_snapshot", "layer_name_snapshot", "provider_type",
    "feature_identity_json", "operation_type", "attributes_json",
    "geometry_wkb", "geometry_type", "crs_authid", "field_schema_json",
    "user_name", "session_id", "created_at", "restored_from_event_id",
    "entity_fingerprint", "event_schema_version", "new_geometry_wkb",
    "invalidated_at",
)


def _init_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS audit_event (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        layer_id_snapshot TEXT,
        layer_name_snapshot TEXT,
        provider_type TEXT NOT NULL,
        feature_identity_json TEXT,
        operation_type TEXT NOT NULL CHECK(operation_type IN ("INSERT","UPDATE","DELETE")),
        attributes_json TEXT NOT NULL,
        geometry_wkb BLOB,
        geometry_type TEXT DEFAULT "NoGeometry",
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
    )""")
    conn.commit()


def _row_to_event(row):
    return AuditEvent(*row)


def _cutoff_where(datasource_fp, cutoff, include_traces=True):
    op = ">=" if cutoff.inclusive else ">"
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        cutoff_col = "event_id"
    elif cutoff.cutoff_type == CutoffType.BY_DATE:
        cutoff_col = "created_at"
    else:
        return None, []
    ds_cond = "datasource_fingerprint = ? AND " if datasource_fp else ""
    ds_params = [datasource_fp] if datasource_fp else []
    if not include_traces:
        clause = ds_cond + "restored_from_event_id IS NULL AND " + cutoff_col + " " + op + " ?"
        return clause, ds_params + [cutoff.value]
    user_clause = "(restored_from_event_id IS NULL AND " + cutoff_col + " " + op + " ?)"
    trace_clause = ("(restored_from_event_id IS NOT NULL AND invalidated_at IS NULL AND restored_from_event_id IN ("
                   "SELECT event_id FROM audit_event WHERE " + ds_cond + cutoff_col + " " + op + " ?))")
    clause = ds_cond + "(" + user_clause + " OR " + trace_clause + ")"
    params = ds_params + [cutoff.value] + ds_params + [cutoff.value]
    return clause, params


def has_active_restore_traces(conn, datasource_fps, trace_id=""):
    if not datasource_fps:
        return False
    placeholders = ",".join("?" for _ in datasource_fps)
    query = (
        "SELECT 1 FROM audit_event WHERE "
        "restored_from_event_id IS NOT NULL AND invalidated_at IS NULL "
        f"AND datasource_fingerprint IN ({placeholders})"
        " LIMIT 1"
    )
    row = conn.execute(query, list(datasource_fps)).fetchone()
    return row is not None


def setup(ctx):
    ctx.data["conn"] = sqlite3.connect(":memory:")
    _init_schema(ctx.data["conn"])
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    created_user = base.isoformat().replace("+00:00", "")
    created_trace = (base.replace(hour=13)).isoformat().replace("+00:00", "")
    identity_user = '{"fid":1}'
    identity_trace = '{"fid":42}'
    rows = [
        ("proj", _FP, "layer_id", "layer_name", "ogr",
         identity_user, "DELETE", "{}", None, "NoGeometry", "EPSG:4326",
         "[]", "test", "session", created_user, None, "fid:1", 5,
         None, None),
        ("proj", _FP, "layer_id", "layer_name", "ogr",
         identity_trace, "INSERT", "{}", None, "NoGeometry", "EPSG:4326",
         "[]", "test", "session", created_trace, 1, "fid:42", 5,
         None, None),
    ]
    sql = ("INSERT INTO audit_event (project_fingerprint, datasource_fingerprint, "
           "layer_id_snapshot, layer_name_snapshot, provider_type, feature_identity_json, "
           "operation_type, attributes_json, geometry_wkb, geometry_type, crs_authid, "
           "field_schema_json, user_name, session_id, created_at, restored_from_event_id, "
           "entity_fingerprint, event_schema_version, new_geometry_wkb, invalidated_at) "
           "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    ctx.data["conn"].executemany(sql, rows)
    ctx.data["conn"].commit()


def run(ctx):
    conn = ctx.data["conn"]
    ctx.data["active_before_invalidate"] = has_active_restore_traces(
        conn, [_FP], trace_id="test")
    conn.execute(
        "UPDATE audit_event SET invalidated_at = ? WHERE restored_from_event_id IS NOT NULL",
        ("2026-01-01T14:00:00",),
    )
    conn.commit()
    ctx.data["active_after_invalidate"] = has_active_restore_traces(
        conn, [_FP], trace_id="test")


def assertions(ctx):
    return [
        ("detects_active_trace", ctx.data["active_before_invalidate"] is True,
         f"active_before_invalidate={ctx.data['active_before_invalidate']} expected True"),
        ("ignores_invalidated_trace", ctx.data["active_after_invalidate"] is False,
         f"active_after_invalidate={ctx.data['active_after_invalidate']} expected False"),
    ]
