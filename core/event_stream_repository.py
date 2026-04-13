"""Event stream repository for temporal restore (BL-03).

Bounded, indexed reads against the SQLite audit journal.
All queries use parameterized SQL and respect volume limits.
No QGIS dependency.
"""
import sqlite3
from typing import List, Optional

from .audit_backend import AuditEvent
from .search_service import _row_to_event
from .restore_contracts import (
    RestoreCutoff, CutoffType, MAX_EVENTS_PER_RESTORE,
)
from .logger import flog, timed_op

_EVENT_COLUMNS = (
    "event_id, project_fingerprint, datasource_fingerprint,"
    " layer_id_snapshot, layer_name_snapshot, provider_type,"
    " feature_identity_json, operation_type, attributes_json,"
    " geometry_wkb, geometry_type, crs_authid, field_schema_json,"
    " user_name, session_id, created_at, restored_from_event_id,"
    " entity_fingerprint, event_schema_version, new_geometry_wkb"
)


def fetch_entity_stream(
    conn: sqlite3.Connection,
    datasource_fp: str,
    entity_fp: str,
    limit: int = MAX_EVENTS_PER_RESTORE,
) -> List[AuditEvent]:
    """Fetch all events for a single entity, ordered by event_id ASC."""
    query = (
        f"SELECT {_EVENT_COLUMNS} FROM audit_event"
        " WHERE datasource_fingerprint = ? AND entity_fingerprint = ?"
        " ORDER BY event_id ASC LIMIT ?"
    )
    rows = conn.execute(query, (datasource_fp, entity_fp, limit)).fetchall()
    return [_row_to_event(r) for r in rows]


def fetch_events_after_cutoff(
    conn: sqlite3.Connection,
    datasource_fp: str,
    cutoff: RestoreCutoff,
    limit: int = MAX_EVENTS_PER_RESTORE,
    trace_id: str = "",
) -> List[AuditEvent]:
    """Fetch events after a cutoff, ordered for reverse replay (DESC).

    Returns events ordered by event_id DESC so the caller can
    apply compensatory operations from most recent to oldest.
    """
    with timed_op("fetch_events_after_cutoff", trace_id):
        where, params = _cutoff_where(datasource_fp, cutoff)
        if where is None:
            return []
        query = (
            f"SELECT {_EVENT_COLUMNS} FROM audit_event"
            f" WHERE {where} ORDER BY event_id DESC LIMIT ?"
        )
        params.append(limit)
        return [_row_to_event(r) for r in conn.execute(query, params).fetchall()]


def count_events_after_cutoff(
    conn: sqlite3.Connection,
    datasource_fp: str,
    cutoff: RestoreCutoff,
    trace_id: str = "",
) -> int:
    """Count events after a cutoff without loading them."""
    with timed_op("count_events_after_cutoff", trace_id):
        where, params = _cutoff_where(datasource_fp, cutoff)
        if where is None:
            return 0
        row = conn.execute(
            f"SELECT COUNT(*) FROM audit_event WHERE {where}", params
        ).fetchone()
        return row[0] if row else 0


def fetch_events_by_ids(
    conn: sqlite3.Connection,
    event_ids: List[int],
) -> List[AuditEvent]:
    """Fetch specific events by ID, ordered by event_id DESC."""
    if not event_ids:
        return []
    placeholders = ",".join("?" for _ in event_ids)
    query = (
        f"SELECT {_EVENT_COLUMNS} FROM audit_event"
        f" WHERE event_id IN ({placeholders})"
        " ORDER BY event_id DESC"
    )
    return [_row_to_event(r) for r in conn.execute(query, event_ids).fetchall()]


def get_oldest_event_date(
    conn: sqlite3.Connection,
    datasource_fp: Optional[str] = None,
) -> Optional[str]:
    """Return created_at of the oldest event for a datasource (or all), or None."""
    if datasource_fp:
        row = conn.execute(
            "SELECT MIN(created_at) FROM audit_event"
            " WHERE datasource_fingerprint = ?",
            (datasource_fp,),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT MIN(created_at) FROM audit_event",
        ).fetchone()
    if row and row[0]:
        return row[0]
    return None


def _cutoff_where(
    datasource_fp: Optional[str], cutoff: RestoreCutoff,
) -> tuple:
    """Build WHERE clause and params for a cutoff filter.

    When datasource_fp is None, the clause applies to all layers.
    Returns (where_clause, params) or (None, []) if cutoff type is invalid.
    """
    op = ">=" if cutoff.inclusive else ">"
    ds_cond = "datasource_fingerprint = ? AND " if datasource_fp else ""
    ds_params = [datasource_fp] if datasource_fp else []
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        return f"{ds_cond}event_id {op} ?", ds_params + [cutoff.value]
    if cutoff.cutoff_type == CutoffType.BY_DATE:
        return f"{ds_cond}created_at {op} ?", ds_params + [cutoff.value]
    flog(f"event_stream_repository: unknown cutoff type {cutoff.cutoff_type}", "WARNING")
    return None, []
