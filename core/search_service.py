"""Local search service for RecoverLand (RLU-030, RLU-031, RLU-032).

Executes paginated searches against the SQLite audit journal.
All queries are parameterized and bounded. No unbounded result sets.
Runs in a dedicated thread; results communicated via signals.
"""
import sqlite3
import json
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple

from .audit_backend import AuditEvent, SearchCriteria, SearchResult
from .logger import flog
from .serialization import is_layer_audit_field

_MAX_PAGE_SIZE = 500
_DEFAULT_PAGE_SIZE = 100


@dataclass(frozen=True)
class JournalScopeSummary:
    total_count: int
    selected_count: int
    update_count: int
    delete_count: int
    insert_count: int
    user_count: int
    layer_count: int


def search_events(conn: sqlite3.Connection, criteria: SearchCriteria) -> SearchResult:
    """Execute a bounded, paginated search on the audit journal."""
    page_size = min(criteria.page_size or _DEFAULT_PAGE_SIZE, _MAX_PAGE_SIZE)
    page = max(criteria.page, 1)
    offset = (page - 1) * page_size

    where_clause, params = _build_where_clause(criteria)
    total = _count_matching(conn, where_clause, params)
    flog(f"search_events: where={where_clause!r} params={params!r} total={total}")

    query = f"""
        SELECT event_id, project_fingerprint, datasource_fingerprint,
               layer_id_snapshot, layer_name_snapshot, provider_type,
               feature_identity_json, operation_type, attributes_json,
               geometry_wkb, geometry_type, crs_authid, field_schema_json,
               user_name, session_id, created_at, restored_from_event_id
        FROM audit_event
        {where_clause}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """
    all_params = params + [page_size, offset]
    rows = conn.execute(query, all_params).fetchall()
    events = [_row_to_event(row) for row in rows]

    return SearchResult(
        events=events, total_count=total, page=page, page_size=page_size
    )


def count_events(conn: sqlite3.Connection, criteria: SearchCriteria) -> int:
    """Count matching events without loading data."""
    where_clause, params = _build_where_clause(criteria)
    return _count_matching(conn, where_clause, params)


def get_event_by_id(conn: sqlite3.Connection, event_id: int) -> Optional[AuditEvent]:
    """Retrieve a single event by its ID."""
    query = """
        SELECT event_id, project_fingerprint, datasource_fingerprint,
               layer_id_snapshot, layer_name_snapshot, provider_type,
               feature_identity_json, operation_type, attributes_json,
               geometry_wkb, geometry_type, crs_authid, field_schema_json,
               user_name, session_id, created_at, restored_from_event_id
        FROM audit_event WHERE event_id = ?
    """
    row = conn.execute(query, (event_id,)).fetchone()
    if row is None:
        return None
    return _row_to_event(row)


_MAX_DISTINCT_RESULTS = 1000


def get_distinct_layers(conn: sqlite3.Connection) -> List[Dict[str, str]]:
    """List distinct audited layers with their display names."""
    query = """
        SELECT DISTINCT datasource_fingerprint, layer_name_snapshot, provider_type
        FROM audit_event
        ORDER BY layer_name_snapshot
        LIMIT ?
    """
    rows = conn.execute(query, (_MAX_DISTINCT_RESULTS,)).fetchall()
    return [
        {"fingerprint": r[0], "name": r[1], "provider": r[2]}
        for r in rows
    ]


def get_distinct_users(conn: sqlite3.Connection) -> List[str]:
    """List distinct user names in the journal."""
    query = "SELECT DISTINCT user_name FROM audit_event ORDER BY user_name LIMIT ?"
    return [r[0] for r in conn.execute(query, (_MAX_DISTINCT_RESULTS,)).fetchall()]


def summarize_scope(conn: sqlite3.Connection, criteria: SearchCriteria) -> JournalScopeSummary:
    scope_criteria = SearchCriteria(
        datasource_fingerprint=criteria.datasource_fingerprint,
        layer_name=criteria.layer_name,
        operation_type=None,
        user_name=criteria.user_name,
        start_date=criteria.start_date,
        end_date=criteria.end_date,
        page=1,
        page_size=1,
    )
    where_clause, params = _build_where_clause(scope_criteria)
    totals_query = (
        "SELECT COUNT(*), COUNT(DISTINCT user_name), COUNT(DISTINCT datasource_fingerprint) "
        f"FROM audit_event {where_clause}"
    )
    total_row = conn.execute(totals_query, params).fetchone() or (0, 0, 0)
    op_query = f"SELECT operation_type, COUNT(*) FROM audit_event {where_clause} GROUP BY operation_type"
    op_counts = {"UPDATE": 0, "DELETE": 0, "INSERT": 0}
    for op_name, count in conn.execute(op_query, params).fetchall():
        op_counts[str(op_name or "").upper()] = int(count)
    selected_count = count_events(conn, criteria)
    return JournalScopeSummary(
        total_count=int(total_row[0] or 0),
        selected_count=selected_count,
        update_count=op_counts["UPDATE"],
        delete_count=op_counts["DELETE"],
        insert_count=op_counts["INSERT"],
        user_count=int(total_row[1] or 0),
        layer_count=int(total_row[2] or 0),
    )


def _build_where_clause(criteria: SearchCriteria) -> Tuple[str, list]:
    conditions = []
    params = []

    if criteria.datasource_fingerprint:
        conditions.append("datasource_fingerprint = ?")
        params.append(criteria.datasource_fingerprint)

    if criteria.layer_name:
        conditions.append("layer_name_snapshot = ?")
        params.append(criteria.layer_name)

    if criteria.operation_type:
        conditions.append("operation_type = ?")
        params.append(criteria.operation_type)

    if criteria.user_name:
        conditions.append("user_name = ?")
        params.append(criteria.user_name)

    if criteria.start_date:
        conditions.append("created_at >= ?")
        params.append(criteria.start_date)

    if criteria.end_date:
        conditions.append("created_at <= ?")
        params.append(criteria.end_date)

    if not conditions:
        return "", params

    return "WHERE " + " AND ".join(conditions), params


def _count_matching(conn: sqlite3.Connection, where_clause: str, params: list) -> int:
    query = f"SELECT COUNT(*) FROM audit_event {where_clause}"
    row = conn.execute(query, params).fetchone()
    return row[0] if row else 0


def _row_to_event(row: tuple) -> AuditEvent:
    return AuditEvent(
        event_id=row[0],
        project_fingerprint=row[1],
        datasource_fingerprint=row[2],
        layer_id_snapshot=row[3],
        layer_name_snapshot=row[4],
        provider_type=row[5],
        feature_identity_json=row[6],
        operation_type=row[7],
        attributes_json=row[8],
        geometry_wkb=row[9],
        geometry_type=row[10],
        crs_authid=row[11],
        field_schema_json=row[12],
        user_name=row[13],
        session_id=row[14],
        created_at=row[15],
        restored_from_event_id=row[16],
    )


def reconstruct_attributes(event: AuditEvent) -> Dict[str, Any]:
    """Parse attributes_json from an event into a Python dict.

    Handles both full snapshots and delta formats.
    For geometry-only UPDATEs (changed_only is empty), returns empty dict.
    """
    try:
        data = json.loads(event.attributes_json)
    except (json.JSONDecodeError, TypeError):
        return {}

    if "all_attributes" in data:
        return data["all_attributes"]

    if "changed_only" in data:
        result = {}
        for key, change in data["changed_only"].items():
            if is_layer_audit_field(key):
                continue
            if isinstance(change, dict) and "old" in change:
                result[key] = change["old"]
            else:
                result[key] = change
        return result

    return data


def is_geometry_only_update(event: AuditEvent) -> bool:
    """True if this UPDATE changed only geometry, not attributes."""
    if event.operation_type != "UPDATE":
        return False
    try:
        data = json.loads(event.attributes_json)
        if "changed_only" not in data:
            return False
        for key in data["changed_only"].keys():
            if not is_layer_audit_field(key):
                return False
        return True
    except (json.JSONDecodeError, TypeError):
        return False
