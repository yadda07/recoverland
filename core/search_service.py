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
from .logger import flog, timed_op
from .serialization import is_layer_audit_field
from .sql_safety import assert_safe_fragment
from .sqlite_schema import (
    AUDIT_EVENT_SELECT_SQL, build_lightweight_select_sql,
)

_MAX_PAGE_SIZE = 500
_DEFAULT_PAGE_SIZE = 100
_MAX_PARAM_LEN = 1000

_SELECT_COLS = AUDIT_EVENT_SELECT_SQL
_SELECT_COLS_LIGHTWEIGHT = build_lightweight_select_sql()

_BLOB_MARKER = b'\x00'


@dataclass(frozen=True)
class JournalScopeSummary:
    total_count: int
    selected_count: int
    update_count: int
    delete_count: int
    insert_count: int
    user_count: int
    layer_count: int


def search_events(conn: sqlite3.Connection,
                  criteria: SearchCriteria,
                  trace_id: str = "",
                  exclude_blobs: bool = False) -> SearchResult:
    """Execute a bounded, paginated search on the audit journal.

    When *exclude_blobs* is True, geometry BLOBs are replaced by boolean
    markers (BL-PERF-003). Returned events have ``geometry_wkb`` set to a
    1-byte sentinel when the column is non-NULL, otherwise ``None``.
    Use ``get_event_by_id`` or ``fetch_events_by_ids`` to obtain full BLOBs
    when needed (e.g. restore or geometry preview).
    """
    with timed_op("search_events", trace_id):
        page_size = min(criteria.page_size or _DEFAULT_PAGE_SIZE, _MAX_PAGE_SIZE)
        page = max(criteria.page, 1)
        offset = (page - 1) * page_size

        where_clause, params = _build_where_clause(criteria)
        total = _count_matching(conn, where_clause, params)
        flog(f"search_events: conditions={len(params)} total={total}")

        cols = _SELECT_COLS_LIGHTWEIGHT if exclude_blobs else _SELECT_COLS
        assert_safe_fragment(where_clause)
        # B608: static columns list; `where_clause` built with whitelist; values via `?`.
        query = (
            "SELECT " + cols  # nosec B608
            + " FROM audit_event "
            + where_clause
            + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        )
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
    # B608: AUDIT_EVENT_SELECT_SQL is a module-level whitelist; value via `?`.
    query = (
        "SELECT " + AUDIT_EVENT_SELECT_SQL  # nosec B608
        + " FROM audit_event WHERE event_id = ?"
    )
    row = conn.execute(query, (event_id,)).fetchone()
    if row is None:
        return None
    return _row_to_event(row)


_MAX_DISTINCT_RESULTS = 1000


def get_distinct_layers(conn: sqlite3.Connection) -> List[Dict[str, str]]:
    """List distinct audited layers with their display names."""
    try:
        rows = conn.execute(
            "SELECT datasource_fingerprint, layer_name, provider_type"
            " FROM datasource_registry ORDER BY layer_name LIMIT ?",
            (_MAX_DISTINCT_RESULTS,),
        ).fetchall()
        if rows:
            return [
                {"fingerprint": r[0], "name": r[1], "provider": r[2]}
                for r in rows
            ]
    except sqlite3.OperationalError:
        pass
    query = """
        SELECT datasource_fingerprint, layer_name_snapshot, provider_type
        FROM audit_event
        GROUP BY datasource_fingerprint
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
    assert_safe_fragment(where_clause)
    # B608: static aggregate; `where_clause` is internally built and asserted; values via `params`.
    query = (
        "SELECT COUNT(*),"  # nosec B608
        " COUNT(DISTINCT user_name),"
        " COUNT(DISTINCT datasource_fingerprint),"
        " SUM(CASE WHEN operation_type='UPDATE' THEN 1 ELSE 0 END),"
        " SUM(CASE WHEN operation_type='DELETE' THEN 1 ELSE 0 END),"
        " SUM(CASE WHEN operation_type='INSERT' THEN 1 ELSE 0 END)"
        " FROM audit_event " + where_clause
    )
    row = conn.execute(query, params).fetchone() or (0, 0, 0, 0, 0, 0)
    total_count = int(row[0] or 0)
    selected_count = count_events(conn, criteria) if criteria.operation_type else total_count
    return JournalScopeSummary(
        total_count=total_count,
        selected_count=selected_count,
        update_count=int(row[3] or 0),
        delete_count=int(row[4] or 0),
        insert_count=int(row[5] or 0),
        user_count=int(row[1] or 0),
        layer_count=int(row[2] or 0),
    )


def _build_where_clause(criteria: SearchCriteria,
                        include_traces: bool = False) -> Tuple[str, list]:
    conditions = []
    params = []

    def _checked(value: str) -> str:
        if len(value) > _MAX_PARAM_LEN:
            raise ValueError(f"Search parameter too long: {len(value)}")
        return value

    if not include_traces:
        conditions.append("restored_from_event_id IS NULL")

    if criteria.datasource_fingerprint:
        conditions.append("datasource_fingerprint = ?")
        params.append(_checked(criteria.datasource_fingerprint))

    if criteria.layer_name:
        conditions.append("layer_name_snapshot = ?")
        params.append(_checked(criteria.layer_name))

    if criteria.operation_type:
        conditions.append("operation_type = ?")
        params.append(_checked(criteria.operation_type))

    if criteria.user_name:
        conditions.append("user_name = ?")
        params.append(_checked(criteria.user_name))

    if criteria.start_date:
        conditions.append("created_at >= ?")
        params.append(_checked(criteria.start_date))

    if criteria.end_date:
        conditions.append("created_at <= ?")
        params.append(_checked(criteria.end_date))

    if not conditions:
        return "", params

    return "WHERE " + " AND ".join(conditions), params


def _count_matching(conn: sqlite3.Connection, where_clause: str, params: list) -> int:
    assert_safe_fragment(where_clause)
    # Bandit B608: `where_clause` is internally generated with column whitelist
    # and validated by assert_safe_fragment. User values via `params` list.
    query = "SELECT COUNT(*) FROM audit_event " + where_clause  # nosec B608
    row = conn.execute(query, params).fetchone()
    return row[0] if row else 0


def _row_to_event(row: tuple) -> AuditEvent:
    geom_wkb = row[9]
    if isinstance(geom_wkb, int):
        geom_wkb = _BLOB_MARKER if geom_wkb else None
    new_geom = row[19] if len(row) > 19 else None
    if isinstance(new_geom, int):
        new_geom = _BLOB_MARKER if new_geom else None
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
        geometry_wkb=geom_wkb,
        geometry_type=row[10],
        crs_authid=row[11],
        field_schema_json=row[12],
        user_name=row[13],
        session_id=row[14],
        created_at=row[15],
        restored_from_event_id=row[16],
        entity_fingerprint=row[17] if len(row) > 17 else None,
        event_schema_version=row[18] if len(row) > 18 else None,
        new_geometry_wkb=new_geom,
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


def reconstruct_new_attributes(event: AuditEvent) -> Dict[str, Any]:
    """Extract the 'new' (post-edit) attribute values from an event.

    For UPDATE deltas, returns the 'new' side of each changed field.
    For full snapshots (DELETE/INSERT), returns all_attributes as-is.
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
            if isinstance(change, dict) and "new" in change:
                result[key] = change["new"]
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
