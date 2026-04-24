"""Journal audit service (FEAT-07).

Consolidated read-only introspection of an audit journal. Answers the
question "what is in this journal?" in one query pass:
- overall counts per operation
- top N users by activity
- top N layers by activity
- time range

Pure metier module: no QGIS and no Qt dependency, safe for workers.
All queries are bounded by LIMIT; no unbounded result sets.
"""
from dataclasses import dataclass
from typing import List, Optional, Tuple
import sqlite3

from .logger import flog, timed_op


_DEFAULT_TOP_N = 10
_MAX_TOP_N = 100


@dataclass(frozen=True)
class UserActivity:
    user_name: str
    event_count: int
    last_activity: str


@dataclass(frozen=True)
class LayerActivity:
    datasource_fingerprint: str
    layer_name: str
    event_count: int
    last_activity: str


@dataclass(frozen=True)
class JournalAuditReport:
    total_events: int
    active_events: int
    trace_events: int
    insert_count: int
    update_count: int
    delete_count: int
    distinct_users: int
    distinct_layers: int
    oldest_event: Optional[str]
    newest_event: Optional[str]
    top_users: Tuple[UserActivity, ...]
    top_layers: Tuple[LayerActivity, ...]


def build_journal_audit_report(conn: sqlite3.Connection,
                               top_n: int = _DEFAULT_TOP_N,
                               trace_id: str = "") -> JournalAuditReport:
    """Return a consolidated audit report on the journal contents.

    `top_n` is clamped to [1, 100]. Only active events (non-trace) are
    counted for operation/user/layer breakdowns; trace events are
    reported separately.
    """
    top_n = max(1, min(int(top_n), _MAX_TOP_N))

    with timed_op("journal_audit", trace_id):
        counts = _fetch_counts(conn)
        top_users = _fetch_top_users(conn, top_n)
        top_layers = _fetch_top_layers(conn, top_n)

    return JournalAuditReport(
        total_events=counts["total"],
        active_events=counts["active"],
        trace_events=counts["trace"],
        insert_count=counts["insert"],
        update_count=counts["update"],
        delete_count=counts["delete"],
        distinct_users=counts["users"],
        distinct_layers=counts["layers"],
        oldest_event=counts["oldest"],
        newest_event=counts["newest"],
        top_users=tuple(top_users),
        top_layers=tuple(top_layers),
    )


def _fetch_counts(conn: sqlite3.Connection) -> dict:
    """One-pass aggregate counts over the audit_event table."""
    try:
        row = conn.execute(
            "SELECT "
            " COUNT(*), "
            " SUM(CASE WHEN restored_from_event_id IS NULL THEN 1 ELSE 0 END), "
            " SUM(CASE WHEN restored_from_event_id IS NOT NULL THEN 1 ELSE 0 END), "
            " SUM(CASE WHEN operation_type='INSERT' AND restored_from_event_id IS NULL THEN 1 ELSE 0 END), "
            " SUM(CASE WHEN operation_type='UPDATE' AND restored_from_event_id IS NULL THEN 1 ELSE 0 END), "
            " SUM(CASE WHEN operation_type='DELETE' AND restored_from_event_id IS NULL THEN 1 ELSE 0 END), "
            " COUNT(DISTINCT user_name), "
            " COUNT(DISTINCT datasource_fingerprint), "
            " MIN(created_at), "
            " MAX(created_at) "
            "FROM audit_event"
        ).fetchone() or (0,) * 10
    except sqlite3.Error as e:
        flog(f"journal_audit._fetch_counts: {e}", "WARNING")
        row = (0,) * 10
    return {
        "total": int(row[0] or 0),
        "active": int(row[1] or 0),
        "trace": int(row[2] or 0),
        "insert": int(row[3] or 0),
        "update": int(row[4] or 0),
        "delete": int(row[5] or 0),
        "users": int(row[6] or 0),
        "layers": int(row[7] or 0),
        "oldest": row[8],
        "newest": row[9],
    }


def _fetch_top_users(conn: sqlite3.Connection, limit: int) -> List[UserActivity]:
    try:
        rows = conn.execute(
            "SELECT user_name, COUNT(*) AS cnt, MAX(created_at) "
            "FROM audit_event "
            "WHERE restored_from_event_id IS NULL "
            "GROUP BY user_name "
            "ORDER BY cnt DESC "
            "LIMIT ?",
            (limit,),
        ).fetchall()
    except sqlite3.Error as e:
        flog(f"journal_audit._fetch_top_users: {e}", "WARNING")
        return []
    return [
        UserActivity(user_name=r[0] or "", event_count=int(r[1] or 0),
                     last_activity=r[2] or "")
        for r in rows
    ]


def _fetch_top_layers(conn: sqlite3.Connection, limit: int) -> List[LayerActivity]:
    try:
        rows = conn.execute(
            "SELECT datasource_fingerprint, "
            "       MAX(layer_name_snapshot) AS layer_name, "
            "       COUNT(*) AS cnt, "
            "       MAX(created_at) "
            "FROM audit_event "
            "WHERE restored_from_event_id IS NULL "
            "GROUP BY datasource_fingerprint "
            "ORDER BY cnt DESC "
            "LIMIT ?",
            (limit,),
        ).fetchall()
    except sqlite3.Error as e:
        flog(f"journal_audit._fetch_top_layers: {e}", "WARNING")
        return []
    return [
        LayerActivity(datasource_fingerprint=r[0] or "",
                      layer_name=r[1] or "",
                      event_count=int(r[2] or 0),
                      last_activity=r[3] or "")
        for r in rows
    ]
