"""Journal diagnostic metrics for RecoverLand (BL-OPT-08).

Produces a structured report analyzing the audit journal's storage
patterns and identifying optimization potential. Pure SQL — no QGIS
dependency.
"""
from __future__ import annotations

import sqlite3
import time
from typing import Dict, NamedTuple

from .logger import flog


class JournalDiagnosticReport(NamedTuple):
    """Structured diagnostic report for the audit journal."""

    total_events: int
    total_entities: int
    entity_distribution: Dict[str, int]
    blob_geom_count: int
    blob_geom_null_count: int
    blob_new_geom_count: int
    blob_new_geom_null_count: int
    geom_duplicate_count: int
    geom_duplicate_bytes_saved: int
    schema_json_total_bytes: int
    schema_json_distinct_count: int
    schema_json_dedup_bytes: int
    invalidated_count: int
    insert_delete_pairs: int
    elapsed_ms: int


def run_journal_diagnostics(conn: sqlite3.Connection) -> JournalDiagnosticReport:
    """Analyze the journal and return a JournalDiagnosticReport.

    Runs read-only queries. Does not modify the database.
    """
    t0 = time.perf_counter()
    flog("journal_diagnostics event=start", "INFO")

    total_events = _count_total(conn)
    total_entities, entity_dist = _entity_distribution(conn)
    blob_stats = _blob_stats(conn)
    geom_dup_count, geom_dup_bytes = _geom_duplicate_stats(conn)
    schema_total, schema_distinct, schema_dedup = _schema_json_stats(conn)
    invalidated = _invalidated_count(conn)
    pairs = _insert_delete_pairs(conn)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    flog(
        f"journal_diagnostics event=done elapsed_ms={elapsed_ms} "
        f"total_events={total_events} total_entities={total_entities} "
        f"invalidated={invalidated} pairs={pairs}",
        "INFO",
    )

    return JournalDiagnosticReport(
        total_events=total_events,
        total_entities=total_entities,
        entity_distribution=entity_dist,
        blob_geom_count=blob_stats["geom_present"],
        blob_geom_null_count=blob_stats["geom_null"],
        blob_new_geom_count=blob_stats["new_geom_present"],
        blob_new_geom_null_count=blob_stats["new_geom_null"],
        geom_duplicate_count=geom_dup_count,
        geom_duplicate_bytes_saved=geom_dup_bytes,
        schema_json_total_bytes=schema_total,
        schema_json_distinct_count=schema_distinct,
        schema_json_dedup_bytes=schema_dedup,
        invalidated_count=invalidated,
        insert_delete_pairs=pairs,
        elapsed_ms=elapsed_ms,
    )


def _count_total(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event"
        " WHERE restored_from_event_id IS NULL"
    ).fetchone()
    return int(row[0]) if row else 0


def _entity_distribution(conn: sqlite3.Connection):
    """Return (total_entities, distribution dict).

    Distribution buckets: '1', '2-5', '6-10', '11-50', '51+'.
    """
    rows = conn.execute(
        "SELECT entity_fingerprint, COUNT(*) as cnt"
        " FROM audit_event"
        " WHERE restored_from_event_id IS NULL"
        "   AND entity_fingerprint IS NOT NULL"
        " GROUP BY entity_fingerprint"
    ).fetchall()
    total = len(rows)
    buckets = {"1": 0, "2-5": 0, "6-10": 0, "11-50": 0, "51+": 0}
    for _, cnt in rows:
        if cnt == 1:
            buckets["1"] += 1
        elif cnt <= 5:
            buckets["2-5"] += 1
        elif cnt <= 10:
            buckets["6-10"] += 1
        elif cnt <= 50:
            buckets["11-50"] += 1
        else:
            buckets["51+"] += 1
    return total, buckets


def _blob_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    row = conn.execute(
        "SELECT"
        " SUM(CASE WHEN geometry_wkb IS NOT NULL THEN 1 ELSE 0 END),"
        " SUM(CASE WHEN geometry_wkb IS NULL THEN 1 ELSE 0 END),"
        " SUM(CASE WHEN new_geometry_wkb IS NOT NULL THEN 1 ELSE 0 END),"
        " SUM(CASE WHEN new_geometry_wkb IS NULL THEN 1 ELSE 0 END)"
        " FROM audit_event"
        " WHERE restored_from_event_id IS NULL"
    ).fetchone()
    return {
        "geom_present": int(row[0] or 0),
        "geom_null": int(row[1] or 0),
        "new_geom_present": int(row[2] or 0),
        "new_geom_null": int(row[3] or 0),
    }


def _geom_duplicate_stats(conn: sqlite3.Connection):
    """Count events where geometry_wkb == new_geometry_wkb (no actual move).

    Returns (count, estimated_bytes_saved).
    """
    row = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(LENGTH(geometry_wkb)), 0)"
        " FROM audit_event"
        " WHERE restored_from_event_id IS NULL"
        "   AND geometry_wkb IS NOT NULL"
        "   AND new_geometry_wkb IS NOT NULL"
        "   AND geometry_wkb = new_geometry_wkb"
    ).fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def _schema_json_stats(conn: sqlite3.Connection):
    """Measure field_schema_json redundancy.

    Returns (total_bytes, distinct_count, dedup_saved_bytes).
    """
    row = conn.execute(
        "SELECT"
        " COALESCE(SUM(LENGTH(field_schema_json)), 0),"
        " COUNT(DISTINCT field_schema_json)"
        " FROM audit_event"
        " WHERE restored_from_event_id IS NULL"
        "   AND field_schema_json IS NOT NULL"
    ).fetchone()
    total_bytes = int(row[0] or 0)
    distinct_count = int(row[1] or 0)
    avg_len_row = conn.execute(
        "SELECT AVG(LENGTH(field_schema_json))"
        " FROM audit_event"
        " WHERE restored_from_event_id IS NULL"
        "   AND field_schema_json IS NOT NULL"
    ).fetchone()
    avg_len = float(avg_len_row[0] or 0) if avg_len_row else 0.0
    dedup_bytes = total_bytes - int(avg_len * distinct_count)
    if dedup_bytes < 0:
        dedup_bytes = 0
    return total_bytes, distinct_count, dedup_bytes


def _invalidated_count(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event"
        " WHERE invalidated_at IS NOT NULL"
    ).fetchone()
    return int(row[0]) if row else 0


def _insert_delete_pairs(conn: sqlite3.Connection) -> int:
    """Count entities that have both INSERT and DELETE in same session.

    These pairs can be annulled during compaction.
    """
    row = conn.execute(
        "SELECT COUNT(*) FROM ("
        "  SELECT entity_fingerprint, session_id"
        "  FROM audit_event"
        "  WHERE restored_from_event_id IS NULL"
        "    AND entity_fingerprint IS NOT NULL"
        "    AND operation_type = 'INSERT'"
        "  INTERSECT"
        "  SELECT entity_fingerprint, session_id"
        "  FROM audit_event"
        "  WHERE restored_from_event_id IS NULL"
        "    AND entity_fingerprint IS NOT NULL"
        "    AND operation_type = 'DELETE'"
        ")"
    ).fetchone()
    return int(row[0]) if row else 0


def format_diagnostic_report(report: JournalDiagnosticReport) -> str:
    """Format the diagnostic report as human-readable text."""
    lines = [
        "=== Journal Diagnostic Report ===",
        "",
        f"Total events (active): {report.total_events:,}".replace(",", " "),
        f"Total entities:        {report.total_entities:,}".replace(",", " "),
        "",
        "--- Entity distribution ---",
    ]
    for bucket, count in report.entity_distribution.items():
        pct = (count / max(report.total_entities, 1)) * 100
        lines.append(f"  {bucket:>5} events/entity: {count:>6} ({pct:.1f}%)")

    lines.extend([
        "",
        "--- BLOB storage ---",
        f"  geometry_wkb present:     {report.blob_geom_count:>8}",
        f"  geometry_wkb NULL:        {report.blob_geom_null_count:>8}",
        f"  new_geometry_wkb present: {report.blob_new_geom_count:>8}",
        f"  new_geometry_wkb NULL:    {report.blob_new_geom_null_count:>8}",
        "",
        "--- Geometry duplication (geom == new_geom) ---",
        f"  Duplicated events:  {report.geom_duplicate_count:>8}",
        f"  Bytes recoverable:  {_fmt_bytes(report.geom_duplicate_bytes_saved)}",
        "",
        "--- field_schema_json redundancy ---",
        f"  Total size:         {_fmt_bytes(report.schema_json_total_bytes)}",
        f"  Distinct schemas:   {report.schema_json_distinct_count:>8}",
        f"  Dedup savings:      {_fmt_bytes(report.schema_json_dedup_bytes)}",
        "",
        "--- Reclaimable events ---",
        f"  Invalidated traces: {report.invalidated_count:>8}",
        f"  INSERT+DELETE pairs:{report.insert_delete_pairs:>8}",
        "",
        f"Analysis completed in {report.elapsed_ms} ms",
        "=================================",
    ])
    return "\n".join(lines)


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} Ko"
    return f"{n / (1024 * 1024):.2f} Mo"
