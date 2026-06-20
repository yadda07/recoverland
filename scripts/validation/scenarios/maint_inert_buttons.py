"""Scenario BL-MAINT-P1-01: maintenance buttons detect logical garbage.

Creates an isolated SQLite journal, inserts known garbage patterns, and asserts
that the new purge/integrity functions detect and remove them.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile

SCENARIO_ID = "maint_inert_buttons"
INVARIANT = "BL-MAINT-P1-01"
EXPECTED_SIGNATURE = r"maint_inert_buttons scenario=done"


def setup(ctx):
    """Build an isolated journal with controlled garbage."""
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    ctx.data["journal_path"] = path

    conn = sqlite3.connect(path)
    from recoverland.core.sqlite_schema import initialize_schema
    initialize_schema(conn)

    base_event = {
        "project_fingerprint": "proj",
        "datasource_fingerprint": "ds",
        "layer_id_snapshot": "layer",
        "layer_name_snapshot": "layer",
        "provider_type": "memory",
        "feature_identity_json": None,
        "operation_type": "INSERT",
        "attributes_json": "{}",
        "geometry_wkb": None,
        "geometry_type": "NoGeometry",
        "crs_authid": None,
        "field_schema_json": None,
        "user_name": "user",
        "session_id": "session-a",
        "created_at": "2026-01-01T00:00:00Z",
        "restored_from_event_id": None,
        "entity_fingerprint": None,
        "event_schema_version": 1,
        "new_geometry_wkb": None,
        "invalidated_at": None,
    }

    def insert(event):
        cols = ", ".join(event.keys())
        placeholders = ", ".join(["?"] * len(event))
        conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({placeholders})", tuple(event.values()))

    # 1. INSERT/DELETE pair in same session (annullable)
    for i in range(3):
        e = dict(base_event)
        e["entity_fingerprint"] = f"pair-{i}"
        e["operation_type"] = "INSERT"
        e["event_id"] = None
        insert(e)
        e2 = dict(base_event)
        e2["entity_fingerprint"] = f"pair-{i}"
        e2["operation_type"] = "DELETE"
        e2["event_id"] = None
        e2["created_at"] = "2026-01-02T00:00:00Z"
        insert(e2)

    # 2. Invalidated trace
    e3 = dict(base_event)
    e3["entity_fingerprint"] = "invalidated"
    e3["operation_type"] = "UPDATE"
    e3["invalidated_at"] = "2026-06-01T00:00:00Z"
    e3["event_id"] = None
    insert(e3)

    # 3. Orphan trace (restored_from_event_id points to missing event)
    e4 = dict(base_event)
    e4["entity_fingerprint"] = "orphan"
    e4["operation_type"] = "INSERT"
    e4["restored_from_event_id"] = 999999
    e4["event_id"] = None
    insert(e4)

    # 4. A normal active event (must survive)
    e5 = dict(base_event)
    e5["entity_fingerprint"] = "survivor"
    e5["operation_type"] = "INSERT"
    e5["event_id"] = None
    insert(e5)

    conn.commit()
    ctx.data["initial_count"] = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
    conn.close()

    from recoverland.core.logger import flog
    flog(f"maint_inert_buttons setup trace_id={ctx.trace_id} path={path} initial_count={ctx.data['initial_count']}")


def run(ctx):
    """Invoke the new maintenance functions on the isolated journal."""
    path = ctx.data["journal_path"]
    conn = sqlite3.connect(path)
    from recoverland.core.sqlite_schema import apply_pragmas
    apply_pragmas(conn)

    from recoverland.core.retention import (
        count_logical_garbage_events,
        purge_old_events_with_options,
        PurgeOptions,
        RetentionPolicy,
    )
    from recoverland.core.integrity import check_journal_integrity

    ctx.data["garbage_before"] = count_logical_garbage_events(conn)
    policy = RetentionPolicy(retention_days=365, max_events=1_000_000)
    options = PurgeOptions(
        retention=True,
        invalidated=True,
        insert_delete_pairs=True,
        orphan_traces=True,
    )
    ctx.data["purge_result"] = purge_old_events_with_options(conn, policy, options)
    ctx.data["garbage_after"] = count_logical_garbage_events(conn)
    ctx.data["integrity_before"] = check_journal_integrity(path)

    conn.close()

    from recoverland.core.logger import flog
    flog(f"maint_inert_buttons scenario=done trace_id={ctx.trace_id} deleted={ctx.data['purge_result'].deleted_count}")


def assertions(ctx):
    garbage = ctx.data["garbage_before"]
    result = ctx.data["purge_result"]
    garbage_after = ctx.data["garbage_after"]
    integrity = ctx.data["integrity_before"]
    checks = []

    checks.append((
        "a1_invalidated_detected",
        garbage.invalidated_events == 1,
        f"expected 1 invalidated event, got {garbage.invalidated_events}",
    ))
    checks.append((
        "a2_insert_delete_pairs_detected",
        garbage.insert_delete_pairs == 3,
        f"expected 3 insert/delete pairs, got {garbage.insert_delete_pairs}",
    ))
    checks.append((
        "a3_orphan_traces_detected",
        garbage.orphan_traces == 1,
        f"expected 1 orphan trace, got {garbage.orphan_traces}",
    ))
    checks.append((
        "a4_total_logical_garbage",
        garbage.total == 1 + (3 * 2) + 1,
        f"expected 8 logical garbage events, got {garbage.total}",
    ))
    checks.append((
        "a5_purge_removed_invalidated",
        result.invalidated_deleted == 1,
        f"expected 1 invalidated deletion, got {result.invalidated_deleted}",
    ))
    checks.append((
        "a6_purge_removed_pairs",
        result.pair_deleted == 6,
        f"expected 6 pair events deleted, got {result.pair_deleted}",
    ))
    checks.append((
        "a7_purge_removed_orphan_traces",
        result.orphan_trace_deleted == 1,
        f"expected 1 orphan trace deleted, got {result.orphan_trace_deleted}",
    ))
    checks.append((
        "a8_no_logical_garbage_after",
        garbage_after.total == 0,
        f"expected 0 logical garbage after purge, got {garbage_after.total}",
    ))
    checks.append((
        "a9_integrity_detects_orphan_before",
        not integrity.is_healthy and any("missing source event" in i for i in integrity.issues),
        f"expected integrity to flag orphan trace before purge, issues={integrity.issues}",
    ))
    return checks


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
