"""p17_dedup_fid_collision - RecoverLand validation runtime.

Invariant: I-3 (KPI K-3 identity strength); specifically the FID-reuse
sub-case where OGR recycles a FID after a DELETE, producing two
distinct logical entities that share entity_fingerprint='fid:N'.

Backlog item: BL-RW-P1-07.
Root cause: CR-1.

Pre-patch state:
    `core.rewind_dedup._entity_key` returns
    f"{datasource_fp}::{entity_fp}". Two events with the same
    `entity_fingerprint='fid:42'` collide into one bucket inside
    `_collapse_user_chain`, so the chain (INSERT eid=1 -> DELETE eid=2
    -> INSERT eid=3) is treated as a single entity lifecycle and the
    `INSERT (oldest) -> only-UPDATEs -> DELETE (newest)` heuristic
    deletes the latest INSERT alongside the original.

Post-patch state:
    `collapse_rewind_events_with_stats` runs a pre-pass that walks
    events ordered by event_id ASC, tracks each fp via a small state
    machine (None -> open via INSERT -> closed via DELETE -> reopen
    via INSERT triggers a SPLIT) and rewrites the affected event's
    entity_fingerprint to `fp@<split_eid>`. The split is logged via
    `rewind_dedup: fid_recycle_detected fp=fid:42 splits=2 first_eid=A
    second_eid=B`.

Scenario layout (real SQLite journal, no QGIS):
    setup:
        - in-memory SQLite, RecoverLand schema applied
        - insert 3 events sharing entity_fingerprint='fid:42':
            E1 INSERT  eid=1
            E2 DELETE  eid=2
            E3 INSERT  eid=3   <- recycle
    run:
        - fetch all 3 events ordered DESC (collapse_rewind_events
          contract)
        - call collapse_rewind_events_with_stats(events)
        - capture: active list, stats dict, distinct entity keys
          observed inside the function (via the rewrite)

Pre-patch verdict: FAIL.
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "p17_dedup_fid_collision"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DATASOURCE_FP = "p17_test_datasource"
_PROJECT_FP = "p17_test_project"
_RECYCLED_FP = "fid:42"


def _t_iso(t: datetime) -> str:
    return t.replace(microsecond=0).isoformat(sep=" ")


def _seed_events(conn: sqlite3.Connection, t0: datetime) -> dict:
    """Insert 3 events sharing entity_fingerprint='fid:42'.

    Pattern: INSERT(eid=1) -> DELETE(eid=2) -> INSERT(eid=3) where the
    3rd event is a fresh feature that OGR has assigned the recycled
    FID 42.
    """
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    plan = [
        (t0 + timedelta(seconds=1), "INSERT", "first_insert", _RECYCLED_FP),
        (t0 + timedelta(seconds=2), "DELETE", "first_delete", _RECYCLED_FP),
        (t0 + timedelta(seconds=3), "INSERT", "second_insert_recycled", _RECYCLED_FP),
    ]
    eids = []
    for ts, op, label, ent_fp in plan:
        cur = conn.execute(
            "INSERT INTO audit_event ("
            + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
            + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")",
            (
                _PROJECT_FP, _DATASOURCE_FP, "lyr_test", "test_layer",
                "ogr",
                json.dumps({"label": label}),
                op,
                json.dumps({"name": label}),
                None, "NoGeometry", "EPSG:4326",
                json.dumps([{"name": "name", "type": "string"}]),
                "tester", None,
                _t_iso(ts), None,
                ent_fp, 2, None, None,
            ),
        )
        eids.append(cur.lastrowid)
    conn.commit()
    return {"eids": eids, "fp": _RECYCLED_FP}


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    seed = _seed_events(conn, t0)

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["seed"] = seed

    flog(
        f"p17_dedup_fid_collision setup: trace_id={ctx.trace_id} "
        f"eids={seed['eids']} fp={seed['fp']} datasource={_DATASOURCE_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    cutoff_iso = _t_iso(ctx.data["t0"])
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)

    flog(f"p17_dedup_fid_collision run start: trace_id={ctx.trace_id}", "INFO")

    events = fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
    )
    ctx.data["fetched_event_count"] = len(events)

    active, stats = collapse_rewind_events_with_stats(events)
    ctx.data["active"] = active
    ctx.data["active_event_ids"] = sorted(
        e.event_id for e in active if e.event_id is not None
    )
    ctx.data["active_op_types"] = [e.operation_type for e in active]
    ctx.data["active_entity_fps"] = sorted({
        e.entity_fingerprint for e in active if e.entity_fingerprint
    })
    ctx.data["dedup_stats"] = dict(stats)

    flog(
        f"p17_dedup_fid_collision run end: trace_id={ctx.trace_id} "
        f"fetched={len(events)} active_eids={ctx.data['active_event_ids']} "
        f"active_ops={ctx.data['active_op_types']} "
        f"active_fps={ctx.data['active_entity_fps']} "
        f"stats={dict(stats)}",
        "INFO",
    )


_RECYCLE_LOG_RE = re.compile(
    r"rewind_dedup:\s+fid_recycle_detected\s+fp=\S+\s+splits=\d+\s+"
    r"first_eid=\d+\s+second_eid=\d+"
)


def _check_source_pattern(symbol: str) -> tuple[bool, str]:
    rel = Path("core/rewind_dedup.py")
    full = _PLUGIN_ROOT / rel
    if not full.is_file():
        return False, f"missing file: {rel}"
    text = full.read_text(encoding="utf-8", errors="replace")
    if symbol == "detect_fn":
        ok = bool(re.search(r"def\s+_detect_fid_recycle\b", text))
        return ok, ("_detect_fid_recycle defined" if ok
                    else "_detect_fid_recycle not defined")
    if symbol == "split_log":
        ok = bool(re.search(r'fid_recycle_detected', text))
        return ok, ("fid_recycle_detected log present" if ok
                    else "fid_recycle_detected log absent")
    if symbol == "rewrite_call":
        # The collapse_rewind_events_with_stats body must call the
        # detect helper or otherwise rewrite entity_fingerprint with @.
        ok = bool(re.search(
            r"collapse_rewind_events_with_stats[\s\S]{0,2000}?"
            r"(?:_detect_fid_recycle|@\{[^}]+\}|fp@)",
            text,
        ))
        return ok, ("dedup applies fid-recycle rewrite" if ok
                    else "dedup does not apply fid-recycle rewrite")
    return False, f"unknown symbol: {symbol}"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    seed = ctx.data["seed"]

    # ===== Sanity: 3 events fetched =====================================
    out.append((
        "fetched_3_events",
        ctx.data.get("fetched_event_count") == 3,
        f"fetched_event_count={ctx.data.get('fetched_event_count')} expected=3",
    ))

    # ===== Brutal dedup outcome =========================================
    # Without the fix, _collapse_user_chain on a single bucket of 3 events
    # ordered DESC [E3 INSERT, E2 DELETE, E1 INSERT] sees INSERT(newest)
    # ... DELETE ... INSERT(oldest), which is NOT the I->U*->D pattern,
    # so the chain is preserved as-is. Hence pre-patch active_count=3.
    # With the fix, the pre-pass detects the INSERT-after-DELETE recycle,
    # rewrites E3.entity_fingerprint to 'fid:42@3', and the buckets become
    #   bucket1 = [E1 INSERT, E2 DELETE]  -> I->D collapse -> [] (no-op)
    #   bucket2 = [E3 INSERT]              -> kept as-is
    # so the active list has length 1 (E3).
    out.append((
        "post_dedup_active_count_is_1",
        len(ctx.data.get("active") or []) == 1,
        f"len(active)={len(ctx.data.get('active') or [])} expected=1 "
        f"(post-fix: bucket1 [E1 INSERT, E2 DELETE] collapses to no-op; "
        f"only the recycled INSERT E3 survives)",
    ))
    out.append((
        "post_dedup_active_eids_eq_E3",
        ctx.data.get("active_event_ids") == [seed["eids"][2]],
        f"active_event_ids={ctx.data.get('active_event_ids')} "
        f"expected=[{seed['eids'][2]}] (only E3 must remain active)",
    ))
    out.append((
        "post_dedup_two_distinct_entity_keys_seen",
        len(ctx.data.get("active_entity_fps") or []) >= 1
        and any("@" in fp for fp in (ctx.data.get("active_entity_fps") or [])),
        f"active_entity_fps={ctx.data.get('active_entity_fps')} expected: "
        f"at least one fp suffixed by '@<eid>' marking a distinct recycled entity",
    ))

    # ===== Log of split detection =======================================
    out.append(assert_log_contains(
        ctx.records,
        _RECYCLE_LOG_RE.pattern,
        name="fid_recycle_detected_log_present",
        min_count=1,
    ))

    # ===== Source guards ================================================
    for symbol in ("detect_fn", "split_log", "rewrite_call"):
        ok, msg = _check_source_pattern(symbol)
        out.append((f"source__{symbol}", ok, msg))

    # ===== Trace propagation ============================================
    out.append(assert_log_contains(
        ctx.records,
        rf"p17_dedup_fid_collision.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
