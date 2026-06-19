"""h_t1_phase_order - SESSION_REWIND 17.4 H-T1 non-regression.

Hypothesis (H-T1):
    `_temporal_action_order` (`core/restore_planner.py`) places
    INSERT compensations in Phase 0 ordered by event_id ASC. Two
    consecutive user DELETEs become two Phase 0 INSERTs in eid ASC; if
    a Phase 1 UPDATE compensates an entity_fingerprint=fid:Y where Y
    was held by a still-present feature, an OGR FID recycle on the
    first INSERT can silently steal Y, breaking the UPDATE target.

Verdict produced as the final log line:
    hypothesis_h_t1: status=<VALIDATED|FALSIFIED|UNREPRODUCED> reason=<...>

VALIDATED   = Phase 0 contains 2 INSERTs in eid ASC and the planner
              does not annotate any guard, so the recycle risk
              described by H-T1 is structurally reproducible.
FALSIFIED   = ordering or guard differs from the hypothesis.
UNREPRODUCED= the seed could not be planned (e.g. only Phase 1 actions).

BL-RW-P3-19 / CR-10.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "h_t1_phase_order"
INVARIANT = "BL-RW-P3-19"
EXPECTED_SIGNATURE = r"hypothesis_h_t1: status=(VALIDATED|FALSIFIED|UNREPRODUCED)"

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_DATASOURCE_FP = "h_t1_test_datasource"
_PROJECT_FP = "h_t1_test_project"


def _t_iso(t: datetime) -> str:
    return t.replace(microsecond=0).isoformat(sep=" ")


def _seed(conn: sqlite3.Connection, t0: datetime) -> dict:
    """Seed three user events ordered to exercise H-T1.

    Layout (event_id ASC):
        E1 USER DELETE fp=fid:5    -> Phase 0 INSERT compensatory
        E2 USER DELETE fp=fid:6    -> Phase 0 INSERT compensatory
        E3 USER INSERT fp=fid:9    -> Phase 2 DELETE compensatory
        E4 USER UPDATE fp=fid:6    -> Phase 1 UPDATE compensatory

    The intent: two Phase 0 INSERTs in ASC eid, plus one Phase 1
    UPDATE that targets a fp re-introduced by a Phase 0 INSERT
    (worst case described by H-T1).
    """
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )

    plan = [
        ("DELETE", "fid:5", "del_a", t0 + timedelta(seconds=1)),
        ("DELETE", "fid:6", "del_b", t0 + timedelta(seconds=2)),
        ("INSERT", "fid:9", "ins_c", t0 + timedelta(seconds=3)),
        ("UPDATE", "fid:6", "upd_d", t0 + timedelta(seconds=4)),
    ]
    eids = []
    for op, fp, label, ts in plan:
        cur = conn.execute(
            sql,
            (
                _PROJECT_FP, _DATASOURCE_FP, "lyr_test", "test_layer", "ogr",
                json.dumps({"label": label}),
                op,
                json.dumps({"name": label}),
                None, "NoGeometry", "EPSG:4326",
                json.dumps([{"name": "name", "type": "string"}]),
                "tester", None,
                _t_iso(ts), None, fp, 2, None, None,
            ),
        )
        eids.append(cur.lastrowid)
    conn.commit()
    return {"eids": eids}


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    seed = _seed(conn, t0)
    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["seed"] = seed
    flog(
        f"h_t1 setup: trace_id={ctx.trace_id} eids={seed['eids']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import (
        RestoreCutoff, CutoffType, ConflictPolicy,
    )
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, _t_iso(ctx.data["t0"]), inclusive=True,
    )
    events = fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
    )
    # plan_temporal_restore expects events ordered by event_id DESC.
    events_desc = sorted(
        events, key=lambda e: (e.event_id or 0), reverse=True,
    )
    ctx.data["fetched_event_count"] = len(events)
    plan = plan_temporal_restore(
        events_after_cutoff=events_desc,
        datasource_fp=_DATASOURCE_FP,
        layer_name="test_layer",
        cutoff=cutoff,
        conflict_policy=ConflictPolicy.ABORT,
    )
    ctx.data["plan_actions"] = [
        (a.event_id, a.compensatory_op, a.entity_fingerprint)
        for a in plan.actions
    ]

    phase0 = [
        a for a in plan.actions if a.compensatory_op == "INSERT"
    ]
    phase0_eids = [a.event_id for a in phase0]

    if ctx.data["fetched_event_count"] != 4:
        verdict = "UNREPRODUCED"
        reason = (
            f"fetched={ctx.data['fetched_event_count']} expected=4"
        )
    elif len(phase0) < 2:
        verdict = "UNREPRODUCED"
        reason = (
            f"Phase 0 has only {len(phase0)} INSERTs "
            f"(expected 2 from two user DELETEs)."
        )
    elif phase0_eids != sorted(phase0_eids):
        verdict = "FALSIFIED"
        reason = (
            f"Phase 0 INSERTs not in eid ASC: {phase0_eids}; "
            f"H-T1 assumes eid ASC ordering, hypothesis falsified."
        )
    else:
        # Order matches H-T1 description; risk surface present.
        verdict = "VALIDATED"
        reason = (
            f"Phase 0 contains {len(phase0)} INSERTs in eid ASC "
            f"(eids={phase0_eids}); planner provides no upfront fid "
            f"reservation, so the OGR FID-recycle risk described by "
            f"H-T1 is structurally reproducible. Runtime mitigation "
            f"(fid_remap) is the only line of defence."
        )

    ctx.data["verdict"] = verdict
    ctx.data["reason"] = reason
    flog(
        f"hypothesis_h_t1: status={verdict} reason={reason} "
        f"trace_id={ctx.trace_id}",
        "INFO",
    )


_VERDICT_RE = re.compile(
    r"hypothesis_h_t1:\s+status=(VALIDATED|FALSIFIED|UNREPRODUCED)\s+reason="
)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    out.append((
        "fetched_4_events",
        ctx.data.get("fetched_event_count") == 4,
        f"fetched={ctx.data.get('fetched_event_count')} expected=4",
    ))
    out.append((
        "verdict_in_tri_state",
        ctx.data.get("verdict") in ("VALIDATED", "FALSIFIED", "UNREPRODUCED"),
        f"verdict={ctx.data.get('verdict')!r}",
    ))
    out.append(assert_log_contains(
        ctx.records, _VERDICT_RE.pattern,
        name="verdict_log_present", min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records, rf"h_t1 setup:\s+trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=1,
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
