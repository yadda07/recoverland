"""h_v1_trace_invalidated - SESSION_REWIND 17.2 H-V1 non-regression.

Hypothesis (H-V1):
    A trace event from a previous rewind, with `invalidated_at IS NULL`,
    keeps neutralising a current `stress`-like user event whose eid was
    recycled. If true, the dedup pipeline silently drops the user event
    and the rewind never compensates it.

Verdict produced as the final log line:
    hypothesis_h_v1: status=<VALIDATED|FALSIFIED|UNREPRODUCED> reason=<...>

VALIDATED   = bug reproduced (stress event silently neutralised by an
              orphaned trace).
FALSIFIED   = pipeline survived (stress event present in the active set).
UNREPRODUCED= scenario could not exercise the path (e.g. seeding failed).

BL-RW-P3-19 / CR-10.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "h_v1_trace_invalidated"
INVARIANT = "BL-RW-P3-19"
EXPECTED_SIGNATURE = r"hypothesis_h_v1: status=(VALIDATED|FALSIFIED|UNREPRODUCED)"

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_DATASOURCE_FP = "h_v1_test_datasource"
_PROJECT_FP = "h_v1_test_project"
_STRESS_FP = "fid:777"


def _t_iso(t: datetime) -> str:
    return t.replace(microsecond=0).isoformat(sep=" ")


def _seed(conn: sqlite3.Connection, t0: datetime) -> dict:
    """Seed: a stress user INSERT and an orphan trace pointing to it.

    Layout (event_id ASC):
        E1  USER  INSERT  fp=fid:777   (the "stress" event we expect to
                                        survive dedup)
        E2  TRACE INSERT  restored_from_event_id=E1  invalidated_at=NULL
                          (orphan trace from a previous rewind that was
                          undone but did not nullify invalidated_at).

    Pre-fix behaviour: the trace neutralises E1 silently.
    Post-fix behaviour: either invalidated_at is properly set, or the
    pipeline keeps E1 in the active set.
    """
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )

    cur = conn.execute(
        sql,
        (
            _PROJECT_FP, _DATASOURCE_FP, "lyr_test", "test_layer", "ogr",
            json.dumps({"label": "stress_insert"}),
            "INSERT",
            json.dumps({"name": "stress_insert"}),
            None, "NoGeometry", "EPSG:4326",
            json.dumps([{"name": "name", "type": "string"}]),
            "tester", None,
            _t_iso(t0 + timedelta(seconds=1)),
            None,            # restored_from_event_id
            _STRESS_FP, 2, None,
            None,            # invalidated_at
        ),
    )
    stress_eid = cur.lastrowid

    cur = conn.execute(
        sql,
        (
            _PROJECT_FP, _DATASOURCE_FP, "lyr_test", "test_layer", "ogr",
            json.dumps({"label": "trace_orphan"}),
            "INSERT",
            json.dumps({"name": "stress_insert"}),
            None, "NoGeometry", "EPSG:4326",
            json.dumps([{"name": "name", "type": "string"}]),
            "rewind", None,
            _t_iso(t0 + timedelta(seconds=2)),
            stress_eid,      # trace points back at stress
            _STRESS_FP, 2, None,
            None,            # invalidated_at IS NULL  <- the orphan
        ),
    )
    trace_eid = cur.lastrowid
    conn.commit()
    return {"stress_eid": stress_eid, "trace_eid": trace_eid}


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
        f"h_v1 setup: trace_id={ctx.trace_id} "
        f"stress_eid={seed['stress_eid']} trace_eid={seed['trace_eid']}",
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
    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, _t_iso(ctx.data["t0"]), inclusive=True,
    )
    events = fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
    )
    ctx.data["fetched_event_count"] = len(events)
    active, stats = collapse_rewind_events_with_stats(events)
    ctx.data["active_eids"] = sorted(
        e.event_id for e in active if e.event_id is not None
    )
    ctx.data["dedup_stats"] = dict(stats)

    seed = ctx.data["seed"]
    stress_present = seed["stress_eid"] in ctx.data["active_eids"]

    if ctx.data["fetched_event_count"] != 2:
        verdict = "UNREPRODUCED"
        reason = (
            f"fetched={ctx.data['fetched_event_count']} expected=2 "
            f"(seed missed)"
        )
    elif stress_present:
        verdict = "FALSIFIED"
        reason = (
            f"stress_eid={seed['stress_eid']} survived dedup; orphan "
            f"trace was correctly ignored or the pipeline does not "
            f"silently neutralise on a NULL invalidated_at."
        )
    else:
        verdict = "VALIDATED"
        reason = (
            f"stress_eid={seed['stress_eid']} silently neutralised by "
            f"orphan trace_eid={seed['trace_eid']} with invalidated_at=NULL "
            f"(active_eids={ctx.data['active_eids']})."
        )

    ctx.data["verdict"] = verdict
    ctx.data["reason"] = reason
    flog(
        f"hypothesis_h_v1: status={verdict} reason={reason} "
        f"trace_id={ctx.trace_id}",
        "INFO",
    )


_VERDICT_RE = re.compile(
    r"hypothesis_h_v1:\s+status=(VALIDATED|FALSIFIED|UNREPRODUCED)\s+reason="
)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    out.append((
        "fetched_2_events",
        ctx.data.get("fetched_event_count") == 2,
        f"fetched={ctx.data.get('fetched_event_count')} expected=2",
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
        ctx.records, rf"h_v1 setup:\s+trace_id={ctx.trace_id}",
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
