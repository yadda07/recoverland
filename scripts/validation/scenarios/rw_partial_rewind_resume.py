"""rw_partial_rewind_resume - RecoverLand validation runtime.

Invariant: I-8 (a rewind that did not finish can be finished by running it
again on the same date).

Why this case matters
---------------------
A rewind is not guaranteed to compensate every event: a feature can be
locked, its FID moved, the provider can refuse the write, the user can
cancel mid-run. `restore_executor._build_traces_for_succeeded` writes a
trace only for the actions that actually succeeded, which is the right
call -- the journal then describes what really happened.

The consequence is that the layer legitimately ends up in a half-way state,
and the natural user reaction is to run the same rewind again. That retry
MUST pick up exactly the events that failed the first time.

Sequence modelled here (one datasource, two entities):

    t1  U_a   entity A   v0 -> v1
    t2  U_b   entity B   w0 -> w1
    rewind to T (before t1): A is compensated, B fails (no trace for U_b)
    retry the same rewind: U_b -- and only U_b -- must be active

Status: this one is a NON-REGRESSION guard, not a defect proof. Measured
against the pre-patch code it already passed 6/6, because the entity-key
neutralisation that broke the other rewind scenarios only fired on entities
that carry a trace, and the entity that failed carries none.

It is kept because the span-based neutralisation introduced for
`rw_dedup_post_trace_edit` reasons per entity over an ORDER INTERVAL, and
an interval is exactly the kind of construct that starts swallowing
neighbours. Entity B sits inside the trace's time range while having no
trace of its own: if a future refactor widens the span to the datasource,
or keys it on time alone rather than per entity, B disappears from the
retry and a half-applied rewind becomes impossible to finish from the UI.
That failure would be silent -- the dialog would answer "Aucun evenement
apres cette date. Rien a restaurer." on a visibly half-rewound layer.

Note the two entities: the retry must neither forget B nor replay A.

Pre-patch verdict: PASS (6/6, measured).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_partial_rewind_resume"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_FP = "rw_prr_datasource"
_PROJ_FP = "rw_prr_project"


def _insert(conn, ts, op, attrs, entity_fp, restored_from=None):
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event ("
        + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )
    fid = int(entity_fp.split(":")[1])
    cur = conn.execute(sql, (
        _PROJ_FP, _DS_FP, "lyr_prr", "prr_layer", "ogr",
        json.dumps({"fid": fid}), op, json.dumps(attrs, ensure_ascii=False),
        None, "NoGeometry", "EPSG:4326", None,
        "tester", None,
        ts.isoformat(), restored_from,
        entity_fp, 2, None, None,
    ))
    conn.commit()
    return cur.lastrowid


def _active(conn, cutoff_dt, trace_id):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats

    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, cutoff_dt.isoformat(), inclusive=True,
    )
    events = fetch_events_after_cutoff(conn, _DS_FP, cutoff, trace_id=trace_id)
    active, stats = collapse_rewind_events_with_stats(events)
    return sorted(e.event_id for e in active if e.event_id is not None), dict(stats)


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)

    u_a = _insert(conn, t0 + timedelta(seconds=100), "UPDATE",
                  {"changed_only": {"v": {"old": "v0", "new": "v1"}}}, "fid:1")
    u_b = _insert(conn, t0 + timedelta(seconds=200), "UPDATE",
                  {"changed_only": {"w": {"old": "w0", "new": "w1"}}}, "fid:2")

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["u_a"] = u_a
    ctx.data["u_b"] = u_b
    ctx.data["T"] = t0 + timedelta(seconds=50)

    flog(
        f"rw_partial_rewind_resume setup: trace_id={ctx.trace_id} "
        f"u_a={u_a} u_b={u_b} datasource={_DS_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    t0 = ctx.data["t0"]

    flog(f"rw_partial_rewind_resume run start: trace_id={ctx.trace_id}", "INFO")

    # First run: both events are active.
    first, stats_first = _active(conn, ctx.data["T"], ctx.trace_id)
    ctx.data["first_active"] = first
    ctx.data["stats_first"] = stats_first

    # The run partially succeeds: entity A compensated (trace written),
    # entity B failed -> no trace, exactly what _build_traces_for_succeeded
    # produces.
    ctx.data["trace_a"] = _insert(
        conn, t0 + timedelta(seconds=300), "UPDATE",
        {"_restore_ref": ctx.data["u_a"]}, "fid:1",
        restored_from=ctx.data["u_a"],
    )

    # Retry the very same rewind.
    retry, stats_retry = _active(conn, ctx.data["T"], ctx.trace_id)
    ctx.data["retry_active"] = retry
    ctx.data["stats_retry"] = stats_retry

    # And once the retry succeeds too, a third run must find nothing.
    ctx.data["trace_b"] = _insert(
        conn, t0 + timedelta(seconds=400), "UPDATE",
        {"_restore_ref": ctx.data["u_b"]}, "fid:2",
        restored_from=ctx.data["u_b"],
    )
    settled, stats_settled = _active(conn, ctx.data["T"], ctx.trace_id)
    ctx.data["settled_active"] = settled
    ctx.data["stats_settled"] = stats_settled

    flog(
        f"rw_partial_rewind_resume run end: trace_id={ctx.trace_id} "
        f"first={first} retry={retry} settled={settled}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    u_a = ctx.data["u_a"]
    u_b = ctx.data["u_b"]

    out.append((
        "first_run_targets_both_entities",
        ctx.data.get("first_active") == sorted([u_a, u_b]),
        f"first_active={ctx.data.get('first_active')} "
        f"expected={sorted([u_a, u_b])}",
    ))

    # ===== The hole =====================================================
    out.append((
        "retry_resumes_the_failed_entity",
        ctx.data.get("retry_active") == [u_b],
        f"retry_active={ctx.data.get('retry_active')} expected=[{u_b}]. "
        f"Entity B failed during the first run and has no trace, so the "
        f"retry must still compensate it. An empty set means a partially "
        f"applied rewind can never be completed from the UI.",
    ))
    out.append((
        "retry_does_not_replay_the_succeeded_entity",
        u_a not in (ctx.data.get("retry_active") or []),
        f"retry_active={ctx.data.get('retry_active')} must not contain "
        f"U_a={u_a}: replaying its compensation would move entity A a second "
        f"time and accumulate work.",
    ))

    out.append((
        "settled_run_is_a_noop",
        ctx.data.get("settled_active") == [],
        f"settled_active={ctx.data.get('settled_active')} expected=[] "
        f"(both entities compensated: nothing left to do)",
    ))

    stats = ctx.data.get("stats_retry") or {}
    out.append((
        "retry_stats_report_one_drop",
        stats.get("dedup_dropped") == 1 and stats.get("dedup_active") == 1,
        f"dedup_dropped={stats.get('dedup_dropped')} "
        f"dedup_active={stats.get('dedup_active')} expected 1/1 "
        f"stats={stats}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rw_partial_rewind_resume.*trace_id={ctx.trace_id}",
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
