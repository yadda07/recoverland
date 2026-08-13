"""rw_dedup_post_trace_edit - RecoverLand validation runtime.

Invariant: I-8 (restore traces are the persistent dedup memory).
Root cause: over-broad trace neutralisation in `core.rewind_dedup`.

The hole this scenario proves
-----------------------------
`collapse_rewind_events_with_stats` neutralises already-compensated user
events two ways:

    1. by event_id  -- the trace's `restored_from_event_id` names the exact
       user event whose compensation was already applied. Precise, correct.
    2. by entity key -- ANY active trace on an entity drops EVERY user
       event of that entity inside the rewind window.

Rule 2 is wrong for events that happened AFTER the restore. Real sequence:

    t1  user edits feature F                       -> U1
    t2  user rewinds to t0, F returns to state(t0) -> trace T1 (ref = U1)
    t3  user edits F again                         -> U2
    t4  user rewinds to t0 again

At t4 the window holds {U1, T1, U2}. The correct active set is {U2}: U1 was
already compensated, U2 never was. Rule 2 drops U2 as well, so:

    - mixed scope  -> the rewind silently skips F and leaves it at state(U2)
      while reporting success on the other features;
    - F-only scope -> the collapse yields 0 events and the dialog answers
      "Aucun evenement apres cette date. Rien a restaurer." (recover_dialog
      _on_version_fetch_done), which is false: F is NOT at state(t0).

The in-memory auto-undo fallback in the dialog only fires when
`_last_restore_by_ds` is populated, i.e. within the same dialog session; after
a QGIS restart the trace is still active but the in-memory memory is gone, so
no fallback exists. This is the mechanism behind the reported "Rewind cannot
always go back, there are always holes".

History: neutralisation was event_id-only until commit 8101627
("feat(rewind_dedup): collapse_with_stats API + FID-recycle split"), which
added the entity-key set without mentioning it in the commit message and
without a covering scenario.

Scenario layout (real SQLite journal, real fetch + dedup, no QGIS objects):
    setup:
        - in-memory SQLite with the RecoverLand schema
        - E1 UPDATE  eid=1 t0+10s  changed_only status A->B   (user)
        - E2 UPDATE  eid=2 t0+20s  restored_from_event_id=1   (trace)
        - E3 UPDATE  eid=3 t0+30s  changed_only status A->C   (user, post-restore)
    run:
        - fetch_events_after_cutoff(cutoff=t0, inclusive=True)
        - collapse_rewind_events_with_stats(events)

Pre-patch verdict: FAIL (active=[], E3 lost).
Post-patch verdict: PASS (active=[E3]).
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_dedup_post_trace_edit"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DATASOURCE_FP = "rw_dpte_datasource"
_PROJECT_FP = "rw_dpte_project"
_ENTITY_FP = "fid:7"
_IDENTITY = json.dumps({"fid": 7})


def _t_iso(t: datetime) -> str:
    """Same shape as edit_tracker: datetime.now(timezone.utc).isoformat()."""
    return t.isoformat()


def _seed_events(conn: sqlite3.Connection, t0: datetime) -> dict:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event ("
        + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )

    def _row(ts, op, attrs_json, restored_from):
        return (
            _PROJECT_FP, _DATASOURCE_FP, "lyr_dpte", "dpte_layer",
            "ogr",
            _IDENTITY,
            op,
            attrs_json,
            None, "NoGeometry", "EPSG:4326",
            json.dumps([{"name": "status", "type": "string"}]),
            "tester", None,
            _t_iso(ts), restored_from,
            _ENTITY_FP, 2, None, None,
        )

    eids = []
    # E1: the original user edit, after the cutoff.
    cur = conn.execute(sql, _row(
        t0 + timedelta(seconds=10), "UPDATE",
        json.dumps({"changed_only": {"status": {"old": "A", "new": "B"}}}),
        None,
    ))
    eids.append(cur.lastrowid)
    # E2: the trace written by the first rewind (marker payload, ref = E1).
    cur = conn.execute(sql, _row(
        t0 + timedelta(seconds=20), "UPDATE",
        json.dumps({"_restore_ref": eids[0]}),
        eids[0],
    ))
    eids.append(cur.lastrowid)
    # E3: a NEW user edit made after the rewind. Never compensated.
    cur = conn.execute(sql, _row(
        t0 + timedelta(seconds=30), "UPDATE",
        json.dumps({"changed_only": {"status": {"old": "A", "new": "C"}}}),
        None,
    ))
    eids.append(cur.lastrowid)
    conn.commit()
    return {"eids": eids, "entity_fp": _ENTITY_FP}


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    seed = _seed_events(conn, t0)

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["seed"] = seed

    flog(
        f"rw_dedup_post_trace_edit setup: trace_id={ctx.trace_id} "
        f"eids={seed['eids']} entity_fp={seed['entity_fp']} "
        f"datasource={_DATASOURCE_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, _t_iso(ctx.data["t0"]), inclusive=True,
    )

    flog(f"rw_dedup_post_trace_edit run start: trace_id={ctx.trace_id}", "INFO")

    events = fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
    )
    ctx.data["fetched_event_count"] = len(events)
    ctx.data["fetched_eids"] = sorted(
        e.event_id for e in events if e.event_id is not None
    )

    active, stats = collapse_rewind_events_with_stats(events)
    ctx.data["active_event_ids"] = sorted(
        e.event_id for e in active if e.event_id is not None
    )
    ctx.data["dedup_stats"] = dict(stats)

    flog(
        f"rw_dedup_post_trace_edit run end: trace_id={ctx.trace_id} "
        f"fetched={len(events)} active_eids={ctx.data['active_event_ids']} "
        f"stats={dict(stats)}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    e1, e2, e3 = ctx.data["seed"]["eids"]
    active = ctx.data.get("active_event_ids") or []

    # ===== Sanity: fetch brought back the whole window ===================
    out.append((
        "fetched_3_events",
        ctx.data.get("fetched_event_count") == 3,
        f"fetched_event_count={ctx.data.get('fetched_event_count')} expected=3 "
        f"(E1 user, E2 trace, E3 user) eids={ctx.data.get('fetched_eids')}",
    ))

    # ===== The hole ======================================================
    # E3 happened AFTER the restore recorded by E2. No compensation has ever
    # been applied for it, so it MUST stay active or the entity never
    # returns to its cutoff state.
    out.append((
        "post_trace_user_edit_survives",
        e3 in active,
        f"active_event_ids={active} must contain E3={e3} "
        f"(user edit made after the trace; never compensated). "
        f"Absent => rewind silently leaves the feature at its post-restore "
        f"state and the dialog reports 'nothing to restore'.",
    ))

    # ===== Precision: the already-compensated event stays neutralised ====
    out.append((
        "compensated_event_still_neutralised",
        e1 not in active,
        f"active_event_ids={active} must NOT contain E1={e1} "
        f"(already compensated by trace E2={e2}); replaying it would "
        f"double-apply the compensation",
    ))

    out.append((
        "active_set_is_exactly_E3",
        active == [e3],
        f"active_event_ids={active} expected=[{e3}]",
    ))

    # ===== Stats stay coherent ==========================================
    stats = ctx.data.get("dedup_stats") or {}
    out.append((
        "stats_one_user_event_dropped",
        stats.get("dedup_dropped") == 1,
        f"dedup_dropped={stats.get('dedup_dropped')} expected=1 "
        f"(only E1) stats={stats}",
    ))
    out.append((
        "stats_one_active_trace",
        stats.get("traces_active") == 1,
        f"traces_active={stats.get('traces_active')} expected=1 stats={stats}",
    ))

    # ===== Neutralisation must be logged per event ======================
    out.append(assert_log_contains(
        ctx.records,
        rf"rewind_dedup: neutralised eid={e1}\b",
        name="neutralised_e1_logged",
        min_count=1,
    ))

    # ===== Trace propagation ============================================
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_dedup_post_trace_edit.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


_SOURCE_RE = re.compile(r"neutralised_entity_keys")


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
