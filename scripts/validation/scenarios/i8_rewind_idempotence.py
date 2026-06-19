"""i8_rewind_idempotence — RecoverLand validation runtime.

Invariant: I-8 (rewind idempotence: a 2nd rewind on the same cutoff
applies 0 action). Charter §5 line I-8.
Backlog item: BL-RW-P0-03.
Root cause: CR-4.
Charter signature expected:
    fetch_events_after_cutoff: ... include_traces=True ... n_events=N

The dominant bug pattern is that the low-level repository functions
default `include_traces=False`, so any new caller silently misses the
trace events written by the previous rewind, applies the same
compensatory operation a second time, and accumulates features.

Production call sites already pass `include_traces=True` explicitly,
but defence in depth requires the *default* itself to be safe.

Scenario:
    1. Build an in-memory SQLite journal with the RecoverLand schema.
    2. Insert 4 events around a snapshot at T:
        - 2 user events (committed after T, restored_from_event_id IS NULL)
        - 2 trace events (compensating the user events, restored_from_event_id NOT NULL)
    3. Call `fetch_events_after_cutoff` and `count_events_after_cutoff`
       through three modes:
        - default (no `include_traces` argument)
        - explicit False (legacy strict mode, must remain available)
        - explicit True (current safe behaviour)
    4. Inspect the source of the 6 lines that carry the default value
       (event_stream_repository.py:46,101,182 ; version_fetch_thread.py:16,59,72).
    5. Inspect the produced log for the structured signature.

Pre-patch verdict: FAIL (default=False).
Post-patch verdict: PASS (default=True everywhere).
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "i8_rewind_idempotence"
INVARIANT = "I-8"
EXPECTED_SIGNATURE = (
    r"fetch_events_after_cutoff:.*include_traces=True.*n_events=\d+"
)

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DATASOURCE_FP = "i8_test_datasource"
_PROJECT_FP = "i8_test_project"


def _t_iso(t: datetime) -> str:
    return t.replace(microsecond=0).isoformat(sep=" ")


def _seed_events(conn: sqlite3.Connection, t0: datetime) -> dict:
    """Insert 2 user events + 2 trace events. Returns the bookkeeping."""
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    # 2 user events (restored_from_event_id IS NULL).
    user_events = [
        (t0 + timedelta(seconds=1), "INSERT", "user-1"),
        (t0 + timedelta(seconds=2), "DELETE", "user-2"),
    ]
    # 2 trace events compensating the user events.
    # restored_from_event_id will be assigned after we know the auto-incremented
    # event_id of the user rows.
    trace_plan = [
        (t0 + timedelta(seconds=3), "DELETE", "trace-of-user-1", 1),  # compense INSERT user-1
        (t0 + timedelta(seconds=4), "INSERT", "trace-of-user-2", 2),  # compense DELETE user-2
    ]

    user_event_ids = []
    for ts, op, label in user_events:
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
                _t_iso(ts), None,                # created_at, restored_from_event_id
                "fid:1", 2, None, None,
            ),
        )
        user_event_ids.append(cur.lastrowid)

    for ts, op, label, target_index in trace_plan:
        target_event_id = user_event_ids[target_index - 1]
        conn.execute(
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
                _t_iso(ts), target_event_id,     # restored_from_event_id pointe vers le user event
                "fid:1", 2, None, None,
            ),
        )
    conn.commit()
    return {
        "user_event_ids": user_event_ids,
        "n_user": len(user_events),
        "n_trace": len(trace_plan),
    }


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)
    ctx.data["t0"] = t0
    ctx.data["t0_iso"] = _t_iso(t0)

    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ctx.data["conn"] = conn
    seed = _seed_events(conn, t0)
    ctx.data["seed"] = seed

    flog(
        "i8_rewind_idempotence setup: trace_id={tid} t0={t0} "
        "n_user={nu} n_trace={nt} datasource={ds}".format(
            tid=ctx.trace_id, t0=ctx.data["t0_iso"],
            nu=seed["n_user"], nt=seed["n_trace"], ds=_DATASOURCE_FP),
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, count_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    cutoff_iso = ctx.data["t0_iso"]
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)

    flog(
        "i8_rewind_idempotence run start: trace_id={tid} cutoff={c}".format(
            tid=ctx.trace_id, c=cutoff_iso),
        "INFO",
    )

    # Three modes for fetch_events_after_cutoff:
    events_default = fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id)
    n_default_fetch = len(events_default)
    n_strict_fetch = len(fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
        include_traces=False))
    n_with_traces_fetch = len(fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
        include_traces=True))

    # Three modes for count_events_after_cutoff:
    n_default_count = count_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id)
    n_strict_count = count_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
        include_traces=False)
    n_with_traces_count = count_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
        include_traces=True)

    # Brutal idempotence check: run the production dedup on the events
    # returned by the default fetch. If P0-03 holds AND the dedup is
    # correct, every user event must be neutralised by its trace, so
    # the 2nd rewind on this cutoff would apply zero action.
    active, dedup_stats = collapse_rewind_events_with_stats(events_default)
    active_eids = sorted(e.event_id for e in active)

    ctx.data.update({
        "n_default_fetch": n_default_fetch,
        "n_strict_fetch": n_strict_fetch,
        "n_with_traces_fetch": n_with_traces_fetch,
        "n_default_count": n_default_count,
        "n_strict_count": n_strict_count,
        "n_with_traces_count": n_with_traces_count,
        "dedup_active_n": len(active),
        "dedup_active_eids": active_eids,
        "dedup_stats": dict(dedup_stats),
    })

    flog(
        "i8_rewind_idempotence run end: trace_id={tid} "
        "fetch default={a} strict={b} with_traces={c} "
        "count default={d} strict={e} with_traces={f} "
        "dedup_active={g} dedup_stats={h}".format(
            tid=ctx.trace_id,
            a=n_default_fetch, b=n_strict_fetch, c=n_with_traces_fetch,
            d=n_default_count, e=n_strict_count, f=n_with_traces_count,
            g=len(active), h=dict(dedup_stats)),
        "INFO",
    )


_DEFAULT_RE_PATTERNS = {
    "fetch_events_after_cutoff": (
        Path("core/event_stream_repository.py"),
        re.compile(r"def\s+fetch_events_after_cutoff\b"
                   r"[\s\S]*?include_traces\s*:\s*bool\s*=\s*True", re.MULTILINE),
    ),
    "count_events_after_cutoff": (
        Path("core/event_stream_repository.py"),
        re.compile(r"def\s+count_events_after_cutoff\b"
                   r"[\s\S]*?include_traces\s*:\s*bool\s*=\s*True", re.MULTILINE),
    ),
    "_cutoff_where": (
        Path("core/event_stream_repository.py"),
        re.compile(r"def\s+_cutoff_where\b"
                   r"[\s\S]*?include_traces\s*:\s*bool\s*=\s*True", re.MULTILINE),
    ),
    "_run_fetch": (
        Path("version_fetch_thread.py"),
        re.compile(r"def\s+_run_fetch\b\([^)]*include_traces\s*=\s*True"),
    ),
    "_run_fetch_task": (
        Path("version_fetch_thread.py"),
        re.compile(r"def\s+_run_fetch_task\b\([^)]*include_traces\s*=\s*True"),
    ),
    "VersionFetchThread__init__": (
        Path("version_fetch_thread.py"),
        re.compile(r"def\s+__init__\b"
                   r"[\s\S]*?include_traces\s*:\s*bool\s*=\s*True", re.MULTILINE),
    ),
}


def _check_default(symbol: str) -> tuple[bool, str]:
    rel, regex = _DEFAULT_RE_PATTERNS[symbol]
    full = _PLUGIN_ROOT / rel
    if not full.is_file():
        return False, f"missing file: {rel}"
    text = full.read_text(encoding="utf-8", errors="replace")
    if regex.search(text):
        return True, f"{symbol} default=True (in {rel})"
    return False, f"{symbol} default!=True (in {rel})"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    seed = ctx.data["seed"]
    expected_strict = seed["n_user"]                       # 2 user events
    expected_with_traces = seed["n_user"] + seed["n_trace"]  # 4 events total

    out.append((
        "fetch_strict_returns_user_only",
        ctx.data["n_strict_fetch"] == expected_strict,
        f"n_strict_fetch={ctx.data['n_strict_fetch']} expected={expected_strict}",
    ))
    out.append((
        "fetch_with_traces_returns_all",
        ctx.data["n_with_traces_fetch"] == expected_with_traces,
        f"n_with_traces_fetch={ctx.data['n_with_traces_fetch']} expected={expected_with_traces}",
    ))
    out.append((
        "fetch_default_includes_traces",
        ctx.data["n_default_fetch"] == expected_with_traces,
        f"n_default_fetch={ctx.data['n_default_fetch']} "
        f"expected={expected_with_traces} (default must include traces)",
    ))
    out.append((
        "count_default_includes_traces",
        ctx.data["n_default_count"] == expected_with_traces,
        f"n_default_count={ctx.data['n_default_count']} expected={expected_with_traces}",
    ))

    # === Brutal idempotence assertions (I-8) =============================
    out.append((
        "idempotence_2nd_rewind_zero_actions",
        ctx.data["dedup_active_n"] == 0,
        f"dedup_active_n={ctx.data['dedup_active_n']} "
        f"active_eids={ctx.data['dedup_active_eids']} expected=0 "
        f"(2nd rewind on same cutoff must apply zero action; "
        f"non-zero means a trace event is missing or did not neutralise "
        f"its user event)",
    ))
    stats = ctx.data["dedup_stats"]
    out.append((
        "dedup_stats_raw_matches_seed",
        stats.get("raw") == expected_with_traces,
        f"dedup_stats.raw={stats.get('raw')} expected={expected_with_traces}",
    ))
    out.append((
        "dedup_stats_user_matches_seed",
        stats.get("user") == expected_strict,
        f"dedup_stats.user={stats.get('user')} expected={expected_strict}",
    ))
    out.append((
        "dedup_stats_traces_active_matches_seed",
        stats.get("traces_active") == seed["n_trace"],
        f"dedup_stats.traces_active={stats.get('traces_active')} "
        f"expected={seed['n_trace']}",
    ))
    out.append((
        "dedup_stats_dropped_neutralises_all_user",
        stats.get("dedup_dropped") == seed["n_user"],
        f"dedup_stats.dedup_dropped={stats.get('dedup_dropped')} "
        f"expected={seed['n_user']} (every user event must be neutralised "
        f"by its trace event)",
    ))
    # =====================================================================

    for symbol in _DEFAULT_RE_PATTERNS:
        ok, msg = _check_default(symbol)
        out.append((f"source_default_true__{symbol}", ok, msg))

    out.append(assert_log_contains(
        ctx.records,
        EXPECTED_SIGNATURE,
        name="fetch_signature_with_traces_in_log",
        min_count=2,
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"rewind_dedup:.*raw.*user.*traces.*->\s*\d+\s*active",
        name="rewind_dedup_summary_logged",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"i8_rewind_idempotence.*trace_id={ctx.trace_id}",
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
