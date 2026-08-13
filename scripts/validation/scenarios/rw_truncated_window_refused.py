"""rw_truncated_window_refused - RecoverLand validation runtime.

Invariant: I-8 (a rewind either reaches the cutoff state or refuses).
Root cause: the rewind consumed a truncated event window as if complete.

The hole this scenario proves
-----------------------------
`fetch_events_after_cutoff` caps each datasource at MAX_EVENTS_PER_RESTORE
and orders `created_at DESC, event_id DESC`. Past the cap the rows it drops
are therefore the OLDEST ones -- exactly the events that carry the state at
the cutoff. The function logs `status=truncated` and returns the partial
list; scenario `rw_rewind_fetch_limit` already proves that much.

What was missing is the consumer side. `recover_dialog._on_version_fetch_done`
guarded on::

    if total_count > MAX_EVENTS_PER_RESTORE:   # refuse, pick a newer date

`total_count` is `len(events)` AFTER the fetch and AFTER the collapse, and
the fetch is already bounded by the cap, so the condition can never be true
for a single datasource: dead code. A layer with 12 000 events after the
cutoff was rewound using the 10 000 newest, left in an intermediate state,
and reported as a success. The user sees "restauration terminee" on data
that never reached the requested date -- a hole with no warning.

The fix keeps the honest behaviour: refuse. `count_events_after_cutoff` and
`fetch_events_after_cutoff` share the same WHERE clause (documented
contract), so `count > fetched` while the cap was reached means, and only
means, that rows were dropped. Both rewind entry points
(`_on_version_fetch_done`, `_on_post_undo_fetch_done`) must run that check
BEFORE collapsing, because the collapse only shrinks the list further.

Scenario layout (real schema, real repository functions, no QGIS objects):
    setup: in-memory SQLite, 12 000 events on one datasource after t0
    run:   real count_events_after_cutoff + real fetch_events_after_cutoff
    then:  source guard on both dialog handlers

Pre-patch verdict: FAIL (no guard in the dialog).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_truncated_window_refused"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_FP = "rw_twr_datasource"
_PROJ_FP = "rw_twr_project"
_OVER_LIMIT = 12_000


def _seed(conn: sqlite3.Connection, t0: datetime, n: int) -> None:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event ("
        + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )
    rows = []
    for i in range(n):
        ts = (t0 + timedelta(seconds=10 + i)).isoformat()
        rows.append((
            _PROJ_FP, _DS_FP, "lyr_twr", "twr_layer", "ogr",
            json.dumps({"fid": i}),
            "UPDATE",
            json.dumps({"changed_only": {"v": {"old": i, "new": i + 1}}}),
            None, "NoGeometry", "EPSG:4326", None,
            "tester", None,
            ts, None,
            f"fid:{i}", 2, None, None,
        ))
    conn.executemany(sql, rows)
    conn.commit()


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    _seed(conn, t0, _OVER_LIMIT)

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    flog(
        f"rw_truncated_window_refused setup: trace_id={ctx.trace_id} "
        f"seeded={_OVER_LIMIT} datasource={_DS_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, count_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import (
        RestoreCutoff, CutoffType, MAX_EVENTS_PER_RESTORE,
    )
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, ctx.data["t0"].isoformat(), inclusive=True,
    )

    flog(f"rw_truncated_window_refused run start: trace_id={ctx.trace_id}", "INFO")

    ctx.data["counted"] = count_events_after_cutoff(
        conn, _DS_FP, cutoff, trace_id=ctx.trace_id,
    )
    events = fetch_events_after_cutoff(
        conn, _DS_FP, cutoff, trace_id=ctx.trace_id,
    )
    ctx.data["fetched"] = len(events)
    ctx.data["max_per_restore"] = MAX_EVENTS_PER_RESTORE
    # The dropped rows must be the OLDEST: the newest event must survive
    # and the oldest must not.
    created = sorted(e.created_at for e in events)
    ctx.data["oldest_kept"] = created[0] if created else None
    ctx.data["expected_oldest"] = (
        ctx.data["t0"] + timedelta(seconds=10)
    ).isoformat()

    flog(
        f"rw_truncated_window_refused run end: trace_id={ctx.trace_id} "
        f"counted={ctx.data['counted']} fetched={ctx.data['fetched']} "
        f"oldest_kept={ctx.data['oldest_kept']}",
        "INFO",
    )


_DIALOG = Path("recover_dialog.py")
_GUARD = "_refuse_truncated_window"


def _handler_guarded(source: str, handler: str) -> tuple:
    """True when *handler* calls the guard before collapsing the window."""
    start = source.find(f"def {handler}(")
    if start < 0:
        return False, f"{handler} not found in {_DIALOG}"
    collapse = source.find("collapse_rewind_events_with_stats(events)", start)
    if collapse < 0:
        return False, f"{handler} does not collapse events (shape changed)"
    body = source[start:collapse]
    if _GUARD not in body:
        return False, (
            f"{handler} collapses the fetched window without calling "
            f"{_GUARD} first: a truncated window is replayed as if complete"
        )
    return True, f"{handler} calls {_GUARD} before collapsing"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    counted = ctx.data.get("counted")
    fetched = ctx.data.get("fetched")
    cap = ctx.data.get("max_per_restore")

    # ===== The truncation itself ========================================
    out.append((
        "count_reports_true_total",
        counted == _OVER_LIMIT,
        f"counted={counted} expected={_OVER_LIMIT}",
    ))
    out.append((
        "fetch_capped_at_limit",
        fetched == cap,
        f"fetched={fetched} expected={cap} (MAX_EVENTS_PER_RESTORE)",
    ))
    out.append((
        "shortfall_is_detectable",
        fetched is not None and counted is not None
        and fetched >= cap and counted > fetched,
        f"counted={counted} fetched={fetched} cap={cap}: the guard keys on "
        f"'cap reached AND fewer rows than exist', which must hold here",
    ))
    out.append((
        "dropped_rows_are_the_oldest",
        ctx.data.get("oldest_kept") != ctx.data.get("expected_oldest"),
        f"oldest_kept={ctx.data.get('oldest_kept')} "
        f"seeded_oldest={ctx.data.get('expected_oldest')}: the cap keeps the "
        f"NEWEST rows, so the events defining the cutoff state are the ones "
        f"lost -- this is why a truncated replay cannot be allowed",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"fetch_events_after_cutoff:.*status=truncated",
        name="truncation_logged",
        min_count=1,
    ))

    # ===== The consumer must refuse =====================================
    full = _PLUGIN_ROOT / _DIALOG
    if not full.is_file():
        out.append(("dialog_source_readable", False, f"missing {_DIALOG}"))
        return out
    source = full.read_text(encoding="utf-8", errors="replace")

    out.append((
        "guard_defined",
        bool(re.search(rf"def\s+{_GUARD}\b", source)),
        f"{_DIALOG} must define {_GUARD}",
    ))
    for handler in ("_on_version_fetch_done", "_on_post_undo_fetch_done"):
        ok, msg = _handler_guarded(source, handler)
        out.append((f"guard_before_collapse__{handler}", ok, msg))

    out.append((
        "guard_uses_the_fetch_total",
        bool(re.search(r"_version_fetch_total", source)),
        "the true total from count_ready must be stashed and compared",
    ))

    # ===== Trace propagation ============================================
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_truncated_window_refused.*trace_id={ctx.trace_id}",
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
