"""i9_cutoff_inclusivity — RecoverLand validation runtime.

Invariant: I-9 (cutoff temporal inclusivity, charter §5).
Backlog item: BL-RW-P0-01.
Root cause: CR-2 (cutoff semantics).
Charter signature expected (after patch):
    fetch_events_after_cutoff: inclusive=True ... n_events=N

Demonstrates and proves the H-S3 hypothesis from SESSION_REWIND.md §17.1:
events whose `created_at` falls exactly on the cutoff second are
silently dropped when callers instantiate `RestoreCutoff(... inclusive=False)`,
which is the current default at all call sites.

Scenario:
    1. Create an in-memory SQLite journal with the RecoverLand schema.
    2. Insert 6 events distributed across 4 timestamps:
        T-2s   : 2 events  (clearly before cutoff)
        T      : 3 events  (exactly at the boundary)
        T+1s   : 1 event   (clearly after cutoff)
    3. Build a cutoff at exactly T with both inclusive=False and True.
    4. Fetch events for both cases through the production code path
       (core.event_stream_repository.fetch_events_after_cutoff).
    5. Inspect the source of the two production call sites
       (recover_dialog.py and scripts/validate_rewind.py) to detect
       the pathological default `inclusive=False`.
    6. Inspect the produced log for the structured signature that the
       patch is expected to emit.

This scenario does NOT touch any QGIS state. It runs equally from the
QGIS Python console (preferred) or from a plain Python interpreter,
because event_stream_repository has no QGIS dependency.

To run from the QGIS Python console:

    >>> import sys
    >>> from pathlib import Path
    >>> P = Path(r'C:\\Users\\yadda\\AppData\\Roaming\\QGIS\\QGIS4'
    ...          r'\\profiles\\default\\python\\plugins')
    >>> sys.path.insert(0, str(P))
    >>> from scripts.validation.runner import run_scenario
    >>> run_scenario(str(P / 'recoverland/scripts/validation/scenarios/'
    ...                       'i9_cutoff_inclusivity.py'))
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "i9_cutoff_inclusivity"
INVARIANT = "I-9"
EXPECTED_SIGNATURE = r"fetch_events_after_cutoff:.*inclusive=(True|False).*n_events=\d+"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DATASOURCE_FP = "i9_test_datasource"
_PROJECT_FP = "i9_test_project"


def _t_iso(t: datetime) -> str:
    """Format a datetime to RecoverLand's stored format (second precision)."""
    return t.replace(microsecond=0).isoformat(sep=" ")


def _seed_events(conn: sqlite3.Connection, t0: datetime) -> dict:
    """Insert 6 events around t0. Returns the timestamp map for later assertions."""
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    plan = [
        (t0 - timedelta(seconds=2), "INSERT", "before-1"),
        (t0 - timedelta(seconds=2), "UPDATE", "before-2"),
        (t0,                        "INSERT", "boundary-1"),
        (t0,                        "UPDATE", "boundary-2"),
        (t0,                        "DELETE", "boundary-3"),
        (t0 + timedelta(seconds=1), "INSERT", "after-1"),
    ]
    for ts, op, label in plan:
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
                _t_iso(ts), None,
                "fid:1", 2, None, None,
            ),
        )
    conn.commit()
    return {
        "before": _t_iso(t0 - timedelta(seconds=2)),
        "boundary": _t_iso(t0),
        "after": _t_iso(t0 + timedelta(seconds=1)),
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
    timestamps = _seed_events(conn, t0)
    ctx.data["timestamps"] = timestamps

    flog(
        "i9_cutoff_inclusivity setup: trace_id={tid} t0={t0} "
        "events_inserted=6 datasource={ds}".format(
            tid=ctx.trace_id, t0=ctx.data["t0_iso"], ds=_DATASOURCE_FP),
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, count_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    cutoff_iso = ctx.data["t0_iso"]
    flog(
        "i9_cutoff_inclusivity run start: trace_id={tid} cutoff={c}".format(
            tid=ctx.trace_id, c=cutoff_iso),
        "INFO",
    )

    cutoff_strict = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=False)
    cutoff_inclusive = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)

    n_strict = count_events_after_cutoff(conn, _DATASOURCE_FP, cutoff_strict,
                                         trace_id=ctx.trace_id)
    n_inclusive = count_events_after_cutoff(conn, _DATASOURCE_FP, cutoff_inclusive,
                                            trace_id=ctx.trace_id)

    events_strict = fetch_events_after_cutoff(conn, _DATASOURCE_FP, cutoff_strict,
                                              trace_id=ctx.trace_id)
    events_inclusive = fetch_events_after_cutoff(conn, _DATASOURCE_FP,
                                                 cutoff_inclusive,
                                                 trace_id=ctx.trace_id)

    ctx.data["n_strict"] = n_strict
    ctx.data["n_inclusive"] = n_inclusive
    ctx.data["events_strict"] = events_strict
    ctx.data["events_inclusive"] = events_inclusive

    flog(
        "i9_cutoff_inclusivity run end: trace_id={tid} "
        "n_strict={ns} n_inclusive={ni} delta={d}".format(
            tid=ctx.trace_id, ns=n_strict, ni=n_inclusive,
            d=n_inclusive - n_strict),
        "INFO",
    )


def _read_call_site_default(file_relpath: str, line_hint: int,
                            search_window: int = 3) -> str | None:
    """Return the line that contains 'inclusive=...' near line_hint."""
    path = _PLUGIN_ROOT / file_relpath
    if not path.is_file():
        return None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    start = max(0, line_hint - 1 - search_window)
    end = min(len(lines), line_hint - 1 + search_window + 1)
    for line in lines[start:end]:
        if "inclusive=" in line and "RestoreCutoff" in line:
            return line.strip()
    return None


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    out.append((
        "fetch_strict_excludes_boundary",
        ctx.data["n_strict"] == 1,
        f"n_strict={ctx.data['n_strict']} expected=1 (only T+1 event)",
    ))
    out.append((
        "fetch_inclusive_includes_boundary",
        ctx.data["n_inclusive"] == 4,
        f"n_inclusive={ctx.data['n_inclusive']} expected=4 (T x3 + T+1)",
    ))
    delta = ctx.data["n_inclusive"] - ctx.data["n_strict"]
    out.append((
        "boundary_delta_equals_3",
        delta == 3,
        f"delta={delta} expected=3 (3 events on the second boundary)",
    ))

    ui_line = _read_call_site_default("recover_dialog.py", 1688)
    out.append((
        "recover_dialog_uses_inclusive_true",
        ui_line is not None and "inclusive=True" in ui_line,
        f"recover_dialog.py:~1688 -> {ui_line!r}",
    ))

    auto_line = _read_call_site_default("scripts/validate_rewind.py", 183)
    out.append((
        "validate_rewind_uses_inclusive_true",
        auto_line is not None and "inclusive=True" in auto_line,
        f"scripts/validate_rewind.py:~183 -> {auto_line!r}",
    ))

    contracts = (_PLUGIN_ROOT / "core" / "restore_contracts.py").read_text(
        encoding="utf-8", errors="replace")
    has_default = bool(re.search(r"inclusive\s*:\s*bool\s*=\s*True", contracts))
    out.append((
        "restore_cutoff_default_is_true",
        has_default,
        f"core/restore_contracts.py default for 'inclusive' = {has_default}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        EXPECTED_SIGNATURE,
        name="fetch_signature_in_log",
        min_count=2,
    ))

    # The default-True invariant is covered by `restore_cutoff_default_is_true`
    # above, which inspects the contract source. Asserting on the order of
    # fetch calls would couple the test to internal call sequencing that is
    # not an invariant of I-9.

    out.append(assert_log_contains(
        ctx.records,
        rf"i9_cutoff_inclusivity.*trace_id={ctx.trace_id}",
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
