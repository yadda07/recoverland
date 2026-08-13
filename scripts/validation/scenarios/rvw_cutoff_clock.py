"""rvw_cutoff_clock - RecoverLand validation runtime.

Invariant: Review and Rewind must denote the SAME instant for the same
visible date.
Root cause: the Review chain anchored its timeline on the local wall clock
while every ``created_at`` in the journal is UTC.

The hole this scenario proves
-----------------------------
`edit_tracker` writes ``created_at = datetime.now(timezone.utc).isoformat()``.
The Rewind path converts explicitly before querying
(``cutoff_dt.toUTC().toString(...)``, recover_dialog). The Review chain did
not: `canvas_date_bar`, `temporal_timeline_widget` and
`snapshot_rebuild_worker` all took their "now" from ``datetime.now()`` /
``date.today()``, and the resulting naive string went straight into
``created_at <= ?`` and into ``reconstruct_snapshot_at`` (which treats a naive
datetime as UTC).

So the timeline scale and the data sat on two clocks, offset by the machine's
UTC offset:

  * east of Greenwich the lens looked into the future and showed edits the
    user had not scrolled to yet;
  * west of it the lens lagged, hiding the most recent hours -- and the
    Review "state at T" no longer matched what a Rewind to the same visible
    date would produce. Checking a state in Review before applying it with
    Rewind is the whole point of the module, so this quietly broke its
    reason to exist.

The fix routes the Review chain through ``time_format.journal_now`` /
``journal_today`` -- the journal's own clock -- so scale and data agree.

Pre-patch verdict: FAIL (measured on a UTC+02:00 machine).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rvw_cutoff_clock"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_FP = "rvw_clock_datasource"
_REVIEW_CHAIN = (
    Path("widgets/canvas_date_bar.py"),
    Path("widgets/temporal_timeline_widget.py"),
    Path("widgets/snapshot_rebuild_worker.py"),
)
_LOCAL_CLOCK = re.compile(r"(?<![\w.])(?:datetime\.now\(\)|date\.today\(\))")


def setup(ctx):
    from recoverland.core.sqlite_schema import (
        initialize_schema, AUDIT_EVENT_INSERT_SQL,
        AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.logger import flog

    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)

    # An edit committed a minute ago, stamped exactly as edit_tracker does.
    # Not "right now": created_at carries microseconds while a cutoff is
    # second-granular, so an event of the SAME second sorts after it and is
    # deliberately treated as posterior -- the mirror of the rewind cutoff
    # being inclusive. That boundary is not what this scenario is about.
    created_at = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    conn.execute(
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")",
        (
            "rvw_clock_project", _DS_FP, "lyr", "layer", "ogr",
            json.dumps({"fid": 1}), "UPDATE",
            json.dumps({"changed_only": {"v": {"old": "a", "new": "b"}}}),
            None, "NoGeometry", "EPSG:4326", None,
            "tester", None, created_at, None, "fid:1", 2, None, None,
        ),
    )
    conn.commit()

    ctx.data["conn"] = conn
    ctx.data["created_at"] = created_at
    offset = datetime.now().astimezone().utcoffset() or timedelta(0)
    ctx.data["utc_offset_hours"] = offset.total_seconds() / 3600.0

    flog(
        f"rvw_cutoff_clock setup: trace_id={ctx.trace_id} "
        f"created_at={created_at} utc_offset_h={ctx.data['utc_offset_hours']} "
        f"datasource={_DS_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.time_format import journal_now, journal_today
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]

    flog(f"rvw_cutoff_clock run start: trace_id={ctx.trace_id}", "INFO")

    fmt = "%Y-%m-%dT%H:%M:%S"
    # What the date bar emits today, and what it emitted before the fix.
    journal_cutoff = journal_now().strftime(fmt)
    local_cutoff = datetime.now().strftime(fmt)
    # What Rewind sends for the very same instant.
    rewind_cutoff = datetime.now(timezone.utc).strftime(fmt)

    def visible(cutoff: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) FROM audit_event "
            "WHERE datasource_fingerprint = ? AND created_at <= ?",
            (_DS_FP, cutoff),
        ).fetchone()
        return row[0] if row else -1

    ctx.data["journal_cutoff"] = journal_cutoff
    ctx.data["local_cutoff"] = local_cutoff
    ctx.data["rewind_cutoff"] = rewind_cutoff
    ctx.data["visible_journal"] = visible(journal_cutoff)
    ctx.data["visible_local"] = visible(local_cutoff)
    ctx.data["journal_today_iso"] = journal_today().isoformat()
    ctx.data["utc_today_iso"] = datetime.now(timezone.utc).date().isoformat()

    flog(
        f"rvw_cutoff_clock run end: trace_id={ctx.trace_id} "
        f"journal_cutoff={journal_cutoff} rewind_cutoff={rewind_cutoff} "
        f"local_cutoff={local_cutoff} "
        f"visible_journal={ctx.data['visible_journal']} "
        f"visible_local={ctx.data['visible_local']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    offset = ctx.data.get("utc_offset_hours")

    # ===== The two modes must agree to the second =======================
    out.append((
        "review_cutoff_equals_rewind_cutoff",
        ctx.data.get("journal_cutoff") == ctx.data.get("rewind_cutoff"),
        f"review={ctx.data.get('journal_cutoff')} "
        f"rewind={ctx.data.get('rewind_cutoff')}: for the same instant both "
        f"modes must produce the same cutoff string, or checking a state in "
        f"Review says nothing about what Rewind will apply",
    ))
    out.append((
        "journal_today_is_utc_today",
        ctx.data.get("journal_today_iso") == ctx.data.get("utc_today_iso"),
        f"journal_today={ctx.data.get('journal_today_iso')} "
        f"utc_today={ctx.data.get('utc_today_iso')}",
    ))

    # ===== The event committed "now" must be visible ====================
    out.append((
        "recent_event_visible_at_review_cutoff",
        ctx.data.get("visible_journal") == 1,
        f"visible={ctx.data.get('visible_journal')} expected=1: an edit "
        f"committed a minute ago must fall inside a cutoff taken now. "
        f"A zero here means the lens is querying on the wrong clock.",
    ))

    # ===== Divergence, only measurable off UTC ==========================
    if offset:
        out.append((
            "local_clock_would_diverge",
            ctx.data.get("local_cutoff") != ctx.data.get("journal_cutoff"),
            f"local={ctx.data.get('local_cutoff')} "
            f"journal={ctx.data.get('journal_cutoff')} at UTC{offset:+g}h: "
            f"this is the gap the Review lens used to query with",
        ))
    else:
        out.append((
            "divergence_check_skipped_on_utc_machine",
            True,
            "machine runs at UTC+0: the two clocks coincide, so this run "
            "cannot exhibit the offset. The source guard below still holds.",
        ))

    # ===== No local clock left anywhere in the Review chain =============
    for rel in _REVIEW_CHAIN:
        full = _PLUGIN_ROOT / rel
        if not full.is_file():
            out.append((f"source__{rel.name}", False, f"missing {rel}"))
            continue
        src = full.read_text(encoding="utf-8", errors="replace")
        hits = sorted({
            src[:m.start()].count("\n") + 1 for m in _LOCAL_CLOCK.finditer(src)
        })
        out.append((
            f"no_local_clock__{rel.name}",
            not hits,
            f"wall-clock call(s) at line(s) {hits}: the Review chain must take "
            f"its 'now' from time_format.journal_now/journal_today, the same "
            f"clock the journal is written with",
        ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rvw_cutoff_clock.*trace_id={ctx.trace_id}",
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
