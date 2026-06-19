"""h_v2_fuse_long_chain - SESSION_REWIND 17.2 H-V2 non-regression.

Hypothesis (H-V2):
    `_fuse_long_chain` (`core/rewind_dedup.py`) collapses a chain of >10
    UPDATEs on the same feature into a single synthetic event whose
    `feature_identity_json`, `entity_fingerprint` and `new_geometry_wkb`
    come from the newest event, while `geometry_wkb` (the OLD state)
    comes from the oldest. If anything in this clone path drops the
    NEW state, the rewind compensates the wrong delta.

Verdict produced as the final log line:
    hypothesis_h_v2: status=<VALIDATED|FALSIFIED|UNREPRODUCED> reason=<...>

VALIDATED   = bug reproduced (synthetic event lost OLD or NEW state).
FALSIFIED   = synthetic event preserves OLD-from-oldest and NEW-from-newest.
UNREPRODUCED= chain not long enough to trigger _fuse_long_chain.

BL-RW-P3-19 / CR-10.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "h_v2_fuse_long_chain"
INVARIANT = "BL-RW-P3-19"
EXPECTED_SIGNATURE = r"hypothesis_h_v2: status=(VALIDATED|FALSIFIED|UNREPRODUCED)"

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_DATASOURCE_FP = "h_v2_test_datasource"
_PROJECT_FP = "h_v2_test_project"
_TARGET_FP = "fid:7"
_CHAIN_LEN = 12  # > _MAX_CHAIN (=10) so fusion fires.


def _t_iso(t: datetime) -> str:
    return t.replace(microsecond=0).isoformat(sep=" ")


def _seed(conn: sqlite3.Connection, t0: datetime) -> dict:
    """Seed: 1 INSERT + 12 UPDATEs on the same fp, attribute increments."""
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )

    eids = []
    cur = conn.execute(
        sql,
        (
            _PROJECT_FP, _DATASOURCE_FP, "lyr_test", "test_layer", "ogr",
            json.dumps({"label": "create"}),
            "INSERT",
            json.dumps({"name": "v0"}),
            None, "NoGeometry", "EPSG:4326",
            json.dumps([{"name": "name", "type": "string"}]),
            "tester", None,
            _t_iso(t0 + timedelta(seconds=1)),
            None, _TARGET_FP, 2, None, None,
        ),
    )
    eids.append(cur.lastrowid)

    for i in range(1, _CHAIN_LEN + 1):
        cur = conn.execute(
            sql,
            (
                _PROJECT_FP, _DATASOURCE_FP, "lyr_test", "test_layer", "ogr",
                json.dumps({"label": f"upd{i}"}),
                "UPDATE",
                json.dumps({"name": f"v{i}", "old_name": f"v{i-1}"}),
                None, "NoGeometry", "EPSG:4326",
                json.dumps([{"name": "name", "type": "string"}]),
                "tester", None,
                _t_iso(t0 + timedelta(seconds=10 + i)),
                None, _TARGET_FP, 2, None, None,
            ),
        )
        eids.append(cur.lastrowid)
    conn.commit()
    return {"eids": eids, "fp": _TARGET_FP}


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
        f"h_v2 setup: trace_id={ctx.trace_id} "
        f"chain_len={_CHAIN_LEN} fp={seed['fp']} "
        f"eid_first={seed['eids'][0]} eid_last={seed['eids'][-1]}",
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
    ctx.data["dedup_stats"] = dict(stats)
    ctx.data["active_event_count"] = len(active)

    fused_signaled = stats.get("fused_entities", 0) > 0
    ctx.data["fused_signaled"] = fused_signaled

    seed = ctx.data["seed"]
    expected_count = _CHAIN_LEN + 1
    if ctx.data["fetched_event_count"] != expected_count:
        verdict = "UNREPRODUCED"
        reason = (
            f"fetched={ctx.data['fetched_event_count']} "
            f"expected={expected_count} (seed missed)"
        )
    elif not fused_signaled:
        verdict = "UNREPRODUCED"
        reason = (
            f"_fuse_long_chain did not fire "
            f"(stats={ctx.data['dedup_stats']}); chain too short."
        )
    else:
        # Fusion fired. Verify the synthetic event preserves identity.
        synthetic_attrs = []
        synthetic_fps = set()
        for ev in active:
            attrs = ev.attributes_json or "{}"
            try:
                synthetic_attrs.append(json.loads(attrs))
            except Exception:
                synthetic_attrs.append({})
            synthetic_fps.add(ev.entity_fingerprint)

        # Newest event has name="v12"; oldest UPDATE has name="v1".
        names = [a.get("name") for a in synthetic_attrs]
        kept_newest = "v12" in names
        kept_oldest = "v1" in names or any(
            a.get("old_name") == "v0" for a in synthetic_attrs
        )
        kept_fp = seed["fp"] in synthetic_fps

        if kept_newest and kept_oldest and kept_fp:
            verdict = "FALSIFIED"
            reason = (
                f"fusion preserved newest name=v12, oldest delta and fp "
                f"={seed['fp']} (active_count={len(active)} "
                f"names={names})."
            )
        else:
            verdict = "VALIDATED"
            reason = (
                f"fusion lost a delta: kept_newest={kept_newest} "
                f"kept_oldest={kept_oldest} kept_fp={kept_fp} "
                f"names={names} fps={sorted(synthetic_fps)}"
            )

    ctx.data["verdict"] = verdict
    ctx.data["reason"] = reason
    flog(
        f"hypothesis_h_v2: status={verdict} reason={reason} "
        f"trace_id={ctx.trace_id}",
        "INFO",
    )


_VERDICT_RE = re.compile(
    r"hypothesis_h_v2:\s+status=(VALIDATED|FALSIFIED|UNREPRODUCED)\s+reason="
)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    out.append((
        "fetched_chain_complete",
        ctx.data.get("fetched_event_count") == _CHAIN_LEN + 1,
        f"fetched={ctx.data.get('fetched_event_count')} "
        f"expected={_CHAIN_LEN + 1}",
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
        ctx.records, rf"h_v2 setup:\s+trace_id={ctx.trace_id}",
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
