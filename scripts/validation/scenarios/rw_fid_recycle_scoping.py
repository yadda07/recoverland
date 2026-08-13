"""rw_fid_recycle_scoping - RecoverLand validation runtime.

Invariant: I-3 (entity identity) applied to the FID-recycle pre-pass.
Root cause: `_detect_fid_recycle` ran a per-fingerprint state machine over
the WHOLE fetched window, ignoring both the layer an event belongs to and
whether the event still describes the live data.

A "recycle split" rewrites `entity_fingerprint` to ``fp@<eid>``, which cuts
one entity's history into two dedup buckets. When the split is wrong, the
older half stops being compensated: the rewind reports success and leaves
that feature at a post-cutoff state. Two ways to trigger a wrong split:

A. Cross-layer contamination.
   `fid:5` designates a different feature in every layer, but the state
   machine keyed on the fingerprint alone. A rewind covering several layers
   feeds one concatenated list (`version_fetch_thread._run_fetch` extends
   over every fingerprint, `_on_version_fetch_done` collapses the lot), so
   a DELETE of fid 5 in layer B closed the lifeline of fid 5 in layer A,
   and A's next event was recorded as a recycle.

B. Phantom recycle after a restore.
   A DELETE that a previous rewind has already undone did not close
   anything -- the feature is back in the layer. Counting it (or counting
   the trace's compensating INSERT) made a later edit of that same feature
   look like an FID recycle.

Both are silent: a split is logged as a normal `fid_recycle_detected` line,
indistinguishable from a real one.

Scenario layout (pure dedup, no SQLite, no QGIS):
    A: INSERT(A,fid:5) / DELETE(B,fid:5) / UPDATE(A,fid:5)
       -> no split at all, and layer A keeps one single chain
    B: DELETE(X,fid:7) / trace INSERT ref=DELETE / UPDATE(X,fid:7)
       -> no split: the DELETE was undone, the UPDATE hits the same entity
    C: control -- a REAL recycle must still be detected
       INSERT(X,fid:9) / DELETE(X,fid:9) / INSERT(X,fid:9)

Pre-patch verdict: FAIL.
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_fid_recycle_scoping"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_A = "rw_frs_layer_a"
_DS_B = "rw_frs_layer_b"
_DS_X = "rw_frs_layer_x"


def _event(eid, ts, op, ds_fp, entity_fp, restored_from=None):
    from recoverland.core.audit_backend import AuditEvent

    fid = int(entity_fp.split(":")[1])
    if restored_from is not None:
        attrs = {"_restore_ref": restored_from}
    else:
        attrs = {"changed_only": {"v": {"old": "a", "new": "b"}}}
    return AuditEvent(
        event_id=eid,
        project_fingerprint="rw_frs_project",
        datasource_fingerprint=ds_fp,
        layer_id_snapshot=ds_fp,
        layer_name_snapshot=ds_fp,
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type=op,
        attributes_json=json.dumps(attrs),
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=None,
        user_name="tester",
        session_id=None,
        created_at=ts.isoformat(),
        restored_from_event_id=restored_from,
        entity_fingerprint=entity_fp,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def setup(ctx):
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)

    def at(sec):
        return t0 + timedelta(seconds=sec)

    # --- A: two layers, same FID -----------------------------------------
    ctx.data["case_a"] = [
        _event(3, at(30), "UPDATE", _DS_A, "fid:5"),
        _event(2, at(20), "DELETE", _DS_B, "fid:5"),
        _event(1, at(10), "INSERT", _DS_A, "fid:5"),
    ]

    # --- B: a DELETE already undone by a previous rewind -----------------
    ctx.data["case_b"] = [
        _event(200, at(200), "UPDATE", _DS_X, "fid:7"),
        _event(101, at(110), "INSERT", _DS_X, "fid:7", restored_from=100),
        _event(100, at(100), "DELETE", _DS_X, "fid:7"),
    ]

    # --- C: a genuine recycle, must still be split -----------------------
    ctx.data["case_c"] = [
        _event(303, at(330), "INSERT", _DS_X, "fid:9"),
        _event(302, at(320), "DELETE", _DS_X, "fid:9"),
        _event(301, at(310), "INSERT", _DS_X, "fid:9"),
    ]

    flog(
        f"rw_fid_recycle_scoping setup: trace_id={ctx.trace_id} "
        f"layers={_DS_A},{_DS_B},{_DS_X}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.rewind_dedup import (
        _detect_fid_recycle, collapse_rewind_events_with_stats,
    )
    from recoverland.core.logger import flog

    flog(f"rw_fid_recycle_scoping run start: trace_id={ctx.trace_id}", "INFO")

    for case in ("case_a", "case_b", "case_c"):
        events = ctx.data[case]
        active, stats = collapse_rewind_events_with_stats(events)
        ctx.data[f"{case}_active"] = sorted(
            e.event_id for e in active if e.event_id is not None)
        ctx.data[f"{case}_fps"] = sorted({
            (e.datasource_fingerprint, e.entity_fingerprint) for e in active})
        ctx.data[f"{case}_stats"] = dict(stats)

    # Direct look at the pre-pass for the control case.
    splits_c, _ = _detect_fid_recycle(ctx.data["case_c"])
    ctx.data["case_c_split_keys"] = sorted(str(k) for k in splits_c)

    flog(
        f"rw_fid_recycle_scoping run end: trace_id={ctx.trace_id} "
        f"a={ctx.data['case_a_active']} b={ctx.data['case_b_active']} "
        f"c={ctx.data['case_c_active']} c_splits={ctx.data['case_c_split_keys']}",
        "INFO",
    )


def _no_split_marker(fps) -> bool:
    return not any("@" in fp for _ds, fp in fps)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    # ===== A: no cross-layer contamination ==============================
    fps_a = ctx.data.get("case_a_fps") or []
    out.append((
        "case_a_no_split_across_layers",
        _no_split_marker(fps_a),
        f"active fingerprints={fps_a}: a DELETE of fid:5 in layer B must "
        f"not close the lifeline of fid:5 in layer A, so no '@<eid>' split "
        f"marker may appear",
    ))
    out.append((
        "case_a_layer_a_stays_one_entity",
        len({fp for ds, fp in fps_a if ds == _DS_A}) <= 1,
        f"active fingerprints={fps_a}: layer A's events must stay in a "
        f"single dedup bucket, otherwise half its chain is never compensated",
    ))

    # ===== B: no phantom recycle after a restore ========================
    fps_b = ctx.data.get("case_b_fps") or []
    out.append((
        "case_b_no_phantom_split",
        _no_split_marker(fps_b),
        f"active fingerprints={fps_b}: the DELETE was undone by the trace, "
        f"so the later UPDATE hits the very same feature -- not a recycle",
    ))
    out.append((
        "case_b_new_edit_stays_active",
        ctx.data.get("case_b_active") == [200],
        f"case_b_active={ctx.data.get('case_b_active')} expected=[200] "
        f"(the DELETE is already compensated; the post-restore UPDATE is not)",
    ))

    # ===== C: a real recycle is still caught ============================
    out.append((
        "case_c_real_recycle_still_detected",
        len(ctx.data.get("case_c_split_keys") or []) == 1,
        f"split keys={ctx.data.get('case_c_split_keys')} expected exactly 1: "
        f"INSERT/DELETE/INSERT on one layer with no trace IS an FID recycle "
        f"and must keep being split",
    ))
    out.append((
        "case_c_split_key_is_layer_scoped",
        all("," in k or "(" in k for k in (ctx.data.get("case_c_split_keys") or [])),
        f"split keys={ctx.data.get('case_c_split_keys')}: the state machine "
        f"must key on (datasource, entity), not on the fingerprint alone",
    ))
    out.append((
        "case_c_only_recycled_insert_survives",
        ctx.data.get("case_c_active") == [303],
        f"case_c_active={ctx.data.get('case_c_active')} expected=[303] "
        f"(first lifetime collapses to a no-op, the recycled INSERT remains)",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rw_fid_recycle_scoping.*trace_id={ctx.trace_id}",
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
