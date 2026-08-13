"""rw_fuse_attrs_completeness - RecoverLand validation runtime.

Invariant: I-8 (a rewind returns the entity to its exact cutoff state).
Root cause: chain fusion in `core.rewind_dedup` rebuilds the cutoff state
from ONE event of the chain, so every field touched by the other events of
the chain is silently lost.

Two fusion paths, two holes
---------------------------
A. `_fuse_update_delete` (pre-existing entity UPDATEd then DELETEd).
   The synthetic DELETE is built as::

       old_attrs = reconstruct_attributes(oldest)      # oldest is an UPDATE
       {"all_attributes": old_attrs}

   `reconstruct_attributes` on a changed_only payload returns ONLY the
   fields that this UPDATE touched. The compensation for a DELETE is a
   full re-INSERT (`restore_executor._buffer_insert`), which sets only the
   fields present in `all_attributes` -- every other field of the feature
   comes back NULL. Editing one attribute then deleting the feature is an
   ordinary sequence, so this destroys real data on an ordinary rewind.
   The full state IS available: the DELETE event carries `all_attributes`
   (the state at deletion); rolling the UPDATE deltas back over it
   reconstructs the complete cutoff state.

B. `_fuse_long_chain` (more than _MAX_CHAIN=10 events on one entity).
   For an UPDATE...UPDATE chain the synthetic keeps the OLDEST event's
   delta only, so the fields modified by events 2..n-1 are never reverted:
   the rewind reports success while leaving those fields at their
   post-cutoff values. The 11th edit of a feature is what makes the
   difference -- the same scenario with 10 edits restores correctly.

Both are silent: no WARNING, no partial status, no user-visible sign.

Scenario layout (pure dedup, no SQLite, no QGIS):
    A: [UPDATE status A->B] then [DELETE full snapshot] -> fuse
       expect the synthetic to carry name/status/code/note at cutoff
    B: 11 UPDATEs touching f_old (oldest), f_mid (middle), f_new (newest)
       expect the synthetic delta to revert all three

Pre-patch verdict: FAIL.
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_fuse_attrs_completeness"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_FP = "rw_fac_datasource"
_PROJ_FP = "rw_fac_project"


def _t(t0: datetime, secs: int) -> str:
    return (t0 + timedelta(seconds=secs)).isoformat()


def _event(eid, ts, op, attrs, entity_fp, geom=None):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=eid,
        project_fingerprint=_PROJ_FP,
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_fac",
        layer_name_snapshot="fac_layer",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": int(entity_fp.split(":")[1])}),
        operation_type=op,
        attributes_json=json.dumps(attrs, ensure_ascii=False),
        geometry_wkb=geom,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=None,
        user_name="tester",
        session_id=None,
        created_at=ts,
        restored_from_event_id=None,
        entity_fingerprint=entity_fp,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


# Full state of the feature at deletion time (case A).
_STATE_AT_DELETE = {
    "name": "N1", "status": "B", "code": "C1", "note": "X1",
}
# The cutoff state = same feature before the status edit.
_STATE_AT_CUTOFF = {
    "name": "N1", "status": "A", "code": "C1", "note": "X1",
}

_GEOM_AT_CUTOFF = b"\x01\x01\x00\x00\x00cutoff__geom"
_GEOM_AT_DELETE = b"\x01\x01\x00\x00\x00delete__geom"


def setup(ctx):
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)

    # --- Case A: UPDATE (attrs + geom) then DELETE ----------------------
    a_update = _event(
        1, _t(t0, 10), "UPDATE",
        {"changed_only": {"status": {"old": "A", "new": "B"}}},
        "fid:11", geom=_GEOM_AT_CUTOFF,
    )
    a_delete = _event(
        2, _t(t0, 20), "DELETE",
        {"all_attributes": dict(_STATE_AT_DELETE)},
        "fid:11", geom=_GEOM_AT_DELETE,
    )
    # Fetch order is DESC (newest first).
    ctx.data["chain_a"] = [a_delete, a_update]

    # --- Case B: 11 UPDATEs on one entity -------------------------------
    chain_b_asc = []
    # oldest: touches f_old
    chain_b_asc.append(_event(
        101, _t(t0, 10), "UPDATE",
        {"changed_only": {"f_old": {"old": "old0", "new": "old1"}}},
        "fid:22",
    ))
    # middle: touches f_mid (event 6 of 11)
    for i in range(1, 10):
        if i == 5:
            payload = {"changed_only": {"f_mid": {"old": "mid0", "new": "mid1"}}}
        else:
            payload = {"changed_only": {
                "f_pad": {"old": f"pad{i - 1}", "new": f"pad{i}"}}}
        chain_b_asc.append(_event(
            101 + i, _t(t0, 10 + i * 10), "UPDATE", payload, "fid:22",
        ))
    # newest: touches f_new
    chain_b_asc.append(_event(
        111, _t(t0, 120), "UPDATE",
        {"changed_only": {"f_new": {"old": "new0", "new": "new1"}}},
        "fid:22",
    ))
    ctx.data["chain_b"] = list(reversed(chain_b_asc))  # DESC
    ctx.data["chain_b_len"] = len(chain_b_asc)

    flog(
        f"rw_fuse_attrs_completeness setup: trace_id={ctx.trace_id} "
        f"chain_a={len(ctx.data['chain_a'])} chain_b={len(chain_b_asc)} "
        f"datasource={_DS_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.search_service import reconstruct_attributes
    from recoverland.core.serialization import extract_delta_old
    from recoverland.core.logger import flog

    flog(f"rw_fuse_attrs_completeness run start: trace_id={ctx.trace_id}", "INFO")

    # --- Case A ----------------------------------------------------------
    active_a, stats_a = collapse_rewind_events_with_stats(ctx.data["chain_a"])
    ctx.data["active_a_ops"] = [e.operation_type for e in active_a]
    if len(active_a) == 1:
        syn = active_a[0]
        ctx.data["a_restored_attrs"] = reconstruct_attributes(syn)
        ctx.data["a_restored_geom"] = syn.geometry_wkb
    else:
        ctx.data["a_restored_attrs"] = None
        ctx.data["a_restored_geom"] = None
    ctx.data["stats_a"] = dict(stats_a)

    # --- Case B ----------------------------------------------------------
    active_b, stats_b = collapse_rewind_events_with_stats(ctx.data["chain_b"])
    ctx.data["active_b_count"] = len(active_b)
    reverted = {}
    for ev in active_b:
        try:
            payload = json.loads(ev.attributes_json)
        except (ValueError, TypeError):
            continue
        for field, val in (payload.get("changed_only") or {}).items():
            # Oldest event wins: the compensations are applied newest first,
            # so the value the feature ends up with is the oldest OLD.
            reverted[field] = extract_delta_old(val)
    ctx.data["b_reverted"] = reverted
    ctx.data["stats_b"] = dict(stats_b)

    flog(
        f"rw_fuse_attrs_completeness run end: trace_id={ctx.trace_id} "
        f"a_attrs={ctx.data['a_restored_attrs']} "
        f"b_reverted={reverted} "
        f"stats_a={dict(stats_a)} stats_b={dict(stats_b)}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    # ===== Case A: fusion happened as designed ==========================
    out.append((
        "a_fused_into_single_delete",
        ctx.data.get("active_a_ops") == ["DELETE"],
        f"active_a_ops={ctx.data.get('active_a_ops')} expected=['DELETE'] "
        f"(UPDATE->DELETE chain fuses into one synthetic DELETE)",
    ))

    # ===== Case A: the whole feature must come back =====================
    attrs_a = ctx.data.get("a_restored_attrs")
    out.append((
        "a_restores_every_field",
        attrs_a == _STATE_AT_CUTOFF,
        f"restored={attrs_a} expected={_STATE_AT_CUTOFF}. The synthetic "
        f"DELETE feeds _buffer_insert, which only writes the fields present "
        f"in all_attributes: any missing field comes back NULL on the "
        f"re-inserted feature.",
    ))
    missing_a = ([] if not isinstance(attrs_a, dict)
                 else sorted(set(_STATE_AT_CUTOFF) - set(attrs_a)))
    out.append((
        "a_no_field_dropped",
        not missing_a,
        f"fields lost by the fusion: {missing_a}",
    ))
    out.append((
        "a_geometry_is_cutoff_geometry",
        ctx.data.get("a_restored_geom") == _GEOM_AT_CUTOFF,
        f"geom={ctx.data.get('a_restored_geom')!r} "
        f"expected={_GEOM_AT_CUTOFF!r} (old geometry of the oldest UPDATE, "
        f"not the geometry the feature had when it was deleted)",
    ))

    # ===== Case B: long chain keeps every field it must revert ==========
    out.append((
        "b_chain_was_fused",
        (ctx.data.get("chain_b_len") or 0) > 10
        and (ctx.data.get("active_b_count") or 0) < ctx.data.get("chain_b_len"),
        f"chain_len={ctx.data.get('chain_b_len')} "
        f"active={ctx.data.get('active_b_count')} "
        f"(fusion must trigger beyond _MAX_CHAIN=10)",
    ))
    reverted = ctx.data.get("b_reverted") or {}
    expected_b = {"f_old": "old0", "f_mid": "mid0", "f_new": "new0"}
    for field, value in expected_b.items():
        out.append((
            f"b_reverts_{field}",
            reverted.get(field) == value,
            f"{field}={reverted.get(field)!r} expected={value!r} "
            f"(field edited inside a >10 chain; a fusion that keeps only "
            f"one event leaves it at its post-cutoff value) "
            f"reverted={reverted}",
        ))

    # ===== Trace propagation ============================================
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_fuse_attrs_completeness.*trace_id={ctx.trace_id}",
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
