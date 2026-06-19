"""p_executor_trace_id_proof - runtime proof for BL-RW-P3-17 + BL-RW-P2-14.

Drives `_apply_via_buffer` end-to-end on an in-memory QGIS layer with a
known `trace_id`, then verifies that:

  P3-17: every executor log line emitted during the call carries the
         `[trace_id]` prefix or a `trace_id=<id>` kv field. This covers
         both the human-readable `flog` lines and the structured
         `flog_kv` events (BUF_INS, BUF_DEL, BUF_UPD).

  P2-14: each result dict returned by the three buffer compensators
         (`_buffer_insert`, `_buffer_delete`, `_buffer_update`) carries
         a `status` field in {APPLIED, SKIPPED_IDEMPOTENT,
         SKIPPED_GEOMETRY_DRIFT, FAILED} and a `reason_code` non-empty.

Pre-existing scenarios under `hypotheses/` exercise the dedup pipeline
in pure Python; this one fires the actual editing buffer on a real
QGIS layer to keep the proof close to production behaviour.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

SCENARIO_ID = "p_executor_trace_id_proof"
INVARIANT = "BL-RW-P3-17"
EXPECTED_SIGNATURE = (
    r"event=BUF_(?:INS|DEL|UPD)\s+.*trace_id=(?P<tid>[0-9a-f]+)"
)

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]


_KNOWN_STATUSES = {
    "APPLIED",
    "SKIPPED_IDEMPOTENT",
    "SKIPPED_GEOMETRY_DRIFT",
    "FAILED",
}


def _make_layer():
    from qgis.core import QgsVectorLayer
    uri = "Point?crs=EPSG:4326&field=name:string(40)"
    layer = QgsVectorLayer(uri, "p_exec_proof_mem", "memory")
    if not layer.isValid():
        raise RuntimeError("memory layer invalid")
    return layer


def _wkb_point(x: float, y: float) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    g = QgsGeometry.fromPointXY(QgsPointXY(x, y))
    return bytes(g.asWkb())


def _seed_features(layer) -> list:
    """Populate the layer with two committed features.

    Returns a list of (fid, name) tuples. Both features are committed
    so that subsequent compensations operate on a stable baseline.
    """
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    if not layer.startEditing():
        raise RuntimeError("startEditing failed")
    seeded = []
    for x, name in [(1.0, "alpha"), (2.0, "beta")]:
        feat = QgsFeature(layer.fields())
        feat.setAttribute(0, name)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, 0.0)))
        if not layer.addFeature(feat):
            layer.rollBack()
            raise RuntimeError(f"addFeature failed for {name!r}")
        seeded.append((feat.id(), name))
    if not layer.commitChanges():
        raise RuntimeError("commitChanges seed failed")
    return seeded


def _make_event(
    operation: str,
    fid: int,
    name: str,
    *,
    geometry_wkb=None,
    new_geometry_wkb=None,
    eid: int = 1,
    new_name: str = "",
):
    """Build a synthetic AuditEvent compatible with _apply_via_buffer."""
    from recoverland.core.audit_backend import AuditEvent

    identity = {"fid": fid}
    attrs = {"old": {"name": name}}
    if new_name:
        attrs["new"] = {"name": new_name}
    return AuditEvent(
        event_id=eid,
        project_fingerprint="p_exec_proof_proj",
        datasource_fingerprint="p_exec_proof_ds",
        layer_id_snapshot="p_exec_proof_layer_id",
        layer_name_snapshot="p_exec_proof_mem",
        provider_type="memory",
        feature_identity_json=json.dumps(identity),
        operation_type=operation,
        attributes_json=json.dumps(attrs),
        geometry_wkb=geometry_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps([
            {"name": "name", "type": "string"},
        ]),
        user_name="tester",
        session_id=None,
        created_at="2026-05-14 12:00:00",
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=new_geometry_wkb,
        invalidated_at=None,
    )


def setup(ctx):
    from recoverland.core.logger import flog

    layer = _make_layer()
    seeded = _seed_features(layer)
    ctx.data["layer"] = layer
    ctx.data["seeded"] = seeded
    flog(
        f"p_executor_trace_id_proof setup: trace_id={ctx.trace_id} "
        f"seeded={seeded}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.logger import flog

    layer = ctx.data["layer"]
    seeded = ctx.data["seeded"]
    fid_alpha = seeded[0][0]
    fid_beta = seeded[1][0]

    geom_alpha = _wkb_point(1.0, 0.0)
    geom_beta = _wkb_point(2.0, 0.0)
    geom_gamma = _wkb_point(3.0, 0.0)

    if not layer.startEditing():
        raise RuntimeError("layer.startEditing failed")

    fid_remap: dict = {}
    results = []

    # ---- DELETE comp (re-insert a "deleted" feature) ----
    # Source is a USER DELETE for fid=alpha that took the feature out;
    # compensation = INSERT it back. We pass a fresh fid to simulate
    # a feature removed BEFORE the buffer was opened.
    del_event = _make_event(
        "DELETE", fid=999, name="gamma",
        geometry_wkb=geom_gamma,
        eid=1,
    )
    res_ins = _apply_via_buffer(
        layer, "INSERT", del_event, fid_remap, trace_id=ctx.trace_id,
    )
    results.append(("INSERT_comp", res_ins))
    flog(
        f"p_executor_trace_id_proof: INSERT_comp trace_id={ctx.trace_id} "
        f"status={res_ins.get('status')} reason_code={res_ins.get('reason_code')}",
        "INFO",
    )

    # ---- INSERT comp (delete a feature the user inserted) ----
    # Source is a USER INSERT for fid=alpha; compensation = DELETE it.
    ins_event = _make_event(
        "INSERT", fid=fid_alpha, name="alpha",
        geometry_wkb=geom_alpha,
        eid=2,
    )
    res_del = _apply_via_buffer(
        layer, "DELETE", ins_event, fid_remap, trace_id=ctx.trace_id,
    )
    results.append(("DELETE_comp", res_del))
    flog(
        f"p_executor_trace_id_proof: DELETE_comp trace_id={ctx.trace_id} "
        f"status={res_del.get('status')} reason_code={res_del.get('reason_code')}",
        "INFO",
    )

    # ---- UPDATE comp (revert an attribute change) ----
    # Source is a USER UPDATE on fid=beta: name "beta" -> "beta_v2".
    # Compensation = put back "beta".
    upd_event = _make_event(
        "UPDATE", fid=fid_beta, name="beta",
        geometry_wkb=geom_beta,
        new_geometry_wkb=geom_beta,
        eid=3, new_name="beta_v2",
    )
    res_upd = _apply_via_buffer(
        layer, "UPDATE", upd_event, fid_remap, trace_id=ctx.trace_id,
    )
    results.append(("UPDATE_comp", res_upd))
    flog(
        f"p_executor_trace_id_proof: UPDATE_comp trace_id={ctx.trace_id} "
        f"status={res_upd.get('status')} reason_code={res_upd.get('reason_code')}",
        "INFO",
    )

    layer.rollBack()  # discard buffer; we do not care about persistence

    ctx.data["results"] = results


_BUF_TID_RE = re.compile(
    r"event=BUF_(?:INS|DEL|UPD)\s+[^\n]*?trace_id=(?P<tid>[0-9a-f]+)"
)
_APPLY_PREFIX_RE = re.compile(
    r"\[(?P<tid>[0-9a-f]+)\]\s+_apply_via_buffer:\s+comp_op="
)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    results = ctx.data.get("results", [])

    # ----- P2-14: status + reason_code on every result -----
    for label, res in results:
        out.append((
            f"p214_{label}_status_known",
            isinstance(res, dict)
            and res.get("status") in _KNOWN_STATUSES,
            f"{label} status={res.get('status')!r} "
            f"expected one of {sorted(_KNOWN_STATUSES)}",
        ))
        out.append((
            f"p214_{label}_reason_code_set",
            isinstance(res, dict)
            and isinstance(res.get("reason_code"), str)
            and res.get("reason_code") != "",
            f"{label} reason_code={res.get('reason_code')!r} expected non-empty",
        ))
        out.append((
            f"p214_{label}_success_flag_consistent",
            isinstance(res, dict)
            and bool(res.get("success")) == (
                res.get("status") in (
                    "APPLIED", "SKIPPED_IDEMPOTENT", "SKIPPED_GEOMETRY_DRIFT",
                )
            ),
            f"{label} success={res.get('success')} status={res.get('status')}",
        ))

    # ----- P3-17: trace_id present in _apply_via_buffer prefix logs -----
    out.append(assert_log_contains(
        ctx.records,
        rf"\[{ctx.trace_id}\]\s+_apply_via_buffer:\s+comp_op=INSERT",
        name="p317_apply_via_buffer_prefix_INSERT", min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"\[{ctx.trace_id}\]\s+_apply_via_buffer:\s+comp_op=DELETE",
        name="p317_apply_via_buffer_prefix_DELETE", min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"\[{ctx.trace_id}\]\s+_apply_via_buffer:\s+comp_op=UPDATE",
        name="p317_apply_via_buffer_prefix_UPDATE", min_count=1,
    ))

    # ----- P3-17: trace_id present as kv on the structured BUF_* events -----
    found_tids = set()
    for rec in ctx.records:
        m = _BUF_TID_RE.search(rec.raw if hasattr(rec, "raw") else str(rec))
        if m is not None:
            found_tids.add(m.group("tid"))
    out.append((
        "p317_buf_kv_trace_id_present",
        ctx.trace_id in found_tids,
        f"BUF_* kv trace_ids found={sorted(found_tids)} "
        f"expected to contain {ctx.trace_id!r}",
    ))

    # ----- Trace_id propagation in setup line (sanity) -----
    out.append(assert_log_contains(
        ctx.records,
        rf"p_executor_trace_id_proof setup:\s+trace_id={ctx.trace_id}",
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
