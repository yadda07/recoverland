"""Scenario IL-5 - lens_planner.plan_lens_view classification (CR-IL-2).

Verifies that core/lens_planner.py exposes plan_lens_view() and that
it builds EntityTimelines correctly with the right EntityClassification
for the 6 canonical cases.

Cause racine: CR-IL-2 (entity_fingerprint may be missing on legacy
events / shapefile FID recycling). Without a robust grouping strategy
plus classification logic, the renderer would receive ambiguous data.

Acceptance assertions (each is an antithesis):
    1. core.lens_planner module exists and imports without QGIS/Qt.
    2. plan_lens_view exists with the expected signature.
    3. INSERT event in zone -> CREATED_IN_ZONE.
    4. DELETE last event in zone -> DELETED_FROM_ZONE.
    5. UPDATE geom out->in zone -> MOVED_INTO_ZONE.
    6. UPDATE geom in->out zone -> MOVED_OUT_OF_ZONE.
    7. UPDATE attrs only (no geom change) -> ATTR_ONLY_IN_ZONE.
    8. attrs_delta correctly extracted from attributes_json transitions.

Initial verdict: FAIL (file does not exist).
Post-patch verdict: PASS.

Pure Python. No QGIS.
"""
from __future__ import annotations

import importlib.util
import json
import struct
import sys
import types
from pathlib import Path

SCENARIO_ID = "il5_lens_planner_classify"
INVARIANT = "BL-IL-P0-05"
EXPECTED_SIGNATURE = r""

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_PLANNER_PATH = _PLUGIN_ROOT / "core" / "lens_planner.py"


def _le_point_wkb(x: float, y: float) -> bytes:
    return b"\x01" + struct.pack("<I", 1) + struct.pack("<d", x) + struct.pack("<d", y)


def _ensure_core_stub() -> None:
    if "core" in sys.modules and hasattr(sys.modules["core"], "__path__"):
        return
    pkg = types.ModuleType("core")
    pkg.__path__ = [str(_PLUGIN_ROOT / "core")]  # type: ignore[attr-defined]
    sys.modules["core"] = pkg


def _import_real(name: str, relpath: str):
    if name in sys.modules and getattr(sys.modules[name], "__file__", None):
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, str(_PLUGIN_ROOT / relpath)
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "core"
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _import_planner():
    _ensure_core_stub()
    _import_real("core.audit_backend", "core/audit_backend.py")
    _import_real("core.lens_contracts", "core/lens_contracts.py")
    _import_real("core.wkb_envelope", "core/wkb_envelope.py")
    return _import_real("core.lens_planner", "core/lens_planner.py")


def setup(ctx):
    ctx.data["file_exists"] = _PLANNER_PATH.is_file()
    ctx.data["planner"] = None
    ctx.data["import_error"] = None
    if ctx.data["file_exists"]:
        try:
            ctx.data["planner"] = _import_planner()
        except Exception as exc:  # noqa: BLE001
            ctx.data["import_error"] = repr(exc)


def _make_event(
    ab_mod,
    *,
    op: str,
    entity_fp: str,
    created_at: str,
    geom_wkb=None,
    new_geom_wkb=None,
    attrs=None,
    event_id: int = 0,
):
    AuditEvent = ab_mod.AuditEvent
    attrs_json = json.dumps(attrs or {})
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="P",
        datasource_fingerprint="DS_A",
        layer_id_snapshot="L1",
        layer_name_snapshot="L1",
        provider_type="memory",
        feature_identity_json="{}",
        operation_type=op,
        attributes_json=attrs_json,
        geometry_wkb=geom_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="{}",
        user_name="u",
        session_id="S1",
        created_at=created_at,
        restored_from_event_id=None,
        entity_fingerprint=entity_fp,
        event_schema_version=2,
        new_geometry_wkb=new_geom_wkb,
        invalidated_at=None,
    )


def run(ctx):
    planner = ctx.data.get("planner")
    if planner is None:
        return

    ab_mod = sys.modules["core.audit_backend"]
    lc_mod = sys.modules["core.lens_contracts"]
    Selection = lc_mod.LensSelection
    OpFilter = lc_mod.LensOpFilter
    Mode = lc_mod.LensVisualizationMode
    FetchStats = lc_mod.LensFetchStats

    in_zone = _le_point_wkb(5.0, 5.0)
    out_zone = _le_point_wkb(100.0, 100.0)

    # Scenarios sized to cover all 6 classifications + attr_delta.
    events = [
        # ENT_CR: single INSERT in zone -> CREATED_IN_ZONE
        _make_event(ab_mod, op="INSERT", entity_fp="ENT_CR",
                    created_at="2026-05-01T10:00:00Z", geom_wkb=in_zone,
                    attrs={"name": "A", "kind": "x"}, event_id=1),
        # ENT_DEL: INSERT + DELETE in zone -> DELETED_FROM_ZONE (last op = DELETE)
        _make_event(ab_mod, op="INSERT", entity_fp="ENT_DEL",
                    created_at="2026-05-01T10:00:00Z", geom_wkb=in_zone,
                    attrs={"v": 1}, event_id=2),
        _make_event(ab_mod, op="DELETE", entity_fp="ENT_DEL",
                    created_at="2026-05-02T10:00:00Z", geom_wkb=in_zone,
                    attrs={"v": 1}, event_id=3),
        # ENT_IN: UPDATE out -> in -> MOVED_INTO_ZONE
        _make_event(ab_mod, op="UPDATE", entity_fp="ENT_IN",
                    created_at="2026-05-01T10:00:00Z",
                    geom_wkb=out_zone, new_geom_wkb=in_zone,
                    attrs={"v": 1}, event_id=4),
        # ENT_OUT: UPDATE in -> out -> MOVED_OUT_OF_ZONE
        _make_event(ab_mod, op="UPDATE", entity_fp="ENT_OUT",
                    created_at="2026-05-01T10:00:00Z",
                    geom_wkb=in_zone, new_geom_wkb=out_zone,
                    attrs={"v": 1}, event_id=5),
        # ENT_ATTR: UPDATE attrs only (same geom) -> ATTR_ONLY_IN_ZONE
        _make_event(ab_mod, op="UPDATE", entity_fp="ENT_ATTR",
                    created_at="2026-05-01T09:00:00Z",
                    geom_wkb=in_zone, new_geom_wkb=in_zone,
                    attrs={"label": "OLD"}, event_id=6),
        _make_event(ab_mod, op="UPDATE", entity_fp="ENT_ATTR",
                    created_at="2026-05-01T10:00:00Z",
                    geom_wkb=in_zone, new_geom_wkb=in_zone,
                    attrs={"label": "NEW"}, event_id=7),
    ]

    selection = Selection(
        layer_id_snapshot="L1",
        datasource_fp="DS_A",
        bbox_xy=(0.0, 0.0, 10.0, 10.0),
        bbox_crs="EPSG:4326",
        t_min="2026-04-15T00:00:00Z",
        t_max="2026-05-15T00:00:00Z",
        op_filter=OpFilter.ALL,
        mode=Mode.DIFF_WINDOW,
        max_events=5000,
    )
    stats = FetchStats(
        n_events_total=len(events),
        n_events_returned=len(events),
        n_events_truncated=0,
        elapsed_ms=0,
    )

    try:
        plan = planner.plan_lens_view(
            events=events,
            selection=selection,
            layer_name="L1",
            fetch_stats=stats,
        )
        ctx.data["plan"] = plan
        ctx.data["plan_error"] = None
    except Exception as exc:  # noqa: BLE001
        ctx.data["plan"] = None
        ctx.data["plan_error"] = repr(exc)


def _classification_value(entities, key: str):
    timeline = entities.get(key)
    if timeline is None:
        return None
    cls = timeline.classification
    return getattr(cls, "value", str(cls))


def assertions(ctx):
    results = []

    # 1. Module importable
    file_exists = ctx.data["file_exists"]
    planner = ctx.data.get("planner")
    err = ctx.data.get("import_error")
    ok = file_exists and planner is not None and err is None
    msg = (f"file_exists={file_exists} import_error={err}"
           if not ok else "imported ok")
    results.append(("lens_planner_importable", ok, msg))

    if not ok:
        for name in (
            "plan_lens_view_exists",
            "classify_created_in_zone",
            "classify_deleted_from_zone",
            "classify_moved_into_zone",
            "classify_moved_out_of_zone",
            "classify_attr_only_in_zone",
            "attrs_delta_extracted",
        ):
            results.append((name, False, "skipped: module not importable"))
        return results

    # 2. function exists
    has_fn = hasattr(planner, "plan_lens_view")
    results.append((
        "plan_lens_view_exists", has_fn,
        f"hasattr=plan_lens_view -> {has_fn}",
    ))

    plan = ctx.data.get("plan")
    if plan is None:
        for name in (
            "classify_created_in_zone",
            "classify_deleted_from_zone",
            "classify_moved_into_zone",
            "classify_moved_out_of_zone",
            "classify_attr_only_in_zone",
            "attrs_delta_extracted",
        ):
            results.append((name, False, f"plan_lens_view failed: {ctx.data.get('plan_error')}"))
        return results

    entities = plan.entities

    # 3-7. Classification checks
    expected = {
        "ENT_CR": "created_in_zone",
        "ENT_DEL": "deleted_from_zone",
        "ENT_IN": "moved_into_zone",
        "ENT_OUT": "moved_out_of_zone",
        "ENT_ATTR": "attr_only",
    }
    for key, label in [
        ("ENT_CR", "classify_created_in_zone"),
        ("ENT_DEL", "classify_deleted_from_zone"),
        ("ENT_IN", "classify_moved_into_zone"),
        ("ENT_OUT", "classify_moved_out_of_zone"),
        ("ENT_ATTR", "classify_attr_only_in_zone"),
    ]:
        actual = _classification_value(entities, key)
        ok_cls = actual == expected[key]
        results.append((
            label, ok_cls,
            f"{key} expected={expected[key]} actual={actual}",
        ))

    # 8. attrs_delta on ENT_ATTR : label OLD -> NEW
    ent_attr = entities.get("ENT_ATTR")
    delta_ok = False
    delta_msg = "ENT_ATTR missing"
    if ent_attr is not None and ent_attr.states:
        last_state = ent_attr.states[-1]
        delta = last_state.attrs_delta
        delta_ok = delta.get("label") == ("OLD", "NEW")
        delta_msg = f"attrs_delta={delta}"
    results.append(("attrs_delta_extracted", delta_ok, delta_msg))

    return results


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
