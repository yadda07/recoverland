"""Scenario BL-RVW-01: Review replays rewind traces as data (leak).

Proves offline (no QGIS) that temporal_snapshot_engine, fed with trace
events exactly as build_restore_trace_event writes them (operation_type =
compensation op, attributes_json = {"_restore_ref": eid}, geometry_wkb =
None, restored_from_event_id set), produces WRONG as-of-T states for any
cutoff after a rewind:

  A1  entity restored by rewind (trace INSERT) loses its geometry at T
      (geom None -> silently dropped by the overlay populate step).
  A3  entity whose UPDATE was rewound (trace UPDATE) keeps the POST-update
      attrs at T instead of the restored OLD attrs.
  A7  chained traces (trace referencing a trace) resolve to the original
      state through the chain.

Guards (must stay green pre AND post patch):
  A2  attrs replay never leaks the "_restore_ref" marker into attrs.
  A4  geometry walk-back still crosses attrs-only trace UPDATEs.
  A5  trace DELETE (rewound INSERT) -> entity absent at T.
  A6  cutoff BEFORE the traces is unaffected by the fix.
  A8  trace referencing a purged/missing source event degrades without
      exception (geom None accepted), never crashes the reconstruction.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rvw_trace_replay"
INVARIANT = "BL-RVW-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS_PARENT = _PLUGIN_ROOT.parent
if str(_PLUGINS_PARENT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_PARENT))

_FP = "ds_rvw_trace_replay"
_GEOM_A = b"WKB_GEOM_A"
_GEOM_B = b"WKB_GEOM_B"
_GEOM_D = b"WKB_GEOM_D"

_T_AFTER = datetime(2026, 7, 5, 13, 0, 0, tzinfo=timezone.utc)
_T_BEFORE_TRACES = datetime(2026, 7, 5, 11, 30, 0, tzinfo=timezone.utc)


def _install_qgis_shim():
    """Offline-only stub so recoverland.core imports without QGIS."""
    import types

    class _StubMeta(type):
        def __getattr__(cls, name):
            return cls

    class _Stub(metaclass=_StubMeta):
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, *args, **kwargs):
            return self

    def _getattr(attr, _s=_Stub):
        if attr.startswith("__") and attr.endswith("__"):
            raise AttributeError(attr)
        return _s

    def _module(name, package=False):
        mod = types.ModuleType(name)
        mod.__getattr__ = _getattr
        if package:
            mod.__path__ = []
        sys.modules[name] = mod
        return mod

    qgis = _module("qgis", package=True)
    qgis.core = _module("qgis.core")
    qgis.gui = _module("qgis.gui")
    qgis.utils = _module("qgis.utils")
    qgis.PyQt = _module("qgis.PyQt", package=True)
    qgis.PyQt.QtCore = _module("qgis.PyQt.QtCore")
    qgis.PyQt.QtGui = _module("qgis.PyQt.QtGui")
    qgis.PyQt.QtWidgets = _module("qgis.PyQt.QtWidgets")


def _user_event(AuditEvent, eid, op, fid, created, attrs, geom=None, new_geom=None):
    """User event shaped exactly like edit_tracker factories."""
    return AuditEvent(
        event_id=eid,
        project_fingerprint="proj",
        datasource_fingerprint=_FP,
        layer_id_snapshot="layer_id",
        layer_name_snapshot="layer_name",
        provider_type="ogr",
        feature_identity_json='{"fid": %d}' % fid,
        operation_type=op,
        attributes_json=json.dumps(attrs, ensure_ascii=False),
        geometry_wkb=geom,
        geometry_type="Point",
        crs_authid="EPSG:2154",
        field_schema_json="[]",
        user_name="test",
        session_id="sess",
        created_at=created,
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=5,
        new_geometry_wkb=new_geom,
        invalidated_at=None,
    )


def _trace_event(AuditEvent, eid, comp_op, fid, src_eid, created):
    """Trace shaped exactly like restore_service.build_restore_trace_event:
    marker attrs, NO geometry, restored_from_event_id set."""
    return AuditEvent(
        event_id=eid,
        project_fingerprint="proj",
        datasource_fingerprint=_FP,
        layer_id_snapshot="layer_id",
        layer_name_snapshot="layer_name",
        provider_type="ogr",
        feature_identity_json='{"fid": %d}' % fid,
        operation_type=comp_op,
        attributes_json=json.dumps({"_restore_ref": src_eid}),
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:2154",
        field_schema_json=None,
        user_name="test",
        session_id=None,
        created_at=created,
        restored_from_event_id=src_eid,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=5,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _build_cache():
    from recoverland.core.audit_backend import AuditEvent

    events = [
        # E1 fid:1 - INSERT, DELETE, rewind re-inserts (trace INSERT ref DELETE)
        _user_event(AuditEvent, 101, "INSERT", 1, "2026-07-05T10:00:00+00:00",
                    {"all_attributes": {"name": "alpha", "code": "C1"}},
                    geom=_GEOM_A),
        _user_event(AuditEvent, 102, "DELETE", 1, "2026-07-05T11:00:00+00:00",
                    {"all_attributes": {"name": "alpha", "code": "C1"}},
                    geom=_GEOM_A),
        _trace_event(AuditEvent, 201, "INSERT", 1, 102, "2026-07-05T12:00:00+00:00"),
        # E2 fid:2 - INSERT, UPDATE attrs-only, rewind reverts (trace UPDATE)
        _user_event(AuditEvent, 111, "INSERT", 2, "2026-07-05T10:00:00+00:00",
                    {"all_attributes": {"name": "old", "status": "A"}},
                    geom=_GEOM_B),
        _user_event(AuditEvent, 112, "UPDATE", 2, "2026-07-05T11:00:00+00:00",
                    {"changed_only": {"name": {"old": "old", "new": "new"}}}),
        _trace_event(AuditEvent, 202, "UPDATE", 2, 112, "2026-07-05T12:00:00+00:00"),
        # E3 fid:3 - INSERT, rewind removes it (trace DELETE)
        _user_event(AuditEvent, 121, "INSERT", 3, "2026-07-05T10:30:00+00:00",
                    {"all_attributes": {"name": "gamma"}},
                    geom=b"WKB_GEOM_C"),
        _trace_event(AuditEvent, 203, "DELETE", 3, 121, "2026-07-05T12:00:00+00:00"),
        # E4 fid:4 - orphan: trace INSERT referencing a PURGED event id
        _trace_event(AuditEvent, 204, "INSERT", 4, 999, "2026-07-05T12:00:00+00:00"),
        # E5 fid:5 - chained traces: INSERT, trace DELETE(ref INSERT),
        # trace INSERT(ref trace DELETE)  [rewind then rewind-of-rewind]
        _user_event(AuditEvent, 131, "INSERT", 5, "2026-07-05T10:00:00+00:00",
                    {"all_attributes": {"name": "delta"}},
                    geom=_GEOM_D),
        _trace_event(AuditEvent, 205, "DELETE", 5, 131, "2026-07-05T11:45:00+00:00"),
        _trace_event(AuditEvent, 206, "INSERT", 5, 205, "2026-07-05T12:00:00+00:00"),
    ]
    return {_FP: events}


def _attrs(feat):
    if feat is None or not feat.attrs_json:
        return {}
    return json.loads(feat.attrs_json)


def setup(ctx):
    try:
        import qgis  # noqa: F401 - real QGIS runtime available
    except ImportError:
        _install_qgis_shim()
    import importlib
    import recoverland.core.temporal_snapshot_engine as engine
    importlib.reload(engine)
    ctx.data["engine"] = engine


def run(ctx):
    engine = ctx.data["engine"]
    cache = _build_cache()

    crash = ""
    try:
        after = engine.reconstruct_snapshot_at(
            cache, _T_AFTER, trace_id=ctx.trace_id)
        before = engine.reconstruct_snapshot_at(
            cache, _T_BEFORE_TRACES, trace_id=ctx.trace_id)
    except Exception as exc:  # noqa: BLE001
        crash = repr(exc)
        after = before = None
    ctx.data["crash"] = crash
    ctx.data["after"] = after
    ctx.data["before"] = before


def assertions(ctx):
    after = ctx.data["after"]
    before = ctx.data["before"]
    crash = ctx.data["crash"]
    if crash or after is None or before is None:
        return [("no_crash", False, f"reconstruction raised: {crash}")]

    fa = after.features.get(_FP, {})
    fb = before.features.get(_FP, {})
    e1 = fa.get("fid:1")
    e2 = fa.get("fid:2")
    e5 = fa.get("fid:5")
    e4 = fa.get("fid:4")

    return [
        ("a1_trace_insert_restores_geom",
         e1 is not None and e1.geom_wkb == _GEOM_A,
         f"E1 at T geom={e1.geom_wkb if e1 else 'ABSENT'} expected {_GEOM_A} "
         f"(restored feature must keep its geometry, not None/dropped)"),
        ("a2_no_restore_ref_marker_in_attrs",
         e1 is not None and "_restore_ref" not in _attrs(e1)
         and _attrs(e1).get("name") == "alpha",
         f"E1 attrs={_attrs(e1) if e1 else 'ABSENT'} must carry name=alpha "
         f"and never the _restore_ref marker"),
        ("a3_trace_update_restores_old_attrs",
         e2 is not None and _attrs(e2).get("name") == "old",
         f"E2 at T name={_attrs(e2).get('name') if e2 else 'ABSENT'} "
         f"expected 'old' (rewind reverted the UPDATE before T)"),
        ("a4_geom_walkback_through_trace_update",
         e2 is not None and e2.geom_wkb == _GEOM_B,
         f"E2 geom={e2.geom_wkb if e2 else 'ABSENT'} expected {_GEOM_B}"),
        ("a5_trace_delete_absent",
         "fid:3" not in fa,
         f"E3 must be absent at T (rewind deleted it), features={sorted(fa)}"),
        ("a6_pre_trace_cutoff_unaffected",
         "fid:1" not in fb and _attrs(fb.get("fid:2")).get("name") == "new",
         f"before traces: E1 absent (deleted), E2 name=new; "
         f"got features={sorted(fb)} "
         f"e2_name={_attrs(fb.get('fid:2')).get('name') if fb.get('fid:2') else '?'}"),
        ("a7_chained_trace_resolves",
         e5 is not None and e5.geom_wkb == _GEOM_D,
         f"E5 at T geom={e5.geom_wkb if e5 else 'ABSENT'} expected {_GEOM_D} "
         f"(trace INSERT ref trace DELETE ref INSERT must chain)"),
        ("a8_orphan_ref_degrades_not_crashes",
         e4 is None or e4.geom_wkb is None,
         f"E4 (ref purged eid=999) must degrade (absent or geom None), "
         f"got geom={e4.geom_wkb if e4 else 'ABSENT'}"),
    ]
