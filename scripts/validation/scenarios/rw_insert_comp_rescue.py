"""Scenario BL-RW-P1-25: attribute rescue for INSERT-comp DELETE after FID drift.

Offline proof (qgis shim + fake layer) that:
  1. REAL _find_by_attrs_only finds the drifted inserted feature by strict
     attribute match when geometry-based lookup failed (za_sro case, trace
     d8933d22: SNAP_SCAN scanned=22 matches=0 diffs=[]).
  2. Antitheses: ambiguity refused, non-discriminant events refused,
     feature-side NULL is a mismatch, exclude_fids respected.
  3. REAL _occupant_shares_evidence: an occupant sharing nothing with the
     event is rejected (RW-11 narrowed); sharing one attribute passes.
  4. restore_executor no longer blind-trusts the direct FID (source check,
     FAILS on pre-patch source).
"""
import sys
from pathlib import Path

SCENARIO_ID = "rw_insert_comp_rescue"
INVARIANT = "BL-RW-P1-25"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS_PARENT = _PLUGIN_ROOT.parent
if str(_PLUGINS_PARENT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_PARENT))

from scripts.validation.scenarios.rw_trace_lifecycle import _install_qgis_shim  # noqa: E402


class FakeFields:
    def __init__(self, names):
        self._names = list(names)

    def indexOf(self, name):
        try:
            return self._names.index(name)
        except ValueError:
            return -1


class FakeProvider:
    def __init__(self, pk_indexes=()):
        self._pk = list(pk_indexes)

    def pkAttributeIndexes(self):
        return self._pk

    def name(self):
        return "ogr"


class FakeFeature:
    def __init__(self, fid, values):
        self._fid = fid
        self._values = list(values)

    def id(self):
        return self._fid

    def __getitem__(self, idx):
        return self._values[idx]

    def geometry(self):
        return None


class FakeLayer:
    def __init__(self, field_names, features, pk_indexes=()):
        self._fields = FakeFields(field_names)
        self._features = features
        self._provider = FakeProvider(pk_indexes)

    def fields(self):
        return self._fields

    def dataProvider(self):
        return self._provider

    def getFeatures(self, request=None):
        return iter(self._features)


_FIELDS = ["sro", "zs_refpm", "commentair", "exe"]


def _mk_event(attrs, eid=59377):
    import json
    from recoverland.core.audit_backend import AuditEvent
    return AuditEvent(
        event_id=eid, project_fingerprint="proj",
        datasource_fingerprint="ds", layer_id_snapshot="lid",
        layer_name_snapshot="za_sro", provider_type="ogr",
        feature_identity_json='{"fid": 16}', operation_type="INSERT",
        attributes_json=json.dumps({"all_attributes": attrs}),
        geometry_wkb=None, geometry_type="Polygon", crs_authid="EPSG:2154",
        field_schema_json="[]", user_name="test", session_id="s",
        created_at="2026-06-19T13:08:35", restored_from_event_id=None,
        entity_fingerprint="fid:16", event_schema_version=5,
        new_geometry_wkb=None, invalidated_at=None,
    )


def setup(ctx):
    import importlib
    try:
        import qgis  # noqa: F401
    except ImportError:
        _install_qgis_shim()
    import recoverland.core.restore_service as rs
    import recoverland.core.restore_executor as rx
    importlib.reload(rs)
    importlib.reload(rx)
    ctx.data["rs"] = rs
    ctx.data["rx"] = rx
    ctx.data["executor_source"] = (
        _PLUGIN_ROOT / "core" / "restore_executor.py").read_text(encoding="utf-8")


def run(ctx):
    rs = ctx.data["rs"]
    rx = ctx.data["rx"]
    stress_attrs = {
        "sro": "stress_fddf4652", "zs_refpm": "stress_5bf53cfc",
        "commentair": None, "exe": 3608,
    }
    event = _mk_event(stress_attrs)

    # 1. Nominal: drifted stress feature at fid 12, occupant at fid 16.
    layer = FakeLayer(_FIELDS, [
        FakeFeature(11, ["63398/A2D", "PM12", None, 100]),
        FakeFeature(12, ["stress_fddf4652", "stress_5bf53cfc", None, 3608]),
        FakeFeature(16, ["63398/OTHER", "PM99", "real feature", 250]),
    ])
    ctx.data["nominal"] = rs._find_by_attrs_only(layer, event)

    # 2. Antithese ambiguity: two identical candidates -> refuse.
    layer_dup = FakeLayer(_FIELDS, [
        FakeFeature(12, ["stress_fddf4652", "stress_5bf53cfc", None, 3608]),
        FakeFeature(13, ["stress_fddf4652", "stress_5bf53cfc", None, 3608]),
    ])
    ctx.data["ambiguous"] = rs._find_by_attrs_only(layer_dup, event)

    # 3. Antithese non-discriminant: 1 usable attr < min_attrs.
    sparse_event = _mk_event({"sro": "stress_x", "zs_refpm": None,
                              "commentair": None, "exe": None})
    ctx.data["sparse"] = rs._find_by_attrs_only(layer, sparse_event)

    # 4. Antithese feature-side NULL: empty feature must not match.
    layer_nulls = FakeLayer(_FIELDS, [
        FakeFeature(5, [None, None, None, None]),
    ])
    ctx.data["nulls"] = rs._find_by_attrs_only(layer_nulls, event)

    # 5. Antithese exclude_fids: the rescued fid is a comp-INSERT -> skip.
    ctx.data["excluded"] = rs._find_by_attrs_only(
        layer, event, exclude_fids={12})

    # 6. Occupant evidence: fid 16 shares nothing -> False;
    #    fid 12 shares attrs -> True.
    ctx.data["occupant_none"] = rx._occupant_shares_evidence(
        FakeLayer(_FIELDS, [FakeFeature(16, ["63398/OTHER", "PM99", "x", 250])]),
        16, event)
    ctx.data["occupant_attr"] = rx._occupant_shares_evidence(
        FakeLayer(_FIELDS, [FakeFeature(16, ["63398/OTHER", "PM99", "x", 3608])]),
        16, event)


def assertions(ctx):
    src = ctx.data["executor_source"]
    return [
        ("rescue_finds_drifted_feature",
         ctx.data["nominal"] == 12,
         f"nominal={ctx.data['nominal']} expected 12 (drifted stress feature)"),
        ("neg_ambiguous_refused",
         ctx.data["ambiguous"] is None,
         f"ambiguous={ctx.data['ambiguous']} expected None (2 identical candidates)"),
        ("neg_non_discriminant_refused",
         ctx.data["sparse"] is None,
         f"sparse={ctx.data['sparse']} expected None (<2 usable attrs)"),
        ("neg_null_feature_no_match",
         ctx.data["nulls"] is None,
         f"nulls={ctx.data['nulls']} expected None (feature-side NULL = mismatch)"),
        ("neg_exclude_fids_respected",
         ctx.data["excluded"] is None,
         f"excluded={ctx.data['excluded']} expected None (fid 12 excluded)"),
        ("occupant_no_evidence_rejected",
         ctx.data["occupant_none"] is False,
         f"occupant_none={ctx.data['occupant_none']} expected False"),
        ("occupant_shared_attr_accepted",
         ctx.data["occupant_attr"] is True,
         f"occupant_attr={ctx.data['occupant_attr']} expected True (exe=3608)"),
        ("executor_blind_trust_removed",
         "INSERT FID trustworthy: subsequent rounds modified attrs RW-11" not in src,
         "restore_executor.py must not blind-trust the direct FID on snap "
         "mismatch (pre-patch RW-11 string must be gone)"),
        ("executor_rescue_wired",
         "attr_rescue" in src and "_find_by_attrs_only" in src,
         "restore_executor.py must call _find_by_attrs_only (attr_rescue)"),
        ("executor_unverifiable_fails",
         "target_unverifiable" in src,
         "restore_executor.py must FAIL with target_unverifiable instead of "
         "deleting an unverified occupant"),
    ]
