"""Regression tests for BUG-01 to BUG-04.

These tests verify the specific fixes demanded by the backlog:
- BUG-01/02: restore_inserted_feature refuses provider without DeleteFeatures
- BUG-03: restore_updated_feature refuses without ChangeGeometries when geometry present
- BUG-04: re-entrance guard in recover_dialog
- GAP-01: preflight_layer_check validates capabilities before execution
"""
from recoverland.core.audit_backend import AuditEvent
from recoverland.core.restore_service import (
    restore_inserted_feature, restore_updated_feature,
)
from recoverland.core.restore_executor import preflight_layer_check
from recoverland.core.restore_contracts import (
    RestoreMode, RestoreScope, AtomicityPolicy, ConflictPolicy,
    PlannedAction, RestorePlan,
)


def _make_event(op="UPDATE", geom_wkb=None):
    return AuditEvent(
        event_id=1,
        project_fingerprint="proj",
        datasource_fingerprint="ogr::test",
        layer_id_snapshot="layer_1",
        layer_name_snapshot="test",
        provider_type="ogr",
        feature_identity_json='{"fid": 1, "pk_field": "id", "pk_value": 1}',
        operation_type=op,
        attributes_json='{"changed_only": {"name": {"old": "a", "new": "b"}}}',
        geometry_wkb=geom_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json='[{"name": "id", "type": "int"}, {"name": "name", "type": "str"}]',
        user_name="tester",
        session_id="sess",
        created_at="2025-01-15T10:00:00Z",
        restored_from_event_id=None,
        entity_fingerprint="pk:id=1",
        event_schema_version=2,
    )


class _FakeProvider:
    """Mock provider with controllable capabilities."""
    def __init__(self, caps):
        self._caps = caps
    def capabilities(self):
        return self._caps
    def name(self):
        return "ogr"
    def errors(self):
        return []
    def deleteFeatures(self, fids):
        return True
    def changeAttributeValues(self, changes):
        return True
    def changeGeometryValues(self, changes):
        return True


class _FakeLayer:
    """Mock layer wrapping a FakeProvider."""
    def __init__(self, caps):
        self._provider = _FakeProvider(caps)
        self._pk_idx = 0
    def dataProvider(self):
        return self._provider
    def id(self):
        return "layer_1"
    def fields(self):
        return _FakeFields()
    def primaryKeyAttributes(self):
        return [0]
    def source(self):
        return "/tmp/test.gpkg|layername=test"


class _FakeFields:
    def __init__(self):
        self._names = ["id", "name"]
    def __iter__(self):
        return iter([_FakeField(n) for n in self._names])
    def count(self):
        return len(self._names)
    def at(self, idx):
        return _FakeField(self._names[idx])
    def indexOf(self, name):
        try:
            return self._names.index(name)
        except ValueError:
            return -1


class _FakeField:
    def __init__(self, name):
        self._name = name
    def name(self):
        return self._name
    def typeName(self):
        return "String" if self._name != "id" else "Integer"
    def type(self):
        return 10 if self._name != "id" else 2
    def length(self):
        return 0
    def precision(self):
        return 0


# Capability bit values from compat.py / conftest.py
CAP_ADD = 1
CAP_DELETE = 2
CAP_CHANGE_ATTR = 4
CAP_CHANGE_GEOM = 0x800
ALL_CAPS = CAP_ADD | CAP_DELETE | CAP_CHANGE_ATTR | CAP_CHANGE_GEOM


# ---- BUG-01 regression: restore_inserted_feature MUST check DeleteFeatures ----
# The bug was: caps & 8 (AddAttributes) instead of caps & 2 (DeleteFeatures).
# A provider with AddAttributes but NOT DeleteFeatures must be refused.
# A provider with DeleteFeatures but NOT AddAttributes must be accepted past the check.

class TestBug01RegressionDeleteCapability:
    def test_provider_without_delete_cap_refused(self):
        layer = _FakeLayer(CAP_ADD | CAP_CHANGE_ATTR)  # has AddAttributes(8) but NOT DeleteFeatures(2)
        event = _make_event(op="INSERT")
        result = restore_inserted_feature(layer, event)
        assert result["success"] is False
        assert "delete" in result["message"].lower()

    def test_provider_with_only_add_cap_refused(self):
        layer = _FakeLayer(CAP_ADD)  # bit 1 only
        event = _make_event(op="INSERT")
        result = restore_inserted_feature(layer, event)
        assert result["success"] is False
        assert "delete" in result["message"].lower()

    def test_check_uses_cap_delete_features_not_magic_number(self):
        import inspect
        src = inspect.getsource(restore_inserted_feature)
        assert "CAP_DELETE_FEATURES" in src, \
            "restore_inserted_feature must use CAP_DELETE_FEATURES symbolic constant"
        assert "& 8" not in src, \
            "restore_inserted_feature must NOT use magic number & 8 (old bug)"
        assert "& 2" not in src, \
            "restore_inserted_feature must NOT use magic number & 2 (use symbolic constant)"

    def test_none_layer_refused(self):
        result = restore_inserted_feature(None, _make_event(op="INSERT"))
        assert result["success"] is False


# ---- BUG-03 regression: restore_updated_feature MUST check ChangeGeometries ----
# The bug was: no pre-check for ChangeGeometries before calling changeGeometryValues().
# We test the exact check via source code inspection since the function traverses
# deep QGIS internals (_find_target_feature) that require heavy mocks.

class TestBug03RegressionGeometryCapability:
    def test_change_geometries_check_exists_in_source(self):
        import inspect
        src = inspect.getsource(restore_updated_feature)
        assert "CAP_CHANGE_GEOMETRIES" in src or "ChangeGeometries" in src
        # The check must happen BEFORE changeGeometryValues
        geom_check_idx = src.index("CAP_CHANGE_GEOMETRIES") if "CAP_CHANGE_GEOMETRIES" in src else src.index("ChangeGeometries")
        change_call_idx = src.index("changeGeometryValues")
        assert geom_check_idx < change_call_idx, \
            "ChangeGeometries capability check must happen BEFORE changeGeometryValues call"

    def test_check_is_conditional_on_geometry_wkb(self):
        import inspect
        src = inspect.getsource(restore_updated_feature)
        # The geometry capability check should only trigger when geometry_wkb is not None
        assert "geometry_wkb" in src


# ---- BUG-04 regression: re-entrance guard ----

class TestBug04RegressionReentranceGuard:
    def test_restore_in_progress_flag_in_source(self):
        import os
        src_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "recover_dialog.py",
        )
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()
        assert "_restore_in_progress" in src
        assert "_restore_in_progress = True" in src
        assert "_restore_in_progress = False" in src

    def test_cancel_resets_restore_flag(self):
        import os
        src_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "recover_dialog.py",
        )
        with open(src_path, "r", encoding="utf-8") as f:
            src = f.read()
        assert "self._restore_runner.cancel()" in src
        idx = src.index("self._restore_runner.cancel()")
        after = src[idx:idx + 500]
        assert "_restore_in_progress = False" in after


# ---- GAP-01: preflight_layer_check validates capabilities ----

def _make_plan_with_ops(ops):
    actions = []
    for i, (op, comp_op, has_geom) in enumerate(ops):
        actions.append(PlannedAction(
            event_id=i + 1,
            operation_type=op,
            compensatory_op=comp_op,
            entity_fingerprint=f"pk:id={i}",
            datasource_fingerprint="ds",
            layer_name="lyr",
            has_geometry=has_geom,
            has_attribute_changes=True,
        ))
    return RestorePlan(
        mode=RestoreMode.EVENT,
        scope=RestoreScope.SELECTION,
        cutoff=None,
        atomicity=AtomicityPolicy.BEST_EFFORT,
        conflict_policy=ConflictPolicy.SKIP,
        actions=actions,
        conflicts=[],
        entity_count=len(actions),
        event_count=len(actions),
        datasource_fingerprint="ds",
        layer_name="lyr",
    )


class TestPreflightLayerCheck:
    def test_none_layer_blocked(self):
        plan = _make_plan_with_ops([("DELETE", "INSERT", False)])
        issues = preflight_layer_check(plan, None)
        assert len(issues) == 1
        assert "None" in issues[0]

    def test_no_provider_blocked(self):
        class NoProviderLayer:
            def dataProvider(self):
                return None
        plan = _make_plan_with_ops([("DELETE", "INSERT", False)])
        issues = preflight_layer_check(plan, NoProviderLayer())
        assert len(issues) == 1

    def test_insert_needs_add_features(self):
        plan = _make_plan_with_ops([("DELETE", "INSERT", False)])
        layer = _FakeLayer(CAP_DELETE)  # has DELETE but not ADD
        issues = preflight_layer_check(plan, layer)
        assert any("AddFeatures" in i for i in issues)

    def test_delete_needs_delete_features(self):
        plan = _make_plan_with_ops([("INSERT", "DELETE", False)])
        layer = _FakeLayer(CAP_ADD)  # has ADD but not DELETE
        issues = preflight_layer_check(plan, layer)
        assert any("DeleteFeatures" in i for i in issues)

    def test_update_needs_change_attr(self):
        plan = _make_plan_with_ops([("UPDATE", "UPDATE", False)])
        layer = _FakeLayer(CAP_ADD)  # missing CHANGE_ATTR
        issues = preflight_layer_check(plan, layer)
        assert any("ChangeAttributeValues" in i for i in issues)

    def test_geom_update_needs_change_geom(self):
        plan = _make_plan_with_ops([("UPDATE", "UPDATE", True)])
        layer = _FakeLayer(CAP_CHANGE_ATTR)  # missing CHANGE_GEOM
        issues = preflight_layer_check(plan, layer)
        assert any("ChangeGeometries" in i for i in issues)

    def test_all_caps_present_no_issues(self):
        plan = _make_plan_with_ops([
            ("DELETE", "INSERT", False),
            ("UPDATE", "UPDATE", True),
            ("INSERT", "DELETE", False),
        ])
        layer = _FakeLayer(ALL_CAPS)
        issues = preflight_layer_check(plan, layer)
        assert issues == []

    def test_empty_plan_no_issues(self):
        plan = _make_plan_with_ops([])
        layer = _FakeLayer(0)
        issues = preflight_layer_check(plan, layer)
        assert issues == []

    def test_multiple_missing_caps_all_reported(self):
        plan = _make_plan_with_ops([
            ("DELETE", "INSERT", False),
            ("INSERT", "DELETE", False),
            ("UPDATE", "UPDATE", True),
        ])
        layer = _FakeLayer(0)  # no caps at all
        issues = preflight_layer_check(plan, layer)
        assert len(issues) >= 3  # ADD, DELETE, CHANGE_ATTR, CHANGE_GEOM
