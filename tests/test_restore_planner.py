"""Brutal tests for restore_planner.py, preflight, and restore_preview.py.

Every branch, every failure mode, every edge case.
"""
from recoverland.core.audit_backend import AuditEvent
from recoverland.core.restore_contracts import (
    RestoreMode, RestoreScope, CutoffType, ConflictPolicy,
    AtomicityPolicy, PreflightVerdict, RestoreCutoff,
    RestorePlan, MAX_EVENTS_PER_RESTORE, MAX_ENTITIES_PER_RESTORE,
)
from recoverland.core.restore_planner import (
    plan_event_restore, plan_temporal_restore, preflight_check,
    check_retention_coverage, _build_action,
)
from recoverland.core.restore_preview import (
    format_plan_summary, format_preflight_report, format_dry_run_message,
)


def _evt(event_id, op_type, entity_fp="pk:id=1", ds_fp="ogr::test",
         geom_wkb=None, layer_name="test_layer"):
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="proj::test",
        datasource_fingerprint=ds_fp,
        layer_id_snapshot="layer_1",
        layer_name_snapshot=layer_name,
        provider_type="ogr",
        feature_identity_json='{"fid": 1, "pk_field": "id", "pk_value": 1}',
        operation_type=op_type,
        attributes_json='{"all_attributes": {"name": "test"}}',
        geometry_wkb=geom_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="[]",
        user_name="tester",
        session_id="sess_1",
        created_at="2025-01-15T10:00:00Z",
        restored_from_event_id=None,
        entity_fingerprint=entity_fp,
        event_schema_version=2,
    )


# ---- _build_action (internal) ----

class TestBuildAction:
    def test_delete_produces_insert(self):
        action, conflict = _build_action(_evt(1, "DELETE"), require_fingerprint=False)
        assert action is not None
        assert action.compensatory_op == "INSERT"
        assert conflict is None

    def test_update_produces_update(self):
        action, conflict = _build_action(_evt(1, "UPDATE"), require_fingerprint=False)
        assert action.compensatory_op == "UPDATE"

    def test_insert_produces_delete(self):
        action, conflict = _build_action(_evt(1, "INSERT"), require_fingerprint=False)
        assert action.compensatory_op == "DELETE"

    def test_unsupported_op_merge(self):
        action, conflict = _build_action(_evt(1, "MERGE"), require_fingerprint=False)
        assert action is None
        assert conflict is not None
        assert conflict.severity == "blocking"
        assert "unsupported" in conflict.reason

    def test_unsupported_op_empty_string(self):
        action, conflict = _build_action(_evt(1, ""), require_fingerprint=False)
        assert action is None
        assert conflict.severity == "blocking"

    def test_unsupported_op_lowercase(self):
        action, conflict = _build_action(_evt(1, "delete"), require_fingerprint=False)
        assert action is None

    def test_missing_fp_no_require_produces_warning(self):
        action, conflict = _build_action(_evt(1, "DELETE", entity_fp=None), require_fingerprint=False)
        assert action is not None  # action still produced
        assert conflict is not None
        assert conflict.severity == "warning"
        assert "FID-based" in conflict.details

    def test_missing_fp_require_blocks(self):
        action, conflict = _build_action(_evt(1, "DELETE", entity_fp=None), require_fingerprint=True)
        assert action is None
        assert conflict.severity == "blocking"
        assert "stable identity" in conflict.details

    def test_missing_fp_empty_string_treated_as_missing(self):
        action, conflict = _build_action(_evt(1, "DELETE", entity_fp=""), require_fingerprint=True)
        assert action is None
        assert conflict.severity == "blocking"

    def test_event_id_none_uses_zero(self):
        action, conflict = _build_action(_evt(None, "DELETE"), require_fingerprint=False)
        assert action.event_id == 0

    def test_has_geometry_true(self):
        action, _ = _build_action(_evt(1, "UPDATE", geom_wkb=b'\x01\x02'), require_fingerprint=False)
        assert action.has_geometry is True

    def test_has_geometry_false(self):
        action, _ = _build_action(_evt(1, "UPDATE", geom_wkb=None), require_fingerprint=False)
        assert action.has_geometry is False


# ---- plan_event_restore ----

class TestPlanEventRestore:
    def test_single_delete(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        assert plan.mode == RestoreMode.EVENT
        assert plan.scope == RestoreScope.SELECTION
        assert plan.atomicity == AtomicityPolicy.BEST_EFFORT
        assert plan.event_count == 1
        assert plan.entity_count == 1
        assert plan.actions[0].compensatory_op == "INSERT"

    def test_all_three_ops(self):
        events = [_evt(1, "DELETE"), _evt(2, "UPDATE", entity_fp="pk:id=2"), _evt(3, "INSERT", entity_fp="pk:id=3")]
        plan = plan_event_restore(events, "ds", "lyr")
        ops = [a.compensatory_op for a in plan.actions]
        assert ops == ["INSERT", "UPDATE", "DELETE"]
        assert plan.entity_count == 3

    def test_same_entity_multiple_events(self):
        events = [_evt(1, "UPDATE", entity_fp="pk:id=1"), _evt(2, "UPDATE", entity_fp="pk:id=1")]
        plan = plan_event_restore(events, "ds", "lyr")
        assert plan.event_count == 2
        assert plan.entity_count == 1

    def test_empty_list(self):
        plan = plan_event_restore([], "ds", "lyr")
        assert plan.event_count == 0
        assert plan.entity_count == 0
        assert plan.actions == []
        assert plan.conflicts == []

    def test_unsupported_op_skipped_with_conflict(self):
        events = [_evt(1, "DELETE"), _evt(2, "MERGE"), _evt(3, "INSERT", entity_fp="pk:id=3")]
        plan = plan_event_restore(events, "ds", "lyr")
        assert plan.event_count == 2  # MERGE skipped
        assert len(plan.conflicts) == 1
        assert plan.conflicts[0].event_id == 2

    def test_all_unsupported_produces_empty_plan(self):
        events = [_evt(1, "MERGE"), _evt(2, "TRUNCATE")]
        plan = plan_event_restore(events, "ds", "lyr")
        assert plan.event_count == 0
        assert len(plan.conflicts) == 2

    def test_missing_fingerprint_warns_but_keeps_action(self):
        plan = plan_event_restore([_evt(1, "DELETE", entity_fp=None)], "ds", "lyr")
        assert plan.event_count == 1
        assert len(plan.conflicts) == 1
        assert plan.conflicts[0].severity == "warning"

    def test_multiple_missing_fingerprints_unique_entities(self):
        events = [_evt(1, "DELETE", entity_fp=None), _evt(2, "INSERT", entity_fp=None)]
        plan = plan_event_restore(events, "ds", "lyr")
        assert plan.event_count == 2
        assert plan.entity_count == 2  # "eid_1" != "eid_2"

    def test_conflict_policy_passed_through(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr", ConflictPolicy.FORCE)
        assert plan.conflict_policy == ConflictPolicy.FORCE

    def test_layer_name_none(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", None)
        assert plan.layer_name is None

    def test_cutoff_always_none(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        assert plan.cutoff is None


# ---- plan_temporal_restore ----

class TestPlanTemporalRestore:
    def test_basic_reverse_replay(self):
        events = [_evt(3, "UPDATE"), _evt(2, "UPDATE"), _evt(1, "INSERT")]
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore(events, "ds", "lyr", cutoff)
        assert plan.mode == RestoreMode.TEMPORAL
        assert plan.atomicity == AtomicityPolicy.STRICT
        assert plan.event_count == 3

    def test_missing_fingerprint_blocks_and_excludes(self):
        events = [_evt(1, "DELETE", entity_fp=None), _evt(2, "DELETE", entity_fp="pk:id=2")]
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-01-01T00:00:00Z", True)
        plan = plan_temporal_restore(events, "ds", "lyr", cutoff)
        assert plan.event_count == 1  # only event 2 included
        assert len(plan.conflicts) == 1
        assert plan.conflicts[0].severity == "blocking"
        assert plan.conflicts[0].event_id == 1

    def test_all_missing_fingerprints_empty_plan(self):
        events = [_evt(1, "DELETE", entity_fp=None), _evt(2, "INSERT", entity_fp=None)]
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore(events, "ds", "lyr", cutoff)
        assert plan.event_count == 0
        assert len(plan.conflicts) == 2

    def test_unsupported_op_blocked(self):
        events = [_evt(1, "MERGE")]
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore(events, "ds", "lyr", cutoff)
        assert plan.event_count == 0
        assert len(plan.conflicts) == 1

    def test_empty_events(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore([], "ds", "lyr", cutoff)
        assert plan.event_count == 0

    def test_scope_propagated(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore([_evt(1, "DELETE")], "ds", "lyr", cutoff, RestoreScope.DATASOURCE)
        assert plan.scope == RestoreScope.DATASOURCE

    def test_default_conflict_policy_abort(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore([_evt(1, "DELETE")], "ds", "lyr", cutoff)
        assert plan.conflict_policy == ConflictPolicy.ABORT

    def test_mixed_valid_and_blocked(self):
        events = [
            _evt(1, "MERGE"),                      # blocked: unsupported op
            _evt(2, "DELETE", entity_fp=None),      # blocked: no fingerprint
            _evt(3, "UPDATE", entity_fp="pk:id=3"),  # valid
            _evt(4, "INSERT", entity_fp="pk:id=4"),  # valid
        ]
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore(events, "ds", "lyr", cutoff)
        assert plan.event_count == 2
        assert len(plan.conflicts) == 2


# ---- preflight_check ----

class TestPreflightCheck:
    def test_go_on_valid_event_plan(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.GO
        assert report.blocking_reasons == []
        assert report.warnings == []

    def test_blocked_on_empty_plan(self):
        plan = plan_event_restore([], "ds", "lyr")
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED
        assert any("No actions" in r for r in report.blocking_reasons)

    def test_go_with_warnings_on_missing_fp(self):
        plan = plan_event_restore([_evt(1, "DELETE", entity_fp=None)], "ds", "lyr")
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.GO_WITH_WARNINGS
        assert len(report.warnings) == 1

    def test_blocked_on_invalid_cutoff(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, -1, True)
        plan = plan_temporal_restore([_evt(1, "DELETE")], "ds", "lyr", cutoff)
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED
        assert any("Invalid cutoff" in r for r in report.blocking_reasons)

    def test_blocked_on_temporal_best_effort_mismatch(self):
        plan = RestorePlan(
            mode=RestoreMode.TEMPORAL,
            scope=RestoreScope.LAYER,
            cutoff=RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True),
            atomicity=AtomicityPolicy.BEST_EFFORT,  # wrong for temporal
            conflict_policy=ConflictPolicy.ABORT,
            actions=[],
            conflicts=[],
            entity_count=0,
            event_count=0,
            datasource_fingerprint="ds",
            layer_name="lyr",
        )
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED
        assert any("STRICT" in r for r in report.blocking_reasons)

    def test_blocked_on_volume_exceeds_max_events(self):
        plan = RestorePlan(
            mode=RestoreMode.EVENT,
            scope=RestoreScope.SELECTION,
            cutoff=None,
            atomicity=AtomicityPolicy.BEST_EFFORT,
            conflict_policy=ConflictPolicy.SKIP,
            actions=[],
            conflicts=[],
            entity_count=1,
            event_count=MAX_EVENTS_PER_RESTORE + 1,
            datasource_fingerprint="ds",
            layer_name="lyr",
        )
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED
        assert any("Event count" in r for r in report.blocking_reasons)

    def test_blocked_on_volume_exceeds_max_entities(self):
        plan = RestorePlan(
            mode=RestoreMode.EVENT,
            scope=RestoreScope.SELECTION,
            cutoff=None,
            atomicity=AtomicityPolicy.BEST_EFFORT,
            conflict_policy=ConflictPolicy.SKIP,
            actions=[],
            conflicts=[],
            entity_count=MAX_ENTITIES_PER_RESTORE + 1,
            event_count=1,
            datasource_fingerprint="ds",
            layer_name="lyr",
        )
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED

    def test_blocking_conflict_propagates(self):
        events = [_evt(1, "DELETE", entity_fp=None)]
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore(events, "ds", "lyr", cutoff)
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED
        assert any("missing_entity_fingerprint" in r for r in report.blocking_reasons)

    def test_multiple_blocking_reasons_accumulated(self):
        plan = RestorePlan(
            mode=RestoreMode.TEMPORAL,
            scope=RestoreScope.LAYER,
            cutoff=RestoreCutoff(CutoffType.BY_EVENT_ID, -1, True),
            atomicity=AtomicityPolicy.BEST_EFFORT,
            conflict_policy=ConflictPolicy.ABORT,
            actions=[],
            conflicts=[],
            entity_count=MAX_ENTITIES_PER_RESTORE + 1,
            event_count=MAX_EVENTS_PER_RESTORE + 1,
            datasource_fingerprint="ds",
            layer_name="lyr",
        )
        report = preflight_check(plan)
        assert report.verdict == PreflightVerdict.BLOCKED
        assert len(report.blocking_reasons) >= 4  # volume x2 + cutoff + atomicity + no actions

    def test_estimated_duration_always_none(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        report = preflight_check(plan)
        assert report.estimated_duration_ms is None

    def test_plan_reference_in_report(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        report = preflight_check(plan)
        assert report.plan is plan


# ---- Preview formatting ----

class TestPreviewFormatting:
    def test_event_mode_label(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        assert "Evenement" in format_plan_summary(plan)

    def test_temporal_mode_label(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore([_evt(1, "DELETE")], "ds", "lyr", cutoff)
        assert "Temporel" in format_plan_summary(plan)

    def test_summary_shows_counts(self):
        events = [_evt(1, "DELETE"), _evt(2, "INSERT", entity_fp="pk:id=2")]
        plan = plan_event_restore(events, "ds", "lyr")
        summary = format_plan_summary(plan)
        assert "2" in summary  # event count

    def test_summary_shows_cutoff(self):
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-06-01", True)
        plan = plan_temporal_restore([_evt(1, "DELETE")], "ds", "lyr", cutoff)
        summary = format_plan_summary(plan)
        assert "2025-06-01" in summary

    def test_summary_strict_atomicity(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        plan = plan_temporal_restore([_evt(1, "DELETE")], "ds", "lyr", cutoff)
        assert "rollback" in format_plan_summary(plan)

    def test_summary_best_effort_atomicity(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        assert "isolation" in format_plan_summary(plan)

    def test_preflight_go_text(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        report = preflight_check(plan)
        text = format_preflight_report(report)
        assert "PRET" in text
        assert "BLOQUE" not in text

    def test_preflight_blocked_text(self):
        plan = plan_event_restore([], "ds", "lyr")
        report = preflight_check(plan)
        text = format_preflight_report(report)
        assert "BLOQUE" in text

    def test_preflight_warnings_shown(self):
        plan = plan_event_restore([_evt(1, "DELETE", entity_fp=None)], "ds", "lyr")
        report = preflight_check(plan)
        text = format_preflight_report(report)
        assert "avertissement" in text.lower() or "PRET" in text

    def test_dry_run_blocked_message(self):
        plan = plan_event_restore([], "ds", "lyr")
        report = preflight_check(plan)
        text = format_dry_run_message(report)
        assert "bloquee" in text

    def test_dry_run_go_message_asks_continue(self):
        plan = plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")
        report = preflight_check(plan)
        text = format_dry_run_message(report)
        assert "Continuer" in text

    def test_dry_run_shows_counts(self):
        events = [_evt(1, "DELETE"), _evt(2, "INSERT", entity_fp="pk:id=2")]
        plan = plan_event_restore(events, "ds", "lyr")
        report = preflight_check(plan)
        text = format_dry_run_message(report)
        assert "2" in text


# ---- check_retention_coverage (BL-09) ----

class TestRetentionCoverage:
    def test_by_event_id_always_ok(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 42, True)
        assert check_retention_coverage(cutoff, "2025-01-01") is None

    def test_no_events_blocks(self):
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-01-01", True)
        result = check_retention_coverage(cutoff, None)
        assert result is not None
        assert "No events" in result

    def test_cutoff_before_oldest_blocks(self):
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2024-06-01T00:00:00Z", True)
        result = check_retention_coverage(cutoff, "2025-01-01T00:00:00Z")
        assert result is not None
        assert "purged" in result.lower()
        assert "2024-06-01" in result
        assert "2025-01-01" in result

    def test_cutoff_after_oldest_ok(self):
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-06-01T00:00:00Z", True)
        assert check_retention_coverage(cutoff, "2025-01-01T00:00:00Z") is None

    def test_cutoff_equal_oldest_ok(self):
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-01-01T00:00:00Z", True)
        assert check_retention_coverage(cutoff, "2025-01-01T00:00:00Z") is None

    def test_cutoff_one_second_before_oldest_blocks(self):
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2024-12-31T23:59:59Z", True)
        result = check_retention_coverage(cutoff, "2025-01-01T00:00:00Z")
        assert result is not None


# ---- build_restore_session (BL-05 / GAP-04) ----

class TestBuildRestoreSession:
    def _plan(self):
        return plan_event_restore([_evt(1, "DELETE")], "ds", "lyr")

    def _report(self, succeeded, failed):
        from recoverland.core.audit_backend import RestoreReport
        return RestoreReport(succeeded=succeeded, failed=failed, total_requested=len(succeeded) + len(failed))

    def test_completed_status(self):
        from recoverland.core.restore_executor import build_restore_session
        plan = self._plan()
        report = self._report([1], {})
        session = build_restore_session(plan, report, "2025-01-01T00:00:00Z", "2025-01-01T00:00:01Z")
        assert session.status == "completed"
        assert session.succeeded_count == 1
        assert session.failed_count == 0
        assert session.session_id  # non-empty UUID

    def test_failed_status(self):
        from recoverland.core.restore_executor import build_restore_session
        plan = self._plan()
        report = self._report([], {1: "error"})
        session = build_restore_session(plan, report, "t0", "t1")
        assert session.status == "failed"

    def test_partial_status(self):
        from recoverland.core.restore_executor import build_restore_session
        plan = self._plan()
        report = self._report([1], {2: "error"})
        session = build_restore_session(plan, report, "t0", "t1")
        assert session.status == "partial"

    def test_cancelled_status(self):
        from recoverland.core.restore_executor import build_restore_session
        plan = self._plan()
        report = self._report([], {})
        session = build_restore_session(plan, report, "t0", "t1")
        assert session.status == "cancelled"

    def test_session_carries_plan_metadata(self):
        from recoverland.core.restore_executor import build_restore_session
        plan = self._plan()
        report = self._report([1], {})
        session = build_restore_session(plan, report, "t0", "t1")
        assert session.mode == RestoreMode.EVENT
        assert session.scope == RestoreScope.SELECTION
        assert session.datasource_fingerprint == "ds"
        assert session.layer_name == "lyr"
        assert session.started_at == "t0"
        assert session.finished_at == "t1"

    def test_session_id_is_unique(self):
        from recoverland.core.restore_executor import build_restore_session
        plan = self._plan()
        report = self._report([1], {})
        s1 = build_restore_session(plan, report, "t0", "t1")
        s2 = build_restore_session(plan, report, "t0", "t1")
        assert s1.session_id != s2.session_id
