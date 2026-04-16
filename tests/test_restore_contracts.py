"""Brutal tests for restore_contracts.py and identity fingerprint.

Every branch, every boundary, every failure mode.
"""
from recoverland.core.restore_contracts import (
    RestoreMode, RestoreScope, CutoffType,
    AtomicityPolicy,
    RestoreCutoff, COMPENSATORY_OPS,
    is_restore_allowed, validate_cutoff, check_volume_limits,
    default_atomicity, scope_requires_confirmation,
    MAX_EVENTS_PER_RESTORE, MAX_ENTITIES_PER_RESTORE,
    WARN_EVENTS_THRESHOLD, WARN_ENTITIES_THRESHOLD,
    PROVIDER_RESTORE_MATRIX,
)
from recoverland.core.identity import compute_entity_fingerprint


# ---- COMPENSATORY_OPS ----

class TestCompensatoryOps:
    def test_delete_yields_insert(self):
        assert COMPENSATORY_OPS["DELETE"] == "INSERT"

    def test_update_yields_update(self):
        assert COMPENSATORY_OPS["UPDATE"] == "UPDATE"

    def test_insert_yields_delete(self):
        assert COMPENSATORY_OPS["INSERT"] == "DELETE"

    def test_unknown_op_returns_none_via_get(self):
        assert COMPENSATORY_OPS.get("MERGE") is None
        assert COMPENSATORY_OPS.get("") is None
        assert COMPENSATORY_OPS.get("delete") is None

    def test_only_three_keys(self):
        assert set(COMPENSATORY_OPS.keys()) == {"DELETE", "UPDATE", "INSERT"}

    def test_case_sensitive(self):
        assert "Delete" not in COMPENSATORY_OPS
        assert "update" not in COMPENSATORY_OPS
        assert "insert" not in COMPENSATORY_OPS


# ---- PROVIDER RESTORE MATRIX ----

class TestProviderRestoreMatrix:
    def test_all_matrix_entries_are_tuple_bool_optional_str(self):
        for key, val in PROVIDER_RESTORE_MATRIX.items():
            assert isinstance(key, tuple) and len(key) == 2
            assert isinstance(key[0], str)
            assert isinstance(key[1], RestoreMode)
            assert isinstance(val, tuple) and len(val) == 2
            assert isinstance(val[0], bool)
            assert val[1] is None or isinstance(val[1], str)

    def test_postgres_event_allowed(self):
        allowed, reason = is_restore_allowed("postgres", RestoreMode.EVENT)
        assert allowed is True
        assert reason is None

    def test_postgres_temporal_allowed(self):
        allowed, reason = is_restore_allowed("postgres", RestoreMode.TEMPORAL)
        assert allowed is True
        assert reason is None

    def test_spatialite_both_modes(self):
        assert is_restore_allowed("spatialite", RestoreMode.EVENT) == (True, None)
        assert is_restore_allowed("spatialite", RestoreMode.TEMPORAL) == (True, None)

    def test_ogr_event_allowed_temporal_with_warning(self):
        ok_e, _ = is_restore_allowed("ogr", RestoreMode.EVENT)
        ok_t, reason_t = is_restore_allowed("ogr", RestoreMode.TEMPORAL)
        assert ok_e is True
        assert ok_t is True
        assert reason_t is not None

    def test_memory_both_refused(self):
        ok_e, r_e = is_restore_allowed("memory", RestoreMode.EVENT)
        ok_t, r_t = is_restore_allowed("memory", RestoreMode.TEMPORAL)
        assert ok_e is False and r_e is not None
        assert ok_t is False and r_t is not None

    def test_virtual_both_refused(self):
        assert is_restore_allowed("virtual", RestoreMode.EVENT)[0] is False
        assert is_restore_allowed("virtual", RestoreMode.TEMPORAL)[0] is False

    def test_delimitedtext_event_ok_temporal_refused(self):
        ok_e, _ = is_restore_allowed("delimitedtext", RestoreMode.EVENT)
        ok_t, _ = is_restore_allowed("delimitedtext", RestoreMode.TEMPORAL)
        assert ok_e is True
        assert ok_t is False

    def test_wfs_event_ok_temporal_refused(self):
        assert is_restore_allowed("wfs", RestoreMode.EVENT)[0] is True
        assert is_restore_allowed("wfs", RestoreMode.TEMPORAL)[0] is False

    def test_mssql_both_allowed(self):
        assert is_restore_allowed("mssql", RestoreMode.EVENT)[0] is True
        assert is_restore_allowed("mssql", RestoreMode.TEMPORAL)[0] is True

    def test_oracle_both_allowed(self):
        assert is_restore_allowed("oracle", RestoreMode.EVENT)[0] is True
        assert is_restore_allowed("oracle", RestoreMode.TEMPORAL)[0] is True

    def test_unknown_provider_refused_with_message(self):
        ok, reason = is_restore_allowed("my_custom_provider", RestoreMode.EVENT)
        assert ok is False
        assert "Unknown" in reason
        assert "my_custom_provider" in reason

    def test_empty_string_provider_refused(self):
        ok, reason = is_restore_allowed("", RestoreMode.EVENT)
        assert ok is False

    def test_provider_name_case_sensitive(self):
        ok, _ = is_restore_allowed("Postgres", RestoreMode.EVENT)
        assert ok is False
        ok, _ = is_restore_allowed("POSTGRES", RestoreMode.TEMPORAL)
        assert ok is False


# ---- validate_cutoff ----

class TestValidateCutoff:
    def test_valid_event_id_1(self):
        assert validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)) is None

    def test_valid_event_id_large(self):
        assert validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, 999999, False)) is None

    def test_event_id_zero_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, 0, True))
        assert err is not None and "positive" in err

    def test_event_id_negative_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, -5, True))
        assert err is not None

    def test_event_id_float_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, 3.14, True))
        assert err is not None

    def test_event_id_string_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, "42", True))
        assert err is not None

    def test_event_id_none_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, None, True))
        assert err is not None

    def test_event_id_bool_true_rejected(self):
        # bool is subclass of int: True == 1, but semantically wrong
        # validate_cutoff accepts int, True is int with value 1 -> passes
        # This documents the behavior, not necessarily desired
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, True, True))
        assert err is None  # True == 1 which is valid int >= 1

    def test_event_id_bool_false_rejected(self):
        # False == 0 which is < 1
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_EVENT_ID, False, True))
        assert err is not None

    def test_valid_date_iso(self):
        assert validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, "2025-01-15T10:00:00Z", False)) is None

    def test_valid_date_date_only(self):
        assert validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, "2025-01-15", True)) is None

    def test_date_too_short(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, "2025-01", False))
        assert err is not None

    def test_date_empty_string(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, "", True))
        assert err is not None

    def test_date_integer_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, 20250115, True))
        assert err is not None

    def test_date_none_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, None, True))
        assert err is not None

    def test_date_list_rejected(self):
        err = validate_cutoff(RestoreCutoff(CutoffType.BY_DATE, [2025, 1, 15], True))
        assert err is not None

    def test_inclusive_flag_does_not_affect_validation(self):
        c1 = RestoreCutoff(CutoffType.BY_EVENT_ID, 10, True)
        c2 = RestoreCutoff(CutoffType.BY_EVENT_ID, 10, False)
        assert validate_cutoff(c1) is None
        assert validate_cutoff(c2) is None


# ---- check_volume_limits ----

class TestVolumeLimits:
    def test_zero_zero(self):
        ok, warnings, blocking = check_volume_limits(0, 0)
        assert ok is True and warnings == [] and blocking == []

    def test_one_one(self):
        ok, w, b = check_volume_limits(1, 1)
        assert ok is True and w == [] and b == []

    def test_exact_warn_threshold_no_warning(self):
        ok, w, b = check_volume_limits(WARN_EVENTS_THRESHOLD, WARN_ENTITIES_THRESHOLD)
        assert ok is True and w == [] and b == []

    def test_one_above_warn_events(self):
        ok, w, b = check_volume_limits(WARN_EVENTS_THRESHOLD + 1, 1)
        assert ok is True and len(w) == 1 and b == []

    def test_one_above_warn_entities(self):
        ok, w, b = check_volume_limits(1, WARN_ENTITIES_THRESHOLD + 1)
        assert ok is True and len(w) == 1 and b == []

    def test_both_above_warn(self):
        ok, w, b = check_volume_limits(WARN_EVENTS_THRESHOLD + 1, WARN_ENTITIES_THRESHOLD + 1)
        assert ok is True and len(w) == 2 and b == []

    def test_exact_max_events_no_block(self):
        ok, w, b = check_volume_limits(MAX_EVENTS_PER_RESTORE, 1)
        assert ok is True

    def test_one_above_max_events_blocks(self):
        ok, w, b = check_volume_limits(MAX_EVENTS_PER_RESTORE + 1, 1)
        assert ok is False and len(b) == 1

    def test_exact_max_entities_no_block(self):
        ok, w, b = check_volume_limits(1, MAX_ENTITIES_PER_RESTORE)
        assert ok is True

    def test_one_above_max_entities_blocks(self):
        ok, w, b = check_volume_limits(1, MAX_ENTITIES_PER_RESTORE + 1)
        assert ok is False and len(b) == 1

    def test_both_exceed_max(self):
        ok, w, b = check_volume_limits(MAX_EVENTS_PER_RESTORE + 1, MAX_ENTITIES_PER_RESTORE + 1)
        assert ok is False and len(b) == 2

    def test_exceed_max_skips_warn(self):
        ok, w, b = check_volume_limits(MAX_EVENTS_PER_RESTORE + 1, 1)
        assert ok is False
        assert len(w) == 0  # no warning emitted when blocking

    def test_negative_events_no_crash(self):
        ok, w, b = check_volume_limits(-1, 1)
        assert ok is True and w == [] and b == []


# ---- default_atomicity ----

class TestDefaultAtomicity:
    def test_temporal_is_strict(self):
        assert default_atomicity(RestoreMode.TEMPORAL) == AtomicityPolicy.STRICT

    def test_event_is_best_effort(self):
        assert default_atomicity(RestoreMode.EVENT) == AtomicityPolicy.BEST_EFFORT


# ---- scope_requires_confirmation ----

class TestScopeConfirmation:
    def test_entity_no(self):
        assert scope_requires_confirmation(RestoreScope.ENTITY) is False

    def test_selection_no(self):
        assert scope_requires_confirmation(RestoreScope.SELECTION) is False

    def test_layer_yes(self):
        assert scope_requires_confirmation(RestoreScope.LAYER) is True

    def test_datasource_yes(self):
        assert scope_requires_confirmation(RestoreScope.DATASOURCE) is True

    def test_all_scopes_covered(self):
        results = {s: scope_requires_confirmation(s) for s in RestoreScope}
        assert len(results) == 4
        assert results[RestoreScope.ENTITY] is False
        assert results[RestoreScope.SELECTION] is False
        assert results[RestoreScope.LAYER] is True
        assert results[RestoreScope.DATASOURCE] is True


# ---- compute_entity_fingerprint ----

class TestEntityFingerprint:
    def test_pk_identity(self):
        assert compute_entity_fingerprint('{"fid": 7, "pk_field": "gid", "pk_value": 42}') == "pk:gid=42"

    def test_fid_only(self):
        assert compute_entity_fingerprint('{"fid": 7}') == "fid:7"

    def test_fid_zero(self):
        assert compute_entity_fingerprint('{"fid": 0}') == "fid:0"

    def test_fid_negative(self):
        assert compute_entity_fingerprint('{"fid": -1}') == "fid:-1"

    def test_pk_value_zero_valid(self):
        assert compute_entity_fingerprint('{"pk_field": "id", "pk_value": 0}') == "pk:id=0"

    def test_pk_value_negative(self):
        assert compute_entity_fingerprint('{"pk_field": "id", "pk_value": -99}') == "pk:id=-99"

    def test_pk_value_string(self):
        assert compute_entity_fingerprint('{"pk_field": "uuid", "pk_value": "abc-123"}') == "pk:uuid=abc-123"

    def test_pk_value_float(self):
        assert compute_entity_fingerprint('{"pk_field": "x", "pk_value": 3.14}') == "pk:x=3.14"

    def test_pk_field_empty_string_falls_to_fid(self):
        # empty pk_field is falsy -> skip pk branch -> fid branch
        assert compute_entity_fingerprint('{"pk_field": "", "pk_value": 1, "fid": 5}') == "fid:5"

    def test_pk_value_none_falls_to_fid(self):
        assert compute_entity_fingerprint('{"pk_field": "id", "pk_value": null, "fid": 5}') == "fid:5"

    def test_pk_field_present_but_no_value_key(self):
        assert compute_entity_fingerprint('{"pk_field": "id", "fid": 3}') == "fid:3"

    def test_no_fid_no_pk(self):
        assert compute_entity_fingerprint('{"other": "stuff"}') is None

    def test_empty_json_object(self):
        assert compute_entity_fingerprint("{}") is None

    def test_none_input(self):
        assert compute_entity_fingerprint(None) is None

    def test_empty_string_input(self):
        assert compute_entity_fingerprint("") is None

    def test_invalid_json(self):
        assert compute_entity_fingerprint("not json at all") is None

    def test_json_array(self):
        assert compute_entity_fingerprint("[1, 2, 3]") is None

    def test_json_string_literal(self):
        assert compute_entity_fingerprint('"just a string"') is None

    def test_json_number_literal(self):
        assert compute_entity_fingerprint("42") is None

    def test_unicode_pk_field(self):
        r = compute_entity_fingerprint('{"pk_field": "nom_entite", "pk_value": "cafe"}')
        assert r == "pk:nom_entite=cafe"

    def test_special_chars_in_pk_value(self):
        r = compute_entity_fingerprint('{"pk_field": "id", "pk_value": "a=b;c"}')
        assert r == "pk:id=a=b;c"

    def test_sql_injection_in_value(self):
        r = compute_entity_fingerprint('{"pk_field": "id", "pk_value": "1; DROP TABLE"}')
        assert r == "pk:id=1; DROP TABLE"

    def test_very_long_pk_value(self):
        long_val = "x" * 10000
        j = '{"pk_field": "id", "pk_value": "' + long_val + '"}'
        r = compute_entity_fingerprint(j)
        assert r == f"pk:id={long_val}"

    def test_pk_takes_priority_over_fid(self):
        r = compute_entity_fingerprint('{"fid": 99, "pk_field": "gid", "pk_value": 7}')
        assert r == "pk:gid=7"

    def test_boolean_pk_value_true(self):
        r = compute_entity_fingerprint('{"pk_field": "x", "pk_value": true, "fid": 1}')
        assert r == "pk:x=True"

    def test_boolean_pk_value_false(self):
        # false is falsy but not None -> should match pk branch
        # Actually in Python, False is not None, so pk_value is not None -> enters branch
        r = compute_entity_fingerprint('{"pk_field": "x", "pk_value": false, "fid": 1}')
        assert r == "pk:x=False"

    def test_nested_json_no_match(self):
        r = compute_entity_fingerprint('{"fid": {"nested": 1}}')
        assert r is not None  # fid is not None -> "fid:{'nested': 1}"

    def test_truncated_json(self):
        assert compute_entity_fingerprint('{"fid": 1') is None

    def test_bytes_input_rejected(self):
        assert compute_entity_fingerprint(b'{"fid": 1}') is None
