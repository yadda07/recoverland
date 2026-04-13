"""Tests for core.serialization module (RLU-029)."""
import json
import math
import base64
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.serialization import (
    serialize_value, deserialize_value, compute_update_delta,
    build_full_snapshot, _values_equal,
)


class TestSerializeValue(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(serialize_value(None))

    def test_string_passthrough(self):
        self.assertEqual(serialize_value("hello"), "hello")

    def test_empty_string(self):
        self.assertEqual(serialize_value(""), "")

    def test_int_passthrough(self):
        self.assertEqual(serialize_value(42), 42)

    def test_float_passthrough(self):
        self.assertAlmostEqual(serialize_value(3.14), 3.14)

    def test_float_nan_becomes_none(self):
        self.assertIsNone(serialize_value(float('nan')))

    def test_float_inf_becomes_none(self):
        self.assertIsNone(serialize_value(float('inf')))

    def test_bool_true(self):
        self.assertIs(serialize_value(True), True)

    def test_bool_false(self):
        self.assertIs(serialize_value(False), False)

    def test_bytes_to_base64(self):
        raw = b'\x00\x01\x02'
        result = serialize_value(raw)
        self.assertTrue(result.startswith("b64:"))
        decoded = base64.b64decode(result[4:])
        self.assertEqual(decoded, raw)

    def test_list_serialization(self):
        result = serialize_value([1, "two", None])
        self.assertEqual(result, [1, "two", None])

    def test_dict_serialization(self):
        result = serialize_value({"a": 1, "b": None})
        self.assertEqual(result, {"a": 1, "b": None})

    def test_datetime_iso(self):
        from datetime import datetime
        dt = datetime(2025, 3, 15, 10, 30, 0)
        result = serialize_value(dt)
        self.assertEqual(result, "2025-03-15T10:30:00")

    def test_date_iso(self):
        from datetime import date
        d = date(2025, 3, 15)
        result = serialize_value(d)
        self.assertEqual(result, "2025-03-15")

    def test_time_iso(self):
        from datetime import time
        t = time(10, 30, 0)
        result = serialize_value(t)
        self.assertEqual(result, "10:30:00")

    def test_unknown_type_fallback_to_str(self):
        class Custom:
            def __str__(self):
                return "custom_value"
        result = serialize_value(Custom())
        self.assertEqual(result, "custom_value")


class TestDeserializeValue(unittest.TestCase):

    def test_none_returns_none(self):
        self.assertIsNone(deserialize_value(None, "int"))

    def test_int_coercion(self):
        self.assertEqual(deserialize_value("42", "int"), 42)

    def test_float_coercion(self):
        self.assertAlmostEqual(deserialize_value("3.14", "double"), 3.14)

    def test_bool_coercion(self):
        self.assertTrue(deserialize_value(1, "bool"))

    def test_string_coercion(self):
        self.assertEqual(deserialize_value(42, "str"), "42")

    def test_blob_from_base64(self):
        encoded = "b64:" + base64.b64encode(b'\x00\x01').decode()
        result = deserialize_value(encoded, "QByteArray")
        self.assertEqual(result, b'\x00\x01')


class TestUpdateDelta(unittest.TestCase):

    def test_no_change_returns_none(self):
        old = {"a": 1, "b": "x"}
        new = {"a": 1, "b": "x"}
        self.assertIsNone(compute_update_delta(old, new))

    def test_single_change(self):
        old = {"a": 1, "b": "x"}
        new = {"a": 1, "b": "y"}
        result = json.loads(compute_update_delta(old, new))
        self.assertIn("changed_only", result)
        self.assertIn("b", result["changed_only"])
        self.assertEqual(result["changed_only"]["b"]["old"], "x")
        self.assertEqual(result["changed_only"]["b"]["new"], "y")

    def test_multiple_changes(self):
        old = {"a": 1, "b": "x", "c": 3}
        new = {"a": 2, "b": "x", "c": 4}
        result = json.loads(compute_update_delta(old, new))
        changed = result["changed_only"]
        self.assertEqual(len(changed), 2)
        self.assertIn("a", changed)
        self.assertIn("c", changed)

    def test_null_to_value(self):
        old = {"a": None}
        new = {"a": 42}
        result = json.loads(compute_update_delta(old, new))
        self.assertEqual(result["changed_only"]["a"]["old"], None)
        self.assertEqual(result["changed_only"]["a"]["new"], 42)

    def test_layer_audit_fields_are_ignored(self):
        old = {"date modif": "2026-03-23T14:00:00", "modif par": "alice"}
        new = {"date modif": "2026-03-23T15:00:00", "modif par": "bob"}
        delta = compute_update_delta(old, new, ["date modif", "modif par"])
        self.assertIsNone(delta)

    def test_changed_field_scope_keeps_only_business_fields(self):
        old = {
            "name": "A",
            "date modif": "2026-03-23T14:00:00",
            "modif par": "alice",
        }
        new = {
            "name": "B",
            "date modif": "2026-03-23T15:00:00",
            "modif par": "bob",
        }
        result = json.loads(compute_update_delta(
            old, new, ["name", "date modif", "modif par"]))
        self.assertEqual(set(result["changed_only"].keys()), {"name"})


class TestFullSnapshot(unittest.TestCase):

    def test_snapshot_format(self):
        attrs = {"name": "Dupont", "age": 42}
        result = json.loads(build_full_snapshot(attrs))
        self.assertIn("all_attributes", result)
        self.assertEqual(result["all_attributes"]["name"], "Dupont")
        self.assertEqual(result["all_attributes"]["age"], 42)


class TestValuesEqual(unittest.TestCase):

    def test_both_none(self):
        self.assertTrue(_values_equal(None, None))

    def test_one_none(self):
        self.assertFalse(_values_equal(None, 1))
        self.assertFalse(_values_equal(1, None))

    def test_nan_equal(self):
        self.assertTrue(_values_equal(float('nan'), float('nan')))

    def test_different_values(self):
        self.assertFalse(_values_equal(1, 2))

    def test_same_values(self):
        self.assertTrue(_values_equal("abc", "abc"))


class TestSerializeEdgeCases(unittest.TestCase):

    def test_negative_zero_float(self):
        result = serialize_value(-0.0)
        self.assertEqual(result, 0.0)

    def test_very_large_int(self):
        big = 2**63
        self.assertEqual(serialize_value(big), big)

    def test_very_small_float(self):
        tiny = 1e-300
        result = serialize_value(tiny)
        self.assertAlmostEqual(result, tiny)

    def test_nested_dict_in_list(self):
        val = [{"a": [1, 2]}, {"b": {"c": 3}}]
        result = serialize_value(val)
        self.assertEqual(result, val)

    def test_empty_list(self):
        self.assertEqual(serialize_value([]), [])

    def test_empty_bytes(self):
        result = serialize_value(b'')
        self.assertTrue(result.startswith("b64:"))

    def test_large_bytes(self):
        data = b'\xFF' * 100_000
        result = serialize_value(data)
        self.assertTrue(result.startswith("b64:"))
        decoded = base64.b64decode(result[4:])
        self.assertEqual(decoded, data)

    def test_null_representation_becomes_none(self):
        self.assertIsNone(serialize_value("NULL"))


class TestDeserializeEdgeCases(unittest.TestCase):

    def test_float_from_int_string(self):
        result = deserialize_value("42", "double")
        self.assertAlmostEqual(result, 42.0)

    def test_bool_from_zero(self):
        self.assertFalse(deserialize_value(0, "bool"))

    def test_none_passthrough_all_types(self):
        for t in ("int", "double", "bool", "str", "QByteArray"):
            self.assertIsNone(deserialize_value(None, t))

    def test_blob_non_b64_prefix_returns_none(self):
        result = deserialize_value("regular string", "QByteArray")
        self.assertIsNone(result)


class TestUpdateDeltaEdgeCases(unittest.TestCase):

    def test_empty_dicts_returns_none(self):
        self.assertIsNone(compute_update_delta({}, {}))

    def test_new_field_in_new_only(self):
        old = {"a": 1}
        new = {"a": 1, "b": 2}
        result = compute_update_delta(old, new)
        if result is not None:
            parsed = json.loads(result)
            self.assertIn("b", parsed["changed_only"])

    def test_old_field_missing_in_new(self):
        old = {"a": 1, "b": 2}
        new = {"a": 1}
        result = compute_update_delta(old, new)
        if result is not None:
            parsed = json.loads(result)
            self.assertIn("b", parsed["changed_only"])

    def test_none_to_none_no_change(self):
        old = {"a": None}
        new = {"a": None}
        self.assertIsNone(compute_update_delta(old, new))

    def test_nan_to_nan_no_change(self):
        old = {"a": float('nan')}
        new = {"a": float('nan')}
        self.assertIsNone(compute_update_delta(old, new))

    def test_all_audit_fields_ignored(self):
        old = {"date modif": "a", "modif par": "x", "name": "A"}
        new = {"date modif": "b", "modif par": "y", "name": "A"}
        result = compute_update_delta(old, new, ["date modif", "modif par", "name"])
        self.assertIsNone(result)


class TestFullSnapshotEdgeCases(unittest.TestCase):

    def test_empty_attrs(self):
        result = json.loads(build_full_snapshot({}))
        self.assertEqual(result["all_attributes"], {})

    def test_attrs_with_none_values(self):
        result = json.loads(build_full_snapshot({"a": None, "b": 1}))
        self.assertIsNone(result["all_attributes"]["a"])
        self.assertEqual(result["all_attributes"]["b"], 1)

    def test_large_attrs(self):
        attrs = {f"field_{i}": f"value_{i}" for i in range(1000)}
        result = json.loads(build_full_snapshot(attrs))
        self.assertEqual(len(result["all_attributes"]), 1000)


if __name__ == '__main__':
    unittest.main()
