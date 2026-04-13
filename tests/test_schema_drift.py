"""Tests for core.schema_drift module (RLU-053)."""
import json
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.schema_drift import (
    parse_field_schema, compare_schemas, build_field_mapping,
    format_drift_message, FieldInfo, DriftReport,
)


class TestParseFieldSchema(unittest.TestCase):

    def test_valid_schema(self):
        schema_json = json.dumps([
            {"name": "gid", "type": "integer", "length": 0, "precision": 0},
            {"name": "nom", "type": "varchar", "length": 100, "precision": 0},
        ])
        result = parse_field_schema(schema_json)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "gid")
        self.assertEqual(result[1].type_name, "varchar")

    def test_empty_json(self):
        self.assertEqual(parse_field_schema("[]"), [])

    def test_invalid_json(self):
        self.assertEqual(parse_field_schema("not json"), [])

    def test_none_input(self):
        self.assertEqual(parse_field_schema(None), [])


class TestCompareSchemas(unittest.TestCase):

    def _make_schema(self, fields):
        return [FieldInfo(name=f[0], type_name=f[1], length=0, precision=0) for f in fields]

    def test_identical_schemas(self):
        schema = self._make_schema([("gid", "integer"), ("nom", "varchar")])
        drift = compare_schemas(schema, schema)
        self.assertTrue(drift.is_compatible)
        self.assertEqual(len(drift.matched), 2)
        self.assertEqual(len(drift.missing_in_current), 0)

    def test_missing_field_in_current(self):
        historical = self._make_schema([("gid", "integer"), ("old_col", "varchar")])
        current = self._make_schema([("gid", "integer")])
        drift = compare_schemas(historical, current)
        self.assertFalse(drift.is_compatible)
        self.assertIn("old_col", drift.missing_in_current)

    def test_added_field_in_current(self):
        historical = self._make_schema([("gid", "integer")])
        current = self._make_schema([("gid", "integer"), ("new_col", "text")])
        drift = compare_schemas(historical, current)
        self.assertTrue(drift.is_compatible)
        self.assertIn("new_col", drift.added_in_current)

    def test_type_change_incompatible(self):
        historical = self._make_schema([("gid", "integer"), ("val", "varchar")])
        current = self._make_schema([("gid", "integer"), ("val", "boolean")])
        drift = compare_schemas(historical, current)
        self.assertFalse(drift.is_compatible)
        self.assertIn("val", drift.type_changed)

    def test_type_change_compatible(self):
        historical = self._make_schema([("gid", "int4"), ("name", "varchar")])
        current = self._make_schema([("gid", "integer"), ("name", "text")])
        drift = compare_schemas(historical, current)
        self.assertTrue(drift.is_compatible)
        self.assertEqual(len(drift.type_changed), 0)


class TestBuildFieldMapping(unittest.TestCase):

    def test_all_matched(self):
        historical = [FieldInfo("a", "int", 0, 0), FieldInfo("b", "text", 0, 0)]
        drift = DriftReport(
            matched=["a", "b"], missing_in_current=[], added_in_current=[],
            type_changed={}, is_compatible=True,
        )
        mapping = build_field_mapping(drift, historical)
        self.assertEqual(mapping, {"a": "a", "b": "b"})

    def test_missing_excluded(self):
        historical = [FieldInfo("a", "int", 0, 0), FieldInfo("b", "text", 0, 0)]
        drift = DriftReport(
            matched=["a"], missing_in_current=["b"], added_in_current=[],
            type_changed={}, is_compatible=False,
        )
        mapping = build_field_mapping(drift, historical)
        self.assertNotIn("b", mapping)
        self.assertIn("a", mapping)


class TestFormatDriftMessage(unittest.TestCase):

    def test_compatible_message(self):
        drift = DriftReport(
            matched=["a", "b"], missing_in_current=[], added_in_current=[],
            type_changed={}, is_compatible=True,
        )
        msg = format_drift_message(drift)
        self.assertIn("compatible", msg.lower())

    def test_incompatible_message(self):
        drift = DriftReport(
            matched=["a"], missing_in_current=["old_col"], added_in_current=["new_col"],
            type_changed={"val": "int -> text"}, is_compatible=False,
        )
        msg = format_drift_message(drift)
        self.assertIn("old_col", msg)
        self.assertIn("new_col", msg)
        self.assertIn("val", msg)


class TestSchemaDriftEdgeCases(unittest.TestCase):

    def _make_schema(self, fields):
        return [FieldInfo(name=f[0], type_name=f[1], length=0, precision=0) for f in fields]

    def test_empty_historical_empty_current(self):
        drift = compare_schemas([], [])
        self.assertTrue(drift.is_compatible)
        self.assertEqual(len(drift.matched), 0)
        self.assertEqual(len(drift.missing_in_current), 0)
        self.assertEqual(len(drift.added_in_current), 0)

    def test_empty_historical_non_empty_current(self):
        current = self._make_schema([("gid", "integer")])
        drift = compare_schemas([], current)
        self.assertTrue(drift.is_compatible)
        self.assertIn("gid", drift.added_in_current)

    def test_non_empty_historical_empty_current(self):
        historical = self._make_schema([("gid", "integer")])
        drift = compare_schemas(historical, [])
        self.assertFalse(drift.is_compatible)
        self.assertIn("gid", drift.missing_in_current)

    def test_many_fields_all_matched(self):
        fields = [(f"field_{i}", "varchar") for i in range(100)]
        schema = self._make_schema(fields)
        drift = compare_schemas(schema, schema)
        self.assertTrue(drift.is_compatible)
        self.assertEqual(len(drift.matched), 100)

    def test_field_order_does_not_affect_compatibility(self):
        historical = self._make_schema([("a", "int"), ("b", "text"), ("c", "double")])
        current = self._make_schema([("c", "double"), ("a", "int"), ("b", "text")])
        drift = compare_schemas(historical, current)
        self.assertTrue(drift.is_compatible)
        self.assertEqual(len(drift.matched), 3)

    def test_multiple_type_changes(self):
        historical = self._make_schema([("a", "integer"), ("b", "varchar"), ("c", "double")])
        current = self._make_schema([("a", "boolean"), ("b", "integer"), ("c", "double")])
        drift = compare_schemas(historical, current)
        self.assertFalse(drift.is_compatible)
        self.assertGreaterEqual(len(drift.type_changed), 1)

    def test_simultaneous_add_and_remove(self):
        historical = self._make_schema([("a", "int"), ("old", "text")])
        current = self._make_schema([("a", "int"), ("new", "text")])
        drift = compare_schemas(historical, current)
        self.assertFalse(drift.is_compatible)
        self.assertIn("old", drift.missing_in_current)
        self.assertIn("new", drift.added_in_current)


class TestParseFieldSchemaEdgeCases(unittest.TestCase):

    def test_json_with_extra_keys_no_crash(self):
        schema_json = json.dumps([
            {"name": "gid", "type": "integer", "length": 0, "precision": 0, "extra": True}
        ])
        result = parse_field_schema(schema_json)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "gid")

    def test_json_missing_optional_fields(self):
        schema_json = json.dumps([{"name": "gid", "type": "integer"}])
        result = parse_field_schema(schema_json)
        self.assertEqual(len(result), 1)

    def test_json_number_literal(self):
        self.assertEqual(parse_field_schema("42"), [])

    def test_json_string_literal(self):
        self.assertEqual(parse_field_schema('"hello"'), [])

    def test_empty_string(self):
        self.assertEqual(parse_field_schema(""), [])


class TestBuildFieldMappingEdgeCases(unittest.TestCase):

    def test_empty_matched(self):
        drift = DriftReport(
            matched=[], missing_in_current=[], added_in_current=[],
            type_changed={}, is_compatible=True,
        )
        mapping = build_field_mapping(drift, [])
        self.assertEqual(mapping, {})

    def test_type_changed_field_still_mapped(self):
        historical = [FieldInfo("a", "int", 0, 0)]
        drift = DriftReport(
            matched=["a"], missing_in_current=[], added_in_current=[],
            type_changed={"a": "int -> boolean"}, is_compatible=False,
        )
        mapping = build_field_mapping(drift, historical)
        self.assertIn("a", mapping)


if __name__ == '__main__':
    unittest.main()
