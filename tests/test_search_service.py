"""Tests for core.search_service module (RLU-030, RLU-031, RLU-032)."""
import sqlite3
import json
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.audit_backend import AuditEvent, SearchCriteria
from recoverland.core.search_service import (
    search_events, count_events, get_event_by_id,
    get_distinct_layers, get_distinct_users, reconstruct_attributes,
    is_geometry_only_update, summarize_scope,
)


def _insert_event(conn, **overrides):
    defaults = {
        "project_fingerprint": "proj1",
        "datasource_fingerprint": "ogr::C:/data/test.gpkg|layername=parcelles",
        "layer_id_snapshot": "layer_abc",
        "layer_name_snapshot": "parcelles",
        "provider_type": "ogr",
        "feature_identity_json": '{"fid": 1}',
        "operation_type": "DELETE",
        "attributes_json": '{"all_attributes": {"name": "Dupont"}}',
        "geometry_wkb": None,
        "geometry_type": "Point",
        "crs_authid": "EPSG:4326",
        "field_schema_json": '[]',
        "user_name": "testuser",
        "session_id": "sess1",
        "created_at": "2025-03-15T10:00:00",
        "restored_from_event_id": None,
    }
    defaults.update(overrides)
    conn.execute("""
        INSERT INTO audit_event (
            project_fingerprint, datasource_fingerprint, layer_id_snapshot,
            layer_name_snapshot, provider_type, feature_identity_json,
            operation_type, attributes_json, geometry_wkb, geometry_type,
            crs_authid, field_schema_json, user_name, session_id,
            created_at, restored_from_event_id
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        defaults["project_fingerprint"], defaults["datasource_fingerprint"],
        defaults["layer_id_snapshot"], defaults["layer_name_snapshot"],
        defaults["provider_type"], defaults["feature_identity_json"],
        defaults["operation_type"], defaults["attributes_json"],
        defaults["geometry_wkb"], defaults["geometry_type"],
        defaults["crs_authid"], defaults["field_schema_json"],
        defaults["user_name"], defaults["session_id"],
        defaults["created_at"], defaults["restored_from_event_id"],
    ))
    conn.commit()


class TestSearchEvents(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_empty_search(self):
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 0)
        self.assertEqual(len(result.events), 0)

    def test_search_returns_events(self):
        _insert_event(self.conn)
        _insert_event(self.conn, operation_type="UPDATE",
                      attributes_json='{"changed_only": {"name": {"old": "A", "new": "B"}}}')
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 2)
        self.assertEqual(len(result.events), 2)

    def test_search_by_operation_type(self):
        _insert_event(self.conn, operation_type="DELETE")
        _insert_event(self.conn, operation_type="UPDATE",
                      attributes_json='{"changed_only": {}}')
        criteria = SearchCriteria(None, None, "DELETE", None, None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)
        self.assertEqual(result.events[0].operation_type, "DELETE")

    def test_search_by_user(self):
        _insert_event(self.conn, user_name="alice")
        _insert_event(self.conn, user_name="bob")
        criteria = SearchCriteria(None, None, None, "alice", None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)

    def test_search_by_date_range(self):
        _insert_event(self.conn, created_at="2025-01-01T00:00:00")
        _insert_event(self.conn, created_at="2025-06-01T00:00:00")
        criteria = SearchCriteria(
            None, None, None, None,
            "2025-05-01T00:00:00", "2025-07-01T00:00:00", 1, 100
        )
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)

    def test_pagination(self):
        for i in range(15):
            _insert_event(self.conn, created_at=f"2025-03-{i+1:02d}T10:00:00")
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 5)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 15)
        self.assertEqual(len(result.events), 5)
        self.assertEqual(result.page, 1)
        self.assertEqual(result.page_size, 5)

    def test_pagination_page_2(self):
        for i in range(15):
            _insert_event(self.conn, created_at=f"2025-03-{i+1:02d}T10:00:00")
        criteria = SearchCriteria(None, None, None, None, None, None, 2, 5)
        result = search_events(self.conn, criteria)
        self.assertEqual(len(result.events), 5)
        self.assertEqual(result.page, 2)

    def test_page_size_capped(self):
        for i in range(10):
            _insert_event(self.conn, created_at=f"2025-03-{i+1:02d}T10:00:00")
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 99999)
        result = search_events(self.conn, criteria)
        self.assertLessEqual(result.page_size, 500)


class TestCountEvents(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_count_all(self):
        _insert_event(self.conn)
        _insert_event(self.conn)
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)
        self.assertEqual(count_events(self.conn, criteria), 2)


class TestGetEventById(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_existing_event(self):
        _insert_event(self.conn)
        event = get_event_by_id(self.conn, 1)
        self.assertIsNotNone(event)
        self.assertEqual(event.event_id, 1)

    def test_nonexistent_event(self):
        event = get_event_by_id(self.conn, 999)
        self.assertIsNone(event)


class TestDistinctQueries(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_distinct_layers(self):
        _insert_event(self.conn, layer_name_snapshot="parcelles")
        _insert_event(self.conn, layer_name_snapshot="routes",
                      datasource_fingerprint="ogr::routes.gpkg")
        layers = get_distinct_layers(self.conn)
        self.assertEqual(len(layers), 2)

    def test_distinct_users(self):
        _insert_event(self.conn, user_name="alice")
        _insert_event(self.conn, user_name="bob")
        _insert_event(self.conn, user_name="alice")
        users = get_distinct_users(self.conn)
        self.assertEqual(len(users), 2)

    def test_summarize_scope_counts_operations_and_distincts(self):
        _insert_event(self.conn, operation_type="UPDATE", user_name="alice")
        _insert_event(self.conn, operation_type="DELETE", user_name="bob")
        _insert_event(self.conn, operation_type="INSERT", user_name="alice",
                      datasource_fingerprint="ogr::routes.gpkg")
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)
        summary = summarize_scope(self.conn, criteria)
        self.assertEqual(summary.total_count, 3)
        self.assertEqual(summary.selected_count, 3)
        self.assertEqual(summary.update_count, 1)
        self.assertEqual(summary.delete_count, 1)
        self.assertEqual(summary.insert_count, 1)
        self.assertEqual(summary.user_count, 2)
        self.assertEqual(summary.layer_count, 2)

    def test_summarize_scope_respects_selected_operation_count(self):
        _insert_event(self.conn, operation_type="UPDATE")
        _insert_event(self.conn, operation_type="DELETE")
        criteria = SearchCriteria(None, None, "DELETE", None, None, None, 1, 100)
        summary = summarize_scope(self.conn, criteria)
        self.assertEqual(summary.total_count, 2)
        self.assertEqual(summary.selected_count, 1)
        self.assertEqual(summary.delete_count, 1)


class TestReconstructAttributes(unittest.TestCase):

    def test_full_snapshot(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "DELETE",
            '{"all_attributes": {"name": "Dupont", "age": 42}}',
            None, "Point", "EPSG:4326", "[]", "user", "s", "2025-01-01", None,
        )
        attrs = reconstruct_attributes(event)
        self.assertEqual(attrs["name"], "Dupont")
        self.assertEqual(attrs["age"], 42)

    def test_delta_format(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "UPDATE",
            '{"changed_only": {"name": {"old": "A", "new": "B"}}}',
            None, "Point", "EPSG:4326", "[]", "user", "s", "2025-01-01", None,
        )
        attrs = reconstruct_attributes(event)
        self.assertEqual(attrs["name"], "A")

    def test_delta_format_ignores_layer_audit_fields(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "UPDATE",
            '{"changed_only": {"name": {"old": "A", "new": "B"}, '
            '"date modif": {"old": "2026-03-23T14:00:00", "new": "2026-03-23T15:00:00"}, '
            '"modif par": {"old": "alice", "new": "bob"}}}',
            None, "Point", "EPSG:4326", "[]", "user", "s", "2025-01-01", None,
        )
        attrs = reconstruct_attributes(event)
        self.assertEqual(attrs, {"name": "A"})

    def test_geometry_only_with_empty_changed(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "UPDATE",
            '{"changed_only": {}}',
            b'\x01', "Point", "EPSG:4326", "[]", "user", "s", "2025-01-01", None,
        )
        self.assertTrue(is_geometry_only_update(event))

    def test_geometry_only_with_audit_fields_only(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "UPDATE",
            '{"changed_only": {"date modif": {"old": "a", "new": "b"}, '
            '"modif par": {"old": "x", "new": "y"}}}',
            b'\x01', "Point", "EPSG:4326", "[]", "user", "s", "2025-01-01", None,
        )
        self.assertTrue(is_geometry_only_update(event))

    def test_not_geometry_only_with_business_field(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "UPDATE",
            '{"changed_only": {"name": {"old": "A", "new": "B"}, '
            '"date modif": {"old": "a", "new": "b"}}}',
            b'\x01', "Point", "EPSG:4326", "[]", "user", "s", "2025-01-01", None,
        )
        self.assertFalse(is_geometry_only_update(event))

    def test_invalid_json(self):
        event = AuditEvent(
            1, "p", "d", "l", "n", "ogr", '{"fid":1}', "DELETE",
            "not json", None, "Point", None, "[]", "user", "s", "2025-01-01", None,
        )
        attrs = reconstruct_attributes(event)
        self.assertEqual(attrs, {})


class TestSearchCombinedFilters(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_layer_and_operation_filter(self):
        _insert_event(self.conn, operation_type="DELETE", datasource_fingerprint="fp1")
        _insert_event(self.conn, operation_type="UPDATE", datasource_fingerprint="fp1",
                      attributes_json='{"changed_only": {}}')
        _insert_event(self.conn, operation_type="DELETE", datasource_fingerprint="fp2")
        criteria = SearchCriteria("fp1", None, "DELETE", None, None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)

    def test_user_and_date_filter(self):
        _insert_event(self.conn, user_name="alice", created_at="2025-01-01T00:00:00")
        _insert_event(self.conn, user_name="alice", created_at="2025-06-01T00:00:00")
        _insert_event(self.conn, user_name="bob", created_at="2025-06-01T00:00:00")
        criteria = SearchCriteria(
            None, None, None, "alice", "2025-05-01T00:00:00", "2025-07-01T00:00:00", 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)

    def test_all_filters_combined(self):
        _insert_event(self.conn, operation_type="DELETE", user_name="alice",
                      datasource_fingerprint="fp1", created_at="2025-06-15T00:00:00")
        _insert_event(self.conn, operation_type="UPDATE", user_name="alice",
                      datasource_fingerprint="fp1", created_at="2025-06-15T00:00:00",
                      attributes_json='{"changed_only": {}}')
        _insert_event(self.conn, operation_type="DELETE", user_name="bob",
                      datasource_fingerprint="fp1", created_at="2025-06-15T00:00:00")
        criteria = SearchCriteria(
            "fp1", None, "DELETE", "alice", "2025-06-01", "2025-07-01", 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)
        self.assertEqual(result.events[0].user_name, "alice")
        self.assertEqual(result.events[0].operation_type, "DELETE")


class TestSearchPaginationEdgeCases(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_page_zero_treated_as_page_one(self):
        _insert_event(self.conn)
        criteria = SearchCriteria(None, None, None, None, None, None, 0, 100)
        result = search_events(self.conn, criteria)
        self.assertGreaterEqual(result.total_count, 1)

    def test_negative_page_no_crash(self):
        _insert_event(self.conn)
        criteria = SearchCriteria(None, None, None, None, None, None, -1, 100)
        result = search_events(self.conn, criteria)
        self.assertIsNotNone(result)

    def test_page_size_one(self):
        for i in range(5):
            _insert_event(self.conn, created_at=f"2025-01-{i+1:02d}T00:00:00")
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 1)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 5)
        self.assertEqual(len(result.events), 1)

    def test_page_beyond_total_returns_empty(self):
        _insert_event(self.conn)
        criteria = SearchCriteria(None, None, None, None, None, None, 999, 10)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 1)
        self.assertEqual(len(result.events), 0)

    def test_last_page_partial(self):
        for i in range(7):
            _insert_event(self.conn, created_at=f"2025-01-{i+1:02d}T00:00:00")
        criteria = SearchCriteria(None, None, None, None, None, None, 2, 5)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 7)
        self.assertEqual(len(result.events), 2)


class TestSearchSqlInjection(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_sql_injection_in_user_filter(self):
        _insert_event(self.conn, user_name="safe_user")
        toxic = "'; DROP TABLE audit_event; --"
        criteria = SearchCriteria(None, None, None, toxic, None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 0)
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_event'"
        ).fetchone()
        self.assertIsNotNone(tables)

    def test_sql_injection_in_datasource_filter(self):
        _insert_event(self.conn)
        toxic = "'; DROP TABLE audit_event; --"
        criteria = SearchCriteria(toxic, None, None, None, None, None, 1, 100)
        result = search_events(self.conn, criteria)
        self.assertEqual(result.total_count, 0)

    def test_sql_injection_in_date_filter(self):
        _insert_event(self.conn, created_at="2025-06-15T00:00:00")
        toxic = "'; DROP TABLE audit_event; --"
        criteria = SearchCriteria(None, None, None, None, toxic, None, 1, 100)
        search_events(self.conn, criteria)
        tables = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_event'"
        ).fetchone()
        self.assertIsNotNone(tables)


class TestGetEventByIdEdgeCases(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_event_id_zero(self):
        event = get_event_by_id(self.conn, 0)
        self.assertIsNone(event)

    def test_event_id_negative(self):
        event = get_event_by_id(self.conn, -1)
        self.assertIsNone(event)

    def test_event_id_very_large(self):
        event = get_event_by_id(self.conn, 2**53)
        self.assertIsNone(event)

    def test_event_roundtrip_all_fields(self):
        _insert_event(self.conn, user_name="full_test", operation_type="UPDATE",
                      attributes_json='{"changed_only": {"x": {"old": 1, "new": 2}}}',
                      geometry_wkb=b'\x01\x02\x03', crs_authid="EPSG:2154")
        event = get_event_by_id(self.conn, 1)
        self.assertIsNotNone(event)
        self.assertEqual(event.user_name, "full_test")
        self.assertEqual(event.operation_type, "UPDATE")
        self.assertEqual(event.crs_authid, "EPSG:2154")
        self.assertEqual(bytes(event.geometry_wkb), b'\x01\x02\x03')


class TestSummarizeScopeEdgeCases(unittest.TestCase):

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_empty_journal(self):
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)
        summary = summarize_scope(self.conn, criteria)
        self.assertEqual(summary.total_count, 0)
        self.assertEqual(summary.selected_count, 0)
        self.assertEqual(summary.update_count, 0)
        self.assertEqual(summary.delete_count, 0)
        self.assertEqual(summary.insert_count, 0)

    def test_single_operation_type(self):
        for i in range(5):
            _insert_event(self.conn, operation_type="DELETE",
                          created_at=f"2025-01-{i+1:02d}T00:00:00")
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)
        summary = summarize_scope(self.conn, criteria)
        self.assertEqual(summary.total_count, 5)
        self.assertEqual(summary.delete_count, 5)
        self.assertEqual(summary.update_count, 0)
        self.assertEqual(summary.insert_count, 0)


if __name__ == '__main__':
    unittest.main()
