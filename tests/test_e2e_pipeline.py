"""End-to-end integration tests for the full RecoverLand universal pipeline.

Tests the complete flow: write events -> search -> reconstruct -> schema drift -> restore pre-check.
Covers all critical use cases from the backlog.
"""
import sqlite3
import tempfile
import json
import os
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.audit_backend import AuditEvent, SearchCriteria
from recoverland.core.write_queue import WriteQueue
from recoverland.core.search_service import (
    search_events, get_event_by_id,
    get_distinct_layers, get_distinct_users, reconstruct_attributes,
)
from recoverland.core.schema_drift import (
    parse_field_schema, compare_schemas, FieldInfo, build_field_mapping,
)
from recoverland.core.retention import (
    purge_old_events, get_journal_stats, RetentionPolicy,
)
from recoverland.core.integrity import check_journal_integrity
from recoverland.core.local_settings import LocalSettings
from recoverland.core.serialization import (
    compute_update_delta, build_full_snapshot, serialize_value,
)
from recoverland.core.edit_buffer import EditSessionBuffer, FeatureSnapshot


def _event(**overrides):
    defaults = dict(
        event_id=None,
        project_fingerprint="project::C:/projects/test.qgz",
        datasource_fingerprint="ogr::C:/data/parcelles.gpkg|layername=parcelles",
        layer_id_snapshot="layer_abc123",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json='{"fid": 1, "pk_field": "gid", "pk_value": 42}',
        operation_type="DELETE",
        attributes_json='{"all_attributes": {"gid": 42, "nom": "Dupont", "surface": 1234.5}}',
        geometry_wkb=b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@',
        geometry_type="Point",
        crs_authid="EPSG:2154",
        field_schema_json=json.dumps([
            {"name": "gid", "type": "integer", "length": 0, "precision": 0},
            {"name": "nom", "type": "varchar", "length": 100, "precision": 0},
            {"name": "surface", "type": "double", "length": 0, "precision": 2},
        ]),
        user_name="jean.dupont",
        session_id="sess_001",
        created_at="2025-06-15T14:30:00+00:00",
        restored_from_event_id=None,
    )
    defaults.update(overrides)
    return AuditEvent(**defaults)


class TestE2EWriteAndSearch(unittest.TestCase):
    """Write events via WriteQueue, then search and verify."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        for p in (self.tmp_path, self.tmp_path + "-wal", self.tmp_path + "-shm"):
            try:
                os.unlink(p)
            except OSError:
                pass

    def test_write_then_search_delete(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_event(operation_type="DELETE")])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        criteria = SearchCriteria(None, None, "DELETE", None, None, None, 1, 100)
        result = search_events(conn, criteria)
        conn.close()

        self.assertEqual(result.total_count, 1)
        evt = result.events[0]
        self.assertEqual(evt.operation_type, "DELETE")
        self.assertEqual(evt.user_name, "jean.dupont")
        self.assertEqual(evt.crs_authid, "EPSG:2154")
        self.assertIsNotNone(evt.geometry_wkb)

    def test_write_then_search_update_delta(self):
        delta = compute_update_delta(
            {"nom": "Dupont", "surface": 1234.5},
            {"nom": "Martin", "surface": 1234.5},
        )
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_event(operation_type="UPDATE", attributes_json=delta)])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        criteria = SearchCriteria(None, None, "UPDATE", None, None, None, 1, 100)
        result = search_events(conn, criteria)
        conn.close()

        self.assertEqual(result.total_count, 1)
        attrs = reconstruct_attributes(result.events[0])
        self.assertEqual(attrs["nom"], "Dupont")
        self.assertNotIn("surface", attrs)

    def test_write_then_search_insert(self):
        snapshot = build_full_snapshot({"gid": 99, "nom": "New", "surface": 0.0})
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_event(operation_type="INSERT", attributes_json=snapshot)])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        criteria = SearchCriteria(None, None, "INSERT", None, None, None, 1, 100)
        result = search_events(conn, criteria)
        conn.close()

        self.assertEqual(result.total_count, 1)
        attrs = reconstruct_attributes(result.events[0])
        self.assertEqual(attrs["gid"], 99)

    def test_multi_layer_isolation(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([
            _event(datasource_fingerprint="ogr::parcelles.gpkg", layer_name_snapshot="parcelles"),
            _event(datasource_fingerprint="ogr::routes.gpkg", layer_name_snapshot="routes"),
            _event(datasource_fingerprint="ogr::parcelles.gpkg", layer_name_snapshot="parcelles"),
        ])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        layers = get_distinct_layers(conn)
        self.assertEqual(len(layers), 2)

        criteria = SearchCriteria("ogr::parcelles.gpkg", None, None, None, None, None, 1, 100)
        result = search_events(conn, criteria)
        self.assertEqual(result.total_count, 2)

        criteria2 = SearchCriteria("ogr::routes.gpkg", None, None, None, None, None, 1, 100)
        result2 = search_events(conn, criteria2)
        self.assertEqual(result2.total_count, 1)
        conn.close()

    def test_user_filter(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([
            _event(user_name="alice"),
            _event(user_name="bob"),
            _event(user_name="alice"),
        ])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        users = get_distinct_users(conn)
        self.assertEqual(set(users), {"alice", "bob"})

        criteria = SearchCriteria(None, None, None, "alice", None, None, 1, 100)
        result = search_events(conn, criteria)
        self.assertEqual(result.total_count, 2)
        conn.close()

    def test_date_range_filter(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([
            _event(created_at="2025-01-01T00:00:00"),
            _event(created_at="2025-06-15T12:00:00"),
            _event(created_at="2025-12-31T23:59:59"),
        ])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        criteria = SearchCriteria(None, None, None, None, "2025-06-01", "2025-07-01", 1, 100)
        result = search_events(conn, criteria)
        self.assertEqual(result.total_count, 1)
        conn.close()

    def test_pagination_large_result(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        events = [_event(created_at=f"2025-03-{(i % 28)+1:02d}T{i % 24:02d}:00:00") for i in range(250)]
        wq.enqueue(events)
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        criteria = SearchCriteria(None, None, None, None, None, None, 1, 50)
        result = search_events(conn, criteria)
        self.assertEqual(result.total_count, 250)
        self.assertEqual(len(result.events), 50)
        self.assertEqual(result.page, 1)

        criteria2 = SearchCriteria(None, None, None, None, None, None, 3, 50)
        result2 = search_events(conn, criteria2)
        self.assertEqual(len(result2.events), 50)
        self.assertEqual(result2.page, 3)
        conn.close()

    def test_get_event_by_id(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_event(user_name="target_user")])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        evt = get_event_by_id(conn, 1)
        self.assertIsNotNone(evt)
        self.assertEqual(evt.user_name, "target_user")
        conn.close()


class TestE2ESchemaDrift(unittest.TestCase):
    """Test schema drift detection across capture-restore gap."""

    def test_compatible_no_drift(self):
        historical = [
            FieldInfo("gid", "integer", 0, 0),
            FieldInfo("nom", "varchar", 100, 0),
        ]
        current = [
            FieldInfo("gid", "integer", 0, 0),
            FieldInfo("nom", "varchar", 100, 0),
        ]
        drift = compare_schemas(historical, current)
        self.assertTrue(drift.is_compatible)
        mapping = build_field_mapping(drift, historical)
        self.assertEqual(mapping, {"gid": "gid", "nom": "nom"})

    def test_column_added_still_compatible(self):
        historical = [FieldInfo("gid", "integer", 0, 0)]
        current = [
            FieldInfo("gid", "integer", 0, 0),
            FieldInfo("new_col", "text", 0, 0),
        ]
        drift = compare_schemas(historical, current)
        self.assertTrue(drift.is_compatible)
        self.assertIn("new_col", drift.added_in_current)

    def test_column_removed_incompatible(self):
        historical = [
            FieldInfo("gid", "integer", 0, 0),
            FieldInfo("old_col", "varchar", 100, 0),
        ]
        current = [FieldInfo("gid", "integer", 0, 0)]
        drift = compare_schemas(historical, current)
        self.assertFalse(drift.is_compatible)
        self.assertIn("old_col", drift.missing_in_current)

    def test_type_change_within_group(self):
        historical = [FieldInfo("val", "int4", 0, 0)]
        current = [FieldInfo("val", "bigint", 0, 0)]
        drift = compare_schemas(historical, current)
        self.assertTrue(drift.is_compatible)

    def test_type_change_across_groups(self):
        historical = [FieldInfo("val", "integer", 0, 0)]
        current = [FieldInfo("val", "boolean", 0, 0)]
        drift = compare_schemas(historical, current)
        self.assertFalse(drift.is_compatible)
        self.assertIn("val", drift.type_changed)

    def test_stored_schema_round_trip(self):
        schema_json = json.dumps([
            {"name": "gid", "type": "integer", "length": 0, "precision": 0},
            {"name": "geom_type", "type": "varchar", "length": 50, "precision": 0},
        ])
        parsed = parse_field_schema(schema_json)
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0].name, "gid")
        self.assertEqual(parsed[1].type_name, "varchar")


class TestE2ERetention(unittest.TestCase):
    """Test retention and purge across realistic scenarios."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        for p in (self.tmp_path, self.tmp_path + "-wal", self.tmp_path + "-shm"):
            try:
                os.unlink(p)
            except OSError:
                pass

    def test_purge_old_keeps_recent(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([
            _event(created_at="2020-01-01T00:00:00"),
            _event(created_at="2020-06-01T00:00:00"),
            _event(created_at="2026-03-23T10:00:00"),
        ])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        policy = RetentionPolicy(retention_days=30, max_events=1_000_000)
        result = purge_old_events(conn, policy)
        self.assertEqual(result.deleted_count, 2)
        self.assertEqual(result.error, "")

        stats = get_journal_stats(conn)
        self.assertEqual(stats["total_events"], 1)
        conn.close()


class TestE2EIntegrity(unittest.TestCase):
    """Test journal integrity checks."""

    def test_healthy_journal_after_writes(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.execute("""
                INSERT INTO audit_event (
                    project_fingerprint, datasource_fingerprint, provider_type,
                    operation_type, attributes_json, user_name, created_at
                ) VALUES ('p', 'd', 'ogr', 'DELETE', '{}', 'user', '2025-01-01')
            """)
            conn.commit()
            conn.close()

            result = check_journal_integrity(tmp_path)
            self.assertTrue(result.is_healthy)
            self.assertEqual(len(result.issues), 0)
        finally:
            for p in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
                try:
                    os.unlink(p)
                except OSError:
                    pass


class TestE2EEditBuffer(unittest.TestCase):
    """Test edit buffer net effect computation for realistic scenarios."""

    def test_modify_then_delete_same_feature(self):
        buf = EditSessionBuffer("layer_1", "sess_1")
        snap = FeatureSnapshot(42, {"name": "old"}, None, ["name"])
        buf.record_modification(snap)
        buf.record_deletion(snap)
        net = buf.compute_net_effect()
        self.assertIn(42, net["deleted"])
        self.assertNotIn(42, net["modified"])

    def test_add_then_delete_no_effect(self):
        buf = EditSessionBuffer("layer_1", "sess_1")
        buf.record_addition(-1)
        del_snap = FeatureSnapshot(-1, {"name": "tmp"}, None, ["name"])
        buf.record_deletion(del_snap)
        net = buf.compute_net_effect()
        self.assertNotIn(-1, net["deleted"])
        self.assertNotIn(-1, net["added"])

    def test_multiple_edits_on_same_feature(self):
        buf = EditSessionBuffer("layer_1", "sess_1")
        snap1 = FeatureSnapshot(10, {"val": "A"}, None, ["val"])
        snap2 = FeatureSnapshot(10, {"val": "B"}, None, ["val"])
        buf.record_modification(snap1)
        buf.record_modification(snap2)
        self.assertEqual(buf.modified_count, 1)
        stored = buf.get_modified_snapshots()[10]
        self.assertEqual(stored.attributes["val"], "A")

    def test_large_batch_memory_tracking(self):
        buf = EditSessionBuffer("layer_1", "sess_1")
        for i in range(500):
            attrs = {f"field_{j}": f"value_{j}" for j in range(20)}
            snap = FeatureSnapshot(i, attrs, b'\x00' * 100, list(attrs.keys()))
            buf.record_modification(snap)
        self.assertEqual(buf.modified_count, 500)
        self.assertGreater(buf.approx_memory_mb, 0)


class TestE2ELocalSettings(unittest.TestCase):
    """Test settings persistence and retrieval."""

    def test_settings_survive_reconnect(self):
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn1 = sqlite3.connect(tmp_path)
            initialize_schema(conn1)
            settings1 = LocalSettings(conn1)
            settings1.activate_local_mode()
            settings1.set_retention_days(90)
            conn1.close()

            conn2 = sqlite3.connect(tmp_path)
            settings2 = LocalSettings(conn2)
            self.assertTrue(settings2.is_local_active)
            self.assertEqual(settings2.retention_days, 90)
            conn2.close()
        finally:
            for p in (tmp_path, tmp_path + "-wal", tmp_path + "-shm"):
                try:
                    os.unlink(p)
                except OSError:
                    pass


class TestE2ESerialization(unittest.TestCase):
    """Test serialization edge cases across the full pipeline."""

    def test_null_values_in_delta(self):
        old = {"name": "Dupont", "email": None}
        new = {"name": "Martin", "email": "a@b.com"}
        delta = compute_update_delta(old, new)
        self.assertIsNotNone(delta)
        parsed = json.loads(delta)
        self.assertIn("name", parsed["changed_only"])
        self.assertIn("email", parsed["changed_only"])
        self.assertIsNone(parsed["changed_only"]["email"]["old"])

    def test_no_change_returns_none(self):
        old = {"name": "Same", "val": 42}
        new = {"name": "Same", "val": 42}
        self.assertIsNone(compute_update_delta(old, new))

    def test_special_characters_in_names(self):
        old = {"champ avec espaces": "a", "champ-tiret": "b"}
        new = {"champ avec espaces": "x", "champ-tiret": "b"}
        delta = compute_update_delta(old, new)
        parsed = json.loads(delta)
        self.assertIn("champ avec espaces", parsed["changed_only"])

    def test_full_snapshot_with_geometry_null(self):
        attrs = {"gid": 1, "name": "test"}
        snapshot = build_full_snapshot(attrs)
        parsed = json.loads(snapshot)
        self.assertEqual(parsed["all_attributes"]["gid"], 1)

    def test_float_nan_serialization(self):
        result = serialize_value(float('nan'))
        self.assertIsNone(result)

    def test_bytes_serialization(self):
        result = serialize_value(b'\xDE\xAD\xBE\xEF')
        self.assertTrue(result.startswith("b64:"))

    def test_nested_list_serialization(self):
        result = serialize_value([1, [2, 3], {"a": 4}])
        self.assertEqual(result, [1, [2, 3], {"a": 4}])

    def test_empty_dict_serialization(self):
        result = serialize_value({})
        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main()
