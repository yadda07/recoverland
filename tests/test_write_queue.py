"""Tests for core.write_queue module - hard edge cases, validation, crash paths."""
import sqlite3
import tempfile
import time
import os
import unittest
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.audit_backend import AuditEvent
from recoverland.core.write_queue import WriteQueue, _validate_event


def _make_event(**overrides):
    defaults = dict(
        event_id=None,
        project_fingerprint="proj1",
        datasource_fingerprint="ogr::test.gpkg",
        layer_id_snapshot="layer_1",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json='{"fid": 1}',
        operation_type="DELETE",
        attributes_json='{"all_attributes": {"name": "test"}}',
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="[]",
        user_name="testuser",
        session_id="sess1",
        created_at="2025-03-15T10:00:00",
        restored_from_event_id=None,
    )
    defaults.update(overrides)
    return AuditEvent(**defaults)


def _cleanup(path):
    for p in (path, path + "-wal", path + "-shm"):
        try:
            os.unlink(p)
        except OSError:
            pass


class TestWriteQueueBasic(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_enqueue_and_write(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        try:
            wq.enqueue([_make_event(created_at=f"2025-03-{i+1:02d}T10:00:00") for i in range(5)])
            time.sleep(1.0)
        finally:
            wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        conn.close()
        self.assertEqual(count, 5)

    def test_stop_flushes_remaining(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(created_at=f"2025-01-{i+1:02d}T10:00:00") for i in range(10)])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        conn.close()
        self.assertEqual(count, 10)

    def test_empty_queue_stops_cleanly(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.stop()

    def test_pending_count_starts_at_zero(self):
        wq = WriteQueue()
        self.assertEqual(wq.pending_count, 0)

    def test_written_data_is_correct(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(
            user_name="alice",
            operation_type="UPDATE",
            attributes_json='{"changed_only": {"x": {"old": 1, "new": 2}}}',
        )])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute(
            "SELECT user_name, operation_type, attributes_json FROM audit_event"
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], "alice")
        self.assertEqual(row[1], "UPDATE")
        self.assertIn("changed_only", row[2])


class TestWriteQueueAllOperationTypes(unittest.TestCase):
    """Every valid operation type must be persisted correctly."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_insert_update_delete_all_persisted(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([
            _make_event(operation_type="INSERT", created_at="2025-01-01T00:00:00"),
            _make_event(operation_type="UPDATE", created_at="2025-01-02T00:00:00"),
            _make_event(operation_type="DELETE", created_at="2025-01-03T00:00:00"),
        ])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        ops = {r[0] for r in conn.execute("SELECT operation_type FROM audit_event").fetchall()}
        conn.close()
        self.assertEqual(ops, {"INSERT", "UPDATE", "DELETE"})


class TestWriteQueueGeometryBlob(unittest.TestCase):
    """WKB geometry must round-trip through the queue as BLOB."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_geometry_blob_roundtrip(self):
        wkb = b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@'
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(geometry_wkb=wkb)])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute("SELECT geometry_wkb FROM audit_event").fetchone()
        conn.close()
        self.assertEqual(bytes(row[0]), wkb)

    def test_null_geometry_persisted(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(geometry_wkb=None)])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute("SELECT geometry_wkb FROM audit_event").fetchone()
        conn.close()
        self.assertIsNone(row[0])


class TestWriteQueueLargeBatch(unittest.TestCase):
    """Verify the queue handles batches exceeding _MAX_BATCH_SIZE (500)."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_1500_events_all_persisted(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        events = [_make_event(created_at=f"2025-06-{(i%28)+1:02d}T{i%24:02d}:00:00") for i in range(1500)]
        wq.enqueue(events)
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        conn.close()
        self.assertEqual(count, 1500)


class TestWriteQueueStopStartCycle(unittest.TestCase):
    """Queue must work correctly across stop-start cycles."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_stop_start_preserves_data(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(user_name="round1", created_at="2025-01-01T00:00:00")])
        wq.stop()

        wq.start(self.tmp_path)
        wq.enqueue([_make_event(user_name="round2", created_at="2025-01-02T00:00:00")])
        wq.stop()

        conn = sqlite3.connect(self.tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        users = {r[0] for r in conn.execute("SELECT user_name FROM audit_event").fetchall()}
        conn.close()
        self.assertEqual(count, 2)
        self.assertEqual(users, {"round1", "round2"})

    def test_double_stop_no_crash(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.stop()
        wq.stop()

    def test_stop_without_start_no_crash(self):
        wq = WriteQueue()
        wq.stop()


# ---------------------------------------------------------------------------
# Validation: events with bad fields must be REJECTED
# ---------------------------------------------------------------------------
class TestValidateEvent(unittest.TestCase):
    """_validate_event must reject malformed events."""

    def test_valid_event_accepted(self):
        self.assertTrue(_validate_event(_make_event()))

    def test_bad_operation_type_rejected(self):
        self.assertFalse(_validate_event(_make_event(operation_type="MERGE")))
        self.assertFalse(_validate_event(_make_event(operation_type="")))
        self.assertFalse(_validate_event(_make_event(operation_type=None)))

    def test_empty_attributes_json_rejected(self):
        self.assertFalse(_validate_event(_make_event(attributes_json="")))
        self.assertFalse(_validate_event(_make_event(attributes_json=None)))

    def test_non_string_attributes_json_rejected(self):
        self.assertFalse(_validate_event(_make_event(attributes_json=42)))

    def test_empty_created_at_rejected(self):
        self.assertFalse(_validate_event(_make_event(created_at="")))
        self.assertFalse(_validate_event(_make_event(created_at=None)))


class TestWriteQueueValidationIntegration(unittest.TestCase):
    """Invalid events must be silently dropped, valid ones must persist."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_mix_valid_and_invalid_only_valid_persisted(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([
            _make_event(operation_type="DELETE", created_at="2025-01-01T00:00:00"),
            _make_event(operation_type="BOGUS", created_at="2025-01-02T00:00:00"),
            _make_event(operation_type="INSERT", created_at="2025-01-03T00:00:00"),
            _make_event(operation_type="UPDATE", attributes_json=""),
        ])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        conn.close()
        self.assertEqual(count, 2)


class TestWriteQueueUnicodeAndSpecialChars(unittest.TestCase):
    """Events with unicode, special characters, and large text must persist."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_unicode_user_name(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(user_name="jean-pierre d'arc")])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute("SELECT user_name FROM audit_event").fetchone()
        conn.close()
        self.assertEqual(row[0], "jean-pierre d'arc")

    def test_large_attributes_json(self):
        big = json.dumps({"all_attributes": {f"field_{i}": "x" * 1000 for i in range(100)}})
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(attributes_json=big)])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute("SELECT attributes_json FROM audit_event").fetchone()
        conn.close()
        parsed = json.loads(row[0])
        self.assertEqual(len(parsed["all_attributes"]), 100)

    def test_sql_injection_in_layer_name(self):
        toxic = "'; DROP TABLE audit_event; --"
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(layer_name_snapshot=toxic)])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute("SELECT layer_name_snapshot FROM audit_event").fetchone()
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_event'"
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], toxic)
        self.assertIsNotNone(tables)


class TestWriteQueueConcurrentEnqueue(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_concurrent_enqueue_from_multiple_threads(self):
        import threading
        wq = WriteQueue()
        wq.start(self.tmp_path)
        errors = []

        def enqueue_batch(batch_id):
            try:
                events = [
                    _make_event(
                        user_name=f"thread_{batch_id}",
                        created_at=f"2025-01-{(i % 28) + 1:02d}T{batch_id % 24:02d}:00:00",
                    )
                    for i in range(50)
                ]
                wq.enqueue(events)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=enqueue_batch, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wq.stop()
        self.assertEqual(errors, [])
        conn = sqlite3.connect(self.tmp_path)
        count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        conn.close()
        self.assertEqual(count, 250)


class TestWriteQueueEnqueueAfterStop(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_enqueue_before_start_no_crash(self):
        wq = WriteQueue()
        wq.enqueue([_make_event()])

    def test_enqueue_after_stop_no_crash(self):
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.stop()
        wq.enqueue([_make_event()])


class TestWriteQueueAllFieldsPersisted(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        conn = sqlite3.connect(self.tmp_path)
        initialize_schema(conn)
        conn.close()

    def tearDown(self):
        _cleanup(self.tmp_path)

    def test_all_fields_roundtrip(self):
        wkb = b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@'
        wq = WriteQueue()
        wq.start(self.tmp_path)
        wq.enqueue([_make_event(
            project_fingerprint="proj_test",
            datasource_fingerprint="ogr::full.gpkg",
            layer_id_snapshot="lid_1",
            layer_name_snapshot="full_layer",
            provider_type="ogr",
            feature_identity_json='{"fid": 99, "pk_field": "gid", "pk_value": 42}',
            operation_type="UPDATE",
            attributes_json='{"changed_only": {"x": {"old": 1, "new": 2}}}',
            geometry_wkb=wkb,
            geometry_type="Point",
            crs_authid="EPSG:2154",
            field_schema_json='[{"name": "gid", "type": "int"}]',
            user_name="roundtrip_user",
            session_id="sess_rt",
            created_at="2025-06-15T14:30:00",
        )])
        wq.stop()
        conn = sqlite3.connect(self.tmp_path)
        row = conn.execute(
            "SELECT project_fingerprint, datasource_fingerprint, layer_name_snapshot, "
            "provider_type, operation_type, user_name, crs_authid, geometry_wkb "
            "FROM audit_event"
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], "proj_test")
        self.assertEqual(row[1], "ogr::full.gpkg")
        self.assertEqual(row[2], "full_layer")
        self.assertEqual(row[3], "ogr")
        self.assertEqual(row[4], "UPDATE")
        self.assertEqual(row[5], "roundtrip_user")
        self.assertEqual(row[6], "EPSG:2154")
        self.assertEqual(bytes(row[7]), wkb)


class TestValidateEventEdgeCases(unittest.TestCase):

    def test_whitespace_only_operation_type_rejected(self):
        self.assertFalse(_validate_event(_make_event(operation_type="   ")))

    def test_lowercase_operation_rejected(self):
        self.assertFalse(_validate_event(_make_event(operation_type="delete")))
        self.assertFalse(_validate_event(_make_event(operation_type="update")))
        self.assertFalse(_validate_event(_make_event(operation_type="insert")))

    def test_valid_operations_accepted(self):
        self.assertTrue(_validate_event(_make_event(operation_type="DELETE")))
        self.assertTrue(_validate_event(_make_event(operation_type="UPDATE")))
        self.assertTrue(_validate_event(_make_event(operation_type="INSERT")))

    def test_numeric_created_at_accepted(self):
        self.assertTrue(_validate_event(_make_event(created_at=12345)))

    def test_integer_operation_type_rejected(self):
        self.assertFalse(_validate_event(_make_event(operation_type=42)))


if __name__ == '__main__':
    unittest.main()
