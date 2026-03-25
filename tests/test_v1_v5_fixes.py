"""Non-regression tests for V1-V5 audit fixes.

V1: Buffer pressure protection in EditSessionTracker
V2: Bounded LIMIT on get_distinct_layers / get_distinct_users
V3: vacuum_async callback Qt thread safety (structural, not Qt runtime)
V4: Dead code _try_vacuum removed
V5: Orphan journal cleanup
"""
import sqlite3
import os
import tempfile
import time
import unittest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.edit_buffer import EditSessionBuffer, FeatureSnapshot
from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.search_service import (
    get_distinct_layers, get_distinct_users, _MAX_DISTINCT_RESULTS,
)
from recoverland.core.journal_manager import cleanup_orphan_journals


# ---------------------------------------------------------------------------
# V1: Buffer pressure protection
# ---------------------------------------------------------------------------

def _make_snapshot(fid, attrs=None, wkb=None):
    attrs = attrs or {"name": "test", "val": 42}
    return FeatureSnapshot(
        fid=fid,
        attributes=attrs,
        geometry_wkb=wkb,
        field_names=list(attrs.keys()),
    )


class TestBufferPressureCheck(unittest.TestCase):
    """V1: _check_buffer_pressure must detect soft and hard thresholds."""

    def _make_tracker(self):
        from recoverland.core.edit_tracker import EditSessionTracker
        return EditSessionTracker(write_queue=None, journal_manager=None)

    def test_no_pressure_returns_false(self):
        tracker = self._make_tracker()
        buf = EditSessionBuffer("layer_1", "sess_1")
        buf.record_modification(_make_snapshot(1))
        result = tracker._check_buffer_pressure(buf)
        self.assertFalse(result)

    def test_soft_threshold_returns_false(self):
        tracker = self._make_tracker()
        buf = EditSessionBuffer("layer_1", "sess_1")
        for i in range(10001):
            buf.record_modification(_make_snapshot(i, {"f": "x"}))
        self.assertTrue(buf.needs_flush())
        result = tracker._check_buffer_pressure(buf)
        self.assertFalse(result)

    def test_hard_limit_returns_true(self):
        tracker = self._make_tracker()
        buf = EditSessionBuffer("layer_1", "sess_1")
        big_wkb = b'\x00' * (1024 * 1024)
        for i in range(600):
            buf.record_modification(_make_snapshot(i, {"f": "x"}, wkb=big_wkb))
        self.assertGreater(buf.approx_memory_mb, tracker._MEMORY_HARD_LIMIT_MB)
        result = tracker._check_buffer_pressure(buf)
        self.assertTrue(result)

    def test_hard_limit_class_attribute_is_500(self):
        from recoverland.core.edit_tracker import EditSessionTracker
        self.assertEqual(EditSessionTracker._MEMORY_HARD_LIMIT_MB, 500)


# ---------------------------------------------------------------------------
# V2: Bounded distinct queries
# ---------------------------------------------------------------------------

def _insert_event(conn, **overrides):
    defaults = {
        "project_fingerprint": "proj1",
        "datasource_fingerprint": "ogr::test.gpkg",
        "layer_id_snapshot": "layer_abc",
        "layer_name_snapshot": "parcelles",
        "provider_type": "ogr",
        "feature_identity_json": '{"fid": 1}',
        "operation_type": "DELETE",
        "attributes_json": '{"all_attributes": {"name": "X"}}',
        "geometry_wkb": None,
        "geometry_type": "Point",
        "crs_authid": "EPSG:4326",
        "field_schema_json": "[]",
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


class TestBoundedDistinctLayers(unittest.TestCase):
    """V2: get_distinct_layers must be bounded."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_max_distinct_results_constant_exists(self):
        self.assertEqual(_MAX_DISTINCT_RESULTS, 1000)

    def test_returns_results_normally(self):
        _insert_event(self.conn, layer_name_snapshot="parcelles",
                      datasource_fingerprint="fp1")
        _insert_event(self.conn, layer_name_snapshot="routes",
                      datasource_fingerprint="fp2")
        layers = get_distinct_layers(self.conn)
        self.assertEqual(len(layers), 2)

    def test_result_is_bounded(self):
        for i in range(50):
            _insert_event(self.conn,
                          layer_name_snapshot=f"layer_{i:04d}",
                          datasource_fingerprint=f"fp_{i}")
        layers = get_distinct_layers(self.conn)
        self.assertLessEqual(len(layers), _MAX_DISTINCT_RESULTS)
        self.assertEqual(len(layers), 50)


class TestBoundedDistinctUsers(unittest.TestCase):
    """V2: get_distinct_users must be bounded."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        initialize_schema(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_returns_users_normally(self):
        _insert_event(self.conn, user_name="alice")
        _insert_event(self.conn, user_name="bob")
        users = get_distinct_users(self.conn)
        self.assertEqual(len(users), 2)

    def test_result_is_bounded(self):
        for i in range(50):
            _insert_event(self.conn, user_name=f"user_{i:04d}")
        users = get_distinct_users(self.conn)
        self.assertLessEqual(len(users), _MAX_DISTINCT_RESULTS)
        self.assertEqual(len(users), 50)


# ---------------------------------------------------------------------------
# V3: vacuum_async callback safety (structural test)
# ---------------------------------------------------------------------------

class TestVacuumCallbackStructure(unittest.TestCase):
    """V3: journal_maintenance._vacuum_journal uses QTimer.singleShot."""

    def test_on_vacuum_finished_method_exists(self):
        from recoverland.journal_maintenance import JournalMaintenanceDialog
        self.assertTrue(hasattr(JournalMaintenanceDialog, '_on_vacuum_finished'))

    def test_vacuum_journal_source_uses_singleshot(self):
        import inspect
        from recoverland.journal_maintenance import JournalMaintenanceDialog
        source = inspect.getsource(JournalMaintenanceDialog._vacuum_journal)
        self.assertIn("QTimer.singleShot", source)
        self.assertNotIn("self._progress.setVisible(False)", source)


# ---------------------------------------------------------------------------
# V4: Dead code _try_vacuum removed
# ---------------------------------------------------------------------------

class TestDeadCodeRemoved(unittest.TestCase):
    """V4: _try_vacuum must no longer exist in retention module."""

    def test_try_vacuum_not_in_module(self):
        import recoverland.core.retention as retention_mod
        self.assertFalse(hasattr(retention_mod, '_try_vacuum'))


# ---------------------------------------------------------------------------
# V5: Orphan journal cleanup
# ---------------------------------------------------------------------------

class TestOrphanJournalCleanup(unittest.TestCase):
    """V5: cleanup_orphan_journals removes old files, keeps recent and current."""

    def test_removes_old_orphan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audit_dir = os.path.join(tmpdir, "recoverland", "audit")
            os.makedirs(audit_dir)
            old_file = os.path.join(audit_dir, "audit_deadbeef12345678.sqlite")
            with open(old_file, "w") as f:
                f.write("fake")
            old_time = time.time() - (31 * 86400)
            os.utime(old_file, (old_time, old_time))
            removed = cleanup_orphan_journals(tmpdir, max_age_days=30)
            self.assertEqual(removed, 1)
            self.assertFalse(os.path.exists(old_file))

    def test_keeps_recent_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audit_dir = os.path.join(tmpdir, "recoverland", "audit")
            os.makedirs(audit_dir)
            recent_file = os.path.join(audit_dir, "audit_abc123.sqlite")
            with open(recent_file, "w") as f:
                f.write("fake")
            removed = cleanup_orphan_journals(tmpdir, max_age_days=30)
            self.assertEqual(removed, 0)
            self.assertTrue(os.path.exists(recent_file))

    def test_keeps_current_journal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audit_dir = os.path.join(tmpdir, "recoverland", "audit")
            os.makedirs(audit_dir)
            current = os.path.join(audit_dir, "audit_current.sqlite")
            with open(current, "w") as f:
                f.write("fake")
            old_time = time.time() - (60 * 86400)
            os.utime(current, (old_time, old_time))
            removed = cleanup_orphan_journals(
                tmpdir, max_age_days=30, current_path=current)
            self.assertEqual(removed, 0)
            self.assertTrue(os.path.exists(current))

    def test_removes_sidecar_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audit_dir = os.path.join(tmpdir, "recoverland", "audit")
            os.makedirs(audit_dir)
            db_file = os.path.join(audit_dir, "audit_old.sqlite")
            wal_file = db_file + "-wal"
            shm_file = db_file + "-shm"
            for f in (db_file, wal_file, shm_file):
                with open(f, "w") as fh:
                    fh.write("fake")
            old_time = time.time() - (31 * 86400)
            os.utime(db_file, (old_time, old_time))
            removed = cleanup_orphan_journals(tmpdir, max_age_days=30)
            self.assertEqual(removed, 1)
            self.assertFalse(os.path.exists(db_file))
            self.assertFalse(os.path.exists(wal_file))
            self.assertFalse(os.path.exists(shm_file))

    def test_nonexistent_audit_dir_returns_zero(self):
        removed = cleanup_orphan_journals("/nonexistent/path", max_age_days=30)
        self.assertEqual(removed, 0)

    def test_ignores_non_sqlite_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            audit_dir = os.path.join(tmpdir, "recoverland", "audit")
            os.makedirs(audit_dir)
            txt_file = os.path.join(audit_dir, "notes.txt")
            with open(txt_file, "w") as f:
                f.write("notes")
            old_time = time.time() - (60 * 86400)
            os.utime(txt_file, (old_time, old_time))
            removed = cleanup_orphan_journals(tmpdir, max_age_days=30)
            self.assertEqual(removed, 0)
            self.assertTrue(os.path.exists(txt_file))


if __name__ == '__main__':
    unittest.main()
