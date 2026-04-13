"""Tests for core.edit_buffer module (RLU-020)."""
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.edit_buffer import (
    EditSessionBuffer, FeatureSnapshot, _estimate_snapshot_size,
)


def _make_snapshot(fid, attrs=None, wkb=None):
    attrs = attrs or {"name": "test", "val": 42}
    return FeatureSnapshot(
        fid=fid,
        attributes=attrs,
        geometry_wkb=wkb,
        field_names=list(attrs.keys()),
    )


class TestEditSessionBuffer(unittest.TestCase):

    def setUp(self):
        self.buf = EditSessionBuffer("layer_1", "session_abc")

    def test_empty_buffer(self):
        self.assertEqual(self.buf.modified_count, 0)
        self.assertEqual(self.buf.deleted_count, 0)
        self.assertEqual(self.buf.added_count, 0)
        self.assertEqual(self.buf.total_tracked, 0)

    def test_record_modification(self):
        snap = _make_snapshot(1)
        self.buf.record_modification(snap)
        self.assertEqual(self.buf.modified_count, 1)

    def test_modification_first_call_wins(self):
        snap1 = _make_snapshot(1, {"name": "original"})
        snap2 = _make_snapshot(1, {"name": "second_call"})
        self.buf.record_modification(snap1)
        self.buf.record_modification(snap2)
        self.assertEqual(self.buf.modified_count, 1)
        stored = self.buf.get_modified_snapshots()[1]
        self.assertEqual(stored.attributes["name"], "original")

    def test_record_deletion(self):
        snap = _make_snapshot(1)
        self.buf.record_deletion(snap)
        self.assertEqual(self.buf.deleted_count, 1)
        self.assertEqual(self.buf.modified_count, 0)

    def test_deletion_removes_modification(self):
        snap = _make_snapshot(1)
        self.buf.record_modification(snap)
        self.assertEqual(self.buf.modified_count, 1)
        self.buf.record_deletion(snap)
        self.assertEqual(self.buf.modified_count, 0)
        self.assertEqual(self.buf.deleted_count, 1)

    def test_record_addition(self):
        self.buf.record_addition(-1)
        self.assertEqual(self.buf.added_count, 1)

    def test_clear(self):
        self.buf.record_modification(_make_snapshot(1))
        self.buf.record_deletion(_make_snapshot(2))
        self.buf.record_addition(-1)
        self.buf.clear()
        self.assertEqual(self.buf.total_tracked, 0)

    def test_net_effect_simple(self):
        self.buf.record_modification(_make_snapshot(1))
        self.buf.record_deletion(_make_snapshot(2))
        self.buf.record_addition(-1)
        net = self.buf.compute_net_effect()
        self.assertIn(1, net["modified"])
        self.assertIn(2, net["deleted"])
        self.assertIn(-1, net["added"])

    def test_net_effect_add_then_delete(self):
        self.buf.record_addition(-1)
        self.buf.record_deletion(_make_snapshot(-1))
        net = self.buf.compute_net_effect()
        self.assertNotIn(-1, net["deleted"])
        self.assertNotIn(-1, net["added"])

    def test_net_effect_modify_then_delete(self):
        self.buf.record_modification(_make_snapshot(5))
        self.buf.record_deletion(_make_snapshot(5))
        net = self.buf.compute_net_effect()
        self.assertIn(5, net["deleted"])
        self.assertNotIn(5, net["modified"])

    def test_approx_memory_mb(self):
        for i in range(100):
            big_attrs = {f"field_{j}": "x" * 100 for j in range(20)}
            self.buf.record_modification(_make_snapshot(i, big_attrs))
        self.assertGreater(self.buf.approx_memory_mb, 0)


    def test_committed_additions_empty_by_default(self):
        self.assertEqual(self.buf.get_committed_additions(), [])

    def test_record_committed_addition(self):
        self.buf.record_committed_addition({"fid": 42, "attrs_json": '{}'})
        self.buf.record_committed_addition({"fid": 43, "attrs_json": '{}'})
        additions = self.buf.get_committed_additions()
        self.assertEqual(len(additions), 2)
        self.assertEqual(additions[0]["fid"], 42)
        self.assertEqual(additions[1]["fid"], 43)

    def test_clear_also_clears_committed_additions(self):
        self.buf.record_committed_addition({"fid": 42, "attrs_json": '{}'})
        self.buf.clear()
        self.assertEqual(self.buf.get_committed_additions(), [])

    def test_mixed_ops_net_effect(self):
        """Simulate: modify 2 features, add 1, delete 1 in a single session."""
        self.buf.record_modification(_make_snapshot(10))
        self.buf.record_modification(_make_snapshot(11))
        self.buf.record_addition(-1)
        self.buf.record_deletion(_make_snapshot(20))
        net = self.buf.compute_net_effect()
        self.assertEqual(net["modified"], {10, 11})
        self.assertEqual(net["added"], {-1})
        self.assertEqual(net["deleted"], {20})

    def test_committed_additions_independent_of_temp_fids(self):
        """Committed real FIDs are stored separately from temp edit buffer FIDs."""
        self.buf.record_addition(-1)
        self.buf.record_addition(-2)
        self.buf.record_committed_addition({"fid": 100, "attrs_json": '{}'})
        self.buf.record_committed_addition({"fid": 101, "attrs_json": '{}'})
        self.assertEqual(self.buf.get_added_fids(), {-1, -2})
        additions = self.buf.get_committed_additions()
        self.assertEqual({a["fid"] for a in additions}, {100, 101})


class TestEstimateSnapshotSize(unittest.TestCase):

    def test_small_snapshot(self):
        snap = _make_snapshot(1, {"a": "hello"})
        size = _estimate_snapshot_size(snap)
        self.assertGreater(size, 64)

    def test_snapshot_with_geometry(self):
        wkb = b'\x00' * 1000
        snap = _make_snapshot(1, {"a": 1}, wkb=wkb)
        size = _estimate_snapshot_size(snap)
        self.assertGreaterEqual(size, 1000)


class TestEditBufferBoundaryConditions(unittest.TestCase):

    def test_duplicate_addition_idempotent(self):
        buf = EditSessionBuffer("l", "s")
        buf.record_addition(-1)
        buf.record_addition(-1)
        self.assertEqual(buf.added_count, 1)

    def test_delete_nonexistent_fid_no_crash(self):
        buf = EditSessionBuffer("l", "s")
        snap = _make_snapshot(999)
        buf.record_deletion(snap)
        self.assertEqual(buf.deleted_count, 1)

    def test_net_effect_empty_buffer(self):
        buf = EditSessionBuffer("l", "s")
        net = buf.compute_net_effect()
        self.assertEqual(net["modified"], set())
        self.assertEqual(net["deleted"], set())
        self.assertEqual(net["added"], set())

    def test_add_delete_same_fid_cancels_both(self):
        buf = EditSessionBuffer("l", "s")
        buf.record_addition(-1)
        buf.record_deletion(_make_snapshot(-1))
        net = buf.compute_net_effect()
        self.assertNotIn(-1, net["added"])
        self.assertNotIn(-1, net["deleted"])

    def test_modify_same_fid_many_times_first_wins(self):
        buf = EditSessionBuffer("l", "s")
        for i in range(100):
            buf.record_modification(_make_snapshot(1, {"val": f"v{i}"}))
        self.assertEqual(buf.modified_count, 1)
        stored = buf.get_modified_snapshots()[1]
        self.assertEqual(stored.attributes["val"], "v0")

    def test_total_tracked_consistency(self):
        buf = EditSessionBuffer("l", "s")
        buf.record_modification(_make_snapshot(1))
        buf.record_deletion(_make_snapshot(2))
        buf.record_addition(-1)
        self.assertEqual(buf.total_tracked, 3)

    def test_clear_resets_all_counters(self):
        buf = EditSessionBuffer("l", "s")
        buf.record_modification(_make_snapshot(1))
        buf.record_deletion(_make_snapshot(2))
        buf.record_addition(-1)
        buf.record_committed_addition({"fid": 100, "attrs_json": "{}"})
        buf.clear()
        self.assertEqual(buf.modified_count, 0)
        self.assertEqual(buf.deleted_count, 0)
        self.assertEqual(buf.added_count, 0)
        self.assertEqual(buf.total_tracked, 0)
        self.assertEqual(buf.get_committed_additions(), [])

    def test_needs_flush_below_threshold(self):
        buf = EditSessionBuffer("l", "s")
        buf.record_modification(_make_snapshot(1))
        self.assertFalse(buf.needs_flush())

    def test_get_added_fids_returns_set(self):
        buf = EditSessionBuffer("l", "s")
        buf.record_addition(-1)
        buf.record_addition(-2)
        fids = buf.get_added_fids()
        self.assertIsInstance(fids, set)
        self.assertEqual(fids, {-1, -2})

    def test_snapshot_with_none_geometry(self):
        snap = FeatureSnapshot(
            fid=1, attributes={"a": 1}, geometry_wkb=None, field_names=["a"])
        self.assertIsNone(snap.geometry_wkb)

    def test_snapshot_with_large_geometry(self):
        wkb = b'\xFF' * 1_000_000
        snap = FeatureSnapshot(
            fid=1, attributes={"a": 1}, geometry_wkb=wkb, field_names=["a"])
        size = _estimate_snapshot_size(snap)
        self.assertGreaterEqual(size, 1_000_000)


if __name__ == '__main__':
    unittest.main()
