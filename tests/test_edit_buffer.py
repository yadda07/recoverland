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


if __name__ == '__main__':
    unittest.main()
