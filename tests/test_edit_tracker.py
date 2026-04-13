"""Tests for core.edit_tracker signal binding and session lifecycle."""
import importlib.util
import os
import sys
import types
import unittest

_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
_PLUGIN_DIR = os.path.join(_ROOT_DIR, 'recoverland')
_CORE_DIR = os.path.join(_PLUGIN_DIR, 'core')


def _load_edit_tracker_class():
    recoverland_pkg = types.ModuleType('recoverland')
    recoverland_pkg.__path__ = [_PLUGIN_DIR]
    sys.modules.setdefault('recoverland', recoverland_pkg)

    core_pkg = types.ModuleType('recoverland.core')
    core_pkg.__path__ = [_CORE_DIR]
    sys.modules.setdefault('recoverland.core', core_pkg)

    logger_module = types.ModuleType('recoverland.core.logger')
    logger_module.flog = lambda *args, **kwargs: None
    sys.modules['recoverland.core.logger'] = logger_module

    module_name = 'recoverland.core.edit_tracker'
    module_path = os.path.join(_CORE_DIR, 'edit_tracker.py')
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module.EditSessionTracker


EditSessionTracker = _load_edit_tracker_class()


class _Signal:
    def __init__(self):
        self._callbacks = []

    def connect(self, callback):
        self._callbacks.append(callback)

    def disconnect(self):
        self._callbacks.clear()

    def emit(self, *args):
        for callback in list(self._callbacks):
            callback(*args)


class _FakeProvider:
    def __init__(self, provider_name="ogr", caps=1):
        self._provider_name = provider_name
        self._caps = caps

    def name(self):
        return self._provider_name

    def capabilities(self):
        return self._caps


class _FakeLayer:
    def __init__(self, layer_id="layer_123"):
        self._layer_id = layer_id
        self._provider = _FakeProvider()
        self.editingStarted = _Signal()
        self.beforeCommitChanges = _Signal()
        self.committedFeaturesAdded = _Signal()
        self.afterCommitChanges = _Signal()
        self.afterRollBack = _Signal()

    def id(self):
        return self._layer_id

    def name(self):
        return "parcelles"

    def source(self):
        return "C:/data/parcelles.gpkg|layername=parcelles"

    def dataProvider(self):
        return self._provider


class _DummyWriteQueue:
    def __init__(self):
        self.items = []

    def enqueue(self, events):
        self.items.extend(events)


class TestEditTracker(unittest.TestCase):

    def test_before_commit_signal_uses_bound_layer_id(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        layer = _FakeLayer("layer_abc")
        seen = []

        tracker._on_before_commit = lambda layer_id: seen.append(layer_id)
        tracker._bind_signals(layer)

        layer.beforeCommitChanges.emit(True)

        self.assertEqual(seen, ["layer_abc"])

    def test_before_commit_creates_late_session_buffer(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        layer = _FakeLayer("layer_xyz")
        captured = []

        tracker.activate()
        tracker.connect_layer(layer)
        tracker._capture_edit_buffer_state = (
            lambda bound_layer, buf: captured.append((bound_layer.id(), buf.layer_id))
        )

        layer.beforeCommitChanges.emit(True)

        self.assertIn("layer_xyz", tracker._buffers)
        self.assertEqual(captured, [("layer_xyz", "layer_xyz")])


class TestEditTrackerLifecycle(unittest.TestCase):

    def test_activate_deactivate(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        self.assertFalse(tracker.is_active)
        tracker.activate()
        self.assertTrue(tracker.is_active)
        tracker.deactivate()
        self.assertFalse(tracker.is_active)

    def test_inactive_tracker_ignores_editing_started(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        layer = _FakeLayer("l1")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        self.assertNotIn("l1", tracker._buffers)

    def test_active_tracker_creates_buffer_on_editing_started(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l1")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        self.assertIn("l1", tracker._buffers)

    def test_rollback_clears_buffer(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l_rb")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        self.assertIn("l_rb", tracker._buffers)
        layer.afterRollBack.emit()
        self.assertNotIn("l_rb", tracker._buffers)

    def test_rollback_without_buffer_no_crash(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l_no_buf")
        tracker.connect_layer(layer)
        layer.afterRollBack.emit()

    def test_disconnect_layer_removes_all_state(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l_disc")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        self.assertIn("l_disc", tracker._connected_layers)
        self.assertIn("l_disc", tracker._buffers)
        tracker.disconnect_layer(layer)
        self.assertNotIn("l_disc", tracker._connected_layers)
        self.assertNotIn("l_disc", tracker._buffers)
        self.assertNotIn("l_disc", tracker._layer_fingerprints)

    def test_disconnect_all(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        for lid in ("l1", "l2", "l3"):
            layer = _FakeLayer(lid)
            tracker.connect_layer(layer)
            layer.editingStarted.emit()
        self.assertEqual(len(tracker._connected_layers), 3)
        tracker.disconnect_all()
        self.assertEqual(len(tracker._connected_layers), 0)
        self.assertEqual(len(tracker._buffers), 0)

    def test_disconnect_layer_by_id(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l_byid")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        tracker.disconnect_layer_by_id("l_byid")
        self.assertNotIn("l_byid", tracker._connected_layers)

    def test_connect_layer_idempotent(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l_idem")
        tracker.connect_layer(layer)
        tracker.connect_layer(layer)
        self.assertEqual(
            sum(1 for k in tracker._connected_layers if k == "l_idem"), 1)


class TestEditTrackerSuppression(unittest.TestCase):

    def test_suppress_unsuppress_cycle(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        self.assertFalse(tracker.is_suppressed)
        tracker.suppress()
        self.assertTrue(tracker.is_suppressed)
        tracker.unsuppress()
        self.assertFalse(tracker.is_suppressed)

    def test_suppressed_ignores_editing_started(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        tracker.suppress()
        layer = _FakeLayer("l_sup")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        self.assertNotIn("l_sup", tracker._buffers)

    def test_suppressed_ignores_before_commit(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.activate()
        layer = _FakeLayer("l_sup2")
        tracker.connect_layer(layer)
        layer.editingStarted.emit()
        tracker.suppress()
        captured = []
        tracker._capture_edit_buffer_state = lambda *a: captured.append(True)
        layer.beforeCommitChanges.emit(True)
        self.assertEqual(captured, [])

    def test_double_suppress_no_crash(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker.suppress()
        tracker.suppress()
        self.assertTrue(tracker.is_suppressed)
        tracker.unsuppress()
        self.assertFalse(tracker.is_suppressed)


class TestEditTrackerSessionCounter(unittest.TestCase):

    def test_initial_count_zero(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        self.assertEqual(tracker.session_event_count, 0)

    def test_reset_session_count(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        tracker._session_event_count = 42
        tracker.reset_session_count()
        self.assertEqual(tracker.session_event_count, 0)

    def test_mass_delete_threshold_constant(self):
        self.assertEqual(EditSessionTracker._MASS_DELETE_THRESHOLD, 100)

    def test_memory_hard_limit_constant(self):
        self.assertEqual(EditSessionTracker._MEMORY_HARD_LIMIT_MB, 500)


class TestEditTrackerCallbacks(unittest.TestCase):

    def test_commit_callback_setter(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        called = []
        tracker.set_commit_callback(lambda *a: called.append(a))
        self.assertIsNotNone(tracker._on_commit_callback)

    def test_overflow_callback_setter(self):
        tracker = EditSessionTracker(_DummyWriteQueue(), None)
        called = []
        tracker.set_overflow_callback(lambda: called.append(True))
        self.assertIsNotNone(tracker._on_overflow_callback)


if __name__ == '__main__':
    unittest.main()
