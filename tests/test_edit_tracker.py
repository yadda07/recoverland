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


if __name__ == '__main__':
    unittest.main()
