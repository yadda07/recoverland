"""Scenario BL-RVW-P1-03: Review cache invalidation on commit.

These: EditSessionTracker emits committed_features signal via WriteQueue flush callback
after successful persistence, ensuring Review Snapshot cache invalidation occurs with
up-to-date journal state (no race where refresh reads before flush).

Usage:
    python scripts\\validation\\scenarios\\rvw_commit_invalidation.py

Exclusions with reasons:
- Dialog slot logic (_review_on_commit_changed, _refresh_snapshot_for_extent):
  recover_dialog imports qgis.PyQt (QtWidgets/uic) and plugin widgets, unavailable offline.
  Covered at G4 runtime QGIS by the user (logs 'review: commit_signal_received' and
  'review: cache_invalidated ... rebuild_debounced').
- Pan/zoom A2: requires QGIS runtime and actual extent changes, covered at G4.
- Worker churn/coalescing via _review_debounce: requires QGIS runtime and actual worker,
  covered at G4.
- Qt queued inter-thread delivery: offline fake pyqtSignal is synchronous; real Qt
  semantics (queued connections across threads) validated at G4 runtime.

Honesty note on fake pyqtSignal:
  With this fake, committed_features declared as a class attribute of _CommitSignalBridge
  is a shared class object (not a per-instance bound-signal like real Qt). This is
  acceptable for offline testing (single tracker instance). Real Qt semantics (bound
  signals, queued inter-thread delivery) are validated at G4 runtime.
"""
import sys
import tempfile
import time
import types
from datetime import timezone, datetime
from pathlib import Path
from uuid import uuid4

SCENARIO_ID = "rvw_commit_invalidation"
INVARIANT = "BL-RVW-P1-03"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS_PARENT = _PLUGIN_ROOT.parent
if str(_PLUGINS_PARENT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_PARENT))


def _install_qgis_shim():
    """Install QGIS/PyQt shims for offline execution.

    Must be called BEFORE any import of recoverland.* because core/__init__.py
    imports core/logger.py which imports qgis.core.QgsMessageLog.
    """
    try:
        import qgis  # noqa: F401
        return
    except ImportError:
        pass

    class _StubMeta(type):
        def __getattr__(cls, name):
            return cls

    class _Stub(metaclass=_StubMeta):
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, *args, **kwargs):
            return self

        def __getattr__(self, name):
            return _Stub()

    def _module(name, package=False):
        mod = types.ModuleType(name)
        for attr in dir(_Stub):
            if not attr.startswith("_"):
                setattr(mod, attr, _Stub())
        mod.__getattr__ = lambda attr: _Stub()
        if package:
            mod.__path__ = []
        sys.modules[name] = mod
        return mod

    _Stub.__getattr__ = lambda self, name: _Stub()

    class _FakeSignal:
        """Fake pyqtSignal for offline testing.

        Synchronous emit (no queued delivery). Class-level attribute becomes
        shared across instances (acceptable for single-tracker offline test).
        """
        def __init__(self, *args):
            self._slots = []

        def connect(self, slot):
            if slot not in self._slots:
                self._slots.append(slot)

        def disconnect(self, slot):
            if slot not in self._slots:
                raise TypeError("Signal.disconnect() slot not connected")
            self._slots.remove(slot)

        def emit(self, *args):
            for slot in self._slots:
                slot(*args)

    class _FakeQObject:
        def __init__(self, parent=None):
            self.parent = parent

    _module("qgis", package=True)
    _module("qgis.core")
    _module("qgis.gui")
    _module("qgis.utils")

    _module("qgis.PyQt", package=True)
    qqt_core = _module("qgis.PyQt.QtCore")
    qqt_core.pyqtSignal = _FakeSignal
    qqt_core.QObject = _FakeQObject
    _module("qgis.PyQt.QtGui")
    _module("qgis.PyQt.QtWidgets")


_install_qgis_shim()


def _make_temp_db():
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_ci_")
    db_path = str(Path(tmpdir) / "test_journal.db")
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS audit_event (
            event_id INTEGER PRIMARY KEY,
            project_fingerprint TEXT NOT NULL,
            datasource_fingerprint TEXT NOT NULL,
            layer_id_snapshot TEXT NOT NULL,
            layer_name_snapshot TEXT NOT NULL,
            provider_type TEXT NOT NULL,
            feature_identity_json TEXT NOT NULL,
            operation_type TEXT NOT NULL,
            attributes_json TEXT,
            geometry_wkb BLOB,
            geometry_type TEXT,
            crs_authid TEXT,
            field_schema_json TEXT,
            user_name TEXT,
            session_id TEXT,
            created_at TEXT NOT NULL,
            restored_from_event_id INTEGER,
            entity_fingerprint TEXT NOT NULL,
            event_schema_version INTEGER NOT NULL,
            new_geometry_wkb BLOB,
            invalidated_at TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path, tmpdir


class _DummyJM:
    """Dummy JournalManager for EditSessionTracker (not used in this scenario)."""
    pass


def setup(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.audit_backend import AuditEvent

    db_path, tmpdir = _make_temp_db()
    wq = WriteQueue()
    wq.start(db_path)

    tracker = EditSessionTracker(wq, _DummyJM())
    tracker.activate()

    ordered_events = []

    prev_cb = tracker._on_flush_callback

    def _obs(n):
        ordered_events.append(("flush", n))
        prev_cb(n)

    wq.set_flush_callback(_obs)

    def _rec(n):
        ordered_events.append(("emit", n))

    tracker.committed_features.connect(_rec)

    ctx["data"]["signal_slot"] = _rec

    def _wait_flush(timeout=2.0):
        start = time.time()
        initial_len = len(ordered_events)
        while time.time() - start < timeout:
            if len(ordered_events) > initial_len:
                for i in range(initial_len, len(ordered_events)):
                    if ordered_events[i][0] == "flush":
                        return True
            time.sleep(0.02)
        return False

    ctx["data"]["wq"] = wq
    ctx["data"]["tracker"] = tracker
    ctx["data"]["db_path"] = db_path
    ctx["data"]["tmpdir"] = tmpdir
    ctx["data"]["ordered_events"] = ordered_events
    ctx["data"]["AuditEvent"] = AuditEvent
    ctx["data"]["_wait_flush"] = _wait_flush


def _make_event(event_id, ts, AuditEvent):
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="test_proj",
        datasource_fingerprint="test_ds",
        layer_id_snapshot="layer1",
        layer_name_snapshot="TestLayer",
        provider_type="memory",
        feature_identity_json='{"fid": %d}' % event_id,
        operation_type="INSERT",
        attributes_json='{}',
        geometry_wkb=None,
        geometry_type=None,
        crs_authid=None,
        field_schema_json='{}',
        user_name="test",
        session_id="rvw_ci_session",
        created_at=ts,
        restored_from_event_id=None,
        entity_fingerprint="entity%d" % event_id,
        event_schema_version=1,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def run(ctx):
    wq = ctx["data"]["wq"]
    tracker = ctx["data"]["tracker"]
    ordered_events = ctx["data"]["ordered_events"]
    AuditEvent = ctx["data"]["AuditEvent"]
    _wait_flush = ctx["data"]["_wait_flush"]

    results = {}

    event1 = _make_event(1, "2024-01-01T00:00:00Z", AuditEvent)

    ordered_events.clear()
    wq.enqueue([event1])
    flushed = _wait_flush()
    results["P1_A1_flushed"] = flushed
    flush_indices = [i for i, (t, _) in enumerate(ordered_events) if t == "flush"]
    emit_indices = [i for i, (t, _) in enumerate(ordered_events) if t == "emit"]
    results["P1_A1_has_flush"] = len(flush_indices) >= 1
    results["P1_A1_has_emit"] = len(emit_indices) >= 1
    if flush_indices and emit_indices:
        results["P1_A1_flush_before_emit"] = flush_indices[0] < emit_indices[0]
        first_emit_n = ordered_events[emit_indices[0]][1]
        results["P1_A1_emit_n_positive"] = first_emit_n >= 1
    else:
        results["P1_A1_flush_before_emit"] = False
        results["P1_A1_emit_n_positive"] = False

    ordered_events.clear()
    wq.enqueue([event1])
    pre_flush_count = len([i for i, (t, _) in enumerate(ordered_events) if t == "flush"])
    pre_emit_count = len([i for i, (t, _) in enumerate(ordered_events) if t == "emit"])
    results["P2_AT_race_immediate_no_emit"] = pre_emit_count == 0
    if pre_flush_count > 0:
        results["P2_AT_race_immediate_flush_present"] = True
        first_flush_idx = next(i for i, (t, _) in enumerate(ordered_events) if t == "flush")
        results["P2_AT_race_flush_before_emit"] = all(
            ordered_events[i][0] != "emit" for i in range(first_flush_idx, len(ordered_events))
        )
    else:
        results["P2_AT_race_immediate_flush_present"] = False
        results["P2_AT_race_flush_before_emit"] = True
    _wait_flush()
    post_flush_indices = [i for i, (t, _) in enumerate(ordered_events) if t == "flush"]
    post_emit_indices = [i for i, (t, _) in enumerate(ordered_events) if t == "emit"]
    if post_flush_indices and post_emit_indices:
        results["P2_AT_race_after_wait_flush_before_emit"] = post_flush_indices[0] < post_emit_indices[0]
    else:
        results["P2_AT_race_after_wait_flush_before_emit"] = False

    ordered_events.clear()
    time.sleep(0.4)
    results["P3_AT_empty_no_flush"] = len([t for t, _ in ordered_events if t == "flush"]) == 0
    results["P3_AT_empty_no_emit"] = len([t for t, _ in ordered_events if t == "emit"]) == 0

    ordered_events.clear()
    event2 = _make_event(2, "2024-01-01T00:01:00Z", AuditEvent)
    wq.enqueue([event1])
    _wait_flush()
    wq.enqueue([event2])
    _wait_flush()
    emit_count = len([t for t, _ in ordered_events if t == "emit"])
    results["P4_AT_double_emit_count"] = emit_count == 2
    flush_count = len([t for t, _ in ordered_events if t == "flush"])
    results["P4_AT_double_flush_count"] = flush_count == 2
    if flush_count == 2 and emit_count == 2:
        flush_indices = [i for i, (t, _) in enumerate(ordered_events) if t == "flush"]
        emit_indices = [i for i, (t, _) in enumerate(ordered_events) if t == "emit"]
        results["P4_AT_double_order_1"] = flush_indices[0] < emit_indices[0]
        results["P4_AT_double_order_2"] = flush_indices[1] < emit_indices[1]
    else:
        results["P4_AT_double_order_1"] = False
        results["P4_AT_double_order_2"] = False

    ordered_events.clear()
    signal_slot = ctx["data"]["signal_slot"]
    tracker.committed_features.disconnect(signal_slot)
    wq.enqueue([event1])
    _wait_flush()
    results["P5_AT_disconnect_no_emit"] = len([t for t, _ in ordered_events if t == "emit"]) == 0
    results["P5_AT_disconnect_has_flush"] = len([t for t, _ in ordered_events if t == "flush"]) >= 1
    try:
        tracker.committed_features.disconnect(signal_slot)
        results["P5_AT_disconnect_double_error"] = False
    except TypeError:
        results["P5_AT_disconnect_double_error"] = True

    return results


def assertions(ctx):
    run_results = ctx["data"].get("run_results", {})
    assertions_list = []

    def _assert(name, condition, message):
        assertions_list.append((name, condition, message))

    _assert("P1_A1_flushed", run_results.get("P1_A1_flushed", False),
            "P1_A1_flushed=True")
    _assert("P1_A1_has_flush", run_results.get("P1_A1_has_flush", False),
            "P1_A1_has_flush=True")
    _assert("P1_A1_has_emit", run_results.get("P1_A1_has_emit", False),
            "P1_A1_has_emit=True")
    _assert("P1_A1_flush_before_emit", run_results.get("P1_A1_flush_before_emit", False),
            "P1_A1_flush_before_emit=True")
    _assert("P1_A1_emit_n_positive", run_results.get("P1_A1_emit_n_positive", False),
            "P1_A1_emit_n_positive=True")

    _assert("P2_AT_race_immediate_no_emit", run_results.get("P2_AT_race_immediate_no_emit", False),
            "P2_AT_race_immediate_no_emit=True")
    _assert("P2_AT_race_flush_before_emit", run_results.get("P2_AT_race_flush_before_emit", True),
            "P2_AT_race_flush_before_emit=True")
    _assert("P2_AT_race_after_wait_flush_before_emit",
            run_results.get("P2_AT_race_after_wait_flush_before_emit", False),
            "P2_AT_race_after_wait_flush_before_emit=True")

    _assert("P3_AT_empty_no_flush", run_results.get("P3_AT_empty_no_flush", False),
            "P3_AT_empty_no_flush=True")
    _assert("P3_AT_empty_no_emit", run_results.get("P3_AT_empty_no_emit", False),
            "P3_AT_empty_no_emit=True")

    _assert("P4_AT_double_emit_count", run_results.get("P4_AT_double_emit_count", False),
            "P4_AT_double_emit_count=2")
    _assert("P4_AT_double_flush_count", run_results.get("P4_AT_double_flush_count", False),
            "P4_AT_double_flush_count=2")
    _assert("P4_AT_double_order_1", run_results.get("P4_AT_double_order_1", False),
            "P4_AT_double_order_1=True")
    _assert("P4_AT_double_order_2", run_results.get("P4_AT_double_order_2", False),
            "P4_AT_double_order_2=True")

    _assert("P5_AT_disconnect_no_emit", run_results.get("P5_AT_disconnect_no_emit", False),
            "P5_AT_disconnect_no_emit=True")
    _assert("P5_AT_disconnect_has_flush", run_results.get("P5_AT_disconnect_has_flush", False),
            "P5_AT_disconnect_has_flush=True")
    _assert("P5_AT_disconnect_double_error", run_results.get("P5_AT_disconnect_double_error", False),
            "P5_AT_disconnect_double_error=True")

    return assertions_list


def main():
    trace_id = uuid4().hex[:8]
    t0 = datetime.now(timezone.utc)
    ctx = {"trace_id": trace_id, "started_at": t0, "data": {}}

    try:
        setup(ctx)
        run_results = run(ctx)
        ctx["data"]["run_results"] = run_results
        assertions_list = assertions(ctx)

        passed = sum(1 for _, ok, _ in assertions_list if ok)
        failed = len(assertions_list) - passed
        verdict = "PASS" if failed == 0 else "FAIL"

        for name, ok, message in assertions_list:
            status = "[PASS]" if ok else "[FAIL]"
            print(f"{status} {name}: {message}")

        print(f"verdict={verdict} passed={passed}/{len(assertions_list)} trace_id={trace_id}")

        report_dir = Path(__file__).resolve().parents[1] / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        timestamp = t0.strftime("%Y%m%d_%H%M%S")
        report_path = report_dir / f"rvw_commit_invalidation_{timestamp}.json"

        import json
        report = {
            "scenario": SCENARIO_ID,
            "invariant": INVARIANT,
            "trace_id": trace_id,
            "started_at": t0.isoformat(),
            "assertions": [
                {"name": name, "ok": ok, "message": message}
                for name, ok, message in assertions_list
            ],
            "verdict": verdict,
            "passed": passed,
            "failed": failed,
            "total": len(assertions_list),
        }
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"report={report_path}")

        return 0 if verdict == "PASS" else 1

    finally:
        if "wq" in ctx["data"]:
            ctx["data"]["wq"].stop()
        if "tmpdir" in ctx["data"]:
            import shutil
            shutil.rmtree(ctx["data"]["tmpdir"], ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
