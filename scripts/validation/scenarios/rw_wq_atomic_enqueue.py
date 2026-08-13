"""Scenario RL-E2-02: WriteQueue.enqueue atomic all-or-nothing.

Contract:
- INV-1 (atomicite session): pour un lot donne a enqueue(), soit TOUS les events sont mis en queue, soit AUCUN.
- INV-3 (pas de perte silencieuse): un lot rejete pour validation est INTEGRALEMENT sauvegarde dans le fichier pending.
- INV-4 (pending preserve): ne pas modifier _save_lost_events ni le mecanisme de recuperation pending.

Usage:
    python scripts\\validation\\scenarios\\rw_wq_atomic_enqueue.py

Exclusions with reasons:
- Runtime QGIS: offline test with SQLite temp file, no canvas or QgsProject needed.
- Qt signals: not involved in WriteQueue.enqueue logic.
"""
import sys
import tempfile
import time
import types
from pathlib import Path

SCENARIO_ID = "rw_wq_atomic_enqueue"
INVARIANT = "RL-E2-02"

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
            if name == "qgisSettingsDirPath":
                return lambda: r"C:\Users\yadda\AppData\Roaming\QGIS\QGIS4\profiles\default"
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
        """Fake pyqtSignal for offline testing."""
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
    qgis_core = _module("qgis.core")
    qgis_app = _Stub()
    qgis_app.qgisSettingsDirPath = lambda: r"C:\Users\yadda\AppData\Roaming\QGIS\QGIS4\profiles\default"
    qgis_core.QgsApplication = qgis_app
    _module("qgis.gui")
    _module("qgis.utils")

    _module("qgis.PyQt", package=True)
    qqt_core = _module("qgis.PyQt.QtCore")
    qqt_core.pyqtSignal = _FakeSignal
    qqt_core.QObject = _FakeQObject
    _module("qgis.PyQt.QtGui")
    _module("qgis.PyQt.QtWidgets")


_install_qgis_shim()


# Monkey-patch os.path.isdir to handle _Stub (logger.py calls it on qgisSettingsDirPath result)

# Monkey-patch os.path.isdir to handle _Stub (logger.py calls it on qgisSettingsDirPath result)
import os
_original_isdir = os.path.isdir


def _patched_isdir(path):
    if hasattr(path, '__class__') and path.__class__.__name__ == '_Stub':
        return False
    return _original_isdir(path)


os.path.isdir = _patched_isdir


def _make_temp_db():
    tmpdir = tempfile.mkdtemp(prefix="rl_wq_atomic_")
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


def setup(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.audit_backend import AuditEvent

    db_path, tmpdir = _make_temp_db()
    wq = WriteQueue()
    wq.start(db_path)

    def _wait_flush(timeout=2.0):
        start = time.time()
        while time.time() - start < timeout:
            if wq.pending_count == 0:
                return True
            time.sleep(0.02)
        return False

    ctx.data["wq"] = wq
    ctx.data["db_path"] = db_path
    ctx.data["tmpdir"] = tmpdir
    ctx.data["AuditEvent"] = AuditEvent
    ctx.data["_wait_flush"] = _wait_flush


def _make_event(event_id, ts, AuditEvent, operation_type="INSERT"):
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="test_proj",
        datasource_fingerprint="test_ds",
        layer_id_snapshot="layer1",
        layer_name_snapshot="TestLayer",
        provider_type="memory",
        feature_identity_json='{"fid": %d}' % event_id,
        operation_type=operation_type,
        attributes_json='{}',
        geometry_wkb=None,
        geometry_type=None,
        crs_authid=None,
        field_schema_json='{}',
        user_name="test",
        session_id="atomic_session",
        created_at=ts,
        restored_from_event_id=None,
        entity_fingerprint="entity%d" % event_id,
        event_schema_version=1,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def run(ctx):
    from recoverland.core.logger import flog
    import sqlite3

    wq = ctx.data["wq"]
    db_path = ctx.data["db_path"]
    AuditEvent = ctx.data["AuditEvent"]
    _wait_flush = ctx.data["_wait_flush"]
    pending_path = Path(db_path).parent / "recoverland_pending.json"

    results = {}
    trace_id = ctx.trace_id

    flog(f"scenario_run trace_id={trace_id} scenario={SCENARIO_ID}")

    # A1 (BRUTAL): lot with 3rd event invalid -> enqueue returns False AND after flush SQLite contains ZERO events from the batch
    events_a1 = [
        _make_event(1, "2024-01-01T00:00:00Z", AuditEvent),
        _make_event(2, "2024-01-01T00:00:01Z", AuditEvent),
        _make_event(3, "2024-01-01T00:00:02Z", AuditEvent, operation_type="INVALID"),  # Invalid operation_type
        _make_event(4, "2024-01-01T00:00:03Z", AuditEvent),
        _make_event(5, "2024-01-01T00:00:04Z", AuditEvent),
    ]
    result_a1 = wq.enqueue(events_a1)
    _wait_flush()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM audit_event")
    count_a1 = cursor.fetchone()[0]
    conn.close()
    results["A1_enqueue_result"] = result_a1
    results["A1_sqlite_count"] = count_a1
    flog(f"A1 trace_id={trace_id} enqueue_result={result_a1} sqlite_count={count_a1}")

    # A2 (BRUTAL): pending file contains the ENTIRE batch (5 events), not just the invalid one
    pending_count = 0
    if pending_path.exists():
        import json
        with open(pending_path, 'r', encoding='utf-8') as f:
            pending_data = json.load(f)
            if isinstance(pending_data, list):
                pending_count = len(pending_data)
            elif isinstance(pending_data, dict) and 'events' in pending_data:
                pending_count = len(pending_data.get('events', []))
    results["A2_pending_count"] = pending_count
    flog(f"A2 trace_id={trace_id} pending_count={pending_count}")

    # Clean pending for next test
    if pending_path.exists():
        pending_path.unlink()

    # A3: 100% valid batch -> enqueue returns True, after flush SQLite contains exactly 5 events
    events_a3 = [
        _make_event(10, "2024-01-01T00:01:00Z", AuditEvent),
        _make_event(11, "2024-01-01T00:01:01Z", AuditEvent),
        _make_event(12, "2024-01-01T00:01:02Z", AuditEvent),
        _make_event(13, "2024-01-01T00:01:03Z", AuditEvent),
        _make_event(14, "2024-01-01T00:01:04Z", AuditEvent),
    ]
    result_a3 = wq.enqueue(events_a3)
    _wait_flush()
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM audit_event")
    count_a3 = cursor.fetchone()[0]
    conn.close()
    results["A3_enqueue_result"] = result_a3
    results["A3_sqlite_count"] = count_a3
    flog(f"A3 trace_id={trace_id} enqueue_result={result_a3} sqlite_count={count_a3}")

    # A4 (BRUTAL): two successive batches, batch A valid then batch B with one invalid -> SQLite contains exactly events from batch A, zero from batch B
    # Clear SQLite first
    conn = sqlite3.connect(db_path)
    conn.execute("DELETE FROM audit_event")
    conn.commit()
    conn.close()

    events_a4a = [
        _make_event(20, "2024-01-01T00:02:00Z", AuditEvent),
        _make_event(21, "2024-01-01T00:02:01Z", AuditEvent),
    ]
    result_a4a = wq.enqueue(events_a4a)
    _wait_flush()

    events_a4b = [
        _make_event(30, "2024-01-01T00:03:00Z", AuditEvent),
        _make_event(31, "2024-01-01T00:03:01Z", AuditEvent, operation_type="INVALID"),  # Invalid
        _make_event(32, "2024-01-01T00:03:02Z", AuditEvent),
    ]
    result_a4b = wq.enqueue(events_a4b)
    _wait_flush()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM audit_event")
    count_a4 = cursor.fetchone()[0]
    conn.close()
    results["A4_batch_a_result"] = result_a4a
    results["A4_batch_b_result"] = result_a4b
    results["A4_sqlite_count"] = count_a4
    flog(f"A4 trace_id={trace_id} batch_a_result={result_a4a} batch_b_result={result_a4b} sqlite_count={count_a4}")

    # Clean pending for next test
    if pending_path.exists():
        pending_path.unlink()

    # A5: hard-limit branch keeps its behavior (simulate via monkeypatch within scenario only)
    import recoverland.core.write_queue as wq_module
    original_limit = wq_module._QUEUE_HARD_LIMIT
    wq_module._QUEUE_HARD_LIMIT = 0  # Force hard limit to trigger

    events_a5 = [
        _make_event(40, "2024-01-01T00:04:00Z", AuditEvent),
    ]
    result_a5 = wq.enqueue(events_a5)
    _wait_flush()

    pending_saved = pending_path.exists()

    wq_module._QUEUE_HARD_LIMIT = original_limit  # Restore
    results["A5_enqueue_result"] = result_a5
    results["A5_pending_saved"] = pending_saved
    flog(f"A5 trace_id={trace_id} enqueue_result={result_a5} pending_saved={pending_saved}")

    ctx.data["results"] = results

    # Cleanup: stop writer and remove temp dir
    wq.stop()
    import shutil
    shutil.rmtree(ctx.data["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from recoverland.core.logger import flog
    results = ctx.data["results"]
    trace_id = ctx.trace_id

    assertions_list = []

    # A1 (BRUTAL): lot with 3rd event invalid -> enqueue returns False AND after flush SQLite contains ZERO events from the batch
    a1_ok = (not results["A1_enqueue_result"] and results["A1_sqlite_count"] == 0)
    a1_msg = f"A1 trace_id={trace_id} enqueue_result={results['A1_enqueue_result']} sqlite_count={results['A1_sqlite_count']} expected=False,0"
    flog(f"assertion name=A1_BRUTAL ok={a1_ok} {a1_msg}")
    assertions_list.append(("A1_BRUTAL", a1_ok, a1_msg))

    # A2 (BRUTAL): pending file contains the ENTIRE batch (5 events), not just the invalid one
    a2_ok = results["A2_pending_count"] == 5
    a2_msg = f"A2 trace_id={trace_id} pending_count={results['A2_pending_count']} expected=5"
    flog(f"assertion name=A2_BRUTAL ok={a2_ok} {a2_msg}")
    assertions_list.append(("A2_BRUTAL", a2_ok, a2_msg))

    # A3: 100% valid batch -> enqueue returns True, after flush SQLite contains exactly 5 events
    a3_ok = (results["A3_enqueue_result"] and results["A3_sqlite_count"] == 5)
    a3_msg = f"A3 trace_id={trace_id} enqueue_result={results['A3_enqueue_result']} sqlite_count={results['A3_sqlite_count']} expected=True,5"
    flog(f"assertion name=A3 ok={a3_ok} {a3_msg}")
    assertions_list.append(("A3", a3_ok, a3_msg))

    # A4 (BRUTAL): two successive batches, batch A valid then batch B with one invalid -> SQLite contains exactly events from batch A, zero from batch B
    a4_ok = (results["A4_batch_a_result"] and
             not results["A4_batch_b_result"] and  # noqa: W504
             results["A4_sqlite_count"] == 2)  # noqa: W504
    a4_msg = f"A4 trace_id={trace_id} batch_a_result={results['A4_batch_a_result']} batch_b_result={results['A4_batch_b_result']} sqlite_count={results['A4_sqlite_count']} expected=True,False,2"
    flog(f"assertion name=A4_BRUTAL ok={a4_ok} {a4_msg}")
    assertions_list.append(("A4_BRUTAL", a4_ok, a4_msg))

    # A5: hard-limit branch keeps its behavior
    a5_ok = (not results["A5_enqueue_result"] and results["A5_pending_saved"])
    a5_msg = f"A5 trace_id={trace_id} enqueue_result={results['A5_enqueue_result']} pending_saved={results['A5_pending_saved']} expected=False,True"
    flog(f"assertion name=A5 ok={a5_ok} {a5_msg}")
    assertions_list.append(("A5", a5_ok, a5_msg))

    return assertions_list


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
