# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
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
  The shim is installed in setup() and removed at the end of run(), with every
  module imported under it: it used to stay in sys.modules for the rest of the
  process, so later offline scenarios skipped their own shims and shared this
  one's class-level fake signal.

Harness race (audit 2026-09-27, fixed here)
  _wait_flush used to return on the "flush" marker. That marker is appended in
  the WRITER thread before the product callback runs flog (file I/O) and only
  then posts the queued emit: when the post landed after processEvents() had
  started, the emit stayed queued, P2 saw no emit, and P4's first pump
  delivered P2's stale emit -- exactly the 4 failures measured inside the full
  suite (13/17), on a product behaving correctly. Now: _wait_flush waits for
  the emits to catch up with the flushes, every phase drains before clearing,
  P3 pumps while it watches, and _obs widens that window on purpose (50 ms)
  so the scenario is deterministic instead of lucky. The ordering assertions
  were imposed by the wrapper; P6 measures the real invariant with an
  independent SQLite reader. run() stops the WriteQueue and deletes its
  directory (P7 proves the writer is gone before the assertions), and
  teardown(ctx) does it again if run() never got there: the writer thread,
  its connection and its outbox file used to outlive the scenario for the
  whole suite.
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
        return False
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
    return True


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


def _rows_visible(db_path) -> int:
    """Rows an INDEPENDENT connection sees committed, -1 if unreadable."""
    import sqlite3
    try:
        conn = sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)
        try:
            return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _pump_once():
    try:  # real QGIS: drain the queue. Offline shim: no Qt, none needed.
        from qgis.PyQt.QtCore import QCoreApplication
    except Exception:  # noqa: BLE001 - offline fake signals are synchronous
        return
    if QCoreApplication.instance():
        QCoreApplication.processEvents()


def setup(ctx):
    ctx.data["_modules_before"] = set(sys.modules)
    ctx.data["_shim_installed"] = _install_qgis_shim()
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.audit_backend import AuditEvent

    import threading
    db_path, tmpdir = _make_temp_db()
    before = set(threading.enumerate())
    wq = WriteQueue()
    wq.start(db_path)
    ctx.data["own_writers"] = [
        t for t in threading.enumerate()
        if t not in before and t.name == "RecoverLand-Writer"]

    tracker = EditSessionTracker(wq, _DummyJM())
    tracker.activate()

    ordered_events = []
    # Independent readings of the journal, one per flush (writer thread,
    # inside the callback) and one per emit (main thread, in the slot).
    flush_rows = []
    emit_rows = []

    prev_cb = tracker._on_flush_callback

    def _obs(n):
        ordered_events.append(("flush", n))
        flush_rows.append(_rows_visible(db_path))
        # Widen, on purpose, the window between this marker and the queued
        # emit the product posts: the race the old wait lost by luck now
        # happens every time, and the waits below must still be right.
        time.sleep(0.05)
        prev_cb(n)

    wq.set_flush_callback(_obs)

    def _rec(n):
        ordered_events.append(("emit", n))
        emit_rows.append(_rows_visible(db_path))

    tracker.committed_features.connect(_rec)

    ctx.data["signal_slot"] = _rec

    def _counts():
        n_flush = sum(1 for t, _ in ordered_events if t == "flush")
        n_emit = sum(1 for t, _ in ordered_events if t == "emit")
        return n_flush, n_emit

    def _drain(timeout=2.0):
        """Pump until every flush seen so far has its emit (or timeout).

        Called before each phase clears the list: an emit still queued from
        the previous phase would otherwise be delivered inside the next one.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            _pump_once()
            n_flush, n_emit = _counts()
            if n_emit >= n_flush:
                return True
            time.sleep(0.02)
        return False

    def _wait_flush(timeout=2.0, expect_emit=True):
        """Wait for a flush AND its emit, PUMPING the Qt event queue.

        Returns once a new flush is recorded and, when *expect_emit*, once
        the emits have caught up with the flushes. With a disconnected slot
        (expect_emit=False) it still pumps 0.3 s after the flush, so an emit
        that should not arrive has every chance to show up.

        Sleeping alone was not enough and hid seven assertions for as long as
        this scenario existed. Under a real QGIS this module's offline shim
        never installs (import qgis succeeds), so real Qt plays: the
        WriteQueue calls its flush callback from the writer thread while the
        signal bridge is a QObject of the main thread. The connection therefore
        resolves to QUEUED, the emit is posted to the main thread's queue --
        and nothing here ever drained it. The signal was never lost, only
        deferred; measured A/B, pumping takes this scenario from 10/17 to 17/17
        and moves exactly the seven emit assertions, nothing else.

        WHAT THE ORDERING ASSERTIONS DO NOT PROVE: _obs below appends
        ("flush", n) BEFORE calling the product callback that emits, so
        flush-before-emit cannot fail once any emit is observed -- it is
        imposed by this wrapper, not measured on the product. Read those four
        as "the emit was delivered", never as proof of commit ordering. The
        real invariant (BL-RVW-P1-03: the row is committed before the
        notification) is P6, measured by an INDEPENDENT SQLite connection.

        The 13/17 once measured inside the full suite was this wait's own
        race, not the neighbours: it returned on the "flush" marker while
        the emit could still be posted-not-delivered, and that late emit
        leaked into P4. Waiting for the emits to catch up with the flushes
        (and draining before every clear) closes it; _obs widens the window
        on purpose so a regression shows every time, not by luck.
        """
        start = time.time()
        initial_len = len(ordered_events)
        flushed = False
        while time.time() - start < timeout:
            _pump_once()
            flushed = flushed or any(
                ordered_events[i][0] == "flush"
                for i in range(initial_len, len(ordered_events)))
            if flushed:
                n_flush, n_emit = _counts()
                if expect_emit and n_emit >= n_flush:
                    return True
                if not expect_emit:
                    settle = time.time() + 0.3
                    while time.time() < settle:
                        _pump_once()
                        time.sleep(0.02)
                    return True
            time.sleep(0.02)
        return flushed and not expect_emit

    ctx.data["wq"] = wq
    ctx.data["tracker"] = tracker
    ctx.data["db_path"] = db_path
    ctx.data["tmpdir"] = tmpdir
    ctx.data["ordered_events"] = ordered_events
    ctx.data["AuditEvent"] = AuditEvent
    ctx.data["_wait_flush"] = _wait_flush
    ctx.data["_drain"] = _drain
    ctx.data["flush_rows"] = flush_rows
    ctx.data["emit_rows"] = emit_rows


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
    wq = ctx.data["wq"]
    tracker = ctx.data["tracker"]
    ordered_events = ctx.data["ordered_events"]
    AuditEvent = ctx.data["AuditEvent"]
    _wait_flush = ctx.data["_wait_flush"]
    _drain = ctx.data["_drain"]

    results = {}
    try:
        _run_phases(ctx, wq, tracker, ordered_events, AuditEvent,
                    _wait_flush, _drain, results)
    finally:
        _release(ctx)
    # runner.py discards what run() returns -- ctx IS the channel. This
    # scenario was written against a runner that captured the return value,
    # so assertions() read an empty dict and 16 of its 17 checks reported
    # FAIL on a product that was behaving correctly. Stash before returning.
    ctx.data["run_results"] = results
    return results


def teardown(ctx):
    """Runner hook: release what run() may not have reached (idempotent)."""
    _release(ctx)


def _release(ctx):
    """Stop the writer, drop the tracker, delete the journal directory.

    Called at the end of run() (P7 then measures it) and again by
    teardown(ctx); the second call finds nothing left to release.
    """
    import shutil
    if ctx.data.get("_released"):
        return
    ctx.data["_released"] = True
    wq = ctx.data.get("wq")
    if wq is not None:
        try:
            wq.stop()
        except Exception as exc:  # noqa: BLE001 - reported, never raised
            ctx.data["release_error"] = repr(exc)
    tracker = ctx.data.get("tracker")
    if tracker is not None:
        try:
            tracker.deactivate()
        except Exception as exc:  # noqa: BLE001
            ctx.data["release_error"] = repr(exc)
    if ctx.data.get("tmpdir"):
        shutil.rmtree(ctx.data["tmpdir"], ignore_errors=True)
    ctx.data["writer_alive_after_run"] = [
        t.name for t in ctx.data.get("own_writers", []) if t.is_alive()]
    if ctx.data.get("_shim_installed"):
        # Offline only: forget the fake qgis modules AND everything imported
        # on top of them, or the next offline scenario inherits them.
        for name in set(sys.modules) - ctx.data.get("_modules_before", set()):
            sys.modules.pop(name, None)


def _run_phases(ctx, wq, tracker, ordered_events, AuditEvent,
                _wait_flush, _drain, results):

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

    _drain()
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

    _drain()
    ordered_events.clear()
    # Pump while watching: a stray emit sitting in the queue would stay
    # invisible to a plain sleep, and surface inside the next phase.
    _p3_end = time.time() + 0.4
    while time.time() < _p3_end:
        _pump_once()
        time.sleep(0.02)
    results["P3_AT_empty_no_flush"] = len([t for t, _ in ordered_events if t == "flush"]) == 0
    results["P3_AT_empty_no_emit"] = len([t for t, _ in ordered_events if t == "emit"]) == 0

    _drain()
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

    _drain()
    ordered_events.clear()
    signal_slot = ctx.data["signal_slot"]
    tracker.committed_features.disconnect(signal_slot)
    wq.enqueue([event1])
    _wait_flush(expect_emit=False)
    results["P5_AT_disconnect_no_emit"] = len([t for t, _ in ordered_events if t == "emit"]) == 0
    results["P5_AT_disconnect_has_flush"] = len([t for t, _ in ordered_events if t == "flush"]) >= 1
    try:
        tracker.committed_features.disconnect(signal_slot)
        results["P5_AT_disconnect_double_error"] = False
    except TypeError:
        results["P5_AT_disconnect_double_error"] = True

    # P6 -- the real invariant, measured instead of imposed by _obs: every
    # notification comes after the commit it announces. An independent
    # connection read the journal inside each flush callback and again in
    # each emit slot; the emit must never see fewer rows than its flush.
    flush_rows = ctx.data["flush_rows"]
    emit_rows = ctx.data["emit_rows"]
    pairs = list(zip(flush_rows, emit_rows))
    results["P6_rows_committed_before_emit"] = bool(pairs) and all(
        f >= 1 and e >= f for f, e in pairs)
    results["P6_detail"] = f"flush_rows={flush_rows} emit_rows={emit_rows}"


def assertions(ctx):
    run_results = ctx.data.get("run_results", {})
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

    _assert("P6_rows_committed_before_emit",
            run_results.get("P6_rows_committed_before_emit", False),
            "chaque notification committed_features arrive apres que ses lignes "
            "sont lisibles par une connexion independante ; sinon Review relit "
            "le journal avant le commit et ne montre jamais la modification. "
            + str(run_results.get("P6_detail", "")))
    alive = ctx.data.get("writer_alive_after_run")
    _assert("P7_scenario_releases_its_writer",
            alive == [] and not ctx.data.get("release_error"),
            f"ecrivain de ce scenario encore vivant apres run()={alive} "
            f"erreur={ctx.data.get('release_error')} : il survivait a toute la "
            f"suite avec sa connexion et son fichier outbox")

    return assertions_list


def main():
    trace_id = uuid4().hex[:8]
    t0 = datetime.now(timezone.utc)
    from types import SimpleNamespace
    ctx = SimpleNamespace(trace_id=trace_id, started_at=t0, data={})

    try:
        setup(ctx)
        run_results = run(ctx)
        ctx.data["run_results"] = run_results
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
        _release(ctx)


if __name__ == "__main__":
    sys.exit(main())
