"""Scenario RL-E2-02: WriteQueue.enqueue atomic all-or-nothing.

Contract:
- INV-1 (atomicite session): pour un lot donne a enqueue(), soit TOUS les events sont mis en queue, soit AUCUN.
- INV-3 (pas de perte silencieuse): un lot rejete pour validation est INTEGRALEMENT sauvegarde (v7: dans la quarantaine de l'outbox du journal, plus dans le fichier pending).
- INV-4 (pending preserve): ne pas modifier _save_lost_events ni le mecanisme de recuperation pending.

Hypotheses revisees (v7):
- La WriteQueue ne range plus rien dans recoverland_pending.json. Un lot
  valide est fsync'e d'un seul bloc dans l'outbox de son journal
  (<journal>.outbox/) avant toute mise en queue RAM. Un lot qui contient un
  event invalide est refuse en entier (REFUSED_INVALID, falsy) et copie en
  entier dans <journal>.outbox/quarantine. A2 lit donc la quarantaine (les 5
  events, chacun une fois, seul le 3e marque invalide, comptes dans
  health()), exige que le fichier pending legacy reste vide (v7 ne l'ecrit
  jamais) et verifie, apres un redemarrage simule
  (durability.restart_integrate), qu'aucun event du lot n'a atteint le
  journal et que la quarantaine est intacte. Le scenario ne supprime plus
  le fichier pending entre les cas : le redemarrage importe ce que le
  produit y aurait laisse, et le verdict le voit.
- Le refus a 50 000 events (_QUEUE_HARD_LIMIT) n'existe plus : la constante
  est gardee mais plus aucun code ne la lit. L'analogue v7 d'une file
  saturee est un ecrivain VIVANT qui ne peut pas indexer. A5 tient
  BEGIN EXCLUSIVE sur le journal par une seconde connexion plus longtemps
  que busy_timeout (5 s) : l'INSERT de l'ecrivain echoue pour de vrai. Le
  lot de 50 doit etre QUEUED_DURABLE, ecrit en un seul ajout dans l'outbox,
  et durable SUR LE DISQUE a l'instant de l'accuse : le journal et son
  outbox sont copies dans un repertoire neuf juste apres le retour
  d'enqueue(), et un redemarrage de la copie y trouve les 50 (un
  redemarrage dans le meme processus ne le prouve pas, la memoire du
  processus survit). Tant que le verrou tient, rien du lot n'est au journal
  et l'etat de l'ecrivain passe a 'degraded'. Un second lot de 10 est remis
  pendant que l'ecrivain est bloque (le lot qu'une file pleine refusait) :
  memes exigences. Apres liberation du verrou : 50 + 10 au journal,
  exactement une fois, avant et apres redemarrage, et rien dans le fichier
  pending.
- A6 et A7 sont ajoutes (meme invariant, vu a travers l'outbox et le
  redemarrage). A6 : sans ecrivain vivant, le lot est PERSISTED_FALLBACK,
  compte dans health().degraded_events (exactement 50), ecrit dans l'outbox
  en un seul ajout (N lignes contigues suivies du marqueur de fin n=N),
  durable sur le disque a l'accuse (meme preuve par copie), absent du
  journal avant redemarrage ("durable, pas encore consultable"), puis
  integre exactement une fois au redemarrage. A7 : apres redemarrage, chaque
  lot du scenario est au journal en entier ou pas du tout, sans doublon,
  l'outbox est vide et le redemarrage n'importe rien du fichier pending.
- A1, A3, A4 sont inchanges : bool(EnqueueOutcome) garde le sens de
  l'ancien booleen (vrai pour les trois issues durables seulement).
- INV-4 tient toujours : _save_lost_events et l'import du fichier pending
  au demarrage (core/integrity.py) existent encore ; la file n'y range
  simplement plus ses lots.

Usage:
    python scripts\\validation\\scenarios\\rw_wq_atomic_enqueue.py

Exclusions with reasons:
- Runtime QGIS: offline test with SQLite temp file, no canvas or QgsProject needed.
- Qt signals: not involved in WriteQueue.enqueue logic.
"""
import json
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
                return tempfile.gettempdir
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
    qgis_app.qgisSettingsDirPath = tempfile.gettempdir
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


def _entities(ids):
    return ["entity%d" % i for i in ids]


def _entity_where(ids):
    """(where, params) for durability.count_rows restricted to one batch."""
    return ("entity_fingerprint IN (%s)" % ",".join("?" * len(ids)),
            tuple(_entities(ids)))


def _quarantine_names(db_path):
    from recoverland.core.capture_outbox import QUARANTINE_DIRNAME, outbox_dir_for
    qdir = os.path.join(outbox_dir_for(db_path), QUARANTINE_DIRNAME)
    return sorted(os.listdir(qdir)) if os.path.isdir(qdir) else []


def _quarantine_entries(db_path, name):
    """[(entity_fingerprint, reason)] of one quarantine file, None if unreadable."""
    from recoverland.core.capture_outbox import QUARANTINE_DIRNAME, outbox_dir_for
    path = os.path.join(outbox_dir_for(db_path), QUARANTINE_DIRNAME, name)
    out = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw in fh:
                text = raw.strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                except ValueError:
                    out.append(("<unparsable>", ""))
                    continue
                rec = obj.get("e") if isinstance(obj, dict) else None
                out.append(((rec or {}).get("entity_fingerprint"),
                            (obj.get("reason") if isinstance(obj, dict) else "") or ""))
    except OSError:
        return None
    return out


def _outbox_lines(db_path):
    """Every parsed line of the journal's outbox segments, in write order."""
    from recoverland.core.capture_outbox import list_segments, outbox_dir_for
    lines = []
    for _seq, path in list_segments(outbox_dir_for(db_path)):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    try:
                        obj = json.loads(raw)
                    except ValueError:
                        lines.append({"t": "<torn>"})
                        continue
                    if isinstance(obj, dict):
                        lines.append(obj)
        except OSError:
            continue
    return lines


def _outbox_events_of(lines, entities):
    wanted = set(entities)
    return sum(1 for ln in lines if ln.get("t") == "ev"
               and (ln.get("e") or {}).get("entity_fingerprint") in wanted)


def _batch_shape(lines, entities):
    """How the outbox holds one batch: a single contiguous append or not.

    One append = every event line carries the same batch id, the lines are
    contiguous, each entity appears once, and the line right after the last
    event is the batch's end marker with n equal to the batch size.
    """
    wanted = set(entities)
    idx = [i for i, ln in enumerate(lines) if ln.get("t") == "ev"
           and (ln.get("e") or {}).get("entity_fingerprint") in wanted]
    seen = [(lines[i].get("e") or {}).get("entity_fingerprint") for i in idx]
    batch_ids = sorted({str(lines[i].get("b")) for i in idx})
    shape = {"ev_lines": len(idx), "distinct_entities": len(set(seen)),
             "batch_ids": len(batch_ids), "contiguous": False,
             "end_markers": 0, "end_n": None, "end_follows_events": False}
    if idx and len(batch_ids) == 1:
        bid = batch_ids[0]
        ends = [i for i, ln in enumerate(lines)
                if ln.get("t") == "end" and str(ln.get("b")) == bid]
        shape["contiguous"] = idx == list(range(idx[0], idx[0] + len(idx)))
        shape["end_markers"] = len(ends)
        if len(ends) == 1:
            shape["end_n"] = lines[ends[0]].get("n")
            shape["end_follows_events"] = ends[0] == idx[-1] + 1
    return shape


def _one_append(shape, n):
    """True when _batch_shape saw the batch as ONE durable append of n events."""
    shape = shape or {}
    return (shape.get("ev_lines") == n and shape.get("distinct_entities") == n
            and shape.get("batch_ids") == 1 and shape.get("contiguous") is True
            and shape.get("end_markers") == 1 and shape.get("end_n") == n
            and shape.get("end_follows_events") is True)


def _snapshot_disk(db_path, dest_dir):
    """Copy what the disk holds for this journal into a fresh directory.

    Taken right after enqueue() returned, the copy holds only what is ON
    DISK at acknowledgement time: the journal (and its WAL, which a crash
    leaves behind; the -shm index is rebuilt from it, never copied), the
    journal's outbox, and the legacy pending file if one exists. An
    in-process restart_integrate cannot prove that: the RAM queue and the
    open handles of this process survive it.
    Returns (copy_db_path, info).
    """
    import shutil
    from recoverland.core.capture_outbox import outbox_dir_for
    info = {"copied": [], "error": ""}
    copy_db = os.path.join(dest_dir, os.path.basename(db_path))
    try:
        os.makedirs(dest_dir)
        for suffix in ("", "-wal"):
            if os.path.isfile(db_path + suffix):
                shutil.copyfile(db_path + suffix, copy_db + suffix)
                info["copied"].append(os.path.basename(db_path + suffix))
        src_outbox = outbox_dir_for(db_path)
        if os.path.isdir(src_outbox):
            shutil.copytree(src_outbox, outbox_dir_for(copy_db))
            info["copied"].append(os.path.basename(src_outbox))
        pending = os.path.join(os.path.dirname(db_path), "recoverland_pending.json")
        if os.path.isfile(pending):
            shutil.copyfile(pending, os.path.join(dest_dir, "recoverland_pending.json"))
            info["copied"].append("recoverland_pending.json")
    except OSError as exc:
        info["error"] = f"{type(exc).__name__}: {exc}"
    return copy_db, info


def _restart_copy(copy_db, info, groups):
    """Start the plugin on the disk copy; rows found per group of ids."""
    from scripts.validation.durability import count_rows, duplicate_uids, restart_integrate
    out = dict(info)
    if info.get("error"):
        out["rows"] = {k: -1 for k in groups}
        return out
    restart = restart_integrate(copy_db)
    replay = restart.get("replay") or {}
    out["rows"] = {k: count_rows(copy_db, *_entity_where(ids)) for k, ids in groups.items()}
    out["dup_uids"] = duplicate_uids(copy_db)
    out["replay_inserted"] = replay.get("events_inserted")
    out["replay_errors"] = list(replay.get("errors") or [])
    return out


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events, pending_events, restart_integrate,
    )
    import sqlite3

    wq = ctx.data["wq"]
    db_path = ctx.data["db_path"]
    AuditEvent = ctx.data["AuditEvent"]
    _wait_flush = ctx.data["_wait_flush"]
    tmpdir = ctx.data["tmpdir"]
    # v7 never writes recoverland_pending.json: the scenario no longer deletes
    # it between cases, so the simulated restart imports whatever the product
    # really left there, and A2, A5 and A7 see it.

    results = {}
    trace_id = ctx.trace_id

    flog(f"scenario_run trace_id={trace_id} scenario={SCENARIO_ID}")

    # Baseline for A2 (v7): quarantine files and health counters before A1.
    quarantine_before_a1 = set(_quarantine_names(db_path))
    health_before_a1 = wq.health()

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

    # A2 (BRUTAL, v7): the quarantine holds the ENTIRE batch (5 events), not just the invalid one
    a1_ids = [1, 2, 3, 4, 5]
    health_after_a1 = wq.health()
    new_quarantine = sorted(set(_quarantine_names(db_path)) - quarantine_before_a1)
    a2_entries = (_quarantine_entries(db_path, new_quarantine[0])
                  if len(new_quarantine) == 1 else None) or []
    a2 = {
        "outcome": getattr(result_a1, "name", repr(result_a1)),
        "new_quarantine_files": len(new_quarantine),
        "quarantine_file": new_quarantine[0] if len(new_quarantine) == 1 else None,
        "entities": sorted(str(ent) for ent, _why in a2_entries),
        "flagged": sorted(str(ent) for ent, why in a2_entries if why),
        "quarantined_delta": (health_after_a1.quarantined_events
                              - health_before_a1.quarantined_events),
        "lost_delta": health_after_a1.lost_events - health_before_a1.lost_events,
        "outbox_events_of_batch": _outbox_events_of(_outbox_lines(db_path),
                                                    _entities(a1_ids)),
        "legacy_pending_entries": pending_events(db_path),
    }
    results["A2"] = a2
    flog(f"A2 trace_id={trace_id} quarantine={json.dumps(a2, default=str)}")

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

    # A5 (v7): the analogue of a saturated queue is a LIVE writer that cannot
    # index. A second connection holds BEGIN EXCLUSIVE on the journal for
    # longer than its busy_timeout (5 s), so the writer's INSERT really
    # fails (the journal is in WAL mode: readers still see it). The batch of
    # 50 is enqueued under the lock; a second batch of 10 (A5b) is enqueued
    # once the writer is seen stuck, the batch a full RAM queue used to
    # refuse. _QUEUE_HARD_LIMIT is inert in v7 (no code reads it); it is
    # still forced to 0 so that a refusal brought back through that constant
    # fails here. Mass-commit acceptance (60 000) is tx_wq_durable_contract w8.
    import recoverland.core.write_queue as wq_module
    a5_ids = list(range(40, 90))
    a5b_ids = list(range(90, 100))
    both_ids = a5_ids + a5b_ids
    a5 = {"rows_while_locked": [], "b_rows_while_locked": [], "states_seen": []}
    result_a5 = result_a5b = None
    original_limit = wq_module._QUEUE_HARD_LIMIT
    wq_module._QUEUE_HARD_LIMIT = 0  # inert in v7, see above
    locker = sqlite3.connect(db_path, timeout=5.0, isolation_level=None)
    try:
        a5["journal_mode"] = locker.execute("PRAGMA journal_mode").fetchone()[0]
        locker.execute("BEGIN EXCLUSIVE")
        t_lock = time.monotonic()
        result_a5 = wq.enqueue([
            _make_event(i, "2024-01-01T00:04:%02dZ" % (i - 40), AuditEvent)
            for i in a5_ids
        ])
        # Right after enqueue() returned: what the DISK holds, nothing else.
        copy_db, copy_info = _snapshot_disk(db_path, os.path.join(tmpdir, "ack_copy_a5"))
        a5["shape_at_ack"] = _batch_shape(_outbox_lines(db_path), _entities(a5_ids))
        a5["ack_copy"] = _restart_copy(copy_db, copy_info, {"a5": a5_ids})
        a5["rows_while_locked"].append(count_rows(db_path, *_entity_where(a5_ids)))
        a5["degraded_at_s"] = None
        a5["writer_alive_when_degraded"] = None
        while time.monotonic() - t_lock < 12.0:
            h = wq.health()
            if h.state not in a5["states_seen"]:
                a5["states_seen"].append(h.state)
            if h.state == "degraded":
                a5["degraded_at_s"] = round(time.monotonic() - t_lock, 2)
                a5["writer_alive_when_degraded"] = h.writer_alive
                break
            time.sleep(0.1)
        a5["rows_while_locked"].append(count_rows(db_path, *_entity_where(a5_ids)))
        # A5b: the writer is stuck on the lock; the next batch must still be
        # acknowledged only once it is durable, and never refused.
        a5["b_state_at_enqueue"] = wq.health().state
        result_a5b = wq.enqueue([
            _make_event(i, "2024-01-01T00:04:%02dZ" % (i - 40), AuditEvent)
            for i in a5b_ids
        ])
        copy_db, copy_info = _snapshot_disk(db_path, os.path.join(tmpdir, "ack_copy_a5b"))
        a5["b_shape_at_ack"] = _batch_shape(_outbox_lines(db_path), _entities(a5b_ids))
        a5["b_ack_copy"] = _restart_copy(copy_db, copy_info,
                                         {"a5": a5_ids, "a5b": a5b_ids})
        while time.monotonic() - t_lock < 6.5:
            time.sleep(0.1)
        a5["rows_while_locked"].append(count_rows(db_path, *_entity_where(a5_ids)))
        a5["b_rows_while_locked"].append(count_rows(db_path, *_entity_where(a5b_ids)))
        a5["state_before_release"] = wq.health().state
        a5["locked_s"] = round(time.monotonic() - t_lock, 2)
    finally:
        try:
            if locker.in_transaction:
                locker.execute("COMMIT")
        finally:
            locker.close()
            wq_module._QUEUE_HARD_LIMIT = original_limit  # Restore
    # Lock released: the live writer indexes both batches. pending_count is
    # read on the RUNNING generation here, never after stop().
    deadline = time.monotonic() + 25.0
    while time.monotonic() < deadline:
        if (count_rows(db_path, *_entity_where(both_ids)) >= len(both_ids)
                and wq.pending_count == 0):
            break
        time.sleep(0.1)
    a5.update({
        "outcome": getattr(result_a5, "name", repr(result_a5)),
        "truthy": bool(result_a5),
        "b_outcome": getattr(result_a5b, "name", repr(result_a5b)),
        "b_truthy": bool(result_a5b),
        "rows_after_flush": count_rows(db_path, *_entity_where(a5_ids)),
        "b_rows_after_flush": count_rows(db_path, *_entity_where(a5b_ids)),
        "dup_uids_after_flush": duplicate_uids(db_path),
        "state_after_release": wq.health().state,
        "legacy_pending_entries": pending_events(db_path),
    })
    flog(f"A5 trace_id={trace_id} result={json.dumps(a5, default=str)}")

    # A6 (v7, added): no live writer, the v7 analogue of a RAM queue that
    # cannot take the batch. The batch must be one append in the outbox, on
    # disk at ack time (disk copy restarted in a fresh directory), counted in
    # degraded_events, absent from the journal until a restart (no writer
    # can index it), and whole after the restart.
    wq._stop_event.set()
    if wq._thread is not None:
        wq._thread.join(timeout=5)
    a6_ids = list(range(100, 150))
    health_before_a6 = wq.health()
    events_a6 = [
        _make_event(i, "2024-01-01T00:05:%02dZ" % (i - 100), AuditEvent)
        for i in a6_ids
    ]
    result_a6 = wq.enqueue(events_a6)
    # Right after enqueue() returned: what the DISK holds, nothing else.
    copy_db, copy_info = _snapshot_disk(db_path, os.path.join(tmpdir, "ack_copy_a6"))
    shape_a6 = _batch_shape(_outbox_lines(db_path), _entities(a6_ids))
    health_after_a6 = wq.health()
    a6 = {
        "writer_alive": health_before_a6.writer_alive,
        "outcome": getattr(result_a6, "name", repr(result_a6)),
        "truthy": bool(result_a6),
        "degraded": bool(getattr(result_a6, "degraded", False)),
        "shape": shape_a6,
        "ack_copy": _restart_copy(copy_db, copy_info, {"a6": a6_ids}),
        "rows_before_restart": count_rows(db_path, *_entity_where(a6_ids)),
        "degraded_delta": (health_after_a6.degraded_events
                           - health_before_a6.degraded_events),
        "lost_delta": health_after_a6.lost_events - health_before_a6.lost_events,
    }
    flog(f"A6 trace_id={trace_id} result={json.dumps(a6, default=str)}")

    # Stop the writer, then do what the plugin does at the next start:
    # pending import + outbox replay. Every measure below is after restart.
    wq.stop()
    restart = restart_integrate(db_path)
    a2["rows_after_restart"] = count_rows(db_path, *_entity_where(a1_ids))
    a2_after = (_quarantine_entries(db_path, a2["quarantine_file"])
                if a2["quarantine_file"] else None) or []
    a2["entities_after_restart"] = sorted(str(ent) for ent, _why in a2_after)
    a5["rows_after_restart"] = count_rows(db_path, *_entity_where(a5_ids))
    a5["b_rows_after_restart"] = count_rows(db_path, *_entity_where(a5b_ids))
    a6["rows_after_restart"] = count_rows(db_path, *_entity_where(a6_ids))
    replay = restart.get("replay") or {}
    a7 = {
        "rows_a1": a2["rows_after_restart"],
        "rows_a4a": count_rows(db_path, *_entity_where([20, 21])),
        "rows_a4b": count_rows(db_path, *_entity_where([30, 31, 32])),
        "rows_a5": a5["rows_after_restart"],
        "rows_a5b": a5["b_rows_after_restart"],
        "rows_a6": a6["rows_after_restart"],
        "rows_total": count_rows(db_path),
        "dup_uids": duplicate_uids(db_path),
        "outbox_events_left": outbox_events(db_path),
        "replay_errors": list(replay.get("errors") or []),
        "replay_rejected": replay.get("events_rejected"),
        "replay_inserted": replay.get("events_inserted"),
        "pending_recovered": restart.get("pending_recovered"),
    }
    a5["dup_uids"] = a6["dup_uids"] = a7["dup_uids"]
    results["A2"] = a2
    results["A5"] = a5
    results["A6"] = a6
    results["A7"] = a7
    flog(f"A7 trace_id={trace_id} after_restart={json.dumps(a7, default=str)}")

    ctx.data["results"] = results

    # Cleanup: remove temp dir (the writer is already stopped)
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

    # Hypothesis revised (v7): the write queue no longer puts a refused batch
    # in recoverland_pending.json; enqueue() returns REFUSED_INVALID and
    # copies the WHOLE batch into <journal>.outbox/quarantine. Same intent (the
    # entire batch is preserved, not only the invalid event), checked more
    # strictly than a pending count: one quarantine file holding each of the
    # 5 events exactly once, only the 3rd flagged invalid, all 5 counted in
    # health(), none in the outbox, NOTHING in the legacy pending file (the old
    # check pinned its exact content; v7 must leave it empty, so a leak of the
    # valid events there fails), none in the journal and the quarantine intact
    # after a simulated restart (durability.restart_integrate) that now
    # imports whatever the pending file really holds (the scenario no longer
    # deletes it between cases).
    a2 = results["A2"]
    a1_entities = ["entity1", "entity2", "entity3", "entity4", "entity5"]
    a2_ok = (a2.get("outcome") == "REFUSED_INVALID"
             and a2.get("new_quarantine_files") == 1
             and a2.get("entities") == a1_entities
             and a2.get("flagged") == ["entity3"]
             and a2.get("quarantined_delta") == 5
             and a2.get("lost_delta") == 0
             and a2.get("outbox_events_of_batch") == 0
             and a2.get("legacy_pending_entries") == 0
             and a2.get("rows_after_restart") == 0
             and a2.get("entities_after_restart") == a1_entities)
    a2_msg = (f"A2 trace_id={trace_id} {a2} expected=REFUSED_INVALID, 1 quarantine "
              f"file holding entity1..entity5 once each, only entity3 flagged, "
              f"quarantined_delta=5, lost_delta=0, 0 in outbox, 0 in the legacy "
              f"pending file, 0 in journal and quarantine intact after restart")
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

    # Hypothesis revised (v7): there is no hard-limit refusal any more (no
    # code reads _QUEUE_HARD_LIMIT) and the queue no longer writes
    # recoverland_pending.json. The old A5 guarded a queue that cannot take a
    # batch safely: never acknowledge it as queued, and keep it on disk. The
    # v7 analogue of that saturated queue is a LIVE writer that cannot index
    # (journal held by BEGIN EXCLUSIVE for longer than busy_timeout). Same
    # intent, proven on disk instead of by a file's existence:
    # - both batches (50, then 10 while the writer is stuck) are
    #   QUEUED_DURABLE and each is ONE append in the outbox at ack time;
    # - a copy of the journal and its outbox taken right after each enqueue()
    #   returned, restarted in a fresh directory, holds the whole batch (an
    #   in-process restart would not prove it: process memory survives);
    # - while the lock is held: 0 rows of either batch in the journal, the
    #   writer is alive and its health state reaches 'degraded';
    # - after release: 50 + 10 rows, no duplicate uid, before and after a
    #   restart, and nothing in the legacy pending file.
    a5 = results["A5"]
    ack5 = (a5.get("ack_copy") or {}).get("rows") or {}
    ack5b = (a5.get("b_ack_copy") or {}).get("rows") or {}
    locked5 = a5.get("rows_while_locked") or []
    locked5b = a5.get("b_rows_while_locked") or []
    a5_ok = (a5.get("outcome") == "QUEUED_DURABLE" and a5.get("truthy") is True
             and _one_append(a5.get("shape_at_ack"), 50)
             and ack5.get("a5") == 50
             and (a5.get("ack_copy") or {}).get("dup_uids") == 0
             and (a5.get("locked_s") or 0) > 5.0
             and len(locked5) == 3 and all(n == 0 for n in locked5)
             and a5.get("degraded_at_s") is not None
             and a5.get("writer_alive_when_degraded") is True
             and a5.get("b_state_at_enqueue") == "degraded"
             and a5.get("b_outcome") == "QUEUED_DURABLE" and a5.get("b_truthy") is True
             and _one_append(a5.get("b_shape_at_ack"), 10)
             and ack5b.get("a5") == 50 and ack5b.get("a5b") == 10
             and (a5.get("b_ack_copy") or {}).get("dup_uids") == 0
             and len(locked5b) == 1 and all(n == 0 for n in locked5b)
             and a5.get("rows_after_flush") == 50
             and a5.get("b_rows_after_flush") == 10
             and a5.get("dup_uids_after_flush") == 0
             and a5.get("legacy_pending_entries") == 0
             and a5.get("rows_after_restart") == 50
             and a5.get("b_rows_after_restart") == 10
             and a5.get("dup_uids") == 0)
    a5_msg = (f"A5 trace_id={trace_id} {a5} expected=journal locked > 5 s: "
              f"QUEUED_DURABLE and one append of 50 at ack, 50 on a disk copy "
              f"restarted at ack, 0 rows while locked, writer alive and 'degraded' "
              f"while locked; then 10 more enqueued while degraded: QUEUED_DURABLE, "
              f"one append of 10, 50+10 on the disk copy, 0 rows while locked; after "
              f"release 50+10 rows, 0 duplicate uid, 0 pending entries, 50+10 after "
              f"restart")
    flog(f"assertion name=A5 ok={a5_ok} {a5_msg}")
    assertions_list.append(("A5", a5_ok, a5_msg))

    # Added (v7): the only case where v7 keeps a batch out of the RAM queue is
    # a writer that is not live. The batch must be PERSISTED_FALLBACK (durable,
    # never a plain success) and counted as degraded in health() (exactly 50,
    # the outcome's own .degraded flag is only "not QUEUED_DURABLE"), written
    # as ONE append in the outbox (50 contiguous lines, one batch id, end
    # marker n=50 right after), durable on disk at ack time (a copy of the
    # journal and its outbox restarted in a fresh directory holds the 50),
    # NOT in the journal before a restart ("durable, not yet searchable": no
    # writer can have indexed it), and exactly 50 after a restart.
    a6 = results["A6"]
    ack6 = a6.get("ack_copy") or {}
    a6_ok = (a6.get("writer_alive") is False
             and a6.get("outcome") == "PERSISTED_FALLBACK"
             and a6.get("truthy") is True and a6.get("degraded") is True
             and a6.get("degraded_delta") == 50
             and _one_append(a6.get("shape"), 50)
             and (ack6.get("rows") or {}).get("a6") == 50
             and ack6.get("dup_uids") == 0
             and a6.get("rows_before_restart") == 0
             and a6.get("rows_after_restart") == 50
             and a6.get("lost_delta") == 0
             and a6.get("dup_uids") == 0)
    a6_msg = (f"A6 trace_id={trace_id} {a6} expected=writer not alive, "
              f"PERSISTED_FALLBACK (truthy), degraded_delta=50, one contiguous "
              f"append of 50 events ending with n=50, 50 on a disk copy restarted "
              f"at ack, 0 rows before restart, 50 after, 0 lost, 0 duplicate uid")
    flog(f"assertion name=A6_OUTBOX_ALL_OR_NOTHING ok={a6_ok} {a6_msg}")
    assertions_list.append(("A6_OUTBOX_ALL_OR_NOTHING", a6_ok, a6_msg))

    # Added (v7): after the simulated restart (pending import + outbox replay)
    # every batch of the scenario is in the journal whole or not at all:
    # A1 and A4b (refused) 0, A4a 2, A5 50, A5b 10, A6 50, nothing else (A3
    # was deleted by the scenario itself before A4), no duplicate event_uid,
    # nothing left in the outbox, no replay error or rejected row, and nothing
    # imported from the legacy pending file (v7 never writes it and the
    # scenario no longer deletes it, so a leak would be imported here).
    a7 = results["A7"]
    a7_ok = (a7.get("rows_a1") == 0 and a7.get("rows_a4a") == 2
             and a7.get("rows_a4b") == 0 and a7.get("rows_a5") == 50
             and a7.get("rows_a5b") == 10
             and a7.get("rows_a6") == 50 and a7.get("rows_total") == 112
             and a7.get("dup_uids") == 0 and a7.get("outbox_events_left") == 0
             and a7.get("replay_errors") == [] and a7.get("replay_rejected") == 0
             and a7.get("pending_recovered") == 0)
    a7_msg = (f"A7 trace_id={trace_id} {a7} expected=A1 0, A4a 2, A4b 0, A5 50, "
              f"A5b 10, A6 50, total 112, 0 duplicate uid, outbox empty, no replay "
              f"error or rejected row, 0 pending recovered")
    flog(f"assertion name=A7_RESTART_ALL_OR_NOTHING ok={a7_ok} {a7_msg}")
    assertions_list.append(("A7_RESTART_ALL_OR_NOTHING", a7_ok, a7_msg))

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
