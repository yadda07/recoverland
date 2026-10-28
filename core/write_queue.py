# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Write path of the audit journal: durable outbox first, SQLite second.

Contract of :meth:`WriteQueue.enqueue` (I-WQ-01, I-WQ-02)
---------------------------------------------------------
Every call ends in exactly one explicit :class:`EnqueueOutcome`:

``QUEUED_DURABLE``
    The batch is fsync'd in the outbox of its journal AND a live writer
    thread will index it into SQLite. A crash from here on loses nothing:
    the next start replays the outbox, idempotently (``event_uid``).
``PERSISTED_FALLBACK``
    The batch is fsync'd in the outbox but no live writer can index it
    (thread dead, never started, stopping, or the batch belongs to another
    journal than the one being written). Durable, not yet searchable.
``PERSISTED_JOURNAL_SYNC``
    The outbox could not be written; the batch was committed synchronously
    into its journal on the calling thread.
``REFUSED_INVALID``
    The batch is malformed (a capture bug). It is written to the outbox
    quarantine, never dropped, and reported loudly.
``REFUSED_NO_DURABLE_PATH``
    Nothing could be written anywhere (disk full, volume gone). The only
    outcome with a loss: counted in ``lost_events``, logged CRITICAL.

No outcome acknowledges an event that only lives in RAM: the RAM queue
carries copies of batches that are already on disk.

``bool(outcome)`` is True for the three durable outcomes, which keeps
``if not wq.enqueue(...)`` callers correct. Callers that must tell a
degraded capture from a normal one read ``outcome.degraded`` or
:meth:`WriteQueue.health`.

Generations
-----------
Each :meth:`start` creates a new generation: its own queue, stop event,
outbox, journal path and thread. A writer that outlives :meth:`stop`
(stuck on a locked journal) keeps only references to ITS generation: it
can finish writing its own batches into its own journal, it can never
drain the next project's events into the previous project's journal.

Threads
-------
``enqueue``/``prepare_commit``/``stop``/``watchdog_check`` run on the GUI
thread; ``_writer_loop`` runs on ``RecoverLand-Writer``. Nothing here
touches a QGIS object.
"""
from __future__ import annotations

import json
import os
import queue
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, List, NamedTuple, Optional

from .audit_backend import AuditEvent
from .capture_outbox import (
    quarantine_backlog,
    CaptureOutbox, OutboxWriteError, event_to_record, new_event_uid,
    outbox_dir_for, replay_outbox, fault_point,
)
from .journal_writes import (
    ensure_v7_schema, event_row, insert_events_idempotent,
    record_capture_commit, validate_event_row,
)
from .logger import flog
from .sqlite_schema import apply_pragmas


def _durable_pragmas(conn) -> str:
    """Journal pragmas, then a commit that survives a power cut.

    The outbox copy of a batch is released as soon as the journal has
    committed it. With WAL and synchronous=NORMAL a COMMIT reaches the
    disk only at the next checkpoint: a QGIS crash loses nothing (the
    WAL is in the OS cache), but a power cut or an OS crash right after
    the release lost the only durable copy of edits already announced as
    saved. FULL syncs the WAL at every commit. Measured on the writer
    thread (reports/bench_durability_*): 322 ms for 1000 events against
    375 ms with NORMAL, 3.6 ms against 2.0 ms for one; the GUI thread does
    not pay it. Returns the mode really in force.
    """
    apply_pragmas(conn)
    try:
        conn.execute("PRAGMA synchronous=FULL")
        mode = int(conn.execute("PRAGMA synchronous").fetchone()[0])
    except sqlite3.Error:
        return "unknown"
    return {0: "OFF", 1: "NORMAL", 2: "FULL", 3: "EXTRA"}.get(mode, str(mode))


_MAX_BATCH_EVENTS = 500
_FLUSH_TIMEOUT_SEC = 10
_QUEUE_WARNING_THRESHOLD = 10000
_QUEUE_EARLY_WARNING = 40000
_RETRY_BASE_SEC = 0.2
_RETRY_MAX_SEC = 5.0
_POISON_ATTEMPTS = 3
_WAL_CHECKPOINT_INTERVAL_SEC = 60
_STALL_SEC = 30.0

# Old module constants, kept for scripts that import them. The hard limit
# no longer refuses anything: every accepted batch is already durable.
_QUEUE_HARD_LIMIT = 50000
_BATCH_RETRY_COUNT = 3
_BATCH_RETRY_BASE_SEC = _RETRY_BASE_SEC


class EnqueueOutcome(Enum):
    QUEUED_DURABLE = "queued_durable"
    PERSISTED_FALLBACK = "persisted_fallback"
    PERSISTED_JOURNAL_SYNC = "persisted_journal_sync"
    REFUSED_INVALID = "refused_invalid"
    REFUSED_NO_DURABLE_PATH = "refused_no_durable_path"

    def __bool__(self) -> bool:
        return self in _DURABLE_OUTCOMES

    @property
    def durable(self) -> bool:
        return self in _DURABLE_OUTCOMES

    @property
    def degraded(self) -> bool:
        return self is not EnqueueOutcome.QUEUED_DURABLE


_DURABLE_OUTCOMES = frozenset({
    EnqueueOutcome.QUEUED_DURABLE,
    EnqueueOutcome.PERSISTED_FALLBACK,
    EnqueueOutcome.PERSISTED_JOURNAL_SYNC,
})


class WriterState(Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    FAILED = "failed"
    STOPPING = "stopping"


class CommitInfo(NamedTuple):
    """How a QGIS commit ended, carried with the batch that resolves it."""
    commit_id: str
    state: str
    layer_id: Optional[str] = None
    layer_name: Optional[str] = None
    datasource_fingerprint: Optional[str] = None
    session_id: Optional[str] = None
    prepared_at: Optional[str] = None
    detail: Optional[str] = None
    snapshots_json: Optional[str] = None


class WriterHealth(NamedTuple):
    state: str
    writer_alive: bool
    queued_batches: int
    queued_events: int
    outbox_outstanding: int
    open_prepares: int
    last_successful_flush: str
    last_persistence_error: str
    journal_writable: bool
    flushed_events: int
    degraded_events: int
    lost_events: int
    quarantined_events: int
    journal_path: str
    last_replay: Optional[dict]
    # False while the outbox cannot be written (the last attempt failed):
    # edits then go synchronously to the journal, durable but slower, and
    # a crash between the provider commit and that write is not covered.
    outbox_ok: bool = True
    # synchronous mode of the writer connection: FULL, or the outbox copy
    # is released before the batch survives a power cut.
    journal_sync: str = ""


class _Batch(NamedTuple):
    seq: Optional[int]
    batch_id: str
    events: List[AuditEvent]
    commit: Optional[CommitInfo]


class _Generation:
    """Everything one writer thread may touch. Never shared with the next."""

    def __init__(self, gen_id: int, db_path: str, outbox: CaptureOutbox):
        self.gen_id = gen_id
        self.db_path = db_path
        self.outbox = outbox
        self.queue: "queue.Queue[_Batch]" = queue.Queue()
        self.stop_event = threading.Event()
        self.accept_lock = threading.Lock()
        self.accepting = False
        self.stop_requested = False
        self.thread: Optional[threading.Thread] = None
        self.state = WriterState.STARTING
        self.queued_events = 0
        self.inflight_events = 0
        self.ack_failed = False

    @property
    def alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _same_path(a: Optional[str], b: Optional[str]) -> bool:
    if not a or not b:
        return False
    return os.path.normcase(os.path.abspath(a)) == os.path.normcase(os.path.abspath(b))


class WriteQueue:
    """Durable hand-off of captured events to the SQLite journal."""

    def __init__(self):
        self._gen: Optional[_Generation] = None
        self._gen_counter = 0
        self._stats_lock = threading.Lock()
        self._on_early_warning: Optional[Callable[[], None]] = None
        self._on_flush_callback: Optional[Callable[[int], None]] = None
        self._on_failure: Optional[Callable[[WriterHealth], None]] = None
        self._early_warning_emitted = False
        self._last_flush_at = ""
        self._last_flush_mono = 0.0
        self._last_error = ""
        self._journal_writable = True
        self._flushed_events = 0
        self._degraded_events = 0
        self._lost_events = 0
        self._quarantined_events = 0
        self._failure_reported = False
        self._outbox_ok = True
        self._last_db_path: Optional[str] = None
        self.last_replay_report = None
        self._sync_mode = ""
        self._pending_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Compatibility properties
    # ------------------------------------------------------------------

    @property
    def _db_path(self) -> Optional[str]:
        return self._gen.db_path if self._gen is not None else self._last_db_path

    @property
    def _thread(self) -> Optional[threading.Thread]:
        return self._gen.thread if self._gen is not None else None

    @property
    def _running(self) -> bool:
        return self._gen is not None and not self._gen.stop_requested

    @property
    def _outbox(self) -> Optional[CaptureOutbox]:
        return self._gen.outbox if self._gen is not None else None

    @property
    def _stop_event(self) -> threading.Event:
        return self._gen.stop_event if self._gen is not None else threading.Event()

    @property
    def journal_path(self) -> Optional[str]:
        return self._db_path

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, db_path: str) -> None:
        """Replay what a previous run left in the outbox, then start a writer."""
        if self._gen is not None:
            self.stop()
        self._last_db_path = db_path
        self._replay(db_path)
        backlog = quarantine_backlog(db_path)
        with self._stats_lock:
            self._quarantined_events = backlog
        if backlog:
            flog(f"WriteQueue: event=wq_quarantine_backlog n_events={backlog} "
                 f"journal={db_path}: accepted edits kept in quarantine, not in "
                 f"the journal", "WARNING")
        self._gen_counter += 1
        gen = _Generation(self._gen_counter, db_path, CaptureOutbox(db_path))
        gen.thread = threading.Thread(
            target=self._writer_loop, args=(gen,),
            name="RecoverLand-Writer", daemon=True)
        with gen.accept_lock:
            gen.accepting = True
        self._gen = gen
        self._failure_reported = False
        self._early_warning_emitted = False
        gen.thread.start()
        flog(f"WriteQueue: event=wq_started gen={gen.gen_id} journal={db_path} "
             f"outbox={gen.outbox.dir}")

    def _replay(self, db_path: str) -> None:
        conn = None
        try:
            conn = sqlite3.connect(db_path, check_same_thread=False)
            self._sync_mode = _durable_pragmas(conn)
            self.last_replay_report = replay_outbox(db_path, conn)
        except Exception as exc:  # noqa: BLE001 - never block the start
            # A replay that fails keeps every segment it did not finish;
            # letting it raise out of start() left the plugin without a
            # writer for the whole session (capture off) over a quarantine
            # directory that could not be created.
            self._last_error = f"replay: {type(exc).__name__}: {exc}"
            flog(f"WriteQueue: event=wq_replay_failed journal={db_path} "
                 f"error={self._last_error} (outbox kept for the next attempt)",
                 "ERROR")
        finally:
            if conn is not None:
                conn.close()

    def stop(self) -> None:
        """Stop the current writer. Idempotent, safe when never started.

        Every queued batch is already in the outbox: what the writer cannot
        index before the timeout is replayed at the next start, and this
        says so. A writer that outlives the timeout keeps its own
        generation and cannot touch the next one.
        """
        gen = self._gen
        if gen is None:
            return
        gen.stop_requested = True
        with gen.accept_lock:
            gen.accepting = False
        if gen.state not in (WriterState.FAILED,):
            gen.state = WriterState.STOPPING
        gen.stop_event.set()
        if gen.thread is not None:
            gen.thread.join(timeout=_FLUSH_TIMEOUT_SEC)
            if gen.thread.is_alive():
                flog(f"WriteQueue: event=wq_stop_timeout gen={gen.gen_id} the "
                     f"writer did not stop within {_FLUSH_TIMEOUT_SEC}s; it "
                     f"keeps its own journal and outbox, its batches stay "
                     f"durable and are replayed at the next start", "ERROR")
        left = self._drain_ram(gen)
        ram_events = sum(len(b.events) for b in left)
        # What stop() must report is the DURABLE backlog, not only RAM: a
        # dead writer never had anything in RAM (enqueue stops putting
        # there), its whole backlog is in the outbox.
        backlog_events = gen.outbox.unacked_events
        backlog_batches = gen.outbox.outstanding_batches
        if backlog_events or backlog_batches or ram_events:
            flog(f"WriteQueue: event=wq_stop_left_in_outbox "
                 f"n_events={backlog_events} n_batches={backlog_batches} "
                 f"ram_events={ram_events} writer_alive={gen.alive}: not "
                 f"indexed in the journal yet, durable in the outbox, "
                 f"replayed at the next start", "WARNING")
        if not gen.alive:
            gen.outbox.close()
            gen.state = WriterState.STOPPED
        self._gen = None
        flog("WriteQueue: writer thread stopped")

    # ------------------------------------------------------------------
    # Producer API (GUI thread)
    # ------------------------------------------------------------------

    def prepare_commit(self, commit_id: str, payload: dict,
                       journal_path: Optional[str] = None) -> bool:
        """Durably record a commit's pre-edit state before the provider writes."""
        gen = self._gen
        outbox = gen.outbox if gen is not None and (
            journal_path is None or _same_path(journal_path, gen.db_path)) else None
        close_after = False
        if outbox is None and (journal_path or self._last_db_path):
            outbox = CaptureOutbox(journal_path or self._last_db_path)
            close_after = True
        if outbox is None:
            flog(f"WriteQueue: event=wq_prepare_without_journal commit={commit_id}: "
                 f"the pre-commit state is only in memory until the commit ends",
                 "WARNING")
            return False
        try:
            outbox.append_prepare(commit_id, payload)
            self._outbox_ok = True
            return True
        except OutboxWriteError as exc:
            self._outbox_ok = False
            self._last_error = f"prepare: {exc}"
            flog(f"WriteQueue: event=wq_prepare_failed commit={commit_id} "
                 f"error={exc}: a crash during this commit would leave no "
                 f"trace of the pre-edit state", "ERROR")
            return False
        finally:
            if close_after:
                outbox.close()

    def enqueue(self, events: List[AuditEvent],
                commit: Optional[CommitInfo] = None,
                journal_path: Optional[str] = None) -> EnqueueOutcome:
        """Hand a batch over; see the module docstring for the outcomes.

        *journal_path* binds the batch to the journal it was produced for
        (a restore that started on project A). When it is not the journal
        of the running writer, the batch goes to that journal's own outbox
        and waits there for it: never into another project's journal.
        """
        events = list(events or [])
        if not events and commit is None:
            return EnqueueOutcome.QUEUED_DURABLE
        problems = []
        for i, e in enumerate(events):
            why = validate_event_row(event_row(e))
            if why:
                problems.append((i, why))
        gen = self._gen
        target = journal_path or (gen.db_path if gen is not None else self._last_db_path)
        if problems:
            if not self._quarantine_invalid(target, events, problems) and target:
                with self._stats_lock:
                    self._lost_events += len(events)
                flog(f"WriteQueue: event=wq_events_lost n_events={len(events)} "
                     f"reason=quarantine_unwritable", "CRITICAL")
            return EnqueueOutcome.REFUSED_INVALID

        commit_id = commit.commit_id if commit is not None else None
        events = [
            e._replace(event_uid=e.event_uid or new_event_uid(),
                       commit_id=e.commit_id or commit_id)
            for e in events
        ]
        if gen is not None and not _same_path(target, gen.db_path):
            gen = None   # bound to another journal: its outbox, not our writer
        batch_id = uuid.uuid4().hex
        records = [event_to_record(e) for e in events]
        seq = None
        foreign_outbox = None
        try:
            outbox = gen.outbox if gen is not None else None
            if outbox is None and target:
                foreign_outbox = CaptureOutbox(target)
                outbox = foreign_outbox
            if outbox is not None:
                seq = outbox.append_events(
                    batch_id, records, commit_id=commit_id or "",
                    commit_state=commit.state if commit is not None else "",
                    commit_meta=self._commit_meta(commit, len(events)))
        except OutboxWriteError as exc:
            self._last_error = f"outbox: {exc}"
            self._outbox_ok = False
            seq = None
        finally:
            if foreign_outbox is not None:
                foreign_outbox.close()
        if seq is not None:
            self._outbox_ok = True
        if seq is None:
            return self._write_sync_or_refuse(target, events, commit)

        fault_point("wq_after_outbox_before_queue")
        live = False
        if gen is not None and foreign_outbox is None:
            with gen.accept_lock:
                live = gen.accepting and gen.alive
                if live:
                    gen.queue.put(_Batch(seq, batch_id, events, commit))
                    with self._stats_lock:
                        gen.queued_events += len(events)
        if not live:
            with self._stats_lock:
                self._degraded_events += len(events)
            state = gen.state.value if gen is not None else "no_writer_for_journal"
            flog(f"WriteQueue: event=wq_enqueue_no_writer n_events={len(events)} "
                 f"state={state} journal={target}: batch durable in the "
                 f"outbox, indexed when this journal's writer runs", "ERROR")
            return EnqueueOutcome.PERSISTED_FALLBACK
        self._warn_on_depth()
        return EnqueueOutcome.QUEUED_DURABLE

    @staticmethod
    def _commit_meta(commit: Optional[CommitInfo], n_events: int) -> Optional[dict]:
        if commit is None:
            return None
        meta = {k: v for k, v in commit._asdict().items()
                if v is not None and k not in ("commit_id", "state",
                                               "snapshots_json")}
        meta["n_events"] = n_events
        return meta

    def _write_sync_or_refuse(self, path, events, commit) -> EnqueueOutcome:
        """Last road when the outbox cannot be written: the journal itself."""
        if path:
            conn = None
            try:
                conn = sqlite3.connect(path, timeout=5.0)
                self._sync_mode = _durable_pragmas(conn)
                with conn:
                    ensure_v7_schema(conn)
                    inserted, present, rejected = insert_events_idempotent(
                        conn, [event_row(e) for e in events])
                    if commit is not None:
                        _record_commit(conn, commit, len(events))
                with self._stats_lock:
                    self._degraded_events += len(events)
                flog(f"WriteQueue: event=wq_sync_fallback n_events={len(events)} "
                     f"inserted={inserted} already={present} "
                     f"rejected={len(rejected)}: outbox unavailable "
                     f"({self._last_error}), written synchronously", "ERROR")
                return EnqueueOutcome.PERSISTED_JOURNAL_SYNC
            except sqlite3.Error as exc:
                self._last_error = f"{self._last_error}; journal: {exc}"
            finally:
                if conn is not None:
                    conn.close()
        with self._stats_lock:
            self._lost_events += len(events)
        flog(f"WriteQueue: event=wq_events_lost n_events={len(events)} "
             f"commit={getattr(commit, 'commit_id', None)} journal={path} "
             f"error={self._last_error!r}: NO durable storage could be "
             f"written, these edits have no trace", "CRITICAL")
        _jlog("events_lost", "CRITICAL", event_count=len(events),
              commit_id=getattr(commit, "commit_id", None), journal=path,
              error_detail=self._last_error)
        return EnqueueOutcome.REFUSED_NO_DURABLE_PATH

    def _quarantine_invalid(self, path, events, problems, key: str = "") -> bool:
        """Write *events* to the journal's quarantine. True only once fsync'd.

        Counted as quarantined only when the file is really on disk; when it
        cannot be written the events are counted LOST and said so, never
        reported as preserved.
        """
        flog(f"WriteQueue: event=wq_enqueue_rejected n_events={len(events)} "
             f"n_invalid={len(problems)} first={problems[0]}", "ERROR")
        if not path:
            with self._stats_lock:
                self._lost_events += len(events)
            flog(f"WriteQueue: event=wq_events_lost n_events={len(events)} "
                 f"reason=invalid_batch_without_journal: no quarantine can be "
                 f"written, these edits have no trace", "CRITICAL")
            return False
        try:
            qdir = os.path.join(outbox_dir_for(path), "quarantine")
            os.makedirs(qdir, exist_ok=True)
            # Named after the batch when there is one: isolating the same
            # batch again (an error after the quarantine was written sent
            # it back through the loop) finds the file and neither writes
            # nor counts it twice.
            target = os.path.join(
                qdir, f"invalid-{key or uuid.uuid4().hex}.jsonl")
            if key and os.path.exists(target):
                flog(f"WriteQueue: batch {key} already quarantined, not counted again")
                return True
            reasons = dict(problems)
            tmp = target + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                for i, e in enumerate(events):
                    fh.write(json.dumps({"reason": reasons.get(i, ""),
                                         "e": event_to_record(e)},
                                        ensure_ascii=False, default=str) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, target)
        except (OSError, TypeError, ValueError) as exc:
            flog(f"WriteQueue: cannot quarantine invalid batch: {exc}", "ERROR")
            return False
        with self._stats_lock:
            self._quarantined_events += len(events)
        return True

    def _warn_on_depth(self) -> None:
        depth = self.pending_count
        if depth > _QUEUE_EARLY_WARNING and not self._early_warning_emitted:
            self._early_warning_emitted = True
            flog(f"WriteQueue: queue depth={depth} events: the journal lags "
                 f"behind capture (every event is durable in the outbox)",
                 "WARNING")
            if self._on_early_warning is not None:
                try:
                    self._on_early_warning()
                except Exception as exc:  # noqa: BLE001 - a notification
                    flog(f"WriteQueue: early warning callback failed: {exc}",
                         "WARNING")
        elif depth > _QUEUE_WARNING_THRESHOLD:
            flog(f"WriteQueue: queue depth={depth} exceeds threshold", "WARNING")

    def wait_until_drained(self, timeout: float = 10.0) -> bool:
        """Read-your-writes barrier before planning a Restore or a Rewind.

        True when every batch accepted by the running writer is committed
        in the journal. False on timeout or when the writer is not live:
        the caller must then refuse, or say the journal is incomplete.
        """
        gen = self._gen
        if gen is None:
            return True
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._stats_lock:
                busy = gen.queued_events + gen.inflight_events
            if busy == 0:
                return True
            if not gen.alive or time.monotonic() >= deadline:
                flog(f"WriteQueue: event=wq_barrier_not_reached "
                     f"n_events={busy} writer_alive={gen.alive}", "WARNING")
                return False
            time.sleep(0.02)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def set_early_warning_callback(self, callback) -> None:
        self._on_early_warning = callback

    def set_flush_callback(self, callback) -> None:
        """callback(n_events) on the WRITER thread after each committed batch."""
        self._on_flush_callback = callback

    def set_failure_callback(self, callback) -> None:
        """callback(health) on the GUI thread when watchdog_check sees a failure."""
        self._on_failure = callback

    @property
    def pending_count(self) -> int:
        gen = self._gen
        if gen is None:
            return 0
        with self._stats_lock:
            return gen.queued_events + gen.inflight_events

    @property
    def state(self) -> WriterState:
        return self._gen.state if self._gen is not None else WriterState.STOPPED

    def health(self) -> WriterHealth:
        gen = self._gen
        outbox = gen.outbox if gen is not None else None
        with self._stats_lock:
            return WriterHealth(
                state=(gen.state.value if gen is not None
                       else WriterState.STOPPED.value),
                writer_alive=bool(gen is not None and gen.alive),
                queued_batches=gen.queue.qsize() if gen is not None else 0,
                queued_events=((gen.queued_events + gen.inflight_events)
                               if gen is not None else 0),
                outbox_outstanding=outbox.outstanding_batches if outbox else 0,
                open_prepares=outbox.open_prepares if outbox else 0,
                last_successful_flush=self._last_flush_at,
                last_persistence_error=self._last_error,
                journal_writable=self._journal_writable,
                flushed_events=self._flushed_events,
                degraded_events=self._degraded_events,
                lost_events=self._lost_events,
                quarantined_events=self._quarantined_events,
                journal_path=self._db_path or "",
                last_replay=(self.last_replay_report.as_dict()
                             if self.last_replay_report is not None else None),
                outbox_ok=self._outbox_ok,
                journal_sync=self._sync_mode,
            )

    def watchdog_check(self) -> WriterHealth:
        """Periodic check on the GUI thread: detects a dead or stalled writer."""
        gen = self._gen
        if gen is not None:
            if not gen.alive and not gen.stop_requested:
                with gen.accept_lock:
                    gen.accepting = False
                gen.state = WriterState.FAILED
            elif (gen.alive and gen.state == WriterState.RUNNING
                  and self.pending_count > 0 and self._last_flush_mono
                  and time.monotonic() - self._last_flush_mono > _STALL_SEC):
                gen.state = WriterState.DEGRADED
                self._last_error = (self._last_error or
                                    f"writer stalled for more than {_STALL_SEC:.0f}s")
        health = self.health()
        if gen is not None and gen.state in (WriterState.FAILED, WriterState.DEGRADED):
            if not self._failure_reported:
                self._failure_reported = True
                flog(f"WriteQueue: event=wq_watchdog_alert health="
                     f"{json.dumps(health._asdict(), default=str)}", "CRITICAL")
                _jlog("writer_alert", "CRITICAL", state_after=health.state,
                      writer_alive=health.writer_alive,
                      queue_depth=health.queued_events,
                      outbox_outstanding=health.outbox_outstanding,
                      last_successful_flush=health.last_successful_flush,
                      error_detail=health.last_persistence_error)
                if self._on_failure is not None:
                    try:
                        self._on_failure(health)
                    except Exception as exc:  # noqa: BLE001 - a notification
                        flog(f"WriteQueue: failure callback raised: {exc}", "ERROR")
        elif gen is not None and gen.state == WriterState.RUNNING:
            self._failure_reported = False
        return health

    def restart_writer(self) -> bool:
        """Replace a dead writer: replay its outbox, then a new generation."""
        gen = self._gen
        if gen is None:
            return False
        if gen.alive:
            return True
        path = gen.db_path
        stranded = self._drain_ram(gen)
        gen.outbox.close()
        gen.state = WriterState.STOPPED
        self._gen = None
        if stranded:
            flog(f"WriteQueue: {sum(len(b.events) for b in stranded)} event(s) "
                 f"of the dead writer are integrated from the outbox")
        self.start(path)
        flog("WriteQueue: event=wq_writer_restarted")
        return True

    # ------------------------------------------------------------------
    # Writer thread
    # ------------------------------------------------------------------

    def _writer_loop(self, gen: _Generation) -> None:
        conn: Optional[sqlite3.Connection] = None
        last_checkpoint = time.monotonic()
        clean_exit = False
        retry: List[_Batch] = []
        attempts = 0
        backoff = _RETRY_BASE_SEC
        try:
            conn = sqlite3.connect(gen.db_path, check_same_thread=False)
            self._sync_mode = _durable_pragmas(conn)
            with conn:
                ensure_v7_schema(conn)
            gen.state = WriterState.RUNNING
            self._journal_writable = True
            flog(f"WriteQueue: writer connection opened gen={gen.gen_id}")
            while True:
                if gen.stop_event.is_set() and not retry and gen.queue.empty():
                    break
                try:
                    batches = retry or self._take_batches(gen)
                    if not batches:
                        gen.stop_event.wait(timeout=0.1)
                        if time.monotonic() - last_checkpoint > _WAL_CHECKPOINT_INTERVAL_SEC:
                            _try_wal_checkpoint(conn)
                            last_checkpoint = time.monotonic()
                        continue
                    ok, retryable = self._write_batches(gen, conn, batches)
                    if ok:
                        retry, attempts, backoff = [], 0, _RETRY_BASE_SEC
                        if gen.state == WriterState.DEGRADED and not gen.ack_failed:
                            gen.state = WriterState.RUNNING
                            flog("WriteQueue: event=wq_recovered journal writable again")
                        continue
                    retry = batches
                    attempts += 1
                    if not retryable and attempts >= _POISON_ATTEMPTS:
                        # One refused batch must not take the healthy ones
                        # grouped with it down: isolate it, down to the event.
                        self._isolate_poison(gen, conn, batches)
                        retry, attempts = [], 0
                        continue
                    if gen.stop_event.is_set():
                        break
                    gen.stop_event.wait(timeout=backoff)
                    backoff = min(backoff * 2, _RETRY_MAX_SEC)
                except Exception as loop_err:  # noqa: BLE001 - stay alive
                    # ack, checkpoint, quarantine: whatever failed outside
                    # the batch write, the batches are durable in the
                    # outbox. Keep the consumer alive and retry.
                    self._last_error = (f"writer iteration error: "
                                        f"{type(loop_err).__name__}: {loop_err}")
                    gen.state = WriterState.DEGRADED
                    flog(f"WriteQueue: event=wq_loop_error {self._last_error}; "
                         f"writer stays alive, batches kept durable", "ERROR")
                    gen.stop_event.wait(timeout=backoff)
                    backoff = min(backoff * 2, _RETRY_MAX_SEC)
            clean_exit = True
        except sqlite3.Error as exc:
            self._last_error = f"fatal SQLite error: {exc}"
            self._journal_writable = False
            flog(f"WriteQueue: fatal SQLite error: {exc}", "ERROR")
        except BaseException as exc:  # noqa: BLE001 - record ANY death
            self._last_error = f"fatal writer error: {type(exc).__name__}: {exc}"
            flog(f"WriteQueue: fatal writer error: {type(exc).__name__}: {exc}",
                 "ERROR")
            if not isinstance(exc, Exception):
                raise
        finally:
            with gen.accept_lock:
                gen.accepting = False
            gen.state = WriterState.STOPPED if clean_exit else WriterState.FAILED
            self._final_flush(gen, conn, retry)
            if conn is not None:
                try:
                    _try_wal_checkpoint(conn)
                    conn.close()
                except sqlite3.Error:
                    pass
            if gen.stop_requested and self._gen is not gen:
                gen.outbox.close()
            flog(f"WriteQueue: writer connection closed gen={gen.gen_id}")

    def _final_flush(self, gen, conn, retry: List[_Batch]) -> None:
        batches = list(retry) + self._drain_ram(gen)
        if not batches:
            return
        n = sum(len(b.events) for b in batches)
        if conn is not None and self._write_batches(gen, conn, batches)[0]:
            flog(f"WriteQueue: flushed {n} events at shutdown")
            return
        flog(f"WriteQueue: event=wq_final_flush_deferred n_events={n}: kept "
             f"durable in the outbox, replayed at the next start", "ERROR")

    def _take_batches(self, gen: _Generation) -> List[_Batch]:
        out: List[_Batch] = []
        total = 0
        try:
            while total < _MAX_BATCH_EVENTS:
                batch = gen.queue.get_nowait()
                out.append(batch)
                total += max(1, len(batch.events))
        except queue.Empty:
            pass
        if out:
            n = sum(len(b.events) for b in out)
            with self._stats_lock:
                gen.queued_events = max(0, gen.queued_events - n)
                gen.inflight_events += n
        return out

    def _drain_ram(self, gen: _Generation) -> List[_Batch]:
        out: List[_Batch] = []
        try:
            while True:
                out.append(gen.queue.get_nowait())
        except queue.Empty:
            pass
        with self._stats_lock:
            gen.queued_events = max(
                0, gen.queued_events - sum(len(b.events) for b in out))
        return out

    def _write_batches(self, gen, conn, batches: List[_Batch]):
        """One transaction for the group; ack each batch only after COMMIT.

        Returns (ok, retryable).
        """
        n = sum(len(b.events) for b in batches)
        try:
            fault_point("wq_before_sqlite_commit")
            with conn:
                inserted = present = 0
                for b in batches:
                    ins, pre, rejected = insert_events_idempotent(
                        conn, [event_row(e) for e in b.events])
                    inserted += ins
                    present += pre
                    if rejected:
                        raise sqlite3.IntegrityError(
                            f"{len(rejected)} row(s) failed validation: "
                            f"{rejected[0][1]}")
                    if b.commit is not None:
                        _record_commit(conn, b.commit, len(b.events))
        except sqlite3.OperationalError as exc:
            self._on_write_error(gen, exc, n, retryable=True)
            return False, True
        except Exception as exc:  # noqa: BLE001 - never kill the consumer
            self._on_write_error(gen, exc, n, retryable=False)
            return False, False
        fault_point("wq_after_sqlite_commit_before_ack")
        with self._stats_lock:
            gen.inflight_events = max(0, gen.inflight_events - n)
            self._flushed_events += inserted
            self._last_flush_at = _utc_now()
            self._last_flush_mono = time.monotonic()
        acked = all([self._ack(gen, b) for b in batches])
        self._journal_writable = True
        if acked:
            self._last_error = ""
        gen.ack_failed = not acked
        flog(f"WriteQueue: wrote {inserted} events"
             + (f" ({present} already present)" if present else ""))
        if self._on_flush_callback is not None and inserted and self._gen is gen:
            try:
                self._on_flush_callback(inserted)
            except Exception as cb_err:  # noqa: BLE001 - a notification
                flog(f"WriteQueue: flush callback failed after {inserted} "
                     f"events were committed: {cb_err}", "ERROR")
        return True, True

    def _ack(self, gen, batch) -> bool:
        """Release *batch* from the outbox once it is in the journal.

        Called only after COMMIT (or a durable quarantine). A failure here
        loses nothing: the outbox keeps a copy that the next start replays
        idempotently, and a replay never puts back into the journal an
        event the quarantine already holds. It used to escape to the loop,
        which skipped the in-flight bookkeeping (phantom "being written"
        events for the rest of the session, then a false stall alert) or,
        after a quarantine, re-isolated the batch and quarantined it twice.

        Not silent either: the writer is DEGRADED (the outbox cannot be
        trimmed, it grows until the next start) until a later batch is
        written AND released.
        """
        if batch.seq is None:
            return True
        try:
            gen.outbox.ack(batch.seq, len(batch.events))
            return True
        except Exception as exc:  # noqa: BLE001 - contained, said
            self._last_error = (f"outbox ack failed: {type(exc).__name__}: {exc}")
            if gen.state in (WriterState.RUNNING, WriterState.STARTING):
                gen.state = WriterState.DEGRADED
            flog(f"WriteQueue: event=wq_ack_failed batch={batch.batch_id} "
                 f"seq={batch.seq} error={type(exc).__name__}: {exc}: the rows "
                 f"are in the journal, the outbox copy stays and is replayed "
                 f"idempotently at the next start", "ERROR")
            return False

    def _on_write_error(self, gen, exc, n_events: int, retryable: bool) -> None:
        self._last_error = f"{type(exc).__name__}: {exc}"
        self._journal_writable = False
        if gen.state in (WriterState.RUNNING, WriterState.STARTING):
            gen.state = WriterState.DEGRADED
        flog(f"WriteQueue: event=wq_write_failed n_events={n_events} "
             f"retryable={retryable} error={self._last_error}: batch kept "
             f"durable in the outbox, retrying", "ERROR")

    def _insert_one(self, conn, event) -> str:
        """Insert a single event in its own transaction; '' or the error."""
        try:
            with conn:
                _ins, _pre, rejected = insert_events_idempotent(conn, [event_row(event)])
            if rejected:
                return rejected[0][1]
            return ""
        except Exception as exc:  # noqa: BLE001 - isolating a poison
            return f"{type(exc).__name__}: {exc}"

    def _isolate_poison(self, gen: _Generation, conn, batches: List[_Batch]) -> None:
        """Resolve batches the journal keeps refusing, without losing the good ones.

        Each batch is retried alone, then each of its events alone: every
        event the journal accepts is inserted, only the events it refuses
        are written to the quarantine (fsync'd). A batch is acknowledged
        out of the outbox only when ALL its events are either in the
        journal or durably in the quarantine; otherwise it stays in the
        outbox (replayed at the next start) and is reported.
        """
        for b in batches:
            ok, _retryable = self._write_batches(gen, conn, [b])
            if ok:
                continue
            bad, reasons = [], []
            for event in b.events:
                why = self._insert_one(conn, event)
                if why:
                    bad.append(event)
                    reasons.append((len(bad) - 1, f"journal refused: {why}"))
            if b.commit is not None:
                try:
                    with conn:
                        _record_commit(conn, b.commit, len(b.events) - len(bad))
                except Exception as exc:  # noqa: BLE001
                    flog(f"WriteQueue: commit ledger write failed during poison "
                         f"isolation: {exc}", "ERROR")
            quarantined = True
            if bad:
                quarantined = self._quarantine_invalid(
                    gen.db_path, bad, reasons, key=str(b.batch_id or ""))
            n = len(b.events)
            if not quarantined:
                with self._stats_lock:
                    gen.inflight_events = max(0, gen.inflight_events - n)
                    self._degraded_events += len(bad)
                flog(f"WriteQueue: event=wq_poison_parked n_events={len(bad)} "
                     f"batch={b.batch_id}: the quarantine could not be written; "
                     f"the batch stays in the outbox and is retried at the next "
                     f"start", "CRITICAL")
                continue
            with self._stats_lock:
                gen.inflight_events = max(0, gen.inflight_events - n)
                self._flushed_events += n - len(bad)
            self._ack(gen, b)
            flog(f"WriteQueue: event=wq_poison_batch_quarantined n_events={len(bad)} "
                 f"inserted={n - len(bad)} batch={b.batch_id} "
                 f"error={self._last_error}", "CRITICAL")

    # ------------------------------------------------------------------
    # Legacy surface
    # ------------------------------------------------------------------

    def _save_lost_events(self, events: List[AuditEvent]) -> None:
        """Legacy hook: route events to the durable fallback file."""
        path = self._db_path
        if not events or not path:
            return
        with self._pending_lock:
            from .integrity import save_pending_events
            save_pending_events(path, list(events))


def _jlog(event: str, level: str = "INFO", **fields) -> None:
    try:
        from .logger import jlog
        jlog(event, level, component="write_queue", **fields)
    except Exception:  # noqa: BLE001 # nosec B110 - forensic log never breaks the write path
        pass


def _record_commit(conn, commit: CommitInfo, n_events: int) -> None:
    record_capture_commit(
        conn, commit.commit_id, commit.state,
        layer_id=commit.layer_id, layer_name=commit.layer_name,
        datasource_fingerprint=commit.datasource_fingerprint,
        session_id=commit.session_id, prepared_at=commit.prepared_at,
        finalized_at=_utc_now(), n_events=n_events, detail=commit.detail,
        snapshots_json=commit.snapshots_json)


def _try_wal_checkpoint(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.Error as e:
        flog(f"WriteQueue: WAL checkpoint failed: {e}", "WARNING")
