"""Async write queue for SQLite audit journal (RLU-063).

Decouples event capture (UI thread) from SQLite writes (dedicated thread).
Uses a standard queue.Queue for thread-safe producer-consumer.
The writer thread batches inserts for performance.
"""
import queue
import threading
import sqlite3
import time
from typing import Optional, List

from .audit_backend import AuditEvent
from .sqlite_schema import (
    apply_pragmas,
    AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
)
from .logger import flog

_MAX_BATCH_SIZE = 500
_FLUSH_TIMEOUT_SEC = 10
_QUEUE_WARNING_THRESHOLD = 10000
_QUEUE_EARLY_WARNING = 40000
_QUEUE_HARD_LIMIT = 50000
_BATCH_RETRY_COUNT = 3
_BATCH_RETRY_BASE_SEC = 0.2
_WAL_CHECKPOINT_INTERVAL_SEC = 60

_INSERT_SQL = "".join((
    "INSERT INTO audit_event (",  # nosec B608: column list is a module constant
    AUDIT_EVENT_INSERT_SQL,
    ") VALUES (",
    AUDIT_EVENT_INSERT_PLACEHOLDERS,
    ")",
))


class WriteQueue:
    """Thread-safe queue that batches audit events into SQLite."""

    def __init__(self):
        self._queue: queue.Queue = queue.Queue()
        self._thread: Optional[threading.Thread] = None
        self._db_path: Optional[str] = None
        self._stop_event = threading.Event()
        self._running = False
        self._early_warning_emitted = False
        self._on_early_warning = None
        self._on_flush_callback = None
        self._pending_lock = threading.Lock()

    def start(self, db_path: str) -> None:
        """Start the writer thread for the given database path."""
        if self._running:
            self.stop()
        # Queued events belong to the journal they were captured against.
        # Restarting on another project's journal (project A -> project B)
        # would drain them into B with A's fingerprints. Route them to A's
        # pending-recovery file instead of moving them across projects.
        if self._db_path and self._db_path != db_path:
            orphans = self._drain_all()
            if orphans:
                flog(
                    f"WriteQueue: {len(orphans)} event(s) left over from "
                    f"{self._db_path}, saved to pending recovery before "
                    f"switching journal",
                    "WARNING",
                )
                self._save_lost_events(orphans)
        self._db_path = db_path
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._writer_loop,
            name="RecoverLand-Writer",
            daemon=True,
        )
        self._running = True
        self._thread.start()
        flog("WriteQueue: writer thread started")

    def stop(self) -> None:
        """Stop the writer thread, flushing remaining events.

        Idempotent and safe on a queue that was never started.
        """
        was_running = self._running
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=_FLUSH_TIMEOUT_SEC)
            if thread.is_alive():
                flog("WriteQueue: thread did not stop in time", "WARNING")
        self._running = False
        self._thread = None
        # The writer flushes on its way out, but it may have died long before
        # (fatal SQLite error) or an event may have landed after its last
        # drain. Never leave an accepted event in RAM: check the queue
        # ourselves and route what is left to the pending-recovery file.
        leftover = self._drain_all()
        if leftover:
            flog(
                f"WriteQueue: {len(leftover)} event(s) still queued after the "
                f"writer stopped, saved to pending recovery",
                "ERROR",
            )
            self._save_lost_events(leftover)
        if was_running:
            flog("WriteQueue: writer thread stopped")

    def enqueue(self, events: List[AuditEvent]) -> bool:
        """Add events to the write queue (all-or-nothing per batch).

        Validates all events before any are enqueued. If any event is invalid,
        the entire batch is saved to pending recovery and False is returned.
        Returns False and saves events to pending recovery if the queue would
        exceed the hard limit, signaling that tracking should be halted.
        Returns False as well when no live writer thread can drain the queue.
        """
        # A queue without a live consumer is a black hole: returning True
        # would be an acknowledgement for events nobody will ever write, and
        # the caller (EditSessionTracker) drops them on that promise. This
        # covers a queue never started (journal could not be opened), one
        # already stopped (project closing), and one whose writer thread died.
        thread = self._thread
        thread_alive = thread is not None and thread.is_alive()
        if not self._running or not thread_alive:
            flog(
                f"WriteQueue: event=wq_enqueue_no_writer n_events={len(events)} "
                f"running={self._running} thread_alive={thread_alive}, "
                f"saving {len(events)} events to pending recovery",
                "ERROR",
            )
            self._save_lost_events(events)
            return False

        qsize = self._queue.qsize()
        # Pre-check capacity against the batch size to prevent a huge single
        # call (e.g. 100k mass deletes) from silently overflowing after the
        # put loop. The check is a best-effort upper bound: qsize is racy
        # by design in queue.Queue, but since the writer thread only drains,
        # qsize + len(events) is still a safe upper bound for this producer.
        if qsize + len(events) > _QUEUE_HARD_LIMIT:
            flog(
                f"WriteQueue: queue size={qsize} + batch={len(events)} "
                f"exceeds hard limit {_QUEUE_HARD_LIMIT}, "
                f"saving {len(events)} events to pending recovery",
                "ERROR",
            )
            self._save_lost_events(events)
            return False

        # Pre-validate all events before any put() to ensure atomicity (INV-1)
        invalid_count = 0
        first_invalid_idx = -1
        for idx, event in enumerate(events):
            if not _validate_event(event):
                invalid_count += 1
                if first_invalid_idx == -1:
                    first_invalid_idx = idx

        if invalid_count > 0:
            flog(
                f"WriteQueue: event=wq_enqueue_rejected n_events={len(events)} "
                f"n_invalid={invalid_count} first_invalid_idx={first_invalid_idx}",
                "ERROR",
            )
            self._save_lost_events(events)
            return False

        # All events valid: enqueue them
        for event in events:
            self._queue.put(event)

        qsize = self._queue.qsize()
        if qsize > _QUEUE_EARLY_WARNING and not self._early_warning_emitted:
            self._early_warning_emitted = True
            flog(f"WriteQueue: queue size={qsize} at 80% of hard limit", "WARNING")
            if self._on_early_warning is not None:
                self._on_early_warning()
        elif qsize > _QUEUE_WARNING_THRESHOLD:
            flog(f"WriteQueue: queue size={qsize} exceeds threshold", "WARNING")
        return True

    def set_early_warning_callback(self, callback) -> None:
        """Set callback() invoked once when queue reaches 80% of hard limit."""
        self._on_early_warning = callback

    def set_flush_callback(self, callback) -> None:
        """Set callback(n_events) invoked after successful batch write.

        The callback is called by the writer thread after each successful
        batch write (executemany + commit). The argument n_events is the
        number of events in the batch that was just persisted.
        """
        self._on_flush_callback = callback

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    def _writer_loop(self) -> None:
        """Main loop: drain queue in batches and write to SQLite.

        On fatal error the remaining queue is saved to a pending-recovery
        file so events are not silently lost.
        """
        conn: Optional[sqlite3.Connection] = None
        last_checkpoint = time.monotonic()
        try:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            apply_pragmas(conn)
            flog("WriteQueue: writer connection opened")

            while not self._stop_event.is_set():
                # Any unexpected error inside the loop used to end the thread
                # and turn the queue into a silent black hole for the rest of
                # the session. Keep the consumer alive: the next iteration
                # drains the queue again, and a batch that cannot be written
                # is already routed to pending recovery by the write path.
                try:
                    batch = self._drain_batch()
                    if batch:
                        self._write_batch_with_retry(conn, batch)
                    else:
                        self._stop_event.wait(timeout=0.1)
                    now = time.monotonic()
                    if now - last_checkpoint > _WAL_CHECKPOINT_INTERVAL_SEC:
                        _try_wal_checkpoint(conn)
                        last_checkpoint = now
                except Exception as loop_err:  # noqa: BLE001
                    flog(
                        f"WriteQueue: writer loop error, staying alive: "
                        f"{loop_err}",
                        "ERROR",
                    )
                    self._stop_event.wait(timeout=0.1)

        except sqlite3.Error as e:
            flog(f"WriteQueue: fatal SQLite error: {e}", "ERROR")
        except Exception as e:  # noqa: BLE001
            flog(f"WriteQueue: fatal writer error: {e}", "ERROR")
        finally:
            remaining = self._drain_all()
            if remaining and conn is not None:
                try:
                    self._write_batch_with_retry(conn, remaining)
                    flog(f"WriteQueue: flushed {len(remaining)} events at shutdown")
                except Exception as flush_err:
                    flog(f"WriteQueue: flush failed: {flush_err}", "ERROR")
                    self._save_lost_events(remaining)
            elif remaining:
                self._save_lost_events(remaining)
            if conn:
                _try_wal_checkpoint(conn)
                conn.close()
            flog("WriteQueue: writer connection closed")

    def _drain_batch(self) -> List[AuditEvent]:
        batch: List[AuditEvent] = []
        try:
            while len(batch) < _MAX_BATCH_SIZE:
                event = self._queue.get_nowait()
                batch.append(event)
        except queue.Empty:
            pass
        return batch

    def _drain_all(self) -> List[AuditEvent]:
        items: List[AuditEvent] = []
        try:
            while True:
                items.append(self._queue.get_nowait())
        except queue.Empty:
            pass
        return items

    def _write_batch_with_retry(self, conn: sqlite3.Connection,
                                batch: List[AuditEvent]) -> None:
        """Write a batch with retry on transient errors."""
        params = [_event_to_row(e) for e in batch]
        last_err: Optional[Exception] = None
        for attempt in range(_BATCH_RETRY_COUNT):
            try:
                with conn:
                    conn.executemany(_INSERT_SQL, params)
            except sqlite3.OperationalError as e:
                last_err = e
                wait = _BATCH_RETRY_BASE_SEC * (2 ** attempt)
                flog(f"WriteQueue: retry {attempt + 1}/{_BATCH_RETRY_COUNT} "
                     f"after {wait:.1f}s: {e}", "WARNING")
                time.sleep(wait)
                continue
            except sqlite3.Error as e:
                flog(f"WriteQueue: non-retryable batch error: {e}", "ERROR")
                self._save_lost_events(batch)
                return
            flog(f"WriteQueue: wrote {len(batch)} events")
            # The batch is committed. The callback is only a notification
            # (a Qt signal towards a bridge the plugin may already have
            # destroyed): its failure must neither be mistaken for a write
            # failure nor kill the writer thread.
            if self._on_flush_callback is not None:
                try:
                    self._on_flush_callback(len(batch))
                except Exception as cb_err:  # noqa: BLE001
                    flog(f"WriteQueue: flush callback failed after "
                         f"{len(batch)} events were committed: {cb_err}",
                         "ERROR")
            return
        flog(f"WriteQueue: batch failed after {_BATCH_RETRY_COUNT} retries: "
             f"{last_err}", "ERROR")
        self._save_lost_events(batch)

    def _save_lost_events(self, events: List[AuditEvent]) -> None:
        """Persist unwritten events to the recovery file.

        Thread-safe: uses _pending_lock to serialize concurrent calls
        from the producer (UI thread, enqueue overflow) and the writer
        thread (batch failure).
        """
        if not events:
            return
        if not self._db_path:
            # No journal path means no place to write the recovery file.
            # Say it loudly instead of dropping the batch in silence.
            flog(
                f"WriteQueue: {len(events)} event(s) dropped, no journal path "
                f"to write the pending recovery file",
                "ERROR",
            )
            return
        with self._pending_lock:
            try:
                from .integrity import save_pending_events
                save_pending_events(self._db_path, list(events))
            except Exception as e:
                flog(f"WriteQueue: cannot save pending events: {e}", "ERROR")


def _validate_event(event: AuditEvent) -> bool:
    """Reject events with obviously invalid JSON fields before they enter the queue."""
    if not event.operation_type or event.operation_type not in ("INSERT", "UPDATE", "DELETE"):
        flog(
            f"WriteQueue: rejected event with bad operation_type={event.operation_type!r}",
            "WARNING",
        )
        return False
    if not event.attributes_json or not isinstance(event.attributes_json, str):
        flog("WriteQueue: rejected event with empty/non-string attributes_json", "WARNING")
        return False
    if not event.created_at:
        flog("WriteQueue: rejected event with empty created_at", "WARNING")
        return False
    return True


def _try_wal_checkpoint(conn: sqlite3.Connection) -> None:
    """Run a passive WAL checkpoint to keep the WAL file bounded."""
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except sqlite3.Error as e:
        flog(f"WriteQueue: WAL checkpoint failed: {e}", "WARNING")


def _event_to_row(event: AuditEvent) -> tuple:
    return (
        event.project_fingerprint,
        event.datasource_fingerprint,
        event.layer_id_snapshot,
        event.layer_name_snapshot,
        event.provider_type,
        event.feature_identity_json,
        event.operation_type,
        event.attributes_json,
        event.geometry_wkb,
        event.geometry_type,
        event.crs_authid,
        event.field_schema_json,
        event.user_name,
        event.session_id,
        event.created_at,
        event.restored_from_event_id,
        event.entity_fingerprint,
        event.event_schema_version,
        event.new_geometry_wkb,
        event.invalidated_at,
    )
