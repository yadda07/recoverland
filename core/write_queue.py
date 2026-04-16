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
from .sqlite_schema import apply_pragmas
from .logger import flog

_MAX_BATCH_SIZE = 500
_FLUSH_TIMEOUT_SEC = 10
_QUEUE_WARNING_THRESHOLD = 10000
_QUEUE_EARLY_WARNING = 40000
_QUEUE_HARD_LIMIT = 50000
_BATCH_RETRY_COUNT = 3
_BATCH_RETRY_BASE_SEC = 0.2
_WAL_CHECKPOINT_INTERVAL_SEC = 60

_INSERT_SQL = """
    INSERT INTO audit_event (
        project_fingerprint, datasource_fingerprint, layer_id_snapshot,
        layer_name_snapshot, provider_type, feature_identity_json,
        operation_type, attributes_json, geometry_wkb, geometry_type,
        crs_authid, field_schema_json, user_name, session_id,
        created_at, restored_from_event_id,
        entity_fingerprint, event_schema_version, new_geometry_wkb
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


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

    def start(self, db_path: str) -> None:
        """Start the writer thread for the given database path."""
        if self._running:
            self.stop()
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
        """Stop the writer thread, flushing remaining events."""
        if not self._running:
            return
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=_FLUSH_TIMEOUT_SEC)
            if self._thread.is_alive():
                flog("WriteQueue: thread did not stop in time", "WARNING")
        self._running = False
        self._thread = None
        flog("WriteQueue: writer thread stopped")

    def enqueue(self, events: List[AuditEvent]) -> bool:
        """Add events to the write queue.

        Validates JSON fields before accepting. Returns False and saves
        events to the pending recovery file if the queue exceeds the
        hard limit, signaling that tracking should be halted.
        """
        qsize = self._queue.qsize()
        if qsize > _QUEUE_HARD_LIMIT:
            flog(f"WriteQueue: queue size={qsize} exceeds hard limit, "
                 f"saving {len(events)} events to pending recovery", "ERROR")
            self._save_lost_events(events)
            return False
        for event in events:
            if not _validate_event(event):
                continue
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
                batch = self._drain_batch()
                if batch:
                    self._write_batch_with_retry(conn, batch)
                else:
                    self._stop_event.wait(timeout=0.1)
                now = time.monotonic()
                if now - last_checkpoint > _WAL_CHECKPOINT_INTERVAL_SEC:
                    _try_wal_checkpoint(conn)
                    last_checkpoint = now

        except sqlite3.Error as e:
            flog(f"WriteQueue: fatal SQLite error: {e}", "ERROR")
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
                flog(f"WriteQueue: wrote {len(batch)} events")
                return
            except sqlite3.OperationalError as e:
                last_err = e
                wait = _BATCH_RETRY_BASE_SEC * (2 ** attempt)
                flog(f"WriteQueue: retry {attempt+1}/{_BATCH_RETRY_COUNT} "
                     f"after {wait:.1f}s: {e}", "WARNING")
                time.sleep(wait)
            except sqlite3.Error as e:
                flog(f"WriteQueue: non-retryable batch error: {e}", "ERROR")
                self._save_lost_events(batch)
                return
        flog(f"WriteQueue: batch failed after {_BATCH_RETRY_COUNT} retries: "
             f"{last_err}", "ERROR")
        self._save_lost_events(batch)

    def _save_lost_events(self, events: List[AuditEvent]) -> None:
        """Persist unwritten events to the recovery file."""
        if not self._db_path:
            return
        try:
            from .integrity import save_pending_events
            save_pending_events(self._db_path, list(events))
        except Exception as e:
            flog(f"WriteQueue: cannot save pending events: {e}", "ERROR")


def _validate_event(event: AuditEvent) -> bool:
    """Reject events with obviously invalid JSON fields before they enter the queue."""
    if not event.operation_type or event.operation_type not in ("INSERT", "UPDATE", "DELETE"):
        flog(f"WriteQueue: rejected event with bad operation_type={event.operation_type!r}", "WARNING")
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
    )
