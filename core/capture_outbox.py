# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Durable write-ahead outbox between capture and the SQLite journal.

Why it exists
-------------
Before this module, an accepted edit lived ONLY in a ``queue.Queue`` until
the writer thread committed it: 35 ms for one event, 1.3 s for 10 000
(measured, ``scripts/validation/bench_durability.py``), unbounded when the
journal is locked. A QGIS crash inside that window erased edits the user
had already written to his data, and nothing on disk said so.

Writing the journal synchronously on the GUI thread closes the window but
costs 0.1 to 0.4 ms per event (11 B-tree indexes to maintain). An
append-only JSON Lines file with one ``fsync`` per batch costs about
30 us per event plus 1.5 ms: that is the hand-off that makes an event
durable, and the writer thread keeps doing the expensive indexing.

Contract
--------
* ``append_events`` returns only after the batch is on disk (``fsync``).
  From that point the batch survives a crash of QGIS: the next start
  replays it into the journal (``replay_outbox``), idempotently, keyed by
  ``event_uid``.
* ``append_prepare`` records the pre-commit state of a QGIS commit BEFORE
  the provider writes anything. A prepare with no matching ``finalize``
  after a crash is an INTERRUPTED commit: the old state of every feature
  it touched is preserved and reported, never silently dropped.
* A segment is truncated only when every batch and every prepare it holds
  has been acknowledged. Lines that cannot be parsed are moved to a
  quarantine directory, never deleted.

Threads: ``append_*`` run on the capture (GUI) thread, ``ack`` on the
writer thread. Both take ``self._lock``.
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .logger import flog

OUTBOX_SUFFIX = ".outbox"
QUARANTINE_DIRNAME = "quarantine"
_SEGMENT_PREFIX = "seg-"
_SEGMENT_SUFFIX = ".jsonl"
_BYTES_FIELDS = ("geometry_wkb", "new_geometry_wkb")


class OutboxWriteError(OSError):
    """The outbox could not make a batch durable."""


def outbox_dir_for(journal_path: str) -> str:
    """One outbox per journal file, never shared.

    The legacy pending file (``recoverland_pending.json``) lives in the
    journal DIRECTORY, which every unsaved-project journal of a profile
    shares: events saved for one journal were re-imported into another at
    the next start. Naming the outbox after the journal file removes that
    ambiguity by construction.
    """
    return _extended(os.path.abspath(journal_path) + OUTBOX_SUFFIX)


def _extended(path: str) -> str:
    """Windows extended-length form of *path* (no-op elsewhere).

    The outbox lives next to the journal, and journals live next to
    projects: a deep project folder puts '<journal>.outbox/seg-00000001.jsonl'
    past the 260-character MAX_PATH limit (WinError 206, measured). The
    extended-length prefix (two backslashes, a question mark, a backslash)
    lifts that limit for every Win32 file call without requiring the
    LongPathsEnabled registry setting. One definition, shared with the
    journal's own paths (core/long_paths.py).
    """
    from .long_paths import extended
    return extended(path)


# One encoder for every line: json.dumps with options builds a new encoder
# per call, a third of the GUI-thread cost of a 10 000 event commit.
# ASCII output: a lone UTF-16 surrogate in a captured string is escaped
# instead of failing the UTF-8 write of the whole batch; the journal then
# refuses that one event and it goes to the quarantine.
_LINE_ENCODER = json.JSONEncoder(ensure_ascii=True, separators=(",", ":"))


def new_event_uid() -> str:
    # 128 random bits, the same 32 hex characters as uuid4().hex, cheaper.
    return os.urandom(16).hex()


def new_commit_id() -> str:
    return uuid.uuid4().hex


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def event_to_record(event) -> dict:
    """AuditEvent (or dict) -> JSON-safe dict, bytes as ``b64:``."""
    data = event._asdict() if hasattr(event, "_asdict") else dict(event)
    for key in _BYTES_FIELDS:
        value = data.get(key)
        if isinstance(value, (bytes, bytearray, memoryview)):
            data[key] = "b64:" + base64.b64encode(bytes(value)).decode("ascii")
    return data


def record_to_row_values(record: dict) -> dict:
    """Inverse of :func:`event_to_record` for the columns we insert."""
    out = dict(record)
    for key in _BYTES_FIELDS:
        value = out.get(key)
        if isinstance(value, str) and value.startswith("b64:"):
            out[key] = base64.b64decode(value[4:])
    return out


def _segment_seq(name: str) -> Optional[int]:
    if not (name.startswith(_SEGMENT_PREFIX) and name.endswith(_SEGMENT_SUFFIX)):
        return None
    try:
        return int(name[len(_SEGMENT_PREFIX):-len(_SEGMENT_SUFFIX)])
    except ValueError:
        return None


def list_segments(outbox_dir: str) -> List[Tuple[int, str]]:
    if not os.path.isdir(outbox_dir):
        return []
    out = []
    for name in os.listdir(outbox_dir):
        seq = _segment_seq(name)
        if seq is not None:
            out.append((seq, os.path.join(outbox_dir, name)))
    return sorted(out)


class CaptureOutbox:
    """Append-only fsync'd log of captured batches for one journal."""

    def __init__(self, journal_path: str):
        self.journal_path = journal_path
        self.dir = outbox_dir_for(journal_path)
        self._lock = threading.Lock()
        self._fh = None
        self._cur_seq: Optional[int] = None
        self._next_seq = 1
        self._outstanding: Dict[int, int] = {}
        self._unacked_events = 0
        self._prepare_seg: Dict[str, int] = {}
        self._closed = False
        self.last_error: str = ""
        self.last_append_at: str = ""
        self.appended_batches = 0
        self.appended_events = 0
        existing = list_segments(self.dir)
        if existing:
            self._next_seq = existing[-1][0] + 1

    # ------------------------------------------------------------------
    # Producer side (capture thread)
    # ------------------------------------------------------------------

    def append_prepare(self, commit_id: str, payload: dict) -> int:
        """Durably record the pre-commit state of a QGIS commit."""
        line = {"t": "prepare", "c": commit_id, "at": _utc_now()}
        line.update(payload)
        with self._lock:
            seq = self._write_lines([line])
            self._outstanding[seq] = self._outstanding.get(seq, 0) + 1
            self._prepare_seg[commit_id] = seq
            return seq

    def append_finalize(self, batch_id: str, commit_id: str, state: str,
                        detail: str = "") -> int:
        """Resolve a prepare that produced no event.

        Written as an EMPTY batch on purpose: it must travel through the
        writer like any batch, so the journal records the commit
        (``capture_commit``) before the outbox forgets it. The journal is
        the authority a replay consults to tell a resolved prepare from
        an interrupted one.
        """
        return self.append_events(batch_id, [], commit_id=commit_id,
                                  commit_state=state,
                                  commit_meta={"detail": detail, "n_events": 0})

    def append_events(self, batch_id: str, records: List[dict],
                      commit_id: str = "", commit_state: str = "",
                      commit_meta: Optional[dict] = None) -> int:
        """Durably append one batch; returns the segment sequence.

        ``commit_id``/``commit_state``: when the batch is the outcome of a
        prepared commit, the finalize record is written in the same
        append, so the reader sees the events and their resolution
        together or not at all.
        """
        # One event per line: a write torn by a crash loses at most the
        # line being written, every complete line before it is replayed.
        lines = [{"t": "ev", "b": batch_id, "e": rec} for rec in records]
        if commit_id and commit_state:
            fin = {"t": "finalize", "c": commit_id, "state": commit_state,
                   "b": batch_id, "at": _utc_now()}
            if commit_meta:
                fin["meta"] = commit_meta
            lines.append(fin)
        lines.append({"t": "end", "b": batch_id, "n": len(records)})
        with self._lock:
            seq = self._write_lines(lines)
            self._outstanding[seq] = self._outstanding.get(seq, 0) + 1
            self._unacked_events += len(records)
            if commit_id and commit_state:
                self._resolve_prepare_locked(commit_id)
            self.appended_batches += 1
            self.appended_events += len(records)
            return seq

    def _write_lines(self, lines: List[dict]) -> int:
        if self._closed:
            raise OutboxWriteError("outbox is closed")
        try:
            if self._fh is None:
                os.makedirs(self.dir, exist_ok=True)
                self._cur_seq = self._next_seq
                self._next_seq += 1
                path = os.path.join(
                    self.dir, f"{_SEGMENT_PREFIX}{self._cur_seq:08d}{_SEGMENT_SUFFIX}")
                self._fh = open(path, "a", encoding="utf-8", newline="\n")
            encode = _LINE_ENCODER.encode
            payload = "".join(encode(line) + "\n" for line in lines)
            _fault("outbox_before_write")
            if _fault_active("outbox_torn_write"):
                self._fh.write(payload[: max(1, len(payload) // 2)])
                self._fh.flush()
                os.fsync(self._fh.fileno())
                _fault("outbox_torn_write")
            self._fh.write(payload)
            self._fh.flush()
            os.fsync(self._fh.fileno())
            _fault("outbox_after_fsync")
        except OutboxWriteError:
            raise
        except (OSError, ValueError, TypeError) as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            flog(f"CaptureOutbox: event=outbox_write_failed dir={self.dir} "
                 f"error={self.last_error}", "ERROR")
            raise OutboxWriteError(self.last_error) from exc
        self.last_append_at = _utc_now()
        return self._cur_seq

    def _resolve_prepare_locked(self, commit_id: str) -> None:
        seq = self._prepare_seg.pop(commit_id, None)
        if seq is not None:
            self._release_locked(seq)

    # ------------------------------------------------------------------
    # Consumer side (writer thread)
    # ------------------------------------------------------------------

    def ack(self, seq: int, n_events: int = 0) -> None:
        """One batch of segment *seq* is committed in the journal."""
        with self._lock:
            self._unacked_events = max(0, self._unacked_events - int(n_events or 0))
            self._release_locked(seq)

    def _release_locked(self, seq: int) -> None:
        left = self._outstanding.get(seq, 0) - 1
        if left > 0:
            self._outstanding[seq] = left
            return
        self._outstanding.pop(seq, None)
        path = os.path.join(self.dir, f"{_SEGMENT_PREFIX}{seq:08d}{_SEGMENT_SUFFIX}")
        try:
            if seq == self._cur_seq and self._fh is not None:
                # Truncate rather than delete: the next commit reuses the
                # open handle instead of paying a create + delete.
                self._fh.seek(0)
                self._fh.truncate()
                self._fh.flush()
                os.fsync(self._fh.fileno())
            elif os.path.isfile(path):
                os.remove(path)
        except OSError as exc:
            # Not a loss: the segment is replayed idempotently at the next
            # start. Say it, since the outbox then keeps growing.
            self.last_error = f"{type(exc).__name__}: {exc}"
            flog(f"CaptureOutbox: cannot release segment {seq} "
                 f"({self.last_error}), it will be replayed idempotently",
                 "WARNING")

    # ------------------------------------------------------------------

    @property
    def outstanding_batches(self) -> int:
        with self._lock:
            return sum(self._outstanding.values())

    @property
    def unacked_events(self) -> int:
        """Events durable in this outbox and not yet committed in the journal."""
        with self._lock:
            return self._unacked_events

    @property
    def open_prepares(self) -> int:
        with self._lock:
            return len(self._prepare_seg)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._fh is not None:
                try:
                    empty = self._fh.tell() == 0
                    self._fh.close()
                    if empty and self._cur_seq is not None:
                        path = os.path.join(
                            self.dir,
                            f"{_SEGMENT_PREFIX}{self._cur_seq:08d}{_SEGMENT_SUFFIX}")
                        if os.path.isfile(path) and os.path.getsize(path) == 0:
                            os.remove(path)
                except OSError as exc:
                    flog(f"CaptureOutbox: close error {exc}", "WARNING")
                self._fh = None


# ----------------------------------------------------------------------
# Fault injection (tests only; inert unless RECOVERLAND_FAULT is set)
# ----------------------------------------------------------------------

_FAULT_ENV = "RECOVERLAND_FAULT"


def _fault_active(point: str) -> bool:
    return os.environ.get(_FAULT_ENV, "") == point


def _fault(point: str) -> None:
    """Terminate the process abruptly at *point* when asked to.

    Used by the crash-safety scenarios, which run the capture in a child
    process and kill it at a precise spot. ``os._exit`` skips every
    ``finally``, ``atexit`` and destructor: exactly what a crash does.
    """
    if _fault_active(point):
        flog(f"FAULT_INJECTED point={point} pid={os.getpid()}", "CRITICAL")
        os._exit(86)


def fault_point(point: str) -> None:
    _fault(point)


# ----------------------------------------------------------------------
# Replay (startup, or when a writer is restarted after a failure)
# ----------------------------------------------------------------------

class ReplayReport:
    """What a replay found, in the terms the user needs after a crash."""

    def __init__(self):
        self.segments = 0
        self.events_seen = 0
        self.events_inserted = 0
        self.events_already_present = 0
        self.events_rejected = 0
        self.torn_lines = 0
        self.interrupted_commits: List[dict] = []
        self.finalized_commits = 0
        self.quarantined: List[str] = []
        self.errors: List[str] = []
        self.kept_segments = 0
        self.incomplete_batches: List[dict] = []
        self.events_already_quarantined = 0

    def as_dict(self) -> dict:
        return {
            "segments": self.segments,
            "events_seen": self.events_seen,
            "events_inserted": self.events_inserted,
            "events_already_present": self.events_already_present,
            "events_rejected": self.events_rejected,
            "torn_lines": self.torn_lines,
            "interrupted_commits": len(self.interrupted_commits),
            "finalized_commits": self.finalized_commits,
            "quarantined": list(self.quarantined),
            "errors": list(self.errors),
            "kept_segments": self.kept_segments,
            "incomplete_batches": len(self.incomplete_batches),
            "events_already_quarantined": self.events_already_quarantined,
        }

    @property
    def nothing_found(self) -> bool:
        return self.segments == 0


def _parse_segment(path: str):
    lines, torn = [], []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            text = raw.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except ValueError:
                torn.append(raw)
                continue
            if not isinstance(obj, dict) or "t" not in obj:
                torn.append(raw)
                continue
            lines.append(obj)
    return lines, torn


def _quarantine(outbox_dir: str, seg_path: str, torn: List[str],
                report: ReplayReport, kind: str = "torn") -> None:
    """Write *torn* lines to a NEW quarantine file, fsync'd.

    Every file gets a name of its own: a replay quarantines the torn lines
    of a segment and then, in the same second, the rows the journal
    refuses from that segment. With a name built from the segment and the
    second only, the second write replaced the first and the segment was
    then deleted: a torn line (a real edit) was gone while the report said
    it was quarantined. The name now carries a digest of the content and
    the file is created exclusively: two different contents can never
    share a file, and the same content quarantined again (a segment kept
    by a failed replay and replayed later) finds its file already there
    instead of being counted twice.
    """
    import hashlib
    qdir = os.path.join(outbox_dir, QUARANTINE_DIRNAME)
    os.makedirs(qdir, exist_ok=True)
    digest = hashlib.sha256("".join(torn).encode("utf-8", "replace")).hexdigest()[:16]
    target = os.path.join(qdir, f"{os.path.basename(seg_path)}.{kind}.{digest}")
    tmp = target + ".tmp"
    if os.path.exists(target):
        report.quarantined.append(target)
        return
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.writelines(torn)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, target)
    report.quarantined.append(target)


def quarantined_uids(outbox_dir: str) -> set:
    """event_uid of every record the quarantine holds (parsable lines only).

    An event is in the journal OR in the quarantine, never both. When the
    writer quarantines the refused events of a batch and then fails to
    release the batch from the outbox (or QGIS stops in between), the
    outbox still holds them: replayed as is, an event the journal refused
    in memory but accepts from its disk copy landed in the journal while
    the quarantine kept it too, and the user was told about an edit in a
    dead letter that was also in the history.
    """
    qdir = os.path.join(outbox_dir, QUARANTINE_DIRNAME)
    out = set()
    try:
        names = os.listdir(qdir)
    except OSError:
        return out
    for name in names:
        if name.endswith(".tmp"):
            continue
        try:
            with open(os.path.join(qdir, name), "r", encoding="utf-8",
                      errors="replace") as fh:
                for raw in fh:
                    try:
                        rec = json.loads(raw)
                    except ValueError:
                        continue
                    ev = rec.get("e") if isinstance(rec, dict) else None
                    uid = ev.get("event_uid") if isinstance(ev, dict) else None
                    if uid:
                        out.add(uid)
        except OSError:
            continue
    return out


def quarantine_backlog(journal_path: str) -> int:
    """Records waiting in the journal's quarantine, as found on disk.

    The quarantine holds accepted edits the journal refused and torn lines
    of the outbox: they are preserved, not integrated. A counter kept in
    memory forgot them at the next start and the indicator went back to
    PROTECTED while they were still out of the journal. The files are the
    truth: every non-empty line is one record. Files are only ever
    written complete (fsync'd, or renamed into place), so a count is not
    fooled by a write in progress.
    """
    qdir = os.path.join(outbox_dir_for(journal_path), QUARANTINE_DIRNAME)
    total = 0
    try:
        names = os.listdir(qdir)
    except OSError:
        return 0
    for name in names:
        if name.endswith(".tmp"):
            continue
        try:
            with open(os.path.join(qdir, name), "r", encoding="utf-8",
                      errors="replace") as fh:
                total += sum(1 for line in fh if line.strip())
        except OSError:
            continue
    return total


def _preserved_state(prep: dict) -> list:
    """What an interrupted commit leaves for reconciliation: every snapshot
    plus the content of every feature it was adding (kind INSERT).

    An addition keeps, besides its content, what the verdict on it rests
    on: ``unset_fields`` (the fields left to the source: never compared),
    ``twins`` / ``n_twins`` (the features that already held its content
    before the commit, ``[]`` when the capture checked and found none) and
    ``rounding`` (the fields the source rounds on write; its presence says
    the twins were searched as the source stores values, which lets the
    reconciliation recognise a rounded addition). Only the attributes and
    the geometry used to be kept: the reconciliation could not tell the
    addition from a feature that held the same content before the commit,
    so a duplicated feature was "unresolved" whether the provider had
    written it or not.
    """
    out = list(prep.get("snapshots") or [])
    for added in prep.get("added") or []:
        if isinstance(added, dict) and "attributes" in added:
            record = {"kind": "INSERT", "fid": None,
                      "attributes": added.get("attributes") or {},
                      "geometry_wkb": added.get("geometry_wkb")}
            # What crash_reconcile._addition_verdict rests on.
            for key in ("unset_fields", "twins", "n_twins", "rounding"):
                if key in added:
                    record[key] = added[key]
            out.append(record)
    return out


def _commit_recorded(conn: sqlite3.Connection, commit_id: str) -> bool:
    """True when the journal already knows how *commit_id* ended.

    A prepare line can outlive its finalize line on disk (the segment of
    the finalize is released once the writer has committed it, the
    segment of the prepare may still hold other work). The journal row is
    what proves the resolution, not the presence of a line in the outbox.
    """
    try:
        row = conn.execute(
            "SELECT 1 FROM capture_commit WHERE commit_id = ? "
            "AND state != 'INTERRUPTED'", (commit_id,)).fetchone()
        if row is not None:
            return True
        row = conn.execute(
            "SELECT 1 FROM audit_event WHERE commit_id = ? LIMIT 1",
            (commit_id,)).fetchone()
        return row is not None
    except sqlite3.Error:
        return False


def replay_outbox(journal_path: str, conn: sqlite3.Connection,
                  trace_id: str = "") -> ReplayReport:
    """Integrate every durable outbox segment into the journal.

    Idempotent: an event already in the journal (same ``event_uid``) is
    counted, not duplicated. A segment is deleted only after its events
    are committed; a segment that fails is kept for the next attempt.
    Must not run while a live CaptureOutbox writes the same directory.
    """
    from .journal_writes import (
        insert_events_idempotent, record_capture_commit, ensure_v7_schema,
    )
    report = ReplayReport()
    outbox_dir = outbox_dir_for(journal_path)
    segments = list_segments(outbox_dir)
    report.segments = len(segments)
    if not segments:
        return report
    prefix = f"[{trace_id}] " if trace_id else ""
    prepares: Dict[str, dict] = {}
    finalized: Dict[str, dict] = {}
    parsed = []
    # Segments that must survive this replay: something in them could not
    # be put anywhere durable yet. Their valid rows are still integrated
    # (idempotently, by event_uid), so a blocked quarantine never holds
    # back the edits that can be indexed.
    keep = set()
    for seq, path in segments:
        try:
            lines, torn = _parse_segment(path)
        except OSError as exc:
            report.errors.append(f"{os.path.basename(path)}: {exc}")
            report.kept_segments += 1
            continue
        if torn:
            report.torn_lines += len(torn)
            try:
                _quarantine(outbox_dir, path, torn, report)
            except OSError as exc:
                report.errors.append(f"quarantine {os.path.basename(path)}: {exc}")
                keep.add(path)
        parsed.append((seq, path, lines))
        for line in lines:
            if line.get("t") == "prepare":
                prepares[line.get("c", "")] = line
            elif line.get("t") == "finalize":
                finalized[line.get("c", "")] = line

    try:
        with conn:
            ensure_v7_schema(conn)
    except sqlite3.Error as exc:
        report.errors.append(f"schema v7: {exc}")
        report.kept_segments = len(segments)
        flog(f"{prefix}replay_outbox: journal cannot take v7 rows ({exc}), "
             f"{len(segments)} segment(s) kept for the next attempt", "ERROR")
        return report
    in_quarantine = quarantined_uids(outbox_dir)
    for seq, path, lines in parsed:
        records = []
        for line in lines:
            if line.get("t") == "ev" and isinstance(line.get("e"), dict):
                if line["e"].get("event_uid") in in_quarantine:
                    report.events_already_quarantined += 1
                    continue
                records.append(line["e"])
        report.events_seen += len(records)
        try:
            with conn:
                inserted, present, rejected = insert_events_idempotent(
                    conn, [record_to_row_values(r) for r in records])
                if rejected:
                    try:
                        _quarantine(outbox_dir, path, [
                            json.dumps({"reason": why, "e": rec}, ensure_ascii=False,
                                       default=str) + "\n"
                            for rec, why in rejected], report, kind="rejected")
                    except OSError as exc:
                        # The refused rows stay in the segment, which is
                        # kept; the valid ones are committed below.
                        report.errors.append(
                            f"quarantine {os.path.basename(path)}: {exc}")
                        keep.add(path)
                for line in lines:
                    if line.get("t") == "finalize":
                        cid = line.get("c", "")
                        prep = prepares.get(cid, {})
                        record_capture_commit(
                            conn, cid, line.get("state", "COMMITTED"),
                            layer_id=prep.get("layer_id"),
                            layer_name=prep.get("layer_name"),
                            datasource_fingerprint=prep.get("ds"),
                            session_id=prep.get("session_id"),
                            prepared_at=prep.get("at"),
                            finalized_at=line.get("at"),
                            n_snapshots=len(prep.get("snapshots") or []),
                            n_events=(line.get("meta") or {}).get("n_events"),
                            detail=(line.get("meta") or {}).get("detail")
                            or "replayed from outbox",
                            # The ledger must keep the pre-edit state of a
                            # commit whose events could not be built.
                            snapshots_json=(
                                json.dumps(_preserved_state(prep), ensure_ascii=True)
                                if line.get("state") == "GENERATION_FAILED"
                                else None))
            report.events_inserted += inserted
            report.events_already_present += present
            report.events_rejected += len(rejected)
        except sqlite3.Error as exc:
            report.errors.append(f"{os.path.basename(path)}: {exc}")
            report.kept_segments += 1
            flog(f"{prefix}replay_outbox: segment {seq} kept, journal write "
                 f"failed: {exc}", "ERROR")
            continue
        if path in keep:
            report.kept_segments += 1
            flog(f"{prefix}replay_outbox: segment {seq} kept: part of it could "
                 f"not be quarantined, its valid rows are integrated", "ERROR")
            continue
        try:
            os.remove(path)
        except OSError as exc:
            report.errors.append(f"remove {os.path.basename(path)}: {exc}")

    # A batch whose "end" line never made it to disk was being written when
    # the process died. Its complete lines are real edits and are kept; the
    # commit they belong to is recorded INCOMPLETE so nothing downstream
    # takes it for a whole commit (a prepared commit is reported as
    # INTERRUPTED below, with its pre-edit state).
    seen_batches: Dict[str, dict] = {}
    for _seq, _path, lines in parsed:
        for line in lines:
            b = line.get("b")
            if not b:
                continue
            info = seen_batches.setdefault(b, {"events": 0, "end": None, "commits": set()})
            if line.get("t") == "ev" and isinstance(line.get("e"), dict):
                info["events"] += 1
                if line["e"].get("commit_id"):
                    info["commits"].add(line["e"]["commit_id"])
            elif line.get("t") == "end":
                info["end"] = line.get("n")
    for b, info in seen_batches.items():
        if info["end"] is None and info["events"]:
            report.incomplete_batches.append({"batch": b, "events": info["events"],
                                              "commits": sorted(info["commits"])})
    if report.incomplete_batches:
        try:
            with conn:
                for inc in report.incomplete_batches:
                    for cid in inc["commits"]:
                        if cid in prepares:
                            continue
                        record_capture_commit(
                            conn, cid, "INCOMPLETE", finalized_at=None,
                            n_events=inc["events"],
                            detail=(f"torn write: {inc['events']} event(s) of the "
                                    f"batch recovered, its end marker is missing; "
                                    f"the commit may lack events"))
        except sqlite3.Error as exc:
            report.errors.append(f"incomplete batches: {exc}")

    interrupted = [cid for cid in prepares
                   if cid not in finalized and not _commit_recorded(conn, cid)]
    if interrupted:
        try:
            with conn:
                for cid in interrupted:
                    prep = prepares[cid]
                    record_capture_commit(
                        conn, cid, "INTERRUPTED",
                        layer_id=prep.get("layer_id"),
                        layer_name=prep.get("layer_name"),
                        datasource_fingerprint=prep.get("ds"),
                        session_id=prep.get("session_id"),
                        prepared_at=prep.get("at"),
                        finalized_at=None,
                        n_snapshots=len(prep.get("snapshots") or []),
                        n_events=0,
                        detail=("QGIS stopped between the pre-commit capture "
                                "and the end of the provider commit: the "
                                "pre-edit state of these features is kept, "
                                "whether the provider applied the edit is "
                                "unknown"),
                        snapshots_json=json.dumps(
                            _preserved_state(prep), ensure_ascii=True))
                    report.interrupted_commits.append({
                        "commit_id": cid,
                        "layer_name": prep.get("layer_name"),
                        "n_snapshots": len(prep.get("snapshots") or []),
                        "prepared_at": prep.get("at"),
                    })
        except sqlite3.Error as exc:
            report.errors.append(f"interrupted commits: {exc}")
            flog(f"{prefix}replay_outbox: cannot record interrupted commits: "
                 f"{exc}", "ERROR")
    report.finalized_commits = len(finalized)
    try:
        if not list_segments(outbox_dir):
            leftovers = [n for n in os.listdir(outbox_dir) if n != QUARANTINE_DIRNAME]
            if not leftovers and not os.path.isdir(
                    os.path.join(outbox_dir, QUARANTINE_DIRNAME)):
                shutil.rmtree(outbox_dir, ignore_errors=True)
    except OSError:
        pass
    level = "WARNING" if (report.events_inserted or report.interrupted_commits
                          or report.torn_lines or report.errors) else "INFO"
    try:
        from .logger import jlog
        jlog("outbox_replay", level, component="capture_outbox",
             trace_id=trace_id, journal=journal_path, **report.as_dict())
    except Exception:  # noqa: BLE001 # nosec B110 - forensic log never breaks replay
        pass
    flog(f"{prefix}replay_outbox: event=outbox_replayed journal={journal_path} "
         f"report={json.dumps(report.as_dict(), ensure_ascii=False)}", level)
    return report
