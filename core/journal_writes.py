"""Idempotent journal writes and the commit ledger (schema v7).

Two identities the journal never had:

* ``event_uid`` -- one per captured event, assigned at capture. An event
  can reach the journal by two roads (the writer thread, or a replay of
  the outbox after a crash); the UNIQUE index on ``event_uid`` makes the
  second arrival a no-op instead of a duplicate that a later rewind would
  apply twice.
* ``commit_id`` -- one per QGIS commit attempt. ``created_at`` was used as
  the unit of a commit; it is a clock reading, not an identity: two
  layers committed in the same microsecond share it, and a purge that
  cuts "on the timestamp" cannot tell them apart.

``capture_commit`` is the ledger of those commits: how each one ended
(COMMITTED, PARTIAL, NO_WRITE, ROLLED_BACK, INTERRUPTED,
GENERATION_FAILED). It is what a replay consults to know whether a
pre-commit capture was resolved, and what the user reads to know what a
crash left behind.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable, List, Optional, Tuple

from .sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
from .logger import flog

EVENT_EXT_COLUMNS = ("event_uid", "commit_id")
EVENT_WRITE_COLUMNS = tuple(AUDIT_EVENT_INSERT_COLUMNS) + EVENT_EXT_COLUMNS

_VALID_OPS = frozenset({"INSERT", "UPDATE", "DELETE"})

COMMIT_STATES = (
    "COMMITTED", "PARTIAL", "NO_WRITE", "ROLLED_BACK", "INTERRUPTED",
    "GENERATION_FAILED", "RECONCILED", "INCOMPLETE",
)

# NOT NULL columns of audit_event: a replayed payload that lost one of
# them still produces a valid row instead of failing the whole batch.
_ROW_DEFAULTS = {
    "project_fingerprint": "",
    "datasource_fingerprint": "",
    "provider_type": "",
    "attributes_json": "{}",
    "geometry_type": "NoGeometry",
    "user_name": "unknown",
}

_INSERT_SQL = "".join((
    "INSERT INTO audit_event (",  # nosec B608: column list is a module constant
    ", ".join(EVENT_WRITE_COLUMNS),
    ") VALUES (",
    ",".join(["?"] * len(EVENT_WRITE_COLUMNS)),
    ") ON CONFLICT(event_uid) DO NOTHING",
))

V7_DDL = (
    """CREATE TABLE IF NOT EXISTS capture_commit (
        commit_id TEXT PRIMARY KEY,
        layer_id TEXT,
        layer_name TEXT,
        datasource_fingerprint TEXT,
        session_id TEXT,
        state TEXT NOT NULL,
        prepared_at TEXT,
        finalized_at TEXT,
        n_snapshots INTEGER,
        n_events INTEGER,
        detail TEXT,
        snapshots_json TEXT,
        recorded_at TEXT NOT NULL
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_event_uid ON audit_event(event_uid)",
    "CREATE INDEX IF NOT EXISTS idx_event_commit ON audit_event(commit_id)",
    "CREATE INDEX IF NOT EXISTS idx_capture_commit_state ON capture_commit(state)",
    # Newest date a purge removed, per datasource fingerprint. It used to
    # live in process memory only: after a restart nothing said that a
    # date had been covered once, and a rewind to it replayed what
    # survived the purge and announced a complete restore.
    """CREATE TABLE IF NOT EXISTS journal_coverage (
        datasource_fingerprint TEXT PRIMARY KEY,
        purged_through TEXT NOT NULL,
        purged_events INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT NOT NULL
    )""",
)


def has_v7_columns(conn: sqlite3.Connection) -> bool:
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(audit_event)")}
    except sqlite3.Error:
        return False
    return all(c in cols for c in EVENT_EXT_COLUMNS)


def ensure_v7_schema(conn: sqlite3.Connection) -> None:
    """Add event_uid / commit_id and the commit ledger. Idempotent.

    Runs inside the caller's transaction when there is one. Old rows keep
    NULL in both columns: NULLs are distinct in a SQLite UNIQUE index, so
    history written before v7 is untouched. The transaction that creates
    journal_coverage also stamps the instant this journal starts to
    record every purge (see coverage_origin).
    """
    cols = {row[1] for row in conn.execute("PRAGMA table_info(audit_event)")}
    for col in EVENT_EXT_COLUMNS:
        if col not in cols:
            conn.execute(f"ALTER TABLE audit_event ADD COLUMN {col} TEXT")  # nosec B608
    recording = _has_table(conn, "journal_coverage")
    for ddl in V7_DDL:
        conn.execute(ddl)
    if not recording:
        _stamp_coverage_start(conn)


# ----------------------------------------------------------------------
# Coverage stamp: since when the journal records every purge
# ----------------------------------------------------------------------
# A purge by a release older than v7 left no trace in the journal; since
# v7 every purge does (journal_coverage, journal_coverage_gap). Whether a
# journal lived under an older release, the floor readers (the rewind:
# retention._journal_predates_v7; Review:
# snapshot_rebuild_worker.recording_journal_creation) DEDUCE from the
# schema_version clock stamps (versions below 7 written within a minute
# of version 7: created by v7), which a clock that was wrong when an
# older release created the file fools. These keys record it, in
# backend_settings; they protect only a reader that consults
# coverage_origin before its inference:
#   * COVERAGE_SINCE_KEY: when journal_coverage was created in this file,
#     in the same transaction: from then on, every purge by this release
#     leaves a record here.
#   * COVERAGE_ORIGIN_KEY: "created" (no journal history before
#     COVERAGE_SINCE_KEY: no history can have been purged without a
#     record) or "migrated" (the file held events under a schema below 7:
#     the history before COVERAGE_SINCE_KEY may have lost rows without a
#     trace). "created" is written with COVERAGE_SINCE_KEY, in the same
#     transaction, when audit_event has never held a row (a new file, or
#     a legacy journal nothing was ever journaled in: see
#     _never_held_an_event); whoever initializes the schema gets it, a
#     kill cannot separate the two. "migrated" is written by the opener
#     that saw the file before initialize_schema
#     (journal_manager._initialize_journal), right after it, with
#     COVERAGE_ORIGIN_SCHEMA_KEY (the schema it saw). A file that held
#     events and whose opener did not say (killed between the two
#     transactions, or initialized by a bare initialize_schema) gets
#     COVERAGE_SINCE_KEY without an origin: unknown, read as "migrated".
# No stamp (a journal made v7 before these keys existed): the readers
# keep their inference.

COVERAGE_SINCE_KEY = "coverage_recorded_since"
COVERAGE_ORIGIN_KEY = "coverage_origin"
COVERAGE_ORIGIN_SCHEMA_KEY = "coverage_origin_schema"
ORIGIN_CREATED = "created"
ORIGIN_MIGRATED = "migrated"
# The first schema whose purges leave a durable record.
_RECORDING_SCHEMA = 7


def _has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,)).fetchone() is not None


def _never_held_an_event(conn: sqlite3.Connection) -> bool:
    """True when audit_event has never held a row in this file.

    audit_event is AUTOINCREMENT in every schema ever shipped: SQLite keeps
    the largest event_id it ever handed out in sqlite_sequence, and a
    DELETE (every purge), a VACUUM or a migration leaves that row in place
    (no release ever rebuilt the table). An empty table with no such row:
    nothing was ever journaled here, so nothing can have been purged
    without a record. An empty table WITH one was emptied by purges, which
    is not a fresh journal. Unreadable: False (the caller does not claim
    anything).
    """
    try:
        if conn.execute("SELECT 1 FROM audit_event LIMIT 1").fetchone() is not None:
            return False
        row = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = 'audit_event'").fetchone()
    except sqlite3.Error:
        return False
    return row is None or not row[0]


def _stamp_coverage_start(conn: sqlite3.Connection) -> None:
    """Record that purges are recorded from now on. In the caller's tx.

    Replaces an older stamp: a journal_coverage table that has to be
    created again took the records of the earlier purges with it, so
    their history is vouched for only from now on, and what the file was
    before is no longer known (the origin is dropped, the opener sets it
    again only when it saw the file before this schema). A file whose
    audit_event never held a row is "created" here, in this transaction:
    no opener is needed for it and no kill can leave it without origin.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES (?, ?, ?)",
            (COVERAGE_SINCE_KEY, now, now))
        conn.execute(
            "DELETE FROM backend_settings WHERE setting_key IN (?, ?)",
            (COVERAGE_ORIGIN_KEY, COVERAGE_ORIGIN_SCHEMA_KEY))
        if _never_held_an_event(conn):
            conn.execute(
                "INSERT INTO backend_settings (setting_key, setting_value, "
                "updated_at) VALUES (?, ?, ?)",
                (COVERAGE_ORIGIN_KEY, ORIGIN_CREATED, now))
    except sqlite3.OperationalError as exc:
        # No backend_settings: not a whole journal. The readers keep their
        # inference for it.
        flog(f"journal_writes: coverage start not stamped ({exc})", "WARNING")


def schema_before_init(conn: sqlite3.Connection) -> Optional[int]:
    """Schema version of the file before initialize_schema runs on it.

    0 for a file that holds no journal yet (no audit_event, or one left
    empty by an interrupted first initialization, with no version row);
    the highest schema_version otherwise. None when the file cannot tell:
    unreadable, or events without any version row.
    """
    try:
        if not _has_table(conn, "audit_event"):
            return 0
        version = None
        if _has_table(conn, "schema_version"):
            row = conn.execute("SELECT MAX(version_number) FROM schema_version").fetchone()
            version = row[0] if row else None
        if version:
            return int(version)
        empty = conn.execute("SELECT 1 FROM audit_event LIMIT 1").fetchone() is None
        return 0 if empty else None
    except (sqlite3.Error, TypeError, ValueError):
        return None


def record_coverage_origin(conn: sqlite3.Connection,
                           schema_before: Optional[int]) -> Optional[str]:
    """Say what the journal was before it started recording purges.

    *schema_before* is schema_before_init() read on the same connection
    before initialize_schema. Only a file that initialize_schema just
    brought to the recording schema is stamped, with the schema it had.
    "created" is never the opener's claim: the schema transaction wrote
    it from the file itself when audit_event never held a row (a new
    file, or a legacy journal nothing was journaled in). The opener adds
    "migrated" for a file below the recording schema, which stays when
    that transaction wrote nothing; a file it saw with no schema but whose
    audit_event held rows keeps no origin (unknown, read as migrated). A
    journal already at the recording schema keeps what it has (a stamp
    written at its creation, or none: the readers' inference). Its own
    transaction; never raises (the open must not fail for it). Returns
    the origin the journal now holds, "" for none, or None when nothing
    was stamped.
    """
    if schema_before is None or schema_before >= _RECORDING_SCHEMA:
        return None
    writes = [(COVERAGE_ORIGIN_SCHEMA_KEY, str(schema_before))]
    if schema_before > 0:
        writes.insert(0, (COVERAGE_ORIGIN_KEY, ORIGIN_MIGRATED))
    try:
        from .sqlite_schema import get_schema_version
        if get_schema_version(conn) < _RECORDING_SCHEMA:
            return None  # read-only: initialize_schema deferred the migration
        with conn:
            row = conn.execute(
                "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
                (COVERAGE_SINCE_KEY,)).fetchone()
            if not row or not row[0]:
                return None
            for key, value in writes:
                conn.execute(
                    "INSERT OR IGNORE INTO backend_settings (setting_key, "
                    "setting_value, updated_at) VALUES (?, ?, ?)",
                    (key, value, row[0]))
            held = conn.execute(
                "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
                (COVERAGE_ORIGIN_KEY,)).fetchone()
    except sqlite3.Error as exc:
        flog(f"journal_writes: coverage origin not recorded ({exc}); Review "
             f"and the rewind keep their inference for this journal", "WARNING")
        return None
    origin = (held[0] if held else "") or ""
    if schema_before == 0 and origin != ORIGIN_CREATED:
        flog(f"journal_writes: coverage origin unknown for a file with no "
             f"schema whose audit_event already held rows (since={row[0]}): "
             f"read as migrated, its history before that is not vouched for",
             "WARNING")
    else:
        flog(f"journal_writes: coverage origin={origin} schema_before="
             f"{schema_before} since={row[0]}")
    return origin


def coverage_origin(conn: sqlite3.Connection) -> Optional[dict]:
    """Since when this journal records every purge, and what it was before.

    ``{"since": ISO UTC, "origin": "created" | "migrated" | "",
    "schema_before": int | None}``. For the readers of the floor, to be
    consulted BEFORE their schema_version inference (the stamp protects
    nothing a reader does not consult):
      * "created": no journal history before ``since`` (a new file, or a
        legacy journal whose audit_event never held a row), nothing was
        ever purged without a record: without a purge record a layer has
        no floor,
        except rows older than ``since`` (brought by a history carry or
        an import), whose own history is not vouched for below them.
        retention._journal_predates_v7: False;
        snapshot_rebuild_worker.recording_journal_creation: ``since``;
      * "migrated", or "" (the opener could not say: treat it the same):
        the journal lived under a release that purged without a record;
        the legacy guard applies (nothing below the oldest surviving row
        of a layer is vouched for). _journal_predates_v7: True;
        recording_journal_creation: None. ``since`` is a local clock
        reading: it is not a floor on its own.
    None when the journal holds no stamp (made v7 before the stamp
    existed) or it cannot be read: the caller keeps its inference.
    tx_journal_coverage_origin_stamp checks both readers on these rules.
    """
    try:
        rows = dict(conn.execute(
            "SELECT setting_key, setting_value FROM backend_settings "
            "WHERE setting_key IN (?, ?, ?)",
            (COVERAGE_SINCE_KEY, COVERAGE_ORIGIN_KEY, COVERAGE_ORIGIN_SCHEMA_KEY)))
    except sqlite3.Error:
        return None
    since = rows.get(COVERAGE_SINCE_KEY)
    if not since:
        return None
    origin = rows.get(COVERAGE_ORIGIN_KEY) or ""
    if origin not in (ORIGIN_CREATED, ORIGIN_MIGRATED):
        origin = ""
    try:
        before = int(rows[COVERAGE_ORIGIN_SCHEMA_KEY])
    except (KeyError, TypeError, ValueError):
        before = None
    return {"since": since, "origin": origin, "schema_before": before}


def validate_event_row(row: dict) -> str:
    """'' when *row* can be written, else why not."""
    if not isinstance(row, dict):
        return "not a mapping"
    if row.get("operation_type") not in _VALID_OPS:
        return f"invalid operation_type {row.get('operation_type')!r}"
    if not row.get("created_at"):
        return "empty created_at"
    attrs = row.get("attributes_json")
    if not isinstance(attrs, str) or not attrs:
        return "empty or non-string attributes_json"
    return ""


def _row_tuple(row: dict) -> tuple:
    return tuple(
        row.get(col) if row.get(col) is not None else _ROW_DEFAULTS.get(col)
        for col in EVENT_WRITE_COLUMNS)


def insert_events_idempotent(
    conn: sqlite3.Connection, rows: Iterable[dict],
) -> Tuple[int, int, List[Tuple[dict, str]]]:
    """Insert rows keyed by event_uid; returns (inserted, already, rejected).

    ``rejected`` pairs each unusable row with its reason. The caller owns
    them: they are real edits with a defect, never to be dropped silently.
    Runs in the caller's transaction.
    """
    good, rejected = [], []
    for row in rows:
        reason = validate_event_row(row)
        if reason:
            rejected.append((row, reason))
        else:
            good.append(_row_tuple(row))
    if not good:
        return 0, 0, rejected
    before = conn.total_changes
    conn.executemany(_INSERT_SQL, good)
    inserted = conn.total_changes - before
    return inserted, len(good) - inserted, rejected


def event_row(event) -> dict:
    """AuditEvent -> dict keyed by column name (bytes kept as bytes)."""
    data = event._asdict() if hasattr(event, "_asdict") else dict(event)
    data.pop("event_id", None)
    return data


def record_capture_commit(conn: sqlite3.Connection, commit_id: str, state: str,
                          layer_id: Optional[str] = None,
                          layer_name: Optional[str] = None,
                          datasource_fingerprint: Optional[str] = None,
                          session_id: Optional[str] = None,
                          prepared_at: Optional[str] = None,
                          finalized_at: Optional[str] = None,
                          n_snapshots: Optional[int] = None,
                          n_events: Optional[int] = None,
                          detail: Optional[str] = None,
                          snapshots_json: Optional[str] = None) -> None:
    """Upsert one row of the commit ledger. Runs in the caller's transaction."""
    if not commit_id:
        return
    if state not in COMMIT_STATES:
        flog(f"journal_writes: unknown commit state {state!r} for "
             f"{commit_id}, recorded as is", "WARNING")
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO capture_commit (commit_id, layer_id, layer_name, "
        "datasource_fingerprint, session_id, state, prepared_at, finalized_at, "
        "n_snapshots, n_events, detail, snapshots_json, recorded_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(commit_id) DO UPDATE SET "
        "state = excluded.state, "
        "layer_id = COALESCE(excluded.layer_id, capture_commit.layer_id), "
        "layer_name = COALESCE(excluded.layer_name, capture_commit.layer_name), "
        "datasource_fingerprint = COALESCE(excluded.datasource_fingerprint, "
        "capture_commit.datasource_fingerprint), "
        "session_id = COALESCE(excluded.session_id, capture_commit.session_id), "
        "prepared_at = COALESCE(excluded.prepared_at, capture_commit.prepared_at), "
        "finalized_at = COALESCE(excluded.finalized_at, capture_commit.finalized_at), "
        "n_snapshots = COALESCE(excluded.n_snapshots, capture_commit.n_snapshots), "
        "n_events = COALESCE(excluded.n_events, capture_commit.n_events), "
        "detail = COALESCE(excluded.detail, capture_commit.detail), "
        "snapshots_json = COALESCE(excluded.snapshots_json, capture_commit.snapshots_json), "
        "recorded_at = excluded.recorded_at",
        (commit_id, layer_id, layer_name, datasource_fingerprint, session_id,
         state, prepared_at, finalized_at, n_snapshots, n_events, detail,
         snapshots_json, now))
