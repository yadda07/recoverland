"""Durability probes shared by the validation scenarios.

"Durable" means one thing for RecoverLand: the event is found in the
journal after the plugin starts again. Where it waited in between (the
outbox of its journal, the legacy pending file) is an implementation
detail. Scenarios written against one storage location break the day the
storage changes, while the property they meant to prove does not; these
probes measure the property.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Optional


def count_rows(db_path: str, where: str = "", params: tuple = ()) -> int:
    """Rows of audit_event matching *where* (-1 when the journal is unreadable)."""
    try:
        conn = sqlite3.connect(db_path)
        try:
            sql = "SELECT COUNT(*) FROM audit_event"
            if where:
                sql += " WHERE " + where
            return int(conn.execute(sql, params).fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def duplicate_uids(db_path: str) -> int:
    """Number of event_uid values present more than once (-1: no v7 column)."""
    try:
        conn = sqlite3.connect(db_path)
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_event)")}
            if "event_uid" not in cols:
                return -1
            return int(conn.execute(
                "SELECT COUNT(*) FROM (SELECT event_uid FROM audit_event "
                "WHERE event_uid IS NOT NULL GROUP BY event_uid "
                "HAVING COUNT(*) > 1)").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def outbox_events(db_path: str, session: Optional[str] = None) -> int:
    """Events waiting in the journal's outbox (not yet replayed)."""
    from recoverland.core.capture_outbox import list_segments, outbox_dir_for
    total = 0
    for _seq, path in list_segments(outbox_dir_for(db_path)):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    try:
                        line = json.loads(raw)
                    except ValueError:
                        continue
                    if line.get("t") != "ev" or not isinstance(line.get("e"), dict):
                        continue
                    if session is None or line["e"].get("session_id") == session:
                        total += 1
        except OSError:
            continue
    return total


def pending_events(db_path: str) -> int:
    """Entries of the legacy pending file next to the journal (-1: unreadable)."""
    path = os.path.join(os.path.dirname(db_path), "recoverland_pending.json")
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return -1
    return len(data) if isinstance(data, list) else -1


def restart_integrate(db_path: str) -> dict:
    """Do what the plugin does at start: pending import, then outbox replay.

    Returns what each step reported. Must not run while another live
    WriteQueue writes the same journal.
    """
    from recoverland.core.integrity import check_journal_integrity
    from recoverland.core.write_queue import WriteQueue
    integrity = check_journal_integrity(db_path)
    wq = WriteQueue()
    wq.start(db_path)
    report = getattr(wq, "last_replay_report", None)
    wq.stop()
    return {
        "integrity_issues": list(integrity.issues),
        "pending_recovered": integrity.recovered_events,
        "pending_rejected": integrity.pending_rejected,
        "replay": report.as_dict() if report is not None else None,
    }
