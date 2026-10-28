# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Snapshot rebuild worker - per-date SQL query without full cache pre-load.

Architecture
------------
One SQL per date change (debounced by CanvasDateBar at 800 ms).
Query returns the LAST state per entity_fp at or before cutoff_dt.
O(N entities) rows fetched, never O(N events) → low memory footprint.

SQL strategy
------------
CTE ``latest_ts`` isolates MAX(created_at) per entity_fp, then an inner
join fetches the full row.  With an index on
(datasource_fingerprint, entity_fingerprint, created_at) the query is O(log N).
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from datetime import datetime
from functools import partial
from typing import List

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog
from ..core.search_service import _row_to_event
from ..core.time_format import journal_today
from ..core.sqlite_schema import AUDIT_EVENT_COLUMNS

_ALIASED_COLS = ", ".join(f"ae.{c}" for c in AUDIT_EVENT_COLUMNS)


def _self_describing(column: str) -> str:
    """Match the fingerprints of the SAME file opened with display options.

    A file fingerprint carries its own source: ``ogr::<path>|layername=x``.
    Everything a pre-v6 capture appended after it -- ``|subset=``,
    ``|geometrytype=`` -- is a display option, so a row whose fingerprint
    is the asked one PLUS such tokens describes the same table and the
    same features. Recognising it needs no table and no registry, which
    is what REVIEW needs: it rebuilds a past state on a read-only
    connection, on journals that may never have been migrated.

    Two guards keep it exact:
      * the extra part must start at a ``|`` boundary (`substr`, not
        `LIKE`: a path routinely contains ``_``, a LIKE wildcard, and
        would then match neighbouring paths);
      * it must carry no ``layername=`` / ``layerid=`` of its own, or
        ``file.gpkg`` would swallow every layer of that GeoPackage.
    """
    rest = "".join(("substr(", column, ", length(?1) + 1)"))
    return "".join((
        "(substr(", column, ", 1, length(?1) + 1) = ?1 || '|'",
        " AND instr(", rest, ", '|layername=') = 0",
        " AND instr(", rest, ", '|layerid=') = 0)",
    ))


def _scoped(column: str) -> str:
    """Predicate matching a datasource fingerprint AND its v6 aliases.

    REVIEW rebuilds a past state from its own SQL, outside the
    `event_stream_repository` expansion point. Filtering on the bare
    equality made it silently ignore everything captured under an
    obsolete fingerprint: the user was shown a state of the world that
    never existed, with nothing saying it was incomplete.

    Three ways of naming one source, all resolved here: the fingerprint
    itself, the aliases the v6 reconciliation attached to it (the only
    way a DB source can be resolved), and the self-describing forms
    above (which still work on a journal nobody could migrate).

    The fingerprint is bound ONCE (`?1`) so the parameter tuples of the
    callers stay exactly what they were.
    """
    return "".join((
        "(", column, " = ?1 OR ", column, " IN ("
        "SELECT alias_fingerprint FROM datasource_alias "
        "WHERE target_fingerprint = ?1) OR ",
        _self_describing(column), ")",
    ))


def _plain(column: str) -> str:
    """Same predicate for a journal with no `datasource_alias` table."""
    return "".join((
        "(", column, " = ?1 OR ", _self_describing(column), ")",
    ))


# An invalidated TRACE is a compensation an Undo reverted: it was in effect
# from its created_at until its invalidated_at, and the Undo itself writes
# no event. Review reads it, like the Rewind does
# (rewind_dedup.traces_in_effect); dropping it showed every date between a
# compensation and its Undo without the compensation. An invalidated row
# that is not a trace has no such meaning and stays out.
_READABLE = ("(ae.invalidated_at IS NULL"
             " OR ae.restored_from_event_id IS NOT NULL)")


def _build_queries(scope) -> tuple:
    all_events_before = "".join([
        "SELECT ", _ALIASED_COLS,
        " FROM audit_event ae",
        " WHERE ", scope("ae.datasource_fingerprint"),
        " AND ae.created_at <= ?2",
        " AND ", _READABLE,
        " ORDER BY ae.entity_fingerprint, ae.created_at, ae.event_id",
    ])
    # The events AFTER the cutoff are what decides the state at T of every
    # feature that predates the journal: it has no event <= T, and the
    # before-image of its first later event IS its state at T. A trace
    # undone after T is a change after T too (its Undo), even when the
    # trace itself is older: it is fetched here, and the entity is one
    # whose state at T differs from today.
    all_events_after = "".join([
        "SELECT ", _ALIASED_COLS,
        " FROM audit_event ae",
        " WHERE ", scope("ae.datasource_fingerprint"),
        " AND (ae.created_at > ?2 OR (ae.restored_from_event_id IS NOT NULL",
        " AND ae.invalidated_at > ?2))",
        " AND ", _READABLE,
        " ORDER BY ae.entity_fingerprint, ae.created_at, ae.event_id",
    ])
    # The span of the instants at which the state changes: what Review
    # reads (_READABLE), and the Undo of a trace, dated at its
    # invalidated_at. It bounds the marker scan: without the Undos, an Undo
    # made after the last event fell outside it and left no mark.
    date_range = "".join([
        "SELECT MIN(ae.created_at), MAX(MAX(ae.created_at), COALESCE(MAX(",
        "CASE WHEN ae.restored_from_event_id IS NOT NULL",
        " THEN ae.invalidated_at END), ''))",
        " FROM audit_event ae",
        " WHERE ", scope("ae.datasource_fingerprint"),
        " AND ", _READABLE,
    ])
    # CHANGE B: entity_fingerprints with at least one event strictly AFTER
    # the cutoff. These are the ONLY entities whose state at T differs from
    # the current / live data, so Review reconstructs and shows only these
    # (no duplication of the unchanged source layers). feature_identity_json
    # is fetched for the NULL-fingerprint fallback so keys match the
    # reconstruction exactly (see temporal_snapshot_engine.compute_entity_key).
    fps_changed_after = "".join([
        "SELECT DISTINCT entity_fingerprint, feature_identity_json",
        " FROM audit_event ae",
        " WHERE ", scope("ae.datasource_fingerprint"),
        " AND (ae.created_at > ?2 OR (ae.restored_from_event_id IS NOT NULL",
        " AND ae.invalidated_at > ?2))",
        " AND ", _READABLE,
    ])
    return all_events_before, date_range, fps_changed_after, all_events_after


(_SQL_ALL_EVENTS_BEFORE, _SQL_DATE_RANGE,
 _SQL_FPS_CHANGED_AFTER, _SQL_ALL_EVENTS_AFTER) = _build_queries(_scoped)
(_SQL_ALL_EVENTS_BEFORE_NO_ALIAS, _SQL_DATE_RANGE_NO_ALIAS,
 _SQL_FPS_CHANGED_AFTER_NO_ALIAS,
 _SQL_ALL_EVENTS_AFTER_NO_ALIAS) = _build_queries(_plain)

# Oldest surviving row of a source, whatever it is (user event or trace,
# invalidated or not): the guard of a journal that cannot say whether it
# was purged (query_snapshot_floor), the one the Rewind uses as well
# (retention.check_journal_coverage).
_SQL_OLDEST_ROW = "".join((
    "SELECT MIN(created_at) FROM audit_event WHERE ",
    _scoped("datasource_fingerprint")))
_SQL_OLDEST_ROW_NO_ALIAS = "".join((
    "SELECT MIN(created_at) FROM audit_event WHERE ",
    _plain("datasource_fingerprint")))


def _build_repack_queries(scope) -> tuple:
    """Instants at which the records of a source were renumbered.

    On a source whose fid is the record position (Shapefile, GeoJSON) a
    deletion renumbers every record after the deleted one at the repack
    that ends its commit. Two rows say when: a DELETE (user event or
    rewind trace, at its created_at: one instant per commit) and a
    re-insertion by a rewind (INSERT trace) that an Undo reverted, i.e.
    deleted, at its invalidated_at. A DELETE counts even when invalidated:
    the Undo that brought the record back appended it, it did not restore
    the old numbering. The last instant at or before the cutoff and every
    one after it are all the reconstruction needs
    (temporal_snapshot_engine, module step 8).
    """
    source = scope("ae.datasource_fingerprint")
    deletes = "".join((
        "SELECT ae.created_at AS b FROM audit_event ae WHERE ", source,
        " AND ae.operation_type = 'DELETE' AND ae.created_at "))
    undone = "".join((
        "SELECT ae.invalidated_at AS b FROM audit_event ae WHERE ", source,
        " AND ae.operation_type = 'INSERT'",
        " AND ae.restored_from_event_id IS NOT NULL AND ae.invalidated_at "))
    last_at_or_before = "".join((
        "SELECT MAX(b) FROM (", deletes, "<= ?2 UNION ALL ", undone, "<= ?2)"))
    after = "".join((
        "SELECT DISTINCT b FROM (", deletes, "> ?2 UNION ALL ", undone,
        "> ?2) ORDER BY b"))
    return last_at_or_before, after


_SQL_REPACK_AT_OR_BEFORE, _SQL_REPACK_AFTER = _build_repack_queries(_scoped)
(_SQL_REPACK_AT_OR_BEFORE_NO_ALIAS,
 _SQL_REPACK_AFTER_NO_ALIAS) = _build_repack_queries(_plain)

_NO_ALIAS_FALLBACK = {
    _SQL_ALL_EVENTS_BEFORE: _SQL_ALL_EVENTS_BEFORE_NO_ALIAS,
    _SQL_DATE_RANGE: _SQL_DATE_RANGE_NO_ALIAS,
    _SQL_FPS_CHANGED_AFTER: _SQL_FPS_CHANGED_AFTER_NO_ALIAS,
    _SQL_ALL_EVENTS_AFTER: _SQL_ALL_EVENTS_AFTER_NO_ALIAS,
    _SQL_OLDEST_ROW: _SQL_OLDEST_ROW_NO_ALIAS,
    _SQL_REPACK_AT_OR_BEFORE: _SQL_REPACK_AT_OR_BEFORE_NO_ALIAS,
    _SQL_REPACK_AFTER: _SQL_REPACK_AFTER_NO_ALIAS,
}

# Rows streamed between two cancellation checks. A superseded worker
# must let go quickly: the dialog joins it when Review stops.
_CANCEL_CHECK_ROWS = 512


# ------------------------------------------------------------------ #
# Lifetime of the Review threads                                       #
# ------------------------------------------------------------------ #
#
# A QThread must outlive its run(). Two ways Review broke that:
#   * the rebuild worker was a child of the dialog, and the dialog is
#     deleted on unload while the worker can still be streaming rows:
#     ~QThread on a running thread is qFatal, QGIS dies with the user's
#     unsaved work;
#   * the marker worker had no parent and its last reference was
#     dropped by its own result slot, while run() was still closing its
#     connection: same qFatal, on an ordinary pan.
# Every Review thread is therefore unparented and held here from start
# until Qt reports it finished. Nothing else decides when it dies.
_RETAINED: dict = {}
_RETAINED_LOCK = threading.Lock()


def _release_retained(key: int) -> None:
    with _RETAINED_LOCK:
        _RETAINED.pop(key, None)


def retain_until_finished(thread) -> None:
    """Keep a strong reference on *thread* until its ``finished`` signal.

    The slot is a partial on a key, not a closure on the thread: the
    registry is the only owner, so the wrapper is released as soon as
    the thread is done.
    """
    key = id(thread)
    with _RETAINED_LOCK:
        if key in _RETAINED:
            return
        _RETAINED[key] = thread
    thread.finished.connect(partial(_release_retained, key))


def retained_threads() -> list:
    """Review threads still held (running, or finished but not yet reaped)."""
    with _RETAINED_LOCK:
        return list(_RETAINED.values())


def join_review_thread(thread, timeout_ms: int, label: str = "") -> bool:
    """Cancel *thread* and wait for it, bounded. True once it is finished.

    ``cancel()`` interrupts the SQLite statement in flight when the worker
    supports it, so the wait is normally a few milliseconds. A thread that
    outlives the bound stays retained (never destroyed while running) and
    the refusal is logged: the caller must not free what it uses.
    """
    try:
        cancel = getattr(thread, "cancel", None)
        if callable(cancel):
            cancel()
        if not thread.isRunning():
            return True
        done = bool(thread.wait(int(timeout_ms)))
    except RuntimeError as exc:
        # The C++ object is already gone: nothing is running any more.
        flog(f"review: join {label} skipped, thread already deleted ({exc!r})",
             "DEBUG")
        return True
    if not done:
        retain_until_finished(thread)
        flog(f"review: join {label} gave up after {timeout_ms} ms, thread "
             f"still running and retained until it finishes", "WARNING")
    return done


def _execute(conn, sql: str, params: tuple):
    """Run an alias-aware query, degrading on a pre-v4 journal.

    A journal written before the `datasource_alias` table existed has
    nothing to expand: serving the asked fingerprint alone is exactly
    right there, and raising would take REVIEW down on a journal that
    reads perfectly.
    """
    try:
        return conn.execute(sql, params)
    except sqlite3.OperationalError as exc:
        fallback = _NO_ALIAS_FALLBACK.get(sql)
        if fallback is None or "datasource_alias" not in str(exc):
            raise
        flog(f"snap_worker: no datasource_alias table ({exc}); reading the "
             f"asked fingerprint only", "WARNING")
        return conn.execute(fallback, params)


# RL-E1-02 (Option A): volume guard. The reconstruction needs the FULL event
# chain per entity (attrs deltas + geometry walk-back), so we keep the full
# replay but stream rows with a hard budget instead of an unbounded fetchall().
# Beyond the budget the snapshot is flagged ``partial`` (degraded, never silent),
# rather than risking an OOM/UI freeze on very large journals. Tunable.
_SNAPSHOT_ROW_BUDGET = 500000


def _with_fid_stability(info: dict) -> dict:
    """*info* with ``fid_stable``; asked of the layer when the caller did not.

    Runs on the caller's thread (the worker's constructor). A source whose
    layer is not in the project cannot say whether its fids move: it is
    read as a position (False), the conservative answer; the historical
    reading of one ``fid:N`` key as one record is exactly what drew one
    record's state with another's fields on a Shapefile.
    """
    if "fid_stable" in info:
        return info
    from qgis.core import QgsProject  # noqa: PLC0415
    from ..core.commit_guard import fid_is_stable  # noqa: PLC0415
    layer = QgsProject.instance().mapLayer(info.get("layer_id") or "")
    stable = bool(fid_is_stable(layer)) if layer is not None else False
    flog(f"snap_worker: fid_stable not given for layer="
         f"{info.get('layer_name')!r}: {stable} "
         f"({'asked of the layer' if layer is not None else 'layer not in the project'})",
         "INFO" if layer is not None else "WARNING")
    return dict(info, fid_stable=stable)


class SnapshotRebuildWorker(QThread):
    """Fetch the state of each entity at a given date in a background thread.

    ``layer_infos``: one dict per source, ``fingerprint``, ``layer_name``,
    ``layer_id``, ``storage_crs`` and ``fid_stable``, the answer of
    commit_guard.fid_is_stable for the layer (the Review dialog sets it).
    False: the fid is the record position (Shapefile, GeoJSON) and the
    reconstruction cuts its ``fid:`` keys at every renumbering
    (_repack_boundaries). A caller that does not say it gets it asked in
    the constructor, which runs on the caller's (GUI) thread, the only one
    that may touch the layer (_with_fid_stability).

    Signals
    -------
    result_ready : str, object
        ``(trace_id, SnapshotResult)``: emitted on success.
    error : str, str
        ``(trace_id, error_message)``: emitted on fatal error.
    refused : str, str
        ``(trace_id, reason)``: the journal cannot rebuild this date (it
        falls inside an interval a logical purge removed, see
        purged_interval_refusal); nothing was reconstructed.
    """

    result_ready = pyqtSignal(str, object)
    error = pyqtSignal(str, str)
    refused = pyqtSignal(str, str)

    def __init__(
        self,
        journal,
        layer_infos: List[dict],
        cutoff_iso: str,
        bbox_per_layer: dict = None,
        trace_id: str = "",
        row_budget: int = None,
        parent=None,
    ):
        super().__init__(parent)
        self._journal = journal
        self._layer_infos = [_with_fid_stability(info) for info in layer_infos]
        self._cutoff_iso = cutoff_iso
        self._bbox_per_layer = bbox_per_layer or {}
        self._cancelled = False
        self.trace_id = trace_id or uuid.uuid4().hex[:8]
        self._row_budget = row_budget if row_budget is not None else _SNAPSHOT_ROW_BUDGET
        # The read connection, reachable from the GUI thread so cancel()
        # can interrupt a statement in flight (sqlite3 allows it).
        self._conn = None
        self._conn_lock = threading.Lock()

    def cancel(self) -> None:
        """Stop the rebuild: flag it and interrupt the SQL in flight."""
        self._cancelled = True
        with self._conn_lock:
            conn = self._conn
            if conn is not None:
                try:
                    conn.interrupt()
                except Exception as exc:  # noqa: BLE001 - closing under us
                    flog(f"[{self.trace_id}] snap_worker: interrupt skipped "
                         f"({exc!r})", "DEBUG")

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def _stream(self, conn, sql: str, fp: str, keep, events: list,
                total: list) -> bool:
        """Append the rows of *sql* kept by *keep* to *events*.

        Returns False when the row budget is exceeded. ``total`` is a
        one-item counter shared by every call of one rebuild.
        """
        cursor = _execute(conn, sql, (fp, self._cutoff_iso))
        n_seen = 0
        for row in cursor:
            n_seen += 1
            if n_seen % _CANCEL_CHECK_ROWS == 0 and self._cancelled:
                return True
            event = _row_to_event(row)
            if keep is not None and not keep(event):
                continue
            events.append(event)
            total[0] += 1
            if total[0] > self._row_budget:
                return False
        return True

    def run(self) -> None:
        from ..core.temporal_snapshot_engine import (
            compute_entity_key,
            reconstruct_snapshot_at,
        )

        t0 = time.monotonic()
        tid = self.trace_id
        conn = None

        flog(
            f"[{tid}] snap_worker: start cutoff={self._cutoff_iso} "
            f"n_layers={len(self._layer_infos)}",
            "INFO",
        )

        try:
            conn = self._journal.create_read_connection()
            with self._conn_lock:
                self._conn = conn
            if self._cancelled:
                flog(f"[{tid}] snap_worker: cancelled", "INFO")
                return
            cutoff_dt = datetime.fromisoformat(
                self._cutoff_iso.replace("Z", "+00:00")
            )
            # ONE read transaction for every statement below. They used to
            # run in autocommit, one snapshot each: a commit landing between
            # the "what changed after T" read and the "events up to T" read
            # produced a state that never existed in the journal.
            conn.execute("BEGIN")

            # A date inside an interval a logical purge removed cannot be
            # rebuilt: refused by name, before anything is read, like the
            # Rewind refuses it (retention.coverage_refusal). Even a date
            # with no change after it: the purged pair may be the only one.
            refusal = purged_interval_refusal(conn, self._layer_infos,
                                              self._cutoff_iso)
            if refusal:
                flog(f"[{tid}] snap_worker: refused cutoff={self._cutoff_iso} "
                     f"reason={refusal}", "WARNING")
                if not self._cancelled:
                    self.refused.emit(tid, refusal)
                return

            # CHANGE B: Review = "what was different back then". Only entities
            # with >=1 event strictly AFTER the cutoff have a state at T that
            # differs from the live data; everything else is identical to the
            # source layers and must NOT be duplicated. The events after T
            # come FIRST: they give that set, and they also carry the state at
            # T of every feature that predates the journal (the before-image
            # of its first later event). An unchanged date (e.g. today)
            # short-circuits with zero reconstruction.
            changed_after: dict = {}
            after_by_fp: dict = {}
            n_changed_total = 0
            total_rows = [0]
            partial = False
            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] snap_worker: cancelled", "INFO")
                    return
                fp = info["fingerprint"]
                later: list = []
                if not self._stream(conn, _SQL_ALL_EVENTS_AFTER, fp, None,
                                    later, total_rows):
                    partial = True
                keys = {compute_entity_key(e.entity_fingerprint,
                                           e.feature_identity_json)
                        for e in later}
                changed_after[fp] = keys
                after_by_fp[fp] = later
                n_changed_total += len(keys)
                if partial:
                    break

            if n_changed_total == 0 and not partial:
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                flog(
                    f"[{tid}] snap_worker: no_changes_after_cutoff "
                    f"cutoff={self._cutoff_iso} n_entities=0 "
                    f"short_circuit=True elapsed_ms={elapsed_ms}",
                    "INFO",
                )
                empty = reconstruct_snapshot_at({}, cutoff_dt, trace_id=tid)
                if not self._cancelled:
                    self.result_ready.emit(tid, empty)
                return

            mini_cache: dict = {}
            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] snap_worker: cancelled", "INFO")
                    return
                if partial:
                    break

                fp = info["fingerprint"]
                keys = changed_after.get(fp, set())
                # Only the chains of the entities changed after T are
                # replayed: the others are dropped by the filter anyway.
                events: list = []
                if keys and not self._stream(
                        conn, _SQL_ALL_EVENTS_BEFORE, fp,
                        lambda e, _k=keys: compute_entity_key(
                            e.entity_fingerprint, e.feature_identity_json) in _k,
                        events, total_rows):
                    partial = True
                # A trace undone after T is read by both statements (made
                # before T, undone after it): one row, one event.
                seen = {e.event_id for e in events if e.event_id is not None}
                events.extend(e for e in after_by_fp.get(fp, [])
                              if e.event_id is None or e.event_id not in seen)
                mini_cache[fp] = events

                flog(
                    f"[{tid}] snap_worker: layer={info['layer_name']} "
                    f"n_events={len(events)} "
                    f"n_after={len(after_by_fp.get(fp, []))} "
                    f"cutoff={self._cutoff_iso}"
                    f"{' PARTIAL_BUDGET_HIT' if partial else ''}"
                    f" row_budget={self._row_budget}",
                    "WARNING" if partial else "INFO",
                )
            if self._cancelled:
                flog(f"[{tid}] snap_worker: cancelled", "INFO")
                return
            for fp, later in after_by_fp.items():
                if fp not in mini_cache and later:
                    mini_cache[fp] = list(later)
            repack = self._repack_boundaries(conn, tid)

            result = reconstruct_snapshot_at(
                mini_cache, cutoff_dt, trace_id=tid,
                should_cancel=lambda: self._cancelled,
                repack_boundaries=repack,
            )
            if self._cancelled:
                flog(f"[{tid}] snap_worker: cancelled post-reconstruct", "INFO")
                return

            result = self._filter_changed_after(result, changed_after, tid)

            if partial:
                result = result._replace(
                    partial=True,
                    partial_reason=f"row_budget_exceeded:{self._row_budget}",
                )
                flog(
                    f"[{tid}] snap_worker: PARTIAL snapshot "
                    f"total_rows>{self._row_budget} "
                    f"reason=row_budget_exceeded degraded=True",
                    "WARNING",
                )

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            flog(
                f"[{tid}] snap_worker: done "
                f"n_entities={result.n_entities} "
                f"n_changed_after={n_changed_total} "
                f"n_preexisting={result.n_preexisting} "
                f"n_born_after={result.n_born_after} "
                f"n_unresolved={result.n_unresolved} "
                f"n_trace_ref_missing={result.n_trace_ref_missing} "
                f"n_identity_unproven={result.n_identity_unproven} "
                f"total_rows={total_rows[0]} elapsed_ms={elapsed_ms}",
                "INFO",
            )
            if self._cancelled:
                flog(f"[{tid}] snap_worker: cancelled pre-emit", "INFO")
                return
            self.result_ready.emit(tid, result)

        except Exception as exc:  # noqa: BLE001
            if self._cancelled:
                # cancel() interrupts the statement in flight on purpose;
                # the caller has already moved on and expects nothing.
                flog(f"[{tid}] snap_worker: cancelled mid-query ({exc!r})",
                     "INFO")
                return
            flog(f"[{tid}] snap_worker: error={exc!r}", "ERROR")
            self.error.emit(tid, str(exc))

        finally:
            with self._conn_lock:
                self._conn = None
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"[{tid}] snap_worker: read connection close failed "
                         f"({exc!r})", "DEBUG")

    def _repack_boundaries(self, conn, tid: str) -> dict:
        """{fingerprint: [ISO instants]} of the sources whose fid moves.

        Only for the sources whose fid moves (``fid_stable`` False, see
        _with_fid_stability): there a ``fid:N`` key names whatever record
        held position N, and the reconstruction must not walk from one
        record to the next (temporal_snapshot_engine, module step 8). Read
        in the same transaction as the events.
        """
        out: dict = {}
        for info in self._layer_infos:
            if info.get("fid_stable"):
                continue
            fp = info["fingerprint"]
            params = (fp, self._cutoff_iso)
            last = _execute(conn, _SQL_REPACK_AT_OR_BEFORE, params).fetchone()
            instants = [row[0] for row in _execute(conn, _SQL_REPACK_AFTER, params)
                        if row[0]]
            if last and last[0]:
                instants.insert(0, last[0])
            out[fp] = instants
            flog(f"[{tid}] snap_worker: layer={info.get('layer_name')} fid is "
                 f"the record position (fid_stable=False): fid keys cut at "
                 f"{len(instants)} renumbering(s) "
                 f"(last_at_or_before_cutoff={last[0] if last else None})",
                 "INFO")
        return out

    @staticmethod
    def _filter_changed_after(result, changed_after: dict, tid: str):
        """Keep only entities modified/deleted AFTER the cutoff.

        Their state at T differs from the current/live data, so they are the
        only entities Review must paint. Entities present at T but unchanged
        since (no event after T) are identical to the source layers and are
        dropped to avoid duplicating the live data.
        """
        from ..core.temporal_snapshot_engine import recount

        n_before = result.n_entities
        filtered: dict = {}
        n_kept = 0
        for ds_fp, entity_map in result.features.items():
            keep_keys = changed_after.get(ds_fp, set())
            kept = {k: v for k, v in entity_map.items() if k in keep_keys}
            if kept:
                filtered[ds_fp] = kept
                n_kept += len(kept)
        flog(
            f"[{tid}] snap_worker: changed_after_filter "
            f"before={n_before} after={n_kept} dropped={n_before - n_kept}",
            "INFO",
        )
        return recount(result._replace(features=filtered, n_entities=n_kept))


_ENV_MEMO: dict = {}
_ENV_MEMO_MAX = 131072


def _cached_envelope(wkb: bytes):
    """Cached WKB envelope so repeated viewport pans avoid re-parsing."""
    from ..core.wkb_envelope import parse_envelope  # noqa: PLC0415
    key = id(wkb)
    hit = _ENV_MEMO.get(key)
    if hit is not None and hit[0] is wkb:
        return hit[1]
    env = parse_envelope(wkb)
    if len(_ENV_MEMO) >= _ENV_MEMO_MAX:
        _ENV_MEMO.clear()
    _ENV_MEMO[key] = (wkb, env)
    return env


def filter_snapshot_by_bbox(result, bbox_per_layer: dict):
    """Filter a SnapshotResult by bbox using the resolved geom_wkb per feature.

    bbox_per_layer: {datasource_fingerprint: QgsRectangle} in layer CRS.
    Returns a new SnapshotResult with updated features and n_entities.
    Uses pure-Python WKB parser (wkb_envelope) with per-blob memoization.
    """
    if not bbox_per_layer:
        return result

    from ..core.wkb_envelope import envelope_intersects  # noqa: PLC0415

    filtered: dict = {}
    n_kept = 0
    n_dropped = 0
    _diag_done = False

    for ds_fp, entity_map in result.features.items():
        bbox_rect = bbox_per_layer.get(ds_fp)
        if not _diag_done and entity_map:
            first_sf = next(iter(entity_map.values()))
            _gwkb = first_sf.geom_wkb
            _env = _cached_envelope(_gwkb) if _gwkb else None
            flog(
                f"filter_snapshot_diag: ds_fp={ds_fp[:8]} "
                f"bbox_found={bbox_rect is not None} "
                f"geom_wkb_len={len(_gwkb) if _gwkb else 0} "
                f"parsed_env={_env} last_op={first_sf.last_op}",
                "DEBUG",
            )
            _diag_done = True
        if bbox_rect is None:
            filtered[ds_fp] = entity_map
            n_kept += len(entity_map)
            continue
        bbox_tuple = (
            bbox_rect.xMinimum(), bbox_rect.yMinimum(),
            bbox_rect.xMaximum(), bbox_rect.yMaximum(),
        )
        kept: dict = {}
        for entity_fp, sf in entity_map.items():
            if not sf.geom_wkb:
                kept[entity_fp] = sf
                continue
            env = _cached_envelope(sf.geom_wkb)
            if envelope_intersects(env, bbox_tuple):
                kept[entity_fp] = sf
            else:
                n_dropped += 1
        filtered[ds_fp] = kept
        n_kept += len(kept)

    flog(
        f"filter_snapshot_by_bbox: n_kept={n_kept} n_dropped={n_dropped}",
        "DEBUG",
    )
    return result._replace(features=filtered, n_entities=n_kept)


def query_snapshot_date_range(journal, layer_infos: List[dict]) -> tuple:
    """Return ``(first_iso, last_iso)`` from audit_event for given layers.

    Runs on the calling thread (main thread acceptable - 1 row per layer).
    """
    first_iso = ""
    last_iso = ""
    conn = None
    try:
        conn = journal.create_read_connection()
        for info in layer_infos:
            row = _execute(
                conn, _SQL_DATE_RANGE, (info["fingerprint"],)
            ).fetchone()
            if row:
                if row[0] and (not first_iso or row[0] < first_iso):
                    first_iso = row[0]
                if row[1] and (not last_iso or row[1] > last_iso):
                    last_iso = row[1]
        flog(
            f"snapshot_date_range: first={first_iso} last={last_iso}",
            "DEBUG",
        )
    except Exception as exc:  # noqa: BLE001
        flog(f"snapshot_date_range: error={exc!r}", "WARNING")
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001
                flog(f"snapshot_date_range: read connection close failed "
                     f"({exc!r})", "DEBUG")

    today = journal_today().isoformat()
    return (
        first_iso or "2020-01-01T00:00:00",
        last_iso or (today + "T23:59:59"),
    )


_BAR_SECOND = "%Y-%m-%dT%H:%M:%S"


def _utc_instant(iso: str):
    """Aware UTC datetime of a journal timestamp, None when unreadable."""
    from datetime import timezone
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def second_at_or_after(iso: str):
    """First date-bar value (UTC, to the second) at or after *iso*."""
    from datetime import timedelta
    dt = _utc_instant(iso)
    if dt is None:
        return None
    if dt.microsecond:
        dt = dt.replace(microsecond=0) + timedelta(seconds=1)
    return dt.strftime(_BAR_SECOND)


def second_before(iso: str):
    """Last date-bar value (UTC, to the second) strictly before *iso*."""
    from datetime import timedelta
    dt = _utc_instant(iso)
    if dt is None:
        return None
    return (dt - timedelta(seconds=1)).replace(microsecond=0).strftime(_BAR_SECOND)


# Since this journal schema every purge writes its horizon in the journal
# (retention._remember_horizons -> journal_coverage).
_HORIZON_RECORDING_SCHEMA = 7
# A journal CREATED at that schema records its migrations 1..7 in the one
# transaction that creates it; one upgraded from an older version holds the
# older rows from the day it was created.
_CREATION_WINDOW_S = 60


def recording_journal_creation(conn):
    """When this journal was created, if every purge since left a horizon.

    The coverage stamp answers first (journal_writes.coverage_origin):
    "created" -> the instant of its ``since`` (no journal history before
    it; a row older than it was brought in, and `_layer_floor` keeps its
    floor); "migrated", or "" (the opener could not say) -> None: the
    journal lived under a release that purged without a record. It used to
    be the inference below alone, which a clock ahead when an older release
    created the file turns into "created by v7" (no floor below a silent
    purge: tx_journal_coverage_origin_stamp, case C), and which floors the
    first edit after the upgrade of a legacy journal that never held a row
    (case I). An unreadable ``since``: None (the conservative answer).

    No stamp (a journal made v7 before it existed): the inference. That is
    the case of a journal created by a version that records the
    horizon (schema >= 7): its schema_version rows, from the first to the
    one of that schema, were all written when it was created. A journal
    created by an older version may have been purged by it, with the
    horizon kept in session memory only: nothing in it can prove otherwise
    (None). Unreadable: None too (the conservative answer).
    """
    from ..core.journal_writes import ORIGIN_CREATED, coverage_origin
    stamp = coverage_origin(conn)
    if stamp is not None:
        if stamp["origin"] != ORIGIN_CREATED:
            return None
        return _utc_instant(stamp["since"])
    try:
        rows = conn.execute(
            "SELECT version_number, applied_at FROM schema_version").fetchall()
    except sqlite3.Error:
        return None
    stamps = {int(v): _utc_instant(a) for v, a in rows
              if v is not None and a}
    recording = stamps.get(_HORIZON_RECORDING_SCHEMA)
    if recording is None or not stamps or None in stamps.values():
        return None
    first = stamps[min(stamps)]
    if (recording - first).total_seconds() > _CREATION_WINDOW_S:
        return None
    return first


def _layer_floor(conn, fingerprint: str, created):
    """(floor, kind) of one source; kind "horizon", "oldest_event" or "".

    The horizon is `retention.purge_floor`: the purges that removed history
    FROM THE START (retention, excess, orphan traces). It used to be
    `purged_horizon`, which also takes the end of every interval a logical
    purge removed: one maintenance click (an INSERT/DELETE pair purged)
    then refused every date of the layer before the end of that pair, dates
    the journal still rebuilds and the Rewind still accepts. The intervals
    are refused on their own (`purged_interval_refusal`).
    """
    from ..core.retention import has_unvouched_floor, purge_floor
    horizon = purge_floor(fingerprint, conn)
    if horizon:
        floor = second_at_or_after(horizon)
        if floor is None:
            raise ValueError(f"purge horizon unreadable: {horizon!r}")
        return floor, "horizon"
    row = _execute(conn, _SQL_OLDEST_ROW, (fingerprint,)).fetchone()
    oldest = row[0] if row else None
    if not oldest:
        return "", ""
    oldest_at = _utc_instant(oldest)
    if (created is not None and oldest_at is not None and oldest_at >= created
            and not has_unvouched_floor(conn, fingerprint)):
        # Every row of it was journaled here, by a version that records
        # each purge: none happened.
        return "", ""
    # A row older than the journal itself was brought in from elsewhere
    # (imported, copied from another journal): its history there is not
    # known, the oldest row stays the guard.
    floor = second_before(oldest)
    if floor is None:
        raise ValueError(f"oldest event date unreadable: {oldest!r}")
    return floor, "oldest_event"


def query_snapshot_floor(journal, layer_infos: List[dict]):
    """Oldest cutoff Review can answer for these layers.

    A date-bar value (UTC, to the second). "" when unbounded, None when it
    could not be read (the caller then keeps a conservative floor).

    Below the line of a purge the events were deleted: the before-image of
    the first SURVIVING event is the state at that line, not the state at
    an older date. Per source:
      * a purge horizon (the journal_coverage row a purge writes, or this
        session's memory): every deleted row is at or before it, so from
        it on (rounded UP to the second: the bar cannot say 08:30:00.25)
        the journal answers for every entity;
      * no horizon, in a journal created by a version that records every
        horizon (recording_journal_creation) and whose rows were all
        journaled there: it was never purged, no floor;
      * no horizon otherwise (a journal that lived before v7, or a row
        older than the journal, carried or imported from another one): the
        journal cannot say whether it was purged. Before v7 the horizon
        lived in session memory only and v7 does not rebuild it, so the
        oldest surviving row is the guard, as for the Rewind of a journal
        that lived before v7 (retention.coverage_refusal,
        `_legacy_floor_refusal`). The Rewind does not guard the carried
        row: its age cannot tell a carry whose source vouched for it (the
        first save of a new project) from one whose did not, and refusing
        both broke "Undo whole session" after the first save
        (rw_first_save_history_reachable). Review, which writes nothing,
        keeps this floor on it; the carried history of the known limit
        (`_legacy_floor_refusal`) is locked by rw_floor_carried_history.
        The floor is
        the last second STRICTLY before it: there the state is the
        before-image of that row, which is not shown as already done (the
        old inclusive clamp on the first event did exactly that). The
        floor used to be "" here: a date before a purge by an older
        version was answered with the state at the purge line, as if it
        were the state asked;
      * no event at all: nothing to reconstruct, no floor.
    The floor of several sources is the newest one. An interval a logical
    purge removed is no floor: the dates before it are rebuilt, the dates
    inside it are refused per date (`purged_interval_refusal`).
    """
    floor = ""
    kinds = []
    conn = None
    try:
        conn = journal.create_read_connection()
        created = recording_journal_creation(conn)
        for info in layer_infos:
            layer_floor, kind = _layer_floor(conn, info["fingerprint"],
                                             created)
            if kind:
                kinds.append(kind)
            if layer_floor > floor:
                floor = layer_floor
    except Exception as exc:  # noqa: BLE001
        flog(f"snapshot_floor: purge horizon unreadable, Review keeps a "
             f"conservative floor ({exc!r})", "WARNING")
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001
                flog(f"snapshot_floor: read connection close failed "
                     f"({exc!r})", "DEBUG")
    flog(f"snapshot_floor: floor={floor or 'none'} "
         f"n_layers={len(layer_infos)} kinds={sorted(set(kinds))} "
         f"journal_records_every_purge={created is not None}", "INFO")
    return floor


def purged_interval_refusal(conn, layer_infos: List[dict], cutoff_iso: str):
    """Why the journal cannot rebuild *cutoff_iso* for these layers, or None.

    A logical purge (INSERT/DELETE pair, compensation undone by an Undo)
    removes the history of ONE interval of a layer: its dates are
    unrebuildable, those before and after it are not
    (`retention.purged_gaps`, the intervals the Rewind refuses through
    `retention.coverage_refusal`). Both ends are refused, like there. The
    comparison is on instants: the date bar speaks naive UTC to the second,
    the journal aware ISO with microseconds, and a string comparison of the
    two misplaces the ends. An interval whose ends cannot be read refuses
    the date (it cannot be vouched for). The intervals are those
    `purged_gaps` returns: session memory and the journal's rows, read in
    the caller's transaction.
    """
    from ..core.retention import purged_gaps
    asked = _utc_instant(cutoff_iso)
    if asked is None:
        return None
    for info in layer_infos:
        for low, high in purged_gaps(info["fingerprint"], conn):
            low_at, high_at = _utc_instant(low), _utc_instant(high)
            if low_at is None or high_at is None or low_at <= asked <= high_at:
                return (f"History of layer {info.get('layer_name') or '?'!r} "
                        f"purged between {low} and {high} (maintenance purge "
                        f"of logical garbage), requested: {cutoff_iso}; a "
                        f"date before {low} or after {high} can be rebuilt")
    return None


__all__ = [
    "SnapshotRebuildWorker", "query_snapshot_date_range",
    "filter_snapshot_by_bbox", "query_snapshot_floor",
    "second_before", "second_at_or_after",
    "retain_until_finished", "retained_threads", "join_review_thread",
]
