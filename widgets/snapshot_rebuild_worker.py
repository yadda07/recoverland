"""Snapshot rebuild worker — per-date SQL query without full cache pre-load.

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
import time
import uuid
from datetime import datetime
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


def _build_queries(scope) -> tuple:
    all_events_before = "".join([
        "SELECT ", _ALIASED_COLS,
        " FROM audit_event ae",
        " WHERE ", scope("ae.datasource_fingerprint"),
        " AND ae.created_at <= ?2",
        " AND ae.invalidated_at IS NULL",
        " ORDER BY ae.entity_fingerprint, ae.created_at, ae.event_id",
    ])
    date_range = "".join([
        "SELECT MIN(created_at), MAX(created_at)",
        " FROM audit_event",
        " WHERE ", scope("datasource_fingerprint"),
        " AND invalidated_at IS NULL",
    ])
    # CHANGE B: entity_fingerprints with at least one event strictly AFTER
    # the cutoff. These are the ONLY entities whose state at T differs from
    # the current / live data, so Review reconstructs and shows only these
    # (no duplication of the unchanged source layers). feature_identity_json
    # is fetched for the NULL-fingerprint fallback so keys match the
    # reconstruction exactly (see temporal_snapshot_engine.compute_entity_key).
    fps_changed_after = "".join([
        "SELECT DISTINCT entity_fingerprint, feature_identity_json",
        " FROM audit_event",
        " WHERE ", scope("datasource_fingerprint"),
        " AND created_at > ?2",
        " AND invalidated_at IS NULL",
    ])
    return all_events_before, date_range, fps_changed_after


(_SQL_ALL_EVENTS_BEFORE, _SQL_DATE_RANGE,
 _SQL_FPS_CHANGED_AFTER) = _build_queries(_scoped)
(_SQL_ALL_EVENTS_BEFORE_NO_ALIAS, _SQL_DATE_RANGE_NO_ALIAS,
 _SQL_FPS_CHANGED_AFTER_NO_ALIAS) = _build_queries(_plain)

_NO_ALIAS_FALLBACK = {
    _SQL_ALL_EVENTS_BEFORE: _SQL_ALL_EVENTS_BEFORE_NO_ALIAS,
    _SQL_DATE_RANGE: _SQL_DATE_RANGE_NO_ALIAS,
    _SQL_FPS_CHANGED_AFTER: _SQL_FPS_CHANGED_AFTER_NO_ALIAS,
}


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


class SnapshotRebuildWorker(QThread):
    """Fetch the state of each entity at a given date in a background thread.

    Signals
    -------
    result_ready : str, object
        ``(trace_id, SnapshotResult)`` — emitted on success.
    error : str, str
        ``(trace_id, error_message)`` — emitted on fatal error.
    """

    result_ready = pyqtSignal(str, object)
    error = pyqtSignal(str, str)

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
        self._layer_infos = layer_infos
        self._cutoff_iso = cutoff_iso
        self._bbox_per_layer = bbox_per_layer or {}
        self._cancelled = False
        self.trace_id = trace_id or uuid.uuid4().hex[:8]
        self._row_budget = row_budget if row_budget is not None else _SNAPSHOT_ROW_BUDGET

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

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
            cutoff_dt = datetime.fromisoformat(
                self._cutoff_iso.replace("Z", "+00:00")
            )

            # CHANGE B: Review = "what was different back then". Only entities
            # with >=1 event strictly AFTER the cutoff have a state at T that
            # differs from the live data; everything else is identical to the
            # source layers and must NOT be duplicated. Compute that set FIRST
            # (cheap, indexed, no geometry/attr BLOBs) so an unchanged date
            # (e.g. today) short-circuits with zero reconstruction.
            changed_after: dict = {}
            n_changed_total = 0
            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] snap_worker: cancelled", "INFO")
                    return
                fp = info["fingerprint"]
                rows_c = _execute(
                    conn, _SQL_FPS_CHANGED_AFTER, (fp, self._cutoff_iso)
                ).fetchall()
                keys = {compute_entity_key(r[0], r[1]) for r in rows_c}
                changed_after[fp] = keys
                n_changed_total += len(keys)

            if n_changed_total == 0:
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
            total_rows = 0
            partial = False

            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] snap_worker: cancelled", "INFO")
                    return

                fp = info["fingerprint"]
                # Stream the cursor (bounded), never fetchall() unbounded.
                events = []
                cursor = _execute(
                    conn, _SQL_ALL_EVENTS_BEFORE,
                    (fp, self._cutoff_iso),
                )
                for row in cursor:
                    events.append(_row_to_event(row))
                    total_rows += 1
                    if total_rows > self._row_budget:
                        partial = True
                        break
                mini_cache[fp] = events

                flog(
                    f"[{tid}] snap_worker: layer={info['layer_name']} "
                    f"n_events={len(events)} cutoff={self._cutoff_iso}"
                    f"{' PARTIAL_BUDGET_HIT' if partial else ''}"
                    f" row_budget={self._row_budget}",
                    "WARNING" if partial else "INFO",
                )
                if partial:
                    break

            result = reconstruct_snapshot_at(
                mini_cache, cutoff_dt, trace_id=tid,
                should_cancel=lambda: self._cancelled,
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
                f"total_rows={total_rows} elapsed_ms={elapsed_ms}",
                "INFO",
            )
            if self._cancelled:
                flog(f"[{tid}] snap_worker: cancelled pre-emit", "INFO")
                return
            self.result_ready.emit(tid, result)

        except Exception as exc:  # noqa: BLE001
            flog(f"[{tid}] snap_worker: error={exc!r}", "ERROR")
            self.error.emit(tid, str(exc))

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"[{tid}] snap_worker: read connection close failed "
                         f"({exc!r})", "DEBUG")

    @staticmethod
    def _filter_changed_after(result, changed_after: dict, tid: str):
        """Keep only entities modified/deleted AFTER the cutoff.

        Their state at T differs from the current/live data, so they are the
        only entities Review must paint. Entities present at T but unchanged
        since (no event after T) are identical to the source layers and are
        dropped to avoid duplicating the live data.
        """
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
        return result._replace(features=filtered, n_entities=n_kept)


def filter_snapshot_by_bbox(result, bbox_per_layer: dict):
    """Filter a SnapshotResult by bbox using the resolved geom_wkb per feature.

    bbox_per_layer: {datasource_fingerprint: QgsRectangle} in layer CRS.
    Returns a new SnapshotResult with updated features and n_entities.
    Uses pure-Python WKB parser (wkb_envelope) — no QgsGeometry dependency.
    """
    if not bbox_per_layer:
        return result

    from ..core.wkb_envelope import envelope_intersects, parse_envelope  # noqa: PLC0415

    filtered: dict = {}
    n_kept = 0
    n_dropped = 0
    _diag_done = False

    for ds_fp, entity_map in result.features.items():
        bbox_rect = bbox_per_layer.get(ds_fp)
        if not _diag_done and entity_map:
            first_sf = next(iter(entity_map.values()))
            _gwkb = first_sf.geom_wkb
            _env = parse_envelope(_gwkb) if _gwkb else None
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
            env = parse_envelope(sf.geom_wkb)
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

    Runs on the calling thread (main thread acceptable — 1 row per layer).
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


__all__ = ["SnapshotRebuildWorker", "query_snapshot_date_range", "filter_snapshot_by_bbox"]
