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

import time
import uuid
from datetime import datetime
from typing import List

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog
from ..core.search_service import _row_to_event
from ..core.sqlite_schema import AUDIT_EVENT_COLUMNS

_ALIASED_COLS = ", ".join(f"ae.{c}" for c in AUDIT_EVENT_COLUMNS)

_SQL_LATEST_STATE = (
    "WITH latest_ts AS ("
    "  SELECT entity_fingerprint, MAX(created_at) AS max_ts"
    "  FROM audit_event"
    "  WHERE datasource_fingerprint = ? AND created_at <= ?"
    "  AND invalidated_at IS NULL"
    "  GROUP BY entity_fingerprint"
    ") SELECT " + _ALIASED_COLS +
    " FROM audit_event ae"
    " INNER JOIN latest_ts"
    " ON ae.entity_fingerprint = latest_ts.entity_fingerprint"
    " AND ae.created_at = latest_ts.max_ts"
    " WHERE ae.datasource_fingerprint = ?"
    " AND ae.invalidated_at IS NULL"
)

_SQL_DATE_RANGE = (
    "SELECT MIN(created_at), MAX(created_at)"
    " FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND invalidated_at IS NULL"
)


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
        parent=None,
    ):
        super().__init__(parent)
        self._journal = journal
        self._layer_infos = layer_infos
        self._cutoff_iso = cutoff_iso
        self._bbox_per_layer = bbox_per_layer or {}
        self._cancelled = False
        self.trace_id = trace_id or uuid.uuid4().hex[:8]

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def run(self) -> None:
        from ..core.temporal_snapshot_engine import reconstruct_snapshot_at

        t0 = time.monotonic()
        tid = self.trace_id
        conn = None

        has_bbox = bool(self._bbox_per_layer)
        flog(
            f"[{tid}] snap_worker: start cutoff={self._cutoff_iso} "
            f"n_layers={len(self._layer_infos)} bbox_filter={has_bbox}",
            "INFO",
        )

        try:
            conn = self._journal.create_read_connection()
            mini_cache: dict = {}
            total_rows = 0

            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] snap_worker: cancelled", "INFO")
                    return

                fp = info["fingerprint"]
                rows = conn.execute(
                    _SQL_LATEST_STATE,
                    (fp, self._cutoff_iso, fp),
                ).fetchall()
                events = [_row_to_event(r) for r in rows]
                bbox = self._bbox_per_layer.get(fp)
                if bbox is not None:
                    n_before = len(events)
                    events = _filter_by_bbox(events, bbox)
                    flog(
                        f"[{tid}] snap_worker: layer={info['layer_name']} "
                        f"bbox_kept={len(events)} bbox_dropped={n_before - len(events)}",
                        "DEBUG",
                    )
                mini_cache[fp] = events
                total_rows += len(events)

                flog(
                    f"[{tid}] snap_worker: layer={info['layer_name']} "
                    f"n_entity_states={len(events)} cutoff={self._cutoff_iso}",
                    "INFO",
                )

            cutoff_dt = datetime.fromisoformat(
                self._cutoff_iso.replace("Z", "+00:00")
            )
            result = reconstruct_snapshot_at(mini_cache, cutoff_dt, trace_id=tid)

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            flog(
                f"[{tid}] snap_worker: done "
                f"n_entities={result.n_entities} "
                f"total_rows={total_rows} elapsed_ms={elapsed_ms}",
                "INFO",
            )
            self.result_ready.emit(tid, result)

        except Exception as exc:  # noqa: BLE001
            flog(f"[{tid}] snap_worker: error={exc!r}", "ERROR")
            self.error.emit(tid, str(exc))

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass


def _filter_by_bbox(events: list, bbox) -> list:
    """Keep only events whose post-event geometry intersects bbox.

    Events without geometry are kept (non-spatial layers, geometry-less ops).
    On any decoding error the event is kept (fail-open).
    bbox must be a QgsRectangle in the layer's own CRS.
    """
    try:
        from qgis.core import QgsGeometry  # noqa: PLC0415
    except ImportError:
        return events

    result = []
    _diag_done = False
    for ev in events:
        wkb = ev.new_geometry_wkb or ev.geometry_wkb
        if not wkb:
            if not _diag_done:
                flog(
                    f"bbox_filter_diag: first_ev op={ev.operation_type} "
                    f"geom_wkb={type(ev.geometry_wkb).__name__}:{len(ev.geometry_wkb) if ev.geometry_wkb else 0} "
                    f"new_geom_wkb={type(ev.new_geometry_wkb).__name__}:{len(ev.new_geometry_wkb) if ev.new_geometry_wkb else 0} "
                    "→ no_wkb_kept",
                    "DEBUG",
                )
                _diag_done = True
            result.append(ev)
            continue
        try:
            geom = QgsGeometry.fromWkb(wkb)
            is_null = geom.isNull()
            intersects = (not is_null) and geom.boundingBox().intersects(bbox)
            if not _diag_done:
                flog(
                    f"bbox_filter_diag: first_ev op={ev.operation_type} "
                    f"wkb_len={len(wkb)} geom_null={is_null} intersects={intersects}",
                    "DEBUG",
                )
                _diag_done = True
            if is_null or intersects:
                result.append(ev)
        except Exception as _exc:  # noqa: BLE001
            if not _diag_done:
                flog(f"bbox_filter_diag: fromWkb_exception={_exc!r}", "DEBUG")
                _diag_done = True
            result.append(ev)
    return result


def filter_snapshot_by_bbox(result, bbox_per_layer: dict):
    """Filter a SnapshotResult by bbox using the resolved geom_wkb per feature.

    Must be called on the main thread (QgsGeometry).
    bbox_per_layer: {datasource_fingerprint: QgsRectangle} in layer CRS.
    Returns a new SnapshotResult with updated features and n_entities.
    """
    if not bbox_per_layer:
        return result
    try:
        from qgis.core import QgsGeometry  # noqa: PLC0415
    except ImportError:
        return result

    filtered: dict = {}
    n_kept = 0
    n_dropped = 0

    for ds_fp, entity_map in result.features.items():
        bbox = bbox_per_layer.get(ds_fp)
        if bbox is None:
            filtered[ds_fp] = entity_map
            n_kept += len(entity_map)
            continue
        kept: dict = {}
        for entity_fp, sf in entity_map.items():
            if not sf.geom_wkb:
                kept[entity_fp] = sf
                continue
            try:
                geom = QgsGeometry.fromWkb(sf.geom_wkb)
                if geom.isNull() or geom.boundingBox().intersects(bbox):
                    kept[entity_fp] = sf
                else:
                    n_dropped += 1
            except Exception:  # noqa: BLE001
                kept[entity_fp] = sf
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
    import datetime as _dt

    first_iso = ""
    last_iso = ""
    conn = None
    try:
        conn = journal.create_read_connection()
        for info in layer_infos:
            row = conn.execute(
                _SQL_DATE_RANGE, (info["fingerprint"],)
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
            except Exception:  # noqa: BLE001
                pass

    today = _dt.date.today().isoformat()
    return (
        first_iso or "2020-01-01T00:00:00",
        last_iso or (today + "T23:59:59"),
    )


__all__ = ["SnapshotRebuildWorker", "query_snapshot_date_range", "filter_snapshot_by_bbox"]
