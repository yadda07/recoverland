"""Extent-scoped marker scanner for the Review date bar.

CHANGE C: the date-bar markers must reflect modification dates only inside the
current viewport, and update dynamically when the user pans or zooms. This
worker runs the spatial scan in a background thread so the UI stays responsive.

Design constraints
------------------
- Only ``created_at`` and ``operation_type`` are needed for markers; geometry
  columns are needed only for BBOX filtering. The query therefore selects only
  these four columns, avoiding the heavy attribute/schema BLOBs that would be
  loaded by the full event fetch.
- Spatial filtering reuses the same WKB-envelope helpers as the Time Lens
  fetcher (``wkb_envelope``) so behaviour is consistent.
- Hard per-layer limit (``_MARKER_SCAN_LIMIT``) to keep pan/zoom responsive on
  very large journals; truncation is logged but not surfaced as a UI warning
  because markers are a navigational hint, not a data guarantee.
"""
from __future__ import annotations

import time
import uuid
from typing import List

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog
from ..core.wkb_envelope import envelope_intersects, parse_envelope

_MARKER_SCAN_LIMIT = 5000

_SQL_MARKERS_IN_ZONE = (
    "SELECT created_at, operation_type, geometry_wkb, new_geometry_wkb"
    " FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND created_at >= ? AND created_at <= ?"
    " AND invalidated_at IS NULL"
    " ORDER BY created_at ASC LIMIT ?"
)


class MarkerScanWorker(QThread):
    """Background thread that scans modification dates inside a viewport.

    Signals:
        - markers_ready(trace_id: str, markers: List[Tuple[str, str]])
        - error(trace_id: str, message: str)
    """

    markers_ready = pyqtSignal(str, list)
    error = pyqtSignal(str, str)

    def __init__(
        self,
        journal,
        layer_infos: List[dict],
        bbox_per_layer: dict,
        t_min: str,
        t_max: str,
        trace_id: str = "",
    ) -> None:
        super().__init__()
        self._journal = journal
        self._layer_infos = layer_infos
        self._bbox_per_layer = bbox_per_layer
        self._t_min = t_min
        self._t_max = t_max
        self._cancelled = False
        self.trace_id = trace_id or uuid.uuid4().hex[:8]

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def run(self) -> None:
        tid = self.trace_id
        t0 = time.monotonic()
        conn = None
        markers_set: set = set()

        flog(
            f"[{tid}] marker_scan: start t_min={self._t_min} t_max={self._t_max} "
            f"n_layers={len(self._layer_infos)}",
            "INFO",
        )

        try:
            conn = self._journal.create_read_connection()
            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] marker_scan: cancelled", "INFO")
                    return

                ds_fp = info["fingerprint"]
                bbox = self._bbox_per_layer.get(ds_fp)
                if bbox is None:
                    flog(
                        f"[{tid}] marker_scan: no_bbox layer={info.get('layer_name', '?')} "
                        f"datasource={ds_fp}",
                        "DEBUG",
                    )
                    continue

                bbox_xy = (bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum())
                rows = conn.execute(
                    _SQL_MARKERS_IN_ZONE,
                    (ds_fp, self._t_min, self._t_max, _MARKER_SCAN_LIMIT + 1),
                ).fetchall()

                truncated = max(0, len(rows) - _MARKER_SCAN_LIMIT)
                if truncated:
                    rows = rows[:_MARKER_SCAN_LIMIT]

                n_dropped = 0
                for row in rows:
                    created_at, op_type, geom_wkb, new_geom_wkb = row
                    if not created_at:
                        continue
                    geom = geom_wkb or new_geom_wkb
                    if geom is not None:
                        env = parse_envelope(geom)
                        if not envelope_intersects(env, bbox_xy):
                            n_dropped += 1
                            continue
                    markers_set.add((created_at, (op_type or "INSERT").upper()))

                flog(
                    f"[{tid}] marker_scan: layer={info.get('layer_name', '?')} "
                    f"datasource={ds_fp} n_rows={len(rows)} n_dropped_bbox={n_dropped} "
                    f"truncated={truncated}",
                    "DEBUG",
                )

            markers = sorted(markers_set)
            elapsed_ms = int((time.monotonic() - t0) * 1000)
            flog(
                f"[{tid}] marker_scan: done n_markers={len(markers)} "
                f"elapsed_ms={elapsed_ms}",
                "INFO",
            )
            if not self._cancelled:
                self.markers_ready.emit(tid, markers)

        except Exception as exc:  # noqa: BLE001
            flog(f"[{tid}] marker_scan: error={exc!r}", "ERROR")
            self.error.emit(tid, str(exc))

        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
