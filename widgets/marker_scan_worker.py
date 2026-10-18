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
- A mark is an instant at which the state Review draws changes, with the
  Rewind's own rule for the compensations (rewind_dedup.traces_in_effect,
  see trace_change_marks): a trace marks its created_at when it took effect
  then, and its invalidated_at (the Undo, which writes no event) when it
  was in effect until then. The scan used to drop every invalidated row:
  a compensation undone later, and the Undo itself, left no mark while
  Review drew a different state on each side of them.
- cancel() interrupts the SQL statement in flight (sqlite3 allows it from
  another thread): the flag alone was only read between two layers, and a
  superseded scan on a large journal kept Review's join waiting.
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from typing import List, NamedTuple, Optional

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog
from ..core.wkb_envelope import envelope_intersects, parse_envelope

_MARKER_SCAN_LIMIT = 5000

# What an Undo does to the state a trace had applied (an UPDATE is undone
# by an UPDATE): the kind of its mark.
_UNDO_OP = {"INSERT": "DELETE", "DELETE": "INSERT"}


class _MarkRow(NamedTuple):
    """The columns traces_in_effect reads, and the kind of the mark."""

    event_id: Optional[int]
    created_at: Optional[str]
    operation_type: str
    restored_from_event_id: Optional[int]
    invalidated_at: Optional[str]
    event_schema_version: Optional[int]


def _build_markers_sql(scoped: bool) -> str:
    """Marker query, alias-aware or not.

    The date bar draws the marks the user clicks on to travel back. Left
    on the bare fingerprint equality, it stopped showing the days that
    were captured under an obsolete fingerprint: those edits became
    unreachable through the only control that leads to them. The
    fingerprint is bound once (`?1`) so the parameter tuple is unchanged.
    """
    from .snapshot_rebuild_worker import _plain, _scoped

    column = "datasource_fingerprint"
    where = _scoped(column) if scoped else _plain(column)
    # Readable rows are those Review reads (snapshot_rebuild_worker
    # _READABLE): an invalidated TRACE is a compensation an Undo reverted,
    # in effect from its created_at to its invalidated_at; an invalidated
    # row that is not a trace means nothing and stays out. A trace undone
    # inside the window is fetched even when it was made before it.
    return "".join((
        "SELECT created_at, operation_type, geometry_wkb, new_geometry_wkb,",
        " event_id, restored_from_event_id, invalidated_at,",
        " event_schema_version",
        " FROM audit_event",
        " WHERE ", where,
        " AND ((created_at >= ?2 AND created_at <= ?3)",
        " OR (restored_from_event_id IS NOT NULL",
        " AND invalidated_at >= ?2 AND invalidated_at <= ?3))",
        " AND (invalidated_at IS NULL OR restored_from_event_id IS NOT NULL)",
        " ORDER BY created_at ASC LIMIT ?4",
    ))


_SQL_MARKERS_IN_ZONE = _build_markers_sql(True)
_SQL_MARKERS_IN_ZONE_NO_ALIAS = _build_markers_sql(False)


def _execute_markers(conn, params: tuple):
    """Run the marker query, degrading on a journal with no alias table."""
    try:
        return conn.execute(_SQL_MARKERS_IN_ZONE, params)
    except sqlite3.OperationalError as exc:
        if "datasource_alias" not in str(exc):
            raise
        flog(f"marker_scan: no datasource_alias table ({exc}); scanning the "
             f"asked fingerprint only", "WARNING")
        return conn.execute(_SQL_MARKERS_IN_ZONE_NO_ALIAS, params)


def _with_cancellers(trace, children: dict) -> list:
    """*trace* and every trace that references it, transitively."""
    out, seen, todo = [], set(), [trace]
    while todo:
        row = todo.pop()
        if row.event_id in seen:
            continue
        seen.add(row.event_id)
        out.append(row)
        todo.extend(children.get(row.event_id, ()))
    return out


def trace_change_marks(traces: list, t_min: str = "", t_max: str = "") -> list:
    """[(instant, operation)] at which the compensations *traces* changed
    the state, by the Rewind's rule (rewind_dedup.traces_in_effect).

    A trace marks its created_at when it is in effect at that instant (it
    took effect then), and its invalidated_at when it is still in effect
    just before it (the Undo took it out: the inverse operation). A trace
    already out of effect at either instant changed nothing there and
    leaves no mark. Instants, never strings: a trace and its Undo can carry
    different clock formats. Each trace is judged with the traces that
    reference it only (its cancellers, recursively), the only ones that
    can take it out of effect: the whole set per trace was quadratic on a
    mass rewind. Marks outside [t_min, t_max] are dropped.
    """
    from ..core.rewind_dedup import traces_in_effect
    from .snapshot_rebuild_worker import _utc_instant

    lo = _utc_instant(t_min) if t_min else None
    hi = _utc_instant(t_max) if t_max else None
    children: dict = {}
    for t in traces:
        children.setdefault(t.restored_from_event_id, []).append(t)

    def _in_window(instant) -> bool:
        return ((lo is None or instant >= lo) and (hi is None or instant <= hi))

    def _status(family, instant, inclusive: bool) -> dict:
        def happened(ts) -> bool:
            at = _utc_instant(ts) if ts else None
            if at is None:
                return False
            return at <= instant if inclusive else at < instant
        return traces_in_effect(family, happened)

    marks = []
    for t in traces:
        if t.event_id is None:
            continue
        op = (t.operation_type or "UPDATE").upper()
        family = _with_cancellers(t, children)
        made = _utc_instant(t.created_at) if t.created_at else None
        if made is not None and _in_window(made) and _status(family, made, True).get(t.event_id):
            marks.append((t.created_at, op))
        undone = _utc_instant(t.invalidated_at) if t.invalidated_at else None
        if undone is not None and _in_window(undone) and _status(family, undone, False).get(t.event_id):
            marks.append((t.invalidated_at, _UNDO_OP.get(op, op)))
    return marks


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
        # The read connection, reachable from the GUI thread so cancel()
        # can interrupt a statement in flight (sqlite3 allows it).
        self._conn = None
        self._conn_lock = threading.Lock()

    def cancel(self) -> None:
        """Stop the scan: flag it and interrupt the SQL in flight.

        The flag alone was read between two layers only: a superseded scan
        kept reading a large journal to the end, and Review's join
        (snapshot_rebuild_worker.join_review_thread) waited for it or gave
        up with the thread still reading a journal about to be closed.
        """
        self._cancelled = True
        with self._conn_lock:
            conn = self._conn
            if conn is not None:
                try:
                    conn.interrupt()
                except Exception as exc:  # noqa: BLE001 - closing under us
                    flog(f"[{self.trace_id}] marker_scan: interrupt skipped "
                         f"({exc!r})", "DEBUG")

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def _layer_marks(self, conn, info: dict, tid: str) -> Optional[set]:
        """Marks of one layer inside its bbox; None when it has no bbox."""
        ds_fp = info["fingerprint"]
        bbox = self._bbox_per_layer.get(ds_fp)
        if bbox is None:
            flog(
                f"[{tid}] marker_scan: no_bbox layer={info.get('layer_name', '?')} "
                f"datasource={ds_fp}",
                "DEBUG",
            )
            return None

        bbox_xy = (bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum())
        rows = _execute_markers(
            conn,
            (ds_fp, self._t_min, self._t_max, _MARKER_SCAN_LIMIT + 1),
        ).fetchall()

        truncated = max(0, len(rows) - _MARKER_SCAN_LIMIT)
        if truncated:
            rows = rows[:_MARKER_SCAN_LIMIT]

        marks: set = set()
        traces: list = []
        n_dropped = 0
        for row in rows:
            (created_at, op_type, geom_wkb, new_geom_wkb, event_id,
             restored_from, invalidated_at, schema) = row
            if restored_from is not None:
                # No geometry of its own (marker payload): never dropped by
                # the bbox, judged by the in-effect rule below.
                traces.append(_MarkRow(event_id, created_at,
                                       (op_type or "UPDATE").upper(),
                                       restored_from, invalidated_at, schema))
                continue
            if not created_at:
                continue
            geom = geom_wkb or new_geom_wkb
            if geom is not None:
                env = parse_envelope(geom)
                if not envelope_intersects(env, bbox_xy):
                    n_dropped += 1
                    continue
            marks.add((created_at, (op_type or "INSERT").upper()))
        trace_marks = trace_change_marks(traces, self._t_min, self._t_max)
        marks.update(trace_marks)

        flog(
            f"[{tid}] marker_scan: layer={info.get('layer_name', '?')} "
            f"datasource={ds_fp} n_rows={len(rows)} n_dropped_bbox={n_dropped} "
            f"n_traces={len(traces)} n_trace_marks={len(trace_marks)} "
            f"n_undo_marks={sum(1 for t in traces if t.invalidated_at)} "
            f"truncated={truncated}",
            "DEBUG",
        )
        return marks

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
            with self._conn_lock:
                self._conn = conn
            for info in self._layer_infos:
                if self._cancelled:
                    flog(f"[{tid}] marker_scan: cancelled", "INFO")
                    return
                marks = self._layer_marks(conn, info, tid)
                if marks:
                    markers_set.update(marks)

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
            if self._cancelled:
                # cancel() interrupts the statement in flight on purpose;
                # the caller has already moved on and expects nothing.
                flog(f"[{tid}] marker_scan: cancelled mid-query ({exc!r})",
                     "INFO")
                return
            flog(f"[{tid}] marker_scan: error={exc!r}", "ERROR")
            self.error.emit(tid, str(exc))

        finally:
            with self._conn_lock:
                self._conn = None
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"[{tid}] marker_scan: read connection close failed "
                         f"({exc!r})", "DEBUG")
