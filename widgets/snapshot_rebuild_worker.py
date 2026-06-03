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

import json
import time
import uuid
from datetime import datetime
from typing import List

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog
from ..core.search_service import _row_to_event
from ..core.sqlite_schema import AUDIT_EVENT_COLUMNS

_ALIASED_COLS = ", ".join(f"ae.{c}" for c in AUDIT_EVENT_COLUMNS)

_SQL_ALL_EVENTS_BEFORE = (
    "SELECT " + _ALIASED_COLS +
    " FROM audit_event ae"
    " WHERE ae.datasource_fingerprint = ?"
    " AND ae.created_at <= ?"
    " AND ae.invalidated_at IS NULL"
    " ORDER BY ae.entity_fingerprint, ae.created_at, ae.event_id"
)

_SQL_DATE_RANGE = (
    "SELECT MIN(created_at), MAX(created_at)"
    " FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND invalidated_at IS NULL"
)

_SQL_ALL_EVENT_MARKERS = (
    "SELECT DISTINCT created_at, operation_type FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND invalidated_at IS NULL"
    " AND created_at IS NOT NULL"
)

_SQL_TRACKED_FPS = (
    "SELECT DISTINCT entity_fingerprint FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND invalidated_at IS NULL"
    " AND entity_fingerprint IS NOT NULL"
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

        flog(
            f"[{tid}] snap_worker: start cutoff={self._cutoff_iso} "
            f"n_layers={len(self._layer_infos)}",
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
                    _SQL_ALL_EVENTS_BEFORE,
                    (fp, self._cutoff_iso),
                ).fetchall()
                events = [_row_to_event(r) for r in rows]
                mini_cache[fp] = events
                total_rows += len(events)

                flog(
                    f"[{tid}] snap_worker: layer={info['layer_name']} "
                    f"n_events={len(events)} cutoff={self._cutoff_iso}",
                    "INFO",
                )

            cutoff_dt = datetime.fromisoformat(
                self._cutoff_iso.replace("Z", "+00:00")
            )
            result = reconstruct_snapshot_at(mini_cache, cutoff_dt, trace_id=tid)

            all_markers_set: set = set()
            for info in self._layer_infos:
                rows_m = conn.execute(
                    _SQL_ALL_EVENT_MARKERS, (info["fingerprint"],)
                ).fetchall()
                all_markers_set.update(
                    (r[0], (r[1] or "INSERT").upper())
                    for r in rows_m if r[0]
                )
            all_event_markers = tuple(sorted(all_markers_set))
            result = result._replace(all_event_markers=all_event_markers)

            tracked_fps: dict = {}
            n_tracked_total = 0
            for info in self._layer_infos:
                rows_t = conn.execute(
                    _SQL_TRACKED_FPS, (info["fingerprint"],)
                ).fetchall()
                fps = {r[0] for r in rows_t if r[0]}
                tracked_fps[info["fingerprint"]] = fps
                n_tracked_total += len(fps)
            result = result._replace(tracked_fps=tracked_fps)

            elapsed_ms = int((time.monotonic() - t0) * 1000)
            flog(
                f"[{tid}] snap_worker: done "
                f"n_entities={result.n_entities} "
                f"n_all_markers={len(all_event_markers)} "
                f"n_tracked_fps={n_tracked_total} "
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


_BASE_FEATURE_CAP = 50000  # per-layer, viewport-bounded volume guard (BL-RVF-P0-06)


def _resolve_pk_field(layer):
    """Return the layer's primary-key field name, or None (FID-only identity)."""
    try:
        pk_idx = layer.dataProvider().pkAttributeIndexes()
    except Exception:  # noqa: BLE001
        return None
    if not pk_idx:
        return None
    fields = layer.fields()
    for idx in pk_idx:
        if 0 <= idx < fields.count():
            return fields.at(idx).name()
    return None


def _feature_entity_fp(feat, pk_field):
    """Recompute a source feature's entity fingerprint.

    Mirrors identity.compute_entity_fingerprint(compute_feature_identity(...)):
    'pk:<field>=<value>' when a PK value is present, else 'fid:<id>'.
    """
    if pk_field:
        try:
            val = feat[pk_field]
        except (KeyError, IndexError):
            val = None
        if val is not None:
            if not isinstance(val, (int, float, str)):
                val = str(val)
            return f"pk:{pk_field}={val}"
    return f"fid:{feat.id()}"


def merge_untracked_base(result, layer_infos, bbox_per_layer, trace_id=""):
    """Merge current source features that have NO audit events into the snapshot.

    A feature with no event was never modified since tracking began, so its
    CURRENT state IS its state at T (Review = full state, like Rewind but read
    only). Tracked entities (any event, incl. created-after-T) are skipped here
    because the reconstruction engine already resolves them.

    Runs on the QGIS main thread (reads QgsVectorLayer). Bounded by the viewport
    bbox + a hard per-layer feature cap (_BASE_FEATURE_CAP); beyond the cap the
    overflow is dropped with a WARNING (degraded, never unbounded).

    Returns a new SnapshotResult with untracked features appended.
    """
    from qgis.core import QgsFeatureRequest, QgsProject  # noqa: PLC0415
    from ..core.geometry_utils import geometry_to_wkb  # noqa: PLC0415
    from ..core.serialization import serialize_attributes  # noqa: PLC0415
    from ..core.temporal_snapshot_engine import SnapshotFeature  # noqa: PLC0415

    tracked = result.tracked_fps or {}
    project = QgsProject.instance()
    features = {ds: dict(em) for ds, em in result.features.items()}
    n_added_total = 0

    for info in layer_infos:
        ds_fp = info["fingerprint"]
        layer_name = info.get("layer_name", "?")
        try:
            layer = project.mapLayer(info.get("layer_id", ""))
            if layer is None:
                flog(
                    f"[{trace_id}] base_merge: source_layer_missing "
                    f"layer={layer_name}",
                    "WARNING",
                )
                continue
            tracked_set = tracked.get(ds_fp, set())
            ds_feats = features.get(ds_fp) or {}
            field_names = [f.name() for f in layer.fields()]
            pk_field = _resolve_pk_field(layer)
            crs_authid = layer.crs().authid() if layer.crs().isValid() else None

            req = QgsFeatureRequest()
            bbox = bbox_per_layer.get(ds_fp) if bbox_per_layer else None
            if bbox is not None:
                req.setFilterRect(bbox)

            n_seen = 0
            n_added = 0
            n_skip_tracked = 0
            capped = False
            for feat in layer.getFeatures(req):
                n_seen += 1
                if n_seen > _BASE_FEATURE_CAP:
                    capped = True
                    break
                efp = _feature_entity_fp(feat, pk_field)
                if efp in tracked_set or efp in ds_feats:
                    n_skip_tracked += 1
                    continue
                wkb = geometry_to_wkb(feat.geometry())
                if wkb is None:
                    continue
                try:
                    attrs_json = json.dumps(
                        serialize_attributes(feat, field_names),
                        ensure_ascii=False,
                    )
                except (TypeError, ValueError):
                    attrs_json = None
                ds_feats[efp] = SnapshotFeature(
                    entity_fp=efp, geom_wkb=wkb, attrs_json=attrs_json,
                    crs_authid=crs_authid, last_event_id=0,
                    last_op="UNCHANGED", last_created_at="",
                )
                n_added += 1

            if n_added:
                features[ds_fp] = ds_feats
                n_added_total += n_added

            if capped:
                flog(
                    f"[{trace_id}] base_merge: capped layer={layer_name} "
                    f"cap={_BASE_FEATURE_CAP} overflow_dropped",
                    "WARNING",
                )
            flog(
                f"[{trace_id}] base_merge: layer={layer_name} "
                f"n_seen={n_seen} n_tracked_skip={n_skip_tracked} "
                f"n_added={n_added}",
                "INFO",
            )
        except Exception as exc:  # noqa: BLE001
            flog(
                f"[{trace_id}] base_merge: layer_error layer={layer_name} "
                f"error={exc!r}",
                "ERROR",
            )
            continue

    new_n = result.n_entities + n_added_total
    flog(
        f"[{trace_id}] base_merge: done n_added={n_added_total} "
        f"n_entities={new_n}",
        "INFO",
    )
    return result._replace(features=features, n_entities=new_n)


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
