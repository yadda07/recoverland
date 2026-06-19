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
from ..core.identity import get_identity_strength_for_layer
from ..core.search_service import _row_to_event
from ..core.sqlite_schema import AUDIT_EVENT_COLUMNS
from ..core.support_policy import IdentityStrength

_ALIASED_COLS = ", ".join(f"ae.{c}" for c in AUDIT_EVENT_COLUMNS)

_SQL_ALL_EVENTS_BEFORE = "".join([
    "SELECT ", _ALIASED_COLS,
    " FROM audit_event ae",
    " WHERE ae.datasource_fingerprint = ?",
    " AND ae.created_at <= ?",
    " AND ae.invalidated_at IS NULL",
    " ORDER BY ae.entity_fingerprint, ae.created_at, ae.event_id",
])

_SQL_DATE_RANGE = (
    "SELECT MIN(created_at), MAX(created_at)"
    " FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND invalidated_at IS NULL"
)

# CHANGE B: entity_fingerprints with at least one event strictly AFTER the
# cutoff. These are the ONLY entities whose state at T differs from the current
# / live data, so Review reconstructs and shows only these (no duplication of
# the unchanged source layers). feature_identity_json is fetched for the
# NULL-fingerprint fallback so keys match the reconstruction exactly (see
# temporal_snapshot_engine.compute_entity_key).
_SQL_FPS_CHANGED_AFTER = (
    "SELECT DISTINCT entity_fingerprint, feature_identity_json FROM audit_event"
    " WHERE datasource_fingerprint = ?"
    " AND created_at > ?"
    " AND invalidated_at IS NULL"
)

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
                rows_c = conn.execute(
                    _SQL_FPS_CHANGED_AFTER, (fp, self._cutoff_iso)
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
                cursor = conn.execute(
                    _SQL_ALL_EVENTS_BEFORE,
                    (fp, self._cutoff_iso),
                )
                for row in cursor:
                    events.append(_row_to_event(row))
                    total_rows += 1
                    if total_rows > _SNAPSHOT_ROW_BUDGET:
                        partial = True
                        break
                mini_cache[fp] = events

                flog(
                    f"[{tid}] snap_worker: layer={info['layer_name']} "
                    f"n_events={len(events)} cutoff={self._cutoff_iso}"
                    f"{' PARTIAL_BUDGET_HIT' if partial else ''}",
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
                    partial_reason=f"row_budget_exceeded:{_SNAPSHOT_ROW_BUDGET}",
                )
                flog(
                    f"[{tid}] snap_worker: PARTIAL snapshot "
                    f"total_rows>{_SNAPSHOT_ROW_BUDGET} "
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
                except Exception:  # noqa: BLE001
                    pass

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
                    f"geom_wkb={type(ev.geometry_wkb).__name__}:"
                    f"{len(ev.geometry_wkb) if ev.geometry_wkb else 0} "
                    f"new_geom_wkb={type(ev.new_geometry_wkb).__name__}:"
                    f"{len(ev.new_geometry_wkb) if ev.new_geometry_wkb else 0} "
                    "no_wkb_kept",
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


_BASE_FEATURE_CAP = 50000


def _parse_iso_utc(iso):
    """Parse an ISO string to a UTC-aware datetime, or None on failure."""
    if not iso:
        return None
    try:
        from datetime import timezone
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def _resolve_pk_field(layer):
    """Return the layer's primary-key field name, or None (FID-only identity)."""
    try:
        pk_idx = layer.dataProvider().pkAttributeIndexes()
    except Exception:
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
    from qgis.core import QgsFeatureRequest, QgsProject
    from ..core.geometry_utils import geometry_to_wkb
    from ..core.serialization import serialize_attributes
    from ..core.temporal_snapshot_engine import SnapshotFeature

    t0 = time.monotonic()
    tracked = result.tracked_fps or {}
    baseline = result.layer_baseline or {}
    cutoff_dt = result.cutoff_dt
    project = QgsProject.instance()
    features = {ds: dict(em) for ds, em in result.features.items()}
    n_added_total = 0
    n_seen_total = 0
    fid_only_layers = []
    baseline_missing_layers = []

    for info in layer_infos:
        ds_fp = info["fingerprint"]
        layer_name = info.get("layer_name", "?")

        t0_iso = baseline.get(ds_fp)
        t0_dt = _parse_iso_utc(t0_iso)
        if t0_dt is not None and cutoff_dt is not None and cutoff_dt < t0_dt:
            baseline_missing_layers.append(layer_name)
            flog(
                f"[{trace_id}] base_merge: skip_before_baseline layer={layer_name} "
                f"cutoff={cutoff_dt.isoformat()} t0={t0_iso} "
                f"reason=no_tracking_info_at_T",
                "WARNING",
            )
            continue

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

            if pk_field is None:
                strength = get_identity_strength_for_layer(layer)
                if strength in (IdentityStrength.WEAK, IdentityStrength.NONE):
                    fid_only_layers.append(layer_name)
                    flog(
                        f"[{trace_id}] base_merge: no_pk layer={layer_name} "
                        f"identity=fid-only strength={strength.value} "
                        f"unstable_across_renumber as_of_T_risk",
                        "WARNING",
                    )
                else:
                    flog(
                        f"[{trace_id}] base_merge: no_pk layer={layer_name} "
                        f"identity=fid-only strength={strength.value} "
                        f"fid_considered_stable no_warning",
                        "INFO",
                    )

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

            n_seen_total += n_seen
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
        except Exception as exc:
            flog(
                f"[{trace_id}] base_merge: layer_error layer={layer_name} "
                f"error={exc!r}",
                "ERROR",
            )
            continue

    new_n = result.n_entities + n_added_total
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    flog(
        f"[{trace_id}] base_merge: done n_added={n_added_total} "
        f"n_seen={n_seen_total} n_entities={new_n} cap={_BASE_FEATURE_CAP} "
        f"elapsed_ms={elapsed_ms}",
        "INFO",
    )
    if n_added_total:
        flog(
            f"[{trace_id}] base_merge: semantics untracked_assumed_present_at_T "
            f"n_added={n_added_total} caveat=created_after_T_untracked_shown",
            "INFO",
        )
    return result._replace(
        features=features,
        n_entities=new_n,
        fid_only_layers=tuple(fid_only_layers),
        baseline_missing_layers=tuple(baseline_missing_layers),
    )


__all__ = ["SnapshotRebuildWorker", "query_snapshot_date_range", "filter_snapshot_by_bbox", "merge_untracked_base"]
