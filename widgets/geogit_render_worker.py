"""Background worker for GeoGit viewport refresh — heavy CPU off UI thread.

Architecture:
    1. Main thread passes: event cache snapshot, bbox, overlay metadata.
    2. Worker (QThread) does per-layer: bbox filter, plan, repair, reproject,
       build feature data as (wkb_bytes, attributes) tuples.
    3. Worker emits `layer_ready` per layer with pre-built feature data.
    4. Main thread slot: QgsGeometry.fromWkb + addFeatures (~5ms per layer).
    5. Worker emits `all_done` with aggregate totals.

Cancellable: if viewport changes, main thread calls cancel() → worker stops.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from qgis.PyQt.QtCore import QThread, pyqtSignal

from ..core.logger import flog


class _FeatureData:
    """Lightweight container for pre-built feature data (no QGIS objects)."""
    __slots__ = ("wkb", "attributes")

    def __init__(self, wkb: bytes, attributes: list):
        self.wkb = wkb
        self.attributes = attributes


class GeoGitRenderWorker(QThread):
    """Compute GeoGit overlay features in background.

    Signals
    -------
    layer_ready : str, list, list, list
        (fingerprint, past_features, arrow_features, attr_features)
        Each feature is a _FeatureData(wkb, attributes).
    all_done : dict
        {"n_entities": int, "n_features": int, "elapsed_ms": int}
    error : str
        Error message if something goes wrong.
    """

    layer_ready = pyqtSignal(str, object, object, object)
    all_done = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(
        self,
        event_cache: Dict[str, list],
        overlay_metas: List[dict],
        bbox_xy: Tuple[float, ...],
        dst_crs: str,
        t_min: str,
        t_max: str,
        op_filter=None,
        trace_id: str = "",
        parent=None,
    ):
        super().__init__(parent)
        self._event_cache = event_cache
        self._overlay_metas = overlay_metas
        self._bbox_xy = bbox_xy
        self._dst_crs = dst_crs
        self._t_min = t_min
        self._t_max = t_max
        self._op_filter = op_filter
        self._cancelled = False
        self.trace_id = trace_id or uuid.uuid4().hex[:8]

    def cancel(self) -> None:
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def run(self) -> None:
        try:
            self._do_render()
        except Exception as exc:  # noqa: BLE001
            flog(
                f"[{self.trace_id}] geogit_render_worker: error={exc!r}",
                "ERROR",
            )
            self.error.emit(str(exc))

    def _do_render(self) -> None:
        from ..core.lens_planner import plan_lens_view
        from ..core.lens_contracts import LensFetchStats, LensSelection
        from ..core.lens_renderer import (
            _wkb_type_code, _wkb_family, _detect_past_geom_family,
            _feature_attrs,
        )
        from ..core.geometry_utils import (
            geometries_equal,
            is_geometry_present,
            repair_geometry_for_render,
            reproject_geometry_for_render,
        )

        tid = self.trace_id
        t0 = time.monotonic()
        total_entities = 0
        total_features = 0
        now = datetime.now(timezone.utc)

        for meta in self._overlay_metas:
            if self._cancelled:
                flog(f"[{tid}] geogit_render_worker: cancelled", "INFO")
                return

            fp = meta["fingerprint"]
            storage_crs = meta["storage_crs"]
            layer_name = meta["layer_name"]

            events = self._event_cache.get(fp, [])
            if not events:
                self.layer_ready.emit(fp, [], [], [])
                continue

            layer_bbox = self._reproject_bbox(
                self._bbox_xy, self._dst_crs, storage_crs,
            )
            visible_events = self._filter_by_bbox(events, layer_bbox)
            if not visible_events:
                self.layer_ready.emit(fp, [], [], [])
                continue

            if self._cancelled:
                return

            selection = LensSelection(
                layer_id_snapshot=meta.get("layer_id", ""),
                datasource_fp=fp,
                bbox_xy=self._bbox_xy,
                bbox_crs=storage_crs,
                t_min=self._t_min,
                t_max=self._t_max,
                op_filter=self._op_filter,
            )
            stats = LensFetchStats(
                n_events_total=len(visible_events),
                n_events_returned=len(visible_events),
                n_events_truncated=0,
                elapsed_ms=0,
            )
            plan = plan_lens_view(visible_events, selection, layer_name, stats)

            geom_family = _detect_past_geom_family(plan, [])
            transform_cache = {}
            source_fields = meta.get("source_field_names", [])
            ev_index = {
                ev.event_id: ev for ev in visible_events
                if ev.event_id is not None
            }

            past_feats: List[_FeatureData] = []
            arrow_feats: List[_FeatureData] = []
            attr_feats: List[_FeatureData] = []

            for entity_fp, timeline in plan.entities.items():
                if self._cancelled:
                    return

                classification = (
                    timeline.classification.name
                    if hasattr(timeline.classification, "name")
                    else str(timeline.classification)
                )
                # BL-IL-P2-17: condense to 1 past + 1 arrow + 1 marker per entity
                if not timeline.states:
                    continue
                oldest_state = timeline.states[0]
                newest_state = timeline.states[-1]
                representative = newest_state

                # Past geometry: oldest OLD
                if oldest_state.old_geom_wkb and geom_family:
                    code = _wkb_type_code(oldest_state.old_geom_wkb)
                    fam = _wkb_family(code) if code is not None else None
                    if fam == geom_family:
                        geom = repair_geometry_for_render(
                            oldest_state.old_geom_wkb, trace_id=tid,
                        )
                        if is_geometry_present(geom):
                            repro = reproject_geometry_for_render(
                                geom, oldest_state.crs_authid,
                                self._dst_crs, transform_cache,
                                trace_id=tid,
                            )
                            if is_geometry_present(repro):
                                past_feats.append(_FeatureData(
                                    bytes(repro.asWkb()),
                                    self._build_past_attrs(
                                        representative, classification,
                                        now, source_fields,
                                        ev_index,
                                    ),
                                ))

                # Arrow: oldest OLD -> newest NEW (if geom moved)
                first_wkb = oldest_state.old_geom_wkb
                last_wkb = newest_state.new_geom_wkb
                if first_wkb and last_wkb and not geometries_equal(first_wkb, last_wkb):
                    old_g = repair_geometry_for_render(first_wkb, trace_id=tid)
                    new_g = repair_geometry_for_render(last_wkb, trace_id=tid)
                    if is_geometry_present(old_g) and is_geometry_present(new_g):
                        old_r = reproject_geometry_for_render(
                            old_g, oldest_state.crs_authid,
                            self._dst_crs, transform_cache, trace_id=tid,
                        )
                        new_r = reproject_geometry_for_render(
                            new_g, newest_state.crs_authid,
                            self._dst_crs, transform_cache, trace_id=tid,
                        )
                        if is_geometry_present(old_r) and is_geometry_present(new_r):
                            try:
                                old_c = old_r.centroid().asPoint()
                                new_c = new_r.centroid().asPoint()
                                from qgis.core import QgsGeometry
                                arrow = QgsGeometry.fromPolylineXY(
                                    [old_c, new_c],
                                )
                                arrow_feats.append(_FeatureData(
                                    bytes(arrow.asWkb()),
                                    _feature_attrs(
                                        entity_fp, representative,
                                        classification, now,
                                    ),
                                ))
                            except Exception:  # noqa: BLE001
                                pass

                # Attr marker: latest position if any attrs changed
                has_any_attrs = any(st.attrs_delta for st in timeline.states)
                if has_any_attrs:
                    src_wkb = (
                        newest_state.new_geom_wkb
                        or newest_state.old_geom_wkb
                        or oldest_state.old_geom_wkb
                    )
                    if src_wkb:
                        mg = repair_geometry_for_render(src_wkb, trace_id=tid)
                        if is_geometry_present(mg):
                            crs = newest_state.crs_authid or oldest_state.crs_authid
                            mr = reproject_geometry_for_render(
                                mg, crs, self._dst_crs,
                                transform_cache, trace_id=tid,
                            )
                            if is_geometry_present(mr):
                                try:
                                    pt = mr.centroid()
                                    attr_feats.append(_FeatureData(
                                        bytes(pt.asWkb()),
                                        _feature_attrs(
                                            entity_fp, representative,
                                            classification, now,
                                        ),
                                    ))
                                except Exception:  # noqa: BLE001
                                    pass

            total_entities += len(plan.entities)
            total_features += (
                len(past_feats) + len(arrow_feats) + len(attr_feats)
            )

            self.layer_ready.emit(fp, past_feats, arrow_feats, attr_feats)

            flog(
                f"[{tid}] geogit_render_worker: layer={layer_name} "
                f"visible={len(visible_events)} "
                f"entities={len(plan.entities)} "
                f"feats={len(past_feats)+len(arrow_feats)+len(attr_feats)}",
                "DEBUG",
            )

        elapsed_ms = int((time.monotonic() - t0) * 1000)

        import os
        import sys
        mem_mb = 0.0
        try:
            if sys.platform == "win32":
                import ctypes
                from ctypes import wintypes
                kernel32 = ctypes.windll.kernel32
                class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ("cb", wintypes.DWORD),
                        ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t),
                    ]
                pmc = PROCESS_MEMORY_COUNTERS()
                pmc.cb = ctypes.sizeof(pmc)
                handle = kernel32.GetCurrentProcess()
                psapi = ctypes.windll.psapi
                if psapi.GetProcessMemoryInfo(handle, ctypes.byref(pmc), pmc.cb):
                    mem_mb = pmc.WorkingSetSize / (1024 * 1024)
        except Exception:  # noqa: BLE001
            pass

        cache_events = sum(len(v) for v in self._event_cache.values())
        cache_size_kb = sys.getsizeof(self._event_cache) // 1024

        flog(
            f"[{tid}] geogit_render_worker: done "
            f"n_entities={total_entities} n_features={total_features} "
            f"elapsed_ms={elapsed_ms} "
            f"process_rss_mb={mem_mb:.1f} "
            f"cache_events={cache_events} cache_shallow_kb={cache_size_kb}",
            "INFO",
        )
        self.all_done.emit({
            "n_entities": total_entities,
            "n_features": total_features,
            "elapsed_ms": elapsed_ms,
            "process_rss_mb": round(mem_mb, 1),
            "cache_events": cache_events,
        })

    def _reproject_bbox(
        self, bbox_xy: Tuple[float, ...], src_crs: str, dst_crs: str,
    ) -> Tuple[float, ...]:
        if not dst_crs or dst_crs == src_crs:
            return bbox_xy
        try:
            from qgis.core import (
                QgsCoordinateReferenceSystem,
                QgsCoordinateTransform,
                QgsGeometry,
                QgsProject,
                QgsRectangle,
            )
            rect = QgsRectangle(
                bbox_xy[0], bbox_xy[1], bbox_xy[2], bbox_xy[3],
            )
            geom = QgsGeometry.fromRect(rect)
            xform = QgsCoordinateTransform(
                QgsCoordinateReferenceSystem(src_crs),
                QgsCoordinateReferenceSystem(dst_crs),
                QgsProject.instance(),
            )
            geom.transform(xform)
            b = geom.boundingBox()
            return (b.xMinimum(), b.yMinimum(), b.xMaximum(), b.yMaximum())
        except Exception:  # noqa: BLE001
            return bbox_xy

    def _build_past_attrs(
        self, state, classification, now, source_fields, ev_index,
    ) -> list:
        """Build attribute list: source fields + lens metadata.

        For INSERT/DELETE: attributes_json is a full snapshot dict.
        For UPDATE: attributes_json is {"changed_only": {field: {"old":..,"new":..}}}.
        Since we render the OLD geometry, we extract OLD values from deltas.
        """
        from ..core.lens_renderer import _age_label  # noqa: PLC0415

        src_vals = []
        if source_fields:
            flat = {}
            ev = ev_index.get(state.event_id)
            if ev is not None and ev.attributes_json:
                try:
                    parsed = json.loads(ev.attributes_json)
                    if isinstance(parsed, dict):
                        if "changed_only" in parsed:
                            for k, v in parsed["changed_only"].items():
                                if isinstance(v, dict):
                                    flat[k] = v.get("old")
                                else:
                                    flat[k] = v
                        else:
                            flat = parsed
                except (ValueError, TypeError):
                    pass
            for fname in source_fields:
                src_vals.append(flat.get(fname))

        meta = [
            state.event_id,
            state.operation_type,
            state.created_at,
            state.user_name or "",
            _age_label(state.created_at, now),
            classification,
        ]
        return src_vals + meta

    def _filter_by_bbox(self, events: list, bbox_xy) -> list:
        from ..core.wkb_envelope import envelope_intersects, parse_envelope
        visible = []
        for ev in events:
            geom_wkb = ev.geometry_wkb or getattr(ev, "new_geometry_wkb", None)
            if geom_wkb is None:
                visible.append(ev)
                continue
            env = parse_envelope(geom_wkb)
            if envelope_intersects(env, bbox_xy):
                visible.append(ev)
        return visible


__all__ = ["GeoGitRenderWorker"]
