"""GeoGit real-time session — cache + persistent overlay layers.

Architecture:
    1. start()  → load ALL events for time range into RAM (QThread).
                  Create persistent overlay layers ONCE.
    2. refresh_viewport(bbox) → filter cache by bbox in pure Python,
                  truncate + addFeatures on existing layers (~20-80ms).
    3. on_new_events(events) → append to cache, refresh if visible.
    4. stop()   → remove overlays, free cache.

Memory: ~2 KB/event. 5000 events ≈ 10 MB. 50K ≈ 100 MB.
"""
from __future__ import annotations

import json
import uuid
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .logger import flog
from .wkb_envelope import envelope_intersects, parse_envelope


class _LayerOverlays:
    """Tracks 3 persistent QGIS memory layer IDs per source layer."""
    __slots__ = ("past_id", "arrows_id", "attr_id", "source_layer_id",
                 "layer_name", "fingerprint", "storage_crs",
                 "source_field_names")

    def __init__(self):
        self.past_id: str = ""
        self.arrows_id: str = ""
        self.attr_id: str = ""
        self.source_layer_id: str = ""
        self.layer_name: str = ""
        self.fingerprint: str = ""
        self.storage_crs: str = ""
        self.source_field_names: list = []


class GeoGitSession:
    """Manages one active GeoGit real-time session."""

    def __init__(self):
        self._event_cache: Dict[str, list] = {}
        self._overlays: Dict[str, _LayerOverlays] = {}
        self._active = False
        self._dst_crs: str = ""
        self._t_min: str = ""
        self._t_max: str = ""
        self._op_filter = None
        self.trace_id: str = ""
        self._total_cached = 0

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def total_cached_events(self) -> int:
        return self._total_cached

    @property
    def n_layers(self) -> int:
        return len(self._overlays)

    @property
    def overlay_fingerprints(self) -> list:
        """List of fingerprints with overlays, for incremental iteration."""
        return list(self._overlays.keys())

    def refresh_one_layer(
        self, fp: str, bbox_xy: Tuple[float, ...],
    ) -> dict:
        """Refresh ONE source layer's overlays from cache.

        Returns dict: n_entities, n_features, elapsed_ms, layer_name.
        Designed to be called per QTimer tick so each layer is ~20-80ms.
        """
        from qgis.core import (  # noqa: PLC0415
            QgsFeature, QgsGeometry, QgsProject,
        )
        from .geometry_utils import (  # noqa: PLC0415
            geometries_equal,
            is_geometry_present,
            repair_geometry_for_render,
            reproject_geometry_for_render,
        )
        from .lens_planner import plan_lens_view
        from .lens_contracts import LensFetchStats, LensSelection
        from .lens_renderer import (
            _wkb_type_code, _wkb_family, _detect_past_geom_family,
            _feature_attrs,
        )

        ovl = self._overlays.get(fp)
        if ovl is None:
            return {"n_entities": 0, "n_features": 0, "elapsed_ms": 0,
                    "layer_name": "?"}

        tid = self.trace_id
        t0 = time.monotonic()
        transform_cache = {}
        now = datetime.now(timezone.utc)
        project = QgsProject.instance()

        events = self._event_cache.get(fp, [])
        if not events:
            self._truncate_overlay(ovl)
            return {"n_entities": 0, "n_features": 0,
                    "elapsed_ms": int((time.monotonic() - t0) * 1000),
                    "layer_name": ovl.layer_name}

        layer_bbox = self._reproject_bbox(
            bbox_xy, self._dst_crs, ovl.storage_crs,
        )
        visible_events = self._filter_by_bbox(events, layer_bbox)
        if not visible_events:
            self._truncate_overlay(ovl)
            return {"n_entities": 0, "n_features": 0,
                    "elapsed_ms": int((time.monotonic() - t0) * 1000),
                    "layer_name": ovl.layer_name}

        selection = LensSelection(
            layer_id_snapshot=ovl.source_layer_id,
            datasource_fp=fp,
            bbox_xy=bbox_xy,
            bbox_crs=ovl.storage_crs,
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
        plan = plan_lens_view(
            visible_events, selection, ovl.layer_name, stats,
        )

        past_layer = project.mapLayer(ovl.past_id)
        arrows_layer = project.mapLayer(ovl.arrows_id)
        attr_layer = project.mapLayer(ovl.attr_id)

        if past_layer is None or arrows_layer is None or attr_layer is None:
            flog(
                f"[{tid}] geogit_session: overlay missing "
                f"for {ovl.layer_name}",
                "WARNING",
            )
            return {"n_entities": 0, "n_features": 0,
                    "elapsed_ms": int((time.monotonic() - t0) * 1000),
                    "layer_name": ovl.layer_name}

        past_layer.dataProvider().truncate()
        arrows_layer.dataProvider().truncate()
        attr_layer.dataProvider().truncate()

        geom_family = _detect_past_geom_family(plan, [])
        past_feats = []
        arrow_feats = []
        attr_feats = []

        for entity_fp, timeline in plan.entities.items():
            classification = (
                timeline.classification.name
                if hasattr(timeline.classification, "name")
                else str(timeline.classification)
            )
            # BL-IL-P2-17: condense to 1 past + 1 arrow + 1 marker per entity
            oldest_state = timeline.states[0] if timeline.states else None
            newest_state = timeline.states[-1] if timeline.states else None
            representative = newest_state or oldest_state
            if representative is None:
                continue

            # Past geometry: use oldest state's OLD geom
            if oldest_state and oldest_state.old_geom_wkb and geom_family:
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
                            f = QgsFeature(past_layer.fields())
                            f.setGeometry(repro)
                            f.setAttributes(_feature_attrs(
                                entity_fp, representative,
                                classification, now,
                            ))
                            past_feats.append(f)

            # Arrow: oldest OLD -> newest NEW (if geom moved)
            first_wkb = oldest_state.old_geom_wkb if oldest_state else None
            last_wkb = newest_state.new_geom_wkb if newest_state else None
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
                            arrow = QgsGeometry.fromPolylineXY([old_c, new_c])
                            af = QgsFeature(arrows_layer.fields())
                            af.setGeometry(arrow)
                            af.setAttributes(_feature_attrs(
                                entity_fp, representative,
                                classification, now,
                            ))
                            arrow_feats.append(af)
                        except Exception:  # noqa: BLE001
                            pass

            # Attr marker: latest position if any attrs changed
            has_any_attrs = any(st.attrs_delta for st in timeline.states)
            if has_any_attrs:
                src_wkb = (
                    newest_state.new_geom_wkb
                    or newest_state.old_geom_wkb
                    or (oldest_state.old_geom_wkb if oldest_state else None)
                )
                if src_wkb:
                    mg = repair_geometry_for_render(src_wkb, trace_id=tid)
                    if is_geometry_present(mg):
                        crs = newest_state.crs_authid or (
                            oldest_state.crs_authid if oldest_state else None
                        )
                        mr = reproject_geometry_for_render(
                            mg, crs, self._dst_crs,
                            transform_cache, trace_id=tid,
                        )
                        if is_geometry_present(mr):
                            try:
                                pt = mr.centroid()
                                mf = QgsFeature(attr_layer.fields())
                                mf.setGeometry(pt)
                                mf.setAttributes(_feature_attrs(
                                    entity_fp, representative,
                                    classification, now,
                                ))
                                attr_feats.append(mf)
                            except Exception:  # noqa: BLE001
                                pass

        if past_feats:
            past_layer.dataProvider().addFeatures(past_feats)
        if arrow_feats:
            arrows_layer.dataProvider().addFeatures(arrow_feats)
        if attr_feats:
            attr_layer.dataProvider().addFeatures(attr_feats)
        past_layer.updateExtents()
        arrows_layer.updateExtents()
        attr_layer.updateExtents()
        past_layer.triggerRepaint()
        arrows_layer.triggerRepaint()
        attr_layer.triggerRepaint()

        n_feats = len(past_feats) + len(arrow_feats) + len(attr_feats)
        n_ent = len(plan.entities)
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        flog(
            f"[{tid}] geogit_session: refresh_layer={ovl.layer_name} "
            f"visible_events={len(visible_events)} "
            f"n_entities={n_ent} n_features={n_feats} "
            f"elapsed_ms={elapsed_ms}",
            "INFO",
        )
        return {
            "n_entities": n_ent,
            "n_features": n_feats,
            "elapsed_ms": elapsed_ms,
            "layer_name": ovl.layer_name,
        }

    def stop(self) -> None:
        """Remove all overlay layers and free the event cache."""
        if not self._active:
            return
        tid = self.trace_id
        self._active = False
        self._remove_overlay_layers()
        n_cached = self._total_cached
        self._event_cache.clear()
        self._overlays.clear()
        self._total_cached = 0
        flog(
            f"[{tid}] geogit_session: stopped "
            f"freed_events={n_cached}",
            "INFO",
        )

    def append_events(self, fingerprint: str, new_events: list) -> None:
        """Append new events to cache (e.g. from tracker commit)."""
        if fingerprint in self._event_cache:
            self._event_cache[fingerprint].extend(new_events)
            self._total_cached += len(new_events)

    def _reproject_bbox(
        self, bbox_xy: Tuple[float, ...], src_crs: str, dst_crs: str,
    ) -> Tuple[float, ...]:
        """Reproject bbox from src_crs to dst_crs. Returns original on error."""
        if not dst_crs or dst_crs == src_crs:
            return bbox_xy
        try:
            from qgis.core import (  # noqa: PLC0415
                QgsCoordinateReferenceSystem,
                QgsCoordinateTransform,
                QgsGeometry,
                QgsProject,
                QgsRectangle,
            )
            rect = QgsRectangle(bbox_xy[0], bbox_xy[1], bbox_xy[2], bbox_xy[3])
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

    def _filter_by_bbox(self, events: list, bbox_xy) -> list:
        """Fast Python-only bbox filter using WKB envelope."""
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

    def _ensure_group(self) -> None:
        """Create the GeoGit layer group if it does not exist."""
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        root = project.layerTreeRoot()
        if root.findGroup("GeoGit") is None:
            root.insertGroup(0, "GeoGit")

    def _create_one_overlay(self, info: dict, dst_crs: str) -> None:
        """Create 3 persistent overlay layers for one source layer."""
        from qgis.core import QgsProject  # noqa: PLC0415
        from .lens_renderer import _make_overlay_layer, _apply_source_style

        project = QgsProject.instance()
        root = project.layerTreeRoot()
        group = root.findGroup("GeoGit")
        if group is None:
            group = root.insertGroup(0, "GeoGit")

        fp = info["fingerprint"]
        uid = uuid.uuid4().hex[:8]

        events = self._event_cache.get(fp, [])
        geom_family = self._detect_geom_family(events) or "Polygon"

        layer_name = info["layer_name"]
        source_layer = project.mapLayer(info["layer_id"])

        past_lyr = self._make_past_overlay(
            uid, geom_family, dst_crs, source_layer,
        )
        past_lyr.setName(f"{layer_name} (historique)")
        arrows_lyr = _make_overlay_layer(
            f"__rl_lens_{uid}_arrows", "LineString", dst_crs,
        )
        arrows_lyr.setName(f"{layer_name} (deplacements)")
        attr_lyr = _make_overlay_layer(
            f"__rl_lens_{uid}_attr_markers", "Point", dst_crs,
        )
        attr_lyr.setName(f"{layer_name} (modif. attributs)")

        for lyr in (past_lyr, arrows_lyr, attr_lyr):
            lyr.setCustomProperty("_rl_lens_managed", "1")

        _apply_source_style(past_lyr, source_layer, self.trace_id)

        project.addMapLayer(past_lyr, False)
        project.addMapLayer(arrows_lyr, False)
        project.addMapLayer(attr_lyr, False)
        group.addLayer(past_lyr)
        arrows_node = group.addLayer(arrows_lyr)
        attr_node = group.addLayer(attr_lyr)
        if arrows_node is not None:
            arrows_node.setItemVisibilityChecked(False)
        if attr_node is not None:
            attr_node.setItemVisibilityChecked(False)

        ovl = _LayerOverlays()
        ovl.past_id = past_lyr.id()
        ovl.arrows_id = arrows_lyr.id()
        ovl.attr_id = attr_lyr.id()
        ovl.source_layer_id = info["layer_id"]
        ovl.layer_name = info["layer_name"]
        ovl.fingerprint = fp
        ovl.storage_crs = info["storage_crs"]
        if source_layer is not None:
            ovl.source_field_names = [
                f.name() for f in source_layer.fields()
            ]
        self._overlays[fp] = ovl

    def _make_past_overlay(self, uid, geom_family, dst_crs, source_layer):
        """Create geom_past overlay with source schema + lens metadata."""
        from qgis.core import QgsField, QgsFields, QgsVectorLayer  # noqa: PLC0415
        from qgis.PyQt.QtCore import QVariant  # noqa: PLC0415

        _LENS_META = [
            ("_rl_event_id", QVariant.Int),
            ("_rl_op", QVariant.String),
            ("_rl_date", QVariant.String),
            ("_rl_user", QVariant.String),
            ("_rl_age", QVariant.String),
            ("_rl_class", QVariant.String),
        ]

        uri_parts = [f"{geom_family}?crs={dst_crs}"]
        if source_layer is not None:
            for field in source_layer.fields():
                uri_parts.append(
                    f"field={field.name()}:{field.typeName()}"
                )
        for fname, _ in _LENS_META:
            uri_parts.append(f"field={fname}:string")

        uri = "&".join(uri_parts)
        lyr = QgsVectorLayer(uri, f"__rl_lens_{uid}_geom_past", "memory")

        if not lyr.isValid() and source_layer is not None:
            from .lens_renderer import _make_overlay_layer  # noqa: PLC0415
            lyr = _make_overlay_layer(
                f"__rl_lens_{uid}_geom_past", geom_family, dst_crs,
            )
            flog(
                f"[{self.trace_id}] geogit_session: "
                f"source_schema_copy_failed, using lens schema",
                "WARNING",
            )

        return lyr

    def _detect_geom_family(self, events: list) -> Optional[str]:
        """Detect geometry family from cached events."""
        from .lens_renderer import _wkb_type_code, _wkb_family
        for ev in events:
            wkb = ev.geometry_wkb
            if wkb:
                code = _wkb_type_code(wkb)
                if code is not None:
                    return _wkb_family(code)
        return None

    def _truncate_overlay(self, ovl: _LayerOverlays) -> None:
        """Clear features from an overlay's 3 layers."""
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        for lid in (ovl.past_id, ovl.arrows_id, ovl.attr_id):
            lyr = project.mapLayer(lid)
            if lyr is not None:
                lyr.dataProvider().truncate()
                lyr.triggerRepaint()

    def _remove_overlay_layers(self) -> None:
        """Remove all overlay layers from the project in bulk."""
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        ids = []
        for ovl in self._overlays.values():
            ids.extend([ovl.past_id, ovl.arrows_id, ovl.attr_id])
        if ids:
            project.removeMapLayers(ids)
        root = project.layerTreeRoot()
        group = root.findGroup("GeoGit")
        if group is not None and len(group.children()) == 0:
            root.removeChildNode(group)


__all__ = ["GeoGitSession"]
