"""Snapshot overlay session — manages 1 memory layer per source layer.

One layer per fingerprint, showing the RECONSTRUCTED STATE at date T.
Single Review mode — no diff layers.

Naming convention: ``__rl_snap_{uid}_geom``  (prefix avoids purge crosstalk
with ``__rl_lens_`` layers managed by purge_lens_overlays).
Group: ``Review Snapshot``.

Lifecycle::

    session = SnapshotOverlaySession()
    session.start(layer_infos, dst_crs, trace_id)
    session.update(snapshot_result)   # call after each reconstruction
    session.stop()
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Dict, List, Optional

from ..compat import clear_flag
from .logger import flog


_SNAP_META_FIELDS = [
    ("_rl_snap_date", "string"),
    ("_rl_snap_op", "string"),
    ("_rl_snap_user", "string"),
]

_GROUP_NAME = "Review Snapshot"


class _SnapOverlay:
    """Tracks 1 QGIS memory layer ID + source metadata per fingerprint."""

    __slots__ = (
        "layer_id", "source_layer_id", "layer_name",
        "fingerprint", "storage_crs",
    )

    def __init__(self):
        self.layer_id: str = ""
        self.source_layer_id: str = ""
        self.layer_name: str = ""
        self.fingerprint: str = ""
        self.storage_crs: str = ""


class SnapshotOverlaySession:
    """One memory layer per source layer showing the state at date T.

    Thread-safety: all methods must be called from the QGIS main thread.
    """

    def __init__(self):
        self._overlays: Dict[str, _SnapOverlay] = {}
        self._layer_info_map: Dict[str, dict] = {}
        self._active: bool = False
        self._dst_crs: str = ""
        self.trace_id: str = ""
        self._update_gen: int = 0

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def n_layers(self) -> int:
        return len(self._overlays)

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def start(
        self,
        layer_infos: List[dict],
        dst_crs: str,
        trace_id: str = "",
    ) -> None:
        """Create one overlay memory layer per source layer.

        layer_infos items: {fingerprint, layer_id, layer_name, storage_crs}.
        """
        if self._active:
            flog(
                f"[{self.trace_id}] snapshot_overlay: start called while active "
                f"— stopping first",
                "WARNING",
            )
            self.stop()

        self.trace_id = trace_id or uuid.uuid4().hex[:8]
        self._dst_crs = dst_crs
        self._layer_info_map = {info["fingerprint"]: info for info in layer_infos}
        self._active = True
        flog(
            f"[{self.trace_id}] snapshot_overlay: started_lazy "
            f"n_layers_registered={len(self._layer_info_map)} dst_crs={dst_crs}",
            "INFO",
        )

    def start_async_create(
        self,
        layer_infos: List[dict],
        dst_crs: str,
        trace_id: str,
        on_done,
    ) -> None:
        """Register + create overlay layers asynchronously (1 per event loop tick).

        Calls on_done() once all layers are created. The UI stays responsive
        because each _create_overlay() call is deferred via QTimer.singleShot(0).
        """
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415

        self.start(layer_infos, dst_crs, trace_id)
        pending = list(layer_infos)
        total = len(pending)
        created = [0]

        # Freeze the canvas for the whole batch. Each addMapLayer() schedules a
        # full re-render of the (heavy) source project; because creation is
        # spread across event-loop ticks, that render actually executes between
        # every layer (~350ms x N), which froze the UI for ~18s on a 23-layer
        # project (recoverland_debug.log session bbd0d574). Freezing collapses
        # those N renders into a single repaint once creation completes.
        canvas = None
        try:
            from qgis.utils import iface  # noqa: PLC0415
            if iface is not None:
                canvas = iface.mapCanvas()
                canvas.freeze(True)
        except Exception:  # noqa: BLE001
            canvas = None

        # Defer legend construction by disabling ShowLegend flag on the
        # layer tree model. This prevents QgsLayerTreeModel from building
        # legend icons synchronously on layer insertion (which triggers
        # network probes for SVG styles, causing 9-11s gaps per layer).
        # The flag is restored after creation, then legends are rebuilt
        # chunked via refreshLayerLegend() to measure the actual cost.
        model = None
        original_flags = None
        try:
            from qgis.utils import iface  # noqa: PLC0415
            from qgis.core import QgsLayerTreeModel  # noqa: PLC0415
            if iface is not None:
                model = iface.layerTreeView().layerTreeModel()
                if model is not None:
                    original_flags = model.flags()
                    # Resolve ShowLegend enum safely across QGIS versions
                    flag_ns = getattr(QgsLayerTreeModel, "Flag", QgsLayerTreeModel)
                    show_legend = getattr(flag_ns, "ShowLegend", None)
                    if show_legend is not None:
                        # clear_flag, not `original_flags & ~show_legend`: on
                        # PyQt6 that expression collapses to a bare int and
                        # setFlags rejects it, which silently disabled this
                        # whole optimisation on QGIS 4 (the TypeError was
                        # swallowed by the except below).
                        model.setFlags(clear_flag(original_flags, show_legend))
                        flog(
                            f"[{trace_id}] snapshot_overlay: legend_defer applied "
                            f"n_layers={total} mode=show_legend_flag",
                            "INFO",
                        )
                    else:
                        flog(
                            f"[{trace_id}] snapshot_overlay: legend_defer_unavailable "
                            f"reason=show_legend_enum_not_found",
                            "WARNING",
                        )
                        model = None
                        original_flags = None
        except Exception as exc:  # noqa: BLE001
            # ERROR, not WARNING: this branch is how a cross-version break in
            # the flag arithmetic stayed invisible for a whole release. The
            # feature degrades silently, so the log line is the only signal.
            flog(
                f"[{trace_id}] snapshot_overlay: legend_defer_unavailable "
                f"reason={type(exc).__name__} msg={exc}",
                "ERROR",
            )
            model = None
            original_flags = None

        def _thaw() -> None:
            if canvas is not None:
                try:
                    canvas.freeze(False)
                except Exception as exc:  # noqa: BLE001
                    # WARNING: the canvas is left frozen, so the map stops
                    # repainting for the rest of the session.
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: thaw failed, "
                        f"canvas may stay frozen ({exc!r})",
                        "WARNING",
                    )
            if model is not None and original_flags is not None:
                try:
                    model.setFlags(original_flags)
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: legend_defer restored",
                        "DEBUG",
                    )
                except Exception as exc:  # noqa: BLE001
                    # WARNING: the legend keeps the deferred flags, so it
                    # stops reflecting later layer changes.
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: legend flag "
                        f"restore failed, legend stays deferred ({exc!r})",
                        "WARNING",
                    )

        last_end = [time.monotonic()]

        def _step() -> None:
            t_begin = time.monotonic()
            queue_gap_ms = int((t_begin - last_end[0]) * 1000)
            if not self._active:
                _thaw()
                flog(
                    f"[{self.trace_id}] snapshot_overlay: async_create "
                    f"aborted (inactive) created={created[0]}/{total}",
                    "WARNING",
                )
                on_done()
                return
            if not pending:
                _thaw()
                flog(
                    f"[{self.trace_id}] snapshot_overlay: async_create done "
                    f"created={created[0]}/{total}",
                    "INFO",
                )
                # Call on_done immediately to allow snapshot data population
                on_done()
                # Start chunked legend refresh independently (does not call on_done)
                if model is not None and original_flags is not None:
                    self._refresh_legends_chunked(trace_id)
                return
            info = pending.pop(0)
            try:
                self._create_overlay(info)
                created[0] += 1
            except Exception as exc:  # noqa: BLE001
                flog(
                    f"[{self.trace_id}] snapshot_overlay: create_overlay_error "
                    f"layer={info.get('layer_name', '?')} error={exc!r}",
                    "ERROR",
                )
                # Continue with remaining layers even if one fails
                # Flag will be restored when pending is empty or on abort
            step_ms = int((time.monotonic() - t_begin) * 1000)
            if step_ms > 500 or queue_gap_ms > 500:
                flog(
                    f"[{self.trace_id}] snapshot_overlay: async_create_step_slow "
                    f"layer={info.get('layer_name', '?')} "
                    f"step_elapsed_ms={step_ms} queue_gap_ms={queue_gap_ms} "
                    f"progress={created[0]}/{total}",
                    "WARNING",
                )
            last_end[0] = time.monotonic()
            QTimer.singleShot(0, _step)

        QTimer.singleShot(0, _step)

    def _refresh_legends_chunked(self, trace_id: str) -> None:
        """Refresh legend nodes for all created overlays, one per tick.

        This measures the actual cost of legend construction (SVG network
        probes) by timing each refreshLayerLegend() call. If the cost is
        high (~10s per layer), it confirms the legend hypothesis; if low,
        the gaps originate elsewhere.
        """
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415
        from qgis.core import QgsProject  # noqa: PLC0415

        # Resolve model/root/group once before the loop
        model = None
        root = None
        group = None
        try:
            from qgis.utils import iface  # noqa: PLC0415
            if iface is not None:
                model = iface.layerTreeView().layerTreeModel()
                if model is not None:
                    root = QgsProject.instance().layerTreeRoot()
                    if root is not None:
                        group = root.findGroup(_GROUP_NAME)
        except Exception as exc:  # noqa: BLE001
            flog(
                f"[{self.trace_id}] snapshot_overlay: legend handles "
                f"unresolved for the refresh probe, per-layer legend timing "
                f"will be missing from the log ({exc!r})",
                "DEBUG",
            )

        # Build layer_id->name dict once
        layer_id_to_name = {ovl.layer_id: ovl.layer_name for ovl in self._overlays.values()}

        # Collect overlay layer IDs in creation order
        overlay_ids = [ovl.layer_id for ovl in self._overlays.values()]
        pending = list(overlay_ids)
        total = len(pending)
        refreshed = [0]
        last_end = [time.monotonic()]

        def _step() -> None:
            t_begin = time.monotonic()
            queue_gap_ms = int((t_begin - last_end[0]) * 1000)
            if not self._active:
                flog(
                    f"[{trace_id}] snapshot_overlay: legend_refresh aborted "
                    f"refreshed={refreshed[0]}/{total}",
                    "WARNING",
                )
                return
            if not pending:
                flog(
                    f"[{trace_id}] snapshot_overlay: legend_refresh done "
                    f"refreshed={refreshed[0]}/{total}",
                    "INFO",
                )
                return
            layer_id = pending.pop(0)
            try:
                if model is not None and group is not None:
                    node = group.findLayer(layer_id)
                    if node is not None:
                        t0 = time.monotonic()
                        model.refreshLayerLegend(node)
                        elapsed_ms = int((time.monotonic() - t0) * 1000)
                        ovl_name = layer_id_to_name.get(layer_id, "unknown")
                        flog(
                            f"[{trace_id}] snapshot_overlay: legend_refresh "
                            f"layer={ovl_name} elapsed_ms={elapsed_ms}",
                            "DEBUG",
                        )
                        if elapsed_ms > 500 or queue_gap_ms > 500:
                            flog(
                                f"[{trace_id}] snapshot_overlay: legend_refresh_slow "
                                f"layer={ovl_name} elapsed_ms={elapsed_ms} "
                                f"queue_gap_ms={queue_gap_ms} progress={refreshed[0]}/{total}",
                                "WARNING",
                            )
                refreshed[0] += 1
            except Exception as exc:  # noqa: BLE001
                flog(
                    f"[{trace_id}] snapshot_overlay: legend_refresh_error "
                    f"layer_id={layer_id[:8]} error={exc!r}",
                    "ERROR",
                )
            last_end[0] = time.monotonic()
            QTimer.singleShot(0, _step)

        QTimer.singleShot(0, _step)

    def update(self, snapshot_result) -> dict:
        """Repopulate overlay layers from SnapshotResult.

        Returns {n_entities, n_features, elapsed_ms}.
        """
        if not self._active:
            flog(
                f"[{self.trace_id}] snapshot_overlay: update called while inactive",
                "WARNING",
            )
            return {"n_entities": 0, "n_features": 0, "elapsed_ms": 0}

        t0 = time.monotonic()
        total_feats = 0
        ds_fps_with_data = set(snapshot_result.features.keys())

        for ds_fp in ds_fps_with_data:
            if ds_fp not in self._overlays:
                info = self._layer_info_map.get(ds_fp)
                if info is not None:
                    self._create_overlay(info)
                else:
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: ds_fp_not_in_map "
                        f"ds_fp={ds_fp[:8]} known={[k[:8] for k in self._layer_info_map]}",
                        "WARNING",
                    )
            ovl = self._overlays.get(ds_fp)
            if ovl is None:
                continue
            entity_feats = snapshot_result.features.get(ds_fp, {})
            n = self._populate_layer(ds_fp, ovl, entity_feats)
            total_feats += n

        for fp, ovl in list(self._overlays.items()):
            if fp not in ds_fps_with_data:
                from qgis.core import QgsProject  # noqa: PLC0415
                lyr = QgsProject.instance().mapLayer(ovl.layer_id)
                if lyr is not None:
                    lyr.dataProvider().truncate()
                    lyr.triggerRepaint()

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        n_ent = snapshot_result.n_entities
        flog(
            f"[{self.trace_id}] snapshot_overlay: updated "
            f"cutoff={snapshot_result.cutoff_dt.isoformat()} "
            f"n_entities={n_ent} n_features={total_feats} "
            f"elapsed_ms={elapsed_ms}",
            "INFO",
        )
        try:
            from qgis.utils import iface  # noqa: PLC0415
            if iface is not None:
                iface.mapCanvas().refresh()
        except Exception as exc:  # noqa: BLE001
            # WARNING: the overlays hold the snapshot but the map still
            # paints the previous cutoff.
            flog(
                f"[{self.trace_id}] snapshot_overlay: canvas refresh failed "
                f"after update, map may show the previous cutoff ({exc!r})",
                "WARNING",
            )
        return {"n_entities": n_ent, "n_features": total_feats, "elapsed_ms": elapsed_ms}

    def update_async(self, snapshot_result, on_done=None) -> None:
        """Async version of update(): 1 layer per event-loop tick.

        Keeps the UI responsive for 20+ layer projects. Uses a generation
        counter so a new date change can cancel the in-progress update.
        on_done(stats_dict) is called when all layers are populated.
        """
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415

        self._update_gen += 1
        gen = self._update_gen
        t0 = time.monotonic()

        ds_fps_with_data = set(snapshot_result.features.keys())
        pending = []
        for ds_fp in ds_fps_with_data:
            if ds_fp not in self._overlays:
                info = self._layer_info_map.get(ds_fp)
                if info is not None:
                    self._create_overlay(info)
            ovl = self._overlays.get(ds_fp)
            if ovl is not None:
                pending.append((ds_fp, snapshot_result.features[ds_fp], ovl))

        fps_to_clear = [fp for fp in self._overlays if fp not in ds_fps_with_data]
        total_feats = [0]
        n_ent = snapshot_result.n_entities

        def _step() -> None:
            if gen != self._update_gen or not self._active:
                return
            if not pending:
                self._clear_stale_overlays(fps_to_clear)
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                flog(
                    f"[{self.trace_id}] snapshot_overlay: updated_async "
                    f"cutoff={snapshot_result.cutoff_dt.isoformat()} "
                    f"n_entities={n_ent} n_features={total_feats[0]} "
                    f"elapsed_ms={elapsed_ms}",
                    "INFO",
                )
                try:
                    from qgis.utils import iface  # noqa: PLC0415
                    if iface is not None:
                        iface.mapCanvas().refresh()
                except Exception as exc:  # noqa: BLE001
                    # WARNING: the overlays hold the snapshot but the map
                    # still paints the previous cutoff.
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: canvas refresh "
                        f"failed after updated_async, map may show the "
                        f"previous cutoff ({exc!r})",
                        "WARNING",
                    )
                if on_done is not None:
                    on_done({"n_entities": n_ent, "n_features": total_feats[0],
                             "elapsed_ms": elapsed_ms})
                return
            ds_fp, entity_feats, ovl = pending.pop(0)
            total_feats[0] += self._populate_layer(ds_fp, ovl, entity_feats)
            QTimer.singleShot(0, _step)

        QTimer.singleShot(0, _step)

    def _clear_stale_overlays(self, fps: list) -> None:
        """Truncate layers that have no features at the current cutoff."""
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        for fp in fps:
            ovl = self._overlays.get(fp)
            if ovl is None:
                continue
            lyr = project.mapLayer(ovl.layer_id)
            if lyr is not None:
                lyr.dataProvider().truncate()
                lyr.triggerRepaint()

    def stop(self) -> None:
        """Remove all overlay layers and clean up the group."""
        if not self._active:
            return
        tid = self.trace_id
        self._active = False
        self._remove_layers()
        n = len(self._overlays)
        self._overlays.clear()
        flog(
            f"[{tid}] snapshot_overlay: stopped freed_layers={n}",
            "INFO",
        )

    # ------------------------------------------------------------------ #
    # Layer creation                                                       #
    # ------------------------------------------------------------------ #

    def _ensure_group(self) -> None:
        from qgis.core import QgsProject  # noqa: PLC0415
        root = QgsProject.instance().layerTreeRoot()
        if root.findGroup(_GROUP_NAME) is None:
            root.insertGroup(0, _GROUP_NAME)

    def _create_overlay(self, info: dict) -> None:
        from qgis.core import QgsProject  # noqa: PLC0415

        t0 = time.monotonic()
        project = QgsProject.instance()
        fp = info["fingerprint"]
        uid = uuid.uuid4().hex[:8]
        source_layer = project.mapLayer(info["layer_id"])
        if source_layer is None:
            flog(
                f"[{self.trace_id}] snapshot_overlay: source_layer missing "
                f"fp={fp[:8]} — skipping",
                "WARNING",
            )
            return

        geom_family = self._detect_geom_family(source_layer) or "Polygon"
        overlay_crs = info.get("storage_crs") or self._dst_crs
        if overlay_crs != info.get("storage_crs", self._dst_crs):
            flog(
                f"[{self.trace_id}] snapshot_overlay: crs_fallback "
                f"layer={info['layer_name']} used={overlay_crs} "
                f"reason=storage_crs_empty",
                "DEBUG",
            )
        snap_lyr = self._build_snap_layer(
            uid, geom_family, overlay_crs, source_layer,
        )
        snap_lyr.setName(f"{info['layer_name']} (snapshot)")
        snap_lyr.setCustomProperty("_rl_snap_managed", "1")
        t_build = time.monotonic()
        self._clone_style_only(snap_lyr, source_layer)
        t_clone = time.monotonic()

        root = project.layerTreeRoot()
        group = root.findGroup(_GROUP_NAME) or root.insertGroup(0, _GROUP_NAME)
        project.addMapLayer(snap_lyr, False)
        idx = self._group_insert_index(group, info["layer_id"])
        group.insertLayer(idx, snap_lyr)
        t_insert = time.monotonic()
        flog(
            f"[{self.trace_id}] snapshot_overlay: insert_at "
            f"idx={idx}/{len(group.children())} layer={info['layer_name']}",
            "DEBUG",
        )

        ovl = _SnapOverlay()
        ovl.layer_id = snap_lyr.id()
        ovl.source_layer_id = info["layer_id"]
        ovl.layer_name = info["layer_name"]
        ovl.fingerprint = fp
        ovl.storage_crs = info.get("storage_crs") or self._dst_crs
        self._overlays[fp] = ovl

        flog(
            f"[{self.trace_id}] snapshot_overlay: created "
            f"layer={info['layer_name']} "
            f"geom={geom_family} uid={uid} "
            f"overlay_crs={overlay_crs} dst_crs={self._dst_crs} "
            f"build_ms={int((t_build - t0) * 1000)} "
            f"clone_ms={int((t_clone - t_build) * 1000)} "
            f"insert_ms={int((t_insert - t_clone) * 1000)}",
            "DEBUG",
        )

    def _build_snap_layer(self, uid, geom_family, dst_crs, source_layer):
        from qgis.core import QgsVectorLayer  # noqa: PLC0415

        uri_parts = [f"{geom_family}?crs={dst_crs}"]
        for field in source_layer.fields():
            uri_parts.append(f"field={field.name()}:{field.typeName()}")
        for fname, ftype in _SNAP_META_FIELDS:
            uri_parts.append(f"field={fname}:{ftype}")
        uri = "&".join(uri_parts)
        lyr = QgsVectorLayer(uri, f"__rl_snap_{uid}_geom", "memory")
        if not lyr.isValid():
            fallback_uri = f"{geom_family}?crs={dst_crs}"
            lyr = QgsVectorLayer(fallback_uri, f"__rl_snap_{uid}_geom", "memory")
            flog(
                f"[{self.trace_id}] snapshot_overlay: "
                f"source_schema_copy_failed, using minimal schema",
                "WARNING",
            )
        return lyr

    # ------------------------------------------------------------------ #
    # Layer population                                                     #
    # ------------------------------------------------------------------ #

    def _populate_layer(self, fp: str, ovl: _SnapOverlay, entity_feats: dict) -> int:
        from qgis.core import QgsFeature, QgsProject  # noqa: PLC0415
        from .geometry_utils import rebuild_geometry  # noqa: PLC0415

        t0 = time.monotonic()
        project = QgsProject.instance()
        lyr = project.mapLayer(ovl.layer_id)
        if lyr is None:
            flog(
                f"[{self.trace_id}] snapshot_overlay: "
                f"overlay_missing layer={ovl.layer_name}",
                "WARNING",
            )
            return 0

        lyr.dataProvider().truncate()
        feats = []
        source_layer = project.mapLayer(ovl.source_layer_id)
        source_fields = source_layer.fields() if source_layer else None

        lyr_geom_type = lyr.geometryType()
        n_skipped = 0
        for entity_fp, snap_feat in entity_feats.items():
            if snap_feat.geom_wkb is None:
                continue
            geom = rebuild_geometry(snap_feat.geom_wkb)
            if geom is None or geom.isNull():
                continue
            if geom.type() != lyr_geom_type:
                n_skipped += 1
                continue
            f = QgsFeature(lyr.fields())
            f.setGeometry(geom)
            if source_fields:
                _set_source_attrs(f, source_fields, snap_feat)
            f.setAttribute("_rl_snap_date", snap_feat.last_created_at)
            f.setAttribute("_rl_snap_op", snap_feat.last_op)
            feats.append(f)

        n_no_geom = sum(
            1 for sf in entity_feats.values() if sf.geom_wkb is None
        )
        n_null_geom = sum(
            1 for sf in entity_feats.values()
            if sf.geom_wkb is not None and rebuild_geometry(sf.geom_wkb) is None
        ) if n_no_geom < len(entity_feats) else 0

        flog(
            f"[{self.trace_id}] snapshot_overlay: populate_layer "
            f"layer={ovl.layer_name} "
            f"n_entities={len(entity_feats)} n_no_geom={n_no_geom} "
            f"n_null_geom={n_null_geom} n_type_skip={n_skipped} "
            f"n_feats_built={len(feats)} "
            f"lyr_crs={lyr.crs().authid() if lyr.crs().isValid() else '?'} "
            f"storage_crs={ovl.storage_crs} "
            f"build_elapsed_ms={int((time.monotonic() - t0) * 1000)}",
            "INFO",
        )
        fc = 0
        lyr.blockSignals(True)
        try:
            if feats:
                ok, added = lyr.dataProvider().addFeatures(feats)
                n_accepted = len(added) if added else 0
                if not ok:
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: addFeatures FAILED "
                        f"layer={ovl.layer_name} n_feats={len(feats)}",
                        "ERROR",
                    )
                elif n_accepted != len(feats):
                    flog(
                        f"[{self.trace_id}] snapshot_overlay: addFeatures "
                        f"partial layer={ovl.layer_name} "
                        f"sent={len(feats)} accepted={n_accepted} "
                        f"lyr_wkb={lyr.wkbType()}",
                        "WARNING",
                    )
            lyr.updateExtents()
            fc = lyr.featureCount()
        finally:
            lyr.blockSignals(False)
        lyr.triggerRepaint()
        flog(
            f"[{self.trace_id}] snapshot_overlay: provider_count "
            f"layer={ovl.layer_name} featureCount={fc} "
            f"lyr_wkb={lyr.wkbType()}",
            "DEBUG",
        )
        return len(feats)

    # ------------------------------------------------------------------ #
    # Cleanup                                                              #
    # ------------------------------------------------------------------ #

    def _remove_layers(self) -> None:
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        root = project.layerTreeRoot()

        for ovl in self._overlays.values():
            if not ovl.layer_id:
                continue
            node = root.findLayer(ovl.layer_id)
            if node is not None:
                node_parent = node.parent()
                if node_parent is not None:
                    node_parent.removeChildNode(node)

        group = root.findGroup(_GROUP_NAME)
        if group is not None:
            group.removeAllChildren()
            parent = group.parent() or root
            parent.removeChildNode(group)
            flog(f"[{self.trace_id}] snapshot_overlay: group_removed name={_GROUP_NAME}", "INFO")

        ids = [ovl.layer_id for ovl in self._overlays.values() if ovl.layer_id]
        if ids:
            project.removeMapLayers(ids)

        try:
            from qgis.utils import iface  # noqa: PLC0415
            if iface is not None:
                iface.mapCanvas().refresh()
        except Exception as exc:  # noqa: BLE001
            # WARNING: the overlay layers are gone from the project but the
            # canvas may keep painting them, which reads as live data.
            flog(
                f"[{self.trace_id}] snapshot_overlay: canvas refresh failed "
                f"after teardown, removed overlays may stay painted ({exc!r})",
                "WARNING",
            )

    def overlay_layer_ids(self) -> List[str]:
        """Return QGIS layer IDs for all current overlay layers."""
        return [ovl.layer_id for ovl in self._overlays.values() if ovl.layer_id]

    def export_to_geopackage(self, output_path: str) -> dict:
        """Export all snapshot overlays to a single GeoPackage file.

        Returns dict: {n_layers, n_features, elapsed_ms, errors}.
        """
        from qgis.core import (  # noqa: PLC0415
            QgsCoordinateTransformContext,
            QgsProject,
            QgsVectorFileWriter,
        )

        t0 = time.monotonic()
        project = QgsProject.instance()
        ctx = QgsCoordinateTransformContext()
        try:
            ctx = project.transformContext()
        except Exception as exc:  # noqa: BLE001
            flog(
                f"[{self.trace_id}] snapshot_overlay: export falls back to an "
                f"empty transform context, project datum shifts will not be "
                f"applied ({exc!r})",
                "WARNING",
            )

        n_layers = 0
        n_features = 0
        errors: List[str] = []
        first = True

        for fp, ovl in self._overlays.items():
            lyr = project.mapLayer(ovl.layer_id)
            if lyr is None or lyr.featureCount() == 0:
                continue

            opts = QgsVectorFileWriter.SaveVectorOptions()
            opts.driverName = "GPKG"
            opts.layerName = ovl.layer_name or fp[:16]
            if first:
                opts.actionOnExistingFile = (
                    QgsVectorFileWriter.ActionOnExistingFile.CreateOrOverwriteFile
                    if hasattr(QgsVectorFileWriter, 'ActionOnExistingFile')
                    else QgsVectorFileWriter.CreateOrOverwriteFile
                )
                first = False
            else:
                opts.actionOnExistingFile = (
                    QgsVectorFileWriter.ActionOnExistingFile.CreateOrOverwriteLayer
                    if hasattr(QgsVectorFileWriter, 'ActionOnExistingFile')
                    else QgsVectorFileWriter.CreateOrOverwriteLayer
                )

            _err, msg, _new_file, _new_layer = (
                QgsVectorFileWriter.writeAsVectorFormatV3(lyr, output_path, ctx, opts)
            )
            _NO_ERR = getattr(
                QgsVectorFileWriter, 'NoError',
                getattr(getattr(QgsVectorFileWriter, 'WriterError', None), 'NoError', 0),
            )
            if _err != _NO_ERR:
                err_msg = f"{ovl.layer_name}: {msg}"
                errors.append(err_msg)
                flog(
                    f"[{self.trace_id}] snapshot_export: layer_error "
                    f"layer={ovl.layer_name} error={msg}",
                    "ERROR",
                )
            else:
                fc = lyr.featureCount()
                n_features += fc
                n_layers += 1

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        flog(
            f"[{self.trace_id}] snapshot_export: done "
            f"path={output_path} n_layers={n_layers} "
            f"n_features={n_features} errors={len(errors)} "
            f"elapsed_ms={elapsed_ms}",
            "INFO",
        )
        return {
            "n_layers": n_layers,
            "n_features": n_features,
            "elapsed_ms": elapsed_ms,
            "errors": errors,
        }

    _SNAP_OPACITY = 0.65

    def _clone_style_only(self, overlay_layer, source_layer) -> None:
        """Clone source renderer via ``renderer().clone()`` + reduce opacity.

        Uses direct C++ clone (no XML round-trip) to avoid segfaults that
        ``QgsMapLayerStyle.writeToLayer()`` causes on certain renderer types.
        Falls back to default renderer if clone fails.
        """
        t0 = time.monotonic()
        cloned = False
        try:
            renderer = source_layer.renderer()
            if renderer is not None:
                overlay_layer.setRenderer(renderer.clone())
                cloned = True
        except (RuntimeError, Exception) as exc:  # noqa: BLE001
            flog(
                f"[{self.trace_id}] snapshot_overlay: renderer_clone_error="
                f"{exc!r} source={source_layer.name()}",
                "WARNING",
            )

        try:
            overlay_layer.setOpacity(self._SNAP_OPACITY)
        except (RuntimeError, Exception) as exc:  # noqa: BLE001
            flog(
                f"[{self.trace_id}] snapshot_overlay: setOpacity_error={exc!r}",
                "WARNING",
            )

        flog(
            f"[{self.trace_id}] snapshot_overlay: style_applied "
            f"source={source_layer.name()} opacity={self._SNAP_OPACITY} "
            f"renderer_cloned={cloned} "
            f"elapsed_ms={int((time.monotonic() - t0) * 1000)}",
            "DEBUG",
        )

    def _source_tree_position(self, source_layer_id: str) -> int:
        """Return position of source layer in the layer tree (0=top)."""
        from qgis.core import QgsProject  # noqa: PLC0415
        root = QgsProject.instance().layerTreeRoot()
        for i, node in enumerate(root.findLayers()):
            if node.layerId() == source_layer_id:
                return i
        return 999999

    def _group_insert_index(self, group, source_layer_id: str) -> int:
        """Compute insertion index in snapshot group to mirror source tree order."""
        new_pos = self._source_tree_position(source_layer_id)
        for i, child in enumerate(group.children()):
            if not hasattr(child, 'layerId'):
                continue
            child_lid = child.layerId()
            for ovl in self._overlays.values():
                if ovl.layer_id == child_lid:
                    if self._source_tree_position(ovl.source_layer_id) > new_pos:
                        return i
                    break
        return len(group.children())

    def _detect_geom_family(self, source_layer) -> Optional[str]:
        """Return geometry URI token from layer wkbType — O(1), no I/O.

        Returns the *Multi* variant when the source uses multi-part
        geometries.  A memory layer created as ``"Point"`` silently
        rejects MultiPoint features from ``addFeatures``, which causes
        massive data loss on common Multi* source layers.

        Resolution order (QGIS 3.40 → 4.x compat):
        1. ``wkbType()`` → raw WKB code → Multi detection + base family.
        2. ``geometryType()`` enum fallback (loses Multi info).
        """
        _BASE = {0: "Point", 1: "LineString", 2: "Polygon"}
        _MULTI = {0: "MultiPoint", 1: "MultiLineString", 2: "MultiPolygon"}
        _WKB_MULTI_CODES = frozenset({4, 5, 6, 11, 12, 1004, 1005, 1006,
                                      2004, 2005, 2006, 3004, 3005, 3006})

        is_multi = False
        try:
            raw_wkb = int(source_layer.wkbType())
            base_code = raw_wkb % 1000
            if base_code in _WKB_MULTI_CODES:
                is_multi = True
        except Exception as exc:  # noqa: BLE001
            # WARNING: losing Multi detection is the data-loss path this
            # method's docstring describes -- a "Point" memory layer drops
            # every MultiPoint feature silently.
            flog(
                f"[{self.trace_id}] snapshot_overlay: wkbType probe failed, "
                f"Multi detection lost for this overlay ({exc!r})",
                "WARNING",
            )

        try:
            gtype = int(source_layer.geometryType())
            family = (_MULTI if is_multi else _BASE).get(gtype)
            if family:
                return family
        except Exception as exc:  # noqa: BLE001
            flog(
                f"[{self.trace_id}] snapshot_overlay: geometryType probe "
                f"failed, trying QgsWkbTypes ({exc!r})",
                "DEBUG",
            )

        try:
            from qgis.core import QgsWkbTypes  # noqa: PLC0415
            gtype = int(QgsWkbTypes.geometryType(source_layer.wkbType()))
            return (_MULTI if is_multi else _BASE).get(gtype)
        except Exception as exc:  # noqa: BLE001
            # WARNING: returning None means no overlay layer for this
            # source, so its snapshot features are simply not shown.
            flog(
                f"[{self.trace_id}] snapshot_overlay: QgsWkbTypes fallback "
                f"failed, no geometry family for this overlay ({exc!r})",
                "WARNING",
            )
        return None


# ------------------------------------------------------------------ #
# Attribute helpers (module-level, no QGIS deps)                      #
# ------------------------------------------------------------------ #


def _set_source_attrs(feat, source_fields, snap_feat) -> None:
    """Populate source field values from SnapshotFeature.attrs_json.

    attrs_json is a flat {field: value} dict produced by
    temporal_snapshot_engine._build_attrs_at_cutoff.
    """
    if not snap_feat.attrs_json:
        return
    try:
        parsed = json.loads(snap_feat.attrs_json)
    except (ValueError, TypeError):
        return
    if not isinstance(parsed, dict):
        return
    for field in source_fields:
        name = field.name()
        if name not in parsed:
            continue
        feat.setAttribute(name, _coerce_field_value(parsed[name], field))


_NUMERIC_TYPE_NAMES = frozenset({
    "real", "double", "float", "integer", "int", "int2", "int4", "int8",
    "long", "bigint", "numeric", "decimal",
})
_DATE_TYPE_NAMES = frozenset({"date", "datetime", "time"})


def _coerce_field_value(val, field):
    """Convert *val* to a type compatible with *field*, returning None on failure."""
    if val is None:
        return None
    if val == "" or val == "NULL" or val == "null":
        return None
    type_name = field.typeName().lower() if hasattr(field, "typeName") else ""
    if type_name in _NUMERIC_TYPE_NAMES:
        try:
            if "int" in type_name or type_name in ("long", "bigint", "int2", "int4", "int8"):
                return int(val)
            return float(val)
        except (ValueError, TypeError):
            return None
    return val


__all__ = ["SnapshotOverlaySession"]
