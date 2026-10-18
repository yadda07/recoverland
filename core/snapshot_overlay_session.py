"""Snapshot overlay session - manages 1 memory layer per source layer.

One layer per fingerprint, showing the RECONSTRUCTED STATE at date T.
Single Review mode - no diff layers.

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
    ("_rl_etat_incomplet", "string"),
]

# Set, on an overlay or exported feature, to the reasons its state at T is
# not entirely known (SnapshotFeature.unresolved); NULL when it is. A field
# left empty by an unknown value must never read as "NULL at that date".
INCOMPLETE_FIELD = "_rl_etat_incomplet"

_GROUP_NAME = "Review Snapshot"
# Marks the group THIS plugin created, so a user group that happens to
# carry the same name is never emptied nor removed.
_GROUP_MARKER = "_rl_snap_group"
_MANAGED_PROP = "_rl_snap_managed"
_LAYER_PREFIX = "__rl_snap_"

# Name of the table every export writes next to the layers: what was
# exported, at which date, and what was NOT (a GeoPackage read later must
# not pass for "the data at T").
_EXPORT_INFO_TABLE = "recoverland_etat_info"


_SNAP_META_FIELD_NAMES = frozenset(n for n, _t in _SNAP_META_FIELDS)


def _is_managed_layer(layer_id: str, layer) -> bool:
    """True when *layer* is a Review memory overlay (ours or a legacy orphan)."""
    if layer is None:
        return False
    try:
        if layer.providerType() != "memory":
            return False
        prop = layer.customProperty(_MANAGED_PROP)
        if prop == layer_id or prop == "1":
            return True
        return (layer_id.startswith(_LAYER_PREFIX)
                or layer.name().startswith(_LAYER_PREFIX))
    except RuntimeError:
        return False


def _crs_from_spec(spec: str):
    """Build a QgsCoordinateReferenceSystem from an authid or WKT string."""
    from qgis.core import QgsCoordinateReferenceSystem  # noqa: PLC0415
    if not spec:
        return QgsCoordinateReferenceSystem()
    crs = QgsCoordinateReferenceSystem(spec)
    if crs.isValid():
        return crs
    return QgsCoordinateReferenceSystem.fromWkt(spec)


def _overlay_source_fields(lyr) -> list:
    """Non-meta QgsFields carried by an overlay memory layer."""
    try:
        return [fld for fld in lyr.fields()
                if fld.name() not in _SNAP_META_FIELD_NAMES]
    except RuntimeError:
        return []


def _strip_overlay_refs(doc, layer_ids: set) -> int:
    """Remove every element of a project DOM that references an overlay.

    Called from QgsProject.writeProject, which fires AFTER the layer tree
    and the snapping settings were serialised: their overlay entries (and
    the Review group once empty) are cut here. Overlay ids all start with
    ``__rl_snap_`` and cannot collide with user data.
    """
    removed = [0]

    def _references(el) -> bool:
        attrs = el.attributes()
        for i in range(attrs.count()):
            if attrs.item(i).nodeValue() in layer_ids:
                return True
        if el.tagName() == "maplayer":
            return el.firstChildElement("id").text() in layer_ids
        return el.firstChildElement().isNull() and el.text() in layer_ids

    def _walk(parent) -> None:
        child = parent.firstChildElement()
        while not child.isNull():
            nxt = child.nextSiblingElement()
            if _references(child):
                parent.removeChild(child)
                removed[0] += 1
            else:
                _walk(child)
                if (child.tagName() == "layer-tree-group"
                        and child.attribute("name") == _GROUP_NAME
                        and child.firstChildElement("layer-tree-layer").isNull()
                        and child.firstChildElement("layer-tree-group").isNull()):
                    parent.removeChild(child)
                    removed[0] += 1
            child = nxt

    if layer_ids:
        _walk(doc.documentElement())
    return removed[0]


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
        # Generation of the last update_async that ran to its end: while it
        # differs from _update_gen the overlays mix two dates.
        self._update_done_gen: int = 0
        # Overlays taken out of the project while it is being saved.
        self._taken_for_save: list = []
        self._write_hooked: bool = False

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def n_layers(self) -> int:
        return len(self._overlays)

    @property
    def is_updating(self) -> bool:
        """True while an update_async is still repainting the overlays."""
        return self._update_done_gen != self._update_gen

    # ------------------------------------------------------------------ #
    # Keeping the overlays out of the user's project                       #
    # ------------------------------------------------------------------ #

    def _hook_project_write(self, on: bool) -> None:
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        if on and not self._write_hooked:
            project.writeProject.connect(self._on_project_write)
            self._write_hooked = True
        elif not on and self._write_hooked:
            try:
                project.writeProject.disconnect(self._on_project_write)
            except (TypeError, RuntimeError) as exc:
                flog(f"[{self.trace_id}] snapshot_overlay: writeProject "
                     f"disconnect skipped ({exc!r})", "DEBUG")
            self._write_hooked = False

    def _resolve_overlay_layer(self, fp: str, ovl: _SnapOverlay):
        """Live memory overlay for *ovl*, or None if missing / made permanent."""
        from qgis.core import QgsProject  # noqa: PLC0415
        lyr = QgsProject.instance().mapLayer(ovl.layer_id)
        if lyr is None:
            return None
        if _is_managed_layer(ovl.layer_id, lyr):
            return lyr
        self._detach_converted_overlay(ovl, lyr)
        return None

    def _detach_converted_overlay(self, ovl: _SnapOverlay, lyr) -> None:
        """Release an overlay the user made permanent so Review never touches it."""
        from qgis.core import QgsProject  # noqa: PLC0415
        try:
            lyr.removeCustomProperty(_MANAGED_PROP)
            lyr.setReadOnly(False)
        except RuntimeError:
            pass
        try:
            root = QgsProject.instance().layerTreeRoot()
            group = self._own_group()
            if root is not None and group is not None:
                node = group.findLayer(ovl.layer_id)
                if node is not None:
                    idx = list(root.children()).index(group)
                    root.insertChildNode(idx, node.clone())
                    group.removeChildNode(node)
        except (RuntimeError, ValueError):
            pass
        self._overlays.pop(ovl.fingerprint, None)
        flog(
            f"[{self.trace_id}] snapshot_overlay: detached_permanent_overlay "
            f"layer={ovl.layer_name} id={ovl.layer_id}",
            "INFO",
        )

    def _on_project_write(self, doc) -> None:
        """QgsProject.writeProject: the overlays never reach the saved file.

        Review is on while the user saves (Ctrl+S, or the save prompt on
        exit that runs BEFORE plugins unload). The overlays used to be
        written as "<name> (snapshot)" layers that reopened empty, looking
        like real data, forever. Two halves: what is already serialised
        (layer tree, snapping) is stripped from *doc*; the registry and the
        layer order, written right after this signal, no longer contain
        the overlays because they are taken out of the project for the
        duration of the save, then put back with their features.
        """
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        live_ovls = []
        for fp, ovl in list(self._overlays.items()):
            lyr = self._resolve_overlay_layer(fp, ovl)
            if lyr is not None:
                live_ovls.append((ovl, lyr))
        ids = {ovl.layer_id for ovl, _lyr in live_ovls}
        n_dom = _strip_overlay_refs(doc, ids)
        taken = []
        for ovl, lyr in live_ovls:
            got = project.takeMapLayer(lyr)
            if got is not None:
                taken.append((ovl, got))
        self._taken_for_save.extend(taken)
        flog(
            f"[{self.trace_id}] snapshot_overlay: kept_out_of_project_save "
            f"n_dom_removed={n_dom} n_layers_taken={len(taken)}",
            "INFO",
        )
        if taken:
            QTimer.singleShot(0, self._restore_after_save)

    def _restore_after_save(self) -> None:
        """Put the overlays taken for the save back, dirty flag untouched."""
        from qgis.core import QgsProject  # noqa: PLC0415
        taken, self._taken_for_save = self._taken_for_save, []
        if not taken or not self._active:
            return
        project = QgsProject.instance()
        was_dirty = project.isDirty()
        group = self._own_group(create=True)
        for ovl, lyr in taken:
            project.addMapLayer(lyr, False)
            idx = self._group_insert_index(group, ovl.source_layer_id)
            group.insertLayer(idx, lyr)
        if not was_dirty:
            # The save just cleared the flag; putting our own overlays back
            # is not a user change and must not re-arm the save prompt.
            project.setDirty(False)
        flog(
            f"[{self.trace_id}] snapshot_overlay: restored_after_save "
            f"n_layers={len(taken)} dirty={project.isDirty()}",
            "INFO",
        )

    def _own_group(self, create: bool = False):
        """The layer-tree group this plugin created, or None."""
        from qgis.core import QgsLayerTree, QgsProject  # noqa: PLC0415
        root = QgsProject.instance().layerTreeRoot()
        for child in root.children():
            if (QgsLayerTree.isGroup(child) and child.name() == _GROUP_NAME
                    and child.customProperty(_GROUP_MARKER) == "1"):
                return child
        if not create:
            return None
        group = root.insertGroup(0, _GROUP_NAME)
        group.setCustomProperty(_GROUP_MARKER, "1")
        return group

    def _purge_orphans(self) -> int:
        """Remove overlays left by an earlier session or a saved project.

        A project saved by an older version reopens with "(snapshot)"
        layers and a "Review Snapshot" group; the previous stop() then
        emptied the group but left those layers registered and invisible,
        saved again at every save. Only layers carrying the overlay marks
        are touched, and only empty groups of that name are removed.
        """
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        own = set(self.overlay_layer_ids())
        ids = [lid for lid, lyr in project.mapLayers().items()
               if lid not in own and _is_managed_layer(lid, lyr)]
        if ids:
            project.removeMapLayers(ids)
        n_groups = self._remove_review_groups(only_empty=True)
        if ids or n_groups:
            flog(
                f"[{self.trace_id}] snapshot_overlay: orphans_purged "
                f"n_layers={len(ids)} n_groups={n_groups}",
                "WARNING",
            )
        return len(ids)

    def _remove_review_groups(self, only_empty: bool) -> int:
        """Remove the Review groups: ours always (unless *only_empty*), any
        other group of that name only when it is empty."""
        from qgis.core import QgsLayerTree, QgsProject  # noqa: PLC0415
        root = QgsProject.instance().layerTreeRoot()
        removed = 0
        for child in list(root.children()):
            if not QgsLayerTree.isGroup(child) or child.name() != _GROUP_NAME:
                continue
            ours = child.customProperty(_GROUP_MARKER) == "1"
            empty = not child.children()
            if empty or (ours and not only_empty):
                if not empty:
                    # Our group, but the user dropped layers of his own in
                    # it: keep them, at the group's place.
                    idx = root.children().index(child)
                    for node in list(child.children()):
                        root.insertChildNode(idx, node.clone())
                        idx += 1
                root.removeChildNode(child)
                removed += 1
        return removed

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
                f"- stopping first",
                "WARNING",
            )
            self.stop()

        self.trace_id = trace_id or uuid.uuid4().hex[:8]
        self._dst_crs = dst_crs
        self._layer_info_map = {info["fingerprint"]: info for info in layer_infos}
        self._active = True
        self._purge_orphans()
        # A new session proves its evidence again (_CarrierWatch).
        _CARRIERS.close()
        self._hook_project_write(True)
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
                        group = self._own_group()
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
            ovl = self._overlays.get(ds_fp)
            if ovl is None or self._resolve_overlay_layer(ds_fp, ovl) is None:
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
                lyr = self._resolve_overlay_layer(fp, ovl)
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
        layer_jobs = []
        for ds_fp in ds_fps_with_data:
            ovl = self._overlays.get(ds_fp)
            if ovl is None or self._resolve_overlay_layer(ds_fp, ovl) is None:
                info = self._layer_info_map.get(ds_fp)
                if info is not None:
                    self._create_overlay(info)
            ovl = self._overlays.get(ds_fp)
            if ovl is not None:
                layer_jobs.append(self._populate_layer_steps(
                    ds_fp, ovl, snapshot_result.features[ds_fp]))

        fps_to_clear = [fp for fp in list(self._overlays) if fp not in ds_fps_with_data]
        total_feats = [0]
        n_ent = snapshot_result.n_entities

        def _step() -> None:
            if gen != self._update_gen or not self._active:
                return
            t_turn = time.monotonic()
            while layer_jobs:
                try:
                    next(layer_jobs[0])
                except StopIteration as stop:
                    total_feats[0] += int(stop.value or 0)
                    layer_jobs.pop(0)
                if (time.monotonic() - t_turn) * 1000.0 >= _COMPLETION_TURN_MS:
                    break
            if layer_jobs:
                QTimer.singleShot(0, _step)
                return
            self._clear_stale_overlays(fps_to_clear)
            self._update_done_gen = gen
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
                flog(
                    f"[{self.trace_id}] snapshot_overlay: canvas refresh "
                    f"failed after updated_async, map may show the "
                    f"previous cutoff ({exc!r})",
                    "WARNING",
                )
            if on_done is not None:
                on_done({"n_entities": n_ent, "n_features": total_feats[0],
                         "elapsed_ms": elapsed_ms})

        QTimer.singleShot(0, _step)

    def _clear_stale_overlays(self, fps: list) -> None:
        """Truncate layers that have no features at the current cutoff."""
        for fp in fps:
            ovl = self._overlays.get(fp)
            if ovl is None:
                continue
            lyr = self._resolve_overlay_layer(fp, ovl)
            if lyr is not None:
                lyr.dataProvider().truncate()
                lyr.triggerRepaint()

    def clear_all(self) -> None:
        """Empty every overlay now and cancel any update in progress.

        Used when the state at the requested date could not be computed:
        leaving the previous date painted would present it as the answer.
        """
        self._update_gen += 1
        self._update_done_gen = self._update_gen
        for fp, ovl in list(self._overlays.items()):
            lyr = self._resolve_overlay_layer(fp, ovl)
            if lyr is not None:
                lyr.dataProvider().truncate()
                lyr.triggerRepaint()
        flog(f"[{self.trace_id}] snapshot_overlay: cleared "
             f"n_layers={len(self._overlays)}", "INFO")

    def stop(self) -> None:
        """Remove all overlay layers and clean up the group."""
        if not self._active:
            return
        tid = self.trace_id
        self._active = False
        self._update_gen += 1
        self._update_done_gen = self._update_gen
        self._hook_project_write(False)
        # Overlays taken out for a save still in progress: dropping the
        # references deletes them, they were never given back.
        self._taken_for_save = []
        _CARRIERS.close()
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
        self._own_group(create=True)

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
                f"fp={fp[:8]} - skipping",
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
        snap_lyr.setCustomProperty(_MANAGED_PROP, snap_lyr.id())
        # Read-only for the user, not for us: provider writes (truncate,
        # addFeatures) still work. The tracker ignores this layer and every
        # refresh truncates it, so an edit made here would be lost without
        # a trace; restoring goes through Restore / Rewind.
        snap_lyr.setReadOnly(True)
        t_build = time.monotonic()
        self._clone_style_only(snap_lyr, source_layer)
        t_clone = time.monotonic()

        group = self._own_group(create=True)
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
        """Memory layer with the SOURCE schema, types included.

        The schema used to be spelled into the memory URI from each field's
        typeName: provider names the memory provider does not know
        ("Integer64" of a GeoPackage fid, among others) silently became
        strings, and a GeoPackage export then failed on a text "fid". The
        QgsField objects are copied instead; a field the memory provider
        refuses is skipped and logged, never the whole schema.
        """
        from qgis.core import QgsField, QgsVectorLayer  # noqa: PLC0415

        name = f"__rl_snap_{uid}_geom"
        crs_obj = _crs_from_spec(dst_crs)
        if not crs_obj.isValid() and source_layer is not None and source_layer.crs().isValid():
            crs_obj = source_layer.crs()
        uri_crs = crs_obj.authid() if (crs_obj.isValid() and crs_obj.authid()) else ""
        uri = f"{geom_family}?crs={uri_crs}" if uri_crs else geom_family
        lyr = QgsVectorLayer(uri, name, "memory")
        if crs_obj.isValid():
            lyr.setCrs(crs_obj)
        meta = QgsVectorLayer(
            "None?" + "&".join(f"field={n}:{t}" for n, t in _SNAP_META_FIELDS),
            "meta", "memory")
        provider = lyr.dataProvider()
        skipped = []
        for field in list(source_layer.fields()) + list(meta.fields()):
            if not provider.addAttributes([QgsField(field)]):
                skipped.append(field.name())
        lyr.updateFields()
        if skipped:
            flog(
                f"[{self.trace_id}] snapshot_overlay: "
                f"source_schema_copy_partial skipped_fields={skipped}",
                "WARNING",
            )
        return lyr

    # ------------------------------------------------------------------ #
    # Layer population                                                     #
    # ------------------------------------------------------------------ #

    def _populate_layer(self, fp: str, ovl: _SnapOverlay, entity_feats: dict) -> int:
        return _run_through(self._populate_layer_steps(fp, ovl, entity_feats))

    def _populate_layer_steps(self, fp: str, ovl: _SnapOverlay, entity_feats: dict):
        from qgis.core import QgsProject  # noqa: PLC0415

        t0 = time.monotonic()
        project = QgsProject.instance()
        lyr = self._resolve_overlay_layer(fp, ovl)
        if lyr is None:
            info = self._layer_info_map.get(fp)
            if info is not None:
                self._create_overlay(info)
                ovl = self._overlays.get(fp)
                lyr = self._resolve_overlay_layer(fp, ovl) if ovl is not None else None
        if lyr is None:
            flog(
                f"[{self.trace_id}] snapshot_overlay: "
                f"overlay_missing layer={ovl.layer_name if ovl else '?'}",
                "WARNING",
            )
            return 0

        source_layer = project.mapLayer(ovl.source_layer_id)
        if source_layer is not None and source_layer.crs().isValid():
            cur_crs = source_layer.crs()
            cur_spec = cur_crs.authid() or cur_crs.toWkt()
            if lyr.crs() != cur_crs:
                lyr.setCrs(cur_crs)
            ovl.storage_crs = cur_spec
            if fp in self._layer_info_map:
                self._layer_info_map[fp]["storage_crs"] = cur_spec

        lyr.dataProvider().truncate()
        source_fields = (source_layer.fields() if source_layer is not None
                         else _overlay_source_fields(lyr))
        items = list(entity_feats.items())
        n_no_geom = 0
        n_null_at_t = 0
        n_corrupt_geom = 0
        n_skipped = 0
        total_built = 0
        total_accepted = 0

        lyr.blockSignals(True)
        try:
            for start_idx in range(0, len(items), _LIVE_BATCH):
                if start_idx > 0:
                    lyr.blockSignals(False)
                    yield
                    lyr = self._resolve_overlay_layer(fp, ovl)
                    if lyr is None:
                        return total_built
                    lyr.blockSignals(True)
                chunk = dict(items[start_idx:start_idx + _LIVE_BATCH])
                for sf in chunk.values():
                    if sf.geom_wkb is None:
                        n_no_geom += 1
                    elif len(sf.geom_wkb) == 0:
                        n_null_at_t += 1
                feats, build_stats = _build_features(lyr, source_fields, chunk)
                n_corrupt_geom += build_stats["geometry_undecodable"]
                n_skipped += build_stats["type_skipped"]
                total_built += len(feats)
                if feats:
                    ok, added = lyr.dataProvider().addFeatures(feats)
                    n_acc = len(added) if added else 0
                    total_accepted += n_acc
                    if not ok:
                        flog(
                            f"[{self.trace_id}] snapshot_overlay: addFeatures FAILED "
                            f"layer={ovl.layer_name} n_feats={len(feats)}",
                            "ERROR",
                        )
            lyr.updateExtents()
            fc = lyr.featureCount()
        finally:
            lyr.blockSignals(False)

        if total_built and total_accepted != total_built:
            flog(
                f"[{self.trace_id}] snapshot_overlay: addFeatures "
                f"partial layer={ovl.layer_name} "
                f"sent={total_built} accepted={total_accepted} "
                f"lyr_wkb={lyr.wkbType()}",
                "WARNING",
            )
        flog(
            f"[{self.trace_id}] snapshot_overlay: populate_layer "
            f"layer={ovl.layer_name} "
            f"n_entities={len(entity_feats)} n_no_geom={n_no_geom} "
            f"n_null_geom_at_t={n_null_at_t} "
            f"n_corrupt_geom={n_corrupt_geom} n_type_skip={n_skipped} "
            f"n_feats_built={total_built} "
            f"lyr_crs={_crs_key(lyr) or '?'} "
            f"storage_crs={ovl.storage_crs} "
            f"build_elapsed_ms={int((time.monotonic() - t0) * 1000)}",
            "INFO" if not n_corrupt_geom else "WARNING",
        )
        lyr.triggerRepaint()
        flog(
            f"[{self.trace_id}] snapshot_overlay: provider_count "
            f"layer={ovl.layer_name} featureCount={fc} "
            f"lyr_wkb={lyr.wkbType()}",
            "DEBUG",
        )
        return total_built

    # ------------------------------------------------------------------ #
    # Cleanup                                                              #
    # ------------------------------------------------------------------ #

    def _remove_layers(self) -> None:
        """Remove every overlay from the project, then our group."""
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()

        for fp, ovl in list(self._overlays.items()):
            self._resolve_overlay_layer(fp, ovl)
        own = {ovl.layer_id for ovl in self._overlays.values() if ovl.layer_id}
        ids = [lid for lid, lyr in project.mapLayers().items()
               if _is_managed_layer(lid, lyr)]
        if ids:
            project.removeMapLayers(ids)

        n_groups = self._remove_review_groups(only_empty=False)
        if n_groups:
            flog(f"[{self.trace_id}] snapshot_overlay: group_removed "
                 f"name={_GROUP_NAME} n={n_groups}", "INFO")
        orphans = [lid for lid in ids if lid not in own]
        if orphans:
            flog(f"[{self.trace_id}] snapshot_overlay: orphan overlays "
                 f"removed n={len(orphans)}", "WARNING")

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

    def export_to_geopackage(self, output_path: str, snapshot_result=None,
                             cutoff_iso: str = "") -> dict:
        """Export the state at T of the entities changed since T.

        What the file holds is written in the file itself, in the
        ``recoverland_etat_info`` table, and returned in ``scope``: the
        entities that differ from today, in their state at T, over the
        WHOLE extent (``snapshot_result`` is the full reconstruction, not
        the overlays, which only hold the current view). Entities unchanged
        since T are not included; they are identical to the current layers.
        The old export serialised the overlays and announced "Snapshot
        exporte", a file that was then kept as "the data at T" while it
        held a few features of the current view.

        Without ``snapshot_result`` the overlays are exported as they are
        and ``scope`` says it is the current view only.

        An entity that had no geometry at T is written without geometry
        (``n_null_geometry``), never counted as not written.

        An entity whose state at T is only partly known and whose geometry
        is known is written WITH the column ``_rl_etat_incomplet`` holding
        why (``n_unresolved_written``): a field its unknown value left empty
        must never read as "NULL at that date". Every entity not written is
        counted under its cause: geometry unknown at T, undecodable
        geometry, geometry of another type than the layer, source layer
        gone from the project, write refused or not read back.

        Changes since T that no entity at T can be tied to (fids renumbered
        by a deletion, ``n_identity_unproven``) are not in the
        reconstruction: they are counted apart, in the result and in the
        file, never folded into "unchanged, identical to the current
        layers".

        Counts are read back from the written file, never assumed.
        Returns dict: {n_layers, n_features, n_expected, n_not_written,
        n_geometry_unknown, n_geometry_undecodable, n_type_skipped,
        n_layer_missing, n_write_failed, n_unresolved, n_unresolved_written,
        n_trace_ref_missing, n_identity_unproven, n_born_after,
        n_null_geometry, scope, cutoff, layers, elapsed_ms, errors}.
        """
        from qgis.core import (  # noqa: PLC0415
            QgsCoordinateTransformContext,
            QgsProject,
            QgsVectorFileWriter,
            QgsVectorLayer,
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

        scope = ("changed_since_cutoff_all_extent" if snapshot_result is not None
                 else "overlays_current_view")
        sources = self._export_sources(snapshot_result)
        n_expected = sum(src[2] for src in sources)
        omitted = {"geometry_unknown": 0, "geometry_undecodable": 0,
                   "type_skipped": 0, "layer_missing": 0}
        for src in sources:
            for cause in omitted:
                omitted[cause] += src[4].get(cause, 0)
        n_unresolved_written = 0

        n_layers = 0
        n_features = 0
        errors: List[str] = []
        layers_out: List[dict] = []
        used_names: set = set()
        first = True
        _NO_ERR = getattr(
            QgsVectorFileWriter, 'NoError',
            getattr(getattr(QgsVectorFileWriter, 'WriterError', None), 'NoError', 0),
        )

        for base_name, lyr, _n_entities, fp, _stats in sources:
            if lyr is None or lyr.featureCount() == 0:
                continue
            name = _unique_layer_name(base_name or fp[:16], used_names)
            opts = QgsVectorFileWriter.SaveVectorOptions()
            opts.driverName = "GPKG"
            opts.layerName = name
            # A dedicated FID column: the source's own "fid" stays a plain
            # value of the state at T, whatever it holds (NULL included).
            opts.layerOptions = ["FID=rl_fid"]
            # Scoped form only: ActionOnExistingFile is present on Qt5
            # (QGIS 3.40.15) and Qt6, same value 0. Measured, not assumed.
            opts.actionOnExistingFile = (
                QgsVectorFileWriter.ActionOnExistingFile.CreateOrOverwriteFile
                if first else
                QgsVectorFileWriter.ActionOnExistingFile.CreateOrOverwriteLayer
            )
            _err, msg, _new_file, _new_layer = (
                QgsVectorFileWriter.writeAsVectorFormatV3(lyr, output_path, ctx, opts)
            )
            if _err != _NO_ERR:
                errors.append(f"{base_name}: {msg}")
                flog(
                    f"[{self.trace_id}] snapshot_export: layer_error "
                    f"layer={base_name} error={msg}",
                    "ERROR",
                )
                continue
            first = False
            written = QgsVectorLayer(f"{output_path}|layername={name}", name, "ogr")
            fc = written.featureCount() if written.isValid() else 0
            if fc != lyr.featureCount():
                errors.append(f"{base_name}: {lyr.featureCount()} envoyee(s), "
                              f"{fc} relue(s)")
            elif fc:
                null_error = self._write_null_geometries(lyr, written, base_name)
                if null_error:
                    errors.append(null_error)
                n_unresolved_written += _flagged_count(lyr)
            del written
            n_features += fc
            n_layers += 1
            layers_out.append({"name": name, "source": base_name,
                               "fingerprint": fp, "n_features": fc})

        n_not_written = max(0, n_expected - n_features)
        stats = {
            "n_not_written": n_not_written,
            "n_geometry_unknown": omitted["geometry_unknown"],
            "n_geometry_undecodable": omitted["geometry_undecodable"],
            "n_type_skipped": omitted["type_skipped"],
            "n_layer_missing": omitted["layer_missing"],
            # Refused by the writer, or sent and not read back.
            "n_write_failed": max(0, n_not_written - sum(omitted.values())),
            "n_unresolved": (getattr(snapshot_result, "n_unresolved", 0)
                             if snapshot_result else 0),
            "n_unresolved_written": n_unresolved_written,
            "n_trace_ref_missing": (getattr(snapshot_result, "n_trace_ref_missing", 0)
                                    if snapshot_result else 0),
            # Changed since T, their record at T not identifiable (fids
            # renumbered by a deletion): never in the reconstruction, so
            # neither in n_expected nor written, and not identical to the
            # current layers either.
            "n_identity_unproven": (getattr(snapshot_result, "n_identity_unproven", 0)
                                    if snapshot_result else 0),
            "n_born_after": (getattr(snapshot_result, "n_born_after", 0)
                             if snapshot_result else 0),
            "n_null_geometry": _null_geometry_count(snapshot_result),
        }
        info_ok = False
        if n_layers:
            info_ok = self._write_export_info(
                output_path, ctx, snapshot_result, cutoff_iso, scope,
                n_expected, n_features, layers_out, stats)
            if not info_ok:
                errors.append(f"{_EXPORT_INFO_TABLE}: table non ecrite")

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        flog(
            f"[{self.trace_id}] snapshot_export: done "
            f"path={output_path} scope={scope} cutoff={cutoff_iso} "
            f"n_layers={n_layers} n_features={n_features} "
            f"n_expected={n_expected} "
            + " ".join(f"{k}={v}" for k, v in stats.items())
            + f" info_table={info_ok} errors={len(errors)} "
            f"elapsed_ms={elapsed_ms}",
            "INFO" if not errors and not stats["n_unresolved"]
            and not n_not_written and not stats["n_identity_unproven"]
            else "WARNING",
        )
        out = {
            "n_layers": n_layers,
            "n_features": n_features,
            "n_expected": n_expected,
            "scope": scope,
            "cutoff": cutoff_iso,
            "layers": layers_out,
            "elapsed_ms": elapsed_ms,
            "errors": errors,
        }
        out.update(stats)
        return out

    def _write_null_geometries(self, src_lyr, written, base_name: str) -> str:
        """Give back NULL to the entities that had no geometry at T.

        QgsVectorFileWriter stores a feature without geometry as an EMPTY
        one (``POINT EMPTY``, a typed geometry with no coordinate): another
        state, which a re-import would carry. The GeoPackage FIDs follow
        the write order, which is the memory layer's order; the pairing is
        checked (same count, and the written side is empty where the
        source had none) before anything is changed. Returns "" or what
        could not be done, for the caller's error list.
        """
        from qgis.core import QgsGeometry  # noqa: PLC0415
        ranks = [i for i, f in enumerate(src_lyr.getFeatures())
                 if not f.hasGeometry()]
        if not ranks:
            return ""
        out = list(written.getFeatures())
        changes = {}
        for i in ranks:
            geom = out[i].geometry() if i < len(out) else None
            if geom is None or not (geom.isNull() or geom.isEmpty()):
                flog(f"[{self.trace_id}] snapshot_export: null geometry "
                     f"pairing failed layer={base_name} rank={i}", "ERROR")
                return (f"{base_name}: {len(ranks)} entite(s) sans geometrie a "
                        f"cette date ecrite(s) comme geometrie vide")
            changes[out[i].id()] = QgsGeometry()
        if not written.dataProvider().changeGeometryValues(changes):
            flog(f"[{self.trace_id}] snapshot_export: null geometry write "
                 f"refused layer={base_name} n={len(changes)}", "ERROR")
            return (f"{base_name}: {len(changes)} entite(s) sans geometrie a "
                    f"cette date ecrite(s) comme geometrie vide")
        return ""

    def _export_sources(self, snapshot_result) -> list:
        """[(layer name, memory layer to write, n entities, fingerprint,
        {cause: n entities not built})].

        From the full reconstruction when given: one detached memory layer
        per source, built like an overlay but never added to the project.
        """
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        out = []
        if snapshot_result is None:
            for fp, ovl in self._overlays.items():
                out.append((ovl.layer_name, project.mapLayer(ovl.layer_id),
                            0, fp, {}))
            return out
        for fp, entity_feats in snapshot_result.features.items():
            info = self._layer_info_map.get(fp) or {}
            source_layer = project.mapLayer(info.get("layer_id", ""))
            name = info.get("layer_name") or fp[:16]
            if source_layer is None:
                flog(f"[{self.trace_id}] snapshot_export: source layer "
                     f"missing layer={name}, {len(entity_feats)} entite(s) "
                     f"not exported", "WARNING")
                out.append((name, None, len(entity_feats), fp,
                            {"layer_missing": len(entity_feats)}))
                continue
            geom_family = self._detect_geom_family(source_layer) or "Polygon"
            crs = (_crs_key(source_layer)
                   or info.get("storage_crs") or self._dst_crs)
            lyr = self._build_snap_layer(uuid.uuid4().hex[:8], geom_family,
                                         crs, source_layer)
            feats, stats = _build_features(lyr, source_layer.fields(), entity_feats)
            if feats:
                lyr.dataProvider().addFeatures(feats)
            if any(stats.values()):
                flog(f"[{self.trace_id}] snapshot_export: layer={name} "
                     f"not_built={stats}", "WARNING")
            out.append((name, lyr, len(entity_feats), fp, stats))
        return out

    def _write_export_info(self, output_path, ctx, snapshot_result, cutoff_iso,
                           scope, n_expected, n_written, layers_out,
                           stats=None) -> bool:
        """Write the table that says what the GeoPackage is, and is not."""
        from datetime import datetime, timezone  # noqa: PLC0415
        from qgis.core import (  # noqa: PLC0415
            QgsFeature,
            QgsVectorFileWriter,
            QgsVectorLayer,
        )
        rows = [
            ("date_etat_utc", cutoff_iso or "?"),
            ("perimetre", (
                "entites modifiees depuis cette date, dans leur etat a cette "
                "date ; les entites non modifiees depuis ne sont pas incluses "
                "(elles sont identiques aux couches actuelles)"
                if scope != "overlays_current_view" else
                "entites modifiees depuis cette date, dans la vue courante "
                "seulement")),
            ("emprise", "toute l'emprise" if scope != "overlays_current_view"
             else "vue courante de la carte"),
            ("entites_reconstituees", str(n_expected)),
            ("entites_ecrites", str(n_written)),
            ("entites_sans_etat_complet",
             str(getattr(snapshot_result, "n_unresolved", 0) if snapshot_result else 0)),
            ("entites_sans_geometrie_a_cette_date_ecrites_sans_geometrie",
             str(_null_geometry_count(snapshot_result))),
            ("entites_creees_apres_la_date_non_incluses",
             str(getattr(snapshot_result, "n_born_after", 0) if snapshot_result else 0)),
            ("reconstruction_partielle",
             str(bool(getattr(snapshot_result, "partial", False)))),
        ]
        stats = stats or {}
        rows += [
            # Written with _rl_etat_incomplet: an empty field there is an
            # unknown value, not NULL at that date.
            ("entites_ecrites_etat_incomplet_colonne_" + INCOMPLETE_FIELD,
             str(stats.get("n_unresolved_written", 0))),
            ("entites_trace_source_purgee", str(stats.get("n_trace_ref_missing", 0))),
            # Modified since this date, record at this date not identifiable
            # (Shapefile, GeoJSON: fids renumbered by a deletion).
            ("entites_modifiees_identite_non_prouvee_non_ecrites",
             str(stats.get("n_identity_unproven", 0))),
            ("non_ecrites_geometrie_inconnue_a_cette_date",
             str(stats.get("n_geometry_unknown", 0))),
            ("non_ecrites_geometrie_illisible", str(stats.get("n_geometry_undecodable", 0))),
            ("non_ecrites_type_de_geometrie_different_de_la_couche",
             str(stats.get("n_type_skipped", 0))),
            ("non_ecrites_couche_source_absente", str(stats.get("n_layer_missing", 0))),
            ("non_ecrites_ecriture_refusee_ou_non_relue", str(stats.get("n_write_failed", 0))),
            ("genere_le_utc", datetime.now(timezone.utc).isoformat(timespec="seconds")),
        ]
        for lo in layers_out:
            rows.append((f"couche:{lo['name']}",
                         f"source={lo['source']} entites={lo['n_features']}"))
        table = QgsVectorLayer("None?field=cle:string&field=valeur:string",
                               _EXPORT_INFO_TABLE, "memory")
        feats = []
        for key, value in rows:
            f = QgsFeature(table.fields())
            f.setAttributes([key, value])
            feats.append(f)
        table.dataProvider().addFeatures(feats)
        opts = QgsVectorFileWriter.SaveVectorOptions()
        opts.driverName = "GPKG"
        opts.layerName = _EXPORT_INFO_TABLE
        opts.actionOnExistingFile = (
            QgsVectorFileWriter.ActionOnExistingFile.CreateOrOverwriteLayer
        )
        err, msg, _f, _l = QgsVectorFileWriter.writeAsVectorFormatV3(
            table, output_path, ctx, opts)
        _NO_ERR = getattr(
            QgsVectorFileWriter, 'NoError',
            getattr(getattr(QgsVectorFileWriter, 'WriterError', None), 'NoError', 0),
        )
        if err != _NO_ERR:
            flog(f"[{self.trace_id}] snapshot_export: info table not written "
                 f"({msg})", "ERROR")
            return False
        return True

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
        """Return geometry URI token from layer wkbType - O(1), no I/O.

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


def _build_features(lyr, source_fields, entity_feats: dict) -> tuple:
    """QgsFeatures of *lyr* for the entities whose geometry at T is known.

    Returns (features, {cause: n entities not built}). An entity whose
    geometry at T is unknown (None) is not built here: it is counted
    (``geometry_unknown``), never drawn somewhere it was not. An entity
    that had NO geometry at T (GEOMETRY_NULL_MARKER) is built WITHOUT
    geometry: no shape on the map, its attributes at T in the table and
    the export. It used to be dropped as an undecodable blob, and the
    export then called it "not reconstructable" while its state at T was
    fully known. A built entity whose state at T is partly unknown carries
    the reasons in ``_rl_etat_incomplet`` (INCOMPLETE_FIELD).
    """
    from qgis.core import QgsFeature  # noqa: PLC0415
    from .geometry_utils import rebuild_geometry  # noqa: PLC0415
    from .temporal_snapshot_engine import has_null_geometry  # noqa: PLC0415

    feats = []
    lyr_geom_type = lyr.geometryType()
    stats = {"geometry_unknown": 0, "geometry_undecodable": 0,
             "type_skipped": 0}
    for snap_feat in entity_feats.values():
        if snap_feat.geom_wkb is None:
            stats["geometry_unknown"] += 1
            continue
        geom = None
        if not has_null_geometry(snap_feat):
            geom = rebuild_geometry(snap_feat.geom_wkb)
            if geom is None or geom.isNull():
                stats["geometry_undecodable"] += 1
                continue
            if geom.type() != lyr_geom_type:
                stats["type_skipped"] += 1
                continue
        f = QgsFeature(lyr.fields())
        if geom is not None:
            f.setGeometry(geom)
        eff_fields = source_fields if source_fields is not None else _overlay_source_fields(lyr)
        drifted = _set_source_attrs(f, eff_fields, snap_feat)
        unresolved = snap_feat.unresolved or ""
        if drifted and "schema_drift" not in unresolved.split(","):
            unresolved = ",".join(r for r in (unresolved, "schema_drift") if r)
        if not eff_fields and "live_layer_missing" not in unresolved.split(","):
            unresolved = ",".join(r for r in (unresolved, "live_layer_missing") if r)
        f.setAttribute("_rl_snap_date", snap_feat.last_created_at)
        f.setAttribute("_rl_snap_op", snap_feat.last_op)
        f.setAttribute(INCOMPLETE_FIELD, unresolved or None)
        feats.append(f)
    return feats, stats


def _flagged_count(lyr) -> int:
    """Features of *lyr* whose state at T is flagged incomplete."""
    idx = lyr.fields().indexFromName(INCOMPLETE_FIELD)
    if idx < 0:
        return 0
    n = 0
    for f in lyr.getFeatures():
        value = f.attribute(idx)
        if value is not None and str(value) not in ("", "NULL"):
            n += 1
    return n


def _null_geometry_count(snapshot_result) -> int:
    """Entities of *snapshot_result* that had no geometry at T."""
    if snapshot_result is None:
        return 0
    from .temporal_snapshot_engine import count_null_geometry  # noqa: PLC0415
    return count_null_geometry(snapshot_result.features)


def _unique_layer_name(base: str, used: set) -> str:
    """GeoPackage layer name, unique within one export.

    Two source layers routinely share a display name ("parcelles" from two
    files); with CreateOrOverwriteLayer the second silently replaced the
    first while both were counted.
    """
    name = base
    n = 2
    while name.lower() in used:
        name = f"{base}_{n}"
        n += 1
    used.add(name.lower())
    return name


# Longest stretch, in ms, one turn of a `LiveCompletion` holds the GUI
# thread (plus one unit at most: a geometry placed, one bounded pass over
# the carriers of a layer, or one batch of live features read back).
_COMPLETION_TURN_MS = 40

# Live features read back per provider request (`_fetch_live_steps`), and
# live features merged per unit: 2000 of them in one unit held the GUI
# thread ~0.5 s.
_LIVE_BATCH = 200

# The CRS stories the Review settled by its own evidence, per layer and
# capture CRS (geometry_utils._story_key), for the session: read with the
# ones the writes settled, never recorded for them (ReadOnlyStories). A
# relabel or a reprojection happened to the layer, not to one date: a date
# looked at later follows what an earlier one proved, as the writes follow
# what an earlier write proved. Dropped for a layer whose source or CRS is
# no longer the one they were settled under (ReadOnlyStories.forget_stale).
_REVIEW_STORIES: dict = {}


class _CarrierWatch:
    """The carriers the Review proved (geometry_utils.CarrierMemo), and what
    makes it forget them.

    Every Review date asked the provider again, for each edited geometry,
    which committed features still carry a captured position: up to four
    queries per geometry, at every date, about features nobody had touched
    in between. What a complete read proved is now kept across dates, and
    forgotten -- for every layer at once -- as soon as the committed data
    may have changed: a commit on ANY vector layer of the project (another
    layer on the same file writes the same features), a reload
    (dataChanged), a new CRS or source on a layer, the project cleared,
    Review started or stopped (SnapshotOverlaySession.start / stop). A
    layer added to the project is watched from then on. A file changed by
    another program is not seen, as for the dates the dialog keeps
    (_review_snap_cache); a reload of the layer makes it seen.
    """

    _SIGNALS = ("committedFeaturesAdded", "committedFeaturesRemoved",
                "committedGeometriesChanges", "afterCommitChanges",
                "dataChanged", "crsChanged", "dataSourceChanged")

    def __init__(self):
        self._memo = None
        self._project = None
        # layer id -> [(signal name, slot)]; the layer itself is not held,
        # it is looked up in the project to be let go.
        self._watched: dict = {}

    def memo(self):
        """The memo, every vector layer of the project watched."""
        from qgis.core import QgsProject  # noqa: PLC0415
        from .geometry_utils import CarrierMemo  # noqa: PLC0415
        if self._memo is None:
            self._memo = CarrierMemo()
        project = QgsProject.instance()
        if self._project is not project:
            self._unhook_project()
            project.layersAdded.connect(self._watch)
            project.cleared.connect(self._on_cleared)
            self._project = project
        self._watch(project.mapLayers().values())
        return self._memo

    def forget(self, reason: str) -> None:
        memo = self._memo
        if memo is None:
            return
        if len(memo):
            flog(f"review: crs_carriers forgotten n={len(memo)} "
                 f"reason={reason}", "INFO")
        memo.forget()

    def close(self) -> None:
        """Forget everything and stop watching (Review started or stopped)."""
        self.forget("review_session")
        for layer_id in list(self._watched):
            self._unwatch(layer_id)
        self._unhook_project()

    def _watch(self, layers) -> None:
        from qgis.core import QgsVectorLayer  # noqa: PLC0415
        for layer in layers:
            try:
                if not isinstance(layer, QgsVectorLayer):
                    continue
                layer_id = layer.id()
            except RuntimeError:
                continue
            if layer_id in self._watched or _is_managed_layer(layer_id, layer):
                continue  # a Review overlay holds copies, never the data
            slots = []
            for name in self._SIGNALS:
                def slot(*_args, _name=name, _id=layer_id):
                    self.forget(f"{_name} layer={_id}")
                getattr(layer, name).connect(slot)
                slots.append((name, slot))

            def gone(*_args, _id=layer_id):
                self._watched.pop(_id, None)
            layer.willBeDeleted.connect(gone)
            slots.append(("willBeDeleted", gone))
            self._watched[layer_id] = slots

    def _unwatch(self, layer_id: str) -> None:
        slots = self._watched.pop(layer_id, ())
        layer = (self._project.mapLayer(layer_id)
                 if self._project is not None else None)
        if layer is None:
            return  # gone: its connections went with it
        for name, slot in slots:
            try:
                getattr(layer, name).disconnect(slot)
            except (AttributeError, RuntimeError, TypeError):
                pass

    def _on_cleared(self) -> None:
        self.forget("project_cleared")
        self._watched.clear()

    def _unhook_project(self) -> None:
        project, self._project = self._project, None
        if project is None:
            return
        for signal, slot in ((project.layersAdded, self._watch),
                             (project.cleared, self._on_cleared)):
            try:
                signal.disconnect(slot)
            except (RuntimeError, TypeError):
                pass


_CARRIERS = _CarrierWatch()


class ReviewLayerChanged(RuntimeError):
    """A layer a completion reads was removed, or changed CRS, while it ran."""


def _crs_key(layer) -> str:
    try:
        crs = layer.crs()
        return crs.authid() or crs.toWkt()
    except (AttributeError, RuntimeError):
        return ""


class _LayerWatch:
    """The source layers of one completion, looked up again at every unit.

    Between two turns of a `LiveCompletion` the user may remove a layer or
    set its CRS: a wrapper kept from an earlier turn would point at a
    deleted layer, and the geometries of one date would be placed half in
    one CRS, half in another. A layer present when the completion started
    must still be there, under the same CRS, at every unit
    (`ReviewLayerChanged` otherwise). One absent from the start stays
    absent, as it always was (its geometries are left as the journal gives
    them, its live features "live_layer_missing").
    """

    def __init__(self, project, info_by_fp: dict):
        self._project = project
        self._info = info_by_fp
        self._crs: dict = {}
        for ds_fp, info in info_by_fp.items():
            layer = project.mapLayer(info.get("layer_id", ""))
            key = _crs_key(layer) if layer is not None else ""
            if layer is not None and key:
                self._crs[ds_fp] = key

    def now(self, ds_fp: str):
        """The layer of *ds_fp* as it is now, None if absent from the start."""
        if ds_fp not in self._crs:
            return None
        info = self._info.get(ds_fp) or {}
        name = info.get("layer_name") or info.get("layer_id", "?")
        layer = self._project.mapLayer(info.get("layer_id", ""))
        if layer is None:
            raise ReviewLayerChanged(
                f"la couche {name!r} a été retirée du projet pendant la "
                f"reconstruction")
        key = _crs_key(layer)
        if key != self._crs[ds_fp]:
            raise ReviewLayerChanged(
                f"le SCR de la couche {name!r} a changé pendant la "
                f"reconstruction ({self._crs[ds_fp][:40]} -> {key[:40] or '?'})")
        return layer


def _run_through(steps):
    """Run a unit generator to its end; its return value."""
    while True:
        try:
            next(steps)
        except StopIteration as stop:
            return stop.value


def complete_from_live(result, layer_infos: list, trace_id: str = ""):
    """Complete a reconstruction with the live state (GUI thread only).

    A geometry or a field that no event gives never changed since T, so
    its value at T is the one the feature holds now. The live feature is
    read from the PROVIDER (committed state, not an edit buffer) and is
    used only when its identity is PROVEN (see _fetch_live): a feature
    that merely sits at a fid on a layer whose fids move is not proof of
    anything. What cannot be completed is flagged ``unresolved`` and
    counted, never guessed.

    First, every geometry the JOURNAL gives is placed in the layer CRS by
    the Rewind's verdict (_align_steps): the overlays and the export are in
    that CRS, and a past state is drawn where a rewind would put it back,
    or not at all.

    In one go, for the callers that need the answer now. The Review dialog
    runs the same units across event-loop turns (`LiveCompletion`).
    """
    return _run_through(_completion_steps(result, layer_infos, trace_id))


def _completion_steps(result, layer_infos: list, trace_id: str = "",
                      progress: Optional[dict] = None):
    """The work of `complete_from_live`, one unit per ``yield``.

    A unit is one journal geometry placed by the CRS verdict (it reads the
    provider for every witness reading: up to four queries), or the live
    features of one layer read back. The completed result is the return
    value. The layers are taken as they are NOW, when the completion is
    created, not at its first turn, and looked up again at every unit
    (`_LayerWatch`).
    """
    from qgis.core import QgsProject  # noqa: PLC0415
    info_by_fp = {info["fingerprint"]: info for info in layer_infos}
    layers = _LayerWatch(QgsProject.instance(), info_by_fp)
    return _completion_units(result, info_by_fp, layers, trace_id, progress)


def _completion_units(result, info_by_fp: dict, layers, trace_id: str,
                      progress: Optional[dict]):
    import json as _json  # noqa: PLC0415
    from .temporal_snapshot_engine import recount  # noqa: PLC0415

    if result is None or not result.features:
        return result
    result = yield from _align_steps(result, info_by_fp, layers, trace_id,
                                     progress)
    features: dict = {}
    n_needed = 0
    n_done = 0
    for ds_fp, ents in result.features.items():
        cur_layer = layers.now(ds_fp)
        live_field_names = (
            {cur_layer.fields().at(i).name()
             for i in range(cur_layer.fields().count())}
            if cur_layer is not None else None
        )
        needing: dict = {}
        for idx, (k, f) in enumerate(ents.items(), 1):
            if idx % _LIVE_BATCH == 0:
                yield
            if f.geom_from_live or f.attrs_from_live:
                needing[k] = f
        if not needing:
            features[ds_fp] = _flag_layer_or_schema_gaps(
                ents, cur_layer is None, live_field_names, _json)
            continue
        n_needed += len(needing)
        yield
        live, reasons = yield from _fetch_live_steps(
            lambda fp=ds_fp: layers.now(fp), needing)
        updated = dict(ents)
        for n_merged, (key, feat) in enumerate(needing.items(), 1):
            if n_merged % _LIVE_BATCH == 0:
                yield
            lf = live.get(key)
            if lf is None:
                reason = reasons.get(key, "live_feature_not_found")
                updated[key] = feat._replace(
                    geom_from_live=False, attrs_from_live=False,
                    unresolved=",".join(r for r in (feat.unresolved, reason) if r),
                )
                continue
            updated[key] = _merge_live(feat, lf, _json)
            n_done += 1
        features[ds_fp] = _flag_layer_or_schema_gaps(
            updated, cur_layer is None, live_field_names, _json)
    yield
    completed = recount(result._replace(features=features))
    flog(
        f"[{trace_id}] review: live_completion n_needed={n_needed} "
        f"n_completed={n_done} n_unresolved={completed.n_unresolved} "
        f"n_trace_ref_missing={completed.n_trace_ref_missing}",
        "INFO" if n_done == n_needed else "WARNING",
    )
    return completed


class LiveCompletion:
    """`complete_from_live` across event-loop turns (GUI thread).

    The completion of a Review date placed every journal geometry through
    the CRS verdict, which reads the provider for each witness reading, in
    ONE call on the GUI thread: 2000 entities captured under another CRS
    than the layer's held QGIS 3.2 s on a local GeoPackage, again at every
    new date, and minutes on a remote provider (users kill QGIS then, and
    lose their unsaved edits). The same units (`_completion_steps`) now run
    a few at a time, a turn ending once it has held the thread
    ``turn_ms``: the map redraws and the clicks are handled between two
    turns, while the date bar keeps its loading state.

    ``cancel()`` (a new date, a commit, Review stopped) ends it before its
    next turn: nothing is delivered after it. ``on_done(result)`` receives
    the completed result; ``on_failed(message)`` the reason it could not be
    completed (a layer removed or re-declared while it ran, an error);
    ``on_progress(done, total)``, after every turn, how many of the
    geometries to place are placed.
    """

    def __init__(self, result, layer_infos: list, trace_id: str = "",
                 on_done=None, on_failed=None, on_progress=None,
                 turn_ms: float = _COMPLETION_TURN_MS):
        self.trace_id = trace_id
        # idle -> running -> done | failed | cancelled
        self.state = "idle"
        self.n_turns = 0
        self.n_units = 0
        self.max_turn_ms = 0
        self._turn_ms = turn_ms
        self.progress = {"done": 0, "total": 0}
        self._steps = _completion_steps(result, layer_infos, trace_id,
                                        self.progress)
        self._on_done = on_done
        self._on_failed = on_failed
        self._on_progress = on_progress
        self._t0 = 0.0

    @property
    def active(self) -> bool:
        return self.state == "running"

    def start(self) -> None:
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415
        if self.state != "idle":
            return
        self.state = "running"
        self._t0 = time.monotonic()
        QTimer.singleShot(0, self._turn)

    def cancel(self, reason: str = "") -> None:
        if self.state != "running":
            return
        self.state = "cancelled"
        try:
            self._steps.close()
        except ValueError:
            # Only if called from inside a unit, which nothing does.
            pass
        flog(f"[{self.trace_id}] review: live_completion cancelled "
             f"reason={reason or '-'} n_units={self.n_units} "
             f"n_turns={self.n_turns}", "INFO")

    def _turn(self) -> None:
        from qgis.PyQt.QtCore import QTimer  # noqa: PLC0415
        if self.state != "running":
            return
        t0 = time.monotonic()
        self.n_turns += 1
        try:
            while True:
                next(self._steps)
                self.n_units += 1
                if (time.monotonic() - t0) * 1000.0 >= self._turn_ms:
                    break
        except StopIteration as stop:
            self._turn_end(t0)
            self.state = "done"
            flog(f"[{self.trace_id}] review: live_completion done "
                 f"n_units={self.n_units} n_turns={self.n_turns} "
                 f"max_turn_ms={self.max_turn_ms} elapsed_ms="
                 f"{int((time.monotonic() - self._t0) * 1000)}", "INFO")
            if self._on_done is not None:
                self._on_done(stop.value)
            return
        except ReviewLayerChanged as exc:
            self._turn_end(t0)
            self._fail(str(exc), "WARNING")
            return
        except Exception as exc:  # noqa: BLE001 - named to the user, not lost
            import traceback  # noqa: PLC0415
            self._turn_end(t0)
            flog(f"[{self.trace_id}] review: live_completion error "
                 f"{traceback.format_exc()[-1500:]!r}", "ERROR")
            self._fail(f"{type(exc).__name__}: {exc}", "ERROR")
            return
        self._turn_end(t0)
        if self._on_progress is not None:
            self._on_progress(self.progress["done"], self.progress["total"])
        QTimer.singleShot(0, self._turn)

    def _turn_end(self, t0: float) -> None:
        self.max_turn_ms = max(self.max_turn_ms,
                               int((time.monotonic() - t0) * 1000))

    def _fail(self, message: str, level: str) -> None:
        self.state = "failed"
        flog(f"[{self.trace_id}] review: live_completion failed "
             f"reason={message!r} n_units={self.n_units} "
             f"n_turns={self.n_turns}", level)
        if self._on_failed is not None:
            self._on_failed(message)


def _needs_placing(feat, target_crs: str) -> bool:
    """A journal geometry captured under another CRS than *target_crs*."""
    src = getattr(feat, "geom_crs_authid", None)
    return bool(src and feat.geom_wkb and not feat.geom_from_live
                and src != target_crs)


def _align_to_layer_crs(result, info_by_fp: dict, project, trace_id: str = ""):
    """`_align_steps` in one go (the first part of a completion)."""
    return _run_through(_align_steps(
        result, info_by_fp, _LayerWatch(project, info_by_fp), trace_id))


def _align_steps(result, info_by_fp: dict, layers, trace_id: str = "",
                 progress: Optional[dict] = None):
    """*result* with its journal geometries placed in the layer CRS (units).

    The overlays (and the export) are memory layers in the layer's CRS, and
    the journal holds the RAW coordinates of the capture, labelled with the
    CRS the layer declared then (``geom_crs_authid``). They were drawn as
    they are, whatever happened since: after a reprojection of the file in
    place, a past state landed where the numbers of the other CRS fall
    (hundreds or thousands of km away), and where the evidence cannot tell
    a relabel from a reprojection it was drawn anyway, while the Rewind
    refuses to write it. Now the Rewind's verdict decides
    (geometry_utils.crs_verdict, same witnesses: the captured geometries of
    the event that gives the geometry):
      * relabel, same or equivalent CRS: raw, untouched;
      * reprojection: the reprojected geometry;
      * crs_ambiguous / transform failed: no geometry (not drawn, not
        exported), the reason (``crs_ambiguous`` / ``crs_mismatch``,
        geometry_utils.crs_refusal) in ``unresolved``, counted like every
        unknown part of a state at T.
    Only geometries a journal event gives: a live one is already in the
    layer CRS. A capture made in the CRS the layer still declares costs
    nothing (compared by id, no QGIS work).

    The verdicts read the CRS stories the writes settled and record none
    (geometry_utils.ReadOnlyStories, passed explicitly: nothing is swapped
    in the module the writes read). What the Review's own evidence settles
    is kept for the session (`_REVIEW_STORIES`), so an entity without
    evidence of its own follows the evidence of another entity of its
    layer, at this date or an earlier one, and what a date shows never
    decides what a restore writes.

    The evidence (the committed features still carrying a captured
    position) is read per layer in bounded passes, ONE per unit
    (geometry_utils.CaptureCarriers.load_steps), not in up to four provider
    queries per geometry: the same features, compared the same way, so the
    same decisions. What a complete read proved is kept across dates
    (`_CarrierWatch`) until the committed data may have changed: the next
    date reads only what it has not seen. Units: one geometry registered,
    one pass (or one cut of the readings, no read), one geometry placed
    (`_completion_steps`); *progress* (``done`` / ``total`` geometries
    placed) kept up to date; the completed result is the return value.
    """
    from .geometry_utils import (  # noqa: PLC0415
        CRS_REFUSED_STATUSES, CaptureCarriers, ReadOnlyStories, crs_refusal,
        crs_verdict, geometry_to_wkb, rebuild_geometry,
    )
    from .temporal_snapshot_engine import recount  # noqa: PLC0415

    # What has to be placed, per layer, before anything is: the total the
    # date bar shows while the date is being completed. Without its layer
    # (removed from the project during Review) no verdict can be taken: a
    # geometry captured under another CRS than the overlay's is refused
    # instead of being drawn there raw, where nothing proves it was.
    plan = []
    for ds_fp, ents in result.features.items():
        info = info_by_fp.get(ds_fp) or {}
        layer = layers.now(ds_fp)
        layer_crs = ""
        if layer is not None:
            try:
                layer_crs = _crs_key(layer)
            except (AttributeError, RuntimeError):
                layer = None
        overlay_crs = "" if layer is not None else (info.get("storage_crs") or "")
        target = layer_crs if layer is not None else overlay_crs
        keys = []
        for idx, (key, feat) in enumerate(ents.items(), 1):
            if idx % _LIVE_BATCH == 0:
                yield
            if (layer is not None or overlay_crs) and _needs_placing(feat, target):
                keys.append(key)
        plan.append((ds_fp, ents, info, layer is not None, target, keys))
    if progress is not None:
        progress["total"] = sum(len(keys) for *_rest, keys in plan)

    stories = ReadOnlyStories(_REVIEW_STORIES)
    features: dict = {}
    counts = {"checked": 0, "reprojected": 0, "refused": 0}
    for ds_fp, ents, info, has_layer, target, keys in plan:
        updated = None
        decoded: dict = {}
        carriers = None
        if has_layer and keys:
            layer = layers.now(ds_fp)
            dropped = stories.forget_stale(layer)
            if dropped:
                flog(f"[{trace_id}] review: crs_story dropped layer="
                     f"{info.get('layer_name')!r} n={dropped} (settled under "
                     f"another source or CRS than the layer has now)", "INFO")
            carriers = CaptureCarriers(layer, memo=_CARRIERS.memo())
            for key in keys:
                feat = ents[key]
                geom = rebuild_geometry(feat.geom_wkb)
                if geom is None or geom.isNull():
                    continue  # undecodable: counted where the layer is built
                decoded[key] = geom
                carriers.add_capture(geom, feat.geom_crs_authid,
                                     getattr(feat, "geom_witnesses", ()))
                yield
            if not carriers.serves(layers.now(ds_fp)):
                carriers = None
            else:
                # One bounded pass per unit. The layer is looked up before
                # each: removed or re-declared in between, the completion
                # ends by name (ReviewLayerChanged), nothing is read dead.
                passes = carriers.load_steps()
                while True:
                    layers.now(ds_fp)
                    try:
                        next(passes)
                    except StopIteration:
                        break
                    yield
                flog(f"[{trace_id}] review: crs_witness_lookup layer="
                     f"{info.get('layer_name')!r} n_geometries={len(decoded)} "
                     f"n_known={carriers.n_known} n_passes={carriers.n_passes} "
                     f"n_read={carriers.n_read} n_wasted={carriers.n_wasted} "
                     f"n_to_query={carriers.n_uncovered} "
                     f"n_failed_passes={carriers.n_failed_passes} "
                     f"cap={carriers.cap} stopped={carriers.stopped or '-'}",
                     "INFO")
                yield
        for key in keys:
            feat = ents[key]
            src = feat.geom_crs_authid
            if progress is not None:
                progress["done"] += 1
            if not has_layer:
                # One unit each too: 2000 refusals, each logged, held the
                # GUI thread ~0.45 s in one piece.
                yield
                verdict = None
            else:
                geom = decoded.get(key)
                if geom is None:
                    continue  # undecodable: counted where the layer is built
                yield
                verdict = crs_verdict(geom, src, layers.now(ds_fp), trace_id,
                                      getattr(feat, "geom_witnesses", ()),
                                      stories=stories, carriers=carriers)
            counts["checked"] += 1
            if verdict is None:
                counts["refused"] += 1
                flog(f"[{trace_id}] review: crs_refused layer="
                     f"{info.get('layer_name')!r} entity={key} "
                     f"reason_code=crs_mismatch detail=captured in {src}, the "
                     f"layer is no longer in the project: its CRS story cannot "
                     f"be read, not drawn in the overlay ({target})",
                     "WARNING")
                new = feat._replace(
                    geom_wkb=None,
                    unresolved=",".join(r for r in (feat.unresolved,
                                                    "crs_mismatch") if r))
            elif (verdict.status in CRS_REFUSED_STATUSES
                  or verdict.geometry is None):
                counts["refused"] += 1
                reason_code, detail = crs_refusal(verdict.status, src,
                                                  layers.now(ds_fp))
                flog(f"[{trace_id}] review: crs_refused layer="
                     f"{info.get('layer_name')!r} entity={key} "
                     f"reason_code={reason_code} evidence={verdict.evidence} "
                     f"detail={detail}", "WARNING")
                new = feat._replace(
                    geom_wkb=None,
                    unresolved=",".join(r for r in (feat.unresolved,
                                                    reason_code) if r))
            elif verdict.status == "reprojected":
                counts["reprojected"] += 1
                new = feat._replace(geom_wkb=geometry_to_wkb(verdict.geometry),
                                    geom_crs_authid=target or src)
            else:
                continue
            if updated is None:
                updated = dict(ents)
            updated[key] = new
        features[ds_fp] = updated if updated is not None else ents
    if not counts["reprojected"] and not counts["refused"]:
        if counts["checked"]:
            flog(f"[{trace_id}] review: crs_alignment n_checked="
                 f"{counts['checked']} all drawn raw (relabel or same "
                 f"position)", "INFO")
        return result
    flog(f"[{trace_id}] review: crs_alignment n_checked={counts['checked']} "
         f"n_reprojected={counts['reprojected']} n_refused={counts['refused']}",
         "WARNING" if counts["refused"] else "INFO")
    return recount(result._replace(features=features))


def _key_terms(key: str, identity_json: str):
    """Filter expression that reads the key columns of *key* back, or None."""
    from qgis.core import QgsExpression  # noqa: PLC0415
    from .identity import composite_key  # noqa: PLC0415
    try:
        ident = json.loads(identity_json or "{}")
    except (ValueError, TypeError):
        return None
    if not isinstance(ident, dict):
        return None
    if key.startswith("pk:"):
        if ident.get("pk_field") and ident.get("pk_value") is not None:
            return QgsExpression.createFieldEqualityExpression(
                ident["pk_field"], ident["pk_value"])
        return None
    pairs = composite_key(ident) if key.startswith("pkc:") else []
    if not pairs:
        return None
    return "(" + " AND ".join(
        QgsExpression.createFieldEqualityExpression(name, value)
        for name, value in pairs) + ")"


def _fetch_live(layer, needing: dict) -> tuple:
    """({entity key: provider feature}, {entity key: reason when absent}).

    The identity of a live feature is PROVEN, never inferred from where it
    sits:
      * ``fid:N`` only on a layer whose fids are stable
        (commit_guard.fid_is_stable: database providers, GeoPackage...).
        Reading fid N and recomputing ``fid:N`` from what was read proves
        nothing, it is circular. On a Shapefile or a GeoJSON (identity
        MEDIUM) a deletion renumbers every later record at the repack: fid
        N was then ANOTHER feature, whose attributes and geometry were
        painted and exported as the state at T of this one;
      * ``pk:`` / ``pkc:``: the key columns are read back from the
        provider and must give the journal key again.
    A key that more than one live feature gives is not proven either.
    Refused keys are named ``identity_not_proven``.

    In one go; the completion reads the same batches across event-loop
    turns (`_fetch_live_steps`).
    """
    return _run_through(_fetch_live_steps(lambda: layer, needing))


def _fetch_live_steps(get_layer, needing: dict):
    """`_fetch_live`, one provider request per unit (``yield``).

    `_LIVE_BATCH` fids, or key expressions, per request: 2000 live features
    read back in one request held the GUI thread ~0.5 s. The found / refused
    maps are shared by every batch, so a key two live features give is
    still refused whichever batches they come from. *get_layer* gives the
    layer as it is NOW, before every request (the completion's
    `_LayerWatch.now`: a layer removed or given another CRS between two
    turns ends the completion by name instead of being read dead).
    """
    layer = get_layer()
    if layer is None:
        return {}, {k: "live_layer_missing" for k in needing}
    from qgis.core import QgsFeatureRequest  # noqa: PLC0415
    from .commit_guard import fid_is_stable  # noqa: PLC0415
    from .identity import (  # noqa: PLC0415
        compute_entity_fingerprint,
        compute_feature_identity,
        get_identity_strength_for_layer,
    )
    from .support_policy import IdentityStrength  # noqa: PLC0415

    try:
        strength = get_identity_strength_for_layer(layer)
        stable = fid_is_stable(layer)
    except Exception as exc:  # noqa: BLE001
        flog(f"review: live_completion identity strength unreadable "
             f"({exc!r})", "WARNING")
        return {}, {k: "identity_not_proven" for k in needing}
    fid_proven = stable and strength in (IdentityStrength.STRONG,
                                         IdentityStrength.MEDIUM)
    reasons: dict = {}
    fids = []
    key_terms = []
    for idx, (key, feat) in enumerate(needing.items(), 1):
        if idx % _LIVE_BATCH == 0:
            yield
        if key.startswith(("pk:", "pkc:")):
            term = _key_terms(key, feat.identity_json)
            if term is None:
                reasons[key] = "identity_not_proven"
            else:
                key_terms.append(term)
        elif key.startswith("fid:") and fid_proven:
            try:
                fids.append(int(key[4:]))
            except ValueError:
                reasons[key] = "identity_not_proven"
        else:
            reasons[key] = "identity_not_proven"
    if not fid_proven and any(k.startswith("fid:") for k in needing):
        flog(f"review: live_completion refused for fid keys on layer "
             f"{layer.name()!r} (fid stable={stable}, identity={strength}): "
             f"the fid of a feature there is its position, not its identity",
             "WARNING")
    found: dict = {}
    ambiguous: set = set()

    def _collect(request) -> None:
        current = get_layer()
        for lf in current.dataProvider().getFeatures(request):
            key = compute_entity_fingerprint(compute_feature_identity(current, lf))
            if key not in needing or key in reasons:
                continue
            if key in found:
                ambiguous.add(key)
                continue
            found[key] = lf

    requests = [QgsFeatureRequest().setFilterFids(fids[i:i + _LIVE_BATCH])
                for i in range(0, len(fids), _LIVE_BATCH)]
    requests += [QgsFeatureRequest().setFilterExpression(
        " OR ".join(key_terms[i:i + _LIVE_BATCH]))
        for i in range(0, len(key_terms), _LIVE_BATCH)]
    for n_request, request in enumerate(requests):
        if n_request:
            yield
        _collect(request)
    for key in ambiguous:
        found.pop(key, None)
        reasons[key] = "identity_not_proven"
    return found, reasons


def _flag_layer_or_schema_gaps(
    ents: dict, layer_missing: bool, live_field_names: Optional[set], _json,
) -> dict:
    """Flag entities when the source layer is gone or a captured column drifted."""
    from .serialization import is_layer_audit_field  # noqa: PLC0415
    if not layer_missing and not live_field_names:
        return ents
    out = None
    for key, feat in ents.items():
        reasons = [r for r in (feat.unresolved or "").split(",") if r]
        changed = False
        if layer_missing and "live_layer_missing" not in reasons:
            reasons.append("live_layer_missing")
            changed = True
        if live_field_names is not None and "schema_drift" not in reasons:
            schema_at_t = set(getattr(feat, "schema_fields", None) or ())
            if feat.attrs_json:
                try:
                    parsed = _json.loads(feat.attrs_json)
                    if isinstance(parsed, dict):
                        schema_at_t |= set(parsed.keys())
                except (ValueError, TypeError):
                    pass
            if any(n not in live_field_names and not is_layer_audit_field(n)
                   for n in schema_at_t):
                reasons.append("schema_drift")
                changed = True
        if changed:
            if out is None:
                out = dict(ents)
            out[key] = feat._replace(unresolved=",".join(reasons))
    return out if out is not None else ents


def _merge_live(feat, live_feature, _json):
    """SnapshotFeature completed with the live geometry / attributes."""
    from .geometry_utils import geometry_to_wkb  # noqa: PLC0415
    from .serialization import is_layer_audit_field, serialize_value  # noqa: PLC0415

    attrs = {}
    if feat.attrs_json:
        try:
            parsed = _json.loads(feat.attrs_json)
            if isinstance(parsed, dict):
                attrs = parsed
        except (ValueError, TypeError):
            attrs = {}
    schema_at_t = set(getattr(feat, "schema_fields", None) or ())
    fields = live_feature.fields()
    live_names = {fields.at(i).name() for i in range(fields.count())}
    if feat.attrs_from_live:
        for i in range(fields.count()):
            name = fields.at(i).name()
            if schema_at_t and name not in schema_at_t:
                continue
            if name not in attrs:
                attrs[name] = serialize_value(live_feature.attribute(i))
    geom = feat.geom_wkb
    reasons = [r for r in (feat.unresolved or "").split(",") if r]
    known_at_t = schema_at_t | set(attrs.keys())
    if any(n not in live_names and not is_layer_audit_field(n)
           for n in known_at_t):
        if "schema_drift" not in reasons:
            reasons.append("schema_drift")
    if feat.geom_from_live:
        live_geom = live_feature.geometry()
        if live_geom is None or live_geom.isNull():
            from .constants import GEOMETRY_NULL_MARKER  # noqa: PLC0415
            geom = GEOMETRY_NULL_MARKER
        else:
            geom = geometry_to_wkb(live_geom)
        if geom is None and "geometry_unknown" not in reasons:
            reasons.append("geometry_unknown")
    return feat._replace(
        geom_wkb=geom,
        attrs_json=_json.dumps(attrs, ensure_ascii=False) if attrs else None,
        geom_from_live=False,
        attrs_from_live=False,
        unresolved=",".join(reasons),
    )


def _set_source_attrs(feat, source_fields, snap_feat) -> bool:
    """Populate source field values from SnapshotFeature.attrs_json.

    Returns True when a non-audit column known at T is missing from
    *source_fields* (schema drift).
    """
    from .serialization import is_layer_audit_field  # noqa: PLC0415
    if not snap_feat.attrs_json:
        return False
    try:
        parsed = json.loads(snap_feat.attrs_json)
    except (ValueError, TypeError):
        return False
    if not isinstance(parsed, dict):
        return False
    field_list = list(source_fields or ())
    present_names = {field.name() for field in field_list}
    for field in field_list:
        name = field.name()
        if name not in parsed:
            continue
        feat.setAttribute(
            name,
            _coerce_field_value(parsed[name], field),
        )
    known_at_t = set(getattr(snap_feat, "schema_fields", None) or ()) | set(parsed.keys())
    return bool(present_names and any(
        n not in present_names and not is_layer_audit_field(n)
        for n in known_at_t
    ))


_NUMERIC_TYPE_NAMES = frozenset({
    "real", "double", "float", "integer", "int", "int2", "int4", "int8",
    "long", "bigint", "numeric", "decimal", "integer64", "smallint",
    "qulonglong", "qlonglong", "doubleprecision", "float4", "float8",
})
_DATE_TYPE_NAMES = frozenset({"date", "datetime", "time"})


def _coerce_field_value(val, field):
    """Convert *val* to a type compatible with *field*, returning None on failure."""
    if val is None:
        return None
    from .serialization import (  # noqa: PLC0415
        _BLOB_TYPE_NAMES, _BOOL_TYPE_NAMES, _TEXT_TYPE_NAMES,
        _normalize_type_name, deserialize_value,
    )
    raw_type = field.typeName() if hasattr(field, "typeName") else ""
    norm = _normalize_type_name(raw_type)
    if norm in _TEXT_TYPE_NAMES:
        return val if isinstance(val, str) else str(val)
    if val in ("", "NULL", "null") and norm not in _BLOB_TYPE_NAMES:
        return None
    if norm in _BLOB_TYPE_NAMES or norm in _DATE_TYPE_NAMES or norm in _BOOL_TYPE_NAMES:
        try:
            return deserialize_value(val, raw_type)
        except Exception:  # noqa: BLE001
            return None
    if norm in _NUMERIC_TYPE_NAMES:
        try:
            if "int" in norm or norm in ("long", "bigint", "smallint", "qlonglong", "qulonglong"):
                return int(val)
            return float(val)
        except (ValueError, TypeError):
            return None
    try:
        return deserialize_value(val, raw_type)
    except Exception:  # noqa: BLE001
        return val


__all__ = ["SnapshotOverlaySession", "complete_from_live", "INCOMPLETE_FIELD"]
