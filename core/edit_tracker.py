"""Edit session tracker for RecoverLand (RLU-020 to RLU-026).

Listens to QGIS editing signals on all editable layers.
Captures initial feature state before commit, generates audit events
after successful commit, discards on rollback.

All QGIS object access happens on the main thread.
Serialized data is passed to the write queue for async persistence.
"""
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, List

from .audit_backend import AuditEvent
from .edit_buffer import (
    EditSessionBuffer, FeatureSnapshot, create_snapshot_from_feature,
)
from .identity import (
    compute_datasource_fingerprint, compute_feature_identity,
    compute_project_fingerprint, extract_layer_name,
    compute_entity_fingerprint,
)
from .sqlite_schema import CURRENT_SCHEMA_VERSION
from .geometry_utils import geometries_equal, geometry_to_wkb, wkb_short_repr
from .serialization import (
    serialize_attributes, serialize_field_schema,
    compute_update_delta, build_full_snapshot,
)
from .support_policy import evaluate_layer_support, SupportLevel
from .user_identity import resolve_user_name
from .logger import flog


# Sentinel returned by dict.get to distinguish "fid not in committed_geom_changes"
# (provider did not change geometry) from "fid present with explicit None geom"
# (geometry was cleared by the commit).
_SENTINEL = object()


class EditSessionTracker:
    """Tracks editing sessions across all layers in a QGIS project.

    Lifecycle: created when plugin starts, disposed when plugin unloads.
    One tracker instance per plugin lifetime.
    """

    _MASS_DELETE_THRESHOLD = 100

    def __init__(self, write_queue, journal_manager):
        self._write_queue = write_queue
        self._journal_manager = journal_manager
        self._buffers: Dict[str, EditSessionBuffer] = {}
        self._connected_layers: Dict[str, object] = {}
        self._layer_fingerprints: Dict[str, str] = {}
        self._signal_connections: Dict[str, List] = {}
        self._active = False
        self._suppress_depth = 0
        self._allowed_layer_fingerprints: set = set()  # empty = track all
        self._session_event_count = 0
        self._on_commit_callback = None
        self._on_overflow_callback = None

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def is_suppressed(self) -> bool:
        return self._suppress_depth > 0

    def suppress(self) -> None:
        """Temporarily suppress event capture (e.g. during restore).

        Reentrant: each suppress() must be paired with unsuppress().
        The tracker remains suppressed until all callers have unsuppressed.
        """
        self._suppress_depth += 1
        flog(f"EditSessionTracker: suppressed (depth={self._suppress_depth})")

    def unsuppress(self) -> None:
        """Re-enable event capture after restore.

        Reentrant: only actually unsuppresses when depth reaches 0.
        """
        if self._suppress_depth > 0:
            self._suppress_depth -= 1
        flog(f"EditSessionTracker: unsuppressed (depth={self._suppress_depth})")

    def force_unsuppress(self) -> None:
        """Force-reset suppress depth to 0 (cleanup/teardown only)."""
        prev = self._suppress_depth
        self._suppress_depth = 0
        if prev > 0:
            flog(f"EditSessionTracker: force_unsuppress (was depth={prev})")

    @property
    def session_event_count(self) -> int:
        return self._session_event_count

    def set_commit_callback(self, callback) -> None:
        """Set callback(event_count, layer_name, is_mass_delete, delete_count)."""
        self._on_commit_callback = callback

    def set_overflow_callback(self, callback) -> None:
        """Set callback() invoked on the main thread when the write queue overflows.

        The tracker automatically deactivates before calling this.
        Use this to display a blocking user alert.
        """
        self._on_overflow_callback = callback

    def reset_session_count(self) -> None:
        self._session_event_count = 0

    def activate(self) -> None:
        self._active = True
        flog("EditSessionTracker: activated")

    def deactivate(self) -> None:
        self._active = False
        flog("EditSessionTracker: deactivated")

    def set_filter(self, layer_fingerprints: set) -> None:
        """Restrict tracking to these datasource fingerprints. Empty set = track all."""
        self._allowed_layer_fingerprints = set(layer_fingerprints)
        for layer_id in list(self._connected_layers.keys()):
            layer_fp = self._layer_fingerprints.get(layer_id)
            if self._allowed_layer_fingerprints and layer_fp not in self._allowed_layer_fingerprints:
                layer = self._connected_layers.get(layer_id)
                if layer is not None:
                    try:
                        self.disconnect_layer(layer)
                    except RuntimeError:
                        pass
        try:
            from qgis.core import QgsProject, QgsVectorLayer
            for layer in QgsProject.instance().mapLayers().values():
                if isinstance(layer, QgsVectorLayer):
                    self.connect_layer(layer)
        except (ImportError, RuntimeError) as exc:
            # Benign during shutdown or in unit test harness without QgsProject.
            flog(f"EditSessionTracker.set_filter: reconnect skipped: {exc}", "DEBUG")
        flog(f"EditSessionTracker: filter set, {len(self._allowed_layer_fingerprints)} layer(s)")

    def connect_layer(self, layer) -> None:
        """Start monitoring a layer for edit events."""
        layer_id = layer.id()
        layer_fp = compute_datasource_fingerprint(layer)
        if self._allowed_layer_fingerprints and layer_fp not in self._allowed_layer_fingerprints:
            return
        if layer_id in self._connected_layers:
            return
        policy = evaluate_layer_support(layer)
        if policy.support_level == SupportLevel.REFUSED:
            flog(f"EditSessionTracker: refused {layer.name()}: {policy.reason}")
            return
        if not policy.capture:
            return
        self._bind_signals(layer)
        self._connected_layers[layer_id] = layer
        self._layer_fingerprints[layer_id] = layer_fp
        flog(f"EditSessionTracker: connected {layer.name()} [{layer_id}]")

    def disconnect_layer(self, layer) -> None:
        """Stop monitoring a layer."""
        layer_id = layer.id()
        self._unbind_signals(layer)
        self._buffers.pop(layer_id, None)
        self._connected_layers.pop(layer_id, None)
        self._layer_fingerprints.pop(layer_id, None)

    def disconnect_layer_by_id(self, layer_id: str) -> None:
        """Stop monitoring a layer by its ID (for layersRemoved signal)."""
        layer = self._connected_layers.get(layer_id)
        if layer is not None:
            try:
                self._unbind_signals(layer)
            except RuntimeError:
                pass
        self._buffers.pop(layer_id, None)
        self._connected_layers.pop(layer_id, None)
        self._layer_fingerprints.pop(layer_id, None)

    def disconnect_all(self) -> None:
        for layer_id in list(self._connected_layers.keys()):
            layer = self._connected_layers.get(layer_id)
            if layer is not None:
                try:
                    self._unbind_signals(layer)
                except RuntimeError:
                    pass
        self._connected_layers.clear()
        self._layer_fingerprints.clear()
        self._buffers.clear()

    def _bind_signals(self, layer) -> None:
        """Wire all six QgsVectorLayer commit signals so the tracker can
        observe both projected pre-commit state and authoritative post-commit
        state (per QGIS API: commit happens in stages and each stage emits a
        committed*Changes signal once the provider has accepted it)."""
        layer_id = layer.id()
        slots = [
            (layer.editingStarted,
             lambda *_, lid=layer_id: self._on_editing_started(lid)),
            (layer.beforeCommitChanges,
             lambda *_, lid=layer_id: self._on_before_commit(lid)),
            (layer.committedFeaturesAdded,
             lambda _lid, feats, lid=layer_id:
                 self._on_committed_features_added(lid, feats)),
            (layer.committedFeaturesRemoved,
             lambda _lid, fids, lid=layer_id:
                 self._on_committed_features_removed(lid, fids)),
            (layer.committedAttributeValuesChanges,
             lambda _lid, changes, lid=layer_id:
                 self._on_committed_attribute_values_changes(lid, changes)),
            (layer.committedGeometriesChanges,
             lambda _lid, changes, lid=layer_id:
                 self._on_committed_geometries_changes(lid, changes)),
            (layer.afterCommitChanges,
             lambda *_, lid=layer_id: self._on_after_commit(lid)),
            (layer.afterRollBack,
             lambda *_, lid=layer_id: self._on_rollback(lid)),
        ]
        for signal, slot in slots:
            signal.connect(slot)
        self._signal_connections[layer_id] = slots

    def _unbind_signals(self, layer) -> None:
        layer_id = layer.id()
        slots = self._signal_connections.pop(layer_id, [])
        for signal, slot in slots:
            try:
                signal.disconnect(slot)
            except (TypeError, RuntimeError):
                pass

    def _create_buffer(self, layer_id: str) -> EditSessionBuffer:
        session_id = str(uuid.uuid4())
        buf = EditSessionBuffer(layer_id, session_id)
        self._buffers[layer_id] = buf
        return buf

    def _on_editing_started(self, layer_id: str) -> None:
        if not self._active or self.is_suppressed:
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if self._allowed_layer_fingerprints and layer_fp not in self._allowed_layer_fingerprints:
            return
        self._create_buffer(layer_id)
        flog(f"EditSessionTracker: editing started on {layer_id}")

    def _on_before_commit(self, layer_id: str) -> None:
        """Capture initial state of all affected features before commit."""
        if not self._active or self.is_suppressed:
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if self._allowed_layer_fingerprints and layer_fp not in self._allowed_layer_fingerprints:
            return
        layer = self._connected_layers.get(layer_id)
        if layer is None:
            return
        buf = self._buffers.get(layer_id)
        if buf is None:
            buf = self._create_buffer(layer_id)
            flog(f"EditSessionTracker: late session start on {layer_id}")
        self._capture_edit_buffer_state(layer, buf)

    def _on_after_commit(self, layer_id: str) -> None:
        """Generate audit events after successful commit."""
        if not self._active or self.is_suppressed:
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if self._allowed_layer_fingerprints and layer_fp not in self._allowed_layer_fingerprints:
            return
        layer = self._connected_layers.get(layer_id)
        buf = self._buffers.pop(layer_id, None)
        if layer is None or buf is None:
            return
        events = self._generate_events(layer, buf)
        if not events:
            flog(f"EditSessionTracker: commit on {layer_id} produced 0 real events "
                 "(false commit: edit buffer had changes but values were identical)")
            return
        accepted = self._write_queue.enqueue(events)
        if not accepted:
            flog("EditSessionTracker: write queue overflow, "
                 "deactivating tracking to prevent further data loss", "ERROR")
            self.deactivate()
            if self._on_overflow_callback is not None:
                self._on_overflow_callback()
            return
        self._register_datasource(layer)
        self._session_event_count += len(events)
        ops = [e.operation_type for e in events]
        flog(f"EditSessionTracker: {len(events)} events for {layer_id} ops={ops} "
             f"created_at={events[0].created_at}")
        layer_name = events[0].layer_name_snapshot or layer_id
        delete_count = sum(1 for e in events if e.operation_type == "DELETE")
        is_mass_delete = delete_count >= self._MASS_DELETE_THRESHOLD
        if self._on_commit_callback is not None:
            self._on_commit_callback(
                len(events), layer_name, is_mass_delete, delete_count,
                events[0].datasource_fingerprint)

    def _register_datasource(self, layer) -> None:
        """Store the layer's source URI in the journal registry for future restore."""
        try:
            conn = self._journal_manager.get_connection()
            from .datasource_registry import register_datasource
            register_datasource(conn, layer)
        except Exception as e:
            flog(f"EditSessionTracker: datasource register failed: {e}", "WARNING")

    def _on_committed_features_added(self, layer_id: str, features) -> None:
        """Capture full feature data from committedFeaturesAdded signal.

        This signal fires during commit, after the provider has assigned
        real FIDs. We serialize everything here so _generate_events can
        build INSERT events without any provider lookup.
        """
        buf = self._buffers.get(layer_id)
        if buf is None:
            flog(f"EditSessionTracker: committedFeaturesAdded but no buffer "
                 f"for {layer_id}", "WARNING")
            return
        layer = self._connected_layers.get(layer_id)
        if layer is None:
            flog(f"EditSessionTracker: committedFeaturesAdded but no layer "
                 f"for {layer_id}", "WARNING")
            return
        field_names = [f.name() for f in layer.fields()]
        for feature in features:
            attrs = serialize_attributes(feature, field_names)
            wkb = geometry_to_wkb(feature.geometry())
            identity = compute_feature_identity(layer, feature)
            buf.record_committed_addition({
                "fid": feature.id(),
                "attrs_json": build_full_snapshot(attrs),
                "geometry_wkb": wkb,
                "identity_json": identity,
                "entity_fingerprint": compute_entity_fingerprint(identity),
            })
        flog(f"EditSessionTracker: committedFeaturesAdded "
             f"{len(features)} features captured on {layer_id}")

    def _on_committed_features_removed(self, layer_id: str, fids) -> None:
        """Authoritative confirmation of deletes that the provider applied.

        Without this signal the tracker would log a DELETE event for
        every feature the user marked as deleted in the buffer, even if
        the provider rejected the delete (constraint, referential check).
        """
        buf = self._buffers.get(layer_id)
        if buf is None:
            return
        for fid in fids:
            buf.record_committed_deletion(int(fid))
        flog(f"EditSessionTracker: committedFeaturesRemoved "
             f"layer={layer_id} fids={sorted(int(f) for f in fids)[:20]} "
             f"(total={len(fids)})")

    def _on_committed_attribute_values_changes(self, layer_id: str,
                                               changed_attrs) -> None:
        """Authoritative attribute changes accepted by the provider.

        QGIS emits ``{fid_post_commit: {idx: value}}`` after the provider
        confirms the attribute writes for that fid.
        """
        buf = self._buffers.get(layer_id)
        if buf is None:
            return
        for fid, idx_to_value in changed_attrs.items():
            try:
                fid_i = int(fid)
            except (TypeError, ValueError):
                flog(f"EditSessionTracker: committedAttributeValuesChanges "
                     f"unparseable fid={fid!r}, skipped", "WARNING")
                continue
            buf.record_committed_attr_change(fid_i, dict(idx_to_value))
        flog(f"EditSessionTracker: committedAttributeValuesChanges "
             f"layer={layer_id} fids={sorted(int(f) for f in changed_attrs.keys())[:20]} "
             f"(total={len(changed_attrs)})")

    def _on_committed_geometries_changes(self, layer_id: str,
                                         changed_geoms) -> None:
        """Authoritative geometry changes accepted by the provider.

        QGIS emits ``{fid_post_commit: QgsGeometry}`` after the provider
        confirms each geometry write.
        """
        buf = self._buffers.get(layer_id)
        if buf is None:
            return
        for fid, geom in changed_geoms.items():
            try:
                fid_i = int(fid)
            except (TypeError, ValueError):
                flog(f"EditSessionTracker: committedGeometriesChanges "
                     f"unparseable fid={fid!r}, skipped", "WARNING")
                continue
            wkb = geometry_to_wkb(geom)
            buf.record_committed_geom_change(fid_i, wkb)
        flog(f"EditSessionTracker: committedGeometriesChanges "
             f"layer={layer_id} fids={sorted(int(f) for f in changed_geoms.keys())[:20]} "
             f"(total={len(changed_geoms)})")

    def _on_rollback(self, layer_id: str) -> None:
        buf = self._buffers.pop(layer_id, None)
        if buf is not None:
            buf.clear()
            flog(f"EditSessionTracker: rollback, buffer cleared for {layer_id}")

    _MEMORY_HARD_LIMIT_MB = 500

    def _capture_edit_buffer_state(self, layer, buf: EditSessionBuffer) -> None:
        """Read the edit buffer and capture originals from the provider.

        Guards against empty commits: if the edit buffer reports no deletions,
        no attribute changes, no geometry changes, and no additions, skip
        capture entirely. This avoids recording phantom events when a user
        opens edit mode and commits without modifying anything.

        Guards against OOM: checks buffer thresholds after each capture phase
        and stops capture if the hard memory limit is exceeded.
        """
        edit_buf = layer.editBuffer()
        if edit_buf is None:
            return
        deleted_fids = edit_buf.deletedFeatureIds()
        changed_attrs = edit_buf.changedAttributeValues()
        changed_geoms = edit_buf.changedGeometries()
        added_feats = edit_buf.addedFeatures()
        flog(f"EditSessionTracker: edit buffer state on {buf.layer_id}: "
             f"deletes={len(deleted_fids)} "
             f"attr_changes={len(changed_attrs)} "
             f"geom_changes={len(changed_geoms)} "
             f"additions={len(added_feats)}")
        if not (deleted_fids or changed_attrs or changed_geoms or added_feats):
            flog(f"EditSessionTracker: empty edit buffer on {buf.layer_id}, "
                 "commit without modifications, skipping capture")
            return
        provider = layer.dataProvider()
        field_names = [f.name() for f in layer.fields()]
        self._capture_deletions(provider, edit_buf, buf, field_names)
        if self._check_buffer_pressure(buf):
            return
        self._capture_modifications(layer, provider, edit_buf, buf, field_names)
        self._check_buffer_pressure(buf)
        self._capture_additions(edit_buf, buf)

    def _check_buffer_pressure(self, buf: EditSessionBuffer) -> bool:
        """Log warning if buffer exceeds soft threshold; return True if hard limit hit."""
        if buf.approx_memory_mb > self._MEMORY_HARD_LIMIT_MB:
            flog(f"EditSessionTracker: buffer hard limit reached "
                 f"({buf.approx_memory_mb:.0f} MB, {buf.total_tracked} features) "
                 f"on {buf.layer_id}, stopping capture to prevent OOM", "ERROR")
            return True
        if buf.needs_flush():
            flog(f"EditSessionTracker: buffer pressure warning "
                 f"({buf.approx_memory_mb:.0f} MB, {buf.total_tracked} features) "
                 f"on {buf.layer_id}", "WARNING")
        return False

    def _capture_deletions(self, provider, edit_buf, buf, field_names) -> None:
        from qgis.core import QgsFeatureRequest
        deleted_fids = list(edit_buf.deletedFeatureIds())
        if not deleted_fids:
            return
        flog(f"EditSessionTracker: capturing {len(deleted_fids)} deletions, "
             f"FIDs={deleted_fids[:20]}")
        request = QgsFeatureRequest().setFilterFids(deleted_fids)
        captured = 0
        for feature in provider.getFeatures(request):
            snapshot = create_snapshot_from_feature(feature, field_names)
            buf.record_deletion(snapshot)
            captured += 1
        if captured != len(deleted_fids):
            flog(f"EditSessionTracker: deletion capture mismatch: "
                 f"expected={len(deleted_fids)} captured={captured}", "WARNING")

    def _capture_modifications(self, layer, provider, edit_buf, buf, field_names) -> None:
        """Capture OLD state from the provider (pre-commit).

        NEW state is filled later from the authoritative QGIS post-commit
        signals (committedAttributeValuesChanges, committedGeometriesChanges)
        in _generate_events. We keep a projected NEW from layer+buffer as a
        fallback in case a provider does not emit those signals (some
        non-spatial providers historically did not).

        Newly-added features (with temporary FIDs not yet known to the
        provider) are excluded here: they will be recorded as INSERT events
        from committedFeaturesAdded which carries the post-commit fid.
        """
        from qgis.core import QgsFeatureRequest
        changed_attrs = edit_buf.changedAttributeValues()
        changed_geoms = edit_buf.changedGeometries()
        deleted_set = set(edit_buf.deletedFeatureIds())
        added_set = set(edit_buf.addedFeatures().keys())

        geom_fids = set(changed_geoms.keys()) - deleted_set - added_set
        attr_only_fids = ((set(changed_attrs.keys()) - deleted_set - added_set)
                          - geom_fids)

        excluded_added = (set(changed_geoms) | set(changed_attrs)) & added_set
        excluded_deleted = (set(changed_geoms) | set(changed_attrs)) & deleted_set
        flog(f"EditSessionTracker: capture_modifications {buf.layer_id} "
             f"geom_fids={sorted(geom_fids)} "
             f"attr_only_fids={sorted(attr_only_fids)} "
             f"excluded_added={sorted(excluded_added)} "
             f"excluded_deleted={sorted(excluded_deleted)}")

        all_mod_fids = geom_fids | attr_only_fids
        new_state_by_fid = self._capture_new_state(
            layer, all_mod_fids, field_names)

        captured_geom: set = set()
        if geom_fids:
            request = QgsFeatureRequest().setFilterFids(list(geom_fids))
            for feature in provider.getFeatures(request):
                captured_geom.add(feature.id())
                changed_names = self._extract_changed_field_names(
                    feature.id(), changed_attrs, field_names)
                snapshot = create_snapshot_from_feature(
                    feature, field_names, changed_names)
                self._attach_new_state_and_identity(
                    snapshot, layer, feature, new_state_by_fid)
                buf.record_modification(snapshot)
                new_attrs_proj, new_wkb_proj = new_state_by_fid.get(
                    feature.id(), (None, None))
                flog(f"TRACE_CAPTURE: layer={buf.layer_id} fid={feature.id()} "
                     f"OLD_geom={wkb_short_repr(snapshot.geometry_wkb)} "
                     f"NEW_geom_projected={wkb_short_repr(new_wkb_proj)}")
            missing = geom_fids - captured_geom
            if missing:
                flog(f"EditSessionTracker: geom-modification OLD UNCAPTURED "
                     f"layer={buf.layer_id} fids={sorted(missing)} "
                     f"(provider lookup returned no row; UPDATE events for "
                     f"these fids cannot be generated)", "ERROR")

        captured_attr: set = set()
        if attr_only_fids:
            from ..compat import QgisCompat
            request = QgsFeatureRequest().setFilterFids(list(attr_only_fids))
            request.setFlags(QgisCompat.NO_GEOMETRY)
            for feature in provider.getFeatures(request):
                captured_attr.add(feature.id())
                changed_names = self._extract_changed_field_names(
                    feature.id(), changed_attrs, field_names)
                snapshot = create_snapshot_from_feature(
                    feature, field_names, changed_names)
                self._attach_new_state_and_identity(
                    snapshot, layer, feature, new_state_by_fid)
                buf.record_modification(snapshot)
            missing = attr_only_fids - captured_attr
            if missing:
                flog(f"EditSessionTracker: attr-modification OLD UNCAPTURED "
                     f"layer={buf.layer_id} fids={sorted(missing)} "
                     f"(provider lookup returned no row; UPDATE events for "
                     f"these fids cannot be generated)", "ERROR")

    @staticmethod
    def _capture_new_state(layer, fids, field_names) -> Dict[int, tuple]:
        """Return {fid: (new_attrs_dict, new_geom_wkb)} read from layer+buffer."""
        if not fids:
            return {}
        from qgis.core import QgsFeatureRequest
        result: Dict[int, tuple] = {}
        request = QgsFeatureRequest().setFilterFids(list(fids))
        for feature in layer.getFeatures(request):
            attrs = serialize_attributes(feature, field_names)
            wkb = geometry_to_wkb(feature.geometry())
            result[feature.id()] = (attrs, wkb)
        missing = set(fids) - set(result.keys())
        if missing:
            flog(f"EditSessionTracker: _capture_new_state missing fids "
                 f"layer={layer.id()} fids={sorted(missing)} "
                 f"(post-commit lookup will fail; identity may be stale)",
                 "WARNING")
        return result

    @staticmethod
    def _attach_new_state_and_identity(snapshot, layer, old_feature,
                                       new_state_by_fid) -> None:
        """Populate snapshot.new_* and snapshot.identity_json.

        Identity is computed from the OLD feature (pre-commit). For PK-stable
        providers it carries the PK which survives commit. For FID-only
        providers it carries the pre-commit FID; restore falls back to
        post-state geometry lookup when the FID is unstable.
        """
        new_attrs, new_wkb = new_state_by_fid.get(snapshot.fid, (None, None))
        snapshot.new_attributes = new_attrs
        snapshot.new_geometry_wkb = new_wkb
        snapshot.identity_json = compute_feature_identity(layer, old_feature)

    @staticmethod
    def _extract_changed_field_names(fid: int, changed_attrs: Dict,
                                     field_names: List[str]) -> List[str]:
        changed = changed_attrs.get(fid, {})
        names = []
        for idx in sorted(changed.keys()):
            if 0 <= idx < len(field_names):
                names.append(field_names[idx])
        return names

    @staticmethod
    def _capture_additions(edit_buf, buf) -> None:
        for fid in edit_buf.addedFeatures().keys():
            buf.record_addition(fid)

    def _generate_events(self, layer, buf: EditSessionBuffer) -> List[AuditEvent]:
        """Build AuditEvent objects from the buffer after successful commit.

        Filters work plan against authoritative post-commit signals:
          * DELETE events are emitted only for fids confirmed by
            committedFeaturesRemoved (when at least one such signal was
            received; otherwise we trust the pre-commit deletion buffer).
          * UPDATE events use NEW state from committedAttributeValuesChanges
            and committedGeometriesChanges when present, falling back to the
            pre-commit projection captured via layer.getFeatures.
        """
        net = buf.compute_net_effect()
        events: List[AuditEvent] = []
        ds_fp = compute_datasource_fingerprint(layer)
        proj_fp = compute_project_fingerprint()
        layer_name = extract_layer_name(layer)
        provider_type = layer.dataProvider().name()
        user = resolve_user_name()
        now = datetime.now(timezone.utc).isoformat()
        field_schema = serialize_field_schema(layer.fields())
        geom_type, crs = self._get_layer_geometry_info(layer)
        field_names = [f.name() for f in layer.fields()]

        committed_deletions = buf.get_committed_deletions()
        committed_geom_changes = buf.get_committed_geom_changes()
        committed_attr_changes = buf.get_committed_attr_changes()

        deleted_to_emit = (net["deleted"] & committed_deletions
                           if committed_deletions else net["deleted"])
        rejected_deletions = net["deleted"] - deleted_to_emit
        if rejected_deletions:
            flog(f"EditSessionTracker: provider rejected deletions "
                 f"layer={buf.layer_id} fids={sorted(rejected_deletions)} "
                 f"(buffer requested but no committedFeaturesRemoved fired)",
                 "WARNING")

        for fid in deleted_to_emit:
            snap = buf.get_deleted_snapshots().get(fid)
            if snap is None:
                continue
            events.append(self._make_delete_event(
                snap, ds_fp, proj_fp, layer, layer_name,
                provider_type, user, now, field_schema, geom_type, crs,
                buf.session_id,
            ))

        for fid in net["modified"]:
            snap = buf.get_modified_snapshots().get(fid)
            if snap is None:
                continue
            event = self._make_update_event(
                snap, layer, fid, ds_fp, proj_fp, layer_name,
                provider_type, user, now, field_schema, geom_type, crs,
                buf.session_id,
                field_names,
                committed_geom_changes.get(fid, _SENTINEL),
                committed_attr_changes.get(fid),
            )
            if event is not None:
                events.append(event)

        committed = buf.get_committed_additions()
        flog(f"EditSessionTracker: generating events: "
             f"net_deleted={len(net['deleted'])} "
             f"committed_deletions={len(committed_deletions)} "
             f"emitting_deletes={len(deleted_to_emit)} "
             f"net_modified={len(net['modified'])} "
             f"committed_geom_changes={len(committed_geom_changes)} "
             f"committed_attr_changes={len(committed_attr_changes)} "
             f"net_added={len(net['added'])} "
             f"committed_additions={len(committed)}")
        if committed:
            for ca in committed:
                events.append(self._make_insert_event_from_committed(
                    ca, ds_fp, proj_fp, layer, layer_name,
                    provider_type, user, now, field_schema, geom_type, crs,
                    buf.session_id,
                ))
        else:
            for fid in net["added"]:
                event = self._make_insert_event(
                    layer, fid, ds_fp, proj_fp, layer_name,
                    provider_type, user, now, field_schema, geom_type, crs,
                    buf.session_id,
                )
                if event is not None:
                    events.append(event)
                else:
                    flog(f"EditSessionTracker: INSERT event dropped for "
                         f"FID={fid}, feature not found", "WARNING")

        return events

    @staticmethod
    def _get_layer_geometry_info(layer):
        from .geometry_utils import extract_geometry_type, extract_crs_authid
        return extract_geometry_type(layer), extract_crs_authid(layer)

    @staticmethod
    def _make_delete_event(snap, ds_fp, proj_fp, layer, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id) -> AuditEvent:
        attrs_json = build_full_snapshot(snap.attributes)
        identity = compute_feature_identity(layer, _stub_feature(snap))
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer.id(),
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=identity, operation_type="DELETE",
            attributes_json=attrs_json, geometry_wkb=snap.geometry_wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=None,
        )

    @staticmethod
    def _make_update_event(snap, layer, fid, ds_fp, proj_fp, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id,
                           field_names: List[str],
                           committed_geom,
                           committed_attrs) -> Optional[AuditEvent]:
        """Build an UPDATE event from OLD snapshot + NEW state.

        Sources for NEW (in priority order):
          1. Authoritative committed_geom / committed_attrs from QGIS post-
             commit signals when present (provider confirmed the change).
          2. Pre-commit projection from snap.new_attributes / new_geometry_wkb
             (read from layer+buffer before commit) as a fallback for
             providers that do not emit committed*Changes signals.

        Returns None if neither source provides any NEW state (the buffer
        thought the feature was modified but the provider applied nothing).
        """
        new_wkb, geom_authoritative = EditSessionTracker._resolve_new_geom(
            snap, committed_geom)
        new_attrs, attrs_authoritative = EditSessionTracker._resolve_new_attrs(
            snap, committed_attrs, field_names)

        if new_attrs is None and new_wkb is None and not geom_authoritative:
            flog(f"_make_update_event: no NEW state for fid={fid} "
                 f"(neither post-commit signal nor pre-commit projection), "
                 f"dropping UPDATE event", "WARNING")
            return None

        old_attrs = snap.attributes
        new_attrs = new_attrs if new_attrs is not None else old_attrs
        changed_field_names = snap.changed_field_names or snap.field_names
        delta_json = compute_update_delta(
            old_attrs, new_attrs, changed_field_names)
        geom_changed = not geometries_equal(snap.geometry_wkb, new_wkb)

        if delta_json is None and not geom_changed:
            return None
        if delta_json is None:
            delta_json = '{"changed_only": {}}'
        identity = snap.identity_json or f'{{"fid": {fid}}}'

        flog(f"TRACE_EVENT: layer={layer.id()} fid={fid} op=UPDATE "
             f"geom_source={'authoritative' if geom_authoritative else 'projected'} "
             f"attrs_source={'authoritative' if attrs_authoritative else 'projected'} "
             f"geom_changed={geom_changed} "
             f"OLD={wkb_short_repr(snap.geometry_wkb)} "
             f"NEW={wkb_short_repr(new_wkb)} "
             f"identity={identity}")

        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer.id(),
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=identity, operation_type="UPDATE",
            attributes_json=delta_json,
            geometry_wkb=snap.geometry_wkb if geom_changed else None,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=new_wkb if geom_changed else None,
        )

    @staticmethod
    def _resolve_new_geom(snap, committed_geom):
        """Pick the authoritative NEW geometry WKB if available.

        Returns (wkb_or_None, is_authoritative).
        """
        if committed_geom is not _SENTINEL:
            return committed_geom, True
        return snap.new_geometry_wkb, False

    @staticmethod
    def _resolve_new_attrs(snap, committed_attrs,
                           field_names: List[str]):
        """Compose NEW attribute dict from OLD + authoritative committed deltas.

        committed_attrs is {idx: value}. We start from the OLD attrs and
        overlay each idx with its committed value. When the post-commit
        signal does not carry attribute changes for this fid we fall back
        to snap.new_attributes captured pre-commit (may be None).

        Returns (attrs_dict_or_None, is_authoritative).
        """
        if not committed_attrs:
            return snap.new_attributes, False
        merged = dict(snap.attributes or {})
        for idx, value in committed_attrs.items():
            if 0 <= idx < len(field_names):
                from .serialization import serialize_value
                merged[field_names[idx]] = serialize_value(value)
        return merged, True

    @staticmethod
    def _make_insert_event(layer, fid, ds_fp, proj_fp, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id) -> Optional[AuditEvent]:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(fid)
        feature = next(layer.dataProvider().getFeatures(request), None)
        if feature is None:
            return None
        field_names = [f.name() for f in layer.fields()]
        attrs = serialize_attributes(feature, field_names)
        attrs_json = build_full_snapshot(attrs)
        wkb = geometry_to_wkb(feature.geometry())
        identity = compute_feature_identity(layer, feature)
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer.id(),
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=identity, operation_type="INSERT",
            attributes_json=attrs_json, geometry_wkb=wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=wkb,
        )

    @staticmethod
    def _make_insert_event_from_committed(ca, ds_fp, proj_fp, layer,
                                          layer_name, provider_type, user,
                                          now, field_schema, geom_type,
                                          crs, session_id) -> AuditEvent:
        """Build INSERT event from data captured by committedFeaturesAdded."""
        wkb = ca["geometry_wkb"]
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer.id(),
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=ca["identity_json"],
            operation_type="INSERT",
            attributes_json=ca["attrs_json"], geometry_wkb=wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=ca["entity_fingerprint"],
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=wkb,
        )


class _StubFeature:
    """Minimal stand-in for compute_feature_identity when using a snapshot."""

    def __init__(self, fid, attrs):
        self._fid = fid
        self._attrs = attrs

    def id(self):
        return self._fid

    def geometry(self):
        return None

    def __getitem__(self, key):
        return self._attrs.get(key)


def _stub_feature(snap: FeatureSnapshot):
    return _StubFeature(snap.fid, snap.attributes)
