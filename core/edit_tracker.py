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
)
from .geometry_utils import geometries_equal
from .serialization import (
    serialize_attributes, serialize_field_schema,
    compute_update_delta, build_full_snapshot,
)
from .support_policy import evaluate_layer_support, SupportLevel
from .user_identity import resolve_user_name
from .logger import flog


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
        self._allowed_layer_fingerprints: set = set()  # empty = track all
        self._session_event_count = 0
        self._on_commit_callback = None

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def session_event_count(self) -> int:
        return self._session_event_count

    def set_commit_callback(self, callback) -> None:
        """Set callback(event_count, layer_name, is_mass_delete, delete_count)."""
        self._on_commit_callback = callback

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
        except Exception:
            pass
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
        layer_id = layer.id()
        slots = [
            (layer.editingStarted,
             lambda *_, lid=layer_id: self._on_editing_started(lid)),
            (layer.beforeCommitChanges,
             lambda *_, lid=layer_id: self._on_before_commit(lid)),
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
        if not self._active:
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if self._allowed_layer_fingerprints and layer_fp not in self._allowed_layer_fingerprints:
            return
        self._create_buffer(layer_id)
        flog(f"EditSessionTracker: editing started on {layer_id}")

    def _on_before_commit(self, layer_id: str) -> None:
        """Capture initial state of all affected features before commit."""
        if not self._active:
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
        if not self._active:
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
        self._write_queue.enqueue(events)
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
                len(events), layer_name, is_mass_delete, delete_count)

    def _register_datasource(self, layer) -> None:
        """Store the layer's source URI in the journal registry for future restore."""
        try:
            conn = self._journal_manager.get_connection()
            from .datasource_registry import register_datasource
            register_datasource(conn, layer)
        except Exception as e:
            flog(f"EditSessionTracker: datasource register failed: {e}", "WARNING")

    def _on_rollback(self, layer_id: str) -> None:
        buf = self._buffers.pop(layer_id, None)
        if buf is not None:
            buf.clear()
            flog(f"EditSessionTracker: rollback, buffer cleared for {layer_id}")

    def _capture_edit_buffer_state(self, layer, buf: EditSessionBuffer) -> None:
        """Read the edit buffer and capture originals from the provider.

        Guards against empty commits: if the edit buffer reports no deletions,
        no attribute changes, no geometry changes, and no additions, skip
        capture entirely. This avoids recording phantom events when a user
        opens edit mode and commits without modifying anything.
        """
        edit_buf = layer.editBuffer()
        if edit_buf is None:
            return
        has_deletes = bool(edit_buf.deletedFeatureIds())
        has_attr_changes = bool(edit_buf.changedAttributeValues())
        has_geom_changes = bool(edit_buf.changedGeometries())
        has_additions = bool(edit_buf.addedFeatures())
        if not (has_deletes or has_attr_changes or has_geom_changes or has_additions):
            flog(f"EditSessionTracker: empty edit buffer on {buf.layer_id}, "
                 "commit without modifications, skipping capture")
            return
        provider = layer.dataProvider()
        field_names = [f.name() for f in layer.fields()]
        self._capture_deletions(provider, edit_buf, buf, field_names)
        self._capture_modifications(provider, edit_buf, buf, field_names)
        self._capture_additions(edit_buf, buf)

    def _capture_deletions(self, provider, edit_buf, buf, field_names) -> None:
        from qgis.core import QgsFeatureRequest
        deleted_fids = edit_buf.deletedFeatureIds()
        if not deleted_fids:
            return
        request = QgsFeatureRequest().setFilterFids(deleted_fids)
        for feature in provider.getFeatures(request):
            snapshot = create_snapshot_from_feature(feature, field_names)
            buf.record_deletion(snapshot)

    def _capture_modifications(self, provider, edit_buf, buf, field_names) -> None:
        from qgis.core import QgsFeatureRequest
        changed_attrs = edit_buf.changedAttributeValues()
        changed_geoms = edit_buf.changedGeometries()
        modified_fids = set(changed_attrs.keys()) | set(changed_geoms.keys())
        modified_fids -= set(edit_buf.deletedFeatureIds())
        if not modified_fids:
            return
        request = QgsFeatureRequest().setFilterFids(list(modified_fids))
        for feature in provider.getFeatures(request):
            changed_field_names = self._extract_changed_field_names(
                feature.id(), changed_attrs, field_names)
            snapshot = create_snapshot_from_feature(
                feature, field_names, changed_field_names)
            buf.record_modification(snapshot)

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
        """Build AuditEvent objects from the buffer after successful commit."""
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

        for fid in net["deleted"]:
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
            )
            if event is not None:
                events.append(event)

        for fid in net["added"]:
            event = self._make_insert_event(
                layer, fid, ds_fp, proj_fp, layer_name,
                provider_type, user, now, field_schema, geom_type, crs,
                buf.session_id,
            )
            if event is not None:
                events.append(event)

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
        )

    @staticmethod
    def _make_update_event(snap, layer, fid, ds_fp, proj_fp, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id) -> Optional[AuditEvent]:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(fid)
        current = next(layer.dataProvider().getFeatures(request), None)
        if current is None:
            return None
        new_attrs = serialize_attributes(current, snap.field_names)
        changed_field_names = snap.changed_field_names or snap.field_names
        delta_json = compute_update_delta(
            snap.attributes, new_attrs, changed_field_names)
        new_wkb = None
        geom = current.geometry()
        if geom and not geom.isNull() and not geom.isEmpty():
            new_wkb = bytes(geom.asWkb())
        geom_changed = not geometries_equal(snap.geometry_wkb, new_wkb)
        if delta_json is None and not geom_changed:
            return None
        if delta_json is None:
            delta_json = '{"changed_only": {}}'
        identity = compute_feature_identity(layer, current)
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
        )

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
        wkb = None
        geom = feature.geometry()
        if geom and not geom.isNull() and not geom.isEmpty():
            wkb = bytes(geom.asWkb())
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
