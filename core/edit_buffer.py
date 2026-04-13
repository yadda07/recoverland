"""Edit session buffer for RecoverLand (RLU-020).

Stores initial snapshots of features modified during an editing session.
Only the first snapshot per feature is kept (captures pre-edit state).
Handles memory bounding: flushes to staging if threshold is exceeded.
"""
from typing import Dict, Optional, Set, List, Any

from .logger import flog

_DEFAULT_FEATURE_THRESHOLD = 10000
_DEFAULT_MEMORY_MB_THRESHOLD = 200


class FeatureSnapshot:
    """Immutable snapshot of a feature's state before modification."""

    __slots__ = ("fid", "attributes", "geometry_wkb", "field_names", "changed_field_names")

    def __init__(self, fid: int, attributes: Dict[str, Any],
                 geometry_wkb: Optional[bytes], field_names: List[str],
                 changed_field_names: Optional[List[str]] = None):
        self.fid = fid
        self.attributes = attributes
        self.geometry_wkb = geometry_wkb
        self.field_names = field_names
        self.changed_field_names = list(changed_field_names or [])


class EditSessionBuffer:
    """In-memory buffer for one editing session on one layer.

    Tracks initial state of modified features, deleted features,
    and added feature IDs. Discarded on rollback.
    """

    def __init__(self, layer_id: str, session_id: str):
        self.layer_id = layer_id
        self.session_id = session_id
        self._modified: Dict[int, FeatureSnapshot] = {}
        self._deleted: Dict[int, FeatureSnapshot] = {}
        self._added_fids: Set[int] = set()
        self._committed_additions: List[Dict] = []
        self._approx_bytes = 0

    @property
    def modified_count(self) -> int:
        return len(self._modified)

    @property
    def deleted_count(self) -> int:
        return len(self._deleted)

    @property
    def added_count(self) -> int:
        return len(self._added_fids)

    @property
    def total_tracked(self) -> int:
        return len(self._modified) + len(self._deleted) + len(self._added_fids)

    @property
    def approx_memory_mb(self) -> float:
        return self._approx_bytes / (1024 * 1024)

    def record_modification(self, snapshot: FeatureSnapshot) -> None:
        """Record initial state of a modified feature. First call wins."""
        if snapshot.fid in self._modified:
            return
        if snapshot.fid in self._deleted:
            return
        self._modified[snapshot.fid] = snapshot
        self._approx_bytes += _estimate_snapshot_size(snapshot)

    def record_deletion(self, snapshot: FeatureSnapshot) -> None:
        """Record initial state of a deleted feature."""
        self._modified.pop(snapshot.fid, None)
        if snapshot.fid in self._deleted:
            return
        self._deleted[snapshot.fid] = snapshot
        self._approx_bytes += _estimate_snapshot_size(snapshot)

    def record_addition(self, fid: int) -> None:
        """Track a newly added feature ID (temporary FID from edit buffer)."""
        self._added_fids.add(fid)

    def record_committed_addition(self, data: Dict) -> None:
        """Store full feature data captured from committedFeaturesAdded signal."""
        self._committed_additions.append(data)

    def get_committed_additions(self) -> List[Dict]:
        return list(self._committed_additions)

    def get_modified_snapshots(self) -> Dict[int, FeatureSnapshot]:
        return dict(self._modified)

    def get_deleted_snapshots(self) -> Dict[int, FeatureSnapshot]:
        return dict(self._deleted)

    def get_added_fids(self) -> Set[int]:
        return set(self._added_fids)

    def needs_flush(self) -> bool:
        """Check if buffer exceeds memory thresholds."""
        if self.total_tracked > _DEFAULT_FEATURE_THRESHOLD:
            return True
        if self.approx_memory_mb > _DEFAULT_MEMORY_MB_THRESHOLD:
            return True
        return False

    def clear(self) -> None:
        """Discard all buffered data (rollback scenario)."""
        self._modified.clear()
        self._deleted.clear()
        self._added_fids.clear()
        self._committed_additions.clear()
        self._approx_bytes = 0
        flog(f"EditSessionBuffer: cleared for layer {self.layer_id}")

    def compute_net_effect(self) -> Dict[str, Set[int]]:
        """Compute the net effect of all tracked changes.

        Returns dict with keys: 'deleted', 'modified', 'added'.
        Features added then deleted are excluded (no net effect).
        Features modified then deleted appear only in 'deleted'.
        """
        added_then_deleted = self._added_fids & set(self._deleted.keys())
        net_deleted = set(self._deleted.keys()) - added_then_deleted
        net_added = self._added_fids - added_then_deleted
        net_modified = set(self._modified.keys()) - net_deleted - net_added
        return {
            "deleted": net_deleted,
            "modified": net_modified,
            "added": net_added,
        }


_DICT_BASE_OVERHEAD = 232
_ENTRY_OVERHEAD = 100
_LIST_ITEM_OVERHEAD = 8
_SNAPSHOT_OBJECT_OVERHEAD = 120


def _estimate_snapshot_size(snapshot: FeatureSnapshot) -> int:
    size = _SNAPSHOT_OBJECT_OVERHEAD + _DICT_BASE_OVERHEAD
    for key, val in snapshot.attributes.items():
        size += _ENTRY_OVERHEAD + len(key)
        if isinstance(val, str):
            size += len(val) + 50
        elif isinstance(val, (bytes, bytearray)):
            size += len(val) + 40
        else:
            size += 28
    if snapshot.geometry_wkb:
        size += len(snapshot.geometry_wkb) + 40
    size += _LIST_ITEM_OVERHEAD * (len(snapshot.field_names) + len(snapshot.changed_field_names))
    for name in snapshot.field_names:
        size += len(name) + 50
    return size


def create_snapshot_from_feature(feature, field_names: List[str],
                                 changed_field_names: Optional[List[str]] = None) -> FeatureSnapshot:
    """Build a FeatureSnapshot from a QgsFeature.

    Reads attributes and geometry WKB. Must be called from the main thread.
    """
    from .serialization import serialize_value
    attrs = {}
    for name in field_names:
        try:
            attrs[name] = serialize_value(feature[name])
        except (KeyError, IndexError):
            attrs[name] = None

    geom = feature.geometry()
    wkb = None
    if geom is not None and not geom.isNull() and not geom.isEmpty():
        wkb = bytes(geom.asWkb())

    return FeatureSnapshot(
        fid=feature.id(),
        attributes=attrs,
        geometry_wkb=wkb,
        field_names=field_names,
        changed_field_names=changed_field_names,
    )
