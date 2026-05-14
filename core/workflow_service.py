"""Workflow service for RecoverLand restore operations.

Centralizes the grouped-restore and undo-restore logic that was
previously duplicated in the dialog. Pure business logic; no Qt widgets.
"""
from collections import defaultdict
from typing import List, Dict, Optional, Callable, NamedTuple

from .audit_backend import AuditEvent
from .restore_service import restore_batch, undo_restore_batch
from .logger import flog


_temp_layer_ids: List[str] = []


def cleanup_temp_layers() -> int:
    """Remove temporary layers added during restore (RW-15).

    Returns number of layers removed.
    """
    if not _temp_layer_ids:
        return 0
    try:
        from qgis.core import QgsProject
        project = QgsProject.instance()
        removed = 0
        for lid in list(_temp_layer_ids):
            if project.mapLayer(lid) is not None:
                project.removeMapLayer(lid)
                removed += 1
        flog(f"cleanup_temp_layers: removed={removed}/{len(_temp_layer_ids)}")
        _temp_layer_ids.clear()
        return removed
    except Exception as exc:
        flog(f"cleanup_temp_layers: failed: {exc}", "WARNING")
        _temp_layer_ids.clear()
        return 0


class GroupedRestoreResult(NamedTuple):
    total_ok: int
    total_fail: int
    errors: List[str]
    by_ds: Dict[str, list]
    trace_events: list
    failed_eids: List[int] = []
    # BL-RW-P3-18: per-category breakdown propagated by the runners.
    # `applied` is a strict subset of `total_ok`: it excludes events that
    # the executor short-circuited as `skipped_idempotent`, `target_absent`
    # or `geometry_drift`. Conservation invariant:
    #   total_ok + total_fail == applied + skipped_idempotent
    #                          + failed + failed_target_absent
    #                          + failed_geometry_drift
    # (plus a `cancelled` delta when the runner was interrupted).
    applied: int = 0
    skipped_idempotent: int = 0
    failed: int = 0
    failed_target_absent: int = 0
    failed_geometry_drift: int = 0


def execute_grouped_restore(
    events: List[AuditEvent],
    find_layer_fn: Callable[[AuditEvent], object],
    on_group_done: Optional[Callable[[int, int], None]] = None,
) -> GroupedRestoreResult:
    """Execute restore grouped by datasource fingerprint.

    Args:
        events: audit events to restore (in order).
        find_layer_fn: callable(event) -> QgsVectorLayer or None.
        on_group_done: optional callback(processed_count, total_count)
            for progress updates.

    Returns:
        GroupedRestoreResult with totals, errors, by_ds map, and trace events.
    """
    by_ds: Dict[str, list] = defaultdict(list)
    for event in events:
        by_ds[event.datasource_fingerprint].append(event)

    total_ok, total_fail = 0, 0
    errors: List[str] = []
    all_traces: list = []
    processed = 0

    for fp, group in by_ds.items():
        layer = find_layer_fn(group[0])
        if layer is None:
            name = group[0].layer_name_snapshot or fp
            errors.append(f"Couche '{name}' non trouvee dans le projet.")
            total_fail += len(group)
            processed += len(group)
            if on_group_done is not None:
                on_group_done(processed, len(events))
            continue

        report = restore_batch(layer, group)
        total_ok += len(report.succeeded)
        total_fail += len(report.failed)
        for eid, msg in report.failed.items():
            errors.append(f"Evt {eid}: {msg}")
        all_traces.extend(report.trace_events)

        if report.succeeded:
            layer.reload()
        layer.triggerRepaint()

        processed += len(group)
        if on_group_done is not None:
            on_group_done(processed, len(events))

    return GroupedRestoreResult(
        total_ok=total_ok,
        total_fail=total_fail,
        errors=errors,
        by_ds=dict(by_ds),
        trace_events=all_traces,
    )


def execute_grouped_undo(
    by_ds: Dict[str, list],
    find_layer_fn: Callable[[AuditEvent], object],
) -> GroupedRestoreResult:
    """Undo a previous grouped restore.

    Args:
        by_ds: dict mapping datasource fingerprints to event lists
            (as returned by execute_grouped_restore).
        find_layer_fn: callable(event) -> QgsVectorLayer or None.

    Returns:
        GroupedRestoreResult (trace_events always empty for undo).
    """
    total_ok, total_fail = 0, 0
    errors: List[str] = []

    for fp, group in by_ds.items():
        layer = find_layer_fn(group[0])
        if layer is None:
            name = group[0].layer_name_snapshot or fp
            errors.append(f"Couche '{name}' non trouvee dans le projet.")
            total_fail += len(group)
            continue

        report = undo_restore_batch(layer, group)
        total_ok += len(report.succeeded)
        total_fail += len(report.failed)
        for eid, msg in report.failed.items():
            errors.append(f"Evt {eid}: {msg}")

        if report.succeeded:
            layer.reload()
        layer.triggerRepaint()

    return GroupedRestoreResult(
        total_ok=total_ok,
        total_fail=total_fail,
        errors=errors,
        by_ds=dict(by_ds),
        trace_events=[],
    )


def find_target_layer(event: AuditEvent, read_conn=None) -> object:
    """Find the QGIS layer matching an audit event.

    Search order:
    1. Layer ID match in loaded project layers.
    2. Datasource fingerprint match in loaded layers.
    3. Recreate from datasource registry (if read_conn provided).

    Returns QgsVectorLayer or None.
    """
    from qgis.core import QgsProject, QgsVectorLayer
    from .identity import compute_datasource_fingerprint

    for layer in QgsProject.instance().mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue
        if layer.id() == event.layer_id_snapshot:
            return layer

    for layer in QgsProject.instance().mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue
        try:
            if compute_datasource_fingerprint(layer) == event.datasource_fingerprint:
                return layer
        except Exception:
            continue

    if read_conn is not None:
        return _try_restore_from_registry(event, read_conn)
    return None


def _try_restore_from_registry(event: AuditEvent, read_conn) -> object:
    """Recreate a layer from the datasource registry.

    For DB-backed layers (postgres, mssql, oracle): resolves credentials
    from authcfg or QGIS saved connections. Falls back to None if the
    connection cannot be established.
    """
    try:
        from qgis.core import QgsProject
        from .datasource_registry import (
            lookup_datasource, create_layer_from_registry, _DB_PROVIDERS,
        )

        info = lookup_datasource(read_conn, event.datasource_fingerprint)
        if info is None:
            flog(f"find_target_layer: no registry for {event.datasource_fingerprint}")
            return None

        flog(f"find_target_layer: registry hit provider={info.provider_type} "
             f"layer={info.layer_name}")

        layer = create_layer_from_registry(info)
        if layer is not None and layer.isValid():
            QgsProject.instance().addMapLayer(layer, False)
            _temp_layer_ids.append(layer.id())
            flog(f"find_target_layer: temp layer added for restore "
                 f"provider={info.provider_type} id={layer.id()}")
            return layer

        if info.provider_type in _DB_PROVIDERS:
            flog(f"find_target_layer: DB layer '{info.layer_name}' could not "
                 f"reconnect; load it manually in the project first", "WARNING")
        return None
    except Exception as e:
        flog(f"find_target_layer: registry fallback failed: {e}", "WARNING")
        return None
