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
    trace_id: str = "",
) -> GroupedRestoreResult:
    """Execute restore grouped by datasource fingerprint.

    Args:
        events: audit events to restore (in order).
        find_layer_fn: callable(event) -> QgsVectorLayer or None.
        on_group_done: optional callback(processed_count, total_count)
            for progress updates.
        trace_id: BL-RW-P3-17 — propagated to restore_batch so executor
            logs share the rewind chain prefix.

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

        report = restore_batch(layer, group, trace_id=trace_id)
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


# ----- Time Lens lifecycle (BL-IL-P0-09, CR-IL-6) -----------------------


_LENS_LAYER_PREFIX = "__rl_lens_"
_lens_layer_ids: List[str] = []


def purge_lens_overlays(context: str = "manual") -> int:
    """Remove every map layer whose name starts with the Time Lens prefix.

    Called from `recover.initGui` (context="startup"), `recover.unload`
    (context="shutdown"), and `execute_grouped_lens_view` (context=
    "refresh") to enforce acceptance section 1, 2 and 3 of BL-IL-P0-09.
    Loops over ``QgsProject.instance().mapLayers()`` and matches by
    ``layer.name().startswith(_LENS_LAYER_PREFIX)`` so the purge also
    catches layers persisted in a saved .qgs file (CR-IL-6 antithese
    A2: a project saved with Lens active is cleaned at next open).

    Args:
        context: free-form tag carried into the log signature
            (``startup``, ``shutdown``, ``refresh``, ``manual``...).

    Returns:
        Number of layers actually removed.
    """
    try:
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        ids = [
            lyr.id() for lyr in project.mapLayers().values()
            if lyr.name().startswith(_LENS_LAYER_PREFIX)
        ]
        for lid in ids:
            project.removeMapLayer(lid)
        _lens_layer_ids.clear()
        flog(
            f"lens_lifecycle event={context}_cleanup "
            f"n_orphan_layers={len(ids)}",
            "INFO" if ids else "DEBUG",
        )
        return len(ids)
    except Exception as exc:  # noqa: BLE001
        flog(
            f"lens_lifecycle event={context}_cleanup_failed "
            f"type={type(exc).__name__}",
            "WARNING",
        )
        return 0


def execute_grouped_lens_view(
    events,
    selection,
    layer_name: str,
    fetch_stats,
    dst_crs_authid: str,
    trace_id: str = "",
):
    """Plan + render facade for the Time Lens dock (BL-IL-P0-09).

    Acceptance section 3 of BL-IL-P0-09: every refresh purges the
    previous overlay AGAINST QgsProject before adding the new one.
    No accumulation, no layer leak between successive clicks on the
    Rafraichir button.

    Args:
        events: bounded list of `AuditEvent` from
            `event_stream_repository.fetch_events_in_zone`.
        selection: the immutable `LensSelection` that produced *events*.
        layer_name: human-readable layer name (forwarded to the planner
            metadata; used by the dock for titles and toast messages).
        fetch_stats: `LensFetchStats` propagated from the repository.
        dst_crs_authid: canvas CRS authority id (e.g. "EPSG:3857").
        trace_id: opaque correlation id propagated end-to-end.

    Returns:
        ``LensRefreshOutcome(plan, result)`` (BL-IL-P0-10c). The dock
        uses ``outcome.result.overlay_layer_ids`` for legend toggle /
        ``outcome.result.warnings`` for truncation banner, and
        ``outcome.plan.entities`` for the clickable entity list and
        the attribute diff panel.

        Phase 10b returned the bare ``LensRenderResult``; phase 10c
        aggregates plan + result so the dock does not have to recall
        ``plan_lens_view`` just to obtain the per-entity timelines.

    Emits:
        flog: lens_lifecycle event=refresh trace_id=<id>
              n_removed=<n> n_added=<n> n_entities=<n> elapsed_ms=<n>
    """
    import time as _time  # noqa: PLC0415
    from .lens_contracts import LensRefreshOutcome  # noqa: PLC0415
    from .lens_planner import plan_lens_view  # noqa: PLC0415
    from .lens_renderer import execute_lens_render  # noqa: PLC0415

    t0 = _time.monotonic()
    n_removed = purge_lens_overlays("refresh")

    plan = plan_lens_view(events, selection, layer_name, fetch_stats)
    result = execute_lens_render(plan, dst_crs_authid, trace_id=trace_id)

    _lens_layer_ids.clear()
    _lens_layer_ids.extend(result.overlay_layer_ids)

    elapsed_ms = int((_time.monotonic() - t0) * 1000)
    flog(
        f"lens_lifecycle event=refresh trace_id={trace_id} "
        f"n_removed={n_removed} n_added={len(result.overlay_layer_ids)} "
        f"n_entities={result.n_entities} elapsed_ms={elapsed_ms}",
        "INFO",
    )
    return LensRefreshOutcome(plan=plan, result=result)
