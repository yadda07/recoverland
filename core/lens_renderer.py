"""Time Lens renderer (BL-IL-P0-08, phases 08a + 08b).

Consumes a `LensRenderPlan` produced by `core/lens_planner.plan_lens_view`
and materialises three QGIS memory overlay layers:

    1. `__rl_lens_<uuid8>_geom_past`
       The OLD geometry of every audit event in the plan. The geometry
       type is detected from the first non-empty WKB; mixed types in a
       single plan are not supported yet (warning emitted, additional
       types are silently skipped).

    2. `__rl_lens_<uuid8>_arrows`
       A LineString from centroid(OLD) -> centroid(NEW) for every UPDATE
       event whose OLD and NEW geometries are byte-distinct. Zero-length
       arrows (geometry unchanged) are filtered: the attr_markers layer
       carries the attr-only signal instead.

    3. `__rl_lens_<uuid8>_attr_markers`
       A Point at centroid(NEW or OLD) for every UPDATE event that
       carries a non-empty `attrs_delta` AND whose geometry did not
       change between OLD and NEW (or where only one of the two is
       present). Phase 08b: this is what the dock surfaces as the
       "i" marker (info: only attributes changed).

Truncation banner: when `plan.fetch_stats.n_events_truncated > 0`,
`LensRenderResult.warnings` carries `truncated:<n>` so the dock can
render the red header banner described by acceptance §4.

Each feature carries the attribute set required by acceptance §3 of
BL-IL-P0-08: `entity_fp`, `event_id`, `created_at`, `op`, `user`,
`age_label`, `is_repaired`, `classification`.

Single-symbol symbology is shipped at this stage. Categorised gradient
over `age_label` is deferred to a follow-up symbology pass.

Anti-silo IL-I4 OK: this module is **not** in the forbidden list of
levels 0-4 (see lens_charter.md §5). It sits at level 5 alongside
`restore_executor` and `restore_service`.

The geometry hot-path uses helpers shipped in earlier P0 items:

    - `repair_geometry_for_render` (BL-IL-P0-07, CR-IL-4)
    - `reproject_geometry_for_render` (BL-IL-P0-06, CR-IL-3)
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple


# ----- Geometry type detection ------------------------------------------


def _wkb_type_code(wkb: bytes) -> Optional[int]:
    """Extract the wkb type code from the first 5 bytes of a WKB blob.

    Returns the raw int32 (LE/BE handled) or None if the blob is too
    short. The renderer only cares about the family
    (1=Point, 2=LineString, 3=Polygon, 4=MultiPoint, 5=MultiLineString,
    6=MultiPolygon; +1000=Z, +2000=M, +3000=ZM).
    """
    if wkb is None or len(wkb) < 5:
        return None
    is_le = wkb[0] == 1
    type_bytes = wkb[1:5]
    return (
        int.from_bytes(type_bytes, "little")
        if is_le
        else int.from_bytes(type_bytes, "big")
    )


def _wkb_family(type_code: int) -> str:
    """Map a wkb type code to a memory-layer URI geometry keyword."""
    base = type_code % 1000  # drop Z/M/ZM offsets
    return {
        1: "Point",
        2: "LineString",
        3: "Polygon",
        4: "MultiPoint",
        5: "MultiLineString",
        6: "MultiPolygon",
    }.get(base, "Polygon")


def _detect_past_geom_family(plan, warnings: List[str]) -> Optional[str]:
    """Pick the geometry family of the first old_geom_wkb in the plan.

    Mixed-type plans (e.g. a layer that mixes Polygon and MultiPolygon)
    are currently rendered against the first encountered family; events
    with a divergent family are skipped and counted in *warnings*.
    """
    for timeline in plan.entities.values():
        for state in timeline.states:
            if state.old_geom_wkb:
                code = _wkb_type_code(state.old_geom_wkb)
                if code is not None:
                    return _wkb_family(code)
    warnings.append("no_old_geom_in_plan")
    return None


# ----- Age label --------------------------------------------------------


def _age_label(created_at_iso: str, now: Optional[datetime] = None) -> str:
    """Return a short human-readable age label from an ISO UTC timestamp.

    Pure function: deterministic when *now* is provided (the renderer
    injects QgsProject-time so tests can pin the value).
    """
    if not created_at_iso:
        return "?"
    try:
        ts = datetime.fromisoformat(created_at_iso.replace("Z", "+00:00"))
    except ValueError:
        return "?"
    if now is None:
        now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta_days = (now - ts).days
    if delta_days < 1:
        return "today"
    if delta_days == 1:
        return "1d"
    if delta_days < 30:
        return f"{delta_days}d"
    if delta_days < 365:
        return f"{delta_days // 30}mo"
    return f"{delta_days // 365}y"


# ----- Memory layer construction ----------------------------------------


_ATTR_FIELDS = (
    ("entity_fp", "string", 64),
    ("event_id", "integer", None),
    ("created_at", "string", 32),
    ("op", "string", 8),
    ("user", "string", 64),
    ("age_label", "string", 16),
    ("age_days", "integer", None),
    ("is_repaired", "integer", None),
    ("classification", "string", 32),
)

_GEOGIT_GROUP_NAME = "GeoGit"


def _build_memory_uri(geom_family: str, dst_crs_authid: str) -> str:
    """Build a `QgsVectorLayer` memory provider URI with the attr schema."""
    parts = [f"{geom_family}?crs={dst_crs_authid}"]
    for name, qtype, length in _ATTR_FIELDS:
        seg = f"field={name}:{qtype}"
        if length is not None:
            seg += f"({length})"
        parts.append(seg)
    return "&".join(parts)


def _make_overlay_layer(
    name: str,
    geom_family: str,
    dst_crs_authid: str,
):
    """Create a memory `QgsVectorLayer` with the Lens attribute schema."""
    from qgis.core import QgsVectorLayer  # noqa: PLC0415
    uri = _build_memory_uri(geom_family, dst_crs_authid)
    layer = QgsVectorLayer(uri, name, "memory")
    return layer


def _age_days(created_at_iso: str, now: Optional[datetime] = None) -> int:
    """Return the number of days since the event. 0 = today."""
    if not created_at_iso:
        return 9999
    try:
        ts = datetime.fromisoformat(created_at_iso.replace("Z", "+00:00"))
    except ValueError:
        return 9999
    if now is None:
        now = datetime.now(timezone.utc)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0, (now - ts).days)


def _feature_attrs(
    entity_fp: str,
    state,
    classification: str,
    now: datetime,
) -> List:
    return [
        entity_fp,
        int(state.event_id) if state.event_id is not None else None,
        state.created_at,
        state.operation_type,
        state.user_name or "",
        _age_label(state.created_at, now),
        _age_days(state.created_at, now),
        0,  # is_repaired: phase 08a does not track repair drift yet
        classification,
    ]


# ----- Style inheritance (BL-IL-P1-13) ----------------------------------

def _apply_source_style(overlay_layer, source_layer, trace_id: str = ""):
    """Clone the source layer style + graduated opacity by age_days.

    Uses ``QgsMapLayerStyle`` to snapshot the source layer rendering,
    then sets a data-defined opacity expression on the symbol so older
    features appear more transparent (graduated from 90% to 25%).
    """
    from .logger import flog  # noqa: PLC0415

    if source_layer is None:
        return
    try:
        from qgis.core import (  # noqa: PLC0415
            QgsMapLayerStyle,
            QgsProperty,
        )

        style = QgsMapLayerStyle()
        style.readFromLayer(source_layer)
        if not style.isValid():
            flog(
                f"lens_renderer event=style_clone_empty "
                f"trace_id={trace_id} source={source_layer.name()}",
                "DEBUG",
            )
            return
        style.writeToLayer(overlay_layer)
        overlay_layer.setOpacity(1.0)
        _apply_age_opacity(overlay_layer, trace_id)
        overlay_layer.triggerRepaint()
        flog(
            f"lens_renderer event=style_cloned "
            f"trace_id={trace_id} source={source_layer.name()} "
            f"opacity=graduated_by_age_days",
            "INFO",
        )
    except Exception as exc:  # noqa: BLE001
        flog(
            f"lens_renderer event=style_clone_error "
            f"trace_id={trace_id} type={type(exc).__name__}",
            "WARNING",
        )


def _apply_age_opacity(overlay_layer, trace_id: str = "") -> None:
    """Set data-defined opacity on each symbol level based on age_days.

    Expression: scale_linear(age_days, 0, 30, 90, 25) clamped.
    - today (age_days=0) → 90% opacity
    - 30+ days → 25% opacity
    """
    from .logger import flog  # noqa: PLC0415

    try:
        from qgis.core import QgsProperty  # noqa: PLC0415

        renderer = overlay_layer.renderer()
        if renderer is None:
            return
        expr = (
            "scale_linear(\"age_days\", 0, 30, 90, 25)"
        )
        prop = QgsProperty.fromExpression(expr)
        symbol = renderer.symbol() if hasattr(renderer, 'symbol') else None
        if symbol is not None:
            for i in range(symbol.symbolLayerCount()):
                sl = symbol.symbolLayer(i)
                sl.setDataDefinedProperty(
                    sl.PropertyFillColor if hasattr(sl, 'PropertyFillColor')
                    else 0,
                    QgsProperty(),
                )
            symbol.setDataDefinedProperty(
                symbol.PropertyOpacity, prop,
            )
        else:
            symbols = renderer.symbols(
                overlay_layer.createMapRenderer(None)
                if hasattr(overlay_layer, 'createMapRenderer')
                else None
            ) if hasattr(renderer, 'symbols') else []
            for sym in (symbols or []):
                sym.setDataDefinedProperty(
                    sym.PropertyOpacity, prop,
                )
        flog(
            f"lens_renderer event=age_opacity_applied trace_id={trace_id}",
            "DEBUG",
        )
    except Exception as exc:  # noqa: BLE001
        flog(
            f"lens_renderer event=age_opacity_error "
            f"trace_id={trace_id} type={type(exc).__name__} err={exc!r}",
            "WARNING",
        )


# ----- Public API -------------------------------------------------------


def execute_lens_render(
    plan,
    dst_crs_authid: str,
    trace_id: str = "",
    source_layer=None,
):
    """Materialise the two phase-08a overlay memory layers.

    Args:
        plan: a `LensRenderPlan` (see `core/lens_contracts.py`).
        dst_crs_authid: authority id of the destination CRS (canvas CRS),
            e.g. ``"EPSG:3857"``.
        trace_id: opaque correlation id propagated in log signatures.

    Returns:
        A `LensRenderResult` whose `overlay_layer_ids` carries the QGIS
        layer ids of the two memory layers that were just added to
        `QgsProject.instance()`.

    Emits:
        flog: lens_renderer event=overlay_built trace_id=<id>
              layers=<n> n_features_past=<n> n_features_arrows=<n>
              n_skipped_geom=<n> truncated=<bool> elapsed_ms=<n>
    """
    from qgis.core import (  # noqa: PLC0415
        QgsFeature,
        QgsGeometry,
        QgsProject,
    )
    from .geometry_utils import (  # noqa: PLC0415
        geometries_equal,
        is_geometry_present,
        repair_geometry_for_render,
        reproject_geometry_for_render,
    )
    from .lens_contracts import LensRenderResult  # noqa: PLC0415
    from .logger import flog  # noqa: PLC0415

    t0 = time.monotonic()
    warnings: List[str] = []
    transform_cache: Dict[Tuple[str, str], object] = {}

    geom_family = _detect_past_geom_family(plan, warnings)
    if geom_family is None:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        flog(
            f"lens_renderer event=overlay_built trace_id={trace_id} "
            f"layers=0 reason=no_past_geom elapsed_ms={elapsed_ms}",
            "WARNING",
        )
        return LensRenderResult(
            overlay_layer_ids=tuple(),
            n_entities=len(plan.entities),
            n_events_displayed=0,
            n_events_truncated=plan.fetch_stats.n_events_truncated,
            warnings=warnings,
            elapsed_ms=elapsed_ms,
        )

    uuid8 = uuid.uuid4().hex[:8]
    past_name = f"__rl_lens_{uuid8}_geom_past"
    arrows_name = f"__rl_lens_{uuid8}_arrows"
    attr_name = f"__rl_lens_{uuid8}_attr_markers"
    past_layer = _make_overlay_layer(past_name, geom_family, dst_crs_authid)
    arrows_layer = _make_overlay_layer(arrows_name, "LineString", dst_crs_authid)
    attr_layer = _make_overlay_layer(attr_name, "Point", dst_crs_authid)

    now = datetime.now(timezone.utc)
    past_feats: List[QgsFeature] = []
    arrow_feats: List[QgsFeature] = []
    attr_feats: List[QgsFeature] = []
    n_skipped_geom = 0

    for entity_fp, timeline in plan.entities.items():
        classification = (
            timeline.classification.name
            if hasattr(timeline.classification, "name")
            else str(timeline.classification)
        )
        for state in timeline.states:
            # --- past geometry feature ---
            if state.old_geom_wkb:
                code = _wkb_type_code(state.old_geom_wkb)
                fam = _wkb_family(code) if code is not None else None
                if fam != geom_family:
                    n_skipped_geom += 1
                else:
                    geom = repair_geometry_for_render(
                        state.old_geom_wkb, trace_id=trace_id,
                    )
                    if is_geometry_present(geom):
                        repro = reproject_geometry_for_render(
                            geom, state.crs_authid, dst_crs_authid,
                            transform_cache, trace_id=trace_id,
                        )
                        if is_geometry_present(repro):
                            f = QgsFeature(past_layer.fields())
                            f.setGeometry(repro)
                            f.setAttributes(_feature_attrs(
                                entity_fp, state, classification, now,
                            ))
                            past_feats.append(f)
                        else:
                            n_skipped_geom += 1
                    else:
                        n_skipped_geom += 1

            # --- UPDATE: pick arrow (geom changed) OR attr marker ---
            if state.operation_type == "UPDATE":
                geom_unchanged = geometries_equal(
                    state.old_geom_wkb, state.new_geom_wkb,
                )
                has_attrs_delta = bool(state.attrs_delta)
                has_both_geoms = bool(
                    state.old_geom_wkb and state.new_geom_wkb,
                )

                if has_both_geoms and not geom_unchanged:
                    old_g = repair_geometry_for_render(
                        state.old_geom_wkb, trace_id=trace_id,
                    )
                    new_g = repair_geometry_for_render(
                        state.new_geom_wkb, trace_id=trace_id,
                    )
                    if (
                        is_geometry_present(old_g)
                        and is_geometry_present(new_g)
                    ):
                        old_repro = reproject_geometry_for_render(
                            old_g, state.crs_authid, dst_crs_authid,
                            transform_cache, trace_id=trace_id,
                        )
                        new_repro = reproject_geometry_for_render(
                            new_g, state.crs_authid, dst_crs_authid,
                            transform_cache, trace_id=trace_id,
                        )
                        if (
                            is_geometry_present(old_repro)
                            and is_geometry_present(new_repro)
                        ):
                            try:
                                old_c = old_repro.centroid().asPoint()
                                new_c = new_repro.centroid().asPoint()
                                arrow = QgsGeometry.fromPolylineXY(
                                    [old_c, new_c],
                                )
                                f = QgsFeature(arrows_layer.fields())
                                f.setGeometry(arrow)
                                f.setAttributes(_feature_attrs(
                                    entity_fp, state, classification, now,
                                ))
                                arrow_feats.append(f)
                            except Exception:  # noqa: BLE001
                                pass

                elif has_attrs_delta:
                    # attr-only: geometry unchanged or one side missing.
                    src_wkb = state.new_geom_wkb or state.old_geom_wkb
                    if src_wkb:
                        marker_g = repair_geometry_for_render(
                            src_wkb, trace_id=trace_id,
                        )
                        if is_geometry_present(marker_g):
                            marker_repro = reproject_geometry_for_render(
                                marker_g, state.crs_authid, dst_crs_authid,
                                transform_cache, trace_id=trace_id,
                            )
                            if is_geometry_present(marker_repro):
                                try:
                                    pt = marker_repro.centroid()
                                    f = QgsFeature(attr_layer.fields())
                                    f.setGeometry(pt)
                                    f.setAttributes(_feature_attrs(
                                        entity_fp, state, classification, now,
                                    ))
                                    attr_feats.append(f)
                                except Exception:  # noqa: BLE001
                                    pass

    if past_feats:
        past_layer.dataProvider().addFeatures(past_feats)
        past_layer.updateExtents()
    if arrow_feats:
        arrows_layer.dataProvider().addFeatures(arrow_feats)
        arrows_layer.updateExtents()
    if attr_feats:
        attr_layer.dataProvider().addFeatures(attr_feats)
        attr_layer.updateExtents()

    _apply_source_style(past_layer, source_layer, trace_id)

    project = QgsProject.instance()
    root = project.layerTreeRoot()
    group = root.findGroup(_GEOGIT_GROUP_NAME)
    if group is None:
        group = root.insertGroup(0, _GEOGIT_GROUP_NAME)
    project.addMapLayer(past_layer, False)
    project.addMapLayer(arrows_layer, False)
    project.addMapLayer(attr_layer, False)
    group.addLayer(past_layer)
    group.addLayer(arrows_layer)
    group.addLayer(attr_layer)

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    flog(
        f"lens_renderer event=overlay_built trace_id={trace_id} "
        f"layers=3 n_features_past={len(past_feats)} "
        f"n_features_arrows={len(arrow_feats)} "
        f"n_features_attr={len(attr_feats)} "
        f"n_skipped_geom={n_skipped_geom} "
        f"truncated={plan.fetch_stats.n_events_truncated > 0} "
        f"elapsed_ms={elapsed_ms}",
        "INFO",
    )

    if n_skipped_geom > 0:
        warnings.append(f"geom_family_mismatch:{n_skipped_geom}")
    if plan.fetch_stats.n_events_truncated > 0:
        warnings.append(f"truncated:{plan.fetch_stats.n_events_truncated}")

    return LensRenderResult(
        overlay_layer_ids=(
            past_layer.id(), arrows_layer.id(), attr_layer.id(),
        ),
        n_entities=len(plan.entities),
        n_events_displayed=(
            len(past_feats) + len(arrow_feats) + len(attr_feats)
        ),
        n_events_truncated=plan.fetch_stats.n_events_truncated,
        warnings=warnings,
        elapsed_ms=elapsed_ms,
    )


__all__ = [
    "execute_lens_render",
]
