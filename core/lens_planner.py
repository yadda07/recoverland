"""Time Lens planner (BL-IL-P0-05).

Pattern jumeau strict de `core/restore_planner.py`. Zero QGIS, zero Qt.
Takes a bounded list of audit events fetched by
`event_stream_repository.fetch_events_in_zone` and turns it into a
ready-to-render `LensRenderPlan`:

    1. Group events by entity identity (entity_fingerprint preferred,
       feature_identity_json fallback for CR-IL-2 / shapefile FID
       recycling cases).
    2. Order each entity timeline chronologically by created_at then
       event_id.
    3. Build `EntityState` per event with old/new geometries and the
       attribute delta against the previous state.
    4. Classify the entity (CREATED_IN_ZONE, DELETED_FROM_ZONE,
       MOVED_INTO_ZONE, MOVED_OUT_OF_ZONE, ATTR_ONLY_IN_ZONE,
       UPDATED_IN_ZONE, MOVED_WITHIN_ZONE) based on first/last state
       + BBOX intersections.
    5. Bundle everything into a `LensRenderPlan` ready for the renderer.
"""
from __future__ import annotations

import json
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from .audit_backend import AuditEvent
from .lens_contracts import (
    EntityClassification,
    EntityState,
    EntityTimeline,
    LensFetchStats,
    LensRenderPlan,
    LensSelection,
)
from .wkb_envelope import envelope_intersects, parse_envelope


# ----- Entity grouping ---------------------------------------------------


def _entity_key(event: AuditEvent) -> str:
    """Stable per-entity grouping key.

    Preference order:
        1. entity_fingerprint (strong, schema v2+).
        2. feature_identity_json (legacy fallback, CR-IL-2: shapefiles
           with recycled FIDs may collide. The renderer flags the case).
    """
    if event.entity_fingerprint:
        return event.entity_fingerprint
    return f"fid:{event.feature_identity_json}"


# ----- Attribute delta ---------------------------------------------------


def _parse_attrs(json_str: Optional[str]) -> Dict[str, Any]:
    if not json_str:
        return {}
    try:
        parsed = json.loads(json_str)
    except (ValueError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    return parsed


def _compute_attrs_delta(
    old_json: Optional[str],
    new_json: Optional[str],
) -> Dict[str, Tuple[Any, Any]]:
    """Compute per-key (old, new) tuples for changed keys only.

    Keys present only on one side appear with the missing side = None.
    """
    old = _parse_attrs(old_json)
    new = _parse_attrs(new_json)
    delta: Dict[str, Tuple[Any, Any]] = {}
    for key in set(old) | set(new):
        ov = old.get(key)
        nv = new.get(key)
        if ov != nv:
            delta[key] = (ov, nv)
    return delta


# ----- State construction ------------------------------------------------


def _build_state(
    prev: Optional[AuditEvent],
    event: AuditEvent,
) -> EntityState:
    """Compose an EntityState from a (prev, current) audit event pair."""
    op = event.operation_type
    if op == "INSERT":
        old_geom = None
        new_geom = event.geometry_wkb
    elif op == "DELETE":
        old_geom = event.geometry_wkb
        new_geom = None
    elif op == "UPDATE":
        old_geom = event.geometry_wkb
        new_geom = event.new_geometry_wkb or event.geometry_wkb
    else:
        # Unknown op kept defensively with no geom change.
        old_geom = event.geometry_wkb
        new_geom = event.geometry_wkb

    prev_attrs = prev.attributes_json if prev is not None else None
    delta = _compute_attrs_delta(prev_attrs, event.attributes_json)

    return EntityState(
        event_id=event.event_id or 0,
        created_at=event.created_at,
        user_name=event.user_name,
        operation_type=op,
        old_geom_wkb=old_geom,
        new_geom_wkb=new_geom,
        attrs_delta=delta,
        crs_authid=event.crs_authid,
    )


# ----- Classification ----------------------------------------------------


def _classify_timeline(
    states: List[EntityState],
    bbox_xy: Tuple[float, float, float, float],
) -> EntityClassification:
    """Pick the right EntityClassification given the ordered timeline.

    Algorithm:
        - DELETE as last op   -> DELETED_FROM_ZONE.
        - INSERT as first op  -> CREATED_IN_ZONE.
        - Otherwise UPDATEs:
            * No effective geometry change anywhere -> ATTR_ONLY_IN_ZONE.
            * First-old OUT, last-new IN            -> MOVED_INTO_ZONE.
            * First-old IN, last-new OUT            -> MOVED_OUT_OF_ZONE.
            * Both ends inside, geom changed        -> MOVED_WITHIN_ZONE
              (UPDATED_IN_ZONE if no geometric shift was detected).
    """
    if not states:
        return EntityClassification.ATTR_ONLY_IN_ZONE

    first = states[0]
    last = states[-1]

    if last.operation_type == "DELETE":
        return EntityClassification.DELETED_FROM_ZONE
    if first.operation_type == "INSERT":
        return EntityClassification.CREATED_IN_ZONE

    geom_changed = any(
        st.old_geom_wkb != st.new_geom_wkb for st in states
    )
    if not geom_changed:
        return EntityClassification.ATTR_ONLY_IN_ZONE

    first_in = envelope_intersects(parse_envelope(first.old_geom_wkb), bbox_xy)
    last_in = envelope_intersects(parse_envelope(last.new_geom_wkb), bbox_xy)

    if first_in and not last_in:
        return EntityClassification.MOVED_OUT_OF_ZONE
    if not first_in and last_in:
        return EntityClassification.MOVED_INTO_ZONE
    if first_in and last_in:
        return EntityClassification.MOVED_WITHIN_ZONE
    # both endpoints out-of-zone yet entity was returned: rendered as
    # UPDATED_IN_ZONE because at least one intermediate state must have
    # touched the bbox for the repository to have surfaced it.
    return EntityClassification.UPDATED_IN_ZONE


# ----- Public API --------------------------------------------------------


def plan_lens_view(
    events: List[AuditEvent],
    selection: LensSelection,
    layer_name: str,
    fetch_stats: LensFetchStats,
) -> LensRenderPlan:
    """Build a `LensRenderPlan` from a bounded audit event list.

    Args:
        events: list returned by `fetch_events_in_zone`; the planner
            does not re-filter on time or BBOX (assumed pre-filtered).
        selection: the immutable user selection that produced *events*.
        layer_name: human-readable layer name for the plan metadata
            (the renderer uses it for layer titles and toast messages).
        fetch_stats: statistics propagated from the repository.

    Returns:
        A fully populated `LensRenderPlan` ready for the renderer.
    """
    grouped: Dict[str, List[AuditEvent]] = defaultdict(list)
    for ev in events:
        if ev.operation_type not in ("INSERT", "UPDATE", "DELETE"):
            continue
        grouped[_entity_key(ev)].append(ev)

    timelines: Dict[str, EntityTimeline] = {}
    for key, ev_list in grouped.items():
        ev_list.sort(key=lambda e: (e.created_at, e.event_id or 0))
        prev: Optional[AuditEvent] = None
        states: List[EntityState] = []
        for ev in ev_list:
            states.append(_build_state(prev, ev))
            prev = ev
        classification = _classify_timeline(states, selection.bbox_xy)
        timelines[key] = EntityTimeline(
            entity_fp=key,
            classification=classification,
            states=states,
            n_events_filtered=0,
        )

    return LensRenderPlan(
        selection=selection,
        entities=timelines,
        fetch_stats=fetch_stats,
        plan_metadata={
            "layer_name": layer_name,
            "n_entities": len(timelines),
            "n_events": sum(len(t.states) for t in timelines.values()),
        },
    )


__all__ = [
    "plan_lens_view",
]
