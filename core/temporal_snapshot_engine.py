"""Temporal snapshot engine — reconstruct data state at a given date T.

Zero QGIS dependency. Zero Qt dependency. Pure Python.

Takes an in-memory event_cache (populated by GeoGitCacheWorker, already
bounded to a time range) and a cutoff datetime, and returns a SnapshotResult
mapping entity_fp → SnapshotFeature for every entity that existed at T.

Algorithm (forward replay until T):
    For each fingerprint in event_cache:
        1. Group events by entity_fp (same key logic as lens_planner).
        2. Filter each entity's events to created_at <= cutoff_dt.
        3. Sort ASC by (created_at, event_id).
        4. Inspect the last visible event:
           - INSERT | UPDATE  → entity exists at T, record geom + attrs.
           - DELETE           → entity was absent at T (n_absent).
           - No events <= T   → entity not yet created at T (n_unknown).

Performance: O(N) over cached events. No SQL. ~1-5 ms per 5000 events.
"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, NamedTuple, Optional

from .logger import flog


# ------------------------------------------------------------------ #
# Contracts                                                            #
# ------------------------------------------------------------------ #


class SnapshotFeature(NamedTuple):
    """Reconstructed state of one entity at date T.

    geom_wkb   : geometry AT T (post-INSERT or post-UPDATE).
    attrs_json : raw attributes_json of last event <= T.
                 INSERT → full snapshot, UPDATE → delta (old+new per field).
    last_op    : "INSERT" | "UPDATE" — tells the overlay session which
                 interpretation to apply for attrs_json.
    """

    entity_fp: str
    geom_wkb: Optional[bytes]
    attrs_json: Optional[str]
    crs_authid: Optional[str]
    last_event_id: int
    last_op: str
    last_created_at: str


class SnapshotResult(NamedTuple):
    """Full outcome of reconstruct_snapshot_at.

    features  : {datasource_fp: {entity_fp: SnapshotFeature}} — present entities only.
    n_absent  : entities whose last event <= T was DELETE.
    n_unknown : fingerprints with no events <= cutoff (entity born after T).
    """

    features: dict
    cutoff_dt: datetime
    n_fps: int
    n_entities: int
    n_absent: int
    n_unknown: int
    elapsed_ms: int
    trace_id: str


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #


def reconstruct_snapshot_at(
    event_cache: Dict[str, list],
    cutoff_dt: datetime,
    trace_id: str = "",
) -> SnapshotResult:
    """Reconstruct entity states at cutoff_dt from the in-memory event cache.

    Args:
        event_cache : {fingerprint: [AuditEvent]} from GeoGitCacheWorker.
        cutoff_dt   : target instant (naive = assumed UTC).
        trace_id    : correlation id; auto-generated if empty.

    Returns:
        SnapshotResult with features dict and counters.
    """
    if not trace_id:
        trace_id = uuid.uuid4().hex[:8]

    t0 = time.monotonic()
    cutoff_utc = _to_utc(cutoff_dt)
    features: Dict[str, Dict] = {}
    n_absent = 0
    n_unknown = 0
    n_entities = 0

    for ds_fp, events in event_cache.items():
        entity_groups = _group_by_entity(events)
        ds_feats: Dict[str, SnapshotFeature] = {}
        for entity_fp, ev_list in entity_groups.items():
            visible = [e for e in ev_list if _before_cutoff(e, cutoff_utc)]
            if not visible:
                n_unknown += 1
                continue
            feat = _resolve_at_cutoff(entity_fp, visible)
            if feat is None:
                n_absent += 1
            else:
                ds_feats[entity_fp] = feat
        if ds_feats:
            features[ds_fp] = ds_feats
            n_entities += len(ds_feats)

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    flog(
        f"[{trace_id}] geogit_snapshot: reconstruct_at "
        f"cutoff={cutoff_utc.isoformat()} "
        f"n_fps={len(event_cache)} n_entities={n_entities} "
        f"n_absent={n_absent} n_unknown={n_unknown} "
        f"elapsed_ms={elapsed_ms}",
        "INFO",
    )
    return SnapshotResult(
        features=features,
        cutoff_dt=cutoff_utc,
        n_fps=len(event_cache),
        n_entities=n_entities,
        n_absent=n_absent,
        n_unknown=n_unknown,
        elapsed_ms=elapsed_ms,
        trace_id=trace_id,
    )


# ------------------------------------------------------------------ #
# Entity grouping                                                      #
# ------------------------------------------------------------------ #


def _entity_key(event) -> str:
    """Stable per-entity key — mirrors lens_planner._entity_key."""
    if event.entity_fingerprint:
        return event.entity_fingerprint
    return f"fid:{event.feature_identity_json}"


def _group_by_entity(events: list) -> Dict[str, list]:
    """Group and sort events by entity key. Ignores unknown op_types."""
    grouped: Dict[str, list] = {}
    for ev in events:
        if ev.operation_type not in ("INSERT", "UPDATE", "DELETE"):
            continue
        key = _entity_key(ev)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(ev)
    for ev_list in grouped.values():
        ev_list.sort(key=lambda e: (e.created_at or "", e.event_id or 0))
    return grouped


# ------------------------------------------------------------------ #
# Cutoff filtering                                                     #
# ------------------------------------------------------------------ #


def _before_cutoff(event, cutoff_utc: datetime) -> bool:
    """Return True if event.created_at <= cutoff_utc."""
    created = _parse_created_at(event.created_at)
    if created is None:
        return False
    return created <= cutoff_utc


def _parse_created_at(iso: Optional[str]) -> Optional[datetime]:
    """Parse ISO UTC string to timezone-aware datetime. None on failure."""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _to_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ------------------------------------------------------------------ #
# State resolution                                                     #
# ------------------------------------------------------------------ #


def _build_attrs_at_cutoff(visible: list) -> Optional[str]:
    """Replay INSERT base + UPDATE deltas to build full {field: value} at T."""
    attrs: Dict[str, object] = {}
    for ev in visible:
        if not ev.attributes_json:
            continue
        try:
            parsed = json.loads(ev.attributes_json)
        except (ValueError, TypeError):
            continue
        if not isinstance(parsed, dict):
            continue
        if ev.operation_type in ("INSERT", "DELETE"):
            base = parsed.get("all_attributes", {})
            if isinstance(base, dict):
                attrs.update(base)
        elif ev.operation_type == "UPDATE":
            changed = parsed.get("changed_only", {})
            if isinstance(changed, dict):
                for field, val in changed.items():
                    if isinstance(val, (list, tuple)) and len(val) == 2:
                        attrs[field] = val[1]
                    else:
                        attrs[field] = val
    return json.dumps(attrs, ensure_ascii=False) if attrs else None


def _resolve_at_cutoff(
    entity_fp: str,
    visible: list,
) -> Optional[SnapshotFeature]:
    """Build SnapshotFeature from the last visible event, or None if DELETE."""
    last = visible[-1]
    if last.operation_type == "DELETE":
        return None
    return SnapshotFeature(
        entity_fp=entity_fp,
        geom_wkb=_geom_at_cutoff(visible),
        attrs_json=_build_attrs_at_cutoff(visible),
        crs_authid=last.crs_authid,
        last_event_id=last.event_id or 0,
        last_op=last.operation_type,
        last_created_at=last.created_at or "",
    )


def _geom_at_cutoff(visible: list) -> Optional[bytes]:
    """Return the geometry the entity had at cutoff.

    Walks visible events backwards to find the last known geometry:
    - INSERT          : geometry_wkb (the created geometry).
    - UPDATE with geo : new_geometry_wkb, or geometry_wkb as fallback.
    - UPDATE attrs-only (both None): keep walking back to a prior event.
    """
    for ev in reversed(visible):
        op = ev.operation_type
        if op == "INSERT":
            if ev.geometry_wkb is None:
                flog(
                    f"geogit_snapshot: insert_no_geom event_id={ev.event_id}",
                    "WARNING",
                )
            return ev.geometry_wkb
        if op == "UPDATE":
            geom = ev.new_geometry_wkb or ev.geometry_wkb
            if geom is not None:
                return geom
    return None


__all__ = [
    "SnapshotFeature",
    "SnapshotResult",
    "reconstruct_snapshot_at",
]
