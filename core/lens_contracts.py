"""Pure Time Lens contracts for RecoverLand non-destructive history viewer.

Zero QGIS dependency. Zero Qt dependency. Defines visualization modes,
operation filters, entity classification, selection criteria, fetch
statistics and render plan/result structures.

Serves as the single source of truth for BL-IL-P0-02. All Lens logic
(planner, renderer, facade, UI) references these contracts.

Pattern jumeau strict de `core/restore_contracts.py` (invariant IL-I4 de
la charte Time Lens). NamedTuple immuables, Enums avec valeurs string
stables (append-only).
"""
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Tuple


class LensOpFilter(Enum):
    """Operation-level filter applied at fetch time.

    The user picks one value at a time in the dock UI (combo).
    """

    ALL = "all"
    INSERT_ONLY = "insert"
    UPDATE_ONLY = "update"
    DELETE_ONLY = "delete"
    ATTR_ONLY = "attr"
    GEOM_ONLY = "geom"


class LensVisualizationMode(Enum):
    """Rendering modes documented in backlog §6.

    Only DIFF_WINDOW (Mode B) is active at P0. SNAPSHOT_AT_T (Mode A)
    and UPDATES_STACKED (Mode C) ship in P1. ANIMATION (Mode D) in P3.
    """

    DIFF_WINDOW = "diff_window"
    SNAPSHOT_AT_T = "snapshot_at_t"
    UPDATES_STACKED = "updates_stacked"
    ANIMATION = "animation"


class EntityClassification(Enum):
    """How an entity intersects the spatio-temporal lens selection.

    Computed by `lens_planner.plan_lens_view` from the entity's audit
    timeline restricted to the selection window. Append-only.
    """

    CREATED_IN_ZONE = "created_in_zone"
    DELETED_FROM_ZONE = "deleted_from_zone"
    UPDATED_IN_ZONE = "updated_in_zone"
    MOVED_WITHIN_ZONE = "moved_within"
    MOVED_INTO_ZONE = "moved_into"
    MOVED_OUT_OF_ZONE = "moved_out"
    ATTR_ONLY_IN_ZONE = "attr_only"


# Selection / fetch boundary ----------------------------------------------


class LensSelection(NamedTuple):
    """User-defined spatio-temporal selection.

    Produced by `widgets/temporal_lens_dock`, consumed downstream all
    the way to the renderer. Immutable (NamedTuple).
    """

    layer_id_snapshot: str        # QGIS layer id at selection time
    datasource_fp: str            # core.identity.compute_datasource_fingerprint
    bbox_xy: Tuple[float, float, float, float]   # (xmin, ymin, xmax, ymax)
    bbox_crs: str                 # authid of the map canvas CRS, e.g. "EPSG:2154"
    t_min: str                    # ISO UTC, e.g. "2026-05-09T00:00:00Z"
    t_max: str                    # ISO UTC, exclusive upper bound
    op_filter: LensOpFilter
    mode: LensVisualizationMode = LensVisualizationMode.DIFF_WINDOW
    max_events: int = 5000        # hard cap for P0 (D-IL-04 = a)


class LensFetchStats(NamedTuple):
    """Statistics returned by the repository fetch call.

    Produced by `event_stream_repository.fetch_events_in_zone`.
    Allows the renderer to flag truncation and the dock to surface
    "showing 5000 of 12345 events" warnings.
    """

    n_events_total: int       # estimated count BEFORE max_events cap
    n_events_returned: int    # after limit AND post-fetch BBOX filter
    n_events_truncated: int   # max(0, n_events_total - n_events_returned)
    elapsed_ms: int           # wall clock of the fetch+filter step


# Per-entity reconstruction -----------------------------------------------


class EntityState(NamedTuple):
    """A single audit event in the perspective of one entity.

    Carries enough information to render an OLD/NEW geometry pair and
    an attribute diff without re-querying the journal.
    """

    event_id: int
    created_at: str                          # ISO UTC
    user_name: str
    operation_type: str                      # "INSERT" | "UPDATE" | "DELETE"
    old_geom_wkb: Optional[bytes]            # WKB of geometry BEFORE the event
    new_geom_wkb: Optional[bytes]            # WKB of geometry AFTER the event
    attrs_delta: Dict[str, Tuple[Any, Any]]  # field -> (old, new), audit-only fields excluded
    crs_authid: Optional[str]                # CRS of geometries, e.g. "EPSG:2154"


class EntityTimeline(NamedTuple):
    """Ordered, classified history of one entity within the lens selection."""

    entity_fp: str                       # core.identity.compute_entity_fingerprint
    classification: EntityClassification
    states: List[EntityState]            # sorted ascending by created_at
    n_events_filtered: int               # events dropped by op_filter


# Plan & result -----------------------------------------------------------


class LensRenderPlan(NamedTuple):
    """No-QGIS plan handed off to `lens_renderer.execute_lens_render`.

    Produced by `lens_planner.plan_lens_view`. Pure data; testable
    outside QGIS.
    """

    selection: LensSelection
    entities: Dict[str, EntityTimeline]  # key = entity_fingerprint
    fetch_stats: LensFetchStats
    plan_metadata: Dict[str, Any]        # provider_class, schema_version, layer_name, etc.


class LensRenderResult(NamedTuple):
    """Outcome of `lens_renderer.execute_lens_render`.

    Returned to the facade (`workflow_service.execute_grouped_lens_view`)
    which forwards it to the dock for legend updates and toast messages.
    """

    overlay_layer_ids: Tuple[str, ...]   # QgsProject ids of the memory layers
    n_entities: int                      # number of EntityTimelines rendered
    n_events_displayed: int              # sum of len(states) for displayed entities
    n_events_truncated: int              # propagated from LensFetchStats
    warnings: List[str]                  # PII filtered, geom repaired, CRS reprojected, ...
    elapsed_ms: int                      # total render wall clock


__all__ = [
    "LensOpFilter",
    "LensVisualizationMode",
    "EntityClassification",
    "LensSelection",
    "LensFetchStats",
    "EntityState",
    "EntityTimeline",
    "LensRenderPlan",
    "LensRenderResult",
]
