"""Pure restore contracts for RecoverLand time-travel restore.

Zero QGIS dependency. Defines modes, scopes, cutoffs, limits,
conflict policies, provider matrix, and plan structures.

Serves as the single source of truth for BL-00 (cadrage) and
BL-01 (noyau metier). All restore logic references these contracts.
"""
from enum import Enum
from typing import NamedTuple, List, Dict, Optional, Any, Tuple


class RestoreMode(Enum):
    EVENT = "event"
    TEMPORAL = "temporal"


class RestoreScope(Enum):
    ENTITY = "entity"
    SELECTION = "selection"
    LAYER = "layer"
    DATASOURCE = "datasource"


class CutoffType(Enum):
    BY_DATE = "by_date"
    BY_EVENT_ID = "by_event_id"


class ConflictPolicy(Enum):
    ABORT = "abort"
    SKIP = "skip"
    FORCE = "force"


class AtomicityPolicy(Enum):
    STRICT = "strict"
    BEST_EFFORT = "best_effort"


class PreflightVerdict(Enum):
    GO = "go"
    GO_WITH_WARNINGS = "go_with_warnings"
    BLOCKED = "blocked"


COMPENSATORY_OPS: Dict[str, str] = {
    "DELETE": "INSERT",
    "UPDATE": "UPDATE",
    "INSERT": "DELETE",
}


class RestoreCutoff(NamedTuple):
    cutoff_type: CutoffType
    value: Any
    inclusive: bool


class PlannedAction(NamedTuple):
    event_id: int
    operation_type: str
    compensatory_op: str
    entity_fingerprint: Optional[str]
    datasource_fingerprint: str
    layer_name: Optional[str]
    has_geometry: bool
    has_attribute_changes: bool


class Conflict(NamedTuple):
    event_id: int
    reason: str
    severity: str
    details: Optional[str]


class RestorePlan(NamedTuple):
    mode: RestoreMode
    scope: RestoreScope
    cutoff: Optional[RestoreCutoff]
    atomicity: AtomicityPolicy
    conflict_policy: ConflictPolicy
    actions: List[PlannedAction]
    conflicts: List[Conflict]
    entity_count: int
    event_count: int
    datasource_fingerprint: str
    layer_name: Optional[str]


class PreflightReport(NamedTuple):
    verdict: PreflightVerdict
    plan: RestorePlan
    blocking_reasons: List[str]
    warnings: List[str]
    estimated_duration_ms: Optional[int]


class RestoreSession(NamedTuple):
    session_id: str
    mode: RestoreMode
    scope: RestoreScope
    cutoff: Optional['RestoreCutoff']
    datasource_fingerprint: str
    layer_name: Optional[str]
    started_at: str
    finished_at: Optional[str]
    succeeded_count: int
    failed_count: int
    total_requested: int
    status: str  # "completed", "partial", "failed", "cancelled"


MAX_EVENTS_PER_RESTORE = 10_000
MAX_ENTITIES_PER_RESTORE = 1_000
WARN_EVENTS_THRESHOLD = 1_000
WARN_ENTITIES_THRESHOLD = 100

_SCOPE_ORDER = {
    RestoreScope.ENTITY: 0,
    RestoreScope.SELECTION: 1,
    RestoreScope.LAYER: 2,
    RestoreScope.DATASOURCE: 3,
}

PROVIDER_RESTORE_MATRIX: Dict[Tuple[str, RestoreMode], Tuple[bool, Optional[str]]] = {
    ("postgres", RestoreMode.EVENT): (True, None),
    ("postgres", RestoreMode.TEMPORAL): (True, None),
    ("ogr", RestoreMode.EVENT): (True, None),
    ("ogr", RestoreMode.TEMPORAL): (True, "Identity depends on sub-format"),
    ("spatialite", RestoreMode.EVENT): (True, None),
    ("spatialite", RestoreMode.TEMPORAL): (True, None),
    ("memory", RestoreMode.EVENT): (False, "Non-persisted data"),
    ("memory", RestoreMode.TEMPORAL): (False, "Non-persisted data"),
    ("virtual", RestoreMode.EVENT): (False, "Derived layer"),
    ("virtual", RestoreMode.TEMPORAL): (False, "Derived layer"),
    ("delimitedtext", RestoreMode.EVENT): (True, "Identity weak"),
    ("delimitedtext", RestoreMode.TEMPORAL): (False, "Identity too weak for temporal"),
    ("wfs", RestoreMode.EVENT): (True, "Depends on WFS-T capabilities"),
    ("wfs", RestoreMode.TEMPORAL): (False, "Network latency, identity risk"),
    ("mssql", RestoreMode.EVENT): (True, None),
    ("mssql", RestoreMode.TEMPORAL): (True, None),
    ("oracle", RestoreMode.EVENT): (True, None),
    ("oracle", RestoreMode.TEMPORAL): (True, None),
}


def is_restore_allowed(
    provider_name: str, mode: RestoreMode,
) -> Tuple[bool, Optional[str]]:
    """Check if a restore mode is allowed for a given provider."""
    key = (provider_name, mode)
    if key in PROVIDER_RESTORE_MATRIX:
        return PROVIDER_RESTORE_MATRIX[key]
    return (False, f"Unknown provider: {provider_name}")


def validate_cutoff(cutoff: RestoreCutoff) -> Optional[str]:
    """Validate a cutoff value. Returns error message or None if valid."""
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        if not isinstance(cutoff.value, int) or cutoff.value < 1:
            return "event_id must be a positive integer"
    elif cutoff.cutoff_type == CutoffType.BY_DATE:
        if not isinstance(cutoff.value, str) or len(cutoff.value) < 10:
            return "date must be an ISO 8601 string (min 10 chars)"
    return None


def check_volume_limits(
    event_count: int, entity_count: int,
) -> Tuple[bool, List[str], List[str]]:
    """Check volume against limits.

    Returns (allowed, warnings, blocking_reasons).
    """
    warnings: List[str] = []
    blocking: List[str] = []

    if event_count > MAX_EVENTS_PER_RESTORE:
        blocking.append(
            f"Event count {event_count} exceeds limit {MAX_EVENTS_PER_RESTORE}"
        )
    elif event_count > WARN_EVENTS_THRESHOLD:
        warnings.append(
            f"Event count {event_count} above threshold {WARN_EVENTS_THRESHOLD}"
        )

    if entity_count > MAX_ENTITIES_PER_RESTORE:
        blocking.append(
            f"Entity count {entity_count} exceeds limit {MAX_ENTITIES_PER_RESTORE}"
        )
    elif entity_count > WARN_ENTITIES_THRESHOLD:
        warnings.append(
            f"Entity count {entity_count} above threshold {WARN_ENTITIES_THRESHOLD}"
        )

    return (len(blocking) == 0, warnings, blocking)


def default_atomicity(mode: RestoreMode) -> AtomicityPolicy:
    """Return the default atomicity policy for a given restore mode."""
    if mode == RestoreMode.TEMPORAL:
        return AtomicityPolicy.STRICT
    return AtomicityPolicy.BEST_EFFORT


def scope_requires_confirmation(scope: RestoreScope) -> bool:
    """Return True if the scope is wide enough to require explicit confirmation."""
    return _SCOPE_ORDER.get(scope, 0) >= _SCOPE_ORDER[RestoreScope.LAYER]
