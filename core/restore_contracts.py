"""Pure restore contracts for RecoverLand time-travel restore.

Zero QGIS dependency. Defines modes, scopes, cutoffs, limits,
conflict policies, provider matrix, and plan structures.

Serves as the single source of truth for BL-00 (cadrage) and
BL-01 (noyau metier). All restore logic references these contracts.
"""
from collections import namedtuple
from datetime import datetime, timezone
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


MIN_CUTOFF_DATE_LEN = 10  # "YYYY-MM-DD"


def parse_cutoff_date(value: Any) -> Optional[datetime]:
    """Parse a BY_DATE cutoff value, or None when it is not a date.

    Accepts what the product actually produces: the ``isoformat()`` of the
    capture path (with or without microseconds, with or without a UTC
    offset), the ``yyyy-MM-ddTHH:mm:ss`` string the rewind dialog builds,
    a legacy journal stamp using a space separator, and a bare
    ``YYYY-MM-DD``. A trailing ``Z`` is rewritten to ``+00:00`` because
    ``datetime.fromisoformat`` only accepts it from Python 3.11 and the
    plugin still runs on the 3.9 shipped with QGIS 3.40.

    Anything else -- None, empty string, free text -- is NOT a date and
    returns None. Callers must treat that as "no readable bound".
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if len(text) < MIN_CUTOFF_DATE_LEN:
        return None
    if text[-1] in ("Z", "z"):
        text = "".join((text[:-1], "+00:00"))
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def normalize_cutoff_date(value: Any) -> Optional[str]:
    """Journal-comparable spelling of a BY_DATE cutoff value.

    The cutoff is never compared as a date: it goes into SQL as
    ``created_at >= ?``, a STRING comparison against what the capture path
    wrote (``datetime.now(timezone.utc).isoformat()``). Two spellings of
    the same instant must therefore be made identical, or the read window
    silently misses events:

      * ``...T09:00:00.000000+00:00`` and ``...T09:00:00+00:00`` are the
        same instant, but ``'.'`` (0x2E) sorts after ``'+'`` (0x2B). An
        event written on a round second (``isoformat()`` omits a zero
        microsecond) fell OUTSIDE a bound written with microseconds: the
        edit was never undone, the trace that referenced it was never
        read, and the rewind still reported success.
      * an offset other than +00:00 compares character by character
        against a journal that is always UTC.

    Re-emitting through ``datetime.isoformat()`` after converting to UTC
    produces exactly the spelling the journal uses. Two forms are left
    untouched on purpose: a bare ``YYYY-MM-DD`` and a naive value, which
    are PREFIXES of any stamp of the same day/second -- that prefix is
    what makes the dialog's ``yyyy-MM-ddTHH:mm:ss`` bound take the whole
    second, so the separator of the input is preserved too.

    An unusable value normalizes to None: ``created_at >= NULL`` is never
    true, so a bound nobody can read selects NOTHING. The empty string did
    the opposite -- ``created_at >= ''`` is true for every row, i.e.
    "rewind the whole journal" for a bound the user never typed.
    """
    parsed = parse_cutoff_date(value)
    if parsed is None:
        return None
    text = value.strip()
    if len(text) == MIN_CUTOFF_DATE_LEN:
        # Date-only bound: keep the prefix form. Expanding it to midnight
        # would push it PAST every stamp of that day written with a space
        # separator (' ' < 'T'), which empties the window on old journals.
        return text
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(sep=text[MIN_CUTOFF_DATE_LEN])


_RestoreCutoffBase = namedtuple(
    "RestoreCutoff", ("cutoff_type", "value", "inclusive"))


class RestoreCutoff(_RestoreCutoffBase):
    """Read-window bound: what the user asked to go back to.

    Fields: ``cutoff_type`` (CutoffType), ``value``, ``inclusive``.

    Inclusive cutoff: events with `created_at >= value` are included.
    SQLite stores timestamps with second precision: an event committed
    within the same second as the cutoff would otherwise be silently
    dropped, which is the dominant rewind regression pattern (see
    SESSION_REWIND.md §17.1 H-S3).  Default True is the safe behaviour;
    callers needing a strict `>` filter must pass `inclusive=False`
    explicitly.

    A BY_DATE value is normalized at construction
    (:func:`normalize_cutoff_date`) so that every consumer -- the SQL
    window, the retention coverage check, the preflight -- compares the
    same string. Normalizing here rather than in each reader is what
    guarantees they cannot disagree. ``_replace`` bypasses ``__new__``
    (namedtuple builds through ``_make``), so :func:`validate_cutoff`
    re-checks the value instead of trusting the construction path.
    """

    # No class-level field annotations here: assigning `inclusive: bool =
    # True` in the body would shadow the tuple accessor with a plain class
    # attribute and every cutoff would read back as inclusive. The
    # signature below carries both the types and the default.
    __slots__ = ()

    def __new__(cls, cutoff_type: CutoffType, value: Any = None,
                inclusive: bool = True):
        if cutoff_type == CutoffType.BY_DATE:
            value = normalize_cutoff_date(value)
        return super().__new__(cls, cutoff_type, value, inclusive)


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
    """Validate a cutoff value. Returns error message or None if valid.

    A length check is not a date check: "pas-une-date-du-tout" is 20
    characters long and was declared valid, so the user got "no event
    after this date" for a date that does not exist instead of a typing
    error -- and a bound that only LOOKS like a date can select the whole
    journal by string comparison. The value must parse.
    """
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        if not isinstance(cutoff.value, int) or cutoff.value < 1:
            return "event_id must be a positive integer"
    elif cutoff.cutoff_type == CutoffType.BY_DATE:
        if not isinstance(cutoff.value, str) or len(cutoff.value) < MIN_CUTOFF_DATE_LEN:
            return "date must be an ISO 8601 string (min 10 chars)"
        if parse_cutoff_date(cutoff.value) is None:
            return "date is not a readable ISO 8601 timestamp"
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
