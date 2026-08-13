"""Schema drift detection for RecoverLand (RLU-053).

Compares the field schema stored at audit time with the current layer schema.
Produces a mapping report: matched fields, missing fields, added fields,
and type-incompatible fields. Used before restore to decide strategy.
"""
import json
from typing import Dict, List, NamedTuple, Optional


class FieldInfo(NamedTuple):
    name: str
    type_name: str
    length: int
    precision: int


class DriftReport(NamedTuple):
    matched: List[str]
    missing_in_current: List[str]
    added_in_current: List[str]
    type_changed: Dict[str, str]
    is_compatible: bool


class FieldMapping(NamedTuple):
    """A restore mapping together with what it could NOT carry.

    ``mapping`` is the historical-name -> current-name dict consumed by
    ``serialization.iter_mapped_attributes``.

    ``dropped`` names the captured fields that cannot be written back,
    keyed by historical name, with a readable reason. Callers must report
    it: a restore that loses fields in silence hands the user a hybrid
    feature -- some values from yesterday, the rest from today -- while
    announcing a full restore (RLU-053).
    """
    mapping: Dict[str, str]
    dropped: Dict[str, str]


def parse_field_schema(field_schema_json: str) -> List[FieldInfo]:
    """Parse stored field_schema_json into FieldInfo list."""
    try:
        raw = json.loads(field_schema_json)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(raw, list):
        return []
    result = []
    for entry in raw:
        result.append(FieldInfo(
            name=entry.get("name", ""),
            type_name=entry.get("type", ""),
            length=entry.get("length", 0),
            precision=entry.get("precision", 0),
        ))
    return result


def extract_current_schema(layer) -> List[FieldInfo]:
    """Extract current field schema from a QgsVectorLayer."""
    result = []
    for field in layer.fields():
        result.append(FieldInfo(
            name=field.name(),
            type_name=field.typeName(),
            length=field.length(),
            precision=field.precision(),
        ))
    return result


def compare_schemas(historical: List[FieldInfo], current: List[FieldInfo]) -> DriftReport:
    """Compare historical schema with current schema.

    Returns a DriftReport describing all differences.
    """
    hist_by_name = {f.name: f for f in historical}
    curr_by_name = {f.name: f for f in current}

    matched = []
    missing_in_current = []
    type_changed = {}

    for name, hist_field in hist_by_name.items():
        if name not in curr_by_name:
            missing_in_current.append(name)
            continue
        curr_field = curr_by_name[name]
        if _types_compatible(hist_field, curr_field):
            matched.append(name)
        else:
            type_changed[name] = (
                f"{hist_field.type_name} -> {curr_field.type_name}"
            )

    added_in_current = [n for n in curr_by_name if n not in hist_by_name]

    is_compatible = (
        len(missing_in_current) == 0 and len(type_changed) == 0
    )

    return DriftReport(
        matched=matched,
        missing_in_current=missing_in_current,
        added_in_current=added_in_current,
        type_changed=type_changed,
        is_compatible=is_compatible,
    )


def _types_compatible(hist: FieldInfo, curr: FieldInfo) -> bool:
    """Check if two field types are compatible for restore."""
    if hist.type_name.lower() == curr.type_name.lower():
        return True
    compatible_groups = [
        # "integer64" is what OGR reports for a shapefile (.dbf) integer
        # column, and what QGIS shows after exporting a GPKG whose column
        # was plain "Integer". Leaving it out of the group made every
        # integer field of every shapefile look retyped, so every integer
        # was skipped in silence at each restore (RLU-053).
        {"int4", "integer", "int", "int8", "bigint", "smallint", "int2",
         "integer64", "int64", "long", "longlong"},
        {"float8", "double", "real", "float4", "numeric", "decimal"},
        {"varchar", "text", "string", "char", "character varying"},
        {"bool", "boolean"},
        {"date"},
        {"time", "timetz"},
        {"timestamp", "timestamptz", "datetime"},
    ]
    h_lower = hist.type_name.lower()
    c_lower = curr.type_name.lower()
    for group in compatible_groups:
        if h_lower in group and c_lower in group:
            return True
    return False


def build_field_mapping(drift: DriftReport, historical: List[FieldInfo]) -> Dict[str, str]:
    """Build a name-to-name mapping for restore.

    Matched fields map to themselves.

    Fields whose column disappeared stay in the mapping ON PURPOSE:
    ``serialization.iter_mapped_attributes`` resolves them to ``idx < 0``,
    skips them and logs the skip, so the single write choke point of the
    restore path keeps a trace of what the drift costs. They are never
    written -- there is no column left to write them into.

    Type-changed fields are excluded: their column still exists, so
    mapping them would push a value the column cannot hold.

    The mapping alone cannot tell a caller what it lost on the way; the
    list of dropped fields comes from ``dropped_fields`` /
    ``field_mapping_report``, which every write path must report.
    """
    mapping = {}
    for name in drift.matched:
        mapping[name] = name
    for name in drift.missing_in_current:
        mapping[name] = name
    return mapping


def dropped_fields(drift: DriftReport,
                   attrs: Optional[Dict] = None) -> Dict[str, str]:
    """Captured fields the restore cannot write back: name -> reason.

    When *attrs* is given (the attributes the event actually captured),
    only fields carried by this event are reported: a column that drifted
    but was never captured costs the user nothing.
    """
    dropped: Dict[str, str] = {}
    for name in drift.missing_in_current:
        if attrs is None or name in attrs:
            dropped[name] = "field no longer exists in the layer"
    for name, change in drift.type_changed.items():
        if attrs is None or name in attrs:
            dropped[name] = f"incompatible type change ({change})"
    return dropped


def format_dropped_fields(dropped: Dict[str, str]) -> str:
    """Readable summary of the fields a restore had to abandon."""
    return ", ".join(
        f"{name} [{reason}]" for name, reason in sorted(dropped.items())
    )


def field_mapping_report(event, layer=None,
                         drift: Optional[DriftReport] = None,
                         attrs: Optional[Dict] = None) -> FieldMapping:
    """Mapping to apply AND the captured fields it had to abandon.

    Resolution order:
      1. If *drift* is supplied, reuse it (caller already paid for the
         compare). This is the fast path used by restore_service after
         pre_check_restore.
      2. Else if *layer* is supplied, compute drift from the layer's
         current schema. This is the path used by restore_executor's
         buffer ops which do not have a precomputed drift.
      3. Else fall back to identity mapping (every historical field
         maps to itself) and no drop. Defensive default for callers that
         proceed without a precheck (legacy undo paths).

    Reuses parse_field_schema / extract_current_schema / compare_schemas
    so the mapping behaviour is identical across restore_service and
    restore_executor (DUP-04 consolidation).
    """
    hist = parse_field_schema(event.field_schema_json)
    if drift is None and layer is not None:
        drift = compare_schemas(hist, extract_current_schema(layer))
    if drift is None:
        return FieldMapping({f.name: f.name for f in hist}, {})
    return FieldMapping(
        build_field_mapping(drift, hist),
        dropped_fields(drift, attrs),
    )


def safe_field_mapping(event, layer=None,
                       drift: Optional[DriftReport] = None) -> Dict[str, str]:
    """Field-name mapping for restore, drift handled (see FieldMapping).

    Kept for call sites that only need the mapping. Any call site that
    reports an outcome to the user must use ``field_mapping_report`` and
    surface ``dropped``.
    """
    return field_mapping_report(event, layer=layer, drift=drift).mapping


def format_drift_message(drift: DriftReport) -> str:
    """Human-readable drift summary for UI display."""
    if drift.is_compatible:
        return f"Schema compatible ({len(drift.matched)} fields matched)"

    parts = []
    if drift.missing_in_current:
        parts.append(f"Missing: {', '.join(drift.missing_in_current)}")
    if drift.added_in_current:
        parts.append(f"New: {', '.join(drift.added_in_current)}")
    if drift.type_changed:
        changes = [f"{k} ({v})" for k, v in drift.type_changed.items()]
        parts.append(f"Type changed: {', '.join(changes)}")
    return "; ".join(parts)
