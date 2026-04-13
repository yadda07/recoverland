"""Schema drift detection for RecoverLand (RLU-053).

Compares the field schema stored at audit time with the current layer schema.
Produces a mapping report: matched fields, missing fields, added fields,
and type-incompatible fields. Used before restore to decide strategy.
"""
import json
from typing import Dict, List, NamedTuple, Optional

from .logger import flog


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
        len(missing_in_current) == 0
        and len(type_changed) == 0
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
        {"int4", "integer", "int", "int8", "bigint", "smallint", "int2"},
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

    Only matched fields are mapped. Missing and type-changed fields
    are excluded (caller decides strategy).
    """
    mapping = {}
    for name in drift.matched:
        mapping[name] = name
    return mapping


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
