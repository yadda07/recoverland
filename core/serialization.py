"""Attribute serialization for RecoverLand audit journal (RLU-029).

Converts QVariant-based QGIS attribute values to JSON-safe Python types
and back. Every supported QVariant type has a deterministic round-trip.
"""
import json
import base64
import math
from datetime import date, time, datetime
from typing import Any, Dict, Iterable, List, Optional

from .audit_field_policy import is_layer_audit_field  # noqa: F401 re-export


_QVARIANT_NULL_TYPES = frozenset(["QVariant", "Invalid"])


def serialize_value(value: Any) -> Any:
    """Convert a single QGIS attribute value to a JSON-safe Python type."""
    if _is_null(value):
        return None

    if isinstance(value, str):
        return value

    if isinstance(value, bool):
        return value

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return _serialize_float(value)

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, time):
        return value.isoformat()

    if isinstance(value, (bytes, bytearray)):
        return "b64:" + base64.b64encode(bytes(value)).decode("ascii")

    if isinstance(value, (list, tuple)):
        return [serialize_value(v) for v in value]

    if isinstance(value, dict):
        return {str(k): serialize_value(v) for k, v in value.items()}

    return _serialize_qt_type(value)


def _serialize_float(value: float) -> Any:
    if math.isnan(value) or math.isinf(value):
        return None
    return value


def _serialize_qt_type(value: Any) -> Any:
    """Handle Qt-specific types (QDate, QTime, QDateTime, QByteArray)."""
    type_name = type(value).__name__

    if type_name == "QDateTime":
        return value.toString("yyyy-MM-ddTHH:mm:ss") if value.isValid() else None

    if type_name == "QDate":
        return value.toString("yyyy-MM-dd") if value.isValid() else None

    if type_name == "QTime":
        return value.toString("HH:mm:ss") if value.isValid() else None

    if type_name == "QByteArray":
        raw = bytes(value)
        return "b64:" + base64.b64encode(raw).decode("ascii")

    return str(value)


def _is_null(value: Any) -> bool:
    if value is None:
        return True

    type_name = type(value).__name__
    if type_name in _QVARIANT_NULL_TYPES:
        return True

    try:
        from qgis.core import QgsApplication
        null_repr = QgsApplication.nullRepresentation()
        if isinstance(value, str) and value == null_repr:
            return True
    except (ImportError, AttributeError, RuntimeError):
        # Benign: QGIS not initialized (unit tests) or app unavailable.
        pass

    try:
        if hasattr(value, 'isNull') and value.isNull():
            return True
    except (TypeError, RuntimeError):
        # Benign: isNull not callable on this object variant.
        pass

    return False


def deserialize_value(value: Any, target_type_name: str) -> Any:
    """Convert a JSON value back to the expected Python/Qt type."""
    if value is None:
        return None

    if target_type_name in ("QString", "str", "string", "text"):
        return str(value)

    if target_type_name in ("int", "integer", "qlonglong", "qulonglong"):
        return int(value)

    if target_type_name in ("double", "float", "real", "numeric"):
        return float(value)

    if target_type_name in ("bool", "boolean"):
        return bool(value)

    if target_type_name in ("QByteArray", "bytes", "blob"):
        return _deserialize_blob(value)

    if target_type_name in ("date", "QDate"):
        return value

    if target_type_name in ("time", "QTime"):
        return value

    if target_type_name in ("datetime", "QDateTime", "timestamp"):
        return value

    return value


def _deserialize_blob(value: Any) -> Optional[bytes]:
    if isinstance(value, str) and value.startswith("b64:"):
        return base64.b64decode(value[4:])
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return None


def serialize_attributes(feature, field_names: List[str]) -> Dict[str, Any]:
    """Serialize all attributes of a QgsFeature to a JSON-safe dict."""
    result = {}
    for name in field_names:
        try:
            result[name] = serialize_value(feature[name])
        except (KeyError, IndexError):
            result[name] = None
    return result


def serialize_field_schema(fields) -> str:
    """Serialize QgsFields to a JSON string describing the schema."""
    schema = []
    for field in fields:
        schema.append({
            "name": field.name(),
            "type": field.typeName(),
            "length": field.length(),
            "precision": field.precision(),
        })
    return json.dumps(schema, ensure_ascii=False)


def compute_update_delta(old_attrs: Dict, new_attrs: Dict,
                         changed_field_names: Optional[Iterable[str]] = None) -> Optional[str]:
    """Compute delta between old and new attribute dicts.

    Returns JSON string with changed_only format, or None if no change.
    """
    changed = {}
    seen = set()
    keys = changed_field_names if changed_field_names is not None else old_attrs.keys()
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        if is_layer_audit_field(key):
            continue
        if key not in old_attrs:
            continue
        if key not in new_attrs:
            continue
        old_val = old_attrs[key]
        new_val = new_attrs[key]
        if not _values_equal(old_val, new_val):
            changed[key] = {"old": old_val, "new": new_val}

    if not changed:
        return None
    return json.dumps({"changed_only": changed}, ensure_ascii=False)


def build_full_snapshot(attrs: Dict) -> str:
    """Build a full attribute snapshot JSON for DELETE/INSERT events."""
    return json.dumps({"all_attributes": attrs}, ensure_ascii=False)


def _values_equal(val_a: Any, val_b: Any) -> bool:
    if val_a is None and val_b is None:
        return True
    if val_a is None or val_b is None:
        return False
    if isinstance(val_a, float) and isinstance(val_b, float):
        if math.isnan(val_a) and math.isnan(val_b):
            return True
        return val_a == val_b
    return val_a == val_b
