"""Attribute serialization for RecoverLand audit journal (RLU-029).

Converts QVariant-based QGIS attribute values to JSON-safe Python types
and back. Every supported QVariant type has a deterministic round-trip.
"""
import json
import base64
import math
import re
from datetime import date, time, datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from .audit_field_policy import is_layer_audit_field  # noqa: F401 re-export
from .logger import flog


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
        # NaN and the infinities are legitimate measurements ("not
        # measurable", "saturated sensor", "division by zero"). Replacing
        # them with None used to turn them into "not filled in", with no
        # trace of the substitution. json.dumps/json.loads round-trip them.
        return value

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


def _serialize_qt_type(value: Any) -> Any:
    """Handle Qt-specific types (QDate, QTime, QDateTime, QByteArray)."""
    type_name = type(value).__name__

    if type_name == "QDateTime":
        if not value.isValid():
            return None
        # Normalise to UTC before dropping the timezone marker. The stored
        # text keeps its historical "yyyy-MM-ddTHH:mm:ss" shape (no journal
        # migration), but the digits now always denote the same reference
        # frame, so _deserialize_datetime can rebuild the exact instant.
        # Writing the provider's own digits back as a naive string made the
        # provider re-anchor them to local time, subtracting the UTC offset
        # once per restore, cumulatively and without any ceiling.
        return value.toUTC().toString("yyyy-MM-ddTHH:mm:ss")

    if type_name == "QDate":
        return value.toString("yyyy-MM-dd") if value.isValid() else None

    if type_name == "QTime":
        return value.toString("HH:mm:ss") if value.isValid() else None

    if type_name == "QByteArray":
        raw = bytes(value)
        return "b64:" + base64.b64encode(raw).decode("ascii")

    return str(value)


def _is_null(value: Any) -> bool:
    """Return True only for values the provider itself reports as unset.

    Deliberately blind to the display setting that paints empty cells in
    the attribute table: it is a per-workstation preference, and its
    default happens to be the text "NULL". Consulting it here erased any
    legitimate cell whose content matched it, both when capturing the
    before-state (rewind then blanked the cell) and when capturing the
    after-state (target proof then refused the restore).
    """
    if value is None:
        return True

    type_name = type(value).__name__
    if type_name in _QVARIANT_NULL_TYPES:
        return True

    try:
        if hasattr(value, 'isNull') and value.isNull():
            return True
    except (TypeError, RuntimeError):
        # Benign: isNull not callable on this object variant.
        pass

    return False


_BLOB_TYPE_NAMES = frozenset([
    "qbytearray", "bytes", "blob", "binary", "varbinary", "bytea",
])
_DATETIME_TYPE_NAMES = frozenset([
    "qdatetime", "datetime", "datetime2", "timestamp", "timestamptz",
])
_DATE_TYPE_NAMES = frozenset(["qdate", "date"])
_TIME_TYPE_NAMES = frozenset(["qtime", "time"])
_TEXT_TYPE_NAMES = frozenset([
    "qstring", "str", "string", "text", "varchar", "char",
    "charactervarying",
])
_INT_TYPE_NAMES = frozenset([
    "int", "integer", "integer64", "int2", "int4", "int8",
    "smallint", "bigint", "qlonglong", "qulonglong",
])
_REAL_TYPE_NAMES = frozenset([
    "double", "doubleprecision", "float", "float4", "float8", "real",
    "numeric", "decimal",
])
_BOOL_TYPE_NAMES = frozenset(["bool", "boolean"])

# "2026-01-15T13:30:00" (journal form), plus the tolerated variants:
# space separator, fractional seconds, explicit "Z" or "+01:00" offset.
_ISO_DATETIME_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?"
    r"(?:\.\d+)?(Z|z|[+-]\d{2}:?\d{2})?$"
)


def deserialize_value(value: Any, target_type_name: str) -> Any:
    """Convert a journal value back to the type its target field expects.

    *target_type_name* is a provider type name as reported by
    ``QgsField.typeName()`` (``Binary``, ``DateTime``, ``Integer64``,
    ``JSON``, ``String``...), or one of the historical Python/Qt aliases.
    Matching is case- and separator-insensitive.

    An unknown type name returns the value untouched: the journal already
    holds a JSON-safe scalar the provider can take as is, and guessing
    would do more harm than passing through.
    """
    if value is None:
        return None

    name = _normalize_type_name(target_type_name)

    if name in _BLOB_TYPE_NAMES:
        return _deserialize_blob(value)
    if name in _DATETIME_TYPE_NAMES:
        return _deserialize_datetime(value)
    if name in _DATE_TYPE_NAMES:
        return _deserialize_date(value)
    if name in _TIME_TYPE_NAMES:
        return _deserialize_time(value)
    if name in _TEXT_TYPE_NAMES:
        return value if isinstance(value, str) else _stringify(value)
    if name in _BOOL_TYPE_NAMES:
        return _coerce_scalar(value, bool)
    if name in _INT_TYPE_NAMES:
        return _coerce_scalar(value, int)
    if name in _REAL_TYPE_NAMES:
        return _coerce_scalar(value, float)
    return value


def _normalize_type_name(target_type_name: Any) -> str:
    if not isinstance(target_type_name, str):
        return ""
    return re.sub(r"[^a-z0-9]+", "", target_type_name.lower())


def _stringify(value: Any) -> Any:
    """Text form of a scalar; anything richer is left for the provider."""
    if isinstance(value, (bool, int, float)):
        return str(value)
    return value


def _coerce_scalar(value: Any, kind: Any) -> Any:
    """Return *value* as *kind* when the conversion is safe.

    ``type(value) is kind`` on purpose, not ``isinstance``: a bool IS an
    int in Python, and silently turning True into 1 (or 1 into True) is
    exactly the confusion the delta computation guards against.
    Non-numeric input is handed back untouched rather than forced.
    """
    if type(value) is kind:
        return value
    if isinstance(value, (bool, int, float)):
        try:
            return kind(value)
        except (TypeError, ValueError, OverflowError):
            return value
    return value


def _deserialize_blob(value: Any) -> Any:
    """Decode a ``b64:`` payload back into the layer's binary type.

    Returns a QByteArray when Qt is available (what the provider hands
    back when reading the same field), plain bytes otherwise. A payload
    that cannot be decoded is returned untouched rather than dropped:
    losing an attachment in silence is the very failure this path exists
    to prevent.
    """
    if isinstance(value, str) and value.startswith("b64:"):
        try:
            return _as_binary(base64.b64decode(value[4:], validate=True))
        except (ValueError, TypeError):
            flog("deserialize: undecodable b64 payload left as text "
                 f"(len={len(value)})", "WARNING")
            return value
    if isinstance(value, (bytes, bytearray)):
        return _as_binary(bytes(value))
    return value


def _as_binary(raw: bytes) -> Any:
    try:
        from qgis.PyQt.QtCore import QByteArray
    except ImportError:
        # Benign: pure-Python unit tests run without Qt bindings.
        return raw
    return QByteArray(raw)


def _deserialize_datetime(value: Any) -> Any:
    """Rebuild a QDateTime from the journal's textual form.

    The journal stores UTC digits (see :func:`_serialize_qt_type`), so the
    instant is unambiguous; an explicit offset is honoured when one is
    present. Handing the raw string to the provider instead made it
    re-anchor those digits to local time, which subtracted the local UTC
    offset once per restore and drifted further on every cycle.
    """
    if not isinstance(value, str):
        return value
    match = _ISO_DATETIME_RE.match(value.strip())
    if match is None:
        return value
    try:
        from qgis.PyQt.QtCore import QDateTime
    except ImportError:
        # Benign: pure-Python unit tests run without Qt bindings.
        return value
    year, month, day, hour, minute = (int(part) for part in match.groups()[:5])
    second = int(match.group(6) or 0)
    try:
        stamp = datetime(year, month, day, hour, minute, second,
                         tzinfo=timezone.utc)
    except ValueError:
        return value
    epoch = int(stamp.timestamp()) - _utc_offset_seconds(match.group(7))
    return QDateTime.fromSecsSinceEpoch(epoch)


def _utc_offset_seconds(token: Optional[str]) -> int:
    """Seconds carried by an ISO 8601 suffix: '' / 'Z' / '+01:00' / '-0500'."""
    if not token or token in ("Z", "z"):
        return 0
    digits = token[1:].replace(":", "")
    offset = int(digits[:2]) * 3600 + int(digits[2:4] or 0) * 60
    return -offset if token[0] == "-" else offset


def _deserialize_date(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        from qgis.PyQt.QtCore import QDate
    except ImportError:
        # Benign: pure-Python unit tests run without Qt bindings.
        return value
    parsed = QDate.fromString(value.strip()[:10], "yyyy-MM-dd")
    return parsed if parsed.isValid() else value


def _deserialize_time(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        from qgis.PyQt.QtCore import QTime
    except ImportError:
        # Benign: pure-Python unit tests run without Qt bindings.
        return value
    parsed = QTime.fromString(value.strip()[:8], "HH:mm:ss")
    return parsed if parsed.isValid() else value


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


def extract_delta_new(change: Any) -> Any:
    """Return the post-edit ("new") value of a ``changed_only`` delta entry.

    Single source of truth for reading an UPDATE delta, shared by the
    temporal snapshot engine and ``search_service.reconstruct_new_attributes``.
    Supports both the production dict form ``{"old": .., "new": ..}`` and the
    legacy list form ``[old, new]``; any other shape is returned as-is so a
    field whose value happens to be a plain scalar/list is left untouched.
    """
    if isinstance(change, dict) and "new" in change:
        return change["new"]
    if isinstance(change, (list, tuple)) and len(change) == 2:
        return change[1]
    return change


def extract_delta_old(change: Any) -> Any:
    """Return the pre-edit ("old") value of a ``changed_only`` delta entry.

    Mirror of :func:`extract_delta_new` for the ``old`` side. Used to seed the
    attributes of pre-existing entities whose first tracked event is an UPDATE.
    """
    if isinstance(change, dict) and "old" in change:
        return change["old"]
    if isinstance(change, (list, tuple)) and len(change) == 2:
        return change[0]
    return change


def build_full_snapshot(attrs: Dict) -> str:
    """Build a full attribute snapshot JSON for DELETE/INSERT events."""
    return json.dumps({"all_attributes": attrs}, ensure_ascii=False)


def iter_mapped_attributes(mapping: Dict[str, str], attrs: Dict, fields):
    """Yield ``(idx, value)`` for every restorable attribute.

    Centralises the iteration shape duplicated 5 times across
    restore_service and restore_executor (DUP-05): for each
    ``(historical_name, current_name)`` mapping entry, the value is
    yielded only when:
      - the historical name is present in *attrs* (event captured it),
      - the historical name is not a layer-only audit field
        (preserved by the policy),
      - the current name still exists in the target layer's fields
        (``fields.indexOf`` returns >= 0).

    Each caller decides what to do with the yielded ``(idx, value)``:
    set on a buffered feature, accumulate a ``changeAttributeValues``
    dict, or call ``layer.changeAttributeValue`` directly.

    The yielded value is decoded back to the target field's type
    (``deserialize_value``). The journal holds JSON-safe scalars, the
    layer expects QByteArray / QDateTime / QDate / QTime: copying the
    JSON text straight into the field wrote the ASCII of a blob's own
    ``b64:`` label into the blob, and re-anchored timestamps to local
    time. This generator is the single write choke point of the restore
    path, so decoding here covers all five call sites at once.
    """
    _skipped = []
    for hist_name, curr_name in mapping.items():
        if hist_name not in attrs:
            continue
        if is_layer_audit_field(hist_name):
            continue
        idx = fields.indexOf(curr_name)
        if idx < 0:
            _skipped.append(curr_name)
            continue
        yield idx, _decode_for_field(attrs[hist_name], fields, idx)
    if _skipped:
        flog(f"iter_mapped_attributes: {len(_skipped)} field(s) skipped "
             f"(schema drift): {_skipped[:5]}", "WARNING")


def _decode_for_field(value: Any, fields, idx: int) -> Any:
    """Decode *value* for the field at *idx*, never failing the restore."""
    try:
        type_name = fields.at(idx).typeName()
    except (AttributeError, IndexError, TypeError):
        return value
    try:
        return deserialize_value(value, type_name)
    except (TypeError, ValueError, OverflowError) as exc:
        flog(f"iter_mapped_attributes: decode failed idx={idx} "
             f"type={type_name!r}: {exc}", "WARNING")
        return value


def _values_equal(val_a: Any, val_b: Any) -> bool:
    if val_a is None and val_b is None:
        return True
    if val_a is None or val_b is None:
        return False
    # In Python 0 == False and 1 == True, so a field flipping between an
    # integer and a boolean (typical after a re-export) produced no delta:
    # no event was written, nothing was restorable, nothing was shown in
    # the history. "zero measured" and "no" are two different answers.
    if isinstance(val_a, bool) != isinstance(val_b, bool):
        return False
    if isinstance(val_a, float) and isinstance(val_b, float):
        if math.isnan(val_a) and math.isnan(val_b):
            return True
        return val_a == val_b
    return val_a == val_b
