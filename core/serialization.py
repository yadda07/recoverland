"""Attribute serialization for RecoverLand audit journal (RLU-029).

Converts QVariant-based QGIS attribute values to JSON-safe Python types
and back. Every supported QVariant type has a deterministic round-trip.

Date-time text in the journal, by generation
--------------------------------------------
The journal has held three shapes of date-time text, and only the last
one describes itself:

  * events up to schema 5 (plugin <= 5.0.x, and events whose
    ``event_schema_version`` is NULL): ``value.toString('yyyy-MM-ddTHH:mm:ss')``
    of the value AS IT WAS HANDED OVER, no zone, no milliseconds. The
    digits are those of that value's own time spec, which the text does
    not say. DELETE snapshots and UPDATE old values were read from the
    provider; INSERT snapshots and UPDATE new values from the edit buffer
    (the spec the form, the default value or the expression gave).
    Measured on QGIS 4.2.1, the spec belongs to EACH stored value on
    every file provider: a GeoPackage stores a value committed through
    the provider in UTC (a local 14:30 becomes ``13:30:00Z``, handed back
    as UTC), while "Export / Save Features As" (QgsVectorFileWriter) and
    other tools store naive text (handed back as local time), so an
    exported GeoPackage edited afterwards holds both forms; memory,
    GeoJSON, FlatGeobuf, SQLite (OGR) and SpatiaLite keep the spec each
    value was written with; a Shapefile has no date-time type (text, not
    decoded).
    Known limit: the 5.0.1 development builds between commits 9a7e530 and
    bc81cf7 (2026-08-13) already wrote UTC digits under schema 5; their
    events are read like any other schema-5 event;
  * events of schema 6 (plugin 6.0.x): ``value.toUTC()`` digits, same
    shape. Same text, other meaning: only `event_schema_version` tells
    them apart;
  * since v7: ``yyyy-MM-ddTHH:mm:ss.zzz`` with the milliseconds ALWAYS
    written, followed by the spec: ``Z`` for a UTC value, ``+HH:MM`` /
    ``-HH:MM`` for an offset or a named zone (its offset at that instant),
    nothing for a NAIVE (local wall clock) value, whose digits are kept
    untouched. The fraction is the marker: no earlier generation ever
    wrote one. Such a value is rebuilt with the same spec and digits,
    whatever the zone of the workstation that restores it. A Python
    ``datetime`` (rare: a provider hands QDateTime) writes its own
    ``isoformat``, with milliseconds (microseconds when it has some) and
    ``+00:00`` rather than ``Z`` for UTC; it decodes the same way.

Decoding (`deserialize_value` / `iter_mapped_attributes`, which take the
event's ``event_schema_version``):

  * v7 text: its own spec;
  * schema 6 text, and any pre-v7 text read by a caller that does not
    pass the version: UTC digits (the historical default);
  * schema <= 5 text: the digits are read in the time spec 5.x read
    them in, and that spec is established only where it belongs to the
    COLUMN (PostgreSQL: the column type and the session zone fix it) and
    every live value of the field agrees (`provider_time_specs`). On
    every other provider it belonged to the stored value itself, which a
    DELETE removed and an UPDATE overwrote: the rows that remain prove
    nothing about it (the same remaining rows and the same text arise
    from a deleted ``Z`` row and from a deleted naive one, one UTC offset
    apart). There, and for a value typed in a form (whose spec was never
    recorded), for no live value, mixed specs, or a caller that does not
    supply the measurement, the field is refused by name
    (`legacy_datetime_refusals`, `LegacyDateTimeRefused`): never guessed,
    never shifted in silence.

Comparing: a date-time is an INSTANT. `datetime_instant_ms` /
`journal_text_instant_ms` give the milliseconds since the epoch of a live
value or of a v7 text, so two texts of one instant (``14:30:00.000`` and
``13:30:00.000Z`` in Paris in winter) compare equal and two instants with
the same digits do not. `journal_datetime_matches` compares a live value
with journal text at the resolution that text carries (seconds before
v7, milliseconds since), and `instants_match` at the resolution the
stored side carries: a provider that stores whole seconds holds
``14:30:12.000`` for a journaled ``14:30:12.517``.
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

# `event_schema_version` from which date-time digits are UTC (plugin 6.0.0).
UTC_DIGITS_SCHEMA_VERSION = 6

# Sentinel: the caller did not say which event the value comes from. Kept
# distinct from None, which is a real column value (events written before
# the column existed, i.e. legacy digits).
_UNSPECIFIED = object()

# Where a schema <= 5 value was read when it was journaled (see the module
# docstring): the provider (DELETE snapshot, UPDATE old value) or the edit
# buffer (INSERT snapshot, UPDATE new value).
CAPTURED_FROM_PROVIDER = "provider"
CAPTURED_FROM_EDIT_BUFFER = "edit_buffer"

# Time spec tokens of a provider field (`ProviderTimeSpecs`): the digits
# 5.x journaled are read in this spec. An offset is "+HH:MM" / "-HH:MM",
# a named zone "tz:<IANA id>".
SPEC_LOCAL = "local"
SPEC_UTC = "utc"


class LegacyDateTimeUnresolved(Exception):
    """A schema <= 5 date-time whose time spec was not supplied.

    Deliberately NOT a ValueError: the decoders swallow ValueError and hand
    the raw text to the provider, which re-anchors it to local time, i.e.
    the very shift this refusal exists to prevent.
    """


class LegacyDateTimeRefused(Exception):
    """Schema <= 5 date-times that cannot be decoded without guessing.

    Raised by `iter_mapped_attributes` BEFORE it yields anything, so the
    caller has written nothing. ``fields`` maps each field (current name)
    to the reason, in French, for the user. A caller that restores the
    other fields drops these beforehand with `legacy_datetime_refusals`
    and reports them as not restored.
    """

    def __init__(self, fields: Dict[str, str]):
        self.fields = dict(fields)
        detail = "; ".join(f"{name} : {reason}"
                           for name, reason in sorted(self.fields.items()))
        super().__init__(
            f"date-heure journalisee par une version <= 5.x non restauree, "
            f"son fuseau n'est pas etabli ({detail})")


# A lone (unpaired) UTF-16 surrogate: Python can hold it, UTF-8 cannot.
_SURROGATE_RE = re.compile("[\ud800-\udfff]")
_REPLACEMENT_CHAR = "�"


def scrub_surrogates(text: str) -> str:
    """*text* with every lone UTF-16 surrogate replaced by U+FFFD.

    Qt strings are UTF-16 and may hold half of a surrogate pair: a
    default-on-update ``left("comment", 20)`` or a field calculator cut an
    emoji in two, and the edit buffer hands the half to Python as a lone
    surrogate. UTF-8 cannot encode it, so the journal write raised
    UnicodeEncodeError and the whole batch was dropped, unrelated events
    included. The provider itself cannot store it either: it writes
    U+FFFD (verified on GeoPackage), so U+FFFD is also the value the
    datasource really holds after the commit.
    """
    # isascii() is a flag read in CPython: the common case costs nothing.
    if (not isinstance(text, str) or text.isascii()
            or not _SURROGATE_RE.search(text)):
        return text
    count = len(_SURROGATE_RE.findall(text))
    flog(f"serialize: {count} lone UTF-16 surrogate(s) replaced by U+FFFD "
         f"(len={len(text)}); the datasource stores the same replacement",
         "WARNING")
    return _SURROGATE_RE.sub(_REPLACEMENT_CHAR, text)


def journal_json_dumps(payload: Any) -> str:
    """``json.dumps`` for a journal column, guaranteed UTF-8 encodable.

    Values reaching the payload builders normally went through
    `serialize_value`, which already scrubs lone surrogates. This is the
    backstop for anything that did not: one unencodable character must
    never cost the batch its other events.
    """
    text = json.dumps(payload, ensure_ascii=False)
    if text.isascii() or not _SURROGATE_RE.search(text):
        return text
    return json.dumps(_scrub_deep(payload), ensure_ascii=False)


def _scrub_deep(value: Any) -> Any:
    if isinstance(value, str):
        return scrub_surrogates(value)
    if isinstance(value, dict):
        return {_scrub_deep(k): _scrub_deep(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_scrub_deep(v) for v in value]
    return value


def serialize_value(value: Any) -> Any:
    """Convert a single QGIS attribute value to a JSON-safe Python type."""
    if _is_null(value):
        return None

    if isinstance(value, str):
        return scrub_surrogates(value)

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
        # Same contract as a QDateTime (see the module docstring): the
        # fraction is always written, so a naive value is recognisable.
        return value.isoformat(timespec=(
            "microseconds" if value.microsecond % 1000 else "milliseconds"))

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, time):
        return value.isoformat()

    if isinstance(value, (bytes, bytearray)):
        return "b64:" + base64.b64encode(bytes(value)).decode("ascii")

    if isinstance(value, (list, tuple)):
        return [serialize_value(v) for v in value]

    if isinstance(value, dict):
        return {scrub_surrogates(str(k)): serialize_value(v)
                for k, v in value.items()}

    return _serialize_qt_type(value)


def _serialize_qt_type(value: Any) -> Any:
    """Handle Qt-specific types (QDate, QTime, QDateTime, QByteArray)."""
    type_name = type(value).__name__

    if type_name == "QDateTime":
        if not value.isValid():
            return None
        return _qdatetime_to_journal(value)

    if type_name == "QDate":
        return value.toString("yyyy-MM-dd") if value.isValid() else None

    if type_name == "QTime":
        if not value.isValid():
            return None
        # Milliseconds only when there are some: a whole-second time keeps
        # the historical text byte for byte, a sub-second one no longer
        # loses its fraction (an edit of the milliseconds alone produced
        # no delta, hence no event at all).
        if value.msec():
            return value.toString("HH:mm:ss.zzz")
        return value.toString("HH:mm:ss")

    if type_name == "QByteArray":
        raw = bytes(value)
        return "b64:" + base64.b64encode(raw).decode("ascii")

    return scrub_surrogates(str(value))


def _qdatetime_to_journal(value: Any) -> str:
    """Journal text of a valid QDateTime: digits, milliseconds, and spec.

    Schema 6 converted every value to UTC and dropped the zone marker:
    the milliseconds were lost (a DEFAULT now() timestamp came back
    truncated, a sub-second edit was not journaled at all), and a NAIVE
    value was anchored to the zone of the capturing workstation, then
    re-anchored to the zone of the restoring one; a naive value of the
    spring-forward hour came back one hour later.

    Now the value is written in its own spec:
      * UTC                -> ``yyyy-MM-ddTHH:mm:ss.zzzZ``
      * offset / time zone -> ``yyyy-MM-ddTHH:mm:ss.zzz+HH:MM`` (the
        instant and the wall clock of that offset; a named zone keeps
        its offset at that instant)
      * local (naive)      -> ``yyyy-MM-ddTHH:mm:ss.zzz``, digits untouched
    """
    digits = value.toString("yyyy-MM-ddTHH:mm:ss.zzz")
    spec = _time_spec_name(value)
    if spec == "LocalTime":
        return digits
    if spec == "UTC":
        return digits + "Z"
    try:
        offset = int(value.offsetFromUtc())
    except (AttributeError, TypeError, ValueError):
        return value.toUTC().toString("yyyy-MM-ddTHH:mm:ss.zzz") + "Z"
    if spec == "TimeZone" and offset == 0 and _zone_is_utc(value):
        return digits + "Z"
    sign = "-" if offset < 0 else "+"
    offset = abs(offset)
    return f"{digits}{sign}{offset // 3600:02d}:{(offset % 3600) // 60:02d}"


def _time_spec_name(value: Any) -> str:
    try:
        spec = value.timeSpec()
    except (AttributeError, TypeError):
        return "LocalTime"
    name = getattr(spec, "name", None)
    if name:
        return str(name)
    return {0: "LocalTime", 1: "UTC", 2: "OffsetFromUTC", 3: "TimeZone"}.get(
        int(spec), "LocalTime")


def _zone_is_utc(value: Any) -> bool:
    try:
        zone_id = bytes(value.timeZone().id()).decode("ascii", "replace")
    except (AttributeError, TypeError, ValueError):
        return False
    return zone_id in ("UTC", "Etc/UTC", "UTC+00:00")


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

# "2026-01-15T13:30:00.517Z" (v7 journal form), "2026-01-15T13:30:00"
# (schema <= 6), plus the tolerated variants: space separator, any number
# of fractional digits, explicit "Z" or "+01:00" offset.
_ISO_DATETIME_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?"
    r"(?:\.(\d+))?(Z|z|[+-]\d{2}:?\d{2})?$"
)
_ISO_TIME_RE = re.compile(r"^(\d{2}):(\d{2})(?::(\d{2}))?(?:\.(\d+))?$")


def deserialize_value(value: Any, target_type_name: str,
                      event_schema_version: Any = _UNSPECIFIED,
                      legacy_spec: Optional[str] = None) -> Any:
    """Convert a journal value back to the type its target field expects.

    *target_type_name* is a provider type name as reported by
    ``QgsField.typeName()`` (``Binary``, ``DateTime``, ``Integer64``,
    ``JSON``, ``String``...), or one of the historical Python/Qt aliases.
    Matching is case- and separator-insensitive.

    *event_schema_version* is the ``event_schema_version`` of the event
    the value was read from (None when the column is NULL). It only
    matters for date-time text written before v7: UTC digits in schema 6,
    digits of the provider's own time spec up to schema 5 (see the module
    docstring). Not passing it keeps the schema-6 reading.

    *legacy_spec* is that provider time spec (a `ProviderTimeSpecs`
    token) for a schema <= 5 date-time. Without it such a value raises
    `LegacyDateTimeUnresolved`: guessing the zone is how a restored date
    ends up shifted by the UTC offset and announced as a success.

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
        return _deserialize_datetime(value, event_schema_version, legacy_spec)
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


def _deserialize_datetime(value: Any,
                          event_schema_version: Any = _UNSPECIFIED,
                          legacy_spec: Optional[str] = None) -> Any:
    """Rebuild a QDateTime from the journal's textual form.

    v7 text describes itself (see the module docstring): a ``Z`` or an
    offset gives back a value of that spec, a fraction without suffix a
    NAIVE value with the same digits, milliseconds included.

    Text without fraction nor suffix predates v7 and is read according to
    *event_schema_version*:
      * schema 6 (and a caller that does not say): UTC digits, rebuilt as
        the instant they denote. Handing them raw to the provider made it
        re-anchor them to local time, drifting by the UTC offset on every
        restore;
      * schema <= 5, or no version at all: the digits of the time spec the
        provider handed over, *legacy_spec*. Reading them always as local
        time put a GeoPackage value (UTC) back one UTC offset early;
        reading them always as UTC did the same to every provider that
        hands back local time. Without *legacy_spec*,
        `LegacyDateTimeUnresolved` is raised.
    """
    if not isinstance(value, str):
        return value
    match = _ISO_DATETIME_RE.match(value.strip())
    if match is None:
        return value
    try:
        from qgis.PyQt.QtCore import QDate, QDateTime, QTime
    except ImportError:
        # Benign: pure-Python unit tests run without Qt bindings.
        return value
    year, month, day, hour, minute = (int(part) for part in match.groups()[:5])
    second = int(match.group(6) or 0)
    fraction = match.group(7)
    suffix = match.group(8)
    msec = int((fraction + "00")[:3]) if fraction else 0
    try:
        stamp = datetime(year, month, day, hour, minute, second,
                         tzinfo=timezone.utc)
    except ValueError:
        return value
    qdate = QDate(year, month, day)
    qtime = QTime(hour, minute, second, msec)
    if suffix:
        zone = _qtimezone_for_suffix(suffix)
        if zone is not None:
            return QDateTime(qdate, qtime, zone)
        epoch_ms = (int(stamp.timestamp()) - _utc_offset_seconds(suffix)) * 1000
        return QDateTime.fromMSecsSinceEpoch(epoch_ms + msec)
    if fraction is not None:
        return QDateTime(qdate, qtime)
    if _is_provider_digits(event_schema_version):
        if legacy_spec is None:
            raise LegacyDateTimeUnresolved(
                f"texte {value!r} journalise en schema "
                f"{event_schema_version!r} : fuseau du provider non fourni")
        return _qdatetime_in_spec(qdate, qtime, legacy_spec)
    return QDateTime.fromMSecsSinceEpoch(int(stamp.timestamp()) * 1000 + msec)


def _qdatetime_in_spec(qdate, qtime, spec: str):
    """QDateTime of these digits in the spec token *spec*."""
    from qgis.PyQt.QtCore import QDateTime, QTimeZone
    if spec == SPEC_LOCAL:
        return QDateTime(qdate, qtime)
    if spec == SPEC_UTC:
        return QDateTime(qdate, qtime, QTimeZone(QTimeZone.Initialization.UTC))
    if spec.startswith("tz:"):
        zone = QTimeZone(spec[3:].encode("ascii", "replace"))
        if not zone.isValid():
            raise LegacyDateTimeUnresolved(f"fuseau {spec[3:]!r} inconnu de Qt")
        return QDateTime(qdate, qtime, zone)
    zone = _qtimezone_for_suffix(spec)
    if zone is None:
        raise LegacyDateTimeUnresolved(f"decalage {spec!r} illisible")
    return QDateTime(qdate, qtime, zone)


def journal_datetime_matches(live: Any, journal_value: Any,
                             event_schema_version: Any = _UNSPECIFIED) -> bool:
    """True when the live QDateTime *live* IS the journaled *journal_value*.

    One reading only, the one the text's generation implies, at the
    resolution that text carries. The comparator of the restore path
    accepted the journal text when it matched the naive, the UTC OR the
    local rendering of the live value: a value restored shifted by the UTC
    offset matched one of the three, so every proof and every idempotence
    check passed on an altered value.

      * v7 text: instant equality, for every spec, a naive value
        included (both sides are anchored to the zone of this process), at
        the resolution the live (stored) value carries
        (`instants_match`): to the millisecond, or within the second when
        the provider stores whole seconds (SpatiaLite, PostgreSQL
        ``timestamp(0)``) and the journal holds milliseconds;
      * schema 6 text (or no version given): the UTC digits of the live
        value, to the second (6.x wrote no milliseconds);
      * schema <= 5 text: the digits of the live value IN ITS OWN SPEC,
        to the second: that is exactly what 5.x wrote when it read this
        value from the same provider. A value typed in a form and
        converted by a GeoPackage (journaled with the form's local digits,
        handed back in UTC) does not match: the proof refuses rather than
        accept a second reading, which would accept a value shifted by
        the offset too.
    """
    if live is None or not isinstance(journal_value, str):
        return False
    match = _ISO_DATETIME_RE.match(journal_value.strip())
    if match is None:
        return False
    try:
        if not live.isValid():
            return False
        if match.group(7) is not None or match.group(8):
            decoded = _deserialize_datetime(journal_value, event_schema_version)
            if decoded is journal_value or not decoded.isValid():
                return False
            return instants_match(int(live.toMSecsSinceEpoch()),
                                  int(decoded.toMSecsSinceEpoch()))
        seen = live if _is_provider_digits(event_schema_version) else live.toUTC()
        return seen.toString("yyyy-MM-ddTHH:mm:ss") == _legacy_digits(match)
    except (AttributeError, TypeError):
        return False


def instants_match(stored_ms: int, expected_ms: int) -> bool:
    """True when a stored instant holds the expected one.

    Equal to the millisecond, or, when the STORED side has no fraction of
    a second while the expected side has one, less than a second apart:
    v7 journals milliseconds (a ``now()`` default typed in a form), and a
    provider that stores date-times to the second truncates them
    (SpatiaLite, measured: ``14:30:12.517`` written, ``14:30:12.000`` read
    back; Oracle ``DATE``) or rounds them (PostgreSQL ``timestamp(0)``).
    The only whole seconds within a second of a value with a fraction are
    the two that truncating and rounding give. The reverse never matches:
    a provider does not invent milliseconds, so a stored ``.517`` against
    an expected ``.000`` is another value.
    """
    if stored_ms == expected_ms:
        return True
    return (stored_ms % 1000 == 0 and expected_ms % 1000 != 0
            and abs(stored_ms - expected_ms) < 1000)


def _legacy_digits(match) -> str:
    year, month, day, hour, minute = match.groups()[:5]
    return f"{year}-{month}-{day}T{hour}:{minute}:{match.group(6) or '00'}"


def _is_provider_digits(event_schema_version: Any) -> bool:
    """True when pre-v7 date-time digits are in the provider's own spec."""
    if event_schema_version is _UNSPECIFIED:
        return False
    try:
        version = int(event_schema_version)
    except (TypeError, ValueError):
        # NULL column: written before the column existed, long before 6.0.
        return True
    return version < UTC_DIGITS_SCHEMA_VERSION


def _is_pre_v7_text(value: Any) -> bool:
    """True for a date-time text without fraction nor zone suffix."""
    if not isinstance(value, str):
        return False
    match = _ISO_DATETIME_RE.match(value.strip())
    return (match is not None and match.group(7) is None
            and not match.group(8))


def datetime_instant_ms(value: Any) -> Optional[int]:
    """Milliseconds since the epoch of a live date-time value, else None.

    A QDateTime (any spec) or a Python datetime (a naive one is local
    time, like a LocalTime QDateTime). None for anything else, an invalid
    value included: the caller then compares it by its other rules.
    """
    if type(value).__name__ == "QDateTime":
        try:
            return int(value.toMSecsSinceEpoch()) if value.isValid() else None
        except (AttributeError, TypeError):
            return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return round(value.timestamp() * 1000)
        try:
            from qgis.PyQt.QtCore import QDate, QDateTime, QTime
        except ImportError:
            return round(value.timestamp() * 1000)
        return int(QDateTime(
            QDate(value.year, value.month, value.day),
            QTime(value.hour, value.minute, value.second,
                  value.microsecond // 1000)).toMSecsSinceEpoch())
    return None


def journal_text_instant_ms(text: Any) -> Optional[int]:
    """Instant of a date-time TEXT as Qt reads it (what a provider stores).

    A ``Z`` / offset suffix is honoured, a text without one is a local
    wall clock: exactly what QGIS makes of that string when it is handed
    to a date-time field. None when *text* is not a date-time.
    """
    if not isinstance(text, str):
        return None
    match = _ISO_DATETIME_RE.match(text.strip())
    if match is None:
        return None
    if match.group(8):
        value = _deserialize_datetime(text, 7)
        return None if value is text else datetime_instant_ms(value)
    try:
        from qgis.PyQt.QtCore import QDate, QDateTime, QTime
    except ImportError:
        # Benign: pure-Python unit tests run without Qt bindings.
        return None
    year, month, day, hour, minute = (int(part) for part in match.groups()[:5])
    fraction = match.group(7)
    value = QDateTime(QDate(year, month, day), QTime(
        hour, minute, int(match.group(6) or 0),
        int((fraction + "00")[:3]) if fraction else 0))
    return datetime_instant_ms(value)


def _qtimezone_for_suffix(suffix: str):
    """QTimeZone for 'Z' / '+01:00', or None when Qt cannot build one."""
    try:
        from qgis.PyQt.QtCore import QTimeZone
    except ImportError:
        return None
    try:
        if suffix in ("Z", "z"):
            return QTimeZone(QTimeZone.Initialization.UTC)
        offset = _utc_offset_seconds(suffix)
        factory = getattr(QTimeZone, "fromSecondsAheadOfUtc", None)
        return factory(offset) if factory is not None else QTimeZone(offset)
    except (AttributeError, TypeError, ValueError):
        return None


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
    match = _ISO_TIME_RE.match(value.strip())
    if match is None:
        return value
    fraction = match.group(4)
    parsed = QTime(int(match.group(1)), int(match.group(2)),
                   int(match.group(3) or 0),
                   int((fraction + "00")[:3]) if fraction else 0)
    return parsed if parsed.isValid() else value


def serialize_attributes(feature, field_names: List[str]) -> Dict[str, Any]:
    """Serialize all attributes of a QgsFeature to a JSON-safe dict."""
    result = {}
    for name in field_names:
        try:
            result[name] = serialize_value(read_attribute(feature, name))
        except (KeyError, IndexError):
            result[name] = None
    return result


def read_attribute(feature, name: Any) -> Any:
    """Value of attribute *name*, exactly as the feature holds it.

    ``feature[name]`` and ``feature.attribute(name)`` agree on every type
    but one: a text holding a lone UTF-16 surrogate comes back from
    ``feature[name]`` with the half-character silently REMOVED
    ('ab\\ud83dcd' -> 'abcd', verified on QGIS 4.2), while the provider
    stores U+FFFD in its place. The journal then held a value the source
    never had. ``attribute()`` hands the half-character over, and
    `scrub_surrogates` turns it into what the provider stores.

    Stand-in features of the test harness only implement ``__getitem__``.
    """
    getter = getattr(feature, "attribute", None)
    if callable(getter):
        return getter(name)
    return feature[name]


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
    return journal_json_dumps(schema)


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
    return journal_json_dumps({"changed_only": changed})


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
    return journal_json_dumps({"all_attributes": attrs})


# Providers whose date-time spec is fixed by the COLUMN, never by the row:
# PostgreSQL hands every value of a ``timestamp`` column back as a local
# wall clock carrying the stored digits, and converts every value of a
# ``timestamptz`` column to the session zone. There, the spec measured on
# the live values of a column is the spec of every value it ever held,
# a deleted row and an overwritten old value included.
#
# Everywhere else the spec travels with EACH stored value, and a value
# that is no longer stored cannot be measured. Measured on QGIS 4.2.1: a
# GeoPackage written by "Export / Save Features As" (QgsVectorFileWriter)
# holds naive text, handed back as local time, while a value committed
# through the provider holds ``...Z``, handed back as UTC, so an exported
# GeoPackage that is then edited holds both; SQLite (OGR), GeoJSON,
# FlatGeobuf, SpatiaLite and memory keep whatever spec each value was
# written with, so rows imported in UTC and rows typed in local time sit
# side by side. A column that may carry an offset per value (MSSQL
# ``datetimeoffset``, Oracle ``TIMESTAMP WITH TIME ZONE``) is in the same
# case, and the other providers are not measured.
_COLUMN_SPEC_PROVIDERS = ("postgres",)
_PER_VALUE_SPEC_PROVIDERS = ("ogr", "spatialite", "memory", "delimitedtext")


class ProviderTimeSpecs:
    """The time spec in which 5.x read a date-time, per field, when provable.

    Schema <= 5 journaled ``value.toString('yyyy-MM-ddTHH:mm:ss')`` of the
    value the provider (or the edit buffer) handed over, so its digits are
    in THAT value's spec, which the text does not say.

    The spec is established only where it belongs to the column: on a
    provider of `_COLUMN_SPEC_PROVIDERS`, from EVERY non-null live value of
    the field read through the provider (the committed data, never the
    edit buffer of a restore in progress; no sample: a mix past any bound
    would go unseen), and only when they all agree. The token is
    `SPEC_LOCAL`, `SPEC_UTC`, ``+HH:MM`` or ``tz:<id>``.

    Refused, with the reason, and the caller refuses the field:
      * a provider whose spec belongs to each stored value: the value of a
        DELETE snapshot or the old value of an UPDATE is no longer stored,
        and the rows that remain prove nothing about its form. Measured:
        remaining rows all naive while the deleted one held ``Z`` (an
        exported GeoPackage, one feature added in QGIS then deleted) gave
        a restore one UTC offset early, reported "Restored";
      * a value captured from the edit buffer (INSERT snapshot, UPDATE new
        value): the spec the form or the expression gave it was never
        recorded, and no column says it;
      * no live value, or live values whose specs disagree.

    One instance per layer and restore run: the measurement is cached per
    field (an attribute-only, geometry-less read of the whole column).
    """

    def __init__(self, layer, captured_from: str = CAPTURED_FROM_PROVIDER):
        self._layer = layer
        self.captured_from = captured_from
        self._cache: Dict[str, tuple] = {}

    def spec_for(self, field_name: str) -> Optional[str]:
        return self.resolve(field_name)[0]

    def reason_for(self, field_name: str) -> str:
        return self.resolve(field_name)[1]

    def resolve(self, field_name: str) -> tuple:
        """``(token, reason)``: token None when the spec is not established."""
        if field_name not in self._cache:
            try:
                self._cache[field_name] = self._measure(field_name)
            except Exception as exc:  # noqa: BLE001 - unmeasurable is a refusal
                flog(f"serialize: time spec of {field_name!r} unreadable "
                     f"({type(exc).__name__}: {exc})", "WARNING")
                self._cache[field_name] = (
                    None, f"lecture du fuseau impossible ({exc})")
        return self._cache[field_name]

    def _measure(self, field_name: str) -> tuple:
        provider = self._layer.dataProvider()
        name = _provider_name(provider)
        if name not in _COLUMN_SPEC_PROVIDERS:
            return None, _per_value_reason(name)
        if self.captured_from == CAPTURED_FROM_EDIT_BUFFER:
            return None, ("valeur saisie (tampon d'edition) : le fuseau que "
                          "la saisie lui donnait n'a jamais ete journalise, "
                          "et la colonne ne le dit pas")
        fields = provider.fields()
        idx = fields.indexOf(field_name)
        if idx < 0:
            return None, "champ absent du provider"
        tokens = set()
        n_values = 0
        for value in _live_values(provider, field_name, idx):
            token = _spec_token(value)
            if token is None:
                continue
            n_values += 1
            tokens.add(token)
            if len(tokens) > 1:
                break
        if not tokens:
            return None, ("aucune valeur de ce champ dans la couche : le "
                          "fuseau que rend le provider n'est pas mesurable")
        if len(tokens) > 1:
            return None, (f"fuseaux melanges dans ce champ "
                          f"({', '.join(sorted(tokens))}, vus sur "
                          f"{n_values} valeur(s)) : celui de la valeur "
                          f"journalisee n'est pas etabli")
        token = tokens.pop()
        flog(f"serialize: provider time spec of {field_name!r} = {token} "
             f"({n_values} value(s) read, the whole column, "
             f"captured_from={self.captured_from})", "DEBUG")
        return token, ""


def provider_time_specs(layer, captured_from: str = CAPTURED_FROM_PROVIDER
                        ) -> ProviderTimeSpecs:
    """The time-spec measurement of *layer* restores pass to the decoders.

    *captured_from* says where the journaled values were read at capture:
    `CAPTURED_FROM_PROVIDER` for DELETE snapshots and UPDATE old values,
    `CAPTURED_FROM_EDIT_BUFFER` for INSERT snapshots and UPDATE new values.
    """
    return ProviderTimeSpecs(layer, captured_from)


def _provider_name(provider) -> str:
    try:
        return str(provider.name() or "")
    except (AttributeError, RuntimeError):
        return ""


def _per_value_reason(name: str) -> str:
    """Why a provider outside `_COLUMN_SPEC_PROVIDERS` is refused, in French."""
    if name in _PER_VALUE_SPEC_PROVIDERS:
        return (f"le provider {name} garde le fuseau de chaque valeur "
                f"stockee : celui d'une ligne supprimee ou d'une ancienne "
                f"valeur n'est plus lisible, et les lignes restantes ne le "
                f"prouvent pas (une GeoPackage exportee stocke un texte sans "
                f"fuseau, une saisie QGIS un texte UTC)")
    return (f"provider {name or 'inconnu'} non mesure : une colonne peut y "
            f"porter un fuseau par valeur, celui d'une ligne disparue ne se "
            f"deduit pas des autres")


def _live_values(provider, field_name: str, idx: int):
    """Every non-null value of the field, attribute-only, no geometry."""
    from qgis.core import QgsExpression, QgsFeatureRequest
    from ..compat import QgisCompat
    request = QgsFeatureRequest()
    request.setFlags(QgisCompat.NO_GEOMETRY)
    request.setSubsetOfAttributes([idx])
    request.setFilterExpression(
        f"{QgsExpression.quotedColumnRef(field_name)} IS NOT NULL")
    for feature in provider.getFeatures(request):
        yield feature.attribute(idx)


def _spec_token(value: Any) -> Optional[str]:
    """Spec token of one live value; None when it is not a date-time."""
    if type(value).__name__ != "QDateTime" or _is_null(value):
        return None
    if not value.isValid():
        return None
    spec = _time_spec_name(value)
    if spec == "LocalTime":
        return SPEC_LOCAL
    if spec == "UTC":
        return SPEC_UTC
    offset = int(value.offsetFromUtc())
    if spec == "TimeZone":
        if _zone_is_utc(value):
            return SPEC_UTC
        try:
            from qgis.PyQt.QtCore import QTimeZone
            zone_id = bytes(value.timeZone().id()).decode("ascii", "replace")
            system_id = bytes(QTimeZone.systemTimeZoneId()).decode(
                "ascii", "replace")
        except (AttributeError, TypeError, ValueError):
            return None
        if zone_id and zone_id == system_id:
            return SPEC_LOCAL
        return f"tz:{zone_id}" if zone_id else None
    # OffsetFromUTC: a provider that hands back the local wall clock with
    # its offset (PostgreSQL ``timestamptz`` in the session zone, whose
    # offset follows daylight saving) is local time; any other offset is
    # kept as that fixed offset. Known limit: a store holding a FIXED
    # offset that happens to equal the local one for every sampled date
    # is read as local time.
    local_offset = int(value.toLocalTime().offsetFromUtc())
    if offset == local_offset:
        return SPEC_LOCAL
    sign = "-" if offset < 0 else "+"
    offset = abs(offset)
    return f"{sign}{offset // 3600:02d}:{(offset % 3600) // 60:02d}"


def legacy_datetime_refusals(mapping: Dict[str, str], attrs: Dict, fields,
                             event_schema_version: Any,
                             time_specs: Optional[ProviderTimeSpecs]
                             ) -> Dict[str, str]:
    """``{current field name: reason}`` of the schema <= 5 date-times to refuse.

    A caller that restores what it can drops these fields from *mapping*
    before `iter_mapped_attributes` and reports them to the user as not
    restored (with the reason), exactly like a field the schema drift
    forbids. Empty for any event of schema 6 or later, for text that
    carries its own spec, and for a field whose provider spec *time_specs*
    establishes.
    """
    if not _is_provider_digits(event_schema_version):
        return {}
    refused: Dict[str, str] = {}
    for hist_name, curr_name in mapping.items():
        value = attrs.get(hist_name)
        if not _is_pre_v7_text(value):
            continue
        idx = fields.indexOf(curr_name)
        if idx < 0 or not _is_datetime_field(fields, idx):
            continue
        if time_specs is None:
            refused[curr_name] = ("le fuseau du provider n'a pas ete mesure "
                                  "par l'appelant")
            continue
        token, reason = time_specs.resolve(curr_name)
        if token is None:
            refused[curr_name] = reason
    return refused


def _is_datetime_field(fields, idx: int) -> bool:
    try:
        return (_normalize_type_name(fields.at(idx).typeName())
                in _DATETIME_TYPE_NAMES)
    except (AttributeError, IndexError, TypeError):
        return False


def iter_mapped_attributes(mapping: Dict[str, str], attrs: Dict, fields,
                           event_schema_version: Any = _UNSPECIFIED,
                           time_specs: Optional[ProviderTimeSpecs] = None):
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

    Callers pass the ``event_schema_version`` of the event *attrs* comes
    from: date-time text written before v7 cannot be decoded without it
    (see `deserialize_value`). For a schema <= 5 event they also pass
    *time_specs* (`provider_time_specs` of the target layer, with the
    ``captured_from`` of the values written): the digits of a date-time
    are read in the spec the provider hands back. A date-time that cannot
    be read that way (no *time_specs*, or a spec it could not establish)
    raises `LegacyDateTimeRefused` naming every such field, and does so
    BEFORE the first value is yielded: the caller has written nothing.
    Restoring the other fields means dropping those beforehand
    (`legacy_datetime_refusals`) and reporting them.
    """
    decoded = []
    refused: Dict[str, str] = {}
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
        value = attrs[hist_name]
        spec = None
        if (time_specs is not None
                and _is_provider_digits(event_schema_version)
                and _is_pre_v7_text(value)
                and _is_datetime_field(fields, idx)):
            spec = time_specs.spec_for(curr_name)
        try:
            decoded.append((idx, _decode_for_field(
                value, fields, idx, event_schema_version, spec)))
        except LegacyDateTimeUnresolved as exc:
            refused[curr_name] = (
                time_specs.reason_for(curr_name) if time_specs is not None
                else "le fuseau du provider n'a pas ete mesure par "
                     "l'appelant") or str(exc)
    if _skipped:
        flog(f"iter_mapped_attributes: {len(_skipped)} field(s) skipped "
             f"(schema drift): {_skipped[:5]}", "WARNING")
    if refused:
        flog(f"iter_mapped_attributes: schema={event_schema_version!r} "
             f"date-time field(s) refused, nothing yielded: {refused}",
             "ERROR")
        raise LegacyDateTimeRefused(refused)
    for pair in decoded:
        yield pair


def _decode_for_field(value: Any, fields, idx: int,
                      event_schema_version: Any = _UNSPECIFIED,
                      legacy_spec: Optional[str] = None) -> Any:
    """Decode *value* for the field at *idx*, never failing the restore.

    `LegacyDateTimeUnresolved` is the one exception let through: the raw
    text of a schema <= 5 date-time handed to the provider would be
    re-anchored to local time, the shift the refusal exists to prevent.
    """
    try:
        type_name = fields.at(idx).typeName()
    except (AttributeError, IndexError, TypeError):
        return value
    try:
        return deserialize_value(value, type_name, event_schema_version,
                                 legacy_spec)
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
