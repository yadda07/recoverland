"""Local restore service for RecoverLand (RLU-040 to RLU-043).

Restores audited features back into their source layers.
Handles DELETE restore (re-insert), UPDATE restore (revert attributes/geometry),
conflict detection, and batch operations with per-entity isolation.
"""
import json
import math
from typing import List, Dict, Optional, NamedTuple, Any, Tuple

from .audit_backend import AuditEvent, RestoreReport
from .schema_drift import (
    parse_field_schema, extract_current_schema,
    compare_schemas, DriftReport, safe_field_mapping,
    dropped_fields, format_drift_message,
)
from .search_service import reconstruct_attributes, reconstruct_new_attributes
from .geometry_utils import (
    rebuild_geometry, is_geometry_present,
    feature_matches_geometry, get_feature_source,
    geometry_for_layer_crs, extract_crs_authid,
    heavy_diag_enabled,
)
from .identity import (
    get_identity_strength_for_layer, effective_identity, composite_key,
    composite_key_filter,
)
from .support_policy import IdentityStrength
from .serialization import (
    deserialize_value, is_layer_audit_field, iter_mapped_attributes,
    journal_datetime_matches, provider_time_specs, legacy_datetime_refusals,
    CAPTURED_FROM_PROVIDER, CAPTURED_FROM_EDIT_BUFFER,
)
from .constants import is_null_geometry_marker, NULL_MARKER_SINCE_SCHEMA
from .logger import flog
from ..compat import QgisCompat


# "No event known": the comparison has no generation to read the journal
# text with, and keeps the historical type-driven decoding.
_NO_EVENT = object()


def _qgis_vals_equal(actual, expected,
                     event_schema_version: Any = _NO_EVENT) -> bool:
    """Type-aware equality for QGIS layer values vs SQLite-stored strings.

    QGIS reads date/time fields as QDate/QDateTime/QTime objects, but the
    audit journal stores them as ISO strings.  A direct ``!=`` comparison
    always returns True (unequal types), causing snapshot matching to fail
    and re-inserted features to never be found during undo, which leads to
    feature accumulation on repeated rewinds (BUG-REWIND-02).

    Timezone handling (BL-DIAG-P0-01): The journal stores QDateTime WITHOUT
    timezone suffix (serialization.py uses toString without tz). If the
    provider returns the same instant in a different timeSpec (e.g. UTC vs
    OffsetFromUTC), naive toString produces different digits for the same
    instant.  We try UTC-normalized comparison as fallback.

    Binary fields (BL-SER-P0-01): the write side of this module decodes
    every restored value through ``iter_mapped_attributes`` /
    ``deserialize_value``, so a restored blob sits in the layer as the
    QByteArray it always was, while the event still carries the ``b64:``
    text of those same bytes. Comparing the two raw says "different" on
    two identical values, and every lookup built on this function goes
    blind on any layer holding an attachment: the idempotence guard of
    ``restore_deleted_feature`` stops recognising the feature it just
    wrote (replay adds a copy), the attribute rescue scan finds nothing,
    and the strict snapshot scan falls through to its lenient branch,
    which accepts a candidate it could not prove. So the journal side is
    decoded with the SAME decoder the write side uses -- driven here by
    the runtime type of the live value, the only type information a
    comparison has -- before being compared.

    Date-times, when the event is known (*event_schema_version* given, v7):
    one reading of the journal text, the one its generation implies, and
    instant equality to the millisecond
    (`serialization.journal_datetime_matches`). The three renderings below
    (naive, UTC, local digits) accepted the text as soon as ONE of them
    matched: a value in another zone, or one restored shifted by the UTC
    offset, has the same naive digits as another instant, and every proof
    built on this function passed on a value hours away from the journal's.
    They remain only for the callers that hold no event.
    """
    if actual == expected:
        return True
    if (isinstance(actual, float) and isinstance(expected, float)
            and math.isnan(actual) and math.isnan(expected)):
        return True
    if event_schema_version is not _NO_EVENT:
        pair = _datetime_and_text(actual, expected)
        if pair is not None:
            return journal_datetime_matches(pair[0], pair[1],
                                            event_schema_version)
    decoded = _decoded_like(expected, actual, event_schema_version)
    if decoded is not expected:
        try:
            if actual == decoded:
                return True
        except (TypeError, ValueError):
            # Benign: two types the provider never puts side by side.
            pass
    try:
        from qgis.PyQt.QtCore import QDate, QDateTime, QTime
        if isinstance(actual, QDate) and not isinstance(actual, QDateTime):
            return actual.toString('yyyy-MM-dd') == expected
        if isinstance(actual, QDateTime):
            return _qdatetime_matches_str(actual, expected)
        if isinstance(actual, QTime):
            return _qtime_matches_str(actual, expected)
        if isinstance(expected, QDate) and not isinstance(expected, QDateTime):
            return expected.toString('yyyy-MM-dd') == actual
        if isinstance(expected, QDateTime):
            return _qdatetime_matches_str(expected, actual)
        if isinstance(expected, QTime):
            return _qtime_matches_str(expected, actual)
    except ImportError:
        pass
    return False


def _qtime_matches_str(qt, s) -> bool:
    """Compare a live QTime against stored journal text using instants_match."""
    if not isinstance(s, str):
        return False
    from .serialization import _deserialize_time, instants_match
    decoded = _deserialize_time(s)
    if decoded is s or not getattr(decoded, "isValid", lambda: False)():
        return False
    if not qt.isValid():
        return False
    return instants_match(int(qt.msecsSinceStartOfDay()),
                          int(decoded.msecsSinceStartOfDay()))


def _decoded_like(value, actual, event_schema_version: Any = _NO_EVENT):
    """Journal *value* decoded into the runtime type of *actual*.

    ``deserialize_value`` keys on a type NAME, and the names it accepts
    include the Python/Qt ones (``QByteArray``, ``QDateTime``, ``str``,
    ``int``...), so the live value read from the layer can stand in for
    the field metadata a bare comparison does not carry. The event's
    schema version, when known, goes with it.

    Returns *value* itself when nothing was decoded (unknown type, value
    already in that type, decoder refusal), which the caller uses as the
    signal to skip the extra comparison. Never raises: a comparison that
    explodes would abort a scan and lose a restore, and an undecodable
    payload is exactly what the caller's raw comparison already covers.
    """
    if value is None or actual is None:
        return value
    try:
        if event_schema_version is _NO_EVENT:
            return deserialize_value(value, type(actual).__name__)
        return deserialize_value(value, type(actual).__name__,
                                 event_schema_version)
    except Exception as exc:  # noqa: BLE001 - a comparison never raises
        # The decoder refuses on purpose what it cannot read without
        # guessing (a <= 5.x date-time without its provider time spec):
        # for a comparison that is "not proven equal", never a crash of
        # the scan that asked.
        flog(f"_decoded_like: decode failed for {type(actual).__name__}: "
             f"{exc}", "DEBUG")
        return value


def _datetime_and_text(actual, expected) -> Optional[Tuple[Any, str]]:
    """``(live QDateTime, journal text)`` when the pair is one, else None."""
    try:
        from qgis.PyQt.QtCore import QDateTime
    except ImportError:
        return None
    if isinstance(actual, QDateTime) and isinstance(expected, str):
        return actual, expected
    if isinstance(expected, QDateTime) and isinstance(actual, str):
        return expected, actual
    return None


def _qdatetime_matches_str(dt, s) -> bool:
    """Compare a QDateTime against a stored ISO string, timezone-aware.

    Tries:
    1. Naive toString (original timeSpec digits)
    2. UTC-normalized digits (handles provider returning different timeSpec)
    3. LocalTime-normalized digits (handles stored as local, read as UTC)
    """
    fmt = 'yyyy-MM-ddTHH:mm:ss'
    iso = dt.toString(fmt)
    if iso == s or iso.replace('T', ' ') == s:
        return True
    utc_iso = dt.toUTC().toString(fmt)
    if utc_iso == s or utc_iso.replace('T', ' ') == s:
        return True
    local_iso = dt.toLocalTime().toString(fmt)
    if local_iso == s or local_iso.replace('T', ' ') == s:
        return True
    return False


class PreCheckResult(NamedTuple):
    can_restore: bool
    reason: str
    drift: Optional[DriftReport]


def pre_check_restore(layer, event: AuditEvent,
                      write_op: Optional[str] = None) -> PreCheckResult:
    """Validate that a restore operation is feasible on the target layer.

    *write_op* names the event operation whose REVERSAL is about to be
    written, when it is not the event's own: the undo of an INSERT restore
    re-inserts the feature, so it needs what a DELETE restore needs (the
    add capability), not the delete capability the INSERT restore needed.
    """
    if layer is None:
        return PreCheckResult(False, "Target layer not found", None)

    provider = layer.dataProvider()
    if provider is None:
        return PreCheckResult(False, "No data provider", None)

    if not _can_write(provider, write_op or event.operation_type):
        return PreCheckResult(False, "Provider lacks write capability", None)

    hist_schema = parse_field_schema(event.field_schema_json)
    curr_schema = extract_current_schema(layer)
    drift = compare_schemas(hist_schema, curr_schema)

    # RLU-053. Two reasons to refuse, and one message that names BOTH.
    #   - a captured column has vanished: its old value has nowhere to go;
    #   - a captured column was retyped: the value cannot enter it either.
    # The old wording only listed the vanished columns. A user who reads
    # "missing fields [code, alt]" recreates those two, replays the
    # restore convinced the problem is solved, and loses the retyped
    # field without a word. A partial restore announced as a full one is
    # the failure this invariant exists to prevent, so the pre-check
    # refuses instead of writing a hybrid feature.
    captured = reconstruct_attributes(event)
    if not isinstance(captured, dict):
        # Unreadable attribute payload: assume every drifted field was
        # captured rather than let a corrupt event through unchecked.
        captured = None
    lost = dropped_fields(drift, captured)
    if drift.missing_in_current or lost:
        reason = f"Schema drift: {format_drift_message(drift)}"
        return PreCheckResult(False, reason, drift)

    return PreCheckResult(True, "OK", drift)


def _layer_geometry_type(layer):
    """Geometry type of *layer*, or GEOM_UNKNOWN when it cannot be read."""
    try:
        return layer.geometryType()
    except (AttributeError, RuntimeError):
        return QgisCompat.GEOM_UNKNOWN


def _layer_wkb_type_repr(layer) -> str:
    """Readable WKB type of *layer* for diagnostics, never raising."""
    try:
        return str(layer.wkbType())
    except (AttributeError, RuntimeError):
        return "?"


def _dirty_session_refusal(layer_error: Optional[str],
                           context: str) -> Optional[Dict[str, Any]]:
    """Turn a `validate_restore_layer_state` verdict into a refusal dict.

    The event-by-event restore writes THROUGH the provider, under the
    user's editing buffer. His own attribute values are still in that
    buffer, so his next Save replays them over what we just wrote, while
    the geometry -- which was not in his buffer -- keeps the restored
    one. The feature ends up in a state it never had: geometry from
    yesterday, attributes from today, and a journal entry claiming a
    successful restore. `restore_batch` has always consulted the guard;
    the single-event path must too, or the same event on the same layer
    succeeds or fails depending on which button the user pressed.
    """
    if not layer_error:
        return None
    flog(f"{context}: refused, {layer_error}", "WARNING")
    return {
        "success": False,
        "reason_code": "dirty_edit_session",
        "message": layer_error,
    }


# -----------------------------------------------------------------------
# Target identity (v7)
#
# A restore write lands on ONE feature, and nothing downstream can tell
# whether it was the right one: the journal records the restore, the
# report counts it, and the feature that took the write keeps it. So every
# destructive write (delete, overwrite) first resolves its target to one
# of five levels, logged and returned with the result:
#
#   STRONG        a provider primary key designated the feature AND its
#                 content corroborates the state the event describes. A
#                 key alone is no proof: an INTEGER PRIMARY KEY without
#                 AUTOINCREMENT hands the highest id out again, sequences
#                 get reset, business keys get edited by users;
#   MEDIUM        a FID or a content scan designated it, and the content
#                 match is discriminating: two captured values or more,
#                 the geometry no other feature has, or the one value no
#                 other feature carries;
#   WEAK          a candidate shares SOME of the evidence (one common
#                 value, the same outline under other attributes);
#   AMBIGUOUS     several features satisfy the same proof;
#   UNRESOLVABLE  no candidate, nothing comparable, or the read failed.
#
# Only STRONG and MEDIUM may receive a write. WEAK and AMBIGUOUS are named
# refusals, a failed read is its own failure (``scan_failed``), and only a
# read that COMPLETED may be taken as proof of absence.
# -----------------------------------------------------------------------

ID_STRONG = "STRONG"
ID_MEDIUM = "MEDIUM"
ID_WEAK = "WEAK"
ID_AMBIGUOUS = "AMBIGUOUS"
ID_UNRESOLVABLE = "UNRESOLVABLE"
WRITABLE_LEVELS = frozenset((ID_STRONG, ID_MEDIUM))


class TargetScanError(RuntimeError):
    """A read of the target layer failed: what it did not return proves nothing."""


class TargetResolution(NamedTuple):
    """Where a write may land, and how sure we are (levels above).

    *absent* is True only when every read completed and none found the
    feature: the one case where "not found" means "not there".
    *nominated* is the feature the identity pointed at, proven or not.
    """

    fid: Optional[int]
    level: str
    reason: str
    via: str = ""
    absent: bool = False
    nominated: Optional[int] = None

    @property
    def writable(self) -> bool:
        return self.fid is not None and self.level in WRITABLE_LEVELS


def _is_null(value) -> bool:
    """None, or a null QVariant (the QGIS 3 form of an empty cell)."""
    if value is None:
        return True
    if type(value).__name__ == "QVariant":
        try:
            return bool(value.isNull())
        except (AttributeError, RuntimeError, TypeError):
            return False
    return False


def _vals_match(actual, expected, version: Any = _NO_EVENT) -> bool:
    """Equality where NULL matches NULL only.

    The scanners used to skip a field as soon as either side was NULL. A
    snapshot captured with empty cells then matched EVERY feature, and a
    feature with empty cells matched every snapshot: the proof of identity
    was vacuous exactly where the data was thinnest.

    *version* is the ``event_schema_version`` of the event *expected* comes
    from, when known (see `_qgis_vals_equal`).
    """
    a_null, e_null = _is_null(actual), _is_null(expected)
    if a_null or e_null:
        return a_null and e_null
    return _qgis_vals_equal(actual, expected, version)


def _event_version(event) -> Any:
    """``event_schema_version`` of *event* for the comparators, or `_NO_EVENT`."""
    if event is None:
        return _NO_EVENT
    return getattr(event, "event_schema_version", None)


def _is_unset_marker(value) -> bool:
    """True for a `QgsUnsetAttributeValue` the capture journalled as a value.

    An attribute left to the database in the QGIS form ("Autogenerate", a
    DEFAULT clause) travels through the edit buffer as an unset marker;
    the capture of the INSERT journals its repr
    ("<QgsUnsetAttributeValue: AUTO>"), not what the database wrote. It is
    no stored value: it can neither prove nor disprove anything, and it
    must never be written back as text.
    """
    if isinstance(value, str):
        return value.startswith("<QgsUnsetAttributeValue")
    return type(value).__name__ == "QgsUnsetAttributeValue"


def _provider_error_count(source) -> int:
    """Number of errors the provider behind *source* has reported so far."""
    try:
        provider = source.dataProvider() if hasattr(source, "dataProvider") \
            else source
        errors = provider.errors() if provider is not None else ()
        return len(errors)
    except (AttributeError, RuntimeError, TypeError):
        return 0


def _scan(source, request=None, what: str = "", provider_only: bool = False):
    """Features of *source* for *request*; raises TargetScanError on a failed read.

    A provider that cannot open a read (dropped PostGIS connection, locked
    file) returns an INVALID iterator, and an invalid iterator yields
    nothing -- exactly what a layer without the feature yields. Every
    "absent" the restore paths used to conclude rested on that silence:
    during a connection drop each INSERT compensation was reported "already
    absent" and the inserted features stayed in the layer under a
    successful rewind. So the iterator is checked, an exception during the
    read is reported instead of swallowed, and an error the provider pushed
    while reading voids the read.
    """
    from qgis.core import QgsFeatureRequest

    if request is None:
        request = QgsFeatureRequest()
    errors_before = _provider_error_count(source)
    try:
        if provider_only:
            iterator = source.dataProvider().getFeatures(request)
        else:
            iterator = get_feature_source(source)(request)
    except Exception as exc:  # noqa: BLE001 - turned into a named failure
        raise TargetScanError(f"{what}: read failed ({exc})") from exc
    is_valid = getattr(iterator, "isValid", None)
    if callable(is_valid):
        try:
            valid = bool(is_valid())
        except (RuntimeError, TypeError):
            valid = True
        if not valid:
            raise TargetScanError(
                f"{what}: the provider returned an invalid iterator "
                f"(connection or file unavailable)")
    try:
        for feature in iterator:
            yield feature
    except Exception as exc:  # noqa: BLE001 - turned into a named failure
        raise TargetScanError(f"{what}: read interrupted ({exc})") from exc
    if _provider_error_count(source) > errors_before:
        raise TargetScanError(
            f"{what}: the provider reported an error during the read")


def _read_one(source, fid, what: str = "", provider_only: bool = False):
    """Feature *fid* of *source*, or None when it is not there.

    Raises TargetScanError when the read failed. The id is checked on the
    returned feature: a source that ignored the filter must not hand back
    the first feature it has.

    ``setFilterFids([fid])``, not ``QgsFeatureRequest(fid)``: on a layer,
    the single-FID fast path of the vector layer iterator never marks
    itself valid (QGIS 4.2, measured: ``isValid()`` False while it yields
    the feature), which would turn every read into a "failed read".
    """
    from qgis.core import QgsFeatureRequest

    if fid is None:
        return None
    request = QgsFeatureRequest().setFilterFids([int(fid)])
    for feature in _scan(source, request, what,
                         provider_only=provider_only):
        if feature.id() == fid:
            return feature
    return None


def _captured_from(event: AuditEvent, side: str) -> str:
    """Where the capture read the values a restore writes back.

    Schema <= 5 journalled the digits of the date-time it was HANDED: by
    the provider for a DELETE snapshot and the old values of an UPDATE, by
    the edit buffer for an INSERT snapshot and the new values
    (`serialization.provider_time_specs`). A rewind plan row that
    RE-APPLIES its source (``rewind_dedup.PLAN_MARK_REAPPLY``) carries the
    other side: an UPDATE with old and new swapped, the INSERT snapshot
    under a DELETE row. *side*: ``old`` / ``new`` for an UPDATE,
    ``snapshot`` otherwise.
    """
    from .rewind_dedup import plan_item_kind, PLAN_MARK_REAPPLY
    if side == "snapshot":
        from_buffer = event.operation_type == "INSERT"
    else:
        from_buffer = side == "new"
    if plan_item_kind(event) == PLAN_MARK_REAPPLY:
        from_buffer = not from_buffer
    return CAPTURED_FROM_EDIT_BUFFER if from_buffer else CAPTURED_FROM_PROVIDER


def _from_edit_buffer(event: AuditEvent) -> bool:
    """True when the snapshot of *event* was read from the edit buffer.

    An INSERT snapshot (or a rewind row re-applying one) holds what the
    form or the tool handed to the buffer, NOT what the database stored
    in the columns it fills itself: `_comparable_fields(inserted=True)`.
    A DELETE snapshot was read from the provider: every column is the
    stored value.
    """
    return _captured_from(event, "snapshot") == CAPTURED_FROM_EDIT_BUFFER


# fid_cache entry of the time-spec measurement of a restore run, per
# ``captured_from`` (a 2-tuple whose first item is no field name).
_SPECS_TAG = object()


def _time_specs(layer, captured_from: str, cache: Optional[Dict] = None):
    """`serialization.provider_time_specs` of *layer*, once per run.

    The measurement is lazy (nothing is read until a schema <= 5 date-time
    asks) and cached in the batch's *cache* when there is one.
    """
    key = (_SPECS_TAG, captured_from)
    if cache is not None and key in cache:
        return cache[key]
    specs = provider_time_specs(layer, captured_from)
    if cache is not None:
        cache[key] = specs
    return specs


def _legacy_datetime_refusal(mapping: Dict[str, str], attrs: Dict[str, Any],
                             fields, event: AuditEvent, specs,
                             context: str) -> Optional[Dict[str, Any]]:
    """Refusal dict when a schema <= 5 date-time cannot be written exactly.

    5.x journalled the digits of the value in whatever spec it was handed,
    without saying which. When the provider's spec for the field cannot be
    established (`serialization.legacy_datetime_refusals`: no live value to
    measure, mixed specs, a typed value a GeoPackage converted), writing
    the value means guessing the zone: the restore used to write it
    shifted by the UTC offset and report success. Nothing is written for
    the event (a hybrid feature -- some values restored, the date-time
    not -- is the RLU-053 outcome the restore refuses), and the refusal
    names each field with its reason (reason_code
    ``legacy_datetime_refused``).
    """
    refused = legacy_datetime_refusals(
        mapping, attrs, fields, getattr(event, "event_schema_version", None),
        specs)
    if not refused:
        return None
    detail = "; ".join(f"{name}: {why}" for name, why in sorted(refused.items()))
    flog(f"{context}: status=refused reason_code=legacy_datetime_refused "
         f"eid={event.event_id} schema={event.event_schema_version!r} "
         f"fields={sorted(refused)} detail={detail!r}", "ERROR")
    return {
        "success": False,
        "reason_code": "legacy_datetime_refused",
        "refused_fields": dict(refused),
        "message": (f"Date-time journalled by 5.x or earlier cannot be "
                    f"written without guessing its time zone ({detail}); "
                    f"nothing written"),
    }


def _event_identity(layer, event: AuditEvent) -> Dict[str, Any]:
    """The identity of *event* a restore may trust on *layer*.

    v7: `identity.effective_identity` withdraws a journalled key the layer
    does not prove -- the FIRST column of a composite key, journalled that
    way before v7 ("zone_id=12" names the whole zone), or a column that is
    no longer a key of the layer -- and the restore then proves its target
    by the FID and the content. Parsed without it, the legacy pair was
    looked up as a key: several holders, every such restore refused.
    """
    return effective_identity(_parse_identity(event.feature_identity_json),
                              layer)


def _key_pairs(identity: Dict[str, Any]) -> List[Tuple[str, Any]]:
    """``[(column, value), ...]`` of the key *identity* names, or ``[]``.

    One pair for a single-column key (``pk_field`` / ``pk_value``), every
    column of a COMPLETE composite key (``identity.composite_key``). v7
    captures a composite key without ``pk_field``: read through
    ``pk_field`` alone, such an identity had no key at all -- no key-holder
    check before a re-insert, no lookup by key, a duplicate scan blind to
    every key column.
    """
    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")
    if pk_field and pk_value is not None:
        return [(pk_field, pk_value)]
    return composite_key(identity)


def _identity_via(identity: Dict[str, Any]) -> str:
    """``pk`` when *identity* names a key, ``fid`` otherwise."""
    return "pk" if _key_pairs(identity) else "fid"


def _key_repr(identity: Dict[str, Any]) -> str:
    return ", ".join(f"{name}={value!r}"
                     for name, value in _key_pairs(identity)) or "-"


def _lookup_identity(layer, identity: Dict[str, Any],
                     fid_cache: Optional[Dict] = None,
                     baseline: Optional["_Baseline"] = None
                     ) -> Tuple[List[int], str]:
    """FIDs the captured identity designates, and how (``pk`` or ``fid``).

    The lookup reads the committed state (the provider). In a rewind
    session (*baseline*, see `_committed_index`) that state does not move
    until the session's reading is dropped, so the answer for one identity
    is kept and reused by the other compensations of its entity chain.
    """
    index = _committed_index(layer, baseline) if baseline is not None else None
    memo_key = None
    if index is not None:
        memo_key = (tuple((str(name), repr(value))
                          for name, value in _key_pairs(identity)),
                    repr(identity.get("fid")))
        hit = index.lookups.get(memo_key)
        if hit is not None:
            return list(hit[0]), hit[1]
    found, via = _lookup_identity_read(layer, identity, fid_cache)
    if index is not None:
        index.lookups[memo_key] = (tuple(found), via)
    return found, via


def _lookup_identity_read(layer, identity: Dict[str, Any],
                          fid_cache: Optional[Dict] = None
                          ) -> Tuple[List[int], str]:
    """`_lookup_identity`, read from the provider.

    Two FIDs mean the "key" is not unique (a view, a non-unique column):
    the caller must refuse, not pick. Raises TargetScanError when the read
    failed, so that a lost connection is never taken for "not found".

    A composite key is looked up on ALL its columns (an AND), and like a
    single key it must designate exactly one row.

    The filter is `identity.composite_key_filter` for one column as for
    several, built on the provider's fields: a date-time column is
    compared as an INSTANT (``to_datetime(...)`` with its offset). The
    text literal this function used to build is compared as TEXT by a
    provider that compiles the filter into SQL (OGR): a GeoPackage stores
    ``2026-01-15T08:00:00Z``, the journal says
    ``2026-01-15T08:00:00.000Z``, and the key of a row that was there
    designated nothing -- every restore on a table keyed by a date-time
    answered "not found". A key value the helper cannot read is refused
    as a failed lookup (never turned into "no key", which would match
    every row).
    """
    from qgis.core import QgsFeatureRequest, QgsExpression

    pairs = _key_pairs(identity)
    if pairs:
        key = _key_repr(identity)
        if fid_cache is not None and len(pairs) == 1:
            cached = fid_cache.get((pairs[0][0], str(pairs[0][1])))
            if cached is not None:
                return [cached], "pk"
        try:
            fields = layer.dataProvider().fields()
        except (AttributeError, RuntimeError):
            fields = None
        text = composite_key_filter(pairs, fields)
        if not text:
            raise TargetScanError(
                f"primary key lookup {key}: a key value cannot be read "
                f"(journalled as a Qt repr that does not say its value)")
        expr = QgsExpression(text)
        if expr.hasParserError():
            raise TargetScanError(
                f"primary key lookup {key}: {expr.parserErrorString()}")
        request = QgsFeatureRequest(expr).setLimit(2)
        found = [feat.id() for feat in _scan(
            layer, request, f"pk lookup {key}", provider_only=True)]
        return found, "pk"

    fid = identity.get("fid")
    if fid is None:
        return [], "none"
    feature = _read_one(layer, fid, f"fid lookup {fid}", provider_only=True)
    return ([feature.id()] if feature is not None else []), "fid"


def _comparable_fields(layer, attrs: Dict[str, Any], keep_keys=(),
                       inserted: bool = False) -> List[tuple]:
    """``(name, index)`` of the captured fields a content proof may use.

    Audit columns (rewritten by the provider on every write), primary key
    columns (the identity itself, not evidence about it) and fields the
    layer no longer has are left out. *keep_keys* names key columns that
    ARE evidence for the caller (the columns of a composite key, which a
    re-insert writes back as captured).

    A value the capture journalled as a `QgsUnsetAttributeValue` repr is
    left out too: it is not what the database stored. *inserted* marks the
    snapshot of an INSERT, whose columns the provider fills itself (key,
    DEFAULT clause: `_auto_field_names`) hold whatever the database put
    there, not the NULL the form carried: comparing them refused the undo
    of every feature added through the QGIS form on a layer with a DEFAULT
    column ("differs=[cree, ts]"), and the rewind counted the refusal with
    the absent ones while the feature was still in the layer.
    """
    fields = layer.fields()
    try:
        pk_indices = set(layer.dataProvider().pkAttributeIndexes())
    except (AttributeError, RuntimeError):
        pk_indices = set()
    auto = _auto_field_names(layer) if inserted else set()
    out = []
    for name in attrs:
        value = attrs.get(name)
        idx = fields.indexOf(name)
        if idx < 0 or is_layer_audit_field(name) or _is_unset_marker(value):
            continue
        if idx in pk_indices and name not in keep_keys:
            continue
        if name in auto and name not in keep_keys and _is_null(value):
            continue
        out.append((name, idx))
    return out


class _NoGeometry:
    """Captured state "the feature has no geometry" (see `_expected_geometry`)."""

    def __repr__(self) -> str:
        return "<no geometry>"


NO_GEOMETRY = _NoGeometry()


def _is_null_marker(wkb, event_schema_version=NULL_MARKER_SINCE_SCHEMA) -> bool:
    """True for the explicit "no geometry" side of an UPDATE.

    The capture stores a geometry that appears or disappears with
    ``core.constants.GEOMETRY_NULL_MARKER``, a zero-length BLOB: SQL NULL
    still means "geometry unchanged", the empty BLOB means "the feature
    had no geometry on this side of the edit". It is a state, not an
    undecodable WKB: reading it as corrupt refused every restore of a
    geometry that had been added or removed, and falling back to the
    other side of the event wrote the geometry the edit produced.

    Only since journal schema 7, the capture that writes it: no earlier
    version ever wrote a zero-length BLOB, so in an older event one is
    corruption (`core.constants.is_null_geometry_marker`). Every caller
    that holds the event passes its ``event_schema_version`` (the writer
    `_geometry_for_write` included, v7: its guard no longer depends on
    `_legacy_empty_blob_refusal` running first); the default only serves a
    caller that holds no event.
    """
    return is_null_geometry_marker(wkb, event_schema_version)


def _legacy_empty_blob_refusal(layer, event: AuditEvent,
                               wkb: Optional[bytes], eid,
                               context: str) -> Optional[Dict[str, Any]]:
    """Refusal dict when *wkb* is a zero-length BLOB the event cannot mean.

    Before schema 7 the capture never wrote one: a NULL geometry was SQL
    NULL. A zero-length blob in such an event is a truncated journal row,
    and reading it as "the feature had no geometry" WIPED the live
    geometry of a restored feature, re-created features without one, and
    reported success (reason_code ``corrupt_wkb``, the refusal every other
    undecodable blob gets). A plain table ignores the geometric part of
    the event, as `_geometry_for_write` does: nothing to refuse there.
    """
    if wkb is None or len(wkb) != 0:
        return None
    if _layer_geometry_type(layer) == QgisCompat.GEOM_NULL:
        return None
    version = getattr(event, "event_schema_version", None)
    if is_null_geometry_marker(wkb, version):
        return None
    flog(f"{context}[{eid}]: reason_code=corrupt_wkb wkb_len=0 "
         f"event_schema_version={version} status=refused (a zero-length "
         f"geometry is the NULL marker only since schema "
         f"{NULL_MARKER_SINCE_SCHEMA}; in this event it is corruption)",
         "ERROR")
    return {
        "success": False,
        "reason_code": "corrupt_wkb",
        "message": ("Geometry unreadable (zero-length blob in a pre-v7 "
                    "event: corrupt WKB); nothing written"),
    }


def _has_shape(expected_geom) -> bool:
    """True when *expected_geom* is a geometry with coordinates to match."""
    return expected_geom is not None and expected_geom is not NO_GEOMETRY


def _expected_geometry(wkb: Optional[bytes], event: Optional[AuditEvent] = None,
                       layer=None):
    """Decoded captured geometry, in the layer CRS, or None if none to compare.

    The journal holds the coordinates of the capture CRS; the features
    live in the layer CRS. Same alignment as the write side
    (`_geometry_for_write`), so a layer reprojected since the capture does
    not make every proof fail -- or, worse, succeed on the wrong feature.

    Returns `NO_GEOMETRY` when the captured state is "no geometry": the
    NULL marker, or a typed EMPTY geometry (zero coordinates). A proof then
    requires the feature to carry no geometry either; it is evidence, but
    never a discriminating one (many features can lack a geometry). A
    plain table ignores the geometric part of the event, as the write side
    does, and an undecodable blob has nothing to compare (the write side
    refuses it) -- a zero-length blob in a pre-v7 event included: it is
    corruption, not the marker.
    """
    if wkb is None:
        return None
    if layer is not None and _layer_geometry_type(layer) == QgisCompat.GEOM_NULL:
        return None
    if event is not None:
        if _is_null_marker(wkb, getattr(event, "event_schema_version", None)):
            return NO_GEOMETRY
    elif _is_null_marker(wkb):
        return NO_GEOMETRY
    if len(wkb) == 0:
        return None
    geom = rebuild_geometry(wkb)
    if geom is None:
        return None
    try:
        if geom.isEmpty():
            return NO_GEOMETRY
    except (AttributeError, RuntimeError):
        return None
    if event is not None and layer is not None:
        aligned, _status = geometry_for_layer_crs(geom, event.crs_authid, layer)
        if aligned is None:
            return None
        geom = aligned
    return geom


class _Evidence(NamedTuple):
    """What one feature shares with a captured state."""

    geom: str            # "match", "absent" (neither has one), "mismatch",
    #                      or "none" (nothing to compare)
    equal: int           # captured non-NULL values found equal
    mismatched: tuple    # captured fields found different (NULL included)
    compared: int        # captured fields compared

    @property
    def full(self) -> bool:
        return self.geom != "mismatch" and not self.mismatched

    @property
    def pieces(self) -> int:
        return self.equal + (1 if self.geom == "match" else 0)


def _evidence(feature, attrs: Dict[str, Any], comparable: List[tuple],
              expected_geom, expected_wkb=None,
              version: Any = _NO_EVENT) -> _Evidence:
    """Compare *feature* with a captured state, field by field, NULL-aware.

    *version*: ``event_schema_version`` of the event the state comes from.
    """
    if expected_geom is None:
        geom = "none"
    elif expected_geom is NO_GEOMETRY:
        geom = ("mismatch" if is_geometry_present(feature.geometry())
                else "absent")
    elif feature_matches_geometry(feature, expected_geom,
                                  expected_wkb=expected_wkb):
        geom = "match"
    else:
        geom = "mismatch"
    equal = 0
    mismatched = []
    for name, idx in comparable:
        expected = attrs.get(name)
        if _vals_match(feature[idx], expected, version):
            if not _is_null(expected):
                equal += 1
        else:
            mismatched.append(name)
    return _Evidence(geom, equal, tuple(mismatched), len(comparable))


def _state_matches(layer, attrs: Dict[str, Any], comparable: List[tuple],
                   expected_geom, exclude_fids: Optional[set] = None,
                   limit: int = 2, what: str = "",
                   version: Any = _NO_EVENT) -> List[int]:
    """FIDs of the features carrying the WHOLE captured state (up to *limit*).

    Geometry (when given) AND every comparable field, NULLs included.
    `NO_GEOMETRY` has no box to filter on: every feature is read, with its
    geometry, since the proof is its absence.
    Raises TargetScanError when the read failed.
    """
    from qgis.core import QgsFeatureRequest

    request = QgsFeatureRequest()
    expected_wkb = None
    if _has_shape(expected_geom):
        request.setFilterRect(expected_geom.boundingBox())
        try:
            expected_wkb = bytes(expected_geom.asWkb())
        except (TypeError, ValueError):
            expected_wkb = None
    elif expected_geom is None:
        # Attributes only (BL-RW-P1-26): skip geometry materialisation and
        # read only the compared columns -- NULL-captured ones included,
        # since NULL must now match NULL.
        try:
            request.setFlags(QgisCompat.NO_GEOMETRY)
            request.setSubsetOfAttributes([idx for _name, idx in comparable])
        except (AttributeError, TypeError):
            pass
    matches: List[int] = []
    for feature in _scan(layer, request, what):
        if exclude_fids and feature.id() in exclude_fids:
            continue
        if _evidence(feature, attrs, comparable, expected_geom,
                     expected_wkb, version).full:
            matches.append(feature.id())
            if len(matches) >= limit:
                break
    return matches


def _non_null_count(attrs: Dict[str, Any], comparable: List[tuple]) -> int:
    return sum(1 for name, _idx in comparable if not _is_null(attrs.get(name)))


# Encoding drift tolerance of the attribute rescue: a re-encoded geometry
# keeps its place, give or take the precision of its coordinates (1e-7 of
# their magnitude) or a repair of its outline (1 % of its extent).
_DRIFT_RELATIVE = 1e-7
_DRIFT_OF_EXTENT = 0.01


def _geometry_near(layer, fid: int, expected_geom, what: str) -> bool:
    """True when feature *fid* sits where *expected_geom* is.

    Equal geometries (repair allowed: this checks ONE candidate), or
    bounding boxes within the encoding drift tolerance. A feature without
    a geometry is not near anything. Raises TargetScanError.
    """
    feature = _read_one(layer, fid, what)
    if feature is None or not is_geometry_present(feature.geometry()):
        return False
    if feature_matches_geometry(feature, expected_geom, allow_repair=True):
        return True
    try:
        live = feature.geometry().boundingBox()
        want = expected_geom.boundingBox()
        magnitude = max(abs(want.xMinimum()), abs(want.xMaximum()),
                        abs(want.yMinimum()), abs(want.yMaximum()), 1.0)
        tolerance = max(magnitude * _DRIFT_RELATIVE,
                        max(want.width(), want.height()) * _DRIFT_OF_EXTENT)
        return (abs(live.xMinimum() - want.xMinimum()) <= tolerance
                and abs(live.xMaximum() - want.xMaximum()) <= tolerance
                and abs(live.yMinimum() - want.yMinimum()) <= tolerance
                and abs(live.yMaximum() - want.yMaximum()) <= tolerance)
    except (AttributeError, RuntimeError, TypeError):
        return False


def _classify_insert_candidate(layer, fid: int, event: AuditEvent,
                               via: str,
                               baseline: Optional["_Baseline"] = None
                               ) -> Tuple[str, str]:
    """Level of *fid* as the feature an INSERT snapshot describes.

    ``(level, reason)``. STRONG/MEDIUM: may be deleted. WEAK: shares part
    of the snapshot and differs on the rest -- the feature the event
    inserted, edited since, or an unrelated one that happens to share a
    value; nobody can tell, so nobody deletes. UNRESOLVABLE with reason
    ``occupant_unrelated`` / ``occupant_absent``: not this feature.

    The rule this replaces (BL-RW-P1-25, RW-11 narrowed) deleted the
    occupant of a recycled FID as soon as it shared ONE non-null value
    with the snapshot: on a shapefile repacked since the insert, the
    feature now numbered like the inserted one was deleted because both
    sat in the same commune.

    A key nomination needs corroboration beyond the key, and NULL == NULL
    is none: a reused key now held by an empty row matched an unattributed
    snapshot field for field and the row was deleted as STRONG. Where the
    verdict decides a DELETE (*baseline* given: the undo of an INSERT), the
    key is STRONG when no OTHER feature carries the same evidence at the
    baseline (`_key_rarity`, the rule of `_prove_update_state`): the
    matched geometry with every captured value; else the COMBINATION of
    the captured values (with the captured absence of geometry, if any)
    when two or more are non-NULL -- counted, not assumed: two values the
    whole layer carries, and a snapshot without geometry, used to be
    enough; else the one value. A content that does not discriminate is
    left to the key's history (`_key_continuity`): STRONG when the journal
    proves the key never changed hands, WEAK otherwise.

    Without *baseline* the verdict only decides whether a key holder is
    ALREADY the restored feature (`_key_holder_verdict`, a skip that
    writes nothing): a holder carrying the whole snapshot IS that state,
    whoever else carries it, so the discriminating match of the snapshot
    (its geometry, or two captured values) or its one value, unique in the
    live layer, suffices. Raises TargetScanError when the read failed.
    """
    attrs = reconstruct_attributes(event)
    if not isinstance(attrs, dict):
        attrs = {}
    version = _event_version(event)
    comparable = _comparable_fields(layer, attrs,
                                    inserted=_from_edit_buffer(event))
    expected_geom = _expected_geometry(event.geometry_wkb, event, layer)
    feature = _read_one(layer, fid, f"occupant fid={fid}")
    if feature is None:
        return ID_UNRESOLVABLE, "occupant_absent"
    ev = _evidence(feature, attrs, comparable, expected_geom,
                   version=version)
    if ev.full:
        non_null = _non_null_count(attrs, comparable)
        discriminant = ev.geom == "match" or non_null >= 2
        if via == "pk" and baseline is not None:
            level, why = _insert_key_content(layer, fid, event, feature,
                                             attrs, comparable, expected_geom,
                                             ev, baseline)
            if level == ID_STRONG:
                return level, why
            continuous, history = _key_continuity(baseline, layer, event)
            if continuous:
                return ID_STRONG, f"{why} key_continuous ({history})"
            return ID_WEAK, f"{why} key_continuity_unproven ({history})"
        if via == "pk":
            if discriminant:
                return ID_STRONG, f"pk+snapshot:{ev.pieces}"
            if non_null == 1:
                name, idx = next((n, i) for n, i in comparable
                                 if not _is_null(attrs.get(n)))
                holders = _count_value_holders(
                    layer, idx, feature[idx], attrs.get(name),
                    f"key corroboration uniqueness {name}", version=version)
                if holders == 1:
                    return ID_STRONG, f"pk+snapshot value_unique:{name}"
                return ID_WEAK, f"pk+snapshot value_shared:{name}"
            return ID_WEAK, f"pk+snapshot nulls_only:{ev.compared}"
        if discriminant:
            return ID_MEDIUM, f"{via}+snapshot:{ev.pieces}"
        return ID_WEAK, f"{via}+snapshot_non_discriminant:{ev.pieces}"
    if ev.pieces:
        return ID_WEAK, (f"partial_evidence shared={ev.pieces} "
                         f"geom={ev.geom} differs={list(ev.mismatched[:4])}")
    return ID_UNRESOLVABLE, "occupant_unrelated"


def _insert_key_content(layer, fid: int, event: AuditEvent, feature,
                        attrs: Dict[str, Any], comparable: List[tuple],
                        expected_geom, ev: "_Evidence",
                        baseline: "_Baseline") -> Tuple[str, str]:
    """STRONG when no other feature carries the snapshot evidence at baseline.

    See `_classify_insert_candidate`; WEAK otherwise (the caller then asks
    the key's history). Raises TargetScanError.
    """
    version = _event_version(event)
    what = f"occupant key rarity fid={fid}"
    non_null = [(n, i) for n, i in comparable if not _is_null(attrs.get(n))]
    named = [(n, attrs.get(n)) for n, _i in comparable]
    if ev.geom == "match":
        others = _key_rarity(layer, fid, baseline, what, named, expected_geom,
                             version=version)
        found = "pk+snapshot geometry"
    elif len(non_null) >= 2:
        geom = NO_GEOMETRY if expected_geom is NO_GEOMETRY else None
        kind = None
        if geom is NO_GEOMETRY:
            kind = ("null" if _is_null_marker(
                event.geometry_wkb, getattr(event, "event_schema_version",
                                            None)) else "empty")
        name, idx = non_null[0]
        others = _key_rarity(layer, fid, baseline, what, named, geom,
                             absence_kind=kind,
                             filter_on=None if geom is not None
                             else (name, feature[idx]), version=version)
        found = "pk+snapshot combination"
    elif len(non_null) == 1:
        name, idx = non_null[0]
        others = _key_rarity(layer, fid, baseline, what,
                             [(name, attrs.get(name))],
                             filter_on=(name, feature[idx]), version=version)
        found = f"pk+snapshot value:{name}"
    else:
        return ID_WEAK, f"pk+snapshot nulls_only:{ev.compared}"
    if others is None:
        return ID_WEAK, f"{found} rarity_unmeasurable (baseline lost)"
    if others == 0:
        return ID_STRONG, f"{found} unique"
    return ID_WEAK, f"{found} shared"


def _resolve_insert_target(layer, event: AuditEvent,
                           fid_cache: Optional[Dict] = None,
                           exclude_fids: Optional[set] = None,
                           remapped: Optional[int] = None,
                           context: str = "",
                           baseline: Optional["_Baseline"] = None
                           ) -> TargetResolution:
    """Which feature an INSERT undo may delete (rewind, restore, undo).

    Order: the feature the identity designates, proven against the INSERT
    snapshot; for a FID identity only, a relocation by the whole snapshot
    (geometry AND every captured value, NULL-aware), then by the attributes
    alone when the geometry was re-encoded beyond recognition
    (BL-RW-P1-25). A primary key is never relocated by content: the key
    names the entity, and a feature with the same content under another
    key is another entity.

    *exclude_fids*: features the snapshot scans must not return (the ones
    this rewind re-inserted). *baseline*: where a key nomination measures
    rarity (`_Baseline`); by default the batch cache *fid_cache*, whose
    pre-images describe the layer before this operation wrote it.

    What this replaces could delete an unrelated feature four ways: a
    geometry-less full-layer scan that skipped every NULL (an unattributed
    snapshot matched the whole layer), a "lenient" branch that accepted a
    single same-outline candidate differing on its attributes, max(fid)
    to settle a tie, and a PK taken on trust with no look at the feature.

    The attribute rescue exists for a geometry RE-ENCODED (precision drift
    through OGR write cycles): the feature has not moved. So its unique
    candidate must still sit where the snapshot geometry is
    (`_geometry_near`); it used to be taken wherever it was, and an
    unrelated feature sharing two common values and an empty one ("borne",
    "LYON", no number) was deleted 90 units away from the inserted one.
    """
    eid = event.event_id
    identity = _event_identity(layer, event)
    has_pk = bool(_key_pairs(identity))
    attrs = reconstruct_attributes(event)
    if not isinstance(attrs, dict):
        attrs = {}
    version = _event_version(event)
    comparable = _comparable_fields(layer, attrs,
                                    inserted=_from_edit_buffer(event))
    expected_geom = _expected_geometry(event.geometry_wkb, event, layer)
    non_null = _non_null_count(attrs, comparable)
    discriminant = _has_shape(expected_geom) or non_null >= 2

    def _done(res: TargetResolution) -> TargetResolution:
        flog(f"{context} TARGET eid={eid} identity_level={res.level} "
             f"fid={res.fid} via={res.via or '-'} reason={res.reason} "
             f"absent={res.absent} nominated={res.nominated}",
             "INFO" if res.writable or res.absent else "WARNING")
        return res

    try:
        if remapped is not None:
            nominated, via = [remapped], "remap"
        else:
            nominated, via = _lookup_identity(layer, identity, fid_cache,
                                              baseline)
    except TargetScanError as exc:
        return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                      f"scan_failed: {exc}", via="identity"))
    if len(nominated) > 1:
        return _done(TargetResolution(None, ID_AMBIGUOUS, "key_not_unique",
                                      via=via, nominated=nominated[0]))
    nominated_fid = nominated[0] if nominated else None

    weak_reason = None
    occupant = "no_nominee"
    if baseline is None:
        baseline = _Baseline(cache=fid_cache)
    if nominated_fid is not None:
        try:
            level, why = _classify_insert_candidate(
                layer, nominated_fid, event, via, baseline)
        except TargetScanError as exc:
            return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                          f"scan_failed: {exc}", via=via,
                                          nominated=nominated_fid))
        if level in WRITABLE_LEVELS:
            return _done(TargetResolution(nominated_fid, level, why, via=via,
                                          nominated=nominated_fid))
        if level == ID_WEAK:
            weak_reason = why
        else:
            occupant = why

    if has_pk and remapped is None:
        if weak_reason:
            return _done(TargetResolution(None, ID_WEAK, weak_reason, via="pk",
                                          nominated=nominated_fid))
        pk_captured = all(name in attrs for name, _value in
                          _key_pairs(identity))
        if not pk_captured:
            # The snapshot does not carry the key, so a re-creation of the
            # feature cannot have kept it: "the key names nothing / names a
            # stranger" proves nothing about where the entity is now.
            return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                          "key_not_in_snapshot", via="pk",
                                          nominated=nominated_fid))
        # The key names the entity: nobody holds it any more, or it now
        # names a feature that shares nothing with the snapshot (reused).
        reason = "key_reused" if nominated_fid is not None else "key_absent"
        return _done(TargetResolution(None, ID_UNRESOLVABLE, reason,
                                      via="pk", absent=True,
                                      nominated=nominated_fid))

    what = f"{context} snapshot scan eid={eid}"
    try:
        matches = _state_matches(layer, attrs, comparable, expected_geom,
                                 exclude_fids=exclude_fids, what=what,
                                 version=version)
        found_via = "snapshot"
        if not matches and _has_shape(expected_geom) and non_null >= 2:
            matches = _state_matches(layer, attrs, comparable, None,
                                     exclude_fids=exclude_fids,
                                     what=f"{what} (attributes)",
                                     version=version)
            found_via = "attr_rescue"
            if len(matches) == 1 and not _geometry_near(
                    layer, matches[0], expected_geom,
                    f"{what} (rescue geometry)"):
                return _done(TargetResolution(
                    None, ID_WEAK,
                    f"attr_rescue_geometry_far: fid={matches[0]} carries the "
                    f"captured values but not where the snapshot geometry is",
                    via=found_via, nominated=nominated_fid))
    except TargetScanError as exc:
        return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                      f"scan_failed: {exc}", via="scan",
                                      nominated=nominated_fid))
    if len(matches) > 1:
        return _done(TargetResolution(None, ID_AMBIGUOUS,
                                      f"{found_via}_ambiguous:{matches[:5]}",
                                      via=found_via, nominated=nominated_fid))
    if len(matches) == 1:
        if discriminant:
            return _done(TargetResolution(matches[0], ID_MEDIUM,
                                          f"{found_via}_unique",
                                          via=found_via,
                                          nominated=nominated_fid))
        return _done(TargetResolution(None, ID_WEAK,
                                      f"non_discriminant_snapshot "
                                      f"(non_null={non_null}, no geometry "
                                      f"to match)",
                                      via=found_via, nominated=nominated_fid))
    if weak_reason:
        return _done(TargetResolution(None, ID_WEAK, weak_reason, via=via,
                                      nominated=nominated_fid))
    return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                  f"not_found ({occupant})",
                                  via=via, absent=True,
                                  nominated=nominated_fid))


def _unresolved_result(res: TargetResolution, action: str) -> Dict[str, Any]:
    """Result dict for a target that may not receive the write.

    ``scan_failed`` is a hard failure (the read proves nothing, not even
    absence), ``target_ambiguous`` and ``target_unverifiable`` are the
    named refusals the classifier keeps soft: nothing was written.
    """
    base = {"success": False, "identity_level": res.level}
    if res.reason.startswith("scan_failed"):
        base.update(reason_code="scan_failed", message=(
            f"Layer read failed while looking for the target "
            f"({res.reason}); nothing {action}: absence cannot be "
            f"inferred from a failed read"))
    elif res.level == ID_AMBIGUOUS:
        base.update(reason_code="target_ambiguous", message=(
            f"Target is ambiguous ({res.reason}); refusing to pick one, "
            f"nothing {action}"))
    else:
        base.update(reason_code="target_unverifiable", message=(
            f"Target feature could not be verified (identity_level="
            f"{res.level}: {res.reason}); refusing, nothing {action}"))
    return base


def _find_restore_duplicate(layer, event: AuditEvent) -> Optional[int]:
    """FID of a feature already carrying the whole snapshot of *event*.

    Idempotence proof for the re-insertion of a deleted feature. The key
    holder check of `_reinsert_feature` cannot fire on a FID-only layer (a
    shapefile captures no key), and a replay there added one copy per
    click, each answering "Restored".

    Proof is deliberately strict -- geometry AND every captured non-null
    attribute -- and returns None as soon as there is nothing solid to
    compare (unreadable geometry, no comparable attribute). Claiming
    "already there" wrongly would leave the feature missing while
    reporting success, so absence of evidence must never be read as
    evidence of presence.

    Geometry alone is NOT that proof. When the comparable list is empty
    (every captured value null, or nothing left but the PK and the audit
    columns), ``all()`` over it is vacuously true and any feature sharing
    the geometry answered "already restored": a borne re-created at the
    same coordinates with different attributes, a parcel re-drawn on the
    same outline, a network node duplicated on purpose. The re-insertion
    was then refused AND reported as a success -- the feature stayed
    missing while the user read "Already exists". At least one attribute
    must corroborate the geometry.

    v7: the comparison is NULL-aware (a captured empty cell must be empty
    on the candidate too, otherwise a feature carrying MORE than the
    snapshot answered "already restored"), the geometry is compared in the
    layer CRS the restore writes in, and a failed read raises
    TargetScanError instead of answering "no duplicate" -- which re-inserted
    a copy whenever the read failed.

    A captured "no geometry" (NULL marker, typed EMPTY geometry) is a state
    like any other: the candidate must carry no geometry either. It used to
    end the proof ("nothing solid to compare"), so every replay of such a
    restore added one more geometry-less copy.

    The key columns are compared too, single or composite. They are the
    entity: the re-insert writes them back as captured, and a feature
    carrying the same name and outline under ANOTHER key -- the twin
    parcel (12,8) of (12,7), the twin fid 7 of the GeoPackage fid 3 -- is
    another feature. Left out, they let the twin answer "Already exists"
    (a success, counted restored by `restore_batch`, no trace) while the
    feature never came back. They were left out on the premise that "the
    re-insert receives a FRESH fid": false on QGIS 4.2.1, where the
    provider keeps the key written, and `_readback_written` fails any
    re-insert whose key it did not keep. So every key column the identity
    names is evidence, and so is a key column of the layer the snapshot
    carries non-NULL (an identity journalled by its FID alone, on a
    GeoPackage): both are written back as captured. A NULL or
    provider-generated key is not written back, and stays out.
    """
    wkb = event.geometry_wkb
    expected_geom = _expected_geometry(wkb, event, layer)
    spatial = _layer_geometry_type(layer) != QgisCompat.GEOM_NULL
    if spatial and wkb is not None and expected_geom is None:
        # Undecodable or impossible to align: the write side refuses it.
        return None

    attrs = reconstruct_attributes(event)
    if not isinstance(attrs, dict):
        attrs = {}
    comparable = _comparable_fields(layer, attrs,
                                    keep_keys=_written_key_columns(
                                        layer, event, attrs))
    if not _non_null_count(attrs, comparable):
        flog(f"_find_restore_duplicate: eid={event.event_id} "
             f"no comparable attribute (captured={len(attrs)}); geometry "
             f"alone cannot prove the feature is already back", "DEBUG")
        return None

    matches = _state_matches(
        layer, attrs, comparable, expected_geom, limit=1,
        what=f"duplicate scan eid={event.event_id}",
        version=_event_version(event))
    return matches[0] if matches else None


def _written_key_columns(layer, event: AuditEvent,
                         attrs: Dict[str, Any]) -> List[str]:
    """Key columns a re-insert of *attrs* writes back (`_find_restore_duplicate`).

    Every column of the key the identity names, and every key column of
    the layer the snapshot carries non-NULL (not a `QgsUnsetAttributeValue`
    repr).
    """
    keys = [name for name, _value in _key_pairs(_event_identity(layer, event))]
    try:
        provider = layer.dataProvider()
        fields = provider.fields()
        pk_names = [fields.at(idx).name()
                    for idx in (provider.pkAttributeIndexes() or [])
                    if 0 <= idx < fields.count()]
    except (AttributeError, RuntimeError):
        pk_names = []
    for name in pk_names:
        value = attrs.get(name)
        if name not in keys and not _is_null(value) \
                and not _is_unset_marker(value):
            keys.append(name)
    return keys


def _geometry_for_write(layer, event: AuditEvent, wkb: Optional[bytes],
                        eid: int, context: str) -> tuple:
    """Rebuild the geometry of *event* and make it safe to write on *layer*.

    Returns ``(geom, error)``:
      - ``(None, None)``  nothing to write: the event carries no geometry,
        or the target layer is a plain table (the geometric part of the
        event is then inert, the row is still restored);
      - ``(geom, None)``  the geometry may be written as-is. For the NULL
        marker (`core.constants.is_null_geometry_marker`, read with the
        event's ``event_schema_version``: the feature had NO geometry on
        that side of the edit) *geom* is a null ``QgsGeometry``: the write
        removes the geometry, and the read-back checks that none is left.
        A zero-length blob of a pre-v7 event is no marker but a truncated
        row, refused ``corrupt_wkb`` here whatever the caller checked.
        A typed EMPTY geometry is a state too, written as captured;
      - ``(None, error)`` a ready-to-return failure dict: the geometry
        must NOT be written.

    Three refusals, ordered by what they cost the user. The rewind path
    (`restore_executor._buffer_update`) already knows the first one; the
    event-by-event path accepted what the rewind refused.

      1. decode - a truncated or corrupted blob rebuilds to nothing.
         Writing it wipes the geometry of the live feature, which is the
         worst outcome for a recovery tool (reason_code ``corrupt_wkb``,
         the rewind's refusal too).
      2. CRS - the WKB holds the RAW provider coordinates at capture time;
         ``event.crs_authid`` is only the CRS the layer declared then. A
         different CRS now is a relabel (write the raw coordinates) or a
         reprojection of the file (reproject them): the rewind's helper
         (`geometry_utils.geometry_for_layer_crs`) decides from evidence,
         so both engines agree, and refuses rather than guess
         (reason_code ``crs_ambiguous``), or when the capture CRS is not
         usable here (reason_code ``crs_mismatch``). The event's other
         geometries go along as witnesses: the NEW geometry of an UPDATE
         is the one its target still carries, so the target tells the
         story for the OLD one written here -- the story its identity
         proof (`_expected_geometry`, same helper) was checked under. The
         refusal is read on the STATUS: with ``crs_ambiguous`` the helper
         hands a reading meant for comparisons, never for a write.
      3. geometry type - a Point must not be written into a Polygon
         layer: depending on the driver the layer silently becomes
         heterogeneous or the write fails with a raw OGR message
         (reason_code ``geometry_type_mismatch``).
    """
    if wkb is None:
        return None, None

    layer_geom_type = _layer_geometry_type(layer)
    if layer_geom_type == QgisCompat.GEOM_NULL:
        flog(f"{context}[{eid}]: non-spatial layer, event geometry ignored")
        return None, None

    # The event's own schema version decides what a zero-length BLOB is:
    # the NULL marker since schema 7, a truncated row before. Read without
    # it, a 5.x blob was the marker and the live geometry was wiped, unless
    # every caller remembered `_legacy_empty_blob_refusal` first; now such
    # a blob falls through to the decode below and is refused corrupt_wkb.
    if is_null_geometry_marker(wkb, getattr(event, "event_schema_version",
                                            None)):
        from qgis.core import QgsGeometry
        flog(f"{context}[{eid}]: geometry side is the NULL marker (the "
             f"feature had none): writing a NULL geometry")
        return QgsGeometry(), None

    geom = rebuild_geometry(wkb)
    if geom is None:
        flog(f"{context}[{eid}]: reason_code=corrupt_wkb wkb_len={len(wkb)} "
             f"status=refused (writing it would destroy the live geometry)",
             "ERROR")
        return None, {
            "success": False,
            "reason_code": "corrupt_wkb",
            "message": "Geometry unreadable (corrupt WKB); nothing written",
        }

    try:
        empty = bool(geom.isEmpty())
    except (AttributeError, RuntimeError):
        empty = False
    # An EMPTY geometry has no coordinate a CRS change could misplace, and
    # the transform code does not promise to carry one through.
    from .geometry_utils import (
        CRS_MISMATCH_STATUSES, CRS_REFUSED_STATUSES, crs_refusal,
    )
    aligned, crs_status = ((geom, "empty") if empty else
                           geometry_for_layer_crs(
                               geom, event.crs_authid, layer,
                               witnesses=(event.geometry_wkb,
                                          getattr(event, "new_geometry_wkb",
                                                  None))))
    if crs_status in CRS_MISMATCH_STATUSES:
        flog(f"{context}[{eid}]: crs_mismatch event_crs={event.crs_authid} "
             f"layer_crs={extract_crs_authid(layer)} action={crs_status}",
             "WARNING")
    if aligned is None or crs_status in CRS_REFUSED_STATUSES:
        reason_code, message = crs_refusal(crs_status, event.crs_authid, layer)
        flog(f"{context}[{eid}]: reason_code={reason_code} status=refused "
             f"(nothing written)", "ERROR")
        return None, {
            "success": False,
            "reason_code": reason_code,
            "message": message,
        }
    geom = aligned

    type_error = _geometry_type_refusal(layer, geom, event, eid, context)
    if type_error is not None:
        return None, type_error

    return geom, None


def _geometry_type_refusal(layer, geom, event: AuditEvent, eid,
                           context: str) -> Optional[Dict[str, Any]]:
    """Refusal dict when *geom* is not of the layer's geometry type, else None.

    Shared by every write path (event restore, rewind re-insert, undo): a
    Point must not be written into a Polygon layer. Depending on the driver
    the layer silently becomes heterogeneous or the write fails with a raw
    OGR message (reason_code ``geometry_type_mismatch``).
    """
    layer_geom_type = _layer_geometry_type(layer)
    if layer_geom_type in (QgisCompat.GEOM_UNKNOWN, QgisCompat.GEOM_NULL):
        return None
    try:
        geom_type = geom.type()
    except (AttributeError, RuntimeError):
        return None
    if geom_type == layer_geom_type:
        return None
    flog(f"{context}[{eid}]: reason_code=geometry_type_mismatch "
         f"event_geom_type={event.geometry_type} "
         f"layer_geom_type={int(layer_geom_type)} status=refused",
         "ERROR")
    return {
        "success": False,
        "reason_code": "geometry_type_mismatch",
        "message": (
            f"Geometry of the event ({event.geometry_type}) is "
            f"incompatible with the layer; nothing written"
        ),
    }


# -----------------------------------------------------------------------
# Provider read-back (v7)
#
# The event restore and the undo write THROUGH the provider, one call per
# feature, outside any edit buffer: nothing the commit guard of the rewind
# (core.commit_guard) sees. Their outcome used to be the boolean the QGIS
# API returned, and that boolean proves only that the provider raised no
# error: a trigger rewrites the value, a narrowed column truncates it, a
# second call fails after the first one landed. So every provider-direct
# write reads the feature back from a FRESH layer on the same source (the
# file, not a cache) and compares it with what was asked, with the same
# comparators as the commit guard. A contradiction is its own outcome,
# ``failed_postcommit_verification`` -- never a success.
# -----------------------------------------------------------------------

_SERVER_PROVIDERS = ("postgres", "mssql", "oracle", "hana")


def _readback_source(layer):
    """What a read-back reads: the stored state, not a cache.

    A file (OGR, SpatiaLite) is read back through a FRESH layer on the same
    source -- a separate handle, the commit guard's idea -- so what is
    compared is what the file holds. Two exceptions, both read through the
    layer's own provider:

      * a memory layer: its "source" string would open a NEW, empty layer
        and every read-back would find nothing;
      * a database server: its provider queries the server on every read
        already, while building a fresh layer costs metadata round-trips
        per restored event.
    """
    provider = layer.dataProvider()
    try:
        name = provider.name()
    except (AttributeError, RuntimeError):
        name = ""
    if name == "memory" or name in _SERVER_PROVIDERS:
        return provider
    from .commit_guard import _fresh_layer
    return _fresh_layer(layer) or provider


def _auto_field_names(layer) -> set:
    """Columns the provider fills itself (primary key, default clause)."""
    out = set()
    try:
        provider = layer.dataProvider()
        fields = provider.fields()
        for idx in provider.pkAttributeIndexes():
            if 0 <= idx < fields.count():
                out.add(fields.at(idx).name())
        for idx in range(fields.count()):
            if provider.defaultValueClause(idx):
                out.add(fields.at(idx).name())
    except (AttributeError, RuntimeError):
        pass
    return out


def _state_diffs(feature, intended: Dict[str, Any], geom,
                 auto_fields=()) -> List[str]:
    """What *feature* holds that differs from what was written on it."""
    from .commit_guard import values_equal, geometry_equal

    diffs = []
    for name, value in intended.items():
        if name in auto_fields and _is_null(value):
            continue
        try:
            actual = feature[name]
        except (KeyError, IndexError):
            diffs.append(f"{name}: column missing")
            continue
        if not values_equal(actual, value):
            diffs.append(f"{name}: {actual!r} != {value!r}")
    if geom is not None and not geometry_equal(feature.geometry(), geom):
        diffs.append("geometry differs")
    return diffs


def _readback_written(layer, fid, intended: Dict[str, Any], geom,
                      what: str) -> List[str]:
    """Read *fid* back from the file and diff it against what was written."""
    if fid is None or fid < 0:
        return [f"the provider returned no usable id ({fid}) for the write"]
    source = _readback_source(layer)
    try:
        feature = _read_one(source, fid, what)
    except TargetScanError as exc:
        return [f"read-back failed ({exc})"]
    if feature is None:
        return [f"fid {fid} not found by the read-back"]
    return _state_diffs(feature, intended, geom, _auto_field_names(layer))


def _count_state_holders(source, attrs: Dict[str, Any], geom,
                         what: str, provider_only: bool = False) -> int:
    """Features of *source* carrying exactly *attrs* and *geom*."""
    from qgis.core import QgsFeatureRequest
    from .commit_guard import values_equal, geometry_equal

    request = QgsFeatureRequest()
    if is_geometry_present(geom):
        request.setFilterRect(geom.boundingBox())
    count = 0
    for feature in _scan(source, request, what, provider_only=provider_only):
        if not geometry_equal(feature.geometry(), geom):
            continue
        if all(values_equal(feature[name], value)
               for name, value in attrs.items()):
            count += 1
    return count


def _provider_delete(layer, res: "TargetResolution", event: AuditEvent,
                     context: str,
                     cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Delete the resolved target through the provider, then read back.

    Where FIDs are stable the read-back asks for the FID. Where they are
    positional (shapefile, GeoJSON) the provider may renumber the file
    right after the delete, so the read-back counts the features carrying
    the deleted state instead: exactly one fewer than before.

    The pre-image is read from the PROVIDER, so it is indexed by the
    provider's own (stored) columns. Indexed by ``layer.fields()`` it asked
    for the virtual (expression) and joined fields a provider feature does
    not have: KeyError before anything was deleted, and every INSERT undo
    on a layer with a calculated or joined field failed with a bare
    "'nom_maj'". It is also recorded in the batch *cache* before the
    delete (`_record_pre_image`): the deleted feature still belongs to the
    layer the user left, which the next rarity counts measure.
    """
    from qgis.core import QgsGeometry
    from .commit_guard import fid_is_stable

    provider = layer.dataProvider()
    fid = res.fid
    eid = event.event_id or 0
    stable = fid_is_stable(layer)
    names = [f.name() for f in provider.fields()]
    try:
        pre = _read_one(layer, fid, f"{context} pre-image fid={fid}",
                        provider_only=True)
        if pre is None:
            return {"success": False, "reason_code": "target_absent",
                    "identity_level": res.level,
                    "message": "Target vanished before the delete; "
                               "nothing deleted"}
        pre_attrs = {name: pre[name] for name in names}
        pre_geom = QgsGeometry(pre.geometry())
        holders_before = None if stable else _count_state_holders(
            layer, pre_attrs, pre_geom, f"{context} pre-count",
            provider_only=True)
    except TargetScanError as exc:
        flog(f"{context}: pre-image read failed fid={fid}: {exc}", "ERROR")
        return {"success": False, "reason_code": "scan_failed",
                "identity_level": res.level,
                "message": f"Layer read failed before the delete ({exc}); "
                           f"nothing deleted"}

    _record_pre_image(cache, fid, pre)
    _record_delete(cache, layer)
    if not provider.deleteFeatures([fid]):
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Delete failed"
        flog(f"{context}: deleteFeatures failed fid={fid} msg={msg}", "ERROR")
        return {"success": False, "reason_code": "provider_refused",
                "identity_level": res.level, "message": msg}

    problem = None
    landed = False
    source = _readback_source(layer)
    try:
        if stable:
            if _read_one(source, fid, f"{context} read-back") is not None:
                problem = "the feature is still in the layer"
        else:
            after = _count_state_holders(source, pre_attrs, pre_geom,
                                         f"{context} read-back")
            if after != holders_before - 1:
                problem = (f"{after} feature(s) still carry the deleted "
                           f"state, expected {holders_before - 1}")
    except TargetScanError as exc:
        problem = f"read-back failed ({exc})"
        landed = True
    if problem:
        flog(f"{context}: status=FAILED reason_code="
             f"failed_postcommit_verification eid={eid} fid={fid} "
             f"detail={problem!r}", "ERROR")
        return {"success": False,
                "reason_code": "failed_postcommit_verification",
                "identity_level": res.level, "fid": fid, "landed": landed,
                "message": (f"Delete accepted by the provider but not "
                            f"verified: {problem}")}
    flog(f"{context}: OK deleted fid={fid} identity_level={res.level} "
         f"via={res.via} verified_by={'fid' if stable else 'content'}")
    return {"success": True, "message": "Deleted (undo insert)",
            "fid": fid, "identity_level": res.level}


def _key_holder_verdict(layer, holders: List[int], event: AuditEvent,
                        context: str) -> Dict[str, Any]:
    """Result when the captured key is already held by a feature.

    "Some row has this key" used to be read as "the deleted feature is
    back": on a table whose key is reused (INTEGER PRIMARY KEY without
    AUTOINCREMENT hands the highest id out again), restoring a deleted
    feature answered "Already exists" while an unrelated feature held its
    key -- the deleted feature never came back, and the trace neutralised
    its DELETE for every later rewind. The holder must carry the deleted
    snapshot; otherwise the key is taken, and re-inserting under it would
    either fail on the constraint or duplicate the key: refused, named.
    """
    key = _key_repr(_event_identity(layer, event))
    if len(holders) > 1:
        flog(f"{context}: key {key} held by {holders[:5]} status=refused "
             f"reason_code=target_ambiguous", "ERROR")
        return {"success": False, "reason_code": "target_ambiguous",
                "identity_level": ID_AMBIGUOUS,
                "message": f"Key {key} is held by several features; "
                           f"nothing inserted"}
    level, why = _classify_insert_candidate(layer, holders[0], event, "pk")
    if level == ID_STRONG:
        flog(f"{context}: skip, {key} already exists fid={holders[0]} "
             f"identity_level={level} ({why})")
        return {"success": True, "message": "Already exists (skip)",
                "skipped": True, "fid": holders[0], "identity_level": level}
    flog(f"{context}: key {key} held by fid={holders[0]} which does not "
         f"carry the deleted feature ({level}: {why}) status=refused "
         f"reason_code=pk_conflict", "ERROR")
    return {"success": False, "reason_code": "pk_conflict",
            "identity_level": level, "fid": holders[0],
            "message": (f"Key {key} is held by another feature (fid "
                        f"{holders[0]}: {why}); the key was reused or the "
                        f"feature edited since. Nothing inserted")}


def _reinsert_feature(layer, event: AuditEvent, context: str,
                      write_op: Optional[str], wkb: Optional[bytes],
                      done_message: str,
                      cache: Optional[Dict] = None,
                      compensation_guard: bool = False) -> Dict[str, Any]:
    """Re-insert the feature *event* captured, idempotently, then read back.

    Shared by the restore of a DELETE and the undo of an INSERT restore:
    both put back a feature from its snapshot, so both get the same
    guards -- pre-check (capability, schema drift), key holder proof,
    duplicate proof, decode / CRS / geometry type, read-back.

    *compensation_guard*: the restore of a DELETE (Mode A) asks the
    journal, once the feature is known to be missing, whether a trace
    already compensates the event (`_compensation_refusal`). The undo
    never does: the compensation is exactly what it takes back.

    The key holder proof covers a composite key too (all its columns):
    without it, a key already given to another parcel was duplicated.

    The feature is built on the PROVIDER's fields: a provider write takes
    the stored columns, not the layer's virtual or joined ones. A captured
    `QgsUnsetAttributeValue` repr is not written back as text: the column
    is left to its database default, and the result says so.

    The feature it creates is recorded in the batch *cache* as absent from
    the layer the user left (`_ADDED`): it is no holder of anything in the
    rarity counts of the events that follow.
    """
    eid = event.event_id or 0
    check = pre_check_restore(layer, event, write_op=write_op)
    if not check.can_restore:
        flog(f"{context}: pre_check failed: {check.reason}", "WARNING")
        return {"success": False, "message": check.reason}

    identity = _event_identity(layer, event)
    try:
        if _key_pairs(identity):
            holders, _via = _lookup_identity(layer, identity)
            if holders:
                return _key_holder_verdict(layer, holders, event, context)
        # Second line of idempotence: replaying a restore must not add a
        # copy. Double-click, relaunch after a partial rewind, two users on
        # the same GPKG, resume after a crash -- replays are routine, and
        # the key guard above is unreachable on a FID-only layer.
        duplicate_fid = _find_restore_duplicate(layer, event)
    except TargetScanError as exc:
        flog(f"{context}: status=FAILED reason_code=scan_failed {exc}",
             "ERROR")
        return {"success": False, "reason_code": "scan_failed",
                "message": (f"Layer read failed while checking whether the "
                            f"feature is already there ({exc}); nothing "
                            f"inserted")}
    if duplicate_fid is not None:
        flog(f"{context}: skip, snapshot already present "
             f"fid={duplicate_fid} (replay of an applied restore)")
        return {"success": True, "skipped": True, "fid": duplicate_fid,
                "message": "Already exists (skip): feature already restored"}
    if compensation_guard:
        refusal = _compensation_refusal(cache, event, context)
        if refusal is not None:
            return refusal

    attrs = reconstruct_attributes(event)
    if not isinstance(attrs, dict):
        attrs = {}
    blob_error = _legacy_empty_blob_refusal(layer, event, wkb, eid,
                                            context.split("[")[0])
    if blob_error is not None:
        return blob_error
    geom, geom_error = _geometry_for_write(layer, event, wkb, eid,
                                           context.split("[")[0])
    if geom_error is not None:
        return geom_error
    field_mapping = _build_safe_mapping(check.drift, event)
    geom_repr = ("no" if geom is None
                 else "null" if geom.isNull() else "yes")
    flog(f"{context}: attrs={len(attrs)} keys, geom={geom_repr}, "
         f"mapping={len(field_mapping)} fields")

    unset = sorted(name for name, value in attrs.items()
                   if _is_unset_marker(value))
    if unset:
        flog(f"{context}: captured value(s) {unset} are QgsUnsetAttributeValue "
             f"reprs, not stored values: left to the database default",
             "WARNING")
        attrs = {name: value for name, value in attrs.items()
                 if name not in unset}

    from qgis.core import QgsFeature
    provider = layer.dataProvider()
    fields = provider.fields()
    specs = _time_specs(layer, _captured_from(event, "snapshot"))
    refusal = _legacy_datetime_refusal(field_mapping, attrs, fields, event,
                                       specs, context)
    if refusal is not None:
        return refusal
    new_feature = QgsFeature(fields)
    intended = {}
    # Decoded with the event's schema version and, for a <= 5.x date-time,
    # the spec the provider hands back (see `_build_attribute_changes`).
    for idx, value in iter_mapped_attributes(field_mapping, attrs, fields,
                                             event.event_schema_version,
                                             specs):
        new_feature.setAttribute(idx, value)
        intended[fields.at(idx).name()] = value
    if geom is not None:
        new_feature.setGeometry(geom)
    if unset:
        done_message = (f"{done_message} (not journalled, left to the "
                        f"database default: {', '.join(unset)})")

    success, added = provider.addFeatures([new_feature])
    if not success:
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Insert failed"
        flog(f"{context}: addFeatures failed: {msg}", "ERROR")
        return {"success": False, "reason_code": "provider_refused",
                "message": msg}

    new_fid = added[0].id() if added else None
    _record_pre_image(cache, new_fid, _ADDED)
    diffs = _readback_written(layer, new_fid, intended, geom,
                              f"{context} read-back")
    if diffs:
        flog(f"{context}: status=FAILED reason_code="
             f"failed_postcommit_verification new_fid={new_fid} "
             f"diffs={diffs[:5]}", "ERROR")
        return {"success": False,
                "reason_code": "failed_postcommit_verification",
                "landed": True, "fid": new_fid,
                "message": (f"Inserted, but the provider holds something "
                            f"else: {'; '.join(diffs[:4])}")}
    flog(f"{context}: OK new_fid={new_fid} (read back)")
    return {"success": True, "message": done_message, "fid": new_fid}


def restore_deleted_feature(layer, event: AuditEvent,
                            fid_cache: Optional[Dict] = None,
                            journal=None) -> Dict[str, Any]:
    """Re-insert a deleted feature into the target layer.

    Returns dict with 'success', 'message', and optionally 'fid'.
    *fid_cache*: the batch cache, which records the feature created.

    *journal*: a read connection to the journal *event* comes from, the
    same keyword as every other entry point that decides an identity.
    With it, a DELETE a trace already compensates (restored, deleted
    again, restored again) is refused ``already_compensated`` instead of
    re-inserting the feature under a trace that names the first delete
    (`_compensation_refusal`). It cannot vouch for the key of the deleted
    feature: the DELETE itself released it (`_journal_key_continuity`),
    and the key holder proof stays content-based. ``None`` (the default):
    a second restore of a compensated DELETE is not detected.
    """
    fid_cache = _operation_cache(fid_cache, journal)
    eid = event.event_id or 0
    dirty = _dirty_session_refusal(
        validate_restore_layer_state(layer), f"restore_deleted[{eid}]")
    if dirty is not None:
        return dirty
    return _reinsert_feature(layer, event, f"restore_deleted[{eid}]",
                             None, event.geometry_wkb, "Restored",
                             cache=fid_cache, compensation_guard=True)


def restore_inserted_feature(layer, event: AuditEvent,
                             fid_cache: Optional[Dict] = None,
                             journal=None) -> Dict[str, Any]:
    """Undo an INSERT by deleting the inserted feature from the target layer.

    Also the undo of a DELETE restore (the feature the restore re-inserted
    is deleted again). The target is resolved by `_resolve_insert_target`:
    only a STRONG or MEDIUM target is deleted, and the delete is read back.
    Everything else is a named refusal, nothing deleted: a weak or
    ambiguous candidate, a read that failed (``scan_failed``), and a
    feature the completed reads did not find (``target_absent``).

    That last one used to answer success, "Already absent (no snapshot
    match)", counted as done and journalled as a restore trace. But "not
    found" is not "not there": every captured value and the geometry may
    have been edited since the insert, so the feature can still be in the
    layer under a content no proof recognises. The user asked for a delete
    and nothing was deleted; the result must say so, and no trace may
    neutralise the INSERT for the rewinds to come.

    *journal*: a read connection to the journal *event* comes from, the
    evidence that a key never changed hands (`_key_continuity`) and that
    no trace already compensates the INSERT (`_compensation_refusal`:
    inserted, restored, inserted again, restored again deletes the SECOND
    insert under a trace naming the first; refused ``already_compensated``).
    ``None`` (the default): neither is available, the key needs a rare
    content and a second restore is not detected.
    """
    return _delete_inserted_feature(layer, event,
                                    _operation_cache(fid_cache, journal),
                                    compensation_guard=True)


def _delete_inserted_feature(layer, event: AuditEvent, fid_cache: Dict,
                             compensation_guard: bool) -> Dict[str, Any]:
    """`restore_inserted_feature`, and the undo of a DELETE restore.

    *compensation_guard*: asked by the Mode A restore only; the undo takes
    back exactly the compensation the guard looks for.
    """
    if layer is None:
        return {"success": False, "message": "Target layer not found"}

    eid = event.event_id or 0
    context = f"restore_inserted[{eid}]"
    dirty = _dirty_session_refusal(
        validate_restore_layer_state(layer), context)
    if dirty is not None:
        return dirty

    provider = layer.dataProvider()
    if provider is None:
        return {"success": False, "message": "No data provider"}

    if not bool(provider.capabilities() & QgisCompat.CAP_DELETE_FEATURES):
        return {"success": False, "message": "Provider lacks delete capability"}

    strength = get_identity_strength_for_layer(layer)
    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity for restore"}

    res = _resolve_insert_target(layer, event, fid_cache, context=context)
    if res.writable:
        if compensation_guard:
            refusal = _compensation_refusal(fid_cache, event, context)
            if refusal is not None:
                refusal["identity_level"] = res.level
                return refusal
        return _provider_delete(layer, res, event, context, cache=fid_cache)
    if res.absent:
        flog(f"{context}: status=refused reason_code=target_absent "
             f"identity_level={res.level} reason={res.reason} (reads "
             f"completed, the inserted feature was not found: nothing "
             f"deleted)", "WARNING")
        _diagnose_snapshot_miss(layer, event)
        return {"success": False,
                "reason_code": "target_absent",
                "identity_level": res.level,
                "message": (f"Inserted feature not found in the layer "
                            f"({res.reason}); nothing deleted")}
    flog(f"{context}: status=refused identity_level={res.level} "
         f"reason={res.reason}", "ERROR")
    return _unresolved_result(res, "deleted")


def _update_side(event: AuditEvent, side: str) -> Tuple[Dict[str, Any], Optional[bytes]]:
    """``(attributes, wkb)`` of one side of an UPDATE: ``new`` or ``old``.

    The attributes are the CHANGED fields only (``changed_only`` delta):
    their post-edit values for ``new``, their pre-edit values for ``old``.
    """
    if side == "new":
        attrs = reconstruct_new_attributes(event)
        wkb = getattr(event, "new_geometry_wkb", None)
    else:
        attrs = reconstruct_attributes(event)
        wkb = getattr(event, "geometry_wkb", None)
    return (attrs if isinstance(attrs, dict) else {}), wkb


def _absence_kind(geometry) -> str:
    """``null`` (no geometry at all) or ``empty`` (a typed EMPTY one)."""
    if geometry is None or geometry.isNull():
        return "null"
    return "empty"


# -----------------------------------------------------------------------
# Key nominations: rarity on the layer as the user left it, continuity
# from the journal (v7)
#
# A key designates a feature; its content must corroborate it, and a value
# corroborates only when it is RARE: how many OTHER features carry it tells
# how plausible it is that a feature which took the key over carries the
# same by chance. That count is measured on the layer as the user left it
# (the BASELINE), never on the layer this operation is rewriting: the
# features it already wrote carry partly OUR state, the ones it deleted
# are gone, the ones it re-created were not there. Measured on the
# rewritten layer the verdict depended on the order of the events: a batch
# that first wrote the CODE of feature 5, whose NAME V is the user's, then
# restored "nom -> V" on the reused key 3 found V "unique" and renamed
# INTRUS; a batch that first deleted D (name V) did the same. Excluding the
# written features WHOLE (the previous rule) dropped the user's own columns
# with ours.
#
#   * the rewind writes in the edit buffer: the provider still holds the
#     committed state, which is the baseline (the runners refuse a layer
#     with uncommitted user edits before the rewind opens its buffer);
#   * the event restore and the undo write through the provider, so every
#     write and delete first records the PRE-IMAGE of its feature in the
#     batch cache (`_record_pre_image`), and a re-insert records the
#     feature it created as absent from the baseline (`_ADDED`). The
#     baseline is the live layer with those substitutions.
#
# What rarity cannot tell -- a shared boolean on a key that never changed
# hands and one on a key given to INTRUS are the same layer byte for byte --
# the journal can: a key changes hands only after its feature was deleted
# or its key edited, and both are journalled (`_key_continuity`).
#
# The cache entries below are 1-tuples, never a ``(pk_field, value)`` pair:
# no identity lookup can hit them.
# -----------------------------------------------------------------------

_PRE_IMAGES_KEY = ("__pre_operation_images__",)
_BASELINE_LOST_KEY = ("__pre_operation_images_lost__",)
_JOURNAL_KEY = ("__journal__",)
_CONTINUITY_KEY = ("__key_continuity__",)


class _Added:
    """Pre-image of a feature this operation created: not in the baseline."""

    def __repr__(self) -> str:
        return "<added by this operation>"


_ADDED = _Added()
_UNREADABLE = object()


class _Baseline(NamedTuple):
    """Where a key nomination reads the layer as the user left it.

    *provider_only*: the rewind (the committed state is the baseline).
    *cache*: the batch cache (pre-images, journal, continuity verdicts).
    """

    provider_only: bool = False
    cache: Optional[Dict] = None


def _operation_cache(fid_cache: Optional[Dict], journal=None) -> Dict:
    """The batch cache *fid_cache* (created when None), holding *journal*."""
    cache = fid_cache if fid_cache is not None else {}
    if journal is not None:
        cache[_JOURNAL_KEY] = journal
    return cache


def _record_pre_image(cache: Optional[Dict], fid, image) -> None:
    """Remember what feature *fid* was before this operation first wrote it.

    *image* is the feature read from the provider before the write, or
    `_ADDED` for a feature the operation created. The FIRST record wins:
    that is the baseline. Where FIDs are positional (shapefile), a delete
    renumbers the later features and a FID no longer names its pre-image:
    `_record_delete` then marks the baseline lost, and a rarity that needs
    it is not measured (`_pre_images` answers None).
    """
    if cache is None or fid is None:
        return
    images = cache.setdefault(_PRE_IMAGES_KEY, {})
    if image is not _ADDED:
        from qgis.core import QgsFeature
        image = QgsFeature(image)
    images.setdefault(fid, image)


def _record_delete(cache: Optional[Dict], layer) -> None:
    """A delete through the provider: on positional FIDs the baseline is lost."""
    if cache is None:
        return
    from .commit_guard import fid_is_stable
    if not fid_is_stable(layer):
        cache[_BASELINE_LOST_KEY] = True


def _pre_images(baseline: Optional[_Baseline]) -> Optional[Dict]:
    """FID -> pre-image recorded by this operation; None when unusable."""
    if baseline is None or baseline.cache is None:
        return {}
    if baseline.cache.get(_BASELINE_LOST_KEY):
        return None
    images = baseline.cache.get(_PRE_IMAGES_KEY)
    return images if isinstance(images, dict) else {}


def _named(feature, name: str):
    """Value of column *name* on *feature*, or `_UNREADABLE`."""
    try:
        return feature[name]
    except (KeyError, IndexError):
        return _UNREADABLE


def _holds_value(feature, name: str, expected, version) -> bool:
    """True when *feature* carries *expected* in *name*.

    A column the feature cannot show (a virtual field on a provider
    feature) counts as carrying it: an unknown holder makes a value less
    rare, never more.
    """
    actual = _named(feature, name)
    return actual is _UNREADABLE or _vals_match(actual, expected, version)


def _baseline_others(layer, target_fid: int, baseline: _Baseline, holds,
                     what: str, request=None,
                     limit: int = 1) -> Tuple[Optional[int], bool]:
    """``(others, target_seen)`` at the baseline.

    *others*: features OTHER than *target_fid* whose baseline state
    satisfies *holds* (up to *limit*), or None when the baseline is lost.
    *target_seen*: *request* returned the target itself, which lets a
    caller check that a pre-filter did not miss what it should find.
    Raises TargetScanError.
    """
    images = _pre_images(baseline)
    if images is None:
        return None, False
    count = 0
    seen = False
    for feature in _scan(layer, request, what,
                         provider_only=baseline.provider_only):
        fid = feature.id()
        if fid == target_fid:
            seen = True
            continue
        if fid in images:
            continue
        if holds(feature):
            count += 1
            if count >= limit:
                return count, seen
    for fid, image in images.items():
        if fid == target_fid or image is _ADDED:
            continue
        if holds(image):
            count += 1
            if count >= limit:
                break
    return count, seen


def _source_fields(layer, baseline: _Baseline):
    return (layer.dataProvider().fields() if baseline.provider_only
            else layer.fields())


# -----------------------------------------------------------------------
# The committed state of a rewind session, read once (v7)
#
# The rewind proves every key nomination at the committed state
# (`_Baseline(provider_only=True)`), and that state does not move while the
# rewind fills its edit buffer. Yet every compensation read it again: the
# provider filter on the target's LIVE value is only believed when it
# returns the target itself, and from the second edit of a chain on the
# buffer holds a value the committed row does not -- so the proof fell back
# to a read of the whole layer. K edits on N entities cost K x N reads of N
# features (rw_rewind_mass_exec_bench: 7500 compensations on 500 entities,
# 43 s of frozen GUI).
#
# A rewind session (one layer, one edit buffer: the fid_remap of the strict
# runner, see `restore_executor._rewind_baseline`) now reads each column it
# needs ONCE from the provider, and every rarity count of the session is
# computed from that reading with the same comparison (`_vals_match`) and
# the same rule for a column the provider does not show. A column of one
# plain type (int, float, text, bool) is bucketed by value; a candidate
# comes from the bucket of the captured value or of its decoding into that
# type -- the only two values `_vals_match` can find equal on such a
# column -- and is then checked like any other. Any other column is
# checked row by row, in memory.
#
# The reading is dropped as soon as the committed state may have moved:
# a commit attempt (even one that keeps the edit session open, even one
# that fails half-way), a rollback, the edit session closing, the provider
# reporting data changed outside QGIS. A layer whose signals cannot be
# watched keeps the read-each-time path. What no signal reports -- another
# program writing the file while the rewind fills its buffer -- stays the
# limit it already was for the baseline itself.
# -----------------------------------------------------------------------

_SESSION_KEY = ("__rewind_session__",)
_INDEX_KEY = ("__committed_index__",)
_BUCKET_TYPES = (bool, int, float, str)
_NOT_A_COLUMN = object()


def _rewind_session() -> Dict:
    """A fresh batch cache marked as one rewind session (see above)."""
    return {_SESSION_KEY: True}


class _CommittedColumn(NamedTuple):
    """One column of the committed state (`_CommittedIndex`)."""

    values: Dict[int, Any]
    nulls: List[int]
    buckets: Optional[Dict[Any, List[int]]]   # None: check every row
    sample: Any                               # a non-NULL value, its type


def _layer_key(layer) -> str:
    try:
        return f"{layer.id()}|{layer.dataProvider().dataSourceUri()}"
    except (AttributeError, RuntimeError):
        return f"object-{id(layer)}"


class _CommittedIndex:
    """The committed columns a rewind session's rarity counts read."""

    def __init__(self, layer):
        self.layer_key = _layer_key(layer)
        self.stale = False
        self._columns: Dict[str, Any] = {}
        self._geometry: Optional[Dict[int, str]] = None
        self._hooks: list = []
        # identity -> (fids, via) of `_lookup_identity`, same lifetime.
        self.lookups: Dict[tuple, tuple] = {}

    def watch(self, layer) -> bool:
        """Drop the reading when the committed state may move; False if blind."""
        def _moved(*_args):
            self.stale = True
            self.release()

        hooks = []
        try:
            for signal in (layer.beforeCommitChanges, layer.afterCommitChanges,
                           layer.afterRollBack, layer.editingStopped,
                           layer.dataProvider().dataChanged):
                signal.connect(_moved)
                hooks.append(signal)
        except (AttributeError, RuntimeError, TypeError) as exc:
            flog(f"committed index: layer signals unavailable ({exc}); the "
                 f"committed state is read at every proof", "DEBUG")
            self._hooks = [(signal, _moved) for signal in hooks]
            self.release()
            return False
        self._hooks = [(signal, _moved) for signal in hooks]
        return True

    def release(self) -> None:
        hooks, self._hooks = self._hooks, []
        for signal, slot in hooks:
            try:
                signal.disconnect(slot)
            except (TypeError, RuntimeError):
                pass
        self._columns.clear()
        self._geometry = None
        self.lookups.clear()

    def column(self, layer, name: str, what: str) -> Optional[_CommittedColumn]:
        """*name* at the committed state, read on first use; None if the
        provider has no such column. Raises TargetScanError."""
        col = self._columns.get(name)
        if col is None:
            col = _read_committed_column(layer, name, what)
            self._columns[name] = _NOT_A_COLUMN if col is None else col
        return None if col is _NOT_A_COLUMN else col

    def geometry_states(self, layer, what: str) -> Dict[int, str]:
        """FID -> ``present`` / ``null`` / ``empty`` at the committed state."""
        if self._geometry is None:
            from qgis.core import QgsFeatureRequest
            request = QgsFeatureRequest()
            request.setNoAttributes()
            states = {}
            for feature in _scan(layer, request,
                                 f"{what} committed geometries",
                                 provider_only=True):
                live = feature.geometry()
                states[feature.id()] = ("present" if is_geometry_present(live)
                                        else _absence_kind(live))
            self._geometry = states
        return self._geometry

    def others(self, layer, target_fid: int, checks: List[tuple], what: str,
               version: Any = _NO_EVENT, absence: Optional[str] = None,
               no_geometry: bool = False, limit: int = 1) -> Optional[int]:
        """`_key_rarity` for attribute-only or absence-of-geometry evidence.

        None when a checked column is not the provider's (the caller then
        reads the layer, whose rule counts such a column as carried).
        """
        columns = []
        for name, expected in checks:
            col = self.column(layer, name, what)
            if col is None:
                return None
            columns.append((col, expected))
        states = self.geometry_states(layer, what) if no_geometry else None
        if not columns and states is None:
            return None
        candidates = None
        for col, expected in columns:
            found = _column_candidates(col, expected, version)
            if found is not None and (candidates is None
                                      or len(found) < len(candidates)):
                candidates = found
        if candidates is None:
            candidates = (list(states) if states is not None
                          else list(columns[0][0].values))
        count = 0
        for fid in candidates:
            if fid == target_fid:
                continue
            if states is not None:
                state = states.get(fid, "present")
                if state == "present" or (absence is not None
                                          and state != absence):
                    continue
            if all(fid not in col.values
                   or _vals_match(col.values[fid], expected, version)
                   for col, expected in columns):
                count += 1
                if count >= limit:
                    break
        return count


def _read_committed_column(layer, name: str,
                           what: str) -> Optional[_CommittedColumn]:
    """Every committed value of column *name*, one provider read."""
    from qgis.core import QgsFeatureRequest
    fields = layer.dataProvider().fields()
    idx = fields.indexOf(name)
    if idx < 0:
        return None
    request = QgsFeatureRequest().setFlags(QgisCompat.NO_GEOMETRY)
    request.setSubsetOfAttributes([idx])
    values: Dict[int, Any] = {}
    for feature in _scan(layer, request, f"{what} committed column {name}",
                         provider_only=True):
        values[feature.id()] = feature[idx]
    nulls = [fid for fid, value in values.items() if _is_null(value)]
    kinds = {type(value) for value in values.values() if not _is_null(value)}
    buckets, sample = None, None
    if len(kinds) <= 1 and all(kind in _BUCKET_TYPES for kind in kinds):
        buckets = {}
        for fid, value in values.items():
            if not _is_null(value):
                buckets.setdefault(value, []).append(fid)
                sample = value
    flog(f"committed index: column {name!r} read once for the rewind "
         f"session rows={len(values)} bucketed={buckets is not None}",
         "DEBUG")
    return _CommittedColumn(values, nulls, buckets, sample)


def _column_candidates(col: _CommittedColumn, expected,
                       version: Any) -> Optional[List[int]]:
    """FIDs that may carry *expected* in *col*; None when every row may.

    NULL matches NULL only. On a bucketed column (every value of one plain
    type) `_qgis_vals_equal` finds a value equal to *expected* only when it
    is ``==`` to *expected* itself or to its decoding into that type
    (`_decoded_like`, the same for every row of the column), and ``==`` on
    those types agrees with the dict lookup. Anything else: every row.
    """
    if _is_null(expected):
        return col.nulls
    if col.buckets is None or not isinstance(expected, _BUCKET_TYPES):
        return None
    keys = [expected]
    if col.sample is not None:
        decoded = _decoded_like(expected, col.sample, version)
        if decoded is not expected:
            keys.append(decoded)
    found: Dict[int, None] = {}
    for key in keys:
        try:
            found.update(dict.fromkeys(col.buckets.get(key, ())))
        except TypeError:
            return None
    # Once each: the value and its decoding can share a bucket (5 and 5.0).
    return list(found)


def _committed_index(layer, baseline: Optional[_Baseline]
                     ) -> Optional[_CommittedIndex]:
    """The session's reading of the committed state, or None (read each time).

    Only for a rewind session (`_rewind_session`) that reads the committed
    state (``provider_only``) and recorded no pre-image.
    """
    cache = baseline.cache if baseline is not None else None
    if (cache is None or not baseline.provider_only
            or not cache.get(_SESSION_KEY) or _pre_images(baseline) != {}):
        return None
    index = cache.get(_INDEX_KEY)
    if index is not None and (index.stale
                              or index.layer_key != _layer_key(layer)):
        if index.stale:
            flog("committed index: the committed state moved during the "
                 "rewind session (commit, rollback or provider change); "
                 "read again", "INFO")
        index.release()
        index = None
    if index is None:
        index = _CommittedIndex(layer)
        if not index.watch(layer):
            cache.pop(_INDEX_KEY, None)
            return None
        cache[_INDEX_KEY] = index
    return index


def _key_rarity(layer, target_fid: int, baseline: _Baseline, what: str,
                checks: List[tuple], geom=None,
                absence_kind: Optional[str] = None,
                filter_on: Optional[tuple] = None,
                version: Any = _NO_EVENT) -> Optional[int]:
    """OTHER features carrying the evidence of a key nomination, at baseline.

    The evidence is *geom* (a geometry to match, `NO_GEOMETRY` for an
    absence of the captured *absence_kind*, None for none) AND every
    ``(name, expected)`` of *checks*. 0 means the target alone carries it.
    Returns None when the baseline is lost (see `_record_pre_image`).

    Attribute-only evidence is pre-filtered by the provider on
    *filter_on* ``(name, live value of the target)``. A provider evaluates
    a filter its own way and can only UNDER-count; so an answer of "no
    other holder" from the filter is kept only when the filter returned
    the target itself, and read again without the filter otherwise.

    In a rewind session, attribute-only and absence-of-geometry evidence
    is counted on the session's single reading of the committed state
    (`_committed_index`) instead: same comparison, no provider read per
    compensation. Raises TargetScanError.
    """
    from qgis.core import QgsFeatureRequest, QgsExpression

    if geom is None or geom is NO_GEOMETRY:
        index = _committed_index(layer, baseline)
        if index is not None:
            others = index.others(layer, target_fid, checks, what, version,
                                  absence=absence_kind,
                                  no_geometry=geom is NO_GEOMETRY)
            if others is not None:
                return others

    expected_wkb = None
    if _has_shape(geom):
        expected_wkb = bytes(geom.asWkb())

    def holds(feature) -> bool:
        if geom is NO_GEOMETRY:
            live = feature.geometry()
            if is_geometry_present(live):
                return False
            if absence_kind is not None and _absence_kind(live) != absence_kind:
                return False
        elif geom is not None and not feature_matches_geometry(
                feature, geom, expected_wkb=expected_wkb):
            return False
        return all(_holds_value(feature, name, expected, version)
                   for name, expected in checks)

    def base_request():
        request = QgsFeatureRequest()
        if _has_shape(geom):
            request.setFilterRect(geom.boundingBox())
        elif geom is None:
            request.setFlags(QgisCompat.NO_GEOMETRY)
            names = [name for name, _expected in checks]
            fields = _source_fields(layer, baseline)
            if names and all(fields.indexOf(n) >= 0 for n in names):
                request.setSubsetOfAttributes(names, fields)
        return request

    if geom is None and filter_on is not None:
        name, live_value = filter_on
        try:
            request = base_request()
            request.setFilterExpression(
                f"{QgsExpression.quotedColumnRef(name)} = "
                f"{QgsExpression.quotedValue(live_value)}")
            others, seen = _baseline_others(layer, target_fid, baseline,
                                            holds, what, request)
            if others is None or others or seen:
                return others
        except (TypeError, ValueError, AttributeError) as exc:
            flog(f"_key_rarity: provider filter unusable on {name!r} "
                 f"({exc}); scanning", "DEBUG")
    others, _seen = _baseline_others(layer, target_fid, baseline, holds,
                                     what, base_request())
    return others


def _continuity_token(event: Optional[AuditEvent]) -> Optional[tuple]:
    """What tells two rows apart for the continuity verdict cache.

    A plan row of the rewind carries the id of a trace (or of its source)
    and the date of a toggle: the mark (``commit_id``) keeps it apart from
    the journal row of the same id.
    """
    if event is None:
        return None
    return (event.event_id, event.datasource_fingerprint, event.created_at,
            getattr(event, "commit_id", None))


def _key_continuity(baseline: Optional[_Baseline], layer,
                    event: AuditEvent,
                    until: Optional[AuditEvent] = None) -> Tuple[bool, str]:
    """Does the journal prove that the key of *event* never changed hands?

    ``(continuous, why)``, cached per event in the batch cache. Without a
    journal (none handed to this operation) the answer is no: the key is
    then proven by a rare content only, as before.

    *until*: the DELETE whose re-insertion by this rewind created the
    feature being proven (`restore_executor._remap_update_proof`). The
    question is then whether the key named ONE entity from *event* to that
    DELETE: what the DELETE removed is what *event* edited.
    """
    cache = baseline.cache if baseline is not None else None
    journal = cache.get(_JOURNAL_KEY) if cache is not None else None
    if journal is None:
        return False, "no_journal"
    verdicts = cache.setdefault(_CONTINUITY_KEY, {})
    token = (_continuity_token(event), _continuity_token(until))
    if token not in verdicts:
        verdicts[token] = _journal_key_continuity(journal, layer, event,
                                                  until)
        flog(f"key continuity eid={event.event_id} "
             f"until={until.event_id if until is not None else '-'} "
             f"continuous={verdicts[token][0]} why={verdicts[token][1]}")
    return verdicts[token]


class _Anchor(NamedTuple):
    """The journal row a continuity proof starts (or stops) at."""

    event_id: int
    operation_type: str
    created_at: str


def _blob(value) -> Optional[bytes]:
    return bytes(value) if value is not None else None


def _payload_of(text):
    """*text* parsed as JSON, or *text* itself when it is not JSON."""
    try:
        return json.loads(text) if text is not None else None
    except (json.JSONDecodeError, TypeError, ValueError):
        return text


def _journal_anchor(conn, scope: List[str], event: AuditEvent,
                    entity_fp: Optional[str]) -> Tuple[Optional[_Anchor], str]:
    """``(anchor, why)``: the journal row a continuity proof of *event* uses.

    A user event is its own anchor, once the journal holds it (same
    datasource, entity, operation, date and payload: the journal of THIS
    layer). A plan row of the rewind is never a journal row: it carries the
    id of the trace it undoes (or of its source), the date of the toggle,
    and a re-application swaps the OLD and NEW sides
    (`rewind_dedup.build_reapply_item`, `build_recompensate_item`). Its
    anchor is its SOURCE, the user event at the end of the
    ``restored_from_event_id`` chain of the row it names, and only once the
    plan row is exactly what the builder makes of those journal rows: same
    operation, payload, geometries and entity, and a trace depth that fits
    the kind (a re-application undoes an odd-depth compensation, a second
    compensation an even-depth re-application, or none). Held to its own
    id, a plan row never matched: every forward hop and every re-rewind of
    an edit whose value other features share was refused
    ``event_not_in_journal``, journal given or not.

    ``(None, why)`` when the journal does not back the row. Raises
    sqlite3.Error / ValueError on a failed read.
    """
    from .event_stream_repository import _rows_where_in
    from .identity import compute_entity_fingerprint
    from .rewind_dedup import (
        plan_item_kind, build_reapply_item, build_recompensate_item,
        PLAN_MARK_REAPPLY, PLAN_MARK_RECOMPENSATE,
    )
    from .sql_safety import assert_safe_fragment

    kind = plan_item_kind(event)
    if kind is None:
        marks = assert_safe_fragment(",".join("?" for _ in scope))
        # B608: only "?" placeholders are formatted in; values via `?`.
        own = conn.execute(
            "SELECT operation_type, entity_fingerprint, created_at,"  # nosec B608
            " attributes_json FROM audit_event WHERE event_id = ?"
            " AND datasource_fingerprint IN (" + marks + ")",
            [event.event_id] + list(scope)).fetchone()
        if own is None or tuple(own) != (event.operation_type, entity_fp,
                                         event.created_at,
                                         event.attributes_json):
            return None, "event_not_in_journal"
        return _Anchor(event.event_id, event.operation_type,
                       event.created_at or ""), ""
    if kind not in (PLAN_MARK_REAPPLY, PLAN_MARK_RECOMPENSATE):
        return None, f"plan_row_not_writable ({kind})"
    chain: List[AuditEvent] = []
    eid, seen = event.event_id, set()
    while True:
        found = _rows_where_in(conn, "event_id", [eid])
        row = found[0] if found else None
        if row is None or row.datasource_fingerprint not in scope:
            return None, f"plan_row_not_in_journal (row {eid})"
        chain.append(row)
        seen.add(eid)
        eid = row.restored_from_event_id
        if eid is None:
            break
        if eid in seen:
            return None, f"plan_row_trace_cycle (row {eid})"
    named, root = chain[0], chain[-1]
    depth = len(chain) - 1
    position = (event.created_at, 0)
    if kind == PLAN_MARK_REAPPLY and depth % 2 == 1:
        expected = build_reapply_item(root, named, position)
    elif kind == PLAN_MARK_RECOMPENSATE and depth % 2 == 0:
        expected = build_recompensate_item(
            root, position, via=named if depth else None)
    else:
        return None, (f"plan_row_shape_mismatch ({kind} naming row "
                      f"{named.event_id} at trace depth {depth})")
    root_fp = (root.entity_fingerprint
               or compute_entity_fingerprint(root.feature_identity_json))
    if (root_fp != entity_fp
            or expected.operation_type != event.operation_type
            or _payload_of(expected.attributes_json)
            != _payload_of(event.attributes_json)
            or _blob(expected.geometry_wkb) != _blob(event.geometry_wkb)
            or _blob(expected.new_geometry_wkb)
            != _blob(event.new_geometry_wkb)):
        return None, (f"plan_row_not_journalled (row {named.event_id}, "
                      f"source {root.event_id}: not what the journal builds)")
    return _Anchor(root.event_id, root.operation_type,
                   root.created_at or ""), f"source:{root.event_id}"


def _journal_key_continuity(conn, layer, event: AuditEvent,
                            until: Optional[AuditEvent] = None
                            ) -> Tuple[bool, str]:
    """`_key_continuity` read from the journal *conn* (sqlite3, read only).

    Continuous when the journal backs *event* (`_journal_anchor`: the row
    itself, or for a plan row of the rewind the SOURCE user event it
    re-applies or compensates again) and, after that anchor, on the same
    datasource (aliases included), no DELETE nor INSERT of its key -- a
    restore trace included: an undone restore deleted it -- and no UPDATE
    that edited a key column of any feature. "After" is by id OR by date,
    whichever reaches further: an event replayed from the capture outbox
    gets a later id than its date says. For a plan row the scan starts at
    the source, not at the trace the row names: a key given to another
    feature between the two is a change of hands the trace alone hides.

    *until* (a DELETE, see `_key_continuity`) bounds the scan: the rows
    strictly between the anchor and that DELETE, which must come after it
    by id and by date.

    What the journal cannot know stays a limit: a key reused while nothing
    captured the layer (another program, QGIS without the plugin).
    """
    import sqlite3
    from .identity import compute_entity_fingerprint
    from .event_stream_repository import resolve_scope
    from .rewind_dedup import plan_item_kind
    from .sql_safety import assert_safe_fragment

    pairs = _key_pairs(_event_identity(layer, event))
    if not pairs:
        return False, "no_key"
    if event.event_id is None:
        return False, "event_not_journalled"
    if plan_item_kind(event) is None and event.operation_type == "DELETE":
        return False, "the_event_released_the_key"
    entity_fp = (event.entity_fingerprint
                 or compute_entity_fingerprint(event.feature_identity_json))
    names = [name for name, _value in pairs]
    bound = None
    try:
        scope = resolve_scope(conn, event.datasource_fingerprint)
        if not scope:
            return False, "no_datasource"
        anchor, why = _journal_anchor(conn, scope, event, entity_fp)
        if anchor is None:
            return False, why
        if anchor.operation_type == "DELETE":
            return False, f"the_source_released_the_key:{anchor.event_id}"
        if until is not None:
            until_fp = (until.entity_fingerprint or compute_entity_fingerprint(
                until.feature_identity_json))
            if until.event_id is None or until_fp != entity_fp:
                return False, "remap_source_not_this_entity"
            bound, why = _journal_anchor(conn, scope, until, entity_fp)
            if bound is None:
                return False, f"remap_source_{why}"
            if bound.operation_type != "DELETE":
                return False, f"remap_source_not_a_delete:{bound.event_id}"
            if not (bound.event_id > anchor.event_id
                    and bound.created_at >= anchor.created_at):
                return False, f"remap_source_not_after_event:{bound.event_id}"
        marks = assert_safe_fragment(",".join("?" for _ in scope))
        likes = assert_safe_fragment(
            " OR ".join("attributes_json LIKE ?" for _ in names))
        window = " AND event_id != ? AND (event_id > ? OR created_at >= ?)"
        params = list(scope) + [anchor.event_id, anchor.event_id,
                                anchor.created_at]
        if bound is not None:
            window += " AND event_id != ? AND (event_id < ? OR created_at <= ?)"
            params += [bound.event_id, bound.event_id, bound.created_at]
        # B608: only "?" placeholders and literals are formatted in.
        rows = conn.execute(
            "SELECT event_id, operation_type, attributes_json"  # nosec B608
            " FROM audit_event WHERE datasource_fingerprint IN (" + marks + ")"
            + window +
            " AND ((entity_fingerprint = ?"
            " AND operation_type IN ('INSERT', 'DELETE'))"
            " OR (operation_type = 'UPDATE' AND (" + likes + ")))"
            " ORDER BY event_id",
            params + [entity_fp] + [f'%"{name}"%' for name in names]
        ).fetchall()
    except (sqlite3.Error, ValueError) as exc:
        return False, f"journal_unreadable ({exc})"
    for other_id, operation, attributes_json in rows:
        if operation in ("INSERT", "DELETE"):
            return False, f"key_{operation.lower()}_after_event:{other_id}"
        try:
            delta = json.loads(attributes_json or "{}")
        except (json.JSONDecodeError, TypeError):
            return False, f"unreadable_update_after_event:{other_id}"
        changed = delta.get("changed_only") if isinstance(delta, dict) else None
        if isinstance(changed, dict) and any(n in changed for n in names):
            return False, f"key_edited_after_event:{other_id}"
    if bound is not None:
        return True, (f"journal_continuous_from:{anchor.event_id}"
                      f"_to_delete:{bound.event_id}")
    return True, f"journal_continuous_since:{anchor.event_id}"


# -----------------------------------------------------------------------
# A Mode A restore of an event already compensated (v7)
#
# U: 'val: 10 -> 872', restored (trace T1: the layer is back at 10), then
# the user types 872 again (U2) and restores U a second time. The target
# carries U's post-edit state again -- thanks to U2 -- so every identity
# proof passes, and the restore wrote 10 under a SECOND trace naming U.
# Physically it undid U2; the journal said U was restored twice and U2 is
# still in force, and a rewind reading it had to guess which trace counts
# (a restore of U2, the right one, then found nothing to do). Same for a
# DELETE: delete, restore, delete again, restore the first delete again.
#
# So the three Mode A handlers ask the journal, when they hold it, before
# they write: an event a trace in effect compensates
# (`rewind_dedup.traces_in_effect` / `cancelling_trace`: not undone, not
# cancelled) is refused by name (reason_code ``already_compensated``), with
# the trace and the later edit of the same entity; nothing is written, no
# trace is journalled. A restore that finds the feature already at the
# restored state is still a skip (it writes nothing), and the undo paths,
# which undo exactly such a compensation, never ask. Without the journal
# the handlers cannot know, and keep their former behaviour: the runner
# that holds it passes it (`restore_batch(journal=...)`).
# -----------------------------------------------------------------------

_COMPENSATION_KEY = ("__compensation_verdicts__",)


def _compensation_refusal(cache: Optional[Dict], event: AuditEvent,
                          context: str) -> Optional[Dict[str, Any]]:
    """Refusal dict when a trace in effect already compensates *event*.

    None when it does not, or when no journal was handed to the operation.
    Asked by the Mode A handlers only, right before they write (see above).
    """
    journal = cache.get(_JOURNAL_KEY) if cache is not None else None
    eid = event.event_id
    if journal is None or eid is None:
        return None
    verdicts = cache.setdefault(_COMPENSATION_KEY, {})
    token = (eid, event.datasource_fingerprint, event.created_at)
    if token not in verdicts:
        verdicts[token] = _journal_compensation(journal, event)
    found = verdicts[token]
    if found is None:
        return None
    if found[0] == "unreadable":
        flog(f"{context}: status=FAILED reason_code=scan_failed journal read "
             f"failed while checking whether event {eid} was already "
             f"restored ({found[1]})", "ERROR")
        return {"success": False, "reason_code": "scan_failed",
                "message": (f"Journal read failed while checking whether "
                            f"event {eid} was already restored ({found[1]}); "
                            f"nothing written")}
    trace_id, trace_at, later = found
    if later is not None:
        later_id, later_op, later_at = later
        since = (f"event {later_id} ({later_op} of {later_at}) changed this "
                 f"feature since")
        advice = f"restore event {later_id} instead"
    else:
        since = ("the layer changed since that restore (no later edit of "
                 "this feature is journalled under the same identity: a "
                 "change made outside RecoverLand, or after a renumbering)")
        advice = "restore the edit that brought the change instead"
    flog(f"{context}: status=refused reason_code=already_compensated "
         f"eid={eid} trace={trace_id} later={later} (a second restore "
         f"would undo the later change under a trace naming event {eid})",
         "WARNING")
    return {
        "success": False,
        "reason_code": "already_compensated",
        "compensated_by": trace_id,
        "later_event": later[0] if later is not None else None,
        "message": (f"Refused (already_compensated): event {eid} was already "
                    f"restored by trace {trace_id} ({trace_at}), and {since}. "
                    f"Restoring event {eid} again would undo that later "
                    f"change under a trace that names event {eid}: {advice}. "
                    f"Nothing written"),
    }


def _journal_compensation(conn, event: AuditEvent):
    """``(trace id, trace date, later edit or None)`` compensating *event*.

    None when no trace in effect compensates it, or when the journal does
    not hold the event (a journal of another layer proves nothing either
    way: logged). ``("unreadable", why)`` when the read failed. *later*:
    ``(event_id, operation, created_at)`` of the first user edit of the
    same entity (same datasource, aliases included) after the trace.
    """
    import sqlite3
    from .event_stream_repository import _rows_where_in, resolve_scope
    from .rewind_dedup import traces_in_effect, cancelling_trace
    from .identity import compute_entity_fingerprint
    from .sql_safety import assert_safe_fragment

    eid = event.event_id
    try:
        own = conn.execute(
            "SELECT operation_type, created_at FROM audit_event"
            " WHERE event_id = ?", (eid,)).fetchone()
        if own is None or tuple(own) != (event.operation_type,
                                         event.created_at):
            flog(f"compensation guard: event {eid} is not in the journal "
                 f"handed to the restore; whether it was already restored "
                 f"cannot be told", "WARNING")
            return None
        traces: List[AuditEvent] = []
        frontier, seen = [eid], {eid}
        while frontier:
            rows = [row for row in _rows_where_in(
                conn, "restored_from_event_id", frontier)
                if row.event_id not in seen]
            seen.update(row.event_id for row in rows)
            traces.extend(rows)
            frontier = [row.event_id for row in rows]
        if not traces:
            return None
        trace = cancelling_trace(eid, traces, traces_in_effect(traces))
        if trace is None:
            return None
        entity_fp = (event.entity_fingerprint
                     or compute_entity_fingerprint(event.feature_identity_json))
        scope = (resolve_scope(conn, event.datasource_fingerprint)
                 or [event.datasource_fingerprint])
        marks = assert_safe_fragment(",".join("?" for _ in scope))
        # B608: only "?" placeholders are formatted in; values via `?`.
        later = conn.execute(
            "SELECT event_id, operation_type, created_at"  # nosec B608
            " FROM audit_event WHERE datasource_fingerprint IN (" + marks + ")"
            " AND entity_fingerprint = ? AND restored_from_event_id IS NULL"
            " AND event_id != ? AND (event_id > ? OR created_at > ?)"
            " ORDER BY event_id LIMIT 1",
            list(scope) + [entity_fp, eid, trace.event_id,
                           trace.created_at or ""]).fetchone()
    except (sqlite3.Error, ValueError) as exc:
        return ("unreadable", str(exc))
    return (trace.event_id, trace.created_at,
            tuple(later) if later is not None else None)


def _count_geometry_holders(layer, geom, checks: List[tuple], what: str,
                            limit: int = 2,
                            version: Any = _NO_EVENT) -> int:
    """Features carrying *geom* AND the values of *checks* ``[(idx, value)]``.

    For `NO_GEOMETRY` the features WITHOUT a geometry are counted: there is
    no box to filter on, the whole layer is read. The FID-nomination count:
    the whole live layer, the target included (a key nomination counts at
    the baseline instead, `_key_rarity`).
    """
    from qgis.core import QgsFeatureRequest

    request = QgsFeatureRequest()
    expected_wkb = None
    if geom is not NO_GEOMETRY:
        request.setFilterRect(geom.boundingBox())
        expected_wkb = bytes(geom.asWkb())
    count = 0
    for feature in _scan(layer, request, what):
        if geom is NO_GEOMETRY:
            if is_geometry_present(feature.geometry()):
                continue
        elif not feature_matches_geometry(feature, geom,
                                          expected_wkb=expected_wkb):
            continue
        if all(_vals_match(feature[idx], value, version)
               for idx, value in checks):
            count += 1
            if count >= limit:
                break
    return count


def _count_value_holders(layer, idx: int, live_value, expected, what: str,
                         limit: int = 2, version: Any = _NO_EVENT) -> int:
    """Features whose field *idx* carries *expected* (up to *limit*).

    The provider filters first (an attribute filter is cheap where a
    Python scan of the layer is not); a filter the provider evaluates
    differently from our comparison can only UNDER-count, and an
    under-count of zero -- the occupant itself missed -- falls back to the
    Python scan rather than being read as "unique". The FID-nomination
    count, like `_count_geometry_holders`.
    """
    from qgis.core import QgsFeatureRequest, QgsExpression

    def _count(request) -> int:
        count = 0
        for feature in _scan(layer, request, what):
            if _vals_match(feature[idx], expected, version):
                count += 1
                if count >= limit:
                    break
        return count

    def _base():
        request = QgsFeatureRequest()
        request.setFlags(QgisCompat.NO_GEOMETRY)
        request.setSubsetOfAttributes([idx])
        return request

    name = layer.fields().at(idx).name()
    try:
        request = _base()
        request.setFilterExpression(
            f"{QgsExpression.quotedColumnRef(name)} = "
            f"{QgsExpression.quotedValue(live_value)}")
        count = _count(request)
        if count:
            return count
    except (TypeError, ValueError, AttributeError) as exc:
        flog(f"_count_value_holders: provider filter unusable on {name!r} "
             f"({exc}); scanning", "DEBUG")
    return _count(_base())


def _prove_update_state(layer, fid: int, event: AuditEvent, side: str,
                        field_mapping: Dict[str, str],
                        via: str,
                        baseline: Optional[_Baseline] = None) -> Tuple[str, str]:
    """Does feature *fid* carry the *side* state of UPDATE *event*?

    Returns ``(identity_level, reason)``. ``side='new'`` is the proof a
    restore needs (the feature still carries what the edit produced),
    ``side='old'`` the proof an undo of that restore needs (the feature
    still carries what the restore wrote) and the "already done" proof of
    the rewind.

    Every compared field must match (NULL-aware) and the captured geometry
    too. Then the match must DISCRIMINATE, because an UPDATE event only
    carries the fields the edit changed: a FID-only proof resting on one
    boolean ("valide: false -> true") is met by every feature of the layer
    whose ``valide`` is true, including the unrelated one a shapefile repack
    put at the historical FID. So, for a FID nomination:

      - two captured non-NULL values or more: MEDIUM;
      - the captured geometry, carried by no other feature: MEDIUM
        (stacked points sharing it: AMBIGUOUS);
      - one captured value, carried by no other feature: MEDIUM
        (shared: WEAK);
      - NULLs only: WEAK.

    A captured "no geometry" (`NO_GEOMETRY`: the NULL marker of a geometry
    added or removed by the edit, or a typed EMPTY one) is compared too --
    a feature WITH a geometry does not carry that state -- but, being a
    state many features share, it discriminates only when no other
    feature of the layer lacks a geometry with the same values.

    A primary key nomination is STRONG when its content corroborates it,
    and a content corroborates when NO OTHER feature carries it at the
    baseline (`_prove_key_content`, `_key_rarity`): the captured geometry,
    or absence of one, with the captured values; the COMBINATION of the
    captured values when there are two or more (two values the whole
    layer carries, "statut: actif" and "type: borne", prove nothing more
    than one); the one value. Any agreement used to do: a reused key whose
    holder had an empty ``nom`` received "nom: ORIG_OLD -> NULL" as STRONG
    (NULL == NULL), and "statut: inactif -> actif" landed on it because the
    whole layer is ``actif``.

    A content that does not discriminate (NULLs only, a shared value or
    combination, a shared geometry) leaves the key to its history: STRONG
    when the journal proves that the key never changed hands since the
    event (`_key_continuity`), WEAK otherwise -- a named refusal. Restoring
    "flag: False -> True" on a GeoPackage where every other row is True is
    thus possible again, where the key is continuous, and refused where it
    was given to another feature: the two layers are identical, only the
    journal tells them apart.

    The baseline is the layer as the user left it (*baseline*, see
    `_Baseline`), never the layer this operation is rewriting: counted on
    the latter, a batch that first wrote another column of a feature
    carrying the same value, or deleted it, made the value "unique" and the
    verdict depended on the order of the events. A FID nomination keeps
    the full count of the live layer: after a repack, the feature this
    batch already wrote may be the very entity the FID no longer points at.
    """
    if via != "pk":
        return _prove_update_content(layer, fid, event, side, field_mapping)
    evidence = _update_state_evidence(layer, fid, event, side, field_mapping)
    if not isinstance(evidence, _StateEvidence):
        level, reason = evidence
        return level, (reason if reason.startswith("scan_failed")
                       else f"pk+{reason}")
    baseline = baseline or _Baseline()
    try:
        level, reason = _prove_key_content(layer, fid, event, evidence,
                                           baseline)
    except TargetScanError as exc:
        return ID_UNRESOLVABLE, f"scan_failed: {exc}"
    if level == ID_MEDIUM:
        return ID_STRONG, f"pk+{reason}"
    continuous, why = _key_continuity(baseline, layer, event)
    if continuous:
        return ID_STRONG, f"pk+{reason} key_continuous ({why})"
    return ID_WEAK, f"pk+{reason} key_continuity_unproven ({why})"


class _StateEvidence(NamedTuple):
    """What feature *fid* shares with one side of an UPDATE, all of it equal."""

    feature: Any
    checks: List[tuple]      # (name, idx, expected), NULL included
    non_null: int
    single: Optional[tuple]  # (name, idx, live, expected) of the last non-NULL
    geom: Any                # _expected_geometry: None, NO_GEOMETRY or a shape
    wkb: Optional[bytes]
    tag: str
    shape: str


def _update_state_evidence(layer, fid: int, event: AuditEvent, side: str,
                           field_mapping: Dict[str, str]):
    """`_StateEvidence` of *fid*, or ``(level, reason)`` when it disagrees.

    Every compared field must match (NULL-aware), and the captured
    geometry (or absence of one) too.
    """
    tag = "post" if side == "new" else "pre"
    version = _event_version(event)
    attrs, wkb = _update_side(event, side)
    if wkb is None and not attrs:
        return ID_UNRESOLVABLE, f"no_{tag}_state_captured"
    try:
        feature = _read_one(layer, fid, f"{tag}-state proof fid={fid}")
    except TargetScanError as exc:
        return ID_UNRESOLVABLE, f"scan_failed: {exc}"
    if feature is None:
        return ID_UNRESOLVABLE, "target_unreadable"

    fields = layer.fields()
    compared = 0
    checks = []
    single = None
    for hist_name, cur_name in (field_mapping or {}).items():
        if hist_name not in attrs:
            continue
        idx = fields.indexFromName(cur_name)
        if idx < 0:
            continue
        compared += 1
        expected = attrs[hist_name]
        if not _vals_match(feature[idx], expected, version):
            return ID_UNRESOLVABLE, f"{tag}_attr_mismatch:{cur_name}"
        checks.append((cur_name, idx, expected))
        if not _is_null(expected):
            single = (cur_name, idx, feature[idx], expected)
    non_null = sum(1 for _n, _i, value in checks if not _is_null(value))

    geom = _expected_geometry(wkb, event, layer)
    if geom is NO_GEOMETRY:
        if is_geometry_present(feature.geometry()):
            return ID_UNRESOLVABLE, f"{tag}_geom_mismatch"
    elif geom is not None and not feature_matches_geometry(feature, geom):
        return ID_UNRESOLVABLE, f"{tag}_geom_mismatch"
    if compared == 0 and geom is None:
        return ID_UNRESOLVABLE, "no_comparable_field"

    if geom is NO_GEOMETRY:
        shape = f"{tag}_nogeom+attrs:{non_null}"
    elif geom is not None:
        shape = f"{tag}_geom+attrs:{non_null}"
    else:
        shape = f"{tag}_attrs:{non_null}"
    return _StateEvidence(feature, checks, non_null, single, geom, wkb, tag,
                          shape)


def _prove_update_content(layer, fid: int, event: AuditEvent, side: str,
                          field_mapping: Dict[str, str]) -> Tuple[str, str]:
    """The FID-nomination proof of `_prove_update_state` (levels there)."""
    evidence = _update_state_evidence(layer, fid, event, side, field_mapping)
    if not isinstance(evidence, _StateEvidence):
        return evidence
    version = _event_version(event)
    tag, shape, geom = evidence.tag, evidence.shape, evidence.geom
    checks = [(idx, expected) for _name, idx, expected in evidence.checks]
    if evidence.non_null >= 2:
        return ID_MEDIUM, shape
    try:
        if geom is NO_GEOMETRY:
            holders = _count_geometry_holders(
                layer, geom, checks, f"{tag}-state geometry absence uniqueness",
                version=version)
            if holders == 1:
                return ID_MEDIUM, f"{shape} only_feature_without_geometry"
            if holders > 1:
                return ID_WEAK, f"{shape} geometry_absence_shared_by:{holders}"
            return ID_UNRESOLVABLE, f"{shape} geometry_uniqueness_unprovable"
        if geom is not None:
            holders = _count_geometry_holders(
                layer, geom, checks, f"{tag}-state geometry uniqueness",
                version=version)
            if holders == 1:
                return ID_MEDIUM, f"{shape} geometry_unique"
            if holders > 1:
                return ID_AMBIGUOUS, f"{shape} geometry_shared_by:{holders}"
            return ID_UNRESOLVABLE, f"{shape} geometry_uniqueness_unprovable"
        if evidence.single is not None:
            cur_name, idx, live, expected = evidence.single
            holders = _count_value_holders(
                layer, idx, live, expected,
                f"{tag}-state value uniqueness {cur_name}", version=version)
            if holders == 1:
                return ID_MEDIUM, f"{shape} value_unique:{cur_name}"
            return ID_WEAK, f"{shape} value_shared:{cur_name}"
    except TargetScanError as exc:
        return ID_UNRESOLVABLE, f"scan_failed: {exc}"
    return ID_WEAK, f"{shape} nulls_only"


def _prove_key_content(layer, fid: int, event: AuditEvent,
                       evidence: _StateEvidence,
                       baseline: _Baseline) -> Tuple[str, str]:
    """Does the content of *fid* discriminate it, for a key nomination?

    MEDIUM when no OTHER feature carries the same evidence at the baseline
    (`_key_rarity`): the captured geometry, or absence of one of the
    captured kind (the NULL marker, or a typed EMPTY geometry: two states,
    journalled apart), with every captured value; else the COMBINATION of
    the captured values when there are two non-NULL ones or more; else the
    one non-NULL value. WEAK otherwise, NULLs only included. Raises
    TargetScanError.
    """
    version = _event_version(event)
    shape = evidence.shape
    what = f"{evidence.tag}-state key rarity fid={fid}"
    named = [(name, expected) for name, _idx, expected in evidence.checks]
    geom = evidence.geom
    suffix = ""
    if geom is NO_GEOMETRY:
        kind = ("null" if _is_null_marker(
            evidence.wkb, getattr(event, "event_schema_version", None))
            else "empty")
        others = _key_rarity(layer, fid, baseline, what, named, geom,
                             absence_kind=kind, version=version)
        found = "geometry_absence"
    elif geom is not None:
        others = _key_rarity(layer, fid, baseline, what, named, geom,
                             version=version)
        found = "geometry"
    elif evidence.non_null >= 2:
        name, _idx, live, _expected = next(
            (n, i, evidence.feature[i], e) for n, i, e in evidence.checks
            if not _is_null(e))
        others = _key_rarity(layer, fid, baseline, what, named,
                             filter_on=(name, live), version=version)
        found = "combination"
    elif evidence.single is not None:
        name, _idx, live, expected = evidence.single
        others = _key_rarity(layer, fid, baseline, what, [(name, expected)],
                             filter_on=(name, live), version=version)
        found = "value"
        suffix = f":{name}"
    else:
        return ID_WEAK, f"{shape} nulls_only"
    if others is None:
        return ID_WEAK, (f"{shape} {found}_rarity_unmeasurable{suffix} "
                         f"(baseline lost)")
    if others == 0:
        return ID_MEDIUM, f"{shape} {found}_unique{suffix}"
    return ID_WEAK, f"{shape} {found}_shared{suffix}"


def _update_state_candidates(layer, event: AuditEvent, side: str,
                             limit: int = 2, what: str = "") -> List[int]:
    """Features carrying the *side* state of UPDATE *event*, for a relocation.

    Geometry (when captured) AND every changed value, NULL-aware. Only a
    state that discriminates may relocate a write: a geometry, or two
    non-NULL values -- one field is not enough to bet a write on
    (`rw_update_postproof_repack`, case C), and neither is the absence of a
    geometry (`NO_GEOMETRY`). Raises TargetScanError.
    """
    attrs, wkb = _update_side(event, side)
    comparable = _comparable_fields(layer, attrs)
    geom = _expected_geometry(wkb, event, layer)
    if not _has_shape(geom) and _non_null_count(attrs, comparable) < 2:
        return []
    return _state_matches(layer, attrs, comparable, geom, limit=limit,
                          what=what or f"{side}-state relocation",
                          version=_event_version(event))


def _resolve_update_target(layer, event: AuditEvent, identity: Dict[str, Any],
                           field_mapping: Dict[str, str], side: str,
                           fid_cache: Optional[Dict] = None,
                           relocate: bool = True,
                           context: str = "",
                           baseline: Optional[_Baseline] = None
                           ) -> TargetResolution:
    """Which feature carries the *side* state of UPDATE *event*.

    The identity nominates, `_prove_update_state` decides. A FID identity
    that fails the proof may be relocated by a discriminating content scan
    (*relocate*); a primary key never is: the key names the entity.
    *baseline*: where a key nomination measures rarity (`_Baseline`); by
    default the batch cache *fid_cache* and its pre-images.
    """
    eid = event.event_id
    if baseline is None:
        baseline = _Baseline(cache=fid_cache)

    def _done(res: TargetResolution) -> TargetResolution:
        flog(f"{context} TARGET eid={eid} side={side} "
             f"identity_level={res.level} fid={res.fid} via={res.via or '-'} "
             f"reason={res.reason} nominated={res.nominated}",
             "INFO" if res.writable else "WARNING")
        return res

    try:
        nominated, via = _lookup_identity(layer, identity, fid_cache,
                                          baseline)
    except TargetScanError as exc:
        return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                      f"scan_failed: {exc}", via="identity"))
    if len(nominated) > 1:
        return _done(TargetResolution(None, ID_AMBIGUOUS, "key_not_unique",
                                      via=via, nominated=nominated[0]))
    nominated_fid = nominated[0] if nominated else None
    level, why = ID_UNRESOLVABLE, "not_found"
    if nominated_fid is not None:
        level, why = _prove_update_state(layer, nominated_fid, event, side,
                                         field_mapping, via, baseline)
        if level in WRITABLE_LEVELS:
            return _done(TargetResolution(nominated_fid, level, why, via=via,
                                          nominated=nominated_fid))
        if why.startswith("scan_failed"):
            return _done(TargetResolution(None, level, why, via=via,
                                          nominated=nominated_fid))
    if relocate and via != "pk":
        try:
            matches = _update_state_candidates(
                layer, event, side, what=f"{context} {side}-state relocation")
        except TargetScanError as exc:
            return _done(TargetResolution(None, ID_UNRESOLVABLE,
                                          f"scan_failed: {exc}", via="scan",
                                          nominated=nominated_fid))
        if len(matches) == 1:
            return _done(TargetResolution(matches[0], ID_MEDIUM,
                                          f"relocated_by_{side}_state",
                                          via="scan",
                                          nominated=nominated_fid))
        if len(matches) > 1:
            return _done(TargetResolution(None, ID_AMBIGUOUS,
                                          f"{side}_state_ambiguous:"
                                          f"{matches[:5]}",
                                          via="scan",
                                          nominated=nominated_fid))
    return _done(TargetResolution(None, level, why, via=via,
                                  absent=nominated_fid is None,
                                  nominated=nominated_fid))


def _target_already_at_old(layer, fid: int, event: AuditEvent,
                           field_mapping: Dict[str, str],
                           baseline: Optional[_Baseline] = None) -> bool:
    """True when feature *fid* ALREADY carries this event's OLD state.

    BL-RW-P5-28. Writing OLD onto a feature that is already at OLD is a
    no-op by definition, so it has no corruption surface -- and the
    rewind used to answer "target_unverifiable" to it, because the proof
    it demands is that the feature still carries the NEW state, which a
    feature already rolled back does not. 16 of the 59 refusals on the
    user's run of 2026-08-14 were this: the rewind refusing to do nothing,
    and reporting it as a failure.

    v7: "already done" is a claim about ONE feature, so it needs the same
    discriminating proof as a write (`_prove_update_state`, side ``old``).
    With the old one-field comparison, the unrelated occupant of a
    recycled FID that happened to carry the OLD value of a boolean was
    reported "already at the target state" while the real feature was
    never reverted. Silent on anything it cannot prove -- an unprovable
    "already done" must not become a skip.
    """
    via = _identity_via(_event_identity(layer, event))
    level, why = _prove_update_state(layer, fid, event, "old",
                                     field_mapping, via, baseline)
    if level not in WRITABLE_LEVELS:
        flog(f"_target_already_at_old: eid={event.event_id} fid={fid} "
             f"not proven identity_level={level} reason={why}", "DEBUG")
        return False
    return True


def _find_by_attrs_only(
    layer, event: AuditEvent,
    exclude_fids: Optional[set] = None,
    min_attrs: int = 2,
    expected_attrs: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """Attribute-only rescue scan (BL-RW-P1-25).

    Used when the geometry-based lookup finds no candidate (e.g. WKB
    precision drift through OGR write cycles) but the feature may still
    exist under a drifted FID. Matches STRICTLY on every relevant captured
    attribute; NULL matches NULL only, on either side (an empty feature
    must not match everything, nor an empty capture everything).

    Returns the FID of the unique match. Multiple matches are refused
    (no geometry available to bound the risk), fewer than *min_attrs*
    non-NULL comparable attributes make the event non-discriminant and
    return None. A failed read returns None and is logged; the write
    paths call `_resolve_insert_target`, which tells the two apart.

    *expected_attrs* overrides the state to look for. It defaults to the
    event's OLD state, which is what the DELETE path wants; the UPDATE
    relocation passes the NEW state instead, because the feature it is
    hunting for is the one that still carries the post-edit values.
    """
    if expected_attrs is None:
        expected_attrs = reconstruct_attributes(event)
    if not isinstance(expected_attrs, dict):
        expected_attrs = {}
    comparable = _comparable_fields(layer, expected_attrs)
    non_null = _non_null_count(expected_attrs, comparable)
    if non_null < min_attrs:
        flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
             f"non_discriminant n_attrs={non_null} min={min_attrs}",
             "INFO")
        return None
    try:
        matches = _state_matches(
            layer, expected_attrs, comparable, None,
            exclude_fids=exclude_fids,
            what=f"attr_rescue eid={event.event_id}",
            version=_event_version(event))
    except TargetScanError as exc:
        flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
             f"SCAN_ERR={exc}", "WARNING")
        return None
    if len(matches) == 1:
        flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
             f"n_attrs={non_null} result={matches[0]} "
             f"(geometry drifted, strict attr match)", "WARNING")
        return matches[0]
    flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
         f"n_attrs={non_null} matches={len(matches)} result=none"
         f"{' (ambiguous, refused)' if len(matches) > 1 else ''}",
         "INFO")
    return None


def _diagnose_snapshot_miss(layer, event: AuditEvent) -> None:
    """Log why the snapshot scan found 0 strict matches.

    Re-scans the layer using *geometry only* (same bbox prefilter) and,
    for every geom-only candidate, lists the historical attribute keys
    whose persisted value diverges from the captured snapshot. The first
    five candidates and the first five diverging attributes per candidate
    are reported. Side-effect free: only writes WARNING-level log lines.

    Used immediately after a snapshot lookup miss to expose the cause
    (typical: trigger-altered fields, NULL vs None, datetime serialization)
    without changing resolution logic.

    Gated on RECOVERLAND_HEAVY_DIAG: this is a THIRD full scan of the
    layer, after the FID lookup and the snapshot scan already missed, and
    it runs on the GUI thread. Measured on trace a1517527: 0.648 s for a
    single miss on zone_mkt_rip, i.e. a quarter of a second of freeze per
    event on exactly the layers where misses come in runs.
    """
    if not heavy_diag_enabled():
        flog(f"_diagnose_snapshot_miss: eid={event.event_id} skipped "
             f"(set RECOVERLAND_HEAVY_DIAG=1 to diagnose the miss)")
        return

    from qgis.core import QgsFeatureRequest

    expected_geom = rebuild_geometry(event.geometry_wkb) \
        if event.geometry_wkb is not None else None
    if not is_geometry_present(expected_geom):
        flog(f"_diagnose_snapshot_miss: eid={event.event_id} no expected "
             f"geom, cannot diagnose", "WARNING")
        return

    expected_attrs = reconstruct_attributes(event)
    fields = layer.fields()
    relevant = [
        (hist_name, fields.indexOf(hist_name))
        for hist_name in expected_attrs.keys()
        if not is_layer_audit_field(hist_name) and fields.indexOf(hist_name) >= 0
    ]

    request = QgsFeatureRequest()
    if (hasattr(request, "setFilterRect") and hasattr(expected_geom, "boundingBox")):
        request.setFilterRect(expected_geom.boundingBox())
    source = get_feature_source(layer)

    _DIAG_SCAN_CAP = 100
    candidates: List[tuple] = []
    scanned = 0
    try:
        for feature in source(request):
            scanned += 1
            if scanned > _DIAG_SCAN_CAP:
                break
            if not feature_matches_geometry(feature, expected_geom):
                continue
            diffs = []
            for hist_name, idx in relevant:
                actual = feature[idx]
                expected = expected_attrs.get(hist_name)
                if actual != expected:
                    diffs.append(
                        f"{hist_name}: actual={actual!r} expected={expected!r}"
                    )
            candidates.append((feature.id(), diffs))
    except Exception as exc:
        flog(f"_diagnose_snapshot_miss: scan failed "
             f"eid={event.event_id}: {exc}", "WARNING")
        return

    if not candidates:
        flog(f"_diagnose_snapshot_miss: eid={event.event_id} "
             f"0 geom-only candidates in bbox "
             f"identity_fid={_identity_fid(event)}", "WARNING")
        return

    flog(f"_diagnose_snapshot_miss: eid={event.event_id} "
         f"{len(candidates)} geom-only candidate(s) "
         f"identity_fid={_identity_fid(event)}", "WARNING")
    for fid, diffs in candidates[:5]:
        if not diffs:
            flog(f"  fid={fid} attrs all match (audit-field exclusion?)",
                 "WARNING")
        else:
            flog(f"  fid={fid} attr_diffs={diffs[:5]}", "WARNING")


def _identity_fid(event: AuditEvent) -> Optional[int]:
    """Return the historical FID stored in feature_identity_json, or None."""
    try:
        identity = json.loads(event.feature_identity_json or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(identity, dict):
        return None
    fid = identity.get("fid")
    return fid if isinstance(fid, int) else None


class _Proof(tuple):
    """``(ok, reason)`` that also carries the identity level (``.level``).

    Unpacks as the 2-tuple every caller of `_verify_update_target` expects,
    so the executor's rewind path keeps working unchanged while the event
    path reports the level.
    """

    def __new__(cls, ok: bool, reason: str, level: str):
        obj = super().__new__(cls, (ok, reason))
        obj.level = level
        return obj


def _verify_update_target(layer, fid: int, event: AuditEvent,
                          identity: Dict[str, Any],
                          field_mapping: Dict[str, str],
                          baseline: Optional[_Baseline] = None) -> tuple:
    """Can we PROVE that feature *fid* is the one this event describes?

    Returns ``(ok, reason)`` (a `_Proof`, whose ``.level`` is the identity
    level reached).

    A FID is not an identity on shapefiles and GeoJSON: OGR renumbers records
    after a deletion or a re-export, so the feature sitting at the historical
    FID may be a completely unrelated object. Writing the event's OLD state
    onto it destroys that object's data -- the worst possible outcome for a
    recovery tool, and a silent one.

    A primary key (pk_field/pk_value captured) is not an identity either
    until the content agrees: keys get reused and reassigned. So the feature
    must carry the event's NEW state whatever designated it -- NEW geometry
    when captured, NEW value of every changed field -- and, on a FID
    nomination, that state must discriminate (`_prove_update_state`): one
    shared boolean is not a proof. On a key nomination it must discriminate
    too, at the *baseline* (the layer the user left), or the journal must
    prove the key never changed hands. Only STRONG and MEDIUM pass.

    When nothing at all can be compared (legacy event with neither a NEW
    geometry nor a NEW attribute side) the target stays unproven and the
    caller refuses. Refusing loses a restore; applying corrupts a feature.

    *identity* is the one the caller resolved (`_event_identity`): a key
    it names, single or composite, is a ``pk`` nomination.
    """
    via = _identity_via(identity)
    level, why = _prove_update_state(layer, fid, event, "new",
                                     field_mapping, via, baseline)
    ok = level in WRITABLE_LEVELS
    return _Proof(ok, why if ok else f"{why} identity_level={level}", level)


def restore_updated_feature(layer, event: AuditEvent,
                            fid_cache: Optional[Dict] = None,
                            journal=None) -> Dict[str, Any]:
    """Revert a modified feature to its pre-update state.

    Uses the old values from the delta to update the current feature. The
    target must be proven (`_verify_update_target`), the geometry vetted
    (`_geometry_for_write`), and the write goes through `_provider_update`:
    all or nothing, then read back.

    *journal*: a read connection to the journal *event* comes from. With
    it, a key the journal proves continuous is corroborated by a value
    other features share (`_key_continuity`), and an event a trace
    already compensates is refused ``already_compensated`` instead of
    being written over a later edit (`_compensation_refusal`). Without it
    (``None``, the default) only a rare content corroborates a key -- the
    ``key_continuity_unproven`` refusal stays -- and a second restore of
    a compensated event is not detected.
    """
    fid_cache = _operation_cache(fid_cache, journal)
    dirty = _dirty_session_refusal(
        validate_restore_layer_state(layer),
        f"restore_updated[{event.event_id or 0}]")
    if dirty is not None:
        return dirty

    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    strength = get_identity_strength_for_layer(layer)

    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity for restore"}
    identity = _event_identity(layer, event)

    eid = event.event_id or 0
    context = f"restore_updated[{eid}]"
    try:
        nominated, _via = _lookup_identity(layer, identity, fid_cache)
    except TargetScanError as exc:
        flog(f"{context}: status=FAILED reason_code=scan_failed {exc}", "ERROR")
        return {"success": False, "reason_code": "scan_failed",
                "identity_level": ID_UNRESOLVABLE,
                "message": (f"Layer read failed while looking for the target "
                            f"({exc}); nothing written")}
    if not nominated:
        flog(f"{context}: target not found "
             f"identity={(event.feature_identity_json or '')[:80]}", "WARNING")
        return {"success": False, "message": "Target feature not found"}
    if len(nominated) > 1:
        flog(f"{context}: key designates {nominated[:5]} status=refused "
             f"reason_code=target_ambiguous", "ERROR")
        return {"success": False, "reason_code": "target_ambiguous",
                "identity_level": ID_AMBIGUOUS,
                "message": "Target key is held by several features; "
                           "refusing to overwrite any of them"}
    target_fid = nominated[0]

    old_attrs = reconstruct_attributes(event)
    field_mapping = _build_safe_mapping(check.drift, event)

    baseline = _Baseline(cache=fid_cache)
    proven, why = proof = _verify_update_target(
        layer, target_fid, event, identity, field_mapping, baseline)
    if not proven:
        scan_failed = why.startswith("scan_failed")
        if not scan_failed:
            # A replay (double click, relaunch after a partial run) finds
            # the feature already at OLD: it no longer carries NEW, so the
            # proof above fails, yet writing OLD over OLD changes nothing.
            # Only a DISCRIMINATING proof of the OLD state makes it a skip
            # (same rule as the rewind, `_target_already_at_old`); a shared
            # value stays a refusal.
            old_level, old_why = _prove_update_state(
                layer, target_fid, event, "old", field_mapping,
                _identity_via(identity), baseline)
            if old_level in WRITABLE_LEVELS:
                flog(f"{context}: target_fid={target_fid} already_at_old "
                     f"({old_why}) status=SKIPPED_IDEMPOTENT")
                return {"success": True, "skipped": True,
                        "reason_code": "already_at_old", "fid": target_fid,
                        "identity_level": old_level,
                        "message": "Already at the pre-edit state; "
                                   "nothing to do"}
        flog(f"{context}: target_fid={target_fid} "
             f"unverified reason={why} status=refused "
             f"(FID may point at an unrelated feature)", "ERROR")
        return {
            "success": False,
            "reason_code": "scan_failed" if scan_failed else (
                "target_ambiguous" if proof.level == ID_AMBIGUOUS
                else "target_unverifiable"),
            "identity_level": proof.level,
            "message": (
                "Target feature could not be verified "
                f"({why}); refusing to overwrite it"
            ),
        }

    flog(f"{context}: target_fid={target_fid} verified_by={why} "
         f"identity_level={proof.level}")
    # Proven and about to write: an event a trace already compensates is
    # restored a second time only over a LATER edit (`_compensation_refusal`).
    refusal = _compensation_refusal(fid_cache, event, context)
    if refusal is not None:
        refusal["identity_level"] = proof.level
        return refusal
    specs = _time_specs(layer, _captured_from(event, "old"), fid_cache)
    refusal = _legacy_datetime_refusal(
        field_mapping, old_attrs if isinstance(old_attrs, dict) else {},
        layer.dataProvider().fields(), event, specs, context)
    if refusal is not None:
        refusal["identity_level"] = proof.level
        return refusal
    attr_changes = _build_attribute_changes(layer, target_fid, old_attrs,
                                            field_mapping, event, specs)

    provider = layer.dataProvider()
    has_geom = event.geometry_wkb is not None
    if has_geom and not bool(provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
        return {"success": False, "message": "Provider lacks geometry change capability"}

    # Vet the geometry BEFORE touching the feature: a refusal must leave
    # the attributes untouched too, otherwise the feature ends up in a
    # state that existed at no point in its history.
    geom_error = _legacy_empty_blob_refusal(
        layer, event, event.geometry_wkb, eid, "restore_updated")
    geom = None
    if geom_error is None:
        geom, geom_error = _geometry_for_write(
            layer, event, event.geometry_wkb, eid, "restore_updated")
    if geom_error is not None:
        # Both types on one line: the user needs to know what the event
        # carries AND what the layer accepts to understand the refusal.
        flog(f"{context}: geometry refused "
             f"reason_code={geom_error.get('reason_code')} "
             f"event_geometry_type={event.geometry_type!r} "
             f"layer_wkb_type={_layer_wkb_type_repr(layer)}", "WARNING")
        return geom_error

    result = _provider_update(layer, target_fid,
                              attr_changes.get(target_fid, {}), geom, context,
                              cache=fid_cache)
    result["identity_level"] = proof.level
    if not result["success"]:
        return result

    geom_status = "geom_restored" if geom is not None else "no_geom"
    flog(f"{context}: OK fid={target_fid} "
         f"attr_changes={len(attr_changes.get(target_fid, {}))} "
         f"{geom_status} (read back)")
    return {"success": True, "message": "Reverted", "fid": target_fid,
            "identity_level": proof.level}


def _cap_change_features():
    """The provider capability for a one-call attributes+geometry write."""
    try:
        from qgis.core import QgsVectorDataProvider
        return QgsVectorDataProvider.Capability.ChangeFeatures
    except AttributeError:
        return None


def _provider_update(layer, fid: int, attr_values: Dict[int, Any], geom,
                     context: str,
                     cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Write *attr_values* and *geom* on *fid* through the provider, all or
    nothing, then read the feature back.

    The attribute and geometry writes used to be two provider calls, each
    its own transaction, attributes first: a geometry the provider refused
    (a CHECK constraint, a trigger, a lock hit between the two calls) left
    the attributes reverted and the geometry not, the event reported
    "failed", no trace written -- a half-restored feature the user believed
    untouched and no later restore knew about.

    Now: one ``changeFeatures`` call where the provider does it natively;
    otherwise the geometry first (the call most likely to be refused), the
    attributes second, and when the attributes are refused the geometry
    already written is put back and that revert is read back. Whatever the
    path, the feature is read back from the file:

      * refused, and the feature reads back as it was: ``provider_refused``;
      * refused, and the feature reads back modified (the revert did not
        land): ``failed_postcommit_verification`` with ``landed`` set, so
        the caller journals it;
      * accepted, and the feature does not hold what was written (trigger,
        narrowed column): ``failed_postcommit_verification``, ``landed``.

    *attr_values* is keyed by PROVIDER field index (`_build_attribute_changes`):
    the provider writes and reads its stored columns only. The whole
    pre-image is recorded in the batch *cache* before the write
    (`_record_pre_image`): only some columns are written, the others stay
    the user's and still count in the rarity of the events that follow.
    """
    from qgis.core import QgsGeometry

    provider = layer.dataProvider()
    fields = provider.fields()
    try:
        pre = _read_one(layer, fid, f"{context} pre-image", provider_only=True)
    except TargetScanError as exc:
        return {"success": False, "reason_code": "scan_failed",
                "message": (f"Layer read failed before the write ({exc}); "
                            f"nothing written")}
    if pre is None:
        return {"success": False, "reason_code": "target_absent",
                "message": "Target vanished before the write; nothing written"}
    _record_pre_image(cache, fid, pre)
    pre_attrs = {fields.at(idx).name(): pre[idx] for idx in attr_values}
    pre_geom = QgsGeometry(pre.geometry()) if geom is not None else None
    intended = {fields.at(idx).name(): value
                for idx, value in attr_values.items()}

    change_features = _cap_change_features()
    one_call = (bool(attr_values) and geom is not None
                and change_features is not None
                and bool(provider.capabilities() & change_features))
    refused = None
    if one_call:
        if not provider.changeFeatures({fid: attr_values}, {fid: geom}):
            refused = "attributes+geometry"
    else:
        if geom is not None and not provider.changeGeometryValues({fid: geom}):
            refused = "geometry"
        elif attr_values and not provider.changeAttributeValues(
                {fid: attr_values}):
            refused = "attributes"
            if geom is not None:
                provider.changeGeometryValues({fid: pre_geom})

    if refused:
        # The file knows exactly why it said no -- "NOT NULL constraint
        # failed: zones.code" names the blocking column. Dropping that for
        # a bare "Attribute update failed" leaves the user with nothing to
        # act on, so provider.errors() is surfaced as is.
        errors = provider.errors()
        msg = "; ".join(errors) if errors else f"{refused} update failed"
        left = _readback_written(layer, fid, pre_attrs, pre_geom,
                                 f"{context} refusal read-back")
        if left:
            flog(f"{context}: {refused} write refused fid={fid} msg={msg} "
                 f"AND the feature was left modified: {left[:4]} "
                 f"status=FAILED reason_code=failed_postcommit_verification",
                 "CRITICAL")
            return {"success": False,
                    "reason_code": "failed_postcommit_verification",
                    "landed": True, "fid": fid,
                    "message": (f"{msg}; the feature was left partly "
                                f"modified: {'; '.join(left[:4])}")}
        reverted = (" (geometry change reverted, verified)"
                    if refused == "attributes" and geom is not None else "")
        flog(f"{context}: {refused} write refused fid={fid} msg={msg}"
             f"{reverted}", "ERROR")
        return {"success": False, "reason_code": "provider_refused",
                "message": f"{msg}{reverted}"}

    diffs = _readback_written(layer, fid, intended, geom,
                              f"{context} read-back")
    if diffs:
        flog(f"{context}: status=FAILED reason_code="
             f"failed_postcommit_verification fid={fid} diffs={diffs[:5]}",
             "ERROR")
        return {"success": False,
                "reason_code": "failed_postcommit_verification",
                "landed": True, "fid": fid,
                "message": (f"Written, but the provider holds something "
                            f"else: {'; '.join(diffs[:4])}")}
    return {"success": True, "fid": fid}


def validate_restore_layer_state(layer) -> Optional[str]:
    if layer is None:
        return "Target layer not found"
    if hasattr(layer, 'isEditable') and layer.isEditable():
        if hasattr(layer, 'isModified') and layer.isModified():
            return "Target layer has uncommitted edits; commit or rollback before restore"
    return None


def build_fid_cache(layer, events: List[AuditEvent], journal=None) -> Dict:
    """Pre-resolve PK-based identities to FIDs in one batched query per PK field.

    Returns dict mapping (pk_field, pk_value_str) -> fid.
    Events without PK identity are skipped (fallback to per-event lookup),
    and so are a key the layer does not prove (`_event_identity`) and a
    composite key (looked up per event, on all its columns).

    A value held by SEVERAL features is left out of the cache: cached, it
    answered with whichever feature came last, and the per-event lookup
    that reports the ambiguity (``key_not_unique``) never ran.

    The dict is also the batch cache of the operation: the pre-images of
    what it writes (`_record_pre_image`) and, when *journal* (a read
    connection to the journal the events come from) is given, the evidence
    that a key never changed hands (`_key_continuity`).
    """
    from qgis.core import QgsFeatureRequest, QgsExpression
    from collections import defaultdict

    cache: Dict = _operation_cache(None, journal)
    shared: set = set()
    pk_groups: Dict[str, list] = defaultdict(list)

    for event in events:
        if event.operation_type == "DELETE":
            continue
        identity = _event_identity(layer, event)
        pk_field = identity.get("pk_field")
        pk_value = identity.get("pk_value")
        if pk_field and pk_value is not None:
            pk_groups[pk_field].append(pk_value)

    provider = layer.dataProvider()
    for pk_field, pk_values in pk_groups.items():
        unique_values = list(set(str(v) for v in pk_values))
        if not unique_values:
            continue
        escaped_field = QgsExpression.quotedColumnRef(str(pk_field))
        value_list = ", ".join(QgsExpression.quotedValue(v) for v in unique_values)
        expr = QgsExpression(f"{escaped_field} IN ({value_list})")
        if expr.hasParserError():
            flog(f"build_fid_cache: parse error for {pk_field}: {expr.parserErrorString()}", "WARNING")
            continue
        field_idx = layer.fields().indexOf(pk_field)
        request = QgsFeatureRequest(expr)
        if field_idx >= 0:
            request.setSubsetOfAttributes([field_idx])
        request.setFlags(QgisCompat.NO_GEOMETRY)
        for feat in provider.getFeatures(request):
            key = (pk_field, str(feat[pk_field]))
            if key in cache and cache[key] != feat.id():
                shared.add(key)
            cache[key] = feat.id()

    for key in shared:
        flog(f"build_fid_cache: key {key[0]}={key[1]!r} is held by several "
             f"features; not cached (the per-event lookup names the "
             f"ambiguity)", "WARNING")
        cache.pop(key, None)
    return cache


# -----------------------------------------------------------------------
# Dispatch tables (BLK-02). Declared lazily via accessor functions because
# the UNDO table references _undo_update_restore / _undo_insert_restore
# which are defined further down. Module-level eager construction would
# work but keeping the lookup behind helper functions also makes the
# intent explicit: all 3 operations must stay covered in both directions.
# -----------------------------------------------------------------------

def _restore_dispatch():
    """Return {operation_type: handler(layer, event, fid_cache) -> dict}."""
    return {
        "DELETE": restore_deleted_feature,
        "UPDATE": restore_updated_feature,
        "INSERT": restore_inserted_feature,
    }


def _unsupported_result(operation_type: str) -> Dict[str, Any]:
    return {"success": False, "message": f"Unsupported: {operation_type}"}


def _classify_restore_result(result: Dict[str, Any]) -> str:
    """Map a restore step result dict to a stable reason code.

    See `core.restore_executor` for the reason code constants. The
    classification prefers the explicit `reason_code` field when present
    (BL-RW-P0-02 sites); otherwise it falls back to STABLE substrings of
    the `message` field plus the `status` field, so legacy callers that
    did not yet tag results still classify correctly.

    Returned values are mutually exclusive and stable:
        "applied", "applied_partial", "skipped_idempotent",
        "target_absent", "geometry_drift", "failed".
    """
    reason_code = (result.get("reason_code") or "").strip()
    if reason_code:
        if reason_code in ("absent_vs_insert_comp", "insert_target_gone"):
            # insert_target_gone: undoing an INSERT whose feature is no
            # longer in the layer. The end state is already reached and the
            # occupant of its recycled FID was deliberately spared, so this
            # belongs with the idempotent skips -- counting it as a failure
            # told users their rewind had failed on work that was done.
            return "skipped_idempotent"
        # RLU-053: a write that had to abandon captured fields is not a
        # full restore. It keeps its own bucket so it can never be
        # counted next to the complete ones.
        if reason_code == "applied_partial":
            return "applied_partial"
        # BL-RW-P5-28: "already at the pre-edit state" is work already
        # done, not a failure -- same reasoning as insert_target_gone.
        if reason_code == "already_at_old":
            return "skipped_idempotent"
        # target_unverifiable (BL-RW-P1-25) is a refusal that returns
        # BEFORE any buffer mutation: classifying it "failed" made the
        # strict runner roll back whole layers (134 events lost on the
        # 2026-07-05 run). Soft-classify with target_absent.
        #
        # BL-RW-P5-28 measured what that shared name costs a reader: told
        # "59 features absent", the user had 58 present and merely
        # unprovable. The distinction belongs in the REPORT, not here --
        # renaming the class breaks `rw_apply_perf::unverifiable_is_soft`,
        # which pins this contract precisely because the softness matters
        # more than the wording. The precise cause is already carried by
        # `result["message"]` ("...could not be verified (post_geom_
        # mismatch)"); surfacing it up to CYCLE_SUMMARY needs its own
        # bucket in the six-way breakdown, and that is its own change.
        if reason_code in ("target_absent", "identity_mismatch_fid_only",
                           "target_unverifiable", "target_ambiguous",
                           "legacy_datetime_refused",
                           "already_compensated"):
            # target_ambiguous (v7): the same kind of refusal -- nothing
            # written, several features satisfied the proof -- so the same
            # soft class; the message names the candidates.
            # legacy_datetime_refused (v7): returned before any write too
            # (`_legacy_datetime_refusal`); the message names the fields.
            # already_compensated (v7): a Mode A restore of an event a
            # trace already compensates, refused before any write
            # (`_compensation_refusal`); the message names the trace and
            # the later edit.
            return "target_absent"
        # v7: a read that failed proves nothing, and a write the read-back
        # contradicts is not a restore. Both are hard failures: the
        # strict runner must roll the layer back rather than commit
        # around them.
        if reason_code in ("buffer_refused", "scan_failed",
                           "failed_postcommit_verification", "pk_conflict",
                           "provider_refused"):
            return "failed"
        # v7: a geometry that cannot be placed (the capture CRS story is
        # undecided or unusable, the WKB unreadable, the type not the
        # layer's) is a hard failure too, named here and not left to the
        # message parser below: its class used to depend on the wording
        # of the message, and a refusal whose text happened to say
        # "target feature absent", or "drift" and "tolerance", became a
        # soft one the strict runner commits around.
        if reason_code in ("crs_ambiguous", "crs_mismatch", "corrupt_wkb",
                           "geometry_type_mismatch"):
            return "failed"
        # v7: the only feature carrying the INSERT snapshot is one this
        # very rewind re-inserted (`restore_executor._buffer_delete`).
        # Committing around the refusal would write the resurrection the
        # rewind could not justify: hard, the layer is rolled back.
        if reason_code == "target_reinserted_in_batch":
            return "failed"
        # The undo found the feature already at its post-edit state.
        if reason_code == "already_at_new":
            return "skipped_idempotent"
        if reason_code in ("geometry_drift",):
            return "geometry_drift"
        # Unknown reason_code: fall through to legacy parsing.

    msg = (result.get("message") or "").lower()
    status = (result.get("status") or "")
    skipped = bool(result.get("skipped"))
    success = bool(result.get("success"))

    if status == "SKIPPED_GEOMETRY_DRIFT" or (
        "drift" in msg and "tolerance" in msg
    ):
        return "geometry_drift"
    if any((
        "target feature absent" in msg,
        "feature already absent" in msg,
        "identity mismatch" in msg,
    )):
        return "target_absent"
    if success and skipped:
        return "skipped_idempotent"
    if success:
        return "applied"
    return "failed"


def restore_batch(layer, events: List[AuditEvent],
                  fid_cache: Optional[Dict] = None,
                  trace_id: str = "", journal=None) -> RestoreReport:
    """Restore a batch of events with per-entity error isolation.

    BL-RW-P3-17: trace_id is propagated from the runner so the breakdown
    summary log carries the same `[trace_id]` prefix as the rest of the
    rewind chain.

    Traces are built for the writes that happened, and only for them: a
    success that wrote nothing (a skip) gets none, a failure whose write
    landed in the file gets one -- and is named in ``report.landed``, so
    that no consumer has to infer "failed but written" from the traces.

    *fid_cache* is the batch cache (`build_fid_cache`): a runner that
    spreads one layer over several calls passes the same one, so that the
    pre-images recorded by a chunk keep the baseline of the next ones.
    *journal*: a read connection to the journal the events come from (see
    `build_fid_cache`). The handlers read it for two proofs: a key the
    journal proves continuous (`_key_continuity`), and an event a trace
    already compensates, refused ``already_compensated`` instead of being
    written over a later edit (`_compensation_refusal`). Without it
    (``None``, the default) both stay unavailable: the refusals they would
    lift remain, and a second restore of a compensated event is written.
    """
    layer_error = validate_restore_layer_state(layer)
    if layer_error:
        failed = {(e.event_id or 0): layer_error for e in events}
        return RestoreReport([], failed, len(events), ())
    if fid_cache is None:
        fid_cache = build_fid_cache(layer, events, journal=journal)
    elif journal is not None:
        _operation_cache(fid_cache, journal)
    succeeded = []
    failed = {}
    skipped_idempotent: List[int] = []
    failed_target_absent: List[int] = []
    failed_geometry_drift: List[int] = []
    landed: List[int] = []
    traces = []
    dispatch = _restore_dispatch()

    for event in events:
        eid = event.event_id or 0
        try:
            handler = dispatch.get(event.operation_type)
            if handler is None:
                result = _unsupported_result(event.operation_type)
            else:
                result = handler(layer, event, fid_cache)

            reason = _classify_restore_result(result)
            if reason == "target_absent":
                failed_target_absent.append(eid)
            elif reason == "geometry_drift":
                failed_geometry_drift.append(eid)
            elif reason == "skipped_idempotent":
                skipped_idempotent.append(eid)

            if result["success"]:
                succeeded.append(eid)
                # A trace records a WRITE: it neutralises the source event
                # for every later rewind. A skip wrote nothing ("Already
                # exists", a duplicate found, already at the target
                # state): whatever put the data in that state carries its
                # own trace, or none, and a trace here would claim a
                # restore that never ran.
                if not result.get("skipped"):
                    trace = build_restore_trace_event(event, layer)
                    if trace is not None:
                        traces.append(trace)
            else:
                failed[eid] = result["message"]
                if result.get("landed"):
                    # The write reached the file but the read-back
                    # contradicts it (or a refused write could not be
                    # reverted): the data changed, so it is journalled --
                    # the same rule as the rewind's committed mismatches --
                    # while the event is reported failed, never restored.
                    landed.append(eid)
                    trace = build_restore_trace_event(event, layer)
                    if trace is not None:
                        traces.append(trace)
                    flog(f"restore_batch: event {eid} failed after its write "
                         f"landed; journalled as a trace", "WARNING")
        except Exception as e:
            failed[eid] = str(e)
            prefix = f"[{trace_id}] " if trace_id else ""
            flog(f"{prefix}restore_batch: error on event {eid}: {e}", "ERROR")

    prefix = f"[{trace_id}] " if trace_id else ""
    flog(
        f"{prefix}restore_batch: breakdown succeeded={len(succeeded)} "
        f"failed={len(failed)} skipped_idempotent={len(skipped_idempotent)} "
        f"failed_target_absent={len(failed_target_absent)} "
        f"failed_geometry_drift={len(failed_geometry_drift)} "
        f"landed={len(landed)}",
        "INFO",
    )

    return RestoreReport(
        succeeded=succeeded,
        failed=failed,
        total_requested=len(events),
        trace_events=tuple(traces),
        skipped_idempotent=skipped_idempotent,
        failed_target_absent=failed_target_absent,
        failed_geometry_drift=failed_geometry_drift,
        landed=landed,
    )


class UndoBatchContext(NamedTuple):
    """Per-layer state an undo needs, built once and reused across chunks.

    Split out of `undo_restore_batch` so a runner can spread one layer over
    several event-loop ticks (BL-RW-P5-27) without rebuilding this state at
    every tick -- rebuilding it would not merely cost time, it would change
    the RESULT:

    - `ordered` is the RW-16 unwind order: the Rewind applies compensations
      newest-first, so the undo must walk them oldest-first, otherwise an
      UPDATE lands before the re-INSERT it depends on.
    - `fid_cache` maps the events to the FIDs observed BEFORE the first
      write of the batch. Rebuilt mid-batch it would see the FIDs the batch
      itself has just moved, and resolve later events onto the wrong rows.
    - `layer_error` is non-None when preflight refused the layer; the whole
      group then fails with that single message and nothing is applied.
    """

    ordered: List[AuditEvent]
    fid_cache: Dict
    layer_error: Optional[str] = None


def undo_prepare(layer, events: List[AuditEvent],
                 journal=None) -> UndoBatchContext:
    """Build the per-layer context an undo batch needs. Writes nothing.

    Called ONCE per layer, before the first `undo_apply_one`. Its
    `fid_cache` is also the batch cache: the pre-images of what the undo
    writes, and the *journal* (see `build_fid_cache`).
    """
    layer_error = validate_restore_layer_state(layer)
    if layer_error:
        return UndoBatchContext(ordered=[], fid_cache={},
                                layer_error=layer_error)

    ordered_events = list(reversed(events))
    fid_cache = build_fid_cache(layer, ordered_events, journal=journal)

    layer_name = layer.name() if layer and hasattr(layer, 'name') else '?'
    flog(f"undo_prepare: layer={layer_name!r} n_events={len(ordered_events)} "
         f"fid_cache_size={len(fid_cache)}")
    return UndoBatchContext(ordered=ordered_events, fid_cache=fid_cache,
                            layer_error=None)


def undo_apply_one(layer, event: AuditEvent,
                   context: UndoBatchContext,
                   journal=None) -> Dict[str, Any]:
    """Reverse ONE previously restored event. Never raises.

    UPDATE  -> re-apply post-edit ('new') attribute values + new_geometry_wkb
    DELETE  -> delete the feature that was re-inserted by the restore
    INSERT  -> re-insert the feature that was deleted by the restore

    Returns the usual ``{"success", "message", ...}`` dict; an exception is
    turned into a failure for THIS event so one bad row cannot abort the
    rest of the layer -- the isolation `undo_restore_batch` always had.

    *journal*: a read connection to the journal the events come from, the
    evidence that a key never changed hands (`_key_continuity`). Given
    here, it joins the layer's batch cache (``context.fid_cache``) for this
    event and the ones after it; `undo_prepare(journal=...)` gives it to the
    whole layer at once. ``None`` (the default) keeps whatever the context
    holds: without any journal, a key needs a rare content.
    """
    eid = event.event_id or 0
    try:
        if journal is not None:
            _operation_cache(context.fid_cache, journal)
        identity_short = (event.feature_identity_json or '')[:80]
        flog(f"undo_apply_one: dispatch op={event.operation_type} "
             f"eid={eid} identity={identity_short}")
        op = event.operation_type
        if op == "UPDATE":
            result = _undo_update_restore(layer, event, context.fid_cache)
        elif op == "DELETE":
            # The undo of a DELETE restore deletes the feature the restore
            # re-inserted: the compensation guard of a Mode A restore of an
            # INSERT must not refuse it (it is that compensation).
            result = _delete_inserted_feature(layer, event, context.fid_cache,
                                              compensation_guard=False)
        elif op == "INSERT":
            result = _undo_insert_restore(layer, event, context.fid_cache)
        else:
            result = _unsupported_result(op)

        flog(f"undo_apply_one: result op={event.operation_type} eid={eid} "
             f"success={result['success']} skipped={result.get('skipped', False)} "
             f"msg={result.get('message', '')!r}")
        return result
    except Exception as e:  # noqa: BLE001 - per-event isolation, by contract
        flog(f"undo_apply_one: error on event {eid}: {e}", "ERROR")
        return {"success": False, "message": str(e)}


def undo_restore_batch(layer, events: List[AuditEvent],
                       journal=None) -> RestoreReport:
    """Undo a previous restore by reversing each operation, in one call.

    Kept as the synchronous entry point for the callers that legitimately
    want the whole layer at once (workflow_service, validation scenarios).
    `UndoRunner` no longer uses it: holding the GUI thread for a whole
    layer froze QGIS for 118 s on a 108-event group (BL-RW-P5-27); it
    drives `undo_prepare` + `undo_apply_one` on a time budget instead.
    *journal*: see `build_fid_cache`.
    """
    context = undo_prepare(layer, events, journal=journal)
    if context.layer_error:
        failed = {(e.event_id or 0): context.layer_error for e in events}
        return RestoreReport([], failed, len(events), ())

    succeeded = []
    failed = {}
    landed: List[int] = []
    for event in context.ordered:
        eid = event.event_id or 0
        result = undo_apply_one(layer, event, context)
        if result["success"]:
            succeeded.append(eid)
        else:
            failed[eid] = result["message"]
            if result.get("landed"):
                landed.append(eid)

    return RestoreReport(
        succeeded=succeeded,
        failed=failed,
        total_requested=len(events),
        trace_events=(),
        landed=landed,
    )


def _undo_update_restore(layer, event: AuditEvent,
                         fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Reverse a previous UPDATE restore: put the post-edit state back.

    v7. The undo used to resolve its target from the event's ORIGINAL
    identity (or a max(fid) snapshot scan) and write the NEW state onto it
    with no proof at all: after a rewind that had relocated the write (the
    historical FID of a repacked shapefile holding another feature), the
    undo wrote the NEW attributes and geometry onto that other feature; and
    when the restored values had been edited since, the undo overwrote the
    later edit -- silently, since the tracker is suppressed during an undo.
    Now:

      * the target is the feature that carries what the restore WROTE (the
        OLD side of the event, `_resolve_update_target(side='old')`),
        nominated by the identity and, on a FID identity, relocated by a
        discriminating content scan;
      * a feature already back at the NEW state is a skip
        (``already_at_new``): there is nothing to undo;
      * anything else -- restored values edited since, a FID now holding
        another feature, several candidates, a failed read -- is a named
        refusal, never a write;
      * the NEW geometry goes through `_geometry_for_write` (decode, CRS of
        the capture into the layer CRS, geometry type) and the write through
        `_provider_update` (all or nothing, read back).
    """
    eid = event.event_id or 0
    context = f"undo_update[{eid}]"
    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    strength = get_identity_strength_for_layer(layer)
    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity"}
    identity = _event_identity(layer, event)

    field_mapping = _build_safe_mapping(check.drift, event)
    res = _resolve_update_target(layer, event, identity, field_mapping, "old",
                                 fid_cache, relocate=True, context=context)
    if not res.writable:
        if res.reason.startswith("scan_failed"):
            return _unresolved_result(res, "written")
        at_new = _resolve_update_target(layer, event, identity, field_mapping,
                                        "new", fid_cache, relocate=False,
                                        context=context)
        if at_new.writable:
            flog(f"UNDO_UPD eid={eid} fid={at_new.fid} already_at_new "
                 f"identity_level={at_new.level} status=SKIPPED_IDEMPOTENT")
            return {"success": True, "skipped": True,
                    "reason_code": "already_at_new", "fid": at_new.fid,
                    "identity_level": at_new.level,
                    "message": "Already at the post-edit state; nothing to undo"}
        flog(f"UNDO_UPD eid={eid} status=refused identity_level={res.level} "
             f"reason={res.reason} nominated={res.nominated} (the feature "
             f"does not carry what the restore wrote: edited since, or "
             f"another feature)", "ERROR")
        if res.absent:
            return {"success": False, "reason_code": "target_absent",
                    "identity_level": res.level,
                    "message": "Target feature not found"}
        return _unresolved_result(res, "written")

    target_fid = res.fid
    new_attrs = reconstruct_new_attributes(event)
    specs = _time_specs(layer, _captured_from(event, "new"), fid_cache)
    refusal = _legacy_datetime_refusal(
        field_mapping, new_attrs if isinstance(new_attrs, dict) else {},
        layer.dataProvider().fields(), event, specs, context)
    if refusal is not None:
        refusal["identity_level"] = res.level
        return refusal
    attr_changes = _build_attribute_changes(layer, target_fid, new_attrs,
                                            field_mapping, event, specs)

    provider = layer.dataProvider()
    new_geom_wkb = getattr(event, 'new_geometry_wkb', None)
    if new_geom_wkb is not None and not bool(
            provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
        flog(f"UNDO_UPD eid={eid} fid={target_fid} geom=no_cap", "WARNING")
        return {"success": False,
                "message": "Provider lacks geometry change capability"}
    geom_error = _legacy_empty_blob_refusal(layer, event, new_geom_wkb,
                                            eid, "undo_update")
    geom = None
    if geom_error is None:
        geom, geom_error = _geometry_for_write(layer, event, new_geom_wkb,
                                               eid, "undo_update")
    if geom_error is not None:
        return geom_error

    result = _provider_update(layer, target_fid,
                              attr_changes.get(target_fid, {}), geom, context,
                              cache=fid_cache)
    result["identity_level"] = res.level
    if not result["success"]:
        return result
    flog(f"UNDO_UPD eid={eid} fid={target_fid} identity_level={res.level} "
         f"attrs={len(attr_changes.get(target_fid, {}))} "
         f"new_geom={'yes' if geom is not None else 'none'} (read back)")
    return {"success": True, "message": "Undo reverted", "fid": target_fid,
            "identity_level": res.level}


def _undo_insert_restore(layer, event: AuditEvent,
                         fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Re-insert a feature that was deleted by a previous INSERT restore.

    v7: the same path as the restore of a DELETE (`_reinsert_feature`).
    It used to ignore the pre-check verdict (schema drift), write the WKB
    in the capture CRS, insert a feature WITHOUT geometry when the WKB was
    unreadable, add a copy on every replay, and report success without a
    look at what the provider stored.
    """
    provider = layer.dataProvider()
    if provider is None:
        return {"success": False, "message": "No data provider"}
    if not bool(provider.capabilities() & QgisCompat.CAP_ADD_FEATURES):
        return {"success": False, "message": "Provider lacks add capability"}

    eid = event.event_id or 0
    # `is None`, not `or`: the NULL marker is falsy, and it is a state (the
    # feature had no geometry), not a missing side to fall back from.
    geom_wkb = getattr(event, 'new_geometry_wkb', None)
    if geom_wkb is None:
        geom_wkb = event.geometry_wkb
    return _reinsert_feature(layer, event, f"undo_insert[{eid}]", "DELETE",
                             geom_wkb, "Re-inserted (undo delete)",
                             cache=fid_cache)


def build_restore_trace_event(source_event: AuditEvent, layer) -> Optional[AuditEvent]:
    """Build an audit event that traces a restore operation (RLU-044).

    The trace event records that a restore was performed, referencing
    the original event via restored_from_event_id.

    The trace is filed under the SOURCE event's datasource fingerprint,
    not one recomputed from the live layer. Every trace reader filters on
    ``datasource_fingerprint IN resolve_scope(<the event's fingerprint>)``;
    a recomputed form that differs (pre-v6 journal whose alias
    reconciliation was refused, relative vs absolute fingerprint mode, a
    layer found by equivalence rather than by id) filed the trace where
    no reader looks: the next rewind did not see it and compensated the
    same DELETE again (a duplicate), the forward hop found no trace. The
    live layer's fingerprint is only the fallback for an event that has
    none.
    """
    from datetime import datetime, timezone
    from .identity import (
        compute_datasource_fingerprint, compute_project_fingerprint,
        extract_layer_name, compute_entity_fingerprint,
    )
    from .user_identity import resolve_user_name
    from .sqlite_schema import CURRENT_SCHEMA_VERSION

    if source_event.event_id is None:
        return None

    now = datetime.now(timezone.utc).isoformat()
    _UNDO_OP = {"DELETE": "INSERT", "UPDATE": "UPDATE", "INSERT": "DELETE"}
    restore_op = _UNDO_OP.get(source_event.operation_type, "UPDATE")
    ref_json = json.dumps({"_restore_ref": source_event.event_id})

    return AuditEvent(
        event_id=None,
        project_fingerprint=compute_project_fingerprint(),
        datasource_fingerprint=(source_event.datasource_fingerprint
                                or compute_datasource_fingerprint(layer)),
        layer_id_snapshot=layer.id(),
        layer_name_snapshot=extract_layer_name(layer),
        provider_type=layer.dataProvider().name(),
        feature_identity_json=source_event.feature_identity_json,
        operation_type=restore_op,
        attributes_json=ref_json,
        geometry_wkb=None,
        geometry_type=source_event.geometry_type,
        crs_authid=source_event.crs_authid,
        field_schema_json=None,
        user_name=resolve_user_name(),
        session_id=None,
        created_at=now,
        restored_from_event_id=source_event.event_id,
        entity_fingerprint=compute_entity_fingerprint(source_event.feature_identity_json),
        event_schema_version=CURRENT_SCHEMA_VERSION,
    )


def _can_write(provider, operation_type: str) -> bool:
    caps = provider.capabilities()
    if operation_type == "DELETE":
        return bool(caps & QgisCompat.CAP_ADD_FEATURES)
    if operation_type == "UPDATE":
        return bool(caps & QgisCompat.CAP_CHANGE_ATTRIBUTE_VALUES)
    if operation_type == "INSERT":
        return bool(caps & QgisCompat.CAP_DELETE_FEATURES)
    return False


def _parse_identity(identity_json: str) -> Dict[str, Any]:
    try:
        identity = json.loads(identity_json)
    except (json.JSONDecodeError, TypeError):
        return {}
    return identity if isinstance(identity, dict) else {}


def _find_target_feature(layer, identity: Dict,
                         fid_cache: Optional[Dict] = None) -> Optional[int]:
    """Find the FID of the target feature using identity info.

    Lookup priority:
      1. fid_cache (batched PK resolution).
      2. PK-based search (pk_field + pk_value).
      3. Raw FID fallback - only when no PK was captured for this event.

    When a PK was captured at capture time, the FID fallback is intentionally
    refused: FIDs are not stable after commits on many providers (ogr,
    postgres without oid), so a stale FID could address a different feature.

    v7: a thin view of `_lookup_identity` for the callers that only ask
    "which FID". It designates, it does not prove: every write path proves
    the designated feature against the event before writing. A failed read
    and a key held by several features both answer None, logged, so that
    neither can be taken for a target; the write paths call
    `_lookup_identity` themselves to tell "not found" from "read failed".
    """
    try:
        fids, via = _lookup_identity(layer, identity, fid_cache)
    except TargetScanError as exc:
        flog(f"restore: identity lookup failed ({exc}); no target "
             f"designated", "WARNING")
        return None
    if len(fids) > 1:
        flog(f"restore: identity {via} designates several features "
             f"{fids[:5]}; no target designated", "WARNING")
        return None
    if not fids and via == "pk":
        # PK was expected but not found: do not fall back to FID, which is
        # unstable and could point at an unrelated feature.
        flog(f"restore: PK {_key_repr(identity)} not found; refusing FID "
             f"fallback (safety)", "DEBUG")
    return fids[0] if fids else None


def _build_safe_mapping(drift: Optional[DriftReport], event: AuditEvent) -> Dict[str, str]:
    """Thin shim kept for in-module call sites that already hold a drift.

    Delegates to schema_drift.safe_field_mapping so the mapping logic
    lives in one place across restore_service and restore_executor.
    """
    return safe_field_mapping(event, drift=drift)


def _build_attribute_changes(layer, fid: int, old_attrs: Dict,
                             mapping: Dict,
                             event: AuditEvent,
                             time_specs=None) -> Dict[int, Dict[int, Any]]:
    """Build the change dict for provider.changeAttributeValues().

    The values are decoded with the ``event_schema_version`` of *event*:
    date-time digits journalled before v6 are a local wall clock, later
    ones UTC, and only the version tells them apart. Decoded without it, a
    5.x value came back shifted by the UTC offset (15:30 for 14:30 in
    Paris in winter) -- which the read-back then reported as a mismatch.

    Indexed by PROVIDER field: the dict goes to the provider, which knows
    its stored columns only (a virtual or joined field of the layer is not
    one of them).

    *time_specs* (`_time_specs`): for a <= 5.x date-time, the spec the
    provider hands back; the caller has already refused what it cannot
    establish (`_legacy_datetime_refusal`).
    """
    fields = layer.dataProvider().fields()
    changes = {
        idx: value
        for idx, value in iter_mapped_attributes(
            mapping, old_attrs, fields, event.event_schema_version,
            time_specs)
    }
    if not changes:
        return {}
    return {fid: changes}
