"""Local restore service for RecoverLand (RLU-040 to RLU-043).

Restores audited features back into their source layers.
Handles DELETE restore (re-insert), UPDATE restore (revert attributes/geometry),
conflict detection, and batch operations with per-entity isolation.
"""
import json
from typing import List, Dict, Optional, NamedTuple, Any

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
)
from .identity import get_identity_strength_for_layer
from .support_policy import IdentityStrength
from .serialization import is_layer_audit_field, iter_mapped_attributes
from .logger import flog
from ..compat import QgisCompat


def _qgis_vals_equal(actual, expected) -> bool:
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
    """
    if actual == expected:
        return True
    try:
        from qgis.PyQt.QtCore import QDate, QDateTime, QTime
        if isinstance(actual, QDate) and not isinstance(actual, QDateTime):
            return actual.toString('yyyy-MM-dd') == expected
        if isinstance(actual, QDateTime):
            if _qdatetime_matches_str(actual, expected):
                return True
            return False
        if isinstance(actual, QTime):
            return actual.toString('HH:mm:ss') == expected
        if isinstance(expected, QDate) and not isinstance(expected, QDateTime):
            return expected.toString('yyyy-MM-dd') == actual
        if isinstance(expected, QDateTime):
            if _qdatetime_matches_str(expected, actual):
                return True
            return False
        if isinstance(expected, QTime):
            return expected.toString('HH:mm:ss') == actual
    except ImportError:
        pass
    return False


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


def pre_check_restore(layer, event: AuditEvent) -> PreCheckResult:
    """Validate that a restore operation is feasible on the target layer."""
    if layer is None:
        return PreCheckResult(False, "Target layer not found", None)

    provider = layer.dataProvider()
    if provider is None:
        return PreCheckResult(False, "No data provider", None)

    if not _can_write(provider, event.operation_type):
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


def _find_restore_duplicate(layer, event: AuditEvent) -> Optional[int]:
    """FID of a feature already carrying the whole snapshot of *event*.

    Idempotence proof for the re-insertion of a deleted feature. The PK
    guard in `restore_deleted_feature` cannot fire on a FID-only layer (a
    shapefile captures no PK), and cannot fire on a GPKG either: the
    re-insert receives a FRESH fid, so the historical one the guard looks
    for never comes back. Both replays therefore added one copy per
    click, each answering "Restored".

    Proof is deliberately strict -- geometry AND every captured non-null
    attribute -- and returns None as soon as there is nothing solid to
    compare (unreadable geometry, no comparable attribute). Claiming
    "already there" wrongly would leave the feature missing while
    reporting success, so absence of evidence must never be read as
    evidence of presence.
    """
    from qgis.core import QgsFeatureRequest

    expected_geom = (rebuild_geometry(event.geometry_wkb)
                     if event.geometry_wkb is not None else None)
    if event.geometry_wkb is not None and not is_geometry_present(expected_geom):
        return None

    attrs = reconstruct_attributes(event)
    if not isinstance(attrs, dict):
        attrs = {}
    fields = layer.fields()
    try:
        pk_indices = set(layer.dataProvider().pkAttributeIndexes())
    except (AttributeError, RuntimeError):
        pk_indices = set()
    comparable = []
    for name, value in attrs.items():
        idx = fields.indexOf(name)
        if any((value is None, is_layer_audit_field(name),
                idx < 0, idx in pk_indices)):
            continue
        comparable.append((name, idx))
    if expected_geom is None and not comparable:
        return None

    request = QgsFeatureRequest()
    if expected_geom is not None and hasattr(request, "setFilterRect"):
        request.setFilterRect(expected_geom.boundingBox())
    expected_wkb = bytes(expected_geom.asWkb()) if expected_geom else None
    source = get_feature_source(layer)
    try:
        for feature in source(request):
            if expected_geom is not None and not feature_matches_geometry(
                    feature, expected_geom, expected_wkb=expected_wkb):
                continue
            if all(_qgis_vals_equal(feature[idx], attrs[name])
                   for name, idx in comparable):
                return feature.id()
    except Exception as exc:  # noqa: BLE001 - never fail a restore on a scan
        flog(f"_find_restore_duplicate: scan failed "
             f"eid={event.event_id}: {exc}", "WARNING")
    return None


def _geometry_for_write(layer, event: AuditEvent, wkb: Optional[bytes],
                        eid: int, context: str) -> tuple:
    """Rebuild the geometry of *event* and make it safe to write on *layer*.

    Returns ``(geom, error)``:
      - ``(None, None)``  nothing to write: the event carries no geometry,
        or the target layer is a plain table (the geometric part of the
        event is then inert, the row is still restored);
      - ``(geom, None)``  the geometry may be written as-is;
      - ``(None, error)`` a ready-to-return failure dict: the geometry
        must NOT be written.

    Three refusals, ordered by what they cost the user. The rewind path
    (`restore_executor._buffer_update`) already knows the first one; the
    event-by-event path accepted what the rewind refused.

      1. decode - a truncated or corrupted blob rebuilds to nothing.
         Writing it wipes the geometry of the live feature, which is the
         worst outcome for a recovery tool (reason_code ``corrupt_wkb``,
         sibling of the rewind's ``rebuilt_empty``).
      2. CRS - the WKB holds raw coordinates of ``event.crs_authid``, the
         CRS of the layer at capture time. Reproject into the layer CRS,
         or refuse rather than send the feature thousands of kilometres
         away (reason_code ``crs_mismatch``).
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

    aligned, crs_status = geometry_for_layer_crs(
        geom, event.crs_authid, layer)
    if crs_status in ("reprojected", "transform_failed"):
        flog(f"{context}[{eid}]: crs_mismatch event_crs={event.crs_authid} "
             f"layer_crs={extract_crs_authid(layer)} action={crs_status}",
             "WARNING")
    if aligned is None:
        return None, {
            "success": False,
            "reason_code": "crs_mismatch",
            "message": (
                f"Geometry captured in {event.crs_authid} cannot be "
                f"converted to {extract_crs_authid(layer)}; nothing written"
            ),
        }
    geom = aligned

    if layer_geom_type != QgisCompat.GEOM_UNKNOWN:
        try:
            geom_type = geom.type()
        except (AttributeError, RuntimeError):
            geom_type = layer_geom_type
        if geom_type != layer_geom_type:
            flog(f"{context}[{eid}]: reason_code=geometry_type_mismatch "
                 f"event_geom_type={event.geometry_type} "
                 f"layer_geom_type={int(layer_geom_type)} status=refused",
                 "ERROR")
            return None, {
                "success": False,
                "reason_code": "geometry_type_mismatch",
                "message": (
                    f"Geometry of the event ({event.geometry_type}) is "
                    f"incompatible with the layer; nothing written"
                ),
            }

    return geom, None


def restore_deleted_feature(layer, event: AuditEvent) -> Dict[str, Any]:
    """Re-insert a deleted feature into the target layer.

    Returns dict with 'success', 'message', and optionally 'fid'.
    """
    eid = event.event_id or 0
    dirty = _dirty_session_refusal(
        validate_restore_layer_state(layer), f"restore_deleted[{eid}]")
    if dirty is not None:
        return dirty

    check = pre_check_restore(layer, event)
    if not check.can_restore:
        flog(f"restore_deleted[{eid}]: pre_check failed: {check.reason}", "WARNING")
        return {"success": False, "message": check.reason}

    identity = _parse_identity(event.feature_identity_json)
    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")
    if pk_field and pk_value is not None:
        existing_fid = _find_target_feature(layer, identity)
        if existing_fid is not None:
            flog(f"restore_deleted[{eid}]: skip, {pk_field}={pk_value} already exists fid={existing_fid}")
            return {"success": True, "message": "Already exists (skip)",
                    "skipped": True, "fid": existing_fid}

    # Second line of idempotence: replaying a restore must not add a copy.
    # Double-click, relaunch after a partial rewind, two users on the same
    # GPKG, resume after a crash -- replays are routine, and the PK guard
    # above is unreachable on the two most common file formats.
    duplicate_fid = _find_restore_duplicate(layer, event)
    if duplicate_fid is not None:
        flog(f"restore_deleted[{eid}]: skip, snapshot already present "
             f"fid={duplicate_fid} (replay of an applied restore)")
        return {"success": True, "skipped": True, "fid": duplicate_fid,
                "message": "Already exists (skip): feature already restored"}

    attrs = reconstruct_attributes(event)
    geom, geom_error = _geometry_for_write(
        layer, event, event.geometry_wkb, eid, "restore_deleted")
    if geom_error is not None:
        return geom_error
    field_mapping = _build_safe_mapping(check.drift, event)
    geom_str = 'yes' if geom else 'no'
    flog(
        f"restore_deleted[{eid}]: attrs={len(attrs)} keys,"
        f" geom={geom_str}, mapping={len(field_mapping)} fields"
    )

    from qgis.core import QgsFeature
    new_feature = QgsFeature(layer.fields())
    _apply_attributes(new_feature, layer.fields(), attrs, field_mapping)

    if geom is not None:
        new_feature.setGeometry(geom)

    provider = layer.dataProvider()
    success, added = provider.addFeatures([new_feature])
    if not success:
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Insert failed"
        flog(f"restore_deleted[{eid}]: addFeatures failed: {msg}", "ERROR")
        return {"success": False, "message": msg}

    new_fid = added[0].id() if added else None
    flog(f"restore_deleted[{eid}]: OK new_fid={new_fid}")
    return {"success": True, "message": "Restored", "fid": new_fid}


def restore_inserted_feature(layer, event: AuditEvent,
                             fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Undo an INSERT by deleting the inserted feature from the target layer.

    Lookup strategy for layers without a stable PK:
      1. Try the FID stored in the identity. Verify it matches the captured
         snapshot (geom + attrs). If yes, delete it.
      2. Otherwise, scan the layer for a feature that exactly matches the
         snapshot. This is needed when the provider has reused the original
         FID for another feature (shapefile recycles), or when the
         re-inserted feature received a fresh FID at commit time.
      3. Refuse only when neither path yields a confidently-identified
         target. This avoids the silent feature accumulation observed
         when repeated rewinds re-create the same feature each time.
    """
    if layer is None:
        return {"success": False, "message": "Target layer not found"}

    dirty = _dirty_session_refusal(
        validate_restore_layer_state(layer),
        f"restore_inserted[{event.event_id or 0}]")
    if dirty is not None:
        return dirty

    provider = layer.dataProvider()
    if provider is None:
        return {"success": False, "message": "No data provider"}

    if not bool(provider.capabilities() & QgisCompat.CAP_DELETE_FEATURES):
        return {"success": False, "message": "Provider lacks delete capability"}

    identity = _parse_identity(event.feature_identity_json)
    strength = get_identity_strength_for_layer(layer)

    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity for restore"}

    has_pk = bool(identity.get("pk_field")) and identity.get("pk_value") is not None
    target_fid = _find_target_feature(layer, identity, fid_cache)

    if has_pk:
        if target_fid is None:
            return {"success": False, "message": "Target feature not found"}
        return _delete_target_or_fail(provider, target_fid, event)

    if target_fid is not None and _verify_insert_snapshot(
            layer, target_fid, event):
        flog(f"restore_inserted_feature: matched by FID and snapshot "
             f"eid={event.event_id} fid={target_fid}")
        return _delete_target_or_fail(provider, target_fid, event)

    fallback_fid = _find_by_snapshot(layer, event, resolve_ambiguity=True)
    if fallback_fid is not None:
        flog(f"restore_inserted_feature: FID mismatch, recovered by "
             f"snapshot scan eid={event.event_id} "
             f"identity_fid={identity.get('fid')} "
             f"recovered_fid={fallback_fid}")
        return _delete_target_or_fail(provider, fallback_fid, event)

    flog(f"restore_inserted_feature: target absent (FID lookup miss and "
         f"no snapshot match) eid={event.event_id} "
         f"identity_fid={identity.get('fid')}", "WARNING")
    _diagnose_snapshot_miss(layer, event)
    return {"success": True, "message": "Already absent (no snapshot match)",
            "skipped": True}


def _delete_target_or_fail(provider, target_fid: int,
                           event: AuditEvent) -> Dict[str, Any]:
    if not provider.deleteFeatures([target_fid]):
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Delete failed"
        flog(f"restore_inserted_feature: deleteFeatures failed "
             f"eid={event.event_id} fid={target_fid} msg={msg}", "ERROR")
        return {"success": False, "message": msg}
    return {"success": True, "message": "Deleted (undo insert)",
            "fid": target_fid}


def _find_by_snapshot(
    layer, event: AuditEvent,
    resolve_ambiguity: bool = False,
    exclude_fids: Optional[set] = None,
) -> Optional[int]:
    """Scan the layer for the single feature that matches event's snapshot.

    When *resolve_ambiguity* is False (default): returns the FID of the
    unique match, or None when no feature matches or several do (ambiguous:
    refuse to act blindly).

    When *resolve_ambiguity* is True: in case of multiple strict matches
    (geometry + every captured attribute identical), returns the highest
    FID, which is the most recently inserted feature and therefore the
    duplicate created by a previous rewind. The risk of destroying
    unrelated data is bounded because matches are strictly identical on
    geometry AND every attribute, so the kept feature carries the same
    semantics as the deleted one.

    When *exclude_fids* is provided, features whose ``id()`` is in the set
    are skipped during the scan. Used by ``_buffer_delete`` to avoid
    matching freshly-inserted compensatory features (their geometry equals
    the OLD snapshot of the user DELETE event and can mascarade as the
    INSERT target we want to undo).

    Strict matching is used because callers may use the result for a
    *destructive* operation (undo of a previous insert).
    """
    from qgis.core import QgsFeatureRequest

    expected_geom = rebuild_geometry(event.geometry_wkb) \
        if event.geometry_wkb is not None else None
    expected_attrs = reconstruct_attributes(event)
    fields = layer.fields()
    pk_field_indices = set(layer.dataProvider().pkAttributeIndexes())
    relevant = [
        (hist_name, fields.indexOf(hist_name))
        for hist_name in expected_attrs.keys()
        if all((
            not is_layer_audit_field(hist_name),
            fields.indexOf(hist_name) >= 0,
            fields.indexOf(hist_name) not in pk_field_indices,
        ))
    ]
    pk_skipped = [
        hist_name for hist_name in expected_attrs.keys()
        if fields.indexOf(hist_name) in pk_field_indices
    ]

    matches: List[int] = []
    request = QgsFeatureRequest()
    has_geom_filter = all((
        expected_geom is not None,
        hasattr(request, "setFilterRect"),
        hasattr(expected_geom, "boundingBox"),
    ))
    if has_geom_filter:
        request.setFilterRect(expected_geom.boundingBox())
    # BL-RW-P1-26: serialise the expected geometry once per scan, not
    # once per scanned feature (heavy polygons made this dominate the
    # rewind apply phase).
    expected_wkb = None
    if expected_geom is not None and hasattr(expected_geom, "asWkb"):
        try:
            expected_wkb = bytes(expected_geom.asWkb())
        except (TypeError, ValueError):
            expected_wkb = None
    source = get_feature_source(layer)
    scanned = 0
    _diffs: List[str] = []
    _null_skips: List[str] = []
    _geom_ok_fids: List[int] = []
    excluded_count = 0
    try:
        for feature in source(request):
            scanned += 1
            if exclude_fids and feature.id() in exclude_fids:
                excluded_count += 1
                continue
            if expected_geom is not None and not feature_matches_geometry(
                    feature, expected_geom, expected_wkb=expected_wkb):
                continue
            _geom_ok_fids.append(feature.id())
            attrs_ok = True
            for hist_name, idx in relevant:
                expected_val = expected_attrs.get(hist_name)
                if expected_val is None:
                    _null_skips.append(hist_name)
                    continue
                actual_val = feature[idx]
                if actual_val is None:
                    _null_skips.append(hist_name)
                    continue
                if not _qgis_vals_equal(actual_val, expected_val):
                    attrs_ok = False
                    av = repr(actual_val)[:30]
                    ev = repr(expected_val)[:30]
                    _diffs.append(f"f{feature.id()}:{hist_name}({av}\u2260{ev})")
                    break
            if attrs_ok:
                matches.append(feature.id())
                if not resolve_ambiguity and len(matches) > 1:
                    break
    except Exception as exc:
        flog(f"SNAP_SCAN eid={event.event_id} phase=geom SCAN_ERR={exc}", "WARNING")
        return None
    if excluded_count > 0:
        flog(f"SNAP_SCAN eid={event.event_id} phase=geom "
             f"excluded={excluded_count} fids (comp-INSERT collision guard)",
             "DEBUG")

    null_skip_set = list(set(_null_skips))[:4]
    flog(f"SNAP_SCAN eid={event.event_id} phase=geom "
         f"geom_filter={has_geom_filter} scanned={scanned} matches={len(matches)} "
         f"pk_skip={pk_skipped} null_skip={null_skip_set} "
         f"diffs={_diffs[:5] if _diffs else '[]'} result={matches[:5] if matches else 'none'}",
         "DEBUG" if matches else "INFO")

    if not matches and _geom_ok_fids and _diffs:
        _failing_fields = set(
            d.split(':')[1].split('(')[0] for d in _diffs if ':' in d
        )
        if len(_failing_fields) == 1:
            if len(_geom_ok_fids) == 1:
                _lenient_fid = _geom_ok_fids[0]
                flog(f"SNAP_SCAN eid={event.event_id} lenient_match: "
                     f"1 geom candidate fails on field "
                     f"'{next(iter(_failing_fields))}' → fid={_lenient_fid} accepted "
                     f"diffs={_diffs[:3]}", "WARNING")
                return _lenient_fid
            elif resolve_ambiguity and len(_geom_ok_fids) <= 5:
                _lenient_fid = max(_geom_ok_fids)
                flog(f"SNAP_SCAN eid={event.event_id} lenient_match+max_fid: "
                     f"{len(_geom_ok_fids)} geom candidates fail on field "
                     f"'{next(iter(_failing_fields))}' → chosen={_lenient_fid} "
                     f"fids={_geom_ok_fids[:5]} (RW-15)", "WARNING")
                return _lenient_fid
            else:
                flog(f"SNAP_SCAN eid={event.event_id} lenient_REFUSED: "
                     f"{len(_geom_ok_fids)} geom candidates fail on field "
                     f"'{next(iter(_failing_fields))}' → ambiguous, skip "
                     f"fids={_geom_ok_fids[:5]}", "WARNING")
                return None

    if scanned == 0 and has_geom_filter and not matches:
        _diffs_fb: List[str] = []
        _null_skips_fb: List[str] = []
        try:
            for feature in source(QgsFeatureRequest()):
                scanned += 1
                if exclude_fids and feature.id() in exclude_fids:
                    continue
                attrs_ok = True
                for hist_name, idx in relevant:
                    expected_val = expected_attrs.get(hist_name)
                    if expected_val is None:
                        _null_skips_fb.append(hist_name)
                        continue
                    actual_val = feature[idx]
                    if actual_val is None:
                        _null_skips_fb.append(hist_name)
                        continue
                    if not _qgis_vals_equal(actual_val, expected_val):
                        attrs_ok = False
                        av = repr(actual_val)[:30]
                        ev = repr(expected_val)[:30]
                        _diffs_fb.append(f"f{feature.id()}:{hist_name}({av}\u2260{ev})")
                        break
                if attrs_ok:
                    matches.append(feature.id())
                    if not resolve_ambiguity and len(matches) > 1:
                        break
        except Exception as exc:
            flog(f"SNAP_SCAN eid={event.event_id} phase=fallback SCAN_ERR={exc}", "WARNING")
        null_skip_fb_set = list(set(_null_skips_fb))[:4]
        flog(f"SNAP_SCAN eid={event.event_id} phase=fallback "
             f"scanned={scanned} matches={len(matches)} "
             f"pk_skip={pk_skipped} null_skip={null_skip_fb_set} "
             f"diffs={_diffs_fb[:5] if _diffs_fb else '[]'} result={matches[:5] if matches else 'none'}",
             "DEBUG" if matches else "INFO")
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        if resolve_ambiguity:
            chosen = max(matches)
            flog(f"_find_by_snapshot: ambiguity resolved by max-fid heuristic "
                 f"eid={event.event_id} candidates={matches[:5]} chosen={chosen} "
                 f"identity_fid={_identity_fid(event)}", "WARNING")
            return chosen
        flog(f"_find_by_snapshot: ambiguous, {len(matches)} matches found "
             f"eid={event.event_id} fids={matches[:5]}", "WARNING")
    return None


def _find_by_attrs_only(
    layer, event: AuditEvent,
    exclude_fids: Optional[set] = None,
    min_attrs: int = 2,
) -> Optional[int]:
    """Attribute-only rescue scan (BL-RW-P1-25).

    Used when the geometry-based ``_find_by_snapshot`` returns no candidate
    (e.g. WKB precision drift through OGR write cycles) but the feature may
    still exist under a drifted FID. Matches STRICTLY on every relevant
    captured attribute that is non-null in the event; a feature-side NULL
    counts as a mismatch (an empty feature must not match everything).

    Returns the FID of the unique match. Multiple matches are refused
    (no geometry available to bound the risk), fewer than *min_attrs*
    comparable attributes make the event non-discriminant and return None.
    """
    from qgis.core import QgsFeatureRequest

    expected_attrs = reconstruct_attributes(event)
    fields = layer.fields()
    pk_field_indices = set(layer.dataProvider().pkAttributeIndexes())
    relevant = [
        (hist_name, fields.indexOf(hist_name))
        for hist_name, val in expected_attrs.items()
        if all((
            val is not None,
            not is_layer_audit_field(hist_name),
            fields.indexOf(hist_name) >= 0,
            fields.indexOf(hist_name) not in pk_field_indices,
        ))
    ]
    if len(relevant) < min_attrs:
        flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
             f"non_discriminant n_attrs={len(relevant)} min={min_attrs}",
             "INFO")
        return None

    # BL-RW-P1-26: the scan only reads feature.id() and the relevant
    # attribute indexes; skip geometry materialisation (heavy polygons
    # cost ~10ms/feature) and non-relevant attributes.
    request = QgsFeatureRequest()
    try:
        request.setFlags(QgisCompat.NO_GEOMETRY)
        request.setSubsetOfAttributes([idx for _name, idx in relevant])
    except (AttributeError, TypeError):
        pass
    matches: List[int] = []
    scanned = 0
    source = get_feature_source(layer)
    try:
        for feature in source(request):
            scanned += 1
            if exclude_fids and feature.id() in exclude_fids:
                continue
            attrs_ok = True
            for hist_name, idx in relevant:
                actual_val = feature[idx]
                if actual_val is None or not _qgis_vals_equal(
                        actual_val, expected_attrs[hist_name]):
                    attrs_ok = False
                    break
            if attrs_ok:
                matches.append(feature.id())
                if len(matches) > 1:
                    break
    except Exception as exc:
        flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
             f"SCAN_ERR={exc}", "WARNING")
        return None

    if len(matches) == 1:
        flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
             f"scanned={scanned} n_attrs={len(relevant)} result={matches[0]} "
             f"(geometry drifted, strict attr match)", "WARNING")
        return matches[0]
    flog(f"SNAP_SCAN eid={event.event_id} phase=attr_rescue "
         f"scanned={scanned} n_attrs={len(relevant)} "
         f"matches={len(matches)} result=none"
         f"{' (ambiguous, refused)' if len(matches) > 1 else ''}",
         "INFO")
    return None


def _diagnose_snapshot_miss(layer, event: AuditEvent) -> None:
    """Log why _find_by_snapshot found 0 strict matches.

    Re-scans the layer using *geometry only* (same bbox prefilter) and,
    for every geom-only candidate, lists the historical attribute keys
    whose persisted value diverges from the captured snapshot. The first
    five candidates and the first five diverging attributes per candidate
    are reported. Side-effect free: only writes WARNING-level log lines.

    Used immediately after a snapshot lookup miss to expose the cause
    (typical: trigger-altered fields, NULL vs None, datetime serialization)
    without changing resolution logic.
    """
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


def _verify_insert_snapshot(layer, fid: int, event: AuditEvent) -> bool:
    """Return True when feature fid matches the event's INSERT snapshot."""
    from qgis.core import QgsFeatureRequest

    try:
        request = QgsFeatureRequest(fid)
        source = get_feature_source(layer)
        feature = next(iter(source(request)), None)
        if feature is None:
            return False
        if event.geometry_wkb is not None:
            expected = rebuild_geometry(event.geometry_wkb)
            if not feature_matches_geometry(feature, expected):
                return False
        attrs = reconstruct_attributes(event)
        fields = layer.fields()
        for hist_name, value in attrs.items():
            if is_layer_audit_field(hist_name):
                continue
            idx = fields.indexOf(hist_name)
            if idx < 0:
                continue
            if not _qgis_vals_equal(feature[hist_name], value):
                return False
        return True
    except Exception as exc:
        flog(f"_verify_insert_snapshot failed fid={fid}: {exc}", "WARNING")
        return False


def _verify_update_target(layer, fid: int, event: AuditEvent,
                          identity: Dict[str, Any],
                          field_mapping: Dict[str, str]) -> tuple:
    """Can we PROVE that feature *fid* is the one this event describes?

    Returns ``(ok, reason)``.

    A FID is not an identity on shapefiles and GeoJSON: OGR renumbers records
    after a deletion or a re-export, so the feature sitting at the historical
    FID may be a completely unrelated object. Writing the event's OLD state
    onto it destroys that object's data -- the worst possible outcome for a
    recovery tool, and a silent one.

    Proof, in order of strength:
      - a real primary key resolved the target: the FID never entered the
        picture, nothing to check;
      - the event captured a NEW geometry: the feature at *fid* must still
        carry it (same check the rewind executor runs);
      - attribute-only edit: the feature at *fid* must still carry the NEW
        values of every field the event changed.

    When nothing at all can be compared (legacy event with neither a NEW
    geometry nor a NEW attribute side) the target stays unproven and the
    caller refuses. Refusing loses a restore; applying corrupts a feature.
    """
    if identity.get("pk_field") and identity.get("pk_value") is not None:
        return True, "pk"

    from .restore_executor import _target_matches_update_post_state

    if getattr(event, "new_geometry_wkb", None) is not None:
        if _target_matches_update_post_state(layer, fid, event):
            return True, "post_geom"
        return False, "post_geom_mismatch"

    new_attrs = reconstruct_new_attributes(event)
    if not new_attrs:
        return False, "no_post_state_captured"
    feature = layer.getFeature(fid)
    if feature is None or not feature.isValid():
        return False, "target_unreadable"
    fields = layer.fields()
    compared = 0
    for hist_name, cur_name in field_mapping.items():
        if hist_name not in new_attrs:
            continue
        if fields.indexFromName(cur_name) < 0:
            continue
        compared += 1
        if not _qgis_vals_equal(feature[cur_name], new_attrs[hist_name]):
            return False, f"post_attr_mismatch:{cur_name}"
    if compared == 0:
        return False, "no_comparable_field"
    return True, f"post_attrs:{compared}"


def restore_updated_feature(layer, event: AuditEvent,
                            fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Revert a modified feature to its pre-update state.

    Uses the old values from the delta to update the current feature.
    """
    dirty = _dirty_session_refusal(
        validate_restore_layer_state(layer),
        f"restore_updated[{event.event_id or 0}]")
    if dirty is not None:
        return dirty

    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    identity = _parse_identity(event.feature_identity_json)
    strength = get_identity_strength_for_layer(layer)

    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity for restore"}

    target_fid = _find_target_feature(layer, identity, fid_cache)
    eid = event.event_id or 0
    if target_fid is None:
        flog(f"restore_updated[{eid}]: target not found "
             f"identity={(event.feature_identity_json or '')[:80]}", "WARNING")
        return {"success": False, "message": "Target feature not found"}

    old_attrs = reconstruct_attributes(event)
    field_mapping = _build_safe_mapping(check.drift, event)

    proven, why = _verify_update_target(
        layer, target_fid, event, identity, field_mapping)
    if not proven:
        flog(f"restore_updated[{eid}]: target_fid={target_fid} "
             f"unverified reason={why} status=refused "
             f"(FID may point at an unrelated feature)", "ERROR")
        return {
            "success": False,
            "reason_code": "target_unverifiable",
            "message": (
                "Target feature could not be verified "
                f"({why}); refusing to overwrite it"
            ),
        }

    flog(f"restore_updated[{eid}]: target_fid={target_fid} verified_by={why}")
    attr_changes = _build_attribute_changes(layer, target_fid, old_attrs, field_mapping)

    provider = layer.dataProvider()
    has_geom = event.geometry_wkb is not None
    if has_geom and not bool(provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
        return {"success": False, "message": "Provider lacks geometry change capability"}

    # Vet the geometry BEFORE touching the feature: a refusal must leave
    # the attributes untouched too, otherwise the feature ends up in a
    # state that existed at no point in its history.
    geom, geom_error = _geometry_for_write(
        layer, event, event.geometry_wkb, eid, "restore_updated")
    if geom_error is not None:
        # Both types on one line: the user needs to know what the event
        # carries AND what the layer accepts to understand the refusal.
        flog(f"restore_updated[{eid}]: geometry refused "
             f"reason_code={geom_error.get('reason_code')} "
             f"event_geometry_type={event.geometry_type!r} "
             f"layer_wkb_type={_layer_wkb_type_repr(layer)}", "WARNING")
        return geom_error

    if attr_changes:
        success = provider.changeAttributeValues(attr_changes)
        if not success:
            # The file knows exactly why it said no -- "NOT NULL
            # constraint failed: zones.code" names the blocking column.
            # Dropping that for a bare "Attribute update failed" leaves
            # the user with nothing to act on. restore_deleted_feature
            # has always surfaced provider.errors(); so does this path.
            errors = provider.errors()
            msg = "; ".join(errors) if errors else "Attribute update failed"
            flog(f"restore_updated[{eid}]: attribute write refused "
                 f"fid={target_fid} msg={msg}", "ERROR")
            return {"success": False, "reason_code": "provider_refused",
                    "message": msg}

    if geom is not None:
        geom_changes = {target_fid: geom}
        if not provider.changeGeometryValues(geom_changes):
            errors = provider.errors()
            msg = "; ".join(errors) if errors else "Geometry update failed"
            flog(f"restore_updated[{eid}]: geom update failed fid={target_fid} "
                 f"msg={msg}", "ERROR")
            return {"success": False, "reason_code": "provider_refused",
                    "message": msg}

    geom_status = "geom_restored" if geom is not None else "no_geom"
    flog(f"restore_updated[{eid}]: OK fid={target_fid} "
         f"attr_changes={len(attr_changes)} {geom_status}")
    return {"success": True, "message": "Reverted"}


def validate_restore_layer_state(layer) -> Optional[str]:
    if layer is None:
        return "Target layer not found"
    if hasattr(layer, 'isEditable') and layer.isEditable():
        if hasattr(layer, 'isModified') and layer.isModified():
            return "Target layer has uncommitted edits; commit or rollback before restore"
    return None


def build_fid_cache(layer, events: List[AuditEvent]) -> Dict:
    """Pre-resolve PK-based identities to FIDs in one batched query per PK field.

    Returns dict mapping (pk_field, pk_value_str) -> fid.
    Events without PK identity are skipped (fallback to per-event lookup).
    """
    from qgis.core import QgsFeatureRequest, QgsExpression
    from collections import defaultdict

    cache: Dict = {}
    pk_groups: Dict[str, list] = defaultdict(list)

    for event in events:
        if event.operation_type == "DELETE":
            continue
        identity = _parse_identity(event.feature_identity_json)
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
            cache[(pk_field, str(feat[pk_field]))] = feat.id()

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
        "DELETE": lambda layer, event, fid_cache:
            restore_deleted_feature(layer, event),
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
        if reason_code == "absent_vs_insert_comp":
            return "skipped_idempotent"
        # RLU-053: a write that had to abandon captured fields is not a
        # full restore. It keeps its own bucket so it can never be
        # counted next to the complete ones.
        if reason_code == "applied_partial":
            return "applied_partial"
        # target_unverifiable (BL-RW-P1-25) is a refusal that returns
        # BEFORE any buffer mutation: classifying it "failed" made the
        # strict runner roll back whole layers (134 events lost on the
        # 2026-07-05 run). Soft-classify with target_absent.
        if reason_code in ("target_absent", "identity_mismatch_fid_only",
                           "target_unverifiable"):
            return "target_absent"
        if reason_code == "buffer_refused":
            return "failed"
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
                  trace_id: str = "") -> RestoreReport:
    """Restore a batch of events with per-entity error isolation.

    BL-RW-P3-17: trace_id is propagated from the runner so the breakdown
    summary log carries the same `[trace_id]` prefix as the rest of the
    rewind chain.
    """
    layer_error = validate_restore_layer_state(layer)
    if layer_error:
        failed = {(e.event_id or 0): layer_error for e in events}
        return RestoreReport([], failed, len(events), ())
    if fid_cache is None:
        fid_cache = build_fid_cache(layer, events)
    succeeded = []
    failed = {}
    skipped_idempotent: List[int] = []
    failed_target_absent: List[int] = []
    failed_geometry_drift: List[int] = []
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
                trace = build_restore_trace_event(event, layer)
                if trace is not None:
                    traces.append(trace)
            else:
                failed[eid] = result["message"]
        except Exception as e:
            failed[eid] = str(e)
            prefix = f"[{trace_id}] " if trace_id else ""
            flog(f"{prefix}restore_batch: error on event {eid}: {e}", "ERROR")

    prefix = f"[{trace_id}] " if trace_id else ""
    flog(
        f"{prefix}restore_batch: breakdown succeeded={len(succeeded)} "
        f"failed={len(failed)} skipped_idempotent={len(skipped_idempotent)} "
        f"failed_target_absent={len(failed_target_absent)} "
        f"failed_geometry_drift={len(failed_geometry_drift)}",
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
    )


def undo_restore_batch(layer, events: List[AuditEvent]) -> RestoreReport:
    """Undo a previous restore by reversing each operation.

    UPDATE  -> re-apply post-edit ('new') attribute values + new_geometry_wkb
    DELETE  -> delete the feature that was re-inserted by the restore
    INSERT  -> re-insert the feature that was deleted by the restore

    Events are reversed before processing (RW-16): the Rewind applies
    compensations in reverse-chronological order; the undo must unwind
    them in forward-chronological order so that a re-INSERT precedes
    the UPDATE that depends on its existence.
    """
    layer_error = validate_restore_layer_state(layer)
    if layer_error:
        failed = {(e.event_id or 0): layer_error for e in events}
        return RestoreReport([], failed, len(events), ())
    succeeded = []
    failed = {}

    ordered_events = list(reversed(events))
    fid_cache = build_fid_cache(layer, ordered_events)

    layer_name = layer.name() if layer and hasattr(layer, 'name') else '?'
    flog(f"undo_restore_batch: layer={layer_name!r} n_events={len(ordered_events)} "
         f"fid_cache_size={len(fid_cache)}")
    for event in ordered_events:
        eid = event.event_id or 0
        try:
            identity_short = (event.feature_identity_json or '')[:80]
            flog(f"undo_restore_batch: dispatch op={event.operation_type} "
                 f"eid={eid} identity={identity_short}")
            op = event.operation_type
            if op == "UPDATE":
                result = _undo_update_restore(layer, event, fid_cache)
            elif op == "DELETE":
                result = restore_inserted_feature(layer, event, fid_cache)
            elif op == "INSERT":
                result = _undo_insert_restore(layer, event)
            else:
                result = _unsupported_result(op)

            skipped = result.get('skipped', False)
            flog(f"undo_restore_batch: result op={event.operation_type} eid={eid} "
                 f"success={result['success']} skipped={skipped} "
                 f"msg={result.get('message', '')!r}")
            if result["success"]:
                succeeded.append(eid)
            else:
                failed[eid] = result["message"]
        except Exception as e:
            failed[eid] = str(e)
            flog(f"undo_restore_batch: error on event {eid}: {e}", "ERROR")

    return RestoreReport(
        succeeded=succeeded,
        failed=failed,
        total_requested=len(events),
        trace_events=(),
    )


def _undo_update_restore(layer, event: AuditEvent,
                         fid_cache: Optional[Dict] = None) -> Dict[str, Any]:
    """Re-apply post-edit values to reverse a previous UPDATE restore."""
    check = pre_check_restore(layer, event)
    if not check.can_restore:
        return {"success": False, "message": check.reason}

    identity = _parse_identity(event.feature_identity_json)
    strength = get_identity_strength_for_layer(layer)
    if strength == IdentityStrength.NONE:
        return {"success": False, "message": "No stable identity"}

    target_fid = _find_target_feature(layer, identity, fid_cache)
    if target_fid is None:
        target_fid = _find_by_snapshot(layer, event, resolve_ambiguity=True)
        if target_fid is not None:
            flog(f"UNDO_UPD eid={event.event_id} fid_miss→snapshot_hit "
                 f"fid={target_fid}")
        else:
            flog(f"UNDO_UPD eid={event.event_id} target=not_found "
                 f"(fid_miss + snapshot_miss)", "WARNING")
            return {"success": False, "message": "Target feature not found"}

    new_attrs = reconstruct_new_attributes(event)
    field_mapping = _build_safe_mapping(check.drift, event)
    attr_changes = _build_attribute_changes(layer, target_fid, new_attrs, field_mapping)

    provider = layer.dataProvider()
    if attr_changes:
        if not provider.changeAttributeValues(attr_changes):
            flog(f"UNDO_UPD eid={event.event_id} fid={target_fid} attr_FAILED", "ERROR")
            return {"success": False, "message": "Attribute update failed"}

    new_geom_wkb = getattr(event, 'new_geometry_wkb', None)
    if new_geom_wkb is not None:
        if not bool(provider.capabilities() & QgisCompat.CAP_CHANGE_GEOMETRIES):
            flog(f"UNDO_UPD eid={event.event_id} fid={target_fid} geom=no_cap", "WARNING")
            return {"success": False, "message": "Provider lacks geometry change capability"}
        geom = rebuild_geometry(new_geom_wkb)
        if geom is not None:
            from .geometry_utils import wkb_short_repr, feature_geom_short_repr_diag
            before_repr = feature_geom_short_repr_diag(layer, target_fid)
            applying_repr = wkb_short_repr(bytes(geom.asWkb()))
            ok = bool(provider.changeGeometryValues({target_fid: geom}))
            flog(f"UNDO_UPD eid={event.event_id} fid={target_fid} "
                 f"before={before_repr} applying={applying_repr} geom_write={'ok' if ok else 'FAILED'}")
            if not ok:
                return {"success": False, "message": "Geometry update failed"}
    else:
        flog(f"UNDO_UPD eid={event.event_id} fid={target_fid} "
             f"attrs={len(attr_changes)} new_geom=none")
    return {"success": True, "message": "Undo reverted"}


def _undo_insert_restore(layer, event: AuditEvent) -> Dict[str, Any]:
    """Re-insert a feature that was deleted by a previous INSERT restore."""
    provider = layer.dataProvider()
    if provider is None:
        return {"success": False, "message": "No data provider"}
    if not bool(provider.capabilities() & QgisCompat.CAP_ADD_FEATURES):
        return {"success": False, "message": "Provider lacks add capability"}

    attrs = reconstruct_attributes(event)
    geom_wkb = getattr(event, 'new_geometry_wkb', None) or event.geometry_wkb
    geom = rebuild_geometry(geom_wkb)
    check = pre_check_restore(layer, event)
    field_mapping = _build_safe_mapping(check.drift if check else None, event)

    from qgis.core import QgsFeature
    new_feature = QgsFeature(layer.fields())
    _apply_attributes(new_feature, layer.fields(), attrs, field_mapping)
    if geom is not None:
        new_feature.setGeometry(geom)

    success, _ = provider.addFeatures([new_feature])
    if not success:
        errors = provider.errors()
        msg = "; ".join(errors) if errors else "Insert failed"
        return {"success": False, "message": msg}

    return {"success": True, "message": "Re-inserted (undo delete)"}


def build_restore_trace_event(source_event: AuditEvent, layer) -> Optional[AuditEvent]:
    """Build an audit event that traces a restore operation (RLU-044).

    The trace event records that a restore was performed, referencing
    the original event via restored_from_event_id.
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
        datasource_fingerprint=compute_datasource_fingerprint(layer),
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
        return json.loads(identity_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _find_target_feature(layer, identity: Dict,
                         fid_cache: Optional[Dict] = None) -> Optional[int]:
    """Find the FID of the target feature using identity info.

    Lookup priority:
      1. fid_cache (batched PK resolution).
      2. PK-based search (pk_field + pk_value).
      3. Raw FID fallback — only when no PK was captured for this event.

    When a PK was captured at capture time, the FID fallback is intentionally
    refused: FIDs are not stable after commits on many providers (ogr,
    postgres without oid), so a stale FID could address a different feature.
    """
    from qgis.core import QgsFeatureRequest, QgsExpression

    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")
    has_pk = bool(pk_field) and pk_value is not None

    if fid_cache is not None and has_pk:
        cached = fid_cache.get((pk_field, str(pk_value)))
        if cached is not None:
            return cached

    provider = layer.dataProvider()
    if has_pk:
        escaped_field = QgsExpression.quotedColumnRef(str(pk_field))
        escaped_value = QgsExpression.quotedValue(pk_value)
        expr = QgsExpression(f"{escaped_field} = {escaped_value}")
        if expr.hasParserError():
            flog(f"restore: invalid PK expression for field={pk_field} "
                 f"value={pk_value!r}: {expr.parserErrorString()}", "WARNING")
            return None
        request = QgsFeatureRequest(expr).setLimit(1)
        for feat in provider.getFeatures(request):
            return feat.id()
        # PK was expected but not found: do not fall back to FID, which is
        # unstable and could point at an unrelated feature.
        flog(f"restore: PK {pk_field}={pk_value!r} not found; "
             f"refusing FID fallback (safety)", "DEBUG")
        return None

    fid = identity.get("fid")
    if fid is not None:
        request = QgsFeatureRequest(fid)
        for feat in provider.getFeatures(request):
            return feat.id()

    return None


def _build_safe_mapping(drift: Optional[DriftReport], event: AuditEvent) -> Dict[str, str]:
    """Thin shim kept for in-module call sites that already hold a drift.

    Delegates to schema_drift.safe_field_mapping so the mapping logic
    lives in one place across restore_service and restore_executor.
    """
    return safe_field_mapping(event, drift=drift)


def _apply_attributes(feature, fields, attrs: Dict, mapping: Dict) -> None:
    """Apply restored attributes to a QgsFeature using the field mapping."""
    for idx, value in iter_mapped_attributes(mapping, attrs, fields):
        feature.setAttribute(idx, value)


def _build_attribute_changes(layer, fid: int, old_attrs: Dict,
                             mapping: Dict) -> Dict[int, Dict[int, Any]]:
    """Build the change dict for provider.changeAttributeValues()."""
    fields = layer.fields()
    changes = {
        idx: value
        for idx, value in iter_mapped_attributes(mapping, old_attrs, fields)
    }
    if not changes:
        return {}
    return {fid: changes}
