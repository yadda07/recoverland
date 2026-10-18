"""Temporal snapshot engine - reconstruct data state at a given date T.

Zero QGIS dependency. Zero Qt dependency. Pure Python.

Takes an in-memory event_cache (populated by ReviewCacheWorker, already
bounded to a time range) and a cutoff datetime, and returns a SnapshotResult
mapping entity_fp → SnapshotFeature for every entity that existed at T.

Algorithm (forward replay until T):
    For each fingerprint in event_cache:
        1. Group events by entity_fp (same key logic as lens_planner).
        2. Materialize rewind traces: an event with restored_from_event_id
           carries a marker payload ({"_restore_ref": eid}, no geometry),
           NOT the state it applied. Its effective state is resolved from
           the referenced source event (BL-RVW-01). A trace whose source is
           gone (purged, foreign) is kept as an UNKNOWN application: any
           field or the geometry may have changed there (see step 7).
        3. Compensations in effect at T, with the Rewind's own rule
           (rewind_dedup.traces_in_effect): a trace counts from its
           created_at until its invalidated_at (an Undo writes no event, it
           stamps the traces it reverted), and an exact (schema >= 7) trace
           that references a trace cancels it. The events <= T are the user
           events plus the applications in effect at T; a cancelled trace is
           replayed WITH its canceller, whose effective event is its
           inverse, so any edit made between the two keeps its place.
           After T, an undone trace also contributes its Undo, dated at
           invalidated_at: the inverse of what it had applied, placed like
           the Rewind places it (rewind_dedup.undo_position: at one
           instant, the trace written last was undone first). Two traces of
           ONE source stamped with one instant (an Undo written before v7
           stamped every trace of the events it undid, an earlier run's
           included, which it did not revert) are the tie the Rewind
           refuses (event_stream_repository._tied_undo_groups): from that
           instant on, which of them stayed in effect is not in the
           journal, and the tie is an UNKNOWN application there (step 7).
        4. Sort ASC by (created_at, event_id).
        5. Inspect the last visible event:
           - INSERT | UPDATE  → entity exists at T, record geom + attrs.
           - DELETE           → entity was absent at T (n_absent).
           - No events <= T   → counted in n_unknown, then decided by the
             FIRST later event (step 6).
        6. An entity with no event <= T is either born after T (its first
           later event is an INSERT: absent at T, n_born_after when it still
           exists) or it existed before the journal ever saw it (first later
           event UPDATE/DELETE). The journal only starts at tracking time, so
           the second case is the rule for every feature that predates it,
           and its state at T is the BEFORE-IMAGE of that first later event
           (n_preexisting). What no event gives (a geometry or a field never
           changed since T) is the live value: the feature is flagged
           ``geom_from_live`` / ``attrs_from_live`` and the caller completes
           it on the GUI thread (snapshot_overlay_session.complete_from_live).
           What neither the journal nor the live layer can give is flagged in
           ``unresolved`` and counted, never painted as if it were known.
        7. An UNKNOWN application (trace whose source is gone, Undo that
           cannot be expressed) before T resets what the older events said:
           the state right after it is read from the later events and the
           live feature like a before-image. After T it blocks the
           before-image walk: what it may have changed is unknown at T and
           the live value is the state AFTER it, so the entity is flagged
           (``trace_source_missing``) and never completed from live.
        8. On a source whose fid is the record POSITION (Shapefile, GeoJSON:
           ``repack_boundaries``), every DELETE of the source renumbers the
           records after it at the repack, so one ``fid:N`` key names
           several records in turn. Its events are cut at those instants:
           the state at T is read from the events between the last
           renumbering at or before T and the first one after T only.
           What is still unknown there is ``identity_not_proven``, never
           taken past a renumbering. The events after that first
           renumbering are changes of a record whose identity at T is not
           proven: each such stretch is counted (n_identity_unproven), or
           counted born after T when it starts with its INSERT.

Geometry states at T (``SnapshotFeature.geom_wkb``):
    WKB bytes            : the shape at T.
    GEOMETRY_NULL_MARKER : b"", the entity had NO geometry at T. The capture
                           writes it on the side of an UPDATE where the
                           geometry is absent (added or removed by that
                           edit); SQL NULL still means "unchanged". It is a
                           known state: no shape, not an error, and never a
                           reason to read the other side of the edit. Only
                           in an event of schema >= 7
                           (constants.is_null_geometry_marker): no older
                           capture ever wrote an empty BLOB, so there it is
                           corruption, named ``geometry_corrupt``.
    None                 : not given by the journal (completed from the live
                           feature, or unresolved).

Performance: O(N) over cached events. No SQL. ~1-5 ms per 5000 events.
"""
from __future__ import annotations

import json
import time
import uuid
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from typing import Dict, NamedTuple, Optional

from .constants import GEOMETRY_NULL_MARKER, is_null_geometry_marker
from .identity import compute_entity_fingerprint
from .logger import flog
from .serialization import extract_delta_new, extract_delta_old

# How many entities are replayed between two cancellation checks.
_CANCEL_CHECK_EVERY = 256

# Reasons a part of the state at T is unknown (SnapshotFeature.unresolved),
# besides "geometry_unknown" and "attributes_incomplete".
REASON_TRACE_SOURCE_MISSING = "trace_source_missing"
REASON_TRACE_UNDO_UNKNOWN = "trace_undo_state_unknown"
REASON_GEOMETRY_CORRUPT = "geometry_corrupt"
# The rest of the state lies past a renumbering of the fids (module step
# 8), or on a live feature whose identity nothing proves
# (snapshot_overlay_session._fetch_live): same reason, same meaning.
REASON_IDENTITY_NOT_PROVEN = "identity_not_proven"
# An Undo stamped several traces of one source with one instant, before T
# (``_tie_event``), and what that left is given neither by the later events
# nor by the live feature.
REASON_UNDO_TIED = "trace_undo_tied"

# Rows the engine derives from the journal carry a tag in commit_id (a user
# event holds a QGIS commit id or NULL there, a rewind plan item
# "rewind-plan:..."): an application whose payload is unknown, and the Undo
# of an applied trace. Engine-internal, never written anywhere.
_TAG_UNKNOWN = "rl-review:unknown:"
_TAG_UNDO = "rl-review:undo"

# Stands, inside the engine only, for a zero-length geometry BLOB in an
# event older than the null marker: corruption, never "no geometry". It is
# no WKB (byte order 0x00 then a type code no geometry has) and never
# leaves the engine (_completed_state turns it into None + a reason).
_CORRUPT_GEOMETRY = b"\x00rl:empty-blob-before-schema-7"

# From this event schema on, an INSERT whose geometry_wkb is SQL NULL is a
# feature created without geometry (the capture of the null-marker era).
_NULL_INSERT_SCHEMA = 7


# ------------------------------------------------------------------ #
# Contracts                                                            #
# ------------------------------------------------------------------ #


class SnapshotFeature(NamedTuple):
    """Reconstructed state of one entity at date T.

    geom_wkb   : geometry AT T (post-INSERT or post-UPDATE, or the
                 before-image of the first later event). b"" (the
                 GEOMETRY_NULL_MARKER) means the entity had no geometry at
                 T; None means no event gives it (see module docstring).
    attrs_json : flat {field: value} at T.
    last_op    : "INSERT" | "UPDATE" of the last event <= T, "" when the
                 entity has no event <= T (origin "before_image").
    origin     : "replay" (events <= T) or "before_image" (no event <= T,
                 state read from the first later event).
    identity_json   : newest captured identity, used to find the live feature.
    geom_from_live  : no event gives the geometry at T; it never changed
                      since T, so it is the geometry of the live feature.
    attrs_from_live : some fields at T are given by no event; they never
                      changed since T, so they hold their live value.
    unresolved : non-empty reason when part of the state at T is known
                 neither from the journal nor from the live layer.
    geom_crs_authid : the CRS the layer DECLARED when the event that gives
                 ``geom_wkb`` was captured (None when no event gives it).
                 Not ``crs_authid``, which is the anchor's: the geometry may
                 come from an older or a later event. What a display reads
                 to place the raw coordinates (relabel or reprojection,
                 geometry_utils.crs_verdict).
    geom_witnesses : the captured geometries of that event (both sides),
                 the witnesses crs_verdict asks for, like the Rewind hands
                 them for the same event.
    """

    entity_fp: str
    geom_wkb: Optional[bytes]
    attrs_json: Optional[str]
    crs_authid: Optional[str]
    last_event_id: int
    last_op: str
    last_created_at: str
    origin: str = "replay"
    identity_json: str = ""
    geom_from_live: bool = False
    attrs_from_live: bool = False
    unresolved: str = ""
    geom_crs_authid: Optional[str] = None
    geom_witnesses: tuple = ()
    schema_fields: tuple = ()


class SnapshotResult(NamedTuple):
    """Full outcome of reconstruct_snapshot_at.

    features  : {datasource_fp: {entity_fp: SnapshotFeature}} - present entities only.
    n_absent  : entities whose last event <= T was DELETE.
    n_unknown : entities with NO event <= cutoff. Not "born after T": most
                of them predate the journal (see n_preexisting).
    n_born_after  : entities absent at T that exist now (created after T,
                    or deleted before T and re-created since). The live layer
                    still shows them; the caller must say so.
    n_preexisting : entities with no event <= T whose state at T was read
                    from the before-image of their first later event.
    n_unresolved  : present entities whose state at T is partly unknown
                    (``SnapshotFeature.unresolved``); never painted as known.
    n_trace_ref_missing : among them, the entities whose state at T depends
                    on a rewind trace whose source event is gone (purged by
                    retention): what that trace applied is known to no one.
    n_identity_unproven : changes after T that no entity at T can be tied
                    to: on a source whose fids are renumbered by every
                    deletion, the events of a ``fid:N`` key after a
                    renumbering belong to a record whose position at T is
                    not proven (module step 8). Not in ``features`` (its
                    state at T is unknown, not even its existence), but the
                    live layer shows it changed: the caller must say so.
                    One record renumbered twice can count twice.
    partial   : True if the result is incomplete (e.g. volume guard tripped);
                callers MUST surface a degraded/partial indicator (never silent).
    partial_reason : short machine-readable reason when ``partial`` is True.
    """

    features: dict
    cutoff_dt: datetime
    n_fps: int
    n_entities: int
    n_absent: int
    n_unknown: int
    elapsed_ms: int
    trace_id: str
    all_event_markers: tuple = ()
    partial: bool = False
    partial_reason: str = ""
    n_born_after: int = 0
    n_preexisting: int = 0
    n_unresolved: int = 0
    n_trace_ref_missing: int = 0
    n_identity_unproven: int = 0


# ------------------------------------------------------------------ #
# Public API                                                           #
# ------------------------------------------------------------------ #


def reconstruct_snapshot_at(
    event_cache: Dict[str, list],
    cutoff_dt: datetime,
    trace_id: str = "",
    should_cancel=None,
    repack_boundaries: Optional[Dict[str, list]] = None,
) -> SnapshotResult:
    """Reconstruct entity states at cutoff_dt from the in-memory event cache.

    Args:
        event_cache : {fingerprint: [AuditEvent]} from ReviewCacheWorker.
        cutoff_dt   : target instant (naive = assumed UTC).
        trace_id    : correlation id; auto-generated if empty.
        should_cancel : optional zero-arg callable. Checked once per datasource;
                    when it returns True the replay bails out early (the caller
                    is responsible for discarding the partial result). Lets a
                    superseded worker stop wasting CPU during rapid scrubbing.
        repack_boundaries : {fingerprint: [ISO instants]} for the sources
                    whose fid is the record position (commit_guard.fid_is_stable
                    False): the instants at which their records were renumbered
                    (module step 8). At least the last one at or before the
                    cutoff and every one after it. A source absent from it
                    keeps its fids: a ``fid:N`` key is one record there.

    Returns:
        SnapshotResult with features dict and counters.
    """
    if not trace_id:
        trace_id = uuid.uuid4().hex[:8]

    t0 = time.monotonic()
    cutoff_utc = _to_utc(cutoff_dt)
    features: Dict[str, Dict] = {}
    n_absent = 0
    n_unknown = 0
    n_entities = 0
    n_born_after = 0
    n_preexisting = 0
    unproven: list = []

    trace_counters = {"resolved": 0, "missing": 0}

    for ds_fp, events in event_cache.items():
        if should_cancel is not None and should_cancel():
            flog(f"[{trace_id}] review_snapshot: reconstruct cancelled", "DEBUG")
            break
        bounds = None
        if repack_boundaries is not None and ds_fp in repack_boundaries:
            bounds = _repack_bounds(repack_boundaries[ds_fp], cutoff_utc,
                                    trace_id)
        entity_groups = _group_by_entity(events)
        ds_feats: Dict[str, SnapshotFeature] = {}
        for n_done, (entity_fp, ev_list) in enumerate(entity_groups.items()):
            # Per entity, not only per datasource: one layer can hold the
            # whole row budget, and a superseded worker must stop promptly
            # or the dialog waits on it when Review is turned off.
            if (should_cancel is not None and n_done % _CANCEL_CHECK_EVERY == 0
                    and should_cancel()):
                flog(f"[{trace_id}] review_snapshot: reconstruct cancelled", "DEBUG")
                break
            ev_list = _materialize_traces(ev_list, trace_counters)
            visible, later = _timeline_at_cutoff(ev_list, cutoff_utc,
                                                 trace_counters)
            bounded = False
            if bounds is not None and entity_fp.startswith("fid:"):
                visible, later, bounded, tails = _segment_at_repacks(
                    visible, later, bounds)
                for tail in tails:
                    if tail[0].operation_type == "INSERT":
                        # A record created there: it did not exist at T.
                        if tail[-1].operation_type != "DELETE":
                            n_born_after += 1
                        continue
                    unproven.append((ds_fp, entity_fp, tail))
                if not visible and not later:
                    # Nothing of the record that held this position at T.
                    continue
            if not visible:
                n_unknown += 1
                feat = _before_image_at_cutoff(entity_fp, later, bounded)
                if feat is None:
                    if later and later[-1].operation_type != "DELETE":
                        n_born_after += 1
                    continue
                n_preexisting += 1
                ds_feats[entity_fp] = feat
                continue
            feat = _resolve_at_cutoff(entity_fp, visible, later, bounded)
            if feat is None:
                n_absent += 1
                if later and later[-1].operation_type != "DELETE":
                    n_born_after += 1
            else:
                ds_feats[entity_fp] = feat
        if ds_feats:
            features[ds_fp] = ds_feats
            n_entities += len(ds_feats)

    n_unresolved = count_unresolved(features)
    n_trace_ref_missing = count_reason(features, REASON_TRACE_SOURCE_MISSING)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    flog(
        f"[{trace_id}] review_snapshot: reconstruct_at "
        f"cutoff={cutoff_utc.isoformat()} "
        f"n_fps={len(event_cache)} n_entities={n_entities} "
        f"n_absent={n_absent} n_unknown={n_unknown} "
        f"n_preexisting={n_preexisting} n_born_after={n_born_after} "
        f"n_unresolved={n_unresolved} "
        f"n_traces_resolved={trace_counters['resolved']} "
        f"n_trace_ref_missing={trace_counters['missing']} "
        f"n_entities_on_missing_trace_source={n_trace_ref_missing} "
        f"n_identity_unproven={len(unproven)} "
        f"elapsed_ms={elapsed_ms}",
        "INFO",
    )
    if trace_counters.get("undo_tied"):
        flog(
            f"[{trace_id}] review_snapshot: undo_tied "
            f"n_entities={trace_counters['undo_tied']} degraded=True "
            f"reason=undo_stamped_several_traces_of_one_event_with_one_instant "
            f"(an Undo before v7; which trace it reverted is not in the "
            f"journal: the state after it is read from the later events and "
            f"the live feature, or flagged {REASON_UNDO_TIED})",
            "WARNING",
        )
    if trace_counters["missing"]:
        flog(
            f"[{trace_id}] review_snapshot: trace_ref_missing "
            f"n={trace_counters['missing']} degraded=True "
            f"n_entities_flagged={n_trace_ref_missing} "
            f"reason=source_event_purged_or_foreign",
            "WARNING",
        )
    if unproven:
        sample = "; ".join(
            f"{ds[:12]}/{key} events={[e.event_id for e in tail][:6]} "
            f"ops={[e.operation_type for e in tail][:6]}"
            for ds, key, tail in unproven[:10])
        flog(
            f"[{trace_id}] review_snapshot: identity_not_proven "
            f"n={len(unproven)} degraded=True reason=fid_renumbered_by_delete "
            f"(changes after the cutoff under a fid another record held at "
            f"the cutoff; state at the cutoff unknown, not shown) "
            f"sample=[{sample}]",
            "WARNING",
        )
    return SnapshotResult(
        features=features,
        cutoff_dt=cutoff_utc,
        n_fps=len(event_cache),
        n_entities=n_entities,
        n_absent=n_absent,
        n_unknown=n_unknown,
        elapsed_ms=elapsed_ms,
        trace_id=trace_id,
        n_born_after=n_born_after,
        n_preexisting=n_preexisting,
        n_unresolved=n_unresolved,
        n_trace_ref_missing=n_trace_ref_missing,
        n_identity_unproven=len(unproven),
    )


def count_unresolved(features: dict) -> int:
    """Present entities whose state at T is partly unknown."""
    return sum(
        1 for ds_feats in features.values() for feat in ds_feats.values()
        if feat.unresolved
    )


def _event_schema_fields(event) -> tuple:
    """Field names captured in event.field_schema_json, () when absent."""
    raw = getattr(event, "field_schema_json", None) if event is not None else None
    if not raw:
        return ()
    try:
        cols = json.loads(raw)
    except (ValueError, TypeError):
        return ()
    if not isinstance(cols, list):
        return ()
    return tuple(
        c["name"] for c in cols
        if isinstance(c, dict) and isinstance(c.get("name"), str) and c["name"]
    )


def count_reason(features: dict, reason: str) -> int:
    """Present entities whose ``unresolved`` names *reason*."""
    return sum(
        1 for ds_feats in features.values() for feat in ds_feats.values()
        if reason in (feat.unresolved or "").split(",")
    )


def recount(result):
    """*result* with its unresolved counters recomputed from its features.

    Every step that drops or completes entities (the changed-after filter,
    the live completion) must keep the counters the bar and the export
    read in line with what is really left. ``n_identity_unproven`` is not
    one of them: those changes are never in ``features``, no filter or
    completion can resolve them, and it is kept as the replay counted it.
    """
    return result._replace(
        n_unresolved=count_unresolved(result.features),
        n_trace_ref_missing=count_reason(result.features,
                                         REASON_TRACE_SOURCE_MISSING),
    )


def has_null_geometry(feat) -> bool:
    """True when the entity is known to have had NO geometry at T."""
    return feat.geom_wkb is not None and feat.geom_wkb == GEOMETRY_NULL_MARKER


def count_null_geometry(features: dict) -> int:
    """Present entities that had no geometry at T (known state, no shape)."""
    return sum(
        1 for ds_feats in features.values() for feat in ds_feats.values()
        if has_null_geometry(feat)
    )


# ------------------------------------------------------------------ #
# Entity grouping                                                      #
# ------------------------------------------------------------------ #


def _canonical_fallback_key(feature_identity_json) -> str:
    """Canonical entity key when entity_fingerprint is absent.

    IS identity.compute_entity_fingerprint, the function that writes the
    column, so the key of a row without its fingerprint is the key of the
    rows that have it (``pk:<field>=<value>``, ``pkc:<col>=<v>|...`` for a
    composite key, or ``fid:<id>``). It used to be a copy that "mirrored"
    it: the copy did not know the composite form, filed such a row under
    ``fid:<id>`` while the rows of the same entity were under ``pkc:...``,
    and both Review and Rewind (rewind_dedup delegates here) split one
    entity in two. Pure Python, like identity's fingerprint functions.
    An identity nothing can be read from keeps its raw text, as before.
    """
    key = compute_entity_fingerprint(feature_identity_json)
    return key if key else f"fid:{feature_identity_json}"


def compute_entity_key(entity_fingerprint, feature_identity_json) -> str:
    """Canonical per-entity key from the two raw event columns.

    Public twin of :func:`_entity_key` that works on raw column values instead
    of an event object, so off-engine callers (e.g. the snapshot worker's
    ``changed-after-T`` filter) key entities EXACTLY like the reconstruction.
    """
    if entity_fingerprint:
        return entity_fingerprint
    return _canonical_fallback_key(feature_identity_json)


def _entity_key(event) -> str:
    """Stable per-entity key - mirrors lens_planner._entity_key.

    Prefers the stored ``entity_fingerprint`` (canonical, written by
    identity.compute_entity_fingerprint). Falls back to a canonical key derived
    from ``feature_identity_json`` so it stays consistent with the baseline
    merge (see _canonical_fallback_key).
    """
    return compute_entity_key(event.entity_fingerprint, event.feature_identity_json)


def _group_by_entity(events: list) -> Dict[str, list]:
    """Group and sort events by entity key. Ignores unknown op_types."""
    grouped: Dict[str, list] = {}
    for ev in events:
        if ev.operation_type not in ("INSERT", "UPDATE", "DELETE"):
            continue
        key = _entity_key(ev)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(_screen_geometry(ev))
    for ev_list in grouped.values():
        ev_list.sort(key=_order_key)
    return grouped


def _order_key(event) -> tuple:
    return (event.created_at or "", event.event_id or 0)


def _screen_geometry(event):
    """*event* with an empty geometry BLOB it cannot mean marked corrupt.

    The null marker (b"") exists since schema 7 only
    (constants.is_null_geometry_marker); no older capture ever wrote a
    zero-length BLOB, a NULL geometry was SQL NULL. In an older event one is
    a truncated row: read as the marker it showed "no geometry at T", a
    known state, where the state is unknown.
    """
    version = getattr(event, "event_schema_version", None)
    fixes = {}
    for column in ("geometry_wkb", "new_geometry_wkb"):
        wkb = getattr(event, column, None)
        if (wkb is not None and len(wkb) == 0
                and not is_null_geometry_marker(wkb, version)):
            fixes[column] = _CORRUPT_GEOMETRY
    if not fixes:
        return event
    flog(f"review_snapshot: empty_geometry_blob_before_schema_7 "
         f"event_id={event.event_id} event_schema_version={version} "
         f"columns={sorted(fixes)} read_as=corrupt", "WARNING")
    return event._replace(**fixes)


# ------------------------------------------------------------------ #
# Rewind trace materialization (BL-RVW-01)                             #
# ------------------------------------------------------------------ #


def _invert_delta(changed: dict) -> dict:
    """Swap old/new of a changed_only delta (compensation = reverse apply)."""
    inverted: Dict[str, dict] = {}
    for field, val in changed.items():
        inverted[field] = {
            "old": extract_delta_new(val),
            "new": extract_delta_old(val),
        }
    return inverted


def _parsed_attrs(event) -> dict:
    """attributes_json as dict, {} on absent/invalid payload."""
    if not event.attributes_json:
        return {}
    try:
        parsed = json.loads(event.attributes_json)
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _effective_trace(trace, src):
    """Return the trace event rewritten with the state it actually applied.

    build_restore_trace_event writes marker payloads only; the applied state
    lives in the referenced source event:
    - trace INSERT (compensated a DELETE): re-inserted state = the captured
      state of the source (geometry_wkb + all_attributes).
    - trace UPDATE (compensated an UPDATE): reverse delta of the source
      changed_only; restored geometry = source geometry_wkb (old side).
    - trace DELETE (compensated an INSERT): carries the removed state so
      chained traces can resolve through it; replay outcome stays absent.

    Returns None when the source is unusable (purged, foreign, corrupt or
    younger than the trace) - caller degrades loudly.
    """
    if src is None:
        return None
    if (src.created_at or "") > (trace.created_at or ""):
        return None
    op = trace.operation_type
    if op == "INSERT" or op == "DELETE":
        src_attrs = _parsed_attrs(src)
        if "all_attributes" not in src_attrs and src.geometry_wkb is None:
            return None
        # The geometry is the source's: captured in the CRS the layer
        # declared THEN, not the one it declared when the rewind ran.
        return trace._replace(
            geometry_wkb=src.geometry_wkb,
            attributes_json=src.attributes_json,
            crs_authid=src.crs_authid,
        )
    if op == "UPDATE":
        changed = _parsed_attrs(src).get("changed_only", {})
        if not isinstance(changed, dict):
            changed = {}
        # Old side of the compensation = the geometry the source applied,
        # but only when the source captured both sides of a geometry change:
        # it is what a replay AFTER the cutoff reads as the state before it.
        old_side = src.new_geometry_wkb if src.geometry_wkb is not None else None
        return trace._replace(
            attributes_json=json.dumps(
                {"changed_only": _invert_delta(changed)}, ensure_ascii=False,
            ),
            geometry_wkb=old_side,
            new_geometry_wkb=src.geometry_wkb,
            crs_authid=src.crs_authid,
        )
    return None


def _unknown_reason(event) -> str:
    """Why this row's payload is unknown ("" when it is known)."""
    tag = getattr(event, "commit_id", None)
    if isinstance(tag, str) and tag.startswith(_TAG_UNKNOWN):
        return tag[len(_TAG_UNKNOWN):]
    return ""


def _materialize_traces(ev_list: list, counters: dict) -> list:
    """Rewrite rewind traces of one entity chain into effective events.

    Single chronological pass (ev_list is sorted ASC): a trace always
    references an OLDER event of the same entity, so its source is already
    materialized when reached - chained traces (trace referencing a trace)
    resolve without recursion. A trace whose source is gone, or is itself
    unknown, keeps its marker payload, is TAGGED unknown
    (``trace_source_missing``) and counted in counters['missing']: the
    replay must not read it as "nothing changed here" (it used to, and the
    live value, the state after the rewind, was shown as the state at T).
    """
    if not any(e.restored_from_event_id is not None for e in ev_list):
        return ev_list
    by_eid: Dict[int, object] = {}
    out = []
    for ev in ev_list:
        if ev.restored_from_event_id is not None:
            src = by_eid.get(ev.restored_from_event_id)
            eff = None
            if src is not None and not _unknown_reason(src):
                eff = _effective_trace(ev, src)
            if eff is None:
                counters["missing"] += 1
                eff = ev._replace(commit_id=_TAG_UNKNOWN + REASON_TRACE_SOURCE_MISSING)
            else:
                counters["resolved"] += 1
            ev = eff
        out.append(ev)
        if ev.event_id is not None:
            by_eid[ev.event_id] = ev
    return out


def _undo_event(applied):
    """The Undo of an applied trace, dated at its invalidated_at.

    An Undo writes no event: it reverts the layer and stamps the traces it
    reverted. Its effect is the inverse of what the trace had applied. An
    UPDATE whose applied geometry is known on one side only cannot be
    inverted: the Undo is then an unknown row, never a guess.
    """
    base = applied._replace(created_at=applied.invalidated_at,
                            restored_from_event_id=None, invalidated_at=None,
                            commit_id=_TAG_UNDO)
    reason = _unknown_reason(applied)
    if reason:
        return base._replace(commit_id=_TAG_UNKNOWN + reason)
    op = applied.operation_type
    if op == "INSERT":
        return base._replace(operation_type="DELETE")
    if op == "DELETE":
        return base._replace(operation_type="INSERT")
    if applied.geometry_wkb is None and applied.new_geometry_wkb is not None:
        return base._replace(commit_id=_TAG_UNKNOWN + REASON_TRACE_UNDO_UNKNOWN)
    changed = _parsed_attrs(applied).get("changed_only", {})
    if not isinstance(changed, dict):
        changed = {}
    return base._replace(
        attributes_json=json.dumps({"changed_only": _invert_delta(changed)},
                                   ensure_ascii=False),
        geometry_wkb=applied.new_geometry_wkb,
        new_geometry_wkb=applied.geometry_wkb,
    )


def _applied_at(traces: list, cutoff_utc: datetime) -> set:
    """event_ids of the trace applications that make the state at T.

    The Rewind's rule, not a copy of it (rewind_dedup.traces_in_effect):
    a compensation is in effect from its created_at until its
    invalidated_at, and an exact trace in effect cancels the trace it
    references. A cancelled trace is replayed WITH its canceller: the
    canceller's effective event is its inverse, so the pair nets out and
    an edit made between the two keeps its place in the order (dropping
    both would put that edit last).
    """
    from .rewind_dedup import is_exact_trace, traces_in_effect  # noqa: PLC0415 - import cycle

    def happened(ts) -> bool:
        instant = _parse_created_at(ts)
        return instant is not None and instant <= cutoff_utc

    status = traces_in_effect(traces, happened)
    applied = {eid for eid, ok in status.items() if ok}
    for trace in traces:
        if (status.get(trace.event_id) and is_exact_trace(trace)
                and trace.restored_from_event_id in status):
            applied.add(trace.restored_from_event_id)
    return applied


def _tied_groups(traces: list) -> list:
    """The traces of one source an Undo stamped with one instant, per tie.

    The Rewind's rule, not a copy of it
    (event_stream_repository._tied_undo_groups), with every stamp counted:
    the Rewind looks only at the ties after its cutoff (it must undo them
    and cannot), the Review needs them wherever T falls.
    """
    from .event_stream_repository import _tied_undo_groups  # noqa: PLC0415 - import cycle
    return _tied_undo_groups(traces, lambda _instant: False)


def _tie_event(group: list):
    """The Undo that stamped the traces of *group* alike, as an UNKNOWN row.

    Two traces of one source stamped with one instant come from different
    runs (one run never writes two): an Undo written before v7 stamped
    every trace of the events it undid, an earlier run's included, which it
    did not revert. Which of them stayed in effect after it, and so what it
    left in the layer, is not in the journal: the replay dropped them all
    and showed the user events alone, a state the layer never held (the
    field that Undo re-wrote read as the value typed before it). From that
    instant the state is read like after any unknown application (module
    step 7): from the later events and the live feature, or flagged.

    Placed like the Rewind's refusal names it: at the stamp, under the
    newest trace of the group. Its operation is the inverse of theirs
    (they share their source, hence their operation): whether the entity
    exists after it is known, only its state is not.
    """
    anchor = max(group, key=lambda t: t.event_id or 0)
    return _undo_event(anchor)._replace(commit_id=_TAG_UNKNOWN + REASON_UNDO_TIED)


def _timeline_at_cutoff(ev_list: list, cutoff_utc: datetime,
                        counters: Optional[dict] = None) -> tuple:
    """(events that make the state at T, events after T) of one entity.

    Without traces, the plain split. With traces: at or before T, the user
    events and the trace applications in effect at T (``_applied_at``);
    after T, every event and every Undo (see ``_undo_event``) dated after
    T. The worker used to drop every invalidated trace: a date between a
    compensation and its Undo was shown without the compensation, and the
    Undo, which writes no event, was no change "after T" at all.

    The Undos after T are placed like the Rewind places them
    (rewind_dedup.undo_position, instant then newest trace first): an Undo
    before v7 stamped all its traces with one instant, and a tie broken on
    the trace id put them in the order the undone run WROTE them, the
    reverse of the Undo's. An Undo that stamped several traces of ONE
    source alike (``_tied_groups``) is, at or before T, an unknown row
    (``_tie_event``); after T its rows stay as they are: whichever trace it
    reverted, it wrote the same inverse, and every trace of the tie was in
    effect until it. *counters* ``["undo_tied"]`` counts the entities whose
    state at T lies past such a tie.
    """
    from .rewind_dedup import undo_position  # noqa: PLC0415 - import cycle

    traces = [e for e in ev_list if e.restored_from_event_id is not None]
    if not traces:
        return _split_at_cutoff(ev_list, cutoff_utc)
    applied = _applied_at(traces, cutoff_utc)
    visible = []
    for ev in ev_list:
        created = _parse_created_at(ev.created_at)
        if created is None or created > cutoff_utc:
            continue
        if ev.restored_from_event_id is None or ev.event_id in applied:
            visible.append(ev)
    ties = [_tie_event(group) for group in _tied_groups(traces)]
    ties = [tie for tie in ties
            if (_parse_created_at(tie.created_at) or cutoff_utc) <= cutoff_utc]
    if ties:
        # At or before T every trace of the tie is out of effect (stamped),
        # and the unknown row stands for what that Undo left.
        visible = sorted(visible + ties, key=_order_key)
        if counters is not None:
            counters["undo_tied"] = counters.get("undo_tied", 0) + 1
    # An Undo at or before T is never replayed: the trace it reverted is
    # out of effect at T, so neither of the two is in the state at T.
    rows = [((ev.created_at or "", 0, ev.event_id or 0), ev) for ev in ev_list]
    for trace in traces:
        if trace.invalidated_at:
            at, position = undo_position(trace)
            rows.append(((at, 1, position), _undo_event(trace)))
    rows.sort(key=lambda row: row[0])
    later = [ev for _position, ev in rows
             if (_parse_created_at(ev.created_at) or cutoff_utc) > cutoff_utc]
    return visible, _drop_neutral_unknowns(later)


def _drop_neutral_unknowns(later: list) -> list:
    """*later* without an unknown application followed at once by its Undo.

    Nothing of the entity happened between the two: whatever the trace
    applied, the Undo put back the state from before it, so the pair
    changes nothing and must not block the before-image walk (which would
    flag an entity whose state at T is fully known).
    """
    out = []
    i = 0
    while i < len(later):
        ev = later[i]
        nxt = later[i + 1] if i + 1 < len(later) else None
        if (nxt is not None and ev.restored_from_event_id is not None
                and _unknown_reason(ev) and nxt.restored_from_event_id is None
                and nxt.event_id is not None and nxt.event_id == ev.event_id):
            i += 2
            continue
        out.append(ev)
        i += 1
    return out


# ------------------------------------------------------------------ #
# Cutoff filtering                                                     #
# ------------------------------------------------------------------ #


def _before_cutoff(event, cutoff_utc: datetime) -> bool:
    """Return True if event.created_at <= cutoff_utc."""
    created = _parse_created_at(event.created_at)
    if created is None:
        return False
    return created <= cutoff_utc


def _split_at_cutoff(ev_list: list, cutoff_utc: datetime) -> tuple:
    """Split a sorted chain into (events <= T, events > T).

    An event whose date cannot be read belongs to neither side, as it
    never did: it cannot be placed on the timeline.
    """
    visible = []
    later = []
    for ev in ev_list:
        created = _parse_created_at(ev.created_at)
        if created is None:
            continue
        if created <= cutoff_utc:
            visible.append(ev)
        else:
            later.append(ev)
    return visible, later


def _repack_bounds(instants, cutoff_utc: datetime, trace_id: str = "") -> tuple:
    """(last renumbering at or before T or None, renumberings after T ASC).

    An instant that cannot be read cannot be placed: the records may have
    been renumbered anywhere, so nothing of a ``fid:`` key is proven on
    either side of T (the renumbering is put AT T and right after it).
    """
    lo = None
    his = set()
    unreadable = 0
    for iso in instants or ():
        instant = _parse_created_at(iso)
        if instant is None:
            unreadable += 1
            continue
        if instant <= cutoff_utc:
            if lo is None or instant > lo:
                lo = instant
        else:
            his.add(instant)
    if unreadable:
        flog(f"[{trace_id}] review_snapshot: {unreadable} renumbering instant(s) "
             f"unreadable; every fid key of the source is unproven around the "
             f"cutoff", "WARNING")
        lo = cutoff_utc
        his.add(cutoff_utc + timedelta(microseconds=1))
    return lo, sorted(his)


def _segment_at_repacks(visible: list, later: list, bounds: tuple) -> tuple:
    """Cut the timeline of a ``fid:`` key at the renumberings of its source.

    Returns (visible, later, bounded, tails):
      * visible: the events <= T after the last renumbering at or before T.
        Older ones were written under this fid by the record that held the
        position THEN: replayed, they gave this record another one's fields
        (an INSERT's full snapshot, a rename);
      * later: the events after T up to the first renumbering after T, the
        commit of that renumbering included (the capture keys a commit with
        the fids it saw before the repack);
      * bounded: that renumbering exists. Past it the record may hold
        another fid and this fid another record, so what the walk over
        *later* leaves unknown is neither a later event's old side nor the
        live value;
      * tails: the events after that renumbering, one list per stretch
        between two renumberings: each is the change of a record whose
        position at T nothing proves.

    The Rewind reads such a bucket the same way: one fid holds several
    lifetimes after a repack (rewind_dedup._deleted_segments,
    _lifetime_ends_as).
    """
    lo, his = bounds
    if lo is not None:
        visible = [ev for ev in visible if _parse_created_at(ev.created_at) > lo]
    if not his:
        return visible, later, False, []
    own = []
    stretches: Dict[int, list] = {}
    for ev in later:
        index = bisect_left(his, _parse_created_at(ev.created_at))
        if index == 0:
            own.append(ev)
        else:
            stretches.setdefault(index, []).append(ev)
    return visible, own, True, [stretches[i] for i in sorted(stretches)]


def _parse_created_at(iso: Optional[str]) -> Optional[datetime]:
    """Parse ISO UTC string to timezone-aware datetime. None on failure."""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _to_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ------------------------------------------------------------------ #
# State resolution                                                     #
# ------------------------------------------------------------------ #


def _build_attrs_at_cutoff(visible: list) -> Optional[str]:
    """Replay INSERT base + UPDATE deltas to build full {field: value} at T.

    For pre-existing entities (first visible event is UPDATE, no INSERT),
    seeds ``attrs`` from the **old** values of the first UPDATE's
    ``changed_only`` to fill classification fields that were never
    modified.  Subsequent UPDATEs only apply the new values.
    """
    attrs, _full = _replay_attrs(visible)
    return json.dumps(attrs, ensure_ascii=False) if attrs else None


def _replay_attrs(visible: list) -> tuple:
    """(attrs at T, full) from the events <= T.

    ``full`` is True only when an INSERT/DELETE snapshot gave every field.
    A chain that starts with an UPDATE (a feature that predates the
    journal) only knows the fields it touched: the others are NOT null at
    T, they are unknown here and completed from later events or from the
    live feature. Treating them as known is how an export came out with
    NULL owners and areas.
    """
    attrs: Dict[str, object] = {}
    full = False
    for ev in visible:
        if _unknown_reason(ev):
            # Any field may have changed here: nothing an older event said
            # still holds. What follows, the later events and the live
            # feature give the state right after it (module step 7).
            attrs = {}
            full = False
            continue
        parsed = _parsed_attrs(ev)
        if not parsed:
            continue
        if ev.operation_type in ("INSERT", "DELETE"):
            base = parsed.get("all_attributes", {})
            if isinstance(base, dict):
                attrs.update(base)
                full = True
        elif ev.operation_type == "UPDATE":
            changed = parsed.get("changed_only", {})
            if isinstance(changed, dict):
                if not full:
                    for field, val in changed.items():
                        attrs.setdefault(field, extract_delta_old(val))
                for field, val in changed.items():
                    attrs[field] = extract_delta_new(val)
    return attrs, full


def _attrs_from_later(attrs: dict, later: list) -> tuple:
    """Fill the fields unknown at T from the OLD side of later events.

    A field keeps, from T until the first later event that changes it, the
    value that event records as ``old``; a DELETE records every field. The
    walk stops at the first DELETE or INSERT: past it, the values describe
    another life of the entity. It also stops at an unknown row (a trace
    whose source is gone): it may have changed any field still unknown,
    whose value at T is then neither its old side (unknown) nor the live
    one (the state after it). Returns (attrs, full, blocking reason).
    """
    known = set(attrs)
    for ev in later:
        reason = _unknown_reason(ev)
        if reason:
            return attrs, False, reason
        parsed = _parsed_attrs(ev)
        op = ev.operation_type
        if op == "UPDATE":
            changed = parsed.get("changed_only", {})
            if isinstance(changed, dict):
                for field, val in changed.items():
                    if field not in known:
                        attrs[field] = extract_delta_old(val)
                        known.add(field)
            continue
        if op == "DELETE":
            base = parsed.get("all_attributes")
            if isinstance(base, dict):
                for field, val in base.items():
                    if field not in known:
                        attrs[field] = val
                        known.add(field)
                return attrs, True, ""
        return attrs, False, ""
    return attrs, False, ""


def _geom_before_later(later: list) -> tuple:
    """Geometry at T read from later events: (wkb, source, event).

    source is "event" when an event gave it, "live" when no later event
    changed the geometry (so it is the current one), "unknown" when it
    changed and its old side was never captured, or "blocked:<reason>" when
    an unknown row (a trace whose source is gone) comes first: it may have
    changed the geometry, so the live one is not the geometry at T.
    *event* is the event that gave the geometry (its capture CRS places
    the raw coordinates), None otherwise.
    """
    for ev in later:
        reason = _unknown_reason(ev)
        if reason:
            return None, "blocked:" + reason, None
        op = ev.operation_type
        if op == "UPDATE":
            if ev.geometry_wkb is not None:
                # GEOMETRY_NULL_MARKER included: that edit ADDED the
                # geometry, so the entity had none at T.
                return ev.geometry_wkb, "event", ev
            if ev.new_geometry_wkb is not None:
                return None, "unknown", None
            continue
        if op == "DELETE" and ev.geometry_wkb is not None:
            return ev.geometry_wkb, "event", ev
        return None, "unknown", None
    return None, "live", None


def _replay_geom(visible: list) -> tuple:
    """Geometry at T from the events <= T: (wkb, found, event).

    Walks visible events backwards to find the last known geometry:
    - INSERT          : geometry_wkb (the created geometry), found even if None.
    - UPDATE with geo : new_geometry_wkb, or geometry_wkb as fallback.
    - UPDATE attrs-only (both None): keep walking back to a prior event.
    Not found means no event <= T carries the geometry: a feature that
    predates the journal and was only ever edited in its attributes.

    ``new_geometry_wkb == GEOMETRY_NULL_MARKER`` is a found state: the edit
    removed the geometry. The old side is never read in its place, which
    would show a shape the entity no longer had at T.

    Reaching an unknown row (a trace whose source is gone) first means the
    geometry may have changed there: not found, the later events and the
    live feature give the geometry right after it (module step 7).

    *event* is the event that gave the geometry, None when none did.
    """
    for ev in reversed(visible):
        if _unknown_reason(ev):
            return None, False, None
        op = ev.operation_type
        if op == "INSERT":
            if ev.geometry_wkb is None:
                # The capture writes a feature created without geometry as
                # SQL NULL (geometry_utils.geometry_to_wkb): since schema 7
                # that is the known state "no geometry". It was returned as
                # None, "unknown", and nothing counted it: the entity was
                # neither drawn nor announced. Older rows keep None, which
                # _completed_state now names (geometry_unknown).
                known = int(getattr(ev, "event_schema_version", None) or 0) \
                    >= _NULL_INSERT_SCHEMA
                flog(
                    f"review_snapshot: insert_no_geom event_id={ev.event_id} "
                    f"read_as={'no_geometry' if known else 'unknown'}",
                    "INFO" if known else "WARNING",
                )
                return (GEOMETRY_NULL_MARKER if known else None), True, ev
            return ev.geometry_wkb, True, ev
        if op == "UPDATE":
            if ev.new_geometry_wkb is not None:
                return ev.new_geometry_wkb, True, ev
            if ev.geometry_wkb is not None:
                flog(
                    f"review_snapshot: pre_v2_geom_fallback "
                    f"event_id={ev.event_id} "
                    f"op=UPDATE using_old_geom=True",
                    "WARNING",
                )
                return ev.geometry_wkb, True, ev
    return None, False, None


def _completed_state(entity_fp: str, attrs: dict, attrs_full: bool,
                     geom, geom_source: str, later: list, anchor,
                     origin: str, bounded: bool = False,
                     geom_event=None) -> SnapshotFeature:
    """Assemble a SnapshotFeature, completing it from the later events.

    ``anchor`` is the last event <= T (None for a before-image). What the
    journal cannot give is either the live value (the entity still exists
    and that part never changed since T) or unresolved, never a guess.
    A walk blocked by an unknown row, or a corrupt geometry, is named and
    never handed to the live completion.

    ``bounded``: *later* stops at a renumbering of the fids (module step
    8). Past it this fid may hold another record: what is still unknown
    is ``identity_not_proven``, never the live value (which is the state
    of whatever record holds the fid now).

    ``geom_event``: the event <= T that gave *geom* (_replay_geom); its
    capture CRS and captured geometries go along (``geom_crs_authid``,
    ``geom_witnesses``) so a display places the raw coordinates by the
    same verdict as the Rewind (geometry_utils.crs_verdict).
    """
    blocked = ""
    if not attrs_full and later:
        attrs, attrs_full, blocked = _attrs_from_later(attrs, later)
    if geom_source == "later":
        geom, geom_source, geom_event = _geom_before_later(later)
    exists_now = (later[-1].operation_type != "DELETE") if later else True
    reasons = []
    if geom_source.startswith("blocked:"):
        reasons.append(geom_source[len("blocked:"):])
    if blocked and blocked not in reasons:
        reasons.append(blocked)
    if geom is not None and geom == _CORRUPT_GEOMETRY:
        geom = None
        geom_source = "corrupt"
        reasons.append(REASON_GEOMETRY_CORRUPT)
    if (geom_source == "unknown" or (geom_source == "live" and not exists_now)
            or (geom_source == "event" and geom is None)):
        reasons.append("geometry_unknown")
    if not attrs_full and not exists_now and not blocked:
        reasons.append("attributes_incomplete")
    geom_live = geom_source == "live" and exists_now
    attrs_live = not attrs_full and exists_now and not blocked
    if bounded and (geom_live or attrs_live):
        # geom stays None here: _geom_before_later gives no shape for "live".
        reasons.append(REASON_IDENTITY_NOT_PROVEN)
        geom_live = attrs_live = False
    newest = later[-1] if later else anchor
    schema_ev = anchor if anchor is not None else (later[0] if later else None)
    return SnapshotFeature(
        entity_fp=entity_fp,
        geom_wkb=geom,
        attrs_json=json.dumps(attrs, ensure_ascii=False) if attrs else None,
        crs_authid=(anchor.crs_authid if anchor is not None
                    else (later[0].crs_authid if later else None)),
        last_event_id=(anchor.event_id or 0) if anchor is not None else 0,
        last_op=anchor.operation_type if anchor is not None else "",
        last_created_at=(anchor.created_at or "") if anchor is not None else "",
        origin=origin,
        identity_json=(newest.feature_identity_json or "") if newest is not None else "",
        geom_from_live=geom_live,
        attrs_from_live=attrs_live,
        unresolved=",".join(reasons),
        geom_crs_authid=(geom_event.crs_authid
                         if geom_event is not None and geom else None),
        geom_witnesses=((geom_event.geometry_wkb, geom_event.new_geometry_wkb)
                        if geom_event is not None and geom else ()),
        schema_fields=_event_schema_fields(schema_ev),
    )


def _resolve_at_cutoff(
    entity_fp: str,
    visible: list,
    later: Optional[list] = None,
    bounded: bool = False,
) -> Optional[SnapshotFeature]:
    """Build SnapshotFeature from the last visible event, or None if DELETE."""
    last = visible[-1]
    if last.operation_type == "DELETE":
        return None
    attrs, attrs_full = _replay_attrs(visible)
    geom, found, geom_event = _replay_geom(visible)
    feat = _completed_state(
        entity_fp, attrs, attrs_full, geom,
        "event" if found else "later", later or [], last, "replay", bounded,
        geom_event=geom_event,
    )
    # The state was read past an Undo tie (``_tie_event``) and is not
    # entirely known: the tie is why the journal could not give it, named
    # first so the user is not left with "geometry unknown" alone.
    last_unknown = next((ev for ev in reversed(visible) if _unknown_reason(ev)),
                        None)
    if (feat.unresolved and last_unknown is not None
            and _unknown_reason(last_unknown) == REASON_UNDO_TIED):
        rest = [r for r in feat.unresolved.split(",")
                if r and r != REASON_UNDO_TIED]
        feat = feat._replace(unresolved=",".join([REASON_UNDO_TIED] + rest))
    return feat


def _before_image_at_cutoff(entity_fp: str, later: list,
                            bounded: bool = False) -> Optional[SnapshotFeature]:
    """State at T of an entity with no event <= T, or None if absent at T.

    Its first later event decides: an INSERT means it did not exist yet;
    an UPDATE or a DELETE means it existed, and that event's before-image
    (old side of the delta, old geometry, full snapshot of a DELETE) is
    its state at T.
    """
    if not later or later[0].operation_type == "INSERT":
        return None
    return _completed_state(
        entity_fp, {}, False, None, "later", later, None, "before_image",
        bounded,
    )


def _geom_at_cutoff(visible: list) -> Optional[bytes]:
    """Return the geometry the entity had at cutoff.

    None when unknown (or corrupt), GEOMETRY_NULL_MARKER (b"") when it had
    none.
    """
    geom, _found, _event = _replay_geom([_screen_geometry(ev) for ev in visible])
    return None if geom == _CORRUPT_GEOMETRY else geom


__all__ = [
    "SnapshotFeature",
    "SnapshotResult",
    "reconstruct_snapshot_at",
    "compute_entity_key",
    "count_unresolved",
    "count_reason",
    "recount",
    "count_null_geometry",
    "has_null_geometry",
    "REASON_TRACE_SOURCE_MISSING",
    "REASON_TRACE_UNDO_UNKNOWN",
    "REASON_GEOMETRY_CORRUPT",
    "REASON_IDENTITY_NOT_PROVEN",
    "REASON_UNDO_TIED",
]
