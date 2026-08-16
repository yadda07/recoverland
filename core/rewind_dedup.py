"""Rewind event deduplication for temporal restore.

Given N raw events after cutoff (ordered DESC by event_id),
collapse them to the minimal effective set per entity.

Two kinds of events live in the journal:
  - User events: original DELETE / UPDATE / INSERT performed by the user.
  - Trace events: written by a previous restore to record that a user
    event has been compensated (restored_from_event_id != None).

Pipeline:
  1. Drop trace events; collect the set of user event_ids they reference
     (already-compensated user events) and, per entity, the COMPENSATED
     SPAN [oldest referenced event .. newest trace].
  2. Drop the user events that fall inside that span: replaying their
     compensatory action would duplicate the work and accumulate
     features in the layer. User events at or after the newest trace are
     edits made AFTER that rewind; they have never been compensated and
     stay active, otherwise the entity can never be brought back to the
     cutoff state (scenario rw_dedup_post_trace_edit).
  3. Apply the per-entity collapse on the remaining user-only chain:

       first=INSERT, last=DELETE  -> SKIP (created and destroyed after cutoff)
       any other shape            -> keep the entire chain. The restore
                                     planner unwinds it step by step in
                                     DESC order, each compensation
                                     bringing the entity to the state
                                     captured by the next older event.

Naive single-event collapses destroy NEW-side information when the
chain has multiple events on the same entity (e.g. U->U or I->U), which
breaks post-state lookups that rely on the live feature still matching
the captured NEW.

Zero QGIS dependency. Pure deterministic logic.
"""
import json
from typing import List, Dict, Optional, Tuple

from .audit_backend import AuditEvent
from .logger import flog
from .serialization import extract_delta_new, extract_delta_old
from .temporal_snapshot_engine import compute_entity_key


def _entity_key(event: AuditEvent) -> str:
    """Per-entity key, scoped by datasource.

    Delegates the entity part to ``temporal_snapshot_engine.compute_entity_key``
    so the rewind engine and the review engine agree on what one entity IS.
    They did not: with no ``entity_fingerprint`` (rows written before the
    backfill), review canonicalised the identity to ``fid:5`` while rewind
    used the raw ``feature_identity_json`` string. Any change in how that
    JSON is rendered -- key order, spacing -- then split one entity's history
    across several dedup buckets, and each bucket was collapsed on its own.
    Review showed one feature, rewind planned on two.

    The datasource part is the fingerprint the event carries, and the read
    paths hand it over ALREADY canonicalised
    (`datasource_alias.canonicalize_event_fingerprints`): expanding the
    scope in SQL alone brought the old rows back with their historical
    fingerprint, which split one entity across as many buckets as it had
    identities -- an INSERT made under a filtered fingerprint and the
    DELETE that undid it after the filter was removed then looked like two
    unrelated lifetimes and both got compensated.
    """
    return "".join((
        event.datasource_fingerprint or "",
        "::",
        compute_entity_key(event.entity_fingerprint, event.feature_identity_json),
    ))


def _detect_fid_recycle(
    events: List[AuditEvent],
) -> Tuple[Dict[str, int], List[Tuple[str, int, int]]]:
    """Detect FID-recycle patterns within a single fp lifeline.

    Pattern of interest (BL-RW-P1-07, CR-1):
        INSERT(fp=X, eid=A) -> DELETE(fp=X, eid=B>A) -> INSERT(fp=X, eid=C>B)

    Two distinct logical entities share entity_fingerprint='fid:X'
    because OGR/GPKG recycles the FID after the first DELETE. Without
    splitting, both end up in the same dedup bucket.

    Walks events ordered ASC by event_id and runs a per-fp state machine:
        None          --INSERT--> open
        None          --DELETE--> closed       (pre-existing entity deleted)
        None          --UPDATE--> pre_existing (pre-existing entity modified)
        open          --DELETE--> closed
        closed        --INSERT--> open + SPLIT recorded
        closed        --UPDATE--> pre_existing + SPLIT (recycled entity updated)
        pre_existing  --DELETE--> closed
        pre_existing  --INSERT--> open + SPLIT (FID recycled, no DELETE in window)
        open/pre_existing --UPDATE--> unchanged

    The pre_existing state (BL-RW-P1-23-A2) captures entities that existed
    at cutoff time and were modified or deleted after cutoff without an
    INSERT event in the rewind window.  When OGR recycles the FID for a
    new INSERT, the split separates the original entity's events from
    the recycled entity's events so they are bucketed independently.

    Args:
        events: any iterable of AuditEvent. event_id=None and missing
            entity_fingerprint are skipped (defensive).

    Returns:
        fp_split_eid: dict {fp -> event_id of the most recent SPLIT}.
        splits: list of (fp, first_eid, second_eid) for logging.
    """
    fp_split_eid: Dict[tuple, int] = {}
    splits: List[Tuple[str, int, int]] = []
    sorted_events = sorted(
        [e for e in events
         if e.event_id is not None and e.entity_fingerprint],
        key=lambda e: e.event_id,
    )
    fp_state: Dict[tuple, str] = {}
    fp_first_eid: Dict[tuple, int] = {}
    for e in sorted_events:
        # Keyed by (datasource, entity): 'fid:5' means a different feature in
        # every layer. Keying on the fingerprint alone made a multi-layer
        # rewind interleave the lifelines of unrelated features -- a DELETE
        # in layer B closed the state of layer A's fid:5, and the next event
        # in A was recorded as a recycle SPLIT. The split then rewrote the
        # fingerprints of A's events and scattered one entity across two
        # dedup buckets.
        fp_key = (e.datasource_fingerprint, e.entity_fingerprint)
        op = e.operation_type
        state = fp_state.get(fp_key)
        if op == "INSERT":
            if state in ("closed", "pre_existing"):
                fp_split_eid[fp_key] = e.event_id
                splits.append((e.entity_fingerprint,
                               fp_first_eid.get(fp_key), e.event_id))
                fp_first_eid[fp_key] = e.event_id
            elif state is None:
                fp_first_eid[fp_key] = e.event_id
            fp_state[fp_key] = "open"
        elif op == "DELETE":
            if state in ("open", "pre_existing", None):
                fp_state[fp_key] = "closed"
        elif op == "UPDATE":
            if state is None:
                fp_state[fp_key] = "pre_existing"
                fp_first_eid[fp_key] = e.event_id
            elif state == "closed":
                fp_split_eid[fp_key] = e.event_id
                splits.append((e.entity_fingerprint,
                               fp_first_eid.get(fp_key), e.event_id))
                fp_first_eid[fp_key] = e.event_id
                fp_state[fp_key] = "pre_existing"
            # state "open" or "pre_existing": no change
    return fp_split_eid, splits


def _apply_fid_recycle_rewrite(
    events: List[AuditEvent],
    fp_split_eid: Dict[tuple, int],
) -> List[AuditEvent]:
    """Rewrite entity_fingerprint to fp@<split_eid> for events on or
    after a detected FID-recycle split.

    Keys are (datasource_fingerprint, entity_fingerprint) pairs, so a split
    detected in one layer never touches the identically-numbered feature of
    another layer.

    Returns a new list; input is not mutated. Events without a matching
    split are returned unchanged.
    """
    if not fp_split_eid:
        return events
    rewritten: List[AuditEvent] = []
    for e in events:
        fp = e.entity_fingerprint
        fp_key = (e.datasource_fingerprint, fp)
        split_eid = fp_split_eid.get(fp_key)
        if (fp and split_eid is not None and e.event_id is not None
                and e.event_id >= split_eid):
            new_fp = f"{fp}@{split_eid}"
            rewritten.append(e._replace(entity_fingerprint=new_fp))
        else:
            rewritten.append(e)
    return rewritten


def _order_key(event: AuditEvent) -> tuple:
    """Chronological sort key for an event.

    created_at first, event_id as tie-breaker: pending-recovery events are
    re-inserted after a crash and receive an event_id that no longer
    reflects chronological order (same rationale as the ORDER BY in
    event_stream_repository.fetch_events_after_cutoff).
    """
    return ((event.created_at or ""), (event.event_id or 0))


def _is_trace(event: AuditEvent) -> bool:
    return getattr(event, "restored_from_event_id", None) is not None


def _is_invalidated(event: AuditEvent) -> bool:
    return getattr(event, "invalidated_at", None) is not None


def collapse_rewind_events(events: List[AuditEvent]) -> List[AuditEvent]:
    """Collapse raw rewind events to the minimal effective set.

    Args:
        events: ordered by event_id DESC (most recent first), possibly
            including restore_trace_events (restored_from_event_id != None).

    Returns:
        Deduplicated list of USER events to apply compensatory actions
        for. Trace events are filtered out and any user event whose
        compensatory action has already been written by a previous rewind
        (i.e. referenced by a trace via restored_from_event_id) is removed
        from the active set, so the same rewind can be replayed without
        accumulating duplicate features.
    """
    active, _stats = collapse_rewind_events_with_stats(events)
    return active


def collapse_rewind_events_with_stats(
    events: List[AuditEvent],
) -> Tuple[List[AuditEvent], Dict[str, int]]:
    """Same as collapse_rewind_events but also returns canonical stats
    suitable for ``log_cycle_summary`` consumption.

    Stats keys:
        raw, user, traces, traces_invalidated, traces_active,
        dedup_active, dedup_dropped, dedup_redundant
    """
    if not events:
        return [], {
            "raw": 0, "user": 0,
            "traces": 0, "traces_invalidated": 0, "traces_active": 0,
            "dedup_active": 0, "dedup_dropped": 0, "dedup_redundant": 0,
        }

    # Pre-pass: detect FID-recycle (INSERT -> DELETE -> INSERT on the
    # same entity_fingerprint) and rewrite entity_fingerprint of the
    # second cycle so the per-entity bucketing below sees two distinct
    # entities. CR-1 / BL-RW-P1-07.
    #
    # Detection runs on the events that still DESCRIBE THE LIVE DATA:
    #   - traces are excluded: a trace carries the compensating operation
    #     (a DELETE is compensated by an INSERT), so feeding them to the
    #     state machine read as "entity closed then re-INSERTed" and
    #     recorded a phantom recycle split;
    #   - already-compensated user events are excluded too: a DELETE that
    #     a previous rewind has undone did not close anything, the feature
    #     is back. Keeping it made a later UPDATE on that same feature look
    #     like an FID recycle, which split the entity's history in two
    #     buckets and left the older half uncompensated.
    # The rewrite itself still applies to every event, traces included, so
    # a genuine split keeps user events and their traces on the same key.
    compensated_eids = {
        e.restored_from_event_id for e in events
        if _is_trace(e) and not _is_invalidated(e)
        and e.restored_from_event_id is not None
    }
    fp_split_eid, fid_recycle_splits = _detect_fid_recycle([
        e for e in events
        if not _is_trace(e) and e.event_id not in compensated_eids
    ])
    for fp, first_eid, second_eid in fid_recycle_splits:
        flog(f"rewind_dedup: fid_recycle_detected fp={fp} splits=2 "
             f"first_eid={first_eid} second_eid={second_eid}")
    if fp_split_eid:
        events = _apply_fid_recycle_rewrite(events, fp_split_eid)

    # USER events only. The compensated span below is anchored on the event a
    # trace says it compensated, and that anchor is only meaningful if it is a
    # real user edit. When a trace references another trace (chained rewinds),
    # taking the older trace as the low bound stretched the span across the
    # whole period between the two rewinds, and every user edit made in
    # between was neutralised although nothing had ever compensated it -- the
    # very hole the span was introduced to close.
    by_eid: Dict[int, AuditEvent] = {
        e.event_id: e for e in events
        if e.event_id is not None and not _is_trace(e)
    }

    neutralised_user_eids: set = set()
    # Per entity: the compensated WINDOWS, not the whole entity (I-8).
    # {entity_key: [[lo, hi], ...]} -- a LIST, one interval per rewind, never
    # merged into a single min/max. Merging looked harmless and was not: two
    # rewinds on the same entity produced one interval spanning from the first
    # compensated event to the last trace, so any edit made BETWEEN the two
    # rewinds fell inside it and was neutralised although nothing had ever
    # compensated it.
    compensated_span: Dict[str, list] = {}
    user_events: List[AuditEvent] = []
    trace_count = 0
    invalidated_count = 0
    for event in events:
        if _is_trace(event):
            trace_count += 1
            if _is_invalidated(event):
                invalidated_count += 1
                continue
            ref = event.restored_from_event_id
            if ref is None:
                continue
            neutralised_user_eids.add(ref)
            key = _entity_key(event)
            # A rewind compensates a contiguous SUFFIX of the entity's
            # history: everything from its cutoff up to the moment the
            # trace was written. The span below marks exactly that
            # interval. Chain fusion means a single trace can stand for
            # several real events (the synthetic event carries only the
            # oldest event_id), which is why the referenced event_id
            # alone is not enough to neutralise the whole compensated
            # chain -- hence the span rather than a bare eid set.
            src = by_eid.get(ref)
            if src is None:
                # Source purged by retention, belonging to another
                # datasource, or itself a trace. Degenerate span [trace,
                # trace]: only the exact event_id stays neutralised, nothing
                # is swallowed by range. Erring wide here re-opens the hole.
                flog(f"rewind_dedup: trace eid={event.event_id} references "
                     f"eid={ref} which is not a user event in this window; "
                     f"span narrowed to the trace itself", "WARNING")
            lo = _order_key(src if src is not None else event)
            hi = _order_key(event)
            compensated_span.setdefault(key, []).append((lo, hi))
            continue
        user_events.append(event)

    active = []
    dropped = []
    for e in user_events:
        if e.event_id is None:
            active.append(e)
            continue
        if e.event_id in neutralised_user_eids:
            dropped.append(e)
            continue
        spans = compensated_span.get(_entity_key(e)) or ()
        key_e = _order_key(e)
        # Inside ONE of the compensated intervals => already undone by that
        # rewind. Between two intervals, or after the last one => a user edit
        # that no rewind has ever compensated; it MUST stay active, otherwise
        # the entity never returns to its cutoff state and the dialog wrongly
        # reports "nothing to restore" (rw_dedup_post_trace_edit,
        # tx_rewind_broken_trace_memory).
        if any(lo <= key_e < hi for lo, hi in spans):
            dropped.append(e)
        else:
            active.append(e)

    flog(f"rewind_dedup: {len(events)} raw "
         f"({len(user_events)} user, {trace_count} traces, "
         f"{invalidated_count} invalidated) -> "
         f"{len(active)} active "
         f"({len(neutralised_user_eids)} by eid, "
         f"{len(compensated_span)} compensated spans, "
         f"{len(dropped)} total neutralised)")
    # Deliberately NOT gated behind RECOVERLAND_HEAVY_DIAG, unlike the
    # other per-event dumps: `rw_dedup_post_trace_edit` asserts on
    # `rewind_dedup: neutralised eid=<n>`, so these lines are a contract,
    # not diagnostics. The cost is bounded by the number of ALREADY
    # COMPENSATED events (`dropped` is empty on a first rewind) and was
    # measured in the tens of ms -- it is not part of the freeze this
    # invariant is about.
    for e in dropped:
        flog(f"rewind_dedup: neutralised eid={e.event_id} "
             f"op={e.operation_type} "
             f"identity={(e.feature_identity_json or '')[:80]}")

    chained, redundant = _collapse_user_chain_with_stats(active)
    stats = {
        "raw": len(events),
        "user": len(user_events),
        "traces": trace_count,
        "traces_invalidated": invalidated_count,
        "traces_active": trace_count - invalidated_count,
        "dedup_dropped": len(dropped),
        "dedup_redundant": redundant,
        "dedup_active": len(chained),
    }
    return chained, stats


_MAX_CHAIN = 10


def _collapse_user_chain_with_stats(
    events: List[AuditEvent],
) -> Tuple[List[AuditEvent], int]:
    """Same as _collapse_user_chain but also returns the count of events
    eliminated by the chain collapse (raw_in - len(result))."""
    result = _collapse_user_chain(events)
    return result, max(len(events) - len(result), 0)


def _collapse_user_chain(events: List[AuditEvent]) -> List[AuditEvent]:
    """Per-entity collapse on user-only events ordered DESC (newest first).

    Strategy: keep the entire per-entity chain unchanged so that the
    restore planner can unwind the entity step by step in DESC order
    (apply compensation for newest event first, then for older events,
    each step bringing the entity to the state captured by the next
    older event). This is safe because every individual event carries
    a self-consistent OLD snapshot and the planner already orders
    actions per-phase.

    Collapse rules:
      - INSERT(oldest) -> only UPDATEs -> DELETE(newest): net no-op,
        skip the entire chain. Intermediate non-UPDATEs (e.g. a second
        INSERT from fid reuse) prevent the skip so no events are lost.
      - UPDATE(oldest) -> only UPDATEs -> DELETE(newest): fuse into a
        single synthetic DELETE carrying the oldest UPDATE's OLD state
        (= cutoff state).  This eliminates the Phase 1 UPDATE comp that
        would fail with target_absent because the feature was deleted
        and Phase 2 (INSERT) has not yet re-created it.  The synthetic
        DELETE's INSERT compensation restores the entity directly to
        its cutoff state in a single action (BL-RW-P1-23-A2).
      - Chain longer than _MAX_CHAIN: fuse into a synthetic event pair
        (oldest + newest) to cap the number of compensatory actions.
        The synthetic oldest keeps OLD geometry/attrs from the real
        oldest, and the synthetic newest carries the identity and NEW
        geometry from the real newest, ensuring post-state lookups
        still match the live feature.
      - Otherwise: keep all events in the chain.

    Naive single-event collapses destroy NEW-side information when the
    chain has multiple events on the same entity (e.g. U->U or I->U),
    which breaks post-state lookups that rely on the live feature still
    matching the captured NEW.
    """
    if len(events) <= 1:
        return list(events)

    buckets: Dict[str, List[AuditEvent]] = {}
    order: list = []

    for event in events:
        key = _entity_key(event)
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(event)

    result: List[AuditEvent] = []
    skipped_entities = 0
    fused_entities = 0
    cancelled_pairs = 0

    for key in order:
        chain = buckets[key]
        if len(chain) == 1:
            result.append(chain[0])
            continue

        # RW-20: cancel internal INSERT→DELETE pairs (sub-lifetimes that
        # were both born and died within the rewind window).
        chain_before = len(chain)
        chain = _cancel_internal_lifetimes(chain)
        cancelled_pairs += (chain_before - len(chain))
        if not chain:
            skipped_entities += 1
            continue
        if len(chain) == 1:
            result.append(chain[0])
            continue

        newest = chain[0]
        oldest = chain[-1]
        first_op = oldest.operation_type
        last_op = newest.operation_type

        if (first_op == "INSERT" and last_op == "DELETE" and _intermediates_all_updates(chain)):
            skipped_entities += 1
            continue

        if (first_op == "UPDATE" and last_op == "DELETE" and _intermediates_all_updates(chain)):
            fused = _fuse_update_delete(chain)
            result.append(fused)
            fused_entities += 1
            continue

        if len(chain) > _MAX_CHAIN:
            fused = _fuse_long_chain(chain)
            result.extend(fused)
            fused_entities += 1
            continue

        result.extend(chain)

    raw = len(events)
    deduped = len(result)
    if raw != deduped or fused_entities or cancelled_pairs:
        flog(f"rewind_dedup: user_chain {raw} -> {deduped} events"
             f" ({raw - deduped} redundant, {skipped_entities} no-op,"
             f" {fused_entities} fused, {cancelled_pairs} pair-cancelled RW-20)")

    return result


def _cancel_internal_lifetimes(chain_desc: List[AuditEvent]) -> List[AuditEvent]:
    """Cancel paired INSERT→DELETE sub-lifetimes within a single-entity chain.

    Walk chronologically (ASC). Each INSERT opens a lifetime; each DELETE
    closes the most recent open lifetime. Events of closed lifetimes are
    dropped (feature was created and destroyed inside the rewind window;
    no compensation needed). Events not enclosed in a closed lifetime
    (orphan DELETE/UPDATE before first INSERT, or events of the still-open
    final lifetime) are preserved.

    Returns events in the original DESC order. Empty list = entire chain
    cancels (caller treats as no-op).
    """
    chain_asc = list(reversed(chain_desc))
    open_lifetimes: List[List[AuditEvent]] = []
    orphans: List[AuditEvent] = []  # events with no enclosing INSERT
    closed_count = 0
    for event in chain_asc:
        op = event.operation_type
        if op == "INSERT":
            open_lifetimes.append([event])
        elif op == "DELETE":
            if open_lifetimes:
                open_lifetimes.pop()  # paired lifetime cancelled
                closed_count += 1
            else:
                orphans.append(event)
        else:  # UPDATE or other
            if open_lifetimes:
                open_lifetimes[-1].append(event)
            else:
                orphans.append(event)
    if closed_count == 0:
        return chain_desc  # nothing cancelled, preserve original
    survivors_asc: List[AuditEvent] = list(orphans)
    for lt in open_lifetimes:
        survivors_asc.extend(lt)
    return list(reversed(survivors_asc))


def _changed_only(event: AuditEvent) -> Dict[str, dict]:
    """The ``changed_only`` delta of an event, {} when absent or invalid."""
    if not event.attributes_json:
        return {}
    try:
        payload = json.loads(event.attributes_json)
    except (ValueError, TypeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    changed = payload.get("changed_only")
    return changed if isinstance(changed, dict) else {}


def _oldest_old_geometry(chain: List[AuditEvent]) -> Optional[bytes]:
    """OLD geometry of the oldest UPDATE of *chain* that carries one.

    *chain* is DESC (newest first), so overwriting while walking forward
    ends on the oldest -- i.e. the geometry the feature had at the cutoff.
    An attribute-only UPDATE stores no geometry, so the walk must continue
    instead of concluding "geometry unchanged" from the oldest event alone.
    """
    geom = None
    for event in chain:
        if event.operation_type == "UPDATE" and event.geometry_wkb is not None:
            geom = event.geometry_wkb
    return geom


def _intermediates_all_updates(chain: List[AuditEvent]) -> bool:
    """Return True when every event between oldest and newest is UPDATE."""
    for event in chain[1:-1]:
        if event.operation_type != "UPDATE":
            return False
    return True


def _fuse_long_chain(chain: List[AuditEvent]) -> List[AuditEvent]:
    """Fuse a chain longer than _MAX_CHAIN into at most 2 synthetic events.

    Preserves:
      - oldest event's OLD geometry/attrs (= state at window boundary)
      - newest event's identity and NEW geometry (= live feature state)
    The newest is patched with the oldest's geometry_wkb so that a
    single UPDATE compensation brings the feature from NEW back to OLD.
    """
    newest = chain[0]
    oldest = chain[-1]
    first_op = oldest.operation_type
    last_op = newest.operation_type

    if (first_op == "INSERT" and last_op == "DELETE" and _intermediates_all_updates(chain)):
        return []

    if first_op == last_op == "UPDATE":
        # Merge every delta of the chain, not just the oldest one: a field
        # edited by an intermediate UPDATE is absent from the oldest event
        # and would never be reverted (BL-RW fuse completeness). chain is
        # DESC, so the OLD side keeps being overwritten until the oldest
        # event wins (= cutoff value) while the NEW side stays on the
        # newest (= live value, needed for post-state lookups).
        merged: Dict[str, dict] = {}
        for event in chain:
            for field, val in _changed_only(event).items():
                old = extract_delta_old(val)
                if field in merged:
                    merged[field]["old"] = old
                else:
                    merged[field] = {
                        "old": old, "new": extract_delta_new(val),
                    }
        synthetic = oldest._replace(
            feature_identity_json=newest.feature_identity_json,
            entity_fingerprint=newest.entity_fingerprint,
            new_geometry_wkb=newest.new_geometry_wkb,
        )
        if merged:
            synthetic = synthetic._replace(
                attributes_json=json.dumps(
                    {"changed_only": merged}, ensure_ascii=False),
            )
        cutoff_geom = _oldest_old_geometry(chain)
        if cutoff_geom is not None:
            synthetic = synthetic._replace(geometry_wkb=cutoff_geom)
        flog(f"rewind_dedup: fused {len(chain)} UPDATEs into 1 "
             f"synthetic UPDATE (oldest_eid={oldest.event_id} "
             f"newest_eid={newest.event_id} "
             f"merged_fields={len(merged)})")
        return [synthetic]

    flog(f"rewind_dedup: fused {len(chain)} events into 2 "
         f"(oldest={first_op} eid={oldest.event_id}, "
         f"newest={last_op} eid={newest.event_id})")
    return [newest, oldest]


def _fuse_update_delete(chain: List[AuditEvent]) -> AuditEvent:
    """Fuse an UPDATE->DELETE chain into a single synthetic DELETE.

    For entities that existed before the rewind window and were UPDATEd
    then DELETEd, the cutoff state is the oldest UPDATE's OLD state.
    By fusing into a synthetic DELETE with that state, a single INSERT
    compensation (Phase 2) restores the entity to its cutoff state,
    eliminating the Phase 1 UPDATE comp that would fail with target_absent
    (the feature was deleted, and Phase 2 has not yet re-inserted it).

    The synthetic DELETE carries:
      - operation_type = "DELETE" (compensation = INSERT)
      - geometry_wkb = old geometry of the oldest UPDATE that carries one
        (pre-UPDATE = cutoff), with fallback to the newest DELETE's
        geometry when every UPDATE of the chain was attribute-only
      - attributes_json = the COMPLETE state at cutoff
        (format: {"all_attributes": {...}}) so the INSERT compensation
        restores every field
      - identity = newest DELETE's identity (same FID)

    Building the full state matters: the compensation for a DELETE is a
    re-INSERT and `restore_executor._buffer_insert` only writes the fields
    present in ``all_attributes``. Seeding it from the oldest UPDATE's
    delta (which holds the edited fields ONLY) brought the feature back
    with every other field NULL -- silent data loss on the ordinary
    "edit one attribute, then delete" sequence. The DELETE event carries
    the full state at deletion, so rolling the UPDATE deltas back over it
    reconstructs the cutoff state exactly.
    """
    from .search_service import reconstruct_attributes

    oldest = chain[-1]
    newest = chain[0]

    cutoff_attrs = dict(reconstruct_attributes(newest))
    if not cutoff_attrs:
        # Defensive: a DELETE with no all_attributes payload. Degrade to
        # the previous behaviour rather than re-inserting an empty feature.
        cutoff_attrs = dict(reconstruct_attributes(oldest))
        flog(f"rewind_dedup: fuse_update_delete no full snapshot on "
             f"DELETE eid={newest.event_id}, degraded to oldest UPDATE "
             f"delta ({len(cutoff_attrs)} field(s))", "WARNING")
    # chain is DESC: iterating forward applies newest -> oldest, so the
    # OLDEST old value wins = the value the field had at the cutoff.
    for event in chain:
        if event.operation_type != "UPDATE":
            continue
        for field, val in _changed_only(event).items():
            cutoff_attrs[field] = extract_delta_old(val)

    synthetic_attrs = json.dumps(
        {"all_attributes": cutoff_attrs}, ensure_ascii=False)
    synthetic_geom = _oldest_old_geometry(chain)
    if synthetic_geom is None:
        synthetic_geom = newest.geometry_wkb

    synthetic = oldest._replace(
        operation_type="DELETE",
        attributes_json=synthetic_attrs,
        geometry_wkb=synthetic_geom,
        feature_identity_json=newest.feature_identity_json,
        entity_fingerprint=newest.entity_fingerprint,
    )
    flog(f"rewind_dedup: fused UPDATE->DELETE chain ({len(chain)} events) "
         f"into 1 synthetic DELETE (oldest_eid={oldest.event_id} "
         f"newest_eid={newest.event_id})")
    return synthetic
