"""Rewind event deduplication for temporal restore.

Given N raw events after cutoff (ordered DESC by event_id),
collapse them to the minimal effective set per entity.

Two kinds of events live in the journal:
  - User events: original DELETE / UPDATE / INSERT performed by the user.
  - Trace events: written by a previous restore to record that a user
    event has been compensated (restored_from_event_id != None).

Pipeline:
  1. Drop trace events; collect the set of user event_ids they reference
     (already-compensated user events).
  2. Drop those compensated user events from the active set: replaying
     their compensatory action would duplicate the work and accumulate
     features in the layer.
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
from typing import List, Dict, Tuple

from .audit_backend import AuditEvent
from .logger import flog


def _entity_key(event: AuditEvent) -> str:
    if event.entity_fingerprint:
        return f"{event.datasource_fingerprint}::{event.entity_fingerprint}"
    return f"{event.datasource_fingerprint}::{event.feature_identity_json}"


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
    fp_split_eid: Dict[str, int] = {}
    splits: List[Tuple[str, int, int]] = []
    sorted_events = sorted(
        [e for e in events
         if e.event_id is not None and e.entity_fingerprint],
        key=lambda e: e.event_id,
    )
    fp_state: Dict[str, str] = {}
    fp_first_eid: Dict[str, int] = {}
    for e in sorted_events:
        fp = e.entity_fingerprint
        op = e.operation_type
        state = fp_state.get(fp)
        if op == "INSERT":
            if state in ("closed", "pre_existing"):
                fp_split_eid[fp] = e.event_id
                splits.append((fp, fp_first_eid.get(fp), e.event_id))
                fp_first_eid[fp] = e.event_id
            elif state is None:
                fp_first_eid[fp] = e.event_id
            fp_state[fp] = "open"
        elif op == "DELETE":
            if state in ("open", "pre_existing", None):
                fp_state[fp] = "closed"
        elif op == "UPDATE":
            if state is None:
                fp_state[fp] = "pre_existing"
                fp_first_eid[fp] = e.event_id
            elif state == "closed":
                fp_split_eid[fp] = e.event_id
                splits.append((fp, fp_first_eid.get(fp), e.event_id))
                fp_first_eid[fp] = e.event_id
                fp_state[fp] = "pre_existing"
            # state "open" or "pre_existing": no change
    return fp_split_eid, splits


def _apply_fid_recycle_rewrite(
    events: List[AuditEvent],
    fp_split_eid: Dict[str, int],
) -> List[AuditEvent]:
    """Rewrite entity_fingerprint to fp@<split_eid> for events on or
    after a detected FID-recycle split.

    Returns a new list; input is not mutated. Events without a matching
    split are returned unchanged.
    """
    if not fp_split_eid:
        return events
    rewritten: List[AuditEvent] = []
    for e in events:
        fp = e.entity_fingerprint
        if (fp and fp in fp_split_eid and e.event_id is not None and e.event_id >= fp_split_eid[fp]):
            new_fp = f"{fp}@{fp_split_eid[fp]}"
            rewritten.append(e._replace(entity_fingerprint=new_fp))
        else:
            rewritten.append(e)
    return rewritten


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
    fp_split_eid, fid_recycle_splits = _detect_fid_recycle(events)
    for fp, first_eid, second_eid in fid_recycle_splits:
        flog(f"rewind_dedup: fid_recycle_detected fp={fp} splits=2 "
             f"first_eid={first_eid} second_eid={second_eid}")
    if fp_split_eid:
        events = _apply_fid_recycle_rewrite(events, fp_split_eid)

    neutralised_user_eids: set = set()
    neutralised_entity_keys: set = set()
    eid_to_entity_key: Dict[int, str] = {}
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
            if ref is not None:
                neutralised_user_eids.add(ref)
                key = _entity_key(event)
                neutralised_entity_keys.add(key)
            continue
        user_events.append(event)
        if event.event_id is not None:
            eid_to_entity_key[event.event_id] = _entity_key(event)

    active = []
    dropped = []
    for e in user_events:
        if e.event_id is None:
            active.append(e)
            continue
        eid_key = eid_to_entity_key.get(e.event_id)
        if e.event_id in neutralised_user_eids or eid_key in neutralised_entity_keys:
            dropped.append(e)
        else:
            active.append(e)

    flog(f"rewind_dedup: {len(events)} raw "
         f"({len(user_events)} user, {trace_count} traces, "
         f"{invalidated_count} invalidated) -> "
         f"{len(active)} active "
         f"({len(neutralised_user_eids)} by eid, "
         f"{len(neutralised_entity_keys)} entity keys, "
         f"{len(dropped)} total neutralised)")
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
        synthetic = oldest._replace(
            feature_identity_json=newest.feature_identity_json,
            entity_fingerprint=newest.entity_fingerprint,
            new_geometry_wkb=newest.new_geometry_wkb,
        )
        flog(f"rewind_dedup: fused {len(chain)} UPDATEs into 1 "
             f"synthetic UPDATE (oldest_eid={oldest.event_id} "
             f"newest_eid={newest.event_id})")
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
      - geometry_wkb = oldest UPDATE's old geometry (pre-UPDATE = cutoff)
        with fallback to newest DELETE's geometry when the UPDATE was
        attribute-only (geometry_wkb is None)
      - attributes_json = full snapshot of oldest UPDATE's old attrs
        (format: {"all_attributes": {...}}) so the INSERT compensation
        restores all fields to their cutoff state
      - identity = newest DELETE's identity (same FID)
    """
    from .search_service import reconstruct_attributes

    oldest = chain[-1]
    newest = chain[0]

    old_attrs = reconstruct_attributes(oldest)
    synthetic_attrs = json.dumps(
        {"all_attributes": old_attrs}, ensure_ascii=False)
    synthetic_geom = (
        oldest.geometry_wkb
        if oldest.geometry_wkb is not None
        else newest.geometry_wkb
    )

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
