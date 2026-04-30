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
from typing import List, Dict

from .audit_backend import AuditEvent
from .logger import flog


def _entity_key(event: AuditEvent) -> str:
    if event.entity_fingerprint:
        return f"{event.datasource_fingerprint}::{event.entity_fingerprint}"
    return f"{event.datasource_fingerprint}::{event.feature_identity_json}"


def _is_trace(event: AuditEvent) -> bool:
    return getattr(event, "restored_from_event_id", None) is not None


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
    if not events:
        return []

    neutralised_user_eids = set()
    user_events: List[AuditEvent] = []
    trace_count = 0
    for event in events:
        if _is_trace(event):
            trace_count += 1
            ref = event.restored_from_event_id
            if ref is not None:
                neutralised_user_eids.add(ref)
            continue
        user_events.append(event)

    active = [e for e in user_events
              if e.event_id is None or e.event_id not in neutralised_user_eids]

    if trace_count or len(active) != len(user_events):
        flog(f"rewind_dedup: {len(events)} raw "
             f"({len(user_events)} user, {trace_count} traces) -> "
             f"{len(active)} active "
             f"({len(neutralised_user_eids)} neutralised by traces)")

    return _collapse_user_chain(active)


_MAX_CHAIN = 10


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

    for key in order:
        chain = buckets[key]
        if len(chain) == 1:
            result.append(chain[0])
            continue

        newest = chain[0]
        oldest = chain[-1]
        first_op = oldest.operation_type
        last_op = newest.operation_type

        if first_op == "INSERT" and last_op == "DELETE" \
                and _intermediates_all_updates(chain):
            skipped_entities += 1
            continue

        if len(chain) > _MAX_CHAIN:
            fused = _fuse_long_chain(chain)
            result.extend(fused)
            fused_entities += 1
            continue

        result.extend(chain)

    raw = len(events)
    deduped = len(result)
    if raw != deduped or fused_entities:
        flog(f"rewind_dedup: user_chain {raw} -> {deduped} events"
             f" ({raw - deduped} redundant, {skipped_entities} no-op,"
             f" {fused_entities} fused)")

    return result


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

    if first_op == "INSERT" and last_op == "DELETE" \
            and _intermediates_all_updates(chain):
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
