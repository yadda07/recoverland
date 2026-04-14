"""Rewind event deduplication for temporal restore.

Given N raw events after cutoff (ordered DESC by event_id),
collapse them to at most one effective event per entity.

Rules per entity (first_op = oldest after cutoff, last_op = most recent):

  first=INSERT, last!=DELETE  -> keep INSERT (will DELETE the entity)
  first=INSERT, last=DELETE   -> SKIP (created and destroyed after cutoff)
  first=UPDATE, last!=DELETE  -> keep oldest UPDATE (old_values = cutoff state)
  first=UPDATE, last=DELETE   -> keep DELETE then oldest UPDATE (re-insert + revert)
  first=DELETE, last=*        -> keep DELETE (re-insert from snapshot)

Zero QGIS dependency. Pure deterministic logic.
"""
from typing import List, Dict, Tuple

from .audit_backend import AuditEvent
from .logger import flog


def _entity_key(event: AuditEvent) -> str:
    if event.entity_fingerprint:
        return f"{event.datasource_fingerprint}::{event.entity_fingerprint}"
    return f"{event.datasource_fingerprint}::{event.feature_identity_json}"


def collapse_rewind_events(events: List[AuditEvent]) -> List[AuditEvent]:
    """Collapse raw rewind events to the minimal effective set.

    Args:
        events: ordered by event_id DESC (most recent first).

    Returns:
        Deduplicated list, still ordered DESC (most recent first
        within each entity group, but globally the order may shift).
    """
    if len(events) <= 1:
        return list(events)

    buckets: Dict[str, Tuple[AuditEvent, AuditEvent]] = {}
    order: list = []

    for event in events:
        key = _entity_key(event)
        if key not in buckets:
            buckets[key] = (event, event)
            order.append(key)
        else:
            newest, _oldest = buckets[key]
            buckets[key] = (newest, event)

    result: List[AuditEvent] = []
    skipped_entities = 0

    for key in order:
        newest, oldest = buckets[key]
        first_op = oldest.operation_type
        last_op = newest.operation_type

        if first_op == "INSERT":
            if last_op == "DELETE":
                skipped_entities += 1
                continue
            result.append(oldest)

        elif first_op == "UPDATE":
            if last_op == "DELETE":
                result.append(newest)
                result.append(oldest)
            else:
                result.append(oldest)

        elif first_op == "DELETE":
            result.append(oldest)

        else:
            result.append(oldest)

    raw = len(events)
    deduped = len(result)
    if raw != deduped:
        flog(f"rewind_dedup: {raw} -> {deduped} events"
             f" ({raw - deduped} redundant, {skipped_entities} no-op entities)")

    return result
