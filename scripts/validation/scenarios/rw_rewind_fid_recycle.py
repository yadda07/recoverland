"""Scenario BL-RW-P1-23-A2: FID recycle splits distinct logical entities.

Standalone proof that rewind_dedup detects an INSERT -> DELETE -> INSERT
sequence on the same FID and keeps both logical entities distinct.
Without the split, the second INSERT would be merged into the first
entity's bucket, leading to a single (usually no-op) chain and silently
dropping the second feature.
"""
import json
from typing import List, Dict, Tuple, Optional, NamedTuple

SCENARIO_ID = "rw_rewind_fid_recycle"
INVARIANT = "BL-RW-P1-23"

_FP = "datasource_shapefile_fp"


class AuditEvent(NamedTuple):
    event_id: Optional[int]
    project_fingerprint: str
    datasource_fingerprint: str
    layer_id_snapshot: str
    layer_name_snapshot: str
    provider_type: str
    feature_identity_json: str
    operation_type: str
    attributes_json: str
    geometry_wkb: Optional[bytes]
    geometry_type: str
    crs_authid: Optional[str]
    field_schema_json: str
    user_name: str
    session_id: Optional[str]
    created_at: str
    restored_from_event_id: Optional[int]
    entity_fingerprint: Optional[str] = None
    event_schema_version: Optional[int] = None
    new_geometry_wkb: Optional[bytes] = None
    invalidated_at: Optional[str] = None


class _FakeLogger:
    def __init__(self):
        self.messages: List[str] = []

    def __call__(self, msg: str, level: str = "INFO") -> None:
        self.messages.append(f"[{level}] {msg}")


flog = _FakeLogger()


def _entity_key(event: AuditEvent) -> str:
    if event.entity_fingerprint:
        return f"{event.datasource_fingerprint}::{event.entity_fingerprint}"
    return f"{event.datasource_fingerprint}::{event.feature_identity_json}"


def _detect_fid_recycle(
    events: List[AuditEvent],
) -> Tuple[Dict[str, int], List[Tuple[str, int, int]]]:
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
            if state == "closed":
                fp_split_eid[fp] = e.event_id
                splits.append((fp, fp_first_eid[fp], e.event_id))
                fp_first_eid[fp] = e.event_id
            elif state is None:
                fp_first_eid[fp] = e.event_id
            fp_state[fp] = "open"
        elif op == "DELETE":
            if state == "open":
                fp_state[fp] = "closed"
    return fp_split_eid, splits


def _apply_fid_recycle_rewrite(
    events: List[AuditEvent],
    fp_split_eid: Dict[str, int],
) -> List[AuditEvent]:
    if not fp_split_eid:
        return events
    rewritten: List[AuditEvent] = []
    for e in events:
        fp = e.entity_fingerprint
        if (fp and fp in fp_split_eid and
                e.event_id is not None and
                e.event_id >= fp_split_eid[fp]):
            new_fp = f"{fp}@{fp_split_eid[fp]}"
            rewritten.append(e._replace(entity_fingerprint=new_fp))
        else:
            rewritten.append(e)
    return rewritten


def _is_trace(event: AuditEvent) -> bool:
    return getattr(event, "restored_from_event_id", None) is not None


def _is_invalidated(event: AuditEvent) -> bool:
    return getattr(event, "invalidated_at", None) is not None


_MAX_CHAIN = 10


def _cancel_internal_lifetimes(chain_desc: List[AuditEvent]) -> List[AuditEvent]:
    chain_asc = list(reversed(chain_desc))
    open_lifetimes: List[List[AuditEvent]] = []
    orphans: List[AuditEvent] = []
    closed_count = 0
    for event in chain_asc:
        op = event.operation_type
        if op == "INSERT":
            open_lifetimes.append([event])
        elif op == "DELETE":
            if open_lifetimes:
                open_lifetimes.pop()
                closed_count += 1
            else:
                orphans.append(event)
        else:
            if open_lifetimes:
                open_lifetimes[-1].append(event)
            else:
                orphans.append(event)
    if closed_count == 0:
        return chain_desc
    survivors_asc: List[AuditEvent] = list(orphans)
    for lt in open_lifetimes:
        survivors_asc.extend(lt)
    return list(reversed(survivors_asc))


def _intermediates_all_updates(chain: List[AuditEvent]) -> bool:
    for event in chain[1:-1]:
        if event.operation_type != "UPDATE":
            return False
    return True


def _fuse_long_chain(chain: List[AuditEvent]) -> List[AuditEvent]:
    newest = chain[0]
    oldest = chain[-1]
    first_op = oldest.operation_type
    last_op = newest.operation_type
    if (first_op == "INSERT" and last_op == "DELETE" and
            _intermediates_all_updates(chain)):
        return []
    if first_op == last_op == "UPDATE":
        synthetic = oldest._replace(
            feature_identity_json=newest.feature_identity_json,
            entity_fingerprint=newest.entity_fingerprint,
            new_geometry_wkb=newest.new_geometry_wkb,
        )
        return [synthetic]
    return [newest, oldest]


def _collapse_user_chain(events: List[AuditEvent]) -> List[AuditEvent]:
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
    for key in order:
        chain = buckets[key]
        if len(chain) == 1:
            result.append(chain[0])
            continue
        chain_before = len(chain)
        chain = _cancel_internal_lifetimes(chain)
        if not chain:
            continue
        if len(chain) == 1:
            result.append(chain[0])
            continue
        newest = chain[0]
        oldest = chain[-1]
        first_op = oldest.operation_type
        last_op = newest.operation_type
        if (first_op == "INSERT" and last_op == "DELETE" and
                _intermediates_all_updates(chain)):
            continue
        if len(chain) > _MAX_CHAIN:
            fused = _fuse_long_chain(chain)
            result.extend(fused)
            continue
        result.extend(chain)
    return result


def _collapse_user_chain_with_stats(
    events: List[AuditEvent],
) -> Tuple[List[AuditEvent], int]:
    result = _collapse_user_chain(events)
    return result, max(len(events) - len(result), 0)


def collapse_rewind_events_with_stats(
    events: List[AuditEvent],
) -> Tuple[List[AuditEvent], Dict[str, int]]:
    if not events:
        return [], {
            "raw": 0, "user": 0,
            "traces": 0, "traces_invalidated": 0, "traces_active": 0,
            "dedup_active": 0, "dedup_dropped": 0, "dedup_redundant": 0,
        }
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
    flog(f"rewind_dedup: {len(events)} raw ({len(user_events)} user, "
         f"{trace_count} traces, {invalidated_count} invalidated) -> "
         f"{len(active)} active ({len(neutralised_user_eids)} by eid, "
         f"{len(neutralised_entity_keys)} entity keys, "
         f"{len(dropped)} total neutralised)")
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


def _event(eid: int, op: str, fid: int, created: str) -> AuditEvent:
    identity = json.dumps({"fid": fid})
    return AuditEvent(
        event_id=eid,
        project_fingerprint="proj",
        datasource_fingerprint=_FP,
        layer_id_snapshot="layer_id",
        layer_name_snapshot="layer_name",
        provider_type="ogr",
        feature_identity_json=identity,
        operation_type=op,
        attributes_json=json.dumps({}),
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:4326",
        field_schema_json="[]",
        user_name="test",
        session_id="session",
        created_at=created,
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=5,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def setup(ctx):
    # collapse_rewind_events expects DESC order (newest first).
    events = [
        _event(3, "INSERT", 1, "2026-01-01T12:00:00"),
        _event(2, "DELETE", 1, "2026-01-01T11:00:00"),
        _event(1, "INSERT", 1, "2026-01-01T10:00:00"),
    ]
    ctx.data["events"] = events


def run(ctx):
    events = ctx.data["events"]
    active, stats = collapse_rewind_events_with_stats(events)
    ctx.data["active"] = active
    ctx.data["stats"] = stats
    ctx.data["logs"] = list(flog.messages)


def assertions(ctx):
    active = ctx.data["active"]
    stats = ctx.data["stats"]
    logs = ctx.data["logs"]
    detected = any("fid_recycle_detected" in m for m in logs)
    keys_after_rewrite = set()
    for e in ctx.data["events"]:
        if e.event_id >= 3:
            keys_after_rewrite.add(_entity_key(e._replace(entity_fingerprint=f"fid:1@3")))
        else:
            keys_after_rewrite.add(_entity_key(e))
    active_keys = {_entity_key(e) for e in active}
    first_fp_present = any(e.entity_fingerprint == "fid:1" for e in active)
    second_fp_present = any("fid:1@3" in (e.entity_fingerprint or "") for e in active)
    return [
        ("fid_recycle_detected", detected, "log contains fid_recycle_detected"),
        ("split_key_created", any("fid:1@3" in k for k in active_keys),
         f"active_keys={active_keys} expected split key fid:1@3"),
        ("first_lifetime_cancelled", not first_fp_present,
         f"first lifetime fp=fid:1 still active={first_fp_present}"),
        ("second_lifetime_preserved", second_fp_present,
         f"second lifetime fp=fid:1@3 missing in active={active_keys}"),
        ("no_silent_merge", stats["dedup_active"] >= 1,
         f"dedup_active={stats['dedup_active']} expected >=1"),
    ]
