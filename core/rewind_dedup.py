# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Rewind event deduplication for temporal restore.

Given the rewind window of a cutoff T (see
``event_stream_repository.fetch_events_after_cutoff``), return the ordered
list of compensations that takes the layer from its current state to its
state at T.

Three kinds of rows reach the collapse:
  - User events: DELETE / UPDATE / INSERT performed by the user after T.
  - Trace events: written by a previous restore or rewind to record that
    an event has been compensated (restored_from_event_id != None).
  - Plan items: rows the window SYNTHESISED for the history before T
    (``PLAN_MARK_*`` in commit_id). They are not journal rows; they carry
    what must be written to undo a toggle that happened after T on an
    event older than T (a compensation made after T is undone, a
    compensation in effect at T but undone since is made again), or a
    precise refusal when the journal cannot rebuild that state.

What a trace covers (schema 7 and later, ``EXACT_TRACE_SCHEMA``)
---------------------------------------------------------------
A trace compensates EXACTLY the event it references, nothing else. Since
schema 7 the rewind no longer fuses a chain into one synthetic event: a
synthetic event reused the id of the oldest real event with another
payload, so its trace named one event and stood for several. Coverage had
to be GUESSED from a time span (every event of the entity between the
referenced event and the trace), and the guess was wrong whenever a trace
covered less than that span: a Mode A restore of ONE event, a lifetime of
a recycled FID, an action that soft-failed inside a committed layer. The
events it wrongly neutralised were never compensated, and the rewind
answered "nothing to restore" (finding span-neutralises-uncompensated).
The forward hop had the mirror defect: after a restart it rebuilt the
undo from the REAL event the trace named, not from the synthetic payload
the rewind had applied (finding forward-hop-real-vs-synthetic).

A trace that references another trace CANCELS it: that is how a rewind
re-applies an event a previous rewind had compensated (the forward hop is
a compensation of the trace). Only exact traces cancel; a trace-to-trace
row written before schema 7 was hand-made or migrated, its meaning is
unknown, and the older trace stays in effect (tx_rewind_broken_trace_memory
case O3).

Legacy traces (schema < 7)
--------------------------
Older rewinds did fuse. A legacy trace keeps the time-span coverage ONLY
when its shape proves the fusion: its operation is not the inverse of the
referenced event's (UPDATE->DELETE chain fused into a synthetic DELETE).
A same-operation legacy trace followed, on its entity, by at least
``_LEGACY_MAX_CHAIN - 1`` unreferenced user events up to the trace is
AMBIGUOUS: a long chain fused into one event, or a Mode A restore of one
event followed by edits nobody compensated. The two readings need
opposite plans and nothing in the journal tells them apart, so the rewind
is refused by name (``legacy_trace_shape``); it used to guess "fused" and
neutralise the edits silently. Any other legacy trace is exact. A legacy
trace whose source was purged by retention keeps the span down to the
oldest row of the window: nothing can prove what it covered. A span never
covers a row inserted AFTER the trace (larger event_id): the rewind that
wrote the trace never saw it (an edit re-imported from the pending file
keeps its old date but gets a new id).

Identities the journal cannot trust
-----------------------------------
Two cases put rows of ONE feature under two keys, or rows of two features
under one key, and every per-entity rule below would then pair the wrong
rows:
  - a feature a trace RE-INSERTED gets a new FID on a provider that
    allocates them (shapefile): the trace keeps the identity of the event
    it restored, the user's later edits carry the new FID. The plan then
    holds the delete of the re-inserted feature (a history item) and the
    re-insert compensating the user's later DELETE, in two buckets: they
    never cancel, and the executor refuses to delete a feature it has just
    re-inserted ("already absent"): the feature came back, reported done
    (``_pair_reinserted_lifetimes`` pairs them on their full snapshot);
  - a layer whose key has several columns, journaled before v7 as its
    FIRST column only (``pk:zone_id=12`` for every parcel of zone 12): the
    rows of different parcels share a bucket and were cancelled against
    each other (``prove_legacy_keys`` re-keys them);
  - a bare FID (a layer without a primary key) names a RECORD: a
    shapefile renumbers its records when an edit session that deleted one
    ends, so one key holds rows of two features, or one feature's rows
    sit under several keys.
The executor trusts one thing the collapse must therefore guarantee: rows
that share a key in the plan are ONE feature (it follows its remap from a
re-inserted feature to the UPDATE rows of the same key). What the journal
cannot prove is refused by name, never guessed: a re-key on a guess sent
the plan down the executor's resurrection path whenever the guess was
wrong (review findings composite-orphan-update, shp-split-precision,
external-edit-lifetime).

Writes that do not alternate
----------------------------
The net reading (an event is compensated or not; a root before T is
present at T or not) is exact while the writes on a feature alternate:
compensated, re-applied, compensated... A Mode A restore of an event the
user did again writes a SECOND compensation of the same event: physically
it undid the re-done edit. Read as a net, the field lands on the value
before the edit, or the deleted feature comes back, every proof passing
(review finding double-toggle). Then every write is undone at its own
place: the history items one per toggle (event_stream_repository), and
the window's user events one row per trace (``_window_per_write``),
including the edits interleaved with those items.

Pipeline:
  0. Legacy composite-key rows re-keyed (``prove_legacy_keys``).
  1. In-effect status of every trace (not invalidated, not cancelled).
  2. Neutralise the user events an in-effect trace compensates, except
     those whose writes are unwound one by one; refuse by name what a
     legacy trace makes ambiguous.
  3. Detect FID recycling on the surviving user events, the writes of the
     traces and of the history items included (a re-insert is not a
     recycle). A survivor that a later forward hop re-applied is placed at
     that re-application (``_place_reapplied_events``).
     A compensation whose target no key names (a legacy UPDATE of a
     composite key that nothing ties to a record, a legacy key of unknown
     columns naming several records) is refused (``_identity_refusals``).
  4. On a bare FID, rows the plan would link (a re-insert of the key after
     them) that cannot be proven to be one feature are refused
     (``_weak_lifeline_refusals``); a plan delete of a feature a trace
     re-inserted (or the user created) cancels that feature's rows under
     the other FID it carries since, when their states prove it is the
     same feature (``_pair_reinserted_lifetimes``); a creation and a
     deletion under two FIDs that the states can neither pair nor tell
     apart are refused (``_unprovable_lifetime_refusals``).
  5. Plan items join the survivors. Per-entity collapse of that plan,
     rules that only DROP rows, repeated until none applies: a delete
     directly followed by the re-insert of the same snapshot; a run of
     UPDATE rows that leaves the feature as it found it; a lifetime the
     plan would create then destroy (INSERT ... DELETE made after T; a
     feature re-inserted then deleted again; on a key that names a record
     only when the content or the edit session proves one feature, else
     refused). Any other row is kept with its own id and its own payload,
     so its compensation and its trace are exact.
  6. The result is sorted newest first by (created_at, event_id). A plan
     item's created_at is the instant of the toggle it undoes, which puts
     it at its place in that order.

Zero QGIS dependency. Pure deterministic logic.
"""
import json
from bisect import bisect_left
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from .audit_backend import AuditEvent
from .constants import GEOMETRY_NULL_MARKER
from .logger import flog
from .serialization import extract_delta_new, extract_delta_old
from .temporal_snapshot_engine import compute_entity_key


# From this schema on, a trace compensates exactly the event it references
# (no chain fusion) and a trace that references a trace cancels it.
EXACT_TRACE_SCHEMA = 7

# Before schema 7 a chain longer than this was fused into one event.
_LEGACY_MAX_CHAIN = 10

_INVERSE_OP = {"INSERT": "DELETE", "DELETE": "INSERT", "UPDATE": "UPDATE"}

# Plan items carry their kind in commit_id: they are not journal rows and
# belong to no QGIS commit. The trace a restore writes for them is built
# from scratch (build_restore_trace_event) and does not copy the field.
PLAN_MARK_PREFIX = "rewind-plan:"
# Re-apply an event older than T that a compensation made after T undid.
# event_id = that compensation (the trace), so the new trace cancels it.
PLAN_MARK_REAPPLY = PLAN_MARK_PREFIX + "reapply"
# Compensate again an event older than T whose compensation was in effect
# at T and has been undone since. event_id = the event itself.
PLAN_MARK_RECOMPENSATE = PLAN_MARK_PREFIX + "recompensate"
# The state at T needs a payload the journal no longer holds, or a legacy
# fused compensation that cannot be reversed exactly: the planner turns
# this row into a blocking conflict that names the reason.
PLAN_MARK_REFUSED = PLAN_MARK_PREFIX + "refused"
# Operation of a refusal row: unknown to every executor, so a path that
# does not recognise it fails closed ("unsupported_operation").
REFUSED_OP = "REFUSED"

# Shapes of a legacy trace (``legacy_trace_shape``).
LEGACY_EXACT = "exact"
LEGACY_FUSED = "fused"
LEGACY_AMBIGUOUS = "ambiguous"

# Suffix of the entity key of a row whose journaled key names several
# features (first column of a composite key): the row is keyed on its own,
# and refused if it still has to be compensated (``_identity_refusals``).
UNPROVEN_KEY_MARK = "#unproven:"

# Suffix of the entity key of a legacy row whose layer's key columns are
# unknown (no v7 row of the layer in the journal, no live layer given): the
# journaled column may be the first of several, so the key may name several
# features. Such a bucket is read like a bare FID (``_weak_key``).
LEGACY_KEY_MARK = "#keycols-unknown"


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
    traces: Optional[List[AuditEvent]] = None,
) -> Tuple[Dict[tuple, tuple], List[Tuple[str, int, int]]]:
    """Detect FID-recycle patterns within a single fp lifeline.

    Pattern of interest (BL-RW-P1-07, CR-1):
        INSERT(fp=X, t=A) -> DELETE(fp=X, t=B>A) -> INSERT(fp=X, t=C>B)

    Two distinct logical entities share entity_fingerprint='fid:X'
    because OGR/GPKG recycles the FID after the first DELETE. Without
    splitting, both end up in the same dedup bucket.

    Walks events in CHRONOLOGICAL order -- ``_order_key``, i.e.
    (created_at, event_id) -- and runs a per-fp state machine. Ordering on
    event_id alone was a silent disagreement with the rest of the pipeline
    (the SQL window orders on created_at first): an event held back in the
    pending file and re-inserted after a crash receives a RECENT event_id
    although it happened first, and the state machine then read that
    entity's life BACKWARDS. A create-then-delete pair replayed as
    DELETE->INSERT looked like a recycled FID, the lifeline was split in two
    buckets, the pair no longer cancelled, and the rewind RESURRECTED a
    feature the user had created and destroyed after the cutoff (scenario
    tx_rewind_clock_ties, cases T3/T4).

    The state machine:
        None          --INSERT--> open
        None          --DELETE--> closed       (pre-existing entity deleted)
        None          --UPDATE--> pre_existing (pre-existing entity modified)
        open          --DELETE--> closed
        closed        --DELETE--> closed + SPLIT (another record deleted)
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

    *traces*: the compensations of the window, read as the physical writes
    they are, never as a recycle: a trace that RE-INSERTED the feature
    (restore of its DELETE) reopens its lifeline, a trace that deleted it
    closes it. The plan items of the history stand for a write too, the
    one they undo, which is their own operation (a REAPPLY of a DELETE
    undoes a re-insert: INSERT), at that write's place. Without them, a feature deleted, restored by a trace, then
    edited again read as "closed, then UPDATE": a recycled FID. The split
    put the feature's later rows in another bucket than the rows the plan
    builds for its restores, and the rewind re-inserted it twice
    (duplicate key) or not at all (rw_state_at_t_toggles).

    Args:
        events: any iterable of AuditEvent. event_id=None and missing
            entity_fingerprint are skipped (defensive).

    Returns:
        fp_split: dict {(datasource, fp) -> (order_key, event_id) of the
            most recent SPLIT}. The order key is what the rewrite below
            compares against, for the same chronological reason.
        splits: list of (fp, first_eid, second_eid) for logging.
    """
    fp_split: Dict[tuple, tuple] = {}
    splits: List[Tuple[str, int, int]] = []
    writes = {id(t) for t in traces or ()}
    sorted_events = sorted(
        [e for e in list(events) + list(traces or ())
         if e.event_id is not None and e.entity_fingerprint],
        key=_order_key,
    )
    fp_state: Dict[tuple, str] = {}
    fp_first_eid: Dict[tuple, int] = {}
    for e in sorted_events:
        if id(e) in writes:
            fp_key = (e.datasource_fingerprint, e.entity_fingerprint)
            if e.operation_type == "INSERT":
                fp_first_eid.setdefault(fp_key, e.event_id)
                fp_state[fp_key] = "open"
            elif e.operation_type == "DELETE":
                fp_state[fp_key] = "closed"
            continue
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
                fp_split[fp_key] = (_order_key(e), e.event_id)
                splits.append((e.entity_fingerprint,
                               fp_first_eid.get(fp_key), e.event_id))
                fp_first_eid[fp_key] = e.event_id
            elif state is None:
                fp_first_eid[fp_key] = e.event_id
            fp_state[fp_key] = "open"
        elif op == "DELETE":
            if state == "closed":
                # Deleted twice with no re-insert between: the second
                # DELETE removed ANOTHER record under the same key (a
                # shapefile repack renumbered the records, or a legacy key
                # names several features). In one bucket the plan
                # re-inserted both, the executor's remap then pointed at
                # the newer one, and the UPDATE rows of the first feature
                # were written onto the second.
                fp_split[fp_key] = (_order_key(e), e.event_id)
                splits.append((e.entity_fingerprint,
                               fp_first_eid.get(fp_key), e.event_id))
                fp_first_eid[fp_key] = e.event_id
            fp_state[fp_key] = "closed"
        elif op == "UPDATE":
            if state is None:
                fp_state[fp_key] = "pre_existing"
                fp_first_eid[fp_key] = e.event_id
            elif state == "closed":
                fp_split[fp_key] = (_order_key(e), e.event_id)
                splits.append((e.entity_fingerprint,
                               fp_first_eid.get(fp_key), e.event_id))
                fp_first_eid[fp_key] = e.event_id
                fp_state[fp_key] = "pre_existing"
            # state "open" or "pre_existing": no change
    return fp_split, splits


def _apply_fid_recycle_rewrite(
    events: List[AuditEvent],
    fp_split: Dict[tuple, tuple],
) -> List[AuditEvent]:
    """Rewrite entity_fingerprint to fp@<split_eid> for events on or
    after a detected FID-recycle split.

    Keys are (datasource_fingerprint, entity_fingerprint) pairs, so a split
    detected in one layer never touches the identically-numbered feature of
    another layer.

    "On or after" is chronological (``_order_key``), like the detection
    itself: an event re-inserted from the pending file carries a recent
    event_id for an old edit, and comparing raw event_ids moved it into
    the recycled entity's bucket -- the two halves of the split then no
    longer matched the lifelines the state machine had seen.

    The label keeps the split event_id: it is what the log line names, and
    what makes the rewritten fingerprint readable in a diagnostic.

    Returns a new list; input is not mutated. Events without a matching
    split are returned unchanged.
    """
    if not fp_split:
        return events
    rewritten: List[AuditEvent] = []
    for e in events:
        fp = e.entity_fingerprint
        fp_key = (e.datasource_fingerprint, fp)
        split = fp_split.get(fp_key)
        if (fp and split is not None and e.event_id is not None
                and _order_key(e) >= split[0]):
            new_fp = f"{fp}@{split[1]}"
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

    A plan item sits at the place of the toggle it undoes: the created_at
    AND the event_id of that trace. The event_id alone matters: the traces
    of one run are written in a tight loop and often share a clock reading
    (Windows resolves datetime.now() to a few ms). Breaking such a tie on
    the item's own id (the id of the event it re-applies) swapped two
    compensations of the same field and left the older value.
    """
    return ((event.created_at or ""), _position_eid(event))


def chronological_key(event: AuditEvent) -> tuple:
    """The order the rewind unwinds in; shared with the planner."""
    return _order_key(event)


def undo_position(trace: AuditEvent) -> Tuple[str, int]:
    """Place, in that chronology, of the Undo that invalidated *trace*.

    (invalidated_at, -event_id). An Undo writes no event: its only date is
    the stamp it puts on the traces it reverted. Before this version one
    Undo stamped all of them with ONE instant, and a tie was broken on the
    trace id, i.e. on the order the undone run WROTE them: the reverse of
    the Undo's (a run compensates newest first, its Undo re-applies oldest
    first). Two recompensations of one field then ran in the wrong order
    and the field ended on the older NEW value, reported restored
    (rw_undo_tied_stamps_order). At one instant, the trace written last
    was undone first. The dialog now stamps each trace with its own
    instant, strictly increasing in the Undo's order: the id then decides
    nothing.
    """
    return (trace.invalidated_at or "", -int(trace.event_id or 0))


def _is_trace(event: AuditEvent) -> bool:
    return getattr(event, "restored_from_event_id", None) is not None


def _is_invalidated(event: AuditEvent) -> bool:
    return getattr(event, "invalidated_at", None) is not None


# ---------------------------------------------------------------------------
# Trace semantics, shared with the window (event_stream_repository)
# ---------------------------------------------------------------------------

def is_exact_trace(trace: AuditEvent) -> bool:
    """True when *trace* covers exactly the event it references."""
    return int(getattr(trace, "event_schema_version", None) or 0) >= EXACT_TRACE_SCHEMA


def plan_item_kind(event: AuditEvent) -> Optional[str]:
    """``PLAN_MARK_*`` of a synthesised row, None for a journal row."""
    mark = getattr(event, "commit_id", None)
    if isinstance(mark, str) and mark.startswith(PLAN_MARK_PREFIX):
        return mark.split("@", 1)[0]
    return None


def _position_eid(event: AuditEvent) -> int:
    """Tie-breaker of the chronology: the event_id of the toggle for a
    plan item (``<mark>@<trace event_id>``), the row's own otherwise."""
    mark = getattr(event, "commit_id", None)
    if isinstance(mark, str) and mark.startswith(PLAN_MARK_PREFIX) and "@" in mark:
        try:
            return int(mark.rsplit("@", 1)[1])
        except ValueError:
            pass
    return event.event_id or 0


def is_plan_item(event: AuditEvent) -> bool:
    return plan_item_kind(event) is not None


def refusal_reason(event: AuditEvent) -> str:
    """The reason a ``PLAN_MARK_REFUSED`` row carries, "" otherwise."""
    if plan_item_kind(event) != PLAN_MARK_REFUSED:
        return ""
    try:
        payload = json.loads(event.attributes_json or "{}")
    except (ValueError, TypeError):
        return "state at the cutoff cannot be rebuilt"
    reason = payload.get("_rewind_refusal") if isinstance(payload, dict) else None
    return str(reason or "state at the cutoff cannot be rebuilt")


def traces_in_effect(
    traces: List[AuditEvent],
    happened: Optional[Callable[[str], bool]] = None,
) -> Dict[int, bool]:
    """{trace event_id: in effect} at the instant *happened* describes.

    *happened(ts)* says whether an instant is at or before that moment;
    None means NOW (everything has happened). A trace is in effect when it
    has happened, has not been invalidated (undone) by then, and no exact
    trace in effect at that moment cancels it. Evaluated recursively: a
    canceller can itself be cancelled (forward hop, then rewind again).
    """
    by_eid = {t.event_id: t for t in traces if t.event_id is not None}
    cancellers: Dict[int, List[AuditEvent]] = {}
    for t in traces:
        cancellers.setdefault(t.restored_from_event_id, []).append(t)
    memo: Dict[int, bool] = {}

    def _eval(eid: int, visiting: set) -> bool:
        if eid in memo:
            return memo[eid]
        trace = by_eid[eid]
        ok = happened is None or happened(trace.created_at or "")
        if ok and _is_invalidated(trace):
            ok = happened is not None and not happened(trace.invalidated_at)
        if ok:
            visiting.add(eid)
            for child in cancellers.get(eid, ()):
                cid = child.event_id
                if cid is None or cid in visiting or not is_exact_trace(child):
                    continue
                if _eval(cid, visiting):
                    ok = False
                    break
            visiting.discard(eid)
        memo[eid] = ok
        return ok

    for eid in by_eid:
        _eval(eid, set())
    return memo


def cancelling_trace(root_eid: int, traces: List[AuditEvent],
                     status: Dict[int, bool]) -> Optional[AuditEvent]:
    """The in-effect trace that compensates the user event *root_eid*.

    Any trace referencing a USER event compensates it, legacy or exact.
    The newest wins when a journal holds several (a Mode A restore made
    twice): it is the one a new trace must cancel.
    """
    found = None
    for t in traces:
        if t.restored_from_event_id != root_eid or not status.get(t.event_id):
            continue
        if found is None or _order_key(t) > _order_key(found):
            found = t
    return found


def legacy_trace_shape(trace: AuditEvent, source: AuditEvent,
                       n_unreferenced_between: int) -> str:
    """What a trace may stand for: ``LEGACY_EXACT``, ``_FUSED`` or ``_AMBIGUOUS``.

    *n_unreferenced_between*: user events of the same entity between the
    source and the trace that no trace references.

    FUSED is proven by the shape: a Mode A restore of one UPDATE writes an
    UPDATE trace, never a re-insert, so an operation other than the
    inverse of the source's is the synthetic event of a fused chain.
    AMBIGUOUS is the count alone: a fused long chain and a restore of ONE
    event followed by that many untraced edits look the same. Guessing
    either way is a silent wrong state in the other case (fused read as
    exact: the chain is compensated a second time; exact read as fused:
    the edits are neutralised and never compensated), so callers refuse.
    """
    if is_exact_trace(trace):
        return LEGACY_EXACT
    if trace.operation_type != _INVERSE_OP.get(source.operation_type):
        return LEGACY_FUSED
    if n_unreferenced_between >= _LEGACY_MAX_CHAIN - 1:
        return LEGACY_AMBIGUOUS
    return LEGACY_EXACT


def legacy_fused_shape(trace: AuditEvent, source: AuditEvent,
                       n_unreferenced_between: int) -> bool:
    """True when a legacy trace may stand for a fused chain (fused or
    ambiguous: in both cases its undo cannot be exact)."""
    return legacy_trace_shape(
        trace, source, n_unreferenced_between) != LEGACY_EXACT


def _plan_row(template: AuditEvent, event_id: int, position: Tuple[str, int],
              mark: str, **changes) -> AuditEvent:
    """A synthesised plan row: *template* re-stamped, never a trace.

    *position* is (instant, event_id) of the toggle the row undoes: the
    instant becomes its created_at, the event_id rides on the mark.
    """
    stamp, toggle_eid = position
    return template._replace(
        event_id=event_id,
        created_at=stamp,
        restored_from_event_id=None,
        invalidated_at=None,
        commit_id=f"{mark}@{int(toggle_eid or 0)}",
        event_uid=None,
        **changes,
    )


def build_reapply_item(source: AuditEvent, trace: AuditEvent,
                       position: Tuple[str, int]) -> AuditEvent:
    """Plan row whose compensation RE-APPLIES *source* (undoes *trace*).

    The inverse of the source event: compensating an INSERT-like row
    deletes the feature the trace had re-inserted, compensating a
    DELETE-like row re-inserts the feature the trace had deleted, and an
    UPDATE with OLD and NEW swapped writes the source's NEW values back.
    It carries the trace's event_id, so the trace a rewind writes for it
    references the trace and cancels it.
    """
    op = source.operation_type
    if op == "UPDATE":
        payload = {}
        try:
            payload = json.loads(source.attributes_json or "{}")
        except (ValueError, TypeError):
            payload = {}
        changed = payload.get("changed_only") if isinstance(payload, dict) else None
        swapped = {
            field: {"old": extract_delta_new(val), "new": extract_delta_old(val)}
            for field, val in (changed or {}).items()
        }
        return _plan_row(
            source, trace.event_id, position, PLAN_MARK_REAPPLY,
            operation_type="UPDATE",
            attributes_json=json.dumps({"changed_only": swapped},
                                       ensure_ascii=False),
            geometry_wkb=source.new_geometry_wkb,
            new_geometry_wkb=source.geometry_wkb,
        )
    return _plan_row(
        source, trace.event_id, position, PLAN_MARK_REAPPLY,
        operation_type=_INVERSE_OP.get(op, op),
    )


def build_recompensate_item(source: AuditEvent,
                            position: Tuple[str, int],
                            via: Optional[AuditEvent] = None) -> AuditEvent:
    """Plan row that compensates *source* again, at the toggle's place.

    *via*: the trace whose write this row undoes, when that trace
    re-applied *source* (a trace cancelling a compensation, i.e. a forward
    hop). The row then carries the trace's event_id: the trace a rewind
    writes for it references that trace and cancels it, which puts the
    compensation it had cancelled back in effect. Without *via* the row
    carries the source's own id (a direct compensation).
    """
    eid = via.event_id if via is not None else source.event_id
    return _plan_row(source, eid, position, PLAN_MARK_RECOMPENSATE)


def build_refusal_item(anchor: AuditEvent, position: Tuple[str, int],
                       reason: str) -> AuditEvent:
    """Plan row the planner turns into a blocking conflict naming *reason*.

    *anchor* is the trace whose toggle cannot be undone: its event_id and
    identity name the refused entity.
    """
    return _plan_row(
        anchor, anchor.event_id, position, PLAN_MARK_REFUSED,
        operation_type=REFUSED_OP,
        attributes_json=json.dumps({"_rewind_refusal": reason},
                                   ensure_ascii=False),
        geometry_wkb=None,
        new_geometry_wkb=None,
    )


# ---------------------------------------------------------------------------
# Keys of a composite primary key journaled before v7
# ---------------------------------------------------------------------------

def _identity_dict(event: AuditEvent) -> dict:
    try:
        identity = json.loads(event.feature_identity_json or "{}")
    except (ValueError, TypeError):
        return {}
    return identity if isinstance(identity, dict) else {}


def _payload(event: AuditEvent) -> dict:
    try:
        payload = json.loads(event.attributes_json or "{}")
    except (ValueError, TypeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def key_columns_of_identity(identity: dict) -> Optional[List[str]]:
    """Key columns a v7 identity names; None when it names none (FID only)."""
    names = identity.get("pk_fields")
    if isinstance(names, list) and names:
        return [str(n) for n in names]
    if identity.get("pk_field"):
        return [str(identity["pk_field"])]
    return None


def journaled_key_columns(events: Sequence[AuditEvent]) -> Dict[str, List[str]]:
    """{datasource: key columns} as the NEWEST v7 user rows of each say.

    Since v7 a composite key is journaled whole (``pk_fields``), a
    single-column key as ``pk_field``, a keyless layer as a bare FID. The
    newest row naming a key wins: it describes the layer as it is now. A
    datasource whose v7 rows all name the FID only is keyless (``[]``); a
    single feature with a NULL key does not make a keyed layer keyless,
    hence the preference. A datasource with no v7 row is absent (unknown).
    """
    newest: Dict[str, Tuple[tuple, List[str]]] = {}
    keyless: set = set()
    for e in events:
        if _is_trace(e) or is_plan_item(e):
            continue
        if int(getattr(e, "event_schema_version", None) or 0) < EXACT_TRACE_SCHEMA:
            continue
        identity = _identity_dict(e)
        if not identity:
            continue
        ds = e.datasource_fingerprint or ""
        cols = key_columns_of_identity(identity)
        if cols is None:
            keyless.add(ds)
            continue
        key = _order_key(e)
        cur = newest.get(ds)
        if cur is None or key > cur[0]:
            newest[ds] = (key, cols)
    out = {ds: [] for ds in keyless}
    out.update({ds: cols for ds, (_k, cols) in newest.items()})
    return out


def _rebuilt_fingerprint(event: AuditEvent, cols: List[str]) -> Optional[str]:
    """The v7 fingerprint of *event*, from the key columns of its snapshot.

    Only an INSERT or a DELETE carries every attribute; an UPDATE carries
    the changed ones and cannot name its row.
    """
    if event.operation_type not in ("INSERT", "DELETE") or not cols:
        return None
    attrs = _payload(event).get("all_attributes")
    if not isinstance(attrs, dict):
        return None
    values = []
    for name in cols:
        value = attrs.get(name)
        if value is None or isinstance(value, (dict, list)):
            return None
        values.append(value)
    if len(cols) == 1:
        return f"pk:{cols[0]}={values[0]}"
    return "pkc:" + "|".join(
        f"{name}={json.dumps(value, ensure_ascii=False)}"
        for name, value in zip(cols, values))


def prove_legacy_keys(
    events: List[AuditEvent],
    key_columns: Optional[Callable[[str], Optional[Sequence[str]]]] = None,
    known: Optional[Dict[str, List[str]]] = None,
) -> List[AuditEvent]:
    """Re-key the rows whose journaled key does not name ONE feature.

    Before v7 a composite key ``(zone_id, parcel_no)`` was journaled as
    its first column only: ``pk:zone_id=12`` names every parcel of zone
    12. Such rows fell into one bucket, and the collapse cancelled the
    INSERT of one parcel against the DELETE of another: neither was
    compensated and the rewind reported success. The same holds when the
    journaled column is no longer the layer's key.

    The layer's key columns come from *key_columns(datasource)* (the live
    layer, when the caller has it; see ``identity.effective_identity``),
    else from *known*, else from the newest v7 row of the datasource in
    *events* (``journaled_key_columns``). A row is re-keyed when its
    identity carries ``pk_field`` and the key columns say otherwise:
      - an INSERT or DELETE gets the v7 fingerprint rebuilt from its own
        snapshot (``pkc:zone_id=12|parcel_no=7``): proven, and bucketed
        with the v7 rows of the same feature;
      - an UPDATE takes the rebuilt key of the INSERT or DELETE of the
        same record when they prove it (``_tie_legacy_updates``: same
        journaled key, same FID, same edit session, values that chain);
      - any other row is keyed on its own (``UNPROVEN_KEY_MARK``): it is
        never fused, paired or cancelled with another row, and a
        compensation left on such a key is refused by name
        (``_identity_refusals``). It used to reach the executor, which
        relocated it by its post-state onto ANOTHER parcel holding the
        same values and wrote there (review finding composite-orphan-update).
    Traces take the key of the event they reference when it is in
    *events*.

    Unknown key columns (no v7 row of the layer, no live layer): the
    journaled column may be the whole key or the first of several, and
    nothing here tells which. The rows keep their key with
    ``LEGACY_KEY_MARK``: a lifetime on it closes only when the content
    proves one feature, a bucket that names several records is refused
    (``_identity_refusals``). Keying them apart instead (by FID and
    session) splits one feature created, edited and deleted in three
    sessions into three buckets: a rewind with nothing to write then
    re-inserts it and fails on its edit (measured on a single-column
    key: "target feature absent", batch stopped).
    Returns a new list, input order kept.
    """
    if not events:
        return events
    cols_by_ds: Dict[str, List[str]] = dict(journaled_key_columns(events))
    for ds, cols in (known or {}).items():
        cols_by_ds.setdefault(ds, list(cols))
    if key_columns is not None:
        for ds in {e.datasource_fingerprint or "" for e in events}:
            try:
                live = key_columns(ds)
            except Exception as exc:  # noqa: BLE001 - resolver is advisory
                flog(f"rewind_dedup: key columns of {ds[:60]} unreadable "
                     f"({exc}); journaled key columns kept", "WARNING")
                live = None
            if live is not None:
                cols_by_ds[ds] = [str(c) for c in live]
    rekeyed: Dict[int, str] = {}
    unproven_traces: set = set()
    unknown = 0
    todo: List[Tuple[int, AuditEvent, List[str], str]] = []
    out: List[AuditEvent] = []
    for e in events:
        identity = _identity_dict(e)
        pk_field = identity.get("pk_field")
        if (not pk_field or identity.get("pk_value") is None
                or identity.get("pk_fields")):
            out.append(e)
            continue
        plain = f"pk:{pk_field}={identity.get('pk_value')}"
        if e.entity_fingerprint not in (None, "", plain, plain + LEGACY_KEY_MARK):
            out.append(e)  # already re-keyed (the window did it)
            continue
        cols = cols_by_ds.get(e.datasource_fingerprint or "")
        if cols is None:
            unknown += 1
            if (int(getattr(e, "event_schema_version", None) or 0)
                    >= EXACT_TRACE_SCHEMA and not _is_trace(e)):
                out.append(e)
            else:
                out.append(e._replace(entity_fingerprint=plain + LEGACY_KEY_MARK))
            continue
        if cols == [str(pk_field)]:
            out.append(e._replace(entity_fingerprint=e.entity_fingerprint and plain))
            continue
        if _is_trace(e):
            # Re-keyed below, from its source; on its own when the source
            # is not here (its key names no single feature either).
            unproven_traces.add(id(e))
            out.append(e._replace(entity_fingerprint=plain))
            continue
        todo.append((len(out), e, cols, plain))
        out.append(e)
    ties = _tie_legacy_updates([(e, cols) for _i, e, cols, _p in todo])
    for index, e, cols, plain in todo:
        identity = _identity_dict(e)
        fingerprint = (_rebuilt_fingerprint(e, cols)
                       or (ties.get(id(e)) if not is_plan_item(e) else None))
        if fingerprint is None:
            fingerprint = "".join((plain, UNPROVEN_KEY_MARK, str(e.event_id),
                                   "@", str(_position_eid(e))))
        flog(f"rewind_dedup: eid={e.event_id} op={e.operation_type} journaled "
             f"key {identity.get('pk_field')}={identity.get('pk_value')!r} "
             f"does not name one feature (key columns {cols}): keyed "
             f"{fingerprint}", "WARNING")
        if e.event_id is not None and not is_plan_item(e):
            rekeyed[e.event_id] = fingerprint
        out[index] = e._replace(entity_fingerprint=fingerprint)
    if unproven_traces:
        fixed = []
        for e in out:
            if id(e) in unproven_traces:
                key = rekeyed.get(e.restored_from_event_id)
                if key is None:
                    key = "".join((e.entity_fingerprint or "", UNPROVEN_KEY_MARK,
                                   str(e.event_id)))
                e = e._replace(entity_fingerprint=key)
            fixed.append(e)
        out = fixed
    if unknown:
        flog(f"rewind_dedup: {unknown} row(s) carry a key whose columns the "
             f"journal cannot check (no v7 row of their layer, no live layer "
             f"given): kept, read as a key that may name several features",
             "INFO")
    return out


def _tie_legacy_updates(rows: List[Tuple[AuditEvent, List[str]]]
                        ) -> Dict[int, str]:
    """{id(UPDATE row): rebuilt key} for the legacy UPDATE rows proven.

    *rows*: the legacy rows of a composite key being re-keyed, with the
    key columns. An UPDATE carries the changed fields only and cannot name
    its record by itself. It is tied to the rebuilt key of an INSERT or a
    DELETE of the SAME record: same journaled key, same FID (a FID names
    one record for as long as the layer is loaded, and one edit session
    happens on one loaded layer), same edit session, and values that chain
    from one to the other:
      - forward: the INSERT's snapshot holds every OLD value of the
        UPDATE (the UPDATEs between them applied);
      - backward: the DELETE's snapshot holds every NEW value of the
        UPDATE (the UPDATEs between them unwound).
    A row tied both ways to two different keys (its record's key changed)
    stays unproven. Anything else stays unproven, and is refused by name
    if it still has to be compensated.
    """
    groups: Dict[tuple, List[AuditEvent]] = {}
    cols_of: Dict[int, List[str]] = {}
    for e, cols in rows:
        if _is_trace(e) or is_plan_item(e) or not e.session_id:
            continue
        identity = _identity_dict(e)
        if identity.get("fid") is None:
            continue
        group = (e.datasource_fingerprint or "", str(identity.get("pk_field")),
                 json.dumps(identity.get("pk_value"), sort_keys=True, default=str),
                 str(identity.get("fid")), e.session_id)
        groups.setdefault(group, []).append(e)
        cols_of[id(e)] = cols
    forward: Dict[int, str] = {}
    backward: Dict[int, str] = {}
    for members in groups.values():
        members.sort(key=_order_key)
        key, state = None, None
        for e in members:
            if e.operation_type == "INSERT":
                key = _rebuilt_fingerprint(e, cols_of[id(e)])
                snap = _full_state(e)
                state = dict(snap[0]) if key and snap is not None else None
            elif e.operation_type == "UPDATE":
                sides = _attribute_sides(e)
                if (state is None or sides is None or not sides[0]
                        or any(f not in state or state[f] != v
                               for f, v in sides[0].items())):
                    key, state = None, None
                    continue
                forward[id(e)] = key
                state.update(sides[1])
            else:
                key, state = None, None
        key, state = None, None
        for e in reversed(members):
            if e.operation_type == "DELETE":
                key = _rebuilt_fingerprint(e, cols_of[id(e)])
                snap = _full_state(e)
                state = dict(snap[0]) if key and snap is not None else None
            elif e.operation_type == "UPDATE":
                sides = _attribute_sides(e)
                if (state is None or sides is None or not sides[1]
                        or any(f not in state or state[f] != v
                               for f, v in sides[1].items())):
                    key, state = None, None
                    continue
                backward[id(e)] = key
                state.update(sides[0])
            else:
                key, state = None, None
    ties: Dict[int, str] = {}
    for rid in set(forward) | set(backward):
        keys = {k for k in (forward.get(rid), backward.get(rid)) if k}
        if len(keys) == 1:
            ties[rid] = keys.pop()
    return ties


def _attribute_sides(row: AuditEvent) -> Optional[Tuple[dict, dict]]:
    """(OLD values, NEW values) of an UPDATE's changed attributes, or None."""
    changed = _payload(row).get("changed_only")
    if not isinstance(changed, dict):
        return None
    return ({f: extract_delta_old(v) for f, v in changed.items()},
            {f: extract_delta_new(v) for f, v in changed.items()})


# ---------------------------------------------------------------------------
# Collapse
# ---------------------------------------------------------------------------

def collapse_rewind_events(events: List[AuditEvent]) -> List[AuditEvent]:
    """Collapse the rewind window to the ordered list of compensations.

    Args:
        events: the rewind window (user events after the cutoff, the
            traces that reference them, and the plan items the window
            synthesised for the history before the cutoff).

    Returns:
        The rows to compensate, newest first. Trace events are filtered
        out, and every user event an in-effect trace compensates is
        removed, so the same rewind can be replayed without accumulating
        duplicate features.
    """
    active, _stats = collapse_rewind_events_with_stats(events)
    return active


def _empty_stats() -> Dict[str, int]:
    return {
        "raw": 0, "user": 0,
        "traces": 0, "traces_invalidated": 0, "traces_active": 0,
        "dedup_active": 0, "dedup_dropped": 0, "dedup_redundant": 0,
        "plan_reapply": 0, "plan_recompensate": 0, "plan_refused": 0,
        "paired_reinserted": 0,
    }


def _written_before(event: AuditEvent, trace: AuditEvent) -> bool:
    """True when *event* was in the journal when *trace* was written.

    event_id is AUTOINCREMENT: a row with a larger id was inserted after
    the trace, so the rewind that wrote the trace never saw it, whatever
    its created_at. That is the case of an edit held back in the pending
    file and re-imported at the next start: it keeps its original date,
    which can fall inside the span of a legacy trace, but it was never
    compensated and must stay active.
    """
    if event.event_id is None or trace.event_id is None:
        return True
    return event.event_id < trace.event_id


def _count_unreferenced_between(
    source: AuditEvent, trace: AuditEvent,
    by_key: Dict[str, List[AuditEvent]], referenced: set,
) -> int:
    lo = _order_key(source)
    hi = _order_key(trace)
    return sum(
        1 for e in by_key.get(_entity_key(source), ())
        if e.event_id != source.event_id
        and e.event_id not in referenced
        and _written_before(e, trace)
        and lo <= _order_key(e) < hi
    )


def collapse_rewind_events_with_stats(
    events: List[AuditEvent],
    key_columns: Optional[Callable[[str], Optional[Sequence[str]]]] = None,
) -> Tuple[List[AuditEvent], Dict[str, int]]:
    """Same as collapse_rewind_events but also returns canonical stats
    suitable for ``log_cycle_summary`` consumption.

    *key_columns(datasource)*: key columns of the live layer, optional
    (``prove_legacy_keys``).

    Stats keys:
        raw, user, traces, traces_invalidated, traces_active,
        dedup_active, dedup_dropped, dedup_redundant,
        plan_reapply, plan_recompensate, plan_refused, paired_reinserted
    """
    if not events:
        return [], _empty_stats()

    # 0. Rows whose journaled key names several features.
    events = prove_legacy_keys(list(events), key_columns)
    plan_items = [e for e in events if is_plan_item(e)]
    rows = [e for e in events if not is_plan_item(e)]
    traces = [e for e in rows if _is_trace(e)]
    user_events = [e for e in rows if not _is_trace(e)]

    # 1-2. Traces in effect NOW, and the user events they compensate.
    status = traces_in_effect(traces)
    active, dropped, extra_rows, per_write = _neutralise(
        user_events, traces, status, rows, plan_items)
    invalidated_count = sum(1 for t in traces if _is_invalidated(t))
    in_effect_count = sum(1 for v in status.values() if v)
    flog(f"rewind_dedup: {len(events)} raw "
         f"({len(user_events)} user, {len(traces)} traces, "
         f"{invalidated_count} invalidated, {in_effect_count} in effect, "
         f"{len(plan_items) + len(extra_rows)} plan items) -> "
         f"{len(active)} active user events "
         f"({len(dropped)} neutralised)")

    # 3. FID recycling, detected on the events that still describe the
    #    live data (a DELETE a compensation reverted closed nothing), and
    #    on the writes of the traces (a re-insert is not a recycle).
    fp_split, fid_recycle_splits = _detect_fid_recycle(
        active, list(traces) + [p for p in plan_items + extra_rows
                                if plan_item_kind(p) in (PLAN_MARK_REAPPLY,
                                                         PLAN_MARK_RECOMPENSATE)])
    for fp, first_eid, second_eid in fid_recycle_splits:
        flog(f"rewind_dedup: fid_recycle_detected fp={fp} splits=2 "
             f"first_eid={first_eid} second_eid={second_eid}")
    if fp_split:
        # The rows of each write of a user event (`per_write`) follow the
        # split of that event's own rows.
        active = _apply_fid_recycle_rewrite(active, fp_split)
        extra_rows = _apply_fid_recycle_rewrite(extra_rows, fp_split)
    plan_items = plan_items + extra_rows

    # 3b. A user event whose compensation a later rewind cancelled (forward
    #     hop) is in the data since THAT rewind, not since its own date: it
    #     is unwound at the place of the re-application.
    active = _place_reapplied_events(active, traces, status, keep=per_write)

    # 3c. A compensation whose target the journal cannot name never reaches
    #     the executor: it is refused by name.
    active, plan_items = _identity_refusals(active, plan_items)

    # 4. Bare FIDs (a shapefile renumbers its records after a delete): the
    #    rows of one key that cannot be proven to name one feature are
    #    refused by name; a plan delete of a feature a trace re-inserted (or
    #    the user created) and that feature's chain under the FID it carries
    #    since cancel when their states prove it; a creation and a deletion
    #    under two FIDs that the states can neither pair nor tell apart are
    #    refused.
    plan_items = plan_items + _weak_lifeline_refusals(
        active, traces, plan_items, user_events)
    active, plan_items, paired = _pair_reinserted_lifetimes(active, plan_items)
    plan_items = plan_items + _unprovable_lifetime_refusals(
        active, plan_items, user_events, traces)

    # 5. Plan items join the user events; per-entity collapse of the whole
    #    plan, then one global chronological order, newest first.
    kinds = [plan_item_kind(e) for e in plan_items]
    _log_plan_items(plan_items)
    result, redundant = _collapse_user_chain_with_stats(
        list(active) + list(plan_items))
    result.sort(key=_order_key, reverse=True)
    _log_plan_items([e for e in result if plan_item_kind(e) == PLAN_MARK_REFUSED
                     and not any(e is p for p in plan_items)])
    # The key-columns mark is this collapse's own reading of the key;
    # callers match the rows of the plan on the journaled key
    # (recover_dialog._session_undo_plan filters them by it).
    result = [e._replace(entity_fingerprint=e.entity_fingerprint.replace(
                  LEGACY_KEY_MARK, ""))
              if LEGACY_KEY_MARK in (e.entity_fingerprint or "") else e
              for e in result]

    stats = {
        "raw": len(events),
        "user": len(user_events),
        "traces": len(traces),
        "traces_invalidated": invalidated_count,
        "traces_active": in_effect_count,
        "dedup_dropped": len(dropped),
        "dedup_redundant": redundant,
        "dedup_active": len(result),
        "plan_reapply": kinds.count(PLAN_MARK_REAPPLY),
        "plan_recompensate": kinds.count(PLAN_MARK_RECOMPENSATE),
        "plan_refused": sum(1 for e in result
                            if plan_item_kind(e) == PLAN_MARK_REFUSED),
        "paired_reinserted": paired,
    }
    return result, stats


def _full_state(row: AuditEvent) -> Optional[Tuple[dict, Optional[bytes]]]:
    """(all attributes, geometry) a DELETE or INSERT row carries, or None."""
    attrs = _payload(row).get("all_attributes")
    if not isinstance(attrs, dict):
        return None
    geom = row.geometry_wkb
    return dict(attrs), (bytes(geom) if geom is not None else None)


def _state_before_chain(chain_desc: List[AuditEvent]
                        ) -> Optional[Tuple[dict, Optional[bytes]]]:
    """State of a feature before the OLDEST row of its chain, or None.

    The chain must end (newest row) on its only DELETE, which carries the
    whole feature; every older row is an UPDATE whose OLD side is applied
    going back. Any other shape has no complete state to compare.
    """
    if not chain_desc or chain_desc[0].operation_type != "DELETE":
        return None
    if any(r.operation_type != "UPDATE" for r in chain_desc[1:]):
        return None
    state = _full_state(chain_desc[0])
    if state is None:
        return None
    attrs, geom = state
    for row in chain_desc[1:]:
        changed = _payload(row).get("changed_only")
        if not isinstance(changed, dict):
            return None
        for field, delta in changed.items():
            attrs[field] = extract_delta_old(delta)
        if row.geometry_wkb is not None:
            old = bytes(row.geometry_wkb)
            geom = None if old == GEOMETRY_NULL_MARKER else old
    return attrs, geom


def _unwound(attrs: dict, geom: Optional[bytes], rows: List[AuditEvent]
             ) -> Optional[Tuple[dict, Optional[bytes]]]:
    """(attrs, geom) with the OLD sides of UPDATE *rows* (oldest first)
    written back, newest first; None when a row carries no sides."""
    attrs = dict(attrs)
    for row in reversed(rows):
        sides = _update_sides(row)
        if sides is None:
            return None
        for field, value in sides[1].items():
            if field is None:
                geom = value
            else:
                attrs[field] = value
    return attrs, geom


def _pair_reinserted_lifetimes(
    active: List[AuditEvent], plan_items: List[AuditEvent],
) -> Tuple[List[AuditEvent], List[AuditEvent], int]:
    """Cancel a plan delete of a re-inserted feature against that feature's
    chain under its new FID.

    R: a plan row whose compensation DELETES a feature a trace re-inserted
    (REAPPLY of a DELETE, RECOMPENSATE of an INSERT: row operation INSERT).
    It carries the identity of the event the trace restored. On a provider
    that allocates FIDs, the re-inserted feature received a NEW one and the
    user's later edits carry it: a chain C under another key of the same
    datasource. When the user deleted it again, the plan re-inserts it
    (compensation of C's DELETE) and then deletes it (R), two buckets that
    never cancel; the executor refuses to delete what it has just
    re-inserted and the feature comes back, reported done (review finding
    forward-hop-reinsert, shapefile).

    C is R's feature when: all of C's rows are after R's toggle (the
    feature did not exist under that key before the re-insertion, as far
    as the window knows), C holds no user INSERT, C ends on its only
    DELETE, and the state of the feature before C's oldest row (DELETE
    snapshot, UPDATE OLD sides applied going back) equals R's snapshot,
    attributes AND geometry. Then R and C cancel as one lifetime: the plan
    would re-insert the feature, unwind C's edits, and delete it again.

    The same holds for a feature the USER created after T (an INSERT row)
    whose later rows carry another FID: a shapefile repack renumbers the
    records after every delete, so the INSERT and the DELETE of one
    feature landed in two buckets and the feature came back, reported
    done (rw_state_at_t_toggles, shapefile walk). The state compared is
    then the creator's state just before C starts: its snapshot with the
    UPDATE rows of its own bucket made in between applied forward (the
    state each of them proves); those rows go with the pair. Or, read the
    other way with the values the provider stored only, C's start state
    with those UPDATE rows unwound (their OLD sides) must be the creator's
    snapshot: the NEW side of an UPDATE is journaled from the edit buffer,
    before the provider rounds a real to its field's precision or cuts a
    long string, so the forward reading alone missed the pair (review
    finding shp-split-precision).

    Assignment: chains in the order they start, each to the LATEST
    matching creation before it that no earlier chain took (a feature
    deleted, restored, deleted, restored again has one re-insertion per
    lifetime). Pairing writes nothing: it removes a re-insert and a delete
    of the same content. When several features share that content, which
    one is paired changes no attribute, no geometry and no count of the
    result. A chain whose content differs stays as it is here; on a bare
    FID, `_unprovable_lifetime_refusals` then refuses the pair when nothing
    tells the two features apart.
    """
    creators = [p for p in plan_items
                if plan_item_kind(p) in (PLAN_MARK_REAPPLY, PLAN_MARK_RECOMPENSATE)
                and p.operation_type == "INSERT"]
    creators += [e for e in active if e.operation_type == "INSERT"]
    if not creators or not active:
        return active, plan_items, 0
    chains: Dict[str, List[AuditEvent]] = {}
    for e in active:
        chains.setdefault(_entity_key(e), []).append(e)
    by_key: Dict[str, List[AuditEvent]] = {}
    for e in list(active) + list(plan_items):
        by_key.setdefault(_entity_key(e), []).append(e)
    for rows in by_key.values():
        rows.sort(key=_order_key)
    # A user creation whose own bucket holds its end (a later DELETE) is a
    # lifetime of that bucket, cancelled there: only a creation with no
    # end under its own key is looked for under another one.
    creators = [r for r in creators if is_plan_item(r) or not any(
        e.operation_type == "DELETE" and _order_key(e) > _order_key(r)
        for e in by_key.get(_entity_key(r), ()))]
    candidates = []
    for key, chain in chains.items():
        for segment in _deleted_segments(sorted(chain, key=_order_key, reverse=True)):
            state = _state_before_chain(segment)
            if state is None:
                continue
            candidates.append((_order_key(segment[-1]), key, state[0], state[1],
                               segment[0].datasource_fingerprint or "", segment))
    if not candidates:
        return active, plan_items, 0

    by_ds_rows: Dict[str, List[AuditEvent]] = {}
    for e in list(active) + list(plan_items):
        by_ds_rows.setdefault(e.datasource_fingerprint or "", []).append(e)
    for rows in by_ds_rows.values():
        rows.sort(key=_order_key)

    def state_at(creator, r_key, r_pos, until):
        """(state, rows) of the creator's feature just before *until*."""
        snap = _full_state(creator)
        if snap is None:
            return None, []
        attrs, geom = snap
        between = []
        for row in by_key.get(r_key, ()):
            pos = _order_key(row)
            if row is creator or not r_pos < pos < until:
                continue
            if row.operation_type != "UPDATE":
                return None, []  # another lifetime under this key
            sides = _update_sides(row)
            if sides is None:
                return None, []
            for field, value in sides[0].items():
                if field is None:
                    geom = value
                else:
                    attrs[field] = value
            between.append(row)
        return (attrs, geom), between

    def shifted_state_at(creator, r_key, r_pos, until, ds):
        """Follow a shapefile record across session-end delete renumberings."""
        cur_fid = _fid_number(r_key)
        snap = _full_state(creator)
        if cur_fid is None or snap is None or is_plan_item(creator):
            return None, []
        attrs, geom = snap
        between = []
        ds_rows = [row for row in by_ds_rows.get(ds, ())
                   if row is not creator and id(row) not in drop
                   and r_pos < _order_key(row) <= until]
        idx = 0
        while idx < len(ds_rows):
            sess = ds_rows[idx].session_id
            end = idx + 1
            while (end < len(ds_rows) and sess
                   and ds_rows[end].session_id == sess):
                end += 1
            del_fids = []
            for row in ds_rows[idx:end]:
                pos = _order_key(row)
                rfid = _fid_number(_entity_key(row))
                if (row.operation_type == "DELETE" and not is_plan_item(row)
                        and rfid is not None):
                    del_fids.append(rfid)
                if pos >= until or rfid != cur_fid:
                    continue
                if row.operation_type != "UPDATE":
                    return None, []
                sides = _update_sides(row)
                if sides is None:
                    return None, []
                for field, value in sides[0].items():
                    if field is None:
                        geom = value
                    else:
                        attrs[field] = value
                between.append(row)
            if sess and del_fids and _order_key(ds_rows[end - 1]) < until:
                if cur_fid in del_fids:
                    return None, []
                cur_fid -= sum(1 for df in del_fids if df < cur_fid)
            idx = end
        return (attrs, geom), between

    drop: set = set()
    paired = 0
    for oldest, key, attrs, geom, ds, segment in sorted(candidates,
                                                        key=lambda c: c[0]):
        best = None
        for r in creators:
            r_key, r_pos = _entity_key(r), _order_key(r)
            if (id(r) in drop or r_key == key
                    or ds != (r.datasource_fingerprint or "")
                    or not r_pos < oldest):
                continue
            state, between = state_at(r, r_key, r_pos, oldest)
            if state is None or (state != (attrs, geom)
                                 and _unwound(attrs, geom, between)
                                 != _full_state(r)):
                state, between = shifted_state_at(r, r_key, r_pos, oldest, ds)
            if state is None or (state != (attrs, geom)
                                 and _unwound(attrs, geom, between)
                                 != _full_state(r)):
                continue
            if best is None or r_pos > best[1]:
                best = (r, r_pos, between)
        if best is None:
            continue
        r, _pos, between = best
        drop.update(id(e) for e in segment)
        drop.update(id(e) for e in between)
        drop.add(id(r))
        paired += 1
        flog(f"rewind_dedup: the delete of eid={r.event_id} (created at "
             f"{r.created_at}, {(r.entity_fingerprint or '')[:40]}) and the "
             f"user rows {[e.event_id for e in segment]} of {key[-40:]} are "
             f"one lifetime (same state, rows after the creation"
             f"{', with its updates ' + str([e.event_id for e in between]) if between else ''}): "
             f"cancelled")
    if not paired:
        return active, plan_items, 0
    active = [e for e in active if id(e) not in drop]
    plan_items = [p for p in plan_items if id(p) not in drop]
    return active, plan_items, paired


def _weak_key(event: AuditEvent) -> bool:
    """True when the event's key names a RECORD, not a proven feature.

    A bare FID (``fid:``, a layer without a primary key): a shapefile
    renumbers its records after a delete. A legacy key whose columns are
    unknown (``LEGACY_KEY_MARK``): it may be the first column of a key of
    several. Rows of different features can share such a key, so nothing
    is paired or cancelled on the key alone.
    """
    key = compute_entity_key(event.entity_fingerprint, event.feature_identity_json)
    return key.startswith("fid:") or LEGACY_KEY_MARK in key


def _same_edit_session(a: AuditEvent, b: AuditEvent) -> bool:
    """True when two user rows were committed in ONE edit session.

    A FID names one record for a whole edit session: a shapefile repacks
    its records when editing stops, not at each commit (measured: after
    a commit that keeps editing on, the FIDs of the records a delete did
    not remove are unchanged), and a layer's key-to-FID map lives as long
    as the layer is loaded. The edit tracker gives each editingStarted a
    new session id. Plan rows and traces carry no session of their own.
    """
    if is_plan_item(a) or is_plan_item(b) or _is_trace(a) or _is_trace(b):
        return False
    return bool(a.session_id) and a.session_id == b.session_id


def _describe(event: AuditEvent) -> str:
    """How a refusal names a row: the journal event, or the history item."""
    kind = plan_item_kind(event)
    if kind is not None:
        return (f"the {kind[len(PLAN_MARK_PREFIX):]} of trace {event.event_id} "
                f"({event.created_at})")
    return f"event {event.event_id} ({event.operation_type} of {event.created_at})"


def _identity_refusals(active: List[AuditEvent], plan_items: List[AuditEvent]
                       ) -> Tuple[List[AuditEvent], List[AuditEvent]]:
    """Refuse by name the compensations whose target no key names.

      - a row keyed on its own (``UNPROVEN_KEY_MARK``, ``prove_legacy_keys``):
        a legacy UPDATE, or a history item made of one, that no INSERT or
        DELETE of its record ties to a key. It used to reach the executor,
        which withdrew the unproven key, relocated the target by its NEW
        values onto ANOTHER parcel holding them and wrote the old values
        there (review finding composite-orphan-update);
      - a legacy key whose columns are unknown (``LEGACY_KEY_MARK``) under
        which the plan holds rows of several records (several FIDs): the
        journaled column is then not the layer's whole key, and every
        per-key rule, the executor's remap included, would take one
        feature for another.

    Only rows that still have to be compensated reach here: an in-effect
    trace neutralised the others. Returns (active, plan items); the
    refusal of an unproven row replaces it.
    """
    refusals: List[AuditEvent] = []
    kept_active: List[AuditEvent] = []
    kept_items: List[AuditEvent] = []
    for e in active:
        if UNPROVEN_KEY_MARK in (e.entity_fingerprint or ""):
            refusals.append(_unproven_key_refusal(e))
        else:
            kept_active.append(e)
    for e in plan_items:
        if (UNPROVEN_KEY_MARK in (e.entity_fingerprint or "")
                and plan_item_kind(e) != PLAN_MARK_REFUSED):
            refusals.append(_unproven_key_refusal(e))
        else:
            kept_items.append(e)
    records: Dict[str, Dict[str, List[AuditEvent]]] = {}
    for e in kept_active + kept_items:
        key = _entity_key(e)
        if LEGACY_KEY_MARK in key and plan_item_kind(e) != PLAN_MARK_REFUSED:
            fid = _identity_dict(e).get("fid")
            records.setdefault(key, {}).setdefault(str(fid), []).append(e)
    for key, by_fid in records.items():
        if len(by_fid) < 2:
            continue
        rows = sorted((e for group in by_fid.values() for e in group),
                      key=_order_key)
        newest = rows[-1]
        identity = _identity_dict(newest)
        refusals.append(build_refusal_item(
            newest, (newest.created_at, _position_eid(newest)),
            f"the key columns of this layer are unknown to the journal (no "
            f"edit of it captured by this version, no layer given), and "
            f"events {[e.event_id for e in rows][:8]} name the records "
            f"{sorted(by_fid)[:8]} under one journaled key "
            f"{identity.get('pk_field')}={identity.get('pk_value')!r}: that "
            f"column may be the first of a key of several columns, and the "
            f"rewind cannot tell which feature each event changed; choose a "
            f"date at or after {newest.created_at}"))
    for r in refusals:
        flog(f"rewind_dedup: identity unproven, refused eid={r.event_id} "
             f"{(r.entity_fingerprint or '')[:60]}", "ERROR")
    return kept_active, kept_items + refusals


def _unproven_key_refusal(event: AuditEvent) -> AuditEvent:
    identity = _identity_dict(event)
    return build_refusal_item(
        event, (event.created_at, _position_eid(event)),
        f"{_describe(event)} was journaled by an older version of "
        f"RecoverLand with the first column of the layer's key only "
        f"({identity.get('pk_field')}={identity.get('pk_value')!r}, record "
        f"{identity.get('fid')}): no creation or deletion of the same record "
        f"in the same edit session ties it to one feature, and writing it "
        f"back could change another feature holding the same values; choose "
        f"a date at or after {event.created_at}")


def _fid_number(key: str) -> Optional[int]:
    """N of a ``fid:N`` key (``fid:N@<split>`` included), None otherwise."""
    tail = key.rsplit("::", 1)[-1]
    if not tail.startswith("fid:"):
        return None
    digits = tail[4:].split("@", 1)[0].split("#", 1)[0]
    try:
        return int(digits)
    except ValueError:
        return None


def _provider_values(row: AuditEvent) -> Dict[Optional[str], object]:
    """The values of *row* the provider itself returned, geometry as None.

    A DELETE's snapshot and an UPDATE's OLD side are read from the
    provider before the commit, an INSERT's snapshot after it. An
    UPDATE's NEW side is NOT: it is the edit buffer's value, before the
    provider rounds a real to its field's precision or cuts a string to
    its width.
    """
    if row.operation_type in ("INSERT", "DELETE"):
        snap = _full_state(row)
        if snap is None:
            return {}
        out: Dict[Optional[str], object] = dict(snap[0])
        if snap[1] is not None:
            out[None] = snap[1]
        return out
    sides = _update_sides(row)
    return dict(sides[1]) if sides is not None else {}


def _weak_lifeline_refusals(active: List[AuditEvent], traces: List[AuditEvent],
                            plan_items: List[AuditEvent],
                            user_events: List[AuditEvent]) -> List[AuditEvent]:
    """Refuse a bare FID whose rows cannot be proven to name one feature.

    A shapefile renumbers its records when an edit session that deleted
    one ends: ``fid:3`` is C003 for an UPDATE and, after C001 was
    deleted, the feature that followed C003 for a later DELETE. In one
    bucket the plan re-inserts that feature (compensation of the DELETE)
    and the executor, following the key to it, writes C003's old values
    on it (it trusts its remap for an UPDATE without a NEW geometry). The
    same holds for a history item: the re-application of an UPDATE made
    before T carries the record number of that time (rw_state_at_t_toggles,
    shapefile walk seed 109: C003's value written on C000).

    This used to give the DELETE its own key whenever its snapshot
    contradicted the NEW values of the key's earlier UPDATE rows. A NEW
    side is journaled from the edit buffer, before the provider rounds a
    real to its field's precision (1234.56789 stored 1234.57) or cuts a
    string at 254 characters: one feature updated then deleted read as
    two, its lifetime no longer cancelled, and the rewind re-inserted it
    and reported the rest "target absent" (review finding
    shp-split-precision). A re-key also sends the plan down the
    executor's resurrection path whenever it is wrong. Now nothing is
    re-keyed: a bucket whose rows cannot be tied to one feature is
    refused by name, and only when the plan would link them (a re-insert
    of the key).

    On a ``fid:`` key, two rows of different edit sessions (within one
    session a FID never changes) are two features when:
      - an UPDATE row and a re-inserted row (user rows or history items)
        lie on either side of a certain renumbering: a user DELETE of
        this record or a lower one, committed in a session that ended in
        between (a user row's record number is that of its own time, a
        history item's that of its source, older than its place);
      - or, walking the key's user rows in order (a write of the key by
        a trace or a plan row in between restarts the walk), the values
        the provider returned disagree (an INSERT's snapshot, an UPDATE's
        OLD side, a DELETE's snapshot): the record was renumbered, or the
        feature changed outside RecoverLand; the journal cannot tell;
      - or a NEW side disagrees with what the provider later returned,
        and a delete that may have renumbered the record lies between
        them (a rewind's delete: its FID is not journaled). Without such
        a delete a NEW side that differs is the provider's rounding.
    """
    by_key: Dict[str, List[AuditEvent]] = {}
    for e in list(active) + list(plan_items):
        if (not _is_trace(e) and plan_item_kind(e) != PLAN_MARK_REFUSED
                and compute_entity_key(e.entity_fingerprint,
                                       e.feature_identity_json).startswith("fid:")):
            by_key.setdefault(_entity_key(e), []).append(e)
    if not by_key:
        return []
    write_pos: Dict[str, List[tuple]] = {}
    for w in list(traces) + list(plan_items) + [e for e in active if is_plan_item(e)]:
        write_pos.setdefault(_entity_key(w), []).append(_order_key(w))
    # Deletes that may renumber the records: (position, session, FID),
    # sorted by position per layer, and per edit session. A trace or a
    # plan row does not journal the FID it deleted.
    deletes: Dict[str, List[tuple]] = {}
    by_session: Dict[str, List[tuple]] = {}
    for e in user_events:
        if e.operation_type == "DELETE":
            fid = _identity_dict(e).get("fid")
            entry = (_order_key(e), e.session_id, fid if isinstance(fid, int) else None)
            deletes.setdefault(e.datasource_fingerprint or "", []).append(entry)
            if e.session_id:
                by_session.setdefault(e.session_id, []).append(entry)
    for e in list(traces) + list(plan_items):
        if e.operation_type == "DELETE":
            deletes.setdefault(e.datasource_fingerprint or "", []).append(
                (_order_key(e), None, None))
    for entries in deletes.values():
        entries.sort(key=lambda d: d[0])
    positions = {ds: [d[0] for d in entries] for ds, entries in deletes.items()}

    def session_of(row):
        return None if is_plan_item(row) or _is_trace(row) else row.session_id

    def renumbered(a, b, n, certain):
        lo, hi = sorted((a, b), key=_order_key)
        lo_pos, hi_pos = _order_key(lo), _order_key(hi)
        lo_s, hi_s = session_of(lo), session_of(hi)
        ds = hi.datasource_fingerprint or ""
        entries = deletes.get(ds, [])
        # Committed at or after the earlier row, or in its edit session
        # (the records are renumbered when that session ends).
        window = entries[bisect_left(positions.get(ds, []), lo_pos):
                         bisect_left(positions.get(ds, []), hi_pos)]
        if lo_s:
            window = window + [d for d in by_session.get(lo_s, ())
                               if d[0] < lo_pos]
        for _pos, session, fid in window:
            if session and session == hi_s:
                continue
            if fid is not None and n is not None:
                if fid <= n and (session or not certain):
                    return True
            elif not certain:
                return True
        return False

    def refusal(q, row, n, why):
        flog(f"rewind_dedup: {_entity_key(row)[-40:]} rows eid={q.event_id} "
             f"and eid={row.event_id} not proven to be one feature: refused "
             f"by name", "ERROR")
        first = min((q, row), key=_order_key)
        return build_refusal_item(
            row, (row.created_at, _position_eid(row)),
            f"{_describe(q)} and {_describe(row)} are journaled on record {n} "
            f"of a layer without a primary key, {why}, and the rewind would "
            f"re-insert one and write the other's values onto it; choose a "
            f"date at or after {first.created_at}")

    refusals: List[AuditEvent] = []
    for key, rows in by_key.items():
        rows.sort(key=_order_key)
        n = _fid_number(key)
        reinserts = [e for e in rows if e.operation_type == "DELETE"]
        if not reinserts:
            continue
        # A re-insert and an UPDATE row certainly on two features. A history
        # item carries the record number of its source event, which is
        # older than its place: it is only read as the EARLIER row (its
        # number was captured before the renumbering found after it).
        found = None
        for d in reinserts:
            for u in rows:
                later = max((u, d), key=_order_key)
                if (u.operation_type == "UPDATE" and not is_plan_item(later)
                        and not _same_edit_session(u, d)
                        and renumbered(u, d, n, certain=True)):
                    found = (u, d)
                    break
            if found:
                break
        if found:
            u, d = found
            refusals.append(refusal(
                u, d, n, "but this record or a lower one was deleted between "
                "them, and a shapefile renumbers its records when such an edit "
                "session ends: they changed two features"))
            continue
        # The values the provider returned, walked over the user rows.
        newest_reinsert = _order_key(reinserts[-1])
        prov: Dict[Optional[str], tuple] = {}
        buf: Dict[Optional[str], tuple] = {}
        prev = None
        for row in rows:
            if is_plan_item(row):
                continue
            pos = _order_key(row)
            if prev is not None and any(_order_key(prev) < w < pos
                                        for w in write_pos.get(key, ())):
                prov, buf, prev = {}, {}, None
            conflict = None
            if row.operation_type != "INSERT":
                for field, value in _provider_values(row).items():
                    name = "geometry" if field is None else field
                    if field in prov and prov[field][0] != value:
                        q = prov[field][1]
                        if not _same_edit_session(q, row):
                            conflict = (q, name, prov[field][0], value)
                            break
                    if field in buf and buf[field][0] != value:
                        q = buf[field][1]
                        if (not _same_edit_session(q, row)
                                and renumbered(q, row, n, certain=False)):
                            conflict = (q, name, buf[field][0], value)
                            break
            if conflict is not None and newest_reinsert >= pos:
                q, name, before, after = conflict
                refusals.append(refusal(
                    q, row, n,
                    f"but disagree on its {name} ({str(before)[:40]!r} then "
                    f"{str(after)[:40]!r}): a shapefile renumbers its records "
                    f"after a delete, and a feature changed outside "
                    f"RecoverLand reads the same way; the journal cannot "
                    f"prove both changed one feature"))
                break
            if conflict is not None:
                flog(f"rewind_dedup: {key[-40:]} rows eid={conflict[0].event_id} "
                     f"and eid={row.event_id} disagree on {conflict[1]}; no "
                     f"re-insert of the key follows, each is proven on its own "
                     f"by the executor")
            if row.operation_type == "INSERT":
                prov = {f: (v, row) for f, v in _provider_values(row).items()}
                buf = {}
            elif row.operation_type == "UPDATE":
                sides = _update_sides(row)
                if sides is None:
                    prov, buf = {}, {}
                else:
                    for field in sides[1]:
                        prov.pop(field, None)
                    buf.update({f: (v, row) for f, v in sides[0].items()})
            else:
                prov, buf = {}, {}
            prev = row
    return refusals


def _unprovable_lifetime_refusals(active: List[AuditEvent],
                                  plan_items: List[AuditEvent],
                                  user_events: List[AuditEvent],
                                  traces: List[AuditEvent]) -> List[AuditEvent]:
    """Refuse a creation and a deletion under two FIDs that may be one feature.

    What `_pair_reinserted_lifetimes` left: a plan row that deletes a
    feature (a trace's re-insert undone, or a user INSERT whose own key
    shows no end) and, under another bare FID of the layer, a chain that
    ends on a user DELETE after it. When they are one feature whose
    lifetime spans more FIDs than the pairing stitches (two repacks, an
    edit under a third FID), the plan re-inserts it (the DELETE's
    compensation) and then cannot delete it: the executor spares a
    feature it re-inserted in the same batch, and the feature came back
    with the rewind reported done (review of A1, shapefile walk seed
    113).

    They are two features when a value they differ on was changed by no
    UPDATE of the layer between the creation and the chain (a field the
    user never edits, the code of the feature, its geometry). Otherwise
    the journal can neither pair nor separate them: refused by name,
    never both halves planned.
    """
    creators = [p for p in plan_items
                if plan_item_kind(p) in (PLAN_MARK_REAPPLY, PLAN_MARK_RECOMPENSATE)
                and p.operation_type == "INSERT" and _weak_key(p)]
    user_creators = [e for e in active if e.operation_type == "INSERT"
                     and not is_plan_item(e) and _weak_key(e)]
    if not creators and not user_creators:
        return []
    by_key: Dict[str, List[AuditEvent]] = {}
    for e in list(active) + list(plan_items):
        by_key.setdefault(_entity_key(e), []).append(e)
    creators += [r for r in user_creators if not any(
        e.operation_type == "DELETE" and _order_key(e) > _order_key(r)
        for e in by_key.get(_entity_key(r), ()))]
    chains: Dict[str, List[AuditEvent]] = {}
    for e in active:
        if not is_plan_item(e) and _weak_key(e):
            chains.setdefault(_entity_key(e), []).append(e)
    # Fields each UPDATE of the window changed, with its place; None: a
    # trace whose source is not here, which may have changed anything.
    rows_by_id = {e.event_id: e for e in list(user_events) + list(traces)
                  if e.event_id is not None}
    items_by_id = {p.event_id: p for p in plan_items
                   if plan_item_kind(p) in (PLAN_MARK_REAPPLY, PLAN_MARK_RECOMPENSATE)}
    edits: Dict[str, List[tuple]] = {}
    for e in list(user_events) + list(plan_items) + list(traces):
        if e.operation_type != "UPDATE":
            continue
        row, seen = items_by_id.get(e.event_id, e) if _is_trace(e) else e, set()
        while row is not None and _is_trace(row) and row.event_id not in seen:
            seen.add(row.event_id)
            row = rows_by_id.get(row.restored_from_event_id)
        sides = _update_sides(row) if row is not None and not _is_trace(row) else None
        fields = set(sides[1]) if sides is not None else None
        edits.setdefault(e.datasource_fingerprint or "", []).append(
            (_order_key(e), fields))
    # Only a pair that agrees on every value no UPDATE of the window
    # touched can be one feature: pairs are looked for among the rows that
    # share those values (a mass rewind would otherwise compare every
    # creation with every deletion).
    segments = []
    for key, chain in chains.items():
        for segment in _deleted_segments(sorted(chain, key=_order_key, reverse=True)):
            start = _state_before_chain(segment)
            if start is not None:
                segments.append((key, segment, start))
    created_states = [(r, _full_state(r)) for r in creators]
    created_states = [(r, st) for r, st in created_states if st is not None]
    if not segments or not created_states:
        return []
    signature = _unedited_signature(
        [st for _k, _s, st in segments] + [st for _r, st in created_states], edits)
    groups: Dict[tuple, List[tuple]] = {}
    for r, st in created_states:
        ds = r.datasource_fingerprint or ""
        groups.setdefault((ds,) + signature(ds, st), []).append((r, st))
    refusals: List[AuditEvent] = []
    for key, segment, start in segments:
        oldest = _order_key(segment[-1])
        ds = segment[0].datasource_fingerprint or ""
        for r, created in groups.get((ds,) + signature(ds, start), ()):
            if _entity_key(r) == key or not _order_key(r) < oldest:
                continue
            diff = {f for f in set(created[0]) & set(start[0])
                    if created[0][f] != start[0][f]}
            if (created[1] is not None and start[1] is not None
                    and created[1] != start[1]):
                diff.add(None)
            changed: set = set()
            anything = False
            for pos, fields in edits.get(ds, ()):
                if _order_key(r) < pos < oldest:
                    if fields is None:
                        anything = True
                    else:
                        changed |= fields
            if not anything and diff - changed:
                continue
            refusals.append(build_refusal_item(
                segment[0], (segment[0].created_at, _position_eid(segment[0])),
                f"{_describe(segment[0])} deleted, under record "
                f"{_fid_number(key)}, a feature that may be the one "
                f"{_describe(r)} created or re-inserted under record "
                f"{_fid_number(_entity_key(r))}: they differ only on "
                f"values edited in between ("
                f"{sorted('geometry' if f is None else f for f in diff)[:6]}), "
                f"and a shapefile renumbers its records after a delete; "
                f"the journal can neither prove them one feature nor two, "
                f"and the rewind would re-insert it without being able to "
                f"delete it; choose a date at or after {r.created_at}"))
            flog(f"rewind_dedup: creation eid={r.event_id} and the chain "
                 f"{[e.event_id for e in segment]} of {key[-40:]} neither "
                 f"paired nor told apart: refused by name", "ERROR")
            break
    return refusals


def _canonical_value(value):
    """A hashable reading of *value* under which equal values are equal
    (1 and 1.0 included, as ``==`` reads them)."""
    if isinstance(value, bool):
        return ("b", value)
    if isinstance(value, (int, float)):
        return ("n", float(value))
    return ("j", json.dumps(value, sort_keys=True, default=str))


def _unedited_signature(states: List[tuple], edits: Dict[str, List[tuple]]):
    """signature(ds, state): the values of *state* that no UPDATE of *ds* in
    the window changed, on the fields every state carries (the geometry
    when no UPDATE changed one and every state has one). Two states whose
    signatures differ differ on a value nothing edited."""
    common = set.intersection(*(set(st[0]) for st in states))
    with_geom = all(st[1] is not None for st in states)
    edited: Dict[str, Optional[set]] = {}
    for ds, rows in edits.items():
        fields: Optional[set] = set()
        for _pos, changed in rows:
            if changed is None:
                fields = None
                break
            fields |= changed
        edited[ds] = fields

    def signature(ds, state):
        fields = edited.get(ds, set())
        if fields is None:
            return ()
        attrs, geom = state
        out = tuple(_canonical_value(attrs[f]) for f in sorted(common - fields))
        if with_geom and None not in fields:
            out += (geom,)
        return out
    return signature


def _deleted_segments(chain_desc: List[AuditEvent]) -> List[List[AuditEvent]]:
    """The lifetimes of a chain that END on a user DELETE, newest first.

    Each segment is a DELETE and the UPDATEs before it back to the
    previous DELETE. A segment reaching an INSERT was created in the
    window (not a re-inserted feature) and is left out, like the UPDATEs
    after the newest DELETE (the feature is still there). On a shapefile
    one FID holds several lifetimes after a repack (records renumbered),
    hence segments rather than whole chains.
    """
    segments: List[List[AuditEvent]] = []
    cur: Optional[List[AuditEvent]] = None
    for row in chain_desc:
        op = row.operation_type
        if op == "DELETE":
            if cur is not None:
                segments.append(cur)
            cur = [row]
        elif op == "UPDATE":
            if cur is not None:
                cur.append(row)
        elif op == "INSERT":
            cur = None
        else:
            return []
    if cur is not None:
        segments.append(cur)
    return segments


def _place_reapplied_events(active: List[AuditEvent],
                            traces: List[AuditEvent],
                            status: Dict[int, bool],
                            keep: Optional[set] = None) -> List[AuditEvent]:
    """Move a re-applied user event to the place of its re-application.

    *keep*: user events whose every write is already a row of the plan
    (``_window_per_write``): they stay at their own date, the
    re-application has its own row.

    The unwinding order is the order in which the effects entered the
    data, newest first. A user event E compensated by a rewind (trace R)
    and brought back by a later forward hop (trace C, which cancels R)
    has been in the data since C, not since its own created_at. Unwound
    at its own date, its compensation ran AFTER the undo of every toggle
    written between its date and C: two edits of the same field then
    came back in the wrong order and the field ended on the value the
    forward hop had overwritten (found by rw_state_at_t_property, seed
    318: the geometry of C003 reset to its seed value instead of the one
    it had at the chosen date, rewind reported applied).

    Such an event becomes a recompensate plan row at (C.created_at,
    C.event_id): same id, same payload, same trace (it references E), at
    the place of the toggle, like the history items of the window. Only
    cancellations are visible here: the window leaves out invalidated
    traces, so an event brought back by an Undo keeps its own date.
    """
    latest_canceller: Dict[int, AuditEvent] = {}
    for c in traces:
        if not status.get(c.event_id) or not is_exact_trace(c):
            continue
        cur = latest_canceller.get(c.restored_from_event_id)
        if cur is None or _order_key(c) > _order_key(cur):
            latest_canceller[c.restored_from_event_id] = c
    if not latest_canceller:
        return active
    reapplied_at: Dict[int, AuditEvent] = {}
    for t in traces:
        c = latest_canceller.get(t.event_id)
        if c is None:
            continue
        cur = reapplied_at.get(t.restored_from_event_id)
        if cur is None or _order_key(c) > _order_key(cur):
            reapplied_at[t.restored_from_event_id] = c
    out: List[AuditEvent] = []
    for e in active:
        c = reapplied_at.get(e.event_id)
        if keep and e.event_id in keep:
            c = None
        if c is not None and _order_key(c) > _order_key(e):
            flog(f"rewind_dedup: eid={e.event_id} op={e.operation_type} "
                 f"re-applied by trace eid={c.event_id} at {c.created_at}: "
                 f"unwound at that place, not at its own date "
                 f"{e.created_at}")
            e = build_recompensate_item(e, (c.created_at, c.event_id))
        out.append(e)
    return out


def _neutralise(user_events: List[AuditEvent], traces: List[AuditEvent],
                status: Dict[int, bool], rows: List[AuditEvent],
                history_items: Optional[List[AuditEvent]] = None,
                ) -> Tuple[List[AuditEvent], List[AuditEvent],
                           List[AuditEvent], set]:
    """Split the user events into (still to compensate, already compensated)
    and give the extra plan rows (per-write rows, named refusals) with the
    events whose writes they carry (``_window_per_write``).

    By event_id for every trace in effect; by time span only for the
    legacy traces whose shape proves a fused chain (module docstring). A
    legacy trace whose shape is ambiguous, or whose span would be keyed on
    an identity that names several features, is a refusal row: its
    coverage can only be guessed, and a wrong guess is a silent wrong
    state.
    """
    by_eid: Dict[int, AuditEvent] = {
        e.event_id: e for e in user_events if e.event_id is not None
    }
    window_eids = {e.event_id for e in rows if e.event_id is not None}
    # Oldest position the window can name: the low bound of the span of a
    # LEGACY trace whose source was purged, and never wider.
    window_floor = min((_order_key(e) for e in rows), default=("", 0))
    referenced = {t.restored_from_event_id for t in traces}
    by_key: Dict[str, List[AuditEvent]] = {}
    for e in user_events:
        by_key.setdefault(_entity_key(e), []).append(e)

    neutralised_user_eids: set = set()
    refusals: List[AuditEvent] = []
    per_write, per_write_refusals = _window_per_write(
        user_events, traces, status, by_key, history_items)
    refusals.extend(per_write_refusals)
    for items in per_write.values():
        refusals.extend(items)
    # {entity key: [(low, high, trace)]}: a legacy span covers the rows of
    # the entity in [low, high) that the journal held when the trace was
    # written (`_written_before`).
    legacy_spans: Dict[str, list] = {}

    def _open_span(trace, low, what):
        key = _entity_key(trace)
        if UNPROVEN_KEY_MARK in key:
            # The span would stand for the feature's chain, but the key it
            # is read on names several features (first column of a
            # composite key): refused, never spread over another feature.
            refusals.append(build_refusal_item(
                trace, (trace.created_at, trace.event_id or 0),
                f"a compensation written on {trace.created_at} by an older "
                f"version of RecoverLand covers {what} on a key that names "
                f"several features "
                f"({(trace.feature_identity_json or '')[:80]}): what it "
                f"covered cannot be tied to one feature; choose a date at "
                f"or after {trace.created_at}"))
            return
        legacy_spans.setdefault(key, []).append((low, _order_key(trace), trace))

    for trace in traces:
        if not status.get(trace.event_id):
            continue
        ref = trace.restored_from_event_id
        src = by_eid.get(ref)
        if src is not None and ref in per_write:
            continue  # every write of it is in the plan, one row each
        if src is not None:
            neutralised_user_eids.add(ref)
            if is_exact_trace(trace):
                continue
            shape = legacy_trace_shape(trace, src, _count_unreferenced_between(
                src, trace, by_key, referenced))
            if shape == LEGACY_FUSED:
                _open_span(trace, _order_key(src), "a fused chain")
                flog(f"rewind_dedup: legacy trace eid={trace.event_id} "
                     f"op={trace.operation_type} references "
                     f"eid={ref} op={src.operation_type}: fused chain "
                     f"(schema {trace.event_schema_version}), coverage "
                     f"kept as the time span up to the trace", "WARNING")
            elif shape == LEGACY_AMBIGUOUS:
                reason = (
                    f"the compensation of event {ref} written on "
                    f"{trace.created_at} by an older version of RecoverLand "
                    f"is followed by {_LEGACY_MAX_CHAIN - 1} or more edits "
                    f"of the same feature that no compensation names: it "
                    f"may have fused them (a rewind) or covered event {ref} "
                    f"alone (a restore of one edit), and the journal cannot "
                    f"tell; choose a date at or after {trace.created_at}")
                refusals.append(build_refusal_item(
                    trace, (trace.created_at, trace.event_id or 0), reason))
                flog(f"rewind_dedup: legacy trace eid={trace.event_id} "
                     f"references eid={ref}: ambiguous coverage, refused "
                     f"by name", "ERROR")
            continue
        if ref in window_eids:
            # A trace referencing a trace. Exact: the cancellation is
            # already in `status`. Legacy: ignored as a canceller, and it
            # compensates no user event by itself.
            continue
        if not is_exact_trace(trace):
            # LEGACY trace whose source left the journal (retention). It
            # may stand for a fused chain whose surviving members are in
            # this window: nothing proves otherwise, so they stay covered
            # down to the oldest row the window holds. The window only
            # hands over a legacy trace whose source is ABSENT from the
            # journal: one referencing a trace that exists outside the
            # window is a legacy trace-to-trace row, dropped there
            # (event_stream_repository._drop_legacy_trace_links).
            _open_span(trace, window_floor, "a purged event")
            flog(f"rewind_dedup: orphan legacy trace eid={trace.event_id} "
                 f"references eid={ref}, purged from the journal; "
                 f"compensated span opened to the surviving history",
                 "WARNING")
        # An exact orphan covers its purged source only: nothing here. The
        # window names the refusal when the cutoff needs that source.

    active: List[AuditEvent] = []
    dropped: List[AuditEvent] = []
    for e in user_events:
        if e.event_id is None:
            active.append(e)
            continue
        if e.event_id in neutralised_user_eids:
            dropped.append(e)
            continue
        key_e = _order_key(e)
        if any(lo <= key_e < hi and _written_before(e, trace)
               for lo, hi, trace in legacy_spans.get(_entity_key(e), ())):
            dropped.append(e)
            continue
        active.append(e)

    # Deliberately NOT gated behind RECOVERLAND_HEAVY_DIAG, unlike the
    # other per-event dumps: `rw_dedup_post_trace_edit` asserts on
    # `rewind_dedup: neutralised eid=<n>`, so these lines are a contract,
    # not diagnostics. The cost is bounded by the number of ALREADY
    # COMPENSATED events (`dropped` is empty on a first rewind).
    if dropped:
        flog(f"rewind_dedup: neutralised {len(dropped)} event(s): "
             f"{len(neutralised_user_eids)} by event_id, "
             f"{len(legacy_spans)} legacy span(s)")
    for e in dropped:
        flog(f"rewind_dedup: neutralised eid={e.event_id} "
             f"op={e.operation_type} "
             f"identity={(e.feature_identity_json or '')[:80]}")
    return active, dropped, refusals, set(per_write)


def _tree_alternates(tree: List[Tuple[AuditEvent, int]]) -> Tuple[bool, bool]:
    """(the traces of a user event flip it one by one, event present after
    them).

    The second value is the physical replay (last write wins); the window
    reads the event compensated when ANY direct trace of it is in effect,
    and the two disagree after a double compensation whose first one was
    cancelled: then too each write gets its row.

    The event is in the data when it is written; each trace, oldest first,
    must then compensate it (odd depth) while it is present and re-apply
    it (even depth) while it is compensated. The window holds no
    invalidated trace (created and undone after T: a neutral pair). A
    legacy trace-to-trace row is ignored (unknown meaning), and so is a
    legacy trace that does not flip the event: before v7 a restore that
    skipped (nothing to write) still wrote its trace.
    """
    present = True
    for trace, depth in tree:
        if depth >= 2 and not is_exact_trace(trace):
            continue
        compensates = depth % 2 == 1
        if compensates != present:
            if not is_exact_trace(trace):
                continue
            return False, present
        present = not present
    return True, present


def _window_per_write(user_events: List[AuditEvent], traces: List[AuditEvent],
                      status: Dict[int, bool], by_key: Dict[str, List[AuditEvent]],
                      history_items: Optional[List[AuditEvent]] = None,
                      ) -> Tuple[Dict[int, List[AuditEvent]], List[AuditEvent]]:
    """User events after T written over SEVERAL times, with an edit between.

    The window reads a user event E as compensated or not (one trace in
    effect is enough). That net reading is exact while nothing else was
    written on the feature between E's traces. A Mode A restore of E,
    the user doing the edit again, a second restore of E: the second
    trace physically undid the re-done edit, not E, and the net reading
    compensates E once and the re-done edit a second time. A DELETE
    restored twice around a second DELETE came back twice (duplicate
    key, commit refused) or not at all (rw_state_at_t_toggles).

    For such an E every write is undone at its own place, like the history
    items of the window (event_stream_repository._per_toggle_items): E
    keeps its own row, and each trace of its tree gets the inverse of what
    it wrote (odd depth: E re-applied, the new trace cancels it; even
    depth: E compensated again, the new trace cancels the hop). The
    lifetime rule of the collapse then cancels what cancels.

    Triggers, any of them: a user event of E's feature (of E's layer for
    a DELETE, whose re-inserted feature may carry a new FID) strictly
    between E's first and last trace, still in the data (no trace in
    effect compensates it); traces that do not flip E one by one
    (`_tree_alternates`: a second restore of E physically undid an edit
    done again); E lying between two history items of its entity written
    one per write (the window's `_per_toggle_items`): then E and its
    compensation are not a neutral pair at the place the items are
    undone, each has its row. A tree holding a legacy trace, or an INSERT
    (the planner's phases re-insert before they delete), is refused by
    name instead.

    Returns ({E event_id: rows}, refusals).
    """
    trace_by_eid = {t.event_id: t for t in traces if t.event_id is not None}
    by_eid = {e.event_id: e for e in user_events if e.event_id is not None}

    def root_and_depth(t):
        depth, cur, seen = 0, t, set()
        while cur.restored_from_event_id is not None:
            ref = cur.restored_from_event_id
            if ref in seen:
                return None, 0
            seen.add(ref)
            depth += 1
            if ref in by_eid:
                return ref, depth
            cur = trace_by_eid.get(ref)
            if cur is None:
                return None, 0
        return None, 0

    trees: Dict[int, List[Tuple[AuditEvent, int]]] = {}
    for t in traces:
        rid, depth = root_and_depth(t)
        if rid is not None:
            trees.setdefault(rid, []).append((t, depth))
    compensated = {t.restored_from_event_id for t in traces
                   if t.restored_from_event_id in by_eid and status.get(t.event_id)}
    by_ds: Dict[str, List[AuditEvent]] = {}
    for e in user_events:
        by_ds.setdefault(e.datasource_fingerprint or "", []).append(e)

    # Positions of the history items written one per write (several for
    # one entity): an edit of that entity between two of them is unwound
    # with its own compensations, row by row, or the items' proofs meet a
    # state the neutralised pair never restored.
    history_at: Dict[str, List[tuple]] = {}
    for item in history_items or ():
        if plan_item_kind(item) in (PLAN_MARK_REAPPLY, PLAN_MARK_RECOMPENSATE):
            history_at.setdefault(_entity_key(item), []).append(_order_key(item))

    per_write: Dict[int, List[AuditEvent]] = {}
    refusals: List[AuditEvent] = []
    for rid, tree in trees.items():
        root = by_eid[rid]
        tree.sort(key=lambda td: _order_key(td[0]))
        low, high = _order_key(tree[0][0]), _order_key(tree[-1][0])
        between: List = []
        if len(tree) >= 2:
            pool = (by_ds.get(root.datasource_fingerprint or "", ())
                    if root.operation_type == "DELETE"
                    else by_key.get(_entity_key(root), ()))
            between = [u.event_id for u in pool
                       if u.event_id != rid and u.event_id not in compensated
                       and low < _order_key(u) < high]
        marks = history_at.get(_entity_key(root), ())
        position = _order_key(root)
        interleaved = (any(m < position for m in marks)
                       and any(m > position for m in marks))
        alternates, present = _tree_alternates(tree)
        if not (between or interleaved or not alternates
                or present == (rid in compensated)):
            continue
        last = tree[-1][0]
        if root.operation_type == "INSERT" or any(
                not is_exact_trace(t) for t, _d in tree):
            refusals.append(build_refusal_item(
                last, (last.created_at, last.event_id or 0),
                f"event {rid} was compensated or re-applied {len(tree)} "
                f"time(s), not one write after the other or with edits in "
                f"between ({between[:5]}), and these writes cannot be undone "
                f"in order; choose a date at or after {last.created_at}"))
            continue
        rows = []
        for t, depth in tree:
            position = (t.created_at, t.event_id or 0)
            rows.append(build_reapply_item(root, t, position) if depth % 2
                        else build_recompensate_item(root, position, via=t))
        flog(f"rewind_dedup: eid={rid} op={root.operation_type} written over "
             f"{len(tree)} time(s), edits {between[:8]} in between, "
             f"interleaved with history items={interleaved}: one row per "
             f"write, each at its place")
        per_write[rid] = rows
    return per_write, refusals


def _log_plan_items(plan_items: List[AuditEvent]) -> None:
    for item in plan_items:
        kind = plan_item_kind(item)
        flog(f"rewind_dedup: plan_item kind={kind[len(PLAN_MARK_PREFIX):]} "
             f"eid={item.event_id} op={item.operation_type} "
             f"at={item.created_at} "
             f"identity={(item.feature_identity_json or '')[:80]}"
             + (f" reason={refusal_reason(item)!r}"
                if kind == PLAN_MARK_REFUSED else ""),
             "ERROR" if kind == PLAN_MARK_REFUSED else "INFO")


def _collapse_user_chain_with_stats(
    events: List[AuditEvent],
) -> Tuple[List[AuditEvent], int]:
    """Same as _collapse_user_chain but also returns the count of events
    eliminated by the chain collapse (raw_in - len(result))."""
    result = _collapse_user_chain(events)
    return result, max(len(events) - len(result), 0)


def _collapse_user_chain(events: List[AuditEvent]) -> List[AuditEvent]:
    """Per-entity collapse of the plan (user events and plan items).

    Strategy: keep the entire per-entity chain unchanged so that the
    restore planner can unwind the entity step by step, newest first, each
    compensation bringing the entity to the state captured by the next
    older row. Every row keeps its own id and its own payload: that is
    what makes its compensation, and the trace written for it, exact.

    Collapse rules only DROP rows, never rewrite one
    (`_cancel_internal_lifetimes`): a lifetime the plan would create and
    destroy is cancelled, e.g. INSERT -> UPDATEs -> DELETE made after the
    cutoff (RW-20), or a feature the plan would re-insert and then delete
    again; so is a delete followed by the re-insert of the same snapshot,
    and a run of UPDATE rows that leaves the feature as it found it. A
    chain holding a refusal is kept whole: the layer is refused anyway,
    and the refusal must reach the planner.

    Schema 7 removed the two fusions that used to live here (UPDATE..DELETE
    into one synthetic DELETE, chains longer than 10 into one synthetic
    event). A synthetic event carried the id of the OLDEST real event with
    a payload of its own, so its trace could not say what it covered (see
    the module docstring). The UPDATE comps they avoided are reached on the
    re-inserted feature through the planner's fid_remap (Phase 0 re-insert,
    then Phase 1 UPDATEs).
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
    cancelled = 0

    for key in order:
        chain = _inherit_bucket_fingerprint(
            sorted(buckets[key], key=_order_key, reverse=True))
        if len(chain) == 1 or any(
                e.operation_type not in _INVERSE_OP for e in chain):
            result.extend(chain)
            continue
        chain_before = len(chain)
        chain = _cancel_internal_lifetimes(chain)
        cancelled += max(chain_before - len(chain), 0)
        if not chain:
            skipped_entities += 1
            continue
        result.extend(chain)

    raw = len(events)
    deduped = len(result)
    if raw != deduped:
        flog(f"rewind_dedup: user_chain {raw} -> {deduped} events"
             f" ({raw - deduped} redundant, {skipped_entities} no-op,"
             f" {cancelled} cancelled in created-then-destroyed lifetimes"
             f" RW-20)")

    return result


def _inherit_bucket_fingerprint(chain: List[AuditEvent]) -> List[AuditEvent]:
    """Give a legacy row with no entity_fingerprint the one of its entity.

    A row written before the entity_fingerprint backfill lands in the
    right bucket (``_entity_key`` canonicalises its identity JSON), but the
    planner drops any row without a fingerprint: it cannot name the
    feature on its own. Inside a chain whose other rows DO carry the
    fingerprint of that very key, the identity is proven by them. The
    fused synthetic event used to inherit it the same way; without fusion
    the row would be skipped and the feature re-inserted with the value
    edited after the cutoff (tx_rewind_broken_trace_memory, case O2a). A
    row alone in its bucket keeps no fingerprint and stays diagnosed.
    """
    known = {e.entity_fingerprint for e in chain if e.entity_fingerprint}
    if len(known) != 1 or all(e.entity_fingerprint for e in chain):
        return chain
    fingerprint = next(iter(known))
    out = []
    for e in chain:
        if not e.entity_fingerprint and not is_plan_item(e):
            flog(f"rewind_dedup: eid={e.event_id} has no entity_fingerprint; "
                 f"its chain names it {fingerprint}")
            e = e._replace(entity_fingerprint=fingerprint)
        out.append(e)
    return out


def _cancel_internal_lifetimes(chain_desc: List[AuditEvent]) -> List[AuditEvent]:
    """Cancel the lifetimes a single-entity plan would create then destroy.

    Walk in APPLICATION order (newest first, as the compensations are
    written) and read each row by the compensation it writes: a re-insert
    (compensation of a DELETE, or of an INSERT-like plan item) opens a
    lifetime, an UPDATE joins the open lifetime, a delete (compensation of
    an INSERT, or of a DELETE-like plan item) closes the most recent one.
    The rows of a closed lifetime are dropped: the plan would re-insert a
    feature and delete it again, a net no-op. On user events alone this
    is the historical rule read from the other end (an INSERT then a
    DELETE after the cutoff: the feature was born and died in the window).

    Why it matters beyond saving writes: a feature the plan re-inserts is
    a fresh buffer feature, and the executor's FID-reuse guards (negative
    remap ignored, re-inserted features excluded from the snapshot scan)
    rightly refuse to delete it in the same batch. Planning both halves
    left the feature in the layer and the step reported as already done.

    Rows outside a closed lifetime (updates with no open lifetime, a
    delete with nothing to close, the still-open last lifetime) are kept.
    Returns rows in the original DESC order. Empty list = the entire chain
    cancels (caller treats as no-op).

    The mirror shape is cancelled too (``_cancel_replacements``): a delete
    immediately followed, in application order, by the re-insert of the
    SAME snapshot. The planner runs re-inserts before deletes (phases), so
    that pair ran backwards: the re-insert found the feature still there
    and the delete then refused the feature it had just written (a
    DELETE restored, then rewound over with every write undone one by
    one: rw_state_at_t_toggles). Replacements go FIRST: taken after the
    lifetimes, a re-insert of state A could close on the delete of state
    B across the UPDATE A->B between them, and leave "delete the feature,
    re-insert it with another state" under one key, which the phases run
    backwards and the commit check reads as a feature that should be gone.
    Taken first, the same chain reduces to the UPDATE alone.

    And a run of consecutive UPDATE rows that leaves the feature as it
    found it is dropped (``_cancel_update_cycles``): the rows written for
    each write of a restored-twice event, undone one by one, come back
    rewind after rewind, and without that rule every rewind across them
    wrote one more trace per row (plans doubling, rw_state_at_t_toggles).
    The passes repeat until nothing more cancels. A lifetime that a key
    naming a record cannot prove (``_cancel_lifetimes_once``) stops them:
    the chain is kept whole with a refusal row, the layer is refused.
    """
    current = chain_desc
    for _round in range(16):
        reduced, refusals = _cancel_lifetimes_once(
            _cancel_update_cycles(_cancel_replacements(current)))
        if refusals:
            return list(chain_desc) + refusals
        if len(reduced) == len(current):
            break
        current = reduced
    return current if len(current) != len(chain_desc) else chain_desc


def _cancel_lifetimes_once(chain_desc: List[AuditEvent]
                           ) -> Tuple[List[AuditEvent], List[AuditEvent]]:
    """One pass of the lifetime rule of `_cancel_internal_lifetimes`.

    Returns (rows kept, refusals). On a key that names a RECORD
    (``_weak_key``: a bare FID, a legacy key of unknown columns) rows of
    different features can share the bucket. A lifetime there closes on a
    delete whose snapshot is the state it ends in (re-inserted snapshot,
    its UPDATEs unwound), or on a delete committed in the SAME edit
    session as the creation (a FID never changes within one). Anything
    else cannot be proven one feature: the created feature was changed
    outside the journal (another program, the plugin paused, a capture
    outage), or the record was renumbered (a shapefile repack).
    Cancelling would drop another feature's delete; keeping both halves
    re-inserts the feature and then fails to delete what the batch has
    just re-inserted, the rewind reported done (review finding
    external-edit-lifetime). So it is refused by name.
    """
    weak = any(_weak_key(e) for e in chain_desc)
    open_lifetimes: List[List[AuditEvent]] = []
    kept: List[AuditEvent] = []
    refusals: List[AuditEvent] = []
    closed_count = 0
    for event in chain_desc:
        written = _INVERSE_OP.get(event.operation_type)
        if written == "INSERT":
            open_lifetimes.append([event])
        elif written == "DELETE":
            match = None
            for i in range(len(open_lifetimes) - 1, -1, -1):
                if not weak or _lifetime_ends_as(open_lifetimes[i], event):
                    match = i
                    break
            if match is None and open_lifetimes:
                last = open_lifetimes[-1][0]
                if _same_edit_session(last, event):
                    match = len(open_lifetimes) - 1
                    flog(f"rewind_dedup: lifetime eid={event.event_id}.."
                         f"eid={last.event_id} of "
                         f"{(event.entity_fingerprint or '')[:40]} ends in "
                         f"another state than it began, in one edit session "
                         f"(a FID does not change within one): cancelled")
                elif not refusals:
                    refusals.append(_lifetime_refusal(event, last))
            if match is not None:
                open_lifetimes.pop(match)  # created then destroyed: cancelled
                closed_count += 1
            else:
                kept.append(event)
        elif open_lifetimes:
            open_lifetimes[-1].append(event)
        else:
            kept.append(event)
    if refusals:
        return chain_desc, refusals
    if closed_count == 0:
        return chain_desc, []  # nothing cancelled, preserve original
    for lifetime in open_lifetimes:
        kept.extend(lifetime)
    kept.sort(key=_order_key, reverse=True)
    return kept, []


def _lifetime_refusal(created: AuditEvent, deleted: AuditEvent) -> AuditEvent:
    """Refusal of a lifetime a key naming a record cannot prove.

    *created*: the row whose compensation deletes the feature (a user
    INSERT, or the undo of a trace's re-insert); *deleted*: the row whose
    compensation re-inserts one (a user DELETE, or the undo of a trace's
    delete).
    """
    key = compute_entity_key(deleted.entity_fingerprint,
                             deleted.feature_identity_json)
    number = _fid_number(key)
    record = (f"record {number} of a layer without a primary key"
              if number is not None
              else f"the key {key.split(LEGACY_KEY_MARK, 1)[0]} (key columns "
                   f"of the layer unknown)")
    return build_refusal_item(
        deleted, (deleted.created_at, _position_eid(deleted)),
        f"{_describe(created)} created a feature and {_describe(deleted)} "
        f"deleted one under {record}, in other states and not in one edit "
        f"session, with no journaled edit that explains the difference: the "
        f"feature was changed outside RecoverLand, or the record number "
        f"named another feature (a shapefile renumbers its records after a "
        f"delete); the journal cannot prove they are one feature, and the "
        f"rewind would re-insert it without being able to delete it; choose "
        f"a date at or after {created.created_at}")


# An UPDATE run longer than this is checked in windows of this size: the
# cost stays linear in the chain, and a real cycle is short.
_CYCLE_SPAN = 200


def _update_sides(row: AuditEvent) -> Optional[Tuple[dict, dict]]:
    """(values the row's compensation PROVES, values it WRITES), or None.

    Every UPDATE-like row of the plan is compensated the same way: it
    checks the NEW side and writes the OLD side (a REAPPLY row carries the
    sides swapped). The geometry is the key ``None`` when the row
    changed it.
    """
    changed = _payload(row).get("changed_only")
    if not isinstance(changed, dict):
        return None
    proves = {f: extract_delta_new(v) for f, v in changed.items()}
    writes = {f: extract_delta_old(v) for f, v in changed.items()}
    if row.geometry_wkb is not None or row.new_geometry_wkb is not None:
        if row.geometry_wkb is None or row.new_geometry_wkb is None:
            return None
        old, new = bytes(row.geometry_wkb), bytes(row.new_geometry_wkb)
        writes[None] = None if old == GEOMETRY_NULL_MARKER else old
        proves[None] = None if new == GEOMETRY_NULL_MARKER else new
    return proves, writes


def _cancel_update_cycles(rows_desc: List[AuditEvent]) -> List[AuditEvent]:
    """Drop the runs of UPDATE rows that leave the feature as they found it.

    In application order, rows i..j (UPDATEs only, nothing that changes
    the feature's existence between them, each row proving what the rows
    before it in the run wrote) start from the state the first of them
    proves on each field and end on the state the last of them writes on
    it. When the two are equal on every field the run touches, geometry
    included, the run is a net no-op on the feature: the plan would write
    values and write them back. Dropped like a lifetime that
    is created and destroyed: nothing written, nothing traced. The rows
    around it still prove, at the run's two ends, the very state the
    dropped rows would have checked.
    """
    out = list(rows_desc)
    changed = True
    while changed:
        changed = False
        for i in range(len(out)):
            if out[i].operation_type != "UPDATE":
                continue
            first_proven: dict = {}
            last_written: dict = {}
            for j in range(i, min(len(out), i + _CYCLE_SPAN)):
                if out[j].operation_type != "UPDATE":
                    break
                sides = _update_sides(out[j])
                if sides is None:
                    break
                # Each row must prove what the rows before it in the run
                # left: a run whose rows do not chain is no no-op, whatever
                # its two ends say (a row moved over other writes by a
                # forward hop, rw_state_at_t_toggles seed 199).
                if any(field in last_written and last_written[field] != value
                       for field, value in sides[0].items()):
                    break
                for field, value in sides[0].items():
                    first_proven.setdefault(field, value)
                last_written.update(sides[1])
                if first_proven and all(
                        last_written[f] == v for f, v in first_proven.items()):
                    flog(f"rewind_dedup: UPDATE rows "
                         f"{[e.event_id for e in out[i:j + 1]]} of "
                         f"{(out[i].entity_fingerprint or '')[:40]} leave the "
                         f"feature as they found it: dropped")
                    out = out[:i] + out[j + 1:]
                    changed = True
                    break
            if changed:
                break
    return out


def _lifetime_ends_as(lifetime: List[AuditEvent], delete_row: AuditEvent) -> bool:
    """True when the plan's lifetime ends in the state *delete_row* removes.

    *lifetime*: the re-insert (first) and the UPDATE rows joined to it, in
    application order; each UPDATE row writes its OLD side. A row without
    a complete snapshot cannot be checked and is trusted (the key decides,
    as before). Compared on the fields both snapshots carry (a field added
    to the layer in between is not a difference of feature), geometries
    when both have one.
    """
    state = _full_state(lifetime[0])
    target = _full_state(delete_row)
    if state is None or target is None:
        return True
    attrs, geom = state
    for row in lifetime[1:]:
        changed = _payload(row).get("changed_only")
        if not isinstance(changed, dict):
            return True
        for field, delta in changed.items():
            attrs[field] = extract_delta_old(delta)
        if row.geometry_wkb is not None:
            old = bytes(row.geometry_wkb)
            geom = None if old == GEOMETRY_NULL_MARKER else old
    t_attrs, t_geom = target
    if any(attrs[k] != t_attrs[k] for k in set(attrs) & set(t_attrs)):
        return False
    return geom is None or t_geom is None or geom == t_geom


def _cancel_replacements(rows_desc: List[AuditEvent]) -> List[AuditEvent]:
    """Drop a delete directly followed by the re-insert of the same snapshot.

    In application order (newest first) the plan would delete the feature
    and write it back identical: a net no-op, as long as nothing is
    written between them (nothing can be: the feature is gone). Both rows
    must carry a complete snapshot (attributes and geometry) and the two
    must be equal; anything else is kept.
    """
    out = list(rows_desc)
    changed = True
    while changed:
        changed = False
        for i in range(len(out) - 1):
            a, b = out[i], out[i + 1]
            if (_INVERSE_OP.get(a.operation_type) != "DELETE"
                    or _INVERSE_OP.get(b.operation_type) != "INSERT"):
                continue
            sa = _full_state(a)
            if sa is None or sa != _full_state(b):
                continue
            flog(f"rewind_dedup: delete eid={a.event_id} then re-insert "
                 f"eid={b.event_id} of the same snapshot "
                 f"({(a.entity_fingerprint or '')[:40]}): cancelled")
            out = out[:i] + out[i + 2:]
            changed = True
            break
    return out
