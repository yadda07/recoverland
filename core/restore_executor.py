"""Restore executor for RecoverLand (BL-04).

Applies a RestorePlan to a QGIS layer. Supports strict_atomic
(editing buffer + rollback) and best_effort (per-entity isolation).

Must run on main thread only.
"""
from typing import Dict, List, Optional

from .audit_backend import AuditEvent, RestoreReport
from .restore_contracts import (
    RestorePlan, AtomicityPolicy, PreflightVerdict,
    RestoreSession,
)
from .constants import MAKEVALID_DRIFT_TOLERANCE
from .restore_planner import preflight_check
from .restore_service import restore_batch, build_restore_trace_event
from .search_service import reconstruct_attributes
from .schema_drift import (
    safe_field_mapping, field_mapping_report, format_dropped_fields,
)

from .geometry_utils import (
    rebuild_geometry,
    feature_matches_geometry, get_feature_source,
    wkb_short_repr, feature_geom_short_repr_diag,
    heavy_diag_enabled,
    geometry_for_layer_crs, extract_crs_authid,
    _compute_makevalid_drift,
)
from .serialization import iter_mapped_attributes
from .logger import flog, flog_kv


# BL-RW-P3-18: stable reason codes used to tag each restore step result.
# The runner aggregates these into the runner.finished signal so the UI
# can show "applied / skipped_idempotent / failed / failed_target_absent
# / failed_geometry_drift" breakdown.
RESTORE_REASON_APPLIED = "applied"
# RLU-053: the write happened but the schema moved under it, so some
# captured fields never made it back. Distinct from "applied" on purpose:
# an amputated restore must never be counted with the complete ones.
RESTORE_REASON_APPLIED_PARTIAL = "applied_partial"
RESTORE_REASON_SKIPPED_IDEMPOTENT = "skipped_idempotent"
RESTORE_REASON_TARGET_ABSENT = "target_absent"
RESTORE_REASON_GEOMETRY_DRIFT = "geometry_drift"
RESTORE_REASON_FAILED = "failed"


def preflight_layer_check(plan: RestorePlan, layer) -> List[str]:
    """Check layer-level prerequisites that require QGIS objects.

    Returns list of blocking reasons (empty = OK).
    """
    from ..compat import QgisCompat

    blocking: List[str] = []
    if layer is None:
        blocking.append("Layer is None")
        return blocking
    if not hasattr(layer, 'dataProvider') or layer.dataProvider() is None:
        blocking.append("Layer has no data provider")
        return blocking

    caps = layer.dataProvider().capabilities()
    needed_ops = {a.compensatory_op for a in plan.actions}
    cap_map = {
        "INSERT": (QgisCompat.CAP_ADD_FEATURES, "AddFeatures"),
        "DELETE": (QgisCompat.CAP_DELETE_FEATURES, "DeleteFeatures"),
        "UPDATE": (QgisCompat.CAP_CHANGE_ATTRIBUTE_VALUES, "ChangeAttributeValues"),
    }
    for op in needed_ops:
        entry = cap_map.get(op)
        if entry and not (caps & entry[0]):
            blocking.append(f"Provider missing capability: {entry[1]}")

    has_geom_update = any(
        a.has_geometry and a.compensatory_op == "UPDATE" for a in plan.actions
    )
    if has_geom_update and not (caps & QgisCompat.CAP_CHANGE_GEOMETRIES):
        blocking.append("Provider missing capability: ChangeGeometries")

    return blocking


def execute_restore_plan(
    plan: RestorePlan,
    events_by_id: Dict[int, AuditEvent],
    layer,
    trace_id: str = "",
    conn=None,
) -> RestoreReport:
    """Execute a restore plan against a single layer.

    Runs pure preflight, then layer preflight, then dispatches.

    *conn* is a (read) connection to the journal the events come from.
    The coverage guard of the preflight reads, through it, the purge
    horizon the JOURNAL recorded for the layer, which survives a restart.
    Without it only this session's memory of its own purges is consulted:
    after a restart, a rewind to a date the automatic purge had erased
    replayed what survived and produced a state the layer never had,
    announced as a success. Callers that hold the journal pass it. The
    restore paths read it too: it proves that a key never changed hands
    (`restore_service._key_continuity`), the one corroboration of a key
    whose content other features share.
    """
    report = preflight_check(plan, conn=conn)
    if report.verdict == PreflightVerdict.BLOCKED:
        msg = "; ".join(report.blocking_reasons[:3])
        return RestoreReport([], {0: f"Preflight blocked: {msg}"}, plan.event_count)

    layer_issues = preflight_layer_check(plan, layer)
    if layer_issues:
        msg = "; ".join(layer_issues[:3])
        return RestoreReport([], {0: f"Layer check failed: {msg}"}, plan.event_count)

    if plan.atomicity == AtomicityPolicy.STRICT:
        return _execute_strict(plan, events_by_id, layer, trace_id=trace_id,
                               journal=conn)
    return _execute_best_effort(plan, events_by_id, layer, trace_id=trace_id,
                                journal=conn)


def build_restore_session(
    plan: RestorePlan, report: RestoreReport,
    started_at: str, finished_at: str,
) -> RestoreSession:
    """Build a RestoreSession record from execution results."""
    import uuid
    s_count = len(report.succeeded)
    f_count = len(report.failed)
    # A write that reached the file but could not be verified or reverted
    # still changed the data: such a session is partial, not failed. The
    # report names those events (`landed`); a report built without the
    # list still marks them with a trace.
    traced = {getattr(t, "restored_from_event_id", None)
              for t in report.trace_events or ()}
    landed = len(set(getattr(report, "landed", ()) or ())
                 | {eid for eid in report.failed if eid in traced})
    if f_count == 0 and s_count > 0:
        status = "completed"
    elif f_count > 0 and (s_count > 0 or landed > 0):
        status = "partial"
    elif s_count == 0 and f_count > 0:
        status = "failed"
    else:
        status = "cancelled"
    return RestoreSession(
        session_id=str(uuid.uuid4()),
        mode=plan.mode,
        scope=plan.scope,
        cutoff=plan.cutoff,
        datasource_fingerprint=plan.datasource_fingerprint,
        layer_name=plan.layer_name,
        started_at=started_at,
        finished_at=finished_at,
        succeeded_count=s_count,
        failed_count=f_count,
        total_requested=report.total_requested,
        status=status,
    )


def _execute_best_effort(
    plan: RestorePlan,
    events_by_id: Dict[int, AuditEvent],
    layer,
    trace_id: str = "",
    journal=None,
) -> RestoreReport:
    """Per-entity isolation via existing restore_batch."""
    ordered = [events_by_id[a.event_id] for a in plan.actions if a.event_id in events_by_id]
    return restore_batch(layer, ordered, trace_id=trace_id, journal=journal)


def _execute_strict(
    plan: RestorePlan,
    events_by_id: Dict[int, AuditEvent],
    layer,
    trace_id: str = "",
    journal=None,
) -> RestoreReport:
    """All-or-nothing on what the PROVIDER holds, not only on the edit buffer.

    The edit buffer makes the APPLY phase atomic: a refusal inside the loop
    is undone by destroyEditCommand(). It cannot make the COMMIT atomic:
    commitChanges() pushes geometries, attributes, deletions and additions
    stage by stage, and on a non-transactional provider (GeoPackage,
    shapefile) the first stages are already in the file when a later one
    hits a constraint. So the commit is guarded (core.commit_guard):

    * commit accepted: every touched feature is read back from the file;
      an action whose feature does not hold what was asked is reported
      FAILED_POSTCOMMIT_VERIFICATION, never counted as restored;
    * commit refused: what the refused commit wrote is found by reading the
      file back, reverted, and the revert is read back too. The report
      then names every event with its own id and outcome, and a trace is
      journalled for every write that could not be reverted.
    """
    from .commit_guard import (
        CommitGuard, EV_APPLIED_VERIFIED, EV_LANDED_NOT_COMPENSATED,
    )
    prefix = f"[{trace_id}] " if trace_id else ""
    layer_name = layer.name() if hasattr(layer, 'name') else "?"
    flog(f"{prefix}strict_execute: start layer={layer_name} "
         f"actions={len(plan.actions)}")

    all_eids = [a.event_id for a in plan.actions]
    was_editing = layer.isEditable()
    if not was_editing:
        if not layer.startEditing():
            flog(f"{prefix}strict_execute: startEditing failed", "ERROR")
            return RestoreReport(
                [], {eid: "Cannot start editing on layer" for eid in all_eids},
                plan.event_count)

    guard = CommitGuard(layer, trace_id)
    layer.beginEditCommand("RecoverLand: temporal restore")
    succeeded: List[int] = []
    failed: Dict[int, str] = {}
    # One remap for the whole plan, as the strict runner keeps one per
    # layer: a feature re-inserted in phase 0 is reached by the later
    # compensations of its entity (proven, see `_settle_update_target` and
    # `_buffer_delete`), and the dict carries the rewind session's single
    # reading of the committed state (`_rewind_baseline`).
    fid_remap: Dict = {}

    for action in plan.actions:
        event = events_by_id.get(action.event_id)
        if event is None:
            failed[action.event_id] = "Event data not found"
            break

        guard.begin_action(action.event_id)
        result = _apply_via_buffer(
            layer, action.compensatory_op, event, fid_remap,
            trace_id=trace_id, journal=journal,
        )
        guard.end_action(action.event_id, extra_fids=[result.get("fid")])
        if result["success"]:
            succeeded.append(action.event_id)
        else:
            failed[action.event_id] = result["message"]
            break

    if failed:
        flog(f"{prefix}strict_execute: rollback_started "
             f"applied={len(succeeded)} failed={len(failed)}", "WARNING")
        layer.destroyEditCommand()
        if not was_editing:
            layer.rollBack()
        flog(f"{prefix}strict_execute: rollback_done", "WARNING")
        blocker = next(iter(failed))
        for eid in all_eids:
            if eid not in failed:
                failed[eid] = (f"not applied: the atomic batch stopped on "
                               f"event {blocker}")
        return RestoreReport([], failed, plan.event_count)

    layer.endEditCommand()
    if was_editing:
        # The buffer belongs to an edit session the user opened: nothing is
        # written until HE commits. Claiming a restore (and journalling
        # traces) for writes that may still be discarded would be a lie.
        flog(f"{prefix}strict_execute: buffered_in_user_session "
             f"actions={len(succeeded)} (no commit, no trace)", "WARNING")
        return RestoreReport(
            [], {eid: "written to the open edit session, not committed"
                 for eid in succeeded}, plan.event_count)

    flog(f"{prefix}strict_execute: commit_started")
    guard.prepare()
    ok = layer.commitChanges()
    verdict = guard.finish(ok)
    # Drop any provider-side cache so the canvas and the next read show
    # what the file holds after the commit (and the compensation, if any).
    layer.reload()
    if layer.isEditable():
        layer.rollBack()

    if verdict.committed:
        flog(f"{prefix}strict_execute: commit_done verdict={verdict.state}")
        verified = [eid for eid in succeeded
                    if verdict.per_event.get(eid, EV_APPLIED_VERIFIED)
                    == EV_APPLIED_VERIFIED]
        mismatched = {eid: f"FAILED_POSTCOMMIT_VERIFICATION: "
                           f"{verdict.per_event_detail.get(eid, '')}"
                      for eid in succeeded if eid not in verified}
        # Every committed write is journalled, verified or not: the data
        # changed, and a later rewind must know it.
        traces = _build_traces_for_succeeded(succeeded, events_by_id, layer)
        flog(f"{prefix}strict_execute: completed ok={len(verified)} "
             f"mismatch={len(mismatched)} traces={len(traces)}")
        return RestoreReport(verified, mismatched, plan.event_count,
                             trace_events=tuple(traces),
                             landed=list(mismatched))

    msg = "; ".join(verdict.commit_errors) or "Commit failed"
    flog(f"{prefix}strict_execute: commit_failed verdict={verdict.state} "
         f"msg={msg}", "ERROR")
    failed = {}
    for eid in all_eids:
        status = verdict.per_event.get(eid, "FAILED_COMMIT")
        failed[eid] = f"{status}: {verdict.per_event_detail.get(eid, msg)}"
    still_written = verdict.events_with(EV_LANDED_NOT_COMPENSATED)
    traces = _build_traces_for_succeeded(still_written, events_by_id, layer)
    if still_written:
        flog(f"{prefix}strict_execute: event=partial_write_left_in_file "
             f"events={still_written} left={verdict.left_modified[:10]}: "
             f"journalled as traces", "CRITICAL")
    return RestoreReport([], failed, plan.event_count,
                         trace_events=tuple(traces),
                         landed=list(still_written))


def _build_traces_for_succeeded(
    succeeded: List[int],
    events_by_id: Dict[int, AuditEvent],
    layer,
) -> list:
    """Build trace events for successfully restored events."""
    traces = []
    for eid in succeeded:
        event = events_by_id.get(eid)
        if event is None:
            continue
        trace = build_restore_trace_event(event, layer)
        if trace is not None:
            traces.append(trace)
    return traces


def _apply_via_buffer(layer, compensatory_op: str, event: AuditEvent,
                      fid_remap: Optional[Dict] = None,
                      trace_id: str = "", journal=None) -> dict:
    """Apply a single compensatory action via the layer editing buffer.

    fid_remap: shared dict {entity_fingerprint -> new_fid} maintained across
    actions of the same atomic restore. INSERT compensatory writes into it;
    UPDATE/DELETE consult it first. This is required for FID-only identity
    layers where re-inserted features get fresh negative FIDs in the buffer
    that no longer match the historical FID stored in the event.

    BL-RW-P3-17: trace_id flows from the runner so executor logs share
    the same `[trace_id]` prefix as the rest of the rewind chain.

    The same dict also carries the batch cache of the rewind session
    (`_rewind_baseline`): pass ONE remap per layer and per atomic restore,
    never a fresh one per action, or the committed state is read again
    for every compensation.

    *journal*: a read connection to the journal the event comes from; it
    proves that a key never changed hands (`restore_service._key_continuity`).
    ``None`` (the default): a key whose content other features share stays
    refused (``key_continuity_unproven (no_journal)``).
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    layer_name = layer.name() if layer and hasattr(layer, 'name') else '?'
    flog(f"{prefix}_apply_via_buffer: comp_op={compensatory_op} "
         f"eid={event.event_id} orig_op={event.operation_type} "
         f"layer={layer_name!r} "
         f"identity={(event.feature_identity_json or '')[:80]}")
    if compensatory_op == "INSERT":
        return _buffer_insert(layer, event, fid_remap, trace_id=trace_id)
    if compensatory_op == "DELETE":
        return _buffer_delete(layer, event, fid_remap, trace_id=trace_id,
                              journal=journal)
    if compensatory_op == "UPDATE":
        return _buffer_update(layer, event, fid_remap, trace_id=trace_id,
                              journal=journal)
    return {
        "success": False,
        "status": "FAILED",
        "reason_code": "unknown_operation",
        "message": f"Unknown operation: {compensatory_op}",
    }


def _buffer_insert(layer, event: AuditEvent,
                   fid_remap: Optional[Dict] = None,
                   trace_id: str = "") -> dict:
    """Re-insert a deleted feature via editing buffer.

    Plain addFeature. No geometry-based idempotence: scanning the layer
    for an existing feature matching the snapshot caused cross-feature
    corruption (matching a freshly inserted buffer feature or an
    unrelated neighbour, then overwriting its attrs+geom with the wrong
    event's snapshot). If rewinds are repeated, users must rollback
    compensatory inserts explicitly via the strict runner, not rely on
    silent merging here.

    The captured WKB is expressed in the CRS of the capture, so it goes
    through `_geometry_in_layer_crs` before it reaches the buffer: a
    compensatory INSERT on a layer reprojected since then would otherwise
    recreate the feature thousands of kilometres away.

    v7: the same geometry guards as the event path
    (`restore_service._geometry_for_write`). A WKB the event carries but
    that does not decode (interrupted write, partial copy of the journal)
    used to be skipped silently: the feature was re-created WITHOUT
    geometry -- invisible on the map -- and counted "restauree". It is
    refused (``corrupt_wkb``), and so is a geometry of another type than
    the layer's (``geometry_type_mismatch``). A plain table ignores the
    geometric part of the event, as the event path does.

    The NULL marker (a zero-length BLOB) is the state "no geometry" only
    in an event of schema 7 or later, the capture that writes it
    (`core.constants.is_null_geometry_marker`). In an older event a
    zero-length blob is a truncated row: read as the marker, it re-created
    the feature without its geometry and reported "Inserted via buffer".
    """
    from qgis.core import QgsFeature
    from .restore_service import (
        _geometry_type_refusal, _layer_geometry_type, _is_unset_marker,
        _time_specs, _captured_from, _legacy_datetime_refusal,
    )
    from .constants import is_null_geometry_marker
    from ..compat import QgisCompat

    attrs = reconstruct_attributes(event)
    geom = None
    wkb = event.geometry_wkb
    # The NULL marker (a zero-length BLOB) is the state "no geometry", not
    # an unreadable one: the feature is re-created without one.
    if (wkb is not None
            and not is_null_geometry_marker(
                wkb, getattr(event, "event_schema_version", None))
            and _layer_geometry_type(layer) != QgisCompat.GEOM_NULL):
        geom = rebuild_geometry(wkb)
        if geom is None:
            flog(f"_buffer_insert: eid={event.event_id} status=FAILED "
                 f"reason_code=corrupt_wkb wkb_len={len(wkb)} (inserting it "
                 f"would re-create the feature without its geometry)",
                 "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "corrupt_wkb",
                "message": "Geometry unreadable (corrupt WKB); nothing inserted",
            }
        # A typed EMPTY geometry is a captured state, written as is: it
        # has no coordinate a CRS change could misplace.
        if not geom.isEmpty():
            geom, crs_error = _geometry_in_layer_crs(
                layer, geom, event, trace_id=trace_id)
            if crs_error is not None:
                return crs_error
        type_error = _geometry_type_refusal(
            layer, geom, event, event.event_id, "_buffer_insert")
        if type_error is not None:
            return dict(type_error, status="FAILED")
    mapping, dropped = _field_mapping_report(layer, event, attrs)
    fields = layer.fields()
    unset = sorted(name for name, value in (attrs or {}).items()
                   if _is_unset_marker(value))
    if unset:
        # A QgsUnsetAttributeValue repr is not a stored value: never
        # written back as text, the column keeps its database default.
        flog(f"_buffer_insert: eid={event.event_id} captured value(s) "
             f"{unset} are QgsUnsetAttributeValue reprs: left to the "
             f"database default", "WARNING")
        attrs = {name: value for name, value in attrs.items()
                 if name not in unset}
    specs = _time_specs(layer, _captured_from(event, "snapshot"))
    refusal = _legacy_datetime_refusal(mapping, attrs, fields, event, specs,
                                       "_buffer_insert")
    if refusal is not None:
        return dict(refusal, status="FAILED")

    feature = QgsFeature(fields)
    # Decoded with the event's schema version and, for a <= 5.x date-time,
    # the spec the provider hands back (`serialization.provider_time_specs`).
    for idx, value in iter_mapped_attributes(mapping, attrs, fields,
                                             event.event_schema_version,
                                             specs):
        feature.setAttribute(idx, value)

    if geom is not None:
        feature.setGeometry(geom)

    if not layer.addFeature(feature):
        flog(f"_buffer_insert: addFeature failed eid={event.event_id}", "ERROR")
        return {
            "success": False,
            "status": "FAILED",
            "reason_code": "buffer_refused",
            "message": "Buffer addFeature failed",
        }
    new_fid = feature.id()
    fp = event.entity_fingerprint or '?'
    prev = fid_remap.get(fp) if fid_remap and fp != '?' else None
    _record_remap(fid_remap, event, new_fid)
    geom_repr = feature_geom_short_repr_diag(layer, new_fid) if new_fid else '?'
    ins_fields = {
        "eid": event.event_id,
        "fp": fp,
        "buf_fid": new_fid,
        "geom": geom_repr,
    }
    if prev is not None:
        ins_fields["collision_prev"] = prev
    # BL-RW-P3-17: include trace_id so the structured BUF_INS log
    # correlates with the rest of the rewind chain.
    if trace_id:
        ins_fields["trace_id"] = trace_id
    flog_kv("INFO", "BUF_INS", module="restore_executor", **ins_fields)
    # BL-RW-P2-14: tag the success path explicitly so _classify_restore_result
    # doesn't have to fall back to message parsing.
    # RLU-053: a feature re-inserted without the fields the schema lost is
    # not the feature that was deleted; say which ones stayed behind.
    return _buffer_outcome(
        dropped, "Inserted via buffer", event.event_id, fid=new_fid)


def _record_remap(fid_remap: Optional[Dict], event: AuditEvent,
                  new_fid: Optional[int]) -> None:
    """Remember the buffer FID *event*'s re-insertion got, and the event.

    The event (the DELETE-like row compensated) is what the key history of
    a later UPDATE of the same fingerprint is proven against
    (`_remap_update_proof`), under `_REMAP_SOURCE_SLOT`.
    """
    if fid_remap is None or new_fid is None:
        return
    key = event.entity_fingerprint
    if key:
        fid_remap[key] = new_fid
        sources = fid_remap.get(_REMAP_SOURCE_SLOT)
        if not isinstance(sources, dict):
            sources = {}
            fid_remap[_REMAP_SOURCE_SLOT] = sources
        sources[key] = event


def _resolve_target_fid(layer, event: AuditEvent,
                        fid_remap: Optional[Dict]) -> Optional[int]:
    """Return remapped FID for entity if known, else None.

    The remap is built within a single restore session: when a Phase 0
    compensatory INSERT re-creates a feature, it stores the resulting
    buffer FID in fid_remap[entity_fingerprint]. Subsequent Phase 1
    UPDATE / Phase 2 DELETE actions on the same entity must consult the
    remap to reach the freshly-allocated FID, otherwise they look up the
    historical FID, miss, and silently skip.

    Both 'pk:field=value' and 'fid:N' keys are honoured. The dict is
    built fresh for each per-layer restore session in StrictRestoreRunner
    (see _strict_fid_remap reset in begin_strict_for_layer), so stale
    cross-session entries are not a concern.

    The answer is a NOMINATION, never a proof: two events sharing an
    entity_fingerprint may name two entities (``fid:N`` is a position a
    shapefile repack hands to another feature, a key is given again after
    a delete). The UPDATE proves it (`_remap_update_proof`), the DELETE
    too (`restore_service._classify_insert_candidate`, via ``remap``).
    """
    if fid_remap is None:
        return None
    key = event.entity_fingerprint
    if not key:
        flog(f"REMAP_LOOKUP eid={event.event_id} fp=none→None", "DEBUG")
        return None
    result = fid_remap.get(key)
    from itertools import islice
    remap_keys = list(islice((k for k in fid_remap if isinstance(k, str)), 6))
    size = sum(1 for k in fid_remap if isinstance(k, str))
    flog(f"REMAP_LOOKUP eid={event.event_id} fp={key} "
         f"result={result} remap_size={size} remap_keys={remap_keys}", "DEBUG")
    return result


def _buffer_delete(layer, event: AuditEvent,
                   fid_remap: Optional[Dict] = None,
                   trace_id: str = "", journal=None) -> dict:
    """Delete an inserted feature via editing buffer.

    Safety: FIDs are reused across commits by some providers (GPKG/OGR),
    keys get reused, and a committed delete is not taken back. The target
    is resolved by `restore_service._resolve_insert_target` -- the same
    resolver as the event restore and the undo -- and only a STRONG or
    MEDIUM target is deleted (identity levels: see restore_service).

    Outcomes (v7):
      * STRONG / MEDIUM target: deleted, APPLIED;
      * every read completed and the inserted feature is not there: for an
        INSERT source, SKIPPED_IDEMPOTENT (``absent_vs_insert_comp`` when
        nothing was designated, ``insert_target_gone`` when the designated
        feature is unrelated and spared); any other source op has no
        idempotence proof: FAILED ``target_absent``;
      * a candidate sharing only PART of the snapshot: FAILED
        ``target_unverifiable``, nothing deleted. This used to delete the
        occupant of the historical FID as soon as it shared ONE value with
        the snapshot, to accept a same-outline candidate differing on its
        attributes, and to trust a primary key without a look at the
        feature;
      * several candidates: FAILED ``target_ambiguous`` (was: max(fid));
      * a read that failed: FAILED ``scan_failed``, a hard failure. The old
        scanners swallowed the exception, and the silence of a dropped
        connection was reported "already absent" for every compensation.

    A key nomination measures its rarity on the COMMITTED state (see
    `_rewind_baseline`), not on the buffer this rewind is filling.

    Features this rewind re-inserted (the remap values) (v7):
      * a remap entry under the SAME fingerprint, re-inserted a moment ago
        in this buffer, is proven against the INSERT snapshot like any
        nominee (`_classify_insert_candidate`): proven, it is the entity
        the plan re-creates and then deletes, and it is deleted -- it used
        to be discarded unseen, so the delete found nothing, answered
        "already absent", and the commit kept the re-created feature.
        Unproven, the FID was reused by another entity (GPKG/OGR): the
        remap is ignored and the identity resolves the target, the defense
        that rule was for;
      * the snapshot scans leave them out (their content is the OLD
        snapshot of a DELETE this rewind compensates), but when the
        completed reads find the inserted feature NOWHERE ELSE and one of
        them carries the whole INSERT snapshot, "already absent" would be
        false: the feature is in the buffer, re-created by this rewind,
        under another key (a renumbered shapefile names one entity fid:3
        and fid:1). FAILED ``target_reinserted_in_batch``, a hard failure
        the strict runner rolls back: committing around it would write a
        resurrection the rewind cannot justify, reported as a success.
    """
    from .restore_service import (
        _resolve_insert_target, _diagnose_snapshot_miss, ID_AMBIGUOUS,
        _classify_insert_candidate, TargetResolution, TargetScanError,
        WRITABLE_LEVELS,
    )

    fp = event.entity_fingerprint or '?'
    eid = event.event_id
    baseline = _rewind_baseline(journal, fid_remap)
    reinserted = _remapped_fids(fid_remap)
    remapped = _resolve_target_fid(layer, event, fid_remap)
    res = None
    if remapped is not None and remapped < 0:
        try:
            level, why = _classify_insert_candidate(
                layer, remapped, event, "remap", baseline)
        except TargetScanError as exc:
            level, why = None, f"scan_failed: {exc}"
        if level in WRITABLE_LEVELS:
            res = TargetResolution(remapped, level, why, via="remap",
                                   nominated=remapped)
            flog(f"BUF_DEL eid={eid} fp={fp} remap={remapped} proven against "
                 f"the INSERT snapshot identity_level={level} ({why}): the "
                 f"entity this rewind re-created is deleted again")
        else:
            flog(f"BUF_DEL eid={eid} fp={fp} remap={remapped} not proven "
                 f"against the INSERT snapshot (identity_level={level} "
                 f"{why}): FID-reuse defense, resolved from the identity",
                 "WARNING")
            remapped = None
    if res is None:
        res = _resolve_insert_target(
            layer, event, exclude_fids=reinserted or None, remapped=remapped,
            context=f"BUF_DEL eid={eid} fp={fp}", baseline=baseline)

    if not res.writable and res.absent and reinserted:
        try:
            twin = _reinserted_twin(layer, event, reinserted)
        except TargetScanError as exc:
            flog(f"BUF_DEL eid={eid} fp={fp} status=FAILED "
                 f"reason_code=scan_failed detail={exc} (re-inserted twin "
                 f"scan)", "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "scan_failed",
                "identity_level": res.level,
                "message": (f"Failed: layer read failed ({exc}); nothing "
                            f"deleted"),
            }
        if twin is not None:
            flog(f"BUF_DEL eid={eid} fp={fp} status=FAILED "
                 f"reason_code=target_reinserted_in_batch buffer_fid={twin} "
                 f"detail={res.reason!r} (the only feature carrying the "
                 f"INSERT snapshot was re-inserted by this rewind: 'already "
                 f"absent' would be false)", "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "target_reinserted_in_batch",
                "identity_level": res.level,
                "message": (
                    f"Failed: the only feature carrying the inserted "
                    f"snapshot is one this rewind re-inserted a moment ago "
                    f"(buffer fid {twin}); whether it is this entity under "
                    f"another key cannot be proven, nothing deleted"),
            }

    buf = layer.editBuffer()
    pending = set(buf.deletedFeatureIds() or ()) if buf is not None else set()
    # Only when the completed reads found the inserted feature nowhere
    # else (`res.absent`): the designated feature is then the one the
    # buffer already deletes. A WEAK or AMBIGUOUS resolution whose nominee
    # happens to be pending is still unproven -- two identical candidates
    # stand in the layer, none deleted -- and used to answer success here.
    pending_fid = res.nominated if res.absent else None
    if pending_fid is not None and pending_fid in pending:
        # BL-RW-P2-14: an already-pending delete is a proven idempotent
        # result. The buffer holds the deletion; the net effect equals
        # the desired final state, so tag it as SKIPPED_IDEMPOTENT.
        flog(f"BUF_DEL eid={eid} fp={fp} fid={pending_fid} "
             f"already_pending_delete=>SKIPPED_IDEMPOTENT (RW-19b: net "
             f"result correct, FID recycled by OGR)", "WARNING")
        return {
            "success": True,
            "skipped": True,
            "status": "SKIPPED_IDEMPOTENT",
            "reason_code": "already_pending_delete",
            "message": "Already pending delete (OGR FID recycle)",
        }

    if not res.writable:
        # Only an absence established by COMPLETED reads is a skip, and
        # only for an INSERT source (undoing an INSERT means "this feature
        # must not be in the layer", a state already reached). Everything
        # else is a named failure that leaves the buffer untouched.
        if res.reason.startswith("scan_failed"):
            flog(f"BUF_DEL eid={eid} fp={fp} status=FAILED "
                 f"reason_code=scan_failed "
                 f"detail={res.reason!r} (absence cannot be inferred from a "
                 f"failed read)", "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "scan_failed",
                "identity_level": res.level,
                "message": (f"Failed: layer read failed ({res.reason}); "
                            f"nothing deleted"),
            }
        if res.absent and event.operation_type == 'INSERT':
            if res.nominated is None:
                flog(f"BUF_DEL eid={eid} fp={fp} target=none "
                     f"status=SKIPPED_IDEMPOTENT reason_code=absent_vs_insert_comp "
                     f"detail={res.reason!r} (undo of user INSERT already applied)",
                     "INFO")
                return {
                    "success": True, "skipped": True,
                    "status": "SKIPPED_IDEMPOTENT",
                    "reason_code": "absent_vs_insert_comp",
                    "identity_level": res.level,
                    "message": "Skipped (idempotent): feature already absent",
                }
            # The designated feature shares nothing with the snapshot and the
            # scans that completed found the inserted feature nowhere: it is no
            # longer in the layer, and the stranger now holding its FID or key
            # is left untouched.
            flog(f"BUF_DEL eid={eid} fp={fp} fid={res.nominated} "
                 f"status=SKIPPED_ALREADY_ABSENT reason_code=insert_target_gone "
                 f"detail={res.reason!r} (occupant shares no evidence, layer "
                 f"swept: the inserted feature is not there)", "WARNING")
            return {
                "success": True,
                "skipped": True,
                "status": "SKIPPED_ALREADY_ABSENT",
                "reason_code": "insert_target_gone",
                "identity_level": res.level,
                "message": (
                    "Skipped: the inserted feature is no longer in "
                    "the layer; its former FID now holds an unrelated "
                    "feature, left untouched"
                ),
            }
        if res.absent:
            # BL-RW-P0-02: for any other source op, target absence is a hard
            # failure: we cannot prove the user state matches the desired one.
            flog(f"BUF_DEL eid={eid} fp={fp} target=none status=FAILED "
                 f"reason_code=target_absent op={event.operation_type}", "WARNING")
            _diagnose_snapshot_miss(layer, event)
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "target_absent",
                "identity_level": res.level,
                "message": "Failed: target feature absent",
            }
        code = ("target_ambiguous" if res.level == ID_AMBIGUOUS
                else "target_unverifiable")
        flog(f"BUF_DEL eid={eid} fp={fp} nominated={res.nominated} status=FAILED "
             f"reason_code={code} identity_level={res.level} "
             f"detail={res.reason!r} (refusing to delete an unproven feature)",
             "WARNING")
        _diagnose_snapshot_miss(layer, event)
        return {
            "success": False,
            "status": "FAILED",
            "reason_code": code,
            "identity_level": res.level,
            "message": (f"Failed: target could not be proven "
                        f"(identity_level={res.level}: {res.reason}); "
                        f"nothing deleted"),
        }

    target_fid = res.fid
    if not layer.deleteFeature(target_fid):
        # BL-RW-P0-02: buffer refusal is a hard failure -> FAILED.
        # We did not apply, did not prove idempotence; downgrade to FAIL.
        flog(
            f"BUF_DEL eid={eid} fp={fp} fid={target_fid} "
            f"status=FAILED reason_code=buffer_refused",
            "WARNING",
        )
        return {
            "success": False,
            "status": "FAILED",
            "reason_code": "buffer_refused",
            "message": "Failed: buffer deleteFeature refused",
        }
    # BL-RW-P3-17: include trace_id on the structured BUF_DEL log.
    _del_fields = dict(
        eid=eid, fp=fp, fid=target_fid, status="deleted",
        identity_level=res.level, via=res.via,
    )
    if trace_id:
        _del_fields["trace_id"] = trace_id
    flog_kv("INFO", "BUF_DEL", module="restore_executor", **_del_fields)
    # BL-RW-P2-14: tag the success path explicitly.
    return {
        "success": True,
        "status": "APPLIED",
        "reason_code": "applied",
        "identity_level": res.level,
        "message": "Deleted via buffer",
    }


def _reinserted_twin(layer, event: AuditEvent, reinserted: set) -> Optional[int]:
    """FID of a feature this rewind re-inserted carrying *event*'s snapshot.

    The WHOLE INSERT snapshot, the proof `_resolve_insert_target` applies
    (geometry in the layer CRS AND every comparable value, NULL-aware),
    read through the layer so that the buffer's added features are seen.
    None when none does, or when the snapshot has nothing to compare.
    Raises TargetScanError.
    """
    from qgis.core import QgsFeatureRequest
    from .restore_service import (
        _comparable_fields, _from_edit_buffer, _expected_geometry, _evidence,
        _event_version, _has_shape, _scan,
    )

    attrs = reconstruct_attributes(event)
    if not isinstance(attrs, dict):
        attrs = {}
    comparable = _comparable_fields(layer, attrs,
                                    inserted=_from_edit_buffer(event))
    expected = _expected_geometry(event.geometry_wkb, event, layer)
    if not comparable and expected is None:
        return None
    request = QgsFeatureRequest()
    if _has_shape(expected):
        request.setFilterRect(expected.boundingBox())
    else:
        request.setFilterFids(sorted(reinserted))
    version = _event_version(event)
    for feature in _scan(layer, request,
                         f"BUF_DEL eid={event.event_id} re-inserted twin scan"):
        if feature.id() not in reinserted:
            continue
        if _evidence(feature, attrs, comparable, expected,
                     version=version).full:
            return feature.id()
    return None


def _geometry_in_layer_crs(layer, geom, event: AuditEvent,
                           trace_id: str = "") -> tuple:
    """Express *geom* in the CRS of *layer*, or refuse to write it.

    Returns ``(geom, None)`` when the geometry may reach the buffer, and
    ``(None, refusal)`` when it may not -- *refusal* being a
    ready-to-return result dict.

    The journal stores the WKB verbatim: the RAW provider coordinates at
    capture time, while ``event.crs_authid`` is only the CRS the layer
    DECLARED then. When the layer declares another CRS now, the difference
    is either a relabel (Set Layer CRS, a corrected .prj: the raw
    coordinates must be written as they are) or a reprojection of the file
    in place (they must be reprojected). Either wrong guess teleports the
    feature while the rewind reports a success (I-3), so
    `geometry_for_layer_crs` decides from evidence and refuses when the
    evidence cannot tell (reason_code ``crs_ambiguous``) or when the
    capture CRS is unusable here (``crs_mismatch``).

    The event's other geometries go along as witnesses: the NEW geometry of
    an UPDATE is the one its target still carries, so the target tells the
    story for the OLD one written here -- the story its identity proof
    (`restore_service._expected_geometry`, same helper) was checked under.
    Refusal is read on the STATUS: with ``crs_ambiguous`` the helper hands
    a reading meant for comparisons, never for the buffer.

    Same helper as the event-by-event path
    (`restore_service._geometry_for_write`), so both engines take the same
    decision for the same geometry and word a refusal the same way. A
    capture made in the CRS the layer still has costs nothing:
    `geometry_for_layer_crs` returns the geometry untouched, byte for byte.
    """
    from .geometry_utils import (
        CRS_MISMATCH_STATUSES, CRS_REFUSED_STATUSES, crs_refusal,
    )

    aligned, crs_status = geometry_for_layer_crs(
        geom, event.crs_authid, layer, trace_id=trace_id,
        witnesses=(event.geometry_wkb,
                   getattr(event, "new_geometry_wkb", None)))
    if crs_status in CRS_MISMATCH_STATUSES:
        flog(f"BUF_CRS eid={event.event_id} crs_mismatch "
             f"event_crs={event.crs_authid} "
             f"layer_crs={extract_crs_authid(layer)} action={crs_status}",
             "WARNING")
    if aligned is None or crs_status in CRS_REFUSED_STATUSES:
        reason_code, message = crs_refusal(crs_status, event.crs_authid, layer)
        flog(f"BUF_CRS eid={event.event_id} reason_code={reason_code} "
             f"status=refused (nothing written)", "ERROR")
        return None, {
            "success": False,
            "status": "FAILED",
            "reason_code": reason_code,
            "message": message,
        }
    return aligned, None


def _settle_update_target(layer, event: AuditEvent,
                          fid_remap: Optional[Dict],
                          mapping: dict, journal=None) -> tuple:
    """Decide which feature this UPDATE is allowed to write to.

    Returns ``(fid, refusal)``: *refusal* is None once a target is
    settled, otherwise it is a ready-to-return result dict and nothing
    must be written.

    One resolver for every path (`restore_service._resolve_update_target`,
    side ``new``): the identity nominates, the NEW state proves, a FID
    identity may be relocated by a discriminating post-state scan, a key
    never is. Then, in order:

      * proven (STRONG / MEDIUM): write;
      * the read failed: ``scan_failed``, a hard failure -- absence cannot
        be inferred from a failed read;
      * the nominee already carries the OLD state, proven (BL-RW-P5-28):
        ``already_at_old`` skip, writing OLD over OLD changes nothing;
      * several candidates: ``target_ambiguous``;
      * nothing nominated, nothing relocated: ``target_absent``;
      * otherwise: ``target_unverifiable``.

    A remap entry (the feature this same rewind re-created a moment ago
    under the same fingerprint) is a nomination like the others, proven
    by `_remap_update_proof`: it must carry the WHOLE NEW state of the
    event (every changed value and the NEW geometry, aligned on the layer
    CRS), and that state must designate it -- no other feature of the
    layer carries it, or, for a key, the journal proves the key named one
    entity from the edit to the DELETE this rewind re-created. It used to
    be taken on its NEW geometry alone, compared raw: an attribute-only
    edit was written on it unseen (on a repacked shapefile, fid:1 of the
    edit was C003 and fid:1 of the delete C001: C003's old values landed
    on C001), and on a layer reprojected since the capture the raw
    comparison never matched, the remap was dropped and the edit refused
    "target_absent" while the re-created feature was committed in its
    post-edit state. Then on the NEW values without asking whether they
    discriminate: C001 re-created with the name 'un' received C003's old
    name because C003's edit had also produced 'un' (a status, a flag, a
    category: any column with few values), both results "APPLIED".

    A remap entry that carries the NEW state without it designating the
    entry is a candidate nothing can rule out: the refusal is named
    (``target_unverifiable``) and NOTHING is written, neither on it nor on
    the feature the resolver would pick instead -- that one carries the
    same state, and on a key it may even look proven (a rare value at the
    committed state, where the re-created feature is not counted: M6 of
    rs_remap_update_proof).

    A key nomination measures its rarity on the COMMITTED state
    (`_rewind_baseline`): the buffer holds this rewind's own work, and
    counted it made a proof depend on the order of the compensations.
    Leaving the written features out WHOLE (the previous rule) did too: a
    compensation that wrote the code of feature 5 dropped its name V, the
    user's, and V became "unique" on the reused key 3 -- INTRUS renamed,
    APPLIED; a compensation that deleted D (name V) did the same.

    What this replaces trusted a primary key on sight
    (``trusted = remapped is not None or has_pk``): a key reused by another
    feature received the rewind -- INTRUS renamed ORIG_OLD, "APPLIED".
    And its relocation helpers answered None for "read failed", "several
    candidates" and "not found" alike, so a dropped connection was a soft
    ``target_absent`` the strict runner committed around, and two stacked
    candidates read "target feature absent".
    """
    from .restore_service import (
        _event_identity, _prove_update_state, _resolve_update_target,
        _target_already_at_old, ID_AMBIGUOUS, ID_UNRESOLVABLE, ID_WEAK,
        WRITABLE_LEVELS,
    )

    eid = event.event_id
    fp = event.entity_fingerprint or '?'
    remapped = _resolve_target_fid(layer, event, fid_remap)
    if remapped is not None:
        verdict, why = _remap_update_proof(layer, remapped, event, mapping,
                                           fid_remap, journal)
        if verdict == "proven":
            flog(f"_buffer_update: using fid_remap eid={eid} fid={remapped} "
                 f"(carries the post-edit state and it designates it: "
                 f"{why})")
            return remapped, None
        if verdict == "scan_failed":
            flog(f"BUF_UPD eid={eid} fp={fp} remap={remapped} status=FAILED "
                 f"reason_code=scan_failed detail={why!r}", "ERROR")
            return None, {
                "success": False,
                "status": "FAILED",
                "reason_code": "scan_failed",
                "identity_level": ID_UNRESOLVABLE,
                "message": (f"Failed: layer read failed ({why}); nothing "
                            f"written"),
            }
        if verdict == "shared":
            flog(f"REMAP_GUARD eid={eid} fp={fp} remap={remapped} shared "
                 f"({why}) status=refused reason_code=target_unverifiable "
                 f"(the post-edit state does not designate the re-created "
                 f"feature; nothing written, on it or elsewhere)", "WARNING")
            return None, {
                "success": False,
                "status": "FAILED",
                "reason_code": "target_unverifiable",
                "identity_level": ID_WEAK,
                "message": (
                    f"Target feature could not be verified: the feature "
                    f"this rewind re-created under this identity (buffer "
                    f"fid {remapped}) carries the post-edit state, but "
                    f"that state does not designate it ({why}); refusing "
                    f"to write on it or on another feature carrying the "
                    f"same state, nothing written"),
            }
        flog(f"REMAP_GUARD eid={eid} fp={fp} remap={remapped}→discarded "
             f"({why}: it does not carry the post-edit state)", "WARNING")

    identity = _event_identity(layer, event)
    baseline = _rewind_baseline(journal, fid_remap)
    relocated_map = (fid_remap.get(_RELOCATED_SLOT)
                     if isinstance(fid_remap, dict) else None)
    relocated_fid = (relocated_map.get(fp)
                     if isinstance(relocated_map, dict) and fp != "?" else None)
    if relocated_fid is not None:
        r_level, r_why = _prove_update_state(
            layer, relocated_fid, event, "new", mapping, "fid", baseline)
        if r_level in WRITABLE_LEVELS:
            flog(f"_buffer_update: using relocated_fid eid={eid} fp={fp} "
                 f"fid={relocated_fid} ({r_why})")
            return relocated_fid, None
        if r_why.startswith("scan_failed"):
            return None, {
                "success": False,
                "status": "FAILED",
                "reason_code": "scan_failed",
                "identity_level": ID_UNRESOLVABLE,
                "message": (f"Failed: layer read failed ({r_why}); nothing "
                            f"written"),
            }
        relocated_map.pop(fp, None)

    res = _resolve_update_target(layer, event, identity, mapping, "new",
                                 relocate=True,
                                 context=f"BUF_UPD eid={eid} fp={fp}",
                                 baseline=baseline)
    if res.writable:
        if isinstance(fid_remap, dict) and fp != "?" and res.fid is not None:
            fid_remap.setdefault(_RELOCATED_SLOT, {})[fp] = res.fid
        return res.fid, None

    if res.reason.startswith("scan_failed"):
        flog(f"BUF_UPD eid={eid} fp={fp} status=FAILED reason_code=scan_failed "
             f"detail={res.reason!r} (absence cannot be inferred from a "
             f"failed read)", "ERROR")
        return None, {
            "success": False,
            "status": "FAILED",
            "reason_code": "scan_failed",
            "identity_level": res.level,
            "message": (f"Failed: layer read failed ({res.reason}); "
                        f"nothing written"),
        }
    if res.nominated is not None and _target_already_at_old(
            layer, res.nominated, event, mapping, baseline):
        flog(f"BUF_UPD eid={eid} fid={res.nominated} "
             f"already_at_old status=SKIPPED_IDEMPOTENT")
        return None, {
            "success": True,
            "skipped": True,
            "status": "SKIPPED_IDEMPOTENT",
            "reason_code": "already_at_old",
            "message": "Already at the pre-edit state; nothing to do",
        }
    if res.level == ID_AMBIGUOUS:
        flog(f"BUF_UPD eid={eid} fp={fp} status=FAILED "
             f"reason_code=target_ambiguous detail={res.reason!r}", "ERROR")
        return None, {
            "success": False,
            "status": "FAILED",
            "reason_code": "target_ambiguous",
            "identity_level": res.level,
            "message": (f"Failed: target is ambiguous ({res.reason}); "
                        f"refusing to pick one, nothing written"),
        }
    if res.absent:
        # BL-RW-P0-02: UPDATE on an absent feature CANNOT be proven
        # idempotent (the desired final state involves a feature that no
        # longer exists).
        flog(f"_buffer_update: status=FAILED reason_code=target_absent "
             f"eid={eid} identity_keys={list(identity.keys())}", "WARNING")
        return None, {
            "success": False,
            "status": "FAILED",
            "reason_code": "target_absent",
            "identity_level": res.level,
            "message": "Failed: target feature absent",
        }
    flog(f"BUF_UPD eid={eid} fid={res.nominated} unverified "
         f"reason={res.reason} identity_level={res.level} status=refused "
         f"(the identity may point at an unrelated feature)", "ERROR")
    return None, {
        "success": False,
        "status": "FAILED",
        "reason_code": "target_unverifiable",
        "identity_level": res.level,
        "message": (
            "Target feature could not be verified "
            f"({res.reason} identity_level={res.level}); refusing to "
            f"overwrite it"
        ),
    }


# fid_remap slot of the rewind session's batch cache (a 1-tuple: never an
# entity fingerprint, which is always a string).
_SESSION_SLOT = ("__rewind_session__",)
# fid_remap slot of {entity fingerprint: the DELETE-like row whose
# re-insertion created the remap entry} (`_record_remap`).
_REMAP_SOURCE_SLOT = ("__remap_source__",)
# fid_remap slot of {entity fingerprint: live FID a previous UPDATE of the
# same chain resolved/relocated onto}.
_RELOCATED_SLOT = ("__relocated_fids__",)


def _rewind_baseline(journal=None, fid_remap: Optional[Dict] = None):
    """Where a key nomination of the rewind reads the layer the user left.

    The rewind writes in the edit buffer, and the runners refuse a layer
    with uncommitted user edits before they open it (restore_runner's
    layer preflight): the PROVIDER still holds the committed state, which
    is the baseline, compensations already applied or not. *journal*
    carries the key continuity evidence (`restore_service._key_continuity`).

    *fid_remap* is the dict one atomic restore shares across its actions
    (the strict runner's, `_execute_strict`'s): it also carries the batch
    cache of that rewind session (`_SESSION_SLOT`), so the committed state
    is read ONCE per session (`restore_service._committed_index`) and the
    key continuity verdicts are kept, instead of both being rebuilt for
    every compensation -- one read of the whole layer per compensation
    made a mass rewind quadratic (item 4, rw_rewind_mass_exec_bench).
    Without a remap (a lone call) the cache lives for this call only.
    """
    from .restore_service import _Baseline, _operation_cache, _rewind_session
    session = None
    if isinstance(fid_remap, dict):
        session = fid_remap.get(_SESSION_SLOT)
        if not isinstance(session, dict):
            session = _rewind_session()
            fid_remap[_SESSION_SLOT] = session
    return _Baseline(provider_only=True,
                     cache=_operation_cache(session, journal))


def _remapped_fids(fid_remap: Optional[Dict]) -> set:
    """FIDs this rewind gave to the features it re-inserted (remap values)."""
    if not fid_remap:
        return set()
    return {fid for key, fid in fid_remap.items()
            if isinstance(key, str) and isinstance(fid, int)}


def _storability_refusal(layer, geom, event: AuditEvent, target_fid):
    """Refuse a geometry the layer cannot store without losing part of it.

    Returns ``(refusal_or_None, geom_to_write)``. The provider does not
    refuse what it cannot store, it degrades it at commit: a single-part
    layer keeps the first part of a multi-part geometry, a linear layer
    linearises a curve, a 2D layer drops Z. Those are predictable from the
    two types, so they are refused here with a precise reason, leaving the
    feature untouched. What cannot be predicted is caught by the
    post-commit read-back.

    A multi-part geometry with ONE part going into a single-part layer is
    converted, which loses nothing.
    """
    from qgis.core import QgsWkbTypes, QgsGeometry
    try:
        layer_type = layer.wkbType()
        geom_type = geom.wkbType()
        if geom.isEmpty():
            # Nothing to lose: no part, no curve, no Z or M value.
            return None, geom
    except (AttributeError, RuntimeError):
        return None, geom
    try:
        if not layer.isSpatial() or QgsWkbTypes.displayString(layer_type) in (
                "Unknown", "NoGeometry"):
            return None, geom
    except (AttributeError, RuntimeError):
        return None, geom
    reason = None
    if not QgsWkbTypes.isMultiType(layer_type) and QgsWkbTypes.isMultiType(geom_type):
        try:
            parts = geom.asGeometryCollection()
        except (AttributeError, RuntimeError, TypeError):
            parts = []
        if len(parts) == 1:
            single = QgsGeometry(parts[0])
            return None, single
        reason = (f"multi-part geometry ({len(parts)} parts) into a "
                  f"single-part layer: the provider would keep only one part")
    elif QgsWkbTypes.isCurvedType(geom_type) and not QgsWkbTypes.isCurvedType(layer_type):
        reason = "curved geometry into a linear layer: it would be linearised"
    elif QgsWkbTypes.hasZ(geom_type) and not QgsWkbTypes.hasZ(layer_type):
        reason = "geometry with Z into a 2D layer: Z would be dropped"
    elif QgsWkbTypes.hasM(geom_type) and not QgsWkbTypes.hasM(layer_type):
        reason = "geometry with M into a layer without M: M would be dropped"
    if reason is None:
        return None, geom
    flog(f"geometry_write: eid={event.event_id} fid={target_fid} "
         f"status=SKIPPED_GEOMETRY_DRIFT reason_code=geometry_not_storable "
         f"detail={reason!r} layer_type={QgsWkbTypes.displayString(layer_type)} "
         f"geom_type={QgsWkbTypes.displayString(geom_type)}", "WARNING")
    return {
        "success": True,
        "skipped": True,
        "status": "SKIPPED_GEOMETRY_DRIFT",
        "reason_code": "geometry_drift",
        "message": (f"Skipped: {reason}; feature left untouched (neither "
                    f"geometry nor attributes were written)"),
    }, geom


def _buffer_update(layer, event: AuditEvent,
                   fid_remap: Optional[Dict] = None,
                   trace_id: str = "", journal=None) -> dict:
    """Revert attributes and geometry via editing buffer.

    Target resolution lives in `_settle_update_target`, geometry alignment
    in `_geometry_in_layer_crs`; what stays here is the write itself,
    geometry first and attributes last.
    """
    old_attrs = reconstruct_attributes(event)
    mapping, dropped = _field_mapping_report(layer, event, old_attrs)

    target_fid, refusal = _settle_update_target(
        layer, event, fid_remap, mapping, journal)
    if refusal is not None:
        return refusal

    fields = layer.fields()
    # A <= 5.x date-time whose provider spec cannot be established is
    # refused HERE, before the geometry: nothing of the event is written.
    from .restore_service import (
        _time_specs, _captured_from, _legacy_datetime_refusal,
    )
    specs = _time_specs(layer, _captured_from(event, "old"))
    refusal = _legacy_datetime_refusal(
        mapping, old_attrs if isinstance(old_attrs, dict) else {}, fields,
        event, specs, "_buffer_update")
    if refusal is not None:
        return dict(refusal, status="FAILED")

    geom_status = "no_geom_in_event"
    from .constants import is_null_geometry_marker
    if is_null_geometry_marker(event.geometry_wkb,
                               getattr(event, "event_schema_version", None)):
        # The feature had NO geometry before the edit: restoring means
        # removing the one it gained, not leaving it and reporting success.
        # Only a schema 7 event can say so: in an older one an empty BLOB
        # is corruption and falls through to the corrupt_wkb refusal.
        from qgis.core import QgsGeometry
        if not layer.changeGeometry(target_fid, QgsGeometry()):
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "buffer_refused",
                "message": "Removing the geometry was rejected by the buffer",
            }
        geom_status = "set_null"
    elif event.geometry_wkb is not None:
        wkb_len = len(event.geometry_wkb)
        geom = rebuild_geometry(event.geometry_wkb)
        if geom is None:
            geom_status = f"rebuild_failed wkb_len={wkb_len}"
            flog(f"_buffer_update: rebuild_geometry returned None "
                 f"eid={event.event_id} fid={target_fid} wkb_len={wkb_len}",
                 "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "corrupt_wkb",
                "message": "Geometry rebuild failed (corrupt WKB)",
            }
        # An EMPTY geometry (typed, zero coordinates) is a captured state
        # like any other: `rebuild_geometry` already turns an undecodable
        # blob into None. Refusing it here left the feature with the shape
        # it gained after T while the rewind could not give back the empty
        # one it had.
        if geom.isNull():
            geom_status = "rebuilt_null"
            flog(f"_buffer_update: rebuilt geometry null "
                 f"eid={event.event_id} fid={target_fid}", "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "corrupt_wkb",
                "message": "Geometry rebuild failed (null after decode)",
            }
        geom, crs_error = _geometry_in_layer_crs(
            layer, geom, event, trace_id=trace_id)
        if crs_error is not None:
            return crs_error
        storable_refusal, geom = _storability_refusal(
            layer, geom, event, target_fid)
        if storable_refusal is not None:
            return storable_refusal
        # A GEOS-invalid geometry is written AS CAPTURED. Restoring means
        # giving the feature back the shape it had, and the user's data is
        # invalid at the source (third-party exports: 108 self-intersections
        # on one polygon of zone_mkt_rip). makeValid() does not preserve a
        # shape, it replaces it: a bowtie becomes two triangles (area 0 ->
        # 50), overlapping parts are merged. It used to be applied here,
        # then refused on drift for want of a post-commit read-back; that
        # read-back now exists (core.commit_guard), so what the provider
        # really stores is checked after the commit instead of guessed
        # before it.
        try:
            geos_valid = bool(geom.isGeosValid())
        except (AttributeError, RuntimeError):
            geos_valid = True
        if not geos_valid:
            flog(f"geometry_write: eid={event.event_id} fid={target_fid} "
                 f"geos_valid=False action=written_as_captured "
                 f"(no repair; verified after commit)", "WARNING")
            if heavy_diag_enabled():
                # Diagnostic only: how far a repair WOULD have moved it.
                repaired = geom.makeValid()
                if repaired and not repaired.isEmpty():
                    h_before, h_after, drift_units = _compute_makevalid_drift(
                        geom, repaired)
                    flog(f"makevalid_drift: eid={event.event_id} "
                         f"fid={target_fid} wkb_hash_before={h_before} "
                         f"wkb_hash_after={h_after} "
                         f"drift_units={drift_units:.9f} "
                         f"tolerance={MAKEVALID_DRIFT_TOLERANCE:.9f} "
                         f"status=NOT_APPLIED_DIAGNOSTIC")

        # TRACE_RESTORE: forensic snapshot of the buffer state around the
        # changeGeometry call. Costs 1 provider getFeatures and 3 WKB
        # serializations per UPDATE event; gated to keep the rewind
        # responsive (~10-50 ms per event saved on shapefile providers).
        if heavy_diag_enabled():
            before_apply = feature_geom_short_repr_diag(layer, target_fid)
            old_target = wkb_short_repr(event.geometry_wkb)
            identical = _current_geom_matches(layer, target_fid, geom)
        else:
            before_apply = "diag_off"
            old_target = "diag_off"
            identical = False

        change_ok = layer.changeGeometry(target_fid, geom)

        if heavy_diag_enabled():
            after_apply = feature_geom_short_repr_diag(layer, target_fid)
            flog(f"TRACE_RESTORE: eid={event.event_id} fid={target_fid} "
                 f"before_apply={before_apply} "
                 f"target_OLD={old_target} "
                 f"changeGeometry={change_ok} "
                 f"after_apply={after_apply} "
                 f"identical={identical}")

        if not change_ok:
            geom_status = "changeGeometry_rejected"
            flog(f"_buffer_update: changeGeometry REJECTED "
                 f"eid={event.event_id} fid={target_fid} wkb_len={wkb_len}",
                 "ERROR")
            return {
                "success": False,
                "status": "FAILED",
                "reason_code": "buffer_refused",
                "message": "changeGeometry rejected by buffer",
            }

        if identical:
            flog(f"_buffer_update: NO-OP geom identical "
                 f"eid={event.event_id} fid={target_fid}", "WARNING")
            geom_status = f"noop wkb_len={wkb_len}"
        else:
            flog(f"_buffer_update: geom changed "
                 f"eid={event.event_id} fid={target_fid}")
            geom_status = f"applied wkb_len={wkb_len}"

    # Attributes last, and that ordering is the fix, not a style choice:
    # every geometry refusal above (corrupt WKB, empty rebuild, CRS that
    # cannot be converted, drift beyond tolerance, buffer rejection) used to
    # fire with the restored attributes already in the buffer -- and the
    # drift guard returns success/skipped, so the runner committed that
    # half-written feature. Reached only once the geometry is settled, a
    # refusal now leaves the feature exactly as it was found.
    attr_ok = 0
    attr_fail = 0
    # Decoded with the event's schema version: pre-v6 date-time digits are
    # a local wall clock. Decoded without it, a 5.x value was written
    # shifted by the UTC offset (15:30 for 14:30 in Paris in winter) and
    # the commit read-back, comparing against the same mis-decoded value,
    # confirmed it.
    for idx, value in iter_mapped_attributes(
            mapping, old_attrs, fields,
            getattr(event, "event_schema_version", None), specs):
        if layer.changeAttributeValue(target_fid, idx, value):
            attr_ok += 1
        else:
            attr_fail += 1

    if attr_fail > 0 and attr_ok == 0:
        flog(f"_buffer_update: all attr changes REJECTED "
             f"eid={event.event_id} fid={target_fid} attempted={attr_fail}",
             "ERROR")
        return {
            "success": False,
            "status": "FAILED",
            "reason_code": "buffer_refused",
            "message": "All attribute changes rejected",
        }

    # BL-RW-P3-17: include trace_id on the structured BUF_UPD log.
    _upd_fields = dict(
        eid=event.event_id, fid=target_fid,
        attr_ok=attr_ok, attr_fail=attr_fail,
        geom_status=geom_status, status="applied",
    )
    if dropped:
        _upd_fields["dropped"] = ",".join(sorted(dropped))
    if trace_id:
        _upd_fields["trace_id"] = trace_id
    flog_kv("INFO", "BUF_UPD", module="restore_executor", **_upd_fields)
    # BL-RW-P2-14: tag the success path explicitly.
    # RLU-053: the rewind never runs pre_check_restore, so this is the only
    # place where a field abandoned to schema drift can reach the user.
    return _buffer_outcome(
        dropped, "Reverted via buffer", event.event_id, fid=target_fid)


def _safe_field_mapping(layer, event: AuditEvent) -> dict:
    """Build field mapping handling schema drift.

    Delegates to schema_drift.safe_field_mapping (DUP-04 consolidation):
    same algorithm as restore_service.pre_check_restore plus drift,
    but recomputed locally because buffer ops do not run pre_check
    upfront.
    """
    return safe_field_mapping(event, layer=layer)


def _field_mapping_report(layer, event: AuditEvent, attrs=None):
    """Mapping to apply, plus the captured fields the drift forbids.

    Same computation as `_safe_field_mapping`, but keeps the list of
    abandoned fields so the caller can report them. Every buffer op that
    writes attributes must use this one: the rewind never runs
    `pre_check_restore`, so it is the only place where the user can be
    told that his restore came back incomplete (RLU-053).
    """
    return field_mapping_report(event, layer=layer, attrs=attrs)


def _buffer_outcome(dropped: dict, message: str, eid,
                    fid: Optional[int] = None) -> dict:
    """Success dict for a buffer write: APPLIED, or APPLIED_PARTIAL.

    RLU-053. The rewind writes what the current schema still accepts,
    which is the right call -- refusing everything would throw away the
    fields that CAN come back. What is not acceptable is announcing that
    outcome as a complete restore: the user reads "Reverted", believes
    his feature is back to its former state, and keeps a hybrid -- some
    values from yesterday, the others from today -- with nothing in the
    result or the log to warn him.

    So a write that had to abandon captured fields comes back with its
    own status, its own reason code (the one the breakdown counts) and
    the list of the fields left behind.
    """
    result = {
        "success": True,
        "status": "APPLIED",
        "reason_code": RESTORE_REASON_APPLIED,
        "message": message,
    }
    if fid is not None:
        result["fid"] = fid
    if not dropped:
        return result

    names = sorted(dropped)
    flog(f"BUF_DRIFT eid={eid} fid={fid if fid is not None else '?'} "
         f"status=APPLIED_PARTIAL reason_code={RESTORE_REASON_APPLIED_PARTIAL} "
         f"dropped={names} (schema moved since capture: these fields keep "
         f"their current values)", "WARNING")
    result.update({
        "status": "APPLIED_PARTIAL",
        "reason_code": RESTORE_REASON_APPLIED_PARTIAL,
        "partial": True,
        "dropped_fields": names,
        "message": (
            f"{message} - INCOMPLETE: {len(names)} captured field(s) not "
            f"restored: {format_dropped_fields(dropped)}"
        ),
    })
    return result


def _target_matches_update_post_state(layer, fid: int, event,
                                      mapping: dict) -> tuple:
    """``(evidence, how)`` when feature *fid* carries the WHOLE NEW state.

    The first half of the remap proof (`_remap_update_proof`). The feature
    this rewind re-created under the event's fingerprint must hold what
    the edit produced: every changed value (NULL-aware, the event's schema
    version for its date-times) AND the NEW geometry, compared in the
    layer CRS -- the capture CRS aligned through
    `geometry_utils.geometry_for_layer_crs` by
    `restore_service._expected_geometry`, the reading the identity proofs
    and the re-insertion itself use. *evidence* is the
    `restore_service._StateEvidence` the second half counts. ``(None,
    why)`` otherwise, including a NEW state that cannot be compared and a
    read that failed (*why* then starts with ``scan_failed``).

    Replaces a check of the NEW geometry alone, compared RAW:
      * an edit without a NEW geometry (attributes only) was taken on
        trust. On a repacked shapefile the fingerprint fid:1 named C003
        when the edit was captured and C001 when it was deleted: the
        rewind re-created C001 and wrote C003's old values on it, APPLIED;
      * on a layer reprojected since the capture, the raw NEW geometry
        (degrees) never equalled the re-created feature (written in the
        layer CRS by `_buffer_insert`): the remap was dropped, the key
        looked up in the provider where the feature is not yet, and the
        edit refused ``target_absent`` while its post-edit state was
        committed;
      * an unreadable NEW geometry, or none captured at all, answered
        "matches".

    The NULL marker (the edit REMOVED the geometry) is a NEW state too:
    the feature must carry no geometry, and only in an event of schema 7
    or later (`core.constants.is_null_geometry_marker`); in an older one a
    zero-length blob is an unreadable NEW side, which proves nothing.
    """
    from .restore_service import (
        _update_side, _expected_geometry, _update_state_evidence,
        _StateEvidence, _layer_geometry_type,
    )
    from ..compat import QgisCompat

    _attrs, new_wkb = _update_side(event, "new")
    if (new_wkb is not None
            and _layer_geometry_type(layer) != QgisCompat.GEOM_NULL
            and _expected_geometry(new_wkb, event, layer) is None):
        why = "post_geometry_uncomparable (undecodable, or its CRS unusable)"
        flog(f"POST_CHK eid={event.event_id} fid={fid} {why} match=False",
             "DEBUG")
        return None, why
    evidence = _update_state_evidence(layer, fid, event, "new", mapping)
    if not isinstance(evidence, _StateEvidence):
        _level, why = evidence
        flog(f"POST_CHK eid={event.event_id} fid={fid} match=False "
             f"reason={why}", "DEBUG")
        return None, why
    flog(f"POST_CHK eid={event.event_id} fid={fid} match=True "
         f"shape={evidence.shape}", "DEBUG")
    return evidence, evidence.shape


def _remap_update_proof(layer, fid: int, event: AuditEvent, mapping: dict,
                        fid_remap: Optional[Dict], journal=None) -> tuple:
    """``(verdict, why)``: is remap entry *fid* the feature *event* edited?

    The entry is a NOMINATION: the feature this rewind re-created under
    the event's fingerprint. A fingerprint is not an identity: ``fid:N``
    of a shapefile is a position a repack gives to another feature, a key
    is given again after a delete. So the rule of every nomination
    applies (`restore_service._prove_update_state`): carry the post-edit
    state, and be DESIGNATED by it.

      * ``"proven"``: *fid* carries the WHOLE NEW state
        (`_target_matches_update_post_state`) and no other feature of the
        layer does (`_post_state_others`: read through the layer, the edit
        buffer included -- the other features this rewind re-created are
        candidates too); or, when others do and the identity is a key, the
        journal proves that the key named ONE entity from the edit to the
        DELETE whose re-insertion created *fid*
        (`restore_service._key_continuity` with ``until``);
      * ``"shared"``: *fid* carries it, so do others, and nothing tells
        them apart. On a FID identity nothing can: the position named C003
        when the edit was captured and C001 when it was deleted;
      * ``"not_carried"``: *fid* does not carry it (or it cannot be
        compared): it is not a candidate, the resolver decides without it;
      * ``"scan_failed"``: a read failed.

    Counting the combination, not stopping at two captured values (the
    FID-nomination shortcut): two flags or a status and a category are as
    common together as apart, and here the count is one read, filtered.
    """
    from .restore_service import (
        _event_identity, _identity_via, _key_continuity, TargetScanError,
    )

    evidence, how = _target_matches_update_post_state(layer, fid, event,
                                                      mapping)
    if evidence is None:
        verdict = ("scan_failed" if str(how).startswith("scan_failed")
                   else "not_carried")
        return verdict, how
    try:
        others = _post_state_others(layer, fid, event, evidence)
    except TargetScanError as exc:
        return "scan_failed", f"scan_failed: {exc}"
    if not others:
        return "proven", f"{how} carried by no other feature"
    shared = f"{how} also carried by fid(s) {others}"
    if _identity_via(_event_identity(layer, event)) != "pk":
        return "shared", f"{shared}; a FID names a position, not an entity"
    sources = (fid_remap or {}).get(_REMAP_SOURCE_SLOT)
    source = (sources.get(event.entity_fingerprint)
              if isinstance(sources, dict) else None)
    if source is None:
        return "shared", f"{shared}; the re-inserted row is unknown"
    continuous, history = _key_continuity(
        _rewind_baseline(journal, fid_remap), layer, event, until=source)
    if continuous:
        return "proven", f"{shared}; key_continuous ({history})"
    return "shared", f"{shared}; key_continuity_unproven ({history})"


def _post_state_others(layer, fid: int, event: AuditEvent, evidence,
                       limit: int = 2) -> List[int]:
    """FIDs OTHER than *fid* carrying the whole state *evidence* shows.

    *evidence*: the `restore_service._StateEvidence` of *fid* (every
    compared value, NULL included, and the geometry or its absence). Read
    through the layer, the edit buffer included. A captured geometry
    filters by its box; otherwise the provider filters on one captured
    value (the LIVE value of *fid*), believed only when it returns *fid*
    itself -- an expression the provider evaluates differently from our
    comparison is then read again whole. Raises TargetScanError.
    """
    from qgis.core import QgsFeatureRequest, QgsExpression
    from .restore_service import _scan, _evidence, _has_shape, _event_version
    from ..compat import QgisCompat

    attrs = {name: expected for name, _idx, expected in evidence.checks}
    comparable = [(name, idx) for name, idx, _expected in evidence.checks]
    geom = evidence.geom
    version = _event_version(event)
    what = f"remap proof eid={event.event_id} fid={fid} other holders"

    def _request():
        request = QgsFeatureRequest()
        if _has_shape(geom):
            request.setFilterRect(geom.boundingBox())
        elif geom is None and comparable:
            request.setFlags(QgisCompat.NO_GEOMETRY)
            request.setSubsetOfAttributes([idx for _name, idx in comparable])
        return request

    def _read(request):
        others, seen = [], False
        for feature in _scan(layer, request, what):
            if feature.id() == fid:
                seen = True
                continue
            if _evidence(feature, attrs, comparable, geom,
                         version=version).full:
                others.append(feature.id())
                if len(others) >= limit:
                    break
        return others, seen

    if not _has_shape(geom) and evidence.single is not None:
        name, _idx, live, _expected = evidence.single
        try:
            request = _request()
            request.setFilterExpression(
                f"{QgsExpression.quotedColumnRef(name)} = "
                f"{QgsExpression.quotedValue(live)}")
            others, seen = _read(request)
            if others or seen:
                return others
        except (TypeError, ValueError, AttributeError) as exc:
            flog(f"_post_state_others: filter unusable on {name!r} ({exc}); "
                 f"reading the layer", "DEBUG")
    others, _seen = _read(_request())
    return others


def _feature_geometry_matches(feature, expected_geom) -> bool:
    """Local alias kept for in-module call sites; delegates to
    geometry_utils.feature_matches_geometry (DUP-10 consolidation)."""
    return feature_matches_geometry(feature, expected_geom)


def _current_geom_matches(layer, fid: int, expected_geom) -> bool:
    try:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(fid)
        source = get_feature_source(layer)
        for feature in source(request):
            result = _feature_geometry_matches(feature, expected_geom)
            # WKB serialization for the diagnostic CUR_GEOM log is the
            # 2nd most expensive op per UPDATE event after the
            # getFeatures itself. Gate the serialization, keep the
            # matching logic unconditional.
            if heavy_diag_enabled():
                actual = feature.geometry()
                actual_repr = wkb_short_repr(bytes(actual.asWkb())) \
                    if actual and not actual.isNull() and not actual.isEmpty() else 'absent'
                flog(f"CUR_GEOM fid={fid} actual={actual_repr} match={result}", "DEBUG")
            return result
        flog(f"CUR_GEOM fid={fid} actual=fid_not_found match=False", "DEBUG")
    except Exception as exc:
        flog(f"CUR_GEOM fid={fid} lookup_err={exc} match=False", "WARNING")
    return False
