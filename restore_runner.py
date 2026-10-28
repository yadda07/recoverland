# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Non-blocking restore runner for RecoverLand.

Processes restore operations in small batches on the main thread,
yielding to the Qt event loop between batches so the UI stays responsive.

QGIS layer operations (provider.addFeatures, etc.) must run on the
main thread; this runner respects that constraint while avoiding freeze.
"""
from collections import defaultdict
import json
import time
from typing import Any, List, Dict, Optional, Callable

from qgis.PyQt.QtCore import QObject, QTimer, pyqtSignal

from .core.audit_backend import AuditEvent
from .core.restore_service import (
    restore_batch,
    undo_prepare,
    undo_apply_one,
    UndoBatchContext,
    build_fid_cache,
    build_restore_trace_event,
)
from .core.restore_executor import _apply_via_buffer
from .core.workflow_service import GroupedRestoreResult, target_refusal_reason
from .core.logger import flog
from .core.observability import assert_invariant, log_cycle_summary
from .core.geometry_utils import (
    feature_geom_short_repr, heavy_diag_enabled, wkb_short_repr,
)

# Chunk size for the QTimer-driven restore loop. Each chunk is processed
# synchronously on the UI thread; smaller chunks = more frequent yields
# = the logo animation and progress smoother stay responsive. The total
# rewind throughput is barely affected because the per-chunk overhead
# (slice + emit + singleShot) is negligible compared to the actual
# layer.changeGeometry / layer.addFeatures calls inside restore_batch.
_CHUNK_SIZE = 5

# BL-RW-P1-26: the strict runner bounds each event-loop tick by TIME,
# not by a fixed action count. A fixed count freezes the UI when single
# actions are expensive (measured 2026-07-05: 5 actions x ~5s of heavy
# polygon scans = 25.5s single chunk on zone_mkt_rip). 40ms keeps the
# progress animation fluid; the max-actions cap bounds per-tick work
# when actions are cheap.
_CHUNK_BUDGET_MS = 40.0
_CHUNK_MAX_ACTIONS = 50

# Threshold above which a single chunk is suspected of freezing the UI
# thread (Windows reports "(Ne repond pas)" around 5 s, but the animation
# visibly stutters from ~150-200 ms onwards). Shared by the strict runner
# and the undo runner: both bound their ticks the same way, so both must
# name the layer that blew the budget the same way.
_CHUNK_SLOW_MS = 200


def _chunk_should_yield(elapsed_ms: float, n_done: int,
                        budget_ms: float = _CHUNK_BUDGET_MS,
                        max_actions: int = _CHUNK_MAX_ACTIONS) -> bool:
    """Decide whether the strict apply loop must yield to the event loop.

    Progress guarantee: never yields before at least one action ran,
    even when a single action blows the budget (it already happened,
    yielding first would deadlock the plan). Yields once the time
    budget is exhausted or max_actions were processed.
    """
    if n_done <= 0:
        return False
    return elapsed_ms >= budget_ms or n_done >= max_actions


_RUNNER_TO_CYCLE: Dict[str, str] = {
    "RestoreRunner": "event_restore",
    "StrictRestoreRunner": "rewind",
    "UndoRunner": "undo",
}


def _cycle_for_runner(runner_name: str) -> str:
    """Map a runner class to its CYCLE_SUMMARY cycle name."""
    return _RUNNER_TO_CYCLE.get(runner_name, runner_name.lower())


# The mutually-exclusive outcomes a planned action can end in. Every
# action increments exactly one of them; that is what makes the
# conservation invariant below checkable at all. `cancelled` is kept
# apart: those actions were never attempted, so they belong to no
# outcome and must never be folded into a failure.
_OUTCOME_BUCKETS = (
    "applied", "applied_partial", "skipped_idempotent",
    "failed", "failed_target_absent", "failed_geometry_drift",
    # The provider accepted the commit, the read-back contradicts it: the
    # feature does not hold what was written. Never counted as restored.
    "failed_postcommit_verification",
)


def _apply_counters(breakdown: Dict[str, int]) -> Dict[str, int]:
    """Derive the counters CYCLE_SUMMARY publishes from the outcome buckets.

    Measured on a real project (2026-08-12), the line said:

        plan_actions=73 apply_ok=14 apply_skipped=67 apply_fail=59
        applied=6 failed=0 failed_target_absent=59 skipped_idempotent=3

    73 planned actions, 140 counted results, and a rewind that failed on
    59 of them announcing `failed=0`. Three different populations were
    printed side by side as if they partitioned the plan:

      * `apply_skipped` was ``events - events_that_fully_succeeded``, an
        EVENT-level number (73 - 6 = 67) covering every failure and every
        skip a second time, on top of the ACTION-level `apply_ok` and
        `apply_fail`. That is the double count: 14 + 59 already totalled
        the 73 actions before `apply_skipped` was added.
      * `apply_ok` counted everything the provider did not reject
        (6 applied + 3 idempotent skips + 5 drift refusals), so it
        contradicted `applied=6` on the same line.
      * `failed` published the residual "no specific category" bucket
        under the name a reader takes for the failure TOTAL.

    Here the three counters are computed from the buckets and from
    nothing else, so they partition the plan by construction:

        apply_ok      = applied + applied_partial
        apply_skipped = skipped_idempotent
        apply_fail    = failed + failed_target_absent
                        + failed_geometry_drift

    `failed` then publishes that failure total (never 0 while an action
    failed) and the residual keeps the explicit name `failed_other`.

    Returns the buckets plus the derived counters and ``outcome_sum``,
    the total of the six outcome buckets.
    """
    out: Dict[str, int] = {
        key: int(breakdown.get(key, 0) or 0) for key in _OUTCOME_BUCKETS
    }
    out["cancelled"] = int(breakdown.get("cancelled", 0) or 0)
    out["outcome_sum"] = sum(out[key] for key in _OUTCOME_BUCKETS)
    out["apply_ok"] = out["applied"] + out["applied_partial"]
    out["apply_skipped"] = out["skipped_idempotent"]
    out["apply_fail"] = (out["failed"] + out["failed_target_absent"]
                         + out["failed_geometry_drift"]
                         + out["failed_postcommit_verification"])
    return out


def build_apply_report(
    breakdown: Dict[str, int], plan_actions: Optional[int] = None,
) -> Dict[str, int]:
    """The buckets a finished run publishes, for the log AND for the dialog.

    RLU-055: `_apply_counters` fixed the CYCLE_SUMMARY line but the fix
    stopped one layer short. The dialog kept building its end-of-run
    message from `total_ok`/`total_fail`, so the same rewind was
    announced twice with two different stories:

        CYCLE_SUMMARY ... plan_actions=73 apply_ok=6 apply_skipped=3
                          apply_fail=64 failed_target_absent=59
        dialog        ... "14 restauree(s), 59 echouee(s)."

    73 planned actions, 6 entities actually back, and the user was told
    14 came back and 59 failed out of a total he could not compute.
    Everything a report shows now goes through this one function, so the
    message and the line cannot tell two different stories again.

    `plan_actions` is the base the buckets must add up to. ``None`` when
    the caller has no plan (a report rebuilt from an emitted result):
    the buckets then define their own base and the caller must say so
    rather than pass the sum off as a plan.
    """
    report = _apply_counters(breakdown)
    report["plan_actions"] = (
        int(plan_actions) if plan_actions is not None
        else report["outcome_sum"] + report["cancelled"]
    )
    # The residual bucket under the name the dialog reads, the one
    # CYCLE_SUMMARY prints. It was published as `failed` only (the bucket
    # key), `recover_dialog._restore_buckets` read `failed_other`, found
    # nothing, and "N pour une autre raison" was never shown: the failure
    # total announced more failures than its reasons named.
    report["failed_other"] = report["failed"]
    return report


def _assert_bucket_conservation(
    runner_name: str, trace_id: str, counters: Dict[str, int],
    total_ok: int, total_fail: int, plan_actions: Optional[int],
) -> bool:
    """Check the buckets against BOTH accounting bases, loudly.

    * ``total_ok + total_fail`` is what the runner counted while
      applying. This base was already checked.
    * ``plan_actions`` is what CYCLE_SUMMARY publishes, i.e. the number
      the reader adds the apply counters against. It was never checked,
      which is how the 2026-08-12 line could pass the invariant
      (6+0+3+0+59+5 == 14+59 == 73, silence) while publishing 140
      results for 73 actions: the counters on the line were simply not
      the buckets the invariant was verifying.

    Returns True when both bases agree. Never raises: the data changes
    already landed on disk, it is the metrics that are suspect, and a
    rewind must not be reported as crashed because its accounting drifted.
    """
    detail: Dict[str, Any] = {
        "runner": runner_name,
        "applied": counters["applied"],
        "applied_partial": counters["applied_partial"],
        "skipped_idempotent": counters["skipped_idempotent"],
        "failed_other": counters["failed"],
        "failed_target_absent": counters["failed_target_absent"],
        "failed_geometry_drift": counters["failed_geometry_drift"],
        "failed_postcommit_verification": counters["failed_postcommit_verification"],
        "cancelled": counters["cancelled"],
    }
    if trace_id:
        detail["trace"] = trace_id

    outcome_sum = counters["outcome_sum"]
    expected = int(total_ok) + int(total_fail)
    ok_totals = assert_invariant(
        outcome_sum == expected,
        "apply_buckets_vs_totals",
        base="total_ok+total_fail", expected=expected,
        bucket_sum=outcome_sum, delta=outcome_sum - expected,
        **detail,
    )

    ok_plan = True
    if plan_actions is not None:
        published = outcome_sum + counters["cancelled"]
        planned = int(plan_actions)
        ok_plan = assert_invariant(
            published == planned,
            "apply_buckets_vs_plan_actions",
            base="plan_actions", expected=planned,
            bucket_sum=published, delta=published - planned,
            apply_ok=counters["apply_ok"],
            apply_skipped=counters["apply_skipped"],
            apply_fail=counters["apply_fail"],
            **detail,
        )
    return bool(ok_totals and ok_plan)


def _finish_runner(
    runner,
    *,
    runner_name: str,
    by_ds: Dict[str, list],
    total_ok: int,
    total_fail: int,
    errors: List[str],
    traces: list,
    write_queue,
    started_at: float,
    tracker,
    trace_id: str = "",
    status_label: Optional[str] = None,
    failed_eids: Optional[List[int]] = None,
    cycle: Optional[str] = None,
    extra_stats: Optional[Dict[str, int]] = None,
    # BL-RW-P3-18: per-category breakdown propagated to runner.finished.
    breakdown: Optional[Dict[str, int]] = None,
    # RLU-054: the number of actions CYCLE_SUMMARY is accounted against.
    # Supplied by the runner that owns a plan; None on the paths that
    # have no per-action classification (undo).
    plan_actions: Optional[int] = None,
) -> None:
    """Common end-of-run pipeline for the three QObject runners.

    Steps reproduced from the previous per-runner implementations:
      1. Enqueue trace events to the write queue when present (logged at
         ERROR if the queue refuses them so the user knows the journal
         is degraded but the data changes still landed).
      2. Build the GroupedRestoreResult expected by the dialog code.
      3. Log a single ``finish`` line with elapsed_ms and either an
         ok/fail summary (RestoreRunner) or a status_label
         (StrictRestoreRunner).
      4. Emit ``runner.finished`` so the dialog drives the next step.
      5. Always unsuppress the edit tracker on the way out, even when
         the emit raises, otherwise tracking stays paused indefinitely.

    RLU-054: the CYCLE_SUMMARY apply counters are derived from the
    outcome buckets here (see `_apply_counters`) instead of being
    collected from three different populations, and the conservation
    invariant is checked against the base the line publishes as well as
    against the runner's own totals.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    # The run reads the journal no more: closed before `finished` wakes the
    # dialog, which may start another run on the same journal.
    _close_group_journal(runner)
    counters = _apply_counters(breakdown or {})
    applied = counters["applied"]
    # RLU-053: a write that had to abandon captured fields is a success
    # for the provider and a mutilation for the user. It leaves `applied`
    # so that "restored" never counts a hybrid feature.
    applied_partial = counters["applied_partial"]
    skipped_idempotent = counters["skipped_idempotent"]
    failed_other = counters["failed"]
    failed_target_absent = counters["failed_target_absent"]
    failed_geometry_drift = counters["failed_geometry_drift"]
    failed_postcommit = counters["failed_postcommit_verification"]
    cancelled = counters["cancelled"]

    conserved = True
    if breakdown is not None:
        conserved = _assert_bucket_conservation(
            runner_name, trace_id, counters,
            total_ok, total_fail, plan_actions,
        )
    try:
        if traces and write_queue is not None:
            try:
                accepted = write_queue.enqueue(
                    list(traces),
                    journal_path=getattr(runner, "_journal_path", None))
            except Exception as exc:  # noqa: BLE001 - said below, run must end
                # Raised out of here, it ended the chain before `finished`:
                # the dialog stayed "busy" and the user was told nothing.
                flog(f"{prefix}{runner_name}: trace enqueue raised: {exc}",
                     "CRITICAL")
                accepted = False
            if not accepted:
                msg = (
                    "Journal trace write failed; restore data changes"
                    " succeeded but trace events were saved"
                    " for pending recovery"
                )
                errors.append(msg)
                flog(f"{runner_name}: {msg}", "ERROR")

        result = GroupedRestoreResult(
            total_ok=total_ok,
            total_fail=total_fail,
            errors=errors,
            by_ds=dict(by_ds),
            trace_events=traces,
            failed_eids=failed_eids or [],
            applied=applied,
            skipped_idempotent=skipped_idempotent,
            failed=failed_other,
            failed_target_absent=failed_target_absent,
            failed_geometry_drift=failed_geometry_drift,
            failed_postcommit_verification=failed_postcommit,
        )
        elapsed_ms = (
            int((time.monotonic() - started_at) * 1000)
            if started_at else 0
        )
        # `failed_other` is named in full here too: the short `failed=`
        # on this line was read as the failure total by anyone grepping
        # a run, exactly like on CYCLE_SUMMARY.
        status_part = (
            f"status={status_label} " if status_label is not None else ""
        )
        flog(f"{prefix}{runner_name}: finish {status_part}"
             f"ok={total_ok} fail={total_fail} elapsed_ms={elapsed_ms} "
             f"applied={applied} applied_partial={applied_partial} "
             f"skipped_idempotent={skipped_idempotent} "
             f"failed_other={failed_other} "
             f"failed_target_absent={failed_target_absent} "
             f"failed_geometry_drift={failed_geometry_drift} "
             f"failed_postcommit_verification={failed_postcommit} "
             f"cancelled={cancelled}")

        cycle_name = cycle or _cycle_for_runner(runner_name)
        cycle_stats: Dict[str, Any] = dict(extra_stats or {})
        cycle_stats["traces_written"] = (
            (len(traces) if traces else 0)
            + int(getattr(runner, "_traces_flushed", 0) or 0)
        )
        if plan_actions is not None:
            cycle_stats["plan_actions"] = int(plan_actions)
        if breakdown is None:
            # No per-action classification on this path (undo). Publish
            # the provider-level totals and NOT a row of zeroed buckets:
            # `failed=0` printed next to a non-zero `apply_fail` reads as
            # "no failure", which is the opposite of what happened.
            cycle_stats["apply_ok"] = total_ok
            cycle_stats["apply_fail"] = total_fail
        else:
            cycle_stats.update({
                # Disjoint by construction: ok + skipped + fail
                # (+ cancelled) == plan_actions.
                "apply_ok": counters["apply_ok"],
                "apply_skipped": counters["apply_skipped"],
                "apply_fail": counters["apply_fail"],
                "applied": applied,
                "applied_partial": applied_partial,
                "skipped_idempotent": skipped_idempotent,
                # `failed` is the failure TOTAL, `failed_other` the
                # residual bucket. The reverse convention is what
                # announced failed=0 on a rewind that failed 59 times.
                "failed": counters["apply_fail"],
                "failed_other": failed_other,
                "failed_target_absent": failed_target_absent,
                "failed_geometry_drift": failed_geometry_drift,
                "failed_postcommit_verification": failed_postcommit,
            })
            if cancelled:
                cycle_stats["cancelled"] = cancelled
            if not conserved:
                # The counters did not add up; say so on the line the
                # user re-reads rather than only in the CRITICAL above.
                cycle_stats["bucket_conservation"] = "violated"
        log_cycle_summary(trace_id, cycle_name, cycle_stats, elapsed_ms)

        runner.finished.emit(result)
    finally:
        _release_run_suppression(runner, tracker)
        # Safety net: every group closes its own; a crash or a cancel in
        # the middle of one leaves it here.
        _close_group_journal(runner)


def _suppress_for_run(runner, layer) -> None:
    """Keep THIS layer's writes out of the journal while the run writes it.

    Taken when the run is about to write the layer, given back when it is
    done with it (`_release_group_suppression`), not at the end of the
    whole run: a layer already written, or one the run refused, belongs to
    the user again, and every commit he makes there must be journalled.
    """
    tracker = getattr(runner, "_tracker", None)
    if tracker is None or layer is None:
        return
    try:
        layer_id = layer.id()
    except (RuntimeError, AttributeError):
        return
    ids = runner.__dict__.setdefault("_suppressed_ids", [])
    runner.__dict__["_suppressed_current"] = layer_id
    if layer_id in ids:
        return
    if hasattr(tracker, "suppress_runner_layer"):
        tracker.suppress_runner_layer(layer_id)
    elif hasattr(tracker, "suppress_layers"):
        tracker.suppress_layers([layer_id])
    else:
        tracker.suppress()
    ids.append(layer_id)


def _runner_step_ctx(runner, layer):
    """Context manager marking *layer* as actively mutated by *runner* on the stack."""
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        tracker = getattr(runner, "_tracker", None) if runner is not None else None
        lid = ""
        if tracker is not None and layer is not None:
            try:
                lid = layer.id()
            except (RuntimeError, AttributeError):
                lid = ""
        enter = getattr(tracker, "enter_runner_step", None) if lid else None
        leave = getattr(tracker, "leave_runner_step", None) if lid else None
        if callable(enter):
            enter(lid)
        try:
            yield
        finally:
            if callable(leave):
                leave(lid)

    return _ctx()


def _release_group_suppression(runner) -> None:
    """Give back the capture of the layer the run has just finished. Idempotent.

    Called at the end of each layer group. The suppression used to be held
    until the end of the run: in a run over several layers, a layer already
    written stayed suppressed while the next ones were processed, and the
    user's commits on it were dropped by every tracker handler, with no
    event in the journal and no message (revue brev2, suppression tenue).
    """
    layer_id = runner.__dict__.pop("_suppressed_current", None)
    ids = runner.__dict__.get("_suppressed_ids") or []
    if layer_id is None or layer_id not in ids:
        return
    ids.remove(layer_id)
    tracker = getattr(runner, "_tracker", None)
    if tracker is None:
        return
    try:
        if hasattr(tracker, "unsuppress_layers"):
            tracker.unsuppress_layers([layer_id])
        else:
            tracker.unsuppress()
    except Exception as exc:  # noqa: BLE001 - never block the run on this
        flog(f"restore_runner: releasing the capture of layer {layer_id} "
             f"failed: {exc}", "ERROR")


def _open_group_journal(runner, layer_name: str):
    """ONE read-only connection on the run's journal, for the group starting.

    The identity proofs of the executor read the journal the events come
    from: whether a key never changed hands (`restore_service
    ._key_continuity`), and for a rewind the purge horizon the journal
    persisted for the layer (`restore_planner.preflight_check`). The
    runners called them without it: every key whose content other
    features share was refused as unprovable ("value_shared"), and a
    rewind whose own read of the journal failed went on with the purge
    horizon of this session only (a restart forgets it).

    The connection is opened when the run takes a layer and closed when it
    is done with it (`_close_group_journal`, called on every exit path of
    a group and again by `_finish_runner`). The executor keeps it in its
    batch cache for the whole group, across the chunks: it must stay open
    exactly that long.

    Returns ``(conn, error)``: ``(None, "")`` when the run was given no
    journal (the executor then proves keys by rare content only, as
    before), ``(None, reason)`` when the journal is known but cannot be
    read, which each caller must treat as a refusal or say.
    """
    _close_group_journal(runner)
    path = getattr(runner, "_journal_read_path", None)
    if not path:
        return None, ""
    import sqlite3
    conn = None
    try:
        # Looked up at call time: `sqlite_readonly_uri` builds the URI that
        # survives '#', '?' and '%' in folder names and long paths.
        from .core.journal_manager import sqlite_readonly_uri
        conn = sqlite3.connect(sqlite_readonly_uri(path), uri=True)
        # Opening a URI proves nothing about its content: the table the
        # proofs read must answer.
        conn.execute("SELECT 1 FROM audit_event LIMIT 1").fetchall()
    except Exception as exc:  # noqa: BLE001 - said to the caller, never silent
        if conn is not None:
            try:
                conn.close()
            except Exception:  # nosec B110 - already failing, reason kept below
                pass
        return None, f"{type(exc).__name__}: {exc}"
    runner.__dict__["_group_journal"] = conn
    trace_id = getattr(runner, "_trace_id", "")
    prefix = f"[{trace_id}] " if trace_id else ""
    flog(f"{prefix}{type(runner).__name__}: journal_read opened "
         f"layer={layer_name!r}")
    return conn, ""


def _close_group_journal(runner) -> None:
    """Close the read connection of the current group, if any. Idempotent."""
    conn = runner.__dict__.pop("_group_journal", None)
    if conn is None:
        return
    try:
        conn.close()
    except Exception as exc:  # noqa: BLE001 - a close never stops a run
        flog(f"restore_runner: journal read connection close failed "
             f"({exc!r})", "WARNING")


def _unreadable_journal_refusal(group: list, name: str, reason: str) -> str:
    """What a Mode A restore says of a layer refused for an unreadable journal.

    The layer, the reason, why it cannot go on without the journal, and
    the events left as they are, so the user can run them again.
    """
    ids = [event.event_id or 0 for event in group]
    shown = ", ".join(str(i) for i in ids[:10])
    if len(ids) > 10:
        shown += f", ... (+{len(ids) - 10})"
    return (f"Couche '{name}' refusee : le journal ne peut pas etre lu "
            f"({reason}). Sans lui, ni la cle d'une entite que d'autres "
            f"partagent, ni le fait qu'un evenement a deja ete restaure ne "
            f"peuvent etre verifies : ecrite a l'aveugle, la restauration "
            f"pourrait defaire une modification faite depuis. "
            f"{len(ids)} evenement(s) non restaure(s) : {shown}. Rien n'a "
            f"ete ecrit sur cette couche ; recommencez quand le journal est "
            f"de nouveau lisible.")


def _release_run_suppression(runner, tracker) -> None:
    """Undo every per-layer suppression the run still holds. Idempotent.

    The end-of-run safety net: normally each layer was given back at the
    end of its own group already; a cancel or a crash in the middle of a
    group leaves the current one here.
    """
    ids = list(getattr(runner, "_suppressed_ids", []) or [])
    runner.__dict__["_suppressed_ids"] = []
    runner.__dict__.pop("_suppressed_current", None)
    if tracker is None or not ids:
        return
    try:
        if hasattr(tracker, "unsuppress_layers"):
            tracker.unsuppress_layers(ids)
        else:
            for _ in ids:
                tracker.unsuppress()
    except Exception as exc:  # noqa: BLE001 - never block the end of a run
        flog(f"restore_runner: releasing capture suppression failed: {exc}",
             "ERROR")


def _emit_progress(runner, done: int, total: int) -> None:
    """Emit `progress` from a path that accounts a group. Never raises.

    Those paths run inside the crash guards: a slot of the progress bar
    that raised there sent the guard to account the same group a second
    time, and arm a second chain.
    """
    try:
        runner.progress.emit(done, total)
    except Exception as exc:  # noqa: BLE001 - the bar only
        flog(f"{type(runner).__name__}: progress emit failed: {exc}",
             "WARNING")


def _abort_started_run(runner, runner_name: str, exc: BaseException,
                       trace_id: str = "") -> None:
    """Close a run whose start() died before the async chain could begin.

    The three runners suppress the edit tracker in start() and release it
    in _finish(), at the end of a chain driven by QTimer. When the very
    first step raises (layer removed from the project, destroyed C++
    object, SQLite failure), that chain never runs: the tracker used to
    stay suppressed for the rest of the session, so the plugin silently
    stopped journalling the user's edits right after a failed restore -
    exactly when he starts repairing by hand. Finishing the runner here
    releases the lock and tells the dialog the run is over; the caller
    still sees the original exception.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    flog(f"{prefix}{runner_name}: start aborted, releasing capture lock "
         f"({type(exc).__name__}: {exc})", "ERROR")
    runner._errors.append(f"Restauration interrompue: {exc}")
    runner._finish()


def _resolve_runner_layer(
    find_layer_fn: Callable[[AuditEvent], object],
    group: list,
    fp: str,
    runner_name: str,
    uncommitted_msg: str,
    trace_id: str = "",
    runner_owner=None,
):
    """Resolve and validate the target layer for a runner group.

    Common preflight reused by RestoreRunner, StrictRestoreRunner and
    UndoRunner: the lookup fails when the layer is missing from the
    project, refuses to act when the user has uncommitted edits, and
    auto-commits a clean edit session so the runner can take ownership
    of the next provider transaction.

    Returns ``(layer, errors)``. ``errors`` is empty when preflight
    passed (the caller proceeds with the resolved layer). Otherwise the
    caller must record each error message against the whole group and
    advance.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    name = group[0].layer_name_snapshot or fp
    layer = find_layer_fn(group[0])

    if layer is None:
        # A loaded layer refused as the target (a history captured before
        # v7 without its connection service, a re-pointed layer) is not a
        # missing layer: "non trouvee" sent the user looking for a layer
        # he has in front of him, when what he has to do is attach that
        # history to its service (see core.workflow_service).
        reason = target_refusal_reason(group[0])
        flog(f"{prefix}{runner_name}: layer NOT FOUND "
             f"name='{name}' fp={fp} events={len(group)} "
             f"refused_because={reason or '-'}", "ERROR")
        if reason:
            return None, [f"Couche '{name}' refusee comme cible : {reason}."]
        return None, [f"Couche '{name}' non trouvee dans le projet."]

    editing = hasattr(layer, 'isEditable') and layer.isEditable()
    if editing and hasattr(layer, 'isModified') and layer.isModified():
        # Refused, and NOT suppressed: the user is told to save, and the
        # save he makes while the run goes on must be journalled. The
        # suppression used to be taken before this check and held to the
        # end of the run, which dropped exactly that commit.
        flog(f"{prefix}{runner_name}: layer HAS UNSAVED EDITS "
             f"name='{name}' fp={fp}", "ERROR")
        return None, [
            f"Evt {event.event_id or 0}: {uncommitted_msg}"
            for event in group
        ]

    if runner_owner is not None:
        # From here the run writes this layer. Suppressed BEFORE the
        # auto-commit below, which is a write too.
        _suppress_for_run(runner_owner, layer)
    if editing:
        flog(f"{prefix}{runner_name}: auto-closing edit session "
             f"name='{name}' fp={fp}")
        with _runner_step_ctx(runner_owner, layer):
            layer.commitChanges()

    return layer, []


class RestoreRunner(QObject):
    """Stepped restore executor that never blocks the UI.

    Usage:
        runner = RestoreRunner(events, resolver, write_queue)
        runner.progress.connect(on_progress)
        runner.finished.connect(on_done)
        runner.start()
    """

    progress = pyqtSignal(int, int)
    finished = pyqtSignal(object)

    def __init__(
        self,
        events: List[AuditEvent],
        find_layer_fn: Callable[[AuditEvent], object],
        write_queue=None,
        tracker=None,
        trace_id: str = "",
        parent: Optional[QObject] = None,
        journal_path: Optional[str] = None,
    ):
        super().__init__(parent)
        self._events = events
        self._find_layer_fn = find_layer_fn
        self._write_queue = write_queue
        # Traces belong to the journal this run started on, even if the
        # project switches before the run ends.
        self._journal_path = getattr(write_queue, 'journal_path', None)
        # The journal the identity proofs read (`_open_group_journal`): the
        # one the caller planned from, else the one the traces go to.
        self._journal_read_path = journal_path or self._journal_path
        self._tracker = tracker
        self._trace_id = trace_id
        self._cancelled = False

        self._by_ds: Dict[str, list] = defaultdict(list)
        for event in events:
            self._by_ds[event.datasource_fingerprint].append(event)

        self._groups = list(self._by_ds.items())
        self._group_idx = 0
        self._event_idx = 0

        self._total_ok = 0
        self._total_fail = 0
        self._errors: List[str] = []
        self._traces: list = []
        self._processed = 0
        self._started_at = 0.0

        self._current_layer = None
        self._current_fp = ""
        self._group_ok = 0
        self._group_fid_cache: Dict = {}

        # BL-RW-P3-18: per-category breakdown accumulator.
        self._applied = 0
        self._skipped_idempotent = 0
        self._failed_other = 0
        self._failed_target_absent = 0
        self._failed_geometry_drift = 0
        # Failures whose write reached the file anyway: their own bucket,
        # out of `failed_other`, so the report says the file was changed.
        self._failed_postcommit = 0
        # The events this run really wrote: the `applied` bucket, by id.
        # Only they may enter the undo scope and the journal (see _finish).
        self._applied_ids: set = set()
        # Events reported FAILED whose write nevertheless reached the file
        # (v7 `landed`: the read-back contradicts it, or a refused write
        # could not be taken back). Written, so journalled and undoable
        # like the applied ones; still counted as failures.
        self._landed_ids: set = set()

    def start(self) -> None:
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        self._started_at = time.monotonic()
        flog(f"{prefix}RestoreRunner: start events={len(self._events)} groups={len(self._groups)}")
        # No global suppression: the capture is suppressed per layer, when
        # the run takes a layer (_suppress_for_run). Suppressing everything
        # for a minutes-long, non-modal run dropped every commit the user
        # made meanwhile on any other layer.
        try:
            self._advance_group(guarded=False)
        except BaseException as exc:
            _abort_started_run(self, "RestoreRunner", exc, self._trace_id)
            raise

    def cancel(self) -> None:
        self._cancelled = True

    def _advance_group(self, guarded: bool = True) -> None:
        """Take the next layer group; from a QTimer tick, with a crash guard.

        The first group is opened by `start`, whose own handler closes the
        run and lets the caller see the exception (`guarded=False`). Every
        later group is opened from a QTimer tick: a resolver that raised
        (layer deleted, journal read failing), a provider failing the FID
        pre-resolution, ended the chain there without `_finish`, and the
        dialog stayed "busy" until the project was switched.
        """
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return
        if not guarded:
            self._open_group()
            return
        try:
            self._open_group()
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            self._abandon_group("group open", exc)

    def _open_group(self) -> None:
        fp, group = self._groups[self._group_idx]
        self._current_fp = fp
        self._event_idx = 0
        self._group_ok = 0
        self._group_fid_cache = {}
        name = group[0].layer_name_snapshot or fp

        # The journal first: a layer the run will refuse is not touched
        # (no capture suppression, no auto-commit of its edit session).
        journal, unreadable = _open_group_journal(self, name)
        if unreadable:
            # Fail-safe, as StrictRestoreRunner does. The journal is what
            # tells the executor that an event is ALREADY compensated by a
            # trace in effect (`already_compensated`). Without it that
            # refusal is off: the run used to go on, and a second restore
            # of an event the user had edited again since was written,
            # undid that later edit and journalled a second trace naming
            # the first event, under a message saying it was refused.
            prefix = f"[{self._trace_id}] " if self._trace_id else ""
            flog(f"{prefix}RestoreRunner: journal_read unavailable "
                 f"layer={name!r}: {unreadable}; layer refused, nothing "
                 f"written", "ERROR")
            self._refuse_group(group, [
                _unreadable_journal_refusal(group, name, unreadable)])
            return

        layer, errors = _resolve_runner_layer(
            self._find_layer_fn, group, fp,
            runner_name="RestoreRunner",
            uncommitted_msg="Target layer has uncommitted edits; "
                            "commit or rollback before restore",
            trace_id=self._trace_id,
            runner_owner=self,
        )
        if layer is None:
            self._refuse_group(group, errors)
            return

        self._current_layer = layer
        self._group_fid_cache = build_fid_cache(layer, group, journal=journal)
        QTimer.singleShot(0, self._process_chunk)

    def _refuse_group(self, group: list, errors: List[str]) -> None:
        """A group refused before any write: every event failed, named.

        Nothing of it was written. What the run took for it (the capture
        of its layer, its journal connection) is given back at once, and
        the next group is armed.
        """
        self._errors.extend(errors)
        # BL-RW-P4-21: the entire group is accounted as failed_other so the
        # breakdown invariant holds.
        self._failed_other += len(group)
        self._total_fail += len(group)
        self._processed += len(group)
        self._group_idx += 1
        self._current_layer = None
        _release_group_suppression(self)
        _close_group_journal(self)
        self._group_fid_cache = {}
        _emit_progress(self, self._processed, len(self._events))
        QTimer.singleShot(0, self._advance_group)

    def _abandon_group(self, where: str, exc: BaseException) -> None:
        """Account the rest of the current group as failed and go on.

        The dialog owns this runner until its `finished`, and refuses any
        other run meanwhile: an exception out of a QTimer slot ended the
        chain without `_finish`, the dialog stayed "busy" for good and the
        layer stayed suppressed. The rest of the layer is accounted as
        failed (named), its capture and its journal connection are given
        back, and the run goes on.
        """
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        flog(f"{prefix}RestoreRunner: CRASH in {where}: {exc}", "CRITICAL")
        self._errors.append(f"Internal error: {exc}")
        if self._group_idx < len(self._groups):
            _fp, group = self._groups[self._group_idx]
            rest = max(len(group) - self._event_idx, 0)
            self._failed_other += rest
            self._total_fail += rest
            self._processed += rest
            self._event_idx = len(group)
            self._group_idx += 1
        _release_group_suppression(self)
        _close_group_journal(self)
        self._group_fid_cache = {}
        QTimer.singleShot(0, self._advance_group)

    def _process_chunk(self) -> None:
        """One chunk of the current layer, with a crash guard."""
        if self._cancelled:
            self._finish()
            return
        try:
            self._process_chunk_inner()
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            self._abandon_group("chunk", exc)

    def _process_chunk_inner(self) -> None:
        _fp, group = self._groups[self._group_idx]
        layer = self._current_layer
        start = self._event_idx
        end = min(start + _CHUNK_SIZE, len(group))
        chunk = group[start:end]

        with _runner_step_ctx(self, layer):
            report = restore_batch(
                layer, chunk,
                fid_cache=self._group_fid_cache,
                trace_id=self._trace_id,
                journal=self.__dict__.get("_group_journal"),
            )
        self._total_ok += len(report.succeeded)
        self._group_ok += len(report.succeeded)
        self._total_fail += len(report.failed)
        self._traces.extend(report.trace_events)
        for eid, msg in report.failed.items():
            self._errors.append(f"Evt {eid}: {msg}")

        # BL-RW-P3-18: per-category accumulation.
        target_absent_set = set(report.failed_target_absent or [])
        drift_set = set(report.failed_geometry_drift or [])
        skipped_set = set(report.skipped_idempotent or [])
        self._skipped_idempotent += len(skipped_set)
        self._failed_target_absent += len(target_absent_set)
        self._failed_geometry_drift += len(drift_set)
        for eid in report.succeeded:
            if eid in skipped_set or eid in target_absent_set or eid in drift_set:
                continue
            self._applied += 1
            self._applied_ids.add(eid)
        landed = self._landed_failures(report, chunk, layer)
        self._landed_ids.update(landed)
        for eid in report.failed.keys():
            if eid in target_absent_set or eid in drift_set:
                continue
            if eid in landed:
                self._failed_postcommit += 1
            else:
                self._failed_other += 1

        self._event_idx = end
        self._processed += len(chunk)
        self.progress.emit(self._processed, len(self._events))

        if self._event_idx >= len(group):
            if self._group_ok > 0:
                provider = layer.dataProvider()
                if provider is not None and hasattr(provider, 'reloadData'):
                    provider.reloadData()
                if hasattr(layer, 'updateExtents'):
                    layer.updateExtents()
                layer.reload()
            layer.triggerRepaint()
            # The run is done with this layer: its capture is the user's
            # again while the next layers are restored, and its journal
            # connection is closed (the next group opens its own).
            _release_group_suppression(self)
            _close_group_journal(self)
            self._group_fid_cache = {}
            self._group_idx += 1
            QTimer.singleShot(0, self._advance_group)
        else:
            QTimer.singleShot(0, self._process_chunk)

    def _landed_failures(self, report, chunk, layer) -> set:
        """Failed events of `report` whose write nevertheless reached the file.

        Read from the v7 contract (`RestoreReport.landed`). The fallback
        for a report built without that list (an older restore_batch, or a
        caller that leaves the default empty list) is the old mark:
        restore_batch built a trace for a failed event only when its write
        landed. Both are taken: a failure the journal is told about (a
        trace) is in the file as far as the next Rewind is concerned, and
        must stay undoable. A landed failure the report did not trace is
        traced here: the file changed, the journal must say so.
        """
        failed = set(report.failed or {})
        traced = {getattr(t, "restored_from_event_id", None)
                  for t in report.trace_events}
        out = {eid for eid in failed if eid in traced}
        out.update(eid for eid in (getattr(report, "landed", None) or ())
                   if eid in failed)
        by_id = {e.event_id: e for e in chunk}
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        for eid in sorted(out - traced):
            event = by_id.get(eid)
            try:
                trace = (build_restore_trace_event(event, layer)
                         if event is not None else None)
            except Exception as exc:  # noqa: BLE001 - said below, run goes on
                flog(f"{prefix}RestoreRunner: landed trace eid={eid} "
                     f"failed: {exc}", "ERROR")
                trace = None
            if trace is not None:
                self._traces.append(trace)
                continue
            msg = (f"Evt {eid}: ecrit dans le fichier malgre l'echec, mais "
                   f"aucune trace n'a pu etre construite pour le journal")
            self._errors.append(msg)
            flog(f"{prefix}RestoreRunner: landed_untraced eid={eid}", "ERROR")
        return out

    def written_count(self) -> int:
        """Events written to disk so far (applied + landed failures)."""
        return len(self._applied_ids | self._landed_ids)

    def _cancelled_actions(self) -> int:
        """Events a cancel left unprocessed: never attempted, never failed."""
        return max(len(self._events) - self._processed, 0)

    def _outcome_breakdown(self) -> Dict[str, int]:
        """The outcome buckets, single source for the log and the dialog."""
        return {
            "applied": self._applied,
            "skipped_idempotent": self._skipped_idempotent,
            "failed": self._failed_other,
            "failed_target_absent": self._failed_target_absent,
            "failed_geometry_drift": self._failed_geometry_drift,
            # Written, then contradicted or not taken back: a failure the
            # user must read as "the file was changed", never as "another
            # reason" (the rewind path already names it so).
            "failed_postcommit_verification": self._failed_postcommit,
            "cancelled": self._cancelled_actions(),
        }

    def apply_report(self) -> Dict[str, int]:
        """RLU-055: the buckets the dialog must display, plan base included.

        Read by the dialog in the ``finished`` slot, i.e. on the numbers
        this runner has just published on CYCLE_SUMMARY, and derived by
        the very same function: the message and the line say the same
        thing or neither does.
        """
        return build_apply_report(self._outcome_breakdown(), len(self._events))

    def _finish(self) -> None:
        # The published by_ds is the UNDO SCOPE, and the traces tell the
        # journal "this compensation is in the data". Both are restricted
        # to what this run wrote, as StrictRestoreRunner does with
        # `_all_succeeded_ids`. They used to carry every selected event:
        # an Undo (or the silent auto-undo before the next Rewind) then
        # deleted a feature an "already exists" skip had left alone,
        # re-created one an "already absent" skip had not touched, and
        # wrote NEW values over the occupant of a FID the forward path had
        # just refused as unrelated. A skip or a refusal wrote nothing:
        # there is nothing to undo, and nothing to trace.
        # A failure whose write LANDED is the opposite case: the data
        # changed, so it is journalled and stays undoable, as
        # StrictRestoreRunner does with what a refused commit could not
        # take back. Dropping its trace made the next Rewind replay the
        # compensation on top of it (a DELETE: the feature twice).
        written_ids = self._applied_ids | self._landed_ids
        applied_by_ds: Dict[str, list] = {}
        for fp, events in self._by_ds.items():
            written = [e for e in events if e.event_id in written_ids]
            if written:
                applied_by_ds[fp] = written
        traces = [t for t in self._traces
                  if getattr(t, "restored_from_event_id", None)
                  in written_ids]
        n_selected = sum(len(v) for v in self._by_ds.values())
        n_scope = sum(len(v) for v in applied_by_ds.values())
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        if n_scope != n_selected or len(traces) != len(self._traces):
            flog(f"{prefix}RestoreRunner: undo_scope_filtered "
                 f"selected={n_selected} written={n_scope} "
                 f"excluded={n_selected - n_scope} "
                 f"traces_dropped={len(self._traces) - len(traces)} "
                 f"(skipped, refused, failed or never attempted: nothing "
                 f"written, nothing to undo, nothing to journal)")
        if self._landed_ids:
            flog(f"{prefix}RestoreRunner: landed_failures "
                 f"n={len(self._landed_ids)} "
                 f"eids={sorted(self._landed_ids)[:20]} (reported failed; "
                 f"their writes are in the file: journalled and undoable)",
                 "WARNING")
        # RLU-054: one event yields exactly one result here, so the plan
        # base is the event count and whatever a cancellation left
        # unprocessed is `cancelled` -- never silently a failure.
        _finish_runner(
            self,
            runner_name="RestoreRunner",
            by_ds=applied_by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=traces,
            write_queue=self._write_queue,
            started_at=self._started_at,
            tracker=self._tracker,
            trace_id=self._trace_id,
            plan_actions=len(self._events),
            breakdown=self._outcome_breakdown(),
        )


class StrictRestoreRunner(QObject):
    """Atomic per-layer restore executor for temporal mode.

    Uses QGIS editing buffer + rollback: if any event in a layer
    group fails, all changes for that layer are rolled back.
    Non-blocking: one layer per QTimer tick.

    Usage:
        runner = StrictRestoreRunner(events, resolver, cutoff, ...)
        runner.progress.connect(on_progress)
        runner.layer_started.connect(on_layer)
        runner.finished.connect(on_done)
        runner.start()
    """

    progress = pyqtSignal(int, int)
    # RLU-057: (layer_name, index_1based, n_layers, n_events) at the
    # moment a layer group opens. Measured cycles of 95 s and 283 s left
    # the phase label on the text written before the run started, so the
    # only sign of life was a bar creeping over an unnamed total. Naming
    # the layer being written is what tells the user the run is alive and
    # where it currently is.
    layer_started = pyqtSignal(str, int, int, int)
    finished = pyqtSignal(object)

    def __init__(
        self,
        events: List[AuditEvent],
        find_layer_fn: Callable[[AuditEvent], object],
        cutoff,
        write_queue=None,
        tracker=None,
        trace_id: str = "",
        parent: Optional[QObject] = None,
        extra_stats: Optional[Dict[str, int]] = None,
        journal_path: Optional[str] = None,
    ):
        super().__init__(parent)
        self._events = events
        self._find_layer_fn = find_layer_fn
        self._cutoff = cutoff
        self._write_queue = write_queue
        # Traces belong to the journal this run started on, even if the
        # project switches before the run ends.
        self._journal_path = getattr(write_queue, 'journal_path', None)
        # The journal the coverage guard and the identity proofs read
        # (`_open_group_journal`): the one the caller planned from, else
        # the one the traces go to.
        self._journal_read_path = journal_path or self._journal_path
        self._tracker = tracker
        self._trace_id = trace_id
        self._extra_stats = dict(extra_stats) if extra_stats else {}
        self._cancelled = False

        self._by_ds: Dict[str, list] = defaultdict(list)
        for event in events:
            self._by_ds[event.datasource_fingerprint].append(event)

        self._groups = list(self._by_ds.items())
        self._group_idx = 0

        self._total_ok = 0
        self._total_fail = 0
        self._errors: List[str] = []
        self._traces: list = []
        self._processed = 0
        self._started_at = 0.0

        self._strict_plan = None
        self._strict_events_by_id: Dict = {}
        self._strict_layer = None
        self._strict_layer_name = ""
        self._strict_action_idx = 0
        self._strict_succeeded: List[int] = []
        self._strict_was_editing = False
        self._strict_apply_start = 0.0
        self._strict_count_before = -1
        self._strict_fid_remap: Dict = {}
        self._strict_skipped: List[int] = []
        self._all_succeeded_ids: set = set()

        # RLU-054: the base CYCLE_SUMMARY is accounted against. Counted
        # as the run goes, layer by layer, because that is the only place
        # that knows how many actions a plan really carried:
        #   _plan_actions: actions the planner emitted, plus the events
        #     of a group refused whole (no layer, preflight blocked, no
        #     edit session) since those never reached the planner's
        #     verdict and are accounted as one failure each.
        #   _plan_skipped: events the planner dropped (no stable
        #     identity): never applied, never failed, and until now
        #     invisible on the line.
        self._plan_actions = 0
        self._plan_skipped = 0

        # BL-RW-P3-18: per-category breakdown accumulator (mirror of
        # RestoreRunner). Per-layer counters are kept separate so a
        # rollback or a commit failure can credit the whole layer as
        # _failed_other instead of leaving accumulated buckets that
        # never made it to the user-visible state (BL-RW-P4-21).
        self._applied = 0
        self._skipped_idempotent = 0
        self._failed_other = 0
        self._failed_target_absent = 0
        self._failed_geometry_drift = 0
        # RLU-053: features written without every captured field, and the
        # names of the fields left behind. `_apply_via_buffer` says so
        # (status=APPLIED_PARTIAL + dropped_fields); until now nobody
        # read it, so a mutilated rewind was announced as a full one.
        self._applied_partial = 0
        self._partial_dropped: List[str] = []
        # Per-group accumulators reset at every _begin_strict_for_layer
        # and merged into the globals only on commit success.
        self._strict_group_applied = 0
        self._strict_group_applied_partial = 0
        self._strict_group_dropped: List[str] = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0
        self._failed_postcommit_verification = 0
        self._strict_guard = None
        self._strict_mismatched = []
        # Life of the layer group being written, for the crash guards:
        #   _strict_group_open   the apply phase was entered (its actions
        #                        are in `_plan_actions`) and the group is
        #                        not closed yet; claimed by the ONE path
        #                        that closes it (commit or rollback), so a
        #                        second path can neither account it again
        #                        nor start a second chain.
        #   _strict_opened_edit  the run opened the edit session itself.
        #   _strict_command_open the run's edit command is open.
        #   _strict_commit_stage where `_commit_strict` stands, read by its
        #                        crash guard: before_commit, committing,
        #                        committed, refused, accounted.
        self._strict_group_open = False
        self._strict_opened_edit = False
        self._strict_command_open = False
        self._strict_commit_stage = ""
        self._strict_traces_flushed = False
        self._strict_cancelled_in_group = 0
        self._strict_external_committed = False
        self._strict_ext_slots: list = []
        # The group's writes that are in the file and must be journalled
        # (`_journal_committed_writes`).
        self._strict_commit_traces: List[int] = []

    def start(self) -> None:
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        self._started_at = time.monotonic()
        flog(f"{prefix}StrictRestoreRunner: start events={len(self._events)} "
             f"layers={len(self._groups)} strategy=STRICT")
        # No global suppression: the capture is suppressed per layer, when
        # the run takes a layer (_suppress_for_run). Suppressing everything
        # for a minutes-long, non-modal run dropped every commit the user
        # made meanwhile on any other layer.
        try:
            self._advance_group(guarded=False)
        except BaseException as exc:
            _abort_started_run(self, "StrictRestoreRunner", exc, self._trace_id)
            raise

    def cancel(self) -> None:
        self._cancelled = True
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        flog(f"{prefix}StrictRestoreRunner: cancel_requested "
             f"applied={self._total_ok}")

    def partial_restore_report(self) -> tuple:
        """RLU-053: ``(n_features, field_names)`` restored incompletely.

        The count and the abandoned column names of every feature the
        buffer wrote as APPLIED_PARTIAL, once the layers that carried
        them have committed (a rolled-back layer contributes nothing).
        The dialog reads this at the end of the run: it is what turns
        "restauree" into "restauree, sans ces champs". ``(0, ())`` when
        every captured field made it back.

        Kept on the runner rather than in the emitted
        ``GroupedRestoreResult``: that contract lives in
        core/workflow_service.py and is shared with the two other
        runners; extending it belongs to the module that owns it.
        """
        return (self._applied_partial, tuple(self._partial_dropped))

    def written_count(self) -> int:
        """Actions committed to disk so far."""
        return (self._applied + self._applied_partial
                + self._failed_postcommit_verification)

    def _cancelled_actions(self) -> int:
        """Actions of the layers a cancel left untouched or rolled back."""
        return self._strict_cancelled_in_group + sum(
            len(group) for _fp, group in self._groups[self._group_idx:]
        )

    def _outcome_breakdown(self) -> Dict[str, int]:
        """The outcome buckets, single source for the log and the dialog."""
        return {
            "applied": self._applied,
            "applied_partial": self._applied_partial,
            "skipped_idempotent": self._skipped_idempotent,
            "failed": self._failed_other,
            "failed_target_absent": self._failed_target_absent,
            "failed_geometry_drift": self._failed_geometry_drift,
            "failed_postcommit_verification": self._failed_postcommit_verification,
            "cancelled": self._cancelled_actions(),
        }

    def apply_report(self) -> Dict[str, int]:
        """RLU-055: the buckets the dialog must display, plan base included.

        Read by the dialog in the ``finished`` slot, i.e. on the numbers
        this runner has just published on CYCLE_SUMMARY, and derived by
        the very same function: the message the user re-reads and the
        line he greps say the same thing or neither does.

        Kept on the runner rather than in ``GroupedRestoreResult`` for
        the reason `partial_restore_report` gives above: that contract
        lives in core/workflow_service.py.
        """
        cancelled = self._cancelled_actions()
        return build_apply_report(
            self._outcome_breakdown(), self._plan_actions + cancelled,
        )

    @staticmethod
    def _merge_dropped(target: List[str], names) -> None:
        """Append the unseen names of *names* to *target*, order kept.

        The same column is dropped on every feature of a drifted layer;
        the user must read it once, not once per feature.
        """
        for name in names or ():
            text = str(name)
            if text and text not in target:
                target.append(text)

    def _bind_strict_external_commit_watch(self, layer) -> None:
        """Watch *layer* for a user Save while the run holds it in edit mode."""
        self._unbind_strict_external_commit_watch()
        self._strict_external_committed = False
        if layer is None:
            return
        step_entered = [False]
        try:
            lid = layer.id()
        except (RuntimeError, AttributeError):
            return

        def _on_before(*_args):
            if self._strict_commit_stage == "committing":
                return
            guard = self._strict_guard
            foreign = guard.foreign_changes() if guard is not None else []
            if not foreign and self._tracker is not None:
                enter = getattr(self._tracker, "enter_runner_step", None)
                if callable(enter):
                    enter(lid)
                    step_entered[0] = True

        def _on_after(*_args):
            if step_entered[0]:
                step_entered[0] = False
                leave = getattr(self._tracker, "leave_runner_step", None)
                if callable(leave):
                    leave(lid)
            if self._strict_commit_stage != "committing":
                self._strict_external_committed = True

        try:
            layer.beforeCommitChanges.connect(_on_before)
            layer.afterCommitChanges.connect(_on_after)
            self._strict_ext_slots = [
                (layer, layer.beforeCommitChanges, _on_before),
                (layer, layer.afterCommitChanges, _on_after),
            ]
        except (RuntimeError, AttributeError, TypeError):
            self._strict_ext_slots = []

    def _unbind_strict_external_commit_watch(self) -> None:
        slots, self._strict_ext_slots = getattr(self, "_strict_ext_slots", []), []
        for _lyr, sig, slot in slots:
            try:
                sig.disconnect(slot)
            except (RuntimeError, TypeError, AttributeError):
                pass

    def _end_strict_layer(self) -> None:
        """Give back the layer the run has just finished. Idempotent."""
        self._unbind_strict_external_commit_watch()
        self._strict_group_open = False
        self._strict_opened_edit = False
        self._strict_command_open = False
        _release_group_suppression(self)
        _close_group_journal(self)

    def _advance_group(self, guarded: bool = True) -> None:
        """Take the next layer group; from a QTimer tick, with a crash guard.

        The first group is opened by `start`, whose own handler closes the
        run and lets the caller see the exception (`guarded=False`). Every
        later group is opened from a QTimer tick, and an exception there
        (resolver, planner, provider, edit session) ended the chain without
        `_finish`: the dialog stayed "busy" until a project switch, the
        layer stayed suppressed and possibly in edit mode.
        """
        # Safety net: every end of a layer gives it back already.
        self._end_strict_layer()
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return

        fp, group = self._groups[self._group_idx]
        self._group_idx += 1
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        # RLU-057: announce the layer BEFORE the work that can hold the
        # thread for minutes, so the label names what is running and not
        # what has just finished.
        name_hint = (group[0].layer_name_snapshot or fp) if group else fp
        # What the crash guard reads, reset before anything can raise.
        self._strict_layer = None
        self._strict_layer_name = name_hint
        self._strict_plan = None
        try:
            self.layer_started.emit(
                name_hint, self._group_idx, len(self._groups), len(group),
            )
            self._open_strict_group(fp, group, prefix)
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            self._abandon_strict_open(group, prefix, exc, schedule=guarded)
            if not guarded:
                raise

    def _open_strict_group(self, fp: str, group: list, prefix: str) -> None:
        layer, errors = _resolve_runner_layer(
            self._find_layer_fn, group, fp,
            runner_name="StrictRestoreRunner",
            uncommitted_msg="Target layer has uncommitted edits; "
                            "commit or rollback before restore",
            trace_id=self._trace_id,
            runner_owner=self,
        )
        if layer is None:
            # BL-RW-P4-21: layer cannot be resolved -> entire group is
            # accounted as failed_other so the breakdown invariant holds.
            # RLU-054: and as many planned actions, so the failures it
            # adds stay inside the base the summary publishes.
            self._refuse_strict_group(group, errors)
            return

        self._begin_strict_for_layer(fp, group, layer, prefix)

    def _refuse_strict_group(self, group: list, errors) -> None:
        """A group refused before its apply phase: every event failed, named.

        Nothing of it was written. Its layer is given back at once and the
        next group is armed.
        """
        self._errors.extend([errors] if isinstance(errors, str) else errors)
        self._plan_actions += len(group)
        self._failed_other += len(group)
        self._total_fail += len(group)
        self._processed += len(group)
        self._end_strict_layer()
        _emit_progress(self, self._processed, len(self._events))
        QTimer.singleShot(0, self._advance_group)

    def _undo_strict_edit_session(self, prefix: str) -> None:
        """Drop what the run left in the layer's edit buffer. Never raises."""
        layer = self._strict_layer
        if layer is None:
            return
        with _runner_step_ctx(self, layer):
            if self._strict_command_open:
                self._strict_command_open = False
                try:
                    layer.destroyEditCommand()
                except Exception as exc:  # noqa: BLE001 - layer may be gone
                    flog(f"{prefix}StrictRestoreRunner: destroyEditCommand failed "
                         f"layer={self._strict_layer_name!r}: {exc}", "ERROR")
            if self._strict_opened_edit:
                self._strict_opened_edit = False
                try:
                    if layer.isEditable():
                        layer.rollBack()
                except Exception as exc:  # noqa: BLE001 - layer may be gone
                    flog(f"{prefix}StrictRestoreRunner: rollBack failed "
                         f"layer={self._strict_layer_name!r}: {exc}", "ERROR")
                    self._errors.append(
                        f"Couche '{self._strict_layer_name}' : le tampon "
                        f"d'edition du rewind n'a pas pu etre annule ({exc}) ; "
                        f"verifiez la couche avant de l'enregistrer")

    def _abandon_strict_open(self, group: list, prefix: str,
                             exc: BaseException, schedule: bool) -> None:
        """A crash while a layer group was being opened. Never raises.

        No chunk has run yet, so nothing of this group reached the file.
        Once the apply phase was entered (`_strict_group_open`), the
        rollback accounts the whole plan and undoes the edit session;
        before that, the group is refused whole. Either way the layer is
        given back and, from a QTimer tick, the next group is armed.
        """
        name = self._strict_layer_name or "?"
        flog(f"{prefix}StrictRestoreRunner: CRASH while opening "
             f"layer={name!r}: {exc}", "CRITICAL")
        message = (f"Internal error on layer '{name}' before any write: "
                   f"{exc}")
        if self._strict_group_open:
            self._errors.append(message)
            self._rollback_strict(prefix, "internal_crash_on_open",
                                  schedule=schedule)
            return
        self._undo_strict_edit_session(prefix)
        self._errors.append(message)
        self._plan_actions += len(group)
        self._failed_other += len(group)
        self._total_fail += len(group)
        self._processed += len(group)
        self._end_strict_layer()
        _emit_progress(self, self._processed, len(self._events))
        if schedule:
            QTimer.singleShot(0, self._advance_group)

    def _begin_strict_for_layer(
        self, fp: str, group: list, layer, prefix: str,
    ) -> None:
        """Prepare chunked strict restore for one layer (BL-PERF-009)."""
        from .core.restore_planner import plan_temporal_restore
        from .core.restore_executor import preflight_layer_check
        from .core.restore_contracts import PreflightVerdict
        from .core.restore_planner import preflight_check

        layer_name = group[0].layer_name_snapshot or fp
        self._strict_layer = layer
        self._strict_layer_name = layer_name
        self._strict_group_open = False
        self._strict_opened_edit = False
        self._strict_command_open = False
        self._strict_commit_stage = ""
        self._strict_traces_flushed = False
        self._strict_commit_traces = []
        plan = plan_temporal_restore(
            group, fp, layer_name, self._cutoff,
        )

        # ONE read connection for the whole group: the coverage guard below
        # reads it, then every action of the apply phase (key continuity).
        journal, unreadable = _open_group_journal(self, layer_name)
        if unreadable:
            # Fail-safe. The purge horizon the journal persisted for this
            # layer is what refuses a date a purge made unrebuildable after
            # a restart; unread, the rewind used to go on with this
            # session's memory only ("session horizon only", logged) and
            # replay a purged window as a complete one, announced restored.
            flog(f"{prefix}StrictRestoreRunner: journal_read unavailable "
                 f"layer={layer_name!r}: {unreadable}; layer refused",
                 "ERROR")
            self._refuse_strict_group(group, (
                f"Couche '{layer_name}' refusee : le journal ne peut pas "
                f"etre lu ({unreadable}), l'horizon de purge de cette couche "
                f"ne peut donc pas etre verifie. Revenir a cette date "
                f"pourrait rejouer un historique purge et produire un etat "
                f"que la couche n'a jamais eu. Rien n'a ete ecrit."))
            return
        if journal is None:
            flog(f"{prefix}StrictRestoreRunner: no journal given to this run "
                 f"layer={layer_name!r}: purge horizon of this session only, "
                 f"keys proven by rare content only", "WARNING")
        oldest = None
        if journal is not None:
            try:
                from .core.event_stream_repository import get_oldest_event_date
                oldest = get_oldest_event_date(journal, fp)
            except Exception as exc:  # noqa: BLE001 - an announcement only
                flog(f"{prefix}StrictRestoreRunner: first captured date "
                     f"unreadable ({exc})", "WARNING")
        # The durable coverage guard, on the path the UI really runs: the
        # purge horizon the journal persisted for THIS layer. A date before
        # the first captured event is not a purge gap (the history was
        # never recorded, not lost): it is announced, not refused.
        report = preflight_check(plan, conn=journal)
        cut = getattr(self._cutoff, "value", None)
        if oldest and isinstance(cut, str) and cut < oldest:
            self._extra_stats["coverage_before_first_capture"] = (
                self._extra_stats.get("coverage_before_first_capture", 0) + 1)
            flog(f"{prefix}StrictRestoreRunner: event=coverage_before_capture "
                 f"layer={layer_name} requested={cut} first_captured={oldest}: "
                 f"edits made before the journal existed are unknown to it",
                 "WARNING")
        if report.verdict == PreflightVerdict.BLOCKED:
            msg = "; ".join(report.blocking_reasons[:3])
            # BL-RW-P4-21: preflight blocked the whole group -> count
            # every action as failed_other.
            self._refuse_strict_group(group, f"Preflight blocked: {msg}")
            return

        layer_issues = preflight_layer_check(plan, layer)
        if layer_issues:
            msg = "; ".join(layer_issues[:3])
            # BL-RW-P4-21: layer-level preflight refused the layer ->
            # whole group is failed_other.
            self._refuse_strict_group(group, f"Layer check failed: {msg}")
            return

        events_by_id = {e.event_id: e for e in group}

        self._strict_plan = plan
        self._strict_events_by_id = events_by_id
        self._strict_action_idx = 0
        self._strict_succeeded = []
        self._strict_skipped = []
        self._strict_was_editing = layer.isEditable()
        self._strict_apply_start = time.monotonic()
        self._strict_fid_remap = {}
        # BL-RW-P4-21: reset per-group bucket accumulators.
        self._strict_group_applied = 0
        self._strict_group_applied_partial = 0
        self._strict_group_dropped = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0

        if not self._strict_was_editing:
            with _runner_step_ctx(self, layer):
                started = layer.startEditing()
            if not started:
                flog(f"{prefix}StrictRestoreRunner: startEditing failed", "ERROR")
                self._refuse_strict_group(group, "Cannot start editing on layer")
                return
            self._strict_opened_edit = True
        self._bind_strict_external_commit_watch(layer)

        from .core.commit_guard import CommitGuard
        # Records which features each action touches, so the commit can be
        # verified by reading the file back (and compensated if refused).
        self._strict_guard = CommitGuard(layer, self._trace_id)
        self._strict_mismatched = []

        # RLU-054: from here the layer enters the apply phase, so every
        # action of ITS plan will land in exactly one outcome bucket. The
        # events the planner refused (no stable identity) are counted
        # apart: they are neither applied, nor skipped at runtime, nor
        # failed, and folding them into any of those was part of what made
        # the summary add up to twice the plan.
        n_actions = len(plan.actions)
        self._plan_actions += n_actions
        self._plan_skipped += max(len(group) - n_actions, 0)
        # From here exactly one path closes the group: the commit or the
        # rollback, which claims it (see `_rollback_strict`).
        self._strict_group_open = True

        count_before = layer.featureCount()
        self._strict_count_before = count_before

        # Pre-fetch geom snapshots for the forensic EDIT_START log. This is
        # the single biggest cause of the UI freeze during a rewind: it
        # does one provider.getFeatures() per UPDATE event before the first
        # chunk runs. Gate it on the RECOVERLAND_HEAVY_DIAG opt-in.
        n_update_events = sum(
            1 for a in group
            if getattr(a, 'new_geometry_wkb', None)
        )
        if heavy_diag_enabled():
            upd_geom_state = {}
            for a in group:
                wkb = getattr(a, 'new_geometry_wkb', None)
                if not wkb:
                    continue
                try:
                    fid = json.loads(a.feature_identity_json).get('fid')
                    if fid is not None:
                        upd_geom_state[fid] = feature_geom_short_repr(layer, fid)
                except Exception as exc:
                    flog(f"EDIT_START err eid={getattr(a, 'event_id', '?')} exc={exc}", "WARNING")
            flog(f"EDIT_START layer={layer_name} n_geom_upd={len(upd_geom_state)} geom_at_start={upd_geom_state}")
        else:
            flog(f"EDIT_START layer={layer_name} n_geom_upd={n_update_events} geom_at_start=diag_off")

        layer.beginEditCommand("RecoverLand: temporal restore")
        self._strict_command_open = True

        ops = {}
        for a in plan.actions:
            ops[a.compensatory_op] = ops.get(a.compensatory_op, 0) + 1
        flog(f"{prefix}StrictRestoreRunner: begin_strict "
             f"layer={layer_name} actions={plan.event_count} "
             f"compensatory={ops} feat_count_before={count_before}")
        QTimer.singleShot(0, self._process_strict_chunk)

    def _process_strict_chunk(self) -> None:
        """Process a chunk of strict restore actions (BL-PERF-009)."""
        try:
            self._process_strict_chunk_inner()
        except Exception as exc:
            prefix = f"[{self._trace_id}] " if self._trace_id else ""
            flog(f"{prefix}StrictRestoreRunner: CRASH in chunk: {exc}", "CRITICAL")
            self._errors.append(f"Internal error: {exc}")
            self._rollback_strict(prefix, "internal_crash")

    def _process_strict_chunk_inner(self) -> None:
        """Inner chunk logic, called by _process_strict_chunk with crash guard."""
        prefix = f"[{self._trace_id}] " if self._trace_id else ""
        plan = self._strict_plan
        events_by_id = self._strict_events_by_id
        layer = self._strict_layer
        actions = plan.actions
        start = self._strict_action_idx
        chunk_t0 = time.monotonic()

        if (getattr(self, "_strict_external_committed", False)
                or (not self._strict_was_editing and not layer.isEditable())):
            self._handle_external_commit_strict(prefix)
            return
        if self._cancelled:
            self._rollback_strict(prefix, "cancel_requested")
            return
        if self._foreign_edits_abort(prefix):
            return

        i = start
        while i < len(actions):
            elapsed_ms = (time.monotonic() - chunk_t0) * 1000.0
            if _chunk_should_yield(elapsed_ms, i - start):
                break
            action = actions[i]
            i += 1
            event = events_by_id.get(action.event_id)
            if event is None:
                self._errors.append(f"Evt {action.event_id}: Event data not found")
                self._rollback_strict(prefix, "event_not_found")
                return

            guard = self._strict_guard
            if guard is not None:
                guard.begin_action(action.event_id)
            with _runner_step_ctx(self, layer):
                result = _apply_via_buffer(
                    layer, action.compensatory_op, event,
                    self._strict_fid_remap,
                    trace_id=self._trace_id,
                    journal=self.__dict__.get("_group_journal"),
                )
            if guard is not None:
                guard.end_action(action.event_id, [result.get("fid")])
            # BL-RW-P3-18: classify the action result for the breakdown.
            from .core.restore_service import _classify_restore_result
            reason = _classify_restore_result(result)

            if result["success"]:
                skipped = result.get("skipped", False)
                flog(f"{prefix}StrictRestoreRunner: action_ok "
                     f"eid={action.event_id} "
                     f"comp_op={action.compensatory_op} "
                     f"orig_op={action.operation_type} "
                     f"skipped={skipped} reason={reason} "
                     f"fid={result.get('fid', '?')}")
                if skipped:
                    self._strict_skipped.append(action.event_id)
                else:
                    self._strict_succeeded.append(action.event_id)
                    self._all_succeeded_ids.add(action.event_id)
                # BL-RW-P4-21: route bucket increments through the
                # per-group accumulators so a later rollback can
                # discard them cleanly.
                if reason == "target_absent":
                    self._strict_group_failed_target_absent += 1
                elif reason == "geometry_drift":
                    self._strict_group_failed_geometry_drift += 1
                elif reason == "skipped_idempotent":
                    self._strict_group_skipped_idempotent += 1
                elif reason == "applied_partial":
                    # RLU-053: the buffer wrote the feature, but not every
                    # captured field. Counting it with the complete ones
                    # is what let a hybrid entity be announced as
                    # "restored"; it gets its own bucket and its fields
                    # are named to the user at the end of the run.
                    self._strict_group_applied_partial += 1
                    self._merge_dropped(self._strict_group_dropped,
                                        result.get("dropped_fields"))
                    flog(f"{prefix}StrictRestoreRunner: applied_partial "
                         f"eid={action.event_id} "
                         f"dropped={list(result.get('dropped_fields') or ())} "
                         f"msg={result['message']}", "WARNING")
                else:
                    self._strict_group_applied += 1
            else:
                # BL-RW-P4-22: distinguish soft failures (target_absent,
                # geometry_drift) from hard failures. Soft failures are
                # bounded to a single action that left the buffer state
                # untouched, so the batch can keep processing the
                # remaining actions instead of triggering a layer-wide
                # rollback that would discard already-applied valid
                # compensations.
                soft_fail = reason in ("target_absent", "geometry_drift")
                log_level = "WARNING" if soft_fail else "ERROR"
                flog(f"{prefix}StrictRestoreRunner: action_failed "
                     f"eid={action.event_id} comp_op={action.compensatory_op} "
                     f"orig_op={action.operation_type} reason={reason} "
                     f"soft={soft_fail} msg={result['message']}",
                     log_level)
                self._errors.append(
                    f"Evt {action.event_id}: {result['message']}")
                if reason == "target_absent":
                    self._strict_group_failed_target_absent += 1
                elif reason == "geometry_drift":
                    self._strict_group_failed_geometry_drift += 1
                else:
                    self._strict_group_failed_other += 1
                if soft_fail:
                    # Skip the offending action; the buffer is untouched
                    # because _buffer_update/_buffer_delete return early
                    # before any layer mutation when target is absent or
                    # geometry drift is detected. The breakdown counter
                    # already recorded the failure reason; the commit
                    # path will fold it into _total_fail.
                    continue
                self._rollback_strict(prefix, "apply_failed")
                return

        end = i
        self._strict_action_idx = end
        self._processed += (end - start)
        self.progress.emit(self._processed, len(self._events))

        chunk_elapsed_ms = (time.monotonic() - chunk_t0) * 1000.0
        if chunk_elapsed_ms > _CHUNK_SLOW_MS:
            flog(f"{prefix}CHUNK_SLOW layer={self._strict_layer_name!r} "
                 f"actions={start}..{end} n={end - start} "
                 f"elapsed_ms={chunk_elapsed_ms:.0f}", "WARNING")

        if self._strict_action_idx >= len(actions):
            self._commit_strict(prefix)
        else:
            QTimer.singleShot(0, self._process_strict_chunk)

    def _handle_external_commit_strict(self, prefix: str) -> None:
        """Handle a user Save that committed the run's partial buffer mid-layer."""
        if not self._strict_group_open:
            return
        self._strict_group_open = False
        self._strict_command_open = False
        self._strict_opened_edit = False
        plan = self._strict_plan
        total_actions = len(plan.actions) if plan is not None else 0
        ok_count = len(self._strict_succeeded)
        skipped_count = len(self._strict_skipped)
        remaining = max(total_actions - self._strict_action_idx, 0)
        soft_fails = (
            self._strict_group_failed_target_absent
            + self._strict_group_failed_geometry_drift
            + self._strict_group_failed_other
        )
        self._total_ok += ok_count + skipped_count
        self._applied += self._strict_group_applied
        self._applied_partial += self._strict_group_applied_partial
        self._merge_dropped(self._partial_dropped, self._strict_group_dropped)
        self._skipped_idempotent += self._strict_group_skipped_idempotent
        self._failed_target_absent += self._strict_group_failed_target_absent
        self._failed_geometry_drift += self._strict_group_failed_geometry_drift
        self._failed_other += self._strict_group_failed_other + remaining
        self._total_fail += soft_fails + remaining
        self._processed += remaining
        self._reset_group_buckets()
        msg = (
            f"Couche '{self._strict_layer_name}' enregistree par l'utilisateur "
            f"pendant le rewind : {ok_count} compensation(s) deja dans le "
            f"tampon ont ete ecrites dans le fichier, {remaining} action(s) "
            f"restante(s) non executee(s)"
        )
        flog(
            f"{prefix}StrictRestoreRunner: external_commit_detected "
            f"layer={self._strict_layer_name!r} applied={ok_count} "
            f"remaining={remaining}",
            "ERROR",
        )
        self._errors.append(msg)
        self._strict_commit_traces = list(self._strict_succeeded)
        self._journal_committed_writes(prefix)
        _emit_progress(self, self._processed, len(self._events))
        self._end_strict_layer()
        QTimer.singleShot(0, self._advance_group)

    def _rollback_strict(self, prefix: str, reason: str,
                         schedule: bool = True) -> None:
        """Rollback current strict restore layer. Never kills the chain."""
        if not self._strict_group_open:
            flog(f"{prefix}StrictRestoreRunner: rollback reason={reason} on "
                 f"a group already closed: nothing to undo, next step "
                 f"already armed", "WARNING")
            return
        self._strict_group_open = False
        layer = self._strict_layer
        plan = self._strict_plan
        succeeded_count = len(self._strict_succeeded)
        total_actions = len(plan.actions) if plan is not None else 0
        failed_count = total_actions - succeeded_count

        flog(f"{prefix}StrictRestoreRunner: rollback reason={reason} "
             f"applied={succeeded_count} remaining={failed_count}", "WARNING")

        for eid in self._strict_succeeded:
            self._all_succeeded_ids.discard(eid)

        if reason == "cancel_requested":
            self._plan_actions = max(0, self._plan_actions - total_actions)
            self._strict_cancelled_in_group += total_actions
            self._reset_group_buckets()
        else:
            preserved_target_absent = self._strict_group_failed_target_absent
            preserved_geometry_drift = self._strict_group_failed_geometry_drift
            self._failed_target_absent += preserved_target_absent
            self._failed_geometry_drift += preserved_geometry_drift
            remainder_other = (
                total_actions - preserved_target_absent - preserved_geometry_drift
            )
            self._failed_other += max(remainder_other, 0)
            self._reset_group_buckets()
            self._total_fail += total_actions

        remaining = max(total_actions - self._strict_action_idx, 0)
        self._processed += remaining

        try:
            self._undo_strict_edit_session(prefix)
            if layer is not None:
                try:
                    layer.triggerRepaint()
                except Exception as exc:  # noqa: BLE001 - layer may be gone
                    flog(f"{prefix}StrictRestoreRunner: repaint after "
                         f"rollback failed: {exc}", "WARNING")
            _emit_progress(self, self._processed, len(self._events))
        finally:
            self._end_strict_layer()
            if schedule:
                QTimer.singleShot(0, self._advance_group)

    def _log_post_commit_state(self, layer, layer_name: str,
                               prefix: str) -> None:
        """Log target geometry vs persisted geometry for each succeeded UPDATE.

        For every UPDATE compensatory action that returned success during
        the apply phase, read the feature back from the provider AFTER the
        commit and compare the persisted geometry with the OLD WKB written
        in the audit event. This is the ground-truth check: if the
        persisted geometry does not match the target, the commit silently
        re-applied the buffer state or the provider rejected the geom
        update without flagging an error.

        Gated on RECOVERLAND_HEAVY_DIAG: each iteration does one
        provider.getFeatures() and we run the loop on the UI thread.
        Skipped by default to keep the rewind responsive.
        """
        if not heavy_diag_enabled():
            flog(f"{prefix}TRACE_POST_COMMIT: layer={layer_name} skipped "
                 f"(set RECOVERLAND_HEAVY_DIAG=1 to enable per-event check)")
            return
        try:
            import json
        except ImportError:
            return
        succeeded = set(self._strict_succeeded)
        if not succeeded:
            return
        plan = getattr(self, '_strict_plan', None)
        events_by_id = getattr(self, '_strict_events_by_id', {})
        if plan is None:
            return
        check_count = 0
        mismatch_count = 0
        for action in plan.actions:
            if action.compensatory_op != "UPDATE":
                continue
            if action.event_id not in succeeded:
                continue
            event = events_by_id.get(action.event_id)
            if event is None or event.geometry_wkb is None:
                continue
            try:
                identity = json.loads(event.feature_identity_json)
            except (json.JSONDecodeError, TypeError, ValueError):
                identity = {}
            fid = identity.get("fid")
            if fid is None:
                continue
            persisted = feature_geom_short_repr(layer, int(fid))
            target = wkb_short_repr(event.geometry_wkb)
            match = persisted == target
            if not match:
                mismatch_count += 1
            flog(f"{prefix}TRACE_POST_COMMIT: layer={layer_name} "
                 f"eid={event.event_id} fid={fid} "
                 f"target_OLD={target} "
                 f"persisted={persisted} "
                 f"match={match}")
            check_count += 1
        if check_count:
            flog(f"{prefix}TRACE_POST_COMMIT: layer={layer_name} "
                 f"checked={check_count} mismatches={mismatch_count}")

    def _foreign_edits_abort(self, prefix: str) -> bool:
        """Stop the run if the user edited the layer it holds in edit mode.

        Between two QTimer ticks the layer is in edit mode under the run's
        control. Anything the user changed there would be committed with
        the restore and, the layer being suppressed, never journalled. The
        run is rolled back instead and says why: nothing written, nothing
        silently mixed.
        """
        guard = getattr(self, "_strict_guard", None)
        if guard is None:
            return False
        try:
            foreign = guard.foreign_changes()
        except Exception as exc:  # noqa: BLE001 - detection must not crash the run
            flog(f"{prefix}StrictRestoreRunner: foreign edit check failed: {exc}",
                 "WARNING")
            return False
        if not foreign:
            return False
        msg = (f"La couche '{self._strict_layer_name}' a ete modifiee pendant la "
               f"restauration (entites {foreign[:10]}) : restauration de cette "
               f"couche annulee, rien n'a ete ecrit, et les modifications non "
               f"enregistrees saisies sur cette couche ont ete annulees par le rollback")
        flog(f"{prefix}StrictRestoreRunner: event=foreign_edit_detected "
             f"layer={self._strict_layer_name!r} fids={foreign[:20]}", "ERROR")
        self._errors.append(msg)
        self._rollback_strict(prefix, "foreign_edit")
        return True

    def _flush_layer_traces(self, traces, prefix: str) -> None:
        """Hand a committed layer's traces to the journal right away."""
        if not traces:
            return
        wq = self._write_queue
        if wq is None:
            self._traces.extend(traces)
            return
        outcome = wq.enqueue(list(traces),
                             journal_path=getattr(self, "_journal_path", None))
        self._traces_flushed = getattr(self, "_traces_flushed", 0) + len(traces)
        if not outcome:
            msg = (f"Journal trace write failed for {len(traces)} restore "
                   f"trace(s) ({getattr(outcome, 'value', outcome)}): the "
                   f"data changed, the journal does not know it")
            self._errors.append(msg)
            flog(f"{prefix}StrictRestoreRunner: {msg}", "CRITICAL")
        else:
            flog(f"{prefix}StrictRestoreRunner: traces_persisted "
                 f"n={len(traces)} outcome={getattr(outcome, 'value', outcome)}")

    def _commit_strict(self, prefix: str) -> None:
        """Commit current strict restore layer, with a crash guard.

        It runs inside the chunk guard, whose answer to any exception is a
        rollback: a crash AFTER `commitChanges()` (verdict, traces, reload)
        was taken for one before it. The layer's writes, already in the
        file, were counted failed, none of their traces reached the journal
        (the next rewind replays them), and a second exception in that
        rollback ended the chain with the dialog "busy" and the layer
        suppressed. `_commit_crashed` answers for the stage the commit had
        reached (`_strict_commit_stage`).
        """
        self._strict_commit_stage = "before_commit"
        try:
            self._commit_strict_inner(prefix)
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            self._commit_crashed(prefix, exc)

    def _commit_strict_inner(self, prefix: str) -> None:
        layer = self._strict_layer
        layer_name = self._strict_layer_name
        apply_ms = int((time.monotonic() - self._strict_apply_start) * 1000)

        if (getattr(self, "_strict_external_committed", False)
                or (not self._strict_was_editing and not layer.isEditable())):
            self._handle_external_commit_strict(prefix)
            return

        layer.endEditCommand()
        self._strict_command_open = False

        if not self._strict_was_editing:
            commit_start = time.monotonic()
            eb = layer.editBuffer()
            buf_add = len(eb.addedFeatures()) if eb else -1
            buf_del = len(eb.deletedFeatureIds()) if eb else -1
            buf_attr = len(eb.changedAttributeValues()) if eb else -1
            buf_geom = len(eb.changedGeometries()) if eb else -1
            flog(f"{prefix}StrictRestoreRunner: commit_started "
                 f"layer={layer_name} "
                 f"buf_add={buf_add} buf_del={buf_del} "
                 f"buf_attr={buf_attr} buf_geom={buf_geom}")
            from .core.commit_guard import (
                EV_LANDED_NOT_COMPENSATED, EV_FAILED_POSTCOMMIT_VERIFICATION,
            )
            if self._foreign_edits_abort(prefix):
                return
            guard = self._strict_guard
            guard.prepare()
            self._strict_commit_stage = "committing"
            with _runner_step_ctx(self, layer):
                ok = layer.commitChanges()
            self._strict_commit_stage = "committed" if ok else "refused"
            verdict = guard.finish(ok)
            if not verdict.committed:
                self._strict_commit_stage = "refused"
                errors = verdict.commit_errors
                msg = "; ".join(errors) if errors else "Commit failed"
                flog(f"{prefix}StrictRestoreRunner: commit_failed "
                     f"verdict={verdict.state} msg={msg}", "ERROR")
                # What the refused commit wrote was reverted and verified
                # (core.commit_guard). What could NOT be reverted is in the
                # file: it is journalled and kept in the undo scope, and
                # the user is told, never "0 restored" over changed data.
                left = set(verdict.events_with(EV_LANDED_NOT_COMPENSATED))
                for eid in self._strict_succeeded:
                    if eid not in left:
                        self._all_succeeded_ids.discard(eid)
                for eid in left:
                    self._all_succeeded_ids.add(eid)
                # BL-RW-P4-21: commit failure rolls back every action
                # in the layer; per-group bucket accumulators no longer
                # represent reality. Credit the whole layer as
                # failed_other and discard the per-group counters.
                # Claimed here: from now on the group is accounted.
                self._strict_group_open = False
                self._strict_commit_stage = "refused_accounted"
                total_actions = len(self._strict_plan.actions)
                # The writes the compensation could not revert ARE in the
                # file, traced and in the undo scope: counting them failed
                # announced "0 restored" over data the run had changed.
                landed = min(len(left), total_actions)
                self._applied += landed
                self._total_ok += landed
                self._failed_other += total_actions - landed
                self._reset_group_buckets()
                self._total_fail += total_actions - landed
                self._strict_commit_traces = sorted(left)
                if left:
                    self._errors.append(
                        f"Commit failed ({verdict.state}): {len(left)} "
                        f"write(s) stayed in the file and are journalled: "
                        f"{verdict.left_modified[:5]}")
                self._errors.append(
                    f"Commit failed ({verdict.state}): {msg}")
                if layer.isEditable():
                    layer.rollBack()
                self._journal_committed_writes(prefix)
                layer.triggerRepaint()
                self._close_committed_layer()
                return
            commit_ms = int((time.monotonic() - commit_start) * 1000)
            flog(f"{prefix}StrictRestoreRunner: commit_done "
                 f"layer={layer_name} commit_elapsed_ms={commit_ms} "
                 f"verdict={verdict.state}")
            self._strict_mismatched = [
                eid for eid in verdict.events_with(EV_FAILED_POSTCOMMIT_VERIFICATION)
                if eid in self._strict_succeeded]
            for eid in self._strict_mismatched:
                self._errors.append(
                    f"Evt {eid}: FAILED_POSTCOMMIT_VERIFICATION: "
                    f"{verdict.per_event_detail.get(eid, '')}")

        mismatched = list(getattr(self, "_strict_mismatched", []) or [])
        ok_count, skipped_count = self._account_committed_group(
            prefix, layer_name, mismatched)

        # Every committed write is journalled, verified or not: the data
        # changed and a later rewind must know it. Persisted NOW, layer by
        # layer: a crash later in a multi-layer run must not lose the
        # traces of layers already committed (the next rewind would replay
        # them as if they never happened).
        self._journal_committed_writes(prefix)

        self._log_post_commit_state(layer, layer_name, prefix)

        count_after = layer.featureCount()
        count_before = getattr(self, '_strict_count_before', -1)
        if ok_count > 0:
            layer.reload()
            count_after_reload = layer.featureCount()
        else:
            count_after_reload = count_after
        layer.triggerRepaint()

        if skipped_count > 0:
            flog(f"{prefix}StrictRestoreRunner: skipped "
                 f"layer={layer_name} count={skipped_count} "
                 f"reason=target_absent_or_identity_mismatch", "WARNING")

        flog(f"{prefix}StrictRestoreRunner: layer={layer_name} "
             f"ok={ok_count} apply_elapsed_ms={apply_ms} "
             f"feat_before={count_before} feat_after_commit={count_after} "
             f"feat_after_reload={count_after_reload}")
        self._close_committed_layer()

    def _reset_group_buckets(self) -> None:
        self._strict_group_applied = 0
        self._strict_group_applied_partial = 0
        self._strict_group_dropped = []
        self._strict_group_skipped_idempotent = 0
        self._strict_group_failed_other = 0
        self._strict_group_failed_target_absent = 0
        self._strict_group_failed_geometry_drift = 0

    def _account_committed_group(self, prefix: str, layer_name: str,
                                 mismatched: list) -> tuple:
        """Merge a committed group into the run's buckets. Claims the group.

        *mismatched*: the succeeded actions whose write is in the file but
        NOT proven (the read-back contradicts it, or never ran): counted in
        `failed_postcommit_verification`, never as restored, and still
        journalled and undoable. Returns ``(ok_count, skipped_count)``.
        """
        self._strict_group_open = False
        self._strict_commit_stage = "accounted"
        ok_count = len(self._strict_succeeded) - len(mismatched)
        skipped_count = len(self._strict_skipped)
        # BL-RW-P4-21: skipped-idempotent events are successes per the
        # audit_backend contract (`succeeded includes idempotent skips`)
        # and must contribute to total_ok so the breakdown invariant
        # bucket_sum == total_ok + total_fail holds.
        self._total_ok += ok_count + skipped_count
        # BL-RW-P4-22: soft failures (target_absent, geometry_drift,
        # failed_other) accumulated during this layer batch must be
        # credited to _total_fail so the invariant
        # bucket_sum == total_ok + total_fail holds even when the layer
        # commits with partial soft-fail isolation enabled. We compute
        # the delta from total_actions - (ok + skipped) to avoid the
        # double-count that would happen if we naively summed the
        # bucket accumulators: _buffer_update can return success=True,
        # skipped=True, reason_code=geometry_drift (drift skipped case),
        # which legitimately increments BOTH the skipped accumulator
        # (via _strict_skipped) and the failed_geometry_drift bucket
        # (for the breakdown). Counting that event as both ok and fail
        # would break the invariant; treating it as ok (already in
        # skipped_count) is the right call here.
        total_actions = len(self._strict_plan.actions)
        soft_fail_count = max(0, total_actions - ok_count - skipped_count)
        self._total_fail += soft_fail_count
        # BL-RW-P4-21: commit succeeded -> the per-group bucket
        # accumulators reflect the final state and can be merged into
        # the global breakdown counters.
        # A write the read-back contradicts leaves the applied buckets for
        # its own: taken from `applied` first, then from `applied_partial`.
        n_mis = len(mismatched)
        take = min(n_mis, self._strict_group_applied)
        self._strict_group_applied -= take
        self._strict_group_applied_partial -= min(
            n_mis - take, self._strict_group_applied_partial)
        self._failed_postcommit_verification += n_mis
        self._applied += self._strict_group_applied
        # RLU-053: the incomplete writes are on disk now, so their count
        # and their abandoned columns become part of the run report.
        self._applied_partial += self._strict_group_applied_partial
        self._merge_dropped(self._partial_dropped, self._strict_group_dropped)
        self._skipped_idempotent += self._strict_group_skipped_idempotent
        self._failed_other += self._strict_group_failed_other
        self._failed_target_absent += self._strict_group_failed_target_absent
        self._failed_geometry_drift += self._strict_group_failed_geometry_drift
        flog(f"{prefix}StrictRestoreRunner: commit_breakdown "
             f"layer={layer_name} ok={ok_count} skipped={skipped_count} "
             f"soft_fail={soft_fail_count} "
             f"applied_partial={self._strict_group_applied_partial} "
             f"dropped_fields={self._strict_group_dropped} "
             f"failed_target_absent={self._strict_group_failed_target_absent} "
             f"failed_geometry_drift={self._strict_group_failed_geometry_drift} "
             f"failed_other={self._strict_group_failed_other}")
        self._reset_group_buckets()
        self._strict_commit_traces = list(self._strict_succeeded)
        return ok_count, skipped_count

    def _journal_committed_writes(self, prefix: str) -> None:
        """Journal the group's writes that are in the file. Once per group.

        `_strict_commit_traces` names them: every succeeded action of a
        committed layer, or what a refused commit could not take back. A
        trace that cannot be built is named, never skipped in silence: the
        file changed and the journal does not say so.
        """
        if self._strict_traces_flushed:
            return
        self._strict_traces_flushed = True
        layer = self._strict_layer
        traces = []
        for eid in list(self._strict_commit_traces or ()):
            event = self._strict_events_by_id.get(eid)
            if event is None:
                continue
            try:
                trace = build_restore_trace_event(event, layer)
            except Exception as exc:  # noqa: BLE001 - named below, run goes on
                trace = None
                flog(f"{prefix}StrictRestoreRunner: trace eid={eid} not "
                     f"built: {exc}", "ERROR")
            if trace is not None:
                traces.append(trace)
            else:
                self._errors.append(
                    f"Evt {eid}: ecrit dans le fichier, mais aucune trace "
                    f"n'a pu etre construite pour le journal")
        try:
            self._flush_layer_traces(traces, prefix)
        except Exception as exc:  # noqa: BLE001 - named, the run goes on
            msg = (f"Journal trace write failed for {len(traces)} restore "
                   f"trace(s) ({exc}): the data changed, the journal does "
                   f"not know it")
            self._errors.append(msg)
            flog(f"{prefix}StrictRestoreRunner: {msg}", "CRITICAL")

    def _close_committed_layer(self) -> None:
        """The commit path is done with the layer: give it back, arm the next."""
        self._strict_commit_stage = "done"
        self._end_strict_layer()
        QTimer.singleShot(0, self._advance_group)

    def _commit_crashed(self, prefix: str, exc: BaseException) -> None:
        """A crash inside `_commit_strict`, answered for its stage. Never raises.

        before_commit      nothing reached the file: the rollback of any
                           other failure (claims the group, arms the next).
        committing         `commitChanges()` itself raised (a layer deleted
                           under the run): what the file holds is unknown,
                           said as such; nothing is journalled or undoable
                           on a guess.
        committed          the provider accepted the commit, its
                           verification or accounting crashed: the writes
                           are in the file, so they are journalled and stay
                           undoable, but counted unverified
                           (`failed_postcommit_verification`), never
                           restored.
        refused            the provider refused, the compensation's
                           account crashed: unknown, said as such.
        accounted, refused_accounted
                           the buckets are right; what is missing (traces,
                           refresh) is completed or named.
        done               the next step was already armed.
        """
        stage = self._strict_commit_stage
        name = self._strict_layer_name
        flog(f"{prefix}StrictRestoreRunner: CRASH in commit stage={stage} "
             f"layer={name!r}: {exc}", "CRITICAL")
        if stage == "done":
            return
        if stage == "before_commit":
            self._errors.append(
                f"Internal error before the commit of layer '{name}': {exc}")
            self._rollback_strict(prefix, "internal_crash_before_commit")
            return
        try:
            if self._strict_group_open:
                self._account_crashed_commit(prefix, stage, exc)
            else:
                self._errors.append(
                    f"Couche '{name}' : erreur interne apres le commit "
                    f"({exc}) ; le compte rendu de la couche est etabli, "
                    f"verifiez son affichage")
            if stage in ("committed", "accounted", "refused_accounted"):
                self._journal_committed_writes(prefix)
        except Exception as inner:  # noqa: BLE001 - the chain must go on
            flog(f"{prefix}StrictRestoreRunner: commit crash handling "
                 f"failed layer={name!r}: {inner}", "CRITICAL")
            self._errors.append(
                f"Couche '{name}' : erreur interne pendant la reprise du "
                f"commit ({inner}) ; verifiez la couche et le journal")
        finally:
            layer = self._strict_layer
            try:
                if layer is not None:
                    layer.triggerRepaint()
            except Exception as repaint_exc:  # noqa: BLE001 - layer may be gone
                flog(f"{prefix}StrictRestoreRunner: repaint after commit "
                     f"crash failed: {repaint_exc}", "WARNING")
            self._close_committed_layer()

    def _account_crashed_commit(self, prefix: str, stage: str,
                                exc: BaseException) -> None:
        """Buckets of a group whose commit crashed before it was accounted."""
        name = self._strict_layer_name
        if stage == "committed":
            unverified = list(self._strict_succeeded)
            self._strict_mismatched = unverified
            self._account_committed_group(prefix, name, unverified)
            self._errors.append(
                f"Couche '{name}' : ecrite dans le fichier, mais sa "
                f"verification a echoue ({exc}) : {len(unverified)} "
                f"ecriture(s) non verifiee(s), journalisee(s) et "
                f"annulable(s) ; verifiez la couche")
            return
        # committing / refused: what the file holds is not known.
        self._strict_group_open = False
        self._strict_commit_stage = "refused_accounted"
        total_actions = len(self._strict_plan.actions)
        for eid in self._strict_succeeded:
            self._all_succeeded_ids.discard(eid)
        self._failed_other += total_actions
        self._total_fail += total_actions
        self._reset_group_buckets()
        self._strict_commit_traces = []
        self._strict_traces_flushed = True
        layer = self._strict_layer
        try:
            if layer is not None and layer.isEditable():
                layer.rollBack()
        except Exception as rb_exc:  # noqa: BLE001 - layer may be gone
            flog(f"{prefix}StrictRestoreRunner: rollBack after commit crash "
                 f"failed: {rb_exc}", "ERROR")
        self._errors.append(
            f"Couche '{name}' : le commit a echoue sur une erreur interne "
            f"({exc}) ; ce que le fichier contient n'a pas pu etre etabli. "
            f"Rien n'a ete journalise pour cette couche : verifiez-la avant "
            f"de continuer a travailler dessus")

    def _finish(self) -> None:
        status = "cancelled" if self._cancelled else (
            "completed" if self._total_fail == 0 else "partial")
        succeeded_by_ds: Dict[str, list] = {}
        for fp, events in self._by_ds.items():
            ok_events = [e for e in events
                         if e.event_id in self._all_succeeded_ids]
            if ok_events:
                succeeded_by_ds[fp] = ok_events
        excluded_from_undo = sum(len(v) for v in self._by_ds.values()) - sum(
            len(v) for v in succeeded_by_ds.values())
        if excluded_from_undo > 0:
            flog(f"StrictRestoreRunner: by_ds filtered: "
                 f"{excluded_from_undo} event(s) excluded from undo scope")
        # RLU-054: `excluded_from_undo` used to be published as
        # `apply_skipped`. It is an EVENT-level number (every event that
        # did not fully succeed, whatever the reason) and it was printed
        # next to the ACTION-level apply_ok/apply_fail, which already
        # totalled the plan: 73 actions came out as 14+67+59 = 140
        # results. It stays a log line about the undo scope, which is
        # what it measures; the summary now publishes the buckets.
        breakdown = self._outcome_breakdown()
        cancelled = breakdown["cancelled"]
        if cancelled:
            flog(f"StrictRestoreRunner: cancelled "
                 f"actions_never_attempted={cancelled} "
                 f"groups_left={len(self._groups) - self._group_idx}",
                 "WARNING")
        merged_extra: Dict[str, int] = dict(self._extra_stats)
        merged_extra["plan_skipped"] = self._plan_skipped
        if self._applied_partial:
            # RLU-053: named once per run, before the dialog reads them,
            # so the log tells the same story as the message the user
            # gets even when the dialog is closed mid-run.
            flog(f"StrictRestoreRunner: partial_restore "
                 f"features={self._applied_partial} "
                 f"dropped_fields={self._partial_dropped}", "WARNING")
        _finish_runner(
            self,
            runner_name="StrictRestoreRunner",
            by_ds=succeeded_by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=self._traces,
            write_queue=self._write_queue,
            started_at=self._started_at,
            tracker=self._tracker,
            trace_id=self._trace_id,
            status_label=status,
            extra_stats=merged_extra,
            plan_actions=self._plan_actions + cancelled,
            breakdown=breakdown,
        )


class UndoRunner(QObject):
    """Stepped undo executor (same pattern as RestoreRunner)."""

    progress = pyqtSignal(int, int)
    # RLU-057: same contract as StrictRestoreRunner.layer_started. The
    # 139 s auto-undo the user watched was described by the label of the
    # phase before it; the layer names are what make the wait readable.
    layer_started = pyqtSignal(str, int, int, int)
    finished = pyqtSignal(object)

    def __init__(
        self,
        by_ds: Dict[str, list],
        find_layer_fn: Callable[[AuditEvent], object],
        tracker=None,
        parent: Optional[QObject] = None,
        journal_path: Optional[str] = None,
    ):
        super().__init__(parent)
        # The runner's OWN copy of its input. It used to keep the caller's
        # dict: the dialog passed its live undo state, a user edit notified
        # mid-run deleted a layer from it, the runner undid that layer
        # anyway (its groups were already listed) and `undo_outcome` no
        # longer saw it -- its trace stayed active over a reverted
        # compensation. The outcome is accounted against what was run.
        self._by_ds = {fp: list(events) for fp, events in by_ds.items()}
        self._find_layer_fn = find_layer_fn
        self._tracker = tracker
        # The journal the identity proofs of the undo read
        # (`_open_group_journal`); None: keys proven by rare content only.
        self._journal_read_path = journal_path
        self._cancelled = False

        self._groups = list(self._by_ds.items())
        self._group_idx = 0
        self._total_events = sum(len(g) for g in self._by_ds.values())

        self._total_ok = 0
        self._total_fail = 0
        self._errors: List[str] = []
        self._failed_eids: List[int] = []
        self._processed = 0
        # What each event of the input became. `_total_ok` counts a
        # provider success, and "Already absent (no snapshot match)" is a
        # success that wrote nothing: only `_undone_ids` really reverted a
        # compensation (see undo_outcome).
        self._undone_ids: set = set()
        # The same ids in the order they were reverted: the journal records
        # the undo in that order (see undo_outcome).
        self._undone_order: List[int] = []
        self._undo_skipped_ids: List[int] = []

        # Per-layer chunk state (BL-RW-P5-27), reset at every
        # _process_next_group and consumed by _process_undo_chunk.
        self._undo_layer = None
        self._undo_layer_name = ""
        self._undo_group: List[AuditEvent] = []
        self._undo_context: Optional[UndoBatchContext] = None
        self._undo_action_idx = 0
        self._undo_ok = 0
        self._undo_fail = 0
        self._undo_count_before = -1

    def start(self) -> None:
        total_events = sum(len(g) for g in self._by_ds.values())
        flog(f"UndoRunner: start layers={len(self._groups)} "
             f"total_events={total_events}")
        # No global suppression: the capture is suppressed per layer, when
        # the run takes a layer (_suppress_for_run). Suppressing everything
        # for a minutes-long, non-modal run dropped every commit the user
        # made meanwhile on any other layer.
        try:
            self._process_next_group(guarded=False)
        except BaseException as exc:
            _abort_started_run(self, "UndoRunner", exc)
            raise

    def cancel(self) -> None:
        self._cancelled = True

    def written_count(self) -> int:
        """Events reverted on disk so far."""
        return len(self._undone_ids)

    def _process_next_group(self, guarded: bool = True) -> None:
        """Open the next layer group, then hand over to the chunk loop.

        From a QTimer tick with a crash guard (`start` opens the first
        group unguarded, its own handler closes the run): a resolver that
        raised, or an FID pre-resolution failing in `undo_prepare`, ended
        the chain there, and the dialog stayed "busy" until the project
        was switched.
        """
        if self._cancelled or self._group_idx >= len(self._groups):
            self._finish()
            return
        if not guarded:
            self._open_undo_group()
            return
        try:
            self._open_undo_group()
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            self._abandon_undo_open(exc)

    def _abandon_undo_open(self, exc: BaseException) -> None:
        """A crash while a group was opened: no write yet, all of it failed."""
        flog(f"UndoRunner: CRASH while opening group idx={self._group_idx}: "
             f"{exc}", "CRITICAL")
        self._errors.append(f"Internal error: {exc}")
        if self._group_idx < len(self._groups):
            _fp, group = self._groups[self._group_idx]
            self._failed_eids.extend(event.event_id or 0 for event in group)
            self._total_fail += len(group)
            self._processed += len(group)
        self._undo_layer = None
        self._undo_context = None
        self._undo_group = []
        _release_group_suppression(self)
        _close_group_journal(self)
        self._group_idx += 1
        _emit_progress(self, self._processed, self._total_events)
        QTimer.singleShot(0, self._process_next_group)

    def _open_undo_group(self) -> None:
        fp, group = self._groups[self._group_idx]
        layer_name_hint = group[0].layer_name_snapshot if group else '?'
        flog(f"UndoRunner: process_group idx={self._group_idx} "
             f"layer={layer_name_hint!r} n_events={len(group)}")
        # RLU-057: name the layer before the writes start, not after.
        self.layer_started.emit(
            layer_name_hint or fp, self._group_idx + 1,
            len(self._groups), len(group),
        )
        # BL-RW-P5-27: this dump cost 130 ms of frozen UI for 1340 events
        # BEFORE any useful work (measured, trace a1517527). The aggregated
        # line above says how many; the detail is worth an opt-in, not a
        # freeze.
        if heavy_diag_enabled():
            for e in group:
                flog(f"  UndoRunner event: op={e.operation_type} "
                     f"eid={e.event_id} "
                     f"identity={(e.feature_identity_json or '')[:80]}")
        layer, errors = _resolve_runner_layer(
            self._find_layer_fn, group, fp,
            runner_name="UndoRunner",
            uncommitted_msg="Target layer has uncommitted edits",
            runner_owner=self,
        )
        if layer is None:
            self._errors.extend(errors)
            # Named as failures, not left to look "never attempted": the
            # layer is missing or holds unsaved edits, the compensations
            # are still where the restore put them.
            self._failed_eids.extend(event.event_id or 0 for event in group)
            self._total_fail += len(group)
            self._processed += len(group)
            self._group_idx += 1
            _emit_progress(self, self._processed, self._total_events)
            QTimer.singleShot(0, self._process_next_group)
            return

        # ONE read connection for the whole group: `undo_prepare` keeps it
        # in the batch cache every `undo_apply_one` of this layer reads.
        journal, unreadable = _open_group_journal(
            self, layer_name_hint or fp)
        if unreadable:
            # The undo goes on, unlike a Mode A restore (which refuses the
            # layer, see RestoreRunner._open_group). Of the two proofs the
            # journal gives, an undo never asks `already_compensated` (it
            # undoes exactly such a compensation); it only loses the key
            # continuity, and the executor then refuses by name each
            # feature only the journal could identify
            # (key_continuity_unproven (no_journal)), never on a guess.
            # The notice says the undo went on: it used to say the layer's
            # undo was refused, on a layer whose undo was written.
            flog(f"UndoRunner: journal_read unavailable "
                 f"layer={layer_name_hint!r}: {unreadable}; undo goes on, "
                 f"keys proven by rare content only", "ERROR")
            self._errors.append(
                f"Journal illisible pour la couche '{layer_name_hint}' "
                f"({unreadable}) : l'annulation continue sans lui. Une "
                f"entite que seul le journal peut identifier (son contenu "
                f"est partage par d'autres entites) n'est pas annulee et "
                f"est nommee (key_continuity_unproven (no_journal)) ; les "
                f"autres sont annulees.")
        context = undo_prepare(layer, group, journal=journal)
        if context.layer_error:
            # Refused before any write: the layer is the user's again.
            _release_group_suppression(self)
            _close_group_journal(self)
            flog(f"UndoRunner: layer refused layer={layer_name_hint!r} "
                 f"reason={context.layer_error}", "ERROR")
            for event in group:
                eid = event.event_id or 0
                self._errors.append(f"Evt {eid}: {context.layer_error}")
                self._failed_eids.append(eid)
            self._total_fail += len(group)
            self._processed += len(group)
            self._group_idx += 1
            _emit_progress(self, self._processed, self._total_events)
            QTimer.singleShot(0, self._process_next_group)
            return

        # Per-layer state, built once and carried across every tick of this
        # group: rebuilding `ordered` or `fid_cache` mid-group would change
        # the result, not just the cost (see UndoBatchContext).
        self._undo_layer = layer
        self._undo_layer_name = layer_name_hint
        self._undo_group = group
        self._undo_context = context
        self._undo_action_idx = 0
        self._undo_ok = 0
        self._undo_fail = 0
        self._undo_count_before = layer.featureCount()
        QTimer.singleShot(0, self._process_undo_chunk)

    def _process_undo_chunk(self) -> None:
        """Apply a time-bounded slice of the current layer group."""
        try:
            self._process_undo_chunk_inner()
        except Exception as exc:  # noqa: BLE001 - a crash must not wedge the run
            flog(f"UndoRunner: CRASH in chunk: {exc}", "CRITICAL")
            self._errors.append(f"Internal error: {exc}")
            if self._undo_context is not None:
                self._finish_undo_group()
            else:
                # The group closed itself before raising, and its `finally`
                # already advanced the cursor and armed the next step.
                # Closing again would skip a layer and put two chains on
                # the same runner.
                flog("UndoRunner: crash after group close, chain already armed",
                     "WARNING")

    def _process_undo_chunk_inner(self) -> None:
        layer = self._undo_layer
        context = self._undo_context
        ordered = context.ordered
        start = self._undo_action_idx
        chunk_t0 = time.monotonic()

        if self._cancelled:
            # Stop where we are: what is already written stays written, and
            # the untouched tail is reported as unprocessed by _finish.
            flog(f"UndoRunner: cancel_requested "
                 f"layer={self._undo_layer_name!r} "
                 f"applied={self._undo_ok} of {len(ordered)}")
            self._finish_undo_group()
            return

        # The layer was cleared by `_resolve_runner_layer` and
        # `undo_prepare` before the FIRST tick only. Chunking is what makes
        # QGIS answer during the run, so the user can now open an edit
        # session on this very layer between two ticks -- and the writes
        # below go straight to the provider, under his buffer: his next
        # Save would replay his values over what we just reverted, on a
        # layer this runner reports as fully undone.
        if hasattr(layer, 'isEditable') and layer.isEditable():
            msg = ("Couche passee en edition pendant l'annulation ; "
                   "evenements restants abandonnes")
            flog(f"UndoRunner: layer became editable mid-group "
                 f"layer={self._undo_layer_name!r} "
                 f"applied={self._undo_ok} of {len(ordered)}", "ERROR")
            for event in ordered[self._undo_action_idx:]:
                eid = event.event_id or 0
                self._undo_fail += 1
                self._errors.append(f"Evt {eid}: {msg}")
                self._failed_eids.append(eid)
            self._undo_action_idx = len(ordered)
            self._finish_undo_group()
            return

        i = start
        while i < len(ordered):
            elapsed_ms = (time.monotonic() - chunk_t0) * 1000.0
            if _chunk_should_yield(elapsed_ms, i - start):
                break
            event = ordered[i]
            i += 1
            with _runner_step_ctx(self, layer):
                result = undo_apply_one(layer, event, context)
            eid = event.event_id or 0
            if result["success"]:
                self._undo_ok += 1
                if result.get("skipped"):
                    # Target not found, nothing written: the compensation
                    # may still be in the layer, its trace must stay.
                    self._undo_skipped_ids.append(eid)
                else:
                    if eid not in self._undone_ids:
                        self._undone_order.append(eid)
                    self._undone_ids.add(eid)
            else:
                self._undo_fail += 1
                self._errors.append(
                    f"Evt {eid}: {result.get('message', 'echec non decrit')}")
                self._failed_eids.append(eid)

        n_done = i - start
        self._undo_action_idx = i
        chunk_ms = (time.monotonic() - chunk_t0) * 1000.0
        if chunk_ms >= _CHUNK_SLOW_MS:
            # One action alone can blow the budget (the budget is only
            # tested BETWEEN actions); saying so names the layer that needs
            # an index rather than leaving a silent stall.
            flog(f"UndoRunner: CHUNK_SLOW layer={self._undo_layer_name!r} "
                 f"actions={start}..{i} n={n_done} "
                 f"elapsed_ms={int(chunk_ms)} "
                 f"feat_count={self._undo_count_before}", "WARNING")

        # Progress is emitted per CHUNK, not per layer: one emission per
        # layer left the bar frozen for 118 s on the user's run.
        self.progress.emit(self._processed + self._undo_action_idx,
                           self._total_events)

        if self._undo_action_idx >= len(ordered):
            self._finish_undo_group()
            return
        QTimer.singleShot(0, self._process_undo_chunk)

    def _finish_undo_group(self) -> None:
        """Close the current layer: reload, repaint, account, advance.

        Two hard rules, both learned the hard way.

        1. The group state is CLAIMED on entry, before the first call that
           can raise, so this method is idempotent. It runs inside the
           ``try`` of `_process_undo_chunk`, whose crash guard calls it
           again: a second pass would credit the same events twice and
           raise a second time, this time out of a QTimer slot. The chain
           would die there -- and with it `_finish()`, whose ``finally``
           is the only thing that releases the edit tracker.
        2. Every QGIS call is guarded and the advance sits in a
           ``finally``, so no provider failure can stop the run before
           `_finish()`. This mirrors `_rollback_strict`, which always ends
           on `QTimer.singleShot(0, self._advance_group)`.

        `layer.reload()` lands HERE and nowhere else: dropped between two
        chunks it would invalidate the provider FIDs the remaining events
        of this same group still resolve against.
        """
        layer = self._undo_layer
        layer_name_hint = self._undo_layer_name
        group = self._undo_group
        context = self._undo_context
        n_ok, n_fail = self._undo_ok, self._undo_fail
        # Claimed before anything fallible runs.
        self._undo_layer = None
        self._undo_context = None
        self._undo_group = []
        self._undo_ok = 0
        self._undo_fail = 0

        try:
            if context is None or layer is None:
                flog("UndoRunner: finish_group called with no open group, "
                     "advancing", "WARNING")
                return
            ordered = context.ordered

            self._total_ok += n_ok
            self._total_fail += n_fail

            count_after = -1
            try:
                if n_ok:
                    layer.reload()
                    if heavy_diag_enabled():
                        import json as _json
                        geom_check = {}
                        for ev in group:
                            if getattr(ev, 'new_geometry_wkb', None) is not None:
                                try:
                                    fid = _json.loads(
                                        ev.feature_identity_json).get('fid')
                                    if fid is not None:
                                        geom_check[fid] = feature_geom_short_repr(
                                            layer, fid)
                                except Exception as exc:  # noqa: BLE001
                                    # Per-event, but only reachable with
                                    # RECOVERLAND_HEAVY_DIAG on, so the
                                    # volume is opt-in. The missing fid is
                                    # simply absent from RELOAD_CHECK below.
                                    flog(f"UndoRunner: RELOAD_CHECK probe "
                                         f"skipped for eid="
                                         f"{getattr(ev, 'event_id', '?')} "
                                         f"({exc!r})", "DEBUG")
                        flog(f"RELOAD_CHECK layer={layer_name_hint!r} "
                             f"geom_after_reload={geom_check}")
                    else:
                        flog(f"RELOAD_CHECK layer={layer_name_hint!r} "
                             f"geom_after_reload=diag_off "
                             f"(set RECOVERLAND_HEAVY_DIAG=1 to enable)")
                count_after = layer.featureCount()
                layer.triggerRepaint()
            except Exception as exc:  # noqa: BLE001 - layer may be gone
                # The writes already landed; only the refresh failed. Say
                # so and keep going -- the alternative is a dead chain.
                flog(f"UndoRunner: post-apply refresh failed "
                     f"layer={layer_name_hint!r}: {exc}", "ERROR")
                self._errors.append(
                    f"Couche '{layer_name_hint}' non rafraichie: {exc}")

            flog(f"UndoRunner: group_done layer={layer_name_hint!r} "
                 f"ok={n_ok} fail={n_fail} "
                 f"processed={self._undo_action_idx}/{len(ordered)} "
                 f"feat_before={self._undo_count_before} "
                 f"feat_after={count_after}")

            # A cancelled group leaves a tail nobody applied; it must still
            # be accounted, otherwise _processed never reaches
            # _total_events and the bar stops short of the end.
            self._processed += len(group)
            self.progress.emit(self._processed, self._total_events)
        finally:
            # This layer is done, whatever happened to its refresh: its
            # capture and its journal connection are given back before the
            # next layer is undone.
            _release_group_suppression(self)
            _close_group_journal(self)
            self._group_idx += 1
            QTimer.singleShot(0, self._process_next_group)

    def undo_outcome(self) -> Dict[str, Any]:
        """What each input event became: undone, skipped, failed, unattempted.

        The four lists partition the input. Only `undone` reverted a
        compensation: its traces are the only ones the caller may
        invalidate. The three others still have their compensation in the
        layer (a refused write, a target the undo could not find, an event
        a cancel left alone): their traces stay active, otherwise the next
        Rewind replays the compensation on top of itself.

        `undone` lists the ids in the order the undo REVERTED them (the
        reverse of the order the restore applied them), not in input order:
        the caller stamps the invalidation of their traces in that order,
        and a later rewind unwinds those toggles newest first. One stamp
        for the whole undo let the window fall back on the trace ids, i.e.
        on the order the undone restore had WRITTEN them, the opposite one
        (scenario rw_undo_order_journal).
        """
        skipped_set = set(self._undo_skipped_ids)
        failed_set = set(self._failed_eids)
        out: Dict[str, Any] = {"undone": list(self._undone_order),
                               "skipped": [], "failed": [],
                               "unattempted": [],
                               "cancelled": bool(self._cancelled)}
        for events in self._by_ds.values():
            for event in events:
                eid = event.event_id or 0
                if eid in self._undone_ids:
                    continue
                elif eid in skipped_set:
                    out["skipped"].append(eid)
                elif eid in failed_set:
                    out["failed"].append(eid)
                else:
                    out["unattempted"].append(eid)
        return out

    def _finish(self) -> None:
        # The published by_ds is what this run UNDID, nothing else: the
        # caller invalidates the traces of these events and only these.
        # It used to be the whole input, so a failed undo, an undo that
        # found nothing, and the tail a cancel left untouched all lost
        # their trace while their compensation was still in the layer.
        outcome = self.undo_outcome()
        undone_by_ds: Dict[str, list] = {}
        for fp, events in self._by_ds.items():
            done = [e for e in events if (e.event_id or 0) in self._undone_ids]
            if done:
                undone_by_ds[fp] = done
        flog(f"UndoRunner: outcome undone={len(outcome['undone'])} "
             f"skipped={len(outcome['skipped'])} "
             f"failed={len(outcome['failed'])} "
             f"unattempted={len(outcome['unattempted'])} "
             f"cancelled={outcome['cancelled']} (only undone events leave "
             f"the undo scope and lose their trace)")
        _finish_runner(
            self,
            runner_name="UndoRunner",
            by_ds=undone_by_ds,
            total_ok=self._total_ok,
            total_fail=self._total_fail,
            errors=self._errors,
            traces=[],
            write_queue=None,
            started_at=0.0,
            tracker=self._tracker,
            failed_eids=self._failed_eids,
            # Named undo_*: this path has no per-action buckets, and a bare
            # `failed=` next to apply_fail reads as the failure total.
            extra_stats={
                "undone": len(outcome["undone"]),
                "undo_skipped": len(outcome["skipped"]),
                "undo_unattempted": len(outcome["unattempted"]),
            },
        )
