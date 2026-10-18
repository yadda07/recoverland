"""rw_multi_hop_navigation - RecoverLand validation runtime.

Invariant: I-8 (after a rewind to T the layer holds the state at T,
whatever the sequence of rewinds that came before).

What this scenario models
-------------------------
Real users do not rewind once. They go back to an old date, then forward to
a more recent one, then far back again, then repeat the same date twice.
Each hop must land on the requested state -- the engine's memory of "where
we currently are" is what makes that possible.

Today that memory lives in TWO places that disagree:

  * `recover_dialog._last_restore_by_ds` -- in RAM, lost when QGIS closes.
    It drives `_auto_undo_for_rewind`, the only path that re-applies events.
  * the active restore traces in the journal -- durable, survive restarts.
    They drive `rewind_dedup`, which only ever REMOVES work.

So after a restart the durable memory says "these events are compensated"
while the volatile memory that knows how to un-compensate them is gone. The
engine can then only go further back, never forward.

History used here (one entity, one datasource):

    t1  U1  v0 -> v1
    t2  U2  v1 -> v2
    t3  U3  v2 -> v3

    hop A : rewind to T_A in ]t1, t2[   => compensate U3 and U2   -> state v1
    hop B : rewind to T_B in ]t2, t3[   => target state v2, so U2 must be
            RE-APPLIED. Cold start: no in-memory undo state.
    hop C : rewind to T_C before t1     => compensate U1 as well   -> state v0
    hop D : rewind to T_C again          => idempotent, nothing more to do

Hops A, C and D are pure "go further back": the collapse only has to avoid
replaying what is already compensated, and it does. Hop B is the hole: the
window after T_B holds U3 plus its trace, the trace neutralises U3, the
active set is empty and the dialog answers "Aucun evenement apres cette
date. Rien a restaurer." -- while the data actually sits at v1, one state
behind what the user asked for. No warning, no error, wrong data.

The contract asserted below: the set of traces that must be undone to move
to a cutoff is derivable FROM THE JOURNAL ALONE (active traces whose source
event is at or before the new cutoff), so it survives a restart.

Pre-patch verdict: FAIL (hop B silently does nothing).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_multi_hop_navigation"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_FP = "rw_mhn_datasource"
_PROJ_FP = "rw_mhn_project"
_ENTITY_FP = "fid:5"
_IDENTITY = json.dumps({"fid": 5})


def _insert(conn, ts, op, attrs, restored_from=None):
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = (
        "INSERT INTO audit_event ("
        + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )
    cur = conn.execute(sql, (
        _PROJ_FP, _DS_FP, "lyr_mhn", "mhn_layer", "ogr",
        _IDENTITY, op, json.dumps(attrs, ensure_ascii=False),
        None, "NoGeometry", "EPSG:4326", None,
        "tester", None,
        ts.isoformat(), restored_from,
        _ENTITY_FP, 2, None, None,
    ))
    conn.commit()
    return cur.lastrowid


def _delta(old, new):
    return {"changed_only": {"v": {"old": old, "new": new}}}


def _active_set(conn, cutoff_dt, trace_id):
    """Run the REAL rewind read path and return the active event ids."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats

    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, cutoff_dt.isoformat(), inclusive=True,
    )
    events = fetch_events_after_cutoff(
        conn, _DS_FP, cutoff, trace_id=trace_id,
    )
    active, stats = collapse_rewind_events_with_stats(events)
    # Hypothesis revised (v7): this helper read the collapse output as "the
    # USER events to compensate". That hypothesis is objectively false
    # since v7: the forward hop has to run in the SAME chronological pass
    # as the compensations. Hop E proves it: the layer holds v4; undoing
    # the traces first (re-apply U1 -> v1, U2 -> v2) and compensating U4
    # afterwards writes U4's OLD value v0 over them, final v0 instead of
    # v2. So the window carries the forward hop as plan items (commit_id
    # "rewind-plan:<kind>@<toggle>") whose event_id is the TRACE they
    # cancel: the row with id 5 seen in hop B is the re-application of U2
    # (it cancels trace 5), not U3 coming back. The user-event list below
    # is extracted exactly as before (plan items left out), so every
    # original assertion keeps its value and meaning; the plan items and
    # the order of the whole pass are asserted in addition (_ITEMS,
    # _ORDER), never instead.
    _ITEMS[cutoff_dt.isoformat()] = _plan_items(active)
    _ORDER[cutoff_dt.isoformat()] = [
        ((_plan_items([e]) or [("user",)])[0][0], e.event_id) for e in active]
    return (
        sorted(e.event_id for e in active
               if e.event_id is not None and not _plan_items([e])),
        dict(stats),
        cutoff,
    )


_ITEMS: dict = {}
_ORDER: dict = {}


def _plan_items(active):
    """(kind, event_id, values the compensation writes) of each plan item."""
    from recoverland.core.search_service import reconstruct_attributes
    out = []
    for e in active:
        mark = getattr(e, "commit_id", None) or ""
        if mark.startswith("rewind-plan:"):
            kind = mark.split(":", 1)[1].split("@")[0]
            out.append((kind, e.event_id, reconstruct_attributes(e).get("v")))
    return out


def _reapply_set(conn, cutoff_dt, trace_id):
    """Traces that must be undone (= events re-applied) to reach the cutoff.

    Journal-driven, so it works after a restart. Missing before the patch.
    """
    from recoverland.core.event_stream_repository import (
        fetch_active_traces_before_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType

    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, cutoff_dt.isoformat(), inclusive=True,
    )
    traces = fetch_active_traces_before_cutoff(
        conn, [_DS_FP], cutoff, trace_id=trace_id,
    )
    return sorted(t.restored_from_event_id for t in traces
                  if t.restored_from_event_id is not None)


def _dialog_hop(conn, cutoff_dt):
    """Rewind to *cutoff_dt* through the dialog's real entry point.

    `_recover_version_mode` with an EMPTY in-memory undo state (a QGIS
    restart), the real window read (`version_fetch_thread._run_fetch`) on
    a file copy of this journal; the runners are recorders. Returns what
    the rewind was handed and whether anything was undone first.
    """
    import os
    import shutil
    import tempfile
    import recoverland.recover_dialog as rd
    from scripts.validation.scenarios import _undo_dialog_harness as h

    tmp = tempfile.mkdtemp(prefix="rl_mhn_dialog_")
    path = os.path.join(tmp, "journal.sqlite")
    copy = sqlite3.connect(path)
    try:
        conn.backup(copy)
    finally:
        copy.close()
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_DS_FP], cutoff_dt=cutoff_dt)
    undo_runners, stricts, fetches, asked = [], [], [], []
    seams = h.Seams()
    try:
        seams.set(rd, "UndoRunner", h.recording_runner_class(undo_runners))
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda *_a, **_k: None)
        seams.set(rd, "find_target_layer",
                  lambda _e, _c=None: h.FakeLayer("lyr_mhn", "mhn_layer"))
        dlg._is_recovering = True
        dlg._recover_version_mode()
        h.pump_until(lambda: (not dlg._is_recovering
                              and not dlg._restore_in_progress
                              and dlg._restore_runner is None), 5.0)
        return {
            "undo_runners": len(undo_runners),
            "runs": [_plan_items(s.events)
                     + [("user", e.event_id, None) for e in s.events
                        if not _plan_items([e])]
                     for s in stricts],
        }
    finally:
        seams.restore()
        h.close_dialog(dlg)
        shutil.rmtree(tmp, ignore_errors=True)


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)

    u1 = _insert(conn, t0 + timedelta(seconds=100), "UPDATE", _delta("v0", "v1"))
    u2 = _insert(conn, t0 + timedelta(seconds=200), "UPDATE", _delta("v1", "v2"))
    u3 = _insert(conn, t0 + timedelta(seconds=300), "UPDATE", _delta("v2", "v3"))

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["u"] = {"u1": u1, "u2": u2, "u3": u3}
    ctx.data["T_A"] = t0 + timedelta(seconds=150)   # between t1 and t2
    ctx.data["T_B"] = t0 + timedelta(seconds=250)   # between t2 and t3
    ctx.data["T_C"] = t0 + timedelta(seconds=50)    # before t1

    flog(
        f"rw_multi_hop_navigation setup: trace_id={ctx.trace_id} "
        f"u1={u1} u2={u2} u3={u3} datasource={_DS_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    u = ctx.data["u"]
    t0 = ctx.data["t0"]

    flog(f"rw_multi_hop_navigation run start: trace_id={ctx.trace_id}", "INFO")

    # ---- hop A: go back to T_A (compensate U3 then U2) -----------------
    active_a, stats_a, _ = _active_set(conn, ctx.data["T_A"], ctx.trace_id)
    ctx.data["hop_a_active"] = active_a
    ctx.data["hop_a_stats"] = stats_a
    # The rewind runs and writes one trace per compensated event.
    ctx.data["trace_a"] = [
        _insert(conn, t0 + timedelta(seconds=400), "UPDATE",
                {"_restore_ref": u["u3"]}, restored_from=u["u3"]),
        _insert(conn, t0 + timedelta(seconds=401), "UPDATE",
                {"_restore_ref": u["u2"]}, restored_from=u["u2"]),
    ]
    # Layer now holds v1.

    # ---- hop B: forward to T_B (target v2 => U2 must be re-applied) ----
    # Cold start: nothing in RAM, only the journal.
    active_b, stats_b, _ = _active_set(conn, ctx.data["T_B"], ctx.trace_id)
    ctx.data["hop_b_active"] = active_b
    ctx.data["hop_b_items"] = _ITEMS.get(ctx.data["T_B"].isoformat(), [])
    ctx.data["hop_b_stats"] = stats_b
    try:
        ctx.data["hop_b_reapply"] = _reapply_set(
            conn, ctx.data["T_B"], ctx.trace_id)
        ctx.data["hop_b_reapply_error"] = ""
    except Exception as exc:  # noqa: BLE001 - absence of the API is the defect
        ctx.data["hop_b_reapply"] = None
        ctx.data["hop_b_reapply_error"] = repr(exc)
    # The same hop through the dialog's rewind entry point, cold start (no
    # in-memory undo state), on a copy of this journal.
    try:
        ctx.data["hop_b_dialog"] = _dialog_hop(conn, ctx.data["T_B"])
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        ctx.data["hop_b_dialog"] = {"crash": repr(exc)}

    # ---- hop C: far back to T_C (U1 still needs compensating) ----------
    active_c, stats_c, _ = _active_set(conn, ctx.data["T_C"], ctx.trace_id)
    ctx.data["hop_c_active"] = active_c
    ctx.data["hop_c_stats"] = stats_c
    ctx.data["trace_c"] = [
        _insert(conn, t0 + timedelta(seconds=500), "UPDATE",
                {"_restore_ref": u["u1"]}, restored_from=u["u1"]),
    ]
    # Layer now holds v0.

    # ---- hop D: same date again, must be a no-op -----------------------
    active_d, stats_d, _ = _active_set(conn, ctx.data["T_C"], ctx.trace_id)
    ctx.data["hop_d_active"] = active_d
    ctx.data["hop_d_items"] = _ITEMS.get(ctx.data["T_C"].isoformat(), [])
    ctx.data["hop_d_stats"] = stats_d
    try:
        ctx.data["hop_d_reapply"] = _reapply_set(
            conn, ctx.data["T_C"], ctx.trace_id)
    except Exception:  # noqa: BLE001
        ctx.data["hop_d_reapply"] = None

    # ---- hop E: the user EDITS while rewound, then hops forward --------
    # The nastiest combination: the layer sits at v0 with three compensated
    # events behind it, the user edits anyway (v0 -> v4), then asks for T_B.
    # Reaching v2 requires BOTH moves at once: compensate the new edit U4
    # (it is after T_B) and re-apply U1 and U2 (their compensations are
    # before T_B), while U3 stays compensated.
    u4 = _insert(conn, t0 + timedelta(seconds=600), "UPDATE",
                 _delta("v0", "v4"))
    ctx.data["u"]["u4"] = u4
    active_e, stats_e, _ = _active_set(conn, ctx.data["T_B"], ctx.trace_id)
    ctx.data["hop_e_active"] = active_e
    ctx.data["hop_e_items"] = _ITEMS.get(ctx.data["T_B"].isoformat(), [])
    ctx.data["hop_e_order"] = _ORDER.get(ctx.data["T_B"].isoformat(), [])
    ctx.data["hop_e_stats"] = stats_e
    try:
        ctx.data["hop_e_reapply"] = _reapply_set(
            conn, ctx.data["T_B"], ctx.trace_id)
    except Exception:  # noqa: BLE001
        ctx.data["hop_e_reapply"] = None

    flog(
        f"rw_multi_hop_navigation run end: trace_id={ctx.trace_id} "
        f"hop_a={active_a} hop_b={active_b} "
        f"hop_b_reapply={ctx.data.get('hop_b_reapply')} "
        f"hop_c={active_c} hop_d={active_d} "
        f"hop_e={active_e} hop_e_reapply={ctx.data.get('hop_e_reapply')}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    u = ctx.data["u"]

    # ===== hop A: plain rewind backwards ================================
    out.append((
        "hop_a_compensates_u2_and_u3",
        ctx.data.get("hop_a_active") == sorted([u["u2"], u["u3"]]),
        f"hop_a_active={ctx.data.get('hop_a_active')} "
        f"expected={sorted([u['u2'], u['u3']])} (every event after T_A)",
    ))

    # ===== hop B: forward navigation ====================================
    # Nothing left to compensate is CORRECT here: U3 is already undone.
    out.append((
        "hop_b_nothing_left_to_compensate",
        ctx.data.get("hop_b_active") == [],
        f"hop_b_active={ctx.data.get('hop_b_active')} expected=[] "
        f"(U3 was already compensated at hop A)",
    ))
    # ...but the hop is NOT a no-op: U2 must come back.
    out.append((
        "hop_b_reapply_api_exists",
        ctx.data.get("hop_b_reapply") is not None,
        f"fetch_active_traces_before_cutoff unusable: "
        f"{ctx.data.get('hop_b_reapply_error')}. Without a journal-driven "
        f"way to list what must be re-applied, forward navigation depends "
        f"on in-memory state and breaks after a QGIS restart.",
    ))
    # Hypothesis revised (v7): the forward hop is part of the rewind window
    # itself, as a compensation of U2's trace, applied in the same run as
    # the compensations (an edit made after a rewind must be undone BEFORE
    # that rewind's compensation is). Stronger than the journal primitive
    # alone: the window must re-apply exactly U2, writing v2.
    trace_a = ctx.data.get("trace_a") or [None, None]
    out.append((
        "hop_b_window_reapplies_u2",
        ctx.data.get("hop_b_items") == [("reapply", trace_a[1], "v2")],
        f"plan items at T_B={ctx.data.get('hop_b_items')} expected "
        f"[('reapply', {trace_a[1]}, 'v2')]: re-apply U2 (write v2) by "
        f"compensating its trace; nothing else.",
    ))
    out.append((
        "hop_b_reapplies_u2",
        ctx.data.get("hop_b_reapply") == [u["u2"]],
        f"hop_b_reapply={ctx.data.get('hop_b_reapply')} expected=[{u['u2']}]. "
        f"Moving from T_A to the more recent T_B must re-apply U2, otherwise "
        f"the layer stays at v1 while the dialog reports 'nothing to restore'.",
    ))
    out.append((
        "hop_b_does_not_reapply_u3",
        ctx.data.get("hop_b_reapply") is not None
        and u["u3"] not in ctx.data["hop_b_reapply"],
        f"hop_b_reapply={ctx.data.get('hop_b_reapply')} must NOT contain "
        f"U3={u['u3']} (it happened after T_B, it stays compensated)",
    ))

    # ===== hop C: far back ==============================================
    out.append((
        "hop_c_adds_only_u1",
        ctx.data.get("hop_c_active") == [u["u1"]],
        f"hop_c_active={ctx.data.get('hop_c_active')} expected=[{u['u1']}]. "
        f"U2/U3 are already compensated; replaying them would double-apply.",
    ))

    # ===== hop D: same date twice =======================================
    out.append((
        "hop_d_is_idempotent",
        ctx.data.get("hop_d_active") == [],
        f"hop_d_active={ctx.data.get('hop_d_active')} expected=[] "
        f"(rewinding twice to the same date must be a no-op)",
    ))
    out.append((
        "hop_d_window_has_no_plan_item",
        ctx.data.get("hop_d_items") == [],
        f"plan items at T_C again={ctx.data.get('hop_d_items')} expected []",
    ))
    out.append((
        "hop_d_reapplies_nothing",
        ctx.data.get("hop_d_reapply") == [],
        f"hop_d_reapply={ctx.data.get('hop_d_reapply')} expected=[] "
        f"(already at T_C: nothing to undo, nothing to redo)",
    ))

    # ===== hop E: edit while rewound, then hop forward ==================
    u4 = ctx.data["u"].get("u4")
    out.append((
        "hop_e_compensates_the_new_edit",
        ctx.data.get("hop_e_active") == [u4],
        f"hop_e_active={ctx.data.get('hop_e_active')} expected=[{u4}]. The "
        f"edit made while the layer was rewound is after T_B, so it must be "
        f"compensated -- and it must NOT be swallowed by the traces already "
        f"attached to this entity (regression rw_dedup_post_trace_edit).",
    ))
    trace_c = ctx.data.get("trace_c") or [None]
    out.append((
        "hop_e_window_reapplies_u1_and_u2_after_u4",
        sorted(ctx.data.get("hop_e_items") or [], key=lambda x: x[1] or 0)
        == sorted([("reapply", trace_a[1], "v2"), ("reapply", trace_c[0], "v1")],
                  key=lambda x: x[1] or 0),
        f"plan items at T_B after the edit={ctx.data.get('hop_e_items')} "
        f"expected re-apply U2 (v2) and U1 (v1) in the same window as the "
        f"compensation of U4",
    ))
    expected_order = [("user", u4), ("reapply", trace_c[0]),
                      ("reapply", trace_a[1])]
    out.append((
        "hop_e_one_pass_newest_first",
        ctx.data.get("hop_e_order") == expected_order,
        f"pass at T_B={ctx.data.get('hop_e_order')} expected {expected_order}: "
        f"U4 compensated first (v4 -> v0), then U1 re-applied (v1), then U2 "
        f"(v2). Any other order ends on v0 or v1 while the rewind reports "
        f"success.",
    ))
    out.append((
        "hop_e_reapplies_u1_and_u2",
        ctx.data.get("hop_e_reapply") == sorted([u["u1"], u["u2"]]),
        f"hop_e_reapply={ctx.data.get('hop_e_reapply')} "
        f"expected={sorted([u['u1'], u['u2']])}: both compensations are "
        f"before T_B, so both events must come back.",
    ))
    out.append((
        "hop_e_keeps_u3_compensated",
        ctx.data.get("hop_e_reapply") is not None
        and u["u3"] not in ctx.data["hop_e_reapply"]
        and u["u3"] not in (ctx.data.get("hop_e_active") or []),
        f"U3={u['u3']} must be neither re-applied nor compensated again: "
        f"it is after T_B and already undone. "
        f"active={ctx.data.get('hop_e_active')} "
        f"reapply={ctx.data.get('hop_e_reapply')}",
    ))

    # ===== The dialog must actually use it ==============================
    # The window alone changes nothing: the rewind entry point has to hand
    # it to the rewind when RAM is empty (run below, see _dialog_hop).
    dialog = _PLUGIN_ROOT / "recover_dialog.py"
    if not dialog.is_file():
        out.append(("dialog_source_readable", False, "missing recover_dialog.py"))
        return out

    # Hypothesis revised (v7): these three checks read the SOURCE for the
    # mechanism of v6 (the dialog rebuilt an undo scope from the journal
    # with `_reapply_set_from_journal`, assigned it to `_last_restore_by_ds`
    # and undid it before the rewind). That mechanism is objectively gone:
    # the v7 window carries the forward hop itself (hop_b_window_reapplies_u2
    # above), and the undo it fed re-compensated what it had just undone
    # (scenario rw_rewind_no_autoundo). The checks now RUN the dialog's
    # rewind entry point (`_recover_version_mode`, real window read on a copy
    # of this journal, cold start: no in-memory undo state) and assert what
    # the source checks only presumed, which is stronger: the hop is not a
    # no-op after a restart, it hands the rewind exactly the re-application
    # of U2 (writing v2), and nothing is undone before it.
    hop_b_dialog = ctx.data.get("hop_b_dialog") or {}
    out.append((
        "dialog_defines_reapply_set_builder",
        hop_b_dialog.get("runs") == [[("reapply", trace_a[1], "v2")]],
        f"rewind launched by the dialog at T_B, cold start="
        f"{hop_b_dialog}: expected one run re-applying U2 through its "
        f"trace ({trace_a[1]}), writing v2",
    ))
    out.append((
        "rewind_entry_point_rebuilds_undo_scope",
        bool(hop_b_dialog.get("runs")) and not hop_b_dialog.get("crash"),
        f"dialog hop at T_B after a restart={hop_b_dialog}: with no in-memory "
        f"undo state the forward hop must still run, otherwise it silently "
        f"does nothing",
    ))
    out.append((
        "rebuilt_scope_feeds_the_undo_path",
        hop_b_dialog.get("undo_runners") == 0,
        f"undo runners created before the rewind="
        f"{hop_b_dialog.get('undo_runners')}: the forward hop is one "
        f"chronological pass of the window; an undo before it re-compensates "
        f"what it has just reverted",
    ))
    out.append((
        "primitive_is_the_complement_of_the_rewind_predicate",
        bool(re.search(
            r'op\s*=\s*"<"\s*if\s+cutoff\.inclusive\s+else\s*"<="',
            (_PLUGIN_ROOT / "core" / "event_stream_repository.py")
            .read_text(encoding="utf-8", errors="replace"),
        )),
        "fetch_active_traces_before_cutoff must use the strict complement of "
        "the compensation predicate (>= / >), so no event is both replayed "
        "and re-applied, and none is forgotten at the boundary",
    ))

    # ===== Trace propagation ============================================
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_multi_hop_navigation.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


def teardown(ctx):
    """Ferme le journal en memoire ouvert par setup(ctx).

    Sous Python 3.12 une sqlite3.Connection se reference elle-meme (cache
    de requetes) : non fermee, elle restait ouverte apres le scenario,
    jusqu'au prochain passage du ramasse-miettes, pendant un autre.
    """
    conn = ctx.data.pop("conn", None)
    if conn is not None:
        conn.close()


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
