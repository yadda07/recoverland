"""rw_undo_trace_invalidation_truth - une trace reste active tant que sa compensation est dans les donnees.

Constats couverts : trace-invalidation-ignores-undo-outcome (HIGH) et
undo-invalidates-failed-traces (HIGH).

Mecanisme
---------
`UndoRunner._finish` publiait son perimetre d'ENTREE complet. Le dialogue
en deduisait les traces a invalider :

  * `_on_undo_done` invalidait tous les evenements de `result.by_ds`, plus,
    explicitement, ceux dont l'undo avait ECHOUE ;
  * `_on_auto_undo_then_rewind` invalidait tous les evenements de l'entree.

Un undo qui echoue (fournisseur qui refuse, FID deplace), un undo qui ne
retrouve pas l'entite ("Already absent (no snapshot match)", compte comme
un succes) et un evenement jamais tente parce que l'utilisateur a annule
laissent pourtant la compensation DANS la couche. Leur trace invalidee, le
Rewind suivant les voit comme non compenses et les rejoue : pour un DELETE
la compensation est un addFeature sans idempotence, l'entite est inseree
une deuxieme fois, et REWIND_STATE voit le +1 attendu.

Deux fautes aggravantes sur le chemin de l'auto-undo :

  * une invalidation qui echoue (journal verrouille) n'etait qu'un WARNING
    et le Rewind continuait sur un journal qui ne decrit plus les donnees ;
  * un clic Annuler pendant l'auto-undo n'arretait rien : le Rewind
    repartait des que l'undo avait fini.

Ce qui est prouve, sur le vrai `UndoRunner`, les vraies methodes du
dialogue et un vrai journal SQLite (seul le fournisseur est scripte) :

  U  undo manuel : 1 annule, 1 echec, 1 introuvable, 1 jamais tente (clic
     Annuler). Seule la trace de l'annule est invalidee, les trois autres
     restent annulables, et le message ne compte que ce qui a ete remis.
  A  auto-undo avant un Rewind : meme regle d'invalidation, et le compte
     rendu final du Rewind ne se declare pas propre.
  K  clic Annuler pendant l'auto-undo : le Rewind ne repart pas.
  L  invalidation refusee par un verrou : le Rewind s'arrete, en ERROR.

v7 (lot dialogue, tour 3) : l'auto-undo avant un Rewind n'existe plus (la
fenetre v7 est le plan complet, et l'auto-undo re-compensait ce qu'il
venait de defaire, voir `rw_rewind_no_autoundo`). A, K et L sont rejoues
depuis le point d'entree du Rewind avec la vraie lecture de la fenetre ;
leurs assertions sont revisees ("Hypothesis revised (v7)") en garanties au
moins aussi fortes : rien n'est defait ni invalide avant le Rewind (A),
rien ne tourne avant lui et le clic Annuler l'atteint (K), aucune ecriture
du journal avant son run, et l'echec d'invalidation de l'undo manuel sur
journal verrouille est dit en ERROR (L, L2).

Verdict pre-correctif : FAIL. Verdict post-correctif : PASS.
"""
from __future__ import annotations

import sys
import tempfile
from datetime import timedelta
from pathlib import Path

SCENARIO_ID = "rw_undo_trace_invalidation_truth"
INVARIANT = "I-TRACE-TRUTH"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP1 = "ds_undo_truth_routes"
_FP2 = "ds_undo_truth_batiments"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _seed(h, directory, name):
    """Four user events already compensated by a previous run (traces active)."""
    path = h.new_journal(directory, name)
    u1 = h.insert_event(path, _FP1, "routes", "DELETE", 100, fid=1)
    u2 = h.insert_event(path, _FP1, "routes", "DELETE", 110, fid=2)
    u3 = h.insert_event(path, _FP1, "routes", "DELETE", 120, fid=3)
    u4 = h.insert_event(path, _FP2, "batiments", "UPDATE", 130, fid=4)
    for sec, uid in ((500, u1), (501, u2), (502, u3), (503, u4)):
        fp, name = (_FP2, "batiments") if uid == u4 else (_FP1, "routes")
        h.insert_event(path, fp, name, "INSERT" if uid != u4 else "UPDATE",
                       sec, fid=1, restored_from=uid)
    # A fresh user edit after the cutoff, for the Rewind to compensate.
    x = h.insert_event(path, _FP1, "routes", "UPDATE", 700, fid=9)
    return path, {"u1": u1, "u2": u2, "u3": u3, "u4": u4, "x": x}


def _prior_scope(h, path, ids):
    ev = dict(zip(("u1", "u2", "u3", "u4"),
                  h.fetch_events(path, [ids["u1"], ids["u2"], ids["u3"], ids["u4"]])))
    # Newest first, as the rewind runner publishes it.
    return {_FP1: [ev["u3"], ev["u2"], ev["u1"]], _FP2: [ev["u4"]]}


def _install(h, rd, rr, seams, outcomes, undo_calls, logs, asked, on_apply=None):
    layers = {_FP1: h.FakeLayer("lyr_routes", "routes"),
              _FP2: h.FakeLayer("lyr_batiments", "batiments")}
    prepare, apply_one = h.scripted_undo(outcomes, undo_calls, on_apply=on_apply)
    seams.set(rr, "undo_prepare", prepare)
    seams.set(rr, "undo_apply_one", apply_one)
    seams.set(rd, "find_target_layer",
              lambda evt, _conn=None: layers.get(evt.datasource_fingerprint))
    seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
    seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))


def _run_manual(ctx, h, rd, rr):
    path, ids = _seed(h, ctx.data["tmp"].name, "manual.sqlite")
    ctx.data["m_ids"] = ids
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP1, _FP2])
    dlg._last_restore_by_ds = _prior_scope(h, path, ids)
    dlg._last_restore_events = [e for v in dlg._last_restore_by_ds.values() for e in v]
    # The previous run (the last one) wrote the four compensations: what
    # the Undo button reverts (`_manual_undo_scope`).
    dlg._last_run_ids = {e.event_id for e in dlg._last_restore_events}
    undo_calls, logs, asked, done = [], [], [], []

    def on_apply(eid):
        # u3 is the last event of the first layer: the user clicks Cancel
        # there, so the second layer (u4) is never attempted.
        if eid == ids["u3"]:
            dlg.cancel_operation()

    seams = h.Seams()
    try:
        _install(h, rd, rr, seams,
                 {ids["u1"]: "undone", ids["u2"]: "failed", ids["u3"]: "skipped"},
                 undo_calls, logs, asked, on_apply=on_apply)
        original = dlg._on_undo_done

        def _record(res, _orig=original):
            _orig(res)
            done.append(res)
        dlg._on_undo_done = _record
        dlg._undo_last_restore()
        h.pump_until(lambda: bool(done), 10.0)
        ctx.data["m_done"] = bool(done)
        ctx.data["m_calls"] = [eid for _n, eid in undo_calls]
        ctx.data["m_traces"] = h.trace_state(
            path, [ids["u1"], ids["u2"], ids["u3"], ids["u4"]])
        ctx.data["m_scope_after"] = sorted(h.all_ids(dlg._last_restore_by_ds))
        ctx.data["m_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _rewind_dialog(h, rd, path, ids):
    cut = h.T0 + timedelta(seconds=600)
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP1, _FP2], cutoff_dt=cut)
    dlg._last_restore_by_ds = _prior_scope(h, path, ids)
    dlg._last_restore_events = [e for v in dlg._last_restore_by_ds.values() for e in v]
    dlg._last_run_ids = {e.event_id for e in dlg._last_restore_events}
    return dlg


def _in_flight_strict(h, created):
    """StrictRestoreRunner that still writes: it ends when told, or cancelled."""
    from recoverland.core.workflow_service import GroupedRestoreResult

    class InFlightStrict:
        # Hypothesis revised (v7): the dialog also hands the runner the
        # journal it planned from (`journal_path`); the double takes it
        # like the real constructor, nothing else changes.
        def __init__(self, events, resolver, cutoff, write_queue=None,
                     tracker=None, trace_id="", parent=None, extra_stats=None,
                     journal_path=None):
            self.events = list(events)
            self.progress = h.Signal()
            self.layer_started = h.Signal()
            self.finished = h.Signal()
            self._cancelled = False
            self._total_ok = 0
            self._by_ds = {}
            created.append(self)

        def cancel(self):
            self._cancelled = True

        def start(self):
            return None

        def finish(self):
            self.finished.emit(GroupedRestoreResult(
                total_ok=0, total_fail=0, errors=[], by_ds={},
                trace_events=[], applied=0))

    return InFlightStrict


def _drive_rewind(ctx, h, rd, rr, prefix, outcomes_by_name, lock=False,
                  cancel_during_rewind=False):
    """A Rewind confirmed while a previous one is still applied.

    Hypothesis revised (v7): this driver fed `_on_version_fetch_done` a
    scripted window and followed the auto-undo that preceded the Rewind.
    There is no auto-undo any more: the Rewind starts from its entry point
    (`_recover_version_mode`), reads the REAL window of this journal (T=600:
    the previous run's compensations at t=500..503 are part of the state
    asked for, only x at t=700 is after it) and launches the Rewind.
    """
    path, ids = _seed(h, ctx.data["tmp"].name, f"{prefix}.sqlite")
    ctx.data[f"{prefix}_ids"] = ids
    dlg = _rewind_dialog(h, rd, path, ids)
    dlg._journal.lock_writes = lock
    outcomes = {ids[name]: kind for name, kind in outcomes_by_name.items()}
    undo_calls, logs, asked, fetches, stricts = [], [], [], [], []
    seams = h.Seams()
    try:
        _install(h, rd, rr, seams, outcomes, undo_calls, logs, asked)
        seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
        runner_class = (_in_flight_strict(h, stricts) if cancel_during_rewind
                        else h.strict_runner_class(stricts))
        seams.set(rd, "StrictRestoreRunner", runner_class)
        dlg._is_recovering = True
        dlg._recover_version_mode()
        if cancel_during_rewind and stricts:
            # The user clicks "Annuler" while the Rewind writes.
            dlg.cancel_operation()
            stricts[0].finish()
        h.pump_until(lambda: (not dlg._is_recovering
                              and not dlg._restore_in_progress
                              and dlg._restore_runner is None), 10.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data[f"{prefix}_undo_finished"] = True
        ctx.data[f"{prefix}_calls"] = [eid for _n, eid in undo_calls]
        ctx.data[f"{prefix}_traces"] = h.trace_state(
            path, [ids["u1"], ids["u2"], ids["u3"], ids["u4"]])
        ctx.data[f"{prefix}_strict_runs"] = len(stricts)
        ctx.data[f"{prefix}_runs"] = [h.plan_marks(s.events) for s in stricts]
        ctx.data[f"{prefix}_cancelled"] = [s._cancelled for s in stricts]
        ctx.data[f"{prefix}_refetches"] = max(len(fetches) - 1, 0)
        ctx.data[f"{prefix}_write_connections"] = dlg._journal.write_connections
        ctx.data[f"{prefix}_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog

    h = _h()
    ctx.data["tmp"] = tempfile.TemporaryDirectory(prefix="rl_undo_truth_")
    flog(f"rw_undo_trace_invalidation_truth run start: trace_id={ctx.trace_id}")
    try:
        _run_manual(ctx, h, rd, rr)
        # Same shape as U: if anything were undone before the Rewind, these
        # outcomes would decide which traces fall.
        _drive_rewind(ctx, h, rd, rr, "a", {
            "u1": "undone", "u2": "failed", "u3": "skipped", "u4": "undone"})
        _drive_rewind(ctx, h, rd, rr, "k", {}, cancel_during_rewind=True)
        _drive_rewind(ctx, h, rd, rr, "l", {}, lock=True)
        _run_manual_locked(ctx, h, rd, rr)
    finally:
        flog(f"rw_undo_trace_invalidation_truth run end: trace_id={ctx.trace_id}")
        ctx.data["tmp"].cleanup()


def _run_manual_locked(ctx, h, rd, rr):
    """L2: a manual undo whose trace invalidation the journal refuses (lock)."""
    path, ids = _seed(h, ctx.data["tmp"].name, "manual_locked.sqlite")
    ctx.data["l2_ids"] = ids
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP1, _FP2])
    dlg._journal.lock_writes = True
    dlg._last_restore_by_ds = _prior_scope(h, path, ids)
    dlg._last_restore_events = [e for v in dlg._last_restore_by_ds.values() for e in v]
    dlg._last_run_ids = {e.event_id for e in dlg._last_restore_events}
    undo_calls, logs, asked, done = [], [], [], []
    seams = h.Seams()
    try:
        _install(h, rd, rr, seams, {}, undo_calls, logs, asked)
        original = dlg._on_undo_done

        def _record(res, _orig=original):
            _orig(res)
            done.append(res)
        dlg._on_undo_done = _record
        dlg._undo_last_restore()
        h.pump_until(lambda: bool(done), 10.0)
        ctx.data["l2_calls"] = [eid for _n, eid in undo_calls]
        ctx.data["l2_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _final_report(logs):
    """The last message of the Rewind, or ('', '')."""
    for msg, level in reversed(logs):
        if "restauree" in msg or "Rewind" in msg or "Auto-undo" in msg \
                or "action(s)" in msg or "comptabilisee" in msg:
            return msg, level
    return ("", "")


def assertions(ctx):
    m = ctx.data["m_ids"]
    mt = ctx.data.get("m_traces") or {}
    m_logs = ctx.data.get("m_logs") or []
    m_msg = " || ".join(msg for msg, _l in m_logs)
    a = ctx.data["a_ids"]
    at = ctx.data.get("a_traces") or {}
    a_logs = ctx.data.get("a_logs") or []
    a_all = " || ".join(msg for msg, _l in a_logs)
    final_msg, final_level = _final_report(a_logs)
    k_logs = ctx.data.get("k_logs") or []
    lt = ctx.data.get("l_traces") or {}
    l2_logs = ctx.data.get("l2_logs") or []

    return [
        # ------------------------------------------------ U: manual undo
        ("U_undo_ran", ctx.data.get("m_done") is True,
         "l'undo manuel doit aller jusqu'a son slot de fin"),
        ("U_attempts_stop_at_cancel",
         ctx.data.get("m_calls") == [m["u1"], m["u2"], m["u3"]],
         f"inverses tentes={ctx.data.get('m_calls')} : u4 ne doit pas etre "
         f"tente apres le clic Annuler (controle du scenario)"),
        ("U_only_the_undone_trace_is_invalidated",
         mt.get(m["u1"]) == "invalidated" and mt.get(m["u2"]) == "active"
         and mt.get(m["u3"]) == "active" and mt.get(m["u4"]) == "active",
         f"traces apres undo={mt} : seule la trace de u1 (reellement remis) "
         f"peut etre invalidee. u2 (undo refuse), u3 (entite introuvable) et "
         f"u4 (jamais tente) ont toujours leur compensation dans la couche ; "
         f"invalider leur trace fait rejouer la compensation au Rewind "
         f"suivant (DELETE -> entite inseree une deuxieme fois)."),
        ("U_not_undone_events_stay_revertible",
         ctx.data.get("m_scope_after") == sorted([m["u2"], m["u3"], m["u4"]]),
         f"perimetre d'annulation restant={ctx.data.get('m_scope_after')} "
         f"attendu {sorted([m['u2'], m['u3'], m['u4']])} : ce que l'undo n'a "
         f"pas remis est toujours dans les donnees et doit rester annulable."),
        ("U_message_counts_only_what_was_undone",
         "1 entite(s) remise(s)" in m_msg and "1 echec(s)" in m_msg
         and "1 introuvable(s)" in m_msg and "1 non traitee(s)" in m_msg,
         f"messages={m_msg!r} : l'utilisateur doit lire 1 remise, 1 echec, "
         f"1 introuvable, 1 non traitee ; compter l'introuvable comme 'ok' "
         f"annonce une entite remise qui ne l'est pas."),
        # ------------------------------------------------ A: Rewind on a Rewind
        # Hypothesis revised (v7): "the auto-undo invalidates only the undone
        # traces" presumed an undo before the Rewind. There is none: the
        # previous run's compensations (t=500..503) are part of the state at
        # T=600. Stronger: nothing is reverted and NO trace is invalidated.
        ("A_auto_undo_invalidates_only_undone_traces",
         ctx.data.get("a_calls") == []
         and all(at.get(a[k]) == "active" for k in ("u1", "u2", "u3", "u4")),
         f"inverses={ctx.data.get('a_calls')} traces={at} : les "
         f"compensations du run precedent font partie de l'etat a T ; les "
         f"defaire, ou invalider leurs traces, ferait rejouer ou perdre des "
         f"entites."),
        # Hypothesis revised (v7): the Rewind runs, on exactly its window
        # (x, the only edit after T), not on a re-fetch after an undo.
        ("A_rewind_still_runs_after_partial_undo",
         ctx.data.get("a_strict_runs") == 1
         and ctx.data.get("a_runs") == [[("user", a["x"])]],
         f"runs de Rewind={ctx.data.get('a_runs')} attendu "
         f"[[('user', {a['x']})]]."),
        # Hypothesis revised (v7): the report had to confess an incomplete
        # undo. No undo happens: the report must not claim one, and must
        # report the Rewind itself.
        ("A_final_report_is_not_clean",
         "Annulation du Rewind precedent" not in a_all
         and "restauree" in final_msg,
         f"messages={a_all!r} : aucune annulation n'a eu lieu, aucune ne doit "
         f"etre annoncee ; le compte rendu est celui du Rewind."),
        # ------------------------------------------------ K: cancel
        # Hypothesis revised (v7): there is no undo phase before the Rewind
        # to cancel. The guarantee kept: nothing runs before the Rewind the
        # user confirmed, and his click on "Annuler" reaches that Rewind.
        ("K_cancel_during_auto_undo_stops_the_rewind",
         ctx.data.get("k_calls") == [] and ctx.data.get("k_refetches") == 0
         and ctx.data.get("k_cancelled") == [True],
         f"inverses={ctx.data.get('k_calls')} relectures="
         f"{ctx.data.get('k_refetches')} Rewind annule="
         f"{ctx.data.get('k_cancelled')} : le clic Annuler doit arreter le "
         f"Rewind en cours, et rien ne doit tourner avant lui."),
        # ------------------------------------------------ L: lock
        # Hypothesis revised (v7): a Rewind no longer writes the journal
        # before its run (no invalidation), so a lock cannot leave the
        # journal out of step with the data there: checked, with the lock
        # on, that the dialog opens no write connection and every trace
        # stays active; the Rewind itself runs.
        ("L_invalidation_failure_stops_the_rewind",
         ctx.data.get("l_write_connections") == 0
         and all(lt.get(ctx.data["l_ids"][k]) == "active"
                 for k in ("u1", "u2", "u3", "u4"))
         and ctx.data.get("l_refetches") == 0
         and ctx.data.get("l_strict_runs") == 1,
         f"connexions d'ecriture={ctx.data.get('l_write_connections')} "
         f"traces={lt} relectures={ctx.data.get('l_refetches')} runs="
         f"{ctx.data.get('l_strict_runs')} : avant son run, un Rewind ne doit "
         f"rien ecrire au journal."),
        # Hypothesis revised (v7): the invalidation that can meet the lock is
        # the manual undo's (L2): its failure must be an ERROR for the user.
        ("L_invalidation_failure_is_an_error_for_the_user",
         bool(ctx.data.get("l2_calls"))
         and any(level == "ERROR" and "journal" in msg for msg, level in l2_logs),
         f"undo manuel sur journal verrouille : inverses="
         f"{ctx.data.get('l2_calls')} messages={l2_logs} : l'utilisateur "
         f"doit lire, en ERROR, que le journal n'a pas pu etre mis a jour."),
        ("K_cancel_is_said", any("nnul" in msg for msg, _l in k_logs),
         f"messages={k_logs} : l'arret doit etre annonce"),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
