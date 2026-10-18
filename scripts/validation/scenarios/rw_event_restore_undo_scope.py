"""rw_event_restore_undo_scope - perimetre d'annulation d'un restore d'evenements.

Constats couverts : event-restore-undo-scope-unfiltered (CRITICAL) et
autoundo-modeA-overwrites-later-edits (HIGH).

Mecanisme
---------
`RestoreRunner._finish` publiait `by_ds=self._by_ds`, c'est-a-dire TOUS les
evenements selectionnes quel que soit leur sort, et le dialogue en faisait
le perimetre d'annulation (`_last_restore_by_ds`) des que `total_ok > 0`
(`total_ok` compte aussi les sauts). `restore_batch` emet en plus une trace
de restauration pour chaque succes, sauts idempotents compris. `UndoRunner`
applique ensuite l'inverse de chacun :

  * un restore de DELETE saute ("deja present") devient une suppression de
    l'entite qui etait LA avant le restore ;
  * un restore d'INSERT saute ("deja absent") devient une recreation d'une
    entite que l'utilisateur avait supprimee ;
  * un UPDATE refuse (le FID porte desormais une autre entite) devient une
    ecriture des valeurs NEW sur cet occupant, que le chemin aller venait
    justement de declarer sans rapport ;
  * un evenement du mode A refuse pour derive recoit, a l'undo (manuel ou
    silencieux avant le Rewind suivant), les valeurs NEW de l'evenement par
    dessus les editions posterieures de l'utilisateur.

Rien de tout cela n'est journalise (capture suspendue, l'undo n'ecrit pas
de trace) et le message dit "N entite(s) remise(s) dans leur etat initial".

Ce qui est prouve ici, sur le vrai `RestoreRunner`, le vrai `UndoRunner` et
les vraies methodes du dialogue (seules les ecritures fournisseur sont
scriptees) :

  R  un restore de 4 evenements (1 applique, 1 refuse sans cible, 1 saute,
     1 refuse pour derive) ne laisse dans le perimetre d'annulation et dans
     le journal QUE l'evenement reellement ecrit, et l'undo ne touche que
     lui ;
  C  une annulation en cours de run n'introduit jamais dans ce perimetre un
     evenement que le run n'a pas tente ;
  M  une edition de l'utilisateur sur une couche restauree retire cette
     couche du perimetre d'annulation, meme quand le dialogue est ferme
     (`on_events_committed` sortait avant, dialogue invisible : l'undo
     suivant ecrivait les valeurs NEW par-dessus l'edition).

Verdict pre-correctif : FAIL. Verdict post-correctif : PASS.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_event_restore_undo_scope"
INVARIANT = "I-UNDO-SCOPE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_event_undo_scope"
_LAYER = "parcelles"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def setup(ctx):
    h = _h()
    tmp = tempfile.TemporaryDirectory(prefix="rl_event_undo_scope_")
    ctx.data["tmp"] = tmp
    path = h.new_journal(tmp.name)
    ctx.data["path"] = path
    # Case R: four selected events, four different fates.
    ids = {
        "A_delete_applied": h.insert_event(path, _FP, _LAYER, "DELETE", 10, fid=1),
        "B_update_refused": h.insert_event(path, _FP, _LAYER, "UPDATE", 20, fid=7),
        "D_insert_skipped": h.insert_event(path, _FP, _LAYER, "INSERT", 30, fid=3),
        "E_update_drift": h.insert_event(path, _FP, _LAYER, "UPDATE", 40, fid=4),
    }
    ctx.data["ids"] = ids
    # Case C: eleven events, cancelled while the second chunk runs.
    ctx.data["cancel_ids"] = [
        h.insert_event(path, _FP, _LAYER, "DELETE", 100 + i, fid=100 + i)
        for i in range(11)
    ]


def _run_case_r(ctx, h, rd, rr):
    ids = ctx.data["ids"]
    path = ctx.data["path"]
    events = h.fetch_events(path, list(ids.values()))
    journal = h.Journal(path)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, journal, wq, [_FP])
    layer = h.FakeLayer("lyr_parcelles", _LAYER)
    outcomes = {
        ids["A_delete_applied"]: "applied",
        ids["B_update_refused"]: "target_absent",
        ids["D_insert_skipped"]: "skipped",
        ids["E_update_drift"]: "drift",
    }
    restore_calls, undo_calls, asked, logs = [], [], [], []
    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch",
                  h.scripted_restore_batch(outcomes, restore_calls))
        # Hypothesis revised (v7): the runners hand the executor the journal read
        # connection of the layer group (`journal=`, the published signature);
        # the doubles take it like the real functions, nothing else changes.
        seams.set(rr, "build_fid_cache", lambda _layer, _events, journal=None: {})
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer", lambda _evt, _conn=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((msg, level)))

        # Launched exactly like restore_selected_data launches it.
        runner = rr.RestoreRunner(events, lambda _e: layer, wq, tracker=None,
                                  trace_id=ctx.trace_id)
        results = []
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = events
        dlg._restore_runner = runner
        runner.start()
        h.pump_until(lambda: bool(results), 10.0)
        ctx.data["r_finished"] = bool(results)
        result = results[0] if results else None
        ctx.data["r_result_scope"] = sorted(h.all_ids(result.by_ds)) if result else None
        ctx.data["r_report"] = dict(runner.apply_report()) if hasattr(
            runner, "apply_report") else {}
        ctx.data["r_traces"] = sorted(
            i for ids_, _jp, _stopped in wq.enqueued for i in ids_ if i)
        ctx.data["r_undo_state"] = sorted(h.all_ids(dlg._last_restore_by_ds))

        # The user clicks Undo. The real handler asks, then runs UndoRunner
        # on the stored scope; the scripted provider records each inverse.
        done = []
        original = dlg._on_undo_done

        def _record_done(res, _orig=original):
            _orig(res)
            done.append(res)
        dlg._on_undo_done = _record_done
        if dlg._last_restore_by_ds:
            dlg._undo_last_restore()
            h.pump_until(lambda: bool(done), 10.0)
        ctx.data["r_undo_calls"] = sorted(eid for _name, eid in undo_calls)
        ctx.data["r_traces_after_undo"] = h.trace_state(path, list(ids.values()))
        ctx.data["r_errors"] = [m for m, level in logs if level == "ERROR"]
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _run_case_c(ctx, h, rd, rr):
    path = ctx.data["path"]
    cancel_ids = ctx.data["cancel_ids"]
    events = h.fetch_events(path, cancel_ids)
    journal = h.Journal(path)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, journal, wq, [_FP])
    layer = h.FakeLayer("lyr_parcelles", _LAYER)
    restore_calls, logs, asked = [], [], []
    snapshots = {}

    def on_call(n_before):
        # The second chunk is running: the user clicks Cancel.
        if n_before == 5 and "at_cancel" not in snapshots:
            dlg.cancel_operation()
            snapshots["at_cancel"] = sorted(h.all_ids(dlg._last_restore_by_ds))

    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch",
                  h.scripted_restore_batch({}, restore_calls, on_call=on_call))
        seams.set(rr, "build_fid_cache", lambda _layer, _events, journal=None: {})
        seams.set(rd, "find_target_layer", lambda _evt, _conn=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((msg, level)))
        runner = rr.RestoreRunner(events, lambda _e: layer, wq, tracker=None,
                                  trace_id=ctx.trace_id)
        results = []
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = events
        dlg._restore_runner = runner
        dlg._restore_in_progress = True
        runner.start()
        h.pump_until(lambda: bool(results), 10.0)
        ctx.data["c_attempted"] = sorted(restore_calls)
        ctx.data["c_at_cancel"] = snapshots.get("at_cancel")
        ctx.data["c_final"] = sorted(h.all_ids(dlg._last_restore_by_ds))
        ctx.data["c_finished"] = bool(results)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _run_case_m(ctx, h, rd):
    """M: the user edits a restored layer while the dialog is closed."""
    path = ctx.data["path"]
    ids = ctx.data["ids"]
    journal = h.Journal(path)
    dlg = h.build_dialog(rd, journal, h.RecordingWriteQueue(path), [_FP])
    try:
        ev = h.fetch_events(path, [ids["A_delete_applied"]])
        dlg._last_restore_by_ds = {_FP: list(ev)}
        dlg._last_restore_events = list(ev)
        dlg.undo_last_btn.setEnabled(True)
        # The capture flushed a commit of the user on that layer; the plugin
        # notifies the dialog, which is hidden (isVisible() is False).
        dlg.on_events_committed(_FP)
        ctx.data["m_scope_after_edit"] = sorted(h.all_ids(dlg._last_restore_by_ds))
        ctx.data["m_button_after_edit"] = dlg.undo_last_btn.enabled
    finally:
        h.close_dialog(dlg)


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog

    h = _h()
    flog(f"rw_event_restore_undo_scope run start: trace_id={ctx.trace_id}")
    try:
        _run_case_r(ctx, h, rd, rr)
        _run_case_c(ctx, h, rd, rr)
        _run_case_m(ctx, h, rd)
    finally:
        flog(f"rw_event_restore_undo_scope run end: trace_id={ctx.trace_id}")
        ctx.data["tmp"].cleanup()


def assertions(ctx):
    ids = ctx.data["ids"]
    applied = ids["A_delete_applied"]
    refused = ids["B_update_refused"]
    skipped = ids["D_insert_skipped"]
    drift = ids["E_update_drift"]
    report = ctx.data.get("r_report") or {}
    undo_calls = ctx.data.get("r_undo_calls") or []
    attempted = set(ctx.data.get("c_attempted") or [])
    never_attempted = set(ctx.data["cancel_ids"]) - attempted
    at_cancel = ctx.data.get("c_at_cancel")
    final_c = set(ctx.data.get("c_final") or [])

    return [
        ("R_run_finished", ctx.data.get("r_finished") is True,
         "le RestoreRunner doit emettre finished ; sinon rien ne peut etre "
         "affirme sur ce qu'il publie"),
        ("R_published_scope_is_the_written_event_only",
         ctx.data.get("r_result_scope") == [applied],
         f"result.by_ds={ctx.data.get('r_result_scope')} attendu [{applied}] : "
         f"le perimetre publie par le runner doit se limiter a ce qu'il a "
         f"ecrit. Y laisser l'UPDATE refuse ({refused}), l'INSERT saute "
         f"({skipped}) et l'UPDATE en derive ({drift}), c'est programmer "
         f"leur inverse sur des entites que le restore n'a jamais touchees."),
        ("R_journal_traces_only_for_the_written_event",
         ctx.data.get("r_traces") == [applied],
         f"traces journalisees pour {ctx.data.get('r_traces')} attendu "
         f"[{applied}] : une trace dit 'cette compensation est dans les "
         f"donnees'. Pour le saut {skipped} rien n'a ete ecrit ; la trace "
         f"ferait croire a un Rewind ou a un undo que l'entite est la par "
         f"notre fait, et un undo reconstruit depuis le journal la "
         f"supprimerait."),
        ("R_counters_unchanged_non_regression",
         report.get("applied") == 1 and report.get("skipped_idempotent") == 1
         and report.get("failed_target_absent") == 1
         and report.get("failed_geometry_drift") == 1,
         f"apply_report={report} : les compteurs du compte rendu ne doivent "
         f"pas bouger, seul le perimetre d'annulation est en cause"),
        ("R_dialog_undo_scope_is_the_written_event_only",
         ctx.data.get("r_undo_state") == [applied],
         f"_last_restore_by_ds={ctx.data.get('r_undo_state')} attendu "
         f"[{applied}] : le bouton Annuler (et l'auto-undo avant le Rewind "
         f"suivant) reprend exactement ce perimetre."),
        ("R_undo_reverts_only_what_the_restore_wrote",
         undo_calls == [applied],
         f"inverses appliques sur {undo_calls} attendu [{applied}] : "
         f"l'undo a ecrit sur {sorted(set(undo_calls) - {applied})}, c'est "
         f"a dire supprime/recree/ecrase des entites que le restore n'avait "
         f"pas modifiees, sans aucune trace dans le journal."),
        ("R_modeA_refused_events_never_undone",
         refused not in undo_calls and drift not in undo_calls,
         f"inverses={undo_calls} : l'UPDATE refuse ({refused}) et l'UPDATE "
         f"en derive ({drift}) ont ete 'annules' ; leurs valeurs NEW "
         f"ecrasent les editions faites depuis par l'utilisateur."),
        ("R_journal_after_undo_matches_the_data",
         (ctx.data.get("r_traces_after_undo") or {}).get(applied) == "invalidated"
         and all((ctx.data.get("r_traces_after_undo") or {}).get(i) == "none"
                 for i in (refused, skipped, drift))
         and not ctx.data.get("r_errors"),
         f"traces apres undo={ctx.data.get('r_traces_after_undo')} erreurs="
         f"{ctx.data.get('r_errors')} : seule la compensation de {applied} a "
         f"existe puis a ete defaite ; le journal ne doit porter aucune "
         f"trace pour les trois autres, et l'undo doit le constater sans "
         f"erreur."),
        ("M_user_edit_drops_the_layer_undo_scope_even_dialog_closed",
         ctx.data.get("m_scope_after_edit") == []
         and ctx.data.get("m_button_after_edit") is False,
         f"perimetre apres une edition de l'utilisateur="
         f"{ctx.data.get('m_scope_after_edit')} bouton="
         f"{ctx.data.get('m_button_after_edit')} : la couche a ete modifiee "
         f"apres le restore. Son undo (ou l'auto-undo du Rewind suivant) "
         f"ecrirait les valeurs NEW de l'evenement par-dessus cette edition. "
         f"Le dialogue ferme ne doit pas empecher d'oublier ce perimetre."),
        ("C_run_finished", ctx.data.get("c_finished") is True,
         "le runner annule doit quand meme aller jusqu'a finished"),
        ("C_cancel_never_stores_unattempted_events",
         at_cancel is not None and not (set(at_cancel) & never_attempted),
         f"perimetre au clic Annuler={at_cancel}, jamais tentes="
         f"{sorted(never_attempted)} : le bouton Annuler etait rearme sur "
         f"des evenements que le run n'a pas encore (ou jamais) ecrits ; un "
         f"clic a ce moment appliquait leur inverse."),
        ("C_final_scope_is_what_was_written",
         bool(attempted) and final_c == attempted and not (final_c & never_attempted),
         f"perimetre final={sorted(final_c)} tentes={sorted(attempted)} "
         f"jamais tentes={sorted(never_attempted)} : apres une annulation, "
         f"seul ce qui a ete ecrit peut etre annule."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
