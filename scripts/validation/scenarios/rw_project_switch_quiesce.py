"""rw_project_switch_quiesce - un changement de projet arrete le runner avant que le backend ne s'arrete.

Constat couvert : project-switch-does-not-quiesce-runner (MEDIUM).

Mecanisme
---------
`unload` libere le dialogue (et annule son runner) AVANT d'arreter le
backend. Le changement de projet ne le faisait pas : `QgsProject.cleared`
(emis aussi a l'ouverture d'un autre projet) arrete la file d'ecriture et
ferme le journal, puis appelle `on_project_switched`, qui ne touchait pas au
runner en cours. Celui-ci continuait donc, tick apres tick :

  * il ecrivait encore sur les couches du projet A pendant et apres leur
    fermeture ;
  * ses traces partaient a la fin du run, apres l'arret du backend (dans le
    journal du projet suivant, ou nulle part si la chaine mourait sur une
    couche supprimee) : le Rewind suivant dans A rejouait ces restaurations ;
  * son slot de fin rearmait le bouton Annuler du projet B avec le
    perimetre du projet A.

Ce qui est prouve (vrai `RestoreRunner` lance par la vraie methode
`restore_selected_data`, vrai signal `QgsProject.instance().clear()`, et
un greffon de plugin qui fait ce que fait recover.py sur `cleared` :
arreter la file puis appeler `on_project_switched`) :

  Q1  les traces du run partent pendant que le backend du projet A vit ;
  Q2  aucune ecriture sur les couches apres le debut du changement ;
  Q3  le projet B n'herite d'aucun perimetre d'annulation du projet A ;
  Q4  `on_project_switched` seul (sans le crochet) arrete aussi le runner.

Verdict pre-correctif : FAIL. Verdict post-correctif : PASS.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_project_switch_quiesce"
INVARIANT = "I-CAPTURE-ROUTING"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_switch_cadastre"
_N_EVENTS = 11   # three chunks of the event runner: 5 + 5 + 1


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _seed(h, directory, name):
    path = h.new_journal(directory, name)
    ids = [h.insert_event(path, _FP, "cadastre", "DELETE", 100 + i, fid=10 + i)
           for i in range(_N_EVENTS)]
    return path, ids


def _setup_dialog(h, rd, path, ids):
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP])
    ev = h.fetch_events(path, ids)
    dlg._search_events = list(ev)
    dlg.selected_rows = list(range(len(ev)))
    return dlg, wq


def _case_signal(ctx, h, rd, rr):
    """Q1..Q3: the real project clear, while the runner is between chunks."""
    from qgis.core import QgsProject

    path, ids = _seed(h, ctx.data["tmp"].name, "signal.sqlite")
    dlg, wq = _setup_dialog(h, rd, path, ids)
    layer = h.FakeLayer("lyr_cadastre", "cadastre")
    calls, logs, asked, switch = [], [], [], {"at": None, "calls_at": None}
    project = QgsProject.instance()

    def plugin_on_cleared():
        # What recover.py does on `cleared`: backend down, then notify.
        switch["calls_at"] = len(calls)
        wq.stop()
        dlg._journal.is_open = False
        dlg.on_project_switched(tracker=None)

    def on_call(n_before):
        # First chunk: the user opens another project; the clear lands
        # between two ticks of the runner, as a real click would.
        if n_before == 0:
            from qgis.PyQt.QtCore import QTimer
            QTimer.singleShot(0, project.clear)

    seams = h.Seams()
    project.cleared.connect(plugin_on_cleared)
    try:
        h.PreflightDialog.shown = []
        seams.set(rr, "restore_batch",
                  h.scripted_restore_batch({}, calls, on_call=on_call))
        # Hypothesis revised (v7): the runners hand the executor the journal read
        # connection of the layer group (`journal=`, the published signature);
        # the doubles take it like the real functions, nothing else changes.
        seams.set(rr, "build_fid_cache", lambda _layer, _events, journal=None: {})
        seams.set(rd, "find_target_layer", lambda _evt, _conn=None: layer)
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        dlg.restore_selected_data()
        runner = dlg._restore_runner
        finished = []
        if runner is not None:
            runner.finished.connect(finished.append)
        h.pump_until(lambda: bool(finished), 10.0)
        h.pump_until(lambda: False, 0.3)
        ctx.data["q_finished"] = bool(finished)
        ctx.data["q_calls_total"] = len(calls)
        ctx.data["q_calls_at_switch"] = switch["calls_at"]
        ctx.data["q_enqueued"] = [(len(i), stopped) for i, _jp, stopped in wq.enqueued]
        ctx.data["q_scope_after"] = sorted(h.all_ids(dlg._last_restore_by_ds))
        ctx.data["q_undo_enabled"] = dlg.undo_last_btn.enabled
    finally:
        try:
            project.cleared.disconnect(plugin_on_cleared)
        except (TypeError, RuntimeError):
            pass
        hook = getattr(dlg, "_on_project_about_to_clear", None)
        if hook is not None:
            try:
                project.aboutToBeCleared.disconnect(hook)
            except (TypeError, RuntimeError):
                pass
        seams.restore()
        h.close_dialog(dlg)


def _case_direct(ctx, h, rd, rr):
    """Q4: recover.py's own notification, with no hook armed beforehand."""
    path, ids = _seed(h, ctx.data["tmp"].name, "direct.sqlite")
    dlg, wq = _setup_dialog(h, rd, path, ids)
    layer = h.FakeLayer("lyr_cadastre", "cadastre")
    calls, logs, asked, state = [], [], [], {}

    def on_call(n_before):
        if n_before == 0:
            from qgis.PyQt.QtCore import QTimer

            def _switch():
                state["calls_at"] = len(calls)
                wq.stop()
                dlg._journal.is_open = False
                dlg.on_project_switched(tracker=None)
            QTimer.singleShot(0, _switch)

    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch",
                  h.scripted_restore_batch({}, calls, on_call=on_call))
        seams.set(rr, "build_fid_cache", lambda _layer, _events, journal=None: {})
        seams.set(rd, "find_target_layer", lambda _evt, _conn=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        events = list(dlg._search_events)
        runner = rr.RestoreRunner(events, lambda _e: layer, wq, tracker=None,
                                  trace_id=ctx.trace_id)
        finished = []
        runner.finished.connect(finished.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = events
        dlg._restore_runner = runner
        dlg._restore_in_progress = True
        runner.start()
        h.pump_until(lambda: bool(finished), 10.0)
        h.pump_until(lambda: False, 0.3)
        ctx.data["d_calls_total"] = len(calls)
        ctx.data["d_calls_at_switch"] = state.get("calls_at")
        ctx.data["d_scope_after"] = sorted(h.all_ids(dlg._last_restore_by_ds))
        ctx.data["d_finished"] = bool(finished)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog

    h = _h()
    ctx.data["tmp"] = tempfile.TemporaryDirectory(prefix="rl_switch_")
    flog(f"rw_project_switch_quiesce run start: trace_id={ctx.trace_id}")
    try:
        _case_signal(ctx, h, rd, rr)
        _case_direct(ctx, h, rd, rr)
    finally:
        flog(f"rw_project_switch_quiesce run end: trace_id={ctx.trace_id}")
        ctx.data["tmp"].cleanup()


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    enq = ctx.data.get("q_enqueued") or []
    at_switch = ctx.data.get("q_calls_at_switch")
    total = ctx.data.get("q_calls_total")
    d_at = ctx.data.get("d_calls_at_switch")
    d_total = ctx.data.get("d_calls_total")
    return [
        ("Q_run_started_and_finished",
         ctx.data.get("q_finished") is True and at_switch is not None,
         f"finished={ctx.data.get('q_finished')} switch_at={at_switch} : le "
         f"run doit demarrer et le changement de projet survenir en cours "
         f"(controle du scenario)"),
        ("Q1_traces_delivered_while_backend_alive",
         bool(enq) and all(not stopped for _n, stopped in enq),
         f"lots de traces (taille, file_arretee)={enq} : les traces du run "
         f"doivent partir AVANT que le backend du projet A ne s'arrete ; "
         f"apres, elles atterrissent ailleurs ou nulle part et le Rewind "
         f"suivant dans A rejoue ces restaurations."),
        ("Q2_no_write_after_switch_began",
         at_switch is not None and total == at_switch,
         f"ecritures au changement={at_switch}, au total={total} : "
         f"{(total or 0) - (at_switch or 0)} evenement(s) ecrits sur les "
         f"couches du projet A apres le debut de sa fermeture."),
        ("Q3_new_project_inherits_no_undo_scope",
         ctx.data.get("q_scope_after") == []
         and ctx.data.get("q_undo_enabled") is False,
         f"perimetre d'annulation dans le projet B="
         f"{ctx.data.get('q_scope_after')} bouton="
         f"{ctx.data.get('q_undo_enabled')} : un clic Annuler dans B "
         f"appliquerait les inverses d'un restore fait dans A."),
        ("Q4_on_project_switched_alone_stops_the_runner",
         ctx.data.get("d_finished") is True and d_at is not None
         and d_total == d_at and ctx.data.get("d_scope_after") == [],
         f"sans crochet : ecritures au changement={d_at}, au total="
         f"{d_total}, perimetre apres={ctx.data.get('d_scope_after')} ; "
         f"on_project_switched doit annuler et terminer le runner lui-meme."),
        assert_log_contains(ctx.records, r"quiesce_runner: .*reason=",
                            name="Q_quiesce_is_logged"),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
