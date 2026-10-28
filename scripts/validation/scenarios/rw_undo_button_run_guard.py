# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_undo_button_run_guard - aucun second run ne demarre tant qu'un run tient les couches.

Constats couverts : revue brev2 MAJOR (le bouton "Annuler le dernier
restore" reste arme sur le run d'une autre couche pendant un Rewind, et un
clic lance un UndoRunner concurrent) et item G du lot dialogue (le
selecteur de mode reste actif pendant un run, `recover_and_load` et
`_execute_version_restore` ne verifient pas eux-memes qu'un run est en
cours).

Mecanisme
---------
Le dialogue ne possede qu'UN runner (`_restore_runner`) et qu'un drapeau
(`_restore_in_progress`). Depuis que l'auto-undo ne passe plus que par les
couches du Rewind, un Rewind de B alors que le dernier run portait sur A
part directement : rien ne desarme le bouton, et `_undo_last_restore`
n'a aucune garde. Un clic lance un UndoRunner : `_launch_runner` ecrase la
reference du Rewind, `_on_undo_done` remet `_restore_in_progress` a False
et rouvre les controles pendant que le Rewind ecrit encore. Un second run
devient possible, un changement de projet n'arrete plus le Rewind (le
quiesce ne voit que `_restore_runner`), et le compte rendu du Rewind est
construit sans son runner : ses ecritures incompletes disparaissent.

Meme trou par d'autres portes : F5 (`recover_and_load`) ne regarde que
`_is_recovering`, que ni un restore d'evenements ni un Undo ne levent ; le
selecteur de mode reste cliquable et rouvre le bouton Rewind ; un restore
ou un Undo de session partent pendant un Undo.

Ce qui est prouve, sur les vraies methodes du dialogue et les vrais
runners (fournisseur, boites et thread de lecture scriptes) :

  P1  Rewind de 'voirie' en cours, dernier run sur 'parcelles' : bouton
      eteint, clic refuse et dit, le Rewind reste possede (quiesce, compte
      rendu avec ses ecritures incompletes), bouton rearme a la fin sur le
      nouveau dernier run.
  P2  restore d'evenements en cours : l'appel direct a l'Undo est refuse.
  G1  F5 pendant un Undo : aucune lecture ne demarre.
  G2  `_execute_version_restore` appele pendant un Undo : aucun Rewind.
  G3  restore d'evenements et Undo de session pendant un Undo : refuses.
  G4  selecteur de mode eteint pendant un run, rallume a sa fin.
  C   (revue brev2, MINOR) le dialogue est ferme ("Fermer quand meme")
      pendant qu'un resultat de lecture est encore en file : ce resultat,
      livre apres la fermeture, ne questionne ni n'ecrit.
  X   le corollaire de la garde : un restore d'evenements dont le
      fournisseur leve une exception finit quand meme (erreur nommee), et
      le dialogue n'en reste pas occupe pour toujours.

Verdict avant correctif (arbre de travail v7) : FAIL. P1 passe sur le code
d'origine (regression du premier correctif) ; P2 et G y echouent aussi
(trous d'origine).
"""
from __future__ import annotations

import sys
import tempfile
from datetime import timedelta
from pathlib import Path

SCENARIO_ID = "rw_undo_button_run_guard"
INVARIANT = "I-RUN-EXCLUSIVE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP_A = "ds_guard_parcelles"
_FP_V = "ds_guard_voirie"
_FP_R = "ds_guard_routes"
_SESSION = "sess-guard-0001"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _in_flight_strict(created):
    """StrictRestoreRunner qui ecrit encore : il ne finit que sur demande."""
    from recoverland.core.workflow_service import GroupedRestoreResult
    h = _h()

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

        def partial_restore_report(self):
            return (1, ("nom",))

        def apply_report(self):
            return {"applied": 0, "applied_partial": 1, "apply_skipped": 0,
                    "apply_fail": 0, "failed_other": 0, "cancelled": 0,
                    "plan_actions": 1}

        def finish(self):
            by_ds = {}
            for e in self.events:
                by_ds.setdefault(e.datasource_fingerprint, []).append(e)
            self.finished.emit(GroupedRestoreResult(
                total_ok=len(self.events), total_fail=0, errors=[],
                by_ds=by_ds, trace_events=[], applied=0))

    return InFlightStrict


def _refusals(logs):
    return [m for m, lvl in logs if lvl in ("WARNING", "ERROR") and "en cours" in m]


def _case_p1(ctx, directory):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    path = h.new_journal(directory, "p1.sqlite")
    a1 = h.insert_event(path, _FP_A, "parcelles", "UPDATE", 100, fid=1)
    h.insert_event(path, _FP_A, "parcelles", "UPDATE", 400, fid=1, restored_from=a1)
    b2 = h.insert_event(path, _FP_V, "voirie", "UPDATE", 600, fid=6)
    ev_a1, ev_b2 = h.fetch_events(path, [a1, b2])
    ctx.data["p1_ids"] = (a1, b2)
    cut = h.T0 + timedelta(seconds=50)
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_V], cutoff_dt=cut)
    dlg._last_restore_by_ds = {_FP_A: [ev_a1]}
    dlg._last_restore_events = [ev_a1]
    dlg._last_run_ids = {a1}
    dlg.undo_last_btn.setEnabled(True)
    dlg._version_restore_cutoff = h.cutoff(cut)
    dlg._active_restore_trace_id = "rw_guard_p1"
    dlg._rewind_scope_fps = [_FP_V]
    layers = {_FP_A: h.FakeLayer("lyr_parcelles", "parcelles"),
              _FP_V: h.FakeLayer("lyr_voirie", "voirie")}
    undo_calls, logs, asked, stricts = [], [], [], []
    seams = h.Seams()
    try:
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer",
                  lambda evt, _c=None: layers.get(evt.datasource_fingerprint))
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "StrictRestoreRunner", _in_flight_strict(stricts))
        dlg._is_recovering = True
        dlg._on_version_fetch_done([ev_b2])
        h.pump_until(lambda: bool(stricts), 5.0)
        strict = stricts[0] if stricts else None
        ctx.data["p1_owned_before"] = strict is not None and dlg._restore_runner is strict
        ctx.data["p1_btn_during"] = dlg.undo_last_btn.enabled
        n_logs = len(logs)
        dlg._undo_last_restore()
        h.pump_until(lambda: False, 0.3)
        ctx.data["p1_undo_calls"] = [e for _n, e in undo_calls]
        ctx.data["p1_refusal"] = _refusals(logs[n_logs:])
        ctx.data["p1_owned_after"] = strict is not None and dlg._restore_runner is strict
        ctx.data["p1_in_progress"] = bool(dlg._restore_in_progress)
        ctx.data["p1_recover_btn"] = dlg.recover_button.enabled
        # The rewind of 'voirie' ends, one entity written without 'nom'.
        n_logs = len(logs)
        if strict is not None:
            strict.finish()
        ctx.data["p1_report"] = logs[n_logs:]
        ctx.data["p1_btn_after"] = dlg.undo_last_btn.enabled
        ctx.data["p1_last_run"] = sorted(dlg._last_run_ids or ())
        # Same situation, the project switches: the quiesce must see it.
        stricts2 = []
        seams.set(rd, "StrictRestoreRunner", _in_flight_strict(stricts2))
        dlg._is_recovering = True
        dlg._on_version_fetch_done([ev_b2])
        h.pump_until(lambda: bool(stricts2), 5.0)
        dlg._undo_last_restore()
        if stricts2:
            dlg._quiesce_restore_runner("scenario_project_switch", timeout_ms=200)
            ctx.data["p1_quiesce_cancelled"] = stricts2[0]._cancelled
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_p2(ctx, directory):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    path = h.new_journal(directory, "p2.sqlite")
    e1 = h.insert_event(path, _FP_R, "routes", "UPDATE", 100, fid=1)
    h.insert_event(path, _FP_R, "routes", "UPDATE", 400, fid=1, restored_from=e1)
    e2 = h.insert_event(path, _FP_R, "routes", "UPDATE", 110, fid=2)
    ev_e1, ev_e2 = h.fetch_events(path, [e1, e2])
    ctx.data["p2_ids"] = (e1, e2)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP_R])
    dlg._last_restore_by_ds = {_FP_R: [ev_e1]}
    dlg._last_restore_events = [ev_e1]
    dlg._last_run_ids = {e1}
    dlg.undo_last_btn.setEnabled(True)
    layer = h.FakeLayer("lyr_routes", "routes")
    undo_calls, restore_calls, logs, asked, results, seen = [], [], [], [], [], []

    def on_call(_n):
        if not seen:
            seen.append((dlg.undo_last_btn.enabled,
                         dlg.restore_mode_selector.enabled))
            # A direct call, whatever the button shows: the guard itself.
            dlg._undo_last_restore()

    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch",
                  h.scripted_restore_batch({}, restore_calls, on_call=on_call))
        # Hypothesis revised (v7): the runners hand the executor the journal read
        # connection of the layer group (`journal=`, the published signature);
        # the doubles take it like the real functions, nothing else changes.
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        runner = rr.RestoreRunner([ev_e2], lambda _e: layer, wq, tracker=None,
                                  trace_id="rw_guard_p2")
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = [ev_e2]
        dlg._restore_in_progress = True
        dlg._launch_runner(runner)
        h.pump_until(lambda: bool(results), 5.0)
        h.pump_until(lambda: False, 0.3)
        ctx.data["p2_seen"] = seen[0] if seen else None
        ctx.data["p2_undo_calls"] = [e for _n, e in undo_calls]
        ctx.data["p2_refusal"] = _refusals(logs)
        ctx.data["p2_btn_after"] = dlg.undo_last_btn.enabled
        ctx.data["p2_selector_after"] = dlg.restore_mode_selector.enabled
    finally:
        seams.restore()
        h.close_dialog(dlg)


class _Recorder:
    def __init__(self, created):
        self.created = created

    def __call__(self, *args, **kwargs):
        h = _h()
        obj = h.recording_runner_class(self.created)(*args, **kwargs)
        obj.results_ready = h.Signal()
        obj.phase_changed = h.Signal()
        obj.error_occurred = h.Signal()
        obj.isRunning = lambda: False
        return obj


def _case_g(ctx, directory):
    """G1..G3: an Undo is running; every other entry point is tried."""
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    path = h.new_journal(directory, "g.sqlite")
    u1 = h.insert_event(path, _FP_R, "routes", "UPDATE", 100, fid=1,
                        session_id=_SESSION)
    h.insert_event(path, _FP_R, "routes", "UPDATE", 400, fid=1, restored_from=u1)
    u2 = h.insert_event(path, _FP_R, "routes", "UPDATE", 110, fid=2,
                        session_id=_SESSION)
    ev1, ev2 = h.fetch_events(path, [u1, u2])
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_R], cutoff_dt=h.T0 + timedelta(seconds=50))
    dlg._last_restore_by_ds = {_FP_R: [ev1]}
    dlg._last_restore_events = [ev1]
    dlg._last_run_ids = {u1}
    dlg.undo_last_btn.setEnabled(True)
    dlg._search_events = [ev2]
    dlg.selected_rows = [0]
    dlg.restore_mode_selector.mode = lambda: "temporal"
    layer = h.FakeLayer("lyr_routes", "routes")
    undo_calls, logs, asked, fetches, stricts, runners, searches, done = (
        [], [], [], [], [], [], [], [])
    h.PreflightDialog.shown = []
    probe = {}

    def on_apply(_eid):
        if probe:
            return
        undo_runner = dlg._restore_runner
        probe["selector_during"] = dlg.restore_mode_selector.enabled
        n = len(logs)
        dlg.recover_and_load()                       # G1: F5
        probe["g1_fetches"] = len(fetches)
        probe["g1_searches"] = len(searches)
        probe["g1_said"] = _refusals(logs[n:])
        n = len(logs)
        dlg._execute_version_restore([ev2])          # G2: straight in
        probe["g2_stricts"] = len(stricts)
        probe["g2_owned"] = dlg._restore_runner is undo_runner
        probe["g2_in_progress"] = bool(dlg._restore_in_progress)
        probe["g2_said"] = _refusals(logs[n:])
        n = len(logs)
        dlg.restore_selected_data()                  # G3: event restore
        dlg._undo_session(_SESSION)                  # G3: session undo
        probe["g3_runners"] = len(runners) + len(stricts)
        probe["g3_preflight"] = len(h.PreflightDialog.shown)
        probe["g3_said"] = _refusals(logs[n:])
        probe["owned_at_end"] = dlg._restore_runner is undo_runner

    seams = h.Seams()
    try:
        prepare, apply_one = h.scripted_undo({}, undo_calls, on_apply=on_apply)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "VersionFetchThread", h.fetch_thread_class([], fetches))
        seams.set(rd, "StrictRestoreRunner", h.recording_runner_class(stricts))
        seams.set(rd, "RestoreRunner", h.recording_runner_class(runners))
        seams.set(rd, "LocalSearchThread", _Recorder(searches))
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        original = dlg._on_undo_done

        def _record(res, _orig=original):
            _orig(res)
            done.append(res)
        dlg._on_undo_done = _record
        dlg._undo_last_restore()
        h.pump_until(lambda: bool(done), 5.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data["g"] = dict(probe)
        ctx.data["g_undo_calls"] = [e for _n, e in undo_calls]
        ctx.data["g_selector_after"] = dlg.restore_mode_selector.enabled
        ctx.data["g_ids"] = (u1, u2)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_c(ctx, directory):
    """C: a fetch result delivered after the dialog was closed."""
    import recoverland.recover_dialog as rd
    h = _h()
    path = h.new_journal(directory, "c.sqlite")
    b2 = h.insert_event(path, _FP_V, "voirie", "UPDATE", 600, fid=6)
    (ev_b2,) = h.fetch_events(path, [b2])
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_V], cutoff_dt=h.T0 + timedelta(seconds=50))
    dlg._version_restore_cutoff = h.cutoff(h.T0 + timedelta(seconds=50))
    asked, stricts, logs = [], [], []
    seams = h.Seams()
    try:
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "find_target_layer",
                  lambda _e, _c=None: h.FakeLayer("lyr_voirie", "voirie"))
        # "Fermer quand meme": the dialog is cleaned up while the fetch
        # thread's result is still queued; the result arrives afterwards.
        dlg.cleanup_resources()
        dlg._is_recovering = True
        dlg._on_version_fetch_done([ev_b2])
        ctx.data["c_asked"] = len(asked)
        ctx.data["c_runs"] = len(stricts)
        ctx.data["c_recovering"] = bool(dlg._is_recovering)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_x(ctx, directory):
    """X: the provider raises in the middle of an event restore."""
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    path = h.new_journal(directory, "x.sqlite")
    e1 = h.insert_event(path, _FP_R, "routes", "UPDATE", 100, fid=1)
    (ev,) = h.fetch_events(path, [e1])
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP_R])
    layer = h.FakeLayer("lyr_routes", "routes")
    logs, results = [], []

    def exploding_batch(*_a, **_k):
        raise RuntimeError("provider exploded (scripted)")

    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch", exploding_batch)
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        seams.set(rd, "find_target_layer", lambda _e, _c=None: layer)
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        runner = rr.RestoreRunner([ev], lambda _e: layer, wq, tracker=None,
                                  trace_id="rw_guard_x")
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = [ev]
        dlg._restore_in_progress = True
        dlg._launch_runner(runner)
        h.pump_until(lambda: bool(results), 3.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data["x_finished"] = bool(results)
        ctx.data["x_busy_after"] = dlg._run_in_flight() if hasattr(
            dlg, "_run_in_flight") else (
            "restore" if dlg._restore_in_progress or dlg._restore_runner else "")
        ctx.data["x_errors"] = list(getattr(results[0], "errors", []) or []) \
            if results else []
        ctx.data["x_selector_after"] = dlg.restore_mode_selector.enabled
    finally:
        seams.restore()
        h.close_dialog(dlg)


def run(ctx):
    from recoverland.core.logger import flog
    tmp = tempfile.TemporaryDirectory(prefix="rl_run_guard_")
    flog(f"rw_undo_button_run_guard run start: trace_id={ctx.trace_id}")
    try:
        _case_p1(ctx, tmp.name)
        _case_p2(ctx, tmp.name)
        _case_g(ctx, tmp.name)
        _case_c(ctx, tmp.name)
        _case_x(ctx, tmp.name)
    finally:
        flog(f"rw_undo_button_run_guard run end: trace_id={ctx.trace_id}")
        tmp.cleanup()


def assertions(ctx):
    d = ctx.data
    a1, b2 = d.get("p1_ids") or (None, None)
    report = " || ".join(f"{lvl}:{m}" for m, lvl in d.get("p1_report") or [])
    g = d.get("g") or {}
    u1, _u2 = d.get("g_ids") or (None, None)
    return [
        # ------------------------------------------------ P1
        ("P1_control_rewind_in_flight", d.get("p1_owned_before") is True,
         f"Rewind de 'voirie' possede par le dialogue={d.get('p1_owned_before')} "
         f"(controle)"),
        ("P1_undo_button_off_while_a_rewind_writes",
         d.get("p1_btn_during") is False,
         f"bouton pendant le Rewind={d.get('p1_btn_during')} : il reste arme "
         f"sur le run de 'parcelles' pendant que 'voirie' est ecrite."),
        ("P1_click_is_refused_and_said",
         d.get("p1_undo_calls") == [] and bool(d.get("p1_refusal")),
         f"inverses={d.get('p1_undo_calls')} refus={d.get('p1_refusal')} : un "
         f"UndoRunner est parti en parallele du Rewind, ou le refus est muet."),
        ("P1_rewind_still_owned",
         d.get("p1_owned_after") is True and d.get("p1_in_progress") is True
         and d.get("p1_recover_btn") is False,
         f"runner possede={d.get('p1_owned_after')} en_cours="
         f"{d.get('p1_in_progress')} bouton Rewind={d.get('p1_recover_btn')} : "
         f"le dialogue a oublie le Rewind qui ecrit et rouvre les controles."),
        ("P1_report_keeps_its_partial_writes",
         "sans tous leurs champs" in report,
         f"compte rendu={report!r} : l'entite reecrite sans 'nom' doit etre "
         f"dite ; lu sur le mauvais runner, le compte rendu la perd."),
        ("P1_button_rearmed_on_the_new_last_run",
         d.get("p1_btn_after") is True and d.get("p1_last_run") == [b2],
         f"bouton apres le Rewind={d.get('p1_btn_after')} dernier run="
         f"{d.get('p1_last_run')} attendu [{b2}] : a la fin de la chaine le "
         f"bouton pointe sur ce que ce Rewind a ecrit."),
        ("P1_project_switch_stops_the_rewind",
         d.get("p1_quiesce_cancelled") is True,
         f"Rewind annule par le changement de projet="
         f"{d.get('p1_quiesce_cancelled')} : il continuerait d'ecrire sur "
         f"les couches du projet ferme."),
        # ------------------------------------------------ P2
        ("P2_button_and_selector_off_during_event_restore",
         d.get("p2_seen") == (False, False),
         f"(bouton Undo, selecteur de mode) pendant le restore="
         f"{d.get('p2_seen')} attendu (False, False)."),
        ("P2_undo_refused_while_event_restore_writes",
         d.get("p2_undo_calls") == [] and bool(d.get("p2_refusal")),
         f"inverses={d.get('p2_undo_calls')} refus={d.get('p2_refusal')} : "
         f"l'annulation du run precedent tourne en parallele du restore sur "
         f"la meme couche."),
        ("P2_controls_back_at_chain_end",
         d.get("p2_btn_after") is True and d.get("p2_selector_after") is True,
         f"bouton={d.get('p2_btn_after')} selecteur={d.get('p2_selector_after')} "
         f"apres le restore : ils doivent revenir quand la chaine finit."),
        # ------------------------------------------------ G
        ("G_control_undo_ran", d.get("g_undo_calls") == [u1],
         f"inverses={d.get('g_undo_calls')} (controle : l'Undo a tourne)"),
        ("G1_f5_during_undo_starts_nothing",
         g.get("g1_fetches") == 0 and g.get("g1_searches") == 0
         and bool(g.get("g1_said")),
         f"lectures={g.get('g1_fetches')} recherches={g.get('g1_searches')} "
         f"refus={g.get('g1_said')} : F5 pendant l'Undo lance un Rewind "
         f"concurrent."),
        ("G2_execute_version_restore_refuses",
         g.get("g2_stricts") == 0 and g.get("g2_owned") is True
         and g.get("g2_in_progress") is True and bool(g.get("g2_said")),
         f"Rewind crees={g.get('g2_stricts')} Undo toujours possede="
         f"{g.get('g2_owned')} en_cours={g.get('g2_in_progress')} refus="
         f"{g.get('g2_said')} : l'entree du Rewind doit refuser elle-meme."),
        ("G3_restore_and_session_undo_refused",
         g.get("g3_runners") == 0 and g.get("g3_preflight") == 0
         and len(g.get("g3_said") or []) >= 2 and g.get("owned_at_end") is True,
         f"runners={g.get('g3_runners')} preflight={g.get('g3_preflight')} "
         f"refus={g.get('g3_said')} : un restore ou un Undo de session ne "
         f"doit pas partir pendant un Undo, et doit le dire."),
        ("G4_mode_selector_off_during_a_run",
         g.get("selector_during") is False and d.get("g_selector_after") is True,
         f"selecteur pendant l'Undo={g.get('selector_during')} apres="
         f"{d.get('g_selector_after')} : passer en mode Rewind pendant un run "
         f"rouvre le bouton Rewind."),
        # ------------------------------------------------ C
        ("C_closed_dialog_starts_no_rewind",
         d.get("c_asked") == 0 and d.get("c_runs") == 0
         and d.get("c_recovering") is False,
         f"questions={d.get('c_asked')} runs={d.get('c_runs')} garde levee="
         f"{d.get('c_recovering')} : un resultat de lecture arrive apres la "
         f"fermeture du dialogue ne doit ni questionner ni ecrire."),
        # ------------------------------------------------ X
        ("X_crashed_restore_still_ends_and_frees_the_dialog",
         d.get("x_finished") is True and d.get("x_busy_after") == ""
         and any("exploded" in e for e in d.get("x_errors") or [])
         and d.get("x_selector_after") is True,
         f"fin emise={d.get('x_finished')} encore occupe="
         f"{d.get('x_busy_after')!r} erreurs={d.get('x_errors')} selecteur="
         f"{d.get('x_selector_after')} : une exception du fournisseur ne "
         f"doit pas laisser le dialogue bloque sur un run qui ne finira "
         f"jamais (tout refuse, couche suspendue), et doit etre dite."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
