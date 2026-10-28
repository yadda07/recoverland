# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_dialog_scope_misc - trois cablages du dialogue et du runner d'evenements.

Items E, F et H du lot dialogue.

H  `_invalidate_undo_for` comparait l'empreinte notifiee a la lettre avec
   les cles de la reserve d'annulation. Une compensation rangee sous une
   empreinte que le journal declare equivalente (alias v6 : couche
   re-pointee, ancienne empreinte) n'etait pas retiree quand l'utilisateur
   editait la couche : le bouton "Annuler le dernier restore" restait arme
   et proposait d'ecrire les valeurs NEW du restore par-dessus l'edition.
   Prouve : la reserve est rangee sous l'empreinte cible, la notification
   porte l'alias ; l'entree doit tomber et le bouton s'eteindre.

F  `RestoreRunner` deduisait les echecs "ecrits quand meme" (v7 `landed`)
   de la presence d'une trace, faute de liste dans `RestoreReport`. Le
   contrat v7 publie `landed` : le runner doit le lire, et ne deduire des
   traces que pour un rapport ancien qui ne le porte pas. Un echec ecrit
   sans trace dans le rapport sortait du perimetre d'annulation et du
   journal alors que le fichier avait change. Le compte rendu le comptait
   "pour une autre raison" ; il doit dire que le fichier a ete ecrit.
   Prouve avec un `restore_batch` scripte au contrat v7 (la classe
   `RestoreReport` du coeur ne porte pas encore le champ).

E  `restore_selected_data` appelait `preflight_check(plan)` sans connexion
   au journal : l'horizon de purge durable ne pouvait pas etre lu. Pour un
   plan du mode A (sans date de coupure) ce controle ne s'applique pas
   aujourd'hui ; le cablage est verifie pour qu'il le soit des qu'un plan
   en porte une.

W  Suite de la suspension par couche (rw_capture_suppression_scope) : une
   couche deja ecrite est rendue a la capture pendant que le run continue,
   l'utilisateur y enregistre une edition, qui est journalisee. Cette
   couche ne doit pas entrer, a la fin du run, dans ce que "Annuler le
   dernier restore" defait : l'edition de l'utilisateur serait ecrasee par
   les valeurs du restore. Avant : `_invalidate_undo_for` ne voyait que la
   reserve d'avant le run, et la fin du run y versait la couche editee.

Verdict avant correctif : FAIL (H, F, E, W). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
import types
from pathlib import Path

SCENARIO_ID = "rw_dialog_scope_misc"
INVARIANT = "I-UNDO-SCOPE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP_OLD = "ds_misc_parcelles_v5"
_FP_NEW = "ds_misc_parcelles_v6"
_FP_R = "ds_misc_routes"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _case_h(ctx, directory):
    import recoverland.recover_dialog as rd
    from recoverland.core.datasource_alias import add_alias
    h = _h()
    path = h.new_journal(directory, "h.sqlite")
    e1 = h.insert_event(path, _FP_OLD, "parcelles", "UPDATE", 100, fid=1)
    h.insert_event(path, _FP_OLD, "parcelles", "UPDATE", 400, fid=1, restored_from=e1)
    conn = sqlite3.connect(path)
    try:
        ctx.data["h_alias_added"] = add_alias(conn, _FP_NEW, _FP_OLD, "scenario")
    finally:
        conn.close()
    (ev,) = h.fetch_events(path, [e1])
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path), [_FP_OLD])
    dlg._last_restore_by_ds = {_FP_OLD: [ev]}
    dlg._last_restore_events = [ev]
    dlg._last_run_ids = {e1}
    dlg.undo_last_btn.setEnabled(True)
    try:
        # The user saves an edit on the layer, known now under its v6 print.
        dlg.on_events_committed(_FP_NEW)
        ctx.data["h_pool_after"] = h.by_ds_ids(dlg._last_restore_by_ds)
        ctx.data["h_button_after"] = dlg.undo_last_btn.enabled
        ctx.data["h_manual_scope"] = h.by_ds_ids(dlg._manual_undo_scope())
    finally:
        h.close_dialog(dlg)


def _v7_report(succeeded, failed, landed, traces=()):
    return types.SimpleNamespace(
        succeeded=list(succeeded), failed=dict(failed),
        total_requested=len(succeeded) + len(failed), trace_events=tuple(traces),
        skipped_idempotent=[], failed_target_absent=[], failed_geometry_drift=[],
        landed=list(landed))


def _case_f(ctx, directory):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    path = h.new_journal(directory, "f.sqlite")
    r1 = h.insert_event(path, _FP_R, "routes", "UPDATE", 100, fid=1)
    r2 = h.insert_event(path, _FP_R, "routes", "UPDATE", 110, fid=2)
    ev1, ev2 = h.fetch_events(path, [r1, r2])
    ctx.data["f_ids"] = (r1, r2)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP_R])
    layer = h.FakeLayer("lyr_routes", "routes")
    logs, results = [], []

    # Hypothesis revised (v7): the runners hand the executor the journal read
    # connection of the layer group (`journal=`, the published signature);
    # the doubles take it like the real functions, nothing else changes.
    def batch(_layer, events, fid_cache=None, trace_id="", journal=None):
        # r1 applied (with its trace); r2 FAILED, but the provider kept the
        # write (read-back contradicts it): v7 `landed`, no trace built.
        ok = [e for e in events if e.event_id == r1]
        bad = [e for e in events if e.event_id == r2]
        return _v7_report(
            [e.event_id for e in ok],
            {e.event_id: "FAILED_POSTCOMMIT_VERIFICATION: read-back differs "
                         "(scripted)" for e in bad},
            [e.event_id for e in bad],
            [h.trace_for(e) for e in ok])

    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch", batch)
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        runner = rr.RestoreRunner([ev2, ev1], lambda _e: layer, wq, tracker=None,
                                  trace_id="rw_misc_f")
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = [ev2, ev1]
        dlg._restore_in_progress = True
        dlg._launch_runner(runner)
        h.pump_until(lambda: bool(results), 5.0)
        h.pump_until(lambda: False, 0.3)
        res = results[0] if results else None
        ctx.data["f_scope"] = sorted(h.all_ids(getattr(res, "by_ds", {})))
        ctx.data["f_journalled"] = sorted(
            i for ids, _p, _s in wq.enqueued for i in ids if i)
        ctx.data["f_breakdown"] = runner.apply_report()
        ctx.data["f_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_e(ctx, directory):
    import recoverland.recover_dialog as rd
    h = _h()
    path = h.new_journal(directory, "e.sqlite")
    r1 = h.insert_event(path, _FP_R, "routes", "UPDATE", 100, fid=1)
    (ev,) = h.fetch_events(path, [r1])
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path), [_FP_R])
    dlg._search_events = [ev]
    dlg.selected_rows = [0]
    calls, runners, logs = [], [], []
    real = rd.preflight_check

    def recording_preflight(plan, *args, **kwargs):
        conn = kwargs.get("conn")
        live = False
        if conn is not None:
            try:
                conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()
                live = True
            except sqlite3.Error:
                live = False
        calls.append({"conn_passed": conn is not None, "conn_live": live})
        return real(plan, *args, **kwargs)

    seams = h.Seams()
    h.PreflightDialog.shown = []
    try:
        seams.set(rd, "preflight_check", recording_preflight)
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        seams.set(rd, "RestoreRunner", h.recording_runner_class(runners))
        seams.set(rd, "find_target_layer",
                  lambda _evt, _c=None: h.FakeLayer("lyr_routes", "routes"))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        dlg.restore_selected_data()
        ctx.data["e_calls"] = list(calls)
        ctx.data["e_runner_started"] = len(runners)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_w(ctx, directory):
    """W: the user saves an edit on a layer the restore has already written."""
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    fp_a, fp_b = "ds_misc_w_routes", "ds_misc_w_bati"
    path = h.new_journal(directory, "w.sqlite")
    a1 = h.insert_event(path, fp_a, "routes", "UPDATE", 100, fid=1)
    b1 = h.insert_event(path, fp_b, "bati", "UPDATE", 110, fid=2)
    ev_a, ev_b = h.fetch_events(path, [a1, b1])
    ctx.data["w_ids"] = (a1, b1)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [fp_a, fp_b])
    layers = {fp_a: h.FakeLayer("lyr_routes", "routes"),
              fp_b: h.FakeLayer("lyr_bati", "bati")}
    calls, results = [], []

    def on_call(n):
        # Second batch: 'routes' is written and given back to the capture;
        # the user saves an edit there, recover.py forwards the commit.
        if n == 1:
            dlg.on_events_committed(fp_a)

    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch",
                  h.scripted_restore_batch({}, calls, on_call=on_call))
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        seams.set(rd, "find_target_layer",
                  lambda evt, _c=None: layers.get(evt.datasource_fingerprint))
        seams.set(rd, "qlog", lambda *_a, **_k: None)
        runner = rr.RestoreRunner([ev_a, ev_b],
                                  lambda e: layers.get(e.datasource_fingerprint),
                                  wq, tracker=None, trace_id="rw_misc_w")
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = [ev_a, ev_b]
        dlg._restore_in_progress = True
        dlg._launch_runner(runner)
        h.pump_until(lambda: bool(results), 5.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data["w_written"] = sorted(h.all_ids(getattr(results[0], "by_ds", {}))) \
            if results else None
        ctx.data["w_manual_scope"] = h.by_ds_ids(dlg._manual_undo_scope())
    finally:
        seams.restore()
        h.close_dialog(dlg)


def run(ctx):
    from recoverland.core.logger import flog
    tmp = tempfile.TemporaryDirectory(prefix="rl_dialog_misc_")
    flog(f"rw_dialog_scope_misc run start: trace_id={ctx.trace_id}")
    try:
        _case_h(ctx, tmp.name)
        _case_f(ctx, tmp.name)
        _case_e(ctx, tmp.name)
        _case_w(ctx, tmp.name)
    finally:
        flog(f"rw_dialog_scope_misc run end: trace_id={ctx.trace_id}")
        tmp.cleanup()


def assertions(ctx):
    d = ctx.data
    r1, r2 = d.get("f_ids") or (None, None)
    bd = d.get("f_breakdown") or {}
    f_msgs = " || ".join(f"{lvl}:{m}" for m, lvl in d.get("f_logs") or [])
    e_calls = d.get("e_calls") or []
    return [
        ("H_control_alias_registered", d.get("h_alias_added") is True,
         "alias v6 -> v5 enregistre (controle)"),
        ("H_edit_under_an_alias_drops_the_undo_state",
         d.get("h_pool_after") == {} and d.get("h_manual_scope") == {},
         f"reserve apres l'edition={d.get('h_pool_after')} perimetre du bouton="
         f"{d.get('h_manual_scope')} : la couche editee est la meme source ; "
         f"garder sa compensation, c'est proposer d'ecrire les valeurs NEW du "
         f"restore par-dessus l'edition de l'utilisateur."),
        ("H_button_off_after_the_edit", d.get("h_button_after") is False,
         f"bouton apres l'edition={d.get('h_button_after')}"),
        ("F_landed_failure_stays_undoable",
         d.get("f_scope") == sorted([r1, r2]),
         f"perimetre d'annulation={d.get('f_scope')} attendu {sorted([r1, r2])} : "
         f"le fichier porte l'ecriture de {r2} (liste `landed` du rapport) ; "
         f"hors perimetre, l'Undo ne peut plus la defaire."),
        ("F_landed_failure_is_journalled",
         d.get("f_journalled") == sorted([r1, r2]),
         f"traces envoyees au journal={d.get('f_journalled')} : sans trace le "
         f"Rewind suivant rejoue la compensation par-dessus l'ecriture."),
        ("F_landed_failure_has_its_bucket",
         bd.get("failed_postcommit_verification") == 1 and bd.get("failed") == 0
         and bd.get("apply_fail") == 1 and bd.get("applied") == 1,
         f"seaux={ {k: bd.get(k) for k in ('applied', 'apply_fail', 'failed', 'failed_postcommit_verification')} } "
         f"('failed' = residu 'autre raison') : "
         f"l'echec ecrit doit avoir son seau, pas 'autre raison'."),
        ("F_message_says_the_file_was_written",
         "ecrite(s) mais contredite(s)" in f_msgs
         and "pour une autre raison" not in f_msgs,
         f"compte rendu={f_msgs!r} : l'utilisateur doit lire que le fichier a "
         f"ete modifie malgre l'echec."),
        ("W_layer_edited_during_the_run_is_not_undoable",
         d.get("w_written") == sorted(d.get("w_ids") or ())
         and d.get("w_manual_scope") == {"ds_misc_w_bati": [(d.get("w_ids") or (0, 0))[1]]},
         f"ecrit={d.get('w_written')} perimetre du bouton="
         f"{d.get('w_manual_scope')} : 'routes' a ete modifiee par "
         f"l'utilisateur pendant le run (commit journalise) ; l'Undo ne doit "
         f"pas proposer d'ecrire les valeurs du restore par-dessus."),
        ("E_preflight_reads_the_journal",
         bool(e_calls) and all(c["conn_passed"] and c["conn_live"] for c in e_calls)
         and d.get("e_runner_started") == 1,
         f"appels du preflight={e_calls} runner={d.get('e_runner_started')} : "
         f"le preflight du restore doit recevoir la connexion de lecture du "
         f"journal (horizon de purge durable)."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
