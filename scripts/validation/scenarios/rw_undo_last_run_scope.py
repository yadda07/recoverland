# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_undo_last_run_scope - "Annuler le dernier restore" ne defait que le dernier run, et ce qu'il a vraiment defait.

Constats couverts : suites de autoundo-unscoped-layers (CRITICAL),
forward-hop-journal-rebuild-gated (HIGH) et trace-invalidation-ignores-
undo-outcome (HIGH). Trois defauts trouves par la revue adversariale du
premier correctif, sur des enchainements que les scenarios de ce
correctif ne jouaient pas.

Mecanisme 1 : le bouton defaisait tout ce que la session avait ecrit
--------------------------------------------------------------------
Pour ne plus defaire en silence les couches non cochees d'un Rewind, le
premier correctif a fait de `_last_restore_by_ds` l'union, couche par
couche, de tout ce que la session avait ecrit et pas encore defait : les
runs successifs y etaient FUSIONNES, et le journal y versait en plus, avant
toute confirmation, les compensations reconstruites pour un saut en avant.
Or le bouton "Annuler le dernier restore" lit ce meme dictionnaire.

Pour l'utilisateur : il restaure un evenement e1, puis un evenement e2, et
clique "Annuler le dernier restore". La question annonce 2 entites et
l'undo defait aussi e1, qu'il n'a pas demande. Meme chose apres un Rewind
de la couche A puis un Rewind de B : le bouton defait la recuperation de A
avec celle de B. Et apres un Rewind abandonne a la question "Revenir a
l'etat actuel", le restore suivant, une fois annule, emporte avec lui une
compensation ecrite hier par une autre session.

Mecanisme 2 : l'undo manuel partageait son perimetre avec le dialogue
--------------------------------------------------------------------
`_undo_last_restore` passait a `UndoRunner` l'objet MEME
`_last_restore_by_ds`, et le runner le gardait tel quel. Une edition de
l'utilisateur sur une couche de ce perimetre, notifiee pendant l'undo,
retire cette couche du dictionnaire partage (`_invalidate_undo_for`). Le
runner, qui avait deja fige sa liste de groupes, defait la couche quand
meme ; mais `undo_outcome()` relit le dictionnaire mutile et ne la voit
plus : sa trace reste ACTIVE alors que sa compensation a ete defaite, et
le message annonce une entite remise de moins. Le Rewind suivant tient
cette entite pour compensee et ne la rejoue pas.

Ce qui est prouve, sur les vraies methodes du dialogue, les vrais
`RestoreRunner` et `UndoRunner` et un vrai journal SQLite (seuls le
fournisseur, la resolution de couche, les boites et le thread de lecture
sont scriptes) :

  T  deux restores d'evenements successifs sur la meme couche : l'undo ne
     defait que le second ; la trace du premier reste active.
  X  Rewind de B apres un Rewind de A : l'undo ne defait que B.
  J  saut en avant abandonne a la question, puis un restore : l'undo ne
     defait que ce restore, jamais la compensation reconstruite du journal.
  A  edition de l'utilisateur pendant l'undo manuel : ce que le runner a
     defait a sa trace invalidee et est compte.

Verdict avant ce correctif (arbre de travail v7) : FAIL sur T, X, J et A.
Sur le code d'origine, T, J et A passent : ce sont des regressions du
premier correctif, que ce scenario verrouille. X y echoue sur son CONTROLE
(`X_rewind_of_b_ran`) : l'auto-undo non borne du code d'origine menait a
une relecture scriptee vide, aucun Rewind de B ne partait, et le defaut de
perimetre de l'Undo n'y etait donc pas exerce (revue brev2 ; il n'a ete
montre que contre l'arbre de travail d'avant le tour 2).

v7 (lot dialogue, tour 3) : J lit la VRAIE fenetre (le fetch scripte
repondait [] pour un journal qui avait e5 apres T_c). La premiere question
est donc la confirmation du Rewind, qui annonce la re-application de y1
tiree du journal ; l'utilisateur la refuse.
"""
from __future__ import annotations

import sys
import tempfile
from datetime import timedelta
from pathlib import Path

SCENARIO_ID = "rw_undo_last_run_scope"
INVARIANT = "I-UNDO-SCOPE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP_R = "ds_lastrun_routes"
_FP_B = "ds_lastrun_bati"
_FP_A = "ds_lastrun_parcelles"
_FP_V = "ds_lastrun_voirie"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _answers_box(rd, asked: list, answers: list):
    """QMessageBox scripte : chaque question consomme la reponse suivante."""

    class Box:
        @staticmethod
        def question(_parent, title, text, *args, **kwargs):
            asked.append((str(title), str(text)))
            yes = answers.pop(0) if answers else True
            return rd.QtCompat.MSG_YES if yes else rd.QtCompat.MSG_NO

        @staticmethod
        def warning(_parent, title, text, *args, **kwargs):
            asked.append((str(title), str(text)))

        information = warning
        critical = warning

    return Box


def _event_restore(h, rr, dlg, wq, layer, path, eid, trace_id):
    """One event restore, launched and accounted like restore_selected_data."""
    events = h.fetch_events(path, [eid])
    runner = rr.RestoreRunner(events, lambda _e: layer, wq, tracker=None,
                              trace_id=trace_id)
    results = []
    runner.finished.connect(results.append)
    runner.finished.connect(dlg._on_event_restore_done)
    dlg._event_restore_events = events
    dlg._restore_runner = runner
    runner.start()
    h.pump_until(lambda: bool(results), 10.0)
    return bool(results)


def _click_undo(h, dlg):
    done = []
    original = dlg._on_undo_done

    def _record(res, _orig=original):
        _orig(res)
        done.append(res)
    dlg._on_undo_done = _record
    dlg._undo_last_restore()
    h.pump_until(lambda: bool(done), 10.0)
    return bool(done)


def _case_t(ctx, h, rd, rr):
    """T: two event restores on one layer, then 'Annuler le dernier restore'."""
    path = h.new_journal(ctx.data["tmp"].name, "t.sqlite")
    e1 = h.insert_event(path, _FP_R, "routes", "DELETE", 100, fid=1)
    e2 = h.insert_event(path, _FP_R, "routes", "DELETE", 110, fid=2)
    ctx.data["t_ids"] = (e1, e2)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP_R])
    layer = h.FakeLayer("lyr_routes", "routes")
    undo_calls, restore_calls, logs, asked = [], [], [], []
    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch", h.scripted_restore_batch({}, restore_calls))
        # Hypothesis revised (v7): the runners hand the executor the journal read
        # connection of the layer group (`journal=`, the published signature);
        # the doubles take it like the real functions, nothing else changes.
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", _answers_box(rd, asked, []))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        ok = all(_event_restore(h, rr, dlg, wq, layer, path, eid, ctx.trace_id)
                 for eid in (e1, e2))
        ctx.data["t_restores_ok"] = ok
        ctx.data["t_button_armed"] = dlg.undo_last_btn.enabled
        ctx.data["t_undo_done"] = _click_undo(h, dlg)
        ctx.data["t_undo_calls"] = [eid for _n, eid in undo_calls]
        ctx.data["t_asked"] = [text for _t, text in asked]
        ctx.data["t_traces"] = h.trace_state(path, [e1, e2])
        ctx.data["t_button_after"] = dlg.undo_last_btn.enabled
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_x(ctx, h, rd, rr):
    """X: the previous run rewound A; now B is rewound, then Undo."""
    path = h.new_journal(ctx.data["tmp"].name, "x.sqlite")
    a1 = h.insert_event(path, _FP_A, "parcelles", "DELETE", 100, fid=1)
    h.insert_event(path, _FP_A, "parcelles", "INSERT", 400, fid=1,
                   restored_from=a1)
    b2 = h.insert_event(path, _FP_V, "voirie", "UPDATE", 600, fid=6)
    ctx.data["x_ids"] = (a1, b2)
    ev_a1, ev_b2 = h.fetch_events(path, [a1, b2])
    cut = h.T0 + timedelta(seconds=50)
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_V], cutoff_dt=cut)
    # What the rewind of A (the previous run) left: its written scope.
    dlg._last_restore_by_ds = {_FP_A: [ev_a1]}
    dlg._last_restore_events = [ev_a1]
    dlg._last_run_ids = {a1}
    dlg._version_restore_cutoff = h.cutoff(cut)
    dlg._active_restore_trace_id = "rw_lastrun_x"
    layers = {_FP_A: h.FakeLayer("lyr_parcelles", "parcelles"),
              _FP_V: h.FakeLayer("lyr_voirie", "voirie")}
    undo_calls, logs, asked, fetches, stricts = [], [], [], [], []
    seams = h.Seams()
    try:
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer",
                  lambda evt, _c=None: layers.get(evt.datasource_fingerprint))
        seams.set(rd, "QMessageBox", _answers_box(rd, asked, []))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "VersionFetchThread", h.fetch_thread_class([], fetches))
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        # The fetch thread delivers b2: the rewind of B runs (recorded).
        dlg._on_version_fetch_done([ev_b2])
        h.pump_until(lambda: bool(stricts), 5.0)
        # The writer journals the rewind's trace for b2.
        h.insert_event(path, _FP_V, "voirie", "UPDATE", 700, fid=6,
                       restored_from=b2)
        ctx.data["x_rewind_ran"] = len(stricts)
        ctx.data["x_undo_done"] = _click_undo(h, dlg)
        ctx.data["x_undo_calls"] = [eid for _n, eid in undo_calls]
        ctx.data["x_asked"] = [text for _t, text in asked]
        ctx.data["x_traces"] = h.trace_state(path, [a1, b2])
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_j(ctx, h, rd, rr):
    """J: a forward hop declined at the question, then an event restore, then Undo."""
    path = h.new_journal(ctx.data["tmp"].name, "j.sqlite")
    y1 = h.insert_event(path, _FP_B, "bati", "UPDATE", 100, fid=6)
    # Yesterday's session rewound before y1: trace S0, still active.
    h.insert_event(path, _FP_B, "bati", "UPDATE", 150, fid=6, restored_from=y1)
    e5 = h.insert_event(path, _FP_B, "bati", "DELETE", 300, fid=7)
    ctx.data["j_ids"] = (y1, e5)
    t_c = h.T0 + timedelta(seconds=120)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP_B], cutoff_dt=t_c)
    # As built by the dialog: nothing to undo yet, the button is off.
    dlg.undo_last_btn.setEnabled(False)
    layer = h.FakeLayer("lyr_bati", "bati")
    undo_calls, restore_calls, logs, asked, fetches, stricts = [], [], [], [], [], []
    # 1st question ("Revenir a l'etat actuel ?"): No. Then Undo: Yes.
    answers = [False, True]
    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch", h.scripted_restore_batch({}, restore_calls))
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", _answers_box(rd, asked, answers))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        # The REAL window at T_c: e5 (after T_c) to compensate and the
        # journal's compensation of y1 (S0, after T_c) to re-apply. The user
        # declines at the confirmation.
        seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        dlg._recover_version_mode()
        h.pump_until(lambda: bool(asked), 5.0)
        ctx.data["j_first_question"] = asked[0][1] if asked else ""
        ctx.data["j_button_after_decline"] = dlg.undo_last_btn.enabled
        ctx.data["j_restore_ok"] = _event_restore(h, rr, dlg, wq, layer, path,
                                                  e5, ctx.trace_id)
        ctx.data["j_undo_done"] = _click_undo(h, dlg)
        ctx.data["j_undo_calls"] = [eid for _n, eid in undo_calls]
        ctx.data["j_asked"] = [text for _t, text in asked]
        ctx.data["j_traces"] = h.trace_state(path, [y1, e5])
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_a(ctx, h, rd, rr):
    """A: the user saves an edit on 'bati' while the manual undo runs."""
    path = h.new_journal(ctx.data["tmp"].name, "a.sqlite")
    u1 = h.insert_event(path, _FP_R, "routes", "DELETE", 100, fid=1)
    u2 = h.insert_event(path, _FP_B, "bati", "DELETE", 110, fid=2)
    h.insert_event(path, _FP_R, "routes", "INSERT", 500, fid=1, restored_from=u1)
    h.insert_event(path, _FP_B, "bati", "INSERT", 501, fid=2, restored_from=u2)
    ctx.data["a_ids"] = (u1, u2)
    ev = h.fetch_events(path, [u1, u2])
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_R, _FP_B])
    dlg._last_restore_by_ds = {_FP_R: [ev[0]], _FP_B: [ev[1]]}
    dlg._last_restore_events = list(ev)
    dlg._last_run_ids = {u1, u2}
    layers = {_FP_R: h.FakeLayer("lyr_routes", "routes"),
              _FP_B: h.FakeLayer("lyr_bati", "bati")}
    undo_calls, logs, asked = [], [], []

    def on_apply(eid):
        if eid == u1:
            # recover.py delivers on_events_committed(fp) 500 ms after a
            # commit of the user on 'bati', here while 'routes' is undone.
            dlg.on_events_committed(_FP_B)

    seams = h.Seams()
    try:
        prepare, apply_one = h.scripted_undo({}, undo_calls, on_apply=on_apply)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer",
                  lambda evt, _c=None: layers.get(evt.datasource_fingerprint))
        seams.set(rd, "QMessageBox", _answers_box(rd, asked, []))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        ctx.data["a_undo_done"] = _click_undo(h, dlg)
        ctx.data["a_undo_calls"] = [eid for _n, eid in undo_calls]
        ctx.data["a_traces"] = h.trace_state(path, [u1, u2])
        ctx.data["a_scope_after"] = sorted(h.all_ids(dlg._last_restore_by_ds))
        ctx.data["a_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def setup(ctx):
    ctx.data["tmp"] = tempfile.TemporaryDirectory(prefix="rl_undo_lastrun_")


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog

    h = _h()
    flog(f"rw_undo_last_run_scope run start: trace_id={ctx.trace_id}")
    try:
        _case_t(ctx, h, rd, rr)
        _case_x(ctx, h, rd, rr)
        _case_j(ctx, h, rd, rr)
        _case_a(ctx, h, rd, rr)
    finally:
        flog(f"rw_undo_last_run_scope run end: trace_id={ctx.trace_id}")
        ctx.data["tmp"].cleanup()


def assertions(ctx):
    e1, e2 = ctx.data.get("t_ids") or (None, None)
    t_calls = ctx.data.get("t_undo_calls")
    t_asked = " || ".join(ctx.data.get("t_asked") or [])
    tt = ctx.data.get("t_traces") or {}
    a1, b2 = ctx.data.get("x_ids") or (None, None)
    x_calls = ctx.data.get("x_undo_calls")
    xt = ctx.data.get("x_traces") or {}
    x_asked = " || ".join(ctx.data.get("x_asked") or [])
    y1, e5 = ctx.data.get("j_ids") or (None, None)
    j_calls = ctx.data.get("j_undo_calls")
    jt = ctx.data.get("j_traces") or {}
    u1, u2 = ctx.data.get("a_ids") or (None, None)
    at = ctx.data.get("a_traces") or {}
    a_msg = " || ".join(m for m, _l in ctx.data.get("a_logs") or [])

    return [
        # ------------------------------------------------ T
        ("T_both_restores_ran",
         ctx.data.get("t_restores_ok") is True
         and ctx.data.get("t_button_armed") is True
         and ctx.data.get("t_undo_done") is True,
         f"restores={ctx.data.get('t_restores_ok')} bouton="
         f"{ctx.data.get('t_button_armed')} undo={ctx.data.get('t_undo_done')} "
         f"(controle du scenario)"),
        ("T_undo_last_restore_reverts_only_the_last_one",
         t_calls == [e2],
         f"inverses appliques={t_calls} attendu [{e2}] : 'Annuler le dernier "
         f"restore' a aussi defait le restore precedent ({e1}), que "
         f"l'utilisateur n'a pas demande d'annuler."),
        ("T_question_counts_the_last_restore",
         "Remettre 1 entite(s)" in t_asked,
         f"question={t_asked!r} : la confirmation doit annoncer l'entite du "
         f"dernier restore seulement ; en annoncer 2 fait accepter a "
         f"l'aveugle l'annulation du precedent."),
        ("T_previous_restore_trace_stays_active",
         tt.get(e1) == "active" and tt.get(e2) == "invalidated",
         f"traces={tt} : le premier restore est toujours dans les donnees, "
         f"sa trace doit rester active ; celle du second, defait, invalidee."),
        ("T_button_off_once_the_last_restore_is_undone",
         ctx.data.get("t_button_after") is False,
         f"bouton apres l'undo={ctx.data.get('t_button_after')} : le dernier "
         f"restore est entierement defait ; rearmer le bouton sur le restore "
         f"d'avant ferait defaire au clic suivant ce qui n'a jamais ete "
         f"annonce comme 'le dernier'."),
        # ------------------------------------------------ X
        ("X_rewind_of_b_ran",
         ctx.data.get("x_rewind_ran") == 1 and ctx.data.get("x_undo_done") is True,
         f"runs de Rewind={ctx.data.get('x_rewind_ran')} undo="
         f"{ctx.data.get('x_undo_done')} (controle du scenario)"),
        ("X_undo_after_rewind_of_b_leaves_a_alone",
         x_calls == [b2],
         f"inverses appliques={x_calls} attendu [{b2}] : le dernier run est "
         f"le Rewind de 'voirie' ; defaire aussi la recuperation de "
         f"'parcelles' ({a1}) supprime des entites recuperees par un run "
         f"precedent."),
        ("X_question_names_only_the_last_layer",
         "voirie" in x_asked and "parcelles" not in x_asked,
         f"question={x_asked!r} : seule 'voirie' est concernee."),
        ("X_previous_rewind_trace_stays_active",
         xt.get(a1) == "active" and xt.get(b2) == "invalidated",
         f"traces={xt} : la compensation de {a1} est toujours dans "
         f"'parcelles', sa trace doit rester active."),
        # ------------------------------------------------ J
        # Hypothesis revised (v7): the hop used to be offered as "Remettre 1
        # entite(s) ..." (the auto-undo of the journal-rebuilt scope). The
        # real v7 window offers it in the Rewind's own confirmation, with
        # the re-application of y1 counted in: 2 events (e5 and y1).
        ("J_forward_hop_was_offered_and_declined",
         "Revenir au" in (ctx.data.get("j_first_question") or "")
         and "2 evenement(s)" in (ctx.data.get("j_first_question") or "")
         and ctx.data.get("j_restore_ok") is True
         and ctx.data.get("j_undo_done") is True,
         f"premiere question={ctx.data.get('j_first_question')!r} restore="
         f"{ctx.data.get('j_restore_ok')} undo={ctx.data.get('j_undo_done')} "
         f"(controle : le journal a bien fourni la compensation de y1)"),
        ("J_declined_hop_does_not_arm_the_button",
         ctx.data.get("j_button_after_decline") is False,
         f"bouton apres le refus={ctx.data.get('j_button_after_decline')} : "
         f"aucun run n'a eu lieu, il n'y a rien a annuler."),
        ("J_undo_never_reverts_a_journal_rebuilt_compensation",
         j_calls == [e5],
         f"inverses appliques={j_calls} attendu [{e5}] : la compensation de "
         f"{y1} vient d'un Rewind d'une autre session, reconstruite depuis "
         f"le journal pour un saut que l'utilisateur a refuse ; "
         f"'Annuler le dernier restore' l'a defaite avec le restore."),
        ("J_journal_rebuilt_trace_stays_active",
         jt.get(y1) == "active" and jt.get(e5) == "invalidated",
         f"traces={jt} : S0 decrit toujours les donnees."),
        # ------------------------------------------------ A
        ("A_runner_undid_both_layers",
         ctx.data.get("a_undo_done") is True
         and ctx.data.get("a_undo_calls") == [u1, u2],
         f"inverses appliques={ctx.data.get('a_undo_calls')} (controle : le "
         f"runner a fige son perimetre, il defait 'bati' malgre l'edition)"),
        ("A_every_undone_trace_is_invalidated",
         at.get(u1) == "invalidated" and at.get(u2) == "invalidated",
         f"traces={at} : {u2} a ete defait sur 'bati' ; sa trace restee "
         f"active fait croire au Rewind suivant que sa compensation est "
         f"encore la, et il ne la rejoue pas."),
        ("A_message_counts_every_undone_entity",
         "2 entite(s) remise(s)" in a_msg,
         f"messages={a_msg!r} : deux entites ont ete remises ; en annoncer "
         f"une cache l'autre."),
        ("A_nothing_undone_stays_armed",
         ctx.data.get("a_scope_after") == [],
         f"perimetre apres={ctx.data.get('a_scope_after')} : tout a ete "
         f"defait, rien ne doit rester annulable."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
