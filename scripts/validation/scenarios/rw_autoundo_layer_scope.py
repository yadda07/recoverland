# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_autoundo_layer_scope - l'auto-undo d'un Rewind ne deplace que les couches cochees, et le saut en avant ne depend pas de la memoire vive.

Constats couverts : autoundo-unscoped-layers (CRITICAL) et
forward-hop-journal-rebuild-gated (HIGH).

Mecanisme 1 : couches non cochees annulees en silence
-----------------------------------------------------
Quand `_last_restore_by_ds` n'est pas vide, `_auto_undo_for_rewind` passait
le dictionnaire ENTIER (toutes les couches du Rewind ou du restore
precedent) a `UndoRunner`, puis `_on_auto_undo_then_rewind` invalidait les
traces de tous ces evenements et `_refetch_after_auto_undo` ne relisait que
les couches cochees. Aucune intersection avec la selection du nouveau
Rewind n'existait sur ce chemin.

Pour l'utilisateur : suppression accidentelle de 40 parcelles sur la couche
A, Rewind de A, les 40 reviennent. Plus tard il coche seulement B et
rembobine B. La confirmation parle de "N evenement(s) sur 1 couche(s)" ;
l'auto-undo supprime les 40 parcelles recuperees sur A, invalide leurs
traces, et le message final ne parle que de B. La recuperation de A est
defaite sans un mot, et le comptage after_undo correspond a sa propre
attente (-40), donc aucun ecart n'est signale.

Mecanisme 2 : le saut en avant depend de ce qui est en RAM
----------------------------------------------------------
`_reapply_set_from_journal` (les compensations actives AVANT la nouvelle
date, a defaire pour avancer) n'etait appele que si la memoire vive
d'annulation etait vide. Des qu'un Rewind avait eu lieu dans la session,
les traces durables des Rewinds anterieurs devenaient invisibles au saut en
avant : session 1 rembobine a T_a (trace S0), session 2 rembobine a
T_b < T_a puis demande T_c > T_a. Seul le Rewind de la session 2 est defait,
l'evenement compense par S0 reste compense, et la meme demande juste apres
un redemarrage atteint T_c. Le meme saut arrive a deux etats differents.

Ce qui est prouve, sur les vraies methodes du dialogue, le vrai
`UndoRunner` et un vrai journal (le fournisseur et le thread de lecture
sont scriptes) :

  S  Rewind de B avec {A, B} en memoire et seul B coche : seul B est
     defait, la trace de A reste active, A reste annulable, la phase
     affichee compte une couche.
  N  meme situation, curseur a la date courante ("revenir a l'etat
     actuel") : la question compte les entites de B seules, seul B est
     defait.
  F  saut en avant avec une memoire vive non vide : les compensations du
     journal anterieures a la date rejoignent l'undo, dans l'ordre
     chronologique.

Verdict pre-correctif : FAIL. Verdict post-correctif : PASS.

v7 (lot dialogue, tour 3) : il n'y a plus d'auto-undo avant un Rewind. La
fenetre v7 porte le plan complet vers l'etat a T (voir
`rw_rewind_no_autoundo`), et l'auto-undo re-compensait ce qu'il venait de
defaire. Les trois cas sont rejoues avec la VRAIE lecture de la fenetre
(`version_fetch_thread._run_fetch`) sur des chronologies coherentes, et
chaque assertion dont l'hypothese etait "l'auto-undo ne defait que..." est
revisee (commentaires "Hypothesis revised (v7)") en une verification au
moins aussi forte : rien n'est defait, sur aucune couche.
"""
from __future__ import annotations

import sys
import tempfile
from datetime import timedelta
from pathlib import Path

SCENARIO_ID = "rw_autoundo_layer_scope"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP_A = "ds_scope_parcelles"
_FP_B = "ds_scope_voirie"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _install(h, rd, rr, seams, undo_calls, logs, asked, fetches, stricts):
    layers = {_FP_A: h.FakeLayer("lyr_parcelles", "parcelles"),
              _FP_B: h.FakeLayer("lyr_voirie", "voirie")}
    prepare, apply_one = h.scripted_undo({}, undo_calls)
    seams.set(rr, "undo_prepare", prepare)
    seams.set(rr, "undo_apply_one", apply_one)
    seams.set(rd, "find_target_layer",
              lambda evt, _conn=None: layers.get(evt.datasource_fingerprint))
    seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
    seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
    # The REAL window read: a scripted answer is what hid the re-fetch
    # that re-compensated the auto-undo (revue brev2, sonde 3).
    seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
    seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))


def _seed_two_layers(h, directory, name):
    """A: 40-parcel style recovery (DELETE compensated); B: one UPDATE."""
    path = h.new_journal(directory, name)
    a1 = h.insert_event(path, _FP_A, "parcelles", "DELETE", 100, fid=1)
    a2 = h.insert_event(path, _FP_A, "parcelles", "DELETE", 101, fid=2)
    b1 = h.insert_event(path, _FP_B, "voirie", "UPDATE", 110, fid=5)
    for sec, src, fp, lname, op in ((400, a1, _FP_A, "parcelles", "INSERT"),
                                    (401, a2, _FP_A, "parcelles", "INSERT"),
                                    (402, b1, _FP_B, "voirie", "UPDATE")):
        h.insert_event(path, fp, lname, op, sec, fid=1, restored_from=src)
    b2 = h.insert_event(path, _FP_B, "voirie", "UPDATE", 600, fid=6)
    return path, {"a1": a1, "a2": a2, "b1": b1, "b2": b2}


def _idle(dlg):
    return (not dlg._is_recovering and not dlg._restore_in_progress
            and dlg._restore_runner is None)


def _drive(ctx, h, rd, rr, prefix, cutoff_s):
    """A Rewind of B, from the entry point, with {A, B} in the undo state."""
    path, ids = _seed_two_layers(h, ctx.data["tmp"].name, f"{prefix}.sqlite")
    ctx.data[f"{prefix}_ids"] = ids
    ev = dict(zip(("a1", "a2", "b1", "b2"),
                  h.fetch_events(path, [ids["a1"], ids["a2"], ids["b1"], ids["b2"]])))
    cut = h.T0 + timedelta(seconds=cutoff_s)
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_B], cutoff_dt=cut)
    dlg._last_restore_by_ds = {_FP_A: [ev["a2"], ev["a1"]], _FP_B: [ev["b1"]]}
    dlg._last_restore_events = [ev["a2"], ev["a1"], ev["b1"]]
    undo_calls, logs, asked, fetches, stricts = [], [], [], [], []
    seams = h.Seams()
    try:
        _install(h, rd, rr, seams, undo_calls, logs, asked, fetches, stricts)
        dlg._is_recovering = True
        dlg._recover_version_mode()
        h.pump_until(lambda: _idle(dlg), 10.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data[f"{prefix}_undo_layers"] = sorted({name for name, _e in undo_calls})
        ctx.data[f"{prefix}_undo_eids"] = sorted(eid for _n, eid in undo_calls)
        ctx.data[f"{prefix}_traces"] = h.trace_state(
            path, [ids["a1"], ids["a2"], ids["b1"]])
        ctx.data[f"{prefix}_scope_after"] = h.by_ds_ids(dlg._last_restore_by_ds)
        ctx.data[f"{prefix}_phases"] = list(dlg.phases)
        ctx.data[f"{prefix}_asked"] = list(asked)
        ctx.data[f"{prefix}_logs"] = list(logs)
        ctx.data[f"{prefix}_strict_runs"] = len(stricts)
        ctx.data[f"{prefix}_runs"] = [h.plan_marks(s.events) for s in stricts]
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _drive_forward(ctx, h, rd, rr):
    """Session 1 rewound to T_a (trace S0), session 2 to T_b < T_a, now T_c."""
    path = h.new_journal(ctx.data["tmp"].name, "forward.sqlite")
    x1 = h.insert_event(path, _FP_B, "voirie", "UPDATE", 50, fid=5)    # t=50
    y1 = h.insert_event(path, _FP_B, "voirie", "UPDATE", 100, fid=6)   # t=100
    # Session 1: rewind to T_a=90 compensated y1 (trace S0, still active).
    s0 = h.insert_event(path, _FP_B, "voirie", "UPDATE", 150, fid=6,
                        restored_from=y1)
    # Session 2 (this one): rewind to T_b=40 compensated x1.
    s1 = h.insert_event(path, _FP_B, "voirie", "UPDATE", 200, fid=5,
                        restored_from=x1)
    ctx.data["f_ids"] = {"x1": x1, "y1": y1, "s0": s0, "s1": s1}
    ev = dict(zip(("x1", "y1"), h.fetch_events(path, [x1, y1])))
    t_c = h.T0 + timedelta(seconds=120)
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [_FP_B], cutoff_dt=t_c)
    # RAM only knows the session-2 rewind.
    dlg._last_restore_by_ds = {_FP_B: [ev["x1"]]}
    dlg._last_restore_events = [ev["x1"]]
    undo_calls, logs, asked, fetches, stricts = [], [], [], [], []
    seams = h.Seams()
    try:
        _install(h, rd, rr, seams, undo_calls, logs, asked, fetches, stricts)
        dlg._is_recovering = True
        dlg._recover_version_mode()
        h.pump_until(lambda: _idle(dlg), 10.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data["f_fetch_started"] = len(fetches)
        ctx.data["f_undo_eids"] = [eid for _n, eid in undo_calls]
        ctx.data["f_runs"] = [h.plan_marks(s.events) for s in stricts]
        ctx.data["f_traces"] = h.trace_state(path, [x1, y1])
        ctx.data["f_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog

    h = _h()
    ctx.data["tmp"] = tempfile.TemporaryDirectory(prefix="rl_autoundo_scope_")
    flog(f"rw_autoundo_layer_scope run start: trace_id={ctx.trace_id}")
    try:
        # S: a real rewind of B (b2 after the cutoff) while A and B are in RAM.
        _drive(ctx, h, rd, rr, "s", 50)
        # N: slider at the current date, after every trace and every edit:
        # the window is empty (a coherent timeline; the scripted [] of the
        # previous version answered for a journal that had b2 after T).
        _drive(ctx, h, rd, rr, "n", 700)
        _drive_forward(ctx, h, rd, rr)
    finally:
        flog(f"rw_autoundo_layer_scope run end: trace_id={ctx.trace_id}")
        ctx.data["tmp"].cleanup()


def assertions(ctx):
    s = ctx.data["s_ids"]
    n = ctx.data["n_ids"]
    st = ctx.data.get("s_traces") or {}
    nt = ctx.data.get("n_traces") or {}
    phases = " || ".join(ctx.data.get("s_phases") or [])
    n_asked = " || ".join(text for _t, text in ctx.data.get("n_asked") or [])
    n_logs = " || ".join(m for m, _l in ctx.data.get("n_logs") or [])
    f = ctx.data["f_ids"]
    f_runs = ctx.data.get("f_runs") or []
    ft = ctx.data.get("f_traces") or {}

    return [
        # ------------------------------------------------ S
        # Hypothesis revised (v7): "the auto-undo touches only the checked
        # layer" presumed an auto-undo. There is none any more: the check is
        # that NO layer is undone at all, which implies the old one.
        ("S_auto_undo_touches_only_checked_layer",
         ctx.data.get("s_undo_layers") == [],
         f"couches defaites={ctx.data.get('s_undo_layers')} attendu [] : un "
         f"Rewind de 'voirie' applique la fenetre et rien d'autre ; defaire "
         f"'parcelles', c'est supprimer en silence les entites que "
         f"l'utilisateur avait recuperees sur A."),
        ("S_unchecked_layer_traces_stay_active",
         st.get(s["a1"]) == "active" and st.get(s["a2"]) == "active",
         f"traces de A={ {k: st.get(s[k]) for k in ('a1', 'a2')} } : A "
         f"n'a pas bouge, ses compensations sont toujours dans la couche ; "
         f"invalider leurs traces les ferait rejouer."),
        # Hypothesis revised (v7): b1 (t=110) is after T=50 and its
        # compensation is in effect: it is already at its state at T. The
        # auto-undo reverted it, invalidated its trace and the re-fetch
        # compensated it again (churn). Checked now: its trace stays active
        # AND the rewind does not compensate it a second time.
        ("S_checked_layer_trace_invalidated",
         st.get(s["b1"]) == "active"
         and all(eid != s["b1"] for run in ctx.data.get("s_runs") or []
                 for _k, eid in run),
         f"trace de b1={st.get(s['b1'])} runs={ctx.data.get('s_runs')} : b1 "
         f"est deja compense, son etat a T est atteint ; ni defait, ni "
         f"compense une seconde fois."),
        ("S_unchecked_layer_stays_revertible",
         sorted((ctx.data.get("s_scope_after") or {}).get(_FP_A, []))
         == sorted([s["a1"], s["a2"]]),
         f"perimetre d'annulation apres le Rewind de B="
         f"{ctx.data.get('s_scope_after')} : la restauration de A doit "
         f"rester annulable, elle n'a pas ete defaite."),
        # Hypothesis revised (v7): the phase announced the auto-undo; there
        # is no undo phase any more, and none may be announced.
        ("S_phase_counts_only_the_scope",
         "Annulation du Rewind precedent" not in phases,
         f"phases={phases!r} : aucune annulation n'a lieu avant le Rewind, "
         f"aucune ne doit etre annoncee."),
        ("S_rewind_of_b_still_runs",
         ctx.data.get("s_strict_runs") == 1
         and ctx.data.get("s_runs") == [[("user", s["b2"])]],
         f"runs de Rewind={ctx.data.get('s_runs')} attendu "
         f"[[('user', {s['b2']})]] : le Rewind de B compense b2, et lui "
         f"seul."),
        # ------------------------------------------------ N
        # Hypothesis revised (v7): "the question counts only the checked
        # layer" presumed the question "Remettre N entite(s) dans leur etat
        # actuel ?". With the cursor after every trace, the window is empty:
        # the layers ARE at that date, nothing may be undone, so nothing is
        # asked, and the message says so.
        ("N_question_counts_only_checked_layer",
         n_asked == "" and "deja dans leur etat a cette date" in n_logs,
         f"questions={n_asked!r} messages={n_logs!r} : aucune question "
         f"(rien ne sera fait), et le message dit que les couches sont deja "
         f"a cette date."),
        # Hypothesis revised (v7): stronger than "only B undone": nothing
        # undone, and B's compensation stays in effect too.
        ("N_only_checked_layer_undone",
         ctx.data.get("n_undo_layers") == [] and ctx.data.get("n_strict_runs") == 0
         and nt.get(n["a1"]) == "active" and nt.get(n["b1"]) == "active",
         f"couches defaites={ctx.data.get('n_undo_layers')} runs="
         f"{ctx.data.get('n_strict_runs')} traces A={nt.get(n['a1'])} "
         f"B={nt.get(n['b1'])} : A et B doivent rester intactes."),
        # ------------------------------------------------ F
        ("F_fetch_started", (ctx.data.get("f_fetch_started") or 0) >= 1,
         f"relectures={ctx.data.get('f_fetch_started')} : le Rewind doit "
         f"demarrer (controle du scenario) ; messages={ctx.data.get('f_logs')}"),
        # Hypothesis revised (v7): the forward hop re-applied y1 and x1 by
        # undoing their traces. The v7 window carries both re-applications
        # itself (items that compensate S0 and the session's trace); the
        # check is that the rewind gets both, from the journal (S0 is not in
        # RAM), and that nothing is undone beside it.
        ("F_forward_hop_reapplies_journal_traces",
         sorted(eid for run in f_runs for kind, eid in run if kind == "reapply")
         == sorted([f["s0"], f["s1"]])
         and ctx.data.get("f_undo_eids") == [],
         f"runs={f_runs} inverses={ctx.data.get('f_undo_eids')} attendu les "
         f"re-applications de x1 ({f['s1']}) et de y1 ({f['s0']}), sans "
         f"inverse : sans celle de y1 (trace S0 du journal) la couche reste "
         f"a T_a, alors que la meme demande apres un redemarrage atteint "
         f"T_c."),
        # Hypothesis revised (v7): the order of an UNDO (oldest first) was
        # asserted; a window pass is newest first, by the date of the toggle
        # each item undoes: the session's trace (t=200) before S0 (t=150).
        ("F_forward_hop_order_is_chronological",
         f_runs == [[("reapply", f["s1"]), ("reapply", f["s0"])]],
         f"ordre du passage={f_runs} attendu [[('reapply', {f['s1']}), "
         f"('reapply', {f['s0']})]] : le plus recent d'abord (trace de x1 "
         f"a t=200, puis S0 a t=150)."),
        # Hypothesis revised (v7): the re-application is journalled by the
        # trace the rewind writes for the item, which CANCELS S0 (the item
        # carries S0's id); S0 itself is not invalidated any more.
        ("F_journal_trace_invalidated",
         ft.get(f["y1"]) == "active"
         and any(eid == f["s0"] for run in f_runs for kind, eid in run
                 if kind == "reapply"),
         f"trace S0={ft.get(f['y1'])} runs={f_runs} : la re-application de "
         f"y1 porte l'id de S0 ({f['s0']}), la trace ecrite par le Rewind "
         f"l'annule."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
