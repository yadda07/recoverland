# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_rewind_no_autoundo - un Rewind applique la fenetre v7 et rien d'autre : aucune compensation de l'etat a T n'est defaite en silence.

Constats couverts : revue brev2 BLOCKER ("Revenir a l'etat actuel" apres un
Rewind de la session) et item B du lot dialogue (plus d'auto-undo avant un
Rewind).

Mecanisme
---------
Depuis le coeur v7, la fenetre d'un Rewind (`fetch_events_after_cutoff` +
`collapse_rewind_events_with_stats`) porte a elle seule le plan COMPLET
vers l'etat a T, tire du journal : evenements utilisateur apres T, et
elements de plan pour l'historique d'avant T (re-application d'un evenement
qu'une compensation posterieure a T a defait, re-compensation d'un evenement
dont la compensation en vigueur a T a ete defaite depuis, refus nommes).
Une compensation ecrite AVANT T fait partie de l'etat demande.

Le dialogue, lui, defaisait d'abord en silence (`_auto_undo_for_rewind`)
toutes les compensations de la session sur les couches du Rewind (la
reserve `_last_restore_by_ds`, plus celles que `_reapply_set_from_journal`
reconstruisait du journal), invalidait leurs traces, puis relisait la
fenetre a la MEME date. Or cette relecture voit l'invalidation qu'il vient
d'ecrire (invalidated_at = maintenant >= T) : chaque compensation en vigueur
a T redevient un element "re-compenser", reapplique sans question.

Pour l'utilisateur :
  E  il a rembobine la couche dans la session, puis pose le curseur a la
     date du jour ("revenir a l'etat actuel"). La question "Remettre 1
     entite(s) dans leur etat actuel ?" recoit oui ; l'auto-undo remet
     l'entite, la relecture la re-compense aussitot, et le message annonce
     "1 restauree(s)" en INFO. Les donnees finissent dans l'etat rembobine,
     l'inverse de ce qu'il a confirme.
  R  il a recupere une entite supprimee par erreur (Rewind de la session,
     compensation a t=400), puis rembobine a t=500 pour annuler une edition
     posterieure. L'auto-undo supprime l'entite recuperee, la fenetre la
     reinsere (nouveau FID) : va-et-vient destructif, et si la reinsertion
     est refusee l'entite recuperee reste supprimee.
  F  saut en avant : la fenetre porte deja les re-applications ; l'auto-undo
     les faisait par un autre chemin (UndoRunner, invalidations) au lieu
     d'un seul passage chronologique trace.

Ce qui est prouve, sur les vraies methodes du dialogue, le vrai journal
SQLite et la VRAIE lecture de la fenetre (`version_fetch_thread._run_fetch`),
le fournisseur, les boites et le runner de Rewind etant scriptes : aucun
UndoRunner avant un Rewind, le Rewind recoit exactement la fenetre v7, les
traces en vigueur a T restent actives, et le message dit la verite.

Verdict avant correctif (arbre de travail v7) : FAIL (E, R, F).
Verdict apres correctif : PASS.
"""
from __future__ import annotations

import sys
import tempfile
from datetime import timedelta
from pathlib import Path

SCENARIO_ID = "rw_rewind_no_autoundo"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP_E = "ds_noauto_routes"
_FP_R = "ds_noauto_parcelles"
_FP_F = "ds_noauto_voirie"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _drive(ctx, key, path, fp, name, cutoff_s, pool_ids, last_ids):
    """Un Rewind lance par l'utilisateur, de `_recover_version_mode` a la fin."""
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    events = dict(zip(pool_ids, h.fetch_events(path, pool_ids))) if pool_ids else {}
    dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path),
                         [fp], cutoff_dt=h.T0 + timedelta(seconds=cutoff_s))
    if pool_ids:
        dlg._last_restore_by_ds = {fp: [events[i] for i in pool_ids]}
        dlg._last_restore_events = [events[i] for i in pool_ids]
    dlg._last_run_ids = set(last_ids)
    dlg.undo_last_btn.setEnabled(bool(last_ids))
    layer = h.FakeLayer(f"lyr_{name}", name)
    undo_calls, logs, asked, fetches, stricts = [], [], [], [], []
    seams = h.Seams()
    try:
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        dlg._is_recovering = True
        dlg._recover_version_mode()
        h.pump_until(lambda: (not dlg._is_recovering
                              and not dlg._restore_in_progress
                              and dlg._restore_runner is None), 5.0)
        h.pump_until(lambda: False, 0.3)
        ctx.data[f"{key}_undo_calls"] = [eid for _n, eid in undo_calls]
        ctx.data[f"{key}_fetches"] = [h.plan_marks(f.result) for f in fetches]
        ctx.data[f"{key}_runs"] = [h.plan_marks(s.events) for s in stricts]
        ctx.data[f"{key}_asked"] = [text for _t, text in asked]
        ctx.data[f"{key}_logs"] = list(logs)
        ctx.data[f"{key}_button"] = dlg.undo_last_btn.enabled
    finally:
        seams.restore()
        h.close_dialog(dlg)


def _case_e(ctx, directory):
    """E: "revenir a l'etat actuel" apres un Rewind de la session (sonde brev2 3)."""
    h = _h()
    path = h.new_journal(directory, "e.sqlite")
    e1 = h.insert_event(path, _FP_E, "routes", "UPDATE", 100, fid=1)
    h.insert_event(path, _FP_E, "routes", "UPDATE", 400, fid=1,
                   restored_from=e1, schema=7)
    ctx.data["e_ids"] = e1
    _drive(ctx, "e", path, _FP_E, "routes", 500, [e1], [e1])
    ctx.data["e_trace"] = h.trace_state(path, [e1]).get(e1)


def _case_r(ctx, directory):
    """R: Rewind a une date posterieure a une recuperation de la session."""
    h = _h()
    path = h.new_journal(directory, "r.sqlite")
    a1 = h.insert_event(path, _FP_R, "parcelles", "DELETE", 100, fid=1)
    # The session's rewind recovered the parcel at t=400 (INSERT trace).
    h.insert_event(path, _FP_R, "parcelles", "INSERT", 400, fid=1,
                   restored_from=a1, schema=7)
    a2 = h.insert_event(path, _FP_R, "parcelles", "UPDATE", 600, fid=2)
    ctx.data["r_ids"] = (a1, a2)
    _drive(ctx, "r", path, _FP_R, "parcelles", 500, [a1], [a1])
    ctx.data["r_traces"] = h.trace_state(path, [a1])


def _case_f(ctx, directory):
    """F: saut en avant, memoire vive non vide (la session a rembobine x1)."""
    h = _h()
    path = h.new_journal(directory, "f.sqlite")
    x1 = h.insert_event(path, _FP_F, "voirie", "UPDATE", 50, fid=5)
    y1 = h.insert_event(path, _FP_F, "voirie", "UPDATE", 100, fid=6)
    # Yesterday's session compensated y1 (S0), this one compensated x1.
    s0 = h.insert_event(path, _FP_F, "voirie", "UPDATE", 150, fid=6,
                        restored_from=y1, schema=7)
    s1 = h.insert_event(path, _FP_F, "voirie", "UPDATE", 200, fid=5,
                        restored_from=x1, schema=7)
    ctx.data["f_ids"] = {"x1": x1, "y1": y1, "s0": s0, "s1": s1}
    _drive(ctx, "f", path, _FP_F, "voirie", 120, [x1], [x1])
    ctx.data["f_traces"] = h.trace_state(path, [x1, y1])


def run(ctx):
    from recoverland.core.logger import flog
    tmp = tempfile.TemporaryDirectory(prefix="rl_noauto_")
    flog(f"rw_rewind_no_autoundo run start: trace_id={ctx.trace_id}")
    try:
        _case_e(ctx, tmp.name)
        _case_r(ctx, tmp.name)
        _case_f(ctx, tmp.name)
    finally:
        flog(f"rw_rewind_no_autoundo run end: trace_id={ctx.trace_id}")
        tmp.cleanup()


def _msgs(logs) -> str:
    return " || ".join(f"{lvl}:{m}" for m, lvl in logs or [])


def assertions(ctx):
    d = ctx.data
    e1 = d.get("e_ids")
    e_logs = d.get("e_logs") or []
    a1, a2 = d.get("r_ids") or (None, None)
    f = d.get("f_ids") or {}
    f_runs = d.get("f_runs") or []
    return [
        # ------------------------------------------------ E
        ("E_nothing_is_undone_before_the_rewind",
         d.get("e_undo_calls") == [],
         f"inverses appliques={d.get('e_undo_calls')} : le curseur est apres "
         f"la compensation de e1 ({e1}) par la session ; elle fait partie de "
         f"l'etat a cette date, la defaire n'est pas ce que demande un "
         f"Rewind."),
        ("E_no_rewind_run_recompensates",
         d.get("e_runs") == [],
         f"runs de Rewind={d.get('e_runs')} fetchs={d.get('e_fetches')} : la "
         f"relecture a re-compense ce que l'auto-undo venait de defaire ; "
         f"les donnees finissent dans l'etat rembobine, l'inverse de ce que "
         f"l'utilisateur a confirme."),
        ("E_compensation_trace_stays_active",
         d.get("e_trace") == "active",
         f"trace de la compensation de e1={d.get('e_trace')} : rien n'a ete "
         f"defait, le journal doit toujours la tenir pour en vigueur."),
        ("E_message_is_true",
         not any("restauree" in m for m, _l in e_logs)
         and any("deja" in m and "date" in m for m, _l in e_logs),
         f"messages={_msgs(e_logs)!r} : l'utilisateur doit lire que les couches "
         f"sont deja a cette date et que rien n'a change, jamais "
         f"'1 restauree(s)'."),
        ("E_message_names_the_way_back",
         any("Annuler le dernier restore" in m for m, _l in e_logs),
         f"messages={_msgs(e_logs)!r} : le dernier Rewind de la session porte "
         f"sur cette couche ; le message doit dire comment le defaire."),
        ("E_last_run_stays_undoable",
         d.get("e_button") is True,
         f"bouton 'Annuler le dernier restore'={d.get('e_button')} : le "
         f"dernier run n'a pas ete defait, il doit rester annulable."),
        # ------------------------------------------------ R
        ("R_recovered_entity_is_not_deleted",
         d.get("r_undo_calls") == [],
         f"inverses appliques={d.get('r_undo_calls')} : l'entite {a1} "
         f"recuperee a t=400 fait partie de l'etat a t=500 ; l'auto-undo la "
         f"supprimait avant que la fenetre ne la reinsere."),
        ("R_rewind_gets_exactly_the_window",
         d.get("r_runs") == [[("user", a2)]],
         f"runs de Rewind={d.get('r_runs')} attendu [[('user', {a2})]] : "
         f"seule l'edition posterieure a T est a compenser ; une "
         f"re-compensation de {a1} reinsererait l'entite sous un nouveau FID."),
        ("R_recovery_trace_stays_active",
         (d.get("r_traces") or {}).get(a1) == "active",
         f"trace de la recuperation={d.get('r_traces')} : invalider cette "
         f"trace fait croire au journal que l'entite n'a jamais ete "
         f"recuperee."),
        # ------------------------------------------------ F
        ("F_forward_hop_uses_no_undo",
         d.get("f_undo_calls") == [],
         f"inverses appliques={d.get('f_undo_calls')} : le saut en avant est "
         f"dans la fenetre ; le faire par un UndoRunner en plus, c'est "
         f"deux chemins pour une seule demande."),
        ("F_window_reapplies_both_in_one_pass",
         f_runs == [[("reapply", f.get("s1")), ("reapply", f.get("s0"))]],
         f"runs de Rewind={f_runs} attendu [[('reapply', {f.get('s1')}), "
         f"('reapply', {f.get('s0')})]] : x1 (compensation de la session) et "
         f"y1 (compensation d'hier, du journal) reviennent dans le meme "
         f"passage, le plus recent d'abord."),
        ("F_traces_are_cancelled_by_the_run_not_invalidated",
         (d.get("f_traces") or {}) == {f.get("x1"): "active", f.get("y1"): "active"},
         f"traces={d.get('f_traces')} : le Rewind ecrit des traces qui "
         f"annulent S0 et S1 ; les invalider avant le run laissait le journal "
         f"decrire des re-applications que rien n'avait ecrites."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
