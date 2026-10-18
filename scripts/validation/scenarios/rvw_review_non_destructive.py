"""rvw_review_non_destructive - RecoverLand validation runtime.

Invariant: I-RVW-READONLY (Review est strictement non destructif : il ne met
jamais une couche source en edition, ne cree aucun evenement, n'ecrit aucune
donnee, n'influence pas le tracker ; ses calques sont des couches temporaires
RecoverLand qui ne finissent jamais dans le fichier projet, et aucun de ses
threads ne survit a la fermeture).

Ce que le scenario enchaine, comme un utilisateur brutal
--------------------------------------------------------
Review ON puis OFF puis ON, une rafale de changements de date sans attendre,
un commit de l'utilisateur pendant Review, une sauvegarde du projet pendant
Review, le passage Review -> Rewind (sans lancer de Rewind), la fermeture du
dialogue avec Review persistant, puis l'ouverture d'un autre projet et le
dechargement du plugin. A chaque etape les donnees sources, le journal, les
threads et le fichier projet sont compares a ce que l'utilisateur seul a fait.

L'echec concret que ce scenario interdit
----------------------------------------
Review qui ouvre une session d'edition sur une couche source, ajoute une ligne
au journal (le Rewind suivant la rejouerait), laisse un thread lire un journal
ferme apres le dechargement, ou enregistre ses calques dans le .qgs : autant
de traces d'un outil de CONSULTATION dans les donnees de l'utilisateur.

Verdict attendu avant correctif : FAIL (etat a T faux apres la rafale,
calques ecrits dans le projet, Review survivant au changement de projet).
Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_review_non_destructive"
INVARIANT = "I-RVW-READONLY"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS = [
    ("P1", "Alpha", 100.0, 1.0, 1.0),
    ("P2", "Beta", 200.0, 2.0, 2.0),
    ("P3", "Gamma", 300.0, 3.0, 3.0),
    ("P4", "Delta", 400.0, 4.0, 4.0),
]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _provider_state(layer) -> list:
    """What is really stored: committed features, read from the provider."""
    out = []
    for f in layer.dataProvider().getFeatures():
        g = f.geometry()
        out.append((str(f["code"]), str(f["nom"]),
                    g.asWkt(3) if g is not None and not g.isNull() else None))
    return sorted(out)


def _recording_classes(workers):
    import recoverland.widgets.marker_scan_worker as mmod
    import recoverland.widgets.snapshot_rebuild_worker as wmod

    class _RecSnap(wmod.SnapshotRebuildWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            workers.append(self)

    class _RecMarker(mmod.MarkerScanWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            workers.append(self)

    return wmod, mmod, _RecSnap, _RecMarker


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_readonly_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", _ROWS)
    flog(f"rvw_review_non_destructive setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    workers = []
    wmod, mmod, rec_snap, rec_marker = _recording_classes(workers)
    real_snap, real_marker = wmod.SnapshotRebuildWorker, mmod.MarkerScanWorker
    wmod.SnapshotRebuildWorker, mmod.MarkerScanWorker = rec_snap, rec_marker
    iface, plugin = h.start_plugin(d["tmpdir"])
    project = QgsProject.instance()
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        edit_sessions = []
        layer.editingStarted.connect(lambda: edit_sessions.append(time.monotonic()))
        jpath = h.journal_path(plugin)
        d["journal"] = jpath
        rows0 = h.journal_rows(jpath)
        cutoff = h.utc_now_iso()
        d["cutoff"] = cutoff
        time.sleep(1.2)
        h.user_commit(layer, lambda lyr: h.delete(lyr, "P1"))
        h.user_commit(layer, lambda lyr: h.move(lyr, "P2", 20.0, 20.0))
        rows_user = h.wait_rows(jpath, rows0 + 2)
        d["user_rows_1"] = rows_user - rows0

        h.set_view(iface, 0.0, 0.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layer])
        d["on_1"] = h.review_on(dlg, 1)
        h.review_off(dlg)
        d["on_2"] = h.review_on(dlg, 1)

        # ---- a storm of date changes, no waiting --------------------------
        for day in range(1, 11):
            dlg._on_snapshot_date_changed(f"2026-0{1 + day % 9}-1{day % 10}T08:00:00")
            h.pump(0.01)
        d["storm_settled"] = h.review_date(dlg, cutoff)
        d["overlay_after_storm"] = h.overlay_rows(dlg)

        # ---- the user commits during Review ------------------------------
        h.user_commit(layer, lambda lyr: h.rename(lyr, "P3", "Gamma2"))
        rows_user = h.wait_rows(jpath, rows0 + 3)
        d["user_rows_2"] = rows_user - rows0
        h.pump_until(lambda: "P3" in h.overlay_rows(dlg), 8.0)
        h.pump_until(lambda: h.review_idle(dlg), 8.0)
        d["overlay_after_commit"] = h.overlay_rows(dlg)
        expected_state = _provider_state(layer)

        # ---- the user saves the project while Review is on ---------------
        saved = os.path.join(d["tmpdir"], "pendant_review.qgs")
        project.write(saved)
        h.pump(0.3)
        text = open(saved, encoding="utf-8", errors="replace").read()
        d["saved_traces"] = [t for t in ("__rl_snap_", "_rl_snap_managed",
                                         "Review Snapshot", "(snapshot)") if t in text]

        # ---- Review -> Rewind (no Rewind launched) -----------------------
        dlg.restore_mode_selector.setMode("temporal")
        dlg._on_period_mode_changed("temporal")
        h.pump(0.3)
        d["after_rewind_switch"] = {
            "snap_mode": bool(dlg._review_snap_mode),
            "managed_layers": h.managed_layers(),
            "recovering": bool(dlg._is_recovering),
            "rewind_enabled": bool(dlg.recover_button.isEnabled()),
        }

        # ---- back to Review, close the dialog (Review persists) ----------
        d["on_3"] = h.review_on(dlg, 1)
        dlg.close()
        h.pump(0.3)
        d["persists_after_close"] = bool(dlg._review_snap_mode)
        d["state_before_switch"] = _provider_state(layer) == expected_state
        d["layer_editable"] = bool(layer.isEditable())
        d["edit_sessions"] = len(edit_sessions)
        d["journal_rows_before_switch"] = h.journal_rows(jpath) - rows0

        # ---- another project ---------------------------------------------
        other = os.path.join(d["tmpdir"], "autre.qgs")
        project.clear()
        project.write(other)
        project.read(other)
        h.pump(0.6)
        d["review_after_switch"] = bool(dlg._review_snap_mode)
        d["journal_rows_after_switch"] = h.journal_rows(jpath) - rows0
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
        wmod.SnapshotRebuildWorker, mmod.MarkerScanWorker = real_snap, real_marker
    h.pump(0.5)
    d["n_workers"] = len(workers)
    d["workers_running_after_unload"] = sum(1 for w in workers if w.isRunning())
    retained = getattr(wmod, "retained_threads", None)
    d["retained_after_unload"] = (
        sum(1 for t in retained() if t.isRunning()) if callable(retained) else None)
    workers.clear()
    flog(f"rvw_review_non_destructive run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    storm = d.get("overlay_after_storm") or {}
    after = d.get("overlay_after_commit") or {}
    sw = d.get("after_rewind_switch") or {}
    out = []
    out.append((
        "review_on_off_on_works",
        d.get("on_1") is True and d.get("on_2") is True and d.get("on_3") is True,
        f"on_1={d.get('on_1')} on_2={d.get('on_2')} on_3={d.get('on_3')}{crash}",
    ))
    out.append((
        "state_at_t_survives_a_storm_of_dates",
        d.get("storm_settled") is True
        and (storm.get("P1") or {}).get("x") == 1.0
        and (storm.get("P2") or {}).get("x") == 2.0
        and not ({"P3", "P4"} & set(storm)),
        f"calque Review apres la rafale={storm} (attendu P1 a (1,1), P2 a (2,2), "
        f"rien d'autre) : une reponse d'une date intermediaire ne doit pas "
        f"rester affichee",
    ))
    out.append((
        "commit_during_review_is_reflected",
        (after.get("P3") or {}).get("nom") == "Gamma",
        f"calque Review apres le commit de l'utilisateur={after} (attendu P3 "
        f"avec son nom a T 'Gamma')",
    ))
    out.append((
        "review_never_edits_a_source_layer",
        d.get("edit_sessions") == 3 and d.get("layer_editable") is False
        and d.get("state_before_switch") is True,
        f"sessions d'edition sur la couche source={d.get('edit_sessions')} "
        f"(attendu 3, celles de l'utilisateur) en edition={d.get('layer_editable')} "
        f"donnees identiques a ce que l'utilisateur a enregistre="
        f"{d.get('state_before_switch')}",
    ))
    out.append((
        "review_writes_no_journal_row",
        d.get("user_rows_2") == 3 and d.get("journal_rows_before_switch") == 3
        and d.get("journal_rows_after_switch") == 3,
        f"lignes du journal : apres les commits de l'utilisateur="
        f"{d.get('user_rows_2')}, avant le changement de projet="
        f"{d.get('journal_rows_before_switch')}, apres="
        f"{d.get('journal_rows_after_switch')} (attendu 3 partout : Review "
        f"n'ajoute rien qu'un Rewind rejouerait)",
    ))
    out.append((
        "overlays_never_saved_in_the_project",
        d.get("saved_traces") == [],
        f"traces Review dans le projet enregistre={d.get('saved_traces')}",
    ))
    out.append((
        "rewind_switch_ends_review_cleanly",
        sw.get("snap_mode") is False and sw.get("managed_layers") == []
        and sw.get("recovering") is False and sw.get("rewind_enabled") is True,
        f"apres Review -> Rewind : {sw}",
    ))
    out.append((
        "review_does_not_survive_a_project_switch",
        d.get("persists_after_close") is True and d.get("review_after_switch") is False,
        f"persiste apres fermeture={d.get('persists_after_close')} (voulu) ; "
        f"encore actif dans l'autre projet={d.get('review_after_switch')}",
    ))
    out.append((
        "no_review_thread_alive_after_unload",
        (d.get("n_workers") or 0) >= 2 and d.get("workers_running_after_unload") == 0
        and d.get("retained_after_unload") in (None, 0),
        f"workers Review crees={d.get('n_workers')} encore en cours apres "
        f"dechargement={d.get('workers_running_after_unload')} retenus en cours="
        f"{d.get('retained_after_unload')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_review_non_destructive.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
