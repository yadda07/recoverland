"""rvw_commit_rebuild_after_commit - RecoverLand validation runtime.

Invariant: BL-RVW-P1-03 (le rafraichissement automatique de Review sur un
commit reflete le journal APRES le commit notifie).

Le mecanisme
------------
`_review_on_commit_changed` effacait le cache (``_review_snap_raw_result``) et
relancait le minuteur de 600 ms, mais laissait tourner, connecte, le worker en
cours. Ce worker avait lu le journal AVANT le commit. S'il livrait pendant les
600 ms, `_on_snapshot_result` rangeait son resultat perime sous la date
courante ; a l'expiration du minuteur, `_on_snapshot_date_changed` trouvait
``raw_iso == iso`` et reutilisait ce cache sans relire le journal. En plus,
le worker lisait en autocommit, une requete par couche et par phase : un
commit tombant entre deux lectures donnait un etat qui n'a jamais existe.

L'echec concret pour l'utilisateur
----------------------------------
Review a la date d'hier. L'utilisateur change la date (un worker part) et,
pendant la lecture, enregistre une modification. Le resultat d'avant le commit
arrive, il est mis en cache, puis reutilise : l'entite qu'il vient de modifier
n'apparait jamais dans Review tant qu'il ne bouge pas la date.

Partie A : la course, rendue deterministe par un worker simule qui livre au
pire moment (entre la notification du commit et l'expiration du minuteur).
Partie B : le chemin reel de bout en bout (vrai worker, vrai commit pendant
Review) : l'entite modifiee apparait dans le calque Review.

Verdict attendu avant correctif : FAIL (partie A). Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_commit_rebuild_after_commit"
INVARIANT = "BL-RVW-P1-03"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_commit_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", [
        ("P1", "Alpha", 1.0, 1.0, 1.0)])
    flog(f"rvw_commit_rebuild_after_commit setup: trace_id={ctx.trace_id}", "INFO")


def _fake_worker_class(created):
    from qgis.PyQt.QtCore import QObject, pyqtSignal

    class _FakeWorker(QObject):
        """Stands for a SnapshotRebuildWorker that read the journal early."""
        result_ready = pyqtSignal(str, object)
        error = pyqtSignal(str, str)
        finished = pyqtSignal()

        def __init__(self, journal, layer_infos, cutoff_iso,
                     bbox_per_layer=None, trace_id="", row_budget=None,
                     parent=None):
            super().__init__(None)
            self.trace_id = f"fake{len(created)}"
            self.cutoff_iso = cutoff_iso
            self.cancelled = False
            created.append(self)

        def start(self):
            pass

        def cancel(self):
            self.cancelled = True

        def isRunning(self):
            return False

        def wait(self, *_args):
            return True

    return _FakeWorker


def run(ctx):
    from datetime import datetime, timezone
    from recoverland.core.logger import flog
    from recoverland.core.temporal_snapshot_engine import reconstruct_snapshot_at
    import recoverland.widgets.snapshot_rebuild_worker as wmod
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    real_cls = wmod.SnapshotRebuildWorker
    created = []
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        # Two entities born under the journal (INSERT before T).
        h.user_commit(layer, lambda lyr: h.add(lyr, "P7", "Eta", 7.0, 7.0, 7.0))
        h.user_commit(layer, lambda lyr: h.add(lyr, "P8", "Theta", 8.0, 8.0, 8.0))
        rows0 = h.wait_rows(jpath, 2)
        time.sleep(1.2)
        cutoff = h.utc_now_iso()
        d["cutoff"] = cutoff
        time.sleep(1.2)
        h.user_commit(layer, lambda lyr: h.rename(lyr, "P7", "Eta2"))
        h.wait_rows(jpath, rows0 + 1)

        h.set_view(iface, 0.0, 0.0, 10.0, 10.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)
        d["overlay_before"] = h.overlay_rows(dlg)

        # ---- Part A: the race, made deterministic -------------------------
        wmod.SnapshotRebuildWorker = _fake_worker_class(created)
        dlg._on_snapshot_date_changed("2099-01-01T00:00:00")
        w1 = created[0] if created else None
        stale = reconstruct_snapshot_at(
            {}, datetime(2099, 1, 1, tzinfo=timezone.utc), trace_id="STALE")
        dlg._review_on_commit_changed(1)
        if w1 is not None:
            # The pre-commit read lands inside the 600 ms debounce.
            w1.result_ready.emit(w1.trace_id, stale)
        d["stale_cached"] = dlg._review_snap_raw_result is stale
        d["w1_cancelled"] = bool(w1 is not None and w1.cancelled)
        dlg._review_debounce.stop()
        dlg._review_auto_refresh()
        d["n_workers_after_debounce"] = len(created)
        wmod.SnapshotRebuildWorker = real_cls
        for w in created:
            w.finished.emit()

        # ---- Part B: the real path, commit during Review ------------------
        d["review_date_again"] = h.review_date(dlg, cutoff)
        h.user_commit(layer, lambda lyr: h.rename(lyr, "P8", "Theta2"))
        h.wait_rows(jpath, rows0 + 2)
        h.pump_until(lambda: "P8" in h.overlay_rows(dlg), 8.0)
        h.pump_until(lambda: h.review_idle(dlg), 8.0)
        d["overlay_after_commit"] = h.overlay_rows(dlg)
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        wmod.SnapshotRebuildWorker = real_cls
        for w in created:
            try:
                w.finished.emit()
            except RuntimeError:
                pass
        h.shutdown(plugin)
    flog(f"rvw_commit_rebuild_after_commit run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    before = d.get("overlay_before") or {}
    after = d.get("overlay_after_commit") or {}
    out = []
    out.append((
        "setup_review_shows_the_state_at_t",
        d.get("review_on") is True and (before.get("P7") or {}).get("nom") == "Eta",
        f"review_on={d.get('review_on')} calque Review={before}{crash}",
    ))
    out.append((
        "commit_cancels_the_worker_that_read_before_it",
        d.get("w1_cancelled") is True,
        f"worker en vol annule par le commit={d.get('w1_cancelled')} : il a lu "
        f"le journal avant le commit, sa reponse est perimee",
    ))
    out.append((
        "pre_commit_result_is_never_cached",
        d.get("stale_cached") is False,
        f"resultat d'avant le commit mis en cache={d.get('stale_cached')} : il "
        f"serait reutilise tel quel par le rafraichissement",
    ))
    out.append((
        "debounce_rebuilds_from_the_journal",
        d.get("n_workers_after_debounce") == 2,
        f"workers crees={d.get('n_workers_after_debounce')} (attendu 2 : le "
        f"rafraichissement doit relire le journal, pas reutiliser le cache)",
    ))
    out.append((
        "commit_during_review_appears_in_the_overlay",
        (after.get("P8") or {}).get("nom") == "Theta"
        and (after.get("P7") or {}).get("nom") == "Eta",
        f"calque Review apres un commit pendant Review={after} (attendu P8 avec "
        f"son nom a T 'Theta', P7 'Eta')",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_commit_rebuild_after_commit.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
