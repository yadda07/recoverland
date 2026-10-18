"""tx_task_empty_outcome - RecoverLand validation runtime.

Invariant: I-OUTCOME (tout travail de fond se termine par une issue
observable : un resultat, un resultat vide, ou une erreur ; jamais un
composant mort presente comme "en cours").

Le mecanisme
------------
Sur QGIS 3.40+/4.x, `supports_qgs_task()` est vrai : les travaux passent par
QgsTaskWrapper, dont finished() appelle ``on_finished(exception, values)``
seulement ``if self.returned_values``, sinon ``on_finished(exception)``.
`_run_fetch` rend ``[]`` quand la fenetre ne contient aucun evenement :
_handle_task_finished recoit alors result=None et n'emet RIEN, ni
events_ready ni error_occurred. Meme silence quand l'utilisateur annule la
tache depuis le gestionnaire de taches de QGIS : la fonction rend None parce
que task.isCanceled(), alors que self._stopped est faux. Et un index de
geometries vide ({}) n'arrive jamais non plus.

L'echec concret pour l'utilisateur
----------------------------------
Il rembobine a 9 h, puis ramene le curseur apres la derniere modification et
clique Rewind pour revenir : la fenetre compte 0 evenement, aucun signal, la
barre de progression et le logo tournent pour toujours, Rewind, Recherche et
Review restent bloques jusqu'au redemarrage de QGIS. La branche prevue
("Rien a restaurer" / "Remettre N entite(s) dans leur etat actuel ?") est
morte.

Le scenario utilise le vrai QgsTaskManager et un vrai journal SQLite.

Resultat partiel apres une annulation du gestionnaire de taches
---------------------------------------------------------------------
`_boxed` ne regardait l'annulation que si la fonction rendait None. Or
plusieurs travaux rendent ce qu'ils ont deja quand ils voient
task.isCanceled() : les statistiques du journal (`_run_stats` rend le
StatsResult a moitie rempli), l'index de geometries (`_build_digests`
s'arrete et rend le dictionnaire partiel). Le resultat partiel partait
alors comme un resultat complet ("done"). Pour l'utilisateur : il annule
le calcul des statistiques depuis le gestionnaire de taches de QGIS, et le
dialogue affiche "Aucune couche sauvegardee" et vide la liste des couches
du Rewind, alors que le journal en contient. Une annulation par le
gestionnaire doit finir en erreur nommee, jamais en resultat.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

SCENARIO_ID = "tx_task_empty_outcome"
INVARIANT = "I-OUTCOME"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FP = "ogr::tx_task_empty_outcome.gpkg|layername=pts"


class _Journal:
    def __init__(self, path, gate=None):
        self.path = path
        self.is_open = True
        self._gate = gate
        self.entered = threading.Event()

    def create_read_connection(self):
        self.entered.set()
        if self._gate is not None:
            self._gate.wait(10.0)
        return sqlite3.connect(self.path, check_same_thread=False)


def _pump_until(pred, timeout):
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        QCoreApplication.processEvents()
        if pred():
            return True
        time.sleep(0.02)
    QCoreApplication.processEvents()
    return pred()


def _outcomes(thread, result_signal_name):
    got = []
    getattr(thread, result_signal_name).connect(lambda r: got.append(("result", r)))
    thread.error_occurred.connect(lambda e: got.append(("error", e)))
    return got


def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_PLACEHOLDERS, AUDIT_EVENT_INSERT_SQL,
        initialize_schema,
    )
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_task_")
    db = str(Path(tmpdir) / "journal.sqlite")
    conn = sqlite3.connect(db)
    initialize_schema(conn)
    conn.execute(
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")",
        ("proj", _FP, "lyr", "pts", "ogr", json.dumps({"fid": 1}), "UPDATE",
         json.dumps({"changed_only": {"nom": {"old": "a", "new": "b"}}}),
         None, "Point", "EPSG:2154", None, "tester", None,
         "2026-01-01T10:00:00+00:00", None, "fid:1", 5, None, None))
    conn.commit()
    conn.close()
    ctx.data["tmpdir"] = tmpdir
    ctx.data["db"] = db
    flog(f"tx_task_empty_outcome setup: trace_id={ctx.trace_id} db={db}", "INFO")


def run(ctx):
    from qgis.core import QgsVectorLayer
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.logger import flog
    from recoverland.core.restore_contracts import CutoffType, RestoreCutoff
    from recoverland.local_search_thread import LocalSearchThread
    from recoverland.qgs_task_support import supports_qgs_task
    from recoverland.version_fetch_thread import VersionFetchThread
    d = ctx.data
    d["qgs_task_backend"] = bool(supports_qgs_task())
    cutoff = RestoreCutoff(CutoffType.BY_DATE, "2026-06-01T00:00:00", inclusive=True)

    # ---- 1. a window with zero event ------------------------------------
    t1 = VersionFetchThread(_Journal(d["db"]), [_FP], cutoff,
                            trace_id=f"{ctx.trace_id}_E")
    got1 = _outcomes(t1, "events_ready")
    t1.start()
    _pump_until(lambda: got1 and not t1.isRunning(), 6.0)
    _pump_until(lambda: False, 0.3)
    d["empty_window"] = [(k, v if k == "error" else len(v)) for k, v in got1]
    d["empty_window_running"] = bool(t1.isRunning())

    # ---- 2. cancelled from the QGIS task manager -------------------------
    gate = threading.Event()
    j2 = _Journal(d["db"], gate=gate)
    t2 = VersionFetchThread(j2, [_FP], cutoff, trace_id=f"{ctx.trace_id}_C")
    got2 = _outcomes(t2, "events_ready")
    t2.start()
    j2.entered.wait(5.0)
    task = getattr(t2, "_task", None)
    if task is not None:
        task.cancel()          # what the task manager's cancel button does
    gate.set()
    _pump_until(lambda: got2 and not t2.isRunning(), 6.0)
    d["fetch_cancelled_outside"] = [(k, str(v)[:120]) for k, v in got2]

    gate3 = threading.Event()
    j3 = _Journal(d["db"], gate=gate3)
    criteria = SearchCriteria(_FP, None, None, None, None, None, 1, 50)
    t3 = LocalSearchThread(j3, criteria, trace_id=f"{ctx.trace_id}_S")
    got3 = _outcomes(t3, "results_ready")
    t3.start()
    j3.entered.wait(5.0)
    task3 = getattr(t3, "_task", None)
    if task3 is not None:
        task3.cancel()
    gate3.set()
    _pump_until(lambda: got3 and not t3.isRunning(), 6.0)
    d["search_cancelled_outside"] = [(k, str(v)[:120]) for k, v in got3]

    # ---- 2b. a job that hands back what it has when it sees the cancel --
    # _run_stats returns its half-filled StatsResult on is_cancelled():
    # cancelled while it opens its connection, it has computed nothing.
    from recoverland.journal_stats_thread import JournalStatsThread
    gate5 = threading.Event()
    j5 = _Journal(d["db"], gate=gate5)
    t5 = JournalStatsThread(j5, criteria=None, trace_id=f"{ctx.trace_id}_T")
    got5 = _outcomes(t5, "stats_ready")
    t5.start()
    j5.entered.wait(5.0)
    task5 = getattr(t5, "_task", None)
    if task5 is not None:
        task5.cancel()
    gate5.set()
    _pump_until(lambda: got5 and not t5.isRunning(), 6.0)
    _pump_until(lambda: False, 0.3)
    d["stats_cancelled_outside"] = [
        (k, str(v)[:120] if k == "error"
         else f"layers={getattr(v, 'layers', None)} health={getattr(v, 'health', None)}")
        for k, v in got5]

    # ---- 2c. the wrapper itself: a value returned after the cancel ------
    from recoverland import qgs_task_support as qts

    class _CancelledTask:
        def isCanceled(self):
            return True

    def _partial_job(_task):
        return {1: "digest of the first feature only"}
    boxed = qts._boxed(_partial_job)(_CancelledTask())
    d["boxed_partial_after_cancel"] = [str(x) for x in boxed]

    # ---- 3. an empty geometry index ------------------------------------
    from recoverland.layer_index_thread import build_index_async
    empty = QgsVectorLayer("Point?crs=EPSG:2154&field=a:string", "vide", "memory")
    got4 = []
    worker = build_index_async(empty, "vide", f"{ctx.trace_id}_I",
                               on_ready=lambda r: got4.append(("result", r)),
                               on_failed=lambda e: got4.append(("error", e)))
    _pump_until(lambda: bool(got4), 6.0)
    d["empty_index"] = [(k, str(v)[:60]) for k, v in got4]
    d["index_worker"] = worker is not None

    # ---- 4. the user-facing path: Rewind on an empty window -------------
    d.update(_dialog_rewind_empty_window(ctx))
    flog(f"tx_task_empty_outcome run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def _dialog_rewind_empty_window(ctx) -> dict:
    """Real plugin + dialog: Rewind with the slider after the last edit."""
    from qgis.PyQt.QtCore import QDateTime
    from scripts.validation.scenarios import _rvw_harness as h
    out = {}
    tmp = tempfile.mkdtemp(prefix="rl_tx_task_dlg_")
    gpkg = h.make_gpkg(tmp, "cadastre", [("P1", "Alpha", 1.0, 1.0, 1.0)])
    iface, plugin = h.start_plugin(tmp)
    try:
        layer = h.load_layer(gpkg, "cadastre")
        h.pump(0.2)
        h.user_commit(layer, lambda lyr: h.rename(lyr, "P1", "Alpha2"))
        h.wait_rows(h.journal_path(plugin), 1)
        dlg = h.open_dialog(plugin, [layer])
        dlg.restore_mode_selector.setMode("temporal")
        dlg._on_period_mode_changed("temporal")
        dlg.time_slider.cutoff_datetime = lambda: QDateTime.currentDateTimeUtc().addSecs(3600)
        with h.MessageBoxRecorder() as boxes:
            dlg.recover_and_load()
            released = h.pump_until(lambda: not dlg._is_recovering, 8.0)
        out["dialog_released"] = bool(released)
        out["dialog_recovering"] = bool(dlg._is_recovering)
        out["dialog_boxes"] = boxes.texts()
        # Whatever happened, do not leave the guard up for the next scenario.
        dlg._is_recovering = False
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        out["crash"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
        shutil.rmtree(tmp, ignore_errors=True)
    return out


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "runs_on_the_qgstask_backend",
        d.get("qgs_task_backend") is True,
        f"QgsTask actif={d.get('qgs_task_backend')} : le defaut ne vit que "
        f"sur ce chemin, celui de QGIS 3.40+/4.x",
    ))
    out.append((
        "empty_window_is_delivered",
        d.get("empty_window") == [("result", 0)],
        f"issues={d.get('empty_window')} (attendu une seule : events_ready([])) : "
        f"sans elle le dialogue reste 'en cours' pour toute la session",
    ))
    # Hypothesis revised (v7): these two only asked for exactly ONE outcome.
    # A result is not an acceptable one after a cancel from the task
    # manager (the job stopped before its end, whatever it hands back), so
    # the single outcome must now also be the named error: strictly
    # stronger.
    fetch = d.get("fetch_cancelled_outside") or []
    out.append((
        "fetch_cancelled_from_task_manager_has_an_outcome",
        len(fetch) == 1 and fetch[0][0] == "error",
        f"issues={fetch} (attendu exactement une, une erreur nommee : "
        f"un resultat ferait passer un travail interrompu pour complet)",
    ))
    search = d.get("search_cancelled_outside") or []
    out.append((
        "search_cancelled_from_task_manager_has_an_outcome",
        len(search) == 1 and search[0][0] == "error",
        f"issues={search} (attendu exactement une, une erreur nommee)",
    ))
    stats = d.get("stats_cancelled_outside") or []
    out.append((
        "stats_cancelled_from_task_manager_is_an_error_not_a_partial_result",
        len(stats) == 1 and stats[0][0] == "error",
        f"issues={stats} (attendu une seule erreur) : les statistiques "
        f"interrompues arrivaient comme un resultat, le dialogue affichait "
        f"'Aucune couche sauvegardee' et vidait la liste des couches du "
        f"Rewind alors que le journal en contient",
    ))
    boxed = d.get("boxed_partial_after_cancel") or []
    out.append((
        "a_value_returned_after_a_cancel_is_never_delivered",
        len(boxed) == 2 and boxed[0] == "cancelled" and boxed[1] == "None",
        f"_boxed(job partiel apres annulation)={boxed} (attendu "
        f"['cancelled', 'None']) : un index de geometries arrete a la "
        f"premiere entite etait livre comme l'index complet",
    ))
    out.append((
        "empty_index_is_delivered",
        d.get("index_worker") is True and d.get("empty_index") == [("result", "{}")],
        f"issues={d.get('empty_index')} worker={d.get('index_worker')} : un index "
        f"vide est un resultat, pas une attente sans fin",
    ))
    out.append((
        "rewind_on_empty_window_releases_the_dialog",
        d.get("dialog_released") is True and d.get("dialog_recovering") is False,
        f"dialogue libere={d.get('dialog_released')} encore en cours="
        f"{d.get('dialog_recovering')} messages={d.get('dialog_boxes')} : Rewind, "
        f"Recherche et Review restaient bloques jusqu'au redemarrage{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_task_empty_outcome.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
