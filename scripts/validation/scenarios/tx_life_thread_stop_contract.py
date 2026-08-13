"""tx_life_thread_stop_contract - RecoverLand validation runtime.

Invariant: I-THREAD-STOP (quand le dialogue lache la reference d'un
worker, ce worker ne touche plus au journal).

Le mecanisme
------------
Les trois workers de fond (`LocalSearchThread`, `JournalStatsThread`,
`VersionFetchThread`) heritent tous de `TaskEnabledThread`. Sur QGIS
3.40+/4.x, `supports_qgs_task()` est vrai, donc `start()` ne demarre
AUCUN QThread : il soumet une `QgsTask` au gestionnaire de taches et se
contente de poser un drapeau Python.

    def isRunning(self):
        if self._task is not None:
            return self._running
        return QThread.isRunning(self)

    def wait(self, msecs=0):
        if self._task is not None:
            return not self._running
        return QThread.wait(self, msecs)

`wait()` ne se synchronise sur rien. Elle lit un booleen et rend la main.
Or `_running` n'est remis a False que par `_clear_task()`, appele depuis
`_handle_task_finished`, qui arrive par le boucle d'evenements Qt — donc
jamais pendant l'appel a `wait()`, qui ne pompe pas la boucle.

Consequence directe sur le code du dialogue (`recover_dialog.py`) :

    self._version_fetch_thread.stop()
    self._version_fetch_thread.wait(500)          # ligne 2940, 3349
    ...
    if self._stats_thread.isRunning():
        self._stats_thread.wait(500)              # cleanup_resources
    self._stats_thread = None

Le developpeur a ecrit une barriere. Il n'y en a pas. `wait(500)` rend la
main en 0 ms, la reference est mise a None, `cleanup_resources` se
termine, et `RecoverPlugin._shutdown_local_backend` enchaine sur
`self._journal.close()` — pendant que la fonction de la QgsTask, elle,
est encore en train d'ouvrir ou de lire une connexion sur ce journal.

Le scenario d'echec concret
---------------------------
Journal sur un partage reseau ou un NAS (cas d'usage revendique : le
journal vit a cote du .qgz du projet). `create_read_connection()` prend
plusieurs centaines de millisecondes. L'utilisateur lance un REWIND,
change d'avis, ferme la fenetre, et QGIS enchaine sur un autre projet.
Le worker orphelin continue, appelle `count_callback` = un signal Qt
d'un QObject en cours de destruction, et lit un journal deja ferme.

Pour rendre la chose deterministe sans rien changer au produit, le
scenario glisse un proxy de journal dont `create_read_connection()`
attend un verrou que le scenario controle. Le worker est alors
demontrablement encore a l'interieur du journal au moment ou `wait()`
annonce qu'il est termine.

Ce que ce scenario torture (journal SQLite reel de 45 000 evenements,
QgsTaskManager reel, pas d'interface) :
    T1  wait(3000) juste apres start()      -> barriere ou pas ?
    T2  stop() puis wait(3000)              -> annonce "termine" alors que
                                               le worker est encore dedans
    T3  annulation en pleine execution      -> aucun resultat emis apres stop
    T4  double start()                      -> une seule tache soumise
    T5  stop() puis start() en vol          -> redemarrage avale en silence
    T6  stop() avant start()                -> l'objet reste utilisable
    T7  JournalStatsThread sur journal ferme-> pas de plantage, resultat vide
"""
from __future__ import annotations

import shutil
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

SCENARIO_ID = "tx_life_thread_stop_contract"
INVARIANT = "I-THREAD-STOP"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_N_FINGERPRINTS = 150
_N_PER_FINGERPRINT = 300
_GATE_TIMEOUT = 15.0
_PUMP_LIMIT = 12.0


# --------------------------------------------------------------------- #
# Outillage : pompage de la boucle Qt et proxy de journal
# --------------------------------------------------------------------- #

def _pump(predicate, limit: float = _PUMP_LIMIT) -> float:
    """Fait tourner la boucle Qt jusqu'a `predicate()` ou expiration.

    Le lanceur headless ne demarre pas `app.exec_()`, donc les callbacks
    `on_finished` des QgsTask n'arrivent que si quelqu'un pompe.
    """
    from qgis.PyQt.QtCore import QCoreApplication

    t0 = time.monotonic()
    while time.monotonic() - t0 < limit:
        QCoreApplication.processEvents()
        if predicate():
            break
        time.sleep(0.005)
    return time.monotonic() - t0


class _CountingConnection:
    """Proxy de connexion SQLite qui compte les fermetures."""

    def __init__(self, conn, owner):
        self._conn = conn
        self._owner = owner

    def close(self):
        self._owner.note_close()
        return self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _GatedJournal:
    """Proxy de JournalManager dont l'ouverture de connexion est retenue.

    Simule un journal lent a ouvrir (partage reseau, NAS, antivirus).
    Aucun code produit n'est modifie : les workers ne connaissent du
    journal que `is_open`, `path` et `create_read_connection()`.
    """

    def __init__(self, real, gate: threading.Event):
        self._real = real
        self._gate = gate
        self.entered = threading.Event()
        self.opened = threading.Event()
        self._lock = threading.Lock()
        self.n_opened = 0
        self.n_closed = 0
        self.errors = []

    # -- surface consommee par les workers ---------------------------- #
    @property
    def is_open(self):
        return self._real.is_open

    @property
    def path(self):
        return self._real.path

    def create_read_connection(self):
        self.entered.set()
        self._gate.wait(_GATE_TIMEOUT)
        try:
            conn = self._real.create_read_connection()
        except Exception as exc:  # noqa: BLE001 - observe, ne masque pas
            self.errors.append(repr(exc))
            raise
        with self._lock:
            self.n_opened += 1
        self.opened.set()
        return _CountingConnection(conn, self)

    # -- observation --------------------------------------------------- #
    def note_close(self):
        with self._lock:
            self.n_closed += 1

    @property
    def live_connections(self) -> int:
        with self._lock:
            return self.n_opened - self.n_closed


def _seed_journal(path: str) -> int:
    from recoverland.core.sqlite_schema import (
        initialize_schema, AUDIT_EVENT_INSERT_SQL,
        AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    conn = sqlite3.connect(path)
    initialize_schema(conn)
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    rows = []
    for f in range(_N_FINGERPRINTS):
        for i in range(_N_PER_FINGERPRINT):
            rows.append((
                "tx_thread_project", "ds%03d" % f, "lyr", "reseau", "ogr",
                '{"fid": %d}' % i, "UPDATE",
                '{"changed_only": {"statut": {"old": "A", "new": "B"}}}',
                None, "NoGeometry", "EPSG:2154", "[]", "tester", None,
                "2026-08-13T10:%02d:%02d+00:00" % (i % 60, f % 60),
                None, "fid:%d" % i, 2, None, None,
            ))
    conn.executemany(sql, rows)
    conn.commit()
    conn.close()
    return len(rows)


# --------------------------------------------------------------------- #
# Scenario
# --------------------------------------------------------------------- #

def setup(ctx):
    import os
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_thread_")
    path = os.path.join(tmpdir, "j", "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    n_rows = _seed_journal(path)

    journal = JournalManager()
    journal._path = path
    journal._conn = sqlite3.connect(path, check_same_thread=False)

    ctx.data["tmpdir"] = tmpdir
    ctx.data["journal"] = journal
    ctx.data["journal_path"] = path
    ctx.data["n_rows"] = n_rows

    from recoverland.qgs_task_support import supports_qgs_task
    ctx.data["task_backend"] = bool(supports_qgs_task())

    flog(
        f"tx_life_thread_stop_contract setup: trace_id={ctx.trace_id} "
        f"rows={n_rows} task_backend={ctx.data['task_backend']} path={path}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.version_fetch_thread import VersionFetchThread
    from recoverland.local_search_thread import LocalSearchThread
    from recoverland.journal_stats_thread import JournalStatsThread
    from recoverland.core.logger import flog

    journal = ctx.data["journal"]
    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, "2026-08-13T00:00:00+00:00", inclusive=True)
    fps = ["ds%03d" % f for f in range(_N_FINGERPRINTS)]
    criteria = SearchCriteria(None, None, None, None, None, None, 1, 100)

    flog(f"tx_life_thread_stop_contract run start: trace_id={ctx.trace_id}", "INFO")

    # ===== T1 / T2 : wait() n'est pas une barriere ======================
    gate = threading.Event()
    spy = _GatedJournal(journal, gate)
    th = VersionFetchThread(spy, fps, cutoff, trace_id=ctx.trace_id + "_T2")
    emitted = []
    th.events_ready.connect(lambda ev: emitted.append(("events", len(ev))))
    th.error_occurred.connect(lambda e: emitted.append(("error", e)))
    th.start()

    # La tache doit etre reellement entree dans le journal avant de tester
    # wait(), sinon on mesurerait le temps de mise en file, pas la barriere.
    _pump(spy.entered.is_set, limit=5.0)
    ctx.data["t1_worker_started"] = spy.entered.is_set()
    ctx.data["t1_running_before_wait"] = th.isRunning()

    t0 = time.monotonic()
    r1 = th.wait(3000)
    ctx.data["t1_wait_returned"] = r1
    ctx.data["t1_wait_ms"] = int((time.monotonic() - t0) * 1000)

    th.stop()
    t0 = time.monotonic()
    r2 = th.wait(3000)
    ctx.data["t2_wait_returned"] = r2
    ctx.data["t2_wait_ms"] = int((time.monotonic() - t0) * 1000)
    ctx.data["t2_is_running_after_wait"] = th.isRunning()
    # Instant de verite : le worker est-il sorti du journal ?
    ctx.data["t2_worker_still_inside_journal"] = not spy.opened.is_set()

    gate.set()  # on relache le worker
    _pump(lambda: spy.opened.is_set() or bool(spy.errors), limit=6.0)
    ctx.data["t2_worker_finished_later"] = spy.opened.is_set()
    _pump(lambda: not th.isRunning(), limit=6.0)
    ctx.data["t3_emissions_after_stop"] = list(emitted)
    _pump(lambda: spy.live_connections == 0, limit=6.0)
    ctx.data["t3_live_connections"] = spy.live_connections

    # ===== T4 : double start() ==========================================
    gate4 = threading.Event()
    gate4.set()
    spy4 = _GatedJournal(journal, gate4)
    th4 = LocalSearchThread(spy4, criteria, trace_id=ctx.trace_id + "_T4")
    got4 = []
    th4.results_ready.connect(lambda r: got4.append(r))
    th4.error_occurred.connect(lambda e: got4.append(("error", e)))
    th4.start()
    th4.start()
    _pump(lambda: not th4.isRunning() and got4, limit=8.0)
    ctx.data["t4_emissions"] = len(got4)
    ctx.data["t4_connections_opened"] = spy4.n_opened
    ctx.data["t4_live_connections"] = spy4.live_connections

    # ===== T5 : stop() puis start() alors que la tache est en vol =======
    gate5 = threading.Event()
    spy5 = _GatedJournal(journal, gate5)
    th5 = LocalSearchThread(spy5, criteria, trace_id=ctx.trace_id + "_T5")
    got5 = []
    th5.results_ready.connect(lambda r: got5.append(("ok", r)))
    th5.error_occurred.connect(lambda e: got5.append(("error", e)))
    th5.start()
    _pump(spy5.entered.is_set, limit=5.0)
    th5.stop()
    th5.start()                      # l'utilisateur reclique sur Rechercher
    ctx.data["t5_running_after_restart"] = th5.isRunning()
    ctx.data["t5_stopped_flag"] = bool(getattr(th5, "_stopped", None))
    gate5.set()
    _pump(lambda: bool(got5), limit=6.0)
    ctx.data["t5_emissions"] = list(got5)
    _pump(lambda: spy5.live_connections == 0, limit=4.0)

    # ===== T6 : stop() avant tout demarrage =============================
    gate6 = threading.Event()
    gate6.set()
    spy6 = _GatedJournal(journal, gate6)
    th6 = LocalSearchThread(spy6, criteria, trace_id=ctx.trace_id + "_T6")
    got6 = []
    th6.results_ready.connect(lambda r: got6.append(r))
    th6.error_occurred.connect(lambda e: got6.append(("error", e)))
    t6_error = ""
    try:
        th6.stop()
        th6.start()
    except Exception as exc:  # noqa: BLE001
        t6_error = repr(exc)
    _pump(lambda: bool(got6), limit=8.0)
    ctx.data["t6_error"] = t6_error
    ctx.data["t6_emissions"] = len(got6)
    _pump(lambda: spy6.live_connections == 0, limit=4.0)

    # ===== T7 : stats sur un journal deja ferme =========================
    journal.close()
    th7 = JournalStatsThread(journal, criteria, trace_id=ctx.trace_id + "_T7")
    got7 = []
    err7 = []
    th7.stats_ready.connect(lambda r: got7.append(r))
    th7.error_occurred.connect(lambda e: err7.append(e))
    t7_error = ""
    try:
        th7.start()
    except Exception as exc:  # noqa: BLE001
        t7_error = repr(exc)
    _pump(lambda: bool(got7 or err7), limit=8.0)
    ctx.data["t7_start_error"] = t7_error
    ctx.data["t7_error_signals"] = list(err7)
    ctx.data["t7_result_is_empty"] = bool(
        got7 and getattr(got7[0], "stats_cache", "sentinel") is None
        and not getattr(got7[0], "layers", ["x"])
    )
    ctx.data["t7_emissions"] = len(got7)

    flog(
        f"tx_life_thread_stop_contract run end: trace_id={ctx.trace_id} "
        f"t1_wait={ctx.data['t1_wait_returned']}/{ctx.data['t1_wait_ms']}ms "
        f"t2_wait={ctx.data['t2_wait_returned']}/{ctx.data['t2_wait_ms']}ms "
        f"t2_inside={ctx.data['t2_worker_still_inside_journal']} "
        f"t4={ctx.data['t4_emissions']} t5={ctx.data['t5_emissions']} "
        f"t6={ctx.data['t6_emissions']} t7={ctx.data['t7_emissions']}",
        "INFO",
    )

    # Le runner n'a pas de teardown : on rend la main proprement.
    ctx.data["journal"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    d = ctx.data

    # ===== Prerequis ====================================================
    out.append((
        "fixture_runs_on_the_qgstask_backend",
        d.get("task_backend") is True,
        f"supports_qgs_task()={d.get('task_backend')} : sans le backend QgsTask "
        f"le scenario testerait le chemin QThread, qui n'est pas celui utilise "
        f"sur QGIS 3.40+/4.x",
    ))
    out.append((
        "fixture_worker_really_started",
        d.get("t1_worker_started") is True,
        f"le worker n'est jamais entre dans le journal "
        f"(entered={d.get('t1_worker_started')}) : les mesures suivantes "
        f"n'auraient aucun sens",
    ))

    # ===== T1 : wait() n'attend rien ====================================
    out.append((
        "t1_wait_is_a_real_barrier",
        d.get("t1_wait_returned") is True or d.get("t1_wait_ms", 0) >= 2500,
        f"wait(3000) a rendu {d.get('t1_wait_returned')} en "
        f"{d.get('t1_wait_ms')} ms alors que le worker tourne encore "
        f"(isRunning={d.get('t1_running_before_wait')}). Le dialogue ecrit "
        f"`stop(); wait(500)` en croyant poser une barriere : il n'attend rien "
        f"du tout et enchaine sur la fermeture des ressources.",
    ))

    # ===== T2 : "termine" alors que le worker est dedans ================
    out.append((
        "t2_wait_blocks_until_the_worker_left",
        d.get("t2_wait_ms", 0) >= 400
        or d.get("t2_worker_still_inside_journal") is False,
        f"apres stop(), wait(3000) a rendu la main en {d.get('t2_wait_ms')} ms "
        f"(retour={d.get('t2_wait_returned')}) alors que la fonction de la tache "
        f"etait encore a l'interieur de journal.create_read_connection(). "
        f"L'annulation d'une QgsTask n'est qu'une demande : elle n'est prise en "
        f"compte qu'au prochain point de controle du worker, et wait() ne "
        f"l'attend pas.",
    ))
    out.append((
        "t2_worker_is_out_before_the_reference_is_dropped",
        d.get("t2_worker_still_inside_journal") is False,
        f"a l'instant ou cleanup_resources ferait "
        f"`self._version_fetch_thread = None`, le worker etait encore dans le "
        f"journal (dedans={d.get('t2_worker_still_inside_journal')}, sorti plus "
        f"tard={d.get('t2_worker_finished_later')}). La suite du code de "
        f"fermeture appelle `self._journal.close()` et detruit le dialogue : le "
        f"worker orphelin lit un journal ferme et emet count_ready vers un "
        f"QObject en cours de destruction.",
    ))

    # ===== T3 : garantie acquise ========================================
    out.append((
        "t3_no_result_emitted_after_stop",
        not d.get("t3_emissions_after_stop"),
        f"emissions apres stop() : {d.get('t3_emissions_after_stop')} — un "
        f"resultat qui arrive apres l'annulation repeuplerait la liste "
        f"d'evenements d'un REWIND que l'utilisateur a annule",
    ))
    out.append((
        "t3_read_connection_closed",
        d.get("t3_live_connections") == 0,
        f"{d.get('t3_live_connections')} connexion(s) de lecture encore "
        f"ouverte(s) sur le journal apres annulation : le fichier reste tenu et "
        f"la maintenance (purge, VACUUM) echouera",
    ))

    # ===== T4 : garantie acquise ========================================
    out.append((
        "t4_double_start_submits_one_task",
        d.get("t4_emissions") == 1 and d.get("t4_connections_opened") == 1,
        f"double start(): emissions={d.get('t4_emissions')} attendu=1, "
        f"connexions ouvertes={d.get('t4_connections_opened')} attendu=1. Deux "
        f"clics rapides sur Rechercher ne doivent pas lancer deux lectures "
        f"concurrentes du journal.",
    ))
    out.append((
        "t4_no_connection_leak",
        d.get("t4_live_connections") == 0,
        f"{d.get('t4_live_connections')} connexion(s) encore ouverte(s) apres "
        f"un double start()",
    ))

    # ===== T5 : redemarrage avale =======================================
    out.append((
        "t5_restart_in_flight_is_honoured_or_reported",
        bool(d.get("t5_emissions")),
        f"stop() puis start() pendant que la tache tourne : "
        f"emissions={d.get('t5_emissions')} (running apres restart="
        f"{d.get('t5_running_after_restart')}, _stopped="
        f"{d.get('t5_stopped_flag')}). start() sort en silence parce que "
        f"_running est encore vrai : l'utilisateur voit la barre de progression "
        f"tourner indefiniment, aucun resultat et aucune erreur n'arrive jamais.",
    ))

    # ===== T6 : garantie acquise ========================================
    out.append((
        "t6_stop_before_start_leaves_the_worker_usable",
        d.get("t6_error") == "" and d.get("t6_emissions") == 1,
        f"stop() avant start(): erreur={d.get('t6_error')!r} "
        f"emissions={d.get('t6_emissions')} attendu=1 — un stop() defensif sur "
        f"un worker jamais lance ne doit pas le condamner",
    ))

    # ===== T7 : garantie acquise ========================================
    out.append((
        "t7_stats_on_closed_journal_is_survivable",
        d.get("t7_start_error") == "" and d.get("t7_emissions") == 1
        and not d.get("t7_error_signals"),
        f"stats sur journal ferme : erreur_start={d.get('t7_start_error')!r} "
        f"emissions={d.get('t7_emissions')} signaux_erreur="
        f"{d.get('t7_error_signals')} — le rafraichissement des statistiques se "
        f"declenche par timer et peut tomber juste apres la fermeture d'un "
        f"projet ; il doit rendre un resultat vide, pas une boite d'erreur",
    ))
    out.append((
        "t7_result_is_empty_not_stale",
        d.get("t7_result_is_empty") is True,
        f"le resultat rendu sur un journal ferme n'est pas vide "
        f"(vide={d.get('t7_result_is_empty')}) : la barre d'etat afficherait les "
        f"chiffres du projet precedent",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_life_thread_stop_contract.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"\[{ctx.trace_id}_T2\] VersionFetchThread: submitted to QgsTaskManager",
        name="t2_submission_logged",
        min_count=1,
        max_count=1,
    ))

    # ===== Garde de source ==============================================
    src = (_PLUGIN_ROOT / "qgs_task_support.py").read_text(
        encoding="utf-8", errors="replace")
    wait_body = src[src.find("def wait("):src.find("def _submit_task(")]
    out.append((
        "wait_actually_waits_on_the_task",
        "waitForFinished" in wait_body or "QEventLoop" in wait_body
        or "time" in wait_body or "sleep" in wait_body,
        "TaskEnabledThread.wait() retourne `not self._running` sans jamais "
        "attendre ni pomper la boucle Qt ; le parametre msecs est ignore. Tant "
        "qu'elle ne bloque pas reellement, tous les `stop(); wait(500)` du "
        "dialogue sont des barrieres decoratives",
    ))
    start_body = src[src.find("def start(self):"):src.find("def isRunning(")]
    out.append((
        "start_reports_a_refused_restart",
        "flog" in start_body or "return False" in start_body
        or "raise" in start_body,
        "TaskEnabledThread.start() fait `if self._running: return` sans rien "
        "journaliser ni signaler : un redemarrage refuse est indiscernable d'un "
        "redemarrage accepte, cote appelant comme dans le fichier de log",
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
