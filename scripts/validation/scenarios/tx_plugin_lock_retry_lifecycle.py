"""tx_plugin_lock_retry_lifecycle - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (tant que QGIS est ouvert avec des couches suivies,
RecoverLand capture, ou dit clairement qu'il ne capture pas) et I-OBS
(aucune degradation cachee, aucune promesse fausse).

Le mecanisme
------------
Quand une autre instance vivante de QGIS tient le verrou d'ecriture du
journal d'un projet, `JournalManager.open_for_project` leve
`JournalLockError`. `RecoverPlugin._init_local_backend` l'attrapait avant
de creer le suivi : il ecrivait "Fermez-la pour activer l'enregistrement
ici" (faux : rien ne reessayait), puis `set_no_project()` : point gris
"aucun projet ouvert" sur un projet ouvert dont rien n'est enregistre.
Le correctif attendu : le message de `JournalLockError.user_message()`
(qui nomme l'instance), un etat explicite "enregistrement inactif (journal
verrouille)", et une minuterie (~5 s) qui interroge
`JournalManager.writer_lock_holder` et relance l'initialisation quand le
journal est libre. Cette minuterie est un nouveau risque : si elle survit
a un changement de projet ou a `unload()`, elle rouvre plus tard un
journal que personne n'a demande (celui d'un projet ferme, ou apres le
dechargement du plugin, avec un thread ecrivain orphelin).

Trois defauts de cette minuterie elle-meme (revue v7) :
  - la reprise etait annoncee ("l'enregistrement a repris") des que le
    journal etait ouvert, sans verifier que l'initialisation avait abouti :
    une initialisation qui echoue APRES l'ouverture du journal (aucun
    suivi cree, rien n'est capture) etait annoncee comme une reprise, a
    cote de l'alerte ERREUR ;
  - un journal reseau garde en WAL par un client plus ancien
    (`JournalLockError.reason` = journal reseau garde en WAL) n'a pas de
    verrou vivant : `writer_lock_holder` repond "libre", et
    l'initialisation complete etait relancee toutes les 5 s pour toujours
    (a chaque essai : reprise puis remise du fichier de verrou de l'autre
    poste, connexion au journal WAL vivant sur le partage, ligne CRITICAL) ;
  - la lecture d'un projet refusait deux fois : la minuterie a 0 ms de
    `cleared` (`_reinit_after_clear`) reessayait le journal que
    `readProject` venait de se voir refuser, avec une ligne "blank
    project" fausse.

Le scenario d'echec concret
---------------------------
L'utilisateur ouvre un projet partage deja ouvert sur un autre poste : un
point gris "aucun projet", un message qui promet une reprise qui n'arrive
jamais ; il edite des heures sans historique. Variante due au correctif :
il ouvre un autre projet (ou ferme QGIS) pendant l'attente ; 5 s apres la
fermeture de l'autre instance, le plugin rouvre le journal du projet qu'il
a quitte. Variantes de la revue : l'utilisateur lit "l'enregistrement a
repris" et edite, alors que rien n'est capture ; ou un collegue garde le
journal ouvert avec une ancienne version toute la journee, et QGIS rouvre
le journal reseau 720 fois par heure, 720 lignes CRITICAL.

Ce que le scenario execute reellement
-------------------------------------
De VRAIES secondes instances (processus fils, code produit) tiennent les
journaux de P1, P2, P3, P4 ; un VRAI processus fils garde le journal de PW
ouvert en WAL, comme une ancienne version. VRAI RecoverPlugin sous
FakeIface, messages du panneau RecoverLand et de la barre enregistres.
  A  lecture de P1 verrouille : un seul essai d'ouverture a la lecture ;
     etat affiche, message ; 11 s d'attente verrouillee (aucune
     reinitialisation tant que le verrou est tenu, le refus n'est dit
     qu'une fois) ; liberation : reprise seule en moins de 6,5 s, annoncee
     par le texte "a repris".
  B  lecture de P2 verrouille, puis ouverture de Q (autre projet libre) ;
     liberation de P2 : aucune reinitialisation, le journal reste celui
     de Q.
  D  lecture de P4 verrouille ; `SQLiteAuditBackend` leve ensuite (donc
     APRES l'ouverture du journal) ; liberation : aucune annonce de
     reprise, etat ERREUR affiche et alerte d'initialisation incomplete.
  W  journal de PW : ancien format de verrou (une heure, jamais
     rafraichi), journal garde en WAL par le processus fils ;
     `is_network_path` force a vrai pour CE journal (le partage
     administratif \\\\localhost\\C$ n'est pas joignable ici). Un seul
     essai a la lecture ; 16 s d'attente : aucune reinitialisation, aucun
     refus CRITICAL, aucune reprise du fichier de verrou ; l'ancien client
     ferme : reprise seule en moins de 6,5 s, annoncee, et un commit est
     journalise.
  X  les essais que la surveillance s'autorise quand meme s'espacent,
     constantes reduites sur l'instance (tic 250 ms) pour tenir en
     secondes :
     X1 journal reseau garde en WAL (comme W), fichiers -wal/-shm
        presents : essais a l'aveugle seulement, a intervalles croissants
        (1 s, 2 s, 4 s au lieu de 5 min, 15 min, 30 min) ; puis
        l'ancien client ferme : reprise ;
     X2 une vraie instance tient le verrou, mais la sonde
        `writer_lock_holder` est rendue aveugle (elle repond "libre") : les
        essais refuses s'espacent (1 s, 3 s au lieu de 30 s, 5 min) ;
        sonde rendue, instance fermee : reprise malgre l'espacement.
     Sans espacement, un essai a chaque tic (40 en 10 s ici).
  C  lecture de P3 verrouille, puis `unload()` ; liberation de P3 : aucune
     reinitialisation, aucun journal ouvert, aucun thread ecrivain.

Verdict attendu avant correctif : FAIL (A : etat "no_project", message
faux, pas de reprise). B et C gardent la duree de vie de la minuterie
introduite par le correctif (ils passent avant, faute de minuterie).
Avant le correctif de la revue (code v7 de la ronde precedente) : FAIL
(A : deux essais a la lecture ; D : reprise annoncee ; W : deux essais a
la lecture et une reinitialisation toutes les 5 s ; X1 et X2 : un essai a
chaque tic).
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess  # nosec B404: processus fils local lance par le test
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_plugin_lock_retry_lifecycle"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_HOLDER_CODE = r'''
import os, sys
sys.path.insert(0, {parent!r})
from qgis.core import QgsApplication
app = QgsApplication([], False)
app.initQgis()
from {package}.core.journal_manager import JournalManager
jm = JournalManager()
try:
    jm.open_for_project({project!r}, {profile!r})
    print("READY " + str(os.getpid()), flush=True)
except Exception as exc:
    print("FAILED " + type(exc).__name__ + ": " + str(exc), flush=True)
    sys.exit(3)
sys.stdin.readline()
jm.close()
print("CLOSED", flush=True)
'''

# An older RecoverLand on another machine: its connection keeps the journal
# in WAL mode until it closes; it writes no lock this version can see alive.
_OLD_CLIENT_CODE = r'''
import sqlite3, sys
conn = sqlite3.connect({journal!r})
conn.execute("PRAGMA journal_mode=WAL")
n = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
print("READY " + str(n), flush=True)
sys.stdin.readline()
conn.close()
print("CLOSED", flush=True)
'''

_OLD_HOST = "RL-POSTE-ANCIENNE-VERSION"
_FALSE_PROMISE = "Fermez-la pour activer l'enregistrement ici"
_RESUMED = "a repris"
_HEALTHY = ("PROTECTED", "PERSISTING")


def _same(a, b) -> bool:
    """Same file, whatever the spelling (separators, case, long-path prefix)."""
    if not a or not b:
        return False
    return _flat(a) == _flat(b)


def _flat(path: str) -> str:
    text = str(path).replace("\\", "/")
    if text.startswith("//?/"):
        text = text[4:]
    return text.lower() if os.name == "nt" else text


class _PanelRecorder:
    """What reaches the RecoverLand tab of the QGIS message log panel."""

    def __init__(self, real):
        self.real = real
        self.messages = []

    def logMessage(self, message, tag="", *args, **kwargs):  # noqa: N802
        if tag == "RecoverLand":
            self.messages.append(str(message))
        return self.real.logMessage(message, tag, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.real, name)


class _Holder:
    """Another QGIS instance holding the journal of *project*."""

    def __init__(self, tmp: str, project: str, name: str, code: str = ""):
        child_py = os.path.join(tmp, f"holder_{name}.py")
        Path(child_py).write_text(code or _HOLDER_CODE.format(
            parent=str(_PLUGIN_ROOT.parent), package=_PLUGIN_ROOT.name,
            project=project, profile=os.path.join(tmp, f"profil_{name}")),
            encoding="utf-8")
        self.proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
            [sys.executable, child_py], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.ready = (self.proc.stdout.readline() or "").strip()
        self.pid = self.proc.pid

    def release(self) -> str:
        self.proc.stdin.write("\n")
        self.proc.stdin.flush()
        closed = (self.proc.stdout.readline() or "").strip()
        self.proc.wait(timeout=30)
        return closed

    def kill(self) -> None:
        if self.proc.poll() is None:
            try:
                self.proc.kill()
                self.proc.wait(timeout=10)
            except Exception:  # noqa: BLE001
                pass
        for pipe in (self.proc.stdin, self.proc.stdout):
            try:
                pipe.close()
            except Exception:  # noqa: BLE001 # nosec B110 - already closed
                pass


def _make_gpkg(path: str) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i in range(2):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"C{i:03d}", f"NOM_{i:02d}"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(f)
    mem.commitChanges()
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)


def _write_project(tmp: str, name: str, data: str) -> str:
    from qgis.core import QgsProject, QgsVectorLayer
    folder = os.path.join(tmp, name)
    os.makedirs(folder, exist_ok=True)
    project = os.path.join(folder, f"{name}.qgz")
    QgsProject.instance().clear()
    layer = QgsVectorLayer(f"{data}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    QgsProject.instance().write(project)
    QgsProject.instance().clear()
    return project


def _legacy_wal_journal(journal: str) -> str:
    """The journal an older RecoverLand left: WAL mode, and a lock written
    once, an hour ago, never refreshed (the old format), which the
    heartbeat rule takes for a dead instance."""
    import sqlite3
    from recoverland.core.sqlite_schema import initialize_schema
    os.makedirs(os.path.dirname(journal), exist_ok=True)
    conn = sqlite3.connect(journal)
    try:
        initialize_schema(conn)          # local rules: WAL, like the old client
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    finally:
        conn.close()
    lock = journal + ".rlwriter"
    ts = time.time() - 3600
    with open(lock, "w", encoding="utf-8") as fh:
        fh.write(f"4747|{_OLD_HOST}|{int(ts)}\n")
    os.utime(lock, (ts, ts))
    return mode


def _rows(journal: str, value: str, timeout: float = 4.0) -> int:
    """audit_event rows carrying *value*, read-only, waited for."""
    import sqlite3
    from recoverland.core.journal_manager import sqlite_readonly_uri
    from scripts.validation.plugin_harness import pump
    deadline = time.monotonic() + timeout
    got = -1
    while True:
        if journal and os.path.isfile(journal):
            try:
                conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
                try:
                    got = int(conn.execute(
                        "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                        (f"%{value}%",)).fetchone()[0])
                finally:
                    conn.close()
            except sqlite3.Error:
                pass
        if got >= 1 or time.monotonic() >= deadline:
            return got
        pump(0.1)


def _rename_first(value: str) -> bool:
    from qgis.core import QgsProject
    layer = QgsProject.instance().mapLayersByName("parcelles")[0]
    fid = sorted(f.id() for f in layer.getFeatures())[0]
    layer.startEditing()
    layer.changeAttributeValue(fid, layer.fields().indexFromName("nom"), value)
    return bool(layer.commitChanges())


def _count_calls(plugin, counter: dict) -> None:
    """Count every backend (re)initialisation from now on."""
    original = plugin._init_local_backend

    def counted(*a, **k):
        counter["n"] += 1
        return original(*a, **k)
    plugin._init_local_backend = counted


def _time_calls(plugin, times: list) -> None:
    """Record when every backend (re)initialisation starts from now on."""
    original = plugin._init_local_backend

    def timed(*a, **k):
        times.append(time.monotonic())
        return original(*a, **k)
    plugin._init_local_backend = timed


def _gaps(offsets) -> list:
    return [round(b - a, 2) for a, b in zip(offsets, offsets[1:])]


def _spaced_out(offsets, most: int, last_gap_at_least: float) -> bool:
    """A few attempts, each gap at least the one before (0.5 s of timer
    jitter allowed), the last one long: the attempts space out."""
    gaps = _gaps(offsets)
    return (2 <= len(offsets) <= most and bool(gaps)
            and all(b >= a - 0.5 for a, b in zip(gaps, gaps[1:]))
            and gaps[-1] >= last_gap_at_least)


def _snapshot(plugin) -> dict:
    ind = plugin._status_indicator
    text = ""
    try:
        text = ind._text.text() if ind is not None else ""
    except Exception:  # noqa: BLE001
        pass
    return {
        "journal_open": bool(plugin._journal.is_open),
        "journal": plugin._journal.path if plugin._journal.is_open else None,
        "health": plugin._capture_health().state,
        "reasons": list(plugin._capture_health().reasons),
        "indicator": getattr(ind, "_current_level", None),
        "label": text,
        "tooltip": (getattr(ind, "_current_tooltip", "") or "")[:600],
    }


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.core.journal_manager import _resolve_journal_path

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_plrl_")
    data = os.path.join(tmp, "parcelles.gpkg")
    _make_gpkg(data)
    projects = {name: _write_project(tmp, name, data)
                for name in ("P1", "P2", "P3", "P4", "PW", "PX1", "PX2", "Q")}
    ctx.data["tmp"] = tmp
    ctx.data["projects"] = projects
    ctx.data["journals"] = {name: _resolve_journal_path(p, tmp, "")
                            for name, p in projects.items()}
    ctx.data["w_legacy_mode"] = _legacy_wal_journal(ctx.data["journals"]["PW"])
    ctx.data["x1_legacy_mode"] = _legacy_wal_journal(ctx.data["journals"]["PX1"])
    flog(f"tx_plugin_lock_retry_lifecycle setup: trace_id={ctx.trace_id} tmp={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    import recoverland.core.logger as logger_mod
    from scripts.validation.plugin_harness import FakeIface, pump, writer_threads

    d = ctx.data
    tmp = d["tmp"]
    projects = d["projects"]
    panel = _PanelRecorder(logger_mod.QgsMessageLog)
    logger_mod.QgsMessageLog = panel
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    unloaded = False
    holders = []
    d["writers_before"] = writer_threads()
    try:
        plugin.initGui()
        pump(0.3)
        # ---- A: locked, wait, released -----------------------------------
        h1 = _Holder(tmp, projects["P1"], "a")
        holders.append(h1)
        d["a_holder"] = h1.ready
        d["a_holder_pid"] = h1.pid
        msg0, bar0 = len(panel.messages), len(iface.bar.messages)
        read_calls = {"n": 0}
        _count_calls(plugin, read_calls)
        d["a_read"] = bool(QgsProject.instance().read(projects["P1"]))
        pump(0.5)
        del plugin._init_local_backend
        d["a_read_init_calls"] = read_calls["n"]
        d["a_locked"] = _snapshot(plugin)
        err = getattr(plugin._journal, "last_lock_error", None)
        d["a_expected_message"] = err.user_message() if err is not None and hasattr(
            err, "user_message") else ""
        d["a_messages"] = panel.messages[msg0:]
        d["a_bar"] = [m[1] for m in iface.bar.messages[bar0:]]
        held_calls = {"n": 0}
        _count_calls(plugin, held_calls)
        pump(11.0)
        d["a_init_calls_while_held"] = held_calls["n"]
        d["a_messages_while_held"] = panel.messages[msg0:]
        d["a_bar_while_held"] = [m[1] for m in iface.bar.messages[bar0:]]
        msg1, bar1 = len(panel.messages), len(iface.bar.messages)
        d["a_released"] = h1.release()
        pump(6.5)
        d["a_after_release"] = _snapshot(plugin)
        d["a_messages_after_release"] = panel.messages[msg1:]
        d["a_bar_after_release"] = [m[1] for m in iface.bar.messages[bar1:]]
        del plugin._init_local_backend

        # ---- B: locked, then another project, then released -----------
        h2 = _Holder(tmp, projects["P2"], "b")
        holders.append(h2)
        d["b_holder"] = h2.ready
        d["b_read_locked"] = bool(QgsProject.instance().read(projects["P2"]))
        pump(0.5)
        d["b_locked"] = _snapshot(plugin)
        d["b_read_q"] = bool(QgsProject.instance().read(projects["Q"]))
        pump(0.5)
        d["b_on_q"] = _snapshot(plugin)
        b_calls = {"n": 0}
        _count_calls(plugin, b_calls)
        d["b_released"] = h2.release()
        pump(6.5)
        d["b_init_calls_after_switch"] = b_calls["n"]
        d["b_final"] = _snapshot(plugin)
        del plugin._init_local_backend

        # ---- D: locked, released, the re-init fails after the open ------
        import recoverland.recover as rec
        h4 = _Holder(tmp, projects["P4"], "d")
        holders.append(h4)
        d["d_holder"] = h4.ready
        d["d_read_locked"] = bool(QgsProject.instance().read(projects["P4"]))
        pump(0.5)
        d["d_locked"] = _snapshot(plugin)
        real_backend = rec.SQLiteAuditBackend
        booms = {"n": 0}

        def boom(*_a, **_k):
            booms["n"] += 1
            raise RuntimeError("injected: backend construction fails after "
                               "the journal opened")
        rec.SQLiteAuditBackend = boom
        try:
            msg_d, bar_d = len(panel.messages), len(iface.bar.messages)
            d["d_released"] = h4.release()
            pump(6.5)
        finally:
            rec.SQLiteAuditBackend = real_backend
        d["d_booms"] = booms["n"]
        d["d_after"] = _snapshot(plugin)
        d["d_tracker"] = plugin._tracker is not None
        d["d_messages"] = panel.messages[msg_d:]
        d["d_bar"] = [m[1] for m in iface.bar.messages[bar_d:]]

        # ---- W: network journal kept in WAL by an older client ----------
        import recoverland.core.sqlite_schema as ss
        import recoverland.core.journal_manager as jm_mod
        old = _Holder(tmp, projects["PW"], "w", code=_OLD_CLIENT_CODE.format(
            journal=d["journals"]["PW"]))
        holders.append(old)
        d["w_old_client"] = old.ready
        real_net, real_ss_flog, real_jm_flog = ss.is_network_path, ss.flog, jm_mod.flog
        target = _flat(d["journals"]["PW"])
        seen = []

        def spy(message, level="INFO"):
            seen.append((str(level), str(message)))
            return real_ss_flog(message, level)
        ss.is_network_path = lambda p: _flat(p) == target or real_net(p)
        ss.flog = spy
        jm_mod.flog = spy
        try:
            w_calls = {"n": 0}
            _count_calls(plugin, w_calls)
            msg_w, bar_w, seen0 = len(panel.messages), len(iface.bar.messages), len(seen)
            d["w_read"] = bool(QgsProject.instance().read(projects["PW"]))
            pump(0.5)
            d["w_read_init_calls"] = w_calls["n"]
            d["w_read_refusals"] = sum(1 for lvl, m in seen[seen0:]
                                       if lvl == "CRITICAL" and "cannot leave it" in m)
            err = getattr(plugin._journal, "last_lock_error", None)
            d["w_reason"] = str(getattr(err, "reason", "") or "")
            d["w_expected_message"] = err.user_message() if err is not None and hasattr(
                err, "user_message") else ""
            d["w_locked"] = _snapshot(plugin)
            w_calls["n"] = 0
            seen1 = len(seen)
            pump(16.0)
            window = seen[seen1:]
            d["w_init_calls_while_held"] = w_calls["n"]
            d["w_refusals_while_held"] = sum(
                1 for lvl, m in window if lvl == "CRITICAL" and "cannot leave it" in m)
            d["w_reclaims_while_held"] = sum(
                1 for _lvl, m in window if "stale writer lock reclaimed" in m)
            d["w_log_lines_while_held"] = len(window)
            d["w_lock_file_while_held"] = Path(d["journals"]["PW"] + ".rlwriter").read_text(
                encoding="utf-8", errors="replace")[:80]
            d["w_bar_while_held"] = [m[1] for m in iface.bar.messages[bar_w:]]
            msg_w2, bar_w2 = len(panel.messages), len(iface.bar.messages)
            d["w_released"] = old.release()
            pump(6.5)
            d["w_after_release"] = _snapshot(plugin)
            d["w_messages_after_release"] = panel.messages[msg_w2:]
            d["w_bar_after_release"] = [m[1] for m in iface.bar.messages[bar_w2:]]
            d["w_messages"] = panel.messages[msg_w:msg_w2]
            # Still under the network rules: every connection of the
            # journal must agree on its mode.
            if (d.get("w_after_release") or {}).get("journal_open"):
                d["w_commit"] = _rename_first("TX_PLRL_W_APRES")
                d["w_rows"] = _rows(d["journals"]["PW"], "TX_PLRL_W_APRES")
        finally:
            ss.is_network_path = real_net
            ss.flog = real_ss_flog
            jm_mod.flog = real_jm_flog
            try:
                del plugin._init_local_backend
            except AttributeError:
                pass

        # ---- X1: blind attempts on a WAL-held journal space out ---------
        # Constants reduced on the instance (the class keeps 5 s and
        # 5/15/30 min) so the spacing shows within seconds.
        old_x = _Holder(tmp, projects["PX1"], "x1", code=_OLD_CLIENT_CODE.format(
            journal=d["journals"]["PX1"]))
        holders.append(old_x)
        d["x1_old_client"] = old_x.ready
        target_x = _flat(d["journals"]["PX1"])
        ss.is_network_path = lambda p: _flat(p) == target_x or real_net(p)
        plugin._LOCK_RETRY_MS = 250
        plugin._WAL_BLIND_RETRY_S = (1.0, 2.0, 4.0)
        try:
            x1_times = []
            _time_calls(plugin, x1_times)
            d["x1_read"] = bool(QgsProject.instance().read(projects["PX1"]))
            pump(0.3)
            err = getattr(plugin._journal, "last_lock_error", None)
            d["x1_reason"] = str(getattr(err, "reason", "") or "")
            t0 = time.monotonic()
            del x1_times[:]
            pump(10.0)
            d["x1_attempts"] = [round(t - t0, 2) for t in x1_times]
            d["x1_open_while_held"] = bool(plugin._journal.is_open)
            msg_x, bar_x = len(panel.messages), len(iface.bar.messages)
            d["x1_released"] = old_x.release()
            pump(2.5)
            d["x1_after"] = _snapshot(plugin)
            d["x1_resumed"] = [m for m in panel.messages[msg_x:]
                               + [b[1] for b in iface.bar.messages[bar_x:]]
                               if _RESUMED in m]
        finally:
            ss.is_network_path = real_net
            for attr in ("_init_local_backend", "_LOCK_RETRY_MS", "_WAL_BLIND_RETRY_S"):
                try:
                    delattr(plugin, attr)
                except AttributeError:
                    pass

        # ---- X2: attempts the probe allows but that are refused space out --
        # A real instance holds the lock; the probe is made blind to it (it
        # answers "free"), as it is to any holder it cannot see.
        h_x2 = _Holder(tmp, projects["PX2"], "x2")
        holders.append(h_x2)
        d["x2_holder"] = h_x2.ready
        real_jm_cls = rec.JournalManager

        class _BlindProbe(real_jm_cls):
            @staticmethod
            def writer_lock_holder(journal_path):
                return None
        plugin._LOCK_RETRY_MS = 250
        plugin._LOCK_BACKOFF_S = (1.0, 3.0)
        rec.JournalManager = _BlindProbe
        try:
            x2_times = []
            _time_calls(plugin, x2_times)
            bar_x2 = len(iface.bar.messages)
            d["x2_read"] = bool(QgsProject.instance().read(projects["PX2"]))
            pump(0.3)
            err = getattr(plugin._journal, "last_lock_error", None)
            x2_expected = err.user_message() if err is not None and hasattr(
                err, "user_message") else ""
            t0 = time.monotonic()
            del x2_times[:]
            pump(8.0)
            d["x2_attempts"] = [round(t - t0, 2) for t in x2_times]
            d["x2_open_while_held"] = bool(plugin._journal.is_open)
            d["x2_lock_alerts"] = sum(1 for b in iface.bar.messages[bar_x2:]
                                      if x2_expected and x2_expected in b[1])
            rec.JournalManager = real_jm_cls
            msg_x2, bar_x2b = len(panel.messages), len(iface.bar.messages)
            d["x2_released"] = h_x2.release()
            pump(4.5)
            d["x2_after"] = _snapshot(plugin)
            d["x2_resumed"] = [m for m in panel.messages[msg_x2:]
                               + [b[1] for b in iface.bar.messages[bar_x2b:]]
                               if _RESUMED in m]
        finally:
            rec.JournalManager = real_jm_cls
            for attr in ("_init_local_backend", "_LOCK_RETRY_MS", "_LOCK_BACKOFF_S"):
                try:
                    delattr(plugin, attr)
                except AttributeError:
                    pass

        # ---- C: locked, then unload, then released ---------------------
        h3 = _Holder(tmp, projects["P3"], "c")
        holders.append(h3)
        d["c_holder"] = h3.ready
        d["c_read_locked"] = bool(QgsProject.instance().read(projects["P3"]))
        pump(0.5)
        d["c_locked"] = _snapshot(plugin)
        c_calls = {"n": 0}
        _count_calls(plugin, c_calls)
        plugin.unload()
        unloaded = True
        pump(0.3)
        d["c_released"] = h3.release()
        pump(6.5)
        d["c_init_calls_after_unload"] = c_calls["n"]
        d["c_journal_open_after_unload"] = bool(plugin._journal.is_open)
        d["c_writers_after_unload"] = writer_threads() - d["writers_before"]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        logger_mod.QgsMessageLog = panel.real
        if not unloaded:
            try:
                plugin.unload()
            except Exception as exc:  # noqa: BLE001
                d["unload_error"] = repr(exc)
        for holder in holders:
            holder.kill()
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_plugin_lock_retry_lifecycle run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    crash = f"{d.get('crash', '')} {d.get('trace', '')[-600:]}".strip()
    a = d.get("a_locked") or {}
    out.append((
        "fixture_instances_hold_the_journals",
        not d.get("crash") and all((d.get(k) or "").startswith("READY")
                                   for k in ("a_holder", "b_holder", "c_holder"))
        and a.get("journal_open") is False
        and (d.get("b_locked") or {}).get("journal_open") is False
        and (d.get("c_locked") or {}).get("journal_open") is False,
        f"instances={d.get('a_holder')!r}/{d.get('b_holder')!r}/{d.get('c_holder')!r} "
        f"journal ouvert malgre le verrou A/B/C={a.get('journal_open')}/"
        f"{(d.get('b_locked') or {}).get('journal_open')}/"
        f"{(d.get('c_locked') or {}).get('journal_open')} crash={crash!r}",
    ))
    shown = f"{a.get('label', '')} {a.get('tooltip', '')}"
    out.append((
        "a_locked_state_is_explicit_and_names_the_holder",
        a.get("indicator") not in (None, "no_project", "NO_PROJECT")
        and a.get("health") not in ("NO_PROJECT",) + _HEALTHY
        and "inactif" in shown and "verrouill" in shown
        and str(d.get("a_holder_pid")) in shown,
        f"indicateur={a.get('indicator')!r} etat={a.get('health')!r} "
        f"libelle={a.get('label')!r} infobulle={a.get('tooltip')!r} (instance "
        f"pid={d.get('a_holder_pid')}) : un point gris 'aucun projet' sur un "
        f"projet ouvert dont rien n'est enregistre est une fausse assurance",
    ))
    expected = d.get("a_expected_message") or ""
    told = [m for m in (d.get("a_messages") or []) + (d.get("a_bar") or [])
            if expected and expected in m]
    promised = [m for m in (d.get("a_messages_while_held") or []) if _FALSE_PROMISE in m]
    out.append((
        "a_message_is_the_lock_error_user_message",
        bool(told) and not promised,
        f"message attendu (JournalLockError.user_message)={expected!r} ; "
        f"messages={d.get('a_messages')} barre={d.get('a_bar')} ; promesses "
        f"fausses={promised}",
    ))
    lock_alerts = [m for m in (d.get("a_bar_while_held") or []) if expected and expected in m]
    out.append((
        "a_no_reinit_nor_repeat_while_the_lock_is_held",
        d.get("a_init_calls_while_held") == 0 and len(lock_alerts) == 1,
        f"reinitialisations pendant 11 s de verrou tenu="
        f"{d.get('a_init_calls_while_held')} (attendu 0 : on interroge le "
        f"detenteur, on ne rouvre pas) ; alertes du refus dans la barre="
        f"{len(lock_alerts)} (attendu 1)",
    ))
    out.append((
        "a_project_read_tries_the_locked_journal_once",
        d.get("a_read_init_calls") == 1,
        f"essais d'ouverture pendant la lecture de P1={d.get('a_read_init_calls')} "
        f"(attendu 1 : readProject ; la minuterie a 0 ms de 'cleared' ne doit "
        f"pas redemander un journal que readProject vient de se voir refuser, "
        f"chaque refus en double reprend le fichier de verrou de l'autre "
        f"instance et ecrit 'blank project', ce qui est faux)",
    ))
    after = d.get("a_after_release") or {}
    # Tightened (v7 review): any message used to do; the resumption must be
    # said in words ("a repris"), the claim D below proves is never false.
    told_resume = [m for m in (d.get("a_messages_after_release") or [])
                   + (d.get("a_bar_after_release") or []) if _RESUMED in m]
    out.append((
        "a_capture_resumes_alone_and_is_announced",
        (d.get("a_released") or "") == "CLOSED" and after.get("journal_open") is True
        and _same(after.get("journal"), (d.get("journals") or {}).get("P1"))
        and after.get("health") in _HEALTHY and bool(told_resume),
        f"autre instance={d.get('a_released')!r} ; 6,5 s apres : journal="
        f"{after.get('journal')} (attendu {(d.get('journals') or {}).get('P1')}) "
        f"etat={after.get('health')} annonces '{_RESUMED}'={told_resume[:2]} "
        f"(messages={((d.get('a_messages_after_release') or []) + (d.get('a_bar_after_release') or []))[:3]})",
    ))
    b_q = d.get("b_on_q") or {}
    b_final = d.get("b_final") or {}
    q_journal = (d.get("journals") or {}).get("Q")
    out.append((
        "b_project_switch_stops_the_retry",
        _same(b_q.get("journal"), q_journal) and d.get("b_init_calls_after_switch") == 0
        and _same(b_final.get("journal"), q_journal),
        f"journal apres ouverture de Q={b_q.get('journal')} ; reinitialisations "
        f"apres la liberation du journal de P2={d.get('b_init_calls_after_switch')} "
        f"(attendu 0) ; journal final={b_final.get('journal')} (attendu {q_journal}) : "
        f"la minuterie du projet quitte ne doit rien rouvrir",
    ))
    d_locked = d.get("d_locked") or {}
    d_after = d.get("d_after") or {}
    d_said = (d.get("d_messages") or []) + (d.get("d_bar") or [])
    d_claims = [m for m in d_said if _RESUMED in m]
    d_error_told = [m for m in (d.get("d_bar") or []) if "incomplete" in m]
    out.append((
        "d_fixture_reinit_failed_after_the_journal_opened",
        (d.get("d_holder") or "").startswith("READY")
        and d_locked.get("journal_open") is False and d.get("d_released") == "CLOSED"
        and (d.get("d_booms") or 0) >= 1 and d_after.get("journal_open") is True
        and d.get("d_tracker") is False,
        f"instance={d.get('d_holder')!r} journal ouvert pendant le verrou="
        f"{d_locked.get('journal_open')} liberee={d.get('d_released')!r} echecs "
        f"injectes={d.get('d_booms')} journal ouvert apres={d_after.get('journal_open')} "
        f"suivi cree={d.get('d_tracker')}",
    ))
    out.append((
        "d_failed_resumption_is_never_announced_as_resumed",
        not d_claims and d_after.get("health") == "ERROR"
        and d_after.get("indicator") == "ERROR" and bool(d_error_told),
        f"apres la liberation, initialisation en echec apres l'ouverture du "
        f"journal (aucun suivi, rien n'est capture) : annonces '{_RESUMED}'="
        f"{[m[:160] for m in d_claims]} etat={d_after.get('health')} indicateur="
        f"{d_after.get('indicator')} alerte d'initialisation incomplete="
        f"{[m[:120] for m in d_error_told]} : l'utilisateur qui lit que "
        f"l'enregistrement a repris edite sans historique",
    ))
    w_locked = d.get("w_locked") or {}
    out.append((
        "w_fixture_older_client_keeps_the_network_journal_in_wal",
        d.get("w_legacy_mode") == "wal"
        and (d.get("w_old_client") or "").startswith("READY")
        and "WAL" in (d.get("w_reason") or "") and w_locked.get("journal_open") is False
        and w_locked.get("health") == "LOCKED",
        f"mode du journal ancien={d.get('w_legacy_mode')} ancien client="
        f"{d.get('w_old_client')!r} motif du refus={d.get('w_reason')!r} journal "
        f"ouvert={w_locked.get('journal_open')} etat={w_locked.get('health')}",
    ))
    out.append((
        "w_project_read_tries_the_journal_once",
        d.get("w_read_init_calls") == 1 and d.get("w_read_refusals") == 1,
        f"essais d'ouverture pendant la lecture de PW={d.get('w_read_init_calls')} "
        f"refus CRITICAL 'cannot leave it'={d.get('w_read_refusals')} (attendu 1 "
        f"et 1) : chaque essai de trop reprend le verrou de l'autre poste et "
        f"rouvre un journal WAL vivant sur le partage",
    ))
    w_told = [m for m in (d.get("w_bar_while_held") or [])
              if d.get("w_expected_message") and d.get("w_expected_message") in m]
    out.append((
        "w_no_reinit_while_the_older_client_holds_the_journal",
        d.get("w_init_calls_while_held") == 0 and d.get("w_refusals_while_held") == 0
        and d.get("w_reclaims_while_held") == 0 and len(w_told) == 1
        and _OLD_HOST in (d.get("w_lock_file_while_held") or ""),
        f"pendant 16 s, l'ancien client gardant le journal en WAL : "
        f"reinitialisations={d.get('w_init_calls_while_held')} refus CRITICAL="
        f"{d.get('w_refusals_while_held')} reprises du fichier de verrou="
        f"{d.get('w_reclaims_while_held')} lignes de log={d.get('w_log_lines_while_held')} "
        f"(attendu 0, 0, 0 : writer_lock_holder dit 'libre' faute de verrou vivant, "
        f"ce n'est pas une raison de rouvrir toutes les 5 s) ; alertes du refus="
        f"{len(w_told)} (attendu 1) ; fichier de verrou={d.get('w_lock_file_while_held')!r}",
    ))
    w_after = d.get("w_after_release") or {}
    w_resumed = [m for m in (d.get("w_messages_after_release") or [])
                 + (d.get("w_bar_after_release") or []) if _RESUMED in m]
    out.append((
        "w_capture_resumes_when_the_older_client_closes",
        d.get("w_released") == "CLOSED" and w_after.get("journal_open") is True
        and _same(w_after.get("journal"), (d.get("journals") or {}).get("PW"))
        and w_after.get("health") in _HEALTHY and bool(w_resumed)
        and d.get("w_commit") is True and (d.get("w_rows") or 0) >= 1,
        f"ancien client={d.get('w_released')!r} ; 6,5 s apres : journal="
        f"{w_after.get('journal')} etat={w_after.get('health')} annonces="
        f"{[m[:120] for m in w_resumed]} commit={d.get('w_commit')} lignes="
        f"{d.get('w_rows')} : sans essai a l'aveugle, la reprise doit venir de "
        f"la fermeture du journal par l'autre poste",
    ))
    x1_attempts = d.get("x1_attempts") or []
    x1_after = d.get("x1_after") or {}
    out.append((
        "x1_blind_attempts_on_a_wal_held_journal_space_out",
        d.get("x1_legacy_mode") == "wal" and "WAL" in (d.get("x1_reason") or "")
        and d.get("x1_open_while_held") is False
        and _spaced_out(x1_attempts, most=5, last_gap_at_least=3.5),
        f"journal garde en WAL par l'ancien client (motif={d.get('x1_reason')!r}), "
        f"tic 250 ms, intervalles 1/2/4 s : essais en 10 s aux instants "
        f"{x1_attempts} (ecarts {_gaps(x1_attempts)}) ; attendu 2 a 5 essais "
        f"d'ecarts croissants jusqu'a 4 s, pas un essai a chaque tic (40 en 10 s) ; "
        f"en vrai 5 min, 15 min, 30 min",
    ))
    out.append((
        "x1_capture_resumes_once_the_older_client_closes",
        d.get("x1_released") == "CLOSED" and x1_after.get("journal_open") is True
        and _same(x1_after.get("journal"), (d.get("journals") or {}).get("PX1"))
        and x1_after.get("health") in _HEALTHY and bool(d.get("x1_resumed")),
        f"ancien client={d.get('x1_released')!r} ; 2,5 s apres : journal="
        f"{x1_after.get('journal')} etat={x1_after.get('health')} annonces="
        f"{[m[:100] for m in d.get('x1_resumed') or []]} : l'espacement des "
        f"essais a l'aveugle ne doit pas retarder la reprise quand les fichiers "
        f"-wal/-shm disparaissent",
    ))
    x2_attempts = d.get("x2_attempts") or []
    x2_after = d.get("x2_after") or {}
    out.append((
        "x2_refused_attempts_the_probe_allowed_space_out",
        (d.get("x2_holder") or "").startswith("READY")
        and d.get("x2_open_while_held") is False
        and _spaced_out(x2_attempts, most=5, last_gap_at_least=2.5)
        and d.get("x2_lock_alerts") == 1,
        f"sonde aveugle ('libre') devant une instance vivante, tic 250 ms, "
        f"espacement 1/3 s : essais en 8 s aux instants {x2_attempts} (ecarts "
        f"{_gaps(x2_attempts)}) ; attendu 2 a 5 essais d'ecarts croissants "
        f"jusqu'a 3 s, pas un essai a chaque tic (32 en 8 s) ; en vrai 30 s puis "
        f"5 min ; alertes du refus={d.get('x2_lock_alerts')} (attendu 1)",
    ))
    out.append((
        "x2_capture_resumes_once_the_holder_releases",
        d.get("x2_released") == "CLOSED" and x2_after.get("journal_open") is True
        and _same(x2_after.get("journal"), (d.get("journals") or {}).get("PX2"))
        and x2_after.get("health") in _HEALTHY and bool(d.get("x2_resumed")),
        f"instance={d.get('x2_released')!r} ; 4,5 s apres : journal="
        f"{x2_after.get('journal')} etat={x2_after.get('health')} annonces="
        f"{[m[:100] for m in d.get('x2_resumed') or []]}",
    ))
    out.append((
        "c_unload_stops_the_retry",
        d.get("c_init_calls_after_unload") == 0
        and d.get("c_journal_open_after_unload") is False
        and d.get("c_writers_after_unload") == 0 and not d.get("unload_error"),
        f"reinitialisations apres unload={d.get('c_init_calls_after_unload')} "
        f"journal ouvert={d.get('c_journal_open_after_unload')} threads "
        f"ecrivains={d.get('c_writers_after_unload')} erreur={d.get('unload_error')} : "
        f"un plugin decharge ne doit plus rien ouvrir",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_plugin_lock_retry_lifecycle.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
