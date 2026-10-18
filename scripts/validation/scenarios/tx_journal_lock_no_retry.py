"""tx_journal_lock_no_retry - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (tant que QGIS est ouvert avec des couches suivies,
RecoverLand capture, ou dit clairement qu'il ne capture pas) et I-OBS
(aucune degradation cachee).

Le mecanisme
------------
A l'ouverture d'un projet dont le journal est tenu par une autre instance
de QGIS, `JournalManager.open_for_project` leve `JournalLockError`.
`RecoverPlugin._init_local_backend` l'attrape AVANT d'avoir cree le
tracker : la capture est coupee pour toute la session, rien ne reessaie,
l'indicateur affiche "aucun projet ouvert", et le message dit :

    "Une autre instance de QGIS enregistre deja dans ce journal.
     Fermez-la pour activer l'enregistrement ici."

ce qui est faux : fermer l'autre instance ne reactive rien.

Le scenario d'echec concret
---------------------------
L'utilisateur lit le message, ferme l'autre QGIS comme demande, puis edite
pendant des heures. Rien n'est capture ; il ne s'en apercoit qu'au moment
de restaurer.

Ce que le scenario execute reellement
-------------------------------------
Une VRAIE seconde instance (processus fils, code produit) tient le journal
de P.qgz. Le VRAI RecoverPlugin ouvre P.qgz (faux iface, messages QGIS
captes). La seconde instance se ferme. Puis, dans cet ordre :
  1. 6 s d'attente sans aucune action, un commit : capture ou etat
     "inactif" explicite ?
  2. l'utilisateur enregistre le projet (Ctrl+S), un commit : capture ?

Verdict attendu avant correctif : FAIL.
Apres le correctif de journal_manager seul : les refus nomment l'instance
qui tient le journal et un enregistrement relance la capture ; la reprise
SANS action de l'utilisateur et l'etat affiche pendant le verrou relevent
de `RecoverPlugin._init_local_backend` (recover.py), hors de ce perimetre :
ces deux assertions restent rouges tant que ce code n'est pas modifie.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import subprocess  # nosec B404: processus fils local lance par le test
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_lock_no_retry"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_CHILD_CODE = r'''
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

_FALSE_PROMISE = "Fermez-la pour activer l'enregistrement ici"


class _PanelRecorder:
    """What reaches the RecoverLand tab of the QGIS message log panel.

    ``QgsMessageLog.messageReceived`` is not emitted in this headless
    runtime (measured), so the scenario records at the call every
    user-facing ``qlog`` goes through, and forwards it unchanged.
    """

    def __init__(self, real):
        self.real = real
        self.messages = []

    def logMessage(self, message, tag="", *args, **kwargs):  # noqa: N802
        if tag == "RecoverLand":
            self.messages.append(str(message))
        return self.real.logMessage(message, tag, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.real, name)


def _make_gpkg(path: str) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i in range(4):
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


def _rename(index: int, value: str) -> bool:
    from qgis.core import QgsProject
    layers = QgsProject.instance().mapLayersByName("parcelles")
    if not layers:
        return False
    layer = layers[0]
    idx = layer.fields().indexFromName("nom")
    target = None
    for feat in layer.getFeatures():
        if feat["code"] == f"C{index:03d}":
            target = feat.id()
    if target is None:
        return False
    layer.startEditing()
    layer.changeAttributeValue(target, idx, value)
    return bool(layer.commitChanges())


def _count(journal: str, value: str) -> int:
    if not journal or not os.path.isfile(journal):
        return -1
    from recoverland.core.journal_manager import sqlite_readonly_uri
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                (f"%{value}%",)).fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _wait_rows(journal: str, value: str, timeout: float) -> int:
    from scripts.validation.plugin_harness import pump
    deadline = time.monotonic() + timeout
    got = _count(journal, value)
    while got < 1 and time.monotonic() < deadline:
        pump(0.1)
        got = _count(journal, value)
    return got


def setup(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.logger import flog
    from recoverland.core.journal_manager import _resolve_journal_path

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jlnr_")
    data = os.path.join(tmp, "parcelles.gpkg")
    _make_gpkg(data)
    pdir = os.path.join(tmp, "projet")
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "P.qgz")
    layer = QgsVectorLayer(f"{data}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    ctx.data["fixture_write"] = bool(QgsProject.instance().write(project))
    QgsProject.instance().clear()
    ctx.data["tmp"] = tmp
    ctx.data["project"] = project
    ctx.data["journal"] = _resolve_journal_path(project, tmp, "")
    flog(f"tx_journal_lock_no_retry setup: trace_id={ctx.trace_id} "
         f"project={project}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    tmp = d["tmp"]
    child_py = os.path.join(tmp, "second_instance_child.py")
    Path(child_py).write_text(_CHILD_CODE.format(
        parent=str(_PLUGIN_ROOT.parent), package=_PLUGIN_ROOT.name,
        project=d["project"], profile=tmp), encoding="utf-8")
    proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
        [sys.executable, child_py], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    import recoverland.core.logger as logger_mod

    panel = _PanelRecorder(logger_mod.QgsMessageLog)
    logger_mod.QgsMessageLog = panel
    messages = panel.messages
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    try:
        d["child_ready"] = (proc.stdout.readline() or "").strip()
        d["child_pid"] = proc.pid
        plugin.initGui()
        pump(0.3)
        d["read"] = bool(QgsProject.instance().read(d["project"]))
        pump(0.5)
        d["open_while_locked"] = bool(plugin._journal.is_open)
        ind = plugin._status_indicator
        d["status_while_locked"] = getattr(ind, "_current_level", None)
        d["tooltip_while_locked"] = (getattr(ind, "_current_tooltip", "") or "")[:300]
        d["messages_while_locked"] = list(messages)
        d["bar_while_locked"] = iface.bar.texts()[-4:]
        # The other instance goes away, as the message told the user to do.
        proc.stdin.write("\n")
        proc.stdin.flush()
        d["child_closed"] = (proc.stdout.readline() or "").strip()
        proc.wait(timeout=30)
        pump(6.0)
        d["commit_no_action"] = _rename(0, "APRES_LIBERATION")
        d["rows_no_action"] = _wait_rows(d["journal"], "APRES_LIBERATION", 3.0)
        d["status_after_release"] = getattr(ind, "_current_level", None)
        # The user saves the project (Ctrl+S).
        d["save"] = bool(QgsProject.instance().write())
        pump(0.5)
        d["commit_after_save"] = _rename(1, "APRES_SAUVEGARDE")
        d["rows_after_save"] = _wait_rows(d["journal"], "APRES_SAUVEGARDE", 6.0)
        d["messages_all"] = messages[-12:]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        logger_mod.QgsMessageLog = panel.real
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        try:
            proc.kill()
            proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            pass
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_journal_lock_no_retry run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    crash = f"{d.get('crash', '')} {d.get('trace', '')[-500:]}".strip()
    out.append((
        "fixture_seconde_instance_tient_le_journal",
        d.get("fixture_write") is True
        and (d.get("child_ready") or "").startswith("READY")
        and d.get("read") is True and d.get("open_while_locked") is False
        and not d.get("crash"),
        f"projet ecrit={d.get('fixture_write')} seconde instance="
        f"{d.get('child_ready')!r} lu={d.get('read')} journal ouvert malgre le "
        f"verrou={d.get('open_while_locked')} crash={crash!r}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"journal locked by another instance: .*host={socket.gethostname()}"
        rf".*pid={d.get('child_pid')}",
        name="refus_nomme_l_instance_qui_tient_le_journal",
        min_count=1,
    ))
    out.append((
        "enregistrer_le_projet_relance_la_capture",
        d.get("save") is True and d.get("commit_after_save") is True
        and (d.get("rows_after_save") or 0) >= 1,
        f"apres fermeture de l'autre instance puis Ctrl+S : commit="
        f"{d.get('commit_after_save')} lignes journalisees="
        f"{d.get('rows_after_save')} : l'utilisateur qui enregistre son "
        f"projet doit retrouver la capture",
    ))
    resumed = (d.get("rows_no_action") or 0) >= 1
    explicit = d.get("status_after_release") not in (None, "no_project")
    out.append((
        "capture_reprise_seule_ou_etat_inactif_explicite",
        d.get("commit_no_action") is True and (resumed or explicit),
        f"6 s apres la fermeture de l'autre instance, sans action : lignes="
        f"{d.get('rows_no_action')} etat affiche="
        f"{d.get('status_after_release')!r} (pendant le verrou="
        f"{d.get('status_while_locked')!r}). L'utilisateur edite des heures "
        f"sans capture derriere un point gris 'aucun projet'. [perimetre "
        f"recover.py : _init_local_backend doit reessayer ou afficher "
        f"'enregistrement inactif (journal verrouille)']",
    ))
    promised = [m for m in (d.get("messages_while_locked") or [])
                if _FALSE_PROMISE in m]
    out.append((
        "message_ne_promet_pas_une_reprise_qui_n_arrive_pas",
        resumed or not promised,
        f"messages pendant le verrou={d.get('messages_while_locked')} ; "
        f"reprise sans action={resumed}. Promettre que fermer l'autre instance "
        f"active l'enregistrement, sans reessayer, est une fausse assurance. "
        f"[perimetre recover.py]",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_lock_no_retry.*trace_id={ctx.trace_id}",
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
