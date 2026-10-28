# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_lock_lost_carry_fallback - RecoverLand validation runtime.

Invariant: I-OBS-01 (l'etat affiche vient des faits d'execution) et
I-CAPTURE-02 (l'utilisateur sait si ses modifications sont protegees).

Le mecanisme
------------
Deux faits connus de `JournalManager` n'arrivaient ni a l'etat de capture
(`core/capture_health.py`) ni a la barre d'etat :

L  `is_lock_lost` : le battement de coeur du verrou d'ecriture constate
   qu'une AUTRE instance a pris le verrou du journal (elle a juge le notre
   perime, ou un client plus ancien l'a ecrase). Deux instances peuvent
   alors ecrire le meme journal : l'historique n'est plus protege. Seul un
   log CRITICAL le disait ; l'indicateur restait vert "protege".
C  `last_carry_report.fell_back` : a l'enregistrement du projet, le journal
   du projet n'a pas pu recevoir l'historique de la session (tenu par une
   autre instance, non inscriptible) ; la capture continue dans le journal
   de session jusqu'au prochain enregistrement. Une alerte le dit au moment
   de l'enregistrement, puis l'indicateur repasse au vert "protege" : rien
   ne rappelle que le projet ne lit pas encore cet historique.

Le scenario d'echec concret
---------------------------
L : deux postes editent le meme projet partage ; celui dont le verrou a ete
pris continue d'afficher "protege" pendant que les deux ecrivent le journal.
C : l'utilisateur enregistre, ferme l'alerte, travaille l'apres-midi sous
un point vert ; s'il ferme QGIS sans reenregistrer, le projet rouvert ne
montre pas son travail.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin sous FakeIface.
  L  projet enregistre ouvert (battement du verrou ramene a 0,3 s pour la
     duree du cas, `journal_manager._LOCK_HEARTBEAT_SEC`, restaure ensuite) ;
     le fichier de verrou est reecrit au nom d'un autre poste, comme le fait
     une instance qui reprend un verrou ; 6 s (un tic du chien de garde).
  C  une VRAIE seconde instance (processus fils) tient le journal de PC ;
     projet vierge, un commit, "Enregistrer sous" PC : repli sur le journal
     de session ; un commit ; l'autre instance se ferme ; Ctrl+S.

Exige : L -> etat ERREUR nommant la prise de verrou, alerte une fois ;
C -> etat DEGRADE nommant le journal du projet et la reprise au prochain
enregistrement, la capture continue, puis retour a l'etat sain apres un
enregistrement reussi.

Verdict attendu avant correctif : FAIL (L et C : etat sain affiche).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess  # nosec B404: processus fils local lance par le test
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_plugin_lock_lost_carry_fallback"
INVARIANT = "I-OBS-01"

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


def _make_gpkg(path: str) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i in range(3):
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


def _layer(data: str):
    from qgis.core import QgsProject, QgsVectorLayer
    layers = QgsProject.instance().mapLayersByName("parcelles")
    if layers:
        return layers[0]
    layer = QgsVectorLayer(f"{data}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    return layer


def _rename(layer, index: int, value: str) -> bool:
    idx = layer.fields().indexFromName("nom")
    target = [f.id() for f in layer.getFeatures() if f["code"] == f"C{index:03d}"]
    if not target:
        return False
    layer.startEditing()
    layer.changeAttributeValue(target[0], idx, value)
    return bool(layer.commitChanges())


def _rows(journal, value: str, timeout: float = 5.0) -> int:
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


def _snapshot(plugin) -> dict:
    ind = plugin._status_indicator
    health = plugin._capture_health()
    report = getattr(plugin._journal, "last_carry_report", None)
    return {
        "journal": plugin._journal.path if plugin._journal.is_open else None,
        "health": health.state,
        "reasons": list(health.reasons),
        "indicator": getattr(ind, "_current_level", None),
        "tooltip": (getattr(ind, "_current_tooltip", "") or "")[:800],
        "lock_lost": bool(getattr(plugin._journal, "is_lock_lost", False)),
        "fell_back": bool(getattr(report, "fell_back", False)),
    }


def setup(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.logger import flog
    from recoverland.core.journal_manager import _resolve_journal_path

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_pllc_")
    data = os.path.join(tmp, "parcelles.gpkg")
    _make_gpkg(data)
    # L: a saved project, so its journal and lock file are known.
    pl_dir = os.path.join(tmp, "PL")
    os.makedirs(pl_dir, exist_ok=True)
    pl = os.path.join(pl_dir, "PL.qgz")
    layer = QgsVectorLayer(f"{data}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(layer)
    ctx.data["pl_written"] = bool(QgsProject.instance().write(pl))
    QgsProject.instance().clear()
    # C: the project the other instance has open (a first version exists).
    pc_dir = os.path.join(tmp, "PC")
    os.makedirs(pc_dir, exist_ok=True)
    pc = os.path.join(pc_dir, "PC.qgz")
    Path(pc).write_text("<qgis/>", encoding="utf-8")
    ctx.data.update(tmp=tmp, data=data, pl=pl, pc=pc,
                    pc_journal=_resolve_journal_path(pc, tmp, ""))
    flog(f"tx_plugin_lock_lost_carry_fallback setup: trace_id={ctx.trace_id} tmp={tmp}",
         "INFO")


def _part_lock_lost(plugin, iface, d: dict) -> None:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump
    import recoverland.core.journal_manager as jm_mod

    d["l_read"] = bool(QgsProject.instance().read(d["pl"]))
    pump(0.8)
    d["l_before"] = _snapshot(plugin)
    lock_path = (plugin._journal.path or "") + jm_mod._LOCK_SUFFIX
    d["l_lock_file"] = os.path.isfile(lock_path)
    bar0 = len(iface.bar.messages)
    # Another instance takes the journal over: it writes its own name in
    # the lock file, as _acquire_lock does after judging ours stale.
    with open(lock_path, "r+b") as fh:
        old = fh.read()
        new = f"4242|AUTRE-POSTE|{int(time.time())}".encode("utf-8")
        new = new + b" " * max(0, len(old) - len(new) - 1) + b"\n"
        fh.seek(0)
        fh.write(new)
        fh.flush()
    pump(6.0)
    d["l_after"] = _snapshot(plugin)
    d["l_bar"] = [m[1] for m in iface.bar.messages[bar0:]]
    d["l_bar_levels"] = [str(m[2]) for m in iface.bar.messages[bar0:]]


def _part_carry_fallback(plugin, iface, d: dict) -> None:
    from qgis.core import QgsProject
    from scripts.validation.plugin_harness import pump

    tmp = d["tmp"]
    child_py = os.path.join(tmp, "holder_pc.py")
    Path(child_py).write_text(_HOLDER_CODE.format(
        parent=str(_PLUGIN_ROOT.parent), package=_PLUGIN_ROOT.name,
        project=d["pc"], profile=os.path.join(tmp, "profil_autre")),
        encoding="utf-8")
    proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
        [sys.executable, child_py], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        d["c_holder"] = (proc.stdout.readline() or "").strip()
        QgsProject.instance().clear()
        pump(0.6)
        layer = _layer(d["data"])
        pump(0.2)
        d["c_session_journal"] = plugin._journal.path if plugin._journal.is_open else None
        d["c_commit"] = _rename(layer, 0, "C_AVANT_SAUVEGARDE")
        d["c_captured"] = _rows(d["c_session_journal"], "C_AVANT_SAUVEGARDE")
        bar0 = len(iface.bar.messages)
        d["c_write"] = bool(QgsProject.instance().write(d["pc"]))
        pump(1.5)
        d["c_fallback"] = _snapshot(plugin)
        d["c_bar_at_save"] = [m[1] for m in iface.bar.messages[bar0:]]
        d["c_commit_during"] = _rename(_layer(d["data"]), 1, "C_PENDANT_REPLI")
        d["c_rows_during"] = _rows(d["c_fallback"].get("journal"), "C_PENDANT_REPLI")
        pump(0.3)
        d["c_fallback_later"] = _snapshot(plugin)
        proc.stdin.write("\n")
        proc.stdin.flush()
        d["c_holder_closed"] = (proc.stdout.readline() or "").strip()
        proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        for pipe in (proc.stdin, proc.stdout):
            try:
                pipe.close()
            except Exception:  # noqa: BLE001 # nosec B110 - already closed
                pass
    d["c_write_again"] = bool(QgsProject.instance().write())
    pump(2.0)
    d["c_after_save"] = _snapshot(plugin)


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    import recoverland.core.journal_manager as jm_mod
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    saved_heartbeat = jm_mod._LOCK_HEARTBEAT_SEC
    jm_mod._LOCK_HEARTBEAT_SEC = 0.3
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    try:
        plugin.initGui()
        pump(0.3)
        _part_lock_lost(plugin, iface, d)
        jm_mod._LOCK_HEARTBEAT_SEC = saved_heartbeat
        _part_carry_fallback(plugin, iface, d)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        jm_mod._LOCK_HEARTBEAT_SEC = saved_heartbeat
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_plugin_lock_lost_carry_fallback run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")
    shutil.rmtree(d["tmp"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    crash = f"{d.get('crash', '')} {d.get('trace', '')[-600:]}".strip()
    before = d.get("l_before") or {}
    after = d.get("l_after") or {}
    out.append((
        "l_fixture_the_lock_is_really_taken_over",
        not d.get("crash") and d.get("pl_written") is True and d.get("l_read") is True
        and before.get("health") in _HEALTHY and d.get("l_lock_file") is True
        and after.get("lock_lost") is True,
        f"projet ecrit={d.get('pl_written')} lu={d.get('l_read')} etat avant="
        f"{before.get('health')} fichier de verrou={d.get('l_lock_file')} "
        f"is_lock_lost apres reprise={after.get('lock_lost')} crash={crash!r}",
    ))
    reasons = " ".join(after.get("reasons") or [])
    out.append((
        "l_lock_taken_over_is_error_and_named",
        after.get("health") == "ERROR" and after.get("indicator") == "ERROR"
        and "autre instance" in reasons,
        f"etat={after.get('health')} indicateur={after.get('indicator')} "
        f"raisons={after.get('reasons')} : deux instances ecrivent ce journal, "
        f"l'indicateur ne peut pas dire 'protege'",
    ))
    lock_alerts = [t for t in (d.get("l_bar") or []) if "autre instance" in t]
    out.append((
        "l_user_is_told_once",
        len(lock_alerts) == 1,
        f"alertes de la barre sur la reprise du verrou={lock_alerts} "
        f"(toutes={d.get('l_bar')}) : attendu une, pas une par tic",
    ))
    fb = d.get("c_fallback") or {}
    later = d.get("c_fallback_later") or {}
    out.append((
        "c_fixture_save_fell_back_to_the_session_journal",
        (d.get("c_holder") or "").startswith("READY") and d.get("c_commit") is True
        and (d.get("c_captured") or 0) >= 1 and d.get("c_write") is True
        and _same(fb.get("journal"), d.get("c_session_journal"))
        and fb.get("fell_back") is True,
        f"autre instance={d.get('c_holder')!r} commit={d.get('c_commit')} capture="
        f"{d.get('c_captured')} enregistrement={d.get('c_write')} journal apres="
        f"{fb.get('journal')} (session={d.get('c_session_journal')}) repli="
        f"{fb.get('fell_back')} alertes={d.get('c_bar_at_save')}",
    ))
    fb_reasons = " ".join(later.get("reasons") or [])
    out.append((
        "c_fallback_is_degraded_and_named_until_the_next_save",
        fb.get("health") == "DEGRADED" and later.get("health") == "DEGRADED"
        and later.get("indicator") == "DEGRADED"
        and _flat(d.get("pc_journal") or "?") in _flat(fb_reasons)
        and "enregistrement" in fb_reasons,
        f"etat au repli={fb.get('health')} puis={later.get('health')} indicateur="
        f"{later.get('indicator')} raisons={later.get('reasons')} : le projet "
        f"enregistre ne lit pas encore cet historique ; un point vert le cache",
    ))
    out.append((
        "c_capture_goes_on_during_the_fallback",
        d.get("c_commit_during") is True and (d.get("c_rows_during") or 0) >= 1,
        f"commit pendant le repli={d.get('c_commit_during')} lignes="
        f"{d.get('c_rows_during')}",
    ))
    ok_after = d.get("c_after_save") or {}
    out.append((
        "c_back_to_healthy_after_a_successful_save",
        (d.get("c_holder_closed") or "") == "CLOSED" and d.get("c_write_again") is True
        and _same(ok_after.get("journal"), d.get("pc_journal"))
        and ok_after.get("health") in _HEALTHY,
        f"autre instance fermee={d.get('c_holder_closed')!r} Ctrl+S="
        f"{d.get('c_write_again')} journal={ok_after.get('journal')} (attendu "
        f"{d.get('pc_journal')}) etat={ok_after.get('health')} raisons="
        f"{ok_after.get('reasons')}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_plugin_lock_lost_carry_fallback.*trace_id={ctx.trace_id}",
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
