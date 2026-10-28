# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_slot_exception - RecoverLand validation runtime.

Invariant: I-THREAD-01 (aucune exception ne s'echappe d'un slot Qt du
plugin) et I-CAPTURE-02 (tant que RecoverLand ne dit rien, il capture ;
sinon l'utilisateur est prevenu et l'indicateur le montre).

Le mecanisme
------------
`RecoverPlugin` (recover.py) branche ses methodes sur des signaux Qt :
`QgsProject.layersAdded / layersRemoved / readProject / cleared /
projectSaved`, les minuteries (`_reinit_after_clear`, `_watchdog_tick`, la
fermeture `_check` de la surveillance disque, le relais differe vers la
fenetre apres un commit), les signaux de l'indicateur de la barre d'etat
(clic : activer/desactiver, clic droit : ouvrir), et les rappels du suivi
(`_on_events_committed`, `_on_capture_status`). Une exception qui sort d'un
de ces slots :
  - sans gestionnaire global (PyQt6) fait avorter QGIS (0xC0000409),
    avec les editions non enregistrees de toutes les couches ;
  - avec celui de QGIS, arrete le slot a mi-chemin : backend ferme et pas
    rouvert, couche jamais suivie, minuterie morte... et rien ne le dit,
    l'indicateur reste vert ou gris "aucun projet".
Deux cas voisins : la fermeture `_check` appelee pendant
`_init_local_backend` faisait ecrire "L'enregistrement est desactive pour
cette session" alors que la capture tournait ; une exception d'affichage
dans `_on_events_committed` (rappel appele depuis un slot garde du suivi)
etait comptee comme un echec de CAPTURE alors que les evenements etaient
deja durables (fausse alerte, indicateur ERREUR a tort). Et le statut
"slot_failed" du suivi (`core/edit_tracker._guarded`) n'avait aucune
branche dans `_on_capture_status` : aucune alerte.

Le scenario d'echec concret
---------------------------
L'utilisateur ajoute une couche au moment ou le suivi rencontre une
erreur imprevue : la couche n'est jamais suivie, l'indicateur reste vert,
il edite une journee entiere sans historique. Ou QGIS disparait au milieu
d'une ouverture de projet.

Ce que le scenario execute reellement
-------------------------------------
Chaque cas tourne dans un processus QGIS enfant (crash_harness.run_child),
avec le VRAI RecoverPlugin sous plugin_harness.FakeIface, un gestionnaire
d'exception global qui note ce qui atteint Qt (comme celui de QGIS, sans
avorter), et UNE exception injectee dans une dependance appelee par le
slot vise, pendant un vrai evenement Qt :
  K1  layersAdded       tracker.connect_layer leve (ajout d'une couche)
  K2  layersRemoved     tracker.disconnect_layer_by_id leve
  K3  readProject       remove_project_log_handler leve au 2e appel
                        (celui de _on_project_opened ; cleared passe)
  K4  cleared           write_queue.stop leve (Fichier > Nouveau)
  K5  projectSaved      journal.plan_history_carry leve (Enregistrer sous)
  K6  _reinit_after_clear  _notify_dialog_project_switched leve au 2e appel
  K7  _watchdog_tick    l'indicateur detruit (RuntimeError "wrapped C/C++
                        object ... has been deleted") a chaque tic ; la
                        minuterie est ramenee a 100 ms pour la duree du cas
  K8  disque (_check)   check_disk_for_path leve, a l'init puis au tic
                        (minuterie ramenee a 100 ms)
  K9  indicateur clic   tracker.deactivate leve (toggle_requested)
  K10 indicateur clic droit  RecoverDialog leve (open_dialog_requested)
  K11 relais differe    commit, puis unload() avant les 500 ms du relais
                        vers la fenetre (self.dlg vaut alors None)
  K12 _on_events_committed  indicator.pulse leve apres un commit capture
  K13 _watchdog_tick    write_queue.watchdog_check leve a chaque tic
                        (minuterie ramenee a 100 ms) : la verification de
                        vie du thread d'ecriture elle-meme, que le tic
                        avalait dans un try/except local (une ligne ERROR,
                        rien d'autre, et le tic comptait meme comme une
                        reprise)
  K14 etat du thread    write_queue.health leve a chaque appel : lu sans
                        aucun tic d'abord (_capture_health concluait
                        PROTECTED avec writer=None), puis sous les tics
  T1  suivi             tracker._on_before_commit leve (statut slot_failed)

Exige, pour chaque slot : l'exception n'atteint pas Qt (code de sortie 0,
gestionnaire global vide) ; l'echec est journalise en CRITICAL avec sa
trace ; l'utilisateur est prevenu (barre de messages, une fois) ; l'etat
de capture le reflete : ERREUR quand la capture peut etre coupee (K1, K3,
K4, K6, K7, K9, K13, K14), un etat non sain nommant l'echec sinon (K2, K5,
K8, K10, K12). Plus : K3/K4 capturent ou restent en ERREUR, K7 et K13
reviennent a l'etat sain quand le tic reussit de nouveau, K8 continue de
capturer sans pretendre le contraire, K12 ne compte aucun faux echec de
capture, K14 n'est jamais PROTECTED quand l'etat du thread d'ecriture est
illisible (meme entre deux tics) puis revient a l'etat sain, T1 alerte
("dans vos donnees mais PAS dans l'historique") avec l'indicateur en
ERREUR. Une panne repetee a chaque tic (K7, K13, K14) est dite UNE fois.

Verdict attendu avant correctif : FAIL (K1-K4, K6-K11 : exception rendue a
Qt ; K5 : ni trace, ni alerte, ni etat ; K12 : faux echec de capture ;
T1 : aucune alerte). Avant le correctif de la revue v7 : K13 et K14 FAIL
(ni trace CRITICAL, ni alerte, etat PROTECTED sous un thread d'ecriture
que plus rien ne verifie).
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path

SCENARIO_ID = "tx_plugin_slot_exception"
INVARIANT = "I-THREAD-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Common body of every child. "#CASE#" is replaced by the case code.
_CHILD = r'''
import json, sqlite3, tempfile
from pathlib import Path
hooked = []
sys.excepthook = lambda t, v, tb: hooked.append(t.__name__ + ": " + str(v))
from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                       QgsVectorFileWriter, QgsCoordinateTransformContext,
                       QgsProject, QgsSettings)
import recoverland.recover as rec
from recoverland.recover import RecoverPlugin
from recoverland.core.journal_manager import sqlite_readonly_uri
from scripts.validation.plugin_harness import FakeIface, pump

logged = []
_real_flog, _real_qlog = rec.flog, rec.qlog
def _flog(message, level="INFO"):
    logged.append((str(level), str(message)))
    return _real_flog(message, level)
def _qlog(message, level="INFO"):
    logged.append(("PANEL:" + str(level), str(message)))
    return _real_qlog(message, level)
rec.flog, rec.qlog = _flog, _qlog
# The tracker logs its own slot failures (T1).
import recoverland.core.edit_tracker as et
_real_et_flog = et.flog
def _et_flog(message, level="INFO"):
    logged.append((str(level), str(message)))
    return _real_et_flog(message, level)
et.flog = _et_flog

TMP =tempfile.mkdtemp(prefix="rl_pslot_")
STATE = {"unloaded": False}
case = {}

def make_gpkg(name):
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
                         "m", "memory")
    mem.startEditing()
    for i in range(2):
        f = QgsFeature(mem.fields())
        f.setAttributes(["C%03d" % i, "AVANT"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(TMP, name + ".gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    return path

def add_layer(name):
    lyr = QgsVectorLayer(make_gpkg(name) + "|layername=pts", name, "ogr")
    QgsProject.instance().addMapLayer(lyr)
    pump(0.1)
    return lyr

def rename(lyr, value):
    fid = sorted(f.id() for f in lyr.getFeatures())[0]
    lyr.startEditing()
    lyr.changeAttributeValue(fid, lyr.fields().indexFromName("nom"), value)
    return bool(lyr.commitChanges())

def rows(value, timeout=4.0):
    deadline = time.monotonic() + timeout
    got = -1
    while True:
        journal = plugin._journal.path if plugin._journal.is_open else ""
        if journal and os.path.isfile(journal):
            try:
                conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
                got = int(conn.execute(
                    "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                    ("%" + value + "%",)).fetchone()[0])
                conn.close()
            except sqlite3.Error:
                pass
        if got >= 1 or time.monotonic() >= deadline:
            return got
        pump(0.1)

ARMED = []
def arm(obj, attr, message, skip=0, times=1, exc=RuntimeError):
    original = getattr(obj, attr)
    state = {"calls": 0, "raised": 0}
    def boom(*a, **k):
        state["calls"] += 1
        if state["calls"] > skip and state["raised"] < times:
            state["raised"] += 1
            raise exc(message)
        return original(*a, **k)
    setattr(obj, attr, boom)
    ARMED.append((obj, attr, original))
    return state

def disarm_all():
    while ARMED:
        obj, attr, original = ARMED.pop()
        setattr(obj, attr, original)

def observe(tag):
    h = plugin._capture_health()
    ind = plugin._status_indicator
    case[tag] = {
        "state": h.state, "reasons": list(h.reasons),
        "indicator": getattr(ind, "_current_level", None) if ind is not None else None,
        "tooltip": (getattr(ind, "_current_tooltip", "") or "")[:600] if ind is not None else "",
    }

iface = FakeIface()
plugin = RecoverPlugin(iface)
plugin.initGui()
pump(0.3)
bar0 = len(iface.bar.messages)
log0 = len(logged)

#CASE#

disarm_all()
new_bar = [(m[1], str(m[2]), m[3]) for m in iface.bar.messages[bar0:]]
new_log = logged[log0:]
if not STATE["unloaded"]:
    try:
        plugin.unload()
    except Exception as exc:
        case["unload_error"] = type(exc).__name__ + ": " + str(exc)
pump(0.3)
QgsSettings().setValue("RecoverLand/tracking_enabled", True)
out = {"case": case, "bar": new_bar,
       "critical": [m[:4000] for (lvl, m) in new_log if lvl == "CRITICAL"],
       "panel": [m[:600] for (lvl, m) in new_log if lvl.startswith("PANEL:")],
       "hooked": hooked}
print("OUTCOME " + json.dumps(out, default=str))
sys.stdout.flush()
os._exit(0)
'''

# key: (slot under test, injected text, at_risk, observation tag, case code)
# at_risk True: capture may be off -> ERROR; False: capture goes on -> a
# non-healthy state naming the failure; None: no failure expected at all.
_CASES = {
    "K1": ("layersAdded -> _on_layers_added", "injected failure K1 layersAdded",
           True, "obs", '''
st = arm(plugin._tracker, "connect_layer", "injected failure K1 layersAdded")
lyr = add_layer("k1")
observe("obs")
case["commit"] = rename(lyr, "K1_EDIT")
case["rows"] = rows("K1_EDIT", 2.0)
case["raised"] = st["raised"]
'''),
    "K2": ("layersRemoved -> _on_layers_removed", "injected failure K2 layersRemoved",
           False, "obs", '''
lyr = add_layer("k2")
st = arm(plugin._tracker, "disconnect_layer_by_id", "injected failure K2 layersRemoved")
QgsProject.instance().removeMapLayer(lyr.id())
pump(0.2)
observe("obs")
case["raised"] = st["raised"]
'''),
    "K3": ("readProject -> _on_project_opened", "injected failure K3 readProject",
           True, "obs", '''
lyr = add_layer("k3")
proj = os.path.join(TMP, "k3", "P3.qgz")
os.makedirs(os.path.dirname(proj), exist_ok=True)
case["write"] = bool(QgsProject.instance().write(proj))
pump(1.0)
st = arm(rec, "remove_project_log_handler", "injected failure K3 readProject", skip=1)
case["read"] = bool(QgsProject.instance().read(proj))
observe("obs")
pump(0.8)
lyr2 = QgsProject.instance().mapLayersByName("k3")[0]
case["commit"] = rename(lyr2, "K3_EDIT")
case["rows"] = rows("K3_EDIT")
observe("final")
case["raised"] = st["raised"]
'''),
    "K4": ("cleared -> _on_project_closed", "injected failure K4 cleared",
           True, "obs", '''
lyr = add_layer("k4")
st = arm(plugin._write_queue, "stop", "injected failure K4 cleared")
QgsProject.instance().clear()
observe("obs")
pump(0.8)
lyr2 = add_layer("k4b")
case["commit"] = rename(lyr2, "K4_EDIT")
case["rows"] = rows("K4_EDIT", 2.0)
observe("final")
case["raised"] = st["raised"]
'''),
    "K5": ("projectSaved -> _on_project_saved", "injected failure K5 projectSaved",
           False, "obs", '''
lyr = add_layer("k5")
case["session_journal"] = plugin._journal.path
st = arm(plugin._journal, "plan_history_carry", "injected failure K5 projectSaved")
proj = os.path.join(TMP, "k5", "P5.qgz")
os.makedirs(os.path.dirname(proj), exist_ok=True)
case["write"] = bool(QgsProject.instance().write(proj))
pump(1.0)
observe("obs")
case["journal_after"] = plugin._journal.path if plugin._journal.is_open else None
case["commit"] = rename(lyr, "K5_EDIT")
case["rows"] = rows("K5_EDIT")
case["raised"] = st["raised"]
'''),
    "K6": ("QTimer -> _reinit_after_clear", "injected failure K6 reinit",
           True, "obs", '''
st = arm(plugin, "_notify_dialog_project_switched", "injected failure K6 reinit", skip=1)
QgsProject.instance().clear()
pump(0.8)
observe("obs")
case["journal_open"] = plugin._journal.is_open
case["raised"] = st["raised"]
'''),
    "K7": ("QTimer -> _watchdog_tick", "injected failure K7 watchdog",
           True, "obs", '''
ind = plugin._status_indicator
st = arm(ind, "update_health",
         "wrapped C/C++ object of type StatusBarIndicator has been deleted "
         "(injected failure K7 watchdog)", times=10 ** 9)
plugin._watchdog_timer.start(100)
pump(1.2)
observe("obs")
case["raised_during"] = st["raised"]
disarm_all()
pump(0.8)
observe("after")
'''),
    "K8": ("QTimer -> disk check (_check)", "injected failure K8 disk check",
           False, "obs", '''
import recoverland.core.disk_monitor as dm
st = arm(dm, "check_disk_for_path", "injected failure K8 disk check", times=10 ** 9,
         exc=ValueError)
QgsProject.instance().clear()
pump(0.8)
observe("obs_init")
case["journal_open"] = plugin._journal.is_open
case["tracker_active"] = bool(plugin._tracker is not None and plugin._tracker.is_active)
if plugin._disk_timer is not None:
    plugin._disk_timer.start(100)
pump(0.6)
observe("obs")
lyr = add_layer("k8")
case["commit"] = rename(lyr, "K8_EDIT")
case["rows"] = rows("K8_EDIT")
case["raised"] = st["raised"]
'''),
    "K9": ("indicator toggle_requested -> _toggle_tracking", "injected failure K9 toggle",
           True, "obs", '''
st = arm(plugin._tracker, "deactivate", "injected failure K9 toggle")
plugin._status_indicator.toggle_requested.emit()
pump(0.2)
observe("obs")
case["tracker_active"] = plugin._tracker.is_active
case["raised"] = st["raised"]
'''),
    "K10": ("indicator open_dialog_requested -> run", "injected failure K10 open dialog",
            False, "obs", '''
st = arm(rec, "RecoverDialog", "injected failure K10 open dialog")
plugin._status_indicator.open_dialog_requested.emit()
pump(0.2)
observe("obs")
case["raised"] = st["raised"]
'''),
    "K11": ("QTimer.singleShot(500) -> dlg.on_events_committed", "",
            None, "", '''
class _Dlg:
    _review_wants_persist = False
    def __init__(self):
        self.calls = []
    def cleanup_resources(self):
        pass
    def close(self):
        pass
    def deleteLater(self):
        pass
    def on_project_switched(self, tracker=None):
        pass
    def on_events_committed(self, fp=""):
        self.calls.append(fp)
lyr = add_layer("k11")
plugin.dlg = _Dlg()
case["commit"] = rename(lyr, "K11_EDIT")
plugin.unload()
STATE["unloaded"] = True
pump(1.0)
'''),
    "K12": ("tracker commit callback -> _on_events_committed", "injected failure K12 display",
            False, "obs", '''
lyr = add_layer("k12")
st = arm(plugin._status_indicator, "pulse", "injected failure K12 display")
case["commit"] = rename(lyr, "K12_EDIT")
case["rows"] = rows("K12_EDIT")
pump(0.2)
observe("obs")
case["capture_failures"] = int(getattr(plugin._tracker, "capture_failures", 0) or 0)
case["raised"] = st["raised"]
'''),
    "K13": ("QTimer -> _watchdog_tick -> write_queue.watchdog_check",
            "injected failure K13 watchdog_check", True, "obs", '''
st = arm(plugin._write_queue, "watchdog_check", "injected failure K13 watchdog_check",
         times=10 ** 9)
plugin._watchdog_timer.start(100)
pump(1.2)
observe("obs")
case["raised_during"] = st["raised"]
disarm_all()
pump(0.8)
observe("after")
'''),
    "K14": ("write_queue.health -> _capture_health / _watchdog_tick",
            "injected failure K14 writer health", True, "obs", '''
plugin._stop_watchdog()
st = arm(plugin._write_queue, "health", "injected failure K14 writer health",
         times=10 ** 9)
observe("between_ticks")
plugin._start_watchdog()
plugin._watchdog_timer.start(100)
pump(1.2)
observe("obs")
case["raised_during"] = st["raised"]
disarm_all()
pump(0.8)
observe("after")
'''),
    "T1": ("tracker slot beforeCommitChanges -> status slot_failed",
           "injected failure T1 tracker slot", True, "obs", '''
lyr = add_layer("t1")
st = arm(plugin._tracker, "_on_before_commit", "injected failure T1 tracker slot")
case["commit"] = rename(lyr, "T1_EDIT")
pump(0.3)
observe("obs")
case["capture_failures"] = int(getattr(plugin._tracker, "capture_failures", 0) or 0)
case["raised"] = st["raised"]
'''),
}

_HEALTHY = ("PROTECTED", "PERSISTING")
# Failures injected on every tick: said once, not once per tick.
_REPEATED_EVERY_TICK = ("K7", "K13", "K14")


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_plugin_slot_exception setup: trace_id={ctx.trace_id}", "INFO")


def _run_case(key: str, code: str) -> dict:
    import shutil
    import tempfile
    from scripts.validation.crash_harness import run_child
    body = _CHILD.replace("#CASE#", textwrap.dedent(code).strip("\n"))
    # A profile of its own: a toggle or a setting written by one case never
    # leaks into another one.
    cfg = tempfile.mkdtemp(prefix="rl_psc_")
    try:
        rc, so, se = run_child(body, timeout=240,
                               extra_env={"QGIS_CUSTOM_CONFIG_PATH": cfg})
    finally:
        shutil.rmtree(cfg, ignore_errors=True)
    line = next((ln for ln in so.splitlines() if ln.startswith("OUTCOME ")), "")
    try:
        outcome = json.loads(line[len("OUTCOME "):]) if line else None
    except ValueError:
        outcome = None
    return {"rc": rc, "outcome": outcome,
            "stderr_tail": se[-600:] if (rc or outcome is None) else ""}


def run(ctx):
    from concurrent.futures import ThreadPoolExecutor
    from recoverland.core.logger import flog
    d = ctx.data
    # Independent child processes (own profile, own temp dir): run a few at
    # a time, or thirteen QGIS start-ups exceed the runner's timeout on a
    # loaded machine. The threads only wait on subprocesses; no Qt here.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {key: pool.submit(_run_case, key, case[4])
                   for key, case in _CASES.items()}
        for key, future in futures.items():
            try:
                d[key] = future.result()
            except Exception as exc:  # noqa: BLE001 - reported as a FAIL
                d[key] = {"rc": None, "outcome": None,
                          "stderr_tail": f"{type(exc).__name__}: {exc}"}
            flog(f"tx_plugin_slot_exception case={key} trace_id={ctx.trace_id} "
                 f"rc={d[key]['rc']} outcome="
                 f"{json.dumps(d[key]['outcome'], default=str)[:2500]}", "INFO")
    flog(f"tx_plugin_slot_exception run: trace_id={ctx.trace_id} cases={len(_CASES)}",
         "INFO")


def _told(o: dict, text: str) -> list:
    return [t for (t, _lvl, _dur) in (o.get("bar") or []) if text in t]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    for key, (slot, text, at_risk, tag, _code) in _CASES.items():
        c = d.get(key) or {}
        o = c.get("outcome") or {}
        cs = o.get("case") or {}
        out.append((
            f"{key.lower()}_exception_does_not_reach_qt",
            c.get("rc") == 0 and bool(o) and not o.get("hooked"),
            f"{key} ({slot}) : code de sortie={c.get('rc')} exceptions rendues a Qt="
            f"{o.get('hooked')} stderr={c.get('stderr_tail')!r} : sans gestionnaire, "
            f"PyQt6 fait avorter QGIS ; avec celui de QGIS, le slot s'arrete a "
            f"mi-chemin et rien ne le dit",
        ))
        if at_risk is None:
            continue
        crit = [m for m in (o.get("critical") or []) if "Traceback" in m and text in m]
        out.append((
            f"{key.lower()}_failure_logged_critical_with_traceback",
            bool(crit),
            f"{key} ({slot}) : lignes CRITICAL avec trace mentionnant {text!r}="
            f"{len(crit)} (CRITICAL recus={[m[:160] for m in (o.get('critical') or [])][:3]}) : "
            f"sans la trace, l'echec est indiagnosticable chez l'utilisateur",
        ))
        told = _told(o, text)
        repeated = key in _REPEATED_EVERY_TICK
        once = len(told) == 1 if repeated else len(told) >= 1
        out.append((
            f"{key.lower()}_user_is_told" + ("_once" if repeated else ""),
            once,
            f"{key} ({slot}) : messages de la barre nommant l'echec={len(told)} "
            f"{told[:2]} (barre={[t[:120] for (t, _l, _d) in (o.get('bar') or [])][:4]}) : "
            f"un echec du plugin ne doit pas rester dans un fichier de log"
            + (" ; une panne repetee a chaque tic est dite UNE fois" if repeated else ""),
        ))
        obs = cs.get(tag) or {}
        state = obs.get("state")
        if key == "T1":
            health_ok = state == "ERROR" and obs.get("indicator") == "ERROR"
        elif at_risk:
            health_ok = state == "ERROR" and text in " ".join(obs.get("reasons") or [])
            if key != "K7":
                health_ok = health_ok and obs.get("indicator") == "ERROR"
        else:
            health_ok = (state not in _HEALTHY + ("NO_PROJECT",)
                         and text in " ".join(obs.get("reasons") or []))
        out.append((
            f"{key.lower()}_capture_health_reflects_the_failure",
            health_ok,
            f"{key} ({slot}) : etat={state} indicateur={obs.get('indicator')} "
            f"raisons={obs.get('reasons')} (attendu : "
            f"{'ERROR' if at_risk else 'un etat non sain nommant l echec'}) : "
            f"un point vert ou gris pendant que la capture peut etre coupee est "
            f"une fausse assurance",
        ))

    # ---- case-specific ---------------------------------------------------
    def _cs(key):
        return ((d.get(key) or {}).get("outcome") or {}).get("case") or {}

    k3 = _cs("K3")
    out.append((
        "k3_fixture_failure_hit_the_readproject_slot",
        k3.get("write") is True and k3.get("read") is True and k3.get("raised") == 1,
        f"ecrit={k3.get('write')} lu={k3.get('read')} exceptions levees="
        f"{k3.get('raised')} (attendu 1, au 2e appel, dans _on_project_opened)",
    ))
    out.append((
        "k3_capture_resumes_or_error_stays_shown",
        (k3.get("rows") or 0) >= 1 or (k3.get("final") or {}).get("state") == "ERROR",
        f"apres l'echec de readProject : lignes journalisees={k3.get('rows')} "
        f"etat final={(k3.get('final') or {}).get('state')} : soit la capture "
        f"reprend, soit l'indicateur reste en ERREUR",
    ))
    k4 = _cs("K4")
    out.append((
        "k4_capture_resumes_or_error_stays_shown",
        k4.get("raised") == 1 and ((k4.get("rows") or 0) >= 1
                                   or (k4.get("final") or {}).get("state") == "ERROR"),
        f"exceptions levees={k4.get('raised')} lignes journalisees apres "
        f"Nouveau projet={k4.get('rows')} etat final="
        f"{(k4.get('final') or {}).get('state')} indicateur="
        f"{(k4.get('final') or {}).get('indicator')} : l'utilisateur edite le "
        f"nouveau projet ; sans capture, l'indicateur doit le dire",
    ))
    k5 = _cs("K5")
    out.append((
        "k5_capture_goes_on_in_the_session_journal",
        k5.get("raised") == 1 and (k5.get("rows") or 0) >= 1,
        f"exceptions levees={k5.get('raised')} journal apres l'enregistrement="
        f"{k5.get('journal_after')} lignes={k5.get('rows')}",
    ))
    k7 = _cs("K7")
    out.append((
        "k7_watchdog_back_to_healthy_when_the_tick_succeeds_again",
        (k7.get("raised_during") or 0) >= 3
        and (k7.get("after") or {}).get("state") in _HEALTHY,
        f"echecs du tic={k7.get('raised_during')} etat apres retour a la normale="
        f"{(k7.get('after') or {}).get('state')} raisons="
        f"{(k7.get('after') or {}).get('reasons')} : l'ERREUR dure tant que la "
        f"surveillance est en panne, pas au-dela",
    ))
    k13 = _cs("K13")
    out.append((
        "k13_writer_check_back_to_healthy_when_it_succeeds_again",
        (k13.get("raised_during") or 0) >= 3
        and (k13.get("after") or {}).get("state") in _HEALTHY,
        f"echecs de watchdog_check={k13.get('raised_during')} etat apres retour a "
        f"la normale={(k13.get('after') or {}).get('state')} raisons="
        f"{(k13.get('after') or {}).get('reasons')} : l'ERREUR dure tant que la "
        f"verification du thread d'ecriture est en panne, pas au-dela",
    ))
    k14 = _cs("K14")
    k14_between = k14.get("between_ticks") or {}
    out.append((
        "k14_unreadable_writer_health_is_never_protected",
        (k14.get("raised_during") or 0) >= 3
        and k14_between.get("state") == "ERROR"
        and "thread d'ecriture" in " ".join(k14_between.get("reasons") or [])
        and (k14.get("after") or {}).get("state") in _HEALTHY,
        f"etat du thread d'ecriture illisible, sans aucun tic : etat="
        f"{k14_between.get('state')} raisons={k14_between.get('reasons')} "
        f"(attendu ERROR, raison nommant le thread d'ecriture) ; etat apres "
        f"retour a la normale={(k14.get('after') or {}).get('state')} : un "
        f"point vert pose sur un etat que personne ne peut lire est une fausse "
        f"assurance ; un thread d'ecriture mort passerait inapercu",
    ))
    k8o = (d.get("K8") or {}).get("outcome") or {}
    k8 = _cs("K8")
    false_off = [m for m in (k8o.get("panel") or []) + [t for (t, _l, _x) in (k8o.get("bar") or [])]
                 if "desactive" in m.lower()]
    out.append((
        "k8_capture_goes_on_and_nothing_claims_it_is_off",
        k8.get("journal_open") is True and k8.get("tracker_active") is True
        and (k8.get("rows") or 0) >= 1 and not false_off,
        f"journal ouvert={k8.get('journal_open')} suivi actif={k8.get('tracker_active')} "
        f"lignes={k8.get('rows')} messages 'desactive'={false_off[:2]} : la "
        f"surveillance disque en panne ne coupe pas la capture, et aucun message "
        f"ne doit pretendre le contraire",
    ))
    k12 = _cs("K12")
    out.append((
        "k12_display_failure_is_not_a_capture_failure",
        (k12.get("rows") or 0) >= 1 and k12.get("capture_failures") == 0
        and (k12.get("obs") or {}).get("state") != "ERROR",
        f"lignes journalisees={k12.get('rows')} echecs de capture comptes="
        f"{k12.get('capture_failures')} etat={(k12.get('obs') or {}).get('state')} : "
        f"le commit EST dans l'historique ; le dire perdu est une fausse alerte",
    ))
    t1o = (d.get("T1") or {}).get("outcome") or {}
    t1 = _cs("T1")
    t1_told = [t for t in _told(t1o, "injected failure T1") if "historique" in t]
    out.append((
        "t1_slot_failed_status_is_alerted",
        t1.get("commit") is True and (t1.get("capture_failures") or 0) >= 1
        and bool(t1_told),
        f"commit={t1.get('commit')} echecs de capture={t1.get('capture_failures')} "
        f"alertes={t1_told[:1]} (barre={[t[:120] for (t, _l, _x) in (t1o.get('bar') or [])][:3]}) : "
        f"la modification est dans la donnee, pas dans l'historique ; il faut le dire",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_plugin_slot_exception.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
