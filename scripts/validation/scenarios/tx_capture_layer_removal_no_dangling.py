# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_layer_removal_no_dangling - RecoverLand validation runtime.

Invariant: I-THREAD-01 (le plugin ne fait jamais tomber QGIS) applique au
retrait des couches : une couche qui quitte le projet est relachee par le
suivi sans qu'aucun appel ne passe par l'objet C++ qu'elle etait.

Le mecanisme
------------
`EditSessionTracker` garde, pour chaque couche vectorielle du projet, les
signaux Qt LIES (`layer.dataSourceChanged`, `layer.editingStarted`, puis
les huit signaux de commit) avec le slot branche dessus, pour pouvoir les
debrancher plus tard. Un signal lie PyQt retient le pointeur C++ brut de
sa couche, sans aucune garde : `signal.disconnect(slot)` appelle
`QObject::disconnect` sur ce pointeur, que la couche existe encore ou non.

Or le plugin relaie `QgsProject.layersRemoved` vers
`disconnect_layer_by_id`, et ce signal part APRES la destruction des
couches que le projet possedait (removeMapLayer, removeAllMapLayers,
Fichier > Nouveau, ouverture d'un autre projet : `QgsProject.clear()`).
`_unwatch_layer` debranchait alors les signaux stockes d'une couche deja
liberee : lecture de la table virtuelle d'un objet detruit. Selon ce que
le tas a fait de la memoire liberee, rien ne se voit, ou QGIS disparait
(0xC0000005, pile edit_tracker._unwatch_layer < disconnect_layer_by_id <
recover._on_layers_removed ; 3 executions sur 11 a Fichier > Nouveau).
`_unbind_signals`, lui, commencait par `layer.id()` : sur une couche
detruite l'exception sautait le retrait de ses huit signaux de commit,
gardes ensuite sans fin dans `_signal_connections`.

Le scenario d'echec concret
---------------------------
L'utilisateur a des modifications non enregistrees sur plusieurs couches,
retire une couche du projet ou fait Fichier > Nouveau / ouvre un autre
projet : QGIS se ferme brutalement, toutes les modifications en cours de
toutes les couches sont perdues.

Ce que le scenario execute reellement
-------------------------------------
Chaque cas tourne dans son propre processus QGIS enfant
(crash_harness.run_child : un plantage natif est un FAIL, pas la fin de
la suite), avec le VRAI RecoverPlugin sous plugin_harness.FakeIface :
  A  trois couches creees en Python : l'une retiree (removeMapLayer), puis
     Fichier > Nouveau (QgsProject.clear) avec une couche en edition ;
  B  deux couches creees par QGIS a la lecture d'un projet .qgs : l'une
     retiree, puis QgsProject.clear ;
  C  une couche retiree VIVANTE (takeMapLayer) : elle doit etre
     entierement debranchee (le correctif ne doit pas sauter les
     debranchements des couches vivantes) ;
  D  trente cycles "trois couches, une en edition, Fichier > Nouveau",
     sans instrumentation : le symptome reel (plantage) ;
  E  une couche detruite dans le dos du projet (sip.delete : le registre
     l'oublie sans layersRemoved), son id repris par une nouvelle couche
     (QgsMapLayer.setId, comme un projet ou un .qlr qui garde ses ids),
     puis Fichier > Nouveau : disconnect_all est alors le seul
     debranchement, et la nouvelle couche doit etre suivie (les entrees
     de l'ancienne la faisaient ignorer : editee, jamais capturee).
Detection deterministe (A, B) : chaque couche signale elle-meme sa
destruction (`destroyed`, emis dans le destructeur C++, avant
layersRemoved), et un profileur Python (sys.setprofile) voit, pendant tout
le traitement du retrait (boucle d'evenements comprise), chaque appel
C/C++ AVANT qu'il ne s'execute. Un appel dont l'objet est une couche
detruite, ou un signal lie a une couche detruite (le repr d'un signal lie
nomme l'adresse de son objet Python), est ecrit sur la sortie a cet
instant : la preuve survit au plantage qu'il peut provoquer. Apres coup,
tout ce que le suivi retient encore (signaux lies, couches) est parcouru :
une reference a une couche detruite est un plantage differe.

Verdict attendu avant correctif (arbre v7) : FAIL (debranchement de
signaux de couches detruites dans _unwatch_layer, en A, B et E ; appel de
`layer.id()` sur une couche detruite et signaux de commit gardes dans
_signal_connections ; couche E reprise jamais branchee ; plantage
0xC0000005 frequent en D, possible en A, B et E).
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "tx_capture_layer_removal_no_dangling"
INVARIANT = "I-THREAD-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_STRESS_CYCLES = 30

# Common body of every child. "#CASE#" is replaced by the case code.
_CHILD = r'''
import re, tempfile
hooked = []
def _hook(t, v, tb):
    hooked.append(t.__name__ + ": " + str(v))
    sys.__excepthook__(t, v, tb)
sys.excepthook = _hook
from qgis.core import QgsProject, QgsSettings, QgsVectorLayer
from qgis.PyQt import sip
from qgis.PyQt.QtCore import pyqtBoundSignal
from osgeo import ogr
from recoverland.core.edit_tracker import EditSessionTracker
from scripts.validation.plugin_harness import FakeIface, pump

ogr.UseExceptions()
QgsSettings().setValue("RecoverLand/tracking_enabled", True)
QgsSettings().setValue("RecoverLand/tracking_disabled_by_disk", False)
TMP = tempfile.mkdtemp(prefix="rl_unw_")
SRC = os.path.join(TMP, "src.gpkg")
_ds = ogr.GetDriverByName("GPKG").CreateDataSource(SRC)
_l = _ds.CreateLayer("pts", geom_type=ogr.wkbPoint)
_l.CreateField(ogr.FieldDefn("v", ogr.OFTInteger))
_ds = None
PROJ = QgsProject.instance()
out = {}

def emit(tag, payload):
    print(tag + " " + json.dumps(payload, default=str))
    sys.stdout.flush()

def new_layer(name):
    return QgsVectorLayer(SRC + "|layername=pts", name, "ogr")

# id(wrapper) -> name, written by the layer's own `destroyed` signal (it
# fires inside the C++ destructor, before layersRemoved). The wrappers stay
# referenced so their ids stay unique for the whole run.
DEAD, KEEP = {}, []
def mark(layer):
    KEEP.append(layer)
    key, name = id(layer), layer.name()
    layer.destroyed.connect(lambda *_a, k=key, n=name: DEAD.__setitem__(k, n))

ADDR = re.compile(r"object at (0x[0-9A-Fa-f]+)>")
def owner(obj):
    """("signal"|"layer", id of the layer wrapper the call goes through)."""
    if isinstance(obj, pyqtBoundSignal):
        m = ADDR.search(repr(obj))
        return "signal", (int(m.group(1), 16) if m else None)
    return "layer", id(obj)

# c_call is reported BEFORE the C function runs: the line is on stdout
# even when the call it names brings the process down.
def prof(frame, event, arg):
    if event != "c_call":
        return
    obj = getattr(arg, "__self__", None)
    if obj is None or isinstance(obj, type(sys)):
        return
    kind, key = owner(obj)
    if key in DEAD:
        code = frame.f_code
        emit("CALL", {"case": CASE, "kind": kind,
                      "call": getattr(arg, "__name__", "?"), "layer": DEAD[key],
                      "at": os.path.basename(code.co_filename) + ":"
                            + code.co_name + ":" + str(frame.f_lineno)})

def watched(action):
    sys.setprofile(prof)
    try:
        action()
    finally:
        sys.setprofile(None)

def held(root, keys, depth=0, seen=None):
    """What *root* still holds of the layers whose wrapper id is in *keys*."""
    seen = set() if seen is None else seen
    if depth > 6 or id(root) in seen:
        return []
    seen.add(id(root))
    if isinstance(root, pyqtBoundSignal):
        kind, key = owner(root)
        return ["signal of " + str(keys[key])] if key in keys else []
    if id(root) in keys:
        return ["layer " + str(keys[id(root)])]
    if isinstance(root, dict):
        items = list(root.values())
    elif isinstance(root, (list, tuple, set, frozenset)):
        items = list(root)
    else:
        return []
    found = []
    for item in items:
        found.extend(held(item, keys, depth + 1, seen))
    return found

# Which tracker slots a layer still reaches (cases C and E).
REACHED = []
for _name in ("_on_editing_started", "_on_watched_editing"):
    _orig = getattr(EditSessionTracker, _name, None)
    if _orig is None:  # a tracker without that slot (before v7)
        continue
    def _spy(self, lid, *a, _o=_orig, _n=_name, **k):
        REACHED.append((_n, lid))
        return _o(self, lid, *a, **k)
    setattr(EditSessionTracker, _name, _spy)

def tracked(tr, layers):
    if tr is None:
        return None
    return {"watched": sum(1 for l in layers if l.id() in tr.__dict__.get("_watched", {})),
            "connected": sum(1 for l in layers if l.id() in tr._connected_layers)}

def start_plugin():
    from recoverland.recover import RecoverPlugin
    plugin = RecoverPlugin(FakeIface())
    plugin.initGui()
    pump(0.3)
    return plugin

CASE = "#NAME#"
#CASE#

out["hooked"] = list(hooked)
emit("OUT", out)
try:
    plugin.unload()
except Exception as exc:
    emit("UNLOAD", type(exc).__name__ + ": " + str(exc))
os._exit(0)
'''

_CASES = {
    # A: layers made in Python, one removed, then File > New.
    "A": r'''
plugin = start_plugin()
layers = [new_layer("py%d" % i) for i in range(3)]
for l in layers:
    mark(l)
PROJ.addMapLayers(layers)
pump(0.2)
tr = plugin._tracker
out["tracked"] = tracked(tr, layers)
watched(lambda: (PROJ.removeMapLayer(layers[0].id()), pump(0.1)))
out["removed_destroyed"] = id(layers[0]) in DEAD
out["left_after_remove"] = held(tr.__dict__, DEAD) if tr is not None else None
layers[1].startEditing()
watched(lambda: (PROJ.clear(), pump(0.3)))
out["all_destroyed"] = all(id(l) in DEAD for l in layers)
out["left_after_clear"] = held(tr.__dict__, DEAD) if tr is not None else None
''',
    # B: layers created by QGIS itself when a project is read. The project
    # is written before the plugin starts, so the setup touches no tracker.
    "B": r'''
for i in range(2):
    PROJ.addMapLayer(new_layer("prj%d" % i))
qgs = os.path.join(TMP, "b.qgs")
out["written"] = bool(PROJ.write(qgs))
PROJ.clear()
pump(0.1)
plugin = start_plugin()
out["read"] = bool(PROJ.read(qgs))
pump(0.3)
layers = [l for l in PROJ.mapLayers().values() if isinstance(l, QgsVectorLayer)]
for l in layers:
    mark(l)
tr = plugin._tracker
out["count"] = len(layers)
out["tracked"] = tracked(tr, layers)
if layers:
    watched(lambda: (PROJ.removeMapLayer(layers[0].id()), pump(0.1)))
    out["removed_destroyed"] = id(layers[0]) in DEAD
    out["left_after_remove"] = held(tr.__dict__, DEAD) if tr is not None else None
    layers[-1].startEditing()
watched(lambda: (PROJ.clear(), pump(0.3)))
out["all_destroyed"] = bool(layers) and all(id(l) in DEAD for l in layers)
out["left_after_clear"] = held(tr.__dict__, DEAD) if tr is not None else None
''',
    # C: a layer taken out of the project ALIVE.
    "C": r'''
plugin = start_plugin()
live = new_layer("live")
PROJ.addMapLayer(live)
pump(0.1)
tr = plugin._tracker
live_id = live.id()
out["connected"] = tr is not None and live_id in tr._connected_layers
out["watched"] = tr is not None and live_id in tr.__dict__.get("_watched", {live_id: 1})
taken = PROJ.takeMapLayer(live)
pump(0.1)
out["alive"] = taken is not None and not sip.isdeleted(taken)
out["held"] = held(tr.__dict__, {id(taken): "live"}) if tr is not None else None
del REACHED[:]
if taken is not None:
    taken.startEditing()
    pump(0.05)
    taken.rollBack()
out["reached"] = [x for x in REACHED if x[1] == live_id]
''',
    # E: a layer destroyed behind the project's back (QgsMapLayerStore only
    # forgets it, no layersRemoved), its id reused by a new layer, then
    # File > New (disconnect_all is then the only release).
    "E": r'''
plugin = start_plugin()
layers = [new_layer("e%d" % i) for i in range(2)]
for l in layers:
    mark(l)
PROJ.addMapLayers(layers)
pump(0.2)
tr = plugin._tracker
out["tracked"] = tracked(tr, layers)
gone_id = layers[0].id()
watched(lambda: (sip.delete(layers[0]), pump(0.1)))
out["destroyed_unannounced"] = id(layers[0]) in DEAD and PROJ.mapLayer(gone_id) is None
again = new_layer("e_again")
out["id_reused"] = bool(again.setId(gone_id)) and again.id() == gone_id
watched(lambda: (PROJ.addMapLayer(again), pump(0.1)))
out["reused_connected"] = tr is not None and tr._connected_layers.get(gone_id) is again
out["left_after_reuse"] = held(tr.__dict__, DEAD) if tr is not None else None
del REACHED[:]
again.startEditing()
pump(0.05)
again.rollBack()
out["reused_reached"] = [x for x in REACHED if x[1] == gone_id]
watched(lambda: (PROJ.clear(), pump(0.3)))
out["all_destroyed"] = all(id(l) in DEAD for l in layers)
out["left_after_clear"] = held(tr.__dict__, DEAD) if tr is not None else None
''',
    # D: the real symptom, uninstrumented.
    "D": r'''
plugin = start_plugin()
done = 0
for cycle in range(#CYCLES#):
    batch = [new_layer("s%d_%d" % (cycle, i)) for i in range(3)]
    PROJ.addMapLayers(batch)
    pump(0.02)
    batch[cycle % 3].startEditing()
    batch = None
    PROJ.clear()
    pump(0.05)
    done += 1
out["cycles"] = done
''',
}


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def _run_case(key: str, code: str) -> dict:
    import shutil
    import tempfile
    from scripts.validation.crash_harness import run_child
    body = (_CHILD.replace("#NAME#", key)
            .replace("#CASE#", code.strip("\n"))
            .replace("#CYCLES#", str(_STRESS_CYCLES)))
    cfg = tempfile.mkdtemp(prefix="rl_unwc_")
    try:
        rc, so, se = run_child(body, timeout=240,
                               extra_env={"QGIS_CUSTOM_CONFIG_PATH": cfg})
    finally:
        shutil.rmtree(cfg, ignore_errors=True)
    tagged = {"CALL": [], "OUT": [], "UNLOAD": []}
    for line in so.splitlines():
        tag, _, rest = line.partition(" ")
        if tag in tagged:
            try:
                tagged[tag].append(json.loads(rest))
            except ValueError:
                pass
    return {"rc": rc, "calls": tagged["CALL"],
            "outcome": tagged["OUT"][0] if tagged["OUT"] else None,
            "unload": tagged["UNLOAD"],
            "stderr_tail": se[-700:] if (rc or not tagged["OUT"]) else ""}


def run(ctx):
    from concurrent.futures import ThreadPoolExecutor
    from recoverland.core.logger import flog
    d = ctx.data
    # Independent children (own profile, own temp dir); the threads only
    # wait on subprocesses, no Qt here.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {key: pool.submit(_run_case, key, code)
                   for key, code in _CASES.items()}
        for key, future in futures.items():
            try:
                d[key] = future.result()
            except Exception as exc:  # noqa: BLE001 - reported as a FAIL
                d[key] = {"rc": None, "calls": [], "outcome": None,
                          "stderr_tail": f"{type(exc).__name__}: {exc}"}
            flog(f"{SCENARIO_ID} case={key} trace_id={ctx.trace_id} "
                 f"rc={d[key]['rc']} data="
                 f"{json.dumps(d[key], default=str)[:3000]}", "INFO")


def _rc(c: dict) -> str:
    rc = c.get("rc")
    return f"{rc} ({hex(rc & 0xFFFFFFFF)})" if isinstance(rc, int) else str(rc)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    keys = "ABCDE"
    a, b, c, s, e = (d.get(k) or {} for k in keys)
    ao, bo, co, so, eo = (x.get("outcome") or {} for x in (a, b, c, s, e))
    out = []
    crashed = {k: _rc(x) for k, x in zip(keys, (a, b, c, s, e))
               if x.get("rc") != 0 or not x.get("outcome")}
    out.append((
        "qgis_survives_layer_removal_and_file_new",
        not crashed and so.get("cycles") == _STRESS_CYCLES,
        f"processus QGIS enfants tombes={crashed} cycles Fichier > Nouveau "
        f"acheves={so.get('cycles')}/{_STRESS_CYCLES} stderr="
        f"{[x.get('stderr_tail') for x in (a, b, c, s, e) if x.get('stderr_tail')]!r} : "
        f"QGIS disparait quand une couche quitte le projet ; les "
        f"modifications non enregistrees de TOUTES les couches sont perdues",
    ))
    a_tr, b_tr, e_tr = (o.get("tracked") or {} for o in (ao, bo, eo))
    out.append((
        "fixture_layers_tracked_then_destroyed",
        a_tr.get("connected") == 3 and b_tr.get("connected") == bo.get("count") == 2
        and e_tr.get("connected") == 2
        and ao.get("removed_destroyed") is True and ao.get("all_destroyed") is True
        and bo.get("removed_destroyed") is True and bo.get("all_destroyed") is True
        and bo.get("read") is True and eo.get("destroyed_unannounced") is True
        and eo.get("id_reused") is True and eo.get("all_destroyed") is True,
        f"A suivies={a_tr} B suivies={b_tr} (sur {bo.get('count')}) E suivies="
        f"{e_tr} ; A detruites={ao.get('all_destroyed')} B detruites="
        f"{bo.get('all_destroyed')} E detruite sans layersRemoved="
        f"{eo.get('destroyed_unannounced')} id reprise={eo.get('id_reused')} : "
        f"le cas exige des couches suivies puis detruites (processus A={_rc(a)} "
        f"B={_rc(b)} E={_rc(e)})",
    ))
    calls = [x for case in (a, b, e) for x in (case.get("calls") or [])]
    via_signal = [x for x in calls if x.get("kind") == "signal"]
    out.append((
        "no_signal_of_a_destroyed_layer_is_called",
        not via_signal,
        f"appels sur des signaux lies a une couche deja detruite={via_signal[:8]} "
        f"(total {len(via_signal)}) : chacun passe par le pointeur C++ libere "
        f"(QObject::disconnect sur un objet detruit), QGIS peut planter "
        f"(0xC0000005) au retrait d'une couche ou a Fichier > Nouveau",
    ))
    via_layer = [x for x in calls if x.get("kind") == "layer"]
    out.append((
        "destroyed_layer_never_called_into",
        not via_layer,
        f"appels de methode sur une couche deja detruite={via_layer[:8]} "
        f"(total {len(via_layer)}) : le suivi ne doit plus toucher une couche "
        f"detruite ; l'exception qu'il rattrape saute le nettoyage qui suit",
    ))
    left = {f"{k}_{when}": o.get(f"left_after_{when}")
            for k, o in (("A", ao), ("B", bo)) for when in ("remove", "clear")}
    left.update({f"E_{when}": eo.get(f"left_after_{when}")
                 for when in ("reuse", "clear")})
    out.append((
        "no_reference_to_a_destroyed_layer_kept",
        all(v == [] for v in left.values()),
        f"references gardees par le suivi apres le retrait={left} : un signal "
        f"lie a une couche detruite reste un plantage en attente pour le "
        f"prochain debranchement",
    ))
    out.append((
        "layer_removed_alive_is_fully_released",
        co.get("connected") is True and co.get("watched") is True
        and co.get("alive") is True and co.get("held") == []
        and co.get("reached") == [],
        f"suivie={co.get('connected')} vivante apres retrait={co.get('alive')} "
        f"references gardees={co.get('held')} slots encore atteints="
        f"{co.get('reached')} (processus {_rc(c)}) : une couche sortie du "
        f"projet mais vivante doit etre debranchee, sinon ses editions hors "
        f"projet declenchent encore le suivi",
    ))
    reached = [x[0] for x in (eo.get("reused_reached") or [])]
    out.append((
        "layer_reusing_a_destroyed_layer_id_is_tracked",
        eo.get("reused_connected") is True and "_on_editing_started" in reached,
        f"nouvelle couche sous l'id d'une couche detruite sans layersRemoved : "
        f"branchee={eo.get('reused_connected')} slots atteints a l'edition="
        f"{reached} (processus {_rc(e)}) : les entrees de la couche detruite "
        f"faisaient ignorer la nouvelle, editee sans aucune capture",
    ))
    hooked = {k: o.get("hooked") for k, o in zip(keys, (ao, bo, co, so, eo))
              if o.get("hooked")}
    out.append((
        "no_exception_reaches_qt",
        all(x.get("outcome") for x in (a, b, c, s, e)) and not hooked,
        f"exceptions rendues a Qt={hooked} : une exception dans un slot fait "
        f"avorter QGIS",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
