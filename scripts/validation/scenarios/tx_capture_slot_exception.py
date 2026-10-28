# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_slot_exception - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (tant que RecoverLand affiche la capture active,
chaque commit est capture ; sinon l'utilisateur est prevenu) et
I-THREAD-01 (un slot Qt ne laisse jamais s'echapper une exception).

Le mecanisme
------------
Le suivi des editions tient dans des slots branches sur les signaux de
commit de la couche (beforeCommitChanges, committed*, afterCommitChanges).
Une exception qui s'echappe d'un slot appele par Qt :
  - sans gestionnaire global (harnais headless, PyQt6) fait avorter le
    processus : QGIS disparait, avec les editions non enregistrees de
    toutes les couches ;
  - dans QGIS, remonte au gestionnaire de QGIS (boite "Erreur Python") :
    le slot s'arrete a mi-chemin, le commit continue, la modification est
    ecrite dans la donnee et RecoverLand n'en dit rien : l'indicateur reste
    au vert.

Chaque cas tourne dans un processus QGIS enfant, avec un gestionnaire
d'exception global qui imite celui de QGIS (il note l'exception au lieu
d'avorter), et une exception injectee dans un slot pendant un vrai
commit :
  K1  dans le slot beforeCommitChanges ;
  K2  dans le slot d'une etape du commit (committedAttributeValuesChanges) ;
  K3  dans le slot afterCommitChanges.

Exige : l'exception ne s'echappe pas vers Qt ; le commit de l'utilisateur
aboutit ; RecoverLand le dit (statut de capture, compteur d'echecs donc
indicateur en ERROR).

Verdict attendu avant correctif : FAIL (exception rendue a Qt, aucun
statut, indicateur sain).
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "tx_capture_slot_exception"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_CHILD = r'''
import tempfile
from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                       QgsVectorFileWriter, QgsCoordinateTransformContext, QgsProject)
from qgis.PyQt.QtCore import QCoreApplication
hooked = []
sys.excepthook = lambda t, v, tb: hooked.append(f"{{t.__name__}}: {{v}}")
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker

tmp = tempfile.mkdtemp(prefix="rl_slotexc_")
mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(20)", "m", "memory")
mem.startEditing()
f = QgsFeature(mem.fields()); f.setAttributes(["S1", "AVANT"])
f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1, 1))); mem.addFeature(f)
mem.commitChanges()
path = os.path.join(tmp, "s.gpkg")
opts = QgsVectorFileWriter.SaveVectorOptions(); opts.driverName = "GPKG"; opts.layerName = "pts"
QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
lyr = QgsVectorLayer(path + "|layername=pts", "s", "ogr")
QgsProject.instance().addMapLayer(lyr)
project = os.path.join(tmp, "p.qgz"); open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, tmp)
wq = WriteQueue(); wq.start(jm.path)
tracker = EditSessionTracker(wq, jm)
statuses = []
if hasattr(tracker, "set_capture_status_callback"):
    tracker.set_capture_status_callback(lambda k, det: statuses.append(k))
tracker.activate(); tracker.connect_layer(lyr)

target = {target!r}
original = getattr(tracker, target)
def boom(*a, **k):
    raise RuntimeError("injected failure in " + target)
setattr(tracker, target, boom)

fid = next(lyr.getFeatures()).id()
lyr.startEditing()
lyr.changeAttributeValue(fid, lyr.fields().indexFromName("nom"), "APRES")
ok = bool(lyr.commitChanges())
for _ in range(20):
    QCoreApplication.processEvents(); time.sleep(0.02)
check = QgsVectorLayer(path + "|layername=pts", "c", "ogr")
on_disk = [x["nom"] for x in check.getFeatures()]
out = {{"commit": ok, "on_disk": on_disk, "statuses": statuses,
        "capture_failures": int(getattr(tracker, "capture_failures", 0) or 0),
        "hooked": hooked}}
setattr(tracker, target, original)
tracker.deactivate(); tracker.disconnect_all(); wq.stop(); jm.close()
print("OUTCOME " + json.dumps(out))
sys.stdout.flush()
os._exit(0)
'''

_CASES = (
    ("K1", "_on_before_commit"),
    ("K2", "_on_committed_attribute_values_changes"),
    ("K3", "_on_after_commit"),
)


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_capture_slot_exception setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.crash_harness import run_child
    d = ctx.data
    for key, target in _CASES:
        rc, so, se = run_child(_CHILD.format(target=target), timeout=120)
        line = next((ln for ln in so.splitlines() if ln.startswith("OUTCOME ")), "")
        try:
            outcome = json.loads(line[len("OUTCOME "):]) if line else None
        except ValueError:
            outcome = None
        d[key] = {"rc": rc, "outcome": outcome, "stderr_tail": se[-400:] if rc else ""}
    flog(f"tx_capture_slot_exception run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:3000]}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    for key, target in _CASES:
        c = d.get(key) or {}
        o = c.get("outcome") or {}
        out.append((
            f"{key.lower()}_exception_does_not_reach_qt",
            c.get("rc") == 0 and o is not None and not o.get("hooked"),
            f"{key} ({target}) : code de sortie={c.get('rc')} exceptions rendues a "
            f"Qt={o.get('hooked')} stderr={c.get('stderr_tail')!r} : sans "
            f"gestionnaire, PyQt6 fait avorter QGIS ; avec celui de QGIS, le slot "
            f"s'arrete a mi-chemin",
        ))
        out.append((
            f"{key.lower()}_user_commit_lands_and_failure_is_told",
            o.get("commit") is True and o.get("on_disk") == ["APRES"]
            and bool(o.get("statuses")) and (o.get("capture_failures") or 0) >= 1,
            f"{key} ({target}) : commit={o.get('commit')} fichier={o.get('on_disk')} "
            f"statuts={o.get('statuses')} echecs de capture={o.get('capture_failures')} "
            f": la donnee a change, l'historique ne le sait pas, il faut le dire",
        ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_capture_slot_exception.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
