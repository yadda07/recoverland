"""tx_capture_crash_points - RecoverLand validation runtime.

Invariant: I-CRASH-01 (apres un plantage a n'importe quel point de la
capture, on sait sans ambiguite ce qui est au journal, ce qui attendait,
ce qui a ete recupere et ce qui est perdu) et I-CAPTURE-01 (aucune
modification ecrite par le provider ne disparait du journal).

Methode
-------
Un processus enfant QGIS (crash_harness) ouvre un vrai GeoPackage, un vrai
journal, la vraie WriteQueue et le vrai tracker, puis commit une session :
fid 1 renomme et deplace, fid 2 supprime, une entite ajoutee. La variable
RECOVERLAND_FAULT le tue par os._exit a un point precis : ni finally, ni
atexit, ni destructeur, comme un vrai plantage. Le parent redemarre ensuite
comme le ferait QGIS (rejeu de l'outbox, puis branchement de la couche qui
declenche la reconciliation des commits interrompus) et lit le journal.

Points de crash
---------------
none                          temoin : le commit complet est journalise
capture_after_prepare         l'etat d'avant est durable, le provider n'a
                              encore rien ecrit : le commit doit etre
                              INTERRUPTED puis RECONCILED sans evenement,
                              3 operations comptees non appliquees
                              (rien n'a atterri, et on le SAIT)
capture_after_provider_commit le fichier est modifie, aucun evenement n'est
                              encore emis : le pire cas. Avant le correctif :
                              perte totale et indetectable. Attendu : le
                              commit est reconstruit depuis le provider,
                              UPDATE + DELETE + INSERT journalises
wq_after_outbox_before_queue  evenements durables dans l'outbox, pas encore
                              en file ni indexes : integres au redemarrage,
                              exactement une fois
wq_before_sqlite_commit       idem, crash dans le thread ecrivain

Verdict attendu avant correctif : FAIL (pas d'outbox ni de prepare : les
trois derniers cas perdent tout).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_crash_points"
INVARIANT = "I-CRASH-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_CHILD = r'''
from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
    QgsVectorFileWriter, QgsCoordinateTransformContext)
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "data.gpkg")
mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)", "m", "memory")
mem.startEditing()
for i, (c, n) in enumerate([("C001", "PONT"), ("C002", "GARE"), ("C003", "ECOLE")]):
    f = QgsFeature(mem.fields()); f.setAttributes([c, n])
    f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0))); mem.addFeature(f)
mem.commitChanges()
opts = QgsVectorFileWriter.SaveVectorOptions(); opts.driverName = "GPKG"; opts.layerName = "pts"
QgsVectorFileWriter.writeAsVectorFormatV3(mem, gpkg, QgsCoordinateTransformContext(), opts)
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
print("JOURNAL", jm.path, flush=True)
wq = WriteQueue(); wq.start(jm.path)
if os.environ.get("RL_PATCH_CRASH") == "after_provider_commit":
    # Version-independent crash: the provider commit is done, the slot that
    # would journal it kills the process. Works on code without fault points.
    EditSessionTracker._on_after_commit = lambda self, lid: os._exit(86)
tr = EditSessionTracker(wq, jm); tr.activate()
layer = QgsVectorLayer(gpkg + "|layername=pts", "pts", "ogr")
tr.connect_layer(layer)
fids = sorted(f.id() for f in layer.getFeatures())
layer.startEditing()
layer.changeAttributeValue(fids[0], layer.fields().indexFromName("nom"), "PONT_CRASH")
layer.changeGeometry(fids[0], QgsGeometry.fromPointXY(QgsPointXY(11.0, 11.0)))
layer.deleteFeature(fids[1])
nf = QgsFeature(layer.fields())
attrs = [None] * layer.fields().count()
attrs[layer.fields().indexFromName("code")] = "C009"
attrs[layer.fields().indexFromName("nom")] = "AJOUT_CRASH"
nf.setAttributes(attrs); nf.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)))
layer.addFeature(nf)
ok = layer.commitChanges()
print("COMMIT", ok, flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _provider_state(gpkg: str) -> dict:
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{gpkg}|layername=pts", "probe", "ogr")
    out = {}
    for f in lyr.getFeatures():
        pt = f.geometry().asPoint()
        out[f["code"]] = (f["nom"], (round(pt.x(), 3), round(pt.y(), 3)))
    return out


def _journal_facts(journal: str) -> dict:
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        ops = [r[0] for r in conn.execute(
            "SELECT operation_type FROM audit_event "
            "WHERE restored_from_event_id IS NULL ORDER BY event_id")]
        try:
            dups = conn.execute(
                "SELECT COUNT(*) FROM (SELECT event_uid FROM audit_event WHERE "
                "event_uid IS NOT NULL GROUP BY event_uid HAVING COUNT(*) > 1)"
            ).fetchone()[0]
        except sqlite3.Error:
            dups = -1       # journal without event_uid (pre-v7)
        try:
            ledger = [(r[0], r[1]) for r in conn.execute(
                "SELECT state, detail FROM capture_commit ORDER BY recorded_at")]
        except sqlite3.Error:
            ledger = []     # journal without the commit ledger (pre-v7)
        attrs = [r[0] for r in conn.execute("SELECT attributes_json FROM audit_event")]
    finally:
        conn.close()
    return {"ops": sorted(ops), "dups": dups, "ledger": ledger,
            "mentions_crash_value": any("PONT_CRASH" in (a or "") for a in attrs)}


def _restart_and_reconnect(workdir: str) -> dict:
    """What QGIS does at the next start: open, replay, connect the layer."""
    from qgis.core import QgsVectorLayer
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from scripts.validation.plugin_harness import pump
    project = os.path.join(workdir, "p.qgz")
    jm = JournalManager()
    jm.open_for_project(project, workdir)
    wq = WriteQueue()
    wq.start(jm.path)
    report = getattr(wq, "last_replay_report", None)
    replay = report.as_dict() if report is not None else None
    statuses = []
    tr = EditSessionTracker(wq, jm)
    if hasattr(tr, "set_capture_status_callback"):
        tr.set_capture_status_callback(lambda k, d: statuses.append((k, d)))
    tr.activate()
    layer = QgsVectorLayer(os.path.join(workdir, "data.gpkg") + "|layername=pts", "pts", "ogr")
    tr.connect_layer(layer)
    pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    else:
        time.sleep(1.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()
    path = jm.path
    jm.close()
    return {"journal": path, "replay": replay,
            "statuses": [(k, {kk: vv for kk, vv in d.items()
                              if kk in ("landed", "not_applied", "unresolved")})
                         for k, d in statuses]}


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_tx_crash_")
    flog(f"tx_capture_crash_points setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    results = {}
    for point in ("none", "capture_after_prepare", "capture_after_provider_commit",
                  "patched_after_provider_commit",
                  "wq_after_outbox_before_queue", "wq_before_sqlite_commit"):
        workdir = os.path.join(ctx.data["tmpdir"], point)
        os.makedirs(workdir, exist_ok=True)
        patched = point == "patched_after_provider_commit"
        rc, so, se = run_child(_CHILD.format(workdir=workdir),
                               fault="" if (point == "none" or patched) else point,
                               timeout=180, workdir=workdir,
                               extra_env={"RL_PATCH_CRASH": "after_provider_commit"}
                               if patched else None)
        res = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
               "stdout": [ln for ln in so.splitlines()
                          if ln.startswith(("JOURNAL", "COMMIT"))],
               "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else ""}
        try:
            res["provider"] = _provider_state(os.path.join(workdir, "data.gpkg"))
            restart = _restart_and_reconnect(workdir)
            res.update(restart)
            res["journal_facts"] = _journal_facts(restart["journal"])
        except Exception as exc:  # noqa: BLE001 - reported as a failure
            import traceback
            res["crash"] = f"{type(exc).__name__}: {exc}"
            res["trace"] = traceback.format_exc()[-800:]
        results[point] = res
        flog(f"tx_capture_crash_points point={point} trace_id={ctx.trace_id} "
             f"result={json.dumps(res, default=str)[:1500]}", "INFO")
    ctx.data["results"] = results
    shutil.rmtree(ctx.data["tmpdir"], ignore_errors=True)


def _landed(provider: dict) -> bool:
    return (provider.get("C001", (None,))[0] == "PONT_CRASH"
            and "C002" not in provider and "C009" in provider)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    r = ctx.data.get("results", {})

    ctrl = r.get("none", {})
    out.append((
        "control_commit_is_journalled",
        (ctrl.get("journal_facts") or {}).get("ops") == ["DELETE", "INSERT", "UPDATE"],
        f"temoin sans crash : {ctrl.get('journal_facts')} crash={ctrl.get('crash')} "
        f"{ctrl.get('trace', '')} stderr={ctrl.get('stderr')}",
    ))

    pre = r.get("capture_after_prepare", {})
    pf = pre.get("journal_facts") or {}
    out.append((
        "after_prepare_nothing_landed_and_it_is_known",
        pre.get("fault_reached") is True and not _landed(pre.get("provider") or {})
        and pf.get("ops") == [] and any(st == "RECONCILED" for st, _d in pf.get("ledger") or [])
        and any("not_applied=3" in (d or "") for _s, d in pf.get("ledger") or []),
        f"crash apres prepare : provider={pre.get('provider')} journal={pf} "
        f"replay={pre.get('replay')} statuts={pre.get('statuses')} "
        f"crash={pre.get('crash')}",
    ))

    mid = r.get("capture_after_provider_commit", {})
    mf = mid.get("journal_facts") or {}
    out.append((
        "after_provider_commit_every_landed_change_is_recovered",
        mid.get("fault_reached") is True and _landed(mid.get("provider") or {})
        and mf.get("ops") == ["DELETE", "INSERT", "UPDATE"]
        and mf.get("mentions_crash_value") is True
        and any(st == "RECONCILED" for st, _d in mf.get("ledger") or []),
        f"crash entre le commit provider et l'emission des evenements : "
        f"provider={mid.get('provider')} journal={mf} replay={mid.get('replay')} "
        f"statuts={mid.get('statuses')} crash={mid.get('crash')}. C'est la "
        f"fenetre ou le fichier est modifie et ou rien n'est encore journalise.",
    ))

    pat = r.get("patched_after_provider_commit", {})
    patf = pat.get("journal_facts") or {}
    out.append((
        "patched_crash_after_provider_commit_loses_nothing",
        pat.get("fault_reached") is True and _landed(pat.get("provider") or {})
        and patf.get("ops") == ["DELETE", "INSERT", "UPDATE"],
        f"meme crash, injecte sans point de faute (valable sur toute version) : "
        f"provider={pat.get('provider')} journal={patf} crash={pat.get('crash')} "
        f"{pat.get('trace', '')}",
    ))

    for point in ("wq_after_outbox_before_queue", "wq_before_sqlite_commit"):
        res = r.get(point, {})
        facts = res.get("journal_facts") or {}
        out.append((
            f"{point}_events_integrated_exactly_once",
            res.get("fault_reached") is True
            and facts.get("ops") == ["DELETE", "INSERT", "UPDATE"]
            and facts.get("dups") == 0
            and any(st == "COMMITTED" for st, _d in facts.get("ledger") or []),
            f"crash a {point} : journal={facts} replay={res.get('replay')} "
            f"crash={res.get('crash')}",
        ))

    out.append(assert_log_contains(
        ctx.records, rf"tx_capture_crash_points.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=5))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
