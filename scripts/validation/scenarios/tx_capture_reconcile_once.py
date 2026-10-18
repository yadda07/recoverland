"""tx_capture_reconcile_once - RecoverLand validation runtime.

Invariant: I-CRASH-01 (apres un plantage pendant un commit, le
rapprochement dit exactement ce que la source a recu, ni plus ni moins).

Le mecanisme
------------
Au redemarrage qui suit un plantage pendant un commit, le registre garde
le commit INTERRUPTED avec l'etat d'avant de chaque entite. Chaque
``connect_layer`` d'une couche de la meme source lance le rapprochement
(core/crash_reconcile.py) : il lit le registre, compare a la source,
fabrique les evenements de ce qui a atterri et les confie a la file
d'ecriture avec l'etat RECONCILED. Deux trous :

* le registre ne passe a RECONCILED que lorsque le thread d'ecriture a
  valide le lot. Une deuxieme couche sur la meme table (deux styles de la
  meme couche, une couche filtree et une couche complete : courant) ou une
  couche reconnectee (source reparee) lit encore INTERRUPTED et refait le
  meme rapprochement. Rien ne disait, dans le processus, qu'un commit
  etait deja repris ;
* chaque evenement reconstruit recevait un ``event_uid`` tire au hasard :
  ``ON CONFLICT(event_uid)`` ne pouvait pas absorber une deuxieme arrivee.
  Un lot de rapprochement reste dans l'outbox (QGIS arrete avant que le
  thread d'ecriture l'ait indexe), un redemarrage dont la relecture de
  l'outbox echoue (journal verrouille par un autre processus a ce moment)
  refait le rapprochement, puis la relecture suivante insere aussi le
  premier lot.

Ce que l'utilisateur voit : l'historique montre chaque changement deux
fois (une fois par couche connectee), une alerte "rapprochement" par couche
annonce chacune tout le commit, un rewind rejoue deux fois le meme
changement, et le rapport d'annulation du commit compte des evenements
"deja en place" qui n'ont jamais ete des changements distincts.

Cas
---
O1  GeoPackage, plantage apres l'ecriture (C001 renomme, C005 supprime).
    Au redemarrage, deux couches sur la meme table sont connectees, puis la
    premiere est deconnectee et reconnectee. Attendu : exactement un
    evenement par changement (un UPDATE de C001, un DELETE de C005), une
    seule alerte "rapprochement" (landed=2), registre RECONCILED landed=2,
    et annuler le commit (restauration de tous ses evenements) applique
    chacun une fois, sans echec ni "deja en place", et rend le fichier
    d'origine.
O2  meme plantage. Redemarrage B : le rapprochement ecrit son lot dans
    l'outbox et QGIS s'arrete avant que le lot soit indexe (point de faute
    ``wq_after_outbox_before_queue``). Redemarrage C : un autre processus
    tient le journal pendant la relecture de l'outbox (lot de B garde),
    puis la couche est connectee (rapprochement). Redemarrage D : la
    relecture integre le lot de B. Attendu : toujours exactement un
    evenement par changement ; la relecture de D trouve les deux evenements
    deja presents.

Verdict mesure avant correctif : premiere version, sur le worktree avant
la modification : FAIL passed=4/7 (O1 : 6 evenements, 3 alertes ; O2 : 4
evenements, la relecture de D en insere 2) ; l'annulation passait alors,
restore_batch comptant les doublons "deja en place" (4 sur 6). Version
finale (annulation resserree : aucun "deja en place"), correctif retire
en memoire dans ce processus et dans les processus enfants : FAIL passed=3/7.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_reconcile_once"
INVARIANT = "I-CRASH-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_INITIAL = {"C001": "nom 1", "C002": "nom 2", "C003": "nom 3",
            "C004": "nom 4", "C005": "nom 5"}


def _make_gpkg(path: str) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    for i in range(1, 6):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("code", f"C00{i}")
        feat.SetField("nom", f"nom {i}")
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(feat)
    ds = None


def _layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


def _pump(seconds: float = 0.0) -> None:
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + seconds
    while True:
        QCoreApplication.processEvents()
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def _file_state(path: str) -> dict:
    return {f["code"]: f["nom"] for f in _layer(path, "o_state").getFeatures()}


def _journal(journal_path: str) -> dict:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        events = [{"eid": eid, "op": op, "ident": ident, "commit": cid, "uid": uid}
                  for eid, op, ident, cid, uid in conn.execute(
                      "SELECT event_id, operation_type, feature_identity_json, "
                      "commit_id, event_uid FROM audit_event "
                      "WHERE restored_from_event_id IS NULL ORDER BY event_id")]
        ledger = [{"commit": c, "state": s, "detail": d} for c, s, d in conn.execute(
            "SELECT commit_id, state, detail FROM capture_commit ORDER BY recorded_at")]
    finally:
        conn.close()
    for ev in events:
        try:
            ev["fid"] = json.loads(ev.pop("ident") or "{}").get("fid")
        except ValueError:
            ev["fid"] = None
    return {"events": events, "ledger": ledger}


def _stack(workdir: str):
    from recoverland.core.journal_manager import JournalManager
    jm = JournalManager()
    jm.open_for_project(os.path.join(workdir, "p.qgz"), workdir)
    return jm


def _tracker(wq, jm, statuses):
    from recoverland.core.edit_tracker import EditSessionTracker
    tr = EditSessionTracker(wq, jm)
    tr.set_capture_status_callback(lambda k, d: statuses.append((k, dict(d))))
    tr.activate()
    return tr


def _drain_and_stop(wq, tr) -> None:
    _pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()


def _reconciled(statuses) -> list:
    return [{k: d.get(k) for k in ("layer_name", "commits", "landed",
                                   "not_applied", "unresolved")}
            for k, d in statuses if k == "reconciled"]


_CHILD_CRASH = r'''
from qgis.core import QgsVectorLayer
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "o.gpkg")
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
wq = WriteQueue(); wq.start(jm.path)
tr = EditSessionTracker(wq, jm); tr.activate()
layer = QgsVectorLayer(gpkg + "|layername=pts", "pts", "ogr")
tr.connect_layer(layer)
fids = {{f["code"]: f.id() for f in layer.getFeatures()}}
layer.startEditing()
layer.changeAttributeValue(fids["C001"], layer.fields().indexFromName("nom"), "renomme")
layer.deleteFeature(fids["C005"])
print("COMMIT", layer.commitChanges(), flush=True)
os._exit(0)
'''

# Restart B of O2: the reconciliation hands its batch over and QGIS stops
# before the writer thread indexes it.
_CHILD_RECONCILE = r'''
from qgis.core import QgsVectorLayer
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "o.gpkg")
jm = JournalManager(); jm.open_for_project(os.path.join(workdir, "p.qgz"), workdir)
wq = WriteQueue(); wq.start(jm.path)
tr = EditSessionTracker(wq, jm); tr.activate()
tr.connect_layer(QgsVectorLayer(gpkg + "|layername=pts", "pts_b", "ogr"))
print("NO_FAULT", flush=True)
os._exit(0)
'''


def _crash(ctx, key: str) -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], key)
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "o.gpkg")
    _make_gpkg(gpkg)
    fids = {f["code"]: f.id() for f in _layer(gpkg, f"{key}_fix").getFeatures()}
    rc, _so, se = run_child(_CHILD_CRASH.format(workdir=work),
                            fault="capture_after_provider_commit",
                            timeout=180, workdir=work)
    return {"work": work, "gpkg": gpkg, "fids": fids, "rc": rc,
            "fault_reached": rc == FAULT_EXIT_CODE,
            "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else "",
            "file_after_crash": _file_state(gpkg)}


def _undo_commit(res: dict) -> None:
    """Restore every event of the commit, newest first: undoing the commit."""
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_batch
    conn = sqlite3.connect(f"file:{Path(res['journal']).as_posix()}?mode=ro", uri=True)
    try:
        events = [get_event_by_id(conn, ev["eid"])
                  for ev in sorted(res["events"], key=lambda e: -e["eid"])]
        rep = restore_batch(_layer(res["gpkg"], "o_undo"), events, journal=conn)
    finally:
        conn.close()
    res["undo"] = {"requested": rep.total_requested,
                   "succeeded": sorted(rep.succeeded),
                   "failed": {str(k): v for k, v in (rep.failed or {}).items()},
                   "skipped": sorted(rep.skipped_idempotent or [])}
    res["file_after_undo"] = _file_state(res["gpkg"])


def _case_o1(ctx) -> dict:
    from recoverland.core.write_queue import WriteQueue
    res = _crash(ctx, "o1")
    jm = _stack(res["work"])
    wq = WriteQueue()
    wq.start(jm.path)
    statuses = []
    tr = _tracker(wq, jm, statuses)
    layer_a = _layer(res["gpkg"], "pts_a")
    tr.connect_layer(layer_a)
    tr.connect_layer(_layer(res["gpkg"], "pts_b"))
    # The data source of pts_a repaired: disconnected, connected again.
    tr.disconnect_layer(layer_a)
    tr.connect_layer(layer_a)
    _drain_and_stop(wq, tr)
    res["journal"] = jm.path
    jm.close()
    res["reconciled"] = _reconciled(statuses)
    res.update(_journal(res["journal"]))
    _undo_commit(res)
    return res


def _case_o2(ctx) -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    from recoverland.core.write_queue import WriteQueue
    res = _crash(ctx, "o2")
    rc, so, se = run_child(_CHILD_RECONCILE.format(workdir=res["work"]),
                           fault="wq_after_outbox_before_queue",
                           timeout=180, workdir=res["work"])
    res["b"] = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
                "stdout": so.strip()[-200:],
                "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else ""}
    jm = _stack(res["work"])
    res["journal"] = jm.path
    res["after_b"] = _journal(jm.path)
    # Restart C: another process holds the journal while the outbox is
    # replayed, then lets it go.
    lock = sqlite3.connect(jm.path, isolation_level=None, timeout=1.0)
    lock.execute("BEGIN EXCLUSIVE")
    wq = WriteQueue()
    try:
        wq.start(jm.path)
    finally:
        lock.execute("ROLLBACK")
        lock.close()
    rep = getattr(wq, "last_replay_report", None)
    res["replay_c"] = rep.as_dict() if rep is not None else None
    res["replay_c_error"] = str(getattr(wq, "_last_error", "") or "")
    statuses = []
    tr = _tracker(wq, jm, statuses)
    tr.connect_layer(_layer(res["gpkg"], "pts_c"))
    _drain_and_stop(wq, tr)
    res["reconciled"] = _reconciled(statuses)
    res["after_c"] = _journal(jm.path)
    # Restart D: the replay finally integrates B's batch.
    wq2 = WriteQueue()
    wq2.start(jm.path)
    rep = getattr(wq2, "last_replay_report", None)
    res["replay_d"] = rep.as_dict() if rep is not None else None
    wq2.stop()
    jm.close()
    res.update(_journal(res["journal"]))
    return res


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_c_once_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("o1", _case_o1), ("o2", _case_o2)):
        try:
            ctx.data[key] = fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-900:]}
        flog(f"{SCENARIO_ID} case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:3000]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _per_change(res: dict) -> list:
    """(op, code) of every event of the crashed commit, sorted."""
    by_fid = {fid: code for code, fid in (res.get("fids") or {}).items()}
    return sorted((ev["op"], by_fid.get(ev["fid"], ev["fid"]))
                  for ev in res.get("events") or [])


_EXPECTED = [("DELETE", "C005"), ("UPDATE", "C001")]
_AFTER_CRASH = {"C001": "renomme", "C002": "nom 2", "C003": "nom 3", "C004": "nom 4"}


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    o1, o2 = ctx.data.get("o1", {}), ctx.data.get("o2", {})

    # ===== O1: two layers on one source, then a reconnection ===========
    out.append((
        "o1_fixture_crash_after_the_provider_wrote",
        o1.get("fault_reached") is True and o1.get("file_after_crash") == _AFTER_CRASH,
        f"rc={o1.get('rc')} fichier={o1.get('file_after_crash')} stderr="
        f"{o1.get('stderr')} crash={o1.get('crash')} {o1.get('trace', '')}",
    ))
    ledger1 = o1.get("ledger") or []
    out.append((
        "o1_one_event_per_change_whatever_the_layers_on_the_source",
        _per_change(o1) == _EXPECTED
        and len({ev["commit"] for ev in o1.get("events") or []}) == 1
        and [row["state"] for row in ledger1] == ["RECONCILED"]
        and "landed=2 " in (ledger1[0]["detail"] if ledger1 else ""),
        f"evenements={_per_change(o1)} attendu {_EXPECTED} ; registre={ledger1}. "
        f"Chaque couche connectee sur la meme table refaisait le rapprochement : "
        f"l'historique montre chaque changement deux fois et un rewind les "
        f"rejouerait deux fois",
    ))
    rec1 = o1.get("reconciled") or []
    out.append((
        "o1_one_reconciliation_alert",
        len(rec1) == 1 and rec1[0].get("landed") == 2
        and rec1[0].get("not_applied") == 0 and rec1[0].get("unresolved") == 0,
        f"alertes={rec1} attendu une seule, landed=2. Deux alertes annoncent "
        f"chacune tout le commit : l'utilisateur croit a deux commits repris",
    ))
    undo = o1.get("undo") or {}
    out.append((
        "o1_undoing_the_commit_succeeds_on_every_event",
        bool(o1.get("events"))
        and undo.get("requested") == len(o1.get("events"))
        and not undo.get("failed") and not undo.get("skipped")
        and o1.get("file_after_undo") == _INITIAL,
        f"annulation={undo} ; fichier apres={o1.get('file_after_undo')} attendu "
        f"{_INITIAL}, chaque evenement applique une fois, aucun 'deja en place'. "
        f"Avec des doublons, le rapport d'annulation d'un commit de 2 changements "
        f"compte des evenements 'deja en place' qui n'ont jamais ete des "
        f"changements distincts",
    ))

    # ===== O2: a second arrival after a restart =========================
    b = o2.get("b") or {}
    replay_c = o2.get("replay_c") or {}
    after_b = (o2.get("after_b") or {}).get("ledger") or []
    out.append((
        "o2_fixture_batch_left_in_the_outbox_and_replay_blocked",
        o2.get("fault_reached") is True and b.get("fault_reached") is True
        and [r["state"] for r in after_b] == ["INTERRUPTED"]
        and ((int(replay_c.get("kept_segments") or 0) >= 1
              and int(replay_c.get("events_inserted") or 0) == 0)
             or "replay" in (o2.get("replay_c_error") or "")),
        f"crash={o2.get('rc')} B={b} registre apres B={after_b} relecture C="
        f"{replay_c} erreur={o2.get('replay_c_error')} crash={o2.get('crash')} "
        f"{o2.get('trace', '')}",
    ))
    replay_d = o2.get("replay_d") or {}
    out.append((
        "o2_second_arrival_after_a_restart_is_a_no_op",
        _per_change(o2) == _EXPECTED
        and [r["state"] for r in o2.get("ledger") or []] == ["RECONCILED"]
        and int(replay_d.get("events_inserted") or 0) == 0
        and int(replay_d.get("events_already_present") or 0) == 2,
        f"evenements={_per_change(o2)} attendu {_EXPECTED} ; relecture D="
        f"{replay_d} ; alertes C={o2.get('reconciled')} ; registre="
        f"{o2.get('ledger')}. Le lot du premier rapprochement, relu apres le "
        f"second, portait d'autres event_uid : chaque changement arrive deux "
        f"fois dans l'historique",
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
