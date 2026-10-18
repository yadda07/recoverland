"""tx_capture_reconcile_filtered - RecoverLand validation runtime.

Invariant: I-CRASH-01 (apres un plantage pendant un commit, le
rapprochement dit exactement ce que la source a recu, ni plus ni moins).

Le mecanisme
------------
Le rapprochement d'un commit interrompu (core/crash_reconcile.py) lisait la
source a travers le provider de la couche, donc a travers son FILTRE
(Couche > Filtrer). Un filtre est une vue, pas la donnee :

* une entite ajoutee que le filtre cache (saisie sur une couche filtree, la
  nouvelle entite sort aussitot de la vue ; la capture la relit sans le
  filtre au moment du commit) : apres un plantage, la recherche par contenu
  ne la voit pas et la declare "non appliquee". Elle est dans le fichier et
  n'aura jamais d'evenement INSERT : un rewind avant le plantage ne peut
  pas la retirer, et l'utilisateur lit "non appliquee" ;
* Spatialite (et PostgreSQL) appliquent le filtre meme a une lecture par
  FID : une entite qu'une modification fait sortir du filtre est "absente",
  la modification qui a atterri reste indeterminee ;
* pire, une suppression jamais ecrite (plantage avant l'ecriture) sur une
  entite que le filtre de la couche au redemarrage cache : l'entite est lue
  "absente", la suppression est declaree ECRITE et journalisee. Annuler ce
  DELETE fantome recree une entite qui existe deja.

Attendu : le rapprochement lit la source sans le filtre (le meme provider
sans filtre que la recherche de jumeaux et la relecture des ajouts) ; une
lecture par FID d'un second provider n'est retenue que si son FID est la
cle stockee ; sans provider sans filtre, une absence n'est pas prouvee
(indetermine, jamais "ecrit" ni "non applique").

Cas
---
H1  GeoPackage filtre ``code <> 'CACHE'`` : le commit renomme C1 en CACHE et
    ajoute une entite CACHE ; plantage apres l'ecriture ; redemarrage avec
    le meme filtre : 2 ecrites (UPDATE de C1, INSERT de l'ajout).
H2  la meme chose sur Spatialite (filtre applique aux lectures par FID).
H3  Spatialite : suppression de C2 sur la couche sans filtre, plantage
    avant l'ecriture ; au redemarrage la couche porte le filtre
    ``code <> 'C2'`` : non appliquee, aucun DELETE.

Verdict attendu avant correctif : FAIL (H1 ajout non applique, H2 UPDATE
indetermine et ajout non applique, H3 DELETE fantome).
"""
from __future__ import annotations

import inspect
import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_reconcile_filtered"
INVARIANT = "I-CRASH-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_HIDE = "\"code\" <> 'CACHE'"


def _make(path: str, fmt: str) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    if fmt == "gpkg":
        ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(2154)
        lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
        lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
        lyr.CreateField(ogr.FieldDefn("n", ogr.OFTInteger))
        for i in range(1, 4):
            feat = ogr.Feature(lyr.GetLayerDefn())
            feat.SetField("code", f"C{i}")
            feat.SetField("n", i)
            feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
            lyr.CreateFeature(feat)
    else:
        ds = ogr.GetDriverByName("SQLite").CreateDataSource(
            path, options=["SPATIALITE=YES"])
        ds.ExecuteSQL("CREATE TABLE pts (pk INTEGER PRIMARY KEY AUTOINCREMENT, "
                      "code TEXT, n INTEGER)")
        ds.ExecuteSQL("SELECT AddGeometryColumn('pts', 'geom', 2154, 'POINT', 'XY')")
        for i in range(1, 4):
            ds.ExecuteSQL(f"INSERT INTO pts (code, n, geom) VALUES ('C{i}', {i}, "
                          f"MakePoint({i}, {i}, 2154))")
    ds = None


def _open(path, fmt, subset, name):
    from qgis.core import QgsVectorLayer, QgsDataSourceUri
    if fmt == "gpkg":
        layer = QgsVectorLayer(path + "|layername=pts", name, "ogr")
        if subset:
            layer.setSubsetString(subset)
    else:
        uri = QgsDataSourceUri()
        uri.setDatabase(path)
        uri.setDataSource("", "pts", "geom", subset, "pk")
        layer = QgsVectorLayer(uri.uri(), name, "spatialite")
    if not layer.isValid():
        raise RuntimeError("invalid layer " + path)
    return layer


def _rows(path: str, fmt: str) -> list:
    """(code, WKT) of the whole table, without any filter, sorted."""
    layer = _open(path, fmt, "", "h_rows")
    return sorted((f["code"], f.geometry().asWkt()) for f in layer.getFeatures())


def _pump(seconds: float = 0.0) -> None:
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + seconds
    while True:
        QCoreApplication.processEvents()
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def _journal(journal_path: str) -> dict:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        events = [{"op": o, "identity": i, "attrs": a} for o, i, a in conn.execute(
            "SELECT operation_type, feature_identity_json, attributes_json "
            "FROM audit_event WHERE restored_from_event_id IS NULL ORDER BY event_id")]
        ledger = [d for (d,) in conn.execute(
            "SELECT detail FROM capture_commit WHERE state = 'RECONCILED'")]
    finally:
        conn.close()
    return {"events": events, "ledger": ledger}


def _restart(workdir: str, path: str, fmt: str, subset: str) -> dict:
    """What QGIS does at the next start: open, replay, connect the layer."""
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    jm = JournalManager()
    jm.open_for_project(os.path.join(workdir, "p.qgz"), workdir)
    wq = WriteQueue()
    wq.start(jm.path)
    statuses = []
    tr = EditSessionTracker(wq, jm)
    tr.set_capture_status_callback(lambda k, d: statuses.append((k, dict(d))))
    tr.activate()
    tr.connect_layer(_open(path, fmt, subset, "h_restart"))
    _pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()
    journal = jm.path
    jm.close()
    out = {"reconciled": [{k: d.get(k) for k in ("landed", "not_applied", "unresolved")}
                          for k, d in statuses if k == "reconciled"]}
    out.update(_journal(journal))
    return out


_CHILD = r'''
from qgis.core import QgsGeometry, QgsPointXY, QgsVectorLayerUtils
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
wq = WriteQueue(); wq.start(jm.path)
tr = EditSessionTracker(wq, jm); tr.activate()
layer = _open({path!r}, {fmt!r}, {subset!r}, "pts")
tr.connect_layer(layer)
fids = {{f["code"]: f.id() for f in layer.getFeatures()}}
idx = layer.fields().indexFromName("code")
layer.startEditing()
if {mode!r} == "hide":
    layer.changeAttributeValue(fids["C1"], idx, "CACHE")
    layer.addFeature(QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)), {{idx: "CACHE"}}))
else:
    layer.deleteFeature(fids["C2"])
print("COMMIT", layer.commitChanges(), flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _case(ctx, key: str, fmt: str, mode: str, fault: str,
          subset_commit: str, subset_restart: str) -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], key)
    os.makedirs(work, exist_ok=True)
    path = os.path.join(work, "h.gpkg" if fmt == "gpkg" else "h.sqlite")
    _make(path, fmt)
    # The child opens the layer with this module's own opener.
    body = inspect.getsource(_open) + _CHILD.format(
        workdir=work, path=path, fmt=fmt, subset=subset_commit, mode=mode)
    rc, so, se = run_child(body, fault=fault, timeout=180, workdir=work)
    res = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
           "stdout": [ln for ln in so.splitlines() if ln.startswith("COMMIT")],
           "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else "",
           "file_after_crash": _rows(path, fmt)}
    res.update(_restart(work, path, fmt, subset_restart))
    res["file_after_restart"] = _rows(path, fmt)
    return res


def _case_h1(ctx) -> dict:
    return _case(ctx, "h1", "gpkg", "hide", "capture_after_provider_commit",
                 _HIDE, _HIDE)


def _case_h2(ctx) -> dict:
    return _case(ctx, "h2", "sqlite", "hide", "capture_after_provider_commit",
                 _HIDE, _HIDE)


def _case_h3(ctx) -> dict:
    return _case(ctx, "h3", "sqlite", "delete", "capture_after_prepare",
                 "", "\"code\" <> 'C2'")


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_c_filt_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("h1", _case_h1), ("h2", _case_h2), ("h3", _case_h3)):
        try:
            ctx.data[key] = fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-900:]}
        flog(f"{SCENARIO_ID} case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:2500]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _totals(res: dict) -> dict:
    rec = res.get("reconciled") or []
    return {k: sum(int(r.get(k) or 0) for r in rec)
            for k in ("landed", "not_applied", "unresolved")}


def _ops(res: dict) -> list:
    return sorted(e["op"] for e in res.get("events") or [])


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    hidden_file = [("C2", "Point (2 2)"), ("C3", "Point (3 3)"),
                   ("CACHE", "Point (1 1)"), ("CACHE", "Point (9 9)")]
    for key, provider in (("h1", "GeoPackage"), ("h2", "Spatialite")):
        res = ctx.data.get(key, {})
        out.append((
            f"{key}_fixture_{provider.lower()}_commit_landed_then_crash",
            res.get("fault_reached") is True
            and res.get("file_after_crash") == hidden_file,
            f"rc={res.get('rc')} stdout={res.get('stdout')} fichier="
            f"{res.get('file_after_crash')} attendu {hidden_file} stderr="
            f"{res.get('stderr')} crash={res.get('crash')} {res.get('trace', '')}",
        ))
        inserts = [e for e in res.get("events") or [] if e["op"] == "INSERT"]
        out.append((
            f"{key}_{provider.lower()}_filter_does_not_hide_what_landed",
            len(res.get("reconciled") or []) == 1
            and _totals(res) == {"landed": 2, "not_applied": 0, "unresolved": 0}
            and _ops(res) == ["INSERT", "UPDATE"]
            and len(inserts) == 1 and '"CACHE"' in (inserts[0].get("attrs") or ""),
            f"rapprochement={res.get('reconciled')} attendu landed=2 ; "
            f"evenements={res.get('events')} ; registre={res.get('ledger')}. Le "
            f"filtre de la couche cache l'ajout et l'entite renommee : lus a "
            f"travers lui, l'ajout ecrit est declare 'non applique' (jamais "
            f"d'INSERT, un rewind ne peut pas le retirer) et le renommage reste "
            f"indetermine",
        ))
    h3 = ctx.data.get("h3", {})
    intact = [("C1", "Point (1 1)"), ("C2", "Point (2 2)"), ("C3", "Point (3 3)")]
    out.append((
        "h3_fixture_delete_never_written",
        h3.get("fault_reached") is True and h3.get("file_after_crash") == intact,
        f"rc={h3.get('rc')} fichier={h3.get('file_after_crash')} stderr="
        f"{h3.get('stderr')} crash={h3.get('crash')} {h3.get('trace', '')}",
    ))
    out.append((
        "h3_feature_hidden_by_the_filter_is_not_taken_for_deleted",
        len(h3.get("reconciled") or []) == 1
        and _totals(h3) == {"landed": 0, "not_applied": 1, "unresolved": 0}
        and not h3.get("events") and h3.get("file_after_restart") == intact,
        f"rapprochement={h3.get('reconciled')} attendu not_applied=1 ; evenements="
        f"{h3.get('events')} ; registre={h3.get('ledger')}. C2 est dans le fichier, "
        f"seulement cache par le filtre de la couche : la lire 'absente' "
        f"journalise une suppression qui n'a jamais eu lieu, et l'annuler "
        f"recree une entite qui existe deja",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=3))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
