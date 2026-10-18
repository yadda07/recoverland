"""tx_capture_schema_version_stamp - RecoverLand validation runtime.

Invariant: I-CAPTURE-04 (chaque evenement ecrit par la capture porte
event_schema_version = CURRENT_SCHEMA_VERSION).

Le mecanisme
------------
Depuis le schema 7, une geometrie qui apparait ou disparait est ecrite avec
le marqueur explicite ``GEOMETRY_NULL_MARKER`` (BLOB vide). Aucune version
anterieure n'ecrivait de BLOB vide : les consommateurs (restore, rewind,
review) ne lisent donc ce BLOB comme "pas de geometrie" QUE si l'evenement
porte ``event_schema_version >= 7`` (`constants.is_null_geometry_marker`) ;
sinon c'est une corruption, et la restauration est refusee. Un chemin de
capture qui oublierait le tampon, ou le perdrait en route (commit partiel,
reconstruction apres plantage, rejeu de l'outbox), rendrait chaque
restauration d'une geometrie ajoutee ou effacee impossible, avec un
message de corruption la ou la donnee est saine.

Cas
---
C1  commit complet sur un GeoPackage : INSERT saisi comme par le
    formulaire, UPDATE attribut, deplacement, geometrie effacee, geometrie
    ajoutee a une entite qui n'en avait pas, DELETE. Tout evenement porte 7
    et les deux transitions NULL sont reconnues comme marqueur.
C2  commit partiel (index UNIQUE refuse l'ajout) : les evenements des
    etapes ecrites portent 7.
C3  plantage de QGIS juste apres l'ecriture du commit (processus enfant) :
    les evenements reconstruits au redemarrage portent 7. Porter 7 engage
    a respecter le contrat v7 : le commit interrompu efface aussi la
    geometrie de C003 et en donne une a C004. Le rapprochement
    (core/crash_reconcile.py) ecrivait le cote NULL en None, lu en v7
    "geometrie inchangee" (mesure par la revue : restaurer C004 repond
    'Reverted' et laisse Point (40 40), un faux succes ; C003 est refuse
    faute d'etat alors que le journal tient sa geometrie). Attendu : le
    marqueur des deux cotes NULL, et une restauration qui remet reellement
    la geometrie de C003 et retire celle de C004. Et le renommage de C001
    (attribut seul, geometrie jamais lue) reconstruit sans transition de
    geometrie, comme la capture directe l'ecrit.
C4  garde structurelle : chaque construction d'AuditEvent de la capture
    (tracker et rapprochement apres plantage) passe
    ``event_schema_version=CURRENT_SCHEMA_VERSION``, et cette constante vaut
    la version depuis laquelle le marqueur existe.

Verdict attendu : C1, C2, C4 et le tampon de C3 passent sur le code v7 ;
les transitions NULL de C3 echouent tant que le rapprochement ecrit None
(un FAIL designe le chemin fautif). Le code d'origine (schema 6, pas de
marqueur) echoue.
"""
from __future__ import annotations

import ast
import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_schema_version_stamp"
INVARIANT = "I-CAPTURE-04"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(path: str, unique_code: bool = False) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    statut = ogr.FieldDefn("statut", ogr.OFTString)
    statut.SetDefault("'AUTO'")
    lyr.CreateField(statut)
    for i in range(1, 6):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("code", f"C00{i}")
        if i != 4:      # C004 starts without geometry
            feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(feat)
    ds = None
    if unique_code:
        con = sqlite3.connect(path)
        con.execute("CREATE UNIQUE INDEX ux_pts_code ON pts(code)")
        con.commit()
        con.close()


def _layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


class _Stack:
    def __init__(self, workdir: str):
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        os.makedirs(workdir, exist_ok=True)
        self.jm = JournalManager()
        self.jm.open_for_project(
            project_path=os.path.join(workdir, "audit.qgz"), profile_path=workdir)
        self.path = self.jm.path
        self.wq = WriteQueue()
        self.wq.start(self.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()

    def shutdown(self) -> None:
        try:
            self.tracker.deactivate()
            self.tracker.disconnect_all()
        finally:
            self.wq.stop()
            self.jm.close()


def _pump(seconds: float = 0.0) -> None:
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + seconds
    while True:
        QCoreApplication.processEvents()
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def _rows(journal_path: str) -> list:
    from recoverland.core.constants import is_null_geometry_marker
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT event_id, operation_type, event_schema_version, geometry_wkb, "
            "new_geometry_wkb, feature_identity_json FROM audit_event "
            "WHERE restored_from_event_id IS NULL ORDER BY event_id").fetchall()
    finally:
        conn.close()
    out = []
    for eid, op, version, old, new, ident in rows:
        try:
            fid = json.loads(ident or "{}").get("fid")
        except ValueError:
            fid = None
        out.append({"eid": eid, "op": op, "version": version, "fid": fid,
                    "old_len": None if old is None else len(old),
                    "new_len": None if new is None else len(new),
                    "marker_old": is_null_geometry_marker(old, version),
                    "marker_new": is_null_geometry_marker(new, version),
                    "empty_blob": (old is not None and len(old) == 0)
                    or (new is not None and len(new) == 0)})
    return out


def _by_code(layer) -> dict:
    return {f["code"]: f.id() for f in layer.getFeatures()}


def _case_c1(ctx) -> dict:
    from qgis.core import QgsGeometry, QgsPointXY, QgsVectorLayerUtils
    work = os.path.join(ctx.data["tmpdir"], "c1")
    stack = _Stack(work)
    path = os.path.join(work, "c1.gpkg")
    _make_gpkg(path)
    layer = _layer(path, "c1")
    stack.tracker.connect_layer(layer)
    fids = _by_code(layer)
    idx_code = layer.fields().indexFromName("code")
    layer.startEditing()
    layer.changeAttributeValue(fids["C001"], idx_code, "C001_EDIT")
    layer.changeGeometry(fids["C002"], QgsGeometry.fromPointXY(QgsPointXY(20, 20)))
    layer.changeGeometry(fids["C003"], QgsGeometry())
    layer.changeGeometry(fids["C004"], QgsGeometry.fromPointXY(QgsPointXY(40, 40)))
    layer.deleteFeature(fids["C005"])
    feat = QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(9, 9)), {idx_code: "C009"})
    layer.addFeature(feat)
    res = {"commit_ok": bool(layer.commitChanges())}
    _pump(0.3)
    stack.shutdown()
    res["rows"] = _rows(stack.path)
    return res


def _case_c2(ctx) -> dict:
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    work = os.path.join(ctx.data["tmpdir"], "c2")
    stack = _Stack(work)
    path = os.path.join(work, "c2.gpkg")
    _make_gpkg(path, unique_code=True)
    layer = _layer(path, "c2")
    stack.tracker.connect_layer(layer)
    fids = _by_code(layer)
    idx_code = layer.fields().indexFromName("code")
    layer.startEditing()
    layer.changeGeometry(fids["C003"], QgsGeometry())
    layer.changeAttributeValue(fids["C001"], idx_code, "C001_EDIT")
    layer.deleteFeature(fids["C002"])
    dup = QgsFeature(layer.fields())
    attrs = [None] * layer.fields().count()
    attrs[idx_code] = "C005"
    dup.setAttributes(attrs)
    dup.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(9, 9)))
    layer.addFeature(dup)
    res = {"commit_ok": bool(layer.commitChanges())}
    layer.rollBack()
    _pump(0.3)
    stack.shutdown()
    res["rows"] = _rows(stack.path)
    return res


_CHILD = r'''
from qgis.core import QgsVectorLayer, QgsGeometry, QgsPointXY, QgsVectorLayerUtils
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "c3.gpkg")
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
wq = WriteQueue(); wq.start(jm.path)
if os.environ.get("RL_PATCH_CRASH") == "after_provider_commit":
    EditSessionTracker._on_after_commit = lambda self, lid: os._exit(86)
tr = EditSessionTracker(wq, jm); tr.activate()
layer = QgsVectorLayer(gpkg + "|layername=pts", "pts", "ogr")
tr.connect_layer(layer)
fids = {{f["code"]: f.id() for f in layer.getFeatures()}}
idx = layer.fields().indexFromName("code")
layer.startEditing()
layer.changeAttributeValue(fids["C001"], idx, "C001_CRASH")
layer.changeGeometry(fids["C002"], QgsGeometry.fromPointXY(QgsPointXY(22.0, 22.0)))
layer.changeGeometry(fids["C003"], QgsGeometry())
layer.changeGeometry(fids["C004"], QgsGeometry.fromPointXY(QgsPointXY(40.0, 40.0)))
layer.deleteFeature(fids["C005"])
nf = QgsVectorLayerUtils.createFeature(
    layer, QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)), {{idx: "C009"}})
layer.addFeature(nf)
print("COMMIT", layer.commitChanges(), flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _case_c3(ctx) -> dict:
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], "c3")
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "c3.gpkg")
    _make_gpkg(gpkg)
    fids = _by_code(_layer(gpkg, "c3_fixture"))
    import recoverland.core.edit_tracker as et_mod
    has_fault = "capture_after_provider_commit" in Path(et_mod.__file__).read_text(
        encoding="utf-8")
    rc, so, se = run_child(
        _CHILD.format(workdir=work),
        fault="capture_after_provider_commit" if has_fault else "",
        timeout=180, workdir=work,
        extra_env=None if has_fault else {"RL_PATCH_CRASH": "after_provider_commit"})
    res = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
           "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else ""}
    jm = JournalManager()
    jm.open_for_project(os.path.join(work, "p.qgz"), work)
    wq = WriteQueue()
    wq.start(jm.path)
    tr = EditSessionTracker(wq, jm)
    tr.activate()
    tr.connect_layer(_layer(os.path.join(work, "c3.gpkg"), "c3_restart"))
    _pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()
    path = jm.path
    jm.close()
    res["rows"] = _rows(path)
    res["fids"] = fids
    res["file_after_crash"] = _geometries(gpkg)
    # Restore each reconciled NULL transition: the geometry must really
    # come back (C003) or really go away (C004).
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_updated_feature
    res["restores"] = {}
    for code in ("C003", "C004"):
        row = next((r for r in res["rows"]
                    if r["op"] == "UPDATE" and r["fid"] == fids.get(code)), None)
        if row is None:
            continue
        conn = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True)
        try:
            event = get_event_by_id(conn, row["eid"])
        finally:
            conn.close()
        out = restore_updated_feature(_layer(gpkg, f"c3_restore_{code}"), event)
        res["restores"][code] = {k: out.get(k) for k in (
            "success", "identity_level", "reason_code", "message")}
    res["file_after_restores"] = _geometries(gpkg)
    return res


def _geometries(path: str) -> dict:
    """code -> WKT (None: no geometry), read from the file."""
    out = {}
    for f in _layer(path, "c3_geoms").getFeatures():
        g = f.geometry()
        out[f["code"]] = None if g is None or g.isNull() else g.asWkt()
    return out


def _audit_event_calls(path: Path) -> list:
    """Every AuditEvent(...) call of *path*: (line, keyword value source)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        if name != "AuditEvent":
            continue
        kw = next((k for k in node.keywords if k.arg == "event_schema_version"), None)
        out.append((node.lineno, ast.unparse(kw.value) if kw is not None else None))
    return out


def _case_c4(_ctx) -> dict:
    import recoverland.core.edit_tracker as et_mod
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION
    core = Path(et_mod.__file__).resolve().parent
    res = {"current": CURRENT_SCHEMA_VERSION}
    try:
        from recoverland.core.constants import NULL_MARKER_SINCE_SCHEMA
        res["marker_since"] = NULL_MARKER_SINCE_SCHEMA
    except ImportError:
        res["marker_since"] = None
    for name in ("edit_tracker.py", "crash_reconcile.py"):
        path = core / name
        res[name] = _audit_event_calls(path) if path.is_file() else "missing"
    return res


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_k_ver_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("c1", _case_c1), ("c2", _case_c2), ("c3", _case_c3),
                    ("c4", _case_c4)):
        try:
            ctx.data[key] = fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-900:]}
        flog(f"{SCENARIO_ID} case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:2000]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _unstamped(rows, expected) -> list:
    return [(r["eid"], r["op"], r["version"]) for r in rows if r["version"] != expected]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    c1, c2 = ctx.data.get("c1", {}), ctx.data.get("c2", {})
    c3, c4 = ctx.data.get("c3", {}), ctx.data.get("c4", {})
    expected = c4.get("current")

    out.append((
        "c4_current_version_is_the_marker_version",
        expected is not None and expected == c4.get("marker_since") == 7,
        f"CURRENT_SCHEMA_VERSION={expected} NULL_MARKER_SINCE_SCHEMA="
        f"{c4.get('marker_since')} attendu 7 : sinon le marqueur ecrit par la "
        f"capture est lu comme une corruption",
    ))
    bad_calls = {}
    for name in ("edit_tracker.py", "crash_reconcile.py"):
        calls = c4.get(name)
        if not isinstance(calls, list) or not calls:
            bad_calls[name] = calls
            continue
        wrong = [c for c in calls if c[1] != "CURRENT_SCHEMA_VERSION"]
        if wrong:
            bad_calls[name] = wrong
    out.append((
        "c4_every_capture_builder_stamps_the_version",
        not bad_calls,
        f"constructions d'AuditEvent sans event_schema_version="
        f"CURRENT_SCHEMA_VERSION : {bad_calls} ; toutes={c4}",
    ))

    rows1 = c1.get("rows") or []
    ops1 = sorted(r["op"] for r in rows1)
    out.append((
        "c1_committed_events_carry_the_version",
        c1.get("commit_ok") is True
        and ops1 == ["DELETE", "INSERT", "UPDATE", "UPDATE", "UPDATE", "UPDATE"]
        and not _unstamped(rows1, expected),
        f"commit={c1.get('commit_ok')} ops={ops1} non tamponnes="
        f"{_unstamped(rows1, expected)} crash={c1.get('crash')} {c1.get('trace', '')}",
    ))
    markers = [r for r in rows1 if r["marker_old"] or r["marker_new"]]
    raw_empty = [r for r in rows1 if r["empty_blob"]]
    out.append((
        "c1_null_transitions_read_as_marker",
        len(markers) == 2 and len(raw_empty) == 2,
        f"transitions NULL reconnues={[(r['eid'], r['version']) for r in markers]}"
        f" ; BLOB vides ecrits={[(r['eid'], r['version']) for r in raw_empty]} "
        f"attendu 2 et 2 : un BLOB vide non reconnu fait refuser la restauration "
        f"pour corruption",
    ))

    rows2 = c2.get("rows") or []
    out.append((
        "c2_partial_commit_events_carry_the_version",
        c2.get("commit_ok") is False and len(rows2) >= 2
        and not _unstamped(rows2, expected)
        and any(r["marker_new"] for r in rows2),
        f"commit={c2.get('commit_ok')} evenements={rows2} crash={c2.get('crash')} "
        f"{c2.get('trace', '')}",
    ))

    rows3 = c3.get("rows") or []
    out.append((
        "c3_reconciled_events_carry_the_version",
        c3.get("fault_reached") is True and len(rows3) >= 3
        and not _unstamped(rows3, expected),
        f"rc={c3.get('rc')} evenements={rows3} stderr={c3.get('stderr')} "
        f"crash={c3.get('crash')} {c3.get('trace', '')}",
    ))
    fids3 = c3.get("fids") or {}
    upd3 = {code: next((r for r in rows3 if r["op"] == "UPDATE"
                        and r["fid"] == fids3.get(code)), None)
            for code in ("C003", "C004")}
    removed, added = upd3["C003"] or {}, upd3["C004"] or {}
    out.append((
        "c3_reconciled_null_transitions_read_as_marker",
        c3.get("fault_reached") is True
        and (c3.get("file_after_crash") or {}).get("C003") is None
        and (c3.get("file_after_crash") or {}).get("C004") is not None
        and removed.get("marker_new") is True and bool(removed.get("old_len"))
        and added.get("marker_old") is True and bool(added.get("new_len")),
        f"fichier apres plantage={c3.get('file_after_crash')} ; UPDATE C003 "
        f"(geometrie effacee)={removed or None} attendu marker_new ; UPDATE C004 "
        f"(geometrie ajoutee)={added or None} attendu marker_old. Un cote NULL "
        f"ecrit None se lit en v7 'geometrie inchangee' : la restauration "
        f"annonce un succes sans rien changer, ou refuse faute d'etat",
    ))
    renamed = next((r for r in rows3 if r["op"] == "UPDATE"
                    and r["fid"] == fids3.get("C001")), None) or {}
    out.append((
        "c3_reconciled_attribute_change_carries_no_geometry_transition",
        bool(renamed) and renamed.get("old_len") is None
        and renamed.get("new_len") is None,
        f"UPDATE C001 (attribut seul)={renamed or None} attendu old_len=None "
        f"new_len=None comme la capture directe. Son etat 'prepare' n'a pas lu "
        f"la geometrie (NO_GEOMETRY, geometry_captured=False) : le rapprochement "
        f"lit ce None comme 'pas de geometrie' et journalise une geometrie "
        f"apparue qui n'a jamais change",
    ))
    rest3 = c3.get("restores") or {}
    after3 = c3.get("file_after_restores") or {}
    out.append((
        "c3_restoring_a_reconciled_null_transition_really_restores",
        (rest3.get("C003") or {}).get("success") is True
        and (rest3.get("C004") or {}).get("success") is True
        and after3.get("C003") == "Point (3 3)" and after3.get("C004") is None,
        f"restaurations={rest3} ; fichier apres={after3} attendu C003 'Point (3 3)' "
        f"et C004 sans geometrie. Mesure par la revue avant correctif : C004 "
        f"'Reverted' STRONG et garde Point (40 40) (faux succes), C003 refuse "
        f"no_post_state_captured alors que le journal tient sa geometrie",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=4))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
