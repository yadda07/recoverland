# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_update_provider_values - RecoverLand validation runtime.

Invariant: I-CAPTURE-05 (un UPDATE journalise, cote NEW, ce que le provider
a reellement stocke, pas ce que le tampon d'edition lui a demande).

Le mecanisme
------------
Le cote NEW d'un UPDATE venait des signaux ``committedAttributeValuesChanges``
et ``committedGeometriesChanges`` : ils portent ce que le tampon d'edition a
ENVOYE au provider, pas ce que le provider a ecrit. Mesure sur QGIS 4.2.1 :

* Shapefile, champ N(12,2) : le calculateur de champ ecrit ``"val" / 3``,
  le signal porte 3.3333333333333335, le fichier tient 3.33 ;
* GeoPackage, champ date-heure : le signal porte un QDateTime LOCAL
  (14:30, sans fuseau), le fichier tient l'instant en UTC (12:30Z). Le
  journal ecrivait le texte naif "14:30:00.000", que chaque poste relit
  dans SON fuseau : un poste en UTC le lit 14:30Z, deux heures a cote ;
* Shapefile 2D : un point saisi avec un Z arrive en PointZ dans le signal,
  le fichier tient le point 2D.

Le moteur de restauration prouve la cible d'un UPDATE par son etat d'apres
(cote NEW) lu dans la source : 3.33 ne vaut pas 3.3333333333333335, la
preuve echoue (``post_attr_mismatch``) et la modification ne peut JAMAIS
etre annulee, ni par Restore ni par un Rewind, alors que rien n'a bouge
depuis. Attendu : le cote NEW est relu dans la source par FID, par paquets,
PENDANT l'etape du commit (avant la suppression qui renumerote un
Shapefile) ; si la relecture echoue, la valeur du tampon est gardee et un
statut nomme le dit.

Cas
---
U1  Shapefile N(12,2), calculateur de champ sur 23 entites, relecture par
    paquets de 7 : chaque cote NEW vaut le fichier ; annuler l'une d'elles
    reussit et remet 10+i.
U2  meme couche, le meme commit supprime une entite AVANT celle qui est
    modifiee : apres le repack la modifiee a change de FID. Son cote NEW
    doit etre SA valeur stockee, pas celle de l'entite qui a pris son FID.
U3  GeoPackage, date-heure saisie en heure locale : le cote NEW est un
    instant absolu egal au fichier ; restaurer depuis un poste en UTC
    (processus enfant TZ=UTC) reussit.
U4  Shapefile 2D, deplacement saisi en PointZ : le cote NEW est le point
    2D stocke ; l'annulation reussit.
U5  relecture impossible (simulee) : le cote NEW garde la valeur du tampon,
    et le statut ``update_values_unverified`` nomme couche, entites et
    champs.
U6  couche Spatialite filtree, la modification fait sortir l'entite du
    filtre : Spatialite applique le filtre meme a une lecture par FID, la
    relecture passe par un provider sans filtre (FID = cle stockee) et ne
    crie pas "non verifiee" a tort. Garde du correctif : passe avant (rien
    n'etait relu), echoue si la relecture hors filtre manque.

Verdict attendu avant correctif : FAIL (U1..U5).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_update_provider_values"
INVARIANT = "I-CAPTURE-05"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_U5_LAYER = "couche_u5_relecture"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_shp(path: str, n: int) -> None:
    """val N(12,2) = 10 + i, lbl 'L<i>', point (i, i)."""
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("n", srs, ogr.wkbPoint)
    fd = ogr.FieldDefn("val", ogr.OFTReal)
    fd.SetWidth(12)
    fd.SetPrecision(2)
    lyr.CreateField(fd)
    fd = ogr.FieldDefn("lbl", ogr.OFTString)
    fd.SetWidth(8)
    lyr.CreateField(fd)
    for i in range(n):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("val", 10.0 + i)
        feat.SetField("lbl", f"L{i}")
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(feat)
    ds = None


def _make_dt_gpkg(path: str) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("quand", ogr.OFTDateTime))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("code", "T1")
    feat.SetField("quand", "2024/01/01 08:00:00+00")
    feat.SetGeometry(ogr.CreateGeometryFromWkt("POINT (1 1)"))
    lyr.CreateFeature(feat)
    ds = None


def _layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    uri = path if path.endswith(".shp") else f"{path}|layername=pts"
    layer = QgsVectorLayer(uri, name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


class _Stack:
    """Real journal + real WriteQueue + real tracker."""

    def __init__(self, workdir: str):
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        os.makedirs(workdir, exist_ok=True)
        self.project = os.path.join(workdir, "p.qgz")
        open(self.project, "w").close()
        self.jm = JournalManager()
        self.jm.open_for_project(project_path=self.project, profile_path=workdir)
        self.path = self.jm.path
        self.wq = WriteQueue()
        self.wq.start(self.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.statuses = []
        self.tracker.set_capture_status_callback(
            lambda kind, detail: self.statuses.append((kind, dict(detail))))
        self.tracker.activate()

    def shutdown(self) -> None:
        try:
            _pump(0.3)
            if hasattr(self.wq, "wait_until_drained"):
                self.wq.wait_until_drained(5.0)
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


def _updates(journal_path: str) -> list:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT event_id, feature_identity_json, attributes_json, "
            "new_geometry_wkb FROM audit_event WHERE operation_type = 'UPDATE' "
            "AND restored_from_event_id IS NULL ORDER BY event_id").fetchall()
    finally:
        conn.close()
    out = []
    for eid, ident, attrs, new_wkb in rows:
        try:
            fid = json.loads(ident or "{}").get("fid")
        except ValueError:
            fid = None
        try:
            delta = (json.loads(attrs or "{}") or {}).get("changed_only") or {}
        except ValueError:
            delta = {}
        new_wkt = None
        if new_wkb:
            from qgis.core import QgsGeometry
            g = QgsGeometry()
            g.fromWkb(bytes(new_wkb))
            new_wkt = g.asWkt()
        out.append({"eid": eid, "fid": fid,
                    "new": {k: (v or {}).get("new") for k, v in delta.items()},
                    "old": {k: (v or {}).get("old") for k, v in delta.items()},
                    "new_wkt": new_wkt})
    return out


def _restore(journal_path: str, eid: int, layer) -> dict:
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_updated_feature
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        event = get_event_by_id(conn, eid)
    finally:
        conn.close()
    res = restore_updated_feature(layer, event)
    return {k: res.get(k) for k in ("success", "identity_level", "reason_code",
                                    "message")}


def _field_calculator(layer, expression: str, field: str, fids=None) -> int:
    """What the field calculator does: evaluate, then changeAttributeValue."""
    from qgis.core import QgsExpression, QgsExpressionContext, \
        QgsExpressionContextUtils
    expr = QgsExpression(expression)
    context = QgsExpressionContext(
        QgsExpressionContextUtils.globalProjectLayerScopes(layer))
    expr.prepare(context)
    idx = layer.fields().indexFromName(field)
    n = 0
    for feat in layer.getFeatures():
        if fids is not None and feat.id() not in fids:
            continue
        context.setFeature(feat)
        layer.changeAttributeValue(feat.id(), idx, expr.evaluate(context))
        n += 1
    return n


def _file_values(path: str) -> dict:
    """lbl -> (fid, val), read from the file."""
    return {f["lbl"]: (f.id(), f["val"]) for f in _layer(path, "u_probe").getFeatures()}


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def _case_u1(ctx) -> dict:
    from recoverland.core.edit_tracker import EditSessionTracker
    work = os.path.join(ctx.data["tmpdir"], "u1")
    stack = _Stack(work)
    shp = os.path.join(work, "n.shp")
    _make_shp(shp, 23)
    layer = _layer(shp, "u1")
    stack.tracker.connect_layer(layer)
    signal = {}
    layer.committedAttributeValuesChanges.connect(
        lambda _lid, ch: signal.update({int(k): dict(v) for k, v in ch.items()}))
    had = "_READBACK_CHUNK" in EditSessionTracker.__dict__
    saved = EditSessionTracker.__dict__.get("_READBACK_CHUNK")
    EditSessionTracker._READBACK_CHUNK = 7
    try:
        layer.startEditing()
        res = {"edited": _field_calculator(layer, '"val" / 3', "val")}
        res["commit_ok"] = bool(layer.commitChanges())
        _pump(0.3)
    finally:
        if had:
            EditSessionTracker._READBACK_CHUNK = saved
        else:
            del EditSessionTracker._READBACK_CHUNK
    stack.shutdown()
    res["signal_val"] = {fid: v.get(0) for fid, v in signal.items()}
    res["file"] = _file_values(shp)
    res["updates"] = _updates(stack.path)
    target = next((u for u in res["updates"] if u["fid"] == 0), None)
    if target is not None:
        res["restore"] = _restore(stack.path, target["eid"], _layer(shp, "u1_restore"))
        res["file_after_restore"] = _file_values(shp)
    return res


def _case_u2(ctx) -> dict:
    work = os.path.join(ctx.data["tmpdir"], "u2")
    stack = _Stack(work)
    shp = os.path.join(work, "n.shp")
    _make_shp(shp, 5)
    layer = _layer(shp, "u2")
    stack.tracker.connect_layer(layer)
    fids = {f["lbl"]: f.id() for f in layer.getFeatures()}
    layer.startEditing()
    layer.deleteFeature(fids["L1"])
    _field_calculator(layer, '"val" / 12', "val", fids={fids["L3"]})
    res = {"fids_before": fids, "commit_ok": bool(layer.commitChanges())}
    _pump(0.3)
    stack.shutdown()
    res["file"] = _file_values(shp)
    res["updates"] = _updates(stack.path)
    return res


_TZ_CHILD = r'''
import sqlite3
from qgis.core import QgsVectorLayer
from qgis.PyQt.QtCore import QDateTime, QDate, QTime, Qt
from recoverland.core.search_service import get_event_by_id
from recoverland.core.restore_service import restore_updated_feature
probe = QDateTime(QDate(2024, 6, 15), QTime(14, 30, 0), Qt.TimeSpec.LocalTime)
conn = sqlite3.connect({journal!r})
event = get_event_by_id(conn, {eid})
conn.close()
layer = QgsVectorLayer({gpkg!r} + "|layername=pts", "u3_utc", "ogr")
res = restore_updated_feature(layer, event)
after = [f["quand"].toString(Qt.DateFormat.ISODateWithMs) for f in
         QgsVectorLayer({gpkg!r} + "|layername=pts", "u3_utc_after", "ogr").getFeatures()]
print("RESULT", json.dumps({{"offset_s": probe.offsetFromUtc(),
    "restore": {{k: res.get(k) for k in ("success", "identity_level",
                                          "reason_code", "message")}},
    "after": after}}), flush=True)
os._exit(0)
'''


def _case_u3(ctx) -> dict:
    from qgis.PyQt.QtCore import QDateTime, QDate, QTime, Qt
    from recoverland.core.serialization import journal_text_instant_ms
    from scripts.validation.crash_harness import run_child
    work = os.path.join(ctx.data["tmpdir"], "u3")
    stack = _Stack(work)
    gpkg = os.path.join(work, "d.gpkg")
    _make_dt_gpkg(gpkg)
    layer = _layer(gpkg, "u3")
    stack.tracker.connect_layer(layer)
    fid = next(layer.getFeatures()).id()
    local = QDateTime(QDate(2024, 6, 15), QTime(14, 30, 0), Qt.TimeSpec.LocalTime)
    layer.startEditing()
    layer.changeAttributeValue(fid, layer.fields().indexFromName("quand"), local)
    res = {"commit_ok": bool(layer.commitChanges()),
           "written_offset_s": local.offsetFromUtc(),
           "written_ms": local.toMSecsSinceEpoch()}
    _pump(0.3)
    stack.shutdown()
    stored = next(_layer(gpkg, "u3_probe").getFeatures())["quand"]
    res["stored_text"] = stored.toString(Qt.DateFormat.ISODateWithMs)
    res["stored_ms"] = stored.toMSecsSinceEpoch()
    res["updates"] = _updates(stack.path)
    new = (res["updates"][0]["new"].get("quand") if res["updates"] else None)
    res["journal_new"] = new
    try:
        res["journal_new_ms_here"] = journal_text_instant_ms(new) if new else None
    except Exception as exc:  # noqa: BLE001 - reported by the assertion
        res["journal_new_ms_here"] = f"{type(exc).__name__}: {exc}"
    if res["updates"]:
        rc, so, se = run_child(
            _TZ_CHILD.format(journal=Path(stack.path).as_posix(),
                             eid=res["updates"][0]["eid"], gpkg=gpkg),
            timeout=180, workdir=work, extra_env={"TZ": "UTC"})
        line = next((ln for ln in so.splitlines() if ln.startswith("RESULT ")), None)
        res["utc_child"] = json.loads(line[7:]) if line else {"rc": rc, "stderr": se[-600:]}
    return res


def _case_u4(ctx) -> dict:
    from qgis.core import QgsGeometry
    work = os.path.join(ctx.data["tmpdir"], "u4")
    stack = _Stack(work)
    shp = os.path.join(work, "n.shp")
    _make_shp(shp, 3)
    layer = _layer(shp, "u4")
    stack.tracker.connect_layer(layer)
    fid = next(f.id() for f in layer.getFeatures() if f["lbl"] == "L1")
    layer.startEditing()
    layer.changeGeometry(fid, QgsGeometry.fromWkt("POINT Z (7 8 9)"))
    res = {"fid": fid, "commit_ok": bool(layer.commitChanges())}
    _pump(0.3)
    stack.shutdown()
    res["stored_wkt"] = next(f.geometry().asWkt() for f in _layer(shp, "u4_probe").getFeatures()
                             if f["lbl"] == "L1")
    res["updates"] = _updates(stack.path)
    if res["updates"]:
        res["restore"] = _restore(stack.path, res["updates"][0]["eid"],
                                  _layer(shp, "u4_restore"))
        res["wkt_after_restore"] = next(
            f.geometry().asWkt() for f in _layer(shp, "u4_after").getFeatures()
            if f["lbl"] == "L1")
    return res


def _case_u5(ctx) -> dict:
    from recoverland.core.edit_tracker import EditSessionTracker
    work = os.path.join(ctx.data["tmpdir"], "u5")
    stack = _Stack(work)
    shp = os.path.join(work, "n.shp")
    _make_shp(shp, 3)
    layer = _layer(shp, _U5_LAYER)
    stack.tracker.connect_layer(layer)
    # The provider returns nothing on the read-back (a failed read, a
    # provider that filters a read by FID). Harmless on a code without
    # read-back: the attribute is simply never called.
    had = "_read_back_updated" in EditSessionTracker.__dict__
    saved = EditSessionTracker.__dict__.get("_read_back_updated")
    EditSessionTracker._read_back_updated = lambda self, lyr, fids, **kw: {}
    try:
        layer.startEditing()
        _field_calculator(layer, '"val" / 3', "val")
        res = {"commit_ok": bool(layer.commitChanges())}
        _pump(0.3)
    finally:
        if had:
            EditSessionTracker._read_back_updated = saved
        else:
            del EditSessionTracker._read_back_updated
    stack.shutdown()
    res["updates"] = _updates(stack.path)
    res["file"] = _file_values(shp)
    res["statuses"] = [(k, {kk: vv for kk, vv in d.items()
                            if kk in ("layer_name", "fids", "fields", "n_features")})
                       for k, d in stack.statuses]
    return res


def _case_u6(ctx) -> dict:
    """Spatialite filtered layer: the edit moves the feature out of the filter."""
    from qgis.core import QgsVectorLayer, QgsDataSourceUri, QgsFeatureRequest
    from osgeo import ogr
    work = os.path.join(ctx.data["tmpdir"], "u6")
    stack = _Stack(work)
    path = os.path.join(work, "f.sqlite")
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("SQLite").CreateDataSource(path, options=["SPATIALITE=YES"])
    ds.ExecuteSQL("CREATE TABLE pts (pk INTEGER PRIMARY KEY AUTOINCREMENT, "
                  "code TEXT, n INTEGER)")
    ds.ExecuteSQL("SELECT AddGeometryColumn('pts', 'geom', 2154, 'POINT', 'XY')")
    for i in range(3):
        ds.ExecuteSQL(f"INSERT INTO pts (code, n, geom) VALUES ('C{i}', {i}, "
                      f"MakePoint({i}, {i}, 2154))")
    ds = None
    uri = QgsDataSourceUri()
    uri.setDatabase(path)
    uri.setDataSource("", "pts", "geom", "\"code\" <> 'CACHE'", "pk")
    layer = QgsVectorLayer(uri.uri(), "u6", "spatialite")
    stack.tracker.connect_layer(layer)
    fid = next(layer.getFeatures()).id()
    hidden = {}
    layer.committedAttributeValuesChanges.connect(lambda _lid, ch: hidden.update(
        read=[f.id() for f in layer.dataProvider().getFeatures(
            QgsFeatureRequest().setFilterFids([int(k) for k in ch.keys()]))]))
    layer.startEditing()
    layer.changeAttributeValue(fid, layer.fields().indexFromName("code"), "CACHE")
    res = {"fid": fid, "commit_ok": bool(layer.commitChanges())}
    _pump(0.3)
    stack.shutdown()
    res["filtered_read_in_commit"] = hidden.get("read")
    res["updates"] = _updates(stack.path)
    res["statuses"] = [k for k, _d in stack.statuses]
    return res


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_c_upd_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("u1", _case_u1), ("u2", _case_u2), ("u3", _case_u3),
                    ("u4", _case_u4), ("u5", _case_u5), ("u6", _case_u6)):
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


def _close(a, b) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return False


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    u1, u2, u3 = ctx.data.get("u1", {}), ctx.data.get("u2", {}), ctx.data.get("u3", {})
    u4, u5 = ctx.data.get("u4", {}), ctx.data.get("u5", {})

    # ===== U1: Shapefile N(12,2), field calculator, chunked read-back ===
    file1 = u1.get("file") or {}
    sig1 = u1.get("signal_val") or {}
    out.append((
        "u1_fixture_the_file_rounds_what_the_buffer_sent",
        u1.get("commit_ok") is True and u1.get("edited") == 23 and len(file1) == 23
        and _close(file1.get("L0", (None, None))[1], 3.33)
        and _close(sig1.get(0), 10.0 / 3),
        f"commit={u1.get('commit_ok')} edites={u1.get('edited')} signal fid0="
        f"{sig1.get(0)} fichier L0={file1.get('L0')} crash={u1.get('crash')} "
        f"{u1.get('trace', '')}: sans arrondi par la source le cas ne prouve rien",
    ))
    upd1 = {u["fid"]: u for u in u1.get("updates") or []}
    wrong1 = {}
    for lbl, (fid, val) in file1.items():
        new = (upd1.get(fid) or {}).get("new", {}).get("val")
        if not _close(new, val):
            wrong1[lbl] = {"journal": new, "fichier": val}
    out.append((
        "u1_update_new_side_is_what_the_file_stored",
        len(upd1) == 23 and not wrong1,
        f"{len(upd1)} UPDATE ; ecarts journal/fichier={dict(list(wrong1.items())[:5])} "
        f"(total {len(wrong1)}). Le journal dit que le champ vaut ce que le "
        f"calculateur a envoye, le fichier tient la valeur arrondie",
    ))
    rest1 = u1.get("restore") or {}
    after1 = u1.get("file_after_restore") or {}
    out.append((
        "u1_field_calculator_edit_can_be_undone",
        rest1.get("success") is True
        and _close((after1.get("L0") or (None, None))[1], 10.0),
        f"restauration de l'UPDATE fid=0={rest1} ; L0 apres={after1.get('L0')} "
        f"attendu 10.0. La preuve de l'etat d'apres compare 3.33 (fichier) a "
        f"la valeur envoyee : l'edition n'est jamais annulable",
    ))

    # ===== U2: deletion in the same commit renumbers the Shapefile ======
    file2 = u2.get("file") or {}
    fids2 = u2.get("fids_before") or {}
    upd2 = u2.get("updates") or []
    stored_l3 = (file2.get("L3") or (None, None))[1]
    out.append((
        "u2_fixture_the_edited_feature_changed_fid",
        u2.get("commit_ok") is True and "L1" not in file2
        and (file2.get("L3") or (None, None))[0] != fids2.get("L3")
        and _close(stored_l3, 1.08),
        f"commit={u2.get('commit_ok')} fid avant={fids2} fichier apres={file2} "
        f"crash={u2.get('crash')} {u2.get('trace', '')}",
    ))
    out.append((
        "u2_new_side_read_back_before_the_repack",
        len(upd2) == 1 and upd2[0].get("fid") == fids2.get("L3")
        and _close(upd2[0]["new"].get("val"), stored_l3),
        f"UPDATE={upd2} ; L3 stockee={stored_l3} (13/12 arrondi) ; une relecture "
        f"apres la suppression lit au FID {fids2.get('L3')} l'entite suivante "
        f"(14.0) et journalise la valeur d'une AUTRE entite",
    ))

    # ===== U3: GeoPackage local date-time ==============================
    new3 = u3.get("journal_new")
    child3 = u3.get("utc_child") or {}
    out.append((
        "u3_fixture_the_file_holds_the_instant_in_utc",
        u3.get("commit_ok") is True and u3.get("written_offset_s") not in (None, 0)
        and u3.get("stored_ms") == u3.get("written_ms")
        and str(u3.get("stored_text") or "").endswith("Z")
        and child3.get("offset_s") == 0,
        f"commit={u3.get('commit_ok')} decalage du poste={u3.get('written_offset_s')} s "
        f"stocke={u3.get('stored_text')} poste enfant decalage={child3.get('offset_s')} "
        f"crash={u3.get('crash')} {u3.get('trace', '')}: il faut un poste hors UTC "
        f"pour la capture et un poste en UTC pour la restauration",
    ))
    out.append((
        "u3_update_new_side_is_an_absolute_instant",
        isinstance(new3, str) and (new3.endswith("Z") or new3[-6:-5] in "+-")
        and u3.get("journal_new_ms_here") == u3.get("stored_ms"),
        f"cote NEW journalise={new3!r} ; fichier={u3.get('stored_text')}. Un texte "
        f"sans fuseau est relu dans le fuseau de chaque poste : le meme journal "
        f"designe un autre instant ailleurs",
    ))
    rest3 = child3.get("restore") or {}
    out.append((
        "u3_restore_from_a_utc_workstation_succeeds",
        rest3.get("success") is True
        and (child3.get("after") or [None])[0] == "2024-01-01T08:00:00.000Z",
        f"restauration sur un poste en UTC={child3} attendu succes et "
        f"2024-01-01T08:00:00.000Z. Le texte naif '14:30' y vaut 14:30Z, le "
        f"fichier tient 12:30Z : la preuve de l'etat d'apres echoue",
    ))

    # ===== U4: 2D Shapefile, PointZ digitised ==========================
    upd4 = u4.get("updates") or []
    rest4 = u4.get("restore") or {}
    out.append((
        "u4_geometry_new_side_is_the_stored_geometry",
        u4.get("commit_ok") is True and u4.get("stored_wkt") == "Point (7 8)"
        and len(upd4) == 1 and upd4[0].get("new_wkt") == "Point (7 8)",
        f"stocke={u4.get('stored_wkt')} ; cote NEW journalise="
        f"{[u.get('new_wkt') for u in upd4]} crash={u4.get('crash')} "
        f"{u4.get('trace', '')}. Le journal garde un Z que le fichier n'a jamais eu",
    ))
    out.append((
        "u4_geometry_edit_can_be_undone",
        rest4.get("success") is True and u4.get("wkt_after_restore") == "Point (1 1)",
        f"restauration={rest4} ; geometrie apres={u4.get('wkt_after_restore')} "
        f"attendu Point (1 1)",
    ))

    # ===== U5: read-back failed ========================================
    upd5 = u5.get("updates") or []
    st5 = [d for k, d in u5.get("statuses") or [] if k == "update_values_unverified"]
    out.append((
        "u5_failed_read_back_keeps_the_buffer_value_and_says_so",
        u5.get("commit_ok") is True and len(upd5) == 3
        and all(_close(u["new"].get("val"), (10.0 + u["fid"]) / 3) for u in upd5)
        and len(st5) >= 1 and st5[0].get("layer_name") == _U5_LAYER
        and sorted(st5[0].get("fids") or []) == [0, 1, 2]
        and st5[0].get("fields") == ["val"],
        f"UPDATE={[(u['fid'], u['new']) for u in upd5]} ; statuts={u5.get('statuses')} "
        f"crash={u5.get('crash')} {u5.get('trace', '')}. Si la source ne rend pas "
        f"l'entite, la valeur envoyee est gardee (jamais inventee) et l'ecart "
        f"est nomme : couche, entites, champs",
    ))
    out.append(assert_log_contains(
        ctx.records, r"event=update_values_unverified.*" + _U5_LAYER,
        name="u5_forensic_log_names_the_gap"))

    # ===== U6: Spatialite filter hides the edited feature ===============
    u6 = ctx.data.get("u6", {})
    upd6 = u6.get("updates") or []
    out.append((
        "u6_fixture_the_filter_hides_the_edited_feature_from_a_fid_read",
        u6.get("commit_ok") is True and u6.get("filtered_read_in_commit") == [],
        f"commit={u6.get('commit_ok')} lecture par FID sur la couche filtree="
        f"{u6.get('filtered_read_in_commit')} (attendu []) crash={u6.get('crash')} "
        f"{u6.get('trace', '')}",
    ))
    out.append((
        "u6_edit_leaving_the_filter_is_read_back_without_it",
        len(upd6) == 1 and upd6[0]["new"].get("code") == "CACHE"
        and "update_values_unverified" not in (u6.get("statuses") or []),
        f"UPDATE={upd6} ; statuts={u6.get('statuses')}. Spatialite applique le "
        f"filtre meme a une lecture par FID : sans relecture hors filtre, chaque "
        f"modification qui fait sortir l'entite du filtre serait annoncee "
        f"'non verifiee' a tort",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=5))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
