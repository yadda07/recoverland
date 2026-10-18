"""tx_capture_reconcile_coerced - RecoverLand validation runtime.

Invariant: I-CRASH-01 (apres un plantage pendant un commit, le
rapprochement dit exactement ce que la source a recu, ni plus ni moins).

Le mecanisme
------------
Apres un plantage pendant un commit, le rapprochement
(core/crash_reconcile.py) reconnait un ajout dans la source a son contenu :
les valeurs saisies et la geometrie, notees avant l'ecriture. Il les
comparait a l'identique. Or la source ne garde pas toujours ce qu'on lui
donne (mesure sur QGIS 4.2.1) :

* un reel a precision declaree est arrondi : Shapefile N(12,2) et MapInfo
  gardent 3.33 pour 10/3 (le calculateur de champs, $area) ;
* un champ entier recoit la valeur arrondie : 3.7 devient 4 sur
  Shapefile, GeoPackage, MapInfo et Spatialite (la file d'edition garde
  3.7) ;
* un Shapefile enleve les espaces de fin d'un texte ; au-dela de 254
  caracteres il le tronque, MapInfo le tronque a la largeur du champ.

Un ajout ainsi transforme qui avait atterri n'etait retrouve nulle part :
compte "non applique", aucun INSERT. L'utilisateur lit que rien n'a ete
ecrit alors que l'entite est dans ses donnees, et un rewind avant le
plantage ne peut jamais la retirer.

Comparer selon les regles de stockage n'est sur que si la recherche des
jumeaux (les entites qui tenaient deja ce contenu avant le commit) suit
les memes regles : sinon une entite d'avant, egale a l'ajout une fois
arrondie, passe pour l'ajout et annuler cet INSERT supprime l'entite de
l'utilisateur.

Cas
---
K1  Shapefile, plantage apres l'ecriture : quatre ajouts, A (10/3 dans
    N(12,2)), B (texte avec espaces de fin), C (texte de 300 caracteres,
    tronque a 254), E ('ABCDEF' au point d'une entite d'avant 'AB').
    Attendu : A, B et E ecrits, leur INSERT porte ce que le fichier garde
    (3.33, 'TRAIL') et les annuler les retire ; C indetermine avec sa
    raison nommee (content_coerced_by_source), jamais "non applique".
K2  le meme commit, plantage avant l'ecriture : les quatre non appliques,
    aucun INSERT (les regles de stockage n'inventent pas un ajout ; 'AB',
    prefixe court de 'ABCDEF', est un autre texte, pas une coupe a la
    largeur du champ : il ne rend pas l'ajout indetermine).
K3  GeoPackage, champ entier : une entite d'avant (DUP, n=4, point 9 9) et
    un ajout (DUP, n=3.7, meme point, stocke 4). a) plantage avant
    l'ecriture : non applique, l'entite d'avant est notee jumelle ; b)
    plantage apres : ecrit, l'INSERT designe l'ajout et jamais l'entite
    d'avant, l'annuler laisse celle-ci en place.
K4  GeoPackage, le meme commit modifie POST (n 9 -> 4.4, stocke 4) et
    ajoute (POST, n=3.7, meme point, stocke 4) ; plantage apres l'ecriture.
    Avant le commit POST ne tenait pas le contenu de l'ajout, apres il le
    tient : la recherche des jumeaux compare aussi l'etat que le commit
    donne aux entites qu'il modifie, et doit le faire comme la source
    stockera les deux valeurs. Attendu : POST notee jumelle, l'ajout ecrit
    sur sa propre entite, l'UPDATE de POST ecrit.
K6  registre d'une version anterieure (recherche des jumeaux a
    l'identique, sans ``rounding``) construit a la main : l'ajout arrondi
    est dans le fichier : indetermine avec sa raison, pas "non applique" ;
    un autre ajout absent du fichier reste non applique.

Verdict mesure avant correctif : FAIL passed=3/10 sur le worktree avant la
modification (premiere version, sans E ni K4 : K1 landed=0 not_applied=3,
K3a sans jumeau note, K3b non applique, K6 not_applied=2) ; FAIL
passed=3/11 avec K4, correctif retire en memoire dans ce processus et dans
les processus enfants (K4 : POST non notee jumelle, l'ajout non applique).
K2 passe avant comme apres : il garde le correctif honnete (aucun ajout
invente, aucun "indetermine" sur un simple prefixe).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_reconcile_coerced"
INVARIANT = "I-CRASH-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LONG = "Y" * 300


def _make_shp(path: str, extra=()) -> None:
    """val N(12,2) = 10 + i, lbl C(8) 'L<i>', point (i, i); *extra*: (val, lbl, x)."""
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("k", srs, ogr.wkbPoint)
    fd = ogr.FieldDefn("val", ogr.OFTReal)
    fd.SetWidth(12)
    fd.SetPrecision(2)
    lyr.CreateField(fd)
    fd = ogr.FieldDefn("lbl", ogr.OFTString)
    fd.SetWidth(8)
    lyr.CreateField(fd)
    rows = [(10.0 + i, f"L{i}", i) for i in range(3)] + list(extra)
    for val, lbl, x in rows:
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("val", val)
        feat.SetField("lbl", lbl)
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({x} {x})"))
        lyr.CreateFeature(feat)
    ds = None


def _make_gpkg(path: str) -> None:
    """code, n INTEGER; AUTRE (n=1, 1 1), POST (n=9, 7 7), DUP (n=4, 9 9)."""
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("n", ogr.OFTInteger))
    for code, n, x in (("AUTRE", 1, 1), ("POST", 9, 7), ("DUP", 4, 9)):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("code", code)
        feat.SetField("n", n)
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({x} {x})"))
        lyr.CreateFeature(feat)
    ds = None


def _layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    uri = path if path.endswith(".shp") else f"{path}|layername=pts"
    layer = QgsVectorLayer(uri, name, "ogr")
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


def _rows(path: str) -> list:
    """What the file holds, sorted: (x, values...) per feature."""
    out = []
    for f in _layer(path, "k_rows").getFeatures():
        g = f.geometry()
        x = None if g is None or g.isNull() else round(g.asPoint().x(), 3)
        if path.endswith(".shp"):
            lbl = f["lbl"]
            out.append((x, f["val"], lbl if len(lbl or "") < 20 else f"<{len(lbl)} chars>"))
        else:
            out.append((x, f.id(), f["code"], f["n"]))
    return sorted(out, key=repr)


def _journal(journal_path: str) -> dict:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        inserts = []
        for eid, ident, attrs in conn.execute(
                "SELECT event_id, feature_identity_json, attributes_json FROM audit_event "
                "WHERE operation_type = 'INSERT' AND restored_from_event_id IS NULL "
                "ORDER BY event_id"):
            try:
                fid = json.loads(ident or "{}").get("fid")
            except ValueError:
                fid = None
            try:
                values = json.loads(attrs or "{}")
                values = values.get("all_attributes", values)
            except ValueError:
                values = {}
            inserts.append({"eid": eid, "fid": fid, "attrs": values})
        ledger = [{"state": s, "detail": d, "snapshots": json.loads(j) if j else None}
                  for s, d, j in conn.execute(
                      "SELECT state, detail, snapshots_json FROM capture_commit "
                      "ORDER BY recorded_at")]
    finally:
        conn.close()
    return {"inserts": inserts, "ledger": ledger}


def _restart(workdir: str, path: str) -> dict:
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
    tr.connect_layer(_layer(path, "k_restart"))
    _pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()
    journal = jm.path
    jm.close()
    out = {"journal": journal,
           "reconciled": [{k: d.get(k) for k in ("landed", "not_applied", "unresolved")}
                          for k, d in statuses if k == "reconciled"]}
    out.update(_journal(journal))
    return out


def _totals(res: dict) -> dict:
    rec = res.get("reconciled") or []
    return {k: sum(int(r.get(k) or 0) for r in rec)
            for k in ("landed", "not_applied", "unresolved")}


def _detail(res: dict) -> str:
    return " | ".join(str(r.get("detail")) for r in res.get("ledger") or [])


_CHILD = r'''
from qgis.core import QgsVectorLayer, QgsGeometry, QgsPointXY, QgsVectorLayerUtils
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
path = {path!r}
mode = {mode!r}
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
wq = WriteQueue(); wq.start(jm.path)
tr = EditSessionTracker(wq, jm); tr.activate()
uri = path if path.endswith(".shp") else path + "|layername=pts"
layer = QgsVectorLayer(uri, "k", "ogr")
tr.connect_layer(layer)
idx = layer.fields().indexFromName
def add(x, values):
    layer.addFeature(QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(x, x)),
        {{idx(k): v for k, v in values.items()}}))
layer.startEditing()
if path.endswith(".shp"):
    add(50.0, {{"val": 10.0 / 3.0, "lbl": "NEW"}})
    add(60.0, {{"val": 20.5, "lbl": "TRAIL   "}})
    add(70.0, {{"val": 30.0, "lbl": "Y" * 300}})
    add(90.0, {{"val": 5.0, "lbl": "ABCDEF"}})
elif mode == "post":
    post = next(f for f in layer.getFeatures() if f["code"] == "POST")
    layer.changeAttributeValue(post.id(), idx("n"), 4.4)
    add(7.0, {{"code": "POST", "n": 3.7}})
else:
    add(9.0, {{"code": "DUP", "n": 3.7}})
print("COMMIT", layer.commitChanges(), flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _crash_case(ctx, key: str, fault: str, fmt: str, mode: str = "") -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], key)
    os.makedirs(work, exist_ok=True)
    path = os.path.join(work, "k.shp" if fmt == "shp" else "k.gpkg")
    if fmt == "shp":
        _make_shp(path, extra=[(5.0, "AB", 90)])
    else:
        _make_gpkg(path)
    res = {"path": path, "file_before": _rows(path)}
    rc, _so, se = run_child(_CHILD.format(workdir=work, path=path, mode=mode),
                            fault=fault,
                            timeout=180, workdir=work)
    res.update({"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
                "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else "",
                "file_after_crash": _rows(path)})
    res.update(_restart(work, path))
    res["file_after_restart"] = _rows(path)
    return res


def _undo_inserts(res: dict) -> None:
    """Undo every INSERT the journal holds: what a rewind before the crash does."""
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_inserted_feature
    undos = []
    for ins in res.get("inserts") or []:
        conn = sqlite3.connect(f"file:{Path(res['journal']).as_posix()}?mode=ro", uri=True)
        try:
            event = get_event_by_id(conn, ins["eid"])
            out = restore_inserted_feature(_layer(res["path"], f"k_undo_{ins['eid']}"),
                                           event, journal=conn)
        finally:
            conn.close()
        undos.append({k: out.get(k) for k in ("success", "identity_level",
                                              "reason_code", "message")})
    res["undos"] = undos
    res["file_after_undo"] = _rows(res["path"])


def _case_k1(ctx) -> dict:
    res = _crash_case(ctx, "k1", "capture_after_provider_commit", "shp")
    _undo_inserts(res)
    return res


def _case_k2(ctx) -> dict:
    return _crash_case(ctx, "k2", "capture_after_prepare", "shp")


def _case_k3a(ctx) -> dict:
    return _crash_case(ctx, "k3a", "capture_after_prepare", "gpkg")


def _case_k3b(ctx) -> dict:
    res = _crash_case(ctx, "k3b", "capture_after_provider_commit", "gpkg")
    _undo_inserts(res)
    return res


def _case_k4(ctx) -> dict:
    return _crash_case(ctx, "k4", "capture_after_provider_commit", "gpkg", "post")


def _case_k6(ctx) -> dict:
    """Ledger of a build whose twin search compared values as entered."""
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.journal_writes import ensure_v7_schema, record_capture_commit
    from recoverland.core.identity import compute_datasource_fingerprint
    from qgis.core import QgsGeometry, QgsPointXY
    work = os.path.join(ctx.data["tmpdir"], "k6")
    os.makedirs(work, exist_ok=True)
    path = os.path.join(work, "k.shp")
    # The addition A landed (stored 3.33); D never reached the file.
    _make_shp(path, extra=[(3.33, "NEW", 50)])
    open(os.path.join(work, "p.qgz"), "w").close()

    def _wkb(x):
        import base64
        return "b64:" + base64.b64encode(bytes(
            QgsGeometry.fromPointXY(QgsPointXY(x, x)).asWkb())).decode("ascii")
    snaps = [{"kind": "INSERT", "fid": None,
              "attributes": {"val": 10.0 / 3.0, "lbl": "NEW"},
              "geometry_wkb": _wkb(50.0), "unset_fields": [], "twins": [], "n_twins": 0},
             {"kind": "INSERT", "fid": None,
              "attributes": {"val": 7.0 / 3.0, "lbl": "ABSENT"},
              "geometry_wkb": _wkb(80.0), "unset_fields": [], "twins": [], "n_twins": 0}]
    layer = _layer(path, "k6_fix")
    fp = compute_datasource_fingerprint(layer)
    del layer
    jm = JournalManager()
    jm.open_for_project(os.path.join(work, "p.qgz"), work)
    journal = jm.path
    jm.close()
    conn = sqlite3.connect(journal)
    try:
        with conn:
            ensure_v7_schema(conn)
            record_capture_commit(
                conn, "k6legacy", "INTERRUPTED", layer_id="crafted", layer_name="k",
                datasource_fingerprint=fp, session_id="k6",
                prepared_at="2026-09-27T10:00:00+00:00", finalized_at=None,
                n_snapshots=len(snaps), n_events=0,
                detail=f"scenario {SCENARIO_ID} k6: prepare of an earlier build",
                snapshots_json=json.dumps(snaps))
    finally:
        conn.close()
    res = {"path": path, "file_before": _rows(path)}
    res.update(_restart(work, path))
    res["file_after_restart"] = _rows(path)
    return res


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_c_coer_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("k1", _case_k1), ("k2", _case_k2), ("k3a", _case_k3a),
                    ("k3b", _case_k3b), ("k4", _case_k4), ("k6", _case_k6)):
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


def _insert_at(res: dict, **values) -> dict:
    for ins in res.get("inserts") or []:
        attrs = ins.get("attrs") or {}
        if all(attrs.get(k) == v for k, v in values.items()):
            return ins
    return {}


_SHP_BASE = [(0.0, 10.0, "L0"), (1.0, 11.0, "L1"), (2.0, 12.0, "L2"),
             (90.0, 5.0, "AB")]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    k1, k2 = ctx.data.get("k1", {}), ctx.data.get("k2", {})
    k3a, k3b, k6 = ctx.data.get("k3a", {}), ctx.data.get("k3b", {}), ctx.data.get("k6", {})
    k4 = ctx.data.get("k4", {})

    # ===== K1: Shapefile, crash after the provider wrote ================
    after1 = k1.get("file_after_crash") or []
    out.append((
        "k1_fixture_the_source_coerced_what_it_stored",
        k1.get("fault_reached") is True
        and (50.0, 3.33, "NEW") in after1 and (60.0, 20.5, "TRAIL") in after1
        and (70.0, 30.0, "<254 chars>") in after1
        and (90.0, 5.0, "ABCDEF") in after1 and (90.0, 5.0, "AB") in after1,
        f"rc={k1.get('rc')} fichier={after1} stderr={k1.get('stderr')} "
        f"crash={k1.get('crash')} {k1.get('trace', '')}",
    ))
    a_ins = _insert_at(k1, val=3.33, lbl="NEW")
    b_ins = _insert_at(k1, val=20.5, lbl="TRAIL")
    e_ins = _insert_at(k1, val=5.0, lbl="ABCDEF")
    out.append((
        "k1_coerced_additions_that_landed_are_journalled",
        _totals(k1) == {"landed": 3, "not_applied": 0, "unresolved": 1}
        and len(k1.get("inserts") or []) == 3 and bool(a_ins) and bool(b_ins)
        and bool(e_ins),
        f"rapprochement={k1.get('reconciled')} attendu landed=3 unresolved=1 ; "
        f"INSERT={k1.get('inserts')} ; registre={_detail(k1)}. Un ajout que la "
        f"source a arrondi (10/3 -> 3.33) ou dont elle a enleve les espaces de "
        f"fin est dans le fichier : le dire 'non applique' cache a l'utilisateur "
        f"une entite ecrite, qu'aucun rewind ne pourra retirer",
    ))
    out.append((
        "k1_truncated_text_is_unresolved_with_its_reason_never_not_applied",
        "content_coerced_by_source=1" in _detail(k1)
        and _totals(k1).get("not_applied") == 0,
        f"registre={_detail(k1)} rapprochement={k1.get('reconciled')}. Le texte "
        f"de 300 caracteres est dans le fichier, tronque a 254 : l'annoncer "
        f"'non applique' dit a l'utilisateur que rien n'a ete ecrit",
    ))
    undos1 = k1.get("undos") or []
    after_undo1 = k1.get("file_after_undo") or []
    out.append((
        "k1_undoing_the_reconciled_inserts_removes_them",
        len(undos1) == 3 and all(u.get("success") is True for u in undos1)
        and sorted(after_undo1, key=repr) == sorted(
            _SHP_BASE + [(70.0, 30.0, "<254 chars>")], key=repr),
        f"annulations={undos1} ; fichier apres={after_undo1}. L'INSERT "
        f"reconstruit doit designer l'entite ecrite pour qu'un rewind avant le "
        f"plantage la retire",
    ))

    # ===== K2: same commit, crash before the provider wrote =============
    out.append((
        "k2_nothing_written_stays_not_applied",
        k2.get("fault_reached") is True
        and _totals(k2) == {"landed": 0, "not_applied": 4, "unresolved": 0}
        and not k2.get("inserts")
        and sorted(k2.get("file_after_restart") or [], key=repr)
        == sorted(_SHP_BASE, key=repr),
        f"rc={k2.get('rc')} rapprochement={k2.get('reconciled')} attendu "
        f"not_applied=4 ; INSERT={k2.get('inserts')} ; fichier="
        f"{k2.get('file_after_restart')} ; registre={_detail(k2)} crash="
        f"{k2.get('crash')} {k2.get('trace', '')}. Comparer selon les regles "
        f"de stockage ne doit pas inventer un ajout qui n'a jamais atteint le "
        f"fichier",
    ))

    # ===== K3: GeoPackage integer field, a twin before the commit ======
    twin_fid = next((row[1] for row in k3a.get("file_before") or []
                     if row[2] == "DUP"), None)
    snaps3 = ((k3a.get("ledger") or [{}])[0].get("snapshots") or [])
    record3 = next((s for s in snaps3 if isinstance(s, dict)
                    and s.get("kind") in ("INSERT", "INSERT_UNPROVEN")), {})
    out.append((
        "k3a_twin_equal_once_stored_is_set_aside_nothing_landed",
        k3a.get("fault_reached") is True
        and _totals(k3a) == {"landed": 0, "not_applied": 1, "unresolved": 0}
        and not k3a.get("inserts")
        and twin_fid is not None and twin_fid in (record3.get("twins") or []),
        f"rc={k3a.get('rc')} rapprochement={k3a.get('reconciled')} attendu "
        f"not_applied=1 ; INSERT={k3a.get('inserts')} ; enregistrement="
        f"{record3} jumeau attendu fid={twin_fid} ; crash={k3a.get('crash')} "
        f"{k3a.get('trace', '')}. L'entite d'avant (n=4) tient le contenu de "
        f"l'ajout une fois stocke (3.7 -> 4) : non notee jumelle, elle passe "
        f"pour l'ajout et annuler cet INSERT supprime l'entite de l'utilisateur",
    ))
    new_rows = [row for row in k3b.get("file_after_crash") or []
                if row[2] == "DUP" and row[1] != twin_fid]
    ins3 = (k3b.get("inserts") or [{}])[0]
    out.append((
        "k3b_integer_addition_that_landed_is_journalled_on_itself",
        k3b.get("fault_reached") is True and len(new_rows) == 1
        and _totals(k3b) == {"landed": 1, "not_applied": 0, "unresolved": 0}
        and len(k3b.get("inserts") or []) == 1
        and ins3.get("fid") == new_rows[0][1] and ins3.get("fid") != twin_fid
        and (ins3.get("attrs") or {}).get("n") == 4,
        f"rapprochement={k3b.get('reconciled')} attendu landed=1 ; INSERT="
        f"{k3b.get('inserts')} attendu sur fid={new_rows[0][1] if new_rows else None} "
        f"(jamais {twin_fid}) ; fichier={k3b.get('file_after_crash')} ; registre="
        f"{_detail(k3b)} crash={k3b.get('crash')} {k3b.get('trace', '')}. 3.7 "
        f"saisi dans un champ entier est stocke 4 : a l'identique l'ajout "
        f"n'etait retrouve nulle part",
    ))
    undos3 = k3b.get("undos") or []
    after_undo3 = [row[1:] for row in k3b.get("file_after_undo") or []]
    out.append((
        "k3b_undo_removes_the_addition_and_keeps_the_twin",
        len(undos3) == 1 and undos3[0].get("success") is True
        and sorted(after_undo3, key=repr) == sorted(
            [row[1:] for row in k3b.get("file_before") or []], key=repr),
        f"annulation={undos3} ; fichier apres={k3b.get('file_after_undo')} "
        f"attendu {k3b.get('file_before')}. Annuler l'ajout ne doit supprimer "
        f"que lui",
    ))

    # ===== K4: a feature the same commit modifies into a twin ==========
    post_fid = next((row[1] for row in k4.get("file_before") or []
                     if row[2] == "POST"), None)
    snaps4 = ((k4.get("ledger") or [{}])[0].get("snapshots") or [])
    record4 = next((s for s in snaps4 if isinstance(s, dict)
                    and s.get("kind") in ("INSERT", "INSERT_UNPROVEN")), {})
    new4 = [row for row in k4.get("file_after_crash") or []
            if row[2] == "POST" and row[1] != post_fid]
    ins4 = (k4.get("inserts") or [{}])[0]
    out.append((
        "k4_feature_modified_into_a_twin_is_set_aside",
        k4.get("fault_reached") is True and len(new4) == 1
        and post_fid is not None and post_fid in (record4.get("twins") or [])
        and _totals(k4) == {"landed": 2, "not_applied": 0, "unresolved": 0}
        and len(k4.get("inserts") or []) == 1
        and ins4.get("fid") == new4[0][1] and ins4.get("fid") != post_fid,
        f"rc={k4.get('rc')} enregistrement={record4} jumeau attendu fid={post_fid} ; "
        f"rapprochement={k4.get('reconciled')} attendu landed=2 ; INSERT="
        f"{k4.get('inserts')} attendu sur fid={new4[0][1] if new4 else None} ; "
        f"fichier={k4.get('file_after_crash')} ; registre={_detail(k4)} crash="
        f"{k4.get('crash')} {k4.get('trace', '')}. POST (4.4 envoye, 4 stocke) "
        f"tient apres le commit le contenu de l'ajout (3.7 -> 4) : non notee "
        f"jumelle, l'ajout n'est pas prouvable, ou pire POST passe pour lui",
    ))

    # ===== K6: ledger of an earlier build ===============================
    out.append((
        "k6_earlier_ledger_coerced_match_is_unresolved_not_not_applied",
        _totals(k6) == {"landed": 0, "not_applied": 1, "unresolved": 1}
        and "content_coerced_by_source=1" in _detail(k6)
        and not k6.get("inserts"),
        f"rapprochement={k6.get('reconciled')} attendu not_applied=1 "
        f"unresolved=1 ; registre={_detail(k6)} ; INSERT={k6.get('inserts')} "
        f"crash={k6.get('crash')} {k6.get('trace', '')}. Ses jumeaux ont ete "
        f"cherches a l'identique : l'entite a 3.33 peut etre l'ajout comme une "
        f"entite d'avant ; ni 'ecrit' ni 'non applique' n'est prouve",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=6))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
