# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_reconcile_twins - RecoverLand validation runtime.

Invariant: I-CRASH-01 (apres un plantage pendant un commit, le
rapprochement dit exactement ce que la source a recu, ni plus ni moins).

Le mecanisme
------------
Avant que la source n'ecrive un ajout, la capture ne connait pas le FID
qu'il recevra : apres un plantage le rapprochement (core/crash_reconcile.py)
le reconnait a son contenu. "Dupliquer l'entite" ou "Coller les entites"
produisent le contenu d'une entite qui EXISTE deja (les champs laisses a la
source, la cle comprise, ne sont pas compares). Pour ne jamais prendre
l'originale pour l'ajout, la capture cherche ces jumeaux AVANT l'ecriture
et note leurs FID (``twins``), et les champs laisses a la source
(``unset_fields``).

Deux trous :

* le rapprochement ne se servait pas des jumeaux : un ajout qui en avait
  etait compte "indetermine" dans tous les cas. Plantage AVANT l'ecriture :
  l'utilisateur lit une alerte critique "1 indeterminee" alors que rien n'a
  ete ecrit. Plantage APRES : la copie est dans le fichier et n'a JAMAIS
  d'evenement INSERT ; un rewind avant le plantage ne peut pas la retirer,
  l'historique de l'entite commence sans son ajout ;
* ``capture_outbox._preserved_state`` ne recopiait dans le registre des
  commits interrompus que les attributs et la geometrie des ajouts : ni
  ``twins`` (la preuve qu'aucune entite ne tenait ce contenu avant le
  commit) ni ``unset_fields``. Le rapprochement ne pouvait donc pas savoir
  si un contenu retrouve etait l'ajout ou une entite d'avant.

Attendu : une correspondance qui est un jumeau enregistre est ecartee par
son FID quand ce FID est la cle stockee et qu'aucune suppression du meme
commit n'a pu le liberer ; ce qui reste decide (0 : non applique, 1 :
ecrit, plus : indetermine). Sans preuve complete (jumeaux tronques, FID
positionnels, FID d'un jumeau supprime par le meme commit) : indetermine.

Cas
---
D1  copie de SEED ("Dupliquer l'entite"), plantage apres l'etat "prepare",
    avant l'ecriture : non appliquee (exactement), aucun INSERT, fichier
    intact.
D2  la meme copie, plantage apres l'ecriture : ecrite (exactement), un
    INSERT qui designe la copie, jamais l'originale ; l'annuler supprime la
    copie seule.
D3  ajout ordinaire, plantage apres l'ecriture : le registre du commit
    interrompu garde ``twins`` ([] : verifie, aucun) et ``unset_fields``.
D4  registre construit a la main : le seul contenu retrouve est au FID d'un
    jumeau que le MEME commit supprimait (le FID a pu etre repris par la
    copie) : indetermine.
D5  registre construit a la main : jumeaux tronques (n_twins > liste) et
    une correspondance hors liste : indetermine, jamais "ecrit".
D6  registre construit a la main sur un Shapefile (FID positionnels) :
    un jumeau ne s'ecarte pas par son FID : indetermine.

D4 a D6 exigent aussi que le registre NOMME la raison (``twin_fid_not_provable``,
``twin_list_cut``) : "indetermine" sans raison ne dit pas a l'utilisateur ce
qui manque a la preuve.

Verdict attendu avant correctif : FAIL (D1 et D2 indetermines, D3 sans les
cles). Mesure avant correctif : D4 a D6 passaient sur les comptes (tout
ajout a jumeau etait indetermine, sans raison) ; la raison nommee a ete
ajoutee ensuite pour qu'ils gardent le correctif honnete : sans la regle
qu'ils visent (FID libere par une suppression, liste coupee, FID
positionnel), chacun echoue.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_reconcile_twins"
INVARIANT = "I-CRASH-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(path: str) -> None:
    """fid AUTOINCREMENT, statut DEFAULT 'AUTO', cree_le DEFAULT now."""
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
    cree = ogr.FieldDefn("cree_le", ogr.OFTDateTime)
    cree.SetDefault("CURRENT_TIMESTAMP")
    lyr.CreateField(cree)
    for code, x in (("SEED", 1), ("AUTRE", 2)):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("code", code)
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
    """(fid, code) of the file, sorted."""
    return sorted((f.id(), f["code"]) for f in _layer(path, "d_rows").getFeatures())


def _journal(journal_path: str) -> dict:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        inserts = [{"eid": e, "identity": i, "attrs": a} for e, i, a in conn.execute(
            "SELECT event_id, feature_identity_json, attributes_json FROM audit_event "
            "WHERE operation_type = 'INSERT' AND restored_from_event_id IS NULL "
            "ORDER BY event_id")]
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
    tr.connect_layer(_layer(path, "d_restart"))
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


_CHILD = r'''
from qgis.core import (QgsVectorLayer, QgsGeometry, QgsPointXY, QgsProject,
    QgsVectorLayerUtils)
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "d.gpkg")
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
wq = WriteQueue(); wq.start(jm.path)
tr = EditSessionTracker(wq, jm); tr.activate()
layer = QgsVectorLayer(gpkg + "|layername=pts", "pts", "ogr")
QgsProject.instance().addMapLayer(layer)
tr.connect_layer(layer)
layer.startEditing()
idx = layer.fields().indexFromName("code")
if os.environ.get("RL_ADD") == "duplicate":
    # The "Duplicate feature" action of the attribute table / map tool.
    seed = next(f for f in layer.getFeatures() if f["code"] == "SEED")
    dup = QgsVectorLayerUtils.duplicateFeature(layer, seed, QgsProject.instance(), 0)
    print("DUPLICATED", type(dup).__name__, flush=True)
else:
    layer.addFeature(QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)), {{idx: "NOUVEAU"}}))
print("COMMIT", layer.commitChanges(), flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _crash_case(ctx, key: str, fault: str, add: str) -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], key)
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "d.gpkg")
    _make_gpkg(gpkg)
    res = {"seed_fid": next((fid for fid, code in _rows(gpkg) if code == "SEED"), None)}
    rc, so, se = run_child(_CHILD.format(workdir=work), fault=fault, timeout=180,
                           workdir=work, extra_env={"RL_ADD": add})
    res.update({"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
                "stdout": [ln for ln in so.splitlines()
                           if ln.startswith(("DUPLICATED", "COMMIT"))],
                "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else "",
                "file_before_restart": _rows(gpkg)})
    res.update(_restart(work, gpkg))
    res["file_after_restart"] = _rows(gpkg)
    return res


def _undo_inserts(res: dict, path: str) -> None:
    """Undo every INSERT the journal holds: what a rewind before the crash does.

    With the journal handed over, as the restore executor does: the copy
    shares its content with the original, only the journal can prove that
    its key never changed hands.
    """
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_inserted_feature
    undos = []
    for ins in res.get("inserts") or []:
        conn = sqlite3.connect(f"file:{Path(res['journal']).as_posix()}?mode=ro", uri=True)
        try:
            event = get_event_by_id(conn, ins["eid"])
            out = restore_inserted_feature(_layer(path, f"d_undo_{ins['eid']}"), event,
                                           journal=conn)
        finally:
            conn.close()
        undos.append({k: out.get(k) for k in ("success", "identity_level",
                                              "reason_code", "message")})
    res["undos"] = undos
    res["file_after_undo"] = _rows(path)


def _case_d1(ctx) -> dict:
    return _crash_case(ctx, "d1", "capture_after_prepare", "duplicate")


def _case_d2(ctx) -> dict:
    res = _crash_case(ctx, "d2", "capture_after_provider_commit", "duplicate")
    _undo_inserts(res, os.path.join(ctx.data["tmpdir"], "d2", "d.gpkg"))
    return res


def _case_d3(ctx) -> dict:
    return _crash_case(ctx, "d3", "capture_after_provider_commit", "plain")


def _crafted(ctx, key: str, path: str, snapshots_of) -> dict:
    """An INTERRUPTED ledger row built by hand, then a restart."""
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.journal_writes import ensure_v7_schema, record_capture_commit
    from recoverland.core.identity import compute_datasource_fingerprint
    work = os.path.dirname(path)
    open(os.path.join(work, "p.qgz"), "w").close()
    layer = _layer(path, f"{key}_fix")
    snaps = snapshots_of(layer)
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
                conn, f"{key}crafted", "INTERRUPTED", layer_id="crafted",
                layer_name="pts", datasource_fingerprint=fp, session_id=key,
                prepared_at="2026-09-27T10:00:00+00:00", finalized_at=None,
                n_snapshots=len(snaps), n_events=0,
                detail=f"scenario {SCENARIO_ID} {key}",
                snapshots_json=json.dumps(snaps))
    finally:
        conn.close()
    res = {"file_before_restart": _rows(path)}
    res.update(_restart(work, path))
    res["file_after_restart"] = _rows(path)
    return res


def _content_of(feat) -> dict:
    """What the prepare record keeps of an addition copying *feat*."""
    import base64
    from recoverland.core.geometry_utils import geometry_to_wkb
    wkb = geometry_to_wkb(feat.geometry())
    return {"attributes": {"code": feat["code"]},
            "geometry_wkb": "b64:" + base64.b64encode(bytes(wkb)).decode("ascii")}


def _add_feature(path: str, code: str, x: float) -> int:
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    layer = _layer(path, "d_add")
    feat = QgsFeature(layer.fields())
    feat.setAttribute("code", code)
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, x)))
    ok, added = layer.dataProvider().addFeatures([feat])
    if not ok:
        raise RuntimeError("fixture addFeatures refused")
    return added[0].id()


def _case_d4(ctx) -> dict:
    """The only match sits at the FID of a twin the same commit deleted."""
    work = os.path.join(ctx.data["tmpdir"], "d4")
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "d.gpkg")
    _make_gpkg(gpkg)

    def snaps(layer):
        seed = next(f for f in layer.getFeatures() if f["code"] == "SEED")
        from recoverland.core.identity import compute_feature_identity
        deleted = {"kind": "DELETE", "fid": seed.id(),
                   "identity": compute_feature_identity(layer, seed),
                   "attributes": {"code": "SEED", "statut": "AVANT"},
                   "geometry_wkb": _content_of(seed)["geometry_wkb"]}
        copy = dict(_content_of(seed), kind="INSERT_UNPROVEN", fid=None,
                    unset_fields=["fid", "statut", "cree_le"], twins=[seed.id()],
                    n_twins=1, unproven="content_matches_existing_features")
        return [deleted, copy]
    return _crafted(ctx, "d4", gpkg, snaps)


def _case_d5(ctx) -> dict:
    """Truncated twin list: a match outside it may be an unlisted original."""
    work = os.path.join(ctx.data["tmpdir"], "d5")
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "d.gpkg")
    _make_gpkg(gpkg)
    _add_feature(gpkg, "SEED", 1.0)

    def snaps(layer):
        seed = min((f for f in layer.getFeatures() if f["code"] == "SEED"),
                   key=lambda f: f.id())
        return [dict(_content_of(seed), kind="INSERT_UNPROVEN", fid=None,
                     unset_fields=["fid", "statut", "cree_le"], twins=[seed.id()],
                     n_twins=2, unproven="content_matches_existing_features")]
    return _crafted(ctx, "d5", gpkg, snaps)


def _case_d6(ctx) -> dict:
    """Shapefile: FIDs are positions, a twin is not excluded by its FID."""
    from osgeo import ogr, osr
    work = os.path.join(ctx.data["tmpdir"], "d6")
    os.makedirs(work, exist_ok=True)
    shp = os.path.join(work, "d.shp")
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(shp)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("d", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    for code, x in (("SEED", 1), ("AUTRE", 2)):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("code", code)
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({x} {x})"))
        lyr.CreateFeature(feat)
    ds = None

    def snaps(layer):
        seed = next(f for f in layer.getFeatures() if f["code"] == "SEED")
        return [dict(_content_of(seed), kind="INSERT_UNPROVEN", fid=None,
                     unset_fields=[], twins=[seed.id()], n_twins=1,
                     unproven="content_matches_existing_features")]
    return _crafted(ctx, "d6", shp, snaps)


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_c_dup_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("d1", _case_d1), ("d2", _case_d2), ("d3", _case_d3),
                    ("d4", _case_d4), ("d5", _case_d5), ("d6", _case_d6)):
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


def _names(ins: dict, fid) -> bool:
    try:
        ident = json.loads(ins.get("identity") or "{}")
    except ValueError:
        return True
    return fid is not None and (ident.get("fid") == fid or ident.get("pk_value") == fid)


def _interrupted_additions(res: dict) -> list:
    out = []
    for row in res.get("ledger") or []:
        for snap in row.get("snapshots") or []:
            if str(snap.get("kind", "")).startswith("INSERT"):
                out.append({k: v for k, v in snap.items() if k != "geometry_wkb"})
    return out


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    d1, d2, d3 = ctx.data.get("d1", {}), ctx.data.get("d2", {}), ctx.data.get("d3", {})

    # ===== D1: duplicate, crash before the provider wrote ===============
    seed1 = d1.get("seed_fid")
    out.append((
        "d1_fixture_duplicate_prepared_then_crash_before_write",
        d1.get("fault_reached") is True and seed1 is not None
        and d1.get("file_before_restart") == [(1, "SEED"), (2, "AUTRE")]
        and any(a.get("kind") == "INSERT_UNPROVEN" and a.get("twins") == [seed1]
                for a in _interrupted_additions(d1)),
        f"rc={d1.get('rc')} stdout={d1.get('stdout')} fichier={d1.get('file_before_restart')} "
        f"ajouts du registre={_interrupted_additions(d1)} stderr={d1.get('stderr')} "
        f"crash={d1.get('crash')} {d1.get('trace', '')}: sans jumeau enregistre le "
        f"cas ne prouve rien",
    ))
    out.append((
        "d1_duplicate_never_written_is_not_applied",
        _totals(d1) == {"landed": 0, "not_applied": 1, "unresolved": 0}
        and not d1.get("inserts")
        and d1.get("file_after_restart") == [(1, "SEED"), (2, "AUTRE")],
        f"rapprochement={d1.get('reconciled')} attendu landed=0 not_applied=1 "
        f"unresolved=0 ; INSERT={d1.get('inserts')} ; fichier={d1.get('file_after_restart')}. "
        f"Le seul contenu retrouve est l'originale, jumeau enregistre avant le "
        f"commit : la copie n'a pas ete ecrite, et une alerte 'indeterminee' "
        f"annonce a l'utilisateur une incertitude qui n'existe pas",
    ))

    # ===== D2: duplicate, crash after the provider wrote ================
    seed2 = d2.get("seed_fid")
    before2 = d2.get("file_before_restart") or []
    copy2 = [fid for fid, code in before2 if code == "SEED" and fid != seed2]
    out.append((
        "d2_fixture_duplicate_written_then_crash",
        d2.get("fault_reached") is True and seed2 is not None and len(copy2) == 1,
        f"rc={d2.get('rc')} stdout={d2.get('stdout')} fichier={before2} "
        f"stderr={d2.get('stderr')} crash={d2.get('crash')} {d2.get('trace', '')}",
    ))
    ins2 = d2.get("inserts") or []
    out.append((
        "d2_duplicate_written_is_journalled_as_the_copy",
        _totals(d2) == {"landed": 1, "not_applied": 0, "unresolved": 0}
        and len(ins2) == 1 and bool(copy2) and _names(ins2[0], copy2[0])
        and not any(_names(i, seed2) for i in ins2),
        f"rapprochement={d2.get('reconciled')} attendu landed=1 ; INSERT={ins2} ; "
        f"copie fid={copy2} originale fid={seed2}. La copie est dans le fichier : "
        f"sans evenement INSERT un rewind avant le plantage ne peut pas la "
        f"retirer, et son historique commence sans son ajout",
    ))
    undos2 = d2.get("undos") or []
    out.append((
        "d2_undoing_the_reconciled_insert_removes_the_copy_only",
        len(undos2) == 1 and undos2[0].get("success") is True
        and d2.get("file_after_undo") == [(seed2, "SEED"), (2, "AUTRE")],
        f"annulations={undos2} ; fichier apres={d2.get('file_after_undo')} attendu "
        f"[({seed2}, 'SEED'), (2, 'AUTRE')] : l'annulation doit retirer la copie et "
        f"laisser l'originale de l'utilisateur",
    ))

    # ===== D3: the preserved state keeps what the verdict needs ==========
    adds3 = [a for a in _interrupted_additions(d3) if a.get("kind") == "INSERT"]
    out.append((
        "d3_interrupted_commit_keeps_twins_and_unset_fields",
        d3.get("fault_reached") is True and len(adds3) == 1
        and adds3[0].get("twins") == []
        and "statut" in (adds3[0].get("unset_fields") or [])
        and "cree_le" in (adds3[0].get("unset_fields") or [])
        and _totals(d3) == {"landed": 1, "not_applied": 0, "unresolved": 0},
        f"ajouts du registre={adds3} ; rapprochement={d3.get('reconciled')} crash="
        f"{d3.get('crash')} {d3.get('trace', '')}. Sans 'twins' le registre ne "
        f"dit pas qu'aucune entite ne tenait ce contenu avant le commit, sans "
        f"'unset_fields' il confond un NULL saisi et un champ laisse a la source",
    ))

    # ===== D4..D6: incomplete proofs stay unresolved, and say why ========
    for key, reason, why in (
            ("d4", "twin_fid_not_provable=1",
             "le FID du jumeau etait libere par une suppression du meme commit : "
             "la copie a pu le reprendre"),
            ("d5", "twin_list_cut=1",
             "la liste des jumeaux est tronquee : la correspondance hors liste "
             "peut etre une originale non listee"),
            ("d6", "twin_fid_not_provable=1",
             "Shapefile : un FID est une position, il ne designe pas l'entite "
             "d'avant le commit")):
        res = ctx.data.get(key, {})
        tot = _totals(res)
        details = [row.get("detail") or "" for row in res.get("ledger") or []
                   if row.get("state") == "RECONCILED"]
        out.append((
            f"{key}_incomplete_twin_proof_stays_unresolved",
            len(res.get("reconciled") or []) == 1 and tot["landed"] == 0
            and tot["unresolved"] >= 1 and not res.get("inserts")
            and any(reason in d for d in details)
            and res.get("file_after_restart") == res.get("file_before_restart"),
            f"rapprochement={res.get('reconciled')} ; registre={details} (attendu "
            f"la raison {reason}) ; INSERT={res.get('inserts')} ; fichier="
            f"{res.get('file_after_restart')} crash={res.get('crash')} "
            f"{res.get('trace', '')}. Attendu indetermine, et nomme : {why}",
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
