"""tx_capture_insert_provider_values - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (un INSERT journalise ce que le provider a
reellement stocke, jamais un espace reserve).

Le mecanisme
------------
Le formulaire de saisie de QGIS construit la nouvelle entite avec
`QgsVectorLayerUtils.createFeature` : chaque champ que la source remplit
elle-meme (DEFAULT d'une colonne, cle auto-incrementee) recoit un
`QgsUnsetAttributeValue`, qui veut dire "laisse la source decider". Mesure
sur QGIS 4.2.1 : le signal `committedFeaturesAdded` rend l'entite avec le
FID attribue par la source, mais ses attributs sont TOUJOURS ces marqueurs.
GeoPackage (DEFAULT 'AUTO', DEFAULT CURRENT_TIMESTAMP) : le signal porte
``<QgsUnsetAttributeValue: AUTO>`` quand le fichier contient ``AUTO`` et
un horodatage. Spatialite (``pk INTEGER PRIMARY KEY AUTOINCREMENT``) : la
cle elle-meme arrive en ``<QgsUnsetAttributeValue: Autogenerate>``.

`EditSessionTracker._on_committed_features_added` serialisait ce signal tel
quel. Le journal gardait donc le texte de l'espace reserve :

  * annuler l'INSERT (restore, undo, rewind) compare l'instantane au
    contenu du fichier : 'AUTO' ne vaut pas '<QgsUnsetAttributeValue: AUTO>',
    la preuve d'identite tombe a WEAK et la suppression est refusee. La
    saisie de l'utilisateur est inannulable ;
  * sur Spatialite l'identite porte ``pk_value =
    "<QgsUnsetAttributeValue: Autogenerate>"`` : la recherche par cle ne
    trouve rien, et TOUTES les entites ajoutees partagent la meme empreinte
    d'entite, leurs historiques se melangent ;
  * l'enregistrement "prepare" (outbox) porte les memes marqueurs : apres
    un plantage pendant le commit, le rapprochement ne reconnait pas
    l'entite ajoutee dans la source, la declare "non appliquee", et l'ajout
    reellement ecrit n'a jamais d'evenement.

Cas
---
P1  GeoPackage a DEFAULT, deux entites saisies comme par le formulaire :
    chaque INSERT porte exactement les valeurs relues dans la source ;
    annuler l'un d'eux est prouve (STRONG/MEDIUM) et le supprime, lui seul.
P2  Spatialite a cle auto-incrementee : cle reelle dans l'identite et dans
    l'instantane, empreintes d'entite distinctes, annulation prouvee.
P3  plantage de QGIS juste apres l'ecriture du commit (processus enfant,
    os._exit) : au redemarrage l'ajout est reconnu et journalise avec les
    valeurs de la source, pas declare "non applique".
P3b le jumeau (v7, second tour). Pour que P3 reconnaisse l'ajout, l'etat
    "prepare" ne compare plus les champs laisses a la source (la cle fid du
    GeoPackage en fait partie) : il ne reste que le contenu saisi. Or
    "Coller les entites" ou "Dupliquer l'entite" produisent exactement le
    contenu d'une entite qui EXISTE deja. QGIS s'arrete apres l'etat
    "prepare", AVANT que la source n'ecrive : au redemarrage le
    rapprochement trouve une seule entite de meme contenu, l'ORIGINALE, et
    journalise pour elle un INSERT. Annuler cet INSERT (undo, restore,
    rewind avant le plantage) supprime l'entite d'origine de l'utilisateur
    (mesure par la revue : STRONG, 'Deleted (undo insert)'). Attendu :
    aucun INSERT ne designe l'originale, rien n'est declare "ecrit", le
    commit est compte (non applique ou indetermine), le fichier intact.
P3c le meme jumeau, plantage APRES l'ecriture : le fichier tient
    l'originale et la copie. Aucun INSERT ne designe l'originale ; s'il en
    existe un il designe la copie, et l'annuler laisse l'originale.
P3d enchainement brutal : le meme commit renomme l'originale en
    'RENOMME' et ajoute une copie 'RENOMME' au meme endroit ; QGIS meurt
    une fois l'etape attributs ecrite, avant l'etape ajouts. Avant le
    commit l'originale ne ressemblait pas a la copie ; apres le plantage
    elle lui est identique. Aucun INSERT ne doit la designer.
P3e table sans geometrie (seul le contenu attributaire identifie) : une
    ligne dupliquee, plantage avant l'ecriture ; meme attendu que P3b.
P3f la copie noyee dans un collage de cinq entites (recherche en une
    requete sur l'emprise du lot) : 5 non appliquees (v7 : la copie, dont
    le seul contenu retrouve est son jumeau enregistre SEED, est prouvee
    non ecrite ; elle etait comptee indeterminee).
P3g le meme collage, la requete unique plafonnee a 0 entite : elle doit
    renoncer et retomber sur une requete par ajout, pas sauter le controle.
P4  couche filtree dont le filtre exclut la nouvelle entite : elle est
    ecrite dans le fichier et disparait de la vue. La relecture par FID
    d'OGR ignore le filtre (mesure) : l'INSERT porte les valeurs du fichier.
P4b la meme chose sur Spatialite, cas reel (pas simule) : ce provider
    applique le filtre de la couche meme a une lecture par FID (mesure) ;
    la relecture ne rendait rien, la cle etait retiree de l'instantane,
    l'INSERT etait classe sous l'entite 'fid:2' quand chaque evenement
    suivant sur la meme entite l'est sous 'pk:pk=2' : son historique
    coupe en deux, et l'ecart n'etait montre qu'au journal technique.
    Attendu : l'INSERT porte les valeurs du fichier et la vraie cle, et
    une modification ulterieure de la meme entite tombe dans la MEME
    entite d'historique.
P5  la relecture ne rend pas l'entite (lecture echouee, provider qui
    applique le filtre a une lecture par FID ; simule en vidant la
    relecture) : aucune valeur inventee, les champs laisses a la source sont
    ABSENTS de l'instantane (inconnus, pas NULL), l'identite garde la cle
    que QGIS a rendue, et le trou est annonce. Annonce veut dire VU : le
    statut emis par le suivi, passe au vrai RecoverPlugin, doit produire
    un message dans la barre de messages qui nomme la couche.
P6  lot de 23 ajouts Spatialite relu par paquets de 7 (taille de paquet
    abaissee pour le cas) : chaque INSERT porte SA cle, aucune perdue ni
    dupliquee aux frontieres de paquet.

Verdict attendu avant correctif : FAIL (P1..P6 au premier tour ; P3b, P4b
et P5 au second).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_insert_provider_values"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLACEHOLDER = "QgsUnsetAttributeValue"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_defaults_gpkg(path: str) -> None:
    """fid AUTOINCREMENT, statut DEFAULT 'AUTO', cree_le DEFAULT now, n DEFAULT 7."""
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
    n = ogr.FieldDefn("n", ogr.OFTInteger)
    n.SetDefault("7")
    lyr.CreateField(n)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("code", "SEED")
    feat.SetGeometry(ogr.CreateGeometryFromWkt("POINT (1 1)"))
    lyr.CreateFeature(feat)
    feat = None
    ds = None


def _make_spatialite(path: str) -> None:
    from osgeo import ogr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("SQLite").CreateDataSource(
        path, options=["SPATIALITE=YES"])
    ds.ExecuteSQL("CREATE TABLE pts (pk INTEGER PRIMARY KEY AUTOINCREMENT, "
                  "code TEXT, statut TEXT DEFAULT 'AUTO')")
    ds.ExecuteSQL("SELECT AddGeometryColumn('pts', 'geom', 2154, 'POINT', 'XY')")
    ds.ExecuteSQL("INSERT INTO pts (code, geom) VALUES ('SEED', MakePoint(1, 1, 2154))")
    ds = None


def _add_plain_table(path: str) -> None:
    """A table without geometry in the same GeoPackage, one row 'SEED'."""
    from osgeo import ogr
    ogr.UseExceptions()
    ds = ogr.Open(path, 1)
    lyr = ds.CreateLayer("tab", None, ogr.wkbNone)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("code", "SEED")
    lyr.CreateFeature(feat)
    feat = None
    ds = None


def _gpkg_layer(path: str, name: str, subset: str = "", table: str = "pts"):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername={table}", name, "ogr")
    if subset:
        layer.setSubsetString(subset)
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


def _spatialite_layer(path: str, name: str, subset: str = ""):
    from qgis.core import QgsVectorLayer, QgsDataSourceUri
    uri = QgsDataSourceUri()
    uri.setDatabase(path)
    uri.setDataSource("", "pts", "geom", subset, "pk")
    layer = QgsVectorLayer(uri.uri(), name, "spatialite")
    if not layer.isValid():
        raise RuntimeError(f"invalid spatialite layer {path}")
    return layer


class _Stack:
    """Real journal + real WriteQueue + real tracker."""

    def __init__(self, workdir: str):
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker

        os.makedirs(workdir, exist_ok=True)
        self.jm = JournalManager()
        self.jm.open_for_project(
            project_path=os.path.join(workdir, "audit.qgz"),
            profile_path=workdir)
        self.path = self.jm.path
        self.wq = WriteQueue()
        self.wq.start(self.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.statuses = []
        if hasattr(self.tracker, "set_capture_status_callback"):
            self.tracker.set_capture_status_callback(
                lambda kind, detail: self.statuses.append((kind, dict(detail))))
        self.tracker.activate()

    def connect(self, layer) -> bool:
        self.tracker.connect_layer(layer)
        return layer.id() in self.tracker._connected_layers

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


def _form_insert(layer, code: str, x: float) -> list:
    """What the digitising form does: createFeature, then addFeature.

    Returns the type names the buffered feature carries, so the fixture can
    prove the placeholders were really there.
    """
    from qgis.core import QgsGeometry, QgsPointXY, QgsVectorLayerUtils
    idx = layer.fields().indexFromName("code")
    feat = QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(x, x)), {idx: code})
    if not layer.addFeature(feat):
        raise RuntimeError("addFeature refused")
    return [type(v).__name__ for v in feat.attributes()]


def _insert_events(journal_path: str) -> list:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT event_id, feature_identity_json, attributes_json, "
            "entity_fingerprint, event_schema_version FROM audit_event "
            "WHERE operation_type = 'INSERT' AND restored_from_event_id IS NULL "
            "ORDER BY event_id").fetchall()
    finally:
        conn.close()
    out = []
    for eid, ident, attrs, entity, version in rows:
        try:
            parsed = (json.loads(attrs or "{}") or {}).get("all_attributes")
        except ValueError:
            parsed = None
        out.append({"eid": eid, "identity": ident, "attrs": parsed,
                    "raw_attrs": attrs, "entity": entity, "version": version})
    return out


def _provider_rows(layer) -> dict:
    """Provider content serialised by the product's own serializer, by FID."""
    from recoverland.core.serialization import serialize_attributes
    names = [f.name() for f in layer.dataProvider().fields()]
    return {f.id(): serialize_attributes(f, names)
            for f in layer.dataProvider().getFeatures()}


def _undo_insert(journal_path: str, eid: int, layer) -> dict:
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_inserted_feature
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        event = get_event_by_id(conn, eid)
    finally:
        conn.close()
    if event is None:
        return {"error": f"event {eid} not found"}
    res = restore_inserted_feature(layer, event)
    return {k: res.get(k) for k in ("success", "identity_level", "reason_code",
                                    "message")}


def _has_placeholder(events) -> list:
    return [e["eid"] for e in events
            if _PLACEHOLDER in (e.get("raw_attrs") or "")
            or _PLACEHOLDER in (e.get("identity") or "")
            or _PLACEHOLDER in (e.get("entity") or "")]


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def _case_p1(ctx) -> dict:
    work = os.path.join(ctx.data["tmpdir"], "p1")
    stack = _Stack(work)
    path = os.path.join(work, "defauts.gpkg")
    _make_defaults_gpkg(path)
    layer = _gpkg_layer(path, "p1")
    res = {"connected": stack.connect(layer)}
    layer.startEditing()
    res["buffer_types"] = _form_insert(layer, "NEW1", 5.0)
    _form_insert(layer, "NEW2", 6.0)
    res["commit_ok"] = bool(layer.commitChanges())
    _pump(0.3)
    stack.shutdown()
    res["events"] = _insert_events(stack.path)
    probe = _gpkg_layer(path, "p1_probe")
    res["provider"] = _provider_rows(probe)
    first = next((e for e in res["events"]
                  if (e.get("attrs") or {}).get("code") == "NEW1"), None)
    if first is not None:
        res["undo"] = _undo_insert(stack.path, first["eid"], probe)
        res["provider_after_undo"] = sorted(
            r.get("code") for r in _provider_rows(probe).values())
    return res


def _case_p2(ctx) -> dict:
    work = os.path.join(ctx.data["tmpdir"], "p2")
    stack = _Stack(work)
    path = os.path.join(work, "auto.sqlite")
    _make_spatialite(path)
    layer = _spatialite_layer(path, "p2")
    res = {"connected": stack.connect(layer)}
    layer.startEditing()
    res["buffer_types"] = _form_insert(layer, "NEW1", 5.0)
    _form_insert(layer, "NEW2", 6.0)
    res["commit_ok"] = bool(layer.commitChanges())
    _pump(0.3)
    stack.shutdown()
    res["events"] = _insert_events(stack.path)
    probe = _spatialite_layer(path, "p2_probe")
    res["provider"] = _provider_rows(probe)
    first = next((e for e in res["events"]
                  if (e.get("attrs") or {}).get("code") == "NEW1"), None)
    if first is not None:
        res["undo"] = _undo_insert(stack.path, first["eid"], probe)
        res["provider_after_undo"] = sorted(
            r.get("code") for r in _provider_rows(probe).values())
    return res


_CHILD = r'''
from qgis.core import QgsVectorLayer, QgsGeometry, QgsPointXY, QgsVectorLayerUtils
from qgis.PyQt.QtCore import QCoreApplication
from recoverland.core.journal_manager import JournalManager
from recoverland.core.write_queue import WriteQueue
from recoverland.core.edit_tracker import EditSessionTracker
workdir = {workdir!r}
gpkg = os.path.join(workdir, "defauts.gpkg")
project = os.path.join(workdir, "p.qgz")
open(project, "w").close()
jm = JournalManager(); jm.open_for_project(project, workdir)
print("JOURNAL", jm.path, flush=True)
wq = WriteQueue(); wq.start(jm.path)
if os.environ.get("RL_PATCH_CRASH") == "after_provider_commit":
    EditSessionTracker._on_after_commit = lambda self, lid: os._exit(86)
if os.environ.get("RL_UNION_CAP"):
    EditSessionTracker._TWIN_UNION_CANDIDATES = int(os.environ["RL_UNION_CAP"])
tr = EditSessionTracker(wq, jm); tr.activate()
layer = QgsVectorLayer(gpkg + "|layername=" + os.environ.get("RL_TABLE", "pts"), "pts", "ogr")
tr.connect_layer(layer)
layer.startEditing()
idx = layer.fields().indexFromName("code")
twin = os.environ.get("RL_TWIN", "")
if twin in ("1", "post"):
    # Paste / Duplicate: the content of the feature already stored (SEED),
    # every field the source fills (fid, DEFAULTs) left to it.
    seed = next(f for f in layer.getFeatures() if f["code"] == "SEED")
    code = "SEED"
    if twin == "post":
        # The original is renamed in the same commit to what the copy
        # holds, and QGIS dies once the provider wrote the attribute stage,
        # before the additions stage.
        code = "RENOMME"
        layer.changeAttributeValue(seed.id(), idx, code)
        layer.committedAttributeValuesChanges.connect(lambda *a: os._exit(86))
    nf = QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry(seed.geometry()), {{idx: code}})
elif twin == "batch":
    # A paste of five: four new features around SEED and one copy of it
    # (the batch goes through the one-query search of the whole paste).
    seed = next(f for f in layer.getFeatures() if f["code"] == "SEED")
    for i in range(4):
        layer.addFeature(QgsVectorLayerUtils.createFeature(
            layer, QgsGeometry.fromPointXY(QgsPointXY(1.5 + i, 1.5)), {{idx: "LOT%d" % i}}))
    nf = QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry(seed.geometry()), {{idx: "SEED"}})
else:
    nf = QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)), {{idx: "CRASH"}})
layer.addFeature(nf)
ok = layer.commitChanges()
print("COMMIT", ok, flush=True)
for _ in range(40):
    QCoreApplication.processEvents(); time.sleep(0.05)
os._exit(0)
'''


def _restart_and_reconnect(workdir: str, table: str = "pts") -> dict:
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
    if hasattr(tr, "set_capture_status_callback"):
        tr.set_capture_status_callback(lambda k, d: statuses.append((k, dict(d))))
    tr.activate()
    layer = _gpkg_layer(os.path.join(workdir, "defauts.gpkg"), "p3_restart",
                        table=table)
    tr.connect_layer(layer)
    _pump(0.3)
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    tr.deactivate()
    tr.disconnect_all()
    wq.stop()
    path = jm.path
    jm.close()
    return {"journal": path,
            "reconciled": [{k: v for k, v in d.items()
                            if k in ("landed", "not_applied", "unresolved")}
                           for kind, d in statuses if kind == "reconciled"]}


def _case_p3(ctx) -> dict:
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], "p3")
    os.makedirs(work, exist_ok=True)
    _make_defaults_gpkg(os.path.join(work, "defauts.gpkg"))
    # The fault point where it exists (v7 capture); otherwise the same crash
    # obtained by patching the slot that would journal the commit.
    import recoverland.core.edit_tracker as et_mod
    has_fault = "capture_after_provider_commit" in Path(et_mod.__file__).read_text(
        encoding="utf-8")
    rc, so, se = run_child(
        _CHILD.format(workdir=work),
        fault="capture_after_provider_commit" if has_fault else "",
        timeout=180, workdir=work,
        extra_env=None if has_fault else {"RL_PATCH_CRASH": "after_provider_commit"})
    res = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
           "stdout": [ln for ln in so.splitlines()
                      if ln.startswith(("JOURNAL", "COMMIT"))],
           "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else ""}
    probe = _gpkg_layer(os.path.join(work, "defauts.gpkg"), "p3_probe")
    res["provider"] = _provider_rows(probe)
    del probe
    res.update(_restart_and_reconnect(work))
    res["events"] = _insert_events(res["journal"])
    return res


def _case_twin(ctx, key: str, fault: str, twin: str = "1",
               table: str = "pts", extra_env=None) -> dict:
    """P3b..P3g: a copy of SEED committed, QGIS killed at *fault*."""
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    work = os.path.join(ctx.data["tmpdir"], key)
    os.makedirs(work, exist_ok=True)
    gpkg = os.path.join(work, "defauts.gpkg")
    _make_defaults_gpkg(gpkg)
    if table != "pts":
        _add_plain_table(gpkg)

    def rows(tag):
        return sorted((fid, a.get("code")) for fid, a in _provider_rows(
            _gpkg_layer(gpkg, f"{key}_{tag}", table=table)).items())

    res = {"seed_fid": next((fid for fid, code in rows("seed") if code == "SEED"), None)}
    rc, so, se = run_child(_CHILD.format(workdir=work), fault=fault, timeout=180,
                           workdir=work,
                           extra_env=dict({"RL_TWIN": twin, "RL_TABLE": table},
                                          **(extra_env or {})))
    res.update({"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
                "stderr": se[-400:] if rc not in (0, FAULT_EXIT_CODE) else ""})
    res["file_before_restart"] = rows("before")
    res.update(_restart_and_reconnect(work, table))
    res["events"] = _insert_events(res["journal"])
    res["file_after_restart"] = rows("after")
    # What the user gets from the journal: undo every INSERT it now holds
    # (the undo button, a restore, a rewind before the crash all do this).
    undo_layer = _gpkg_layer(gpkg, f"{key}_undo", table=table)
    res["undos"] = [dict(_undo_insert(res["journal"], e["eid"], undo_layer),
                         identity=e.get("identity"))
                    for e in res["events"]]
    res["file_after_undo"] = rows("final")
    return res


def _case_p3b(ctx) -> dict:
    return _case_twin(ctx, "p3b", "capture_after_prepare")


def _case_p3c(ctx) -> dict:
    return _case_twin(ctx, "p3c", "capture_after_provider_commit")


def _case_p3d(ctx) -> dict:
    # The crash comes from the child's own slot on the attribute stage.
    return _case_twin(ctx, "p3d", "", twin="post")


def _case_p3e(ctx) -> dict:
    return _case_twin(ctx, "p3e", "capture_after_prepare", table="tab")


def _case_p3f(ctx) -> dict:
    return _case_twin(ctx, "p3f", "capture_after_prepare", twin="batch")


def _case_p3g(ctx) -> dict:
    # Same paste, the one-query search capped at 0 feature: it must give up
    # and fall back to one query per addition, not skip the check.
    return _case_twin(ctx, "p3g", "capture_after_prepare", twin="batch",
                      extra_env={"RL_UNION_CAP": "0"})


def _case_p4(ctx) -> dict:
    work = os.path.join(ctx.data["tmpdir"], "p4")
    stack = _Stack(work)
    path = os.path.join(work, "filtre.gpkg")
    _make_defaults_gpkg(path)
    # The layer shows everything but code 'CACHE': the new feature is written
    # to the file and vanishes from the view.
    layer = _gpkg_layer(path, "p4", subset="\"code\" <> 'CACHE'")
    res = {"connected": stack.connect(layer)}
    layer.startEditing()
    res["buffer_types"] = _form_insert(layer, "CACHE", 7.0)
    res["commit_ok"] = bool(layer.commitChanges())
    _pump(0.3)
    stack.shutdown()
    res["events"] = _insert_events(stack.path)
    res["provider"] = _provider_rows(_gpkg_layer(path, "p4_probe"))
    return res


def _case_p4b(ctx) -> dict:
    """Spatialite filtered layer: the provider filters even a read by FID."""
    from qgis.core import QgsFeatureRequest
    work = os.path.join(ctx.data["tmpdir"], "p4b")
    stack = _Stack(work)
    path = os.path.join(work, "filtre.sqlite")
    _make_spatialite(path)
    layer = _spatialite_layer(path, "p4b", subset="\"code\" <> 'CACHE'")
    plain = _spatialite_layer(path, "p4b_plain")
    res = {"connected": stack.connect(layer) and stack.connect(plain)}
    layer.startEditing()
    res["buffer_types"] = _form_insert(layer, "CACHE", 7.0)
    added = []
    layer.committedFeaturesAdded.connect(
        lambda _lid, feats: added.extend(f.id() for f in feats))
    res["commit_ok"] = bool(layer.commitChanges())
    _pump(0.3)
    # Fixture: the provider of the filtered layer really does not return the
    # new feature on a read by FID (else the case proves nothing).
    res["filtered_read_by_fid"] = [
        f.id() for f in layer.dataProvider().getFeatures(
            QgsFeatureRequest().setFilterFids(added))] if added else None
    res["added_fids"] = list(added)
    # The same feature edited later, through the unfiltered layer: its
    # UPDATE must land in the entity of the INSERT.
    target = next((f for f in plain.getFeatures() if f["code"] == "CACHE"), None)
    if target is not None:
        plain.startEditing()
        plain.changeAttributeValue(target.id(), plain.fields().indexFromName("statut"),
                                   "VU")
        res["edit_ok"] = bool(plain.commitChanges())
        _pump(0.3)
    stack.shutdown()
    res["events"] = _insert_events(stack.path)
    conn = sqlite3.connect(f"file:{Path(stack.path).as_posix()}?mode=ro", uri=True)
    try:
        res["updates"] = [{"identity": i, "entity": e} for i, e in conn.execute(
            "SELECT feature_identity_json, entity_fingerprint FROM audit_event "
            "WHERE operation_type = 'UPDATE' ORDER BY event_id").fetchall()]
    finally:
        conn.close()
    res["statuses"] = [k for k, _d in stack.statuses]
    res["provider"] = _provider_rows(_spatialite_layer(path, "p4b_probe"))
    return res


def _case_p5(ctx) -> dict:
    from recoverland.core.edit_tracker import EditSessionTracker
    work = os.path.join(ctx.data["tmpdir"], "p5")
    stack = _Stack(work)
    path = os.path.join(work, "relecture.gpkg")
    _make_defaults_gpkg(path)
    layer = _gpkg_layer(path, _P5_LAYER)
    res = {"connected": stack.connect(layer)}
    # The provider returns nothing on the read-back (a failed read, or a
    # provider that applies the layer filter to a read by FID). Harmless on
    # a code without read-back: the attribute is simply never called.
    had = "_read_back_committed" in EditSessionTracker.__dict__
    saved = EditSessionTracker.__dict__.get("_read_back_committed")
    EditSessionTracker._read_back_committed = lambda self, lyr, feats: {}
    try:
        layer.startEditing()
        res["buffer_types"] = _form_insert(layer, "PERDU", 7.0)
        res["commit_ok"] = bool(layer.commitChanges())
        _pump(0.3)
    finally:
        if had:
            EditSessionTracker._read_back_committed = saved
        else:
            del EditSessionTracker._read_back_committed
    stack.shutdown()
    res["events"] = _insert_events(stack.path)
    res["provider"] = _provider_rows(_gpkg_layer(path, "p5_probe"))
    res["statuses"] = [(k, {kk: vv for kk, vv in d.items()
                            if kk in ("fids", "fields", "layer_name")})
                       for k, d in stack.statuses]
    gap = next((d for k, d in stack.statuses if k == "insert_values_unverified"), None)
    if gap is not None:
        res["shown"] = _shown_by_plugin("insert_values_unverified", gap)
    return res


_P5_LAYER = "couche_p5_relecture"


def _shown_by_plugin(kind: str, detail: dict) -> dict:
    """What the real RecoverPlugin puts in front of the user for a status.

    The tracker's status callback IS RecoverPlugin._on_capture_status
    (recover.py wires it in _init_local_backend); the status is handed to it
    exactly as the tracker emitted it, and the fake iface records the
    message bar.
    """
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    out = {}
    try:
        plugin.initGui()
        pump(0.3)
        before = len(iface.bar.messages)
        plugin._on_capture_status(kind, dict(detail))
        pump(0.1)
        out["bar"] = [(m[1], str(m[2])) for m in iface.bar.messages[before:]]
    except Exception as exc:  # noqa: BLE001 - reported by the assertion
        out["crash"] = f"{type(exc).__name__}: {exc}"
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            out["unload_error"] = f"{type(exc).__name__}: {exc}"
        pump(0.2)
    return out


def _case_p6(ctx) -> dict:
    from recoverland.core.edit_tracker import EditSessionTracker
    work = os.path.join(ctx.data["tmpdir"], "p6")
    stack = _Stack(work)
    path = os.path.join(work, "lot.sqlite")
    _make_spatialite(path)
    layer = _spatialite_layer(path, "p6")
    res = {"connected": stack.connect(layer)}
    had = "_READBACK_CHUNK" in EditSessionTracker.__dict__
    saved = EditSessionTracker.__dict__.get("_READBACK_CHUNK")
    EditSessionTracker._READBACK_CHUNK = 7
    try:
        layer.startEditing()
        for i in range(23):
            _form_insert(layer, f"LOT{i:02d}", 10.0 + i)
        res["commit_ok"] = bool(layer.commitChanges())
        _pump(0.3)
    finally:
        if had:
            EditSessionTracker._READBACK_CHUNK = saved
        else:
            del EditSessionTracker._READBACK_CHUNK
    stack.shutdown()
    res["events"] = _insert_events(stack.path)
    res["provider"] = _provider_rows(_spatialite_layer(path, "p6_probe"))
    return res


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_k_ins_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} "
         f"tmpdir={ctx.data['tmpdir']}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("p1", _case_p1), ("p2", _case_p2), ("p3", _case_p3),
                    ("p3b", _case_p3b), ("p3c", _case_p3c),
                    ("p3d", _case_p3d), ("p3e", _case_p3e),
                    ("p3f", _case_p3f), ("p3g", _case_p3g),
                    ("p4", _case_p4), ("p4b", _case_p4b),
                    ("p5", _case_p5), ("p6", _case_p6)):
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


def _by_code(events, code):
    return next((e for e in events if (e.get("attrs") or {}).get("code") == code), None)


def _provider_by_code(rows, code):
    for fid, attrs in (rows or {}).items():
        if attrs.get("code") == code:
            return fid, attrs
    return None, None


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    p1, p2 = ctx.data.get("p1", {}), ctx.data.get("p2", {})
    p3, p4 = ctx.data.get("p3", {}), ctx.data.get("p4", {})
    p5, p6 = ctx.data.get("p5", {}), ctx.data.get("p6", {})
    p3b, p3c = ctx.data.get("p3b", {}), ctx.data.get("p3c", {})
    p4b = ctx.data.get("p4b", {})

    # ===== P1: GeoPackage with DEFAULT columns =========================
    _fid1, prov_new1 = _provider_by_code(p1.get("provider"), "NEW1")
    out.append((
        "p1_fixture_form_insert_left_values_to_the_file",
        p1.get("connected") is True and p1.get("commit_ok") is True
        and _PLACEHOLDER in (p1.get("buffer_types") or [])
        and prov_new1 is not None and prov_new1.get("statut") == "AUTO"
        and prov_new1.get("cree_le") and prov_new1.get("n") == 7,
        f"connected={p1.get('connected')} commit={p1.get('commit_ok')} "
        f"types saisis={p1.get('buffer_types')} fichier={prov_new1} "
        f"crash={p1.get('crash')} {p1.get('trace', '')}: sans valeur laissee "
        f"a la source le reste du cas ne prouve rien",
    ))
    ev1 = p1.get("events") or []
    out.append((
        "p1_insert_never_journals_a_placeholder",
        len(ev1) == 2 and not _has_placeholder(ev1),
        f"{len(ev1)} INSERT, evenements avec espace reserve={_has_placeholder(ev1)} "
        f"; ex.={[e.get('attrs') for e in ev1]}. Le journal dit que le champ "
        f"valait '<QgsUnsetAttributeValue: AUTO>' alors que le fichier "
        f"contient 'AUTO' : l'historique ment sur ce qui a ete saisi",
    ))
    mism = []
    for code in ("NEW1", "NEW2"):
        fid, prov = _provider_by_code(p1.get("provider"), code)
        evt = _by_code(ev1, code)
        ident = json.loads(evt["identity"]) if evt and evt.get("identity") else {}
        if (evt is None or evt.get("attrs") != prov
                or ident.get("pk_value") != fid):
            mism.append({"code": code, "journal": evt and evt.get("attrs"),
                         "fichier": prov, "identity": ident, "fid": fid})
    out.append((
        "p1_insert_holds_exactly_what_the_provider_stored",
        len(ev1) == 2 and not mism,
        f"ecarts journal/fichier={mism}. Chaque valeur de l'instantane INSERT "
        f"doit etre celle que la source a ecrite (DEFAULT compris)",
    ))
    undo1 = p1.get("undo") or {}
    out.append((
        "p1_undo_of_the_form_insert_is_proven",
        undo1.get("success") is True
        and undo1.get("identity_level") in ("STRONG", "MEDIUM")
        and p1.get("provider_after_undo") == ["NEW2", "SEED"],
        f"annulation={undo1} ; fichier apres={p1.get('provider_after_undo')} "
        f"attendu ['NEW2', 'SEED']. Avec l'espace reserve la preuve tombe a "
        f"WEAK : l'utilisateur ne peut pas annuler sa propre saisie",
    ))

    # ===== P2: Spatialite auto-increment key ===========================
    ev2 = p2.get("events") or []
    out.append((
        "p2_fixture_key_left_to_the_provider",
        p2.get("connected") is True and p2.get("commit_ok") is True
        and _PLACEHOLDER in (p2.get("buffer_types") or []),
        f"connected={p2.get('connected')} commit={p2.get('commit_ok')} "
        f"types={p2.get('buffer_types')} crash={p2.get('crash')} "
        f"{p2.get('trace', '')}",
    ))
    bad_keys = []
    for code in ("NEW1", "NEW2"):
        fid, prov = _provider_by_code(p2.get("provider"), code)
        evt = _by_code(ev2, code)
        ident = json.loads(evt["identity"]) if evt and evt.get("identity") else {}
        if (evt is None or prov is None or ident.get("pk_field") != "pk"
                or ident.get("pk_value") != prov.get("pk")
                or evt.get("attrs") != prov
                or evt.get("entity") != f"pk:pk={prov.get('pk')}"):
            bad_keys.append({"code": code, "identity": ident,
                             "entity": evt and evt.get("entity"),
                             "journal": evt and evt.get("attrs"), "fichier": prov})
    out.append((
        "p2_identity_carries_the_real_key",
        len(ev2) == 2 and not bad_keys and not _has_placeholder(ev2),
        f"ecarts={bad_keys} ; espace reserve dans={_has_placeholder(ev2)}. "
        f"Une cle '<QgsUnsetAttributeValue: Autogenerate>' ne designe aucune "
        f"entite et la meme empreinte est donnee a toutes les entites ajoutees",
    ))
    entities = [e.get("entity") for e in ev2]
    out.append((
        "p2_each_inserted_feature_is_its_own_entity",
        len(entities) == 2 and len(set(entities)) == 2,
        f"empreintes d'entite={entities} : deux saisies distinctes ne doivent "
        f"pas partager un historique",
    ))
    undo2 = p2.get("undo") or {}
    out.append((
        "p2_undo_of_the_autoincrement_insert_is_proven",
        undo2.get("success") is True
        and undo2.get("identity_level") in ("STRONG", "MEDIUM")
        and p2.get("provider_after_undo") == ["NEW2", "SEED"],
        f"annulation={undo2} ; fichier apres={p2.get('provider_after_undo')} "
        f"attendu ['NEW2', 'SEED']",
    ))

    # ===== P3: crash right after the provider wrote ====================
    _fid3, prov_crash = _provider_by_code(p3.get("provider"), "CRASH")
    ev3 = p3.get("events") or []
    crash_evt = _by_code(ev3, "CRASH")
    out.append((
        "p3_fixture_crash_after_the_file_was_written",
        p3.get("fault_reached") is True and prov_crash is not None,
        f"rc={p3.get('rc')} stdout={p3.get('stdout')} fichier={prov_crash} "
        f"stderr={p3.get('stderr')} crash={p3.get('crash')} {p3.get('trace', '')}",
    ))
    out.append((
        "p3_insert_that_landed_is_recognised_after_the_crash",
        crash_evt is not None and crash_evt.get("attrs") == prov_crash
        and not _has_placeholder(ev3),
        f"rapprochement={p3.get('reconciled')} ; INSERT journalises="
        f"{[e.get('attrs') for e in ev3]} ; fichier={prov_crash}. L'etat "
        f"'prepare' portait les espaces reserves : l'entite reellement ecrite "
        f"est declaree non appliquee et n'aura jamais d'evenement",
    ))

    # ===== P3b / P3c: a pasted copy of an existing feature, crash ========
    def _names(evt, fid):
        try:
            ident = json.loads(evt.get("identity") or "{}")
        except ValueError:
            return True
        return fid is not None and (ident.get("fid") == fid or ident.get("pk_value") == fid)

    def _counts(res):
        rec = res.get("reconciled") or []
        tot = {k: sum(int(r.get(k) or 0) for r in rec)
               for k in ("landed", "not_applied", "unresolved")}
        return len(rec), tot

    seed_b = p3b.get("seed_fid")
    out.append((
        "p3b_fixture_crash_before_the_provider_wrote",
        p3b.get("fault_reached") is True and seed_b is not None
        and p3b.get("file_before_restart") == [(seed_b, "SEED")],
        f"rc={p3b.get('rc')} fichier au redemarrage={p3b.get('file_before_restart')} "
        f"attendu [({seed_b}, 'SEED')] stderr={p3b.get('stderr')} "
        f"crash={p3b.get('crash')} {p3b.get('trace', '')}",
    ))
    n_rec_b, tot_b = _counts(p3b)
    wrong_b = [e for e in p3b.get("events") or [] if _names(e, seed_b)]
    out.append((
        "p3b_pasted_copy_not_written_is_never_taken_for_the_original",
        n_rec_b == 1 and tot_b["landed"] == 0
        and tot_b["not_applied"] + tot_b["unresolved"] == 1
        and not wrong_b
        and p3b.get("file_after_undo") == [(seed_b, "SEED")],
        f"rapprochement={p3b.get('reconciled')} ; INSERT designant l'originale "
        f"fid={seed_b} : {[(e['eid'], e.get('identity')) for e in wrong_b]} ; "
        f"annulations={p3b.get('undos')} ; fichier apres annulation="
        f"{p3b.get('file_after_undo')}. La copie n'a jamais ete ecrite : un "
        f"INSERT pour l'entite d'origine fait que l'annuler SUPPRIME la saisie "
        f"d'origine de l'utilisateur",
    ))
    seed_c = p3c.get("seed_fid")
    before_c = p3c.get("file_before_restart") or []
    copy_c = [fid for fid, code in before_c if code == "SEED" and fid != seed_c]
    out.append((
        "p3c_fixture_crash_after_the_provider_wrote_the_copy",
        p3c.get("fault_reached") is True and seed_c is not None
        and (seed_c, "SEED") in before_c and len(copy_c) == 1,
        f"rc={p3c.get('rc')} fichier au redemarrage={before_c} stderr="
        f"{p3c.get('stderr')} crash={p3c.get('crash')} {p3c.get('trace', '')}",
    ))
    n_rec_c, tot_c = _counts(p3c)
    ev3c = p3c.get("events") or []
    wrong_c = [e for e in ev3c if _names(e, seed_c)]
    right_c = [e for e in ev3c if copy_c and _names(e, copy_c[0])]
    out.append((
        "p3c_pasted_copy_written_never_designates_the_original",
        n_rec_c == 1 and tot_c["not_applied"] == 0
        and tot_c["landed"] + tot_c["unresolved"] == 1
        and not wrong_c and len(right_c) == tot_c["landed"]
        and (seed_c, "SEED") in (p3c.get("file_after_undo") or []),
        f"rapprochement={p3c.get('reconciled')} ; INSERT={[(e['eid'], e.get('identity')) for e in ev3c]}"
        f" ; copie fid={copy_c} originale fid={seed_c} ; annulations="
        f"{p3c.get('undos')} ; fichier apres annulation={p3c.get('file_after_undo')}. "
        f"La copie est dans le fichier : elle est journalisee sous SA cle ou "
        f"declaree indeterminee, jamais 'non appliquee', et l'originale survit",
    ))
    p3d, p3e = ctx.data.get("p3d", {}), ctx.data.get("p3e", {})
    seed_d = p3d.get("seed_fid")
    out.append((
        "p3d_fixture_crash_between_attribute_and_addition_stages",
        p3d.get("fault_reached") is True and seed_d is not None
        and p3d.get("file_before_restart") == [(seed_d, "RENOMME")],
        f"rc={p3d.get('rc')} fichier au redemarrage={p3d.get('file_before_restart')} "
        f"attendu [({seed_d}, 'RENOMME')] : l'etape attributs ecrite, pas "
        f"l'ajout. stderr={p3d.get('stderr')} crash={p3d.get('crash')} "
        f"{p3d.get('trace', '')}",
    ))
    n_rec_d, _tot_d = _counts(p3d)
    ev3d = p3d.get("events") or []
    out.append((
        "p3d_copy_of_what_the_commit_made_of_the_original_is_not_the_original",
        n_rec_d == 1 and not ev3d
        and (seed_d, "RENOMME") in (p3d.get("file_after_undo") or []),
        f"rapprochement={p3d.get('reconciled')} ; INSERT={[(e['eid'], e.get('identity')) for e in ev3d]}"
        f" ; annulations={p3d.get('undos')} ; fichier apres annulation="
        f"{p3d.get('file_after_undo')}. L'ajout n'a jamais atteint le fichier ; "
        f"l'originale, renommee par le meme commit, porte maintenant le "
        f"contenu de la copie : un INSERT pour elle la fait supprimer",
    ))
    seed_e = p3e.get("seed_fid")
    n_rec_e, tot_e = _counts(p3e)
    wrong_e = [e for e in p3e.get("events") or [] if _names(e, seed_e)]
    out.append((
        "p3e_table_without_geometry_copy_not_written_is_not_the_original",
        p3e.get("fault_reached") is True and seed_e is not None
        and p3e.get("file_before_restart") == [(seed_e, "SEED")]
        and n_rec_e == 1 and tot_e["landed"] == 0
        and tot_e["not_applied"] + tot_e["unresolved"] == 1 and not wrong_e
        and p3e.get("file_after_undo") == [(seed_e, "SEED")],
        f"rc={p3e.get('rc')} fichier au redemarrage={p3e.get('file_before_restart')} "
        f"rapprochement={p3e.get('reconciled')} ; INSERT designant l'originale="
        f"{[(e['eid'], e.get('identity')) for e in wrong_e]} ; fichier apres "
        f"annulation={p3e.get('file_after_undo')} crash={p3e.get('crash')} "
        f"{p3e.get('trace', '')}. Sans geometrie la seule preuve est le "
        f"contenu attributaire : la ligne dupliquee EST l'originale",
    ))
    for key in ("p3f", "p3g"):
        res = ctx.data.get(key, {})
        seed_x = res.get("seed_fid")
        n_rec_x, tot_x = _counts(res)
        ev_x = res.get("events") or []
        # Hypothesis revised (v7): the copy was expected "unresolved"
        # because the reconciliation could not set its recorded twin (SEED)
        # aside; it now does (crash_reconcile._addition_verdict, twins kept
        # by capture_outbox._preserved_state). The only feature holding the
        # copy's content is that twin, at its own stored key: the copy
        # provably did not land, so the five additions are not_applied.
        # Equivalent or stronger for what the case proves: a copy the
        # search MISSED has no twin to set aside, SEED is then its unique
        # match and becomes a "landed" INSERT, which this check refuses
        # (landed=0, no INSERT, file intact) exactly as before.
        out.append((
            f"{key}_copy_inside_a_paste_is_found_{'in_one_query' if key == 'p3f' else 'after_the_capped_query_gave_up'}",
            res.get("fault_reached") is True and seed_x is not None
            and res.get("file_before_restart") == [(seed_x, "SEED")]
            and n_rec_x == 1 and tot_x == {"landed": 0, "not_applied": 5, "unresolved": 0}
            and not ev_x and res.get("file_after_undo") == [(seed_x, "SEED")],
            f"rc={res.get('rc')} fichier au redemarrage={res.get('file_before_restart')} "
            f"rapprochement={res.get('reconciled')} attendu landed=0 not_applied=5 "
            f"unresolved=0 ; INSERT={[(e['eid'], e.get('identity')) for e in ev_x]} ; "
            f"fichier apres annulation={res.get('file_after_undo')} crash="
            f"{res.get('crash')} {res.get('trace', '')}. La copie noyee dans un "
            f"collage de cinq ne doit pas echapper a la recherche",
        ))

    # ===== P4: filtered layer, the new feature leaves the view ==========
    ev4 = p4.get("events") or []
    cache = _by_code(ev4, "CACHE")
    _fid4, prov4 = _provider_by_code(p4.get("provider"), "CACHE")
    out.append((
        "p4_fixture_feature_written_but_hidden_by_the_filter",
        p4.get("commit_ok") is True and prov4 is not None,
        f"commit={p4.get('commit_ok')} fichier={prov4} crash={p4.get('crash')} "
        f"{p4.get('trace', '')}",
    ))
    out.append((
        "p4_filtered_layer_insert_holds_what_the_file_holds",
        cache is not None and cache.get("attrs") == prov4
        and not _has_placeholder(ev4),
        f"INSERT={cache and cache.get('attrs')} ; fichier={prov4}",
    ))

    # ===== P4b: Spatialite filtered layer, real read-back miss ===========
    ev4b = p4b.get("events") or []
    cache_b = _by_code(ev4b, "CACHE")
    _fid4b, prov4b = _provider_by_code(p4b.get("provider"), "CACHE")
    added4b = p4b.get("added_fids") or []
    out.append((
        "p4b_fixture_spatialite_filter_hides_the_new_feature_from_a_fid_read",
        p4b.get("connected") is True and p4b.get("commit_ok") is True
        and prov4b is not None and len(added4b) == 1
        and p4b.get("filtered_read_by_fid") == []
        and p4b.get("edit_ok") is True,
        f"connected={p4b.get('connected')} commit={p4b.get('commit_ok')} "
        f"fichier={prov4b} fids ajoutes={added4b} lecture par FID sur la couche "
        f"filtree={p4b.get('filtered_read_by_fid')} (attendu []) modif="
        f"{p4b.get('edit_ok')} crash={p4b.get('crash')} {p4b.get('trace', '')}",
    ))
    ident4b = json.loads(cache_b["identity"]) if cache_b and cache_b.get("identity") else {}
    at_insert = dict(prov4b or {}, statut="AUTO")
    out.append((
        "p4b_filtered_spatialite_insert_holds_the_file_values_and_key",
        cache_b is not None and cache_b.get("attrs") == at_insert
        and ident4b.get("pk_field") == "pk"
        and ident4b.get("pk_value") == (prov4b or {}).get("pk")
        and not _has_placeholder(ev4b),
        f"INSERT={cache_b and cache_b.get('attrs')} identite={ident4b} ; fichier "
        f"a l'insertion={at_insert}. La cle et les DEFAULT choisis par la source "
        f"manquaient : l'annulation ne peut pas prouver l'entite par sa cle",
    ))
    upd4b = p4b.get("updates") or []
    out.append((
        "p4b_later_edit_lands_in_the_same_entity_history",
        cache_b is not None and len(upd4b) == 1
        and upd4b[0].get("entity") == cache_b.get("entity")
        and "insert_values_unverified" not in (p4b.get("statuses") or []),
        f"entite de l'INSERT={cache_b and cache_b.get('entity')} ; UPDATE suivant="
        f"{upd4b} ; statuts={p4b.get('statuses')}. Deux entites d'historique "
        f"pour une seule entite : la fiche de l'entite montre la modification "
        f"sans son ajout, et un rewind les traite separement",
    ))

    # ===== P5: provider read-back does not return the feature ===========
    ev5 = p5.get("events") or []
    lost = _by_code(ev5, "PERDU")
    fid5, prov5 = _provider_by_code(p5.get("provider"), "PERDU")
    ident5 = json.loads(lost["identity"]) if lost and lost.get("identity") else {}
    attrs5 = (lost or {}).get("attrs") or {}
    out.append((
        "p5_fixture_feature_written",
        p5.get("commit_ok") is True and prov5 is not None,
        f"commit={p5.get('commit_ok')} fichier={prov5} crash={p5.get('crash')} "
        f"{p5.get('trace', '')}",
    ))
    out.append((
        "p5_unknown_values_are_absent_never_invented",
        lost is not None and not _has_placeholder(ev5)
        and not any(k in attrs5 for k in ("statut", "cree_le", "n"))
        and attrs5.get("code") == "PERDU"
        and ident5.get("pk_value") == fid5,
        f"INSERT={lost} ; fichier={prov5}. Ce que la source a decide n'a pas "
        f"pu etre relu : l'instantane ne doit ni porter l'espace reserve ni "
        f"inventer un NULL, et l'identite doit rester la cle reelle",
    ))
    kinds = [k for k, _d in p5.get("statuses") or []]
    out.append((
        "p5_the_gap_is_announced",
        "insert_values_unverified" in kinds,
        f"statuts de capture={p5.get('statuses')} : un INSERT journalise sans "
        f"les valeurs choisies par la source doit etre signale, pas tu",
    ))
    shown5 = p5.get("shown") or {}
    bar5 = shown5.get("bar") or []
    out.append((
        "p5_the_user_sees_the_gap",
        bool(bar5) and any(_P5_LAYER in text for text, _lvl in bar5)
        and not shown5.get("crash"),
        f"barre de messages apres le statut insert_values_unverified={bar5} "
        f"crash={shown5.get('crash')} unload={shown5.get('unload_error')}. "
        f"Le trou n'atteint que le journal technique : l'utilisateur croit son "
        f"ajout journalise en entier",
    ))

    # ===== P6: a batch read back over several chunks ===================
    ev6 = p6.get("events") or []
    prov6 = {a.get("code"): a for a in (p6.get("provider") or {}).values()
             if str(a.get("code", "")).startswith("LOT")}
    wrong6 = []
    for evt in ev6:
        attrs = evt.get("attrs") or {}
        ident = json.loads(evt["identity"]) if evt.get("identity") else {}
        prov = prov6.get(attrs.get("code"))
        if prov is None or attrs != prov or ident.get("pk_value") != prov.get("pk"):
            wrong6.append({"journal": attrs, "identity": ident, "fichier": prov})
    keys6 = [json.loads(e["identity"]).get("pk_value") for e in ev6
             if e.get("identity")]
    out.append((
        "p6_batch_read_back_across_chunks_keeps_every_key",
        p6.get("commit_ok") is True and len(prov6) == 23 and len(ev6) == 23
        and not wrong6 and len(set(map(str, keys6))) == 23
        and not _has_placeholder(ev6),
        f"commit={p6.get('commit_ok')} fichier={len(prov6)} INSERT={len(ev6)} "
        f"ecarts={wrong6[:3]} cles={keys6} crash={p6.get('crash')} "
        f"{p6.get('trace', '')}",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=5))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
