"""rw_guard_buffer_keys_ids_only - RecoverLand validation runtime.

Invariant: I-REWIND-01 sous charge : ce que CommitGuard sait de chaque
action d'un rewind (les entites qu'elle a changees, ajoutees, supprimees)
reste exact, quel que soit le tampon d'edition de la couche, et ne coute
pas une copie de toutes les valeurs du tampon a chaque action.

Le mecanisme
------------
`core.commit_guard.CommitGuard.begin_action` et `end_action` lisent les
cles du tampon d'edition avant et apres CHAQUE action d'un rewind
(`_buffer_keys`) : la difference dit quelles entites l'action a touchees,
ce qui sert ensuite a attribuer a chaque evenement son verdict apres le
commit (`_verify_commit`, `_attribute_refusal`), et a reperer ce que
l'utilisateur a saisi pendant le rewind (`foreign_changes`). En v7, cette
lecture copiait `changedAttributeValues()`, `changedGeometries()` et
`addedFeatures()` : chaque valeur, chaque geometrie, chaque entite ajoutee
du tampon converties en Python, deux fois par action, pour n'en garder
que les cles. Le cout d'une action croit avec la taille du tampon, celui
du rewind avec son carre : 4 500 compensations sur 300 entites passaient
13 a 16 s dans cette seule lecture (sur 38 a 47 s d'execution mesures,
poste charge, QGIS 4.2.1).

Un premier correctif lisait `allAddedOrEditedFeatures()` (les ids seuls)
et classait chaque id par son SIGNE : negatif = ajoute, positif = change.
C'est vrai du tampon ordinaire (QgsVectorLayerEditBuffer), qui donne a une
entite ajoutee un id temporaire negatif. C'est faux du tampon d'une couche
en groupe de transactions (Projet > Proprietes > Mode de transaction :
groupes de transactions automatiques, GeoPackage, SpatiaLite,
PostgreSQL) : QgsVectorLayerEditPassthrough insere tout de suite et garde
l'entite ajoutee sous le fid POSITIF que le fournisseur lui a donne.
L'insertion restauree passait alors pour une modification, disparaissait
de `_event_added`, et `_verify_commit` ne cherchait plus si le fournisseur
tenait les valeurs demandees.

Echec concret pour l'utilisateur :
  - en groupe de transactions, une suppression annulee (insertion
    restauree) dont le fournisseur stocke une autre valeur (declencheur,
    valeur par defaut, regle de la base) est annoncee RESTAUREE ET
    VERIFIEE : fausse reussite, la donnee ne correspond pas a ce que
    l'historique disait ;
  - l'attribution d'un refus (`_attribute_refusal`) lit la meme tenue de
    compte : une insertion absente de `_event_added` qui a atterri serait
    annoncee "non appliquee" au lieu de "ecrite, non annulee", sans trace
    journalisee (en groupe de transactions, ce chemin ne recoit
    aujourd'hui aucun ajout atterri, committedFeaturesAdded n'etant pas
    emis : l'exposition reelle est la fausse reussite ci-dessus) ;
  - un rewind de masse fige la barre de progression des dizaines de
    secondes de plus que necessaire (cout de la copie des valeurs).

Ce que le scenario fait
-----------------------
A. Tampon ordinaire : un GeoPackage de 200 entites en edition, un vrai
   CommitGuard, et 150 actions variees, chacune entre begin_action et
   end_action : attribut et geometrie d'entites existantes, ajout,
   attribut et geometrie d'une entite ajoutee, suppression, annulation
   (undo) au milieu d'une action.
     - Reference calculee par le scenario sur les tables COMPLETES du
       tampon, avant et apres chaque action : les ensembles par evenement
       du garde (`_event_fids`, `_event_added`, `_event_deleted`),
       `_buffer_keys` et `foreign_changes` doivent lui etre identiques a
       chaque pas (ce que le garde prouve ne change pas).
     - Un espion pose sur la classe du tampon compte, pendant les appels
       du garde seulement, les lectures qui copient les valeurs : il n'en
       faut aucune.
B. Groupe de transactions : les memes 150 actions sur un GeoPackage ajoute
   au projet apres setTransactionMode(AutomaticGroups) ; le tampon doit
   etre un QgsVectorLayerEditPassthrough, et les memes egalites pas a pas
   et le meme espion s'appliquent.
C. De bout en bout, dans les deux modes : un GeoPackage dont un
   declencheur AFTER INSERT ajoute 1000 a la valeur inseree, et la vraie
   restauration (`restore_executor.execute_restore_plan`) d'un evenement
   DELETE, donc une INSERTION de valeur 5. Le fichier tient 1005 : le
   verdict du garde pour l'evenement doit etre
   FAILED_POSTCOMMIT_VERIFICATION, et le rapport ne doit pas le compter
   comme restaure. Puis l'attribution d'un refus (`_attribute_refusal`)
   est interrogee sur l'etat reel du garde, avec pour ajout atterri le fid
   que le fichier donne a l'entite inseree : elle doit etre rattachee a
   son evenement (LANDED_NOT_COMPENSATED, pas FAILED_COMMIT). Le refus est
   simule (un commit de GeoPackage refuse APRES l'insertion ne se provoque
   pas de facon fiable) ; ce qui est verifie est la tenue de compte du
   garde que cette attribution consomme.
   Temoin : la meme restauration sans declencheur. En tampon ordinaire
   elle doit etre RESTAUREE ET VERIFIEE (le classement des ids ne cree
   pas de faux echec). En groupe de transactions son verdict est seulement
   rapporte, pas exige : QgsVectorLayerEditPassthrough n'emet pas
   committedFeaturesAdded, et `_verify_commit` (hors du perimetre de ce
   scenario) n'apparie une insertion demandee qu'aux fids de ce signal ;
   il la declare donc non verifiee meme quand le fichier tient la bonne
   valeur (faux echec, jamais fausse reussite ; comportement v7 d'avant
   le classement par signe, que ce dernier masquait en ne verifiant
   plus rien).

Verdict attendu avant tout correctif (v7, tables completes) : FAIL (trois
lectures de valeurs par lecture des cles, dans les deux modes). Avec le
classement par signe : FAIL (groupe de transactions : ajouts classes
comme modifications, insertion contredite annoncee restauree et
verifiee, insertion atterrie non attribuee).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_guard_buffer_keys_ids_only"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_N = 200
_ACTIONS = 150
_VALUE_READS = ("changedAttributeValues", "changedGeometries", "addedFeatures")

# Cas C : valeur demandee par la restauration, ajout du declencheur.
_E2E_EVENT = 7
_E2E_VALUE = 5
_E2E_BUMP = 1000


def _set_transaction_mode(grouped: bool) -> None:
    from qgis.core import Qgis, QgsProject
    QgsProject.instance().setTransactionMode(
        Qgis.TransactionMode.AutomaticGroups if grouped
        else Qgis.TransactionMode.Disabled)


def _make_layer(tmp, name):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(12)"
                         "&field=val:integer&field=note:string(40)", "m", "memory")
    mem.startEditing()
    for i in range(_N):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"K{i:04d}", i, "texte " * 5])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i, i)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmp, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return QgsVectorLayer(f"{path}|layername=pts", name, "ogr")


def _reference_keys(layer):
    """Les cles, lues sur les tables completes du tampon (la definition)."""
    eb = layer.editBuffer()
    changed = (set(eb.changedAttributeValues().keys())
               | set(eb.changedGeometries().keys()))
    return changed, set(eb.addedFeatures().keys()), set(eb.deletedFeatureIds())


def _act(layer, step, fids, idx_val, idx_note):
    """Une action de rewind : une ou deux editions du tampon."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    fid = fids[step % len(fids)]
    kind = step % 6
    if kind == 0:
        layer.changeAttributeValue(fid, idx_val, 100000 + step)
    elif kind == 1:
        layer.changeGeometry(fid, QgsGeometry.fromPointXY(QgsPointXY(step, -step)))
    elif kind == 2:
        f = QgsFeature(layer.fields())
        f.setAttributes([None, f"N{step}", step, "ajout"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(-step, step)))
        layer.addFeature(f)
    elif kind == 3:
        added = sorted(layer.editBuffer().addedFeatures().keys())
        if added:
            layer.changeAttributeValue(added[0], idx_note, f"modifie {step}")
            layer.changeGeometry(added[0], QgsGeometry.fromPointXY(QgsPointXY(1, step)))
        else:
            layer.changeAttributeValue(fid, idx_note, f"modifie {step}")
    elif kind == 4:
        layer.deleteFeature(fids[(step * 7) % len(fids)])
    else:
        layer.changeAttributeValue(fid, idx_val, -step)
        layer.undoStack().undo()
        layer.changeAttributeValue(fids[(step + 3) % len(fids)], idx_note, "apres undo")


def _exercise(layer, trace_id):
    """Les 150 actions sous un vrai CommitGuard, comparees pas a pas a la
    reference des tables completes. La couche doit etre en edition."""
    from recoverland.core.commit_guard import CommitGuard
    out = {"valid": layer.isValid(), "buffer": type(layer.editBuffer()).__name__}
    fids = sorted(f.id() for f in layer.getFeatures())
    idx_val = layer.fields().indexFromName("val")
    idx_note = layer.fields().indexFromName("note")
    guard = CommitGuard(layer, trace_id=trace_id)

    # Espion sur les CLASSES du tampon (celle de l'instance et ses bases
    # qui definissent la methode) : compte les lectures qui copient les
    # valeurs, seulement pendant que le garde lit le tampon.
    state = {"in_guard": False, "reads": {n: 0 for n in _VALUE_READS}}
    patched = []

    def _spy(name, orig):
        def spy(self, *a, **k):
            if state["in_guard"]:
                state["reads"][name] += 1
            return orig(self, *a, **k)
        return spy

    for cls in type(layer.editBuffer()).__mro__:
        for name in _VALUE_READS:
            if name in vars(cls):
                # The raw descriptor is put back; the spy calls the
                # callable the class hands out for it.
                patched.append((cls, name, vars(cls)[name]))
                setattr(cls, name, _spy(name, getattr(cls, name)))

    def guarded(fn, *a):
        state["in_guard"] = True
        try:
            return fn(*a)
        finally:
            state["in_guard"] = False

    mismatches = []
    keys_calls, keys_s = 0, 0.0
    ref = (set(), set(), set())
    try:
        for step in range(_ACTIONS):
            eid = 1000 + step
            before = _reference_keys(layer)
            guarded(guard.begin_action, eid)
            _act(layer, step, fids, idx_val, idx_note)
            guarded(guard.end_action, eid)
            after = _reference_keys(layer)
            want = {
                "fids": {f for f in after[0] - before[0] if f >= 0},
                "added": after[1] - before[1],
                "deleted": after[2] - before[2],
            }
            got = {
                "fids": guard._event_fids.get(eid, set()),
                "added": guard._event_added.get(eid, set()),
                "deleted": guard._event_deleted.get(eid, set()),
            }
            t = time.perf_counter()
            keys = guarded(guard._buffer_keys)
            keys_s += time.perf_counter() - t
            keys_calls += 1
            if got != want or keys != after:
                mismatches.append({"step": step, "want": {k: sorted(v) for k, v in want.items()},
                                   "got": {k: sorted(v) for k, v in got.items()},
                                   "keys_equal": keys == after})
        # A user edit between two actions: foreign_changes must name it.
        layer.changeAttributeValue(fids[-1], idx_note, "saisie utilisateur")
        foreign = guarded(guard.foreign_changes)
        ref = _reference_keys(layer)
        known = set().union(*guard._event_fids.values(), *guard._event_deleted.values())
        known_added = set().union(*guard._event_added.values())
        want_foreign = sorted((ref[0] | ref[2]) - known) + sorted(ref[1] - known_added)
        out["foreign"] = {"got": foreign, "want": want_foreign}
        # Reference cost, same buffer, same process (information only).
        t = time.perf_counter()
        for _ in range(keys_calls):
            _reference_keys(layer)
        ref_s = time.perf_counter() - t
    finally:
        for cls, name, orig in patched:
            setattr(cls, name, orig)
        layer.rollBack()
    out["mismatches"] = mismatches
    out["reads"] = dict(state["reads"])
    out["sizes"] = [len(x) for x in ref]
    out["cost_us"] = {"guard": round(keys_s / max(1, keys_calls) * 1e6, 1),
                      "full_maps": round(ref_s / max(1, keys_calls) * 1e6, 1)}
    return out


# ---------------------------------------------------------------- cas C
def _make_trigger_gpkg(path, trigger=True):
    """GPKG de 3 points ; avec `trigger`, un declencheur AFTER INSERT
    ajoute _E2E_BUMP a la valeur inseree (comme le ferait un declencheur
    PostgreSQL)."""
    from osgeo import ogr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    lyr = ds.CreateLayer("pts", geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("v", ogr.OFTInteger))
    for i in range(3):
        f = ogr.Feature(lyr.GetLayerDefn())
        f.SetField("code", f"K{i}")
        f.SetField("v", i)
        f.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(f)
    ds = None
    if not trigger:
        return
    con = sqlite3.connect(path)
    con.execute(f"CREATE TRIGGER rl_gbk_bump AFTER INSERT ON pts "
                f"BEGIN UPDATE pts SET v = v + {_E2E_BUMP} WHERE fid = NEW.fid; END;")
    con.commit()
    con.close()


def _delete_event():
    """Un DELETE journalise : sa restauration est l'INSERTION de v=5."""
    from qgis.core import QgsGeometry, QgsPointXY
    from recoverland.core.audit_backend import AuditEvent
    wkb = bytes(QgsGeometry.fromPointXY(QgsPointXY(9, 9)).asWkb())
    return AuditEvent(
        event_id=_E2E_EVENT, project_fingerprint="rl_gbk_project",
        datasource_fingerprint="rl_gbk_ds", layer_id_snapshot="lyr_gbk",
        layer_name_snapshot="pts", provider_type="ogr",
        feature_identity_json=json.dumps({"fid": 77, "pk_field": "fid", "pk_value": 77}),
        operation_type="DELETE",
        attributes_json=json.dumps({"all_attributes": {"code": "K77", "v": _E2E_VALUE}}),
        geometry_wkb=wkb, geometry_type="Point", crs_authid="EPSG:4326",
        field_schema_json=json.dumps([
            {"name": "code", "type": "String", "length": 0, "precision": 0},
            {"name": "v", "type": "Integer", "length": 0, "precision": 0}]),
        user_name="tester", session_id=None,
        created_at=datetime(2026, 8, 12, 11, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None, entity_fingerprint="fid:77",
        event_schema_version=2, new_geometry_wkb=None, invalidated_at=None)


def _restore_insert(tmp, grouped, trace_id, trigger=True):
    """La vraie restauration d'un DELETE (INSERT), avec ou sans le
    declencheur qui contredit la valeur inseree."""
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core import commit_guard as cg
    from recoverland.core.restore_contracts import (
        CutoffType, RestoreCutoff, RestoreScope)
    from recoverland.core.restore_executor import execute_restore_plan
    from recoverland.core.restore_planner import plan_temporal_restore
    name = ("grp_ins" if grouped else "buf_ins") + ("_trg" if trigger else "_ok")
    path = os.path.join(tmp, f"{name}.gpkg")
    _make_trigger_gpkg(path, trigger)
    proj = QgsProject.instance()
    proj.clear()
    _set_transaction_mode(grouped)
    layer = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    proj.addMapLayer(layer)
    out = {"valid": layer.isValid()}
    # Le garde reel de la restauration, et son verdict, sont captures au
    # passage (la classe est rendue intacte ensuite).
    seen = {}
    orig_finish = cg.CommitGuard.finish

    def finish_spy(self, ok):
        verdict = orig_finish(self, ok)
        seen["guard"], seen["verdict"] = self, verdict
        seen["buffer"] = type(self.layer.editBuffer()).__name__ if self.layer.editBuffer() else None
        return verdict

    ev = _delete_event()
    cutoff = RestoreCutoff(
        cutoff_type=CutoffType.BY_DATE,
        value=datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc).isoformat())
    cg.CommitGuard.finish = finish_spy
    try:
        plan = plan_temporal_restore([ev], "rl_gbk_ds", "pts", cutoff,
                                     scope=RestoreScope.LAYER)
        out["ops"] = [a.compensatory_op for a in plan.actions]
        report = execute_restore_plan(plan, {_E2E_EVENT: ev}, layer, trace_id=trace_id)
    finally:
        cg.CommitGuard.finish = orig_finish
    out["succeeded"] = list(report.succeeded)
    out["failed"] = {int(k): str(v)[:200] for k, v in report.failed.items()}
    verdict, guard = seen.get("verdict"), seen.get("guard")
    out["per_event"] = dict(verdict.per_event) if verdict else None
    out["state"] = verdict.state if verdict else None
    out["event_added"] = sorted(guard._event_added.get(_E2E_EVENT, ())) if guard else None
    out["event_fids"] = sorted(guard._event_fids.get(_E2E_EVENT, ())) if guard else None
    out["signal_added"] = list(verdict.added_fids) if verdict else None
    check = QgsVectorLayer(f"{path}|layername=pts", "relu", "ogr")
    feats = [(str(f["code"]), f["v"], int(f.id())) for f in check.getFeatures()]
    del check
    out["stored"] = sorted((code, v) for code, v, _fid in feats)
    # Attribution d'un refus, sur l'etat reel du garde : l'insertion qui a
    # atterri, designee par le fid que le FOURNISSEUR lui a donne (lu dans
    # le fichier, independamment du garde), doit etre rattachee a son
    # evenement. En groupe de transactions, committedFeaturesAdded n'est
    # pas emis (signal_added vide, mesure) : le fichier est la seule
    # source de ce fid.
    landed = [fid for code, _v, fid in feats if code == "K77"]
    out["landed_added"] = landed
    if guard is not None:
        refused = cg.CommitVerdict()
        refused.commit_errors = ["refus simule apres atterrissage de l'ajout"]
        guard._attribute_refusal(refused, set(), set(landed), compensated=False)
        out["refusal_status"] = refused.per_event.get(_E2E_EVENT)
    proj.removeMapLayer(layer.id())
    proj.clear()
    _set_transaction_mode(False)
    return out


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    _set_transaction_mode(False)
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_gbk_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    d = ctx.data
    proj = QgsProject.instance()

    # A. Tampon ordinaire (hors groupe de transactions).
    layer = _make_layer(d["tmp"], "buffered")
    layer.startEditing()
    res = _exercise(layer, ctx.trace_id)
    del layer
    d.update({k: res[k] for k in ("valid", "mismatches", "reads", "sizes",
                                  "cost_us", "foreign")})
    d["buffer"] = res["buffer"]
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} mode=buffered "
         f"buffer={res['buffer']} actions={_ACTIONS} "
         f"mismatches={len(res['mismatches'])} value_reads={res['reads']} "
         f"buffer_sizes={res['sizes']} cost_us={res['cost_us']}", "INFO")

    # B. Groupe de transactions automatique : tampon passthrough.
    proj.clear()
    _set_transaction_mode(True)
    try:
        layer = _make_layer(d["tmp"], "grouped")
        proj.addMapLayer(layer)
        layer.startEditing()
        res = _exercise(layer, ctx.trace_id)
        proj.removeMapLayer(layer.id())
    finally:
        proj.clear()
        _set_transaction_mode(False)
    d["group"] = res
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} mode=transaction_group "
         f"buffer={res['buffer']} actions={_ACTIONS} "
         f"mismatches={len(res['mismatches'])} first={res['mismatches'][:2]} "
         f"value_reads={res['reads']} buffer_sizes={res['sizes']} "
         f"cost_us={res['cost_us']}", "INFO")

    # C. Restauration reelle d'une insertion que le fournisseur contredit,
    # et le temoin : la meme insertion, que le fournisseur garde telle
    # quelle.
    d["e2e"], d["control"] = {}, {}
    for grouped in (False, True):
        mode = "transaction_group" if grouped else "buffered"
        for trigger, bucket in ((True, d["e2e"]), (False, d["control"])):
            try:
                bucket[mode] = _restore_insert(d["tmp"], grouped, ctx.trace_id,
                                               trigger=trigger)
            finally:
                proj.clear()
                _set_transaction_mode(False)
            flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} e2e mode={mode} "
                 f"trigger={trigger} result={bucket[mode]}", "INFO")


def teardown(ctx):
    try:
        from qgis.core import QgsProject
        QgsProject.instance().clear()
        _set_transaction_mode(False)
    except Exception:  # noqa: BLE001
        pass
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def _e2e_assertion(d, mode):
    from recoverland.core.commit_guard import EV_FAILED_POSTCOMMIT_VERIFICATION
    r = (d.get("e2e") or {}).get(mode) or {}
    c = (d.get("control") or {}).get(mode) or {}
    stored_v = [v for code, v in (r.get("stored") or []) if code == "K77"]
    fixture_ok = (r.get("valid") is True and r.get("ops") == ["INSERT"]
                  and stored_v == [_E2E_VALUE + _E2E_BUMP])
    status = (r.get("per_event") or {}).get(_E2E_EVENT)
    failed = str((r.get("failed") or {}).get(_E2E_EVENT, ""))
    # The verdict must come from the insert check: the insert is in the
    # guard's added bookkeeping of its event.
    ok = (fixture_ok and status == EV_FAILED_POSTCOMMIT_VERIFICATION
          and bool(r.get("event_added"))
          and _E2E_EVENT not in (r.get("succeeded") or [])
          and failed.startswith(EV_FAILED_POSTCOMMIT_VERIFICATION))
    return (
        f"{'group' if mode == 'transaction_group' else 'buffered'}"
        f"_restored_insert_contradicted_by_provider_is_not_verified",
        ok,
        f"mode={mode} ops={r.get('ops')} valeur stockee={stored_v} (demandee "
        f"{_E2E_VALUE}, declencheur +{_E2E_BUMP}) verdict du garde={status} "
        f"etat={r.get('state')} _event_added={r.get('event_added')} "
        f"_event_fids={r.get('event_fids')} restaures={r.get('succeeded')} "
        f"echecs={r.get('failed')} (temoin sans declencheur : verdict="
        f"{(c.get('per_event') or {}).get(_E2E_EVENT)} valeur stockee="
        f"{[v for code, v in (c.get('stored') or []) if code == 'K77']}) : "
        f"le fichier ne tient pas ce que l'historique disait ; annoncer "
        f"cette insertion restauree et verifiee est une fausse reussite",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    from recoverland.core.commit_guard import (
        EV_APPLIED_VERIFIED, EV_LANDED_NOT_COMPENSATED)
    d = ctx.data
    out = []
    sizes = d.get("sizes") or [0, 0, 0]
    out.append((
        "fixture_buffer_holds_every_kind_of_edit",
        d.get("valid") is True and all(n > 0 for n in sizes),
        f"couche valide={d.get('valid')} tampon final (changees, ajoutees, "
        f"supprimees)={sizes} : les trois tables doivent etre peuplees",
    ))
    mm = d.get("mismatches")
    out.append((
        "per_action_sets_equal_the_full_map_reference",
        mm == [],
        f"actions ou le garde differe de la reference={len(mm or [])} "
        f"premiers={(mm or [])[:3]} : un ensemble faux attribue a un "
        f"evenement le verdict d'un autre apres le commit du rewind",
    ))
    fo = d.get("foreign") or {}
    out.append((
        "foreign_changes_equal_the_full_map_reference",
        bool(fo) and fo.get("got") == fo.get("want") and bool(fo.get("want")),
        f"saisie de l'utilisateur pendant le rewind : garde={fo.get('got')} "
        f"reference={fo.get('want')} : une saisie non reperee serait "
        f"enregistree avec le rewind, hors journal",
    ))
    reads = d.get("reads") or {}
    out.append((
        "action_bookkeeping_copies_no_buffer_value",
        bool(reads) and not any(reads.values()),
        f"lectures copiant les valeurs du tampon pendant begin_action/"
        f"end_action/foreign_changes={reads} (cout par lecture des cles : "
        f"garde={(d.get('cost_us') or {}).get('guard')} us, tables completes="
        f"{(d.get('cost_us') or {}).get('full_maps')} us, tampon={sizes}) : "
        f"chaque copie convertit toutes les valeurs, geometries et entites "
        f"ajoutees du tampon, deux fois par action ; un rewind de masse y "
        f"passait le tiers de son temps",
    ))

    # B. Groupe de transactions.
    g = d.get("group") or {}
    gsizes = g.get("sizes") or [0, 0, 0]
    out.append((
        "group_fixture_is_a_passthrough_buffer_with_every_kind_of_edit",
        g.get("valid") is True and g.get("buffer") == "QgsVectorLayerEditPassthrough"
        and all(n > 0 for n in gsizes),
        f"couche valide={g.get('valid')} tampon={g.get('buffer')} tables "
        f"finales (changees, ajoutees, supprimees)={gsizes} : le cas du "
        f"groupe de transactions n'est pas exerce",
    ))
    gmm = g.get("mismatches")
    out.append((
        "group_per_action_sets_equal_the_full_map_reference",
        gmm == [],
        f"groupe de transactions : actions ou le garde differe de la "
        f"reference={len(gmm or [])}/{_ACTIONS} premiers={(gmm or [])[:3]} : "
        f"une entite ajoutee (fid reel positif) prise pour une entite "
        f"modifiee sort de _event_added, et son insertion n'est plus "
        f"verifiee apres le commit",
    ))
    gfo = g.get("foreign") or {}
    out.append((
        "group_foreign_changes_equal_the_full_map_reference",
        bool(gfo) and gfo.get("got") == gfo.get("want") and bool(gfo.get("want")),
        f"groupe de transactions, saisie de l'utilisateur pendant le rewind : "
        f"garde={gfo.get('got')} reference={gfo.get('want')} : une saisie "
        f"non reperee serait enregistree avec le rewind, hors journal",
    ))
    greads = g.get("reads") or {}
    out.append((
        "group_bookkeeping_copies_no_buffer_value",
        bool(greads) and not any(greads.values()),
        f"groupe de transactions : lectures copiant les valeurs={greads} "
        f"(cout garde={(g.get('cost_us') or {}).get('guard')} us, tables "
        f"completes={(g.get('cost_us') or {}).get('full_maps')} us)",
    ))

    # C. De bout en bout.
    out.append(_e2e_assertion(d, "buffered"))
    out.append(_e2e_assertion(d, "transaction_group"))
    c = (d.get("control") or {}).get("buffered") or {}
    c_stored = [v for code, v in (c.get("stored") or []) if code == "K77"]
    out.append((
        "buffered_restored_insert_kept_by_provider_is_verified",
        c.get("ops") == ["INSERT"] and c_stored == [_E2E_VALUE]
        and (c.get("per_event") or {}).get(_E2E_EVENT) == EV_APPLIED_VERIFIED
        and _E2E_EVENT in (c.get("succeeded") or []),
        f"temoin, tampon ordinaire, sans declencheur : valeur stockee="
        f"{c_stored} verdict du garde={(c.get('per_event') or {}).get(_E2E_EVENT)} "
        f"restaures={c.get('succeeded')} echecs={c.get('failed')} : une "
        f"insertion juste annoncee en echec ferait refaire a l'utilisateur "
        f"une restauration deja faite",
    ))
    for mode, label in (("buffered", "buffered"), ("transaction_group", "group")):
        r = (d.get("e2e") or {}).get(mode) or {}
        out.append((
            f"{label}_landed_insert_is_attributed_to_its_event_on_refusal",
            bool(r.get("landed_added"))
            and r.get("refusal_status") == EV_LANDED_NOT_COMPENSATED,
            f"mode={mode} ajout atterri (fid du fichier)={r.get('landed_added')} "
            f"(committedFeaturesAdded={r.get('signal_added')}) statut "
            f"attribue a l'evenement {_E2E_EVENT}={r.get('refusal_status')} "
            f"_event_added={r.get('event_added')} : un commit refuse apres "
            f"l'atterrissage de l'insertion l'annoncerait 'non appliquee' "
            f"et ne journaliserait aucune trace de ce qui reste ecrit",
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
