# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_insert_undo_evidence - RecoverLand validation runtime.

Invariant: I-3 (on ne supprime que l'entite prouvee) et fail-loud (aucun
succes annonce pour une suppression qui n'a pas eu lieu, aucun refus sur
l'entite qu'il fallait supprimer).
Constats resolus (revue du tour precedent):
  - BLOCKER `_resolve_insert_target`, branche attr_rescue: la relocalisation
    par les seuls attributs ignorait la geometrie que l'instantane porte;
  - MAJOR `_classify_insert_candidate` / `_comparable_fields`: une entite
    saisie par le formulaire QGIS sur une couche a colonnes DEFAULT ne
    pouvait plus etre annulee;
  - MINOR `_buffer_delete`: le raccourci "deja en attente de suppression"
    masquait une cible AMBIGUE derriere un succes.

Le mecanisme
------------
1. Annuler un INSERT, c'est supprimer l'entite inseree. Quand l'instantane
   complet (geometrie + attributs) ne retrouve rien, une relocalisation par
   les attributs seuls rattrape une geometrie re-encodee (derive de
   precision OGR). Elle ne regardait plus du tout la geometrie: une borne
   sans rapport, a 90 unites de la, qui partage "borne", "LYON" et un
   numero vide, etait acceptee (MEDIUM) et supprimee.
2. Pour une entite creee par le formulaire, les captures anterieures
   journalisaient les attributs du signal committedFeaturesAdded, ou les
   colonnes laissees a la base portent un `QgsUnsetAttributeValue`: le
   journal contient "<QgsUnsetAttributeValue: AUTO>" au lieu de la valeur
   que la base a ecrite ('AUTO', l'horodatage CURRENT_TIMESTAMP). La
   capture v7 relit desormais le fournisseur, mais ces evenements sont
   deja dans les journaux. La preuve stricte v7 comparait ces colonnes que
   le fournisseur remplit lui-meme: "differs=[cree, ts]", WEAK, refus. Le
   rewind comptait ce refus avec les absents: le rapport lisait "absent"
   alors que l'entite etait toujours la.
3. Si le FID designe etait deja supprime dans le tampon, la compensation
   repondait "Already pending delete", succes, meme quand le resolveur
   venait de dire AMBIGUOUS: deux candidats, aucun supprime, un succes.

Ce que l'utilisateur voit
-------------------------
1. Il annule l'ajout d'une borne disparue depuis; le plugin supprime une
   autre borne de Lyon et annonce "Deleted (undo insert)".
2. Il ajoute un point par le formulaire dans une GeoPackage qui horodate
   ses lignes, puis revient en arriere: le point reste, le rapport dit
   "cible absente".
3. Deux points identiques, le rewind dit "deja supprime" et n'en retire
   aucun.

Cas
---
    E1  rewind: INSERT {borne, LYON, NULL} a (99 99), disparu; une borne
        LYON sans numero a (10 10): refus, couche intacte
    E2  meme cas par la restauration de l'INSERT
    E3  temoin: l'entite inseree est a (99.0000001 98.9999999) (derive
        d'encodage), meme contenu: supprimee
    A1  entite creee par le formulaire sur une GeoPackage a colonnes
        DEFAULT 'AUTO' et CURRENT_TIMESTAMP, INSERT journalise comme le
        faisaient les captures anterieures (attributs du signal, schema
        6); annulation par restore_inserted_feature: supprimee
    A2  meme evenement, compensation DELETE du rewind: supprimee
    A3  antithese: la meme entite dont le nom a change depuis n'est pas
        supprimee (les colonnes par defaut sont ecartees, pas la preuve)
    A4  temoin: capture v7 REELLE (relecture du fournisseur) de la meme
        saisie; annulation: supprimee
    P1  FID designe deja en attente de suppression, instantane porte par
        deux entites identiques: refus target_ambiguous, pas un succes

Verdict avant correctif (code du tour precedent): FAIL (E1, E2, P1, et
A1/A2 refuses "differs=[cree, ts]"). Sur le code d'origine: FAIL (E1, E2,
A3, P1); A1/A2 y passent, le refus des colonnes DEFAULT est une
regression du tour precedent. Apres: PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rs_insert_undo_evidence"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_BORNE_FIELDS = ["type:string(20)", "commune:string(20)", "numero:string(20)"]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _borne_insert(eid, fid):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="INSERT",
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}",
        field_schema_json=fx.schema(("type", "string"),
                                    ("commune", "string"),
                                    ("numero", "string")),
        attributes_json=json.dumps({"all_attributes": {
            "type": "borne", "commune": "LYON", "numero": None}}),
        geometry_wkb=fx.wkb_wkt("Point (99 99)"), event_schema_version=7)


def _defaults_gpkg(tmpdir, filename="horodate"):
    """GeoPackage dont deux colonnes sont remplies par la base."""
    from osgeo import ogr, osr
    path = os.path.join(tmpdir, f"{filename}.gpkg")
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    lyr = ds.CreateLayer("horodate", srs=srs, geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    cree = ogr.FieldDefn("cree", ogr.OFTString)
    cree.SetDefault("'AUTO'")
    lyr.CreateField(cree)
    stamp = ogr.FieldDefn("ts", ogr.OFTDateTime)
    stamp.SetDefault("CURRENT_TIMESTAMP")
    lyr.CreateField(stamp)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetFID(1)
    feat.SetField("nom", "EXISTANT")
    feat.SetField("cree", "x")
    feat.SetGeometry(ogr.CreateGeometryFromWkt("POINT (1 1)"))
    lyr.CreateFeature(feat)
    feat = None
    ds = None
    return path


def _capture_form_insert(ctx, tmp, path):
    """Capture reelle: l'utilisateur ajoute un point par le formulaire."""
    from qgis.core import (QgsProject, QgsVectorLayer, QgsGeometry,
                           QgsPointXY, QgsVectorLayerUtils)
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff,
    )
    from scripts.validation.plugin_harness import pump

    lyr = QgsVectorLayer(f"{path}|layername=horodate", "horodate", "ogr")
    QgsProject.instance().addMapLayer(lyr)
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()
    tracker.connect_layer(lyr)
    fp = compute_datasource_fingerprint(lyr)
    events = []
    try:
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        lyr.startEditing()
        idx = lyr.fields().indexOf("nom")
        feat = QgsVectorLayerUtils.createFeature(
            lyr, QgsGeometry.fromPointXY(QgsPointXY(4.0, 4.0)),
            {idx: "NOUVEAU"})
        ctx.data["form_attrs"] = [repr(v) for v in feat.attributes()]
        lyr.addFeature(feat)
        ctx.data["a4_commit"] = bool(lyr.commitChanges())
        pump(0.3)
        if hasattr(wq, "wait_until_drained"):
            wq.wait_until_drained(5.0)
        else:
            time.sleep(1.0)
        conn = jm.create_read_connection()
        try:
            events = fetch_events_after_cutoff(
                conn, fp, RestoreCutoff(CutoffType.BY_DATE, t0),
                trace_id=ctx.trace_id, include_traces=False)
        finally:
            conn.close()
    finally:
        tracker.deactivate()
        tracker.disconnect_all()
        wq.stop()
        jm.close()
        QgsProject.instance().removeMapLayer(lyr.id())
    inserts = [e for e in events if e.operation_type == "INSERT"]
    return inserts[0] if inserts else None


def _signal_insert_event(path):
    """INSERT par le formulaire, journalise comme les captures anterieures.

    Les attributs sont ceux du signal committedFeaturesAdded, serialises
    tels quels, avec l'identite que la capture calcule: exactement ce
    qu'un journal 6.x contient pour cette saisie.
    """
    from qgis.core import QgsGeometry, QgsPointXY, QgsVectorLayerUtils
    from recoverland.core.identity import compute_feature_identity
    from recoverland.core.serialization import (
        serialize_attributes, build_full_snapshot, serialize_field_schema,
    )
    fx = _fx()
    layer = fx.open_layer(path, "horodate")
    names = [f.name() for f in layer.dataProvider().fields()]
    captured = []

    def _on_added(_layer_id, feats):
        for feat in feats:
            captured.append((serialize_attributes(feat, names),
                             compute_feature_identity(layer, feat),
                             bytes(feat.geometry().asWkb())))

    layer.committedFeaturesAdded.connect(_on_added)
    layer.startEditing()
    idx = layer.fields().indexOf("nom")
    feat = QgsVectorLayerUtils.createFeature(
        layer, QgsGeometry.fromPointXY(QgsPointXY(4.0, 4.0)), {idx: "NOUVEAU"})
    layer.addFeature(feat)
    committed = bool(layer.commitChanges())
    if not captured:
        return None, committed
    attrs, identity, wkb = captured[0]
    event = fx.audit_event(
        event_id=703, operation_type="INSERT", feature_identity_json=identity,
        entity_fingerprint="pk:fid=legacy",
        field_schema_json=serialize_field_schema(layer.fields()),
        attributes_json=build_full_snapshot(attrs), geometry_wkb=wkb,
        event_schema_version=6)
    return event, committed


def _gpkg_rows(path):
    return _fx().file_rows(path, "horodate", with_fid=True)


def setup(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tmp = tempfile.mkdtemp(prefix="rl_rs_iue_")
    ctx.data["tmpdir"] = tmp
    unrelated = [(["borne", "LYON", None], "Point (10 10)"),
                 (["borne", "PARIS", "12"], "Point (20 20)"),
                 (["borne", "LYON", "7"], "Point (30 30)")]
    for key in ("e1", "e2"):
        ctx.data[f"shp_{key}"] = fx.make_shapefile(
            tmp, f"bornes_{key}", "Point", _BORNE_FIELDS, unrelated)
    ctx.data["shp_e3"] = fx.make_shapefile(
        tmp, "bornes_e3", "Point", _BORNE_FIELDS, [
            (["borne", "LYON", None], "Point (99.0000001 98.9999999)"),
            (["borne", "PARIS", "12"], "Point (20 20)")])
    ctx.data["shp_p1"] = fx.make_shapefile(
        tmp, "doublons_p1", "Point", ["nom:string(20)", "code:string(20)"], [
            (["Z", "z"], "Point (0 0)"), (["D", "d"], "Point (5 5)"),
            (["D", "d"], "Point (5 5)")])
    ctx.data["before"] = {
        key: fx.file_rows(ctx.data[f"shp_{key}"])
        for key in ("e1", "e2", "e3", "p1")}
    flog(f"rs_insert_undo_evidence setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import restore_inserted_feature
    from recoverland.core.restore_executor import _apply_via_buffer
    fx = _fx()
    d = ctx.data
    tid = ctx.trace_id
    res = {}
    flog(f"rs_insert_undo_evidence run start: trace_id={tid}", "INFO")

    def _safe(fn, *args):
        try:
            return dict(fn(*args))
        except Exception as exc:  # noqa: BLE001 - un crash est un resultat
            return {"success": None, "crash": repr(exc)}

    ev_e = _borne_insert(701, 7)
    res["e1"] = fx.buffer_apply(fx.open_layer(d["shp_e1"]), "DELETE", ev_e, tid)
    res["e2"] = _safe(restore_inserted_feature, fx.open_layer(d["shp_e2"]),
                      ev_e)
    res["e3"] = fx.buffer_apply(fx.open_layer(d["shp_e3"]), "DELETE", ev_e, tid)

    # A: the database fills two columns itself.
    try:
        path_a1 = _defaults_gpkg(d["tmpdir"])
        ev_a, d["form_commit"] = _signal_insert_event(path_a1)
        d["a_event"] = (None if ev_a is None else
                        (ev_a.attributes_json, ev_a.feature_identity_json))
        d["a_before"] = _gpkg_rows(path_a1)
        path_a2 = os.path.join(d["tmpdir"], "horodate_rewind.gpkg")
        path_a3 = os.path.join(d["tmpdir"], "horodate_renamed.gpkg")
        shutil.copyfile(path_a1, path_a2)
        shutil.copyfile(path_a1, path_a3)
        if ev_a is not None:
            res["a1"] = _safe(restore_inserted_feature,
                              fx.open_layer(path_a1, "horodate"), ev_a)
            res["a2"] = fx.buffer_apply(fx.open_layer(path_a2, "horodate"),
                                        "DELETE", ev_a, tid)
            renamed = fx.open_layer(path_a3, "horodate")
            fid = json.loads(ev_a.feature_identity_json).get("fid")
            renamed.dataProvider().changeAttributeValues(
                {fid: {renamed.fields().indexOf("nom"): "RENOMME"}})
            del renamed
            d["a3_before"] = _gpkg_rows(path_a3)
            res["a3"] = _safe(restore_inserted_feature,
                              fx.open_layer(path_a3, "horodate"), ev_a)
        # A4: the same input through the real v7 capture.
        path_a4 = _defaults_gpkg(d["tmpdir"], "horodate_v7")
        ev_a4 = _capture_form_insert(ctx, d["tmpdir"], path_a4)
        d["a4_event"] = (None if ev_a4 is None else
                         (ev_a4.attributes_json, ev_a4.feature_identity_json))
        if ev_a4 is not None:
            res["a4"] = _safe(restore_inserted_feature,
                              fx.open_layer(path_a4, "horodate"), ev_a4)
        d["a_after"] = {"a1": _gpkg_rows(path_a1), "a2": _gpkg_rows(path_a2),
                        "a3": _gpkg_rows(path_a3), "a4": _gpkg_rows(path_a4)}
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        import traceback
        d["a_crash"] = f"{type(exc).__name__}: {exc}"
        d["a_trace"] = traceback.format_exc()[-1200:]
    finally:
        QgsProject.instance().clear()

    # P1: fid 0 already deleted in the buffer, two identical candidates.
    layer = fx.open_layer(d["shp_p1"])
    layer.startEditing()
    layer.deleteFeature(0)
    ev_p = fx.audit_event(
        event_id=702, operation_type="INSERT",
        feature_identity_json=json.dumps({"fid": 0}),
        entity_fingerprint="fid:0",
        field_schema_json=fx.schema(("nom", "string"), ("code", "string")),
        attributes_json=json.dumps({"all_attributes": {"nom": "D",
                                                       "code": "d"}}),
        geometry_wkb=fx.wkb_wkt("Point (5 5)"), event_schema_version=7)
    try:
        res["p1"] = dict(_apply_via_buffer(layer, "DELETE", ev_p, {},
                                           trace_id=tid))
    except Exception as exc:  # noqa: BLE001
        res["p1"] = {"success": None, "crash": repr(exc)}
    d["p1_pending"] = sorted(layer.editBuffer().deletedFeatureIds())
    layer.rollBack()

    d["res"] = res
    d["after"] = {key: fx.file_rows(d[f"shp_{key}"])
                  for key in ("e1", "e2", "e3", "p1")}
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"), value.get("identity_level"))
               for key, value in res.items()}
    flog(f"rs_insert_undo_evidence run end: trace_id={tid} "
         f"results={summary} a_crash={d.get('a_crash')}", "INFO")
    shutil.rmtree(d.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    fx = _fx()

    d = ctx.data
    out = []
    res = d.get("res") or {}
    before = d.get("before") or {}
    after = d.get("after") or {}

    for key, label in (("e1", "rewind"), ("e2", "restauration de l'INSERT")):
        out.append((
            f"{key}_unrelated_borne_spared",
            after.get(key) == before.get(key),
            f"{label}: bornes apres={after.get(key)} avant={before.get(key)}. "
            f"La borne inseree a (99 99) a disparu; la borne LYON sans numero "
            f"a (10 10) ne partage que deux valeurs banales et un vide: la "
            f"supprimer detruit la donnee d'un tiers.",
        ))
        out.append((
            f"{key}_not_reported_applied",
            fx.not_applied(res.get(key))
            and (res.get(key) or {}).get("reason_code")
            not in (None, "applied"),
            f"{label}: resultat={res.get(key)}. La relocalisation par les "
            f"attributs seuls a trouve une entite loin de la geometrie "
            f"capturee: refus nomme attendu, pas 'Deleted'.",
        ))
    e3 = res.get("e3") or {}
    out.append((
        "e3_control_encoding_drift_still_rescued",
        e3.get("success") is True and not e3.get("skipped")
        and not any("LYON" in repr(r) for r in after.get("e3") or []),
        f"temoin: resultat={e3} apres={after.get('e3')}. L'entite inseree "
        f"est la, a 1e-7 pres de sa geometrie capturee (derive d'encodage): "
        f"elle doit etre supprimee, sinon le rewind ne rend plus l'etat.",
    ))

    out.append((
        "a_capture_ran",
        d.get("a_crash") is None and d.get("form_commit") is True
        and "QgsUnsetAttributeValue" in str(d.get("a_event")),
        f"saisie par formulaire journalisee comme avant v7: crash="
        f"{d.get('a_crash')} trace={d.get('a_trace')} evenement="
        f"{d.get('a_event')}: l'instantane doit porter les marqueurs "
        f"QgsUnsetAttributeValue, sinon le cas ne teste rien",
    ))
    a_after = d.get("a_after") or {}
    for key, label in (("a1", "annulation de l'INSERT"),
                       ("a2", "compensation DELETE du rewind")):
        result = res.get(key) or {}
        out.append((
            f"{key}_form_insert_undone",
            result.get("success") is True and not result.get("skipped")
            and not any("NOUVEAU" in repr(r) for r in a_after.get(key) or []),
            f"{label}: resultat={result} fichier apres={a_after.get(key)} "
            f"evenement={d.get('a_event')}. Le point ajoute par le formulaire "
            f"est la, intact; les colonnes que la base remplit elle-meme "
            f"(DEFAULT) ne sont pas une preuve contre lui. Refuser laisse le "
            f"point et le rapport le compte 'absent'.",
        ))
        out.append((
            f"{key}_existing_row_kept",
            any("EXISTANT" in repr(r) for r in a_after.get(key) or []),
            f"{label}: fichier apres={a_after.get(key)}",
        ))
    a4 = res.get("a4") or {}
    out.append((
        "a4_control_v7_capture_undone",
        d.get("a4_commit") is True and a4.get("success") is True
        and not a4.get("skipped")
        and not any("NOUVEAU" in repr(r) for r in a_after.get("a4") or []),
        f"temoin capture v7: resultat={a4} evenement={d.get('a4_event')} "
        f"fichier apres={a_after.get('a4')}",
    ))
    a3 = res.get("a3") or {}
    out.append((
        "a3_edited_feature_not_deleted",
        a3.get("success") is not True
        and a_after.get("a3") == d.get("a3_before"),
        f"antithese: le point a ete renomme depuis l'ajout: resultat={a3} "
        f"apres={a_after.get('a3')} avant={d.get('a3_before')}. Ecarter les "
        f"colonnes DEFAULT ne doit pas ecarter la preuve.",
    ))

    p1 = res.get("p1") or {}
    out.append((
        "p1_ambiguity_not_hidden_behind_pending_delete",
        p1.get("success") is False
        and p1.get("reason_code") == "target_ambiguous",
        f"resultat={p1} suppressions en attente={d.get('p1_pending')}. Le FID "
        f"designe est deja supprime dans le tampon, mais l'instantane est "
        f"porte par deux entites identiques: aucune n'est supprimee, le "
        f"resultat doit le dire (target_ambiguous), pas 'Already pending "
        f"delete'.",
    ))
    out.append((
        "p1_layer_untouched",
        after.get("p1") == before.get("p1"),
        f"apres={after.get('p1')} avant={before.get('p1')}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_insert_undo_evidence.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
