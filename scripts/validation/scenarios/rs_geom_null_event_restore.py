# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_geom_null_event_restore - RecoverLand validation runtime.

Invariant: I-8 (la restauration rend la geometrie capturee, y compris son
absence) et I-3 (la preuve de cible compare aussi l'absence de geometrie).
Complement du marqueur GEOMETRY_NULL_MARKER sur le chemin evenement par
evenement (restauration et annulation), le rewind etant couvert par
tx_geom_null_transitions.

Le mecanisme
------------
La capture enregistre desormais une geometrie qui APPARAIT ou DISPARAIT
avec un marqueur explicite, un BLOB de longueur nulle (b""), distinct du
NULL SQL qui veut toujours dire "geometrie inchangee". Un UPDATE dont
geometry_wkb vaut b"" decrit une entite qui n'avait PAS de geometrie avant
l'edition; new_geometry_wkb == b"", une entite qui n'en a plus apres.

Le chemin evenement de restore_service ne connaissait pas ce marqueur:
`rebuild_geometry(b"")` rend None, lu comme un WKB corrompu. Restaurer
l'ajout d'une geometrie etait refuse (corrupt_wkb) au lieu de rendre la
geometrie NULLE; la preuve de cible par l'etat NEW ne comparait rien pour
une geometrie supprimee (aucun champ modifie: "no_comparable_field") et
refusait de rendre le point; l'annulation reinjectait, via `new or old`,
l'autre cote de l'evenement quand le marqueur etait falsy.

Ce que l'utilisateur voit
-------------------------
Il numerise la geometrie d'une ligne importee sans geometrie, puis veut
revenir en arriere depuis l'historique: "Geometry unreadable (corrupt
WKB)". Il efface par erreur la geometrie d'un point: la restauration de
cet evenement refuse ("could not be verified"), le point reste perdu alors
que le journal le contient. Une geometrie VIDE (POINT EMPTY, type garde)
est un etat legitime: elle doit revenir vide, pas etre refusee.

Cas
---
    R1  capture reelle sur GeoPackage (cle fid): G1 NULL -> point,
        G2 point -> NULL, G3 VIDE -> point. restore_batch des trois
        evenements: le fichier revient a t0 (NULL, point, VIDE), trois
        succes, trois traces (trois ecritures).
    R2  annulation de ces restaurations: le fichier revient a l'etat
        apres edition.
    M0  shapefile (FID seul), evenement NULL -> point construit avec le
        marqueur, tel que la capture l'ecrit: la restauration rend une
        geometrie NULLE (et non "corrupt_wkb"); son annulation rend le
        point.
    M1  shapefile (FID seul), evenement point -> NULL construit avec le
        marqueur; l'entite est la seule sans geometrie: preuve suffisante,
        le point revient.
    M2  meme evenement, mais deux entites sont sans geometrie et l'edition
        n'a touche aucun attribut: l'absence de geometrie ne designe
        personne. Refus nomme, fichier intact.

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rs_geom_null_event_restore"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SHP_FIELDS = ["nom:string(20)", "code:string(10)"]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _gpkg(tmpdir):
    """G1 sans geometrie, G2 point (2 2), G3 POINT EMPTY; cle = fid."""
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry,
                           QgsPointXY, QgsVectorFileWriter,
                           QgsCoordinateTransformContext)
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(20)",
        "m", "memory")
    mem.startEditing()
    for code, nom in (("G1", "SANS_GEOM"), ("G2", "AVEC_GEOM"),
                      ("G3", "GEOM_VIDE")):
        feat = QgsFeature(mem.fields())
        feat.setAttributes([code, nom])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.0, 2.0)))
        mem.addFeature(feat)
    mem.commitChanges()
    path = os.path.join(tmpdir, "g.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    # The writer stores a missing geometry as POINT EMPTY: the NULL and
    # EMPTY states are set through the provider instead.
    lyr = QgsVectorLayer(f"{path}|layername=pts", "g", "ogr")
    by_code = {f["code"]: f.id() for f in lyr.getFeatures()}
    lyr.dataProvider().changeGeometryValues({
        by_code["G1"]: QgsGeometry(),
        by_code["G3"]: QgsGeometry.fromWkt("Point EMPTY"),
    })
    lyr.reload()
    return path, lyr


def _state(path, layername="pts"):
    """{code: WKT ou None} relu par une couche neuve (le fichier)."""
    from qgis.core import QgsVectorLayer
    uri = f"{path}|layername={layername}" if path.endswith(".gpkg") else path
    lyr = QgsVectorLayer(uri, "probe", "ogr")
    out = {}
    for feat in lyr.getFeatures():
        geom = feat.geometry()
        out[feat["code"]] = (None if geom is None or geom.isNull()
                             else geom.asWkt(3))
    del lyr
    return out


def _fid_of(layer, code):
    for feat in layer.getFeatures():
        if feat["code"] == code:
            return feat.id()
    return None


def _capture(ctx, tmp):
    """Real capture of the three transitions; returns the UPDATE events."""
    from qgis.core import QgsProject, QgsGeometry, QgsPointXY
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff,
    )
    from scripts.validation.plugin_harness import pump

    d = ctx.data
    path, lyr = _gpkg(tmp)
    d["gpkg"] = path
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
    try:
        d["t0"] = _state(path)
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        lyr.startEditing()
        lyr.changeGeometry(_fid_of(lyr, "G1"),
                           QgsGeometry.fromPointXY(QgsPointXY(7.0, 7.0)))
        lyr.changeGeometry(_fid_of(lyr, "G2"), QgsGeometry())
        lyr.changeGeometry(_fid_of(lyr, "G3"),
                           QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)))
        d["commit"] = bool(lyr.commitChanges())
        pump(0.3)
        if hasattr(wq, "wait_until_drained"):
            wq.wait_until_drained(5.0)
        else:
            time.sleep(1.0)
        d["after_edit"] = _state(path)
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
    return lyr, [e for e in events if e.operation_type == "UPDATE"
                 and e.restored_from_event_id is None]


# Hypothesis revised (v7): the two hand-built marker events below used the
# fixture's default event_schema_version (5). "Built with the marker, as
# the capture writes it" is only true of a schema 7 event: the capture that
# writes the zero-length BLOB stamps 7, and in an older event that BLOB is
# corruption (core.constants.is_null_geometry_marker; the restore now
# refuses it, see rs_null_marker_versioned). The events are stamped 7, the
# assertions are unchanged: this is the same case, stated truthfully.
_MARKER_SCHEMA = 7


def _marker_event(eid, fid):
    """UPDATE point (4 4) -> no geometry, FID-only, no field changed."""
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE", crs_authid="EPSG:4326",
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}",
        attributes_json=json.dumps({"changed_only": {}}),
        geometry_wkb=fx.wkb_wkt("Point (4 4)"), new_geometry_wkb=b"",
        field_schema_json=fx.schema(("nom", "string"), ("code", "string")),
        event_schema_version=_MARKER_SCHEMA)


def _added_geometry_event(eid, fid):
    """UPDATE no geometry -> point (7 7), FID-only, no field changed."""
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE", crs_authid="EPSG:4326",
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}",
        attributes_json=json.dumps({"changed_only": {}}),
        geometry_wkb=b"", new_geometry_wkb=fx.wkb_wkt("Point (7 7)"),
        field_schema_json=fx.schema(("nom", "string"), ("code", "string")),
        event_schema_version=_MARKER_SCHEMA)


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    fx = _fx()
    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_rs_gner_")
    ctx.data["tmpdir"] = tmp
    # M0: B received its geometry (7 7) after an import without one.
    ctx.data["shp_m0"] = fx.make_shapefile(tmp, "m0", "Point", _SHP_FIELDS, [
        (["A", "K1"], "Point (1 1)"), (["B", "K2"], "Point (7 7)")])
    # M1: B is the only feature without a geometry (it lost it).
    ctx.data["shp_m1"] = fx.make_shapefile(tmp, "m1", "Point", _SHP_FIELDS, [
        (["A", "K1"], "Point (1 1)"), (["B", "K2"], None),
        (["C", "K3"], "Point (3 3)")])
    # M2: B and D both lack one; nothing tells them apart.
    ctx.data["shp_m2"] = fx.make_shapefile(tmp, "m2", "Point", _SHP_FIELDS, [
        (["A", "K1"], "Point (1 1)"), (["B", "K2"], None),
        (["D", "K4"], None)])
    ctx.data["before_m2"] = _state(ctx.data["shp_m2"])
    flog(f"rs_geom_null_event_restore setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_batch, undo_restore_batch, restore_updated_feature,
        undo_prepare, undo_apply_one,
    )
    fx = _fx()
    d = ctx.data
    tid = ctx.trace_id
    flog(f"rs_geom_null_event_restore run start: trace_id={tid}", "INFO")
    try:
        lyr, events = _capture(ctx, d["tmpdir"])
        d["n_events"] = len(events)
        # What the capture wrote on each side: the NULL marker (b"") or a WKB.
        d["markers"] = sorted(
            ("old=marker" if e.geometry_wkb == b"" else "old=wkb",
             "new=marker" if e.new_geometry_wkb == b"" else "new=wkb")
            for e in events)
        report = restore_batch(lyr, events, trace_id=tid)
        d["r1"] = {"succeeded": len(report.succeeded),
                   "failed": {int(k): str(v)[:200]
                              for k, v in report.failed.items()},
                   "traces": len(report.trace_events or ())}
        d["after_restore"] = _state(d["gpkg"])
        undo = undo_restore_batch(lyr, events)
        d["r2"] = {"succeeded": len(undo.succeeded),
                   "failed": {int(k): str(v)[:200]
                              for k, v in undo.failed.items()}}
        d["after_undo"] = _state(d["gpkg"])
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    finally:
        QgsProject.instance().clear()

    layer = fx.open_layer(d["shp_m0"])
    ev_m0 = _added_geometry_event(500, _code_fid(layer, "B"))
    d["m0"] = dict(restore_updated_feature(layer, ev_m0))
    del layer
    d["after_m0"] = _state(d["shp_m0"])
    layer = fx.open_layer(d["shp_m0"])
    d["m0_undo"] = dict(undo_apply_one(layer, ev_m0,
                                       undo_prepare(layer, [ev_m0])))
    del layer
    d["after_m0_undo"] = _state(d["shp_m0"])

    layer = fx.open_layer(d["shp_m1"])
    d["m1"] = dict(restore_updated_feature(
        layer, _marker_event(501, _code_fid(layer, "B"))))
    del layer
    d["after_m1"] = _state(d["shp_m1"])
    layer = fx.open_layer(d["shp_m2"])
    d["m2"] = dict(restore_updated_feature(
        layer, _marker_event(502, _code_fid(layer, "B"))))
    del layer
    d["after_m2"] = _state(d["shp_m2"])

    flog(f"rs_geom_null_event_restore run end: trace_id={tid} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:2500]}",
         "INFO")
    shutil.rmtree(d.get("tmpdir", ""), ignore_errors=True)


def _code_fid(layer, nom):
    for feat in layer.getFeatures():
        if feat["nom"] == nom:
            return feat.id()
    return None


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    t0 = d.get("t0") or {}
    edited = d.get("after_edit") or {}
    restored = d.get("after_restore") or {}
    undone = d.get("after_undo") or {}
    r1 = d.get("r1") or {}
    r2 = d.get("r2") or {}

    out.append((
        "fixture_transitions_captured",
        d.get("commit") is True and d.get("n_events") == 3
        and t0 == {"G1": None, "G2": "Point (2 2)", "G3": "Point EMPTY"}
        and edited == {"G1": "Point (7 7)", "G2": None, "G3": "Point (9 9)"},
        f"t0={t0} apres edition={edited} evenements={d.get('n_events')} "
        f"marqueurs={d.get('markers')} crash={d.get('crash')} "
        f"{d.get('trace', '')[-500:]}",
    ))
    out.append((
        "r1_added_geometry_restored_to_null",
        restored.get("G1") is None and "G1" in restored,
        f"G1 t0=NULL apres restauration={restored.get('G1')} "
        f"rapport={r1}: la geometrie numerisee apres coup doit disparaitre; "
        f"le marqueur 'pas de geometrie' n'est pas un WKB corrompu.",
    ))
    out.append((
        "r1_removed_geometry_restored",
        restored.get("G2") == "Point (2 2)",
        f"G2 t0=Point (2 2) apres restauration={restored.get('G2')} "
        f"rapport={r1}: le point efface est dans le journal, la "
        f"restauration doit le rendre.",
    ))
    out.append((
        "r1_empty_geometry_restored_empty",
        restored.get("G3") == "Point EMPTY",
        f"G3 t0=Point EMPTY apres restauration={restored.get('G3')} "
        f"rapport={r1}: une geometrie vide capturee est un etat, pas une "
        f"corruption.",
    ))
    out.append((
        "r1_reported_and_traced_as_three_writes",
        r1.get("succeeded") == 3 and not r1.get("failed")
        and r1.get("traces") == 3,
        f"rapport={r1}: trois ecritures verifiees, trois traces attendues",
    ))
    out.append((
        "r2_undo_gives_back_the_edited_state",
        undone == edited and edited != {} and r2.get("succeeded") == 3
        and not r2.get("failed"),
        f"annulation={r2} etat={undone} attendu={edited}: l'annulation doit "
        f"remettre le point numerise, l'absence de geometrie et le point "
        f"(9 9), sans reinjecter l'autre cote de l'evenement.",
    ))

    m0 = d.get("m0") or {}
    after_m0 = d.get("after_m0") or {}
    out.append((
        "m0_null_marker_restored_as_null_geometry",
        m0.get("success") is True and "K2" in after_m0
        and after_m0.get("K2") is None
        and after_m0.get("K1") == "Point (1 1)",
        f"resultat={m0} apres={after_m0}: l'evenement dit que B n'avait "
        f"pas de geometrie avant l'edition (marqueur b''); la restauration "
        f"doit la retirer, pas refuser un 'WKB corrompu' ni laisser le "
        f"point numerise.",
    ))
    m0_undo = d.get("m0_undo") or {}
    out.append((
        "m0_undo_gives_the_point_back",
        m0_undo.get("success") is True
        and (d.get("after_m0_undo") or {}).get("K2") == "Point (7 7)",
        f"annulation={m0_undo} apres={d.get('after_m0_undo')}: B porte "
        f"l'etat ecrit par la restauration (aucune geometrie) et elle "
        f"seule; l'annulation doit lui rendre le point (7 7).",
    ))

    m1 = d.get("m1") or {}
    after_m1 = d.get("after_m1") or {}
    out.append((
        "m1_fid_only_unique_absence_restores_the_point",
        m1.get("success") is True and after_m1.get("K2") == "Point (4 4)",
        f"resultat={m1} apres={after_m1}: B est la seule entite sans "
        f"geometrie, comme l'etat NEW de l'evenement: la preuve suffit, "
        f"le point efface doit revenir.",
    ))
    m2 = d.get("m2") or {}
    out.append((
        "m2_shared_absence_is_a_named_refusal",
        m2.get("success") is False
        and m2.get("reason_code") in ("target_unverifiable",
                                      "target_ambiguous")
        and d.get("after_m2") == d.get("before_m2"),
        f"resultat={m2} apres={d.get('after_m2')} avant={d.get('before_m2')}"
        f": deux entites sans geometrie, aucun champ modifie: rien ne "
        f"designe B plutot que D. Ecrire le point sur l'une d'elles au "
        f"hasard serait pire que refuser.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_geom_null_event_restore.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
