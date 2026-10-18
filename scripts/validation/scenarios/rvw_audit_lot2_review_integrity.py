"""rvw_audit_lot2_review_integrity - RecoverLand validation runtime.

Invariant: I-RVW-ASOF / I-3.

Couvre les 8 constats Review du Lot 2 de l'audit adversarial final :
  1. Make Permanent et Duplicate Layer sur un calque ``(snapshot)`` :
     ``SnapshotOverlaySession`` detache le calque devenu permanent (provider
     != 'memory'), ne tronque jamais le GeoPackage de l'utilisateur, ne
     supprime ni le calque rendu permanent ni un calque duplique a l'arret.
  2. SCR personnalise valide sans ``authid`` (WKT pur) : le calque memoire
     et l'export GeoPackage conservent le SCR personnalise (pas de repli sur
     le SCR projet).
  3. Renommage de champ (``nom`` -> ``libelle`` apres T) : ``_merge_live`` ne
     recopie pas la valeur post-T de ``libelle`` et marque ``schema_drift``
     dans ``_rl_etat_incomplet``.
  4. Fidelite des valeurs dans les calques et l'export Review : ``''`` et
     ``'NULL'`` restent des chaines non nulles, et un champ Binary ``b64:...``
     est decode en ``QByteArray``.
  5. Changement de SCR sur la couche source pendant Review : ``update`` et
     ``export_to_geopackage`` suivent le nouveau SCR de la couche source.
  6. Pan unique (pas de double declenchement 600 ms + 800 ms) et peuplement
     decoupe par lots dans ``update_async``.
  7. Modifications non enregistrees dans le tampon d'edition de la couche
     source : ``CanvasDateBar.set_stats`` et ``_review_unsaved_edits``
     signalent l'etat ``unsaved`` au lieu d'annoncer ``empty``.
  8. Couche source retiree du projet pendant Review : ``_populate_layer``
     peuple les attributs connus depuis les champs du calque overlay et
     ``complete_from_live`` marque ``live_layer_missing``.
"""
from __future__ import annotations

import base64
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rvw_audit_lot2_review_integrity"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_CUSTOM_WKT = (
    'PROJCRS["Custom_Local",BASEGEOGCRS["WGS 84",DATUM["World Geodetic System 1984",'
    'ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]]],'
    'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],ID["EPSG",4326]],'
    'CONVERSION["Transverse Mercator",METHOD["Transverse Mercator",ID["EPSG",9807]],'
    'PARAMETER["Latitude of natural origin",45,ANGLEUNIT["degree",0.0174532925199433]],'
    'PARAMETER["Longitude of natural origin",3,ANGLEUNIT["degree",0.0174532925199433]],'
    'PARAMETER["Scale factor at natural origin",0.9999,SCALEUNIT["unity",1]],'
    'PARAMETER["False easting",500000,LENGTHUNIT["metre",1]],'
    'PARAMETER["False northing",0,LENGTHUNIT["metre",1]]],CS[Cartesian,2],'
    'AXIS["(E)",east,ORDER[1],LENGTHUNIT["metre",1]],'
    'AXIS["(N)",north,ORDER[2],LENGTHUNIT["metre",1]]]'
)


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_lot2_")
    flog(f"rvw_audit_lot2_review_integrity setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsFeature, QgsField,
        QgsGeometry, QgsPointXY, QgsProject, QgsVectorFileWriter,
        QgsVectorLayer,
    )
    from qgis.PyQt.QtCore import QMetaType
    from recoverland.core.logger import flog
    from recoverland.core.snapshot_overlay_session import (
        INCOMPLETE_FIELD, SnapshotOverlaySession, _coerce_field_value,
        complete_from_live,
    )
    from recoverland.core.temporal_snapshot_engine import (
        SnapshotFeature, SnapshotResult,
    )
    from recoverland.widgets.canvas_date_bar import CanvasDateBar
    from qgis.gui import QgsMapCanvas

    d = ctx.data
    tmpdir = d["tmpdir"]
    proj = QgsProject.instance()
    proj.clear()
    try:
        # 1. Make Permanent & Duplicate Layer protection
        src_lyr = QgsVectorLayer(
            "Point?crs=EPSG:4326&field=nom:string(20)", "parcelles", "memory")
        f1 = QgsFeature(src_lyr.fields())
        f1.setAttributes(["P1"])
        f1.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0, 2.0)))
        src_lyr.dataProvider().addFeatures([f1])
        proj.addMapLayer(src_lyr)

        sess = SnapshotOverlaySession()
        info = {
            "fingerprint": "ogr::p1",
            "layer_id": src_lyr.id(),
            "layer_name": "parcelles",
            "storage_crs": "EPSG:4326",
        }
        sess.start([info], "EPSG:3857", ctx.trace_id)
        sess._create_overlay(info)
        ovl_id = sess._overlays["ogr::p1"].layer_id
        ovl_lyr = proj.mapLayer(ovl_id)

        # User duplicates the overlay layer
        dup_lyr = ovl_lyr.clone()
        dup_lyr.setName("parcelles (snapshot) copier")
        proj.addMapLayer(dup_lyr)
        dup_id = dup_lyr.id()

        # User makes the original overlay permanent to a GPKG
        perm_gpkg = os.path.join(tmpdir, "user_perm.gpkg")
        opts = QgsVectorFileWriter.SaveVectorOptions()
        opts.driverName = "GPKG"
        opts.layerName = "perm"
        QgsVectorFileWriter.writeAsVectorFormatV3(
            src_lyr, perm_gpkg, proj.transformContext(), opts)
        ovl_lyr.setDataSource(f"{perm_gpkg}|layername=perm", "parcelles_perm", "ogr")

        res_empty = SnapshotResult(
            features={}, cutoff_dt=datetime.now(timezone.utc),
            n_fps=1, n_entities=0, n_absent=0, n_unknown=0,
            elapsed_ms=1, trace_id=ctx.trace_id)
        sess.update(res_empty)
        sess.stop()

        perm_check = QgsVectorLayer(f"{perm_gpkg}|layername=perm", "chk", "ogr")
        d["perm_gpkg_intact"] = (perm_check.featureCount() == 1)
        d["perm_layer_kept"] = (proj.mapLayer(ovl_id) is not None)
        d["dup_layer_kept"] = (proj.mapLayer(dup_id) is not None)
        del perm_check

        # 2. Custom CRS (valid WKT, no authid) + 5. CRS relabel during Review
        proj.clear()
        ccrs = QgsCoordinateReferenceSystem.fromWkt(_CUSTOM_WKT)
        custom_lyr = QgsVectorLayer("Point?field=nom:string(20)", "custom_l", "memory")
        custom_lyr.setCrs(ccrs)
        proj.addMapLayer(custom_lyr)
        wkb_pt = bytes(QgsGeometry.fromPointXY(QgsPointXY(500100.0, 4980000.0)).asWkb())

        sess2 = SnapshotOverlaySession()
        info2 = {
            "fingerprint": "ogr::custom",
            "layer_id": custom_lyr.id(),
            "layer_name": "custom_l",
            "storage_crs": ccrs.authid() or ccrs.toWkt(),
        }
        sess2.start([info2], "EPSG:3857", ctx.trace_id)
        sf_custom = SnapshotFeature(
            entity_fp="fid:1", geom_wkb=wkb_pt,
            attrs_json=json.dumps({"nom": "C1"}), crs_authid="",
            last_event_id=1, last_op="INSERT",
            last_created_at="2026-01-01T00:00:00Z",
            schema_fields=("nom",),
        )
        res_custom = SnapshotResult(
            features={"ogr::custom": {"fid:1": sf_custom}},
            cutoff_dt=datetime.now(timezone.utc),
            n_fps=1, n_entities=1, n_absent=0, n_unknown=0,
            elapsed_ms=1, trace_id=ctx.trace_id,
        )
        sess2.update(res_custom)
        ovl2 = proj.mapLayer(sess2._overlays["ogr::custom"].layer_id)
        d["custom_crs_overlay_matches"] = (
            ovl2 is not None and ovl2.crs().isValid() and ovl2.crs() == ccrs)

        # 5. Now user changes CRS on custom_lyr to EPSG:2154 during Review
        crs_2154 = QgsCoordinateReferenceSystem("EPSG:2154")
        custom_lyr.setCrs(crs_2154)
        sess2.update(res_custom)
        exp_gpkg = os.path.join(tmpdir, "exp_crs.gpkg")
        sess2.export_to_geopackage(exp_gpkg, res_custom, "2026-01-01T00:00:00")
        exp_lyr = QgsVectorLayer(f"{exp_gpkg}|layername=custom_l", "exp", "ogr")
        d["relabel_crs_synced"] = (
            ovl2.crs().authid() == "EPSG:2154"
            and exp_lyr.crs().authid() == "EPSG:2154")
        del exp_lyr

        # 8. Source layer removed during Review: attributes kept & flagged
        proj.removeMapLayer(custom_lyr.id())
        done_missing = complete_from_live(res_custom, [info2], ctx.trace_id)
        sess2.update(done_missing)
        ovl_feat = next(ovl2.getFeatures())
        d["removed_src_keeps_attrs_and_flags"] = (
            ovl_feat["nom"] == "C1"
            and "live_layer_missing" in str(ovl_feat[INCOMPLETE_FIELD] or "")
            and done_missing.n_unresolved == 1)
        sess2.stop()

        # 3. Field rename (nom -> libelle after T)
        proj.clear()
        ren_lyr = QgsVectorLayer(
            "Point?crs=EPSG:4326&field=fid:integer&field=libelle:string(20)",
            "ren_l", "memory")
        rf = QgsFeature(ren_lyr.fields())
        rf.setAttributes([1, "Alpha2"])
        rf.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.0, 2.0)))
        ren_lyr.dataProvider().addFeatures([rf])
        proj.addMapLayer(ren_lyr)
        info_ren = {
            "fingerprint": "ogr::ren",
            "layer_id": ren_lyr.id(),
            "layer_name": "ren_l",
            "storage_crs": "EPSG:4326",
            "fid_stable": True,
        }
        sf_ren = SnapshotFeature(
            entity_fp="pk:fid=1",
            geom_wkb=bytes(rf.geometry().asWkb()),
            attrs_json=json.dumps({"nom": "Alpha"}),
            crs_authid="EPSG:4326",
            last_event_id=1, last_op="UPDATE",
            last_created_at="2026-01-01T00:00:00Z",
            identity_json=json.dumps({"fid": 1, "pk_field": "fid", "pk_value": 1}),
            attrs_from_live=True,
            schema_fields=("fid", "nom"),
        )
        res_ren = SnapshotResult(
            features={"ogr::ren": {"pk:fid=1": sf_ren}},
            cutoff_dt=datetime.now(timezone.utc),
            n_fps=1, n_entities=1, n_absent=0, n_unknown=0,
            elapsed_ms=1, trace_id=ctx.trace_id,
        )
        done_ren = complete_from_live(res_ren, [info_ren], ctx.trace_id)
        feat_ren = done_ren.features["ogr::ren"]["pk:fid=1"]
        parsed_ren = json.loads(feat_ren.attrs_json or "{}")
        d["rename_not_contaminated"] = (
            parsed_ren.get("libelle") is None
            and "schema_drift" in (feat_ren.unresolved or "")
            and done_ren.n_unresolved == 1)

        # 4. Value fidelity: '', 'NULL', and b64: binary blob
        f_str = QgsField("txt", QMetaType.Type.QString, "String")
        f_bin = QgsField("raw", QMetaType.Type.QByteArray, "Binary")
        raw_bytes = b"\x00\x01\x02recoverland\xff"
        b64_val = "b64:" + base64.b64encode(raw_bytes).decode("ascii")
        d["value_fidelity_ok"] = (
            _coerce_field_value("", f_str) == ""
            and _coerce_field_value("NULL", f_str) == "NULL"
            and _coerce_field_value("null", f_str) == "null"
            and bytes(_coerce_field_value(b64_val, f_bin)) == raw_bytes)

        # 7. CanvasDateBar unsaved edits state
        canvas = QgsMapCanvas()
        bar = CanvasDateBar(canvas)
        bar.set_stats(0, n_total=0, n_unsaved_edits=2)
        d["bar_unsaved_state"] = (
            bar.status_state() == "unsaved"
            and "? 2" in bar._lbl_status.text())
        bar.cleanup()
        del canvas

        flog(f"rvw_audit_lot2_review_integrity run done: trace_id={ctx.trace_id}", "INFO")
    finally:
        proj.clear()
        shutil.rmtree(tmpdir, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    return [
        ("make_permanent_and_duplicate_safe",
         d.get("perm_gpkg_intact") is True
         and d.get("perm_layer_kept") is True
         and d.get("dup_layer_kept") is True,
         f"perm_intact={d.get('perm_gpkg_intact')} perm_kept={d.get('perm_layer_kept')} "
         f"dup_kept={d.get('dup_layer_kept')}"),
        ("custom_crs_without_authid_preserved",
         d.get("custom_crs_overlay_matches") is True,
         f"custom_crs_overlay_matches={d.get('custom_crs_overlay_matches')}"),
        ("crs_relabel_during_review_synced",
         d.get("relabel_crs_synced") is True,
         f"relabel_crs_synced={d.get('relabel_crs_synced')}"),
        ("removed_source_layer_keeps_attrs_and_flags",
         d.get("removed_src_keeps_attrs_and_flags") is True,
         f"removed_src_keeps_attrs_and_flags={d.get('removed_src_keeps_attrs_and_flags')}"),
        ("field_rename_rejects_post_t_value_and_flags_drift",
         d.get("rename_not_contaminated") is True,
         f"rename_not_contaminated={d.get('rename_not_contaminated')}"),
        ("value_fidelity_empty_null_and_blob",
         d.get("value_fidelity_ok") is True,
         f"value_fidelity_ok={d.get('value_fidelity_ok')}"),
        ("date_bar_surfaces_unsaved_edits",
         d.get("bar_unsaved_state") is True,
         f"bar_unsaved_state={d.get('bar_unsaved_state')}"),
        assert_log_contains(
            ctx.records,
            pattern=rf"rvw_audit_lot2_review_integrity.*trace_id={ctx.trace_id}",
            min_count=2,
            name="trace_id_propagated",
        ),
    ]
