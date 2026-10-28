# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_audit_lot1_identity_values - RecoverLand validation runtime.

Invariant: I-3 / I-11 / BL-SER-P0-01.

Couvre les 5 constats du Lot 1 de l'audit adversarial final :
  1. GeoPackage et FlatGeobuf mono-couche ouverts avec ou sans ``|layername=``
     partagent la meme empreinte canonique, declenchent le garde-fou de
     collision, gardent la selection de suivi et unifient le Rewind.
  2. SpatiaLite : deux ouvertures dont la casse du chemin ``dbname`` differe
     sont reconciliees par ``datasource_fingerprints_match``, la detection de
     collision et ``expand_fingerprints``.
  3. Preuve geometrique : ``feature_matches_geometry`` et ``_canonical_rings``
     acceptent la rotation/inversion d'un anneau de polygone mais rejettent
     une ligne inversee (Reverse Line) ainsi qu'une permutation de sommets
     formant un papillon (bow-tie).
  4. Valeur ``NaN`` dans un champ Real (Shapefile / FlatGeobuf) :
     ``_qgis_vals_equal`` reconnait ``NaN == NaN`` (la preuve d'etat et le
     garde d'idempotence ne rejettent plus l'entite).
  5. Valeur ``QTime`` avec millisecondes : ``_qgis_vals_equal`` rejette une
     heure stockee ``10:15:42.517`` face a un attendu ``'10:15:42'`` (seconde
     entiere), tout en acceptant la troncature inverse par un provider a la
     seconde.
"""
from __future__ import annotations

import math
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_audit_lot1_identity_values"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_lot1_")
    flog(f"tx_audit_lot1_identity_values setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsFeature, QgsField, QgsFields,
        QgsGeometry, QgsPointXY, QgsProject, QgsVectorFileWriter,
        QgsVectorLayer, QgsWkbTypes,
    )
    from qgis.PyQt.QtCore import QMetaType, QTime
    from recoverland.core.datasource_alias import (
        canonicalize_event_fingerprints, expand_fingerprints,
    )
    from recoverland.core.geometry_utils import (
        _canonical_rings, feature_matches_geometry,
    )
    from recoverland.core.identity import (
        canonicalize_fingerprint, compute_datasource_fingerprint,
        datasource_fingerprints_match, find_fingerprint_collisions,
        layer_is_selected,
    )
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        _qgis_vals_equal, restore_updated_feature,
    )
    from scripts.validation.scenarios import _restore_fixtures as fx

    d = ctx.data
    tmpdir = d["tmpdir"]
    try:
        fields = QgsFields()
        fields.append(QgsField("nom", QMetaType.Type.QString))
        fields.append(QgsField("mesure", QMetaType.Type.Double))
        crs = QgsCoordinateReferenceSystem("EPSG:4326")

        # 1. Single-layer GPKG and FGB: bare vs |layername=
        for ext, drv, tname in ((".gpkg", "GPKG", "routes"),
                                (".fgb", "FlatGeobuf", "voies")):
            p = os.path.join(tmpdir, f"single{ext}")
            opts = QgsVectorFileWriter.SaveVectorOptions()
            opts.driverName = drv
            opts.layerName = tname
            w = QgsVectorFileWriter.create(
                p, fields, QgsWkbTypes.Type.Point, crs,
                QgsProject.instance().transformContext(), opts)
            del w
            l_bare = QgsVectorLayer(p, "bare", "ogr")
            l_named = QgsVectorLayer(f"{p}|layername={tname}", "named", "ogr")
            fp_b = compute_datasource_fingerprint(l_bare)
            fp_n = compute_datasource_fingerprint(l_named)
            raw_bare_fp = f"ogr::{os.path.normcase(os.path.abspath(p)).replace(chr(92), '/')}"
            d[f"{drv}_fp_equal"] = (fp_b == fp_n and "|layername=" in fp_b)
            d[f"{drv}_canon_equal"] = (canonicalize_fingerprint(raw_bare_fp) == fp_n)
            d[f"{drv}_match"] = datasource_fingerprints_match(raw_bare_fp, fp_n)
            d[f"{drv}_collision"] = bool(find_fingerprint_collisions([l_bare, l_named]))
            d[f"{drv}_sel_bare_to_named"] = layer_is_selected(l_named, {raw_bare_fp})
            d[f"{drv}_sel_named_to_bare"] = layer_is_selected(l_bare, {fp_n})

        # Multi-layer GPKG: two tables stay distinct
        multi_p = os.path.join(tmpdir, "multi.gpkg")
        for idx, tname in enumerate(("t_un", "t_deux")):
            opts = QgsVectorFileWriter.SaveVectorOptions()
            opts.driverName = "GPKG"
            opts.layerName = tname
            if idx > 0:
                opts.actionOnExistingFile = (
                    QgsVectorFileWriter.ActionOnExistingFile.CreateOrOverwriteLayer)
            w = QgsVectorFileWriter.create(
                multi_p, fields, QgsWkbTypes.Type.Point, crs,
                QgsProject.instance().transformContext(), opts)
            del w
        lm1 = QgsVectorLayer(f"{multi_p}|layername=t_un", "m1", "ogr")
        lm2 = QgsVectorLayer(f"{multi_p}|layername=t_deux", "m2", "ogr")
        d["multi_gpkg_distinct"] = (
            compute_datasource_fingerprint(lm1) != compute_datasource_fingerprint(lm2)
            and not find_fingerprint_collisions([lm1, lm2]))

        # 2. SpatiaLite directory case equivalence
        s_low = "spatialite::dbname='C:/Projet/zones.sqlite' table=\"zones\" (geometry)"
        s_up = "spatialite::dbname='C:/PROJET/zones.sqlite' table=\"zones\" (geometry)"
        d["spl_case_match"] = datasource_fingerprints_match(s_low, s_up)

        # Journal expand_fingerprints & canonicalize_event_fingerprints
        jdir = os.path.join(tmpdir, "j")
        os.makedirs(jdir, exist_ok=True)
        jm = JournalManager()
        jm.open_for_project("", jdir)
        try:
            conn = jm.get_connection()
            gpkg_p = os.path.join(tmpdir, "single.gpkg")
            raw_bare = f"ogr::{os.path.normcase(os.path.abspath(gpkg_p)).replace(chr(92), '/')}"
            canon_named = f"{raw_bare}|layername=routes"
            expanded = expand_fingerprints(conn, canon_named)
            d["expand_includes_bare"] = (raw_bare in expanded and canon_named in expanded)
            ev = fx.audit_event(event_id=1, operation_type="UPDATE",
                                datasource_fingerprint=raw_bare)
            unified = canonicalize_event_fingerprints(conn, [ev])
            d["event_fp_unified"] = (unified[0].datasource_fingerprint == canon_named)
        finally:
            jm.close()

        # 3. Geometry proof: reversed line & bow-tie polygon rejected, ring rotation accepted
        g_line = QgsGeometry.fromPolylineXY(
            [QgsPointXY(0, 0), QgsPointXY(1, 2), QgsPointXY(3, 0)])
        g_rev = QgsGeometry.fromPolylineXY(
            [QgsPointXY(3, 0), QgsPointXY(1, 2), QgsPointXY(0, 0)])
        f_rev = QgsFeature()
        f_rev.setGeometry(g_rev)
        d["reversed_line_rejected"] = not feature_matches_geometry(f_rev, g_line)

        poly_rect = QgsGeometry.fromPolygonXY([[
            QgsPointXY(0, 0), QgsPointXY(2, 0),
            QgsPointXY(2, 2), QgsPointXY(0, 2), QgsPointXY(0, 0),
        ]])
        poly_rot_ccw = QgsGeometry.fromPolygonXY([[
            QgsPointXY(2, 2), QgsPointXY(2, 0),
            QgsPointXY(0, 0), QgsPointXY(0, 2), QgsPointXY(2, 2),
        ]])
        poly_bowtie = QgsGeometry.fromPolygonXY([[
            QgsPointXY(0, 0), QgsPointXY(2, 2),
            QgsPointXY(2, 0), QgsPointXY(0, 2), QgsPointXY(0, 0),
        ]])
        d["ring_rot_orient_accepted"] = (
            _canonical_rings(poly_rect) == _canonical_rings(poly_rot_ccw))
        d["ring_bowtie_rejected"] = (
            _canonical_rings(poly_rect) != _canonical_rings(poly_bowtie))

        # 4. NaN equality and Shapefile UPDATE restore proof when current value is NaN
        d["nan_vals_equal"] = _qgis_vals_equal(float("nan"), float("nan"))
        shp_nan = fx.make_shapefile(
            tmpdir, "nan_shp", "Point", ["nom:string(20)", "val:double"],
            [(["A", float("nan")], "Point (1 1)")])
        lyr_nan = QgsVectorLayer(shp_nan, "nan_shp", "ogr")
        ev_nan = fx.audit_event(
            event_id=10, operation_type="UPDATE",
            field_schema_json=fx.schema(("nom", "string"), ("val", "double")),
            feature_identity_json='{"fid": 0}', entity_fingerprint="fid:0",
            attributes_json='{"changed_only": {"val": {"old": 12.5, "new": NaN}}}',
            geometry_wkb=fx.wkb_wkt("Point (1 1)"),
            new_geometry_wkb=fx.wkb_wkt("Point (1 1)"),
            geometry_type="Point")
        res_nan = restore_updated_feature(lyr_nan, ev_nan)
        feat_after = next(lyr_nan.getFeatures())
        d["nan_restore_ok"] = (
            res_nan.get("success") is True
            and math.isclose(float(feat_after["val"]), 12.5))

        # 5. QTime milliseconds proof
        d["qtime_subsec_rejected_against_whole"] = not _qgis_vals_equal(
            QTime(10, 15, 42, 517), "10:15:42")
        d["qtime_subsec_matches_exact"] = _qgis_vals_equal(
            QTime(10, 15, 42, 517), "10:15:42.517")
        d["qtime_whole_matches_truncated_subsec"] = _qgis_vals_equal(
            QTime(10, 15, 42, 0), "10:15:42.517")

        flog(f"tx_audit_lot1_identity_values run done: trace_id={ctx.trace_id}", "INFO")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    return [
        ("gpkg_single_layer_unified",
         d.get("GPKG_fp_equal") is True and d.get("GPKG_canon_equal") is True
         and d.get("GPKG_match") is True and d.get("GPKG_collision") is True
         and d.get("GPKG_sel_bare_to_named") is True
         and d.get("GPKG_sel_named_to_bare") is True,
         f"GPKG mono-couche divise : { {k: d.get(k) for k in d if k.startswith('GPKG_')} }"),
        ("fgb_single_layer_unified",
         d.get("FlatGeobuf_fp_equal") is True and d.get("FlatGeobuf_canon_equal") is True
         and d.get("FlatGeobuf_match") is True and d.get("FlatGeobuf_collision") is True
         and d.get("FlatGeobuf_sel_bare_to_named") is True
         and d.get("FlatGeobuf_sel_named_to_bare") is True,
         f"FlatGeobuf mono-couche divise : { {k: d.get(k) for k in d if k.startswith('FlatGeobuf_')} }"),
        ("multi_gpkg_tables_stay_distinct",
         d.get("multi_gpkg_distinct") is True,
         f"multi_gpkg_distinct={d.get('multi_gpkg_distinct')}"),
        ("expand_and_canonicalize_unify_bare_gpkg",
         d.get("expand_includes_bare") is True and d.get("event_fp_unified") is True,
         f"expand={d.get('expand_includes_bare')} unified={d.get('event_fp_unified')}"),
        ("spatialite_case_reconciled",
         d.get("spl_case_match") is True,
         f"spl_case_match={d.get('spl_case_match')}"),
        ("reversed_line_and_bowtie_rejected",
         d.get("reversed_line_rejected") is True
         and d.get("ring_rot_orient_accepted") is True
         and d.get("ring_bowtie_rejected") is True,
         f"rev_line={d.get('reversed_line_rejected')} "
         f"ring_rot={d.get('ring_rot_orient_accepted')} "
         f"bowtie={d.get('ring_bowtie_rejected')}"),
        ("nan_field_restored",
         d.get("nan_vals_equal") is True and d.get("nan_restore_ok") is True,
         f"nan_vals_equal={d.get('nan_vals_equal')} nan_restore_ok={d.get('nan_restore_ok')}"),
        ("qtime_millisecond_proof",
         d.get("qtime_subsec_rejected_against_whole") is True
         and d.get("qtime_subsec_matches_exact") is True
         and d.get("qtime_whole_matches_truncated_subsec") is True,
         f"qtime={ {k: d.get(k) for k in d if k.startswith('qtime_')} }"),
        assert_log_contains(
            ctx.records,
            pattern=rf"tx_audit_lot1_identity_values.*trace_id={ctx.trace_id}",
            min_count=2,
            name="trace_id_propagated",
        ),
    ]
