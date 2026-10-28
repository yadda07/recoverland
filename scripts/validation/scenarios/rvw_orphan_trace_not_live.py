# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_orphan_trace_not_live - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat a T ou nomme ce qu'il ne sait
pas ; il ne presente jamais l'etat actuel comme l'etat a T).

Le mecanisme
------------
Une trace de Rewind ne porte pas l'etat qu'elle a applique : son
attributes_json vaut ``{"_restore_ref": N}`` et sa geometrie est NULL ; cet
etat se lit dans l'evenement source N. La retention peut purger N et laisser
la trace (la trace devient orpheline). ``_materialize_traces`` la gardait
alors telle quelle, sans la signaler :

  * APRES T (image avant d'une entite preexistante) : ``_attrs_from_later``
    et ``_geom_before_later`` la sautaient comme "rien de change". Aucun
    champ ni la geometrie n'etant donne par un evenement, l'entite partait
    avec geom_from_live / attrs_from_live, et complete_from_live y mettait
    la valeur ACTUELLE, c'est-a-dire l'etat remis par le Rewind, pas celui
    de T. Rien ne la disait incomplete (n_unresolved=0).
  * AVANT T : ses changements etaient ignores. Une valeur donnee par un
    evenement plus ancien, que la trace a pourtant ecrasee, restait "connue"
    et l'emportait sur la valeur actuelle, qui etait la bonne. Une trace
    INSERT orpheline marquait l'etat "complet" avec zero attribut et sans
    geometrie : l'entite n'etait ni dessinee ni comptee.

L'echec concret pour l'utilisateur
----------------------------------
Le 1er mai il renomme une parcelle A -> B, le 10 juin un Rewind la remet a
A ; la retention purge ensuite l'edition du 1er mai. Review au 1er juin :
la parcelle s'appelait B, Review affiche A (l'etat d'apres le Rewind), sans
aucune alerte ; l'export ecrit A comme "etat au 1er juin".

Cas (un meme cache d'evenements, completion par une vraie couche memoire)
------------------------------------------------------------------------
O1  entite preexistante, trace UPDATE orpheline apres T : etat INCONNU,
    nomme (trace_source_missing), jamais complete avec la valeur actuelle.
O1c garde : la meme histoire avec la source presente -> nom=B, complet.
O2  UPDATE B->C (conserve), trace orpheline avant T (qui a remis le nom a
    A), UPDATE apres T : a T nom=A (valeur actuelle, que rien n'a change
    depuis la trace), pas C.
O3  trace INSERT orpheline avant T (re-insertion), renommee apres T : a T,
    dessinee (geometrie actuelle) avec son ancien nom, jamais "rien".
O5  garde : renommee apres T puis trace orpheline ANNULEE juste apres (la
    paire ne change rien) : etat a T connu, complet (nom=D5, surface
    actuelle), jamais signale a tort.
O4  SnapshotResult.n_trace_ref_missing, la barre et la table d'information
    de l'export disent combien d'entites dependent d'une trace orpheline.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rvw_orphan_trace_not_live"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FP = "ds_rvw_orphan_trace"
_T = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_T_ISO = "2026-06-01T12:00:00"


def _ev(eid, fid, op, created, attrs, src=None, geom=None, invalidated=None):
    from recoverland.core.audit_backend import AuditEvent
    return AuditEvent(
        event_id=eid, project_fingerprint="p", datasource_fingerprint=_FP,
        layer_id_snapshot="l", layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": fid, "pk_field": "fid", "pk_value": fid}),
        operation_type=op, attributes_json=json.dumps(attrs),
        geometry_wkb=geom, geometry_type="Point", crs_authid="EPSG:2154",
        field_schema_json="[]", user_name="u",
        session_id=None if src else "s", created_at=created,
        restored_from_event_id=src, entity_fingerprint=f"pk:fid={fid}",
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=invalidated)


def _events():
    return [
        # O1: the source (event 10) was purged; only the trace is left.
        _ev(50, 1, "UPDATE", "2026-06-10T10:00:00+00:00", {"_restore_ref": 10}, src=10),
        # O1c: same history, source kept.
        _ev(20, 2, "UPDATE", "2026-05-01T10:00:00+00:00",
            {"changed_only": {"nom": {"old": "A", "new": "B"}}}),
        _ev(51, 2, "UPDATE", "2026-06-10T10:00:00+00:00", {"_restore_ref": 20}, src=20),
        # O2: source 30 (nom A->B) purged, B->C kept, trace before T.
        _ev(31, 3, "UPDATE", "2026-04-20T10:00:00+00:00",
            {"changed_only": {"nom": {"old": "B", "new": "C"}}}),
        _ev(52, 3, "UPDATE", "2026-05-10T10:00:00+00:00", {"_restore_ref": 30}, src=30),
        _ev(33, 3, "UPDATE", "2026-06-10T10:00:00+00:00",
            {"changed_only": {"surface": {"old": 1.0, "new": 2.0}}}),
        # O3: re-insertion whose DELETE (40) was purged, renamed after T.
        _ev(53, 4, "INSERT", "2026-05-10T10:00:00+00:00", {"_restore_ref": 40}, src=40),
        _ev(41, 4, "UPDATE", "2026-06-10T10:00:00+00:00",
            {"changed_only": {"nom": {"old": "N4", "new": "N4b"}}}),
        # O5: renamed after T, then an orphan trace that was UNDONE right
        # after: the pair changes nothing, the state at T is fully known.
        _ev(61, 5, "UPDATE", "2026-06-05T10:00:00+00:00",
            {"changed_only": {"nom": {"old": "D5", "new": "E5"}}}),
        _ev(54, 5, "UPDATE", "2026-06-10T10:00:00+00:00", {"_restore_ref": 60},
            src=60, invalidated="2026-06-12T10:00:00+00:00"),
    ]


_KEYS = ("pk:fid=1", "pk:fid=2", "pk:fid=3", "pk:fid=4", "pk:fid=5")

# Live features, in fid order (a new GeoPackage numbers them from 1).
_LIVE = [("A", 5.0, 1.0), ("A", 5.0, 2.0), ("A", 2.0, 3.0), ("N4b", 4.0, 4.0),
         ("E5", 55.0, 5.0)]


def _make_live_layer(tmpdir):
    """A GeoPackage (stable fids, as a user layer) holding the live state."""
    from qgis.core import (QgsCoordinateTransformContext, QgsFeature,
                           QgsGeometry, QgsPointXY, QgsProject,
                           QgsVectorFileWriter, QgsVectorLayer)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=nom:string&field=surface:double",
                         "seed", "memory")
    feats = []
    for nom, surface, xy in _LIVE:
        f = QgsFeature(mem.fields())
        f.setAttributes([nom, surface])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(xy, xy)))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, "live.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(f"{path}|layername=parcelles", "parcelles", "ogr")
    QgsProject.instance().addMapLayer(lyr)
    return lyr


def _state(feat):
    from recoverland.core.geometry_utils import rebuild_geometry
    if feat is None:
        return None
    attrs = json.loads(feat.attrs_json) if feat.attrs_json else {}
    pt = None
    if feat.geom_wkb:
        g = rebuild_geometry(feat.geom_wkb)
        if g is not None and not g.isNull():
            p = g.asPoint()
            pt = (round(p.x(), 3), round(p.y(), 3))
    return {"nom": attrs.get("nom"), "surface": attrs.get("surface"), "xy": pt,
            "geom_known": feat.geom_wkb is not None,
            "unresolved": feat.unresolved,
            "from_live": (feat.geom_from_live, feat.attrs_from_live)}


def _read_info(path):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=recoverland_etat_info", "i", "ogr")
    if not lyr.isValid():
        return {}
    return {str(f["cle"]): str(f["valeur"]) for f in lyr.getFeatures()}


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rvw_orphan_")
    flog(f"rvw_orphan_trace_not_live setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.core.snapshot_overlay_session import (
        SnapshotOverlaySession, complete_from_live,
    )
    from recoverland.core.temporal_snapshot_engine import reconstruct_snapshot_at
    d = ctx.data
    lyr = None
    snap = None
    try:
        lyr = _make_live_layer(d["tmpdir"])
        d["live_fids"] = sorted((f.id(), f["nom"]) for f in lyr.getFeatures())
        info = {"fingerprint": _FP, "layer_id": lyr.id(),
                "layer_name": "parcelles", "storage_crs": "EPSG:2154"}
        raw = reconstruct_snapshot_at({_FP: _events()}, _T, trace_id=ctx.trace_id)
        d["engine"] = {k: _state(raw.features.get(_FP, {}).get(k))
                       for k in _KEYS}
        done = complete_from_live(raw, [info], ctx.trace_id)
        d["review"] = {k: _state(done.features.get(_FP, {}).get(k))
                       for k in _KEYS}
        d["n_unresolved"] = done.n_unresolved
        d["n_trace_ref_missing"] = getattr(done, "n_trace_ref_missing", None)

        from recoverland.widgets.canvas_date_bar import CanvasDateBar
        from qgis.gui import QgsMapCanvas
        canvas = QgsMapCanvas()
        bar = CanvasDateBar(canvas)
        try:
            bar.set_stats(3, n_total=3, n_unresolved=done.n_unresolved,
                          n_trace_ref_missing=getattr(done, "n_trace_ref_missing", 0))
            d["bar_tip"] = bar._lbl_status.toolTip()
        except TypeError as exc:
            d["bar_tip"] = f"set_stats refused: {exc}"
        bar.cleanup()

        snap = SnapshotOverlaySession()
        snap.start([info], "EPSG:2154", ctx.trace_id)
        out = os.path.join(d["tmpdir"], "etat.gpkg")
        d["export"] = {k: v for k, v in snap.export_to_geopackage(
            out, snapshot_result=done, cutoff_iso=_T_ISO).items() if k != "layers"}
        d["info"] = _read_info(out)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if snap is not None:
            snap.stop()
        if lyr is not None:
            QgsProject.instance().removeMapLayer(lyr.id())
    flog(f"rvw_orphan_trace_not_live run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    eng = d.get("engine") or {}
    rev = d.get("review") or {}
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    o1e = eng.get("pk:fid=1") or {}
    o1 = rev.get("pk:fid=1") or {}
    out = []
    out.append((
        "o1_orphan_after_t_is_named_not_left_to_live",
        "trace_source_missing" in (o1e.get("unresolved") or "")
        and o1e.get("from_live") == (False, False),
        f"moteur, O1 a T={o1e or 'ABSENTE'} : la trace orpheline apres T peut "
        f"avoir change n'importe quel champ, l'entite doit etre inconnue "
        f"(trace_source_missing) et non confiee a la couche actuelle{crash}",
    ))
    out.append((
        "o1_review_never_shows_the_value_the_rewind_put_back",
        o1.get("nom") != "A" and "trace_source_missing" in (o1.get("unresolved") or ""),
        f"Review, O1 a T={o1 or 'ABSENTE'} : 'A' est la valeur remise par le "
        f"Rewind du 10 juin, pas celle du 1er juin{crash}",
    ))
    o1c = rev.get("pk:fid=2") or {}
    out.append((
        "o1c_guard_source_present_is_resolved",
        o1c.get("nom") == "B" and o1c.get("xy") == (2.0, 2.0) and not o1c.get("unresolved"),
        f"Review, O1c (source presente) a T={o1c or 'ABSENTE'} (attendu nom=B "
        f"en (2,2), complet){crash}",
    ))
    o2 = rev.get("pk:fid=3") or {}
    out.append((
        "o2_orphan_before_t_overrides_older_values",
        o2.get("nom") == "A" and o2.get("surface") == 1.0 and o2.get("xy") == (3.0, 3.0),
        f"Review, O2 a T={o2 or 'ABSENTE'} (attendu nom=A, surface=1 en (3,3) : "
        f"la trace orpheline a ecrase le nom C, la valeur actuelle A n'a pas "
        f"change depuis){crash}",
    ))
    o3 = rev.get("pk:fid=4") or {}
    out.append((
        "o3_orphan_reinsert_before_t_is_drawn_not_dropped",
        (o3.get("nom") == "N4" and o3.get("xy") == (4.0, 4.0) and o3.get("surface") == 4.0)
        or bool(o3.get("unresolved")),
        f"Review, O3 a T={o3 or 'ABSENTE'} (attendu nom=N4 en (4,4), ou au "
        f"moins comptee comme incomplete : jamais ni dessinee ni comptee){crash}",
    ))
    o5 = rev.get("pk:fid=5") or {}
    out.append((
        "o5_guard_orphan_undone_at_once_is_neutral",
        o5.get("nom") == "D5" and o5.get("surface") == 55.0
        and o5.get("xy") == (5.0, 5.0) and not o5.get("unresolved"),
        f"Review, O5 a T={o5 or 'ABSENTE'} (attendu nom=D5, surface=55 en "
        f"(5,5), complet : une trace annulee aussitot ne change rien){crash}",
    ))
    out.append((
        "o4_result_counts_entities_depending_on_a_purged_source",
        d.get("n_trace_ref_missing") == 1 and (d.get("n_unresolved") or 0) >= 1,
        f"n_trace_ref_missing={d.get('n_trace_ref_missing')} n_unresolved="
        f"{d.get('n_unresolved')} (attendu 1 : O1){crash}",
    ))
    out.append((
        "o4_bar_names_the_purged_trace_source",
        "purg" in (d.get("bar_tip") or ""),
        f"infobulle de la barre={d.get('bar_tip')!r} : elle doit dire qu'une "
        f"entite depend d'une trace dont l'evenement source a ete purge",
    ))
    info = d.get("info") or {}
    out.append((
        "o4_export_info_table_names_it",
        info.get("entites_trace_source_purgee") == "1",
        f"table recoverland_etat_info={info or 'ABSENTE'} export={d.get('export')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_orphan_trace_not_live.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
