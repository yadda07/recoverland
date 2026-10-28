# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_display_crs_verdict - RecoverLand validation runtime.

Invariant: I-3 (un etat passe est montre la ou il etait, ou pas du tout)
et I-8 (ce qui ne peut pas etre place est refuse en le nommant), cote
AFFICHAGE : Review et l'apercu de geometrie suivent le verdict de SCR du
Rewind (geometry_utils.crs_verdict).

Le mecanisme
------------
Le journal garde les coordonnees BRUTES de la capture ; `crs_authid` n'est
que le SCR que la couche DECLARAIT alors. Quand la couche en declare un
autre aujourd'hui, deux histoires sont possibles : changement d'etiquette
(Definir le SCR, .prj corrige : les coordonnees brutes sont deja dans le
SCR de la couche) ou reprojection du fichier en place (il faut reprojeter).
Le Rewind decide sur preuves et refuse d'ecrire quand rien ne departage
(crs_ambiguous). L'affichage ne suivait pas ce verdict :
  * l'apercu de geometrie (core/geometry_preview.GeometryPreviewManager
    .show) reprojetait TOUJOURS depuis le SCR de capture : apres un
    changement d'etiquette, l'etat passe etait dessine a la position
    reprojetee, a des centaines de km de celle ou le Rewind le remet ;
  * les calques Review (snapshot_overlay_session) dessinaient TOUJOURS les
    coordonnees brutes dans le SCR actuel : apres une reprojection en
    place, l'etat passe tombait la ou tombent les nombres de l'autre SCR ;
  * quand rien ne departage, les deux dessinaient quand meme, la ou le
    Rewind refuse d'ecrire.

L'echec concret pour l'utilisateur
----------------------------------
(L) Sa couche Lambert-93 avait ete reglee par erreur en CC46 pendant des
editions, puis remise en EPSG:2154 : l'apercu d'un evenement montre
l'entite en mer, a ~1 600 km, alors que le Rewind la remet a sa place.
(R) Son fichier en degres a ete reecrit en Lambert-93 au meme chemin :
Review montre la parcelle A, a une date anterieure, pres de l'origine du
Lambert-93 (au large de l'Espagne) au lieu de Paris. (A) Degres declares
EPSG:3857, corriges en EPSG:4326, couche vide : rien ne departage ; Review
et l'apercu dessinent quand meme une position que rien ne prouve, sans le
dire, alors que le Rewind refuse "crs_ambiguous".

Ce que le scenario pilote : de vrais GeoPackages (fournisseur OGR), un vrai
journal (schema du plugin), le vrai SnapshotRebuildWorker, la vraie
complete_from_live, la vraie SnapshotOverlaySession (calques dessines) et
le vrai GeometryPreviewManager sur un vrai QgsMapCanvas. L'apercu du cas
(L) montre la geometrie que porte l'entite (son propre temoin) ; l'apercu
que fait le dialogue Restore de l'ancienne geometrie de cet UPDATE, sans
les temoins de l'evenement, est l'objet de rvw_restore_preview_witnesses.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import math
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rvw_display_crs_verdict"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_CUTOFF = "2026-03-01T09:00:00"
_EDIT_AT = "2026-03-01T10:00:00.250000+00:00"

# (R) captured in degrees, file rewritten in Lambert-93 at the same path.
_R_OLD = (2.35, 48.85)
_R_NEW = (2.3505, 48.8505)
# (L) Lambert-93 coordinates captured while the layer declared CC46.
_L_OLD = (651000.0, 6861000.0)
_L_NEW = (652000.0, 6862000.0)
# (A) degrees declared EPSG:3857, the layer now EPSG:4326 and empty.
_A_GEOM = (2.36, 48.86)


def _pt(x, y):
    from qgis.core import QgsGeometry, QgsPointXY
    return QgsGeometry.fromPointXY(QgsPointXY(x, y))


def _wkb(x, y) -> bytes:
    return bytes(_pt(x, y).asWkb())


def _to_l93(x, y):
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsPointXY,
        QgsProject,
    )
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem("EPSG:4326"),
                                QgsCoordinateReferenceSystem("EPSG:2154"),
                                QgsProject.instance())
    p = tr.transform(QgsPointXY(x, y))
    return (p.x(), p.y())


def _gpkg(tmpdir, name, epsg, rows) -> str:
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsVectorFileWriter,
        QgsVectorLayer,
    )
    mem = QgsVectorLayer(f"Point?crs={epsg}&field=code:string(10)&"
                         f"field=nom:string(20)", "seed", "memory")
    feats = []
    for code, nom, xy in rows:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, nom])
        f.setGeometry(_pt(*xy))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _load(path, name):
    from qgis.core import QgsProject, QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not lyr.isValid():
        raise RuntimeError(f"invalid layer {path}")
    QgsProject.instance().addMapLayer(lyr)
    return lyr


class _Journal:
    def __init__(self, path):
        self.path = path
        self.is_open = True

    def create_read_connection(self):
        return sqlite3.connect(self.path, check_same_thread=False)


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_crs_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"rvw_display_crs_verdict setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def _build(ctx) -> dict:
    """Three layers, their journal rows, their layer infos."""
    from recoverland.core.identity import (
        compute_entity_fingerprint, compute_feature_identity,
    )
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    tmpdir = ctx.data["tmpdir"]
    layers = {
        "R": _load(_gpkg(tmpdir, "reproj", "EPSG:2154",
                         [("A", "A", _to_l93(*_R_NEW))]), "reproj"),
        "L": _load(_gpkg(tmpdir, "relabel", "EPSG:2154",
                         [("B", "B", _L_NEW)]), "relabel"),
        "A": _load(_gpkg(tmpdir, "ambig", "EPSG:4326",
                         [("Z", "Z", (0.0, 0.0))]), "ambig"),
    }
    # (A): the layer keeps nothing near the deleted feature: its only
    # other feature is removed, the file is empty as in the reviewer's case.
    lyr_a = layers["A"]
    lyr_a.dataProvider().deleteFeatures([f.id() for f in lyr_a.getFeatures()])

    def ident(layer):
        f = next(layer.getFeatures())
        ident_json = compute_feature_identity(layer, f)
        return ident_json, compute_entity_fingerprint(ident_json)

    path = os.path.join(tmpdir, "journal.sqlite")
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    ensure_v7_schema(conn)
    base = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    base.update({"project_fingerprint": "p", "layer_id_snapshot": "l",
                 "provider_type": "ogr", "geometry_type": "Point",
                 "user_name": "u", "event_schema_version": 7,
                 "created_at": _EDIT_AT})
    r_ident, r_key = ident(layers["R"])
    l_ident, l_key = ident(layers["L"])
    rows = [
        dict(base, event_id=1, datasource_fingerprint="ogr::rvw_crs_reproj",
             layer_name_snapshot="reproj", crs_authid="EPSG:4326",
             feature_identity_json=r_ident, entity_fingerprint=r_key,
             operation_type="UPDATE",
             attributes_json=json.dumps({"changed_only": {"nom": {"old": "A0", "new": "A"}}}),
             geometry_wkb=_wkb(*_R_OLD), new_geometry_wkb=_wkb(*_R_NEW)),
        dict(base, event_id=2, datasource_fingerprint="ogr::rvw_crs_relabel",
             layer_name_snapshot="relabel", crs_authid="EPSG:3946",
             feature_identity_json=l_ident, entity_fingerprint=l_key,
             operation_type="UPDATE",
             attributes_json=json.dumps({"changed_only": {"nom": {"old": "B0", "new": "B"}}}),
             geometry_wkb=_wkb(*_L_OLD), new_geometry_wkb=_wkb(*_L_NEW)),
        dict(base, event_id=3, datasource_fingerprint="ogr::rvw_crs_ambig",
             layer_name_snapshot="ambig", crs_authid="EPSG:3857",
             feature_identity_json=json.dumps({"fid": 7}), entity_fingerprint="fid:7",
             operation_type="DELETE",
             attributes_json=json.dumps({"all_attributes": {"code": "C", "nom": "C"}}),
             geometry_wkb=_wkb(*_A_GEOM)),
    ]
    cols = list(AUDIT_EVENT_COLUMNS)
    with conn:
        conn.executemany(
            f"INSERT INTO audit_event ({', '.join(cols)}) VALUES "  # nosec B608
            f"({', '.join('?' for _ in cols)})",
            [tuple(r[c] for c in cols) for r in rows])
    conn.close()
    infos = {
        key: {"fingerprint": fp, "layer_name": layers[key].name(),
              "layer_id": layers[key].id(),
              "storage_crs": layers[key].crs().authid(), "fid_stable": True}
        for key, fp in (("R", "ogr::rvw_crs_reproj"), ("L", "ogr::rvw_crs_relabel"),
                        ("A", "ogr::rvw_crs_ambig"))
    }
    return {"layers": layers, "journal": path, "infos": infos}


def _preview(canvas, layer, wkb, capture_crs, dst_epsg) -> dict:
    """The real preview on a real canvas: what it draws, or why not."""
    import qgis.utils
    from qgis.core import QgsCoordinateReferenceSystem
    from recoverland.core.geometry_preview import GeometryPreviewManager
    from scripts.validation.plugin_harness import FakeIface
    canvas.setDestinationCrs(QgsCoordinateReferenceSystem(dst_epsg))
    saved = qgis.utils.iface
    fake = FakeIface()
    qgis.utils.iface = fake
    try:
        mgr = GeometryPreviewManager(canvas)
        drawn = bool(mgr.show(wkb, capture_crs, layer))
        box = mgr._last_bbox if mgr._band is not None else None
        out = {
            "drawn": drawn,
            "band": mgr._band is not None,
            "at": ([box.center().x(), box.center().y()] if box is not None else None),
            "refusal": getattr(mgr, "last_refusal", None),
            "messages": [f"{t} | {x}" for t, x, _l, _d in fake.bar.messages],
        }
        mgr.clear()
    finally:
        qgis.utils.iface = saved
    return out


def _review(journal, info) -> dict:
    """Real rebuild, real live completion, real overlay: what is drawn."""
    from qgis.core import QgsProject
    from recoverland.core.snapshot_overlay_session import (
        SnapshotOverlaySession, complete_from_live,
    )
    from recoverland.widgets.snapshot_rebuild_worker import SnapshotRebuildWorker
    got = []
    worker = SnapshotRebuildWorker(_Journal(journal), [info], _CUTOFF)
    worker.result_ready.connect(lambda _t, r: got.append(r))
    worker.error.connect(lambda _t, e: got.append(e))
    worker.run()
    if not got or isinstance(got[0], str):
        return {"error": str(got)}
    done = complete_from_live(got[0], [info], "rvw_crs")
    snap = SnapshotOverlaySession()
    drawn = {}
    try:
        snap.start([info], "EPSG:2154", "rvw_crs")
        snap.update(done)
        for lid in snap.overlay_layer_ids():
            lyr = QgsProject.instance().mapLayer(lid)
            for f in lyr.getFeatures() if lyr is not None else ():
                g = f.geometry()
                if g is None or g.isNull():
                    continue
                p = g.asPoint()
                drawn[str(f["code"])] = [p.x(), p.y()]
    finally:
        snap.stop()
    feats = [f for ents in done.features.values() for f in ents.values()]
    return {
        "drawn": drawn,
        "n_entities": len(feats),
        "unresolved": [f.unresolved for f in feats],
        "n_unresolved": done.n_unresolved,
        "messages": _dialog_messages(done),
    }


def _dialog_messages(done) -> list:
    """What the Review dialog tells the user about this reconstruction.

    The dialog's own method (RecoverDialog._review_surface_snapshot_gaps),
    run on a stand-in carrying only what it reads: the message bar and tr.
    """
    from recoverland.recover_dialog import RecoverDialog
    from scripts.validation.plugin_harness import FakeIface

    class _Stub:
        def __init__(self):
            self.iface = FakeIface()

        @staticmethod
        def tr(text):
            return text

    stub = _Stub()
    RecoverDialog._review_surface_snapshot_gaps(stub, "rvw_crs", done)
    return [f"{t} | {x}" for t, x, _l, _d in stub.iface.bar.messages]


def run(ctx):
    from qgis.gui import QgsMapCanvas
    from recoverland.core.logger import flog
    d = ctx.data
    built = None
    canvas = None
    try:
        built = _build(ctx)
        layers, infos = built["layers"], built["infos"]
        canvas = QgsMapCanvas()
        canvas.resize(800, 600)
        # (L) previews the geometry the live feature carries: it is its own
        # witness. What the Restore dialog previews for this UPDATE (the OLD
        # side, without the event's witnesses) is rvw_restore_preview_witnesses.
        d["preview_L"] = _preview(canvas, layers["L"], _wkb(*_L_NEW),
                                  "EPSG:3946", "EPSG:2154")
        d["preview_A"] = _preview(canvas, layers["A"], _wkb(*_A_GEOM),
                                  "EPSG:3857", "EPSG:4326")
        d["expected_R"] = list(_to_l93(*_R_OLD))
        for key in ("R", "L", "A"):
            d[f"review_{key}"] = _review(built["journal"], infos[key])
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if built is not None:
            from qgis.core import QgsProject
            for lyr in built["layers"].values():
                QgsProject.instance().removeMapLayer(lyr.id())
        canvas = None
    flog(f"rvw_display_crs_verdict run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _near(a, b, tol) -> bool:
    return (a is not None and b is not None
            and math.hypot(a[0] - b[0], a[1] - b[1]) <= tol)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    pl = d.get("preview_L") or {}
    out.append((
        "preview_after_a_relabel_draws_the_raw_position",
        pl.get("drawn") is True and _near(pl.get("at"), _L_NEW, 1.0),
        f"apercu={pl} (attendu dessine a {_L_NEW}, la ou l'entite est et ou "
        f"le Rewind la remet) : reprojete depuis le SCR de capture, il "
        f"tombait a des centaines de km{crash}",
    ))
    pa = d.get("preview_A") or {}
    out.append((
        "preview_refuses_by_name_when_the_crs_story_is_ambiguous",
        pa.get("drawn") is False and pa.get("band") is False
        and "relabel" in (pa.get("refusal") or "")
        and any("relabel" in m for m in pa.get("messages") or []),
        f"apercu={pa} : rien ne departage un changement d'etiquette d'une "
        f"reprojection, le Rewind refuse (crs_ambiguous) ; l'apercu doit "
        f"ne rien dessiner et le dire{crash}",
    ))
    rr = d.get("review_R") or {}
    out.append((
        "review_after_an_in_place_reprojection_draws_the_reprojected_state",
        _near((rr.get("drawn") or {}).get("A"), d.get("expected_R"), 1.0),
        f"Review={rr} (attendu A a {d.get('expected_R')}, sa position a cette "
        f"date dans le SCR actuel) : les coordonnees brutes en degres "
        f"etaient dessinees comme du Lambert-93{crash}",
    ))
    rl = d.get("review_L") or {}
    out.append((
        "review_after_a_relabel_keeps_the_raw_state",
        _near((rl.get("drawn") or {}).get("B"), _L_OLD, 1.0)
        and not any(rl.get("unresolved") or []),
        f"Review={rl} (attendu B a {_L_OLD}, brut, etat complet){crash}",
    ))
    ra = d.get("review_A") or {}
    out.append((
        "review_refuses_by_name_when_the_crs_story_is_ambiguous",
        ra.get("n_entities") == 1 and not (ra.get("drawn") or {})
        and any("crs_ambiguous" in (u or "") for u in ra.get("unresolved") or [])
        and ra.get("n_unresolved") == 1,
        f"Review={ra} (attendu : C existait a cette date, NON dessinee, "
        f"raison crs_ambiguous, comptee) : elle etait dessinee a une position "
        f"que rien ne prouve, la ou le Rewind refuse d'ecrire{crash}",
    ))
    out.append((
        "review_names_the_crs_refusal_to_the_user",
        any("SCR" in m for m in ra.get("messages") or [])
        and not any("SCR" in m for m in (rr.get("messages") or [])
                    + (rl.get("messages") or [])),
        f"messages A={ra.get('messages')} R={rr.get('messages')} "
        f"L={rl.get('messages')} : l'utilisateur doit savoir que l'entite "
        f"n'est pas dessinee faute de pouvoir la placer (SCR), pas seulement "
        f"qu'un etat est incomplet{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_display_crs_verdict.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
