# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_display_story_readonly - RecoverLand validation runtime.

Invariant: I-3 (jamais d'ecriture sur une position non prouvee) : un
AFFICHAGE ne decide jamais d'une ecriture.

Le mecanisme
------------
Le verdict de SCR (geometry_utils.crs_verdict) garde, par couche et par
SCR de capture, l'"histoire" que les preuves ont etablie (changement
d'etiquette ou reprojection en place, table `_CRS_STORIES`). Une geometrie
sans preuve propre suit cette histoire ; sans histoire, elle est refusee
(crs_ambiguous). L'apercu de geometrie (GeometryPreviewManager.show) et
Review (snapshot_overlay_session.complete_from_live) passent par ce meme
verdict pour dessiner, et l'ECRIVAIENT : la table apprenait l'histoire d'un
simple clic. L'apercu du dialogue Restore ne passe aucun temoin
(recover_dialog._update_geometry_preview) : il etablit l'histoire a partir
d'une partie seulement des preuves d'un evenement, alors que l'ensemble est
un conflit.

L'echec concret pour l'utilisateur
----------------------------------
(P) Couche en EPSG:4326, evenements captures quand elle se declarait
EPSG:3857. Il clique la ligne de l'evenement E1 dans Restore (apercu),
puis restaure l'evenement E2, qui n'a aucune preuve propre : sans le clic,
la restauration est refusee (crs_ambiguous) ; apres le clic, E2 est ecrite
brute en (2.40, 48.90), position que rien ne prouve. (R) Couche Lambert-93
capturee en CC46 puis corrigee : il regarde une date dans Review, puis
restaure la suppression E2, sans preuve propre : la date regardee suffit a
faire ecrire E2. Ce qu'il a regarde decide de ce qui est ecrit.

Ce que le scenario pilote : de vrais GeoPackages (fournisseur OGR), le vrai
GeometryPreviewManager sur un vrai QgsMapCanvas appele comme le dialogue
l'appelle, le vrai SnapshotRebuildWorker et la vraie complete_from_live sur
un vrai journal, et la question d'ecriture posee a l'aide que les deux
moteurs d'ecriture utilisent (geometry_utils.geometry_for_layer_crs, avec
les temoins de l'evenement comme le Rewind les passe). Controle : la meme
question sur une couche identique jamais affichee. Et l'autre sens :
l'affichage LIT toujours une histoire etablie par une ecriture.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rvw_display_story_readonly"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# (P) degrees declared EPSG:3857, the layer now EPSG:4326.
_P_E1_OLD = (2.36, 48.86)
_P_E1_NEW = (2.37, 48.87)
_P_E2 = (2.40, 48.90)
# (R) Lambert-93 numbers captured while the layer declared CC46 (EPSG:3946).
_R_OLD = (651000.0, 6861000.0)
_R_NEW = (652000.0, 6862000.0)
_R_E2 = (655000.0, 6865000.0)
_CUTOFF = "2026-03-01T09:00:00"


def _pt(x, y):
    from qgis.core import QgsGeometry, QgsPointXY
    return QgsGeometry.fromPointXY(QgsPointXY(x, y))


def _wkb(xy) -> bytes:
    return bytes(_pt(*xy).asWkb())


def _repro(xy, src, dst):
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsPointXY,
        QgsProject,
    )
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem(src),
                                QgsCoordinateReferenceSystem(dst),
                                QgsProject.instance())
    p = tr.transform(QgsPointXY(*xy))
    return (p.x(), p.y())


def _layer(tmpdir, name, epsg, rows):
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsProject,
        QgsVectorFileWriter, QgsVectorLayer,
    )
    mem = QgsVectorLayer(f"Point?crs={epsg}&field=code:string(10)&"
                         f"field=nom:string(20)", "seed", "memory")
    feats = []
    for code, xy in rows:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, code])
        f.setGeometry(_pt(*xy))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not lyr.isValid():
        raise RuntimeError(f"invalid layer {path}")
    QgsProject.instance().addMapLayer(lyr)
    return lyr


def _stories():
    """The per-layer CRS stories, as the write paths will read them."""
    from recoverland.core import geometry_utils
    table = getattr(geometry_utils, "_CRS_STORIES", None)
    return dict(table) if isinstance(table, dict) else {"<unreadable>": repr(table)}


def _changed(before: dict, after: dict) -> list:
    return sorted(str(k) for k in set(before) | set(after)
                  if before.get(k) != after.get(k))


def _write_question(layer, xy, src, witnesses) -> dict:
    """What a restore of this geometry would write (the helper both engines use)."""
    from recoverland.core.geometry_utils import geometry_for_layer_crs
    geom, status = geometry_for_layer_crs(_pt(*xy), src, layer, "rvw_story",
                                          witnesses)
    at = None
    if geom is not None and not geom.isNull():
        p = geom.asPoint()
        at = [p.x(), p.y()]
    return {"status": status, "at": at}


def _preview(canvas, layer, xy, src, dst_epsg) -> dict:
    """The Restore preview, called as recover_dialog._update_geometry_preview calls it."""
    import qgis.utils
    from qgis.core import QgsCoordinateReferenceSystem
    from recoverland.core.geometry_preview import GeometryPreviewManager
    from scripts.validation.plugin_harness import FakeIface
    canvas.setDestinationCrs(QgsCoordinateReferenceSystem(dst_epsg))
    saved = qgis.utils.iface
    qgis.utils.iface = FakeIface()
    try:
        mgr = GeometryPreviewManager(canvas)
        drawn = bool(mgr.show(_wkb(xy), src, layer))
        box = mgr._last_bbox if mgr._band is not None else None
        out = {"drawn": drawn,
               "at": [box.center().x(), box.center().y()] if box is not None else None,
               "refusal": mgr.last_refusal}
        mgr.clear()
    finally:
        qgis.utils.iface = saved
    return out


class _Journal:
    def __init__(self, path):
        self.path = path
        self.is_open = True

    def create_read_connection(self):
        return sqlite3.connect(self.path, check_same_thread=False)


def _review(tmpdir, layer, fp) -> dict:
    """A Review date on *layer*: real journal, real rebuild, real live completion."""
    from recoverland.core.identity import (
        compute_entity_fingerprint, compute_feature_identity,
    )
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.snapshot_overlay_session import complete_from_live
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    from recoverland.widgets.snapshot_rebuild_worker import SnapshotRebuildWorker
    b = next(layer.getFeatures())
    b_ident = compute_feature_identity(layer, b)
    path = os.path.join(tmpdir, f"journal_{layer.name()}.sqlite")
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    ensure_v7_schema(conn)
    base = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    base.update({"project_fingerprint": "p", "layer_id_snapshot": "l",
                 "provider_type": "ogr", "geometry_type": "Point",
                 "user_name": "u", "event_schema_version": 7,
                 "datasource_fingerprint": fp, "layer_name_snapshot": fp,
                 "crs_authid": "EPSG:3946"})
    rows = [
        # E2: deleted BEFORE the date looked at, so not part of the Review.
        dict(base, event_id=1, created_at="2026-03-01T08:00:00.250000+00:00",
             feature_identity_json=json.dumps({"fid": 99}),
             entity_fingerprint="fid:99", operation_type="DELETE",
             attributes_json=json.dumps({"all_attributes": {"code": "E2", "nom": "E2"}}),
             geometry_wkb=_wkb(_R_E2)),
        # B moved AFTER the date: Review draws its old position.
        dict(base, event_id=2, created_at="2026-03-01T10:00:00.250000+00:00",
             feature_identity_json=b_ident,
             entity_fingerprint=compute_entity_fingerprint(b_ident),
             operation_type="UPDATE",
             attributes_json=json.dumps({"changed_only": {"nom": {"old": "B0", "new": "B"}}}),
             geometry_wkb=_wkb(_R_OLD), new_geometry_wkb=_wkb(_R_NEW)),
    ]
    cols = list(AUDIT_EVENT_COLUMNS)
    with conn:
        conn.executemany(
            f"INSERT INTO audit_event ({', '.join(cols)}) VALUES "  # nosec B608
            f"({', '.join('?' for _ in cols)})",
            [tuple(r[c] for c in cols) for r in rows])
    conn.close()
    info = {"fingerprint": fp, "layer_name": layer.name(), "layer_id": layer.id(),
            "storage_crs": layer.crs().authid(), "fid_stable": True}
    got = []
    worker = SnapshotRebuildWorker(_Journal(path), [info], _CUTOFF)
    worker.result_ready.connect(lambda _t, r: got.append(r))
    worker.error.connect(lambda _t, e: got.append(e))
    worker.run()
    if not got or isinstance(got[0], str):
        return {"error": str(got)}
    done = complete_from_live(got[0], [info], "rvw_story")
    feats = [f for ents in done.features.values() for f in ents.values()]
    drawn = []
    for f in feats:
        if f.geom_wkb:
            from recoverland.core.geometry_utils import rebuild_geometry
            g = rebuild_geometry(f.geom_wkb)
            if g is not None and not g.isNull():
                drawn.append([g.asPoint().x(), g.asPoint().y()])
    return {"n_entities": len(feats), "drawn": drawn,
            "unresolved": [f.unresolved for f in feats]}


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_story_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"rvw_display_story_readonly setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from qgis.gui import QgsMapCanvas
    from recoverland.core.logger import flog
    d = ctx.data
    tmp = d["tmpdir"]
    layers = []
    canvas = QgsMapCanvas()
    canvas.resize(800, 600)
    try:
        y_at = _repro(_P_E1_NEW, "EPSG:3857", "EPSG:4326")
        p_rows = [("X", _P_E1_OLD), ("Y", y_at)]
        e1_witnesses = (_wkb(_P_E1_OLD), _wkb(_P_E1_NEW))
        e2_p = (_wkb(_P_E2), None)
        # (P) control: never displayed.
        p0 = _layer(tmp, "p_ctl", "EPSG:4326", p_rows)
        layers.append(p0)
        d["p_control_e1"] = _write_question(p0, _P_E1_OLD, "EPSG:3857", e1_witnesses)
        d["p_control_e2"] = _write_question(p0, _P_E2, "EPSG:3857", e2_p)
        # (P) the user clicks E1 in Restore, then restores E2.
        p1 = _layer(tmp, "p_prv", "EPSG:4326", p_rows)
        layers.append(p1)
        before = _stories()
        d["p_preview_e1"] = _preview(canvas, p1, _P_E1_OLD, "EPSG:3857", "EPSG:4326")
        d["p_stories_changed"] = _changed(before, _stories())
        d["p_after_e2"] = _write_question(p1, _P_E2, "EPSG:3857", e2_p)
        d["p_after_e1"] = _write_question(p1, _P_E1_OLD, "EPSG:3857", e1_witnesses)

        r_rows = [("B", _R_NEW)]
        e2_r = (_wkb(_R_E2), None)
        # (R) control: never displayed.
        r0 = _layer(tmp, "r_ctl", "EPSG:2154", r_rows)
        layers.append(r0)
        d["r_control_e2"] = _write_question(r0, _R_E2, "EPSG:3946", e2_r)
        # (R) the user looks at a date in Review, then restores E2.
        r1 = _layer(tmp, "r_rvw", "EPSG:2154", r_rows)
        layers.append(r1)
        before = _stories()
        d["r_review"] = _review(tmp, r1, "ogr::rvw_story_r1")
        d["r_stories_changed"] = _changed(before, _stories())
        d["r_after_e2"] = _write_question(r1, _R_E2, "EPSG:3946", e2_r)
        # The other way: a story a WRITE settled is what the display follows.
        r2 = _layer(tmp, "r_read", "EPSG:2154", r_rows)
        layers.append(r2)
        d["r_write_settles"] = _write_question(
            r2, _R_OLD, "EPSG:3946", (_wkb(_R_OLD), _wkb(_R_NEW)))
        d["r_preview_e2_follows"] = _preview(canvas, r2, _R_E2, "EPSG:3946", "EPSG:2154")
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        for lyr in layers:
            QgsProject.instance().removeMapLayer(lyr.id())
        canvas = None
    flog(f"rvw_display_story_readonly run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _near(a, b, tol=1e-6) -> bool:
    return (a is not None and b is not None
            and abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "control_writes_without_evidence_are_refused",
        (d.get("p_control_e2") or {}).get("status") == "crs_ambiguous"
        and (d.get("p_control_e1") or {}).get("status") == "crs_ambiguous"
        and (d.get("r_control_e2") or {}).get("status") == "crs_ambiguous",
        f"controle (jamais affiche) : P E1={d.get('p_control_e1')} P E2="
        f"{d.get('p_control_e2')} R E2={d.get('r_control_e2')} (attendu "
        f"crs_ambiguous : rien ne prouve la position){crash}",
    ))
    out.append((
        "a_restore_preview_never_turns_a_refused_write_into_a_write",
        (d.get("p_after_e2") or {}).get("status") == "crs_ambiguous"
        and (d.get("p_after_e1") or {}).get("status") == "crs_ambiguous",
        f"apres l'apercu de E1 ({d.get('p_preview_e1')}) : ecriture de E2="
        f"{d.get('p_after_e2')}, de E1={d.get('p_after_e1')} (attendu "
        f"crs_ambiguous, comme sans l'apercu) : un clic sur une ligne ferait "
        f"ecrire E2 a une position que rien ne prouve{crash}",
    ))
    rv = d.get("r_review") or {}
    out.append((
        "a_review_date_never_turns_a_refused_write_into_a_write",
        rv.get("n_entities") == 1 and _near((rv.get("drawn") or [None])[0], _R_OLD, 1e-3)
        and (d.get("r_after_e2") or {}).get("status") == "crs_ambiguous",
        f"Review={rv} (B dessinee brute a {_R_OLD} sur son temoin) puis "
        f"ecriture de E2={d.get('r_after_e2')} (attendu crs_ambiguous, comme "
        f"sans la date regardee){crash}",
    ))
    out.append((
        "display_records_no_crs_story",
        d.get("p_stories_changed") == [] and d.get("r_stories_changed") == [],
        f"histoires modifiees par l'apercu={d.get('p_stories_changed')} par "
        f"Review={d.get('r_stories_changed')} (attendu aucune : l'affichage "
        f"lit les histoires, il n'en etablit pas){crash}",
    ))
    rs = d.get("r_write_settles") or {}
    rp = d.get("r_preview_e2_follows") or {}
    out.append((
        "display_still_reads_a_story_a_write_settled",
        rs.get("status") == "relabeled" and rp.get("drawn") is True
        and _near(rp.get("at"), _R_E2, 1e-3),
        f"ecriture qui etablit l'histoire={rs} puis apercu de E2={rp} (attendu "
        f"dessine brut a {_R_E2} : l'affichage suit ce que l'ecriture a "
        f"prouve){crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_display_story_readonly.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
