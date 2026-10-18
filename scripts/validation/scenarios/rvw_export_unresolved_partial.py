"""rvw_export_unresolved_partial - RecoverLand validation runtime.

Invariant: I-NO-FALSE-SUCCESS (un fichier "etat a T" ne presente jamais un
etat incomplet comme complet ; un NULL n'y vaut jamais "inconnu" sans le
dire ; chaque omission est nommee par sa cause).

Le mecanisme
------------
L'export ecrit l'etat a T des entites modifiees depuis T. Une entite dont
l'etat a T n'est connu qu'en partie (``SnapshotFeature.unresolved`` :
identity_not_proven, live_feature_not_found, attributes_incomplete...) mais
dont la geometrie est connue etait ecrite comme les autres :
_set_source_attrs laisse NULL chaque champ dont la valeur a T est inconnue,
puis le dialogue affichait QMessageBox.information "Export termine" et la
comptait parmi les "entites ecrites dans leur etat a T". Seule la table
recoverland_etat_info mentionnait entites_sans_etat_complet. De plus toutes
les omissions etaient mises sur le compte de "sans geometrie
reconstituable", y compris une couche source absente du projet ou une
geometrie d'un autre type que la couche. Enfin le refus d'export n'etait
evalue qu'AVANT la boite "Enregistrer sous" : une validation de la couche
arrivant pendant qu'elle est ouverte videait la reconstruction, et l'export
tombait sur un AttributeError (trace Python) au lieu d'un refus nomme.

L'echec concret pour l'utilisateur
----------------------------------
Couche Shapefile ; apres T, B deplacee, C renommee, A supprimee (le fichier
est recompacte). Export de l'etat a T : "Export termine", B ecrite avec des
champs vides (lus plus tard comme "NULL a cette date"), sans rien qui
distingue une valeur inconnue d'une valeur nulle.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rvw_export_unresolved_partial"
INVARIANT = "I-NO-FALSE-SUCCESS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FLAG = "_rl_etat_incomplet"


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _fx():
    from scripts.validation.scenarios import _rvw_unstable_fid
    return _rvw_unstable_fid


def _read_gpkg(path: str) -> dict:
    from qgis.core import QgsVectorLayer
    out = {"layers": {}, "info": {}, "fields": {}}
    if not os.path.isfile(path):
        return out
    probe = QgsVectorLayer(path, "probe", "ogr")
    names = [s.split("!!::!!")[1] for s in probe.dataProvider().subLayers()] \
        if probe.isValid() else []
    for name in names:
        lyr = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
        if not lyr.isValid():
            continue
        if name == "recoverland_etat_info":
            for f in lyr.getFeatures():
                out["info"][str(f["cle"])] = str(f["valeur"])
            continue
        fields = lyr.fields().names()
        out["fields"][name] = fields
        rows = []
        for f in lyr.getFeatures():
            g = f.geometry()
            pt = g.asPoint() if g is not None and not g.isNull() and not g.isEmpty() else None
            flag = f[_FLAG] if _FLAG in fields else "<absent>"
            rows.append({
                "code": None if _h().is_null(f["code"]) else str(f["code"]),
                "nom": None if _h().is_null(f["nom"]) else str(f["nom"]),
                "x": pt.x() if pt else None, "y": pt.y() if pt else None,
                "flag": None if _h().is_null(flag) else str(flag),
            })
        out["layers"][name] = rows
    return out


def _crafted(raw, layer_fp):
    """The same reconstruction plus a line in a point layer and an entity
    of a layer that is not in the project any more."""
    from qgis.core import QgsGeometry
    from recoverland.core.temporal_snapshot_engine import SnapshotFeature
    line = bytes(QgsGeometry.fromWkt("LineString (0 0, 1 1)").asWkb())
    point = bytes(QgsGeometry.fromWkt("Point (5 5)").asWkb())
    feats = {fp: dict(ents) for fp, ents in raw.features.items()}
    feats.setdefault(layer_fp, {})["fid:99"] = SnapshotFeature(
        entity_fp="fid:99", geom_wkb=line, attrs_json=json.dumps({"code": "L"}),
        crs_authid="EPSG:2154", last_event_id=0, last_op="",
        last_created_at="", origin="before_image")
    feats["ds_rvw_layer_gone"] = {"fid:1": SnapshotFeature(
        entity_fp="fid:1", geom_wkb=point, attrs_json=json.dumps({"code": "G"}),
        crs_authid="EPSG:2154", last_event_id=0, last_op="",
        last_created_at="", origin="before_image")}
    n = sum(len(v) for v in feats.values())
    return raw._replace(features=feats, n_entities=n)


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_expu_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["shp"] = _fx().make_shp(tmpdir)
    flog(f"rvw_export_unresolved_partial setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from qgis.PyQt import QtWidgets
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    h = _h()
    fx = _fx()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    saved_dialog = QtWidgets.QFileDialog.getSaveFileName
    try:
        layer = fx.load_shp(d["shp"])
        h.pump(0.2)
        cutoff = fx.edit_after_cutoff(plugin, layer, d)
        h.set_view(iface, -5.0, -5.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)

        # ---- 1. the export the user asks for -----------------------------
        out_path = os.path.join(d["tmpdir"], "etat.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (out_path, "GeoPackage (*.gpkg)"))
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        d["calls"] = boxes.calls
        d["file"] = _read_gpkg(out_path)

        # ---- 2. a type mismatch and a vanished source layer ---------------
        raw = dlg._review_snap_raw_result
        fp = compute_datasource_fingerprint(layer)
        dlg._review_snap_raw_result = _crafted(raw, fp)
        crafted_path = os.path.join(d["tmpdir"], "etat_crafted.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (crafted_path, "GeoPackage (*.gpkg)"))
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        d["crafted_calls"] = boxes.calls
        dlg._review_snap_raw_result = raw

        # ---- 3. a commit lands while "Save as" is open -------------------
        late_path = os.path.join(d["tmpdir"], "etat_late.gpkg")

        def _dialog_with_commit(*_a, **_k):
            dlg._review_on_commit_changed(1)
            return late_path, "GeoPackage (*.gpkg)"
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(_dialog_with_commit)
        with h.MessageBoxRecorder() as boxes:
            try:
                dlg._on_snapshot_export_requested()
                d["late_exception"] = None
            except Exception as exc:  # noqa: BLE001 - what the user would get
                d["late_exception"] = f"{type(exc).__name__}: {exc}"
        d["late_calls"] = boxes.calls
        d["late_written"] = os.path.isfile(late_path)
        h.pump_until(lambda: h.review_idle(dlg), 10.0)
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        QtWidgets.QFileDialog.getSaveFileName = saved_dialog
        h.shutdown(plugin)
    flog(f"rvw_export_unresolved_partial run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    calls = d.get("calls") or []
    kinds = [k for k, _t in calls]
    text = " ".join(t for _k, t in calls)
    f = d.get("file") or {}
    rows = [r for rs in (f.get("layers") or {}).values() for r in rs]
    fields = [n for ns in (f.get("fields") or {}).values() for n in ns]
    by_xy = {(r["x"], r["y"]): r for r in rows}
    b = by_xy.get((1.0, 1.0)) or {}
    a = by_xy.get((0.0, 0.0)) or {}
    out = []
    out.append((
        "setup_real_edits_repack_and_review",
        d.get("commits") == [True, True, True] and _fx().fids_moved(d)
        and d.get("review_on") is True and d.get("review_date") is True,
        f"commits={d.get('commits')} fid avant={d.get('fids_before')} apres="
        f"{d.get('fids_after')} review_on={d.get('review_on')}{crash}",
    ))
    out.append((
        "incomplete_export_is_announced_partial_not_done",
        kinds == ["warning"] and "partiel" in text.lower()
        and "entièrement connu" in text,
        f"messages={calls} : des entites dont l'etat a T n'est pas entierement "
        f"connu sont dans le fichier, 'Export termine' serait un faux succes{crash}",
    ))
    out.append((
        "every_written_incomplete_entity_is_flagged_in_the_file",
        _FLAG in fields and bool(b) and b.get("flag") and a and a.get("flag") is None,
        f"colonnes={sorted(set(fields))} B={b or 'ABSENTE'} A={a or 'ABSENTE'} : "
        f"B (identite non prouvee, attributs inconnus) doit porter {_FLAG} ; "
        f"A (etat complet) non : sinon un champ vide se lit 'NULL a cette date'",
    ))
    out.append((
        "file_holds_no_foreign_values",
        b.get("code") in (None, "B") and not any(r["code"] == "D" for r in rows)
        and not any(r["code"] == "C" and r["x"] == 3.0 for r in rows),
        f"lignes du fichier={rows} : aucune ne doit porter les valeurs d'une "
        f"autre entite",
    ))
    out.append((
        "unknown_geometry_omission_named_as_such",
        "géométrie à cette date est inconnue" in text,
        f"message={text!r} : C (identite non prouvee, geometrie jamais "
        f"journalisee) n'est pas ecrite, et la cause doit etre dite",
    ))
    ctext = " ".join(t for _k, t in (d.get("crafted_calls") or []))
    out.append((
        "type_mismatch_and_missing_layer_named_separately",
        "type" in ctext and "couche source absente" in ctext,
        f"message={ctext!r} : une ligne dans une couche de points et une "
        f"couche retiree du projet ne sont pas 'sans geometrie reconstituable'",
    ))
    late_kinds = [k for k, _t in (d.get("late_calls") or [])]
    out.append((
        "commit_during_save_dialog_is_a_named_refusal",
        d.get("late_exception") is None and late_kinds == ["warning"]
        and d.get("late_written") is False,
        f"exception={d.get('late_exception')} messages={d.get('late_calls')} "
        f"fichier ecrit={d.get('late_written')} : une validation pendant la "
        f"boite 'Enregistrer sous' doit donner un refus nomme, pas une trace "
        f"Python ni un fichier d'un autre etat",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_export_unresolved_partial.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
