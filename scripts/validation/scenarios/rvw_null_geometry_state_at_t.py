# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_null_geometry_state_at_t - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat des donnees a la date T, y
compris l'ABSENCE de geometrie d'une entite a T ; il ne la presente ni
comme une corruption, ni comme un etat inconnu, ni avec la geometrie de
l'autre cote de la modification).

Le mecanisme
------------
La capture enregistre desormais une geometrie qui apparait ou disparait
avec un marqueur explicite, ``GEOMETRY_NULL_MARKER`` (b"", BLOB de
longueur nulle) ; SQL NULL veut toujours dire "geometrie inchangee". Sur un
UPDATE, ``new_geometry_wkb == b""`` dit que l'entite n'avait plus de
geometrie apres la modification, ``geometry_wkb == b""`` qu'elle n'en avait
pas avant. Le moteur de Review lisait bien ce b"" comme la geometrie a T,
mais la session des calques le traitait comme un blob illisible : compte
comme geometrie corrompue dans le journal de bord, entite ecartee des
calques Review et de l'export, et l'export annoncait "N entite(s) sans
geometrie reconstituable n'ont pas ete ecrites". Une entite sans geometrie
a T que rien n'a touchee depuis (geometrie lue sur la couche actuelle,
nulle) etait de plus declaree "etat incomplet".

L'echec concret pour l'utilisateur
----------------------------------
Couche de points importee avec des lignes sans geometrie, numerisees
ensuite. Review a une date T anterieure a la numerisation : les entites qui
n'avaient pas de geometrie a T n'apparaissent pas dans la table des calques
Review (leurs attributs a T sont introuvables), la barre les compte pourtant,
un message "etat incomplet" s'affiche, et l'export les dit illisibles alors
que leur etat a T (pas de geometrie, ces attributs) est parfaitement connu.

Cas (entites preexistantes au journal, sauf mention)
----------------------------------------------------
N1  point (1,1), geometrie SUPPRIMEE apres T : a T, le point (1,1).
N2  sans geometrie, geometrie AJOUTEE apres T en (2,2) : a T, aucune forme
    (et surtout pas (2,2)), attributs presents.
N3  point (3,3), geometrie supprimee AVANT T, renommee apres T : a T, aucune
    forme, nom d'avant le renommage.
N4  sans geometrie, geometrie ajoutee avant T en (4,4), deplacee apres T :
    a T, (4,4) (cote "apres" du marqueur, pas un repli sur l'autre cote).
N5  point (5,5), geometrie supprimee avant T, remise en (50,50) apres T : a
    T, aucune forme.
N6  sans geometrie, seulement renommee apres T : a T, aucune forme (la
    geometrie actuelle, nulle, n'a jamais change), etat complet.
N7  point (7,7), jamais modifiee : absente des calques Review.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_null_geometry_state_at_t"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS = [
    ("N1", "Un", 1.0, 1.0, 1.0),
    ("N2", "Deux", 2.0, None, None),
    ("N3", "Trois", 3.0, 3.0, 3.0),
    ("N4", "Quatre", 4.0, None, None),
    ("N5", "Cinq", 5.0, 5.0, 5.0),
    ("N6", "Six", 6.0, None, None),
    ("N7", "Sept", 7.0, 7.0, 7.0),
]
_NO_SHAPE = ("N2", "N3", "N5", "N6")


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _geom_state(wkb):
    """"null" (b"": no geometry at T), "unknown" (None), or (x, y)."""
    if wkb is None:
        return "unknown"
    if len(wkb) == 0:
        return "null"
    from recoverland.core.geometry_utils import rebuild_geometry
    g = rebuild_geometry(wkb)
    if g is None or g.isNull():
        return "corrupt"
    p = g.asPoint()
    return (round(p.x(), 6), round(p.y(), 6))


def _read_gpkg(path: str) -> dict:
    """{code: {"nom", "x", "y", "has_geom"}} over every data layer, + info."""
    from qgis.core import QgsVectorLayer
    out = {"rows": {}, "info": {}}
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
        for f in lyr.getFeatures():
            g = f.geometry()
            null = g is None or g.isNull()
            has = not null and not g.isEmpty()
            pt = g.asPoint() if has else None
            out["rows"][str(f["code"])] = {
                "nom": str(f["nom"]), "has_geom": has, "null": null,
                "x": pt.x() if pt else None, "y": pt.y() if pt else None,
            }
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_nullgeom_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "reseau", _ROWS)
    flog(f"rvw_null_geometry_state_at_t setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _before_t(lyr):
    h = _h()
    h.clear_geometry(lyr, "N3")
    h.move(lyr, "N4", 4.0, 4.0)
    h.clear_geometry(lyr, "N5")


def _after_t(lyr):
    h = _h()
    h.clear_geometry(lyr, "N1")
    h.move(lyr, "N2", 2.0, 2.0)
    h.rename(lyr, "N3", "Trois2")
    h.move(lyr, "N4", 40.0, 40.0)
    h.move(lyr, "N5", 50.0, 50.0)
    h.rename(lyr, "N6", "Six2")


def _worker_view(plugin, layer, cutoff, keys, trace_id) -> dict:
    """The real worker, run synchronously, then the live completion."""
    from recoverland.core.identity import compute_datasource_fingerprint
    import recoverland.widgets.snapshot_rebuild_worker as wmod
    info = {"fingerprint": compute_datasource_fingerprint(layer),
            "layer_name": layer.name(), "layer_id": layer.id(),
            "storage_crs": "EPSG:2154"}
    captured = {}
    worker = wmod.SnapshotRebuildWorker(plugin._journal, [info], cutoff,
                                        trace_id=trace_id)
    worker.result_ready.connect(lambda t, r: captured.__setitem__("result", r))
    worker.error.connect(lambda t, e: captured.__setitem__("error", e))
    worker.run()
    res = captured.get("result")
    try:
        from recoverland.core.snapshot_overlay_session import complete_from_live
        res = complete_from_live(res, [info], trace_id)
    except ImportError:
        pass  # a version without live completion: the raw result is its answer
    feats = res.features.get(info["fingerprint"], {}) if res is not None else {}
    out = {"error": captured.get("error"),
           "n_unresolved": getattr(res, "n_unresolved", None),
           "geom": {}, "nom": {}, "unresolved": {}}
    for code, key in keys.items():
        sf = feats.get(key)
        if sf is None:
            continue
        out["geom"][code] = _geom_state(sf.geom_wkb)
        out["nom"][code] = json.loads(sf.attrs_json or "{}").get("nom")
        out["unresolved"][code] = getattr(sf, "unresolved", "")
    return out


def run(ctx):
    from qgis.PyQt import QtWidgets
    from recoverland.core.identity import (
        compute_entity_fingerprint, compute_feature_identity,
    )
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    saved_dialog = QtWidgets.QFileDialog.getSaveFileName
    try:
        layer = h.load_layer(d["gpkg"], "reseau")
        h.pump(0.2)
        d["seed_no_geom"] = sorted(
            f["code"] for f in layer.getFeatures()
            if f.geometry() is None or f.geometry().isNull())
        keys = {f["code"]: compute_entity_fingerprint(compute_feature_identity(layer, f))
                for f in layer.getFeatures()}
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)

        d["commits"] = [h.user_commit(layer, _before_t)]
        time.sleep(1.2)
        # T: after the geometry edits of N3/N4/N5, before everything else.
        cutoff = h.utc_now_iso()
        d["cutoff"] = cutoff
        time.sleep(1.2)
        d["commits"].append(h.user_commit(layer, _after_t))
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 9) - rows0
        d["live_after"] = {
            f["code"]: (None if f.geometry() is None or f.geometry().isNull()
                        else (f.geometry().asPoint().x(), f.geometry().asPoint().y()))
            for f in layer.getFeatures()}

        d["worker"] = _worker_view(plugin, layer, cutoff, keys, ctx.trace_id)

        # ---- the dialog, as the user drives it -------------------------
        h.set_view(iface, -5.0, -5.0, 60.0, 60.0)
        dlg = h.open_dialog(plugin, [layer])
        n_msgs = len(iface.bar.messages)
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)
        d["overlay"] = h.overlay_rows(dlg)
        bar = dlg._review_date_bar
        d["bar_text"] = bar._lbl_status.text() if bar is not None else None
        d["bar_tip"] = bar._lbl_status.toolTip() if bar is not None else None
        d["new_messages"] = [m[:2] for m in iface.bar.messages[n_msgs:]]

        # ---- the export of the state at T --------------------------------
        out_path = os.path.join(d["tmpdir"], "etat.gpkg")
        QtWidgets.QFileDialog.getSaveFileName = staticmethod(
            lambda *a, **k: (out_path, "GeoPackage (*.gpkg)"))
        with h.MessageBoxRecorder() as boxes:
            dlg._on_snapshot_export_requested()
        d["export_messages"] = boxes.texts()
        d["file"] = _read_gpkg(out_path)
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        QtWidgets.QFileDialog.getSaveFileName = saved_dialog
        h.shutdown(plugin)
    flog(f"rvw_null_geometry_state_at_t run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    w = d.get("worker") or {}
    wg = w.get("geom") or {}
    ov = d.get("overlay") or {}
    f = d.get("file") or {}
    exported = f.get("rows") or {}
    info = f.get("info") or {}
    tip = d.get("bar_tip") or ""
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    live = d.get("live_after") or {}
    out = []
    out.append((
        "setup_real_null_transitions_captured",
        d.get("commits") == [True, True] and d.get("journal_rows", 0) >= 9
        and d.get("seed_no_geom") == ["N2", "N4", "N6"]
        and live.get("N1") is None and live.get("N2") == (2.0, 2.0)
        and live.get("N3") is None and live.get("N5") == (50.0, 50.0),
        f"commits={d.get('commits')} lignes journal={d.get('journal_rows')} "
        f"sans geometrie au depart={d.get('seed_no_geom')} etat actuel={live}{crash}",
    ))
    out.append((
        "worker_removed_after_t_has_its_geometry_at_t",
        wg.get("N1") == (1.0, 1.0),
        f"N1 (geometrie supprimee apres T) a T={wg.get('N1')} (attendu (1,1)) ; "
        f"sinon Review dit que N1 n'avait pas de forme a T. erreur={w.get('error')}",
    ))
    out.append((
        "worker_added_after_t_has_no_geometry_at_t",
        wg.get("N2") == "null" and not (w.get("unresolved") or {}).get("N2"),
        f"N2 (geometrie ajoutee apres T) a T={wg.get('N2')} non resolue="
        f"{(w.get('unresolved') or {}).get('N2')!r} (attendu 'null' : aucune "
        f"geometrie, etat connu, jamais la geometrie numerisee depuis)",
    ))
    out.append((
        "worker_marker_sides_are_read_not_swapped",
        wg.get("N3") == "null" and wg.get("N4") == (4.0, 4.0) and wg.get("N5") == "null",
        f"a T : N3={wg.get('N3')} (attendu null) N4={wg.get('N4')} (attendu (4,4)) "
        f"N5={wg.get('N5')} (attendu null) : b\"\" d'un cote n'est jamais un "
        f"repli vers l'autre cote",
    ))
    out.append((
        "worker_untouched_null_geometry_is_known",
        wg.get("N6") == "null" and not (w.get("unresolved") or {}).get("N6")
        and w.get("n_unresolved") == 0,
        f"N6 (sans geometrie, jamais touchee en geometrie) a T={wg.get('N6')} "
        f"non resolue={(w.get('unresolved') or {}).get('N6')!r} "
        f"n_unresolved={w.get('n_unresolved')} : une geometrie actuelle nulle "
        f"qui n'a jamais change est l'etat a T, pas un etat inconnu",
    ))
    n1 = ov.get("N1") or {}
    n4 = ov.get("N4") or {}
    out.append((
        "overlay_draws_the_shapes_at_t",
        n1.get("x") == 1.0 and n1.get("y") == 1.0 and n1.get("nom") == "Un"
        and n4.get("x") == 4.0 and n4.get("y") == 4.0,
        f"calque Review N1={n1 or 'ABSENTE'} (attendu (1,1) Un) N4={n4 or 'ABSENTE'} "
        f"(attendu (4,4)) ; review_on={d.get('review_on')} "
        f"review_date={d.get('review_date')}{crash}",
    ))
    shapeless = {c: ov.get(c) for c in _NO_SHAPE}
    out.append((
        "overlay_lists_shapeless_entities_with_their_attributes_at_t",
        all(r is not None and r.get("x") is None for r in shapeless.values())
        and (ov.get("N2") or {}).get("nom") == "Deux"
        and (ov.get("N3") or {}).get("nom") == "Trois"
        and (ov.get("N6") or {}).get("nom") == "Six",
        f"calque Review={shapeless} (attendu N2 Deux, N3 Trois, N5, N6 Six, "
        f"presentes SANS forme) : leurs attributs a T doivent etre lisibles, "
        f"et aucune ne doit porter la geometrie recue depuis",
    ))
    out.append((
        "overlay_excludes_the_unchanged_entity",
        "N7" not in ov,
        f"entites du calque Review={sorted(ov)} : N7, jamais modifiee, est "
        f"identique a la couche actuelle",
    ))
    out.append((
        "no_false_incomplete_state_alarm",
        not any("incomplet" in (t + m).lower() for t, m in d.get("new_messages") or [])
        and "entièrement connu" not in tip,
        f"messages={d.get('new_messages')} infobulle={tip!r} : l'absence de "
        f"geometrie a T est un etat connu, pas un etat incomplet",
    ))
    out.append((
        "bar_says_which_entities_had_no_geometry",
        "sans géométrie à cette date" in tip and d.get("bar_text") not in (None, "∅"),
        f"barre={d.get('bar_text')!r} infobulle={tip!r} : la couche actuelle "
        f"affiche la geometrie recue depuis par N2/N5, la barre doit dire que "
        f"ces entites n'en avaient pas a T",
    ))
    e1 = exported.get("N1") or {}
    out.append((
        "export_writes_shapeless_entities_as_such",
        e1.get("x") == 1.0 and all(
            (exported.get(c) or {}).get("null") is True for c in _NO_SHAPE)
        and set(_NO_SHAPE) <= set(exported),
        f"fichier={exported} : N2/N3/N5/N6 doivent y etre, geometrie NULLE (ni "
        f"absentes, ni une geometrie vide 'POINT EMPTY', autre etat qu'une "
        f"reimportation porterait) ; messages={d.get('export_messages')}",
    ))
    msg = " ".join(d.get("export_messages") or [])
    out.append((
        "export_does_not_call_them_unreadable",
        "reconstituable" not in msg
        and info.get("entites_ecrites") == info.get("entites_reconstituees")
        and info.get("entites_ecrites") not in (None, "0"),
        f"message={msg!r} table={info} : une entite sans geometrie a T est "
        f"ecrite telle quelle, jamais annoncee comme illisible ou non ecrite",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_null_geometry_state_at_t.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
