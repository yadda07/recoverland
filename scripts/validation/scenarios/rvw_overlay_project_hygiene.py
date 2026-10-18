"""rvw_overlay_project_hygiene - RecoverLand validation runtime.

Invariant: I-RVW-NO-TRACE (Review ne laisse aucune trace dans le projet de
l'utilisateur ; toute edition que l'utilisateur peut faire est soit capturee,
soit refusee, jamais jetee en silence).

Les mecanismes
--------------
1. Ajouter les calques Review salit le projet. Review persistant (voulu), un
   Ctrl+S ou la question "enregistrer ?" a la fermeture de QGIS, qui passe
   AVANT le dechargement des extensions, ecrivait dans le .qgs/.qgz les
   couches memoire "<nom> (snapshot)", leur propriete _rl_snap_managed et le
   groupe "Review Snapshot". Le nettoyage documente (purge_lens_overlays) n'a
   aucun appelant. A la session Review suivante, stop() videait le groupe
   (removeAllChildren) mais ne retirait du projet que SES couches : les
   orphelines perdaient leur noeud mais restaient enregistrees, invisibles,
   et reecrites a chaque sauvegarde. Un groupe utilisateur nomme "Review
   Snapshot" etait vide de la meme facon.
2. Les calques Review n'etaient pas en lecture seule : l'utilisateur pouvait
   les editer, le tracker les ignore volontairement, et le rafraichissement
   suivant (truncate + addFeatures) effacait l'edition sans rien dire.

L'echec concret pour l'utilisateur
----------------------------------
Il lance Review, ferme QGIS et repond "Enregistrer". A la reouverture, le
projet montre des couches "parcelles (snapshot)" vides qui ressemblent a de
vraies donnees ; apres la session Review suivante elles disparaissent de
l'arbre mais restent dans le projet, et dans chaque sauvegarde. Ou bien il
corrige un attribut dans "parcelles (snapshot)" croyant restaurer une
valeur : le deplacement de carte suivant l'efface, sans evenement ni alerte.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_overlay_project_hygiene"
INVARIANT = "I-RVW-NO-TRACE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _file_traces(path: str) -> list:
    if not os.path.isfile(path):
        return ["file missing"]
    text = open(path, encoding="utf-8", errors="replace").read()
    return [t for t in ("__rl_snap_", "_rl_snap_managed", "Review Snapshot",
                        "(snapshot)") if t in text]


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_hygiene_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", [
        ("P1", "Alpha", 1.0, 1.0, 1.0)])
    flog(f"rvw_overlay_project_hygiene setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    project = QgsProject.instance()
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        h.user_commit(layer, lambda lyr: h.add(lyr, "P7", "Eta", 7.0, 7.0, 7.0))
        rows0 = h.wait_rows(jpath, 1)
        time.sleep(1.2)
        cutoff = h.utc_now_iso()
        time.sleep(1.2)
        h.user_commit(layer, lambda lyr: h.rename(lyr, "P7", "Eta2"))
        h.wait_rows(jpath, rows0 + 1)
        h.set_view(iface, 0.0, 0.0, 10.0, 10.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)
        overlays = h.overlay_layers(dlg)
        d["n_overlays"] = len(overlays)

        # ---- 2. the overlay refuses edits ---------------------------------
        if overlays:
            ovl = overlays[0]
            d["overlay_read_only"] = bool(ovl.readOnly())
            d["overlay_edit_started"] = bool(ovl.startEditing())
            if ovl.isEditable():
                ovl.rollBack()

        # ---- 1a. the user saves while Review is on ------------------------
        saved = os.path.join(d["tmpdir"], "enregistre.qgs")
        d["write_ok"] = bool(project.write(saved))
        h.pump(0.3)
        d["file_traces"] = _file_traces(saved)
        d["overlays_back_after_save"] = len(h.overlay_layers(dlg))
        d["overlay_rows_after_save"] = h.overlay_rows(dlg)
        d["dirty_after_save"] = bool(project.isDirty())
        h.review_off(dlg)

        # ---- 1b. a project saved by an older version ----------------------
        # Built exactly as the old session did: a managed memory layer in a
        # "Review Snapshot" group, saved with the project. Plus a group of
        # the USER that happens to carry the same name.
        orphan = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string",
                                "__rl_snap_deadbeef_geom", "memory")
        orphan.setName("cadastre (snapshot)")
        orphan.setCustomProperty("_rl_snap_managed", "1")
        root = project.layerTreeRoot()
        user_layer = QgsVectorLayer("Point?crs=EPSG:2154&field=a:string",
                                    "mes notes", "memory")
        user_group = root.insertGroup(0, "Review Snapshot")
        project.addMapLayer(user_layer, False)
        user_group.insertLayer(0, user_layer)
        old_group = root.insertGroup(1, "Review Snapshot")
        project.addMapLayer(orphan, False)
        old_group.insertLayer(0, orphan)
        legacy = os.path.join(d["tmpdir"], "ancien.qgs")
        project.write(legacy)
        project.clear()
        h.pump(0.3)
        project.read(legacy)
        h.pump(0.5)
        layer = project.mapLayersByName("cadastre")[0]
        d["orphans_after_open"] = len(h.managed_layers())
        dlg = h.open_dialog(plugin, [layer])
        d["review_on_2"] = h.review_on(dlg, 1)
        h.review_off(dlg)
        h.pump(0.3)
        d["managed_after_stop"] = h.managed_layers()
        d["user_layer_still_in_tree"] = bool(
            [n for n in project.layerTreeRoot().findLayers()
             if n.layer() is not None and n.layer().name() == "mes notes"])
        again = os.path.join(d["tmpdir"], "reenregistre.qgs")
        project.write(again)
        d["file_traces_after_review"] = [
            t for t in _file_traces(again) if t != "Review Snapshot"]
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_overlay_project_hygiene run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "setup_review_with_overlays",
        d.get("review_on") is True and (d.get("n_overlays") or 0) >= 1,
        f"review_on={d.get('review_on')} calques={d.get('n_overlays')}{crash}",
    ))
    out.append((
        "overlay_refuses_user_edits",
        d.get("overlay_read_only") is True and d.get("overlay_edit_started") is False,
        f"lecture seule={d.get('overlay_read_only')} startEditing accepte="
        f"{d.get('overlay_edit_started')} : une edition ici serait effacee au "
        f"prochain rafraichissement sans evenement ni alerte",
    ))
    out.append((
        "saved_project_holds_no_overlay",
        d.get("write_ok") is True and d.get("file_traces") == [],
        f"traces Review dans le fichier enregistre={d.get('file_traces')} : a "
        f"la reouverture elles passeraient pour de vraies couches",
    ))
    out.append((
        "review_continues_after_the_save",
        (d.get("overlays_back_after_save") or 0) >= 1
        and "P7" in (d.get("overlay_rows_after_save") or {})
        and d.get("dirty_after_save") is False,
        f"calques apres sauvegarde={d.get('overlays_back_after_save')} "
        f"contenu={d.get('overlay_rows_after_save')} projet sale="
        f"{d.get('dirty_after_save')} : la sauvegarde ne doit ni casser Review "
        f"ni rearmer la question d'enregistrement",
    ))
    out.append((
        "legacy_orphans_are_purged",
        (d.get("orphans_after_open") or 0) >= 1 and d.get("managed_after_stop") == [],
        f"orphelines a l'ouverture={d.get('orphans_after_open')} restantes apres "
        f"Review={d.get('managed_after_stop')} : sinon elles restent enregistrees, "
        f"invisibles, et reecrites a chaque sauvegarde",
    ))
    out.append((
        "user_group_of_the_same_name_is_not_emptied",
        d.get("user_layer_still_in_tree") is True,
        f"couche utilisateur encore dans l'arbre={d.get('user_layer_still_in_tree')}",
    ))
    out.append((
        "no_overlay_trace_saved_after_review",
        d.get("file_traces_after_review") == [],
        f"traces dans le projet reenregistre={d.get('file_traces_after_review')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_overlay_project_hygiene.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
