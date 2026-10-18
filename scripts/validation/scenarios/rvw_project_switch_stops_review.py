"""rvw_project_switch_stops_review - RecoverLand validation runtime.

Invariant: I-RVW-SCOPE (Review decrit le journal et les couches du projet
courant, ou il est arrete ; jamais une interface perimee qui pretend montrer
une reconstruction).

Le mecanisme
------------
Review est concu pour survivre a la fermeture du dialogue (cleanup_resources
le laisse vivre). Au changement de projet, le plugin arrete le backend,
ouvre un nouveau journal, un nouveau tracker, et appelle on_project_switched,
qui remet a zero la recherche et l'annulation mais n'arrete jamais Review.
Le nettoyage du projet retire les calques Review, mais la session reste
active avec des identifiants de couches morts, la barre de dates et la
pastille restent, _review_layer_infos garde les empreintes de l'ancien projet
et committed_features reste connecte au tracker mort.

L'echec concret pour l'utilisateur
----------------------------------
Review actif dans le projet A (la barre affiche 37 entites), dialogue ferme,
l'utilisateur ouvre le projet B. La barre est toujours sur la carte de B ; un
deplacement de carte lui fait afficher 37 entites alors que rien n'est
dessine ; les commits de B ne rafraichissent rien ; bouger la date interroge
le journal de B avec les empreintes de A et affiche "aucune entite".

Le scenario pilote le vrai plugin : Review actif, dialogue ferme (Review
persiste, comme concu), puis ouverture d'un autre projet.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_project_switch_stops_review"
INVARIANT = "I-RVW-SCOPE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_switch_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", [
        ("P1", "Alpha", 1.0, 1.0, 1.0)])
    flog(f"rvw_project_switch_stops_review setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        canvas = iface.mapCanvas()
        receivers0 = canvas.receivers(canvas.extentsChanged)
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
        d["overlay_a"] = h.overlay_rows(dlg)
        # The user closes the dialog: Review persists, by design.
        dlg.close()
        h.pump(0.3)
        d["persists_after_close"] = bool(dlg._review_snap_mode)

        # ---- the user opens another project ----------------------------
        other = os.path.join(d["tmpdir"], "projet_b.qgs")
        QgsProject.instance().clear()
        QgsProject.instance().write(other)
        QgsProject.instance().read(other)
        h.pump(0.6)
        d["snap_mode"] = bool(dlg._review_snap_mode)
        d["review_active"] = bool(dlg._review_active)
        d["date_bar"] = dlg._review_date_bar is not None
        d["session"] = dlg._review_snap_session is not None
        d["layer_infos"] = len(getattr(dlg, "_review_layer_infos", []) or [])
        d["extent_receivers_left"] = canvas.receivers(canvas.extentsChanged) - receivers0
        w = dlg._review_status_widget
        d["pill_visible"] = bool(w is not None and w.isVisible())
        d["managed_layers"] = h.managed_layers()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_project_switch_stops_review run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "setup_review_persists_after_close",
        d.get("review_on") is True and "P7" in (d.get("overlay_a") or {})
        and d.get("persists_after_close") is True,
        f"review_on={d.get('review_on')} calque={d.get('overlay_a')} "
        f"persiste apres fermeture={d.get('persists_after_close')}{crash}",
    ))
    out.append((
        "review_session_ends_with_the_project",
        d.get("snap_mode") is False and d.get("session") is False
        and d.get("review_active") is False,
        f"snap_mode={d.get('snap_mode')} session={d.get('session')} "
        f"auto_refresh={d.get('review_active')} : une session du projet A ne "
        f"doit pas survivre dans le projet B",
    ))
    out.append((
        "no_stale_date_bar_on_the_new_canvas",
        d.get("date_bar") is False and d.get("pill_visible") is False,
        f"barre de dates={d.get('date_bar')} pastille visible="
        f"{d.get('pill_visible')} : elles afficheraient les chiffres du projet A",
    ))
    out.append((
        "no_listener_left_on_the_canvas",
        d.get("extent_receivers_left") == 0,
        f"recepteurs extentsChanged restants={d.get('extent_receivers_left')}",
    ))
    out.append((
        "old_project_scope_forgotten",
        d.get("layer_infos") == 0 and not d.get("managed_layers"),
        f"couches Review memorisees={d.get('layer_infos')} calques Review="
        f"{d.get('managed_layers')} : les empreintes de A ne doivent pas "
        f"interroger le journal de B",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_project_switch_stops_review.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
