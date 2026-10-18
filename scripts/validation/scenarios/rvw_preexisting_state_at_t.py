"""rvw_preexisting_state_at_t - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat des donnees a la date T, ou
dit explicitement ce qu'il ne peut pas montrer ; il ne presente jamais
l'etat actuel, ni une absence, comme l'etat a T).

Le mecanisme
------------
Le journal ne commence qu'au demarrage du suivi : toutes les entites qui
existaient avant n'ont AUCUN evenement INSERT. Le worker de Review ne lisait
que les evenements ``created_at <= T``. Pour une entite dont le premier
evenement capture est un UPDATE ou un DELETE posterieur a T, la liste des
evenements visibles etait vide et le moteur la comptait comme "nee apres T"
(n_unknown), puis le filtre "modifiee apres T" la jetait. Or le DELETE porte
l'image complete de l'entite (all_attributes + geometry_wkb) et l'UPDATE porte
les anciennes valeurs et l'ancienne geometrie : l'etat a T etait dans le
journal, jamais lu. S'y ajoutait un plancher de date : le curseur etait
ramene au PREMIER evenement du journal, inclusivement, donc la premiere
modification apparaissait comme deja faite a une date anterieure.

L'echec concret pour l'utilisateur
----------------------------------
Couche de parcelles existante, suivi active. Apres T, l'utilisateur supprime
P1, deplace P2, renomme P3 et cree P6. Review a la date T : les calques Review
restent vides, la carte montre P2 a sa NOUVELLE place, pas de P1, et la barre
affiche le symbole vide "aucune entite a cette date". L'utilisateur conclut
que P1 n'existait pas a T et que P2 etait deja deplace : c'est faux.

Ce que le scenario pilote : le vrai plugin, un vrai GeoPackage, de vraies
editions capturees par le tracker, le vrai worker (execution synchrone) puis
le vrai dialogue en mode Review.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_preexisting_state_at_t"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS = [
    ("P1", "Alpha", 100.0, 1.0, 1.0),
    ("P2", "Beta", 200.0, 2.0, 2.0),
    ("P3", "Gamma", 300.0, 3.0, 3.0),
    ("P4", "Delta", 400.0, 4.0, 4.0),
    ("P5", "Epsilon", 500.0, 5.0, 5.0),
]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _point_of(wkb):
    from recoverland.core.geometry_utils import rebuild_geometry
    g = rebuild_geometry(wkb) if wkb else None
    if g is None or g.isNull():
        return None
    p = g.asPoint()
    return (round(p.x(), 6), round(p.y(), 6))


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_preexist_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", _ROWS)
    flog(f"rvw_preexisting_state_at_t setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.identity import (
        compute_datasource_fingerprint, compute_entity_fingerprint,
        compute_feature_identity,
    )
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        keys = {}
        for f in layer.getFeatures():
            keys[f["code"]] = compute_entity_fingerprint(
                compute_feature_identity(layer, f))
        d["keys"] = keys
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)

        # T: the state every feature was in, before any captured edit.
        cutoff = h.utc_now_iso()
        d["cutoff"] = cutoff
        time.sleep(1.2)
        d["commits"] = [
            h.user_commit(layer, lambda lyr: h.delete(lyr, "P1")),
            h.user_commit(layer, lambda lyr: h.move(lyr, "P2", 20.0, 20.0)),
            h.user_commit(layer, lambda lyr: h.rename(lyr, "P3", "Gamma2")),
            h.user_commit(layer, lambda lyr: h.add(lyr, "P6", "Zeta", 600.0, 6.0, 6.0)),
        ]
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 4) - rows0

        # ---- worker, run synchronously ---------------------------------
        import recoverland.widgets.snapshot_rebuild_worker as wmod
        info = {"fingerprint": compute_datasource_fingerprint(layer),
                "layer_name": layer.name(), "layer_id": layer.id(),
                "storage_crs": "EPSG:2154"}
        captured = {}
        worker = wmod.SnapshotRebuildWorker(plugin._journal, [info], cutoff,
                                            trace_id=ctx.trace_id)
        worker.result_ready.connect(lambda t, r: captured.__setitem__("result", r))
        worker.error.connect(lambda t, e: captured.__setitem__("error", e))
        worker.run()
        res = captured.get("result")
        d["worker_error"] = captured.get("error")
        feats = res.features.get(info["fingerprint"], {}) if res is not None else {}
        d["worker"] = {
            "n_entities": getattr(res, "n_entities", None),
            "n_born_after": getattr(res, "n_born_after", 0),
            "present": sorted(code for code, k in keys.items() if k in feats),
            "P1_point": _point_of(feats[keys["P1"]].geom_wkb) if keys["P1"] in feats else None,
            "P2_point": _point_of(feats[keys["P2"]].geom_wkb) if keys["P2"] in feats else None,
            "P1_attrs": (json.loads(feats[keys["P1"]].attrs_json or "{}")
                         if keys["P1"] in feats else {}),
        }

        # ---- the dialog, as the user drives it -------------------------
        h.set_view(iface, 0.0, 0.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)
        d["overlay"] = h.overlay_rows(dlg)
        bar = dlg._review_date_bar
        d["bar_text"] = bar._lbl_status.text() if bar is not None else None
        d["bar_tip"] = bar._lbl_status.toolTip() if bar is not None else None
        d["source_rows_now"] = sorted(f["code"] for f in layer.getFeatures())
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_preexisting_state_at_t run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    w = d.get("worker") or {}
    ov = d.get("overlay") or {}
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "setup_real_edits_captured",
        d.get("commits") == [True, True, True, True] and d.get("journal_rows", 0) >= 4,
        f"commits={d.get('commits')} lignes journal={d.get('journal_rows')}{crash}",
    ))
    out.append((
        "worker_deleted_after_t_is_present_at_t",
        w.get("P1_point") == (1.0, 1.0) and w.get("P1_attrs", {}).get("nom") == "Alpha",
        f"P1 supprimee apres T : point a T={w.get('P1_point')} attributs="
        f"{w.get('P1_attrs')} (attendu (1,1), nom=Alpha) ; sinon l'utilisateur "
        f"lit que P1 n'existait pas a T. erreur worker={d.get('worker_error')}",
    ))
    out.append((
        "worker_moved_after_t_has_its_old_geometry",
        w.get("P2_point") == (2.0, 2.0),
        f"P2 deplacee apres T : point a T={w.get('P2_point')} (attendu (2,2)) ; "
        f"sinon Review montre P2 deja a sa nouvelle place",
    ))
    out.append((
        "worker_counts_the_changed_preexisting_entities",
        w.get("present") == ["P1", "P2", "P3"] and w.get("n_entities") == 3,
        f"entites reconstituees={w.get('present')} n_entities={w.get('n_entities')} "
        f"(attendu P1,P2,P3 : les trois qui different de l'etat actuel)",
    ))
    out.append((
        "worker_counts_born_after_t",
        (w.get("n_born_after") or 0) >= 1,
        f"n_born_after={w.get('n_born_after')} : P6, creee apres T, doit etre "
        f"comptee (la couche actuelle l'affiche alors qu'elle n'existait pas)",
    ))
    p1 = ov.get("P1") or {}
    out.append((
        "overlay_shows_the_deleted_entity_at_t",
        p1.get("x") == 1.0 and p1.get("y") == 1.0 and p1.get("nom") == "Alpha"
        and p1.get("surface") == 100.0,
        f"calque Review, P1={p1 or 'ABSENTE'} (attendu (1,1) Alpha 100) ; "
        f"review_on={d.get('review_on')} review_date={d.get('review_date')}{crash}",
    ))
    p2 = ov.get("P2") or {}
    out.append((
        "overlay_moved_entity_at_old_place_with_all_attributes",
        p2.get("x") == 2.0 and p2.get("y") == 2.0 and p2.get("nom") == "Beta"
        and p2.get("surface") == 200.0,
        f"calque Review, P2={p2 or 'ABSENTE'} (attendu (2,2) Beta 200 : la "
        f"surface n'a jamais change, elle vaut la valeur actuelle, pas NULL)",
    ))
    p3 = ov.get("P3") or {}
    out.append((
        "overlay_attribute_only_edit_old_value_current_geometry",
        p3.get("x") == 3.0 and p3.get("y") == 3.0 and p3.get("nom") == "Gamma",
        f"calque Review, P3={p3 or 'ABSENTE'} (attendu (3,3) nom=Gamma : sa "
        f"geometrie n'est dans aucun evenement, c'est la geometrie actuelle)",
    ))
    out.append((
        "overlay_excludes_unchanged_and_born_after",
        not ({"P4", "P5", "P6"} & set(ov)),
        f"entites du calque Review={sorted(ov)} : P4/P5 inchangees (identiques "
        f"aux couches actuelles) et P6 nee apres T ne doivent pas y etre",
    ))
    out.append((
        "bar_is_not_the_empty_answer",
        d.get("bar_text") not in (None, "∅"),
        f"barre={d.get('bar_text')!r} infobulle={d.get('bar_tip')!r} : trois "
        f"entites different a T, le symbole vide serait un mensonge",
    ))
    out.append((
        "bar_says_what_the_overlay_cannot_hide",
        "après cette date" in (d.get("bar_tip") or ""),
        f"infobulle={d.get('bar_tip')!r} : P6 creee apres T reste affichee par "
        f"la couche actuelle, la barre doit le dire",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_preexisting_state_at_t.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
