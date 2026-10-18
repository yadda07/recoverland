"""rvw_unstable_fid_live_identity - RecoverLand validation runtime.

Invariant: I-RVW-ASOF + ne jamais attribuer a une entite l'etat d'une
autre (une identite non prouvee est nommee, jamais devinee).

Le mecanisme
------------
Ce qu'aucun evenement ne donne de l'etat a T d'une entite preexistante (sa
geometrie, un champ jamais modifie depuis) est lu sur l'entite ACTUELLE par
snapshot_overlay_session.complete_from_live -> _fetch_live. Pour une cle
``fid:N``, _fetch_live lit le fid N puis "verifie" que l'identite de ce
qu'il a lu vaut ``fid:N`` : c'est circulaire, le fid lu redonne toujours
``fid:N``. Le seul vrai filtre etait la force d'identite, et MEDIUM y
laisse passer Shapefile et GeoJSON, justement les formats dont les fid
bougent apres un recompactage (core.commit_guard.fid_is_stable les
refuse ; le commentaire du code cite lui-meme "shapefile after a repack").

L'echec concret pour l'utilisateur
----------------------------------
Couche Shapefile A, B, C, D. Apres T : B deplacee, C renommee, A supprimee
(le fichier est recompacte : C prend le fid de B, D celui de C). Review a
la date T : B est dessinee a sa place d'origine mais avec les attributs de
C (code "C"), et C est dessinee a la place de D avec le code et la surface
de D. La barre ne signale aucune lacune ; l'export ecrit ces entites comme
"etat a T".

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rvw_unstable_fid_live_identity"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _fx():
    from scripts.validation.scenarios import _rvw_unstable_fid
    return _rvw_unstable_fid


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
    return {"code": attrs.get("code"), "nom": attrs.get("nom"),
            "surface": attrs.get("surface"), "xy": pt,
            "unresolved": feat.unresolved}


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_shp_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["shp"] = _fx().make_shp(tmpdir)
    flog(f"rvw_unstable_fid_live_identity setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from recoverland.core.snapshot_overlay_session import complete_from_live
    h = _h()
    fx = _fx()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = fx.load_shp(d["shp"])
        h.pump(0.2)
        cutoff = fx.edit_after_cutoff(plugin, layer, d)

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
        if res is not None:
            res = complete_from_live(res, [info], ctx.trace_id)
            feats = res.features.get(info["fingerprint"], {})
            d["review"] = {k: _state(v) for k, v in feats.items()}
            d["n_unresolved"] = res.n_unresolved

        h.set_view(iface, -5.0, -5.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["review_date"] = h.review_date(dlg, cutoff)
        d["overlay"] = h.overlay_rows(dlg)
        bar = dlg._review_date_bar
        d["bar_tip"] = bar._lbl_status.toolTip() if bar is not None else None
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_unstable_fid_live_identity run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    rev = d.get("review") or {}
    ov = d.get("overlay") or {}
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    b = rev.get("fid:1") or {}
    c = rev.get("fid:2") or {}
    a = rev.get("fid:0") or {}
    out = []
    out.append((
        "setup_real_edits_and_repack_moved_the_fids",
        d.get("commits") == [True, True, True] and d.get("journal_rows", 0) >= 3
        and _fx().fids_moved(d),
        f"commits={d.get('commits')} lignes={d.get('journal_rows')} fid avant="
        f"{d.get('fids_before')} apres={d.get('fids_after')}{crash}",
    ))
    out.append((
        "moved_entity_never_takes_another_entitys_attributes",
        bool(b) and b.get("xy") == (1.0, 1.0) and b.get("code") in (None, "B")
        and b.get("nom") in (None, "Beta"),
        f"B a T={b or 'ABSENTE'} : sa geometrie a T est (1,1) ; ses attributs "
        f"ne peuvent pas venir de la ligne qui porte aujourd'hui son ancien "
        f"fid (C) erreur worker={d.get('worker_error')}{crash}",
    ))
    out.append((
        "renamed_entity_never_drawn_at_another_entitys_place",
        c.get("xy") in (None, (2.0, 2.0)) and c.get("code") in (None, "C")
        and c.get("surface") in (None, 30.0),
        f"C a T={c or 'ABSENTE'} : jamais en (3,3) avec le code ou la surface "
        f"de D, qui porte aujourd'hui son ancien fid{crash}",
    ))
    out.append((
        "unproven_identity_is_named_and_counted",
        "identity_not_proven" in (b.get("unresolved") or "")
        and "identity_not_proven" in (c.get("unresolved") or "")
        and (d.get("n_unresolved") or 0) >= 2,
        f"B.unresolved={b.get('unresolved')!r} C.unresolved={c.get('unresolved')!r} "
        f"n_unresolved={d.get('n_unresolved')} : sur une couche dont les fid "
        f"bougent, la completion par la couche actuelle n'est pas prouvee{crash}",
    ))
    out.append((
        "deleted_entity_full_state_from_the_journal",
        a.get("xy") == (0.0, 0.0) and a.get("code") == "A" and a.get("nom") == "Alpha"
        and not a.get("unresolved"),
        f"A a T={a or 'ABSENTE'} (attendu (0,0) A Alpha, etat complet : le "
        f"DELETE porte toute l'entite, aucune identite a prouver){crash}",
    ))
    foreign = [(code, r) for code, r in ov.items()
               if code == "D" or (code == "C" and r.get("x") == 3.0)
               or (r.get("x") == 1.0 and code not in ("B", "None")
                   and not _h().is_null(code))]
    out.append((
        "overlay_shows_no_foreign_state",
        not foreign and bool(ov),
        f"calques Review={ov} : aucune entite ne doit y porter l'etat d'une "
        f"autre (lignes fautives={foreign}) ; review_on={d.get('review_on')}{crash}",
    ))
    out.append((
        "bar_announces_the_incomplete_entities",
        "pas entièrement connu" in (d.get("bar_tip") or ""),
        f"infobulle={d.get('bar_tip')!r} : les entites dont l'identite n'est "
        f"pas prouvee doivent etre annoncees",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_unstable_fid_live_identity.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
