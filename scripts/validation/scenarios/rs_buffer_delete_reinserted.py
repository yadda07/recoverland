# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_buffer_delete_reinserted - RecoverLand validation runtime.

Invariant: I-REWIND-01 (apres un rewind, le fichier contient l'etat a T,
ou rien n'est ecrit et le refus est nomme) et fail-loud (aucun succes
annonce sur une compensation qui n'a pas eu lieu).
Constat resolu: item 2 du tour executeur. `restore_executor._buffer_delete`
repondait "deja absente" (SKIPPED_ALREADY_ABSENT / absent_vs_insert_comp)
quand la seule entite portant l'instantane de l'INSERT a annuler etait
une entite que le MEME rewind venait de re-inserer, et ecartait sans
preuve une entree negative de fid_remap, meme quand elle designait la
meme entite.

Le mecanisme
------------
Un rewind applique d'abord les re-insertions (phase 0), puis les
suppressions (phase 2), dans un seul tampon d'edition. `fid_remap` note,
par empreinte d'entite, le FID que chaque re-insertion a recu dans le
tampon (negatif). La suppression qui compense un INSERT:

  * ecartait toute entree negative du remap ("defense contre le
    recyclage de FID") sans regarder l'entite: quand le plan re-insere
    PUIS supprime la meme entite (meme empreinte), la suppression ne
    visait plus rien;
  * excluait ensuite du balayage de l'instantane toutes les entites
    re-inserees par le rewind, ne trouvait donc rien, et concluait
    "l'entite inseree n'est plus dans la couche": saut idempotent,
    compte comme reussi.

Sur un shapefile renumerote, la meme entite logique porte deux cles
(fid:3 pour la suppression de l'utilisateur, fid:1 pour l'insertion que
le rewind annule): la re-insertion ressuscite l'entite, la suppression
se declare sans objet, le rewind est annonce reussi et l'entite est de
retour dans le fichier. C'est la reproduction silencieuse de
rw_state_at_t_toggles (RL_TOGGLE_PROVIDERS=shp RL_TOGGLE_SEEDS=113
RL_TOGGLE_STEPS=36), ramenee ici a deux compensations deterministes.

Ce que l'utilisateur voit
-------------------------
Il demande l'etat d'hier, ou l'entite C001 n'existait pas: le rapport dit
"termine", et C001 est dans sa couche.

Cas (tampon d'edition reel, commit reel, logique soft/hard du
StrictRestoreRunner reproduite a l'identique)
----------------------------------------------------------------------
    R1  shapefile: re-insertion de C001 (suppression sous fid:3), puis
        annulation d'un INSERT de C001 sous fid:1. La seule entite qui
        porte l'instantane est celle que le rewind vient de re-inserer:
        refus NOMME target_reinserted_in_batch, echec dur (le runner
        annule la couche), fichier intact.
    R2  shapefile: re-insertion puis suppression de la MEME entite (meme
        empreinte fid:3): l'entree du remap est prouvee contre
        l'instantane de l'INSERT et supprimee, APPLIED; apres commit
        l'entite n'est pas dans le fichier (pas de copie ressuscitee).
    R3  GeoPackage, temoin de la defense de recyclage: A (fid 5) a ete
        supprimee, B a recu le fid 5. La re-insertion de A ecrit le remap
        pk:fid=5; l'annulation de l'INSERT de B ne doit PAS viser A (son
        contenu n'est pas celui de B): B est supprimee, A revient.

Verdict avant correctif: FAIL (R1 annonce un saut et ressuscite C001, R2
ressuscite l'entite). Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile

SCENARIO_ID = "rs_buffer_delete_reinserted"
INVARIANT = "I-REWIND-01"

EXPECTED_SIGNATURE = r"BUF_DEL .*reason_code=target_reinserted_in_batch"

_SHP_FIELDS = ("code:string(12)", "nom:string(20)", "val:integer")
_SCHEMA = (("code", "string"), ("nom", "string"), ("val", "integer"))
# The layer the user left: C001 is absent (deleted again after a rewind
# had re-inserted it).
_ROWS = [(["C000", "zero", 0], "POINT(0 0)"),
         (["C002", "deux", 20], "POINT(20 10)"),
         (["C003", "trois", 30], "POINT(30 15)")]
_C001 = {"code": "C001", "nom": "un", "val": 10}
_C001_WKT = "POINT(10 5)"


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _event(eid, op, fid, attrs, wkt, identity=None, minute=0):
    fx = _fx()
    ident = identity or {"fid": fid}
    from recoverland.core.identity import compute_entity_fingerprint
    ident_json = json.dumps(ident)
    return fx.audit_event(
        event_id=eid, operation_type=op, feature_identity_json=ident_json,
        entity_fingerprint=compute_entity_fingerprint(ident_json),
        attributes_json=json.dumps({"all_attributes": attrs}),
        geometry_wkb=fx.wkb_wkt(wkt), geometry_type="Point",
        crs_authid="EPSG:2154", field_schema_json=fx.schema(*_SCHEMA),
        created_at=f"2026-09-01T09:{minute:02d}:00+00:00",
        event_schema_version=7)


def _mini_rewind(layer, actions, tid):
    """The StrictRestoreRunner loop: one buffer, one shared remap, the
    runner's soft/hard split, then the commit (or the rollback)."""
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.restore_service import _classify_restore_result
    remap = {}
    results = []
    layer.startEditing()
    for comp_op, event in actions:
        result = dict(_apply_via_buffer(layer, comp_op, event, remap,
                                        trace_id=tid))
        reason = _classify_restore_result(result)
        result["_class"] = reason
        results.append(result)
        if not result.get("success") and reason not in (
                "target_absent", "geometry_drift"):
            layer.rollBack()
            return {"results": results, "committed": False,
                    "rolled_back": True}
    committed = bool(layer.commitChanges())
    if not committed:
        layer.rollBack()
    return {"results": results, "committed": committed, "rolled_back": False}


def _codes(path, name=""):
    fx = _fx()
    layer = fx.open_layer(path, name)
    out = sorted(str(f["code"]) for f in layer.getFeatures())
    del layer
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rs_bdr_")
    flog(f"rs_buffer_delete_reinserted setup: trace_id={ctx.trace_id}")


def _shp(tmp, name):
    fx = _fx()
    return fx.make_shapefile(tmp, name, "Point", _SHP_FIELDS, _ROWS,
                             crs="EPSG:2154")


def run(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tmp, tid = ctx.data["tmp"], ctx.trace_id
    out = {}
    try:
        # R1: two keys for one logical entity (shapefile renumbered).
        path = _shp(tmp, "r1")
        layer = fx.open_layer(path)
        reinsert = _event(11, "DELETE", 3, _C001, _C001_WKT, minute=30)
        undo_insert = _event(12, "INSERT", 1, _C001, _C001_WKT, minute=20)
        out["r1_before"] = _codes(path)
        out["r1"] = _mini_rewind(layer, [("INSERT", reinsert),
                                         ("DELETE", undo_insert)], tid)
        del layer
        out["r1_after"] = _codes(path)

        # R2: one entity, re-inserted then deleted in the same buffer.
        path = _shp(tmp, "r2")
        layer = fx.open_layer(path)
        created = _event(21, "INSERT", 3, _C001, _C001_WKT, minute=10)
        deleted = _event(22, "DELETE", 3, _C001, _C001_WKT, minute=40)
        out["r2_before"] = _codes(path)
        out["r2"] = _mini_rewind(layer, [("INSERT", deleted),
                                         ("DELETE", created)], tid)
        del layer
        out["r2_after"] = _codes(path)

        # R3: GeoPackage, key 5 reused by B after A was deleted.
        from osgeo import ogr
        fields = [("code", ogr.OFTString), ("nom", ogr.OFTString),
                  ("val", ogr.OFTInteger)]
        gpath = fx.make_gpkg(tmp, "r3", ogr.wkbPoint, fields, [
            (1, ["C000", "zero", 0], "POINT(0 0)"),
            (5, ["B", "bee", 55], "POINT(50 50)"),
        ], epsg=2154)
        layer = fx.open_layer(gpath, "r3")
        ident5 = {"fid": 5, "pk_field": "fid", "pk_value": 5}
        del_a = _event(31, "DELETE", 5, dict(fid=5, code="A", nom="ay",
                                             val=5), "POINT(5 5)",
                       identity=ident5, minute=10)
        ins_b = _event(32, "INSERT", 5, dict(fid=5, code="B", nom="bee",
                                             val=55), "POINT(50 50)",
                       identity=ident5, minute=20)
        out["r3_before"] = _codes(gpath, "r3")
        out["r3"] = _mini_rewind(layer, [("INSERT", del_a),
                                         ("DELETE", ins_b)], tid)
        del layer
        out["r3_after"] = _codes(gpath, "r3")
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    ctx.data["out"] = out
    summary = {k: [(r.get("status"), r.get("reason_code"))
                   for r in v["results"]]
               for k, v in out.items() if isinstance(v, dict) and "results" in v}
    flog(f"rs_buffer_delete_reinserted run: trace_id={tid} {summary} "
         f"error={out.get('error')}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = []
    res.append(("no_crash", "error" not in out,
                f"{out.get('error')} {out.get('trace', '')}"))

    r1 = out.get("r1") or {}
    r1_del = (r1.get("results") or [{}, {}])[-1]
    res.append((
        "r1_delete_of_a_reinserted_twin_is_refused_by_name",
        r1_del.get("success") is False
        and r1_del.get("reason_code") == "target_reinserted_in_batch",
        f"resultat de la suppression={r1_del}: la seule entite portant "
        f"l'instantane de l'INSERT est celle que ce rewind vient de "
        f"re-inserer; repondre 'deja absente' ressuscite C001 sous un "
        f"rewind annonce reussi.",
    ))
    res.append((
        "r1_refusal_is_hard_so_the_layer_is_rolled_back",
        r1_del.get("_class") == "failed" and r1.get("rolled_back") is True,
        f"classe={r1_del.get('_class')} rollback={r1.get('rolled_back')}: "
        f"committer autour de ce refus ecrirait la resurrection que le "
        f"rewind n'a pas pu justifier.",
    ))
    res.append((
        "r1_file_does_not_get_c001_back",
        out.get("r1_after") == out.get("r1_before")
        and "C001" not in (out.get("r1_after") or []),
        f"avant={out.get('r1_before')} apres={out.get('r1_after')}: C001 "
        f"etait absente a T et l'est encore; le fichier ne doit pas la "
        f"recevoir.",
    ))

    r2 = out.get("r2") or {}
    r2_del = (r2.get("results") or [{}, {}])[-1]
    res.append((
        "r2_same_entity_reinserted_then_deleted_is_applied",
        r2_del.get("success") is True and r2_del.get("status") == "APPLIED",
        f"resultat de la suppression={r2_del}: le remap designe l'entite "
        f"que ce rewind vient de re-inserer sous la MEME empreinte; prouvee "
        f"contre l'instantane de l'INSERT, elle doit etre supprimee.",
    ))
    res.append((
        "r2_no_resurrected_copy_in_the_file",
        r2.get("committed") is True
        and out.get("r2_after") == out.get("r2_before"),
        f"commit={r2.get('committed')} avant={out.get('r2_before')} "
        f"apres={out.get('r2_after')}: l'entite creee puis supprimee apres "
        f"T ne doit pas revenir.",
    ))

    r3 = out.get("r3") or {}
    r3_res = r3.get("results") or [{}, {}]
    res.append((
        "r3_fid_reuse_defense_kept",
        r3.get("committed") is True
        and out.get("r3_after") == ["A", "C000"]
        and r3_res[-1].get("status") == "APPLIED",
        f"resultats={[(r.get('status'), r.get('reason_code')) for r in r3_res]} "
        f"apres={out.get('r3_after')}: la re-insertion de A partage la cle 5 "
        f"avec B; l'annulation de l'INSERT de B doit supprimer B, pas A.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_buffer_delete_reinserted run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(root))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
