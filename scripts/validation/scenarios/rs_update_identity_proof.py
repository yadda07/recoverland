"""rs_update_identity_proof - RecoverLand validation runtime.

Invariant: I-3 (identite prouvee avant d'ecraser une entite).
Constats resolus: weak-update-target-proof (HIGH),
pk-trusted-without-evidence (HIGH, volet UPDATE),
weak-identity-destructive-fallbacks (HIGH, volet d: relocalisation).

Le mecanisme
------------
Un UPDATE purement attributaire ne transporte que les champs modifies
(delta ``changed_only``) et aucune geometrie. Pour prouver que l'occupant
du FID historique est bien l'entite editee, `_verify_update_target` ne
comparait QUE les nouvelles valeurs des champs modifies: une seule valeur,
souvent un booleen ou une enumeration, suffisait. `_target_already_at_old`
faisait la meme comparaison etroite pour declarer "deja a l'etat cible".
La relocalisation par geometrie prenait la PREMIERE entite de meme
geometrie, sans refuser l'ambiguite, y compris pour une identite a cle
primaire que la recherche par cle venait de ne pas trouver. Enfin une cle
primaire etait acceptee sans aucune verification de contenu.

Ce que l'utilisateur voit
-------------------------
Shapefile reorganise apres l'edition "valide: 0 -> 1" de la parcelle P
(FID 1). Le FID 1 porte maintenant Q, dont valide vaut aussi 1, comme la
moitie de la couche. La restauration ecrit valide=0 sur Q, annonce
"restauree"; P reste a 1. Si Q valait deja 0, le rewind annonce "deja a
l'etat cible" alors que P n'a jamais ete restauree. Deux points empiles:
le rewind deplace l'un des deux au hasard. Une cle reutilisee: l'ancienne
valeur est ecrite sur la parcelle INTRUS.

Cas
---
    U1  restauration, un seul booleen partage par d'autres: refus nomme
    U2  rewind, occupant deja a l'ancienne valeur partagee: pas de saut
    U3  rewind, geometrie NEW portee par deux points empiles: refus
    U4  rewind, cle introuvable, doublon de meme geometrie: pas de
        relocalisation d'une identite a cle
    U5  restauration, cle reutilisee par INTRUS: refus
    C1  temoin FID: valeur NEW unique dans la couche -> restauree
    C2  temoin cle: contenu corrobore -> restauree

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_update_identity_proof"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SHP_FIELDS = ["nom:string(20)", "valide:integer"]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _gpkg_fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def _update_event(eid, identity, changed, old_wkt=None, new_wkt=None):
    fx = _fx()
    fp = (f"pk:fid={identity['pk_value']}" if "pk_value" in identity
          else f"fid:{identity['fid']}")
    if "pk_value" in identity:
        fields = fx.schema(("fid", "integer"), ("nom", "string"),
                           ("code", "string"))
    else:
        fields = fx.schema(("nom", "string"), ("valide", "integer"))
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE", field_schema_json=fields,
        feature_identity_json=json.dumps(identity), entity_fingerprint=fp,
        attributes_json=json.dumps({"changed_only": {
            k: {"old": old, "new": new} for k, (old, new) in changed.items()}}),
        geometry_wkb=fx.wkb_wkt(old_wkt) if old_wkt else None,
        new_geometry_wkb=fx.wkb_wkt(new_wkt) if new_wkt else None,
        geometry_type="Point")


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()

    tmpdir = tempfile.mkdtemp(prefix="rl_rs_uip_")
    ctx.data["tmpdir"] = tmpdir
    # U1: FID 1 holds Q (valide=1), like half the layer.
    ctx.data["shp_u1"] = fx.make_shapefile(tmpdir, "u1", "Point", _SHP_FIELDS, [
        (["A", 1], "Point (1 1)"), (["Q", 1], "Point (2 2)"),
        (["B", 1], "Point (3 3)"), (["P", 1], "Point (4 4)"),
    ])
    # U2: FID 1 holds Q already at the OLD value 0, shared by others; P,
    # the real target, still carries NEW (1) elsewhere.
    ctx.data["shp_u2"] = fx.make_shapefile(tmpdir, "u2", "Point", _SHP_FIELDS, [
        (["A", 0], "Point (1 1)"), (["Q", 0], "Point (2 2)"),
        (["B", 0], "Point (3 3)"), (["P", 1], "Point (4 4)"),
    ])
    # U3: two address points stacked at (5 5).
    ctx.data["shp_u3"] = fx.make_shapefile(tmpdir, "u3", "Point", _SHP_FIELDS, [
        (["S1", 7], "Point (5 5)"), (["S2", 7], "Point (5 5)"),
        (["F", 2], "Point (8 8)"),
    ])
    # C1: attribute-only edit whose NEW value is unique in the layer.
    ctx.data["shp_c1"] = fx.make_shapefile(tmpdir, "c1", "Point", _SHP_FIELDS, [
        (["A", 1], "Point (1 1)"), (["PONT", 1], "Point (2 2)"),
    ])
    # U4 / U5 / C2: GeoPackage, fid is the primary key.
    ctx.data["gpkg_u4"] = fx.make_gpkg(
        tmpdir, "u4", ogr.wkbPoint, _gpkg_fields(), [
            (1, ["A", "K1"], "POINT (1 1)"),
            (6, ["Y_DOUBLON", "K6"], "POINT (9 9)"),
        ],
    )
    for key in ("u5", "c2"):
        path = fx.make_gpkg(tmpdir, key, ogr.wkbPoint, _gpkg_fields(), [
            (1, ["A", "K1"], "POINT (1 1)"),
            (3, ["ORIG", "K3"], "POINT (3 3)"),
        ])
        fx.gpkg_delete_and_reuse(path, key, 3, _gpkg_fields(),
                                 ["INTRUS", "K9"], "POINT (8 8)")
        ctx.data[f"gpkg_{key}"] = path
    ctx.data["before"] = {
        "u1": fx.file_rows(ctx.data["shp_u1"]),
        "u2": fx.file_rows(ctx.data["shp_u2"]),
        "u3": fx.file_rows(ctx.data["shp_u3"]),
        "u4": fx.file_rows(ctx.data["gpkg_u4"], "u4"),
        "u5": fx.file_rows(ctx.data["gpkg_u5"], "u5"),
    }
    flog(f"rs_update_identity_proof setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _fid_of(layer, nom):
    for feat in layer.getFeatures():
        if feat["nom"] == nom:
            return feat.id()
    return None


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import restore_updated_feature
    fx = _fx()
    tid = ctx.trace_id
    res = {}
    flog(f"rs_update_identity_proof run start: trace_id={tid}", "INFO")

    layer = fx.open_layer(ctx.data["shp_u1"])
    q_fid = _fid_of(layer, "Q")
    ev_u1 = _update_event(201, {"fid": q_fid}, {"valide": (0, 1)})
    res["u1"] = dict(restore_updated_feature(layer, ev_u1))

    layer = fx.open_layer(ctx.data["shp_u2"])
    ev_u2 = _update_event(202, {"fid": _fid_of(layer, "Q")},
                          {"valide": (0, 1)})
    res["u2"] = fx.buffer_apply(layer, "UPDATE", ev_u2, tid)

    layer = fx.open_layer(ctx.data["shp_u3"])
    ev_u3 = _update_event(203, {"fid": 9}, {}, old_wkt="Point (0 0)",
                          new_wkt="Point (5 5)")
    res["u3"] = fx.buffer_apply(layer, "UPDATE", ev_u3, tid)

    layer = fx.open_layer(ctx.data["gpkg_u4"], "u4")
    ev_u4 = _update_event(204, {"fid": 5, "pk_field": "fid", "pk_value": 5},
                          {"nom": ("X_OLD", "X")}, old_wkt="POINT (1 1)",
                          new_wkt="POINT (9 9)")
    res["u4"] = fx.buffer_apply(layer, "UPDATE", ev_u4, tid)

    layer = fx.open_layer(ctx.data["gpkg_u5"], "u5")
    ev_u5 = _update_event(205, {"fid": 3, "pk_field": "fid", "pk_value": 3},
                          {"nom": ("ORIG_OLD", "ORIG")})
    res["u5"] = dict(restore_updated_feature(layer, ev_u5))

    layer = fx.open_layer(ctx.data["shp_c1"])
    ev_c1 = _update_event(206, {"fid": _fid_of(layer, "PONT")},
                          {"nom": ("PILE", "PONT")})
    res["c1"] = dict(restore_updated_feature(layer, ev_c1))

    layer = fx.open_layer(ctx.data["gpkg_c2"], "c2")
    ev_c2 = _update_event(207, {"fid": 3, "pk_field": "fid", "pk_value": 3},
                          {"nom": ("INTRUS_AVANT", "INTRUS")})
    res["c2"] = dict(restore_updated_feature(layer, ev_c2))

    ctx.data["res"] = res
    ctx.data["after"] = {
        "u1": fx.file_rows(ctx.data["shp_u1"]),
        "u2": fx.file_rows(ctx.data["shp_u2"]),
        "u3": fx.file_rows(ctx.data["shp_u3"]),
        "u4": fx.file_rows(ctx.data["gpkg_u4"], "u4"),
        "u5": fx.file_rows(ctx.data["gpkg_u5"], "u5"),
        "c1": fx.file_rows(ctx.data["shp_c1"]),
        "c2": fx.file_rows(ctx.data["gpkg_c2"], "c2"),
    }
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"))
               for key, value in res.items()}
    flog(f"rs_update_identity_proof run end: trace_id={tid} "
         f"results={summary}", "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    res = ctx.data.get("res") or {}
    before = ctx.data.get("before") or {}
    after = ctx.data.get("after") or {}

    cases = {
        "u1": ("restauration: un seul booleen, partage par toute la couche",
               "Q a recu valide=0 a la place de P"),
        "u3": ("rewind: geometrie NEW portee par deux points empiles",
               "l'un des deux points empiles a ete deplace au hasard"),
        "u4": ("rewind: cle 5 introuvable, doublon Y de meme geometrie",
               "Y a recu l'ancien nom et l'ancienne geometrie de X"),
        "u5": ("restauration: cle 3 reutilisee par INTRUS",
               "INTRUS a recu l'ancien nom de ORIG"),
    }
    for key, (label, damage) in cases.items():
        out.append((
            f"{key}_layer_untouched",
            after.get(key) == before.get(key),
            f"{label}: apres={after.get(key)} avant={before.get(key)}: "
            f"{damage}, et le resultat annonce une restauration.",
        ))
        out.append((
            f"{key}_is_a_refusal",
            (res.get(key) or {}).get("success") is False,
            f"{label}: resultat={res.get(key)}: la cible n'est pas prouvee, "
            f"le resultat doit etre un refus nomme",
        ))
    u1 = res.get("u1") or {}
    out.append((
        "u1_refusal_is_named_unverifiable",
        u1.get("reason_code") == "target_unverifiable"
        and u1.get("identity_level") == "WEAK"
        and "value_shared" in str(u1.get("message", "")),
        f"resultat={u1}: attendu reason_code=target_unverifiable, "
        f"identity_level=WEAK et la cause nommee (valeur partagee): le "
        f"refus doit tenir a la preuve trop faible, pas a autre chose",
    ))
    u5 = res.get("u5") or {}
    out.append((
        "u5_refusal_names_the_contradiction",
        "post_attr_mismatch" in str(u5.get("message", "")),
        f"resultat={u5}: la cle designe INTRUS, dont le contenu contredit "
        f"l'etat NEW de l'evenement; le message doit le dire",
    ))

    u2 = res.get("u2") or {}
    out.append((
        "u2_not_counted_already_done",
        not u2.get("skipped") and u2.get("reason_code") != "already_at_old",
        f"resultat={u2}: Q porte l'ancienne valeur 0 comme la moitie de la "
        f"couche; rien ne prouve que Q est P. Annoncer 'deja a l'etat "
        f"cible' cache que P (valide=1) n'a jamais ete restauree.",
    ))
    out.append((
        "u2_layer_untouched",
        after.get("u2") == before.get("u2"),
        f"apres={after.get('u2')} avant={before.get('u2')}",
    ))

    c1 = res.get("c1") or {}
    out.append((
        "control_unique_value_restored",
        c1.get("success") is True
        and any(r[0] == "PILE" for r in after.get("c1") or []),
        f"temoin FID: resultat={c1} apres={after.get('c1')}. La valeur NEW "
        f"'PONT' n'existe qu'une fois dans la couche: la preuve est "
        f"suffisante, la restauration doit passer.",
    ))
    c2 = res.get("c2") or {}
    out.append((
        "control_corroborated_key_restored",
        c2.get("success") is True
        and any(r[1] == "INTRUS_AVANT" for r in after.get("c2") or []),
        f"temoin cle: resultat={c2} apres={after.get('c2')}. La cle 3 "
        f"designe INTRUS, qui porte bien l'etat NEW de l'evenement: la "
        f"restauration doit passer.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_update_identity_proof.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
