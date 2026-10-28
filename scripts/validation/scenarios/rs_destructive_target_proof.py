# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_destructive_target_proof - RecoverLand validation runtime.

Invariant: I-3 (identite prouvee avant toute ecriture destructrice).
Constats resolus: snapshot-fallback-deletes-unrelated (CRITICAL),
weak-identity-destructive-fallbacks (HIGH), pk-trusted-without-evidence
(HIGH, volet suppression / reinsertion).

Le mecanisme
------------
Annuler un INSERT, c'est supprimer l'entite inseree. Trois chemins le font
(la compensation DELETE du rewind `_buffer_delete`, la restauration d'un
INSERT `restore_inserted_feature`, et l'annulation d'une restauration de
DELETE qui passe par la meme fonction), et tous cherchaient leur cible par
`_find_by_snapshot(resolve_ambiguity=True)` quand le FID ne prouvait rien:

  a. si la boite englobante de l'entite inseree etait vide, un second
     balayage parcourait TOUTE la couche sans test de geometrie en sautant
     chaque attribut NULL d'un cote ou de l'autre: un instantane saisi sans
     attributs (borne digitalisee, pas encore renseignee) correspondait a
     TOUTES les entites, et l'egalite etait tranchee par max(fid);
  b. une branche "lenient" acceptait l'unique candidat de meme geometrie
     qui echouait au test attributaire: le balayage s'arretant au premier
     champ different, ce candidat pouvait differer sur TOUS ses attributs;
  c. `_buffer_delete` supprimait l'occupant du FID historique des qu'il
     partageait UNE valeur non nulle avec l'instantane;
  d. une cle primaire etait crue sur parole: sur une table dont la cle est
     reutilisee, l'entite qui porte desormais la cle etait supprimee, et
     restaurer un DELETE repondait "Already exists" sans regarder l'entite.

Ce que l'utilisateur voit
-------------------------
Il revient au 1er septembre sur sa couche de bornes. La borne inseree
apres cette date a deja disparu. Le rewind supprime a sa place la borne la
plus recente de la couche, affiche "restauree", ecrit la trace: la donnee
d'un tiers est detruite et le journal affirme une restauration reussie.
Avec une cle reutilisee, la parcelle INTRUS est supprimee a la place de
l'ancienne, et la restauration d'une parcelle supprimee repond "deja
presente" alors qu'elle ne reviendra jamais.

Cas
---
    A  instantane tout-NULL a (100 200), le FID historique tient une
       borne sans rapport: rewind, restauration, annulation.
    B  instantane dont la geometrie est celle de Q, attributs tous
       differents: rewind, restauration (Q ne doit pas bouger).
    C  l'occupant du FID historique partage la seule commune: rewind.
    D  GeoPackage, cle fid=3 reutilisee par INTRUS: rewind, restauration
       de l'INSERT, restauration du DELETE (pas de "deja presente").
    T  temoin: l'entite inseree est bien la, instantane complet: elle
       doit etre supprimee (la preuve ne bloque pas le cas legitime).

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_destructive_target_proof"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SHP_FIELDS = ["nom:string(20)", "code:string(20)", "commune:string(20)"]
_BORNES = [
    (["B1", "C1", "LYON"], "Point (10 10)"),
    (["B2", "C2", "LYON"], "Point (20 20)"),
    (["B3", "C3", "PARIS"], "Point (30 30)"),
    (["Q", "Z9", "NICE"], "Point (50 50)"),
    (["LEGIT", "L1", "BREST"], "Point (60 60)"),
]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _gpkg_fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def setup(ctx):
    from recoverland.core.logger import flog
    fx = _fx()

    tmpdir = tempfile.mkdtemp(prefix="rl_rs_dtp_")
    ctx.data["tmpdir"] = tmpdir
    # One shapefile per path under test: each path must meet the layer
    # untouched, otherwise a first deletion would hide the second.
    for key in ("a1", "a2", "a3", "b1", "b2", "c1", "t1"):
        ctx.data[f"shp_{key}"] = fx.make_shapefile(
            tmpdir, f"bornes_{key}", "Point", _SHP_FIELDS, _BORNES)
    ctx.data["before_shp"] = fx.file_rows(ctx.data["shp_a1"])

    from osgeo import ogr
    for key in ("d1", "d2", "d3"):
        path = fx.make_gpkg(tmpdir, f"parcelles_{key}", ogr.wkbPoint,
                            _gpkg_fields(), [
                                (1, ["P1", "K1"], "POINT (1 1)"),
                                (2, ["P2", "K2"], "POINT (2 2)"),
                                (3, ["ORIG", "K3"], "POINT (3 3)"),
                            ])
        # The key 3 is reused by an unrelated parcel.
        fx.gpkg_delete_and_reuse(path, f"parcelles_{key}", 3,
                                 _gpkg_fields(), ["INTRUS", "K9"],
                                 "POINT (8 8)")
        ctx.data[f"gpkg_{key}"] = path
    ctx.data["before_gpkg"] = fx.file_rows(ctx.data["gpkg_d1"], "parcelles_d1")
    flog(f"rs_destructive_target_proof setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir} bornes={len(ctx.data['before_shp'])}", "INFO")


def _insert_event(eid, attrs, wkt, identity, op="INSERT"):
    fx = _fx()
    fields = (fx.schema(("fid", "integer"), ("nom", "string"),
                        ("code", "string"))
              if "pk_value" in identity else
              fx.schema(("nom", "string"), ("code", "string"),
                        ("commune", "string")))
    return fx.audit_event(
        event_id=eid, operation_type=op, field_schema_json=fields,
        feature_identity_json=json.dumps(identity),
        entity_fingerprint=(f"pk:fid={identity['pk_value']}"
                            if "pk_value" in identity
                            else f"fid:{identity['fid']}"),
        attributes_json=json.dumps({"all_attributes": attrs}),
        geometry_wkb=fx.wkb_wkt(wkt), geometry_type="Point")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_inserted_feature, restore_deleted_feature, undo_prepare,
        undo_apply_one,
    )
    fx = _fx()
    tid = ctx.trace_id
    res = {}
    after = {}

    flog(f"rs_destructive_target_proof run start: trace_id={tid}", "INFO")

    # --- A: an unattributed borne inserted at (100 200), gone since ------
    nulls = {"nom": None, "code": None, "commune": None}
    ev_a = _insert_event(101, nulls, "Point (100 200)", {"fid": 1})
    res["a1"] = fx.buffer_apply(fx.open_layer(ctx.data["shp_a1"]),
                                "DELETE", ev_a, tid)
    res["a2"] = dict(restore_inserted_feature(
        fx.open_layer(ctx.data["shp_a2"]), ev_a))
    ev_a_del = _insert_event(102, nulls, "Point (100 200)", {"fid": 1},
                             op="DELETE")
    layer_a3 = fx.open_layer(ctx.data["shp_a3"])
    res["a3"] = dict(undo_apply_one(
        layer_a3, ev_a_del, undo_prepare(layer_a3, [ev_a_del])))

    # --- B: same outline as Q, every attribute different -----------------
    ev_b = _insert_event(103, {"nom": "PARCELLE", "code": "P1",
                               "commune": "LYON"},
                         "Point (50 50)", {"fid": 9})
    res["b1"] = fx.buffer_apply(fx.open_layer(ctx.data["shp_b1"]),
                                "DELETE", ev_b, tid)
    res["b2"] = dict(restore_inserted_feature(
        fx.open_layer(ctx.data["shp_b2"]), ev_b))

    # --- C: the occupant of the historical FID shares the commune only ---
    ev_c = _insert_event(104, {"nom": "BORNE_X", "code": "CX",
                               "commune": "LYON"},
                         "Point (70 70)", {"fid": 0})
    res["c1"] = fx.buffer_apply(fx.open_layer(ctx.data["shp_c1"]),
                                "DELETE", ev_c, tid)

    # --- D: key 3 reused by INTRUS ---------------------------------------
    ident = {"fid": 3, "pk_field": "fid", "pk_value": 3}
    orig = {"fid": 3, "nom": "ORIG", "code": "K3"}
    ev_d = _insert_event(105, orig, "POINT (3 3)", ident)
    res["d1"] = fx.buffer_apply(
        fx.open_layer(ctx.data["gpkg_d1"], "parcelles_d1"), "DELETE", ev_d, tid)
    res["d2"] = dict(restore_inserted_feature(
        fx.open_layer(ctx.data["gpkg_d2"], "parcelles_d2"), ev_d))
    ev_d_del = _insert_event(106, orig, "POINT (3 3)", ident, op="DELETE")
    res["d3"] = dict(restore_deleted_feature(
        fx.open_layer(ctx.data["gpkg_d3"], "parcelles_d3"), ev_d_del))

    # --- T: control, the inserted feature is there -----------------------
    legit_fid = None
    layer_t = fx.open_layer(ctx.data["shp_t1"])
    for feat in layer_t.getFeatures():
        if feat["nom"] == "LEGIT":
            legit_fid = feat.id()
    ev_t = _insert_event(107, {"nom": "LEGIT", "code": "L1",
                               "commune": "BREST"},
                         "Point (60 60)", {"fid": legit_fid})
    res["t1"] = fx.buffer_apply(layer_t, "DELETE", ev_t, tid)

    for key in ("a1", "a2", "a3", "b1", "b2", "c1", "t1"):
        after[key] = fx.file_rows(ctx.data[f"shp_{key}"])
    for key in ("d1", "d2", "d3"):
        after[key] = fx.file_rows(ctx.data[f"gpkg_{key}"], f"parcelles_{key}")
    ctx.data["res"] = res
    ctx.data["after"] = after
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"))
               for key, value in res.items()}
    flog(f"rs_destructive_target_proof run end: trace_id={tid} "
         f"results={summary}", "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    fx = _fx()

    out = []
    res = ctx.data.get("res") or {}
    after = ctx.data.get("after") or {}
    before_shp = ctx.data.get("before_shp") or []
    before_gpkg = ctx.data.get("before_gpkg") or []

    out.append((
        "fixture_seeded",
        len(before_shp) == len(_BORNES) and len(before_gpkg) == 3
        and any("INTRUS" in repr(r) for r in before_gpkg),
        f"bornes={len(before_shp)} parcelles={before_gpkg}",
    ))

    labels = {
        "a1": "rewind (compensation DELETE) d'un INSERT sans attributs",
        "a2": "restauration de l'INSERT sans attributs",
        "a3": "annulation d'une restauration de DELETE sans attributs",
        "b1": "rewind: meme contour que Q, attributs tous differents",
        "b2": "restauration: meme contour que Q, attributs tous differents",
        "c1": "rewind: l'occupant du FID ne partage que la commune",
    }
    for key, label in labels.items():
        out.append((
            f"{key}_no_feature_destroyed",
            after.get(key) == before_shp,
            f"{label}: couche apres={after.get(key)} avant={before_shp}. "
            f"Une borne sans rapport a ete supprimee a la place de l'entite "
            f"inseree, disparue depuis: l'utilisateur perd la donnee d'un "
            f"tiers et lit 'restauree'.",
        ))
        out.append((
            f"{key}_not_reported_applied",
            fx.not_applied(res.get(key)),
            f"{label}: resultat={res.get(key)}. Rien ne designe l'entite "
            f"inseree avec certitude: le resultat doit etre un refus nomme "
            f"ou un saut declare, jamais 'applied'.",
        ))
    out.append((
        "c1_weak_evidence_is_a_named_refusal",
        (res.get("c1") or {}).get("reason_code") == "target_unverifiable",
        f"resultat={res.get('c1')}: une seule valeur commune (LYON) ne "
        f"prouve rien; attendu reason_code=target_unverifiable",
    ))

    for key, label in (("d1", "rewind"), ("d2", "restauration de l'INSERT")):
        out.append((
            f"{key}_reused_key_holder_spared",
            after.get(key) == before_gpkg,
            f"{label}: parcelles apres={after.get(key)} avant={before_gpkg}. "
            f"La cle fid=3 est desormais portee par INTRUS, qui ne partage "
            f"rien avec l'instantane: la supprimer detruit une parcelle "
            f"sans rapport.",
        ))
        out.append((
            f"{key}_not_reported_applied",
            fx.not_applied(res.get(key)),
            f"{label}: resultat={res.get(key)}",
        ))
    d3 = res.get("d3") or {}
    out.append((
        "d3_reused_key_is_not_already_exists",
        not (d3.get("success") is True and d3.get("skipped") is True),
        f"restauration du DELETE de ORIG: resultat={d3}. La cle fid=3 est "
        f"portee par INTRUS: repondre 'Already exists (skip)' laisse ORIG "
        f"definitivement absente et la trace neutralise son DELETE pour "
        f"tous les rewinds suivants.",
    ))
    out.append((
        "d3_layer_untouched",
        after.get("d3") == before_gpkg or any(
            "ORIG" in repr(r) for r in after.get("d3") or []),
        f"parcelles apres={after.get('d3')}: soit rien n'est ecrit (refus), "
        f"soit ORIG est reellement revenue",
    ))

    t1 = res.get("t1") or {}
    out.append((
        "control_legit_target_deleted",
        t1.get("success") is True and not t1.get("skipped")
        and not any("LEGIT" in repr(r) for r in after.get("t1") or []),
        f"temoin: resultat={t1} couche apres={after.get('t1')}. L'entite "
        f"inseree est la, avec tout son instantane: elle doit etre "
        f"supprimee, sinon le rewind ne rend plus l'etat demande.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_destructive_target_proof.*trace_id={ctx.trace_id}",
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
