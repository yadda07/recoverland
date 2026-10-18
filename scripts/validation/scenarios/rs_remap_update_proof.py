"""rs_remap_update_proof - RecoverLand validation runtime.

Invariant: I-3 (jamais d'ecriture sur une entite dont l'identite n'est
pas prouvee) et I-REWIND-01 (l'etat a T, ou un refus nomme).
Constats resolus: items 3 et 7 (partie c) du tour executeur.
`restore_executor._settle_update_target` prenait l'entree de `fid_remap`
(l'entite que ce rewind vient de re-inserer sous la meme empreinte) sans
preuve des NOUVELLES valeurs, et `_target_matches_update_post_state`
comparait la NOUVELLE geometrie BRUTE, sans l'aligner sur le SCR actuel
de la couche.

Le mecanisme
------------
Un rewind re-insere une entite (phase 0) puis annule les modifications
anterieures de la meme empreinte (phase 1) sur le FID que la
re-insertion a recu dans le tampon. Cette entree du remap etait la seule
cible ecrite sans preuve:

  * modification sans nouvelle geometrie (attributs seuls): aucune
    verification. Sur un shapefile renumerote, l'empreinte fid:1 de la
    modification designait C003 au moment de l'edition, et fid:1 de la
    suppression designait C001: l'executeur ecrivait les anciennes
    valeurs de C003 sur C001 re-inseree, et C003 gardait sa valeur
    d'apres. Deux entites fausses, "APPLIED";
  * modification avec geometrie, sur une couche reprojetee depuis la
    capture: la geometrie d'apres (en degres, SCR de capture) etait
    comparee brute a l'entite re-inseree (ecrite en Lambert-93 par la
    re-insertion, qui aligne). Toujours differentes: le remap etait
    ecarte, la cle cherchee dans le fournisseur, ou l'entite n'est pas
    encore, et la modification refusee "target_absent": le rewind
    committait l'entite dans son etat d'apres, pas celui de T.

Ce que l'utilisateur voit
-------------------------
M1: apres le rewind, C001 porte le nom de C003 d'avant l'edition, et
C003 n'est pas revenue. M2: l'entite revient, mais a sa position et avec
son nom d'APRES la modification, alors qu'il a demande l'etat d'avant.

Cas (tampon d'edition reel, commit reel, logique du StrictRestoreRunner)
------------------------------------------------------------------------
    M1  shapefile: re-insertion de C001 (suppression sous fid:1), puis
        annulation de 'nom: old3 -> new3' journalisee sous fid:1 (C003 a
        l'epoque), sans geometrie. C001 garde 'un'; C003 (au fid 1,
        seule a porter 'new3') revient a 'old3'.
    M2  GeoPackage en EPSG:2154, evenements captures en EPSG:4326 (le
        fichier a ete reprojete en place): re-insertion de X (cle 7),
        puis annulation de 'nom: avant -> apres' + deplacement. X revient
        a 'avant', a la position d'avant (reprojetee), APPLIED.

Verdict avant correctif: FAIL. Apres: PASS.

Revue du tour executeur (BLOCKER): la NOUVELLE valeur doit aussi
DISCRIMINER
--------------------------------------------------------------------
Le correctif de M1 exigeait que l'entree du remap porte les nouvelles
valeurs, sans exiger qu'elles la distinguent. M1 ne passait que parce que
'new3' n'etait porte par personne d'autre. Si C001, re-inseree, porte par
hasard la NOUVELLE valeur de la modification journalisee sous fid:1 (C003
a l'epoque), une seule valeur egale suffisait: C001 recevait l'ancienne
valeur de C003, C003 gardait sa valeur d'apres, les deux resultats
"APPLIED". Un statut, un drapeau, une categorie: n'importe quelle colonne
a peu de valeurs le declenche. Meme exposition sur une GeoPackage dont la
cle a change de mains entre la modification et la suppression.

L'entree du remap est desormais une NOMINATION comme une autre: elle est
prise quand l'etat d'apres la designe SEULE (aucune autre entite de la
couche, tampon compris, ne le porte en entier), ou, pour une cle, quand
le journal prouve que la cle n'a pas change de mains entre la
modification et la suppression que ce rewind vient de compenser.
Sinon, deux entites portent l'etat d'apres et rien ne dit laquelle a ete
modifiee: refus nomme, rien d'ecrit (ni sur l'entite re-inseree, ni sur
une autre, fut-elle "prouvee" par sa rarete dans l'etat commite: M6).

    M3  M1 avec une valeur PARTAGEE: C003 'nom: old3 -> un' sous fid:1,
        C001 re-inseree porte aussi 'un'. C001 garde 'un'; C003 recoit
        'old3' ou la modification est refusee par son nom.
    M4  meme cas sur un entier a deux valeurs ('val: 0 -> 1', C001 a 1).
    M7  DEUX valeurs partagees ('nom: x -> un' et 'val: 0 -> 1', C001 a
        'un' et 1): deux valeurs ne sont pas rares parce qu'elles sont
        deux, elles sont comptees (la combinaison, tampon compris).
    M5  GeoPackage, cle 7: 'val: 0 -> 1' puis suppression de E, les deux
        au journal (la valeur 1 est portee par les autres lignes). Rewind
        avec le journal: la cle est continue entre les deux, E revient a
        0, APPLIED. M5c: le meme sans journal: refus nomme.
    M5r le journal montre la cle 7 supprimee puis donnee a INTRUS entre
        la modification (sur E) et la suppression (d'INTRUS): INTRUS
        re-inseree n'est pas l'entite modifiee, elle garde val 1, refus.
    M6  garde: sans journal, la cle 7 est tenue dans le fichier par
        INTRUS qui porte 'RARE' (seule dans l'etat commite), et E
        re-inseree porte 'RARE' aussi. Ecarter le remap puis laisser la
        cle decider ecrirait sur INTRUS (cle + rarete = STRONG): INTRUS
        doit garder 'RARE'.

Verdict avant correctif (tour de revue): FAIL (M3, M4, M7 et M5r
ecrivent sur la mauvaise entite et annoncent APPLIED; M5c et M6 ecrivent
sur l'entite re-inseree sans preuve, APPLIED); M5 passe avant et doit le
rester. Code d'origine: FAIL 1/15 (aucun journal accepte). M6 echoue
aussi quand le resolveur decide seul apres avoir ecarte le remap (INTRUS
ecrite), M7 quand deux valeurs sont acceptees sans etre comptees (C001
ecrite). Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_remap_update_proof"
INVARIANT = "I-3"

_SCHEMA = (("code", "string"), ("nom", "string"), ("val", "integer"))
_SHP_FIELDS = ("code:string(12)", "nom:string(20)", "val:integer")
# The shapefile as the user left it: C003 sits at fid 1 and carries the
# value its edit produced ('new3'); C001 is absent (deleted).
_SHP_ROWS = [(["C000", "zero", 0], "POINT(0 0)"),
             (["C003", "new3", 30], "POINT(30 15)"),
             (["C002", "deux", 20], "POINT(20 10)")]
# Degrees captured before the file was rewritten in Lambert-93.
_OLD_DEG = (2.30, 48.80)
_NEW_DEG = (2.35, 48.85)


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _ev(eid, op, identity, payload, geom=None, new_geom=None, crs="EPSG:2154",
        minute=0):
    fx = _fx()
    from recoverland.core.identity import compute_entity_fingerprint
    ident = json.dumps(identity)
    return fx.audit_event(
        event_id=eid, operation_type=op, feature_identity_json=ident,
        entity_fingerprint=compute_entity_fingerprint(ident),
        attributes_json=json.dumps(payload), geometry_wkb=geom,
        new_geometry_wkb=new_geom, geometry_type="Point", crs_authid=crs,
        field_schema_json=fx.schema(*_SCHEMA),
        created_at=f"2026-09-01T09:{minute:02d}:00+00:00",
        event_schema_version=7)


def _mini_rewind(layer, actions, tid, journal=None):
    """The StrictRestoreRunner loop (one buffer, one remap, soft/hard).

    *journal*: handed to every action, as the runner does, when given.
    """
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.restore_service import _classify_restore_result
    remap, results = {}, []
    extra = {"journal": journal} if journal is not None else {}
    layer.startEditing()
    for comp_op, event in actions:
        result = dict(_apply_via_buffer(layer, comp_op, event, remap,
                                        trace_id=tid, **extra))
        result["_class"] = _classify_restore_result(result)
        results.append(result)
        if not result.get("success") and result["_class"] not in (
                "target_absent", "geometry_drift"):
            layer.rollBack()
            return {"results": results, "committed": False}
    committed = bool(layer.commitChanges())
    if not committed:
        layer.rollBack()
    return {"results": results, "committed": committed}


def _rows(path, name=""):
    fx = _fx()
    layer = fx.open_layer(path, name)
    out = {}
    for f in layer.getFeatures():
        geom = f.geometry()
        point = None
        if geom is not None and not geom.isNull():
            p = geom.asPoint()
            point = (round(p.x(), 1), round(p.y(), 1))
        out[str(f["code"])] = (str(f["nom"]), point)
    del layer
    return out


def _vals(path, name=""):
    """{code: val} du fichier, relu par une couche neuve."""
    fx = _fx()
    layer = fx.open_layer(path, name)
    out = {str(f["code"]): f["val"] for f in layer.getFeatures()}
    del layer
    return out


def _journal():
    import sqlite3
    from recoverland.core.sqlite_schema import initialize_schema
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _journalled(conn, event):
    """Ecrit *event* au journal et le relit (event_id attribue par SQLite)."""
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
        AUDIT_EVENT_INSERT_COLUMNS,
    )
    from recoverland.core.search_service import get_event_by_id
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    cur = conn.execute(sql, tuple(getattr(event, c)
                                  for c in AUDIT_EVENT_INSERT_COLUMNS))
    conn.commit()
    return get_event_by_id(conn, cur.lastrowid)


_KEY7 = {"fid": 7, "pk_field": "fid", "pk_value": 7}
_GPKG_ROWS = [(1, ["C000", "zero", 1], "POINT(0 0)"),
              (2, ["C002", "deux", 1], "POINT(20 10)")]


def _gpkg_case(tmp, name, rows, story, with_journal, tid):
    """Rewind [re-insertion de la suppression, annulation de U], GeoPackage.

    Cle fid 7. *story* = [(op, payload, wkt ou None, minute)]: le PREMIER
    est U (la modification), le DERNIER la suppression que le rewind
    compense; tout est journalise quand *with_journal*.
    """
    from osgeo import ogr
    fx = _fx()
    fields = [("code", ogr.OFTString), ("nom", ogr.OFTString),
              ("val", ogr.OFTInteger)]
    path = fx.make_gpkg(tmp, name, ogr.wkbPoint, fields, rows, epsg=2154)
    layer = fx.open_layer(path, name)
    conn = _journal()
    events = []
    for i, (op, payload, wkt, minute) in enumerate(story):
        event = _ev(900 + i, op, _KEY7, payload,
                    geom=fx.wkb_wkt(wkt) if wkt else None, minute=minute)
        events.append(_journalled(conn, event) if with_journal else event)
    out = _mini_rewind(layer, [("INSERT", events[-1]),
                               ("UPDATE", events[0])], tid,
                       journal=conn if with_journal else None)
    del layer
    conn.close()
    out["after"] = _vals(path, name)
    names = fx.open_layer(path, name)
    out["names"] = {str(f["code"]): str(f["nom"]) for f in names.getFeatures()}
    del names
    return out


def _l93(x, y):
    from qgis.core import (QgsCoordinateReferenceSystem,
                           QgsCoordinateTransform, QgsProject, QgsPointXY)
    tr = QgsCoordinateTransform(QgsCoordinateReferenceSystem("EPSG:4326"),
                                QgsCoordinateReferenceSystem("EPSG:2154"),
                                QgsProject.instance())
    p = tr.transform(QgsPointXY(x, y))
    return (round(p.x(), 1), round(p.y(), 1))


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rs_rup_")
    flog(f"rs_remap_update_proof setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tmp, tid = ctx.data["tmp"], ctx.trace_id
    out = {}
    try:
        # M1: attribute-only UPDATE whose fingerprint named C003 back then.
        path = fx.make_shapefile(tmp, "m1", "Point", _SHP_FIELDS, _SHP_ROWS,
                                 crs="EPSG:2154")
        layer = fx.open_layer(path)
        deleted = _ev(11, "DELETE", {"fid": 1},
                      {"all_attributes": {"code": "C001", "nom": "un",
                                          "val": 10}},
                      geom=fx.wkb_wkt("POINT(10 5)"), minute=40)
        edited = _ev(12, "UPDATE", {"fid": 1},
                     {"changed_only": {"nom": {"old": "old3",
                                               "new": "new3"}}},
                     minute=20)
        out["m1"] = _mini_rewind(layer, [("INSERT", deleted),
                                         ("UPDATE", edited)], tid)
        del layer
        out["m1_after"] = _rows(path)

        # M2: GeoPackage reprojected since the capture (4326 -> 2154).
        from osgeo import ogr
        fields = [("code", ogr.OFTString), ("nom", ogr.OFTString),
                  ("val", ogr.OFTInteger)]
        x0, y0 = _l93(2.0, 48.5)
        gpath = fx.make_gpkg(tmp, "m2", ogr.wkbPoint, fields, [
            (1, ["C000", "zero", 0], f"POINT({x0} {y0})"),
        ], epsg=2154)
        layer = fx.open_layer(gpath, "m2")
        ident = {"fid": 7, "pk_field": "fid", "pk_value": 7}
        deleted = _ev(21, "DELETE", ident,
                      {"all_attributes": {"fid": 7, "code": "X",
                                          "nom": "apres", "val": 7}},
                      geom=fx.wkb_point(*_NEW_DEG), crs="EPSG:4326",
                      minute=40)
        edited = _ev(22, "UPDATE", ident,
                     {"changed_only": {"nom": {"old": "avant",
                                               "new": "apres"}}},
                     geom=fx.wkb_point(*_OLD_DEG),
                     new_geom=fx.wkb_point(*_NEW_DEG), crs="EPSG:4326",
                     minute=20)
        out["m2"] = _mini_rewind(layer, [("INSERT", deleted),
                                         ("UPDATE", edited)], tid)
        del layer
        out["m2_after"] = _rows(gpath, "m2")
        out["m2_expected_old"] = _l93(*_OLD_DEG)
        out["m2_expected_new"] = _l93(*_NEW_DEG)

        # M3: M1 where C001, re-inserted, also carries the NEW value 'un'
        # of the edit journalled under fid:1 (C003 back then).
        rows = [(["C000", "zero", 0], "POINT(0 0)"),
                (["C003", "un", 30], "POINT(30 15)"),
                (["C002", "deux", 20], "POINT(20 10)")]
        path = fx.make_shapefile(tmp, "m3", "Point", _SHP_FIELDS, rows,
                                 crs="EPSG:2154")
        layer = fx.open_layer(path)
        deleted = _ev(31, "DELETE", {"fid": 1},
                      {"all_attributes": {"code": "C001", "nom": "un",
                                          "val": 10}},
                      geom=fx.wkb_wkt("POINT(10 5)"), minute=40)
        edited = _ev(32, "UPDATE", {"fid": 1},
                     {"changed_only": {"nom": {"old": "old3",
                                               "new": "un"}}},
                     minute=20)
        out["m3"] = _mini_rewind(layer, [("INSERT", deleted),
                                         ("UPDATE", edited)], tid)
        del layer
        out["m3_after"] = _rows(path)

        # M4: the same on a flag-like integer ('val: 0 -> 1', C001 has 1).
        rows = [(["C000", "zero", 0], "POINT(0 0)"),
                (["C003", "trois", 1], "POINT(30 15)"),
                (["C002", "deux", 0], "POINT(20 10)")]
        path = fx.make_shapefile(tmp, "m4", "Point", _SHP_FIELDS, rows,
                                 crs="EPSG:2154")
        layer = fx.open_layer(path)
        deleted = _ev(41, "DELETE", {"fid": 1},
                      {"all_attributes": {"code": "C001", "nom": "un",
                                          "val": 1}},
                      geom=fx.wkb_wkt("POINT(10 5)"), minute=40)
        edited = _ev(42, "UPDATE", {"fid": 1},
                     {"changed_only": {"val": {"old": 0, "new": 1}}},
                     minute=20)
        out["m4"] = _mini_rewind(layer, [("INSERT", deleted),
                                         ("UPDATE", edited)], tid)
        del layer
        out["m4_after"] = _vals(path)

        # M7: TWO shared values ('nom: x -> un' and 'val: 0 -> 1' on C003,
        # C001 re-inserted with nom 'un' and val 1).
        rows = [(["C000", "zero", 0], "POINT(0 0)"),
                (["C003", "un", 1], "POINT(30 15)"),
                (["C002", "deux", 0], "POINT(20 10)")]
        path = fx.make_shapefile(tmp, "m7", "Point", _SHP_FIELDS, rows,
                                 crs="EPSG:2154")
        layer = fx.open_layer(path)
        deleted = _ev(71, "DELETE", {"fid": 1},
                      {"all_attributes": {"code": "C001", "nom": "un",
                                          "val": 1}},
                      geom=fx.wkb_wkt("POINT(10 5)"), minute=40)
        edited = _ev(72, "UPDATE", {"fid": 1},
                     {"changed_only": {"nom": {"old": "x", "new": "un"},
                                       "val": {"old": 0, "new": 1}}},
                     minute=20)
        out["m7"] = _mini_rewind(layer, [("INSERT", deleted),
                                         ("UPDATE", edited)], tid)
        del layer
        out["m7_after"] = _vals(path)
        out["m7_names"] = {code: nom for code, (nom, _p) in
                           _rows(path).items()}

        # M5 / M5c: E (key 7) 'val: 0 -> 1', then deleted; 1 is carried by
        # every other row. M5r: key 7 given to INTRUS in between.
        upd = ("UPDATE", {"changed_only": {"val": {"old": 0, "new": 1}}},
               None, 20)
        del_e = ("DELETE", {"all_attributes": {"fid": 7, "code": "E",
                                               "nom": "e", "val": 1}},
                 "POINT(10 5)", 40)
        out["m5"] = _gpkg_case(tmp, "m5", _GPKG_ROWS, [upd, del_e], True,
                               tid)
        out["m5c"] = _gpkg_case(tmp, "m5c", _GPKG_ROWS, [upd, del_e], False,
                                tid)
        intrus = {"fid": 7, "code": "INTRUS", "nom": "intrus", "val": 1}
        out["m5r"] = _gpkg_case(tmp, "m5r", _GPKG_ROWS, [
            upd,
            ("DELETE", {"all_attributes": {"fid": 7, "code": "E", "nom": "e",
                                           "val": 1}}, "POINT(10 5)", 25),
            ("INSERT", {"all_attributes": intrus}, "POINT(40 20)", 26),
            ("DELETE", {"all_attributes": intrus}, "POINT(40 20)", 40),
        ], True, tid)

        # M6: INTRUS holds key 7 in the file and carries 'RARE', alone in
        # the committed state; E, re-inserted (its snapshot has no key: a
        # fresh fid), carries 'RARE' too. No journal.
        out["m6"] = _gpkg_case(tmp, "m6", _GPKG_ROWS + [
            (7, ["INTRUS", "RARE", 9], "POINT(40 20)")], [
            ("UPDATE", {"changed_only": {"nom": {"old": "a",
                                                 "new": "RARE"}}}, None, 20),
            ("DELETE", {"all_attributes": {"code": "E", "nom": "RARE",
                                           "val": 3}}, "POINT(10 5)", 40),
        ], False, tid)
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    ctx.data["out"] = out
    summary = {k: [(r.get("status"), r.get("reason_code"))
                   for r in v["results"]]
               for k, v in out.items() if isinstance(v, dict) and "results" in v}
    flog(f"rs_remap_update_proof run: trace_id={tid} {summary} "
         f"error={out.get('error')}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = [("no_crash", "error" not in out,
            f"{out.get('error')} {out.get('trace', '')}")]

    m1 = out.get("m1_after") or {}
    m1_res = (out.get("m1") or {}).get("results") or []
    res.append((
        "m1_reinserted_entity_keeps_its_own_values",
        (m1.get("C001") or ("?",))[0] == "un",
        f"C001 apres le rewind={m1.get('C001')} (attendu 'un'), "
        f"resultats={[(r.get('status'), r.get('reason_code')) for r in m1_res]}"
        f": l'entree du remap a recu les anciennes valeurs de C003 sans "
        f"que ses nouvelles valeurs ('new3') aient ete verifiees.",
    ))
    res.append((
        "m1_update_reaches_the_entity_carrying_the_new_state",
        (m1.get("C003") or ("?",))[0] == "old3",
        f"C003 apres le rewind={m1.get('C003')} (attendu 'old3'): la "
        f"modification appartient a l'entite qui porte encore 'new3'.",
    ))

    m2 = out.get("m2_after") or {}
    m2_res = (out.get("m2") or {}).get("results") or [{}, {}]
    upd = m2_res[-1] if m2_res else {}
    x = m2.get("X") or ("?", None)
    want = out.get("m2_expected_old")
    near = (x[1] is not None and want is not None
            and abs(x[1][0] - want[0]) < 1.0 and abs(x[1][1] - want[1]) < 1.0)
    res.append((
        "m2_update_on_the_reinserted_feature_is_applied",
        upd.get("status") == "APPLIED",
        f"resultat de la modification={upd}: la nouvelle geometrie capturee "
        f"en EPSG:4326 doit etre comparee a l'entite re-inseree dans le SCR "
        f"de la couche (EPSG:2154), pas brute.",
    ))
    res.append((
        "m2_state_at_t_lands_in_the_file",
        x[0] == "avant" and near,
        f"X apres le rewind={x} attendu=('avant', ~{want}) (etat d'apres="
        f"('apres', ~{out.get('m2_expected_new')})): le rewind ne doit pas "
        f"committer l'entite dans son etat d'apres la modification.",
    ))

    refusals = ("target_ambiguous", "target_unverifiable")

    def _upd(key):
        results = (out.get(key) or {}).get("results") or []
        return results[-1] if len(results) >= 2 else {}

    def _named_refusal(result):
        return (result.get("success") is False
                and result.get("reason_code") in refusals)

    def _seen(key):
        return [(r.get("status"), r.get("reason_code"), r.get("message"))
                for r in (out.get(key) or {}).get("results") or []]

    m3 = out.get("m3_after") or {}
    res.append((
        "m3_shared_value_does_not_designate_the_reinserted_entity",
        (m3.get("C001") or ("un",))[0] == "un",
        f"C001 apres le rewind={m3.get('C001')} (attendu 'un', son propre "
        f"nom), resultats={_seen('m3')}: l'entree du remap a ete prise sur "
        f"une valeur ('un') que C003 porte aussi, et C001 a recu l'ancienne "
        f"valeur de C003.",
    ))
    res.append((
        "m3_update_written_on_c003_or_refused_by_name",
        (m3.get("C003") or ("?",))[0] == "old3"
        or (_named_refusal(_upd("m3"))
            and (m3.get("C003") or ("?",))[0] == "un"),
        f"C003 apres le rewind={m3.get('C003')}, modification="
        f"{_seen('m3')[-1:]}: soit C003 revient a 'old3', soit la "
        f"modification est refusee par son nom; jamais un APPLIED qui "
        f"laisse C003 a 'un'.",
    ))
    m4 = out.get("m4_after") or {}
    res.append((
        "m4_flag_value_does_not_designate_the_reinserted_entity",
        m4.get("C001", 1) == 1,
        f"C001 apres le rewind val={m4.get('C001')} (attendu 1), resultats="
        f"{_seen('m4')}: un entier a deux valeurs ne designe pas l'entite "
        f"re-inseree, qui a recu l'ancienne valeur de C003.",
    ))
    res.append((
        "m4_update_written_on_c003_or_refused_by_name",
        m4.get("C003") == 0
        or (_named_refusal(_upd("m4")) and m4.get("C003") == 1),
        f"C003 apres le rewind val={m4.get('C003')}, modification="
        f"{_seen('m4')[-1:]}: soit C003 revient a 0, soit la modification "
        f"est refusee par son nom.",
    ))
    m7v = out.get("m7_after") or {}
    m7n = out.get("m7_names") or {}
    res.append((
        "m7_two_shared_values_do_not_designate_the_reinserted_entity",
        m7v.get("C001", 1) == 1 and m7n.get("C001", "un") == "un"
        and (m7v.get("C003") == 0
             or (_named_refusal(_upd("m7")) and m7v.get("C003") == 1)),
        f"C001 apres le rewind=({m7n.get('C001')!r}, {m7v.get('C001')}) "
        f"(attendu ('un', 1)), C003=({m7n.get('C003')!r}, "
        f"{m7v.get('C003')}), resultats={_seen('m7')}: deux valeurs que "
        f"C003 porte aussi ne designent pas plus l'entite re-inseree "
        f"qu'une seule; les compter, pas les supposer rares.",
    ))
    m5 = out.get("m5") or {}
    res.append((
        "m5_continuous_key_restores_the_reinserted_entity",
        [r.get("status") for r in m5.get("results") or []]
        == ["APPLIED", "APPLIED"] and m5.get("committed") is True
        and (m5.get("after") or {}).get("E") == 0,
        f"cle 7 continue entre la modification et la suppression (le "
        f"journal le prouve): resultats={_seen('m5')} fichier="
        f"{m5.get('after')} (attendu E a 0). La valeur 1 est partagee, "
        f"mais l'histoire de la cle designe l'entite re-inseree: la "
        f"refuser ici rendrait ce rewind impossible.",
    ))
    m5c = out.get("m5c") or {}
    res.append((
        "m5c_without_journal_refused_by_name",
        _named_refusal(_upd("m5c"))
        and (m5c.get("after") or {}).get("E") == 1,
        f"le meme rewind sans journal: modification={_seen('m5c')[-1:]} "
        f"fichier={m5c.get('after')}: rien ne distingue cette couche de "
        f"M5r, la valeur 1 partagee ne prouve pas l'entite.",
    ))
    m5r = out.get("m5r") or {}
    res.append((
        "m5r_key_given_away_between_edit_and_delete_refused",
        _named_refusal(_upd("m5r"))
        and (m5r.get("after") or {}).get("INTRUS") == 1,
        f"le journal montre la cle 7 supprimee puis donnee a INTRUS entre "
        f"la modification (sur E) et la suppression (d'INTRUS): "
        f"modification={_seen('m5r')[-1:]} fichier={m5r.get('after')} "
        f"(attendu INTRUS a 1): INTRUS a recu l'ancienne valeur de E.",
    ))
    m6 = (out.get("m6") or {}).get("names") or {}
    res.append((
        "m6_committed_key_holder_not_written_in_place_of_the_remap",
        m6.get("INTRUS") == "RARE" and _named_refusal(_upd("m6")),
        f"INTRUS (cle 7 dans le fichier, seule a porter 'RARE' dans l'etat "
        f"commite) et E re-inseree portent l'etat d'apres: modification="
        f"{_seen('m6')[-1:]} fichier={m6} (attendu INTRUS 'RARE' et un "
        f"refus nomme). Ecarter le remap puis laisser la cle decider ecrit "
        f"sur INTRUS.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_remap_update_proof run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
