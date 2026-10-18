"""rs_pk_corroboration_proof - RecoverLand validation runtime.

Invariant: I-3 (identite prouvee avant toute ecriture) et fail-loud (un
refus nomme sa vraie cause).
Constats resolus: revue du tour precedent, BLOCKER "une cle primaire
corroboree par NULL == NULL ou par une valeur partagee est STRONG"
(`_prove_update_state`, `_classify_insert_candidate`); item B
(`restore_executor._settle_update_target` croit une cle sur parole sur le
chemin REWIND); MINOR "le rewind d'un UPDATE confond lecture echouee,
ambiguite et absence"; item H (code mort qui croit une cle sans preuve).

Le mecanisme
------------
Une cle primaire n'est pas une identite tant que le contenu ne confirme
pas: INTEGER PRIMARY KEY sans AUTOINCREMENT redonne le plus grand id,
les sequences sont remises a zero. La preuve v7 exigeait donc que
l'entite designee par la cle porte l'etat de l'evenement, mais pour une
cle TOUTE concordance comptait: NULL == NULL, ou une seule valeur que la
moitie de la couche porte ("statut: actif"). La cle reutilisee par une
entite sans nom recevait l'ancien nom de l'autre, etiquetee STRONG. Sur le
chemin du rewind c'etait pire: `_settle_update_target` posait
``trusted = remapped is not None or has_pk`` et n'appelait AUCUNE preuve
pour une cle. Enfin la relocalisation du rewind rendait None pour "lecture
echouee", "plusieurs candidats" et "introuvable": une coupure de connexion
devenait un refus mou "target_absent", jamais annule par le runner strict.

Ce que l'utilisateur voit
-------------------------
Il restaure la modification "nom: ORIG_OLD -> (vide)" de la parcelle 3.
La cle 3 a ete redonnee a INTRUS, dont le nom est vide lui aussi: INTRUS
est renommee ORIG_OLD, la restauration annonce "Reverted" (STRONG). Il
revient a hier sur la couche: INTRUS est renommee ORIG_OLD, le rewind
annonce "APPLIED". Pendant une coupure PostGIS, le rewind compte les
UPDATE "cible absente" et valide le reste de la couche autour d'eux.

Cas
---
    B1  restauration, cle reutilisee, seule concordance NULL == NULL
    B2  restauration, cle reutilisee, une seule valeur partagee (actif)
    B3  annulation d'une restauration, cle reutilisee, concordance NULL
    R1  rewind, cle reutilisee, contenu contredit (nom INTRUS != ORIG)
    R2  rewind, cle reutilisee, seule concordance NULL == NULL
    R3  rewind, cle reutilisee, une seule valeur partagee
    I1  annulation d'un INSERT (restauration et rewind), cle reutilisee,
        instantane tout-NULL sans geometrie
    R4  rewind pendant une coupure: refus dur scan_failed, pas "absent"
    R5  rewind, etat NEW porte par deux points empiles: target_ambiguous
    C1  temoin restauration: la cle designe l'entite, valeur NEW unique
    C2  temoin rewind: idem, par le tampon d'edition
    C3  temoin rewind: deux valeurs NEW, dont une partagee
    C4  temoin INSERT: la cle designe l'entite inseree, instantane complet
    F1  antithese FID, restore_batch de deux UPDATE de la MEME entite R
        (shapefile reorganise entre les deux captures): E1 remet R a la
        valeur V, E2 (FID perime -> un etranger qui porte V) ne doit pas
        ecrire sur l'etranger. L'unicite d'une nomination par FID compte
        TOUTE la couche, y compris ce que le lot vient d'ecrire: R, ecrit
        par E1, est peut-etre l'entite que le FID ne designe plus.
    H   plus aucune fonction de restore_executor ne croit une cle sur
        parole (`_find_existing_by_identity` retiree)

Revue suivante: la rarete mesuree sur une couche deja reecrite
--------------------------------------------------------------
Pour une cle, la rarete d'UNE valeur decide si elle corrobore la cle. Le
tour precedent la mesurait sans les entites que l'operation avait deja
ecrites, ENTIERES, alors qu'elle n'en avait ecrit que quelques colonnes:
le reste est la donnee de l'utilisateur et peut porter la meme valeur. Et
une entite que l'operation venait de SUPPRIMER disparaissait du compte.
Un evenement anterieur du meme lot (restauration, annulation ou rewind)
qui touchait un autre porteur de la valeur rendait donc la valeur
"unique": la cle reutilisee par INTRUS etait ecrite, STRONG / APPLIED, et
le verdict dependait de l'ordre des evenements. La rarete se mesure
maintenant sur la couche TELLE QUE L'UTILISATEUR L'A LAISSEE: l'etat
valide du fournisseur pour le rewind (son tampon n'y est pas), les
pre-images des entites ecrites ou supprimees pour la restauration et
l'annulation (elles ecrivent directement dans le fournisseur).

Ce que l'utilisateur voit: il restaure deux modifications ensemble, le
code de l'entite 5 et le nom de la parcelle 3. La cle 3 a ete redonnee a
INTRUS, qui porte le nom V, comme l'entite 5. Seule, la seconde est
refusee; en lot apres la premiere, INTRUS est renommee ORIG_OLD, et le
journal dit "restaure".

    P0  temoin: restore_batch [E2] seul (cle 3 reutilisee, 'nom -> V'
        porte aussi par l'entite 5): refuse
    P1  restore_batch [E1 (cle 5, code seul), E2]: E2 refuse, INTRUS
        intacte, E1 applique
    P3  ordre inverse [E2, E1]: meme issue que P1
    P2  rewind, E1 puis E2 dans le meme tampon: E2 refuse, INTRUS
        intacte apres le commit; P2c temoin E2 seul dans le tampon
    U0  temoin: annulation de la restauration de E2 seule: refusee
    U1  undo_restore_batch qui defait E1 d'abord: E2 toujours refusee
    Q0  temoin: restore_batch [UPDATE cle 3] seul: refuse
    Q1  restore_batch [annulation de l'INSERT de D (nom V), UPDATE cle 3]:
        D supprimee, l'UPDATE refuse (V etait porte par D et INTRUS)
    Q2  rewind: suppression de D puis UPDATE cle 3 dans le meme tampon
    P4  cle reutilisee, 'statut: inactif -> actif, type: regard -> borne':
        deux valeurs que toute la couche porte ne corroborent pas la cle
        (la COMBINAISON est comptee, pas le nombre de valeurs)
    P5  temoin de P4: deux valeurs dont la combinaison est unique:
        la restauration passe (STRONG)

Mesure avant ce correctif: code du tour precedent 37/43 (P1, P2, U1, Q1,
Q2 et P4 ecrivent sur INTRUS; les temoins P0, P3, P2c, U0, Q0 refusent
deja), code d'origine 9/43.

Verdict avant correctif (code du tour precedent et code d'origine): FAIL.
F1 passait avec le code du tour precedent (la regle FID compte toute la
couche) et echoue sur le code d'origine; il garde la restriction de
l'exclusion aux cles (mesure: une exclusion etendue aux FID ecrit U sur
l'etranger). Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_pk_corroboration_proof"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SHP_FIELDS = ["nom:string(20)", "code:string(20)"]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _fields(with_statut=False):
    from osgeo import ogr
    out = [("nom", ogr.OFTString), ("code", ogr.OFTString)]
    if with_statut:
        out.append(("statut", ogr.OFTString))
    return out


def _gpkg(tmpdir, name, fields, rows, reused):
    """GeoPackage *name*, puis la cle 3 redonnee a une autre entite.

    *rows* = [(fid, [valeurs], wkt)], *reused* = ([valeurs], wkt): valeurs
    NULL permises (ce que `gpkg_delete_and_reuse` ne sait pas ecrire).
    """
    from osgeo import ogr
    path = _fx().make_gpkg(tmpdir, name, ogr.wkbPoint, fields, rows)
    ds = ogr.Open(path, 1)
    lyr = ds.GetLayerByName(name)
    lyr.DeleteFeature(3)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetFID(3)
    values, wkt = reused
    for (fname, _t), value in zip(fields, values):
        if value is None:
            feat.SetFieldNull(fname)
        else:
            feat.SetField(fname, value)
    if wkt:
        feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
    lyr.CreateFeature(feat)
    feat = None
    ds.FlushCache()
    ds = None
    return path


def _schema(with_statut=False):
    pairs = [("fid", "integer"), ("nom", "string"), ("code", "string")]
    if with_statut:
        pairs.append(("statut", "string"))
    return _fx().schema(*pairs)


def _pk(fid=3):
    return {"fid": fid, "pk_field": "fid", "pk_value": fid}


def _update(eid, changed, with_statut=False, identity=None, old_wkt=None,
            new_wkt=None):
    fx = _fx()
    identity = identity or _pk()
    fp = (f"pk:fid={identity['pk_value']}" if "pk_value" in identity
          else f"fid:{identity['fid']}")
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE",
        field_schema_json=(_schema(with_statut) if "pk_value" in identity
                           else fx.schema(("nom", "string"),
                                          ("code", "string"))),
        feature_identity_json=json.dumps(identity), entity_fingerprint=fp,
        attributes_json=json.dumps({"changed_only": {
            k: {"old": old, "new": new} for k, (old, new) in changed.items()}}),
        geometry_wkb=fx.wkb_wkt(old_wkt) if old_wkt else None,
        new_geometry_wkb=fx.wkb_wkt(new_wkt) if new_wkt else None,
        geometry_type="Point", event_schema_version=7)


def _insert(eid, attrs, wkt=None, key=3):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="INSERT", field_schema_json=_schema(),
        feature_identity_json=json.dumps(_pk(key)),
        entity_fingerprint=f"pk:fid={key}",
        attributes_json=json.dumps({"all_attributes": attrs}),
        geometry_wkb=fx.wkb_wkt(wkt) if wkt else None,
        geometry_type="Point", event_schema_version=7)


_STT_FIELDS = ("nom", "statut", "type")


def _stt_fields():
    from osgeo import ogr
    return [(name, ogr.OFTString) for name in _STT_FIELDS]


def _update_stt(eid, changed):
    """UPDATE de la cle 3 sur la couche (nom, statut, type)."""
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE",
        field_schema_json=fx.schema(("fid", "integer"),
                                    *[(n, "string") for n in _STT_FIELDS]),
        feature_identity_json=json.dumps(_pk()), entity_fingerprint="pk:fid=3",
        attributes_json=json.dumps({"changed_only": {
            k: {"old": old, "new": new} for k, (old, new) in changed.items()}}),
        geometry_type="Point", event_schema_version=7)


def _batch(layer, events, tid):
    """restore_batch, rendu comme ce que le rapport annonce."""
    from recoverland.core.restore_service import restore_batch
    try:
        report = restore_batch(layer, events, trace_id=tid)
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        return {"crash": repr(exc)}
    return {"succeeded": sorted(report.succeeded),
            "failed": {int(k): str(v)[:200] for k, v in report.failed.items()}}


def _undo(layer, events):
    """undo_restore_batch (il defait les evenements du dernier au premier)."""
    from recoverland.core.restore_service import undo_restore_batch
    try:
        report = undo_restore_batch(layer, events)
    except Exception as exc:  # noqa: BLE001
        return {"crash": repr(exc)}
    return {"succeeded": sorted(report.succeeded),
            "failed": {int(k): str(v)[:200] for k, v in report.failed.items()}}


def _rewind(layer, actions, tid):
    """Plusieurs compensations du rewind dans UN tampon, puis le commit."""
    from recoverland.core.restore_executor import _apply_via_buffer
    layer.startEditing()
    results = []
    for comp_op, event in actions:
        try:
            results.append(dict(_apply_via_buffer(layer, comp_op, event, {},
                                                  trace_id=tid)))
        except Exception as exc:  # noqa: BLE001
            results.append({"success": None, "crash": repr(exc)})
    committed = bool(layer.commitChanges())
    if not committed:
        layer.rollBack()
    return {"results": results, "commit": committed}


def _row(rows, fid):
    """Ligne de *fid* dans une photographie ``file_rows(with_fid=True)``."""
    return next((r for r in rows or [] if r[0] == fid), None)


class _DeadProvider:
    """Provider dont les lectures echouent comme une connexion coupee."""

    def __init__(self, real):
        self._real = real

    def getFeatures(self, request=None):
        from qgis.core import QgsFeatureIterator
        return QgsFeatureIterator()

    def __getattr__(self, name):
        return getattr(self._real, name)


class _DeadLayer:
    """Couche dont toutes les lectures rendent un iterateur invalide."""

    def __init__(self, real):
        self._real = real
        self._provider = _DeadProvider(real.dataProvider())

    def dataProvider(self):
        return self._provider

    def getFeatures(self, request=None):
        from qgis.core import QgsFeatureIterator
        return QgsFeatureIterator()

    def __getattr__(self, name):
        return getattr(self._real, name)


_BASE = [(1, ["A", "K1"], "POINT (1 1)"), (3, ["ORIG", "K3"], "POINT (3 3)")]
_BASE_STATUT = [(1, ["A", "K1", "actif"], "POINT (1 1)"),
                (2, ["B", "K2", "actif"], "POINT (2 2)"),
                (3, ["ORIG", "K3", "actif"], "POINT (3 3)")]


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmpdir = tempfile.mkdtemp(prefix="rl_rs_pkc_")
    ctx.data["tmpdir"] = tmpdir
    null_intrus = ([None, "K9"], "POINT (8 8)")
    named_intrus = (["INTRUS", "K9"], "POINT (8 8)")
    statut_intrus = (["INTRUS", "K9", "actif"], "POINT (8 8)")
    specs = {
        "b1": (False, _BASE, null_intrus),
        "b2": (True, _BASE_STATUT, statut_intrus),
        "b3": (False, _BASE, null_intrus),
        "r1": (False, _BASE, named_intrus),
        "r2": (False, _BASE, null_intrus),
        "r3": (True, _BASE_STATUT, statut_intrus),
        "i1": (False, _BASE, ([None, None], "POINT (8 8)")),
        "i2": (False, _BASE, ([None, None], "POINT (8 8)")),
        "c1": (False, _BASE, named_intrus),
        "c2": (False, _BASE, named_intrus),
        "c3": (True, _BASE_STATUT, statut_intrus),
    }
    paths = {}
    for key, (statut, rows, reused) in specs.items():
        paths[key] = _gpkg(tmpdir, key, _fields(statut), rows, reused)
    # C4: the key still names the inserted feature itself.
    paths["c4"] = fx.make_gpkg(tmpdir, "c4", ogr.wkbPoint, _fields(), _BASE)
    # R4 / R5: shapefiles, FID identity.
    paths["r4"] = fx.make_shapefile(tmpdir, "r4", "Point", _SHP_FIELDS, [
        (["GARE", "C1"], "Point (1 1)"), (["PONT", "C2"], "Point (2 2)")])
    paths["r5"] = fx.make_shapefile(tmpdir, "r5", "Point", _SHP_FIELDS, [
        (["S1", "C7"], "Point (5 5)"), (["S2", "C7"], "Point (5 5)"),
        (["F", "C8"], "Point (8 8)")])
    # F1: R carries W (its latest value), the stranger E carries V.
    paths["f1"] = fx.make_shapefile(tmpdir, "f1", "Point", _SHP_FIELDS, [
        (["R", "W"], "Point (1 1)"), (["E", "V"], "Point (2 2)")])
    # P / U / Q: the key 3 went to INTRUS, which carries the name V; one
    # other feature carries V too (user data the operation may touch).
    v_intrus = (["V", "K9"], "POINT (8 8)")
    p_rows = [(1, ["A", "K1"], "POINT (1 1)"),
              (3, ["ORIG", "K3"], "POINT (3 3)"),
              (5, ["V", "K5"], "POINT (5 5)")]
    u_rows = [(1, ["A", "K1"], "POINT (1 1)"),
              (3, ["ORIG", "K3"], "POINT (3 3)"),
              (5, ["V", "K5_OLD"], "POINT (5 5)")]
    q_rows = [(1, ["A", "K1"], "POINT (1 1)"),
              (3, ["ORIG", "K3"], "POINT (3 3)"),
              (6, ["V", "KD"], "POINT (6 6)")]
    for key in ("p0", "p1", "p3", "p2", "p2c"):
        paths[key] = _gpkg(tmpdir, key, _fields(), p_rows, v_intrus)
    for key in ("u0", "u1"):
        paths[key] = _gpkg(tmpdir, key, _fields(), u_rows, v_intrus)
    for key in ("q0", "q1", "q2"):
        paths[key] = _gpkg(tmpdir, key, _fields(), q_rows, v_intrus)
    # P4 / P5: every row is actif and borne, the key 3 went to INTRUS.
    stt_rows = [(1, ["A", "actif", "borne"], "POINT (1 1)"),
                (2, ["B", "actif", "borne"], "POINT (2 2)"),
                (3, ["ORIG", "actif", "borne"], "POINT (3 3)")]
    paths["p4"] = _gpkg(tmpdir, "p4", _stt_fields(), stt_rows,
                        (["INTRUS", "actif", "borne"], "POINT (8 8)"))
    paths["p5"] = _gpkg(tmpdir, "p5", _stt_fields(), stt_rows,
                        (["INTRUS", "actif", "puits"], "POINT (8 8)"))
    ctx.data["paths"] = paths
    ctx.data["before"] = {key: _rows(key, path) for key, path in paths.items()}
    flog(f"rs_pk_corroboration_proof setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _rows(key, path):
    fx = _fx()
    if path.endswith(".gpkg"):
        return fx.file_rows(path, key, with_fid=True)
    return fx.file_rows(path)


def _open(key, path):
    fx = _fx()
    return fx.open_layer(path, key if path.endswith(".gpkg") else "")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_updated_feature, restore_inserted_feature, undo_prepare,
        undo_apply_one, _classify_restore_result,
    )
    from recoverland.core.restore_executor import _apply_via_buffer
    fx = _fx()
    tid = ctx.trace_id
    paths = ctx.data["paths"]
    res = {}
    flog(f"rs_pk_corroboration_proof run start: trace_id={tid}", "INFO")

    def _safe(fn, *args):
        try:
            return dict(fn(*args))
        except Exception as exc:  # noqa: BLE001 - un crash est un resultat
            return {"success": None, "crash": repr(exc)}

    res["b1"] = _safe(restore_updated_feature, _open("b1", paths["b1"]),
                      _update(601, {"nom": ("ORIG_OLD", None)}))
    res["b2"] = _safe(restore_updated_feature, _open("b2", paths["b2"]),
                      _update(602, {"statut": ("inactif", "actif")}, True))
    ev_b3 = _update(603, {"nom": (None, "ORIG_NEW")})
    layer = _open("b3", paths["b3"])
    res["b3"] = _safe(undo_apply_one, layer, ev_b3, undo_prepare(layer, [ev_b3]))

    res["r1"] = fx.buffer_apply(_open("r1", paths["r1"]), "UPDATE",
                                _update(604, {"nom": ("ORIG_OLD", "ORIG")}),
                                tid)
    res["r2"] = fx.buffer_apply(_open("r2", paths["r2"]), "UPDATE",
                                _update(605, {"nom": ("ORIG_OLD", None)}), tid)
    res["r3"] = fx.buffer_apply(_open("r3", paths["r3"]), "UPDATE",
                                _update(606, {"statut": ("inactif", "actif")},
                                        True), tid)

    ev_i = _insert(607, {"fid": 3, "nom": None, "code": None})
    res["i1"] = _safe(restore_inserted_feature, _open("i1", paths["i1"]), ev_i)
    res["i2"] = fx.buffer_apply(_open("i2", paths["i2"]), "DELETE", ev_i, tid)

    # R4: the connection drops during the rewind of an UPDATE.
    real = _open("r4", paths["r4"])
    dead = _DeadLayer(real)
    ev_r4 = _update(608, {"nom": ("GARE_AVANT", "GARE")},
                    identity={"fid": 0})
    real.startEditing()
    try:
        res["r4"] = dict(_apply_via_buffer(dead, "UPDATE", ev_r4, {},
                                           trace_id=tid))
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        res["r4"] = {"success": None, "crash": repr(exc)}
    real.rollBack()
    res["r4_class"] = _classify_restore_result(res["r4"])

    # R5: FID 9 is gone, the NEW geometry sits on two stacked points.
    res["r5"] = fx.buffer_apply(
        _open("r5", paths["r5"]), "UPDATE",
        _update(609, {}, identity={"fid": 9}, old_wkt="Point (0 0)",
                new_wkt="Point (5 5)"), tid)

    res["c1"] = _safe(restore_updated_feature, _open("c1", paths["c1"]),
                      _update(610, {"nom": ("INTRUS_AVANT", "INTRUS")}))
    res["c2"] = fx.buffer_apply(_open("c2", paths["c2"]), "UPDATE",
                                _update(611, {"nom": ("INTRUS_AVANT",
                                                      "INTRUS")}), tid)
    res["c3"] = fx.buffer_apply(
        _open("c3", paths["c3"]), "UPDATE",
        _update(612, {"nom": ("X_AVANT", "INTRUS"),
                      "statut": ("inactif", "actif")}, True), tid)
    res["c4"] = _safe(restore_inserted_feature, _open("c4", paths["c4"]),
                      _insert(613, {"fid": 3, "nom": "ORIG", "code": "K3"},
                              "POINT (3 3)"))

    # F1: E1 (V -> W, captured after the repack, FID of R) then E2
    # (U -> V, captured before it: its FID now names the stranger E).
    from recoverland.core.restore_service import restore_batch
    layer = _open("f1", paths["f1"])
    fids = {f["nom"]: f.id() for f in layer.getFeatures()}
    ev_e1 = _update(614, {"code": ("V", "W")}, identity={"fid": fids["R"]})
    ev_e2 = _update(615, {"code": ("U", "V")}, identity={"fid": fids["E"]})
    try:
        report = restore_batch(layer, [ev_e1, ev_e2], trace_id=tid)
        res["f1"] = {"succeeded": sorted(report.succeeded),
                     "failed": {int(k): str(v)[:160]
                                for k, v in report.failed.items()}}
    except Exception as exc:  # noqa: BLE001
        res["f1"] = {"crash": repr(exc)}

    # P: E1 writes the code of feature 5 (its name V stays the user's),
    # E2 restores 'nom: ORIG_OLD -> V' on the reused key 3.
    ev_p1 = _update(620, {"code": ("K5_OLD", "K5")}, identity=_pk(5))
    ev_p2 = _update(621, {"nom": ("ORIG_OLD", "V")})
    res["p0"] = _batch(_open("p0", paths["p0"]), [ev_p2], tid)
    res["p1"] = _batch(_open("p1", paths["p1"]), [ev_p1, ev_p2], tid)
    res["p3"] = _batch(_open("p3", paths["p3"]), [ev_p2, ev_p1], tid)
    res["p2"] = _rewind(_open("p2", paths["p2"]),
                        [("UPDATE", ev_p1), ("UPDATE", ev_p2)], tid)
    res["p2c"] = _rewind(_open("p2c", paths["p2c"]), [("UPDATE", ev_p2)], tid)

    # U: undo of two restores; the undo walks the list backwards, so E1
    # (code of feature 5) is undone first.
    ev_u1 = _update(622, {"code": ("K5_OLD", "K5")}, identity=_pk(5))
    ev_u2 = _update(623, {"nom": ("V", "ORIG_NEW")})
    res["u0"] = _undo(_open("u0", paths["u0"]), [ev_u2])
    res["u1"] = _undo(_open("u1", paths["u1"]), [ev_u2, ev_u1])

    # Q: the INSERT of D (name V) is undone first, then the UPDATE of key 3.
    ev_ins_d = _insert(624, {"fid": 6, "nom": "V", "code": "KD"},
                       "POINT (6 6)", key=6)
    ev_q = _update(625, {"nom": ("ORIG_OLD", "V")})
    res["q0"] = _batch(_open("q0", paths["q0"]), [ev_q], tid)
    res["q1"] = _batch(_open("q1", paths["q1"]), [ev_ins_d, ev_q], tid)
    res["q2"] = _rewind(_open("q2", paths["q2"]),
                        [("DELETE", ev_ins_d), ("UPDATE", ev_q)], tid)

    # P4 / P5: two captured values; what counts is how many features carry
    # the COMBINATION.
    res["p4"] = _safe(restore_updated_feature, _open("p4", paths["p4"]),
                      _update_stt(626, {"statut": ("inactif", "actif"),
                                        "type": ("regard", "borne")}))
    res["p5"] = _safe(restore_updated_feature, _open("p5", paths["p5"]),
                      _update_stt(627, {"statut": ("inactif", "actif"),
                                        "type": ("regard", "puits")}))

    ctx.data["res"] = res
    ctx.data["after"] = {key: _rows(key, path) for key, path in paths.items()}
    ctx.data["executor_source"] = (
        _PLUGIN_ROOT / "core" / "restore_executor.py").read_text(
            encoding="utf-8", errors="replace")
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"), value.get("identity_level"))
               for key, value in res.items() if isinstance(value, dict)}
    flog(f"rs_pk_corroboration_proof run end: trace_id={tid} "
         f"results={summary}", "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


_REFUSALS = ("target_unverifiable", "target_ambiguous")


def _batch_refused(report, eid) -> bool:
    return eid in (report.get("failed") or {}) \
        and eid not in (report.get("succeeded") or [])


def _baseline_assertions(res, before, after):
    """Cas P / U / Q / P4 / P5: rarete mesuree sur la couche laissee."""
    out = []

    def intrus_kept(key):
        return (_row(after.get(key), 3) == _row(before.get(key), 3)
                and _row(after.get(key), 3) is not None)

    damage = ("INTRUS, qui tient la cle 3 redonnee, perd son nom au profit "
              "de celui d'une autre entite, sous un rapport 'restaure'")
    for key, label in (
            ("p0", "restore_batch [E2] seul"),
            ("p1", "restore_batch [E1 (code de l'entite 5), E2]"),
            ("p3", "restore_batch [E2, E1] (ordre inverse)")):
        report = res.get(key) or {}
        out.append((
            f"{key}_reused_key_refused_whatever_the_order",
            _batch_refused(report, 621) and intrus_kept(key),
            f"{label}: rapport={report} ligne 3 avant="
            f"{_row(before.get(key), 3)} apres={_row(after.get(key), 3)}. "
            f"Avant le lot, V est porte par INTRUS ET par l'entite 5: la "
            f"valeur ne corrobore pas la cle. E1 n'a ecrit que le code de 5, "
            f"son nom V reste la donnee de l'utilisateur: {damage}.",
        ))
    for key in ("p1", "p3"):
        report = res.get(key) or {}
        row5 = _row(after.get(key), 5) or ()
        out.append((
            f"{key}_other_event_still_applied",
            620 in (report.get("succeeded") or []) and "K5_OLD" in row5,
            f"E1 (code de l'entite 5, valeur unique) doit passer quel que "
            f"soit l'ordre: rapport={report} ligne 5={row5}",
        ))

    p2 = res.get("p2") or {}
    p2_results = p2.get("results") or [{}, {}]
    out.append((
        "p2_rewind_same_buffer_refuses_reused_key",
        len(p2_results) == 2 and p2_results[0].get("success") is True
        and p2_results[1].get("success") is False
        and p2_results[1].get("reason_code") in _REFUSALS
        and p2.get("commit") is True and intrus_kept("p2")
        and "K5_OLD" in (_row(after.get("p2"), 5) or ()),
        f"rewind, E1 puis E2 dans le meme tampon: resultats={p2_results} "
        f"commit={p2.get('commit')} ligne 3 apres={_row(after.get('p2'), 3)}. "
        f"Le tampon porte deja la compensation de E1: la compter (ou "
        f"l'ecarter entiere) change le verdict de E2; la couche que "
        f"l'utilisateur a laissee (l'etat valide du fournisseur) porte V "
        f"deux fois: {damage}.",
    ))
    p2c = (res.get("p2c") or {}).get("results") or [{}]
    out.append((
        "p2c_control_rewind_alone_refused",
        p2c[0].get("success") is False
        and p2c[0].get("reason_code") in _REFUSALS and intrus_kept("p2c"),
        f"temoin: E2 seul dans le tampon: resultat={p2c}",
    ))

    for key, label in (("u0", "annulation de E2 seule"),
                       ("u1", "undo_restore_batch qui defait E1 d'abord")):
        report = res.get(key) or {}
        out.append((
            f"{key}_undo_reused_key_refused",
            _batch_refused(report, 623) and intrus_kept(key),
            f"{label}: rapport={report} ligne 3 apres="
            f"{_row(after.get(key), 3)}. L'annulation ecrirait ORIG_NEW sur "
            f"INTRUS: V est porte par INTRUS et par l'entite 5 (dont E1 "
            f"n'a defait que le code).",
        ))
    u1 = res.get("u1") or {}
    out.append((
        "u1_other_undo_applied",
        622 in (u1.get("succeeded") or [])
        and "K5" in (_row(after.get("u1"), 5) or ()),
        f"l'annulation de E1 (code K5_OLD unique) doit passer: rapport={u1} "
        f"ligne 5={_row(after.get('u1'), 5)}",
    ))

    for key, label in (("q0", "restore_batch [UPDATE cle 3] seul"),
                       ("q1", "restore_batch [annulation de l'INSERT de D, "
                              "UPDATE cle 3]")):
        report = res.get(key) or {}
        out.append((
            f"{key}_deleted_holder_still_counts",
            _batch_refused(report, 625) and intrus_kept(key),
            f"{label}: rapport={report} ligne 3 apres="
            f"{_row(after.get(key), 3)}. Avant le lot, V est porte par D et "
            f"par INTRUS; que le lot supprime D d'abord ne rend pas V "
            f"unique: {damage}.",
        ))
    q1 = res.get("q1") or {}
    out.append((
        "q1_insert_undo_applied",
        624 in (q1.get("succeeded") or [])
        and _row(after.get("q1"), 6) is None,
        f"l'annulation de l'INSERT de D (instantane complet, geometrie "
        f"comprise) doit passer: rapport={q1} lignes={after.get('q1')}",
    ))
    q2 = res.get("q2") or {}
    q2_results = q2.get("results") or [{}, {}]
    out.append((
        "q2_rewind_deleted_holder_still_counts",
        len(q2_results) == 2 and q2_results[0].get("success") is True
        and q2_results[1].get("success") is False
        and q2_results[1].get("reason_code") in _REFUSALS
        and q2.get("commit") is True and intrus_kept("q2")
        and _row(after.get("q2"), 6) is None,
        f"rewind, suppression de D puis UPDATE cle 3 dans le meme tampon: "
        f"resultats={q2_results} commit={q2.get('commit')} lignes="
        f"{after.get('q2')}. {damage}.",
    ))

    p4 = res.get("p4") or {}
    out.append((
        "p4_two_layer_wide_values_do_not_corroborate",
        p4.get("success") is False and p4.get("reason_code") in _REFUSALS
        and after.get("p4") == before.get("p4"),
        f"restauration 'statut: inactif -> actif, type: regard -> borne' sur "
        f"la cle 3 redonnee a INTRUS: resultat={p4} apres={after.get('p4')}. "
        f"Toute la couche est 'actif'/'borne': deux valeurs que tout le "
        f"monde porte ne prouvent rien de plus qu'une; INTRUS passerait "
        f"'inactif'/'regard'.",
    ))
    p5 = res.get("p5") or {}
    row3 = _row(after.get("p5"), 3) or ()
    out.append((
        "p5_control_unique_combination_restored",
        p5.get("success") is True and p5.get("identity_level") == "STRONG"
        and "inactif" in row3 and "regard" in row3,
        f"temoin: 'statut: inactif -> actif, type: regard -> puits', seule "
        f"INTRUS porte (actif, puits): resultat={p5} ligne 3={row3}. La "
        f"combinaison est unique, la restauration doit passer.",
    ))
    return out


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    res = ctx.data.get("res") or {}
    before = ctx.data.get("before") or {}
    after = ctx.data.get("after") or {}

    refusals = {
        "b1": ("restauration 'nom: ORIG_OLD -> vide', cle 3 redonnee a une "
               "entite sans nom", "INTRUS renommee ORIG_OLD"),
        "b2": ("restauration 'statut: inactif -> actif', toute la couche "
               "est active", "INTRUS passee inactive"),
        "b3": ("annulation de la restauration 'nom: vide -> ORIG_NEW', cle "
               "redonnee a une entite sans nom", "INTRUS renommee ORIG_NEW"),
        "r1": ("rewind 'nom: ORIG_OLD -> ORIG', la cle 3 designe INTRUS",
               "INTRUS renommee ORIG_OLD"),
        "r2": ("rewind 'nom: ORIG_OLD -> vide', cle redonnee a une entite "
               "sans nom", "INTRUS renommee ORIG_OLD"),
        "r3": ("rewind 'statut: inactif -> actif', toute la couche est "
               "active", "INTRUS passee inactive"),
        "i1": ("annulation d'un INSERT tout-NULL, cle redonnee a une entite "
               "vide", "INTRUS supprimee"),
        "i2": ("rewind d'un INSERT tout-NULL, cle redonnee a une entite "
               "vide", "INTRUS supprimee"),
    }
    for key, (label, damage) in refusals.items():
        result = res.get(key) or {}
        out.append((
            f"{key}_layer_untouched",
            after.get(key) == before.get(key),
            f"{label}: apres={after.get(key)} avant={before.get(key)}: "
            f"{damage}, alors que rien ne prouve que la cle designe encore "
            f"l'entite de l'evenement.",
        ))
        out.append((
            f"{key}_is_a_named_refusal",
            result.get("success") is False
            and result.get("reason_code") in ("target_unverifiable",
                                              "target_ambiguous"),
            f"{label}: resultat={result}. Une concordance NULL == NULL ou une "
            f"seule valeur partagee ne prouve rien: attendu un refus nomme "
            f"(target_unverifiable), jamais 'Reverted' ni 'APPLIED'.",
        ))

    r4 = res.get("r4") or {}
    out.append((
        "r4_failed_read_is_a_hard_failure",
        r4.get("success") is False and r4.get("reason_code") == "scan_failed"
        and res.get("r4_class") == "failed",
        f"rewind d'un UPDATE pendant une coupure: resultat={r4} classe="
        f"{res.get('r4_class')}. Une lecture echouee ne prouve pas l'absence: "
        f"'target_absent' est un refus mou, jamais annule, et le runner "
        f"strict valide le reste de la couche autour.",
    ))
    out.append((
        "r4_layer_untouched",
        after.get("r4") == before.get("r4"),
        f"apres={after.get('r4')} avant={before.get('r4')}",
    ))
    r5 = res.get("r5") or {}
    out.append((
        "r5_ambiguity_is_named",
        r5.get("success") is False
        and r5.get("reason_code") == "target_ambiguous",
        f"rewind: l'etat NEW est porte par deux points empiles: resultat="
        f"{r5}. Le refus doit dire 'ambigu' (deux candidats), pas "
        f"'cible absente': l'utilisateur chercherait une entite disparue.",
    ))
    out.append((
        "r5_layer_untouched",
        after.get("r5") == before.get("r5"),
        f"apres={after.get('r5')} avant={before.get('r5')}: un des points "
        f"empiles a ete deplace au hasard",
    ))

    c1 = res.get("c1") or {}
    out.append((
        "c1_control_key_with_unique_value_restored",
        c1.get("success") is True and c1.get("identity_level") == "STRONG"
        and any("INTRUS_AVANT" in repr(r) for r in after.get("c1") or []),
        f"temoin restauration: resultat={c1} apres={after.get('c1')}. La cle "
        f"designe INTRUS, qui porte la valeur NEW 'INTRUS', unique dans la "
        f"couche: la preuve suffit, la restauration doit passer.",
    ))
    c2 = res.get("c2") or {}
    out.append((
        "c2_control_rewind_key_with_unique_value_applied",
        c2.get("success") is True and not c2.get("skipped")
        and any("INTRUS_AVANT" in repr(r) for r in after.get("c2") or []),
        f"temoin rewind: resultat={c2} apres={after.get('c2')}",
    ))
    c3 = res.get("c3") or {}
    out.append((
        "c3_control_rewind_two_values_applied",
        c3.get("success") is True and not c3.get("skipped")
        and any("X_AVANT" in repr(r) and "inactif" in repr(r)
                for r in after.get("c3") or []),
        f"temoin rewind a deux valeurs NEW (nom unique, statut partage): "
        f"resultat={c3} apres={after.get('c3')}",
    ))
    c4 = res.get("c4") or {}
    out.append((
        "c4_control_insert_undone",
        c4.get("success") is True and not c4.get("skipped")
        and not any("ORIG" in repr(r) for r in after.get("c4") or []),
        f"temoin INSERT: resultat={c4} apres={after.get('c4')}. La cle et "
        f"l'instantane complet (geometrie comprise) designent l'entite: "
        f"elle doit etre supprimee.",
    ))

    f1 = res.get("f1") or {}
    out.append((
        "f1_batch_write_does_not_make_a_stranger_unique",
        f1.get("succeeded") == [614] and 615 in (f1.get("failed") or {})
        and ("E", "V", "Point (2 2)") in (after.get("f1") or [])
        and ("R", "V", "Point (1 1)") in (after.get("f1") or []),
        f"lot E1 puis E2 sur R, FID de E2 perime (designe l'etranger E): "
        f"resultat={f1} fichier apres={after.get('f1')}. E1 rend V a R; E "
        f"porte aussi V. Ne pas compter R (ecrit par le lot) rendrait E "
        f"'unique' et E2 ecrirait U sur un etranger: la valeur V est "
        f"partagee, E2 doit etre refuse.",
    ))

    out.extend(_baseline_assertions(res, before, after))

    src = ctx.data.get("executor_source") or ""
    out.append((
        "h_no_key_trusting_helper_left",
        "def _find_existing_by_identity" not in src
        and "def _occupant_shares_evidence" not in src
        and "def _apply_insert_if_already_present" not in src,
        "restore_executor garde des fonctions mortes dont l'une "
        "(_find_existing_by_identity) croit une cle primaire sans lire "
        "l'entite: rebranchee, elle ecraserait l'entite qui a recu la cle.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_pk_corroboration_proof.*trace_id={ctx.trace_id}",
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
