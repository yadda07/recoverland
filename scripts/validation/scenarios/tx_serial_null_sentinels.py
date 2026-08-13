"""tx_serial_null_sentinels - RecoverLand validation runtime.

Invariant: BL-SER-P0-02 (NULL, chaine vide, 0 et False sont quatre
informations differentes et doivent le rester du debut a la fin).
Cause racine: `serialization._is_null()` traite comme NULL toute chaine
egale a `QgsApplication.nullRepresentation()`, dont la valeur par defaut
sous QGIS est le texte ``"NULL"``.

Le trou que ce scenario prouve
------------------------------
`nullRepresentation()` est un reglage d'AFFICHAGE : il indique comment la
table attributaire doit peindre une case vide. `_is_null()` s'en sert
comme d'un test de VALEUR. Consequence, sur un jeu de donnees ou le texte
"NULL" est une valeur legitime (import CSV, code d'etat, champ libre
saisi a la main, table livree par un tiers) :

  1. Cote capture ANCIENNE valeur. Un champ contenant "NULL" est journalise
     comme NULL. Le rewind rend donc une case vide la ou l'utilisateur
     avait le texte "NULL" : la valeur est perdue, definitivement, et le
     journal ne garde aucune trace de ce qu'elle etait.
  2. Cote capture NOUVELLE valeur. Si l'utilisateur SAISIT "NULL", le
     journal enregistre que l'etat d'apres est vide. Sur une couche sans
     cle primaire (shapefile, GeoJSON), la preuve de cible ajoutee dans
     `_verify_update_target` compare l'etat vivant ("NULL") a l'etat
     d'apres journalise (None), conclut que la cible n'est pas la bonne, et
     REFUSE. L'utilisateur ne peut plus annuler sa propre faute de frappe :
     l'entite devient definitivement non restaurable.

Le meme mecanisme frappe les flottants speciaux : `_serialize_float`
remplace NaN et les infinis par None. Une mesure "non mesurable" devient
"non renseignee" au retour, sans avertissement.

En revanche NULL / "" / 0 / False restent bien distincts a la traversee
du JSON : ce scenario le verifie aussi, parce qu'une garantie acquise
compte autant qu'un defaut.

Scenario (vrai GeoPackage + vrai shapefile, dossiers temporaires):
    partie 1 - fonctions pures : serialize_value sur les sentinelles,
        flottants speciaux, matrice de distinction de compute_update_delta.
    partie 2 - GPKG (cle primaire reelle), 5 entites, un cycle
        edition -> delta -> journal SQLite -> restore_updated_feature ->
        relecture, une entite par sentinelle.
    partie 3 - shapefile (identite FID seule) : l'utilisateur saisit le
        texte "NULL", puis tente de revenir en arriere.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_serial_null_sentinels"
INVARIANT = "BL-SER-P0-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_GPKG_LAYER = "etats"
_DS_FP = "tx_sns_datasource"
_PROJ_FP = "tx_sns_project"

_GPKG_URI = ("Point?crs=EPSG:4326"
             "&field=etat:string(0)"
             "&field=flag:boolean"
             "&field=nb:long")
_SHP_URI = "Point?crs=EPSG:4326&field=etat:string(30)&field=code:string(10)"

_LITERAL_NULL = "NULL"


def _short(value, limit: int = 60) -> str:
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + "..."


# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------

def _write_layer(mem, tmpdir: str, driver: str, basename: str,
                 layer_name: str = ""):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    path = str(Path(tmpdir) / basename)
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = driver
    if layer_name:
        opts.layerName = layer_name
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"{driver} write failed: {err}")
    uri = f"{path}|layername={layer_name}" if layer_name else path
    layer = QgsVectorLayer(uri, basename, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"layer invalid: {uri}")
    return layer


def _make_gpkg(tmpdir: str):
    """GPKG a 5 entites, une par sentinelle a eprouver."""
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY

    mem = QgsVectorLayer(_GPKG_URI, "seed", "memory")
    layer = _write_layer(mem, tmpdir, "GPKG", "etats.gpkg", _GPKG_LAYER)

    # (etat, flag, nb) -- la valeur eprouvee est en gras dans le commentaire
    rows = [
        ("", True, 1),                  # A: chaine VIDE
        (_LITERAL_NULL, True, 1),       # B: texte litteral "NULL"
        ("libre", False, 1),            # C: booleen FAUX
        ("libre", True, None),          # D: entier NULL
        ("libre", True, 0),             # E: entier ZERO
    ]
    feats = []
    for i, (etat, flag, nb) in enumerate(rows):
        f = QgsFeature(layer.fields())
        f.setAttribute("etat", etat)
        f.setAttribute("flag", flag)
        f.setAttribute("nb", nb)
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, 45.0)))
        feats.append(f)
    ok, _ = layer.dataProvider().addFeatures(feats)
    if not ok:
        raise RuntimeError(f"gpkg seed failed: {layer.dataProvider().errors()}")
    layer.reload()
    return layer


def _make_shp(tmpdir: str):
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY

    mem = QgsVectorLayer(_SHP_URI, "seed", "memory")
    mem.startEditing()
    f = QgsFeature(mem.fields())
    f.setAttributes(["libre", "P1"])
    f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(7.0, 45.0)))
    mem.addFeature(f)
    mem.commitChanges()
    return _write_layer(mem, tmpdir, "ESRI Shapefile", "etats.shp")


# ----------------------------------------------------------------------
# Journal
# ----------------------------------------------------------------------

def _insert_update_event(conn, layer, fid: int, delta_json: str,
                         identity_json: str) -> int:
    from recoverland.core.serialization import serialize_field_schema
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    row = (
        _PROJ_FP, _DS_FP, layer.id(), layer.name(), "ogr",
        identity_json, "UPDATE", delta_json,
        None, "Point", "EPSG:4326",
        serialize_field_schema(layer.fields()), "tester", None,
        datetime.now(timezone.utc).isoformat(), None,
        compute_entity_fingerprint(identity_json), 5, None, None,
    )
    cur = conn.execute(sql, row)
    conn.commit()
    return cur.lastrowid


def _feature_by_fid(layer, fid: int):
    from qgis.core import QgsFeatureRequest
    return next(layer.getFeatures(QgsFeatureRequest(fid)), None)


def _edit_and_restore(ctx, conn, layer, fid: int, field: str, new_value,
                      identity_json: str) -> dict:
    """Un cycle complet edition -> delta -> journal -> restauration.

    Retourne un dict decrivant ce que l'utilisateur observe.
    """
    from recoverland.core.serialization import (
        serialize_attributes, compute_update_delta,
    )
    from recoverland.core.search_service import (
        get_event_by_id, reconstruct_new_attributes,
    )
    from recoverland.core.restore_service import restore_updated_feature

    names = [f.name() for f in layer.fields()]
    feat = _feature_by_fid(layer, fid)
    reference = feat[field]                       # la verite: l'etat d'avant
    old_attrs = serialize_attributes(feat, names)

    idx = layer.fields().indexOf(field)
    layer.dataProvider().changeAttributeValues({fid: {idx: new_value}})
    layer.reload()
    feat2 = _feature_by_fid(layer, fid)
    live_after_edit = feat2[field]
    new_attrs = serialize_attributes(feat2, names)

    delta = compute_update_delta(old_attrs, new_attrs, [field])
    out = {
        "field": field,
        "reference": reference,
        "live_after_edit": live_after_edit,
        "delta": delta,
        "captured_old": old_attrs.get(field),
        "captured_new": new_attrs.get(field),
    }
    if delta is None:
        # Aucun evenement ne sera ecrit: la modification est invisible.
        out["restore_result"] = None
        out["restored"] = live_after_edit
        return out

    eid = _insert_update_event(conn, layer, fid, delta, identity_json)
    event = get_event_by_id(conn, eid)
    out["event_id"] = eid
    out["journal_post_state"] = reconstruct_new_attributes(event).get(field)
    out["restore_result"] = restore_updated_feature(layer, event)
    layer.reload()
    feat3 = _feature_by_fid(layer, fid)
    out["restored"] = feat3[field] if feat3 is not None else "<entite absente>"
    return out


# ----------------------------------------------------------------------
# setup / run / assertions
# ----------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_sns_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _make_gpkg(tmpdir)
    ctx.data["shp"] = _make_shp(tmpdir)

    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ctx.data["conn"] = conn

    from qgis.core import QgsApplication
    ctx.data["null_repr"] = QgsApplication.nullRepresentation()

    flog(
        f"tx_serial_null_sentinels setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} null_representation={ctx.data['null_repr']!r}",
        "INFO",
    )


def _run_pure(ctx):
    from recoverland.core.serialization import (
        serialize_value, compute_update_delta,
    )

    sentinels = {}
    for label, value in (("none", None), ("empty", ""), ("zero", 0),
                         ("false", False), ("literal_null", _LITERAL_NULL),
                         ("literal_null_lower", "null"), ("zero_float", 0.0)):
        ser = serialize_value(value)
        # aller-retour JSON reel, comme dans attributes_json
        back = json.loads(json.dumps({"v": ser}))["v"]
        sentinels[label] = {
            "ser": ser, "ser_type": type(ser).__name__,
            "json_back": back, "json_type": type(back).__name__,
        }
    ctx.data["sentinels"] = sentinels

    floats = {}
    for label, value in (("nan", float("nan")), ("pos_inf", float("inf")),
                         ("neg_inf", float("-inf"))):
        floats[label] = serialize_value(value)
    ctx.data["special_floats"] = floats

    matrix = {}
    for label, old, new in (
        ("none_to_empty", None, ""),
        ("empty_to_none", "", None),
        ("zero_to_false", 0, False),
        ("false_to_zero", False, 0),
        ("one_to_true", 1, True),
        ("empty_to_zero", "", 0),
        ("zero_to_text_zero", 0, "0"),
    ):
        matrix[label] = compute_update_delta({"f": old}, {"f": new}, ["f"])
    ctx.data["delta_matrix"] = matrix


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.identity import compute_feature_identity
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_updated_feature

    flog(f"tx_serial_null_sentinels run start: trace_id={ctx.trace_id}", "INFO")

    _run_pure(ctx)

    # ---- partie 2 : GPKG, une entite par sentinelle ---------------------
    gpkg = ctx.data["gpkg"]
    conn = ctx.data["conn"]
    fids = sorted(f.id() for f in gpkg.getFeatures())
    ctx.data["gpkg_fids"] = fids

    cases = {}
    plan = [
        ("A_empty_string", fids[0], "etat", "occupe"),
        ("B_literal_null", fids[1], "etat", "occupe"),
        ("C_bool_false", fids[2], "flag", True),
        ("D_int_null", fids[3], "nb", 7),
        ("E_int_zero", fids[4], "nb", 7),
    ]
    for label, fid, field, new_value in plan:
        feat = _feature_by_fid(gpkg, fid)
        identity = compute_feature_identity(gpkg, feat)
        cases[label] = _edit_and_restore(
            ctx, conn, gpkg, fid, field, new_value, identity)
    ctx.data["gpkg_cases"] = {
        k: {kk: (_short(vv) if kk != "restore_result" else vv)
            for kk, vv in v.items()}
        for k, v in cases.items()
    }
    ctx.data["gpkg_raw"] = {
        k: {"reference": v["reference"], "restored": v["restored"],
            "delta": v["delta"], "journal_post_state": v.get("journal_post_state"),
            "restore_result": v.get("restore_result")}
        for k, v in cases.items()
    }

    # ---- partie 3 : shapefile FID-seul, saisie du texte "NULL" ----------
    shp = ctx.data["shp"]
    from recoverland.core.serialization import (
        serialize_attributes, compute_update_delta,
    )
    from recoverland.core.identity import get_identity_strength_for_layer

    ctx.data["shp_strength"] = str(get_identity_strength_for_layer(shp))
    names = [f.name() for f in shp.fields()]
    feat = next(shp.getFeatures())
    shp_fid = feat.id()
    reference = feat["etat"]
    old_attrs = serialize_attributes(feat, names)

    idx = shp.fields().indexOf("etat")
    shp.dataProvider().changeAttributeValues({shp_fid: {idx: _LITERAL_NULL}})
    shp.reload()
    feat2 = next(shp.getFeatures())
    new_attrs = serialize_attributes(feat2, names)
    delta = compute_update_delta(old_attrs, new_attrs, ["etat"])

    identity_json = json.dumps({"fid": shp_fid})
    shp_out = {
        "reference": reference,
        "live_after_edit": feat2["etat"],
        "captured_new": new_attrs.get("etat"),
        "delta": delta,
    }
    if delta is not None:
        eid = _insert_update_event(conn, shp, shp_fid, delta, identity_json)
        event = get_event_by_id(conn, eid)
        shp_out["restore_result"] = restore_updated_feature(shp, event)
        shp.reload()
        feat3 = next(shp.getFeatures(), None)
        shp_out["restored"] = feat3["etat"] if feat3 is not None else None
    else:
        shp_out["restore_result"] = None
        shp_out["restored"] = feat2["etat"]
    ctx.data["shp_case"] = shp_out

    flog(
        f"tx_serial_null_sentinels run end: trace_id={ctx.trace_id} "
        f"gpkg_cases={list(cases)} shp_restore={shp_out.get('restore_result')}",
        "INFO",
    )

    ctx.data["gpkg"] = None
    ctx.data["shp"] = None
    conn.close()
    ctx.data["conn"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    sent = ctx.data.get("sentinels") or {}
    floats = ctx.data.get("special_floats") or {}
    matrix = ctx.data.get("delta_matrix") or {}
    raw = ctx.data.get("gpkg_raw") or {}
    shp = ctx.data.get("shp_case") or {}

    # ===== Partie 1 : les quatre sentinelles restent distinctes ==========
    quad = [sent.get(k, {}).get("json_back", "<absent>")
            for k in ("none", "empty", "zero", "false")]
    typed = [(type(v).__name__, v) for v in quad]
    out.append((
        "null_empty_zero_false_stay_distinct",
        len(set(typed)) == 4,
        f"apres serialisation + JSON les quatre valeurs ne sont plus "
        f"discernables : {typed}. NULL='non renseigne', ''='renseigne mais "
        f"vide', 0='zero mesure', False='non' : les confondre change le sens "
        f"de la donnee restauree.",
    ))
    out.append((
        "false_stays_a_bool_not_an_int",
        sent.get("false", {}).get("json_type") == "bool",
        f"False est revenu en {sent.get('false', {}).get('json_type')} "
        f"({sent.get('false', {}).get('json_back')!r})",
    ))
    out.append((
        "zero_stays_an_int_not_a_bool",
        sent.get("zero", {}).get("json_type") == "int",
        f"0 est revenu en {sent.get('zero', {}).get('json_type')}",
    ))

    # ===== Partie 1 : le texte litteral "NULL" ===========================
    out.append((
        "literal_null_text_is_preserved",
        sent.get("literal_null", {}).get("ser") == _LITERAL_NULL,
        f"le texte {_LITERAL_NULL!r} est capture comme "
        f"{sent.get('literal_null', {}).get('ser')!r}. _is_null() compare la "
        f"valeur a QgsApplication.nullRepresentation() "
        f"({ctx.data.get('null_repr')!r}), qui est un reglage D'AFFICHAGE, pas "
        f"une valeur. Toute donnee contenant ce texte est journalisee vide et "
        f"perdue au rewind.",
    ))
    out.append((
        "lowercase_null_is_preserved",
        sent.get("literal_null_lower", {}).get("ser") == "null",
        f"asymetrie de casse : 'null' -> "
        f"{sent.get('literal_null_lower', {}).get('ser')!r} alors que 'NULL' "
        f"est efface. Le comportement depend d'un reglage utilisateur, donc "
        f"deux postes differents ne journalisent pas la meme chose.",
    ))

    # ===== Partie 1 : flottants speciaux =================================
    out.append((
        "special_floats_are_not_silently_nulled",
        all(v is not None for v in floats.values()),
        f"NaN et les infinis sont remplaces par NULL a la capture : {floats}. "
        f"Une mesure 'non mesurable' (division par zero, capteur sature) "
        f"devient 'non renseignee' au retour, sans avertissement ni trace.",
    ))

    # ===== Partie 1 : matrice de distinction du delta ====================
    out.append((
        "delta_sees_empty_vs_null",
        matrix.get("none_to_empty") is not None
        and matrix.get("empty_to_none") is not None,
        f"le passage NULL <-> chaine vide n'est pas detecte comme une "
        f"modification : {matrix.get('none_to_empty')} / "
        f"{matrix.get('empty_to_none')}",
    ))
    out.append((
        "delta_sees_zero_vs_false",
        matrix.get("zero_to_false") is not None
        and matrix.get("false_to_zero") is not None,
        f"0 et False sont confondus par _values_equal (0 == False en Python) : "
        f"delta 0->False={matrix.get('zero_to_false')}, "
        f"False->0={matrix.get('false_to_zero')}. Une bascule de type sur un "
        f"champ (Integer devenu Boolean apres export) passe sous le radar : "
        f"aucun evenement n'est ecrit, donc rien a restaurer plus tard.",
    ))
    out.append((
        "delta_sees_one_vs_true",
        matrix.get("one_to_true") is not None,
        f"1 et True sont confondus : delta={matrix.get('one_to_true')}",
    ))

    # ===== Partie 2 : GPKG, aller-retour par sentinelle ==================
    def _case(label, expected_desc):
        c = raw.get(label) or {}
        ref = c.get("reference")
        got = c.get("restored")
        return c, ref, got, ref == got

    c, ref, got, ok = _case("A_empty_string", "")
    out.append((
        "gpkg_empty_string_restored_as_empty_string", ok,
        f"la chaine vide n'est pas revenue : attendu={ref!r} obtenu={got!r} "
        f"(delta={_short(c.get('delta'), 120)})",
    ))

    c, ref, got, ok = _case("B_literal_null", _LITERAL_NULL)
    out.append((
        "gpkg_literal_null_restored_as_text", ok,
        f"le texte {_LITERAL_NULL!r} est perdu par le rewind : "
        f"attendu={ref!r} obtenu={got!r}. Le delta journalise ne contient "
        f"meme pas la valeur d'origine (delta={_short(c.get('delta'), 140)}) : "
        f"aucune reparation n'est possible a posteriori, l'information n'a "
        f"jamais ete ecrite.",
    ))

    c, ref, got, ok = _case("C_bool_false", False)
    out.append((
        "gpkg_false_restored_as_false",
        ok and got is False,
        f"le booleen FAUX n'est pas revenu : attendu=False obtenu={got!r}. "
        f"'non' ne doit jamais devenir 'inconnu'.",
    ))

    c, ref, got, ok = _case("D_int_null", None)
    out.append((
        "gpkg_null_int_restored_as_null",
        got is None,
        f"le NULL entier n'est pas revenu : attendu=None obtenu={got!r}. "
        f"Une case vide remplie par erreur doit pouvoir redevenir vide.",
    ))

    c, ref, got, ok = _case("E_int_zero", 0)
    out.append((
        "gpkg_zero_restored_as_zero",
        ok and got == 0 and got is not None,
        f"le zero entier n'est pas revenu : attendu=0 obtenu={got!r}",
    ))

    # ===== Partie 3 : shapefile FID-seul, saisie de "NULL" ===============
    out.append((
        "shp_fixture_is_fid_only",
        ctx.data.get("shp_strength") not in (None, "IdentityStrength.NONE"),
        f"identity strength={ctx.data.get('shp_strength')} : le scenario a "
        f"besoin d'une couche sans cle primaire, sinon la preuve de cible "
        f"n'est jamais sollicitee",
    ))
    out.append((
        "shp_journal_post_state_matches_reality",
        shp.get("captured_new") == _LITERAL_NULL,
        f"l'etat d'apres journalise est {shp.get('captured_new')!r} alors que "
        f"la couche contient {shp.get('live_after_edit')!r}. Le journal ment "
        f"sur l'etat courant : la vue REVIEW reconstruira une entite vide, et "
        f"toute preuve de cible basee sur cet etat echouera.",
    ))
    res = shp.get("restore_result") or {}
    out.append((
        "shp_user_can_undo_his_own_typo",
        res.get("success") is True,
        f"restore_updated_feature repond {res}. En saisissant le texte "
        f"{_LITERAL_NULL!r} dans un champ, l'utilisateur rend son entite "
        f"DEFINITIVEMENT non restaurable sur une couche sans cle primaire : "
        f"la preuve de cible compare l'etat vivant ({shp.get('live_after_edit')!r}) "
        f"a l'etat d'apres journalise ({shp.get('captured_new')!r}) et refuse.",
    ))
    out.append((
        "shp_value_actually_reverted",
        shp.get("restored") == shp.get("reference"),
        f"la valeur n'est pas revenue : attendu={shp.get('reference')!r} "
        f"obtenu={shp.get('restored')!r}",
    ))

    # ===== Preuve de mecanisme ===========================================
    src = (_PLUGIN_ROOT / "core" / "serialization.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "is_null_does_not_use_display_setting",
        "nullRepresentation" not in src,
        "serialization._is_null() interroge "
        "QgsApplication.nullRepresentation(), un reglage d'affichage "
        "modifiable par l'utilisateur (Options > General > Representation "
        "des valeurs NULL). La capture du journal ne doit dependre d'aucun "
        "reglage d'interface : deux postes regles differemment produiraient "
        "deux journaux differents pour la meme donnee.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_null_sentinels.*trace_id={ctx.trace_id}",
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
