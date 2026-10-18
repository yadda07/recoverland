"""rs_null_marker_versioned - RecoverLand validation runtime.

Invariant: I-8 (aucune geometrie effacee sur la foi d'une donnee
corrompue) et fail-loud (un blob illisible est refuse et nomme).
Constats resolus: item F (le marqueur "pas de geometrie" lu sans regarder
la generation de l'evenement) et MAJOR de la revue du tour precedent
(`tx_geom_corrupt_and_typemismatch::zero_length_wkb_is_not_silently_
ignored` revisee a tort: un evenement de schema 2 voyait sa geometrie
effacee et la restauration annoncee reussie).

Le mecanisme
------------
Depuis le schema 7, la capture ecrit un BLOB de longueur nulle
(`GEOMETRY_NULL_MARKER`) pour dire "l'entite n'avait pas de geometrie de
ce cote de l'edition". AUCUNE version anterieure n'a jamais ecrit un tel
blob (une geometrie NULL etait un NULL SQL): dans un evenement de schema
<= 6, un blob vide est une corruption (copie partielle du journal,
ecriture interrompue). `restore_service._is_null_marker` et
`restore_executor._buffer_insert` / `_target_matches_update_post_state`
lisaient le marqueur quelle que soit la version: la restauration ECRIVAIT
une geometrie NULLE sur la foi d'un blob corrompu, le rewind recreait une
entite sans geometrie, et la preuve de cible tenait "pas de geometrie"
pour un etat capture. `core.constants.is_null_geometry_marker(wkb,
event_schema_version)` ne reconnait le marqueur que dans un evenement de
schema >= 7.

Ce que l'utilisateur voit
-------------------------
Son journal 5.x a ete copie en partie (disque plein). Il restaure le
renommage d'un poteau: le nom revient, le point disparait de la carte, et
le plugin annonce "Reverted". Un rewind recree un poteau supprime...
invisible, sans geometrie, compte "restaure".

Cas
---
    N1  restauration d'un UPDATE schema 5, cote OLD = blob vide: refus
        corrupt_wkb, geometrie et nom intacts
    N2  rewind (compensation INSERT) d'un DELETE schema 5 a blob vide:
        refus corrupt_wkb, rien d'insere
    N3  restauration d'un DELETE schema 5 a blob vide: refus, rien d'insere
    N4  annulation d'une restauration d'INSERT schema 5 a blob vide: refus
    N5  preuve de cible: UPDATE schema 5 dont le cote NEW est un blob vide,
        seule entite sans geometrie: pas de preuve par l'absence, refus
    T1  temoin schema 7: meme UPDATE qu'en N1, la geometrie est retiree
        (le marqueur est un etat)
    T2  temoin schema 7: meme DELETE qu'en N2, l'entite revient sans
        geometrie

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_null_marker_versioned"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = ["nom:string(20)", "code:string(10)"]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _schema():
    return _fx().schema(("nom", "string"), ("code", "string"))


def _update(eid, fid, old_wkb, new_wkb, changed, version):
    return _fx().audit_event(
        event_id=eid, operation_type="UPDATE",
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}", field_schema_json=_schema(),
        attributes_json=json.dumps({"changed_only": {
            k: {"old": old, "new": new} for k, (old, new) in changed.items()}}),
        geometry_wkb=old_wkb, new_geometry_wkb=new_wkb,
        event_schema_version=version)


def _snapshot_event(eid, op, fid, wkb, version, nom="POTEAU", code="P9"):
    return _fx().audit_event(
        event_id=eid, operation_type=op,
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}", field_schema_json=_schema(),
        attributes_json=json.dumps({"all_attributes": {"nom": nom,
                                                       "code": code}}),
        geometry_wkb=wkb, event_schema_version=version)


def _fid_of(layer, nom):
    for feat in layer.getFeatures():
        if feat["nom"] == nom:
            return feat.id()
    return None


def setup(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tmp = tempfile.mkdtemp(prefix="rl_rs_nmv_")
    ctx.data["tmpdir"] = tmp
    base = [(["A", "K1"], "Point (1 1)"), (["B", "K2"], "Point (2 2)")]
    paths = {}
    for key in ("n1", "n2", "n3", "n4", "t1", "t2"):
        paths[key] = fx.make_shapefile(tmp, key, "Point", _FIELDS, base)
    # N5: B is the only feature without a geometry.
    paths["n5"] = fx.make_shapefile(tmp, "n5", "Point", _FIELDS, [
        (["A", "K1"], "Point (1 1)"), (["B", "K2"], None),
        (["C", "K3"], "Point (3 3)")])
    ctx.data["paths"] = paths
    ctx.data["before"] = {k: fx.file_rows(p) for k, p in paths.items()}
    flog(f"rs_null_marker_versioned setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_updated_feature, restore_deleted_feature, undo_prepare,
        undo_apply_one,
    )
    fx = _fx()
    d = ctx.data
    paths = d["paths"]
    tid = ctx.trace_id
    res = {}
    flog(f"rs_null_marker_versioned run start: trace_id={tid}", "INFO")

    def _safe(fn, *args):
        try:
            return dict(fn(*args))
        except Exception as exc:  # noqa: BLE001 - un crash est un resultat
            return {"success": None, "crash": repr(exc)}

    point_b = fx.wkb_wkt("Point (2 2)")
    for key, version in (("n1", 5), ("t1", 7)):
        layer = fx.open_layer(paths[key])
        res[key] = _safe(restore_updated_feature, layer, _update(
            1100 + version, _fid_of(layer, "B"), b"", point_b,
            {"nom": ("B_AVANT", "B")}, version))
        del layer

    for key, version in (("n2", 5), ("t2", 7)):
        res[key] = fx.buffer_apply(
            fx.open_layer(paths[key]), "INSERT",
            _snapshot_event(1110 + version, "DELETE", 9, b"", version), tid)

    res["n3"] = _safe(restore_deleted_feature, fx.open_layer(paths["n3"]),
                      _snapshot_event(1120, "DELETE", 9, b"", 5))
    ev_n4 = _snapshot_event(1121, "INSERT", 9, b"", 5)
    layer = fx.open_layer(paths["n4"])
    res["n4"] = _safe(undo_apply_one, layer, ev_n4,
                      undo_prepare(layer, [ev_n4]))
    del layer

    layer = fx.open_layer(paths["n5"])
    res["n5"] = _safe(restore_updated_feature, layer, _update(
        1122, _fid_of(layer, "B"), fx.wkb_wkt("Point (4 4)"), b"", {}, 5))
    del layer

    d["res"] = res
    d["after"] = {k: fx.file_rows(p) for k, p in paths.items()}
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"))
               for key, value in res.items()}
    flog(f"rs_null_marker_versioned run end: trace_id={tid} "
         f"results={summary}", "INFO")
    shutil.rmtree(d.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    res = d.get("res") or {}
    before = d.get("before") or {}
    after = d.get("after") or {}

    refusals = {
        "n1": "restauration d'un UPDATE 5.x dont le cote OLD est un blob "
              "vide (le point disparait, 'Reverted')",
        "n2": "rewind d'un DELETE 5.x a blob vide (poteau recree sans "
              "geometrie, invisible)",
        "n3": "restauration d'un DELETE 5.x a blob vide (poteau recree sans "
              "geometrie)",
        "n4": "annulation d'une restauration d'INSERT 5.x a blob vide",
    }
    for key, label in refusals.items():
        result = res.get(key) or {}
        out.append((
            f"{key}_corrupt_blob_refused",
            result.get("success") is False
            and result.get("reason_code") == "corrupt_wkb",
            f"{label}: resultat={result}. Aucune capture anterieure au schema "
            f"7 n'a ecrit de blob vide: c'est une corruption, a refuser "
            f"(corrupt_wkb), jamais un 'pas de geometrie'.",
        ))
        out.append((
            f"{key}_layer_untouched",
            after.get(key) == before.get(key),
            f"{label}: fichier avant={before.get(key)} apres={after.get(key)}",
        ))
    n5 = res.get("n5") or {}
    out.append((
        "n5_corrupt_new_side_proves_nothing",
        n5.get("success") is False and after.get("n5") == before.get("n5"),
        f"resultat={n5} fichier apres={after.get('n5')}. Le cote NEW est un "
        f"blob vide dans un evenement 5.x: il ne dit pas 'l'entite n'a plus "
        f"de geometrie', il ne dit rien. Ecrire le point sur B parce qu'elle "
        f"est la seule sans geometrie repose sur une donnee corrompue.",
    ))
    t1 = res.get("t1") or {}
    out.append((
        "t1_control_v7_marker_removes_geometry",
        t1.get("success") is True
        and ("B_AVANT", "K2", None) in (after.get("t1") or []),
        f"temoin schema 7: resultat={t1} fichier apres={after.get('t1')}. "
        f"Le marqueur dit que B n'avait pas de geometrie avant l'edition: la "
        f"restauration la retire.",
    ))
    t2 = res.get("t2") or {}
    out.append((
        "t2_control_v7_marker_reinserts_without_geometry",
        t2.get("success") is True
        and ("POTEAU", "P9", None) in (after.get("t2") or []),
        f"temoin schema 7: resultat={t2} fichier apres={after.get('t2')}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rs_null_marker_versioned.*trace_id={ctx.trace_id}",
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
