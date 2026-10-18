"""rs_provider_write_readback - RecoverLand validation runtime.

Invariant: fail-loud (jamais de succes annonce sans verifier l'etat reel
du provider; jamais d'ecriture partielle presentee comme intacte).
Constats resolus: best-effort-update-non-atomic (HIGH) et
no-provider-readback (HIGH) sur les chemins qui ecrivent directement par
le provider: restore_updated_feature, restore_deleted_feature,
_undo_update_restore.

Le mecanisme
------------
La restauration d'evenement et l'annulation n'ecrivent pas dans un tampon
d'edition: un appel provider par entite, hors du garde de commit du
rewind. Leur verdict etait le booleen rendu par l'API QGIS, qui prouve
seulement que le provider n'a pas signale d'erreur. Un declencheur qui
normalise une valeur, une colonne retrecie qui tronque, et le fichier ne
contient pas ce qui a ete demande: "Reverted", trace ecrite, l'evenement
neutralise pour tous les rewinds suivants.

Pire, un UPDATE restaure ecrivait ses attributs PUIS sa geometrie, deux
appels, deux transactions. Geometrie refusee (contrainte, declencheur,
verrou pris entre les deux appels): les attributs restaient ecrits, le
resultat disait "echouee", aucune trace. L'utilisateur croit l'entite
intacte; elle est a moitie restauree et aucun evenement ne le dit.

Le decor
--------
GeoPackage reels, avec des declencheurs SQLite comme en posent les
equipes qui gerent un reseau: l'un interdit de deplacer une geometrie,
l'autre met les noms en majuscules, un troisieme prefixe les codes a
l'insertion.

Cas
---
    R1  geometrie refusee apres des attributs: rien d'ecrit, ou un
        partiel journalise -- jamais un partiel silencieux
    R2  nom normalise par le declencheur: pas de "Reverted"
    R3  reinsertion dont le code est reecrit: pas de "Restored", et le
        lot journalise l'ecriture (trace) tout en la comptant en echec
    R4  annulation dont le nom est normalise: pas de "Undo reverted"
    C1  temoin sans declencheur: restauration verifiee, succes

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_provider_write_readback"
INVARIANT = "fail-loud"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_TRIGGERS = {
    "lock_geom": (
        "CREATE TRIGGER lock_geom BEFORE UPDATE ON {t} "
        "WHEN NEW.geom IS NOT OLD.geom BEGIN "
        "SELECT RAISE(ABORT, 'geometry locked by network rule'); END;"),
    "upper_nom": (
        "CREATE TRIGGER upper_nom AFTER UPDATE ON {t} "
        "WHEN NEW.nom IS NOT OLD.nom BEGIN "
        "UPDATE {t} SET nom = upper(NEW.nom) WHERE fid = NEW.fid; END;"),
    "prefix_code": (
        "CREATE TRIGGER prefix_code AFTER INSERT ON {t} BEGIN "
        "UPDATE {t} SET code = 'X-' || NEW.code WHERE fid = NEW.fid; END;"),
}


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _make(tmpdir, key, rows, triggers):
    from osgeo import ogr
    fx = _fx()
    path = fx.make_gpkg(tmpdir, key, ogr.wkbPoint,
                        [("nom", ogr.OFTString), ("code", ogr.OFTString)],
                        rows)
    conn = sqlite3.connect(path)
    try:
        for name in triggers:
            conn.execute(_TRIGGERS[name].format(t=key))
        conn.commit()
    finally:
        conn.close()
    return path


def _event(eid, op, attrs_json, old_wkt=None, new_wkt=None, fid=1):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type=op,
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        entity_fingerprint=f"pk:fid={fid}",
        attributes_json=json.dumps(attrs_json),
        geometry_wkb=fx.wkb_wkt(old_wkt) if old_wkt else None,
        new_geometry_wkb=fx.wkb_wkt(new_wkt) if new_wkt else None,
        field_schema_json=fx.schema(("fid", "integer"), ("nom", "string"),
                                    ("code", "string")))


def setup(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tmpdir = tempfile.mkdtemp(prefix="rl_rs_pwr_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["r1"] = _make(tmpdir, "r1", [(1, ["PONT", "K1"], "POINT (1 1)")],
                           ["lock_geom"])
    ctx.data["r1b"] = _make(tmpdir, "r1b", [(1, ["PONT", "K1"], "POINT (1 1)")],
                            ["lock_geom"])
    ctx.data["r2"] = _make(tmpdir, "r2", [(1, ["PONT", "K1"], "POINT (1 1)")],
                           ["upper_nom"])
    ctx.data["r3"] = _make(tmpdir, "r3", [(1, ["RESTE", "K1"], "POINT (1 1)")],
                           ["prefix_code"])
    ctx.data["r4"] = _make(tmpdir, "r4", [(1, ["avant", "K1"], "POINT (1 1)")],
                           ["upper_nom"])
    ctx.data["c1"] = _make(tmpdir, "c1", [(1, ["PONT", "K1"], "POINT (1 1)")],
                           [])
    ctx.data["before"] = {key: fx.file_rows(ctx.data[key], key)
                          for key in ("r1", "r1b", "r2", "r3", "r4", "c1")}
    flog(f"rs_provider_write_readback setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_updated_feature, restore_batch, undo_prepare, undo_apply_one,
    )
    fx = _fx()
    tid = ctx.trace_id
    res = {}
    flog(f"rs_provider_write_readback run start: trace_id={tid}", "INFO")

    # R1: the parcel was moved from (0 0) to (1 1) and renamed; the network
    # rule now forbids moving it back.
    ev_r1 = _event(501, "UPDATE", {"changed_only": {
        "nom": {"old": "PONT_AVANT", "new": "PONT"}}},
        old_wkt="POINT (0 0)", new_wkt="POINT (1 1)")
    res["r1"] = dict(restore_updated_feature(
        fx.open_layer(ctx.data["r1"], "r1"), ev_r1))
    report = restore_batch(fx.open_layer(ctx.data["r1b"], "r1b"), [ev_r1],
                           trace_id=tid)
    res["r1b"] = {"succeeded": list(report.succeeded),
                  "failed": {int(k): str(v)[:200]
                             for k, v in report.failed.items()},
                  "traces": len(report.trace_events or ())}

    # R2: the rename is normalised to upper case by a trigger.
    ev_r2 = _event(502, "UPDATE", {"changed_only": {
        "nom": {"old": "pont_avant", "new": "PONT"}}})
    res["r2"] = dict(restore_updated_feature(
        fx.open_layer(ctx.data["r2"], "r2"), ev_r2))

    # R3: re-insert, the code is prefixed by an insert trigger.
    ev_r3 = _event(503, "DELETE", {"all_attributes": {
        "fid": 7, "nom": "BORNE", "code": "K7"}}, old_wkt="POINT (7 7)", fid=7)
    report = restore_batch(fx.open_layer(ctx.data["r3"], "r3"), [ev_r3],
                           trace_id=tid)
    res["r3"] = {"succeeded": list(report.succeeded),
                 "failed": {int(k): str(v)[:200]
                            for k, v in report.failed.items()},
                 "traces": len(report.trace_events or ())}

    # R4: undo of a restore (avant -> pont), normalised by the trigger.
    ev_r4 = _event(504, "UPDATE", {"changed_only": {
        "nom": {"old": "avant", "new": "pont"}}})
    layer = fx.open_layer(ctx.data["r4"], "r4")
    res["r4"] = dict(undo_apply_one(layer, ev_r4,
                                    undo_prepare(layer, [ev_r4])))

    # C1: control, no trigger.
    ev_c1 = _event(505, "UPDATE", {"changed_only": {
        "nom": {"old": "PONT_AVANT", "new": "PONT"}}},
        old_wkt="POINT (0 0)", new_wkt="POINT (1 1)")
    res["c1"] = dict(restore_updated_feature(
        fx.open_layer(ctx.data["c1"], "c1"), ev_c1))

    ctx.data["res"] = res
    ctx.data["after"] = {key: fx.file_rows(ctx.data[key], key)
                         for key in ("r1", "r1b", "r2", "r3", "r4", "c1")}
    flog(f"rs_provider_write_readback run end: trace_id={tid} res={res}",
         "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    res = ctx.data.get("res") or {}
    before = ctx.data.get("before") or {}
    after = ctx.data.get("after") or {}

    r1 = res.get("r1") or {}
    r1_untouched = after.get("r1") == before.get("r1")
    out.append((
        "r1_no_silent_partial_write",
        r1_untouched or (r1.get("landed") is True
                         and r1.get("reason_code")
                         == "failed_postcommit_verification"),
        f"resultat={r1} apres={after.get('r1')} avant={before.get('r1')}: "
        f"le nom a ete restaure, la geometrie refusee; l'entite est a moitie "
        f"restauree et le resultat se contente de dire 'echouee'.",
    ))
    out.append((
        "r1_reported_failed_with_provider_reason",
        r1.get("success") is False
        and "geometry locked" in str(r1.get("message", "")),
        f"resultat={r1}: l'echec doit citer la raison du provider",
    ))
    r1b = res.get("r1b") or {}
    out.append((
        "r1b_batch_partial_is_journalled_or_absent",
        after.get("r1b") == before.get("r1b") or r1b.get("traces") == 1,
        f"restore_batch={r1b} apres={after.get('r1b')}: une ecriture "
        f"partielle restee dans le fichier doit etre journalisee (trace), "
        f"sinon aucune restauration ni annulation ulterieure ne la connait",
    ))

    r2 = res.get("r2") or {}
    out.append((
        "r2_readback_contradiction_is_not_success",
        r2.get("success") is False
        and r2.get("reason_code") == "failed_postcommit_verification",
        f"resultat={r2} apres={after.get('r2')}: le fichier contient "
        f"'PONT_AVANT' alors que la restauration a ecrit 'pont_avant' et "
        f"annonce 'Reverted'",
    ))
    out.append((
        "r2_message_names_the_field",
        "nom" in str(r2.get("message", "")),
        f"message={r2.get('message')!r}: l'utilisateur doit savoir quel "
        f"champ ne tient pas la valeur restauree",
    ))

    r3 = res.get("r3") or {}
    out.append((
        "r3_reinsert_contradiction_not_counted_restored",
        r3.get("succeeded") == [] and 503 in (r3.get("failed") or {}),
        f"restore_batch={r3} apres={after.get('r3')}: le code relu est "
        f"'X-K7', pas 'K7'; la reinsertion ne peut pas compter comme "
        f"restauree",
    ))
    out.append((
        "r3_landed_write_is_journalled",
        r3.get("traces") == 1,
        f"traces={r3.get('traces')}: l'entite a bien ete ajoutee au "
        f"fichier; l'ecriture doit etre journalisee meme si elle est "
        f"comptee en echec",
    ))

    r4 = res.get("r4") or {}
    out.append((
        "r4_undo_readback_contradiction_is_not_success",
        r4.get("success") is False
        and r4.get("reason_code") == "failed_postcommit_verification",
        f"resultat={r4} apres={after.get('r4')}: l'annulation annonce "
        f"'Undo reverted' alors que le fichier contient 'PONT'",
    ))

    c1 = res.get("c1") or {}
    out.append((
        "control_verified_restore_succeeds",
        c1.get("success") is True
        and any(r[1] == "PONT_AVANT" and r[-1] == "Point (0 0)"
                for r in after.get("c1") or []),
        f"temoin: resultat={c1} apres={after.get('c1')}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rs_provider_write_readback.*trace_id={ctx.trace_id}",
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
