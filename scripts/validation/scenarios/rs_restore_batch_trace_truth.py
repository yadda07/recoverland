# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_restore_batch_trace_truth - RecoverLand validation runtime.

Invariant: I-TRACE-TRUTH (une trace de restauration n'existe que pour une
ecriture qui a eu lieu) et fail-loud (un echec n'est jamais annonce
reussi).

Le mecanisme
------------
Une trace de restauration neutralise l'evenement source pour tous les
rewinds suivants: elle dit "cette modification a ete compensee". Or
`restore_batch` construisait une trace pour CHAQUE succes, y compris les
sauts qui n'ont rien ecrit: "Already exists (skip)" (la cle est tenue par
l'entite deja revenue), le doublon deja present, et "Already absent (no
snapshot match)". Ce dernier etait en plus un succes: la restauration d'un
INSERT qui ne retrouvait pas l'entite inseree repondait "deja absente",
alors que ne pas la trouver ne prouve pas qu'elle n'est plus la (toutes
ses valeurs et sa geometrie ont pu etre modifiees depuis).

Ce que l'utilisateur voit
-------------------------
Il restaure trois evenements. Deux etaient deja appliques, le troisieme
(supprimer une entite inseree) ne trouve pas sa cible. La boite dit "3
restaurees", le journal recoit trois traces. Au rewind suivant, l'INSERT
est tenu pour compense: l'entite inseree, si elle est encore la sous un
autre contenu, n'est plus jamais retiree, et personne ne l'a su.

Cas
---
    T1  DELETE deja restaure, cle tenue par l'entite elle-meme: saut, pas
        de trace
    T2  DELETE deja restaure, identite FID seule, doublon present: saut,
        pas de trace
    T4  UPDATE deja restaure (rejeu, double clic): l'entite porte l'etat
        d'avant l'edition, prouve par la cle et le contenu: saut
        already_at_old, pas de trace (et pas un refus "could not be
        verified", qui nommerait une fausse cause)
    T3  INSERT dont l'entite inseree est introuvable (lectures completes):
        refus nomme target_absent, compte en echec, pas de trace, couche
        intacte -- par restore_batch et par l'annulation d'une
        restauration de DELETE
    C   temoin: DELETE dont l'entite manque: reinseree, UNE trace, qui
        designe cet evenement

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_restore_batch_trace_truth"
INVARIANT = "I-TRACE-TRUTH"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _gpkg_fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def _schema():
    return _fx().schema(("fid", "integer"), ("nom", "string"),
                        ("code", "string"))


def _event(eid, op, identity, attrs, wkt):
    fx = _fx()
    fp = (f"pk:fid={identity['pk_value']}" if "pk_value" in identity
          else f"fid:{identity['fid']}")
    return fx.audit_event(
        event_id=eid, operation_type=op, field_schema_json=_schema(),
        feature_identity_json=json.dumps(identity), entity_fingerprint=fp,
        attributes_json=json.dumps({"all_attributes": attrs}),
        geometry_wkb=fx.wkb_wkt(wkt), new_geometry_wkb=(
            fx.wkb_wkt(wkt) if op == "INSERT" else None),
        geometry_type="Point")


def _update_event(eid, identity, changed):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type="UPDATE", field_schema_json=_schema(),
        feature_identity_json=json.dumps(identity),
        entity_fingerprint=f"pk:fid={identity['pk_value']}",
        attributes_json=json.dumps({"changed_only": {
            k: {"old": old, "new": new} for k, (old, new) in changed.items()}}),
        geometry_type="Point")


def _rows():
    return [
        (1, ["RESTE", "K1"], "POINT (1 1)"),
        (2, ["DEJA_LA", "K2"], "POINT (2 2)"),
        (4, ["DOUBLON", "K4"], "POINT (4 4)"),
        (5, ["REVERTI", "K5"], "POINT (5 5)"),
    ]


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmp = tempfile.mkdtemp(prefix="rl_rs_btt_")
    ctx.data["tmpdir"] = tmp
    for key in ("batch", "undo"):
        ctx.data[f"gpkg_{key}"] = fx.make_gpkg(
            tmp, key, ogr.wkbPoint, _gpkg_fields(), _rows())
    ctx.data["before"] = fx.file_rows(ctx.data["gpkg_batch"], "batch")
    flog(f"rs_restore_batch_trace_truth setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_batch, undo_prepare, undo_apply_one,
    )
    fx = _fx()
    d = ctx.data
    tid = ctx.trace_id
    flog(f"rs_restore_batch_trace_truth run start: trace_id={tid}", "INFO")

    t1 = _event(601, "DELETE", {"fid": 2, "pk_field": "fid", "pk_value": 2},
                {"fid": 2, "nom": "DEJA_LA", "code": "K2"}, "POINT (2 2)")
    t2 = _event(602, "DELETE", {"fid": 42},
                {"nom": "DOUBLON", "code": "K4"}, "POINT (4 4)")
    t3 = _event(603, "INSERT", {"fid": 77},
                {"nom": "INSEREE", "code": "K7"}, "POINT (7 7)")
    ctl = _event(604, "DELETE", {"fid": 88},
                 {"nom": "REVENUE", "code": "K8"}, "POINT (8 8)")
    t4 = _update_event(606, {"fid": 5, "pk_field": "fid", "pk_value": 5},
                       {"nom": ("REVERTI", "NOUVEAU")})

    layer = fx.open_layer(d["gpkg_batch"], "batch")
    report = restore_batch(layer, [t1, t2, t3, t4, ctl], trace_id=tid)
    d["batch"] = {
        "succeeded": sorted(report.succeeded),
        "failed": {int(k): str(v)[:200] for k, v in report.failed.items()},
        "skipped_idempotent": sorted(report.skipped_idempotent or []),
        "failed_target_absent": sorted(report.failed_target_absent or []),
        "trace_refs": sorted(t.restored_from_event_id
                             for t in report.trace_events or ()),
    }
    del layer
    d["after_batch"] = fx.file_rows(d["gpkg_batch"], "batch")

    # The undo of a DELETE restore deletes the re-inserted feature; here it
    # was deleted since, and nothing else carries its snapshot.
    layer = fx.open_layer(d["gpkg_undo"], "undo")
    undo_ev = _event(605, "DELETE", {"fid": 77},
                     {"nom": "INSEREE", "code": "K7"}, "POINT (7 7)")
    d["undo"] = dict(undo_apply_one(layer, undo_ev,
                                    undo_prepare(layer, [undo_ev])))
    del layer
    d["after_undo"] = fx.file_rows(d["gpkg_undo"], "undo")

    flog(f"rs_restore_batch_trace_truth run end: trace_id={tid} "
         f"batch={d['batch']} undo={d['undo']}", "INFO")
    shutil.rmtree(d.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    batch = d.get("batch") or {}
    before = d.get("before") or []
    after = d.get("after_batch") or []

    out.append((
        "skips_leave_no_trace",
        not {601, 602, 606} & set(batch.get("trace_refs") or []),
        f"lot={batch}: T1, T2 et T4 n'ont rien ecrit (deja restaures). Une "
        f"trace pour eux affirme au journal une restauration qui n'a pas eu "
        f"lieu et neutralise leur evenement pour tous les rewinds suivants.",
    ))
    out.append((
        "skips_counted_as_skips",
        batch.get("skipped_idempotent") == [601, 602, 606]
        and {601, 602, 606} <= set(batch.get("succeeded") or []),
        f"lot={batch}: T1, T2 et T4 sont des sauts declares, rien d'autre; "
        f"T4 rejoue une restauration deja faite: la refuser comme 'cible non "
        f"verifiable' nommerait une fausse cause",
    ))
    out.append((
        "t3_absent_insert_is_a_failure",
        603 in (batch.get("failed") or {})
        and 603 not in (batch.get("succeeded") or []),
        f"lot={batch}: l'entite inseree est introuvable; rien n'a ete "
        f"supprime. L'annoncer 'deja absente' et la compter restauree ment "
        f"a l'utilisateur, qui a demande une suppression.",
    ))
    out.append((
        "t3_failure_is_named_target_absent",
        batch.get("failed_target_absent") == [603]
        and "not found" in (batch.get("failed") or {}).get(603, ""),
        f"lot={batch}: attendu la categorie target_absent et un message "
        f"qui dit que l'entite n'a pas ete trouvee",
    ))
    out.append((
        "t3_leaves_no_trace",
        603 not in (batch.get("trace_refs") or []),
        f"traces={batch.get('trace_refs')}: une trace tiendrait l'INSERT "
        f"pour compense; si l'entite est encore la sous un autre contenu, "
        f"aucun rewind ne la retirera plus.",
    ))
    out.append((
        "control_write_has_its_trace",
        batch.get("trace_refs") == [604] and 604 in (batch.get("succeeded")
                                                     or []),
        f"traces={batch.get('trace_refs')} lot={batch}: la seule ecriture "
        f"du lot (reinsertion de REVENUE) doit avoir sa trace, et elle "
        f"seule.",
    ))
    # Rows are (fid, nom, code, wkt); the fid a re-insert receives is the
    # provider's choice, so the comparison is on the content.
    content = sorted((r[1:] for r in after), key=repr)
    expected = sorted([r[1:] for r in before]
                      + [("REVENUE", "K8", "Point (8 8)")], key=repr)
    out.append((
        "layer_holds_exactly_the_write",
        content == expected,
        f"apres={content} attendu={expected}: T1 a T4 ne doivent rien "
        f"ecrire, C une seule entite",
    ))

    undo = d.get("undo") or {}
    out.append((
        "undo_absent_is_a_named_refusal",
        undo.get("success") is False
        and undo.get("reason_code") == "target_absent"
        and d.get("after_undo") == before,
        f"annulation={undo} apres={d.get('after_undo')}: l'entite a "
        f"retirer est introuvable; l'annulation n'a rien fait et doit le "
        f"dire, pas repondre 'Already absent' compte en succes.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_restore_batch_trace_truth.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
