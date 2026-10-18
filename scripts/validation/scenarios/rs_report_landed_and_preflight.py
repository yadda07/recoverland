"""rs_report_landed_and_preflight - RecoverLand validation runtime.

Invariant: fail-loud (ce qui a touche le fichier est nomme comme tel) et
I-COVERAGE (on ne rejoue pas un etat que le journal ne peut plus servir).
Constats resolus: item G (`RestoreReport` sans liste explicite des echecs
ECRITS: le runner les devinait a partir des traces) et item I
(`restore_executor.execute_restore_plan` appelait
`restore_planner.preflight_check` sans connexion au journal).

Le mecanisme
------------
G. Un evenement peut echouer APRES que son ecriture a atteint le fichier:
   un declencheur reecrit la valeur, la relecture contredit ce qui a ete
   demande. `restore_batch` le compte en echec et journalise une trace
   (la donnee a change), mais le rapport n'avait pas de liste pour ces
   evenements: `RestoreRunner` les reconstituait en croisant les traces et
   les echecs, et `build_restore_session` comptait toutes les traces. Un
   consommateur qui ne fait pas ce croisement lit "echec, rien ecrit".
I. La garde de couverture d'un rewind lit l'horizon de purge que le
   JOURNAL a enregistre (il survit au redemarrage). `execute_restore_plan`
   ne pouvait pas le lui transmettre: sans connexion, seule la memoire de
   session etait consultee. Apres un redemarrage, un rewind vers une date
   purgee rejouait ce qui a survecu et produisait un etat que la couche
   n'a jamais eu.

Ce que l'utilisateur voit
-------------------------
G. La boite de dialogue annonce "1 echec" pour une restauration dont
   l'ecriture est dans le fichier, sans dire qu'il faut la verifier.
I. Hier soir la purge automatique a efface l'historique de la couche
   jusqu'au 1er septembre 05:00. Ce matin il revient au 1er septembre
   00:00: le rewind s'execute et annonce un succes.

Cas
---
    L1  restore_batch: un DELETE reinsere, reecrit par un declencheur:
        l'evenement est dans `failed` ET dans `landed`, avec sa trace
    L2  dans le meme lot, un saut prouve et une restauration propre ne
        sont pas dans `landed`
    P1  execute_restore_plan avec la connexion du journal dont l'horizon
        de purge couvre la date demandee: bloque, rien ecrit
    P2  temoin: meme plan, journal sans purge: execute

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_report_landed_and_preflight"
INVARIANT = "fail-loud"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS = "rs_report_landed_and_preflight::ds"
_TRIGGER = ("CREATE TRIGGER prefix_code AFTER INSERT ON {t} BEGIN "
            "UPDATE {t} SET code = 'X-' || NEW.code WHERE fid = NEW.fid; END;")


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def _event(eid, op, attrs, fid, wkt, changed=None):
    fx = _fx()
    payload = ({"changed_only": changed} if changed is not None
               else {"all_attributes": attrs})
    return fx.audit_event(
        event_id=eid, operation_type=op, datasource_fingerprint=_DS,
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        entity_fingerprint=f"pk:fid={fid}",
        field_schema_json=fx.schema(("fid", "integer"), ("nom", "string"),
                                    ("code", "string")),
        attributes_json=json.dumps(payload),
        geometry_wkb=fx.wkb_wkt(wkt) if wkt else None,
        event_schema_version=7)


def _journal(horizon):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.journal_writes import ensure_v7_schema
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ensure_v7_schema(conn)
    if horizon:
        conn.execute(
            "INSERT INTO journal_coverage (datasource_fingerprint, "
            "purged_through, purged_events, updated_at) VALUES (?,?,?,?)",
            (_DS, horizon, 12, "2026-09-02T06:00:00"))
    conn.commit()
    return conn


def setup(ctx):
    from osgeo import ogr
    from recoverland.core.logger import flog
    fx = _fx()
    tmp = tempfile.mkdtemp(prefix="rl_rs_rlp_")
    ctx.data["tmpdir"] = tmp
    path = fx.make_gpkg(tmp, "l1", ogr.wkbPoint, _fields(), [
        (1, ["PONT", "K1"], "POINT (1 1)"),
        (2, ["GARE", "K2"], "POINT (2 2)")])
    conn = sqlite3.connect(path)
    try:
        conn.execute(_TRIGGER.format(t="l1"))
        conn.commit()
    finally:
        conn.close()
    ctx.data["path_l1"] = path
    for key in ("p1", "p2"):
        ctx.data[key] = fx.make_gpkg(tmp, key, ogr.wkbPoint, _fields(), [
            (1, ["PONT", "K1"], "POINT (1 1)")])
    ctx.data["before_p1"] = fx.file_rows(ctx.data["p1"], "p1")
    flog(f"rs_report_landed_and_preflight setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import restore_batch
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core import restore_executor
    fx = _fx()
    d = ctx.data
    tid = ctx.trace_id
    flog(f"rs_report_landed_and_preflight run start: trace_id={tid}", "INFO")

    landed_ev = _event(1201, "DELETE", {"fid": 7, "nom": "BORNE",
                                        "code": "K7"}, 7, "POINT (7 7)")
    skip_ev = _event(1202, "DELETE", {"fid": 1, "nom": "PONT", "code": "K1"},
                     1, "POINT (1 1)")
    clean_ev = _event(1203, "UPDATE", None, 2, None,
                      changed={"nom": {"old": "GARE_AVANT", "new": "GARE"}})
    try:
        report = restore_batch(fx.open_layer(d["path_l1"], "l1"),
                               [landed_ev, skip_ev, clean_ev], trace_id=tid)
        d["l1"] = {
            "succeeded": sorted(report.succeeded),
            "failed": {int(k): str(v)[:160] for k, v in report.failed.items()},
            "skipped": sorted(report.skipped_idempotent or []),
            "traced": sorted(getattr(t, "restored_from_event_id", None)
                             for t in report.trace_events or ()),
            "landed": (sorted(report.landed)
                       if hasattr(report, "landed") else "no landed field"),
        }
    except Exception as exc:  # noqa: BLE001
        d["l1"] = {"crash": f"{type(exc).__name__}: {exc}"}

    event = _event(1204, "UPDATE", None, 1, None,
                   changed={"nom": {"old": "PONT_AVANT", "new": "PONT"}})
    cutoff = RestoreCutoff(CutoffType.BY_DATE, "2026-09-01T00:00:00")
    for key, horizon in (("p1", "2026-09-01T05:00:00"), ("p2", None)):
        plan = plan_temporal_restore([event], _DS, "p", cutoff)
        conn = _journal(horizon)
        try:
            report = restore_executor.execute_restore_plan(
                plan, {event.event_id: event}, fx.open_layer(d[key], key),
                trace_id=tid, conn=conn)
            d[key + "_res"] = {"succeeded": list(report.succeeded),
                               "failed": {int(k): str(v)[:200]
                                          for k, v in report.failed.items()}}
        except Exception as exc:  # noqa: BLE001 - un crash est un resultat
            d[key + "_res"] = {"crash": f"{type(exc).__name__}: {exc}"}
        finally:
            conn.close()
        d[key + "_after"] = fx.file_rows(d[key], key)
    flog(f"rs_report_landed_and_preflight run end: trace_id={tid} "
         f"l1={d.get('l1')} p1={d.get('p1_res')} p2={d.get('p2_res')}",
         "INFO")
    shutil.rmtree(d.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    l1 = d.get("l1") or {}
    out.append((
        "l1_landed_failure_listed",
        l1.get("landed") == [1201] and 1201 in (l1.get("failed") or {})
        and 1201 in (l1.get("traced") or []),
        f"lot={l1}. Le DELETE 1201 est reinsere puis reecrit par un "
        f"declencheur: echec, MAIS ecrit. Le rapport doit le dire dans une "
        f"liste explicite (landed), pas laisser chaque consommateur le "
        f"deviner en croisant traces et echecs.",
    ))
    out.append((
        "l2_skip_and_clean_not_landed",
        isinstance(l1.get("landed"), list)
        and 1202 not in l1["landed"] and 1203 not in l1["landed"]
        and 1202 in (l1.get("skipped") or [])
        and 1203 in (l1.get("succeeded") or []),
        f"lot={l1}: un saut prouve et une restauration verifiee ne sont pas "
        f"des echecs ecrits",
    ))
    p1 = d.get("p1_res") or {}
    failed = " ".join((p1.get("failed") or {}).values())
    out.append((
        "p1_durable_horizon_blocks",
        not p1.get("succeeded") and "Retention coverage" in failed
        and d.get("p1_after") == d.get("before_p1"),
        f"resultat={p1} fichier apres={d.get('p1_after')}. Le journal a "
        f"enregistre une purge jusqu'au 2026-09-01T05:00:00; revenir au "
        f"2026-09-01T00:00:00 rejoue ce qui a survecu et annonce un succes. "
        f"execute_restore_plan doit transmettre la connexion du journal a "
        f"preflight_check.",
    ))
    p2 = d.get("p2_res") or {}
    out.append((
        "p2_control_no_purge_executes",
        p2.get("succeeded") == [1204] and not p2.get("failed"),
        f"temoin sans purge: resultat={p2} fichier apres={d.get('p2_after')}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rs_report_landed_and_preflight.*trace_id={ctx.trace_id}",
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
