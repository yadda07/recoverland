"""tx_rewind_broken_trace_memory - RecoverLand validation runtime.

Invariant: I-8 (les traces de restauration sont la memoire persistante du
dedup) + I-3 (identite stable au moment d'ecrire).
Sujet: ce qui arrive quand cette memoire est ABIMEE - trace dont la source a
ete purgee, trace qui pointe sur une autre trace, journal ancien dont les
lignes n'ont jamais recu leur entity_fingerprint.

Le mecanisme
------------
Le dedup de rewind reconstruit "ce qui a deja ete compense" a partir de deux
informations portees par la trace :

    ref  = trace.restored_from_event_id        -> l'evenement compense
    span = [_order_key(ref) .. _order_key(trace)]

La fusion de chaines fait qu'UNE trace represente souvent PLUSIEURS
evenements reels : la trace ne reference que le plus ancien de la chaine, et
c'est le span qui couvre les autres. Toute la protection repose donc sur la
presence de l'evenement reference.

Or la lecture de fenetre (`event_stream_repository._cutoff_where`) ne ramene
une trace que si son ``restored_from_event_id`` figure parmi les event_id
encore presents :

    restored_from_event_id IN (SELECT event_id FROM audit_event WHERE ...)

La purge de retention supprime les evenements les plus anciens PAR DATE.
Quand l'horizon de retention tombe au milieu d'une chaine fusionnee, il
emporte exactement l'evenement que la trace reference. La trace, elle, reste
dans la table (la purge des traces orphelines est desactivee par defaut :
``PurgeOptions.orphan_traces = False``) mais devient INVISIBLE pour le
rewind. Le span disparait avec elle.

Scenario d'echec concret pour l'utilisateur
-------------------------------------------
Cas O1 - l'utilisateur edite puis supprime une entite, puis lance un rewind
qui la remet en place. Un an plus tard la retention purge les plus vieux
evenements. Il refait le meme rewind : la trace n'est plus lue, le DELETE
est recompense une SECONDE fois, et sa compensation etant un INSERT,
l'entite est reinseree en DOUBLE dans la couche. C'est le mode d'echec
"accumulation d'entites" que I-8 existe pour empecher.

Cas O2 - journal ancien : certaines lignes d'une meme entite portent un
entity_fingerprint, d'autres NULL. Depuis `compute_entity_key`, le dedup
canonicalise l'identite (``{"fid": 42}`` -> ``fid:42``) et les deux lignes
retombent dans le MEME seau : le scenario le verifie plutot que de le
supposer. Mais le planificateur, lui, n'a pas suivi : `_build_action` exige
un entity_fingerprint NON VIDE et refuse la ligne ancienne avec un conflit
BLOQUANT, et `preflight_check` transforme le moindre conflit bloquant en
refus de TOUT le groupe. Une seule ligne heritee suffit donc a annuler le
rewind de toute la couche - y compris les entites parfaitement identifiees.
Pire, le resultat n'est pas previsible : quand la ligne ancienne est fusionnee
avec un evenement plus recent, l'evenement synthetique herite du
fingerprint du plus recent et passe le controle sans rien dire.

Cas O3 - trace pointant sur une autre trace. Defensif : le produit actuel
n'ecrit jamais une telle trace (`undo_restore_batch` ne journalise rien,
`restore_planner._build_action` refuse toute trace, la recherche exclut les
traces). Le cas est teste parce qu'un journal edite a la main ou une base
migree le produirait, et parce que la consequence est grave : le span
s'etire de la premiere trace jusqu'a la seconde et avale au passage une
edition utilisateur qui n'a JAMAIS ete compensee.

Plan du scenario (journal SQLite reel, purge simulee par DELETE, zero QGIS) :
    O1  ds=tx_bt_purged  fid:72001  U(t+05) U(t+20) D(t+25) + trace(t+40)->U(t+05)
                                    puis suppression de U(t+05) par la retention
    O2a ds=tx_bt_nullfp  fid:72002  UPDATE sans fingerprint + DELETE avec
    O2b ds=tx_bt_legacy  fid:72004 sans fingerprint, fid:72005 avec
    O3  ds=tx_bt_chain   fid:72003  U -> trace(U) -> U -> trace(trace)
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_rewind_broken_trace_memory"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "tx_bt_project"
_FIELDS = json.dumps([
    {"name": "code", "type": "string"},
    {"name": "nom", "type": "string"},
])

_DS_PURGED = "tx_bt_purged"
_DS_NULLFP = "tx_bt_nullfp"
_DS_LEGACY = "tx_bt_legacy"
_DS_CHAIN = "tx_bt_chain"

_FP_PURGED = "fid:72001"
_FP_NULLFP = "fid:72002"
_FP_CHAIN = "fid:72003"
_FP_LEGACY_OLD = "fid:72004"
_FP_LEGACY_NEW = "fid:72005"


def _iso(t: datetime) -> str:
    return t.isoformat()


def _insert(conn, ds, ts, op, attrs, fp, identity, restored_from=None) -> int:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    sql = (
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
        + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )
    row = (
        _PROJECT_FP, ds, "lyr_" + ds, ds, "ogr",
        identity, op, json.dumps(attrs, ensure_ascii=False),
        None, "Point", "EPSG:4326", _FIELDS,
        "tester", None, ts, restored_from, fp, 2, None, None,
    )
    return conn.execute(sql, row).lastrowid


def _delta(field, old, new):
    return {"changed_only": {field: {"old": old, "new": new}}}


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)

    eids = {}
    ident_purged = json.dumps({"fid": 72001})
    ident_nullfp = json.dumps({"fid": 72002})
    ident_chain = json.dumps({"fid": 72003})

    # ================= O1 : trace dont la source sera purgee =============
    # Un evenement decoy sur une autre entite, plus ancien que la date de
    # rewind demandee : il garantit que check_retention_coverage ne bloque
    # pas (l'historique restant couvre bien la date demandee).
    eids["o1_decoy"] = _insert(
        conn, _DS_PURGED, _iso(t0 + timedelta(seconds=15)), "UPDATE",
        _delta("code", "X", "Y"), "fid:72009", json.dumps({"fid": 72009}))
    # La chaine reelle de l'entite, fusionnee par le premier rewind.
    eids["o1_u_old"] = _insert(
        conn, _DS_PURGED, _iso(t0 + timedelta(seconds=5)), "UPDATE",
        _delta("code", "A", "B"), _FP_PURGED, ident_purged)
    eids["o1_u_mid"] = _insert(
        conn, _DS_PURGED, _iso(t0 + timedelta(seconds=20)), "UPDATE",
        _delta("code", "B", "C"), _FP_PURGED, ident_purged)
    eids["o1_delete"] = _insert(
        conn, _DS_PURGED, _iso(t0 + timedelta(seconds=25)), "DELETE",
        {"all_attributes": {"code": "C", "nom": "PONT"}},
        _FP_PURGED, ident_purged)
    # Le premier rewind a fusionne U(t+05) U(t+20) D(t+25) en un DELETE
    # synthetique qui porte l'event_id du PLUS ANCIEN : la trace ne
    # reference donc que celui-la.
    eids["o1_trace"] = _insert(
        conn, _DS_PURGED, _iso(t0 + timedelta(seconds=40)), "INSERT",
        {"_restore_ref": eids["o1_u_old"]}, _FP_PURGED, ident_purged,
        restored_from=eids["o1_u_old"])

    # ================= O2a : fingerprint NULL sur la meme entite =========
    eids["o2_update"] = _insert(
        conn, _DS_NULLFP, _iso(t0 + timedelta(seconds=10)), "UPDATE",
        _delta("code", "A", "B"), None, ident_nullfp)
    eids["o2_delete"] = _insert(
        conn, _DS_NULLFP, _iso(t0 + timedelta(seconds=20)), "DELETE",
        {"all_attributes": {"code": "B", "nom": "PONT"}},
        _FP_NULLFP, ident_nullfp)

    # ================= O2b : une seule ligne heritee dans la couche ======
    eids["o2b_legacy"] = _insert(
        conn, _DS_LEGACY, _iso(t0 + timedelta(seconds=10)), "UPDATE",
        _delta("code", "A", "B"), None, json.dumps({"fid": 72004}))
    eids["o2b_modern"] = _insert(
        conn, _DS_LEGACY, _iso(t0 + timedelta(seconds=11)), "UPDATE",
        _delta("code", "K", "L"), _FP_LEGACY_NEW, json.dumps({"fid": 72005}))

    # ================= O3 : trace referencant une trace ==================
    eids["o3_u1"] = _insert(
        conn, _DS_CHAIN, _iso(t0 + timedelta(seconds=10)), "UPDATE",
        _delta("code", "A", "B"), _FP_CHAIN, ident_chain)
    eids["o3_t1"] = _insert(
        conn, _DS_CHAIN, _iso(t0 + timedelta(seconds=20)), "UPDATE",
        {"_restore_ref": eids["o3_u1"]}, _FP_CHAIN, ident_chain,
        restored_from=eids["o3_u1"])
    eids["o3_u2"] = _insert(
        conn, _DS_CHAIN, _iso(t0 + timedelta(seconds=30)), "UPDATE",
        _delta("code", "A", "D"), _FP_CHAIN, ident_chain)
    eids["o3_t2"] = _insert(
        conn, _DS_CHAIN, _iso(t0 + timedelta(seconds=40)), "UPDATE",
        {"_restore_ref": eids["o3_t1"]}, _FP_CHAIN, ident_chain,
        restored_from=eids["o3_t1"])

    conn.commit()

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["eids"] = eids

    flog(
        f"tx_rewind_broken_trace_memory setup: trace_id={ctx.trace_id} "
        f"eids={eids}",
        "INFO",
    )


def _collapse(ctx, ds, cutoff_dt):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats

    cutoff = RestoreCutoff(CutoffType.BY_DATE, _iso(cutoff_dt), inclusive=True)
    events = fetch_events_after_cutoff(
        ctx.data["conn"], ds, cutoff, trace_id=ctx.trace_id)
    active, stats = collapse_rewind_events_with_stats(events)
    return cutoff, events, active, dict(stats)


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.search_service import reconstruct_attributes
    from recoverland.core.event_stream_repository import (
        get_oldest_event_date, has_active_restore_traces,
    )
    from recoverland.core.restore_planner import (
        check_retention_coverage, plan_temporal_restore, preflight_check,
    )

    conn = ctx.data["conn"]
    t0 = ctx.data["t0"]
    eids = ctx.data["eids"]

    flog(f"tx_rewind_broken_trace_memory run start: trace_id={ctx.trace_id}",
         "INFO")

    # ===== O1 : la retention purge la source de la trace =================
    # Purge par date, exactement ce que fait purge_old_events : tout ce qui
    # est plus ancien que l'horizon disparait. Ici l'horizon tombe entre
    # U(t+05) et le decoy (t+15).
    horizon = _iso(t0 + timedelta(seconds=10))
    conn.execute(
        "DELETE FROM audit_event WHERE datasource_fingerprint = ? "
        "AND created_at < ? AND restored_from_event_id IS NULL",
        (_DS_PURGED, horizon))
    conn.commit()
    ctx.data["o1_source_row_count"] = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE event_id = ?",
        (eids["o1_u_old"],)).fetchone()[0]
    ctx.data["o1_trace_row_count"] = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE event_id = ? "
        "AND invalidated_at IS NULL",
        (eids["o1_trace"],)).fetchone()[0]

    # L'utilisateur redemande le meme rewind. La date choisie tombe
    # exactement sur le created_at de U(t+20) : fenetre inclusive.
    cutoff, events, active, stats = _collapse(
        ctx, _DS_PURGED, t0 + timedelta(seconds=20))
    ctx.data["o1"] = {
        "fetched": [e.event_id for e in events],
        "active": [e.event_id for e in active],
        "active_ops": [e.operation_type for e in active],
        "restored_attrs": [reconstruct_attributes(e) for e in active],
        "stats": stats,
    }
    ctx.data["o1_oldest_date"] = get_oldest_event_date(conn, _DS_PURGED)
    ctx.data["o1_retention_block"] = check_retention_coverage(
        cutoff, ctx.data["o1_oldest_date"])
    ctx.data["o1_active_trace_guard"] = has_active_restore_traces(
        conn, [_DS_PURGED], trace_id=ctx.trace_id)

    # ===== O2a : fingerprint NULL sur la meme entite =====================
    cutoff2, events2, active2, stats2 = _collapse(ctx, _DS_NULLFP, t0)
    plan2 = plan_temporal_restore(active2, _DS_NULLFP, "nullfp", cutoff2)
    report2 = preflight_check(plan2)
    ctx.data["o2a"] = {
        "fetched": [e.event_id for e in events2],
        "active": [e.event_id for e in active2],
        "active_ops": [e.operation_type for e in active2],
        "active_fps": [e.entity_fingerprint for e in active2],
        "restored_attrs": [reconstruct_attributes(e) for e in active2],
        "verdict": report2.verdict.value,
        "stats": stats2,
    }

    # ===== O2b : une ligne heritee au milieu d'une couche saine ==========
    cutoff3, events3, active3, stats3 = _collapse(ctx, _DS_LEGACY, t0)
    plan3 = plan_temporal_restore(active3, _DS_LEGACY, "legacy", cutoff3)
    report3 = preflight_check(plan3)
    ctx.data["o2b"] = {
        "active": [e.event_id for e in active3],
        "planned_ids": [a.event_id for a in plan3.actions],
        "conflicts": [(c.event_id, c.reason, c.severity)
                      for c in plan3.conflicts],
        "verdict": report3.verdict.value,
        "blocking": list(report3.blocking_reasons),
        "stats": stats3,
    }

    # ===== O3 : trace referencant une trace ==============================
    _c4, events4, active4, stats4 = _collapse(ctx, _DS_CHAIN, t0)
    ctx.data["o3"] = {
        "fetched": [e.event_id for e in events4],
        "active": [e.event_id for e in active4],
        "stats": stats4,
    }

    flog(
        f"tx_rewind_broken_trace_memory run end: trace_id={ctx.trace_id} "
        f"o1_active={ctx.data['o1']['active']} "
        f"o2a_active={ctx.data['o2a']['active']} "
        f"o2b_verdict={ctx.data['o2b']['verdict']} "
        f"o3_active={ctx.data['o3']['active']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    eids = ctx.data["eids"]
    o1 = ctx.data.get("o1") or {}
    o2a = ctx.data.get("o2a") or {}
    o2b = ctx.data.get("o2b") or {}
    o3 = ctx.data.get("o3") or {}

    # ===== O1 : la memoire de dedup survit-elle a la retention ? =========
    out.append((
        "purge_removed_the_referenced_event",
        ctx.data.get("o1_source_row_count") == 0,
        f"lignes restantes pour l'evenement reference="
        f"{ctx.data.get('o1_source_row_count')} attendu=0 : le scenario doit "
        f"reproduire une purge de retention reelle",
    ))
    out.append((
        "trace_row_survives_the_purge",
        ctx.data.get("o1_trace_row_count") == 1,
        f"lignes de trace restantes={ctx.data.get('o1_trace_row_count')} "
        f"attendu=1 : PurgeOptions.orphan_traces vaut False par defaut, la "
        f"trace reste donc bien dans le journal",
    ))
    out.append((
        "orphan_trace_still_read_by_rewind",
        eids["o1_trace"] in (o1.get("fetched") or []),
        f"fenetre lue={o1.get('fetched')} : la trace {eids['o1_trace']} est "
        f"toujours dans la table mais la clause de lecture exige que son "
        f"evenement source existe encore. Source purgee = memoire du rewind "
        f"invisible, sans aucun message pour l'utilisateur",
    ))
    out.append((
        "already_compensated_chain_not_replayed",
        (o1.get("active") or []) == [],
        f"evenements actifs={o1.get('active')} ops={o1.get('active_ops')} "
        f"attendu=[] : cette chaine a deja ete compensee par le rewind qui a "
        f"ecrit la trace. Rejouee, la compensation du DELETE est un INSERT : "
        f"l'entite est reinseree une SECONDE fois et l'utilisateur se retrouve "
        f"avec un doublon dans sa couche",
    ))
    out.append((
        "no_duplicate_reinsertion_planned",
        "DELETE" not in (o1.get("active_ops") or []),
        f"ops actives={o1.get('active_ops')} etat reinsere="
        f"{o1.get('restored_attrs')} : tout DELETE laisse actif ici produit une "
        f"reinsertion en double de l'entite fid:72001",
    ))
    # Ce qui protege encore l'utilisateur : le garde-fou "restauration deja
    # active" ne depend pas de la source, lui.
    out.append((
        "active_trace_guard_still_fires",
        ctx.data.get("o1_active_trace_guard") is True,
        f"has_active_restore_traces={ctx.data.get('o1_active_trace_guard')} "
        f"attendu=True : c'est le dernier avertissement affiche a "
        f"l'utilisateur avant un rewind sur une couche deja restauree",
    ))
    out.append((
        "retention_coverage_does_not_block",
        ctx.data.get("o1_retention_block") is None,
        f"blocage retention={ctx.data.get('o1_retention_block')!r} "
        f"oldest={ctx.data.get('o1_oldest_date')!r} : la date demandee est bien "
        f"couverte par l'historique restant, donc rien n'arrete l'operation de "
        f"ce cote la. L'assertion documente que ce garde-fou ne couvre PAS la "
        f"trace orpheline",
    ))

    # ===== O2a : fingerprint NULL, meme entite ==========================
    out.append((
        "nullfp_events_share_their_entity_bucket",
        len(o2a.get("active") or []) == 1
        and (o2a.get("active_ops") or []) == ["DELETE"],
        f"actifs={o2a.get('active')} ops={o2a.get('active_ops')} attendu=1 seul "
        f"DELETE fusionne : les deux evenements decrivent la MEME entite "
        f"(fid 72002). S'ils tombent dans deux seaux, la fusion UPDATE->DELETE "
        f"n'a plus lieu et l'entite revient avec la mauvaise valeur",
    ))
    out.append((
        "nullfp_restores_the_cutoff_state",
        (o2a.get("restored_attrs") or [{}])[0] == {"code": "A", "nom": "PONT"},
        f"etat reinsere={o2a.get('restored_attrs')} attendu="
        f"{{'code': 'A', 'nom': 'PONT'}} : l'entite doit revenir avec la valeur "
        f"d'avant l'edition, meme quand la ligne ancienne n'a pas de "
        f"fingerprint",
    ))
    out.append((
        "fusion_does_not_invent_an_identity",
        (o2a.get("active_fps") or [None])[0] == _FP_NULLFP,
        f"fingerprint de l'evenement fusionne={o2a.get('active_fps')} : "
        f"l'evenement synthetique herite du fingerprint du plus recent. Ici "
        f"c'est la bonne entite, mais cela signifie qu'une ligne sans identite "
        f"stable passe le controle I-3 uniquement parce qu'elle a ete fusionnee "
        f"(verdict preflight={o2a.get('verdict')!r})",
    ))

    # ===== O2b : une ligne heritee bloque-t-elle toute la couche ? =======
    out.append((
        "legacy_row_is_diagnosed",
        any(r == "missing_entity_fingerprint"
            for (_e, r, _s) in (o2b.get("conflicts") or [])),
        f"conflits={o2b.get('conflicts')} : le planificateur doit nommer "
        f"explicitement la ligne sans identite stable",
    ))
    out.append((
        "legacy_row_does_not_cancel_the_whole_layer",
        o2b.get("verdict") != "blocked",
        f"verdict preflight={o2b.get('verdict')!r} blocages={o2b.get('blocking')} "
        f"actions retenues={o2b.get('planned_ids')} : une seule ligne heritee "
        f"(sans entity_fingerprint) suffit a faire refuser le rewind de TOUTE "
        f"la couche, y compris l'entite fid:72005 qui est parfaitement "
        f"identifiee. L'utilisateur voit 'Preflight blocked' et perd la "
        f"restauration des entites saines, alors que le dedup, lui, sait "
        f"parfaitement de quelle entite parle la ligne ancienne",
    ))

    # ===== O3 : trace pointant sur une trace ============================
    out.append((
        "trace_chain_does_not_swallow_a_user_edit",
        eids["o3_u2"] in (o3.get("active") or []),
        f"actifs={o3.get('active')} doit contenir {eids['o3_u2']} : cette "
        f"edition a ete faite APRES le rewind trace par {eids['o3_t1']} et n'a "
        f"jamais ete compensee. Une trace qui reference une autre trace etire "
        f"l'intervalle de neutralisation jusqu'a elle et avale l'edition au "
        f"passage : le rewind repond 'rien a restaurer' alors que l'entite "
        f"n'est PAS revenue a son etat",
    ))
    out.append((
        "trace_chain_keeps_the_compensated_edit_neutral",
        eids["o3_u1"] not in (o3.get("active") or []),
        f"actifs={o3.get('active')} ne doit pas contenir {eids['o3_u1']} "
        f"(deja compense par la trace {eids['o3_t1']}) : le rejouer "
        f"appliquerait la compensation deux fois",
    ))

    # ===== Propagation de trace =========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_rewind_broken_trace_memory.*trace_id={ctx.trace_id}",
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
