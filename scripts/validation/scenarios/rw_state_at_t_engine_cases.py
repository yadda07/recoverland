"""rw_state_at_t_engine_cases - RecoverLand validation runtime.

Invariant: I-8 / I-REWIND-01 (un rewind a la date T rend EXACTEMENT
l'etat a T, calcule depuis le seul journal : les compensations faites
avant T en font partie, celles faites apres T sont defaites, et un
evenement n'est neutralise que s'il a reellement ete compense).

Le mecanisme
------------
Le moteur de rewind (`event_stream_repository.fetch_events_after_cutoff`
puis `rewind_dedup.collapse_rewind_events_with_stats`) lit le journal et
produit la liste des compensations a appliquer. Trois defauts du moteur
d'origine, chacun reproduit ici sur un journal SQLite reel, avec des
traces construites par la vraie fonction du produit
(`restore_service.build_restore_trace_event`) :

A/B  span-neutralises-uncompensated : une trace neutralisait TOUS les
     evenements de l'entite entre l'evenement reference et la trace (un
     "span" suppose couvrir une chaine fusionnee). Une restauration
     unitaire (Mode A) d'UNE edition, ou un rewind dont une action a
     echoue sans trace (FID recycle), faisaient disparaitre des
     evenements jamais compenses. Pour l'utilisateur : "Aucun evenement
     apres cette date. Rien a restaurer." alors que la couche n'est pas
     revenue a la date demandee, et un rewind relance ne termine jamais
     le travail.
C    forward-hop-real-vs-synthetic : la fusion UPDATE->DELETE produisait un
     DELETE synthetique portant l'event_id de l'UPDATE. La trace nommait
     l'UPDATE ; apres un redemarrage, le saut en avant rejouait l'UPDATE
     reel au lieu de supprimer l'entite : une entite supprimee par
     l'utilisateur ressuscite, et le rapport dit "termine".
D    rewind-ignores-compensations-before-T : une compensation ecrite AVANT
     T etait defaite par le saut suivant (09:00 suppression accidentelle
     de 3 entites, 10:00 rewind qui les recupere, 10:30 autre edition,
     11:00 rewind a 10:15 : les 3 entites sont supprimees a nouveau).
E    une compensation en vigueur a T puis annulee (Annuler) apres T doit
     etre refaite : sinon l'etat rendu n'est pas celui de T.
F    saut en avant puis retour : la re-application d'un evenement est une
     compensation de la trace (trace -> trace) ; T1, T2 puis T1 doit
     retomber sur l'etat de T1, et rejouer la meme date ne fait rien.
H/I  quand l'etat a T ne peut pas etre reconstruit (compensation fusionnee
     par une ancienne version, evenement purge par la retention), le
     rewind doit le REFUSER en le nommant, pas ecrire un etat faux.
H2   la fusion d'une longue chaine d'UPDATE (plus de dix) en UN UPDATE
     portant l'id du plus ancien : le saut en avant la defaisait comme si
     elle ne couvrait que cet UPDATE (re-application de U1 seul, les dix
     autres restent defaits dans la couche mais comptes comme revenus).
J    une edition retenue dans le fichier pending pendant un rewind puis
     reimportee au demarrage garde sa date mais recoit un id POSTERIEUR a
     la trace : l'intervalle d'une ancienne trace fusionnee ne doit pas la
     cacher, elle n'a jamais ete compensee.

K    une edition compensee par un rewind puis re-appliquee par un saut en
     avant est dans la couche depuis ce saut : un rewind suivant doit la
     defaire a cette place, sinon elle ecrase une valeur re-appliquee
     juste avant (trouve par rw_state_at_t_property, graine 318).

Verdict attendu avant correctif : FAIL (A, B, C, D, E, F, H, H2, I, J, K).
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_state_at_t_engine_cases"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PROJ = "rw_satec_project"


def _iso(t0, secs):
    return (t0 + timedelta(seconds=secs)).isoformat()


def _ev(ds, op, attrs, fid, ts):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION
    ident = json.dumps({"fid": fid})
    return AuditEvent(
        event_id=None, project_fingerprint=_PROJ, datasource_fingerprint=ds,
        layer_id_snapshot="lyr_" + ds, layer_name_snapshot=ds,
        provider_type="ogr", feature_identity_json=ident,
        operation_type=op, attributes_json=json.dumps(attrs),
        geometry_wkb=None, geometry_type="NoGeometry", crs_authid="EPSG:4326",
        field_schema_json=None, user_name="tester", session_id="s-" + ds,
        created_at=ts, restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(ident),
        event_schema_version=CURRENT_SCHEMA_VERSION,
    )


def _insert(conn, ev):
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_COLUMNS, AUDIT_EVENT_INSERT_PLACEHOLDERS,
        AUDIT_EVENT_INSERT_SQL,
    )
    cur = conn.execute(
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")",
        [getattr(ev, c) for c in AUDIT_EVENT_INSERT_COLUMNS])
    conn.commit()
    return ev._replace(event_id=cur.lastrowid)


def _trace(conn, layer, source, ts, schema=None):
    """The trace the product writes after compensating *source*."""
    from recoverland.core.restore_service import build_restore_trace_event
    trace = build_restore_trace_event(source, layer)
    trace = trace._replace(datasource_fingerprint=source.datasource_fingerprint,
                           created_at=ts)
    if schema is not None:
        trace = trace._replace(event_schema_version=schema)
    return _insert(conn, trace)


def _cutoff(ts):
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    return RestoreCutoff(CutoffType.BY_DATE, ts, inclusive=True)


def _window(ctx, ds, ts):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    events = fetch_events_after_cutoff(ctx.data["conn"], ds, _cutoff(ts),
                                       trace_id=ctx.trace_id)
    active, stats = collapse_rewind_events_with_stats(events)
    return active, dict(stats)


def _kind(e):
    mark = getattr(e, "commit_id", None) or ""
    return mark.split(":", 1)[1].split("@")[0] if mark.startswith("rewind-plan:") else "user"


def _describe(active):
    return [(e.event_id, e.operation_type, _kind(e)) for e in active]


def _old_attrs(e):
    from recoverland.core.search_service import reconstruct_attributes
    return reconstruct_attributes(e)


def setup(ctx):
    from qgis.core import QgsVectorLayer
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ctx.data["conn"] = conn
    ctx.data["layer"] = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=a:integer&field=b:integer&field=nom:string",
        "rw_satec_mem", "memory")
    ctx.data["t0"] = datetime(2026, 9, 1, 8, 0, 0, tzinfo=timezone.utc)
    flog(f"rw_state_at_t_engine_cases setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    try:
        _run_cases(ctx)
    except Exception as exc:  # noqa: BLE001 - reported by the assertions
        import traceback
        ctx.data["crash"] = f"{type(exc).__name__}: {exc}"
        ctx.data["trace"] = traceback.format_exc()[-2000:]
    flog(f"rw_state_at_t_engine_cases run: trace_id={ctx.trace_id} "
         f"crash={ctx.data.get('crash')}", "INFO")


def _run_cases(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_active_traces_before_cutoff,
    )
    from recoverland.core.restore_planner import (
        plan_temporal_restore, preflight_check,
    )
    conn = ctx.data["conn"]
    lyr = ctx.data["layer"]
    t0 = ctx.data["t0"]
    d = ctx.data

    # ---- A: Mode A restore of ONE edition of an entity ------------------
    ds = "rw_satec_a"
    e1 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 1}}}, 7, _iso(t0, 10)))
    e2 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"b": {"old": 0, "new": 1}}}, 7, _iso(t0, 20)))
    _trace(conn, lyr, e1, _iso(t0, 30))
    active, stats = _window(ctx, ds, _iso(t0, 5))
    d["a"] = {"e1": e1.event_id, "e2": e2.event_id,
              "active": _describe(active), "stats": stats}

    # ---- B: FID recycled, the second action soft-failed (no trace) ------
    ds = "rw_satec_b"
    ed = _insert(conn, _ev(ds, "DELETE", {"all_attributes": {"nom": "A"}}, 5, _iso(t0, 10)))
    ei = _insert(conn, _ev(ds, "INSERT", {"all_attributes": {"nom": "B"}}, 5, _iso(t0, 20)))
    _trace(conn, lyr, ed, _iso(t0, 30))
    active, stats = _window(ctx, ds, _iso(t0, 5))
    d["b"] = {"ed": ed.event_id, "ei": ei.event_id,
              "active": _describe(active), "stats": stats}

    # ---- C: UPDATE then DELETE, rewind, then forward hops ----------------
    ds = "rw_satec_c"
    u1 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 1, "new": 2}}}, 9, _iso(t0, 10)))
    dd = _insert(conn, _ev(ds, "DELETE", {"all_attributes": {"a": 2, "b": 5}}, 9, _iso(t0, 20)))
    rows = {u1.event_id: u1, dd.event_id: dd}
    active, _stats = _window(ctx, ds, _iso(t0, 5))
    d["c_first"] = _describe(active)
    d["c_rewrites"] = [
        (e.event_id, e.operation_type, e.attributes_json)
        for e in active
        if e.event_id not in rows
        or e.operation_type != rows[e.event_id].operation_type
        or e.attributes_json != rows[e.event_id].attributes_json]
    # The runner writes one trace per compensated row, as the product does.
    for e in active:
        _trace(conn, lyr, e, _iso(t0, 30))
    mid, _ = _window(ctx, ds, _iso(t0, 15))
    d["c_mid"] = _describe(mid)
    d["c_mid_writes"] = [_old_attrs(e) for e in mid]
    late, _ = _window(ctx, ds, _iso(t0, 40))
    d["c_late"] = _describe(late)
    d["c_late_undo"] = [t.restored_from_event_id for t in
                        fetch_active_traces_before_cutoff(
                            conn, [ds], _cutoff(_iso(t0, 40)),
                            trace_id=ctx.trace_id)]
    d["c_ids"] = {"u1": u1.event_id, "d": dd.event_id}

    # ---- D: compensations before T are history ---------------------------
    ds = "rw_satec_d"
    t9 = t0 + timedelta(hours=1)       # 09:00
    dels = [_insert(conn, _ev(ds, "DELETE", {"all_attributes": {"nom": f"N{i}"}}, 21 + i,
                              t9.isoformat())) for i in range(3)]
    t10 = t0 + timedelta(hours=2)      # 10:00: the rewind recovers them
    for e in dels:
        _trace(conn, lyr, e, t10.isoformat())
    t1030 = t10 + timedelta(minutes=30)
    upd = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"nom": {"old": "X", "new": "Y"}}},
                            30, t1030.isoformat()))
    t1015 = (t10 + timedelta(minutes=15)).isoformat()
    active, _ = _window(ctx, ds, t1015)
    d["d_1015"] = _describe(active)
    d["d_1015_undo"] = [t.restored_from_event_id for t in
                        fetch_active_traces_before_cutoff(
                            conn, [ds], _cutoff(t1015), trace_id=ctx.trace_id)]
    t0930 = (t9 + timedelta(minutes=30)).isoformat()
    active, _ = _window(ctx, ds, t0930)
    d["d_0930"] = _describe(active)
    d["d_ids"] = {"dels": [e.event_id for e in dels], "upd": upd.event_id}

    # ---- E: a compensation in effect at T, undone after T ----------------
    ds = "rw_satec_e"
    de = _insert(conn, _ev(ds, "DELETE", {"all_attributes": {"nom": "E"}}, 41, _iso(t0, 10)))
    tr = _trace(conn, lyr, de, _iso(t0, 20))
    conn.execute("UPDATE audit_event SET invalidated_at = ? WHERE event_id = ?",
                 (_iso(t0, 40), tr.event_id))
    conn.commit()
    d["e_30"] = _describe(_window(ctx, ds, _iso(t0, 30))[0])
    d["e_15"] = _describe(_window(ctx, ds, _iso(t0, 15))[0])
    d["e_45"] = _describe(_window(ctx, ds, _iso(t0, 45))[0])
    d["e_ids"] = {"d": de.event_id, "trace": tr.event_id}

    # ---- F: rewind, forward hop, back again ------------------------------
    ds = "rw_satec_f"
    uf = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 1}}}, 51, _iso(t0, 10)))
    back, _ = _window(ctx, ds, _iso(t0, 5))
    r1 = None
    for e in back:
        r1 = _trace(conn, lyr, e, _iso(t0, 20))
    hop, _ = _window(ctx, ds, _iso(t0, 15))
    d["f_hop"] = _describe(hop)
    d["f_hop_writes"] = [_old_attrs(e) for e in hop]
    items = [e for e in hop if _kind(e) == "reapply"]
    if items:
        pseudo = items[0]
    else:
        # Pre-fix: the window has no forward hop. Build the row the fixed
        # engine would build, so the rest of the case still runs.
        pseudo = uf._replace(event_id=r1.event_id if r1 else None,
                             attributes_json=json.dumps(
                                 {"changed_only": {"a": {"old": 1, "new": 0}}}))
    _trace(conn, lyr, pseudo, _iso(t0, 30))
    d["f_again_15"] = _describe(_window(ctx, ds, _iso(t0, 15))[0])
    d["f_back_5"] = _describe(_window(ctx, ds, _iso(t0, 5))[0])
    d["f_mid_25"] = _describe(_window(ctx, ds, _iso(t0, 25))[0])
    d["f_ids"] = {"u": uf.event_id, "r1": r1.event_id if r1 else None}

    # ---- H: a legacy FUSED compensation toggled after T -----------------
    ds = "rw_satec_h"
    uh = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 1, "new": 2}}}, 61, _iso(t0, 10)))
    _insert(conn, _ev(ds, "DELETE", {"all_attributes": {"a": 2}}, 61, _iso(t0, 20)))
    # schema 6: the rewind fused U+D into a DELETE carrying U's id.
    fused = uh._replace(operation_type="DELETE",
                        attributes_json=json.dumps({"all_attributes": {"a": 1}}))
    _trace(conn, lyr, fused, _iso(t0, 30), schema=6)
    active, _ = _window(ctx, ds, _iso(t0, 15))
    plan = plan_temporal_restore(active, ds, "h", _cutoff(_iso(t0, 15)))
    report = preflight_check(plan)
    d["h"] = {"active": _describe(active), "verdict": report.verdict.value,
              "blocking": list(report.blocking_reasons)}

    # ---- H2: a legacy fused LONG chain (11 UPDATEs, one trace) -----------
    # Schema 6 fused a chain of more than ten UPDATEs into ONE synthetic
    # UPDATE carrying the oldest id: its trace names U1 and stands for all
    # eleven. At T between U1 and U2 the field held 1; undoing the trace
    # event by event re-applies U1 only, and the journal then believes the
    # ten others are back while the data still has them undone.
    ds = "rw_satec_h2"
    chain = [_insert(conn, _ev(ds, "UPDATE",
                               {"changed_only": {"a": {"old": k, "new": k + 1}}},
                               62, _iso(t0, 10 + k))) for k in range(11)]
    _trace(conn, lyr, chain[0], _iso(t0, 40), schema=6)
    t_h2 = (t0 + timedelta(seconds=10, milliseconds=500)).isoformat()
    active, _ = _window(ctx, ds, t_h2)
    plan = plan_temporal_restore(active, ds, "h2", _cutoff(t_h2))
    report = preflight_check(plan)
    d["h2"] = {"active": _describe(active), "verdict": report.verdict.value,
               "blocking": list(report.blocking_reasons),
               "undo": [t.restored_from_event_id for t in
                        fetch_active_traces_before_cutoff(
                            conn, [ds], _cutoff(t_h2), trace_id=ctx.trace_id)],
               "u1": chain[0].event_id}

    # ---- J: an edit re-imported from the pending file, inside a legacy span
    # U then D fused by schema 6 into one DELETE (trace references U). P, an
    # UPDATE of field b made between U and D, was held back in the pending
    # file during that rewind and re-imported at the next start: it keeps
    # its date but gets an id LARGER than the trace's. The rewind never saw
    # it, so it was never compensated.
    ds = "rw_satec_j"
    uj = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 1}}}, 63, _iso(t0, 10)))
    dj = _insert(conn, _ev(ds, "DELETE", {"all_attributes": {"a": 1, "b": 1}}, 63, _iso(t0, 20)))
    fused_j = uj._replace(operation_type="DELETE",
                          attributes_json=json.dumps({"all_attributes": {"a": 0, "b": 1}}))
    tj = _trace(conn, lyr, fused_j, _iso(t0, 30), schema=6)
    pj = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"b": {"old": 0, "new": 1}}}, 63, _iso(t0, 15)))
    active, _ = _window(ctx, ds, _iso(t0, 5))
    d["j"] = {"active": _describe(active), "u": uj.event_id, "d": dj.event_id,
              "p": pj.event_id, "trace": tj.event_id}

    # ---- K: an edit re-applied by a forward hop, then a rewind before it -
    # (found by rw_state_at_t_property, seed 318). Every step runs the real
    # window, plans it, writes the product's traces in plan order, and
    # replays the writes on one field: the value must be the state at each
    # chosen date.
    ds = "rw_satec_k"
    value = {"a": 0}
    k_steps = []

    def _rewind_k(cutoff_s, trace_s):
        rows, _ = _window(ctx, ds, _iso(t0, cutoff_s))
        plan_k = plan_temporal_restore(rows, ds, "k", _cutoff(_iso(t0, cutoff_s)))
        by_row = {e.event_id: e for e in rows}
        for action in plan_k.actions:
            row = by_row[action.event_id]
            value.update(_old_attrs(row))
            _trace(conn, lyr, row, _iso(t0, trace_s))
        k_steps.append((cutoff_s, [(e.event_id, e.operation_type, _kind(e))
                                   for e in (by_row[a.event_id] for a in plan_k.actions)],
                        value.get("a")))

    uk3 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 1}}}, 91, _iso(t0, 10)))
    value["a"] = 1
    _rewind_k(5, 20)                     # before U3: a = 0
    uk7 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 2}}}, 91, _iso(t0, 30)))
    value["a"] = 2
    _rewind_k(15, 40)                    # U3 in effect, U7 not yet: a = 1
    _rewind_k(35, 50)                    # U3 compensated, U7 made: a = 2
    _rewind_k(15, 60)                    # back to 15: a = 1
    d["k"] = {"steps": k_steps, "u3": uk3.event_id, "u7": uk7.event_id}

    # ---- I: exact compensation after T of an event retention purged ------
    ds = "rw_satec_i"
    ui = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 1}}}, 71, _iso(t0, 10)))
    _trace(conn, lyr, ui, _iso(t0, 20))
    conn.execute("DELETE FROM audit_event WHERE event_id = ?", (ui.event_id,))
    conn.commit()
    active, _ = _window(ctx, ds, _iso(t0, 15))
    plan = plan_temporal_restore(active, ds, "i", _cutoff(_iso(t0, 15)))
    report = preflight_check(plan)
    d["i"] = {"active": _describe(active), "verdict": report.verdict.value,
              "blocking": list(report.blocking_reasons)}

    # ---- G: two toggles of one field written within one clock reading ----
    # (found by rw_state_at_t_property, seed 2). U1 is compensated by R1,
    # U2 is made, then one run writes R2 (compensates U2) and R3 (cancels R1,
    # re-applying U1) with the SAME created_at. At T between U2 and that run,
    # the field holds U2's value: undo R3 first (U1 compensated again), then
    # R2 (U2 re-applied). The tie must break on the traces' ids.
    ds = "rw_satec_g"
    ug1 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 1}}}, 81, _iso(t0, 10)))
    r1 = _trace(conn, lyr, ug1, _iso(t0, 20))
    ug2 = _insert(conn, _ev(ds, "UPDATE", {"changed_only": {"a": {"old": 0, "new": 2}}}, 81, _iso(t0, 30)))
    _trace(conn, lyr, ug2, _iso(t0, 50))
    cancel = ug1._replace(event_id=r1.event_id, attributes_json=json.dumps(
        {"changed_only": {"a": {"old": 1, "new": 0}}}))
    _trace(conn, lyr, cancel, _iso(t0, 50))
    active, _ = _window(ctx, ds, _iso(t0, 40))
    plan = plan_temporal_restore(active, ds, "g", _cutoff(_iso(t0, 40)))
    by_id = {e.event_id: e for e in active}
    state = {"a": 1}   # now: U1 re-applied, U2 compensated
    for action in plan.actions:
        state.update(_old_attrs(by_id[action.event_id]))
    d["g"] = {"active": _describe(active), "final": state,
              "order": [a.event_id for a in plan.actions]}


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    if d.get("crash"):
        out.append(("run_did_not_crash", False,
                    f"{d.get('crash')} {d.get('trace', '')}"))

    a = d.get("a") or {}
    a_ids = [x[0] for x in a.get("active") or []]
    out.append((
        "a_mode_a_trace_neutralises_only_its_event",
        a.get("e2") in a_ids and a.get("e1") not in a_ids,
        f"actifs={a.get('active')} E1={a.get('e1')} (restaure seul en Mode A) "
        f"E2={a.get('e2')} (jamais compense) : le rewind avant E1 doit remettre "
        f"b a 0. Sinon l'utilisateur lit 'Rien a restaurer' alors que b vaut "
        f"toujours 1. stats={a.get('stats')}",
    ))

    b = d.get("b") or {}
    b_ids = [x[0] for x in b.get("active") or []]
    out.append((
        "b_retry_finishes_a_partial_rewind",
        b.get("ei") in b_ids and b.get("ed") not in b_ids,
        f"actifs={b.get('active')} DELETE A={b.get('ed')} (compense) INSERT B="
        f"{b.get('ei')} (FID recycle, echec sans trace) : relancer le meme rewind "
        f"doit supprimer B. Sinon B ne peut plus jamais etre retire.",
    ))

    out.append((
        "c_no_synthetic_event_reuses_a_real_id",
        d.get("c_rewrites") == [] and len(d.get("c_first") or []) == 2,
        f"lignes planifiees differentes de la ligne du journal de meme id="
        f"{d.get('c_rewrites')} plan={d.get('c_first')} : une trace ecrite pour "
        f"une ligne synthetique nomme un evenement reel qu'elle n'a pas compense ; "
        f"apres un redemarrage, le saut en avant rejoue le mauvais evenement.",
    ))
    c_ids = d.get("c_ids") or {}
    mid = d.get("c_mid") or []
    out.append((
        "c_forward_hop_reapplies_the_update_only",
        len(mid) == 1 and mid[0][2] == "reapply" and mid[0][1] == "UPDATE"
        and (d.get("c_mid_writes") or [{}])[0] == {"a": 2},
        f"plan pour T entre U1 et D={mid} ecritures={d.get('c_mid_writes')} : "
        f"a T l'entite existe avec a=2. Il faut re-appliquer U1 et laisser D "
        f"compense ; recompenser D la reinsererait une seconde fois.",
    ))
    out.append((
        "c_rewind_to_now_touches_nothing",
        d.get("c_late") == [] and d.get("c_late_undo") == [],
        f"plan={d.get('c_late')} traces a defaire={d.get('c_late_undo')} "
        f"(U1={c_ids.get('u1')}) : les compensations sont anterieures a T, elles "
        f"font partie de l'etat demande. Les defaire ressuscite l'entite que "
        f"l'utilisateur avait supprimee.",
    ))

    d_ids = d.get("d_ids") or {}
    out.append((
        "d_compensations_before_t_are_kept",
        d.get("d_1015_undo") == []
        and [x[0] for x in d.get("d_1015") or []] == [d_ids.get("upd")],
        f"a 10:15 : traces a defaire={d.get('d_1015_undo')} plan={d.get('d_1015')} "
        f"attendu : seule l'edition de 10:30 ({d_ids.get('upd')}). Defaire les "
        f"traces de 10:00 supprime a nouveau les 3 entites recuperees, alors que "
        f"la Revue a 10:15 les montre.",
    ))
    d0930 = d.get("d_0930") or []
    out.append((
        "d_forward_hop_in_one_chronological_run",
        len(d0930) == 4 and d0930[0][0] == d_ids.get("upd")
        and sorted(x[2] for x in d0930[1:]) == ["reapply"] * 3
        and all(x[1] == "INSERT" for x in d0930[1:]),
        f"a 09:30 : plan={d0930} : compenser d'abord l'edition de 10:30, puis "
        f"re-supprimer les 3 entites (etat a 09:30), dans un seul passage "
        f"ordonne du plus recent au plus ancien.",
    ))

    e_ids = d.get("e_ids") or {}
    e30 = d.get("e_30") or []
    out.append((
        "e_undone_compensation_is_made_again",
        len(e30) == 1 and e30[0][0] == e_ids.get("d")
        and e30[0][2] == "recompensate" and e30[0][1] == "DELETE",
        f"T entre la re-insertion et l'Annuler : plan={e30} : a T l'entite "
        f"existait ; sans recompensation le rewind rend un etat qui n'a jamais "
        f"existe a T.",
    ))
    out.append((
        "e_nothing_outside_the_toggle",
        d.get("e_15") == [] and d.get("e_45") == [],
        f"T avant la re-insertion={d.get('e_15')} T apres l'Annuler="
        f"{d.get('e_45')} : aucun travail attendu.",
    ))

    f_ids = d.get("f_ids") or {}
    hop = d.get("f_hop") or []
    out.append((
        "f_forward_hop_is_a_compensation_of_the_trace",
        len(hop) == 1 and hop[0][2] == "reapply" and hop[0][0] == f_ids.get("r1")
        and (d.get("f_hop_writes") or [{}])[0] == {"a": 1},
        f"saut en avant={hop} ecritures={d.get('f_hop_writes')} : re-appliquer "
        f"U (a=1), la trace ecrite annule la trace {f_ids.get('r1')}.",
    ))
    out.append((
        "f_same_date_twice_is_a_no_op",
        d.get("f_again_15") == [],
        f"meme date rejouee : plan={d.get('f_again_15')} attendu [].",
    ))
    back5 = d.get("f_back_5") or []
    # U is compensated again under its own id, placed at the forward hop
    # that re-applied it (case K explains why the place matters).
    out.append((
        "f_t1_t2_t1_lands_back_on_t1",
        [x[0] for x in back5] == [f_ids.get("u")]
        and back5[0][1] == "UPDATE" and back5[0][2] == "recompensate",
        f"retour a T1 : plan={back5} : U doit etre compense de nouveau, sous "
        f"son propre id, a la place du saut en avant qui l'a re-applique "
        f"(sa premiere trace est annulee par ce saut).",
    ))
    mid25 = d.get("f_mid_25") or []
    out.append((
        "f_between_the_two_traces_recompensates",
        len(mid25) == 1 and mid25[0][0] == f_ids.get("u")
        and mid25[0][2] == "recompensate",
        f"T entre la trace et son annulation : plan={mid25} : a T, U etait "
        f"compense.",
    ))

    g = d.get("g") or {}
    out.append((
        "g_same_instant_toggles_unwind_in_trace_order",
        g.get("final") == {"a": 2} and len(g.get("active") or []) == 2,
        f"plan={g.get('active')} ordre={g.get('order')} valeur finale="
        f"{g.get('final')} attendu a=2 : deux traces d'un meme passage "
        f"partagent l'horodatage ; les defaire dans le mauvais ordre laisse "
        f"la valeur d'avant.",
    ))

    h = d.get("h") or {}
    out.append((
        "h_legacy_fused_compensation_is_refused_by_name",
        h.get("verdict") == "blocked"
        and any("fused" in r for r in h.get("blocking") or []),
        f"verdict={h.get('verdict')} motifs={h.get('blocking')} plan="
        f"{h.get('active')} : l'ancienne compensation fusionnee ne peut pas "
        f"etre defaite exactement ; ecrire quand meme reinsere l'entite en "
        f"double ou la laisse dans un etat faux.",
    ))
    h2 = d.get("h2") or {}
    out.append((
        "h2_legacy_fused_long_chain_is_refused_not_half_undone",
        h2.get("verdict") == "blocked"
        and any("fused" in r for r in h2.get("blocking") or [])
        and h2.get("u1") not in (h2.get("undo") or []),
        f"verdict={h2.get('verdict')} motifs={h2.get('blocking')} traces a "
        f"defaire une par une={h2.get('undo')} (U1={h2.get('u1')}) : l'ancienne "
        f"trace compensait 11 UPDATE d'un coup ; la defaire comme si elle ne "
        f"couvrait que U1 re-applique U1 seul, le journal croit les 10 autres "
        f"revenus alors que la couche les a toujours defaits.",
    ))
    j = d.get("j") or {}
    j_ids = [x[0] for x in j.get("active") or []]
    out.append((
        "j_reimported_edit_is_not_hidden_by_a_legacy_span",
        j.get("p") in j_ids and j.get("u") not in j_ids and j.get("d") not in j_ids,
        f"actifs={j.get('active')} P={j.get('p')} (reimporte du fichier pending "
        f"apres la trace {j.get('trace')}) U={j.get('u')} D={j.get('d')} : P n'a "
        f"jamais ete compense ; le cacher laisse b=1 apres un rewind qui annonce "
        f"l'etat d'avant U.",
    ))
    k = d.get("k") or {}
    k_values = [(s[0], s[2]) for s in k.get("steps") or []]
    out.append((
        "k_reapplied_edit_is_unwound_where_it_was_reapplied",
        k_values == [(5, 0), (15, 1), (35, 2), (15, 1)],
        f"(date, valeur de a apres le rewind)={k_values} attendu "
        f"[(5, 0), (15, 1), (35, 2), (15, 1)] ; plans={k.get('steps')} : "
        f"U7={k.get('u7')} re-applique par le saut en avant vers 35 est dans "
        f"la couche depuis ce saut ; le compenser a sa propre date, APRES la "
        f"re-application de U3={k.get('u3')}, ecrase la valeur de U3 et "
        f"laisse a=0 au lieu de 1, rewind annonce applique.",
    ))
    i = d.get("i") or {}
    out.append((
        "i_purged_source_is_refused_by_name",
        i.get("verdict") == "blocked"
        and any("purged" in r for r in i.get("blocking") or []),
        f"verdict={i.get('verdict')} motifs={i.get('blocking')} plan="
        f"{i.get('active')} : l'etat a T contient une edition que la retention "
        f"a purgee ; 'Rien a restaurer' serait un faux succes.",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"rw_state_at_t_engine_cases.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


def teardown(ctx):
    """Ferme le journal en memoire ouvert par setup(ctx).

    Sous Python 3.12 une sqlite3.Connection se reference elle-meme (cache
    de requetes) : non fermee, elle restait ouverte apres le scenario,
    jusqu'au prochain passage du ramasse-miettes, pendant un autre.
    """
    conn = ctx.data.pop("conn", None)
    if conn is not None:
        conn.close()


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
