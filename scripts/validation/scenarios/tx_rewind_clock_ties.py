"""tx_rewind_clock_ties - RecoverLand validation runtime.

Invariant: I-8 (le journal est la seule memoire du rewind) + I-9 (fenetre de
lecture inclusive).
Sujet: l'ordre chronologique reel des evenements quand l'horloge et le
compteur d'event_id ne sont PAS d'accord.

Le mecanisme
------------
Deux tris coexistent dans le pipeline de rewind :

  * la lecture (`event_stream_repository.fetch_events_after_cutoff`) et la
    logique de span (`rewind_dedup._order_key`) trient sur
    ``(created_at, event_id)`` -- l'horloge d'abord, l'event_id seulement
    comme depart de tie-break ;
  * la detection de recyclage de FID (`rewind_dedup._detect_fid_recycle`)
    trie sur ``event_id`` SEUL :

        sorted_events = sorted([...], key=lambda e: e.event_id)

Tant que le journal est ecrit dans l'ordre, les deux tris coincident. La
reintegration des evenements en attente casse cet accord : un evenement
ancien (created_at vieux) est reinsere apres un crash et recoit un event_id
RECENT. A partir de la, l'automate de recyclage de FID lit la vie de
l'entite A L'ENVERS.

Scenario d'echec concret pour l'utilisateur
-------------------------------------------
Cas T3 - l'utilisateur cree une entite puis la supprime, le tout apres la
date de rewind. L'INSERT est reste en attente (crash), il est reintegre
apres le DELETE et recoit donc un event_id plus grand. L'automate voit
DELETE puis INSERT : il croit a un recyclage de FID, coupe la vie de
l'entite en deux seaux, et la paire INSERT/DELETE ne s'annule plus. Le
rewind RESSUSCITE une entite que l'utilisateur avait creee ET supprimee :
un fantome apparait dans la couche.

Cas T4 - l'utilisateur modifie un attribut puis supprime l'entite, l'UPDATE
etant l'evenement reintegre. Meme faux split : la fusion UPDATE->DELETE
n'a plus lieu. Consequence double :
  * l'entite revient avec la valeur POST-edition ("B") au lieu de sa valeur
    a la date de rewind ("A") -- perte de donnee silencieuse ;
  * la compensation UPDATE de phase 1 vise une entite qui n'existe pas
    encore (phase 2 ne l'a pas encore reinseree) et echoue en
    ``target_absent`` -- c'est exactement le mode d'echec que la fusion
    UPDATE->DELETE avait ete ecrite pour supprimer.

Cas T1 et T2 verifient l'autre moitie du sujet : deux evenements portant le
MEME created_at a la microseconde pres sur la meme entite. Le tri doit
rester deterministe et designer le bon evenement "le plus ancien", sinon la
valeur restauree est celle du milieu de la chaine.

Plan du scenario (journal SQLite reel, dedup reel, zero objet QGIS) :
    T1  ds=tx_ct_tie_uu  fid:71001  UPDATE A->B et UPDATE B->C, meme instant
    T2  ds=tx_ct_tie_ud  fid:71002  UPDATE A->B puis DELETE, meme instant
    T3  ds=tx_ct_ooo_id  fid:71003  INSERT(t+10, eid grand) / DELETE(t+20, eid petit)
    T4  ds=tx_ct_ooo_ud  fid:71004  UPDATE(t+10, eid grand) / DELETE(t+20, eid petit)
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_rewind_clock_ties"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "tx_ct_project"
_FIELDS = json.dumps([
    {"name": "code", "type": "string"},
    {"name": "nom", "type": "string"},
])

# Un fingerprint d'entite unique par cas : les assertions de log doivent
# pouvoir viser ce scenario et pas un voisin qui tournerait dans la meme
# fenetre de log.
_DS_TIE_UU = "tx_ct_tie_uu"
_DS_TIE_UD = "tx_ct_tie_ud"
_DS_OOO_ID = "tx_ct_ooo_id"
_DS_OOO_UD = "tx_ct_ooo_ud"

_FP_TIE_UU = "fid:71001"
_FP_TIE_UD = "fid:71002"
_FP_OOO_ID = "fid:71003"
_FP_OOO_UD = "fid:71004"


def _iso(t: datetime) -> str:
    """Meme forme que edit_tracker : datetime.now(timezone.utc).isoformat()."""
    return t.isoformat()


def _identity(fp: str) -> str:
    return json.dumps({"fid": int(fp.split(":")[1])})


def _insert(conn, ds, ts, op, attrs, fp, restored_from=None) -> int:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    sql = (
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
        + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )
    row = (
        _PROJECT_FP, ds, "lyr_" + ds, ds, "ogr",
        _identity(fp), op, json.dumps(attrs, ensure_ascii=False),
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

    # --- T1 : deux UPDATE au MEME instant (microseconde identique) --------
    tie = _iso(t0 + timedelta(seconds=10, microseconds=123456))
    eids["t1_first"] = _insert(
        conn, _DS_TIE_UU, tie, "UPDATE",
        _delta("code", "A", "B"), _FP_TIE_UU)
    eids["t1_second"] = _insert(
        conn, _DS_TIE_UU, tie, "UPDATE",
        _delta("code", "B", "C"), _FP_TIE_UU)

    # --- T2 : UPDATE puis DELETE au MEME instant --------------------------
    tie2 = _iso(t0 + timedelta(seconds=20, microseconds=654321))
    eids["t2_update"] = _insert(
        conn, _DS_TIE_UD, tie2, "UPDATE",
        _delta("code", "A", "B"), _FP_TIE_UD)
    eids["t2_delete"] = _insert(
        conn, _DS_TIE_UD, tie2, "DELETE",
        {"all_attributes": {"code": "B", "nom": "PONT"}}, _FP_TIE_UD)

    # --- T3 : INSERT reintegre APRES le DELETE (event_id a l'envers) ------
    # Ordre d'insertion = ordre des event_id. Le DELETE (t+20) est ecrit en
    # premier, l'INSERT (t+10) est l'evenement en attente reintegre ensuite.
    eids["t3_delete"] = _insert(
        conn, _DS_OOO_ID, _iso(t0 + timedelta(seconds=20)), "DELETE",
        {"all_attributes": {"code": "Z", "nom": "TEMP"}}, _FP_OOO_ID)
    eids["t3_insert"] = _insert(
        conn, _DS_OOO_ID, _iso(t0 + timedelta(seconds=10)), "INSERT",
        {"all_attributes": {"code": "Z", "nom": "TEMP"}}, _FP_OOO_ID)

    # --- T4 : UPDATE reintegre APRES le DELETE ----------------------------
    eids["t4_delete"] = _insert(
        conn, _DS_OOO_UD, _iso(t0 + timedelta(seconds=20)), "DELETE",
        {"all_attributes": {"code": "B", "nom": "PONT"}}, _FP_OOO_UD)
    eids["t4_update"] = _insert(
        conn, _DS_OOO_UD, _iso(t0 + timedelta(seconds=10)), "UPDATE",
        _delta("code", "A", "B"), _FP_OOO_UD)

    conn.commit()

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["eids"] = eids

    flog(
        f"tx_rewind_clock_ties setup: trace_id={ctx.trace_id} eids={eids}",
        "INFO",
    )


def _collapse(ctx, ds):
    """fetch + dedup reels pour une datasource, cutoff = t0 inclusif."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats

    cutoff = RestoreCutoff(CutoffType.BY_DATE, _iso(ctx.data["t0"]), inclusive=True)
    events = fetch_events_after_cutoff(
        ctx.data["conn"], ds, cutoff, trace_id=ctx.trace_id)
    active, stats = collapse_rewind_events_with_stats(events)
    return {
        "fetched": [e.event_id for e in events],
        "active": [e.event_id for e in active],
        "active_ops": [e.operation_type for e in active],
        "events": events,
        "active_events": active,
        "stats": dict(stats),
    }


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.search_service import reconstruct_attributes
    from recoverland.core.serialization import extract_delta_old
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType

    flog(f"tx_rewind_clock_ties run start: trace_id={ctx.trace_id}", "INFO")

    t1 = _collapse(ctx, _DS_TIE_UU)
    t2 = _collapse(ctx, _DS_TIE_UD)
    t3 = _collapse(ctx, _DS_OOO_ID)
    t4 = _collapse(ctx, _DS_OOO_UD)

    # T1 : quelle valeur le rewind va-t-il finalement reecrire ?
    # L'evenement le plus ancien de la chaine porte la valeur d'origine.
    if t1["active_events"]:
        oldest = t1["active_events"][-1]
        payload = json.loads(oldest.attributes_json).get("changed_only", {})
        t1["oldest_old_value"] = extract_delta_old(payload.get("code"))
    cutoff = RestoreCutoff(CutoffType.BY_DATE, _iso(ctx.data["t0"]), inclusive=True)
    plan = plan_temporal_restore(
        t1["active_events"], _DS_TIE_UU, "tie_uu", cutoff)
    t1["plan_order"] = [a.event_id for a in plan.actions]
    t1["plan_ops"] = [a.compensatory_op for a in plan.actions]

    # T2 / T4 : etat que la compensation INSERT va reecrire dans la couche.
    for case in (t2, t4):
        case["restored_attrs"] = [
            reconstruct_attributes(e) for e in case["active_events"]
        ]

    for name, case in (("t1", t1), ("t2", t2), ("t3", t3), ("t4", t4)):
        case.pop("events", None)
        case.pop("active_events", None)
        ctx.data[name] = case

    flog(
        f"tx_rewind_clock_ties run end: trace_id={ctx.trace_id} "
        f"t1={t1['active']} t2={t2['active']} "
        f"t3={t3['active']} t4={t4['active']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_absent, assert_log_contains

    out = []
    eids = ctx.data["eids"]
    t1 = ctx.data.get("t1") or {}
    t2 = ctx.data.get("t2") or {}
    t3 = ctx.data.get("t3") or {}
    t4 = ctx.data.get("t4") or {}

    # ===== T1 : egalite parfaite de created_at, deux UPDATE ==============
    e1a, e1b = eids["t1_first"], eids["t1_second"]
    out.append((
        "tie_uu_fetch_is_deterministic",
        t1.get("fetched") == [e1b, e1a],
        f"fetched={t1.get('fetched')} attendu=[{e1b}, {e1a}] : deux editions "
        f"enregistrees a la meme microseconde doivent quand meme etre lues du "
        f"plus recent au plus ancien, sinon le rewind rejoue les compensations "
        f"dans le desordre et l'attribut finit sur une valeur intermediaire",
    ))
    out.append((
        "tie_uu_chain_kept_whole",
        sorted(t1.get("active") or []) == sorted([e1a, e1b]),
        f"active={t1.get('active')} attendu={{{e1a}, {e1b}}} : aucune des deux "
        f"editions ne doit disparaitre, elles ne sont compensees par aucune trace",
    ))
    out.append((
        "tie_uu_restores_the_original_value",
        t1.get("oldest_old_value") == "A",
        f"valeur restauree={t1.get('oldest_old_value')!r} attendu='A' : si le tri "
        f"designe la mauvaise edition comme la plus ancienne, l'utilisateur "
        f"recupere 'B' (l'etat intermediaire) au lieu de la valeur qu'il avait "
        f"a la date demandee",
    ))
    out.append((
        "tie_uu_plan_undoes_newest_first",
        t1.get("plan_order") == [e1b, e1a],
        f"ordre du plan={t1.get('plan_order')} attendu=[{e1b}, {e1a}] : la "
        f"compensation la plus recente doit passer en premier, sinon la "
        f"deuxieme compensation ecrase le travail de la premiere",
    ))

    # ===== T2 : egalite de created_at, UPDATE puis DELETE ================
    out.append((
        "tie_ud_fused_into_one_delete",
        (t2.get("active_ops") == ["DELETE"]),
        f"active_ops={t2.get('active_ops')} attendu=['DELETE'] : la chaine "
        f"UPDATE->DELETE doit fusionner en une seule reinsertion, sinon la "
        f"compensation UPDATE vise une entite encore absente et echoue",
    ))
    restored_t2 = (t2.get("restored_attrs") or [{}])[0]
    out.append((
        "tie_ud_reinserts_the_cutoff_state",
        restored_t2 == {"code": "A", "nom": "PONT"},
        f"etat reinsere={restored_t2} attendu={{'code': 'A', 'nom': 'PONT'}} : "
        f"a egalite d'horodatage la fusion doit retenir la valeur d'AVANT "
        f"l'edition ('A'), sinon l'entite revient avec la valeur editee et "
        f"l'utilisateur croit que le rewind a fonctionne",
    ))

    # ===== T3 : event_id a l'envers, INSERT puis DELETE ==================
    out.append((
        "ooo_insert_delete_is_a_noop",
        (t3.get("active") or []) == [],
        f"active={t3.get('active')} attendu=[] : l'entite a ete creee PUIS "
        f"supprimee apres la date de rewind, il n'y a rien a restaurer. "
        f"Elle est ici ressuscitee parce que l'INSERT, reintegre apres coup, "
        f"porte un event_id plus grand que le DELETE : un fantome apparait "
        f"dans la couche a chaque rewind",
    ))
    out.append((
        "ooo_no_ghost_reinsertion",
        "DELETE" not in (t3.get("active_ops") or []),
        f"active_ops={t3.get('active_ops')} : un DELETE laisse actif se "
        f"compense par un INSERT. L'entite supprimee par l'utilisateur est "
        f"donc reecrite dans la couche alors qu'elle n'existait pas a la date "
        f"demandee",
    ))
    out.append(assert_log_absent(
        ctx.records,
        rf"fid_recycle_detected fp={_FP_OOO_ID}\b",
        name="ooo_no_phantom_fid_recycle",
    ))

    # ===== T4 : event_id a l'envers, UPDATE puis DELETE ==================
    out.append((
        "ooo_update_delete_fused",
        t4.get("active_ops") == ["DELETE"],
        f"active_ops={t4.get('active_ops')} attendu=['DELETE'] : la chaine "
        f"UPDATE->DELETE doit fusionner. Non fusionnee, la compensation UPDATE "
        f"de phase 1 vise une entite que la phase 2 n'a pas encore reinseree "
        f"et echoue en target_absent : le rewind se termine 'partiellement "
        f"applique' sans que l'utilisateur sache quoi",
    ))
    restored_t4 = (t4.get("restored_attrs") or [{}])[0]
    out.append((
        "ooo_reinserts_the_cutoff_state",
        restored_t4 == {"code": "A", "nom": "PONT"},
        f"etat reinsere={restored_t4} attendu={{'code': 'A', 'nom': 'PONT'}} : "
        f"l'entite doit revenir avec la valeur qu'elle avait a la date "
        f"demandee. Elle revient avec 'B', la valeur editee apres cette date : "
        f"perte de donnee silencieuse, le rewind se declare reussi",
    ))
    out.append(assert_log_absent(
        ctx.records,
        rf"fid_recycle_detected fp={_FP_OOO_UD}\b",
        name="ooo_ud_no_phantom_fid_recycle",
    ))

    # ===== Isolation entre couches ======================================
    out.append((
        "cases_stay_isolated",
        all(len(c.get("fetched") or []) == 2 for c in (t1, t2, t3, t4)),
        f"tailles de fenetre t1={len(t1.get('fetched') or [])} "
        f"t2={len(t2.get('fetched') or [])} t3={len(t3.get('fetched') or [])} "
        f"t4={len(t4.get('fetched') or [])} attendu=2 partout : chaque cas doit "
        f"lire uniquement sa propre couche",
    ))

    # ===== Propagation de trace =========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_rewind_clock_ties.*trace_id={ctx.trace_id}",
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
