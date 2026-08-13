"""tx_rewind_mass_and_multilayer - RecoverLand validation runtime.

Invariant: I-8 (dedup de rewind) - completude de la fusion de chaines longues
et etancheite entre couches.
Sujet: le rewind en VOLUME, la ou la fusion de chaines longues se declenche
en masse, et le rewind MULTI-COUCHES ou le meme FID designe trois entites
differentes.

Le mecanisme
------------
Au-dela de ``_MAX_CHAIN`` (10) evenements sur une meme entite, le dedup ne
garde plus la chaine : il la FUSIONNE en un evenement synthetique unique
(`rewind_dedup._fuse_long_chain`). Ce synthetique doit porter, pour CHAQUE
champ touche quelque part dans la chaine, la valeur d'AVANT le tout premier
changement - c'est elle qui sera reecrite dans la couche. Un champ oublie
n'est jamais restaure et personne ne s'en apercoit : l'operation se declare
reussie.

La fusion parcourt la chaine du plus recent au plus ancien et ecrase a
chaque tour le cote "old" du champ. Sur une entite isolee c'est facile a
verifier ; le risque reel apparait en masse, quand 500 chaines sont
fusionnees dans la meme passe et que les seaux par entite doivent rester
etanches. Une seule collision de cle et une entite recupere les valeurs
d'une autre.

Deuxieme front : le FID. ``fid:5`` designe une entite differente dans chaque
couche. Le keying du dedup porte la datasource (``_entity_key``), la
detection de recyclage de FID est scopee par ``(datasource, fingerprint)``,
et la neutralisation par trace est scopee de la meme facon. Ce scenario le
verifie de deux facons : couche par couche (ce que fait le produit) ET sur
le flux concatene et retrie par horodatage (ce que ferait une lecture
globale), car les deux doivent donner exactement le meme resultat.

Scenario d'echec concret pour l'utilisateur
-------------------------------------------
M1 - un leve de terrain : 500 objets, 15 corrections d'attributs chacun
(15 champs differents). L'utilisateur revient a la veille. Si la fusion
perd des champs ou melange les entites, la couche revient avec des valeurs
d'une autre entite, et le rapport final affiche "500 entites restaurees".

M2 - trois couches (reseau, parcelles, batiments) partagent le FID 5.
La couche parcelles a deja ete rembobinee (elle porte une trace). Si la
trace ou un faux recyclage de FID deborde d'une couche a l'autre, le rewind
saute l'entite 5 du reseau en silence ou la restaure deux fois.

Plan du scenario (journal SQLite reel, dedup reel, zero objet QGIS) :
    M1  ds=tx_mx_mass    500 entites x 15 UPDATE = 7500 evenements
    M2  ds=tx_mx_net / tx_mx_par / tx_mx_bat, tous avec fid:50005
          net : U -> U                (2 actifs)
          par : U -> trace(U) -> U    (1 actif, l'edition post-rewind)
          bat : INSERT -> DELETE      (0 actif, vie entiere dans la fenetre)
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_rewind_mass_and_multilayer"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "tx_mx_project"

_DS_MASS = "tx_mx_mass"
_DS_NET = "tx_mx_net"
_DS_PAR = "tx_mx_par"
_DS_BAT = "tx_mx_bat"

_N_ENTITIES = 500
_N_EDITS = 15          # > _MAX_CHAIN (10) : declenche _fuse_long_chain
_FP_SHARED = "fid:50005"   # le meme FID dans les trois couches
_MAX_COLLAPSE_MS = 15000


def _iso(t: datetime) -> str:
    return t.isoformat()


def _fields_json(n_fields: int) -> str:
    return json.dumps(
        [{"name": f"f{k:02d}", "type": "string"} for k in range(n_fields)])


def _row(ds, ts, op, attrs, fp, identity, restored_from=None):
    return (
        _PROJECT_FP, ds, "lyr_" + ds, ds, "ogr",
        identity, op, json.dumps(attrs, ensure_ascii=False),
        None, "Point", "EPSG:4326", _fields_json(_N_EDITS),
        "tester", None, ts, restored_from, fp, 2, None, None,
    )


def _insert_sql():
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    return ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
            + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")


def _delta(field, old, new):
    return {"changed_only": {field: {"old": old, "new": new}}}


def _cut_value(entity_no: int, field_no: int) -> str:
    """Valeur du champ AVANT la premiere edition = valeur a restaurer."""
    return f"e{entity_no:03d}f{field_no:02d}cut"


def _live_value(entity_no: int, field_no: int) -> str:
    return f"e{entity_no:03d}f{field_no:02d}live"


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    sql = _insert_sql()

    # ===== M1 : 500 entites, 15 editions chacune ========================
    # Les editions sont entrelacees (tour par tour) comme dans une vraie
    # session : ronde 0 sur les 500 entites, puis ronde 1, etc.
    batch = []
    for round_no in range(_N_EDITS):
        ts = _iso(t0 + timedelta(seconds=10 + round_no))
        field = f"f{round_no:02d}"
        for entity_no in range(_N_ENTITIES):
            fp = f"fid:{80000 + entity_no}"
            batch.append(_row(
                _DS_MASS, ts, "UPDATE",
                _delta(field, _cut_value(entity_no, round_no),
                       _live_value(entity_no, round_no)),
                fp, json.dumps({"fid": 80000 + entity_no})))
    conn.executemany(sql, batch)

    # ===== M2 : le meme FID dans trois couches ==========================
    ident = json.dumps({"fid": 50005})
    eids = {}

    def ins(ds, secs, op, attrs, restored_from=None):
        return conn.execute(sql, _row(
            ds, _iso(t0 + timedelta(seconds=secs)), op, attrs,
            _FP_SHARED, ident, restored_from)).lastrowid

    eids["net_u1"] = ins(_DS_NET, 10, "UPDATE", _delta("f00", "A", "B"))
    eids["net_u2"] = ins(_DS_NET, 20, "UPDATE", _delta("f00", "B", "C"))

    eids["par_u1"] = ins(_DS_PAR, 10, "UPDATE", _delta("f00", "A", "B"))
    eids["par_trace"] = ins(
        _DS_PAR, 20, "UPDATE", {"_restore_ref": eids["par_u1"]},
        restored_from=eids["par_u1"])
    eids["par_u2"] = ins(_DS_PAR, 30, "UPDATE", _delta("f00", "A", "D"))

    eids["bat_insert"] = ins(
        _DS_BAT, 10, "INSERT", {"all_attributes": {"f00": "N"}})
    eids["bat_delete"] = ins(
        _DS_BAT, 20, "DELETE", {"all_attributes": {"f00": "N"}})

    conn.commit()

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["eids"] = eids

    flog(
        f"tx_rewind_mass_and_multilayer setup: trace_id={ctx.trace_id} "
        f"mass_rows={len(batch)} multilayer_eids={eids}",
        "INFO",
    )


def _fetch(ctx, ds):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType

    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, _iso(ctx.data["t0"]), inclusive=True)
    return cutoff, fetch_events_after_cutoff(
        ctx.data["conn"], ds, cutoff, trace_id=ctx.trace_id)


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import plan_temporal_restore, preflight_check

    flog(f"tx_rewind_mass_and_multilayer run start: trace_id={ctx.trace_id}",
         "INFO")

    # ===== M1 : fusion de chaines longues en masse ======================
    cutoff, events = _fetch(ctx, _DS_MASS)
    started = time.monotonic()
    active, stats = collapse_rewind_events_with_stats(events)
    elapsed_ms = int((time.monotonic() - started) * 1000)

    # Pour chaque entite : les champs presents et leur valeur "old", qui est
    # exactement ce que la compensation reecrira dans la couche.
    per_entity = {}
    for ev in active:
        payload = {}
        try:
            payload = json.loads(ev.attributes_json).get("changed_only", {})
        except (ValueError, TypeError):
            payload = {}
        per_entity[ev.entity_fingerprint] = {
            name: (chg.get("old"), chg.get("new"))
            for name, chg in payload.items()
        }

    missing_fields = []      # (entite, nb_champs)
    wrong_values = []        # (entite, champ, obtenu, attendu)
    for entity_no in range(_N_ENTITIES):
        fp = f"fid:{80000 + entity_no}"
        got = per_entity.get(fp)
        if got is None:
            missing_fields.append((fp, None))
            continue
        if len(got) != _N_EDITS:
            missing_fields.append((fp, len(got)))
        for field_no in range(_N_EDITS):
            name = f"f{field_no:02d}"
            pair = got.get(name)
            expected = _cut_value(entity_no, field_no)
            if pair is None or pair[0] != expected:
                wrong_values.append(
                    (fp, name, None if pair is None else pair[0], expected))

    plan = plan_temporal_restore(active, _DS_MASS, "mass", cutoff)
    report = preflight_check(plan)

    ctx.data["mass"] = {
        "fetched": len(events),
        "active": len(active),
        "entities_seen": len(per_entity),
        "missing_fields": missing_fields[:5],
        "n_missing": len(missing_fields),
        "wrong_values": wrong_values[:5],
        "n_wrong": len(wrong_values),
        "elapsed_ms": elapsed_ms,
        "verdict": report.verdict.value,
        "n_actions": len(plan.actions),
        "entity_count": plan.entity_count,
        "stats": dict(stats),
    }

    # ===== M2 : trois couches, le meme FID ==============================
    per_layer = {}
    all_events = []
    for ds in (_DS_NET, _DS_PAR, _DS_BAT):
        _c, evts = _fetch(ctx, ds)
        all_events.extend(evts)
        act, st = collapse_rewind_events_with_stats(evts)
        per_layer[ds] = {
            "fetched": [e.event_id for e in evts],
            "active": sorted(e.event_id for e in act),
            "stats": dict(st),
        }

    # Lecture "globale" : les trois couches melangees et retriees comme le
    # ferait une requete sans filtre de datasource.
    all_events.sort(
        key=lambda e: ((e.created_at or ""), (e.event_id or 0)), reverse=True)
    union_active, union_stats = collapse_rewind_events_with_stats(all_events)
    union_by_ds = {}
    for ev in union_active:
        union_by_ds.setdefault(ev.datasource_fingerprint, []).append(ev.event_id)
    for ds in union_by_ds:
        union_by_ds[ds] = sorted(union_by_ds[ds])

    ctx.data["multilayer"] = {
        "per_layer": per_layer,
        "union": union_by_ds,
        "union_stats": dict(union_stats),
    }

    flog(
        f"tx_rewind_mass_and_multilayer run end: trace_id={ctx.trace_id} "
        f"mass_active={ctx.data['mass']['active']} "
        f"mass_wrong={ctx.data['mass']['n_wrong']} "
        f"elapsed_ms={elapsed_ms} union={union_by_ds}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_absent, assert_log_contains

    out = []
    eids = ctx.data["eids"]
    mass = ctx.data.get("mass") or {}
    multi = ctx.data.get("multilayer") or {}
    per_layer = multi.get("per_layer") or {}
    union = multi.get("union") or {}

    # ===== M1 : volume et completude de la fusion =======================
    out.append((
        "mass_window_is_complete",
        mass.get("fetched") == _N_ENTITIES * _N_EDITS,
        f"evenements lus={mass.get('fetched')} attendu="
        f"{_N_ENTITIES * _N_EDITS} : une fenetre tronquee invaliderait tout le "
        f"reste du cas",
    ))
    out.append((
        "mass_one_action_per_entity",
        mass.get("active") == _N_ENTITIES,
        f"evenements actifs={mass.get('active')} attendu={_N_ENTITIES} : chaque "
        f"entite doit se resumer a UNE reecriture. Plus, et la couche subit "
        f"des ecritures redondantes ; moins, et des entites ne sont jamais "
        f"restaurees",
    ))
    out.append((
        "mass_every_entity_keeps_all_its_fields",
        mass.get("n_missing") == 0,
        f"entites incompletes={mass.get('n_missing')} sur {_N_ENTITIES} "
        f"(echantillon={mass.get('missing_fields')}) : un champ absent de "
        f"l'evenement fusionne n'est JAMAIS remis a sa valeur d'origine, et le "
        f"rapport annonce quand meme la restauration reussie",
    ))
    out.append((
        "mass_every_field_carries_its_own_cutoff_value",
        mass.get("n_wrong") == 0,
        f"valeurs fausses={mass.get('n_wrong')} sur "
        f"{_N_ENTITIES * _N_EDITS} (echantillon={mass.get('wrong_values')}) : "
        f"chaque champ doit revenir a la valeur qu'il avait a la date "
        f"demandee, pour SON entite. Une valeur venue d'une autre entite est "
        f"une corruption silencieuse des donnees",
    ))
    out.append((
        "mass_no_entity_lost",
        mass.get("entities_seen") == _N_ENTITIES,
        f"entites presentes dans le resultat={mass.get('entities_seen')} "
        f"attendu={_N_ENTITIES} : une entite absente du resultat reste dans "
        f"son etat edite sans que rien ne le signale",
    ))
    out.append((
        "mass_plan_is_not_refused",
        mass.get("verdict") in ("go", "go_with_warnings")
        and mass.get("n_actions") == _N_ENTITIES,
        f"verdict preflight={mass.get('verdict')!r} actions="
        f"{mass.get('n_actions')} entites={mass.get('entity_count')} : 500 "
        f"entites restent sous la limite de {1000}, le rewind doit partir "
        f"(avec avertissement de volume)",
    ))
    out.append((
        "mass_collapse_stays_responsive",
        (mass.get("elapsed_ms") or _MAX_COLLAPSE_MS + 1) < _MAX_COLLAPSE_MS,
        f"duree du dedup={mass.get('elapsed_ms')} ms pour "
        f"{_N_ENTITIES * _N_EDITS} evenements, plafond={_MAX_COLLAPSE_MS} ms : "
        f"le dedup tourne avant l'ecriture, une lenteur ici fige la fenetre "
        f"pendant que l'utilisateur attend",
    ))

    # ===== M2 : etancheite entre couches ================================
    net = per_layer.get(_DS_NET) or {}
    par = per_layer.get(_DS_PAR) or {}
    bat = per_layer.get(_DS_BAT) or {}

    out.append((
        "layer_without_trace_keeps_both_edits",
        net.get("active") == sorted([eids["net_u1"], eids["net_u2"]]),
        f"actifs couche reseau={net.get('active')} attendu="
        f"{sorted([eids['net_u1'], eids['net_u2']])} : cette couche n'a jamais "
        f"ete rembobinee, la trace de la couche parcelles ne doit pas "
        f"neutraliser son entite fid:50005 qui porte pourtant le meme FID",
    ))
    out.append((
        "layer_with_trace_keeps_only_the_post_rewind_edit",
        par.get("active") == [eids["par_u2"]],
        f"actifs couche parcelles={par.get('active')} attendu="
        f"[{eids['par_u2']}] : l'edition compensee doit rester neutralisee et "
        f"celle faite apres le rewind doit survivre",
    ))
    out.append((
        "layer_born_and_dead_in_window_has_nothing_to_restore",
        bat.get("active") == [],
        f"actifs couche batiments={bat.get('active')} attendu=[] : l'entite a "
        f"ete creee puis supprimee apres la date demandee, la rejouer "
        f"ressusciterait un fantome",
    ))
    out.append((
        "global_read_matches_per_layer_read",
        union == {ds: v["active"] for ds, v in per_layer.items() if v["active"]},
        f"lecture globale={union} lecture par couche="
        f"{{ds: v['active'] for ds, v in per_layer.items()}} : le meme FID "
        f"dans trois couches ne doit pas changer le resultat selon que le "
        f"journal est lu couche par couche ou d'un bloc",
    ))
    out.append(assert_log_absent(
        ctx.records,
        rf"fid_recycle_detected fp={_FP_SHARED}\b",
        name="no_cross_layer_fid_recycle_split",
    ))

    # ===== Propagation de trace =========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_rewind_mass_and_multilayer.*trace_id={ctx.trace_id}",
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
