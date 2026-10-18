"""rw_planner_chronology - RecoverLand validation runtime.

Invariant: I-8 (les compensations d'un rewind sont appliquees du plus
recent au plus ancien, dans la MEME chronologie que la lecture et le dedup).

Le mecanisme
------------
La lecture de la fenetre (`fetch_events_after_cutoff`) et le dedup
(`rewind_dedup._order_key`) ordonnent sur (created_at, event_id). Le
planificateur (`restore_planner._temporal_action_order`) triait la phase
des UPDATE sur ``-event_id``. Les deux ordres coincident tant que les
event_id suivent le temps. Ce n'est plus le cas apres un crash : un
evenement retenu dans le fichier pending puis reimporte garde son
created_at d'origine mais recoit un event_id NEUF, plus grand que celui
des editions faites entre-temps.

Echec concret pour l'utilisateur
--------------------------------
U_old (a:0->1, 10:00, event_id 500 apres reimport) et U_new (a:1->2,
11:00, event_id 100), rewind a 09:00. Le plan appliquait 500 puis 100 :
a=0 puis a=1. La couche finit sur a=1 au lieu de 0, les deux actions sont
comptees "appliquees" (sur une couche a cle primaire,
`_settle_update_target` fait confiance a la cle et ecrit OLD sans relire
l'etat vivant).

Le scenario simule l'ecriture des compensations dans l'ordre du plan et
verifie la valeur finale, puis que l'ordre ne depend pas de l'ordre
d'entree.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "rw_planner_chronology"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS = "rw_pc_datasource"


def _ev(eid, ts, old, new):
    from recoverland.core.audit_backend import AuditEvent
    return AuditEvent(
        event_id=eid, project_fingerprint="rw_pc_project",
        datasource_fingerprint=_DS, layer_id_snapshot="lyr_pc",
        layer_name_snapshot="pc", provider_type="ogr",
        feature_identity_json=json.dumps({"fid": 3, "pk_field": "gid",
                                          "pk_value": 3}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {"a": {"old": old, "new": new}}}),
        geometry_wkb=None, geometry_type="NoGeometry", crs_authid=None,
        field_schema_json=None, user_name="tester", session_id="s",
        created_at=ts, restored_from_event_id=None,
        entity_fingerprint="pk:gid=3", event_schema_version=7,
    )


def setup(ctx):
    from recoverland.core.logger import flog
    # 10:00 edit re-imported from the pending file after a crash: old date,
    # NEW event_id. 11:00 edit captured normally in between.
    ctx.data["u_old"] = _ev(500, "2026-09-01T10:00:00+00:00", 0, 1)
    ctx.data["u_new"] = _ev(100, "2026-09-01T11:00:00+00:00", 1, 2)
    flog(f"rw_planner_chronology setup: trace_id={ctx.trace_id}", "INFO")


def _apply(plan, events):
    """Write each UPDATE compensation's OLD values, in plan order."""
    from recoverland.core.search_service import reconstruct_attributes
    by_id = {e.event_id: e for e in events}
    state = {"a": 2}
    for action in plan.actions:
        state.update(reconstruct_attributes(by_id[action.event_id]))
    return state


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    cutoff = RestoreCutoff(CutoffType.BY_DATE, "2026-09-01T09:00:00+00:00",
                           inclusive=True)
    old, new = ctx.data["u_old"], ctx.data["u_new"]
    orders = {}
    finals = {}
    for label, events in (("fetch_order", [new, old]),
                          ("eid_desc", [old, new]),
                          ("collapsed", collapse_rewind_events_with_stats(
                              [old, new])[0])):
        plan = plan_temporal_restore(events, _DS, "pc", cutoff)
        orders[label] = [a.event_id for a in plan.actions]
        finals[label] = _apply(plan, events)
    ctx.data["orders"] = orders
    ctx.data["finals"] = finals
    flog(f"rw_planner_chronology run: trace_id={ctx.trace_id} "
         f"orders={orders} finals={finals}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    orders = ctx.data.get("orders") or {}
    finals = ctx.data.get("finals") or {}
    out = [(
        "newest_update_is_undone_first",
        orders.get("fetch_order") == [100, 500],
        f"ordre du plan={orders.get('fetch_order')} attendu=[100, 500] : 100 "
        f"(11:00) doit etre defait avant 500 (10:00, reimporte). Sinon la "
        f"couche finit sur la valeur intermediaire.",
    ), (
        "field_returns_to_its_value_at_the_cutoff",
        all(f == {"a": 0} for f in finals.values()) and len(finals) == 3,
        f"valeur finale par ordre d'entree={finals} attendu a=0 partout : "
        f"a=1 = l'utilisateur recoit une couche qui n'a jamais existe a la "
        f"date demandee, annoncee 'appliquee'.",
    ), (
        "plan_does_not_depend_on_input_order",
        len({tuple(v) for v in orders.values()}) == 1,
        f"ordres={orders} : le plan doit suivre la chronologie, pas l'ordre "
        f"dans lequel les evenements lui sont passes.",
    )]
    out.append(assert_log_contains(
        ctx.records, rf"rw_planner_chronology.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
