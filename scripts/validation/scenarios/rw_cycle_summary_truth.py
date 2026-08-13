"""rw_cycle_summary_truth - RecoverLand validation runtime.

Invariant: I-LOG (the line a user re-reads after a rewind must not
contradict itself).
Root cause: the `CYCLE_SUMMARY` apply counters were collected from three
different populations, while the conservation invariant that was meant to
catch exactly that only knew a fourth one.

The hole this scenario proves
-----------------------------
Measured on a real project (2026-08-12), one single line:

    CYCLE_SUMMARY cycle=rewind raw=632 user=370 traces=262
      traces_active=262 dedup_active=73 dedup_dropped=283
      dedup_redundant=14 plan_actions=73
      apply_ok=14 apply_skipped=67 apply_fail=59
      traces_written=6 applied=6 applied_partial=0 failed=0
      failed_geometry_drift=5 failed_target_absent=59
      skipped_idempotent=3

Three contradictions, all in the line the operator reads first:

1. The plan carried 73 actions, the line counts 14+67+59 = 140 results.
   `apply_skipped` was `events - events_that_fully_succeeded` (73 - 6 =
   67): an EVENT-level number covering every failure and every skip a
   SECOND time, printed next to the ACTION-level `apply_ok`/`apply_fail`
   which already totalled the 73 actions on their own.
2. `applied=6` next to `apply_ok=14`. `apply_ok` counted everything the
   provider did not reject (6 applied + 3 idempotent skips + 5 geometry
   drift refusals), so it was never the number of entities that came
   back.
3. `failed=0` while 59 actions failed. `failed` published the residual
   "no more precise reason" bucket under the name a reader takes for the
   failure TOTAL: a rewind that left 59 entities behind announced zero
   failure to the person checking whether his data is back.

Why the guard stayed silent
---------------------------
`restore_runner._finish_runner` did hold a conservation invariant, but on
another base: it compared the six outcome buckets to
`total_ok + total_fail`, the runner's internal accounting. On the
measured run that base agreed (6+0+3+0+59+5 == 14+59 == 73), so nothing
was logged, while the line published counters that had never been part of
the check. An invariant that cannot see the numbers it protects is
silence with extra steps.

The fix under test
------------------
* `restore_runner._apply_counters` derives the published counters from
  the outcome buckets and from nothing else, so they partition the plan
  by construction:
      apply_ok      = applied + applied_partial
      apply_skipped = skipped_idempotent
      apply_fail    = failed + failed_target_absent + failed_geometry_drift
  `failed` publishes that failure total; the residual keeps the explicit
  name `failed_other`.
* `restore_runner._assert_bucket_conservation` checks the buckets against
  `plan_actions` — the base the reader sums against — as well as against
  the runner totals, and logs `INVARIANT_VIOLATED` (CRITICAL) plus
  `bucket_conservation=violated` on the summary itself when they drift.
* actions never attempted (the user cancelled) go to `cancelled`, never
  into a failure bucket.

Scenario layout (real StrictRestoreRunner / UndoRunner, real
`_finish_runner`, synthetic per-action report, no QGIS layer touched):
    setup: synthetic AuditEvents over two datasources
    run:
      A  the reported breakdown replayed as-is, with its dedup stats
      B  a mixed report: successes, partial writes, idempotent skips,
         target_absent and geometry_drift failures, over two layers
      C  antithesis: a report where one population is counted twice --
         the guard must FIRE instead of publishing a coherent-looking lie
      D  the undo path, which has no per-action classification at all
      E  a cancelled rewind: never-attempted actions are not failures
Pre-patch verdict: FAIL (A totals 140 and announces failed=0, C silent).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "rw_cycle_summary_truth"
INVARIANT = "I-LOG"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PROJ_FP = "rw_cst_project"
_DS_A = "rw_cst_datasource_a"
_DS_B = "rw_cst_datasource_b"
_T0 = "2026-08-12T09:00:00+00:00"

# The dedup stats the dialog stashes before starting the runner, taken
# from the real measurement so case A reproduces the reported line.
_REPORTED_DEDUP = {
    "raw": 632, "user": 370, "traces": 262, "traces_active": 262,
    "dedup_active": 73, "dedup_dropped": 283, "dedup_redundant": 14,
}


def _mk_event(eid: int, ds_fp: str, layer_name: str):
    """One audit event, minimal but real (the runner only groups them)."""
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=eid,
        project_fingerprint=_PROJ_FP,
        datasource_fingerprint=ds_fp,
        layer_id_snapshot=f"lyr_{layer_name}",
        layer_name_snapshot=layer_name,
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": eid}),
        operation_type="UPDATE",
        attributes_json=json.dumps(
            {"changed_only": {"v": {"old": eid, "new": eid + 1}}}),
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps([{"name": "v", "type": "integer"}]),
        user_name="tester",
        session_id=None,
        created_at=_T0,
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{eid}",
    )


def _mk_events(n_a: int, n_b: int) -> list:
    """`n_a` events on datasource A then `n_b` on datasource B."""
    events = [_mk_event(1000 + i, _DS_A, "couche_a") for i in range(n_a)]
    events += [_mk_event(2000 + i, _DS_B, "couche_b") for i in range(n_b)]
    return events


def _make_runner(events: list, trace_id: str, extra_stats=None):
    """A real StrictRestoreRunner, never started (no layer is touched)."""
    from recoverland.restore_runner import StrictRestoreRunner
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType

    cutoff = RestoreCutoff(CutoffType.BY_DATE, _T0, inclusive=True)
    return StrictRestoreRunner(
        events,
        lambda _event: None,
        cutoff,
        write_queue=None,
        tracker=None,
        trace_id=trace_id,
        extra_stats=extra_stats,
    )


def _inject_report(runner, report: dict, groups_reached=None) -> None:
    """Load a synthetic per-action report into the runner accumulators.

    This is the state a real run leaves behind when the last layer has
    committed: the six outcome buckets, the plan base, and the totals the
    apply phase counted. `groups_reached` below the number of groups is
    what a cancellation leaves.
    """
    runner._applied = report["applied"]
    runner._applied_partial = report.get("applied_partial", 0)
    runner._skipped_idempotent = report["skipped_idempotent"]
    runner._failed_other = report.get("failed", 0)
    runner._failed_target_absent = report["failed_target_absent"]
    runner._failed_geometry_drift = report["failed_geometry_drift"]
    runner._plan_actions = report["plan_actions"]
    runner._plan_skipped = report.get("plan_skipped", 0)
    runner._total_ok = report["total_ok"]
    runner._total_fail = report["total_fail"]
    runner._all_succeeded_ids = set(report.get("succeeded_ids", ()))
    runner._group_idx = (
        len(runner._groups) if groups_reached is None else groups_reached
    )


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["tid_a"] = f"{ctx.trace_id}a"
    ctx.data["tid_b"] = f"{ctx.trace_id}b"
    ctx.data["tid_c"] = f"{ctx.trace_id}c"
    ctx.data["tid_e"] = f"{ctx.trace_id}e"

    # --- A: the reported run, 73 actions on two layers ------------------
    ctx.data["events_a"] = _mk_events(50, 23)
    ctx.data["report_a"] = {
        "applied": 6, "applied_partial": 0, "skipped_idempotent": 3,
        "failed": 0, "failed_target_absent": 59, "failed_geometry_drift": 5,
        "plan_actions": 73, "plan_skipped": 0,
        "total_ok": 14, "total_fail": 59,
        "succeeded_ids": [1000 + i for i in range(6)],
    }
    # --- B: a mixed report, 3 events refused by the planner -------------
    ctx.data["events_b"] = _mk_events(25, 15)
    ctx.data["report_b"] = {
        "applied": 12, "applied_partial": 4, "skipped_idempotent": 6,
        "failed": 2, "failed_target_absent": 9, "failed_geometry_drift": 4,
        "plan_actions": 37, "plan_skipped": 3,
        "total_ok": 22, "total_fail": 15,
        "succeeded_ids": [1000 + i for i in range(16)],
    }
    # --- C: the same run with one population counted twice --------------
    ctx.data["events_c"] = _mk_events(25, 15)
    ctx.data["report_c"] = dict(ctx.data["report_b"])
    ctx.data["report_c"]["skipped_idempotent"] = 12
    # --- E: cancelled after the first layer -----------------------------
    ctx.data["events_e"] = _mk_events(25, 15)
    ctx.data["report_e"] = {
        "applied": 0, "applied_partial": 0, "skipped_idempotent": 0,
        "failed": 25, "failed_target_absent": 0, "failed_geometry_drift": 0,
        "plan_actions": 25, "plan_skipped": 0,
        "total_ok": 0, "total_fail": 25,
        "succeeded_ids": [],
    }

    flog(
        f"rw_cycle_summary_truth setup: trace_id={ctx.trace_id} "
        f"tids={ctx.data['tid_a']},{ctx.data['tid_b']},"
        f"{ctx.data['tid_c']},{ctx.data['tid_e']} "
        f"reported_plan_actions={ctx.data['report_a']['plan_actions']}",
        "INFO",
    )


def run(ctx):
    from recoverland.restore_runner import UndoRunner
    from recoverland.core.logger import flog

    flog(f"rw_cycle_summary_truth run start: trace_id={ctx.trace_id}", "INFO")

    # ===== A: replay the reported breakdown =============================
    runner_a = _make_runner(
        ctx.data["events_a"], ctx.data["tid_a"], extra_stats=_REPORTED_DEDUP)
    _inject_report(runner_a, ctx.data["report_a"])
    runner_a._finish()

    # ===== B: mixed report (successes, partials, skips, failures) =======
    runner_b = _make_runner(ctx.data["events_b"], ctx.data["tid_b"])
    _inject_report(runner_b, ctx.data["report_b"])
    runner_b._finish()

    # ===== C: antithesis, one population counted twice ==================
    runner_c = _make_runner(ctx.data["events_c"], ctx.data["tid_c"])
    _inject_report(runner_c, ctx.data["report_c"])
    runner_c._finish()

    # ===== D: the undo path, no per-action classification ===============
    by_ds = {_DS_A: _mk_events(8, 0)}
    runner_d = UndoRunner(by_ds, lambda _event: None, tracker=None)
    runner_d._total_ok = 3
    runner_d._total_fail = 5
    runner_d._finish()

    # ===== E: cancelled after the first of two layers ===================
    runner_e = _make_runner(ctx.data["events_e"], ctx.data["tid_e"])
    _inject_report(runner_e, ctx.data["report_e"], groups_reached=1)
    runner_e._cancelled = True
    runner_e._finish()

    flog(
        f"rw_cycle_summary_truth run end: trace_id={ctx.trace_id} "
        f"cases=A,B,C,D,E",
        "INFO",
    )


# --------------------------------------------------------------------------
# Reading the line back
# --------------------------------------------------------------------------


def _summary_fields(ctx, needle: str) -> dict:
    """key=value pairs of the first CYCLE_SUMMARY record matching *needle*."""
    for rec in ctx.records:
        if "CYCLE_SUMMARY" in rec.raw and needle in rec.raw:
            return dict(rec.fields)
    return {}


def _ival(fields: dict, key: str, default: int = 0) -> int:
    try:
        return int(fields[key])
    except (KeyError, TypeError, ValueError):
        return default


def _violations(ctx, trace_id: str) -> list:
    return [r for r in ctx.records
            if "INVARIANT_VIOLATED" in r.raw and f"trace={trace_id}" in r.raw]


def _conservation_assertions(fields: dict, label: str) -> list:
    """The two rules the mission locks, applied to one summary line."""
    plan_actions = _ival(fields, "plan_actions", -1)
    apply_ok = _ival(fields, "apply_ok", -1)
    apply_skipped = _ival(fields, "apply_skipped")
    apply_fail = _ival(fields, "apply_fail", -1)
    cancelled = _ival(fields, "cancelled")
    total = apply_ok + apply_skipped + apply_fail + cancelled
    failed = _ival(fields, "failed", -1)
    failed_sum = (_ival(fields, "failed_other")
                  + _ival(fields, "failed_target_absent")
                  + _ival(fields, "failed_geometry_drift"))
    applied_sum = _ival(fields, "applied") + _ival(fields, "applied_partial")

    return [
        (f"{label}__somme_des_seaux_egale_au_plan",
         plan_actions >= 0 and total == plan_actions,
         f"apply_ok={apply_ok} + apply_skipped={apply_skipped} + "
         f"apply_fail={apply_fail} + cancelled={cancelled} = {total}, "
         f"alors que le plan portait plan_actions={plan_actions} action(s). "
         f"Une ligne qui compte plus de resultats que d'actions ne permet "
         f"plus de savoir combien d'entites sont revenues."),
        (f"{label}__aucun_echec_annonce_a_zero",
         not (apply_fail > 0 and failed == 0),
         f"failed={failed} alors que apply_fail={apply_fail} action(s) ont "
         f"echoue. C'est la ligne que l'utilisateur relit apres un rewind "
         f"rate : elle lui annoncerait zero echec."),
        (f"{label}__failed_est_bien_le_total_des_echecs",
         failed == failed_sum == apply_fail,
         f"failed={failed} doit valoir failed_other + failed_target_absent "
         f"+ failed_geometry_drift = {failed_sum} et apply_fail={apply_fail}. "
         f"Sinon 'failed' publie le seau residuel sous le nom que le "
         f"lecteur prend pour le total."),
        (f"{label}__apply_ok_est_le_nombre_d_entites_revenues",
         apply_ok == applied_sum,
         f"apply_ok={apply_ok} doit valoir applied + applied_partial = "
         f"{applied_sum}. Sinon la ligne annonce plus d'entites restaurees "
         f"qu'il n'en est revenu."),
    ]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    fields_a = _summary_fields(ctx, f"[{ctx.data['tid_a']}] ")
    fields_b = _summary_fields(ctx, f"[{ctx.data['tid_b']}] ")
    fields_c = _summary_fields(ctx, f"[{ctx.data['tid_c']}] ")
    fields_d = _summary_fields(ctx, "cycle=undo")
    fields_e = _summary_fields(ctx, f"[{ctx.data['tid_e']}] ")

    # ===== The lines exist at all =======================================
    for label, fields in (("A", fields_a), ("B", fields_b),
                          ("C", fields_c), ("D", fields_d), ("E", fields_e)):
        out.append((
            f"cas_{label}__ligne_cycle_summary_emise",
            bool(fields),
            f"aucune ligne CYCLE_SUMMARY relue pour le cas {label} : sans "
            f"elle, l'utilisateur n'a aucun compte rendu de son rewind",
        ))
    if not (fields_a and fields_b and fields_c and fields_e):
        return out

    # ===== A: the reported run, replayed ================================
    out.extend(_conservation_assertions(fields_a, "cas_A_reporte"))
    out.append((
        "cas_A_reporte__plus_de_double_comptage",
        _ival(fields_a, "apply_ok") + _ival(fields_a, "apply_skipped")
        + _ival(fields_a, "apply_fail") == 73,
        f"le compte rendu mesure chez l'utilisateur additionnait "
        f"14+67+59=140 resultats pour 73 actions ; relu ici : "
        f"apply_ok={fields_a.get('apply_ok')} "
        f"apply_skipped={fields_a.get('apply_skipped')} "
        f"apply_fail={fields_a.get('apply_fail')}",
    ))
    out.append((
        "cas_A_reporte__apply_skipped_ne_recompte_pas_les_echecs",
        _ival(fields_a, "apply_skipped")
        == _ival(fields_a, "skipped_idempotent"),
        f"apply_skipped={fields_a.get('apply_skipped')} doit ne contenir que "
        f"les actions sautees car deja a l'etat cible "
        f"(skipped_idempotent={fields_a.get('skipped_idempotent')}). La "
        f"valeur 67 mesuree etait 'evenements - succes', qui recompte "
        f"chaque echec et chaque saut une deuxieme fois.",
    ))
    out.append((
        "cas_A_reporte__59_echecs_visibles",
        _ival(fields_a, "failed") == 64
        and _ival(fields_a, "failed_target_absent") == 59,
        f"failed={fields_a.get('failed')} "
        f"(attendu 64 = 59 cibles absentes + 5 derives de geometrie), "
        f"failed_target_absent={fields_a.get('failed_target_absent')}. "
        f"La ligne mesuree annoncait failed=0.",
    ))
    out.append((
        "cas_A_reporte__stats_dedup_conservees",
        _ival(fields_a, "dedup_active") == 73
        and _ival(fields_a, "raw") == 632,
        f"raw={fields_a.get('raw')} dedup_active={fields_a.get('dedup_active')} "
        f"attendus 632 et 73 : les compteurs amont ne doivent pas bouger, "
        f"c'est le seul moyen de relier le plan au journal.",
    ))

    # ===== B: mixed report ==============================================
    out.extend(_conservation_assertions(fields_b, "cas_B_mixte"))
    out.append((
        "cas_B_mixte__partiels_hors_des_restaurations_completes",
        _ival(fields_b, "applied") == 12
        and _ival(fields_b, "applied_partial") == 4,
        f"applied={fields_b.get('applied')} "
        f"applied_partial={fields_b.get('applied_partial')} attendus 12 et 4 : "
        f"une entite ecrite sans tous ses champs ne doit jamais etre comptee "
        f"avec les entites revenues intactes.",
    ))
    out.append((
        "cas_B_mixte__evenements_refuses_par_le_planificateur_visibles",
        _ival(fields_b, "plan_skipped") == 3
        and _ival(fields_b, "plan_actions")
        + _ival(fields_b, "plan_skipped") == 40,
        f"plan_actions={fields_b.get('plan_actions')} + "
        f"plan_skipped={fields_b.get('plan_skipped')} doit couvrir les 40 "
        f"evenements soumis. Les evenements refuses faute d'identite stable "
        f"ne reviendront pas : les taire, c'est promettre un etat cible "
        f"jamais atteint.",
    ))
    out.append((
        "cas_B_mixte__garde_silencieuse_quand_tout_est_coherent",
        not _violations(ctx, ctx.data["tid_b"]),
        f"{len(_violations(ctx, ctx.data['tid_b']))} INVARIANT_VIOLATED sur "
        f"un compte rendu coherent : une garde qui crie a tort finit ignoree "
        f"le jour ou elle a raison.",
    ))

    # ===== C: antithesis, the guard must fire ===========================
    violations_c = _violations(ctx, ctx.data["tid_c"])
    out.append((
        "cas_C_antithese__la_garde_se_declenche",
        len(violations_c) >= 1,
        f"{len(violations_c)} INVARIANT_VIOLATED alors qu'une population est "
        f"comptee deux fois (skipped_idempotent=12 pour 37 actions "
        f"planifiees). C'est exactement le defaut mesure chez l'utilisateur : "
        f"les seaux ne totalisaient pas le plan et rien ne le disait.",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"INVARIANT_VIOLATED name=apply_buckets_vs_plan_actions.*"
        rf"trace={ctx.data['tid_c']}",
        name="cas_C_antithese__base_plan_actions_verifiee",
        min_count=1,
    ))
    out.append((
        "cas_C_antithese__ligne_marquee_non_fiable",
        fields_c.get("bucket_conservation") == "violated",
        f"bucket_conservation={fields_c.get('bucket_conservation')} attendu "
        f"'violated' : la ligne que l'utilisateur relit doit dire elle-meme "
        f"que ses compteurs ne s'additionnent pas, pas seulement une ligne "
        f"CRITICAL plus haut dans le journal.",
    ))

    # ===== D: the undo path has no buckets ==============================
    out.append((
        "cas_D_undo__aucun_seau_a_zero_invente",
        "failed" not in fields_d and "applied" not in fields_d,
        f"la ligne d'undo publie failed={fields_d.get('failed')} "
        f"applied={fields_d.get('applied')} alors que ce chemin ne classe "
        f"rien par action : des seaux a zero a cote de "
        f"apply_fail={fields_d.get('apply_fail')} se lisent comme 'aucun "
        f"echec'.",
    ))
    out.append((
        "cas_D_undo__totaux_publies",
        _ival(fields_d, "apply_ok", -1) == 3
        and _ival(fields_d, "apply_fail", -1) == 5,
        f"apply_ok={fields_d.get('apply_ok')} "
        f"apply_fail={fields_d.get('apply_fail')} attendus 3 et 5 : l'undo "
        f"doit continuer a annoncer ses echecs.",
    ))

    # ===== E: a cancelled rewind ========================================
    out.extend(_conservation_assertions(fields_e, "cas_E_annule"))
    out.append((
        "cas_E_annule__les_actions_jamais_tentees_ne_sont_pas_des_echecs",
        _ival(fields_e, "cancelled") == 15
        and _ival(fields_e, "apply_fail") == 25,
        f"cancelled={fields_e.get('cancelled')} "
        f"apply_fail={fields_e.get('apply_fail')} attendus 15 et 25 : une "
        f"couche jamais traitee parce que l'utilisateur a annule n'a pas "
        f"echoue, et la compter comme telle envoie chercher une panne "
        f"inexistante.",
    ))
    out.append((
        "cas_E_annule__garde_silencieuse",
        not _violations(ctx, ctx.data["tid_e"]),
        f"{len(_violations(ctx, ctx.data['tid_e']))} INVARIANT_VIOLATED sur "
        f"une annulation correctement comptee : l'annulation est un cas "
        f"nominal, pas une derive de compteurs.",
    ))

    # ===== Trace propagation ============================================
    out.append(assert_log_contains(
        ctx.records,
        rf"rw_cycle_summary_truth.*trace_id={ctx.trace_id}",
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
