# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_report_failed_other_shown - le compte rendu nomme toutes les causes d'echec.

Invariant : I-REPORT (le message de fin additionne ce que le plan contenait,
cause par cause, comme CYCLE_SUMMARY).

Le mecanisme
------------
Le message de fin d'une restauration ou d'un rewind est construit par
`recover_dialog._restore_buckets` a partir du rapport du runner
(`restore_runner.build_apply_report`). Le rapport publie le residu des
echecs ("pour une autre raison" : ecriture refusee, entite introuvable
sous une autre forme, plantage) sous la cle du seau, `failed` ; le
dialogue lisait `failed_other`, une cle que le rapport ne portait pas :
toujours 0.

Ce que l'utilisateur voyait
---------------------------
"4 action(s) planifiee(s) : 1 restauree(s), 3 echouee(s) (1 sans cible)."
Trois echecs annonces, une seule cause nommee : les deux ecritures
refusees n'apparaissaient nulle part, et le total des causes ne faisait
pas le total des echecs.

Ce qui est prouve
-----------------
A  De bout en bout : vrai RestoreRunner (ecritures scriptees : 1 appliquee,
   1 sans cible, 2 refusees), vrai slot de fin du dialogue, vrai message :
   "2 pour une autre raison" y figure, et les causes additionnees font le
   total des echecs, qui fait avec le reste le total du plan.
B  Le rapport d'un rewind (`build_apply_report`, seaux du StrictRestore-
   Runner) donne au dialogue le meme residu que CYCLE_SUMMARY.

Verdict avant correctif : FAIL (A, B). Apres : PASS.
"""
from __future__ import annotations

import re
import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_report_failed_other_shown"
INVARIANT = "I-REPORT"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_report_failed_other"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _numbers(msg):
    """{label: count} of the causes the message names."""
    out = {}
    for count, label in re.findall(
            r"(\d+) (sans cible|pour une autre raison|refusee\(s\) pour derive"
            r"|ecrite\(s\) mais contredite\(s\))", msg or ""):
        out[label] = int(count)
    total = re.search(r"(\d+) echouee\(s\)", msg or "")
    planned = re.search(r"(\d+) action\(s\) planifiee\(s\)", msg or "")
    return out, (int(total.group(1)) if total else None), (
        int(planned.group(1)) if planned else None)


def _case_a(ctx, tmp):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h = _h()
    journal = h.new_journal(tmp, "a.sqlite")
    ids = [h.insert_event(journal, _FP, "routes", "UPDATE", 10 + i, fid=i + 1)
           for i in range(4)]
    events = h.fetch_events(journal, ids)
    outcomes = {ids[0]: "applied", ids[1]: "target_absent",
                ids[2]: "refused", ids[3]: "refused"}
    wq = h.RecordingWriteQueue(journal)
    dlg = h.build_dialog(rd, h.Journal(journal), wq, [_FP])
    layer = h.FakeLayer("lyr_routes", "routes")
    logs, calls, results = [], [], []
    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch", h.scripted_restore_batch(outcomes, calls))
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        runner = rr.RestoreRunner(list(events), lambda _e: layer, wq,
                                  tracker=None, trace_id="rw_rfo_a")
        runner.finished.connect(results.append)
        runner.finished.connect(dlg._on_event_restore_done)
        dlg._event_restore_events = list(events)
        dlg._restore_in_progress = True
        dlg._restore_started_at = 0.0
        dlg._launch_runner(runner)
        h.pump_until(lambda: bool(results), 5.0)
        pending = getattr(dlg, "_pending_restore_feedback", None)
        if pending is not None:
            dlg._display_deferred_restore_feedback()
    finally:
        seams.restore()
        h.close_dialog(dlg)
    ctx.data["a_logs"] = [m for m, _l in logs]
    ctx.data["a_report"] = dict(runner.apply_report())


def _case_b(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.workflow_service import GroupedRestoreResult
    breakdown = {"applied": 3, "skipped_idempotent": 1, "failed": 2,
                 "failed_target_absent": 1, "failed_geometry_drift": 1,
                 "failed_postcommit_verification": 1}
    report = rr.build_apply_report(breakdown, 9)
    result = GroupedRestoreResult(
        total_ok=4, total_fail=5, errors=[], by_ds={}, trace_events=[],
        applied=3, skipped_idempotent=1, failed=2, failed_target_absent=1,
        failed_geometry_drift=1, failed_postcommit_verification=1)
    buckets = rd._restore_buckets(result, report=report)
    ctx.data["b_report"] = report
    ctx.data["b_buckets"] = buckets


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_rfo_")
    flog(f"rw_report_failed_other_shown setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    ctx.data["errors"] = {}
    for name, case in (("a", lambda: _case_a(ctx, ctx.data["tmpdir"])),
                       ("b", lambda: _case_b(ctx))):
        try:
            case()
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data["errors"][name] = (f"{type(exc).__name__}: {exc} "
                                        f"{traceback.format_exc()[-900:]}")
    flog(f"rw_report_failed_other_shown run: trace_id={ctx.trace_id} "
         f"a={ctx.data.get('a_logs')} b={ctx.data.get('b_buckets')} "
         f"errors={ctx.data['errors']}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", not d.get("errors"), f"crash={d.get('errors')}")]
    msg = next((m for m in d.get("a_logs") or []
                if "action(s) planifiee(s)" in m), "")
    causes, total, planned = _numbers(msg)
    out.append((
        "a_other_reason_named_in_the_message",
        causes.get("pour une autre raison") == 2,
        f"message={msg!r} : 2 ecritures refusees, elles doivent apparaitre "
        f"comme '2 pour une autre raison'. Sans elles, 3 echecs sont "
        f"annonces et un seul explique.",
    ))
    out.append((
        "a_causes_add_up_to_the_failures_and_the_plan",
        total == 3 and sum(causes.values()) == total and planned == 4,
        f"causes={causes} total des echecs={total} plan={planned} rapport="
        f"{d.get('a_report')} : les causes nommees doivent faire le total des "
        f"echecs, et 1 restauree + 3 echouees le plan de 4.",
    ))
    b = d.get("b_buckets") or {}
    report = d.get("b_report") or {}
    out.append((
        "b_rewind_residual_reaches_the_dialog",
        b.get("failed_other") == 2 and b.get("failed_total") == 5
        and b.get("failed_absent") + b.get("failed_drift")
        + b.get("failed_postcommit") + b.get("failed_other")
        == b.get("failed_total")
        and b.get("applied") + b.get("applied_partial") + b.get("skipped")
        + b.get("failed_total") + b.get("cancelled") == b.get("planned") == 9,
        f"seaux du dialogue={b} rapport={report} : le residu publie par le "
        f"runner (2) doit etre celui que le dialogue affiche, et les seaux "
        f"doivent faire le plan (9).",
    ))
    return out
