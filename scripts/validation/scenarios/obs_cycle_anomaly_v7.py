# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""obs_cycle_anomaly_v7 - RecoverLand validation runtime.

Invariant: I-OBS (une ligne de log qui explique un cycle dit ce que le
produit fait, jamais ce qu'il faisait avant).

Le mecanisme
------------
`core/observability._detect_anomalies`, regle 1 (`all_skipped`), emet une
ligne CYCLE_ANOMALY quand toutes les actions d'un Rewind sont sautees. Son
texte disait :

    "Likely cause: previous rewind not undone (auto-undo skipped or
     failed), schema drift, or external modification of the layer between
     fetch and apply. Check STATE logs for _last_restore_by_ds and
     _is_recovering around this cycle."

C'est la mecanique d'avant v7. Il n'y a plus d'auto-undo avant un Rewind
(rw_rewind_no_autoundo) ; `apply_skipped` ne compte plus que les sauts
idempotents (`restore_runner._apply_counters` : apply_skipped =
skipped_idempotent, l'entite avait deja l'etat que l'action lui donnerait),
et une derive ou une entite changee entre la lecture et l'ecriture sont
des ECHECS (failed_geometry_drift, failed_target_absent), pas des sauts.
La docstring de `log_cycle_summary` listait encore un cycle "auto_undo"
que restore_runner ne produit plus (`_RUNNER_TO_CYCLE` : event_restore,
rewind, undo).

Le scenario d'echec concret
---------------------------
Un Rewind ne fait rien (tout saute). Le support lit CYCLE_ANOMALY, cherche
un auto-undo rate et les transitions de `_last_restore_by_ds` : il n'y en a
pas, il conclut a un defaut du Rewind, alors que la vraie cause (l'etat
atteint sans trace active : restore dont les traces manquent, edition hors
RecoverLand, editions qui s'annulent) n'est jamais nommee.

Ce que le scenario execute reellement
-------------------------------------
Le VRAI `CycleStats.finalize` sur les compteurs d'un Rewind dont toutes les
actions sont des sauts idempotents, relu dans le vrai fichier de log, et la
docstring de `log_cycle_summary`.

Verdict attendu avant correctif : FAIL (texte et docstring d'avant v7).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SCENARIO_ID = "obs_cycle_anomaly_v7"
INVARIANT = "I-OBS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_STALE = ("auto-undo", "auto_undo", "_last_restore_by_ds")


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.observability import CycleStats, log_cycle_summary

    # A Rewind whose three actions all found their feature already at the
    # target state (restore_runner._apply_counters: apply_skipped is
    # skipped_idempotent).
    stats = CycleStats(trace_id=ctx.trace_id)
    stats.merge(user=3, dedup_active=3, plan_actions=3, apply_ok=0,
                apply_skipped=3, apply_fail=0, skipped_idempotent=3,
                traces_written=0)
    stats.finalize("rewind", elapsed_ms=5)
    ctx.data["doc"] = log_cycle_summary.__doc__ or ""
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    lines = [r.raw for r in ctx.records
             if f"[{ctx.trace_id}] CYCLE_ANOMALY rewind code=all_skipped" in r.raw]
    stale = sorted({s for s in _STALE for ln in lines if s in ln})
    out.append((
        "all_skipped_emitted",
        len(lines) == 1,
        f"lignes CYCLE_ANOMALY all_skipped={len(lines)} : un Rewind qui ne fait "
        f"rien doit rester signale",
    ))
    out.append((
        "all_skipped_does_not_blame_a_mechanism_v7_removed",
        bool(lines) and not stale,
        f"references perimees={stale} dans {[ln[-500:] for ln in lines]} : "
        f"le support cherche un auto-undo qui n'existe plus",
    ))
    out.append((
        "all_skipped_names_the_v7_meaning",
        bool(lines) and all(
            "skipped_idempotent" in ln
            and re.search(r"already held the\s+state", ln)
            and "trace" in ln for ln in lines),
        f"lignes={[ln[-500:] for ln in lines]} : doit dire que chaque entite "
        f"avait deja l'etat vise (skipped_idempotent) et orienter vers les "
        f"traces",
    ))
    doc = ctx.data.get("doc") or ""
    out.append((
        "log_cycle_summary_lists_only_v7_cycles",
        "auto_undo" not in doc and all(c in doc for c in (
            '"rewind"', '"undo"', '"event_restore"')),
        f"docstring de log_cycle_summary={doc[:300]!r}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID} (setup|run): trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
