# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""obs_undo_partial_truth - RecoverLand validation runtime.

Invariant: I-OBS (une ligne de log qui explique un cycle dit ce que le
produit fait, jamais l'inverse).

Le mecanisme
------------
`core/observability._detect_anomalies`, regle 6 (`undo_partial`), emet une
ligne CYCLE_ANOMALY apres un undo partiellement reussi. Son texte disait :

    "Traces for failed events are invalidated so a follow-up rewind can
     retry; traces for succeeded events stay active so dedup neutralizes
     them."

C'est l'inverse du produit v7. `UndoRunner.undo_outcome` partitionne
l'entree en undone / skipped / failed / unattempted, et le dialogue
(`_on_undo_done`, et l'auto-undo quand il existe) n'invalide QUE les traces
des evenements REELLEMENT annules (`_invalidate_undone_traces(outcome
["undone"], ...)`) : un echec, un introuvable, un evenement jamais tente
gardent leur compensation dans la couche, donc leur trace ACTIVE, pour que
le Rewind suivant ne rejoue pas la compensation par-dessus elle-meme
(rw_undo_trace_invalidation_truth le prouve sur le vrai UndoRunner et un
vrai journal).

Le scenario d'echec concret
---------------------------
Apres un undo partiel, le support lit CYCLE_ANOMALY : "les traces des
echecs sont invalidees, relancez un Rewind pour reessayer". Il relance :
le Rewind ne touche pas ces entites (leurs traces actives les neutralisent)
et il conclut a un defaut du Rewind, ou supprime les traces a la main et
fait rejouer une compensation deja presente (un DELETE compense deux fois
reinsere l'entite en double).

Ce que le scenario execute reellement
-------------------------------------
Le VRAI `CycleStats.finalize` sur les compteurs d'un undo partiel (cycles
"undo" et "auto_undo"), relu dans le vrai fichier de log ; et, comme garde
de source (le dialogue n'est pas pilotable ici), la portee de TOUS les
appels d'invalidation de recover_dialog.py (quelle que soit la methode qui
les porte : le chemin d'auto-undo est en cours de refonte ailleurs).

Verdict attendu avant correctif : FAIL (le texte affirme l'inverse).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

SCENARIO_ID = "obs_undo_partial_truth"
INVARIANT = "I-OBS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FALSE_CLAIMS = (
    "Traces for failed events are invalidated",
    "follow-up rewind can retry",
    "traces for succeeded events stay active",
)


def _body(src: str, name: str) -> str:
    start = src.find(f"    def {name}(")
    if start < 0:
        return ""
    nxt = src.find("\n    def ", start + 10)
    return src[start:nxt if nxt > 0 else len(src)]


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"obs_undo_partial_truth setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.observability import CycleStats

    # The counters an UndoRunner publishes for 1 undone, 1 skipped, 1
    # failed, 1 unattempted (restore_runner._finish / _finish_runner).
    for cycle in ("undo", "auto_undo"):
        stats = CycleStats(trace_id=ctx.trace_id)
        stats.merge(plan_actions=4, apply_ok=1, apply_skipped=1, apply_fail=1,
                    undone=1, undo_skipped=1, undo_unattempted=1)
        stats.finalize(cycle, elapsed_ms=7)
    src = (_PLUGIN_ROOT / "recover_dialog.py").read_text(encoding="utf-8",
                                                         errors="replace")
    # Every place the dialog invalidates traces, whatever its method: what
    # it passes is the scope the anomaly text must describe.
    ctx.data["invalidation_scopes"] = [
        m.group(1).strip() for m in re.finditer(
            r"(?<!def )_invalidate_undone_traces\(\s*([^,]+),", src)]
    ctx.data["raw_invalidation_calls"] = [
        m.group(0) for m in re.finditer(
            r"(?<!def )_invalidate_trace_events\(\s*[^)]*\)", src)]
    ctx.data["undo_done_body"] = _body(src, "_on_undo_done")
    flog(f"obs_undo_partial_truth run: trace_id={ctx.trace_id}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    lines = [r.raw for r in ctx.records
             if f"[{ctx.trace_id}] CYCLE_ANOMALY" in r.raw and "code=undo_partial" in r.raw]
    out.append((
        "undo_partial_anomaly_emitted_for_both_cycles",
        any(" undo code=" in ln for ln in lines) and any(" auto_undo code=" in ln for ln in lines),
        f"lignes CYCLE_ANOMALY undo_partial={len(lines)} : un undo partiel doit "
        f"rester signale",
    ))
    false = [c for c in _FALSE_CLAIMS for ln in lines if c.lower() in ln.lower()]
    out.append((
        "undo_partial_does_not_claim_failed_traces_are_invalidated",
        bool(lines) and not false,
        f"affirmations fausses trouvees={sorted(set(false))} dans "
        f"{[ln[-420:] for ln in lines[:1]]} : le produit garde ACTIVES les traces "
        f"des echecs ; dire l'inverse envoie le support vers un Rewind qui ne "
        f"les touchera pas, ou vers une suppression de traces qui fait rejouer "
        f"une compensation deja presente",
    ))
    truth = [ln for ln in lines
             if re.search(r"only the undone", ln, re.I)
             and re.search(r"failed.*skipped.*unattempted", ln, re.I)
             and re.search(r"stay(s)? active", ln, re.I)]
    out.append((
        "undo_partial_states_the_product_rule",
        len(truth) == len(lines) and bool(lines),
        f"lignes qui disent la regle (seuls les annules perdent leur trace ; "
        f"echecs, introuvables, non tentes la gardent active)={len(truth)}/{len(lines)}",
    ))
    scopes = ctx.data.get("invalidation_scopes") or []
    raw = ctx.data.get("raw_invalidation_calls") or []
    scope = 'outcome["undone"]'
    out.append((
        "the_rule_is_the_product_s_rule",
        bool(scopes) and all(sc == scope for sc in scopes)
        and f"_invalidate_undone_traces({scope}" in (ctx.data.get("undo_done_body") or "")
        and all(call.replace(" ", "") == "_invalidate_trace_events(ids)"
                for call in raw),
        f"portees passees a _invalidate_undone_traces dans recover_dialog.py={scopes} "
        f"(attendu : uniquement {scope}, dont _on_undo_done) ; appels directs de "
        f"l'UPDATE={raw} (attendu : seulement depuis _invalidate_undone_traces) : "
        f"si le dialogue change de portee, ce texte doit changer avec lui",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"obs_undo_partial_truth.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
