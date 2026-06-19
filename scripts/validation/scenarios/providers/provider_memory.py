"""provider_memory - RecoverLand validation runtime, BL-RW-P2-10.

Invariant: I-10 (memory provider has no stable identity and must be
refused by the EditSessionTracker).

Per `core/support_policy.py` + `core/identity.py`, a layer whose
provider is `memory` reports `IdentityStrength.NONE`. Capture would
produce events keyed on an FID that disappears the moment QGIS is
restarted, so the tracker MUST refuse to connect such layers
(`connect_layer -> action=refused reason=no_stable_identity`).

Thèse (phase A) : un layer memory est refusé et aucun event n'est
produit.

Antithèses brutales (phase B) :
  * M-anti-1 : refus persiste apres un cycle suppress/unsuppress. Le
    critère de refus est la policy (`IdentityStrength.NONE`), pas
    l'état suppress du tracker.
  * M-anti-2 : `connect_layer` est idempotent. Un second appel ne doit
    pas ajouter le layer aux connectés.

Le K-3 signature `validate_rewind: provider=memory driver=memory
score=100` est emis a la fin.
"""
from __future__ import annotations

from pathlib import Path

SCENARIO_ID = "provider_memory"
INVARIANT = "I-10"

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]


def setup(ctx):
    from recoverland.core.logger import flog
    flog(
        f"provider_memory setup: trace_id={ctx.trace_id}",
        "INFO",
    )


def _phase_b_antithese(ctx) -> dict:
    """Antithese brutale : refus memory tient sous cycle suppress/unsuppress
    et reste idempotent au double connect_layer.
    """
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.providers._common import (
        make_memory_layer, open_temp_journal, close_temp_journal,
    )

    out = {
        "m_anti1_refused_under_suppress": False,
        "m_anti1_refused_after_unsuppress": False,
        "m_anti2_double_connect_count": -1,
        "error": None,
    }

    jm = None
    wq = None
    try:
        jm, wq, _ = open_temp_journal("rl_p10_mem_anti_")
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()

        # M-anti-1 : refus persiste sous suppress
        layer1 = make_memory_layer("p10_mem_anti1")
        tracker.suppress()
        try:
            tracker.connect_layer(layer1)
            out["m_anti1_refused_under_suppress"] = (
                layer1.id() not in tracker._connected_layers
            )
        finally:
            tracker.unsuppress()
        # second connect after unsuppress: must still refuse
        tracker.connect_layer(layer1)
        out["m_anti1_refused_after_unsuppress"] = (
            layer1.id() not in tracker._connected_layers
        )

        # M-anti-2 : double connect idempotent
        layer2 = make_memory_layer("p10_mem_anti2")
        tracker.connect_layer(layer2)
        before = len(tracker._connected_layers)
        tracker.connect_layer(layer2)
        after = len(tracker._connected_layers)
        out["m_anti2_double_connect_count"] = after - before

        flog(
            f"p10_mem_antithese: refused_under_suppress="
            f"{out['m_anti1_refused_under_suppress']} "
            f"refused_after_unsuppress="
            f"{out['m_anti1_refused_after_unsuppress']} "
            f"double_connect_delta={out['m_anti2_double_connect_count']} "
            f"trace_id={ctx.trace_id}",
            "INFO" if (
                out["m_anti1_refused_under_suppress"]
                and out["m_anti1_refused_after_unsuppress"]
                and out["m_anti2_double_connect_count"] == 0
            ) else "ERROR",
        )
    except Exception as exc:
        out["error"] = repr(exc)
        flog(
            f"p10_mem_antithese: exception={out['error']} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        close_temp_journal(jm, wq)

    return out


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.providers._common import (
        make_memory_layer, run_capture_only_cycle, emit_validate_rewind,
    )

    flog(f"provider_memory run start: trace_id={ctx.trace_id}", "INFO")

    layer = None
    try:
        layer = make_memory_layer("p10_mem")
        result = run_capture_only_cycle(
            layer,
            provider_label="memory",
            driver_label="memory",
            ctx=ctx,
            expect_refusal=True,
        )
    except Exception as exc:
        result = {
            "provider": "memory", "driver": "memory",
            "score": 0, "event_count": 0,
            "connect_refused": False, "error": repr(exc),
        }
        flog(
            f"provider_memory: exception={result['error']} score=0 "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )

    ctx.data["result"] = result
    ctx.data["antithese"] = _phase_b_antithese(ctx)
    emit_validate_rewind(
        result["provider"], result["driver"], result["score"], layer=layer
    )
    flog(
        f"provider_memory run end: trace_id={ctx.trace_id} "
        f"score={result['score']} connect_refused={result['connect_refused']} "
        f"event_count={result['event_count']} error={result['error']!r} "
        f"antithese={ctx.data['antithese']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    result = ctx.data.get("result") or {}

    out.append((
        "memory_connect_refused",
        result.get("connect_refused") is True,
        f"connect_refused={result.get('connect_refused')} expected=True "
        f"(memory provider has IdentityStrength.NONE and MUST be refused)",
    ))

    out.append((
        "memory_score_is_100",
        result.get("score") == 100,
        f"score={result.get('score')} expected=100 "
        f"(refusal is the expected behaviour for memory)",
    ))

    out.append((
        "memory_no_events_captured",
        result.get("event_count", 0) == 0,
        f"event_count={result.get('event_count')} expected=0",
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker\.connect_layer.*memory.*action=refused.*"
        r"reason=no_stable_identity",
        name="memory_warning_logged_no_stable_identity",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"validate_rewind:\s+layer=\S+\s+provider=memory\s+driver=memory\s+"
        r"identity_strength=none\s+score=100",
        name="memory_validate_rewind_signature",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"provider_memory.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    anti = ctx.data.get("antithese") or {}

    out.append((
        "antithese_M1_refused_under_suppress",
        anti.get("m_anti1_refused_under_suppress") is True,
        f"m_anti1_refused_under_suppress="
        f"{anti.get('m_anti1_refused_under_suppress')} expected=True "
        f"(memory must be refused even under suppress; "
        f"refus is policy-based, not state-based)",
    ))

    out.append((
        "antithese_M1_refused_after_unsuppress",
        anti.get("m_anti1_refused_after_unsuppress") is True,
        f"m_anti1_refused_after_unsuppress="
        f"{anti.get('m_anti1_refused_after_unsuppress')} expected=True "
        f"(memory refusal persists after suppress/unsuppress cycle)",
    ))

    out.append((
        "antithese_M2_double_connect_idempotent",
        anti.get("m_anti2_double_connect_count") == 0,
        f"m_anti2_double_connect_count="
        f"{anti.get('m_anti2_double_connect_count')} expected=0 "
        f"(double connect_layer on memory must NOT change connected count)",
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
