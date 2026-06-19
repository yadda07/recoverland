"""provider_shp - RecoverLand validation runtime, BL-RW-P2-10.

Invariant: I-10 (OGR ESRI Shapefile layers complete the basic capture
cycle: connect, edit through Qt, commit, audit event landed in the
journal).

Note: per `core/support_policy.py:refine_ogr_identity`, a `.shp`
source has `IdentityStrength.MEDIUM`, so the tracker emits an INFO
signature `action=accepted_untested` (NOT `action=warned`, which is
reserved for `IdentityStrength.WEAK` formats such as `.csv` / `.xlsx`
/ `.kml`). The capture cycle is expected to produce events; the score
is binary 0/100.

Thèse (phase A) : un Shapefile normal, connecté à un tracker actif,
produit au moins un event INSERT après commitChanges Qt.

Antithèse brutale (phase B) :
  * A6-bis : `tracker.activate()` -> `connect_layer(shp)` -> 
    `tracker.deactivate()` -> commit Qt. Le tracker DOIT cesser de
    capturer même si le layer reste techniquement connecté. Verifie
    le guard `if not self._active` sur tous les handlers de signaux.

Le K-3 signature `validate_rewind: provider=ogr driver=ESRI
Shapefile score=...` est emis a la fin (phase A).
"""
from __future__ import annotations

from pathlib import Path

SCENARIO_ID = "provider_shp"
INVARIANT = "I-10"

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]


def setup(ctx):
    from recoverland.core.logger import flog
    flog(
        f"provider_shp setup: trace_id={ctx.trace_id}",
        "INFO",
    )


def _phase_b_antithese(ctx) -> dict:
    """Antithese brutale A6-bis : commit après deactivate doit produire
    0 event même si le layer reste connecté côté tracker.
    """
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.providers._common import (
        make_temp_dir, cleanup_temp_dir, make_shp_layer,
        open_temp_journal, close_temp_journal, wait_for_events,
        add_point_feature,
    )

    out = {
        "a6_events_after_deactivate": -1,
        "a6_commit_after_deactivate_ok": False,
        "a6_layer_still_connected": False,
        "error": None,
    }

    jm = None
    wq = None
    tmpdir_fix = None
    tmpdir_jrnl = None
    try:
        tmpdir_fix = make_temp_dir("rl_p10_shp_antiA6_fix_")
        layer, _ = make_shp_layer(tmpdir_fix, layer_name="a6")
        jm, wq, tmpdir_jrnl = open_temp_journal("rl_p10_shp_antiA6_jrnl_")
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        out["a6_layer_still_connected"] = layer.id() in tracker._connected_layers
        tracker.deactivate()
        commit_ok = add_point_feature(layer, "a6_after_deactivate")
        out["a6_commit_after_deactivate_ok"] = bool(commit_ok)
        fp = compute_datasource_fingerprint(layer)
        events = wait_for_events(jm, fp, expected_min=1, timeout_s=1.0)
        out["a6_events_after_deactivate"] = len(events)
        flog(
            f"p10_shp_antiA6: commit_ok={commit_ok} "
            f"layer_connected={out['a6_layer_still_connected']} "
            f"events_after_deactivate={len(events)} "
            f"trace_id={ctx.trace_id}",
            "INFO" if len(events) == 0 else "ERROR",
        )
    except Exception as exc:
        out["error"] = repr(exc)
        flog(
            f"p10_shp_antithese: exception={out['error']} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        close_temp_journal(jm, wq)
        cleanup_temp_dir(tmpdir_fix)
        cleanup_temp_dir(tmpdir_jrnl)

    return out


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.providers._common import (
        make_temp_dir, cleanup_temp_dir, make_shp_layer,
        run_capture_only_cycle, emit_validate_rewind,
    )

    flog(f"provider_shp run start: trace_id={ctx.trace_id}", "INFO")

    tmpdir = None
    layer = None
    try:
        tmpdir = make_temp_dir("rl_p10_shp_fixture_")
        layer, shp_path = make_shp_layer(tmpdir)
        ctx.data["shp_path"] = shp_path
        result = run_capture_only_cycle(
            layer,
            provider_label="ogr",
            driver_label="ESRI Shapefile",
            ctx=ctx,
            expect_refusal=False,
        )
    except Exception as exc:
        result = {
            "provider": "ogr", "driver": "ESRI Shapefile",
            "score": 0, "event_count": 0,
            "connect_refused": False, "error": repr(exc),
        }
        flog(
            f"provider_shp: exception={result['error']} score=0 "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )

    ctx.data["result"] = result
    ctx.data["antithese"] = _phase_b_antithese(ctx)
    emit_validate_rewind(
        result["provider"], result["driver"], result["score"], layer=layer
    )
    # Cleanup tmpdir AFTER emit_validate_rewind to keep the layer valid
    # for identity introspection.
    cleanup_temp_dir(tmpdir)
    flog(
        f"provider_shp run end: trace_id={ctx.trace_id} "
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
        "shp_layer_connected",
        result.get("connect_refused") is False,
        f"connect_refused={result.get('connect_refused')} expected=False "
        f"(Shapefile MEDIUM identity connects with action=accepted_untested)",
    ))

    out.append((
        "shp_event_captured",
        result.get("event_count", 0) >= 1,
        f"event_count={result.get('event_count')} expected>=1 "
        f"(Qt commit on Shapefile must produce at least one INSERT event)",
    ))

    out.append((
        "shp_score_is_100",
        result.get("score") == 100,
        f"score={result.get('score')} expected=100 "
        f"error={result.get('error')!r}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker\.connect_layer.*driver=ESRI Shapefile.*"
        r"action=accepted_untested",
        name="shp_connect_accepted_untested_log",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"validate_rewind:\s+layer=\S+\s+provider=ogr\s+driver=ESRI Shapefile"
        r"\s+identity_strength=medium\s+score=100",
        name="shp_validate_rewind_signature",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"provider_shp.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    anti = ctx.data.get("antithese") or {}

    out.append((
        "antitheseA6_zero_event_after_deactivate",
        anti.get("a6_events_after_deactivate") == 0,
        f"a6_events_after_deactivate={anti.get('a6_events_after_deactivate')} "
        f"expected=0 (commit AFTER deactivate must produce NO event). "
        f"layer_still_connected={anti.get('a6_layer_still_connected')} "
        f"commit_ok={anti.get('a6_commit_after_deactivate_ok')} "
        f"error={anti.get('error')!r}",
    ))

    out.append((
        "antitheseA6_layer_was_connected_before_deactivate",
        anti.get("a6_layer_still_connected") is True,
        f"a6_layer_still_connected={anti.get('a6_layer_still_connected')} "
        f"expected=True (layer must be connected for A6-bis to be a "
        f"meaningful antithesis; if False, the guard is not exercised)",
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
