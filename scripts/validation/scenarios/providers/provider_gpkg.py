"""provider_gpkg - RecoverLand validation runtime, BL-RW-P2-10.

Invariant: I-10 (OGR-GPKG layers complete the basic capture cycle:
connect, edit through Qt, commit, audit event landed in the journal).

Thèse (phase A) : un GPKG normal, connecté à un tracker actif, produit
au moins un event INSERT après un commitChanges Qt.

Antithèses brutales (phase B) :
  * A2-bis : un commitChanges Qt qui arrive AVANT `tracker.activate()`
    NE DOIT PRODUIRE AUCUN event (defense en profondeur sur le guard
    `if not self._active` dans tous les handlers).
  * A4-bis : 3 addFeature successifs dans une seule transaction Qt
    DOIVENT donner 3 events INSERT distincts avec entity_fingerprints
    différents (test que multi-feature dans 1 commit n'est pas
    accidentellement mergé en 1 seul event).

Le K-3 signature `validate_rewind: provider=ogr driver=GPKG
score=...` est emis a la fin (phase A).
"""
from __future__ import annotations

from pathlib import Path

SCENARIO_ID = "provider_gpkg"
INVARIANT = "I-10"

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]


def setup(ctx):
    from recoverland.core.logger import flog
    flog(
        f"provider_gpkg setup: trace_id={ctx.trace_id}",
        "INFO",
    )


def _phase_b_antithese(ctx) -> dict:
    """Antithese brutale : A2-bis (commit before activate) + A4-bis
    (multi-insert one transaction). Returns metrics dict for assertions.
    """
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.providers._common import (
        make_temp_dir, cleanup_temp_dir, make_gpkg_layer,
        open_temp_journal, close_temp_journal, wait_for_events,
    )

    out = {
        "a2_events_before_activate": -1,
        "a2_commit_before_activate_ok": False,
        "a4_events_after_multi_insert": -1,
        "a4_distinct_entity_fingerprints": -1,
        "a4_all_inserts": False,
        "error": None,
    }

    jm = None
    wq = None
    tmpdir_fix = None
    tmpdir_jrnl = None
    try:
        # ===== A2-bis : commit BEFORE activate() =================
        tmpdir_fix = make_temp_dir("rl_p10_gpkg_antiA2_fix_")
        layer_a2, _ = make_gpkg_layer(tmpdir_fix, layer_name="a2")
        jm, wq, tmpdir_jrnl = open_temp_journal("rl_p10_gpkg_antiA2_jrnl_")
        tracker = EditSessionTracker(wq, jm)
        # Voluntarily NOT activating the tracker.
        tracker.connect_layer(layer_a2)
        from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
        if layer_a2.startEditing():
            feat = QgsFeature(layer_a2.fields())
            feat.setAttribute(layer_a2.fields().indexFromName("name"), "a2_pre_activate")
            feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0, 1.0)))
            layer_a2.addFeature(feat)
            commit_ok = layer_a2.commitChanges()
        else:
            commit_ok = False
        out["a2_commit_before_activate_ok"] = bool(commit_ok)
        fp_a2 = compute_datasource_fingerprint(layer_a2)
        # poll explicitly to allow any spurious event time to land
        events_a2 = wait_for_events(jm, fp_a2, expected_min=1, timeout_s=1.0)
        out["a2_events_before_activate"] = len(events_a2)
        flog(
            f"p10_gpkg_antiA2: commit_ok={commit_ok} events={len(events_a2)} "
            f"fingerprint={fp_a2!r} trace_id={ctx.trace_id}",
            "INFO" if len(events_a2) == 0 else "ERROR",
        )
        close_temp_journal(jm, wq)
        jm = wq = None

        # ===== A4-bis : 3 INSERT in one transaction =============
        jm, wq, tmpdir_jrnl2 = open_temp_journal("rl_p10_gpkg_antiA4_jrnl_")
        tmpdir_fix2 = make_temp_dir("rl_p10_gpkg_antiA4_fix_")
        layer_a4, _ = make_gpkg_layer(tmpdir_fix2, layer_name="a4")
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer_a4)
        if layer_a4.startEditing():
            for i in range(3):
                feat = QgsFeature(layer_a4.fields())
                feat.setAttribute(layer_a4.fields().indexFromName("name"),
                                  f"a4_n{i}")
                feat.setGeometry(QgsGeometry.fromPointXY(
                    QgsPointXY(float(i), float(i))))
                layer_a4.addFeature(feat)
            commit_ok2 = layer_a4.commitChanges()
        else:
            commit_ok2 = False
        fp_a4 = compute_datasource_fingerprint(layer_a4)
        events_a4 = wait_for_events(jm, fp_a4, expected_min=3, timeout_s=5.0)
        out["a4_events_after_multi_insert"] = len(events_a4)
        out["a4_distinct_entity_fingerprints"] = len({
            getattr(e, "entity_fingerprint", None) for e in events_a4
        })
        out["a4_all_inserts"] = all(
            getattr(e, "operation_type", None) == "INSERT" for e in events_a4
        ) if events_a4 else False
        flog(
            f"p10_gpkg_antiA4: commit_ok={commit_ok2} "
            f"events={len(events_a4)} "
            f"distinct_entity_fps={out['a4_distinct_entity_fingerprints']} "
            f"all_inserts={out['a4_all_inserts']} trace_id={ctx.trace_id}",
            "INFO" if out["a4_all_inserts"] and len(events_a4) == 3 else "ERROR",
        )
        cleanup_temp_dir(tmpdir_fix2)
        cleanup_temp_dir(tmpdir_jrnl2)
    except Exception as exc:
        out["error"] = repr(exc)
        flog(
            f"p10_gpkg_antithese: exception={out['error']} trace_id={ctx.trace_id}",
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
        make_temp_dir, cleanup_temp_dir, make_gpkg_layer,
        run_capture_only_cycle, emit_validate_rewind,
    )

    flog(f"provider_gpkg run start: trace_id={ctx.trace_id}", "INFO")

    tmpdir = None
    layer = None
    try:
        tmpdir = make_temp_dir("rl_p10_gpkg_fixture_")
        layer, gpkg_path = make_gpkg_layer(tmpdir)
        ctx.data["gpkg_path"] = gpkg_path
        result = run_capture_only_cycle(
            layer,
            provider_label="ogr",
            driver_label="GPKG",
            ctx=ctx,
            expect_refusal=False,
        )
    except Exception as exc:
        result = {
            "provider": "ogr", "driver": "GPKG",
            "score": 0, "event_count": 0,
            "connect_refused": False, "error": repr(exc),
        }
        flog(
            f"provider_gpkg: exception={result['error']} score=0 "
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
        f"provider_gpkg run end: trace_id={ctx.trace_id} "
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
        "gpkg_layer_connected",
        result.get("connect_refused") is False,
        f"connect_refused={result.get('connect_refused')} expected=False "
        f"(GPKG identity_strength=STRONG, tracker must connect)",
    ))

    out.append((
        "gpkg_event_captured",
        result.get("event_count", 0) >= 1,
        f"event_count={result.get('event_count')} expected>=1 "
        f"(Qt commit on GPKG must produce at least one INSERT event)",
    ))

    out.append((
        "gpkg_score_is_100",
        result.get("score") == 100,
        f"score={result.get('score')} expected=100 "
        f"error={result.get('error')!r}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker\.connect_layer.*driver=GPKG.*action=accepted",
        name="gpkg_connect_accepted_log",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        r"validate_rewind:\s+layer=\S+\s+provider=ogr\s+driver=GPKG\s+"
        r"identity_strength=strong\s+score=100",
        name="gpkg_validate_rewind_signature",
        min_count=1,
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"provider_gpkg.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    anti = ctx.data.get("antithese") or {}

    out.append((
        "antitheseA2_zero_event_before_activate",
        anti.get("a2_events_before_activate") == 0,
        f"a2_events_before_activate={anti.get('a2_events_before_activate')} "
        f"expected=0 (commit BEFORE tracker.activate() must produce NO event). "
        f"a2_commit_ok={anti.get('a2_commit_before_activate_ok')} "
        f"error={anti.get('error')!r}",
    ))

    out.append((
        "antitheseA4_three_distinct_inserts",
        anti.get("a4_events_after_multi_insert") == 3
        and anti.get("a4_distinct_entity_fingerprints") == 3
        and anti.get("a4_all_inserts") is True,
        f"a4_events={anti.get('a4_events_after_multi_insert')} expected=3 ; "
        f"a4_distinct_entity_fps={anti.get('a4_distinct_entity_fingerprints')} "
        f"expected=3 ; a4_all_inserts={anti.get('a4_all_inserts')} expected=True "
        f"(multi-feature commit must NOT be merged)",
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
