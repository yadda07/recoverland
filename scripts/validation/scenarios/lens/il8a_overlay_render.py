"""
il8a_overlay_render.py  -  Validation BL-IL-P0-08a (renderer phase 1)
======================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il8a_overlay_render.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il8a_overlay_render.py', 'exec'))

Pattern stress_edit : script linéaire, pas de runner, pas de guard.
"""
import re
import struct
import sys
import time
import uuid
from pathlib import Path

from qgis.core import QgsProject

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il8a] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


# --- Test 1 : module + symbol present in source ---
renderer_path = _PLUGIN / 'core' / 'lens_renderer.py'
helper_in_source = (
    renderer_path.is_file()
    and 'def execute_lens_render' in renderer_path.read_text(encoding='utf-8')
)
results.append((
    'helper_defined_in_source',
    helper_in_source,
    f"def execute_lens_render in core/lens_renderer.py = {helper_in_source}",
))


def _le_point_wkb(x, y):
    return b'\x01' + struct.pack('<I', 1) + struct.pack('<d', x) + struct.pack('<d', y)


def _cleanup_overlays():
    """Remove any __rl_lens_* layer left by a previous run."""
    project = QgsProject.instance()
    to_remove = [
        lyr.id() for lyr in project.mapLayers().values()
        if lyr.name().startswith('__rl_lens_')
    ]
    for lid in to_remove:
        project.removeMapLayer(lid)
    return len(to_remove)


if not helper_in_source:
    print("[il8a] helper not in source, aborting")
    for name, ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
else:
    import importlib
    # Force reload so a freshly modified source is picked up.
    for mod_name in (
        'recoverland.core.lens_renderer',
        'recoverland.core.geometry_utils',
        'recoverland.core.lens_contracts',
        'recoverland.core.lens_planner',
    ):
        if mod_name in sys.modules:
            importlib.reload(sys.modules[mod_name])

    import recoverland.core.lens_renderer as _lr
    importlib.reload(_lr)
    execute_lens_render = _lr.execute_lens_render

    from recoverland.core.lens_contracts import (
        EntityClassification,
        EntityState,
        EntityTimeline,
        LensFetchStats,
        LensOpFilter,
        LensRenderPlan,
        LensSelection,
    )

    n_purged = _cleanup_overlays()
    print(f"[il8a] cleanup: {n_purged} legacy __rl_lens_* layer(s) removed")

    # --- Build a minimal LensRenderPlan fixture ---
    # 2 entities in EPSG:2154, central France:
    #   fp_insert: a single INSERT (no old geom, only new at P1)
    #   fp_update: a single UPDATE (old at P1, new at P2)
    p1 = _le_point_wkb(750000.0, 6480000.0)
    p2 = _le_point_wkb(750100.0, 6480100.0)

    selection = LensSelection(
        layer_id_snapshot='dummy_layer_id',
        datasource_fp='dummy_fp',
        bbox_xy=(749000.0, 6479000.0, 751000.0, 6481000.0),
        bbox_crs='EPSG:2154',
        t_min='2026-05-01T00:00:00Z',
        t_max='2026-05-16T23:59:59Z',
        op_filter=LensOpFilter.ALL,
        max_events=1000,
    )

    state_insert = EntityState(
        event_id=1001,
        created_at='2026-05-10T10:00:00Z',
        user_name='alice',
        operation_type='INSERT',
        old_geom_wkb=None,
        new_geom_wkb=p1,
        attrs_delta={},
        crs_authid='EPSG:2154',
    )
    state_update = EntityState(
        event_id=1002,
        created_at='2026-05-12T11:00:00Z',
        user_name='bob',
        operation_type='UPDATE',
        old_geom_wkb=p1,
        new_geom_wkb=p2,
        attrs_delta={'name': ('a', 'b')},
        crs_authid='EPSG:2154',
    )

    timeline_insert = EntityTimeline(
        entity_fp='fp_insert',
        classification=EntityClassification.CREATED_IN_ZONE,
        states=[state_insert],
        n_events_filtered=0,
    )
    timeline_update = EntityTimeline(
        entity_fp='fp_update',
        classification=EntityClassification.UPDATED_IN_ZONE,
        states=[state_update],
        n_events_filtered=0,
    )

    fetch_stats = LensFetchStats(
        n_events_total=2,
        n_events_returned=2,
        n_events_truncated=0,
        elapsed_ms=0,
    )

    plan = LensRenderPlan(
        selection=selection,
        entities={'fp_insert': timeline_insert, 'fp_update': timeline_update},
        fetch_stats=fetch_stats,
        plan_metadata={'layer_name': 'test_layer'},
    )

    # --- Test 2 : execute_lens_render returns 2 layer ids ---
    try:
        result = execute_lens_render(plan, 'EPSG:3857', trace_id=trace_id)
        ok = (
            result is not None
            and hasattr(result, 'overlay_layer_ids')
            and len(result.overlay_layer_ids) == 2
        )
        msg = f"overlay_layer_ids={result.overlay_layer_ids if result else None}"
    except Exception as exc:
        result = None
        ok, msg = False, f"raised: {exc!r}"
    results.append(('result_has_2_layer_ids', ok, msg))

    project = QgsProject.instance()

    # --- Test 3 : both layers actually exist in the project ---
    if result is not None and len(result.overlay_layer_ids) == 2:
        lyrs = [project.mapLayer(lid) for lid in result.overlay_layer_ids]
        ok = all(l is not None for l in lyrs)
        msg = f"layers={[l.name() if l else None for l in lyrs]}"
    else:
        ok, msg = False, "no result"
    results.append(('layers_exist_in_project', ok, msg))

    # --- Test 4 : layer names match the IL-I3 prefix regex ---
    if result is not None and len(result.overlay_layer_ids) == 2:
        names = [project.mapLayer(lid).name() for lid in result.overlay_layer_ids]
        rgx = re.compile(r'^__rl_lens_[0-9a-f]{8}_(geom_past|arrows)$')
        ok = all(rgx.match(n) for n in names)
        msg = f"names={names}"
    else:
        ok, msg = False, "no result"
    results.append(('layer_names_match_prefix', ok, msg))

    # --- Test 5 : past layer has 1 feature (the UPDATE old geom) ---
    if result is not None and len(result.overlay_layer_ids) == 2:
        past_lyr = project.mapLayer(result.overlay_layer_ids[0])
        n_past = past_lyr.featureCount() if past_lyr else -1
        ok = n_past == 1
        msg = f"n_features_past={n_past} (expected 1: UPDATE old_geom)"
    else:
        ok, msg = False, "no result"
    results.append(('past_layer_feature_count', ok, msg))

    # --- Test 6 : arrows layer has 1 feature (the UPDATE old->new) ---
    if result is not None and len(result.overlay_layer_ids) == 2:
        arrows_lyr = project.mapLayer(result.overlay_layer_ids[1])
        n_arr = arrows_lyr.featureCount() if arrows_lyr else -1
        ok = n_arr == 1
        msg = f"n_features_arrows={n_arr} (expected 1: UPDATE old->new)"
    else:
        ok, msg = False, "no result"
    results.append(('arrows_layer_feature_count', ok, msg))

    # --- Test 7 : log signature emitted ---
    time.sleep(0.3)
    try:
        from qgis.utils import plugins
        log_path = Path(plugins['recoverland'].api_log_path())
        log_content = log_path.read_text(encoding='utf-8', errors='ignore')
        signature = 'lens_renderer event=overlay_built'
        ok = signature in log_content and trace_id in log_content
        msg = f"signature='{signature}' trace_id={trace_id} found={ok}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('log_signature_emitted', ok, msg))

    # --- Cleanup the overlays we just added so the next run starts clean ---
    n_cleaned = _cleanup_overlays()
    print(f"[il8a] cleanup post-test: {n_cleaned} layer(s) removed")


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il8a] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il8a] === END trace_id={trace_id} ===")
