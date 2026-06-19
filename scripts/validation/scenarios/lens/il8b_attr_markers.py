"""
il8b_attr_markers.py  -  Validation BL-IL-P0-08b (3rd layer + truncation)
=========================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il8b_attr_markers.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il8b_attr_markers.py', 'exec'))

Pattern stress_edit : script lineaire, pas de runner, pas de guard.

Fixture: 3 entites en EPSG:2154
  fp_insert : INSERT (pas de old) - aucune feature 08b
  fp_update_geom : UPDATE old=p1 / new=p2 - past + arrow
  fp_update_attr : UPDATE old==new (bytes) + attrs_delta - past + attr_marker
fetch_stats.n_events_truncated = 5 - warning truncated:5 attendu
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
print(f"[il8b] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


def _le_point_wkb(x, y):
    return b'\x01' + struct.pack('<I', 1) + struct.pack('<d', x) + struct.pack('<d', y)


def _cleanup_overlays():
    project = QgsProject.instance()
    to_remove = [
        lyr.id() for lyr in project.mapLayers().values()
        if lyr.name().startswith('__rl_lens_')
    ]
    for lid in to_remove:
        project.removeMapLayer(lid)
    return len(to_remove)


# --- Test 1 : module + symbol present in source ---
renderer_path = _PLUGIN / 'core' / 'lens_renderer.py'
src = renderer_path.read_text(encoding='utf-8')
attr_marker_in_source = '__rl_lens_{uuid8}_attr_markers' in src
truncated_in_source = '"truncated:{plan.fetch_stats.n_events_truncated}"' in src or 'truncated:{plan.fetch_stats.n_events_truncated}' in src
helper_in_source = (
    'def execute_lens_render' in src
    and attr_marker_in_source
    and truncated_in_source
)
results.append((
    'helper_08b_in_source',
    helper_in_source,
    f"execute_lens_render={('def execute_lens_render' in src)} "
    f"attr_markers_layer={attr_marker_in_source} "
    f"truncated_warning={truncated_in_source}",
))

if not helper_in_source:
    print("[il8b] phase 08b not in source, aborting")
    for name, ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
else:
    import importlib
    for mod_name in (
        'recoverland.core.lens_renderer',
        'recoverland.core.geometry_utils',
        'recoverland.core.lens_contracts',
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
    print(f"[il8b] cleanup: {n_purged} legacy __rl_lens_* layer(s) removed")

    p1 = _le_point_wkb(750000.0, 6480000.0)
    p2 = _le_point_wkb(750100.0, 6480100.0)

    selection = LensSelection(
        layer_id_snapshot='dummy',
        datasource_fp='dummy',
        bbox_xy=(749000.0, 6479000.0, 751000.0, 6481000.0),
        bbox_crs='EPSG:2154',
        t_min='2026-05-01T00:00:00Z',
        t_max='2026-05-16T23:59:59Z',
        op_filter=LensOpFilter.ALL,
        max_events=1000,
    )

    state_insert = EntityState(
        event_id=2001, created_at='2026-05-10T10:00:00Z',
        user_name='alice', operation_type='INSERT',
        old_geom_wkb=None, new_geom_wkb=p1, attrs_delta={},
        crs_authid='EPSG:2154',
    )
    state_update_geom = EntityState(
        event_id=2002, created_at='2026-05-12T11:00:00Z',
        user_name='bob', operation_type='UPDATE',
        old_geom_wkb=p1, new_geom_wkb=p2,
        attrs_delta={'name': ('a', 'b')}, crs_authid='EPSG:2154',
    )
    state_update_attr = EntityState(
        event_id=2003, created_at='2026-05-13T12:00:00Z',
        user_name='carol', operation_type='UPDATE',
        old_geom_wkb=p1, new_geom_wkb=p1,  # bytes-equal
        attrs_delta={'status': ('open', 'closed')},
        crs_authid='EPSG:2154',
    )

    plan = LensRenderPlan(
        selection=selection,
        entities={
            'fp_insert': EntityTimeline(
                'fp_insert', EntityClassification.CREATED_IN_ZONE,
                [state_insert], 0,
            ),
            'fp_update_geom': EntityTimeline(
                'fp_update_geom', EntityClassification.UPDATED_IN_ZONE,
                [state_update_geom], 0,
            ),
            'fp_update_attr': EntityTimeline(
                'fp_update_attr', EntityClassification.ATTR_ONLY_IN_ZONE,
                [state_update_attr], 0,
            ),
        },
        fetch_stats=LensFetchStats(
            n_events_total=8,
            n_events_returned=3,
            n_events_truncated=5,
            elapsed_ms=0,
        ),
        plan_metadata={'layer_name': 'test'},
    )

    # --- Test 2 : 3 layer ids returned ---
    try:
        result = execute_lens_render(plan, 'EPSG:3857', trace_id=trace_id)
        ok = (
            result is not None
            and len(result.overlay_layer_ids) == 3
        )
        msg = f"overlay_layer_ids count={len(result.overlay_layer_ids) if result else None}"
    except Exception as exc:
        result = None
        ok, msg = False, f"raised: {exc!r}"
    results.append(('result_has_3_layer_ids', ok, msg))

    project = QgsProject.instance()

    # --- Test 3 : 3rd layer name matches the attr_markers prefix ---
    if result is not None and len(result.overlay_layer_ids) == 3:
        attr_lyr = project.mapLayer(result.overlay_layer_ids[2])
        ok = attr_lyr is not None and bool(re.match(
            r'^__rl_lens_[0-9a-f]{8}_attr_markers$', attr_lyr.name(),
        ))
        msg = f"name={attr_lyr.name() if attr_lyr else None}"
    else:
        ok, msg = False, "no result"
    results.append(('attr_markers_layer_name_match', ok, msg))

    # --- Test 4 : attr_markers contains exactly 1 feature (the attr-only UPDATE) ---
    if result is not None and len(result.overlay_layer_ids) == 3:
        attr_lyr = project.mapLayer(result.overlay_layer_ids[2])
        n_attr = attr_lyr.featureCount() if attr_lyr else -1
        ok = n_attr == 1
        msg = f"n_features_attr={n_attr} (expected 1: attr-only UPDATE)"
    else:
        ok, msg = False, "no result"
    results.append(('attr_markers_feature_count', ok, msg))

    # --- Test 5 : arrows has 1 feature, NOT 2 (attr-only UPDATE filtered) ---
    if result is not None and len(result.overlay_layer_ids) == 3:
        arrows_lyr = project.mapLayer(result.overlay_layer_ids[1])
        n_arr = arrows_lyr.featureCount() if arrows_lyr else -1
        ok = n_arr == 1
        msg = f"n_features_arrows={n_arr} (expected 1: only geom-changing UPDATE)"
    else:
        ok, msg = False, "no result"
    results.append(('arrows_filters_zero_length', ok, msg))

    # --- Test 6 : truncation warning propagated ---
    if result is not None:
        warns = list(result.warnings or [])
        has_trunc = any(w.startswith('truncated:') for w in warns)
        ok = has_trunc and 'truncated:5' in warns
        msg = f"warnings={warns}"
    else:
        ok, msg = False, "no result"
    results.append(('truncation_warning_in_result', ok, msg))

    # --- Test 7 : log signature carries layers=3 + n_features_attr ---
    time.sleep(0.3)
    try:
        from qgis.utils import plugins
        log_path = Path(plugins['recoverland'].api_log_path())
        log_content = log_path.read_text(encoding='utf-8', errors='ignore')
        sig = 'lens_renderer event=overlay_built'
        ok = (
            sig in log_content
            and trace_id in log_content
            and 'layers=3' in log_content
            and 'n_features_attr=1' in log_content
        )
        msg = f"signature found={ok} (looking for layers=3 + n_features_attr=1)"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('log_signature_layers_3', ok, msg))

    n_cleaned = _cleanup_overlays()
    print(f"[il8b] cleanup post-test: {n_cleaned} layer(s) removed")


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il8b] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il8b] === END trace_id={trace_id} ===")
