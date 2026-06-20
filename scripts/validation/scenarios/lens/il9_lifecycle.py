"""
il9_lifecycle.py  -  Validation BL-IL-P0-09 (lifecycle cleanup + facade)
========================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il9_lifecycle.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il9_lifecycle.py', 'exec'))

Pattern stress_edit : script lineaire, pas de runner, pas de guard.
"""
import struct
import sys
import time
import uuid
from pathlib import Path

from qgis.core import QgsProject, QgsVectorLayer

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il9] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


def _le_point_wkb(x, y):
    return b'\x01' + struct.pack('<I', 1) + struct.pack('<d', x) + struct.pack('<d', y)


def _add_fake_lens_layer(name):
    """Inject a fake __rl_lens_* memory layer in QgsProject."""
    lyr = QgsVectorLayer(
        "Point?crs=EPSG:3857&field=stub:string",
        name,
        "memory",
    )
    QgsProject.instance().addMapLayer(lyr, True)
    return lyr.id()


def _count_lens_layers():
    return sum(
        1 for lyr in QgsProject.instance().mapLayers().values()
        if lyr.name().startswith('__rl_lens_')
    )


# --- Test 1 : module symbols + recover.py wiring ---
wf_src = (_PLUGIN / 'core' / 'workflow_service.py').read_text(encoding='utf-8')
rec_src = (_PLUGIN / 'recover.py').read_text(encoding='utf-8')

helper_in_source = (
    'def purge_lens_overlays' in wf_src
    and 'def execute_grouped_lens_view' in wf_src
)
wired_initgui = 'purge_lens_overlays("startup")' in rec_src
wired_unload = 'purge_lens_overlays("shutdown")' in rec_src
results.append((
    'lifecycle_symbols_and_wiring',
    helper_in_source and wired_initgui and wired_unload,
    f"helpers={helper_in_source} initGui_wired={wired_initgui} "
    f"unload_wired={wired_unload}",
))

import importlib
for mod_name in (
    'recoverland.core.workflow_service',
    'recoverland.core.lens_renderer',
    'recoverland.core.lens_planner',
    'recoverland.core.lens_contracts',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

import recoverland.core.workflow_service as _ws
importlib.reload(_ws)
purge_lens_overlays = _ws.purge_lens_overlays
execute_grouped_lens_view = _ws.execute_grouped_lens_view

# Make sure we start from a clean state.
purge_lens_overlays("preflight")

# --- Test 2 : purge removes only __rl_lens_* layers ---
non_lens = QgsVectorLayer(
    "Point?crs=EPSG:3857&field=stub:string",
    "user_layer_keep_me",
    "memory",
)
QgsProject.instance().addMapLayer(non_lens, True)
non_lens_id = non_lens.id()

_add_fake_lens_layer('__rl_lens_fake1_geom_past')
_add_fake_lens_layer('__rl_lens_fake1_arrows')
n_before = _count_lens_layers()
n_purged = purge_lens_overlays("test")
n_after = _count_lens_layers()
non_lens_still_there = QgsProject.instance().mapLayer(non_lens_id) is not None
results.append((
    'purge_only_rl_lens_layers',
    n_before == 2 and n_purged == 2 and n_after == 0 and non_lens_still_there,
    f"n_before={n_before} n_purged={n_purged} n_after={n_after} "
    f"non_lens_kept={non_lens_still_there}",
))

# --- Build fixture for facade test ---
from recoverland.core.lens_contracts import (
    LensFetchStats,
    LensOpFilter,
    LensSelection,
)
from recoverland.core.audit_backend import AuditEvent

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

p1 = _le_point_wkb(750000.0, 6480000.0)
p2 = _le_point_wkb(750100.0, 6480100.0)

# The facade goes through plan_lens_view, which takes AuditEvent objects.
# We craft one minimal UPDATE event with an entity_fingerprint so the
# planner groups it into one EntityTimeline.
ev = AuditEvent(
    event_id=9001,
    project_fingerprint='proj',
    datasource_fingerprint='dummy',
    layer_id_snapshot='lyr',
    layer_name_snapshot='test_layer',
    provider_type='memory',
    feature_identity_json='{"fid": 1}',
    operation_type='UPDATE',
    attributes_json='{}',
    geometry_wkb=p1,
    geometry_type='Point',
    crs_authid='EPSG:2154',
    field_schema_json='{}',
    user_name='alice',
    session_id=None,
    created_at='2026-05-12T11:00:00Z',
    restored_from_event_id=None,
    entity_fingerprint='fp_facade',
    new_geometry_wkb=p2,
)

fetch_stats = LensFetchStats(
    n_events_total=1,
    n_events_returned=1,
    n_events_truncated=0,
    elapsed_ms=0,
)

# --- Test 3 : facade returns a LensRefreshOutcome with 3 overlay layer ids ---
# Phase 10c changed the return type from LensRenderResult to
# LensRefreshOutcome(plan, result). The dock now also uses outcome.plan
# for the clickable entity list.
try:
    outcome1 = execute_grouped_lens_view(
        [ev], selection, 'test_layer', fetch_stats, 'EPSG:3857',
        trace_id=trace_id,
    )
    result1 = outcome1.result if outcome1 is not None else None
    ok = (
        outcome1 is not None
        and result1 is not None
        and len(result1.overlay_layer_ids) == 3
        and outcome1.plan is not None
    )
    msg = (
        f"overlay_layer_ids count="
        f"{len(result1.overlay_layer_ids) if result1 else None} "
        f"plan_present={outcome1.plan is not None if outcome1 else False}"
    )
except Exception as exc:
    outcome1 = None
    result1 = None
    ok, msg = False, f"raised: {exc!r}"
results.append(('facade_returns_3_layers', ok, msg))

# --- Test 4 : second facade call purges first overlay (no accumulation) ---
n_lens_after_first = _count_lens_layers()
try:
    outcome2 = execute_grouped_lens_view(
        [ev], selection, 'test_layer', fetch_stats, 'EPSG:3857',
        trace_id=trace_id,
    )
    result2 = outcome2.result if outcome2 is not None else None
    n_lens_after_second = _count_lens_layers()
    # After the second call, only the SECOND overlay (3 layers) should be in
    # the project. The first overlay must have been purged before render.
    ok = (
        n_lens_after_first == 3
        and n_lens_after_second == 3
        and result2 is not None
        and result1 is not None
        and result2.overlay_layer_ids != result1.overlay_layer_ids
    )
    msg = (
        f"after_first={n_lens_after_first} after_second={n_lens_after_second} "
        f"ids_changed={(result2.overlay_layer_ids != result1.overlay_layer_ids) if (result1 and result2) else None}"
    )
except Exception as exc:
    ok, msg = False, f"raised: {exc!r}"
results.append(('facade_purges_previous_on_refresh', ok, msg))

# --- Test 5 : log signature 'startup_cleanup' OR equivalent context emitted ---
# (Test 2 already triggered a 'test_cleanup' event; we look it up in the log.)
time.sleep(0.3)
try:
    from qgis.utils import plugins
    log_path = Path(plugins['recoverland'].api_log_path())
    log_content = log_path.read_text(encoding='utf-8', errors='ignore')
    has_test_cleanup = 'lens_lifecycle event=test_cleanup' in log_content
    has_refresh = 'lens_lifecycle event=refresh' in log_content
    has_trace = trace_id in log_content
    ok = has_test_cleanup and has_refresh and has_trace
    msg = (
        f"test_cleanup={has_test_cleanup} refresh={has_refresh} "
        f"trace_id={has_trace}"
    )
except Exception as exc:
    ok, msg = False, f"raised: {exc!r}"
results.append(('log_signatures_present', ok, msg))

# --- Test 6 : after a final purge, 0 __rl_lens_* layers remain ---
purge_lens_overlays("final")
n_lens_final = _count_lens_layers()
non_lens_still_there = QgsProject.instance().mapLayer(non_lens_id) is not None
results.append((
    'final_purge_zero_orphan',
    n_lens_final == 0 and non_lens_still_there,
    f"n_lens_final={n_lens_final} non_lens_kept={non_lens_still_there}",
))

# --- Cleanup helper layer ---
QgsProject.instance().removeMapLayer(non_lens_id)


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il9] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il9] === END trace_id={trace_id} ===")
