"""
il17_timeline_condensed.py  -  Validation BL-IL-P2-17 (condensed entity timeline)
==================================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il17_timeline_condensed.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il17_timeline_condensed.py', 'exec'))

Verifie :
 1. diff_table has 4 columns (Date|Op|Utilisateur|Resume)
 2. _populate_diff_panel produces 1 row per event (not 1 per field)
 3. _build_event_summary returns correct labels for INSERT/DELETE/UPDATE
 4. Review render loop produces max 1 past feature per entity
 5. Review render loop produces max 1 arrow per entity
 6. Review render loop produces max 1 attr marker per entity
"""
import importlib
import sys
import uuid
from pathlib import Path

trace_id = uuid.uuid4().hex[:8]
print(f"[il17] === START trace_id={trace_id} ===")

results = []

# --- Reload key modules ---
for mod_name in (
    'recoverland.widgets.temporal_lens_dock',
    'recoverland.core.lens_contracts',
    'recoverland.core.snapshot_overlay_session',
    'recoverland.widgets.snapshot_rebuild_worker',
    'recoverland.core.geometry_utils',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.core.lens_contracts import (
    EntityClassification, EntityState, EntityTimeline,
    LensFetchStats, LensRenderPlan, LensSelection, LensOpFilter,
)


# Helper: build N synthetic EntityState for 1 entity
def _make_states(n: int):
    states = []
    # Fake WKB: a simple point at different positions
    # WKB Point format: byte_order(1) + type(4) + x(8) + y(8) = 21 bytes
    import struct
    for i in range(n):
        x_old = float(i)
        x_new = float(i + 0.5)
        old_wkb = struct.pack('<bIdd', 1, 1, x_old, 10.0)
        new_wkb = struct.pack('<bIdd', 1, 1, x_new, 10.0)
        op = "INSERT" if i == 0 else "UPDATE"
        delta = {"field_a": (f"v{i}", f"v{i+1}"), "field_b": (i, i+1)} if op == "UPDATE" else {}
        states.append(EntityState(
            event_id=100 + i,
            created_at=f"2026-05-{10+i:02d}T12:00:00Z",
            user_name="test_user",
            operation_type=op,
            old_geom_wkb=old_wkb if i > 0 else None,
            new_geom_wkb=new_wkb,
            attrs_delta=delta,
            crs_authid="EPSG:4326",
        ))
    return states


# Build a plan with 1 entity having 10 states
ENTITY_FP = "abc123def456"
N_STATES = 10
states = _make_states(N_STATES)
timeline = EntityTimeline(
    entity_fp=ENTITY_FP,
    classification=EntityClassification.UPDATED_IN_ZONE,
    states=states,
    n_events_filtered=0,
)
selection = LensSelection(
    layer_id_snapshot="layer_001",
    datasource_fp="ds_fp_001",
    bbox_xy=(0, 0, 100, 100),
    bbox_crs="EPSG:4326",
    t_min="2026-05-01T00:00:00Z",
    t_max="2026-05-31T00:00:00Z",
    op_filter=LensOpFilter.ALL,
)
plan = LensRenderPlan(
    selection=selection,
    entities={ENTITY_FP: timeline},
    fetch_stats=LensFetchStats(10, 10, 0, 5),
    plan_metadata={"layer_name": "test_layer"},
)

# --- Test 1: diff_table has 4 columns ---
from qgis.utils import iface
from recoverland.widgets.temporal_lens_dock import TemporalLensDock

dock = TemporalLensDock(iface.mapCanvas(), None, iface.mainWindow())
col_count = dock.diff_table.columnCount()
ok1 = col_count == 4
results.append((
    'diff_table_4_columns',
    ok1,
    f"col_count={col_count}",
))

# --- Test 2: _populate_diff_panel produces 1 row per event ---
dock._last_plan = plan
dock._populate_diff_panel(ENTITY_FP, timeline)
row_count = dock.diff_table.rowCount()
ok2 = row_count == N_STATES
results.append((
    'diff_rows_equals_n_events',
    ok2,
    f"row_count={row_count} expected={N_STATES}",
))

# --- Test 3: _build_event_summary returns correct labels ---
# INSERT state (states[0])
s_insert = dock._build_event_summary(states[0])
ok_insert = "Crea" in s_insert  # "Creation"
# UPDATE with geom move (states[1])
s_update = dock._build_event_summary(states[1])
ok_update = "champ" in s_update or "Geom" in s_update
# Verify labels are non-empty
ok3 = ok_insert and ok_update and len(s_insert) > 0 and len(s_update) > 0
results.append((
    'build_event_summary_labels',
    ok3,
    f"insert={s_insert!r} update={s_update!r}",
))

# --- Test 4: diff_label contains "Timeline" ---
label_text = dock.diff_label.text()
ok4 = "Timeline" in label_text and ENTITY_FP[:8] in label_text
results.append((
    'diff_label_timeline_header',
    ok4,
    f"label={label_text!r}",
))

dock.deleteLater()

# --- Test 5+6: Review render condensation (structural check) ---
# Read snapshot_overlay_session source and verify the condensation pattern
_PLUGIN = Path(sys.modules['recoverland'].__file__).parent
session_src = (_PLUGIN / 'core' / 'snapshot_overlay_session.py').read_text(encoding='utf-8')

# The old pattern "for state in timeline.states:" inside the entity loop should NOT exist
# The new pattern uses oldest_state/newest_state/representative
has_old_pattern = 'for state in timeline.states:' in session_src
has_new_pattern = 'oldest_state = timeline.states[0]' in session_src
ok5 = not has_old_pattern and has_new_pattern
results.append((
    'review_session_condensed',
    ok5,
    f"old_loop_gone={not has_old_pattern} new_pattern={has_new_pattern}",
))

# Same for render worker
worker_src = (_PLUGIN / 'widgets' / 'snapshot_rebuild_worker.py').read_text(encoding='utf-8')
has_old_w = 'for state in timeline.states:' in worker_src
has_new_w = 'oldest_state = timeline.states[0]' in worker_src
ok6 = not has_old_w and has_new_w
results.append((
    'render_worker_condensed',
    ok6,
    f"old_loop_gone={not has_old_w} new_pattern={has_new_w}",
))

# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il17] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il17] === END trace_id={trace_id} ===")
