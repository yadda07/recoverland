"""
il10c_entity_list_diff.py  -  Validation BL-IL-P0-10c (entity list + diff)
============================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il10c_entity_list_diff.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il10c_entity_list_diff.py', 'exec'))

Phase 10c ajoute la liste cliquable des entites + le panneau diff attrs
au dock 10a/10b. Le scenario verifie :
    * LensRefreshOutcome contract (plan + result agreges)
    * widgets ajoutes (entity_list, diff_panel, diff_table)
    * populate_entity_list a partir d'un fake plan
    * on_entity_clicked peuple le diff + setExtent appele
    * clear_entity_panels remet a zero
"""
import importlib
import sys
import uuid
from pathlib import Path

from qgis.core import QgsGeometry, QgsPointXY
from qgis.utils import iface

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il10c] === START trace_id={trace_id} ===")

results = []


# --- Reload modules so re-runs pick up edits ---
for mod_name in (
    'recoverland.core.lens_contracts',
    'recoverland.core.workflow_service',
    'recoverland.widgets.temporal_lens_polygon_map_tool',
    'recoverland.widgets.temporal_lens_map_tool',
    'recoverland.widgets.temporal_lens_dock',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.core.lens_contracts import (
    EntityClassification, EntityState, EntityTimeline,
    LensFetchStats, LensOpFilter, LensRefreshOutcome,
    LensRenderPlan, LensSelection,
)
from recoverland.widgets.temporal_lens_dock import TemporalLensDock


# --- Test 1 : LensRefreshOutcome + dock helpers present ---
dock_path = _PLUGIN / 'widgets' / 'temporal_lens_dock.py'
ws_path = _PLUGIN / 'core' / 'workflow_service.py'
ct_path = _PLUGIN / 'core' / 'lens_contracts.py'

dock_src = dock_path.read_text(encoding='utf-8') if dock_path.is_file() else ''
ws_src = ws_path.read_text(encoding='utf-8') if ws_path.is_file() else ''
ct_src = ct_path.read_text(encoding='utf-8') if ct_path.is_file() else ''

ok_outcome_class = 'class LensRefreshOutcome' in ct_src
ok_facade_returns_outcome = 'LensRefreshOutcome(plan=plan, result=result)' in ws_src
ok_dock_entity_list = 'self.entity_list' in dock_src
ok_dock_diff_panel = 'self.diff_panel' in dock_src
ok_dock_diff_table = 'self.diff_table' in dock_src
ok_helper_populate = 'def _populate_entity_list' in dock_src
ok_helper_click = 'def _on_entity_clicked' in dock_src
ok_helper_center = 'def _center_canvas_on_entity' in dock_src
ok_helper_diff = 'def _populate_diff_panel' in dock_src
ok_helper_clear = 'def _clear_entity_panels' in dock_src

ok = (
    ok_outcome_class and ok_facade_returns_outcome
    and ok_dock_entity_list and ok_dock_diff_panel and ok_dock_diff_table
    and ok_helper_populate and ok_helper_click and ok_helper_center
    and ok_helper_diff and ok_helper_clear
)
msg = (
    f"outcome={ok_outcome_class} facade={ok_facade_returns_outcome} "
    f"entity_list={ok_dock_entity_list} diff_panel={ok_dock_diff_panel} "
    f"diff_table={ok_dock_diff_table} "
    f"populate={ok_helper_populate} click={ok_helper_click} "
    f"center={ok_helper_center} diff={ok_helper_diff} clear={ok_helper_clear}"
)
results.append(('module_and_contract_present', ok, msg))


# --- Build a dock instance for the runtime tests ---
plugin = sys.modules.get('recoverland')
plugins_dict = getattr(__import__('qgis.utils', fromlist=['plugins']), 'plugins', {})
plugin_instance = plugins_dict.get('recoverland')
journal = getattr(plugin_instance, '_journal', None) if plugin_instance else None
try:
    dock = TemporalLensDock(iface, journal=journal)
except Exception as exc:
    dock = None
    print(f"[il10c] dock build FAILED: {exc!r}")


# --- Test 2 : dock has all 10c widgets, diff panel hidden initially ---
if dock is not None:
    try:
        present = {
            'entity_list': hasattr(dock, 'entity_list'),
            'diff_panel': hasattr(dock, 'diff_panel'),
            'diff_table': hasattr(dock, 'diff_table'),
            'diff_label': hasattr(dock, 'diff_label'),
            '_last_plan': hasattr(dock, '_last_plan'),
        }
        hidden = (
            dock.diff_panel.isHidden()
            if present['diff_panel'] else False
        )
        empty_list = (
            dock.entity_list.count() == 0
            if present['entity_list'] else False
        )
        ok = all(present.values()) and hidden and empty_list
        msg = (
            " ".join(f"{k}={v}" for k, v in present.items())
            + f" diff_hidden={hidden} list_empty={empty_list}"
        )
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('dock_has_10c_widgets_initial_hidden', ok, msg))


# --- Build a fake plan with 2 entities for the population tests ---
def _make_state(op, old_attrs, new_attrs, wkb=None):
    attrs_delta = {}
    keys = set(old_attrs or {}) | set(new_attrs or {})
    for k in keys:
        attrs_delta[k] = (
            (old_attrs or {}).get(k), (new_attrs or {}).get(k),
        )
    # EntityState requires 8 fields including user_name (omitting it
    # raised TypeError on the first il10c run).
    return EntityState(
        event_id=1,
        created_at='2026-05-16T10:00:00+00:00',
        user_name='test_user',
        operation_type=op,
        old_geom_wkb=wkb,
        new_geom_wkb=wkb,
        attrs_delta=attrs_delta,
        crs_authid='EPSG:4326',
    )


point_wkb = bytes(QgsGeometry.fromPointXY(QgsPointXY(10.0, 20.0)).asWkb())

timeline_a = EntityTimeline(
    entity_fp='aaaaaaaa11111111',
    classification=EntityClassification.UPDATED_IN_ZONE,
    states=[
        _make_state('UPDATE', {'nom': 'Avant'}, {'nom': 'Apres'}, wkb=point_wkb),
        _make_state('UPDATE', {'pop': 100}, {'pop': 150}, wkb=point_wkb),
    ],
    n_events_filtered=0,
)
timeline_b = EntityTimeline(
    entity_fp='bbbbbbbb22222222',
    classification=EntityClassification.CREATED_IN_ZONE,
    states=[_make_state('INSERT', None, {'nom': 'Nouveau'}, wkb=point_wkb)],
    n_events_filtered=0,
)
fake_selection = LensSelection(
    layer_id_snapshot='test_layer',
    datasource_fp='deadbeef',
    bbox_xy=(0.0, 0.0, 50.0, 50.0),
    bbox_crs='EPSG:4326',
    t_min='2026-05-01T00:00:00+00:00',
    t_max='2026-05-31T00:00:00+00:00',
    op_filter=LensOpFilter.ALL,
)
fake_plan = LensRenderPlan(
    selection=fake_selection,
    entities={
        timeline_a.entity_fp: timeline_a,
        timeline_b.entity_fp: timeline_b,
    },
    fetch_stats=LensFetchStats(
        n_events_total=3, n_events_returned=3,
        n_events_truncated=0, elapsed_ms=0,
    ),
    plan_metadata={'layer_name': 'test_layer'},
)


# --- Test 3 : _populate_entity_list fills the QListWidget correctly ---
if dock is not None:
    try:
        dock._populate_entity_list(fake_plan)
        n_items = dock.entity_list.count()
        fps_in_list = []
        for i in range(n_items):
            it = dock.entity_list.item(i)
            fp = it.data(0x0100)  # Qt.UserRole = 0x0100; QtCompat.USER_ROLE
            fps_in_list.append(fp)
        # Compare as sets because dict iteration order is preserved in
        # CPython 3.7+ but the test must not depend on it.
        ok = (
            n_items == 2
            and set(fps_in_list) == {
                timeline_a.entity_fp, timeline_b.entity_fp,
            }
        )
        msg = f"n_items={n_items} fps={sorted(fps_in_list)}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('populate_entity_list_from_fake_plan', ok, msg))


# --- Test 4 : _on_entity_clicked populates diff and shows the panel ---
if dock is not None:
    try:
        dock._last_plan = fake_plan  # populate would have set this too
        # Click on the first item -> timeline_a (2 states, 2 attr changes)
        first_item = dock.entity_list.item(0)
        first_fp = first_item.data(0x0100)
        target_timeline = fake_plan.entities[first_fp]
        n_expected_rows = sum(
            len(st.attrs_delta) for st in target_timeline.states
            if st.attrs_delta
        )
        dock._on_entity_clicked(first_item)
        n_rows = dock.diff_table.rowCount()
        visible = not dock.diff_panel.isHidden()
        ok = n_rows == n_expected_rows and visible
        msg = (
            f"fp={first_fp[:8]} n_rows={n_rows} "
            f"expected={n_expected_rows} visible={visible}"
        )
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('on_entity_clicked_populates_diff', ok, msg))


# --- Test 5 : _center_canvas_on_entity calls setExtent without crashing ---
if dock is not None and iface is not None:
    try:
        canvas = iface.mapCanvas()
        extent_before = canvas.extent()
        dock._last_plan = fake_plan
        dock._center_canvas_on_entity(timeline_a.entity_fp, timeline_a)
        extent_after = canvas.extent()
        # We don't assert exact bbox math (depends on CRS settings) -
        # only that the call returned cleanly. A WARNING log will be
        # emitted by the dock on reproject failure but no exception.
        ok = True
        msg = (
            f"before=({extent_before.xMinimum():.1f},"
            f"{extent_before.yMinimum():.1f}) "
            f"after=({extent_after.xMinimum():.1f},"
            f"{extent_after.yMinimum():.1f})"
        )
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock or iface None"
results.append(('center_canvas_no_crash', ok, msg))


# --- Test 6 : _clear_entity_panels resets everything ---
if dock is not None:
    try:
        # State: populated + diff visible (from earlier tests).
        dock._clear_entity_panels()
        ok = (
            dock.entity_list.count() == 0
            and dock.diff_table.rowCount() == 0
            and dock.diff_panel.isHidden()
            and dock._last_plan is None
        )
        msg = (
            f"list={dock.entity_list.count()} "
            f"diff_rows={dock.diff_table.rowCount()} "
            f"hidden={dock.diff_panel.isHidden()} "
            f"last_plan={dock._last_plan}"
        )
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('clear_entity_panels_resets', ok, msg))


# --- Test 7 : LensRefreshOutcome contract is a NamedTuple(plan, result) ---
try:
    fields = LensRefreshOutcome._fields
    ok = fields == ('plan', 'result')
    msg = f"fields={fields}"
except Exception as exc:
    ok, msg = False, f"raised: {exc!r}"
results.append(('outcome_namedtuple_contract', ok, msg))


# Cleanup
if dock is not None:
    try:
        dock.deleteLater()
    except Exception:  # noqa: BLE001
        pass


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il10c] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il10c] === END trace_id={trace_id} ===")
