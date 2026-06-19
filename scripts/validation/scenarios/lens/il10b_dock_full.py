"""
il10b_dock_full.py  -  Validation BL-IL-P0-10b (dock complet, phase 2)
============================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il10b_dock_full.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il10b_dock_full.py', 'exec'))

Phase 10b ajoute polygon map tool + datepicker + filtre op + legende
dynamique au dock existant. Validation structurelle + fonctionnelle de
chaque element en restant en console (pas de simulation de drag canvas).
"""
import sys
import uuid
from collections import namedtuple
from pathlib import Path

from qgis.core import QgsGeometry, QgsRectangle
from qgis.utils import iface, plugins

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il10b] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


# --- Test 1 : modules + symbols present ---
poly_path = _PLUGIN / 'widgets' / 'temporal_lens_polygon_map_tool.py'
dock_path = _PLUGIN / 'widgets' / 'temporal_lens_dock.py'

poly_src = poly_path.read_text(encoding='utf-8') if poly_path.is_file() else ''
dock_src = dock_path.read_text(encoding='utf-8') if dock_path.is_file() else ''

ok_poly_class = 'class LensPolygonMapTool' in poly_src
ok_dock_polygon_btn = 'self.select_polygon_button' in dock_src
ok_dock_op_combo = 'self.op_combo' in dock_src
ok_dock_presets = '_PRESETS' in dock_src and 'preset_combo' in dock_src
ok_dock_legend = '_legend_swatches' in dock_src and 'legend_age_label' in dock_src
ok_dock_filter_helper = 'def _filter_events_by_op_filter' in dock_src
ok_dock_empty_msg = 'Aucune modification trouvee' in dock_src

results.append((
    'modules_and_wiring_present',
    (ok_poly_class and ok_dock_polygon_btn and ok_dock_op_combo
     and ok_dock_presets and ok_dock_legend and ok_dock_filter_helper
     and ok_dock_empty_msg),
    f"poly_cls={ok_poly_class} polygon_btn={ok_dock_polygon_btn} "
    f"op_combo={ok_dock_op_combo} presets={ok_dock_presets} "
    f"legend={ok_dock_legend} filter={ok_dock_filter_helper} "
    f"empty_msg={ok_dock_empty_msg}",
))


# --- Reload modules so re-runs pick up edits ---
import importlib
for mod_name in (
    'recoverland.widgets.temporal_lens_polygon_map_tool',
    'recoverland.widgets.temporal_lens_map_tool',
    'recoverland.widgets.temporal_lens_dock',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.core.lens_contracts import LensOpFilter
from recoverland.widgets.temporal_lens_polygon_map_tool import LensPolygonMapTool
from recoverland.widgets.temporal_lens_dock import TemporalLensDock


# --- Test 2 : polygon map tool instantiable + signal exists ---
canvas = iface.mapCanvas()
try:
    ptool = LensPolygonMapTool(canvas)
    ok = (
        ptool is not None
        and hasattr(ptool, 'selection_completed')
        and hasattr(ptool, 'reset')
        and hasattr(ptool, 'canvasDoubleClickEvent')
        and hasattr(ptool, 'canvasPressEvent')
    )
    msg = (
        f"selection_completed={hasattr(ptool, 'selection_completed')} "
        f"reset={hasattr(ptool, 'reset')} "
        f"dblclick={hasattr(ptool, 'canvasDoubleClickEvent')} "
        f"press={hasattr(ptool, 'canvasPressEvent')}"
    )
except Exception as exc:
    ptool = None
    ok, msg = False, f"raised: {exc!r}"
results.append(('polygon_map_tool_instantiable', ok, msg))


# --- Test 3 : polygon signal_completed actually fires (synthetic emit) ---
captured = []
if ptool is not None:
    ptool.selection_completed.connect(lambda g: captured.append(g))
    test_rect = QgsRectangle(0.0, 0.0, 10.0, 10.0)
    ptool.selection_completed.emit(QgsGeometry.fromRect(test_rect))
    ok = (
        len(captured) == 1
        and captured[0] is not None
        and not captured[0].isEmpty()
    )
    msg = (
        f"captured={len(captured)} "
        f"type={type(captured[0]).__name__ if captured else None}"
    )
else:
    ok, msg = False, "ptool=None"
results.append(('polygon_map_tool_signal_emits', ok, msg))


# --- Test 4 : dock instantiable with all phase 10b widgets ---
plugin = plugins.get('recoverland')
journal = getattr(plugin, '_journal', None) if plugin is not None else None
try:
    dock = TemporalLensDock(iface, journal=journal)
    widgets_present = {
        'layer_combo': hasattr(dock, 'layer_combo'),
        'select_button': hasattr(dock, 'select_button'),
        'select_polygon_button': hasattr(dock, 'select_polygon_button'),
        'preset_combo': hasattr(dock, 'preset_combo'),
        't_min_input': hasattr(dock, 't_min_input'),
        't_max_input': hasattr(dock, 't_max_input'),
        'op_combo': hasattr(dock, 'op_combo'),
        'refresh_button': hasattr(dock, 'refresh_button'),
        'status_label': hasattr(dock, 'status_label'),
        'legend_widget': hasattr(dock, 'legend_widget'),
        'legend_age_label': hasattr(dock, 'legend_age_label'),
        'disable_button': hasattr(dock, 'disable_button'),
    }
    ok = all(widgets_present.values())
    msg = " ".join(f"{k}={v}" for k, v in widgets_present.items())
except Exception as exc:
    dock = None
    ok, msg = False, f"raised: {exc!r}"
results.append(('dock_has_10b_widgets', ok, msg))


# --- Test 5 : default preset = "30 derniers jours" applied ---
if dock is not None:
    try:
        preset_idx = dock.preset_combo.currentIndex()
        preset_label = dock.preset_combo.currentText()
        t_min_qdt = dock.t_min_input.dateTime()
        t_max_qdt = dock.t_max_input.dateTime()
        # Calculate span in days (msecs between t_min and t_max)
        span_ms = t_min_qdt.msecsTo(t_max_qdt)
        span_days = span_ms / (1000.0 * 86400)
        # 30 derniers jours: t_min = now - 30d, t_max = now -> span ~ 30 days
        ok = (
            preset_idx == 2
            and 'jours' in preset_label.lower()
            and 29.5 <= span_days <= 30.5
        )
        msg = (
            f"preset_idx={preset_idx} label='{preset_label}' "
            f"span_days={span_days:.2f}"
        )
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('preset_default_30_days', ok, msg))


# --- Test 6 : op_combo has all 6 LensOpFilter values ---
if dock is not None:
    try:
        n_items = dock.op_combo.count()
        values_seen = set()
        for i in range(n_items):
            values_seen.add(dock.op_combo.itemData(i))
        expected = {f.value for f in LensOpFilter}
        ok = n_items == 6 and values_seen == expected
        msg = f"n_items={n_items} values={sorted(values_seen)}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('op_combo_has_6_items', ok, msg))


# --- Test 7 : legend has 5 colored swatches ---
if dock is not None:
    try:
        swatches = getattr(dock, '_legend_swatches', [])
        ok = len(swatches) == 5
        msg = f"n_swatches={len(swatches)}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('legend_has_5_swatches', ok, msg))


# --- Test 8 : _filter_events_by_op_filter pure behaviour ---
# Build a minimal fake event NamedTuple compatible with the predicate.
FakeEvent = namedtuple(
    'FakeEvent',
    ['operation_type', 'geometry_wkb', 'new_geometry_wkb'],
)
if dock is not None:
    try:
        wkb_a = b'\x00WKB_A\x00'
        wkb_b = b'\x00WKB_B\x00'
        evs = [
            FakeEvent('INSERT', None, None),
            FakeEvent('UPDATE', wkb_a, wkb_a),   # attr-only (geom unchanged)
            FakeEvent('UPDATE', wkb_a, wkb_b),   # geom changed
            FakeEvent('DELETE', wkb_a, None),
        ]
        all_kept = dock._filter_events_by_op_filter(evs, LensOpFilter.ALL)
        only_insert = dock._filter_events_by_op_filter(
            evs, LensOpFilter.INSERT_ONLY)
        only_update = dock._filter_events_by_op_filter(
            evs, LensOpFilter.UPDATE_ONLY)
        only_delete = dock._filter_events_by_op_filter(
            evs, LensOpFilter.DELETE_ONLY)
        only_attr = dock._filter_events_by_op_filter(
            evs, LensOpFilter.ATTR_ONLY)
        only_geom = dock._filter_events_by_op_filter(
            evs, LensOpFilter.GEOM_ONLY)
        ok = (
            len(all_kept) == 4
            and [e.operation_type for e in only_insert] == ['INSERT']
            and len(only_update) == 2
            and [e.operation_type for e in only_delete] == ['DELETE']
            and [e.geometry_wkb for e in only_attr] == [wkb_a]
            and [e.new_geometry_wkb for e in only_attr] == [wkb_a]
            and [e.geometry_wkb for e in only_geom] == [wkb_a]
            and [e.new_geometry_wkb for e in only_geom] == [wkb_b]
        )
        msg = (
            f"all={len(all_kept)} insert={len(only_insert)} "
            f"update={len(only_update)} delete={len(only_delete)} "
            f"attr={len(only_attr)} geom={len(only_geom)}"
        )
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('filter_events_by_op_filter_pure', ok, msg))


# Cleanup dock if alive
if dock is not None:
    try:
        dock.deleteLater()
    except Exception:  # noqa: BLE001
        pass


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il10b] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il10b] === END trace_id={trace_id} ===")
