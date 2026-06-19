"""
il10a_dock_widget.py  -  Validation BL-IL-P0-10a (dock + map tool, phase 1)
============================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il10a_dock_widget.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il10a_dock_widget.py', 'exec'))

Phase 10a teste l'instanciation + cablage UI sans simulation de drag canvas.
"""
import sys
import uuid
from pathlib import Path

from qgis.core import QgsGeometry, QgsRectangle
from qgis.utils import iface, plugins

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il10a] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


# --- Test 1 : modules + symbols present + recover.py wiring ---
dock_path = _PLUGIN / 'widgets' / 'temporal_lens_dock.py'
tool_path = _PLUGIN / 'widgets' / 'temporal_lens_map_tool.py'
rec_src = (_PLUGIN / 'recover.py').read_text(encoding='utf-8')

dock_src = dock_path.read_text(encoding='utf-8') if dock_path.is_file() else ''
tool_src = tool_path.read_text(encoding='utf-8') if tool_path.is_file() else ''

ok_dock_class = 'class TemporalLensDock' in dock_src
ok_tool_class = 'class LensRectangleMapTool' in tool_src
ok_wiring_action = 'self.lens_action = QAction' in rec_src
ok_wiring_method = 'def open_lens_dock' in rec_src

results.append((
    'modules_and_wiring_present',
    ok_dock_class and ok_tool_class and ok_wiring_action and ok_wiring_method,
    f"dock_cls={ok_dock_class} tool_cls={ok_tool_class} "
    f"action={ok_wiring_action} method={ok_wiring_method}",
))


# --- Reload modules so re-runs pick up edits ---
import importlib
for mod_name in (
    'recoverland.widgets.temporal_lens_map_tool',
    'recoverland.widgets.temporal_lens_dock',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.widgets.temporal_lens_map_tool import LensRectangleMapTool
from recoverland.widgets.temporal_lens_dock import TemporalLensDock


# --- Test 2 : map tool instantiable + signal exists ---
canvas = iface.mapCanvas()
try:
    tool = LensRectangleMapTool(canvas)
    ok = (
        tool is not None
        and hasattr(tool, 'selection_completed')
        and hasattr(tool, 'reset')
        and hasattr(tool, 'canvasReleaseEvent')
    )
    msg = (
        f"selection_completed={hasattr(tool, 'selection_completed')} "
        f"reset={hasattr(tool, 'reset')} "
        f"canvasReleaseEvent={hasattr(tool, 'canvasReleaseEvent')}"
    )
except Exception as exc:
    tool = None
    ok, msg = False, f"raised: {exc!r}"
results.append(('map_tool_instantiable', ok, msg))


# --- Test 3 : selection_completed actually fires (simulated emission) ---
captured_geoms = []
if tool is not None:
    tool.selection_completed.connect(lambda g: captured_geoms.append(g))
    test_rect = QgsRectangle(0.0, 0.0, 10.0, 10.0)
    tool.selection_completed.emit(QgsGeometry.fromRect(test_rect))
    ok = (
        len(captured_geoms) == 1
        and captured_geoms[0] is not None
        and not captured_geoms[0].isEmpty()
    )
    msg = (
        f"captured={len(captured_geoms)} type="
        f"{type(captured_geoms[0]).__name__ if captured_geoms else None}"
    )
else:
    ok, msg = False, "tool=None"
results.append(('map_tool_signal_emits', ok, msg))


# --- Test 4 : dock instantiable with the plugin journal ---
plugin = plugins.get('recoverland')
journal = getattr(plugin, '_journal', None) if plugin is not None else None
try:
    dock = TemporalLensDock(iface, journal=journal)
    ok = (
        dock is not None
        and hasattr(dock, 'layer_combo')
        and hasattr(dock, 'select_button')
        and hasattr(dock, 'refresh_button')
        and hasattr(dock, 'disable_button')
        and hasattr(dock, 'status_label')
    )
    msg = (
        f"layer_combo={hasattr(dock, 'layer_combo')} "
        f"select={hasattr(dock, 'select_button')} "
        f"refresh={hasattr(dock, 'refresh_button')} "
        f"disable={hasattr(dock, 'disable_button')} "
        f"status={hasattr(dock, 'status_label')}"
    )
except Exception as exc:
    dock = None
    ok, msg = False, f"raised: {exc!r}"
results.append(('dock_instantiable_with_buttons', ok, msg))


# --- Test 5 : refresh button disabled initially, layer combo populated ---
if dock is not None:
    refresh_disabled = not dock.refresh_button.isEnabled()
    combo_n = dock.layer_combo.count()
    # If 0 audited layers in the test project, the combo contains the
    # disabled placeholder "(Aucune couche audited...)" so count >= 1.
    ok = refresh_disabled and combo_n >= 1
    msg = (
        f"refresh_disabled={refresh_disabled} combo_count={combo_n}"
    )
else:
    ok, msg = False, "dock=None"
results.append(('initial_state_clean', ok, msg))


# --- Test 6 : disable button purges overlays even before any refresh ---
# (Smoke: clicking disable should call purge_lens_overlays without crash
#  and close the dock; we just call the slot and check no exception.)
if dock is not None:
    try:
        dock._on_disable_button()
        ok = True
        msg = "ok"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
else:
    ok, msg = False, "dock=None"
results.append(('disable_button_smoke', ok, msg))


# --- Test 7 : log signature for populate_done emitted with audited count ---
import time
time.sleep(0.3)
try:
    log_path = Path(plugins['recoverland'].api_log_path())
    log_content = log_path.read_text(encoding='utf-8', errors='ignore')
    sig = 'lens_dock event=populate_done n_audited='
    ok = sig in log_content
    msg = f"signature='{sig}' found={ok}"
except Exception as exc:
    ok, msg = False, f"raised: {exc!r}"
results.append(('log_signature_populate', ok, msg))


# Cleanup dock if it's still alive.
if dock is not None:
    try:
        dock.deleteLater()
    except Exception:  # noqa: BLE001
        pass


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il10a] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il10a] === END trace_id={trace_id} ===")
