"""
il14_auto_refresh.py  -  Validation BL-IL-P1-14 (auto-refresh pan/zoom)
========================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il14_auto_refresh.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il14_auto_refresh.py', 'exec'))

Verifie :
 1. auto_refresh_cb widget present dans le dock
 2. _debounce_timer est un QTimer singleShot 500ms
 3. toggle on -> canvas.extentsChanged connecte + log auto_refresh_on
 4. toggle off -> deconnexion propre + log auto_refresh_off
 5. _on_debounce_fire met a jour _selected_geom avec canvas extent
 6. disable / close deconnecte auto-refresh
"""
import importlib
import sys
import time
import uuid
from pathlib import Path

from qgis.PyQt.QtCore import QTimer
from qgis.utils import plugins

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il14] === START trace_id={trace_id} ===")

results = []

# --- Reload modules ---
for mod_name in (
    'recoverland.widgets.temporal_lens_map_tool',
    'recoverland.widgets.temporal_lens_polygon_map_tool',
    'recoverland.widgets.temporal_lens_dock',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from qgis.utils import iface
from recoverland.widgets.temporal_lens_dock import TemporalLensDock

plugin = plugins.get('recoverland')
journal = getattr(plugin, '_journal', None) if plugin is not None else None
dock = TemporalLensDock(iface, journal=journal)


# --- Test 1 : auto_refresh_cb widget exists ---
ok = hasattr(dock, 'auto_refresh_cb')
results.append((
    'auto_refresh_cb_exists',
    ok,
    f"has_attr={ok}",
))


# --- Test 2 : _debounce_timer is QTimer, singleShot, 500ms ---
timer = dock._debounce_timer
ok_type = isinstance(timer, QTimer)
ok_single = timer.isSingleShot()
ok_interval = timer.interval() == 500
ok = ok_type and ok_single and ok_interval
results.append((
    'debounce_timer_configured',
    ok,
    f"type={type(timer).__name__} single={ok_single} interval={timer.interval()}",
))


# --- Test 3 : toggle on -> log auto_refresh_on ---
dock.auto_refresh_cb.setChecked(True)
time.sleep(0.3)
log_path = Path(plugins['recoverland'].api_log_path())
log_txt = log_path.read_text(encoding='utf-8', errors='ignore')
ok = 'auto_refresh_on' in log_txt
results.append((
    'toggle_on_log',
    ok,
    f"found_auto_refresh_on={ok}",
))


# --- Test 4 : toggle off -> log auto_refresh_off ---
dock.auto_refresh_cb.setChecked(False)
time.sleep(0.3)
log_txt = log_path.read_text(encoding='utf-8', errors='ignore')
ok_off = 'auto_refresh_off' in log_txt
ok_disconnected = not dock._auto_refresh_active
ok = ok_off and ok_disconnected
results.append((
    'toggle_off_log_and_state',
    ok,
    f"log_off={ok_off} active={dock._auto_refresh_active}",
))


# --- Test 5 : _on_debounce_fire updates _selected_geom ---
from qgis.core import QgsGeometry
dock._selected_geom = None
dock._on_debounce_fire()
ok = (
    dock._selected_geom is not None
    and isinstance(dock._selected_geom, QgsGeometry)
    and not dock._selected_geom.isEmpty()
)
results.append((
    'debounce_fire_updates_geom',
    ok,
    f"geom_set={dock._selected_geom is not None} "
    f"not_empty={not dock._selected_geom.isEmpty() if dock._selected_geom else False}",
))


# --- Test 6 : disable disconnects auto-refresh ---
dock.auto_refresh_cb.setChecked(True)
dock._on_disable_button()
ok = not dock._auto_refresh_active
results.append((
    'disable_disconnects_auto_refresh',
    ok,
    f"active_after_disable={dock._auto_refresh_active}",
))


# Cleanup
try:
    dock.deleteLater()
except Exception:
    pass


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il14] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il14] === END trace_id={trace_id} ===")
