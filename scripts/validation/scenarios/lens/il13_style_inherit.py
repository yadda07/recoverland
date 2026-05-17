"""
il13_style_inherit.py  -  Validation BL-IL-P1-13 (style heritage + opacite)
============================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il13_style_inherit.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il13_style_inherit.py', 'exec'))

Verifie :
 1. _apply_source_style existe dans lens_renderer
 2. execute_lens_render accepte source_layer kwarg
 3. execute_grouped_lens_view accepte source_layer kwarg
 4. dock passe source_layer=layer a execute_grouped_lens_view
 5. _GHOST_OPACITY defini a 0.4
 6. log signature style_cloned emise au runtime (si couche dispo)
"""
import importlib
import inspect
import sys
import uuid
from pathlib import Path

from qgis.utils import plugins

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il13] === START trace_id={trace_id} ===")

results = []


# --- Reload modules ---
for mod_name in (
    'recoverland.core.lens_renderer',
    'recoverland.core.workflow_service',
    'recoverland.widgets.temporal_lens_dock',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.core.lens_renderer import (
    _apply_source_style,
    _GHOST_OPACITY,
    execute_lens_render,
)
from recoverland.core.workflow_service import execute_grouped_lens_view


# --- Test 1 : _apply_source_style callable ---
ok = callable(_apply_source_style)
results.append((
    'apply_source_style_exists',
    ok,
    f"callable={ok}",
))


# --- Test 2 : execute_lens_render accepts source_layer ---
sig = inspect.signature(execute_lens_render)
ok = 'source_layer' in sig.parameters
results.append((
    'render_accepts_source_layer',
    ok,
    f"params={list(sig.parameters.keys())}",
))


# --- Test 3 : execute_grouped_lens_view accepts source_layer ---
sig2 = inspect.signature(execute_grouped_lens_view)
ok = 'source_layer' in sig2.parameters
results.append((
    'facade_accepts_source_layer',
    ok,
    f"params={list(sig2.parameters.keys())}",
))


# --- Test 4 : dock passes source_layer=layer ---
dock_src = (
    _PLUGIN / 'widgets' / 'temporal_lens_dock.py'
).read_text(encoding='utf-8')
ok = 'source_layer=layer' in dock_src
results.append((
    'dock_passes_source_layer',
    ok,
    f"found={'source_layer=layer' in dock_src}",
))


# --- Test 5 : _GHOST_OPACITY == 0.4 ---
ok = _GHOST_OPACITY == 0.4
results.append((
    'ghost_opacity_value',
    ok,
    f"value={_GHOST_OPACITY}",
))


# --- Test 6 : smoke _apply_source_style with a real QGIS layer ---
from qgis.core import QgsProject, QgsVectorLayer

layers = list(QgsProject.instance().mapLayers().values())
source = None
for lyr in layers:
    if hasattr(lyr, 'renderer') and not lyr.name().startswith('__rl_lens_'):
        source = lyr
        break

if source is not None:
    overlay = QgsVectorLayer(
        'Polygon?crs=EPSG:4326&field=id:integer',
        '__rl_test_style_clone',
        'memory',
    )
    try:
        _apply_source_style(overlay, source, trace_id=trace_id)
        opacity_ok = abs(overlay.opacity() - _GHOST_OPACITY) < 0.01
        import time
        time.sleep(0.3)
        log_path = Path(plugins['recoverland'].api_log_path())
        log_txt = log_path.read_text(encoding='utf-8', errors='ignore')
        log_ok = 'style_cloned' in log_txt and trace_id in log_txt
        ok = opacity_ok and log_ok
        msg = f"opacity={overlay.opacity():.2f} log_sig={log_ok}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    finally:
        del overlay
else:
    ok = True
    msg = "no_source_layer_in_project (skipped, vacuously true)"

results.append((
    'smoke_style_clone_runtime',
    ok,
    msg,
))


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il13] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il13] === END trace_id={trace_id} ===")
