"""
il7_repair_geom_qgis.py  -  Validation BL-IL-P0-07 (geometry repair)
====================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il7_repair_geom_qgis.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il7_repair_geom_qgis.py', 'exec'))

Pattern stress_edit : script linéaire, pas de runner, pas de guard.
"""
import struct
import sys
import time
import uuid
from pathlib import Path

from qgis.core import QgsGeometry  # noqa: F401  (verifies QGIS context)

# --- Resolve plugin root via sys.modules (no __file__ needed) ---
_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il7] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


# --- Test 1 : helper present in core/geometry_utils.py ---
geom_utils_src = (_PLUGIN / 'core' / 'geometry_utils.py').read_text(encoding='utf-8')
helper_in_source = 'def repair_geometry_for_render' in geom_utils_src
results.append((
    'helper_defined_in_source',
    helper_in_source,
    f"def repair_geometry_for_render in core/geometry_utils.py = {helper_in_source}",
))

if not helper_in_source:
    print("[il7] helper not in source, aborting")
    for name, ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
else:
    import importlib
    import recoverland.core.geometry_utils as _gu
    importlib.reload(_gu)
    repair_geometry_for_render = _gu.repair_geometry_for_render

    # --- Test 2 : valid POINT passthrough ---
    point_wkb = (b'\x01' + struct.pack('<I', 1)
                 + struct.pack('<d', 5.0) + struct.pack('<d', 5.0))
    try:
        g = repair_geometry_for_render(point_wkb, trace_id=trace_id)
        ok = g is not None and g.isGeosValid()
        msg = f"isGeosValid={ok} wkbType={g.wkbType() if g else None}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('valid_point_passthrough', ok, msg))

    # --- Test 3 : self-intersecting bowtie polygon must be repaired ---
    coords = [(0.0, 0.0), (10.0, 10.0), (0.0, 10.0), (10.0, 0.0), (0.0, 0.0)]
    bowtie = (b'\x01' + struct.pack('<I', 3) + struct.pack('<I', 1)
              + struct.pack('<I', len(coords)))
    for x, y in coords:
        bowtie += struct.pack('<d', x) + struct.pack('<d', y)
    try:
        g = repair_geometry_for_render(bowtie, trace_id=trace_id)
        ok = g is not None and g.isGeosValid() and not g.isEmpty()
        msg = f"isGeosValid={ok} wkbType={g.wkbType() if g else None}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('bowtie_polygon_repaired', ok, msg))

    # --- Test 4 : None input returns None without exception ---
    try:
        g = repair_geometry_for_render(None, trace_id=trace_id)
        ok = g is None
        msg = f"got={g!r}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('none_input_returns_none', ok, msg))

    # --- Test 5 : corrupted WKB does not crash ---
    try:
        g = repair_geometry_for_render(
            b'\x01\xff\xff\xff\xff\x00\x00', trace_id=trace_id,
        )
        ok = (g is None
              or (hasattr(g, 'isEmpty') and g.isEmpty())
              or (hasattr(g, 'isGeosValid') and not g.isGeosValid()))
        msg = f"got={g!r}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('corrupt_input_returns_none_or_invalid', ok, msg))

    # --- Test 6 : log signature emitted ---
    time.sleep(0.3)  # let the log handler flush
    try:
        from qgis.utils import plugins
        log_path = Path(plugins['recoverland'].api_log_path())
        log_content = log_path.read_text(encoding='utf-8', errors='ignore')
        signature = "lens_geom_repair event=repaired"
        ok = signature in log_content and trace_id in log_content
        msg = f"signature='{signature}' trace_id={trace_id} found={ok}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('log_signature_emitted', ok, msg))


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il7] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il7] === END trace_id={trace_id} ===")
