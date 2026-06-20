"""
il6_crs_reprojection.py  -  Validation BL-IL-P0-06 (CRS reprojection)
=====================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il6_crs_reprojection.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il6_crs_reprojection.py', 'exec'))

Pattern stress_edit : script linéaire, pas de runner, pas de guard.
"""
import sys
import time
import uuid
from pathlib import Path

from qgis.core import QgsGeometry, QgsPointXY  # noqa: F401

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il6] === START trace_id={trace_id} ===")

results = []  # list of (name, ok, msg)


# --- Test 1 : helper present in core/geometry_utils.py ---
geom_utils_src = (_PLUGIN / 'core' / 'geometry_utils.py').read_text(encoding='utf-8')
helper_in_source = 'def reproject_geometry_for_render' in geom_utils_src
results.append((
    'helper_defined_in_source',
    helper_in_source,
    f"def reproject_geometry_for_render in core/geometry_utils.py = {helper_in_source}",
))

if not helper_in_source:
    print("[il6] helper not in source, aborting")
    for name, ok, msg in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
else:
    import importlib
    import recoverland.core.geometry_utils as _gu
    importlib.reload(_gu)
    reproject_geometry_for_render = _gu.reproject_geometry_for_render

    cache = {}

    # --- Test 2 : valid reprojection EPSG:2154 -> EPSG:3857 ---
    # Lambert-93 (750000, 6480000) is in central France (~3.05E, 45.49N).
    # Web Mercator coords for that area: x in [300k, 400k], y in [5.6M, 5.8M].
    point_2154 = QgsGeometry.fromPointXY(QgsPointXY(750000.0, 6480000.0))
    try:
        g = reproject_geometry_for_render(
            point_2154, 'EPSG:2154', 'EPSG:3857', cache, trace_id=trace_id,
        )
        if g is None or g.isEmpty():
            ok, msg = False, f"got={g!r}"
        else:
            p = g.asPoint()
            in_mercator_france = (200000 < p.x() < 500000) and (5500000 < p.y() < 6000000)
            ok = in_mercator_france
            msg = f"reprojected to ({p.x():.1f},{p.y():.1f}) in_mercator_france={ok}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('valid_point_reproject_2154_to_3857', ok, msg))

    # --- Test 3 : second call same pair = cache hit ---
    n_before = len(cache)
    point_2 = QgsGeometry.fromPointXY(QgsPointXY(750100.0, 6480100.0))
    try:
        g = reproject_geometry_for_render(
            point_2, 'EPSG:2154', 'EPSG:3857', cache, trace_id=trace_id,
        )
        n_after = len(cache)
        ok = (n_before == 1 and n_after == 1 and g is not None)
        msg = f"cache size before={n_before} after={n_after} (expected 1/1)"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('cache_hit_second_call', ok, msg))

    # --- Test 4 : None src CRS returns None ---
    point_3 = QgsGeometry.fromPointXY(QgsPointXY(750000.0, 6480000.0))
    try:
        g = reproject_geometry_for_render(
            point_3, None, 'EPSG:3857', cache, trace_id=trace_id,
        )
        ok = g is None
        msg = f"got={g!r}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('none_src_crs_returns_none', ok, msg))

    # --- Test 5 : invalid src CRS returns None ---
    point_4 = QgsGeometry.fromPointXY(QgsPointXY(750000.0, 6480000.0))
    try:
        g = reproject_geometry_for_render(
            point_4, 'EPSG:99999', 'EPSG:3857', cache, trace_id=trace_id,
        )
        ok = g is None
        msg = f"got={g!r}"
    except Exception as exc:
        ok, msg = False, f"raised: {exc!r}"
    results.append(('invalid_src_crs_returns_none', ok, msg))

    # --- Test 6 & 7 : log signatures (reprojected + skipped) ---
    time.sleep(0.3)
    try:
        from qgis.utils import plugins
        log_path = Path(plugins['recoverland'].api_log_path())
        log_content = log_path.read_text(encoding='utf-8', errors='ignore')

        sig_reproj = "lens_geom_reproject event=reprojected"
        ok_r = sig_reproj in log_content and trace_id in log_content
        results.append((
            'log_signature_reprojected',
            ok_r,
            f"signature='{sig_reproj}' trace_id={trace_id} found={ok_r}",
        ))

        sig_skip = "lens_geom_reproject event=skipped"
        ok_s = sig_skip in log_content and trace_id in log_content
        results.append((
            'log_signature_skipped',
            ok_s,
            f"signature='{sig_skip}' trace_id={trace_id} found={ok_s}",
        ))
    except Exception as exc:
        results.append(('log_signature_reprojected', False, f"raised: {exc!r}"))
        results.append(('log_signature_skipped', False, f"raised: {exc!r}"))


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il6] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il6] === END trace_id={trace_id} ===")
