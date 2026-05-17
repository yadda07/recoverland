"""
il12_i18n.py  -  Validation BL-IL-P0-12 (Internationalisation FR+EN)
=====================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il12_i18n.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il12_i18n.py', 'exec'))

Verifie :
 1. temporal_lens_dock.py contient >= 49 appels self.tr(
 2. recoverland_en.ts contient le context TemporalLensDock
 3. Le context TemporalLensDock contient >= 56 entries <message>
 4. recoverland_en.qm existe et est plus recent que le .ts
 5. Dock instanciable apres wrapping (pas de crash au self.tr())
 6. Aucun label hardcode restant dans setText/addItem/QLabel/QPushButton sans self.tr(
 7. map tool + polygon tool + lens_renderer : zero string UI hardcodee
"""
import os
import re
import sys
import uuid
from pathlib import Path

from qgis.utils import iface, plugins

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il12] === START trace_id={trace_id} ===")

results = []


# --- Test 1 : self.tr( count in temporal_lens_dock.py >= 49 ---
dock_path = _PLUGIN / 'widgets' / 'temporal_lens_dock.py'
dock_src = dock_path.read_text(encoding='utf-8') if dock_path.is_file() else ''
tr_count = len(re.findall(r'self\.tr\(', dock_src))
ok = tr_count >= 49
results.append((
    'dock_self_tr_count',
    ok,
    f"count={tr_count} expected>=49",
))


# --- Test 2 : recoverland_en.ts contains TemporalLensDock context ---
ts_path = _PLUGIN / 'i18n' / 'recoverland_en.ts'
ts_src = ts_path.read_text(encoding='utf-8') if ts_path.is_file() else ''
ctx_tag = '<name>TemporalLensDock</name>'
ok = ctx_tag in ts_src
results.append((
    'ts_has_context_TemporalLensDock',
    ok,
    f"found={ok} in={ts_path.name}",
))


# --- Test 3 : TemporalLensDock context has >= 56 <message> entries ---
ctx_pattern = re.compile(
    r'<context>\s*<name>TemporalLensDock</name>(.*?)</context>',
    re.DOTALL,
)
ctx_match = ctx_pattern.search(ts_src)
msg_count = 0
if ctx_match:
    msg_count = len(re.findall(r'<message>', ctx_match.group(1)))
ok = msg_count >= 56
results.append((
    'ts_context_message_count',
    ok,
    f"msg_count={msg_count} expected>=56",
))


# --- Test 4 : .qm exists and is newer than .ts ---
qm_path = _PLUGIN / 'i18n' / 'recoverland_en.qm'
qm_exists = qm_path.is_file()
qm_newer = False
if qm_exists and ts_path.is_file():
    qm_newer = os.path.getmtime(str(qm_path)) >= os.path.getmtime(str(ts_path))
ok = qm_exists and qm_newer
results.append((
    'qm_compiled_and_fresh',
    ok,
    f"exists={qm_exists} newer_than_ts={qm_newer}",
))


# --- Test 5 : dock instantiable after wrapping (no crash on self.tr) ---
import importlib
for mod_name in (
    'recoverland.widgets.temporal_lens_map_tool',
    'recoverland.widgets.temporal_lens_polygon_map_tool',
    'recoverland.widgets.temporal_lens_dock',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.widgets.temporal_lens_dock import TemporalLensDock

plugin = plugins.get('recoverland')
journal = getattr(plugin, '_journal', None) if plugin is not None else None
try:
    dock = TemporalLensDock(iface, journal=journal)
    ok_inst = dock is not None
    ok_label = dock.status_label.text() != ''
    ok_legend = dock.legend_age_label.text() != ''
    ok = ok_inst and ok_label and ok_legend
    msg = (
        f"instantiated={ok_inst} "
        f"status_text_set={ok_label} "
        f"legend_text_set={ok_legend}"
    )
except Exception as exc:
    dock = None
    ok, msg = False, f"raised: {exc!r}"
results.append(('dock_instantiable_after_tr', ok, msg))


# --- Test 6 : no bare hardcoded FR label without self.tr() in dock ---
# Pattern: setText("...", addItem("...", QLabel("...", QPushButton("...
# that do NOT have self.tr( before the opening quote.
bare_pattern = re.compile(
    r'(?:setText|addItem|QLabel|QPushButton)\(\s*"(?!__)'
)
bare_hits = []
for i, line in enumerate(dock_src.splitlines(), 1):
    stripped = line.strip()
    if stripped.startswith('#') or stripped.startswith('_flog'):
        continue
    if bare_pattern.search(line) and 'self.tr(' not in line:
        bare_hits.append(f"L{i}")
ok = len(bare_hits) == 0
results.append((
    'no_bare_hardcoded_labels',
    ok,
    f"bare_hits={len(bare_hits)} lines={bare_hits[:5]}",
))


# --- Test 7 : map tools + renderer have zero UI-facing hardcoded strings ---
# These files should NOT have setText/addItem/QLabel/QPushButton with bare
# strings, confirming no i18n wrapping was needed for them.
tool_paths = [
    _PLUGIN / 'widgets' / 'temporal_lens_map_tool.py',
    _PLUGIN / 'widgets' / 'temporal_lens_polygon_map_tool.py',
    _PLUGIN / 'core' / 'lens_renderer.py',
]
ui_pattern = re.compile(
    r'(?:setText|addItem|QLabel|QPushButton)\('
)
offending = []
for p in tool_paths:
    if not p.is_file():
        continue
    src = p.read_text(encoding='utf-8')
    for i, line in enumerate(src.splitlines(), 1):
        if ui_pattern.search(line):
            offending.append(f"{p.name}:L{i}")
ok = len(offending) == 0
results.append((
    'no_ui_strings_in_tools_renderer',
    ok,
    f"offending={len(offending)} files={offending[:5]}",
))


# Cleanup
if dock is not None:
    try:
        dock.deleteLater()
    except Exception:
        pass


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il12] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il12] === END trace_id={trace_id} ===")
