"""
il15_geogit_mode.py  -  Validation BL-IL-P1-15 (mode Review integre dans RecoverDialog)
=========================================================================================
Lance depuis la console Python QGIS :

    exec(compile(Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il15_geogit_mode.py').read_text(), 'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/lens/il15_geogit_mode.py', 'exec'))

Verifie :
 1. RestoreModeSelector a 3 modes valides (event, temporal, review)
 2. RestoreModeSelector a un bouton _btn_review
 3. setMode('review') fonctionne sans erreur
 4. recover_dialog source contient _recover_review_mode
 5. Mode review: bouton Recover texte=Visualiser + layer_input visible
 6. Mode review: results_group masque + restore_button masque
"""
import importlib
import sys
import uuid
from pathlib import Path

from qgis.utils import iface, plugins

_PLUGIN = Path(sys.modules['recoverland'].__file__).parent

trace_id = uuid.uuid4().hex[:8]
print(f"[il15] === START trace_id={trace_id} ===")

results = []

# --- Reload ---
for mod_name in (
    'recoverland.widgets.restore_mode_selector',
    'recoverland.recover_dialog',
):
    if mod_name in sys.modules:
        importlib.reload(sys.modules[mod_name])

from recoverland.widgets.restore_mode_selector import RestoreModeSelector


# --- Test 1 : VALID_MODES contains 3 modes ---
ok = hasattr(RestoreModeSelector, 'VALID_MODES')
modes = getattr(RestoreModeSelector, 'VALID_MODES', ())
ok = ok and set(modes) == {'event', 'temporal', 'review'}
results.append((
    'valid_modes_3',
    ok,
    f"modes={modes}",
))


# --- Test 2 : _btn_review exists ---
sel = RestoreModeSelector()
ok = hasattr(sel, '_btn_review')
results.append((
    'btn_review_exists',
    ok,
    f"has_btn={ok}",
))


# --- Test 3 : setMode('review') works ---
try:
    sel.setMode('review')
    ok = sel.mode() == 'review'
    msg = f"mode_after_set={sel.mode()}"
except Exception as exc:
    ok, msg = False, f"raised: {exc!r}"
results.append((
    'setMode_review_works',
    ok,
    msg,
))
sel.deleteLater()


# --- Test 4 : _recover_review_mode exists in RecoverDialog source ---
dialog_src = (
    _PLUGIN / 'recover_dialog.py'
).read_text(encoding='utf-8')
ok = 'def _recover_review_mode(self)' in dialog_src
results.append((
    'recover_review_mode_exists',
    ok,
    f"found={ok}",
))


# --- Test 5+6 : instantiate dialog, switch to review, check widget state ---
plugin = plugins.get('recoverland')
journal = getattr(plugin, '_journal', None) if plugin is not None else None
tracker = getattr(plugin, '_tracker', None) if plugin is not None else None
write_queue = getattr(plugin, '_write_queue', None) if plugin is not None else None

from recoverland.recover_dialog import RecoverDialog

try:
    dlg = RecoverDialog(iface, journal=journal, tracker=tracker,
                        write_queue=write_queue)
    dlg.restore_mode_selector.setMode('review')
    dlg._on_period_mode_changed('review')

    btn_text = dlg.recover_button.text()
    layer_visible = dlg.layer_input.isVisible()
    ok5 = ('Visualiser' in btn_text) and layer_visible
    results.append((
        'review_btn_text_and_layer_visible',
        ok5,
        f"btn_text={btn_text!r} layer_visible={layer_visible}",
    ))

    results_hidden = not dlg.results_group.isVisible()
    restore_hidden = not dlg.restore_button.isVisible()
    ok6 = results_hidden and restore_hidden
    results.append((
        'review_results_and_restore_hidden',
        ok6,
        f"results_hidden={results_hidden} restore_hidden={restore_hidden}",
    ))
    dlg.deleteLater()
except Exception as exc:
    results.append(('review_btn_text_and_layer_visible', False, f"raised: {exc!r}"))
    results.append(('review_results_and_restore_hidden', False, f"raised: {exc!r}"))


# --- Bilan ---
n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = 'PASS' if n_pass == n_total else 'FAIL'

print(f"[il15] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[il15] === END trace_id={trace_id} ===")
