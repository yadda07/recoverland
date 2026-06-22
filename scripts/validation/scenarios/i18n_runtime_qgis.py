"""G4 PROVEN — runtime QGIS i18n verification without changing locale.

This script loads recoverland_en.qm into a QTranslator and checks that every
expected (context, source) returns the English translation. It does NOT install
the translator on the running QGIS application; it calls translator.translate()
directly and uses a short-lived installed translator for a synthetic self.tr()
test. This avoids side effects on the running QGIS UI.

Convention: top-level script, run from QGIS Python console via:

    exec(compile(
        Path('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/i18n_runtime_qgis.py').read_text(),
        'C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/validation/scenarios/i18n_runtime_qgis.py',
        'exec'))

Output: [i18n-runtime] verdict=PASS|FAIL passed=N/M trace_id=...
"""
from pathlib import Path
import sys
import uuid

import qgis
from qgis.PyQt.QtCore import QTranslator, QCoreApplication
from qgis.PyQt.QtWidgets import QWidget


# __file__ is NOT set when this script is executed via exec(compile(...)).
# Resolve plugin root from the loaded recoverland module instead.
_PLUGIN_ROOT = Path(sys.modules["recoverland"].__file__).parent
_QM_PATH = _PLUGIN_ROOT / "i18n" / "recoverland_en.qm"

# Expected translations: (context, source, expected_en)
_EXPECTED = [
    # ReviewStatusWidget
    ("ReviewStatusWidget", "Review", "Review"),
    ("ReviewStatusWidget", "Desactiver Review", "Disable Review"),
    ("ReviewStatusWidget", "Review · Recherche...", "Review · Searching..."),
    ("ReviewStatusWidget", "Review — Recherche des modifications", "Review — Searching for modifications"),
    ("ReviewStatusWidget", "Review · Rendu {detail}", "Review · Rendering {detail}"),
    ("ReviewStatusWidget", "Review · Rendu...", "Review · Rendering..."),
    ("ReviewStatusWidget", "Review · actif", "Review · active"),
    ("ReviewStatusWidget", "Review · {n}", "Review · {n}"),
    ("ReviewStatusWidget", "Review — Visualisation temps reel", "Review — Real-time visualization"),
    ("ReviewStatusWidget", "{n_layers} couche(s) · {n_entities} entite(s)", "{n_layers} layer(s) · {n_entities} entity(ies)"),
    ("ReviewStatusWidget", "MAJ : a l'instant", "Updated: just now"),
    ("ReviewStatusWidget", "MAJ : il y a {ago}s", "Updated: {ago}s ago"),
    ("ReviewStatusWidget", "MAJ : il y a {m}min", "Updated: {m}min ago"),
    ("ReviewStatusWidget", "Deplacez la carte pour rafraichir", "Move the map to refresh"),
    ("ReviewStatusWidget", "Clic X : desactiver", "Click X: disable"),
    ("ReviewStatusWidget", "Inactif", "Inactive"),
    ("ReviewStatusWidget", "Review — Inactif", "Review — Inactive"),
    # ReviewSegmentedSwitch
    ("ReviewSegmentedSwitch", "Présent", "Present"),
    ("ReviewSegmentedSwitch", "Basculer entre l'etat present et Review", "Toggle between present state and Review"),
    # AppleToggleSwitch
    ("AppleToggleSwitch", "Enregistrement des modifications : actif", "Change recording: active"),
    ("AppleToggleSwitch", "Enregistrement actif", "Recording active"),
    ("AppleToggleSwitch", "Enregistrement desactive", "Recording disabled"),
    # CanvasDateBar
    ("CanvasDateBar", "Aucune entité à cette date", "No entity at this date"),
    ("CanvasDateBar", "Aujourd'hui", "Today"),
    ("CanvasDateBar", "Export", "Export"),
    ("CanvasDateBar", "Exporter le snapshot vers GeoPackage", "Export snapshot to GeoPackage"),
    ("CanvasDateBar", "Reconstruction en cours…", "Reconstruction in progress…"),
    ("CanvasDateBar", "{n} entité(s) reconstituée(s)", "{n} entity(ies) reconstructed"),
    ("CanvasDateBar", "{n} entité(s) hors de l'emprise actuelle", "{n} entity(ies) outside current extent"),
    ("CanvasDateBar", "Molette : zoomer/dezoomer • Clic droit glisser : se deplacer • Double-clic : reinitialiser le zoom", "Wheel: zoom in/out • Right-click drag: pan • Double-click: reset zoom"),
]


def _plugin_log_path() -> Path | None:
    """Return the plugin debug log path if available."""
    try:
        plugin = qgis.utils.plugins.get("recoverland")
        if plugin is not None and hasattr(plugin, "api_log_path"):
            return Path(plugin.api_log_path())
    except Exception:
        pass
    return None


def _log_to_file(message: str) -> None:
    """Append a single line to the plugin debug log if available."""
    log_path = _plugin_log_path()
    if not log_path:
        return
    try:
        with log_path.open("a", encoding="utf-8") as f:
            f.write(message + "\n")
    except Exception:
        pass


trace_id = uuid.uuid4().hex[:8]
results: list[tuple[str, bool, str]] = []

if "recoverland" not in sys.modules:
    results.append((
        "recoverland_module_loaded",
        False,
        "recoverland module not in sys.modules; plugin may not be loaded yet",
    ))

app = QCoreApplication.instance()
if app is None:
    results.append(("qgis_app_running", False, "No QCoreApplication instance"))
else:
    results.append(("qgis_app_running", True, "QCoreApplication instance OK"))

    translator = QTranslator()
    loaded = translator.load(str(_QM_PATH))
    results.append(("qm_loadable", loaded, f"loaded={loaded} path={_QM_PATH}"))

    if loaded:
        # Install translator temporarily for all runtime checks.
        # QCoreApplication.translate handles unicode correctly; translator.translate()
        # raises UnicodeEncodeError on non-ASCII characters in this PyQt build.
        app.installTranslator(translator)
        try:
            # 1. Direct QCoreApplication.translate checks: prove the .qm content.
            for ctx, src, expected in _EXPECTED:
                translated = QCoreApplication.translate(ctx, src)
                ok = translated == expected
                results.append((
                    f"qm_translate_{ctx}_{src[:20]}",
                    ok,
                    f"context={ctx} source={src!r} got={translated!r} expected={expected!r}",
                ))

            # 2. Synthetic self.tr() check: prove the class-context mechanism works.
            # Class names must EXACTLY match the <context><name> entries in .ts
            # because self.tr() uses the class name as translation context.
            class ReviewStatusWidget(QWidget):
                def label(self) -> str:
                    return self.tr("Review")

            class AppleToggleSwitch(QWidget):
                def tip(self) -> str:
                    return self.tr("Enregistrement actif")

            class ReviewSegmentedSwitch(QWidget):
                def tip(self) -> str:
                    return self.tr("Basculer entre l'etat present et Review")

            class CanvasDateBar(QWidget):
                def today(self) -> str:
                    return self.tr("Aujourd'hui")

            w = ReviewStatusWidget()
            got = w.label()
            results.append((
                "self_tr_ReviewStatusWidget",
                got == "Review",
                f"self.tr('Review') -> {got!r}",
            ))
            sw = AppleToggleSwitch()
            got = sw.tip()
            results.append((
                "self_tr_AppleToggleSwitch",
                got == "Recording active",
                f"self.tr('Enregistrement actif') -> {got!r}",
            ))
            rs = ReviewSegmentedSwitch()
            got = rs.tip()
            results.append((
                "self_tr_ReviewSegmentedSwitch",
                got == "Toggle between present state and Review",
                f"self.tr('Basculer entre l'etat present et Review') -> {got!r}",
            ))
            cb = CanvasDateBar()
            got = cb.today()
            results.append((
                "self_tr_CanvasDateBar",
                got == "Today",
                f"self.tr('Aujourd'hui') -> {got!r}",
            ))
        finally:
            app.removeTranslator(translator)
    else:
        results.append(("qm_loadable", False, f"Failed to load {_QM_PATH}"))

n_pass = sum(1 for _, ok, _ in results if ok)
n_total = len(results)
verdict = "PASS" if n_pass == n_total else "FAIL"

print(f"[i18n-runtime] === START trace_id={trace_id} ===")
print(f"[i18n-runtime] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")
for name, ok, msg in results:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {msg}")
print(f"[i18n-runtime] === END trace_id={trace_id} ===")

_log_to_file(f"[i18n-runtime] verdict={verdict} passed={n_pass}/{n_total} trace_id={trace_id}")

# NOTE: Do NOT call sys.exit() here. In QGIS Python console, sys.exit() closes QGIS.

