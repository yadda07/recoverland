"""Scenario IL-2 - lens_contracts purity gate.

Verifies that core/lens_contracts.py exists, compiles, contains the 9
expected symbols, and stays pure-Python (no QGIS/Qt import).

Cause racine: prerequis architectural. Without frozen contracts each
downstream module would invent its own NamedTuples and diverge (cf.
AP-INT-2 of the backlog).

Acceptance assertions (each is also an antithesis test):
    1. File core/lens_contracts.py exists.
    2. File compiles via py_compile.
    3. Import is QGIS-free: 'qgis' must NOT be in sys.modules after import.
    4. Import is Qt-free: 'PyQt5'/'PyQt6'/'qtpy' must NOT be in sys.modules.
    5. All 9 expected symbols are defined in the module.
    6. All 9 symbols are re-exported from core/__init__.py.
    7. The three Enums have string values (stable, append-only).
    8. The six NamedTuples are immutable (defined via typing.NamedTuple).

Initial expected verdict: FAIL (file does not exist yet).
Post-patch expected verdict: PASS.

This scenario runs without QGIS. It is a structural check.
"""
from __future__ import annotations

import importlib
import importlib.util
import py_compile
import sys
from pathlib import Path

SCENARIO_ID = "il2_contracts_pure"
INVARIANT = "BL-IL-P0-02"
EXPECTED_SIGNATURE = r""  # pure-python structural check

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_CONTRACTS_PATH = _PLUGIN_ROOT / "core" / "lens_contracts.py"
_INIT_PATH = _PLUGIN_ROOT / "core" / "__init__.py"

_EXPECTED_SYMBOLS = (
    "LensOpFilter",
    "LensVisualizationMode",
    "EntityClassification",
    "LensSelection",
    "LensFetchStats",
    "EntityState",
    "EntityTimeline",
    "LensRenderPlan",
    "LensRenderResult",
)

_QGIS_MODULE_PREFIXES = ("qgis", "PyQt5", "PyQt6", "qtpy", "PySide2", "PySide6")


def setup(ctx):
    ctx.data["contracts_path"] = _CONTRACTS_PATH
    ctx.data["init_path"] = _INIT_PATH
    ctx.data["contracts_exists"] = _CONTRACTS_PATH.is_file()


def _safe_import_contracts():
    """Try to import core.lens_contracts in isolation.

    Returns (module_or_None, error_or_None, leaked_modules).
    leaked_modules is the list of qgis/Qt-related modules present in
    sys.modules AFTER the import attempt.
    """
    # Snapshot baseline of qgis-related modules already loaded (e.g.
    # because the test runner itself imported them).
    baseline = {
        name for name in sys.modules
        if any(name == p or name.startswith(p + ".")
               for p in _QGIS_MODULE_PREFIXES)
    }

    # Drop any cached version of the module so importlib re-executes it.
    mod_name = "recoverland.core.lens_contracts"
    sys.modules.pop(mod_name, None)
    sys.modules.pop("core.lens_contracts", None)

    spec = importlib.util.spec_from_file_location(
        "core.lens_contracts",
        str(_CONTRACTS_PATH),
    )
    if spec is None or spec.loader is None:
        return None, "cannot build importlib spec", []
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001 - we want to report any failure
        return None, repr(exc), []

    after = {
        name for name in sys.modules
        if any(name == p or name.startswith(p + ".")
               for p in _QGIS_MODULE_PREFIXES)
    }
    leaked = sorted(after - baseline)
    return module, None, leaked


def _check_symbols_in_init(text: str) -> tuple[bool, str]:
    missing = [s for s in _EXPECTED_SYMBOLS if s not in text]
    if missing:
        return False, f"core/__init__.py missing symbols: {missing}"
    return True, f"all {len(_EXPECTED_SYMBOLS)} symbols mentioned in core/__init__.py"


def _check_module_symbols(module) -> tuple[bool, str]:
    missing = [s for s in _EXPECTED_SYMBOLS if not hasattr(module, s)]
    if missing:
        return False, f"module attributes missing: {missing}"
    return True, f"all {len(_EXPECTED_SYMBOLS)} symbols defined in module"


def _check_enums(module) -> tuple[bool, str]:
    """The three Enums must have str values (stable, append-only)."""
    issues = []
    for enum_name in ("LensOpFilter", "LensVisualizationMode", "EntityClassification"):
        cls = getattr(module, enum_name, None)
        if cls is None:
            issues.append(f"{enum_name}: missing")
            continue
        if not hasattr(cls, "__members__"):
            issues.append(f"{enum_name}: not an Enum")
            continue
        for member_name, member in cls.__members__.items():
            if not isinstance(member.value, str):
                issues.append(
                    f"{enum_name}.{member_name} value is not str ({type(member.value).__name__})"
                )
    if issues:
        return False, "; ".join(issues)
    return True, "all 3 enums have str values"


def _check_named_tuples(module) -> tuple[bool, str]:
    """The six NamedTuples must inherit from tuple and have _fields."""
    issues = []
    expected_tuples = (
        "LensSelection", "LensFetchStats", "EntityState",
        "EntityTimeline", "LensRenderPlan", "LensRenderResult",
    )
    for name in expected_tuples:
        cls = getattr(module, name, None)
        if cls is None:
            issues.append(f"{name}: missing")
            continue
        if not hasattr(cls, "_fields"):
            issues.append(f"{name}: missing _fields (not a NamedTuple?)")
            continue
        if not issubclass(cls, tuple):
            issues.append(f"{name}: does not inherit from tuple")
    if issues:
        return False, "; ".join(issues)
    return True, f"all {len(expected_tuples)} NamedTuples valid"


def run(ctx):
    """No-op: file inspection only."""
    return


def assertions(ctx):
    results = []

    exists = ctx.data["contracts_exists"]
    results.append((
        "contracts_file_exists",
        exists,
        f"path={ctx.data['contracts_path']} exists={exists}",
    ))

    if not exists:
        for name in (
            "contracts_compiles",
            "contracts_qgis_free",
            "contracts_qt_free",
            "contracts_symbols_defined",
            "contracts_symbols_exported",
            "contracts_enums_have_str_values",
            "contracts_named_tuples_valid",
        ):
            results.append((name, False, "skipped: contracts file missing"))
        return results

    # 2. py_compile
    try:
        py_compile.compile(str(_CONTRACTS_PATH), doraise=True)
        results.append(("contracts_compiles", True, "py_compile OK"))
        compiles = True
    except py_compile.PyCompileError as exc:
        results.append(("contracts_compiles", False, f"py_compile FAIL: {exc.msg}"))
        compiles = False
    except Exception as exc:  # noqa: BLE001
        results.append(("contracts_compiles", False, f"py_compile error: {exc!r}"))
        compiles = False

    if not compiles:
        for name in (
            "contracts_qgis_free",
            "contracts_qt_free",
            "contracts_symbols_defined",
            "contracts_symbols_exported",
            "contracts_enums_have_str_values",
            "contracts_named_tuples_valid",
        ):
            results.append((name, False, "skipped: file does not compile"))
        return results

    # 3. & 4. Import in isolation, check no QGIS/Qt leak
    module, import_error, leaked = _safe_import_contracts()
    if module is None:
        for name in (
            "contracts_qgis_free",
            "contracts_qt_free",
            "contracts_symbols_defined",
            "contracts_symbols_exported",
            "contracts_enums_have_str_values",
            "contracts_named_tuples_valid",
        ):
            results.append((name, False, f"skipped: import failed: {import_error}"))
        return results

    qgis_leaks = [m for m in leaked if m == "qgis" or m.startswith("qgis.")]
    qt_leaks = [m for m in leaked
                if any(m == p or m.startswith(p + ".")
                       for p in _QGIS_MODULE_PREFIXES if p != "qgis")]

    results.append((
        "contracts_qgis_free",
        not qgis_leaks,
        f"qgis modules loaded by import: {qgis_leaks or 'none'}",
    ))
    results.append((
        "contracts_qt_free",
        not qt_leaks,
        f"Qt modules loaded by import: {qt_leaks or 'none'}",
    ))

    ok, msg = _check_module_symbols(module)
    results.append(("contracts_symbols_defined", ok, msg))

    init_text = _INIT_PATH.read_text(encoding="utf-8", errors="replace")
    ok, msg = _check_symbols_in_init(init_text)
    results.append(("contracts_symbols_exported", ok, msg))

    ok, msg = _check_enums(module)
    results.append(("contracts_enums_have_str_values", ok, msg))

    ok, msg = _check_named_tuples(module)
    results.append(("contracts_named_tuples_valid", ok, msg))

    return results


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
