"""Public-surface guard for the RecoverDialog God Object extraction.

BL-DIAG-P2-14 / Phase 0 (safety net). Protects risk R-2 of
``docs/backlog_god_object_recover_dialog_2026-06-21.md``: the only surface of
``RecoverDialog`` consumed by ``recover.py`` MUST stay byte-stable across every
extraction phase, and ``recover.py`` itself MUST stay untouched.

This is pure static analysis (ast + hashlib): no QGIS, no Qt, runnable in CI and
re-run as a precondition of every phase. It does NOT prove behaviour (that is the
golden-log job); it proves the contract shape did not drift.

Usage (from plugin root):
    python -m scripts.validation.god_object_surface_guard            # verify
    python -m scripts.validation.god_object_surface_guard --capture  # (re)baseline

Exit code 0 = PASS, 1 = FAIL. Output is structured key=value lines.
"""
from __future__ import annotations

import ast
import hashlib
import json
import sys
from pathlib import Path

_PLUGIN_ROOT = Path(__file__).resolve().parent.parent.parent
_DIALOG = _PLUGIN_ROOT / "recover_dialog.py"
_RECOVER = _PLUGIN_ROOT / "recover.py"
_BASELINE = Path(__file__).resolve().parent / "god_object_baseline.json"

# Intangible public surface (section 2 of the backlog). Arg names are positional
# order including ``self``; a rename, reorder, add or drop fails the guard.
_EXPECTED_METHODS = {
    "__init__": ["self", "iface", "journal", "tracker", "write_queue"],
    "cleanup_resources": ["self"],
    "on_project_switched": ["self", "tracker"],
    "on_events_committed": ["self", "edited_fingerprint"],
}
_EXPECTED_WRITABLE_ATTR = "_review_wants_persist"
_EXPECTED_BASE = "QDialog"


def _log(level: str, event: str, **fields) -> None:
    parts = [f"level={level}", "module=god_object_surface_guard", f"event={event}"]
    parts += [f"{k}={v}" for k, v in fields.items()]
    print(" ".join(parts))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _find_class(tree: ast.Module, name: str) -> ast.ClassDef | None:
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == name:
            return node
    return None


def _arg_names(fn: ast.FunctionDef) -> list[str]:
    a = fn.args
    return [arg.arg for arg in (*a.posonlyargs, *a.args)]


def _assigns_attr(cls: ast.ClassDef, attr: str) -> bool:
    for sub in ast.walk(cls):
        if isinstance(sub, ast.Assign):
            for tgt in sub.targets:
                if (isinstance(tgt, ast.Attribute) and tgt.attr == attr
                        and isinstance(tgt.value, ast.Name) and tgt.value.id == "self"):
                    return True
    return False


def _check_surface_tree(tree: ast.Module, results: list[tuple[str, bool, str]]) -> None:
    cls = _find_class(tree, "RecoverDialog")
    if cls is None:
        results.append(("class_RecoverDialog_present", False, "class not found"))
        return
    results.append(("class_RecoverDialog_present", True, "found"))

    base_names = {b.id for b in cls.bases if isinstance(b, ast.Name)}
    results.append((
        f"inherits_{_EXPECTED_BASE}",
        _EXPECTED_BASE in base_names,
        f"bases={sorted(base_names)}",
    ))

    methods = {
        n.name: n for n in cls.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for name, expected_args in _EXPECTED_METHODS.items():
        fn = methods.get(name)
        if fn is None:
            results.append((f"method_{name}", False, "missing"))
            continue
        actual = _arg_names(fn)
        ok = actual == expected_args
        results.append((
            f"method_{name}",
            ok,
            f"actual={actual} expected={expected_args}",
        ))

    results.append((
        f"attr_{_EXPECTED_WRITABLE_ATTR}_assigned",
        _assigns_attr(cls, _EXPECTED_WRITABLE_ATTR),
        "assigned in class body",
    ))


def _check_surface(results: list[tuple[str, bool, str]]) -> None:
    tree = ast.parse(_DIALOG.read_text(encoding="utf-8"))
    _check_surface_tree(tree, results)


def _check_recover_hash(results: list[tuple[str, bool, str]], capture: bool) -> None:
    current = _sha256(_RECOVER)
    if capture:
        _BASELINE.write_text(
            json.dumps({"recover_py_sha256": current}, indent=2) + "\n",
            encoding="utf-8",
        )
        _log("INFO", "BASELINE_CAPTURED", path=_BASELINE.name, sha=current[:12])
        results.append(("recover_py_baseline_captured", True, current[:12]))
        return
    if not _BASELINE.exists():
        results.append((
            "recover_py_hash_unchanged",
            True,
            "NO_BASELINE (run --capture to lock recover.py); skipped",
        ))
        _log("WARNING", "NO_BASELINE", hint="run --capture")
        return
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8")).get("recover_py_sha256", "")
    ok = current == baseline
    results.append((
        "recover_py_hash_unchanged",
        ok,
        f"current={current[:12]} baseline={baseline[:12]}",
    ))


# --- Antithese: the guard MUST be able to FAIL. Each fixture violates exactly
# one clause of the contract; the guard is expected to report >=1 failing check.
_SELFTEST_FIXTURES = {
    "renamed_public_method": (
        "class RecoverDialog(QDialog, LoggerMixin):\n"
        "    def __init__(self, iface, journal=None, tracker=None, write_queue=None): self._review_wants_persist=False\n"
        "    def cleanup_resources(self): pass\n"
        "    def on_project_renamed(self, tracker=None): pass\n"  # renamed!
        "    def on_events_committed(self, edited_fingerprint=''): pass\n"
    ),
    "dropped_init_arg": (
        "class RecoverDialog(QDialog, LoggerMixin):\n"
        "    def __init__(self, iface, journal=None): self._review_wants_persist=False\n"  # missing args!
        "    def cleanup_resources(self): pass\n"
        "    def on_project_switched(self, tracker=None): pass\n"
        "    def on_events_committed(self, edited_fingerprint=''): pass\n"
    ),
    "missing_persist_attr": (
        "class RecoverDialog(QDialog, LoggerMixin):\n"
        "    def __init__(self, iface, journal=None, tracker=None, write_queue=None): pass\n"  # no attr!
        "    def cleanup_resources(self): pass\n"
        "    def on_project_switched(self, tracker=None): pass\n"
        "    def on_events_committed(self, edited_fingerprint=''): pass\n"
    ),
    "lost_qdialog_base": (
        "class RecoverDialog(object):\n"  # not a QDialog!
        "    def __init__(self, iface, journal=None, tracker=None, write_queue=None): self._review_wants_persist=False\n"
        "    def cleanup_resources(self): pass\n"
        "    def on_project_switched(self, tracker=None): pass\n"
        "    def on_events_committed(self, edited_fingerprint=''): pass\n"
    ),
}


def _selftest() -> int:
    """Prove the guard detects each contract violation (antithese, G3.5)."""
    all_caught = True
    for label, src in _SELFTEST_FIXTURES.items():
        results: list[tuple[str, bool, str]] = []
        _check_surface_tree(ast.parse(src), results)
        caught = any(not ok for _, ok, _ in results)
        all_caught = all_caught and caught
        failing = [name for name, ok, _ in results if not ok]
        _log("INFO" if caught else "ERROR", "SELFTEST",
             fixture=label, detected_violation=caught, failing_checks=failing)
    _log("INFO" if all_caught else "ERROR", "SELFTEST_VERDICT",
         verdict="PASS" if all_caught else "FAIL",
         note="guard_can_fail" if all_caught else "GUARD_IS_A_RUBBER_STAMP")
    return 0 if all_caught else 1


def main(capture: bool) -> int:
    if not _DIALOG.exists() or not _RECOVER.exists():
        _log("ERROR", "SOURCE_MISSING", dialog=_DIALOG.exists(), recover=_RECOVER.exists())
        return 1

    results: list[tuple[str, bool, str]] = []
    _check_surface(results)
    _check_recover_hash(results, capture)

    passed = sum(1 for _, ok, _ in results if ok)
    failed = len(results) - passed
    for name, ok, detail in results:
        _log("INFO" if ok else "ERROR", "CHECK",
             name=name, ok=ok, detail=f"'{detail}'")
    verdict = "PASS" if failed == 0 else "FAIL"
    _log("INFO" if failed == 0 else "ERROR", "VERDICT",
         verdict=verdict, passed=passed, failed=failed, total=len(results))
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        raise SystemExit(_selftest())
    raise SystemExit(main(capture="--capture" in sys.argv))
