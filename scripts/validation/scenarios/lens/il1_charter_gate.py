"""Scenario IL-1 - Charter gate.

Verifies the Time Lens oracle (docs/lens_charter.md) exists and contains
the required structural sections before any Lens code lands. Implements
the antithesis-driven acceptance for BL-IL-P0-01.

Cause racine: prerequis methodologique. Without a frozen oracle, the
/critique reviewer profile BLOCKs every Lens PR by default.

Acceptance criteria (each one is an assertion):
    1. Charter file exists at docs/lens_charter.md.
    2. Charter contains the 5 Lens invariants IL-I1..IL-I5.
    3. Charter contains the 5 technical KPIs K-IL-1..K-IL-5.
    4. Charter cross-references rewind_charter.md for shared invariants.
    5. Charter declares the validation method (log evidence / runtime QGIS).

Initial expected verdict: FAIL (charter file does not exist yet).
Post-patch expected verdict: PASS (charter file created by BL-IL-P0-01).

This scenario runs without QGIS. It is a documentary check.
"""
from __future__ import annotations

from pathlib import Path

SCENARIO_ID = "il1_charter_gate"
INVARIANT = "BL-IL-P0-01"
EXPECTED_SIGNATURE = r""  # pure file check, no log signature

# Resolve the plugin root once. The scenario file lives at
# <root>/scripts/validation/scenarios/lens/il1_charter_gate.py
_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_CHARTER_PATH = _PLUGIN_ROOT / "docs" / "lens_charter.md"
_REWIND_CHARTER_PATH = _PLUGIN_ROOT / "docs" / "rewind_charter.md"


def setup(ctx):
    """No state to prepare; record file paths in ctx.data for assertions."""
    ctx.data["charter_path"] = _CHARTER_PATH
    ctx.data["rewind_charter_path"] = _REWIND_CHARTER_PATH
    ctx.data["charter_exists"] = _CHARTER_PATH.is_file()
    if ctx.data["charter_exists"]:
        ctx.data["charter_text"] = _CHARTER_PATH.read_text(
            encoding="utf-8", errors="replace"
        )
    else:
        ctx.data["charter_text"] = ""


def run(ctx):
    """No action under test; this scenario only inspects files."""
    return


def _check_invariants(text: str) -> tuple[bool, str]:
    missing = []
    for n in range(1, 6):
        marker = f"IL-I{n}"
        if marker not in text:
            missing.append(marker)
    if missing:
        return False, f"missing invariants: {missing}"
    return True, "all IL-I1..IL-I5 present"


def _check_kpis(text: str) -> tuple[bool, str]:
    missing = []
    for n in range(1, 6):
        marker = f"K-IL-{n}"
        if marker not in text:
            missing.append(marker)
    if missing:
        return False, f"missing KPIs: {missing}"
    return True, "all K-IL-1..K-IL-5 present"


def _check_rewind_reference(text: str) -> tuple[bool, str]:
    if "rewind_charter" in text:
        return True, "references rewind_charter found"
    return False, "no reference to rewind_charter.md"


def _check_validation_method(text: str) -> tuple[bool, str]:
    lowered = text.lower()
    keywords = ("validation", "log", "runtime")
    missing = [kw for kw in keywords if kw not in lowered]
    if missing:
        return False, f"validation-method keywords missing: {missing}"
    return True, "validation method declared (validation/log/runtime keywords found)"


def assertions(ctx):
    results = []

    charter_path: Path = ctx.data["charter_path"]
    exists = ctx.data["charter_exists"]
    results.append((
        "charter_file_exists",
        exists,
        f"path={charter_path} exists={exists}",
    ))

    if not exists:
        # Short-circuit: other assertions cannot be evaluated without the file.
        # Mark them as FAIL with explicit reason.
        results.append((
            "charter_invariants_present",
            False,
            "skipped: charter file missing",
        ))
        results.append((
            "charter_kpis_present",
            False,
            "skipped: charter file missing",
        ))
        results.append((
            "charter_references_rewind",
            False,
            "skipped: charter file missing",
        ))
        results.append((
            "charter_declares_validation_method",
            False,
            "skipped: charter file missing",
        ))
        return results

    text = ctx.data["charter_text"]

    ok, msg = _check_invariants(text)
    results.append(("charter_invariants_present", ok, msg))

    ok, msg = _check_kpis(text)
    results.append(("charter_kpis_present", ok, msg))

    ok, msg = _check_rewind_reference(text)
    results.append(("charter_references_rewind", ok, msg))

    ok, msg = _check_validation_method(text)
    results.append(("charter_declares_validation_method", ok, msg))

    return results


if __name__ == "__main__":
    # Allow running directly without the full QGIS runner.
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        import sys
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
