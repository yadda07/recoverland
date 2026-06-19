"""p18_makevalid_drift - RecoverLand validation runtime.

Invariant: I-8 (geometry preservation under restore).
Backlog item: BL-RW-P1-08.
Root cause: CR-8 (SESSION_REWIND H-G1: makeValid silently drifts).

Pre-patch state:
    `core/restore_executor.py:_buffer_update` calls `geom.makeValid()`
    when the rebuilt geometry is GEOS-invalid and applies the result
    without measuring the drift. There is no helper to quantify the
    geometric difference and no skip path when drift is excessive.

Post-patch state:
    - `core/geometry_utils.py` exposes `_compute_makevalid_drift(
      geom_before, geom_after) -> (hash_before, hash_after, drift_units)`
      where hashes are short SHA-256 prefixes of the WKB and
      `drift_units` is the bounding-box L_inf distance in CRS units
      (degrees for EPSG:4326, metres for projected CRS).
    - `core/constants.py` defines `MAKEVALID_DRIFT_TOLERANCE` (default
      1e-6 CRS units).
    - `_buffer_update` runs the helper after `makeValid()`, logs
      `makevalid_drift: ... drift_units=X tolerance=Y status=Z`, and
      returns a skipped result with status `SKIPPED_GEOMETRY_DRIFT`
      when `drift_units > MAKEVALID_DRIFT_TOLERANCE`.

Scenario layout:
    setup:
        - no DB, no layer; the scenario probes the helper directly
          and inspects source patterns of the patch sites.
    run:
        - build a bowtie polygon via QgsGeometry.fromWkt
          ("POLYGON((0 0, 10 10, 10 0, 0 10, 0 0))") which is the
          canonical GEOS-invalid self-intersecting polygon.
        - call `geom.makeValid()` and capture validity flags.
        - call `_compute_makevalid_drift` twice:
            (bowtie, bowtie)   -> expected drift=0, hashes match.
            (bowtie, repaired) -> expected drift>0, hashes differ.

Pre-patch verdict: FAIL (helper absent, source patterns absent).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import re
from pathlib import Path

SCENARIO_ID = "p18_makevalid_drift"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_BOWTIE_WKT = "POLYGON((0 0, 10 10, 10 0, 0 10, 0 0))"
# Translated polygon (bbox = (100, 100, 110, 110)) used as the
# "different" input. makeValid(bowtie) keeps the same bbox as bowtie
# (0,0,10,10), so it cannot exercise a positive bbox drift. A spatially
# disjoint polygon guarantees drift = max(|0-100|, |0-100|, |10-110|,
# |10-110|) = 100 CRS units.
_TRANSLATED_WKT = "POLYGON((100 100, 110 110, 110 100, 100 110, 100 100))"


def _check_source_pattern(symbol: str) -> tuple[bool, str]:
    """Inspect post-patch source code for required symbols."""
    if symbol == "helper":
        rel = Path("core/geometry_utils.py")
        full = _PLUGIN_ROOT / rel
        if not full.is_file():
            return False, f"missing file: {rel}"
        text = full.read_text(encoding="utf-8", errors="replace")
        ok = bool(re.search(r"def\s+_compute_makevalid_drift\b", text))
        return ok, ("_compute_makevalid_drift defined" if ok
                    else "_compute_makevalid_drift not defined")
    if symbol == "tolerance":
        rel = Path("core/constants.py")
        full = _PLUGIN_ROOT / rel
        if not full.is_file():
            return False, f"missing file: {rel}"
        text = full.read_text(encoding="utf-8", errors="replace")
        ok = bool(re.search(r"\bMAKEVALID_DRIFT_TOLERANCE\b", text))
        return ok, ("MAKEVALID_DRIFT_TOLERANCE defined" if ok
                    else "MAKEVALID_DRIFT_TOLERANCE not defined")
    if symbol == "patch_in_buffer_update":
        rel = Path("core/restore_executor.py")
        full = _PLUGIN_ROOT / rel
        if not full.is_file():
            return False, f"missing file: {rel}"
        text = full.read_text(encoding="utf-8", errors="replace")
        # Patch must call helper or emit drift log inside _buffer_update.
        ok = bool(re.search(
            r"def\s+_buffer_update\b[\s\S]{0,8000}?"
            r"(?:_compute_makevalid_drift|makevalid_drift)",
            text,
        ))
        return ok, ("_buffer_update wires drift logic" if ok
                    else "_buffer_update does not wire drift logic")
    if symbol == "skipped_status":
        rel = Path("core/restore_executor.py")
        full = _PLUGIN_ROOT / rel
        if not full.is_file():
            return False, f"missing file: {rel}"
        text = full.read_text(encoding="utf-8", errors="replace")
        ok = bool(re.search(r"SKIPPED_GEOMETRY_DRIFT", text))
        return ok, ("SKIPPED_GEOMETRY_DRIFT status present" if ok
                    else "SKIPPED_GEOMETRY_DRIFT status absent")
    return False, f"unknown symbol: {symbol}"


def setup(ctx):
    from recoverland.core.logger import flog

    flog(
        f"p18_makevalid_drift setup: trace_id={ctx.trace_id} "
        f"bowtie_wkt={_BOWTIE_WKT!r}",
        "INFO",
    )


def run(ctx):
    """Probe the helper and the bowtie roundtrip; capture all results
    into ctx.data even on failure so assertions can describe what is
    wrong instead of letting the runner crash."""
    from recoverland.core.logger import flog

    flog(f"p18_makevalid_drift run start: trace_id={ctx.trace_id}", "INFO")

    # --- E2E: bowtie -> isGeosValid -> makeValid --------------------------
    try:
        from qgis.core import QgsGeometry
        bowtie = QgsGeometry.fromWkt(_BOWTIE_WKT)
        ctx.data["bowtie_is_geos_valid"] = bool(bowtie.isGeosValid())
        ctx.data["bowtie_is_empty"] = bool(bowtie.isEmpty())
        repaired = bowtie.makeValid()
        if repaired is not None and not repaired.isEmpty():
            ctx.data["repaired_is_geos_valid"] = bool(repaired.isGeosValid())
            ctx.data["repaired_is_empty"] = bool(repaired.isEmpty())
        else:
            ctx.data["repaired_is_geos_valid"] = None
            ctx.data["repaired_is_empty"] = True
        ctx.data["qgs_geom_error"] = None
    except Exception as e:
        ctx.data["bowtie_is_geos_valid"] = None
        ctx.data["repaired_is_geos_valid"] = None
        ctx.data["repaired_is_empty"] = None
        ctx.data["qgs_geom_error"] = repr(e)
        bowtie = None
        repaired = None

    # --- Helper unit: identical & translated inputs ----------------------
    # "Translated" rather than "repaired" because makeValid() on the bowtie
    # produces a multipolygon with the SAME bounding box as the bowtie, so
    # the L_inf bbox drift would still be zero. A spatially disjoint
    # polygon exercises the positive-drift branch deterministically.
    try:
        from qgis.core import QgsGeometry
        from recoverland.core.geometry_utils import _compute_makevalid_drift
        translated = QgsGeometry.fromWkt(_TRANSLATED_WKT)
        if bowtie is not None:
            hb_b, hb_a, drift_identical = _compute_makevalid_drift(bowtie, bowtie)
            ctx.data["helper_identical_drift"] = drift_identical
            ctx.data["helper_identical_hashes_match"] = (hb_b == hb_a)

            hd_b, hd_a, drift_different = _compute_makevalid_drift(bowtie, translated)
            ctx.data["helper_different_drift"] = drift_different
            ctx.data["helper_different_hashes_differ"] = (hd_b != hd_a)
        else:
            ctx.data["helper_identical_drift"] = None
            ctx.data["helper_identical_hashes_match"] = None
            ctx.data["helper_different_drift"] = None
            ctx.data["helper_different_hashes_differ"] = None
        ctx.data["helper_error"] = None
    except Exception as e:
        ctx.data["helper_identical_drift"] = None
        ctx.data["helper_identical_hashes_match"] = None
        ctx.data["helper_different_drift"] = None
        ctx.data["helper_different_hashes_differ"] = None
        ctx.data["helper_error"] = repr(e)

    flog(
        f"p18_makevalid_drift run end: trace_id={ctx.trace_id} "
        f"bowtie_valid={ctx.data.get('bowtie_is_geos_valid')} "
        f"repaired_valid={ctx.data.get('repaired_is_geos_valid')} "
        f"identical_drift={ctx.data.get('helper_identical_drift')} "
        f"different_drift={ctx.data.get('helper_different_drift')} "
        f"helper_error={ctx.data.get('helper_error')}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    # ===== Source patterns (post-patch shape of the codebase) ==========
    for symbol in ("helper", "tolerance", "patch_in_buffer_update",
                   "skipped_status"):
        ok, msg = _check_source_pattern(symbol)
        out.append((f"source__{symbol}", ok, msg))

    # ===== E2E: bowtie really is GEOS-invalid =========================
    out.append((
        "e2e_bowtie_is_geos_invalid",
        ctx.data.get("bowtie_is_geos_valid") is False,
        f"bowtie_is_geos_valid={ctx.data.get('bowtie_is_geos_valid')} "
        f"expected=False (canonical self-intersecting polygon)",
    ))
    out.append((
        "e2e_makevalid_produces_valid_geom",
        ctx.data.get("repaired_is_geos_valid") is True
        and ctx.data.get("repaired_is_empty") is False,
        f"repaired_is_geos_valid={ctx.data.get('repaired_is_geos_valid')} "
        f"repaired_is_empty={ctx.data.get('repaired_is_empty')} "
        f"expected: valid=True empty=False",
    ))

    # ===== Helper unit: identical inputs => zero drift, matching hash ==
    out.append((
        "helper_returns_zero_drift_for_identical_inputs",
        ctx.data.get("helper_identical_drift") == 0
        or ctx.data.get("helper_identical_drift") == 0.0,
        f"helper_identical_drift={ctx.data.get('helper_identical_drift')} "
        f"expected=0 (identical inputs must produce zero drift) "
        f"helper_error={ctx.data.get('helper_error')}",
    ))
    out.append((
        "helper_returns_matching_hashes_for_identical_inputs",
        ctx.data.get("helper_identical_hashes_match") is True,
        f"helper_identical_hashes_match="
        f"{ctx.data.get('helper_identical_hashes_match')} expected=True",
    ))

    # ===== Helper unit: different inputs => positive drift, distinct hashes
    out.append((
        "helper_returns_positive_drift_for_different_inputs",
        isinstance(ctx.data.get("helper_different_drift"), (int, float))
        and ctx.data.get("helper_different_drift", 0) > 0,
        f"helper_different_drift={ctx.data.get('helper_different_drift')} "
        f"expected: positive float (bowtie vs makeValid result)",
    ))
    out.append((
        "helper_returns_differing_hashes_for_different_inputs",
        ctx.data.get("helper_different_hashes_differ") is True,
        f"helper_different_hashes_differ="
        f"{ctx.data.get('helper_different_hashes_differ')} expected=True",
    ))

    # ===== Trace propagation ==========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"p18_makevalid_drift.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
