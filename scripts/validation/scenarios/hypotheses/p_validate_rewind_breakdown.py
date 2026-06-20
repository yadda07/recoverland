"""p_validate_rewind_breakdown - runtime proof for BL-RW-P1-09.

`scripts/validate_rewind.py` must expose the 5-bucket rewind breakdown
in the generated text report:

    REWIND  : applied=N skipped_idempotent=N failed=N
              failed_target_absent=N failed_geometry_drift=N

This scenario:
    1. imports `scripts.validate_rewind` (auto-run is guarded behind
       `__name__ == "__main__"`).
    2. writes a minimal snapshot JSON in a tempdir and points the
       module's `SNAPSHOT_PATH` at it.
    3. injects synthetic, distinguishable values into
       `_LAST_REWIND_BREAKDOWN`.
    4. calls `validate_rewind()` and reads
       `scripts/rewind_report_latest.txt`.
    5. asserts the REWIND line contains the exact 5-bucket counters.

Backups and restores `rewind_report_latest.txt` so the committed
reference is not clobbered by the test.

BL-RW-P1-09 / CR-7 / CR-8.
"""
from __future__ import annotations

import json
import re
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "p_validate_rewind_breakdown"
INVARIANT = "BL-RW-P1-09"
EXPECTED_SIGNATURE = (
    r"REWIND\s+:\s+applied=\d+\s+skipped_idempotent=\d+\s+failed=\d+\s+"
    r"failed_target_absent=\d+\s+failed_geometry_drift=\d+"
)

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_SCRIPTS_DIR = _PLUGIN_ROOT / "scripts"

# Distinct synthetic values so we know each one comes from us and not
# from a real previous run.
_INJECT = {
    "applied": 7,
    "skipped_idempotent": 3,
    "failed": 2,
    "failed_target_absent": 1,
    "failed_geometry_drift": 5,
    "total_ok": 10,
    "total_fail": 8,
    "errors": [],
}

_REWIND_LINE_RE = re.compile(
    r"REWIND\s+:\s+applied=(?P<applied>\d+)\s+"
    r"skipped_idempotent=(?P<skipped_idempotent>\d+)\s+"
    r"failed=(?P<failed>\d+)\s+"
    r"failed_target_absent=(?P<failed_target_absent>\d+)\s+"
    r"failed_geometry_drift=(?P<failed_geometry_drift>\d+)"
)


def _write_minimal_snapshot(target_path: Path) -> None:
    """Write a snapshot JSON with zero layers so validate_rewind has no
    real diff work to do but still produces a full report."""
    payload = {
        "snapshot_time": "2026-05-15T07:00:00",
        "layers": {},
    }
    target_path.write_text(json.dumps(payload), encoding="utf-8")


def setup(ctx):
    import sys
    from recoverland.core.logger import flog

    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))

    # Drop a previously cached version so the guarded __name__ logic
    # picks up the new import-friendly module on every run.
    sys.modules.pop("scripts.validate_rewind", None)

    import scripts.validate_rewind as vr  # noqa: WPS433

    tmpdir = Path(tempfile.mkdtemp(prefix="p_validate_rewind_"))
    snap_path = tmpdir / "stress_snapshot_inline.json"
    _write_minimal_snapshot(snap_path)

    report_path = _SCRIPTS_DIR / "rewind_report_latest.txt"
    backup_path = None
    if report_path.is_file():
        backup_path = tmpdir / "rewind_report_latest.bak"
        shutil.copyfile(report_path, backup_path)

    ctx.data["vr"] = vr
    ctx.data["tmpdir"] = tmpdir
    ctx.data["snap_path"] = snap_path
    ctx.data["report_path"] = report_path
    ctx.data["backup_path"] = backup_path

    flog(
        f"p_validate_rewind_breakdown setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} backup={'yes' if backup_path else 'no'}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    vr = ctx.data["vr"]
    snap_path = ctx.data["snap_path"]

    # Inject synthetic breakdown so the REWIND line is populated by
    # KNOWN values, not by leftover state from a real run.
    vr._LAST_REWIND_BREAKDOWN = dict(_INJECT)

    flog(
        f"p_validate_rewind_breakdown: invoking validate_rewind "
        f"snap={snap_path} trace_id={ctx.trace_id}",
        "INFO",
    )
    summary = vr.validate_rewind(snapshot_path=str(snap_path))
    ctx.data["summary"] = summary

    report_text = ctx.data["report_path"].read_text(encoding="utf-8")
    ctx.data["report_text"] = report_text

    match = _REWIND_LINE_RE.search(report_text)
    ctx.data["rewind_match"] = (
        {k: int(v) for k, v in match.groupdict().items()}
        if match is not None else None
    )

    flog(
        f"p_validate_rewind_breakdown: rewind_line_match="
        f"{ctx.data['rewind_match']} trace_id={ctx.trace_id}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    match = ctx.data.get("rewind_match")
    out.append((
        "p109_rewind_line_present",
        match is not None,
        f"REWIND line found={match is not None} pattern={_REWIND_LINE_RE.pattern!r}",
    ))

    expected = {
        k: _INJECT[k] for k in (
            "applied", "skipped_idempotent", "failed",
            "failed_target_absent", "failed_geometry_drift",
        )
    }
    out.append((
        "p109_rewind_line_values_match",
        match == expected,
        f"observed={match} expected={expected}",
    ))

    # Trace_id propagation in our setup log line (sanity).
    out.append(assert_log_contains(
        ctx.records,
        rf"p_validate_rewind_breakdown setup:\s+trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=1,
    ))

    # Cleanup: restore the original report, drop the tempdir.
    report_path = ctx.data.get("report_path")
    backup_path = ctx.data.get("backup_path")
    if report_path is not None and backup_path is not None:
        try:
            shutil.copyfile(backup_path, report_path)
        except Exception:
            pass
    tmpdir = ctx.data.get("tmpdir")
    if tmpdir is not None:
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
