"""Runner for RecoverLand runtime validation scenarios.

A scenario is a Python module that exposes:

    SCENARIO_ID: str                 - unique, snake_case
    INVARIANT: str                   - "I-N" or "BL-RW-PX-NN"
    setup(ctx)                        - prepare state (idempotent)
    run(ctx)                          - perform the action under test
    assertions(ctx) -> list[Assertion]

ctx is a SimpleNamespace populated by the runner:

    ctx.scenario_id, ctx.invariant, ctx.t0, ctx.trace_id
    ctx.log_path, ctx.log_offset_start, ctx.log_offset_end
    ctx.records   (filled after run, before assertions)
    ctx.data      (free for the scenario to stash state)
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable

from .assert_log import AssertionSummary, summarize
from .parse_log import LogRecord, log_file_size, read_records

_HERE = Path(__file__).resolve().parent
_REPORTS_DIR = _HERE / "reports"
_REGRESSION_DIR = _HERE / "scenarios" / "regression"


def _resolve_log_path() -> Path:
    """Return the RecoverLand debug log path via the plugin public API.

    Falls back to the conventional profile location if the plugin is
    not loaded (offline tooling).
    """
    try:
        from qgis.utils import plugins  # type: ignore
        plugin = plugins.get("recoverland")
        if plugin is not None and hasattr(plugin, "api_log_path"):
            return Path(plugin.api_log_path())
    except Exception:
        pass
    # Fallback: standard profile location for the dev workstation
    return (
        Path.home()
        / "AppData" / "Roaming" / "QGIS" / "QGIS4"
        / "profiles" / "default"
        / "recoverland" / "recoverland_debug.log"
    )


def _new_trace_id() -> str:
    return uuid.uuid4().hex[:8]


def _ensure_reports_dir() -> Path:
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return _REPORTS_DIR


def _build_ctx(scenario_module) -> SimpleNamespace:
    scenario_id = getattr(scenario_module, "SCENARIO_ID", scenario_module.__name__)
    invariant = getattr(scenario_module, "INVARIANT", "")
    return SimpleNamespace(
        scenario_id=scenario_id,
        invariant=invariant,
        t0=datetime.now(timezone.utc),
        trace_id=_new_trace_id(),
        log_path=_resolve_log_path(),
        log_offset_start=0,
        log_offset_end=0,
        records=[],
        data={},
    )


def _capture_log_window(ctx: SimpleNamespace) -> list[LogRecord]:
    return read_records(
        ctx.log_path,
        start_offset=ctx.log_offset_start,
    )


def run_scenario(scenario_or_path) -> dict:
    """Run a scenario module (or path to one) and return the verdict dict.

    The verdict is also written to `scripts/validation/reports/<id>_<ts>.json`.
    """
    scenario_module = _load_module(scenario_or_path)
    ctx = _build_ctx(scenario_module)

    print(f"[validation] start scenario={ctx.scenario_id} "
          f"invariant={ctx.invariant} trace_id={ctx.trace_id}")

    ctx.log_offset_start = log_file_size(ctx.log_path)
    t_setup_start = time.monotonic()

    setup = getattr(scenario_module, "setup", None)
    if setup is not None:
        setup(ctx)
    t_run_start = time.monotonic()
    scenario_module.run(ctx)
    t_run_end = time.monotonic()

    # Give the rotating handler a chance to flush.
    time.sleep(0.2)
    ctx.log_offset_end = log_file_size(ctx.log_path)
    ctx.records = _capture_log_window(ctx)

    raw_assertions = list(scenario_module.assertions(ctx))
    summary: AssertionSummary = summarize(raw_assertions)

    duration_ms = int((time.monotonic() - t_setup_start) * 1000)
    run_ms = int((t_run_end - t_run_start) * 1000)

    verdict = {
        "scenario": ctx.scenario_id,
        "invariant": ctx.invariant,
        "trace_id": ctx.trace_id,
        "started_at": ctx.t0.isoformat(),
        "duration_ms": duration_ms,
        "run_ms": run_ms,
        "log_path": str(ctx.log_path),
        "log_window_bytes": ctx.log_offset_end - ctx.log_offset_start,
        "log_records_captured": len(ctx.records),
        "assertions": [
            {"name": name, "ok": ok, "message": msg}
            for (name, ok, msg) in raw_assertions
        ],
        "verdict": summary.verdict,
        "passed": summary.passed,
        "failed": summary.failed,
        "total": summary.total,
    }

    report_path = _ensure_reports_dir() / _report_filename(ctx)
    report_path.write_text(json.dumps(verdict, indent=2), encoding="utf-8")
    print(f"[validation] verdict={verdict['verdict']} "
          f"passed={summary.passed}/{summary.total} "
          f"duration_ms={duration_ms} report={report_path}")
    for name, ok, msg in raw_assertions:
        marker = "PASS" if ok else "FAIL"
        print(f"  [{marker}] {name}: {msg}")
    return verdict


def run_regression_suite() -> dict:
    """Run every scenario under scenarios/regression/ and aggregate."""
    if not _REGRESSION_DIR.is_dir():
        print(f"[validation] no regression suite at {_REGRESSION_DIR}")
        return {"verdict": "FAIL", "scenarios": [], "reason": "no_regression_dir"}

    scenarios = sorted(p for p in _REGRESSION_DIR.glob("*.py")
                       if not p.name.startswith("_"))
    verdicts = []
    n_pass = 0
    n_fail = 0
    for path in scenarios:
        try:
            verdict = run_scenario(path)
        except Exception as exc:  # noqa: BLE001 - scenario errors are FAIL
            verdict = {
                "scenario": path.stem, "verdict": "FAIL",
                "error": repr(exc),
            }
            print(f"[validation] ERROR in {path.name}: {exc!r}")
        verdicts.append(verdict)
        if verdict.get("verdict") == "PASS":
            n_pass += 1
        else:
            n_fail += 1

    suite_verdict = "PASS" if n_fail == 0 and n_pass > 0 else "FAIL"
    summary_path = _ensure_reports_dir() / f"regression_{_ts()}.json"
    summary = {
        "verdict": suite_verdict,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "scenarios": verdicts,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[validation] regression suite verdict={suite_verdict} "
          f"pass={n_pass} fail={n_fail} report={summary_path}")
    return summary


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _report_filename(ctx: SimpleNamespace) -> str:
    return f"{ctx.scenario_id}_{_ts()}.json"


def _load_module(scenario_or_path):
    """Load a scenario from a module object, path, or __file__ string."""
    if hasattr(scenario_or_path, "run"):
        return scenario_or_path
    path = Path(str(scenario_or_path))
    if not path.exists():
        raise FileNotFoundError(f"scenario not found: {path}")
    import importlib.util
    spec = importlib.util.spec_from_file_location(path.stem, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load scenario from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "run"):
        raise AttributeError(f"scenario {path.name} has no run(ctx)")
    return module


__all__ = ["run_scenario", "run_regression_suite"]
