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
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from .assert_log import AssertionSummary, summarize
from .parse_log import LogRecord, log_file_size, read_records

_HERE = Path(__file__).resolve().parent

# Scenarios import their helpers as ``scripts.validation.assert_log``, which
# needs the PLUGIN ROOT on sys.path. headless_run.py puts it there; the QGIS
# Python console does not, so calling run_scenario() from the console failed
# with ModuleNotFoundError halfway through -- after setup() and run() had
# already written to disk. Doing it here makes the runner work from any entry
# point, which is the point of a test harness.
_PLUGIN_ROOT = _HERE.parents[1]
for _extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)


def _alias_scripts_package() -> None:
    """Bind ``scripts.validation`` to this very package in sys.modules.

    Extending sys.path is not enough inside a live QGIS: something else has
    already imported a top-level ``scripts`` (QGIS ships one), so the name is
    bound in sys.modules and Python never re-resolves it -- the path entry is
    simply ignored and the scenario dies on ModuleNotFoundError, after setup()
    and run() have already written to disk.

    Aliasing is safe both ways round: when ``scripts.validation`` already
    resolves to us, nothing is touched.
    """
    import importlib

    try:
        mod = importlib.import_module("scripts.validation")
        if Path(getattr(mod, "__file__", "") or "").resolve().parent == _HERE:
            return
    except Exception:  # noqa: BLE001 - absent or foreign, both handled below
        pass
    try:
        pkg = importlib.import_module(__package__ or "recoverland.scripts.validation")
        parent_name = (__package__ or "recoverland.scripts.validation").rsplit(".", 1)[0]
        sys.modules["scripts"] = importlib.import_module(parent_name)
        sys.modules["scripts.validation"] = pkg
    except Exception as exc:  # noqa: BLE001 - never break a run over this
        print(f"[validation] could not alias scripts.validation: {exc!r}")


_alias_scripts_package()
_REPORTS_DIR = _HERE / "reports"
_REGRESSION_DIR = _HERE / "scenarios" / "regression"


def _resolve_log_path() -> Path:
    """Return the RecoverLand debug log path.

    Order matters, and the first entry is the one that makes log-based
    assertions work anywhere:

    1. ``core.logger._LOG_FILE`` — the path the logger ACTUALLY opened, once
       and for all, at import time. Asking the logger removes any chance of
       the runner and the logger disagreeing, which is exactly what happened
       on a headless run: the logger resolved the profile through
       QgsApplication while the runner rebuilt a hard-coded Windows path, so
       every ``assert_log_contains`` came back with n=0 on code that had
       behaved perfectly. The same mismatch would make the suite unrunnable
       on Linux/CI, where that hard-coded path does not exist at all.
    2. the plugin's public API, when the plugin is loaded in a live QGIS.
    3. the conventional profile location, last resort for offline tooling.
    """
    try:
        from recoverland.core.logger import _LOG_FILE  # type: ignore
        if _LOG_FILE:
            return Path(_LOG_FILE)
    except Exception:
        pass
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
