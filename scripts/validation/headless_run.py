"""Headless runner for the setup/run/assertions scenarios (Windows + Linux).

`ci_run.py` drives the two scenarios that expose the ``run() -> dict``
contract. Everything under ``scenarios/`` uses the other contract
(``setup(ctx) / run(ctx) / assertions(ctx)`` consumed by ``runner.py``) and
had no headless entry point at all: the scenarios could only be run one by
one from the QGIS Python console, which is why several of them rotted
unnoticed. This module runs any number of them in one go.

Exit codes, kept distinct on purpose
------------------------------------
0  every scenario this runner can drive passed
1  at least one scenario RAN and failed (assertion) or crashed (error)
2  nothing was run -- unknown scenario name, or no scenario found

Conflating 2 with 1 is what makes a test harness untrustworthy: a typo in an
argument, or a contract this runner cannot drive, then reads exactly like a
regression in the code under test. Scenarios on the ``run() -> dict``
contract are reported as NOT RUN -- never as PASS, since unproven is not
proven, and never as FAIL, since they were never executed.

Why the QGIS_CUSTOM_CONFIG_PATH dance
-------------------------------------
`core.logger` resolves its log file through ``QgsApplication.qgisSettingsDirPath()``
at IMPORT time, while `runner.py` reads its log window from the conventional
profile location. Without an initialised QgsApplication the logger falls
back to the plugin directory, the two paths diverge, and every log-based
assertion fails with ``n=0`` even though the code under test behaved
perfectly. So: set the config path, build QgsApplication, and only THEN
import anything from the plugin.

QGIS appends ``profiles/<name>`` to QGIS_CUSTOM_CONFIG_PATH itself, so the
variable must point at the config ROOT (``.../QGIS/QGIS4``), not at the
profile directory.

Usage (from the plugin root, with a QGIS python)::

    python-qgis.bat scripts/validation/headless_run.py                    # all
    python-qgis.bat scripts/validation/headless_run.py scenarios/rw_*.py  # subset

On Linux/CI::

    xvfb-run -a python3 scripts/validation/headless_run.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PLUGIN_ROOT = _HERE.parents[1]
_SCENARIOS_DIR = _HERE / "scenarios"


def _resolve_config_root() -> str:
    """Config ROOT holding ``profiles/<name>`` — the plugin lives under it.

    Layout: <config_root>/profiles/<profile>/python/plugins/<plugin>.
    Walk up rather than count levels, so a non-standard checkout that is
    simply not under a profile yields "" instead of a wrong path (QGIS then
    keeps its own default and only the log-based assertions degrade).
    """
    env = os.environ.get("QGIS_CUSTOM_CONFIG_PATH")
    if env:
        return env
    for parent in _PLUGIN_ROOT.parents:
        if parent.name == "profiles" and parent.parent != parent:
            return str(parent.parent)
    return ""


def _collect(argv) -> tuple:
    """Resolve CLI targets to scenario files.

    Returns ``(paths, unresolved)``. Keeping the two apart matters: an
    unresolved target means the suite did NOT run, which is a different
    verdict from a scenario that ran and failed. Folding the first into the
    second -- as this function used to, by appending a non-existent path and
    letting run_scenario raise FileNotFoundError -- makes a typo in an
    argument look exactly like a regression in the code under test.

    A bare scenario id (``rw_apply_perf``) resolves against scenarios/, which
    is how scenarios are named everywhere else (SCENARIO_ID, reports, CI
    lists); requiring ``scenarios/rw_apply_perf.py`` from the plugin root was
    a trap, since the same word works in the QGIS console.
    """
    if not argv:
        return (sorted(p for p in _SCENARIOS_DIR.glob("*.py")
                       if not p.name.startswith("_")), [])
    out = []
    unresolved = []
    for target in argv:
        candidates = []
        raw = Path(target)
        if raw.is_absolute():
            candidates.append(raw)
        else:
            candidates.append(Path.cwd() / target)
            candidates.append(_PLUGIN_ROOT / target)
            if not target.endswith(".py"):
                candidates.append(_SCENARIOS_DIR / f"{target}.py")
                candidates.append(_SCENARIOS_DIR / target)
        for cand in candidates:
            if cand.is_dir():
                out.extend(sorted(p for p in cand.glob("*.py")
                                  if not p.name.startswith("_")))
                break
            if cand.is_file():
                out.append(cand.resolve())
                break
        else:
            unresolved.append(target)
    return (out, unresolved)


def _contract_mismatch(module) -> str:
    """Return why ``module`` is not this runner's contract, or "" if it is.

    Two scenario contracts coexist under scenarios/: ``setup(ctx)/run(ctx)/
    assertions(ctx)`` driven by runner.py, and ``run() -> dict`` driven by
    ci_run.py. This runner can only drive the first. Detecting the second
    BEFORE running it is what keeps the report honest: those scenarios used
    to be executed anyway, raise ``TypeError: run() takes 0 positional
    arguments``, and land in the FAILURES list -- which inflated 5 real
    failures into 11 and made every reader believe six subsystems were
    broken when in truth they had never been run here at all.

    Reported as "not run", never as PASS: a scenario this runner cannot drive
    is unproven, and claiming otherwise would be the same lie in the other
    direction.
    """
    import inspect

    run = getattr(module, "run", None)
    if not callable(run):
        return "no run() at all"
    if not callable(getattr(module, "assertions", None)):
        return "no assertions(ctx) -- ci_run.py contract"
    try:
        params = [
            p for p in inspect.signature(run).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    except (TypeError, ValueError):  # builtins / C callables: let it run
        return ""
    if not params:
        return "run() takes no ctx -- ci_run.py contract"
    return ""


def main(argv) -> int:
    config_root = _resolve_config_root()
    if config_root:
        os.environ["QGIS_CUSTOM_CONFIG_PATH"] = config_root

    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)

    from qgis.core import QgsApplication

    prefix = os.environ.get("QGIS_PREFIX_PATH")
    if prefix:
        QgsApplication.setPrefixPath(prefix, True)
    app = QgsApplication([], False)
    app.initQgis()
    print(f"[headless] settings_dir={QgsApplication.qgisSettingsDirPath()}")

    from scripts.validation.runner import _load_module, run_scenario

    scenarios, unresolved = _collect(argv)
    if unresolved:
        print(f"[headless] unknown scenario(s): {', '.join(unresolved)}",
              file=sys.stderr)
        print(f"[headless] looked under {_SCENARIOS_DIR}", file=sys.stderr)
        print("[headless] nothing was run -- exit 2 (could not run), "
              "not exit 1 (ran and failed)", file=sys.stderr)
        return 2
    if not scenarios:
        print("[headless] no scenario found", file=sys.stderr)
        return 2

    failures = []
    errors = []
    skipped = []
    try:
        for path in scenarios:
            try:
                module = _load_module(str(path))
            except AttributeError as exc:
                # A THIRD shape exists: a top-level script that runs its own
                # checks at import and prints its own verdict (i18n_runtime_qgis
                # does, 36/36, driven from the QGIS console by exec(compile(..))).
                # runner._load_module rejects it for having no run(); that is not
                # a defect, so do not call it one. Its own output stands in the
                # log next to ours.
                if "has no run" in str(exc):
                    skipped.append((path.stem,
                                    "self-reporting script -- prints its own "
                                    "verdict at import, see [i18n-runtime]-style "
                                    "lines above"))
                    continue
                import traceback
                traceback.print_exc()
                errors.append((path.stem, f"load failed: {exc!r}"))
                continue
            except Exception as exc:  # noqa: BLE001 - unloadable is a real defect
                import traceback
                traceback.print_exc()
                errors.append((path.stem, f"load failed: {exc!r}"))
                continue
            reason = _contract_mismatch(module)
            if reason:
                skipped.append((path.stem, reason))
                continue
            try:
                verdict = run_scenario(module)
            except Exception as exc:  # noqa: BLE001 - a broken scenario is a defect
                import traceback
                traceback.print_exc()
                errors.append((path.stem, repr(exc)))
                continue
            if verdict.get("verdict") != "PASS":
                failures.append((path.stem, verdict.get("failed")))
    finally:
        try:
            app.exitQgis()
        except Exception:  # noqa: BLE001
            pass

    n_ran = len(scenarios) - len(skipped)
    n_pass = n_ran - len(failures) - len(errors)

    print()
    print(f"[headless] {n_pass}/{n_ran} scenario(s) PASS")
    if skipped:
        print(f"[headless] {len(skipped)} NOT RUN by this runner "
              f"(other contract -- see ci_run.py), so neither PASS nor FAIL:")
        for name, detail in skipped:
            print(f"  ~ {name}: {detail}")
    if errors:
        print("[headless] ERRORS (ran and crashed):")
        for name, detail in errors:
            print(f"  ! {name}: {detail}")
    if failures:
        print("[headless] FAILURES (ran, assertions failed):")
        for name, detail in failures:
            print(f"  - {name}: {detail} assertion(s)")
    if failures or errors:
        return 1
    print("[headless] ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
