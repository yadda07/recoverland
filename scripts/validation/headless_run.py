"""Headless runner for the setup/run/assertions scenarios (Windows + Linux).

`ci_run.py` drives the two scenarios that expose the ``run() -> dict``
contract. Everything under ``scenarios/`` uses the other contract
(``setup(ctx) / run(ctx) / assertions(ctx)`` consumed by ``runner.py``) and
had no headless entry point at all: the scenarios could only be run one by
one from the QGIS Python console, which is why several of them rotted
unnoticed. This module runs any number of them in one go and exits non-zero
if a single assertion fails.

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


def _collect(argv) -> list:
    if not argv:
        return sorted(p for p in _SCENARIOS_DIR.glob("*.py")
                      if not p.name.startswith("_"))
    out = []
    for target in argv:
        path = Path(target)
        if not path.is_absolute():
            path = (Path.cwd() / target).resolve()
        if path.is_dir():
            out.extend(sorted(p for p in path.glob("*.py")
                              if not p.name.startswith("_")))
        else:
            out.append(path)
    return out


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

    from scripts.validation.runner import run_scenario

    scenarios = _collect(argv)
    if not scenarios:
        print("[headless] no scenario found", file=sys.stderr)
        return 2

    failures = []
    try:
        for path in scenarios:
            try:
                verdict = run_scenario(str(path))
            except Exception as exc:  # noqa: BLE001 - a broken scenario is a failure
                import traceback
                traceback.print_exc()
                failures.append((path.stem, repr(exc)))
                continue
            if verdict.get("verdict") != "PASS":
                failures.append((path.stem, verdict.get("failed")))
    finally:
        try:
            app.exitQgis()
        except Exception:  # noqa: BLE001
            pass

    print()
    print(f"[headless] {len(scenarios) - len(failures)}/{len(scenarios)} scenario(s) PASS")
    if failures:
        print("[headless] FAILURES:")
        for name, detail in failures:
            print(f"  - {name}: {detail}")
        return 1
    print("[headless] ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
