"""Headless CI runner for RecoverLand runtime validation scenarios (RL-E3-02).

Initialises a headless QgsApplication, runs each runtime scenario under
``scripts/validation/scenarios/`` and exits non-zero if ANY scenario does not
return ``verdict == "PASS"``. This is the CI counterpart of running
``scenario.run()`` in the QGIS Python console (charte AGENTS.md §4: preuve =
logs runtime QGIS, pas de mocks pytest).

Usage (inside a QGIS environment / docker image)::

    xvfb-run -a python3 -m recoverland.scripts.validation.ci_run

The plugin package must be importable as ``recoverland`` (put the parent of the
plugin directory on PYTHONPATH and check it out into a ``recoverland`` folder).
"""
from __future__ import annotations

import importlib
import sys
import traceback

# Scenarios to run in CI. Each module must expose ``run() -> dict`` with a
# ``verdict`` key ("PASS"/"FAIL"). Add new scenarios here as they land.
_SCENARIOS = (
    "recoverland.scripts.validation.scenarios.rv_snapshot_asof",
    "recoverland.scripts.validation.scenarios.rv_snapshot_volume",
)


def _init_qgis():
    """Initialise a headless QgsApplication. Returns the app instance."""
    from qgis.core import QgsApplication

    # Prefix path: honour QGIS_PREFIX_PATH when set (docker images do), else
    # fall back to the conventional /usr install.
    import os
    prefix = os.environ.get("QGIS_PREFIX_PATH", "/usr")
    QgsApplication.setPrefixPath(prefix, True)
    app = QgsApplication([], False)
    app.initQgis()
    return app


def main() -> int:
    try:
        app = _init_qgis()
    except Exception:  # noqa: BLE001
        sys.stderr.write("FATAL: could not initialise QGIS headless\n")
        traceback.print_exc()
        return 2

    failures = []
    try:
        for name in _SCENARIOS:
            print(f"=== CI scenario: {name} ===", flush=True)
            try:
                mod = importlib.import_module(name)
                result = mod.run()
            except Exception:  # noqa: BLE001
                traceback.print_exc()
                failures.append((name, "EXCEPTION"))
                continue
            verdict = (result or {}).get("verdict")
            if verdict != "PASS":
                failures.append((name, (result or {}).get("failed", verdict)))
    finally:
        try:
            app.exitQgis()
        except Exception:  # noqa: BLE001
            pass

    if failures:
        sys.stderr.write("\nCI VALIDATION FAILED:\n")
        for name, detail in failures:
            sys.stderr.write(f"  - {name}: {detail}\n")
        return 1

    print("\nCI VALIDATION PASSED: all scenarios green.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
