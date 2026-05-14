"""Root conftest for RecoverLand.

Decision D-2026-05-14-01 (see docs/rewind_charter.md §11): RecoverLand
validation is performed via runtime scenarios under
`scripts/validation/scenarios/`, not via pytest with mocked QGIS.

This file is kept as a defensive no-op so that:

1. The `tests/` directory (currently empty) can stay deleted without
   breaking any tooling that still invokes pytest accidentally.
2. If the historical pytest suite is restored via
   `git restore --source=bea5792^ -- tests/`, the bootstrap is
   automatically reinstated.

If `tests/conftest.py` exists, we load it (legacy behaviour: it sets
up QGIS mocks before the package is imported). Otherwise we exit
quietly so pytest collection does not crash.
"""
import os
import sys


def _try_load_legacy_test_bootstrap() -> None:
    if 'qgis' in sys.modules:
        return  # running inside QGIS, no mock setup needed
    here = os.path.dirname(os.path.abspath(__file__))
    legacy = os.path.join(here, "tests", "conftest.py")
    if not os.path.isfile(legacy):
        return  # no historical tests/ tree, no-op
    parent = os.path.dirname(here)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    import importlib.util
    spec = importlib.util.spec_from_file_location("tests.conftest", legacy)
    if spec is None or spec.loader is None:
        return
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)


_try_load_legacy_test_bootstrap()
