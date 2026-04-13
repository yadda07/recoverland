"""Root conftest: ensures qgis mocks are loaded before package __init__.py.

Pytest loads this conftest BEFORE collecting tests and importing the
recoverland package. The mock setup must happen here so that
recoverland/__init__.py (which imports qgis) finds the mocks in place.
The full fixtures remain in tests/conftest.py which is loaded afterwards.
"""
import sys
import os

# Only set up mocks if running outside QGIS
if 'qgis' not in sys.modules:
    # Ensure tests/conftest.py is importable
    _here = os.path.dirname(os.path.abspath(__file__))
    _parent = os.path.dirname(_here)
    if _parent not in sys.path:
        sys.path.insert(0, _parent)

    # Force-load the test conftest which sets up all qgis mocks
    # at module level (inside its own 'if qgis not in sys.modules' guard)
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        "tests.conftest",
        os.path.join(_here, "tests", "conftest.py"),
    )
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
