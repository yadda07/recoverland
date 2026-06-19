"""Template scenario for RecoverLand runtime validation.

Copy this file, rename it `<id>_<short_name>.py`, fill the four
hooks. Run it from the QGIS Python console:

    from pathlib import Path
    SCRIPTS = Path(r'C:\\Users\\<user>\\AppData\\Roaming\\QGIS\\QGIS4'
                   r'\\profiles\\default\\python\\plugins\\recoverland\\scripts')
    import sys
    sys.path.insert(0, str(SCRIPTS.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(str(SCRIPTS / 'validation/scenarios/<your>.py'))

Contract:
    SCENARIO_ID: str           snake_case unique id, used in the report filename
    INVARIANT: str             "I-N" or "BL-RW-PX-NN"
    setup(ctx):                idempotent state preparation
    run(ctx):                  the action under test
    assertions(ctx) -> list:   list of (name, ok, message) tuples
"""
from __future__ import annotations

# Always declared, never None:
SCENARIO_ID = "template"
INVARIANT = ""  # e.g. "I-9" or "BL-RW-P0-01"

# Expected log signature documented for grep-ability:
# example: r"BUF_DEL.*status=APPLIED"
EXPECTED_SIGNATURE = r""


def setup(ctx):
    """Prepare the initial state. Must be idempotent across runs.

    Use ctx.data dict to stash any value needed by run/assertions.
    Example:
        from qgis.core import QgsProject
        layer = QgsProject.instance().mapLayersByName("zone_mkt_rip")[0]
        ctx.data["layer"] = layer
        ctx.data["initial_count"] = layer.featureCount()
    """
    return


def run(ctx):
    """Perform the action under test. Must finish in < 60s.

    Emit at least one log line that includes ctx.trace_id so the
    assertions can locate the relevant log window precisely.
    Example:
        from recoverland.core.logger import flog
        flog(f"scenario_run trace_id={ctx.trace_id} scenario={ctx.scenario_id}")
        # ... action under test ...
    """
    return


def assertions(ctx):
    """Return a list of (name, ok, message) tuples.

    Use helpers from scripts.validation.assert_log to build them.
    Example:
        from scripts.validation.assert_log import assert_log_contains
        return [
            assert_log_contains(ctx.records, r"scenario_run trace_id=" + ctx.trace_id,
                                name="trace_id_emitted"),
            # ... other assertions ...
        ]
    """
    return []


if __name__ == "__main__":
    # Convenience: allow running directly from QGIS console via
    # exec(compile(open(__file__).read(), __file__, 'exec'))
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
