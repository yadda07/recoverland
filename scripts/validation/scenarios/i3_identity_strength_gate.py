"""i3_identity_strength_gate — RecoverLand validation runtime.

Invariant: I-3 (KPI K-3 covers identity strength matrix; no silent
fallback when a provider has weak or missing identity).
Backlog item: BL-RW-P0-04.
Root cause: CR-1.
Charter signature expected:
    EditSessionTracker.connect_layer: layer="X" provider=Y driver=Z
    identity_strength=W support_level=V action=accepted|warned|refused

Pre-patch state: `EditSessionTracker.connect_layer` only emits
`EditSessionTracker: connected {name} [{id}]` (lapidary, no strength,
no warning). The user has no way to learn that a memory layer
(strength=NONE) or a shapefile (strength=MEDIUM) will limit rewind
reliability.

Scenario (brutal, two-extremes coverage):
    1. Build TWO QgsVectorLayers covering the matrix extremes:
        - a `memory` provider layer (identity_strength=NONE)
        - a real GPKG file via OGR (identity_strength=STRONG)
    2. Wire a fresh `EditSessionTracker` and call connect_layer on each.
    3. For each layer, assert the OBSERVABLE outcome:
        - the strength returned by `evaluate_layer_support` matches
        - the structured log line is present with the right fields
        - the tracker._connected_layers state is True or False as
          dictated by the strength gate
    4. Source-level guards: connect_layer emits identity_strength,
       support_level, and action; reads policy.identity_strength.

Pre-patch verdict: FAIL (memory was silently captured).
Post-patch verdict: PASS (memory refused, GPKG accepted with full
structured log on both paths).
"""
from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

SCENARIO_ID = "i3_identity_strength_gate"
INVARIANT = "I-3"
EXPECTED_SIGNATURE = (
    r"EditSessionTracker\.connect_layer:\s+layer=.*"
    r"provider=\S+\s+driver=\S+\s+identity_strength=\S+\s+"
    r"support_level=\S+\s+action=(accepted|accepted_untested|warned|refused)"
)

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_memory_layer():
    """Create a memory QgsVectorLayer suitable for the test."""
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=name:string", "i3_test_memory", "memory")
    if not layer.isValid():
        raise RuntimeError("memory layer creation failed")
    return layer


def _make_gpkg_fixture() -> tuple[str, str]:
    """Create a real GPKG file on disk via OGR. Returns (path, tmpdir).

    A GPKG file has a hidden integer FID column managed by SQLite, which
    is what `refine_ogr_identity` keys off to return STRONG.
    """
    from osgeo import ogr

    tmpdir = tempfile.mkdtemp(prefix="rl_i3_gpkg_")
    path = os.path.join(tmpdir, "test.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("GPKG driver not available in OGR runtime")
    ds = driver.CreateDataSource(path)
    if ds is None:
        raise RuntimeError(f"OGR CreateDataSource returned None for {path}")
    lyr = ds.CreateLayer("points", geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("name", "f1")
    geom = ogr.Geometry(ogr.wkbPoint)
    geom.AddPoint(0.0, 0.0)
    feat.SetGeometry(geom)
    lyr.CreateFeature(feat)
    ds.FlushCache()
    ds = None  # close
    return path, tmpdir


def _make_gpkg_layer(gpkg_path: str):
    from qgis.core import QgsVectorLayer
    uri = f"{gpkg_path}|layername=points"
    layer = QgsVectorLayer(uri, "i3_test_gpkg", "ogr")
    if not layer.isValid():
        raise RuntimeError(
            f"GPKG layer invalid for uri={uri!r} "
            f"(provider error={layer.dataProvider().error().message() if layer.dataProvider() else 'no provider'})")
    return layer


def _make_tracker():
    """Build a fresh EditSessionTracker with no journal side effects.

    The tracker only needs an inert write_queue/journal_manager pair
    for `connect_layer`. We pass dummies that raise on use, since the
    scenario does not write events.
    """
    from recoverland.core.edit_tracker import EditSessionTracker

    class _DummyWQ:
        def add(self, *args, **kwargs):
            raise AssertionError("write_queue.add unexpected in i3 scenario")

    class _DummyJM:
        def get_active_session_id(self):
            return None

    return EditSessionTracker(_DummyWQ(), _DummyJM())


def setup(ctx):
    from recoverland.core.logger import flog

    mem_layer = _make_memory_layer()
    gpkg_path, gpkg_tmpdir = _make_gpkg_fixture()
    gpkg_layer = _make_gpkg_layer(gpkg_path)
    tracker = _make_tracker()
    tracker.activate()

    ctx.data["mem_layer"] = mem_layer
    ctx.data["gpkg_layer"] = gpkg_layer
    ctx.data["gpkg_path"] = gpkg_path
    ctx.data["gpkg_tmpdir"] = gpkg_tmpdir
    ctx.data["tracker"] = tracker

    flog(
        "i3_identity_strength_gate setup: trace_id={tid} "
        "mem_layer={mn} mem_id={mi} mem_provider={mp} "
        "gpkg_layer={gn} gpkg_id={gi} gpkg_provider={gp} gpkg_path={gpath}"
        .format(
            tid=ctx.trace_id,
            mn=mem_layer.name(), mi=mem_layer.id(),
            mp=mem_layer.dataProvider().name(),
            gn=gpkg_layer.name(), gi=gpkg_layer.id(),
            gp=gpkg_layer.dataProvider().name(), gpath=gpkg_path),
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.support_policy import evaluate_layer_support

    mem_layer = ctx.data["mem_layer"]
    gpkg_layer = ctx.data["gpkg_layer"]
    tracker = ctx.data["tracker"]

    flog(
        "i3_identity_strength_gate run start: trace_id={tid}".format(
            tid=ctx.trace_id),
        "INFO",
    )

    # ---- memory layer (NONE, must be refused) ----
    mem_policy = evaluate_layer_support(mem_layer)
    ctx.data["mem_strength"] = mem_policy.identity_strength.value
    ctx.data["mem_support"] = mem_policy.support_level.value
    ctx.data["mem_capture"] = mem_policy.capture
    tracker.connect_layer(mem_layer)
    ctx.data["mem_connected"] = mem_layer.id() in tracker._connected_layers

    # ---- gpkg layer (STRONG, must be accepted) ----
    gpkg_policy = evaluate_layer_support(gpkg_layer)
    ctx.data["gpkg_strength"] = gpkg_policy.identity_strength.value
    ctx.data["gpkg_support"] = gpkg_policy.support_level.value
    ctx.data["gpkg_capture"] = gpkg_policy.capture
    tracker.connect_layer(gpkg_layer)
    ctx.data["gpkg_connected"] = gpkg_layer.id() in tracker._connected_layers

    flog(
        "i3_identity_strength_gate run end: trace_id={tid} "
        "mem_strength={ms} mem_connected={mc} "
        "gpkg_strength={gs} gpkg_connected={gc}".format(
            tid=ctx.trace_id,
            ms=ctx.data["mem_strength"], mc=ctx.data["mem_connected"],
            gs=ctx.data["gpkg_strength"], gc=ctx.data["gpkg_connected"]),
        "INFO",
    )


_SOURCE_PATTERNS = {
    "connect_layer_emits_strength": (
        Path("core/edit_tracker.py"),
        re.compile(
            r"def\s+connect_layer\b[\s\S]*?identity_strength=",
            re.MULTILINE),
    ),
    "connect_layer_emits_action": (
        Path("core/edit_tracker.py"),
        re.compile(
            r"def\s+connect_layer\b[\s\S]*?action=",
            re.MULTILINE),
    ),
    "connect_layer_reads_policy_strength": (
        Path("core/edit_tracker.py"),
        re.compile(
            r"def\s+connect_layer\b[\s\S]*?policy\.identity_strength",
            re.MULTILINE),
    ),
}


def _check_source_pattern(symbol: str) -> tuple[bool, str]:
    rel, regex = _SOURCE_PATTERNS[symbol]
    full = _PLUGIN_ROOT / rel
    if not full.is_file():
        return False, f"missing file: {rel}"
    text = full.read_text(encoding="utf-8", errors="replace")
    if regex.search(text):
        return True, f"{symbol} pattern present in {rel}"
    return False, f"{symbol} pattern absent in {rel}"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    # ---- memory layer (NONE) -------------------------------------------
    out.append((
        "mem_strength_is_none",
        ctx.data.get("mem_strength") == "none",
        f"mem_strength={ctx.data.get('mem_strength')} expected=none",
    ))
    out.append((
        "mem_layer_not_connected_when_NONE",
        ctx.data.get("mem_connected") is False,
        f"mem_connected={ctx.data.get('mem_connected')} "
        f"expected=False (NONE strength must refuse capture)",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker\.connect_layer:.*provider=memory.*"
        r"identity_strength=none.*action=refused",
        name="mem_refused_log_present",
        min_count=1,
    ))

    # ---- gpkg layer (STRONG) -------------------------------------------
    out.append((
        "gpkg_strength_is_strong",
        ctx.data.get("gpkg_strength") == "strong",
        f"gpkg_strength={ctx.data.get('gpkg_strength')} expected=strong",
    ))
    out.append((
        "gpkg_layer_connected_when_STRONG",
        ctx.data.get("gpkg_connected") is True,
        f"gpkg_connected={ctx.data.get('gpkg_connected')} "
        f"expected=True (STRONG strength must accept capture)",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"EditSessionTracker\.connect_layer:.*provider=ogr.*"
        r"identity_strength=strong.*action=accepted(?!_untested)",
        name="gpkg_accepted_log_present",
        min_count=1,
    ))

    # ---- generic structured signature must appear at least twice -------
    out.append(assert_log_contains(
        ctx.records,
        EXPECTED_SIGNATURE,
        name="connect_layer_structured_log_present",
        min_count=2,
    ))

    # ---- source guards --------------------------------------------------
    for symbol in _SOURCE_PATTERNS:
        ok, msg = _check_source_pattern(symbol)
        out.append((f"source__{symbol}", ok, msg))

    # ---- trace propagation ---------------------------------------------
    out.append(assert_log_contains(
        ctx.records,
        rf"i3_identity_strength_gate.*trace_id={ctx.trace_id}",
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
