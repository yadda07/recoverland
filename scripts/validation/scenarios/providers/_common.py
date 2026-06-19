"""Common helpers for the BL-RW-P2-10 provider matrix scenarios.

Each scenario under `scripts/validation/scenarios/providers/` exercises
the capture and rewind cycle on a real `QgsVectorLayer` backed by a
specific provider (memory / OGR-GPKG / OGR-Shapefile / ...). The DB
providers (postgres / mssql / oracle) are intentionally out of scope
for the first iteration; they require saved connections and are
tracked under `BL-RW-P2-10-DB-FOLLOWUP`.

This module exposes:
  * `make_gpkg_layer(tmpdir)`        : OGR-GPKG fixture
  * `make_shp_layer(tmpdir)`         : OGR-Shapefile fixture
  * `make_memory_layer()`            : in-memory QgsVectorLayer fixture
  * `add_point_feature(layer, name)` : scripted INSERT through Qt
  * `make_temp_dir(prefix)`          : tempfile.mkdtemp wrapper
  * `cleanup_temp_dir(path)`         : shutil.rmtree wrapper
  * `run_capture_only_cycle(layer, provider_label, driver_label, ctx)`
        capture path only: install tracker on a tempfile SQLite journal,
        commit an INSERT through Qt, count audit events. Returns a dict
        with `provider`, `driver`, `score`, `event_count`,
        `connect_refused`, `error`. score=100 when the invariant for
        the provider class is fulfilled; score=0 otherwise. Caller is
        expected to emit the `validate_rewind` log signature themselves
        so that the message stays grouped with the scenario trace_id.

Refusing capture on `memory` is the EXPECTED behaviour (memory
provider has identity_strength=NONE per support_policy). The helper
exposes that branch via `expect_refusal=True` so the memory scenario
can score 100 when the tracker refuses the layer.
"""
from __future__ import annotations

import os
import shutil
import tempfile
import time
import uuid
from typing import Optional


def make_temp_dir(prefix: str) -> str:
    return tempfile.mkdtemp(prefix=prefix)


def cleanup_temp_dir(path: Optional[str]) -> None:
    if not path:
        return
    try:
        shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass


def make_gpkg_layer(tmpdir: str, layer_name: str = "points"):
    """Create a GPKG via OGR + load it as a QgsVectorLayer."""
    from osgeo import ogr
    from qgis.core import QgsVectorLayer

    path = os.path.join(tmpdir, "test.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("GPKG driver not available")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer(layer_name, geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    ds.FlushCache()
    ds = None
    uri = f"{path}|layername={layer_name}"
    layer = QgsVectorLayer(uri, f"p10_gpkg_{layer_name}", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"GPKG layer invalid for uri={uri!r}")
    return layer, path


def make_shp_layer(tmpdir: str, layer_name: str = "points"):
    """Create a Shapefile via OGR + load it as a QgsVectorLayer."""
    from osgeo import ogr
    from qgis.core import QgsVectorLayer

    path = os.path.join(tmpdir, f"{layer_name}.shp")
    driver = ogr.GetDriverByName("ESRI Shapefile")
    if driver is None:
        raise RuntimeError("ESRI Shapefile driver not available")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer(layer_name, geom_type=ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    ds.FlushCache()
    ds = None
    layer = QgsVectorLayer(path, f"p10_shp_{layer_name}", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"Shapefile layer invalid for path={path!r}")
    return layer, path


def make_memory_layer(layer_name: str = "points"):
    """Create an in-memory QgsVectorLayer for the refused-capture test."""
    from qgis.core import QgsVectorLayer

    uri = "Point?crs=EPSG:4326&field=name:string(10)"
    layer = QgsVectorLayer(uri, f"p10_mem_{layer_name}", "memory")
    if not layer.isValid():
        raise RuntimeError("memory layer invalid")
    return layer


def add_point_feature(layer, name_value: str) -> bool:
    """Start editing, add a single point feature at (0,0), commit.

    Returns True on full success (commitChanges True), False otherwise.
    """
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    if not layer.startEditing():
        return False
    feat = QgsFeature(layer.fields())
    idx = layer.fields().indexFromName("name")
    if idx >= 0:
        feat.setAttribute(idx, name_value)
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(0.0, 0.0)))
    if not layer.addFeature(feat):
        layer.rollBack()
        return False
    if not layer.commitChanges():
        return False
    return True


def open_temp_journal(prefix: str):
    """Open a fresh SQLite journal in a tempdir; returns (jm, wq, tmpdir).

    Public so antithese phases inside the provider scenarios can drive a
    fresh journal independently of `run_capture_only_cycle`.
    """
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue

    tmpdir = make_temp_dir(prefix)
    journal_path = os.path.join(tmpdir, "audit.db")
    jm = JournalManager()
    jm.open_for_project(project_path=journal_path, profile_path=tmpdir)
    wq = WriteQueue()
    wq.start(jm.path)
    return jm, wq, tmpdir


_open_temp_journal = open_temp_journal  # legacy alias used by run_capture_only_cycle


def close_temp_journal(jm, wq) -> None:
    """Stop write_queue + close journal manager. Idempotent."""
    _close_temp_journal(jm, wq)


def wait_for_events(jm, datasource_fingerprint: str,
                    expected_min: int, timeout_s: float = 5.0):
    """Public alias for `_wait_for_events` so antithese phases can reuse it."""
    return _wait_for_events(jm, datasource_fingerprint, expected_min, timeout_s)


def _close_temp_journal(jm, wq) -> None:
    try:
        if wq is not None:
            wq.stop()
    except Exception:
        pass
    try:
        if jm is not None and jm.is_open:
            jm.close()
    except Exception:
        pass


def _wait_for_events(jm, datasource_fingerprint: str,
                     expected_min: int, timeout_s: float = 5.0):
    """Poll the journal until at least `expected_min` events for the given
    datasource have landed, or until `timeout_s` elapses. Returns the
    list of events found (may be shorter than `expected_min` on timeout).
    """
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events

    deadline = time.monotonic() + timeout_s
    last_events = []
    while time.monotonic() < deadline:
        try:
            conn = jm.create_read_connection()
            try:
                criteria = SearchCriteria(
                    datasource_fingerprint=datasource_fingerprint,
                    layer_name=None,
                    operation_type=None,
                    user_name=None,
                    start_date=None,
                    end_date=None,
                    page=1,
                    page_size=1000,
                )
                result = search_events(conn, criteria)
                last_events = list(getattr(result, "events", []) or [])
            finally:
                conn.close()
        except Exception:
            last_events = []
        if len(last_events) >= expected_min:
            return last_events
        time.sleep(0.05)
    return last_events


def run_capture_only_cycle(layer, provider_label: str,
                           driver_label: str, ctx,
                           expect_refusal: bool = False) -> dict:
    """Install a tracker on a fresh journal, commit one INSERT through
    Qt, count audit events, score the run.

    Returns:
        {
            "provider": str, "driver": str, "score": 0 | 100,
            "event_count": int, "connect_refused": bool, "error": str | None,
        }
    """
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog

    jm = None
    wq = None
    tmpdir = None
    score = 0
    event_count = 0
    connect_refused = False
    error = None

    try:
        jm, wq, tmpdir = _open_temp_journal(f"rl_p10_{provider_label}_")
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()

        before_connected = set(tracker._connected_layers.keys())
        tracker.connect_layer(layer)
        after_connected = set(tracker._connected_layers.keys())
        connect_refused = layer.id() not in after_connected

        if expect_refusal:
            score = 100 if connect_refused else 0
            flog(
                f"p10_provider_cycle: provider={provider_label} "
                f"driver={driver_label} expect_refusal=True "
                f"connect_refused={connect_refused} "
                f"connected_layers_before={sorted(before_connected)} "
                f"connected_layers_after={sorted(after_connected)} "
                f"score={score} trace_id={ctx.trace_id}",
                "INFO",
            )
            return {
                "provider": provider_label, "driver": driver_label,
                "score": score, "event_count": 0,
                "connect_refused": connect_refused, "error": None,
            }

        if connect_refused:
            error = "tracker refused to connect the layer"
            flog(
                f"p10_provider_cycle: provider={provider_label} "
                f"driver={driver_label} connect_refused=True score=0 "
                f"trace_id={ctx.trace_id}",
                "ERROR",
            )
            return {
                "provider": provider_label, "driver": driver_label,
                "score": 0, "event_count": 0,
                "connect_refused": True, "error": error,
            }

        commit_ok = add_point_feature(layer, f"p10_{provider_label}_{uuid.uuid4().hex[:6]}")
        if not commit_ok:
            error = "commitChanges returned False"
            flog(
                f"p10_provider_cycle: provider={provider_label} "
                f"driver={driver_label} commit_ok=False score=0 "
                f"trace_id={ctx.trace_id}",
                "ERROR",
            )
            return {
                "provider": provider_label, "driver": driver_label,
                "score": 0, "event_count": 0,
                "connect_refused": False, "error": error,
            }

        fp = compute_datasource_fingerprint(layer)
        events = _wait_for_events(jm, fp, expected_min=1, timeout_s=5.0)
        event_count = len(events)
        score = 100 if event_count >= 1 else 0
        flog(
            f"p10_provider_cycle: provider={provider_label} "
            f"driver={driver_label} commit_ok=True "
            f"event_count={event_count} score={score} "
            f"trace_id={ctx.trace_id}",
            "INFO" if score == 100 else "ERROR",
        )
        return {
            "provider": provider_label, "driver": driver_label,
            "score": score, "event_count": event_count,
            "connect_refused": False, "error": None,
        }
    except Exception as exc:
        error = repr(exc)
        flog(
            f"p10_provider_cycle: provider={provider_label} "
            f"driver={driver_label} exception={error} score=0 "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
        return {
            "provider": provider_label, "driver": driver_label,
            "score": 0, "event_count": event_count,
            "connect_refused": connect_refused, "error": error,
        }
    finally:
        _close_temp_journal(jm, wq)
        cleanup_temp_dir(tmpdir)


def emit_validate_rewind(provider_label: str, driver_label: str,
                         score: int, layer=None) -> None:
    """Emit the K-3 signature log line.

    BL-RW-P3-20: the line carries `layer=<name>` and
    `identity_strength=<level>` so the validation matrix can group runs
    by provider class. The layer arg is optional: when missing we fall
    back to `layer=<unknown>` and `identity_strength=<unknown>` with a
    DEBUG note so downstream tooling can detect the gap.
    """
    from recoverland.core.logger import flog

    layer_name = "<unknown>"
    identity_strength = "<unknown>"
    if layer is not None:
        try:
            from recoverland.core.identity import (
                extract_layer_name, get_identity_strength_for_layer,
            )
            layer_name = extract_layer_name(layer) or "<empty>"
            identity_strength = get_identity_strength_for_layer(layer).value
        except Exception as exc:
            flog(
                f"emit_validate_rewind: layer introspection failed "
                f"err={type(exc).__name__} msg={exc!r}",
                "DEBUG",
            )

    # Escape spaces / quotes in layer_name to keep the key=value format
    # parseable by downstream log tooling.
    safe_layer_name = layer_name.replace('"', '_').replace("'", '_')
    if any(c.isspace() for c in safe_layer_name):
        safe_layer_name = '"' + safe_layer_name + '"'

    flog(
        f"validate_rewind: layer={safe_layer_name} "
        f"provider={provider_label} driver={driver_label} "
        f"identity_strength={identity_strength} score={score}",
        "INFO",
    )
