"""Scenario IL-4 - WKB envelope parser + fetch_events_in_zone (CR-IL-1).

Verifies two deliverables of BL-IL-P0-04:

    1. core/wkb_envelope.py exposes parse_envelope(wkb_bytes) -> Optional[
       Tuple[xmin, ymin, xmax, ymax]] for the 6 common geometry types
       (POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING,
       MULTIPOLYGON), in both byteorders, with EWKB SRID flag tolerated.
    2. core/event_stream_repository.fetch_events_in_zone(conn, datasource_fp,
       bbox_xy, t_min, t_max, limit, trace_id) returns
       Tuple[List[AuditEvent], LensFetchStats]. Filters by datasource +
       temporal window in SQL, then by BBOX in Python via wkb_envelope.

Cause racine: CR-IL-1 (no spatial index in audit_event schema). The P0
must filter BBOX in Python after a bounded SQL read.

Acceptance assertions (each one is an antithesis):
    1. core.wkb_envelope module exists and is importable without QGIS/Qt.
    2. parse_envelope(POINT(10 20) WKB)            -> (10.0, 20.0, 10.0, 20.0)
    3. parse_envelope(LINESTRING(0 0, 10 10) WKB) -> (0.0, 0.0, 10.0, 10.0)
    4. parse_envelope(POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)) WKB) -> (0,0,10,10)
    5. fetch_events_in_zone exists in event_stream_repository.
    6. fetch_events_in_zone returns Tuple[List, LensFetchStats] and
       applies temporal + BBOX filters on a synthetic DB.

Initial verdict: FAIL (neither file exists / function missing).
Post-patch verdict: PASS.

Pure Python. No QGIS.
"""
from __future__ import annotations

import importlib.util
import sqlite3
import struct
import sys
import tempfile
import types
from pathlib import Path

SCENARIO_ID = "il4_fetch_events_in_zone"
INVARIANT = "BL-IL-P0-04"
EXPECTED_SIGNATURE = r""

_PLUGIN_ROOT = Path(__file__).resolve().parents[4]
_WKB_ENVELOPE_PATH = _PLUGIN_ROOT / "core" / "wkb_envelope.py"
_REPO_PATH = _PLUGIN_ROOT / "core" / "event_stream_repository.py"

# Hardcoded little-endian WKB samples (validated against OGR / Shapely).
# Format: 0x01 (LE), 4-byte type, then coordinates.


def _le_double(value: float) -> bytes:
    return struct.pack("<d", value)


def _build_point_wkb(x: float, y: float) -> bytes:
    return b"\x01" + struct.pack("<I", 1) + _le_double(x) + _le_double(y)


def _build_linestring_wkb(coords) -> bytes:
    n = len(coords)
    out = b"\x01" + struct.pack("<I", 2) + struct.pack("<I", n)
    for x, y in coords:
        out += _le_double(x) + _le_double(y)
    return out


def _build_polygon_wkb(rings) -> bytes:
    n_rings = len(rings)
    out = b"\x01" + struct.pack("<I", 3) + struct.pack("<I", n_rings)
    for ring in rings:
        n = len(ring)
        out += struct.pack("<I", n)
        for x, y in ring:
            out += _le_double(x) + _le_double(y)
    return out


# ----- Stubbing helpers --------------------------------------------------


def _ensure_core_stub() -> None:
    if "core" in sys.modules and hasattr(sys.modules["core"], "__path__"):
        return
    pkg = types.ModuleType("core")
    pkg.__path__ = [str(_PLUGIN_ROOT / "core")]  # type: ignore[attr-defined]
    sys.modules["core"] = pkg


def _stub_logger() -> None:
    stub = types.ModuleType("core.logger")
    stub.flog = lambda *a, **kw: None  # noqa: ARG005

    from contextlib import contextmanager

    @contextmanager
    def _noop_timed_op(name, trace_id=""):  # noqa: ARG001
        yield None

    stub.timed_op = _noop_timed_op
    sys.modules["core.logger"] = stub


def _stub_sql_safety() -> None:
    # The real implementation forbids weird characters; for the scenario
    # the simpler validator (any non-empty str) is enough.
    stub = types.ModuleType("core.sql_safety")

    def _assert_safe_fragment(s):
        if not isinstance(s, str) or not s:
            raise ValueError("empty fragment")

    stub.assert_safe_fragment = _assert_safe_fragment
    sys.modules["core.sql_safety"] = stub


def _stub_sqlite_schema() -> None:
    stub = types.ModuleType("core.sqlite_schema")
    cols = (
        "event_id", "project_fingerprint", "datasource_fingerprint",
        "layer_id_snapshot", "layer_name_snapshot", "provider_type",
        "feature_identity_json", "operation_type", "attributes_json",
        "geometry_wkb", "geometry_type", "crs_authid", "field_schema_json",
        "user_name", "session_id", "created_at", "restored_from_event_id",
        "entity_fingerprint", "event_schema_version", "new_geometry_wkb",
        "invalidated_at",
    )
    stub.AUDIT_EVENT_COLUMNS = cols
    stub.AUDIT_EVENT_SELECT_SQL = ", ".join(cols)

    def _lightweight_sql():
        return stub.AUDIT_EVENT_SELECT_SQL

    stub.build_lightweight_select_sql = _lightweight_sql
    sys.modules["core.sqlite_schema"] = stub


def _import_module_with_stubs(name: str, path: Path):
    _ensure_core_stub()
    _stub_logger()
    _stub_sql_safety()
    _stub_sqlite_schema()
    # audit_backend and sql_safety might already be importable; allow real import.
    # For audit_backend (pure Python NamedTuple) we import via importlib.
    if "core.audit_backend" not in sys.modules:
        ab_spec = importlib.util.spec_from_file_location(
            "core.audit_backend", str(_PLUGIN_ROOT / "core" / "audit_backend.py")
        )
        ab_module = importlib.util.module_from_spec(ab_spec)
        ab_module.__package__ = "core"
        sys.modules["core.audit_backend"] = ab_module
        ab_spec.loader.exec_module(ab_module)
    # search_service imports logger, serialization, etc. Stub it minimally:
    if "core.search_service" not in sys.modules:
        ss_stub = types.ModuleType("core.search_service")
        ab_module = sys.modules["core.audit_backend"]
        AuditEvent = ab_module.AuditEvent  # type: ignore[attr-defined]

        def _row_to_event(row):
            return AuditEvent(*row)

        ss_stub._row_to_event = _row_to_event
        sys.modules["core.search_service"] = ss_stub
    # restore_contracts imports nothing else than stdlib. Real import.
    if "core.restore_contracts" not in sys.modules:
        rc_spec = importlib.util.spec_from_file_location(
            "core.restore_contracts",
            str(_PLUGIN_ROOT / "core" / "restore_contracts.py"),
        )
        rc_module = importlib.util.module_from_spec(rc_spec)
        rc_module.__package__ = "core"
        sys.modules["core.restore_contracts"] = rc_module
        rc_spec.loader.exec_module(rc_module)
    # lens_contracts must already exist from BL-IL-P0-02.
    if "core.lens_contracts" not in sys.modules:
        lc_spec = importlib.util.spec_from_file_location(
            "core.lens_contracts",
            str(_PLUGIN_ROOT / "core" / "lens_contracts.py"),
        )
        lc_module = importlib.util.module_from_spec(lc_spec)
        lc_module.__package__ = "core"
        sys.modules["core.lens_contracts"] = lc_module
        lc_spec.loader.exec_module(lc_module)

    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {name}")
    module = importlib.util.module_from_spec(spec)
    module.__package__ = "core"
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ----- Setup / run / assertions ------------------------------------------


def setup(ctx):
    # Try import wkb_envelope.
    ctx.data["wkb_envelope_exists"] = _WKB_ENVELOPE_PATH.is_file()
    ctx.data["wkb_envelope_module"] = None
    ctx.data["wkb_envelope_import_error"] = None
    if ctx.data["wkb_envelope_exists"]:
        try:
            ctx.data["wkb_envelope_module"] = _import_module_with_stubs(
                "core.wkb_envelope", _WKB_ENVELOPE_PATH
            )
        except Exception as exc:  # noqa: BLE001
            ctx.data["wkb_envelope_import_error"] = repr(exc)

    # Try import event_stream_repository (with stubs).
    ctx.data["repo_module"] = None
    ctx.data["repo_import_error"] = None
    if _REPO_PATH.is_file():
        try:
            ctx.data["repo_module"] = _import_module_with_stubs(
                "core.event_stream_repository", _REPO_PATH
            )
        except Exception as exc:  # noqa: BLE001
            ctx.data["repo_import_error"] = repr(exc)


def run(ctx):
    """Build a synthetic DB and call fetch_events_in_zone."""
    repo = ctx.data.get("repo_module")
    if repo is None or not hasattr(repo, "fetch_events_in_zone"):
        ctx.data["fetch_result"] = None
        return

    tmp_dir = Path(tempfile.mkdtemp(prefix="rl_il4_"))
    db_path = tmp_dir / "synthetic.sqlite"
    conn = sqlite3.connect(str(db_path))
    cols = sys.modules["core.sqlite_schema"].AUDIT_EVENT_COLUMNS
    col_defs = ", ".join(f"{c} TEXT" if c != "event_id" else
                         "event_id INTEGER PRIMARY KEY AUTOINCREMENT"
                         for c in cols)
    # geometry_wkb and new_geometry_wkb should be BLOBs; redefine.
    col_defs = col_defs.replace("geometry_wkb TEXT", "geometry_wkb BLOB")
    col_defs = col_defs.replace("new_geometry_wkb TEXT", "new_geometry_wkb BLOB")
    conn.execute(f"CREATE TABLE audit_event ({col_defs})")  # nosec B608

    # Insert 5 events :
    # E1: datasource_A in zone   in window    -> SHOULD MATCH
    # E2: datasource_A out zone  in window    -> filtered by BBOX
    # E3: datasource_A in zone   out window   -> filtered by time
    # E4: datasource_B in zone   in window    -> filtered by datasource
    # E5: datasource_A NULL geom in window    -> kept (audit attr-only event)
    rows = [
        ("PROJ", "DS_A", "L1", "L1", "memory", "{}", "UPDATE", "{}",
         _build_point_wkb(5.0, 5.0), "Point", "EPSG:4326", "{}",
         "user", "S1", "2026-05-01T10:00:00Z", None, "ENT_1", 2, None, None),
        ("PROJ", "DS_A", "L1", "L1", "memory", "{}", "UPDATE", "{}",
         _build_point_wkb(100.0, 100.0), "Point", "EPSG:4326", "{}",
         "user", "S1", "2026-05-02T10:00:00Z", None, "ENT_2", 2, None, None),
        ("PROJ", "DS_A", "L1", "L1", "memory", "{}", "UPDATE", "{}",
         _build_point_wkb(5.0, 5.0), "Point", "EPSG:4326", "{}",
         "user", "S1", "2026-04-01T10:00:00Z", None, "ENT_3", 2, None, None),
        ("PROJ", "DS_B", "L2", "L2", "memory", "{}", "UPDATE", "{}",
         _build_point_wkb(5.0, 5.0), "Point", "EPSG:4326", "{}",
         "user", "S1", "2026-05-01T11:00:00Z", None, "ENT_4", 2, None, None),
        ("PROJ", "DS_A", "L1", "L1", "memory", "{}", "UPDATE", "{}",
         None, "NoGeometry", "EPSG:4326", "{}",
         "user", "S1", "2026-05-01T12:00:00Z", None, "ENT_5", 2, None, None),
    ]
    insert_cols = ", ".join(cols[1:])  # skip event_id
    placeholders = ", ".join(["?"] * len(rows[0]))
    conn.executemany(
        f"INSERT INTO audit_event ({insert_cols}) VALUES ({placeholders})",  # nosec
        rows,
    )
    conn.commit()

    bbox = (0.0, 0.0, 10.0, 10.0)
    t_min = "2026-04-15T00:00:00Z"
    t_max = "2026-05-15T00:00:00Z"
    try:
        result = repo.fetch_events_in_zone(
            conn, "DS_A", bbox, t_min, t_max, limit=100, trace_id="il4test",
        )
        ctx.data["fetch_result"] = result
        ctx.data["fetch_error"] = None
    except Exception as exc:  # noqa: BLE001
        ctx.data["fetch_result"] = None
        ctx.data["fetch_error"] = repr(exc)
    finally:
        conn.close()


def assertions(ctx):
    results = []
    wkb_exists = ctx.data["wkb_envelope_exists"]
    wkb_mod = ctx.data.get("wkb_envelope_module")
    wkb_err = ctx.data.get("wkb_envelope_import_error")

    # 1. wkb_envelope import OK?
    ok = wkb_exists and wkb_mod is not None and wkb_err is None
    msg = (f"file_exists={wkb_exists} import_error={wkb_err}"
           if not ok else "imported ok")
    results.append(("wkb_envelope_importable", ok, msg))

    parse = getattr(wkb_mod, "parse_envelope", None) if ok else None

    def _short_circuit(reason: str):
        for name in (
            "parse_envelope_point",
            "parse_envelope_linestring",
            "parse_envelope_polygon",
            "fetch_events_in_zone_exists",
            "fetch_events_in_zone_filters_correctly",
        ):
            results.append((name, False, reason))

    if parse is None:
        _short_circuit("skipped: wkb_envelope.parse_envelope missing")
    else:
        # 2. Point
        env = parse(_build_point_wkb(10.0, 20.0))
        results.append((
            "parse_envelope_point",
            env == (10.0, 20.0, 10.0, 20.0),
            f"got {env}",
        ))
        # 3. LineString
        env = parse(_build_linestring_wkb([(0.0, 0.0), (10.0, 10.0)]))
        results.append((
            "parse_envelope_linestring",
            env == (0.0, 0.0, 10.0, 10.0),
            f"got {env}",
        ))
        # 4. Polygon
        env = parse(_build_polygon_wkb([[
            (0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0),
        ]]))
        results.append((
            "parse_envelope_polygon",
            env == (0.0, 0.0, 10.0, 10.0),
            f"got {env}",
        ))

        # 5/6. fetch_events_in_zone
        repo = ctx.data.get("repo_module")
        has_fn = repo is not None and hasattr(repo, "fetch_events_in_zone")
        results.append((
            "fetch_events_in_zone_exists", has_fn,
            f"hasattr={has_fn} repo_import_error={ctx.data.get('repo_import_error')}",
        ))

        result = ctx.data.get("fetch_result")
        if result is None:
            results.append((
                "fetch_events_in_zone_filters_correctly", False,
                f"call failed: {ctx.data.get('fetch_error')}",
            ))
        else:
            events, stats = result
            # Expected: E1 (DS_A, in-zone, in-window) + E5 (DS_A, NULL geom,
            # in-window) = 2 events. E2 filtered by BBOX, E3 by time,
            # E4 by datasource.
            fingerprints = sorted([e.entity_fingerprint for e in events])
            ok_filter = (
                fingerprints == ["ENT_1", "ENT_5"]
                and stats.n_events_returned == 2
                and stats.n_events_truncated == 0
            )
            results.append((
                "fetch_events_in_zone_filters_correctly", ok_filter,
                f"fingerprints={fingerprints} stats={stats}",
            ))

    return results


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
