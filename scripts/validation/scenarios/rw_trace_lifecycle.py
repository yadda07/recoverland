"""Scenario BL-RW-P0-24: trace lifecycle = persistent dedup memory.

Proves offline (no QGIS) that:
  1. The REAL fetch + dedup pipeline neutralises already-compensated user
     events when restore traces stay ACTIVE across sessions (invariant I-8).
  2. The exact wipe SQL used by _invalidate_orphan_traces_on_open destroys
     that memory and re-activates every compensated event (leak mechanism).
  3. recover_dialog.py no longer wipes traces at open nor blocks rewind on
     active traces (source inspection: FAILS before patch, gate G2).
  4. Scoped invalidation (undo path) re-activates ONLY the undone events.
"""
import re
import sqlite3
import sys
from pathlib import Path

SCENARIO_ID = "rw_trace_lifecycle"
INVARIANT = "BL-RW-P0-24"

# A4 (G3.5): unscoped trace-wipe SQL pattern, function-name independent.
# Matches any UPDATE setting invalidated_at on ALL active traces without an
# 'IN (' scoping list between the two predicates.
_WIPE_PATTERN = re.compile(
    r"SET\s+invalidated_at[\s\S]{0,200}?"
    r"restored_from_event_id\s+IS\s+NOT\s+NULL[\s\S]{0,120}?"
    r"invalidated_at\s+IS\s+NULL"
)


def _find_unscoped_wipe(source: str):
    hits = []
    for m in _WIPE_PATTERN.finditer(source):
        if "IN (" not in m.group(0):
            hits.append(source[:m.start()].count("\n") + 1)
    return hits


_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PLUGINS_PARENT = _PLUGIN_ROOT.parent
if str(_PLUGINS_PARENT) not in sys.path:
    sys.path.insert(0, str(_PLUGINS_PARENT))

_FP = "ds_trace_lifecycle"
_DDL = """CREATE TABLE IF NOT EXISTS audit_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_fingerprint TEXT NOT NULL,
    datasource_fingerprint TEXT NOT NULL,
    layer_id_snapshot TEXT,
    layer_name_snapshot TEXT,
    provider_type TEXT NOT NULL,
    feature_identity_json TEXT,
    operation_type TEXT NOT NULL CHECK(operation_type IN ('INSERT','UPDATE','DELETE')),
    attributes_json TEXT NOT NULL,
    geometry_wkb BLOB,
    geometry_type TEXT DEFAULT 'NoGeometry',
    crs_authid TEXT,
    field_schema_json TEXT,
    user_name TEXT NOT NULL,
    session_id TEXT,
    created_at TEXT NOT NULL,
    restored_from_event_id INTEGER,
    entity_fingerprint TEXT,
    event_schema_version INTEGER,
    new_geometry_wkb BLOB,
    invalidated_at TEXT
)"""

_INSERT_SQL = (
    "INSERT INTO audit_event (event_id, project_fingerprint, "
    "datasource_fingerprint, layer_id_snapshot, layer_name_snapshot, "
    "provider_type, feature_identity_json, operation_type, attributes_json, "
    "geometry_wkb, geometry_type, crs_authid, field_schema_json, user_name, "
    "session_id, created_at, restored_from_event_id, entity_fingerprint, "
    "event_schema_version, new_geometry_wkb, invalidated_at) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

# The exact statement recover_dialog._invalidate_orphan_traces_on_open runs.
_WIPE_SQL = (
    "UPDATE audit_event SET invalidated_at = '2026-07-05T00:00:00' "
    "WHERE restored_from_event_id IS NOT NULL AND invalidated_at IS NULL"
)


def _mk_user(eid, op, fid, created_at):
    return (eid, "proj", _FP, "layer_id", "layer_name", "ogr",
            '{"fid": %d}' % fid, op, '{"all_attributes": {"name": "v"}}',
            None, "NoGeometry", "EPSG:2154", "[]", "test", "sess",
            created_at, None, f"fid:{fid}", 5, None, None)


def _mk_trace(eid, comp_op, fid, src_eid, created_at, invalidated_at=None):
    return (eid, "proj", _FP, "layer_id", "layer_name", "ogr",
            '{"fid": %d}' % fid, comp_op, '{"all_attributes": {}}',
            None, "NoGeometry", "EPSG:2154", "[]", "test", "sess_restore",
            created_at, src_eid, f"fid:{fid}", 5, None, invalidated_at)


def _fresh_journal():
    conn = sqlite3.connect(":memory:")
    conn.execute(_DDL)
    rows = [
        _mk_user(100, "UPDATE", 9, "2026-07-01T10:00:00"),
        _mk_user(101, "INSERT", 1, "2026-07-01T12:00:00"),
        _mk_user(102, "UPDATE", 2, "2026-07-01T12:05:00"),
        _mk_trace(201, "DELETE", 1, 101, "2026-07-01T13:00:00"),
        _mk_trace(202, "UPDATE", 2, 102, "2026-07-01T13:00:01"),
        _mk_user(103, "UPDATE", 3, "2026-07-01T14:00:00"),
    ]
    conn.executemany(_INSERT_SQL, rows)
    conn.commit()
    return conn


def _fetch_active_eids(esr, dedup, conn, cutoff_value):
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_value, inclusive=True)
    events = esr.fetch_events_after_cutoff(conn, _FP, cutoff)
    active, stats = dedup.collapse_rewind_events_with_stats(events)
    return events, {e.event_id for e in active}, stats


def _install_qgis_shim():
    """Offline-only stub so recoverland.core imports without QGIS.

    Every attribute resolves to a permissive placeholder class; the modules
    under test (event_stream_repository, rewind_dedup) never touch QGIS API.
    In the QGIS console the real qgis package is present and this is a no-op.
    """
    import types

    class _StubMeta(type):
        def __getattr__(cls, name):
            return cls

    class _Stub(metaclass=_StubMeta):
        def __init__(self, *args, **kwargs):
            pass

        def __call__(self, *args, **kwargs):
            return self

    def _getattr(attr, _s=_Stub):
        if attr.startswith("__") and attr.endswith("__"):
            raise AttributeError(attr)
        return _s

    def _module(name, package=False):
        mod = types.ModuleType(name)
        mod.__getattr__ = _getattr
        if package:
            mod.__path__ = []
        sys.modules[name] = mod
        return mod

    qgis = _module("qgis", package=True)
    qgis.core = _module("qgis.core")
    qgis.gui = _module("qgis.gui")
    qgis.utils = _module("qgis.utils")
    qgis.PyQt = _module("qgis.PyQt", package=True)
    qgis.PyQt.QtCore = _module("qgis.PyQt.QtCore")
    qgis.PyQt.QtGui = _module("qgis.PyQt.QtGui")
    qgis.PyQt.QtWidgets = _module("qgis.PyQt.QtWidgets")


def setup(ctx):
    import importlib
    try:
        import qgis  # noqa: F401 - real QGIS runtime available
    except ImportError:
        _install_qgis_shim()
    import recoverland.core.event_stream_repository as esr
    import recoverland.core.rewind_dedup as dedup
    importlib.reload(esr)
    importlib.reload(dedup)
    ctx.data["esr"] = esr
    ctx.data["dedup"] = dedup
    ctx.data["dialog_source"] = (
        _PLUGIN_ROOT / "recover_dialog.py").read_text(encoding="utf-8")


def run(ctx):
    esr = ctx.data["esr"]
    dedup = ctx.data["dedup"]

    # 1. Active traces neutralise compensated events (I-8 nominal).
    conn = _fresh_journal()
    events, active_eids, stats = _fetch_active_eids(
        esr, dedup, conn, "2026-07-01T11:00:00")
    ctx.data["nominal_fetched"] = len(events)
    ctx.data["nominal_active"] = active_eids
    ctx.data["nominal_stats"] = stats

    # 2. Wipe SQL destroys the dedup memory (leak mechanism demo).
    conn.execute(_WIPE_SQL)
    conn.commit()
    _, active_after_wipe, _ = _fetch_active_eids(
        esr, dedup, conn, "2026-07-01T11:00:00")
    ctx.data["wipe_active"] = active_after_wipe
    conn.close()

    # 3. Scoped undo invalidation re-activates ONLY the undone event.
    conn = _fresh_journal()
    conn.execute(
        "UPDATE audit_event SET invalidated_at = '2026-07-05T01:00:00' "
        "WHERE restored_from_event_id IN (102) AND invalidated_at IS NULL")
    conn.commit()
    _, active_after_undo, _ = _fetch_active_eids(
        esr, dedup, conn, "2026-07-01T11:00:00")
    ctx.data["undo_active"] = active_after_undo
    conn.close()

    # 4. Stacked rewind with EARLIER cutoff: older event compensated once.
    conn = _fresh_journal()
    _, active_stacked, _ = _fetch_active_eids(
        esr, dedup, conn, "2026-07-01T09:00:00")
    ctx.data["stacked_active"] = active_stacked

    # 5. Trace pointing to a purged (missing) user event: harmless.
    conn.execute(_INSERT_SQL, _mk_trace(
        203, "UPDATE", 7, 999, "2026-07-01T13:00:02"))
    conn.commit()
    _, active_orphan, _ = _fetch_active_eids(
        esr, dedup, conn, "2026-07-01T09:00:00")
    ctx.data["orphan_active"] = active_orphan
    conn.close()


def assertions(ctx):
    src = ctx.data["dialog_source"]
    nominal = ctx.data["nominal_active"]
    return [
        ("fetch_includes_active_traces",
         ctx.data["nominal_fetched"] == 5,
         f"fetched={ctx.data['nominal_fetched']} expected 5 (3 user + 2 traces)"),
        ("dedup_neutralises_compensated",
         nominal == {103},
         f"active={sorted(nominal)} expected [103]"),
        ("neg_compensated_not_reapplied",
         101 not in nominal and 102 not in nominal,
         f"eids 101/102 must NOT be active, active={sorted(nominal)}"),
        ("wipe_sql_destroys_memory",
         ctx.data["wipe_active"] == {101, 102, 103},
         f"after wipe active={sorted(ctx.data['wipe_active'])} expected "
         f"[101, 102, 103] (re-compensation = leak mechanism)"),
        ("undo_scoped_reactivation",
         ctx.data["undo_active"] == {102, 103},
         f"after scoped undo active={sorted(ctx.data['undo_active'])} "
         f"expected [102, 103] (101 stays neutralised)"),
        ("stacked_rewind_earlier_cutoff",
         ctx.data["stacked_active"] == {100, 103},
         f"active={sorted(ctx.data['stacked_active'])} expected [100, 103]"),
        ("orphan_trace_harmless",
         ctx.data["orphan_active"] == {100, 103},
         f"active={sorted(ctx.data['orphan_active'])} expected [100, 103] "
         f"(trace to purged eid=999 must not crash nor neutralise)"),
        ("dialog_wipe_removed",
         "_invalidate_orphan_traces_on_open" not in src,
         "recover_dialog.py must not define nor call "
         "_invalidate_orphan_traces_on_open (unscoped trace wipe at open)"),
        ("dialog_no_unscoped_wipe_sql",
         not _find_unscoped_wipe(src),
         f"unscoped trace-wipe SQL found at lines "
         f"{_find_unscoped_wipe(src)} (A4: function-name independent check; "
         f"any bulk invalidation of traces must be scoped with IN (...))"),
        ("dialog_guard_block_removed",
         "BLOCKED active_restore_traces" not in src,
         "recover_dialog.py must not block rewind on active traces "
         "(dedup neutralisation replaces the guard)"),
        ("dialog_scoped_undo_invalidation_kept",
         "_invalidate_trace_events" in src
         and "restored_from_event_id IN (" in src,
         "scoped undo invalidation path must remain in recover_dialog.py"),
    ]
