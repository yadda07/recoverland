"""Scenario BL-RW-P1-23-A1: rewind fetch truncates at MAX_EVENTS_PER_RESTORE.

Standalone proof (no QGIS) that fetch_events_after_cutoff silently returns
only MAX_EVENTS_PER_RESTORE rows even when count_events_after_cutoff reports
more. This breaks the promise "all events for a layer are recovered".
"""
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, NamedTuple, Optional

SCENARIO_ID = "rw_rewind_fetch_limit"
INVARIANT = "BL-RW-P1-23"

_OVER_LIMIT = 12_000
_MAX_EVENTS_PER_RESTORE = 10_000
_FP = "datasource_test_fp"
_FP_A2 = "datasource_fid_collision"
_FP_AT1 = "datasource_at1_multi_update"
_FP_AT2 = "datasource_at2_delete_only"
_FP_AT3 = "datasource_at3_recycle_with_update"


class RestoreCutoff(NamedTuple):
    cutoff_type: str
    value: str
    inclusive: bool = True


class CutoffType:
    BY_DATE = "BY_DATE"
    BY_EVENT_ID = "BY_EVENT_ID"


class AuditEvent(NamedTuple):
    event_id: Optional[int]
    project_fingerprint: str
    datasource_fingerprint: str
    layer_id_snapshot: str
    layer_name_snapshot: str
    provider_type: str
    feature_identity_json: str
    operation_type: str
    attributes_json: str
    geometry_wkb: Optional[bytes]
    geometry_type: str
    crs_authid: Optional[str]
    field_schema_json: str
    user_name: str
    session_id: Optional[str]
    created_at: str
    restored_from_event_id: Optional[int]
    entity_fingerprint: Optional[str] = None
    event_schema_version: Optional[int] = None
    new_geometry_wkb: Optional[bytes] = None
    invalidated_at: Optional[str] = None


_EVENT_COLUMNS = (
    "event_id", "project_fingerprint", "datasource_fingerprint",
    "layer_id_snapshot", "layer_name_snapshot", "provider_type",
    "feature_identity_json", "operation_type", "attributes_json",
    "geometry_wkb", "geometry_type", "crs_authid", "field_schema_json",
    "user_name", "session_id", "created_at", "restored_from_event_id",
    "entity_fingerprint", "event_schema_version", "new_geometry_wkb",
    "invalidated_at",
)


def _row_to_event(row):
    geom_wkb = row[9]
    new_geom = row[19] if len(row) > 19 else None
    return AuditEvent(
        event_id=row[0],
        project_fingerprint=row[1],
        datasource_fingerprint=row[2],
        layer_id_snapshot=row[3],
        layer_name_snapshot=row[4],
        provider_type=row[5],
        feature_identity_json=row[6],
        operation_type=row[7],
        attributes_json=row[8],
        geometry_wkb=geom_wkb,
        geometry_type=row[10],
        crs_authid=row[11],
        field_schema_json=row[12],
        user_name=row[13],
        session_id=row[14],
        created_at=row[15],
        restored_from_event_id=row[16],
        entity_fingerprint=row[17] if len(row) > 17 else None,
        event_schema_version=row[18] if len(row) > 18 else None,
        new_geometry_wkb=new_geom,
        invalidated_at=row[20] if len(row) > 20 else None,
    )


def _cutoff_where(datasource_fp, cutoff, include_traces=True):
    op = ">=" if cutoff.inclusive else ">"
    if cutoff.cutoff_type == CutoffType.BY_EVENT_ID:
        cutoff_col = "event_id"
    elif cutoff.cutoff_type == CutoffType.BY_DATE:
        cutoff_col = "created_at"
    else:
        return None, []
    ds_cond = "datasource_fingerprint = ? AND " if datasource_fp else ""
    ds_params = [datasource_fp] if datasource_fp else []
    if not include_traces:
        clause = ds_cond + "restored_from_event_id IS NULL AND " + cutoff_col + " " + op + " ?"
        return clause, ds_params + [cutoff.value]
    user_clause = "(restored_from_event_id IS NULL AND " + cutoff_col + " " + op + " ?)"
    trace_clause = ("(restored_from_event_id IS NOT NULL AND invalidated_at IS NULL AND restored_from_event_id IN ("
                   "SELECT event_id FROM audit_event WHERE " + ds_cond + cutoff_col + " " + op + " ?))")
    clause = ds_cond + "(" + user_clause + " OR " + trace_clause + ")"
    params = ds_params + [cutoff.value] + ds_params + [cutoff.value]
    return clause, params


def fetch_events_after_cutoff(conn, datasource_fp, cutoff, limit=_MAX_EVENTS_PER_RESTORE):
    where, params = _cutoff_where(datasource_fp, cutoff, include_traces=True)
    if where is None:
        return []
    cols = ", ".join(_EVENT_COLUMNS)
    query = "SELECT " + cols + " FROM audit_event WHERE " + where + " ORDER BY created_at DESC, event_id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [_row_to_event(r) for r in rows]


def count_events_after_cutoff(conn, datasource_fp, cutoff):
    where, params = _cutoff_where(datasource_fp, cutoff, include_traces=True)
    if where is None:
        return 0
    row = conn.execute("SELECT COUNT(*) FROM audit_event WHERE " + where, params).fetchone()
    return row[0] if row else 0


def _init_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS audit_event (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_fingerprint TEXT NOT NULL,
        datasource_fingerprint TEXT NOT NULL,
        layer_id_snapshot TEXT,
        layer_name_snapshot TEXT,
        provider_type TEXT NOT NULL,
        feature_identity_json TEXT,
        operation_type TEXT NOT NULL CHECK(operation_type IN ("INSERT","UPDATE","DELETE")),
        attributes_json TEXT NOT NULL,
        geometry_wkb BLOB,
        geometry_type TEXT DEFAULT "NoGeometry",
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
    )""")
    conn.commit()


def _insert_event(conn, eid, ds_fp, fid, op, attrs_json, geom_wkb,
                  entity_fp, created_at):
    conn.execute(
        "INSERT INTO audit_event ("
        "project_fingerprint, datasource_fingerprint, "
        "layer_id_snapshot, layer_name_snapshot, provider_type, "
        "feature_identity_json, operation_type, attributes_json, "
        "geometry_wkb, geometry_type, crs_authid, field_schema_json, "
        "user_name, session_id, created_at, restored_from_event_id, "
        "entity_fingerprint, event_schema_version, new_geometry_wkb, "
        "invalidated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("proj", ds_fp, "layer_id", "layer_name", "memory",
         json.dumps({"fid": fid}), op, attrs_json,
         geom_wkb, "NoGeometry", "EPSG:4326", "[]",
         "test", "session", created_at, None,
         entity_fp, 5, None, None),
    )


def setup(ctx):
    ctx.data["conn"] = sqlite3.connect(":memory:")
    _init_schema(ctx.data["conn"])
    base = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    rows = []
    for i in range(_OVER_LIMIT):
        created = (base + timedelta(seconds=i)).isoformat().replace("+00:00", "")
        rows.append((
            "proj", _FP, "layer_id", "layer_name", "memory",
            json.dumps({"fid": i}),
            "UPDATE", json.dumps({"changed_only": {"name": {"old": "A", "new": "B"}}}),
            None, "NoGeometry", "EPSG:4326", "[]", "test", "session", created,
            None, "entity_{}".format(i), 5, None, None,
        ))
    sql = ("INSERT INTO audit_event (project_fingerprint, datasource_fingerprint, "
           "layer_id_snapshot, layer_name_snapshot, provider_type, feature_identity_json, "
           "operation_type, attributes_json, geometry_wkb, geometry_type, crs_authid, "
           "field_schema_json, user_name, session_id, created_at, restored_from_event_id, "
           "entity_fingerprint, event_schema_version, new_geometry_wkb, invalidated_at) "
           "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    ctx.data["conn"].executemany(sql, rows)

    a2_base = datetime(2026, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
    _insert_event(
        ctx.data["conn"], None, _FP_A2, 100, "UPDATE",
        json.dumps({"changed_only": {"name": {"old": "Original", "new": "Modified"}}}),
        None, "fid:100",
        (a2_base + timedelta(seconds=1)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_A2, 100, "DELETE",
        json.dumps({"all_attributes": {"name": "Modified"}}),
        None, "fid:100",
        (a2_base + timedelta(seconds=2)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_A2, 100, "INSERT",
        json.dumps({"all_attributes": {"name": "NewEntity"}}),
        None, "fid:100",
        (a2_base + timedelta(seconds=3)).isoformat().replace("+00:00", ""),
    )

    ctx.data["conn"].commit()
    ctx.data["cutoff"] = RestoreCutoff(
        CutoffType.BY_DATE,
        base.isoformat().replace("+00:00", ""),
        inclusive=True,
    )
    at1_base = datetime(2026, 1, 1, 14, 0, 0, tzinfo=timezone.utc)
    _insert_event(
        ctx.data["conn"], None, _FP_AT1, 200, "UPDATE",
        json.dumps({"changed_only": {"name": {"old": "V0", "new": "V1"}}}),
        None, "fid:200",
        (at1_base + timedelta(seconds=1)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_AT1, 200, "UPDATE",
        json.dumps({"changed_only": {"name": {"old": "V1", "new": "V2"}}}),
        None, "fid:200",
        (at1_base + timedelta(seconds=2)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_AT1, 200, "DELETE",
        json.dumps({"all_attributes": {"name": "V2"}}),
        None, "fid:200",
        (at1_base + timedelta(seconds=3)).isoformat().replace("+00:00", ""),
    )

    at2_base = datetime(2026, 1, 1, 15, 0, 0, tzinfo=timezone.utc)
    _insert_event(
        ctx.data["conn"], None, _FP_AT2, 300, "DELETE",
        json.dumps({"all_attributes": {"name": "Lonely"}}),
        None, "fid:300",
        (at2_base + timedelta(seconds=1)).isoformat().replace("+00:00", ""),
    )

    at3_base = datetime(2026, 1, 1, 16, 0, 0, tzinfo=timezone.utc)
    _insert_event(
        ctx.data["conn"], None, _FP_AT3, 400, "UPDATE",
        json.dumps({"changed_only": {"name": {"old": "Orig3", "new": "Mod3"}}}),
        None, "fid:400",
        (at3_base + timedelta(seconds=1)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_AT3, 400, "DELETE",
        json.dumps({"all_attributes": {"name": "Mod3"}}),
        None, "fid:400",
        (at3_base + timedelta(seconds=2)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_AT3, 400, "INSERT",
        json.dumps({"all_attributes": {"name": "Recycled3"}}),
        None, "fid:400",
        (at3_base + timedelta(seconds=3)).isoformat().replace("+00:00", ""),
    )
    _insert_event(
        ctx.data["conn"], None, _FP_AT3, 400, "UPDATE",
        json.dumps({"changed_only": {"name": {"old": "Recycled3", "new": "RecycledMod3"}}}),
        None, "fid:400",
        (at3_base + timedelta(seconds=4)).isoformat().replace("+00:00", ""),
    )

    ctx.data["conn"].commit()
    ctx.data["a2_cutoff"] = RestoreCutoff(
        CutoffType.BY_DATE,
        a2_base.isoformat().replace("+00:00", ""),
        inclusive=True,
    )
    ctx.data["at1_cutoff"] = RestoreCutoff(
        CutoffType.BY_DATE,
        at1_base.isoformat().replace("+00:00", ""),
        inclusive=True,
    )
    ctx.data["at2_cutoff"] = RestoreCutoff(
        CutoffType.BY_DATE,
        at2_base.isoformat().replace("+00:00", ""),
        inclusive=True,
    )
    ctx.data["at3_cutoff"] = RestoreCutoff(
        CutoffType.BY_DATE,
        at3_base.isoformat().replace("+00:00", ""),
        inclusive=True,
    )


def run(ctx):
    conn = ctx.data["conn"]
    cutoff = ctx.data["cutoff"]
    ctx.data["count"] = count_events_after_cutoff(conn, _FP, cutoff)
    ctx.data["fetched"] = fetch_events_after_cutoff(conn, _FP, cutoff)
    ctx.data["fetched_len"] = len(ctx.data["fetched"])

    a2_cutoff = ctx.data["a2_cutoff"]
    a2_events = fetch_events_after_cutoff(conn, _FP_A2, a2_cutoff)
    ctx.data["a2_events"] = a2_events
    ctx.data["a2_events_len"] = len(a2_events)

    import importlib
    import recoverland.core.rewind_dedup as _rd
    importlib.reload(_rd)
    from recoverland.core.rewind_dedup import collapse_rewind_events
    plugin_events = [
        AuditEvent(
            event_id=e.event_id, project_fingerprint=e.project_fingerprint,
            datasource_fingerprint=e.datasource_fingerprint,
            layer_id_snapshot=e.layer_id_snapshot,
            layer_name_snapshot=e.layer_name_snapshot,
            provider_type=e.provider_type,
            feature_identity_json=e.feature_identity_json,
            operation_type=e.operation_type,
            attributes_json=e.attributes_json,
            geometry_wkb=e.geometry_wkb,
            geometry_type=e.geometry_type,
            crs_authid=e.crs_authid,
            field_schema_json=e.field_schema_json,
            user_name=e.user_name,
            session_id=e.session_id,
            created_at=e.created_at,
            restored_from_event_id=e.restored_from_event_id,
            entity_fingerprint=e.entity_fingerprint,
            event_schema_version=e.event_schema_version,
            new_geometry_wkb=e.new_geometry_wkb,
            invalidated_at=e.invalidated_at,
        )
        for e in a2_events
    ]
    ctx.data["a2_collapsed"] = collapse_rewind_events(plugin_events)
    ctx.data["a2_collapsed_len"] = len(ctx.data["a2_collapsed"])

    ops = [e.operation_type for e in ctx.data["a2_collapsed"]]
    ctx.data["a2_ops"] = ops

    synthetic_delete = None
    for e in ctx.data["a2_collapsed"]:
        if e.operation_type == "DELETE":
            synthetic_delete = e
            break
    ctx.data["a2_synthetic_delete"] = synthetic_delete
    if synthetic_delete is not None:
        import json as _json
        attrs = _json.loads(synthetic_delete.attributes_json)
        ctx.data["a2_synthetic_attrs"] = attrs.get("all_attributes", {})
        ctx.data["a2_synthetic_fp"] = synthetic_delete.entity_fingerprint

    def _fetch_and_collapse(ds_fp, cutoff_key):
        cutoff = ctx.data[cutoff_key]
        events = fetch_events_after_cutoff(conn, ds_fp, cutoff)
        plugin_evs = [
            AuditEvent(
                event_id=e.event_id, project_fingerprint=e.project_fingerprint,
                datasource_fingerprint=e.datasource_fingerprint,
                layer_id_snapshot=e.layer_id_snapshot,
                layer_name_snapshot=e.layer_name_snapshot,
                provider_type=e.provider_type,
                feature_identity_json=e.feature_identity_json,
                operation_type=e.operation_type,
                attributes_json=e.attributes_json,
                geometry_wkb=e.geometry_wkb,
                geometry_type=e.geometry_type,
                crs_authid=e.crs_authid,
                field_schema_json=e.field_schema_json,
                user_name=e.user_name,
                session_id=e.session_id,
                created_at=e.created_at,
                restored_from_event_id=e.restored_from_event_id,
                entity_fingerprint=e.entity_fingerprint,
                event_schema_version=e.event_schema_version,
                new_geometry_wkb=e.new_geometry_wkb,
                invalidated_at=e.invalidated_at,
            )
            for e in events
        ]
        return collapse_rewind_events(plugin_evs)

    at1_collapsed = _fetch_and_collapse(_FP_AT1, "at1_cutoff")
    ctx.data["at1_collapsed"] = at1_collapsed
    ctx.data["at1_len"] = len(at1_collapsed)
    ctx.data["at1_ops"] = [e.operation_type for e in at1_collapsed]
    at1_del = None
    for e in at1_collapsed:
        if e.operation_type == "DELETE":
            at1_del = e
            break
    ctx.data["at1_del"] = at1_del
    if at1_del is not None:
        import json as _json2
        at1_attrs = _json2.loads(at1_del.attributes_json)
        ctx.data["at1_del_attrs"] = at1_attrs.get("all_attributes", {})

    at2_collapsed = _fetch_and_collapse(_FP_AT2, "at2_cutoff")
    ctx.data["at2_collapsed"] = at2_collapsed
    ctx.data["at2_len"] = len(at2_collapsed)
    ctx.data["at2_ops"] = [e.operation_type for e in at2_collapsed]

    at3_collapsed = _fetch_and_collapse(_FP_AT3, "at3_cutoff")
    ctx.data["at3_collapsed"] = at3_collapsed
    ctx.data["at3_len"] = len(at3_collapsed)
    ctx.data["at3_ops"] = [e.operation_type for e in at3_collapsed]
    at3_fps = set()
    for e in at3_collapsed:
        if e.entity_fingerprint:
            at3_fps.add(e.entity_fingerprint)
    ctx.data["at3_fps"] = at3_fps


def assertions(ctx):
    count = ctx.data["count"]
    fetched_len = ctx.data["fetched_len"]
    a2_events_len = ctx.data["a2_events_len"]
    a2_collapsed_len = ctx.data["a2_collapsed_len"]
    a2_ops = ctx.data["a2_ops"]
    a2_synthetic_delete = ctx.data["a2_synthetic_delete"]
    a2_synthetic_attrs = ctx.data.get("a2_synthetic_attrs", {})
    a2_synthetic_fp = ctx.data.get("a2_synthetic_fp", "")
    at1_len = ctx.data["at1_len"]
    at1_ops = ctx.data["at1_ops"]
    at1_del_attrs = ctx.data.get("at1_del_attrs", {})
    at2_len = ctx.data["at2_len"]
    at2_ops = ctx.data["at2_ops"]
    at3_len = ctx.data["at3_len"]
    at3_ops = ctx.data["at3_ops"]
    at3_fps = ctx.data["at3_fps"]
    return [
        ("fetch_respects_limit",
         fetched_len == _MAX_EVENTS_PER_RESTORE,
         "fetched={} limit={}".format(fetched_len, _MAX_EVENTS_PER_RESTORE)),
        ("count_exceeds_limit",
         count > _MAX_EVENTS_PER_RESTORE,
         "count={} limit={}".format(count, _MAX_EVENTS_PER_RESTORE)),
        ("a2_events_fetched", a2_events_len == 3,
         "a2_events={}".format(a2_events_len)),
        ("a2_collapsed_to_2", a2_collapsed_len == 2,
         "collapsed={} expected=2".format(a2_collapsed_len)),
        ("a2_has_synthetic_delete", a2_synthetic_delete is not None,
         "synthetic_delete={}".format(a2_synthetic_delete)),
        ("a2_synthetic_has_cutoff_attrs",
         a2_synthetic_attrs.get("name") == "Original",
         "attrs={}".format(a2_synthetic_attrs)),
        ("a2_synthetic_fp_not_recycled",
         "@" not in a2_synthetic_fp,
         "fp={}".format(a2_synthetic_fp)),
        ("a2_ops_are_delete_insert",
         a2_ops == ["DELETE", "INSERT"] or a2_ops == ["INSERT", "DELETE"],
         "ops={}".format(a2_ops)),
        ("at1_collapsed_to_1", at1_len == 1,
         "at1_len={} expected=1".format(at1_len)),
        ("at1_oldest_old_preserved",
         at1_del_attrs.get("name") == "V0",
         "at1_attrs={} expected name=V0".format(at1_del_attrs)),
        ("at2_delete_only_unchanged", at2_len == 1 and at2_ops == ["DELETE"],
         "at2_len={} ops={}".format(at2_len, at2_ops)),
        ("at3_split_to_3", at3_len == 3,
         "at3_len={} expected=3".format(at3_len)),
        ("at3_has_two_distinct_fps", len(at3_fps) == 2,
         "at3_fps={} expected 2 distinct".format(at3_fps)),
        ("at3_ops_delete_insert_update",
         sorted(at3_ops) == ["DELETE", "INSERT", "UPDATE"],
         "at3_ops={}".format(at3_ops)),
    ]
