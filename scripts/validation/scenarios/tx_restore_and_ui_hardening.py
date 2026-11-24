# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_restore_and_ui_hardening - RecoverLand validation runtime.

Invariant: Qt5/Qt6 compat helpers, 3D Z/M geometry equality, strict bool-vs-int
value equality, WKB envelope edge cases, rewind_dedup NULL-geom safety,
search_service malformed JSON + pagination clamping, >1000-ID chunked fetch,
HTML tooltip escaping, ISO offset parsing, and UI widget guards all hold.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_restore_and_ui_hardening"
INVARIANT = "I-RESTORE-UI-HARDENING"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def setup(ctx):
    ctx.data["tmp_dir"] = tempfile.mkdtemp(prefix="rl_restore_ui_")


def run(ctx):
    from qgis.PyQt.QtCore import QCoreApplication, QDate
    from qgis.core import QgsFeature, QgsGeometry
    from recoverland.compat import installed_font_families, utc_qtimezone
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.commit_guard import geometry_equal, values_equal
    from recoverland.core.event_stream_repository import fetch_events_by_ids
    from recoverland.core.geometry_utils import feature_matches_geometry
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.logger import flog
    from recoverland.core.rewind_dedup import GEOMETRY_NULL_MARKER, _states_equal
    from recoverland.core.search_service import (
        SearchCriteria,
        is_geometry_only_update,
        reconstruct_attributes,
        reconstruct_new_attributes,
        search_events,
    )
    from recoverland.core.serialization import serialize_value
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.wkb_envelope import parse_envelope
    from recoverland.recover_dialog import RecoverDialog
    from recoverland.widgets.temporal_timeline_widget import TemporalTimelineWidget
    from recoverland.widgets.toggle_switch import AppleToggleSwitch

    tmp_dir = ctx.data["tmp_dir"]
    flog(f"tx_restore_and_ui_hardening: start trace_id={ctx.trace_id}")
    res = {}

    try:
        # 1. Compat helpers & serialization
        tz = utc_qtimezone()
        res["utc_tz_valid"] = tz is not None and tz.isValid()
        fams = installed_font_families()
        res["font_families_nonempty"] = isinstance(fams, (set, list)) and len(fams) > 0

        ser_mv = serialize_value(memoryview(b"abc"))
        ser_dt = serialize_value(QDate(2026, 3, 15))
        ser_json = json.dumps({"mv": ser_mv, "dt": ser_dt})
        parsed_ser = json.loads(ser_json)
        res["serialize_mv_and_qdate_ok"] = (
            isinstance(parsed_ser, dict)
            and parsed_ser.get("mv") is not None
            and "2026-03-15" in str(parsed_ser.get("dt", ""))
        )

        # 2. ISO datetime parsing with positive and negative UTC offsets
        dt_neg = RecoverDialog._parse_iso_datetime("2026-01-01T10:00:00-05:00")
        dt_pos = RecoverDialog._parse_iso_datetime("2026-01-01T10:00:00+02:00")
        res["iso_neg_offset_utc"] = dt_neg.toUTC().toString("yyyy-MM-ddTHH:mm:ss")
        res["iso_pos_offset_utc"] = dt_pos.toUTC().toString("yyyy-MM-ddTHH:mm:ss")

        # 3. parse_envelope on POINT EMPTY, valid Point, and CircularString
        empty_pt = QgsGeometry.fromWkt("POINT EMPTY")
        empty_wkb = bytes(empty_pt.asWkb()) if not empty_pt.isNull() else b""
        res["empty_pt_envelope_none"] = (
            parse_envelope(empty_wkb) is None if empty_wkb else True
        )
        curve = QgsGeometry.fromWkt("CircularString(0 0, 1 1, 2 0)")
        curve_env = parse_envelope(bytes(curve.asWkb()))
        pt_env = parse_envelope(bytes(QgsGeometry.fromWkt("Point(3 4)").asWkb()))
        res["curve_envelope_valid"] = (
            curve_env is None and pt_env == (3.0, 4.0, 3.0, 4.0)
        )

        # 4. Strict bool-vs-int & 3D Z geometry equality
        res["bool_not_equal_int"] = (
            values_equal(True, 1) is False
            and values_equal(False, 0) is False
            and values_equal(1, 1.0) is True
        )
        g_z1 = QgsGeometry.fromWkt("PointZ (1 2 10)")
        g_z2 = QgsGeometry.fromWkt("PointZ (1 2 20)")
        g_z1_dup = QgsGeometry.fromWkt("PointZ (1 2 10)")
        line_z1 = QgsGeometry.fromWkt("LineStringZ (0 0 1, 1 1 2)")
        line_z2 = QgsGeometry.fromWkt("LineStringZ (0 0 1, 1 1 9)")
        poly_z1 = QgsGeometry.fromWkt("PolygonZ ((0 0 1, 2 0 1, 2 2 1, 0 0 1))")
        poly_z2 = QgsGeometry.fromWkt("PolygonZ ((0 0 1, 2 0 1, 2 2 5, 0 0 1))")
        f_z1 = QgsFeature()
        f_z1.setGeometry(g_z1)
        f_poly_z1 = QgsFeature()
        f_poly_z1.setGeometry(poly_z1)
        res["geom_z_diff_detected"] = (
            geometry_equal(g_z1, g_z2) is False
            and geometry_equal(g_z1, g_z1_dup) is True
            and geometry_equal(line_z1, line_z2) is False
            and geometry_equal(poly_z1, poly_z2) is False
            and feature_matches_geometry(f_z1, g_z2) is False
            and feature_matches_geometry(f_z1, g_z1_dup) is True
            and feature_matches_geometry(f_poly_z1, poly_z2) is False
        )

        # 5. rewind_dedup NULL geom never matches non-null geom
        res["dedup_null_geom_strict"] = (
            _states_equal(({"a": 1}, None), ({"a": 1}, b"\x01\x01")) is False
            and _states_equal(({"a": 1}, None), ({"a": 1}, None)) is True
            and _states_equal(({"a": True}, None), ({"a": 1}, None)) is False
            and GEOMETRY_NULL_MARKER is not None
        )

        # 6. search_service malformed JSON + clamped pagination + >1000 IDs fetch
        db_path = os.path.join(tmp_dir, "search_hard.sqlite")
        conn = sqlite3.connect(db_path)
        try:
            initialize_schema(conn)
            ensure_v7_schema(conn)
            now_iso = datetime.now(timezone.utc).isoformat()
            rows = [
                ("p", "ogr::/tmp/x.gpkg", "L1", "layer_x", "ogr", "{}",
                 "UPDATE", '{"changed_only":{"v":{"old":1,"new":2}}}',
                 "Point", "u", now_iso, f"fid:{i}")
                for i in range(1, 1051)
            ]
            conn.executemany(
                "INSERT INTO audit_event ("
                "project_fingerprint, datasource_fingerprint, layer_id_snapshot, "
                "layer_name_snapshot, provider_type, feature_identity_json, "
                "operation_type, attributes_json, geometry_type, user_name, "
                "created_at, entity_fingerprint) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
            sr = search_events(
                conn,
                SearchCriteria(None, None, None, None, None, None,
                               page=0, page_size=-10),
            )
            res["search_clamped_ok"] = (
                sr.page == 1 and sr.page_size == 1 and len(sr.events) == 1
            )
            fetched_1050 = fetch_events_by_ids(conn, list(range(1, 1051)))
            res["fetch_1050_count"] = len(fetched_1050)
        finally:
            conn.close()

        bad_evt = AuditEvent(
            event_id=1, project_fingerprint="p", datasource_fingerprint="ogr::x",
            layer_id_snapshot="L", layer_name_snapshot="<script>x</script>",
            provider_type="ogr", feature_identity_json="{}",
            operation_type="UPDATE", attributes_json="[1,2,3]",
            geometry_wkb=None, geometry_type="Point", crs_authid="EPSG:4326",
            field_schema_json="{}", user_name="u", session_id="s",
            created_at="2026-01-01T00:00:00", restored_from_event_id=None,
            entity_fingerprint="fid:1", event_schema_version=2,
            new_geometry_wkb=None, invalidated_at=None,
        )
        res["malformed_json_safe"] = (
            reconstruct_attributes(bad_evt) == {}
            and reconstruct_new_attributes(bad_evt) == {}
            and is_geometry_only_update(bad_evt) is False
        )

        # 7. Tooltip HTML escaping + widget guards
        class _TrStub:
            def tr(self, s):
                return s

        tip = RecoverDialog._build_event_diff_tooltip(
            _TrStub(), bad_evt,
            {"changed_only": {"<b>k</b>": {"old": "<img src=x>", "new": "a&b"}}},
            {}, {"<b>k</b>"}, False,
        )
        res["tooltip_escaped"] = (
            "<script>" not in tip
            and "&lt;script&gt;" in tip
            and "<img" not in tip
            and "&lt;img" in tip
        )

        tl = TemporalTimelineWidget()
        tl._pan_by(20.0)
        tl._zoom_at(1.2, 50.0)
        res["timeline_pan_before_range_safe"] = True
        tl.deleteLater()

        tog = AppleToggleSwitch()
        res["toggle_initial_state_matches_tip"] = (
            tog.isChecked() is False and "desactive" in tog.toolTip().lower()
        )
        tog.deleteLater()
        QCoreApplication.processEvents()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    flog(f"tx_restore_and_ui_hardening: done trace_id={ctx.trace_id}")
    ctx.data["res"] = res


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    r = ctx.data.get("res") or {}
    return [
        (
            "compat_and_serialization_ok",
            (r.get("utc_tz_valid") is True
             and r.get("font_families_nonempty") is True
             and r.get("serialize_mv_and_qdate_ok") is True),
            f"tz={r.get('utc_tz_valid')} fams={r.get('font_families_nonempty')} "
            f"ser={r.get('serialize_mv_and_qdate_ok')}",
        ),
        (
            "iso_offsets_converted_to_utc",
            (r.get("iso_neg_offset_utc") == "2026-01-01T15:00:00"
             and r.get("iso_pos_offset_utc") == "2026-01-01T08:00:00"),
            f"neg={r.get('iso_neg_offset_utc')} pos={r.get('iso_pos_offset_utc')}",
        ),
        (
            "wkb_envelope_empty_and_curve",
            (r.get("empty_pt_envelope_none") is True
             and r.get("curve_envelope_valid") is True),
            f"empty={r.get('empty_pt_envelope_none')} curve={r.get('curve_envelope_valid')}",
        ),
        (
            "strict_bool_and_3d_z_geometry",
            (r.get("bool_not_equal_int") is True
             and r.get("geom_z_diff_detected") is True
             and r.get("dedup_null_geom_strict") is True),
            f"bool={r.get('bool_not_equal_int')} z={r.get('geom_z_diff_detected')} "
            f"dedup={r.get('dedup_null_geom_strict')}",
        ),
        (
            "search_clamping_malformed_json_and_chunked_1050_fetch",
            (r.get("search_clamped_ok") is True
             and r.get("fetch_1050_count") == 1050
             and r.get("malformed_json_safe") is True),
            f"clamp={r.get('search_clamped_ok')} fetch={r.get('fetch_1050_count')} "
            f"json={r.get('malformed_json_safe')}",
        ),
        (
            "ui_tooltip_escaping_and_widget_guards",
            (r.get("tooltip_escaped") is True
             and r.get("timeline_pan_before_range_safe") is True
             and r.get("toggle_initial_state_matches_tip") is True),
            f"tip={r.get('tooltip_escaped')} tl={r.get('timeline_pan_before_range_safe')} "
            f"tog={r.get('toggle_initial_state_matches_tip')}",
        ),
        assert_log_contains(
            ctx.records,
            rf"tx_restore_and_ui_hardening.*trace_id={ctx.trace_id}",
            name="trace_id_propagated",
            min_count=2,
        ),
    ]


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
