# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_sqlite_durability_hardening - RecoverLand validation runtime.

Invariant: SQLite durability, schema safety, corruption classification,
read-only PRAGMAs, UNC URI encoding, explicit PurgeOptions(retention=False),
and WriteQueue health/drain/poison-isolation invariants hold under faults.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_sqlite_durability_hardening"
INVARIANT = "I-SQLITE-DURABILITY"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_event(eid_tag: str, created_at: str, commit_id: str = None,
                geometry_wkb=None):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=None,
        project_fingerprint="proj::test",
        datasource_fingerprint="ogr::/tmp/test.gpkg",
        layer_id_snapshot="lyr_1",
        layer_name_snapshot="test",
        provider_type="ogr",
        feature_identity_json=f'{{"kind":"fid","fid":{eid_tag}}}',
        operation_type="UPDATE",
        attributes_json='{"changed_only":{"a":{"old":1,"new":2}}}',
        geometry_wkb=geometry_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="{}",
        user_name="tester",
        session_id="sess_1",
        created_at=created_at,
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{eid_tag}",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
        event_uid=f"uid-{eid_tag}-{created_at}",
        commit_id=commit_id,
    )


def setup(ctx):
    ctx.data["tmp_dir"] = tempfile.mkdtemp(prefix="rl_sqlite_hard_")


def run(ctx):
    from recoverland.core.capture_outbox import CaptureOutbox, replay_outbox
    from recoverland.core.integrity import _is_corruption_error, check_journal_integrity
    from recoverland.core.journal_manager import JournalManager, sqlite_readonly_uri
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.logger import flog
    from recoverland.core.retention import (
        PurgeOptions,
        RetentionPolicy,
        purge_old_events_with_options,
    )
    from recoverland.core.sqlite_schema import (
        NewerSchemaError,
        apply_pragmas,
        initialize_schema,
    )
    from recoverland.core.write_queue import CommitInfo, WriteQueue

    tmp_dir = ctx.data["tmp_dir"]
    flog(f"tx_sqlite_durability_hardening: start trace_id={ctx.trace_id}")
    res = {}

    try:
        # 1. Outbox INTERRUPTED commit durability on unresolved prepare
        db_outbox = os.path.join(tmp_dir, "outbox_test.sqlite")
        conn = sqlite3.connect(db_outbox)
        try:
            initialize_schema(conn)
            ensure_v7_schema(conn)
            ob = CaptureOutbox(db_outbox)
            ob.append_prepare(
                "cid_interrupted_1",
                {
                    "layer_id": "L1",
                    "layer_name": "parcels",
                    "datasource_fingerprint": "ogr::/tmp/parcels.gpkg",
                    "session_id": "s1",
                },
            )
            ob.close()
            rep = replay_outbox(db_outbox, conn)
            row = conn.execute(
                "SELECT state FROM capture_commit WHERE commit_id = ?",
                ("cid_interrupted_1",),
            ).fetchone()
            res["outbox_interrupted_state"] = row[0] if row else None
            res["outbox_interrupted_count"] = len(rep.interrupted_commits)
        finally:
            conn.close()

        # 2. PurgeOptions(retention=False, invalidated=True) preserves old rows
        db_purge = os.path.join(tmp_dir, "purge_test.sqlite")
        conn = sqlite3.connect(db_purge)
        try:
            initialize_schema(conn)
            ensure_v7_schema(conn)
            old_ts = (datetime.now(timezone.utc) - timedelta(days=400)).isoformat()
            now_ts = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "INSERT INTO audit_event ("
                "project_fingerprint, datasource_fingerprint, layer_id_snapshot, "
                "layer_name_snapshot, provider_type, feature_identity_json, "
                "operation_type, attributes_json, geometry_type, user_name, "
                "created_at, entity_fingerprint) VALUES "
                "('p','ogr::x','l','x','ogr','{}','UPDATE','{}','Point','u',?,'fid:1')",
                (old_ts,),
            )
            conn.execute(
                "INSERT INTO audit_event ("
                "project_fingerprint, datasource_fingerprint, layer_id_snapshot, "
                "layer_name_snapshot, provider_type, feature_identity_json, "
                "operation_type, attributes_json, geometry_type, user_name, "
                "created_at, entity_fingerprint, invalidated_at) VALUES "
                "('p','ogr::x','l','x','ogr','{}','UPDATE','{}','Point','u',?,'fid:2',?)",
                (now_ts, now_ts),
            )
            conn.commit()
            pr = purge_old_events_with_options(
                conn,
                RetentionPolicy(retention_days=30, max_events=100),
                PurgeOptions(retention=False, invalidated=True),
            )
            remaining = conn.execute(
                "SELECT COUNT(*) FROM audit_event"
            ).fetchone()[0]
            res["purge_retention_deleted"] = pr.retention_deleted
            res["purge_invalidated_deleted"] = pr.invalidated_deleted
            res["purge_old_preserved"] = remaining == 1
        finally:
            conn.close()

        # 3. Malformed feature_identity_json backfill terminates + v8 NewerSchemaError
        db_schema = os.path.join(tmp_dir, "schema_test.sqlite")
        conn = sqlite3.connect(db_schema)
        try:
            initialize_schema(conn)
            conn.execute(
                "INSERT INTO audit_event ("
                "project_fingerprint, datasource_fingerprint, layer_id_snapshot, "
                "layer_name_snapshot, provider_type, feature_identity_json, "
                "operation_type, attributes_json, geometry_type, user_name, "
                "created_at, entity_fingerprint) VALUES "
                "('p','ogr::x','l','x','ogr','{broken_json','UPDATE','{}','Point','u',?,NULL)",
                (datetime.now(timezone.utc).isoformat(),),
            )
            conn.commit()
            ensure_v7_schema(conn)
            res["malformed_backfill_terminated"] = True
        finally:
            conn.close()

        v8_dir = os.path.join(tmp_dir, "v8_proj")
        os.makedirs(os.path.join(v8_dir, ".recoverland"), exist_ok=True)
        proj_v8 = os.path.join(v8_dir, "future.qgz")
        with open(proj_v8, "wb") as fh:
            fh.write(b"")
        db_v8 = os.path.join(v8_dir, ".recoverland", "recoverland_audit.sqlite")
        conn = sqlite3.connect(db_v8)
        try:
            initialize_schema(conn)
            conn.execute(
                "INSERT INTO schema_version (version_number, applied_at, description) "
                "VALUES (8, ?, 'future')",
                (datetime.now(timezone.utc).isoformat(),),
            )
            conn.commit()
        finally:
            conn.close()

        jm_v8 = JournalManager()
        raised_newer = False
        try:
            jm_v8.open_for_project(proj_v8, tmp_dir)
        except NewerSchemaError:
            raised_newer = True
        finally:
            if jm_v8.is_open:
                jm_v8.close()
        res["v8_raises_newer_schema"] = raised_newer

        # 4. apply_pragmas(read_only=True) on local DELETE-mode DB
        db_ro = os.path.join(tmp_dir, "ro_delete.sqlite")
        conn_w = sqlite3.connect(db_ro)
        try:
            conn_w.execute("PRAGMA journal_mode=DELETE")
            conn_w.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            conn_w.commit()
        finally:
            conn_w.close()
        ro_ok = False
        conn_r = sqlite3.connect(sqlite_readonly_uri(db_ro), uri=True)
        try:
            apply_pragmas(conn_r, read_only=True)
            conn_r.execute("SELECT COUNT(*) FROM t").fetchone()
            ro_ok = True
        finally:
            conn_r.close()
        res["ro_delete_pragmas_ok"] = ro_ok

        # 5. is_corrupt classification
        res["busy_not_corrupt"] = (
            _is_corruption_error(sqlite3.OperationalError("database is locked"))
            is False
        )
        res["readonly_not_corrupt"] = (
            _is_corruption_error(
                sqlite3.OperationalError("attempt to write a readonly database")
            )
            is False
        )
        res["malformed_is_corrupt"] = (
            _is_corruption_error(
                sqlite3.DatabaseError("database disk image is malformed")
            )
            is True
        )

        db_bad = os.path.join(tmp_dir, "corrupt.sqlite")
        with open(db_bad, "wb") as fh:
            fh.write(b"NOT A SQLITE DATABASE HEADER" * 200)
        res_bad = check_journal_integrity(db_bad)
        res["corrupt_file_flagged"] = (
            res_bad.is_healthy is False and res_bad.is_corrupt is True
        )

        # 6. UNC URI encoding (Windows normalizes //server/share to \\server\share)
        unc_win = sqlite_readonly_uri(r"\\fileserver\gis_share\proj\audit.sqlite")
        unc_fwd = sqlite_readonly_uri("//fileserver/gis_share/proj/audit.sqlite")
        res["unc_win_uri"] = unc_win
        res["unc_fwd_uri"] = unc_fwd
        expected_unc = (
            r"file:\\fileserver\gis_share\proj\audit.sqlite?mode=ro"
            if os.name == "nt"
            else "file://fileserver/gis_share/proj/audit.sqlite?mode=ro"
        )
        res["unc_uri_valid"] = (
            unc_win == expected_unc and unc_fwd == expected_unc
        )

        # 7. WriteQueue: dead writer drain=False, idle watchdog != stalled,
        # poison isolation preserves CommitInfo
        wq_dir = os.path.join(tmp_dir, "wq_proj")
        os.makedirs(wq_dir, exist_ok=True)
        proj_wq = os.path.join(wq_dir, "wq_hard.qgz")
        with open(proj_wq, "wb") as fh:
            fh.write(b"")
        jm = JournalManager()
        db_wq = jm.open_for_project(proj_wq, tmp_dir)
        wq = WriteQueue()
        try:
            wq.start(db_wq)
            # Idle queue with stale _last_flush_mono must NOT report stalled
            wq._last_flush_mono = time.monotonic() - 120.0
            wd = wq.watchdog_check()
            res["idle_watchdog_not_stalled"] = (
                wd.state != "degraded" and "stalled" not in (wd.last_persistence_error or "")
            )

            # Poison isolation preserves commit record alongside healthy event
            class _BoomBytes(bytes):
                def __conform__(self, _protocol):
                    raise RuntimeError("injected_poison")

            now_iso = datetime.now(timezone.utc).isoformat()
            cid = "cid_poison_mix"
            ev_ok = _make_event("101", now_iso, commit_id=cid)
            ev_bad = _make_event("102", now_iso, commit_id=cid,
                                 geometry_wkb=_BoomBytes(b"\x01\x01"))
            commit = CommitInfo(
                commit_id=cid,
                state="COMMITTED",
                layer_id="L1",
                layer_name="test",
                datasource_fingerprint="ogr::/tmp/test.gpkg",
                session_id="sess_1",
            )
            wq.enqueue([ev_ok, ev_bad], commit=commit)
            drained_mix = wq.wait_until_drained(timeout=8.0)
            res["poison_mix_drained"] = drained_mix

            rc = jm.create_read_connection()
            try:
                ev_n = rc.execute(
                    "SELECT COUNT(*) FROM audit_event WHERE entity_fingerprint='fid:101'"
                ).fetchone()[0]
                cm_row = rc.execute(
                    "SELECT state FROM capture_commit WHERE commit_id=?", (cid,)
                ).fetchone()
                res["poison_healthy_saved"] = ev_n == 1
                res["poison_commit_preserved"] = (cm_row is not None and cm_row[0] == "COMMITTED")
            finally:
                rc.close()

            # Dead writer + fallback unindexed batch => wait_until_drained returns False
            wq._stop_event.set()
            if wq._thread is not None:
                wq._thread.join(timeout=5.0)
            wq.enqueue([_make_event("103", now_iso)])
            res["dead_writer_drain_false"] = (
                wq.wait_until_drained(timeout=0.3) is False
            )
        finally:
            wq.stop()
            jm.close()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    flog(f"tx_sqlite_durability_hardening: done trace_id={ctx.trace_id}")
    ctx.data["res"] = res


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    r = ctx.data.get("res") or {}
    return [
        (
            "outbox_interrupted_commit_persisted",
            (r.get("outbox_interrupted_state") == "INTERRUPTED"
             and r.get("outbox_interrupted_count") == 1),
            f"state={r.get('outbox_interrupted_state')} n={r.get('outbox_interrupted_count')}",
        ),
        (
            "purge_options_retention_false_preserves_old",
            (r.get("purge_retention_deleted") == 0
             and r.get("purge_invalidated_deleted") == 1
             and r.get("purge_old_preserved") is True),
            f"ret={r.get('purge_retention_deleted')} inv={r.get('purge_invalidated_deleted')} "
            f"preserved={r.get('purge_old_preserved')}",
        ),
        (
            "malformed_backfill_and_v8_guard",
            (r.get("malformed_backfill_terminated") is True
             and r.get("v8_raises_newer_schema") is True),
            f"backfill={r.get('malformed_backfill_terminated')} v8={r.get('v8_raises_newer_schema')}",
        ),
        (
            "readonly_pragmas_on_delete_mode_db",
            r.get("ro_delete_pragmas_ok") is True,
            f"ro_ok={r.get('ro_delete_pragmas_ok')}",
        ),
        (
            "corruption_classification_accurate",
            (r.get("busy_not_corrupt") is True
             and r.get("readonly_not_corrupt") is True
             and r.get("malformed_is_corrupt") is True
             and r.get("corrupt_file_flagged") is True),
            f"busy={r.get('busy_not_corrupt')} ro={r.get('readonly_not_corrupt')} "
            f"malformed={r.get('malformed_is_corrupt')} file={r.get('corrupt_file_flagged')}",
        ),
        (
            "unc_readonly_uri_valid",
            r.get("unc_uri_valid") is True,
            f"win={r.get('unc_win_uri')} fwd={r.get('unc_fwd_uri')}",
        ),
        (
            "wq_idle_watchdog_and_dead_drain_and_poison_commit",
            (r.get("idle_watchdog_not_stalled") is True
             and r.get("poison_mix_drained") is True
             and r.get("poison_healthy_saved") is True
             and r.get("poison_commit_preserved") is True
             and r.get("dead_writer_drain_false") is True),
            f"idle={r.get('idle_watchdog_not_stalled')} mix={r.get('poison_mix_drained')} "
            f"ok={r.get('poison_healthy_saved')} commit={r.get('poison_commit_preserved')} "
            f"dead={r.get('dead_writer_drain_false')}",
        ),
        assert_log_contains(
            ctx.records,
            rf"tx_sqlite_durability_hardening.*trace_id={ctx.trace_id}",
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
