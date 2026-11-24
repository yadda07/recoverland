# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_machine_de_guerre_chaos - RecoverLand adversarial chaos regression.

Covers Phase B red-team edge cases across SQLite durability, live outbox
replay, clock skew, 3D Z/M NaN geometry keys, VSI fingerprints, URI
sanitization, Shapefile 10-char schema changes, virtual field skipping,
NoGeometry Review tables, CommitGuard foreign mutations on already-touched
FIDs, subsetString readback, vacuum singleton locking, and maintenance
dialog close lifecycle.
"""
from __future__ import annotations

import errno
import os
import shutil
import sqlite3
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_machine_de_guerre_chaos"
INVARIANT = "I-CHAOS-HARDENING"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_record(uid: str, ts: str, commit_id: str = ""):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.capture_outbox import event_to_record

    ev = AuditEvent(
        event_id=None,
        project_fingerprint="proj::chaos",
        datasource_fingerprint="ogr::/tmp/chaos.gpkg",
        layer_id_snapshot="lyr_1",
        layer_name_snapshot="chaos",
        provider_type="ogr",
        feature_identity_json='{"kind":"fid","fid":1}',
        operation_type="UPDATE",
        attributes_json='{"changed_only":{"a":{"old":1,"new":2}}}',
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="{}",
        user_name="tester",
        session_id="sess_chaos",
        created_at=ts,
        restored_from_event_id=None,
        entity_fingerprint="fid:1",
        event_schema_version=7,
        new_geometry_wkb=None,
        invalidated_at=None,
        event_uid=uid,
        commit_id=commit_id or None,
    )
    return event_to_record(ev)


def setup(ctx):
    ctx.data["tmp_dir"] = tempfile.mkdtemp(prefix="rl_chaos_")


def _check_live_outbox_sealed_replay(tmp: str) -> tuple:
    from recoverland.core.capture_outbox import (
        CaptureOutbox, replay_sealed_segments,
    )
    from recoverland.core.sqlite_schema import initialize_schema

    db = os.path.join(tmp, "outbox_live.sqlite")
    conn = sqlite3.connect(db)
    initialize_schema(conn)
    box = CaptureOutbox(db)
    try:
        ts = datetime.now(timezone.utc).isoformat()
        box.append_events("b1", [_make_record("u-live-1", ts, "c1")],
                          commit_id="c1", commit_state="COMMITTED")
        # Force rotation so prepare lives in a separate segment.
        if box._fh is not None:
            box._fh.close()
            box._fh = None
        box.append_prepare("c_open", {"layer_id": "l1", "layer_name": "L"})
        rep = replay_sealed_segments(box, conn)
        # Subsequent GUI write must still succeed on the live outbox.
        box.append_events("b2", [_make_record("u-live-2", ts, "c_open")],
                          commit_id="c_open", commit_state="COMMITTED")
        rows = conn.execute(
            "SELECT event_uid FROM audit_event WHERE event_uid = 'u-live-1'"
        ).fetchall()
        interrupted = conn.execute(
            "SELECT commit_id FROM capture_commit WHERE state = 'INTERRUPTED'"
        ).fetchall()
        ok = (
            len(rows) == 1
            and len(interrupted) == 0
            and os.path.isdir(box.dir)
            and rep.errors == []
        )
        return ok, f"rows={len(rows)} interrupted={len(interrupted)} errors={rep.errors}"
    finally:
        box.close()
        conn.close()


def _check_outbox_enospc_truncation(tmp: str) -> tuple:
    from recoverland.core.capture_outbox import (
        CaptureOutbox, OutboxWriteError, _parse_segment, list_segments,
    )

    db = os.path.join(tmp, "outbox_enospc.sqlite")
    box = CaptureOutbox(db)
    try:
        ts = datetime.now(timezone.utc).isoformat()
        box.append_events("b_ok", [_make_record("u-ok-1", ts)])

        orig_fh = box._fh

        class _FailingFh:
            def __init__(self, inner):
                self._inner = inner

            def tell(self):
                return self._inner.tell()

            def seek(self, pos):
                return self._inner.seek(pos)

            def truncate(self, pos):
                return self._inner.truncate(pos)

            def write(self, data):
                self._inner.write(data[:10])
                raise OSError(errno.ENOSPC, "No space left on device")

            def flush(self):
                return self._inner.flush()

            def fileno(self):
                return self._inner.fileno()

            def close(self):
                return self._inner.close()

        box._fh = _FailingFh(orig_fh)
        raised = False
        try:
            box.append_events("b_fail", [_make_record("u-fail-1", ts)])
        except OutboxWriteError:
            raised = True
        segs = list_segments(box.dir)
        torn_total = 0
        for _seq, p in segs:
            _lines, torn = _parse_segment(p)
            torn_total += len(torn)
        ok = raised and torn_total == 0 and len(segs) == 1
        return ok, f"raised={raised} torn={torn_total} segs={len(segs)}"
    finally:
        box.close()


def _check_retention_atomic_and_clock_guard(tmp: str) -> tuple:
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.journal_writes import insert_events_idempotent
    from recoverland.core.retention import (
        RetentionPolicy, PurgeOptions, purge_old_events_with_options,
        purge_floor, _purge_horizon,
    )

    _purge_horizon.clear()
    db = os.path.join(tmp, "ret_atomic.sqlite")
    conn = sqlite3.connect(db)
    try:
        initialize_schema(conn)
        old_ts = (datetime.now(timezone.utc) - timedelta(days=400)).isoformat()
        rec = _make_record("u-ret-1", old_ts)
        rec["datasource_fingerprint"] = "ogr::/tmp/ret_test.gpkg"
        with conn:
            insert_events_idempotent(conn, [rec])
        # Simulate SQLITE_FULL mid-DELETE via BEFORE DELETE trigger.
        conn.execute(
            "CREATE TRIGGER block_del BEFORE DELETE ON audit_event "
            "BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END"
        )
        res_err = purge_old_events_with_options(
            conn, RetentionPolicy(retention_days=30, max_events=100000),
            PurgeOptions(retention=True))
        floor_after_err = purge_floor("ogr::/tmp/ret_test.gpkg", conn)
        conn.execute("DROP TRIGGER block_del")
        # Auto-purge forward clock jump guard (cutoff > MAX(created_at)).
        res_auto = purge_old_events_with_options(
            conn, RetentionPolicy(retention_days=30, max_events=100000),
            PurgeOptions(retention=True), is_auto=True)
        remaining = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        ok = (
            bool(res_err.error)
            and not floor_after_err
            and res_auto.deleted_count == 0
            and remaining == 1
        )
        return ok, (f"err={bool(res_err.error)} floor={floor_after_err!r} "
                    f"auto_del={res_auto.deleted_count} rem={remaining}")
    finally:
        _purge_horizon.clear()
        conn.close()


def _check_zm_nan_geometry_keys() -> tuple:
    from qgis.core import QgsGeometry, QgsLineString, QgsPoint, QgsPolygon
    from recoverland.core.commit_guard import _vertex_key, geometry_equal

    def _mk_poly(pts):
        ls = QgsLineString([QgsPoint(x, y, z) for x, y, z in pts])
        poly = QgsPolygon()
        poly.setExteriorRing(ls)
        return QgsGeometry(poly)

    nan = float("nan")
    g1 = _mk_poly([(0, 0, 1.0), (10, 0, nan), (10, 10, 2.0), (0, 10, 1.0), (0, 0, 1.0)])
    g2 = _mk_poly([(0, 0, 1.0), (0, 10, 1.0), (10, 10, 2.0), (10, 0, nan), (0, 0, 1.0)])
    g3 = _mk_poly([(50, 50, 1.0), (60, 50, nan), (60, 60, 2.0), (50, 60, 1.0), (50, 50, 1.0)])
    k1 = _vertex_key(g1)
    k2 = _vertex_key(g2)
    k3 = _vertex_key(g3)
    eq12 = geometry_equal(g1, g2)
    eq13 = geometry_equal(g1, g3)
    ok = (
        not g1.isNull()
        and k1 is not None
        and k1 == k2
        and k1 != k3
        and eq12 is True
        and eq13 is False
    )
    return ok, (f"null={g1.isNull()} k1={k1 is not None} k1==k2={k1 == k2} "
                f"k1!=k3={k1 != k3} eq12={eq12} eq13={eq13}")


def _check_vsi_and_uri_sanitization() -> tuple:
    from recoverland.core.identity import _is_filesystem_path
    from recoverland.core.datasource_registry import _strip_password_from_uri
    from recoverland.recover import _sanitize_project_label

    vsi_ok = not _is_filesystem_path("/vsizip/C:/data/archive.zip/parcels.shp")
    win_path = r"C:\Users\alice\proj.qgz"
    label_path = _sanitize_project_label(win_path)
    label_url = _sanitize_project_label(
        "postgresql://alice:s3cr3t@host:5432/gis")
    pg_uri = (
        "dbname='gis' host=localhost port=5432 user='alice' "
        "password='secret' authcfg=abc1234 table=\"public\".\"t\" (geom)"
    )
    stripped_pg = _strip_password_from_uri(pg_uri, "postgres")
    ok = (
        vsi_ok
        and label_path == win_path
        and "s3cr3t" not in label_url
        and "postgresql://" in label_url
        and "authcfg=abc1234" in stripped_pg
        and "secret" not in stripped_pg
    )
    return ok, (f"vsi={vsi_ok} path={label_path!r} url={label_url!r} "
                f"pg={stripped_pg!r}")


def _check_shapefile_field_calc_and_virtual_skip(tmp: str) -> tuple:
    from qgis.core import (
        QgsVectorLayer, QgsField, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    from qgis.PyQt.QtCore import QCoreApplication, QMetaType
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.serialization import iter_mapped_attributes

    int_type = getattr(getattr(QMetaType, "Type", QMetaType), "Int")

    gpkg_path = os.path.join(tmp, "fc_test.gpkg")
    shp_path = os.path.join(tmp, "fc_test.shp")
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=code:string(10)", "s", "memory")
    mem.startEditing()
    f = QgsFeature(mem.fields())
    f.setAttributes(["A1"])
    f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0, 2.0)))
    mem.addFeature(f)
    mem.commitChanges()
    opts_gpkg = QgsVectorFileWriter.SaveVectorOptions()
    opts_gpkg.driverName = "GPKG"
    opts_gpkg.layerName = "fc_layer"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, gpkg_path, QgsCoordinateTransformContext(), opts_gpkg)
    opts_shp = QgsVectorFileWriter.SaveVectorOptions()
    opts_shp.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, shp_path, QgsCoordinateTransformContext(), opts_shp)

    lyr = QgsVectorLayer(f"{gpkg_path}|layername=fc_layer", "fc_gpkg", "ogr")
    shp_lyr = QgsVectorLayer(shp_path, "fc_shp", "ogr")
    captured = []

    class _DummyQueue:
        def enqueue(self, events, commit=None, journal_path=None):
            captured.extend(events)
            return True

        def prepare_commit(self, *args, **kwargs):
            return True

    statuses = []
    tracker = EditSessionTracker(_DummyQueue(), None)
    tracker.set_capture_status_callback(lambda k, d: statuses.append((k, d)))
    tracker.activate()
    tracker.connect_layer(lyr)
    tracker.connect_layer(shp_lyr)
    try:
        # 1. GPKG Field Calculator: create new field + populate in same commit.
        lyr.startEditing()
        lyr.addAttribute(QgsField("calculated_field_long", int_type))
        idx = lyr.fields().indexOf("calculated_field_long")
        fid = next(lyr.getFeatures()).id()
        lyr.changeAttributeValue(fid, idx, 42)
        committed = lyr.commitChanges()
        QCoreApplication.processEvents()

        # 2. _read_back_changed_values must handle a missing/truncated Shapefile
        # field name without raising KeyError.
        shp_fid = next(shp_lyr.getFeatures()).id()
        fake_changes = {shp_fid: {0: "A1", 1: 7}}
        unv = tracker._read_back_changed_values(
            shp_lyr, fake_changes, ["code", "very_long_calculated_name"])
        QCoreApplication.processEvents()

        # 3. Virtual expression field must be skipped by iter_mapped_attributes.
        v_idx = lyr.addExpressionField("1 + 1", QgsField("virt_col", int_type))
        yielded = list(iter_mapped_attributes(
            {"virt_col": "virt_col", "code": "code"},
            {"virt_col": 99, "code": "A1"},
            lyr.fields(),
            event_schema_version=7,
        ))
        yielded_names = [lyr.fields().at(i).name() for i, _v in yielded]
        unverified_hits = [k for k, _d in statuses if k == "update_values_unverified"]
        slot_fails = [k for k, _d in statuses if k == "slot_failed"]
        has_new_schema = any(
            "calculated_field_long" in (e.field_schema_json or "") for e in captured
        )
        ok = (
            committed
            and v_idx >= 0
            and "virt_col" not in yielded_names
            and "code" in yielded_names
            and not unverified_hits
            and not slot_fails
            and has_new_schema
            and "very_long_calculated_name" in unv.get(shp_fid, [])
        )
        return ok, (f"committed={committed} yielded={yielded_names} "
                    f"unverified={unverified_hits} slot_fails={slot_fails} "
                    f"has_new_schema={has_new_schema} unv={unv}")
    finally:
        tracker.disconnect_all()
        del shp_lyr
        del lyr


def _check_nogeom_review_and_subset_and_foreign_edit(tmp: str) -> tuple:
    from qgis.core import (
        QgsProject, QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    from recoverland.core.snapshot_overlay_session import (
        _LayerWatch, SnapshotOverlaySession,
    )
    from recoverland.core.commit_guard import CommitGuard, _fresh_layer

    gpkg_tbl = os.path.join(tmp, "chaos_tbl.gpkg")
    gpkg_pts = os.path.join(tmp, "chaos_pts.gpkg")
    # 1. NoGeometry table
    tbl = QgsVectorLayer("None?field=label:string(20)", "tbl_src", "memory")
    tbl.startEditing()
    tf = QgsFeature(tbl.fields())
    tf.setAttributes(["row1"])
    tbl.addFeature(tf)
    tbl.commitChanges()
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "plain_table"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        tbl, gpkg_tbl, QgsCoordinateTransformContext(), opts)

    # 2. Spatial table for subsetString + CommitGuard external mutation
    pts = QgsVectorLayer("Point?crs=EPSG:4326&field=code:string(10)", "pts_src", "memory")
    pts.startEditing()
    for code, x in (("A", 1.0), ("B", 2.0)):
        pf = QgsFeature(pts.fields())
        pf.setAttributes([code])
        pf.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, 0.0)))
        pts.addFeature(pf)
    pts.commitChanges()
    opts2 = QgsVectorFileWriter.SaveVectorOptions()
    opts2.driverName = "GPKG"
    opts2.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        pts, gpkg_pts, QgsCoordinateTransformContext(), opts2)

    proj = QgsProject.instance()
    tbl_lyr = QgsVectorLayer(f"{gpkg_tbl}|layername=plain_table", "plain_table", "ogr")
    pts_lyr = QgsVectorLayer(f"{gpkg_pts}|layername=pts", "pts", "ogr")
    proj.addMapLayer(tbl_lyr)
    proj.addMapLayer(pts_lyr)
    try:
        # _LayerWatch keeps NoGeometry layer instead of returning None
        watch = _LayerWatch(proj, {"fp_tbl": {"layer_id": tbl_lyr.id(), "layer_name": "plain_table"}})
        watch_ok = watch.now("fp_tbl") is not None
        sess = SnapshotOverlaySession()
        null_err = sess._write_null_geometries(tbl_lyr, tbl_lyr, "plain_table")

        # _fresh_layer strips subsetString
        pts_lyr.setSubsetString("code = 'A'")
        filtered_cnt = pts_lyr.featureCount()
        fresh = _fresh_layer(pts_lyr)
        unfiltered_cnt = fresh.featureCount() if fresh is not None else -1
        del fresh
        pts_lyr.setSubsetString("")

        # CommitGuard catches external edit on an ALREADY-touched FID
        pts_lyr.startEditing()
        guard = CommitGuard(pts_lyr)
        fids = [feat.id() for feat in pts_lyr.getFeatures()]
        target_fid = fids[0]
        col_idx = pts_lyr.fields().indexOf("code")
        guard.begin_action(100)
        pts_lyr.changeAttributeValue(target_fid, col_idx, "X")
        guard.end_action(100, extra_fids=[target_fid])
        foreign_before = guard.foreign_changes()
        # External mutation on the SAME already-touched FID outside begin/end:
        pts_lyr.changeAttributeValue(target_fid, col_idx, "Y_EXTERNAL")
        foreign_after = guard.foreign_changes()
        guard.detach()
        pts_lyr.rollBack()

        ok = (
            watch_ok
            and null_err == ""
            and filtered_cnt == 1
            and unfiltered_cnt == 2
            and foreign_before == []
            and target_fid in foreign_after
        )
        return ok, (f"watch={watch_ok} null_err={null_err!r} filt={filtered_cnt} "
                    f"unfilt={unfiltered_cnt} fb={foreign_before} fa={foreign_after}")
    finally:
        proj.removeMapLayer(tbl_lyr.id())
        proj.removeMapLayer(pts_lyr.id())


def _check_vacuum_singleton_and_maint_close(tmp: str) -> tuple:
    from recoverland.core import retention
    from recoverland.core.journal_manager import JournalManager
    from recoverland.journal_maintenance import JournalMaintenanceDialog

    # 1. Holding _vacuum_lock must NOT overwrite _vacuum_thread
    sentinel_thread = threading.Thread(target=lambda: None, name="sentinel")
    prev_thread = retention._vacuum_thread
    retention._vacuum_thread = sentinel_thread
    cb_results = []
    acquired = retention._vacuum_lock.acquire(blocking=False)
    try:
        retention.vacuum_async(
            os.path.join(tmp, "dummy.sqlite"),
            callback=lambda ok: cb_results.append(ok),
        )
        thread_preserved = retention._vacuum_thread is sentinel_thread
    finally:
        if acquired:
            retention._vacuum_lock.release()
        retention._vacuum_thread = prev_thread

    # 2. Open and close JournalMaintenanceDialog via reject() and close() without VACUUM
    jm = JournalManager()
    db_path = os.path.join(tmp, "maint_close.sqlite")
    jm.open_for_project("", tmp)
    maint_ok = False
    try:
        dlg = JournalMaintenanceDialog(jm)
        dlg.reject()
        dlg.close()
        dlg.deleteLater()
        maint_ok = True
    finally:
        jm.close()
        if os.path.exists(db_path):
            os.remove(db_path)

    ok = acquired and thread_preserved and cb_results == [False] and maint_ok
    return ok, f"acq={acquired} preserved={thread_preserved} cb={cb_results} maint={maint_ok}"


def run(ctx):
    tmp = ctx.data["tmp_dir"]
    ctx.data["live_outbox"] = _check_live_outbox_sealed_replay(tmp)
    ctx.data["enospc_trunc"] = _check_outbox_enospc_truncation(tmp)
    ctx.data["retention_atomic"] = _check_retention_atomic_and_clock_guard(tmp)
    ctx.data["zm_nan"] = _check_zm_nan_geometry_keys()
    ctx.data["vsi_uri"] = _check_vsi_and_uri_sanitization()
    ctx.data["shp_fc_virt"] = _check_shapefile_field_calc_and_virtual_skip(tmp)
    ctx.data["nogeom_subset_foreign"] = _check_nogeom_review_and_subset_and_foreign_edit(tmp)
    ctx.data["vac_and_maint"] = _check_vacuum_singleton_and_maint_close(tmp)


def teardown(ctx):
    tmp = ctx.data.get("tmp_dir")
    if tmp and os.path.isdir(tmp):
        shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    out = []
    for name in (
        "live_outbox",
        "enospc_trunc",
        "retention_atomic",
        "zm_nan",
        "vsi_uri",
        "shp_fc_virt",
        "nogeom_subset_foreign",
        "vac_and_maint",
    ):
        ok, detail = ctx.data.get(name, (False, "not run"))
        out.append((name, bool(ok), detail))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
