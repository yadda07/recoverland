# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_temp_memory_layer_silence - RecoverLand validation runtime.

Invariant: temporary/memory, virtual, managed overlay, read-only, and invalid
layers NEVER surface user-facing errors, alerts, or checkboxes in RecoverLand;
Make Permanent (memory -> GPKG via dataSourceChanged) auto-connects tracking;
and unchecking all layers persists the ["__NONE__"] sentinel cleanly.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_temp_memory_layer_silence"
INVARIANT = "I-TEMP-LAYER-SILENCE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(tmp_dir: str, name: str = "real_layer") -> str:
    from qgis.core import (
        QgsFeature,
        QgsGeometry,
        QgsPointXY,
        QgsProject,
        QgsVectorFileWriter,
        QgsVectorLayer,
    )

    path = os.path.join(tmp_dir, f"{name}.gpkg")
    src = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=nom:string(32)", name, "memory",
    )
    feat = QgsFeature(src.fields())
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.0, 48.0)))
    feat["nom"] = "init"
    src.dataProvider().addFeatures([feat])
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = name
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        src, path, QgsProject.instance().transformContext(), opts,
    )
    if err[0] != QgsVectorFileWriter.NoError:
        raise RuntimeError(f"GPKG write failed: {err}")
    return path


def _make_memory_layer(name: str):
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY, QgsVectorLayer

    lyr = QgsVectorLayer("Point?crs=EPSG:4326&field=nom:string(32)", name, "memory")
    feat = QgsFeature(lyr.fields())
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0, 1.0)))
    feat["nom"] = "scratch"
    lyr.dataProvider().addFeatures([feat])
    return lyr


def setup(ctx):
    ctx.data["tmp_dir"] = tempfile.mkdtemp(prefix="rl_temp_silence_")


def run(ctx):
    from qgis.PyQt.QtCore import QCoreApplication
    from qgis.core import QgsProject, QgsSettings, QgsVectorLayer
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.logger import flog
    from recoverland.core.restore_contracts import (
        AtomicityPolicy,
        ConflictPolicy,
        PlannedAction,
        RestoreMode,
        RestorePlan,
        RestoreScope,
    )
    from recoverland.core.restore_executor import preflight_layer_check
    from recoverland.core.support_policy import (
        SupportLevel,
        evaluate_layer_support,
        is_capture_supported,
        is_ephemeral_layer,
        is_restore_supported,
    )
    from recoverland.core.workflow_service import purge_lens_overlays
    from recoverland.core.write_queue import WriteQueue
    from recoverland.recover import RecoverPlugin

    tmp_dir = ctx.data["tmp_dir"]
    flog(f"tx_temp_memory_layer_silence: start trace_id={ctx.trace_id}")

    proj_path = os.path.join(tmp_dir, "silence_proj.qgz")
    with open(proj_path, "wb") as fh:
        fh.write(b"")
    gpkg_path = _make_gpkg(tmp_dir, "parcels")
    gpkg_make_perm = _make_gpkg(tmp_dir, "promoted")
    project = QgsProject.instance()
    settings = QgsSettings()
    saved_fp_setting = settings.value("RecoverLand/tracked_layer_fingerprints", None)

    journal = None
    wq = None
    tracker = None
    added_ids = []
    res = {}

    try:
        journal = JournalManager()
        journal_path = journal.open_for_project(proj_path, tmp_dir)
        wq = WriteQueue()
        wq.start(journal_path)
        tracker = EditSessionTracker(wq, journal)

        status_events = []
        tracker.set_capture_status_callback(
            lambda kind, detail: status_events.append((kind, dict(detail or {})))
        )

        # 1. Scratch memory layer
        mem = _make_memory_layer("user_scratch")
        res["mem_is_ephemeral"] = is_ephemeral_layer(mem)
        mem_eval = evaluate_layer_support(mem)
        res["mem_support_level"] = mem_eval.support_level == SupportLevel.INFORMATIONAL
        res["mem_capture_supported"] = is_capture_supported(mem)
        res["mem_restore_allowed"] = is_restore_supported(mem)

        # 2. Managed review overlay & lens overlay customProperty safety
        snap_overlay = _make_memory_layer("rl_snap_ov")
        snap_overlay.setCustomProperty("_rl_snap_managed", True)
        res["snap_overlay_ephemeral"] = is_ephemeral_layer(snap_overlay)

        lens_false = _make_memory_layer("not_lens")
        lens_false.setCustomProperty("_rl_lens_managed", "false")
        lens_true = _make_memory_layer("is_lens")
        lens_true.setCustomProperty("_rl_lens_managed", True)
        lens_false_id = lens_false.id()
        lens_true_id = lens_true.id()
        project.addMapLayer(lens_false, False)
        project.addMapLayer(lens_true, False)
        added_ids.extend([lens_false_id, lens_true_id])
        purged = purge_lens_overlays()
        res["purge_lens_count"] = purged
        res["lens_false_survived"] = project.mapLayer(lens_false_id) is not None
        res["lens_true_removed"] = project.mapLayer(lens_true_id) is None
        if project.mapLayer(lens_false_id) is not None:
            project.removeMapLayer(lens_false_id)
        added_ids = []

        # 3. Invalid / broken layer
        broken = QgsVectorLayer(
            os.path.join(tmp_dir, "does_not_exist.gpkg"), "broken_lyr", "ogr",
        )
        broken_eval = evaluate_layer_support(broken)
        res["broken_capture"] = broken_eval.capture
        res["broken_restore"] = broken_eval.restore

        # 4. Real GPKG layer + memory + overlay in project
        real = QgsVectorLayer(gpkg_path, "parcels", "ogr")
        project.addMapLayer(real, False)
        project.addMapLayer(mem, False)
        project.addMapLayer(snap_overlay, False)
        added_ids.extend([real.id(), mem.id(), snap_overlay.id()])

        tracker.activate()
        tracker.connect_layer(real)
        tracker.connect_layer(mem)
        tracker.connect_layer(snap_overlay)
        tracker.connect_layer(broken)
        res["real_connected"] = real.id() in tracker._connected_layers
        res["mem_connected"] = mem.id() in tracker._connected_layers
        res["broken_connect_res"] = broken.id() in tracker._connected_layers

        # Edit and commit the memory layer + snap overlay: zero status alerts!
        mem.startEditing()
        fid_mem = next(mem.getFeatures()).id()
        mem.changeAttributeValue(fid_mem, 0, "edited_scratch")
        mem.commitChanges()
        QCoreApplication.processEvents()

        snap_overlay.startEditing()
        fid_ov = next(snap_overlay.getFeatures()).id()
        snap_overlay.changeAttributeValue(fid_ov, 0, "edited_ov")
        snap_overlay.commitChanges()
        QCoreApplication.processEvents()

        mem_alerts = [
            ev for ev in status_events
            if ev[0] == "untracked_edit"
            and ev[1].get("layer_name") in ("user_scratch", "rl_snap_ov")
        ]
        res["mem_untracked_alerts"] = len(mem_alerts)

        # 5. Make Permanent simulation: memory layer repointed to real GPKG
        mem_promote = _make_memory_layer("promote_me")
        project.addMapLayer(mem_promote, False)
        added_ids.append(mem_promote.id())
        tracker.connect_layer(mem_promote)
        res["promote_connected_before"] = mem_promote.id() in tracker._connected_layers
        mem_promote.setDataSource(gpkg_make_perm, "promote_me", "ogr")
        QCoreApplication.processEvents()
        res["promote_connected_after"] = mem_promote.id() in tracker._connected_layers

        # Edit promoted layer and verify event lands in journal
        mem_promote.startEditing()
        pfid = next(mem_promote.getFeatures()).id()
        mem_promote.changeAttributeValue(pfid, 1, "after_promote")
        mem_promote.commitChanges()
        QCoreApplication.processEvents()
        wq.wait_until_drained(timeout=5.0)
        time.sleep(0.05)

        conn = journal.create_read_connection()
        try:
            row = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()
            res["promoted_events_count"] = int(row[0]) if row else 0
        finally:
            conn.close()

        # 6. RecoverPlugin._on_capture_status defense-in-depth
        alerts_captured = []

        class _PluginStub:
            _unloaded = False
            _uncapturable_layers = {}
            _project_capture_failures = 0
            _unverified_inserts = {}
            _unverified_updates = {}
            _prepare_failures = 0

            def _guarded_call(self, _key, _label, _at_risk, _recovers,
                              fn, *args, **kwargs):
                return fn(*args, **kwargs)

            def _alert(self, key, text, level="WARNING", duration=15):
                alerts_captured.append((key, level, duration, text))

            def _update_status_bar(self):
                pass

            def _status_fids(self, detail):
                return RecoverPlugin._status_fids(detail)

        stub = _PluginStub()
        RecoverPlugin._on_capture_status(
            stub, "untracked_edit",
            {"layer_name": "temp", "fingerprint": "memory::Point?crs=EPSG:4326",
             "reason": "unsupported_provider"},
        )
        RecoverPlugin._on_capture_status(
            stub, "untracked_edit",
            {"layer_name": "virt", "fingerprint": "virtual::?query=select 1",
             "reason": "unsupported_provider"},
        )
        res["plugin_alerts_for_ephemeral"] = len(alerts_captured)

        RecoverPlugin._on_capture_status(
            stub, "untracked_edit",
            {"layer_name": "parcels", "fingerprint": "ogr::/tmp/parcels.gpkg",
             "reason": "not_in_tracking_selection"},
        )
        res["plugin_alert_for_real"] = (
            alerts_captured[-1][1:3] if alerts_captured else None
        )

        # 7. Preflight refuses memory restore
        plan = RestorePlan(
            mode=RestoreMode.EVENT,
            scope=RestoreScope.ENTITY,
            cutoff=None,
            atomicity=AtomicityPolicy.STRICT,
            conflict_policy=ConflictPolicy.ABORT,
            actions=[PlannedAction(
                event_id=1,
                operation_type="UPDATE",
                compensatory_op="UPDATE",
                entity_fingerprint="fid:1",
                datasource_fingerprint="memory::Point",
                layer_name="user_scratch",
                has_geometry=False,
                has_attribute_changes=True,
            )],
            conflicts=[],
            entity_count=1,
            event_count=1,
            datasource_fingerprint="memory::Point",
            layer_name="user_scratch",
        )
        issues = preflight_layer_check(plan, mem)
        res["preflight_mem_refused"] = bool(issues)

        # 8. Dialog checkbox panel excludes ephemeral/managed layers and
        # persists ["__NONE__"] when all unchecked.
        from qgis.PyQt.QtWidgets import QCheckBox, QVBoxLayout, QWidget, QLabel
        from recoverland.recover_dialog import RecoverDialog

        settings.setValue("RecoverLand/tracked_layer_fingerprints", json.dumps([]))

        class _DialogStub:
            _tracker = tracker

            def tr(self, s):
                return s

            def _load_tracked_layer_ids(self):
                return RecoverDialog._load_tracked_layer_ids(self)

            def _on_layer_filter_changed(self):
                return RecoverDialog._on_layer_filter_changed(self)

        dstub = _DialogStub()
        host = QWidget()
        layout = QVBoxLayout(host)
        layout.addStretch()
        dstub.layers_container_layout = layout
        dstub.layers_status_label = QLabel()
        RecoverDialog._refresh_layers_panel(dstub)

        cb_names = []
        cbs = []
        for i in range(layout.count()):
            w = layout.itemAt(i).widget()
            if isinstance(w, QCheckBox):
                cb_names.append(w.text())
                cbs.append(w)
        res["panel_checkbox_names"] = sorted(cb_names)

        # Uncheck all real checkboxes and verify ["__NONE__"] persisted
        for cb in cbs:
            cb.blockSignals(True)
            cb.setChecked(False)
            cb.blockSignals(False)
        RecoverDialog._on_layer_filter_changed(dstub)
        raw_saved = settings.value("RecoverLand/tracked_layer_fingerprints", "")
        res["none_saved_raw"] = raw_saved
        res["none_reloaded"] = sorted(
            RecoverDialog._load_tracked_layer_ids(dstub)
        )
        res["tracker_filter_none"] = sorted(tracker._allowed_layer_fingerprints)
        host.deleteLater()
        QCoreApplication.processEvents()

    finally:
        if tracker is not None:
            tracker.deactivate()
        if wq is not None:
            wq.stop()
        if journal is not None:
            journal.close()
        for lid in list(added_ids):
            if project.mapLayer(lid) is not None:
                project.removeMapLayer(lid)
        if saved_fp_setting is None:
            settings.remove("RecoverLand/tracked_layer_fingerprints")
        else:
            settings.setValue("RecoverLand/tracked_layer_fingerprints", saved_fp_setting)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    flog(f"tx_temp_memory_layer_silence: done trace_id={ctx.trace_id}")
    ctx.data["res"] = res


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    r = ctx.data.get("res") or {}
    out = [
        (
            "memory_is_ephemeral_and_informational",
            (r.get("mem_is_ephemeral") is True
             and r.get("mem_support_level") is True
             and r.get("mem_capture_supported") is False
             and r.get("mem_restore_allowed") is False),
            f"res={r}",
        ),
        (
            "snap_overlay_ephemeral_and_purge_lens_exact",
            (r.get("snap_overlay_ephemeral") is True
             and r.get("purge_lens_count") == 1
             and r.get("lens_false_survived") is True
             and r.get("lens_true_removed") is True),
            f"purge={r.get('purge_lens_count')} survived={r.get('lens_false_survived')}",
        ),
        (
            "broken_layer_rejected_cleanly",
            (r.get("broken_capture") is False
             and r.get("broken_restore") is False
             and r.get("broken_connect_res") is False),
            f"broken={r.get('broken_capture')}/{r.get('broken_restore')}/{r.get('broken_connect_res')}",
        ),
        (
            "memory_edit_produces_zero_user_alerts",
            (r.get("real_connected") is True
             and r.get("mem_connected") is False
             and r.get("mem_untracked_alerts") == 0),
            f"real={r.get('real_connected')} mem={r.get('mem_connected')} "
            f"alerts={r.get('mem_untracked_alerts')}",
        ),
        (
            "make_permanent_auto_connects_and_captures",
            (r.get("promote_connected_before") is False
             and r.get("promote_connected_after") is True
             and r.get("promoted_events_count", 0) >= 1),
            f"before={r.get('promote_connected_before')} "
            f"after={r.get('promote_connected_after')} "
            f"events={r.get('promoted_events_count')}",
        ),
        (
            "plugin_silences_ephemeral_and_downgrades_real_warning",
            (r.get("plugin_alerts_for_ephemeral") == 0
             and r.get("plugin_alert_for_real") == ("WARNING", 10)),
            f"ephemeral={r.get('plugin_alerts_for_ephemeral')} "
            f"real={r.get('plugin_alert_for_real')}",
        ),
        (
            "preflight_refuses_memory_restore",
            r.get("preflight_mem_refused") is True,
            f"refused={r.get('preflight_mem_refused')}",
        ),
        (
            "dialog_panel_excludes_ephemeral_and_persists_none_sentinel",
            (r.get("panel_checkbox_names") == ["parcels", "promote_me"]
             and r.get("none_reloaded") == ["__NONE__"]
             and r.get("tracker_filter_none") == ["__NONE__"]),
            f"names={r.get('panel_checkbox_names')} "
            f"none={r.get('none_reloaded')} filter={r.get('tracker_filter_none')}",
        ),
        assert_log_contains(
            ctx.records,
            rf"tx_temp_memory_layer_silence.*trace_id={ctx.trace_id}",
            name="trace_id_propagated",
            min_count=2,
        ),
    ]
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
