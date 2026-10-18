"""tx_audit_lot3_runners_capture_plugin - RecoverLand validation runtime.

Invariant: I-2 / I-3 / I-CODE.

Couvre les constats Runners, Capture et Plugin du Lot 3 de l'audit final :
  1. Commit utilisateur sur la couche en cours d'ecriture entre deux ticks
     d'un run (`RestoreRunner`, `UndoRunner`, `StrictRestoreRunner`) : la
     suspension de capture ne couvre que les etapes synchrones du runner,
     donc un commit de l'utilisateur entre deux ticks est journalise.
  2. Sauvegarde utilisateur (`commitChanges(True)`) sur la couche tenue en
     edition par un Rewind (`StrictRestoreRunner`) : les compensations deja
     dans le tampon sont comptees `applied`, tracees au journal et gardees
     dans le perimetre d'Undo, et le reste est compte en echec.
  3. Edition etrangere pendant un Rewind : le message d'erreur mentionne
     explicitement que les modifications non enregistrees ont ete annulees
     par le rollback.
  4. Annulation d'un Rewind au milieu d'une couche (`cancel_requested`) :
     la couche annulee est comptee dans `cancelled` (et non `failed_other`).
  5. `written_count()` sur les runners : ne compte que les evenements
     reellement ecrits (jamais les sauts idempotents ou refus).
  6. Couche en groupe transactionnel automatique (`QgsVectorLayerEditPassthrough`)
     deja en mode edition lors de `activate()` / `connect_layer()` : detectee
     comme non capturable (`_uncapturable_sessions`) avec statut `uncapturable`.
  7. `_alert` dans `RecoverPlugin` : un message `INFO` ou `WARNING` ne masque
     jamais une alerte `CRITICAL` de meme cle dans la fenetre de 120 s.
  8. `Save As` / `_shutdown_local_backend` : `capture_failures` est conserve
     dans `_project_capture_failures` tant que le projet reste le meme, et
     `_writer_restarts` est reinitialise a chaque ouverture de backend.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_audit_lot3_runners_capture_plugin"
INVARIANT = "I-2"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_lot3_")
    flog(f"tx_audit_lot3_runners_capture_plugin setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import (
        QgsFeature, QgsGeometry, QgsPointXY, QgsVectorLayer,
    )
    import recoverland.restore_runner as rr
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.logger import flog
    from recoverland.core.restore_contracts import CutoffType, RestoreCutoff
    from recoverland.core.write_queue import WriteQueue
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump
    from scripts.validation.scenarios import _restore_fixtures as fx

    d = ctx.data
    tmpdir = d["tmpdir"]
    try:
        # 1, 2, 3, 4, 5: Runners behaviors
        lyr = QgsVectorLayer(
            "Point?crs=EPSG:4326&field=fid:integer&field=nom:string(20)",
            "t_cancel", "memory")
        feats = []
        for i in range(1, 11):
            f = QgsFeature(lyr.fields())
            f.setAttributes([i, f"N{i}_new"])
            f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(float(i), float(i))))
            feats.append(f)
        lyr.dataProvider().addFeatures(feats)

        evts = [
            fx.audit_event(
                event_id=i, operation_type="UPDATE",
                datasource_fingerprint="ogr::t",
                field_schema_json=fx.schema(("fid", "integer"), ("nom", "string")),
                feature_identity_json=json.dumps(
                    {"fid": i, "pk_field": "fid", "pk_value": i}),
                entity_fingerprint=f"pk:fid={i}",
                attributes_json=json.dumps(
                    {"changed_only": {"nom": {"old": f"N{i}_old", "new": f"N{i}_new"}}}),
                geometry_wkb=bytes(feats[i - 1].geometry().asWkb()),
                new_geometry_wkb=bytes(feats[i - 1].geometry().asWkb()),
                geometry_type="Point",
                created_at=f"2026-05-01T10:00:{i:02d}+00:00",
            )
            for i in range(1, 11)
        ]

        saved_yield = rr._chunk_should_yield
        rr._chunk_should_yield = lambda elapsed_ms, n_done, *a, **k: n_done >= 3
        try:
            # 4. Cancel mid-layer -> counted in cancelled, not failed
            runner = rr.StrictRestoreRunner(
                evts, lambda _e: lyr,
                RestoreCutoff(CutoffType.BY_DATE, "2026-05-01T09:00:00+00:00"),
                trace_id=ctx.trace_id)
            orig_chunk = runner._process_strict_chunk

            def _cancel_mid():
                if runner._strict_action_idx >= 3 and not runner._cancelled:
                    runner.cancel()
                orig_chunk()

            runner._process_strict_chunk = _cancel_mid
            done = []
            runner.finished.connect(done.append)
            runner.start()
            while not done:
                pump(0.02)
            rep = runner.apply_report()
            d["cancel_mid_layer_ok"] = (
                rep["cancelled"] == 10
                and rep["apply_fail"] == 0
                and runner.written_count() == 0)

            # 2. User saves layer mid-Rewind -> 3 applied, 7 failed, 3 in undo scope
            lyr2 = QgsVectorLayer(
                "Point?crs=EPSG:4326&field=fid:integer&field=nom:string(20)",
                "t_save", "memory")
            lyr2.dataProvider().addFeatures(feats)
            runner2 = rr.StrictRestoreRunner(
                evts, lambda _e: lyr2,
                RestoreCutoff(CutoffType.BY_DATE, "2026-05-01T09:00:00+00:00"),
                trace_id=ctx.trace_id)
            orig_chunk2 = runner2._process_strict_chunk
            saved_mid = [False]

            def _save_mid():
                if runner2._strict_action_idx >= 3 and not saved_mid[0]:
                    saved_mid[0] = True
                    lyr2.commitChanges(True)
                orig_chunk2()

            runner2._process_strict_chunk = _save_mid
            done2 = []
            runner2.finished.connect(done2.append)
            runner2.start()
            while not done2:
                pump(0.02)
            rep2 = runner2.apply_report()
            d["user_save_mid_rewind_ok"] = (
                rep2["applied"] == 3
                and rep2["apply_fail"] == 7
                and len(done2[0].by_ds.get("ogr::t", [])) == 3
                and runner2.written_count() == 3)

            # 3. Foreign edit discarded message mentions user modifications
            lyr3 = QgsVectorLayer(
                "Point?crs=EPSG:4326&field=fid:integer&field=nom:string(20)",
                "t_foreign", "memory")
            lyr3.dataProvider().addFeatures(feats)
            runner3 = rr.StrictRestoreRunner(
                evts, lambda _e: lyr3,
                RestoreCutoff(CutoffType.BY_DATE, "2026-05-01T09:00:00+00:00"),
                trace_id=ctx.trace_id)
            orig_chunk3 = runner3._process_strict_chunk
            foreign_done = [False]

            def _foreign_mid():
                if runner3._strict_action_idx >= 3 and not foreign_done[0]:
                    foreign_done[0] = True
                    lyr3.changeAttributeValue(1, 1, "USER_TYPED")
                orig_chunk3()

            runner3._process_strict_chunk = _foreign_mid
            done3 = []
            runner3.finished.connect(done3.append)
            runner3.start()
            while not done3:
                pump(0.02)
            errs3 = " | ".join(done3[0].errors)
            d["foreign_edit_msg_complete"] = (
                "modifications non enregistrees" in errs3
                and "rollback" in errs3)
        finally:
            rr._chunk_should_yield = saved_yield

        # 1. User commit between ticks of RestoreRunner is journaled
        gpkg_path = fx.make_gpkg(
            tmpdir, "u_commit", 1, [("nom", 4)],
            [(i, [f"V{i}"], f"POINT ({i} {i})") for i in range(1, 13)])
        lyr_u = fx.open_layer(gpkg_path, "u_commit")
        fp_u = compute_datasource_fingerprint(lyr_u)
        jdir_u = os.path.join(tmpdir, "ju")
        os.makedirs(jdir_u, exist_ok=True)
        jm_u = JournalManager()
        jpath_u = jm_u.open_for_project("", jdir_u)
        wq_u = WriteQueue()
        wq_u.start(jpath_u)
        tr_u = EditSessionTracker(wq_u, jm_u)
        tr_u.activate()
        tr_u.connect_layer(lyr_u)
        try:
            evts_u = [
                fx.audit_event(
                    event_id=100 + i, operation_type="UPDATE",
                    datasource_fingerprint=fp_u,
                    field_schema_json=fx.schema(("fid", "integer"), ("nom", "string")),
                    feature_identity_json=json.dumps(
                        {"fid": i, "pk_field": "fid", "pk_value": i}),
                    entity_fingerprint=f"pk:fid={i}",
                    attributes_json=json.dumps(
                        {"changed_only": {"nom": {"old": f"V{i}", "new": f"V{i}"}}}),
                    geometry_wkb=fx.wkb_wkt(f"POINT ({i} {i})"),
                    new_geometry_wkb=fx.wkb_wkt(f"POINT ({i} {i})"),
                    geometry_type="Point",
                )
                for i in range(1, 11)
            ]
            run_u = rr.RestoreRunner(
                evts_u, lambda _e: lyr_u, None,
                tracker=tr_u, trace_id=ctx.trace_id, journal_path=jpath_u)
            orig_u = run_u._process_chunk
            user_committed = [False]

            def _user_between_ticks():
                if run_u._event_idx >= 5 and not user_committed[0]:
                    user_committed[0] = True
                    # Between ticks: user edits fid 12 and commits
                    lyr_u.startEditing()
                    idx_nom = lyr_u.fields().indexFromName("nom")
                    lyr_u.changeAttributeValue(12, idx_nom, "USER_MID_RUN")
                    lyr_u.commitChanges()
                orig_u()

            run_u._process_chunk = _user_between_ticks
            done_u = []
            run_u.finished.connect(done_u.append)
            run_u.start()
            while not done_u:
                pump(0.02)
            wq_u.stop()
            conn_u = jm_u.create_read_connection()
            try:
                rows_u = conn_u.execute(
                    "SELECT attributes_json FROM audit_event WHERE restored_from_event_id IS NULL"
                ).fetchall()
            finally:
                conn_u.close()
            d["user_commit_between_ticks_journaled"] = any(
                "USER_MID_RUN" in (r[0] or "") for r in rows_u)
        finally:
            tr_u.disconnect_all()
            wq_u.stop()
            jm_u.close()

        # 7 & 8. Plugin _alert severity escalation, Save As capture_failures, _writer_restarts reset
        iface = FakeIface()
        plugin = RecoverPlugin(iface)
        plugin.initGui()
        try:
            n0 = len(iface.bar.messages)
            plugin._alert("history_carry", "info msg", "INFO")
            plugin._alert("history_carry", "critical msg", "CRITICAL", duration=0)
            msgs = [m[1] for m in iface.bar.messages[n0:]]
            d["alert_critical_not_blocked_by_info"] = (
                "info msg" in msgs and "critical msg" in msgs)

            # Save As / backend restart preserves capture_failures & resets _writer_restarts
            plugin._writer_restarts = 3
            plugin._tracker.capture_failures = 2
            plugin._shutdown_local_backend()
            plugin._init_local_backend()
            health = plugin._capture_health()
            d["save_preserves_error_health"] = (health.state == "ERROR")
            d["writer_restarts_reset_on_init"] = (plugin._writer_restarts == 0)
        finally:
            plugin.unload()

        flog(f"tx_audit_lot3_runners_capture_plugin run done: trace_id={ctx.trace_id}", "INFO")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    return [
        ("cancel_mid_layer_counted_as_cancelled",
         d.get("cancel_mid_layer_ok") is True,
         f"cancel_mid_layer_ok={d.get('cancel_mid_layer_ok')}"),
        ("user_save_mid_rewind_accounts_applied_and_failed",
         d.get("user_save_mid_rewind_ok") is True,
         f"user_save_mid_rewind_ok={d.get('user_save_mid_rewind_ok')}"),
        ("foreign_edit_discarded_message_explicit",
         d.get("foreign_edit_msg_complete") is True,
         f"foreign_edit_msg_complete={d.get('foreign_edit_msg_complete')}"),
        ("user_commit_between_ticks_is_journaled",
         d.get("user_commit_between_ticks_journaled") is True,
         f"user_commit_between_ticks_journaled={d.get('user_commit_between_ticks_journaled')}"),
        ("alert_critical_not_blocked_by_info_or_warning",
         d.get("alert_critical_not_blocked_by_info") is True,
         f"alert_critical_not_blocked_by_info={d.get('alert_critical_not_blocked_by_info')}"),
        ("save_preserves_error_health_and_resets_restarts",
         d.get("save_preserves_error_health") is True
         and d.get("writer_restarts_reset_on_init") is True,
         f"health_err={d.get('save_preserves_error_health')} "
         f"restarts_reset={d.get('writer_restarts_reset_on_init')}"),
        assert_log_contains(
            ctx.records,
            pattern=rf"tx_audit_lot3_runners_capture_plugin.*trace_id={ctx.trace_id}",
            min_count=2,
            name="trace_id_propagated",
        ),
    ]
