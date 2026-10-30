# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
from qgis.PyQt.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                                 QComboBox, QMessageBox, QProgressBar,
                                 QCheckBox, QApplication,
                                 QTableWidget, QTableWidgetItem, QLineEdit,
                                 QGraphicsDropShadowEffect, QWidget,
                                 QScrollArea, QFrame, QMenu, QShortcut,
                                 QStackedWidget, QListWidget, QListWidgetItem)
from qgis.PyQt.QtCore import (QDateTime, QDate, QTimer,
                              QVariantAnimation)
from qgis.PyQt.QtGui import QIcon, QColor, QKeySequence
from qgis.core import (
    QgsVectorLayer, QgsProject, QgsSettings,
    QgsGeometry, QgsCoordinateTransform, QgsCoordinateReferenceSystem,
)
from qgis.gui import QgsCollapsibleGroupBox, QgsDateTimeEdit
from .compat import QtCompat, QgisCompat
from .core import (
    flog, qlog, LoggerMixin, LayerStatsCache,
    SearchCriteria,
    reconstruct_attributes,
    compute_datasource_fingerprint,
    is_geometry_only_update,
    format_relative_time,
    check_disk_for_path, format_disk_message,
    GeometryPreviewManager,
    plan_event_restore, preflight_check,
    format_plan_summary, format_preflight_report,
    find_target_layer, cleanup_temp_layers,
    is_layer_audit_field,
    generate_trace_id,
    get_event_by_id, fetch_events_by_ids,
    fetch_events_by_session, count_events_by_session,
    _BLOB_MARKER,
)
from .core.geometry_utils import heavy_diag_enabled
from .core.observability import log_state_transition
from .journal_info_bar import JournalInfoBar, SmartBarState, SmartBarTileState
from .journal_maintenance import JournalMaintenanceDialog
from .local_search_thread import LocalSearchThread
from .journal_stats_thread import JournalStatsThread
from .restore_runner import RestoreRunner, StrictRestoreRunner, UndoRunner
from .version_fetch_thread import VersionFetchThread
from .widgets import (AppleToggleSwitch, ReviewSegmentedSwitch,
                      ThemedLogoWidget, RestoreModeSelector,
                      RestorePreflightDialog, TimeSliderWidget,
                      ActionButtonBar, build_dialog_stylesheet,
                      resolve_theme, theme_icon)
import os
import json
import time
import uuid

CHANGE_TYPE_COLORS = {
    "modified": QColor(66, 133, 244, 60),
    "emptied": QColor(219, 68, 55, 55),
    "populated": QColor(52, 168, 83, 55),
    "geometry": QColor(255, 152, 0, 60),
}


def _change_type_labels():
    from qgis.PyQt.QtCore import QCoreApplication

    def _tr(msg):
        return QCoreApplication.translate("RecoverDialog", msg)
    return {
        "modified": _tr("Valeur modifiee"),
        "emptied": _tr("Valeur videe"),
        "populated": _tr("Valeur ajoutee"),
        "geometry": _tr("Geometrie modifiee"),
    }


_MIN_RECOVER_ANIMATION_SEC = 0.85
_MIN_RESTORE_ANIMATION_SEC = 0.70
# Read-your-writes barrier before a plan (see _journal_barrier): long
# enough for a normal backlog, short enough not to freeze the dialog.
_JOURNAL_BARRIER_TIMEOUT_S = 5.0
# How long a project switch waits for a cancelled runner to reach its end.
_QUIESCE_TIMEOUT_MS = 15000


def _flog_events(label: str, events, trace_id: str = "") -> None:
    """Log a rewind event set: one aggregated line, detail on opt-in.

    These dumps run on the GUI thread, one line per event, and every line
    is formatted and flushed to two file handlers. On the run that
    prompted this (trace a1517527) the four dump sites cost ~270 ms of
    frozen interface before any useful work, and the per-event detail was
    only ever read while chasing a defect. The aggregated line is kept
    unconditionally so a run stays identifiable in the log; the detail is
    behind RECOVERLAND_HEAVY_DIAG, like EDIT_START and RELOAD_CHECK.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    if not heavy_diag_enabled():
        ops: dict = {}
        for e in events:
            ops[e.operation_type] = ops.get(e.operation_type, 0) + 1
        flog(f"{prefix}events[{label}]: n={len(events)} ops={ops} "
             f"detail=diag_off (set RECOVERLAND_HEAVY_DIAG=1 to enable)")
        return
    for e in events:
        flog(f"  -> {label} eid={e.event_id} op={e.operation_type} "
             f"identity={(e.feature_identity_json or '')[:80]}")


def _restore_buckets(result, partial_n: int = 0, report=None) -> dict:
    """Bucket a finished restore the way CYCLE_SUMMARY buckets it.

    RLU-055. Measured on a real rewind (2026-08-12), the plan carried 73
    actions: 6 entities came back, 3 were already at the target state, 5
    were refused for geometry drift and 59 had no target left. The log
    line said exactly that; the dialog, built from `total_ok` and
    `total_fail`, said "14 restauree(s), 59 echouee(s)".

    Both numbers were wrong in the direction that hurts: `total_ok`
    counts everything the provider did not reject (the 6 writes plus the
    3 skips plus the 5 refusals), so it announces 14 entities restored
    when 6 came back; `total_fail` counts only the actions that reached
    the writer, so it hides 5 of the 64 failures. And 14 + 59 = 73 by
    coincidence, which is what made the line look complete.

    So the message is built from the same buckets as the log line, and
    from nothing else:

        applied + applied_partial + skipped + failed (+ cancelled)
            == the plan

    `report` is ``runner.apply_report()``: the only source that knows the
    plan size, the partial writes and the actions a cancel never
    attempted. Without it (the runner was already released) the buckets
    are rebuilt from the emitted result, which carries every failure
    category but no plan base -- `plan_known` tells the caller which of
    the two it is reading, so a partial count is never displayed as a
    plan.
    """
    if report:
        applied = int(report.get("applied", 0) or 0)
        partial = int(report.get("applied_partial", 0) or 0)
        skipped = int(report.get("apply_skipped", 0) or 0)
        # `apply_fail` is the failure TOTAL; the residual bucket that the
        # runner also publishes under the short name `failed` is only one
        # of its parts (see restore_runner._apply_counters).
        failed_total = int(report.get("apply_fail", 0) or 0)
        absent = int(report.get("failed_target_absent", 0) or 0)
        drift = int(report.get("failed_geometry_drift", 0) or 0)
        postcommit = int(report.get("failed_postcommit_verification", 0) or 0)
        # The residual is what the total leaves once its named parts are
        # taken out, so the reasons shown always add up to the failures
        # announced. It used to be read from `failed_other`, a key the
        # runner's report did not carry: always 0, and a run with 3
        # failures "for another reason" was announced "3 echouee(s)" with
        # no reason at all. The published bucket is checked against it.
        other = failed_total - absent - drift - postcommit
        published = report.get("failed_other", report.get("failed"))
        cancelled = int(report.get("cancelled", 0) or 0)
        planned = int(report.get("plan_actions", 0) or 0)
        plan_known = True
        from .core.observability import assert_invariant
        assert_invariant(
            other >= 0 and (published is None or int(published) == other),
            "dialog_failure_reasons_vs_total",
            failed_total=failed_total, absent=absent, drift=drift,
            postcommit=postcommit, residual=other, published=published)
        assert_invariant(
            applied + partial + skipped + failed_total + cancelled == planned,
            "dialog_buckets_vs_plan",
            applied=applied, applied_partial=partial, skipped=skipped,
            failed_total=failed_total, cancelled=cancelled, planned=planned)
        other = max(other, 0)
    else:
        applied = int(getattr(result, "applied", 0) or 0)
        partial = int(partial_n or 0)
        skipped = int(getattr(result, "skipped_idempotent", 0) or 0)
        absent = int(getattr(result, "failed_target_absent", 0) or 0)
        drift = int(getattr(result, "failed_geometry_drift", 0) or 0)
        postcommit = int(getattr(result, "failed_postcommit_verification", 0) or 0)
        # `GroupedRestoreResult.failed` is the RESIDUAL bucket, not the
        # total: the contract in core/workflow_service.py says so, and
        # displaying it as the total is the defect this function exists
        # to close.
        other = int(getattr(result, "failed", 0) or 0)
        failed_total = absent + drift + postcommit + other
        cancelled = 0
        planned = applied + partial + skipped + failed_total
        plan_known = False
    return {
        "applied": applied,
        "applied_partial": partial,
        "skipped": skipped,
        "failed_total": failed_total,
        "failed_absent": absent,
        "failed_drift": drift,
        "failed_postcommit": postcommit,
        "failed_other": other,
        "cancelled": cancelled,
        "planned": planned,
        "plan_known": plan_known,
    }


class RecoverDialog(QDialog, LoggerMixin):
    """Main dialog for RecoverLand plugin."""

    # Critical lifecycle flags whose every transition is logged. Stuck
    # values here are the root cause of "rewind blocked" / "undo button
    # never re-enables" classes of bug, so we trace them automatically.
    _WATCHED_ATTRS = (
        "_is_recovering",
        "_restore_in_progress",
        "_last_restore_by_ds",
        "_last_restore_events",
        # What "Annuler le dernier restore" reverts (the last run), and
        # the journal mark that tells its traces from older ones.
        "_last_run_ids",
        "_last_run_mark",
        "_active_restore_trace_id",
        "_restore_runner",
    )

    def __setattr__(self, name: str, value):
        if name in type(self)._WATCHED_ATTRS:
            old = self.__dict__.get(name, "<unset>")
            try:
                log_state_transition("RecoverDialog", name, old, value)
            except Exception:  # nosec B110 # pragma: no cover
                # No log line here, deliberately: this handler guards the
                # state-transition logger itself, so if that logger is what
                # failed, logging from the handler fails the same way. And
                # __setattr__ runs on every assignment to a watched flag --
                # a raise here would break the setters that gate the whole
                # run. Logging never breaks setters.
                pass
            # A run has ~25 exit paths, each resetting this flag. Hooking
            # the transition itself is the only way to guarantee the canvas
            # is unfrozen and the refresh timer restarted on every one of
            # them -- a canvas left frozen would be worse than a slow run.
            # `old is "<unset>"` is the initial assignment in __init__, not
            # a transition: acting on it would thaw and REFRESH the canvas
            # every time the dialog is built.
            if (name == "_is_recovering" and old != "<unset>"
                    and bool(old) != bool(value)):
                try:
                    self._set_ui_quiet_period(bool(value))
                except Exception:  # nosec B110 # pragma: no cover
                    # Same rule as the handler above: no log line inside a
                    # setter guard. _set_ui_quiet_period stops timers and
                    # touches the canvas, so when it fails the logger may
                    # be inside the same broken teardown -- and a raise
                    # here would abort the assignment to _is_recovering
                    # that all ~25 exit paths depend on. Never break a
                    # setter.
                    pass
        super().__setattr__(name, value)

    def _set_ui_quiet_period(self, active: bool) -> None:
        """Stop the periodic UI work that competes with a running operation.

        Measured on trace b3d06716: the 10 s journal-status timer fired 8
        times DURING the run, each tick rebuilding the layer stats cache
        (23 layers, 3109 events) and then, on the GUI thread, the layer
        combo, the scope list and the 2215 steps of the slider.

        This is hooked on `_is_recovering`, which covers the three modes,
        so it must stay harmless for the read-only ones (Review, Event):
        stopping a refresh timer is. Freezing the CANVAS is not -- it
        would blank the map during a plain journal search -- so that lives
        in `_freeze_canvas_for_write`, armed only where writing starts.
        """
        timer = getattr(self, "_layer_refresh_timer", None)
        if timer is not None:
            if active:
                timer.stop()
            elif self.isVisible():
                # Only a visible dialog needs the periodic refresh;
                # hideEvent stops it on purpose.
                timer.start()
        if not active:
            self._freeze_canvas_for_write(False)
        flog(f"ui_quiet_period: active={active} "
             f"refresh_timer={'stopped' if active else 'resumed'}")

    def _freeze_canvas_for_write(self, active: bool) -> None:
        """Hold the canvas still while a run WRITES to the layers.

        A rewind fires layer.reload() + triggerRepaint() per group; on a
        23-layer project the repaints alone cost ~18 s of frozen UI, which
        is why core/snapshot_overlay_session.py already freezes the canvas
        the same way. Thawing is idempotent and only refreshes when we are
        the ones who froze, so a plain search never repaints the project
        for nothing, and a canvas frozen by another tool is left alone.
        """
        if bool(getattr(self, "_canvas_frozen_by_run", False)) == bool(active):
            return
        canvas = None
        iface = getattr(self, "iface", None)
        if iface is not None and hasattr(iface, "mapCanvas"):
            canvas = iface.mapCanvas()
        if canvas is None or not hasattr(canvas, "freeze"):
            return
        canvas.freeze(active)
        self._canvas_frozen_by_run = bool(active)
        if not active and hasattr(canvas, "refresh"):
            canvas.refresh()
        flog(f"canvas_freeze_for_write: {'frozen' if active else 'thawed'}")

    def __init__(self, iface, journal=None, tracker=None,
                 write_queue=None):
        flog("RecoverDialog.__init__: START")
        super().__init__(iface.mainWindow())
        self.iface = iface
        self._journal = journal
        self._tracker = tracker
        self._write_queue = write_queue
        self.setWindowTitle(self.tr("RecoverLand - Récupération de données"))
        self.setMinimumWidth(750)
        self.setMinimumHeight(600)
        self.resize(800, 700)
        self.setWindowFlags(self.windowFlags() | QtCompat.WINDOW_MAXIMIZE_BUTTON_HINT)
        self.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_PREFERRED)
        self._apply_global_button_style()

        icon_path = os.path.join(os.path.dirname(__file__), "logo.svg")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        self._search_events = []
        self._all_attr_keys = []
        self.worker_thread = None
        self.selected_rows = []
        self._modified_col_indices = set()
        self.table_widget = None
        self.sync_in_progress = False
        self._is_recovering = False
        self._smart_bar_message_override = ""
        self._smart_bar_summary = None
        self._recover_started_at = 0.0
        self._pending_search_result = None
        self._restore_started_at = 0.0
        self._pending_restore_feedback = None
        self._restore_runner = None
        # RLU-055: a cancelled runner still finishes; the dialog keeps it
        # here so the end-of-run message reads its buckets instead of
        # falling back to raw totals.
        self._cancelled_restore_runner = None
        self._last_restore_events = None
        # Per layer, the compensations the runs of this session wrote and
        # nothing has undone since, in the order they were applied.
        self._last_restore_by_ds = None
        # The event ids the LAST run wrote and nothing has undone since:
        # the only scope of "Annuler le dernier restore" (see
        # `_manual_undo_scope`).
        self._last_run_ids: set = set()
        # The journal high-water mark (MAX(event_id)) read when that last
        # run was launched: the traces it wrote are above it, the traces of
        # earlier runs on the same events are not (`_run_trace_split`).
        # `_run_journal_mark` is the mark of the run in flight, handed over
        # by `_record_last_run`. None: unknown.
        self._last_run_mark = None
        self._run_journal_mark = None
        # Set by cleanup_resources: a late slot (a fetch result queued
        # before the close) must not start a run on a closed dialog.
        self._closing = False
        self._pending_cycle_stats = None
        # RLU-056: the layers whose feature count does not match the state
        # the last rewind transition asked for. Filled by
        # `_rewind_count_snapshot`, drained by `_report_state_mismatches`.
        self._state_mismatches: list = []
        self._active_search_trace_id = ""
        self._active_restore_trace_id = ""
        self._dialog_read_conn = None
        # (journal_path, context) already told to the user: the degraded
        # reconciliation is a property of the journal, not of the click,
        # so the project-open warning must not repeat on every refresh.
        self._reconciliation_notified: set = set()
        self._version_fetch_thread = None
        self._version_restore_cutoff = None
        self._geom_preview = GeometryPreviewManager(iface.mapCanvas())
        self._stats_cache = LayerStatsCache()
        self._initial_bounds_applied = False
        self._stats_thread = None
        self._last_health = None
        self._last_size_str = ""
        self._review_active = False
        self._review_wants_persist = False
        self._review_status_widget = None
        self._review_snap_mode = False
        self._review_snap_session = None
        self._review_date_bar = None
        self._review_snap_pending_iso = ""
        self._review_global_t0_iso = ""
        self._review_snap_cache: dict = {}
        self._review_snap_worker = None
        self._review_snap_zombies: list = []
        self._review_snap_ext_debounce = None
        self._review_snap_bbox_per_layer: dict = {}
        # Raw reconstruction (tracked entities, pre-bbox-filter)
        # cached per cutoff date so pan/zoom can re-apply it WITHOUT re-querying
        # the journal (reconstruction depends only on the date, not the extent).
        self._review_snap_raw_result = None
        self._review_snap_raw_iso = ""
        # The live completion of the date the worker answered, run across
        # event-loop turns (snapshot_overlay_session.LiveCompletion).
        self._review_snap_completion = None
        # Extent-scoped marker scanner (CHANGE C): runs on pan/zoom settle to show
        # modification dates only inside the current viewport.
        self._review_marker_worker = None
        self._review_marker_zombies: list = []
        self._review_marker_bbox_signature = ""
        self._review_global_t1_iso = ""

        self.setup_ui()

        self.progress_timer = QTimer(self)
        self.progress_timer.timeout.connect(self.pulse_progress_bar)
        self._stats_debounce_timer = QTimer(self)
        self._stats_debounce_timer.setSingleShot(True)
        self._stats_debounce_timer.setInterval(300)
        self._stats_debounce_timer.timeout.connect(self._launch_stats_worker)
        self._layer_refresh_timer = QTimer(self)
        self._layer_refresh_timer.setInterval(10000)
        self._layer_refresh_timer.timeout.connect(self._refresh_journal_status)
        self._layer_refresh_timer.start()
        self._review_debounce = QTimer(self)
        self._review_debounce.setSingleShot(True)
        self._review_debounce.setInterval(600)
        self._review_debounce.timeout.connect(self._review_auto_refresh)
        self._setup_review_status_bar()
        self._refresh_layers_panel()
        tracking_on = QgsSettings().value("RecoverLand/tracking_enabled", True, type=bool)
        self._programmatic_toggle = True
        try:
            self.tracking_toggle.setChecked(tracking_on)
        finally:
            self._programmatic_toggle = False
        if not tracking_on:
            t = resolve_theme(self)
            self.tracking_label.setText(self.tr("Enregistrement désactivé"))
            self.tracking_label.setStyleSheet(f"color: {t.erreur.name()}; font-weight: 600;")
        self._refresh_smart_bar()
        self._check_onboarding()
        self._check_disk_space()

    def _cancel_snap_worker(self, disconnect_signals: bool = False) -> None:
        """Cancel current snap worker and park it in zombie list for reaping.

        The live completion of a date the worker already answered is the
        rest of the same reconstruction: it is cancelled with it (a new
        date, a commit, Review stopped), and delivers nothing afterwards.
        """
        job = getattr(self, "_review_snap_completion", None)
        if job is not None:
            job.cancel("superseded")
            self._review_snap_completion = None
        worker = self._review_snap_worker
        if worker is None:
            return
        if disconnect_signals:
            for signal in (worker.result_ready, worker.error,
                           getattr(worker, "refused", None)):
                if signal is None:
                    continue
                try:
                    signal.disconnect()
                except (TypeError, RuntimeError):
                    pass
        worker.cancel()
        self._review_snap_zombies.append(worker)
        worker.finished.connect(self._reap_snap_zombies)
        self._review_snap_worker = None

    def _check_onboarding(self) -> None:
        """UX-F01: Show onboarding panel on first launch."""
        done = QgsSettings().value("RecoverLand/onboarding_done", False, type=bool)
        if done:
            return
        self.iface.messageBar().pushMessage(
            self.tr("Bienvenue dans RecoverLand"),
            self.tr(
                "RecoverLand enregistre automatiquement vos modifications. "
                "Utilisez Recover pour retrouver vos donnees, "
                "puis Restore pour les reinjecter dans vos couches. "
                "Le journal est stocke localement a cote de votre projet."
            ),
            QgisCompat.MSG_INFO, 0,
        )
        QgsSettings().setValue("RecoverLand/onboarding_done", True)

    def _check_disk_space(self) -> None:
        """UX-A04: Check disk space and warn if low."""
        path = self._journal.path if self._journal else ""
        if not path:
            return
        status = check_disk_for_path(path)
        if status.is_critical:
            msg = format_disk_message(status)
            qlog(msg, "ERROR")
            if self._tracker and self.tracking_toggle.isChecked():
                self._programmatic_toggle = True
                try:
                    self._on_tracking_toggled(False)
                    self.tracking_toggle.setChecked(False, animated=True)
                finally:
                    self._programmatic_toggle = False
        elif status.is_low:
            msg = format_disk_message(status)
            qlog(msg, "WARNING")

    def _set_journal_info_visible(self, visible: bool) -> None:
        if not hasattr(self, 'smart_bar'):
            return
        self.smart_bar.setVisible(True)
        self.smart_bar.setEnabled(visible)

    def _build_search_criteria(self) -> SearchCriteria:
        fingerprint = self.layer_input.currentData() or None
        op_key = self.operation_input.currentData() or "ALL"
        operation = None if op_key == "ALL" else op_key
        start_utc = self.start_input.dateTime().toUTC()
        end_utc = self.end_input.dateTime().toUTC()
        return SearchCriteria(
            datasource_fingerprint=fingerprint,
            layer_name=None,
            operation_type=operation,
            user_name=None,
            start_date=start_utc.toString("yyyy-MM-ddTHH:mm:ss"),
            end_date=end_utc.toString("yyyy-MM-ddTHH:mm:ss"),
            page=1,
            page_size=500,
        )

    def _current_smart_bar_keys(self) -> tuple:
        op_key = self.operation_input.currentData() or "ALL"
        return (op_key,)

    def _build_smart_bar_message(self, summary) -> str:
        if summary.total_count == 0:
            return self.tr("Aucune activité dans le périmètre courant.")
        op_key = self.operation_input.currentData() or "ALL"
        if op_key != "ALL":
            return self._build_filtered_smart_bar_message(
                summary, self.operation_input.currentText())
        return self._build_scope_activity_message(summary)

    def _build_filtered_smart_bar_message(self, summary, operation: str) -> str:
        if summary.selected_count == 0:
            return self.tr("Filtre {op} : aucun événement visible.").format(op=operation)
        if summary.selected_count == summary.total_count:
            return self.tr("Filtre {op} : le périmètre courant ne contient que ce type.").format(op=operation)
        hidden_count = summary.total_count - summary.selected_count
        return self.tr("Filtre {op} : {selected} visible(s), {hidden} masqué(s).").format(
            op=operation,
            selected=summary.selected_count,
            hidden=hidden_count,
        )

    def _build_scope_activity_message(self, summary) -> str:
        if summary.update_count == summary.total_count:
            return self.tr("Cette sélection ne contient que des mises à jour.")
        if summary.delete_count == summary.total_count:
            return self.tr("Cette sélection ne contient que des suppressions.")
        if summary.insert_count == summary.total_count:
            return self.tr("Cette sélection ne contient que des ajouts.")
        if summary.user_count <= 1:
            return self.tr("Activité d'un seul utilisateur dans cette sélection.")
        return self.tr("Activité répartie entre {count} utilisateur(s) dans cette sélection.").format(
            count=summary.user_count,
        )

    def _build_smart_bar_tiles(self, summary) -> tuple:
        pal_color = self.palette().highlight().color()
        return (
            SmartBarTileState("ALL", self.tr("Total"), str(summary.total_count), pal_color,
                              self.tr("Réinitialiser le filtre d'opération")),
            SmartBarTileState("UPDATE", self.tr("Updates"), str(summary.update_count), pal_color,
                              self.tr("Basculer le filtre sur UPDATE")),
            SmartBarTileState("DELETE", self.tr("Suppr."), str(summary.delete_count), pal_color,
                              self.tr("Basculer le filtre sur DELETE")),
            SmartBarTileState("INSERT", self.tr("Ajouts"), str(summary.insert_count), pal_color,
                              self.tr("Basculer le filtre sur INSERT")),
        )

    def _build_smart_bar_state(self, summary, size_str: str) -> SmartBarState:
        override = self._smart_bar_message_override
        self._smart_bar_message_override = ""
        health_level = "healthy"
        health_message = ""
        health_suggestion = ""
        if self._last_health is not None:
            health_level = self._last_health.level
            health_message = self._last_health.message
            health_suggestion = self._last_health.suggestion
        if summary is None:
            message = override or self.tr("Ouvrez un projet QGIS pour activer le journal local.")
            return SmartBarState(
                title=self.tr("Journal local"),
                meta="",
                message=message,
                mode="disabled",
                active_keys=(),
                tiles=(
                    SmartBarTileState("ALL", self.tr("Total"), "0", self.palette().highlight().color(), ""),
                    SmartBarTileState("UPDATE", self.tr("Updates"), "0", self.palette().highlight().color(), ""),
                    SmartBarTileState("DELETE", self.tr("Suppr."), "0", self.palette().highlight().color(), ""),
                    SmartBarTileState("INSERT", self.tr("Ajouts"), "0", self.palette().highlight().color(), ""),
                ),
                health_level=health_level,
                health_message=health_message,
                health_suggestion=health_suggestion,
            )
        message = override or self._build_smart_bar_message(summary)
        pending = self._get_write_queue_pending()
        meta_parts = [size_str]
        if self._last_health is not None and self._last_health.event_count > 0:
            evt_fmt = f"{self._last_health.event_count:,}".replace(",", " ")
            meta_parts.append(self.tr("{count} événement(s)").format(count=evt_fmt))
        meta_parts.append(self.tr("{count} couche(s)").format(count=summary.layer_count))
        if pending > 0:
            meta_parts.append(self.tr("{count} en attente").format(count=pending))
        meta = " | ".join(meta_parts)
        return SmartBarState(
            title=self.tr("Journal local"),
            meta=meta,
            message=message,
            mode="ready",
            active_keys=self._current_smart_bar_keys(),
            tiles=self._build_smart_bar_tiles(summary),
            health_level=health_level,
            health_message=health_message,
            health_suggestion=health_suggestion,
        )

    def _ambiguous_datasource_names(self) -> list:
        """Layer names the v6 reconciliation refused to attach.

        Two DB tables that collapsed into one fingerprint are not an
        identity: the migration declined to guess which one owns the old
        history rather than attach it to the wrong table. The user has to
        read those names before he scopes a rewind on them.
        """
        conn = self._get_dialog_read_conn()
        if conn is None:
            return []
        try:
            from .core.datasource_alias import get_ambiguous_datasources
            ambiguous = get_ambiguous_datasources(conn)
        except Exception as exc:  # noqa: BLE001 - a warning never blocks
            flog(f"ambiguous_datasource_names: unreadable: {exc}", "WARNING")
            return []
        names = []
        for item in ambiguous:
            for name in (item.layer_names or ()):
                if name and name not in names:
                    names.append(name)
            if not item.layer_names and item.fingerprint:
                short = item.fingerprint[-40:]
                if short not in names:
                    names.append(short)
        return names

    def _warn_if_reconciliation_degraded(self, context: str) -> bool:
        """Tell the user that part of the history is not attached.

        `JournalManager` has known this since the journal was opened and
        `datasource_alias.describe_reconciliation` has known how to say
        it; nothing read either, so a rewind was offered on a scope whose
        older events it cannot reach, without a word. Returns True when a
        warning was emitted.

        The ``project`` context speaks once per journal (it runs on every
        show and every project switch); ``rewind`` speaks every time,
        because it warns about the operation the user just launched.
        """
        journal = self._journal
        if journal is None or not journal.is_open:
            return False
        # Not only a degraded reconciliation: a history attached on the
        # single-service ASSUMPTION (`ReconciliationReport.assumed`) must be
        # said too, so the user can tell the plugin it guessed wrong. The
        # gate used to be `is_reconciliation_degraded` alone, which is
        # False for that case: the assumption was never shown, although
        # `describe_reconciliation` spells it out (`needs_notice`).
        try:
            degraded = bool(journal.is_reconciliation_degraded)
        except Exception as exc:  # noqa: BLE001 - never block on a warning
            flog(f"reconciliation warning: unreadable state: {exc}", "WARNING")
            return False

        key = (journal.path or "", context)
        if context == "project":
            if key in self._reconciliation_notified:
                return False
        conn = self._get_dialog_read_conn()
        if conn is not None:
            # The report as stored NOW: a history attached or taken back
            # from the results menu changed it after the journal was opened.
            try:
                from .core.datasource_alias import load_reconciliation
                report = load_reconciliation(conn)
                if report is not None:
                    degraded = bool(report.is_degraded)
            except Exception as exc:  # noqa: BLE001 - the flag above stands
                flog(f"reconciliation warning: report unreadable: {exc}",
                     "WARNING")

        try:
            message = journal.reconciliation_warning()
        except Exception as exc:  # noqa: BLE001
            flog(f"reconciliation warning: undescribable: {exc}", "WARNING")
            message = ""
        if not message:
            return False
        if context == "project":
            self._reconciliation_notified.add(key)

        names = self._ambiguous_datasource_names()
        if names:
            # describe_reconciliation stops at three; a rewind must name
            # every source it may under-cover, not the first three.
            message += " " + self.tr(
                "Sources concernees : {names}."
            ).format(names=", ".join(names))
        services = self._service_history_lines()
        if services:
            message += " " + services
        if context == "rewind":
            message = (self.tr(
                "Restauration lancee sur un historique partiellement "
                "rattache.") if degraded else self.tr(
                "Restauration lancee sur un historique rattache par "
                "hypothese.")) + " " + message
        qlog(message, "WARNING")
        flog(f"reconciliation_warning_shown context={context} "
             f"degraded={degraded} ambiguous={len(names)} "
             f"journal={journal.path}", "WARNING")
        return True

    def _service_history_lines(self) -> str:
        """Each refused service-less history, its known services, and the way out.

        '' when there is none. The reconciliation refused to attach these
        histories because several connection services are known; the
        user is the only one who can say which database the old events
        came from, and he needs the names to say it.
        """
        conn = self._get_dialog_read_conn()
        if conn is None:
            return ""
        try:
            from .core.datasource_alias import service_history_candidates
            candidates = service_history_candidates(conn)
        except Exception as exc:  # noqa: BLE001 - a warning never blocks
            flog(f"service_history_lines: unreadable: {exc}", "WARNING")
            return ""
        if not candidates:
            return ""
        parts = []
        for item in candidates:
            label = ", ".join(item.layer_names) or item.fingerprint[-40:]
            known = ", ".join(item.known_services) or self.tr("aucun")
            parts.append(self.tr("{layers} (services connus : {known})").format(
                layers=label, known=known))
        return self.tr(
            "Historique d'avant v7 non rattache faute de savoir de quel "
            "service il vient : {items}. Pour le rattacher, clic droit sur un "
            "de ses evenements dans les resultats de recherche, puis "
            "'Rattacher cet historique a un service'."
        ).format(items="; ".join(parts))

    def _get_dialog_read_conn(self):
        """Return a reusable read connection with proper PRAGMAs."""
        if self._dialog_read_conn is not None:
            return self._dialog_read_conn

        if self._journal is None or not self._journal.is_open:
            return None
        try:
            self._dialog_read_conn = self._journal.create_read_connection()
        except Exception as e:
            flog(f"_get_dialog_read_conn: {e}", "WARNING")
            return None
        return self._dialog_read_conn

    def _close_dialog_read_conn(self) -> None:
        if self._dialog_read_conn is not None:
            try:
                self._dialog_read_conn.close()
            except Exception as exc:  # noqa: BLE001
                flog(f"_close_dialog_read_conn: dialog read connection close "
                     f"failed, handle dropped anyway ({exc!r})", "DEBUG")
            self._dialog_read_conn = None

    def _run_journal_path(self):
        """The journal a run's identity proofs and coverage guard must read.

        Handed to every runner (`journal_path`): each opens its own
        read-only connection per layer group on it. None when no journal
        is open; the entry points refuse to plan anything then.
        """
        journal = self._journal
        if journal is None or not getattr(journal, "is_open", False):
            return None
        return getattr(journal, "path", None) or None

    def _live_key_columns(self, events):
        """``key_columns(datasource)`` for the collapse: the LIVE layer's key.

        Events journaled before v7 name a composite key by its first column
        only (``pk:zone_id=12`` for every parcel of zone 12). The collapse
        re-keys them from their own snapshot when it knows the layer's key
        columns (`rewind_dedup.prove_legacy_keys`); without a v7 row of the
        layer in the window it did not, and kept a key that names several
        features: the whole bucket was refused, and an INSERT of one parcel
        could cancel the DELETE of another. The layer loaded in the project
        is the authority on its key: the same `pkAttributeIndexes` capture
        reads (`identity.compute_feature_identity`). None for a datasource
        whose layer cannot be resolved or inspected: the journal's own rows
        decide then, as before.
        """
        first = {}
        for event in events or ():
            first.setdefault(event.datasource_fingerprint or "", event)
        cache = {}

        def key_columns(datasource):
            if datasource in cache:
                return cache[datasource]
            names = None
            event = first.get(datasource or "")
            if event is not None:
                # Loaded layers only (no read connection): planning happens
                # before the user confirms, and the registry fallback of
                # `find_target_layer` would add a temporary layer to his
                # project for a rewind he may decline.
                layer = find_target_layer(event, None)
                if layer is not None:
                    provider = layer.dataProvider()
                    fields = layer.fields()
                    names = [fields.at(i).name()
                             for i in (provider.pkAttributeIndexes() or [])
                             if 0 <= i < fields.count()]
            cache[datasource] = names
            flog(f"live_key_columns: datasource={str(datasource)[:60]} "
                 f"columns={names}")
            return names

        return key_columns

    def _on_layer_changed(self, _index=None) -> None:
        """React to layer combo change: update dates and ops from cache, then refresh."""
        fp = self.layer_input.currentData() or None
        if fp and not self._stats_cache.is_empty():
            stats = self._stats_cache.get(fp)
            if stats:
                self._apply_cached_ops(stats.operation_types)
                self._apply_cached_date_bounds(stats.min_date, stats.max_date)
        elif not self._stats_cache.is_empty():
            self._apply_cached_ops(self._stats_cache.global_operation_types())
            self._apply_cached_date_bounds(
                self._stats_cache.global_min_date(),
                self._stats_cache.global_max_date(),
            )
        self._request_stats_refresh()

    def _apply_cached_ops(self, op_types) -> None:
        """Update operation combo from cached operation types (instant)."""
        prev_key = self.operation_input.currentData() or "ALL"
        self.operation_input.blockSignals(True)
        self.operation_input.clear()
        present = [op for op in ("UPDATE", "DELETE", "INSERT") if op in op_types]
        if len(present) != 1:
            self.operation_input.addItem(self.tr("Toutes"), "ALL")
        for op in present:
            self.operation_input.addItem(op, op)
        idx = self.operation_input.findData(prev_key)
        if idx >= 0:
            self.operation_input.setCurrentIndex(idx)
        self.operation_input.blockSignals(False)

    def _apply_cached_date_bounds(self, min_date_str, max_date_str=None) -> None:
        """Align start_input to the journal span.

        First call: stretch start_input back to min_date.
        end_input stays at 'now' (managed by _advance_end_date_to_now).
        Subsequent calls: only raise start_input's lower bound.
        """
        if not min_date_str:
            return
        min_dt = self._parse_iso_datetime(min_date_str)
        if min_dt is None or not min_dt.isValid():
            return
        self.start_input.setMinimumDateTime(min_dt)

        if not self._initial_bounds_applied:
            self.start_input.setDateTime(min_dt)
            self._initial_bounds_applied = True
            return

        if self.start_input.dateTime() < min_dt:
            self.start_input.setDateTime(min_dt)

    def _advance_end_date_to_now(self) -> None:
        """Advance end_input to now so new events appear in the dashboard.

        Only advances the value when the user has not manually restricted
        the end date (i.e. end date is still at or near its maximum).
        Always advances the maximum so the user can reach current time.
        """
        now = QDateTime.currentDateTime()
        old_max = self.end_input.maximumDateTime()
        at_max = self.end_input.dateTime().secsTo(old_max) <= 60
        self.end_input.blockSignals(True)
        self.end_input.setMaximumDateTime(now)
        if at_max:
            self.end_input.setDateTime(now)
        self.end_input.blockSignals(False)

    def _refresh_smart_bar(self, _value=None) -> None:
        if not hasattr(self, 'smart_bar'):
            return
        path = self._journal.path if self._journal is not None else ""
        self._refresh_operation_types(self._smart_bar_summary)
        self.smart_bar.setToolTip(path or "")
        self.smart_bar.apply_state(
            self._build_smart_bar_state(self._smart_bar_summary, self._last_size_str)
        )

    def _refresh_operation_types(self, summary) -> None:
        """Update Operation combo to only show types present in the journal scope."""
        prev_key = self.operation_input.currentData() or "ALL"
        self.operation_input.blockSignals(True)
        self.operation_input.clear()
        if summary is None:
            self.operation_input.addItem(self.tr("Toutes"), "ALL")
            self.operation_input.blockSignals(False)
            return
        present = []
        if summary.update_count > 0:
            present.append("UPDATE")
        if summary.delete_count > 0:
            present.append("DELETE")
        if summary.insert_count > 0:
            present.append("INSERT")
        if len(present) != 1:
            self.operation_input.addItem(self.tr("Toutes"), "ALL")
        for op in present:
            self.operation_input.addItem(op, op)
        idx = self.operation_input.findData(prev_key)
        if idx >= 0:
            self.operation_input.setCurrentIndex(idx)
        self.operation_input.blockSignals(False)

    def _on_smart_bar_metric_activated(self, metric_key: str) -> None:
        current_key = self.operation_input.currentData() or "ALL"
        target_key = "ALL" if (metric_key != "ALL" and current_key == metric_key) else metric_key
        idx = self.operation_input.findData(target_key)
        if idx >= 0:
            self.operation_input.setCurrentIndex(idx)

    def _refresh_journal_status(self) -> None:
        """Refresh journal label, layer list and user list from local SQLite journal."""
        if not self.isVisible():
            return
        if self._journal is None or not self._journal.is_open:
            self._smart_bar_summary = None
            self._smart_bar_message_override = self.tr("Ouvrez un projet QGIS pour activer le journal local.")
            self.layer_input.blockSignals(True)
            self.layer_input.clear()
            self.layer_input.addItem(self.tr("Aucune couche sauvegardée"), "")
            self.layer_input.blockSignals(False)
            self.layer_input.setEnabled(False)
            self.operation_input.setEnabled(False)
            self.recover_button.setEnabled(False)
            self._refresh_smart_bar()
            return
        self._set_journal_info_visible(True)
        # Said BEFORE the per-layer scoping below is offered: the list of
        # layers about to be proposed hides the fact that part of their
        # history is not attached to them.
        self._warn_if_reconciliation_degraded("project")
        self._request_stats_refresh()

    def _request_stats_refresh(self) -> None:
        """Debounced trigger for background stats refresh (BL-PERF-001)."""
        self._stats_debounce_timer.start()

    def _launch_stats_worker(self) -> None:
        """Start background stats computation. Called by debounce timer."""
        if self._stats_thread is not None and self._stats_thread.isRunning():
            self._stats_thread.stop()
        criteria = self._build_search_criteria()
        self._stats_thread = JournalStatsThread(
            self._journal, criteria=criteria)
        self._stats_thread.stats_ready.connect(self._on_stats_ready)
        self._stats_thread.error_occurred.connect(self._on_stats_error)
        self._stats_thread.start()

    def _on_stats_ready(self, result) -> None:
        """Apply stats results from background thread to UI (BL-PERF-001)."""
        if result.stats_cache is not None:
            self._stats_cache = result.stats_cache
        self._last_health = result.health
        self._last_size_str = result.size_str
        self._smart_bar_summary = result.summary

        if not self._initial_bounds_applied and not self._stats_cache.is_empty():
            self._apply_cached_date_bounds(
                self._stats_cache.global_min_date(),
                self._stats_cache.global_max_date(),
            )
        self._apply_journal_layers(result.layers)
        if self.restore_mode_selector.mode() == "temporal":
            self._refresh_slider_bounds()
            if not self._is_recovering:
                self.recover_button.setEnabled(True)
        self._advance_end_date_to_now()
        self._refresh_smart_bar()

    def _on_stats_error(self, message: str) -> None:
        flog(f"_on_stats_error: {message}", "WARNING")

    def _apply_journal_layers(self, layers) -> None:
        """Apply layer list from background stats result to UI combos."""
        current_fp = self.layer_input.currentData()
        self.layer_input.blockSignals(True)
        self.layer_input.clear()
        if layers:
            self.layer_input.addItem(self.tr("Toutes les couches sauvegardées"), "")
        else:
            self.layer_input.addItem(self.tr("Aucune couche sauvegardée"), "")
        for lyr in layers:
            self.layer_input.addItem(lyr['name'], lyr['fingerprint'])
        idx = self.layer_input.findData(current_fp)
        if idx >= 0:
            self.layer_input.setCurrentIndex(idx)
        elif self.layer_input.count() > 1:
            self.layer_input.setCurrentIndex(1)
        self.layer_input.blockSignals(False)

        prev_checks = {}
        for i in range(self._version_layer_list.count()):
            it = self._version_layer_list.item(i)
            fp = it.data(QtCompat.USER_ROLE)
            if fp:
                prev_checks[fp] = it.checkState()
        self._version_layer_list.clear()
        for lyr in layers:
            item = QListWidgetItem(lyr['name'])
            item.setFlags(item.flags() | QtCompat.ITEM_IS_USER_CHECKABLE)
            saved = prev_checks.get(lyr['fingerprint'])
            item.setCheckState(saved if saved is not None else QtCompat.CHECKED)
            item.setData(QtCompat.USER_ROLE, lyr['fingerprint'])
            self._version_layer_list.addItem(item)

        has_layers = bool(layers)
        self.layer_input.setEnabled(has_layers)
        self.operation_input.setEnabled(has_layers)
        self.recover_button.setEnabled(has_layers)

    def _on_tracking_toggled(self, enabled: bool) -> None:
        """Persist toggle state and activate/deactivate the edit tracker.

        BL-UX-005: Disabling tracking has business-critical consequences
        (no future restore possible). Confirm before disabling.
        BL-UX-004: Visible log so user knows tracking state after dialog close.
        """
        if not enabled and self._is_user_initiated_toggle():
            reply = QMessageBox.question(
                self,
                self.tr("Desactiver l'enregistrement ?"),
                self.tr(
                    "Les modifications futures ne seront plus enregistrees. "
                    "Vous ne pourrez pas les restaurer ulterieurement.\n\n"
                    "Continuer ?"
                ),
                QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO,
            )
            if reply != QtCompat.MSG_YES:
                self.tracking_toggle.blockSignals(True)
                self.tracking_toggle.setChecked(True)
                self.tracking_toggle.blockSignals(False)
                return

        QgsSettings().setValue("RecoverLand/tracking_enabled", enabled)
        if self._tracker is not None:
            if enabled:
                self._tracker.activate()
            else:
                self._tracker.deactivate()
        t = resolve_theme(self)
        if enabled:
            self.tracking_label.setText(self.tr("Enregistrement actif"))
            self.tracking_label.setStyleSheet(f"color: {t.acheve.name()}; font-weight: 600;")
            qlog(self.tr("Enregistrement des modifications active."))
        else:
            self.tracking_label.setText(self.tr("Enregistrement désactivé"))
            self.tracking_label.setStyleSheet(f"color: {t.erreur.name()}; font-weight: 600;")
            qlog(self.tr(
                "Enregistrement desactive. Les modifications ne sont plus tracees."
            ), "WARNING")
        flog(f"RecoverDialog: tracking {'enabled' if enabled else 'disabled'}")
        self._refresh_smart_bar()

    def _is_user_initiated_toggle(self) -> bool:
        """True if the toggle was triggered by user click vs programmatic.

        Programmatic toggles (disk full auto-disable, init from settings)
        bypass confirmation to avoid blocking critical UX flows.
        """
        return not getattr(self, '_programmatic_toggle', False)

    def _load_tracked_layer_ids(self) -> set:
        """Return persisted tracked datasource fingerprints. Empty set = all layers."""
        settings = QgsSettings()
        raw = settings.value("RecoverLand/tracked_layer_fingerprints", None)
        if raw is None:
            raw = settings.value("RecoverLand/tracked_layers", "[]")
        try:
            ids = json.loads(raw if isinstance(raw, str) else "[]")
            if not isinstance(ids, list):
                return set()
            if any(not isinstance(val, str) or "::" not in val for val in ids):
                return set()
            return set(ids)
        except Exception:
            return set()

    def _refresh_layers_panel(self) -> None:
        from .core.identity import layer_is_selected
        """Rebuild checkboxes from currently loaded vector layers."""
        allowed = self._load_tracked_layer_ids()
        layout = self.layers_container_layout
        while layout.count() > 1:
            item = layout.takeAt(0)
            if item and item.widget():
                item.widget().deleteLater()

        layers = [
            lyr for lyr in QgsProject.instance().mapLayers().values()
            if isinstance(lyr, QgsVectorLayer)
        ]
        all_tracked = True
        for layer in layers:
            cb = QCheckBox(layer.name())
            layer_fp = compute_datasource_fingerprint(layer)
            cb.setProperty("layer_fingerprint", layer_fp)
            # Same equivalence the tracker uses: a selection persisted
            # under the pre-v6 rules must keep showing its layers ticked,
            # otherwise the panel reads 'not tracked' and the first save
            # silently drops them from the selection.
            checked = layer_is_selected(layer, allowed)
            if not checked:
                all_tracked = False
            cb.blockSignals(True)
            cb.setChecked(checked)
            cb.blockSignals(False)
            cb.stateChanged.connect(self._on_layer_filter_changed)
            layout.insertWidget(layout.count() - 1, cb)

        if not layers:
            self.layers_status_label.setText(self.tr("Aucune couche vecteur chargée"))
        elif all_tracked:
            self.layers_status_label.setText(self.tr("Toutes les couches surveillées"))
        else:
            n = sum(1 for lyr in layers if layer_is_selected(lyr, allowed))
            self.layers_status_label.setText(
                self.tr("{n} / {total} couche(s) surveillée(s)").format(n=n, total=len(layers)))

        if self._tracker is not None:
            self._tracker.set_filter(set() if all_tracked else allowed)

    def _on_layer_filter_changed(self) -> None:
        """Persist checkbox state and update tracker filter."""
        layout = self.layers_container_layout
        allowed = set()
        total = 0
        for i in range(layout.count()):
            item = layout.itemAt(i)
            if item is None:
                continue
            cb = item.widget()
            if not isinstance(cb, QCheckBox):
                continue
            total += 1
            if cb.isChecked():
                allowed.add(cb.property("layer_fingerprint"))

        all_tracked = len(allowed) == total
        save_ids = [] if all_tracked else list(allowed)
        QgsSettings().setValue("RecoverLand/tracked_layer_fingerprints", json.dumps(save_ids))

        filter_set = set() if all_tracked else allowed
        if self._tracker is not None:
            self._tracker.set_filter(filter_set)

        if all_tracked:
            self.layers_status_label.setText(self.tr("Toutes les couches surveillées"))
        else:
            self.layers_status_label.setText(
                self.tr("{n} / {total} couche(s) surveillée(s)").format(n=len(allowed), total=total))

    def _apply_global_button_style(self) -> None:
        t = resolve_theme(self)
        css = build_dialog_stylesheet(t)
        if self.styleSheet() != css:
            self.setStyleSheet(css)

    def _build_status_frame(self, t) -> QFrame:
        status_frame = QFrame()
        status_frame.setObjectName("statusFrame")
        status_frame.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        status_layout = QHBoxLayout(status_frame)
        status_layout.setContentsMargins(0, 0, 0, 0)
        status_layout.setSpacing(8)

        self.smart_bar = JournalInfoBar()
        self.smart_bar.metricActivated.connect(self._on_smart_bar_metric_activated)
        self.smart_bar.maintenanceRequested.connect(self._open_maintenance)

        tracking_panel = QWidget()
        tracking_panel.setSizePolicy(QtCompat.SIZE_FIXED, QtCompat.SIZE_FIXED)
        tracking_panel.setStyleSheet("background: transparent;")
        tracking_layout = QHBoxLayout(tracking_panel)
        tracking_layout.setContentsMargins(6, 0, 0, 0)
        tracking_layout.setSpacing(6)

        self.tracking_label = QLabel(self.tr("Enregistrement actif"))
        self.tracking_label.setStyleSheet(f"color: {t.acheve.name()}; font-weight: 600;")
        self.tracking_toggle = AppleToggleSwitch()
        self.tracking_toggle.toggled.connect(self._on_tracking_toggled)

        tracking_layout.addWidget(self.tracking_label)
        tracking_layout.addWidget(self.tracking_toggle)
        status_layout.addWidget(self.smart_bar, 1)
        self.smart_bar.add_trailing_widget(tracking_panel)
        return status_frame

    def _build_layers_group(self, t) -> QgsCollapsibleGroupBox:
        layers_group = QgsCollapsibleGroupBox()
        layers_group.setTitle(self.tr("Couches surveillées"))
        layers_group.setCollapsed(True)
        layers_vbox = QVBoxLayout()
        layers_vbox.setSpacing(6)

        layers_header = QHBoxLayout()
        self.layers_status_label = QLabel(self.tr("Toutes les couches surveillées"))
        self.layers_status_label.setStyleSheet(f"color: {t.secondaire.name()}; font-weight: 500;")
        refresh_layers_btn = QPushButton()
        refresh_layers_btn.setObjectName("rlToolBtn")
        refresh_layers_btn.setIcon(theme_icon("refresh", color=t.texte, size=16))
        refresh_layers_btn.setFixedSize(28, 28)
        refresh_layers_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        refresh_layers_btn.setToolTip(self.tr("Rafraîchir la liste"))
        refresh_layers_btn.clicked.connect(self._refresh_layers_panel)
        layers_header.addWidget(self.layers_status_label, 1)
        layers_header.addWidget(refresh_layers_btn)

        self.layers_scroll = QScrollArea()
        self.layers_scroll.setWidgetResizable(True)
        self.layers_scroll.setMaximumHeight(150)
        self.layers_scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")
        self.layers_container = QWidget()
        self.layers_container_layout = QVBoxLayout()
        self.layers_container_layout.setSpacing(4)
        self.layers_container_layout.setContentsMargins(4, 4, 4, 4)
        self.layers_container_layout.addStretch()
        self.layers_container.setLayout(self.layers_container_layout)
        self.layers_scroll.setWidget(self.layers_container)

        layers_vbox.addLayout(layers_header)
        layers_vbox.addWidget(self.layers_scroll)
        layers_group.setLayout(layers_vbox)
        layers_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        layers_group.setMinimumHeight(0)
        return layers_group

    def _build_version_layer_stack(self, t) -> QStackedWidget:
        self._version_layer_list = QListWidget()
        self._version_layer_list.setToolTip(self.tr("Cochez les couches a restaurer"))
        self._version_layer_list.setMaximumHeight(92)
        self._version_layer_btns = QWidget()
        vl_btn_layout = QHBoxLayout(self._version_layer_btns)
        vl_btn_layout.setContentsMargins(0, 0, 0, 0)
        vl_btn_layout.setSpacing(6)
        vl_btn_layout.addStretch()
        self._vl_select_all_btn = QPushButton()
        self._vl_select_all_btn.setObjectName("rlToolBtn")
        self._vl_select_all_btn.setIcon(theme_icon("select-all", color=t.texte, size=16))
        self._vl_select_all_btn.setToolTip(self.tr("Tout selectionner"))
        self._vl_select_all_btn.setFixedSize(28, 28)
        self._vl_select_all_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._vl_select_all_btn.clicked.connect(self._version_layers_check_all)
        self._vl_deselect_btn = QPushButton()
        self._vl_deselect_btn.setObjectName("rlToolBtn")
        self._vl_deselect_btn.setIcon(theme_icon("deselect-all", color=t.texte, size=16))
        self._vl_deselect_btn.setToolTip(self.tr("Tout deselectionner"))
        self._vl_deselect_btn.setFixedSize(28, 28)
        self._vl_deselect_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._vl_deselect_btn.clicked.connect(self._version_layers_uncheck_all)
        vl_btn_layout.addWidget(self._vl_select_all_btn)
        vl_btn_layout.addWidget(self._vl_deselect_btn)

        vl_page = QWidget()
        vl_lay = QVBoxLayout(vl_page)
        vl_lay.setContentsMargins(0, 0, 0, 0)
        vl_lay.setSpacing(6)
        vl_lay.addWidget(self._version_layer_list)
        vl_lay.addWidget(self._version_layer_btns)

        self._layer_sel_stack = QStackedWidget()
        self._layer_sel_stack.addWidget(QWidget())
        self._layer_sel_stack.addWidget(vl_page)
        self._layer_sel_stack.setFixedHeight(
            self._version_layer_list.maximumHeight() + self._vl_select_all_btn.height() + 6
        )
        self._layer_sel_stack.setCurrentIndex(0)
        return self._layer_sel_stack

    def _build_selection_group(self, t) -> QgsCollapsibleGroupBox:
        selection_group = QgsCollapsibleGroupBox()
        selection_group.setTitle(self.tr("Sélection"))
        selection_outer = QVBoxLayout()
        selection_outer.setSpacing(8)
        selection_row = QHBoxLayout()
        selection_row.setSpacing(10)

        self.layer_input = QComboBox()
        self.layer_input.setToolTip(self.tr("Couche dont les modifications sont enregistrées dans le journal local"))
        self.layer_input.currentIndexChanged.connect(self._on_layer_changed)

        self.operation_input = QComboBox()
        self.operation_input.addItem(self.tr("Toutes"), "ALL")
        self.operation_input.setToolTip(self.tr("Type d'opération à rechercher"))
        self.operation_input.currentIndexChanged.connect(self._refresh_smart_bar)

        selection_row.addWidget(QLabel(self.tr("Couche:")))
        selection_row.addWidget(self.layer_input, 3)
        selection_row.addWidget(QLabel(self.tr("Opération:")))
        selection_row.addWidget(self.operation_input, 1)
        selection_outer.addLayout(selection_row)
        selection_outer.addWidget(self._build_version_layer_stack(t))
        selection_group.setLayout(selection_outer)
        selection_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        selection_group.setMinimumHeight(0)
        return selection_group

    def _build_date_shortcuts_row(self, t) -> QHBoxLayout:
        date_shortcuts = QHBoxLayout()
        date_shortcuts.setSpacing(6)
        clock_icon = theme_icon("timer", color=t.secondaire, size=13)
        chip_css = (
            f"QPushButton {{ border-radius: 13px; padding: 2px 8px; font-size: 11px; "
            f"font-weight: 600; background-color: {t.surface_douce.name()}; "
            f"color: {t.texte.name()}; border: 1px solid {t.ligne_controle.name()}; }}"
            f"QPushButton:hover {{ background-color: {t.accent_doux.name()}; "
            f"color: {t.accent.name()}; border-color: {t.accent.name()}; }}"
        )
        specs = (
            ("10 min", 84, "10min", self.tr("Dernieres 10 minutes")),
            ("30 min", 84, "30min", self.tr("Dernieres 30 minutes")),
            (self.tr("1 heure"), 88, "1hour", self.tr("Derniere heure")),
            (self.tr("1 jour"), 84, "1day", self.tr("Dernieres 24 heures")),
            (self.tr("Aujourd'hui"), 104, "today", self.tr("Depuis minuit aujourd'hui")),
            (self.tr("Semaine"), 90, "week", self.tr("Depuis lundi 00:00")),
            (self.tr("Tout"), 76, "all", self.tr("Tout l'historique du journal")),
        )
        for label, width, key, tip in specs:
            btn = QPushButton(label)
            btn.setIcon(clock_icon)
            btn.setFixedSize(width, 28)
            btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
            btn.setStyleSheet(chip_css)
            btn.setToolTip(tip)
            btn.clicked.connect(lambda _c=False, k=key: self.set_period(k))
            date_shortcuts.addWidget(btn)
        date_shortcuts.addStretch()
        return date_shortcuts

    def _build_date_group(self, t) -> QgsCollapsibleGroupBox:
        date_group = QgsCollapsibleGroupBox()
        date_group.setTitle(self.tr("Période"))
        date_layout = QVBoxLayout()
        date_layout.setSpacing(8)
        self.restore_mode_selector = RestoreModeSelector()
        date_layout.addWidget(self.restore_mode_selector)
        self._period_stack = QStackedWidget()

        event_page = QWidget()
        event_vbox = QVBoxLayout(event_page)
        event_vbox.setContentsMargins(0, 0, 0, 0)
        event_vbox.setSpacing(8)

        today = QDateTime.currentDateTime()
        self.start_input = QgsDateTimeEdit()
        self.start_input.setAllowNull(False)
        self.start_input.setDateTime(today.addDays(-3650))
        self.start_input.setDisplayFormat("dd/MM/yyyy HH:mm")
        self.start_input.dateTimeChanged.connect(self._validate_dates)
        self.start_input.dateTimeChanged.connect(self._refresh_smart_bar)

        self.end_input = QgsDateTimeEdit()
        self.end_input.setAllowNull(False)
        self.end_input.setDateTime(today)
        self.end_input.setDisplayFormat("dd/MM/yyyy HH:mm")
        self.end_input.setMaximumDateTime(today)
        self.end_input.dateTimeChanged.connect(self._validate_dates)
        self.end_input.dateTimeChanged.connect(self._refresh_smart_bar)

        dates_row = QHBoxLayout()
        dates_row.setSpacing(10)
        dates_row.addWidget(QLabel(self.tr("Date debut:")))
        dates_row.addWidget(self.start_input, 1)
        dates_row.addWidget(QLabel(self.tr("Date fin:")))
        dates_row.addWidget(self.end_input, 1)

        self._dates_container = QWidget()
        dc_lay = QVBoxLayout(self._dates_container)
        dc_lay.setContentsMargins(0, 0, 0, 0)
        dc_lay.setSpacing(8)
        dc_lay.addLayout(dates_row)
        dc_lay.addLayout(self._build_date_shortcuts_row(t))
        event_vbox.addWidget(self._dates_container)

        self.time_slider = TimeSliderWidget()
        self._period_stack.addWidget(event_page)
        self._period_stack.addWidget(self.time_slider)
        self._period_stack.setCurrentIndex(0)

        self.restore_mode_selector.modeChanged.connect(self._on_period_mode_changed)
        date_layout.addWidget(self._period_stack)
        date_group.setLayout(date_layout)
        date_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        date_group.setMinimumHeight(0)
        return date_group

    def _build_options_group(self, t) -> QgsCollapsibleGroupBox:
        options_group = QgsCollapsibleGroupBox(self.tr("Options"))
        options_layout = QHBoxLayout()
        options_layout.setSpacing(20)

        self.auto_zoom_check = QCheckBox(self.tr("Zoomer automatiquement sur les résultats"))
        self.auto_zoom_check.setIcon(theme_icon("zoom-selection", color=t.accent, size=16))
        self.auto_zoom_check.setChecked(True)

        self.open_attribute_check = QCheckBox(self.tr("Ouvrir la table d'attributs après chargement"))
        self.open_attribute_check.setIcon(theme_icon("table", color=t.accent, size=16))
        self.open_attribute_check.setChecked(False)

        options_layout.addWidget(self.auto_zoom_check)
        options_layout.addWidget(self.open_attribute_check)
        options_layout.addStretch()
        options_group.setLayout(options_layout)
        options_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        options_group.setMinimumHeight(0)
        return options_group

    def _build_results_selection_row(self, t) -> QHBoxLayout:
        selection_buttons_layout = QHBoxLayout()
        selection_buttons_layout.setSpacing(8)
        self.select_all_button = QPushButton(self.tr("Tout sélectionner"))
        self.select_all_button.setIcon(theme_icon("select-all", color=t.texte, size=16))
        self.select_all_button.setFixedHeight(34)
        self.select_all_button.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.select_all_button.clicked.connect(self.select_all_rows)
        self.select_all_button.setEnabled(False)
        self.select_all_button.setToolTip(self.tr("Sélectionner toutes les lignes du tableau"))

        self.select_none_button = QPushButton(self.tr("Tout désélectionner"))
        self.select_none_button.setIcon(theme_icon("deselect-all", color=t.texte, size=16))
        self.select_none_button.setFixedHeight(34)
        self.select_none_button.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.select_none_button.clicked.connect(self.select_none_rows)
        self.select_none_button.setEnabled(False)
        self.select_none_button.setToolTip(self.tr("Désélectionner toutes les lignes du tableau"))

        self.selection_count_label = QLabel("")
        self.selection_count_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 11px; font-weight: 700; "
            f"color: {t.accent.name()};"
        )
        selection_buttons_layout.addWidget(self.select_all_button)
        selection_buttons_layout.addWidget(self.select_none_button)
        selection_buttons_layout.addStretch()
        selection_buttons_layout.addWidget(self.selection_count_label)
        return selection_buttons_layout

    def _build_results_group(self, t) -> QgsCollapsibleGroupBox:
        results_group = QgsCollapsibleGroupBox()
        results_group.setTitle(self.tr("Résultats"))
        results_group.setCollapsed(True)
        results_layout = QVBoxLayout()
        results_layout.setSpacing(8)
        self.results_info_label = QLabel(self.tr("Aucune donnée récupérée"))
        self.results_info_label.setStyleSheet(f"color: {t.secondaire.name()}; font-weight: 500;")

        self.search_filter = QLineEdit()
        self.search_filter.setPlaceholderText(self.tr("Rechercher (GID, utilisateur, ...)"))
        self.search_filter.setClearButtonEnabled(True)
        self.search_filter.setToolTip(self.tr("Filtrer les résultats en temps réel"))
        self.search_filter.textChanged.connect(self._filter_results)
        self.search_filter.setVisible(False)

        self.change_legend = self._build_change_legend()
        self.change_legend.setVisible(False)

        self.table_widget = QTableWidget()
        self.table_widget.setSelectionBehavior(QtCompat.SELECT_ROWS)
        self.table_widget.setAlternatingRowColors(False)
        self.table_widget.horizontalHeader().setStretchLastSection(True)
        self.table_widget.setMinimumHeight(120)
        self.table_widget.setSortingEnabled(True)
        self.table_widget.setSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_PREFERRED)
        self.table_widget.itemSelectionChanged.connect(self.on_selection_changed)
        self.table_widget.setContextMenuPolicy(QtCompat.CUSTOM_CONTEXT_MENU)
        self.table_widget.customContextMenuRequested.connect(self._show_results_context_menu)

        results_layout.addWidget(self.results_info_label)
        results_layout.addWidget(self.search_filter)
        results_layout.addWidget(self.change_legend)
        results_layout.addWidget(self.table_widget)
        results_layout.addLayout(self._build_results_selection_row(t))
        results_group.setLayout(results_layout)
        results_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_EXPANDING)
        results_group.setMinimumHeight(0)
        return results_group

    def _build_action_bar_segments(self) -> list:
        return [
            {
                "label": self.tr("Fermer"),
                "icon": theme_icon("close", size=16),
                "icon_name": "close",
                "tooltip": self.tr("Fermer"),
                "enabled": True,
                "visible": True,
            },
            {
                "label": "Recover",
                "icon": theme_icon("refresh", size=16),
                "icon_name": "refresh",
                "tooltip": self.tr("Lancer la recherche (F5)"),
                "enabled": True,
                "visible": True,
            },
            {
                "label": "Restore",
                "icon": theme_icon("restore", size=16),
                "icon_name": "restore",
                "tooltip": "",
                "enabled": False,
                "visible": True,
            },
            {
                "label": "Last",
                "icon": theme_icon("undo", size=16),
                "icon_name": "undo",
                "tooltip": self.tr("Annuler le dernier restore"),
                "enabled": False,
                "visible": True,
            },
        ]

    def _build_footer_bar(self, t) -> QHBoxLayout:
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        button_height = 36

        self._action_bar = ActionButtonBar(self._build_action_bar_segments(), self)
        self._action_bar.setPrimaryIndex(-1)
        self.cancel_button = self._action_bar.segment(0)
        self.recover_button = self._action_bar.segment(1)
        self.restore_button = self._action_bar.segment(2)
        self.undo_last_btn = self._action_bar.segment(3)
        self.cancel_button.clicked.connect(self.cancel_operation)
        self.recover_button.clicked.connect(self.recover_and_load)
        self.restore_button.clicked.connect(self.restore_selected_data)
        self.undo_last_btn.clicked.connect(self._undo_last_restore)

        self._glow_color = QColor(t.accent.red(), t.accent.green(), t.accent.blue(), 180)
        self._apply_logo_glow_effect()

        self.help_button = QPushButton("")
        self.help_button.setObjectName("rlToolBtn")
        self.help_button.setIcon(theme_icon("help", color=t.texte, size=18))
        self.help_button.setFixedSize(button_height, button_height)
        self.help_button.setToolTip(self.tr("Ouvrir la documentation"))
        self.help_button.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.help_button.clicked.connect(self._open_help)

        self._review_toggle = ReviewSegmentedSwitch(self)
        self._review_toggle.setToolTip(self.tr("Activer/desactiver la visualisation Review"))
        self._review_toggle.setVisible(False)
        self._review_toggle.toggled.connect(self._on_review_toggle)

        self._review_viz_combo = QComboBox(self)
        self._review_viz_combo.addItem(self.tr("Snapshot (état à T)"), "snapshot_at_t")
        self._review_viz_combo.setVisible(False)

        right_spacer = QWidget(self)
        right_spacer.setFixedSize(button_height, button_height)

        button_layout.addWidget(self.help_button)
        button_layout.addStretch()
        button_layout.addWidget(self._action_bar)
        button_layout.addWidget(self._review_viz_combo)
        button_layout.addWidget(self._review_toggle)
        button_layout.addStretch()
        button_layout.addWidget(right_spacer)
        return button_layout

    def setup_ui(self):
        t = resolve_theme(self)
        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)

        logo_label = ThemedLogoWidget()
        logo_label.setSizePolicy(QtCompat.SIZE_FIXED, QtCompat.SIZE_FIXED)
        self.logo_label = logo_label
        self._load_themed_logo()

        status_frame = self._build_status_frame(t)
        layers_group = self._build_layers_group(t)
        selection_group = self._build_selection_group(t)
        date_group = self._build_date_group(t)
        options_group = self._build_options_group(t)
        self.results_group = self._build_results_group(t)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        self._progress_smoother = QVariantAnimation(self)
        self._progress_smoother.setDuration(240)
        self._progress_smoother.setEasingCurve(QtCompat.EASE_OUT_CUBIC)
        self._progress_smoother.valueChanged.connect(
            self._on_progress_smoother_value)
        self.progress_phase_label = QLabel("")
        self.progress_phase_label.setStyleSheet(
            f"color: {t.secondaire.name()}; font-size: 11px; font-weight: 600;"
        )
        self.progress_phase_label.setVisible(False)
        flog(
            f"setup_ui: progress_bar created, id={id(self.progress_bar)},"
            f" visible={self.progress_bar.isVisible()},"
            f" parent={self.progress_bar.parent()}"
        )

        button_layout = self._build_footer_bar(t)
        main_layout.addWidget(logo_label, 0, QtCompat.ALIGN_HCENTER)
        main_layout.addWidget(status_frame)
        main_layout.addWidget(layers_group)
        main_layout.addWidget(selection_group)
        main_layout.addWidget(date_group)
        main_layout.addWidget(options_group)
        main_layout.addWidget(self.results_group, 1)
        main_layout.addWidget(self.progress_phase_label)
        main_layout.addWidget(self.progress_bar)
        main_layout.addLayout(button_layout)
        self.setLayout(main_layout)

        self._on_period_mode_changed(self.restore_mode_selector.mode())
        self._setup_shortcuts()

    def _open_help(self) -> None:
        """Open local HTML documentation in the default browser."""
        from qgis.PyQt.QtCore import QUrl
        from qgis.PyQt.QtGui import QDesktopServices
        docs_path = os.path.join(os.path.dirname(__file__), "docs", "index.html")
        if not os.path.isfile(docs_path):
            flog(f"RecoverDialog: docs not found at {docs_path}", "WARNING")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(docs_path))

    def _setup_shortcuts(self) -> None:
        """UX-F02: Register keyboard shortcuts with tooltip hints."""
        QShortcut(QKeySequence("F5"), self, self.recover_and_load)
        QShortcut(QKeySequence("Ctrl+F"), self, self._focus_search_filter)
        QShortcut(QKeySequence("F1"), self, self._open_help)
        self.recover_button.setToolTip(self.tr("Lancer la recherche (F5)"))
        self.search_filter.setToolTip(self.tr("Filtrer les résultats (Ctrl+F)"))
        self.help_button.setToolTip(self.tr("Ouvrir la documentation (F1)"))

    def _focus_search_filter(self) -> None:
        if self.search_filter.isVisible():
            self.search_filter.setFocus()
            self.search_filter.selectAll()

    def _show_results_context_menu(self, pos) -> None:
        """UX-C03: Context menu on results table."""
        if self.table_widget.rowCount() == 0:
            return
        menu = QMenu(self)
        selected = self.table_widget.selectionModel().selectedRows()
        if selected:
            restore_act = menu.addAction(
                theme_icon("restore", size=16),
                self.tr("Restaurer ({count} ligne(s))").format(count=len(selected)))
            restore_act.triggered.connect(self.restore_selected_data)
            menu.addSeparator()
        copy_act = menu.addAction(self.tr("Copier les valeurs"))
        copy_act.triggered.connect(self._copy_selected_to_clipboard)
        if selected and len(selected) == 1:
            row = selected[0].row()
            if row < len(self._search_events):
                event = self._search_events[row]
                layer_name = event.layer_name_snapshot or ""
                if layer_name:
                    filter_act = menu.addAction(
                        self.tr("Filtrer sur '{name}'").format(name=layer_name))
                    filter_act.triggered.connect(
                        lambda: self._filter_on_layer(event.datasource_fingerprint))
                self._append_undo_session_menu(menu, event)
                self._append_service_history_menu(menu, event)
        menu.addSeparator()
        sel_all = menu.addAction(self.tr("Tout selectionner"))
        sel_all.triggered.connect(self.select_all_rows)
        desel = menu.addAction(self.tr("Tout deselectionner"))
        desel.triggered.connect(self.select_none_rows)
        global_pos = self.table_widget.viewport().mapToGlobal(pos)
        # exec() only: measured present on Qt5 (QGIS 3.40.15) as well as Qt6,
        # while exec_() is ABSENT on Qt6. The old else-branch could therefore
        # never run on any supported runtime, and only PyQt6 could ever need it.
        menu.exec(global_pos)

    def _copy_selected_to_clipboard(self) -> None:
        """Copy selected rows to clipboard in tab-separated format."""
        rows = sorted(set(idx.row() for idx in self.table_widget.selectionModel().selectedRows()))
        if not rows:
            return
        lines = []
        headers = []
        for c in range(self.table_widget.columnCount()):
            h = self.table_widget.horizontalHeaderItem(c)
            headers.append(h.text() if h else "")
        lines.append("\t".join(headers))
        for r in rows:
            cells = []
            for c in range(self.table_widget.columnCount()):
                item = self.table_widget.item(r, c)
                cells.append(item.text() if item else "")
            lines.append("\t".join(cells))
        QApplication.clipboard().setText("\n".join(lines))
        qlog(self.tr("{count} ligne(s) copiee(s).").format(count=len(rows)))

    def _filter_on_layer(self, fingerprint: str) -> None:
        """Set layer combo to the given fingerprint."""
        idx = self.layer_input.findData(fingerprint)
        if idx >= 0:
            self.layer_input.setCurrentIndex(idx)

    def _get_write_queue_pending(self) -> int:
        """UX-A03: Return pending event count from write queue."""
        if self._write_queue is None:
            return 0
        try:
            return self._write_queue.pending_count
        except Exception:
            return 0

    def _open_maintenance(self) -> None:
        """UX-D01: Open the journal maintenance dialog."""
        dlg = JournalMaintenanceDialog(self._journal, parent=self)
        dlg.exec()  # exec_() is absent on Qt6; exec() exists on both. See above.
        self._stats_cache.invalidate()
        self._close_dialog_read_conn()
        self._refresh_journal_status()
        flog("_open_maintenance: stats invalidated and refresh requested after maintenance")

    def _build_empty_result_suggestion(self) -> str:
        """UX-B02: Build contextual suggestion when search returns 0 results."""
        parts = []
        if not self.tracking_toggle.isChecked():
            parts.append(self.tr(
                "L'enregistrement est desactive. "
                "Activez-le pour capturer les modifications."))
            return " ".join(parts)
        start = self.start_input.dateTime()
        end = self.end_input.dateTime()
        span_secs = start.secsTo(end)
        if span_secs < 3600:
            parts.append(self.tr("La periode est courte (< 1h). Essayez d'elargir a 24h ou a toute la periode."))
        if self.layer_input.currentData():
            parts.append(self.tr(
                "Un filtre de couche est actif. "
                "Essayez 'Toutes les couches sauvegardees'."))
        if (self.operation_input.currentData() or "ALL") != "ALL":
            parts.append(self.tr(
                "Filtre operation actif ({op}). "
                "Essayez 'Toutes'.").format(op=self.operation_input.currentText()))
        if not parts:
            parts.append(self.tr("Aucun evenement pour ces criteres. Verifiez la periode et la couche."))
        return " ".join(parts)

    def _apply_glow_effect(self, widget, color: QColor) -> None:
        """Apply animated neon glow effect to widget."""
        effect = QGraphicsDropShadowEffect(widget)
        effect.setBlurRadius(12)
        effect.setOffset(0, 0)
        effect.setColor(color)
        widget.setGraphicsEffect(effect)

        widget.setProperty("glow_color", color)
        widget.setProperty("glow_base_blur", 12)
        widget.setProperty("glow_hover_blur", 25)
        widget.setProperty("glow_base_alpha", color.alpha())
        widget.setProperty("glow_hover_alpha", min(255, color.alpha() + 75))

        widget.installEventFilter(self)

    def _apply_logo_glow_effect(self) -> None:
        """Retire tout QGraphicsDropShadowEffect du logo pour eviter le flou CPU par frame."""
        if hasattr(self, "logo_label") and self.logo_label is not None:
            self.logo_label.setGraphicsEffect(None)

    def _start_logo_activity(self, color: QColor, mode: str) -> None:
        if mode == "recover":
            self.logo_label.start_recovery_effect(color)
        elif mode == "restore":
            self.logo_label.start_restore_effect(color)
        else:
            raise ValueError(f"Unknown logo activity mode: {mode}")

    def _stop_logo_activity(self) -> None:
        if not hasattr(self, 'logo_label'):
            return
        self.logo_label.stop_recovery_effect()

    def _pulse_logo_glow(self, color: QColor) -> None:
        """Conserve pour compatibilite interne sans declencher une seconde animation."""
        return

    def changeEvent(self, event):
        """Re-render logo and dialog theme when QGIS palette changes (dark/light mode)."""
        super().changeEvent(event)
        if getattr(self, '_refreshing_theme', False):
            return
        if event.type() == QtCompat.EVENT_PALETTE_CHANGE and hasattr(self, 'logo_label'):
            self._refreshing_theme = True
            try:
                self._apply_global_button_style()
                self._load_themed_logo()
                self._refresh_smart_bar()
            finally:
                self._refreshing_theme = False

    def eventFilter(self, obj, event) -> bool:
        """Handle hover events for glow animation."""
        if event.type() == QtCompat.EVENT_ENTER:
            self._animate_glow(obj, hover_in=True)
        elif event.type() == QtCompat.EVENT_LEAVE:
            self._animate_glow(obj, hover_in=False)
        return super().eventFilter(obj, event)

    def _animate_glow(self, widget, hover_in: bool) -> None:
        """Animate glow effect on hover."""
        effect = widget.graphicsEffect()
        if not isinstance(effect, QGraphicsDropShadowEffect):
            return

        base_blur = widget.property("glow_base_blur") or 12
        hover_blur = widget.property("glow_hover_blur") or 25
        base_alpha = widget.property("glow_base_alpha") or 180
        hover_alpha = widget.property("glow_hover_alpha") or 255
        color = widget.property("glow_color")

        if not color:
            return

        start_blur = effect.blurRadius()
        end_blur = hover_blur if hover_in else base_blur
        start_alpha = effect.color().alpha()
        end_alpha = hover_alpha if hover_in else base_alpha

        anim = QVariantAnimation(widget)
        anim.setDuration(200)
        anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        anim.setStartValue(0.0)
        anim.setEndValue(1.0)

        def update_effect(progress):
            blur = start_blur + (end_blur - start_blur) * progress
            alpha = int(start_alpha + (end_alpha - start_alpha) * progress)
            effect.setBlurRadius(blur)
            new_color = QColor(color.red(), color.green(), color.blue(), alpha)
            effect.setColor(new_color)

        anim.valueChanged.connect(update_effect)
        anim.start()

    def _load_themed_logo(self):
        """Load logo.svg with text color adapted to the current QGIS theme."""
        logo_path = os.path.join(os.path.dirname(__file__), "logo.svg")
        if not os.path.exists(logo_path):
            self.logo_label.update()
            return

        try:
            text_color = self.palette().windowText().color()
            color_hex = text_color.name()

            with open(logo_path, 'r', encoding='utf-8') as f:
                svg_template = f.read()

            svg_data = svg_template.replace("{TEXT_COLOR}", color_hex)
            self.logo_label.load_svg_data(svg_data)
        except Exception as e:
            flog(f"_load_themed_logo error: {e}", "WARNING")
            self.logo_label.update()

    def _filter_results(self, text: str):
        """Filter results table rows based on search text."""
        search = text.lower().strip()
        for row in range(self.table_widget.rowCount()):
            match = False
            if not search:
                match = True
            else:
                for col in range(self.table_widget.columnCount()):
                    item = self.table_widget.item(row, col)
                    if item and search in item.text().lower():
                        match = True
                        break
            self.table_widget.setRowHidden(row, not match)

    def _build_event_diff_tooltip(self, event, parsed_data, attrs: dict,
                                  changed_keys: set, geom_changed: bool) -> str:
        """US-4.10.03: Build a rich HTML tooltip describing this event's diff.

        For UPDATE : two-column before/after of modified fields (max 10).
        For INSERT : list of created attribute values (max 10).
        For DELETE : list of pre-deletion attribute values (max 10).
        Includes a geometry note if relevant. Never includes user_name or
        other identity fields (A3 of US-03 - PII discipline).
        """
        op = event.operation_type or ""
        header_parts = [f"<b>{op}</b>"]
        if event.layer_name_snapshot:
            header_parts.append(f"<i>{event.layer_name_snapshot}</i>")
        if geom_changed:
            header_parts.append(self.tr("Geometrie modifiee"))

        rows_html = []
        if op == "UPDATE" and parsed_data and "changed_only" in parsed_data:
            changes = parsed_data["changed_only"]
            items = [(k, v) for k, v in changes.items()
                     if not is_layer_audit_field(k)]
            for k, change in items[:10]:
                if isinstance(change, dict):
                    old = change.get("old", "")
                    new = change.get("new", "")
                    rows_html.append(
                        f"<tr><td><b>{k}</b></td>"
                        f"<td style='color:#888'>{old}</td>"
                        f"<td>&rarr;</td>"
                        f"<td>{new}</td></tr>")
            if len(items) > 10:
                rows_html.append(
                    f"<tr><td colspan='4'><i>"
                    f"...et {len(items) - 10} autre(s)</i></td></tr>")
        elif op in ("INSERT", "DELETE"):
            items = [(k, v) for k, v in attrs.items()
                     if not is_layer_audit_field(k)]
            label = (self.tr("Valeurs creees")
                     if op == "INSERT" else self.tr("Valeurs supprimees"))
            header_parts.append(label)
            for k, v in items[:10]:
                rows_html.append(
                    f"<tr><td><b>{k}</b></td><td>{v}</td></tr>")
            if len(items) > 10:
                rows_html.append(
                    f"<tr><td colspan='2'><i>"
                    f"...et {len(items) - 10} autre(s)</i></td></tr>")

        html = "<br>".join(header_parts)
        if rows_html:
            html += "<table>" + "".join(rows_html) + "</table>"
        return html

    def _build_change_legend(self):
        """Build color legend widget with icons for accessibility (UX-E03)."""
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 2, 0, 2)
        layout.setSpacing(4)
        for change_type in ("modified", "emptied", "populated", "geometry"):
            color = CHANGE_TYPE_COLORS[change_type]
            r, g, b, a = color.red(), color.green(), color.blue(), color.alpha()
            swatch = QWidget()
            swatch.setFixedSize(12, 12)
            swatch.setStyleSheet(
                f"background:rgba({r},{g},{b},{a});"
                f"border:1px solid rgba({r},{g},{b},140);"
                f"border-radius:2px;"
            )
            label = QLabel(_change_type_labels()[change_type])
            label.setStyleSheet("font-size:11px; padding-left:2px; padding-right:8px;")
            layout.addWidget(swatch)
            layout.addWidget(label)
        layout.addStretch()
        self._modified_only_check = QCheckBox(self.tr("Modifications uniquement"))
        self._modified_only_check.setToolTip(self.tr("Afficher uniquement les colonnes modifiées"))
        self._modified_only_check.setStyleSheet("font-size:11px;")
        self._modified_only_check.stateChanged.connect(self._toggle_modified_columns)
        layout.addWidget(self._modified_only_check)
        return container

    def _toggle_modified_columns(self):
        """Show only modified columns when checkbox is checked (UX-E04)."""
        show_only_modified = self._modified_only_check.isChecked()
        QgsSettings().setValue("RecoverLand/modified_columns_only", show_only_modified)
        if not show_only_modified:
            for col in range(self.table_widget.columnCount()):
                self.table_widget.setColumnHidden(col, False)
            return
        always_visible = {'gid', 'user_name', 'audit_timestamp'}
        for col in range(self.table_widget.columnCount()):
            header = self.table_widget.horizontalHeaderItem(col)
            col_name = header.text() if header else ""
            keep = col in self._modified_col_indices or col_name in always_visible
            self.table_widget.setColumnHidden(col, not keep)

    def _on_period_mode_changed(self, mode: str) -> None:
        """Switch period stack and layer widget between event, version and review modes."""
        flog(f"period_mode: switched to {mode}")
        is_version = (mode == "temporal")
        is_review = (mode == "review")
        if is_version:
            self._review_stop_session()
            self._review_toggle.blockSignals(True)
            self._review_toggle.setChecked(False)
            self._review_toggle.blockSignals(False)
            self._review_toggle.setVisible(False)
            self._review_viz_combo.setVisible(False)
            self.recover_button.setVisible(True)
            self.cancel_button.setVisible(True)
            self.undo_last_btn.setVisible(True)
            self._period_stack.setCurrentIndex(1)
            self.layer_input.setVisible(False)
            self._layer_sel_stack.setCurrentIndex(1)
            self.results_group.setVisible(False)
            self.restore_button.setVisible(False)
            self.recover_button.setText(self.tr("Rewind"))
            self.recover_button.setIcon(theme_icon("rewind", size=16))
            if hasattr(self.recover_button, "setIconName"):
                self.recover_button.setIconName("rewind")
            self._action_bar.setPrimaryIndex(1)
            # A restore or an Undo in flight does not raise _is_recovering:
            # reopening the Rewind button under it offered a second run.
            if not self._is_recovering and not self._run_in_flight():
                self.recover_button.setEnabled(True)
            self._refresh_slider_bounds()
        elif is_review:
            self._period_stack.setCurrentIndex(0)
            self._dates_container.setVisible(False)
            self.layer_input.setVisible(False)
            self._layer_sel_stack.setCurrentIndex(1)
            self.results_group.setVisible(False)
            self.restore_button.setVisible(False)
            self.recover_button.setVisible(False)
            self.cancel_button.setVisible(False)
            self.undo_last_btn.setVisible(False)
            self._review_toggle.setVisible(True)
        else:
            self._dates_container.setVisible(True)
            self._review_stop_session()
            self._review_toggle.blockSignals(True)
            self._review_toggle.setChecked(False)
            self._review_toggle.blockSignals(False)
            self._review_toggle.setVisible(False)
            self._review_viz_combo.setVisible(False)
            self.recover_button.setVisible(True)
            self.cancel_button.setVisible(True)
            self.undo_last_btn.setVisible(True)
            self._period_stack.setCurrentIndex(0)
            self._layer_sel_stack.setCurrentIndex(0)
            self.layer_input.setVisible(True)
            has_rows = self.table_widget is not None and self.table_widget.rowCount() > 0
            self.results_group.setVisible(has_rows)
            self.restore_button.setVisible(True)
            self.recover_button.setText(self.tr("Recover"))
            self.recover_button.setIcon(theme_icon("refresh", size=16))
            if hasattr(self.recover_button, "setIconName"):
                self.recover_button.setIconName("refresh")
            self._action_bar.setPrimaryIndex(2 if self.restore_button.isEnabled() else 1)

    def _version_layers_check_all(self) -> None:
        for i in range(self._version_layer_list.count()):
            self._version_layer_list.item(i).setCheckState(QtCompat.CHECKED)

    def _version_layers_uncheck_all(self) -> None:
        for i in range(self._version_layer_list.count()):
            self._version_layer_list.item(i).setCheckState(QtCompat.UNCHECKED)

    _SLIDER_DEFAULT_OFFSET_SECS = 600

    def _refresh_slider_bounds(self) -> None:
        """Configure slider bounds from the stats cache (no live query).

        The slider minimum is set 1 second before the oldest event so that
        when the slider is at its lowest position, the exclusive cutoff (>)
        still includes the oldest event.  Without this offset, events at
        exactly the oldest timestamp are excluded and the rewind finds 0
        results - the root cause of "rewind broken after purge".
        """
        if self._stats_cache.is_empty():
            self.time_slider.disable()
            return
        oldest_str = self._stats_cache.global_min_date()
        if not oldest_str:
            self.time_slider.disable()
            return
        oldest_dt = self._parse_iso_datetime(oldest_str)
        if oldest_dt is None or not oldest_dt.isValid():
            flog(f"slider_bounds: cannot parse cached min date: {oldest_str}", "WARNING")
            self.time_slider.disable()
            return
        oldest_dt = oldest_dt.addSecs(-1)
        newest_dt = QDateTime.currentDateTime()
        max_str = self._stats_cache.global_max_date()
        initial_dt = None
        if max_str:
            max_dt = self._parse_iso_datetime(max_str)
            if max_dt is not None and max_dt.isValid():
                initial_dt = max_dt.addSecs(-self._SLIDER_DEFAULT_OFFSET_SECS)
        self.time_slider.set_bounds(oldest_dt, newest_dt, initial=initial_dt)

    @staticmethod
    def _parse_iso_datetime(iso_str: str):
        """Parse ISO datetime string to QDateTime (UTC).

        All stored timestamps in RecoverLand are UTC. The parsed
        QDateTime is marked as UTC so that .toUTC() is a no-op and
        .toLocalTime() converts correctly for display widgets.
        """
        clean = iso_str.strip()
        if '+' in clean[10:]:
            clean = clean[:clean.index('+', 10)]
        elif clean.endswith('Z'):
            clean = clean[:-1]
        if '.' in clean:
            clean = clean[:clean.index('.')]
        dt = QDateTime.fromString(clean, "yyyy-MM-ddTHH:mm:ss")
        if not dt.isValid():
            dt = QDateTime.fromString(clean, "yyyy-MM-dd HH:mm:ss")
        if dt.isValid():
            dt.setTimeSpec(QtCompat.UTC)
        return dt

    def _get_version_checked_fingerprints(self) -> list:
        """Return list of fingerprints checked in the version layer list."""
        fps = []
        for i in range(self._version_layer_list.count()):
            item = self._version_layer_list.item(i)
            if item.checkState() == QtCompat.CHECKED:
                fp = item.data(QtCompat.USER_ROLE)
                if fp:
                    fps.append(fp)
        return fps

    def _validate_dates(self):
        """Real-time date validation: disable recover button if dates are invalid."""
        if hasattr(self, 'restore_mode_selector') and self.restore_mode_selector.mode() == "temporal":
            return
        start_dt = self.start_input.dateTime()
        end_dt = self.end_input.dateTime()
        is_valid = start_dt < end_dt
        if hasattr(self, 'recover_button') and self.recover_button.isEnabled() != is_valid:
            if not self._is_recovering:
                self.recover_button.setEnabled(is_valid)
        if not is_valid and start_dt == end_dt:
            pass

    def update_phase(self, phase_text: str):
        """Update progress phase label from thread signal."""
        self.progress_phase_label.setText(phase_text)
        self.progress_phase_label.setVisible(True)

    def set_period(self, period):
        """Set period from shortcut buttons."""
        flog(
            f"set_period: called period={period} "
            f"start_enabled={self.start_input.isEnabled()} "
            f"end_enabled={self.end_input.isEnabled()}",
            "DEBUG",
        )
        today = QDateTime.currentDateTime()
        self.end_input.setMaximumDateTime(today)
        ms_today = today.time().msecsSinceStartOfDay()
        midnight = today.addMSecs(-ms_today)

        if period == "today":
            self.start_input.setDateTime(midnight)
            self.end_input.setDateTime(today)
        elif period == "week":
            dow = int(QDate.currentDate().dayOfWeek()) - 1
            self.start_input.setDateTime(midnight.addDays(-dow))
            self.end_input.setDateTime(today)
        elif period == "1day":
            self.start_input.setDateTime(today.addSecs(-86400))
            self.end_input.setDateTime(today)
        elif period == "1hour":
            self.start_input.setDateTime(today.addSecs(-3600))
            self.end_input.setDateTime(today)
        elif period == "30min":
            self.start_input.setDateTime(today.addSecs(-1800))
            self.end_input.setDateTime(today)
        elif period == "10min":
            self.start_input.setDateTime(today.addSecs(-600))
            self.end_input.setDateTime(today)
        elif period == "all":
            self.start_input.setDateTime(self.start_input.minimumDateTime())
            self.end_input.setDateTime(today)
        elif isinstance(period, int):
            if period == 0:
                self.start_input.setDateTime(midnight)
                self.end_input.setDateTime(today)
            elif period == 1:
                yesterday_start = midnight.addDays(-1)
                yesterday_end = midnight.addSecs(-1)
                self.start_input.setDateTime(yesterday_start)
                self.end_input.setDateTime(yesterday_end)
            else:
                self.start_input.setDateTime(today.addDays(-period))
                self.end_input.setDateTime(today)

    def pulse_progress_bar(self):
        """Arrete le timer de pulsation sans basculer la barre en animation indeterminee."""
        if self.progress_timer.isActive():
            self.progress_timer.stop()

    def update_progress(self, value):
        """Update progress bar value.

        Called from thread signals dispatched via Qt event loop.
        Routes through ``_smooth_set_progress`` so the bar glides instead
        of jumping in coarse steps (the runners emit per chunk, not per
        event, so raw setValue would look like a staircase).
        """
        self._smooth_set_progress(value)

    def _on_progress_smoother_value(self, val) -> None:
        """Slot for ``_progress_smoother.valueChanged``.

        QVariantAnimation feeds intermediate floats; the progress bar
        accepts ints only, so we round here.
        """
        if self.progress_bar.maximum() == 0:
            return
        self.progress_bar.setValue(int(round(float(val))))

    def _smooth_set_progress(self, target_value: int) -> None:
        """Met a jour progress_bar directement en O(1) sans QVariantAnimation concurrente."""
        if self.progress_bar.maximum() == 0:
            self.progress_bar.setRange(0, 100)
        if self._progress_smoother.state() == QtCompat.ANIM_STATE_RUNNING:
            self._progress_smoother.stop()
        clamped = max(0, min(100, int(target_value)))
        if clamped != self.progress_bar.value():
            self.progress_bar.setValue(clamped)

    def on_events_committed(self, edited_fingerprint="") -> None:
        """Auto-refresh smart bar and event table after new events are flushed.

        Called by the plugin after a short delay post-commit so the
        WriteQueue has time to flush.
        """
        # The user edited this layer after a restore: its undo state is
        # stale whatever the dialog shows. Kept behind the visibility test,
        # an edit made while the dialog was closed left it armed, and the
        # next Undo or auto-undo wrote the event's NEW values over that
        # edit. The run's own writes are suppressed, so they never land
        # here.
        self._invalidate_undo_for(edited_fingerprint)
        if not self.isVisible():
            return
        if self._is_recovering or self._run_in_flight():
            return
        self._close_dialog_read_conn()
        self._request_stats_refresh()
        flog("on_events_committed: stats refresh requested")
        if self.restore_mode_selector.mode() != "event":
            return
        if not hasattr(self, '_search_events') or not self._search_events:
            return
        flog("on_events_committed: auto-refreshing event table")
        self.recover_and_load()

    def recover_and_load(self):
        """Launch recovery from local SQLite journal."""
        flog("recover_and_load: START")
        # F5 and the Review toggle reach this without the Rewind button: a
        # run that writes the layers (event restore, Undo) does not raise
        # `_is_recovering`, so that test alone let a Rewind start beside it.
        # Checked before the button is touched: the run owns it.
        if self._refuse_while_busy(self.tr("Recherche ou Rewind"),
                                   check_recovering=False):
            return
        self._closing = False
        self.recover_button.setEnabled(False)
        if not self.validate_inputs():
            self.recover_button.setEnabled(True)
            return
        if self._is_recovering:
            flog("recover_and_load: blocked, restore already in progress", "WARNING")
            self.recover_button.setEnabled(True)
            return
        self._is_recovering = True
        self.enable_controls(False)
        self.progress_bar.setValue(0)

        current_mode = self.restore_mode_selector.mode()
        if current_mode == "temporal":
            self._recover_version_mode()
        elif current_mode == "review":
            self._recover_review_mode()
        else:
            self._recover_event_mode()

    def _resolve_layer_by_fingerprint(self, fingerprint: str):
        """Find the QgsVectorLayer in the project matching *fingerprint*."""
        from .core.identity import compute_datasource_fingerprint
        for lyr in QgsProject.instance().mapLayers().values():
            if not hasattr(lyr, 'dataProvider'):
                continue
            try:
                if compute_datasource_fingerprint(lyr) == fingerprint:
                    return lyr
            except Exception as exc:  # noqa: BLE001
                # `continue` stays: an unfingerprintable layer cannot match,
                # and skipping it lets the search reach the next candidate.
                # No lyr.id() in the message -- the usual cause is a deleted
                # C++ wrapper, whose id() would raise inside this handler.
                flog(f"_resolve_layer_by_fingerprint: fingerprint failed, "
                     f"candidate layer skipped ({exc!r})", "DEBUG")
                continue
        return None

    def _recover_review_mode(self) -> None:
        """Review toggle: start a session (cache + persistent overlays).

        1. Load ALL events into RAM cache (background QThread).
        2. Create persistent overlay layers ONCE.
        3. Render visible features from cache.
        4. On zoom/pan: re-filter cache (~20-80ms, no SQL).
        """
        # recover_and_load() raised the run guard for this start; Review
        # owns it until _init_snapshot_direct or _review_done lowers it.
        # _review_stop_session may only release a guard Review holds: it
        # runs on every mode switch, a Rewind in progress included.
        self._review_holds_run_guard = True
        canvas = self.iface.mapCanvas()
        dest_crs = canvas.mapSettings().destinationCrs()
        src_crs = (dest_crs.authid() or dest_crs.toWkt()) if dest_crs.isValid() else ""

        checked_fps = self._get_checked_layer_fingerprints()
        if not checked_fps:
            self._review_done("--------", "no layer checked")
            return

        t_min = self.start_input.dateTime().toUTC().toString("yyyy-MM-ddTHH:mm:ss")
        t_max = self.end_input.dateTime().toUTC().toString("yyyy-MM-ddTHH:mm:ss")

        from .core.commit_guard import fid_is_stable  # noqa: PLC0415

        layer_infos = []
        root = QgsProject.instance().layerTreeRoot()
        for fingerprint in checked_fps:
            layer = self._resolve_layer_by_fingerprint(fingerprint)
            if layer is None:
                continue
            node = root.findLayer(layer.id())
            if node is not None and not node.itemVisibilityChecked():
                flog(
                    f"review: skip_invisible layer={layer.name()} fp={fingerprint[:8]}",
                    "DEBUG",
                )
                continue
            storage_crs = ((layer.crs().authid() or layer.crs().toWkt())
                           if layer.crs().isValid() else src_crs)
            # Asked here, on the GUI thread: the worker may not touch the
            # layer. On a Shapefile or a GeoJSON the fid is the record
            # position, renumbered by every deletion; the reconstruction
            # then never reads one fid key as one record across it.
            fid_stable = fid_is_stable(layer)
            if not fid_stable:
                flog(f"review: layer={layer.name()} fp={fingerprint[:8]} "
                     f"fid_stable=False (fid = record position)", "INFO")
            layer_infos.append({
                "fingerprint": fingerprint,
                "layer_name": layer.name(),
                "layer_id": layer.id(),
                "storage_crs": storage_crs,
                "fid_stable": fid_stable,
            })

        if not layer_infos:
            self._review_done("--------", "no valid layers")
            return

        tree_root = root
        tree_layers = tree_root.findLayers()
        tree_order = {n.layerId(): i for i, n in enumerate(tree_layers)}
        layer_infos.sort(key=lambda x: tree_order.get(x["layer_id"], 999999))

        self._review_layer_infos = layer_infos
        self._review_src_crs = src_crs
        self._review_t_min = t_min
        self._review_t_max = t_max

        import uuid as _uuid
        trace_id = _uuid.uuid4().hex[:8]
        flog(
            f"[{trace_id}] review: snapshot_mode_start "
            f"n_layers={len(layer_infos)}",
            "INFO",
        )
        self.update_phase(self.tr("Review Snapshot : initialisation..."))
        self._init_snapshot_direct(layer_infos, src_crs, trace_id)

    def _get_checked_layer_fingerprints(self) -> list:
        """Return fingerprints of all checked items in _version_layer_list."""
        fps = []
        for i in range(self._version_layer_list.count()):
            item = self._version_layer_list.item(i)
            if item.checkState() == QtCompat.CHECKED:
                fp = item.data(QtCompat.USER_ROLE)
                if fp:
                    fps.append(fp)
        return fps

    def _canvas_bbox_in_crs(self, canvas, src_crs: str, dst_crs: str):
        """Return canvas extent as (xmin, ymin, xmax, ymax) in dst_crs, or None."""
        extent_geom = QgsGeometry.fromRect(canvas.extent())
        geom = QgsGeometry(extent_geom)
        if dst_crs and dst_crs != src_crs:
            xform = QgsCoordinateTransform(
                QgsCoordinateReferenceSystem(src_crs),
                QgsCoordinateReferenceSystem(dst_crs),
                QgsProject.instance(),
            )
            try:
                geom.transform(xform)
            except Exception:  # noqa: BLE001
                return None
        bbox = geom.boundingBox()
        return (
            bbox.xMinimum(), bbox.yMinimum(),
            bbox.xMaximum(), bbox.yMaximum(),
        )

    def _on_review_toggle(self, checked: bool) -> None:
        """Handle Apple toggle ON/OFF for Review mode."""
        self._log_review_state(f"toggle_before_checked={checked}")
        flog(
            f"review: toggle_called checked={checked} "
            f"snap_mode={self._review_snap_mode} "
            f"wants_persist={self._review_wants_persist} "
            f"bar={'yes' if self._review_date_bar is not None else 'none'} "
            f"session={'yes' if self._review_snap_session is not None else 'none'}",
            "INFO",
        )
        if checked:
            flog("review: toggle ON", "INFO")
            self._review_wants_persist = True
            self._show_review_status_bar()
            if self._review_status_widget is not None:
                self._review_status_widget.activate()
                self._review_status_widget.set_phase("fetch")
            QTimer.singleShot(0, self.recover_and_load)
        else:
            flog("review: toggle OFF", "INFO")
            self._review_viz_combo.setVisible(False)
            self._review_stop_session()

    def _on_review_viz_changed(self, index: int) -> None:
        """Restart snapshot on viz combo change (only snapshot_at_t remains)."""
        self._stop_snapshot_mode()
        self._review_stop_session()
        if self._review_toggle.isChecked():
            self.recover_and_load()

    def _review_stop_from_statusbar(self) -> None:
        """Slot: user clicked OFF on the QGIS status bar widget."""
        self._log_review_state("stop_from_statusbar")
        import traceback as _tb
        _stack = "".join(_tb.format_stack(limit=8))
        flog(f"review: stop_from_statusbar STACK={_stack!r}", "WARNING")
        self._review_stop_session()
        self._review_toggle.blockSignals(True)
        self._review_toggle.setChecked(False)
        self._review_toggle.blockSignals(False)
        self._review_toggle.setEnabled(True)

    def _review_stop_session(self) -> None:
        """Stop Review session, remove overlays, reset state."""
        self._log_review_state("stop_session")
        flog(
            f"review: stop_session snap_mode={self._review_snap_mode} "
            f"wants_persist={self._review_wants_persist} "
            f"bar={'yes' if self._review_date_bar is not None else 'none'} "
            f"session={'yes' if self._review_snap_session is not None else 'none'}",
            "INFO",
        )
        self._stop_snapshot_mode()
        self._review_wants_persist = False
        self._review_disconnect_auto_refresh()
        # This method runs on EVERY switch to Rewind or Restore, whether
        # Review was on or not. It used to lower _is_recovering and
        # re-enable the controls unconditionally: switching modes in the
        # middle of a Rewind reopened the Rewind button and let a second
        # run write concurrently with the first. Review releases only the
        # guard it raised itself, and never under a restore.
        if getattr(self, "_review_holds_run_guard", False):
            self._review_holds_run_guard = False
            if (self._restore_runner is None
                    and not getattr(self, "_restore_in_progress", False)):
                self._is_recovering = False
                self.enable_controls(True)
        if not self._is_recovering:
            self.update_phase("")
        else:
            flog("review: stop_session left the run guard alone "
                 "(held by a running restore/rewind/search)", "INFO")
        if self._review_status_widget is not None:
            self._review_status_widget.deactivate()
        self._hide_review_status_bar()
        flog("review: session stopped", "INFO")

    def _log_review_state(self, caller: str) -> None:
        """Centralized snapshot of Review state for diagnostics."""
        flog(
            f"review_state caller={caller} "
            f"snap_mode={self._review_snap_mode} "
            f"wants_persist={self._review_wants_persist} "
            f"bar={'yes' if self._review_date_bar is not None else 'none'} "
            f"session={'yes' if self._review_snap_session is not None else 'none'} "
            f"status_widget={'yes' if self._review_status_widget is not None else 'none'} "
            f"active={self._review_active}",
            "DEBUG",
        )

    def _review_done(self, trace_id: str, reason: str) -> None:
        """Unlock UI and log after a Review early exit."""
        flog(f"[{trace_id}] review: {reason}", "WARNING")
        self._review_holds_run_guard = False
        self._is_recovering = False
        self.enable_controls(True)
        self._review_toggle.blockSignals(True)
        self._review_toggle.setChecked(False)
        self._review_toggle.blockSignals(False)
        self._review_toggle.setEnabled(True)
        if self._review_status_widget is not None:
            self._review_status_widget.deactivate()
        self._hide_review_status_bar()

    def _init_snapshot_direct(self, layer_infos: list, src_crs: str, trace_id: str) -> None:
        """Start snapshot mode: create overlays + date bar. Zero pre-load.

        Layer creation is async (one per event-loop tick) to keep the UI
        responsive during the transition from normal mode to Review mode.
        The first snapshot load is triggered only AFTER all overlays exist.
        """
        import datetime as _dt
        from .core.snapshot_overlay_session import SnapshotOverlaySession
        from .widgets.canvas_date_bar import CanvasDateBar

        flog(
            f"[{trace_id}] review: snapshot_init_direct n_layers={len(layer_infos)}",
            "INFO",
        )
        if self._review_snap_mode:
            # A second start over a live session used to orphan the first
            # one's overlays, date bar and workers.
            flog(f"[{trace_id}] review: snapshot_init over a live session, "
                 f"stopping it first", "WARNING")
            self._stop_snapshot_mode()

        snap = SnapshotOverlaySession()
        self._review_snap_session = snap
        self._review_snap_mode = True
        self._review_snap_error = ""
        self._review_floor_iso = None
        self._review_floor_notified = False
        self._review_connect_auto_refresh()

        today = _dt.date.today().isoformat()
        first_iso = ((_dt.date.today() - _dt.timedelta(days=5 * 365)).isoformat() + "T00:00:00")
        last_iso = today + "T23:59:59"

        canvas = self.iface.mapCanvas()
        flog(
            f"[{trace_id}] review: canvas_size={canvas.width()}x{canvas.height()}",
            "DEBUG",
        )
        bar = CanvasDateBar(canvas)
        bar.set_ceiling(self)
        bar.set_range(first_iso, last_iso)
        bar.date_changed.connect(self._on_snapshot_date_changed)
        bar.export_requested.connect(self._on_snapshot_export_requested)
        try:
            canvas.mapCanvasRefreshed.connect(bar.raise_safe)
            flog(f"[{trace_id}] review: bar_raise_safe_connected", "DEBUG")
        except Exception as _e:
            flog(f"[{trace_id}] review: bar_raise_connect_failed err={_e}", "WARNING")
        bar.show()
        self._review_date_bar = bar

        ext_debounce = QTimer(self)
        ext_debounce.setSingleShot(True)
        ext_debounce.setInterval(800)
        ext_debounce.timeout.connect(self._on_review_snap_bbox_fire)
        self._review_snap_ext_debounce = ext_debounce
        canvas.extentsChanged.connect(self._on_review_snap_extent_changed)
        flog(
            f"[{trace_id}] review: snap_extent_debounce_wired interval_ms=800",
            "DEBUG",
        )

        self._review_holds_run_guard = False
        self._is_recovering = False
        self.enable_controls(True)
        self._review_toggle.setEnabled(True)
        self.update_phase(self.tr("Review Snapshot : création des couches…"))
        flog(
            f"[{trace_id}] review: snapshot_bar_shown "
            f"bar_visible={bar.isVisible()} "
            f"bar_geom={bar.x()},{bar.y()} {bar.width()}x{bar.height()}",
            "INFO",
        )

        def _on_layers_ready() -> None:
            if not self._review_snap_mode or self._review_snap_session is not snap:
                return
            self.update_phase("")
            flog(
                f"[{trace_id}] review: snapshot_ready n_layers={snap.n_layers}",
                "INFO",
            )
            from .widgets.snapshot_rebuild_worker import query_snapshot_floor
            # The oldest date Review can answer: a purge horizon, not the
            # first captured event (before it, every feature is in the state
            # its first event records as old).
            self._review_floor_iso = query_snapshot_floor(self._journal, layer_infos)
            QTimer.singleShot(100, lambda: self._deferred_set_bar_range(layer_infos))
            self._on_snapshot_date_changed(bar.current_date_iso())

        snap.start_async_create(layer_infos, src_crs, trace_id, _on_layers_ready)

    def _deferred_set_bar_range(self, layer_infos: list) -> None:
        """Run date-range SQL off the critical path - updates bar after first paint."""
        from .widgets.snapshot_rebuild_worker import query_snapshot_date_range
        bar = self._review_date_bar
        if bar is None or not self._review_snap_mode:
            return
        first_iso, last_iso = query_snapshot_date_range(self._journal, layer_infos)
        bar.set_range(first_iso, last_iso)
        self._review_global_t0_iso = first_iso
        self._review_global_t1_iso = last_iso
        flog(
            f"review: bar_range_updated first={first_iso} last={last_iso} "
            f"global_t0={first_iso} global_t1={last_iso}",
            "DEBUG",
        )
        # Now that the full temporal range is known, refresh markers for the
        # current viewport (CHANGE C).
        if self._review_snap_mode:
            self._refresh_extent_markers(
                self._compute_bbox_per_layer(),
                uuid.uuid4().hex[:8],
            )

    def _stop_snapshot_mode(self) -> None:
        """Cleanup worker + date bar + snapshot overlay. No-op if not active."""
        flog(
            f"review: stop_snapshot_mode_called snap_mode={self._review_snap_mode} "
            f"bar={'yes' if self._review_date_bar is not None else 'none'} "
            f"session={'yes' if self._review_snap_session is not None else 'none'}",
            "INFO",
        )
        if not self._review_snap_mode:
            # Workers cancelled by an earlier stop may still be unwinding;
            # this is the last call before the dialog can be deleted.
            self._review_join_workers()
            flog("review: stop_snapshot_mode NOOP snap_mode=False", "INFO")
            return
        self._review_snap_mode = False
        try:
            self.iface.mapCanvas().extentsChanged.disconnect(
                self._on_review_snap_extent_changed
            )
        except (TypeError, RuntimeError):
            pass
        if self._review_snap_ext_debounce is not None:
            self._review_snap_ext_debounce.stop()
            self._review_snap_ext_debounce.deleteLater()
            self._review_snap_ext_debounce = None
        self._cancel_snap_worker(disconnect_signals=True)
        self._review_snap_expected_trace = ""
        flog("review: snap_worker cancelled", "DEBUG")
        self._cancel_marker_worker()
        self._review_marker_bbox_signature = ""
        flog("review: marker_worker cancelled", "DEBUG")
        self._review_join_workers()
        if self._review_date_bar is not None:
            try:
                self.iface.mapCanvas().mapCanvasRefreshed.disconnect(
                    self._review_date_bar.raise_safe
                )
            except Exception as exc:  # noqa: BLE001
                # Benign and expected when the connection was never made.
                flog(f"review: date_bar raise_safe disconnect skipped "
                     f"({exc!r})", "DEBUG")
            self._review_date_bar.cleanup()
            self._review_date_bar = None
            flog("review: date_bar cleaned up", "INFO")
            try:
                self.iface.mapCanvas().update()
            except Exception as exc:  # noqa: BLE001
                flog(f"review: canvas update after date_bar removal failed, "
                     f"the bar may stay painted ({exc!r})", "DEBUG")
        else:
            flog("review: date_bar was already None", "WARNING")
        if self._review_snap_session is not None:
            self._review_snap_session.stop()
            self._review_snap_session = None
            flog("review: snap_session stopped", "INFO")
        else:
            flog("review: snap_session was already None", "WARNING")
        self._review_snap_cache = {}
        self._review_snap_raw_result = None
        self._review_snap_raw_iso = ""
        self._review_snap_error = ""
        flog("review: snapshot_mode_stopped", "INFO")

    def _review_join_workers(self, timeout_ms: int = 3000) -> None:
        """Cancel and join every Review thread, active and parked.

        The dialog is deleted on unload right after cleanup_resources().
        Nothing used to wait for the Review workers: a rebuild still
        streaming rows, or a marker scan still closing its connection, died
        with its owner (qFatal, QGIS gone with the user's unsaved work).
        cancel() interrupts the SQLite statement in flight, so the join is
        normally immediate; a thread that outlives the bound stays retained
        by snapshot_rebuild_worker until it finishes, never destroyed.
        """
        from .widgets.snapshot_rebuild_worker import join_review_thread
        workers = []
        for attr in ("_review_snap_worker", "_review_marker_worker"):
            worker = getattr(self, attr, None)
            if worker is not None:
                workers.append(worker)
        workers.extend(self._review_snap_zombies)
        workers.extend(self._review_marker_zombies)
        n_left = 0
        for worker in workers:
            if not join_review_thread(worker, timeout_ms,
                                      getattr(worker, "trace_id", "?")):
                n_left += 1
        self._review_snap_zombies = []
        self._review_marker_zombies = []
        if workers:
            flog(f"review: workers_joined n={len(workers)} still_running={n_left}",
                 "INFO" if not n_left else "WARNING")

    def _clamp_cutoff_to_baseline(self, iso: str) -> str:
        """Never request a cutoff Review cannot answer, and say so.

        The floor (snapshot_rebuild_worker.query_snapshot_floor) is the date
        from which the journal answers for every entity: its purge horizon
        when it records one, else the last second BEFORE its oldest
        surviving row (a journal purged by an older version records no
        horizon). Below the floor the events were deleted: the before-image
        of the first surviving event is the state at the purge line, which
        was shown as the state of the date asked. The date is brought to
        the floor, the bar shows it and the user is told (once per
        session). The floor is never the first event itself: that inclusive
        clamp showed the first edit as already done (RL-E1-03). When the
        floor cannot be read, the second before the first event is used.
        """
        from .widgets.snapshot_rebuild_worker import second_before
        floor = getattr(self, "_review_floor_iso", None)
        if floor is None:
            t0_iso = self._review_global_t0_iso
            floor = (second_before(t0_iso) or "") if t0_iso else ""
        if not floor:
            return iso
        req_dt = self._parse_iso_datetime(iso)
        floor_dt = self._parse_iso_datetime(floor)
        if req_dt.isValid() and floor_dt.isValid() and req_dt < floor_dt:
            flog(
                f"review: cutoff_clamped_to_floor requested={iso} floor={floor}",
                "INFO",
            )
            if self._review_date_bar is not None:
                self._review_date_bar.set_value_iso(floor)
            if not getattr(self, "_review_floor_notified", False):
                self._review_floor_notified = True
                self.iface.messageBar().pushMessage(
                    self.tr("Review : date ramenée"),
                    self.tr(
                        "Le journal ne permet pas de reconstituer l'état avant "
                        "le {floor} (UTC) : l'historique antérieur a été purgé "
                        "ou n'a pas été journalisé. La date demandée ({asked}) "
                        "a été ramenée au {floor}."
                    ).format(floor=floor, asked=iso),
                    QgisCompat.MSG_WARNING, 0,
                )
            return floor
        return iso

    def _compute_bbox_per_layer(self) -> dict:
        """Map each layer fingerprint -> current canvas extent in its storage CRS."""
        from qgis.core import (  # noqa: PLC0415
            QgsCoordinateTransform,
            QgsProject,
        )
        from .core.snapshot_overlay_session import _crs_from_spec  # noqa: PLC0415
        canvas = self.iface.mapCanvas()
        extent = canvas.extent()
        project = QgsProject.instance()
        project_crs = project.crs()
        bbox_per_layer: dict = {}
        for _info in self._review_layer_infos:
            _fp = _info["fingerprint"]
            _lyr = project.mapLayer(_info.get("layer_id", ""))
            if _lyr is not None and _lyr.crs().isValid():
                _lcrs = _lyr.crs()
                _info["storage_crs"] = _lcrs.authid() or _lcrs.toWkt()
            else:
                _lcrs = _crs_from_spec(_info.get("storage_crs", ""))
            if not _lcrs.isValid() or not project_crs.isValid() or _lcrs == project_crs:
                bbox_per_layer[_fp] = extent
            else:
                _tr = QgsCoordinateTransform(project_crs, _lcrs, project)
                try:
                    bbox_per_layer[_fp] = _tr.transformBoundingBox(extent)
                except Exception:  # noqa: BLE001
                    bbox_per_layer[_fp] = extent
        flog(
            f"review: snap_bbox_computed n_layers={len(bbox_per_layer)} "
            f"project_crs={project_crs.authid()}",
            "DEBUG",
        )
        return bbox_per_layer

    def _on_snapshot_date_changed(self, iso: str) -> None:
        """Slot: CanvasDateBar emitted a NEW date → reconstruct via worker.

        Reconstruction reads the journal, so a date change always needs the
        background worker. Pan/zoom keeps the same date and is handled by
        _refresh_snapshot_for_extent WITHOUT a worker (see
        _on_review_snap_bbox_fire) - that is the core fix for the review-mode
        slowness: panning no longer re-runs SQL + event replay.
        """
        from .widgets.snapshot_rebuild_worker import (
            SnapshotRebuildWorker,
            retain_until_finished,
        )

        iso = self._clamp_cutoff_to_baseline(iso)
        self._review_snap_pending_iso = iso
        flog(f"review: snapshot_date_changed iso={iso}", "DEBUG")

        # This very date is being completed (a pan or the extent debounce
        # asks for it again): it goes on, and is applied to the extent of
        # the moment it ends. Restarted at every pan, the completion of a
        # mass-edited date never ended. A commit cancels it first
        # (_review_on_commit_changed), so its rebuild does start again.
        job = getattr(self, "_review_snap_completion", None)
        if (job is not None and job.active and self._review_snap_mode
                and getattr(self, "_review_snap_completion_iso", "") == iso):
            flog(f"[{job.trace_id}] review: snapshot_date_kept iso={iso} "
                 f"(its completion is running)", "DEBUG")
            return

        self._cancel_snap_worker(disconnect_signals=True)
        self._review_snap_expected_trace = ""

        if not self._review_snap_mode or self._review_snap_session is None:
            flog("review: snapshot_date_changed skipped mode_inactive", "DEBUG")
            return

        # Same cutoff already reconstructed → no journal re-read, just re-apply
        # the cached result to the current viewport.
        if (self._review_snap_raw_result is not None and self._review_snap_raw_iso == iso):
            flog(f"review: snapshot_reuse_cached iso={iso}", "DEBUG")
            self._refresh_snapshot_for_extent()
            return

        self._review_snap_error = ""
        if self._review_date_bar is not None:
            self._review_date_bar.set_loading()

        # No parent: a child of the dialog was destroyed WITH it on unload
        # while still running (qFatal). The worker is retained until Qt
        # reports it finished, and joined by _review_join_workers.
        worker = SnapshotRebuildWorker(
            self._journal,
            self._review_layer_infos,
            iso,
            bbox_per_layer=None,
        )
        worker.result_ready.connect(self._on_snapshot_result)
        worker.error.connect(self._on_snapshot_error)
        # A date inside a purged interval (the worker names it). getattr: a
        # stand-in worker without the signal stays usable.
        refused = getattr(worker, "refused", None)
        if refused is not None:
            refused.connect(self._on_snapshot_refused)
        retain_until_finished(worker)
        self._review_snap_worker = worker
        # Only the answer of THIS worker is accepted. A superseded one may
        # already have posted its result before being disconnected: cached
        # under the new date (or after a commit), it was reused as if fresh.
        self._review_snap_expected_trace = worker.trace_id
        self._review_snap_expected_iso = iso
        worker.start()
        flog(
            f"[{worker.trace_id}] review: snap_worker_started iso={iso}",
            "INFO",
        )

    def _on_snapshot_result(self, trace_id: str, result) -> None:
        """Slot: SnapshotRebuildWorker delivered a reconstruction.

        Starts its live completion (`LiveCompletion`, across event-loop
        turns); `_on_snapshot_completed` then caches the result (pre-bbox)
        keyed by date so pan/zoom can re-apply it cheaply, and applies it to
        the current extent.
        """
        expected = getattr(self, "_review_snap_expected_trace", "")
        if trace_id != expected:
            flog(f"[{trace_id}] review: snap_result_discarded stale "
                 f"expected={expected or 'none'}", "INFO")
            return
        self._review_snap_expected_trace = ""
        self._review_snap_worker = None
        snap = self._review_snap_session
        if snap is None or not self._review_snap_mode:
            flog(f"[{trace_id}] review: snap_result_discarded mode_inactive", "DEBUG")
            return

        from .core.snapshot_overlay_session import LiveCompletion
        iso = getattr(self, "_review_snap_expected_iso",
                      self._review_snap_pending_iso)
        # What no event gives (a geometry or a field untouched since T) is
        # the live value, and every journal geometry is placed by the CRS
        # verdict: both on the GUI thread, once per date. In one call that
        # held QGIS for seconds on a mass-edited layer (minutes on a remote
        # provider), at every date: it now runs across event-loop turns,
        # the bar keeping its loading state, and a new date, a commit or
        # Review stopped cancels it (_cancel_snap_worker).
        job = LiveCompletion(
            result, self._review_layer_infos, trace_id,
            on_done=lambda completed: self._on_snapshot_completed(
                job, trace_id, iso, completed),
            on_failed=lambda message: self._on_snapshot_completion_failed(
                job, trace_id, iso, message),
            on_progress=lambda done, total: self._on_snapshot_completion_progress(
                job, done, total))
        self._review_snap_completion = job
        self._review_snap_completion_iso = iso
        job.start()

    def _on_snapshot_completion_progress(self, job, done: int, total: int) -> None:
        """The bar says how far the completion of the date is (loading state)."""
        if getattr(self, "_review_snap_completion", None) is not job:
            return
        bar = self._review_date_bar
        if bar is not None and bar.status_state() == "loading":
            bar.set_loading(done, total)

    def _on_snapshot_completed(self, job, trace_id: str, iso: str, result) -> None:
        """The live completion of *iso* ended: cache it and draw it."""
        if getattr(self, "_review_snap_completion", None) is not job:
            flog(f"[{trace_id}] review: snap_completion_discarded stale "
                 f"iso={iso}", "INFO")
            return
        self._review_snap_completion = None
        if self._review_snap_session is None or not self._review_snap_mode:
            flog(f"[{trace_id}] review: snap_completion_discarded "
                 f"mode_inactive", "DEBUG")
            return
        self._review_snap_raw_result = result
        self._review_snap_raw_iso = iso
        self._review_surface_snapshot_gaps(trace_id, result)
        self._apply_snapshot_to_extent(result, trace_id)

    def _on_snapshot_completion_failed(self, job, trace_id: str, iso: str,
                                       message: str) -> None:
        """The live completion of *iso* could not end: say why, show nothing.

        A layer removed from the project, or given another CRS, while the
        date was being completed (or an error): half of the state would be
        placed one way and half another. The overlays are emptied as for a
        failed rebuild, and the message says the date can be asked again.
        """
        if getattr(self, "_review_snap_completion", None) is not job:
            return
        self._review_snap_completion = None
        self._review_snap_error = message or "?"
        self._review_snap_raw_result = None
        self._review_snap_raw_iso = ""
        flog(f"[{trace_id}] review: snap_completion_failed iso={iso} "
             f"reason={message}", "WARNING")
        if self._review_snap_session is not None:
            self._review_snap_session.clear_all()
        if self._review_date_bar is not None:
            self._review_date_bar.set_error(message)
        self.iface.messageBar().pushMessage(
            self.tr("Review : reconstruction interrompue"),
            self.tr("L'état à la date {date} n'a pas pu être établi ({msg}). "
                    "Les calques Review ont été vidés : la carte ne montre pas "
                    "l'état à cette date. Choisissez une date pour relancer "
                    "la reconstruction.").format(date=iso or "?", msg=message),
            QgisCompat.MSG_WARNING, 0,
        )

    def _review_surface_snapshot_gaps(self, trace_id: str, result) -> None:
        """Say, once per reconstruction, what the overlays cannot show."""
        n_unresolved = getattr(result, "n_unresolved", 0)
        if n_unresolved:
            flog(f"[{trace_id}] review: warn_unresolved n={n_unresolved}",
                 "WARNING")
            self.iface.messageBar().pushMessage(
                self.tr("Review : état incomplet"),
                self.tr(
                    "{n} entité(s) existaient à cette date mais leur état "
                    "n'est pas entièrement connu (journal ou couche actuelle "
                    "insuffisants) : elles ne sont pas dessinées ou le sont "
                    "partiellement."
                ).format(n=n_unresolved),
                QgisCompat.MSG_WARNING, 0,
            )
        # Among them, a geometry the Rewind's CRS verdict refuses to place
        # (snapshot_overlay_session._align_to_layer_crs): named apart, it is
        # the one the user can act on (the CRS the layer declares).
        from .core.temporal_snapshot_engine import count_reason
        features = getattr(result, "features", None) or {}
        n_crs = (count_reason(features, "crs_ambiguous")
                 + count_reason(features, "crs_mismatch"))
        if n_crs:
            flog(f"[{trace_id}] review: warn_crs_refused n={n_crs}", "WARNING")
            self.iface.messageBar().pushMessage(
                self.tr("Review : SCR non départagé"),
                self.tr(
                    "{n} entité(s) ont à cette date une géométrie capturée "
                    "dans un autre SCR que celui de la couche, et rien ne dit "
                    "si ce SCR a été corrigé (mêmes coordonnées) ou si le "
                    "fichier a été reprojeté (coordonnées converties) : elles "
                    "ne sont pas dessinées, comme le Rewind refuse de les "
                    "écrire."
                ).format(n=n_crs),
                QgisCompat.MSG_WARNING, 0,
            )
        n_unproven = getattr(result, "n_identity_unproven", 0)
        if n_unproven:
            flog(f"[{trace_id}] review: warn_identity_unproven n={n_unproven}",
                 "WARNING")
            self.iface.messageBar().pushMessage(
                self.tr("Review : identité non prouvée"),
                self.tr(
                    "{n} entité(s) modifiée(s) après cette date ne peuvent "
                    "pas être rattachées à leur état à cette date : sur un "
                    "Shapefile ou un GeoJSON, chaque suppression renumérote "
                    "les fid, et le journal ne prouve plus quelle entité "
                    "elles étaient. Leur état à cette date n'est pas "
                    "montré ; ce que montrent les couches actuelles est leur "
                    "état après ces modifications."
                ).format(n=n_unproven),
                QgisCompat.MSG_WARNING, 0,
            )
        unsaved_names, n_unsaved = RecoverDialog._review_unsaved_edits(self)
        if n_unsaved:
            flog(f"[{trace_id}] review: warn_unsaved_edit_buffer "
                 f"n={n_unsaved} layers={unsaved_names}", "WARNING")
            self.iface.messageBar().pushMessage(
                self.tr("Review : modifications non enregistrées"),
                self.tr(
                    "La couche {layers} contient {n} modification(s) non "
                    "enregistrée(s) dans son tampon d'édition : elles ne sont "
                    "pas dans le journal et restent affichées par la couche "
                    "actuelle sur la carte."
                ).format(layers=", ".join(unsaved_names), n=n_unsaved),
                QgisCompat.MSG_WARNING, 0,
            )

    def _review_unsaved_edits(self) -> tuple:
        """([layer names], total modified/added/deleted features in edit buffers)."""
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        names = []
        total = 0
        for info in getattr(self, "_review_layer_infos", None) or ():
            lyr = project.mapLayer(info.get("layer_id", ""))
            if lyr is None:
                continue
            try:
                if not (lyr.isEditable() and lyr.isModified()):
                    continue
                eb = lyr.editBuffer()
                n = 0
                if eb is not None:
                    ids_of = getattr(eb, "allAddedOrEditedFeatures", None)
                    edited = set(ids_of()) if callable(ids_of) else (
                        set(eb.changedAttributeValues().keys())
                        | set(eb.changedGeometries().keys())
                        | set(eb.addedFeatures().keys())
                    )
                    n = len(edited | set(eb.deletedFeatureIds()))
                total += max(1, n)
                names.append(lyr.name())
            except RuntimeError:
                continue
        return names, total

    def _refresh_snapshot_for_extent(self) -> None:
        """Re-apply the cached reconstruction to the current viewport (pan/zoom).

        Runs on the UI thread but never touches the journal - only the bbox
        filter and the overlay repaint. Safe to call on every settled pan/zoom.
        """
        if self._review_snap_raw_result is None or not self._review_snap_mode:
            return
        trace_id = uuid.uuid4().hex[:8]
        self._apply_snapshot_to_extent(self._review_snap_raw_result, trace_id)

    def _apply_snapshot_to_extent(self, result, trace_id: str) -> None:
        """Filter a reconstruction by the current extent and repaint overlays.

        Review shows only entities whose state at T differs from the live data
        (changed-after-T, filtered in the worker); the unmodified source layers
        are never duplicated. No journal access here (UI-thread safe). Extent
        markers are refreshed separately (extent-scoped, background)."""
        from .widgets.snapshot_rebuild_worker import filter_snapshot_by_bbox
        snap = self._review_snap_session
        bar = self._review_date_bar
        if snap is None or not self._review_snap_mode:
            return

        bbox_per_layer = self._compute_bbox_per_layer()
        self._review_snap_bbox_per_layer = bbox_per_layer

        n_before = result.n_entities
        if bbox_per_layer:
            result = filter_snapshot_by_bbox(result, bbox_per_layer)
            flog(
                f"[{trace_id}] review: snap_bbox_post_filter "
                f"before={n_before} after={result.n_entities} "
                f"dropped={n_before - result.n_entities}",
                "INFO",
            )

        self._surface_snapshot_warnings(trace_id, result)

        n_total_before_bbox = n_before if bbox_per_layer else result.n_entities
        n_born_after = getattr(result, "n_born_after", 0)
        n_unresolved = getattr(self._review_snap_raw_result, "n_unresolved", 0)
        n_trace_ref_missing = getattr(self._review_snap_raw_result,
                                      "n_trace_ref_missing", 0)
        n_identity_unproven = getattr(self._review_snap_raw_result,
                                      "n_identity_unproven", 0)
        # Listed without shape in the overlays; the live layers may still
        # draw a geometry they received after T, so the bar says it.
        from .core.temporal_snapshot_engine import count_null_geometry
        n_null_geometry = count_null_geometry(result.features)

        def _on_update_done(stats: dict) -> None:
            if bar is not None and self._review_snap_mode:
                _names, n_unsaved = self._review_unsaved_edits()
                bar.set_stats(stats["n_entities"], n_total=n_total_before_bbox,
                              n_born_after=n_born_after,
                              n_unresolved=n_unresolved,
                              n_null_geometry=n_null_geometry,
                              n_trace_ref_missing=n_trace_ref_missing,
                              n_identity_unproven=n_identity_unproven,
                              n_unsaved_edits=n_unsaved)
            flog(
                f"[{trace_id}] review: snap_result_applied "
                f"n_entities={stats['n_entities']} "
                f"n_total={n_total_before_bbox} elapsed_ms={stats['elapsed_ms']}",
                "INFO",
            )

        snap.update_async(result, _on_update_done)
        self._refresh_extent_markers(bbox_per_layer, trace_id)

    def _surface_snapshot_warnings(self, trace_id: str, result) -> None:
        """Surface degraded/partial snapshot conditions to the user (RL-E1-02).

        Pushed as a persistent (duration=0) message-bar warning so the user is
        never left with a silently-partial as-of-T view.
        """
        if getattr(result, "partial", False):
            flog(
                f"[{trace_id}] review: warn_partial reason={result.partial_reason}",
                "WARNING",
            )
            self.iface.messageBar().pushMessage(
                self.tr("Instantane partiel"),
                self.tr(
                    "Le journal depasse le budget de lecture : l'instantane affiche "
                    "est INCOMPLET ({reason}). Reduisez la fenetre temporelle ou la "
                    "zone d'affichage pour un resultat complet."
                ).format(reason=result.partial_reason),
                QgisCompat.MSG_WARNING, 0,
            )

    def _on_snapshot_export_requested(self) -> None:
        """Slot: user clicked Export: write the state at T to a GeoPackage.

        The export used to serialise the overlays (changed entities in the
        CURRENT VIEW only, attributes possibly incomplete) and announce
        "Snapshot exporte", a file then kept as "the data at T". It now
        writes the full reconstruction of the date (every extent), says
        what it holds in the message AND in the file, and refuses while the
        state of the date is not established (reconstruction running,
        overlays being repainted, failed or partial reconstruction).

        The refusal is evaluated again once "Save as" is closed: that dialog
        runs the event loop, and a commit notification or a worker error
        arriving meanwhile emptied the reconstruction (the export then died
        on an AttributeError) or replaced it. The result exported is the
        one checked at that point, with its date.

        An export that holds entities whose state at T is only partly known,
        or that could not write some of them, is announced "Export partiel"
        with each population named: the written ones carry
        ``_rl_etat_incomplet`` (an empty field there is an unknown value),
        the omitted ones are counted by cause. It used to say "Export
        termine" and count them among the entities written in their state
        at T.
        """
        snap = self._review_snap_session
        if snap is None or not snap.is_active:
            flog("review: export_requested but no active snapshot session", "WARNING")
            return

        from qgis.PyQt.QtWidgets import QFileDialog, QMessageBox  # noqa: PLC0415
        iso = self._review_snap_pending_iso or "?"
        if self._review_export_refused(iso, self._review_export_refusal()):
            return

        stamp = "".join(ch for ch in iso if ch.isdigit())[:14] or "date"
        default_name = f"recoverland_etat_{stamp}_modifiees.gpkg"
        path, _ = QFileDialog.getSaveFileName(
            self,
            self.tr("Exporter l'état à cette date des entités modifiées depuis"),
            default_name,
            "GeoPackage (*.gpkg)",
        )
        if not path:
            return

        # Checked again: the dialog above ran the event loop.
        refusal = self._review_export_refusal()
        if not refusal and (self._review_snap_pending_iso or "?") != iso:
            refusal = self.tr("la date a changé pendant le choix du fichier")
        if self._review_export_refused(iso, refusal):
            return
        snap = self._review_snap_session
        raw = self._review_snap_raw_result
        flog(
            f"review: snapshot_export_start path={path} iso={iso} "
            f"n_entities={raw.n_entities}",
            "INFO",
        )
        result = snap.export_to_geopackage(path, snapshot_result=raw, cutoff_iso=iso)
        notes = self._review_export_notes(result)
        if result["n_born_after"]:
            notes.append(self.tr(
                "{n} entité(s) créée(s) après cette date ne sont pas "
                "incluses (elles n'existaient pas).").format(n=result["n_born_after"]))
        if result.get("n_null_geometry"):
            notes.append(self.tr(
                "{n} entité(s) n'avaient pas de géométrie à cette date : "
                "elles sont écrites sans géométrie.").format(
                    n=result["n_null_geometry"]))
        summary = self.tr(
            "État au {date} des {n_expected} entité(s) modifiée(s) depuis "
            "cette date, sur toute l'emprise : {n_features} entité(s) "
            "écrite(s) dans {n_layers} couche(s).\n"
            "Les entités non modifiées depuis cette date ne sont pas "
            "incluses : elles sont identiques aux couches actuelles.\n"
            "Le détail est dans la table recoverland_etat_info du fichier."
        ).format(date=iso, n_expected=result["n_expected"],
                 n_features=result["n_features"], n_layers=result["n_layers"])
        body = "\n".join([summary] + notes + ["", path])
        partial = bool(result["errors"] or result["n_not_written"]
                       or result.get("n_unresolved")
                       or result.get("n_identity_unproven"))
        if result["errors"]:
            QMessageBox.warning(
                self,
                self.tr("Export partiel"),
                self.tr("{n} erreur(s) : {errors}").format(
                    n=len(result["errors"]),
                    errors="; ".join(result["errors"]),
                ) + "\n\n" + body,
            )
        elif partial:
            QMessageBox.warning(self, self.tr("Export partiel"), body)
        else:
            QMessageBox.information(self, self.tr("Export terminé"), body)
        flog(
            f"review: snapshot_export_done n_layers={result['n_layers']} "
            f"n_features={result['n_features']} n_expected={result['n_expected']} "
            f"n_not_written={result['n_not_written']} "
            f"n_unresolved={result.get('n_unresolved', 0)} "
            f"n_unresolved_written={result.get('n_unresolved_written', 0)} "
            f"n_identity_unproven={result.get('n_identity_unproven', 0)} "
            f"scope={result['scope']} partial={partial} "
            f"errors={len(result['errors'])} elapsed_ms={result['elapsed_ms']}",
            "WARNING" if partial else "INFO",
        )

    def _review_export_refused(self, iso: str, refusal: str) -> bool:
        """Show *refusal* (nothing is written) and return True, or False."""
        if not refusal:
            return False
        from qgis.PyQt.QtWidgets import QMessageBox  # noqa: PLC0415
        flog(f"review: snapshot_export_refused iso={iso} reason={refusal!r}",
             "WARNING")
        QMessageBox.warning(
            self, self.tr("Export impossible"),
            self.tr("L'état à cette date n'est pas établi : {reason}\n"
                    "Rien n'a été écrit.").format(reason=refusal),
        )
        return True

    def _review_export_notes(self, result: dict) -> list:
        """What the file does not hold as known, each population by its cause."""
        from .core.snapshot_overlay_session import INCOMPLETE_FIELD
        notes = []
        if result.get("n_unresolved"):
            notes.append(self.tr(
                "{n} entité(s) dont l'état à cette date n'est pas entièrement "
                "connu (journal ou couche actuelle insuffisants)."
            ).format(n=result["n_unresolved"]))
        if result.get("n_unresolved_written"):
            notes.append(self.tr(
                "{n} d'entre elles sont écrites avec la colonne {field}, qui en "
                "donne la raison : un champ vide y est une valeur inconnue, pas "
                "NULL à cette date."
            ).format(n=result["n_unresolved_written"], field=INCOMPLETE_FIELD))
        if result.get("n_trace_ref_missing"):
            notes.append(self.tr(
                "{n} entité(s) dépendent d'un retour arrière dont l'événement "
                "source a été purgé du journal."
            ).format(n=result["n_trace_ref_missing"]))
        if result.get("n_identity_unproven"):
            # Not among the entities counted above: their state at this
            # date is unknown, their existence at it included.
            notes.append(self.tr(
                "{n} entité(s) modifiée(s) après cette date sans identité "
                "prouvée à cette date (fid renumérotés par une suppression : "
                "Shapefile, GeoJSON) n'ont pas été écrites : leur état à "
                "cette date est inconnu, et elles ne sont pas identiques "
                "aux couches actuelles."
            ).format(n=result["n_identity_unproven"]))
        causes = (
            ("n_geometry_unknown", self.tr(
                "{n} entité(s) dont la géométrie à cette date est inconnue "
                "n'ont pas été écrites.")),
            ("n_geometry_undecodable", self.tr(
                "{n} entité(s) dont la géométrie journalisée est illisible "
                "n'ont pas été écrites.")),
            ("n_type_skipped", self.tr(
                "{n} entité(s) dont la géométrie n'est pas du type de la "
                "couche n'ont pas été écrites.")),
            ("n_layer_missing", self.tr(
                "{n} entité(s) d'une couche source absente du projet n'ont "
                "pas été écrites.")),
            ("n_write_failed", self.tr(
                "{n} entité(s) n'ont pas pu être écrites (écriture refusée ou "
                "non relue).")),
        )
        for key, text in causes:
            if result.get(key):
                notes.append(text.format(n=result[key]))
        return notes

    def _review_export_refusal(self) -> str:
        """Why the state at the current date cannot be exported, or ""."""
        snap = self._review_snap_session
        job = getattr(self, "_review_snap_completion", None)
        if self._review_snap_worker is not None or (job is not None and job.active):
            return self.tr("la reconstruction de cette date est en cours")
        if getattr(self, "_review_snap_error", ""):
            return self.tr("la reconstruction a échoué ({msg})").format(
                msg=self._review_snap_error)
        raw = self._review_snap_raw_result
        if raw is None or self._review_snap_raw_iso != self._review_snap_pending_iso:
            return self.tr("aucune reconstruction terminée pour cette date")
        if getattr(raw, "partial", False):
            return self.tr("la reconstruction est partielle ({reason})").format(
                reason=getattr(raw, "partial_reason", "?"))
        if snap is not None and getattr(snap, "is_updating", False):
            return self.tr("la carte est encore en cours de mise à jour")
        return ""

    def _on_snapshot_error(self, trace_id: str, message: str) -> None:
        """Slot: SnapshotRebuildWorker encountered a fatal error.

        Fail loud. It used to set the bar to the empty-set symbol ("no
        entity at this date") and leave the previous date's overlays
        painted: a failed read was shown as a valid, empty answer.
        """
        expected = getattr(self, "_review_snap_expected_trace", "")
        if trace_id != expected:
            flog(f"[{trace_id}] review: snap_worker_error from a superseded "
                 f"worker ignored msg={message}", "WARNING")
            return
        self._review_snap_expected_trace = ""
        self._review_snap_worker = None
        self._review_snap_error = message or "?"
        self._review_snap_raw_result = None
        self._review_snap_raw_iso = ""
        flog(
            f"[{trace_id}] review: snap_worker_error msg={message}",
            "ERROR",
        )
        if self._review_snap_session is not None:
            self._review_snap_session.clear_all()
        if self._review_date_bar is not None:
            self._review_date_bar.set_error(message)
        self.iface.messageBar().pushMessage(
            self.tr("Review : reconstruction impossible"),
            self.tr("L'état à la date {date} n'a pas pu être lu dans le "
                    "journal ({msg}). Les calques Review ont été vidés : la "
                    "carte ne montre pas l'état à cette date.").format(
                date=self._review_snap_pending_iso or "?", msg=message),
            QgisCompat.MSG_CRITICAL, 0,
        )

    def _on_snapshot_refused(self, trace_id: str, reason: str) -> None:
        """Slot: the journal cannot rebuild this date, the worker says why.

        A date inside an interval a logical purge removed (the Rewind
        refuses the same date, retention.coverage_refusal). It is not moved
        to another date, which would draw another state under the date
        asked: the overlays are emptied, the bar shows the refusal and the
        message names the interval, and that a date on either side of it
        can be shown.
        """
        expected = getattr(self, "_review_snap_expected_trace", "")
        if trace_id != expected:
            flog(f"[{trace_id}] review: snap_refusal from a superseded "
                 f"worker ignored reason={reason}", "INFO")
            return
        self._review_snap_expected_trace = ""
        self._review_snap_worker = None
        self._review_snap_error = reason or "?"
        self._review_snap_raw_result = None
        self._review_snap_raw_iso = ""
        flog(f"[{trace_id}] review: snap_refused "
             f"iso={self._review_snap_pending_iso} reason={reason}", "WARNING")
        if self._review_snap_session is not None:
            self._review_snap_session.clear_all()
        if self._review_date_bar is not None:
            self._review_date_bar.set_error(reason)
        # The wording of _on_snapshot_error, the reason names the interval.
        self.iface.messageBar().pushMessage(
            self.tr("Review : reconstruction impossible"),
            self.tr("L'état à la date {date} n'a pas pu être lu dans le "
                    "journal ({msg}). Les calques Review ont été vidés : la "
                    "carte ne montre pas l'état à cette date.").format(
                date=self._review_snap_pending_iso or "?", msg=reason),
            QgisCompat.MSG_WARNING, 0,
        )

    def _reap_snap_zombies(self) -> None:
        """Remove finished workers from the zombie list (prevent GC of running QThread)."""
        alive = [w for w in self._review_snap_zombies if w.isRunning()]
        reaped = len(self._review_snap_zombies) - len(alive)
        self._review_snap_zombies = alive
        if reaped:
            flog(f"review: snap_zombies reaped={reaped} remaining={len(alive)}", "DEBUG")

    def _cancel_marker_worker(self) -> None:
        """Cancel current marker worker and park it for reaping."""
        worker = self._review_marker_worker
        if worker is None:
            return
        try:
            worker.markers_ready.disconnect()
            worker.error.disconnect()
        except (TypeError, RuntimeError):
            pass
        worker.cancel()
        self._review_marker_zombies.append(worker)
        worker.finished.connect(self._reap_marker_zombies)
        self._review_marker_worker = None

    def _reap_marker_zombies(self) -> None:
        """Remove finished marker workers from the zombie list."""
        alive = [w for w in self._review_marker_zombies if w.isRunning()]
        reaped = len(self._review_marker_zombies) - len(alive)
        self._review_marker_zombies = alive
        if reaped:
            flog(f"review: marker_zombies reaped={reaped} remaining={len(alive)}", "DEBUG")

    def _on_markers_ready(self, trace_id: str, markers: list) -> None:
        """Slot: MarkerScanWorker found modification dates inside the viewport."""
        self._review_marker_worker = None
        if not self._review_snap_mode:
            return
        bar = self._review_date_bar
        if bar is None:
            return
        bar.set_markers(markers, as_ticks=True)
        first = markers[0][0] if markers else None
        last = markers[-1][0] if markers else None
        flog(
            f"[{trace_id}] review: extent_markers_applied n={len(markers)} "
            f"first={first} last={last} as_ticks=True",
            "INFO",
        )

    def _on_marker_error(self, trace_id: str, message: str) -> None:
        """Slot: MarkerScanWorker encountered a fatal error."""
        self._review_marker_worker = None
        flog(f"[{trace_id}] review: marker_worker_error msg={message}", "ERROR")

    def _refresh_extent_markers(self, bbox_per_layer: dict, trace_id: str) -> None:
        """Spawn a background scan of modification dates inside the viewport.

        CHANGE C: markers are extent-scoped, so panning/zooming recomputes which
        dates have events in the current view. Skipped if the viewport signature
        is unchanged since the last scan.
        """
        if not self._review_snap_mode or self._journal is None:
            return
        t_min = self._review_global_t0_iso
        t_max = self._review_global_t1_iso
        if not t_min or not t_max:
            flog(
                f"[{trace_id}] review: extent_markers skipped "
                f"reason=no_global_date_range",
                "DEBUG",
            )
            return

        sig_parts = []
        for info in self._review_layer_infos:
            fp = info["fingerprint"]
            bbox = bbox_per_layer.get(fp)
            if bbox is not None:
                sig_parts.append(
                    f"{fp}:{bbox.xMinimum():.3f}:{bbox.yMinimum():.3f}:"
                    f"{bbox.xMaximum():.3f}:{bbox.yMaximum():.3f}"
                )
        signature = "|".join(sorted(sig_parts))
        if signature == self._review_marker_bbox_signature:
            flog(
                f"[{trace_id}] review: extent_markers unchanged_signature skip",
                "DEBUG",
            )
            return
        self._review_marker_bbox_signature = signature

        self._cancel_marker_worker()
        from .widgets.marker_scan_worker import MarkerScanWorker
        worker = MarkerScanWorker(
            journal=self._journal,
            layer_infos=self._review_layer_infos,
            bbox_per_layer=bbox_per_layer,
            t_min=t_min,
            t_max=t_max,
            trace_id=trace_id,
        )
        worker.markers_ready.connect(self._on_markers_ready)
        worker.error.connect(self._on_marker_error)
        # Unparented, and its result slot drops self._review_marker_worker
        # while run() is still closing its connection: without this hold,
        # that was the last reference and the running QThread was deleted
        # (qFatal) on an ordinary pan.
        from .widgets.snapshot_rebuild_worker import retain_until_finished
        retain_until_finished(worker)
        self._review_marker_worker = worker
        worker.start()
        flog(
            f"[{trace_id}] review: marker_worker_started "
            f"t_min={t_min} t_max={t_max} n_layers={len(self._review_layer_infos)}",
            "INFO",
        )

    def _on_review_snap_extent_changed(self) -> None:
        """Slot: canvas extent changed - (re)start spatial debounce."""
        if not self._review_snap_mode:
            return
        debounce_alive = self._review_snap_ext_debounce is not None
        flog(
            f"review: snap_extent_signal_received debounce_alive={debounce_alive}",
            "DEBUG",
        )
        if self._review_snap_ext_debounce is not None:
            self._review_snap_ext_debounce.start()

    def _on_review_snap_bbox_fire(self) -> None:
        """Slot: spatial debounce expired - refresh snapshot for current extent.

        Pan/zoom only changes the viewport, not the cutoff date: re-apply the
        cached reconstruction instead of spawning a worker (no SQL + replay).
        Falls back to a full reload only if no cached result matches the date.
        """
        if not self._review_snap_mode or not self._review_snap_pending_iso:
            return
        if (self._review_snap_raw_result is not None and self._review_snap_raw_iso == self._review_snap_pending_iso):
            flog(
                f"review: snap_extent_reapply iso={self._review_snap_pending_iso}",
                "DEBUG",
            )
            self._refresh_snapshot_for_extent()
        else:
            flog(
                f"review: snap_extent_reload iso={self._review_snap_pending_iso}",
                "DEBUG",
            )
            self._on_snapshot_date_changed(self._review_snap_pending_iso)

    def _setup_review_status_bar(self) -> None:
        """Create the Review status widget (not yet in status bar)."""
        try:
            from .widgets.review_status_widget import ReviewStatusWidget
            self._review_status_widget = ReviewStatusWidget()
            self._review_status_widget.stop_requested.connect(
                self._review_stop_from_statusbar,
            )
            flog("review: status_bar widget created", "DEBUG")
        except Exception as exc:  # noqa: BLE001
            flog(f"review: status_bar setup failed: {exc!r}", "WARNING")
            self._review_status_widget = None

    def _show_review_status_bar(self) -> None:
        """Insert the Review pill into the QGIS status bar (visible).

        Self-heals if the pill was torn down on a previous dialog close: the
        dialog instance is reused across reopen (recover.py caches self.dlg),
        so __init__ does not run again and _review_status_widget would stay
        None forever, leaving Review with no status indicator nor stop control.
        """
        if self._review_status_widget is None:
            flog("review: status_bar widget missing on show - recreating", "INFO")
            self._setup_review_status_bar()
        if self._review_status_widget is None:
            flog("review: status_bar show skipped (widget unavailable)", "WARNING")
            return
        try:
            status_bar = self.iface.mainWindow().statusBar()
            status_bar.insertPermanentWidget(0, self._review_status_widget)
            self._review_status_widget.show()
            flog("review: status_bar widget shown", "DEBUG")
        except Exception as exc:  # noqa: BLE001
            flog(f"review: status_bar show failed: {exc!r}", "WARNING")

    def _hide_review_status_bar(self) -> None:
        """Remove the Review pill from the QGIS status bar."""
        if self._review_status_widget is None:
            return
        try:
            status_bar = self.iface.mainWindow().statusBar()
            status_bar.removeWidget(self._review_status_widget)
        except Exception as exc:  # noqa: BLE001
            # WARNING: three of the four callers do not destroy the widget
            # afterwards, so a failure here leaves the Review pill in the
            # status bar telling the user he is still in Review mode.
            flog(f"review: status_bar removeWidget failed, the Review pill "
                 f"may stay visible ({exc!r})", "WARNING")

    def _teardown_review_status_bar(self) -> None:
        """Destroy the Review status widget entirely."""
        self._hide_review_status_bar()
        if self._review_status_widget is not None:
            self._review_status_widget.deleteLater()
        self._review_status_widget = None

    def _review_connect_auto_refresh(self) -> None:
        """Connect canvas extentsChanged + journal signal for live Review refresh."""
        if self._review_active:
            return
        self._review_active = True
        canvas = self.iface.mapCanvas()
        canvas.extentsChanged.connect(self._review_on_extent_changed)
        # The tracker we listen to, kept: on a project switch self._tracker
        # is replaced, and disconnecting "the current one" missed ours.
        self._review_tracker = self._tracker
        if self._tracker is not None:
            self._tracker.committed_features.connect(self._review_on_commit_changed)
        # A Review session describes ONE project's journal and layers. It
        # used to survive a project switch (it persists when the dialog
        # closes): dead overlay ids, the old project's counts on the new
        # canvas, fingerprints of project A queried in the journal of B.
        from qgis.core import QgsProject  # noqa: PLC0415
        QgsProject.instance().aboutToBeCleared.connect(
            self._review_on_project_clearing)
        self._show_review_status_bar()
        if self._review_status_widget is not None:
            self._review_status_widget.activate()
        flog("review: auto_refresh connected", "INFO")

    def _review_disconnect_auto_refresh(self) -> None:
        """Disconnect canvas and journal signals for Review auto-refresh."""
        if not self._review_active:
            return
        self._review_active = False
        self._review_debounce.stop()
        canvas = self.iface.mapCanvas()
        try:
            canvas.extentsChanged.disconnect(self._review_on_extent_changed)
        except TypeError as exc:
            flog(f"review: canvas_disconnect_failed reason={exc}", "WARNING")
        tracker = getattr(self, "_review_tracker", self._tracker)
        self._review_tracker = None
        if tracker is not None:
            try:
                tracker.committed_features.disconnect(self._review_on_commit_changed)
            except (AttributeError, TypeError, RuntimeError) as exc:
                flog(f"review: tracker_disconnect_failed reason={exc}", "WARNING")
        try:
            from qgis.core import QgsProject  # noqa: PLC0415
            QgsProject.instance().aboutToBeCleared.disconnect(
                self._review_on_project_clearing)
        except (TypeError, RuntimeError) as exc:
            flog(f"review: project_clear_disconnect skipped ({exc!r})", "DEBUG")
        if self._review_status_widget is not None:
            self._review_status_widget.deactivate()
        self._hide_review_status_bar()
        flog("review: auto_refresh disconnected", "INFO")

    def _review_on_extent_changed(self, *_args) -> None:
        """Slot for canvas extentsChanged - ignored when snapshot debounce owns pans."""
        if not self._review_active:
            return
        if self._review_snap_mode and self._review_snap_ext_debounce is not None:
            return
        self._review_debounce.start()

    def _review_auto_refresh(self) -> None:
        """Debounce fire: refresh viewport (snapshot mode re-fires date change)."""
        if not self._review_active:
            return
        if self._review_snap_mode and self._review_snap_pending_iso:
            self._on_snapshot_date_changed(self._review_snap_pending_iso)

    def _review_on_commit_changed(self, n_events: int) -> None:
        """Slot for tracker committed_features signal (after WriteQueue flush).

        Invalidates the snapshot cache and triggers a rebuild when in Review
        Snapshot mode. This ensures commits during a session are reflected
        immediately without requiring a date change.

        Args:
            n_events: Number of events persisted in the flush batch.
        """
        if not self._review_active:
            return
        if not self._review_snap_mode:
            return
        if not self._review_snap_pending_iso:
            return
        flog(
            f"review: commit_signal_received n_events={n_events} "
            f"iso={self._review_snap_pending_iso} invalidating_cache",
            "INFO",
        )
        # The worker in flight read the journal BEFORE this commit. Left
        # running, its result landed inside the debounce, was cached under
        # the current date, and the debounce then reused it: Review never
        # showed the commit. Cancel it and forget which answer we expect.
        self._cancel_snap_worker(disconnect_signals=True)
        self._review_snap_expected_trace = ""
        self._review_snap_raw_result = None
        self._review_snap_raw_iso = ""
        flog(
            f"review: cache_invalidated iso={self._review_snap_pending_iso} "
            f"rebuild_debounced",
            "INFO",
        )
        self._review_debounce.start()

    def _review_on_project_clearing(self) -> None:
        """QgsProject.aboutToBeCleared: end Review before the layers vanish.

        Runs before the project's layers are removed and before the plugin
        replaces the journal and the tracker, so the overlays, the date bar,
        the workers and the commit listener are all released while they
        still refer to the project they were built for.
        """
        flog("review: project_clearing, stopping the Review session", "INFO")
        self._review_wants_persist = False
        self._review_stop_session()
        toggle = getattr(self, "_review_toggle", None)
        if toggle is not None:
            toggle.blockSignals(True)
            toggle.setChecked(False)
            toggle.blockSignals(False)
            toggle.setEnabled(True)
        self._review_layer_infos = []

    def _recover_event_mode(self) -> None:
        """Event mode: threaded search with start/end date range."""
        self.progress_bar.setVisible(False)
        if self.progress_timer.isActive():
            self.progress_timer.stop()

        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.stop()
            self.worker_thread.wait(500)

        criteria = self._build_search_criteria()
        trace_id = generate_trace_id()
        self._active_search_trace_id = trace_id
        flog(
            f"[{trace_id}] recover_event: start"
            f" layer={criteria.datasource_fingerprint}"
            f" op={criteria.operation_type}"
            f" start={criteria.start_date} end={criteria.end_date}"
        )

        self.cancel_button.setText(self.tr("Arreter"))
        self.cancel_button.setIcon(theme_icon("stop", size=16))
        if hasattr(self.cancel_button, "setIconName"):
            self.cancel_button.setIconName("stop")

        self.worker_thread = LocalSearchThread(self._journal, criteria, trace_id=trace_id)
        self.worker_thread.results_ready.connect(self._on_search_complete)
        self.worker_thread.phase_changed.connect(self.update_phase)
        self.worker_thread.error_occurred.connect(self.on_error)
        self._recover_started_at = time.monotonic()
        self._pending_search_result = None
        self._start_logo_activity(self._glow_color, "recover")
        QTimer.singleShot(100, self.worker_thread.start)

    def _recover_version_mode(self) -> None:
        """Version/temporal mode: fetch events after cutoff in background thread."""
        from .core.restore_contracts import RestoreCutoff, CutoffType

        cutoff_dt = self.time_slider.cutoff_datetime()
        cutoff_iso = cutoff_dt.toUTC().toString("yyyy-MM-ddTHH:mm:ss")

        checked_fps = self._get_version_checked_fingerprints()
        if not checked_fps:
            self._is_recovering = False
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            qlog(self.tr("Cochez au moins une couche."), "WARNING")
            return

        # Inclusive=True: rewind compensates events committed within the
        # same second as the snapshot (cf. core.restore_contracts).
        cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)
        self._version_restore_cutoff = cutoff
        trace_id = generate_trace_id()
        self._active_restore_trace_id = trace_id

        # A fingerprint that names two loaded layers is not an identity: the
        # events of both merge into one datasource, the collapse buckets them
        # together and a compensation can land on the wrong table. Known cause:
        # the DB providers capture the schema instead of the table
        # (identity._normalize_db_source). Refuse rather than operate on a
        # merged identity -- correcting the fingerprint itself rewrites a value
        # stored in every journal row and needs a migration.
        if self._refuse_fingerprint_collision(checked_fps, trace_id):
            self._active_restore_trace_id = ""
            return

        # A fingerprint the v6 migration refused to attach is the softer
        # cousin of the collision above: the events are all there, but the
        # ones written under the old identity stay out of this scope. The
        # user must read that before the rewind writes anything, not after
        # wondering why his oldest edits were not undone.
        self._warn_if_reconciliation_degraded("rewind")
        # Read-your-writes: plan on a journal that holds every edit made.
        if not self._journal_barrier(self.tr("Rewind"), checked_fps, trace_id):
            self._active_restore_trace_id = ""
            return
        # The scope of THIS rewind, fixed now: the undo state and the final
        # message follow it, not the checkboxes of later.
        self._rewind_scope_fps = list(checked_fps)
        # From here until the chain ends nothing else may start: the Undo
        # button would otherwise stay armed on the previous run while this
        # one is planned, confirmed and written (revue brev2, P1).
        self.undo_last_btn.setEnabled(False)

        # v7: the window IS the plan. `fetch_events_after_cutoff` carries,
        # from the journal alone, the user events after the cutoff and the
        # plan items of the history before it (a compensation made after
        # the cutoff re-applied, one in effect at the cutoff and undone
        # since made again, refusals by name), and the collapse orders them
        # in one chronological pass. A compensation made BEFORE the cutoff
        # is part of the state asked for.
        #
        # The dialog used to undo its own undo pool first, plus the
        # compensations `_reapply_set_from_journal` rebuilt from the
        # journal, invalidate their traces, and re-read the window at the
        # same date. That re-read saw the invalidations just written
        # (invalidated_at = now, after the cutoff) and compensated again
        # everything the undo had reverted: "Revenir a l'etat actuel" ended
        # in the rewound state reported as restored, and a rewind to a date
        # after a recovery deleted the recovered features before the window
        # re-inserted them (scenario rw_rewind_no_autoundo). Nothing is
        # undone before a rewind now: the window alone is applied.
        include_traces = True
        flog(f"[{trace_id}] recover_version: start scope={len(checked_fps)} layer(s) "
             f"cutoff={cutoff_iso} include_traces={include_traces} "
             f"(the window is the complete plan, nothing undone first)")

        self.progress_bar.setVisible(False)
        self._start_logo_activity(self._glow_color, "recover")
        self._recover_started_at = time.monotonic()
        self._version_cutoff_dt = cutoff_dt

        if self._version_fetch_thread and self._version_fetch_thread.isRunning():
            self._version_fetch_thread.stop()
            self._version_fetch_thread.wait(500)

        self._version_fetch_total = 0
        self._version_fetch_thread = VersionFetchThread(
            self._journal, checked_fps, cutoff, trace_id=trace_id,
            include_traces=include_traces)
        self._version_fetch_thread.count_ready.connect(self._on_version_fetch_count_ready)
        self._version_fetch_thread.events_ready.connect(self._on_version_fetch_done)
        self._version_fetch_thread.error_occurred.connect(self._on_version_fetch_error)
        self._version_fetch_thread.start()

    def _on_version_fetch_count_ready(self, total: int) -> None:
        # Stashed so _on_version_fetch_done can tell a complete window from a
        # window truncated by the per-datasource LIMIT. count_events_after_cutoff
        # and fetch_events_after_cutoff share the same predicate, so any
        # shortfall means events were dropped by the cap.
        self._version_fetch_total = total
        if total > 0:
            self.update_phase(self.tr("{count} evenement(s) a analyser").format(count=total))

    def _on_version_fetch_error(self, error_msg: str) -> None:
        """Handle error from version fetch thread."""
        trace_id = self._active_restore_trace_id
        if trace_id:
            flog(f"[{trace_id}] recover_version: fetch_error={error_msg}", "ERROR")
            self._active_restore_trace_id = ""
        self._is_recovering = False
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)
        self.on_error(error_msg)

    def _refuse_fingerprint_collision(self, checked_fps, trace_id: str) -> bool:
        """Abort when a checked fingerprint denotes more than one loaded layer.

        Returns True when the caller must stop. Never blocks on an internal
        error: a guard that crashes a rewind is worse than the guard's absence.
        """
        from .core.identity import find_fingerprint_collisions

        try:
            from qgis.core import QgsProject, QgsVectorLayer
            layers = [
                lyr for lyr in QgsProject.instance().mapLayers().values()
                if isinstance(lyr, QgsVectorLayer) and lyr.isValid()
            ]
            collisions = find_fingerprint_collisions(layers)
        except Exception as exc:  # noqa: BLE001
            flog(f"[{trace_id}] recover_version: collision check failed: {exc}",
                 "WARNING")
            return False

        hit = {fp: names for fp, names in collisions.items() if fp in checked_fps}
        if not hit:
            return False

        names = sorted({n for names in hit.values() for n in names})
        flog(f"[{trace_id}] recover_version: fingerprint_collision "
             f"n_fingerprints={len(hit)} layers={names} status=refused", "ERROR")
        self._is_recovering = False
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)
        QMessageBox.warning(
            self,
            self.tr("Couches non distinguables"),
            self.tr(
                "Ces couches partagent la meme empreinte de source : {layers}.\n\n"
                "Leurs historiques sont melanges et une restauration pourrait "
                "s'appliquer a la mauvaise couche. Restauration annulee."
            ).format(layers=", ".join(names)),
        )
        return True

    def _refuse_truncated_window(self, raw_count: int, trace_id: str) -> bool:
        """Abort the rewind when the fetched window was cut by the LIMIT.

        ``fetch_events_after_cutoff`` caps each datasource at
        MAX_EVENTS_PER_RESTORE and orders by ``created_at DESC``, so the
        events it drops are the OLDEST ones -- precisely those that carry
        the state at the cutoff. Replaying a truncated window brings the
        layer to some intermediate state and reports success: a silent
        partial rewind. The post-collapse volume guard cannot catch this
        because the fetched list is already bounded by the cap.

        ``count_events_after_cutoff`` and ``fetch_events_after_cutoff``
        share the same WHERE clause, so a shortfall can only come from the
        cap. Both conditions are required: the cap must have been reached
        AND fewer events returned than exist, so that a concurrent
        retention purge between the two queries is not mistaken for
        truncation.

        Returns True when the caller must stop.
        """
        from .core.restore_contracts import MAX_EVENTS_PER_RESTORE

        fetch_total = getattr(self, "_version_fetch_total", 0)
        if raw_count < MAX_EVENTS_PER_RESTORE or fetch_total <= raw_count:
            return False

        flog(f"[{trace_id}] rewind: truncated_window fetched={raw_count} "
             f"total={fetch_total} dropped={fetch_total - raw_count} "
             f"status=refused (oldest events dropped by the cap)", "ERROR")
        self._is_recovering = False
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)
        if trace_id:
            self._active_restore_trace_id = ""
        qlog(self.tr(
            "{count} evenements depassent la limite ({limit}). "
            "Choisissez une date plus recente."
        ).format(count=fetch_total, limit=MAX_EVENTS_PER_RESTORE), "WARNING")
        return True

    def _on_version_fetch_done(self, events) -> None:
        """Handle events fetched by background thread; run confirmations on main thread."""
        from .core.restore_contracts import MAX_EVENTS_PER_RESTORE, WARN_EVENTS_THRESHOLD
        from .core import collapse_rewind_events_with_stats

        raw_count = len(events)
        trace_id = self._active_restore_trace_id
        if getattr(self, "_closing", False):
            # A fetch result queued before the dialog was closed: nobody is
            # there to confirm, and nothing may be written on its behalf.
            flog(f"[{trace_id}] on_version_fetch_done: dialog closed, "
                 f"{raw_count} event(s) dropped, nothing planned", "WARNING")
            self._is_recovering = False
            return
        if self._refuse_truncated_window(raw_count, trace_id):
            return

        # The live layers' key columns: a pre-v7 composite key is re-keyed
        # from its snapshot instead of refused (`_live_key_columns`).
        events, dedup_stats = collapse_rewind_events_with_stats(
            events, key_columns=self._live_key_columns(events))
        total_count = len(events)
        cutoff_dt = getattr(self, '_version_cutoff_dt', None)
        # Stash dedup stats so the runner can fold them into CYCLE_SUMMARY.
        self._pending_cycle_stats = dict(dedup_stats)
        flog(f"[{trace_id}] on_version_fetch_done: raw={raw_count} "
             f"collapsed={total_count} "
             f"plan_reapply={dedup_stats.get('plan_reapply', 0)} "
             f"plan_recompensate={dedup_stats.get('plan_recompensate', 0)} "
             f"plan_refused={dedup_stats.get('plan_refused', 0)}")
        _flog_events("active", events, trace_id)

        if total_count == 0:
            # An empty window means the layers already hold their state at
            # this date: nothing is undone, nothing is written. The
            # compensations of this session's runs made before the date are
            # part of that state (the cursor at "now" included); undoing
            # them is what "Annuler le dernier restore" is for.
            if trace_id:
                self._active_restore_trace_id = ""
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            msg = self.tr(
                "Les couches sont deja dans leur etat a cette date : aucun "
                "evenement a compenser, rien n'a ete modifie.")
            if self._last_run_in_scope(self._rewind_scope()):
                msg += " " + self.tr(
                    "Le dernier Rewind ou restore de ces couches fait partie "
                    "de cet etat : pour le defaire, utilisez 'Annuler le "
                    "dernier restore', ou choisissez une date anterieure a "
                    "ce Rewind.")
            flog(f"[{trace_id}] on_version_fetch_done: empty window, "
                 f"layers already at the cutoff, nothing undone")
            qlog(msg)
            return

        if total_count > MAX_EVENTS_PER_RESTORE:
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            if trace_id:
                self._active_restore_trace_id = ""
            msg = self.tr(
                "{count} evenements depassent la limite ({limit}). "
                "Choisissez une date plus recente."
            ).format(count=total_count, limit=MAX_EVENTS_PER_RESTORE)
            qlog(msg, "WARNING")
            return

        self._stop_logo_activity()
        if total_count > WARN_EVENTS_THRESHOLD:
            reply = QMessageBox.question(
                self,
                self.tr("Volume important"),
                self.tr("{count} evenements seront analyses. Continuer ?").format(
                    count=total_count),
                QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
            if reply != QtCompat.MSG_YES:
                self._is_recovering = False
                self._stop_logo_activity()
                self.progress_bar.setVisible(False)
                self.enable_controls(True)
                self.recover_button.setEnabled(True)
                if trace_id:
                    self._active_restore_trace_id = ""
                return

        self.time_slider.set_event_count(len(events))

        n_layers = len({e.datasource_fingerprint for e in events})
        cutoff_label = cutoff_dt.toString("dd/MM/yyyy HH:mm:ss") if cutoff_dt else "?"
        reply = QMessageBox.question(
            self,
            self.tr("Confirmer la restauration"),
            self.tr(
                "Revenir au {date} ?\n\n"
                "{count} evenement(s) sur {n_layers} couche(s) seront rejoues en inverse."
            ).format(
                date=cutoff_label,
                count=len(events),
                n_layers=n_layers,
            ),
            QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
        if reply != QtCompat.MSG_YES:
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            if trace_id:
                self._active_restore_trace_id = ""
            return

        self._execute_version_restore(events)

    def _rewind_scope(self) -> list:
        """Fingerprints the current rewind moves, fixed when it started."""
        scope = getattr(self, "_rewind_scope_fps", None)
        if scope:
            return list(scope)
        return list(self._get_version_checked_fingerprints())

    def _expand_scope(self, fps) -> set:
        """`fps` plus the obsolete fingerprints the journal aliases to them."""
        out = {fp for fp in (fps or []) if fp}
        read_conn = self._get_dialog_read_conn()
        if read_conn is None:
            return out
        try:
            from .core.event_stream_repository import resolve_scope
            for fp in list(out):
                out.update(resolve_scope(read_conn, fp))
        except Exception as exc:  # noqa: BLE001 - the plain set still scopes
            flog(f"undo_scope: alias expansion failed, plain fingerprints "
                 f"used ({exc})", "WARNING")
        return out

    @staticmethod
    def _merge_undo_scopes(first, second) -> dict:
        """Per-layer union by event id, in the ORDER OF APPLICATION.

        `second` is the run that has just ended: its events were applied
        after everything in `first`, so they go after them, in the order
        that run applied them (an event applied again moves to its new
        place). `UndoRunner` walks each layer backwards (`undo_prepare`
        reverses it): the undo is then the exact reverse of what was
        applied. The pool used to be re-sorted on (created_at, event_id);
        a plan item carries the date of the toggle it undoes and its
        chronology breaks ties on the TOGGLE's id, not on its own, and a
        plan applies INSERT, UPDATE and DELETE compensations in phases:
        neither is the order a re-sort by date gives back.
        """
        merged: dict = {}
        for scope in (first or {}, second or {}):
            for fp, evts in scope.items():
                bucket = merged.setdefault(fp, {})
                for e in evts or ():
                    key = e.event_id if e.event_id is not None else id(e)
                    bucket.pop(key, None)
                    bucket[key] = e
        return {fp: list(bucket.values())
                for fp, bucket in merged.items() if bucket}

    def _in_application_order(self, by_ds, cutoff) -> dict:
        """What a strict run wrote, each layer in the order its plan applied it.

        `StrictRestoreRunner` publishes its written scope in INPUT order
        (the collapsed window, newest first); its planner applies the
        compensations in phases (INSERT, then UPDATE newest first, then
        DELETE). The same planner, on the same events, gives that order
        back: the undo can then be its exact reverse.
        """
        from .core.restore_planner import plan_temporal_restore
        out: dict = {}
        for fp, evts in (by_ds or {}).items():
            evts = list(evts or ())
            rank: dict = {}
            if evts:
                try:
                    plan = plan_temporal_restore(
                        evts, fp, evts[0].layer_name_snapshot or fp, cutoff)
                    rank = {a.event_id: i for i, a in enumerate(plan.actions)}
                except Exception as exc:  # noqa: BLE001 - input order kept
                    flog(f"undo_scope: plan order unreadable for fp="
                         f"{fp[:16]} ({exc}); input order kept", "WARNING")
            out[fp] = sorted(evts, key=lambda e: rank.get(e.event_id, len(rank)))
        return out

    def _without_layers_edited_during_run(self, by_ds) -> dict:
        """`by_ds` minus the layers the user edited while the run went on.

        The capture is given back to a layer as soon as the run is done
        with it, so the user can save there while the next layers are
        written, and his commit is journalled. Such a layer leaves the undo
        state like any edited layer (`_invalidate_undo_for`): an Undo would
        otherwise offer to write the run's values over his edit. Consumes
        the set gathered during the run.
        """
        edited = set(getattr(self, "_edited_during_run", None) or ())
        self._edited_during_run = set()
        if not edited:
            return by_ds
        kept = {fp: evts for fp, evts in (by_ds or {}).items() if fp not in edited}
        if len(kept) != len(by_ds or {}):
            flog(f"undo_scope: {len(by_ds) - len(kept)} layer(s) edited by the "
                 f"user during the run, left out of the undo state")
        return kept

    def _last_run_in_scope(self, fps) -> bool:
        """True when the last run wrote on one of the layers `fps` names."""
        scope = self._expand_scope(fps)
        return any(fp in scope for fp in self._manual_undo_scope())

    def _set_undo_state(self, by_ds, arm_button: bool = True) -> None:
        """Install `by_ds` as the undo pool; the button follows the last run unless a run owns it."""
        state = {fp: list(evts) for fp, evts in (by_ds or {}).items() if evts}
        self._last_restore_by_ds = state or None
        self._last_restore_events = (
            [e for evts in state.values() for e in evts] or None)
        self.undo_last_btn.setEnabled(
            bool(self._manual_undo_scope()) and arm_button
            and not self._run_in_flight())

    def _manual_undo_scope(self) -> dict:
        """What "Annuler le dernier restore" reverts: the last run, nothing else.

        The pool (`_last_restore_by_ds`) is per layer and cumulative: runs
        are merged into it so that a Rewind of B leaves the compensations
        of A to a later Rewind of A, and the journal adds to it, for a
        forward hop, compensations written by other sessions. Fed from the
        pool, the button undid the restore before the last one as well, a
        previous Rewind of another layer, and a compensation rebuilt from
        the journal for a hop the user had declined; its question counted
        them all under "le dernier restore". Returns fresh lists: the undo
        runs on a copy, never on the live state.
        """
        run_ids = getattr(self, "_last_run_ids", None) or set()
        scope: dict = {}
        for fp, evts in (self._last_restore_by_ds or {}).items():
            mine = [e for e in evts if e.event_id in run_ids]
            if mine:
                scope[fp] = mine
        return scope

    def _record_last_run(self, written_by_ds) -> None:
        """The run that just ended wrote `written_by_ds`: it becomes the last run.

        Its journal mark, read at its launch (`_launch_runner`), goes with
        it: the Undo invalidates the traces written above that mark and no
        other (`_run_trace_split`). Consumed here, so that a run recorded
        without a launch of its own never borrows another run's mark.
        """
        self._last_run_ids = {
            e.event_id for evts in (written_by_ds or {}).values()
            for e in evts if e.event_id is not None}
        self._last_run_mark = getattr(self, "_run_journal_mark", None)
        self._run_journal_mark = None

    def _drop_undone(self, undone_ids, arm_button: bool = True) -> None:
        """Take what an undo really reverted out of the pool and the last run.

        Everything else stays as it is NOW. In particular a layer the user
        edited while the undo ran has already left the pool
        (`_invalidate_undo_for`) and is not re-armed from the undo input.
        """
        done = {int(i) for i in (undone_ids or []) if i}
        left = {fp: [e for e in evts if (e.event_id or 0) not in done]
                for fp, evts in (self._last_restore_by_ds or {}).items()}
        self._last_run_ids = (
            (getattr(self, "_last_run_ids", None) or set()) - done)
        self._set_undo_state(left, arm_button=arm_button)

    def _undo_outcome(self, runner, result, undo_input) -> dict:
        """What an undo did to each input event.

        Read from the runner (`UndoRunner.undo_outcome`); when it is gone,
        rebuilt from the result, whose by_ds is the UNDONE scope by
        contract. Anything not proven undone stays in the pool
        (`_drop_undone` takes out the undone ones only): its compensation
        may still be in the layer.
        """
        outcome = None
        reader = getattr(runner, "undo_outcome", None)
        if callable(reader):
            try:
                outcome = dict(reader())
            except Exception as exc:  # noqa: BLE001 - fall back on the result
                flog(f"undo_outcome: unreadable ({exc}), rebuilt from the "
                     f"result", "WARNING")
        if outcome is None:
            undone = {e.event_id for evts in (result.by_ds or {}).values()
                      for e in evts if e.event_id}
            failed = set(result.failed_eids or []) - undone
            rest = [e.event_id or 0 for evts in (undo_input or {}).values()
                    for e in evts
                    if e.event_id not in undone and e.event_id not in failed]
            outcome = {"undone": sorted(undone), "failed": sorted(failed),
                       "skipped": [], "unattempted": rest, "cancelled": False}
        return outcome

    def _format_undo_shortfall(self, outcome: dict) -> str:
        """Name what an undo left in place; empty when it reverted everything."""
        parts = []
        if outcome.get("failed"):
            parts.append(self.tr("{count} echec(s)").format(
                count=len(outcome["failed"])))
        if outcome.get("skipped"):
            parts.append(self.tr("{count} introuvable(s) laissee(s) en "
                                 "l'etat").format(count=len(outcome["skipped"])))
        if outcome.get("unattempted"):
            parts.append(self.tr("{count} non traitee(s)").format(
                count=len(outcome["unattempted"])))
        return ", ".join(parts)

    def _execute_version_restore(self, events) -> None:
        """Execute reverse replay restore (async, strict atomic per layer)."""
        busy = self._run_in_flight()
        if busy or getattr(self, "_closing", False):
            # Defence in depth: the entry points refuse a second run, this
            # is the last door before the layers are written. The run in
            # flight keeps its flags, its runner and its controls: only
            # this rewind's own preparation is released.
            flog(f"[{self._active_restore_trace_id}] execute_version_restore: "
                 f"refused, n={len(events)} busy={busy or '-'} "
                 f"closing={getattr(self, '_closing', False)}", "ERROR")
            self._is_recovering = False
            if busy:
                qlog(self._busy_message(self.tr("Rewind"), busy), "WARNING")
            return
        trace_id = self._active_restore_trace_id or generate_trace_id()
        self._active_restore_trace_id = trace_id
        self.recover_button.setEnabled(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._start_logo_activity(self._glow_color, "restore")
        self._restore_in_progress = True
        self._version_restore_events = events
        self._restore_started_at = time.monotonic()

        cutoff = self._version_restore_cutoff
        flog(f"[{trace_id}] execute_version_restore: "
             f"n={len(events)} cutoff={'set' if cutoff else 'MISSING'}")
        _flog_events("restore", events, trace_id)
        n_layers = len({e.datasource_fingerprint for e in events})
        self.update_phase(self.tr(
            "Application : {count} evenement(s) sur {layers} couche(s)"
        ).format(count=len(events), layers=n_layers))
        self._freeze_canvas_for_write(True)
        events_by_ds_preview: dict = {}
        read_conn = self._get_dialog_read_conn()
        for e in events:
            fp = e.datasource_fingerprint or ''
            events_by_ds_preview.setdefault(fp, []).append(e)
        self._restore_counts_before = self._rewind_count_snapshot(
            "before_restore", events_by_ds_preview)

        def resolver(evt):
            return find_target_layer(evt, read_conn)

        # `journal_path`: the journal this rewind was planned from. The
        # runner reads it per layer for the durable purge horizon and the
        # key continuity proofs, and refuses a layer whose journal it
        # cannot read (never the session memory alone).
        runner = StrictRestoreRunner(
            events, resolver, cutoff,
            write_queue=self._write_queue,
            tracker=self._tracker, trace_id=trace_id,
            parent=self,
            extra_stats=getattr(self, '_pending_cycle_stats', None),
            journal_path=self._run_journal_path(),
        )
        # Consume the stash; subsequent runs must repopulate it.
        self._pending_cycle_stats = None
        runner.progress.connect(self._on_restore_runner_progress)
        runner.layer_started.connect(self._on_restore_runner_layer)
        runner.finished.connect(self._on_version_restore_done)
        self._launch_runner(runner)

    def _on_restore_runner_progress(self, done: int, total: int) -> None:
        if total > 0:
            self._smooth_set_progress(int(done / total * 100))

    def _end_progress_ui(self) -> None:
        """Bar and phase label off: this cycle is over.

        RLU-057: the phase label now follows the run layer by layer, so
        leaving it up after the last layer would keep claiming a write in
        progress on a cycle that has already reported.
        """
        self.progress_bar.setVisible(False)
        self.progress_phase_label.setVisible(False)

    def _on_restore_runner_layer(self, layer_name: str, index: int,
                                 n_layers: int, n_events: int) -> None:
        """RLU-057: name the layer currently being written.

        Cycles of 95 s and 283 s were measured on the graphics thread
        (one died in flight at 282 s). The work still runs there, so the
        only thing that can be promised is that the wait is legible: the
        phase label follows the run layer by layer instead of keeping the
        text written before it started.
        """
        self.update_phase(self.tr(
            "Couche {index}/{total} : {name} ({count} evenement(s))"
        ).format(index=index, total=n_layers, name=layer_name,
                 count=n_events))

    def _on_version_restore_done(self, result) -> None:
        trace_id = self._active_restore_trace_id
        self._restore_in_progress = False
        # RLU-053: read the partial bucket off the runner BEFORE dropping
        # the reference; it is the only place that knows which captured
        # fields the buffer had to abandon on the way in.
        # RLU-055: `_cancelled_restore_runner` is the runner a cancel
        # already detached; it keeps running to its own _finish, and its
        # report is the only truthful account of what the user got.
        runner = self._restore_runner or self._cancelled_restore_runner
        partial_n, partial_fields = self._partial_restore_report(runner)
        apply_report = self._runner_apply_report(runner)
        self._restore_runner = None
        self._cancelled_restore_runner = None
        cleanup_temp_layers()
        self._smooth_set_progress(100)
        self._stop_logo_activity()
        if trace_id:
            started = self._restore_started_at
            elapsed_ms = int((time.monotonic() - started) * 1000) if started else 0
            flog(
                f"[{trace_id}] recover_version: done"
                f" ok={result.total_ok} fail={result.total_fail}"
                f" elapsed_ms={elapsed_ms}"
            )
            self._active_restore_trace_id = ""

        before = getattr(self, '_restore_counts_before', {})
        restore_by_ds = result.by_ds or {}
        self._rewind_count_snapshot("after_restore", restore_by_ds, before=before)
        self._restore_counts_before = {}
        events = getattr(self, '_version_restore_events', None)
        if result.by_ds and events:
            # Merged per layer into the pool, in the order the plan applied
            # it: the button, "Annuler le dernier restore", points at this
            # run and at it alone, and walks it backwards.
            written = self._without_layers_edited_during_run(
                self._in_application_order(
                    result.by_ds, self._version_restore_cutoff))
            self._record_last_run(written)
            self._set_undo_state(self._merge_undo_scopes(
                self._last_restore_by_ds, written))
            flog(f"undo_last_btn: ENABLED ({result.total_ok} events)")
            for fp, evts in (result.by_ds or {}).items():
                layer_name = evts[0].layer_name_snapshot if evts else '?'
                flog(f"  stored_undo layer={layer_name!r} "
                     f"fp={fp[:16]}... n={len(evts)}")
        else:
            flog("on_version_restore_done: nothing written "
                 "-> undo state NOT updated")
        self._version_restore_events = None

        detail_lines = self._build_restore_summary(result.by_ds)
        partial_line = self._format_partial_restore(partial_n, partial_fields)
        # RLU-055: the same buckets as the CYCLE_SUMMARY line of this run.
        # `total_ok` / `total_fail` describe neither the entities that came
        # back nor the failures: see `_restore_buckets`.
        buckets = _restore_buckets(result, partial_n=partial_n,
                                   report=apply_report)
        msg = self._format_restore_outcome(buckets)
        # RLU-056: a rewind whose actions all reported success and whose
        # layers do not hold the expected number of entities is not a
        # success. The snapshot above already measured it; peek before the
        # report is graded, then say it out loud right after.
        state_mismatch = bool(self._state_mismatches)
        clean = (buckets["failed_total"] == 0 and buckets["cancelled"] == 0
                 and not result.errors and not partial_line
                 and not state_mismatch)
        if result.errors:
            msg += " | " + " | ".join(result.errors[:5])
        qlog(msg + partial_line + detail_lines,
             "INFO" if clean else "WARNING")
        self._report_state_mismatches()
        flog(f"[{trace_id}] recover_version: report planned={buckets['planned']} "
             f"applied={buckets['applied']} "
             f"partial={buckets['applied_partial']} "
             f"skipped={buckets['skipped']} failed={buckets['failed_total']} "
             f"cancelled={buckets['cancelled']} "
             f"plan_known={buckets['plan_known']} "
             f"state_mismatch={state_mismatch}")

        if result.total_ok > 0:
            flog(f"recover_version: refreshing canvas for {result.total_ok} ok")
            self.iface.mapCanvas().refresh()
            self._open_attribute_tables_if_requested(restore_by_ds, "version")

        self._end_progress_ui()
        self.enable_controls(True)
        self.recover_button.setEnabled(True)

    def _rewind_count_snapshot(self, label: str, by_ds: dict,
                               before: dict = None) -> dict:
        """Log feature counts per layer at a Rewind transition.

        Returns {fp: feat_count} for later comparison.
        For undo:    expected_delta = +1 per DELETE orig, -1 per INSERT orig
        For restore: expected_delta = +1 per DELETE event, -1 per INSERT event

        RLU-056: when ``before`` is supplied, every layer whose measured
        delta contradicts the expected one is also pushed onto
        `_state_mismatches`, for `_report_state_mismatches` to tell the
        user. Until now the comparison was computed, printed as
        ``[MISMATCH]`` in the log file and then dropped, so the run that
        produced

            CYCLE_SUMMARY cycle=undo apply_ok=273 apply_fail=0
            REWIND_STATE [after_undo] zone_mkt_rip expected_delta=+5
                                      actual_delta=+9 [MISMATCH]

        was announced to the user as a complete success while two layers
        were left in a state he never asked for. A count that does not
        match means the layer is NOT at the requested state, whatever the
        number of actions that reported success.
        """
        if not by_ds:
            return {}
        read_conn = self._get_dialog_read_conn()
        counts = {}
        is_undo = 'undo' in label
        mismatches = []
        flog(f"=== REWIND_STATE [{label}] ===")
        for fp, evts in by_ds.items():
            if not evts:
                continue
            layer_name = evts[0].layer_name_snapshot or '?'
            layer = find_target_layer(evts[0], read_conn)
            feat_now = layer.featureCount() if layer else -1
            counts[fp] = feat_now
            op_counts = {}
            for e in evts:
                op_counts[e.operation_type] = op_counts.get(e.operation_type, 0) + 1
            ops_str = ' '.join(
                f"{op}x{n}" for op, n in sorted(op_counts.items()))
            n_del = op_counts.get('DELETE', 0)
            n_ins = op_counts.get('INSERT', 0)
            if is_undo:
                exp_delta = n_ins - n_del
            else:
                exp_delta = n_del - n_ins
            line = (f"  layer={layer_name!r} feat={feat_now} "
                    f"n={len(evts)} ({ops_str}) expected_delta={exp_delta:+d}")
            if before is not None and fp in before:
                feat_before = before[fp]
                if feat_now < 0 or feat_before < 0:
                    # An unresolved layer or a provider that does not know
                    # its own count yields -1. Comparing it would invent a
                    # mismatch out of a missing measurement, and a warning
                    # the user cannot check is worse than silence: say the
                    # comparison could not be made.
                    line += (f" actual_delta=? [UNKNOWN] "
                             f"feat_before={feat_before}")
                else:
                    actual_delta = feat_now - feat_before
                    match = "OK" if actual_delta == exp_delta else "MISMATCH"
                    line += (f" actual_delta={actual_delta:+d} [{match}]")
                    if match == "MISMATCH":
                        missing = exp_delta - actual_delta
                        line += f" missing={missing:+d}"
                        mismatches.append({
                            "layer": layer_name,
                            "expected_delta": exp_delta,
                            "actual_delta": actual_delta,
                            "missing": missing,
                            "feat_before": feat_before,
                            "feat_after": feat_now,
                        })
            flog(line)
        flog(f"=== END REWIND_STATE [{label}] ===")
        if before is not None:
            self._state_mismatches = mismatches
        return counts

    def _format_state_mismatches(self, mismatches: list) -> str:
        """Name the layers left off the requested state, one per line.

        Empty string when every layer landed where it was asked to, so
        the caller can test the result directly.

        ``missing > 0`` means entities the plan expected to be there are
        absent; ``missing < 0`` means the layer carries more entities
        than the plan asked for. Both are stated as such: "4 entites
        manquantes" and "4 entites en trop" send the user to two very
        different places in his data.
        """
        if not mismatches:
            return ""
        lines = []
        for item in mismatches:
            missing = int(item["missing"])
            if missing > 0:
                gap = self.tr("{count} entite(s) manquante(s)").format(
                    count=missing)
            else:
                gap = self.tr("{count} entite(s) en trop").format(
                    count=-missing)
            lines.append(self.tr(
                "  - {layer} : {gap} (attendu {expected:+d}, mesure "
                "{actual:+d} ; {before} -> {after} entite(s))"
            ).format(layer=item["layer"], gap=gap,
                     expected=int(item["expected_delta"]),
                     actual=int(item["actual_delta"]),
                     before=int(item["feat_before"]),
                     after=int(item["feat_after"])))
        head = self.tr(
            "Etat non conforme : {count} couche(s) ne sont pas dans l'etat "
            "demande."
        ).format(count=len(mismatches))
        tail = self.tr(
            "Le nombre d'entites mesure sur la couche ne correspond pas au "
            "plan applique. Verifiez ces couches avant de continuer a "
            "travailler dessus."
        )
        return head + "\n" + "\n".join(lines) + "\n" + tail

    def _report_state_mismatches(self) -> bool:
        """Tell the user about the mismatches the last snapshot measured.

        Returns True when at least one layer was reported, so the caller
        can downgrade its own end-of-run message: a rewind that counted
        zero failed actions and still left a layer off its target is not
        a success, and this is the single most important thing the plugin
        can say after a rewind.

        Drains `_state_mismatches`: the same measurement must never be
        announced twice, and the next transition refills it.
        """
        mismatches = list(self._state_mismatches or [])
        self._state_mismatches = []
        if not mismatches:
            return False
        for item in mismatches:
            flog(f"STATE_MISMATCH layer={item['layer']!r} "
                 f"expected_delta={int(item['expected_delta']):+d} "
                 f"actual_delta={int(item['actual_delta']):+d} "
                 f"missing={int(item['missing']):+d} "
                 f"feat_before={item['feat_before']} "
                 f"feat_after={item['feat_after']}", "WARNING")
        qlog(self._format_state_mismatches(mismatches), "WARNING")
        return True

    def _partial_restore_report(self, runner) -> tuple:
        """RLU-053: ``(n_features, field_names)`` restored incompletely.

        Asks the runner that just finished how many features it wrote
        without every captured field, and which columns stayed behind.
        ``(0, ())`` for a runner that cannot answer (event restore, undo):
        those paths refuse a drifted event instead of writing a hybrid,
        so they have nothing to declare.
        """
        reader = getattr(runner, "partial_restore_report", None)
        if not callable(reader):
            return (0, ())
        try:
            count, fields = reader()
        except Exception as exc:  # noqa: BLE001 - a report never crashes
            flog(f"partial_restore_report: unreadable: {exc}", "WARNING")
            return (0, ())
        return (int(count or 0), tuple(fields or ()))

    def _runner_apply_report(self, runner):
        """RLU-055: the buckets the runner just published, or ``None``.

        Asked in the ``finished`` slot, before the reference is dropped,
        so the message the user reads carries the numbers of the
        CYCLE_SUMMARY line of the same run. ``None`` for a runner that
        classifies nothing per action (undo): inventing zeroed buckets
        for it would read as "no failure", which is exactly the lie this
        whole path is closing.
        """
        reader = getattr(runner, "apply_report", None)
        if not callable(reader):
            return None
        try:
            report = dict(reader())
        except Exception as exc:  # noqa: BLE001 - a report never crashes
            flog(f"apply_report: unreadable: {exc}", "WARNING")
            return None
        return report

    def _format_partial_restore(self, count: int, fields: tuple) -> str:
        """One line naming the fields a rewind could not put back.

        Empty string when the rewind was complete, so the caller can test
        it directly. The field names are the whole point: knowing that
        "3 entities are incomplete" without knowing WHICH columns leaves
        the user unable to repair anything by hand.
        """
        if count <= 0:
            return ""
        if fields:
            return "\n" + self.tr(
                "{count} entite(s) reecrite(s) sans tous leurs champs : "
                "{fields} n'ont pas ete restaures (le schema de la couche "
                "a change depuis la capture)."
            ).format(count=count, fields=", ".join(fields))
        return "\n" + self.tr(
            "{count} entite(s) reecrite(s) sans tous leurs champs captures "
            "(le schema de la couche a change depuis la capture)."
        ).format(count=count)

    def _format_restore_outcome(self, buckets: dict) -> str:
        """The end-of-run message: one bucket per outcome, totalling the plan.

        RLU-055. The buckets come from `_restore_buckets`, i.e. from the
        report the runner published, never from `total_ok`/`total_fail`:
        the run measured on 2026-08-12 planned 73 actions, brought 6
        entities back and was announced as "14 restauree(s), 59
        echouee(s)". Named one by one, the same run reads:

            73 action(s) planifiee(s) : 6 restauree(s), 3 deja a l'etat
            cible, 64 echouee(s) (59 sans cible, 5 refusee(s) pour
            derive de geometrie).

        A skip is not a failure and never joins the failure count: the
        entity is already in the state the user asked for. It is not a
        restoration either, so it stays out of "restauree(s)" as well -
        that is why the two numbers are printed side by side instead of
        being merged into one reassuring total.
        """
        applied = buckets["applied"]
        planned = buckets["planned"]
        if buckets["plan_known"] and planned and applied == planned:
            return self.tr(
                "{count} entite(s) restauree(s) avec succes."
            ).format(count=applied)

        parts = [self.tr("{count} restauree(s)").format(count=applied)]
        if buckets["applied_partial"]:
            parts.append(self.tr("{count} incomplete(s)").format(
                count=buckets["applied_partial"]))
        if buckets["skipped"]:
            parts.append(self.tr("{count} deja a l'etat cible").format(
                count=buckets["skipped"]))
        if buckets["failed_total"]:
            reasons = []
            if buckets["failed_absent"]:
                reasons.append(self.tr("{count} sans cible").format(
                    count=buckets["failed_absent"]))
            if buckets["failed_drift"]:
                reasons.append(
                    self.tr("{count} refusee(s) pour derive de geometrie")
                    .format(count=buckets["failed_drift"]))
            if buckets.get("failed_postcommit"):
                reasons.append(self.tr(
                    "{count} ecrite(s) mais contredite(s) par la relecture "
                    "du fichier").format(count=buckets["failed_postcommit"]))
            if buckets["failed_other"]:
                reasons.append(self.tr("{count} pour une autre raison").format(
                    count=buckets["failed_other"]))
            failed_text = self.tr("{count} echouee(s)").format(
                count=buckets["failed_total"])
            if reasons:
                failed_text += " (" + ", ".join(reasons) + ")"
            parts.append(failed_text)
        if buckets["cancelled"]:
            parts.append(self.tr("{count} annulee(s) avant tentative").format(
                count=buckets["cancelled"]))

        details = ", ".join(parts)
        if buckets["plan_known"]:
            return self.tr("{planned} action(s) planifiee(s) : {details}.").format(
                planned=planned, details=details)
        # No plan base available (the runner was released before the
        # report could be read): state what was accounted for, and do not
        # pass that sum off as the size of the plan.
        return self.tr("{total} action(s) comptabilisee(s) : {details}.").format(
            total=planned, details=details)

    def _build_restore_summary(self, by_ds: dict) -> str:
        """Build per-layer operation breakdown from by_ds event groups."""
        if not by_ds:
            return ""
        parts = []
        for _fp, events in by_ds.items():
            if not events:
                continue
            name = events[0].layer_name_snapshot or "?"
            counts = {}
            for e in events:
                counts[e.operation_type] = counts.get(e.operation_type, 0) + 1
            tokens = []
            if counts.get("DELETE"):
                tokens.append(f"{counts['DELETE']} suppression(s)")
            if counts.get("UPDATE"):
                tokens.append(f"{counts['UPDATE']} modification(s)")
            if counts.get("INSERT"):
                tokens.append(f"{counts['INSERT']} insertion(s)")
            if tokens:
                parts.append(f"{name} : {', '.join(tokens)}")
        if not parts:
            return ""
        return "\n" + "\n".join(parts)

    def _on_search_complete(self, result) -> None:
        """Handle search results from local SQLite journal.

        Enforces a minimum animation duration so the logo effect
        always completes at least 2 full sweep cycles before the
        results are displayed.
        """
        elapsed = time.monotonic() - self._recover_started_at
        remaining = _MIN_RECOVER_ANIMATION_SEC - elapsed
        if remaining > 0:
            self._pending_search_result = result
            QTimer.singleShot(int(remaining * 1000), self._display_deferred_result)
            return
        self._display_search_result(result)

    def _flush_deferred(self, attr_name, display_fn) -> None:
        """Generic deferred-display: read pending, reset, call display."""
        data = getattr(self, attr_name)
        setattr(self, attr_name, None)
        if data is None:
            return
        display_fn(data)

    def _display_deferred_result(self) -> None:
        """Called by the deferred timer after the minimum animation elapsed."""
        self._flush_deferred("_pending_search_result", self._display_search_result)

    def _display_search_result(self, result) -> None:
        """Finalize UI after search results are ready and animation is done."""
        trace_id = self._active_search_trace_id
        self.progress_timer.stop()
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.progress_phase_label.setVisible(False)
        self.cancel_button.setText(self.tr("Fermer"))
        self.cancel_button.setIcon(theme_icon("close", size=16))
        if hasattr(self.cancel_button, "setIconName"):
            self.cancel_button.setIconName("close")
        self.enable_controls(True)

        flog(f"_display_search_result: total_count={result.total_count} events={len(result.events)}")
        if trace_id:
            started = self._recover_started_at
            elapsed_ms = int((time.monotonic() - started) * 1000) if started else 0
            flog(
                f"[{trace_id}] recover_event: done"
                f" total={result.total_count}"
                f" returned={len(result.events)}"
                f" elapsed_ms={elapsed_ms}"
            )
            self._active_search_trace_id = ""
        if result.total_count == 0:
            suggestion = self._build_empty_result_suggestion()
            qlog(suggestion)
            self._refresh_smart_bar()
            return

        self._search_events = result.events
        self._populate_results_table(result.events, result.total_count)
        self._smart_bar_message_override = self.tr(
            "{count} événement(s) chargé(s) dans la table.").format(count=result.total_count)
        self._refresh_smart_bar()

    def _display_deferred_restore_feedback(self) -> None:
        self._flush_deferred("_pending_restore_feedback", self._display_restore_feedback)

    def _display_restore_feedback(self, feedback) -> None:
        """RLU-055: report an event restore by its buckets, like the log."""
        buckets, errors = feedback
        self._end_progress_ui()
        self._stop_logo_activity()
        self.restore_button.setEnabled(bool(self.selected_rows))
        self.recover_button.setEnabled(True)

        written = buckets["applied"] + buckets["applied_partial"]
        failed = buckets["failed_total"]
        if buckets["planned"] == 0 and errors:
            # Nothing was even attempted: the errors are the whole story.
            qlog("Restauration: " + " | ".join(errors[:5]), "ERROR")
        else:
            msg = self._format_restore_outcome(buckets)
            if errors:
                msg += " | " + " | ".join(errors[:5])
            if written == 0 and (failed or errors):
                # Nothing came back: severity of the previous message,
                # kept on a count the user can act on.
                level = "ERROR"
            elif failed or errors or buckets["applied_partial"] \
                    or buckets["cancelled"]:
                level = "WARNING"
            else:
                level = "INFO"
            qlog(msg, level)
        if written > 0:
            self.iface.mapCanvas().refreshAllLayers()

    def _populate_results_table(self, events, total: int) -> None:
        """Fill the results table from AuditEvent objects."""
        attr_keys = []
        seen = set()
        for event in events:
            for k in reconstruct_attributes(event):
                if k not in seen:
                    seen.add(k)
                    attr_keys.append(k)
        self._all_attr_keys = attr_keys

        has_geom_change = any(is_geometry_only_update(e) or e.geometry_wkb is not None for e in events)
        fixed_cols = ["#", self.tr("Date"), self.tr("Utilisateur"), self.tr("Opération"), self.tr("Couche")]
        if has_geom_change:
            fixed_cols.append(self.tr("Géométrie"))
        columns = fixed_cols + attr_keys
        n_fixed = len(fixed_cols)
        self.table_widget.setSortingEnabled(False)
        self.table_widget.setRowCount(len(events))
        self.table_widget.setColumnCount(len(columns))
        self.table_widget.setHorizontalHeaderLabels(columns)
        self._modified_col_indices = set()

        for row_idx, event in enumerate(events):
            is_update = event.operation_type == "UPDATE"
            parsed_data = None
            if event.attributes_json:
                try:
                    parsed_data = json.loads(event.attributes_json)
                except (json.JSONDecodeError, TypeError):
                    pass

            attrs = {}
            changed_keys = set()
            if parsed_data is not None:
                if "all_attributes" in parsed_data:
                    attrs = parsed_data["all_attributes"]
                elif "changed_only" in parsed_data:
                    changed_keys = set(parsed_data["changed_only"].keys())
                    for k, v in parsed_data["changed_only"].items():
                        if isinstance(v, dict) and "old" in v:
                            attrs[k] = v["old"]
                        else:
                            attrs[k] = v
                else:
                    attrs = parsed_data

            date_str = format_relative_time(event.created_at or "")
            op_label = event.operation_type or ""
            if event.restored_from_event_id is not None:
                op_label = f"{op_label} [Restaure]"
            row_values = [
                str(event.event_id or ""), date_str,
                event.user_name or "", op_label,
                event.layer_name_snapshot or "",
            ]
            geom_changed = False
            if has_geom_change:
                # Not all((...)): the tuple is built before all() reads it, so
                # changed_only was read on INSERT/DELETE rows (all_attributes)
                # and raised KeyError. The helper checks each guard in turn.
                is_geom_only = is_geometry_only_update(event)
                geom_changed = is_geom_only or (is_update and event.geometry_wkb is not None)
                row_values.append(self.tr("Modifiée") if geom_changed else "")
            diff_tooltip = self._build_event_diff_tooltip(
                event, parsed_data, attrs, changed_keys, geom_changed)
            for col_idx, val in enumerate(row_values):
                item = QTableWidgetItem(val)
                if col_idx == 0:
                    item.setData(QtCompat.USER_ROLE, row_idx)
                if has_geom_change and col_idx == len(row_values) - 1 and geom_changed:
                    item.setBackground(CHANGE_TYPE_COLORS["geometry"])
                if col_idx == 3 and diff_tooltip:
                    item.setToolTip(diff_tooltip)
                self.table_widget.setItem(row_idx, col_idx, item)

            op = event.operation_type or ""
            for col_offset, key in enumerate(attr_keys):
                val = attrs.get(key)
                item = QTableWidgetItem(str(val) if val is not None else "")
                col_idx = n_fixed + col_offset
                has_value = val is not None and val != ""
                if is_update and key in changed_keys:
                    item.setBackground(CHANGE_TYPE_COLORS["modified"])
                    self._modified_col_indices.add(col_idx)
                    if parsed_data is not None:
                        change = parsed_data.get("changed_only", {}).get(key, {})
                        if isinstance(change, dict):
                            item.setToolTip(self.tr("Ancien: {old}\nActuel: {new}").format(
                                old=val, new=change.get('new')))
                elif op in ("INSERT", "DELETE") and has_value:
                    # Per user feedback (2026-05-16): no per-cell coloring for
                    # INSERT/DELETE -- the whole row is new/gone, color coding
                    # adds noise rather than information. Tracking is kept so
                    # the "modifications only" column filter still surfaces
                    # the active columns.
                    self._modified_col_indices.add(col_idx)
                self.table_widget.setItem(row_idx, col_idx, item)

        self.table_widget.setSortingEnabled(True)
        self.table_widget.resizeColumnsToContents()
        self.results_info_label.setText(
            self.tr("{count} événement(s) trouvé(s), sélectionnez les lignes à restaurer").format(count=total)
        )
        self.results_group.setTitle(self.tr("Résultats ({count} événements)").format(count=total))
        self.results_group.setVisible(True)
        self.results_group.setCollapsed(False)
        self.select_all_button.setEnabled(True)
        self.select_none_button.setEnabled(True)
        self.search_filter.setVisible(True)
        self.search_filter.clear()
        has_any_highlight = bool(self._modified_col_indices) or has_geom_change
        self.change_legend.setVisible(has_any_highlight)
        saved_mod_only = QgsSettings().value(
            "RecoverLand/modified_columns_only", False, type=bool)
        if saved_mod_only and has_any_highlight:
            self._modified_only_check.setChecked(True)

    def on_error(self, error_message):
        """Error handler with exploitable messages (UX-H01)."""
        trace_id = self._active_restore_trace_id or self._active_search_trace_id
        self.progress_timer.stop()
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.progress_phase_label.setVisible(False)
        self.cancel_button.setText(self.tr("Fermer"))
        self.cancel_button.setIcon(theme_icon("close", size=16))
        if hasattr(self.cancel_button, "setIconName"):
            self.cancel_button.setIconName("close")
        self.enable_controls(True)
        if trace_id:
            flog(f"[{trace_id}] recover_dialog: error={error_message}", "ERROR")
            self._active_search_trace_id = ""
            self._active_restore_trace_id = ""
        flog(f"Erreur de recuperation: {error_message}", "ERROR")
        user_msg = self._humanize_error(
            self.tr("Impossible de recuperer les donnees"),
            error_message)
        qlog(user_msg, "ERROR")

    def cancel_operation(self):
        """Cancel op: stop running thread, pending result, or close dialog."""
        if self._pending_search_result is not None:
            self._pending_search_result = None
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(theme_icon("close", size=16))
            if hasattr(self.cancel_button, "setIconName"):
                self.cancel_button.setIconName("close")
            self.enable_controls(True)
            self._active_search_trace_id = ""
            qlog(self.tr("Recuperation annulee."))
            return
        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.stop()
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(theme_icon("close", size=16))
            if hasattr(self.cancel_button, "setIconName"):
                self.cancel_button.setIconName("close")
            self.enable_controls(True)
            self._active_search_trace_id = ""
            qlog(self.tr("Recuperation annulee."))
        elif self._version_fetch_thread and self._version_fetch_thread.isRunning():
            self._version_fetch_thread.stop()
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(theme_icon("close", size=16))
            if hasattr(self.cancel_button, "setIconName"):
                self.cancel_button.setIconName("close")
            self.enable_controls(True)
            self._active_restore_trace_id = ""
            qlog(self.tr("Recuperation annulee."))
        elif self._restore_runner is not None:
            written_fn = getattr(self._restore_runner, "written_count", None)
            applied = (
                written_fn() if callable(written_fn)
                else getattr(self._restore_runner, "_applied", 0)
            )
            is_strict = isinstance(self._restore_runner, StrictRestoreRunner)
            is_undo = isinstance(self._restore_runner, UndoRunner)
            self._restore_runner.cancel()
            # RLU-055: cancel() only asks; the runner still walks to its
            # own _finish and emits `finished`. Keeping it reachable is
            # what lets the final message count the actions the user
            # cancelled as cancelled, and not as failures.
            self._cancelled_restore_runner = self._restore_runner
            self._restore_runner = None
            self._restore_in_progress = False
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(theme_icon("close", size=16))
            if hasattr(self.cancel_button, "setIconName"):
                self.cancel_button.setIconName("close")
            self.enable_controls(True)
            trace_id = self._active_restore_trace_id
            self._active_restore_trace_id = ""
            # The undo state is NOT touched here. It used to be set to the
            # runner's whole input (`_by_ds`), i.e. also the events it had
            # not reached yet and would never reach: a click on Undo in
            # that window applied their inverse. The runner still walks to
            # its own `finished`, whose slot stores what it really wrote.
            if is_undo:
                qlog(self.tr(
                    "Annulation interrompue a votre demande ; le detail de ce "
                    "qui a ete remis suit a la fin de l'etape en cours."),
                    "WARNING")
            elif applied > 0:
                if is_strict:
                    msg = self.tr(
                        "Rewind interrompu. {count} entite(s) restaurees sur "
                        "les couches deja terminees ; couche en cours annulee. "
                        "Annulation du restore possible."
                    ).format(count=applied)
                else:
                    msg = self.tr(
                        "Restauration interrompue, {count} operation(s) "
                        "deja appliquee(s). Annulation possible."
                    ).format(count=applied)
                qlog(msg, "WARNING")
            else:
                qlog(self.tr("Restauration annulee, aucune modification appliquee."))
            flog(f"[{trace_id}] cancel_operation: applied={applied} strict={is_strict}")
        else:
            self.reject()

    def enable_controls(self, enabled=True):
        """Toggle controls"""
        if enabled:
            self._is_recovering = False
        self.recover_button.setEnabled(enabled)
        self.layer_input.setEnabled(enabled)
        self.operation_input.setEnabled(enabled)
        self.start_input.setEnabled(enabled)
        self.end_input.setEnabled(enabled)
        if enabled:
            # Every end of a chain comes through here: the Undo button and
            # the mode selector follow the state, unless a run still holds
            # the layers (a cancelled one walking to its end included).
            self._sync_run_controls()

    def _resolve_event_indices(self) -> list:
        """Map selected visual rows to original _search_events indices via UserRole."""
        indices = []
        for index in self.table_widget.selectionModel().selectedRows():
            item = self.table_widget.item(index.row(), 0)
            if item is None:
                continue
            event_idx = item.data(QtCompat.USER_ROLE)
            if event_idx is not None:
                indices.append(event_idx)
        return indices

    def on_selection_changed(self):
        """Selection change"""
        if self.sync_in_progress:
            return
        self.selected_rows = self._resolve_event_indices()
        total = self.table_widget.rowCount()
        selected = len(self.selected_rows)
        label = (
            self.tr("{selected} / {total} sélectionnées")
            .format(selected=selected, total=total)
            if selected > 0 else ""
        )
        self.selection_count_label.setText(label)
        self.restore_button.setEnabled(selected > 0)
        self._update_geometry_preview()

    def _update_geometry_preview(self) -> None:
        """P1.1: Show old geometry on canvas when exactly one geometry event is selected.

        Every exit path logs its reason so a missing zoom/flash can be
        diagnosed from the trace (user rule: log every critical branch).
        """
        n_rows = len(self.selected_rows)
        if n_rows == 0:
            self._geom_preview.clear()
            flog("_update_geometry_preview: cleared reason=no_selection",
                 "DEBUG")
            return
        if n_rows != 1:
            self._geom_preview.clear()
            flog(
                f"_update_geometry_preview: cleared reason=multi_selection "
                f"n_rows={n_rows}",
                "DEBUG",
            )
            return
        idx = self.selected_rows[0]
        if idx >= len(self._search_events):
            self._geom_preview.clear()
            flog(
                f"_update_geometry_preview: cleared reason=idx_oob "
                f"idx={idx} n_events={len(self._search_events)}",
                "WARNING",
            )
            return
        event = self._search_events[idx]
        if not event.geometry_wkb:
            self._geom_preview.clear()
            flog(
                f"_update_geometry_preview: cleared reason=no_geometry_wkb "
                f"event_id={event.event_id} op={event.operation_type}",
                "INFO",
            )
            return
        wkb = event.geometry_wkb
        # The event's other captured geometry: with the displayed one, the
        # witnesses the Rewind places it with (`restore_executor
        # ._geometry_in_layer_crs`). The table rows are lightweight: both
        # columns come as a 1-byte marker and are read back together.
        new_wkb = getattr(event, "new_geometry_wkb", None)
        wkb_source = "lightweight"
        if _BLOB_MARKER in (wkb, new_wkb) and event.event_id is not None:
            conn = self._get_dialog_read_conn()
            if conn is not None:
                full = get_event_by_id(conn, event.event_id)
                if full is not None:
                    wkb = full.geometry_wkb
                    new_wkb = full.new_geometry_wkb
                    wkb_source = "full_fetch"
        if not wkb or wkb == _BLOB_MARKER:
            self._geom_preview.clear()
            flog(
                f"_update_geometry_preview: cleared reason=blob_marker_unresolved "
                f"event_id={event.event_id}",
                "WARNING",
            )
            return
        # Without the witnesses the display cannot tell a relabel from a
        # reprojection when the layer CRS changed, and refused a geometry
        # the Rewind puts back. A witness left as the marker is not one
        # (a 1-byte non-geometry): none is given then, and the refusal says
        # the witnesses were not provided rather than that they disagree.
        witnesses = (wkb, new_wkb)
        if new_wkb == _BLOB_MARKER:
            witnesses = ()
            flog(
                f"_update_geometry_preview: witness_unresolved "
                f"event_id={event.event_id} (new geometry not read back; "
                f"the verdict gets the displayed geometry alone)",
                "WARNING",
            )
        target_layer = None
        try:
            target_layer = find_target_layer(event, self._get_dialog_read_conn())
        except Exception as exc:
            flog(
                f"_update_geometry_preview: target_layer_resolve_failed "
                f"event_id={event.event_id} err={exc}",
                "WARNING",
            )
        rendered = self._geom_preview.show(wkb, event.crs_authid, target_layer,
                                           witnesses=witnesses)
        if not rendered:
            flog(
                f"_update_geometry_preview: show_returned_false "
                f"event_id={event.event_id} crs={event.crs_authid} "
                f"wkb_source={wkb_source} witnesses={len(witnesses)}",
                "WARNING",
            )
            return
        zoom_enabled = self.auto_zoom_check.isChecked()
        flog(
            f"_update_geometry_preview: rendered "
            f"event_id={event.event_id} op={event.operation_type} "
            f"crs={event.crs_authid or '-'} wkb_source={wkb_source} "
            f"zoom_enabled={zoom_enabled}",
            "INFO",
        )
        if zoom_enabled:
            self._geom_preview.zoom_to_preview()
        self._geom_preview.flash()

    def select_all_rows(self):
        """Select all"""
        self.table_widget.selectAll()

    def select_none_rows(self):
        """Select none"""
        self.table_widget.clearSelection()

    def restore_selected_data(self):
        """Restore selected audit events to their source QGIS layers."""
        # Refused by name while anything else holds the layers: this used
        # to return in silence on `_restore_in_progress` alone.
        if self._refuse_while_busy(self.tr("Restauration")):
            return
        if not self.selected_rows:
            qlog(self.tr("Selectionnez au moins une ligne a restaurer."), "WARNING")
            return

        lightweight_events = [
            self._search_events[r]
            for r in self.selected_rows
            if r < len(self._search_events)
        ]
        if not lightweight_events:
            return

        ds_fps = {e.datasource_fingerprint for e in lightweight_events}
        # Before the plan and the preflight: the journal must hold every
        # edit already made on these layers.
        if not self._journal_barrier(self.tr("Restauration"), ds_fps):
            return
        ds_fp = next(iter(ds_fps)) if len(ds_fps) == 1 else "mixed"
        layer_names = sorted({e.layer_name_snapshot or "?" for e in lightweight_events})
        layer_label = ", ".join(layer_names)

        plan = plan_event_restore(lightweight_events, ds_fp, layer_label)
        # With the journal's read connection: the purge horizon the journal
        # persisted is the durable one (restore_planner._purged_horizon_reason);
        # without it only this process's memory of its own purges was read.
        report = preflight_check(plan, conn=self._get_dialog_read_conn())

        layer_warnings, layer_blocking = self._preflight_layer_checks(lightweight_events)
        merged_warnings = list(report.warnings) + layer_warnings
        merged_blocking = list(report.blocking_reasons) + layer_blocking
        if merged_blocking:
            from .core.restore_contracts import PreflightVerdict, PreflightReport
            report = PreflightReport(
                verdict=PreflightVerdict.BLOCKED,
                plan=report.plan,
                blocking_reasons=merged_blocking,
                warnings=merged_warnings,
                estimated_duration_ms=report.estimated_duration_ms,
            )
        elif layer_warnings and report.verdict.value == "go":
            from .core.restore_contracts import PreflightVerdict, PreflightReport
            report = PreflightReport(
                verdict=PreflightVerdict.GO_WITH_WARNINGS,
                plan=report.plan,
                blocking_reasons=merged_blocking,
                warnings=merged_warnings,
                estimated_duration_ms=report.estimated_duration_ms,
            )

        verdict_key = report.verdict.value
        summary_text = format_plan_summary(plan)
        detail_text = format_preflight_report(report)
        is_blocked = verdict_key == "blocked"

        dlg = RestorePreflightDialog(self)
        dlg.set_preflight_data(verdict_key, summary_text, detail_text, is_blocked)
        _accepted = getattr(getattr(QDialog, 'DialogCode', None), 'Accepted', None) or getattr(QDialog, 'Accepted')
        if dlg.exec() != _accepted:
            return

        trace_id = generate_trace_id()
        self._active_restore_trace_id = trace_id
        self.restore_button.setEnabled(False)
        self.recover_button.setEnabled(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._restore_started_at = time.monotonic()
        self._pending_restore_feedback = None
        self._start_logo_activity(self._glow_color, "restore")
        self._restore_in_progress = True

        # From here until the runner owns the run, a failure must give the
        # dialog back: see `_abort_event_restore_setup`.
        try:
            # The runner gets the events re-read in full from the journal,
            # ALL of them or none. The table rows are lightweight (their
            # geometry is a 1-byte marker): without a read connection they
            # were handed to the runner as they are, and refused there as
            # "Geometry unreadable" or "could not be verified", a false
            # reason. An event purged since the search (the maintenance
            # does not refresh the table) was dropped in silence: the run
            # executed fewer events than the preflight the user confirmed,
            # reported them restored, and never named the others.
            import sqlite3
            read_conn = self._get_dialog_read_conn()
            if read_conn is None:
                raise sqlite3.OperationalError(
                    "no read connection on the journal")
            event_ids = [e.event_id for e in lightweight_events]
            if any(eid is None for eid in event_ids):
                raise ValueError("a selected row has no journal event id")
            # EventsNotInJournal names every missing id; the whole run is
            # refused with them (`_abort_event_restore_setup`).
            selected_events = fetch_events_by_ids(read_conn, event_ids,
                                                  require_all=True)
            self._event_restore_events = selected_events
            flog(f"[{trace_id}] restore_event: start selected={len(selected_events)} datasources={len(ds_fps)}")

            def resolver(evt):
                return find_target_layer(evt, read_conn)

            runner = RestoreRunner(selected_events, resolver, self._write_queue,
                                   tracker=self._tracker, trace_id=trace_id,
                                   parent=self,
                                   journal_path=self._run_journal_path())
            runner.progress.connect(self._on_restore_runner_progress)
            runner.finished.connect(self._on_event_restore_done)
        except Exception as exc:  # noqa: BLE001 - named, dialog given back
            self._abort_event_restore_setup(trace_id, exc,
                                            selected=lightweight_events)
            return
        self._launch_runner(runner)

    def _abort_event_restore_setup(self, trace_id: str,
                                   exc: BaseException,
                                   selected=()) -> None:
        """A restore that failed between its confirmation and its runner.

        The dialog has already put itself in "restore in progress" (flag,
        buttons, bar, logo) when it reads the selected events back from the
        journal. That read raised on a journal a writer holds past the busy
        timeout ("database is locked", a journal on a network share): the
        exception went out of the slot to Qt, no runner was made, no message
        was shown, and `_restore_in_progress` stayed set, so every later
        action was refused "une restauration est en cours" until the project
        changed. Nothing has been written at this point: everything the
        restore took is given back and the reason is named.

        `EventsNotInJournal`: selected events the journal no longer holds.
        Each one is named, from the row the user chose (*selected*).
        """
        import sqlite3
        from .core.event_stream_repository import EventsNotInJournal
        flog(f"[{trace_id}] restore_event: setup failed before any write "
             f"({type(exc).__name__}: {exc}); dialog released", "ERROR")
        self._restore_in_progress = False
        self._active_restore_trace_id = ""
        self._event_restore_events = None
        self._stop_logo_activity()
        self._end_progress_ui()
        self.restore_button.setEnabled(bool(self.selected_rows))
        self.recover_button.setEnabled(True)
        self._sync_run_controls()
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, EventsNotInJournal):
            # Named as the table shows them: its id column, its date.
            rows = {e.event_id: e for e in selected or ()}
            named = []
            for eid in exc.missing_ids:
                row = rows.get(eid)
                if row is None:
                    named.append(f"#{eid}")
                    continue
                named.append(self.tr(
                    "#{eid} ({op} sur '{layer}', {date})"
                ).format(eid=eid, op=row.operation_type or "?",
                         layer=row.layer_name_snapshot or "?",
                         date=format_relative_time(row.created_at or "")
                         or "?"))
            qlog(self.tr(
                "Restauration non lancee : {n} des {total} evenement(s) "
                "choisi(s) ne sont plus dans le journal (purges depuis la "
                "recherche, par exemple par la maintenance du journal) : "
                "{events}. Restaurer les autres seuls ne serait pas la "
                "restauration confirmee et peut laisser une entite dans un "
                "etat intermediaire. Rien n'a ete ecrit. Relancez la "
                "recherche pour afficher l'historique restant, puis "
                "choisissez a nouveau."
            ).format(n=len(exc.missing_ids), total=exc.asked,
                     events="; ".join(named)), "ERROR")
        elif isinstance(exc, sqlite3.Error):
            qlog(self.tr(
                "Restauration non lancee : les evenements choisis n'ont pas "
                "pu etre relus dans le journal ({error}). Rien n'a ete ecrit. "
                "Le journal est peut-etre tenu par un autre programme ou une "
                "autre instance de QGIS : recommencez quand il est de nouveau "
                "lisible."
            ).format(error=error), "ERROR")
        else:
            qlog(self.tr(
                "Restauration non lancee : sa preparation a echoue ({error}). "
                "Rien n'a ete ecrit."
            ).format(error=error), "ERROR")

    def _on_event_restore_done(self, result) -> None:
        trace_id = self._active_restore_trace_id
        self._restore_in_progress = False
        # RLU-055: read the buckets before the reference goes, exactly
        # like the rewind path does.
        runner = self._restore_runner or self._cancelled_restore_runner
        apply_report = self._runner_apply_report(runner)
        self._restore_runner = None
        self._cancelled_restore_runner = None
        cleanup_temp_layers()
        self._smooth_set_progress(100)
        if trace_id:
            started = self._restore_started_at
            elapsed_ms = int((time.monotonic() - started) * 1000) if started else 0
            flog(
                f"[{trace_id}] restore_event: done"
                f" ok={result.total_ok} fail={result.total_fail}"
                f" elapsed_ms={elapsed_ms}"
            )
            self._active_restore_trace_id = ""

        events = getattr(self, '_event_restore_events', None)
        # result.by_ds is what the runner WROTE (skips, refusals and never
        # attempted events excluded), not what was selected: `total_ok`
        # counts the skips too, so it cannot gate the undo scope. Merged
        # into the pool, in the order the runner applied it (its input
        # order, layer by layer); the button points at this run alone.
        if result.by_ds and events:
            written = self._without_layers_edited_during_run(result.by_ds)
            self._record_last_run(written)
            self._set_undo_state(self._merge_undo_scopes(
                self._last_restore_by_ds, written))
        self._event_restore_events = None
        # The chain ends here: the Undo button and the mode selector the
        # runner held are given back (the other controls come back with
        # the deferred feedback).
        self._sync_run_controls()

        if result.total_ok > 0:
            self._open_attribute_tables_if_requested(result.by_ds, "event")

        # RLU-055: the deferred feedback carries the buckets, not the two
        # totals it used to carry: the message built from `total_ok` /
        # `total_fail` announced entities that never came back and hid
        # part of the failures (see `_restore_buckets`).
        buckets = _restore_buckets(result, report=apply_report)
        feedback = (buckets, tuple(result.errors))
        elapsed = time.monotonic() - self._restore_started_at
        remaining = _MIN_RESTORE_ANIMATION_SEC - elapsed
        if remaining > 0:
            self._pending_restore_feedback = feedback
            QTimer.singleShot(int(remaining * 1000), self._display_deferred_restore_feedback)
            return
        self._display_restore_feedback(feedback)

    def _open_attribute_tables_if_requested(self, by_ds: dict, source: str) -> None:
        """US-FIX-02 (DC-2): Open attribute table per restored layer if checked.

        Reads `open_attribute_check` state. For each unique datasource in by_ds,
        resolves the actual QgsVectorLayer via `find_target_layer` and asks QGIS
        to display its attribute table. Failures are logged but never raised.
        """
        if not by_ds:
            return
        if not self.open_attribute_check.isChecked():
            return
        read_conn = self._get_dialog_read_conn()
        opened = 0
        for fp, evts in by_ds.items():
            if not evts:
                continue
            try:
                layer = find_target_layer(evts[0], read_conn)
            except Exception as exc:
                flog(
                    f"open_attribute_table: resolver_error source={source} "
                    f"fp={fp[:16]}... err={exc}",
                    "WARNING",
                )
                continue
            if layer is None:
                flog(
                    f"open_attribute_table: layer_not_found source={source} "
                    f"fp={fp[:16]}...",
                    "DEBUG",
                )
                continue
            try:
                self.iface.showAttributeTable(layer)
                opened += 1
                flog(
                    f"open_attribute_table: opened source={source} "
                    f"layer_id={layer.id()} name={layer.name()!r}",
                    "DEBUG",
                )
            except Exception as exc:
                flog(
                    f"open_attribute_table: failed source={source} "
                    f"layer={layer.name()!r} err={exc}",
                    "WARNING",
                )
        if opened > 0:
            flog(
                f"open_attribute_table: total_opened={opened} source={source}"
            )

    def _invalidate_undo_for(self, edited_fingerprint: str) -> None:
        """Remove a single layer from the undo state after user edits it.

        The layer is matched through the journal's alias scope, not by the
        letter of its fingerprint: a compensation filed under a fingerprint
        the journal declares equivalent (v6 alias of a re-pointed layer,
        obsolete pre-v6 form) belongs to the same source, and left in the
        pool it kept the button armed to write the restore's NEW values
        over the user's edit.
        """
        if not edited_fingerprint:
            return
        in_flight = bool(self._run_in_flight())
        if not in_flight and not self._last_restore_by_ds:
            return
        scope = self._expand_scope([edited_fingerprint])
        if in_flight:
            # Captured because the run no longer holds that layer (the
            # capture is given back layer by layer): what the run wrote
            # there must not become undoable at its end either
            # (`_without_layers_edited_during_run`).
            self._edited_during_run = (
                set(getattr(self, "_edited_during_run", None) or ()) | scope)
        if not self._last_restore_by_ds:
            return
        hit = [fp for fp in self._last_restore_by_ds if fp in scope]
        if not hit:
            return
        name = self._last_restore_by_ds[hit[0]][0].layer_name_snapshot
        dropped = {e.event_id for fp in hit
                   for e in self._last_restore_by_ds[fp]}
        for fp in hit:
            del self._last_restore_by_ds[fp]
        # Out of the last run too, for good: nothing may re-arm the button
        # on a layer the user has edited since.
        self._last_run_ids = (
            (getattr(self, "_last_run_ids", None) or set()) - dropped)
        self._last_restore_events = [
            e for e in (self._last_restore_events or [])
            if e.datasource_fingerprint not in hit
        ]
        flog(f"undo: '{name}' invalidated (edited, fingerprints={len(hit)}), "
             f"{len(self._last_restore_by_ds)} layer(s) still undo-able")
        if not self._last_restore_by_ds:
            self._last_restore_events = None
        if not self._manual_undo_scope():
            # Never enabled here: a run in flight owns the button.
            self.undo_last_btn.setEnabled(False)
            flog("undo_last_btn: DISABLED (last run's layers edited)")

    def _append_undo_session_menu(self, menu, event) -> None:
        """US-4.10.01: Append an 'undo entire session' entry to the menu.

        If the event has a session_id, show the count and wire _undo_session.
        If session_id is NULL (legacy pre-v5 event), show a disabled entry
        with an explanatory tooltip (A6 of US-01).
        """
        session_id = event.session_id
        menu.addSeparator()
        if not session_id:
            legacy_act = menu.addAction(
                self.tr("Annuler toute la session (indisponible)"))
            legacy_act.setEnabled(False)
            legacy_act.setToolTip(self.tr(
                "Evenement legacy, groupement de session indisponible."))
            flog(
                f"undo_session_menu: skipped reason=legacy_null_session "
                f"event_id={event.event_id}",
                "DEBUG",
            )
            return
        n_events = self._count_session_events(session_id)
        if n_events == 0:
            return
        session_date = (event.created_at or "")[:16] or "?"
        undo_act = menu.addAction(
            theme_icon("undo", size=16),
            self.tr("Annuler toute la session d'edition ({n} evenements)").format(
                n=n_events))
        undo_act.setToolTip(self.tr("Session du {date}").format(date=session_date))
        undo_act.triggered.connect(lambda: self._undo_session(session_id))

    def _service_history_state(self, fingerprint: str) -> tuple:
        """``(candidate, manual_target)`` of a service-less history, or Nones.

        *candidate*: the history the reconciliation refused to attach
        (`datasource_alias.service_history_candidates`), with the services
        known for its provider. *manual_target*: the fingerprint the user
        attached it to through `_attach_service_history`, which he can
        take back. Both None when neither applies or the journal cannot
        be read.
        """
        conn = self._get_dialog_read_conn()
        if not fingerprint or conn is None:
            return None, None
        try:
            from .core import datasource_alias as da
            candidate = next((c for c in da.service_history_candidates(conn)
                              if c.fingerprint == fingerprint), None)
            note = getattr(da, "_NOTE_MANUAL_SERVICE",
                           "rattachement manuel : service ")
            manual = next((target for alias, target, _at, text
                           in da.list_aliases(conn)
                           if alias == fingerprint
                           and (text or "").startswith(note)), None)
        except Exception as exc:  # noqa: BLE001 - a menu never blocks
            flog(f"service_history_menu: journal unreadable ({exc})", "WARNING")
            return None, None
        return candidate, manual

    def _append_service_history_menu(self, menu, event) -> None:
        """Attach a refused service-less history to a service, or take it back.

        The reconciliation refuses to attach a pre-v7 database history
        whose connection service it cannot tell (several services known):
        the history stayed out of every rewind, the layer that reads it was
        refused as a target, and nothing let the user say which service
        the old events came from. This is that way, on an event of the
        history, and its undo.
        """
        candidate, manual = self._service_history_state(
            event.datasource_fingerprint or "")
        if candidate is None and manual is None:
            return
        menu.addSeparator()
        fingerprint = event.datasource_fingerprint
        if candidate is not None:
            attach_act = menu.addAction(
                self.tr("Rattacher cet historique a un service..."))
            attach_act.setToolTip(self.tr(
                "Services connus : {known}").format(
                    known=", ".join(candidate.known_services) or "-"))
            attach_act.triggered.connect(
                lambda: self._attach_service_history(fingerprint))
        if manual is not None:
            detach_act = menu.addAction(
                self.tr("Retirer le rattachement de cet historique au service"))
            detach_act.triggered.connect(
                lambda: self._detach_service_history(fingerprint))

    def _attach_service_history(self, fingerprint: str) -> bool:
        """Ask which service a refused history comes from, then attach it.

        `datasource_alias.attach_service_history` does the attaching and
        refuses what it cannot prove (another table, a scope that does not
        hold the events); its answer is shown as is. Returns True when the
        history was attached.
        """
        from qgis.PyQt.QtWidgets import QInputDialog
        if self._refuse_while_busy(self.tr("Rattachement d'historique")):
            return False
        candidate, _manual = self._service_history_state(fingerprint)
        if candidate is None:
            qlog(self.tr("Cet historique n'est plus a rattacher."), "WARNING")
            return False
        label = ", ".join(candidate.layer_names) or fingerprint[-40:]
        services = list(candidate.known_services)
        if not services:
            qlog(self.tr(
                "Aucun service de connexion connu pour '{layers}' : "
                "impossible de rattacher son historique."
            ).format(layers=label), "WARNING")
            return False
        service, ok = QInputDialog.getItem(
            self, self.tr("Rattacher un historique"),
            self.tr(
                "De quel service de connexion viennent les evenements "
                "d'avant v7 de '{layers}' ?\n({detail})\n\nSi vous ne le "
                "savez pas, annulez : un historique rattache a la mauvaise "
                "base ferait rejouer ses editions sur d'autres lignes."
            ).format(layers=label, detail=candidate.detail or "-"),
            services, 0, False)
        if not ok or not service:
            flog(f"service_history: attach declined fp={fingerprint[-60:]}")
            return False
        journal = self._journal
        conn = None
        try:
            from .core.datasource_alias import attach_service_history
            conn = journal.create_write_connection()
            attached, message = attach_service_history(conn, fingerprint,
                                                       str(service))
        except Exception as exc:  # noqa: BLE001 - said, never guessed
            attached, message = False, str(exc)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"service_history: write connection close failed "
                         f"({exc!r})", "WARNING")
        flog(f"service_history: attach fp={fingerprint[-60:]} "
             f"service={service!r} ok={attached} msg={message}",
             "WARNING" if attached else "ERROR")
        if attached:
            qlog(self.tr(
                "'{layers}' : {message}. Clic droit sur un de ses evenements "
                "pour retirer ce rattachement."
            ).format(layers=label, message=message), "WARNING")
        else:
            qlog(self.tr("'{layers}' : {message}.").format(
                layers=label, message=message), "WARNING")
        return bool(attached)

    def _detach_service_history(self, fingerprint: str) -> bool:
        """Take back a history the user attached to a service.

        The alias goes (`datasource_alias.remove_alias`), then the
        reconciliation examines the journal again (`force`): the history
        is refused again with its reason and its known services, instead
        of dropping out of the report while attached to nothing.
        """
        if self._refuse_while_busy(self.tr("Retrait de rattachement")):
            return False
        _candidate, manual = self._service_history_state(fingerprint)
        if manual is None:
            return False
        reply = QMessageBox.question(
            self, self.tr("Retirer le rattachement"),
            self.tr(
                "Retirer le rattachement de cet historique a {target} ?\n\n"
                "Ses evenements d'avant v7 ne seront plus vus par un Rewind "
                "de cette couche."
            ).format(target=manual),
            QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
        if reply != QtCompat.MSG_YES:
            return False
        conn = None
        removed = 0
        try:
            from .core.datasource_alias import (
                remove_alias, reconcile_legacy_fingerprints,
            )
            conn = self._journal.create_write_connection()
            removed = remove_alias(conn, fingerprint)
            if removed:
                # The pass writes inside the caller's transaction (the
                # journal open commits it with the schema): committed here.
                with conn:
                    reconcile_legacy_fingerprints(conn, force=True)
        except Exception as exc:  # noqa: BLE001 - said, never guessed
            flog(f"service_history: detach failed fp={fingerprint[-60:]} "
                 f"({exc})", "ERROR")
            qlog(self.tr("Retrait du rattachement impossible : {error}").format(
                error=exc), "ERROR")
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"service_history: write connection close failed "
                         f"({exc!r})", "WARNING")
        flog(f"service_history: detach fp={fingerprint[-60:]} "
             f"removed={removed}", "WARNING")
        qlog(self.tr(
            "Rattachement retire : cet historique est de nouveau hors de "
            "portee des Rewind de la couche."), "WARNING")
        return bool(removed)

    def _count_session_events(self, session_id: str) -> int:
        """US-4.10.01: Count events of a session for the context-menu label."""
        if not session_id:
            return 0
        read_conn = self._get_dialog_read_conn()
        if read_conn is None:
            return 0
        try:
            return count_events_by_session(read_conn, session_id)
        except Exception as exc:
            flog(
                f"_count_session_events: error session_id={session_id} "
                f"err={exc}",
                "WARNING",
            )
            return 0

    def _undo_session(self, session_id: str) -> None:
        """US-4.10.01: undo a whole edit session, with the guarantees of a Rewind.

        The session's USER events used to go to `UndoRunner`, whose
        `undo_apply_one` reverses a RESTORE: an INSERT of the session was
        inserted a second time, a DELETE deleted a snapshot match instead of
        bringing the feature back, an UPDATE rewrote its NEW values, no trace
        was written, and the message announced the session "neutralisee"
        (scenario rw_undo_session_compensates). Undoing a session is
        compensating its events: the same engine as a Rewind
        (`StrictRestoreRunner`: identity proofs, per-layer atomicity,
        read-back of the file, traces), on the session's events collapsed
        with their traces (`_session_undo_plan`).
        """
        from .core.restore_contracts import (
            RestoreCutoff, CutoffType, MAX_EVENTS_PER_RESTORE,
        )
        from .core.rewind_dedup import chronological_key

        if not session_id:
            return
        if self._refuse_while_busy(self.tr("Annulation de session")):
            return
        # The session's last edits may still be on their way to the journal.
        if not self._journal_barrier(self.tr("Annulation de session"), None):
            return
        read_conn = self._get_dialog_read_conn()
        if read_conn is None:
            return

        t0 = time.monotonic()
        try:
            n_total = count_events_by_session(read_conn, session_id)
            events = fetch_events_by_session(read_conn, session_id)
        except Exception as exc:
            flog(
                f"_undo_session: fetch_error session_id={session_id} err={exc}",
                "ERROR",
            )
            qlog(self.tr("Erreur lors de la lecture de la session."), "ERROR")
            return
        fetch_ms = int((time.monotonic() - t0) * 1000)
        if not events:
            qlog(self.tr("Aucun evenement a annuler pour cette session."),
                 "WARNING")
            flog(
                f"_undo_session: no_events session_id={session_id} "
                f"fetch_elapsed_ms={fetch_ms}",
                "INFO",
            )
            return
        if n_total > len(events):
            # The read is capped: compensating the part that came back is a
            # partial undo announced as the undo of the session.
            flog(f"_undo_session: refused session_id={session_id} "
                 f"n_total={n_total} fetched={len(events)} (capped)", "ERROR")
            qlog(self.tr(
                "Annulation de session refusee : la session compte {count} "
                "evenements, au-dela de la limite ({limit}) ; son annulation "
                "serait partielle. Utilisez un Rewind a une date anterieure a "
                "la session."
            ).format(count=n_total, limit=MAX_EVENTS_PER_RESTORE), "WARNING")
            return

        try:
            plan_events, refused, stats = self._session_undo_plan(
                read_conn, events)
        except Exception as exc:  # noqa: BLE001 - unproven is refused
            flog(f"_undo_session: plan failed session_id={session_id} "
                 f"err={exc}", "ERROR")
            qlog(self.tr(
                "Annulation de session refusee : le journal de la session "
                "n'a pas pu etre lu entierement ({error})."
            ).format(error=exc), "ERROR")
            return
        refused_line = self._format_session_refusals(refused)
        if not plan_events:
            if refused_line:
                qlog(self.tr("Annulation de session : rien n'a ete modifie.")
                     + "\n" + refused_line, "WARNING")
            else:
                qlog(self.tr(
                    "Cette session est deja annulee : ses modifications sont "
                    "toutes compensees, rien n'a ete modifie."))
            flog(f"_undo_session: nothing to compensate session_id={session_id} "
                 f"refused={len(refused)}", "INFO")
            return

        by_ds: dict = {}
        for e in plan_events:
            by_ds.setdefault(e.datasource_fingerprint, []).append(e)
        layer_names = sorted({e.layer_name_snapshot or '?' for e in plan_events})
        op_counts = {}
        for e in plan_events:
            op_counts[e.operation_type] = op_counts.get(e.operation_type, 0) + 1
        ops_str = ', '.join(f"{n} {op}" for op, n in sorted(op_counts.items()))
        layers_str = '\n'.join(f"  - {n}" for n in layer_names)

        msg = self.tr(
            "Annuler la session d'edition entiere ?\n\n"
            "Session : {sid}\n"
            "{total} evenement(s) sur {nlayers} couche(s).\n"
            "Operations : {ops}\n\n"
            "Couches touchees :\n{layers}"
        ).format(
            sid=session_id[:8] + "...", total=len(plan_events),
            nlayers=len(by_ds), ops=ops_str, layers=layers_str,
        )
        if refused_line:
            msg += "\n\n" + refused_line

        reply = QMessageBox.question(
            self, self.tr("Annuler la session"),
            msg, QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO,
        )
        if reply != QtCompat.MSG_YES:
            flog(
                f"_undo_session: cancelled_by_user session_id={session_id}",
                "INFO",
            )
            return

        trace_id = generate_trace_id()
        flog(
            f"[{trace_id}] undo_session: start "
            f"session_id={session_id} n_events={len(events)} "
            f"to_compensate={len(plan_events)} refused={len(refused)} "
            f"n_layers={len(by_ds)} fetch_elapsed_ms={fetch_ms}",
            "INFO",
        )
        oldest = min(events, key=chronological_key)
        # "Before the session": the date of its first event, inclusive, for
        # the plan and its coverage preflight.
        cutoff = RestoreCutoff(CutoffType.BY_DATE, oldest.created_at or "",
                               inclusive=True)

        self._active_restore_trace_id = trace_id
        self._current_undo_session_id = session_id
        self._undo_session_cutoff = cutoff
        self._undo_session_refused = list(refused)
        self.restore_button.setEnabled(False)
        self.recover_button.setEnabled(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._restore_started_at = time.monotonic()
        self._start_logo_activity(self._glow_color, "restore")
        self._restore_in_progress = True

        def resolver(evt):
            return find_target_layer(evt, read_conn)

        runner = StrictRestoreRunner(
            plan_events, resolver, cutoff,
            write_queue=self._write_queue,
            tracker=self._tracker, trace_id=trace_id,
            parent=self,
            extra_stats=dict(stats or {}),
            journal_path=self._run_journal_path(),
        )
        runner.progress.connect(self._on_restore_runner_progress)
        runner.layer_started.connect(self._on_restore_runner_layer)
        runner.finished.connect(self._on_undo_session_done)
        self._launch_runner(runner)

    def _session_undo_plan(self, read_conn, events) -> tuple:
        """``(to_compensate, refused, stats)`` for "Annuler toute la session".

        The session's events with every trace that references them, whole
        chains included, collapsed like a rewind window: an event already
        compensated (by a Rewind, a restore, an earlier undo of the
        session) is not compensated twice, and one a forward hop re-applied
        is compensated at the place of that re-application.

        An entity edited AFTER the session by an event outside it is
        refused, whatever became of that edit: writing the session's old
        values over it would erase a later modification the journal still
        describes as applied. Refusing is better than overwriting; the user
        can rewind to a date before the session instead.
        ``refused`` lists ``(layer name, entity, later event id)``.
        """
        from .core import collapse_rewind_events_with_stats
        from .core.event_stream_repository import (
            fetch_entity_stream, fetch_events_by_ids,
        )
        from .core.restore_contracts import MAX_EVENTS_PER_RESTORE
        from .core.rewind_dedup import chronological_key

        ids = [e.event_id for e in events if e.event_id is not None]
        traces = fetch_events_by_ids(
            read_conn, self._trace_tree_ids(read_conn, ids))
        active, stats = collapse_rewind_events_with_stats(
            list(events) + traces, key_columns=self._live_key_columns(events))

        session_ids = set(ids)
        newest: dict = {}
        names: dict = {}
        for e in events:
            key = (e.datasource_fingerprint, e.entity_fingerprint)
            mark = chronological_key(e)
            if key not in newest or mark > newest[key]:
                newest[key] = mark
            names[key] = e.layer_name_snapshot or e.datasource_fingerprint
        blocked: dict = {}
        for key, last in newest.items():
            fp, entity = key
            if not entity:
                continue
            stream = fetch_entity_stream(read_conn, fp, entity)
            if len(stream) >= MAX_EVENTS_PER_RESTORE:
                # The history of this entity was capped: a later edit cannot
                # be ruled out.
                blocked[key] = None
                continue
            later = [r for r in stream
                     if r.restored_from_event_id is None
                     and r.event_id not in session_ids
                     and chronological_key(r) > last]
            if later:
                blocked[key] = later[0].event_id
        keep = [e for e in active
                if (e.datasource_fingerprint, e.entity_fingerprint) not in blocked]
        refused = [(names.get(key, key[0]), key[1], eid)
                   for key, eid in blocked.items()]
        flog(f"_session_undo_plan: events={len(events)} traces={len(traces)} "
             f"active={len(active)} to_compensate={len(keep)} "
             f"refused_entities={len(refused)}")
        return keep, refused, stats

    @staticmethod
    def _trace_tree_ids(read_conn, source_ids) -> list:
        """Ids of every trace below `source_ids`: traces, traces of traces..."""
        out: list = []
        ids = sorted({int(i) for i in (source_ids or []) if i})
        for start in range(0, len(ids), 500):
            chunk = ids[start:start + 500]
            ph = ",".join("?" for _ in chunk)
            # B608: `ph` is only `?,?,...`; the ids are bound as parameters.
            rows = read_conn.execute(
                "WITH RECURSIVE tree(id) AS ("  # nosec B608
                " SELECT event_id FROM audit_event"
                " WHERE restored_from_event_id IN (" + ph + ")"
                " UNION"
                " SELECT a.event_id FROM audit_event a"
                " JOIN tree t ON a.restored_from_event_id = t.id)"
                " SELECT id FROM tree", chunk).fetchall()
            out.extend(int(r[0]) for r in rows)
        return sorted(set(out))

    def _format_session_refusals(self, refused) -> str:
        """Name the entities an undo of session leaves alone, and why."""
        if not refused:
            return ""
        shown = ", ".join(
            f"{layer} {entity}" for layer, entity, _eid in list(refused)[:10])
        more = len(refused) - min(len(refused), 10)
        if more:
            shown += self.tr(" (+{count} autre(s))").format(count=more)
        return self.tr(
            "{count} entite(s) modifiee(s) apres la session par une autre "
            "edition, non annulee(s) pour ne pas effacer cette modification "
            "posterieure : {names}. Pour les remettre dans leur etat d'avant "
            "la session, utilisez un Rewind a une date anterieure a la session."
        ).format(count=len(refused), names=shown)

    def _on_undo_session_done(self, result) -> None:
        """US-4.10.01: the session undo ended: undo state, report by bucket, UI."""
        trace_id = self._active_restore_trace_id
        session_id = getattr(self, '_current_undo_session_id', '')
        self._restore_in_progress = False
        # Read off the runner before its reference goes, like the rewind.
        runner = self._restore_runner or self._cancelled_restore_runner
        partial_n, partial_fields = self._partial_restore_report(runner)
        apply_report = self._runner_apply_report(runner)
        self._restore_runner = None
        self._cancelled_restore_runner = None
        cleanup_temp_layers()
        self._smooth_set_progress(100)
        self._stop_logo_activity()
        refused = list(getattr(self, "_undo_session_refused", []) or [])
        self._undo_session_refused = []
        cutoff = getattr(self, "_undo_session_cutoff", None)
        self._undo_session_cutoff = None

        if trace_id:
            started = self._restore_started_at
            elapsed_ms = int((time.monotonic() - started) * 1000) if started else 0
            flog(
                f"[{trace_id}] undo_session: done "
                f"session_id={session_id} "
                f"ok={result.total_ok} fail={result.total_fail} "
                f"refused={len(refused)} elapsed_ms={elapsed_ms}",
                "INFO",
            )
            self._active_restore_trace_id = ""

        if result.by_ds:
            # A run like any other: "Annuler le dernier restore" brings the
            # session back, in the reverse order of this undo.
            written = self._without_layers_edited_during_run(
                self._in_application_order(result.by_ds, cutoff))
            self._record_last_run(written)
            self._set_undo_state(self._merge_undo_scopes(
                self._last_restore_by_ds, written))

        buckets = _restore_buckets(result, partial_n=partial_n,
                                   report=apply_report)
        partial_line = self._format_partial_restore(partial_n, partial_fields)
        refused_line = self._format_session_refusals(refused)
        msg = self.tr("Annulation de la session : {outcome}").format(
            outcome=self._format_restore_outcome(buckets))
        clean = (buckets["failed_total"] == 0 and buckets["cancelled"] == 0
                 and not result.errors and not partial_line and not refused)
        if result.errors:
            msg += " | " + " | ".join(result.errors[:5])
        if refused_line:
            msg += "\n" + refused_line
        qlog(msg + partial_line + self._build_restore_summary(result.by_ds),
             "INFO" if clean else "WARNING")

        self._end_progress_ui()
        self.enable_controls(True)
        self.recover_button.setEnabled(True)
        self.restore_button.setEnabled(True)
        self._current_undo_session_id = ""
        self.iface.mapCanvas().refresh()

    def _undo_last_restore(self):
        """Undo the last restore: revert data to its pre-restore state."""
        # Nothing else may hold the layers: with a Rewind of another layer
        # in flight the button used to stay armed, and a click started an
        # UndoRunner beside it (revue brev2, P1). Refused before anything is
        # asked, and without touching the run's controls.
        if self._refuse_while_busy(self.tr("Annulation")):
            return
        # The last run and nothing else, as a copy: the runner accounts
        # against what it was given, whatever the live pool becomes while
        # it runs (an edit notified mid-run drops a layer from the pool).
        by_ds = self._manual_undo_scope()
        if not by_ds:
            flog("undo_last: requested but no undo state available")
            return
        # The traces of the restore to undo may still be on their way to
        # the journal: invalidating them before they land would leave them
        # active over data the undo has reverted.
        if not self._journal_barrier(self.tr("Annulation"), list(by_ds)):
            return

        layer_lines = []
        total = 0
        for fp, evts in by_ds.items():
            name = evts[0].layer_name_snapshot or fp
            layer_lines.append(f"  - {name} ({len(evts)} op.)")
            total += len(evts)
        flog(f"undo_last: requested layers={len(by_ds)} events={total}")

        reply = QMessageBox.question(
            self,
            self.tr("Annuler le restore"),
            self.tr("Remettre {count} entite(s) dans leur etat avant le restore ?\n\n{layers}").format(
                count=total, layers="\n".join(layer_lines)),
            QtCompat.MSG_YES | QtCompat.MSG_NO,
            QtCompat.MSG_NO,
        )
        if reply != QtCompat.MSG_YES:
            return

        self.undo_last_btn.setEnabled(False)
        self._restore_in_progress = True
        self.recover_button.setEnabled(False)
        self.enable_controls(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._restore_started_at = time.monotonic()
        self._start_logo_activity(self._glow_color, "restore")

        read_conn = self._get_dialog_read_conn()

        def resolver(evt):
            return find_target_layer(evt, read_conn)

        runner = UndoRunner(by_ds, resolver, tracker=self._tracker, parent=self,
                            journal_path=self._run_journal_path())
        runner.progress.connect(self._on_restore_runner_progress)
        runner.layer_started.connect(self._on_restore_runner_layer)
        runner.finished.connect(self._on_undo_done)
        self._undo_input_by_ds = by_ds
        self._launch_runner(runner)

    def _on_undo_done(self, result) -> None:
        """Account a manual undo by what it really reverted.

        trace-invalidation-ignores-undo-outcome / undo-invalidates-failed-
        traces: every event of the input used to have its trace
        invalidated, plus the failed ones explicitly, and the undo state was
        dropped whole. A failed undo, an undo that found nothing ("Already
        absent", counted as a success) and the tail a cancel left alone
        still have their compensation in the layer: invalidating their
        trace made the next Rewind replay it (a DELETE compensation is an
        addFeature: the feature came back twice). Now only the undone
        events lose their trace and leave the undo state; the others stay
        revertible, and the message counts each population by its name.
        """
        runner = self._restore_runner or self._cancelled_restore_runner
        undo_input = getattr(self, "_undo_input_by_ds", None) or {}
        self._undo_input_by_ds = None
        outcome = self._undo_outcome(runner, result, undo_input)
        self._restore_in_progress = False
        self._restore_runner = None
        self._cancelled_restore_runner = None
        # What was undone leaves the pool and the last run; what was not
        # stays revertible (unless the user edited its layer meanwhile).
        self._drop_undone(outcome["undone"])

        self._smooth_set_progress(100)
        self._stop_logo_activity()
        self._end_progress_ui()
        self.enable_controls(True)
        self.recover_button.setEnabled(True)

        n_undone = len(outcome["undone"])
        shortfall = self._format_undo_shortfall(outcome)
        if n_undone and not shortfall:
            qlog(self.tr("{count} entite(s) remise(s) dans leur etat initial.").format(
                count=n_undone))
        elif n_undone:
            qlog(self.tr(
                "Annulation partielle : {count} entite(s) remise(s) dans leur "
                "etat initial ; {rest}. Les entites non remises restent "
                "annulables."
            ).format(count=n_undone, rest=shortfall), "WARNING")
        else:
            qlog(self.tr("Annulation : aucune entite remise ({rest}).").format(
                rest=shortfall or "0")
                + (" | " + " | ".join(result.errors[:5]) if result.errors else ""),
                "ERROR")

        if n_undone:
            self.iface.mapCanvas().refreshAllLayers()

        # RW-stale-dedup symmetry: a manual undo that reverts a previous
        # rewind's data MUST also invalidate the corresponding traces, or
        # the next rewind on the same scope sees them as still "active" and
        # collapses to a no-op. Only for what was really reverted, and
        # read back: a journal that cannot be brought in line is said.
        if not self._invalidate_undone_traces(outcome["undone"], "undo_done"):
            qlog(self.tr(
                "Annulation appliquee aux couches, mais le journal n'a pas pu "
                "etre mis a jour pour {count} entite(s) (verrou, trace du "
                "restore annule absente, ou trace d'un restore anterieur "
                "touchee). Un Rewind sur ces couches partirait d'un "
                "historique faux : verifiez-les avant de rembobiner."
            ).format(count=n_undone), "ERROR")

    def _journal_high_water_mark(self):
        """MAX(event_id) of the journal now, or None when it cannot be read.

        Read on a connection of its own: a long-lived read handle can sit
        on an old snapshot, and a mark read below the real one would count
        an older run's traces as this run's.
        """
        journal = self._journal
        if journal is None or not getattr(journal, "is_open", False):
            flog("run_mark: journal not open, mark unknown (the Undo will "
                 "fall back on the newest trace of each event)", "WARNING")
            return None
        conn = None
        try:
            conn = journal.create_read_connection()
            row = conn.execute(
                "SELECT COALESCE(MAX(event_id), 0) FROM audit_event").fetchone()
            return int(row[0])
        except Exception as exc:  # noqa: BLE001 - unknown is said, never guessed
            flog(f"run_mark: unreadable ({exc}), mark unknown (the Undo will "
                 f"fall back on the newest trace of each event)", "WARNING")
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"run_mark: read connection close failed ({exc!r})",
                         "DEBUG")

    def _run_trace_split(self, conn, source_ids) -> dict:
        """``{source id: (own, older)}``: the ACTIVE traces of these events.

        `own` are the traces the last run wrote: above the journal mark read
        at its launch (event_id is AUTOINCREMENT, a later row has a larger
        id). `older` are the traces earlier runs wrote on the same events.
        Under v7 an event can hold both at once: a forward hop cancels a
        compensation with a trace of the trace and leaves the first one
        active, and a later Rewind (or "Annuler toute la session")
        compensates the event again. The Undo of that last run used to
        invalidate every active trace of the event: the journal then said
        the earlier run's compensation was undone too, and a Rewind to a
        date between the runs wrote the wrong value, reported restored
        (scenario rw_undo_invalidates_own_trace).

        Without a mark, `own` falls back on the newest active trace of each
        event. Both lists are in id order. Raises when the journal cannot
        be read.
        """
        mark = getattr(self, "_last_run_mark", None)
        ids = [int(i) for i in source_ids]
        active: dict = {}
        for start in range(0, len(ids), 500):
            chunk = ids[start:start + 500]
            ph = ','.join('?' * len(chunk))
            for tid, sid in conn.execute(
                    f"SELECT event_id, restored_from_event_id FROM audit_event "  # nosec B608
                    f"WHERE restored_from_event_id IN ({ph}) "
                    f"AND invalidated_at IS NULL ORDER BY event_id",
                    chunk).fetchall():
                active.setdefault(int(sid), []).append(int(tid))
        split: dict = {}
        for sid, tids in active.items():
            own = ([t for t in tids if t > mark] if mark is not None
                   else tids[-1:])
            split[sid] = (own, [t for t in tids if t not in own])
        return split

    def _invalidate_trace_events(self, undone_ids: list) -> int:
        """Soft-delete the traces the undone run wrote for these events (RW-02).

        Only the last run's own traces (`_run_trace_split`): an older
        trace of the same event belongs to an earlier run whose
        compensation this undo does not touch. The selection and the
        UPDATE run in one write transaction, so nothing lands between what
        is chosen and what is stamped.

        Uses a dedicated write connection (NOT a read-only one) because the
        ``audit_event`` table is being mutated. SQLite WAL serialises writers
        so concurrent activity from the WriteQueue is safe but may briefly
        block on the file lock.

        Returns the number of rows actually invalidated. Returns 0 on
        failure or when no matching trace exists.

        `undone_ids` comes in the order the undo REVERTED the events, and
        each trace gets its own instant, strictly increasing in that order.
        One instant for the whole undo erased that order: a later rewind
        unwinds these toggles newest first and, at equal instants, fell back
        on the trace ids, i.e. on the order the undone restore had written
        them, the opposite of the undo's. Two edits of one field then came
        back in the wrong order and the field ended on the older NEW value,
        reported restored (scenario rw_undo_order_journal).
        """
        from datetime import datetime, timedelta, timezone
        if not undone_ids:
            return 0
        base = datetime.now(timezone.utc)
        mark = getattr(self, "_last_run_mark", None)
        conn = None
        try:
            conn = self._journal.create_write_connection()
            conn.execute("BEGIN IMMEDIATE")
            split = self._run_trace_split(conn, undone_ids)
            # Undo order across events; within one event, newest first.
            targets = [tid for sid in undone_ids
                       for tid in reversed(split.get(int(sid), ((), ()))[0])]
            stamps = [
                ((base + timedelta(microseconds=i)).isoformat(
                    timespec="microseconds"), tid)
                for i, tid in enumerate(targets)]
            rowcount = 0
            if stamps:
                cursor = conn.executemany(
                    "UPDATE audit_event SET invalidated_at = ? "
                    "WHERE event_id = ? AND invalidated_at IS NULL",
                    stamps,
                )
                rowcount = cursor.rowcount
            conn.commit()
            flog(f"_invalidate_trace_events: undone_ids_n={len(undone_ids)} "
                 f"run_mark={mark if mark is not None else 'unknown(newest)'} "
                 f"own_traces={targets[:20]} rows_updated={rowcount} "
                 f"from={stamps[0][0] if stamps else '-'} "
                 f"to={stamps[-1][0] if stamps else '-'} (in undo order)")
            return rowcount if rowcount is not None and rowcount >= 0 else 0
        except Exception as exc:
            flog(f"_invalidate_trace_events: trace invalidation failed: "
                 f"{exc}", "WARNING")
            return 0
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    # WARNING: this is the write connection used for the
                    # invalidation UPDATE. A handle that will not close can
                    # hold the journal lock against the next writer.
                    flog(f"_invalidate_trace_events: write connection close "
                         f"failed, journal lock may be held ({exc!r})",
                         "WARNING")

    def _active_trace_sources(self, source_ids: list):
        """``{source id: (own, older)}`` of the active traces, or None if unreadable.

        The same selection as the UPDATE (`_run_trace_split`), read on a
        connection of its own so that it sees the last commit, not the
        snapshot of a long-lived handle.
        """
        journal = self._journal
        if journal is None or not getattr(journal, "is_open", False):
            return None
        conn = None
        try:
            conn = journal.create_read_connection()
            return self._run_trace_split(conn, source_ids)
        except Exception as exc:  # noqa: BLE001 - unknown is reported as such
            flog(f"_active_trace_sources: unreadable ({exc})", "WARNING")
            return None
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"_active_trace_sources: read connection close "
                         f"failed ({exc!r})", "DEBUG")

    def _invalidate_undone_traces(self, undone_ids: list, context: str) -> bool:
        """Invalidate the traces the undone run wrote, and prove it by reading back.

        True when every undone event had an active trace of the undone run
        before (`_run_trace_split`), each of those is invalidated after,
        and every OLDER active trace of these events is still active: an
        earlier run's compensation, cancelled by a trace of the trace, is
        not this undo's to touch, and the event may keep an active trace.
        False when the journal could not be read or written (lock, closed
        journal), when an undone event had no active trace of the run to
        invalidate (still queued, in the pending file, or never written: it
        may come back ACTIVE after this undo; an older trace is never taken
        in its place), when a trace of the run is still active, or when an
        older trace was invalidated. The journal then no longer describes
        the layers, and the caller must say so. An invalidation failure used
        to be a WARNING, and a rowcount of 0 was indistinguishable from
        success.

        The ids keep the order the undo reverted them in (duplicates
        dropped): `_invalidate_trace_events` records that order.
        """
        ids = list(dict.fromkeys(int(i) for i in (undone_ids or []) if i))
        if not ids:
            return True
        before = self._active_trace_sources(ids)
        rows = self._invalidate_trace_events(ids)
        after = self._active_trace_sources(ids)
        mark = getattr(self, "_last_run_mark", None)
        own: dict = {}
        collateral: list = []
        if before is None or after is None:
            missing, still = ids, ids
        else:
            own = {t: sid for sid, (mine, _old) in before.items() for t in mine}
            older = {t for _mine, old in before.values() for t in old}
            active_after = {t for mine, old in after.values()
                            for t in (*mine, *old)}
            missing = [sid for sid in ids if not before.get(sid, ((), ()))[0]]
            still = sorted({sid for t, sid in own.items() if t in active_after})
            collateral = sorted(older - active_after)
            guessed = sorted(sid for sid, (_mine, old) in before.items() if old)
            if mark is None and guessed:
                # No mark: the newest trace was taken as the run's for events
                # that hold several. Right unless the run wrote none for one
                # of them; the log keeps the guess visible.
                flog(f"{context}: run mark unknown, newest active trace taken "
                     f"as the undone run's for events with older active "
                     f"traces={guessed[:20]}", "WARNING")
        ok = (before is not None and after is not None and not missing
              and not still and not collateral)
        flog(f"{context}: trace invalidation: undone_eids_n={len(ids)} "
             f"run_mark={mark if mark is not None else 'unknown(newest)'} "
             f"run_traces_n={len(own)} rows_updated={rows} "
             f"without_run_trace={missing[:20]} "
             f"still_active={still[:20]} older_invalidated={collateral[:20]} "
             f"verdict={'ok' if ok else 'JOURNAL_OUT_OF_SYNC'}",
             "INFO" if ok else "ERROR")
        return ok

    def reset_undo_state(self) -> None:
        """Drop the last-restore memory (called by the plugin on project switch).

        Prevents undoing a restore made in project A onto the layers of project B.
        """
        had_state = bool(self._last_restore_by_ds)
        self._last_restore_events = None
        self._last_restore_by_ds = None
        self._last_run_ids = set()
        self._last_run_mark = None
        self._run_journal_mark = None
        self._rewind_scope_fps = None
        self._undo_input_by_ds = None
        self._undo_session_refused = []
        self._undo_session_cutoff = None
        if hasattr(self, 'undo_last_btn'):
            self.undo_last_btn.setEnabled(False)
        if had_state:
            flog("RecoverDialog: undo state reset (project switched)")

    def _journal_barrier(self, action: str, scope_fps=None,
                         trace_id: str = "") -> bool:
        """Read-your-writes before planning a Restore, a Rewind or an Undo.

        unflushed-and-pending-events-invisible: the plan reads committed
        rows only. Edits still in the write queue, in the durable outbox of
        a writer that is not indexing, or in the pending-recovery file, are
        real edits the plan cannot see: they were not compensated, the run
        was announced complete, and once imported at the next start they
        sat inside the span of a trace written meanwhile and stayed
        neutralised ("Rien a restaurer" on a wrong value).

        True when the plan may go on. Otherwise the refusal is logged and
        shown with its reason, the controls are released, and False is
        returned: nothing has been asked, planned or written.
        """
        prefix = f"[{trace_id}] " if trace_id else ""
        reason = ""
        wq = self._write_queue
        if wq is not None:
            try:
                drained = bool(wq.wait_until_drained(
                    timeout=_JOURNAL_BARRIER_TIMEOUT_S))
            except Exception as exc:  # noqa: BLE001 - unproven is refused
                flog(f"{prefix}journal_barrier: barrier failed ({exc})", "ERROR")
                drained = False
            if not drained:
                backlog = self._get_write_queue_pending()
                reason = self.tr(
                    "{count} modification(s) recente(s) pas encore ecrite(s) "
                    "dans le journal"
                ).format(count=backlog) if backlog else self.tr(
                    "l'enregistreur du journal ne confirme pas avoir tout "
                    "ecrit")
            elif callable(getattr(wq, "health", None)):
                try:
                    health = wq.health()
                    # Event batches durable in the outbox and not indexed
                    # yet (a writer that is dead, stopped or behind). An
                    # open prepare is a commit in flight, not a backlog.
                    backlog = max(0, int(getattr(health, "outbox_outstanding", 0) or 0)
                                  - int(getattr(health, "open_prepares", 0) or 0))
                except Exception as exc:  # noqa: BLE001 - unproven is refused
                    flog(f"{prefix}journal_barrier: health unreadable ({exc})",
                         "ERROR")
                    backlog = -1
                if backlog:
                    reason = self.tr(
                        "des lots de modifications attendent d'etre indexes "
                        "dans le journal (enregistreur arrete ou en retard)")
        shared_only = False
        if not reason:
            reasons = []
            waiting = self._pending_file_events(scope_fps)
            for n_pending, pending_path, own in waiting:
                if own:
                    reasons.append(self.tr(
                        "{count} modification(s) de ces couches attendent dans "
                        "le fichier de secours {path}, reintegre au prochain "
                        "demarrage"
                    ).format(count=n_pending, path=pending_path))
                else:
                    # The profile's shared legacy file is never imported:
                    # a restart does not bring these edits back, the user
                    # has to deal with the file (integrity._report_shared_pending).
                    reasons.append(self.tr(
                        "{count} modification(s) de ces couches attendent dans "
                        "le fichier de secours {path}, partage par tous les "
                        "projets non enregistres du profil et jamais "
                        "reintegre automatiquement (le journal auquel "
                        "appartiennent ses evenements ne peut pas etre "
                        "prouve)"
                    ).format(count=n_pending, path=pending_path))
            reason = " ; ".join(reasons)
            shared_only = bool(waiting) and all(
                not own for _n, _p, own in waiting)
        if not reason:
            return True
        flog(f"{prefix}journal_barrier: action={action} status=refused "
             f"reason={reason}", "ERROR")
        if not self._run_in_flight():
            # Released only when nothing else holds them: a refusal must
            # never reopen the controls of a run that still writes.
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
        if shared_only:
            qlog(self.tr(
                "{action} refuse(e) : le journal est incomplet ({reason}). "
                "Planifier maintenant ignorerait ces modifications et "
                "annoncerait un etat faux. Un redemarrage ne les reintegrera "
                "pas : examinez ce fichier, puis deplacez-le hors du dossier "
                "une fois ces modifications traitees ou jugees etrangeres a "
                "ce projet."
            ).format(action=action, reason=reason), "WARNING")
            return False
        qlog(self.tr(
            "{action} refuse(e) : le journal est incomplet ({reason}). "
            "Planifier maintenant ignorerait ces modifications et annoncerait "
            "un etat faux. Reessayez dans quelques secondes ; si le refus "
            "persiste, redemarrez QGIS pour reintegrer les modifications en "
            "attente."
        ).format(action=action, reason=reason), "WARNING")
        return False

    def _pending_file_events(self, scope_fps) -> list:
        """``[(n, path, own)]``: events of the scope waiting outside the journal.

        Every pending file whose events may belong to this journal
        (`integrity.pending_files_for`): its own, reintegrated at the next
        start (``own`` True), and for a journal of the profile's shared
        audit directory the LEGACY file shared by every unsaved-project
        journal, never imported because the journal its events belong to
        cannot be proven (``own`` False). Only the own file used to be
        read: edits of these very layers waiting in the shared file were
        invisible to the plan, which then announced a state that ignores
        them. Only the files holding events of the scope are listed.

        ``scope_fps`` None means every layer. A file that cannot be read
        cannot be proven empty for the scope: it counts as one.
        """
        path = getattr(self._journal, "path", None) if self._journal else None
        if not path:
            return []
        try:
            from .core.integrity import pending_files_for, _get_pending_path
            own_path = _get_pending_path(path)
            files = pending_files_for(path)
        except Exception as exc:  # noqa: BLE001 - same location, spelled out
            flog(f"journal_barrier: pending files unresolved ({exc}); "
                 f"the file next to the journal is read", "WARNING")
            own_path = os.path.join(os.path.dirname(path),
                                    "recoverland_pending.json")
            files = [own_path]
        scope = self._expand_scope(scope_fps) if scope_fps else None
        out = []
        for pending in files:
            if not os.path.isfile(pending):
                continue
            try:
                with open(pending, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except (OSError, ValueError) as exc:
                flog(f"journal_barrier: pending file unreadable {pending} "
                     f"({exc})", "ERROR")
                data = None
            if not isinstance(data, list):
                n = 1
            else:
                n = sum(1 for evt in data if isinstance(evt, dict) and (
                    scope is None
                    or evt.get("datasource_fingerprint") in scope))
            if n:
                try:
                    from .core.long_paths import plain
                    shown = plain(pending)
                except Exception:  # noqa: BLE001 - shown as it is spelled
                    shown = pending
                out.append((n, shown, pending == own_path))
        return out

    def _run_in_flight(self) -> str:
        """Class name of the run that holds the layers, "" when none does.

        A runner the dialog owns, or one a cancel detached and that still
        walks to its end: both still write. `_restore_in_progress` covers
        the moment a run is set up before its runner exists.
        """
        runner = (getattr(self, "_restore_runner", None)
                  or getattr(self, "_cancelled_restore_runner", None))
        if runner is not None:
            return type(runner).__name__
        if getattr(self, "_restore_in_progress", False):
            return "restore"
        return ""

    def _busy_message(self, action: str, busy: str) -> str:
        """The refusal the user reads: what was refused, and what holds the layers."""
        what = {
            "StrictRestoreRunner": self.tr("un Rewind"),
            "UndoRunner": self.tr("une annulation"),
            "RestoreRunner": self.tr("une restauration"),
            "recovering": self.tr("une recherche ou la preparation d'un Rewind"),
        }.get(busy, self.tr("une restauration"))
        return self.tr(
            "{action} refuse(e) : {what} est en cours sur les couches. "
            "Attendez sa fin, ou annulez-la, puis recommencez."
        ).format(action=action, what=what)

    def _refuse_while_busy(self, action: str,
                           check_recovering: bool = True) -> bool:
        """True, and said, when another operation holds the dialog.

        Every entry point that plans or writes asks this FIRST, and a
        refusal touches nothing else: the run in flight owns its flags,
        its runner and the controls. The Undo button had no such guard: a
        click during a Rewind of another layer started an UndoRunner beside
        it, `_launch_runner` overwrote the Rewind's reference and the
        undo's end reopened the controls while the Rewind still wrote
        (revue brev2, P1). F5, the Review toggle, a restore or an Undo of
        session had the same hole.
        """
        busy = self._run_in_flight()
        if not busy and check_recovering and getattr(self, "_is_recovering", False):
            busy = "recovering"
        if not busy:
            return False
        flog(f"run_guard: action={action!r} refused busy={busy}", "WARNING")
        qlog(self._busy_message(action, busy), "WARNING")
        return True

    def _sync_run_controls(self) -> None:
        """Undo button and mode selector follow the state; off while a run holds the layers."""
        busy = bool(self._run_in_flight())
        selector = getattr(self, "restore_mode_selector", None)
        if selector is not None:
            # A switch to Rewind mode re-enabled the Rewind button under a
            # restore or an Undo in flight.
            selector.setEnabled(not busy)
        button = getattr(self, "undo_last_btn", None)
        if button is not None:
            button.setEnabled(not busy and bool(self._manual_undo_scope()))

    def _launch_runner(self, runner) -> None:
        """Start a runner the dialog owns until its `finished`.

        The project-clear guard is armed first: a runner must never outlive
        the project whose layers it writes (project-switch-does-not-
        quiesce-runner). From here until the finished slot of this runner,
        the Undo button and the mode selector are off.

        The journal mark is read here, after the entry point's barrier and
        before the runner writes anything: every trace this run writes has
        a larger id (`_record_last_run` keeps it with the run).
        """
        self._run_journal_mark = self._journal_high_water_mark()
        self._restore_runner = runner
        self._edited_during_run = set()
        self._sync_run_controls()
        self._arm_project_clear_guard()
        runner.start()

    def _arm_project_clear_guard(self) -> None:
        if getattr(self, "_project_clear_guard_armed", False):
            return
        try:
            QgsProject.instance().aboutToBeCleared.connect(
                self._on_project_about_to_clear)
            self._project_clear_guard_armed = True
        except (AttributeError, TypeError, RuntimeError) as exc:
            flog(f"project_clear_guard: not armed ({exc}); "
                 f"on_project_switched still stops the runner", "WARNING")

    def _on_project_about_to_clear(self) -> None:
        """The project is going: end the runner while its layers and journal exist.

        `QgsProject.aboutToBeCleared` comes before `cleared`, on which the
        plugin stops the write queue and closes the journal (and before
        `readProject` when another project is opened). Unload already
        released the dialog before the backend; a project switch did not.
        """
        self._quiesce_restore_runner("project_about_to_clear")

    def _quiesce_restore_runner(self, reason: str,
                                timeout_ms: int = 0) -> bool:
        """Cancel the running runner and wait for its `finished`, bounded.

        cancel() only asks: the runner stops at its next tick, and its
        finished slot then accounts what it wrote (undo state, trace
        invalidation) while the project it wrote to is still there; its
        traces leave while that project's writer still runs. No run is
        chained after another's end any more (the undo that preceded a
        Rewind is gone), so nothing new starts from that slot. A runner
        that does not end in time is detached: its late end must not arm
        the next project's Undo with this project's scope.
        """
        from qgis.PyQt.QtCore import QEventLoop
        runner = self._restore_runner or self._cancelled_restore_runner
        if runner is None:
            return True
        timeout_ms = int(timeout_ms or _QUIESCE_TIMEOUT_MS)
        name = type(runner).__name__
        flog(f"quiesce_runner: reason={reason} runner={name} cancelling, "
             f"waiting up to {timeout_ms} ms for its end", "WARNING")
        finished = []
        loop = QEventLoop()

        def _on_finished(_result=None):
            finished.append(True)
            loop.quit()

        runner.finished.connect(_on_finished)
        timer = QTimer()
        timer.setSingleShot(True)
        timer.timeout.connect(loop.quit)
        try:
            runner.cancel()
            if not finished:
                timer.start(timeout_ms)
                loop.exec(QEventLoop.ProcessEventsFlag.ExcludeUserInputEvents)
        finally:
            timer.stop()
            try:
                runner.finished.disconnect(_on_finished)
            except (TypeError, RuntimeError):
                pass
        if finished:
            flog(f"quiesce_runner: reason={reason} runner={name} ended "
                 f"before the project went")
            return True
        try:
            runner.finished.disconnect()
        except (TypeError, RuntimeError):
            pass
        self._restore_runner = None
        self._cancelled_restore_runner = None
        self._restore_in_progress = False
        # Its finished slot will never come here: the controls it held are
        # given back now.
        self._sync_run_controls()
        flog(f"quiesce_runner: reason={reason} runner={name} still running "
             f"after {timeout_ms} ms; detached from the dialog", "ERROR")
        qlog(self.tr(
            "Une restauration en cours n'a pas pu se terminer avant le "
            "changement de projet ; elle a ete detachee. Verifiez les couches "
            "du projet precedent a sa reouverture."), "ERROR")
        return False

    def on_project_switched(self, tracker=None) -> None:
        """Full reset when the QGIS project changes.

        Drops the stale read connection, stats cache, date bounds, search
        results and undo state so the dialog picks up the new journal.
        """
        # First: a runner still alive here escaped the project-clear guard.
        # It is stopped before anything else goes, and its end cannot arm
        # the new project's Undo (the reset below runs after it).
        self._quiesce_restore_runner("project_switched")
        thread = self._version_fetch_thread
        if thread is not None:
            # A fetch of the old journal must not feed a rewind here.
            self._disconnect_thread_signals(thread)
            try:
                if thread.isRunning():
                    thread.stop()
                    thread.wait(500)
            except RuntimeError as exc:
                flog(f"on_project_switched: fetch thread stop: {exc}", "DEBUG")
            self._version_fetch_thread = None
        self._close_dialog_read_conn()
        self._stats_cache = LayerStatsCache()
        # New project, new journal, new reconciliation verdict: the
        # warning is due again.
        self._reconciliation_notified = set()
        self._initial_bounds_applied = False
        self._search_events = []
        self._all_attr_keys = []
        self._smart_bar_summary = None
        if tracker is not None:
            self._tracker = tracker
        self.reset_undo_state()
        if hasattr(self, 'table_widget') and self.table_widget is not None:
            self.table_widget.setRowCount(0)
            self.table_widget.setColumnCount(0)
        self._refresh_layers_panel()
        self._refresh_journal_status()
        flog("RecoverDialog: on_project_switched complete")

    def showEvent(self, event):
        super().showEvent(event)
        flog("RecoverDialog: shown")
        self._stats_cache.invalidate()
        if hasattr(self, '_layer_refresh_timer'):
            self._layer_refresh_timer.start()
        self._refresh_journal_status()
        if self._review_wants_persist and self._review_snap_mode:
            self.restore_mode_selector.setMode("review")
            self._review_toggle.blockSignals(True)
            self._review_toggle.setChecked(True)
            self._review_toggle.blockSignals(False)
            self._review_toggle.setEnabled(True)
            flog(
                "showEvent: review_snap_mode restored on reopen "
                f"bar={'yes' if self._review_date_bar is not None else 'none'} "
                f"status_widget={'yes' if self._review_status_widget is not None else 'none'}",
                "INFO",
            )
            # The date bar lives on the canvas viewport, so it survives the
            # dialog hide - but its geometry/z-order can be stale after a
            # canvas resize while we were hidden, leaving it invisible. Force
            # a reposition (which re-shows it) once Qt has re-mapped us.
            bar = self._review_date_bar
            if bar is not None:
                QTimer.singleShot(0, bar._reposition)
                QTimer.singleShot(250, bar._reposition)
                flog("showEvent: date_bar reposition rescheduled on reopen", "DEBUG")
            else:
                flog(
                    "showEvent: date_bar missing on reopen "
                    "(snap_mode=True, bar=None) - bar lost",
                    "WARNING",
                )
            # Defensive: ensure the status bar pill stays visible. Calling
            # _show_review_status_bar unconditionally lets it self-heal the
            # widget if a prior dialog close tore it down.
            self._show_review_status_bar()
            if self._review_status_widget is not None:
                self._review_status_widget.activate()
        elif self._review_snap_mode and not self._review_wants_persist:
            flog("showEvent: snap_mode orphan detected - stopping", "WARNING")
            self._stop_snapshot_mode()

    def hideEvent(self, event):
        super().hideEvent(event)
        flog(
            "RecoverDialog: hideEvent "
            f"snap_mode={self._review_snap_mode} "
            f"wants_persist={self._review_wants_persist}",
            "DEBUG",
        )
        if hasattr(self, '_layer_refresh_timer'):
            self._layer_refresh_timer.stop()
        if hasattr(self, '_stats_debounce_timer'):
            self._stats_debounce_timer.stop()

    def closeEvent(self, event):
        """Close event - data stays in its current state (rewound or not)."""
        flog(
            "RecoverDialog: closeEvent "
            f"snap_mode={self._review_snap_mode} "
            f"wants_persist={self._review_wants_persist}",
            "INFO",
        )
        if getattr(self, '_restore_in_progress', False):
            from qgis.PyQt.QtWidgets import QMessageBox
            reply = QMessageBox.question(
                self,
                self.tr("Restauration en cours"),
                self.tr(
                    "Une restauration est en cours.\n"
                    "Fermer maintenant annulera la couche en cours "
                    "et perdra les modifications non encore appliquees.\n\n"
                    "Fermer quand meme ?"
                ),
                QtCompat.MSG_YES | QtCompat.MSG_NO,
                QtCompat.MSG_NO,
            )
            if reply != QtCompat.MSG_YES:
                flog("closeEvent: restore in progress, user chose to keep open")
                event.ignore()
                return
            flog("closeEvent: restore in progress, user confirmed close")
        flog("RecoverDialog: closing")
        self.cleanup_resources()
        event.accept()

    def reject(self):
        """Reject dialog (Escape key or external close)."""
        if getattr(self, '_restore_in_progress', False):
            flog("reject: restore in progress, ignoring Escape/reject")
            return
        self.cleanup_resources()
        super().reject()

    def cleanup_resources(self) -> None:
        """Cleanup threads and resources."""
        try:
            # A slot queued before the close (a fetch result, a runner's
            # end) must not start a run on a dialog the user has closed:
            # `_on_version_fetch_done` and `_execute_version_restore` read
            # this. Lowered by the next `recover_and_load`.
            self._closing = True
            self._active_search_trace_id = ""
            self._active_restore_trace_id = ""
            self._close_dialog_read_conn()
            if self._stats_thread is not None:
                self._stats_thread.stop()
                if self._stats_thread.isRunning():
                    self._stats_thread.wait(500)
                self._stats_thread = None
            if hasattr(self, '_stats_debounce_timer'):
                self._stats_debounce_timer.stop()
            if self._version_fetch_thread is not None:
                self._version_fetch_thread.stop()
                if self._version_fetch_thread.isRunning():
                    self._version_fetch_thread.wait(500)
                self._version_fetch_thread = None
            if hasattr(self, 'worker_thread') and self.worker_thread:
                self._disconnect_thread_signals(self.worker_thread)
                if self.worker_thread.isRunning():
                    self.worker_thread.stop()
                self.worker_thread = None

            # Cleanup async restore runner: keep _cancelled_restore_runner so
            # the runner's finished slot still reads its apply_report and
            # partial_restore_report when the tick completes.
            if self._restore_runner is not None:
                self._restore_runner.cancel()
                self._cancelled_restore_runner = self._restore_runner
                self._restore_runner = None
            self._sync_run_controls()

            # Stop progress timer
            if hasattr(self, 'progress_timer') and self.progress_timer.isActive():
                self.progress_timer.stop()
            if hasattr(self, 'logo_label'):
                self.logo_label.stop_recovery_effect()
            if hasattr(self, '_geom_preview'):
                self._geom_preview.clear()
            snap_persist = self._review_snap_mode and self._review_wants_persist
            flog(
                f"cleanup_resources: persist_decision "
                f"snap_mode={self._review_snap_mode} "
                f"wants_persist={self._review_wants_persist} "
                f"snap_persist={snap_persist} "
                f"bar={'yes' if self._review_date_bar is not None else 'none'} "
                f"session={'yes' if self._review_snap_session is not None else 'none'} "
                f"status_widget={'yes' if self._review_status_widget is not None else 'none'}",
                "INFO",
            )
            if snap_persist:
                flog(
                    "cleanup_resources: Review snapshot persisting",
                    "INFO",
                )
                bar = self._review_date_bar
                if bar is not None:
                    from qgis.PyQt.QtCore import QTimer as _QTimer  # noqa: PLC0415
                    _QTimer.singleShot(200, bar._reposition)
                    _QTimer.singleShot(600, bar._reposition)
                    _QTimer.singleShot(1200, bar._reposition)
                    flog("cleanup_resources: date_bar raise scheduled x3", "DEBUG")
            else:
                self._stop_snapshot_mode()

            review_persist = snap_persist or (
                self._review_wants_persist and self._review_snap_session is not None
            )
            if review_persist:
                flog(
                    "cleanup_resources: Review active keeping session alive "
                    f"snap_persist={snap_persist}",
                    "INFO",
                )
            else:
                flog(
                    "cleanup_resources: Review not persisting, tearing down status bar",
                    "INFO",
                )
                self._review_disconnect_auto_refresh()
                self._teardown_review_status_bar()
        except Exception as e:
            flog(f"cleanup_resources error: {e}", "WARNING")

    def _disconnect_thread_signals(self, thread) -> None:
        """Disconnect all pyqtSignal attributes from a thread.

        Introspects the thread instance to find every Qt signal and tries to
        disconnect it. Signals that were never connected raise TypeError /
        RuntimeError; those are logged at DEBUG level and ignored.
        """
        for name in dir(thread):
            if name.startswith('_'):
                continue
            try:
                attr = getattr(thread, name)
            except (AttributeError, RuntimeError):
                continue
            disconnect = getattr(attr, 'disconnect', None)
            if not callable(disconnect):
                continue
            try:
                disconnect()
            except (TypeError, RuntimeError) as exc:
                flog(f"disconnect_thread_signals: {name}: {exc}", "DEBUG")

    def _preflight_layer_checks(self, events):
        """Resolve layers and check capabilities for preflight (BL-QA-004).

        Returns (warnings: List[str], blocking: List[str]).
        """
        from collections import defaultdict
        from .core.restore_executor import preflight_layer_check
        from .core.restore_planner import plan_event_restore

        warnings = []
        blocking = []

        by_ds = defaultdict(list)
        for e in events:
            by_ds[e.datasource_fingerprint].append(e)

        read_conn = self._get_dialog_read_conn()
        for fp, group in by_ds.items():
            name = group[0].layer_name_snapshot or fp
            layer = find_target_layer(group[0], read_conn)
            if layer is None:
                # A loaded layer REFUSED as the target (history captured
                # before v7 without its connection service, layer re-pointed
                # to another table) is not a missing layer: the reason is
                # what tells the user what to do.
                from .core.workflow_service import target_refusal_reason
                reason = target_refusal_reason(group[0])
                if reason:
                    blocking.append(self.tr(
                        "Couche '{name}' refusee comme cible : {reason}."
                    ).format(name=name, reason=reason))
                else:
                    blocking.append(self.tr(
                        "Couche '{name}' introuvable dans le projet."
                    ).format(name=name))
                continue

            if hasattr(layer, 'isEditable') and layer.isEditable():
                if hasattr(layer, 'isModified') and layer.isModified():
                    blocking.append(
                        self.tr("Couche '{name}' a des modifications non sauvegardees.").format(
                            name=name))
                    continue

            mini_plan = plan_event_restore(group, fp, name)
            issues = preflight_layer_check(mini_plan, layer)
            for issue in issues:
                blocking.append(f"{name}: {issue}")

        return warnings, blocking

    def validate_inputs(self):
        """Validate inputs with exploitable messages (UX-H01)."""
        if self._journal is None or not self._journal.is_open:
            qlog(self.tr(
                "Aucun journal local disponible. "
                "Ouvrez un projet QGIS pour activer l'enregistrement."
            ), "WARNING")
            return False
        if self.restore_mode_selector.mode() == "temporal":
            cutoff_dt = self.time_slider.cutoff_datetime()
            if not cutoff_dt.isValid():
                qlog(self.tr("Date de retour invalide. Deplacez le curseur temporel."), "WARNING")
                return False
            return True
        start_qdt = self.start_input.dateTime()
        end_qdt = self.end_input.dateTime()
        if not start_qdt.isValid() or not end_qdt.isValid():
            qlog(self.tr("Dates invalides. Selectionnez une periode valide."), "WARNING")
            return False
        start_datetime = start_qdt.toPyDateTime()
        end_datetime = end_qdt.toPyDateTime()
        if start_datetime >= end_datetime:
            qlog(self.tr(
                "La date de debut doit etre anterieure a la date de fin. "
                "Ajustez les dates ou utilisez un raccourci de periode."
            ), "WARNING")
            return False
        return True

    def _humanize_error(self, what: str, technical: str) -> str:
        """UX-H01: Convert technical error to user-friendly message."""
        lower = technical.lower()
        if "connection" in lower or "connect" in lower:
            return self.tr("{what} : connexion au journal impossible. "
                           "Verifiez que le fichier est accessible.").format(what=what)
        if "locked" in lower or "busy" in lower:
            return self.tr("{what} : le journal est verrouille par un autre processus. "
                           "Fermez les autres instances et reessayez.").format(what=what)
        if "disk" in lower or "space" in lower or "full" in lower:
            return self.tr("{what} : espace disque insuffisant. "
                           "Liberez de l'espace ou purgez les anciens evenements.").format(what=what)
        if "permission" in lower or "access" in lower:
            return self.tr("{what} : acces refuse au fichier journal. "
                           "Verifiez les permissions du dossier.").format(what=what)
        if "corrupt" in lower or "malformed" in lower:
            return self.tr("{what} : le journal semble endommage. "
                           "Ouvrez la maintenance pour verifier l'integrite.").format(what=what)
        return f"{what} : {technical}"
