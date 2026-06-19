from qgis.PyQt.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                                 QComboBox, QMessageBox, QProgressBar,
                                 QCheckBox, QApplication,
                                 QTableWidget, QTableWidgetItem, QLineEdit,
                                 QGraphicsDropShadowEffect, QWidget,
                                 QScrollArea, QFrame, QMenu, QShortcut,
                                 QStackedWidget, QListWidget, QListWidgetItem)
from qgis.PyQt.QtCore import (QDateTime, QDate, QTime, QTimer,
                              QVariantAnimation)
from qgis.PyQt.QtGui import QIcon, QColor, QKeySequence
from qgis.core import (
    QgsVectorLayer, QgsProject, QgsApplication, QgsSettings,
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
                      ActionButtonBar)
import os
import json
import time
import uuid

CHANGE_TYPE_COLORS = {
    "modified":  QColor(66, 133, 244, 60),
    "emptied":   QColor(219, 68, 55, 55),
    "populated": QColor(52, 168, 83, 55),
    "geometry":  QColor(255, 152, 0, 60),
}


def _change_type_labels():
    from qgis.PyQt.QtCore import QCoreApplication

    def _tr(msg):
        return QCoreApplication.translate("RecoverDialog", msg)
    return {
        "modified":  _tr("Valeur modifiee"),
        "emptied":   _tr("Valeur videe"),
        "populated": _tr("Valeur ajoutee"),
        "geometry":  _tr("Geometrie modifiee"),
    }


_MIN_RECOVER_ANIMATION_SEC = 3.0
_MIN_RESTORE_ANIMATION_SEC = 1.8


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
        "_pending_rewind_events",
        "_active_restore_trace_id",
        "_restore_runner",
    )

    def __setattr__(self, name: str, value):
        if name in type(self)._WATCHED_ATTRS:
            try:
                old = self.__dict__.get(name, "<unset>")
                log_state_transition("RecoverDialog", name, old, value)
            except Exception:  # pragma: no cover - logging never breaks setters
                pass
        super().__setattr__(name, value)

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
        self._last_restore_events = None
        self._last_restore_by_ds = None
        self._pending_rewind_events = None
        self._pending_cycle_stats = None
        self._active_search_trace_id = ""
        self._active_restore_trace_id = ""
        self._dialog_read_conn = None
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
            self.tracking_label.setText(self.tr("Enregistrement désactivé"))
            self.tracking_label.setStyleSheet("color: #e74c3c; font-weight: 600;")
        self._refresh_smart_bar()
        self._check_onboarding()
        self._check_disk_space()

    def _cancel_snap_worker(self, disconnect_signals: bool = False) -> None:
        """Cancel current snap worker and park it in zombie list for reaping."""
        worker = self._review_snap_worker
        if worker is None:
            return
        if disconnect_signals:
            try:
                worker.result_ready.disconnect()
                worker.error.disconnect()
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
            except Exception:
                pass
            self._dialog_read_conn = None

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
        if enabled:
            self.tracking_label.setText(self.tr("Enregistrement actif"))
            self.tracking_label.setStyleSheet("color: #2ecc71; font-weight: 600;")
            qlog(self.tr("Enregistrement des modifications active."))
        else:
            self.tracking_label.setText(self.tr("Enregistrement désactivé"))
            self.tracking_label.setStyleSheet("color: #e74c3c; font-weight: 600;")
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
            checked = (not allowed) or (layer_fp in allowed)
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
            n = sum(1 for lyr in layers if compute_datasource_fingerprint(lyr) in allowed)
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
        pal = self.palette()
        hl = pal.highlight().color()
        mid = pal.mid().color()
        base = pal.base().color()
        text = pal.text().color()
        hl_text = pal.highlightedText().color()
        border = f"rgba({mid.red()},{mid.green()},{mid.blue()},120)"
        bg = f"rgba({base.red()},{base.green()},{base.blue()},220)"
        hover_bg = f"rgba({base.red()},{base.green()},{base.blue()},255)"
        pressed_bg = f"rgba({hl.red()},{hl.green()},{hl.blue()},210)"
        checked_bg = f"rgba({hl.red()},{hl.green()},{hl.blue()},200)"
        dis_text = f"rgba({text.red()},{text.green()},{text.blue()},90)"
        dis_border = f"rgba({mid.red()},{mid.green()},{mid.blue()},60)"
        dis_bg = f"rgba({base.red()},{base.green()},{base.blue()},120)"
        self.setStyleSheet(
            "QPushButton {"
            f"  background-color: {bg};"
            f"  color: palette(text);"
            f"  border: 1px solid {border};"
            f"  border-radius: 6px;"
            f"  padding: 5px 14px;"
            "}"
            "QPushButton:hover {"
            f"  background-color: {hover_bg};"
            "}"
            "QPushButton:pressed {"
            f"  background-color: {pressed_bg};"
            f"  color: {hl_text.name()};"
            f"  border: none;"
            f"  font-weight: bold;"
            "}"
            "QPushButton:checked {"
            f"  background-color: {checked_bg};"
            f"  color: {hl_text.name()};"
            f"  border: none;"
            f"  font-weight: bold;"
            "}"
            "QPushButton:disabled {"
            f"  color: {dis_text};"
            f"  border: 1px solid {dis_border};"
            f"  background-color: {dis_bg};"
            "}"
        )

    def setup_ui(self):

        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)
        logo_label = ThemedLogoWidget()
        logo_label.setSizePolicy(QtCompat.SIZE_FIXED, QtCompat.SIZE_FIXED)
        self.logo_label = logo_label
        self._load_themed_logo()
        # Status Frame (Sleek modern header instead of QFormLayout GroupBox)
        status_frame = QFrame()
        status_frame.setObjectName("statusFrame")
        status_frame.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        status_layout = QHBoxLayout(status_frame)
        status_layout.setContentsMargins(4, 4, 4, 4)
        status_layout.setSpacing(8)

        self.smart_bar = JournalInfoBar()
        self.smart_bar.metricActivated.connect(self._on_smart_bar_metric_activated)
        self.smart_bar.maintenanceRequested.connect(self._open_maintenance)

        tracking_panel = QWidget()
        tracking_panel.setSizePolicy(QtCompat.SIZE_FIXED, QtCompat.SIZE_FIXED)
        tracking_panel.setStyleSheet("background: transparent;")
        tracking_layout = QHBoxLayout(tracking_panel)
        tracking_layout.setContentsMargins(4, 0, 0, 0)
        tracking_layout.setSpacing(6)

        self.tracking_label = QLabel(self.tr("Enregistrement actif"))
        self.tracking_label.setStyleSheet("font-weight: bold;")
        self.tracking_toggle = AppleToggleSwitch()
        self.tracking_toggle.toggled.connect(self._on_tracking_toggled)

        tracking_layout.addWidget(self.tracking_label)
        tracking_layout.addWidget(self.tracking_toggle)

        status_layout.addWidget(self.smart_bar, 1)
        self.smart_bar.add_trailing_widget(tracking_panel)

        layers_group = QgsCollapsibleGroupBox()
        layers_group.setTitle(self.tr("Couches surveillées"))
        layers_group.setCollapsed(True)
        layers_vbox = QVBoxLayout()
        layers_vbox.setSpacing(6)

        layers_header = QHBoxLayout()
        self.layers_status_label = QLabel(self.tr("Toutes les couches surveillées"))
        self.layers_status_label.setStyleSheet("font-style: italic; color: #555;")
        refresh_layers_btn = QPushButton()
        refresh_layers_btn.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        refresh_layers_btn.setFixedSize(24, 24)
        refresh_layers_btn.setToolTip(self.tr("Rafraîchir la liste"))
        refresh_layers_btn.clicked.connect(self._refresh_layers_panel)
        layers_header.addWidget(self.layers_status_label, 1)
        layers_header.addWidget(refresh_layers_btn)

        self.layers_scroll = QScrollArea()
        self.layers_scroll.setWidgetResizable(True)
        self.layers_scroll.setMaximumHeight(160)
        self.layers_scroll.setStyleSheet("QScrollArea { border: none; }")
        self.layers_container = QWidget()
        self.layers_container_layout = QVBoxLayout()
        self.layers_container_layout.setSpacing(3)
        self.layers_container_layout.setContentsMargins(4, 4, 4, 4)
        self.layers_container_layout.addStretch()
        self.layers_container.setLayout(self.layers_container_layout)
        self.layers_scroll.setWidget(self.layers_container)

        layers_vbox.addLayout(layers_header)
        layers_vbox.addWidget(self.layers_scroll)
        layers_group.setLayout(layers_vbox)

        selection_group = QgsCollapsibleGroupBox()
        selection_group.setTitle(self.tr("Sélection"))
        selection_outer = QVBoxLayout()
        selection_outer.setSpacing(6)

        selection_row = QHBoxLayout()
        selection_row.setSpacing(10)

        self.layer_input = QComboBox()
        self.layer_input.setToolTip(self.tr("Couche dont les modifications sont enregistrées dans le journal local"))
        self.layer_input.currentIndexChanged.connect(self._on_layer_changed)

        self._version_layer_list = QListWidget()
        self._version_layer_list.setToolTip(self.tr("Cochez les couches a restaurer"))
        self._version_layer_list.setMaximumHeight(120)
        self._version_layer_list.setStyleSheet("QListWidget { border: 1px solid palette(mid); border-radius: 3px; }")
        self._version_layer_btns = QWidget()
        vl_btn_layout = QHBoxLayout(self._version_layer_btns)
        vl_btn_layout.setContentsMargins(0, 0, 0, 0)
        vl_btn_layout.setSpacing(4)
        vl_btn_layout.addStretch()
        self._vl_select_all_btn = QPushButton()
        self._vl_select_all_btn.setIcon(QgsApplication.getThemeIcon('/mActionSelectAll.svg'))
        self._vl_select_all_btn.setToolTip(self.tr("Tout selectionner"))
        self._vl_select_all_btn.setFixedSize(26, 26)
        self._vl_select_all_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._vl_select_all_btn.clicked.connect(self._version_layers_check_all)
        self._vl_deselect_btn = QPushButton()
        self._vl_deselect_btn.setIcon(QgsApplication.getThemeIcon('/mActionDeselectAll.svg'))
        self._vl_deselect_btn.setToolTip(self.tr("Tout deselectionner"))
        self._vl_deselect_btn.setFixedSize(26, 26)
        self._vl_deselect_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._vl_deselect_btn.clicked.connect(self._version_layers_uncheck_all)
        vl_btn_layout.addWidget(self._vl_select_all_btn)
        vl_btn_layout.addWidget(self._vl_deselect_btn)
        self.operation_input = QComboBox()
        self.operation_input.addItem(self.tr("Toutes"), "ALL")
        self.operation_input.setToolTip(self.tr("Type d'opération à rechercher"))
        self.operation_input.currentIndexChanged.connect(self._refresh_smart_bar)

        layer_label = QLabel(self.tr("Couche:"))
        op_label = QLabel(self.tr("Opération:"))
        selection_row.addWidget(layer_label)
        selection_row.addWidget(self.layer_input, 3)
        selection_row.addWidget(op_label)
        selection_row.addWidget(self.operation_input, 1)
        selection_outer.addLayout(selection_row)

        _vl_page = QWidget()
        _vl_lay = QVBoxLayout(_vl_page)
        _vl_lay.setContentsMargins(0, 0, 0, 0)
        _vl_lay.setSpacing(6)
        _vl_lay.addWidget(self._version_layer_list)
        _vl_lay.addWidget(self._version_layer_btns)

        self._layer_sel_stack = QStackedWidget()
        self._layer_sel_stack.addWidget(QWidget())  # page 0: event
        self._layer_sel_stack.addWidget(_vl_page)    # page 1: version/review
        _list_h = self._version_layer_list.maximumHeight()
        _btn_h = self._vl_select_all_btn.height() + 6
        self._layer_sel_stack.setFixedHeight(_list_h + _btn_h)
        self._layer_sel_stack.setCurrentIndex(0)
        selection_outer.addWidget(self._layer_sel_stack)
        selection_group.setLayout(selection_outer)
        date_group = QgsCollapsibleGroupBox()
        date_group.setTitle(self.tr("Période"))
        date_layout = QVBoxLayout()
        date_layout.setSpacing(6)

        self.restore_mode_selector = RestoreModeSelector()
        date_layout.addWidget(self.restore_mode_selector)

        self._period_stack = QStackedWidget()

        event_page = QWidget()
        event_vbox = QVBoxLayout(event_page)
        event_vbox.setContentsMargins(0, 0, 0, 0)
        event_vbox.setSpacing(6)

        today = QDateTime.currentDateTime()
        self.start_input = QgsDateTimeEdit()
        self.start_input.setDateTime(today.addDays(-3650))
        self.start_input.setDisplayFormat("dd/MM/yyyy HH:mm")
        self.start_input.dateTimeChanged.connect(self._validate_dates)
        self.start_input.dateTimeChanged.connect(self._refresh_smart_bar)

        self.end_input = QgsDateTimeEdit()
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

        date_shortcuts = QHBoxLayout()
        date_shortcuts.setSpacing(8)
        shortcut_button_width = 90
        shortcut_button_height = 28

        clock_icon = QgsApplication.getThemeIcon('/mIconClock.svg')

        min10_btn = QPushButton("10 min")
        min10_btn.setIcon(clock_icon)
        min10_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        min10_btn.clicked.connect(lambda: self.set_period("10min"))
        min10_btn.setToolTip(self.tr("Dernieres 10 minutes"))

        min30_btn = QPushButton("30 min")
        min30_btn.setIcon(clock_icon)
        min30_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        min30_btn.clicked.connect(lambda: self.set_period("30min"))
        min30_btn.setToolTip(self.tr("Dernieres 30 minutes"))

        hour1_btn = QPushButton(self.tr("1 heure"))
        hour1_btn.setIcon(clock_icon)
        hour1_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        hour1_btn.clicked.connect(lambda: self.set_period("1hour"))
        hour1_btn.setToolTip(self.tr("Derniere heure"))

        day1_btn = QPushButton(self.tr("1 jour"))
        day1_btn.setIcon(clock_icon)
        day1_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        day1_btn.clicked.connect(lambda: self.set_period("1day"))
        day1_btn.setToolTip(self.tr("Dernieres 24 heures"))

        today_btn = QPushButton(self.tr("Aujourd'hui"))
        today_btn.setIcon(clock_icon)
        today_btn.setFixedSize(shortcut_button_width + 10, shortcut_button_height)
        today_btn.clicked.connect(lambda: self.set_period("today"))
        today_btn.setToolTip(self.tr("Depuis minuit aujourd'hui"))

        week_btn = QPushButton(self.tr("Semaine"))
        week_btn.setIcon(clock_icon)
        week_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        week_btn.clicked.connect(lambda: self.set_period("week"))
        week_btn.setToolTip(self.tr("Depuis lundi 00:00"))

        all_btn = QPushButton(self.tr("Tout"))
        all_btn.setIcon(clock_icon)
        all_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        all_btn.clicked.connect(lambda: self.set_period("all"))
        all_btn.setToolTip(self.tr("Tout l'historique du journal"))

        date_shortcuts.addWidget(min10_btn)
        date_shortcuts.addWidget(min30_btn)
        date_shortcuts.addWidget(hour1_btn)
        date_shortcuts.addWidget(day1_btn)
        date_shortcuts.addWidget(today_btn)
        date_shortcuts.addWidget(week_btn)
        date_shortcuts.addWidget(all_btn)
        date_shortcuts.addStretch()

        self._dates_container = QWidget()
        _dc_lay = QVBoxLayout(self._dates_container)
        _dc_lay.setContentsMargins(0, 0, 0, 0)
        _dc_lay.setSpacing(6)
        _dc_lay.addLayout(dates_row)
        _dc_lay.addLayout(date_shortcuts)
        event_vbox.addWidget(self._dates_container)

        self.time_slider = TimeSliderWidget()

        self._period_stack.addWidget(event_page)
        self._period_stack.addWidget(self.time_slider)
        self._period_stack.setCurrentIndex(0)

        self.restore_mode_selector.modeChanged.connect(self._on_period_mode_changed)

        date_layout.addWidget(self._period_stack)
        date_group.setLayout(date_layout)
        options_group = QgsCollapsibleGroupBox(self.tr("Options"))
        options_layout = QHBoxLayout()
        options_layout.setSpacing(20)

        self.auto_zoom_check = QCheckBox(self.tr("Zoomer automatiquement sur les résultats"))
        self.auto_zoom_check.setIcon(QgsApplication.getThemeIcon('/mActionZoomToSelected.svg'))
        self.auto_zoom_check.setChecked(True)

        self.open_attribute_check = QCheckBox(self.tr("Ouvrir la table d'attributs après chargement"))
        self.open_attribute_check.setIcon(QgsApplication.getThemeIcon('/mActionOpenTable.svg'))
        self.open_attribute_check.setChecked(False)

        options_layout.addWidget(self.auto_zoom_check)
        options_layout.addWidget(self.open_attribute_check)
        options_layout.addStretch()
        options_group.setLayout(options_layout)
        results_group = QgsCollapsibleGroupBox()
        results_group.setTitle(self.tr("Résultats"))
        results_group.setCollapsed(True)
        results_layout = QVBoxLayout()
        self.results_info_label = QLabel(self.tr("Aucune donnée récupérée"))

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
        self.table_widget.setMinimumHeight(100)
        self.table_widget.setSortingEnabled(True)
        self.table_widget.setSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_PREFERRED)
        self.table_widget.itemSelectionChanged.connect(self.on_selection_changed)
        self.table_widget.setContextMenuPolicy(QtCompat.CUSTOM_CONTEXT_MENU)
        self.table_widget.customContextMenuRequested.connect(self._show_results_context_menu)
        selection_buttons_layout = QHBoxLayout()
        selection_buttons_layout.setSpacing(8)
        selection_button_height = 32
        self.select_all_button = QPushButton(self.tr("Tout sélectionner"))
        select_all_icon = QgsApplication.getThemeIcon('/mActionSelectAll.svg')
        self.select_all_button.setIcon(select_all_icon)
        self.select_all_button.setFixedHeight(selection_button_height)
        self.select_all_button.clicked.connect(self.select_all_rows)
        self.select_all_button.setEnabled(False)
        self.select_all_button.setToolTip(self.tr("Sélectionner toutes les lignes du tableau"))
        self.select_none_button = QPushButton(self.tr("Tout désélectionner"))
        deselect_icon = QgsApplication.getThemeIcon('/mActionDeselectAll.svg')
        self.select_none_button.setIcon(deselect_icon)
        self.select_none_button.setFixedHeight(selection_button_height)
        self.select_none_button.clicked.connect(self.select_none_rows)
        self.select_none_button.setEnabled(False)
        self.select_none_button.setToolTip(self.tr("Désélectionner toutes les lignes du tableau"))

        self.selection_count_label = QLabel("")
        selection_buttons_layout.addWidget(self.select_all_button)
        selection_buttons_layout.addWidget(self.select_none_button)
        selection_buttons_layout.addStretch()
        selection_buttons_layout.addWidget(self.selection_count_label)

        results_layout.addWidget(self.results_info_label)
        results_layout.addWidget(self.search_filter)
        results_layout.addWidget(self.change_legend)
        results_layout.addWidget(self.table_widget)
        results_layout.addLayout(selection_buttons_layout)
        results_group.setLayout(results_layout)

        self.results_group = results_group

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(False)
        # Smoothing animation: interpolates progress_bar value between
        # successive emits from the restore runner. The runner only emits
        # once per chunk (~20 events), so the bar would otherwise jump in
        # ~5% increments. This QVariantAnimation runs on Qt's global
        # animation scheduler (same as the logo widget), so progress
        # smoothing and the logo animation share a single clock and do not
        # compete for QTimer slots in the UI thread.
        self._progress_smoother = QVariantAnimation(self)
        self._progress_smoother.setDuration(300)
        self._progress_smoother.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        self._progress_smoother.valueChanged.connect(
            self._on_progress_smoother_value)
        self.progress_phase_label = QLabel("")
        self.progress_phase_label.setVisible(False)
        flog(
            f"setup_ui: progress_bar created, id={id(self.progress_bar)},"
            f" visible={self.progress_bar.isVisible()},"
            f" parent={self.progress_bar.parent()}"
        )

        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        button_height = 35

        self._action_bar = ActionButtonBar(
            [
                {
                    "label": self.tr("Fermer"),
                    "icon": QgsApplication.getThemeIcon('/mActionRemove.svg'),
                    "tooltip": self.tr("Fermer"),
                    "enabled": True,
                    "visible": True,
                },
                {
                    "label": "Recover",
                    "icon": QgsApplication.getThemeIcon('/mActionRefresh.svg'),
                    "tooltip": self.tr("Lancer la recherche (F5)"),
                    "enabled": True,
                    "visible": True,
                },
                {
                    "label": "Restore",
                    "icon": QgsApplication.getThemeIcon('/mActionSaveAllEdits.svg'),
                    "tooltip": "",
                    "enabled": False,
                    "visible": True,
                },
                {
                    "label": "Last",
                    "icon": QgsApplication.getThemeIcon('/mActionUndo.svg'),
                    "tooltip": self.tr("Annuler le dernier restore"),
                    "enabled": False,
                    "visible": True,
                },
            ],
            self,
        )
        self._action_bar.setPrimaryIndex(-1)
        self.cancel_button = self._action_bar.segment(0)
        self.recover_button = self._action_bar.segment(1)
        self.restore_button = self._action_bar.segment(2)
        self.undo_last_btn = self._action_bar.segment(3)
        self.cancel_button.clicked.connect(self.cancel_operation)
        self.recover_button.clicked.connect(self.recover_and_load)
        self.restore_button.clicked.connect(self.restore_selected_data)
        self.undo_last_btn.clicked.connect(self._undo_last_restore)

        pal_hl = self.palette().highlight().color()
        glow_color = QColor(pal_hl.red(), pal_hl.green(), pal_hl.blue(), 180)
        self._glow_color = glow_color
        self._apply_logo_glow_effect()

        self.help_button = QPushButton("?")
        self.help_button.setFixedSize(button_height, button_height)
        self.help_button.setToolTip(self.tr("Ouvrir la documentation"))
        self.help_button.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.help_button.setStyleSheet(
            "QPushButton { font-weight: bold; }"
        )
        self.help_button.clicked.connect(self._open_help)

        self._review_toggle = ReviewSegmentedSwitch(self)
        self._review_toggle.setToolTip(self.tr("Activer/desactiver la visualisation Review"))
        self._review_toggle.setVisible(False)
        self._review_toggle.toggled.connect(self._on_review_toggle)

        self._review_viz_combo = QComboBox(self)
        self._review_viz_combo.addItem(self.tr("Snapshot (état à T)"), "snapshot_at_t")
        self._review_viz_combo.setVisible(False)

        button_layout.addWidget(self.help_button)
        button_layout.addStretch()
        button_layout.addWidget(self._action_bar)
        button_layout.addWidget(self._review_viz_combo)
        button_layout.addWidget(self._review_toggle)
        button_layout.addStretch()
        right_spacer = QWidget(self)
        right_spacer.setFixedSize(button_height, button_height)
        button_layout.addWidget(right_spacer)
        main_layout.addWidget(logo_label, 0, QtCompat.ALIGN_HCENTER)
        main_layout.addWidget(status_frame)
        main_layout.addWidget(layers_group)
        layers_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        layers_group.setMinimumHeight(0)
        main_layout.addWidget(selection_group)
        main_layout.addWidget(date_group)
        main_layout.addWidget(options_group)
        main_layout.addWidget(self.results_group, 1)
        main_layout.addWidget(self.progress_phase_label)
        main_layout.addWidget(self.progress_bar)
        main_layout.addLayout(button_layout)
        selection_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        selection_group.setMinimumHeight(0)

        date_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        date_group.setMinimumHeight(0)

        options_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        options_group.setMinimumHeight(0)

        self.results_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_EXPANDING)
        self.results_group.setMinimumHeight(0)

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
                QgsApplication.getThemeIcon('/mActionSaveAllEdits.svg'),
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
        menu.addSeparator()
        sel_all = menu.addAction(self.tr("Tout selectionner"))
        sel_all.triggered.connect(self.select_all_rows)
        desel = menu.addAction(self.tr("Tout deselectionner"))
        desel.triggered.connect(self.select_none_rows)
        global_pos = self.table_widget.viewport().mapToGlobal(pos)
        if hasattr(menu, 'exec'):
            menu.exec(global_pos)
        else:
            menu.exec_(global_pos)

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
        if hasattr(dlg, 'exec'):
            dlg.exec()
        else:
            dlg.exec_()
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
        effect = QGraphicsDropShadowEffect(self.logo_label)
        effect.setBlurRadius(0)
        effect.setOffset(0, 0)
        gc = self._glow_color
        effect.setColor(QColor(gc.red(), gc.green(), gc.blue(), 0))
        self.logo_label.setGraphicsEffect(effect)
        self.logo_label.setProperty("glow_color", gc)
        self.logo_label.setProperty("glow_base_blur", 0)
        self.logo_label.setProperty("glow_hover_blur", 28)
        self.logo_label.setProperty("glow_base_alpha", 0)
        self.logo_label.setProperty("glow_hover_alpha", min(255, gc.alpha() + 75))

    def _start_logo_activity(self, color: QColor, mode: str) -> None:
        if mode == "recover":
            self.logo_label.start_recovery_effect(color)
        elif mode == "restore":
            self.logo_label.start_restore_effect(color)
        else:
            raise ValueError(f"Unknown logo activity mode: {mode}")
        self._pulse_logo_glow(color)

    def _stop_logo_activity(self) -> None:
        if not hasattr(self, 'logo_label'):
            return
        self.logo_label.stop_recovery_effect()
        self._animate_glow(self.logo_label, hover_in=False)

    def _pulse_logo_glow(self, color: QColor) -> None:
        effect = self.logo_label.graphicsEffect()
        if not isinstance(effect, QGraphicsDropShadowEffect):
            self._apply_logo_glow_effect()
            effect = self.logo_label.graphicsEffect()
        if not isinstance(effect, QGraphicsDropShadowEffect):
            return

        effect.setBlurRadius(0)
        effect.setOffset(0, 0)
        effect.setColor(QColor(color.red(), color.green(), color.blue(), 0))
        self.logo_label.setProperty("glow_color", color)
        self.logo_label.setProperty("glow_base_blur", 0)
        self.logo_label.setProperty("glow_hover_blur", 28)
        self.logo_label.setProperty("glow_base_alpha", 0)
        self.logo_label.setProperty("glow_hover_alpha", min(255, color.alpha() + 75))

        self._animate_glow(self.logo_label, hover_in=True)
        QTimer.singleShot(260, lambda: self._animate_glow(self.logo_label, hover_in=False))

    def changeEvent(self, event):
        """Re-render logo when QGIS theme/palette changes (dark mode)."""
        super().changeEvent(event)
        if getattr(self, '_refreshing_theme', False):
            return
        if event.type() == QtCompat.EVENT_PALETTE_CHANGE and hasattr(self, 'logo_label'):
            self._refreshing_theme = True
            try:
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
        other identity fields (A3 of US-03 — PII discipline).
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
            self.recover_button.setIcon(QgsApplication.getThemeIcon('/mActionUndo.svg'))
            self._action_bar.setPrimaryIndex(1)
            if not self._is_recovering:
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
            self.recover_button.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
            self._action_bar.setPrimaryIndex(2)

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
        results — the root cause of "rewind broken after purge".
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
        flog(f"set_period: called period={period} start_enabled={self.start_input.isEnabled()} end_enabled={self.end_input.isEnabled()}", "DEBUG")
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
        """Pulse progress bar to indeterminate mode.

        Called by progress_timer (QTimer). No processEvents needed;
        the timer fires within the Qt event loop.
        """
        value = self.progress_bar.value()
        if value == 0:
            self.progress_bar.setRange(0, 0)

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
        # The smoother is only used when the bar is in determinate mode;
        # if it was switched to indeterminate (maximum==0) mid-animation,
        # silently drop the update to avoid resetting the range.
        if self.progress_bar.maximum() == 0:
            return
        self.progress_bar.setValue(int(round(float(val))))

    def _smooth_set_progress(self, target_value: int) -> None:
        """Animate progress_bar from its current value to ``target_value``.

        Behaviour matrix:
        - indeterminate mode (max==0) ........ switch to determinate first
        - target == current displayed value .. no-op
        - target <= 0 or target >= 100 ....... immediate (reset / done),
          stop any in-flight animation to avoid an over-shoot afterwards
        - any other target ................... 300 ms eased interpolation;
          if a previous animation is still running, restart from the
          currently displayed value so the visual stays continuous.
        """
        if self.progress_bar.maximum() == 0:
            self.progress_bar.setRange(0, 100)
        current = self.progress_bar.value()
        if target_value == current:
            return
        running = (self._progress_smoother.state()
                   == QtCompat.ANIM_STATE_RUNNING)
        if target_value <= 0 or target_value >= 100:
            if running:
                self._progress_smoother.stop()
            self.progress_bar.setValue(target_value)
            return
        if running:
            self._progress_smoother.stop()
            current = self.progress_bar.value()
        self._progress_smoother.setStartValue(float(current))
        self._progress_smoother.setEndValue(float(target_value))
        self._progress_smoother.start()

    def on_events_committed(self, edited_fingerprint="") -> None:
        """Auto-refresh smart bar and event table after new events are flushed.

        Called by the plugin after a short delay post-commit so the
        WriteQueue has time to flush.
        """
        if not self.isVisible():
            return
        if self._is_recovering:
            return
        self._invalidate_undo_for(edited_fingerprint)
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
            except Exception:  # noqa: BLE001
                continue
        return None

    def _recover_review_mode(self) -> None:
        """Review toggle: start a session (cache + persistent overlays).

        1. Load ALL events into RAM cache (background QThread).
        2. Create persistent overlay layers ONCE.
        3. Render visible features from cache.
        4. On zoom/pan: re-filter cache (~20-80ms, no SQL).
        """
        canvas = self.iface.mapCanvas()
        src_crs = canvas.mapSettings().destinationCrs().authid()

        checked_fps = self._get_checked_layer_fingerprints()
        if not checked_fps:
            self._review_done("--------", "no layer checked")
            return

        t_min = self.start_input.dateTime().toUTC().toString("yyyy-MM-ddTHH:mm:ss")
        t_max = self.end_input.dateTime().toUTC().toString("yyyy-MM-ddTHH:mm:ss")

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
            storage_crs = layer.crs().authid() if layer.crs().isValid() else src_crs
            layer_infos.append({
                "fingerprint": fingerprint,
                "layer_name": layer.name(),
                "layer_id": layer.id(),
                "storage_crs": storage_crs,
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
        self._is_recovering = False
        self.enable_controls(True)
        self.update_phase("")
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

        snap = SnapshotOverlaySession()
        self._review_snap_session = snap
        self._review_snap_mode = True

        today = _dt.date.today().isoformat()
        first_iso = ((_dt.date.today() - _dt.timedelta(days=5 * 365)).isoformat()
                     + "T00:00:00")
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
            if not self._review_snap_mode:
                return
            self.update_phase("")
            flog(
                f"[{trace_id}] review: snapshot_ready n_layers={snap.n_layers}",
                "INFO",
            )
            QTimer.singleShot(100, lambda: self._deferred_set_bar_range(layer_infos))
            self._on_snapshot_date_changed(bar.current_date_iso())

        snap.start_async_create(layer_infos, src_crs, trace_id, _on_layers_ready)

    def _deferred_set_bar_range(self, layer_infos: list) -> None:
        """Run date-range SQL off the critical path — updates bar after first paint."""
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
        self._cancel_snap_worker()
        flog("review: snap_worker cancelled", "DEBUG")
        self._cancel_marker_worker()
        self._review_marker_bbox_signature = ""
        flog("review: marker_worker cancelled", "DEBUG")
        if self._review_date_bar is not None:
            try:
                self.iface.mapCanvas().mapCanvasRefreshed.disconnect(
                    self._review_date_bar.raise_safe
                )
            except Exception:
                pass
            self._review_date_bar.cleanup()
            self._review_date_bar = None
            flog("review: date_bar cleaned up", "INFO")
            try:
                self.iface.mapCanvas().update()
            except Exception:
                pass
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
        flog("review: snapshot_mode_stopped", "INFO")

    def _clamp_cutoff_to_baseline(self, iso: str) -> str:
        """Never request a cutoff earlier than the global tracking start (t0).

        Scrubbing before t0 lands in the empty 'pre-tracking void' where every
        layer is flagged before-baseline (noisy). We snap the requested cutoff
        (and the visible slider) up to t0 instead (RL-E1-03).
        """
        t0_iso = self._review_global_t0_iso
        if not t0_iso:
            return iso
        req_dt = self._parse_iso_datetime(iso)
        t0_dt = self._parse_iso_datetime(t0_iso)
        if req_dt.isValid() and t0_dt.isValid() and req_dt < t0_dt:
            flog(
                f"review: cutoff_clamped_to_t0 requested={iso} t0={t0_iso}",
                "DEBUG",
            )
            if self._review_date_bar is not None:
                self._review_date_bar.set_value_iso(t0_iso)
            return t0_iso
        return iso

    def _compute_bbox_per_layer(self) -> dict:
        """Map each layer fingerprint → current canvas extent in its storage CRS.

        Cheap (no I/O): a single canvas.extent() read + per-layer CRS transform.
        Used by the apply step on both date change and pan/zoom.
        """
        from qgis.core import (  # noqa: PLC0415
            QgsCoordinateReferenceSystem,
            QgsCoordinateTransform,
            QgsProject,
        )
        canvas = self.iface.mapCanvas()
        extent = canvas.extent()
        project_crs = QgsProject.instance().crs()
        bbox_per_layer: dict = {}
        for _info in self._review_layer_infos:
            _fp = _info["fingerprint"]
            _scrs = _info.get("storage_crs", "")
            if not _scrs or _scrs == project_crs.authid():
                bbox_per_layer[_fp] = extent
            else:
                _lcrs = QgsCoordinateReferenceSystem(_scrs)
                if not _lcrs.isValid():
                    bbox_per_layer[_fp] = extent
                else:
                    _tr = QgsCoordinateTransform(
                        project_crs, _lcrs, QgsProject.instance()
                    )
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
        _on_review_snap_bbox_fire) — that is the core fix for the review-mode
        slowness: panning no longer re-runs SQL + event replay.
        """
        from .widgets.snapshot_rebuild_worker import SnapshotRebuildWorker

        iso = self._clamp_cutoff_to_baseline(iso)
        self._review_snap_pending_iso = iso
        flog(f"review: snapshot_date_changed iso={iso}", "DEBUG")

        self._cancel_snap_worker(disconnect_signals=True)

        if not self._review_snap_mode or self._review_snap_session is None:
            flog("review: snapshot_date_changed skipped mode_inactive", "DEBUG")
            return

        # Same cutoff already reconstructed → no journal re-read, just re-apply
        # the cached result to the current viewport.
        if (self._review_snap_raw_result is not None
                and self._review_snap_raw_iso == iso):
            flog(f"review: snapshot_reuse_cached iso={iso}", "DEBUG")
            self._refresh_snapshot_for_extent()
            return

        if self._review_date_bar is not None:
            self._review_date_bar.set_loading()

        worker = SnapshotRebuildWorker(
            self._journal,
            self._review_layer_infos,
            iso,
            bbox_per_layer=None,
            parent=self,
        )
        worker.result_ready.connect(self._on_snapshot_result)
        worker.error.connect(self._on_snapshot_error)
        self._review_snap_worker = worker
        worker.start()
        flog(
            f"[{worker.trace_id}] review: snap_worker_started iso={iso}",
            "INFO",
        )

    def _on_snapshot_result(self, trace_id: str, result) -> None:
        """Slot: SnapshotRebuildWorker delivered a reconstruction.

        Caches the RAW result (pre-bbox, pre-merge) keyed by date so pan/zoom
        can re-apply it cheaply, then applies it to the current extent.
        """
        self._review_snap_worker = None
        snap = self._review_snap_session
        if snap is None or not self._review_snap_mode:
            flog(f"[{trace_id}] review: snap_result_discarded mode_inactive", "DEBUG")
            return

        self._review_snap_raw_result = result
        self._review_snap_raw_iso = self._review_snap_pending_iso
        self._apply_snapshot_to_extent(result, trace_id)

    def _refresh_snapshot_for_extent(self) -> None:
        """Re-apply the cached reconstruction to the current viewport (pan/zoom).

        Runs on the UI thread but never touches the journal — only the bbox
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

        def _on_update_done(stats: dict) -> None:
            if bar is not None and self._review_snap_mode:
                bar.set_stats(stats["n_entities"], n_total=n_total_before_bbox)
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
        """Slot: user clicked Export — save snapshot overlays to GeoPackage."""
        snap = self._review_snap_session
        bar = self._review_date_bar
        if snap is None or not snap.is_active:
            flog("review: export_requested but no active snapshot session", "WARNING")
            return

        from datetime import datetime as _dt  # noqa: PLC0415
        ts = _dt.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"recoverland_snapshot_{ts}.gpkg"
        iso = bar.current_date_iso() if bar else "?"

        from qgis.PyQt.QtWidgets import QFileDialog, QMessageBox  # noqa: PLC0415
        path, _ = QFileDialog.getSaveFileName(
            self,
            self.tr("Exporter le snapshot vers GeoPackage"),
            default_name,
            "GeoPackage (*.gpkg)",
        )
        if not path:
            return

        flog(
            f"review: snapshot_export_start path={path} iso={iso}",
            "INFO",
        )
        result = snap.export_to_geopackage(path)
        if result["errors"]:
            QMessageBox.warning(
                self,
                self.tr("Export partiel"),
                self.tr("{n} couche(s) en erreur : {errors}").format(
                    n=len(result["errors"]),
                    errors="; ".join(result["errors"]),
                ),
            )
        else:
            QMessageBox.information(
                self,
                self.tr("Export terminé"),
                self.tr(
                    "Snapshot exporté : {n_layers} couche(s), "
                    "{n_features} entité(s) ({elapsed_ms} ms).\n\n{path}"
                ).format(**result, path=path),
            )
        flog(
            f"review: snapshot_export_done n_layers={result['n_layers']} "
            f"n_features={result['n_features']} "
            f"errors={len(result['errors'])} elapsed_ms={result['elapsed_ms']}",
            "INFO",
        )

    def _on_snapshot_error(self, trace_id: str, message: str) -> None:
        """Slot: SnapshotRebuildWorker encountered a fatal error."""
        self._review_snap_worker = None
        if self._review_date_bar is not None:
            self._review_date_bar.set_stats(0)
        flog(
            f"[{trace_id}] review: snap_worker_error msg={message}",
            "ERROR",
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
        self._review_marker_worker = worker
        worker.start()
        flog(
            f"[{trace_id}] review: marker_worker_started "
            f"t_min={t_min} t_max={t_max} n_layers={len(self._review_layer_infos)}",
            "INFO",
        )

    def _on_review_snap_extent_changed(self) -> None:
        """Slot: canvas extent changed — (re)start spatial debounce."""
        if not self._review_snap_mode:
            return
        flog("review: snap_extent_signal_received debounce_alive=%s" % (self._review_snap_ext_debounce is not None), "DEBUG")
        if self._review_snap_ext_debounce is not None:
            self._review_snap_ext_debounce.start()

    def _on_review_snap_bbox_fire(self) -> None:
        """Slot: spatial debounce expired — refresh snapshot for current extent.

        Pan/zoom only changes the viewport, not the cutoff date: re-apply the
        cached reconstruction instead of spawning a worker (no SQL + replay).
        Falls back to a full reload only if no cached result matches the date.
        """
        if not self._review_snap_mode or not self._review_snap_pending_iso:
            return
        if (self._review_snap_raw_result is not None
                and self._review_snap_raw_iso == self._review_snap_pending_iso):
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
        """Insert the Review pill into the QGIS status bar (visible)."""
        if self._review_status_widget is None:
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
        except Exception:  # noqa: BLE001
            pass

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
        if self._tracker is not None:
            try:
                self._tracker.committed_features.connect(self._review_on_extent_changed)
            except (AttributeError, TypeError):
                pass
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
        except TypeError:
            pass
        if self._tracker is not None:
            try:
                self._tracker.committed_features.disconnect(self._review_on_extent_changed)
            except (AttributeError, TypeError):
                pass
        if self._review_status_widget is not None:
            self._review_status_widget.deactivate()
        self._hide_review_status_bar()
        flog("review: auto_refresh disconnected", "INFO")

    def _review_on_extent_changed(self, *_args) -> None:
        """Slot for canvas extentsChanged or journal committed — debounce 600ms."""
        if not self._review_active:
            return
        self._review_debounce.start()

    def _review_auto_refresh(self) -> None:
        """Debounce fire: refresh viewport (snapshot mode re-fires date change)."""
        if not self._review_active:
            return
        if self._review_snap_mode and self._review_snap_pending_iso:
            self._on_snapshot_date_changed(self._review_snap_pending_iso)

    def _recover_event_mode(self) -> None:
        """Event mode: threaded search with start/end date range."""
        self.progress_bar.setVisible(True)
        self.progress_timer.start(50)

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
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mTaskCancel.svg'))

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
        from .core.event_stream_repository import has_active_restore_traces

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
        undo_layers = len(self._last_restore_by_ds) if self._last_restore_by_ds else 0
        undo_events = (sum(len(v) for v in self._last_restore_by_ds.values())
                       if self._last_restore_by_ds else 0)

        # BL-RW-P1-23-A2: if the in-memory undo state is gone (dialog closed /
        # QGIS restarted) but the journal still contains active restore traces,
        # a new rewind would replay compensation on top of an already restored
        # layer.  For FID-only layers this can corrupt or skip features.  Block
        # until the user explicitly undoes the previous restore via the UI.
        if undo_layers == 0:
            read_conn = self._get_dialog_read_conn()
            if read_conn is not None and has_active_restore_traces(
                read_conn, list(checked_fps), trace_id=trace_id
            ):
                flog(f"[{trace_id}] recover_version: BLOCKED active_restore_traces "
                     f"datasource_count={len(checked_fps)}", "WARNING")
                self._is_recovering = False
                self.enable_controls(True)
                self.recover_button.setEnabled(True)
                qlog(self.tr(
                    "Un rewind/restore actif existe pour une couche selectionnee. "
                    "Annulez-le via 'Annuler le dernier restore' avant de rewinder."
                ), "WARNING")
                return

        include_traces = True
        flog(f"[{trace_id}] recover_version: start scope={len(checked_fps)} layer(s) "
             f"cutoff={cutoff_iso} undo_state=layers={undo_layers}/events={undo_events} "
             f"include_traces={include_traces}")

        self.progress_bar.setVisible(True)
        self._start_logo_activity(self._glow_color, "recover")
        self._recover_started_at = time.monotonic()
        self._version_cutoff_dt = cutoff_dt

        if self._version_fetch_thread and self._version_fetch_thread.isRunning():
            self._version_fetch_thread.stop()
            self._version_fetch_thread.wait(500)

        self._version_fetch_thread = VersionFetchThread(
            self._journal, checked_fps, cutoff, trace_id=trace_id,
            include_traces=include_traces)
        self._version_fetch_thread.count_ready.connect(self._on_version_fetch_count_ready)
        self._version_fetch_thread.events_ready.connect(self._on_version_fetch_done)
        self._version_fetch_thread.error_occurred.connect(self._on_version_fetch_error)
        self._version_fetch_thread.start()

    def _on_version_fetch_count_ready(self, total: int) -> None:
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

    def _on_version_fetch_done(self, events) -> None:
        """Handle events fetched by background thread; run confirmations on main thread."""
        from .core.restore_contracts import MAX_EVENTS_PER_RESTORE, WARN_EVENTS_THRESHOLD
        from .core import collapse_rewind_events_with_stats

        raw_count = len(events)
        events, dedup_stats = collapse_rewind_events_with_stats(events)
        total_count = len(events)
        cutoff_dt = getattr(self, '_version_cutoff_dt', None)
        trace_id = self._active_restore_trace_id
        prior_rewind = bool(self._last_restore_by_ds)
        prior_count = (sum(len(v) for v in self._last_restore_by_ds.values())
                       if self._last_restore_by_ds else 0)
        # Stash dedup stats so the runner can fold them into CYCLE_SUMMARY.
        self._pending_cycle_stats = dict(dedup_stats)
        flog(f"[{trace_id}] on_version_fetch_done: raw={raw_count} "
             f"collapsed={total_count} "
             f"prior_rewind={prior_rewind} prior_events={prior_count}")
        for e in events:
            flog(f"  -> active eid={e.event_id} op={e.operation_type} "
                 f"identity={(e.feature_identity_json or '')[:80]}")

        if total_count == 0:
            if self._last_restore_by_ds:
                dropped = dedup_stats.get('dedup_dropped', 0)
                traces_active = dedup_stats.get('traces_active', 0)
                if dropped > 0 and traces_active > 0:
                    flog(f"[{trace_id}] on_version_fetch_done: "
                         f"dedup_dropped={dropped} traces_active={traces_active} "
                         f"events neutralized by prior rewind traces; "
                         f"auto-undo + re-fetch")
                    self._pending_rewind_events = None
                    self._force_refetch_after_undo = True
                    self._auto_undo_for_rewind()
                    return
                n_undo = sum(len(v) for v in self._last_restore_by_ds.values())
                reply = QMessageBox.question(
                    self,
                    self.tr("Revenir a l'etat actuel"),
                    self.tr("Remettre {count} entite(s) dans leur etat actuel ?").format(
                        count=n_undo),
                    QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
                if reply != QtCompat.MSG_YES:
                    self._is_recovering = False
                    self._stop_logo_activity()
                    self.progress_bar.setVisible(False)
                    self.enable_controls(True)
                    self.recover_button.setEnabled(True)
                    return
                flog(f"auto-undo: slider at current date, returning to current state ({n_undo} entities) "
                     f"force_refetch=True (RW-18)")
                self._pending_rewind_events = None
                self._force_refetch_after_undo = True
                self._auto_undo_for_rewind()
                return
            if trace_id:
                self._active_restore_trace_id = ""
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            qlog(self.tr("Aucun evenement apres cette date. Rien a restaurer."))
            return

        if total_count > MAX_EVENTS_PER_RESTORE:
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            if trace_id:
                self._active_restore_trace_id = ""
            qlog(self.tr("{count} evenements depassent la limite ({limit}). "
                         "Choisissez une date plus recente.").format(
                     count=total_count, limit=MAX_EVENTS_PER_RESTORE), "WARNING")
            return

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

        if self._last_restore_by_ds:
            self._pending_rewind_events = events
            flog("auto-undo: previous rewind active, undoing before new rewind")
            self._auto_undo_for_rewind()
            return

        self._execute_version_restore(events)

    def _auto_undo_for_rewind(self) -> None:
        """Silently undo the previous Rewind, then apply the pending new one."""
        by_ds = self._last_restore_by_ds
        total_undo = sum(len(v) for v in by_ds.values()) if by_ds else 0
        flog(f"auto_undo_for_rewind: START layers={len(by_ds) if by_ds else 0} "
             f"events_to_undo={total_undo}")
        for fp, evts in (by_ds or {}).items():
            for e in evts:
                flog(f"  undo op={e.operation_type} eid={e.event_id} "
                     f"identity={(e.feature_identity_json or '')[:80]}")
        self._undo_counts_before = self._rewind_count_snapshot(
            "before_undo", by_ds)
        self._last_restore_events = None
        self._last_restore_by_ds = None
        self.undo_last_btn.setEnabled(False)

        read_conn = self._get_dialog_read_conn()

        def resolver(evt):
            return find_target_layer(evt, read_conn)

        runner = UndoRunner(by_ds, resolver, tracker=self._tracker, parent=self)
        runner.progress.connect(self._on_restore_runner_progress)
        runner.finished.connect(self._on_auto_undo_then_rewind)
        self._restore_runner = runner
        runner.start()

    def _on_auto_undo_then_rewind(self, result) -> None:
        """After auto-undo completes, proceed with the pending Rewind."""
        self._restore_runner = None
        flog(f"auto-undo done: ok={result.total_ok} fail={result.total_fail}")
        if result.total_fail > 0:
            flog(f"auto-undo: {result.total_fail} failures: {result.errors[:5]}", "WARNING")
            qlog(self.tr(
                "Annulation du Rewind precedent partielle: {fail} entite(s) "
                "n'ont pas pu etre localisees (FID deplace). "
                "Le Rewind continue malgre tout."
            ).format(fail=result.total_fail), "WARNING")
        events = getattr(self, '_pending_rewind_events', None)
        self._pending_rewind_events = None
        force_refetch = getattr(self, '_force_refetch_after_undo', False)
        self._force_refetch_after_undo = False
        undo_by_ds = getattr(result, 'by_ds', {}) or {}
        before = getattr(self, '_undo_counts_before', {})
        self._rewind_count_snapshot("after_undo", undo_by_ds, before=before)
        self._undo_counts_before = {}
        flog(f"on_auto_undo_then_rewind: undo_ok={result.total_ok} "
             f"undo_fail={result.total_fail} "
             f"pending_events_stale={len(events) if events else 0} "
             f"force_refetch={force_refetch}")
        if events or force_refetch:
            # RW-stale-dedup fix: the `_pending_rewind_events` list was produced
            # by `collapse_rewind_events_with_stats` BEFORE this auto-undo.
            # It dropped every user event whose trace (from the prior rewind we
            # just undid) was still flagged active. Applying it now would
            # re-play only the residual "always-skipped" events, masking the
            # actual rewind. Discard the stale plan, invalidate the undone
            # events' traces, and re-run fetch+dedup against the current state.
            undone_eids = [
                e.event_id for evts in undo_by_ds.values()
                for e in evts if e.event_id
            ]
            invalidated_n = 0
            if undone_eids:
                invalidated_n = self._invalidate_trace_events(undone_eids)
                flog(f"auto-undo: trace invalidation: "
                     f"undone_eids_n={len(undone_eids)} "
                     f"rows_updated={invalidated_n} "
                     f"(re-fetch will dedup against current state)")
                if invalidated_n == 0:
                    flog("auto-undo: WARNING no traces invalidated despite "
                         f"{len(undone_eids)} undone event(s); next fetch "
                         "will likely still see stale dedup",
                         "WARNING")
            self._refetch_after_auto_undo()
        else:
            self._restore_in_progress = False
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            if result.total_ok > 0:
                qlog(self.tr("{count} entite(s) remise(s) a l'etat actuel.").format(
                    count=result.total_ok))
                self.iface.mapCanvas().refresh()

    def _refetch_after_auto_undo(self) -> None:
        """Re-run the version fetch after auto-undo invalidated stale traces.

        Routes the result to ``_on_post_undo_fetch_done`` which re-runs dedup
        and applies the fresh plan without re-prompting the user (they already
        confirmed before the auto-undo).
        """
        cutoff = self._version_restore_cutoff
        trace_id = self._active_restore_trace_id
        if cutoff is None:
            flog("refetch_after_auto_undo: no cutoff saved -> abort", "ERROR")
            self._restore_in_progress = False
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            return
        checked_fps = self._get_version_checked_fingerprints()
        if not checked_fps:
            flog("refetch_after_auto_undo: no checked layers -> abort",
                 "WARNING")
            self._restore_in_progress = False
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            return
        flog(f"[{trace_id}] refetch_after_auto_undo: "
             f"scope={len(checked_fps)} layer(s) cutoff_reuse=True")

        if (self._version_fetch_thread
                and self._version_fetch_thread.isRunning()):
            self._version_fetch_thread.stop()
            self._version_fetch_thread.wait(500)

        self._version_fetch_thread = VersionFetchThread(
            self._journal, checked_fps, cutoff, trace_id=trace_id,
            include_traces=True)
        self._version_fetch_thread.events_ready.connect(
            self._on_post_undo_fetch_done)
        self._version_fetch_thread.error_occurred.connect(
            self._on_version_fetch_error)
        self._version_fetch_thread.start()

    def _on_post_undo_fetch_done(self, events) -> None:
        """Handle re-fetched events after auto-undo: re-dedup and execute.

        User already confirmed before the auto-undo, so skip volume warnings
        and the date confirmation dialog. The cutoff is unchanged.
        """
        from .core import collapse_rewind_events_with_stats

        raw_count = len(events)
        events, dedup_stats = collapse_rewind_events_with_stats(events)
        self._pending_cycle_stats = dict(dedup_stats)
        trace_id = self._active_restore_trace_id
        flog(f"[{trace_id}] on_post_undo_fetch_done: raw={raw_count} "
             f"collapsed={len(events)}")
        for e in events:
            flog(f"  -> active eid={e.event_id} op={e.operation_type} "
                 f"identity={(e.feature_identity_json or '')[:80]}")

        if not events:
            flog("on_post_undo_fetch_done: no active events after re-dedup "
                 "-> finish")
            self._restore_in_progress = False
            self._is_recovering = False
            self._stop_logo_activity()
            self.progress_bar.setVisible(False)
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            if trace_id:
                self._active_restore_trace_id = ""
            qlog(self.tr(
                "Auto-undo termine. Aucun evenement supplementaire a rejouer."
            ))
            return

        self._execute_version_restore(events)

    def _execute_version_restore(self, events) -> None:
        """Execute reverse replay restore (async, strict atomic per layer)."""
        trace_id = self._active_restore_trace_id or generate_trace_id()
        self._active_restore_trace_id = trace_id
        self.recover_button.setEnabled(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._restore_in_progress = True
        self._version_restore_events = events
        self._restore_started_at = time.monotonic()

        cutoff = self._version_restore_cutoff
        flog(f"[{trace_id}] execute_version_restore: "
             f"n={len(events)} cutoff={'set' if cutoff else 'MISSING'}")
        for e in events:
            flog(f"  -> restore eid={e.event_id} op={e.operation_type} "
                 f"identity={(e.feature_identity_json or '')[:80]}")
        events_by_ds_preview: dict = {}
        read_conn = self._get_dialog_read_conn()
        for e in events:
            fp = e.datasource_fingerprint or ''
            events_by_ds_preview.setdefault(fp, []).append(e)
        self._restore_counts_before = self._rewind_count_snapshot(
            "before_restore", events_by_ds_preview)

        def resolver(evt):
            return find_target_layer(evt, read_conn)

        runner = StrictRestoreRunner(
            events, resolver, cutoff,
            write_queue=self._write_queue,
            tracker=self._tracker, trace_id=trace_id,
            parent=self,
            extra_stats=getattr(self, '_pending_cycle_stats', None),
        )
        # Consume the stash; subsequent runs must repopulate it.
        self._pending_cycle_stats = None
        runner.progress.connect(self._on_restore_runner_progress)
        runner.finished.connect(self._on_version_restore_done)
        self._restore_runner = runner
        runner.start()

    def _on_restore_runner_progress(self, done: int, total: int) -> None:
        if total > 0:
            self._smooth_set_progress(int(done / total * 100))

    def _on_version_restore_done(self, result) -> None:
        trace_id = self._active_restore_trace_id
        self._restore_in_progress = False
        self._restore_runner = None
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
        if result.total_ok > 0 and events:
            self._last_restore_events = events
            self._last_restore_by_ds = result.by_ds
            self.undo_last_btn.setEnabled(True)
            flog(f"undo_last_btn: ENABLED ({result.total_ok} events)")
            for fp, evts in (result.by_ds or {}).items():
                layer_name = evts[0].layer_name_snapshot if evts else '?'
                flog(f"  stored_undo layer={layer_name!r} "
                     f"fp={fp[:16]}... n={len(evts)}")
        else:
            flog(f"on_version_restore_done: ok=0 or no events "
                 f"-> undo state NOT updated")
        self._version_restore_events = None

        detail_lines = self._build_restore_summary(result.by_ds)
        if result.total_fail == 0 and not result.errors:
            summary = self.tr("{count} entite(s) restauree(s) avec succes.").format(count=result.total_ok)
            qlog(summary + detail_lines)
        else:
            msg = self.tr("{ok} restauree(s), {fail} echouee(s).").format(ok=result.total_ok, fail=result.total_fail)
            if result.errors:
                msg += " | " + " | ".join(result.errors[:5])
            qlog(msg + detail_lines, "WARNING")

        if result.total_ok > 0:
            flog(f"recover_version: refreshing canvas for {result.total_ok} ok")
            self.iface.mapCanvas().refresh()
            self._open_attribute_tables_if_requested(restore_by_ds, "version")

        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)

    def _rewind_count_snapshot(self, label: str, by_ds: dict,
                                before: dict = None) -> dict:
        """Log feature counts per layer at a Rewind transition.

        Returns {fp: feat_count} for later comparison.
        For undo:    expected_delta = +1 per DELETE orig, -1 per INSERT orig
        For restore: expected_delta = +1 per DELETE event, -1 per INSERT event
        """
        if not by_ds:
            return {}
        read_conn = self._get_dialog_read_conn()
        counts = {}
        is_undo = 'undo' in label
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
                actual_delta = feat_now - before[fp]
                match = "OK" if actual_delta == exp_delta else "MISMATCH"
                line += (f" actual_delta={actual_delta:+d} [{match}]")
                if match == "MISMATCH":
                    missing = exp_delta - actual_delta
                    line += f" missing={missing:+d}"
            flog(line)
        flog(f"=== END REWIND_STATE [{label}] ===")
        return counts

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
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
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
        total_ok, total_fail, errors = feedback
        self.progress_bar.setVisible(False)
        self._stop_logo_activity()
        self.restore_button.setEnabled(bool(self.selected_rows))
        self.recover_button.setEnabled(True)

        if total_ok > 0 and total_fail == 0 and not errors:
            qlog(self.tr("{count} entite(s) restauree(s) avec succes.").format(count=total_ok))
        elif total_ok > 0:
            msg = self.tr("{ok} restauree(s), {fail} echouee(s).").format(ok=total_ok, fail=total_fail)
            if errors:
                msg += " | " + " | ".join(errors[:5])
            qlog(msg, "WARNING")
        else:
            qlog("Restauration: " + " | ".join(errors[:5]), "ERROR")
        if total_ok > 0:
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
                is_geom_only = (is_update and parsed_data is not None
                                and "changed_only" in parsed_data
                                and all(is_layer_audit_field(k) for k in parsed_data["changed_only"]))
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
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
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
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
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
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
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
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            self._active_restore_trace_id = ""
            qlog(self.tr("Recuperation annulee."))
        elif self._restore_runner is not None:
            applied = getattr(self._restore_runner, '_total_ok', 0)
            by_ds = dict(getattr(self._restore_runner, '_by_ds', {}))
            is_strict = isinstance(self._restore_runner, StrictRestoreRunner)
            self._restore_runner.cancel()
            self._restore_runner = None
            self._restore_in_progress = False
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            trace_id = self._active_restore_trace_id
            self._active_restore_trace_id = ""
            if applied > 0:
                events = getattr(self, '_version_restore_events', None) or \
                         getattr(self, '_event_restore_events', None)
                if events and by_ds:
                    self._last_restore_events = events
                    self._last_restore_by_ds = by_ds
                    self.undo_last_btn.setEnabled(True)
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
        wkb_source = "lightweight"
        if wkb == _BLOB_MARKER and event.event_id is not None:
            conn = self._get_dialog_read_conn()
            if conn is not None:
                full = get_event_by_id(conn, event.event_id)
                if full is not None:
                    wkb = full.geometry_wkb
                    wkb_source = "full_fetch"
        if not wkb or wkb == _BLOB_MARKER:
            self._geom_preview.clear()
            flog(
                f"_update_geometry_preview: cleared reason=blob_marker_unresolved "
                f"event_id={event.event_id}",
                "WARNING",
            )
            return
        target_layer = None
        try:
            target_layer = find_target_layer(event, self._get_dialog_read_conn())
        except Exception as exc:
            flog(
                f"_update_geometry_preview: target_layer_resolve_failed "
                f"event_id={event.event_id} err={exc}",
                "WARNING",
            )
        rendered = self._geom_preview.show(wkb, event.crs_authid, target_layer)
        if not rendered:
            flog(
                f"_update_geometry_preview: show_returned_false "
                f"event_id={event.event_id} crs={event.crs_authid} "
                f"wkb_source={wkb_source}",
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
        if getattr(self, '_restore_in_progress', False):
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
        ds_fp = next(iter(ds_fps)) if len(ds_fps) == 1 else "mixed"
        layer_names = sorted({e.layer_name_snapshot or "?" for e in lightweight_events})
        layer_label = ", ".join(layer_names)

        plan = plan_event_restore(lightweight_events, ds_fp, layer_label)
        report = preflight_check(plan)

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

        read_conn = self._get_dialog_read_conn()
        event_ids = [e.event_id for e in lightweight_events if e.event_id is not None]
        if read_conn is not None and event_ids:
            selected_events = fetch_events_by_ids(read_conn, event_ids)
        else:
            selected_events = lightweight_events
        self._event_restore_events = selected_events
        flog(f"[{trace_id}] restore_event: start selected={len(selected_events)} datasources={len(ds_fps)}")

        def resolver(evt):
            return find_target_layer(evt, read_conn)

        runner = RestoreRunner(selected_events, resolver, self._write_queue,
                               tracker=self._tracker, trace_id=trace_id,
                               parent=self)
        runner.progress.connect(self._on_restore_runner_progress)
        runner.finished.connect(self._on_event_restore_done)
        self._restore_runner = runner
        runner.start()

    def _on_event_restore_done(self, result) -> None:
        trace_id = self._active_restore_trace_id
        self._restore_in_progress = False
        self._restore_runner = None
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
        if result.total_ok > 0 and events:
            self._last_restore_events = events
            self._last_restore_by_ds = result.by_ds
            self.undo_last_btn.setEnabled(True)
        self._event_restore_events = None

        if result.total_ok > 0:
            self._open_attribute_tables_if_requested(result.by_ds, "event")

        feedback = (result.total_ok, result.total_fail, tuple(result.errors))
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
        """Remove a single layer from the undo state after user edits it."""
        if not self._last_restore_by_ds:
            return
        if not edited_fingerprint:
            return
        if edited_fingerprint not in self._last_restore_by_ds:
            return
        name = self._last_restore_by_ds[edited_fingerprint][0].layer_name_snapshot
        del self._last_restore_by_ds[edited_fingerprint]
        self._last_restore_events = [
            e for e in (self._last_restore_events or [])
            if e.datasource_fingerprint != edited_fingerprint
        ]
        flog(f"undo: '{name}' invalidated (edited), "
             f"{len(self._last_restore_by_ds)} layer(s) still undo-able")
        if not self._last_restore_by_ds:
            self._last_restore_events = None
            self.undo_last_btn.setEnabled(False)
            flog("undo_last_btn: DISABLED (all layers edited)")

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
            QgsApplication.getThemeIcon('/mActionUndo.svg'),
            self.tr("Annuler toute la session d'edition ({n} evenements)").format(
                n=n_events))
        undo_act.setToolTip(self.tr("Session du {date}").format(date=session_date))
        undo_act.triggered.connect(lambda: self._undo_session(session_id))

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
        """US-4.10.01: Reverse all events of a given edit session.

        Fetches the user events of the session, builds the by_ds payload,
        confirms with the user, then dispatches to UndoRunner. Reuses the
        existing rewind_dedup logic for idempotence.
        """
        if not session_id:
            return
        if getattr(self, '_restore_in_progress', False):
            qlog(self.tr("Un restore est deja en cours."), "WARNING")
            return
        read_conn = self._get_dialog_read_conn()
        if read_conn is None:
            return

        t0 = time.monotonic()
        try:
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

        by_ds = {}
        for e in events:
            by_ds.setdefault(e.datasource_fingerprint, []).append(e)

        layer_names = sorted({e.layer_name_snapshot or '?' for e in events})
        op_counts = {}
        for e in events:
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
            sid=session_id[:8] + "...", total=len(events),
            nlayers=len(by_ds), ops=ops_str, layers=layers_str,
        )

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
            f"n_layers={len(by_ds)} fetch_elapsed_ms={fetch_ms}",
            "INFO",
        )

        self._active_restore_trace_id = trace_id
        self._current_undo_session_id = session_id
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

        runner = UndoRunner(by_ds, resolver, tracker=self._tracker, parent=self)
        runner.progress.connect(self._on_restore_runner_progress)
        runner.finished.connect(self._on_undo_session_done)
        self._restore_runner = runner
        runner.start()

    def _on_undo_session_done(self, result) -> None:
        """US-4.10.01: Callback after undo_session completes."""
        trace_id = self._active_restore_trace_id
        session_id = getattr(self, '_current_undo_session_id', '')
        self._restore_in_progress = False
        self._restore_runner = None
        cleanup_temp_layers()
        self._smooth_set_progress(100)
        self._stop_logo_activity()

        if trace_id:
            started = self._restore_started_at
            elapsed_ms = int((time.monotonic() - started) * 1000) if started else 0
            flog(
                f"[{trace_id}] undo_session: done "
                f"session_id={session_id} "
                f"ok={result.total_ok} fail={result.total_fail} "
                f"elapsed_ms={elapsed_ms}",
                "INFO",
            )
            self._active_restore_trace_id = ""

        if result.total_fail == 0:
            qlog(self.tr("Session annulee : {ok} evenement(s) neutralise(s).").format(
                ok=result.total_ok))
        else:
            qlog(self.tr("Annulation partielle : {ok} OK, {fail} echec(s).").format(
                ok=result.total_ok, fail=result.total_fail), "WARNING")

        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)
        self.restore_button.setEnabled(True)
        self._current_undo_session_id = ""
        self.iface.mapCanvas().refresh()

    def _undo_last_restore(self):
        """Undo the last restore: revert data to its pre-restore state."""
        by_ds = self._last_restore_by_ds
        if not by_ds:
            flog("undo_last: requested but no undo state available")
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

        runner = UndoRunner(by_ds, resolver, tracker=self._tracker, parent=self)
        runner.progress.connect(self._on_restore_runner_progress)
        runner.finished.connect(self._on_undo_done)
        self._restore_runner = runner
        runner.start()

    def _on_undo_done(self, result) -> None:
        self._restore_in_progress = False
        self._restore_runner = None
        self._last_restore_events = None
        self._last_restore_by_ds = None

        self._smooth_set_progress(100)
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)

        if result.total_fail == 0 and result.total_ok > 0:
            qlog(self.tr("{count} entite(s) remise(s) dans leur etat initial.").format(count=result.total_ok))
        elif result.total_ok > 0:
            qlog(self.tr("Annulation partielle: {ok} ok, {fail} echec(s).").format(
                ok=result.total_ok, fail=result.total_fail), "WARNING")
        else:
            qlog("Annulation: " + " | ".join(result.errors[:5]), "ERROR")

        if result.total_ok > 0:
            self.iface.mapCanvas().refreshAllLayers()

        if result.total_ok > 0 and result.by_ds:
            # RW-stale-dedup symmetry: a manual undo that reverts a previous
            # rewind's data MUST also invalidate the corresponding traces.
            # Otherwise the next rewind on the same scope would see those
            # traces as still "active", dedup would neutralise the matching
            # user events, and the rewind would collapse to a no-op (or to
            # the residual always-skipped events). Until 2026-05 this branch
            # said "kept traces active so dedup will neutralize them", but
            # the actual UPDATE silently failed (readonly DB), so the
            # documented behaviour never took effect. Restoring DB
            # consistency makes the latent bug visible; invalidate instead.
            ok_eids = [
                e.event_id for evts in result.by_ds.values()
                for e in evts if e.event_id
            ]
            if result.failed_eids:
                n_failed = self._invalidate_trace_events(result.failed_eids)
                flog(f"undo_done: trace invalidation (failed): "
                     f"failed_eids_n={len(result.failed_eids)} "
                     f"rows_updated={n_failed}")
            n_ok = self._invalidate_trace_events(ok_eids) if ok_eids else 0
            flog(f"undo_done: trace invalidation (succeeded): "
                 f"ok_eids_n={len(ok_eids)} rows_updated={n_ok} "
                 f"(rewind on same scope will now see those events as "
                 f"active again)")

    def _invalidate_trace_events(self, undone_ids: list) -> int:
        """Soft-delete trace events referencing undone event IDs (RW-02).

        Uses a dedicated write connection (NOT a read-only one) because the
        ``audit_event`` table is being mutated. SQLite WAL serialises writers
        so concurrent activity from the WriteQueue is safe but may briefly
        block on the file lock.

        Returns the number of rows actually invalidated. Returns 0 on
        failure or when no matching trace exists.
        """
        from datetime import datetime, timezone
        if not undone_ids:
            return 0
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = None
        try:
            conn = self._journal.create_write_connection()
            ph = ','.join('?' * len(undone_ids))
            cursor = conn.execute(
                f"UPDATE audit_event SET invalidated_at = ? "  # nosec B608
                f"WHERE restored_from_event_id IN ({ph}) "
                f"AND invalidated_at IS NULL",
                [now_iso] + undone_ids,
            )
            rowcount = cursor.rowcount
            conn.commit()
            flog(f"_invalidate_trace_events: undone_ids_n={len(undone_ids)} "
                 f"rows_updated={rowcount} at={now_iso}")
            return rowcount if rowcount is not None and rowcount >= 0 else 0
        except Exception as exc:
            flog(f"_invalidate_trace_events: trace invalidation failed: "
                 f"{exc}", "WARNING")
            return 0
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _invalidate_orphan_traces_on_open(self) -> None:
        """Purge all active trace events that have no in-memory undo state.

        Called at dialog open (when _last_restore_by_ds is None) and on every
        project switch so that the rewind dedup never sees traces from a
        previous QGIS session that were never invalidated.
        """
        if self._journal is None or not getattr(self._journal, 'is_open', False):
            flog("_invalidate_orphan_traces_on_open: skipped journal=None or closed")
            return
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = None
        try:
            conn = self._journal.create_write_connection()
            cursor = conn.execute(
                "UPDATE audit_event SET invalidated_at = ? "
                "WHERE restored_from_event_id IS NOT NULL "
                "AND invalidated_at IS NULL",
                [now_iso],
            )
            rowcount = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0
            conn.commit()
            flog(
                f"_invalidate_orphan_traces_on_open: "
                f"legacy_traces_purged={rowcount} at={now_iso}"
            )
        except Exception as exc:
            flog(f"_invalidate_orphan_traces_on_open: failed: {exc}", "WARNING")
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def reset_undo_state(self) -> None:
        """Drop the last-restore memory (called by the plugin on project switch).

        Prevents undoing a restore made in project A onto the layers of project B.
        """
        had_state = bool(self._last_restore_by_ds)
        self._last_restore_events = None
        self._last_restore_by_ds = None
        self._pending_rewind_events = None
        if hasattr(self, 'undo_last_btn'):
            self.undo_last_btn.setEnabled(False)
        self._invalidate_orphan_traces_on_open()
        if had_state:
            flog("RecoverDialog: undo state reset (project switched)")

    def on_project_switched(self, tracker=None) -> None:
        """Full reset when the QGIS project changes.

        Drops the stale read connection, stats cache, date bounds, search
        results and undo state so the dialog picks up the new journal.
        """
        self._close_dialog_read_conn()
        self._stats_cache = LayerStatsCache()
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
        if not self._last_restore_by_ds:
            self._invalidate_orphan_traces_on_open()
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
            # dialog hide — but its geometry/z-order can be stale after a
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
                    "(snap_mode=True, bar=None) — bar lost",
                    "WARNING",
                )
            # Defensive: ensure the status bar pill stays visible.
            if self._review_status_widget is not None:
                self._show_review_status_bar()
                self._review_status_widget.activate()
        elif self._review_snap_mode and not self._review_wants_persist:
            flog("showEvent: snap_mode orphan detected — stopping", "WARNING")
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
        """Close event — data stays in its current state (rewound or not)."""
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

            # Cleanup async restore runner
            if self._restore_runner is not None:
                self._restore_runner.cancel()
                if self._tracker is not None and self._tracker.is_suppressed:
                    self._tracker.force_unsuppress()
                self._restore_runner = None

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
                blocking.append(
                    self.tr("Couche '{name}' introuvable dans le projet.").format(name=name))
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
