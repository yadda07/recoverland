from qgis.PyQt.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                              QComboBox, QMessageBox, QProgressBar,
                              QCheckBox, QApplication,
                              QTableWidget, QTableWidgetItem, QLineEdit,
                              QGraphicsDropShadowEffect, QWidget,
                              QScrollArea, QFrame, QMenu, QShortcut, QAction,
                              QStackedWidget, QListWidget, QListWidgetItem)
from qgis.PyQt.QtCore import (QDateTime, QDate, QTime, QTimer,
                              QVariantAnimation, QRectF,
                              QEasingCurve, Qt)
from qgis.PyQt.QtGui import QIcon, QColor, QKeySequence
from qgis.core import (QgsVectorLayer, QgsProject, QgsApplication, QgsSettings)
from qgis.gui import QgsCollapsibleGroupBox, QgsDateTimeEdit
from .compat import QtCompat, QgisCompat
from .core import (
    flog, qlog, LoggerMixin, LayerStatsCache,
    search_events, SearchCriteria,
    get_distinct_layers, summarize_scope, reconstruct_attributes,
    get_journal_size_bytes, format_journal_size,
    compute_datasource_fingerprint,
    is_geometry_only_update,
    evaluate_journal_health, format_integrity_message,
    get_journal_stats, HealthLevel,
    format_relative_time, format_full_timestamp,
    check_disk_for_path, format_disk_message,
    GeometryPreviewManager,
    plan_event_restore, preflight_check,
    format_plan_summary, format_preflight_report,
    execute_grouped_restore, execute_grouped_undo, find_target_layer,
    is_layer_audit_field,
    generate_trace_id,
)
from .journal_info_bar import JournalInfoBar, SmartBarState, SmartBarTileState
from .journal_maintenance import JournalMaintenanceDialog
from .local_search_thread import LocalSearchThread
from .restore_runner import RestoreRunner, UndoRunner
from .version_fetch_thread import VersionFetchThread
from .widgets import (AppleToggleSwitch, ThemedLogoWidget, RestoreModeSelector,
                      RestorePreflightDialog, TimeSliderWidget)
import os
import json
import time

CHANGE_TYPE_COLORS = {
    "modified":  QColor(66, 133, 244, 60),
    "emptied":   QColor(219, 68, 55, 55),
    "populated": QColor(52, 168, 83, 55),
    "geometry":  QColor(255, 152, 0, 60),
}

def _change_type_labels():
    from qgis.PyQt.QtCore import QCoreApplication
    _tr = lambda msg: QCoreApplication.translate("RecoverDialog", msg)
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
    
    def __init__(self, iface, router=None, journal=None, tracker=None,
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
        self.setStyleSheet("")
        
        icon_path = os.path.join(os.path.dirname(__file__), "logo.svg")
        if os.path.exists(icon_path):
            self.setWindowIcon(QIcon(icon_path))
        self._search_events = []
        self._all_attr_keys = []
        self.worker_thread = None
        self.restore_thread = None
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
        self._active_search_trace_id = ""
        self._active_restore_trace_id = ""
        self._dialog_read_conn = None
        self._version_fetch_thread = None
        self._geom_preview = GeometryPreviewManager(iface.mapCanvas())
        self._stats_cache = LayerStatsCache()

        self.setup_ui()

        self.progress_timer = QTimer(self)
        self.progress_timer.timeout.connect(self.pulse_progress_bar)
        self._layer_refresh_timer = QTimer(self)
        self._layer_refresh_timer.setInterval(10000)
        self._layer_refresh_timer.timeout.connect(self._refresh_journal_status)
        self._layer_refresh_timer.start()
        self._refresh_journal_status()
        self._refresh_layers_panel()
        tracking_on = QgsSettings().value("RecoverLand/tracking_enabled", True, type=bool)
        self.tracking_toggle.setChecked(tracking_on)
        if not tracking_on:
            self.tracking_label.setText(self.tr("Enregistrement désactivé"))
            self.tracking_label.setStyleSheet("color: #e74c3c; font-weight: 600;")
        self._refresh_smart_bar()
        self._check_onboarding()
        self._check_disk_space()

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
                self._on_tracking_toggled(False)
                self.tracking_toggle.setChecked(False, animated=True)
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
        op_text = self.operation_input.currentText()
        operation = None if op_text == "Toutes" else op_text
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
        op_text = self.operation_input.currentText()
        return ("ALL",) if op_text == "Toutes" else (op_text,)

    def _build_smart_bar_message(self, summary) -> str:
        if summary.total_count == 0:
            return self.tr("Aucune activité dans le périmètre courant.")
        operation = self.operation_input.currentText()
        if operation != self.tr("Toutes"):
            return self._build_filtered_smart_bar_message(summary, operation)
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
        if self._journal is not None and self._journal.is_open and self._journal.path:
            health = self._evaluate_health()
            if health is not None:
                health_level = health.level
                health_message = health.message
                health_suggestion = health.suggestion
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
        meta_parts = [size_str, self.tr("{count} couche(s)").format(count=summary.layer_count)]
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

    def _evaluate_health(self):
        """Compute journal health status from size and event stats."""
        path = self._journal.path if self._journal else ""
        if not path:
            return None
        size_bytes = get_journal_size_bytes(path)
        conn = self._get_dialog_read_conn()
        if conn is None:
            return evaluate_journal_health(size_bytes, 0, "", "")
        try:
            stats = get_journal_stats(conn)
            return evaluate_journal_health(
                size_bytes,
                stats["total_events"],
                stats.get("oldest_event", ""),
                stats.get("newest_event", ""),
            )
        except Exception as e:
            flog(f"_evaluate_health: {e}", "WARNING")
            return evaluate_journal_health(size_bytes, 0, "", "")

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
        self._refresh_smart_bar()

    def _apply_cached_ops(self, op_types) -> None:
        """Update operation combo from cached operation types (instant)."""
        prev = self.operation_input.currentText()
        self.operation_input.blockSignals(True)
        self.operation_input.clear()
        present = [op for op in ("UPDATE", "DELETE", "INSERT") if op in op_types]
        if len(present) != 1:
            self.operation_input.addItem(self.tr("Toutes"))
        for op in present:
            self.operation_input.addItem(op)
        idx = self.operation_input.findText(prev)
        if idx >= 0:
            self.operation_input.setCurrentIndex(idx)
        self.operation_input.blockSignals(False)

    def _apply_cached_date_bounds(self, min_date_str, _max_date_str=None) -> None:
        """Set start_input minimum from cached min date (instant)."""
        if not min_date_str:
            return
        min_dt = self._parse_iso_datetime(min_date_str)
        if min_dt is None or not min_dt.isValid():
            return
        self.start_input.setMinimumDateTime(min_dt)
        if self.start_input.dateTime() < min_dt:
            self.start_input.setDateTime(min_dt)

    def _rebuild_stats_cache(self) -> None:
        """Rebuild the per-layer stats cache from the journal."""
        conn = self._get_dialog_read_conn()
        if conn is not None:
            self._stats_cache.build(conn)

    def _refresh_smart_bar(self, _value=None) -> None:
        if not hasattr(self, 'smart_bar'):
            return
        path = self._journal.path if self._journal is not None else ""
        summary = None
        size_str = ""
        if self._journal is not None and self._journal.is_open and path:
            size_str = format_journal_size(get_journal_size_bytes(path))
            conn = self._get_dialog_read_conn()
            if conn is not None:
                try:
                    summary = summarize_scope(conn, self._build_search_criteria())
                except Exception as e:
                    flog(f"_refresh_smart_bar: {e}", "WARNING")
                    self._close_dialog_read_conn()
        self._smart_bar_summary = summary
        self._refresh_operation_types(summary)
        self.smart_bar.setToolTip(path or "")
        self.smart_bar.apply_state(self._build_smart_bar_state(summary, size_str))

    def _refresh_operation_types(self, summary) -> None:
        """Update Operation combo to only show types present in the journal scope."""
        prev = self.operation_input.currentText()
        self.operation_input.blockSignals(True)
        self.operation_input.clear()
        if summary is None:
            self.operation_input.addItem(self.tr("Toutes"))
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
            self.operation_input.addItem(self.tr("Toutes"))
        for op in present:
            self.operation_input.addItem(op)
        idx = self.operation_input.findText(prev)
        if idx >= 0:
            self.operation_input.setCurrentIndex(idx)
        self.operation_input.blockSignals(False)

    def _on_smart_bar_metric_activated(self, metric_key: str) -> None:
        target = self.tr("Toutes") if metric_key == "ALL" else metric_key
        current = self.operation_input.currentText()
        if metric_key != "ALL" and current == target:
            self.operation_input.setCurrentText(self.tr("Toutes"))
            return
        self.operation_input.setCurrentText(target)

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
        self._rebuild_stats_cache()
        self._load_journal_layers()
        self._refresh_smart_bar()

    def _load_journal_layers(self) -> None:
        """Populate layer combobox from distinct audited layers in the journal.

        Uses a fresh SQLite connection to bypass stale WAL snapshots.
        Preserves the current user selection if the item still exists.
        """
        if self._journal is None or not self._journal.is_open:
            return
        path = self._journal.path
        if not path:
            return

        current_fp = self.layer_input.currentData()
        conn = self._get_dialog_read_conn()
        if conn is None:
            return
        try:
            layers = get_distinct_layers(conn)
        except Exception as e:
            flog(f"_load_journal_layers: {e}", "WARNING")
            self._close_dialog_read_conn()
            return

        self.layer_input.blockSignals(True)
        self.layer_input.clear()
        if layers:
            self.layer_input.addItem(self.tr("Toutes les couches sauvegardées"), "")
        else:
            self.layer_input.addItem(self.tr("Aucune couche sauvegardée"), "")
        for lyr in layers:
            label = f"{lyr['name']}"
            self.layer_input.addItem(label, lyr['fingerprint'])

        idx = self.layer_input.findData(current_fp)
        if idx >= 0:
            self.layer_input.setCurrentIndex(idx)
        elif self.layer_input.count() > 1:
            self.layer_input.setCurrentIndex(1)
        self.layer_input.blockSignals(False)

        self._version_layer_list.clear()
        for lyr in layers:
            item = QListWidgetItem(lyr['name'])
            item.setFlags(item.flags() | QtCompat.ITEM_IS_USER_CHECKABLE)
            item.setCheckState(QtCompat.CHECKED)
            item.setData(QtCompat.USER_ROLE, lyr['fingerprint'])
            self._version_layer_list.addItem(item)

        has_layers = bool(layers)
        self.layer_input.setEnabled(has_layers)
        self.operation_input.setEnabled(has_layers)
        self.recover_button.setEnabled(has_layers)

    def _on_tracking_toggled(self, enabled: bool) -> None:
        """Persist toggle state and activate/deactivate the edit tracker."""
        QgsSettings().setValue("RecoverLand/tracking_enabled", enabled)
        if self._tracker is not None:
            if enabled:
                self._tracker.activate()
            else:
                self._tracker.deactivate()
        if enabled:
            self.tracking_label.setText(self.tr("Enregistrement actif"))
            self.tracking_label.setStyleSheet("color: #2ecc71; font-weight: 600;")
        else:
            self.tracking_label.setText(self.tr("Enregistrement désactivé"))
            self.tracking_label.setStyleSheet("color: #e74c3c; font-weight: 600;")
        flog(f"RecoverDialog: tracking {'enabled' if enabled else 'disabled'}")
        self._refresh_smart_bar()

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
            self.layers_status_label.setText(self.tr("{n} / {total} couche(s) surveillée(s)").format(n=n, total=len(layers)))

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
            self.layers_status_label.setText(self.tr("{n} / {total} couche(s) surveillée(s)").format(n=len(allowed), total=total))

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
        self._version_layer_list.setVisible(False)

        self.operation_input = QComboBox()
        self.operation_input.addItems([self.tr("Toutes")])
        self.operation_input.setToolTip(self.tr("Type d'opération à rechercher"))
        self.operation_input.currentIndexChanged.connect(self._refresh_smart_bar)

        layer_label = QLabel(self.tr("Couche:"))
        op_label = QLabel(self.tr("Opération:"))
        selection_row.addWidget(layer_label)
        selection_row.addWidget(self.layer_input, 3)
        selection_row.addWidget(op_label)
        selection_row.addWidget(self.operation_input, 1)
        selection_outer.addLayout(selection_row)
        selection_outer.addWidget(self._version_layer_list)
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
        self.start_input.setDateTime(today.addDays(-7))
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

        date_shortcuts.addWidget(min10_btn)
        date_shortcuts.addWidget(min30_btn)
        date_shortcuts.addWidget(hour1_btn)
        date_shortcuts.addWidget(day1_btn)
        date_shortcuts.addWidget(today_btn)
        date_shortcuts.addWidget(week_btn)
        date_shortcuts.addStretch()

        event_vbox.addLayout(dates_row)
        event_vbox.addLayout(date_shortcuts)

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
        self.table_widget.setAlternatingRowColors(True)
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
        self.progress_phase_label = QLabel("")
        self.progress_phase_label.setVisible(False)
        flog(f"setup_ui: progress_bar created, id={id(self.progress_bar)}, visible={self.progress_bar.isVisible()}, parent={self.progress_bar.parent()}")
        
        button_layout = QHBoxLayout()
        button_layout.setSpacing(10)
        button_width = 120
        button_height = 35
        
        self.cancel_button = QPushButton(self.tr("Fermer"))
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
        self.cancel_button.setFixedSize(button_width, button_height)
        self.cancel_button.clicked.connect(self.cancel_operation)
        
        self.undo_last_btn = QPushButton("Last")
        self.undo_last_btn.setIcon(QgsApplication.getThemeIcon('/mActionUndo.svg'))
        self.undo_last_btn.setFixedSize(button_width, button_height)
        self.undo_last_btn.setToolTip(self.tr("Annuler le dernier restore"))
        self.undo_last_btn.clicked.connect(self._undo_last_restore)
        self.undo_last_btn.setEnabled(False)

        self.recover_button = QPushButton("Recover")
        self.recover_button.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        self.recover_button.setFixedSize(button_width, button_height)
        self.recover_button.clicked.connect(self.recover_and_load)
        
        self.restore_button = QPushButton("Restore")
        self.restore_button.setIcon(QgsApplication.getThemeIcon('/mActionSaveAllEdits.svg'))
        self.restore_button.setFixedSize(button_width, button_height)
        self.restore_button.clicked.connect(self.restore_selected_data)
        self.restore_button.setEnabled(False)
        
        pal_hl = self.palette().highlight().color()
        glow_color = QColor(pal_hl.red(), pal_hl.green(), pal_hl.blue(), 180)
        self._apply_glow_effect(self.recover_button, glow_color)
        self._apply_glow_effect(self.restore_button, glow_color)
        self._glow_color = glow_color
        self._apply_logo_glow_effect()
        
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.recover_button)
        button_layout.addWidget(self.restore_button)
        button_layout.addWidget(self.undo_last_btn)
        button_layout.addStretch()
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
        
        self.recover_button.setEnabled(False)
        self._setup_shortcuts()

    def _setup_shortcuts(self) -> None:
        """UX-F02: Register keyboard shortcuts with tooltip hints."""
        QShortcut(QKeySequence("F5"), self, self.recover_and_load)
        QShortcut(QKeySequence("Ctrl+F"), self, self._focus_search_filter)
        self.recover_button.setToolTip(self.tr("Lancer la recherche (F5)"))
        self.search_filter.setToolTip(self.tr("Filtrer les résultats (Ctrl+F)"))

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
        self._refresh_smart_bar()

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
            parts.append(self.tr("La periode est courte (< 1h). Essayez d'elargir a 24h ou 7 jours."))
        if self.layer_input.currentData():
            parts.append(self.tr(
                "Un filtre de couche est actif. "
                "Essayez 'Toutes les couches sauvegardees'."))
        if self.operation_input.currentText() != self.tr("Toutes"):
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
        """Switch period stack and layer widget between event and version modes."""
        flog(f"period_mode: switched to {mode}")
        is_version = (mode == "temporal")
        if is_version:
            self._period_stack.setCurrentIndex(1)
            self.layer_input.setVisible(False)
            self._version_layer_list.setVisible(True)
            self.results_group.setVisible(False)
            self.restore_button.setVisible(False)
            self.recover_button.setText(self.tr("Rewind"))
            self.recover_button.setIcon(QgsApplication.getThemeIcon('/mActionUndo.svg'))
            self._refresh_slider_bounds()
        else:
            self._period_stack.setCurrentIndex(0)
            self._version_layer_list.setVisible(False)
            self.layer_input.setVisible(True)
            self.results_group.setVisible(True)
            self.restore_button.setVisible(True)
            self.recover_button.setText(self.tr("Recover"))
            self.recover_button.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))

    def _refresh_slider_bounds(self) -> None:
        """Configure slider bounds from the stats cache (no live query)."""
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
        newest_dt = QDateTime.currentDateTime()
        self.time_slider.set_bounds(oldest_dt, newest_dt)

    @staticmethod
    def _parse_iso_datetime(iso_str: str):
        """Parse ISO datetime string to QDateTime, handling microseconds and timezone."""
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
        today = QDateTime.currentDateTime()
        self.end_input.setMaximumDateTime(today)
        
        if period == "today":
            self.start_input.setDateTime(QDateTime(QDate.currentDate(), QTime(0, 0, 0)))
            self.end_input.setDateTime(today)
        elif period == "week":
            current = QDate.currentDate()
            monday = current.addDays(-(current.dayOfWeek() - 1))
            self.start_input.setDateTime(QDateTime(monday, QTime(0, 0, 0)))
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
        elif isinstance(period, int):
            if period == 0:
                self.start_input.setDateTime(QDateTime(QDate.currentDate(), QTime(0, 0, 0)))
                self.end_input.setDateTime(today)
            elif period == 1:
                yesterday = QDate.currentDate().addDays(-1)
                self.start_input.setDateTime(QDateTime(yesterday, QTime(0, 0, 0)))
                self.end_input.setDateTime(QDateTime(yesterday, QTime(23, 59, 59)))
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
        No processEvents needed.
        """
        if self.progress_bar.maximum() == 0:
            self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)

    def on_events_committed(self) -> None:
        """Auto-refresh smart bar and event table after new events are flushed.

        Called by the plugin after a short delay post-commit so the
        WriteQueue has time to flush.
        """
        if not self.isVisible():
            return
        if self._is_recovering:
            return
        self._close_dialog_read_conn()
        self._rebuild_stats_cache()
        self._refresh_smart_bar()
        flog("on_events_committed: smart bar refreshed")
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
            return
        self._is_recovering = True
        self.enable_controls(False)
        self.progress_bar.setValue(0)

        if self.restore_mode_selector.mode() == "temporal":
            self._recover_version_mode()
        else:
            self._recover_event_mode()

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
        flog(f"[{trace_id}] recover_event: start layer={criteria.datasource_fingerprint} op={criteria.operation_type} start={criteria.start_date} end={criteria.end_date}")

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

        cutoff_dt = self.time_slider.cutoff_datetime()
        cutoff_iso = cutoff_dt.toUTC().toString("yyyy-MM-ddTHH:mm:ss")

        checked_fps = self._get_version_checked_fingerprints()
        if not checked_fps:
            self._is_recovering = False
            self.enable_controls(True)
            self.recover_button.setEnabled(True)
            qlog(self.tr("Cochez au moins une couche."), "WARNING")
            return

        cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=False)
        trace_id = generate_trace_id()
        self._active_restore_trace_id = trace_id
        flog(f"[{trace_id}] recover_version: start scope={len(checked_fps)} layer(s) cutoff={cutoff_iso}")

        self.progress_bar.setVisible(True)
        self._start_logo_activity(self._glow_color, "recover")
        self._recover_started_at = time.monotonic()
        self._version_cutoff_dt = cutoff_dt

        if self._version_fetch_thread and self._version_fetch_thread.isRunning():
            self._version_fetch_thread.stop()
            self._version_fetch_thread.wait(500)

        self._version_fetch_thread = VersionFetchThread(
            self._journal, checked_fps, cutoff, trace_id=trace_id)
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

        total_count = len(events)
        cutoff_dt = getattr(self, '_version_cutoff_dt', None)
        trace_id = self._active_restore_trace_id
        if trace_id:
            flog(f"[{trace_id}] recover_version: fetched total={total_count}")

        if total_count == 0:
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

        self._execute_version_restore(events)

    def _execute_version_restore(self, events) -> None:
        """Execute reverse replay restore (async, non-blocking)."""
        trace_id = self._active_restore_trace_id or generate_trace_id()
        self._active_restore_trace_id = trace_id
        self.recover_button.setEnabled(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._restore_in_progress = True
        self._version_restore_events = events
        self._restore_started_at = time.monotonic()
        flog(f"[{trace_id}] recover_version: execute_restore events={len(events)}")

        read_conn = self._get_dialog_read_conn()
        resolver = lambda evt: find_target_layer(evt, read_conn)

        runner = RestoreRunner(events, resolver, self._write_queue,
                               tracker=self._tracker, trace_id=trace_id,
                               parent=self)
        runner.progress.connect(self._on_restore_runner_progress)
        runner.finished.connect(self._on_version_restore_done)
        self._restore_runner = runner
        runner.start()

    def _on_restore_runner_progress(self, done: int, total: int) -> None:
        if total > 0:
            self.progress_bar.setValue(int(done / total * 100))

    def _on_version_restore_done(self, result) -> None:
        trace_id = self._active_restore_trace_id
        self._restore_in_progress = False
        self._restore_runner = None
        self.progress_bar.setValue(100)
        self._stop_logo_activity()
        if trace_id:
            elapsed_ms = int((time.monotonic() - self._restore_started_at) * 1000) if self._restore_started_at else 0
            flog(f"[{trace_id}] recover_version: done ok={result.total_ok} fail={result.total_fail} elapsed_ms={elapsed_ms}")
            self._active_restore_trace_id = ""

        events = getattr(self, '_version_restore_events', None)
        if result.total_ok > 0 and events:
            self._last_restore_events = events
            self._last_restore_by_ds = result.by_ds
            self.undo_last_btn.setEnabled(True)
        self._version_restore_events = None

        if result.total_fail == 0 and not result.errors:
            qlog(self.tr("{count} entite(s) restauree(s) avec succes.").format(count=result.total_ok))
        else:
            msg = self.tr("{ok} restauree(s), {fail} echouee(s).").format(ok=result.total_ok, fail=result.total_fail)
            if result.errors:
                msg += " | " + " | ".join(result.errors[:5])
            qlog(msg, "WARNING")

        self.progress_bar.setVisible(False)
        self.enable_controls(True)
        self.recover_button.setEnabled(True)


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
            elapsed_ms = int((time.monotonic() - self._recover_started_at) * 1000) if self._recover_started_at else 0
            flog(f"[{trace_id}] recover_event: done total={result.total_count} returned={len(result.events)} elapsed_ms={elapsed_ms}")
            self._active_search_trace_id = ""
        if result.total_count == 0:
            suggestion = self._build_empty_result_suggestion()
            qlog(suggestion)
            self._refresh_smart_bar()
            return

        self._search_events = result.events
        self._populate_results_table(result.events, result.total_count)
        self._smart_bar_message_override = self.tr("{count} événement(s) chargé(s) dans la table.").format(count=result.total_count)
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
            self.iface.mapCanvas().refresh()

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
            for col_idx, val in enumerate(row_values):
                item = QTableWidgetItem(val)
                if col_idx == 0:
                    item.setData(QtCompat.USER_ROLE, row_idx)
                if has_geom_change and col_idx == len(row_values) - 1 and geom_changed:
                    item.setBackground(CHANGE_TYPE_COLORS["geometry"])
                self.table_widget.setItem(row_idx, col_idx, item)

            for col_offset, key in enumerate(attr_keys):
                val = attrs.get(key)
                item = QTableWidgetItem(str(val) if val is not None else "")
                col_idx = n_fixed + col_offset
                if is_update and key in changed_keys:
                    item.setBackground(CHANGE_TYPE_COLORS["modified"])
                    self._modified_col_indices.add(col_idx)
                    if parsed_data is not None:
                        change = parsed_data.get("changed_only", {}).get(key, {})
                        if isinstance(change, dict):
                            item.setToolTip(self.tr("Ancien: {old}\nActuel: {new}").format(old=val, new=change.get('new')))
                self.table_widget.setItem(row_idx, col_idx, item)

        self.table_widget.setSortingEnabled(True)
        self.table_widget.resizeColumnsToContents()
        self.results_info_label.setText(
            self.tr("{count} événement(s) trouvé(s), sélectionnez les lignes à restaurer").format(count=total)
        )
        self.results_group.setTitle(self.tr("Résultats ({count} événements)").format(count=total))
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
            self._restore_runner.cancel()
            if self._tracker is not None and self._tracker.is_suppressed:
                self._tracker.unsuppress()
            self._restore_runner = None
            self._restore_in_progress = False
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            self._active_restore_trace_id = ""
            qlog(self.tr("Restauration annulee."))
        elif self.restore_thread and self.restore_thread.isRunning():
            self.restore_thread.stop()
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText(self.tr("Fermer"))
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            self._active_restore_trace_id = ""
            qlog(self.tr("Restauration annulee."))
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
    
    
    @staticmethod
    def _get_non_comparable_columns(column_names) -> set:
        """Return set of column indices to skip during UPDATE diff.
        
        Skips: gid, user_name, audit_timestamp, geometry columns.
        """
        skip_names = {'gid', 'user_name', 'audit_timestamp', 'geom', 'the_geom', 'geometry', 'wkb_geometry'}
        return {i for i, name in enumerate(column_names) if name.lower() in skip_names}
    
    @staticmethod
    def _values_differ(audit_val, current_val) -> bool:
        """Compare two cell values, handling None and type coercion."""
        if audit_val is None and current_val is None:
            return False
        if audit_val is None or current_val is None:
            return True
        return str(audit_val) != str(current_val)

    @staticmethod
    def _is_date_column(col_name, *values):
        """Detect date/timestamp column by name pattern or value type."""
        import datetime
        name_lower = col_name.lower()
        date_keywords = ('date', 'timestamp', 'time', 'created', 'modified', 'maj')
        if any(kw in name_lower for kw in date_keywords):
            return True
        for val in values:
            if isinstance(val, (datetime.date, datetime.datetime)):
                return True
        return False

    @staticmethod
    def _classify_change(col_name, audit_val, current_val):
        """Classify change type for UPDATE diff semiology.

        Returns: 'modified', 'emptied', 'populated', 'date', or None.
        """
        audit_empty = audit_val is None or str(audit_val).strip() == ''
        current_empty = current_val is None or str(current_val).strip() == ''
        if audit_empty and current_empty:
            return None
        if not audit_empty and not current_empty and str(audit_val) == str(current_val):
            return None
        is_date = RecoverDialog._is_date_column(col_name, audit_val, current_val)
        if not audit_empty and current_empty:
            return "date" if is_date else "emptied"
        if audit_empty and not current_empty:
            return "date" if is_date else "populated"
        return "date" if is_date else "modified"

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
        self.selection_count_label.setText(self.tr("{selected} / {total} sélectionnées").format(selected=selected, total=total) if selected > 0 else "")
        self.restore_button.setEnabled(selected > 0)
        self._update_geometry_preview()
    
    def _update_geometry_preview(self) -> None:
        """P1.1: Show old geometry on canvas when exactly one geometry event is selected."""
        if not self.selected_rows:
            self._geom_preview.clear()
            return
        if len(self.selected_rows) != 1:
            self._geom_preview.clear()
            return
        idx = self.selected_rows[0]
        if idx >= len(self._search_events):
            self._geom_preview.clear()
            return
        event = self._search_events[idx]
        if not event.geometry_wkb:
            self._geom_preview.clear()
            return
        self._geom_preview.show(event.geometry_wkb, event.crs_authid)

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

        selected_events = [
            self._search_events[r]
            for r in self.selected_rows
            if r < len(self._search_events)
        ]
        if not selected_events:
            return

        ds_fps = {e.datasource_fingerprint for e in selected_events}
        ds_fp = next(iter(ds_fps)) if len(ds_fps) == 1 else "mixed"
        layer_names = sorted({e.layer_name_snapshot or "?" for e in selected_events})
        layer_label = ", ".join(layer_names)

        plan = plan_event_restore(selected_events, ds_fp, layer_label)
        report = preflight_check(plan)

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
        self._event_restore_events = selected_events
        flog(f"[{trace_id}] restore_event: start selected={len(selected_events)} datasources={len(ds_fps)}")

        read_conn = self._get_dialog_read_conn()
        resolver = lambda evt: find_target_layer(evt, read_conn)

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
        self.progress_bar.setValue(100)
        if trace_id:
            elapsed_ms = int((time.monotonic() - self._restore_started_at) * 1000) if self._restore_started_at else 0
            flog(f"[{trace_id}] restore_event: done ok={result.total_ok} fail={result.total_fail} elapsed_ms={elapsed_ms}")
            self._active_restore_trace_id = ""

        events = getattr(self, '_event_restore_events', None)
        if result.total_ok > 0 and events:
            self._last_restore_events = events
            self._last_restore_by_ds = result.by_ds
            self.undo_last_btn.setEnabled(True)
        self._event_restore_events = None

        feedback = (result.total_ok, result.total_fail, tuple(result.errors))
        elapsed = time.monotonic() - self._restore_started_at
        remaining = _MIN_RESTORE_ANIMATION_SEC - elapsed
        if remaining > 0:
            self._pending_restore_feedback = feedback
            QTimer.singleShot(int(remaining * 1000), self._display_deferred_restore_feedback)
            return
        self._display_restore_feedback(feedback)

    def _undo_last_restore(self):
        """Undo the last restore: revert data to its pre-restore state."""
        events = getattr(self, '_last_restore_events', None)
        by_ds = getattr(self, '_last_restore_by_ds', None)
        if not events or not by_ds:
            return

        reply = QMessageBox.question(
            self,
            self.tr("Annuler le restore"),
            self.tr("Remettre les {count} entite(s) dans leur etat avant le restore ?").format(
                count=len(events)),
            QtCompat.MSG_YES | QtCompat.MSG_NO,
            QtCompat.MSG_NO,
        )
        if reply != QtCompat.MSG_YES:
            return

        self.undo_last_btn.setEnabled(False)
        self._restore_in_progress = True

        read_conn = self._get_dialog_read_conn()
        resolver = lambda evt: find_target_layer(evt, read_conn)

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

        if result.total_fail == 0 and result.total_ok > 0:
            qlog(self.tr("{count} entite(s) remise(s) dans leur etat initial.").format(count=result.total_ok))
        elif result.total_ok > 0:
            qlog(self.tr("Annulation partielle: {ok} ok, {fail} echec(s).").format(ok=result.total_ok, fail=result.total_fail), "WARNING")
        else:
            qlog("Annulation: " + " | ".join(result.errors[:5]), "ERROR")

    def showEvent(self, event):
        super().showEvent(event)
        if hasattr(self, '_layer_refresh_timer'):
            self._layer_refresh_timer.start()

    def hideEvent(self, event):
        super().hideEvent(event)
        if hasattr(self, '_layer_refresh_timer'):
            self._layer_refresh_timer.stop()

    def closeEvent(self, event):
        """Close event"""
        self.cleanup_resources()
        event.accept()
    
    def reject(self):
        """Reject dialog"""
        self.cleanup_resources()
        super().reject()
    
    def cleanup_resources(self) -> None:
        """Cleanup threads and resources."""
        try:
            self._active_search_trace_id = ""
            self._active_restore_trace_id = ""
            self._close_dialog_read_conn()
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
                    self._tracker.unsuppress()
                self._restore_runner = None

            # Cleanup restore thread
            if hasattr(self, 'restore_thread') and self.restore_thread:
                self._disconnect_thread_signals(self.restore_thread)
                if self.restore_thread.isRunning():
                    self.restore_thread.stop()
                self.restore_thread = None
            
            # Stop progress timer
            if hasattr(self, 'progress_timer') and self.progress_timer.isActive():
                self.progress_timer.stop()
            if hasattr(self, 'logo_label'):
                self.logo_label.stop_recovery_effect()
            if hasattr(self, '_geom_preview'):
                self._geom_preview.clear()
        except Exception as e:
            self.log_error(f"Erreur cleanup: {e}")
    
    def _disconnect_thread_signals(self, thread) -> None:
        """Safely disconnect all signals from a thread."""
        signals = ['progress_updated', 'phase_changed', 'process_complete', 
                   'restore_complete', 'error_occurred', 'log_message']
        for sig_name in signals:
            if hasattr(thread, sig_name):
                try:
                    getattr(thread, sig_name).disconnect()
                except (TypeError, RuntimeError):
                    pass  # Signal was not connected
    
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
        start_datetime = self.start_input.dateTime().toPyDateTime()
        end_datetime = self.end_input.dateTime().toPyDateTime()
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
