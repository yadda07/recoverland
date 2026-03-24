from qgis.PyQt.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
                              QComboBox, QMessageBox, QProgressBar,
                              QFormLayout, QCheckBox, QApplication,
                              QTableWidget, QTableWidgetItem, QLineEdit,
                              QFileDialog, QGraphicsDropShadowEffect, QWidget,
                              QScrollArea, QFrame, QMenu, QShortcut, QAction)
from qgis.PyQt.QtCore import (QDateTime, QDate, QTime, QTimer,
                              QVariantAnimation, QRectF,
                              QEasingCurve, Qt)
from qgis.PyQt.QtGui import QIcon, QColor, QKeySequence
from qgis.core import (QgsVectorLayer, QgsProject, QgsApplication, QgsSettings)
from qgis.gui import QgsCollapsibleGroupBox, QgsDateTimeEdit, QgsMessageBar
from .compat import QtCompat, QgisCompat
from .core import (
    flog, LoggerMixin,
    search_events, SearchCriteria,
    get_distinct_layers, summarize_scope, reconstruct_attributes,
    get_journal_size_bytes, format_journal_size,
    restore_batch, compute_datasource_fingerprint,
    is_geometry_only_update,
    evaluate_journal_health, format_integrity_message,
    get_journal_stats, HealthLevel,
    format_relative_time, format_full_timestamp,
    check_disk_for_path, format_disk_message,
)
from .journal_info_bar import JournalInfoBar, SmartBarState, SmartBarTileState
from .journal_maintenance import JournalMaintenanceDialog
from .local_search_thread import LocalSearchThread
from .widgets import AppleToggleSwitch, ThemedLogoWidget
import os
import json
import time
from collections import defaultdict

CHANGE_TYPE_COLORS = {
    "modified":  QColor(66, 133, 244, 60),
    "emptied":   QColor(219, 68, 55, 55),
    "populated": QColor(52, 168, 83, 55),
    "geometry":  QColor(255, 152, 0, 60),
}

CHANGE_TYPE_LABELS = {
    "modified":  "Valeur modifiee",
    "emptied":   "Valeur videe",
    "populated": "Valeur ajoutee",
    "geometry":  "Geometrie modifiee",
}


RECOVER_GLOW_COLOR = QColor(219, 177, 52, 180)
RESTORE_GLOW_COLOR = QColor(40, 167, 69, 180)
_MIN_RECOVER_ANIMATION_SEC = 3.0


class RecoverDialog(QDialog, LoggerMixin):
    """Main dialog for RecoverLand plugin."""
    
    def __init__(self, iface, router=None, journal=None, tracker=None,
                 write_queue=None):
        flog("RecoverDialog.__init__: START")
        super().__init__(iface.mainWindow())
        self.iface = iface
        self._router = router
        self._journal = journal
        self._tracker = tracker
        self._write_queue = write_queue
        self.setWindowTitle("RecoverLand - Récupération de données")
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
        self._dialog_read_conn = None

        self.setup_ui()

        self.progress_timer = QTimer(self)
        self.progress_timer.timeout.connect(self.pulse_progress_bar)
        self._layer_refresh_timer = QTimer(self)
        self._layer_refresh_timer.setInterval(3000)
        self._layer_refresh_timer.timeout.connect(self._refresh_journal_status)
        self._layer_refresh_timer.start()
        self._refresh_journal_status()
        self._refresh_layers_panel()
        tracking_on = QgsSettings().value("RecoverLand/tracking_enabled", True, type=bool)
        self.tracking_toggle.setChecked(tracking_on)
        if not tracking_on:
            self.tracking_label.setText("Enregistrement désactivé")
            self.tracking_label.setStyleSheet("color: #e74c3c; font-weight: 600;")
        self._refresh_smart_bar()
        self._check_onboarding()
        self._check_disk_space()

    def _check_onboarding(self) -> None:
        """UX-F01: Show onboarding panel on first launch."""
        done = QgsSettings().value("RecoverLand/onboarding_done", False, type=bool)
        if done:
            return
        self.message_bar.pushMessage(
            "Bienvenue dans RecoverLand",
            "RecoverLand enregistre automatiquement vos modifications. "
            "Utilisez Recover pour retrouver vos donnees, "
            "puis Restore pour les reinjecter dans vos couches. "
            "Le journal est stocke localement a cote de votre projet.",
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
            self.message_bar.pushMessage("Espace disque", msg, QgisCompat.MSG_CRITICAL, 0)
            if self._tracker and self.tracking_toggle.isChecked():
                self._on_tracking_toggled(False)
                self.tracking_toggle.setChecked(False, animated=True)
        elif status.is_low:
            msg = format_disk_message(status)
            self.message_bar.pushMessage("Espace disque", msg, QgisCompat.MSG_WARNING, 10)

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
            suffix = "Enregistrement actif." if self.tracking_toggle.isChecked() else "Enregistrement désactivé."
            return f"Aucun événement dans le scope courant. {suffix}"
        count_text = f"{summary.selected_count} / {summary.total_count} événement(s)"
        if summary.selected_count == summary.total_count:
            count_text = f"{summary.total_count} événement(s)"
        if self.operation_input.currentText() != "Toutes":
            count_text = f"{count_text} pour {self.operation_input.currentText()}"
        else:
            count_text = f"{count_text} dans le scope courant"
        suffix = "Enregistrement actif." if self.tracking_toggle.isChecked() else "Enregistrement désactivé."
        return f"{count_text}. {suffix}"

    def _build_smart_bar_tiles(self, summary) -> tuple:
        total_color = self.palette().color(QtCompat.PALETTE_HIGHLIGHT)
        return (
            SmartBarTileState("ALL", "Total", str(summary.total_count), total_color,
                              "Réinitialiser le filtre d'opération"),
            SmartBarTileState("UPDATE", "Updates", str(summary.update_count), QColor(66, 133, 244),
                              "Basculer le filtre sur UPDATE"),
            SmartBarTileState("DELETE", "Suppr.", str(summary.delete_count), QColor(219, 68, 55),
                              "Basculer le filtre sur DELETE"),
            SmartBarTileState("INSERT", "Ajouts", str(summary.insert_count), QColor(52, 168, 83),
                              "Basculer le filtre sur INSERT"),
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
            message = override or "Ouvrez un projet QGIS pour activer le journal local."
            return SmartBarState(
                title="Journal local",
                meta="",
                message=message,
                mode="disabled",
                active_keys=(),
                tiles=(
                    SmartBarTileState("ALL", "Total", "0", self.palette().color(QtCompat.PALETTE_HIGHLIGHT), ""),
                    SmartBarTileState("UPDATE", "Updates", "0", QColor(66, 133, 244), ""),
                    SmartBarTileState("DELETE", "Suppr.", "0", QColor(219, 68, 55), ""),
                    SmartBarTileState("INSERT", "Ajouts", "0", QColor(52, 168, 83), ""),
                ),
                health_level=health_level,
                health_message=health_message,
                health_suggestion=health_suggestion,
            )
        message = override or self._build_smart_bar_message(summary)
        pending = self._get_write_queue_pending()
        meta_parts = [size_str, f"{summary.layer_count} couche(s)"]
        if pending > 0:
            meta_parts.append(f"{pending} en attente")
        meta = " | ".join(meta_parts)
        return SmartBarState(
            title="Journal local",
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
        self.smart_bar.setToolTip(path or "")
        self.smart_bar.apply_state(self._build_smart_bar_state(summary, size_str))

    def _on_smart_bar_metric_activated(self, metric_key: str) -> None:
        target = "Toutes" if metric_key == "ALL" else metric_key
        current = self.operation_input.currentText()
        if metric_key != "ALL" and current == target:
            self.operation_input.setCurrentText("Toutes")
            return
        self.operation_input.setCurrentText(target)

    def _refresh_journal_status(self) -> None:
        """Refresh journal label, layer list and user list from local SQLite journal."""
        if self._journal is None or not self._journal.is_open:
            self._smart_bar_summary = None
            self._smart_bar_message_override = "Ouvrez un projet QGIS pour activer le journal local."
            self.layer_input.blockSignals(True)
            self.layer_input.clear()
            self.layer_input.addItem("Aucune couche sauvegardée", "")
            self.layer_input.blockSignals(False)
            self.layer_input.setEnabled(False)
            self.operation_input.setEnabled(False)
            self.recover_button.setEnabled(False)
            self._refresh_smart_bar()
            return
        self._set_journal_info_visible(True)
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
            self.layer_input.addItem("Toutes les couches sauvegardées", "")
        else:
            self.layer_input.addItem("Aucune couche sauvegardée", "")
        for lyr in layers:
            label = f"{lyr['name']}"
            self.layer_input.addItem(label, lyr['fingerprint'])

        idx = self.layer_input.findData(current_fp)
        if idx >= 0:
            self.layer_input.setCurrentIndex(idx)
        elif self.layer_input.count() > 1:
            self.layer_input.setCurrentIndex(1)
        self.layer_input.blockSignals(False)
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
            self.tracking_label.setText("Enregistrement actif")
            self.tracking_label.setStyleSheet("color: #2ecc71; font-weight: 600;")
        else:
            self.tracking_label.setText("Enregistrement désactivé")
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
            self.layers_status_label.setText("Aucune couche vecteur chargée")
        elif all_tracked:
            self.layers_status_label.setText("Toutes les couches surveillées")
        else:
            n = sum(1 for lyr in layers if compute_datasource_fingerprint(lyr) in allowed)
            self.layers_status_label.setText(f"{n} / {len(layers)} couche(s) surveillée(s)")

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
            self.layers_status_label.setText("Toutes les couches surveillées")
        else:
            self.layers_status_label.setText(f"{len(allowed)} / {total} couche(s) surveillée(s)")

    def setup_ui(self):
        
        main_layout = QVBoxLayout()
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)
        logo_label = ThemedLogoWidget()
        logo_label.setSizePolicy(QtCompat.SIZE_FIXED, QtCompat.SIZE_FIXED)
        self.logo_label = logo_label
        self._load_themed_logo()
        self.message_bar = QgsMessageBar()
        self.message_bar.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        self.message_bar.setMinimumHeight(0)
        # Status Frame (Sleek modern header instead of QFormLayout GroupBox)
        status_frame = QFrame()
        status_frame.setObjectName("statusFrame")
        status_frame.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        status_frame.setStyleSheet("""
            QFrame#statusFrame {
                background-color: rgba(150, 150, 150, 20);
                border-radius: 14px;
            }
        """)
        status_layout = QHBoxLayout(status_frame)
        status_layout.setContentsMargins(10, 10, 10, 10)
        status_layout.setSpacing(12)

        self.smart_bar = JournalInfoBar()
        self.smart_bar.metricActivated.connect(self._on_smart_bar_metric_activated)
        self.smart_bar.maintenanceRequested.connect(self._open_maintenance)

        tracking_panel = QWidget()
        tracking_panel.setSizePolicy(QtCompat.SIZE_FIXED, QtCompat.SIZE_FIXED)
        tracking_panel.setStyleSheet("background: transparent;")
        tracking_layout = QHBoxLayout(tracking_panel)
        tracking_layout.setContentsMargins(4, 0, 4, 0)
        tracking_layout.setSpacing(6)
        
        self.tracking_label = QLabel("Enregistrement actif")
        self.tracking_label.setStyleSheet("font-size: 12px; font-weight: bold; color: #2ecc71;")
        self.tracking_toggle = AppleToggleSwitch()
        self.tracking_toggle.toggled.connect(self._on_tracking_toggled)
        
        tracking_layout.addWidget(self.tracking_label)
        tracking_layout.addWidget(self.tracking_toggle)
        status_layout.addWidget(self.smart_bar, 1)
        status_layout.addWidget(tracking_panel, 0, QtCompat.ALIGN_VCENTER)

        layers_group = QgsCollapsibleGroupBox()
        layers_group.setTitle("Couches surveillées")
        layers_group.setCollapsed(True)
        layers_vbox = QVBoxLayout()
        layers_vbox.setSpacing(6)

        layers_header = QHBoxLayout()
        self.layers_status_label = QLabel("Toutes les couches surveillées")
        self.layers_status_label.setStyleSheet("font-style: italic; color: #555;")
        refresh_layers_btn = QPushButton()
        refresh_layers_btn.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        refresh_layers_btn.setFixedSize(24, 24)
        refresh_layers_btn.setToolTip("Rafraîchir la liste")
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
        selection_group.setTitle("Sélection")
        selection_layout = QFormLayout()
        
        self.layer_input = QComboBox()
        self.layer_input.setToolTip("Couche dont les modifications sont enregistrées dans le journal local")
        self.layer_input.currentIndexChanged.connect(self._refresh_smart_bar)

        self.operation_input = QComboBox()
        self.operation_input.addItems(["Toutes", "UPDATE", "DELETE", "INSERT"])
        self.operation_input.setToolTip("Type d'opération à rechercher")
        self.operation_input.currentIndexChanged.connect(self._refresh_smart_bar)

        selection_layout.addRow("Couche:", self.layer_input)
        selection_layout.addRow("Opération:", self.operation_input)
        selection_group.setLayout(selection_layout)
        date_group = QgsCollapsibleGroupBox()
        date_group.setTitle("Période")
        date_layout = QFormLayout()
        
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
        
        date_shortcuts = QHBoxLayout()
        date_shortcuts.setSpacing(8)
        shortcut_button_width = 90
        shortcut_button_height = 28
        
        clock_icon = QgsApplication.getThemeIcon('/mIconClock.svg')
        
        min10_btn = QPushButton("10 min")
        min10_btn.setIcon(clock_icon)
        min10_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        min10_btn.clicked.connect(lambda: self.set_period("10min"))
        min10_btn.setToolTip("Dernières 10 minutes")
        
        min30_btn = QPushButton("30 min")
        min30_btn.setIcon(clock_icon)
        min30_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        min30_btn.clicked.connect(lambda: self.set_period("30min"))
        min30_btn.setToolTip("Dernières 30 minutes")
        
        hour1_btn = QPushButton("1 heure")
        hour1_btn.setIcon(clock_icon)
        hour1_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        hour1_btn.clicked.connect(lambda: self.set_period("1hour"))
        hour1_btn.setToolTip("Dernière heure")
        
        day1_btn = QPushButton("1 jour")
        day1_btn.setIcon(clock_icon)
        day1_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        day1_btn.clicked.connect(lambda: self.set_period("1day"))
        day1_btn.setToolTip("Dernières 24 heures")
        
        today_btn = QPushButton("Aujourd'hui")
        today_btn.setIcon(clock_icon)
        today_btn.setFixedSize(shortcut_button_width + 10, shortcut_button_height)
        today_btn.clicked.connect(lambda: self.set_period("today"))
        today_btn.setToolTip("Depuis minuit aujourd'hui")

        week_btn = QPushButton("Semaine")
        week_btn.setIcon(clock_icon)
        week_btn.setFixedSize(shortcut_button_width, shortcut_button_height)
        week_btn.clicked.connect(lambda: self.set_period("week"))
        week_btn.setToolTip("Depuis lundi 00:00")

        date_shortcuts.addWidget(min10_btn)
        date_shortcuts.addWidget(min30_btn)
        date_shortcuts.addWidget(hour1_btn)
        date_shortcuts.addWidget(day1_btn)
        date_shortcuts.addWidget(today_btn)
        date_shortcuts.addWidget(week_btn)
        date_shortcuts.addStretch()
        
        date_layout.addRow("Date début:", self.start_input)
        date_layout.addRow("Date fin:", self.end_input)
        date_layout.addRow("Raccourcis:", date_shortcuts)
        date_group.setLayout(date_layout)
        options_group = QgsCollapsibleGroupBox("Options")
        options_layout = QVBoxLayout()
        
        self.auto_zoom_check = QCheckBox("Zoomer automatiquement sur les résultats")
        self.auto_zoom_check.setIcon(QgsApplication.getThemeIcon('/mActionZoomToSelected.svg'))
        self.auto_zoom_check.setChecked(True)
        
        self.open_attribute_check = QCheckBox("Ouvrir la table d'attributs après chargement")
        self.open_attribute_check.setIcon(QgsApplication.getThemeIcon('/mActionOpenTable.svg'))
        self.open_attribute_check.setChecked(False)
        
        options_layout.addWidget(self.auto_zoom_check)
        options_layout.addWidget(self.open_attribute_check)
        options_group.setLayout(options_layout)
        results_group = QgsCollapsibleGroupBox()
        results_group.setTitle("Résultats")
        results_group.setCollapsed(True)
        results_layout = QVBoxLayout()
        self.results_info_label = QLabel("Aucune donnée récupérée")
        
        self.search_filter = QLineEdit()
        self.search_filter.setPlaceholderText("Rechercher (GID, utilisateur, ...)")
        self.search_filter.setClearButtonEnabled(True)
        self.search_filter.setToolTip("Filtrer les résultats en temps réel")
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
        self.select_all_button = QPushButton("Tout sélectionner")
        select_all_icon = QgsApplication.getThemeIcon('/mActionSelectAll.svg')
        self.select_all_button.setIcon(select_all_icon)
        self.select_all_button.setFixedHeight(selection_button_height)
        self.select_all_button.clicked.connect(self.select_all_rows)
        self.select_all_button.setEnabled(False)
        self.select_all_button.setToolTip("Sélectionner toutes les lignes du tableau")
        self.select_none_button = QPushButton("Tout désélectionner") 
        deselect_icon = QgsApplication.getThemeIcon('/mActionDeselectAll.svg')
        self.select_none_button.setIcon(deselect_icon)
        self.select_none_button.setFixedHeight(selection_button_height)
        self.select_none_button.clicked.connect(self.select_none_rows)
        self.select_none_button.setEnabled(False)
        self.select_none_button.setToolTip("Désélectionner toutes les lignes du tableau")
        
        self.export_csv_button = QPushButton("Exporter CSV")
        self.export_csv_button.setIcon(QgsApplication.getThemeIcon('/mActionFileSave.svg'))
        self.export_csv_button.setFixedHeight(selection_button_height)
        self.export_csv_button.setToolTip("Exporter les résultats en fichier CSV")
        self.export_csv_button.clicked.connect(self._export_csv)
        self.export_csv_button.setEnabled(False)
        
        self.selection_count_label = QLabel("")
        selection_buttons_layout.addWidget(self.select_all_button)
        selection_buttons_layout.addWidget(self.select_none_button)
        selection_buttons_layout.addWidget(self.export_csv_button)
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
        
        self.cancel_button = QPushButton("Fermer")
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
        self.cancel_button.setFixedSize(button_width, button_height)
        self.cancel_button.clicked.connect(self.cancel_operation)
        
        self.quick_last_btn = QPushButton("Derniers")
        self.quick_last_btn.setIcon(QgsApplication.getThemeIcon('/mIconClock.svg'))
        self.quick_last_btn.setFixedSize(button_width, button_height)
        self.quick_last_btn.setToolTip("Rechercher les 50 derniers evenements (24h)")
        self.quick_last_btn.clicked.connect(self._quick_last_changes)

        self.recover_button = QPushButton("Recover")
        self.recover_button.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        self.recover_button.setFixedSize(button_width, button_height)
        self.recover_button.clicked.connect(self.recover_and_load)
        
        self.restore_button = QPushButton("Restore")
        self.restore_button.setIcon(QgsApplication.getThemeIcon('/mActionSaveAllEdits.svg'))
        self.restore_button.setFixedSize(button_width, button_height)
        self.restore_button.clicked.connect(self.restore_selected_data)
        self.restore_button.setEnabled(False)
        
        self._apply_glow_effect(self.recover_button, RECOVER_GLOW_COLOR)
        self._apply_glow_effect(self.restore_button, RESTORE_GLOW_COLOR)
        self._apply_logo_glow_effect()
        
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_button)
        button_layout.addWidget(self.quick_last_btn)
        button_layout.addWidget(self.recover_button)
        button_layout.addWidget(self.restore_button)
        button_layout.addStretch()
        main_layout.addWidget(logo_label, 0, QtCompat.ALIGN_HCENTER)
        main_layout.addWidget(self.message_bar)
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
        date_group.setMinimumHeight(120)
        
        options_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_FIXED)
        options_group.setMinimumHeight(80)
        
        self.results_group.setSizePolicy(QtCompat.SIZE_PREFERRED, QtCompat.SIZE_EXPANDING)
        self.results_group.setMinimumHeight(0)
        
        self.setLayout(main_layout)
        
        self.recover_button.setEnabled(False)
        self._setup_shortcuts()

    def _setup_shortcuts(self) -> None:
        """UX-F02: Register keyboard shortcuts with tooltip hints."""
        QShortcut(QKeySequence("F5"), self, self.recover_and_load)
        QShortcut(QKeySequence("Ctrl+F"), self, self._focus_search_filter)
        QShortcut(QKeySequence("Ctrl+E"), self, self._export_csv)
        self.recover_button.setToolTip("Lancer la recherche (F5)")
        self.export_csv_button.setToolTip("Exporter les résultats (Ctrl+E)")
        self.search_filter.setToolTip("Filtrer les résultats (Ctrl+F)")

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
                f"Restaurer ({len(selected)} ligne(s))")
            restore_act.triggered.connect(self.restore_selected_data)
            menu.addSeparator()
        copy_act = menu.addAction("Copier les valeurs")
        copy_act.triggered.connect(self._copy_selected_to_clipboard)
        if selected and len(selected) == 1:
            row = selected[0].row()
            if row < len(self._search_events):
                event = self._search_events[row]
                layer_name = event.layer_name_snapshot or ""
                if layer_name:
                    filter_act = menu.addAction(
                        f"Filtrer sur '{layer_name}'")
                    filter_act.triggered.connect(
                        lambda: self._filter_on_layer(event.datasource_fingerprint))
        menu.addSeparator()
        sel_all = menu.addAction("Tout selectionner")
        sel_all.triggered.connect(self.select_all_rows)
        desel = menu.addAction("Tout deselectionner")
        desel.triggered.connect(self.select_none_rows)
        menu.exec_(self.table_widget.viewport().mapToGlobal(pos))

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
        self.message_bar.pushMessage(
            "Copie", f"{len(rows)} ligne(s) copiee(s).",
            QgisCompat.MSG_SUCCESS, 3)

    def _filter_on_layer(self, fingerprint: str) -> None:
        """Set layer combo to the given fingerprint."""
        idx = self.layer_input.findData(fingerprint)
        if idx >= 0:
            self.layer_input.setCurrentIndex(idx)

    def _quick_last_changes(self) -> None:
        """UX-B01: One-click search for last 50 events in 24h."""
        self.set_period("1day")
        self.layer_input.setCurrentIndex(0)
        self.operation_input.setCurrentText("Toutes")
        self.recover_and_load()

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
        dlg.exec_()
        self._refresh_smart_bar()

    def _build_empty_result_suggestion(self) -> str:
        """UX-B02: Build contextual suggestion when search returns 0 results."""
        parts = []
        if not self.tracking_toggle.isChecked():
            parts.append(
                "L'enregistrement est desactive. "
                "Activez-le pour capturer les modifications.")
            return " ".join(parts)
        start = self.start_input.dateTime()
        end = self.end_input.dateTime()
        span_secs = start.secsTo(end)
        if span_secs < 3600:
            parts.append("La periode est courte (< 1h). Essayez d'elargir a 24h ou 7 jours.")
        if self.layer_input.currentData():
            parts.append(
                "Un filtre de couche est actif. "
                "Essayez 'Toutes les couches sauvegardees'.")
        if self.operation_input.currentText() != "Toutes":
            parts.append(
                f"Filtre operation actif ({self.operation_input.currentText()}). "
                "Essayez 'Toutes'.")
        if not parts:
            parts.append("Aucun evenement pour ces criteres. Verifiez la periode et la couche.")
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
        effect.setColor(QColor(RECOVER_GLOW_COLOR.red(), RECOVER_GLOW_COLOR.green(), RECOVER_GLOW_COLOR.blue(), 0))
        self.logo_label.setGraphicsEffect(effect)
        self.logo_label.setProperty("glow_color", RECOVER_GLOW_COLOR)
        self.logo_label.setProperty("glow_base_blur", 0)
        self.logo_label.setProperty("glow_hover_blur", 28)
        self.logo_label.setProperty("glow_base_alpha", 0)
        self.logo_label.setProperty("glow_hover_alpha", min(255, RECOVER_GLOW_COLOR.alpha() + 75))

    def _start_logo_activity(self, color: QColor) -> None:
        self.logo_label.start_recovery_effect(color)
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
        if event.type() == QtCompat.EVENT_PALETTE_CHANGE and hasattr(self, 'logo_label'):
            self._load_themed_logo()
            self._refresh_smart_bar()
    
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
            text_color = self.palette().color(QtCompat.PALETTE_WINDOW_TEXT)
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
            label = QLabel(CHANGE_TYPE_LABELS[change_type])
            label.setStyleSheet("font-size:11px; padding-left:2px; padding-right:8px;")
            layout.addWidget(swatch)
            layout.addWidget(label)
        layout.addStretch()
        self._modified_only_check = QCheckBox("Modifications uniquement")
        self._modified_only_check.setToolTip("Afficher uniquement les colonnes modifiées")
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

    def _export_csv(self):
        """Export results table to CSV file."""
        if self.table_widget.rowCount() == 0:
            return
        filepath, _ = QFileDialog.getSaveFileName(
            self, "Exporter les résultats", "", "CSV (*.csv);;Tous les fichiers (*)"
        )
        if not filepath:
            return
        try:
            import csv
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                headers = []
                for c in range(self.table_widget.columnCount()):
                    item = self.table_widget.horizontalHeaderItem(c)
                    headers.append(item.text() if item else f"col_{c}")
                writer.writerow(headers)
                for row in range(self.table_widget.rowCount()):
                    if not self.table_widget.isRowHidden(row):
                        row_data = []
                        for col in range(self.table_widget.columnCount()):
                            item = self.table_widget.item(row, col)
                            row_data.append(item.text() if item else "")
                        writer.writerow(row_data)
            self.message_bar.pushMessage("Export", f"Résultats exportés vers {os.path.basename(filepath)}", QgisCompat.MSG_SUCCESS, 5)
        except Exception as e:
            self.message_bar.pushMessage("Erreur export", str(e), QgisCompat.MSG_CRITICAL, 0)
    
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
            self.start_input.setDateTime(QDateTime(QDate.currentDate(), QTime(0, 0, 0)))
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
        """Pulse progress"""
        value = self.progress_bar.value()
        if value == 0:
            self.progress_bar.setRange(0, 0)
        QApplication.processEvents()

    def update_progress(self, value):
        """Update progress"""
        if self.progress_bar.maximum() == 0:
            self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(value)
        QApplication.processEvents()

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
        
        self.progress_bar.setVisible(True)
        self.progress_timer.start(50)

        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.stop()
            self.worker_thread.wait(500)

        criteria = self._build_search_criteria()
        flog(f"recover_and_load: layer={criteria.datasource_fingerprint} op={criteria.operation_type} start={criteria.start_date} end={criteria.end_date}")

        self.cancel_button.setText("Arrêter")
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mTaskCancel.svg'))

        self.worker_thread = LocalSearchThread(self._journal, criteria)
        self.worker_thread.results_ready.connect(self._on_search_complete)
        self.worker_thread.phase_changed.connect(self.update_phase)
        self.worker_thread.error_occurred.connect(self.on_error)
        self._recover_started_at = time.monotonic()
        self._pending_search_result = None
        self._start_logo_activity(RECOVER_GLOW_COLOR)
        QTimer.singleShot(100, self.worker_thread.start)

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

    def _display_deferred_result(self) -> None:
        """Called by the deferred timer after the minimum animation elapsed."""
        result = self._pending_search_result
        self._pending_search_result = None
        if result is None:
            return
        self._display_search_result(result)

    def _display_search_result(self, result) -> None:
        """Finalize UI after search results are ready and animation is done."""
        self.progress_timer.stop()
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.progress_phase_label.setVisible(False)
        self.cancel_button.setText("Fermer")
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
        self.enable_controls(True)

        flog(f"_display_search_result: total_count={result.total_count} events={len(result.events)}")
        if result.total_count == 0:
            suggestion = self._build_empty_result_suggestion()
            self.message_bar.pushMessage(
                "Information",
                suggestion,
                QgisCompat.MSG_INFO, 8,
            )
            self._refresh_smart_bar()
            return

        self._search_events = result.events
        self._populate_results_table(result.events, result.total_count)
        self._smart_bar_message_override = f"{result.total_count} événement(s) chargé(s) dans la table."
        self._refresh_smart_bar()

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
        fixed_cols = ["#", "Date", "Utilisateur", "Opération", "Couche"]
        if has_geom_change:
            fixed_cols.append("Géométrie")
        columns = fixed_cols + attr_keys
        n_fixed = len(fixed_cols)
        self.table_widget.setSortingEnabled(False)
        self.table_widget.setRowCount(len(events))
        self.table_widget.setColumnCount(len(columns))
        self.table_widget.setHorizontalHeaderLabels(columns)
        self._modified_col_indices = set()

        for row_idx, event in enumerate(events):
            attrs = reconstruct_attributes(event)
            is_update = event.operation_type == "UPDATE"
            changed_keys = set()
            if is_update and event.attributes_json:
                try:
                    data = json.loads(event.attributes_json)
                    if "changed_only" in data:
                        changed_keys = set(data["changed_only"].keys())
                except (json.JSONDecodeError, TypeError):
                    pass

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
                geom_changed = is_geometry_only_update(event) or (
                    is_update and event.geometry_wkb is not None)
                row_values.append("Modifiée" if geom_changed else "")
            for col_idx, val in enumerate(row_values):
                item = QTableWidgetItem(val)
                if has_geom_change and col_idx == len(row_values) - 1 and val:
                    item.setBackground(CHANGE_TYPE_COLORS["geometry"])
                if geom_changed and is_geometry_only_update(event):
                    item.setBackground(CHANGE_TYPE_COLORS["geometry"])
                self.table_widget.setItem(row_idx, col_idx, item)

            for col_offset, key in enumerate(attr_keys):
                val = attrs.get(key)
                item = QTableWidgetItem(str(val) if val is not None else "")
                col_idx = n_fixed + col_offset
                if is_update and key in changed_keys:
                    item.setBackground(CHANGE_TYPE_COLORS["modified"])
                    self._modified_col_indices.add(col_idx)
                    try:
                        data = json.loads(event.attributes_json)
                        change = data.get("changed_only", {}).get(key, {})
                        if isinstance(change, dict):
                            item.setToolTip(f"Ancien: {val}\nActuel: {change.get('new')}")
                    except (json.JSONDecodeError, TypeError):
                        pass
                self.table_widget.setItem(row_idx, col_idx, item)

        self.table_widget.setSortingEnabled(True)
        self.table_widget.resizeColumnsToContents()
        self.results_info_label.setText(
            f"{total} \u00e9v\u00e9nement(s) trouv\u00e9(s), s\u00e9lectionnez les lignes \u00e0 restaurer"
        )
        self.results_group.setTitle(f"Résultats ({total} événements)")
        self.results_group.setCollapsed(False)
        self.select_all_button.setEnabled(True)
        self.select_none_button.setEnabled(True)
        self.export_csv_button.setEnabled(True)
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
        self.progress_timer.stop()
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.progress_phase_label.setVisible(False)
        self.cancel_button.setText("Fermer")
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
        self.enable_controls(True)
        self.log_error(f"Erreur de recuperation: {error_message}")
        user_msg = self._humanize_error(
            "Impossible de recuperer les donnees",
            error_message)
        self.message_bar.pushMessage(
            "Erreur", user_msg, QgisCompat.MSG_CRITICAL, 0)
    
    def on_log_message(self, message, level):
        """Log handler"""
        if level == 0:
            self.log_info(message)
        elif level == 1:
            self.log_warning(message)
        elif level == 2:
            self.log_error(message)
    
    def cancel_operation(self):
        """Cancel op: stop running thread, pending result, or close dialog."""
        if self._pending_search_result is not None:
            self._pending_search_result = None
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText("Fermer")
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            self.message_bar.pushMessage(
                "Opération annulée",
                "La récupération de données a été annulée",
                QgisCompat.MSG_WARNING, 3,
            )
            return
        if self.worker_thread and self.worker_thread.isRunning():
            self.worker_thread.stop()
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText("Fermer")
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            self.message_bar.pushMessage(
                "Opération annulée", 
                "La récupération de données a été annulée", 
                QgisCompat.MSG_WARNING, 
                3
            )
        elif self.restore_thread and self.restore_thread.isRunning():
            self.restore_thread.stop()
            self._stop_logo_activity()
            self.progress_timer.stop()
            self.progress_bar.setVisible(False)
            self.progress_phase_label.setVisible(False)
            self.cancel_button.setText("Fermer")
            self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
            self.enable_controls(True)
            self.message_bar.pushMessage(
                "Restauration annulée", 
                "La restauration a été annulée", 
                QgisCompat.MSG_WARNING, 
                3
            )
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

    def on_selection_changed(self):
        """Selection change"""
        if self.sync_in_progress:
            return
        selected_rows = set()
        for index in self.table_widget.selectionModel().selectedRows():
            selected_rows.add(index.row())
        self.selected_rows = list(selected_rows)
        total = self.table_widget.rowCount()
        selected = len(self.selected_rows)
        self.selection_count_label.setText(f"{selected} / {total} sélectionnées" if selected > 0 else "")
        self.restore_button.setEnabled(selected > 0)
    
    def select_all_rows(self):
        """Select all"""
        self.table_widget.selectAll()
    
    def select_none_rows(self):
        """Select none"""
        self.table_widget.clearSelection()
    
    
    
    def restore_selected_data(self):
        """Restore selected audit events to their source QGIS layers."""
        if not self.selected_rows:
            self.message_bar.pushMessage("Attention", "Sélectionnez au moins une ligne à restaurer.", QgisCompat.MSG_WARNING, 5)
            return

        selected_events = [
            self._search_events[r]
            for r in self.selected_rows
            if r < len(self._search_events)
        ]
        if not selected_events:
            return

        first = selected_events[0]
        layer_name = first.layer_name_snapshot or "?"
        reply = QMessageBox.question(
            self,
            "Confirmation de restauration",
            f"Restaurer {len(selected_events)} entité(s) [{first.operation_type}] "
            f"de la couche '{layer_name}' ?\n\n"
            "La couche cible doit être ouverte dans le projet QGIS.",
            QtCompat.MSG_YES | QtCompat.MSG_NO,
            QtCompat.MSG_NO,
        )
        if reply != QtCompat.MSG_YES:
            return

        by_ds = defaultdict(list)
        for event in selected_events:
            by_ds[event.datasource_fingerprint].append(event)

        total_ok, total_fail, errors = 0, 0, []
        self.restore_button.setEnabled(False)
        self.recover_button.setEnabled(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)
        self._start_logo_activity(RESTORE_GLOW_COLOR)

        processed = 0
        for fp, events_group in by_ds.items():
            layer = self._find_target_layer(events_group[0])
            if layer is None:
                name = events_group[0].layer_name_snapshot or fp
                errors.append(f"Couche '{name}' non trouvée dans le projet.")
                total_fail += len(events_group)
                continue
            report = restore_batch(layer, events_group)
            total_ok += len(report.succeeded)
            total_fail += len(report.failed)
            for eid, msg in report.failed.items():
                errors.append(f"Evt {eid}: {msg}")
            layer.triggerRepaint()
            processed += len(events_group)
            self.progress_bar.setValue(int(processed / len(selected_events) * 100))
            QApplication.processEvents()

        self.progress_bar.setVisible(False)
        self._stop_logo_activity()
        self.restore_button.setEnabled(bool(self.selected_rows))
        self.recover_button.setEnabled(True)

        if total_ok > 0 and total_fail == 0:
            self.message_bar.pushMessage(
                "Restauration", f"{total_ok} entité(s) restaurée(s) avec succès.",
                QgisCompat.MSG_SUCCESS, 5,
            )
            self.iface.mapCanvas().refresh()
        elif total_ok > 0:
            self.message_bar.pushMessage(
                "Restauration partielle", f"{total_ok} ok, {total_fail} échec(s).",
                QgisCompat.MSG_WARNING, 0,
            )
        else:
            self.message_bar.pushMessage(
                "Erreur de restauration", " | ".join(errors[:5]),
                QgisCompat.MSG_CRITICAL, 0,
            )

    def _find_target_layer(self, event):
        """Find the QGIS layer matching an audit event.

        Search order:
        1. Layer ID match in currently loaded project layers
        2. Datasource fingerprint match in loaded layers
        3. Fallback: recreate a temporary layer from the datasource registry
           (uses stored URI + QGIS auth system, never stores passwords)
        """
        for layer in QgsProject.instance().mapLayers().values():
            if not isinstance(layer, QgsVectorLayer):
                continue
            if layer.id() == event.layer_id_snapshot:
                return layer
        for layer in QgsProject.instance().mapLayers().values():
            if not isinstance(layer, QgsVectorLayer):
                continue
            try:
                if compute_datasource_fingerprint(layer) == event.datasource_fingerprint:
                    return layer
            except Exception:
                continue
        return self._try_restore_from_registry(event)

    def _try_restore_from_registry(self, event):
        """Attempt to create a layer from the datasource registry."""
        if self._journal is None or not self._journal.is_open:
            return None
        try:
            from .core.datasource_registry import lookup_datasource, create_layer_from_registry
            conn = self._get_dialog_read_conn()
            if conn is None:
                return None
            info = lookup_datasource(conn, event.datasource_fingerprint)
            if info is None:
                flog(f"_find_target_layer: no registry entry for {event.datasource_fingerprint}")
                return None
            flog(f"_find_target_layer: recreating layer from registry "
                 f"provider={info.provider_type} name={info.layer_name}")
            layer = create_layer_from_registry(info)
            if layer is not None and layer.isValid():
                QgsProject.instance().addMapLayer(layer, False)
                flog(f"_find_target_layer: temp layer added for restore")
                return layer
            return None
        except Exception as e:
            flog(f"_find_target_layer: registry fallback failed: {e}", "WARNING")
            return None
    
    def on_restore_complete(self, success, message, count):
        """Restore complete"""
        self.progress_timer.stop()
        self._stop_logo_activity()
        self.progress_bar.setVisible(False)
        self.cancel_button.setText("Fermer")
        self.cancel_button.setIcon(QgsApplication.getThemeIcon('/mActionRemove.svg'))
        self.restore_button.setEnabled(bool(self.selected_rows))
        self.recover_button.setEnabled(True)
        self.select_all_button.setEnabled(True)
        self.select_none_button.setEnabled(True)
        if success:
            self.iface.messageBar().pushMessage("RecoverLand", message, QgisCompat.MSG_SUCCESS, 5)
            self.table_widget.clearSelection()
        else:
            self.message_bar.pushMessage("Erreur de restauration", message, QgisCompat.MSG_CRITICAL, 0)
    
    
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
            self._close_dialog_read_conn()
            if hasattr(self, 'worker_thread') and self.worker_thread:
                self._disconnect_thread_signals(self.worker_thread)
                if self.worker_thread.isRunning():
                    self.worker_thread.stop()
                self.worker_thread = None
            
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
            self.message_bar.pushMessage(
                "Attention",
                "Aucun journal local disponible. "
                "Ouvrez un projet QGIS pour activer l'enregistrement.",
                QgisCompat.MSG_WARNING, 5)
            return False
        start_datetime = self.start_input.dateTime().toPyDateTime()
        end_datetime = self.end_input.dateTime().toPyDateTime()
        if start_datetime >= end_datetime:
            self.message_bar.pushMessage(
                "Attention",
                "La date de debut doit etre anterieure a la date de fin. "
                "Ajustez les dates ou utilisez un raccourci de periode.",
                QgisCompat.MSG_WARNING, 5)
            return False
        return True

    @staticmethod
    def _humanize_error(what: str, technical: str) -> str:
        """UX-H01: Convert technical error to user-friendly message."""
        lower = technical.lower()
        if "connection" in lower or "connect" in lower:
            return (f"{what} : connexion au journal impossible. "
                    "Verifiez que le fichier est accessible.")
        if "locked" in lower or "busy" in lower:
            return (f"{what} : le journal est verrouille par un autre processus. "
                    "Fermez les autres instances et reessayez.")
        if "disk" in lower or "space" in lower or "full" in lower:
            return (f"{what} : espace disque insuffisant. "
                    "Liberez de l'espace ou purgez les anciens evenements.")
        if "permission" in lower or "access" in lower:
            return (f"{what} : acces refuse au fichier journal. "
                    "Verifiez les permissions du dossier.")
        if "corrupt" in lower or "malformed" in lower:
            return (f"{what} : le journal semble endommage. "
                    "Ouvrez la maintenance pour verifier l'integrite.")
        return f"{what} : {technical}"
