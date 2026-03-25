try:
    from qgis.PyQt.QtWidgets import QAction
except ImportError:
    from qgis.PyQt.QtGui import QAction
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import QgsProject, QgsApplication, QgsSettings
from .recover_dialog import RecoverDialog
from .themed_action_icon import ThemedActionIconController
from .core import (
    flog, JournalManager, WriteQueue, EditSessionTracker,
    BackendRouter, SQLiteAuditBackend,
    check_journal_integrity,
    get_journal_size_bytes, format_journal_size,
    get_journal_stats, evaluate_journal_health, HealthLevel,
    format_integrity_message,
)
from .core.journal_manager import cleanup_orphan_journals
from .core.user_identity import invalidate_cache as _invalidate_user_cache
from .status_bar_widget import StatusBarIndicator
import os
import json


class RecoverPlugin:
    def __init__(self, iface):
        self.iface = iface
        self.action = None
        self.dlg = None
        self._themed_action_icon = None
        self._journal = JournalManager()
        self._write_queue = WriteQueue()
        self._tracker = None
        self._router = BackendRouter()
        self._sqlite_backend = None
        self._status_indicator = None
        self._integrity_result = None

    def initGui(self):
        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        logo_path = os.path.join(os.path.dirname(__file__), "logo.svg")
        theme_icon_path = icon_path if os.path.exists(icon_path) else None
        icon = None
        if os.path.exists(icon_path):
            icon = QIcon(icon_path)
        elif os.path.exists(logo_path):
            icon = QIcon(logo_path)
        
        if icon and not icon.isNull():
            self.action = QAction(icon, "RecoverLand", self.iface.mainWindow())
            self.action.setIconVisibleInMenu(True)
        else:
            self.action = QAction("RecoverLand", self.iface.mainWindow())
        
        self.action.setIconVisibleInMenu(True)
        self.action.setToolTip(QCoreApplication.translate("RecoverPlugin", "RecoverLand - Récupération de données d'audit"))
        self.action.triggered.connect(self.run)
        self.iface.addPluginToMenu("RecoverLand", self.action)
        self.iface.addToolBarIcon(self.action)

        if theme_icon_path:
            self._themed_action_icon = ThemedActionIconController(
                self.iface.mainWindow(),
                self.action,
                theme_icon_path,
            )

        QgsProject.instance().layersAdded.connect(self._on_layers_added)
        QgsProject.instance().layersRemoved.connect(self._on_layers_removed)
        QgsProject.instance().readProject.connect(self._on_project_opened)
        QgsProject.instance().cleared.connect(self._on_project_closed)

        self._init_local_backend()
        self._setup_status_bar()

    def unload(self):
        self._shutdown_local_backend()

        try:
            QgsProject.instance().layersAdded.disconnect(self._on_layers_added)
            QgsProject.instance().layersRemoved.disconnect(self._on_layers_removed)
            QgsProject.instance().readProject.disconnect(self._on_project_opened)
            QgsProject.instance().cleared.disconnect(self._on_project_closed)
        except (TypeError, RuntimeError):
            pass

        if self._status_indicator is not None:
            self.iface.statusBarIface().removeWidget(self._status_indicator)
            self._status_indicator = None

        if self._themed_action_icon is not None:
            self._themed_action_icon.dispose()
            self._themed_action_icon = None
        self.iface.removePluginMenu("RecoverLand", self.action)
        self.iface.removeToolBarIcon(self.action)

    def run(self):
        if self.dlg is None:
            self.dlg = RecoverDialog(
                self.iface, self._router, self._journal,
                tracker=self._tracker,
                write_queue=self._write_queue,
            )
        self.dlg.show()
        self.dlg.raise_()

    def _init_local_backend(self) -> None:
        """Initialize local SQLite backend and edit tracker."""
        try:
            project_path = QgsProject.instance().absoluteFilePath() or ""
            profile_path = QgsApplication.qgisSettingsDirPath()
            journal_path = self._journal.open_for_project(project_path, profile_path)

            integrity = check_journal_integrity(journal_path)
            self._integrity_result = integrity
            if not integrity.is_healthy:
                for issue in integrity.issues:
                    flog(f"Journal issue: {issue}", "WARNING")
                self._notify_integrity_issues(integrity)

            self._write_queue.start(journal_path)
            self._sqlite_backend = SQLiteAuditBackend(self._journal, self._write_queue)
            self._router.set_sqlite_backend(self._sqlite_backend)
            self._router.activate_local_mode()

            self._tracker = EditSessionTracker(self._write_queue, self._journal)
            self._tracker.set_commit_callback(self._on_events_committed)
            tracking_on = QgsSettings().value(
                "RecoverLand/tracking_enabled", True, type=bool
            )
            if tracking_on:
                self._tracker.activate()

            settings = QgsSettings()
            raw = settings.value("RecoverLand/tracked_layer_fingerprints", None)
            if raw is None:
                raw = settings.value("RecoverLand/tracked_layers", "[]")
            try:
                ids = json.loads(raw if isinstance(raw, str) else "[]")
                if (
                    isinstance(ids, list) and ids
                    and all(isinstance(val, str) and "::" in val for val in ids)
                ):
                    self._tracker.set_filter(set(ids))
            except Exception:
                pass

            self._connect_existing_layers()

            try:
                cleanup_orphan_journals(
                    profile_path,
                    current_path=journal_path,
                )
            except Exception as oe:
                flog(f"RecoverPlugin: orphan cleanup error: {oe}", "WARNING")

            flog("RecoverPlugin: local backend initialized")
        except Exception as e:
            flog(f"RecoverPlugin: local backend init failed: {e}", "ERROR")

    def _shutdown_local_backend(self) -> None:
        """Cleanly shut down local backend, flushing pending events."""
        _invalidate_user_cache()

        if self._tracker is not None:
            self._tracker.deactivate()
            self._tracker.disconnect_all()
            self._tracker = None

        self._write_queue.stop()

        if self._sqlite_backend is not None:
            self._sqlite_backend.close()
            self._sqlite_backend = None

        self._journal.close()
        self._router.deactivate_local_mode()
        self._router.clear_cache()

    def _connect_existing_layers(self) -> None:
        """Connect edit tracking to all currently loaded vector layers."""
        if self._tracker is None:
            return
        from qgis.core import QgsVectorLayer
        for layer in QgsProject.instance().mapLayers().values():
            if isinstance(layer, QgsVectorLayer):
                self._tracker.connect_layer(layer)

    def _on_layers_added(self, layers) -> None:
        if self._tracker is None:
            return
        from qgis.core import QgsVectorLayer
        for layer in layers:
            if isinstance(layer, QgsVectorLayer):
                self._tracker.connect_layer(layer)

    def _on_layers_removed(self, layer_ids) -> None:
        if self._tracker is None:
            return
        for lid in layer_ids:
            self._router.invalidate_layer(lid)

    def _on_project_opened(self) -> None:
        self._shutdown_local_backend()
        self._init_local_backend()
        self._update_status_bar()

    def _on_project_closed(self) -> None:
        self._shutdown_local_backend()
        if self._status_indicator is not None:
            self._status_indicator.set_no_project()

    def _setup_status_bar(self) -> None:
        """UX-G01: Add persistent status indicator to QGIS status bar."""
        try:
            self._status_indicator = StatusBarIndicator()
            self._status_indicator.clicked.connect(self.run)
            self.iface.statusBarIface().addPermanentWidget(self._status_indicator)
            self._update_status_bar()
        except Exception as e:
            flog(f"RecoverPlugin: status bar setup failed: {e}", "WARNING")

    def _update_status_bar(self) -> None:
        """Refresh the status bar indicator from current state."""
        if self._status_indicator is None:
            return
        if not self._journal.is_open or not self._journal.path:
            self._status_indicator.set_no_project()
            return
        tracking = QgsSettings().value("RecoverLand/tracking_enabled", True, type=bool)
        path = self._journal.path
        size_bytes = get_journal_size_bytes(path)
        size_str = format_journal_size(size_bytes)
        event_count = 0
        health_level = "healthy"
        try:
            conn = self._journal.create_read_connection()
            stats = get_journal_stats(conn)
            event_count = stats["total_events"]
            health = evaluate_journal_health(
                size_bytes, event_count,
                stats.get("oldest_event", ""),
                stats.get("newest_event", ""))
            health_level = health.level
            conn.close()
        except Exception:
            pass
        self._status_indicator.update_state(
            tracking, health_level, event_count, size_str)

    def _on_events_committed(self, event_count, layer_name,
                              is_mass_delete, delete_count) -> None:
        """UX-G02 + UX-B04: Handle commit callback from tracker."""
        show_notif = QgsSettings().value(
            "RecoverLand/show_commit_notifications", True, type=bool)
        from .compat import QgisCompat
        if is_mass_delete:
            self.iface.messageBar().pushMessage(
                "RecoverLand",
                QCoreApplication.translate(
                    "RecoverPlugin",
                    "Suppression massive detectee : {count} entite(s) "
                    "supprimee(s) sur '{layer}'. "
                    "Ouvrez RecoverLand pour verifier."
                ).format(count=delete_count, layer=layer_name),
                QgisCompat.MSG_WARNING, 10)
        elif show_notif:
            self.iface.messageBar().pushMessage(
                "RecoverLand",
                QCoreApplication.translate(
                    "RecoverPlugin",
                    "{count} modification(s) enregistree(s) sur '{layer}'."
                ).format(count=event_count, layer=layer_name),
                QgisCompat.MSG_SUCCESS, 3)
        self._update_status_bar()

    def _notify_integrity_issues(self, integrity) -> None:
        """UX-A02: Show integrity issues to user via QGIS message bar."""
        msg = format_integrity_message(integrity.issues, integrity.recovered_events)
        if msg is None:
            return
        try:
            from .compat import QgisCompat
            level = QgisCompat.MSG_WARNING if integrity.issues else QgisCompat.MSG_INFO
            self.iface.messageBar().pushMessage("RecoverLand", msg, level, 10)
        except Exception as e:
            flog(f"RecoverPlugin: notify integrity failed: {e}", "WARNING")
