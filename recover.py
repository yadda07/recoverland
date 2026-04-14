try:
    from qgis.PyQt.QtWidgets import QAction
except ImportError:
    from qgis.PyQt.QtGui import QAction
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtCore import QCoreApplication, QTimer
from qgis.core import QgsProject, QgsApplication, QgsSettings
from .recover_dialog import RecoverDialog
from .themed_action_icon import ThemedActionIconController
from .core import (
    flog, qlog, JournalManager, WriteQueue, EditSessionTracker,
    BackendRouter, SQLiteAuditBackend,
    check_journal_integrity,
    get_journal_size_bytes, format_journal_size,
    get_journal_stats, evaluate_journal_health,
    format_integrity_message,
    generate_trace_id,
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
        self._disk_timer = None
        self._disk_journal_path = None

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

            integrity_trace_id = generate_trace_id()
            flog(f"[{integrity_trace_id}] RecoverPlugin: integrity check start path={journal_path}")
            integrity = check_journal_integrity(journal_path, trace_id=integrity_trace_id)
            self._integrity_result = integrity
            if not integrity.is_healthy:
                for issue in integrity.issues:
                    flog(f"Journal issue: {issue}", "WARNING")
                if self._is_corrupt(integrity):
                    journal_path = self._handle_corrupt_journal(journal_path, profile_path, project_path)
                else:
                    self._notify_integrity_issues(integrity)

            self._write_queue.start(journal_path)
            self._sqlite_backend = SQLiteAuditBackend(self._journal, self._write_queue)
            self._router.set_sqlite_backend(self._sqlite_backend)
            self._router.activate_local_mode()

            self._tracker = EditSessionTracker(self._write_queue, self._journal)
            self._tracker.set_commit_callback(self._on_events_committed)
            self._tracker.set_overflow_callback(self._on_queue_overflow)
            self._write_queue.set_early_warning_callback(self._on_queue_early_warning)
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

            self._run_auto_purge_if_enabled()
            self._start_disk_monitor(journal_path)

            flog("RecoverPlugin: local backend initialized")
        except Exception as e:
            flog(f"RecoverPlugin: local backend init failed: {e}", "ERROR")
            qlog(QCoreApplication.translate(
                "RecoverPlugin",
                "Initialisation du journal impossible. "
                "L'enregistrement est desactive pour cette session."
            ), "ERROR")
            if self._status_indicator is not None:
                self._status_indicator.set_no_project()

    def _shutdown_local_backend(self) -> None:
        """Cleanly shut down local backend, flushing pending events."""
        _invalidate_user_cache()
        self._stop_disk_monitor()

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
            self._tracker.disconnect_layer_by_id(lid)
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
                              is_mass_delete, delete_count,
                              datasource_fp="") -> None:
        """UX-G02 + UX-B04: Handle commit callback from tracker."""
        if is_mass_delete:
            qlog(QCoreApplication.translate(
                "RecoverPlugin",
                "Suppression massive: {count} entite(s) supprimee(s) sur '{layer}'."
            ).format(count=delete_count, layer=layer_name), "WARNING")
        else:
            qlog(QCoreApplication.translate(
                "RecoverPlugin",
                "{count} modification(s) enregistree(s) sur '{layer}'."
            ).format(count=event_count, layer=layer_name))
        if self._sqlite_backend is not None:
            self._sqlite_backend.invalidate_read_cache()
        self._update_status_bar()
        if self.dlg is not None:
            fp = datasource_fp
            QTimer.singleShot(500, lambda: self.dlg.on_events_committed(fp))

    def _run_auto_purge_if_enabled(self) -> None:
        """P1.2 / P0.4: Execute auto-purge at startup if the setting is enabled."""
        settings = QgsSettings()
        if not settings.value("RecoverLand/auto_purge", False, type=bool):
            return
        if not self._journal or not self._journal.is_open:
            return
        try:
            from .core import purge_old_events, RetentionPolicy
            days = int(settings.value("RecoverLand/retention_days", 365))
            max_ev = int(settings.value("RecoverLand/max_events", 1_000_000))
            policy = RetentionPolicy(retention_days=days, max_events=max_ev)
            conn = self._journal.get_connection()
            trace_id = generate_trace_id()
            flog(f"[{trace_id}] RecoverPlugin: auto-purge start days={days} max_events={max_ev}")
            result = purge_old_events(conn, policy, trace_id=trace_id)
            if result.deleted_count > 0:
                flog(f"RecoverPlugin: auto-purge deleted {result.deleted_count} events")
        except Exception as e:
            flog(f"RecoverPlugin: auto-purge failed: {e}", "WARNING")

    def _start_disk_monitor(self, journal_path: str) -> None:
        """P1.2 / P0.4: Start a periodic timer checking disk space every 5 minutes."""
        from .core.disk_monitor import check_disk_for_path, _CHECK_INTERVAL_SEC
        self._disk_journal_path = journal_path

        def _check():
            if not self._journal or not self._journal.is_open:
                return
            status = check_disk_for_path(self._disk_journal_path)
            if status.is_critical:
                if self._tracker and self._tracker.is_active:
                    self._tracker.deactivate()
                    QgsSettings().setValue("RecoverLand/tracking_enabled", False)
                    from .core.disk_monitor import format_disk_message
                    qlog(format_disk_message(status), "ERROR")
                    self._update_status_bar()

        self._disk_timer = QTimer()
        self._disk_timer.timeout.connect(_check)
        self._disk_timer.start(_CHECK_INTERVAL_SEC * 1000)
        _check()

    def _stop_disk_monitor(self) -> None:
        """Stop the periodic disk space timer."""
        if self._disk_timer is not None:
            self._disk_timer.stop()
            self._disk_timer = None

    @staticmethod
    def _is_corrupt(integrity) -> bool:
        """Return True if integrity issues indicate actual database corruption."""
        for issue in integrity.issues:
            lower = issue.lower()
            if "integrity check failed" in lower or "cannot open" in lower:
                return True
        return False

    def _handle_corrupt_journal(self, journal_path: str,
                                profile_path: str,
                                project_path: str) -> str:
        """P0.4: Rename corrupt journal and create a fresh one.

        Returns the new journal path (which may be the same path
        if the corrupt file was successfully renamed).
        """
        from qgis.PyQt.QtWidgets import QMessageBox
        from .compat import QtCompat
        from datetime import datetime, timezone

        self._journal.close()

        reply = QMessageBox.question(
            self.iface.mainWindow(),
            QCoreApplication.translate("RecoverPlugin", "Journal corrompu"),
            QCoreApplication.translate(
                "RecoverPlugin",
                "Le journal d'audit est corrompu.\n"
                "Voulez-vous le renommer et creer un nouveau journal ?\n\n"
                "Le fichier corrompu sera conserve pour analyse."
            ),
            QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_YES)

        if reply == QtCompat.MSG_YES:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            corrupt_path = journal_path + f".corrupt_{ts}"
            try:
                os.rename(journal_path, corrupt_path)
                for suffix in ("-wal", "-shm"):
                    sidecar = journal_path + suffix
                    if os.path.exists(sidecar):
                        os.rename(sidecar, corrupt_path + suffix)
                flog(f"RecoverPlugin: corrupt journal renamed to {corrupt_path}")
            except OSError as e:
                flog(f"RecoverPlugin: rename failed: {e}", "ERROR")

        new_path = self._journal.open_for_project(project_path, profile_path)
        flog(f"RecoverPlugin: fresh journal created at {new_path}")
        return new_path

    def _on_queue_early_warning(self) -> None:
        """MED-07: Warn user when write queue reaches 80% of hard limit."""
        qlog(QCoreApplication.translate(
            "RecoverPlugin",
            "File d'ecriture a 80%% de capacite. "
            "Si le probleme persiste, l'enregistrement sera desactive. "
            "Verifiez les performances disque."
        ), "WARNING")
        self._update_status_bar()

    def _on_queue_overflow(self) -> None:
        """P0.2: Alert user when write queue overflows and tracking is halted."""
        QgsSettings().setValue("RecoverLand/tracking_enabled", False)
        qlog(QCoreApplication.translate(
            "RecoverPlugin",
            "File d'ecriture saturee: enregistrement desactive. "
            "Evenements en attente sauvegardes. "
            "Relancez le suivi manuellement."
        ), "ERROR")
        self._update_status_bar()

    def _notify_integrity_issues(self, integrity) -> None:
        """UX-A02: Show integrity issues to user via QGIS message bar."""
        msg = format_integrity_message(integrity.issues, integrity.recovered_events)
        if msg is None:
            return
        try:
            level = "WARNING" if integrity.issues else "INFO"
            qlog(msg, level)
        except Exception as e:
            flog(f"RecoverPlugin: notify integrity failed: {e}", "WARNING")
