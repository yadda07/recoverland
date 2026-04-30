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
    SQLiteAuditBackend,
    check_journal_integrity,
    get_journal_size_bytes, format_journal_size,
    get_journal_stats, evaluate_journal_health,
    format_integrity_message,
    generate_trace_id,
)
from .core.journal_manager import cleanup_orphan_journals, JournalLockError
from .core.user_identity import invalidate_cache as _invalidate_user_cache
from .status_bar_widget import StatusBarIndicator
import os
import json
from typing import Optional


def _detect_duplicate_recoverland(my_package: str) -> Optional[str]:
    """Return name of another loaded RecoverLand-class plugin, or None.

    Inspects qgis.utils.plugins for any entry whose plugin instance class is
    named 'RecoverPlugin' but whose package name differs from ours.
    Called at initGui() so QGIS has already filled qgis.utils.plugins for
    every plugin loaded before us in alphabetical order. Plugins loaded
    after us will run their own initGui later and detect us symmetrically.
    """
    try:
        import qgis.utils  # type: ignore
        for name, plugin_obj in dict(qgis.utils.plugins).items():
            if name == my_package:
                continue
            if type(plugin_obj).__name__ == 'RecoverPlugin':
                return name
    except Exception as exc:  # pragma: no cover - defensive: never block init
        flog(f"RecoverPlugin: duplicate detection error: {exc}", "WARNING")
    return None


class RecoverPlugin:
    def __init__(self, iface):
        self.iface = iface
        self.action = None
        self.dlg = None
        self._themed_action_icon = None
        self._journal = JournalManager()
        self._write_queue = WriteQueue()
        self._tracker = None
        self._sqlite_backend = None
        self._status_indicator = None
        self._integrity_result = None
        self._disk_timer = None
        self._disk_journal_path = None
        self._duplicate_of: Optional[str] = None

    def initGui(self):
        my_package = __package__ or os.path.basename(os.path.dirname(__file__))
        self._duplicate_of = _detect_duplicate_recoverland(my_package)
        if self._duplicate_of is not None:
            flog(
                f"RecoverPlugin: duplicate plugin detected my_package={my_package} "
                f"other_package={self._duplicate_of} mode=degraded",
                "CRITICAL",
            )
            qlog(QCoreApplication.translate(
                "RecoverPlugin",
                "RecoverLand : un autre plugin RecoverLand est deja charge "
                "(dossier '{other}'). Cette instance ('{mine}') est desactivee "
                "pour eviter la double capture des editions. Desactivez '{other}' "
                "dans le gestionnaire d'extensions puis redemarrez QGIS."
            ).format(other=self._duplicate_of, mine=my_package), "CRITICAL")
            self.action = QAction(
                "RecoverLand (desactive: doublon)", self.iface.mainWindow())
            self.action.triggered.connect(self._show_duplicate_warning)
            self.iface.addPluginToMenu("RecoverLand", self.action)
            return

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
        self.action.setToolTip(QCoreApplication.translate(
            "RecoverPlugin", "RecoverLand - Récupération de données d'audit"))
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
        flog("RecoverPlugin: initGui complete")

    def _show_duplicate_warning(self):
        """Triggered when user clicks the disabled-duplicate menu entry."""
        try:
            from qgis.PyQt.QtWidgets import QMessageBox
            QMessageBox.warning(
                self.iface.mainWindow(),
                QCoreApplication.translate("RecoverPlugin", "RecoverLand : doublon detecte"),
                QCoreApplication.translate(
                    "RecoverPlugin",
                    "Cette instance de RecoverLand est desactivee car un autre "
                    "plugin RecoverLand est deja charge dans le dossier '{other}'.\n\n"
                    "Pour resoudre :\n"
                    "1. Extensions > Gerer et installer les extensions > Installees\n"
                    "2. Decocher la version dans le dossier '{other}'\n"
                    "3. Redemarrer QGIS"
                ).format(other=self._duplicate_of or "?"),
            )
        except Exception as exc:  # pragma: no cover
            flog(f"RecoverPlugin: duplicate warning dialog error: {exc}", "WARNING")

    def unload(self):
        flog("RecoverPlugin: unload requested")
        if self._duplicate_of is not None:
            if self.action is not None:
                self.iface.removePluginMenu("RecoverLand", self.action)
            return

        self._shutdown_local_backend()

        try:
            QgsProject.instance().layersAdded.disconnect(self._on_layers_added)
            QgsProject.instance().layersRemoved.disconnect(self._on_layers_removed)
            QgsProject.instance().readProject.disconnect(self._on_project_opened)
            QgsProject.instance().cleared.disconnect(self._on_project_closed)
        except (TypeError, RuntimeError) as exc:
            flog(f"RecoverPlugin.unload: disconnect issue: {exc}", "DEBUG")

        if self._status_indicator is not None:
            self.iface.statusBarIface().removeWidget(self._status_indicator)
            self._status_indicator = None

        if self._themed_action_icon is not None:
            self._themed_action_icon.dispose()
            self._themed_action_icon = None
        self.iface.removePluginMenu("RecoverLand", self.action)
        self.iface.removeToolBarIcon(self.action)

    def run(self):
        if self._duplicate_of is not None:
            self._show_duplicate_warning()
            return
        first_open = self.dlg is None
        flog(f"RecoverPlugin: dialog requested first_open={first_open}")
        if self.dlg is None:
            self.dlg = RecoverDialog(
                self.iface, journal=self._journal,
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
            except (ValueError, TypeError) as exc:
                flog(f"RecoverPlugin: cannot parse tracked_layer_fingerprints: {exc}", "WARNING")

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

            if self._journal.is_lock_degraded:
                qlog(QCoreApplication.translate(
                    "RecoverPlugin",
                    "Protection multi-instance indisponible. "
                    "Evitez d'ouvrir ce projet dans plusieurs QGIS simultanement."
                ), "WARNING")

            flog("RecoverPlugin: local backend initialized")
        except JournalLockError as lock_err:
            flog(f"RecoverPlugin: journal locked by another instance: {lock_err}", "ERROR")
            qlog(QCoreApplication.translate(
                "RecoverPlugin",
                "Une autre instance de QGIS enregistre deja dans ce journal. "
                "Fermez-la pour activer l'enregistrement ici."
            ), "ERROR")
            if self._status_indicator is not None:
                self._status_indicator.set_no_project()
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

    def _on_project_opened(self) -> None:
        path = QgsProject.instance().absoluteFilePath() or "<unsaved>"
        flog(f"RecoverPlugin: project_opened path={path}")
        self._shutdown_local_backend()
        self._init_local_backend()
        self._update_status_bar()
        self._notify_dialog_project_switched()

    def _on_project_closed(self) -> None:
        flog("RecoverPlugin: project_closed")
        self._shutdown_local_backend()
        if self._status_indicator is not None:
            self._status_indicator.set_no_project()
        self._notify_dialog_project_switched()

    def _notify_dialog_project_switched(self) -> None:
        """Tell the dialog (if open) to fully reset for the new project."""
        if self.dlg is None:
            return
        switch = getattr(self.dlg, 'on_project_switched', None)
        if callable(switch):
            try:
                switch(tracker=self._tracker)
            except Exception as e:
                flog(f"RecoverPlugin: on_project_switched failed: {e}", "WARNING")

    def _setup_status_bar(self) -> None:
        """UX-G01: Add persistent status indicator to QGIS status bar."""
        try:
            self._status_indicator = StatusBarIndicator()
            self._status_indicator.toggle_requested.connect(self._toggle_tracking)
            self._status_indicator.open_dialog_requested.connect(self.run)
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
        except Exception as exc:
            flog(f"RecoverPlugin._update_status_bar: stats read failed: {exc}", "WARNING")
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
        if self._status_indicator is not None:
            self._status_indicator.pulse()
        if self.dlg is not None:
            fp = datasource_fp
            QTimer.singleShot(500, lambda: self.dlg.on_events_committed(fp))

    def _toggle_tracking(self) -> None:
        """Toggle tracking on/off from status bar click."""
        if self._tracker is None:
            return
        settings = QgsSettings()
        currently_on = settings.value(
            "RecoverLand/tracking_enabled", True, type=bool,
        )
        if currently_on:
            self._tracker.deactivate()
            settings.setValue("RecoverLand/tracking_enabled", False)
            flog("RecoverPlugin: tracking disabled by user")
        else:
            self._tracker.activate()
            settings.setValue("RecoverLand/tracking_enabled", True)
            flog("RecoverPlugin: tracking enabled by user")
        self._update_status_bar()

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

    # -----------------------------------------------------------------
    # Public API (FEAT-05): read-only observability surface for
    # PyQGIS console, other plugins, and external monitoring.
    # These methods never mutate plugin state.
    # -----------------------------------------------------------------

    def api_journal_path(self) -> Optional[str]:
        """Return the current journal file path, or None if none is open."""
        if self._journal is None:
            return None
        return self._journal.path

    def api_log_path(self) -> str:
        """Return the absolute path of the RecoverLand debug log file."""
        from .core.logger import _LOG_FILE
        return _LOG_FILE

    def api_is_tracking(self) -> bool:
        """Return True if edit-session tracking is active (non-suppressed)."""
        if self._tracker is None:
            return False
        return self._tracker.is_active and not self._tracker.is_suppressed

    def api_stats(self) -> dict:
        """Return a read-only snapshot of journal + queue + tracker stats.

        Keys: 'journal_path', 'journal_bytes', 'queue_pending',
              'tracker_active', 'session_events'.
        All values are safe to serialize (int, str, bool, None).
        """
        journal_path = self.api_journal_path()
        journal_bytes = 0
        if journal_path:
            try:
                journal_bytes = get_journal_size_bytes(journal_path)
            except OSError as exc:
                flog(f"api_stats: size read failed: {exc}", "DEBUG")
        queue_pending = 0
        if self._write_queue is not None:
            queue_pending = self._write_queue.pending_count
        session_events = 0
        if self._tracker is not None:
            session_events = self._tracker.session_event_count
        return {
            "journal_path": journal_path,
            "journal_bytes": journal_bytes,
            "queue_pending": queue_pending,
            "tracker_active": self.api_is_tracking(),
            "session_events": session_events,
        }
