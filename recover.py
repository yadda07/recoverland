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
    get_journal_stats,
    format_integrity_message,
    generate_trace_id,
    set_project_log_path, remove_project_log_handler,
)
from .core.journal_manager import cleanup_orphan_journals, JournalLockError
# JournalLockError.reason of a network journal an older client keeps in WAL.
from .core.journal_manager import _WAL_HELD_REASON
# How the tracker names the geometry among the fields of a status.
from .core.edit_tracker import _GEOMETRY_FIELD_LABEL
from .core.long_paths import when_needed
from .core.user_identity import invalidate_cache as _invalidate_user_cache
from .status_bar_widget import StatusBarIndicator
import functools
import inspect
import os
import json
import time
import traceback
from typing import Optional


def _positional_limit(fn) -> Optional[int]:
    """How many positional arguments *fn* takes after ``self`` (None: any)."""
    params = list(inspect.signature(fn).parameters.values())
    if any(p.kind is p.VAR_POSITIONAL for p in params):
        return None
    positional = [p for p in params
                  if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return max(0, len(positional) - 1)


def _plugin_slot(label: str, capture_at_risk: bool = True,
                 recovers: bool = False):
    """A plugin method called by Qt (signal, timer) or by the tracker.

    An exception escaping a slot called from C++ aborts QGIS under PyQt6
    when no global hook is installed (measured: 0xC0000409); with the hook
    of QGIS, the slot stops half-way (backend closed and not reopened, a
    layer never tracked, a timer gone) while the indicator keeps its
    colour. Here nothing escapes: the failure is logged at CRITICAL with
    its traceback, announced once in the message bar, and kept in the
    capture health until it is over (RecoverPlugin._slot_failed).

    ``label``: French, what failed, in the user's words (translated when
    shown). ``capture_at_risk``: the failure may leave capture off (ERROR)
    rather than only a service of the plugin (DEGRADED). ``recovers``: a
    later complete run of the same slot proves the failure over (periodic
    checks, idempotent actions); the others are cleared by the next
    backend initialisation, which rewires everything.

    Signals may carry more arguments than the method takes (QAction's
    ``checked``, readProject's document): they are dropped, the way PyQt
    does for a plain callable.
    """
    def decorate(fn):
        limit = _positional_limit(fn)

        @functools.wraps(fn)
        def slot(self, *args, **kwargs):
            if limit is not None:
                args = args[:limit]
            return self._guarded_call(fn.__name__, label, capture_at_risk,
                                      recovers, fn, self, *args, **kwargs)
        return slot
    return decorate


def _same_path(first: str, second: str) -> bool:
    try:
        return (os.path.normcase(os.path.abspath(first))
                == os.path.normcase(os.path.abspath(second)))
    except (TypeError, ValueError):
        return False


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
        self._disk_low_warned = False
        self._duplicate_of: Optional[str] = None
        # Runtime facts the status indicator is computed from (never a
        # QgsSettings flag alone).
        self._watchdog_timer = None
        self._reinit_timer = None
        self._last_disk_status = None
        self._prepare_failures = 0
        self._uncapturable_layers = set()
        # Additions of the open project journaled without the values their
        # source chose (tracker status 'insert_values_unverified'): layer id
        # -> {"name", "features", "fields"}. Kept in the capture health
        # until another project is opened (_forget_project_facts).
        self._unverified_inserts = {}
        # Changes of the open project whose NEW side is the value sent, not
        # read back (tracker status 'update_values_unverified'): layer id ->
        # {"name", "commits": {commit key: {"fids", "n"}}, "fields"}. Same
        # lifetime as _unverified_inserts.
        self._unverified_updates = {}
        self._alert_last = {}
        self._writer_restarts = 0
        self._project_capture_failures = 0
        self._cached_status_stats = None
        # Plugin slots that failed and are not over yet: key -> record
        # (see _slot_failed). The capture health reads them.
        self._slot_failures = {}
        # Another instance holds the project's journal: the refusal, and
        # the timer that watches for the journal to come free.
        self._journal_lock = None
        self._announced_lock = None
        self._lock_retry_timer = None
        self._lock_retry_path = ""
        # Refusals of that journal in a row, so the attempts space out
        # (see _retry_locked_journal); reset by a success or another project.
        self._lock_last_refusal = 0.0
        self._lock_free_refusals = 0
        self._lock_blind_attempts = 0
        self._lock_lost_told = False
        self._writer_health_error = ""

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
        # projectSaved, not fileNameChanged: the latter fires before the
        # file exists (Save As names the project, then writes it; a failed
        # write leaves the new name and no file) and again inside every
        # readProject, which already reinitializes the backend.
        QgsProject.instance().projectSaved.connect(self._on_project_saved)

        self._init_local_backend()
        self._setup_status_bar()
        flog("RecoverPlugin: initGui complete")

    @_plugin_slot("avertissement de doublon", capture_at_risk=False)
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
        if self._reinit_timer is not None:
            self._reinit_timer.stop()
            self._reinit_timer = None
        self._stop_lock_retry()
        if self._duplicate_of is not None:
            if self.action is not None:
                self.iface.removePluginMenu("RecoverLand", self.action)
            return

        # The dialog goes first, and that order is the whole point. Its
        # background readers (stats, version fetch) hold the journal, and
        # its restore runner still has trace events to enqueue. Shutting
        # the backend down first closed the journal under a worker that
        # was still reading it, and stopped the write queue under a
        # restore that was still writing: the layer edits landed, their
        # trace events did not, and the next rewind replayed them.
        # cleanup_resources() joins the workers and cancels the runner,
        # which is exactly what closeEvent() does while everything is
        # still alive.
        self._release_dialog()

        try:
            self._shutdown_local_backend()
        except Exception as exc:  # noqa: BLE001 - see below
            # The signals below must be disconnected whatever the backend
            # does on its way out: an unload stopped here left every slot
            # of an unloaded plugin connected to QgsProject.
            flog(f"RecoverPlugin.unload: backend shutdown failed: "
                 f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}",
                 "CRITICAL")

        # On its own: a save after unload must not find this handler still
        # connected, whatever the other disconnections do.
        try:
            QgsProject.instance().projectSaved.disconnect(self._on_project_saved)
        except (TypeError, RuntimeError) as exc:
            flog(f"RecoverPlugin.unload: projectSaved disconnect issue: {exc}", "DEBUG")
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

    def _release_dialog(self) -> None:
        """Tear the dialog down while the journal is still open.

        Called before _shutdown_local_backend so the dialog's background
        readers are joined and its restore runner cancelled before the
        write queue stops and the journal closes. Never raises: unload
        must finish removing the plugin from the UI whatever the dialog
        does on its way out.
        """
        if self.dlg is None:
            return
        try:
            self.dlg._review_wants_persist = False
            self.dlg.cleanup_resources()
        except Exception as exc:  # noqa: BLE001
            flog(f"RecoverPlugin.unload: dlg cleanup error: {exc}", "WARNING")
        try:
            self.dlg.close()
            self.dlg.deleteLater()
        except Exception as exc:  # noqa: BLE001
            # Still swallowed on purpose -- unload must finish removing the
            # plugin from the UI. But a dialog that refused to close is how
            # a widget survives an unload and reappears after a reload.
            flog(f"RecoverPlugin.unload: dlg close/deleteLater failed, the "
                 f"widget may outlive the unload ({exc!r})", "WARNING")
        self.dlg = None
        flog("RecoverPlugin.unload: dialog cleaned", "INFO")

    @_plugin_slot("ouverture de la fenetre RecoverLand", capture_at_risk=False,
                  recovers=True)
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

    def _init_local_backend(self) -> bool:
        """Initialize local SQLite backend and edit tracker.

        Returns True only when the initialisation ran to its end (journal
        open, tracker created and wired); False after a refusal or a
        failure, whatever was opened on the way. A caller that announces
        something ("l'enregistrement a repris") reads this, never
        ``journal.is_open`` alone: a failure after the open leaves the
        journal open with no tracker, and nothing captured.

        What the user is left with is always said, never guessed:
        - another instance holds the project's journal: its own words
          (JournalLockError.user_message), the status "enregistrement
          inactif (journal verrouille)", and a timer that reopens the
          journal as soon as it is free (_retry_locked_journal);
        - any other failure: CRITICAL with traceback, an alert that says
          whether nothing or only part of the backend is up, and ERROR in
          the capture health. It used to say "L'enregistrement est
          desactive pour cette session" even when the failure came after
          the tracker was capturing, under a grey "aucun projet" dot.
        A fresh backend rewires everything a failed slot may have left half
        done, so the failures recorded so far are dropped here; whatever
        fails during this initialisation is recorded again.
        """
        self._stop_lock_retry()
        self._journal_lock = None
        self._lock_lost_told = False
        self._writer_restarts = 0
        self._cached_status_stats = None
        self._slot_failures.clear()
        try:
            project_path = QgsProject.instance().absoluteFilePath() or ""
            profile_path = QgsApplication.qgisSettingsDirPath()
            journal_path = self._journal.open_for_project(project_path, profile_path)
            set_project_log_path(journal_path)

            integrity_trace_id = generate_trace_id()
            flog(f"[{integrity_trace_id}] RecoverPlugin: integrity check start path={journal_path}")
            integrity = check_journal_integrity(journal_path, trace_id=integrity_trace_id)
            self._integrity_result = integrity
            if integrity.pending_rejected > 0:
                qlog(f"RecoverLand: {integrity.pending_rejected} pending event(s) "
                     f"could not be recovered (see log for details)", "WARNING")
            if not integrity.is_healthy:
                for issue in integrity.issues:
                    flog(f"Journal issue: {issue}", "WARNING")
                if self._is_corrupt(integrity):
                    journal_path = self._handle_corrupt_journal(journal_path, profile_path, project_path)
                else:
                    self._notify_integrity_issues(integrity)

            self._write_queue.start(journal_path)
            self._report_outbox_replay()
            self._sqlite_backend = SQLiteAuditBackend(self._journal, self._write_queue)

            self._tracker = EditSessionTracker(self._write_queue, self._journal)
            self._tracker.set_commit_callback(self._on_events_committed)
            self._tracker.set_overflow_callback(self._on_queue_overflow)
            self._tracker.set_capture_status_callback(self._on_capture_status)
            self._write_queue.set_early_warning_callback(self._on_queue_early_warning)
            self._write_queue.set_failure_callback(self._on_writer_failure)
            self._start_watchdog()
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
                if (isinstance(ids, list) and ids and all(isinstance(val, str) and "::" in val for val in ids)):
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

            self._announced_lock = None
            self._reset_lock_refusals()
            flog("RecoverPlugin: local backend initialized")
            return True
        except JournalLockError as lock_err:
            self._on_journal_locked(lock_err)
            return False
        except Exception as e:  # noqa: BLE001 - said, see the docstring
            tr = QCoreApplication.translate
            if self._journal.is_open:
                text = tr(
                    "RecoverPlugin",
                    "Initialisation de RecoverLand incomplete ({err}) : "
                    "l'enregistrement des modifications peut etre partiel ou "
                    "inactif. L'indicateur de la barre d'etat passe en ERREUR ; "
                    "rouvrez le projet pour relancer la capture. Le detail est "
                    "dans le journal de RecoverLand.")
            else:
                text = tr(
                    "RecoverPlugin",
                    "Initialisation du journal RecoverLand impossible ({err}) : "
                    "l'enregistrement est INACTIF pour ce projet. Rouvrez ou "
                    "enregistrez le projet pour reessayer ; le detail est dans "
                    "le journal de RecoverLand.")
            self._slot_failed(
                "_init_local_backend", "initialisation du journal", e, True,
                text=text.format(err=f"{type(e).__name__}: {e}"))
            return False

    def _shutdown_local_backend(self) -> None:
        """Cleanly shut down local backend, flushing pending events."""
        # First: a lock retry left running would reopen, a few seconds
        # later, the journal of a project that is no longer open.
        self._stop_lock_retry()
        self._journal_lock = None
        self._announced_lock = None
        self._reset_lock_refusals()
        _invalidate_user_cache()
        self._stop_disk_monitor()
        self._stop_watchdog()

        if self._tracker is not None:
            self._project_capture_failures += int(
                getattr(self._tracker, "capture_failures", 0) or 0)
            self._tracker.deactivate()
            self._tracker.disconnect_all()
            self._tracker = None
        self._cached_status_stats = None

        self._write_queue.stop()

        if self._sqlite_backend is not None:
            self._sqlite_backend.close()
            self._sqlite_backend = None

        self._journal.close()
        remove_project_log_handler()

    def _connect_existing_layers(self) -> None:
        """Connect edit tracking to all currently loaded vector layers."""
        self._connect_layers(QgsProject.instance().mapLayers().values())

    def _connect_layers(self, layers) -> None:
        """Track each vector layer; one that cannot be tracked is named.

        Layer by layer: a failure on one layer used to stop the loop, and
        every layer after it went untracked along with it, silently.
        """
        if self._tracker is None:
            return
        from qgis.core import QgsVectorLayer
        for layer in layers:
            if not isinstance(layer, QgsVectorLayer):
                continue
            try:
                self._tracker.connect_layer(layer)
            except Exception as exc:  # noqa: BLE001 - named, see _slot_failed
                try:
                    name, layer_id = layer.name(), layer.id()
                except RuntimeError:
                    name, layer_id = "?", "?"
                self._slot_failed(
                    f"connect_layer:{layer_id}",
                    f"suivi de la couche '{name}' (ses modifications ne "
                    f"sont PAS enregistrees)", exc, True)

    @_plugin_slot("ajout de couches au projet")
    def _on_layers_added(self, layers) -> None:
        self._connect_layers(layers)

    @_plugin_slot("retrait de couches du projet", capture_at_risk=False)
    def _on_layers_removed(self, layer_ids) -> None:
        if self._tracker is None:
            return
        for lid in layer_ids:
            self._tracker.disconnect_layer_by_id(lid)

    @_plugin_slot("ouverture du projet")
    def _on_project_opened(self) -> None:
        path = QgsProject.instance().absoluteFilePath() or "<unsaved>"
        flog(f"RecoverPlugin: project_opened path={path}")
        self._forget_project_facts()
        self._shutdown_local_backend()
        self._init_local_backend()
        self._update_status_bar()
        self._notify_dialog_project_switched()

    @_plugin_slot("fermeture du projet")
    def _on_project_closed(self) -> None:
        flog("RecoverPlugin: project_closed")
        self._forget_project_facts()
        self._shutdown_local_backend()
        # From the facts, not set_no_project(): a failure recorded on the
        # way must stay visible.
        self._update_status_bar()
        self._notify_dialog_project_switched()
        # File > New and Close Project emit `cleared` and nothing after: the
        # backend used to stay down, and every edit of the new, unsaved
        # project went unrecorded behind a grey "no project" dot. Opening a
        # project emits `cleared` THEN `readProject` inside the same call,
        # so a zero-delay timer runs after both: it only opens the blank
        # project's journal when readProject did not open one.
        if self._reinit_timer is None:
            self._reinit_timer = QTimer()
            self._reinit_timer.setSingleShot(True)
            self._reinit_timer.timeout.connect(self._reinit_after_clear)
        self._reinit_timer.start(0)

    @_plugin_slot("ouverture du journal du nouveau projet")
    def _reinit_after_clear(self) -> None:
        if self._duplicate_of is not None:
            return
        if self._journal.is_open:
            return
        if self._journal_lock is not None:
            # readProject, in the same call as `cleared`, has just been
            # refused this project's journal by its lock, and the lock retry
            # watches it (the shutdown on `cleared` dropped any older
            # refusal). Asking again here doubled every refusal: one more
            # round trip through the other instance's lock file (and, for a
            # network journal an older client keeps in WAL, one more open of
            # the live journal), under a "blank project" line that was
            # false.
            flog("RecoverPlugin: journal refused at the project read, the "
                 "lock retry watches it; no second attempt", "DEBUG")
            return
        flog("RecoverPlugin: blank project, opening its journal so its edits "
             "are captured")
        self._init_local_backend()
        self._update_status_bar()
        self._notify_dialog_project_switched()

    def _forget_project_facts(self) -> None:
        """Facts about the history of the project being left.

        Called when a project is closed or another one read, never by a
        backend reinitialisation of the same project (a save that moves the
        journal carries that history along, gaps included). What is dropped
        was announced when it happened (persistent alert) and stays in the
        log; the indicator of the next project is about its own journal.
        """
        if self._unverified_inserts:
            flog(f"RecoverPlugin: project left, its incomplete additions stop "
                 f"counting in the capture health "
                 f"({sorted(v['name'] for v in self._unverified_inserts.values())})")
        if self._unverified_updates:
            flog(f"RecoverPlugin: project left, its changes not read back stop "
                 f"counting in the capture health "
                 f"({sorted(v['name'] for v in self._unverified_updates.values())})")
        self._unverified_inserts.clear()
        self._unverified_updates.clear()
        self._project_capture_failures = 0
        self._writer_restarts = 0
        self._cached_status_stats = None

    @_plugin_slot("rattachement du journal au projet enregistre")
    def _on_project_saved(self, _after_write: bool = False,
                          _saved_name: Optional[str] = None,
                          _planned: bool = False) -> None:
        """Make the journal follow the project to where it will be read.

        An unsaved project journals into a per-session file of the profile;
        saved, it is reopened from <project dir>/.recoverland/ (or, kept in
        a GeoPackage / PostgreSQL storage, from a journal keyed on its URI).
        Nothing listened to projectSaved: after the first "Save As" the
        session kept writing to the profile file, the reopened project
        found none of it, and the orphan was deleted 30 days later. "Save
        As" into another directory split the history the same way.

        Two steps, because the signal fires INSIDE QgsProject.write(),
        before a .qgz archive is on disk (measured, QGIS 4.2: isfile() is
        False there):

        1. In the signal: plan the carry, NOW. "New", "Open" and "Quit"
           answer "Save" by writing the project then clearing it (or
           unloading the plugins) in the same call, with no event-loop turn
           in between; a plan made one turn later found a closed journal
           and a blank project, and the history stayed behind. The plan is
           durable (profile) and carried out by JournalManager.close(),
           whoever closes the session journal; after a crash, by the next
           open of the project's journal.
        2. One event-loop turn later (_after_write), if the project is still
           the one saved: switch the backend to the project's journal (the
           close carries the history, under that journal's writer lock,
           idempotently by event_uid). When the project's journal cannot
           take it (locked by another instance, not writable), capture
           stays on the journal that holds the history and the next save
           retries: the user is told, nothing is split. A save while no
           journal is open (refused at open because another instance held
           it) is the moment to try again. If the project was cleared or
           replaced in between, the carry has already run: only its outcome
           is left to tell. After an unload nothing is reopened.

        Never raises: an exception escaping a slot called from C++ makes
        PyQt abort QGIS (measured: 0xC0000409 in the middle of the save).
        A carry that could not even be planned is said and kept in the
        capture health (DEGRADED, capture goes on in the current journal)
        until the next save plans it again; a failed switch is ERROR.
        """
        if self._duplicate_of is not None:
            return
        if not _after_write:
            saved_name = ""
            planned = False
            try:
                saved_name = QgsProject.instance().fileName() or ""
                if self._journal.is_open:
                    planned = self._journal.plan_history_carry(
                        QgsProject.instance().absoluteFilePath() or "",
                        QgsApplication.qgisSettingsDirPath(),
                        being_written=True) is not None
            except Exception as exc:  # noqa: BLE001 - see the docstring
                self._slot_failed(
                    "_on_project_saved.plan",
                    "preparation du rattachement de l'historique au projet "
                    "enregistre", exc, False,
                    text=QCoreApplication.translate(
                        "RecoverPlugin",
                        "Projet enregistre, mais RecoverLand n'a pas pu "
                        "preparer le rattachement de l'historique de cette "
                        "session a son journal ({err}). La capture continue "
                        "dans le journal actuel, sans perte ; enregistrez de "
                        "nouveau le projet pour reessayer. Le detail est dans "
                        "le journal de RecoverLand."
                    ).format(err=f"{type(exc).__name__}: {exc}"))
            QTimer.singleShot(
                0, lambda: self._on_project_saved(True, saved_name, planned))
            return
        if not self._journal.is_open and self._status_indicator is None:
            return
        try:
            report = None
            if (QgsProject.instance().fileName() or "") != (_saved_name or ""):
                # Cleared or replaced since the save: the session journal
                # was closed on the way, which carried the history.
                report = self._journal.last_carry_report if _planned else None
                flog(f"RecoverPlugin: project saved then closed in the same "
                     f"call; history carry outcome="
                     f"{report.as_dict() if report is not None else None}")
            else:
                project_path = QgsProject.instance().absoluteFilePath() or ""
                pending = self._journal.pending_carry
                # when_needed: a plain path of 260 characters or more
                # answers False for a file that exists (core/long_paths.py):
                # the save of a project in a deep folder was taken for a
                # failed one, the carry cancelled, and the history left in
                # the profile journal the orphan cleanup deletes.
                if (pending is not None and pending.project_file
                        and not os.path.isfile(when_needed(pending.project_file))):
                    self._journal.cancel_history_carry(
                        f"the save did not write {pending.project_file}")
                    return
                tracker = self._tracker
                restoring = self.dlg is not None and (
                    getattr(self.dlg, "_restore_runner", None) is not None)
                layer_ids = getattr(tracker, "suppressed_layer_ids", ()) or ()
                if callable(layer_ids):
                    layer_ids = layer_ids()
                suppressed = tracker is not None and (
                    bool(tracker.is_suppressed) or bool(layer_ids))
                if restoring or suppressed:
                    # A restore is writing through this backend: never pull
                    # it out from under it. Look again when it is done; a
                    # closed journal by then means unload or another
                    # project, whose close carried the history.
                    flog("RecoverPlugin: project saved during a restore, "
                         "journal switch deferred")
                    QTimer.singleShot(1000, lambda: self._resume_saved_follow_up(
                        _saved_name, _planned))
                    return
                if not self._journal.is_open:
                    flog("RecoverPlugin: project saved while no journal is "
                         "open, trying to open it again")
                    self._init_local_backend()
                    self._update_status_bar()
                    self._notify_dialog_project_switched()
                    return
                if pending is None:
                    return
                flog(f"RecoverPlugin: project saved, the journal follows it "
                     f"{pending.source} -> {pending.target} "
                     f"(project {project_path or _saved_name})")
                self._shutdown_local_backend()
                self._init_local_backend()
                self._update_status_bar()
                self._notify_dialog_project_switched()
                report = self._journal.last_carry_report
            if report is not None:
                level, text = report.user_message()
                if text:
                    self._alert("history_carry", text, level,
                                duration=0 if level == "CRITICAL" else 15)
        except Exception as exc:  # noqa: BLE001 - see the docstring
            self._slot_failed(
                "_on_project_saved", "rattachement du journal au projet "
                "enregistre", exc, True, alert_key="history_carry",
                text=QCoreApplication.translate(
                    "RecoverPlugin",
                    "Projet enregistre, mais RecoverLand n'a pas pu rattacher "
                    "son journal au projet ({err}). Verifiez l'indicateur de la "
                    "barre d'etat ; le detail est dans le journal de "
                    "RecoverLand."
                ).format(err=f"{type(exc).__name__}: {exc}"))

    @_plugin_slot("rattachement du journal au projet enregistre")
    def _resume_saved_follow_up(self, saved_name: Optional[str],
                                planned: bool) -> None:
        """The switch deferred by a restore: only if a journal is still open.

        A journal closed by then means unload or another project, whose
        close already carried the history.
        """
        if self._journal.is_open:
            self._on_project_saved(True, saved_name, planned)

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

    @staticmethod
    def _status_stats_fingerprint(path: str) -> tuple:
        """Cheap file+WAL stat signature to avoid re-querying unchanged journals."""
        out = [path]
        for p in (path, path + "-wal"):
            try:
                st = os.stat(when_needed(p))
                out.append((int(st.st_mtime_ns), int(st.st_size)))
            except OSError:
                out.append(None)
        return tuple(out)

    def _update_status_bar(self) -> None:
        """Refresh the status bar indicator from current state."""
        if self._status_indicator is None:
            return
        if not self._journal.is_open or not self._journal.path:
            # No journal is not always "no project": a journal held by
            # another instance, or a failure that left nothing open, is
            # said as such (LOCKED, ERROR), never as a grey dot.
            health = self._capture_health()
            if health.state == "NO_PROJECT":
                self._status_indicator.set_no_project()
            else:
                self._status_indicator.update_health(health)
            return
        path = self._journal.path
        size_bytes = get_journal_size_bytes(path)
        size_str = format_journal_size(size_bytes)
        stat_key = self._status_stats_fingerprint(path)
        cached = self._cached_status_stats
        if cached is not None and cached[0] == stat_key and not cached[3]:
            _k, event_count, recoverable_from, stats_error = cached
        else:
            event_count = 0
            recoverable_from = ""
            stats_error = ""
            try:
                conn = self._journal.create_read_connection()
                try:
                    stats = get_journal_stats(conn)
                    event_count = stats["total_events"]
                    from .core.retention import journal_coverage
                    recoverable_from = journal_coverage(conn).get("recoverable_from") or ""
                finally:
                    conn.close()
            except Exception as exc:
                stats_error = str(exc)
                flog(f"RecoverPlugin._update_status_bar: stats read failed: {exc}", "WARNING")
            self._cached_status_stats = (
                stat_key, event_count, recoverable_from, stats_error)
        health = self._capture_health()
        if stats_error:
            health.reasons.append(f"lecture du journal impossible : {stats_error}")
        self._status_indicator.update_health(
            health, journal_path=path, event_count=event_count,
            size_str=size_str, recoverable_from=recoverable_from)

    # Guarded here, not only by the tracker: this runs AFTER the events were
    # handed to durable storage, and a display failure raised into the
    # tracker's slot was counted as a failed CAPTURE (false alert, ERROR).
    @_plugin_slot("affichage d'un commit enregistre", capture_at_risk=False,
                  recovers=True)
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
        self._cached_status_stats = None
        self._update_status_bar()
        if self._status_indicator is not None:
            self._status_indicator.pulse()
        if self.dlg is not None:
            fp = datasource_fp
            QTimer.singleShot(500, lambda: self._forward_commit_to_dialog(fp))

    @_plugin_slot("mise a jour de la fenetre apres un commit",
                  capture_at_risk=False, recovers=True)
    def _forward_commit_to_dialog(self, datasource_fp: str) -> None:
        """Half a second after a commit: the dialog may be gone by then.

        An unload (plugin reload, QGIS quitting) inside those 500 ms set
        self.dlg to None, and the timer raised AttributeError into Qt.
        """
        dlg = self.dlg
        if dlg is None:
            return
        dlg.on_events_committed(datasource_fp)

    @_plugin_slot("activation / desactivation de l'enregistrement",
                  recovers=True)
    def _toggle_tracking(self) -> None:
        """Toggle tracking on/off from status bar click."""
        if self._tracker is None:
            return
        settings = QgsSettings()
        currently_on = settings.value(
            "RecoverLand/tracking_enabled", True, type=bool,
        )
        # Either way the user has taken over: the disk monitor must not
        # undo his choice when free space changes.
        settings.setValue("RecoverLand/tracking_disabled_by_disk", False)
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
        from .core.disk_monitor import (
            check_disk_for_path, format_disk_message, _CHECK_INTERVAL_SEC,
        )
        self._disk_journal_path = journal_path
        self._disk_low_warned = False

        def _check():
            if not self._journal or not self._journal.is_open:
                return
            status = check_disk_for_path(self._disk_journal_path)
            self._last_disk_status = status
            settings = QgsSettings()
            if status.is_critical:
                self._disk_low_warned = False
                if self._tracker and self._tracker.is_active:
                    self._tracker.deactivate()
                    settings.setValue("RecoverLand/tracking_enabled", False)
                    # Remember who cut the recording. The setting is
                    # persistent: without this marker the user stays
                    # without capture after freeing the disk, even across
                    # a restart, until he thinks of clicking the status
                    # bar indicator again.
                    settings.setValue(
                        "RecoverLand/tracking_disabled_by_disk", True)
                    self._alert("disk", format_disk_message(status),
                                "CRITICAL", duration=0)
                    self._update_status_bar()
                return
            if status.is_low:
                # Early warning: below the critical threshold the
                # recording is cut without notice, so say it once while
                # there is still room to act.
                if not self._disk_low_warned:
                    self._disk_low_warned = True
                    self._alert("disk_low", format_disk_message(status),
                                "WARNING")
                self._update_status_bar()
                return
            self._disk_low_warned = False
            if not settings.value(
                "RecoverLand/tracking_disabled_by_disk", False, type=bool,
            ):
                return
            settings.setValue("RecoverLand/tracking_disabled_by_disk", False)
            settings.setValue("RecoverLand/tracking_enabled", True)
            if self._tracker and not self._tracker.is_active:
                self._tracker.activate()
            qlog(QCoreApplication.translate(
                "RecoverPlugin",
                "Espace disque de nouveau suffisant : "
                "l'enregistrement a ete reactive."
            ))
            self._update_status_bar()

        def guarded_check():
            # A timer slot, and also called right here, inside
            # _init_local_backend: raised there, it made the init report
            # "L'enregistrement est desactive" while the tracker was already
            # capturing. It stops nothing; its failure is said (DEGRADED)
            # until a later check completes.
            self._guarded_call(
                "_check_disk", "surveillance de l'espace disque (la coupure "
                "avant disque plein ne fonctionne plus)", False, True, _check)

        self._disk_timer = QTimer()
        self._disk_timer.timeout.connect(guarded_check)
        self._disk_timer.start(_CHECK_INTERVAL_SEC * 1000)
        guarded_check()

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

        The journal moves with its companion files or not at all
        (_set_journal_aside), and the user reads the outcome: where the
        copy kept for analysis is, or why nothing could be moved.
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
            error, stranded = self._set_journal_aside(journal_path, corrupt_path)
            tr = QCoreApplication.translate
            if not error:
                flog(f"RecoverPlugin: corrupt journal renamed to {corrupt_path}")
                self._alert("corrupt_journal", tr(
                    "RecoverPlugin",
                    "Journal RecoverLand corrompu mis de cote pour analyse : "
                    "{aside}. Un nouveau journal est cree a sa place ; "
                    "l'historique que contenait l'ancien n'est plus dans le "
                    "journal que Restore et Rewind lisent."
                ).format(aside=corrupt_path), "WARNING", duration=0)
            else:
                flog(f"RecoverPlugin: rename failed: {error} "
                     f"stranded={stranded}", "ERROR")
                where = tr(
                    "RecoverPlugin",
                    "il reste en place, avec ses fichiers compagnons, et il "
                    "est rouvert tel quel") if not stranded else tr(
                    "RecoverPlugin",
                    "ces fichiers n'ont pas pu reprendre leur place et doivent "
                    "etre remis a cote de lui a la main : {files}"
                ).format(files=", ".join(stranded))
                self._alert("corrupt_journal", tr(
                    "RecoverPlugin",
                    "Le journal RecoverLand corrompu ({path}) n'a pas pu etre "
                    "mis de cote ({err}) : {where}. L'enregistrement des "
                    "modifications peut echouer ; fermez ce qui utilise ce "
                    "fichier puis rouvrez le projet."
                ).format(path=journal_path, err=error, where=where),
                    "CRITICAL", duration=0)

        new_path = self._journal.open_for_project(project_path, profile_path)
        set_project_log_path(new_path)
        flog(f"RecoverPlugin: fresh journal created at {new_path}")
        return new_path

    # The files SQLite keeps beside a journal: they belong to it and move
    # with it, or not at all.
    _JOURNAL_COMPANIONS = ("-wal", "-shm", "-journal")

    @classmethod
    def _set_journal_aside(cls, journal_path: str, aside: str):
        """Move a journal and its companion files to *aside*, all or nothing.

        Returns (error, stranded): ('', []) when everything moved; else the
        error, and every move already made is undone, ``stranded`` naming
        the files that could not be put back (normally none). Each path goes
        through when_needed: *aside* is 24 characters longer than the
        journal, so a plain journal of 232 to 250 characters (left plain by
        core.long_paths) puts it past MAX_PATH (measured: WinError 3). The
        rename of the journal then failed, or worse, succeeded while its
        -wal did not: the fresh journal opened empty next to the old -wal,
        which SQLite deletes as stale, and the writes it held vanished from
        the copy kept "for analysis".
        """
        moves = [(journal_path, aside)]
        for suffix in cls._JOURNAL_COMPANIONS:
            if os.path.lexists(when_needed(journal_path + suffix)):
                moves.append((journal_path + suffix, aside + suffix))
        done = []
        try:
            for src, dst in moves:
                os.rename(when_needed(src), when_needed(dst))
                done.append((src, dst))
        except OSError as exc:
            stranded = []
            for src, dst in reversed(done):
                try:
                    os.rename(when_needed(dst), when_needed(src))
                except OSError as back:
                    flog(f"RecoverPlugin: {dst} could not be moved back to "
                         f"{src}: {back}", "CRITICAL")
                    stranded.append(dst)
            return f"{type(exc).__name__}: {exc}", stranded
        return "", []

    @_plugin_slot("alerte de remplissage de la file d'ecriture",
                  capture_at_risk=False, recovers=True)
    def _on_queue_early_warning(self) -> None:
        """MED-07: the journal lags behind capture; say what that costs.

        WriteQueue calls this once per writer, on the GUI thread (enqueue
        runs in the tracker's commit slots and the runners' steps), when
        more than _QUEUE_EARLY_WARNING events wait for the journal. v7 never
        refuses nor switches capture off on volume: each of those events is
        already durable in the outbox. What the lag costs is a Restore, a
        Rewind or an Undo started meanwhile: its plan reads the journal, so
        it waits for the journal to catch up (RecoverDialog._journal_barrier)
        and is refused, with its reason, while it has not. The old text
        showed "80%%" (both signs: nothing formats this string) and promised
        a capture switched off that never happens.
        """
        try:
            pending = int(self._write_queue.pending_count)
        except Exception as exc:  # noqa: BLE001 - the count is only shown
            flog(f"RecoverPlugin: queue depth unreadable: {exc}", "WARNING")
            pending = 0
        count = str(pending) if pending else QCoreApplication.translate(
            "RecoverPlugin", "de nombreuses")
        self._alert("queue_lag", QCoreApplication.translate(
            "RecoverPlugin",
            "Le journal RecoverLand prend du retard sur l'enregistrement : "
            "{n} modification(s) attendent d'y etre ecrites. Aucune n'est "
            "perdue : chacune est deja conservee de facon durable (outbox) et "
            "la capture continue. Tant que ce retard dure, un Restore, un "
            "Rewind ou une annulation attend que le journal soit a jour, et "
            "est refuse s'il ne l'est pas a temps. Verifiez les performances "
            "du disque du journal."
        ).format(n=count), "WARNING")
        self._update_status_bar()

    @_plugin_slot("alerte de perte de modifications", capture_at_risk=False,
                  recovers=True)
    def _on_queue_overflow(self) -> None:
        """A commit's events reached NO durable storage (the only loss path).

        The write queue no longer refuses on volume. This fires when neither
        the outbox nor the journal could be written (volume gone, disk full)
        or when a batch was malformed and set aside. The edits are in the
        user's data already: say so loudly. Capture is NOT switched off
        persistently any more: switching it off guaranteed the loss of every
        later edit, and the next commit may well find the disk back.
        """
        self._alert("lost", QCoreApplication.translate(
            "RecoverPlugin",
            "Des modifications ont ete ecrites dans vos donnees mais "
            "RecoverLand n'a pu les enregistrer nulle part (journal et "
            "stockage de secours inaccessibles). Verifiez le disque du "
            "journal."), "CRITICAL", duration=0)
        self._update_status_bar()

    # -----------------------------------------------------------------
    # Capture health: watchdog, alerts, truthful status
    # -----------------------------------------------------------------

    _WATCHDOG_MS = 5000
    _ALERT_REPEAT_S = 120

    def _start_watchdog(self) -> None:
        self._stop_watchdog()
        self._watchdog_timer = QTimer()
        self._watchdog_timer.timeout.connect(self._watchdog_tick)
        self._watchdog_timer.start(self._WATCHDOG_MS)

    def _stop_watchdog(self) -> None:
        if self._watchdog_timer is not None:
            self._watchdog_timer.stop()
            self._watchdog_timer = None

    @_plugin_slot("surveillance de l'ecriture du journal (chien de garde)",
                  recovers=True)
    def _watchdog_tick(self) -> None:
        """Every 5 s: is the writer alive, is it keeping up, is the journal ours?

        The writer check runs under a key of its own (_guarded_call): a
        failure is CRITICAL with its traceback, said once, and ERROR in the
        capture health until a check completes again, while the rest of the
        tick still runs. It used to be swallowed here with an ERROR line:
        the tick then completed (and even counted as a recovery), and since
        watchdog_check is what marks a dead writer FAILED, a writer that
        died under a failing check stayed under a green dot.
        """
        self._guarded_call(
            "_watchdog_check", "surveillance du thread d'ecriture du journal",
            True, True, self._write_queue.watchdog_check)
        self._check_lock_lost()
        self._update_status_bar()

    def _check_lock_lost(self) -> None:
        """Another instance took this journal's writer lock over: say it once.

        The lock heartbeat detects it (JournalManager.is_lock_lost) and only
        logged it: from then on two instances may write the journal while
        the indicator said "protege". The capture health turns to ERROR; the
        message bar says it once per backend.
        """
        if not (self._journal.is_open and getattr(self._journal, "is_lock_lost", False)):
            return
        if self._lock_lost_told:
            return
        self._lock_lost_told = True
        flog(f"RecoverPlugin: event=journal_lock_lost path={self._journal.path}",
             "CRITICAL")
        self._alert("lock_lost", QCoreApplication.translate(
            "RecoverPlugin",
            "Le verrou du journal RecoverLand ({path}) a ete pris par une autre "
            "instance de QGIS : deux instances peuvent ecrire ce journal et "
            "les modifications faites ici ne sont plus protegees. Fermez "
            "l'autre instance puis rouvrez le projet."
        ).format(path=self._journal.path), "CRITICAL", duration=0)

    @_plugin_slot("reprise apres l'arret du thread d'ecriture")
    def _on_writer_failure(self, health) -> None:
        """The watchdog found the writer dead or stalled (GUI thread)."""
        state = getattr(health, "state", "?")
        err = getattr(health, "last_persistence_error", "")
        if state == "failed" and self._writer_restarts < 3:
            self._writer_restarts += 1
            restarted = False
            try:
                restarted = self._write_queue.restart_writer()
            except Exception as exc:  # noqa: BLE001
                flog(f"RecoverPlugin: writer restart failed: {exc}", "ERROR")
            flog(f"RecoverPlugin: event=writer_restart attempt="
                 f"{self._writer_restarts} ok={restarted} error={err!r}",
                 "CRITICAL")
            if restarted:
                self._alert("writer", QCoreApplication.translate(
                    "RecoverPlugin",
                    "Le thread d'ecriture de RecoverLand s'etait arrete ({err}). "
                    "Il a ete redemarre ; aucune modification n'a ete perdue "
                    "(elles etaient conservees dans l'outbox durable)."
                ).format(err=err or state), "WARNING")
                self._update_status_bar()
                return
        self._alert("writer", QCoreApplication.translate(
            "RecoverPlugin",
            "Ecriture du journal RecoverLand en echec ({err}). Vos "
            "modifications restent conservees de facon durable dans l'outbox "
            "et seront integrees des que le journal sera accessible."
        ).format(err=err or state), "CRITICAL", duration=0)
        self._update_status_bar()

    @_plugin_slot("alerte de capture", capture_at_risk=False, recovers=True)
    def _on_capture_status(self, kind: str, detail: dict) -> None:
        """Anything the tracker did that is not a normal, durable capture."""
        layer = detail.get("layer_name") or detail.get("layer_id") or "?"
        n = detail.get("n_events", 0)
        tr = QCoreApplication.translate
        if kind == "slot_failed":
            # core/edit_tracker._guarded: a capture slot raised. The commit
            # went on; the tracker counted it (capture_failures: ERROR). The
            # slot comes as PyQt's signature ("2beforeCommitChanges(bool)"):
            # the user reads the signal's name.
            slot = str(detail.get("slot") or "?").split("(")[0].lstrip("0123456789")
            self._alert(f"slot_failed:{slot}", tr(
                "RecoverPlugin",
                "Capture RecoverLand en echec (etape {slot} : {error}). Les "
                "modifications validees a ce moment sont ecrites dans vos "
                "donnees mais PAS dans l'historique RecoverLand : un Restore "
                "ou un Rewind ne les connaitra pas. Le detail est dans le "
                "journal de RecoverLand."
            ).format(slot=slot or "?",
                     error=detail.get("error") or "?"), "CRITICAL", duration=0)
        elif kind == "partial_commit":
            self._alert(f"partial:{detail.get('commit_id')}", tr(
                "RecoverPlugin",
                "Enregistrement partiel sur '{layer}' : la source a refuse une "
                "partie du commit. Les {n} modification(s) reellement ecrites "
                "sont journalisees ; le reste n'a pas ete ecrit."
            ).format(layer=layer, n=n), "WARNING")
        elif kind == "degraded":
            self._alert("degraded", tr(
                "RecoverPlugin",
                "Journal RecoverLand momentanement indisponible : les "
                "modifications de '{layer}' sont conservees dans le stockage "
                "de secours durable et seront integrees au journal."
            ).format(layer=layer), "WARNING")
        elif kind == "lost":
            self._on_queue_overflow()
        elif kind == "generation_failed":
            self._alert(f"gen:{detail.get('commit_id')}", tr(
                "RecoverPlugin",
                "RecoverLand n'a pas pu construire l'historique d'un commit sur "
                "'{layer}' ({error}). L'etat d'avant des entites est conserve "
                "au registre des commits pour analyse."
            ).format(layer=layer, error=detail.get("error", "")), "CRITICAL",
                duration=0)
        elif kind == "uncapturable":
            self._uncapturable_layers.add(detail.get("layer_id"))
            self._alert(f"uncap:{detail.get('layer_id')}", tr(
                "RecoverPlugin",
                "La couche '{layer}' est editee en groupe transactionnel "
                "automatique : RecoverLand ne peut pas lire son etat d'avant "
                "et NE CAPTURE PAS ses modifications."
            ).format(layer=layer), "CRITICAL", duration=0)
        elif kind == "prepare_not_durable":
            self._prepare_failures += 1
            self._alert("prepare", tr(
                "RecoverPlugin",
                "Etat d'avant du commit sur '{layer}' non sauvegarde sur disque "
                "avant l'ecriture : un plantage pendant ce commit ne serait pas "
                "rattrapable."
            ).format(layer=layer), "WARNING")
        elif kind == "reconciled":
            self._alert(f"reconciled:{detail.get('layer_id')}", tr(
                "RecoverPlugin",
                "Commit(s) interrompu(s) par un arret de QGIS sur '{layer}' "
                "rapproche(s) de la source : {landed} modification(s) "
                "reellement ecrite(s) journalisee(s), {na} non appliquee(s), "
                "{un} indeterminee(s)."
            ).format(layer=layer, landed=detail.get("landed", 0),
                     na=detail.get("not_applied", 0),
                     un=detail.get("unresolved", 0)),
                "CRITICAL" if detail.get("unresolved") else "WARNING",
                duration=0 if detail.get("unresolved") else 15)
        elif kind == "untracked_edit":
            self._alert(f"untracked:{detail.get('layer_id')}", tr(
                "RecoverPlugin",
                "La couche '{layer}' passe en edition mais RecoverLand ne la "
                "suit pas ({reason}) : ses modifications ne seront PAS "
                "enregistrees."
            ).format(layer=layer, reason=(
                tr("RecoverPlugin", "exclue de la selection des couches suivies")
                if detail.get("reason") == "not_in_tracking_selection"
                else tr("RecoverPlugin", "source non prise en charge"))),
                "CRITICAL", duration=0)
        elif kind == "large_commit":
            self._alert(f"large:{detail.get('layer_id')}", tr(
                "RecoverPlugin",
                "Commit tres volumineux capture sur '{layer}' ({n} entites, "
                "{mb} Mo) : il est enregistre, la capture continue."
            ).format(layer=layer, n=detail.get("features", "?"),
                     mb=detail.get("memory_mb", "?")), "WARNING")
        elif kind == "schema_change_not_journaled":
            self._alert(f"schema:{detail.get('layer_id')}", tr(
                "RecoverPlugin",
                "Suppression de champ(s) {fields} sur '{layer}' : les valeurs "
                "de ces champs ne sont PAS journalisees. Un restore ou un "
                "rewind ne pourra pas les rendre."
            ).format(fields=", ".join(detail.get("fields") or []), layer=layer),
                "CRITICAL", duration=0)
        elif kind == "insert_values_unverified":
            # core/edit_tracker._on_committed_features_added: the read-back
            # after the commit did not return these additions, so the
            # values the source chose (column DEFAULT, auto key, trigger)
            # are left out of their INSERT snapshot, never invented. Only
            # the technical log said it: the user took the addition for
            # journaled in full.
            _fids, n_features, shown = self._status_fids(detail)
            fields = [str(name) for name in (detail.get("fields") or [])]
            self._record_unverified_inserts(
                detail.get("layer_id") or layer, layer, n_features, fields)
            key = detail.get("commit_id") or f"{detail.get('layer_id')}:{shown}"
            self._alert(f"insert_unverified:{key}", tr(
                "RecoverPlugin",
                "Ajout(s) sur '{layer}' journalise(s) sans les valeurs choisies "
                "par la source : la relecture apres le commit n'a pas rendu "
                "{n} entite(s) (FID {fids}). Les champs {fields} de ces "
                "entites ne sont PAS dans leur historique (valeur inconnue, "
                "jamais inventee) : cet historique est incomplet. Le detail est "
                "dans le journal de RecoverLand."
            ).format(layer=layer, n=n_features, fids=shown,
                     fields=", ".join(fields) or "?"), "CRITICAL", duration=0)
        elif kind == "update_values_unverified":
            # core/edit_tracker._announce_unverified_update: the read-back
            # after the commit did not return these changed features, so
            # the NEW side of their UPDATE keeps what the edit buffer sent,
            # which the source may have stored otherwise (N(12,2) rounding,
            # UTC instant, 2D geometry). A Restore or a Rewind proves its
            # target by that side: it may refuse (post_attr_mismatch), and
            # the history may show a value the data never held. Only the
            # technical log said it, under a green indicator.
            fids, n_features, shown = self._status_fids(detail)
            fields = [tr("RecoverPlugin", "geometrie")
                      if name == _GEOMETRY_FIELD_LABEL else str(name)
                      for name in (detail.get("fields") or [])]
            # Without a commit id, a key of its own: never merged with, nor
            # silenced by, another commit's.
            commit_key = str(detail.get("commit_id")
                             or f"sans-commit-{time.monotonic_ns()}")
            self._record_unverified_updates(
                detail.get("layer_id") or layer, layer, commit_key, fids,
                n_features, fields)
            # The attributes and the geometry of one commit come as two
            # statuses with the same commit id: the fields tell them apart.
            self._alert(f"update_unverified:{commit_key}:{','.join(fields)}", tr(
                "RecoverPlugin",
                "Modification(s) sur '{layer}' journalisee(s) sans relecture : "
                "la source n'a pas rendu {n} entite(s) apres le commit (FID "
                "{fids}). Valeur(s) concernee(s) : {fields}. L'historique garde "
                "la valeur envoyee par QGIS, qui peut differer de celle que la "
                "source a enregistree (arrondi, fuseau horaire, geometrie "
                "simplifiee) : un Restore ou un Rewind de ces modifications "
                "peut etre refuse faute de la retrouver dans vos donnees. Le "
                "detail est dans le journal de RecoverLand."
            ).format(layer=layer, n=n_features, fids=shown,
                     fields=", ".join(fields) or "?"), "CRITICAL", duration=0)
        elif kind == "paused_buffer_pressure":
            self._alert("pressure", tr(
                "RecoverPlugin",
                "Capture RecoverLand mise en pause apres un commit tres "
                "volumineux ({mb} Mo) ; elle reprend a la prochaine session "
                "d'edition."
            ).format(mb=detail.get("memory_mb", "?")), "CRITICAL", duration=0)
        else:
            # Every status is something the tracker did not capture the
            # normal way. One this version has no branch for (added to the
            # tracker later) used to end in the technical log alone.
            self._alert(f"status:{kind}:{detail.get('commit_id') or layer}", tr(
                "RecoverPlugin",
                "La capture RecoverLand signale sur '{layer}' un cas que cette "
                "version du plugin ne sait pas decrire ({kind}) : l'historique "
                "de ce commit peut etre incomplet. Le detail est dans le "
                "journal de RecoverLand."
            ).format(layer=layer, kind=kind), "CRITICAL", duration=0)
        self._update_status_bar()

    @staticmethod
    def _status_fids(detail: dict):
        """(FIDs, total, text of at most 10 FIDs) of a capture status.

        The tracker caps the list it sends; ``n_features`` is the total.
        """
        fids = list(detail.get("fids") or [])
        n_features = int(detail.get("n_features") or len(fids) or 0)
        shown = ", ".join(str(fid) for fid in fids[:10]) or "?"
        if n_features > min(len(fids), 10):
            shown += QCoreApplication.translate(
                "RecoverPlugin", " ... ({n} au total)").format(n=n_features)
        return fids, n_features, shown

    _ALERT_LEVEL_RANK = {"INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

    def _alert(self, key: str, text: str, level: str = "WARNING",
               duration: int = 15) -> None:
        """Message bar + log, at most once per (key, severity) every _ALERT_REPEAT_S."""
        import time as _time
        qlog(text, level)
        try:
            from .core.logger import jlog
            jlog("user_alert", level, component="plugin", key=key, text=text)
        except Exception:  # noqa: BLE001 # nosec B110 - forensic log never breaks alert
            pass
        now = _time.monotonic()
        rank = self._ALERT_LEVEL_RANK.get(level, 2)
        prev = self._alert_last.get(key)
        if isinstance(prev, tuple):
            last, prev_rank = prev
        else:
            last, prev_rank = float(prev or 0.0), 4
        if last and (now - last < self._ALERT_REPEAT_S) and rank <= prev_rank:
            return
        new_rank = max(rank, prev_rank) if (last and now - last < self._ALERT_REPEAT_S) else rank
        self._alert_last[key] = (now, new_rank)
        try:
            from qgis.core import Qgis
            levels = {"INFO": Qgis.MessageLevel.Info,
                      "WARNING": Qgis.MessageLevel.Warning,
                      "ERROR": Qgis.MessageLevel.Critical,
                      "CRITICAL": Qgis.MessageLevel.Critical}
            bar = self.iface.messageBar()
            bar.pushMessage("RecoverLand", text,
                            levels.get(level, Qgis.MessageLevel.Warning),
                            duration)
        except Exception as exc:  # noqa: BLE001 - headless, shutting down
            flog(f"RecoverPlugin: message bar unavailable: {exc}", "DEBUG")

    def _capture_health(self):
        from .core.capture_health import evaluate_capture_health, disk_state
        tracker = self._tracker
        writer = None
        writer_error = ""
        try:
            writer = self._write_queue.health()
        except Exception as exc:  # noqa: BLE001 - said below, and ERROR
            # Not "no news, good news": writer=None used to end in
            # PROTECTED. The capture health says ERROR (writer_error); the
            # watchdog's own call reports it (CRITICAL, traceback, once).
            # Here, once per distinct error: this runs at every refresh.
            writer_error = f"{type(exc).__name__}: {exc}"
            if writer_error != self._writer_health_error:
                flog(f"RecoverPlugin: writer health unavailable: {writer_error}",
                     "WARNING")
        self._writer_health_error = writer_error
        journal_open = bool(self._journal.is_open)
        return evaluate_capture_health(
            journal_open=journal_open,
            tracker_active=bool(tracker is not None and tracker.is_active),
            writer=writer,
            disk=disk_state(self._last_disk_status),
            disabled_by_disk=QgsSettings().value(
                "RecoverLand/tracking_disabled_by_disk", False, type=bool),
            pressure_paused=bool(tracker is not None
                                 and getattr(tracker, "_pressure_paused", False)),
            capture_failures=(
                self._project_capture_failures
                + int(getattr(tracker, "capture_failures", 0) or 0)
            ),
            uncapturable_layers=len(self._uncapturable_layers),
            prepare_failures=self._prepare_failures,
            journal_locked="" if journal_open else self._lock_description(),
            lock_lost=bool(journal_open
                           and getattr(self._journal, "is_lock_lost", False)),
            carry_fallback=self._carry_fallback_reason(),
            internal_failures=[
                (f"{rec['label']} ({rec['error']})", rec["at_risk"])
                for rec in list(self._slot_failures.values())],
            writer_error=writer_error,
            unverified_inserts=[
                (rec["name"], rec["features"], sorted(rec["fields"]))
                for rec in list(self._unverified_inserts.values())],
            unverified_updates=[
                (rec["name"], self._unverified_update_count(rec),
                 sorted(rec["fields"]))
                for rec in list(self._unverified_updates.values())],
        )

    def _record_unverified_inserts(self, layer_key: str, name: str,
                                   n_features: int, fields) -> None:
        """Keep an incomplete addition in the capture health (DEGRADED).

        Per layer, over the commits of the open project: every commit adds
        new features, so the counts add up; the field names are merged.
        """
        rec = self._unverified_inserts.setdefault(
            str(layer_key), {"name": name, "features": 0, "fields": set()})
        rec["name"] = name
        rec["features"] += max(0, int(n_features or 0))
        rec["fields"].update(fields or ())

    def _record_unverified_updates(self, layer_key: str, name: str,
                                   commit_key: str, fids, n_features: int,
                                   fields) -> None:
        """Keep the changes whose NEW side was not read back (DEGRADED).

        Per layer and per commit: the attributes and the geometry of one
        commit come as two statuses that may name the same features, and a
        feature changed in one commit is ONE journaled UPDATE.
        """
        rec = self._unverified_updates.setdefault(
            str(layer_key), {"name": name, "commits": {}, "fields": set()})
        rec["name"] = name
        commit = rec["commits"].setdefault(str(commit_key),
                                           {"fids": set(), "n": 0})
        commit["fids"].update(str(fid) for fid in (fids or ()))
        commit["n"] = max(commit["n"], max(0, int(n_features or 0)))
        rec["fields"].update(fields or ())

    @staticmethod
    def _unverified_update_count(rec: dict) -> int:
        """Journaled UPDATEs of one layer whose NEW side was not read back.

        The union of the FIDs of each commit, exact while the tracker sends
        the whole list (50 FIDs); past it, the largest total of that commit,
        a lower bound (a sum would count twice a feature whose attributes
        and geometry were both left unread).
        """
        return sum(max(len(c["fids"]), c["n"]) for c in rec["commits"].values())

    def _carry_fallback_reason(self) -> str:
        """The last save could not carry the history: capture stayed behind.

        JournalManager keeps capturing into the journal that holds the
        history (last_carry_report.fell_back) and the next save retries. An
        alert says it at the save; this keeps it in the health until then.
        Only while that source journal is the open one: another project
        opened since has nothing to do with it.
        """
        report = getattr(self._journal, "last_carry_report", None)
        if report is None or not getattr(report, "fell_back", False):
            return ""
        current = self._journal.path if self._journal.is_open else None
        source = str(getattr(report, "source", "") or "")
        if not current or not source or not _same_path(source, current):
            return ""
        return (f"historique de la session non rattache au journal du projet "
                f"enregistre ({getattr(report, 'target', '') or '?'}) : "
                f"{getattr(report, 'error', '') or 'journal indisponible'} ; "
                f"la capture continue dans {source}, sans perte, jusqu'au "
                f"prochain enregistrement du projet, qui retentera le "
                f"rattachement")

    # -----------------------------------------------------------------
    # Slot failures: nothing escapes to Qt, nothing is hidden
    # -----------------------------------------------------------------

    def _guarded_call(self, key: str, label: str, capture_at_risk: bool,
                      recovers: bool, fn, *args, **kwargs):
        """Run *fn*; a failure is reported (_slot_failed), never raised."""
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - see _plugin_slot
            self._slot_failed(key, label, exc, capture_at_risk)
            return None
        if recovers and key in self._slot_failures:
            self._slot_recovered(key)
        return result

    def _slot_failed(self, key: str, label: str, exc: BaseException,
                     capture_at_risk: bool, text: str = "",
                     alert_key: str = "") -> None:
        """Log (CRITICAL, traceback), tell once, keep it in the capture health.

        Never raises: it runs where an exception has just been caught. A
        failure that repeats (a timer slot every 5 s) is logged again at
        most once per _ALERT_REPEAT_S, with its count, and not announced
        again.
        """
        try:
            shown = QCoreApplication.translate("RecoverPlugin", label) if label else key
            error = f"{type(exc).__name__}: {exc}"
            now = time.monotonic()
            previous = self._slot_failures.get(key)
            if previous is not None and previous["error"] == error:
                previous["count"] += 1
                if now - previous["logged_at"] >= self._ALERT_REPEAT_S:
                    previous["logged_at"] = now
                    flog(f"RecoverPlugin: event=slot_failed slot={key} "
                         f"repeated={previous['count']} error={error}", "CRITICAL")
                return
            self._slot_failures[key] = {
                "label": shown, "error": error, "at_risk": bool(capture_at_risk),
                "count": 1, "logged_at": now}
            trace = "".join(traceback.format_exception(
                type(exc), exc, exc.__traceback__))
            if len(trace) > 6000:
                trace = trace[:1500] + "\n...\n" + trace[-4000:]
            flog(f"RecoverPlugin: event=slot_failed slot={key} ({shown}) "
                 f"capture_at_risk={bool(capture_at_risk)} error={error}\n"
                 f"{trace}", "CRITICAL")
            tr = QCoreApplication.translate
            if not text:
                if capture_at_risk:
                    text = tr(
                        "RecoverPlugin",
                        "RecoverLand : defaillance interne ({what} : {err}). "
                        "L'enregistrement des modifications peut etre coupe ou "
                        "incomplet ; l'indicateur de la barre d'etat passe en "
                        "ERREUR. S'il y reste, rouvrez le projet (ou redemarrez "
                        "QGIS) ; le detail est dans le journal de RecoverLand.")
                else:
                    text = tr(
                        "RecoverPlugin",
                        "RecoverLand : defaillance interne ({what} : {err}). "
                        "La capture continue ; le detail est dans le journal de "
                        "RecoverLand.")
                text = text.format(what=shown, err=error)
            self._alert(alert_key or f"slot:{key}", text,
                        "CRITICAL" if capture_at_risk else "WARNING",
                        duration=0 if capture_at_risk else 30)
        except Exception as report_exc:  # noqa: BLE001 - last resort
            try:
                flog(f"RecoverPlugin: the failure of {key} ({exc!r}) could not "
                     f"be reported: {report_exc!r}", "CRITICAL")
            except Exception:  # noqa: BLE001 # nosec B110 - nothing left to tell
                pass
            return
        try:
            self._update_status_bar()
        except Exception as bar_exc:  # noqa: BLE001 - the bar may be what failed
            flog(f"RecoverPlugin: status bar refresh after the failure of "
                 f"{key} failed: {type(bar_exc).__name__}: {bar_exc}", "WARNING")

    def _slot_recovered(self, key: str) -> None:
        """A slot whose failure was recorded has just run to the end."""
        record = self._slot_failures.pop(key, None)
        if record is None:
            return
        flog(f"RecoverPlugin: event=slot_recovered slot={key} after "
             f"{record['count']} failure(s) ({record['error']})", "WARNING")
        if record["at_risk"]:
            self._alert(f"slot_ok:{key}", QCoreApplication.translate(
                "RecoverPlugin",
                "RecoverLand : {what} fonctionne de nouveau."
            ).format(what=record["label"]), "INFO")
        try:
            self._update_status_bar()
        except Exception as bar_exc:  # noqa: BLE001 - never raise from here
            flog(f"RecoverPlugin: status bar refresh after the recovery of "
                 f"{key} failed: {bar_exc}", "WARNING")

    # -----------------------------------------------------------------
    # Journal held by another instance: say it, show it, watch for it
    # -----------------------------------------------------------------

    _LOCK_RETRY_MS = 5000
    # An attempt the probe allowed and that was refused all the same (a
    # holder the probe cannot see, a third instance quicker than this one):
    # the next ones wait this long after the last refusal (then the last
    # value for good). Never an attempt every 5 s.
    _LOCK_BACKOFF_S = (30, 300)
    # A network journal an older client keeps in WAL: that client leaves no
    # live writer lock, only the journal's -wal / -shm files, there as long
    # as it has the journal open. While they are there, an attempt only at
    # these intervals, in case that client died with them open (an open
    # then succeeds and SQLite takes them over).
    _WAL_BLIND_RETRY_S = (300, 900, 1800)

    def _on_journal_locked(self, lock_err) -> None:
        """The project's journal is held by another live QGIS instance.

        Capture is inactive here. The user reads JournalLockError's own
        words (who holds it, what it means) and the status "enregistrement
        inactif (journal verrouille)"; a timer watches the holder and
        reopens the journal once it is free (_retry_locked_journal). The
        old text promised that closing the other instance would activate
        recording, and nothing ever retried. A refusal repeated by the
        retry (the same holder) is not announced again. The alert is keyed
        on the journal: under one fixed key, the refusal of a second
        project within _ALERT_REPEAT_S never reached the message bar.
        """
        path = str(getattr(lock_err, "journal_path", "") or "")
        if not _same_path(path, self._lock_retry_path):
            # Another journal than the one watched so far: its own count.
            self._reset_lock_refusals()
        self._lock_last_refusal = time.monotonic()
        signature = (path, str(getattr(lock_err, "reason", "") or ""),
                     getattr(lock_err, "holder_pid", None),
                     str(getattr(lock_err, "holder_host", "") or ""))
        again = self._announced_lock == signature
        flog(f"RecoverPlugin: journal locked by another instance: {lock_err}",
             "DEBUG" if again else "ERROR")
        self._journal_lock = lock_err
        if not again:
            self._announced_lock = signature
            user_message = getattr(lock_err, "user_message", None)
            text = user_message() if callable(user_message) else str(lock_err)
            if path and self._lock_is_wal_held(lock_err):
                text += " " + QCoreApplication.translate(
                    "RecoverPlugin",
                    "RecoverLand verifie toutes les {s} s si l'autre poste a "
                    "ferme le journal et reprend alors l'enregistrement de "
                    "lui-meme ; il le signalera."
                ).format(s=self._LOCK_RETRY_MS // 1000)
            elif path:
                text += " " + QCoreApplication.translate(
                    "RecoverPlugin",
                    "RecoverLand verifie toutes les {s} s si le journal est "
                    "libere et reprend alors l'enregistrement de lui-meme ; il "
                    "le signalera."
                ).format(s=self._LOCK_RETRY_MS // 1000)
            self._alert(f"journal_lock:{path}", text, "CRITICAL", duration=0)
        self._start_lock_retry(path)
        self._update_status_bar()

    def _reset_lock_refusals(self) -> None:
        self._lock_last_refusal = 0.0
        self._lock_free_refusals = 0
        self._lock_blind_attempts = 0

    @staticmethod
    def _lock_is_wal_held(lock_err) -> bool:
        """Refused because an older client keeps a network journal in WAL."""
        return (lock_err is not None and str(getattr(lock_err, "reason", "")
                                             or "") == _WAL_HELD_REASON)

    @staticmethod
    def _wal_side_files(journal_path: str) -> list:
        """The journal's -wal / -shm files still on disk (read-only probe).

        SQLite keeps them while any connection has the journal open in WAL
        mode and deletes them when the last one closes cleanly: for a
        network journal an older client keeps in WAL, they are the only
        sign that client is still there (its lock is never refreshed and
        reads as dead).
        """
        found = []
        for suffix in ("-wal", "-shm"):
            try:
                if os.path.exists(when_needed(journal_path + suffix)):
                    found.append(suffix)
            except (TypeError, ValueError, OSError):
                continue
        return found

    def _lock_description(self) -> str:
        """Who holds the journal, for the status (LOCKED); '' when nobody."""
        err = self._journal_lock
        if err is None:
            return ""
        who = []
        host = str(getattr(err, "holder_host", "") or "")
        pid = getattr(err, "holder_pid", None)
        if host:
            who.append(f"poste {host}")
        if pid is not None:
            who.append(f"processus {pid}")
        holder = f" ({', '.join(who)})" if who else ""
        path = str(getattr(err, "journal_path", "") or "")
        every = self._LOCK_RETRY_MS // 1000
        if self._lock_retry_timer is None:
            retry = ""
        elif self._lock_is_wal_held(err):
            retry = (f" ; verification toutes les {every} s, l'enregistrement "
                     f"reprend de lui-meme quand l'autre poste ferme le journal")
        else:
            retry = (f" ; verification toutes les {every} s, l'enregistrement "
                     f"reprend de lui-meme quand l'autre instance libere le "
                     f"journal")
        return (f"journal {path or '?'} tenu par une autre instance de QGIS"
                f"{holder} ; les modifications faites ici ne sont PAS "
                f"enregistrees{retry}")

    def _start_lock_retry(self, journal_path: str) -> None:
        self._stop_lock_retry()
        if not journal_path or self._duplicate_of is not None:
            return
        self._lock_retry_path = journal_path
        timer = QTimer()
        timer.timeout.connect(self._retry_locked_journal)
        timer.start(self._LOCK_RETRY_MS)
        self._lock_retry_timer = timer

    def _stop_lock_retry(self) -> None:
        """Called by _init_local_backend, _shutdown_local_backend and unload."""
        timer, self._lock_retry_timer = self._lock_retry_timer, None
        if timer is not None:
            timer.stop()

    @_plugin_slot("reprise de l'enregistrement apres le verrou du journal")
    def _retry_locked_journal(self) -> None:
        """Every _LOCK_RETRY_MS: is the journal free? Then capture resumes.

        The probes are read-only and take nothing; an attempt is not: it
        takes the writer lock (reclaiming a lock that reads as dead), opens
        a connection to the journal and, on a share, tries to change its
        journal mode, with its log lines (one of them CRITICAL). Made every
        5 s against a holder the lock probe cannot see (an older client
        keeping a network journal in WAL leaves no live lock:
        writer_lock_holder said "free"), that was 720 refusals an hour, each
        a round trip through the other client's lock file and an open of
        the live WAL journal over SMB. So:
        - a live writer lock (JournalManager.writer_lock_holder): no
          attempt;
        - a network journal an older client keeps in WAL: no attempt while
          its -wal / -shm files are there (_wal_side_files), except a blind
          one at growing intervals (_WAL_BLIND_RETRY_S), in case that client
          died with them open;
        - the probes say free: an attempt; refused all the same, the next
          ones wait longer (_LOCK_BACKOFF_S).
        The resumption is announced only when it happened
        (_announce_resumption).
        """
        if self._lock_retry_timer is None or self._duplicate_of is not None:
            return
        if self._journal.is_open:
            self._stop_lock_retry()
            return
        path = self._lock_retry_path
        if JournalManager.writer_lock_holder(path) is not None:
            return
        wal = self._lock_is_wal_held(self._journal_lock)
        side_files = self._wal_side_files(path) if wal else []
        if side_files:
            blind, done = True, self._lock_blind_attempts
            wait = self._WAL_BLIND_RETRY_S[
                min(done, len(self._WAL_BLIND_RETRY_S) - 1)]
        else:
            blind, done = False, self._lock_free_refusals
            wait = (0 if done == 0 else self._LOCK_BACKOFF_S[
                min(done, len(self._LOCK_BACKOFF_S)) - 1])
        if time.monotonic() - self._lock_last_refusal < wait:
            return
        if blind:
            flog(f"RecoverPlugin: journal {path} still has its "
                 f"{'/'.join(side_files)} file(s) (an older client may keep it "
                 f"open in WAL); blind attempt {done + 1}, {wait} s after the "
                 f"last refusal")
        else:
            flog(f"RecoverPlugin: journal {path} looks free (no live writer "
                 f"lock{', no -wal/-shm file left' if wal else ''}); attempt "
                 f"to resume capture ({done} refused so far)")
        self._stop_lock_retry()
        completed = self._init_local_backend()
        self._update_status_bar()
        self._notify_dialog_project_switched()
        if self._journal_lock is not None:
            # Refused again (_on_journal_locked has restarted the watch): the
            # next attempt of this kind waits longer.
            if blind:
                self._lock_blind_attempts += 1
            else:
                self._lock_free_refusals += 1
            return
        self._announce_resumption(completed, path)

    def _announce_resumption(self, completed: bool, lock_path: str) -> None:
        """Say that capture resumed, only when it did.

        It used to be said whenever the journal was open afterwards: an
        initialisation that failed after the open (no tracker, nothing
        captured) was announced as "l'enregistrement a repris" next to its
        own ERROR alert. The capture health decides: recording (PROTECTED,
        PERSISTING, DEGRADED) is announced; paused is said as paused;
        anything else leaves the failure's own alert alone.
        """
        state = self._capture_health().state if completed else ""
        path = self._journal.path if self._journal.is_open else lock_path
        tr = QCoreApplication.translate
        if state in ("PROTECTED", "PERSISTING", "DEGRADED"):
            self._alert(f"journal_released:{path}", tr(
                "RecoverPlugin",
                "Journal RecoverLand libere par l'autre instance de QGIS : "
                "l'enregistrement a repris dans cette instance ({path})."
            ).format(path=path), "INFO")
        elif state == "PAUSED":
            self._alert(f"journal_released:{path}", tr(
                "RecoverPlugin",
                "Journal RecoverLand libere par l'autre instance de QGIS et "
                "rouvert ({path}), mais l'enregistrement reste en pause dans "
                "cette instance : voir l'indicateur de la barre d'etat."
            ).format(path=path), "WARNING")
        else:
            flog(f"RecoverPlugin: journal {path} released by the other "
                 f"instance but capture did not resume (initialisation "
                 f"completed={completed}, capture state={state or 'n/a'}); its "
                 f"own alert stands, no resumption is announced", "WARNING")

    def _report_outbox_replay(self) -> None:
        """Say what a previous run left behind and what came back from it."""
        report = getattr(self._write_queue, "last_replay_report", None)
        if report is None or report.nothing_found:
            return
        info = report.as_dict()
        tr = QCoreApplication.translate
        if info["events_inserted"]:
            self._alert("replay", tr(
                "RecoverPlugin",
                "{n} modification(s) enregistree(s) lors de la session "
                "precedente mais pas encore indexee(s) ont ete integrees au "
                "journal."
            ).format(n=info["events_inserted"]), "INFO")
        if info["interrupted_commits"]:
            self._alert("interrupted", tr(
                "RecoverPlugin",
                "{n} commit(s) interrompu(s) par un arret de QGIS : l'etat "
                "d'avant des entites concernees est conserve au registre des "
                "commits ; verifiez ces couches."
            ).format(n=info["interrupted_commits"]), "CRITICAL", duration=0)
        if info["torn_lines"] or info["errors"]:
            self._alert("replay_err", tr(
                "RecoverPlugin",
                "Reprise partielle du stockage de secours : {torn} ligne(s) "
                "illisible(s) mise(s) en quarantaine, {err} erreur(s)."
            ).format(torn=info["torn_lines"], err=len(info["errors"])),
                "CRITICAL", duration=0)

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
