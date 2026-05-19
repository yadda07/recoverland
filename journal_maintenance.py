"""Journal maintenance panel for RecoverLand (UX-D01, UX-D02).

Provides journal health overview, retention configuration,
purge actions, vacuum, integrity check and export.
"""
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QSpinBox, QGroupBox, QMessageBox, QFileDialog,
    QProgressBar, QCheckBox,
)
from qgis.PyQt.QtCore import QTimer, QUrl, QElapsedTimer
from qgis.PyQt.QtGui import QDesktopServices
from qgis.core import QgsApplication, QgsSettings

from .compat import QtCompat
from .core import (
    flog,
    get_journal_size_bytes,
    get_journal_stats, count_purgeable_events,
    purge_old_events, vacuum_async,
    RetentionPolicy, check_journal_integrity,
)
from .core.health_monitor import (
    evaluate_journal_health, HealthLevel, _format_size,
    format_integrity_message,
)
from .core.time_format import compute_history_span
import os
import sqlite3


class JournalMaintenanceDialog(QDialog):
    """Maintenance and configuration dialog for the audit journal."""

    def __init__(self, journal, parent=None):
        super().__init__(parent)
        self._journal = journal
        self._vacuum_running = False
        self._vacuum_size_before = 0
        self._vacuum_timer = QElapsedTimer()
        self.setWindowTitle(self.tr("RecoverLand - Maintenance du journal"))
        self.setMinimumWidth(520)
        self.setMinimumHeight(400)
        self.resize(560, 480)
        self._setup_ui()
        self._refresh_stats()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(14, 14, 14, 14)

        info_group = QGroupBox(self.tr("Journal"))
        info_layout = QVBoxLayout()
        info_layout.setSpacing(4)
        self._path_label = QLabel("")
        self._path_label.setWordWrap(True)
        self._path_label.setTextInteractionFlags(QtCompat.TEXT_SELECTABLE_BY_MOUSE)
        self._size_label = QLabel("")
        self._events_label = QLabel("")
        self._span_label = QLabel("")
        self._schema_label = QLabel("")
        self._health_label = QLabel("")
        self._health_label.setWordWrap(True)
        info_layout.addWidget(self._path_label)
        row1 = QHBoxLayout()
        row1.setSpacing(16)
        row1.addWidget(self._size_label)
        row1.addWidget(self._events_label)
        row1.addWidget(self._span_label)
        row1.addWidget(self._schema_label)
        row1.addStretch()
        info_layout.addLayout(row1)
        info_layout.addWidget(self._health_label)
        info_group.setLayout(info_layout)

        retention_group = QGroupBox(self.tr("Retention"))
        retention_layout = QVBoxLayout()
        retention_layout.setSpacing(6)
        self._retention_days_spin = QSpinBox()
        self._retention_days_spin.setRange(7, 3650)
        self._retention_days_spin.setSuffix(self.tr(" jours"))
        self._retention_days_spin.setToolTip(self.tr("Duree de conservation des evenements"))
        self._max_events_spin = QSpinBox()
        self._max_events_spin.setRange(10000, 10_000_000)
        self._max_events_spin.setSingleStep(10000)
        self._max_events_spin.setToolTip(self.tr("Nombre maximum d'evenements"))
        self._auto_purge_check = QCheckBox(self.tr("Auto-purge au demarrage"))
        self._auto_purge_check.setToolTip(
            self.tr("Si active, les evenements hors politique sont supprimes a l'ouverture du journal"))
        spin_row = QHBoxLayout()
        spin_row.setSpacing(10)
        spin_row.addWidget(QLabel(self.tr("Conservation :")))
        spin_row.addWidget(self._retention_days_spin, 1)
        spin_row.addWidget(QLabel(self.tr("Max :")))
        spin_row.addWidget(self._max_events_spin, 1)
        retention_layout.addLayout(spin_row)
        save_row = QHBoxLayout()
        save_row.setSpacing(8)
        self._save_retention_btn = QPushButton(self.tr("Enregistrer"))
        self._save_retention_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSave.svg'))
        self._save_retention_btn.clicked.connect(self._save_retention)
        save_row.addWidget(self._auto_purge_check)
        save_row.addStretch()
        save_row.addWidget(self._save_retention_btn)
        retention_layout.addLayout(save_row)
        retention_group.setLayout(retention_layout)

        actions_group = QGroupBox(self.tr("Actions"))
        actions_layout = QVBoxLayout()
        actions_layout.setSpacing(6)

        purge_row = QHBoxLayout()
        purge_row.setSpacing(8)
        self._purge_btn = QPushButton(self.tr("Purger"))
        self._purge_btn.setIcon(QgsApplication.getThemeIcon('/mActionDeleteSelected.svg'))
        self._purge_btn.setToolTip(self.tr("Supprimer les evenements hors politique de retention"))
        self._purge_btn.clicked.connect(self._purge_events)
        self._purge_info = QLabel("")
        purge_row.addWidget(self._purge_btn)
        purge_row.addWidget(self._purge_info, 1)
        actions_layout.addLayout(purge_row)

        vacuum_row = QHBoxLayout()
        vacuum_row.setSpacing(8)
        self._vacuum_btn = QPushButton(self.tr("Compacter (VACUUM)"))
        self._vacuum_btn.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        self._vacuum_btn.setToolTip(self.tr("Recuperer l'espace disque apres une purge"))
        self._vacuum_btn.clicked.connect(self._vacuum_journal)
        self._vacuum_status = QLabel("")
        self._vacuum_status.setWordWrap(True)
        vacuum_row.addWidget(self._vacuum_btn)
        vacuum_row.addWidget(self._vacuum_status, 1)
        actions_layout.addLayout(vacuum_row)

        tools_row = QHBoxLayout()
        tools_row.setSpacing(6)
        integrity_btn = QPushButton(self.tr("Integrite"))
        integrity_btn.setIcon(QgsApplication.getThemeIcon('/mActionCheckGeometry.svg'))
        integrity_btn.setToolTip(self.tr("Lancer une verification d'integrite du journal"))
        integrity_btn.clicked.connect(self._check_integrity)
        export_btn = QPushButton(self.tr("Exporter"))
        export_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSaveAs.svg'))
        export_btn.setToolTip(self.tr("Copier le fichier journal vers un emplacement choisi"))
        export_btn.clicked.connect(self._export_journal)
        analyze_btn = QPushButton(self.tr("Analyser"))
        analyze_btn.setIcon(QgsApplication.getThemeIcon('/mActionIdentify.svg'))
        analyze_btn.setToolTip(
            self.tr("Mesurer la distribution, les doublons et le potentiel d'optimisation"))
        analyze_btn.clicked.connect(self._analyze_journal)
        open_folder_btn = QPushButton(self.tr("Dossier"))
        open_folder_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileOpen.svg'))
        open_folder_btn.setToolTip(self.tr("Ouvrir le dossier contenant le journal"))
        open_folder_btn.clicked.connect(self._open_journal_folder)
        tools_row.addWidget(integrity_btn)
        tools_row.addWidget(export_btn)
        tools_row.addWidget(analyze_btn)
        tools_row.addWidget(open_folder_btn)
        actions_layout.addLayout(tools_row)

        actions_group.setLayout(actions_layout)

        self._progress = QProgressBar()
        self._progress.setVisible(False)
        self._progress.setMaximumHeight(6)

        close_btn = QPushButton(self.tr("Fermer"))
        close_btn.clicked.connect(self.accept)

        layout.addWidget(info_group)
        layout.addWidget(retention_group)
        layout.addWidget(actions_group)
        layout.addWidget(self._progress)
        layout.addStretch()
        layout.addWidget(close_btn, 0, QtCompat.ALIGN_RIGHT)
        self.setLayout(layout)

    def _refresh_stats(self) -> None:
        """Refresh all displayed journal statistics."""
        path = self._journal.path if self._journal and self._journal.is_open else ""
        self._path_label.setText(path or self.tr("Aucun journal actif"))
        if not path:
            self._size_label.setText("")
            self._events_label.setText("")
            self._span_label.setText("")
            self._schema_label.setText("")
            self._health_label.setText(self.tr("Aucun journal"))
            return

        size_bytes = get_journal_size_bytes(path)
        self._size_label.setText(_format_size(size_bytes))

        conn = None
        try:
            conn = self._journal.create_read_connection()
            stats = get_journal_stats(conn)
            total = stats["total_events"]
            oldest = stats.get("oldest_event", "")
            newest = stats.get("newest_event", "")
            count_str = f"{total:,}".replace(",", " ")
            self._events_label.setText(f"{count_str} evt")
            span = compute_history_span(oldest, newest)
            self._span_label.setText(span if span else "")

            from .core.sqlite_schema import get_schema_version
            version = get_schema_version(conn)
            self._schema_label.setText(f"v{version}")

            health = evaluate_journal_health(size_bytes, total, oldest, newest)
            if health.level == HealthLevel.HEALTHY:
                self._health_label.setText(self.tr("Sain"))
                self._health_label.setStyleSheet("color: #2ecc71; font-weight: 600;")
            elif health.level == HealthLevel.INFO:
                self._health_label.setText(self.tr("Information : ") + health.message)
                self._health_label.setStyleSheet("color: #4285f4; font-weight: 600;")
            elif health.level == HealthLevel.WARNING:
                self._health_label.setText(self.tr("Attention : ") + health.message)
                self._health_label.setStyleSheet("color: #ff9800; font-weight: 600;")
            else:
                self._health_label.setText(self.tr("Critique : ") + health.message)
                self._health_label.setStyleSheet("color: #db4437; font-weight: 600;")

            policy = self._load_retention_policy()
            purgeable = count_purgeable_events(conn, policy)
            if purgeable > 0:
                self._purge_info.setText(self.tr("{count} evenement(s) purgeable(s)").format(count=purgeable))
            else:
                self._purge_info.setText(self.tr("Rien a purger"))
        except Exception as e:
            flog(f"JournalMaintenanceDialog: stats error: {e}", "WARNING")
            self._events_label.setText(self.tr("Erreur"))
            self._health_label.setText(self.tr("Impossible de lire les statistiques"))
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

        settings = QgsSettings()
        self._retention_days_spin.setValue(
            int(settings.value("RecoverLand/retention_days", 365)))
        self._max_events_spin.setValue(
            int(settings.value("RecoverLand/max_events", 1_000_000)))
        self._auto_purge_check.setChecked(
            settings.value("RecoverLand/auto_purge", False, type=bool))

    def _load_retention_policy(self) -> RetentionPolicy:
        settings = QgsSettings()
        days = int(settings.value("RecoverLand/retention_days", 365))
        max_ev = int(settings.value("RecoverLand/max_events", 1_000_000))
        return RetentionPolicy(retention_days=days, max_events=max_ev)

    def _save_retention(self) -> None:
        settings = QgsSettings()
        settings.setValue("RecoverLand/retention_days", self._retention_days_spin.value())
        settings.setValue("RecoverLand/max_events", self._max_events_spin.value())
        settings.setValue("RecoverLand/auto_purge", self._auto_purge_check.isChecked())
        flog(f"retention: saved days={self._retention_days_spin.value()} "
             f"max={self._max_events_spin.value()} "
             f"auto_purge={self._auto_purge_check.isChecked()}", "INFO")
        self._flash_save_confirm()

    def _flash_save_confirm(self) -> None:
        """Briefly show success state on the save button (icon + text + color)."""
        btn = self._save_retention_btn
        btn.setText(self.tr("Enregistre"))
        btn.setIcon(QgsApplication.getThemeIcon('/mIconSuccess.svg'))
        btn.setStyleSheet("color: #2ecc71; font-weight: 600;")
        btn.setEnabled(False)
        QTimer.singleShot(2000, self._reset_save_btn)

    def _reset_save_btn(self) -> None:
        btn = self._save_retention_btn
        btn.setText(self.tr("Enregistrer"))
        btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSave.svg'))
        btn.setStyleSheet("")
        btn.setEnabled(True)

    def _purge_events(self) -> None:
        if not self._journal or not self._journal.is_open:
            return
        policy = self._load_retention_policy()
        conn = None
        try:
            conn = self._journal.create_read_connection()
            purgeable = count_purgeable_events(conn, policy)
        except Exception:
            purgeable = 0
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        if purgeable == 0:
            QMessageBox.information(self, self.tr("Purge"), self.tr("Aucun evenement a purger."))
            return
        reply = QMessageBox.question(
            self, self.tr("Confirmer la purge"),
            self.tr("Supprimer {count} evenement(s) anciens ?\n"
                    "Cette action est irreversible.").format(count=purgeable),
            QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
        if reply != QtCompat.MSG_YES:
            return
        conn = None
        try:
            conn = self._journal.get_connection()
            result = purge_old_events(conn, policy)
            QMessageBox.information(
                self, self.tr("Purge terminee"),
                self.tr("{count} evenement(s) supprime(s).").format(count=result.deleted_count))
        except Exception as e:
            flog(f"purge error: {e}", "ERROR")
            QMessageBox.warning(
                self, self.tr("Erreur de purge"),
                self.tr("Une erreur est survenue lors de la purge. "
                        "Consultez le journal de logs."))
        self._refresh_stats()

    def _vacuum_journal(self) -> None:
        if not self._journal or not self._journal.path:
            return
        if self._vacuum_running:
            QMessageBox.information(
                self, self.tr("Compactage"),
                self.tr("Un compactage est deja en cours. Veuillez patienter."))
            return

        path = self._journal.path
        size_before = get_journal_size_bytes(path)
        size_str = _format_size(size_before)

        free_pages, page_size = self._get_freelist_info(path)
        reclaimable = free_pages * page_size
        if free_pages == 0:
            self._vacuum_status.setText(self.tr("Deja compacte"))
            self._vacuum_status.setStyleSheet("color: #4285f4; font-weight: 600;")
            QMessageBox.information(
                self, self.tr("Compactage inutile"),
                self.tr(
                    "Le journal est deja compacte (0 pages libres).\n\n"
                    "Taille : {size}\n\n"
                    "Le compactage ne recupere de l'espace que si des\n"
                    "evenements ont ete purges au prealable.\n\n"
                    "Utilisez d'abord 'Purger' pour supprimer les anciens\n"
                    "evenements, puis compactez."
                ).format(size=size_str))
            flog(f"vacuum: skipped, freelist_count=0 size={size_before}", "INFO")
            return

        reclaimable_str = _format_size(reclaimable)
        reply = QMessageBox.question(
            self, self.tr("Confirmer le compactage"),
            self.tr(
                "Le compactage (VACUUM) reconstruit le fichier journal.\n\n"
                "Taille actuelle : {size}\n"
                "Espace recuperable : {reclaimable} ({pages} pages libres)\n\n"
                "Cette operation reecrit physiquement tout le fichier.\n"
                "Continuer ?"
            ).format(size=size_str, reclaimable=reclaimable_str, pages=free_pages),
            QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
        if reply != QtCompat.MSG_YES:
            return

        self._vacuum_running = True
        self._vacuum_size_before = size_before
        self._vacuum_btn.setEnabled(False)
        self._vacuum_timer.start()
        self._vacuum_status.setText(self.tr("Compactage en cours..."))
        self._vacuum_status.setStyleSheet("color: #ff9800; font-weight: 600;")
        self._progress.setRange(0, 0)
        self._progress.setVisible(True)
        flog(f"vacuum: user initiated, size_before={size_before}", "INFO")

        def on_done(success):
            QTimer.singleShot(0, lambda: self._on_vacuum_finished(success))

        vacuum_async(path, callback=on_done)

    def _on_vacuum_finished(self, success: bool) -> None:
        elapsed_ms = self._vacuum_timer.elapsed()
        self._progress.setVisible(False)
        self._vacuum_running = False
        self._vacuum_btn.setEnabled(True)

        if success:
            path = self._journal.path
            size_after = get_journal_size_bytes(path) if path else 0
            saved = self._vacuum_size_before - size_after
            elapsed_s = elapsed_ms / 1000.0
            before_str = _format_size(self._vacuum_size_before)
            after_str = _format_size(size_after)
            saved_str = _format_size(max(saved, 0))

            result_msg = self.tr(
                "Compactage termine en {time:.1f}s\n"
                "Avant : {before}\n"
                "Apres : {after}\n"
                "Espace recupere : {saved}"
            ).format(time=elapsed_s, before=before_str,
                     after=after_str, saved=saved_str)

            self._vacuum_status.setText(
                self.tr("Termine ({time:.1f}s) - {saved} recupere(s)").format(
                    time=elapsed_s, saved=saved_str))
            self._vacuum_status.setStyleSheet("color: #2ecc71; font-weight: 600;")

            flog(f"vacuum: done elapsed_ms={elapsed_ms} "
                 f"size_before={self._vacuum_size_before} "
                 f"size_after={size_after} saved={saved}", "INFO")

            QMessageBox.information(
                self, self.tr("Compactage termine"), result_msg)
            self._refresh_stats()
            self._flash_size_change(before_str, after_str)
        else:
            self._vacuum_status.setText(self.tr("Echec du compactage"))
            self._vacuum_status.setStyleSheet("color: #db4437; font-weight: 600;")
            flog(f"vacuum: failed elapsed_ms={elapsed_ms}", "ERROR")
            QMessageBox.warning(
                self, self.tr("Echec du compactage"),
                self.tr(
                    "Le compactage a echoue apres {time:.1f}s.\n"
                    "Le journal n'a pas ete modifie.\n\n"
                    "Verifiez l'integrite du journal et consultez les logs."
                ).format(time=elapsed_ms / 1000.0))

    def _get_freelist_info(self, path: str) -> tuple:
        """Return (freelist_count, page_size) from SQLite PRAGMAs."""
        try:
            conn = sqlite3.connect(path)
            free = conn.execute("PRAGMA freelist_count").fetchone()[0]
            psize = conn.execute("PRAGMA page_size").fetchone()[0]
            conn.close()
            return (free, psize)
        except (sqlite3.Error, TypeError):
            return (0, 4096)

    def _flash_size_change(self, before_str: str, after_str: str) -> None:
        """Animate the size label to show the before→after transition."""
        self._size_label.setText(f"{before_str}  →  {after_str}")
        self._size_label.setStyleSheet(
            "color: #2ecc71; font-weight: 700; font-size: 13px;")
        QTimer.singleShot(4000, self._reset_size_label_style)

    def _reset_size_label_style(self) -> None:
        self._size_label.setStyleSheet("")

    def _check_integrity(self) -> None:
        if not self._journal or not self._journal.path:
            return
        result = check_journal_integrity(self._journal.path)
        if result.is_healthy:
            msg = self.tr("Le journal est sain.")
            if result.recovered_events > 0:
                msg += "\n" + self.tr("{count} evenement(s) recupere(s).").format(count=result.recovered_events)
            QMessageBox.information(self, self.tr("Integrite"), msg)
        else:
            human_msg = format_integrity_message(result.issues, result.recovered_events)
            QMessageBox.warning(self, self.tr("Problemes detectes"), human_msg or self.tr("Anomalies detectees."))

    def _export_journal(self) -> None:
        if not self._journal or not self._journal.path:
            return
        dest, _ = QFileDialog.getSaveFileName(
            self, self.tr("Exporter le journal"), "",
            self.tr("SQLite (*.sqlite);;Tous les fichiers (*)"))
        if not dest:
            return
        src_conn = None
        dst_conn = None
        try:
            src_conn = self._journal.create_read_connection()
            dst_conn = sqlite3.connect(dest)
            src_conn.backup(dst_conn)
            QMessageBox.information(
                self, self.tr("Export"),
                self.tr("Journal exporte vers {name}.").format(
                    name=os.path.basename(dest)))
        except (sqlite3.Error, OSError) as e:
            flog(f"_export_journal: backup failed: {e}", "ERROR")
            QMessageBox.warning(
                self, self.tr("Erreur d'export"),
                self.tr("Une erreur est survenue lors de l'export. "
                        "Consultez le journal de logs."))
        finally:
            if dst_conn:
                try:
                    dst_conn.close()
                except Exception:
                    pass
            if src_conn:
                try:
                    src_conn.close()
                except Exception:
                    pass

    def _analyze_journal(self) -> None:
        """Run BL-OPT-08 diagnostic and display the report."""
        if not self._journal or not self._journal.is_open:
            return
        from .core.journal_diagnostics import (
            run_journal_diagnostics, format_diagnostic_report,
        )
        conn = None
        try:
            conn = self._journal.create_read_connection()
            report = run_journal_diagnostics(conn)
            text = format_diagnostic_report(report)
            flog(
                f"journal_diagnostics event=displayed "
                f"total_events={report.total_events} "
                f"elapsed_ms={report.elapsed_ms}",
                "INFO",
            )
            from qgis.PyQt.QtWidgets import QTextEdit
            dlg = QDialog(self)
            dlg.setWindowTitle(self.tr("Diagnostic du journal"))
            dlg.setMinimumSize(520, 400)
            lay = QVBoxLayout(dlg)
            te = QTextEdit(dlg)
            te.setReadOnly(True)
            te.setFontFamily("Consolas, monospace")
            te.setPlainText(text)
            lay.addWidget(te)
            btn = QPushButton(self.tr("Fermer"), dlg)
            btn.clicked.connect(dlg.accept)
            lay.addWidget(btn, 0, QtCompat.ALIGN_RIGHT)
            dlg.exec()
        except Exception as e:
            flog(f"journal_diagnostics error: {e}", "ERROR")
            QMessageBox.warning(
                self, self.tr("Erreur d'analyse"),
                self.tr("Une erreur est survenue lors de l'analyse. "
                        "Consultez le journal de logs."))
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def _open_journal_folder(self) -> None:
        if not self._journal or not self._journal.path:
            return
        folder = os.path.realpath(os.path.dirname(self._journal.path))
        if not os.path.isdir(folder):
            return
        url = QUrl.fromLocalFile(folder)
        if not QDesktopServices.openUrl(url):
            flog(f"journal_maintenance: cannot open folder {folder}", "WARNING")
