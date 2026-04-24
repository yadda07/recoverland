"""Journal maintenance panel for RecoverLand (UX-D01, UX-D02).

Provides journal health overview, retention configuration,
purge actions, vacuum, integrity check and export.
"""
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QSpinBox, QGroupBox, QFormLayout, QMessageBox, QFileDialog,
    QProgressBar, QCheckBox,
)
from qgis.PyQt.QtCore import QTimer, QUrl
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
        self.setWindowTitle(self.tr("RecoverLand - Maintenance du journal"))
        self.setMinimumWidth(520)
        self.setMinimumHeight(400)
        self.resize(560, 480)
        self._setup_ui()
        self._refresh_stats()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        info_group = QGroupBox(self.tr("Informations du journal"))
        info_layout = QFormLayout()
        self._path_label = QLabel("")
        self._path_label.setWordWrap(True)
        self._path_label.setTextInteractionFlags(QtCompat.TEXT_SELECTABLE_BY_MOUSE)
        self._size_label = QLabel("")
        self._events_label = QLabel("")
        self._span_label = QLabel("")
        self._schema_label = QLabel("")
        self._health_label = QLabel("")
        self._health_label.setWordWrap(True)
        info_layout.addRow(self.tr("Chemin :"), self._path_label)
        info_layout.addRow(self.tr("Taille :"), self._size_label)
        info_layout.addRow(self.tr("Evenements :"), self._events_label)
        info_layout.addRow(self.tr("Historique :"), self._span_label)
        info_layout.addRow(self.tr("Schema :"), self._schema_label)
        info_layout.addRow(self.tr("Sante :"), self._health_label)
        info_group.setLayout(info_layout)

        open_folder_btn = QPushButton(self.tr("Ouvrir le dossier"))
        open_folder_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileOpen.svg'))
        open_folder_btn.setToolTip(self.tr("Ouvrir le dossier contenant le journal"))
        open_folder_btn.clicked.connect(self._open_journal_folder)

        retention_group = QGroupBox(self.tr("Politique de retention"))
        retention_layout = QFormLayout()
        self._retention_days_spin = QSpinBox()
        self._retention_days_spin.setRange(7, 3650)
        self._retention_days_spin.setSuffix(self.tr(" jours"))
        self._retention_days_spin.setToolTip(self.tr("Duree de conservation des evenements"))
        self._max_events_spin = QSpinBox()
        self._max_events_spin.setRange(10000, 10_000_000)
        self._max_events_spin.setSingleStep(10000)
        self._max_events_spin.setToolTip(self.tr("Nombre maximum d'evenements"))
        self._auto_purge_check = QCheckBox(self.tr("Purger automatiquement au demarrage"))
        self._auto_purge_check.setToolTip(
            self.tr("Si active, les evenements hors politique sont supprimes a l'ouverture du journal"))
        retention_layout.addRow(self.tr("Conservation :"), self._retention_days_spin)
        retention_layout.addRow(self.tr("Maximum :"), self._max_events_spin)
        retention_layout.addRow("", self._auto_purge_check)
        save_retention_btn = QPushButton(self.tr("Enregistrer la politique"))
        save_retention_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSave.svg'))
        save_retention_btn.clicked.connect(self._save_retention)
        retention_layout.addRow("", save_retention_btn)
        retention_group.setLayout(retention_layout)

        actions_group = QGroupBox(self.tr("Actions"))
        actions_layout = QVBoxLayout()
        actions_layout.setSpacing(8)

        purge_row = QHBoxLayout()
        self._purge_btn = QPushButton(self.tr("Purger les anciens evenements"))
        self._purge_btn.setIcon(QgsApplication.getThemeIcon('/mActionDeleteSelected.svg'))
        self._purge_btn.setToolTip(self.tr("Supprimer les evenements hors politique de retention"))
        self._purge_btn.clicked.connect(self._purge_events)
        self._purge_info = QLabel("")
        purge_row.addWidget(self._purge_btn)
        purge_row.addWidget(self._purge_info, 1)
        actions_layout.addLayout(purge_row)

        vacuum_btn = QPushButton(self.tr("Compacter le journal (VACUUM)"))
        vacuum_btn.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        vacuum_btn.setToolTip(self.tr("Recuperer l'espace disque apres une purge"))
        vacuum_btn.clicked.connect(self._vacuum_journal)
        actions_layout.addWidget(vacuum_btn)

        integrity_btn = QPushButton(self.tr("Verifier l'integrite"))
        integrity_btn.setIcon(QgsApplication.getThemeIcon('/mActionCheckGeometry.svg'))
        integrity_btn.setToolTip(self.tr("Lancer une verification d'integrite du journal"))
        integrity_btn.clicked.connect(self._check_integrity)
        actions_layout.addWidget(integrity_btn)

        export_btn = QPushButton(self.tr("Exporter le journal"))
        export_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSaveAs.svg'))
        export_btn.setToolTip(self.tr("Copier le fichier journal vers un emplacement choisi"))
        export_btn.clicked.connect(self._export_journal)
        actions_layout.addWidget(export_btn)

        actions_group.setLayout(actions_layout)

        self._progress = QProgressBar()
        self._progress.setVisible(False)

        close_btn = QPushButton(self.tr("Fermer"))
        close_btn.clicked.connect(self.accept)

        layout.addWidget(info_group)
        layout.addWidget(open_folder_btn)
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
            self._size_label.setText("-")
            self._events_label.setText("-")
            self._span_label.setText("-")
            self._schema_label.setText("-")
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
            self._events_label.setText(f"{total:,}".replace(",", " "))
            span = compute_history_span(oldest, newest)
            self._span_label.setText(span if span else "-")

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
        flog(f"Retention policy saved: {self._retention_days_spin.value()}d, "
             f"{self._max_events_spin.value()} max")

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
        self._progress.setRange(0, 0)
        self._progress.setVisible(True)

        def on_done(success):
            QTimer.singleShot(0, lambda: self._on_vacuum_finished(success))

        vacuum_async(self._journal.path, callback=on_done)

    def _on_vacuum_finished(self, success: bool) -> None:
        self._progress.setVisible(False)
        if success:
            self._refresh_stats()
        else:
            QMessageBox.warning(self, self.tr("VACUUM"), self.tr("Le compactage a echoue."))

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

    def _open_journal_folder(self) -> None:
        if not self._journal or not self._journal.path:
            return
        folder = os.path.realpath(os.path.dirname(self._journal.path))
        if not os.path.isdir(folder):
            return
        url = QUrl.fromLocalFile(folder)
        if not QDesktopServices.openUrl(url):
            flog(f"journal_maintenance: cannot open folder {folder}", "WARNING")
