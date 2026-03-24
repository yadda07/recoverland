"""Journal maintenance panel for RecoverLand (UX-D01, UX-D02).

Provides journal health overview, retention configuration,
purge actions, vacuum, integrity check and export.
"""
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QSpinBox, QGroupBox, QFormLayout, QMessageBox, QFileDialog,
    QProgressBar, QCheckBox,
)
from qgis.PyQt.QtCore import QTimer
from qgis.core import QgsApplication, QgsSettings

from .compat import QtCompat, QgisCompat
from .core import (
    flog,
    get_journal_size_bytes, format_journal_size,
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
import shutil


class JournalMaintenanceDialog(QDialog):
    """Maintenance and configuration dialog for the audit journal."""

    def __init__(self, journal, parent=None):
        super().__init__(parent)
        self._journal = journal
        self.setWindowTitle("RecoverLand - Maintenance du journal")
        self.setMinimumWidth(520)
        self.setMinimumHeight(400)
        self.resize(560, 480)
        self._setup_ui()
        self._refresh_stats()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        info_group = QGroupBox("Informations du journal")
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
        info_layout.addRow("Chemin :", self._path_label)
        info_layout.addRow("Taille :", self._size_label)
        info_layout.addRow("Evenements :", self._events_label)
        info_layout.addRow("Historique :", self._span_label)
        info_layout.addRow("Schema :", self._schema_label)
        info_layout.addRow("Sante :", self._health_label)
        info_group.setLayout(info_layout)

        open_folder_btn = QPushButton("Ouvrir le dossier")
        open_folder_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileOpen.svg'))
        open_folder_btn.setToolTip("Ouvrir le dossier contenant le journal")
        open_folder_btn.clicked.connect(self._open_journal_folder)

        retention_group = QGroupBox("Politique de retention")
        retention_layout = QFormLayout()
        self._retention_days_spin = QSpinBox()
        self._retention_days_spin.setRange(7, 3650)
        self._retention_days_spin.setSuffix(" jours")
        self._retention_days_spin.setToolTip("Duree de conservation des evenements")
        self._max_events_spin = QSpinBox()
        self._max_events_spin.setRange(10000, 10_000_000)
        self._max_events_spin.setSingleStep(10000)
        self._max_events_spin.setToolTip("Nombre maximum d'evenements")
        self._auto_purge_check = QCheckBox("Purger automatiquement au demarrage")
        self._auto_purge_check.setToolTip(
            "Si active, les evenements hors politique sont supprimes a l'ouverture du journal")
        retention_layout.addRow("Conservation :", self._retention_days_spin)
        retention_layout.addRow("Maximum :", self._max_events_spin)
        retention_layout.addRow("", self._auto_purge_check)
        save_retention_btn = QPushButton("Enregistrer la politique")
        save_retention_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSave.svg'))
        save_retention_btn.clicked.connect(self._save_retention)
        retention_layout.addRow("", save_retention_btn)
        retention_group.setLayout(retention_layout)

        actions_group = QGroupBox("Actions")
        actions_layout = QVBoxLayout()
        actions_layout.setSpacing(8)

        purge_row = QHBoxLayout()
        self._purge_btn = QPushButton("Purger les anciens evenements")
        self._purge_btn.setIcon(QgsApplication.getThemeIcon('/mActionDeleteSelected.svg'))
        self._purge_btn.setToolTip("Supprimer les evenements hors politique de retention")
        self._purge_btn.clicked.connect(self._purge_events)
        self._purge_info = QLabel("")
        purge_row.addWidget(self._purge_btn)
        purge_row.addWidget(self._purge_info, 1)
        actions_layout.addLayout(purge_row)

        vacuum_btn = QPushButton("Compacter le journal (VACUUM)")
        vacuum_btn.setIcon(QgsApplication.getThemeIcon('/mActionRefresh.svg'))
        vacuum_btn.setToolTip("Recuperer l'espace disque apres une purge")
        vacuum_btn.clicked.connect(self._vacuum_journal)
        actions_layout.addWidget(vacuum_btn)

        integrity_btn = QPushButton("Verifier l'integrite")
        integrity_btn.setIcon(QgsApplication.getThemeIcon('/mActionCheckGeometry.svg'))
        integrity_btn.setToolTip("Lancer une verification d'integrite du journal")
        integrity_btn.clicked.connect(self._check_integrity)
        actions_layout.addWidget(integrity_btn)

        export_btn = QPushButton("Exporter le journal")
        export_btn.setIcon(QgsApplication.getThemeIcon('/mActionFileSaveAs.svg'))
        export_btn.setToolTip("Copier le fichier journal vers un emplacement choisi")
        export_btn.clicked.connect(self._export_journal)
        actions_layout.addWidget(export_btn)

        actions_group.setLayout(actions_layout)

        self._progress = QProgressBar()
        self._progress.setVisible(False)

        close_btn = QPushButton("Fermer")
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
        self._path_label.setText(path or "Aucun journal actif")
        if not path:
            self._size_label.setText("-")
            self._events_label.setText("-")
            self._span_label.setText("-")
            self._schema_label.setText("-")
            self._health_label.setText("Aucun journal")
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
                self._health_label.setText("Sain")
                self._health_label.setStyleSheet("color: #2ecc71; font-weight: 600;")
            elif health.level == HealthLevel.INFO:
                self._health_label.setText("Information : " + health.message)
                self._health_label.setStyleSheet("color: #4285f4; font-weight: 600;")
            elif health.level == HealthLevel.WARNING:
                self._health_label.setText("Attention : " + health.message)
                self._health_label.setStyleSheet("color: #ff9800; font-weight: 600;")
            else:
                self._health_label.setText("Critique : " + health.message)
                self._health_label.setStyleSheet("color: #db4437; font-weight: 600;")

            policy = self._load_retention_policy()
            purgeable = count_purgeable_events(conn, policy)
            if purgeable > 0:
                self._purge_info.setText(f"{purgeable} evenement(s) purgeable(s)")
            else:
                self._purge_info.setText("Rien a purger")
        except Exception as e:
            flog(f"JournalMaintenanceDialog: stats error: {e}", "WARNING")
            self._events_label.setText("Erreur")
            self._health_label.setText("Impossible de lire les statistiques")
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
            QMessageBox.information(self, "Purge", "Aucun evenement a purger.")
            return
        reply = QMessageBox.question(
            self, "Confirmer la purge",
            f"Supprimer {purgeable} evenement(s) anciens ?\n"
            "Cette action est irreversible.",
            QtCompat.MSG_YES | QtCompat.MSG_NO, QtCompat.MSG_NO)
        if reply != QtCompat.MSG_YES:
            return
        conn = None
        try:
            conn = self._journal.get_connection()
            result = purge_old_events(conn, policy)
            QMessageBox.information(
                self, "Purge terminee",
                f"{result.deleted_count} evenement(s) supprime(s).")
        except Exception as e:
            QMessageBox.warning(self, "Erreur de purge", str(e))
        self._refresh_stats()

    def _vacuum_journal(self) -> None:
        if not self._journal or not self._journal.path:
            return
        self._progress.setRange(0, 0)
        self._progress.setVisible(True)

        def on_done(success):
            self._progress.setVisible(False)
            if success:
                self._refresh_stats()
            else:
                QMessageBox.warning(self, "VACUUM", "Le compactage a echoue.")

        vacuum_async(self._journal.path, callback=on_done)

    def _check_integrity(self) -> None:
        if not self._journal or not self._journal.path:
            return
        result = check_journal_integrity(self._journal.path)
        if result.is_healthy:
            msg = "Le journal est sain."
            if result.recovered_events > 0:
                msg += f"\n{result.recovered_events} evenement(s) recupere(s)."
            QMessageBox.information(self, "Integrite", msg)
        else:
            human_msg = format_integrity_message(result.issues, result.recovered_events)
            QMessageBox.warning(self, "Problemes detectes", human_msg or "Anomalies detectees.")

    def _export_journal(self) -> None:
        if not self._journal or not self._journal.path:
            return
        source = self._journal.path
        dest, _ = QFileDialog.getSaveFileName(
            self, "Exporter le journal", "",
            "SQLite (*.sqlite);;Tous les fichiers (*)")
        if not dest:
            return
        try:
            shutil.copy2(source, dest)
            wal = source + "-wal"
            if os.path.exists(wal):
                shutil.copy2(wal, dest + "-wal")
            QMessageBox.information(
                self, "Export", f"Journal exporte vers {os.path.basename(dest)}.")
        except OSError as e:
            QMessageBox.warning(self, "Erreur d'export", str(e))

    def _open_journal_folder(self) -> None:
        if not self._journal or not self._journal.path:
            return
        folder = os.path.dirname(self._journal.path)
        if os.path.isdir(folder):
            os.startfile(folder)
