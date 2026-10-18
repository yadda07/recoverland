"""Journal maintenance panel for RecoverLand (UX-D01, UX-D02).

Provides journal health overview, retention configuration,
purge actions, vacuum, integrity check and export.
"""
from typing import Optional
import os
import sqlite3

from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QSpinBox, QGroupBox, QMessageBox, QFileDialog,
    QProgressBar, QCheckBox,
)
from qgis.PyQt.QtCore import QTimer, QUrl, QElapsedTimer, QObject, pyqtSignal
from qgis.PyQt.QtGui import QDesktopServices
from qgis.core import QgsSettings

from .compat import QtCompat
from .core import (
    flog,
    get_journal_size_bytes,
    get_journal_stats, count_purgeable_events, count_logical_garbage_events,
    purge_old_events_with_options, vacuum_async,
    RetentionPolicy, PurgeOptions, check_journal_integrity,
)
from .core.health_monitor import (
    evaluate_journal_health, HealthLevel, _format_size,
    format_integrity_message,
)
from .core.time_format import compute_history_span
from .core.long_paths import plain, when_needed
from .widgets.icons import theme_icon
from .widgets.theme import (
    HAUTEUR_COMPACTE, ICONE_XS, RAYON_CONTROLE,
    build_dialog_stylesheet, resolve_theme, rgba_css,
)


class _VacuumBridge(QObject):
    """Carries the end of an async VACUUM back to the GUI thread."""

    finished = pyqtSignal(bool)


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

    def _build_info_group(self, t) -> QGroupBox:
        info_group = QGroupBox(self.tr("Journal"))
        info_layout = QVBoxLayout()
        info_layout.setSpacing(6)
        self._path_label = QLabel("")
        self._path_label.setWordWrap(True)
        self._path_label.setTextInteractionFlags(QtCompat.TEXT_SELECTABLE_BY_MOUSE)
        self._path_label.setStyleSheet(
            f"font-family: '{t.police_mono}'; font-size: 11px; color: {t.secondaire.name()};"
        )
        pill_css = (
            f"font-family: '{t.police_titre}'; font-size: 11px; font-weight: 700; "
            f"color: {t.texte.name()}; background-color: {t.surface_douce.name()}; "
            f"border: 1px solid {t.ligne.name()}; border-radius: 10px; padding: 2px 8px;"
        )
        self._size_label = QLabel("")
        self._events_label = QLabel("")
        self._span_label = QLabel("")
        self._schema_label = QLabel("")
        for pill in (self._size_label, self._events_label, self._span_label, self._schema_label):
            pill.setStyleSheet(pill_css)
        self._health_label = QLabel("")
        self._health_label.setWordWrap(True)
        info_layout.addWidget(self._path_label)
        row1 = QHBoxLayout()
        row1.setSpacing(8)
        row1.addWidget(self._size_label)
        row1.addWidget(self._events_label)
        row1.addWidget(self._span_label)
        row1.addWidget(self._schema_label)
        row1.addStretch()
        info_layout.addLayout(row1)
        info_layout.addWidget(self._health_label)
        info_group.setLayout(info_layout)
        return info_group

    def _build_retention_group(self, t) -> QGroupBox:
        retention_group = QGroupBox(self.tr("Retention"))
        retention_layout = QVBoxLayout()
        retention_layout.setSpacing(8)
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
        self._save_retention_btn.setIcon(theme_icon("save", color=t.texte, size=ICONE_XS))
        self._save_retention_btn.setMinimumHeight(HAUTEUR_COMPACTE)
        self._save_retention_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._save_retention_btn.clicked.connect(self._save_retention)
        save_row.addWidget(self._auto_purge_check)
        save_row.addStretch()
        save_row.addWidget(self._save_retention_btn)
        retention_layout.addLayout(save_row)
        retention_group.setLayout(retention_layout)
        return retention_group

    def _build_tools_row(self, t) -> QHBoxLayout:
        tools_row = QHBoxLayout()
        tools_row.setSpacing(6)
        specs = (
            (self.tr("Integrite"), "shield-check", t.acheve,
             self.tr("Lancer une verification d'integrite du journal"), self._check_integrity),
            (self.tr("Exporter"), "download", t.texte,
             self.tr("Copier le fichier journal vers un emplacement choisi"), self._export_journal),
            (self.tr("Analyser"), "search", t.texte,
             self.tr("Mesurer la distribution, les doublons et le potentiel d'optimisation"), self._analyze_journal),
            (self.tr("Dossier"), "folder-open", t.texte,
             self.tr("Ouvrir le dossier contenant le journal"), self._open_journal_folder),
        )
        for label, ic_name, col, tip, slot in specs:
            btn = QPushButton(label)
            btn.setIcon(theme_icon(ic_name, color=col, size=ICONE_XS))
            btn.setMinimumHeight(HAUTEUR_COMPACTE)
            btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
            btn.setToolTip(tip)
            btn.clicked.connect(slot)
            tools_row.addWidget(btn)
        return tools_row

    def _build_actions_group(self, t) -> QGroupBox:
        actions_group = QGroupBox(self.tr("Actions"))
        actions_layout = QVBoxLayout()
        actions_layout.setSpacing(8)

        purge_row = QHBoxLayout()
        purge_row.setSpacing(8)
        self._purge_btn = QPushButton(self.tr("Purger"))
        self._purge_btn.setIcon(theme_icon("delete", color=t.erreur, size=ICONE_XS))
        self._purge_btn.setMinimumHeight(HAUTEUR_COMPACTE)
        self._purge_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._purge_btn.setToolTip(self.tr("Supprimer les evenements hors politique de retention"))
        self._purge_btn.clicked.connect(self._purge_events)
        self._purge_info = QLabel("")
        self._purge_info.setStyleSheet(f"color: {t.secondaire.name()}; font-size: 11px;")
        purge_row.addWidget(self._purge_btn)
        purge_row.addWidget(self._purge_info, 1)
        actions_layout.addLayout(purge_row)

        vacuum_row = QHBoxLayout()
        vacuum_row.setSpacing(8)
        self._vacuum_btn = QPushButton(self.tr("Compacter (VACUUM)"))
        self._vacuum_btn.setIcon(theme_icon("refresh", color=t.accent, size=ICONE_XS))
        self._vacuum_btn.setMinimumHeight(HAUTEUR_COMPACTE)
        self._vacuum_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._vacuum_btn.setToolTip(self.tr("Recuperer l'espace disque apres une purge"))
        self._vacuum_btn.clicked.connect(self._vacuum_journal)
        self._vacuum_status = QLabel("")
        self._vacuum_status.setWordWrap(True)
        vacuum_row.addWidget(self._vacuum_btn)
        vacuum_row.addWidget(self._vacuum_status, 1)
        actions_layout.addLayout(vacuum_row)
        actions_layout.addLayout(self._build_tools_row(t))
        actions_group.setLayout(actions_layout)
        return actions_group

    def _setup_ui(self) -> None:
        t = resolve_theme(self)
        self.setStyleSheet(build_dialog_stylesheet(t))
        layout = QVBoxLayout()
        layout.setSpacing(10)
        layout.setContentsMargins(16, 16, 16, 16)

        self._progress = QProgressBar()
        self._progress.setVisible(False)
        self._progress.setMaximumHeight(6)

        close_btn = QPushButton(self.tr("Fermer"))
        close_btn.setIcon(theme_icon("close", color=t.texte, size=ICONE_XS))
        close_btn.setMinimumHeight(HAUTEUR_COMPACTE)
        close_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        close_btn.clicked.connect(self.accept)

        layout.addWidget(self._build_info_group(t))
        layout.addWidget(self._build_retention_group(t))
        layout.addWidget(self._build_actions_group(t))
        layout.addWidget(self._progress)
        layout.addStretch()
        layout.addWidget(close_btn, 0, QtCompat.ALIGN_RIGHT)
        self.setLayout(layout)

    def _refresh_stats(self) -> None:
        """Refresh all displayed journal statistics."""
        path = self._journal.path if self._journal and self._journal.is_open else ""
        # Shown without the extended-length prefix of a journal past
        # MAX_PATH (core/long_paths): the user must be able to paste it.
        self._path_label.setText(plain(path) or self.tr("Aucun journal actif"))
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

            t = resolve_theme(self)
            health = evaluate_journal_health(size_bytes, total, oldest, newest)
            if health.level == HealthLevel.HEALTHY:
                self._health_label.setText(self.tr("Sain"))
                h_col = t.acheve
            elif health.level == HealthLevel.INFO:
                self._health_label.setText(self.tr("Information : ") + health.message)
                h_col = t.accent
            elif health.level == HealthLevel.WARNING:
                self._health_label.setText(self.tr("Attention : ") + health.message)
                h_col = t.ambre
            else:
                self._health_label.setText(self.tr("Critique : ") + health.message)
                h_col = t.erreur
            self._health_label.setStyleSheet(
                f"color: {h_col.name()}; background: {rgba_css(h_col, 22)}; "
                f"border: 1px solid {rgba_css(h_col, 85)}; border-radius: {RAYON_CONTROLE}px; "
                f"padding: 3px 8px; font-weight: 600;"
            )

            policy = self._load_retention_policy()
            old_purgeable = count_purgeable_events(conn, policy)
            garbage = count_logical_garbage_events(conn)
            total_purgeable = old_purgeable + garbage.total
            if total_purgeable > 0:
                parts = []
                if old_purgeable:
                    parts.append(self.tr("{count} ancien(s)").format(count=old_purgeable))
                if garbage.invalidated_events:
                    parts.append(self.tr("{count} trace(s) invalidee(s)").format(count=garbage.invalidated_events))
                if garbage.insert_delete_pairs:
                    parts.append(self.tr("{count} paire(s) INSERT/DELETE").format(count=garbage.insert_delete_pairs))
                if garbage.orphan_traces:
                    parts.append(self.tr("{count} orphelin(s)").format(count=garbage.orphan_traces))
                text = self.tr("{count} evenement(s) purgeable(s)").format(count=total_purgeable)
                if parts:
                    text += " (" + ", ".join(parts) + ")"
                self._purge_info.setText(text)
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
                except Exception as exc:  # noqa: BLE001
                    flog(f"_refresh_stats: stats read connection close "
                         f"failed ({exc!r})", "DEBUG")

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
        t = resolve_theme(self)
        btn = self._save_retention_btn
        btn.setText(self.tr("Enregistre"))
        btn.setIcon(theme_icon("check-circle", color=t.acheve, size=ICONE_XS))
        btn.setStyleSheet(f"color: {t.acheve.name()}; font-weight: 600;")
        btn.setEnabled(False)
        QTimer.singleShot(2000, self._reset_save_btn)

    def _reset_save_btn(self) -> None:
        t = resolve_theme(self)
        btn = self._save_retention_btn
        btn.setText(self.tr("Enregistrer"))
        btn.setIcon(theme_icon("save", color=t.texte, size=ICONE_XS))
        btn.setStyleSheet("")
        btn.setEnabled(True)

    def _purge_events(self) -> None:
        if not self._journal or not self._journal.is_open:
            return
        policy = self._load_retention_policy()
        conn = None
        impact = None
        try:
            conn = self._journal.create_read_connection()
            old_purgeable = count_purgeable_events(conn, policy)
            garbage = count_logical_garbage_events(conn)
            total_purgeable = old_purgeable + garbage.total
            # What each purge would take away from the rewind, read BEFORE
            # the user chooses: the dialog says it next to each box.
            from .core.retention import logical_purge_impact
            impact = logical_purge_impact(conn)
        except Exception:
            old_purgeable = 0
            garbage = None
            total_purgeable = 0
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"_purge_events: purgeable-count read connection "
                         f"close failed ({exc!r})", "DEBUG")
        if total_purgeable == 0:
            QMessageBox.information(self, self.tr("Purge"), self.tr("Aucun evenement a purger."))
            return
        from .core.retention import retention_cutoff
        options = self._ask_purge_options(
            old_purgeable, garbage, impact, retention_cutoff(policy))
        if options is None:
            return
        conn = None
        try:
            conn = self._journal.get_connection()
            result = purge_old_events_with_options(conn, policy, options)
            msg = self.tr("{count} evenement(s) supprime(s).").format(count=result.deleted_count)
            if result.deleted_count > 0:
                parts = []
                if result.retention_deleted:
                    parts.append(self.tr("{count} ancien(s)").format(count=result.retention_deleted))
                if result.excess_deleted:
                    parts.append(self.tr("{count} exces").format(count=result.excess_deleted))
                if result.invalidated_deleted:
                    parts.append(self.tr("{count} invalide(s)").format(count=result.invalidated_deleted))
                if result.pair_deleted:
                    parts.append(self.tr("{count} paire(s)").format(count=result.pair_deleted))
                if result.orphan_trace_deleted:
                    parts.append(self.tr("{count} orphelin(s)").format(count=result.orphan_trace_deleted))
                if parts:
                    msg += "\n" + self.tr("Details : ") + ", ".join(parts)
            QMessageBox.information(self, self.tr("Purge terminee"), msg)
        except Exception as e:
            flog(f"purge error: {e}", "ERROR")
            QMessageBox.warning(
                self, self.tr("Erreur de purge"),
                self.tr("Une erreur est survenue lors de la purge. "
                        "Consultez le journal de logs."))
        self._refresh_stats()

    def _ask_purge_options(self, old_purgeable: int, garbage, impact=None,
                           retention_before: str = "") -> Optional[PurgeOptions]:
        """Show a dialog to choose purge scope. Returns None if cancelled.

        Every purge takes dates away from the rewind, and each box says
        which ones (*impact*: ``retention.logical_purge_impact``;
        *retention_before*: the date the retention purge deletes before).
        The logical purges are UNCHECKED by default: they delete rows the
        rewind still reads, and a default click used to make dates of the
        layer unreachable without the user being told. The retention purge
        stays checked: it applies the retention period the user set.
        """
        from qgis.PyQt.QtWidgets import (
            QDialog, QVBoxLayout, QCheckBox, QDialogButtonBox, QLabel,
        )
        from .core.time_format import format_full_timestamp
        t = resolve_theme(self)
        dlg = QDialog(self)
        dlg.setStyleSheet(build_dialog_stylesheet(t))
        dlg.setWindowTitle(self.tr("Options de purge"))
        dlg.setMinimumWidth(380)
        lay = QVBoxLayout(dlg)
        intro = QLabel(self.tr(
            "Chaque purge retire des dates a l'historique de rewind. Les "
            "purges decochees conservent ces dates ; ne cochez que ce que "
            "vous acceptez de ne plus pouvoir rembobiner."))
        intro.setWordWrap(True)
        lay.addWidget(intro)
        checks = []

        def _cost(text):
            label = QLabel(text)
            label.setWordWrap(True)
            label.setStyleSheet(f"color: {t.ambre.name()}; margin-left: 22px;")
            lay.addWidget(label)

        def _interval_cost(intervals):
            if not intervals:
                return ""
            return self.tr(
                "Les rewinds tombant dans {count} intervalle(s) compris "
                "entre le {start} et le {end}, sur {layers} couche(s), "
                "seront refuses ; en dehors de ces intervalles, ils restent "
                "possibles.").format(
                    start=format_full_timestamp(min(i[1] for i in intervals)),
                    end=format_full_timestamp(max(i[2] for i in intervals)),
                    count=len(intervals),
                    layers=len({i[0] for i in intervals}))

        if old_purgeable:
            chk = QCheckBox(self.tr("Evenements anciens ({count})").format(count=old_purgeable))
            chk.setChecked(True)
            lay.addWidget(chk)
            checks.append(("retention", chk))
            if retention_before:
                _cost(self.tr(
                    "Les rewinds a une date anterieure au {date} seront "
                    "refuses.").format(date=format_full_timestamp(retention_before)))
        if garbage and garbage.invalidated_events:
            chk = QCheckBox(self.tr("Traces invalidées ({count})").format(count=garbage.invalidated_events))
            chk.setChecked(False)
            lay.addWidget(chk)
            checks.append(("invalidated", chk))
            text = _interval_cost(impact.invalidated if impact else [])
            if text:
                _cost(text)
        if garbage and garbage.insert_delete_pairs:
            chk = QCheckBox(self.tr("Paires INSERT/DELETE ({count})").format(count=garbage.insert_delete_pairs))
            chk.setChecked(False)
            lay.addWidget(chk)
            checks.append(("insert_delete_pairs", chk))
            text = _interval_cost(impact.insert_delete_pairs if impact else [])
            if text:
                _cost(text)
        if garbage and garbage.orphan_traces:
            chk = QCheckBox(self.tr("Traces orphelines ({count})").format(count=garbage.orphan_traces))
            chk.setChecked(False)
            lay.addWidget(chk)
            checks.append(("orphan_traces", chk))
            orphans = impact.orphan_traces if impact else []
            if orphans:
                _cost(self.tr(
                    "Sur {layers} couche(s), les rewinds a une date "
                    "anterieure ou egale a celle de leur trace orpheline (au "
                    "plus tard le {date}) seront refuses.").format(
                        date=format_full_timestamp(max(o[1] for o in orphans)),
                        layers=len({o[0] for o in orphans})))
        if not checks:
            return None
        bbox = QDialogButtonBox(QtCompat.BUTTON_OK | QtCompat.BUTTON_CANCEL, dlg)
        bbox.accepted.connect(dlg.accept)
        bbox.rejected.connect(dlg.reject)
        lay.addWidget(bbox)
        if dlg.exec() != QtCompat.DIALOG_ACCEPTED:
            return None
        kw = {name: chk.isChecked() for name, chk in checks}
        return PurgeOptions(**kw)

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
        t = resolve_theme(self)
        if free_pages == 0:
            garbage = self._count_logical_garbage()
            if garbage and garbage.total > 0:
                self._vacuum_status.setText(
                    self.tr("{count} donnees mortes ; purger d'abord").format(count=garbage.total))
                self._vacuum_status.setStyleSheet(f"color: {t.ambre.name()}; font-weight: 600;")
                QMessageBox.information(
                    self, self.tr("Compactage inutile"),
                    self.tr(
                        "Le journal n'a pas de pages libres, mais il contient\n"
                        "{count} evenement(s) logiquement mort(s) "
                        "(traces invalidées, paires INSERT/DELETE, orphelins).\n\n"
                        "Utilisez d'abord 'Purger' pour les supprimer, puis compactez."
                    ).format(count=garbage.total))
                flog(f"vacuum: skipped, freelist_count=0 logical_garbage={garbage.total}", "INFO")
                return
            self._vacuum_status.setText(self.tr("Deja compacte"))
            self._vacuum_status.setStyleSheet(f"color: {t.accent.name()}; font-weight: 600;")
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
        self._vacuum_status.setStyleSheet(f"color: {t.ambre.name()}; font-weight: 600;")
        self._progress.setRange(0, 0)
        self._progress.setVisible(True)
        flog(f"vacuum: user initiated, size_before={size_before}", "INFO")

        self._vacuum_bridge = _VacuumBridge(self)
        self._vacuum_bridge.finished.connect(self._on_vacuum_finished)
        vacuum_async(path, callback=self._vacuum_bridge.finished.emit)

    def _on_vacuum_finished(self, success: bool) -> None:
        t = resolve_theme(self)
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
            self._vacuum_status.setStyleSheet(f"color: {t.acheve.name()}; font-weight: 600;")

            flog(f"vacuum: done elapsed_ms={elapsed_ms} "
                 f"size_before={self._vacuum_size_before} "
                 f"size_after={size_after} saved={saved}",
                 "INFO" if saved > 0 else "WARNING")
            if saved <= 0:
                flog("vacuum: the journal did not shrink; another process "
                     "may keep it mapped in memory or hold a read "
                     "transaction on its WAL", "WARNING")

            QMessageBox.information(
                self, self.tr("Compactage termine"), result_msg)
            self._refresh_stats()
            self._flash_size_change(before_str, after_str)
        else:
            self._vacuum_status.setText(self.tr("Echec du compactage"))
            self._vacuum_status.setStyleSheet(f"color: {t.erreur.name()}; font-weight: 600;")
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
        conn = None
        try:
            conn = sqlite3.connect(path)
            free = conn.execute("PRAGMA freelist_count").fetchone()[0]
            psize = conn.execute("PRAGMA page_size").fetchone()[0]
            return (free, psize)
        except (sqlite3.Error, TypeError) as exc:
            flog(f"vacuum: free pages of {path} unreadable ({exc})", "WARNING")
            return (0, 4096)
        finally:
            if conn is not None:
                conn.close()

    def _count_logical_garbage(self):
        """Return LogicalGarbageCount for the active journal, or None on error."""
        if not self._journal or not self._journal.is_open:
            return None
        conn = None
        try:
            conn = self._journal.create_read_connection()
            return count_logical_garbage_events(conn)
        except Exception as e:
            flog(f"JournalMaintenanceDialog: logical garbage count error: {e}", "WARNING")
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"_count_logical_garbage: read connection close "
                         f"failed ({exc!r})", "DEBUG")

    def _flash_size_change(self, before_str: str, after_str: str) -> None:
        """Animate the size label to show the before->after transition."""
        t = resolve_theme(self)
        self._size_label.setText(f"{before_str}  ->  {after_str}")
        self._size_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; color: {t.acheve.name()}; "
            f"background-color: {t.acheve_doux.name()}; border: 1px solid {t.acheve.name()}; "
            f"border-radius: 10px; padding: 2px 8px; font-weight: 700; font-size: 12px;"
        )
        QTimer.singleShot(4000, self._reset_size_label_style)

    def _reset_size_label_style(self) -> None:
        t = resolve_theme(self)
        self._size_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 11px; font-weight: 700; "
            f"color: {t.texte.name()}; background-color: {t.surface_douce.name()}; "
            f"border: 1px solid {t.ligne.name()}; border-radius: 10px; padding: 2px 8px;"
        )

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
                except Exception as exc:  # noqa: BLE001
                    # WARNING: the success dialog has already been shown,
                    # so an unflushed destination means the user keeps an
                    # export he believes is complete.
                    flog(f"_export_journal: destination connection close "
                         f"failed, exported file may be incomplete "
                         f"({exc!r})", "WARNING")
            if src_conn:
                try:
                    src_conn.close()
                except Exception as exc:  # noqa: BLE001
                    flog(f"_export_journal: source read connection close "
                         f"failed ({exc!r})", "DEBUG")

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
            t = resolve_theme(self)
            dlg = QDialog(self)
            dlg.setStyleSheet(build_dialog_stylesheet(t))
            dlg.setWindowTitle(self.tr("Diagnostic du journal"))
            dlg.setMinimumSize(540, 420)
            lay = QVBoxLayout(dlg)
            te = QTextEdit(dlg)
            te.setReadOnly(True)
            te.setStyleSheet(
                f"QTextEdit {{ background-color: {t.surface_douce.name()}; "
                f"color: {t.texte.name()}; border: 1px solid {t.ligne.name()}; "
                f"border-radius: {RAYON_CONTROLE}px; padding: 8px; "
                f"font-family: '{t.police_mono}'; font-size: 12px; }}"
            )
            te.setPlainText(text)
            lay.addWidget(te)
            btn = QPushButton(self.tr("Fermer"), dlg)
            btn.setIcon(theme_icon("close", color=t.texte, size=ICONE_XS))
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
                except Exception as exc:  # noqa: BLE001
                    flog(f"_analyze_journal: diagnostics read connection "
                         f"close failed ({exc!r})", "DEBUG")

    def _open_journal_folder(self) -> None:
        if not self._journal or not self._journal.path:
            return
        # A journal past MAX_PATH is held in its extended-length spelling
        # (core/long_paths): right for a Win32 file call, not for an
        # address. QUrl made it a share on a host named "?", and the click
        # opened nothing. The folder is checked in the spelling that
        # reaches it, opened and named in the plain one.
        folder = os.path.realpath(when_needed(os.path.dirname(self._journal.path)))
        shown = plain(folder)
        if not os.path.isdir(folder):
            why = "folder not found"
        elif QDesktopServices.openUrl(QUrl.fromLocalFile(shown)):
            return
        else:
            why = "the desktop did not open it"
        flog(f"journal_maintenance: cannot open folder {shown} ({why})", "WARNING")
        QMessageBox.warning(
            self, self.tr("Dossier du journal"),
            self.tr("Le dossier du journal n'a pas pu etre ouvert. "
                    "Il se trouve ici :\n{folder}").format(folder=shown))
