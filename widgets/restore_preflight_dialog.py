# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Preflight / dry-run confirmation dialog for restore operations (style VTT).

Qt5/Qt6 compatible via qgis.PyQt. Shows plan summary, warnings,
blocking reasons, and asks for user confirmation before execution.
"""
from qgis.PyQt.QtWidgets import (
    QDialog, QFrame, QHBoxLayout, QLabel, QPushButton, QTextEdit, QVBoxLayout,
)

from ..compat import QtCompat
from .icons import couleur_etat, icone_etat, theme_icon, theme_pixmap
from .theme import (
    HAUTEUR_COMPACTE,
    ICONE_MD,
    ICONE_XS,
    RAYON_CONTROLE,
    RAYON_PANNEAU,
    build_dialog_stylesheet,
    resolve_theme,
    rgba_css,
)

_VERDICT_LABELS_KEYS = ("go", "go_with_warnings", "blocked")


class RestorePreflightDialog(QDialog):
    """Modal dialog showing restore preflight results (styled on VTT tokens).

    Call exec() and check result: QDialog.Accepted = proceed, Rejected = cancel.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("RecoverLand - Pre-vol de restauration"))
        self.setMinimumWidth(540)
        self.setMinimumHeight(400)
        self._verdict = "go"

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        self._verdict_banner = QFrame(self)
        self._verdict_banner.setObjectName("rlVerdictBanner")
        banner_lay = QHBoxLayout(self._verdict_banner)
        banner_lay.setContentsMargins(12, 8, 12, 8)
        banner_lay.setSpacing(10)
        self._verdict_icon = QLabel(self._verdict_banner)
        self._verdict_icon.setFixedSize(ICONE_MD, ICONE_MD)
        self._verdict_icon.setStyleSheet("background: transparent; border: none;")
        self._verdict_label = QLabel(self._verdict_banner)
        self._verdict_label.setAlignment(QtCompat.ALIGN_LEFT | QtCompat.ALIGN_VCENTER)
        banner_lay.addWidget(self._verdict_icon, 0, QtCompat.ALIGN_VCENTER)
        banner_lay.addWidget(self._verdict_label, 1)
        layout.addWidget(self._verdict_banner)

        self._summary_label = QLabel()
        self._summary_label.setWordWrap(True)
        layout.addWidget(self._summary_label)

        self._details = QTextEdit()
        self._details.setReadOnly(True)
        self._details.setMinimumHeight(140)
        layout.addWidget(self._details, 1)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)

        self._cancel_btn = QPushButton(self.tr("Annuler"))
        self._cancel_btn.setFixedHeight(HAUTEUR_COMPACTE + 2)
        self._cancel_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._cancel_btn.clicked.connect(self.reject)

        self._proceed_btn = QPushButton(self.tr("Executer la restauration"))
        self._proceed_btn.setFixedHeight(HAUTEUR_COMPACTE + 2)
        self._proceed_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._proceed_btn.clicked.connect(self.accept)

        btn_layout.addStretch()
        btn_layout.addWidget(self._cancel_btn)
        btn_layout.addWidget(self._proceed_btn)
        layout.addLayout(btn_layout)
        self._refresh_theme()

    def _refresh_theme(self) -> None:
        t = resolve_theme(self)
        self.setStyleSheet(build_dialog_stylesheet(t))
        sem = couleur_etat(self._verdict, t)
        pm = theme_pixmap(icone_etat(self._verdict), color=sem, size=ICONE_MD)
        if not pm.isNull():
            self._verdict_icon.setPixmap(pm)
        self._verdict_banner.setStyleSheet(
            f"QFrame#rlVerdictBanner {{"
            f"  background-color: {rgba_css(sem, 24)};"
            f"  border: 1px solid {rgba_css(sem, 100)};"
            f"  border-radius: {RAYON_PANNEAU}px;"
            f"}}"
        )
        self._verdict_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 15px; font-weight: 700; "
            f"color: {sem.name()}; background: transparent;"
        )
        self._summary_label.setStyleSheet(
            f"font-size: 13px; color: {t.texte.name()};"
        )
        self._details.setStyleSheet(
            f"QTextEdit {{ background-color: {t.surface_douce.name()}; "
            f"color: {t.texte.name()}; border: 1px solid {t.ligne.name()}; "
            f"border-radius: {RAYON_CONTROLE}px; padding: 8px; "
            f"font-family: '{t.police_mono}'; font-size: 12px; }}"
        )
        self._cancel_btn.setIcon(theme_icon("close", color=t.texte, size=ICONE_XS))
        is_enabled = self._proceed_btn.isEnabled()
        fg = t.texte_accent if is_enabled else t.tertiaire
        ic_name = "check-circle" if is_enabled else "stop"
        self._proceed_btn.setIcon(theme_icon(ic_name, color=fg, size=ICONE_XS))
        self._proceed_btn.setStyleSheet(
            f"QPushButton {{ background-color: {t.acheve.name()}; "
            f"color: {t.texte_accent.name()}; border: none; "
            f"border-radius: {RAYON_CONTROLE}px; padding: 6px 18px; font-weight: 700; }}"
            f"QPushButton:hover {{ background-color: {t.accent.name()}; }}"
            f"QPushButton:disabled {{ background-color: {t.surface_douce.name()}; "
            f"color: {t.tertiaire.name()}; border: 1px solid {t.ligne.name()}; }}"
        )

    def set_preflight_data(
        self, verdict: str, summary_text: str, detail_text: str,
        is_blocked: bool,
    ) -> None:
        """Populate the dialog with preflight results."""
        self._verdict = verdict
        _verdict_tr = {
            "go": self.tr("PRET"),
            "go_with_warnings": self.tr("PRET (avertissements)"),
            "blocked": self.tr("BLOQUE"),
        }
        label_text = _verdict_tr.get(verdict, verdict.upper())
        self._verdict_label.setText(label_text)
        self._summary_label.setText(summary_text)
        self._details.setPlainText(detail_text)

        self._proceed_btn.setEnabled(not is_blocked)
        if is_blocked:
            self._proceed_btn.setText(self.tr("Restauration impossible"))
            self._proceed_btn.setToolTip(
                self.tr("Le pre-vol a detecte des problemes bloquants")
            )
        self._refresh_theme()
