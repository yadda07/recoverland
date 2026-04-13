"""Preflight / dry-run confirmation dialog for restore operations.

Qt5/Qt6 compatible via qgis.PyQt. Shows plan summary, warnings,
blocking reasons, and asks for user confirmation before execution.
"""
from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTextEdit, QFrame,
)

from ..compat import QtCompat


_VERDICT_STYLES = {
    "go": ("background-color: #27ae60; color: white; padding: 4px 12px; "
           "border-radius: 4px; font-weight: bold;"),
    "go_with_warnings": ("background-color: #f39c12; color: white; padding: 4px 12px; "
                         "border-radius: 4px; font-weight: bold;"),
    "blocked": ("background-color: #e74c3c; color: white; padding: 4px 12px; "
                "border-radius: 4px; font-weight: bold;"),
}

_VERDICT_LABELS_KEYS = ("go", "go_with_warnings", "blocked")


class RestorePreflightDialog(QDialog):
    """Modal dialog showing restore preflight results.

    Call exec() and check result: QDialog.Accepted = proceed, Rejected = cancel.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(self.tr("RecoverLand - Pre-vol de restauration"))
        self.setMinimumWidth(520)
        self.setMinimumHeight(380)

        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        self._verdict_label = QLabel()
        self._verdict_label.setAlignment(QtCompat.ALIGN_CENTER)
        layout.addWidget(self._verdict_label)

        sep = QFrame()
        _hline = getattr(getattr(QFrame, 'Shape', None), 'HLine', None) or getattr(QFrame, 'HLine')
        sep.setFrameShape(_hline)
        sep.setStyleSheet("color: palette(mid);")
        layout.addWidget(sep)

        self._summary_label = QLabel()
        self._summary_label.setWordWrap(True)
        self._summary_label.setStyleSheet("font-size: 13px;")
        layout.addWidget(self._summary_label)

        self._details = QTextEdit()
        self._details.setReadOnly(True)
        self._details.setStyleSheet(
            "QTextEdit { background: palette(base); border: 1px solid palette(mid); "
            "border-radius: 4px; padding: 6px; font-family: monospace; font-size: 12px; }"
        )
        self._details.setMinimumHeight(120)
        layout.addWidget(self._details, 1)

        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)

        self._cancel_btn = QPushButton(self.tr("Annuler"))
        self._cancel_btn.setFixedHeight(34)
        self._cancel_btn.clicked.connect(self.reject)

        self._proceed_btn = QPushButton(self.tr("Executer la restauration"))
        self._proceed_btn.setFixedHeight(34)
        self._proceed_btn.setStyleSheet(
            "QPushButton { background-color: #27ae60; color: white; "
            "border: none; border-radius: 6px; padding: 6px 20px; font-weight: bold; }"
            "QPushButton:disabled { background-color: #95a5a6; }"
        )
        self._proceed_btn.clicked.connect(self.accept)

        btn_layout.addStretch()
        btn_layout.addWidget(self._cancel_btn)
        btn_layout.addWidget(self._proceed_btn)
        layout.addLayout(btn_layout)

    def set_preflight_data(
        self, verdict: str, summary_text: str, detail_text: str,
        is_blocked: bool,
    ) -> None:
        """Populate the dialog with preflight results.

        Args:
            verdict: one of "go", "go_with_warnings", "blocked"
            summary_text: short plan summary (mode, counts, scope)
            detail_text: full formatted preflight report
            is_blocked: if True, the proceed button is disabled
        """
        _verdict_tr = {
            "go": self.tr("PRET"),
            "go_with_warnings": self.tr("PRET (avertissements)"),
            "blocked": self.tr("BLOQUE"),
        }
        label_text = _verdict_tr.get(verdict, verdict.upper())
        style = _VERDICT_STYLES.get(verdict, "")
        self._verdict_label.setText(label_text)
        self._verdict_label.setStyleSheet(style + " font-size: 16px;")

        self._summary_label.setText(summary_text)
        self._details.setPlainText(detail_text)

        self._proceed_btn.setEnabled(not is_blocked)
        if is_blocked:
            self._proceed_btn.setText(self.tr("Restauration impossible"))
            self._proceed_btn.setToolTip(
                self.tr("Le pre-vol a detecte des problemes bloquants")
            )
