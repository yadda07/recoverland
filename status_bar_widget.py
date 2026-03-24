"""QGIS status bar indicator for RecoverLand (UX-G01).

Shows a persistent icon in the QGIS status bar reflecting
tracking state and journal health. Click opens the dialog.
"""
from qgis.PyQt.QtWidgets import QLabel, QWidget, QHBoxLayout
from qgis.PyQt.QtCore import Qt, pyqtSignal
from qgis.PyQt.QtGui import QColor

from .compat import QtCompat
from .core.health_monitor import HealthLevel


_STATUS_COLORS = {
    HealthLevel.HEALTHY: QColor(46, 204, 113),
    HealthLevel.INFO: QColor(66, 133, 244),
    HealthLevel.WARNING: QColor(255, 152, 0),
    HealthLevel.CRITICAL: QColor(219, 68, 55),
    "disabled": QColor(149, 165, 166),
    "no_project": QColor(189, 195, 199),
}


class StatusBarIndicator(QWidget):
    """Compact status indicator for the QGIS status bar."""

    clicked = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._dot = QLabel(self)
        self._dot.setFixedSize(10, 10)
        self._text = QLabel("RecoverLand", self)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 0)
        layout.setSpacing(4)
        layout.addWidget(self._dot)
        layout.addWidget(self._text)
        self.setFixedHeight(20)
        self._apply_state("no_project", "RecoverLand : aucun projet")

    def update_state(
        self,
        tracking_active: bool,
        health_level: str,
        event_count: int,
        size_str: str,
    ) -> None:
        """Update the indicator from current plugin state."""
        if not tracking_active:
            self._apply_state(
                "disabled",
                f"RecoverLand : enregistrement desactive ({size_str})")
            return
        tooltip = (
            f"RecoverLand : actif, "
            f"{event_count} evenement(s), {size_str}"
        )
        self._apply_state(health_level, tooltip)

    def set_no_project(self) -> None:
        self._apply_state("no_project", "RecoverLand : aucun projet ouvert")

    def mousePressEvent(self, _event) -> None:
        self.clicked.emit()

    def _apply_state(self, level: str, tooltip: str) -> None:
        color = _STATUS_COLORS.get(level, _STATUS_COLORS["no_project"])
        self._dot.setStyleSheet(
            f"background-color: {color.name()}; "
            f"border-radius: 5px; "
            f"min-width: 10px; min-height: 10px; "
            f"max-width: 10px; max-height: 10px;"
        )
        self.setToolTip(tooltip)
