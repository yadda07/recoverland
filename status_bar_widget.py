"""QGIS status bar indicator for RecoverLand (UX-G01).

Shows a persistent icon in the QGIS status bar reflecting
tracking state and journal health.
Left-click toggles tracking; right-click opens the dialog.
"""
from qgis.PyQt.QtWidgets import (
    QLabel, QWidget, QHBoxLayout, QGraphicsOpacityEffect,
)
from qgis.PyQt.QtCore import pyqtSignal, QPropertyAnimation
from qgis.PyQt.QtGui import QColor

from .compat import QtCompat
from .core.health_monitor import HealthLevel


_STATUS_COLORS = {
    HealthLevel.HEALTHY: QColor(46, 204, 113),
    HealthLevel.INFO: QColor(66, 133, 244),
    HealthLevel.WARNING: QColor(255, 152, 0),
    HealthLevel.CRITICAL: QColor(219, 68, 55),
    "disabled": QColor(231, 76, 60),
    "no_project": QColor(189, 195, 199),
}


class StatusBarIndicator(QWidget):
    """Compact status indicator for the QGIS status bar."""

    toggle_requested = pyqtSignal()
    open_dialog_requested = pyqtSignal()

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
        self._current_level = "no_project"
        self._pulse_anim = None
        self._opacity_effect = QGraphicsOpacityEffect(self._dot)
        self._opacity_effect.setOpacity(1.0)
        self._dot.setGraphicsEffect(self._opacity_effect)
        self._apply_state("no_project", self.tr("RecoverLand : aucun projet"))

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
                self.tr("RecoverLand : enregistrement desactive ({size})").format(size=size_str))
            return
        tooltip = self.tr(
            "RecoverLand : actif, {count} evenement(s), {size}"
        ).format(count=event_count, size=size_str)
        self._apply_state(health_level, tooltip)

    def set_no_project(self) -> None:
        self._apply_state("no_project", self.tr("RecoverLand : aucun projet ouvert"))

    def pulse(self) -> None:
        """Subtle opacity pulse on the dot (event commit feedback)."""
        if self._pulse_anim is not None:
            if self._pulse_anim.state() == QtCompat.ANIM_STATE_RUNNING:
                return
        self._pulse_anim = QPropertyAnimation(
            self._opacity_effect, b"opacity",
        )
        self._pulse_anim.setDuration(600)
        self._pulse_anim.setKeyValueAt(0.0, 1.0)
        self._pulse_anim.setKeyValueAt(0.5, 0.3)
        self._pulse_anim.setKeyValueAt(1.0, 1.0)
        self._pulse_anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        self._pulse_anim.start()

    def mousePressEvent(self, event) -> None:
        if event.button() == QtCompat.LEFT_BUTTON:
            self.toggle_requested.emit()
        elif event.button() == QtCompat.RIGHT_BUTTON:
            self.open_dialog_requested.emit()

    def _apply_state(self, level: str, tooltip: str) -> None:
        self._current_level = level
        color = _STATUS_COLORS.get(level, _STATUS_COLORS["no_project"])
        self._dot.setStyleSheet(
            f"background-color: {color.name()}; "
            f"border-radius: 5px; "
            f"min-width: 10px; min-height: 10px; "
            f"max-width: 10px; max-height: 10px;"
        )
        hint = self.tr("Clic : activer/desactiver | Clic droit : ouvrir")
        full_tip = tooltip + "\n" + hint
        self._current_tooltip = full_tip
        self.setToolTip(full_tip)
