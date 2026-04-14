"""Mode selector widget for RestoreMode (Event / Temporal).

Qt5/Qt6 compatible via qgis.PyQt. Emits modeChanged(str) when the
user switches between "event" and "temporal".
"""
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import (
    QWidget, QHBoxLayout, QPushButton, QLabel, QButtonGroup,
)
from ..compat import QtCompat


def _rgba(color, alpha: int) -> str:
    return f"rgba({color.red()}, {color.green()}, {color.blue()}, {alpha})"


class RestoreModeSelector(QWidget):
    """Two-button toggle selector for restore mode."""

    modeChanged = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self._label = QLabel(self.tr("Mode :"))
        self._label.setStyleSheet("font-weight: bold;")

        self._btn_event = QPushButton(self.tr("Action"))
        self._btn_event.setToolTip(
            self.tr("Annuler un ou plusieurs evenements selectionnes")
        )
        self._btn_event.setCursor(QtCompat.POINTING_HAND_CURSOR)

        self._btn_temporal = QPushButton(self.tr("Version"))
        self._btn_temporal.setToolTip(
            self.tr("Retour a un point dans le temps (reverse replay)")
        )
        self._btn_temporal.setCursor(QtCompat.POINTING_HAND_CURSOR)

        self._btn_event.setCheckable(True)
        self._btn_temporal.setCheckable(True)

        self._group = QButtonGroup(self)
        self._group.setExclusive(True)
        self._group.addButton(self._btn_event, 0)
        self._group.addButton(self._btn_temporal, 1)

        self._btn_event.clicked.connect(lambda: self._on_select("event"))
        self._btn_temporal.clicked.connect(lambda: self._on_select("temporal"))

        layout.addWidget(self._label)
        layout.addWidget(self._btn_temporal)
        layout.addWidget(self._btn_event)
        layout.addStretch()

        self._current = "temporal"
        self._apply_styles()

    def mode(self) -> str:
        return self._current

    def setMode(self, mode: str) -> None:
        if mode not in ("event", "temporal"):
            return
        self._current = mode
        self._apply_styles()

    def _on_select(self, mode: str) -> None:
        if mode == self._current:
            return
        self._current = mode
        self._apply_styles()
        self.modeChanged.emit(mode)

    def _apply_styles(self) -> None:
        pal = self.palette()
        hl = pal.highlight().color()
        mid = pal.mid().color()
        hl_text = pal.highlightedText().color()
        ev_active = self._current == "event"
        active_ss = (
            f"QPushButton {{ background-color: {_rgba(hl, 200)};"
            f" color: {hl_text.name()};"
            f" border: none; border-radius: 4px;"
            f" padding: 6px 16px; font-weight: bold; }}"
        )
        inactive_ss = (
            f"QPushButton {{ background-color: transparent;"
            f" color: palette(text);"
            f" border: 1px solid {_rgba(mid, 120)};"
            f" border-radius: 4px; padding: 6px 16px; }}"
        )
        self._btn_event.setStyleSheet(active_ss if ev_active else inactive_ss)
        self._btn_temporal.setStyleSheet(active_ss if not ev_active else inactive_ss)
        self._btn_event.setChecked(ev_active)
        self._btn_temporal.setChecked(not ev_active)
