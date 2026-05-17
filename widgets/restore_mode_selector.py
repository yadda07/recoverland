"""Mode selector widget for RestoreMode (Event / Temporal / GeoGit).

Qt5/Qt6 compatible via qgis.PyQt. Emits modeChanged(str) when the
user switches between "event", "temporal" and "geogit".
"""
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import (
    QWidget, QHBoxLayout, QPushButton, QLabel, QButtonGroup,
)
from ..compat import QtCompat


def _rgba(color, alpha: int) -> str:
    return f"rgba({color.red()}, {color.green()}, {color.blue()}, {alpha})"


class RestoreModeSelector(QWidget):
    """Three-button toggle selector for restore mode."""

    VALID_MODES = ("event", "temporal", "geogit")

    modeChanged = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self._label = QLabel(self.tr("Mode :"))
        self._label.setStyleSheet("font-weight: bold;")

        self._btn_temporal = QPushButton(self.tr("Version"))
        self._btn_temporal.setToolTip(
            self.tr("Retour a un point dans le temps (reverse replay)")
        )
        self._btn_temporal.setCursor(QtCompat.POINTING_HAND_CURSOR)

        self._btn_event = QPushButton(self.tr("Action"))
        self._btn_event.setToolTip(
            self.tr("Annuler un ou plusieurs evenements selectionnes")
        )
        self._btn_event.setCursor(QtCompat.POINTING_HAND_CURSOR)

        self._btn_geogit = QPushButton(self.tr("GeoGit"))
        self._btn_geogit.setToolTip(
            self.tr("Visualiser l'historique spatial dans une zone")
        )
        self._btn_geogit.setCursor(QtCompat.POINTING_HAND_CURSOR)

        self._btn_event.setCheckable(True)
        self._btn_temporal.setCheckable(True)
        self._btn_geogit.setCheckable(True)

        self._group = QButtonGroup(self)
        self._group.setExclusive(True)
        self._group.addButton(self._btn_temporal, 0)
        self._group.addButton(self._btn_event, 1)
        self._group.addButton(self._btn_geogit, 2)

        self._btn_event.clicked.connect(lambda: self._on_select("event"))
        self._btn_temporal.clicked.connect(lambda: self._on_select("temporal"))
        self._btn_geogit.clicked.connect(lambda: self._on_select("geogit"))

        layout.addWidget(self._label)
        layout.addWidget(self._btn_temporal)
        layout.addWidget(self._btn_event)
        layout.addWidget(self._btn_geogit)
        layout.addStretch()

        self._current = "temporal"
        self._buttons = {
            "temporal": self._btn_temporal,
            "event": self._btn_event,
            "geogit": self._btn_geogit,
        }
        self._apply_styles()

    def mode(self) -> str:
        return self._current

    def setMode(self, mode: str) -> None:
        if mode not in self.VALID_MODES:
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
        for mode_key, btn in self._buttons.items():
            is_active = (mode_key == self._current)
            btn.setStyleSheet(active_ss if is_active else inactive_ss)
            btn.setChecked(is_active)
