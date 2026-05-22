"""Mode selector widget for RestoreMode (Event / Temporal / Review).

Qt5/Qt6 compatible via qgis.PyQt. Emits modeChanged(str) when the
user switches between "event", "temporal" and "review".
"""
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import QWidget, QHBoxLayout, QLabel

from .segmented_switch import SegmentedSwitch


class RestoreModeSelector(QWidget):
    """Three-segment animated toggle selector for restore mode."""

    VALID_MODES = ("event", "temporal", "review")
    _MODE_IDX = {"temporal": 0, "event": 1, "review": 2}
    _IDX_MODE = {0: "temporal", 1: "event", 2: "review"}

    modeChanged = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self._label = QLabel(self.tr("Mode :"))
        self._label.setStyleSheet("font-weight: bold;")

        self._switch = SegmentedSwitch(
            [self.tr("Rewind"), self.tr("Restore"), self.tr("Review")],
            self,
        )
        self._switch.setToolTip(self.tr("Choisir le mode de restauration"))
        self._switch.segmentClicked.connect(self._on_segment)

        layout.addWidget(self._label)
        layout.addWidget(self._switch)
        layout.addStretch()

        self._current = "temporal"

    def mode(self) -> str:
        return self._current

    def setMode(self, mode: str) -> None:
        if mode not in self.VALID_MODES:
            return
        self._current = mode
        self._switch.setCurrentIndex(self._MODE_IDX[mode])

    def _on_segment(self, idx: int) -> None:
        mode = self._IDX_MODE[idx]
        if mode == self._current:
            return
        self._current = mode
        self.modeChanged.emit(mode)
