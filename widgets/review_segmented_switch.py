"""Two-segment Review toggle — thin wrapper around SegmentedSwitch.

Emits ``toggled(bool)`` when the user clicks a segment.
Index 0 = "Présent" (unchecked), index 1 = "Review" (checked).
"""
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import QWidget, QHBoxLayout

from .segmented_switch import SegmentedSwitch


class ReviewSegmentedSwitch(QWidget):

    toggled = pyqtSignal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._checked = False
        self._switch = SegmentedSwitch(
            [self.tr("Présent"), "Review"], self,
        )
        self._switch.setToolTip(self.tr("Basculer entre l'etat present et Review"))
        self._switch.segmentClicked.connect(self._on_segment)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self._switch)
        self.setFixedSize(self._switch.size())

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, value: bool, animated: bool = False):
        if self._checked == value:
            return
        self._checked = value
        self._switch.setCurrentIndex(1 if value else 0, animated=animated)

    def _on_segment(self, idx: int) -> None:
        checked = idx == 1
        if checked == self._checked:
            return
        self._checked = checked
        self.toggled.emit(checked)
