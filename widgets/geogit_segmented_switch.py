from qgis.PyQt.QtCore import QRectF, QVariantAnimation, pyqtSignal
from qgis.PyQt.QtGui import QColor, QPainter
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat


class GeoGitSegmentedSwitch(QWidget):

    toggled = pyqtSignal(bool)

    _WIDTH = 240
    _HEIGHT = 35
    _SEGMENT_WIDTH = 120
    _MARGIN = 3

    def __init__(self, parent=None):
        super().__init__(parent)
        self._checked = False
        self._anim_pos = 0.0
        self._emitting = False
        self._anim = QVariantAnimation(self)
        self._anim.setDuration(180)
        self._anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        self._anim.valueChanged.connect(self._update_pos)
        self.setFixedSize(self._WIDTH, self._HEIGHT)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setToolTip(self.tr("Basculer entre l'etat present et GeoGit"))

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, value: bool, animated: bool = False):
        if self._emitting:
            return
        if self._checked == value:
            return
        self._checked = value
        self._set_anim_target(value, animated)

    def _set_anim_target(self, checked: bool, animated: bool) -> None:
        target = 1.0 if checked else 0.0
        self._anim.stop()
        if animated:
            self._anim.setStartValue(float(self._anim_pos))
            self._anim.setEndValue(float(target))
            self._anim.start()
            return
        self._anim_pos = target
        self.update()

    def _update_pos(self, pos) -> None:
        self._anim_pos = float(pos)
        self.update()

    def mousePressEvent(self, event) -> None:
        checked = self._event_x(event) >= self._SEGMENT_WIDTH
        if checked == self._checked:
            return
        self._checked = checked
        self._set_anim_target(checked, animated=True)
        self._emitting = True
        self.toggled.emit(self._checked)
        self._emitting = False

    def _event_x(self, event) -> float:
        if hasattr(event, "position"):
            return float(event.position().x())
        return float(event.x())

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QtCompat.ANTIALIAS)
        self._paint_background(painter)
        self._paint_slider(painter)
        self._paint_labels(painter)

    def _paint_background(self, painter: QPainter) -> None:
        pal = self.palette()
        mid = pal.mid().color()
        base = pal.base().color()
        painter.setPen(QColor(mid.red(), mid.green(), mid.blue(), 120))
        painter.setBrush(QColor(base.red(), base.green(), base.blue(), 230))
        painter.drawRoundedRect(QRectF(0.5, 0.5, self.width() - 1, self.height() - 1), 6, 6)

    def _paint_slider(self, painter: QPainter) -> None:
        pal = self.palette()
        highlight = pal.highlight().color()
        x = self._MARGIN + self._SEGMENT_WIDTH * self._anim_pos
        rect = QRectF(
            x,
            self._MARGIN,
            self._SEGMENT_WIDTH - (self._MARGIN * 2),
            self.height() - (self._MARGIN * 2),
        )
        painter.setPen(QtCompat.NO_PEN)
        painter.setBrush(QColor(highlight.red(), highlight.green(), highlight.blue(), 210))
        painter.drawRoundedRect(rect, 5, 5)

    def _paint_labels(self, painter: QPainter) -> None:
        checked = self._anim_pos >= 0.5
        left_rect = QRectF(0, 0, self._SEGMENT_WIDTH, self.height())
        right_rect = QRectF(self._SEGMENT_WIDTH, 0, self._SEGMENT_WIDTH, self.height())
        self._paint_label(painter, self.tr("Présent"), left_rect, not checked)
        self._paint_label(painter, "GeoGit", right_rect, checked)

    def _paint_label(self, painter: QPainter, text: str, rect: QRectF, active: bool) -> None:
        pal = self.palette()
        color = pal.highlightedText().color() if active else pal.text().color()
        font = painter.font()
        font.setBold(active)
        painter.setFont(font)
        painter.setPen(color)
        painter.drawText(rect, QtCompat.ALIGN_CENTER, text)
