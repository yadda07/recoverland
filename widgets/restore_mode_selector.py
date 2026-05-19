"""Mode selector widget for RestoreMode (Event / Temporal / GeoGit).

Qt5/Qt6 compatible via qgis.PyQt. Emits modeChanged(str) when the
user switches between "event", "temporal" and "geogit".
"""
from qgis.PyQt.QtCore import QRectF, QVariantAnimation, pyqtSignal
from qgis.PyQt.QtGui import QColor, QPainter
from qgis.PyQt.QtWidgets import QWidget, QHBoxLayout, QLabel

from ..compat import QtCompat


class _ModeSwitch(QWidget):
    """Custom-painted 3-segment switch with sliding cursor animation."""

    segmentClicked = pyqtSignal(int)

    _HEIGHT = 35
    _SEGMENT_W = 100
    _SEGMENTS = 3
    _MARGIN = 3

    def __init__(self, labels, parent=None):
        super().__init__(parent)
        self._labels = labels
        self._current = 0
        self._anim_pos = 0.0
        self._emitting = False
        self._anim = QVariantAnimation(self)
        self._anim.setDuration(180)
        self._anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        self._anim.valueChanged.connect(self._on_anim)
        self.setFixedSize(self._SEGMENT_W * self._SEGMENTS, self._HEIGHT)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)

    def currentIndex(self) -> int:
        return self._current

    def setCurrentIndex(self, idx: int, animated: bool = False) -> None:
        if self._emitting:
            return
        idx = max(0, min(idx, self._SEGMENTS - 1))
        if idx == self._current:
            return
        self._current = idx
        self._move_slider(idx, animated)

    def _move_slider(self, idx: int, animated: bool) -> None:
        target = float(idx)
        self._anim.stop()
        if animated:
            self._anim.setStartValue(float(self._anim_pos))
            self._anim.setEndValue(target)
            self._anim.start()
            return
        self._anim_pos = target
        self.update()

    def _on_anim(self, val) -> None:
        self._anim_pos = float(val)
        self.update()

    def mousePressEvent(self, event) -> None:
        x = self._event_x(event)
        idx = min(int(x / self._SEGMENT_W), self._SEGMENTS - 1)
        if idx < 0:
            idx = 0
        if idx == self._current:
            return
        self._current = idx
        self._move_slider(idx, animated=True)
        self._emitting = True
        self.segmentClicked.emit(idx)
        self._emitting = False

    def _event_x(self, event) -> float:
        if hasattr(event, "position"):
            return float(event.position().x())
        return float(event.x())

    def paintEvent(self, _event) -> None:
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        self._draw_bg(p)
        self._draw_slider(p)
        self._draw_labels(p)

    def _draw_bg(self, p: QPainter) -> None:
        pal = self.palette()
        mid = pal.mid().color()
        base = pal.base().color()
        p.setPen(QColor(mid.red(), mid.green(), mid.blue(), 120))
        p.setBrush(QColor(base.red(), base.green(), base.blue(), 230))
        p.drawRoundedRect(
            QRectF(0.5, 0.5, self.width() - 1, self.height() - 1), 6, 6)

    def _draw_slider(self, p: QPainter) -> None:
        pal = self.palette()
        hl = pal.highlight().color()
        m = self._MARGIN
        x = m + self._SEGMENT_W * self._anim_pos
        rect = QRectF(x, m, self._SEGMENT_W - m * 2, self.height() - m * 2)
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(QColor(hl.red(), hl.green(), hl.blue(), 210))
        p.drawRoundedRect(rect, 5, 5)

    def _draw_labels(self, p: QPainter) -> None:
        active_i = round(self._anim_pos)
        pal = self.palette()
        for i, label in enumerate(self._labels):
            rect = QRectF(
                i * self._SEGMENT_W, 0, self._SEGMENT_W, self.height())
            active = (i == active_i)
            color = pal.highlightedText().color() if active else pal.text().color()
            font = p.font()
            font.setBold(active)
            p.setFont(font)
            p.setPen(color)
            p.drawText(rect, QtCompat.ALIGN_CENTER, label)


class RestoreModeSelector(QWidget):
    """Three-segment animated toggle selector for restore mode."""

    VALID_MODES = ("event", "temporal", "geogit")
    _MODE_IDX = {"temporal": 0, "event": 1, "geogit": 2}
    _IDX_MODE = {0: "temporal", 1: "event", 2: "geogit"}

    modeChanged = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self._label = QLabel(self.tr("Mode :"))
        self._label.setStyleSheet("font-weight: bold;")

        self._switch = _ModeSwitch(
            [self.tr("Version"), self.tr("Action"), self.tr("GeoGit")],
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
