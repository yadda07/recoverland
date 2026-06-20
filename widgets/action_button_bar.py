"""Segmented action bar with sliding hover and primary highlight."""
from qgis.PyQt.QtCore import QObject, QRectF, QVariantAnimation, pyqtSignal
from qgis.PyQt.QtGui import QColor, QFontMetrics, QPainter
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat


class _Segment:
    __slots__ = ("label", "icon", "tooltip", "enabled", "visible")

    def __init__(self, label, icon=None, tooltip="", enabled=True, visible=True):
        self.label = label
        self.icon = icon
        self.tooltip = tooltip
        self.enabled = enabled
        self.visible = visible


class SegmentProxy(QObject):
    clicked = pyqtSignal()

    def __init__(self, bar, index):
        super().__init__(bar)
        self._bar = bar
        self._index = index
        self._bar.segmentClicked.connect(self._on_bar_clicked)

    def _on_bar_clicked(self, idx):
        if idx == self._index:
            self.clicked.emit()

    def setText(self, text):
        self._bar.setSegmentLabel(self._index, text)

    def setIcon(self, icon):
        self._bar.setSegmentIcon(self._index, icon)

    def setToolTip(self, tooltip):
        self._bar.setSegmentTooltip(self._index, tooltip)

    def setEnabled(self, enabled):
        self._bar.setSegmentEnabled(self._index, enabled)

    def setVisible(self, visible):
        self._bar.setSegmentVisible(self._index, visible)

    def isEnabled(self):
        return self._bar.segmentEnabled(self._index)

    def isVisible(self):
        return self._bar.segmentVisible(self._index)


class ActionButtonBar(QWidget):
    """Segmented action bar with sliding hover and primary highlight."""

    segmentClicked = pyqtSignal(int)

    _HEIGHT = 35
    _SEGMENT_W = 120
    _MARGIN_PRIMARY = 0
    _MARGIN_HOVER = 4
    _ICON_SIZE = 16
    _ICON_SPACING = 6
    _SLIDER_ALPHA_PRIMARY = 210
    _SLIDER_ALPHA_HOVER = 130

    def __init__(self, segments, parent=None):
        super().__init__(parent)
        self._segments = [_Segment(**s) if isinstance(s, dict) else _Segment(s) for s in segments]
        self._visible = [i for i, s in enumerate(self._segments) if s.visible]
        self._primary_idx = -1
        self._hover_idx = -1
        self._press_idx = -1
        self._slider_pos = 0.0
        self._hover_pos = -1.0
        self._anim = self._build_anim(self._on_slider_anim)
        self._hover_anim = self._build_anim(self._on_hover_anim)
        self._proxies = [SegmentProxy(self, i) for i in range(len(self._segments))]
        self.setMouseTracking(True)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._update_geometry()

    def _build_anim(self, callback):
        anim = QVariantAnimation(self)
        anim.setDuration(180)
        anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        anim.valueChanged.connect(callback)
        return anim

    def _on_slider_anim(self, val):
        self._slider_pos = float(val)
        self.update()

    def _on_hover_anim(self, val):
        self._hover_pos = float(val)
        self.update()

    def segment(self, index):
        return self._proxies[index]

    def setSegmentLabel(self, index, label):
        self._segments[index].label = label
        self.update()

    def setSegmentIcon(self, index, icon):
        self._segments[index].icon = icon
        self.update()

    def setSegmentTooltip(self, index, tooltip):
        self._segments[index].tooltip = tooltip
        self.update()

    def setSegmentEnabled(self, index, enabled):
        seg = self._segments[index]
        if seg.enabled == enabled:
            return
        seg.enabled = enabled
        self.update()

    def segmentEnabled(self, index):
        return self._segments[index].enabled

    def setSegmentVisible(self, index, visible):
        seg = self._segments[index]
        if seg.visible == visible:
            return
        seg.visible = visible
        self._rebuild_visible()
        self._update_geometry()
        if self._primary_idx not in self._visible and self._visible:
            self._primary_idx = self._visible[0]
        self._clamp_slider_pos()
        self.update()

    def segmentVisible(self, index):
        return self._segments[index].visible

    def setPrimaryIndex(self, index):
        if index < 0 or index >= len(self._segments):
            return
        self._primary_idx = index
        self._move_slider(self._visual_index(index), self._anim, self._slider_pos)
        self.update()

    def primaryIndex(self):
        return self._primary_idx

    def _rebuild_visible(self):
        self._visible = [i for i, s in enumerate(self._segments) if s.visible]

    def _update_geometry(self):
        self.setVisible(bool(self._visible))
        self.setFixedSize(self._SEGMENT_W * len(self._visible), self._HEIGHT)
        self.update()

    def _clamp_slider_pos(self):
        if not self._visible:
            self._slider_pos = -1.0
        else:
            self._slider_pos = max(
                0.0, min(self._slider_pos, float(len(self._visible) - 1))
            )
        self._anim.stop()

    def _visual_index(self, logical_idx):
        try:
            return self._visible.index(logical_idx)
        except ValueError:
            return -1

    def _move_slider(self, visual_idx, anim, current_pos):
        target = float(visual_idx) if visual_idx >= 0 else -1.0
        anim.stop()
        anim.setStartValue(float(current_pos))
        anim.setEndValue(target)
        anim.start()

    def _logical_index_at(self, x):
        if not self._visible:
            return -1
        visual = min(int(x / self._SEGMENT_W), len(self._visible) - 1)
        return self._visible[visual]

    def _event_x(self, event):
        if hasattr(event, "position"):
            return float(event.position().x())
        return float(event.x())

    def mouseMoveEvent(self, event):
        idx = self._logical_index_at(self._event_x(event))
        if idx != self._hover_idx:
            self._hover_idx = idx
            self._set_tooltip_for(idx)
        visual = self._visual_index(idx) if idx >= 0 else -1
        self._move_hover_slider(visual)

    def leaveEvent(self, event):
        self._hover_idx = -1
        self._set_tooltip_for(-1)
        self._move_hover_slider(-1)

    def _set_tooltip_for(self, logical_idx):
        text = ""
        if logical_idx >= 0:
            text = self._segments[logical_idx].tooltip
        self.setToolTip(text)

    def _move_hover_slider(self, visual_idx):
        self._move_slider(visual_idx, self._hover_anim, self._hover_pos)

    def mousePressEvent(self, event):
        idx = self._logical_index_at(self._event_x(event))
        if idx < 0 or not self._segments[idx].enabled:
            return
        self._press_idx = idx
        self.update()

    def mouseReleaseEvent(self, event):
        idx = self._logical_index_at(self._event_x(event))
        prev = self._press_idx
        self._press_idx = -1
        self.update()
        if idx == prev and idx >= 0 and self._segments[idx].enabled:
            self.segmentClicked.emit(idx)

    def paintEvent(self, _event):
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        self._draw_bg(p)
        self._draw_slider(p, self._slider_pos, self._SLIDER_ALPHA_PRIMARY, self._MARGIN_PRIMARY)
        self._draw_slider(p, self._hover_pos, self._SLIDER_ALPHA_HOVER, self._MARGIN_HOVER)
        self._draw_segments(p)

    def _draw_bg(self, p):
        pal = self.palette()
        mid = pal.mid().color()
        base = pal.base().color()
        p.setPen(QColor(mid.red(), mid.green(), mid.blue(), 200))
        p.setBrush(QColor(base.red(), base.green(), base.blue(), 230))
        p.drawRoundedRect(
            QRectF(0, 0, self.width(), self.height()), 6, 6)

    def _draw_slider(self, p, visual_pos, alpha, margin=0):
        if visual_pos < 0 or not self._visible:
            return
        pal = self.palette()
        hl = pal.highlight().color()
        x = margin + self._SEGMENT_W * visual_pos
        rect = QRectF(
            x, margin, self._SEGMENT_W - margin * 2, self.height() - margin * 2)
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(QColor(hl.red(), hl.green(), hl.blue(), alpha))
        p.drawRoundedRect(rect, 5, 5)

    def _draw_segments(self, p):
        for visual, logical in enumerate(self._visible):
            seg = self._segments[logical]
            rect = QRectF(visual * self._SEGMENT_W, 0, self._SEGMENT_W, self._HEIGHT)
            is_primary = (logical == self._primary_idx)
            is_pressed = (logical == self._press_idx)
            self._draw_segment(p, seg, rect, is_primary, is_pressed)

    def _draw_segment(self, p, seg, rect, is_primary, is_pressed):
        pal = self.palette()
        enabled = seg.enabled
        color = pal.highlightedText().color() if is_primary else pal.text().color()
        alpha = 90 if not enabled else (180 if is_pressed else 255)
        color = QColor(color.red(), color.green(), color.blue(), alpha)
        font = p.font()
        font.setBold(is_primary)
        p.setFont(font)

        fm = QFontMetrics(font)
        text_w = fm.horizontalAdvance(seg.label)
        icon_w = 0
        if seg.icon is not None:
            icon_w = self._ICON_SIZE + self._ICON_SPACING

        group_w = text_w + icon_w
        x = rect.x() + (self._SEGMENT_W - group_w) / 2

        if seg.icon is not None:
            icon_y = rect.y() + (self._HEIGHT - self._ICON_SIZE) / 2
            pixmap = seg.icon.pixmap(self._ICON_SIZE, self._ICON_SIZE)
            if not enabled:
                p.setOpacity(0.4)
            p.drawPixmap(int(x), int(icon_y), pixmap)
            if not enabled:
                p.setOpacity(1.0)
            x += icon_w

        p.setPen(color)
        text_rect = QRectF(x, 0, text_w, self._HEIGHT)
        p.drawText(text_rect, QtCompat.ALIGN_CENTER, seg.label)
