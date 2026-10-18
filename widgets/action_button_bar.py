"""Barre d'actions segmentee inspiree de Bouton.qml de VTT (rendu vectoriel direct)."""
from qgis.PyQt.QtCore import QObject, QRectF, QVariantAnimation, pyqtSignal
from qgis.PyQt.QtGui import QFontMetrics, QPainter, QPen
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat
from .icons import draw_svg_icon
from .theme import (
    HAUTEUR_COMPACTE, ICONE_XS, MICRO_DUREE, RAYON_CONTROLE,
    apply_named_weight, resolve_theme, should_animate,
)


class _Segment:
    __slots__ = ("label", "icon", "icon_name", "tooltip", "enabled", "visible")

    def __init__(self, label, icon=None, tooltip="", enabled=True, visible=True, icon_name=""):
        self.label = label
        self.icon = icon
        self.icon_name = icon_name
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

    def setIconName(self, icon_name: str):
        self._bar.setSegmentIconName(self._index, icon_name)

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
    """Segmented action bar with sliding hover and primary highlight (VTT tokens)."""

    segmentClicked = pyqtSignal(int)

    _HEIGHT = HAUTEUR_COMPACTE + 4
    _SEGMENT_W = 124
    _MARGIN_PRIMARY = 3
    _MARGIN_HOVER = 3
    _ICON_SIZE = ICONE_XS
    _ICON_SPACING = 7

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
        anim.setDuration(MICRO_DUREE)
        anim.setEasingCurve(QtCompat.EASE_OUT_CUBIC)
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

    def setSegmentIconName(self, index: int, icon_name: str):
        self._segments[index].icon_name = icon_name
        self.update()

    def setSegmentTooltip(self, index, tooltip):
        self._segments[index].tooltip = tooltip
        self.update()

    def setSegmentEnabled(self, index, enabled):
        seg = self._segments[index]
        if seg.enabled != enabled:
            seg.enabled = enabled
            self.update()

    def segmentEnabled(self, index):
        return self._segments[index].enabled

    def setSegmentVisible(self, index, visible):
        seg = self._segments[index]
        if seg.visible == visible:
            return
        seg.visible = visible
        self._visible = [i for i, s in enumerate(self._segments) if s.visible]
        self._update_geometry()
        if self._primary_idx not in self._visible and self._visible:
            self._primary_idx = self._visible[0]
        self._clamp_slider_pos()
        self.update()

    def segmentVisible(self, index):
        return self._segments[index].visible

    def setPrimaryIndex(self, index):
        if 0 <= index < len(self._segments):
            self._primary_idx = index
            target = self._visual_index(index)
            if not self.isVisible():
                self._anim.stop()
                self._slider_pos = float(target) if target >= 0 else -1.0
            else:
                self._move_slider(target, self._anim, self._slider_pos)
            self.update()

    def primaryIndex(self):
        return self._primary_idx

    def _update_geometry(self):
        self.setVisible(bool(self._visible))
        self.setFixedSize(self._SEGMENT_W * len(self._visible), self._HEIGHT)
        self.update()

    def _clamp_slider_pos(self):
        if not self._visible:
            self._slider_pos = -1.0
        else:
            self._slider_pos = max(0.0, min(self._slider_pos, float(len(self._visible) - 1)))
        self._anim.stop()

    def _visual_index(self, logical_idx):
        return self._visible.index(logical_idx) if logical_idx in self._visible else -1

    def _move_slider(self, visual_idx, anim, current_pos):
        target = float(visual_idx) if visual_idx >= 0 else -1.0
        anim.stop()
        if not should_animate() or target < 0 or current_pos < 0:
            if anim is self._anim:
                self._slider_pos = target
            else:
                self._hover_pos = target
            self.update()
            return
        anim.setStartValue(float(current_pos))
        anim.setEndValue(target)
        anim.start()

    def _logical_index_at(self, x):
        if not self._visible or x < 0 or x >= self._SEGMENT_W * len(self._visible):
            return -1
        return self._visible[min(int(x / self._SEGMENT_W), len(self._visible) - 1)]

    @staticmethod
    def _event_x(event):
        return float(event.position().x()) if hasattr(event, "position") else float(event.x())

    def mouseMoveEvent(self, event):
        idx = self._logical_index_at(self._event_x(event))
        if idx != self._hover_idx:
            self._hover_idx = idx
            self.setToolTip(self._segments[idx].tooltip if idx >= 0 else "")
        self._move_slider(self._visual_index(idx) if idx >= 0 else -1, self._hover_anim, self._hover_pos)

    def leaveEvent(self, _event):
        self._hover_idx = -1
        self.setToolTip("")
        self._move_slider(-1, self._hover_anim, self._hover_pos)

    def mousePressEvent(self, event):
        idx = self._logical_index_at(self._event_x(event))
        if idx >= 0 and self._segments[idx].enabled:
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
        t = resolve_theme(self)
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        p.setPen(QPen(t.ligne_controle, 1))
        p.setBrush(t.surface)
        p.drawRoundedRect(QRectF(0.5, 0.5, self.width() - 1, self.height() - 1), RAYON_CONTROLE, RAYON_CONTROLE)
        self._draw_hover_pill(p, t)
        self._draw_primary_pill(p, t)
        for visual, logical in enumerate(self._visible):
            rect = QRectF(visual * self._SEGMENT_W, 0, self._SEGMENT_W, self._HEIGHT)
            self._draw_one_segment(p, t, self._segments[logical], rect, logical)

    def _draw_hover_pill(self, p: QPainter, t):
        if self._hover_pos < 0 or not self._visible:
            return
        m = self._MARGIN_HOVER
        rect = QRectF(m + self._SEGMENT_W * self._hover_pos, m, self._SEGMENT_W - m * 2, self.height() - m * 2)
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(t.surface_survol)
        p.drawRoundedRect(rect, RAYON_CONTROLE - 1, RAYON_CONTROLE - 1)

    def _draw_primary_pill(self, p: QPainter, t):
        if self._slider_pos < 0 or not self._visible or self._primary_idx < 0:
            return
        seg = self._segments[self._primary_idx]
        if not seg.enabled:
            return
        m = self._MARGIN_PRIMARY
        rect = QRectF(m + self._SEGMENT_W * self._slider_pos, m, self._SEGMENT_W - m * 2, self.height() - m * 2)
        p.setPen(QPen(t.accent, 1.2))
        p.setBrush(t.surface_pressee if self._press_idx == self._primary_idx else t.accent_doux)
        p.drawRoundedRect(rect, RAYON_CONTROLE - 1, RAYON_CONTROLE - 1)

    def _draw_one_segment(self, p: QPainter, t, seg, rect, logical_idx: int):
        is_primary = logical_idx == self._primary_idx
        color = t.tertiaire if not seg.enabled else (t.accent if is_primary else t.texte)
        font = p.font()
        font.setFamily(t.police)
        apply_named_weight(font, "bold" if is_primary else "demi")
        p.setFont(font)
        text_w = QFontMetrics(font).horizontalAdvance(seg.label)
        has_icon = bool(seg.icon_name or seg.icon is not None)
        icon_w = (self._ICON_SIZE + self._ICON_SPACING) if has_icon else 0
        x = rect.x() + (self._SEGMENT_W - (text_w + icon_w)) / 2.0
        if has_icon:
            icon_y = rect.y() + (self._HEIGHT - self._ICON_SIZE) / 2.0
            if seg.icon_name:
                draw_svg_icon(p, seg.icon_name, QRectF(x, icon_y, self._ICON_SIZE, self._ICON_SIZE), color)
            else:
                pm = seg.icon.pixmap(self._ICON_SIZE, self._ICON_SIZE)
                if not pm.isNull():
                    p.drawPixmap(int(round(x)), int(round(icon_y)), pm)
            x += icon_w
        p.setPen(color)
        p.drawText(
            QRectF(x, 0, text_w + 4, self._HEIGHT),
            int(QtCompat.ALIGN_LEFT) | int(QtCompat.ALIGN_VCENTER),
            seg.label,
        )
