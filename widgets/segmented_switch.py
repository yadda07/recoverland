"""Selecteur segmente anime inspire de VTT (contour actif sobre, sans soulignement redondant).

Peint sur les jetons ThemeTokens avec :
- curseur glissant OutCubic (fond accent_doux + contour 1 px accent, sans barre de soulignement) ;
- pictogrammes SVG vectoriels rendus directement sur QPainter (nettete HiDPI, zero fond) ;
- survol discret et etat desactive par teinte eteinte (jamais d'opacite seule).
"""
from __future__ import annotations

from typing import Optional, Sequence

from qgis.PyQt.QtCore import QRectF, QVariantAnimation, pyqtSignal
from qgis.PyQt.QtGui import QFontMetrics, QPainter, QPen
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat
from .icons import draw_svg_icon
from .theme import (
    HAUTEUR_COMPACTE,
    ICONE_XS,
    NAVIGATION_DUREE,
    RAYON_CONTROLE,
    apply_named_weight,
    resolve_theme,
    should_animate,
)


class SegmentedSwitch(QWidget):
    """Selecteur a N segments avec curseur glissant et pictogrammes vectoriels nets."""

    segmentClicked = pyqtSignal(int)

    _HEIGHT = HAUTEUR_COMPACTE + 4
    _SEGMENT_W = 124
    _MARGIN = 3

    def __init__(
        self,
        labels: Sequence[str],
        parent: Optional[QWidget] = None,
        icons: Optional[Sequence[str]] = None,
    ) -> None:
        super().__init__(parent)
        self._labels = list(labels)
        self._icons = list(icons) if icons else []
        self._n_segments = len(self._labels)
        self._current = 0
        self._hover_idx = -1
        self._anim_pos = 0.0
        self._emitting = False
        self._anim = QVariantAnimation(self)
        self._anim.setDuration(NAVIGATION_DUREE)
        self._anim.setEasingCurve(QtCompat.EASE_OUT_CUBIC)
        self._anim.valueChanged.connect(self._on_anim)
        self.setMouseTracking(True)
        self.setFocusPolicy(QtCompat.FOCUS_TAB)
        self.setFixedSize(self._SEGMENT_W * self._n_segments, self._HEIGHT)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)

    def currentIndex(self) -> int:
        return self._current

    def setCurrentIndex(self, idx: int, animated: bool = False) -> None:
        if self._emitting:
            return
        idx = max(0, min(idx, self._n_segments - 1))
        if idx == self._current:
            return
        self._current = idx
        self._move_slider(idx, animated and should_animate())

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

    def mouseMoveEvent(self, event) -> None:
        x = self._event_x(event)
        idx = int(x / self._SEGMENT_W) if 0 <= x < self.width() else -1
        if idx != self._hover_idx:
            self._hover_idx = idx
            self.update()

    def leaveEvent(self, _event) -> None:
        if self._hover_idx != -1:
            self._hover_idx = -1
            self.update()

    def mousePressEvent(self, event) -> None:
        if not self.isEnabled():
            return
        x = self._event_x(event)
        if x < 0 or x >= self.width():
            return
        idx = min(int(x / self._SEGMENT_W), self._n_segments - 1)
        if idx == self._current:
            return
        self._current = idx
        self._move_slider(idx, animated=should_animate())
        self._emitting = True
        self.segmentClicked.emit(idx)
        self._emitting = False

    @staticmethod
    def _event_x(event) -> float:
        if hasattr(event, "position"):
            return float(event.position().x())
        return float(event.x())

    def paintEvent(self, _event) -> None:
        t = resolve_theme(self)
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        self._draw_bg(p, t)
        self._draw_hover(p, t)
        self._draw_slider(p, t)
        self._draw_labels(p, t)

    def _draw_bg(self, p: QPainter, t) -> None:
        bg = t.surface_douce if not self.isEnabled() else t.surface
        border = t.accent if self.hasFocus() else t.ligne_controle
        p.setPen(QPen(border, 1.5 if self.hasFocus() else 1))
        p.setBrush(bg)
        p.drawRoundedRect(
            QRectF(0.5, 0.5, self.width() - 1, self.height() - 1),
            RAYON_CONTROLE, RAYON_CONTROLE,
        )

    def _draw_hover(self, p: QPainter, t) -> None:
        if not self.isEnabled() or self._hover_idx < 0 or self._hover_idx == self._current:
            return
        m = self._MARGIN
        x = m + self._SEGMENT_W * self._hover_idx
        rect = QRectF(x, m, self._SEGMENT_W - m * 2, self.height() - m * 2)
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(t.surface_survol)
        p.drawRoundedRect(rect, RAYON_CONTROLE - 1, RAYON_CONTROLE - 1)

    def _draw_slider(self, p: QPainter, t) -> None:
        # Affirmation sobre : uniquement le contour + fond doux (sans soulignement en bas)
        m = self._MARGIN
        x = m + self._SEGMENT_W * self._anim_pos
        w = self._SEGMENT_W - m * 2
        h = self.height() - m * 2
        rect = QRectF(x, m, w, h)
        fill = t.surface_douce if not self.isEnabled() else t.accent_doux
        border = t.ligne if not self.isEnabled() else t.accent
        p.setPen(QPen(border, 1.2))
        p.setBrush(fill)
        p.drawRoundedRect(rect, RAYON_CONTROLE - 1, RAYON_CONTROLE - 1)

    def _draw_labels(self, p: QPainter, t) -> None:
        active_i = round(self._anim_pos)
        for i, label in enumerate(self._labels):
            rect = QRectF(i * self._SEGMENT_W, 0, self._SEGMENT_W, self.height())
            active = i == active_i
            if not self.isEnabled():
                color = t.tertiaire
            elif active:
                color = t.accent
            else:
                color = t.secondaire
            font = p.font()
            font.setFamily(t.police)
            apply_named_weight(font, "bold" if active else "normal")
            p.setFont(font)
            fm = QFontMetrics(font)
            text_w = fm.horizontalAdvance(label)
            icon_name = self._icons[i] if i < len(self._icons) else ""
            icon_w = (ICONE_XS + 7) if icon_name else 0
            start_x = rect.x() + (self._SEGMENT_W - (text_w + icon_w)) / 2.0
            if icon_name:
                iy = rect.y() + (self.height() - ICONE_XS) / 2.0
                draw_svg_icon(p, icon_name, QRectF(start_x, iy, ICONE_XS, ICONE_XS), color)
                start_x += icon_w
            p.setPen(color)
            p.drawText(
                QRectF(start_x, 0, text_w + 4, self.height()),
                int(QtCompat.ALIGN_LEFT) | int(QtCompat.ALIGN_VCENTER),
                label,
            )


__all__ = ["SegmentedSwitch"]
