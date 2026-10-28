# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Widget de logo SVG thematique pour RecoverLand avec onde optique unique en cache HiDPI.

Remplace l'ancienne double animation (particules de fumee + ruban en rotation
avec re-rasterisation SVG a chaque frame) par une unique onde chromatique
directionnelle peinte en CompositionMode_SourceAtop sur un QPixmap HiDPI
pre-rendu une seule fois au chargement ou au changement de palette.
"""
from __future__ import annotations

import math
import time
from typing import Optional, Tuple

from qgis.PyQt.QtCore import QByteArray, QRectF, QVariantAnimation
from qgis.PyQt.QtGui import QColor, QLinearGradient, QPainter, QPixmap
from qgis.PyQt.QtSvg import QSvgRenderer
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat
from ..core.logger import flog
from .theme import blend, should_animate

_DEFAULT_WIDTH = 460
_LOGO_HEIGHT = 70
_LOGO_TOP = 4
_DEFAULT_ASPECT_RATIO = 6.57
_FALLBACK_FONT_PT = 16
_CYCLE_DURATION_MS = 1600
_OPACITY_STEP_IN = 0.14
_OPACITY_STEP_OUT = 0.12
_MIN_FRAME_INTERVAL_S = 0.030
_WAVE_WIDTH_RATIO = 0.28
_WAVE_MIN_HALF_W = 84.0
_WAVE_SLANT_Y = 22.0
_LINEAR_EASE_WEIGHT = 0.35
_COSINE_EASE_WEIGHT = 0.65
_BRAND_GOLD_HEX = "#febf00"
_SPECULAR_LIGHT_HEX = "#ffffff"
_CREST_GOLD_RATIO = 0.55
_CREST_LIGHT_RATIO = 0.40
_ALPHA_WING = 145
_ALPHA_BODY = 235
_ALPHA_CREST = 250
_STOP_WING_L = 0.18
_STOP_BODY_L = 0.38
_STOP_CREST = 0.50
_STOP_BODY_R = 0.62
_STOP_WING_R = 0.82


class ThemedLogoWidget(QWidget):
    """Logo vectoriel pre-rendu en cache avec onde lumineuse directionnelle unique."""

    _CYCLE_DURATION_MS = _CYCLE_DURATION_MS
    _OPACITY_STEP = _OPACITY_STEP_IN

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._renderer = QSvgRenderer(self)
        self._fallback_text = "RECOVERLAND"
        self._logo_height = _LOGO_HEIGHT
        self._logo_top = _LOGO_TOP
        self._cycle_phase = 0.0
        self._smoke_opacity = 0.0
        self._smoke_active = False
        self._smoke_color = QColor(0, 0, 0, 0)
        self._effect_mode = "recover"
        self._last_repaint_ts = 0.0
        self._base_pixmap: Optional[QPixmap] = None
        self._work_pixmap: Optional[QPixmap] = None
        self._cache_key: Optional[Tuple[int, int, float]] = None
        self._anim = QVariantAnimation(self)
        self._anim.setStartValue(0.0)
        self._anim.setEndValue(1.0)
        self._anim.setDuration(_CYCLE_DURATION_MS)
        self._anim.setLoopCount(-1)
        self._anim.valueChanged.connect(self._on_anim_value)
        font = self.font()
        font.setPointSize(_FALLBACK_FONT_PT)
        font.setBold(True)
        self.setFont(font)
        self.setFixedSize(_DEFAULT_WIDTH, self._logo_height + self._logo_top * 2)

    def load_svg_data(self, svg_data: str) -> bool:
        """Charge le contenu SVG et reconstruit le QPixmap HiDPI statique en cache."""
        svg_clean = svg_data.replace('width="100%"', "").replace('height="100%"', "")
        self._renderer.load(QByteArray(svg_clean.encode("utf-8")))
        if not self._renderer.isValid():
            self._invalidate_cache()
            self.update()
            flog("module=widgets.themed_logo fn=load_svg_data valid=False", "WARNING")
            return False
        view_box = self._renderer.viewBoxF()
        ratio = view_box.width() / view_box.height() if view_box.height() > 0 else _DEFAULT_ASPECT_RATIO
        width = max(int(round(self._logo_height * ratio)), 1)
        self.setFixedSize(width, self._logo_height + self._logo_top * 2)
        self._rebuild_pixmap_cache()
        self.update()
        flog(
            f"module=widgets.themed_logo fn=load_svg_data valid=True width={width} height={self.height()}",
            "DEBUG",
        )
        return True

    def start_recovery_effect(self, color: QColor) -> None:
        """Active l'onde de balayage avant (mode recherche)."""
        self._start_effect(color, "recover")

    def start_restore_effect(self, color: QColor) -> None:
        """Active l'onde de rembobinage temporel inverse (mode Rewind / Restore)."""
        self._start_effect(color, "restore")

    def _start_effect(self, color: QColor, mode: str) -> None:
        if mode not in {"recover", "restore"}:
            raise ValueError(f"Unknown logo effect mode: {mode}")
        prev_mode = self._effect_mode
        was_active = self._smoke_active
        self._smoke_color = QColor(color.red(), color.green(), color.blue(), color.alpha())
        self._effect_mode = mode
        if not should_animate():
            self._smoke_active = False
            self._smoke_opacity = 0.0
            flog("module=widgets.themed_logo fn=_start_effect skipped=reduced_motion", "DEBUG")
            return
        self._smoke_active = True
        self._last_repaint_ts = 0.0
        if self._anim.state() != QtCompat.ANIM_STATE_RUNNING:
            if self._smoke_opacity <= 0.0:
                self._cycle_phase = 0.0
            self._anim.start()
        self.update()
        flog(
            f"module=widgets.themed_logo fn=_start_effect state={was_active}->True "
            f"mode={prev_mode}->{mode} color={self._smoke_color.name()}",
            "DEBUG",
        )

    def stop_recovery_effect(self) -> None:
        """Demande l'extinction progressive de l'onde et l'arret du timer d'animation."""
        was_active = self._smoke_active
        self._smoke_active = False
        self._last_repaint_ts = 0.0
        self.update()
        if was_active:
            flog("module=widgets.themed_logo fn=stop_recovery_effect state=True->False", "DEBUG")

    def _on_anim_value(self, val: object) -> None:
        now = time.monotonic()
        self._cycle_phase = float(val)
        if self._smoke_active:
            if (now - self._last_repaint_ts) < _MIN_FRAME_INTERVAL_S and self._smoke_opacity >= 1.0:
                return
            self._smoke_opacity = min(1.0, self._smoke_opacity + _OPACITY_STEP_IN)
            self._last_repaint_ts = now
            self.update()
            return
        self._smoke_opacity = max(0.0, self._smoke_opacity - _OPACITY_STEP_OUT)
        if self._smoke_opacity <= 0.0 and self._anim.state() == QtCompat.ANIM_STATE_RUNNING:
            self._anim.stop()
            self._cycle_phase = 0.0
        self._last_repaint_ts = now
        self.update()

    def _current_dpr(self) -> float:
        getter = getattr(self, "devicePixelRatioF", None)
        if callable(getter):
            try:
                return max(1.0, float(getter()))
            except Exception:
                return 1.0
        return 1.0

    def _invalidate_cache(self) -> None:
        self._base_pixmap = None
        self._work_pixmap = None
        self._cache_key = None

    def _ensure_pixmap_cache(self) -> None:
        dpr = round(self._current_dpr(), 2)
        key = (max(1, self.width()), max(1, self._logo_height), dpr)
        if self._base_pixmap is None or self._work_pixmap is None or self._cache_key != key:
            self._rebuild_pixmap_cache()

    def _rebuild_pixmap_cache(self) -> None:
        dpr = self._current_dpr()
        w = max(1, self.width())
        h = max(1, self._logo_height)
        px_w = max(1, int(round(w * dpr)))
        px_h = max(1, int(round(h * dpr)))
        base = QPixmap(px_w, px_h)
        base.setDevicePixelRatio(dpr)
        base.fill(QColor(0, 0, 0, 0))
        painter = QPainter(base)
        painter.setRenderHint(QtCompat.ANTIALIAS, True)
        painter.setRenderHint(QtCompat.SMOOTH_PIXMAP, True)
        target_rect = QRectF(0.0, 0.0, float(w), float(h))
        if self._renderer.isValid():
            self._renderer.render(painter, target_rect)
        else:
            painter.setPen(self.palette().windowText().color())
            painter.drawText(target_rect.toRect(), QtCompat.ALIGN_CENTER, self._fallback_text)
        painter.end()
        work = QPixmap(px_w, px_h)
        work.setDevicePixelRatio(dpr)
        work.fill(QColor(0, 0, 0, 0))
        self._base_pixmap = base
        self._work_pixmap = work
        self._cache_key = (w, h, round(dpr, 2))

    @staticmethod
    def _ease_wave(phase: float) -> float:
        t = max(0.0, min(1.0, float(phase)))
        cosine = 0.5 - 0.5 * math.cos(math.pi * t)
        return _LINEAR_EASE_WEIGHT * t + _COSINE_EASE_WEIGHT * cosine

    def _build_wave_gradient(self, center_x: float, half_w: float) -> QLinearGradient:
        direction = -1.0 if self._effect_mode == "restore" else 1.0
        grad = QLinearGradient(
            center_x - half_w,
            0.0,
            center_x + half_w,
            _WAVE_SLANT_Y * direction,
        )
        accent = QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue())
        gold_mix = blend(accent, QColor(_BRAND_GOLD_HEX), _CREST_GOLD_RATIO)
        crest = blend(gold_mix, QColor(_SPECULAR_LIGHT_HEX), _CREST_LIGHT_RATIO)
        vis = max(0.0, min(1.0, self._smoke_opacity))
        wing_c = QColor(accent.red(), accent.green(), accent.blue(), int(_ALPHA_WING * vis))
        body_c = QColor(accent.red(), accent.green(), accent.blue(), int(_ALPHA_BODY * vis))
        crest_c = QColor(crest.red(), crest.green(), crest.blue(), int(_ALPHA_CREST * vis))
        clear_c = QColor(accent.red(), accent.green(), accent.blue(), 0)
        grad.setColorAt(0.0, clear_c)
        grad.setColorAt(_STOP_WING_L, wing_c)
        grad.setColorAt(_STOP_BODY_L, body_c)
        grad.setColorAt(_STOP_CREST, crest_c)
        grad.setColorAt(_STOP_BODY_R, body_c)
        grad.setColorAt(_STOP_WING_R, wing_c)
        grad.setColorAt(1.0, clear_c)
        return grad

    def _compose_wave_pixmap(self) -> QPixmap:
        self._ensure_pixmap_cache()
        if self._base_pixmap is None or self._work_pixmap is None:
            return QPixmap()
        if self._smoke_opacity <= 0.001:
            return self._base_pixmap
        w = float(max(1, self.width()))
        h = float(max(1, self._logo_height))
        eased = self._ease_wave(self._cycle_phase)
        progress = (1.0 - eased) if self._effect_mode == "restore" else eased
        half_w = max(w * _WAVE_WIDTH_RATIO, _WAVE_MIN_HALF_W)
        center_x = -half_w + (w + 2.0 * half_w) * progress
        wp = QPainter(self._work_pixmap)
        wp.setRenderHint(QtCompat.ANTIALIAS, True)
        wp.setCompositionMode(QtCompat.COMPOSITION_SOURCE)
        wp.drawPixmap(0, 0, self._base_pixmap)
        wp.setCompositionMode(QtCompat.COMPOSITION_SOURCE_ATOP)
        wp.fillRect(QRectF(0.0, 0.0, w, h), self._build_wave_gradient(center_x, half_w))
        wp.end()
        return self._work_pixmap

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._invalidate_cache()

    def paintEvent(self, _event) -> None:
        pixmap = self._compose_wave_pixmap()
        if pixmap.isNull():
            return
        painter = QPainter(self)
        painter.setRenderHint(QtCompat.SMOOTH_PIXMAP, True)
        painter.drawPixmap(0, self._logo_top, pixmap)
