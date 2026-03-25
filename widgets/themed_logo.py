"""Themed SVG logo widget with smoke/sweep animation for RecoverLand."""
from qgis.PyQt.QtCore import QByteArray, QRectF, QTimer
from qgis.PyQt.QtGui import QPainter, QColor, QLinearGradient
from qgis.PyQt.QtSvg import QSvgRenderer
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat


class ThemedLogoWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._renderer = QSvgRenderer(self)
        self._fallback_text = "RECOVERLAND"
        self._logo_height = 70
        self._logo_top = 12
        self._smoke_progress = 0.0
        self._smoke_opacity = 0.0
        self._smoke_active = False
        self._recovery_sweep_progress = 0.0
        self._smoke_color = QColor(0, 0, 0, 0)
        self._effect_mode = "recover"
        self._smoke_timer = QTimer(self)
        self._smoke_timer.timeout.connect(self._advance_smoke)
        font = self.font()
        font.setPointSize(16)
        font.setBold(True)
        self.setFont(font)
        self.setFixedSize(460, self._logo_height + self._logo_top)

    def load_svg_data(self, svg_data: str) -> bool:
        svg_clean = svg_data.replace('width="100%"', '').replace('height="100%"', '')
        self._renderer.load(QByteArray(svg_clean.encode('utf-8')))
        if not self._renderer.isValid():
            self.update()
            return False
        view_box = self._renderer.viewBoxF()
        ratio = view_box.width() / view_box.height() if view_box.height() > 0 else 6.57
        width = max(int(round(self._logo_height * ratio)), 1)
        self.setFixedSize(width, self._logo_height + self._logo_top)
        self.update()
        return True

    def start_recovery_effect(self, color: QColor) -> None:
        self._start_effect(color, "recover")

    def start_restore_effect(self, color: QColor) -> None:
        self._start_effect(color, "restore")

    def _start_effect(self, color: QColor, mode: str) -> None:
        if mode not in {"recover", "restore"}:
            raise ValueError(f"Unknown logo effect mode: {mode}")
        self._smoke_color = QColor(color.red(), color.green(), color.blue(), color.alpha())
        self._effect_mode = mode
        self._smoke_active = True
        if self._smoke_opacity <= 0.0:
            self._smoke_progress = 0.0
            self._recovery_sweep_progress = 0.0
        if not self._smoke_timer.isActive():
            self._smoke_timer.start(50)
        self.update()

    def stop_recovery_effect(self) -> None:
        self._smoke_active = False
        self.update()

    def _advance_smoke(self) -> None:
        self._smoke_progress = (self._smoke_progress + 0.02) % 1.0
        self._recovery_sweep_progress = (self._recovery_sweep_progress + 0.03) % 1.0
        if self._smoke_active:
            self._smoke_opacity = min(1.0, self._smoke_opacity + 0.08)
        else:
            self._smoke_opacity = max(0.0, self._smoke_opacity - 0.08)
            if self._smoke_opacity <= 0.0:
                self._smoke_timer.stop()
                self._smoke_progress = 0.0
                self._recovery_sweep_progress = 0.0
        self.update()

    def _paint_smoke(self, painter: QPainter, logo_rect: QRectF) -> None:
        if self._effect_mode == "restore":
            self._paint_restore_smoke(painter, logo_rect)
            return
        self._paint_recover_smoke(painter)

    def _paint_recover_smoke(self, painter: QPainter) -> None:
        if self._smoke_opacity <= 0.0:
            return
        particles = (
            (0.34, -0.03, 7.0, 0.00),
            (0.45, 0.02, 9.0, 0.25),
            (0.57, -0.02, 8.0, 0.50),
            (0.68, 0.03, 6.5, 0.75),
        )
        painter.setPen(QColor(0, 0, 0, 0))
        for x_ratio, drift, base_radius, phase_shift in particles:
            phase = (self._smoke_progress + phase_shift) % 1.0
            alpha = int(self._smoke_color.alpha() * 0.16 * self._smoke_opacity * (1.0 - phase) * (1.0 - phase))
            if alpha <= 0:
                continue
            color = QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), alpha)
            radius = base_radius + phase * 10.0
            center_x = self.width() * x_ratio + self.width() * drift * phase
            center_y = self._logo_top + 12.0 - phase * 32.0
            painter.setBrush(color)
            painter.drawEllipse(QRectF(center_x - radius, center_y - radius, radius * 2.0, radius * 1.6))

    def _paint_restore_smoke(self, painter: QPainter, logo_rect: QRectF) -> None:
        if self._smoke_opacity <= 0.0:
            return
        particles = (
            (0.16, 0.40, -20.0, 6.5, 0.00),
            (0.84, 0.60, -22.0, 8.0, 0.18),
            (0.28, 0.47, -16.0, 7.0, 0.38),
            (0.72, 0.53, -18.0, 7.5, 0.58),
            (0.50, 0.50, -26.0, 9.0, 0.80),
        )
        target_y = logo_rect.top() + logo_rect.height() * 0.42
        painter.setPen(QColor(0, 0, 0, 0))
        for start_x_ratio, end_x_ratio, start_y_offset, base_radius, phase_shift in particles:
            phase = (self._smoke_progress + phase_shift) % 1.0
            alpha = int(self._smoke_color.alpha() * 0.18 * self._smoke_opacity * (0.35 + 0.65 * phase))
            if alpha <= 0:
                continue
            color = QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), alpha)
            radius = base_radius + (1.0 - phase) * 8.0
            start_x = self.width() * start_x_ratio
            end_x = self.width() * end_x_ratio
            center_x = start_x + (end_x - start_x) * phase
            start_y = self._logo_top + start_y_offset
            center_y = start_y + (target_y - start_y) * phase
            painter.setBrush(color)
            painter.drawEllipse(QRectF(center_x - radius, center_y - radius, radius * 2.0, radius * 1.55))

    def _paint_recovery_sweep(self, painter: QPainter, logo_rect: QRectF) -> None:
        if self._smoke_opacity <= 0.0:
            return
        band_width = max(logo_rect.width() * 0.18, 42.0)
        travel_width = logo_rect.width() + band_width * 2.0
        center_x = logo_rect.left() - band_width + travel_width * self._recovery_sweep_progress
        alpha = int(self._smoke_color.alpha() * 0.14 * self._smoke_opacity)
        if alpha <= 0:
            return
        gradient = QLinearGradient(center_x - band_width / 2.0, 0.0, center_x + band_width / 2.0, 0.0)
        gradient.setColorAt(0.0, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), 0))
        gradient.setColorAt(0.4, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), int(alpha * 0.45)))
        gradient.setColorAt(0.5, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), alpha))
        gradient.setColorAt(0.6, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), int(alpha * 0.45)))
        gradient.setColorAt(1.0, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), 0))
        painter.save()
        painter.setClipRect(logo_rect)
        painter.setCompositionMode(QtCompat.COMPOSITION_SCREEN)
        painter.fillRect(QRectF(center_x - band_width / 2.0, logo_rect.top(), band_width, logo_rect.height()), gradient)
        painter.restore()

    def _paint_restore_sweep(self, painter: QPainter, logo_rect: QRectF) -> None:
        if self._smoke_opacity <= 0.0:
            return
        band_width = max(logo_rect.width() * 0.14, 34.0)
        travel_width = logo_rect.width() + band_width * 2.0
        center_x = logo_rect.right() + band_width - travel_width * self._recovery_sweep_progress
        alpha = int(self._smoke_color.alpha() * 0.18 * self._smoke_opacity)
        if alpha <= 0:
            return
        gradient = QLinearGradient(center_x + band_width / 2.0, 0.0, center_x - band_width / 2.0, 0.0)
        gradient.setColorAt(0.0, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), 0))
        gradient.setColorAt(0.4, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), int(alpha * 0.45)))
        gradient.setColorAt(0.5, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), alpha))
        gradient.setColorAt(0.6, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), int(alpha * 0.45)))
        gradient.setColorAt(1.0, QColor(self._smoke_color.red(), self._smoke_color.green(), self._smoke_color.blue(), 0))
        painter.save()
        painter.setClipRect(logo_rect)
        painter.setCompositionMode(QtCompat.COMPOSITION_SCREEN)
        painter.fillRect(QRectF(center_x - band_width / 2.0, logo_rect.top(), band_width, logo_rect.height()), gradient)
        painter.restore()

    def paintEvent(self, _event):
        painter = QPainter(self)
        painter.setRenderHint(QtCompat.ANTIALIAS, True)
        painter.setRenderHint(QtCompat.SMOOTH_PIXMAP, True)
        logo_rect = QRectF(0, self._logo_top, self.width(), self._logo_height)
        self._paint_smoke(painter, logo_rect)
        if self._renderer.isValid():
            self._renderer.render(painter, logo_rect)
        else:
            painter.setPen(self.palette().windowText().color())
            painter.drawText(logo_rect.toRect(), QtCompat.ALIGN_CENTER, self._fallback_text)
        if self._effect_mode == "restore":
            self._paint_restore_sweep(painter, logo_rect)
            return
        self._paint_recovery_sweep(painter, logo_rect)
