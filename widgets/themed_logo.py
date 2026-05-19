"""Themed SVG logo widget for RecoverLand.

Animation: a luminous line that pivots 45 deg per letter as it sweeps
across the letters of "RECOVERLAND". The animation is driven by a
QVariantAnimation, which is scheduled by Qt's global animation timer.
Compared to a Python QTimer, the global scheduler integrates elapsed
time, so short stalls of the UI thread (e.g. a slow chunk in the
restore runner) do not freeze the animation: the phase catches up as
soon as the event loop is free again, instead of dropping ticks.
"""
from qgis.PyQt.QtCore import QByteArray, QRectF, QVariantAnimation
from qgis.PyQt.QtGui import QPainter, QColor, QLinearGradient
from qgis.PyQt.QtSvg import QSvgRenderer
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat


class ThemedLogoWidget(QWidget):
    # "RECOVERLAND" -> 11 letters; one letter every ~500 ms (cycle ~5.5 s).
    _N_LETTERS = 11
    _CYCLE_DURATION_MS = 5500
    # Per-letter rotation increment (deg). 45 deg keeps an 8-step cycle so
    # the line completes 11*45 = 495 deg per cycle, never frozen on an axis.
    _ROT_PER_LETTER_DEG = 45.0
    # Fraction of one letter span used to fade-out on exit / fade-in on entry,
    # so the wrap from the last letter back to the first is visually seamless.
    _WRAP_FADE_ZONE = 0.18
    # Per-frame opacity step (fade in/out of the whole effect).
    _OPACITY_STEP = 0.05

    def __init__(self, parent=None):
        super().__init__(parent)
        self._renderer = QSvgRenderer(self)
        self._fallback_text = "RECOVERLAND"
        self._logo_height = 70
        self._logo_top = 12
        # Animation state (driven by self._anim).
        self._cycle_phase = 0.0  # in [0, 1), wraps every cycle
        self._smoke_opacity = 0.0  # global fade in/out of the effect
        self._smoke_active = False
        self._smoke_color = QColor(0, 0, 0, 0)
        self._effect_mode = "recover"
        # Single linear 0->1 animation that loops forever; the paint code
        # derives both the pivoting line phase and the ambient particles
        # phase from this single source of truth.
        self._anim = QVariantAnimation(self)
        self._anim.setStartValue(0.0)
        self._anim.setEndValue(1.0)
        self._anim.setDuration(self._CYCLE_DURATION_MS)
        self._anim.setLoopCount(-1)
        self._anim.valueChanged.connect(self._on_anim_value)
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
        if self._anim.state() != QtCompat.ANIM_STATE_RUNNING:
            # Fresh start (not a resume): rewind the cycle so the line
            # appears at the first letter rather than mid-sweep.
            if self._smoke_opacity <= 0.0:
                self._cycle_phase = 0.0
            self._anim.start()
        self.update()

    def stop_recovery_effect(self) -> None:
        self._smoke_active = False
        # The fade-out happens inside _on_anim_value; the animation
        # auto-stops once opacity reaches zero. Trigger an immediate
        # repaint so the fade starts even when the runner has just
        # finished and no further paint events are scheduled.
        self.update()

    def _on_anim_value(self, val) -> None:
        self._cycle_phase = float(val)
        if self._smoke_active:
            self._smoke_opacity = min(1.0, self._smoke_opacity + self._OPACITY_STEP)
        else:
            self._smoke_opacity = max(0.0, self._smoke_opacity - self._OPACITY_STEP)
            if (self._smoke_opacity <= 0.0
                    and self._anim.state() == QtCompat.ANIM_STATE_RUNNING):
                self._anim.stop()
                self._cycle_phase = 0.0
        self.update()

    @staticmethod
    def _ease_in_out_cubic(t: float) -> float:
        """Cubic ease used to make the line briefly settle on each letter."""
        if t < 0.5:
            return 4.0 * t * t * t
        f = 2.0 * t - 2.0
        return 0.5 * f * f * f + 1.0

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
        # 2 particle cycles per letter cycle: similar speed to the legacy
        # QTimer 50 ms / +0.02 increment (1 cycle every ~2.5 s).
        smoke_loop = (self._cycle_phase * 2.0) % 1.0
        painter.setPen(QColor(0, 0, 0, 0))
        for x_ratio, drift, base_radius, phase_shift in particles:
            phase = (smoke_loop + phase_shift) % 1.0
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
        # Same 2x loop multiplier as _paint_recover_smoke: keeps the
        # ambient particles at the legacy QTimer-driven pace independent
        # of the (slower) main letter-pivot cycle.
        smoke_loop = (self._cycle_phase * 2.0) % 1.0
        target_y = logo_rect.top() + logo_rect.height() * 0.42
        painter.setPen(QColor(0, 0, 0, 0))
        for start_x_ratio, end_x_ratio, start_y_offset, base_radius, phase_shift in particles:
            phase = (smoke_loop + phase_shift) % 1.0
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

    def _paint_pivoting_ribbon(self, painter: QPainter, logo_rect: QRectF) -> None:
        """Soft luminous ribbon that pivots ``_ROT_PER_LETTER_DEG`` per letter.

        Two stacked QLinearGradient rectangles, oriented along the ribbon
        axis and faded laterally, rendered in screen composition mode:
        - outer ribbon : wide band, theme color, soft alpha curve
        - inner core   : narrower band, white incandescent streak

        The ribbon slides from one letter center to the next with a cubic
        ease (it briefly "settles" on each letter) and the rotation is
        continuous, so the visual is a single ribbon caught in a slow
        spin while it travels across the logo. Wrap-around at the end of
        the cycle is hidden behind a short fade-out + fade-in.

        This is intentionally close to the legacy horizontal sweep look
        (transparent -> color -> transparent gradient passing through
        the letters) but oriented along an axis that rotates per letter.
        """
        if self._smoke_opacity <= 0.0:
            return
        n = self._N_LETTERS
        letter_phase = self._cycle_phase * n  # 0..n
        idx_int = int(letter_phase) % n
        sub_raw = letter_phase - int(letter_phase)
        sub_eased = self._ease_in_out_cubic(sub_raw)

        # Letter centers as ratios of the logo width. The N+1th target sits
        # virtually beyond the right edge so the ribbon sweeps out on the
        # last letter; the wrap fade-out + fade-in below hides the position jump.
        x_curr_ratio = (idx_int + 0.5) / n
        next_idx = (idx_int + 1) % n
        if next_idx == 0:  # wrapping from last to first letter
            x_next_ratio = x_curr_ratio + 1.0 / n
        else:
            x_next_ratio = (next_idx + 0.5) / n
        x_ratio = x_curr_ratio + (x_next_ratio - x_curr_ratio) * sub_eased
        pos_x = logo_rect.left() + x_ratio * logo_rect.width()
        pos_y = logo_rect.top() + logo_rect.height() * 0.5
        angle_deg = self._ROT_PER_LETTER_DEG * letter_phase

        # Smooth the wrap so the position jump from idx=n-1 back to idx=0 is
        # invisible: fade-out near the end of the last letter, fade-in near
        # the start of the first letter.
        edge_visibility = 1.0
        zone = self._WRAP_FADE_ZONE
        if idx_int == n - 1 and sub_raw > (1.0 - zone):
            edge_visibility = (1.0 - sub_raw) / zone
        elif idx_int == 0 and sub_raw < zone:
            edge_visibility = sub_raw / zone
        visibility = self._smoke_opacity * edge_visibility
        if visibility <= 0.0:
            return

        # Ribbon dimensions in the local frame (origin = letter center,
        # X axis = lateral gradient direction, Y axis = ribbon length).
        # The ribbon must be long enough to cross the letter height when
        # rotated 45-90 deg without exposing the rectangle ends. The
        # thickness is intentionally larger than a single letter zone so
        # the visual reads as a wide "band of light" sweeping across the
        # logo, not a thin slice.
        letter_zone_width = logo_rect.width() / n
        ribbon_thickness = max(letter_zone_width * 1.8, 64.0)
        ribbon_length = logo_rect.height() * 1.6

        painter.save()
        painter.setClipRect(logo_rect)
        painter.setCompositionMode(QtCompat.COMPOSITION_SCREEN)
        painter.translate(pos_x, pos_y)
        painter.rotate(angle_deg)
        painter.setPen(QColor(0, 0, 0, 0))

        r, g, b = (self._smoke_color.red(),
                   self._smoke_color.green(),
                   self._smoke_color.blue())
        base_alpha = self._smoke_color.alpha()

        # Outer ribbon: soft theme-colored band. Mirrors the legacy sweep
        # stop pattern (0 / 0.4 / 0.5 / 0.6 / 1.0) so the visual feels
        # familiar; the difference is that the band is now rotated. The
        # 0.13 multiplier keeps the ribbon discreet over the letters,
        # the eye reads it as a passing veil rather than an overlay.
        outer_alpha = int(base_alpha * 0.08 * visibility)
        if outer_alpha > 0:
            outer = QLinearGradient(
                -ribbon_thickness / 2.0, 0.0,
                ribbon_thickness / 2.0, 0.0,
            )
            outer.setColorAt(0.0, QColor(r, g, b, 0))
            outer.setColorAt(0.4, QColor(r, g, b, int(outer_alpha * 0.45)))
            outer.setColorAt(0.5, QColor(r, g, b, outer_alpha))
            outer.setColorAt(0.6, QColor(r, g, b, int(outer_alpha * 0.45)))
            outer.setColorAt(1.0, QColor(r, g, b, 0))
            painter.fillRect(
                QRectF(-ribbon_thickness / 2.0, -ribbon_length / 2.0,
                       ribbon_thickness, ribbon_length),
                outer,
            )

        # Inner ribbon: narrow white incandescent streak so the ribbon
        # reads as a band of light passing through the letters. The
        # 0.32 multiplier balances with the dimmer outer band: a soft
        # bright line, not a pure white flash.
        inner_thickness = ribbon_thickness * 0.32
        inner_alpha = int(255 * 0.18 * visibility)
        if inner_alpha > 0:
            inner = QLinearGradient(
                -inner_thickness / 2.0, 0.0,
                inner_thickness / 2.0, 0.0,
            )
            inner.setColorAt(0.0, QColor(255, 255, 255, 0))
            inner.setColorAt(0.5, QColor(255, 255, 255, inner_alpha))
            inner.setColorAt(1.0, QColor(255, 255, 255, 0))
            painter.fillRect(
                QRectF(-inner_thickness / 2.0, -ribbon_length / 2.0,
                       inner_thickness, ribbon_length),
                inner,
            )

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
        self._paint_pivoting_ribbon(painter, logo_rect)
