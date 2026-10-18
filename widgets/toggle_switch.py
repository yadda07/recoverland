"""Interrupteur a bascule anime pour RecoverLand (inspire des jetons VTT)."""
from qgis.PyQt.QtCore import QRectF, QVariantAnimation, pyqtSignal
from qgis.PyQt.QtGui import QColor, QPainter, QPen
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat
from .theme import MICRO_DUREE, blend, resolve_theme, should_animate


class AppleToggleSwitch(QWidget):
    """Animated toggle switch styled on VTT semantic tokens."""

    toggled = pyqtSignal(bool)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._checked = False
        self._anim_pos = 0.0  # 0.0 = OFF, 1.0 = ON
        self._anim = QVariantAnimation(self)
        self._anim.setDuration(MICRO_DUREE)
        self._anim.setEasingCurve(QtCompat.EASE_OUT_CUBIC)
        self._anim.valueChanged.connect(self._update_pos)
        self.setFixedSize(46, 26)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setToolTip(self.tr("Enregistrement des modifications : actif"))

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, value: bool, animated: bool = False):
        if self._checked == value:
            return
        self._checked = value
        target = 1.0 if value else 0.0
        if animated and should_animate():
            self._anim.stop()
            self._anim.setStartValue(float(self._anim_pos))
            self._anim.setEndValue(float(target))
            self._anim.start()
        else:
            self._anim_pos = target
            self.update()
        tip = self.tr("Enregistrement actif") if value else self.tr("Enregistrement desactive")
        self.setToolTip(tip)

    def _update_pos(self, pos):
        self._anim_pos = float(pos)
        self.update()

    def mousePressEvent(self, event):
        if not self.isEnabled():
            return
        self.setChecked(not self._checked, animated=True)
        self.toggled.emit(self._checked)

    def paintEvent(self, _event):
        t_tokens = resolve_theme(self)
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        w, h = self.width(), self.height()
        radius = h / 2.0
        ratio = self._anim_pos

        track_color = blend(t_tokens.surface_douce, t_tokens.acheve, ratio)
        border_color = blend(t_tokens.ligne_controle, t_tokens.acheve, ratio)
        p.setPen(QPen(border_color, 1))
        p.setBrush(track_color)
        p.drawRoundedRect(QRectF(0.5, 0.5, w - 1, h - 1), radius, radius)

        margin = 3.0
        knob_d = h - 2 * margin
        travel = w - 2 * margin - knob_d
        knob_x = margin + travel * ratio
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(QColor(0, 0, 0, 45))
        p.drawEllipse(QRectF(knob_x, margin + 1.0, knob_d, knob_d))

        p.setBrush(QColor("#ffffff"))
        p.drawEllipse(QRectF(knob_x, margin, knob_d, knob_d))
