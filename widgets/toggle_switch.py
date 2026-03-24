"""Apple-style animated toggle switch widget for RecoverLand."""
from qgis.PyQt.QtCore import QVariantAnimation, QRectF, pyqtSignal
from qgis.PyQt.QtGui import QPainter, QColor
from qgis.PyQt.QtWidgets import QWidget

from ..compat import QtCompat


class AppleToggleSwitch(QWidget):
    """Animated Apple-style on/off toggle switch."""

    toggled = pyqtSignal(bool)

    _COLOR_ON  = QColor(52, 199, 89)    # Apple system green
    _COLOR_OFF = QColor(142, 142, 147)  # Apple system gray

    def __init__(self, parent=None):
        super().__init__(parent)
        self._checked = False
        self._anim_pos = 0.0  # 0.0 = OFF, 1.0 = ON
        self._anim = QVariantAnimation(self)
        self._anim.setDuration(220)
        self._anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        self._anim.valueChanged.connect(self._update_pos)
        self.setFixedSize(50, 28)
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setToolTip("Enregistrement des modifications : actif")

    def isChecked(self) -> bool:
        return self._checked

    def setChecked(self, value: bool, animated: bool = False):
        if self._checked == value:
            return
        self._checked = value
        target = 1.0 if value else 0.0
        if animated:
            self._anim.stop()
            self._anim.setStartValue(float(self._anim_pos))
            self._anim.setEndValue(float(target))
            self._anim.start()
        else:
            self._anim_pos = target
            self.update()
        tip = "Enregistrement actif" if value else "Enregistrement desactive"
        self.setToolTip(tip)

    def _update_pos(self, pos):
        self._anim_pos = pos
        self.update()

    def mousePressEvent(self, event):
        self.setChecked(not self._checked, animated=True)
        self.toggled.emit(self._checked)

    def paintEvent(self, _event):
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        w, h = self.width(), self.height()
        radius = h / 2.0
        t = self._anim_pos

        r = int(self._COLOR_OFF.red()   + (self._COLOR_ON.red()   - self._COLOR_OFF.red())   * t)
        g = int(self._COLOR_OFF.green() + (self._COLOR_ON.green() - self._COLOR_OFF.green()) * t)
        b = int(self._COLOR_OFF.blue()  + (self._COLOR_ON.blue()  - self._COLOR_OFF.blue())  * t)
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(QColor(r, g, b))
        p.drawRoundedRect(QRectF(0, 0, w, h), radius, radius)

        margin = 3.0
        knob_d = h - 2 * margin
        travel = w - 2 * margin - knob_d
        knob_x = margin + travel * t
        p.setBrush(QColor(0, 0, 0, 35))
        p.drawEllipse(QRectF(knob_x, margin + 1.5, knob_d, knob_d))

        p.setBrush(QColor(255, 255, 255))
        p.drawEllipse(QRectF(knob_x, margin, knob_d, knob_d))
