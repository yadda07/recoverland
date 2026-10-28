# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Time slider widget for temporal restore mode (style VTT).

Horizontal QSlider mapped to a datetime range, synchronized with a
QgsDateTimeEdit. Emits cutoffChanged(QDateTime) when the user moves
the slider or edits the date manually.

Qt5/Qt6 compatible via qgis.PyQt. Granularity adapts to range width:
- < 24 h  : 1 second
- < 30 d  : 1 minute
- >= 30 d : 1 hour
"""
from qgis.PyQt.QtCore import QDateTime, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QHBoxLayout, QLabel, QSlider, QVBoxLayout, QWidget,
)
from qgis.gui import QgsDateTimeEdit

from ..compat import QtCompat
from ..core.logger import flog
from .theme import resolve_theme

_SECS_1H = 3600
_SECS_24H = 86400
_SECS_30D = 2592000


def _compute_granularity(range_secs: int) -> int:
    """Return step size in seconds based on range width."""
    if range_secs <= _SECS_24H:
        return 1
    if range_secs <= _SECS_30D:
        return 60
    return _SECS_1H


class TimeSliderWidget(QWidget):
    """Horizontal time slider with synchronized date editor and VTT pastille counter."""

    cutoffChanged = pyqtSignal(QDateTime)

    def __init__(self, parent=None):
        super().__init__(parent)
        self._oldest_secs = 0
        self._newest_secs = 0
        self._granularity = 60
        self._syncing = False

        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(6)

        slider_row = QHBoxLayout()
        slider_row.setContentsMargins(0, 0, 0, 0)
        slider_row.setSpacing(8)

        self._oldest_label = QLabel("")
        self._newest_label = QLabel("")

        self._slider = QSlider(QtCompat.HORIZONTAL, self)
        self._slider.setTickPosition(QtCompat.TICK_BELOW)
        self._slider.setMinimum(0)
        self._slider.setMaximum(0)
        self._slider.setEnabled(False)

        slider_row.addWidget(self._oldest_label)
        slider_row.addWidget(self._slider, 1)
        slider_row.addWidget(self._newest_label)

        date_row = QHBoxLayout()
        date_row.setContentsMargins(0, 0, 0, 0)
        date_row.setSpacing(8)

        self._date_label = QLabel(self.tr("Revenir au :"))

        self._date_edit = QgsDateTimeEdit()
        self._date_edit.setAllowNull(False)
        self._date_edit.setDisplayFormat("dd/MM/yyyy HH:mm:ss")
        self._date_edit.setEnabled(False)

        self._count_label = QLabel("")
        self._count_label.setVisible(False)

        date_row.addWidget(self._date_label)
        date_row.addWidget(self._date_edit, 1)
        date_row.addWidget(self._count_label)

        root.addLayout(slider_row)
        root.addLayout(date_row)

        self._slider.valueChanged.connect(self._on_slider_changed)
        self._date_edit.dateTimeChanged.connect(self._on_date_edited)
        self._refresh_theme()

    def _refresh_theme(self) -> None:
        t = resolve_theme(self)
        bound_css = (
            f"font-family: '{t.police_titre}'; font-size: 11px; "
            f"font-weight: 600; color: {t.secondaire.name()};"
        )
        self._oldest_label.setStyleSheet(bound_css)
        self._newest_label.setStyleSheet(bound_css)
        self._date_label.setStyleSheet(
            f"font-family: '{t.police}'; font-weight: 600; color: {t.texte.name()};"
        )
        self._count_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 11px; font-weight: 700; "
            f"color: {t.accent.name()}; background-color: {t.accent_doux.name()}; "
            f"border: 1px solid {t.accent.name()}; border-radius: 10px; padding: 2px 8px;"
        )
        self._slider.setStyleSheet(
            f"QSlider::groove:horizontal {{"
            f"  height: 6px; background: {t.surface_douce.name()}; "
            f"  border: 1px solid {t.ligne.name()}; border-radius: 3px;"
            f"}}"
            f"QSlider::sub-page:horizontal {{"
            f"  background: {t.accent.name()}; border-radius: 3px;"
            f"}}"
            f"QSlider::handle:horizontal {{"
            f"  background: {t.surface.name()}; border: 2px solid {t.accent.name()}; "
            f"  width: 14px; height: 14px; margin: -5px 0; border-radius: 8px;"
            f"}}"
            f"QSlider::handle:horizontal:disabled {{"
            f"  background: {t.surface_douce.name()}; border: 1px solid {t.ligne.name()};"
            f"}}"
        )

    def set_bounds(self, oldest: QDateTime, newest: QDateTime,
                   initial: QDateTime = None) -> None:
        """Configure the slider range from oldest to newest datetime."""
        if not oldest.isValid() or not newest.isValid():
            flog("time_slider: invalid bounds, disabling", "WARNING")
            self.disable()
            return

        o_secs = int(oldest.toSecsSinceEpoch())
        n_secs = int(newest.toSecsSinceEpoch())

        if o_secs >= n_secs:
            flog("time_slider: oldest >= newest, swapping", "WARNING")
            o_secs, n_secs = n_secs, o_secs
            oldest, newest = newest, oldest

        range_secs = n_secs - o_secs
        self._granularity = _compute_granularity(range_secs)
        self._oldest_secs = o_secs
        self._newest_secs = n_secs

        steps = max(1, range_secs // self._granularity)

        init_step = 0
        init_dt = oldest
        if initial is not None and initial.isValid():
            i_secs = max(o_secs, min(int(initial.toSecsSinceEpoch()), n_secs))
            init_step = (i_secs - o_secs) // self._granularity
            init_dt = QDateTime.fromSecsSinceEpoch(
                o_secs + init_step * self._granularity)

        self._syncing = True
        self._slider.setMinimum(0)
        self._slider.setMaximum(steps)
        self._slider.setTickInterval(max(1, steps // 10))
        self._slider.setPageStep(max(1, steps // 20))
        self._slider.setValue(init_step)
        self._slider.setEnabled(True)

        self._date_edit.setMinimumDateTime(oldest)
        self._date_edit.setMaximumDateTime(newest)
        self._date_edit.setDateTime(init_dt)
        self._date_edit.setEnabled(True)

        self._oldest_label.setText(oldest.toString("dd/MM/yy HH:mm"))
        self._newest_label.setText(newest.toString("dd/MM/yy HH:mm"))
        self._syncing = False

        flog(
            f"time_slider: bounds set [{oldest.toString('yyyy-MM-ddTHH:mm:ss')}"
            f"..{newest.toString('yyyy-MM-ddTHH:mm:ss')}],"
            f" granularity={self._granularity}s, steps={steps},"
            f" initial_step={init_step}"
        )

    def set_event_count(self, count: int) -> None:
        """Display event count that would be affected by current cutoff."""
        if count < 0:
            self._count_label.setText("")
            self._count_label.setVisible(False)
            return
        self._count_label.setText(
            self.tr("{count} evenement(s)").format(count=count)
        )
        self._count_label.setVisible(True)

    def cutoff_datetime(self) -> QDateTime:
        """Return the currently selected cutoff datetime."""
        return self._date_edit.dateTime()

    def _on_slider_changed(self, value: int) -> None:
        if self._syncing:
            return
        self._syncing = True
        secs = min(self._oldest_secs + value * self._granularity, self._newest_secs)
        dt = QDateTime.fromSecsSinceEpoch(secs)
        self._date_edit.setDateTime(dt)
        self._syncing = False
        self.cutoffChanged.emit(dt)

    def _on_date_edited(self, dt: QDateTime) -> None:
        if self._syncing:
            return
        self._syncing = True
        secs = max(self._oldest_secs, min(int(dt.toSecsSinceEpoch()), self._newest_secs))
        step = (secs - self._oldest_secs) // self._granularity
        self._slider.setValue(step)
        self._syncing = False
        self.cutoffChanged.emit(dt)

    def disable(self) -> None:
        self._slider.setEnabled(False)
        self._slider.setMinimum(0)
        self._slider.setMaximum(0)
        self._date_edit.setEnabled(False)
        self._oldest_label.setText("")
        self._newest_label.setText("")
        self._count_label.setText("")
        self._count_label.setVisible(False)
        self.setToolTip(self.tr("Aucun evenement enregistre"))
