"""Canvas date bar widget — slim overlay anchored to bottom of QGIS map canvas.

Architecture:
    CanvasDateBar is a QWidget with parent=iface.mapCanvas().
    It overlays the canvas at y = canvas.height() - BAR_HEIGHT.
    Canvas resize events are caught via an installEventFilter on the canvas.
    Slider ↔ QDateEdit are kept in sync via a _syncing guard (no signal loop).

Public API::

    bar = CanvasDateBar(iface.mapCanvas())
    bar.set_range("2024-01-01", "2026-05-20")
    bar.date_changed.connect(my_slot)
    bar.show()
    # ...
    bar.set_stats(47)          # after reconstruction
    bar.set_loading()          # while worker runs
    bar.cleanup()              # before session stop
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

from qgis.PyQt.QtCore import QDate, QEvent, QPoint, Qt, QTimer, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QDateEdit, QHBoxLayout, QLabel, QPushButton,
    QSizePolicy, QSlider, QWidget,
)

from ..compat import QtCompat
from ..core.logger import flog

_BAR_HEIGHT = 40
_SLIDER_MAX = 10_000
_MARKER_H = 8
_MARKER_W = 5


try:
    _WA_MOUSE_TRANSPARENT = Qt.WidgetAttribute.WA_TransparentForMouseEvents
except AttributeError:
    _WA_MOUSE_TRANSPARENT = Qt.WA_TransparentForMouseEvents


class _HistoryMarkers(QWidget):
    """Transparent overlay that draws ▲ event-date markers on the slider groove.

    Positions are set as normalized floats in [0.0, 1.0].
    Completely mouse-transparent so the slider underneath still works.
    """

    def __init__(self, parent: QWidget):
        super().__init__(parent)
        self.setAttribute(_WA_MOUSE_TRANSPARENT, True)
        self._positions: list = []

    def set_positions(self, positions: list) -> None:
        self._positions = list(positions)
        self.update()

    def clear(self) -> None:
        self._positions = []
        self.update()

    def paintEvent(self, event) -> None:  # noqa: N802
        if not self._positions:
            return
        from qgis.PyQt.QtGui import QBrush, QColor, QPainter, QPolygon  # noqa: PLC0415
        try:
            aa = QPainter.RenderHint.Antialiasing
            no_pen = Qt.PenStyle.NoPen
        except AttributeError:
            aa = QPainter.Antialiasing
            no_pen = Qt.NoPen
        p = QPainter(self)
        p.setRenderHint(aa)
        p.setBrush(QBrush(QColor(255, 200, 60, 220)))
        p.setPen(no_pen)
        w = max(1, self.width())
        h = self.height()
        for pos in self._positions:
            x = int(pos * w)
            poly = QPolygon([
                QPoint(x - _MARKER_W, h),
                QPoint(x + _MARKER_W, h),
                QPoint(x, h - _MARKER_H),
            ])
            p.drawPolygon(poly)
        p.end()


def _qevent_resize_type() -> int:
    """Return QEvent.Resize integer value, Qt5/Qt6 compatible."""
    ns = getattr(QEvent, 'Type', None)
    if ns is not None:
        val = getattr(ns, 'Resize', None)
        if val is not None:
            return int(val)
    return int(getattr(QEvent, 'Resize', 14))


_RESIZE_EVENT_TYPE = _qevent_resize_type()


def _qevent_move_type() -> int:
    """Return QEvent.Move integer value, Qt5/Qt6 compatible."""
    ns = getattr(QEvent, 'Type', None)
    if ns is not None:
        val = getattr(ns, 'Move', None)
        if val is not None:
            return int(val)
    return int(getattr(QEvent, 'Move', 13))


_MOVE_EVENT_TYPE = _qevent_move_type()


def _qevent_layout_request_type() -> int:
    """Return QEvent.LayoutRequest integer value, Qt5/Qt6 compatible."""
    ns = getattr(QEvent, 'Type', None)
    if ns is not None:
        val = getattr(ns, 'LayoutRequest', None)
        if val is not None:
            return int(val)
    return int(getattr(QEvent, 'LayoutRequest', 76))


_LAYOUT_REQUEST_TYPE = _qevent_layout_request_type()
_CANVAS_EVENT_TYPES = frozenset({_RESIZE_EVENT_TYPE, _MOVE_EVENT_TYPE})
_WIN_EVENT_TYPES = frozenset({_RESIZE_EVENT_TYPE, _MOVE_EVENT_TYPE, _LAYOUT_REQUEST_TYPE})


def _purge_stale_bars() -> None:
    """Destroy any orphan CanvasDateBar widgets left over from a previous load.

    Works across plugin reloads: identifies bars by the ``_rl_canvas_date_bar``
    marker attribute rather than isinstance(), which would fail after module reload.
    """
    try:
        from qgis.PyQt.QtWidgets import QApplication  # noqa: PLC0415
        destroyed = 0
        for w in QApplication.topLevelWidgets():
            if not getattr(w, '_rl_canvas_date_bar', False):
                continue
            if getattr(w, '_closing', False):
                continue
            try:
                w._closing = True
                if hasattr(w, '_debounce'):
                    w._debounce.stop()
                if hasattr(w, '_canvas'):
                    w._canvas.removeEventFilter(w)
                if hasattr(w, '_main_win') and w._main_win is not None:
                    w._main_win.removeEventFilter(w)
                w.close()
                w.deleteLater()
                destroyed += 1
            except Exception:  # noqa: BLE001
                pass
        if destroyed:
            flog(f"canvas_date_bar: purged stale_bars n={destroyed}", "WARNING")
    except Exception:  # noqa: BLE001
        pass


class CanvasDateBar(QWidget):
    """Slim date bar anchored to the bottom of the QGIS map canvas.

    Signals
    -------
    date_changed : str
        ISO 8601 string (``yyyy-MM-ddT00:00:00``) emitted after 800 ms debounce.
    """

    date_changed = pyqtSignal(str)

    def __init__(self, canvas, parent=None):
        _purge_stale_bars()
        super().__init__(None)          # intentionally no Qt parent
        self._rl_canvas_date_bar = True  # marker for _purge_stale_bars
        self._canvas = canvas
        self._main_win = canvas.window()
        self._closing = False
        self._base_date: Optional[date] = None
        self._end_date: Optional[date] = None
        self._total_days: int = 1
        self._syncing: bool = False

        try:
            _flags = (
                Qt.WindowType.Tool
                | Qt.WindowType.FramelessWindowHint
                | Qt.WindowType.WindowDoesNotAcceptFocus
            )
        except AttributeError:
            _flags = (
                Qt.Tool
                | Qt.FramelessWindowHint
                | Qt.WindowDoesNotAcceptFocus
            )
        self.setWindowFlags(_flags)

        self._debounce = QTimer(self)
        self._debounce.setSingleShot(True)
        self._debounce.setInterval(800)
        self._debounce.timeout.connect(self._emit_date_changed)

        self._reposition_timer = QTimer(self)
        self._reposition_timer.setSingleShot(True)
        self._reposition_timer.setInterval(200)
        self._reposition_timer.timeout.connect(self._reposition)

        self._build_ui()
        self._apply_style()
        canvas.installEventFilter(self)
        if self._main_win is not None:
            self._main_win.installEventFilter(self)
        self._reposition()
        self._set_transient_parent()
        flog(
            f"canvas_date_bar: created canvas={canvas.__class__.__name__}",
            "DEBUG",
        )

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def set_range(self, first_iso: str, last_iso: str) -> None:
        """Configure slider range. first_iso / last_iso are ISO date strings."""
        try:
            self._base_date = _parse_iso_date(first_iso)
            self._end_date = _parse_iso_date(last_iso)
        except (ValueError, TypeError):
            self._base_date = date.today() - timedelta(days=365)
            self._end_date = date.today()

        self._total_days = max(1, (self._end_date - self._base_date).days)
        today = date.today()
        self._date_edit.setMinimumDate(
            QDate(self._base_date.year, self._base_date.month, self._base_date.day)
        )
        self._date_edit.setMaximumDate(
            QDate(today.year, today.month, today.day)
        )
        self._go_today()
        flog(
            f"canvas_date_bar: range_set "
            f"first={first_iso} last={last_iso} total_days={self._total_days}",
            "DEBUG",
        )

    def set_stats(self, n_entities: int) -> None:
        """Update right-side status label after a successful reconstruction."""
        if n_entities == 0:
            self._lbl_status.setText(self.tr("Aucune entité à cette date"))
            self._lbl_status.setStyleSheet("color: #aaaaaa; font-size: 11px;")
        else:
            txt = self.tr("{n} entité(s) reconstituée(s)").format(n=n_entities)
            self._lbl_status.setText(txt)
            self._lbl_status.setStyleSheet("color: #80c8ff; font-size: 11px;")

    def set_loading(self) -> None:
        """Show loading indicator while the rebuild worker is running."""
        self._lbl_status.setText(self.tr("Reconstruction…"))
        self._lbl_status.setStyleSheet("color: #ffcc44; font-size: 11px;")

    def current_date_iso(self) -> str:
        """Return selected date as ISO 8601 string (time part set to 00:00:00)."""
        qd = self._date_edit.date()
        return f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}T00:00:00"

    def cleanup(self) -> None:
        """Detach event filter and schedule widget deletion."""
        self._closing = True
        self._debounce.stop()
        self._reposition_timer.stop()
        self._canvas.removeEventFilter(self)
        if self._main_win is not None:
            self._main_win.removeEventFilter(self)
        self.close()
        self.deleteLater()
        flog("canvas_date_bar: cleanup done", "DEBUG")

    # ------------------------------------------------------------------ #
    # Qt overrides                                                         #
    # ------------------------------------------------------------------ #

    def showEvent(self, event) -> None:
        """Reposition and raise after Qt maps the widget to screen."""
        super().showEvent(event)
        flog("canvas_date_bar: showEvent", "DEBUG")
        self._reposition()

    def hideEvent(self, event) -> None:
        """Track why/when the bar becomes invisible."""
        import traceback as _tb
        super().hideEvent(event)
        stack = "".join(_tb.format_stack(limit=6))
        flog(f"canvas_date_bar: hideEvent stack={stack!r}", "WARNING")

    def closeEvent(self, event) -> None:
        """Ignore OS-level close — only cleanup() may close the bar."""
        if not self._closing:
            flog("canvas_date_bar: closeEvent ignored (not from cleanup)", "WARNING")
            event.ignore()
            return
        super().closeEvent(event)

    def eventFilter(self, obj, event) -> bool:
        """Reposition after any canvas resize/move or main-window layout change."""
        evt = int(event.type())
        if obj is self._canvas and evt in _CANVAS_EVENT_TYPES:
            QTimer.singleShot(150, self._reposition)
        elif obj is self._main_win and evt in _WIN_EVENT_TYPES:
            self._reposition_timer.start()  # coalesced at +200ms
        return super().eventFilter(obj, event)

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 2, 8, 2)
        layout.setSpacing(6)

        self._lbl_icon = QLabel("🕐", self)
        self._lbl_icon.setFixedWidth(22)
        layout.addWidget(self._lbl_icon)

        self._date_edit = QDateEdit(self)
        self._date_edit.setCalendarPopup(True)
        self._date_edit.setDisplayFormat("dd/MM/yyyy")
        self._date_edit.setFixedWidth(100)
        layout.addWidget(self._date_edit)

        self._slider = QSlider(QtCompat.HORIZONTAL, self)
        self._slider.setMinimum(0)
        self._slider.setMaximum(_SLIDER_MAX)
        sp = QSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._slider.setSizePolicy(sp)
        layout.addWidget(self._slider)

        self._marker_overlay = _HistoryMarkers(self)

        self._btn_today = QPushButton(self.tr("Aujourd'hui"), self)
        self._btn_today.setFixedWidth(90)
        layout.addWidget(self._btn_today)

        self._lbl_status = QLabel("", self)
        self._lbl_status.setMinimumWidth(210)
        layout.addWidget(self._lbl_status)

        self._slider.valueChanged.connect(self._on_slider_changed)
        self._date_edit.dateChanged.connect(self._on_date_edit_changed)
        self._btn_today.clicked.connect(self._go_today)

    def _apply_style(self) -> None:
        self.setStyleSheet(
            "CanvasDateBar {"
            "  background-color: rgba(28,28,28,215);"
            "  border-top: 1px solid rgba(85,85,85,180);"
            "}"
            "QLabel { color: #dcdcdc; font-size: 12px; }"
            "QPushButton {"
            "  color: #dcdcdc; background: rgba(55,55,55,200);"
            "  border: 1px solid #555; border-radius: 3px;"
            "  padding: 2px 6px; font-size: 11px;"
            "}"
            "QPushButton:hover { background: rgba(80,80,80,220); }"
            "QDateEdit {"
            "  color: #dcdcdc; background: rgba(45,45,45,200);"
            "  border: 1px solid #555; border-radius: 3px;"
            "  padding: 1px 4px; font-size: 12px;"
            "}"
            "QDateEdit::drop-down { border: none; width: 16px; }"
            "QSlider::groove:horizontal {"
            "  height: 4px; background: #505050; border-radius: 2px;"
            "}"
            "QSlider::handle:horizontal {"
            "  width: 14px; height: 14px; margin: -5px 0;"
            "  background: #4a90d9; border-radius: 7px;"
            "}"
            "QSlider::sub-page:horizontal {"
            "  background: #4a90d9; border-radius: 2px;"
            "}"
        )

    def _set_transient_parent(self) -> None:
        """Make bar stay on top of QGIS only (not all apps)."""
        try:
            if self._main_win is None:
                return
            self.show()  # windowHandle() only valid after show
            bar_win = self.windowHandle()
            main_win = self._main_win.windowHandle()
            if bar_win is not None and main_win is not None:
                bar_win.setTransientParent(main_win)
                flog("canvas_date_bar: transient_parent_set", "DEBUG")
        except Exception as _e:  # noqa: BLE001
            flog(f"canvas_date_bar: transient_parent_err {_e}", "WARNING")

    def _reposition(self) -> None:
        if self._closing:
            return
        cw = self._canvas.width()
        ch = self._canvas.height()
        try:
            g = self._canvas.mapToGlobal(QPoint(0, ch - _BAR_HEIGHT))
            self.resize(cw, _BAR_HEIGHT)
            self.move(g)
        except Exception as _e:
            flog(f"canvas_date_bar: reposition_err {_e}", "WARNING")
        self.raise_()
        if not self.isVisible():
            self.show()
        flog(
            f"canvas_date_bar: reposition cw={cw} ch={ch} "
            f"visible={self.isVisible()} pos=({self.x()},{self.y()})",
            "DEBUG",
        )
        QTimer.singleShot(0, self._reposition_markers)

    def set_markers(self, date_isos: list) -> None:
        """Draw ▲ markers at each date position. date_isos is a list of ISO strings."""
        if self._base_date is None or self._total_days < 1:
            self._marker_overlay.clear()
            return
        positions = []
        for iso in date_isos:
            try:
                d = _parse_iso_date(iso)
                offset = (d - self._base_date).days
                pos = max(0.0, min(1.0, offset / self._total_days))
                positions.append(pos)
            except (ValueError, TypeError, AttributeError):
                pass
        self._marker_overlay.set_positions(positions)
        flog(f"canvas_date_bar: markers_set n={len(positions)}", "DEBUG")

    def _reposition_markers(self) -> None:
        """Align the marker overlay to the slider's current screen geometry."""
        if not hasattr(self, '_marker_overlay'):
            return
        sr = self._slider.geometry()
        self._marker_overlay.setGeometry(sr.x(), sr.y(), sr.width(), sr.height())
        self._marker_overlay.raise_()

    def _on_slider_changed(self, value: int) -> None:
        if self._syncing or self._base_date is None:
            return
        self._syncing = True
        try:
            offset = int(round(value / _SLIDER_MAX * self._total_days))
            new_date = min(
                self._base_date + timedelta(days=offset),
                date.today(),
            )
            self._date_edit.setDate(
                QDate(new_date.year, new_date.month, new_date.day)
            )
        finally:
            self._syncing = False
        self._debounce.start()

    def _on_date_edit_changed(self, qd: QDate) -> None:
        if self._syncing or self._base_date is None:
            return
        self._syncing = True
        try:
            d = date(qd.year(), qd.month(), qd.day())
            offset = max(0, (d - self._base_date).days)
            value = int(round(offset / self._total_days * _SLIDER_MAX))
            self._slider.setValue(max(0, min(_SLIDER_MAX, value)))
        finally:
            self._syncing = False
        self._debounce.start()

    def _go_today(self) -> None:
        today = date.today()
        qd = QDate(today.year, today.month, today.day)
        self._syncing = True
        try:
            self._date_edit.setDate(qd)
            self._slider.setValue(_SLIDER_MAX)
        finally:
            self._syncing = False
        self._emit_date_changed()

    def _emit_date_changed(self) -> None:
        iso = self.current_date_iso()
        flog(f"canvas_date_bar: date_changed iso={iso}", "DEBUG")
        self.date_changed.emit(iso)


def _parse_iso_date(iso: str) -> date:
    """Parse ISO date/datetime string to a date object."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).date()


__all__ = ["CanvasDateBar"]
