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

from qgis.PyQt.QtCore import QDate, QTime, QTimer, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QDateEdit, QHBoxLayout, QLabel, QPushButton,
    QSizePolicy, QTimeEdit, QWidget,
)

from ..compat import QtCompat
from ..core.logger import flog
from ..core.time_format import parse_iso_date
from .temporal_timeline_widget import TemporalTimelineWidget

_BAR_HEIGHT = 46

_CANVAS_EVENT_TYPES = frozenset({int(QtCompat.EVENT_RESIZE), int(QtCompat.EVENT_MOVE)})


def _purge_stale_bars(canvas) -> None:
    """Destroy any orphan CanvasDateBar widgets left over from a previous load.

    Works across plugin reloads: identifies bars by the ``_rl_canvas_date_bar``
    marker attribute rather than isinstance(), which would fail after module reload.
    Checks both canvas children (current architecture) and top-level widgets
    (previous architecture) for backward compatibility.
    """
    try:
        from qgis.PyQt.QtWidgets import QApplication, QWidget as _QW  # noqa: PLC0415
        destroyed = 0
        seen = set()
        candidates = list(canvas.findChildren(_QW)) + list(QApplication.topLevelWidgets())
        for w in candidates:
            wid = id(w)
            if wid in seen:
                continue
            seen.add(wid)
            if not getattr(w, '_rl_canvas_date_bar', False):
                continue
            if getattr(w, '_closing', False):
                continue
            try:
                w._closing = True
                if hasattr(w, '_debounce'):
                    w._debounce.stop()
                if hasattr(w, '_viewport') and w._viewport is not None:
                    w._viewport.removeEventFilter(w)
                if hasattr(w, '_canvas'):
                    w._canvas.removeEventFilter(w)
                if hasattr(w, '_main_win') and w._main_win is not None:
                    w._main_win.removeEventFilter(w)
                w.hide()
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
    export_requested = pyqtSignal()

    def __init__(self, canvas, parent=None):
        _purge_stale_bars(canvas)
        viewport = canvas.viewport()    # canvas is a QGraphicsView/QAbstractScrollArea
        super().__init__(viewport)      # child of the viewport → actually rendered on top
        self._rl_canvas_date_bar = True  # marker for _purge_stale_bars
        self._canvas = canvas
        self._viewport = viewport
        self._ceiling = None
        self._closing = False
        self._base_date: Optional[date] = None
        self._end_date: Optional[date] = None
        self._total_days: int = 1
        self._syncing: bool = False

        self._debounce = QTimer(self)
        self._debounce.setSingleShot(True)
        self._debounce.setInterval(800)
        self._debounce.timeout.connect(self._emit_date_changed)

        self._reposition_timer = QTimer(self)
        self._reposition_timer.setSingleShot(True)
        self._reposition_timer.setInterval(200)
        self._reposition_timer.timeout.connect(self._reposition)

        self.setAttribute(QtCompat.WA_STYLED_BACKGROUND, True)
        self.setAutoFillBackground(True)
        self._build_ui()
        self._apply_style()
        viewport.installEventFilter(self)
        self._reposition()
        flog(
            f"canvas_date_bar: created canvas={canvas.__class__.__name__}",
            "DEBUG",
        )

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def set_range(
        self, first_iso: str, last_iso: str, preserve_cursor: bool = False
    ) -> None:
        """Configure slider range. first_iso / last_iso are ISO date strings.

        Args:
            preserve_cursor: when True, keep the current absolute date instead of
                jumping to today. Used when the range is refitted to markers so
                the user does not lose their position.
        """
        try:
            self._base_date = parse_iso_date(first_iso)
            self._end_date = parse_iso_date(last_iso)
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

        current_iso = self._timeline.current_date_iso() if preserve_cursor else None
        self._timeline.set_range(first_iso, last_iso)
        if preserve_cursor and current_iso:
            self._syncing = True
            try:
                self._timeline.set_value_iso(current_iso)
                self._on_timeline_date_changed(current_iso)
            finally:
                self._syncing = False
        else:
            self._go_today()
        flog(
            f"canvas_date_bar: range_set "
            f"first={first_iso} last={last_iso} total_days={self._total_days} "
            f"preserve_cursor={preserve_cursor}",
            "DEBUG",
        )

    def set_stats(self, n_entities: int, n_total: int = -1) -> None:
        """Update right-side status indicator after a successful reconstruction.

        Uses compact icons to avoid layout shifts:
        - ``∅``       — no data at this date
        - ``◎ N ↗``   — N entities outside viewport (orange)
        - ``✦ N``     — N entities visible (blue)

        Full description is available via tooltip.

        Args:
            n_entities: entities visible in the current viewport.
            n_total: entities globally at this date (-1 = unknown/same as n_entities).
        """
        if n_total < 0:
            n_total = n_entities
        if n_entities == 0 and n_total == 0:
            self._lbl_status.setText("∅")
            self._lbl_status.setToolTip(self.tr("Aucune entité à cette date"))
            self._lbl_status.setStyleSheet("color: #888888; font-size: 13px;")
        elif n_entities == 0 and n_total > 0:
            self._lbl_status.setText(f"◎ {n_total} ↗")
            self._lbl_status.setToolTip(
                self.tr("{n} entité(s) hors de l'emprise actuelle").format(n=n_total)
            )
            self._lbl_status.setStyleSheet("color: #ffaa44; font-size: 12px;")
        else:
            self._lbl_status.setText(f"✦ {n_entities}")
            self._lbl_status.setToolTip(
                self.tr("{n} entité(s) reconstituée(s)").format(n=n_entities)
            )
            self._lbl_status.setStyleSheet("color: #80c8ff; font-size: 12px;")

    def set_loading(self) -> None:
        """Show loading indicator while the rebuild worker is running."""
        self._lbl_status.setText("⟳")
        self._lbl_status.setToolTip(self.tr("Reconstruction en cours…"))
        self._lbl_status.setStyleSheet("color: #ffcc44; font-size: 14px;")

    def current_date_iso(self) -> str:
        """Return selected date+time as ISO 8601 string."""
        qd = self._date_edit.date()
        qt = self._time_edit.time()
        return (
            f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}"
            f"T{qt.hour():02d}:{qt.minute():02d}:{qt.second():02d}"
        )

    def set_value_iso(self, iso: str) -> None:
        """Move the handle to ``iso`` programmatically, without emitting.

        Used to snap the slider after the requested cutoff is clamped to the
        tracking-start baseline (RL-E1-03).
        """
        try:
            d = parse_iso_date(iso)
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return
        self._syncing = True
        try:
            self._date_edit.setDate(QDate(d.year, d.month, d.day))
            self._time_edit.setTime(QTime(dt.hour, dt.minute, dt.second))
            self._timeline.set_value_iso(iso)
        finally:
            self._syncing = False

    def cleanup(self) -> None:
        """Detach event filter and schedule widget deletion."""
        self._closing = True
        self._debounce.stop()
        self._reposition_timer.stop()
        self._viewport.removeEventFilter(self)
        self.hide()
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
        """Reposition after viewport resize/move; steal wheel over the timeline.

        QgsMapCanvas is a QGraphicsView that grabs wheel events at the viewport
        level to zoom the map, so the timeline's own wheelEvent rarely fires.
        We intercept wheel events landing on the timeline here and redirect them
        to its zoom, consuming the event so the map does not zoom underneath.
        """
        if obj is self._viewport:
            evt = int(event.type())
            if evt in _CANVAS_EVENT_TYPES:
                QTimer.singleShot(150, self._reposition)
            elif evt == int(QtCompat.EVENT_WHEEL) and self._maybe_zoom_timeline(event):
                return True
        return super().eventFilter(obj, event)

    def _maybe_zoom_timeline(self, event) -> bool:
        """Forward a viewport wheel event to the timeline when it is on top of it."""
        if self._closing or self._timeline is None:
            return False
        try:
            gp = event.globalPosition().toPoint()
        except (AttributeError, TypeError):
            gp = event.globalPos()
        local = self._timeline.mapFromGlobal(gp)
        over = self._timeline.rect().contains(local)
        flog(
            f"canvas_date_bar: viewport_wheel over_timeline={over} "
            f"local=({local.x()},{local.y()})",
            "DEBUG",
        )
        if not over:
            return False
        return self._timeline.handle_wheel(local.x(), event.angleDelta().y())

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

        self._time_edit = QTimeEdit(self)
        self._time_edit.setDisplayFormat("HH:mm")
        self._time_edit.setFixedWidth(58)
        layout.addWidget(self._time_edit)

        self._timeline = TemporalTimelineWidget(self)
        sp = QSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._timeline.setSizePolicy(sp)
        self._timeline.setFixedHeight(36)
        self._timeline.setToolTip(
            self.tr(
                "Molette : zoomer/dezoomer • Clic droit glisser : se deplacer • "
                "Double-clic : reinitialiser le zoom"
            )
        )
        layout.addWidget(self._timeline)
        self._timeline.date_changed.connect(self._on_timeline_date_changed)

        self._btn_today = QPushButton(self.tr("Aujourd'hui"), self)
        self._btn_today.setFixedWidth(90)
        layout.addWidget(self._btn_today)

        self._btn_export = QPushButton(self.tr("Export"), self)
        self._btn_export.setFixedWidth(60)
        self._btn_export.setToolTip(self.tr("Exporter le snapshot vers GeoPackage"))
        layout.addWidget(self._btn_export)

        self._lbl_status = QLabel("", self)
        self._lbl_status.setFixedWidth(72)
        layout.addWidget(self._lbl_status)

        self._date_edit.dateChanged.connect(self._on_date_edit_changed)
        self._time_edit.timeChanged.connect(self._on_time_changed)
        self._btn_today.clicked.connect(self._go_today)
        self._btn_export.clicked.connect(self.export_requested)

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
        )

    def _reposition(self) -> None:
        if self._closing:
            return
        cw = self._viewport.width()
        ch = self._viewport.height()
        self.resize(cw, _BAR_HEIGHT)
        self.move(0, ch - _BAR_HEIGHT)
        self.raise_()
        if not self.isVisible():
            self.show()
        flog(
            f"canvas_date_bar: reposition cw={cw} ch={ch} "
            f"visible={self.isVisible()} pos=({self.x()},{self.y()})",
            "DEBUG",
        )

    def set_ceiling(self, widget) -> None:
        """Set the widget that must always stay above this bar (e.g. the dialog)."""
        self._ceiling = widget
        flog(f"canvas_date_bar: ceiling_set widget={widget.__class__.__name__}", "DEBUG")

    def raise_safe(self) -> None:
        """Raise bar above canvas, then re-raise ceiling widget above bar.

        Guarantees z-order: canvas < bar < ceiling (dialog).
        Use this as slot for mapCanvasRefreshed instead of bare raise_().
        """
        self._raise_safe()

    def _raise_safe(self) -> None:
        self.raise_()

    def set_markers(self, date_isos: list, as_ticks: bool = False) -> None:
        """Pass markers to timeline. Accepts plain ISO strings or (iso, op_type) tuples.

        Args:
            as_ticks: when True, the same dates are also rendered as major ruler
                ticks (modification-centric mode). Default False preserves the
                calendar ladder for other consumers.
        """
        self._timeline.set_markers(date_isos)
        if as_ticks:
            tick_dates = [
                str(item[0]) if isinstance(item, (list, tuple)) else str(item)
                for item in date_isos or []
            ]
            self._timeline.set_marker_ticks(tick_dates)
            self._fit_range_to_marker_dates(tick_dates)
        else:
            self._timeline.set_marker_ticks(None)
        flog(
            f"canvas_date_bar: markers_set n={len(date_isos)} as_ticks={as_ticks}",
            "DEBUG",
        )

    def _fit_range_to_marker_dates(self, tick_dates: list[str]) -> None:
        """Set the visible date range to the marker span with padding.

        In Review mode the bar must always contain the modification events in
        the current viewport: start = first marker - padding, end = last marker
        + padding. The cursor stays at the same absolute date so the user does
        not lose control.
        """
        parsed: list[datetime] = []
        for raw in tick_dates:
            try:
                parsed.append(datetime.fromisoformat(raw.replace("Z", "+00:00")))
            except (ValueError, TypeError):
                pass
        if not parsed:
            return
        min_dt = min(parsed)
        max_dt = max(parsed)
        span = max(timedelta(seconds=1), max_dt - min_dt)
        padding = max(timedelta(hours=1), span * 0.02)
        start = (min_dt - padding).isoformat()
        end = (max_dt + padding).isoformat()
        self.set_range(start, end, preserve_cursor=True)
        flog(
            f"canvas_date_bar: range_fitted_to_markers "
            f"start={start} end={end} n={len(parsed)} "
            f"padding_hours={padding.total_seconds() / 3600:.2f}",
            "DEBUG",
        )

    def _on_timeline_date_changed(self, iso: str) -> None:
        """Sync QDateEdit + QTimeEdit when user drags / zooms the timeline."""
        if self._syncing:
            return
        self._syncing = True
        try:
            d = parse_iso_date(iso)
            self._date_edit.setDate(QDate(d.year, d.month, d.day))
            try:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                self._time_edit.setTime(QTime(dt.hour, dt.minute, dt.second))
            except (ValueError, TypeError):
                pass
        except (ValueError, TypeError):
            pass
        finally:
            self._syncing = False
        self._debounce.start()

    def _on_date_edit_changed(self, qd: QDate) -> None:
        if self._syncing or self._base_date is None:
            return
        self._syncing = True
        try:
            iso = f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}T00:00:00"
            self._timeline.set_value_iso(iso)
        finally:
            self._syncing = False
        self._debounce.start()

    def _go_today(self) -> None:
        today = date.today()
        now = datetime.now()
        qd = QDate(today.year, today.month, today.day)
        self._syncing = True
        try:
            self._date_edit.setDate(qd)
            self._time_edit.setTime(QTime(now.hour, now.minute, 0))
            # Reset the timeline view to the full range so the cursor at
            # "today" is guaranteed to be visible, instead of vanishing when
            # the user is zoomed elsewhere (e.g. Review mode on a past window).
            self._timeline.reset_view()
            self._timeline.set_value_iso(
                f"{today.year:04d}-{today.month:02d}-{today.day:02d}T00:00:00"
            )
        finally:
            self._syncing = False
        self._emit_date_changed()

    def _on_time_changed(self, _qt: QTime) -> None:
        if self._syncing:
            return
        self._debounce.start()

    def _emit_date_changed(self) -> None:
        iso = self.current_date_iso()
        flog(f"canvas_date_bar: date_changed iso={iso}", "DEBUG")
        self.date_changed.emit(iso)


__all__ = ["CanvasDateBar"]
