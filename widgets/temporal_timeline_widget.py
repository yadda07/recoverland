"""Custom painted temporal timeline widget — replaces QSlider + _HistoryMarkers.

Design principles:
- Pure QWidget with paintEvent: no style-sheet artefacts, DPI-aware.
- Markers colored by operation type (INSERT=green, UPDATE=orange, DELETE=red).
- Adaptive ruler: shows hours / days / months / years based on total range.
- Magnetic snap: dragging within 10px of a marker snaps to it.
- Single signal: date_changed(str) emits ISO 8601 datetime on user interaction.
- Zero QGIS dependency: usable in tests without iface.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from qgis.PyQt.QtCore import QPoint, QRect, QSize, Qt, pyqtSignal
from qgis.PyQt.QtGui import QColor, QFont, QPainter, QPen, QPolygon
from qgis.PyQt.QtWidgets import QToolTip, QWidget

from ..compat import QtCompat
from ..core.logger import flog

_TRACK_H = 4
_HANDLE_R = 7
_MARKER_H = 8
_MARKER_W = 6
_RULER_H = 14
_PADDING_X = _HANDLE_R + 2
_SNAP_PX = 10
_HOVER_THRESHOLD_PX = 10

_COLOR_TRACK_DONE = QColor("#3a91ff")
_COLOR_TRACK_TODO = QColor("#505050")
_COLOR_HANDLE = QColor("#4a90d9")
_COLOR_HANDLE_HOVER = QColor("#80c8ff")
_COLOR_RULER = QColor("#888888")
_COLOR_OP = {
    "INSERT": QColor("#2ecc71"),
    "UPDATE": QColor("#f39c12"),
    "DELETE": QColor("#e74c3c"),
    "default": QColor("#f0c040"),
}


class TemporalTimelineWidget(QWidget):
    """Custom painted timeline replacing QSlider + _HistoryMarkers overlay.

    Public API (mirrors the old QSlider + _HistoryMarkers combo):
        set_range(first_iso, last_iso)   — configure time axis
        set_markers(date_op_pairs)       — list of (iso_str, op_type_str)
        current_date_iso() -> str        — current ISO 8601 datetime
        set_value_iso(iso)               — programmatically move handle

    Signal:
        date_changed(str)  — emitted with ISO when user drags / snaps
    """

    date_changed = pyqtSignal(str)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setMinimumHeight(28)

        self._base_date: Optional[date] = None
        self._end_date: Optional[date] = None
        self._total_days: int = 1
        self._current_date: Optional[date] = None
        self._current_time: Tuple[int, int, int] = (0, 0, 0)

        self._markers: List[Tuple[float, str, str]] = []

        self._dragging = False
        self._hover_pos: Optional[int] = None
        self._hover_iso: Optional[str] = None

    def set_range(self, first_iso: str, last_iso: str) -> None:
        try:
            self._base_date = _parse_iso_date(first_iso)
            self._end_date = _parse_iso_date(last_iso)
            self._total_days = max(1, (self._end_date - self._base_date).days)
        except (ValueError, TypeError) as exc:
            flog(f"timeline: set_range error {exc!r}", "WARNING")
            return
        if self._current_date is None:
            self._current_date = self._end_date
        self.update()
        flog(
            f"timeline: range_set first={first_iso} last={last_iso} "
            f"total_days={self._total_days}",
            "DEBUG",
        )

    def set_markers(self, date_op_pairs: list) -> None:
        """Accept list of (iso_str, op_type) or plain iso_str."""
        if self._base_date is None:
            self._markers = []
            return
        markers = []
        for item in date_op_pairs:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                iso, op = str(item[0]), str(item[1]).upper()
            else:
                iso, op = str(item), "default"
            try:
                d = _parse_iso_date(iso)
                frac = max(0.0, min(1.0, (d - self._base_date).days / self._total_days))
                markers.append((frac, iso, op))
            except (ValueError, TypeError):
                pass
        self._markers = markers
        self.update()
        flog(f"timeline: markers_set n={len(markers)}", "DEBUG")

    def current_date_iso(self) -> str:
        if self._current_date is None:
            return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        h, m, s = self._current_time
        return (
            f"{self._current_date.year:04d}-{self._current_date.month:02d}"
            f"-{self._current_date.day:02d}T{h:02d}:{m:02d}:{s:02d}"
        )

    def set_value_iso(self, iso: str) -> None:
        try:
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
            self._current_date = dt.date()
            self._current_time = (dt.hour, dt.minute, dt.second)
        except (ValueError, TypeError):
            return
        self.update()

    def sizeHint(self) -> QSize:
        return QSize(300, 28)

    def paintEvent(self, _event) -> None:
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        try:
            self._paint(p)
        finally:
            p.end()

    def _track_rect(self) -> Tuple[int, int, int, int]:
        cy = self.height() // 2
        x0 = _PADDING_X
        x1 = self.width() - _PADDING_X
        return x0, cy - _TRACK_H // 2, x1 - x0, _TRACK_H

    def _handle_x(self) -> int:
        if self._base_date is None or self._current_date is None:
            return self.width() - _PADDING_X
        frac = max(0.0, min(1.0, (self._current_date - self._base_date).days / self._total_days))
        x0, _, w, _ = self._track_rect()
        return int(x0 + frac * w)

    def _frac_to_x(self, frac: float) -> int:
        x0, _, w, _ = self._track_rect()
        return int(x0 + frac * w)

    def _x_to_frac(self, x: int) -> float:
        x0, _, w, _ = self._track_rect()
        return max(0.0, min(1.0, (x - x0) / max(1, w)))

    def _paint(self, p: QPainter) -> None:
        x0, ty, tw, th = self._track_rect()
        cy = self.height() // 2
        hx = self._handle_x()

        p.setPen(QtCompat.NO_PEN)

        p.setBrush(_COLOR_TRACK_DONE)
        p.drawRect(x0, ty, hx - x0, th)

        p.setBrush(_COLOR_TRACK_TODO)
        p.drawRect(hx, ty, x0 + tw - hx, th)

        self._paint_markers(p, cy)

        handle_color = _COLOR_HANDLE_HOVER if self._hover_pos is not None and abs(self._hover_pos - hx) < _HANDLE_R + 4 else _COLOR_HANDLE
        p.setBrush(handle_color)
        p.setPen(QPen(QColor("#2a6aad"), 1))
        p.drawEllipse(QPoint(hx, cy), _HANDLE_R, _HANDLE_R)

        self._paint_ruler(p, x0, tw, cy)

    def _paint_markers(self, p: QPainter, cy: int) -> None:
        for frac, _iso, op in self._markers:
            mx = self._frac_to_x(frac)
            color = _COLOR_OP.get(op, _COLOR_OP["default"])
            p.setBrush(color)
            p.setPen(QtCompat.NO_PEN)
            tip_y = cy - _HANDLE_R - 2
            pts = [
                QPoint(mx, tip_y),
                QPoint(mx - _MARKER_W // 2, tip_y - _MARKER_H),
                QPoint(mx + _MARKER_W // 2, tip_y - _MARKER_H),
            ]
            p.drawPolygon(QPolygon(pts))

    def _paint_ruler(self, p: QPainter, x0: int, tw: int, cy: int) -> None:
        if self._base_date is None or self._total_days < 1:
            return
        p.setPen(QPen(_COLOR_RULER, 1))
        font = QFont()
        font.setPointSize(7)
        p.setFont(font)

        steps, fmt = _ruler_steps(self._total_days)
        base = self._base_date
        end = self._end_date or (base + timedelta(days=self._total_days))
        cur = base
        while cur <= end:
            frac = (cur - base).days / self._total_days
            lx = int(x0 + frac * tw)
            p.drawLine(lx, cy + _HANDLE_R + 1, lx, cy + _HANDLE_R + 4)
            label = cur.strftime(fmt)
            p.drawText(QRect(lx - 20, cy + _HANDLE_R + 4, 40, _RULER_H), 0x0004, label)
            try:
                cur = _add_step(cur, steps)
            except (OverflowError, ValueError):
                break

    def mousePressEvent(self, event) -> None:
        if event.button() == QtCompat.LEFT_BUTTON:
            self._dragging = True
            self._move_to_x(event.pos().x(), emit=False)
            snapped = self._try_snap(event.pos().x())
            if not snapped:
                self.date_changed.emit(self.current_date_iso())
            flog(f"timeline: press x={event.pos().x()} snapped={snapped}", "DEBUG")

    def mouseMoveEvent(self, event) -> None:
        x = event.pos().x()
        self._hover_pos = x
        if self._dragging:
            snapped = self._try_snap(x)
            if not snapped:
                self._move_to_x(x, emit=True)
        else:
            iso = self._marker_at_x(x)
            if iso != self._hover_iso:
                self._hover_iso = iso
                if iso:
                    QToolTip.showText(
                        self.mapToGlobal(event.pos()),
                        _format_tooltip(iso),
                        self,
                    )
                    flog(f"timeline: marker_hover x={x} iso={iso}", "DEBUG")
        self.update()

    def mouseReleaseEvent(self, _event) -> None:
        if self._dragging:
            self._dragging = False
            self.date_changed.emit(self.current_date_iso())
            flog(f"timeline: release iso={self.current_date_iso()}", "DEBUG")

    def mouseDoubleClickEvent(self, event) -> None:
        iso = self._marker_at_x(event.pos().x())
        if iso:
            self.set_value_iso(iso)
            self.date_changed.emit(self.current_date_iso())
            flog(f"timeline: dblclick_snap iso={iso}", "DEBUG")

    def leaveEvent(self, _event) -> None:
        self._hover_pos = None
        self._hover_iso = None
        self.update()

    def _move_to_x(self, x: int, emit: bool) -> None:
        if self._base_date is None:
            return
        frac = self._x_to_frac(x)
        offset = int(round(frac * self._total_days))
        new_date = min(self._base_date + timedelta(days=offset), date.today())
        self._current_date = new_date
        self.update()
        if emit:
            self.date_changed.emit(self.current_date_iso())

    def _try_snap(self, x: int) -> bool:
        iso = self._marker_at_x(x)
        if not iso:
            return False
        self.set_value_iso(iso)
        self.update()
        self.date_changed.emit(self.current_date_iso())
        flog(f"timeline: marker_snapped x={x} iso={iso} magnetic=True", "DEBUG")
        return True

    def _marker_at_x(self, x: int) -> Optional[str]:
        best_dist = _HOVER_THRESHOLD_PX + 1
        best_iso: Optional[str] = None
        for frac, iso, _ in self._markers:
            mx = self._frac_to_x(frac)
            dist = abs(x - mx)
            if dist < best_dist:
                best_dist = dist
                best_iso = iso
        return best_iso


def _parse_iso_date(iso: str) -> date:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).date()


def _format_tooltip(iso: str) -> str:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except (ValueError, TypeError):
        return iso


def _ruler_steps(total_days: int) -> Tuple[str, str]:
    """Return (step_key, date_fmt) appropriate for the given range in days."""
    if total_days <= 14:
        return ("1d", "%d/%m")
    if total_days <= 90:
        return ("7d", "%d/%m")
    if total_days <= 730:
        return ("month", "%m/%Y")
    if total_days <= 3650:
        return ("year", "%Y")
    return ("5y", "%Y")


def _add_step(d: date, step: str) -> date:
    """Advance date by one ruler step. All steps operate on date objects only."""
    if step == "1d":
        return d + timedelta(days=1)
    if step == "7d":
        return d + timedelta(days=7)
    if step == "month":
        m = d.month % 12 + 1
        y = d.year + (1 if d.month == 12 else 0)
        return d.replace(year=y, month=m, day=1)
    if step == "year":
        return d.replace(year=d.year + 1, month=1, day=1)
    return d.replace(year=d.year + 5, month=1, day=1)


__all__ = ["TemporalTimelineWidget"]
