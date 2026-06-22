"""Interactive temporal timeline widget — zoomable, calendar-aware ribbon.

Design principles:
- Pure QWidget with paintEvent: DPI-aware, no style-sheet artefacts.
- Datetime-precision internal model with a *view window* decoupled from the
  full data range, so the user can zoom from years down to minutes.
- Interactive zoom (mouse wheel, anchored on the cursor) and pan
  (right / middle button drag); double-click on empty space resets to full.
- Adaptive ruler: calendar-aligned major + minor ticks whose granularity
  refines automatically with the visible span and the available pixel width.
- Availability segments: periods with recorded activity are drawn as solid
  coloured bands; quiet / untracked periods are hatched, so coverage is
  legible at a glance.
- Markers coloured by operation type (INSERT=green, UPDATE=orange,
  DELETE=red), magnetic snapping on drag.
- Single signal: ``date_changed(str)`` emits an ISO 8601 datetime (with time)
  on user interaction.
- Zero QGIS dependency: usable in tests without iface.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from qgis.PyQt.QtCore import QPoint, QRect, QSize, pyqtSignal
from qgis.PyQt.QtGui import (
    QBrush, QColor, QFont, QFontMetrics, QLinearGradient, QPainter, QPen,
    QPolygon,
)
from qgis.PyQt.QtWidgets import QToolTip, QWidget

from ..compat import QtCompat
from ..core.logger import flog
from ..core.time_format import parse_iso_date

# ---- geometry -------------------------------------------------------------
_TRACK_H = 7
_HANDLE_R = 7
_MARKER_H = 7
_MARKER_W = 7
_RULER_H = 15
_PADDING_X = _HANDLE_R + 3
_SNAP_PX = 10
_HOVER_THRESHOLD_PX = 10
_MIN_SEG_PX = 3
_MIN_MAJOR_PX = 62
_MIN_MINOR_PX = 9

# Hard limits for the zoomable view window.
_MIN_SPAN_SEC = 3600          # 1 hour
_ZOOM_IN_FACTOR = 0.80
_ZOOM_OUT_FACTOR = 1.25

_EPOCH = datetime(1970, 1, 1)

# ---- palette --------------------------------------------------------------
_COLOR_TRACK_BASE = QColor("#26292e")     # tracked-but-quiet baseline
_COLOR_TRACK_HATCH = QColor("#3a3f47")    # "no data" texture
_COLOR_SEG_A = QColor("#2f7fd1")          # covered band gradient (top)
_COLOR_SEG_B = QColor("#3a91ff")          # covered band gradient (bottom)
_COLOR_SEG_EDGE = QColor(120, 190, 255, 90)
_COLOR_PAST_TINT = QColor(58, 145, 255, 38)
_COLOR_HANDLE = QColor("#eaf3ff")
_COLOR_HANDLE_RING = QColor("#3a91ff")
_COLOR_HANDLE_HOVER = QColor("#ffffff")
_COLOR_RULER_MAJOR = QColor("#9aa3ad")
_COLOR_RULER_MINOR = QColor(120, 128, 138, 150)
_COLOR_RULER_TEXT = QColor("#aab2bd")
_COLOR_CROSSHAIR = QColor(220, 230, 245, 130)
_COLOR_OOV = QColor("#80c8ff")            # out-of-view chevron
_COLOR_OP = {
    "INSERT": QColor("#2ecc71"),
    "UPDATE": QColor("#f39c12"),
    "DELETE": QColor("#e74c3c"),
    "default": QColor("#f0c040"),
}

# Calendar step ladder: (unit, n, approx_seconds, label_fmt).
_STEPS: List[Tuple[str, int, float, str]] = [
    ("minute", 1, 60, "%H:%M"),
    ("minute", 5, 300, "%H:%M"),
    ("minute", 15, 900, "%H:%M"),
    ("minute", 30, 1800, "%H:%M"),
    ("hour", 1, 3600, "%H:%M"),
    ("hour", 3, 10800, "%H:%M"),
    ("hour", 6, 21600, "%H:%M"),
    ("hour", 12, 43200, "%H:%M"),
    ("day", 1, 86400, "%d/%m"),
    ("day", 2, 172800, "%d/%m"),
    ("week", 1, 604800, "%d/%m"),
    ("month", 1, 2629800, "%b %y"),
    ("month", 3, 7889400, "%b %y"),
    ("year", 1, 31557600, "%Y"),
    ("year", 2, 63115200, "%Y"),
    ("year", 5, 157788000, "%Y"),
    ("year", 10, 315576000, "%Y"),
]


def _to_secs(dt: datetime) -> float:
    return (dt - _EPOCH).total_seconds()


def _parse_dt(iso: str) -> Optional[datetime]:
    """Parse an ISO string to a naive datetime (drops tz for stable epoch math)."""
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except (ValueError, TypeError):
        try:
            d = parse_iso_date(str(iso))
            return datetime(d.year, d.month, d.day)
        except (ValueError, TypeError):
            return None


class TemporalTimelineWidget(QWidget):
    """Interactive painted timeline (zoom + pan + availability segments).

    Public API (unchanged, mirrors the previous widget):
        set_range(first_iso, last_iso)   — configure time axis (full range)
        set_markers(date_op_pairs)       — list of (iso_str, op_type_str)
        current_date_iso() -> str        — current ISO 8601 datetime
        set_value_iso(iso)               — programmatically move handle

    Signal:
        date_changed(str)  — emitted with ISO datetime when user drags / snaps
    """

    date_changed = pyqtSignal(str)

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setMouseTracking(True)
        self.setMinimumHeight(34)

        # Full data range and the current (zoomable) visible window.
        self._full_start: Optional[datetime] = None
        self._full_end: Optional[datetime] = None
        self._view_start: Optional[datetime] = None
        self._view_end: Optional[datetime] = None

        self._current_dt: Optional[datetime] = None

        # (datetime, op_type)
        self._markers: List[Tuple[datetime, str]] = []
        # Activity clusters [(start, end), ...] derived from markers.
        self._segments: List[Tuple[datetime, datetime]] = []

        # Optional datetimes to render as major ruler ticks (modification-centric
        # mode). None means "fall back to calendar ladder".
        self._marker_ticks: Optional[List[datetime]] = None

        self._dragging = False
        self._panning = False
        self._pan_last_x: Optional[int] = None
        self._hover_pos: Optional[int] = None
        self._hover_iso: Optional[str] = None

    # ------------------------------------------------------------------ #
    # Public API                                                          #
    # ------------------------------------------------------------------ #

    def set_range(self, first_iso: str, last_iso: str) -> None:
        start = _parse_dt(first_iso)
        end = _parse_dt(last_iso)
        if start is None or end is None:
            flog(f"timeline: set_range error first={first_iso} last={last_iso}", "WARNING")
            return
        now = datetime.now()
        if end < start:
            start, end = end, start
        # Never let the axis run past "now" — future has no snapshots.
        if end > now:
            end = now
        if end <= start:
            end = start + timedelta(hours=1)
        self._full_start, self._full_end = start, end
        self._view_start, self._view_end = start, end
        if self._current_dt is None:
            self._current_dt = end
        self._rederive_segments()
        self.update()
        flog(
            f"timeline: range_set first={start.isoformat()} last={end.isoformat()}",
            "DEBUG",
        )

    def set_markers(self, date_op_pairs: list) -> None:
        """Accept list of (iso_str, op_type) or plain iso_str."""
        markers: List[Tuple[datetime, str]] = []
        for item in date_op_pairs or []:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                iso, op = str(item[0]), str(item[1]).upper()
            else:
                iso, op = str(item), "default"
            dt = _parse_dt(iso)
            if dt is not None:
                markers.append((dt, op))
        markers.sort(key=lambda m: m[0])
        self._markers = markers
        self._rederive_segments()
        self.update()
        flog(f"timeline: markers_set n={len(markers)}", "DEBUG")

    def set_marker_ticks(self, date_isos: Optional[list]) -> None:
        """Set datetimes that should be rendered as major ruler ticks.

        ``None`` or empty list disables marker-centric ticks and restores the
        default calendar ladder. Used by Review mode to make the date bar
        show the actual modification dates inside the current map extent.
        """
        ticks: List[datetime] = []
        for item in date_isos or []:
            if isinstance(item, (list, tuple)) and len(item) >= 1:
                iso = str(item[0])
            else:
                iso = str(item)
            dt = _parse_dt(iso)
            if dt is not None:
                ticks.append(dt)
        ticks.sort()
        self._marker_ticks = ticks if ticks else None
        self.update()
        flog(
            f"timeline: marker_ticks_set n={len(ticks)} enabled={self._marker_ticks is not None}",
            "DEBUG",
        )

    def current_date_iso(self) -> str:
        dt = self._current_dt or datetime.now()
        return dt.strftime("%Y-%m-%dT%H:%M:%S")

    def set_value_iso(self, iso: str) -> None:
        dt = _parse_dt(iso)
        if dt is None:
            return
        self._current_dt = dt
        self.update()

    def sizeHint(self) -> QSize:
        return QSize(320, 34)

    # ------------------------------------------------------------------ #
    # Coordinate mapping (datetime <-> pixels, within the view window)    #
    # ------------------------------------------------------------------ #

    def _track_rect(self) -> Tuple[int, int, int, int]:
        x0 = _PADDING_X
        x1 = self.width() - _PADDING_X
        ty = self._track_cy() - _TRACK_H // 2
        return x0, ty, max(1, x1 - x0), _TRACK_H

    def _track_cy(self) -> int:
        # Track sits in the upper area; the ruler occupies the bottom strip.
        usable = self.height() - _RULER_H
        return max(_HANDLE_R + _MARKER_H + 2, usable // 2 + 2)

    def _view_bounds_sec(self) -> Tuple[float, float]:
        vs = self._view_start or _EPOCH
        ve = self._view_end or (vs + timedelta(hours=1))
        a, b = _to_secs(vs), _to_secs(ve)
        if b <= a:
            b = a + 1.0
        return a, b

    def _dt_to_x(self, dt: datetime) -> float:
        a, b = self._view_bounds_sec()
        x0, _, w, _ = self._track_rect()
        frac = (_to_secs(dt) - a) / (b - a)
        return x0 + frac * w

    def _x_to_dt(self, x: int) -> datetime:
        a, b = self._view_bounds_sec()
        x0, _, w, _ = self._track_rect()
        frac = (x - x0) / max(1, w)
        frac = max(0.0, min(1.0, frac))
        secs = a + frac * (b - a)
        dt = _EPOCH + timedelta(seconds=secs)
        return self._clamp_full(dt)

    def _clamp_full(self, dt: datetime) -> datetime:
        if self._full_start and dt < self._full_start:
            return self._full_start
        if self._full_end and dt > self._full_end:
            return self._full_end
        return dt

    def _handle_x(self) -> float:
        if self._current_dt is None:
            x0, _, w, _ = self._track_rect()
            return x0 + w
        return self._dt_to_x(self._current_dt)

    # ------------------------------------------------------------------ #
    # Painting                                                            #
    # ------------------------------------------------------------------ #

    def paintEvent(self, _event) -> None:
        p = QPainter(self)
        p.setRenderHint(QtCompat.ANTIALIAS)
        try:
            self._paint(p)
        finally:
            p.end()

    def _paint(self, p: QPainter) -> None:
        x0, ty, tw, th = self._track_rect()
        cy = self._track_cy()
        radius = th / 2.0

        # 1. Track baseline (tracked-but-quiet).
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(_COLOR_TRACK_BASE)
        p.drawRoundedRect(QRect(x0, ty, tw, th), radius, radius)

        # 2. "No data" hatch over the whole track, clipped to the rounded rect.
        p.save()
        p.setClipRect(QRect(x0, ty, tw, th))
        hatch = QBrush(_COLOR_TRACK_HATCH, QtCompat.BRUSH_BDIAG)
        p.setBrush(hatch)
        p.drawRect(QRect(x0, ty, tw, th))
        p.restore()

        # 3. Availability segments (solid colour over the hatch).
        self._paint_segments(p, ty, th)

        # 4. Subtle "past" tint up to the handle.
        hx = self._handle_x()
        hx_c = max(x0, min(x0 + tw, hx))
        if hx_c > x0:
            p.save()
            p.setClipRect(QRect(x0, ty, tw, th))
            p.setPen(QtCompat.NO_PEN)
            p.setBrush(_COLOR_PAST_TINT)
            p.drawRect(QRect(x0, ty, int(hx_c - x0), th))
            p.restore()

        # 5. Op-type markers (visible window only).
        self._paint_markers(p, cy)

        # 6. Ruler (adaptive ticks).
        self._paint_ruler(p, x0, tw, cy)

        # 7. Hover crosshair + floating label.
        self._paint_crosshair(p, x0, tw, ty, th)

        # 8. Selection handle (or out-of-view chevron).
        self._paint_handle(p, x0, tw, cy, hx)

    def _paint_segments(self, p: QPainter, ty: int, th: int) -> None:
        if not self._segments:
            return
        x0, _, tw, _ = self._track_rect()
        radius = th / 2.0
        p.save()
        p.setClipRect(QRect(x0, ty, tw, th))
        for seg_start, seg_end in self._segments:
            xa = self._dt_to_x(seg_start)
            xb = self._dt_to_x(seg_end)
            if xb < x0 or xa > x0 + tw:
                continue
            xa = max(x0, xa)
            xb = min(x0 + tw, xb)
            w = max(_MIN_SEG_PX, xb - xa)
            grad = QLinearGradient(0, ty, 0, ty + th)
            grad.setColorAt(0.0, _COLOR_SEG_A)
            grad.setColorAt(1.0, _COLOR_SEG_B)
            p.setPen(QtCompat.NO_PEN)
            p.setBrush(QBrush(grad))
            rect = QRect(int(xa), ty, int(w), th)
            p.drawRoundedRect(rect, radius, radius)
            p.setPen(QPen(_COLOR_SEG_EDGE, 1))
            p.setBrush(QtCompat.NO_BRUSH)
            p.drawRoundedRect(rect, radius, radius)
        p.restore()

    def _paint_markers(self, p: QPainter, cy: int) -> None:
        x0, _, tw, _ = self._track_rect()
        tip_y = cy - _TRACK_H // 2 - 3
        for dt, op in self._markers:
            mx = self._dt_to_x(dt)
            if mx < x0 - 2 or mx > x0 + tw + 2:
                continue
            mx_i = int(round(mx))
            color = _COLOR_OP.get(op, _COLOR_OP["default"])
            p.setBrush(color)
            p.setPen(QtCompat.NO_PEN)
            pts = [
                QPoint(mx_i, tip_y),
                QPoint(mx_i - _MARKER_W // 2, tip_y - _MARKER_H),
                QPoint(mx_i + _MARKER_W // 2, tip_y - _MARKER_H),
            ]
            p.drawPolygon(QPolygon(pts))

    def _paint_ruler(self, p: QPainter, x0: int, tw: int, cy: int) -> None:
        if self._view_start is None or self._view_end is None:
            return
        major, minor = _select_steps(self._view_start, self._view_end, tw)
        top_y = cy + _TRACK_H // 2 + 2

        # Calendar ladder as minor ticks (short, unlabeled) for scale reference.
        if minor is not None:
            p.setPen(QPen(_COLOR_RULER_MINOR, 1))
            for tdt in _iter_ticks(self._view_start, self._view_end, minor):
                lx = self._dt_to_x(tdt)
                if lx < x0 or lx > x0 + tw:
                    continue
                p.drawLine(int(lx), top_y, int(lx), top_y + 3)

        font = QFont()
        font.setPointSize(7)
        p.setFont(font)
        fm = QFontMetrics(font)
        align = int(QtCompat.ALIGN_HCENTER) | int(QtCompat.ALIGN_TOP)
        last_label_right = -10000
        n_major_drawn = 0

        # Marker dates as major ticks (long, labeled) when in modification-centric
        # mode; otherwise fall back to the calendar ladder as major ticks.
        if self._marker_ticks:
            visible_ticks = self._deduplicate_marker_ticks([
                dt for dt in self._marker_ticks
                if self._view_start <= dt <= self._view_end
            ])
            for tdt in visible_ticks:
                lx = self._dt_to_x(tdt)
                if lx < x0 - 1 or lx > x0 + tw + 1:
                    continue
                label = _format_marker_tick_label(tdt, self._view_start, self._view_end)
                lw = fm.horizontalAdvance(label)
                lleft = int(lx) - lw // 2
                if lleft <= last_label_right + 4:
                    continue
                p.setPen(QPen(_COLOR_RULER_MAJOR, 1))
                p.drawLine(int(lx), top_y, int(lx), top_y + 5)
                p.setPen(QPen(_COLOR_RULER_TEXT, 1))
                p.drawText(
                    QRect(int(lx) - 40, top_y + 4, 80, _RULER_H - 4),
                    align, label,
                )
                last_label_right = lleft + lw
                n_major_drawn += 1
            flog(
                f"timeline: paint_ruler_marker_mode "
                f"total_ticks={len(self._marker_ticks)} "
                f"visible_ticks={len(visible_ticks)} "
                f"drawn={n_major_drawn} "
                f"view={self._view_start.isoformat()}..{self._view_end.isoformat()}",
                "DEBUG",
            )
        else:
            # Major ticks (long, labeled).
            for tdt in _iter_ticks(self._view_start, self._view_end, major):
                lx = self._dt_to_x(tdt)
                if lx < x0 - 1 or lx > x0 + tw + 1:
                    continue
                label = tdt.strftime(major[3])
                lw = fm.horizontalAdvance(label)
                lleft = int(lx) - lw // 2
                if lleft <= last_label_right + 4:
                    continue
                p.setPen(QPen(_COLOR_RULER_MAJOR, 1))
                p.drawLine(int(lx), top_y, int(lx), top_y + 5)
                p.setPen(QPen(_COLOR_RULER_TEXT, 1))
                p.drawText(
                    QRect(int(lx) - 40, top_y + 4, 80, _RULER_H - 4),
                    align, label,
                )
                last_label_right = lleft + lw
                n_major_drawn += 1
            flog(
                f"timeline: paint_ruler_calendar_mode "
                f"major={major[0]}-{major[1]} drawn={n_major_drawn}",
                "DEBUG",
            )

    def _paint_crosshair(self, p: QPainter, x0: int, tw: int, ty: int, th: int) -> None:
        if self._hover_pos is None or self._dragging or self._panning:
            return
        hx = self._hover_pos
        if hx < x0 or hx > x0 + tw:
            return
        p.setPen(QPen(_COLOR_CROSSHAIR, 1, QtCompat.DASH_LINE))
        p.drawLine(hx, 1, hx, ty + th + 1)

        dt = self._x_to_dt(hx)
        label = _format_dt_label(dt, self._view_start, self._view_end)
        font = QFont()
        font.setPointSize(7)
        p.setFont(font)
        fm = QFontMetrics(font)
        lw = fm.horizontalAdvance(label) + 8
        lh = fm.height() + 2
        bx = max(x0, min(x0 + tw - lw, hx - lw // 2))
        rect = QRect(int(bx), 0, int(lw), int(lh))
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(QColor(20, 24, 30, 220))
        p.drawRoundedRect(rect, 3, 3)
        p.setPen(QPen(QColor("#dfe6ef"), 1))
        p.drawText(rect, int(QtCompat.ALIGN_CENTER), label)

    def _paint_handle(self, p: QPainter, x0: int, tw: int, cy: int, hx: float) -> None:
        # Out-of-view: draw an edge chevron pointing toward the handle.
        if hx < x0:
            self._paint_chevron(p, x0 + 1, cy, left=True)
            return
        if hx > x0 + tw:
            self._paint_chevron(p, x0 + tw - 1, cy, left=False)
            return

        hx_i = int(round(hx))
        ty = cy - _TRACK_H // 2
        # Vertical stem through the track.
        p.setPen(QPen(_COLOR_HANDLE_RING, 2))
        p.drawLine(hx_i, ty - 4, hx_i, ty + _TRACK_H + 4)

        hovering = (self._hover_pos is not None and abs(self._hover_pos - hx_i) < _HANDLE_R + 4)
        fill = _COLOR_HANDLE_HOVER if hovering else _COLOR_HANDLE
        p.setBrush(fill)
        p.setPen(QPen(_COLOR_HANDLE_RING, 2))
        p.drawEllipse(QPoint(hx_i, cy), _HANDLE_R, _HANDLE_R)

    def _paint_chevron(self, p: QPainter, x: int, cy: int, left: bool) -> None:
        p.setPen(QtCompat.NO_PEN)
        p.setBrush(_COLOR_OOV)
        d = 5
        if left:
            pts = [QPoint(x, cy), QPoint(x + d, cy - d), QPoint(x + d, cy + d)]
        else:
            pts = [QPoint(x, cy), QPoint(x - d, cy - d), QPoint(x - d, cy + d)]
        p.drawPolygon(QPolygon(pts))

    # ------------------------------------------------------------------ #
    # Mouse / wheel interaction                                           #
    # ------------------------------------------------------------------ #

    def mousePressEvent(self, event) -> None:
        btn = event.button()
        if btn == QtCompat.LEFT_BUTTON:
            self._dragging = True
            x = event.pos().x()
            snapped = self._try_snap(x)
            if not snapped:
                self._move_to_x(x, emit=True)
            flog(f"timeline: press x={x} snapped={snapped}", "DEBUG")
        elif btn in (QtCompat.RIGHT_BUTTON, QtCompat.MIDDLE_BUTTON):
            self._panning = True
            self._pan_last_x = event.pos().x()
            self.setCursor(QtCompat.CLOSED_HAND_CURSOR)

    def mouseMoveEvent(self, event) -> None:
        x = event.pos().x()
        self._hover_pos = x
        if self._panning and self._pan_last_x is not None:
            self._pan_by(self._pan_last_x - x)
            self._pan_last_x = x
            self.update()
            return
        if self._dragging:
            if not self._try_snap(x):
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
        self.update()

    def mouseReleaseEvent(self, event) -> None:
        if self._dragging:
            self._dragging = False
            self.date_changed.emit(self.current_date_iso())
            flog(f"timeline: release iso={self.current_date_iso()}", "DEBUG")
        if self._panning:
            self._panning = False
            self._pan_last_x = None
            self.unsetCursor()

    def mouseDoubleClickEvent(self, event) -> None:
        x = event.pos().x()
        iso = self._marker_at_x(x)
        if iso:
            self.set_value_iso(iso)
            self.date_changed.emit(self.current_date_iso())
            flog(f"timeline: dblclick_snap iso={iso}", "DEBUG")
            return
        # Empty space: reset zoom to the full range.
        self._reset_view()
        flog("timeline: dblclick_reset_view", "DEBUG")

    def wheelEvent(self, event) -> None:
        # Note: when hosted over a QgsMapCanvas, wheel events are usually
        # intercepted by the canvas viewport (it zooms the map). CanvasDateBar
        # forwards those to handle_wheel(); this override only fires on the rare
        # platforms that deliver the wheel straight to the child widget.
        if self.handle_wheel(self._wheel_x(event), event.angleDelta().y()):
            event.accept()
        else:
            super().wheelEvent(event)

    def handle_wheel(self, x_local: int, delta: int) -> bool:
        """Zoom the visible window around ``x_local``. Returns True if handled.

        Public so the parent overlay can forward wheel events the map canvas
        would otherwise swallow.
        """
        if self._full_start is None or self._full_end is None or delta == 0:
            flog(
                f"timeline: wheel_ignored full_set={self._full_start is not None} "
                f"delta={delta}",
                "DEBUG",
            )
            return False
        before = (self._view_start, self._view_end)
        self._zoom_at(x_local, _ZOOM_IN_FACTOR if delta > 0 else _ZOOM_OUT_FACTOR)
        flog(
            f"timeline: wheel_zoom x={x_local} delta={delta} "
            f"view={self._view_start.isoformat()}..{self._view_end.isoformat()} "
            f"changed={before != (self._view_start, self._view_end)}",
            "DEBUG",
        )
        return True

    def leaveEvent(self, _event) -> None:
        self._hover_pos = None
        self._hover_iso = None
        self.update()

    # ------------------------------------------------------------------ #
    # Interaction helpers                                                 #
    # ------------------------------------------------------------------ #

    def _wheel_x(self, event) -> int:
        try:
            return int(event.position().x())
        except (AttributeError, TypeError):
            return event.x()

    def _move_to_x(self, x: int, emit: bool) -> None:
        if self._full_start is None:
            return
        self._current_dt = self._x_to_dt(x)
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
        x0, _, tw, _ = self._track_rect()
        best_dist = _HOVER_THRESHOLD_PX + 1
        best_iso: Optional[str] = None
        for dt, _op in self._markers:
            mx = self._dt_to_x(dt)
            if mx < x0 - 2 or mx > x0 + tw + 2:
                continue
            dist = abs(x - mx)
            if dist < best_dist:
                best_dist = dist
                best_iso = dt.strftime("%Y-%m-%dT%H:%M:%S")
        return best_iso

    def _zoom_at(self, x: int, factor: float) -> None:
        a, b = self._view_bounds_sec()
        x0, _, w, _ = self._track_rect()
        frac = max(0.0, min(1.0, (x - x0) / max(1, w)))
        anchor = a + frac * (b - a)

        full_a, full_b = _to_secs(self._full_start), _to_secs(self._full_end)
        full_span = max(_MIN_SPAN_SEC, full_b - full_a)
        span = (b - a) * factor
        span = max(_MIN_SPAN_SEC, min(full_span, span))

        new_a = anchor - frac * span
        new_b = new_a + span
        # Keep the window inside the full range.
        if new_a < full_a:
            new_a, new_b = full_a, full_a + span
        if new_b > full_b:
            new_b, new_a = full_b, full_b - span
        if new_a < full_a:
            new_a = full_a
        self._view_start = _EPOCH + timedelta(seconds=new_a)
        self._view_end = _EPOCH + timedelta(seconds=new_b)
        self.update()

    def _pan_by(self, dx_px: int) -> None:
        if dx_px == 0:
            return
        a, b = self._view_bounds_sec()
        _, _, w, _ = self._track_rect()
        span = b - a
        d_sec = dx_px * span / max(1, w)
        full_a, full_b = _to_secs(self._full_start), _to_secs(self._full_end)
        new_a = a + d_sec
        new_b = b + d_sec
        if new_a < full_a:
            new_a, new_b = full_a, full_a + span
        if new_b > full_b:
            new_b, new_a = full_b, full_b - span
        self._view_start = _EPOCH + timedelta(seconds=new_a)
        self._view_end = _EPOCH + timedelta(seconds=new_b)

    def _reset_view(self) -> None:
        if self._full_start and self._full_end:
            self._view_start, self._view_end = self._full_start, self._full_end
            self.update()

    def reset_view(self) -> None:
        """Public alias: reset the visible window to the full data range."""
        self._reset_view()
        flog(
            f"timeline: reset_view full={self._full_start}..{self._full_end} "
            f"view={self._view_start}..{self._view_end}",
            "DEBUG",
        )

    # ------------------------------------------------------------------ #
    # Segment derivation                                                  #
    # ------------------------------------------------------------------ #

    def _rederive_segments(self) -> None:
        self._segments = _cluster_markers(
            [dt for dt, _ in self._markers], self._full_start, self._full_end,
        )

    def _deduplicate_marker_ticks(self, ticks: List[datetime]) -> List[datetime]:
        """Return representative marker ticks without day duplication.

        When zoomed out (>3 days visible) keep one tick per day so labels do
        not overlap. When zoomed in keep the exact timestamps.
        """
        if not ticks:
            return []
        span = 0.0
        if self._view_start and self._view_end:
            span = max(0.0, _to_secs(self._view_end) - _to_secs(self._view_start))
        if 0 < span <= 3 * 86400:
            return ticks
        seen: set = set()
        result: List[datetime] = []
        for dt in ticks:
            key = (dt.year, dt.month, dt.day)
            if key not in seen:
                seen.add(key)
                result.append(dt)
        return result


# ---------------------------------------------------------------------- #
# Module-level helpers                                                    #
# ---------------------------------------------------------------------- #

def _cluster_markers(
    dts: List[datetime],
    full_start: Optional[datetime],
    full_end: Optional[datetime],
) -> List[Tuple[datetime, datetime]]:
    """Group event datetimes into activity segments [(start, end), ...].

    Consecutive events separated by a gap shorter than ~3% of the total span
    (min 1 hour) are merged into a single covered band.
    """
    if not dts:
        return []
    ordered = sorted(dts)
    if full_start and full_end:
        total = max(_MIN_SPAN_SEC, _to_secs(full_end) - _to_secs(full_start))
    else:
        total = max(_MIN_SPAN_SEC, _to_secs(ordered[-1]) - _to_secs(ordered[0]))
    gap_threshold = max(3600.0, total * 0.03)

    segments: List[Tuple[datetime, datetime]] = []
    seg_start = ordered[0]
    prev = ordered[0]
    for dt in ordered[1:]:
        if _to_secs(dt) - _to_secs(prev) > gap_threshold:
            segments.append((seg_start, prev))
            seg_start = dt
        prev = dt
    segments.append((seg_start, prev))
    return segments


def _select_steps(
    view_start: datetime, view_end: datetime, width_px: int,
) -> Tuple[Tuple[str, int, float, str], Optional[Tuple[str, int, float, str]]]:
    """Pick (major, minor) calendar steps for the visible span and pixel width."""
    span = max(1.0, _to_secs(view_end) - _to_secs(view_start))
    major = _STEPS[-1]
    for step in _STEPS:
        spacing = width_px * step[2] / span
        if spacing >= _MIN_MAJOR_PX:
            major = step
            break
    minor: Optional[Tuple[str, int, float, str]] = None
    major_idx = _STEPS.index(major)
    for i in range(major_idx - 1, -1, -1):
        spacing = width_px * _STEPS[i][2] / span
        if spacing >= _MIN_MINOR_PX:
            minor = _STEPS[i]
        else:
            break
    return major, minor


def _floor_dt(dt: datetime, unit: str, n: int) -> datetime:
    if unit == "minute":
        m = (dt.minute // n) * n
        return dt.replace(minute=m, second=0, microsecond=0)
    if unit == "hour":
        h = (dt.hour // n) * n
        return dt.replace(hour=h, minute=0, second=0, microsecond=0)
    if unit == "day":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    if unit == "week":
        base = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return base - timedelta(days=base.weekday())
    if unit == "month":
        m = ((dt.month - 1) // n) * n + 1
        return dt.replace(month=m, day=1, hour=0, minute=0, second=0, microsecond=0)
    # year
    y = (dt.year // n) * n
    if y < 1:
        y = 1
    return dt.replace(year=y, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)


def _advance(dt: datetime, unit: str, n: int) -> datetime:
    if unit == "minute":
        return dt + timedelta(minutes=n)
    if unit == "hour":
        return dt + timedelta(hours=n)
    if unit == "day":
        return dt + timedelta(days=n)
    if unit == "week":
        return dt + timedelta(weeks=n)
    if unit == "month":
        total = (dt.year * 12 + (dt.month - 1)) + n
        y, m = divmod(total, 12)
        return dt.replace(year=y, month=m + 1, day=1)
    return dt.replace(year=dt.year + n, month=1, day=1)


def _iter_ticks(view_start: datetime, view_end: datetime, step):
    """Yield aligned tick datetimes within [view_start, view_end]."""
    unit, n = step[0], step[1]
    cur = _floor_dt(view_start, unit, n)
    if cur < view_start:
        cur = _advance(cur, unit, n)
    guard = 0
    while cur <= view_end and guard < 5000:
        yield cur
        try:
            cur = _advance(cur, unit, n)
        except (OverflowError, ValueError):
            break
        guard += 1


def _format_dt_label(
    dt: datetime, view_start: Optional[datetime], view_end: Optional[datetime],
) -> str:
    """Format the crosshair label, including time only when usefully zoomed."""
    span = 0.0
    if view_start and view_end:
        span = _to_secs(view_end) - _to_secs(view_start)
    if 0 < span <= 3 * 86400:
        return dt.strftime("%d/%m/%Y %H:%M")
    return dt.strftime("%d/%m/%Y")


def _format_marker_tick_label(
    dt: datetime, view_start: Optional[datetime], view_end: Optional[datetime],
) -> str:
    """Format a ruler tick label at a modification date."""
    return _format_dt_label(dt, view_start, view_end)


def _format_tooltip(iso: str) -> str:
    dt = _parse_dt(iso)
    if dt is None:
        return iso
    return dt.strftime("%d/%m/%Y %H:%M:%S")


__all__ = ["TemporalTimelineWidget"]
