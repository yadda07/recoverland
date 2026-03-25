import os
import re

from qgis.PyQt.QtCore import QByteArray, QObject, QRectF
from qgis.PyQt.QtGui import QColor, QIcon, QPainter, QPixmap
from qgis.PyQt.QtSvg import QSvgRenderer

from .compat import QtCompat
from .core.logger import flog

_ICON_SIZES = (16, 24, 32, 48)


class ThemedActionIconController(QObject):
    def __init__(self, host_widget, action, svg_path):
        super().__init__(host_widget)
        self._host_widget = host_widget
        self._action = action
        self._svg_template = _read_svg_template(svg_path)
        if self._host_widget is not None:
            self._host_widget.installEventFilter(self)
        self.refresh()

    def dispose(self):
        if self._host_widget is not None:
            try:
                self._host_widget.removeEventFilter(self)
            except (AttributeError, RuntimeError, TypeError):
                pass
        self._host_widget = None
        self._action = None

    def refresh(self):
        if self._action is None or not self._svg_template:
            return
        text_color = _resolve_window_text_color(self._host_widget)
        if text_color is None:
            return
        icon = _build_icon(self._svg_template, text_color, self._host_widget)
        if icon is None or icon.isNull():
            return
        self._action.setIcon(icon)

    def eventFilter(self, watched, event):
        if watched is self._host_widget and event.type() == QtCompat.EVENT_PALETTE_CHANGE:
            if not getattr(self, '_refreshing', False):
                self._refreshing = True
                try:
                    self.refresh()
                finally:
                    self._refreshing = False
        return False


def _read_svg_template(svg_path):
    if not os.path.exists(svg_path):
        return ""
    try:
        with open(svg_path, 'r', encoding='utf-8') as handle:
            return handle.read()
    except OSError as exc:
        flog(f"ThemedActionIconController read error: {exc}", "WARNING")
        return ""


def _resolve_window_text_color(host_widget):
    if host_widget is None:
        return None
    try:
        return host_widget.palette().windowText().color()
    except (AttributeError, RuntimeError, TypeError) as exc:
        flog(f"ThemedActionIconController palette error: {exc}", "WARNING")
        return None


def _build_icon(svg_template, text_color, host_widget):
    svg_data = _replace_logo_text_fill(svg_template, text_color.name())
    renderer = QSvgRenderer(QByteArray(svg_data.encode('utf-8')))
    if not renderer.isValid():
        flog("ThemedActionIconController invalid SVG renderer", "WARNING")
        return None
    icon = QIcon()
    device_pixel_ratio = _resolve_device_pixel_ratio(host_widget)
    for size in _ICON_SIZES:
        pixmap = _render_pixmap(renderer, size, device_pixel_ratio)
        if not pixmap.isNull():
            icon.addPixmap(pixmap)
    if icon.isNull():
        return None
    return icon


def _replace_logo_text_fill(svg_template, color_hex):
    patterns = (
        r'(<g\b[^>]*\bid="logo-text"[^>]*\bfill=")[^"]+(")',
        r'(<g\b[^>]*\bfill=")[^"]+("[^>]*\bid="logo-text"[^>]*>)',
    )
    for pattern in patterns:
        updated_svg, replaced_count = re.subn(pattern, rf'\1{color_hex}\2', svg_template, count=1)
        if replaced_count == 1:
            return updated_svg
    return svg_template


def _resolve_device_pixel_ratio(host_widget):
    if host_widget is None:
        return 1.0
    ratio_getter = getattr(host_widget, 'devicePixelRatioF', None)
    if callable(ratio_getter):
        ratio = ratio_getter()
        if ratio and ratio > 0:
            return float(ratio)
    legacy_ratio_getter = getattr(host_widget, 'devicePixelRatio', None)
    if callable(legacy_ratio_getter):
        ratio = legacy_ratio_getter()
        if ratio and ratio > 0:
            return float(ratio)
    return 1.0


def _render_pixmap(renderer, size, device_pixel_ratio):
    pixel_size = max(int(round(size * device_pixel_ratio)), 1)
    pixmap = QPixmap(pixel_size, pixel_size)
    pixmap.fill(QColor(0, 0, 0, 0))
    pixmap.setDevicePixelRatio(device_pixel_ratio)
    painter = QPainter(pixmap)
    view_box = renderer.viewBoxF()
    renderer.render(painter, _target_rect(float(size), view_box))
    painter.end()
    return pixmap


def _target_rect(size, view_box):
    if view_box.height() <= 0 or view_box.width() <= 0:
        return QRectF(0.0, 0.0, size, size)
    aspect_ratio = view_box.width() / view_box.height()
    if aspect_ratio >= 1.0:
        width = size
        height = size / aspect_ratio
    else:
        height = size
        width = size * aspect_ratio
    x = (size - width) / 2.0
    y = (size - height) / 2.0
    return QRectF(x, y, width, height)
