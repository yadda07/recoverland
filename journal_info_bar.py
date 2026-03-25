from dataclasses import dataclass
from typing import Dict, Tuple

from qgis.PyQt.QtCore import QByteArray, QRectF, Qt, pyqtSignal
from qgis.PyQt.QtGui import QColor, QPainter
from qgis.PyQt.QtSvg import QSvgRenderer
from qgis.PyQt.QtWidgets import QPushButton, QHBoxLayout, QLabel, QVBoxLayout, QWidget

from .compat import QtCompat

_ICON_TEMPLATES: Dict[str, str] = {
    "ALL": """
<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 24 24\">
  <ellipse cx=\"12\" cy=\"6\" rx=\"7\" ry=\"3\" fill=\"{ACCENT}\" fill-opacity=\"0.18\" stroke=\"{ACCENT}\" stroke-width=\"1.6\"/>
  <path d=\"M5 6v6c0 1.7 3.1 3 7 3s7-1.3 7-3V6\" fill=\"{ACCENT}\" fill-opacity=\"0.10\" stroke=\"{ACCENT}\" stroke-width=\"1.6\" stroke-linejoin=\"round\"/>
  <path d=\"M5 12v6c0 1.7 3.1 3 7 3s7-1.3 7-3v-6\" fill=\"{ACCENT}\" fill-opacity=\"0.10\" stroke=\"{ACCENT}\" stroke-width=\"1.6\" stroke-linejoin=\"round\"/>
</svg>
""",
    "UPDATE": """
<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"-1 -1 26 26\">
  <path d=\"M7 8a6 6 0 1 1-1 8\" fill=\"none\" stroke=\"{ACCENT}\" stroke-width=\"1.8\" stroke-linecap=\"round\"/>
  <path d=\"M7 4v4h4\" fill=\"none\" stroke=\"{ACCENT}\" stroke-width=\"1.8\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>
  <circle cx=\"17\" cy=\"16\" r=\"2.4\" fill=\"{ACCENT}\" fill-opacity=\"0.18\" stroke=\"{ACCENT}\" stroke-width=\"1.4\"/>
</svg>
""",
    "DELETE": """
<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 24 24\">
  <path d=\"M9 8V6h6v2\" fill=\"none\" stroke=\"{ACCENT}\" stroke-width=\"1.7\" stroke-linecap=\"round\"/>
  <path d=\"M6 8h12\" fill=\"none\" stroke=\"{ACCENT}\" stroke-width=\"1.7\" stroke-linecap=\"round\"/>
  <rect x=\"7.5\" y=\"8\" width=\"9\" height=\"10\" rx=\"2\" fill=\"{ACCENT}\" fill-opacity=\"0.12\" stroke=\"{ACCENT}\" stroke-width=\"1.6\"/>
  <path d=\"M10.5 11v4M13.5 11v4\" fill=\"none\" stroke=\"{ACCENT}\" stroke-width=\"1.5\" stroke-linecap=\"round\"/>
</svg>
""",
    "INSERT": """
<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 24 24\">
  <circle cx=\"12\" cy=\"12\" r=\"7.2\" fill=\"{ACCENT}\" fill-opacity=\"0.14\" stroke=\"{ACCENT}\" stroke-width=\"1.7\"/>
  <path d=\"M12 8v8M8 12h8\" fill=\"none\" stroke=\"{ACCENT}\" stroke-width=\"1.9\" stroke-linecap=\"round\"/>
</svg>
""",
}


@dataclass(frozen=True)
class SmartBarTileState:
    key: str
    label: str
    value: str
    accent: QColor
    tooltip: str


@dataclass(frozen=True)
class SmartBarState:
    title: str
    meta: str
    message: str
    mode: str
    active_keys: Tuple[str, ...]
    tiles: Tuple[SmartBarTileState, ...]
    health_level: str = "healthy"
    health_message: str = ""
    health_suggestion: str = ""


def _rgba(color: QColor, alpha: int) -> str:
    return f"rgba({color.red()}, {color.green()}, {color.blue()}, {alpha})"


class SvgGlyph(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._renderer = QSvgRenderer(self)
        self.setFixedSize(22, 22)

    def load_icon(self, icon_key: str, accent: QColor) -> None:
        template = _ICON_TEMPLATES.get(icon_key, _ICON_TEMPLATES["ALL"])
        svg_data = template.replace("{ACCENT}", accent.name())
        self._renderer.load(QByteArray(svg_data.encode("utf-8")))
        self.update()

    def paintEvent(self, _event):
        if not self._renderer.isValid():
            return
        painter = QPainter(self)
        painter.setRenderHint(QtCompat.ANTIALIAS, True)
        painter.setRenderHint(QtCompat.SMOOTH_PIXMAP, True)
        self._renderer.render(painter, QRectF(0, 0, self.width(), self.height()))


class SmartBarTile(QPushButton):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._accent = QColor(96, 96, 96)
        self._visual_mode = "idle"
        self.setObjectName("smartBarTile")
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setMinimumHeight(54)
        self.setMinimumWidth(90)
        self.setSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._icon = SvgGlyph(self)
        self._value_label = QLabel("0", self)
        self._title_label = QLabel("", self)
        self._value_label.setObjectName("smartBarTileValue")
        self._title_label.setObjectName("smartBarTileTitle")
        for child in (self._icon, self._value_label, self._title_label):
            child.setAttribute(QtCompat.WA_TRANSPARENT_FOR_MOUSE)
        content = QHBoxLayout(self)
        content.setContentsMargins(10, 8, 10, 8)
        content.setSpacing(8)
        text_col = QVBoxLayout()
        text_col.setContentsMargins(0, 0, 0, 0)
        text_col.setSpacing(1)
        text_col.addWidget(self._value_label)
        text_col.addWidget(self._title_label)
        content.addWidget(self._icon, 0, QtCompat.ALIGN_VCENTER)
        content.addLayout(text_col, 1)
        self._refresh_theme()

    def apply_state(self, tile_state: SmartBarTileState) -> None:
        self.setProperty("metric_key", tile_state.key)
        self._accent = tile_state.accent
        self._icon.load_icon(tile_state.key, tile_state.accent)
        self._value_label.setText(tile_state.value)
        self._title_label.setText(tile_state.label)
        self.setToolTip(tile_state.tooltip)
        self._refresh_theme()

    def set_visual_mode(self, mode: str) -> None:
        self._visual_mode = mode
        self.setEnabled(mode != "disabled")
        self._refresh_theme()

    def _refresh_theme(self) -> None:
        text = self.palette().windowText().color()
        accent = self._accent
        if self._visual_mode == "active":
            bg = _rgba(accent, 34)
            border = _rgba(accent, 138)
            hover_bg = _rgba(accent, 42)
            value_color = accent.name()
            title_color = text.name()
        elif self._visual_mode == "disabled":
            bg = _rgba(text, 10)
            border = _rgba(text, 22)
            hover_bg = bg
            value_color = _rgba(text, 88)
            title_color = _rgba(text, 112)
        else:
            bg = _rgba(accent, 16)
            border = _rgba(accent, 56)
            hover_bg = _rgba(accent, 24)
            value_color = text.name()
            title_color = _rgba(text, 170)
        self.setStyleSheet(
            f"QPushButton#smartBarTile {{"
            f"background-color: {bg};"
            f"border: 1px solid {border};"
            f"border-radius: 12px;"
            f"text-align: left;"
            f"padding: 0px;"
            f"}}"
            f"QPushButton#smartBarTile:hover {{"
            f"background-color: {hover_bg};"
            f"border-color: {border};"
            f"}}"
        )
        self._value_label.setStyleSheet(
            f"font-size: 15px; font-weight: 700; color: {value_color};"
        )
        self._title_label.setStyleSheet(
            f"font-size: 10px; font-weight: 600; color: {title_color};"
        )


_HEALTH_COLORS = {
    "healthy": None,
    "info": QColor(66, 133, 244),
    "warning": QColor(255, 152, 0),
    "critical": QColor(219, 68, 55),
}


class JournalInfoBar(QWidget):
    metricActivated = pyqtSignal(str)
    maintenanceRequested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("journalInfoBar")
        self.setSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._title_label = QLabel(self)
        self._meta_label = QLabel(self)
        self._message_label = QLabel(self)
        self._message_label.setWordWrap(True)
        self._health_label = QLabel(self)
        self._health_label.setWordWrap(True)
        self._health_label.setVisible(False)
        self._health_label.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._health_label.mousePressEvent = lambda _e: self.maintenanceRequested.emit()
        self._current_health = "healthy"
        self._refreshing = False
        self._tiles: Dict[str, SmartBarTile] = {}
        root = QVBoxLayout(self)
        root.setContentsMargins(12, 10, 12, 10)
        root.setSpacing(6)
        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.setSpacing(10)
        top.addWidget(self._title_label)
        top.addWidget(self._meta_label)
        top.addStretch(1)
        top.addWidget(self._message_label, 2)
        self._top_layout = top
        tiles_row = QHBoxLayout()
        tiles_row.setContentsMargins(0, 0, 0, 0)
        tiles_row.setSpacing(8)
        for key in ("ALL", "UPDATE", "DELETE", "INSERT"):
            tile = SmartBarTile(self)
            tile.clicked.connect(self._emit_metric)
            self._tiles[key] = tile
            tiles_row.addWidget(tile, 1)
        root.addLayout(top)
        root.addWidget(self._health_label)
        root.addLayout(tiles_row)
        self._refresh_theme()

    def apply_state(self, state: SmartBarState) -> None:
        active_keys = set(state.active_keys)
        by_key = {tile.key: tile for tile in state.tiles}
        self._title_label.setText(state.title)
        self._meta_label.setText(state.meta)
        self._meta_label.setVisible(bool(state.meta))
        self._message_label.setText(state.message)
        self._message_label.setVisible(bool(state.message))
        self._current_health = state.health_level
        has_health = bool(state.health_message)
        self._health_label.setText(state.health_message)
        self._health_label.setToolTip(state.health_suggestion)
        self._health_label.setVisible(has_health)
        for key, widget in self._tiles.items():
            tile_state = by_key.get(key)
            if tile_state is not None:
                widget.apply_state(tile_state)
            if state.mode != "ready":
                widget.set_visual_mode("disabled")
            elif key in active_keys:
                widget.set_visual_mode("active")
            else:
                widget.set_visual_mode("idle")
        self._refresh_theme()

    def add_trailing_widget(self, widget) -> None:
        self._top_layout.addWidget(widget)

    def _emit_metric(self) -> None:
        tile = self.sender()
        key = tile.property("metric_key") if tile is not None else None
        if key:
            self.metricActivated.emit(key)

    def changeEvent(self, event):
        super().changeEvent(event)
        if self._refreshing:
            return
        if event.type() == QtCompat.EVENT_PALETTE_CHANGE:
            self._refreshing = True
            try:
                self._refresh_theme()
                for tile in self._tiles.values():
                    tile._refresh_theme()
            finally:
                self._refreshing = False

    def _refresh_theme(self) -> None:
        pal = self.palette()
        text = pal.windowText().color()
        highlight = pal.highlight().color()
        mid = pal.mid().color()
        self.setStyleSheet(
            f"QWidget#journalInfoBar {{"
            f"background-color: {_rgba(highlight, 14)};"
            f"border: 1px solid {_rgba(mid, 60)};"
            f"border-radius: 14px;"
            f"}}"
        )
        self._title_label.setStyleSheet(
            f"font-size: 12px; font-weight: 700; color: {text.name()};"
        )
        self._meta_label.setStyleSheet(
            f"font-size: 11px; font-weight: 500; color: {_rgba(text, 150)};"
        )
        self._message_label.setStyleSheet(
            f"font-size: 11px; font-weight: 500; color: {_rgba(text, 180)};"
        )
        health_color = _HEALTH_COLORS.get(self._current_health)
        if health_color is not None:
            self._health_label.setStyleSheet(
                f"font-size: 11px; font-weight: 600; "
                f"color: {health_color.name()}; "
                f"padding: 2px 6px; "
                f"background: {_rgba(health_color, 20)}; "
                f"border-radius: 6px;"
            )
        else:
            self._health_label.setStyleSheet(
                f"font-size: 11px; font-weight: 500; color: {_rgba(text, 150)};"
            )
