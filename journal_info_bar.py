"""Bandeau de synthese et d'etat du journal local (contour actif sans soulignement redondant)."""
from dataclasses import dataclass
from typing import Dict, Tuple

from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtGui import QColor
from qgis.PyQt.QtWidgets import QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from .compat import QtCompat
from .widgets.icons import couleur_etat, icone_etat, theme_pixmap
from .widgets.theme import (
    HAUTEUR_CONFORTABLE,
    ICONE_MD,
    ICONE_SM,
    RAYON_CONTROLE,
    RAYON_PANNEAU,
    resolve_theme,
    rgba_css,
)


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


class SmartBarTile(QPushButton):
    """Tuile d'indicateur et de filtre d'operation (contour actif seul, zero fond parasite)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._visual_mode = "idle"
        self._metric_key = "ALL"
        self.setObjectName("smartBarTile")
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setFixedHeight(HAUTEUR_CONFORTABLE)
        self.setMinimumWidth(96)
        self.setSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._icon_label = QLabel(self)
        self._icon_label.setFixedSize(ICONE_MD, ICONE_MD)
        self._icon_label.setStyleSheet("background: transparent; border: none;")
        self._value_label = QLabel("0", self)
        self._title_label = QLabel("", self)
        for child in (self._icon_label, self._value_label, self._title_label):
            child.setAttribute(QtCompat.WA_TRANSPARENT_FOR_MOUSE)
        content = QHBoxLayout(self)
        content.setContentsMargins(12, 4, 12, 4)
        content.setSpacing(10)
        text_col = QVBoxLayout()
        text_col.setContentsMargins(0, 0, 0, 0)
        text_col.setSpacing(0)
        text_col.addStretch(1)
        text_col.addWidget(self._value_label)
        text_col.addWidget(self._title_label)
        text_col.addStretch(1)
        content.addWidget(self._icon_label, 0, QtCompat.ALIGN_VCENTER)
        content.addLayout(text_col, 1)
        self._refresh_theme()

    def apply_state(self, tile_state: SmartBarTileState) -> None:
        self._metric_key = tile_state.key
        self.setProperty("metric_key", tile_state.key)
        self._value_label.setText(tile_state.value)
        self._title_label.setText(tile_state.label)
        self.setToolTip(tile_state.tooltip)
        self._refresh_theme()

    def set_visual_mode(self, mode: str) -> None:
        self._visual_mode = mode
        self.setEnabled(mode != "disabled")
        self._refresh_theme()

    def _refresh_theme(self) -> None:
        t = resolve_theme(self)
        sem_color = couleur_etat(self._metric_key, t)
        if self._visual_mode == "active":
            bg = t.accent_doux.name()
            border = f"1px solid {t.accent.name()}"
            hover_bg = t.surface_survol.name()
            val_color = t.texte.name()
            lbl_color = t.accent.name()
            ic_color = sem_color
        elif self._visual_mode == "disabled":
            bg = t.surface_douce.name()
            border = f"1px solid {t.ligne.name()}"
            hover_bg = bg
            val_color = t.tertiaire.name()
            lbl_color = t.tertiaire.name()
            ic_color = t.tertiaire
        else:
            bg = t.surface_douce.name()
            border = f"1px solid {t.ligne.name()}"
            hover_bg = t.surface_survol.name()
            val_color = t.texte.name()
            lbl_color = t.secondaire.name()
            ic_color = sem_color

        pm = theme_pixmap(icone_etat(self._metric_key), color=ic_color, size=ICONE_MD)
        if not pm.isNull():
            self._icon_label.setPixmap(pm)

        self.setStyleSheet(
            f"QPushButton#smartBarTile {{"
            f"  background-color: {bg};"
            f"  border: {border};"
            f"  border-radius: {RAYON_CONTROLE}px;"
            f"  text-align: left;"
            f"  padding: 0px;"
            f"}}"
            f"QPushButton#smartBarTile:hover {{"
            f"  background-color: {hover_bg};"
            f"  border-color: {t.accent.name()};"
            f"}}"
        )
        self._value_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 14px; font-weight: 700; "
            f"color: {val_color}; background: transparent; border: none;"
        )
        self._title_label.setStyleSheet(
            f"font-family: '{t.police}'; font-size: 10px; font-weight: 600; "
            f"color: {lbl_color}; background: transparent; border: none;"
        )


class JournalInfoBar(QWidget):
    """Bandeau de source et d'activite du journal local inspire de VTT."""

    metricActivated = pyqtSignal(str)
    maintenanceRequested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("journalInfoBar")
        self.setSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._source_icon = QLabel(self)
        self._source_icon.setFixedSize(ICONE_SM, ICONE_SM)
        self._source_icon.setStyleSheet("background: transparent; border: none;")
        self._status_dot = QLabel(self)
        self._status_dot.setFixedSize(8, 8)
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
        self._current_mode = "ready"
        self._refreshing = False
        self._tiles: Dict[str, SmartBarTile] = {}

        root = QVBoxLayout(self)
        root.setContentsMargins(12, 8, 12, 8)
        root.setSpacing(6)
        top = QHBoxLayout()
        top.setContentsMargins(0, 0, 0, 0)
        top.setSpacing(8)
        top.addWidget(self._source_icon, 0, QtCompat.ALIGN_VCENTER)
        top.addWidget(self._status_dot, 0, QtCompat.ALIGN_VCENTER)
        top.addWidget(self._title_label, 0, QtCompat.ALIGN_VCENTER)
        top.addWidget(self._meta_label, 0, QtCompat.ALIGN_VCENTER)
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
        self._current_mode = state.mode
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
        t = resolve_theme(self)
        dot_color = (
            t.tertiaire if self._current_mode != "ready"
            else couleur_etat(self._current_health, t)
        )
        pm = theme_pixmap("database", color=dot_color, size=ICONE_SM)
        if not pm.isNull():
            self._source_icon.setPixmap(pm)
        self._status_dot.setStyleSheet(
            f"background-color: {dot_color.name()}; border-radius: 4px; border: none; "
            f"min-width: 8px; min-height: 8px; max-width: 8px; max-height: 8px;"
        )
        self.setStyleSheet(
            f"QWidget#journalInfoBar {{"
            f"  background-color: {t.surface.name()};"
            f"  border: 1px solid {t.ligne.name()};"
            f"  border-radius: {RAYON_PANNEAU}px;"
            f"}}"
        )
        self._title_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 13px; font-weight: 700; "
            f"color: {t.texte.name()}; background: transparent; border: none;"
        )
        self._meta_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 11px; font-weight: 600; "
            f"color: {t.secondaire.name()}; background-color: {t.surface_douce.name()}; "
            f"border: 1px solid {t.ligne.name()}; border-radius: 10px; padding: 2px 8px;"
        )
        self._message_label.setStyleSheet(
            f"font-size: 11px; font-weight: 500; color: {t.secondaire.name()}; "
            f"background: transparent; border: none;"
        )
        if self._current_health in ("warning", "critical", "info"):
            h_col = couleur_etat(self._current_health, t)
            self._health_label.setStyleSheet(
                f"font-size: 11px; font-weight: 600; color: {h_col.name()}; "
                f"padding: 4px 8px; background: {rgba_css(h_col, 24)}; "
                f"border: 1px solid {rgba_css(h_col, 90)}; border-radius: {RAYON_CONTROLE}px;"
            )
        else:
            self._health_label.setStyleSheet(
                f"font-size: 11px; font-weight: 500; color: {t.secondaire.name()}; "
                f"background: transparent; border: none;"
            )
