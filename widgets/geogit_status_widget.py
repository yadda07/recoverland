"""RecoverLand status bar widget — Apple-like pill indicator.

A single premium widget in the QGIS status bar that adapts
contextually to show the most relevant information:

1. **Idle** — subtle "RL" pill with tracking status (on/off)
2. **GeoGit active** — animated dot + entity count + OFF button
3. **Alert** — brief toast for captures, restores, errors

Design principles:
- Minimal footprint, maximum signal
- Appears only when relevant, fades when not needed
- One pill, multiple modes — no clutter
"""
from __future__ import annotations

import time
from typing import Optional

from qgis.PyQt.QtWidgets import (
    QHBoxLayout, QLabel, QPushButton, QWidget, QGraphicsOpacityEffect,
)
from qgis.PyQt.QtCore import (
    QPropertyAnimation, QSize, QTimer, pyqtSignal,
)

from ..compat import QtCompat
from ..core.logger import flog


_ICON_SIZE = QSize(14, 14)

_PILL_BASE = (
    "QWidget#rl_pill {"
    "  border-radius: 10px;"
    "  padding: 2px 8px;"
    "}"
)
_PILL_IDLE = _PILL_BASE.replace("}", "  background: #2c3e50; color: #ecf0f1; }")
_PILL_ACTIVE = _PILL_BASE.replace("}", "  background: #1a252f; color: #2ecc71; }")
_PILL_ALERT = _PILL_BASE.replace("}", "  background: #e74c3c; color: #ffffff; }")
_PILL_WORKING = _PILL_BASE.replace("}", "  background: #1a252f; color: #f39c12; }")


class GeoGitStatusWidget(QWidget):
    """RecoverLand status bar pill — contextual, minimal, elegant."""

    stop_requested = pyqtSignal()

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.setObjectName("rl_pill")
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setFixedHeight(22)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 0, 8, 0)
        layout.setSpacing(5)

        self._dot = QLabel(self)
        self._dot.setFixedSize(8, 8)
        self._dot.setStyleSheet(self._dot_css("#95a5a6"))
        layout.addWidget(self._dot)

        self._text_label = QLabel("GeoGit", self)
        self._text_label.setStyleSheet(
            "font-size: 11px; font-weight: 600; letter-spacing: 0.5px;"
        )
        layout.addWidget(self._text_label)

        self._stop_btn = QPushButton("", self)
        self._stop_btn.setFixedSize(16, 16)
        self._stop_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._stop_btn.setStyleSheet(
            "QPushButton { font-size: 9px; font-weight: bold; "
            "border: none; border-radius: 8px; "
            "background: rgba(231,76,60,0.9); color: white; "
            "padding: 0; margin: 0; }"
            "QPushButton:hover { background: #c0392b; }"
        )
        self._stop_btn.setToolTip("Desactiver GeoGit")
        self._stop_btn.clicked.connect(self.stop_requested.emit)
        self._stop_btn.setVisible(False)
        layout.addWidget(self._stop_btn)

        self._opacity_effect = QGraphicsOpacityEffect(self._dot)
        self._opacity_effect.setOpacity(1.0)
        self._dot.setGraphicsEffect(self._opacity_effect)
        self._pulse_anim: Optional[QPropertyAnimation] = None

        self._alert_timer = QTimer(self)
        self._alert_timer.setSingleShot(True)
        self._alert_timer.timeout.connect(self._dismiss_alert)

        self._last_refresh_ts: Optional[float] = None
        self._n_entities = 0
        self._n_layers = 0
        self._active = False
        self._alert_active = False

        self._set_idle()
        self.hide()

    def activate(self) -> None:
        """GeoGit mode ON — green dot, entity count, stop button."""
        self._active = True
        self._dot.setStyleSheet(self._dot_css("#2ecc71"))
        self._stop_btn.setVisible(True)
        self._stop_btn.setEnabled(False)
        QTimer.singleShot(600, lambda: self._stop_btn.setEnabled(True))
        self.setStyleSheet(_PILL_ACTIVE)
        self._update_text()
        self.setToolTip(self._build_tooltip())
        self.show()
        flog("geogit_status event=activated", "DEBUG")

    def deactivate(self) -> None:
        """GeoGit mode OFF — back to idle."""
        self._active = False
        self._n_entities = 0
        self._n_layers = 0
        self._last_refresh_ts = None
        self._stop_btn.setVisible(False)
        self._set_idle()
        flog("geogit_status event=deactivated", "DEBUG")

    def update_stats(
        self,
        n_entities: int,
        n_layers: int,
        n_overlays: int = 0,
    ) -> None:
        """Update after a GeoGit refresh cycle."""
        self._n_entities = n_entities
        self._n_layers = n_layers
        self._last_refresh_ts = time.time()
        self._update_text()
        self.setToolTip(self._build_tooltip())
        self._pulse()
        flog(
            f"geogit_status event=stats_updated "
            f"n_entities={n_entities} n_layers={n_layers}",
            "DEBUG",
        )

    def set_refreshing(self) -> None:
        """Visual feedback: fetch phase starting."""
        self._dot.setStyleSheet(self._dot_css("#f39c12"))
        self.setStyleSheet(_PILL_WORKING)
        self._text_label.setText("GeoGit · Recherche...")
        self.setToolTip("GeoGit — Recherche des modifications")

    def set_phase(self, phase: str, detail: str = "") -> None:
        """Update to reflect current processing phase."""
        if phase == "fetch":
            self._dot.setStyleSheet(self._dot_css("#f39c12"))
            self.setStyleSheet(_PILL_WORKING)
            self._text_label.setText("GeoGit · Recherche...")
        elif phase == "render":
            self._dot.setStyleSheet(self._dot_css("#3498db"))
            self.setStyleSheet(_PILL_WORKING)
            txt = f"GeoGit · Rendu {detail}" if detail else "GeoGit · Rendu..."
            self._text_label.setText(txt)
        else:
            self._dot.setStyleSheet(self._dot_css("#2ecc71"))
            self.setStyleSheet(_PILL_ACTIVE)
            self._update_text()

    def show_alert(self, message: str, duration_ms: int = 3000) -> None:
        """Show a brief toast-style alert (captures, errors, etc.)."""
        self._alert_active = True
        self.setStyleSheet(_PILL_ALERT)
        self._text_label.setText(message)
        self._dot.setStyleSheet(self._dot_css("#ffffff"))
        self.show()
        self._alert_timer.start(duration_ms)
        flog(f"geogit_status event=alert msg={message}", "DEBUG")

    def _dismiss_alert(self) -> None:
        self._alert_active = False
        if self._active:
            self.activate()
        else:
            self._set_idle()

    def _update_text(self) -> None:
        if not self._active:
            self._text_label.setText("GeoGit")
            return
        if self._n_entities == 0:
            self._text_label.setText("GeoGit · actif")
        else:
            self._text_label.setText(f"GeoGit · {self._n_entities}")

    def _build_tooltip(self) -> str:
        lines = ["GeoGit — Visualisation temps reel"]
        if self._active:
            lines.append(
                f"{self._n_layers} couche(s) · "
                f"{self._n_entities} entite(s)"
            )
            if self._last_refresh_ts is not None:
                ago = int(time.time() - self._last_refresh_ts)
                if ago < 5:
                    lines.append("MAJ : a l'instant")
                elif ago < 60:
                    lines.append(f"MAJ : il y a {ago}s")
                else:
                    lines.append(f"MAJ : il y a {ago // 60}min")
            lines.append("")
            lines.append("Deplacez la carte pour rafraichir")
            lines.append("Clic X : desactiver")
        else:
            lines.append("Inactif")
        return "\n".join(lines)

    def _set_idle(self) -> None:
        self._dot.setStyleSheet(self._dot_css("#95a5a6"))
        self.setStyleSheet(_PILL_IDLE)
        self._text_label.setText("GeoGit")
        self.setToolTip("GeoGit — Inactif")
        self.hide()

    @staticmethod
    def _dot_css(color: str) -> str:
        return (
            f"background-color: {color}; border-radius: 4px; "
            f"min-width: 8px; min-height: 8px; "
            f"max-width: 8px; max-height: 8px;"
        )

    def _pulse(self) -> None:
        """Subtle opacity pulse on the dot (refresh feedback)."""
        if self._pulse_anim is not None:
            if self._pulse_anim.state() == QtCompat.ANIM_STATE_RUNNING:
                return
        self._pulse_anim = QPropertyAnimation(
            self._opacity_effect, b"opacity",
        )
        self._pulse_anim.setDuration(400)
        self._pulse_anim.setKeyValueAt(0.0, 1.0)
        self._pulse_anim.setKeyValueAt(0.5, 0.3)
        self._pulse_anim.setKeyValueAt(1.0, 1.0)
        self._pulse_anim.setEasingCurve(QtCompat.EASE_IN_OUT_QUAD)
        self._pulse_anim.start()


__all__ = ["GeoGitStatusWidget"]
