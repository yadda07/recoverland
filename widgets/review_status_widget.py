# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""RecoverLand status bar widget - pastille contextuelle inspiree de VTT (Pastille.qml).

Trois modes :
1. Idle : pastille discrete en attente
2. Review active : point anime + compteur d'entites + bouton d'arret SVG
3. Alert : notification temporaire
"""
from __future__ import annotations

import time
from typing import Optional

from qgis.PyQt.QtCore import QPropertyAnimation, QSize, QTimer, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QGraphicsOpacityEffect, QHBoxLayout, QLabel, QPushButton, QWidget,
)

from ..compat import QtCompat
from ..core.logger import flog
from .icons import theme_icon
from .theme import resolve_theme

_ICON_SIZE = QSize(12, 12)


class ReviewStatusWidget(QWidget):
    """RecoverLand status bar pill - contextual, minimal, VTT-themed."""

    stop_requested = pyqtSignal()

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.setObjectName("rl_pill")
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self.setFixedHeight(24)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 0, 6, 0)
        layout.setSpacing(6)

        self._dot = QLabel(self)
        self._dot.setFixedSize(8, 8)
        layout.addWidget(self._dot)

        self._text_label = QLabel(self.tr("Review"), self)
        layout.addWidget(self._text_label)

        self._stop_btn = QPushButton("", self)
        self._stop_btn.setFixedSize(16, 16)
        self._stop_btn.setIconSize(_ICON_SIZE)
        self._stop_btn.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._stop_btn.setToolTip(self.tr("Desactiver Review"))
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

    def _apply_pill_theme(self, mode: str) -> None:
        t = resolve_theme(self)
        if mode == "active":
            bg, fg, border, dot = t.acheve_doux, t.acheve, t.acheve, t.acheve
        elif mode == "working":
            bg, fg, border, dot = t.ambre_doux, t.ambre, t.ambre, t.ambre
        elif mode == "render":
            bg, fg, border, dot = t.accent_doux, t.accent, t.accent, t.accent
        elif mode == "alert":
            bg, fg, border, dot = t.erreur_douce, t.erreur, t.erreur, t.erreur
        else:
            bg, fg, border, dot = t.surface_douce, t.secondaire, t.ligne_controle, t.tertiaire

        self.setStyleSheet(
            f"QWidget#rl_pill {{"
            f"  background-color: {bg.name()};"
            f"  border: 1px solid {border.name()};"
            f"  border-radius: 12px;"
            f"  padding: 2px 8px;"
            f"}}"
        )
        self._text_label.setStyleSheet(
            f"font-family: '{t.police_titre}'; font-size: 11px; "
            f"font-weight: 600; color: {fg.name()}; background: transparent;"
        )
        self._dot.setStyleSheet(self._dot_css(dot.name()))
        self._stop_btn.setIcon(theme_icon("close", color=t.erreur, size=12))
        self._stop_btn.setStyleSheet(
            f"QPushButton {{ border: none; border-radius: 8px; "
            f"background: {t.erreur_douce.name()}; padding: 0; margin: 0; }}"
            f"QPushButton:hover {{ background: {t.surface_survol.name()}; }}"
        )

    def activate(self) -> None:
        """Review mode ON - semantic green pill, entity count, stop button."""
        self._active = True
        self._stop_btn.setVisible(True)
        self._stop_btn.setEnabled(False)
        QTimer.singleShot(600, lambda: self._stop_btn.setEnabled(True))
        self._apply_pill_theme("active")
        self._update_text()
        self.setToolTip(self._build_tooltip())
        self.show()
        flog("review_status event=activated", "DEBUG")

    def deactivate(self) -> None:
        """Review mode OFF - back to idle."""
        self._active = False
        self._n_entities = 0
        self._n_layers = 0
        self._last_refresh_ts = None
        self._stop_btn.setVisible(False)
        self._set_idle()
        flog("review_status event=deactivated", "DEBUG")

    def update_stats(
        self, n_entities: int, n_layers: int, n_overlays: int = 0,
    ) -> None:
        """Update after a Review refresh cycle."""
        self._n_entities = n_entities
        self._n_layers = n_layers
        self._last_refresh_ts = time.time()
        self._apply_pill_theme("active")
        self._update_text()
        self.setToolTip(self._build_tooltip())
        self._pulse()
        flog(
            f"review_status event=stats_updated "
            f"n_entities={n_entities} n_layers={n_layers}",
            "DEBUG",
        )

    def set_refreshing(self) -> None:
        """Visual feedback: fetch phase starting."""
        self._apply_pill_theme("working")
        self._text_label.setText(self.tr("Review · Recherche..."))
        self.setToolTip(self.tr("Review : Recherche des modifications"))

    def set_phase(self, phase: str, detail: str = "") -> None:
        """Update to reflect current processing phase."""
        if phase == "fetch":
            self._apply_pill_theme("working")
            self._text_label.setText(self.tr("Review · Recherche..."))
        elif phase == "render":
            self._apply_pill_theme("render")
            txt = (
                self.tr("Review · Rendu {detail}").format(detail=detail)
                if detail else self.tr("Review · Rendu...")
            )
            self._text_label.setText(txt)
        else:
            self._apply_pill_theme("active")
            self._update_text()

    def show_alert(self, message: str, duration_ms: int = 3000) -> None:
        """Show a brief toast-style alert (captures, errors, etc.)."""
        self._alert_active = True
        self._apply_pill_theme("alert")
        self._text_label.setText(message)
        self.show()
        self._alert_timer.start(duration_ms)
        flog(f"review_status event=alert msg={message}", "DEBUG")

    def _dismiss_alert(self) -> None:
        self._alert_active = False
        if self._active:
            self.activate()
        else:
            self._set_idle()

    def _update_text(self) -> None:
        if not self._active:
            self._text_label.setText(self.tr("Review"))
            return
        if self._n_entities == 0:
            self._text_label.setText(self.tr("Review · actif"))
        else:
            self._text_label.setText(self.tr("Review · {n}").format(n=self._n_entities))

    def _build_tooltip(self) -> str:
        lines = [self.tr("Review : Visualisation temps reel")]
        if self._active:
            lines.append(
                self.tr("{n_layers} couche(s) · {n_entities} entite(s)").format(
                    n_layers=self._n_layers, n_entities=self._n_entities,
                )
            )
            if self._last_refresh_ts is not None:
                ago = int(time.time() - self._last_refresh_ts)
                if ago < 5:
                    lines.append(self.tr("MAJ : a l'instant"))
                elif ago < 60:
                    lines.append(self.tr("MAJ : il y a {ago}s").format(ago=ago))
                else:
                    lines.append(self.tr("MAJ : il y a {m}min").format(m=ago // 60))
            lines.append("")
            lines.append(self.tr("Deplacez la carte pour rafraichir"))
            lines.append(self.tr("Clic X : desactiver"))
        else:
            lines.append(self.tr("Inactif"))
        return "\n".join(lines)

    def _set_idle(self) -> None:
        self._apply_pill_theme("idle")
        self._text_label.setText(self.tr("Review"))
        self.setToolTip(self.tr("Review : Inactif"))
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
        self._pulse_anim.setEasingCurve(QtCompat.EASE_OUT_CUBIC)
        self._pulse_anim.start()


__all__ = ["ReviewStatusWidget"]
