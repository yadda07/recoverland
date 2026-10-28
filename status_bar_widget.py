# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""QGIS status bar indicator for RecoverLand (UX-G01, style vttPastilleSource).

Shows a persistent capsule in the QGIS status bar reflecting
tracking state and journal health.
Left-click toggles tracking; right-click opens the dialog.
"""
from qgis.PyQt.QtCore import QPropertyAnimation, pyqtSignal
from qgis.PyQt.QtGui import QColor
from qgis.PyQt.QtWidgets import (
    QGraphicsOpacityEffect, QHBoxLayout, QLabel, QWidget,
)

from .compat import QtCompat
from .core.health_monitor import HealthLevel
from .widgets.theme import resolve_theme

_CAPTURE_COLORS = {
    "PROTECTED": QColor(46, 204, 113),
    "PERSISTING": QColor(66, 133, 244),
    "DEGRADED": QColor(255, 152, 0),
    "PAUSED": QColor(149, 165, 166),
    "ERROR": QColor(219, 68, 55),
    "LOCKED": QColor(219, 68, 55),
    "NO_PROJECT": QColor(189, 195, 199),
}

_STATUS_COLORS = {
    HealthLevel.HEALTHY: QColor(46, 204, 113),
    HealthLevel.INFO: QColor(66, 133, 244),
    HealthLevel.WARNING: QColor(255, 152, 0),
    HealthLevel.CRITICAL: QColor(219, 68, 55),
    "disabled": QColor(231, 76, 60),
    "no_project": QColor(189, 195, 199),
}


def _semantic_dot_color(state: str, fallback: QColor, widget: QWidget) -> QColor:
    t = resolve_theme(widget)
    key = (state or "").upper()
    if key in ("PROTECTED", "HEALTHY"):
        return t.acheve
    if key in ("PERSISTING", "INFO"):
        return t.accent
    if key in ("DEGRADED", "WARNING"):
        return t.ambre
    if key in ("ERROR", "LOCKED", "CRITICAL", "DISABLED"):
        return t.erreur
    return t.tertiaire if key in ("PAUSED", "NO_PROJECT") else fallback


class StatusBarIndicator(QWidget):
    """Compact status indicator for the QGIS status bar (pastille VTT)."""

    toggle_requested = pyqtSignal()
    open_dialog_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("rlStatusBarCapsule")
        self.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._dot = QLabel(self)
        self._dot.setFixedSize(8, 8)
        self._text = QLabel("RecoverLand", self)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 0, 8, 0)
        layout.setSpacing(6)
        layout.addWidget(self._dot)
        layout.addWidget(self._text)
        self.setFixedHeight(22)
        self._current_level = "no_project"
        self._pulse_anim = None
        self._opacity_effect = QGraphicsOpacityEffect(self._dot)
        self._opacity_effect.setOpacity(1.0)
        self._dot.setGraphicsEffect(self._opacity_effect)
        self._apply_state("no_project", self.tr("RecoverLand : aucun projet"))

    def update_state(
        self,
        tracking_active: bool,
        health_level: str,
        event_count: int,
        size_str: str,
    ) -> None:
        """Update the indicator from current plugin state."""
        if not tracking_active:
            self._apply_state(
                "disabled",
                self.tr("RecoverLand : enregistrement desactive ({size})").format(size=size_str))
            return
        tooltip = self.tr(
            "RecoverLand : actif, {count} evenement(s), {size}"
        ).format(count=event_count, size=size_str)
        self._apply_state(health_level, tooltip)

    def update_health(self, health, journal_path: str = "",
                      event_count: int = 0, size_str: str = "",
                      recoverable_from: str = "") -> None:
        """Show the capture state computed from runtime facts."""
        labels = {
            "PROTECTED": self.tr("protege"),
            "PERSISTING": self.tr("ecriture en cours"),
            "DEGRADED": self.tr("degrade"),
            "PAUSED": self.tr("en pause"),
            "ERROR": self.tr("ERREUR"),
            "LOCKED": self.tr("enregistrement inactif (journal verrouille)"),
            "NO_PROJECT": self.tr("aucun projet"),
        }
        state = getattr(health, "state", "NO_PROJECT")
        label = labels.get(state, state)
        lines = [self.tr("RecoverLand : {label}").format(label=label)]
        for reason in list(getattr(health, "reasons", []) or [])[:6]:
            lines.append("  - " + reason)
        if journal_path:
            lines.append(self.tr("Journal : {path} ({size}, {count} evenement(s))")
                         .format(path=journal_path, size=size_str, count=event_count))
        queued = getattr(health, "queued_events", 0)
        if queued:
            lines.append(self.tr("En attente d'indexation : {n} (deja durables)")
                         .format(n=queued))
        last = getattr(health, "last_successful_flush", "")
        if last:
            lines.append(self.tr("Derniere ecriture reussie : {t}").format(t=last[:19]))
        if recoverable_from:
            lines.append(self.tr("Restaurable a partir du : {t}")
                         .format(t=recoverable_from[:19]))
        disk = getattr(health, "disk", "")
        if disk and disk != "HEALTHY":
            lines.append(self.tr("Disque : {d}").format(d=disk))
        self._text.setText("RecoverLand" if state == "PROTECTED"
                           else f"RecoverLand · {label}")
        self._current_level = state
        fallback = _CAPTURE_COLORS.get(state, _CAPTURE_COLORS["NO_PROJECT"])
        color = _semantic_dot_color(state, fallback, self)
        self._paint(color, "\n".join(lines))

    def set_no_project(self) -> None:
        self._apply_state("no_project", self.tr("RecoverLand : aucun projet ouvert"))

    def pulse(self) -> None:
        """Subtle opacity pulse on the dot (event commit feedback)."""
        if self._pulse_anim is not None:
            if self._pulse_anim.state() == QtCompat.ANIM_STATE_RUNNING:
                return
        self._pulse_anim = QPropertyAnimation(
            self._opacity_effect, b"opacity",
        )
        self._pulse_anim.setDuration(600)
        self._pulse_anim.setKeyValueAt(0.0, 1.0)
        self._pulse_anim.setKeyValueAt(0.5, 0.3)
        self._pulse_anim.setKeyValueAt(1.0, 1.0)
        self._pulse_anim.setEasingCurve(QtCompat.EASE_OUT_CUBIC)
        self._pulse_anim.start()

    def mousePressEvent(self, event) -> None:
        if event.button() == QtCompat.LEFT_BUTTON:
            self.toggle_requested.emit()
        elif event.button() == QtCompat.RIGHT_BUTTON:
            self.open_dialog_requested.emit()

    def _apply_state(self, level: str, tooltip: str) -> None:
        self._current_level = level
        self._text.setText("RecoverLand")
        fallback = _STATUS_COLORS.get(level, _STATUS_COLORS["no_project"])
        color = _semantic_dot_color(level, fallback, self)
        self._paint(color, tooltip)

    def _paint(self, color: QColor, tooltip: str) -> None:
        t = resolve_theme(self)
        self.setStyleSheet(
            f"QWidget#rlStatusBarCapsule {{"
            f"  background-color: {t.surface_douce.name()};"
            f"  border: 1px solid {t.ligne.name()};"
            f"  border-radius: 11px;"
            f"}}"
        )
        self._text.setStyleSheet(
            f"font-family: '{t.police}'; font-size: 11px; font-weight: 600; "
            f"color: {t.texte.name()}; background: transparent;"
        )
        self._dot.setStyleSheet(
            f"background-color: {color.name()}; "
            f"border-radius: 4px; "
            f"min-width: 8px; min-height: 8px; "
            f"max-width: 8px; max-height: 8px;"
        )
        hint = self.tr("Clic : activer/desactiver | Clic droit : ouvrir")
        full_tip = tooltip + "\n" + hint
        self._current_tooltip = full_tip
        self.setToolTip(full_tip)
