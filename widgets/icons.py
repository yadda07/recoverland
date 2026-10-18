"""Registre unique d'icones SVG vectorielles modernes (Lucide 24x24, doctrine VTT).

Caracteristiques :
- Geometries 24x24 epurees, aerees, sans remplissage parasite ni chevauchement ;
- Rendu vectoriel direct sur QPainter (draw_svg_icon) sans pixmap intermediaire ;
- Rendu QPixmap/QIcon HiDPI (2x) sur fond 100 % transparent ARGB ;
- Cache LRU borne a 256 entrees et 2 Mio ;
- Table unique de correspondance etat/operation -> signe et couleur semantique.
"""
from __future__ import annotations

from collections import OrderedDict
from threading import RLock
from typing import Dict, Optional, Tuple

from qgis.PyQt.QtCore import QByteArray, QRectF
from qgis.PyQt.QtGui import QColor, QIcon, QPainter, QPixmap
from qgis.PyQt.QtSvg import QSvgRenderer

from ..compat import QtCompat
from ..core.logger import flog
from .theme import ICONE_SM, ThemeTokens, resolve_theme

_SVG_HEAD = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" '
    'viewBox="0 0 24 24" fill="none" stroke="{COLOR}" '
    'stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round">'
)
_SVG_TAIL = "</svg>"

# Geometries vectorielles Lucide 24x24 : epurees, lisibles a 16-24 px, zero fond.
_DESSINS_CORPS: Dict[str, str] = {
    "database": (
        '<ellipse cx="12" cy="5" rx="9" ry="3"/>'
        '<path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5"/>'
        '<path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3"/>'
    ),
    "refresh": (
        '<path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>'
        '<path d="M3 3v5h5"/>'
        '<path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/>'
        '<path d="M16 16h5v5"/>'
    ),
    "rewind": (
        '<path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>'
        '<path d="M3 3v5h5"/>'
        '<path d="M12 7v5l3 2"/>'
    ),
    "restore": (
        '<path d="M9 14 4 9l5-5"/>'
        '<path d="M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5a5.5 5.5 0 0 1-5.5 5.5H11"/>'
    ),
    "history": (
        '<path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>'
        '<path d="M3 3v5h5"/>'
        '<path d="M12 7v5l3 2"/>'
    ),
    "undo": (
        '<path d="M3 7v6h6"/>'
        '<path d="M21 17a9 9 0 0 0-9-9 9 9 0 0 0-6 2.3L3 13"/>'
    ),
    "review": (
        '<path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7-10-7-10-7Z"/>'
        '<circle cx="12" cy="12" r="3"/>'
    ),
    "search": '<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>',
    "check": '<path d="M20 6 9 17l-5-5"/>',
    "check-circle": '<circle cx="12" cy="12" r="9"/><path d="m9 12 2 2 4-4"/>',
    "warning": (
        '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/>'
        '<path d="M12 9v4"/><path d="M12 17h.01"/>'
    ),
    "error": '<circle cx="12" cy="12" r="9"/><path d="M12 8v4"/><path d="M12 16h.01"/>',
    "stop": '<circle cx="12" cy="12" r="9"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/>',
    "info": '<circle cx="12" cy="12" r="9"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    "help": (
        '<circle cx="12" cy="12" r="9"/>'
        '<path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>'
        '<path d="M12 17h.01"/>'
    ),
    "close": '<path d="M18 6 6 18"/><path d="m6 6 12 12"/>',
    "chevron": '<path d="m9 18 6-6-6-6"/>',
    "chevron-down": '<path d="m6 9 6 6 6-6"/>',
    "folder-open": (
        '<path d="m6 14 1.5-2.9A2 2 0 0 1 9.24 10H20a2 2 0 0 1 1.94 2.5'
        'l-1.54 6a2 2 0 0 1-1.95 1.5H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2'
        'h3.9a2 2 0 0 1 1.69.9l.81 1.2a2 2 0 0 0 1.67.9H18a2 2 0 0 1 2 2v2"/>'
    ),
    "save": (
        '<path d="M15.2 3a2 2 0 0 1 1.4.6l3.8 3.8a2 2 0 0 1 .6 1.4V19a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2z"/>'
        '<path d="M17 21v-7a1 1 0 0 0-1-1H8a1 1 0 0 0-1 1v7"/>'
        '<path d="M7 3v4a1 1 0 0 0 1 1h7"/>'
    ),
    "download": (
        '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
        '<path d="m7 10 5 5 5-5"/><path d="M12 15V3"/>'
    ),
    "upload": (
        '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
        '<path d="m17 8-5-5-5 5"/><path d="M12 3v12"/>'
    ),
    "table": (
        '<rect width="18" height="18" x="3" y="3" rx="2"/>'
        '<path d="M3 9h18"/><path d="M3 15h18"/><path d="M9 3v18"/>'
    ),
    "zoom-selection": (
        '<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/>'
        '<path d="M11 8v6"/><path d="M8 11h6"/>'
    ),
    "filter": '<polygon points="22 3 2 3 10 12.46 10 19 14 21 14 12.46 22 3"/>',
    "calendar": (
        '<rect width="18" height="18" x="3" y="4" rx="2"/>'
        '<path d="M16 2v4"/><path d="M8 2v4"/><path d="M3 10h18"/>'
    ),
    "timer": '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
    "edit": '<path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/>',
    "add": '<circle cx="12" cy="12" r="9"/><path d="M8 12h8"/><path d="M12 8v8"/>',
    "delete": (
        '<path d="M3 6h18"/>'
        '<path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/>'
        '<path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>'
        '<path d="M10 11v6"/><path d="M14 11v6"/>'
    ),
    "layers": (
        '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83'
        'l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/>'
        '<path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65"/>'
        '<path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65"/>'
    ),
    "shield-check": (
        '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01'
        'C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72'
        'a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/>'
        '<path d="m9 12 2 2 4-4"/>'
    ),
    "select-all": '<rect width="18" height="18" x="3" y="3" rx="2"/><path d="m9 12 2 2 4-4"/>',
    "deselect-all": '<rect width="18" height="18" x="3" y="3" rx="2"/>',
    "unknown": (
        '<circle cx="12" cy="12" r="9"/>'
        '<path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>'
        '<path d="M12 17h.01"/>'
    ),
}


def _build_svg_bytes(name: str, fg: QColor) -> Optional[QByteArray]:
    body = _DESSINS_CORPS.get(name)
    if body is None:
        return None
    svg_str = _SVG_HEAD.format(COLOR=fg.name()) + body + _SVG_TAIL
    return QByteArray(svg_str.encode("utf-8"))


class _IconCache:
    """Cache LRU d'images SVG HiDPI et de QSvgRenderer, borne a 256 entrees / 2 Mio."""

    def __init__(self, max_entries: int = 256, max_bytes: int = 2 * 1024 * 1024) -> None:
        self._cache: OrderedDict[Tuple[str, int, int], QPixmap] = OrderedDict()
        self._renderers: OrderedDict[Tuple[str, int], QSvgRenderer] = OrderedDict()
        self._bytes = 0
        self._max_entries = max(1, int(max_entries))
        self._max_bytes = max(1, int(max_bytes))
        self._lock = RLock()

    def renderer(self, name: str, fg: QColor) -> Optional[QSvgRenderer]:
        key = (name, fg.rgba())
        with self._lock:
            rend = self._renderers.get(key)
            if rend is not None:
                self._renderers.move_to_end(key)
                return rend
        svg_bytes = _build_svg_bytes(name, fg)
        if svg_bytes is None:
            return None
        rend = QSvgRenderer(svg_bytes)
        if not rend.isValid():
            return None
        with self._lock:
            if len(self._renderers) >= self._max_entries:
                self._renderers.popitem(last=False)
            self._renderers[key] = rend
        return rend

    def get(self, name: str, size: int, fg: QColor) -> QPixmap:
        logical = max(1, min(96, int(size)))
        dpr = 2.0
        px = int(logical * dpr)
        key = (name, logical, fg.rgba())
        with self._lock:
            cached = self._cache.get(key)
            if cached is not None:
                self._cache.move_to_end(key)
                return cached
        rend = self.renderer(name, fg)
        if rend is None:
            return QPixmap()
        pixmap = QPixmap(px, px)
        pixmap.fill(QColor(0, 0, 0, 0))
        pixmap.setDevicePixelRatio(dpr)
        painter = QPainter(pixmap)
        painter.setRenderHint(QtCompat.ANTIALIAS, True)
        painter.setRenderHint(QtCompat.SMOOTH_PIXMAP, True)
        rend.render(painter, QRectF(0.0, 0.0, float(logical), float(logical)))
        painter.end()

        cost = px * px * 4
        with self._lock:
            while self._cache and (
                len(self._cache) >= self._max_entries or self._bytes + cost > self._max_bytes
            ):
                _, evicted = self._cache.popitem(last=False)
                self._bytes = max(0, self._bytes - evicted.width() * evicted.height() * 4)
            self._cache[key] = pixmap
            self._bytes += cost
        return pixmap


_GLOBAL_ICON_CACHE = _IconCache()


def draw_svg_icon(painter: QPainter, name: str, rect: QRectF, color: QColor) -> None:
    """Dessine directement le vecteur SVG sur `painter` (nettete parfaite toute densite)."""
    rend = _GLOBAL_ICON_CACHE.renderer(name, color)
    if rend is not None:
        rend.render(painter, rect)


def theme_pixmap(
    name: str, color: Optional[QColor] = None, size: int = ICONE_SM,
) -> QPixmap:
    """Rend un QPixmap HiDPI (2x) 100 % transparent du signe `name`."""
    fg = color if color is not None else resolve_theme().texte
    return _GLOBAL_ICON_CACHE.get(name, size, fg)


def theme_icon(
    name: str, color: Optional[QColor] = None, size: int = ICONE_SM,
) -> QIcon:
    """Rend une QIcon multi-resolution sans fond parasite du signe `name`."""
    fg = color if color is not None else resolve_theme().texte
    icon = QIcon()
    for sz in (size, max(24, size)):
        pm = _GLOBAL_ICON_CACHE.get(name, sz, fg)
        if not pm.isNull():
            icon.addPixmap(pm)
    return icon


def icone_etat(code: str) -> str:
    """Source unique des signes d'etat et d'operation (une forme = un sens)."""
    mapping = {
        "ALL": "layers",
        "UPDATE": "edit",
        "DELETE": "delete",
        "INSERT": "add",
        "HEALTHY": "check-circle",
        "PROTECTED": "shield-check",
        "PERSISTING": "refresh",
        "INFO": "info",
        "WARNING": "warning",
        "DEGRADED": "warning",
        "CRITICAL": "error",
        "ERROR": "error",
        "LOCKED": "stop",
        "BLOCKED": "stop",
        "PAUSED": "timer",
        "GO": "check-circle",
        "GO_WITH_WARNINGS": "warning",
    }
    return mapping.get((code or "").upper(), "unknown")


def couleur_etat(code: str, tokens: Optional[ThemeTokens] = None) -> QColor:
    """Couleur semantique associee a un code d'etat ou d'operation."""
    t = tokens or resolve_theme()
    key = (code or "").upper()
    if key in ("INSERT", "HEALTHY", "PROTECTED", "GO", "ACHEVE"):
        return t.acheve
    if key in ("UPDATE", "WARNING", "DEGRADED", "GO_WITH_WARNINGS", "AMBRE"):
        return t.ambre
    if key in ("DELETE", "CRITICAL", "ERROR", "LOCKED", "BLOCKED"):
        return t.erreur
    if key in ("ALL", "INFO", "PERSISTING", "ACCENT"):
        return t.accent
    if key in ("PLANIFIE", "REVIEW"):
        return t.planifie
    return t.secondaire


flog("module=widgets.icons fn=init n_glyphs=" + str(len(_DESSINS_CORPS)), "DEBUG")

__all__ = ["couleur_etat", "draw_svg_icon", "icone_etat", "theme_icon", "theme_pixmap"]
