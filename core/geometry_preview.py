"""Geometry preview on QGIS canvas for RecoverLand (P1.1).

Displays the old geometry from an audit event as a QgsRubberBand
on the map canvas. Manages lifecycle: one preview at a time,
cleared on deselect or dialog close.
"""
from typing import Optional

from qgis.core import QgsGeometry, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject
from qgis.gui import QgsRubberBand, QgsMapCanvas

from .logger import flog
from ..compat import QtCompat

try:
    from qgis.core import Qgis
    _POLYGON_TYPE = Qgis.GeometryType.Polygon
    _LINE_TYPE = Qgis.GeometryType.Line
    _POINT_TYPE = Qgis.GeometryType.Point
except (AttributeError, ImportError):
    _POLYGON_TYPE = 2
    _LINE_TYPE = 1
    _POINT_TYPE = 0


def _geom_band_type(geom: QgsGeometry):
    """Return the QgsRubberBand geometry type matching the QgsGeometry."""
    g_type = geom.type()
    if g_type == _POLYGON_TYPE:
        return _POLYGON_TYPE
    if g_type == _LINE_TYPE:
        return _LINE_TYPE
    return _POINT_TYPE


class GeometryPreviewManager:
    """Manages a single QgsRubberBand preview on the canvas."""

    def __init__(self, canvas: QgsMapCanvas):
        self._canvas = canvas
        self._band: Optional[QgsRubberBand] = None

    def show(self, wkb: bytes, crs_authid: Optional[str]) -> bool:
        """Render old geometry on the canvas. Returns True on success."""
        self.clear()
        if not wkb:
            return False
        geom = QgsGeometry()
        geom.fromWkb(wkb)
        if geom.isNull() or geom.isEmpty():
            flog("GeometryPreview: empty geometry from WKB", "WARNING")
            return False

        if crs_authid:
            try:
                src_crs = QgsCoordinateReferenceSystem(crs_authid)
                dst_crs = self._canvas.mapSettings().destinationCrs()
                if src_crs.isValid() and dst_crs.isValid() and src_crs != dst_crs:
                    xform = QgsCoordinateTransform(src_crs, dst_crs, QgsProject.instance())
                    geom.transform(xform)
            except Exception as e:
                flog(f"GeometryPreview: CRS transform failed: {e}", "WARNING")

        band_type = _geom_band_type(geom)
        self._band = QgsRubberBand(self._canvas, band_type)
        self._band.setColor(self._style_color())
        self._band.setWidth(2)
        self._band.setLineStyle(QtCompat.DASH_LINE)
        self._band.setToGeometry(geom)
        self._canvas.refresh()
        return True

    def zoom_to_preview(self) -> None:
        """Zoom canvas to the current preview extent with padding."""
        if self._band is None:
            return
        rect = self._band.asGeometry().boundingBox()
        if rect.isNull() or rect.isEmpty():
            return
        rect.scale(1.5)
        self._canvas.setExtent(rect)
        self._canvas.refresh()

    def clear(self) -> None:
        """Remove the current preview from the canvas."""
        if self._band is not None:
            self._canvas.scene().removeItem(self._band)
            self._band = None

    @property
    def is_active(self) -> bool:
        return self._band is not None

    @staticmethod
    def _style_color():
        from qgis.PyQt.QtGui import QColor
        return QColor(219, 68, 55, 140)
