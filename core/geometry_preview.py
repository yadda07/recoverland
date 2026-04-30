"""Geometry preview on QGIS canvas for RecoverLand (P1.1).

Displays the old geometry from an audit event as a QgsRubberBand
on the map canvas. Manages lifecycle: one preview at a time,
cleared on deselect or dialog close.
"""
from typing import Optional

from qgis.core import QgsGeometry, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject
from qgis.gui import QgsRubberBand, QgsMapCanvas

from .geometry_utils import is_geometry_present
from .logger import flog
from ..compat import QtCompat, QgisCompat


def _geom_band_type(geom: QgsGeometry):
    """Return the QgsRubberBand geometry type matching the QgsGeometry.

    Uses QgisCompat.GEOM_* which resolves to Qgis.GeometryType (3.30+),
    QgsWkbTypes.GeometryType (3.22-3.28 fallback), or int (test stubs only).
    """
    g_type = geom.type()
    if g_type == QgisCompat.GEOM_POLYGON:
        return QgisCompat.GEOM_POLYGON
    if g_type == QgisCompat.GEOM_LINE:
        return QgisCompat.GEOM_LINE
    return QgisCompat.GEOM_POINT


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
        if not is_geometry_present(geom):
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
