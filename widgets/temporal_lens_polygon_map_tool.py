"""Polygon map tool for the Time Lens dock (BL-IL-P0-10, phase 10b).

Captures a free-form polygon on the canvas and emits the resulting
`QgsGeometry` (polygon, canvas CRS) via the `selection_completed` Qt
signal.

Workflow:
    1. Left click adds a vertex (rubber band updates).
    2. Mouse move shows a temporary edge to the cursor.
    3. Double click validates (>=3 vertices required).
    4. Esc / right click cancel.
"""
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtGui import QColor
from qgis.core import QgsGeometry, QgsPointXY, QgsWkbTypes
from qgis.gui import QgsMapTool, QgsRubberBand

from ..compat import QtCompat


class LensPolygonMapTool(QgsMapTool):
    """A `QgsMapTool` for drawing a free-form polygon on the canvas.

    Emits `selection_completed(QgsGeometry)` on double-click with at
    least 3 distinct vertices. Right click / Esc cancel and clear the
    rubber band silently (no signal).

    The caller is expected to (a) connect to `selection_completed`,
    (b) restore the previous map tool after handling the signal,
    (c) call `reset()` to abort externally.
    """

    selection_completed = pyqtSignal(object)  # QgsGeometry (Polygon)

    def __init__(self, canvas):
        super().__init__(canvas)
        self._canvas = canvas
        self._points = []  # list[QgsPointXY], in canvas CRS
        self._rubber = QgsRubberBand(canvas, QgsWkbTypes.PolygonGeometry)
        self._rubber.setColor(QColor(255, 165, 0, 80))    # translucent orange
        self._rubber.setStrokeColor(QColor(255, 100, 0))  # solid border
        self._rubber.setWidth(2)
        self.setCursor(QtCompat.CROSS_CURSOR)

    # ----- map tool overrides -------------------------------------------

    def canvasPressEvent(self, event):
        if event.button() == QtCompat.RIGHT_BUTTON:
            self.reset()
            return
        if event.button() == QtCompat.LEFT_BUTTON:
            pt = self.toMapCoordinates(event.pos())
            self._points.append(pt)
            self._update_rubber()

    def canvasMoveEvent(self, event):
        if not self._points:
            return
        cursor = self.toMapCoordinates(event.pos())
        self._update_rubber(cursor)

    def canvasDoubleClickEvent(self, event):
        # On double-click Qt also fires the press for the second click,
        # so `self._points` already contains the duplicated final vertex.
        # Drop it before validating to match the user's intent.
        if event.button() != QtCompat.LEFT_BUTTON:
            return
        if self._points:
            self._points.pop()
        if len(self._points) < 3:
            self.reset()
            return
        ring = list(self._points) + [self._points[0]]
        geom = QgsGeometry.fromPolygonXY([ring])
        self.reset()
        self.selection_completed.emit(geom)

    def keyPressEvent(self, event):
        if event.key() == QtCompat.KEY_ESCAPE:
            self.reset()

    # ----- helpers ------------------------------------------------------

    def reset(self):
        """Drop in-flight vertices and clear the rubber band."""
        self._points = []
        self._rubber.reset(QgsWkbTypes.PolygonGeometry)

    def _update_rubber(self, cursor=None):
        self._rubber.reset(QgsWkbTypes.PolygonGeometry)
        pts = list(self._points)
        if cursor is not None and pts:
            pts.append(cursor)
        if len(pts) < 2:
            return
        if len(pts) == 2:
            geom = QgsGeometry.fromPolylineXY(pts)
        else:
            ring = pts + [pts[0]]
            geom = QgsGeometry.fromPolygonXY([ring])
        self._rubber.addGeometry(geom, None)

    def deactivate(self):
        """QgsMapTool hook: called when another tool takes over."""
        self.reset()
        super().deactivate()


__all__ = [
    "LensPolygonMapTool",
]
