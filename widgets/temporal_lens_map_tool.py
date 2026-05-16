"""Rectangle map tool for the Time Lens dock (BL-IL-P0-10, phase 10a).

Captures a 2-point rectangle on the canvas and emits the resulting
`QgsGeometry` (polygon, canvas CRS) via the `selection_completed` Qt
signal. Esc cancels the in-flight rubber band; right-click also cancels.

The polygon variant (free-form polygon, double-click validates) is
deferred to phase 10b.
"""
from qgis.PyQt.QtCore import Qt, pyqtSignal
from qgis.PyQt.QtGui import QColor
from qgis.core import QgsGeometry, QgsPointXY, QgsRectangle, QgsWkbTypes
from qgis.gui import QgsMapTool, QgsRubberBand


class LensRectangleMapTool(QgsMapTool):
    """A `QgsMapTool` that lets the user drag a rectangle on the canvas.

    Workflow:
        1. Press button -> start point captured.
        2. Move -> rubber band updates live.
        3. Release -> rectangle geometry is built in the canvas CRS and
           emitted via `selection_completed`.
        4. Esc / right click -> rubber band is cleared, no signal emitted.

    The caller is expected to (a) connect to `selection_completed`,
    (b) restore the previous map tool after handling the signal,
    (c) call `reset()` if it wants to abort externally.
    """

    selection_completed = pyqtSignal(object)  # QgsGeometry (Polygon)

    def __init__(self, canvas):
        super().__init__(canvas)
        self._canvas = canvas
        self._start_point = None
        self._rubber = QgsRubberBand(canvas, QgsWkbTypes.PolygonGeometry)
        self._rubber.setColor(QColor(255, 165, 0, 80))    # translucent orange
        self._rubber.setStrokeColor(QColor(255, 100, 0))  # solid border
        self._rubber.setWidth(2)
        self.setCursor(Qt.CrossCursor)

    # ----- map tool overrides -------------------------------------------

    def canvasPressEvent(self, event):
        if event.button() == Qt.RightButton:
            self.reset()
            return
        self._start_point = self.toMapCoordinates(event.pos())
        self._rubber.reset(QgsWkbTypes.PolygonGeometry)
        self._update_rubber(self._start_point, self._start_point)

    def canvasMoveEvent(self, event):
        if self._start_point is None:
            return
        end = self.toMapCoordinates(event.pos())
        self._update_rubber(self._start_point, end)

    def canvasReleaseEvent(self, event):
        if event.button() != Qt.LeftButton or self._start_point is None:
            return
        end = self.toMapCoordinates(event.pos())
        rect = QgsRectangle(
            min(self._start_point.x(), end.x()),
            min(self._start_point.y(), end.y()),
            max(self._start_point.x(), end.x()),
            max(self._start_point.y(), end.y()),
        )
        self._start_point = None
        self._rubber.reset(QgsWkbTypes.PolygonGeometry)
        # Skip degenerate clicks (single point, no drag).
        if rect.width() <= 0 or rect.height() <= 0:
            return
        geom = QgsGeometry.fromRect(rect)
        self.selection_completed.emit(geom)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            self.reset()

    # ----- helpers ------------------------------------------------------

    def reset(self):
        """Cancel any in-flight rubber band and forget the start point."""
        self._start_point = None
        self._rubber.reset(QgsWkbTypes.PolygonGeometry)

    def _update_rubber(self, p1: QgsPointXY, p2: QgsPointXY):
        rect = QgsRectangle(
            min(p1.x(), p2.x()),
            min(p1.y(), p2.y()),
            max(p1.x(), p2.x()),
            max(p1.y(), p2.y()),
        )
        self._rubber.reset(QgsWkbTypes.PolygonGeometry)
        self._rubber.addGeometry(QgsGeometry.fromRect(rect), None)

    def deactivate(self):
        """QgsMapTool hook: called when another tool takes over."""
        self.reset()
        super().deactivate()


__all__ = [
    "LensRectangleMapTool",
]
