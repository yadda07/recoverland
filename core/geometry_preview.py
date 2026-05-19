"""Geometry preview on QGIS canvas for RecoverLand (P1.1).

Displays the old geometry from an audit event as a QgsRubberBand
on the map canvas. Manages lifecycle: one preview at a time,
cleared on deselect or dialog close.
"""
from typing import Optional

from qgis.core import (
    QgsGeometry, QgsCoordinateReferenceSystem, QgsCoordinateTransform,
    QgsProject, QgsRectangle,
)
from qgis.gui import QgsRubberBand, QgsMapCanvas
from qgis.PyQt.QtCore import QTimer

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
        self._last_bbox: Optional[QgsRectangle] = None
        self._flash_timer: Optional[QTimer] = None
        self._flash_remaining: int = 0
        self._flash_base_color = None
        self._flash_base_width: int = 2

    def show(self, wkb: bytes, crs_authid: Optional[str],
             target_layer=None) -> bool:
        """Render old geometry on the canvas. Returns True on success.

        When ``target_layer`` is provided, the rubberband adopts the
        layer's primary color (single-symbol renderer) with reduced
        opacity so the user can tell at a glance "this is the geometry
        of layer X as a temporary preview". Falls back to the default
        red dash on any failure.
        """
        self.clear()
        if not wkb:
            flog("GeometryPreview.show: skipped reason=empty_wkb", "INFO")
            return False
        geom = QgsGeometry()
        geom.fromWkb(wkb)
        if not is_geometry_present(geom):
            flog("GeometryPreview.show: skipped reason=empty_geometry_from_wkb",
                 "WARNING")
            return False

        transform_status = "no_transform"
        if crs_authid:
            try:
                src_crs = QgsCoordinateReferenceSystem(crs_authid)
                dst_crs = self._canvas.mapSettings().destinationCrs()
                if src_crs.isValid() and dst_crs.isValid() and src_crs != dst_crs:
                    xform = QgsCoordinateTransform(src_crs, dst_crs, QgsProject.instance())
                    geom.transform(xform)
                    transform_status = (f"transformed src={crs_authid} "
                                        f"dst={dst_crs.authid()}")
                elif not src_crs.isValid():
                    transform_status = f"src_crs_invalid src={crs_authid}"
                else:
                    transform_status = "same_crs"
            except Exception as e:
                flog(f"GeometryPreview.show: CRS transform failed: {e}",
                     "WARNING")
                transform_status = f"transform_failed err={e}"

        band_type = _geom_band_type(geom)
        self._band = QgsRubberBand(self._canvas, band_type)
        style_status = self._apply_layer_style(self._band, target_layer, geom)
        self._band.setToGeometry(geom)
        self._canvas.refresh()
        bbox = geom.boundingBox()
        self._last_bbox = QgsRectangle(bbox)
        flog(
            f"GeometryPreview.show: rendered crs_authid={crs_authid or '-'} "
            f"transform={transform_status} style={style_status} "
            f"bbox=({bbox.xMinimum():.4f},{bbox.yMinimum():.4f},"
            f"{bbox.xMaximum():.4f},{bbox.yMaximum():.4f})",
            "INFO",
        )
        return True

    def zoom_to_preview(self) -> None:
        """Center and zoom canvas to the current preview extent.

        Uses the bbox cached at show() time, not asGeometry() of the band:
        QgsRubberBand.asGeometry() can transiently return null after a
        setToGeometry call (Qt timing), which was causing the second-click
        zoom to be silently skipped.
        For point geometries the bounding box has zero area, so we
        synthesize a small window around the centroid.
        """
        if self._last_bbox is None:
            flog("zoom_to_preview: skipped reason=no_cached_bbox", "WARNING")
            return
        rect = QgsRectangle(self._last_bbox)
        if rect.isNull():
            flog("zoom_to_preview: skipped reason=null_cached_bbox", "WARNING")
            return
        if rect.width() == 0 and rect.height() == 0:
            cx, cy = rect.center().x(), rect.center().y()
            dst_crs = self._canvas.mapSettings().destinationCrs()
            padding = 0.0005 if dst_crs.isGeographic() else 50.0
            rect = QgsRectangle(
                cx - padding, cy - padding,
                cx + padding, cy + padding,
            )
            flog(
                f"zoom_to_preview: synth_point_window cx={cx:.4f} cy={cy:.4f} "
                f"padding={padding}",
                "INFO",
            )
        else:
            rect.scale(1.5)
        self._canvas.setExtent(rect)
        self._canvas.refresh()
        flog(
            f"zoom_to_preview: applied "
            f"xmin={rect.xMinimum():.4f} ymin={rect.yMinimum():.4f} "
            f"xmax={rect.xMaximum():.4f} ymax={rect.yMaximum():.4f}",
            "INFO",
        )

    def flash(self, n_blinks: int = 4, interval_ms: int = 180) -> None:
        """Make the rubberband blink to draw attention.

        Alternates between a bright yellow and the normal red color
        ``n_blinks`` times. Safe to call when no band is shown (no-op).
        Cancels any in-flight flash to avoid color leakage between rows.
        """
        if self._band is None:
            flog("GeometryPreview.flash: skipped reason=no_band", "DEBUG")
            return
        self._stop_flash()
        # Note: QgsRubberBand has no public color()/width() getters, so we
        # rely on _flash_base_color and _flash_base_width which are set by
        # _apply_layer_style / _apply_default_style at show() time.
        self._flash_remaining = n_blinks * 2
        self._flash_timer = QTimer(self._canvas)
        self._flash_timer.setInterval(interval_ms)
        self._flash_timer.timeout.connect(self._flash_tick)
        self._flash_timer.start()
        flog(
            f"GeometryPreview.flash: started n_blinks={n_blinks} "
            f"interval_ms={interval_ms}",
            "DEBUG",
        )

    def _flash_tick(self) -> None:
        """One step of the flash animation."""
        if self._band is None or self._flash_remaining <= 0:
            self._stop_flash()
            return
        base_color = self._flash_base_color or self._style_color()
        if self._flash_remaining % 2 == 0:
            self._band.setColor(self._flash_highlight_color())
            self._band.setWidth(max(self._flash_base_width + 2, 4))
        else:
            self._band.setColor(base_color)
            self._band.setWidth(self._flash_base_width)
        self._band.update()
        self._flash_remaining -= 1
        if self._flash_remaining <= 0:
            self._band.setColor(base_color)
            self._band.setWidth(self._flash_base_width)
            self._band.update()
            self._stop_flash()

    def _stop_flash(self) -> None:
        """Stop any in-flight flash animation and release the timer."""
        if self._flash_timer is not None:
            self._flash_timer.stop()
            self._flash_timer = None
        self._flash_remaining = 0

    def clear(self) -> None:
        """Remove the current preview from the canvas."""
        self._stop_flash()
        if self._band is not None:
            self._canvas.scene().removeItem(self._band)
            self._band = None
        self._last_bbox = None

    @staticmethod
    def _style_color():
        from qgis.PyQt.QtGui import QColor
        return QColor(219, 68, 55, 140)

    @staticmethod
    def _flash_highlight_color():
        from qgis.PyQt.QtGui import QColor
        return QColor(255, 235, 0, 220)

    def _apply_layer_style(self, band, target_layer, geom) -> str:
        """Style the rubberband after the target layer with reduced opacity.

        Inherits the primary color of the layer's single-symbol renderer.
        Keeps a dashed outline to flag the band as a temporary preview
        (vs. a real feature). Falls back to the red default on any failure
        or when the renderer is more complex than single-symbol.
        Returns a status string for the show() log line.
        Also caches the applied color and width into _flash_base_color /
        _flash_base_width so flash() can restore them at the end of the
        animation (QgsRubberBand has no public getters for these).
        """
        color = self._extract_layer_color(target_layer)
        if color is None:
            self._apply_default_style(band)
            return "fallback_default"
        from qgis.PyQt.QtGui import QColor
        primary = QColor(color)
        primary.setAlpha(180)
        band.setColor(primary)
        if geom.type() == QgisCompat.GEOM_POLYGON:
            fill = QColor(color)
            fill.setAlpha(70)
            try:
                band.setFillColor(fill)
            except Exception:
                pass
        band.setWidth(2)
        band.setLineStyle(QtCompat.DASH_LINE)
        self._flash_base_color = primary
        self._flash_base_width = 2
        return f"layer_color={primary.name()}"

    def _apply_default_style(self, band) -> None:
        """Default red dash style used when no target layer is available."""
        base = self._style_color()
        band.setColor(base)
        band.setWidth(2)
        band.setLineStyle(QtCompat.DASH_LINE)
        self._flash_base_color = base
        self._flash_base_width = 2

    @staticmethod
    def _extract_layer_color(layer):
        """Return the primary QColor of a single-symbol renderer, or None.

        Multi-symbol, rule-based or categorized renderers fall back to
        None because picking one symbol per row would require evaluating
        the renderer against an actual feature (out of scope here).
        """
        if layer is None:
            return None
        try:
            renderer = layer.renderer()
            if renderer is None:
                return None
            symbol = None
            if hasattr(renderer, "symbol"):
                try:
                    symbol = renderer.symbol()
                except Exception:
                    symbol = None
            if symbol is None:
                return None
            return symbol.color()
        except Exception as exc:
            flog(
                f"_extract_layer_color: exception err={exc}",
                "WARNING",
            )
            return None
