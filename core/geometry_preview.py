# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Geometry preview on QGIS canvas for RecoverLand (P1.1).

Displays the old geometry from an audit event as a QgsRubberBand
on the map canvas. Manages lifecycle: one preview at a time,
cleared on deselect or dialog close.

Where a journal geometry is drawn is decided like where the Rewind writes
it (`display_verdict`, geometry_utils.crs_verdict): the journal holds RAW
coordinates, and the CRS the layer declared at capture is only a label.
After a relabel (Set Layer CRS, a corrected .prj) the raw coordinates are
already in the layer CRS; after a reprojection of the file in place they
must be reprojected; when the evidence cannot tell, nothing is drawn and
the refusal is named. Reprojecting by the capture CRS, as the preview
did, drew a past state hundreds of km away from where the Rewind puts it.

A display READS the stories the writes settled and never records one
(`DisplayStories`): what the user merely looked at must not decide what a
restore writes.
"""
import threading
from contextlib import contextmanager
from typing import NamedTuple, Optional

from qgis.core import (
    QgsGeometry, QgsCoordinateReferenceSystem, QgsCoordinateTransform,
    QgsProject, QgsRectangle,
)
from qgis.gui import QgsRubberBand, QgsMapCanvas
from qgis.PyQt.QtCore import QCoreApplication, QTimer

from .geometry_utils import is_geometry_present
from .logger import flog
from ..compat import QtCompat, QgisCompat


class DisplayVerdict(NamedTuple):
    """Where a captured geometry is drawn (`display_verdict`).

    ``geometry``: the geometry expressed in ``crs``, None when it may not
    be drawn. ``status``: the crs_verdict status, or ``no_layer_crs`` when
    no layer CRS can be read. ``reason_code`` and ``refusal``: empty, or
    what names the refusal (``crs_ambiguous``, ``crs_mismatch``).
    """

    geometry: object
    crs: object
    status: str
    reason_code: str = ""
    refusal: str = ""


def _crs_text(crs) -> str:
    try:
        return crs.authid() or crs.description() or "unknown CRS"
    except (AttributeError, RuntimeError):
        return "unknown CRS"


class DisplayStories:
    """The CRS stories as a display may know them: read, never recorded.

    geometry_utils.crs_verdict keeps, per layer and capture CRS, the story
    the evidence settled (relabel or reprojection, ``_CRS_STORIES``), and
    every later geometry without evidence of its own follows it: the
    writes of the rewind and of the event restore included. The displays
    went through the same verdict and RECORDED what they saw. The Restore
    preview, given one geometry of an event without the others, settled a
    story from part of the evidence (the whole of it being a conflict), and
    a later write with no evidence of its own then followed that story
    instead of being refused: which rows the user had clicked decided
    whether an unproven position was written.

    A display now decides against this view (`reading_stories`). It reads
    every story the writes settled, as they are when it asks; what it
    settles itself stays here, so the other entities of one Review date
    follow it as they would in a rewind to that date, and nothing reaches
    the table the writes read. When its own story and a settled one
    disagree, the answer is the conflict a write would reach. Only the
    thread that installed the view goes through it (every verdict runs on
    the GUI thread); another thread would read and write the real table.
    """

    def __init__(self):
        self._own: dict = {}
        self._settled = None
        self._thread = None

    def _mine(self) -> bool:
        return threading.get_ident() == self._thread

    def get(self, key, default=None):
        settled = self._settled.get(key) if self._settled is not None else None
        if not self._mine():
            return settled if settled is not None else default
        own = self._own.get(key)
        if own is None or (settled is not None and settled[0] == own[0]):
            return settled if settled is not None else default
        if settled is None:
            return own
        return ("conflict", f"{settled[0]}({settled[1]})_vs_display_"
                            f"{own[0]}({own[1]})")

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __contains__(self, key) -> bool:
        return self.get(key) is not None

    def __setitem__(self, key, value) -> None:
        if not self._mine():
            self._settled[key] = value
            return
        if self._own.get(key) != value:
            scope = (f"layer={key[0]} src_crs={key[2]}"
                     if isinstance(key, tuple) and len(key) > 2 else f"key={key!r}")
            flog(f"display: crs_story kept to the display, not recorded "
                 f"{scope} story={value!r} (a write follows only what a "
                 f"write proved)", "INFO")
        self._own[key] = value


@contextmanager
def reading_stories(view: DisplayStories):
    """Run crs_verdict against *view* instead of the stories the writes read.

    Yields False, and installs nothing, when the story table cannot be
    found where crs_verdict keeps it: the caller must then not consult the
    verdict at all, or what it sees would be recorded for the writes.
    """
    from . import geometry_utils  # noqa: PLC0415
    real = getattr(geometry_utils, "_CRS_STORIES", None)
    if isinstance(real, DisplayStories):
        # Already reading through a view (a display inside a display):
        # the outer one stays, nothing reaches the real table either way.
        yield True
        return
    if not isinstance(real, dict):
        yield False
        return
    view._settled = real
    view._thread = threading.get_ident()
    geometry_utils._CRS_STORIES = view
    try:
        yield True
    finally:
        geometry_utils._CRS_STORIES = real


# crs_verdict evidence for which the geometry alone said nothing either way.
_NO_EVIDENCE = ("no_evidence", "weak_area_uncorroborated",
                "weak_area_disagrees_with_story")
# crs_verdict evidence that names both stories at once.
_CONTRADICTION = ("witness_conflict", "evidence_conflict", "story_conflict",
                  "contradicts_story")


def _ambiguous_text(capture_crs_authid, layer_crs, evidence: str,
                    witnesses_given: bool) -> str:
    """Why a crs_ambiguous geometry is not drawn, in the terms of its evidence.

    The Restore preview is called with the displayed geometry alone. It
    used to say that "the evidence available" could not tell, which is
    false there: the evidence is the event's other captured geometry, the
    one the Rewind decides with, and the preview was never given it.
    """
    head = (f"Geometry captured in {capture_crs_authid}, layer now in "
            f"{_crs_text(layer_crs)}")
    relabel = "a relabel (same coordinates, CRS declaration corrected)"
    reprojection = "a reprojection (coordinates converted)"
    if evidence in _NO_EVIDENCE and not witnesses_given:
        return (f"{head}: not drawn. The display was given this geometry "
                f"alone: the witnesses (the other captured geometries of its "
                f"event, which the Rewind decides with) were not provided, "
                f"and this geometry alone does not tell {relabel} from "
                f"{reprojection}")
    if evidence in _NO_EVIDENCE:
        return (f"{head}: cannot tell {relabel} from {reprojection} from the "
                f"evidence available (the event's captured geometries, the "
                f"features still carrying one, the CRS area of use); not drawn")
    if evidence in _CONTRADICTION:
        return (f"{head}: the evidence attests both {relabel} and "
                f"{reprojection} ({evidence}); not drawn, as the Rewind "
                f"refuses to write it")
    if evidence == "witness_read_failed":
        return (f"{head}: the layer could not be read to look for the "
                f"features still carrying a captured position; not drawn")
    return (f"{head}: relabel or reprojection not established ({evidence}); "
            f"not drawn")


def display_verdict(geom, capture_crs_authid: Optional[str], layer,
                    trace_id: str = "", witnesses=(),
                    stories: Optional[DisplayStories] = None) -> DisplayVerdict:
    """Place *geom*, captured under *capture_crs_authid*, for display.

    The Rewind's verdict (geometry_utils.crs_verdict, same story per layer
    and capture CRS, same witnesses: the captured geometries of the event),
    so a past state is drawn where a rewind would put it back:
      * same CRS, equivalent ids, relabel, no capture CRS: the raw
        coordinates, in the layer CRS;
      * reprojection: the reprojected geometry, in the layer CRS;
      * crs_ambiguous, transform_failed: None, and the refusal named (in
        the terms of the evidence: a caller that gave no witness is told
        so, not that the evidence cannot tell).
    The verdict reads the stories through *stories* (a fresh
    `DisplayStories` when None: this geometry alone), never recording one
    for the writes. Without a layer CRS to read the story from, the capture
    CRS is the only declaration there is: the geometry in that CRS, as
    before the verdict existed.
    """
    from .geometry_utils import CRS_REFUSED_STATUSES, crs_verdict  # noqa: PLC0415
    layer_crs = None
    try:
        crs = layer.crs() if layer is not None else None
        if crs is not None and crs.isValid():
            layer_crs = crs
    except (AttributeError, RuntimeError):
        layer_crs = None
    if layer_crs is None:
        declared = QgsCoordinateReferenceSystem(capture_crs_authid or "")
        return DisplayVerdict(geom, declared, "no_layer_crs")
    with reading_stories(stories if stories is not None else DisplayStories()) as ok:
        if not ok:
            flog("display_verdict: the CRS story table was not found where "
                 "crs_verdict keeps it; the verdict is not consulted (it "
                 "would record what the display sees for the writes)", "ERROR")
            return DisplayVerdict(
                None, layer_crs, "story_unreadable", "crs_mismatch",
                f"Geometry captured in {capture_crs_authid}: the CRS verdict "
                f"could not be consulted without recording it; not drawn")
        verdict = crs_verdict(geom, capture_crs_authid, layer, trace_id, witnesses)
    if verdict.status in CRS_REFUSED_STATUSES or verdict.geometry is None:
        if verdict.status == "crs_ambiguous":
            code = "crs_ambiguous"
            given = any(w is not None for w in (witnesses or ()))
            text = _ambiguous_text(capture_crs_authid, layer_crs,
                                   verdict.evidence, given)
        else:
            code = "crs_mismatch"
            text = (f"Geometry captured in {capture_crs_authid} cannot be "
                    f"placed in {_crs_text(layer_crs)}; not drawn")
        return DisplayVerdict(None, layer_crs, verdict.status, code, text)
    return DisplayVerdict(verdict.geometry, layer_crs, verdict.status)


def _announce(title: str, text: str) -> bool:
    """Push *text* to the QGIS message bar; False when there is none."""
    try:
        from qgis.utils import iface  # noqa: PLC0415
        if iface is None:
            return False
        iface.messageBar().pushMessage(title, text, QgisCompat.MSG_WARNING, 0)
        return True
    except Exception as exc:  # noqa: BLE001 - the refusal is logged anyway
        flog(f"GeometryPreview: refusal not announced ({exc!r})", "WARNING")
        return False


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
        # Why the last show() drew nothing, "" when it drew.
        self.last_refusal = ""
        self._band: Optional[QgsRubberBand] = None
        self._last_bbox: Optional[QgsRectangle] = None
        self._flash_timer: Optional[QTimer] = None
        self._flash_remaining: int = 0
        self._flash_base_color = None
        self._flash_base_width: int = 2

    def show(self, wkb: bytes, crs_authid: Optional[str],
             target_layer=None, witnesses=()) -> bool:
        """Render old geometry on the canvas. Returns True on success.

        When ``target_layer`` is provided, the rubberband adopts the
        layer's primary color (single-symbol renderer) with reduced
        opacity so the user can tell at a glance "this is the geometry
        of layer X as a temporary preview". Falls back to the default
        red dash on any failure.

        The position is `display_verdict` (the Rewind's CRS verdict, with
        *witnesses*: the other captured geometries of the event). A
        geometry it refuses, or one that cannot be transformed to the
        canvas, is not drawn: ``last_refusal`` names why and the QGIS
        message bar says it. Drawing it anyway (the untransformed
        coordinates after a failed transform) put it where it never was.
        """
        self.clear()
        self.last_refusal = ""
        if not wkb:
            flog("GeometryPreview.show: skipped reason=empty_wkb", "INFO")
            return False
        geom = QgsGeometry()
        geom.fromWkb(wkb)
        if not is_geometry_present(geom):
            flog("GeometryPreview.show: skipped reason=empty_geometry_from_wkb",
                 "WARNING")
            return False

        verdict = display_verdict(geom, crs_authid, target_layer,
                                  witnesses=witnesses)
        if verdict.geometry is None:
            return self._refuse(verdict.reason_code, verdict.refusal,
                                crs_authid)
        geom = verdict.geometry
        src_crs = verdict.crs
        transform_status = f"no_transform verdict={verdict.status}"
        if src_crs is not None and src_crs.isValid():
            try:
                dst_crs = self._canvas.mapSettings().destinationCrs()
                if dst_crs.isValid() and src_crs != dst_crs:
                    xform = QgsCoordinateTransform(src_crs, dst_crs, QgsProject.instance())
                    geom = QgsGeometry(geom)
                    status = geom.transform(xform)
                    # Qgis.GeometryOperationResult on Qt6 (no int equality),
                    # an int before: Success is 0 either way.
                    if int(getattr(status, "value", status) or 0) != 0:
                        raise ValueError(f"transform status {status}")
                    transform_status = (f"transformed src={_crs_text(src_crs)} "
                                        f"dst={dst_crs.authid()} "
                                        f"verdict={verdict.status}")
                else:
                    transform_status = f"same_crs verdict={verdict.status}"
            except Exception as e:  # noqa: BLE001 - QgsCsException and others
                return self._refuse(
                    "crs_mismatch",
                    f"Geometry of layer CRS {_crs_text(src_crs)} cannot be "
                    f"placed on the map CRS ({e}); not drawn", crs_authid)

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

    def _refuse(self, reason_code: str, refusal: str,
                crs_authid: Optional[str]) -> bool:
        """Draw nothing, say why (the refusal state of the preview)."""
        self.last_refusal = refusal or reason_code or "?"
        flog(f"GeometryPreview.show: refused reason_code={reason_code} "
             f"capture_crs={crs_authid or '-'} detail={self.last_refusal}",
             "WARNING")
        _announce(
            QCoreApplication.translate("GeometryPreviewManager",
                                       "Aperçu de géométrie non affiché"),
            self.last_refusal)
        return False

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
            except Exception as exc:  # noqa: BLE001
                flog("GeometryPreview._apply_layer_style: rubberband refused "
                     f"setFillColor, preview stays outline-only ({exc!r})",
                     "DEBUG")
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
