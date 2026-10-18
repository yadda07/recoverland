"""Canvas date bar widget - slim overlay anchored to bottom of QGIS map canvas.

Architecture:
    CanvasDateBar is a QWidget with parent=iface.mapCanvas().
    It overlays the canvas at y = canvas.height() - BAR_HEIGHT.
    Canvas resize events are caught via an installEventFilter on the canvas.
    Slider <-> QDateEdit are kept in sync via a _syncing guard (no signal loop).

Public API::

    bar = CanvasDateBar(iface.mapCanvas())
    bar.set_range("2024-01-01", "2026-05-20")
    bar.date_changed.connect(my_slot)
    bar.show()
    # ...
    bar.set_stats(47)          # after reconstruction
    bar.set_loading()          # while worker runs
    bar.cleanup()              # before session stop
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

from qgis.PyQt.QtCore import QDate, QTime, QTimer, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QDateEdit, QHBoxLayout, QLabel, QPushButton,
    QSizePolicy, QTimeEdit, QWidget,
)

from ..compat import QtCompat
from ..core.logger import flog
from ..core.time_format import journal_now, journal_today, parse_iso_date
from .icons import theme_icon, theme_pixmap
from .temporal_timeline_widget import TemporalTimelineWidget
from .theme import ICONE_SM, ICONE_XS, RAYON_CONTROLE, resolve_theme, rgba_css

_BAR_HEIGHT = 48

_CANVAS_EVENT_TYPES = frozenset({int(QtCompat.EVENT_RESIZE), int(QtCompat.EVENT_MOVE)})


def _purge_stale_bars(canvas) -> None:
    """Destroy any orphan CanvasDateBar widgets left over from a previous load.

    Works across plugin reloads: identifies bars by the ``_rl_canvas_date_bar``
    marker attribute rather than isinstance(), which would fail after module reload.
    Checks both canvas children (current architecture) and top-level widgets
    (previous architecture) for backward compatibility.
    """
    try:
        from qgis.PyQt.QtWidgets import QApplication, QWidget as _QW  # noqa: PLC0415
        destroyed = 0
        seen = set()
        candidates = list(canvas.findChildren(_QW)) + list(QApplication.topLevelWidgets())
        for w in candidates:
            wid = id(w)
            if wid in seen:
                continue
            seen.add(wid)
            if not getattr(w, '_rl_canvas_date_bar', False):
                continue
            if getattr(w, '_closing', False):
                continue
            try:
                w._closing = True
                if hasattr(w, '_debounce'):
                    w._debounce.stop()
                if hasattr(w, '_viewport') and w._viewport is not None:
                    w._viewport.removeEventFilter(w)
                if hasattr(w, '_canvas'):
                    w._canvas.removeEventFilter(w)
                if hasattr(w, '_main_win') and w._main_win is not None:
                    w._main_win.removeEventFilter(w)
                w.hide()
                w.deleteLater()
                destroyed += 1
            except Exception as exc:  # noqa: BLE001
                # WARNING: a bar that refuses to tear down stays painted on
                # the canvas as a ghost, still filtering events.
                flog(f"canvas_date_bar: stale bar teardown failed, ghost bar "
                     f"may remain ({exc!r})", "WARNING")
        if destroyed:
            flog(f"canvas_date_bar: purged stale_bars n={destroyed}", "WARNING")
    except Exception as exc:  # noqa: BLE001
        # Swallowed on purpose: this runs from CanvasDateBar.__init__ and a
        # raise here would cost Review its date bar entirely. The message
        # carries nothing but exc!r, so it cannot raise in turn.
        flog(f"canvas_date_bar: stale bar purge aborted, leftovers from a "
             f"previous load may remain ({exc!r})", "WARNING")


class CanvasDateBar(QWidget):
    """Slim date bar anchored to the bottom of the QGIS map canvas.

    Signals
    -------
    date_changed : str
        ISO 8601 string (``yyyy-MM-ddT00:00:00``) emitted after 800 ms debounce.
    """

    date_changed = pyqtSignal(str)
    export_requested = pyqtSignal()

    def __init__(self, canvas, parent=None):
        _purge_stale_bars(canvas)
        viewport = canvas.viewport()    # canvas is a QGraphicsView/QAbstractScrollArea
        super().__init__(viewport)      # child of the viewport → actually rendered on top
        self._rl_canvas_date_bar = True  # marker for _purge_stale_bars
        self._canvas = canvas
        self._viewport = viewport
        self._ceiling = None
        self._closing = False
        self._base_date: Optional[date] = None
        self._end_date: Optional[date] = None
        self._total_days: int = 1
        self._syncing: bool = False

        self._debounce = QTimer(self)
        self._debounce.setSingleShot(True)
        self._debounce.setInterval(800)
        self._debounce.timeout.connect(self._emit_date_changed)

        self._reposition_timer = QTimer(self)
        self._reposition_timer.setSingleShot(True)
        self._reposition_timer.setInterval(200)
        self._reposition_timer.timeout.connect(self._reposition)

        self.setAttribute(QtCompat.WA_STYLED_BACKGROUND, True)
        self.setAutoFillBackground(True)
        self._build_ui()
        self._apply_style()
        viewport.installEventFilter(self)
        self._reposition()
        flog(
            f"canvas_date_bar: created canvas={canvas.__class__.__name__}",
            "DEBUG",
        )

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def set_range(
        self, first_iso: str, last_iso: str, preserve_cursor: bool = False
    ) -> None:
        """Configure slider range. first_iso / last_iso are ISO date strings.

        Args:
            preserve_cursor: when True, keep the current absolute date instead of
                jumping to today. Used when the range is refitted to markers so
                the user does not lose their position.
        """
        try:
            self._base_date = parse_iso_date(first_iso)
            self._end_date = parse_iso_date(last_iso)
        except (ValueError, TypeError):
            self._base_date = journal_today() - timedelta(days=365)
            self._end_date = journal_today()

        self._total_days = max(1, (self._end_date - self._base_date).days)
        today = journal_today()

        # Capture the cursor BEFORE touching the bounds. Moving them makes Qt
        # clamp the displayed date, which emits dateChanged and re-enters
        # _on_date_edit_changed; reading the cursor afterwards would preserve
        # the clamped value instead of the user's. The guard must cover the
        # bounds mutation for the same reason, and the clamp may still have
        # armed the debounce, so disarm it.
        current_iso = self._timeline.current_date_iso() if preserve_cursor else None
        self._syncing = True
        try:
            self._date_edit.setMinimumDate(
                QDate(self._base_date.year, self._base_date.month, self._base_date.day)
            )
            self._date_edit.setMaximumDate(
                QDate(today.year, today.month, today.day)
            )
        finally:
            self._syncing = False
        self._debounce.stop()

        self._timeline.set_range(first_iso, last_iso)
        if preserve_cursor and current_iso:
            # Clamp into the new span: otherwise the QDateEdit bounds re-clamp
            # the value and the handle and the editors diverge again.
            lo = f"{self._base_date.isoformat()}T00:00:00"
            hi = f"{self._end_date.isoformat()}T23:59:59"
            current_iso = min(max(current_iso, lo), hi)
            self._syncing = True
            try:
                self._timeline.set_value_iso(current_iso)
                # _apply_iso_to_editors, not _on_timeline_date_changed: the
                # latter returns immediately while _syncing is True, so the
                # editors were never moved back onto the preserved cursor.
                self._apply_iso_to_editors(current_iso)
            finally:
                self._syncing = False
            self._debounce.stop()
        else:
            self._go_today()
        flog(
            f"canvas_date_bar: range_set "
            f"first={first_iso} last={last_iso} total_days={self._total_days} "
            f"preserve_cursor={preserve_cursor}",
            "DEBUG",
        )

    def set_stats(self, n_entities: int, n_total: int = -1,
                  n_born_after: int = 0, n_unresolved: int = 0,
                  n_null_geometry: int = 0,
                  n_trace_ref_missing: int = 0,
                  n_identity_unproven: int = 0,
                  n_unsaved_edits: int = 0) -> None:
        """Update right-side status indicator after a successful reconstruction.

        Uses compact icons to avoid layout shifts:
        - ``∅``       : no entity differs from the current layers at this date
        - ``◎ N ↗``   : N entities outside viewport (orange)
        - ``✦ N``     : N entities visible (blue)

        Full description is available via tooltip, including what the
        overlays cannot show: entities created after the date (still drawn
        by the current layers) and entities whose state at the date is
        partly unknown.

        Args:
            n_entities: entities visible in the current viewport.
            n_total: entities globally at this date (-1 = unknown/same as n_entities).
            n_born_after: entities absent at this date that exist now.
            n_unresolved: entities whose state at this date is partly unknown.
            n_null_geometry: entities that had no geometry at this date
                (known state: listed without shape, the current layers may
                draw a geometry added since).
            n_trace_ref_missing: among the unresolved ones, entities whose
                state depends on a rewind whose source event was purged.
            n_identity_unproven: entities changed after the date that no
                entity at the date can be tied to (fids renumbered by a
                deletion: Shapefile, GeoJSON). Not among the entities
                counted: with none of those, the date is NOT "nothing
                differs" (``∅``), the status says what is missing.
        """
        if n_total < 0:
            n_total = n_entities
        notes = []
        if n_born_after:
            notes.append(self.tr(
                "{n} entité(s) créée(s) après cette date restent affichées "
                "par les couches actuelles").format(n=n_born_after))
        if n_null_geometry:
            notes.append(self.tr(
                "{n} entité(s) sans géométrie à cette date (aucune forme "
                "dessinée ; les couches actuelles montrent la géométrie "
                "reçue depuis, s'il y en a une)").format(n=n_null_geometry))
        if n_unresolved:
            notes.append(self.tr(
                "{n} entité(s) dont l'état à cette date n'est pas "
                "entièrement connu (non dessinée(s) ou incomplète(s))"
            ).format(n=n_unresolved))
        if n_trace_ref_missing:
            notes.append(self.tr(
                "dont {n} modifiée(s) après cette date par un retour arrière "
                "dont l'événement source a été purgé du journal"
            ).format(n=n_trace_ref_missing))
        if n_identity_unproven:
            notes.append(self.tr(
                "{n} entité(s) modifiée(s) après cette date sans identité "
                "prouvée à cette date (fid renumérotés par une suppression : "
                "Shapefile, GeoJSON) : leur état à cette date n'est pas "
                "montré"
            ).format(n=n_identity_unproven))
        if n_unsaved_edits:
            notes.append(self.tr(
                "{n} modification(s) non enregistrée(s) en cours d'édition "
                "sont affichées par les couches actuelles (hors journal)"
            ).format(n=n_unsaved_edits))
        suffix = "".join("\n" + note for note in notes)
        if n_entities == 0 and n_total == 0 and (n_identity_unproven or n_unsaved_edits):
            count_warn = n_identity_unproven or n_unsaved_edits
            self._status_state = "unproven" if n_identity_unproven else "unsaved"
            self._lbl_status.setText(f"? {count_warn}")
            self._lbl_status.setToolTip(self.tr(
                "Des entités ont changé depuis cette date, mais aucune n'a pu "
                "être reconstituée") + suffix)
            self._style_status_pill("warning")
        elif n_entities == 0 and n_total == 0:
            self._status_state = "empty"
            self._lbl_status.setText("∅")
            self._lbl_status.setToolTip(self.tr(
                "Aucune entité ne diffère de l'état actuel à cette date") + suffix)
            self._style_status_pill("idle")
        elif n_entities == 0 and n_total > 0:
            self._status_state = "outside"
            self._lbl_status.setText(f"◎ {n_total} ↗")
            self._lbl_status.setToolTip(
                self.tr("{n} entité(s) hors de l'emprise actuelle").format(n=n_total)
                + suffix
            )
            self._style_status_pill("warning")
        else:
            self._status_state = "entities"
            self._lbl_status.setText(f"✦ {n_entities}")
            self._lbl_status.setToolTip(
                self.tr("{n} entité(s) reconstituée(s)").format(n=n_entities)
                + suffix
            )
            self._style_status_pill("accent")

    def set_loading(self, done: int = 0, total: int = 0) -> None:
        """Show loading indicator while the date is being rebuilt.

        *done* / *total*: the journal geometries placed so far by the CRS
        verdict, when the live completion of the date has them to place
        (it runs across event-loop turns and can take seconds).
        """
        self._status_state = "loading"
        self._lbl_status.setText("⟳")
        tip = self.tr("Reconstruction en cours…")
        if total:
            tip += "\n" + self.tr(
                "{done} / {total} géométrie(s) placée(s) dans le SCR de la "
                "couche").format(done=done, total=total)
        self._lbl_status.setToolTip(tip)
        self._style_status_pill("warning")

    def set_error(self, message: str) -> None:
        """Show that the reconstruction FAILED, never as an empty result.

        The empty-set symbol means "nothing differs at this date", a valid
        answer. A failed read is the absence of an answer: rendering it as
        ``∅`` told the user that nothing had changed.
        """
        self._status_state = "error"
        self._lbl_status.setText("⚠")
        self._lbl_status.setToolTip(self.tr(
            "Reconstruction impossible à cette date : {msg}\n"
            "La carte ne montre PAS l'état à cette date."
        ).format(msg=message))
        self._style_status_pill("error")

    def _style_status_pill(self, tone: str) -> None:
        t = resolve_theme(self)
        if tone == "accent":
            fg, bg, bd = t.accent, t.accent_doux, t.accent
        elif tone == "warning":
            fg, bg, bd = t.ambre, t.ambre_doux, t.ambre
        elif tone == "error":
            fg, bg, bd = t.erreur, t.erreur_douce, t.erreur
        else:
            fg, bg, bd = t.secondaire, t.surface_douce, t.ligne
        style = (
            f"font-family: '{t.police_titre}'; font-size: 12px; font-weight: 700; "
            f"color: {fg.name()}; background-color: {bg.name()}; "
            f"border: 1px solid {bd.name()}; border-radius: 10px; padding: 2px 8px;"
        )
        if self._lbl_status.styleSheet() != style:
            self._lbl_status.setStyleSheet(style)

    def status_state(self) -> str:
        """"empty" | "outside" | "entities" | "loading" | "error" | ""."""
        return getattr(self, "_status_state", "")

    def current_date_iso(self) -> str:
        """Return selected date+time as ISO 8601 string."""
        qd = self._date_edit.date()
        qt = self._time_edit.time()
        return (
            f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}"
            f"T{qt.hour():02d}:{qt.minute():02d}:{qt.second():02d}"
        )

    def set_value_iso(self, iso: str) -> None:
        """Move the handle to ``iso`` programmatically, without emitting.

        Used to snap the slider after the requested cutoff is clamped to the
        tracking-start baseline (RL-E1-03).
        """
        try:
            d = parse_iso_date(iso)
            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return
        self._syncing = True
        try:
            self._date_edit.setDate(QDate(d.year, d.month, d.day))
            self._time_edit.setTime(QTime(dt.hour, dt.minute, dt.second))
            self._timeline.set_value_iso(iso)
        finally:
            self._syncing = False

    def cleanup(self) -> None:
        """Detach event filter and schedule widget deletion."""
        self._closing = True
        self._debounce.stop()
        self._reposition_timer.stop()
        self._viewport.removeEventFilter(self)
        self.hide()
        self.deleteLater()
        flog("canvas_date_bar: cleanup done", "DEBUG")

    # ------------------------------------------------------------------ #
    # Qt overrides                                                         #
    # ------------------------------------------------------------------ #

    def showEvent(self, event) -> None:
        """Reposition and raise after Qt maps the widget to screen."""
        super().showEvent(event)
        flog("canvas_date_bar: showEvent", "DEBUG")
        self._reposition()

    def hideEvent(self, event) -> None:
        """Track why/when the bar becomes invisible."""
        import traceback as _tb
        super().hideEvent(event)
        stack = "".join(_tb.format_stack(limit=6))
        flog(f"canvas_date_bar: hideEvent stack={stack!r}", "WARNING")

    def closeEvent(self, event) -> None:
        """Ignore OS-level close - only cleanup() may close the bar."""
        if not self._closing:
            flog("canvas_date_bar: closeEvent ignored (not from cleanup)", "WARNING")
            event.ignore()
            return
        super().closeEvent(event)

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if getattr(self, "_refreshing_style", False):
            return
        if event.type() == QtCompat.EVENT_PALETTE_CHANGE:
            self._refreshing_style = True
            try:
                self._apply_style()
            finally:
                self._refreshing_style = False

    def eventFilter(self, obj, event) -> bool:
        """Reposition after viewport resize/move; steal wheel over the timeline.

        QgsMapCanvas is a QGraphicsView that grabs wheel events at the viewport
        level to zoom the map, so the timeline's own wheelEvent rarely fires.
        We intercept wheel events landing on the timeline here and redirect them
        to its zoom, consuming the event so the map does not zoom underneath.
        """
        if obj is self._viewport:
            evt = int(event.type())
            if evt in _CANVAS_EVENT_TYPES:
                QTimer.singleShot(150, self._reposition)
            elif evt == int(QtCompat.EVENT_WHEEL) and self._maybe_zoom_timeline(event):
                return True
        return super().eventFilter(obj, event)

    def _maybe_zoom_timeline(self, event) -> bool:
        """Forward a viewport wheel event to the timeline when it is on top of it."""
        # isVisible(): a hidden-but-not-closing bar would otherwise keep
        # swallowing wheel events over its stale timeline rect, leaving a dead
        # zone where the map refuses to zoom.
        if self._closing or self._timeline is None or not self.isVisible():
            return False
        try:
            gp = event.globalPosition().toPoint()
        except (AttributeError, TypeError):
            gp = event.globalPos()
        local = self._timeline.mapFromGlobal(gp)
        over = self._timeline.rect().contains(local)
        flog(
            f"canvas_date_bar: viewport_wheel over_timeline={over} "
            f"local=({local.x()},{local.y()})",
            "DEBUG",
        )
        if not over:
            return False
        return self._timeline.handle_wheel(local.x(), event.angleDelta().y())

    # ------------------------------------------------------------------ #
    # Private                                                              #
    # ------------------------------------------------------------------ #

    def _build_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(12, 4, 12, 4)
        layout.setSpacing(8)

        self._lbl_icon = QLabel("", self)
        self._lbl_icon.setFixedSize(20, 20)
        self._lbl_icon.setStyleSheet("background: transparent; border: none;")
        layout.addWidget(self._lbl_icon)

        self._date_edit = QDateEdit(self)
        self._date_edit.setCalendarPopup(True)
        self._date_edit.setDisplayFormat("dd/MM/yyyy")
        self._date_edit.setFixedWidth(104)
        layout.addWidget(self._date_edit)

        self._time_edit = QTimeEdit(self)
        self._time_edit.setDisplayFormat("HH:mm")
        self._time_edit.setFixedWidth(64)
        layout.addWidget(self._time_edit)

        self._timeline = TemporalTimelineWidget(self)
        sp = QSizePolicy(QtCompat.SIZE_EXPANDING, QtCompat.SIZE_FIXED)
        self._timeline.setSizePolicy(sp)
        self._timeline.setFixedHeight(36)
        self._timeline.setToolTip(
            self.tr(
                "Molette : zoomer/dezoomer • Clic droit glisser : se deplacer • "
                "Double-clic : reinitialiser le zoom"
            )
        )
        layout.addWidget(self._timeline)
        self._timeline.date_changed.connect(self._on_timeline_date_changed)

        self._btn_today = QPushButton(self.tr("Aujourd'hui"), self)
        self._btn_today.setFixedWidth(100)
        self._btn_today.setCursor(QtCompat.POINTING_HAND_CURSOR)
        layout.addWidget(self._btn_today)

        self._btn_export = QPushButton(self.tr("Export"), self)
        self._btn_export.setFixedWidth(80)
        self._btn_export.setCursor(QtCompat.POINTING_HAND_CURSOR)
        self._btn_export.setToolTip(self.tr("Exporter le snapshot vers GeoPackage"))
        layout.addWidget(self._btn_export)

        self._lbl_status = QLabel("", self)
        self._lbl_status.setMinimumWidth(68)
        self._lbl_status.setAlignment(QtCompat.ALIGN_CENTER)
        layout.addWidget(self._lbl_status)

        self._date_edit.dateChanged.connect(self._on_date_edit_changed)
        self._time_edit.timeChanged.connect(self._on_time_changed)
        self._btn_today.clicked.connect(self._go_today)
        self._btn_export.clicked.connect(self.export_requested)

    def _apply_style(self) -> None:
        t = resolve_theme(self)
        pm = theme_pixmap("history", color=t.accent, size=ICONE_SM)
        if not pm.isNull():
            self._lbl_icon.setPixmap(pm)
        self._btn_today.setIcon(theme_icon("calendar", color=t.texte, size=ICONE_XS))
        self._btn_export.setIcon(theme_icon("download", color=t.texte, size=ICONE_XS))
        self.setStyleSheet(
            f"CanvasDateBar {{"
            f"  background-color: {rgba_css(t.fond, 238)};"
            f"  border-top: 1px solid {t.ligne_controle.name()};"
            f"}}"
            f"QLabel {{ color: {t.texte.name()}; font-size: 12px; }}"
            f"QPushButton {{"
            f"  color: {t.texte.name()}; background: {t.surface.name()};"
            f"  border: 1px solid {t.ligne_controle.name()}; "
            f"  border-radius: {RAYON_CONTROLE}px;"
            f"  padding: 3px 8px; font-size: 11px; font-weight: 600;"
            f"}}"
            f"QPushButton:hover {{"
            f"  background: {t.surface_survol.name()}; "
            f"  border-color: {t.accent.name()};"
            f"}}"
            f"QDateEdit, QTimeEdit {{"
            f"  color: {t.texte.name()}; background: {t.surface_champ.name()};"
            f"  border: 1px solid {t.ligne_controle.name()}; "
            f"  border-radius: {RAYON_CONTROLE}px;"
            f"  padding: 2px 6px; font-size: 12px;"
            f"}}"
            f"QDateEdit::drop-down {{ border: none; width: 16px; }}"
        )

    def _reposition(self) -> None:
        if self._closing:
            return
        cw = self._viewport.width()
        ch = self._viewport.height()
        self.resize(cw, _BAR_HEIGHT)
        self.move(0, ch - _BAR_HEIGHT)
        self.raise_()
        if not self.isVisible():
            self.show()
        flog(
            f"canvas_date_bar: reposition cw={cw} ch={ch} "
            f"visible={self.isVisible()} pos=({self.x()},{self.y()})",
            "DEBUG",
        )

    def set_ceiling(self, widget) -> None:
        """Set the widget that must always stay above this bar (e.g. the dialog)."""
        self._ceiling = widget
        flog(f"canvas_date_bar: ceiling_set widget={widget.__class__.__name__}", "DEBUG")

    def raise_safe(self) -> None:
        """Raise bar above canvas, then re-raise ceiling widget above bar.

        Guarantees z-order: canvas < bar < ceiling (dialog).
        Use this as slot for mapCanvasRefreshed instead of bare raise_().
        """
        self._raise_safe()

    def _raise_safe(self) -> None:
        self.raise_()

    def set_markers(self, date_isos: list, as_ticks: bool = False) -> None:
        """Pass markers to timeline. Accepts plain ISO strings or (iso, op_type) tuples.

        Args:
            as_ticks: when True, the same dates are also rendered as major ruler
                ticks (modification-centric mode). Default False preserves the
                calendar ladder for other consumers.
        """
        self._timeline.set_markers(date_isos)
        if as_ticks:
            tick_dates = [
                str(item[0]) if isinstance(item, (list, tuple)) else str(item)
                for item in date_isos or []
            ]
            self._timeline.set_marker_ticks(tick_dates)
            self._fit_range_to_marker_dates(tick_dates)
        else:
            self._timeline.set_marker_ticks(None)
        flog(
            f"canvas_date_bar: markers_set n={len(date_isos)} as_ticks={as_ticks}",
            "DEBUG",
        )

    def _fit_range_to_marker_dates(self, tick_dates: list[str]) -> None:
        """Set the visible date range to the marker span with padding.

        In Review mode the bar must always contain the modification events in
        the current viewport: start = first marker - padding, end = last marker
        + padding. The cursor stays at the same absolute date so the user does
        not lose control.
        """
        parsed: list[datetime] = []
        for raw in tick_dates:
            try:
                parsed.append(datetime.fromisoformat(raw.replace("Z", "+00:00")))
            except (ValueError, TypeError):
                pass
        if not parsed:
            return
        min_dt = min(parsed)
        max_dt = max(parsed)
        span = max(timedelta(seconds=1), max_dt - min_dt)
        padding = max(timedelta(hours=1), span * 0.02)
        start = (min_dt - padding).isoformat()
        end = (max_dt + padding).isoformat()
        self.set_range(start, end, preserve_cursor=True)
        flog(
            f"canvas_date_bar: range_fitted_to_markers "
            f"start={start} end={end} n={len(parsed)} "
            f"padding_hours={padding.total_seconds() / 3600:.2f}",
            "DEBUG",
        )

    def _apply_iso_to_editors(self, iso: str) -> None:
        """Move QDateEdit + QTimeEdit onto *iso*, without touching _syncing.

        Guard-free on purpose: callers that already hold the _syncing guard
        (set_range) need the write to happen, and _on_timeline_date_changed
        owns the guard for the interactive path.
        """
        try:
            d = parse_iso_date(iso)
            self._date_edit.setDate(QDate(d.year, d.month, d.day))
            try:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                self._time_edit.setTime(QTime(dt.hour, dt.minute, dt.second))
            except (ValueError, TypeError):
                pass
        except (ValueError, TypeError):
            pass

    def _on_timeline_date_changed(self, iso: str) -> None:
        """Sync QDateEdit + QTimeEdit when user drags / zooms the timeline."""
        if self._syncing:
            return
        self._syncing = True
        try:
            self._apply_iso_to_editors(iso)
        finally:
            self._syncing = False
        self._debounce.start()

    def _on_date_edit_changed(self, qd: QDate) -> None:
        if self._syncing or self._base_date is None:
            return
        self._syncing = True
        try:
            iso = f"{qd.year():04d}-{qd.month():02d}-{qd.day():02d}T00:00:00"
            self._timeline.set_value_iso(iso)
        finally:
            self._syncing = False
        self._debounce.start()

    def _go_today(self) -> None:
        # Journal clock, not wall clock: the cutoff this produces is compared
        # against UTC created_at values (see time_format.journal_now).
        today = journal_today()
        now = journal_now()
        qd = QDate(today.year, today.month, today.day)
        self._syncing = True
        try:
            self._date_edit.setDate(qd)
            self._time_edit.setTime(QTime(now.hour, now.minute, 0))
            # Reset the timeline view to the full range so the cursor at
            # "today" is guaranteed to be visible, instead of vanishing when
            # the user is zoomed elsewhere (e.g. Review mode on a past window).
            self._timeline.reset_view()
            self._timeline.set_value_iso(
                f"{today.year:04d}-{today.month:02d}-{today.day:02d}T00:00:00"
            )
        finally:
            self._syncing = False
        self._emit_date_changed()

    def _on_time_changed(self, _qt: QTime) -> None:
        if self._syncing:
            return
        self._debounce.start()

    def _emit_date_changed(self) -> None:
        iso = self.current_date_iso()
        flog(f"canvas_date_bar: date_changed iso={iso}", "DEBUG")
        self.date_changed.emit(iso)


__all__ = ["CanvasDateBar"]
