"""Minimal dock widget for the Time Lens (BL-IL-P0-10, phase 10a).

End-to-end wiring of the prototype:

    User picks an audited layer in the combobox -> clicks the rectangle
    button -> drags a zone on the map canvas -> clicks Refresh -> the
    facade `execute_grouped_lens_view` produces the 3 overlay layers
    and the status label reports the entity / event counts.

This phase ships the strict minimum required to *use* Time Lens:

    - Combobox of audited layers (filtered via `datasource_registry`).
    - Button "Select rectangle" (activates `LensRectangleMapTool`).
    - Button "Refresh" (calls the facade, updates status).
    - Status label (selection state + render counters + truncation).
    - Button "Disable Lens" (purge overlays + close dock).

Phase 10b will add the polygon map tool, the date range picker, the
operation filter combobox, the colour legend, the truncation banner and
the entity list. Phase 10c will add the attribute diff panel.
"""
from datetime import datetime, timezone
from typing import Optional

from qgis.PyQt.QtCore import QDateTime
from qgis.PyQt.QtWidgets import (
    QComboBox,
    QDockWidget,
    QFrame,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)
from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsGeometry,
    QgsProject,
    QgsRectangle,
)
from qgis.gui import QgsDateTimeEdit

from ..compat import QtCompat
from ..core.lens_contracts import LensOpFilter
from ..core.time_format import format_relative_time
from .temporal_lens_map_tool import LensRectangleMapTool
from .temporal_lens_polygon_map_tool import LensPolygonMapTool

# Static legend palette (RGB hex). Kept in sync with lens_renderer
# operation classes; the renderer itself does not yet expose its
# palette as a public constant, so we redeclare it here (5 entries).
_LEGEND_PALETTE = (
    ("INSERT", "#4CAF50", "creations"),
    ("UPDATE", "#FF9800", "modifications"),
    ("DELETE", "#F44336", "suppressions"),
    ("ATTR",   "#2196F3", "attributs seuls"),
    ("GEOM",   "#9C27B0", "geometrie seule"),
)

_PRESETS = (
    ("Aujourd'hui", 0),
    ("7 derniers jours", 7),
    ("30 derniers jours", 30),
    ("Personnalise", None),
)
_DEFAULT_PRESET_INDEX = 2  # 30 derniers jours


# Logging is wired lazily so the module imports cleanly off-QGIS for the
# structural assertion in the validation scenario.
def _flog(msg, level="INFO"):
    try:
        from ..core.logger import flog
        flog(msg, level)
    except Exception:  # pragma: no cover - logger always available in QGIS
        pass


class TemporalLensDock(QDockWidget):
    """The minimum-viable Time Lens dock (phase 10a).

    Args:
        iface: the QGIS iface (used for the map canvas).
        journal: the plugin's `JournalManager`. A read connection is
            opened lazily on the first Refresh click and closed when
            the dock is closed.
        parent: optional parent widget.
    """

    OBJECT_NAME = "RecoverLandTemporalLensDock"

    def __init__(self, iface, journal, parent=None):
        super().__init__("Time Lens", parent)
        self.setObjectName(self.OBJECT_NAME)
        self.setAllowedAreas(QtCompat.DOCK_AREA_LEFT | QtCompat.DOCK_AREA_RIGHT)

        self._iface = iface
        self._journal = journal
        self._canvas = iface.mapCanvas() if iface is not None else None

        # Selection / tool state.
        self._selected_geom = None              # QgsGeometry in canvas CRS
        self._lens_map_tool: Optional[LensRectangleMapTool] = None
        self._prev_map_tool = None
        self._read_conn = None
        # Phase 10c: keep the last LensRenderPlan so we can resolve
        # entity_fp -> EntityTimeline on click without re-fetching.
        self._last_plan = None

        self._build_ui()
        self._populate_layers_combo()

    # ----- UI construction ----------------------------------------------

    def _build_ui(self) -> None:
        root = QWidget(self)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        layout.addWidget(QLabel(self.tr("Couche audited :")))
        self.layer_combo = QComboBox(self)
        layout.addWidget(self.layer_combo)

        sel_row = QHBoxLayout()
        self.select_button = QPushButton(self.tr("Rectangle"), self)
        self.select_button.setCheckable(True)
        self.select_button.clicked.connect(self._on_select_button)
        sel_row.addWidget(self.select_button)

        self.select_polygon_button = QPushButton(self.tr("Polygone"), self)
        self.select_polygon_button.setCheckable(True)
        self.select_polygon_button.clicked.connect(
            self._on_select_polygon_button
        )
        sel_row.addWidget(self.select_polygon_button)
        layout.addLayout(sel_row)

        layout.addWidget(QLabel(self.tr("Plage temporelle :")))
        self.preset_combo = QComboBox(self)
        for label, _ in _PRESETS:
            self.preset_combo.addItem(self.tr(label))
        self.preset_combo.setCurrentIndex(_DEFAULT_PRESET_INDEX)
        self.preset_combo.currentIndexChanged.connect(self._on_preset_changed)
        layout.addWidget(self.preset_combo)

        now_qdt = QDateTime.currentDateTime()
        date_row = QHBoxLayout()
        date_row.addWidget(QLabel(self.tr("De :")))
        self.t_min_input = QgsDateTimeEdit()
        self.t_min_input.setDisplayFormat("dd/MM/yyyy HH:mm")
        self.t_min_input.dateTimeChanged.connect(self._update_legend_age)
        date_row.addWidget(self.t_min_input)
        layout.addLayout(date_row)

        date_row2 = QHBoxLayout()
        date_row2.addWidget(QLabel(self.tr("A :")))
        self.t_max_input = QgsDateTimeEdit()
        self.t_max_input.setDisplayFormat("dd/MM/yyyy HH:mm")
        self.t_max_input.setDateTime(now_qdt)
        date_row2.addWidget(self.t_max_input)
        layout.addLayout(date_row2)

        layout.addWidget(QLabel(self.tr("Filtre operation :")))
        self.op_combo = QComboBox(self)
        self.op_combo.addItem(self.tr("Tout"), LensOpFilter.ALL.value)
        self.op_combo.addItem(self.tr("Insertions uniquement"), LensOpFilter.INSERT_ONLY.value)
        self.op_combo.addItem(self.tr("Mises a jour uniquement"), LensOpFilter.UPDATE_ONLY.value)
        self.op_combo.addItem(self.tr("Suppressions uniquement"), LensOpFilter.DELETE_ONLY.value)
        self.op_combo.addItem(self.tr("Attributs seuls"), LensOpFilter.ATTR_ONLY.value)
        self.op_combo.addItem(self.tr("Geometrie seule"), LensOpFilter.GEOM_ONLY.value)
        layout.addWidget(self.op_combo)

        self.refresh_button = QPushButton(self.tr("Rafraichir"), self)
        self.refresh_button.setEnabled(False)
        self.refresh_button.clicked.connect(self._on_refresh_button)
        layout.addWidget(self.refresh_button)

        self.status_label = QLabel(self.tr("Aucune selection."), self)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        layout.addWidget(QLabel(self.tr("Legende :")))
        self.legend_widget = self._build_legend_widget()
        layout.addWidget(self.legend_widget)
        self.legend_age_label = QLabel(self.tr("Plage non definie."), self)
        self.legend_age_label.setWordWrap(True)
        layout.addWidget(self.legend_age_label)

        layout.addWidget(QLabel(self.tr("Entites :")))
        self.entity_list = QListWidget(self)
        self.entity_list.setMaximumHeight(150)
        self.entity_list.itemClicked.connect(self._on_entity_clicked)
        layout.addWidget(self.entity_list)

        self.diff_panel = QFrame(self)
        self.diff_panel.setFrameShape(QtCompat.HLINE)
        diff_v = QVBoxLayout(self.diff_panel)
        diff_v.setContentsMargins(0, 4, 0, 4)
        diff_v.setSpacing(2)
        self.diff_label = QLabel(
            self.tr("Selectionnez une entite pour voir le diff."), self
        )
        self.diff_label.setWordWrap(True)
        diff_v.addWidget(self.diff_label)
        self.diff_table = QTableWidget(0, 5, self)
        self.diff_table.setHorizontalHeaderLabels(
            [self.tr("Date"), self.tr("Op"), self.tr("Champ"),
             self.tr("Ancien"), self.tr("Nouveau")]
        )
        self.diff_table.horizontalHeader().setStretchLastSection(True)
        self.diff_table.setEditTriggers(QtCompat.NO_EDIT_TRIGGERS)
        self.diff_table.setAlternatingRowColors(True)
        self.diff_table.setMaximumHeight(200)
        diff_v.addWidget(self.diff_table)
        self.diff_panel.hide()
        layout.addWidget(self.diff_panel)

        layout.addStretch(1)

        self.disable_button = QPushButton(self.tr("Desactiver Lens"), self)
        self.disable_button.clicked.connect(self._on_disable_button)
        layout.addWidget(self.disable_button)

        self.setWidget(root)
        # Apply the default preset NOW so the t_min/t_max widgets are
        # populated coherently before the user clicks Rafraichir.
        self._on_preset_changed(_DEFAULT_PRESET_INDEX)

    def _build_legend_widget(self) -> QFrame:
        """Static 5-row legend (color swatch + label).

        Acceptance section 1.6 of BL-IL-P0-10. The dynamic *"il y a Xj"*
        label sits BELOW the swatches and is refreshed by
        :meth:`_update_legend_age` every time t_min changes.
        """
        frame = QFrame(self)
        frame.setFrameShape(QtCompat.HLINE)  # subtle separator look
        v = QVBoxLayout(frame)
        v.setContentsMargins(0, 4, 0, 4)
        v.setSpacing(2)
        self._legend_swatches = []
        for code, color_hex, descr in _LEGEND_PALETTE:
            row = QHBoxLayout()
            swatch = QLabel(self)
            swatch.setFixedSize(16, 16)
            swatch.setStyleSheet(
                f"background-color: {color_hex}; border: 1px solid #555;"
            )
            row.addWidget(swatch)
            row.addWidget(QLabel(f"{code} ({self.tr(descr)})", self))
            row.addStretch(1)
            v.addLayout(row)
            self._legend_swatches.append(swatch)
        return frame

    # ----- combobox population ------------------------------------------

    def _populate_layers_combo(self) -> None:
        """Fill the combobox with audited layers only (acceptance section 3).

        For each `QgsMapLayer` in the project, compute its datasource
        fingerprint via `core.identity.compute_datasource_fingerprint`
        and look it up in `datasource_registry`. Hit = audited.
        """
        from ..core.identity import compute_datasource_fingerprint
        from ..core.datasource_registry import lookup_datasource

        self.layer_combo.clear()
        if self._journal is None or not self._journal.is_open:
            self.layer_combo.addItem(self.tr("(Journal non ouvert)"), None)
            self.layer_combo.setEnabled(False)
            return

        try:
            conn = self._journal.create_read_connection()
        except Exception as exc:  # noqa: BLE001
            _flog(
                f"lens_dock event=populate_failed type={type(exc).__name__}",
                "WARNING",
            )
            self.layer_combo.addItem(self.tr("(Erreur d'ouverture du journal)"), None)
            self.layer_combo.setEnabled(False)
            return

        n_audited = 0
        try:
            for lyr in QgsProject.instance().mapLayers().values():
                # Skip our own overlays.
                if lyr.name().startswith("__rl_lens_"):
                    continue
                if not hasattr(lyr, "source"):
                    continue
                try:
                    fp = compute_datasource_fingerprint(lyr)
                except Exception:  # noqa: BLE001
                    continue
                info = lookup_datasource(conn, fp)
                if info is None:
                    continue
                # Store layer.id() so we can resolve back to the live
                # QgsMapLayer at Refresh time (compatible avec
                # acceptance section 3 strict).
                self.layer_combo.addItem(
                    f"{lyr.name()}  ({info.provider_type})",
                    {
                        "layer_id": lyr.id(),
                        "datasource_fp": info.fingerprint,
                        "crs_authid": info.crs_authid,
                    },
                )
                n_audited += 1
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

        if n_audited == 0:
            self.layer_combo.addItem(self.tr("(Aucune couche audited dans le projet)"), None)
            self.layer_combo.setEnabled(False)
        else:
            self.layer_combo.setEnabled(True)
        _flog(
            f"lens_dock event=populate_done n_audited={n_audited}",
            "INFO" if n_audited else "DEBUG",
        )

    # ----- selection workflow -------------------------------------------

    def _on_select_button(self, checked: bool) -> None:
        self._activate_map_tool(
            checked,
            LensRectangleMapTool,
            self.select_button,
            self.select_polygon_button,
            self.tr("Tracez un rectangle sur la carte (clic-glisser-relacher)."),
        )

    def _on_select_polygon_button(self, checked: bool) -> None:
        self._activate_map_tool(
            checked,
            LensPolygonMapTool,
            self.select_polygon_button,
            self.select_button,
            self.tr("Tracez un polygone (clic = sommet, double-clic = valider, Esc = annuler)."),
        )

    def _on_map_selection_completed(self, geom) -> None:
        self._selected_geom = geom
        self.refresh_button.setEnabled(True)
        for btn in (self.select_button, self.select_polygon_button):
            btn.blockSignals(True)
            btn.setChecked(False)
            btn.blockSignals(False)
        self._restore_previous_map_tool()
        bbox = geom.boundingBox()
        self.status_label.setText(
            self.tr(
                "Zone selectionnee: x=[{xmin}, {xmax}], "
                "y=[{ymin}, {ymax}]. "
                "Cliquez Rafraichir."
            ).format(
                xmin=f"{bbox.xMinimum():.1f}",
                xmax=f"{bbox.xMaximum():.1f}",
                ymin=f"{bbox.yMinimum():.1f}",
                ymax=f"{bbox.yMaximum():.1f}",
            )
        )

    def _restore_previous_map_tool(self) -> None:
        if self._canvas is not None and self._prev_map_tool is not None:
            self._canvas.setMapTool(self._prev_map_tool)
        if self._lens_map_tool is not None:
            try:
                self._lens_map_tool.selection_completed.disconnect(
                    self._on_map_selection_completed,
                )
            except (TypeError, RuntimeError):
                pass
            self._lens_map_tool = None
        self._prev_map_tool = None

    # ----- refresh = plan + render --------------------------------------

    def _on_refresh_button(self) -> None:
        import uuid as _uuid
        from ..core.event_stream_repository import fetch_events_in_zone
        from ..core.lens_contracts import LensFetchStats, LensSelection
        from ..core.workflow_service import execute_grouped_lens_view

        # 10c: every refresh wipes the entity list and the diff panel so
        # the user never sees stale clickable entries from the previous
        # selection while the new fetch is still in flight.
        self._clear_entity_panels()

        data = self.layer_combo.currentData()
        if data is None or self._selected_geom is None:
            self.status_label.setText(self.tr("Selection incomplete."))
            return
        if self._journal is None or not self._journal.is_open:
            self.status_label.setText(self.tr("Journal non ouvert."))
            return

        layer = QgsProject.instance().mapLayer(data["layer_id"])
        if layer is None:
            self.status_label.setText(self.tr("Couche disparue du projet."))
            return

        src_crs_authid = (
            self._canvas.mapSettings().destinationCrs().authid()
            if self._canvas is not None
            else "EPSG:3857"
        )
        dst_crs_authid_render = src_crs_authid  # render in canvas CRS
        bbox_crs_storage = data.get("crs_authid") or src_crs_authid

        # Reproject the canvas-CRS bbox to the storage CRS of the audit
        # geometries. fetch_events_in_zone does strict AABB filtering
        # against the stored WKB envelope, so the bbox MUST be in that
        # CRS. The renderer reprojects the resulting geometries back to
        # the canvas CRS.
        geom_in_storage = QgsGeometry(self._selected_geom)
        if bbox_crs_storage and bbox_crs_storage != src_crs_authid:
            transform = QgsCoordinateTransform(
                QgsCoordinateReferenceSystem(src_crs_authid),
                QgsCoordinateReferenceSystem(bbox_crs_storage),
                QgsProject.instance(),
            )
            try:
                geom_in_storage.transform(transform)
            except Exception as exc:  # noqa: BLE001
                self.status_label.setText(
                    self.tr("Reprojection bbox echec: {error_type}").format(
                        error_type=type(exc).__name__
                    ),
                )
                return
        bbox = geom_in_storage.boundingBox()
        bbox_xy = (
            bbox.xMinimum(), bbox.yMinimum(),
            bbox.xMaximum(), bbox.yMaximum(),
        )

        t_min = self._get_iso_from_qdt(self.t_min_input)
        t_max = self._get_iso_from_qdt(self.t_max_input)
        op_filter_value = self.op_combo.currentData() or LensOpFilter.ALL.value
        op_filter = LensOpFilter(op_filter_value)
        _flog(
            f"lens_dock event=refresh_clicked layer={layer.name()} "
            f"t_min={t_min} t_max={t_max} op_filter={op_filter.value}",
            "INFO",
        )

        selection = LensSelection(
            layer_id_snapshot=data["layer_id"],
            datasource_fp=data["datasource_fp"],
            bbox_xy=bbox_xy,
            bbox_crs=bbox_crs_storage,
            t_min=t_min,
            t_max=t_max,
            op_filter=op_filter,
        )

        trace_id = _uuid.uuid4().hex[:8]
        try:
            conn = self._journal.create_read_connection()
        except Exception as exc:  # noqa: BLE001
            self.status_label.setText(
                self.tr("Connexion lecture echec: {error_type}").format(
                    error_type=type(exc).__name__
                ),
            )
            return

        try:
            events, fetch_stats = fetch_events_in_zone(
                conn,
                data["datasource_fp"],
                bbox_xy,
                t_min,
                t_max,
                limit=selection.max_events,
                trace_id=trace_id,
            )
        except Exception as exc:  # noqa: BLE001
            _flog(
                f"lens_dock event=fetch_failed trace_id={trace_id} "
                f"type={type(exc).__name__}",
                "WARNING",
            )
            self.status_label.setText(
                self.tr("Lecture journal echec: {error_type}").format(
                    error_type=type(exc).__name__
                ),
            )
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            return

        # Apply op_filter post-fetch (the repository does NOT filter by
        # op_filter; this keeps the planner pure and lets the dock
        # change the filter without re-querying SQL).
        n_pre = len(events)
        if op_filter != LensOpFilter.ALL:
            events = self._filter_events_by_op_filter(events, op_filter)
            fetch_stats = LensFetchStats(
                n_events_total=fetch_stats.n_events_total,
                n_events_returned=len(events),
                n_events_truncated=fetch_stats.n_events_truncated,
                elapsed_ms=fetch_stats.elapsed_ms,
            )
        _flog(
            f"lens_dock event=op_filter_applied trace_id={trace_id} "
            f"op_filter={op_filter.value} n_pre={n_pre} n_post={len(events)}",
            "DEBUG" if op_filter == LensOpFilter.ALL else "INFO",
        )

        try:
            outcome = execute_grouped_lens_view(
                events,
                selection,
                layer.name(),
                fetch_stats,
                dst_crs_authid_render,
                trace_id=trace_id,
                source_layer=layer,
            )
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

        # 10c: keep the plan so _on_entity_clicked can resolve fingerprint
        # -> EntityTimeline; expose render counters via outcome.result.
        self._last_plan = outcome.plan
        result = outcome.result
        self._populate_entity_list(outcome.plan)

        if result.n_entities == 0:
            self.status_label.setText(
                self.tr("Aucune modification trouvee dans cette zone et cette fenetre.")
            )
        else:
            truncated_note = (
                self.tr(" ({n} tronques)").format(n=result.n_events_truncated)
                if result.n_events_truncated > 0
                else ""
            )
            self.status_label.setText(
                self.tr(
                    "{n_entities} entites - "
                    "{n_events} events affiches{truncated} "
                    "({elapsed} ms, trace={trace})"
                ).format(
                    n_entities=result.n_entities,
                    n_events=result.n_events_displayed,
                    truncated=truncated_note,
                    elapsed=result.elapsed_ms,
                    trace=trace_id,
                )
            )
        _flog(
            f"lens_dock event=refresh_done trace_id={trace_id} "
            f"n_entities={result.n_entities} "
            f"n_events_displayed={result.n_events_displayed} "
            f"n_truncated={result.n_events_truncated} "
            f"op_filter={op_filter.value}",
            "INFO",
        )

    # ----- helpers (phase 10b) ------------------------------------------

    def _activate_map_tool(self, checked, tool_cls, self_button,
                           other_button, hint_message):
        """Mutually exclusive activation of rectangle / polygon map tool.

        Acceptance section 1.2 (BL-IL-P0-10): the user can only have ONE
        Lens selection tool active at a time. Switching from rectangle
        to polygon (or vice versa) must restore the previous canvas
        map tool cleanly before installing the new one.
        """
        other_button.blockSignals(True)
        other_button.setChecked(False)
        other_button.blockSignals(False)
        if not checked:
            self._restore_previous_map_tool()
            self.status_label.setText(self.tr("Selection annulee."))
            return
        if self.layer_combo.currentData() is None:
            self.status_label.setText(self.tr("Choisissez d'abord une couche auditee."))
            self_button.setChecked(False)
            return
        if self._canvas is None:
            self.status_label.setText(self.tr("Canvas QGIS indisponible."))
            self_button.setChecked(False)
            return
        if self._lens_map_tool is not None:
            self._restore_previous_map_tool()
        self._prev_map_tool = self._canvas.mapTool()
        self._lens_map_tool = tool_cls(self._canvas)
        self._lens_map_tool.selection_completed.connect(
            self._on_map_selection_completed,
        )
        self._canvas.setMapTool(self._lens_map_tool)
        self.status_label.setText(hint_message)
        data = self.layer_combo.currentData() or {}
        _flog(
            f"lens_dock event=lens_activated tool={tool_cls.__name__} "
            f"layer_id={data.get('layer_id', '')}",
            "INFO",
        )

    def _filter_events_by_op_filter(self, events, op_filter):
        """Apply :class:`LensOpFilter` to a list of ``AuditEvent`` (pure).

        ATTR_ONLY = UPDATE event whose geometry did NOT change.
        GEOM_ONLY = UPDATE event whose geometry DID change.
        Both predicates fall back to comparing ``geometry_wkb`` and
        ``new_geometry_wkb`` raw bytes.
        """
        if op_filter == LensOpFilter.ALL:
            return events
        out = []
        for ev in events:
            op = ev.operation_type
            if op_filter == LensOpFilter.INSERT_ONLY and op == "INSERT":
                out.append(ev)
            elif op_filter == LensOpFilter.UPDATE_ONLY and op == "UPDATE":
                out.append(ev)
            elif op_filter == LensOpFilter.DELETE_ONLY and op == "DELETE":
                out.append(ev)
            elif op_filter == LensOpFilter.ATTR_ONLY and op == "UPDATE":
                new_wkb = getattr(ev, "new_geometry_wkb", None)
                if new_wkb is None or new_wkb == ev.geometry_wkb:
                    out.append(ev)
            elif op_filter == LensOpFilter.GEOM_ONLY and op == "UPDATE":
                new_wkb = getattr(ev, "new_geometry_wkb", None)
                if new_wkb is not None and new_wkb != ev.geometry_wkb:
                    out.append(ev)
        return out

    def _get_iso_from_qdt(self, qdt_widget) -> str:
        """Convert a ``QgsDateTimeEdit`` value to an ISO UTC string."""
        qdt = qdt_widget.dateTime()
        ms = qdt.toMSecsSinceEpoch()
        py_dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return py_dt.isoformat()

    def _on_preset_changed(self, index: int) -> None:
        """Apply a DateRange preset to the t_min / t_max widgets.

        Acceptance 1.3 (BL-IL-P0-10). "Personnalise" leaves the date
        widgets untouched so the user can craft any window.
        """
        if not (0 <= index < len(_PRESETS)):
            return
        _label, days = _PRESETS[index]
        if days is None:
            return
        now = QDateTime.currentDateTime()
        if days == 0:
            time_of_day = now.time()
            seconds_since_midnight = (
                time_of_day.hour() * 3600
                + time_of_day.minute() * 60
                + time_of_day.second()
            )
            t_min_qdt = now.addSecs(-seconds_since_midnight)
        else:
            t_min_qdt = now.addDays(-days)
        self.t_min_input.blockSignals(True)
        self.t_max_input.blockSignals(True)
        self.t_min_input.setDateTime(t_min_qdt)
        self.t_max_input.setDateTime(now)
        self.t_min_input.blockSignals(False)
        self.t_max_input.blockSignals(False)
        self._update_legend_age()

    def _update_legend_age(self) -> None:
        """Refresh the *Depuis :* label below the legend swatches."""
        try:
            t_min_iso = self._get_iso_from_qdt(self.t_min_input)
            rel = format_relative_time(t_min_iso)
            absolute = self.t_min_input.dateTime().toString("dd/MM/yyyy HH:mm")
            self.legend_age_label.setText(
                self.tr("Depuis : {rel} ({absolute})").format(
                    rel=rel, absolute=absolute
                )
            )
        except Exception:  # noqa: BLE001
            self.legend_age_label.setText(self.tr("Plage non definie."))

    # ----- entity list + diff panel (phase 10c) -------------------------

    def _clear_entity_panels(self) -> None:
        """Reset the clickable entity list and hide the diff panel.

        Called at the start of every refresh and on Desactiver Lens so
        stale entries from a previous selection never linger.
        """
        self.entity_list.clear()
        self.diff_table.setRowCount(0)
        self.diff_label.setText(
            self.tr("Selectionnez une entite pour voir le diff.")
        )
        self.diff_panel.hide()
        self._last_plan = None

    def _populate_entity_list(self, plan) -> None:
        """Fill the entity list from ``plan.entities`` (BL-IL-P0-10c).

        Each item label is *fp8 - classification - N states*; the full
        fingerprint is stashed in ``Qt.UserRole`` so the click handler
        can resolve it back to the matching ``EntityTimeline`` without
        reparsing the label.
        """
        self.entity_list.clear()
        if plan is None or not plan.entities:
            return
        for entity_fp, timeline in plan.entities.items():
            cls_value = (
                timeline.classification.value
                if hasattr(timeline.classification, "value")
                else str(timeline.classification)
            )
            label = self.tr(
                "{fp} - {cls} - {n} states"
            ).format(
                fp=entity_fp[:8],
                cls=cls_value,
                n=len(timeline.states),
            )
            item = QListWidgetItem(label)
            item.setData(QtCompat.USER_ROLE, entity_fp)
            self.entity_list.addItem(item)
        _flog(
            f"lens_dock event=entity_list_populated "
            f"n_entities={len(plan.entities)}",
            "DEBUG",
        )

    def _on_entity_clicked(self, item) -> None:
        """Handler for QListWidget.itemClicked (acceptance 1.8).

        Centres the canvas on the entity's latest geometry and populates
        the diff panel. Bails out silently when ``self._last_plan`` is
        ``None`` (no refresh yet) or when the fingerprint is unknown.
        """
        if self._last_plan is None or item is None:
            return
        entity_fp = item.data(QtCompat.USER_ROLE)
        if not entity_fp:
            return
        timeline = self._last_plan.entities.get(entity_fp)
        if timeline is None:
            _flog(
                f"lens_dock event=entity_click_unknown_fp fp={entity_fp[:8]}",
                "WARNING",
            )
            return
        self._center_canvas_on_entity(entity_fp, timeline)
        self._populate_diff_panel(entity_fp, timeline)
        _flog(
            f"lens_dock event=entity_clicked fp={entity_fp[:8]} "
            f"n_states={len(timeline.states)}",
            "INFO",
        )

    def _center_canvas_on_entity(self, entity_fp, timeline) -> None:
        """Centre the canvas on the most recent state with a geometry.

        Falls back to the previous state if the latest one has no
        geometry (e.g. an attribute-only update). Reprojects from the
        storage CRS to the canvas CRS when needed.
        """
        from ..core.wkb_envelope import parse_envelope  # noqa: PLC0415

        if self._canvas is None or not timeline.states:
            return
        last_geom_wkb = None
        last_state = None
        for st in reversed(timeline.states):
            wkb = st.new_geom_wkb or st.old_geom_wkb
            if wkb is not None:
                last_geom_wkb = wkb
                last_state = st
                break
        if last_geom_wkb is None:
            _flog(
                f"lens_dock event=entity_center_no_geom fp={entity_fp[:8]}",
                "WARNING",
            )
            return
        env = parse_envelope(last_geom_wkb)
        if env is None:
            return
        bbox = QgsRectangle(env[0], env[1], env[2], env[3])
        storage_crs = (
            (last_state.crs_authid if last_state else None)
            or self._last_plan.selection.bbox_crs
        )
        canvas_crs = self._canvas.mapSettings().destinationCrs().authid()
        if storage_crs and canvas_crs and storage_crs != canvas_crs:
            try:
                tr = QgsCoordinateTransform(
                    QgsCoordinateReferenceSystem(storage_crs),
                    QgsCoordinateReferenceSystem(canvas_crs),
                    QgsProject.instance(),
                )
                bbox = tr.transformBoundingBox(bbox)
            except Exception as exc:  # noqa: BLE001
                _flog(
                    f"lens_dock event=entity_center_reproject_failed "
                    f"fp={entity_fp[:8]} type={type(exc).__name__}",
                    "WARNING",
                )
                return
        bbox.scale(1.3)  # 30 % buffer for context around the entity
        self._canvas.setExtent(bbox)
        self._canvas.refresh()
        _flog(
            f"lens_dock event=entity_centered fp={entity_fp[:8]} "
            f"crs_src={storage_crs} crs_dst={canvas_crs}",
            "INFO",
        )

    def _populate_diff_panel(self, entity_fp, timeline) -> None:
        """Fill the diff table with one row per (state, attribute) pair.

        Phase 10c MVP: raw display of ``attrs_delta`` as returned by the
        planner. Schema drift handling (IL-P1-12) is out of scope here:
        if a field name disappeared from the layer, it is still shown
        verbatim.
        """
        rows = []
        for st in timeline.states:
            if not st.attrs_delta:
                continue
            ts = (st.created_at or "")[:19]
            for field, pair in st.attrs_delta.items():
                try:
                    old, new = pair
                except (TypeError, ValueError):
                    old, new = None, pair
                rows.append((ts, st.operation_type, field, old, new))

        self.diff_label.setText(
            self.tr(
                "Diff entite {fp} - {n_states} states - "
                "{n_changes} changements"
            ).format(
                fp=entity_fp[:8],
                n_states=len(timeline.states),
                n_changes=len(rows),
            )
        )
        self.diff_table.setRowCount(len(rows))
        for i, (ts, op, field, old, new) in enumerate(rows):
            self.diff_table.setItem(i, 0, QTableWidgetItem(ts))
            self.diff_table.setItem(i, 1, QTableWidgetItem(op))
            self.diff_table.setItem(i, 2, QTableWidgetItem(str(field)))
            self.diff_table.setItem(
                i, 3,
                QTableWidgetItem(self.tr("(vide)") if old is None else str(old)),
            )
            self.diff_table.setItem(
                i, 4,
                QTableWidgetItem(self.tr("(vide)") if new is None else str(new)),
            )
        self.diff_panel.show()
        _flog(
            f"lens_dock event=diff_panel_populated fp={entity_fp[:8]} "
            f"n_rows={len(rows)}",
            "DEBUG",
        )

    # ----- disable / close ----------------------------------------------

    def _on_disable_button(self) -> None:
        from ..core.workflow_service import purge_lens_overlays
        purge_lens_overlays("disable_lens")
        self._clear_entity_panels()
        self._restore_previous_map_tool()
        self.close()

    def closeEvent(self, event):
        self._restore_previous_map_tool()
        try:
            from ..core.workflow_service import purge_lens_overlays
            purge_lens_overlays("dock_closed")
        except Exception:  # noqa: BLE001
            pass
        super().closeEvent(event)


__all__ = [
    "TemporalLensDock",
]
