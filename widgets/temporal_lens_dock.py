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
from datetime import datetime, timedelta, timezone
from typing import Optional

from qgis.PyQt.QtCore import Qt
from qgis.PyQt.QtWidgets import (
    QComboBox,
    QDockWidget,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)
from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsGeometry,
    QgsProject,
)

from .temporal_lens_map_tool import LensRectangleMapTool


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
        self.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)

        self._iface = iface
        self._journal = journal
        self._canvas = iface.mapCanvas() if iface is not None else None

        # Selection / tool state.
        self._selected_geom = None              # QgsGeometry in canvas CRS
        self._lens_map_tool: Optional[LensRectangleMapTool] = None
        self._prev_map_tool = None
        self._read_conn = None

        self._build_ui()
        self._populate_layers_combo()

    # ----- UI construction ----------------------------------------------

    def _build_ui(self) -> None:
        root = QWidget(self)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        layout.addWidget(QLabel("Couche audited :"))
        self.layer_combo = QComboBox(self)
        layout.addWidget(self.layer_combo)

        btn_row = QHBoxLayout()
        self.select_button = QPushButton("Selectionner rectangle", self)
        self.select_button.setCheckable(True)
        self.select_button.clicked.connect(self._on_select_button)
        btn_row.addWidget(self.select_button)

        self.refresh_button = QPushButton("Rafraichir", self)
        self.refresh_button.setEnabled(False)
        self.refresh_button.clicked.connect(self._on_refresh_button)
        btn_row.addWidget(self.refresh_button)
        layout.addLayout(btn_row)

        self.status_label = QLabel("Aucune selection.", self)
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        layout.addStretch(1)

        self.disable_button = QPushButton("Desactiver Lens", self)
        self.disable_button.clicked.connect(self._on_disable_button)
        layout.addWidget(self.disable_button)

        self.setWidget(root)

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
            self.layer_combo.addItem("(Journal non ouvert)", None)
            self.layer_combo.setEnabled(False)
            return

        try:
            conn = self._journal.create_read_connection()
        except Exception as exc:  # noqa: BLE001
            _flog(
                f"lens_dock event=populate_failed type={type(exc).__name__}",
                "WARNING",
            )
            self.layer_combo.addItem("(Erreur d'ouverture du journal)", None)
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
            self.layer_combo.addItem("(Aucune couche audited dans le projet)", None)
            self.layer_combo.setEnabled(False)
        else:
            self.layer_combo.setEnabled(True)
        _flog(
            f"lens_dock event=populate_done n_audited={n_audited}",
            "INFO" if n_audited else "DEBUG",
        )

    # ----- selection workflow -------------------------------------------

    def _on_select_button(self, checked: bool) -> None:
        if not checked:
            # User unchecked: cancel selection in flight.
            self._restore_previous_map_tool()
            self.status_label.setText("Selection annulee.")
            return

        if self.layer_combo.currentData() is None:
            self.status_label.setText("Choisissez d'abord une couche auditee.")
            self.select_button.setChecked(False)
            return

        if self._canvas is None:
            self.status_label.setText("Canvas QGIS indisponible.")
            self.select_button.setChecked(False)
            return

        self._prev_map_tool = self._canvas.mapTool()
        self._lens_map_tool = LensRectangleMapTool(self._canvas)
        self._lens_map_tool.selection_completed.connect(
            self._on_map_selection_completed,
        )
        self._canvas.setMapTool(self._lens_map_tool)
        self.status_label.setText(
            "Tracez un rectangle sur la carte (clic-glisser-relacher).",
        )

    def _on_map_selection_completed(self, geom) -> None:
        self._selected_geom = geom
        self.refresh_button.setEnabled(True)
        self.select_button.setChecked(False)
        self._restore_previous_map_tool()
        bbox = geom.boundingBox()
        self.status_label.setText(
            f"Zone selectionnee: x=[{bbox.xMinimum():.1f}, {bbox.xMaximum():.1f}], "
            f"y=[{bbox.yMinimum():.1f}, {bbox.yMaximum():.1f}]. "
            "Cliquez Rafraichir."
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
        from ..core.lens_contracts import LensOpFilter, LensSelection
        from ..core.workflow_service import execute_grouped_lens_view

        data = self.layer_combo.currentData()
        if data is None or self._selected_geom is None:
            self.status_label.setText("Selection incomplete.")
            return
        if self._journal is None or not self._journal.is_open:
            self.status_label.setText("Journal non ouvert.")
            return

        layer = QgsProject.instance().mapLayer(data["layer_id"])
        if layer is None:
            self.status_label.setText("Couche disparue du projet.")
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
                    f"Reprojection bbox echec: {type(exc).__name__}",
                )
                return
        bbox = geom_in_storage.boundingBox()
        bbox_xy = (
            bbox.xMinimum(), bbox.yMinimum(),
            bbox.xMaximum(), bbox.yMaximum(),
        )

        now = datetime.now(timezone.utc)
        t_min = (now - timedelta(days=30)).isoformat()
        t_max = (now + timedelta(days=1)).isoformat()

        selection = LensSelection(
            layer_id_snapshot=data["layer_id"],
            datasource_fp=data["datasource_fp"],
            bbox_xy=bbox_xy,
            bbox_crs=bbox_crs_storage,
            t_min=t_min,
            t_max=t_max,
            op_filter=LensOpFilter.ALL,
        )

        trace_id = _uuid.uuid4().hex[:8]
        try:
            conn = self._journal.create_read_connection()
        except Exception as exc:  # noqa: BLE001
            self.status_label.setText(
                f"Connexion lecture echec: {type(exc).__name__}",
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
                f"Lecture journal echec: {type(exc).__name__}",
            )
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            return

        try:
            result = execute_grouped_lens_view(
                events,
                selection,
                layer.name(),
                fetch_stats,
                dst_crs_authid_render,
                trace_id=trace_id,
            )
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

        truncated_note = (
            f" ({result.n_events_truncated} tronques)"
            if result.n_events_truncated > 0
            else ""
        )
        self.status_label.setText(
            f"{result.n_entities} entites - "
            f"{result.n_events_displayed} events affiches{truncated_note} "
            f"({result.elapsed_ms} ms, trace={trace_id})"
        )
        _flog(
            f"lens_dock event=refresh_done trace_id={trace_id} "
            f"n_entities={result.n_entities} "
            f"n_events_displayed={result.n_events_displayed} "
            f"n_truncated={result.n_events_truncated}",
            "INFO",
        )

    # ----- disable / close ----------------------------------------------

    def _on_disable_button(self) -> None:
        from ..core.workflow_service import purge_lens_overlays
        purge_lens_overlays("disable_lens")
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
