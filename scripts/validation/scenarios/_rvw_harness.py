"""Outils partages des scenarios Review (pas un scenario : prefixe _).

Pilote le VRAI RecoverPlugin et le VRAI RecoverDialog sous un faux iface
(scripts/validation/plugin_harness.FakeIface), sur de vraies couches
GeoPackage et le vrai journal SQLite du plugin. Rien du produit n'est
simule : les evenements sont captures par l'EditSessionTracker a partir
d'editions reelles, comme chez l'utilisateur.

Les entites creees dans le GeoPackage AVANT le demarrage du plugin sont
des entites "preexistantes" : le journal ne les a jamais vues naitre, ce
qui est le cas de toutes les donnees d'un utilisateur au premier jour.
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

FIELDS = (("code", "string(10)"), ("nom", "string(40)"), ("surface", "double"))


def pump(seconds: float = 0.0) -> None:
    from scripts.validation.plugin_harness import pump as _pump
    _pump(seconds)


def pump_until(predicate, timeout: float = 8.0, step: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while True:
        pump(0)
        try:
            if predicate():
                return True
        except Exception:  # noqa: BLE001 - a predicate on a dying object
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(step)


def utc_now_iso() -> str:
    """Cutoff at second granularity, as the date bar emits it (UTC)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def make_gpkg(tmpdir: str, name: str, rows, layer_name: str = "parcelles") -> str:
    """GeoPackage of points; rows = [(code, nom, surface, x, y)].

    ``x is None`` seeds an entity WITHOUT geometry (SQL NULL in the file).
    """
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsVectorLayer,
    )
    uri = "Point?crs=EPSG:2154&" + "&".join(f"field={n}:{t}" for n, t in FIELDS)
    mem = QgsVectorLayer(uri, "seed", "memory")
    feats = []
    for code, nom, surface, x, y in rows:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, nom, surface])
        if x is not None:
            f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, y)))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = layer_name
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    no_geom = [code for code, _n, _s, x, _y in rows if x is None]
    if no_geom:
        # The writer turns a feature without geometry into POINT EMPTY:
        # the NULL state is set through the provider, as a user file has it.
        lyr = QgsVectorLayer(f"{path}|layername={layer_name}", "seed", "ogr")
        lyr.dataProvider().changeGeometryValues({
            f.id(): QgsGeometry() for f in lyr.getFeatures()
            if f["code"] in no_geom})
        del lyr
    return path


def load_layer(path: str, display_name: str, layer_name: str = "parcelles"):
    from qgis.core import QgsProject, QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername={layer_name}", display_name, "ogr")
    if not lyr.isValid():
        raise RuntimeError(f"invalid layer {path}")
    QgsProject.instance().addMapLayer(lyr)
    return lyr


def fid_of(layer, code: str) -> int:
    for f in layer.getFeatures():
        if f["code"] == code:
            return f.id()
    raise KeyError(code)


def user_commit(layer, action) -> bool:
    """One user edit session: startEditing, action(layer), commitChanges."""
    layer.startEditing()
    action(layer)
    return bool(layer.commitChanges())


def move(layer, code: str, x: float, y: float):
    from qgis.core import QgsGeometry, QgsPointXY
    layer.changeGeometry(fid_of(layer, code),
                         QgsGeometry.fromPointXY(QgsPointXY(x, y)))


def clear_geometry(layer, code: str):
    """The user removes the geometry of an entity (it keeps its row)."""
    from qgis.core import QgsGeometry
    layer.changeGeometry(fid_of(layer, code), QgsGeometry())


def rename(layer, code: str, value: str):
    layer.changeAttributeValue(fid_of(layer, code),
                               layer.fields().indexFromName("nom"), value)


def delete(layer, code: str):
    layer.deleteFeature(fid_of(layer, code))


def add(layer, code: str, nom: str, surface: float, x: float, y: float):
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    f = QgsFeature(layer.fields())
    attrs = [None] * layer.fields().count()
    attrs[layer.fields().indexFromName("code")] = code
    attrs[layer.fields().indexFromName("nom")] = nom
    attrs[layer.fields().indexFromName("surface")] = surface
    f.setAttributes(attrs)
    f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, y)))
    layer.addFeature(f)


def journal_rows(journal_path) -> int:
    if not journal_path or not os.path.isfile(journal_path):
        return -1
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
    finally:
        conn.close()


def wait_rows(journal_path, n: int, timeout: float = 8.0) -> int:
    got = journal_rows(journal_path)
    deadline = time.monotonic() + timeout
    while got < n and time.monotonic() < deadline:
        pump(0.1)
        got = journal_rows(journal_path)
    return got


def start_plugin(tmpdir: str):
    """RecoverPlugin under FakeIface, then the user opens a saved project.

    Opening a project file is the path every user takes: readProject opens
    the journal next to it. Tracking on, project CRS EPSG:2154.
    """
    from qgis.core import QgsCoordinateReferenceSystem, QgsProject, QgsSettings
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface
    project = QgsProject.instance()
    project.clear()
    project.setCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
    project_path = os.path.join(tmpdir, "projet.qgs")
    project.write(project_path)
    project.clear()
    QgsSettings().setValue("RecoverLand/tracking_enabled", True)
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    plugin.initGui()
    pump(0.2)
    project.read(project_path)
    pump(0.4)
    return iface, plugin


def journal_path(plugin):
    j = plugin._journal
    return j.path if j is not None and j.is_open else None


def set_view(iface, xmin, ymin, xmax, ymax) -> None:
    from qgis.core import QgsCoordinateReferenceSystem, QgsRectangle
    canvas = iface.mapCanvas()
    canvas.resize(800, 600)
    canvas.setDestinationCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
    canvas.setExtent(QgsRectangle(xmin, ymin, xmax, ymax))
    pump(0.05)


def open_dialog(plugin, layers):
    """plugin.run() then the layer list the stats worker would fill."""
    from recoverland.core.identity import compute_datasource_fingerprint
    plugin.run()
    dlg = plugin.dlg
    pump(0.3)
    dlg._apply_journal_layers([
        {"name": lyr.name(), "fingerprint": compute_datasource_fingerprint(lyr)}
        for lyr in layers
    ])
    return dlg


def review_idle(dlg) -> bool:
    """No rebuild in flight and the overlays repainted."""
    if dlg._review_snap_worker is not None:
        return False
    snap = dlg._review_snap_session
    if snap is not None and getattr(snap, "is_updating", False):
        return False
    bar = dlg._review_date_bar
    if bar is not None and bar._lbl_status.text() == "⟳":
        return False
    return True


def review_on(dlg, n_layers: int, timeout: float = 10.0) -> bool:
    """User switches to Review mode and clicks the toggle ON."""
    dlg.restore_mode_selector.setMode("review")
    dlg._on_period_mode_changed("review")
    dlg._review_toggle._on_segment(1)
    ok = pump_until(
        lambda: dlg._review_snap_session is not None
        and dlg._review_snap_session.n_layers >= n_layers
        and dlg._review_date_bar is not None, timeout)
    pump(0.4)
    return ok and pump_until(lambda: review_idle(dlg), timeout)


def review_off(dlg) -> None:
    dlg._review_toggle._on_segment(0)
    pump(0.2)


def review_date(dlg, iso: str, timeout: float = 10.0) -> bool:
    """The date bar emits *iso*; wait until the overlays show it."""
    dlg._on_snapshot_date_changed(iso)
    pump(0.1)
    ok = pump_until(lambda: review_idle(dlg), timeout)
    pump(0.3)
    return ok


def overlay_layers(dlg) -> list:
    from qgis.core import QgsProject
    snap = dlg._review_snap_session
    if snap is None:
        return []
    out = []
    for lid in snap.overlay_layer_ids():
        lyr = QgsProject.instance().mapLayer(lid)
        if lyr is not None:
            out.append(lyr)
    return out


def overlay_rows(dlg) -> dict:
    """{code: {"nom", "surface", "x", "y", "layer"}} over every overlay."""
    rows = {}
    for lyr in overlay_layers(dlg):
        for f in lyr.getFeatures():
            g = f.geometry()
            pt = g.asPoint() if g is not None and not g.isNull() else None
            code = f["code"] if "code" in f.fields().names() else None
            rows[str(code)] = {
                "nom": f["nom"] if "nom" in f.fields().names() else None,
                "surface": f["surface"] if "surface" in f.fields().names() else None,
                "x": pt.x() if pt is not None else None,
                "y": pt.y() if pt is not None else None,
                "layer": lyr.name(),
            }
    return rows


def managed_layers() -> list:
    from qgis.core import QgsProject
    return [lid for lid, lyr in QgsProject.instance().mapLayers().items()
            if lid.startswith("__rl_snap_")
            or lyr.customProperty("_rl_snap_managed") == "1"]


def is_null(value) -> bool:
    if value is None:
        return True
    try:
        from qgis.PyQt.QtCore import QVariant
        if isinstance(value, QVariant) and value.isNull():
            return True
    except Exception:  # noqa: BLE001
        pass
    return str(value) in ("NULL", "")


class MessageBoxRecorder:
    """Replace the static QMessageBox calls: headless, nothing may block."""

    def __init__(self, answer=None):
        self.calls = []
        self._answer = answer
        self._saved = {}

    def __enter__(self):
        from qgis.PyQt.QtWidgets import QMessageBox
        yes = getattr(QMessageBox, "StandardButton", QMessageBox).No
        answer = self._answer if self._answer is not None else yes
        for kind in ("information", "warning", "critical", "question"):
            self._saved[kind] = getattr(QMessageBox, kind)

            def _fake(*args, _kind=kind, **kwargs):
                texts = [str(a) for a in args[1:3]] if len(args) >= 3 else [str(a) for a in args]
                self.calls.append((_kind, " | ".join(texts)))
                return answer
            setattr(QMessageBox, kind, staticmethod(_fake))
        return self

    def __exit__(self, *exc):
        from qgis.PyQt.QtWidgets import QMessageBox
        for kind, fn in self._saved.items():
            setattr(QMessageBox, kind, fn)
        return False

    def texts(self) -> list:
        return [t for _k, t in self.calls]


def shutdown(plugin) -> None:
    from qgis.core import QgsProject
    try:
        plugin.unload()
    finally:
        pump(0.3)
        QgsProject.instance().clear()
        pump(0.2)
