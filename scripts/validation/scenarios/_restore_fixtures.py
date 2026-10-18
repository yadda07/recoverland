"""Fixtures partagees par les scenarios de preuve de cible (v7).

Couches reelles (shapefile, GeoPackage) ecrites dans un dossier temporaire,
evenements d'audit construits a la main, et une photographie du fichier
relue depuis le disque par une couche NEUVE : c'est ce que l'utilisateur
retrouve en rouvrant sa table attributaire, cache et tampon d'edition
exclus.

Le prefixe ``_`` tient ce module hors de la collecte des scenarios.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def wkb_point(x: float, y: float) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def wkb_wkt(wkt: str) -> bytes:
    from qgis.core import QgsGeometry
    return bytes(QgsGeometry.fromWkt(wkt).asWkb())


def make_shapefile(tmpdir: str, name: str, geom_type: str, fields,
                   rows, crs: str = "EPSG:4326") -> str:
    """Shapefile *name* avec *rows* = [([valeurs...], wkt), ...]."""
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsVectorFileWriter,
        QgsCoordinateTransformContext,
    )

    spec = "&".join(f"field={f}" for f in fields)
    mem = QgsVectorLayer(f"{geom_type}?crs={crs}&{spec}", "seed", "memory")
    mem.startEditing()
    for values, wkt in rows:
        feat = QgsFeature(mem.fields())
        feat.setAttributes(list(values))
        if wkt:
            feat.setGeometry(QgsGeometry.fromWkt(wkt))
        mem.addFeature(feat)
    mem.commitChanges()
    path = str(Path(tmpdir) / f"{name}.shp")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def make_gpkg(tmpdir: str, name: str, geom_wkb_type, fields, rows,
              epsg: int = 4326) -> str:
    """GeoPackage *name* (couche du meme nom) aux FID EXPLICITES.

    *fields* = [(nom, ogr.OFTxxx)], *rows* = [(fid, [valeurs...], wkt)].
    """
    from osgeo import ogr, osr

    path = str(Path(tmpdir) / f"{name}.gpkg")
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    lyr = ds.CreateLayer(name, srs=srs, geom_type=geom_wkb_type)
    for fname, ftype in fields:
        lyr.CreateField(ogr.FieldDefn(fname, ftype))
    for fid, values, wkt in rows:
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetFID(fid)
        for (fname, _t), value in zip(fields, values):
            if value is None:
                feat.SetFieldNull(fname)
            else:
                feat.SetField(fname, value)
        if wkt:
            feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
        lyr.CreateFeature(feat)
        feat = None
    ds.FlushCache()
    ds = None
    return path


def gpkg_delete_and_reuse(path: str, name: str, fid: int, fields,
                          values, wkt: str) -> None:
    """Supprime l'entite *fid* puis en cree une AUTRE sous le meme FID.

    Ce que fait une cle INTEGER PRIMARY KEY sans AUTOINCREMENT, une
    sequence remise a zero, ou un import qui renumerote : la valeur de
    cle designe desormais une entite qui n'a rien a voir.
    """
    from osgeo import ogr

    ds = ogr.Open(path, 1)
    lyr = ds.GetLayerByName(name)
    lyr.DeleteFeature(fid)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetFID(fid)
    for (fname, _t), value in zip(fields, values):
        feat.SetField(fname, value)
    feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
    lyr.CreateFeature(feat)
    feat = None
    ds.FlushCache()
    ds = None


def open_layer(path: str, name: str = ""):
    from qgis.core import QgsVectorLayer
    uri = f"{path}|layername={name}" if path.endswith(".gpkg") else path
    layer = QgsVectorLayer(uri, name or Path(path).stem, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche invalide: {uri}")
    return layer


def file_rows(path: str, name: str = "", with_fid: bool = False) -> list:
    """Contenu du fichier, relu par une couche neuve, trie."""
    layer = open_layer(path, name)
    names = [f.name() for f in layer.fields()]
    out = []
    for feat in layer.getFeatures():
        geom = feat.geometry()
        wkt = geom.asWkt(6) if geom is not None and not geom.isNull() else None
        row = tuple(feat[n] for n in names) + (wkt,)
        out.append(((feat.id(),) + row) if with_fid else row)
    del layer
    return sorted(out, key=repr)


def schema(*fields) -> str:
    """field_schema_json of an event: ``schema(("nom", "string"), ...)``.

    Without it the restore computes an empty field mapping and compares
    nothing: a proof would then fail for want of fields, not for want of
    identity, and prove nothing about the guard under test.
    """
    return json.dumps([{"name": name, "type": ftype} for name, ftype in fields])


def audit_event(**overrides):
    from recoverland.core.audit_backend import AuditEvent

    payload = dict(
        event_id=1,
        project_fingerprint="v7_target_proof_project",
        datasource_fingerprint="v7_target_proof_ds",
        layer_id_snapshot="lyr_v7",
        layer_name_snapshot="cible",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": 0}),
        operation_type="INSERT",
        attributes_json=json.dumps({"all_attributes": {}}),
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="[]",
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 9, 1, 9, 0, 0,
                            tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint="fid:0",
        event_schema_version=5,
        new_geometry_wkb=None,
        invalidated_at=None,
    )
    payload.update(overrides)
    return AuditEvent(**payload)


def not_applied(result) -> bool:
    """Ni ecrit ni annonce ecrit: un refus, ou un saut declare."""
    result = result or {}
    return result.get("success") is False or result.get("skipped") is True


def buffer_apply(layer, comp_op: str, event, trace_id: str) -> dict:
    """Une action compensatoire du rewind, dans un tampon, puis commit."""
    from recoverland.core.restore_executor import _apply_via_buffer

    layer.startEditing()
    try:
        result = dict(_apply_via_buffer(layer, comp_op, event, {},
                                        trace_id=trace_id))
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        result = {"success": False, "crash": repr(exc)}
    if not layer.commitChanges():
        layer.rollBack()
    return result
