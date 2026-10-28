# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Fixture partagee des scenarios Review sur une couche a fid instables
(pas un scenario : prefixe _).

Une couche Shapefile, comme en ont tous les utilisateurs : le fid y est la
position de l'enregistrement. Apres T l'utilisateur deplace B (geometrie
seule), renomme C (attribut seul), puis supprime A ; a l'enregistrement,
OGR recompacte le fichier et B, C, D changent de fid (B 1->0, C 2->1,
D 3->2). Le journal les connait sous leur fid d'avant : ``fid:1`` pour B,
``fid:2`` pour C.

Le vrai plugin sous FakeIface, les vraies editions capturees par le
tracker ; seul le format de la couche change par rapport a _rvw_harness.
"""
from __future__ import annotations

import os
import time

ROWS = [
    ("A", "Alpha", 10.0, 0.0, 0.0),
    ("B", "Beta", 20.0, 1.0, 1.0),
    ("C", "Gamma", 30.0, 2.0, 2.0),
    ("D", "Delta", 40.0, 3.0, 3.0),
]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def make_shp(tmpdir: str, name: str = "parcelles") -> str:
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsVectorLayer,
    )
    from scripts.validation.scenarios._rvw_harness import FIELDS
    uri = "Point?crs=EPSG:2154&" + "&".join(f"field={n}:{t}" for n, t in FIELDS)
    mem = QgsVectorLayer(uri, "seed", "memory")
    feats = []
    for code, nom, surface, x, y in ROWS:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, nom, surface])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, y)))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"{name}.shp")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def load_shp(path: str, display_name: str = "parcelles"):
    from qgis.core import QgsProject, QgsVectorLayer
    lyr = QgsVectorLayer(path, display_name, "ogr")
    if not lyr.isValid():
        raise RuntimeError(f"invalid layer {path}")
    QgsProject.instance().addMapLayer(lyr)
    return lyr


def edit_after_cutoff(plugin, layer, data: dict) -> str:
    """T, then the three user edits; returns T. Fills *data*."""
    h = _h()
    jpath = h.journal_path(plugin)
    rows0 = h.journal_rows(jpath)
    data["fids_before"] = sorted((f.id(), f["code"]) for f in layer.getFeatures())
    cutoff = h.utc_now_iso()
    time.sleep(1.2)
    data["commits"] = [
        h.user_commit(layer, lambda lyr: h.move(lyr, "B", 10.0, 10.0)),
        h.user_commit(layer, lambda lyr: h.rename(lyr, "C", "Gamma2")),
        h.user_commit(layer, lambda lyr: h.delete(lyr, "A")),
    ]
    data["journal_rows"] = h.wait_rows(jpath, rows0 + 3) - rows0
    layer.reload()
    data["fids_after"] = sorted((f.id(), f["code"]) for f in layer.getFeatures())
    data["cutoff"] = cutoff
    return cutoff


def fids_moved(data: dict) -> bool:
    """The precondition: the repack renumbered B, C and D."""
    return (data.get("fids_before") == [(0, "A"), (1, "B"), (2, "C"), (3, "D")]
            and data.get("fids_after") == [(0, "B"), (1, "C"), (2, "D")])
