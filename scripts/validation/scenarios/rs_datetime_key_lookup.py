"""rs_datetime_key_lookup - RecoverLand validation runtime.

Invariant: I-3 sans refus injustifie: une cle que la couche detient
designe l'entite, quel que soit le type de sa colonne.
Constat resolu: item 6 du tour executeur.
`restore_service._lookup_identity` construisait son propre filtre avec la
valeur de la cle en LITTERAL TEXTE (``"obs" = '2026-01-15T08:00:00.000Z'``)
au lieu d'utiliser l'aide du module identite
(`identity.composite_key_filter`), qui compare une colonne date-heure
comme un INSTANT (``to_datetime(...)`` avec son decalage).

Le mecanisme
------------
Un fournisseur qui compile les filtres en SQL (OGR: GeoPackage, SQLite,
PostgreSQL) compare le litteral texte au TEXTE qu'il stocke. Une
GeoPackage stocke une date-heure sous sa forme UTC sans millisecondes
(``2026-01-15T08:00:00Z``): le litteral du journal ne l'egale jamais, la
recherche par cle ne trouve rien, alors que la ligne est la. Sur une
table dont la cle est une date-heure (releves horodates, table
partitionnee par date), toutes les restaurations repondaient donc "cible
introuvable": modification non restauree, annulation d'ajout refusee
("inserted feature not found").

Ce que l'utilisateur voit
-------------------------
Sur sa table de releves, il ne peut ni annuler une correction de valeur
ni supprimer un releve saisi par erreur: RecoverLand affirme que la
ligne n'existe pas, alors qu'elle est affichee dans la table attributaire.

Cas (vraie GeoPackage, fournisseur OGR reel qui compile le filtre; la
cle est declaree sur la colonne date-heure par un mandataire du
fournisseur, comme le fait un fournisseur base de donnees)
------------------------------------------------------------------------
    K1  restauration de 'nom: avant -> apres' sur la ligne de cle
        2026-01-15T08:00Z: la ligne est trouvee par sa cle, 'avant'
        revient (STRONG)
    K2  annulation de l'ajout de la ligne de cle 2026-01-15T09:00Z: la
        ligne est trouvee et supprimee
    K3  temoin: cle absente (aucune ligne a 2026-01-15T10:00Z): refus
        nomme, rien d'ecrit

Verdict avant correctif: FAIL (K1 "Target feature not found", K2
"target_absent"). Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_datetime_key_lookup"
INVARIANT = "I-3"


class PkProvider:
    """Le fournisseur OGR reel, sa cle declaree sur *pk_indexes*."""

    def __init__(self, inner, pk_indexes):
        self._inner = inner
        self._pk = list(pk_indexes)

    def pkAttributeIndexes(self):
        return list(self._pk)

    def __getattr__(self, item):
        return getattr(self._inner, item)


class PkLayer:
    """La couche reelle vue a travers `PkProvider`."""

    def __init__(self, layer, pk_indexes):
        self._layer = layer
        self._provider = PkProvider(layer.dataProvider(), pk_indexes)

    def dataProvider(self):
        return self._provider

    def __getattr__(self, item):
        return getattr(self._layer, item)


def _qdt(hour):
    from qgis.PyQt.QtCore import QDateTime, QDate, QTime, QTimeZone
    return QDateTime(QDate(2026, 1, 15), QTime(hour, 0, 0),
                     QTimeZone(QTimeZone.Initialization.UTC))


def _gpkg(tmp):
    from qgis.core import (QgsVectorLayer, QgsVectorFileWriter, QgsFeature,
                           QgsCoordinateTransformContext, QgsGeometry,
                           QgsPointXY)
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=obs:datetime"
                         "&field=nom:string(20)", "seed", "memory")
    path = str(Path(tmp) / "releves.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "releves"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    layer = QgsVectorLayer(f"{path}|layername=releves", "releves", "ogr")
    feats = []
    for i, (hour, nom) in enumerate(((8, "apres"), (9, "neuf"),
                                     (11, "autre"))):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("obs", _qdt(hour))
        feat.setAttribute("nom", nom)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1.0 + i, 45.0)))
        feats.append(feat)
    if not layer.dataProvider().addFeatures(feats)[0]:
        raise RuntimeError("seed impossible")
    layer.reload()
    return path, layer


def _event(layer, op, identity, attributes, geom=None):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.serialization import serialize_field_schema
    return AuditEvent(
        event_id=5, project_fingerprint="rs_dtk_project",
        datasource_fingerprint="rs_dtk_ds", layer_id_snapshot=layer.id(),
        layer_name_snapshot="releves", provider_type="ogr",
        feature_identity_json=identity, operation_type=op,
        attributes_json=json.dumps(attributes), geometry_wkb=geom,
        geometry_type="Point", crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id=None,
        created_at="2026-09-01T09:00:00+00:00", restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(identity),
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)


def _rows(path):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=releves", "r", "ogr")
    out = sorted((f["obs"].toUTC().toString("HH:mm"), f["nom"])
                 for f in layer.getFeatures())
    del layer
    return out


def _safe(fn, *args):
    try:
        return dict(fn(*args))
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        return {"success": None, "crash": repr(exc)}


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rs_dtk_")
    flog(f"rs_datetime_key_lookup setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.identity import compute_feature_identity
    from recoverland.core.serialization import (serialize_attributes,
                                                serialize_value)
    from recoverland.core.restore_service import (restore_updated_feature,
                                                  restore_inserted_feature)
    out = {}
    try:
        path, raw = _gpkg(ctx.data["tmp"])
        obs_idx = raw.fields().indexOf("obs")
        layer = PkLayer(raw, [obs_idx])
        names = [f.name() for f in raw.fields()]
        feats = {f["nom"]: f for f in raw.getFeatures()}
        out["before"] = _rows(path)

        # K1: 'nom: avant -> apres' on the 08:00 row (already at 'apres').
        ident8 = compute_feature_identity(layer, feats["apres"])
        out["ident8"] = ident8
        upd = _event(raw, "UPDATE", ident8, {"changed_only": {
            "nom": {"old": "avant", "new": "apres"}}})
        out["k1"] = _safe(restore_updated_feature, layer, upd)
        raw.reload()
        out["k1_rows"] = _rows(path)

        # K2: undo of the INSERT of the 09:00 row.
        ident9 = compute_feature_identity(layer, feats["neuf"])
        snap = serialize_attributes(feats["neuf"], names)
        ins = _event(raw, "INSERT", ident9, {"all_attributes": snap},
                     geom=bytes(feats["neuf"].geometry().asWkb()))
        out["k2"] = _safe(restore_inserted_feature, layer, ins)
        raw.reload()
        out["k2_rows"] = _rows(path)

        # K3: a key nobody holds.
        ident10 = json.dumps({"fid": 99, "pk_field": "obs",
                              "pk_value": serialize_value(_qdt(10))})
        upd10 = _event(raw, "UPDATE", ident10, {"changed_only": {
            "nom": {"old": "x", "new": "y"}}})
        out["k3"] = _safe(restore_updated_feature, layer, upd10)
        raw.reload()
        out["k3_rows"] = _rows(path)
        del layer, raw
    except Exception as exc:  # noqa: BLE001
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    ctx.data["out"] = out
    flog(f"rs_datetime_key_lookup run: trace_id={ctx.trace_id} "
         f"k1={out.get('k1')} k2={out.get('k2')} k3={out.get('k3')}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = [("no_crash", "error" not in out,
            f"{out.get('error')} {out.get('trace', '')}")]
    k1 = out.get("k1") or {}
    res.append((
        "k1_update_found_by_its_datetime_key",
        k1.get("success") is True
        and ("08:00", "avant") in (out.get("k1_rows") or []),
        f"resultat={k1} lignes={out.get('k1_rows')} identite="
        f"{out.get('ident8')}: la ligne de cle 08:00 est dans la couche; un "
        f"litteral texte compare au texte stocke par la GeoPackage ne la "
        f"trouve pas et la restauration repond 'cible introuvable'.",
    ))
    k2 = out.get("k2") or {}
    res.append((
        "k2_insert_undo_found_by_its_datetime_key",
        k2.get("success") is True
        and all(h != "09:00" for h, _n in (out.get("k2_rows") or [])),
        f"resultat={k2} lignes={out.get('k2_rows')}: la ligne ajoutee a "
        f"09:00 doit etre trouvee par sa cle et supprimee.",
    ))
    k3 = out.get("k3") or {}
    res.append((
        "k3_absent_key_is_a_named_refusal",
        k3.get("success") is False
        and out.get("k3_rows") == out.get("k2_rows"),
        f"resultat={k3}: une cle que personne ne detient ne designe rien, "
        f"rien ne doit etre ecrit.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_datetime_key_lookup run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
