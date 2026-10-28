# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_composite_pk_identity - RecoverLand validation runtime.

Invariant: I-3 (une identite d'entite designe UNE entite ; une ecriture
destructive exige une cible prouvee et unique ; aucun succes n'est annonce
pour une restauration qui n'a pas eu lieu).

Le trou que ce scenario prouve
------------------------------
`identity.compute_feature_identity` parcourait `pkAttributeIndexes()` et
s'arretait a la PREMIERE colonne non nulle de la cle primaire. Une cle
composite `PRIMARY KEY (zone_id, parcel_no)` (obligatoire sur une table
partitionnee PostGIS, courante sur une vue) etait donc journalisee comme
`{"pk_field": "zone_id", "pk_value": 12}` : une "identite" partagee par
toutes les parcelles de la zone 12, et l'empreinte d'entite `pk:zone_id=12`
avec elle. Tous les chemins de restauration font ensuite confiance a cette
paire sans relire la ligne :

  * restaurer l'UPDATE de (12,5) ecrit l'ancien nom sur la premiere ligne
    de la zone 12 venue, (12,1), et repond "Reverted" ;
  * annuler l'INSERT de (12,9) supprime (12,1) ;
  * restaurer le DELETE de (12,7) repond "Already exists (skip)" des
    qu'une autre parcelle de la zone 12 existe : succes annonce, rien
    restaure, et la trace ecrite neutralise l'evenement pour toujours ;
  * le rewind range INSERT(12,9) et DELETE(12,7) dans la MEME chaine
    d'entite et les annule l'un par l'autre : ni l'un ni l'autre n'est
    compense.

Le dispositif : une vraie couche QGIS (provider memory) dont le provider
est presente comme `postgres` avec une cle (zone_id, parcel_no), exactement
ce que QGIS expose pour une table PostGIS a cle composite. Les evenements
sont construits par les fonctions de capture reelles et rejoues par les
fonctions de restauration reelles de `restore_service` et par
`rewind_dedup`.

Cle composite a colonne DATE ou DATE-HEURE (second tour)
--------------------------------------------------------
Une table partitionnee par date porte sa colonne de partition dans sa
cle : ``PRIMARY KEY (id, obs_date)``, le cas meme que cite `identity`.
La valeur de chaque colonne etait journalisee par ``str()`` : pour une
date, le repr Python ``PyQt6.QtCore.QDate(2026, 1, 15)``. Le filtre de
cle ``"obs" = 'PyQt6.QtCore.QDate(2026, 1, 15)'`` ne designe aucune ligne :
chaque restauration sur une telle table repondait "Target feature not
found" alors que la ligne etait la, et l'empreinte d'entite changeait
avec la liaison PyQt (QGIS 3 / QGIS 4). Mesure ici sur des cles (id,
date) et (id, date-heure) : identite en texte journal, filtre qui
designe LA ligne, restauration d'un UPDATE reussie sur sa ligne, DELETE
restaure ou refuse en le nommant, repr deja journalise (PyQt6 et PyQt5)
decode par `effective_identity`, repr illisible (decalage perdu) retire
et nomme au lieu d'etre cherche.

Verdict attendu avant correctif : FAIL (identite tronquee, ecritures sur
la mauvaise ligne, faux "Already exists", chaines fusionnees ; au second
tour : cle temporelle en repr, "Target feature not found").
Verdict attendu apres correctif : PASS pour les evenements captures en v7.
Les evenements DEJA journalises avec la premiere colonne seule sont mesures
de bout en bout par `rw_identity_serial_consumer_wiring` (cablage de
`effective_identity` dans les chemins de restauration) ; ici, seul le
retrait de la paire non prouvee par `effective_identity` est verifie.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_composite_pk_identity"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_URI = ("Point?crs=EPSG:2154&field=zone_id:integer&field=parcel_no:integer"
        "&field=nom:string(40)")
# (zone_id, parcel_no, nom, x) : quatre parcelles de la zone 12.
_ROWS = [(12, 1, "parcelle un", 1.0), (12, 5, "b", 5.0),
         (12, 7, "parcelle sept", 7.0), (12, 9, "parcelle neuf", 9.0)]
_T0 = datetime(2026, 9, 1, 8, 0, 0, tzinfo=timezone.utc)


class PkProvider:
    """Le provider reel, avec la cle composite et le nom d'un provider DB."""

    def __init__(self, inner, pk_indexes, name):
        self._inner = inner
        self._pk = list(pk_indexes)
        self._name = name

    def pkAttributeIndexes(self):
        return list(self._pk)

    def name(self):
        return self._name

    def __getattr__(self, item):
        return getattr(self._inner, item)


class PkLayer:
    """La couche reelle vue a travers `PkProvider`."""

    def __init__(self, layer, pk_indexes=(0, 1), name="postgres"):
        self._layer = layer
        self._provider = PkProvider(layer.dataProvider(), pk_indexes, name)

    def dataProvider(self):
        return self._provider

    def __getattr__(self, item):
        return getattr(self._layer, item)


def _make_layer():
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY

    mem = QgsVectorLayer(_URI, "parcelles_zone", "memory")
    if not mem.isValid():
        raise RuntimeError("couche memoire invalide")
    feats = []
    for zone, parcel, nom, x in _ROWS:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([zone, parcel, nom])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, 100.0)))
        feats.append(feat)
    ok, _added = mem.dataProvider().addFeatures(feats)
    if not ok:
        raise RuntimeError("seed impossible")
    return mem


def _rows(layer):
    return sorted((f["zone_id"], f["parcel_no"], f["nom"])
                  for f in layer.getFeatures())


def _feature(layer, zone, parcel):
    for feat in layer.getFeatures():
        if feat["zone_id"] == zone and feat["parcel_no"] == parcel:
            return feat
    return None


def _event(layer, feature, operation, attributes_json, offset_s,
           eid, geometry=True, identity=None):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.serialization import serialize_field_schema
    from recoverland.core.geometry_utils import geometry_to_wkb

    if identity is None:
        identity = compute_feature_identity(layer, feature)
    wkb = geometry_to_wkb(feature.geometry()) if geometry else None
    return AuditEvent(
        event_id=eid, project_fingerprint="proj_cpk",
        datasource_fingerprint="postgres::host=h port=5432 dbname=cad "
                               "schema=public table=parcelles",
        layer_id_snapshot="lyr_parcelles", layer_name_snapshot="parcelles",
        provider_type="postgres", feature_identity_json=identity,
        operation_type=operation, attributes_json=attributes_json,
        geometry_wkb=wkb, geometry_type="Point", crs_authid="EPSG:2154",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id="sess_cpk",
        created_at=(_T0 + timedelta(seconds=offset_s)).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(identity),
        event_schema_version=7, new_geometry_wkb=None,
    )


def _snapshot_json(feature):
    from recoverland.core.serialization import (
        serialize_attributes, build_full_snapshot,
    )
    names = [f.name() for f in feature.fields()]
    return build_full_snapshot(serialize_attributes(feature, names))


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - reported by the assertions
        return {"success": None, "exception": repr(exc)}


def _temporal_case(kind):
    """Cle (id, obs) ou obs est une date ou une date-heure : deux lignes."""
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime
    if kind == "date":
        keys = (QDate(2026, 1, 15), QDate(2026, 1, 16))
    else:
        keys = (QDateTime(QDate(2026, 1, 15), QTime(8, 0)),
                QDateTime(QDate(2026, 1, 16), QTime(8, 0)))
    mem = QgsVectorLayer(f"Point?crs=EPSG:2154&field=id:integer"
                         f"&field=obs:{kind}&field=statut:string(20)", "obs",
                         "memory")
    feats = []
    for x, key in enumerate(keys):
        feat = QgsFeature(mem.fields())
        feat.setAttributes([5, key, "libre"])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(float(x), 100.0)))
        feats.append(feat)
    if not mem.dataProvider().addFeatures(feats)[0]:
        raise RuntimeError("seed impossible")
    return mem, keys


def _temporal_rows(layer):
    def text(value):
        if type(value).__name__ == "QDateTime":
            return value.toString("yyyy-MM-ddTHH:mm")
        if type(value).__name__ == "QDate":
            return value.toString("yyyy-MM-dd")
        return repr(value)
    return sorted((f["id"], text(f["obs"]), f["statut"])
                  for f in layer.getFeatures())


def _phase_temporal_keys(out):
    from qgis.core import QgsFeatureRequest
    from recoverland.core import identity as ident
    from recoverland.core import restore_service as rs
    from recoverland.core.serialization import compute_update_delta
    out["temporal"] = {}
    for kind in ("date", "datetime"):
        res = {}
        base, keys = _temporal_case(kind)
        layer = PkLayer(base)
        first = next(f for f in base.getFeatures() if f["obs"] == keys[0])
        identity = ident.compute_feature_identity(layer, first)
        res["identity"] = json.loads(identity)
        res["fingerprint"] = ident.compute_entity_fingerprint(identity)
        pairs = ident.composite_key(res["identity"])
        try:
            flt = ident.composite_key_filter(pairs, base.fields())
        except TypeError:
            # Filtre sans les champs de la couche (code anterieur).
            flt = ident.composite_key_filter(pairs)
        res["filter"] = flt
        res["filter_fids"] = sorted(f.id() for f in base.getFeatures(
            QgsFeatureRequest().setFilterExpression(flt))) if flt else None
        res["fid"] = first.id()
        # UPDATE : statut libre -> occupe sur la ligne 1, puis restauration.
        base.dataProvider().changeAttributeValues(
            {first.id(): {base.fields().indexOf("statut"): "occupe"}})
        delta = compute_update_delta({"statut": "libre"},
                                     {"statut": "occupe"}, ["statut"])
        upd = _event(layer, first, "UPDATE", delta, 0, 301, geometry=False,
                     identity=identity)
        res["update"] = _safe(rs.restore_updated_feature, layer, upd)
        res["rows_after_update"] = _temporal_rows(base)
        # Historique deja journalise : la meme cle en repr, PyQt6 et PyQt5.
        for binding in ("PyQt6", "PyQt5"):
            legacy = dict(res["identity"])
            legacy["pk_values"] = [5, repr(keys[0]).replace("PyQt6", binding)]
            res[f"effective_{binding}"] = ident.effective_identity(legacy,
                                                                   layer)
        base.dataProvider().changeAttributeValues(
            {first.id(): {base.fields().indexOf("statut"): "occupe"}})
        legacy = dict(res["identity"])
        legacy["pk_values"] = [5, repr(keys[0])]
        res["legacy_update"] = _safe(
            rs.restore_updated_feature, layer,
            upd._replace(event_id=302, feature_identity_json=json.dumps(
                legacy)))
        res["rows_after_legacy_update"] = _temporal_rows(base)
        # DELETE de la ligne 1 : restauree, ou refus nomme.
        dele = _event(layer, first, "DELETE", _snapshot_json(first), 60, 303,
                      identity=identity)
        base.dataProvider().deleteFeatures([first.id()])
        res["delete"] = _safe(rs.restore_deleted_feature, layer, dele)
        res["rows_after_delete"] = _temporal_rows(base)
        out["temporal"][kind] = res
    # Cle SIMPLE sur la date : forme historique gardee (empreintes deja
    # journalisees), mais la recherche doit designer la ligne.
    base, keys = _temporal_case("date")
    single = PkLayer(base, pk_indexes=(1,))
    second = next(f for f in base.getFeatures() if f["obs"] == keys[1])
    identity = ident.compute_feature_identity(single, second)
    base.dataProvider().changeAttributeValues(
        {second.id(): {base.fields().indexOf("statut"): "occupe"}})
    delta = compute_update_delta({"statut": "libre"}, {"statut": "occupe"},
                                 ["statut"])
    out["single_date_identity"] = json.loads(identity)
    out["single_date_update"] = _safe(
        rs.restore_updated_feature, single,
        _event(single, second, "UPDATE", delta, 0, 304, geometry=False,
               identity=identity))
    out["single_date_rows"] = _temporal_rows(base)
    # Un repr qui ne dit pas sa valeur (decalage) : jamais cherche tel quel.
    blind = {"fid": 1, "pk_fields": ["id", "obs"], "pk_values": [
        5, "PyQt6.QtCore.QDateTime(2026, 1, 15, 8, 0, 0, 0, "
           "PyQt6.QtCore.Qt.TimeSpec(2))"]}
    base, _keys = _temporal_case("datetime")
    out["effective_blind"] = ident.effective_identity(blind, PkLayer(base))


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"rw_composite_pk_identity setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core import identity as ident
    from recoverland.core import restore_service as rs
    from recoverland.core.serialization import compute_update_delta
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.logger import flog

    out = ctx.data
    out["error"] = None
    try:
        base = _make_layer()
        layer = PkLayer(base)

        # ---- capture : identites et empreintes -------------------------
        f5 = _feature(base, 12, 5)
        f7 = _feature(base, 12, 7)
        f9 = _feature(base, 12, 9)
        id5 = json.loads(ident.compute_feature_identity(layer, f5))
        id7 = json.loads(ident.compute_feature_identity(layer, f7))
        out["identity_12_7"] = id7
        out["fid_12_5"] = f5.id()
        out["fp_12_5"] = ident.compute_entity_fingerprint(json.dumps(id5))
        out["fp_12_7"] = ident.compute_entity_fingerprint(json.dumps(id7))
        # Cle composite dont une colonne est NULL : n'identifie rien.
        incomplete = json.loads(ident.compute_feature_identity(
            layer, _FakeNullPk(f7)))
        out["identity_incomplete"] = incomplete
        # Le filtre que les consommateurs posent pour trouver LA ligne :
        # toutes les colonnes de la cle, une seule ligne tenue.
        key_filter = getattr(ident, "composite_key_filter", None)
        out["filter_available"] = key_filter is not None
        if key_filter is not None:
            from qgis.core import QgsExpression, QgsFeatureRequest
            expr = key_filter(ident.composite_key(id7))
            out["filter_expr"] = expr
            out["filter_rows"] = sorted(
                (f["zone_id"], f["parcel_no"]) for f in base.getFeatures(
                    QgsFeatureRequest().setFilterExpression(expr)))
            quoted = key_filter([("nom", "l'eau \"vive\""), ("zone_id", 12)])
            out["filter_quoted_ok"] = (
                not QgsExpression(quoted).hasParserError())
            out["filter_empty"] = key_filter([])

        # ---- UPDATE : l'utilisateur a renomme (12,5) 'avant' -> 'b' -----
        upd = _event(layer, f5, "UPDATE",
                     compute_update_delta({"nom": "avant"}, {"nom": "b"},
                                          ["nom"]),
                     0, 101, geometry=False)
        out["update_result"] = _safe(rs.restore_updated_feature, layer, upd)
        out["rows_after_update"] = _rows(base)

        # FID perime (une autre session PostGIS a renumerote) : il pointe
        # sur (12,1), dont le nom n'est pas le NEW de l'evenement.
        stale_identity = json.loads(upd.feature_identity_json)
        stale_identity["fid"] = _feature(base, 12, 1).id()
        stale = upd._replace(event_id=102,
                             feature_identity_json=json.dumps(stale_identity))
        out["rows_before_stale"] = _rows(base)
        out["stale_result"] = _safe(rs.restore_updated_feature, layer, stale)
        out["rows_after_stale"] = _rows(base)

        # ---- INSERT de (12,9) a annuler ---------------------------------
        ins = _event(layer, f9, "INSERT", _snapshot_json(f9), 60, 103)
        out["insert_result"] = _safe(rs.restore_inserted_feature, layer, ins)
        out["rows_after_insert_undo"] = _rows(base)
        # Rejeu : (12,9) n'est plus la, rien d'autre ne doit partir.
        out["insert_replay_result"] = _safe(
            rs.restore_inserted_feature, layer, ins._replace(event_id=104))
        out["rows_after_insert_replay"] = _rows(base)

        # ---- DELETE de (12,7) a restaurer -------------------------------
        dele = _event(layer, f7, "DELETE", _snapshot_json(f7), 120, 105)
        base.dataProvider().deleteFeatures([f7.id()])
        out["rows_after_user_delete"] = _rows(base)
        out["delete_result"] = _safe(rs.restore_deleted_feature, layer, dele)
        out["rows_after_delete_restore"] = _rows(base)

        # ---- rewind : deux parcelles, deux chaines d'entite ---------------
        # Ordre du fetch reel : le plus recent d'abord.
        active, _stats = collapse_rewind_events_with_stats([dele, ins])
        out["dedup_active"] = sorted(
            (e.operation_type, e.entity_fingerprint) for e in active)
        # UPDATE de (12,5) puis DELETE de (12,7) : fusionnes dans une meme
        # chaine, ils deviennent UN DELETE synthetique qui porte l'ancien
        # nom de (12,5) et la modification de (12,5) n'est plus compensee.
        active, _stats = collapse_rewind_events_with_stats([dele, upd])
        out["dedup_update_delete"] = sorted(
            (e.operation_type, json.loads(e.attributes_json))
            for e in active)

        # ---- evenement legacy : premiere colonne seule ------------------
        eff = getattr(ident, "effective_identity", None)
        legacy = {"fid": f5.id(), "pk_field": "zone_id", "pk_value": 12}
        out["effective_available"] = eff is not None
        out["effective_legacy"] = eff(legacy, layer) if eff else None
        single = PkLayer(base, pk_indexes=(0,))
        out["effective_single"] = eff(legacy, single) if eff else None

        # ---- cle composite a colonne date / date-heure --------------------
        _phase_temporal_keys(out)
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
    flog(f"rw_composite_pk_identity run: trace_id={ctx.trace_id} "
         f"error={out.get('error')} fp5={out.get('fp_12_5')!r} "
         f"fp7={out.get('fp_12_7')!r} update={out.get('update_result')} "
         f"insert={out.get('insert_result')} "
         f"delete={out.get('delete_result')} "
         f"dedup={out.get('dedup_active')}",
         "INFO" if out.get("error") is None else "ERROR")


class _FakeNullPk:
    """Entite dont la seconde colonne de cle n'est pas encore attribuee."""

    def __init__(self, feature):
        self._feature = feature

    def id(self):
        return self._feature.id()

    def __getitem__(self, name):
        if name == "parcel_no":
            return None
        return self._feature[name]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    out.append(("scenario_ran", d.get("error") is None,
                f"le scenario a plante : {d.get('error')}"))

    id7 = d.get("identity_12_7") or {}
    out.append((
        "identity_carries_every_key_column",
        id7.get("pk_fields") == ["zone_id", "parcel_no"]
        and id7.get("pk_values") == [12, 7],
        f"identite journalisee de la parcelle (12,7) = {id7} : une cle "
        f"(zone_id, parcel_no) journalisee sur sa premiere colonne seule "
        f"designe toute la zone 12, et chaque restauration ecrit sur une "
        f"parcelle au hasard de cette zone.",
    ))
    out.append((
        "identity_offers_no_blind_single_key",
        "pk_field" not in id7 and "pk_value" not in id7,
        f"identite={id7} : la paire pk_field/pk_value est celle que les "
        f"chemins de restauration croient sans relire la ligne ; une cle "
        f"composite ne doit pas la presenter.",
    ))
    inc = d.get("identity_incomplete") or {}
    out.append((
        "incomplete_key_identifies_nothing",
        inc.get("pk_incomplete") is True and "pk_field" not in inc
        and "pk_values" not in inc,
        f"identite d'une entite dont parcel_no est NULL = {inc} : une cle a "
        f"moitie remplie ne designe aucune ligne, elle doit etre marquee "
        f"incomplete et laisser la preuve par l'etat.",
    ))
    out.append((
        "entity_fingerprints_distinct",
        d.get("fp_12_5") and d.get("fp_12_7")
        and d.get("fp_12_5") != d.get("fp_12_7"),
        f"empreinte (12,5)={d.get('fp_12_5')!r} (12,7)={d.get('fp_12_7')!r} : "
        f"deux parcelles distinctes partagent une empreinte d'entite ; le "
        f"rewind et la revue melangent leurs historiques.",
    ))

    out.append((
        "composite_key_filter_selects_one_row",
        d.get("filter_available") is True
        and d.get("filter_rows") == [(12, 7)]
        and d.get("filter_quoted_ok") is True and d.get("filter_empty") == "",
        f"filtre de la cle de (12,7) = {d.get('filter_expr')!r} -> lignes "
        f"{d.get('filter_rows')} (attendu [(12, 7)]) ; valeur avec "
        f"apostrophe et guillemets analysable={d.get('filter_quoted_ok')!r} ; "
        f"cle vide -> {d.get('filter_empty')!r} (attendu ''). Un filtre sur "
        f"une colonne seule designe toute la zone 12 ; une cle vide ne doit "
        f"jamais devenir 'toutes les lignes'.",
    ))

    upd = d.get("update_result") or {}
    rows = d.get("rows_after_update") or []
    out.append((
        "update_restore_writes_on_its_own_row",
        (12, 5, "avant") in rows and (12, 1, "parcelle un") in rows,
        f"apres restauration de l'UPDATE de (12,5) : lignes={rows} "
        f"resultat={upd}. L'ancien nom doit revenir sur (12,5) et nulle part "
        f"ailleurs ; ecrit sur (12,1), il detruit le nom d'une autre "
        f"parcelle en annoncant 'Reverted'.",
    ))
    stale = d.get("stale_result") or {}
    # Hypothesis revised (v7): "sans preuve" ne tient plus, la restauration
    # localise une identite composite par sa cle COMPLETE
    # (identity.composite_key), qui designe (12,5) quel que soit le FID
    # perime. Deux issues sont donc justes : un refus, ou le constat que
    # (12,5) est deja dans l'etat d'avant l'edition, sans rien ecrire. La
    # protection est la meme (aucune ligne ne bouge) et le controle plus
    # strict : un succes doit designer (12,5) et etre un saut.
    out.append((
        "stale_fid_never_overwrites_another_row",
        d.get("rows_after_stale") == d.get("rows_before_stale")
        and (stale.get("success") is not True
             or (stale.get("skipped") is True
                 and stale.get("fid") == d.get("fid_12_5"))),
        f"FID perime pointant sur (12,1) : resultat={stale} lignes avant="
        f"{d.get('rows_before_stale')} apres={d.get('rows_after_stale')} "
        f"(fid de (12,5)={d.get('fid_12_5')!r}). La restauration doit "
        f"refuser, ou reconnaitre par la cle complete que (12,5) est deja "
        f"restauree ; jamais ecraser la parcelle que le FID designe par "
        f"hasard.",
    ))

    rows = d.get("rows_after_insert_undo") or []
    out.append((
        "insert_undo_never_deletes_another_row",
        all(r in rows for r in ((12, 1, "parcelle un"),
                                (12, 7, "parcelle sept"))),
        f"apres annulation de l'INSERT de (12,9) : lignes={rows} resultat="
        f"{d.get('insert_result')}. Supprimer (12,1) ou (12,7) efface une "
        f"parcelle que l'utilisateur n'a jamais touchee.",
    ))
    out.append((
        "insert_undo_removes_its_row",
        (12, 9, "parcelle neuf") not in rows,
        f"apres annulation de l'INSERT de (12,9) : lignes={rows} resultat="
        f"{d.get('insert_result')}. La parcelle inseree doit disparaitre, "
        f"elle est designee par sa cle complete et prouvee par son etat.",
    ))
    out.append((
        "insert_replay_deletes_nothing",
        d.get("rows_after_insert_replay") == d.get("rows_after_insert_undo"),
        f"rejeu de l'annulation alors que (12,9) est deja absente : lignes "
        f"avant={d.get('rows_after_insert_undo')} apres="
        f"{d.get('rows_after_insert_replay')} resultat="
        f"{d.get('insert_replay_result')}. Un double clic ne doit rien "
        f"supprimer d'autre.",
    ))

    dres = d.get("delete_result") or {}
    rows = d.get("rows_after_delete_restore") or []
    out.append((
        "delete_restore_really_reinserts",
        (12, 7, "parcelle sept") in rows and not dres.get("skipped"),
        f"restauration du DELETE de (12,7) : resultat={dres} lignes={rows}. "
        f"'Already exists (skip)' parce qu'une AUTRE parcelle de la zone 12 "
        f"existe est un succes annonce pour une restauration qui n'a pas eu "
        f"lieu, et la trace ecrite l'enterre pour tous les rewinds suivants.",
    ))
    active = d.get("dedup_active") or []
    out.append((
        "rewind_keeps_two_entities_apart",
        len(active) == 2,
        f"evenements actifs apres deduplication={active} : INSERT(12,9) et "
        f"DELETE(12,7) sont deux parcelles ; fusionnes dans une meme chaine "
        f"ils s'annulent et le rewind ne compense ni l'un ni l'autre.",
    ))
    fused = d.get("dedup_update_delete") or []
    ops = sorted(op for op, _attrs in fused)
    deleted = [attrs for op, attrs in fused if op == "DELETE"]
    out.append((
        "rewind_never_fuses_two_parcels",
        ops == ["DELETE", "UPDATE"] and len(deleted) == 1
        and (deleted[0].get("all_attributes") or {}).get("nom")
        == "parcelle sept",
        f"UPDATE(12,5) + DELETE(12,7) apres deduplication={fused} : fusionnes "
        f"en un DELETE synthetique, le rewind recree (12,7) avec le nom de "
        f"(12,5) et ne remet jamais l'ancien nom de (12,5).",
    ))

    legacy = d.get("effective_legacy") or {}
    out.append((
        "legacy_first_column_identity_withdrawn",
        d.get("effective_available") is True and "pk_field" not in legacy
        and legacy.get("pk_unproven") == "legacy_first_column_of_composite_key",
        f"effective_identity(evenement pre-v7 zone_id=12, couche a cle "
        f"composite)={d.get('effective_legacy')} : l'historique deja "
        f"journalise porte la premiere colonne seule ; la restauration ne "
        f"doit plus s'y fier et doit savoir pourquoi.",
    ))
    single = d.get("effective_single") or {}
    out.append((
        "single_column_identity_untouched",
        d.get("effective_available") is True
        and single.get("pk_field") == "zone_id"
        and single.get("pk_value") == 12,
        f"effective_identity sur une couche a cle simple zone_id="
        f"{d.get('effective_single')} : une cle simple reste une preuve, la "
        f"retirer degraderait toutes les couches saines.",
    ))
    for kind, want in (("date", "2026-01-15"),
                       ("datetime", "2026-01-15T08:00:00.000")):
        res = (d.get("temporal") or {}).get(kind) or {}
        ident_t = res.get("identity") or {}
        out.append((
            f"temporal_key_{kind}_journaled_as_its_value",
            ident_t.get("pk_values") == [5, want]
            and "PyQt" not in str(res.get("fingerprint")),
            f"cle (id, {kind}) journalisee {ident_t} empreinte="
            f"{res.get('fingerprint')!r} : attendu [5, {want!r}]. Le repr "
            f"Python d'un objet Qt ne designe aucune ligne et change avec la "
            f"liaison PyQt (QGIS 3 / QGIS 4)."))
        out.append((
            f"temporal_key_{kind}_filter_selects_its_row",
            res.get("filter_fids") == [res.get("fid")],
            f"filtre de la cle (id, {kind}) = {res.get('filter')!r} -> fids "
            f"{res.get('filter_fids')} (attendu [{res.get('fid')}]). Un "
            f"filtre qui ne designe rien fait refuser chaque restauration "
            f"sur une table partitionnee par date."))
        rows = res.get("rows_after_update") or []
        out.append((
            f"temporal_key_{kind}_update_restored_on_its_row",
            (res.get("update") or {}).get("success") is True
            and [r[2] for r in rows] == ["libre", "libre"],
            f"restauration de l'UPDATE (statut occupe -> libre) de la ligne "
            f"(5, {want}) : resultat={res.get('update')} lignes={rows}. "
            f"'Target feature not found' alors que la ligne est la : la "
            f"modification ne peut jamais etre annulee."))
        rows = res.get("rows_after_legacy_update") or []
        effective6 = res.get("effective_PyQt6") or {}
        effective5 = res.get("effective_PyQt5") or {}
        out.append((
            f"temporal_key_{kind}_legacy_repr_decoded",
            effective6.get("pk_values") == [5, want]
            and effective5.get("pk_values") == [5, want]
            and (res.get("legacy_update") or {}).get("success") is True
            and [r[2] for r in rows] == ["libre", "libre"],
            f"cle deja journalisee en repr : PyQt6 -> {effective6} ; PyQt5 -> "
            f"{effective5} ; restauration d'un UPDATE journalise ainsi -> "
            f"{res.get('legacy_update')} lignes={rows}. L'historique existant "
            f"doit designer sa ligne, sur QGIS 3 comme sur QGIS 4."))
        dres = res.get("delete") or {}
        rows = res.get("rows_after_delete") or []
        back = [r for r in rows if r[1].startswith(want[:10])]
        out.append((
            f"temporal_key_{kind}_delete_restored_or_named",
            (dres.get("success") is True and len(back) == 1
             and back[0][2] == "libre" and len(rows) == 2)
            or (dres.get("success") is not True
                and bool(dres.get("reason_code")) and len(rows) == 1),
            f"restauration du DELETE de (5, {want}) : resultat={dres} "
            f"lignes={rows}. La ligne doit revenir, ou la restauration "
            f"refuser en nommant pourquoi ; jamais un succes sans la ligne."))
    single = d.get("single_date_identity") or {}
    rows = d.get("single_date_rows") or []
    out.append((
        "temporal_single_key_historical_form_found",
        str(single.get("pk_value", "")).startswith("PyQt")
        and (d.get("single_date_update") or {}).get("success") is True
        and [r[2] for r in rows] == ["libre", "libre"],
        f"cle simple sur une date : identite {single} (forme historique "
        f"gardee, les empreintes deja journalisees en dependent), "
        f"restauration d'un UPDATE -> {d.get('single_date_update')} lignes="
        f"{rows}. La recherche doit decoder la valeur et trouver sa ligne."))
    blind = d.get("effective_blind") or {}
    out.append((
        "temporal_key_unreadable_repr_withdrawn_and_named",
        "pk_values" not in blind
        and blind.get("pk_unproven") == "key_value_not_decodable",
        f"cle journalisee en repr d'une date-heure a decalage (le repr ne dit "
        f"pas le decalage) -> {blind}. Cherchee telle quelle elle ne designe "
        f"rien et fait repondre 'not found' ; elle doit etre retiree en "
        f"disant pourquoi, la cible prouvee par son contenu."))
    out.append(assert_log_contains(
        ctx.records, rf"rw_composite_pk_identity run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
