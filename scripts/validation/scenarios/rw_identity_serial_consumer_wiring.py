# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_identity_serial_consumer_wiring - RecoverLand validation runtime.

Invariant: I-3 et BL-SER-P0-01, cote CONSOMMATEURS : les chemins de
restauration doivent appliquer ce que `core.identity` et
`core.serialization` savent desormais dire.

Pourquoi un scenario a part
---------------------------
Deux defauts ont leur racine dans les donnees DEJA journalisees, que la
capture v7 ne peut plus corriger ; ils ne se ferment qu'au moment ou la
restauration lit l'evenement :

1. Cle composite, evenements anterieurs a v7. Ils portent
   ``{"pk_field": "zone_id", "pk_value": 12}`` : la premiere colonne seule
   d'une cle (zone_id, parcel_no). `identity.effective_identity(identity,
   couche)` retire cette paire quand la couche a une cle de plusieurs
   colonnes et dit pourquoi (``pk_unproven``). Tant que
   `restore_service._parse_identity` / `restore_executor` ne l'appellent
   pas, un UPDATE ancien est encore ecrit sur la premiere parcelle de la
   zone 12, et un DELETE ancien repond encore "Already exists (skip)".

2. Date-heure des evenements <= 5.x. `serialization.iter_mapped_attributes`
   et `deserialize_value` prennent maintenant ``event_schema_version`` ;
   tant que `restore_service` / `restore_executor` ne le transmettent pas,
   un evenement 5.x est restaure decale de l'offset UTC (15:30 au lieu de
   14:30 a Paris en hiver) et annonce reussi. Deux chemins sont mesures :
   la restauration d'un DELETE (`restore_deleted_feature`) et la
   compensation d'un UPDATE par le rewind (`restore_executor._buffer_update`,
   qui ecrit les valeurs OLD de l'evenement dans le tampon d'edition).

Ce scenario exerce les VRAIS chemins de restauration de bout en bout. Il
echoue tant que ce cablage n'est pas fait dans les modules consommateurs,
et passe des qu'il l'est. Il ne relache rien : il mesure ce que
l'utilisateur recupere.

Hypothesis revised (v7): les phases date-heure journalisaient ``14:30:00``
pour 14:30 locales en 5.x sur une GeoPackage. Mesure : 5.x ecrivait
``toString`` de la valeur que le PROVIDER rendait, et une GeoPackage rend
en UTC (``13:30:00``). Avec la fixture fausse, la phase DELETE passait
alors que la restauration elle-meme repondait ``success=False`` (faux
vert). Les textes 5.x viennent maintenant d'une vraie capture (valeur
ecrite par le provider, relue, formatee comme 5.x). Le cablage demande
aux consommateurs devient : la version de l'evenement ET le fuseau que
le provider rend pour ce champ (`serialization.provider_time_specs`), les
champs refuses etant retires et signales (`legacy_datetime_refusals`).
La phase de compensation d'un UPDATE ajoute une couche SQLite (OGR), qui
rend l'heure locale : sur une GeoPackage seule, la lecture UTC par defaut
d'un appelant non cable tombe juste par hasard et ne prouverait rien.
Les assertions sont renforcees : la restauration doit aussi REPONDRE
succes. Une assertion distincte, `legacy_datetime_never_written_shifted`,
exige des maintenant qu'un evenement 5.x ne soit jamais ecrit decale :
restaure exact, ou refuse en le nommant.

Hypothesis revised (v7), second tour : la preuve du cablage supposait
que, sur une GeoPackage (DELETE) et sur une SQLite OGR (compensation d'un
UPDATE), les valeurs vivantes du champ etablissent le fuseau de la valeur
supprimee ou ecrasee. C'est faux, et mesure par les mondes jumeaux de
`tx_serial_legacy_per_row_spec` : sur ces providers le fuseau appartient a
chaque valeur stockee, les memes lignes restantes et le meme texte 5.x
viennent d'une ligne ``Z`` comme d'une ligne naive, une heure plus tot.
Le seul resultat juste y est le refus nomme, que les assertions
`..._never_written_shifted` exigent toujours sur les deux fichiers. Le
cablage lui-meme (version de l'evenement ET `provider_time_specs` transmis
par `restore_deleted_feature` et `_buffer_update`) est maintenant prouve
la ou le fuseau est etabli : un provider a type de colonne (PostgreSQL,
simule par une couche memoire presentee sous le nom ``postgres``). Memes
exigences qu'avant (14:30 relues ET succes) : un appelant qui ne passe pas
la version lit les chiffres en UTC (15:30), un appelant qui ne passe pas
la mesure est refuse ; l'un comme l'autre echoue.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_identity_serial_consumer_wiring"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _legacy_event(base_event, identity):
    from recoverland.core.identity import compute_entity_fingerprint
    ident = json.dumps(identity)
    return base_event._replace(feature_identity_json=ident,
                               entity_fingerprint=compute_entity_fingerprint(ident),
                               event_schema_version=5)


def _phase_composite(d):
    from scripts.validation.scenarios.rw_composite_pk_identity import (
        PkLayer, _make_layer, _feature, _event, _rows, _snapshot_json, _safe,
    )
    from recoverland.core import restore_service as rs
    from recoverland.core.serialization import compute_update_delta

    base = _make_layer()
    layer = PkLayer(base)
    f5 = _feature(base, 12, 5)
    f7 = _feature(base, 12, 7)
    legacy5 = {"fid": f5.id(), "pk_field": "zone_id", "pk_value": 12}
    legacy7 = {"fid": f7.id(), "pk_field": "zone_id", "pk_value": 12}

    upd = _legacy_event(_event(
        layer, f5, "UPDATE",
        compute_update_delta({"nom": "avant"}, {"nom": "b"}, ["nom"]),
        0, 201, geometry=False), legacy5)
    d["legacy_update"] = _safe(rs.restore_updated_feature, layer, upd)
    d["legacy_rows_after_update"] = _rows(base)

    dele = _legacy_event(_event(layer, f7, "DELETE", _snapshot_json(f7),
                                60, 202), legacy7)
    base.dataProvider().deleteFeatures([f7.id()])
    d["legacy_delete"] = _safe(rs.restore_deleted_feature, layer, dele)
    d["legacy_rows_after_delete"] = _rows(base)


def _dt_layer(tmpdir, name, driver):
    """Couche fichier (GeoPackage ou SQLite OGR) de constats horodates."""
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=label:string(20)"
                         "&field=dtm:datetime", "seed", "memory")
    ext = "gpkg" if driver == "GPKG" else "sqlite"
    path = os.path.join(tmpdir, f"{name}.{ext}")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = driver
    opts.layerName = "constats"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return QgsVectorLayer(f"{path}|layername=constats", "constats", "ogr")


def _add_constat(layer, label, hour, minute):
    """Une saisie de l'utilisateur (heure murale locale) validee en base."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime
    feat = QgsFeature(layer.fields())
    feat.setAttribute("label", label)
    feat.setAttribute("dtm", QDateTime(QDate(2026, 1, 15), QTime(hour, minute)))
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.35, 48.85)))
    layer.dataProvider().addFeatures([feat])
    layer.reload()
    return next(f for f in layer.dataProvider().getFeatures()
                if f["label"] == label)


def _v5_text(value):
    """Exactement ce que 5.x journalisait : toString sans fuseau."""
    return value.toString("yyyy-MM-ddTHH:mm:ss")


def _wall_clocks(layer, label):
    return [f["dtm"].toLocalTime().toString("yyyy-MM-ddTHH:mm:ss")
            if hasattr(f["dtm"], "toLocalTime") else repr(f["dtm"])
            for f in layer.getFeatures() if f["label"] == label]


def _column_layer():
    """Provider a TYPE DE COLONNE : une couche memoire vue comme ``postgres``.

    Le fuseau d'une colonne PostgreSQL est fixe par son type et le fuseau
    de la session, jamais par la ligne : c'est la que le fuseau d'une
    valeur 5.x disparue est etabli, donc la que le cablage se prouve.
    Renvoie ``(couche reelle, couche vue par la restauration)``.
    """
    from qgis.core import QgsVectorLayer
    from scripts.validation.scenarios.rw_composite_pk_identity import PkLayer
    base = QgsVectorLayer("Point?crs=EPSG:4326&field=label:string(20)"
                          "&field=dtm:datetime", "constats", "memory")
    return base, PkLayer(base, pk_indexes=(), name="postgres")


def _phase_datetime(d, tmpdir):
    gpkg = _dt_layer(tmpdir, "wiring_dt", "GPKG")
    _restore_v5_delete(d, "", gpkg, gpkg)
    base, layer = _column_layer()
    _restore_v5_delete(d, "col_", layer, base)


def _restore_v5_delete(d, tag, layer, base):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.restore_service import restore_deleted_feature
    from recoverland.core.serialization import (
        build_full_snapshot, serialize_field_schema,
    )
    from scripts.validation.scenarios.rw_composite_pk_identity import _safe

    # Une autre ligne du meme champ reste dans la couche : c'est sur elle
    # que le fuseau rendu par le provider se mesure.
    _add_constat(base, "voisin", 9, 0)
    deleted = _add_constat(base, "constat", 14, 30)
    d[f"{tag}dt_v5_text"] = _v5_text(deleted["dtm"])
    base.dataProvider().deleteFeatures([deleted.id()])
    base.reload()
    event = AuditEvent(
        event_id=301, project_fingerprint="p", datasource_fingerprint="ds",
        layer_id_snapshot=layer.id(), layer_name_snapshot="constats",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": deleted.id()}),
        operation_type="DELETE",
        attributes_json=build_full_snapshot(
            {"label": "constat", "dtm": d[f"{tag}dt_v5_text"]}),
        geometry_wkb=None, geometry_type="Point", crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id="s", created_at="2026-02-01T10:00:00",
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{deleted.id()}",
        event_schema_version=5, new_geometry_wkb=None,
    )
    d[f"{tag}dt_result"] = _safe(restore_deleted_feature, layer, event)
    base.reload()
    d[f"{tag}dt_restored"] = _wall_clocks(base, "constat")


def _phase_rewind_update(d, tmpdir):
    """Le rewind compense un UPDATE 5.x : dtm 14:30 -> 16:00 (heure murale).

    Sur une GeoPackage (rendue en UTC) et sur une SQLite OGR (rendue en
    heure locale) : le texte 'old' est celui que 5.x lisait du provider,
    le texte 'new' celui du tampon d'edition (saisie locale).
    """
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.restore_executor import _buffer_update
    from recoverland.core.serialization import serialize_field_schema
    from scripts.validation.scenarios.rw_composite_pk_identity import _safe

    d["upd"] = {}
    for driver in ("GPKG", "SQLite", "COLONNE"):
        if driver == "COLONNE":
            base, layer = _column_layer()
        else:
            base = layer = _dt_layer(tmpdir, f"wiring_upd_{driver}", driver)
        _add_constat(base, "voisin", 9, 0)
        before = _add_constat(base, "constat", 14, 30)
        old_text = _v5_text(before["dtm"])
        base.dataProvider().changeAttributeValues({before.id(): {
            base.fields().indexOf("dtm"): before["dtm"].addSecs(5400)}})
        base.reload()
        event = AuditEvent(
            event_id=401, project_fingerprint="p", datasource_fingerprint="ds",
            layer_id_snapshot=layer.id(), layer_name_snapshot="constats",
            provider_type="ogr",
            feature_identity_json=json.dumps({"fid": before.id()}),
            operation_type="UPDATE",
            attributes_json=json.dumps({"changed_only": {"dtm": {
                "old": old_text, "new": "2026-01-15T16:00:00"}}}),
            geometry_wkb=None, geometry_type="Point", crs_authid="EPSG:4326",
            field_schema_json=serialize_field_schema(layer.fields()),
            user_name="tester", session_id="s",
            created_at="2026-02-01T10:00:00", restored_from_event_id=None,
            entity_fingerprint=f"fid:{before.id()}",
            event_schema_version=5, new_geometry_wkb=None,
        )
        base.startEditing()
        result = _safe(_buffer_update, layer, event)
        commit = bool(base.commitChanges())
        if not commit:
            base.rollBack()
        base.reload()
        d["upd"][driver] = {"old_text": old_text, "result": result,
                            "commit": commit,
                            "restored": _wall_clocks(base, "constat")}


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"rw_identity_serial_consumer_wiring setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime
    d = ctx.data
    d["error"] = None
    d["offset_s"] = int(QDateTime(QDate(2026, 1, 15), QTime(14, 30))
                        .offsetFromUtc())
    tmpdir = tempfile.mkdtemp(prefix="rl_wiring_")
    try:
        _phase_composite(d)
        _phase_datetime(d, tmpdir)
        _phase_rewind_update(d, tmpdir)
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    flog(f"rw_identity_serial_consumer_wiring run: trace_id={ctx.trace_id} "
         f"update={d.get('legacy_update')} delete={d.get('legacy_delete')} "
         f"dt={d.get('dt_restored')} dt_result={d.get('dt_result')} "
         f"col_dt={d.get('col_dt_restored')} "
         f"col_dt_result={d.get('col_dt_result')} "
         f"upd={d.get('upd')} error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    rows_u = d.get("legacy_rows_after_update") or []
    rows_d = d.get("legacy_rows_after_delete") or []
    dres = d.get("legacy_delete") or {}
    return [
        ("scenario_ran", d.get("error") is None,
         f"le scenario a plante : {d.get('error')}"),
        # Les phases date-heure ne prouvent rien a decalage UTC nul.
        ("precondition_zone_has_an_offset",
         d.get("offset_s") not in (None, 0),
         f"decalage UTC du poste le 15 janvier 2026 = {d.get('offset_s')!r} s : "
         f"lancez ce scenario dans un fuseau decale (Europe/Paris)."),
        ("legacy_composite_update_never_hits_another_row",
         (12, 1, "parcelle un") in rows_u,
         f"UPDATE journalise avant v7 (zone_id=12 seul) restaure : lignes="
         f"{rows_u} resultat={d.get('legacy_update')}. La restauration doit "
         f"passer l'identite par identity.effective_identity : ecrit sur "
         f"(12,1), l'ancien nom de (12,5) detruit celui d'une autre parcelle."),
        ("legacy_composite_delete_no_false_success",
         (12, 7, "parcelle sept") in rows_d
         or (dres.get("success") is False and bool(dres.get("reason_code"))),
         f"DELETE journalise avant v7 (zone_id=12 seul) restaure : resultat="
         f"{dres} lignes={rows_d}. La parcelle doit revenir, ou la "
         f"restauration doit refuser en nommant pourquoi ; 'Already exists' "
         f"parce qu'une AUTRE parcelle de la zone 12 existe est un faux "
         f"succes."),
        # Hypothesis revised (v7): texte 5.x reel (chiffres rendus par le
        # provider) au lieu des chiffres locaux, et la restauration doit
        # aussi REPONDRE succes (le faux vert de la version precedente :
        # valeur relue juste, resultat success=False). Second tour : prouve
        # sur un provider a type de colonne, seul endroit ou le fuseau d'une
        # ligne supprimee est etabli (voir la docstring) ; la GeoPackage
        # reste mesuree par legacy_datetime_never_written_shifted.
        ("legacy_datetime_restored_at_its_wall_clock",
         d.get("col_dt_restored") == ["2026-01-15T14:30:00"]
         and (d.get("col_dt_result") or {}).get("success") is True,
         f"evenement 5.x {d.get('col_dt_v5_text')!r} (14:30 locales, colonne "
         f"typee) restaure par restore_deleted_feature : relu "
         f"{d.get('col_dt_restored')} resultat={d.get('col_dt_result')}. Les "
         f"chemins de restauration doivent transmettre "
         f"event.event_schema_version ET serialization.provider_time_specs"
         f"(couche) a iter_mapped_attributes, et signaler les champs de "
         f"legacy_datetime_refusals."),
        ("legacy_datetime_never_written_shifted",
         d.get("dt_restored") == ["2026-01-15T14:30:00"]
         or (d.get("dt_restored") == []
             and (d.get("dt_result") or {}).get("success") is not True
             and "dtm" in str(d.get("dt_result"))),
         f"evenement 5.x {d.get('dt_v5_text')!r} : relu {d.get('dt_restored')} "
         f"resultat={d.get('dt_result')}. Restaure a 14:30, ou refuse en "
         f"nommant le champ sans rien ecrire ; jamais ecrit decale de "
         f"l'offset UTC."),
        # Hypothesis revised (v7): texte 'old' reel de chaque provider. Sur
        # une GeoPackage, le texte 'new' 5.x est celui du tampon d'edition
        # (saisie locale) alors que la GeoPackage rend la valeur en UTC : le
        # type de la saisie n'etant enregistre nulle part, l'etat d'apres
        # l'edition ne peut pas etre prouve et le seul resultat juste y est
        # "exact ou refus nomme". Second tour : la SQLite OGR garde le
        # fuseau de CHAQUE valeur, l'ancienne valeur ecrasee n'y est donc pas
        # etablie non plus (exact ou refus nomme, exige ci-dessous) ; le
        # cablage est prouve sur une colonne typee, ou la valeur DOIT revenir.
        ("legacy_datetime_rewind_update_at_its_wall_clock",
         ((d.get("upd") or {}).get("COLONNE") or {}).get("restored")
         == ["2026-01-15T14:30:00"]
         and ((((d.get("upd") or {}).get("COLONNE") or {}).get("result")
               or {}).get("success") is True),
         f"rewind d'un UPDATE 5.x (dtm 14:30 -> 16:00, heure murale) par "
         f"_buffer_update sur une colonne typee : "
         f"{(d.get('upd') or {}).get('COLONNE')}. "
         f"La valeur d'avant l'edition doit revenir a 14:30 : _buffer_update "
         f"doit transmettre la version ET provider_time_specs(couche) ; lue "
         f"en UTC, elle revient a 15:30 et est annoncee appliquee."),
        ("legacy_datetime_rewind_update_never_written_shifted",
         all(res.get("restored") == ["2026-01-15T14:30:00"]
             or (res.get("restored") == ["2026-01-15T16:00:00"]
                 and (res.get("result") or {}).get("success") is not True
                 and bool((res.get("result") or {}).get("reason_code")
                          or (res.get("result") or {}).get("exception")))
             for res in ((d.get("upd") or {}).get(drv) or {}
                         for drv in ("GPKG", "SQLite", "COLONNE"))),
         f"rewind d'un UPDATE 5.x : {d.get('upd')}. Sur chaque provider, la "
         f"valeur revient a 14:30, ou elle reste a 16:00 avec un refus nomme ; "
         f"jamais decalee de l'offset UTC."),
        assert_log_contains(
            ctx.records,
            rf"rw_identity_serial_consumer_wiring run: trace_id={ctx.trace_id}",
            name="trace_id_propagated"),
    ]


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
