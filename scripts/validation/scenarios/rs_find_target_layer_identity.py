"""rs_find_target_layer_identity - RecoverLand validation runtime.

Invariant: I-3 (on n'ecrit jamais sur une couche dont l'identite n'est pas
prouvee ; refuser vaut mieux que modifier la mauvaise table, et le refus
est nomme).

Le trou que ce scenario prouve
------------------------------
`workflow_service.find_target_layer` choisit la couche ou une restauration
d'evenement (et son annulation) va ecrire :

  1. etape 1 : la couche dont l'identifiant QGIS egale
     `layer_id_snapshot`. Or "Changer la source de donnees" repointe une
     couche vers une AUTRE table (un autre fichier, une copie de
     sauvegarde, un autre service) en gardant son identifiant : la
     restauration ecrivait l'evenement de ``routes`` dans ``rivieres`` ;
  2. etape 2 : une couche dont l'empreinte "correspond". La
     correspondance est volontairement tolerante pour qu'une couche suivie
     sous 6.x reste capturee : une ancienne empreinte sans service
     correspond a la meme table atteinte par N'IMPORTE quel service. Un
     evenement capture sur ``test`` s'ecrivait donc sur la couche ``prod``
     chargee, alors que le rewind, lui, le refuse.

Attendu : l'identifiant ne suffit pas, l'empreinte (portee des alias
comprise) doit le confirmer ; une correspondance de service non prouvee
est refusee, sauf rattachement explicite (alias) ; un refus laisse une
raison lisible par l'appelant, qui la montre a l'utilisateur au lieu de
"couche non trouvee".

Verdict attendu avant correctif : FAIL (couche repointee choisie, couche
prod choisie pour un evenement sans service).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_find_target_layer_identity"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LEGACY = "postgres::host= port=5432 dbname= schema=public table=routes"
_PROD = ("postgres::service=prod host= port=5432 dbname= schema=public "
         "table=routes")


def _gpkg(tmpdir, name, table):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
        QgsFeature, QgsGeometry, QgsPointXY,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=nom:string(20)",
                         "seed", "memory")
    feat = QgsFeature(mem.fields())
    feat.setAttribute("nom", table)
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(1, 1)))
    mem.dataProvider().addFeatures([feat])
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = table
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return f"{path}|layername={table}"


def _event(fp, layer_id):
    from recoverland.core.audit_backend import AuditEvent
    return AuditEvent(
        event_id=1, project_fingerprint="p", datasource_fingerprint=fp,
        layer_id_snapshot=layer_id, layer_name_snapshot="routes",
        provider_type=fp.split("::")[0], feature_identity_json='{"fid": 1}',
        operation_type="UPDATE", attributes_json=json.dumps(
            {"changed_only": {"nom": {"old": "a", "new": "b"}}}),
        geometry_wkb=None, geometry_type="Point", crs_authid="EPSG:4326",
        field_schema_json="[]", user_name="t", session_id="s",
        created_at="2026-09-01T10:00:00+00:00", restored_from_event_id=None,
        entity_fingerprint="fid:1", event_schema_version=7,
        new_geometry_wkb=None)


def _repoint(layer, source):
    try:
        from qgis.core import QgsDataProvider
        layer.setDataSource(source, layer.name(), "ogr",
                            QgsDataProvider.ProviderOptions())
    except TypeError:
        layer.setDataSource(source, layer.name(), "ogr")


def _alias_conn(pairs):
    from recoverland.core.sqlite_schema import get_all_ddl
    conn = sqlite3.connect(":memory:")
    for ddl in get_all_ddl():
        conn.execute(ddl)
    for alias, target in pairs:
        conn.execute("INSERT INTO datasource_alias (alias_fingerprint, "
                     "target_fingerprint, created_at, note) VALUES "
                     "(?, ?, '2026-09-01', 'test')", (alias, target))
    conn.commit()
    return conn


def _name(layer):
    return None if layer is None else layer.name()


def _reason(ws, event):
    getter = getattr(ws, "target_refusal_reason", None)
    return getter(event) if getter is not None else None


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_ftl_")
    flog(f"rs_find_target_layer_identity setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core import identity as ident
    from recoverland.core import workflow_service as ws
    from recoverland.core.logger import flog

    d = ctx.data
    d["error"] = None
    project = QgsProject.instance()
    added = []
    real_fp = ident.compute_datasource_fingerprint
    try:
        tmp = d["tmpdir"]
        routes = _gpkg(tmp, "reseau", "routes")
        rivieres = _gpkg(tmp, "hydro", "rivieres")
        layer_l = QgsVectorLayer(routes, "L", "ogr")
        project.addMapLayer(layer_l, False)
        added.append(layer_l.id())
        event = _event(real_fp(layer_l), layer_l.id())
        # Temoin : identifiant et empreinte concordent.
        d["plain"] = _name(ws.find_target_layer(event))
        # Temoin : ancienne empreinte avec filtre d'affichage, meme fichier.
        legacy_filtered = _event(real_fp(layer_l) + "|subset=\"nom\"='x'",
                                 layer_l.id())
        d["legacy_filtered"] = _name(ws.find_target_layer(legacy_filtered))
        # 1. La couche est repointee vers une autre table, meme identifiant.
        _repoint(layer_l, rivieres)
        d["repointed_source"] = layer_l.source()
        d["repointed_alone"] = _name(ws.find_target_layer(event))
        d["repointed_alone_reason"] = _reason(ws, event)
        # 2. La bonne table est chargee par une autre couche.
        layer_m = QgsVectorLayer(routes, "M", "ogr")
        project.addMapLayer(layer_m, False)
        added.append(layer_m.id())
        d["repointed_with_right_layer"] = _name(ws.find_target_layer(event))

        # 3. Evenement d'avant v7 sans service ; couche chargee par prod.
        layer_s = QgsVectorLayer("Point?crs=EPSG:4326", "S", "memory")
        project.addMapLayer(layer_s, False)
        added.append(layer_s.id())

        def fake_fp(layer):
            if layer.id() == layer_s.id():
                return _PROD
            return real_fp(layer)
        ident.compute_datasource_fingerprint = fake_fp
        svc_event = _event(_LEGACY, "id_de_la_couche_de_test")
        d["service_unproven"] = _name(ws.find_target_layer(svc_event))
        d["service_unproven_reason"] = _reason(ws, svc_event)
        conn = _alias_conn([])
        d["service_unproven_conn"] = _name(ws.find_target_layer(svc_event, conn))
        conn.close()
        # 4. Rattache explicitement (alias) : la couche prod est la cible.
        conn = _alias_conn([(_LEGACY, _PROD)])
        d["service_aliased"] = _name(ws.find_target_layer(svc_event, conn))
        conn.close()
        # 5. Meme identifiant de couche, empreinte sans service : l'id ne
        #    prouve pas la base.
        svc_same_id = _event(_LEGACY, layer_s.id())
        d["service_same_id"] = _name(ws.find_target_layer(svc_same_id))
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        ident.compute_datasource_fingerprint = real_fp
        for lid in added:
            if project.mapLayer(lid) is not None:
                project.removeMapLayer(lid)
    flog(f"rs_find_target_layer_identity run: trace_id={ctx.trace_id} "
         f"plain={d.get('plain')} repointed_alone={d.get('repointed_alone')} "
         f"with_right={d.get('repointed_with_right_layer')} "
         f"svc={d.get('service_unproven')} aliased={d.get('service_aliased')} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    return [
        ("scenario_ran", d.get("error") is None,
         f"le scenario a plante : {d.get('error')}"),
        ("plain_match_kept", d.get("plain") == "L"
         and d.get("legacy_filtered") == "L",
         f"identifiant et empreinte concordants -> {d.get('plain')!r}, "
         f"ancienne empreinte filtree du meme fichier -> "
         f"{d.get('legacy_filtered')!r} (attendu 'L' deux fois) : le cas sain "
         f"ne doit pas etre refuse."),
        ("repointed_layer_not_trusted_by_id",
         d.get("repointed_alone") is None
         and bool(d.get("repointed_alone_reason")),
         f"couche L repointee de routes vers rivieres ({d.get('repointed_source')}) "
         f"-> {d.get('repointed_alone')!r}, raison="
         f"{d.get('repointed_alone_reason')!r}. L'evenement de routes serait "
         f"ecrit dans rivieres, par fid, et annonce reussi."),
        ("repointed_layer_skipped_for_the_right_one",
         d.get("repointed_with_right_layer") == "M",
         f"avec la couche M chargee sur routes -> "
         f"{d.get('repointed_with_right_layer')!r} (attendu 'M')."),
        ("service_unproven_refused",
         d.get("service_unproven") is None
         and d.get("service_unproven_conn") is None
         and "service" in (d.get("service_unproven_reason") or ""),
         f"evenement sans service, couche chargee par prod -> "
         f"{d.get('service_unproven')!r} (avec journal : "
         f"{d.get('service_unproven_conn')!r}), raison="
         f"{d.get('service_unproven_reason')!r}. L'evenement peut venir de "
         f"test : l'ecrire sur prod, c'est modifier une autre base, alors "
         f"que le rewind le refuse."),
        ("service_same_id_refused",
         d.get("service_same_id") is None,
         f"meme identifiant de couche, empreinte sans service -> "
         f"{d.get('service_same_id')!r} : l'identifiant d'une couche ne dit "
         f"pas quelle base son service designait a l'epoque."),
        ("service_aliased_accepted",
         d.get("service_aliased") == "S",
         f"historique rattache a prod par un alias -> "
         f"{d.get('service_aliased')!r} (attendu 'S') : le refus doit rester "
         f"cible."),
        assert_log_contains(
            ctx.records,
            rf"rs_find_target_layer_identity run: trace_id={ctx.trace_id}",
            name="trace_id_propagated"),
    ]


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
