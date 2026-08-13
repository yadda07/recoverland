"""tx_exec_strict_atomicity_breach - RecoverLand validation runtime.

Invariant: BL-04 (le mode STRICT est tout-ou-rien : ce que le rapport
annonce doit correspondre a l'octet pres a ce qui est sur le disque).

Le mecanisme
------------
`restore_executor._execute_strict` promet l'atomicite via le tampon
d'edition QGIS :

    layer.startEditing()
    layer.beginEditCommand(...)
    for action in plan.actions:      # ecritures dans le TAMPON
        _apply_via_buffer(...)
    layer.endEditCommand()
    if not layer.commitChanges():    # ecriture reelle sur le disque
        layer.rollBack()
        return RestoreReport([], {0: msg}, plan.event_count)

Le raisonnement tient tant que l'echec survient DANS la boucle : le
tampon n'a rien ecrit, `destroyEditCommand()` annule tout. Il ne tient
plus quand l'echec survient au COMMIT, et c'est justement la que les
vraies contraintes du fichier se declenchent (index UNIQUE, NOT NULL,
type refuse, disque plein, verrou).

`QgsVectorLayer.commitChanges()` n'est pas une transaction unique : il
pousse les changements d'attributs, PUIS les geometries, PUIS les ajouts
d'entites, chacun par un appel provider distinct. Sur GPKG (SQLite) les
attributs sont deja ECRITS ET VALIDES sur le disque quand l'ajout d'une
entite viole un index UNIQUE. `commitChanges()` renvoie False, et
`rollBack()` ne fait que vider le tampon : il ne peut plus reprendre ce
qui est parti dans le fichier.

Le scenario d'echec concret
---------------------------
Un collegue a re-cree une entite portant le code C001, puis on demande
un REWIND qui doit (a) reculer l'entite A a son etat d'avant et
(b) re-inserer une entite supprimee qui portait elle aussi C001. Un
index UNIQUE existe sur `code`. Au commit, l'insertion est refusee.

Ce que l'utilisateur voit : "0 evenement restaure, echec".
Ce qu'il a reellement dans son fichier : l'entite A a ete reculee, nom
ET geometrie modifies, definitivement, sans aucune trace au journal
(les trace_events ne sont construits que sur le chemin succes). Il n'y
a donc plus aucun moyen de revenir en arriere : le plugin ne sait meme
pas qu'il a ecrit.

Pire, l'echec est indexe sous `event_id=0` (`{0: msg}`), un identifiant
qui n'existe pas dans le journal : l'interface ne peut pas dire QUEL
evenement a bloque le lot.

Verdict attendu : FAIL sur l'atomicite disque tant que
`_execute_strict` ne verifie pas l'etat reel apres un commit refuse.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_exec_strict_atomicity_breach"
INVARIANT = "BL-04"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_esab_datasource"
_PROJECT_FP = "tx_esab_project"
_LAYER = "zones"

# Etat sur disque avant restauration
_A_NOM = "PONT"
_A_CODE = "C001"
_A_XY = (1.0, 1.0)
_B_NOM = "GARE"
_B_CODE = "C002"

# Etat que la restauration veut ecrire sur A (evenement UPDATE)
_A_NOM_AVANT = "PONT_AVANT_EDITION"
_A_XY_AVANT = (11.0, 11.0)

# Entite supprimee que la restauration veut re-inserer : son code entre
# en collision avec l'index UNIQUE -> le commit sera refuse.
_C_NOM = "ANCIEN_PONT"
_C_CODE = "C001"

_SCHEMA = [
    {"name": "nom", "type": "String", "length": 40, "precision": 0},
    {"name": "code", "type": "String", "length": 10, "precision": 0},
    {"name": "pop", "type": "Integer", "length": 10, "precision": 0},
]


def _wkb_point(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def _make_gpkg(tmpdir: str) -> str:
    """Cree un GPKG a deux entites, avec un index UNIQUE sur `code`."""
    from osgeo import ogr

    path = str(Path(tmpdir) / "zones.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    datasource = driver.CreateDataSource(path)
    layer = datasource.CreateLayer(_LAYER, geom_type=ogr.wkbPoint)
    layer.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    layer.CreateField(ogr.FieldDefn("pop", ogr.OFTInteger))
    for nom, code, pop, (x, y) in (
        (_A_NOM, _A_CODE, 100, _A_XY),
        (_B_NOM, _B_CODE, 200, (2.0, 2.0)),
    ):
        feature = ogr.Feature(layer.GetLayerDefn())
        feature.SetField("nom", nom)
        feature.SetField("code", code)
        feature.SetField("pop", pop)
        feature.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT({x} {y})"))
        layer.CreateFeature(feature)
        feature = None
    datasource.FlushCache()
    datasource = None

    conn = sqlite3.connect(path)
    conn.execute(f"CREATE UNIQUE INDEX ux_{_LAYER}_code ON {_LAYER}(code)")
    conn.commit()
    conn.close()
    return path


def _open(path: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername={_LAYER}", _LAYER, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path}")
    return layer


def _snapshot(path: str) -> list:
    """Relit le FICHIER (couche neuve, aucun cache) et decrit son contenu."""
    layer = _open(path)
    rows = []
    for feature in layer.getFeatures():
        geom = feature.geometry()
        point = geom.asPoint() if geom is not None and not geom.isNull() else None
        rows.append({
            "nom": feature["nom"],
            "code": feature["code"],
            "xy": (round(point.x(), 4), round(point.y(), 4)) if point else None,
        })
    rows.sort(key=lambda r: (str(r["code"]), str(r["nom"])))
    del layer
    return rows


def _update_event(fid: int) -> "object":
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=10,
        project_fingerprint=_PROJECT_FP,
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_esab",
        layer_name_snapshot=_LAYER,
        provider_type="ogr",
        feature_identity_json=json.dumps(
            {"fid": fid, "pk_field": "fid", "pk_value": fid}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {
            "nom": {"old": _A_NOM_AVANT, "new": _A_NOM},
        }}),
        geometry_wkb=_wkb_point(*_A_XY_AVANT),
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps(_SCHEMA),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 10, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{fid}",
        event_schema_version=2,
        new_geometry_wkb=_wkb_point(*_A_XY),
        invalidated_at=None,
    )


def _delete_event() -> "object":
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=11,
        project_fingerprint=_PROJECT_FP,
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_esab",
        layer_name_snapshot=_LAYER,
        provider_type="ogr",
        feature_identity_json=json.dumps(
            {"fid": 77, "pk_field": "fid", "pk_value": 77}),
        operation_type="DELETE",
        attributes_json=json.dumps({"all_attributes": {
            "nom": _C_NOM, "code": _C_CODE, "pop": 300,
        }}),
        geometry_wkb=_wkb_point(3.0, 3.0),
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=json.dumps(_SCHEMA),
        user_name="tester",
        session_id=None,
        created_at=datetime(2026, 8, 12, 11, 0, 0, tzinfo=timezone.utc).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint="fid:77",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_esab_")
    ctx.data["tmpdir"] = tmpdir
    path = _make_gpkg(tmpdir)
    ctx.data["path"] = path
    ctx.data["before"] = _snapshot(path)

    layer = _open(path)
    fids = [f.id() for f in layer.getFeatures()]
    ctx.data["fid_a"] = fids[0]
    ctx.data["layer"] = layer

    flog(
        f"tx_exec_strict_atomicity_breach setup: trace_id={ctx.trace_id} "
        f"path={path} fid_a={ctx.data['fid_a']} before={ctx.data['before']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_contracts import (
        CutoffType, RestoreCutoff, RestoreScope,
    )
    from recoverland.core.restore_executor import (
        execute_restore_plan, build_restore_session,
    )
    from recoverland.core.restore_planner import plan_temporal_restore

    layer = ctx.data["layer"]
    upd = _update_event(ctx.data["fid_a"])
    dele = _delete_event()
    events_by_id = {10: upd, 11: dele}

    cutoff = RestoreCutoff(
        cutoff_type=CutoffType.BY_DATE,
        value=datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc).isoformat(),
    )
    plan = plan_temporal_restore(
        [dele, upd], _DS_FP, _LAYER, cutoff, scope=RestoreScope.LAYER)
    ctx.data["plan_ops"] = [a.compensatory_op for a in plan.actions]
    ctx.data["atomicity"] = plan.atomicity.value

    flog(f"tx_exec_strict_atomicity_breach run: trace_id={ctx.trace_id} "
         f"ops={ctx.data['plan_ops']}", "INFO")

    report = execute_restore_plan(
        plan, events_by_id, layer, trace_id=ctx.trace_id)

    ctx.data["succeeded"] = list(report.succeeded)
    ctx.data["failed"] = {int(k): str(v)[:160] for k, v in report.failed.items()}
    ctx.data["traces"] = len(report.trace_events or ())
    ctx.data["session_status"] = build_restore_session(
        plan, report, "2026-08-12T12:00:00+00:00",
        "2026-08-12T12:00:01+00:00").status
    ctx.data["layer_editable_after"] = bool(layer.isEditable())

    # On lache toute reference QGIS AVANT de relire le fichier, sinon la
    # relecture pourrait servir un cache provider au lieu du disque.
    ctx.data["layer"] = None
    del layer
    ctx.data["after"] = _snapshot(ctx.data["path"])

    flog(
        f"tx_exec_strict_atomicity_breach done: trace_id={ctx.trace_id} "
        f"succeeded={ctx.data['succeeded']} failed={ctx.data['failed']} "
        f"traces={ctx.data['traces']} after={ctx.data['after']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    before = ctx.data.get("before") or []
    after = ctx.data.get("after") or []
    succeeded = ctx.data.get("succeeded") or []
    failed = ctx.data.get("failed") or {}
    traces = ctx.data.get("traces") or 0

    def _row(rows, nom_or_code):
        for row in rows:
            if nom_or_code in (row.get("nom"), row.get("code")):
                return row
        return {}

    a_before = _row(before, _A_NOM)
    a_after = _row(after, _A_CODE)

    # ===== Amorcage =====================================================
    out.append((
        "fixture_seeded",
        len(before) == 2 and a_before.get("xy") == _A_XY,
        f"etat initial={before}: le scenario a besoin des deux entites "
        f"A({_A_NOM}) et B({_B_NOM}) avec A en {_A_XY}",
    ))
    out.append((
        "plan_is_strict_insert_first",
        ctx.data.get("atomicity") == "strict"
        and ctx.data.get("plan_ops") == ["INSERT", "UPDATE"],
        f"atomicite={ctx.data.get('atomicity')} ops={ctx.data.get('plan_ops')}: "
        f"le rewind temporel doit passer en STRICT et ordonner l'INSERT "
        f"compensatoire avant l'UPDATE, sinon le scenario ne teste pas le "
        f"chemin tout-ou-rien",
    ))

    # ===== Ce que le plugin ANNONCE ====================================
    out.append((
        "report_announces_total_failure",
        len(succeeded) == 0 and bool(failed),
        f"succeeded={succeeded} failed={failed}: le commit a ete refuse, "
        f"le rapport doit bien annoncer un echec (sinon le scenario ne "
        f"reproduit pas la panne visee)",
    ))

    # ===== Ce qui est REELLEMENT sur le disque ==========================
    out.append((
        "disk_attribute_not_half_written",
        a_after.get("nom") == _A_NOM,
        f"le rapport annonce 0 restauration mais le fichier contient "
        f"nom={a_after.get('nom')!r} au lieu de {_A_NOM!r}: la restauration a "
        f"ete ecrite sur le disque et validee alors que l'utilisateur est "
        f"informe que RIEN n'a ete restaure. Il ne saura jamais que cette "
        f"entite a bouge.",
    ))
    out.append((
        "disk_geometry_not_half_written",
        a_after.get("xy") == _A_XY,
        f"le rapport annonce 0 restauration mais l'entite est passee de "
        f"{_A_XY} a {a_after.get('xy')}: la geometrie a ete deplacee "
        f"definitivement pendant un lot declare en echec",
    ))
    out.append((
        "no_orphan_feature_left_behind",
        len(after) == len(before),
        f"nombre d'entites avant={len(before)} apres={len(after)} "
        f"({after}): un lot en echec ne doit laisser aucune entite "
        f"compensatoire derriere lui",
    ))

    # ===== Le rapport doit rester exploitable ===========================
    out.append((
        "failure_is_attributed_to_a_real_event",
        bool(failed) and 0 not in failed,
        f"failed={failed}: l'echec est indexe sous event_id=0, un "
        f"identifiant qui n'existe pas au journal. L'utilisateur voit "
        f"\"echec\" sans pouvoir savoir quel evenement a bloque le lot ni "
        f"lesquels avaient deja ete appliques.",
    ))
    disk_changed = after != before
    out.append((
        "a_disk_write_is_always_journalled",
        (not disk_changed) or traces > 0,
        f"le disque a change (avant={before} apres={after}) mais le rapport "
        f"ne porte aucun evenement de trace (traces={traces}): le journal "
        f"ignore cette ecriture, donc aucun rewind ni annulation ulterieure "
        f"ne pourra la retrouver",
    ))
    out.append((
        "session_status_matches_the_data",
        ("partial" if disk_changed else "failed")
        == ctx.data.get("session_status"),
        f"status de session={ctx.data.get('session_status')!r} alors que le "
        f"disque a{'' if disk_changed else ' non'} change: la session "
        f"enregistree dans l'historique doit refleter l'etat reel des "
        f"donnees, pas l'intention",
    ))
    out.append((
        "layer_left_clean",
        ctx.data.get("layer_editable_after") is False,
        f"isEditable apres coup={ctx.data.get('layer_editable_after')}: un lot "
        f"echoue doit rendre la couche dans l'etat ou il l'a trouvee, sinon "
        f"l'utilisateur herite d'une session d'edition ouverte qu'il n'a pas "
        f"demandee",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"\[{ctx.trace_id}\] strict_execute: commit_failed",
        name="commit_failure_logged_with_trace_id",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_exec_strict_atomicity_breach.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Garde de source ==============================================
    src = (_PLUGIN_ROOT / "core" / "restore_executor.py").read_text(
        encoding="utf-8", errors="replace")
    start = src.find("def _execute_strict(")
    end = src.find("def _build_traces_for_succeeded(", start)
    body = src[start:end] if start >= 0 and end > start else ""
    out.append((
        "commit_failure_path_inspects_the_data",
        "commitChanges" in body and any(
            token in body for token in ("dataProvider", "getFeature", "reload")
        ),
        "apres un commitChanges() refuse, _execute_strict appelle rollBack() "
        "et declare tout en echec sans jamais relire la couche: il ne peut "
        "donc pas savoir si une partie du lot est deja partie sur le disque",
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
