# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_rewind_rarity_index - RecoverLand validation runtime.

Invariant: I-REWIND-01 sous charge (un rewind de masse s'execute en un
temps lineaire en nombre de compensations) sans affaiblir I-3 (la preuve
de rarete d'une cle reste exacte).
Constat resolu: item 4 du tour executeur, mesure par
rw_rewind_mass_exec_bench: 1500 compensations sur 100 entites 3,6 s,
4500 sur 300 entites 20 s, 7500 sur 500 entites 43 s.

Le mecanisme
------------
Chaque compensation d'UPDATE sur une cle prouve sa cible: la valeur
restauree doit etre RARE dans l'etat committe (la base de reference du
rewind, `restore_service._key_rarity`). Le fournisseur est d'abord
interroge avec un filtre sur la valeur vivante de la cible; ce filtre
n'est cru que s'il retourne la cible elle-meme (sinon un fournisseur qui
evalue le filtre autrement pourrait sous-compter). Dans une chaine de K
editions d'une entite, a partir de la deuxieme compensation la cible
porte dans le TAMPON une valeur que l'etat committe n'a pas: le filtre ne
la voit pas, et la preuve relit TOUTE la couche. K x N compensations, une
relecture de N entites chacune: un cout en N^2, pour une base de
reference qui ne change pas pendant tout le rewind (le rewind n'ecrit que
dans le tampon).

Ce que l'utilisateur voit
-------------------------
Le rewind d'une journee de saisie sur une grosse couche fige QGIS de
longues minutes (la barre de progression n'avance plus).

Ce que le correctif doit garder
-------------------------------
La lecture unique de l'etat committe par session de rewind ne doit rien
changer au verdict: une valeur partagee reste partagee (refus nomme), une
valeur rare reste rare, et une session dont le tampon a ete committe en
cours de route ne reutilise pas une lecture perimee.

Cas (vraie GeoPackage, vrai tampon d'edition, remap partage comme dans
le StrictRestoreRunner)
------------------------------------------------------------------------
    B1  chaine de K=15 editions de 'val' sur N=100 entites (le banc):
        toutes les compensations APPLIED, et le nombre d'entites lues
        dans le fournisseur pour les preuves de rarete reste lineaire
        (au plus 3 par compensation, contre ~N avant)
    B2  valeur partagee ('flag: False -> True', les autres lignes a True)
        dans une session deja chaude: refus nomme value_shared, comme sans
        la lecture unique
    B3  meme session, le tampon est committe puis rouvert, une autre ligne
        prend la valeur restauree dans l'etat committe: la preuve suivante
        la voit (refus), la lecture de la session precedente n'est pas
        reutilisee

Verdict avant correctif: FAIL (B1: lectures en N^2). Apres: PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time

SCENARIO_ID = "rw_rewind_rarity_index"
INVARIANT = "I-REWIND-01"

_N = int(os.environ.get("RL_RARITY_N", "100"))
_K = int(os.environ.get("RL_RARITY_K", "15"))


def _make_layer(tmp, name, rows):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(12)"
                         "&field=val:integer&field=flag:boolean", "m", "memory")
    mem.startEditing()
    for i, (code, val, flag) in enumerate(rows):
        f = QgsFeature(mem.fields())
        f.setAttributes([code, val, flag])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i, i)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmp, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = name
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    from qgis.core import QgsVectorLayer as Lyr
    return Lyr(f"{path}|layername={name}", name, "ogr")


def _update_event(eid, layer, fid, field, old, new, minute):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.serialization import serialize_field_schema
    identity = json.dumps({"fid": fid, "pk_field": "fid", "pk_value": fid})
    return AuditEvent(
        event_id=eid, project_fingerprint="p", datasource_fingerprint="ds",
        layer_id_snapshot=layer.id(), layer_name_snapshot=layer.name(),
        provider_type="ogr", feature_identity_json=identity,
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {field: {
            "old": old, "new": new}}}),
        geometry_wkb=None, geometry_type="Point", crs_authid="EPSG:2154",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="u", session_id=None,
        created_at=f"2026-01-01T{1 + minute // 60:02d}:{minute % 60:02d}:00+00:00",
        restored_from_event_id=None, entity_fingerprint=f"pk:fid={fid}",
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)


class _ReadCounter:
    """Counts the features the rarity proofs read through the provider."""

    def __init__(self):
        self.features = 0
        self._orig = None

    def __enter__(self):
        from recoverland.core import restore_service as rs
        self._orig = rs._scan
        counter = self

        def counting(source, request=None, what="", provider_only=False):
            for feature in counter._orig(source, request, what,
                                         provider_only=provider_only):
                if "rarity" in what or "committed" in what:
                    counter.features += 1
                yield feature
        rs._scan = counting
        return self

    def __exit__(self, *exc):
        from recoverland.core import restore_service as rs
        rs._scan = self._orig
        return False


def setup(ctx):
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rw_rri_")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_executor import _apply_via_buffer
    tmp, tid = ctx.data["tmp"], ctx.trace_id
    out = {}
    try:
        # B1: the bench shape, values unique per entity.
        rows = [(f"B{i:04d}", i * 1000 + _K, True) for i in range(_N)]
        layer = _make_layer(tmp, "b1", rows)
        fids = {f["code"]: f.id() for f in layer.getFeatures()}
        actions = []
        eid = 0
        for k in reversed(range(_K)):          # newest edit first
            for i in range(_N):
                eid += 1
                actions.append(_update_event(
                    eid, layer, fids[f"B{i:04d}"], "val", i * 1000 + k,
                    i * 1000 + k + 1, k))
        remap = {}
        layer.startEditing()
        statuses = {}
        t0 = time.monotonic()
        with _ReadCounter() as counter:
            for event in actions:
                result = _apply_via_buffer(layer, "UPDATE", event, remap,
                                           trace_id=tid)
                key = (result.get("status"), result.get("reason_code"))
                statuses[str(key)] = statuses.get(str(key), 0) + 1
        out["b1_s"] = round(time.monotonic() - t0, 2)
        out["b1_reads"] = counter.features
        out["b1_statuses"] = statuses
        out["b1_commit"] = bool(layer.commitChanges())
        out["b1_restored"] = sum(1 for f in layer.getFeatures()
                                 if f["val"] % 1000 == 0)

        # B2 / B3: one session, a shared value, then a commit mid-session.
        rows = [(f"F{i}", i, True) for i in range(6)]
        rows[2] = ("F2", 2, False)
        rows[4] = ("F4", 4, False)
        layer = _make_layer(tmp, "b2", rows)
        fids = {f["code"]: f.id() for f in layer.getFeatures()}
        remap = {}
        layer.startEditing()
        # Warm the session on a unique value first ('val: 100 -> 0' of F0).
        warm = _update_event(900, layer, fids["F0"], "val", 100, 0, 1)
        idx = layer.fields().indexOf("val")
        out["b2_warm"] = dict(_apply_via_buffer(layer, "UPDATE", warm, remap,
                                                trace_id=tid))
        # F2 was edited flag False -> True; F4 still False, the rest True.
        # Committed without leaving the edit session: the SAME edit buffer
        # goes on, only the committed state moved.
        fidx = layer.fields().indexOf("flag")
        layer.changeAttributeValue(fids["F2"], fidx, True)
        layer.commitChanges(False)
        shared = _update_event(901, layer, fids["F2"], "flag", False, True, 2)
        out["b2"] = dict(_apply_via_buffer(layer, "UPDATE", shared, remap,
                                           trace_id=tid))
        # B3: commit, then F5 takes val 5 -> 3 in the committed state; the
        # restore of F3 'val: 33 -> 3' must see F5 holding 3.
        layer.commitChanges(False)
        layer.changeAttributeValue(fids["F3"], idx, 3)
        layer.changeAttributeValue(fids["F5"], idx, 3)
        layer.commitChanges(False)
        late = _update_event(902, layer, fids["F3"], "val", 33, 3, 3)
        out["b3"] = dict(_apply_via_buffer(layer, "UPDATE", late, remap,
                                           trace_id=tid))
        layer.rollBack()
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    ctx.data["out"] = out
    flog(f"rw_rewind_rarity_index run: trace_id={tid} n={_N} k={_K} "
         f"b1_s={out.get('b1_s')} b1_reads={out.get('b1_reads')} "
         f"b1={out.get('b1_statuses')} "
         f"b2={(out.get('b2') or {}).get('reason_code')} "
         f"b3={(out.get('b3') or {}).get('reason_code')}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = [("no_crash", "error" not in out,
            f"{out.get('error')} {out.get('trace', '')}")]
    total = _N * _K
    applied = (out.get("b1_statuses") or {}).get(str(("APPLIED", "applied")), 0)
    res.append((
        "b1_every_compensation_applied",
        applied == total and out.get("b1_commit") is True
        and out.get("b1_restored") == _N,
        f"statuts={out.get('b1_statuses')} commit={out.get('b1_commit')} "
        f"revenues={out.get('b1_restored')}/{_N}",
    ))
    reads = out.get("b1_reads")
    res.append((
        "b1_rarity_reads_stay_linear",
        reads is not None and reads <= 3 * total,
        f"{reads} entites lues par les preuves de rarete pour {total} "
        f"compensations sur {_N} entites (plafond {3 * total}); en "
        f"{out.get('b1_s')} s. Une relecture de la couche par compensation "
        f"donne ~{(_K - 1) * _N * _N}: un rewind de masse fige QGIS.",
    ))
    b2 = out.get("b2") or {}
    res.append((
        "b2_shared_value_still_refused_in_a_warm_session",
        b2.get("success") is False
        and "value_shared" in (b2.get("message") or ""),
        f"{b2}: 'flag: False -> True' ou d'autres lignes portent True ne "
        f"corrobore pas la cle; la lecture unique ne doit pas changer ce "
        f"verdict.",
    ))
    b3 = out.get("b3") or {}
    res.append((
        "b3_commit_mid_session_is_seen",
        b3.get("success") is False
        and "shared" in (b3.get("message") or ""),
        f"{b3}: F5 porte 3 dans l'etat committe depuis le dernier commit; une "
        f"lecture de la session d'avant le commit la croirait rare.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rw_rewind_rarity_index run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
