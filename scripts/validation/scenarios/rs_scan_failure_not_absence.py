# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_scan_failure_not_absence - RecoverLand validation runtime.

Invariant: fail-loud (aucune exception avalee sans consequence, aucun
faux succes).
Constat resolu: absence-from-failed-scans (HIGH).

Le mecanisme
------------
Pour annuler un INSERT, le plugin cherche l'entite inseree: par son
identite, puis par un balayage de la couche. Chaque lecteur
(`_find_target_feature`, `_find_by_snapshot`, `_find_by_attrs_only`, la
garde anti-doublon de la restauration d'un DELETE) avalait toute exception
et iterait `provider.getFeatures()` sans regarder si l'iterateur etait
valide. Or un provider qui ne peut pas ouvrir la lecture (connexion
PostGIS coupee, fichier verrouille) rend un iterateur INVALIDE, qui ne
produit rien: exactement ce que produit une couche ou l'entite n'est pas.
Ce silence etait lu comme une preuve d'absence: "deja a l'etat cible",
compte skipped_idempotent, succes.

Ce que l'utilisateur voit
-------------------------
La connexion PostGIS tombe quelques secondes pendant un rewind. Chaque
compensation DELETE est annoncee "deja a l'etat cible"; les entites
inserees apres la date cible restent dans la couche, et le rewind se
declare reussi pour elles. Rejouee pendant la coupure, la restauration
d'un DELETE ne voit pas l'entite deja restauree et en ajoute une copie.

Le decor
--------
Une vraie couche shapefile, enveloppee dans un mandataire dont TOUTES les
lectures rendent un `QgsFeatureIterator()` invalide (ce que rend un
provider sans connexion); les ecritures passent au vrai provider. Les
entites existent bel et bien dans le fichier.

Cas
---
    S1  restauration d'un INSERT pendant la coupure
    S2  compensation DELETE du rewind pendant la coupure
    S3  restore_batch: l'evenement ne doit pas etre compte "skipped"
    S4  rejeu de la restauration d'un DELETE deja restaure: pas de copie

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_scan_failure_not_absence"
INVARIANT = "fail-loud"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SHP_FIELDS = ["nom:string(20)", "code:string(20)"]
_ROWS = [
    (["GARE", "C1"], "Point (1 1)"),
    (["INSERE", "C2"], "Point (2 2)"),
    (["RESTAUREE", "C3"], "Point (3 3)"),
]


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


class _DeadProvider:
    """Provider whose reads fail like a dropped connection; writes pass."""

    def __init__(self, real):
        self._real = real

    def getFeatures(self, request=None):
        from qgis.core import QgsFeatureIterator
        return QgsFeatureIterator()

    def __getattr__(self, name):
        return getattr(self._real, name)


class _DeadLayer:
    """Layer proxy: every read returns an invalid iterator."""

    def __init__(self, real):
        self._real = real
        self._provider = _DeadProvider(real.dataProvider())

    def dataProvider(self):
        return self._provider

    def getFeatures(self, request=None):
        from qgis.core import QgsFeatureIterator
        return QgsFeatureIterator()

    def __getattr__(self, name):
        return getattr(self._real, name)


def _event(eid, op, nom, code, wkt, fid):
    fx = _fx()
    return fx.audit_event(
        event_id=eid, operation_type=op,
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}",
        attributes_json=json.dumps({"all_attributes": {
            "nom": nom, "code": code}}),
        geometry_wkb=fx.wkb_wkt(wkt),
        field_schema_json=fx.schema(("nom", "string"), ("code", "string")))


def setup(ctx):
    from recoverland.core.logger import flog
    fx = _fx()
    tmpdir = tempfile.mkdtemp(prefix="rl_rs_sfna_")
    ctx.data["tmpdir"] = tmpdir
    for key in ("s1", "s2", "s3", "s4"):
        ctx.data[f"shp_{key}"] = fx.make_shapefile(
            tmpdir, f"reseau_{key}", "Point", _SHP_FIELDS, _ROWS)
    ctx.data["before"] = fx.file_rows(ctx.data["shp_s1"])
    probe = _DeadLayer(fx.open_layer(ctx.data["shp_s1"])).getFeatures()
    ctx.data["probe_invalid"] = not probe.isValid()
    flog(f"rs_scan_failure_not_absence setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir} dead_iterator_invalid={ctx.data['probe_invalid']}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        restore_inserted_feature, restore_deleted_feature, restore_batch,
    )
    fx = _fx()
    tid = ctx.trace_id
    res = {}
    flog(f"rs_scan_failure_not_absence run start: trace_id={tid}", "INFO")

    ev_ins = _event(301, "INSERT", "INSERE", "C2", "Point (2 2)", 1)
    res["s1"] = dict(restore_inserted_feature(
        _DeadLayer(fx.open_layer(ctx.data["shp_s1"])), ev_ins))
    res["s2"] = fx.buffer_apply(
        _DeadLayer(fx.open_layer(ctx.data["shp_s2"])), "DELETE", ev_ins, tid)
    report = restore_batch(_DeadLayer(fx.open_layer(ctx.data["shp_s3"])),
                           [ev_ins], fid_cache={}, trace_id=tid)
    res["s3"] = {"succeeded": list(report.succeeded),
                 "failed": {int(k): str(v)[:160]
                            for k, v in report.failed.items()},
                 "skipped_idempotent": list(report.skipped_idempotent or []),
                 "traces": len(report.trace_events or ())}
    ev_del = _event(304, "DELETE", "RESTAUREE", "C3", "Point (3 3)", 57)
    res["s4"] = dict(restore_deleted_feature(
        _DeadLayer(fx.open_layer(ctx.data["shp_s4"])), ev_del))

    ctx.data["res"] = res
    ctx.data["after"] = {key: fx.file_rows(ctx.data[f"shp_{key}"])
                         for key in ("s1", "s2", "s3", "s4")}
    flog(f"rs_scan_failure_not_absence run end: trace_id={tid} res={res}",
         "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    res = ctx.data.get("res") or {}
    after = ctx.data.get("after") or {}
    before = ctx.data.get("before") or []

    out.append((
        "fixture_reads_really_fail",
        bool(ctx.data.get("probe_invalid")),
        "le mandataire doit rendre un iterateur invalide, sinon le "
        "scenario ne simule pas une coupure",
    ))

    for key, label in (("s1", "restauration d'un INSERT"),
                       ("s2", "compensation DELETE du rewind")):
        result = res.get(key) or {}
        out.append((
            f"{key}_failed_read_is_a_failure",
            result.get("success") is False,
            f"{label} pendant la coupure: resultat={result}. L'entite "
            f"INSERE est dans le fichier; une lecture qui a echoue ne "
            f"prouve pas son absence. Annoncer 'deja absente' laisse "
            f"l'entite en place sous un rewind declare reussi.",
        ))
        out.append((
            f"{key}_failure_is_named",
            result.get("reason_code") == "scan_failed",
            f"{label}: reason_code={result.get('reason_code')!r}, attendu "
            f"'scan_failed' (l'utilisateur doit savoir que c'est la "
            f"lecture qui a echoue, pas l'entite qui manque)",
        ))
        out.append((
            f"{key}_file_untouched",
            after.get(key) == before,
            f"{label}: apres={after.get(key)} avant={before}",
        ))

    s3 = res.get("s3") or {}
    out.append((
        "s3_not_counted_skipped",
        s3.get("skipped_idempotent") == [] and s3.get("succeeded") == []
        and 301 in (s3.get("failed") or {}),
        f"restore_batch={s3}: l'evenement doit etre compte en echec, pas "
        f"en 'deja a l'etat cible'",
    ))
    out.append((
        "s3_no_trace_for_an_unproven_skip",
        s3.get("traces") == 0,
        f"traces={s3.get('traces')}: une trace neutraliserait l'INSERT "
        f"pour tous les rewinds suivants alors que rien n'a ete fait",
    ))

    s4 = res.get("s4") or {}
    out.append((
        "s4_replay_adds_no_copy",
        after.get("s4") == before,
        f"rejeu pendant la coupure: apres={after.get('s4')} avant={before} "
        f"resultat={s4}. La garde anti-doublon n'a rien lu et a conclu "
        f"'pas de doublon': une seconde copie de RESTAUREE est ajoutee.",
    ))
    out.append((
        "s4_is_a_named_failure",
        s4.get("success") is False and s4.get("reason_code") == "scan_failed",
        f"resultat={s4}: attendu un echec scan_failed",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rs_scan_failure_not_absence.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
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
