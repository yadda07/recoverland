# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_classify_and_marker - RecoverLand validation runtime.

Invariant: fail-safe (une garde ne depend pas de ses appelants) et
fail-loud (la classe d'un refus nomme ne depend pas de sa formulation).
Constats resolus: item 7 (parties a et b) du tour executeur.

Le mecanisme
------------
a. `restore_service._classify_restore_result` classe chaque resultat
   dans l'une des six classes que le StrictRestoreRunner lit pour decider
   s'il annule la couche (classe "failed": echec dur) ou s'il continue
   (target_absent, geometry_drift: refus mous). Les refus de SCR
   (reason_code crs_ambiguous, crs_mismatch) n'etaient pas listes: ils
   tombaient dans l'analyse du TEXTE du message. Leur classe dependait
   donc de leur formulation: un message qui contient "target feature
   absent", ou "drift" et "tolerance", rend mou un refus que le rewind
   doit traiter comme dur (la geometrie ne peut pas etre placee: rien
   ecrit, la couche annulee plutot qu'un etat qui n'est ni T ni
   maintenant).
b. `restore_service._geometry_for_write` lisait un BLOB de longueur nulle
   comme le marqueur "pas de geometrie" sans regarder la version du
   schema de l'evenement. Avant le schema 7, un tel BLOB est une ligne
   tronquee: l'ecrire EFFACE la geometrie vivante de l'entite. Seuls ses
   appelants actuels le protegeaient (`_legacy_empty_blob_refusal` avant
   l'appel); tout nouvel appelant qui l'oubliait effacait des geometries
   en annoncant un succes.

Ce que l'utilisateur voit
-------------------------
a. Un rewind qui ne peut pas placer une geometrie est committe autour du
   refus ou annule selon la tournure d'une phrase.
b. Une entite restauree depuis un journal 5.x abime perd sa geometrie:
   elle disparait de la carte, "Reverted".

Cas
---
    C1  crs_ambiguous / crs_mismatch, avec les messages de
        `geometry_utils.crs_refusal` ET avec des messages qui trompent
        l'analyse de texte: classe "failed" dans tous les cas
    C2  `_geometry_for_write` sur un BLOB vide d'un evenement de schema 5:
        refus corrupt_wkb (rien a ecrire); sur celui d'un evenement de
        schema 7: le marqueur, une geometrie NULLE a ecrire (temoin)

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

SCENARIO_ID = "rs_classify_and_marker"
INVARIANT = "fail-safe"


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"rs_classify_and_marker setup: trace_id={ctx.trace_id}")


def run(ctx):
    from qgis.core import QgsVectorLayer
    from recoverland.core.logger import flog
    from recoverland.core import restore_service as rs
    from recoverland.core.geometry_utils import crs_refusal
    from scripts.validation.scenarios import _restore_fixtures as fx
    out = {}
    layer = QgsVectorLayer("Point?crs=EPSG:2154&field=nom:string(10)",
                           "m", "memory")
    classes = {}
    for code in ("crs_ambiguous", "crs_mismatch"):
        _c, real = crs_refusal(code if code == "crs_ambiguous"
                               else "transform_failed", "EPSG:9999", layer)
        for tag, msg in (("real", real),
                         ("absent_wording", "Failed: target feature absent"),
                         ("drift_wording", "drift beyond tolerance")):
            classes[f"{code}:{tag}"] = rs._classify_restore_result(
                {"success": False, "status": "FAILED", "reason_code": code,
                 "message": msg})
    out["classes"] = classes

    markers = {}
    for version in (5, 7):
        event = fx.audit_event(event_id=version, operation_type="UPDATE",
                               geometry_wkb=b"", event_schema_version=version,
                               crs_authid="EPSG:2154")
        try:
            geom, err = rs._geometry_for_write(layer, event, b"", version,
                                               "rs_classify_and_marker")
            markers[version] = {
                "geom": (None if geom is None
                         else "null" if geom.isNull() else "shape"),
                "error": (err or {}).get("reason_code")}
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            markers[version] = {"crash": repr(exc)}
    out["markers"] = markers
    ctx.data["out"] = out
    flog(f"rs_classify_and_marker run: trace_id={ctx.trace_id} out={out}")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    classes = out.get("classes") or {}
    res = []
    for key in sorted(classes):
        res.append((
            f"c1_{key.replace(':', '_')}_is_a_hard_failure",
            classes[key] == "failed",
            f"classe={classes[key]!r}: un refus de SCR ne peut pas placer la "
            f"geometrie; sa classe ne doit pas dependre de la formulation "
            f"du message (un refus mou laisse le rewind committer autour).",
        ))
    m5 = (out.get("markers") or {}).get(5) or {}
    m7 = (out.get("markers") or {}).get(7) or {}
    res.append((
        "c2_empty_blob_of_a_v5_event_is_refused_by_the_writer_itself",
        m5.get("geom") is None and m5.get("error") == "corrupt_wkb",
        f"{m5}: avant le schema 7 un BLOB vide est une ligne tronquee; "
        f"le lire comme 'pas de geometrie' efface la geometrie vivante.",
    ))
    res.append((
        "c2_empty_blob_of_a_v7_event_is_the_null_marker",
        m7.get("geom") == "null" and m7.get("error") is None,
        f"{m7}: depuis le schema 7 le BLOB vide est l'etat 'sans geometrie'.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_classify_and_marker run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
