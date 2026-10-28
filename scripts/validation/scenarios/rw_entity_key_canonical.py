# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_entity_key_canonical - RecoverLand validation runtime.

Invariant: I-3. Le moteur Rewind et le moteur Review doivent designer la
MEME entite par la meme cle.
Cause racine: chacun avait sa propre notion d'entite quand
``entity_fingerprint`` est absent.

Le trou prouve ici
------------------
Deux moteurs coexistent dans le plugin:

  * REVIEW reconstruit un etat a T (``temporal_snapshot_engine``) et groupe
    par ``compute_entity_key``, qui canonicalise l'identite: ``{"fid": 5}``
    devient ``fid:5``;
  * REWIND rejoue les compensations (``rewind_dedup``) et groupait par la
    CHAINE JSON BRUTE ``feature_identity_json``.

Tant que le JSON est rendu a l'identique, personne ne voit rien. Mais la
chaine depend du rendu: ordre des cles, espaces. Il suffit que
``compute_feature_identity`` construise son dictionnaire dans un autre ordre
-- une refonte, une cle ajoutee, une version differente du plugin ayant
ecrit une partie du journal -- pour que l'historique d'une meme entite se
retrouve reparti sur plusieurs seaux de dedup. Chaque seau est alors
collapse pour lui-meme: la regle "INSERT puis DELETE = no-op" ne voit plus
la paire, une chaine d'UPDATE perd son maillon le plus ancien, et le rewind
laisse l'entite dans un etat intermediaire. Review, lui, continue d'afficher
une seule entite: l'utilisateur voit juste dans la lentille ce que le rewind
ne saura pas produire.

Concerne les lignes sans ``entity_fingerprint``, c'est-a-dire les journaux
anterieurs au retro-remplissage, et toute ligne pour laquelle le calcul de
l'empreinte a echoue.

Le correctif fait deleguer ``rewind_dedup._entity_key`` a la fonction du
moteur Review: une seule definition de ce qu'est une entite, partagee.

Disposition (pur offline, ni SQLite ni QGIS):
    cas A: trois rendus JSON de la meme identite -> une seule cle
    cas B: ordre des cles inverse sur une identite a cle primaire -> idem
    cas C: le dedup reel voit bien UNE chaine et applique sa regle de
           collapse, au lieu de trois chaines d'un evenement
    cas D: garde-fou -- deux entites reellement distinctes restent distinctes,
           et deux couches differentes ne fusionnent pas

Verdict pre-correctif: FAIL.
Verdict post-correctif: PASS.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_entity_key_canonical"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS = "rw_ekc_datasource"
_DS_AUTRE = "rw_ekc_autre_couche"


def _event(eid, ts, op, identity_json, ds=_DS, fp=None):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=eid,
        project_fingerprint="rw_ekc_project",
        datasource_fingerprint=ds,
        layer_id_snapshot="lyr",
        layer_name_snapshot="couche",
        provider_type="ogr",
        feature_identity_json=identity_json,
        operation_type=op,
        attributes_json=json.dumps({"all_attributes": {"v": eid}})
        if op in ("INSERT", "DELETE")
        else json.dumps({"changed_only": {"v": {"old": eid - 1, "new": eid}}}),
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:4326",
        field_schema_json=None,
        user_name="tester",
        session_id=None,
        created_at=ts.isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=fp,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _preflight(actifs, t0):
    """(verdict, raisons bloquantes) du planificateur sur le plan *actifs*."""
    if not actifs:
        return ("vide", [])
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.restore_planner import (
        plan_temporal_restore, preflight_check)
    plan = plan_temporal_restore(
        actifs, _DS, "couche",
        RestoreCutoff(CutoffType.BY_DATE, t0.isoformat(), inclusive=True))
    report = preflight_check(plan)
    return (report.verdict.name, list(report.blocking_reasons))


# La meme entite, rendue de trois facons differentes.
_RENDUS = ('{"fid": 5}', '{"fid":5}', '{ "fid" : 5 }')


def setup(ctx):
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)
    ctx.data["t0"] = t0

    # Cas C: INSERT puis DELETE de la MEME entite, ecrits avec deux rendus
    # differents. Vu comme une seule chaine, c'est un no-op net (l'entite est
    # nee et morte apres le cutoff): rien a compenser. Vu comme deux entites,
    # le plugin planifie deux compensations, dont une suppression sur une
    # entite qu'il vient de recreer.
    ctx.data["chaine_ins_del"] = [
        _event(2, t0 + timedelta(seconds=20), "DELETE", '{"fid":9}'),
        _event(1, t0 + timedelta(seconds=10), "INSERT", '{"fid": 9}'),
    ]
    # Cas C bis: la meme paire, le DELETE retirant exactement ce que l'INSERT
    # a cree. La chaine se prouve alors par son contenu: plan vide, sans
    # refus.
    ctx.data["chaine_ins_del_meme_etat"] = [
        _event(2, t0 + timedelta(seconds=20), "DELETE", '{"fid":9}')._replace(
            attributes_json=json.dumps({"all_attributes": {"v": 1}})),
        _event(1, t0 + timedelta(seconds=10), "INSERT", '{"fid": 9}'),
    ]

    # Cas D: deux entites reellement distinctes, et la meme entite dans une
    # autre couche.
    ctx.data["distinctes"] = [
        _event(11, t0 + timedelta(seconds=30), "UPDATE", '{"fid": 1}'),
        _event(12, t0 + timedelta(seconds=31), "UPDATE", '{"fid": 2}'),
        _event(13, t0 + timedelta(seconds=32), "UPDATE", '{"fid": 1}', ds=_DS_AUTRE),
    ]

    flog(
        f"rw_entity_key_canonical setup: trace_id={ctx.trace_id} "
        f"rendus={len(_RENDUS)} datasource={_DS}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.rewind_dedup import (
        _entity_key, collapse_rewind_events_with_stats,
    )
    from recoverland.core.temporal_snapshot_engine import compute_entity_key
    from recoverland.core.logger import flog
    from recoverland.core import rewind_dedup

    refusal_reason = getattr(rewind_dedup, "refusal_reason", lambda e: "")
    t0 = ctx.data["t0"]

    flog(f"rw_entity_key_canonical run start: trace_id={ctx.trace_id}", "INFO")

    # --- cas A: un seul et meme objet, trois rendus ----------------------
    ctx.data["cles_rewind"] = sorted({
        _entity_key(_event(1, t0, "UPDATE", rendu)) for rendu in _RENDUS
    })
    ctx.data["cles_review"] = sorted({
        compute_entity_key(None, rendu) for rendu in _RENDUS
    })

    # --- cas B: ordre des cles inverse sur une identite a cle primaire ---
    a = json.dumps({"fid": 7, "pk_field": "gid", "pk_value": 42})
    b = json.dumps({"pk_value": 42, "pk_field": "gid", "fid": 7})
    ctx.data["cle_pk_a"] = _entity_key(_event(1, t0, "UPDATE", a))
    ctx.data["cle_pk_b"] = _entity_key(_event(2, t0, "UPDATE", b))

    # --- cas C: le dedup reel ---------------------------------------------
    actifs, stats = collapse_rewind_events_with_stats(ctx.data["chaine_ins_del"])
    ctx.data["ins_del_actifs"] = sorted(
        e.event_id for e in actifs if e.event_id is not None)
    ctx.data["ins_del_stats"] = dict(stats)
    ctx.data["ins_del_refus"] = [refusal_reason(e) for e in actifs
                                 if refusal_reason(e)]
    ctx.data["ins_del_preflight"] = _preflight(actifs, t0)
    actifs, _stats = collapse_rewind_events_with_stats(
        ctx.data["chaine_ins_del_meme_etat"])
    ctx.data["ins_del_meme_etat_actifs"] = sorted(
        e.event_id for e in actifs if e.event_id is not None)

    # --- cas D: garde-fou --------------------------------------------------
    ctx.data["cles_distinctes"] = sorted({
        _entity_key(e) for e in ctx.data["distinctes"]
    })

    flog(
        f"rw_entity_key_canonical run end: trace_id={ctx.trace_id} "
        f"cles_rewind={ctx.data['cles_rewind']} "
        f"ins_del_actifs={ctx.data['ins_del_actifs']} "
        f"cles_distinctes={len(ctx.data['cles_distinctes'])}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    cles_rewind = ctx.data.get("cles_rewind") or []
    cles_review = ctx.data.get("cles_review") or []

    # ===== Cas A: le trou ================================================
    out.append((
        "rendus_json_donnent_une_seule_cle",
        len(cles_rewind) == 1,
        f"Rewind produit {len(cles_rewind)} cle(s) pour un seul objet: "
        f"{cles_rewind}. Chaque cle est un seau de dedup separe, donc "
        f"l'historique de l'entite est decoupe et les regles de collapse ne "
        f"voient plus la chaine complete.",
    ))
    out.append((
        "review_et_rewind_saccordent",
        len(cles_review) == 1 and len(cles_rewind) == 1,
        f"review={cles_review} rewind={cles_rewind}: les deux moteurs doivent "
        f"designer la meme entite, sinon la lentille montre un etat que le "
        f"rewind ne sait pas produire.",
    ))

    # ===== Cas B =========================================================
    out.append((
        "ordre_des_cles_sans_effet",
        ctx.data.get("cle_pk_a") == ctx.data.get("cle_pk_b"),
        f"a={ctx.data.get('cle_pk_a')!r} b={ctx.data.get('cle_pk_b')!r}: la "
        f"meme entite ecrite avec les cles JSON dans un autre ordre doit "
        f"rester la meme entite.",
    ))

    # ===== Cas C: consequence reelle sur le dedup ========================
    # Hypothesis revised (v7): "an INSERT then a DELETE of the same FID is
    # one entity born and dead after the cutoff" is false on a key that
    # names a RECORD: the INSERT created {"v": 1}, the DELETE removed
    # {"v": 2} with no UPDATE between them, which is either a feature
    # changed outside the journal or another feature the record number
    # named after a shapefile repack; nothing in these rows (no session)
    # tells which. Cancelling is then a guess, and keeping both halves the
    # executor's resurrection path. Accepted now: the plan is empty, or it
    # holds ONE named refusal that names BOTH events and the planner
    # blocks it (nothing is compensated). The check keeps its purpose and
    # its strength: a refusal naming both events exists only if the three
    # renderings landed in ONE bucket; split across buckets, the plan
    # holds two compensations and no such refusal, and the preflight goes.
    # The equal-state variant (cas C bis) must still give an empty plan.
    refus = ctx.data.get("ins_del_refus") or []
    verdict, raisons = ctx.data.get("ins_del_preflight") or ("?", [])
    un_seau = (len(refus) == 1 and "event 1 (" in refus[0]
               and "event 2 (" in refus[0] and verdict == "BLOCKED")
    out.append((
        "insert_puis_delete_reste_un_no_op",
        ctx.data.get("ins_del_actifs") == [] or un_seau,
        f"actifs={ctx.data.get('ins_del_actifs')} refus={refus} "
        f"preflight={verdict} {raisons[:2]} attendu=[] ou un refus nomme des "
        f"deux evenements: l'entite est nee et morte apres le cutoff, il n'y "
        f"a rien a compenser. Si les deux evenements tombent dans des seaux "
        f"differents, le plugin planifie une re-creation puis une suppression "
        f"sur une entite qui n'aurait jamais du bouger. "
        f"stats={ctx.data.get('ins_del_stats')}",
    ))
    out.append((
        "insert_puis_delete_meme_etat_annule",
        ctx.data.get("ins_del_meme_etat_actifs") == [],
        f"actifs={ctx.data.get('ins_del_meme_etat_actifs')} attendu=[]: le "
        f"DELETE retire exactement ce que l'INSERT a cree, sur les rendus du "
        f"meme FID; rien a compenser, rien a refuser.",
    ))

    # ===== Cas D: pas de sur-fusion ======================================
    out.append((
        "entites_distinctes_restent_distinctes",
        len(ctx.data.get("cles_distinctes") or []) == 3,
        f"cles={ctx.data.get('cles_distinctes')}: fid:1 et fid:2 de la meme "
        f"couche, plus fid:1 d'une AUTRE couche, doivent faire trois cles. "
        f"Canonicaliser ne doit pas fusionner ce qui est different.",
    ))

    # ===== Une seule definition partagee =================================
    src = (_PLUGIN_ROOT / "core" / "rewind_dedup.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "cle_dentite_deleguee_au_moteur_review",
        "compute_entity_key" in src,
        "rewind_dedup._entity_key doit deleguer a "
        "temporal_snapshot_engine.compute_entity_key: deux definitions de ce "
        "qu'est une entite finissent toujours par diverger.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rw_entity_key_canonical.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
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
