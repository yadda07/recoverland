"""mig_alias_reconciliation - RecoverLand validation runtime.

Invariant: I-11 (une empreinte de datasource designe UNE source, et
l'historique d'une source reste entier a travers les changements
d'identite).

CE QUE CE SCENARIO PROUVE
-------------------------
La migration v6 doit RECONCILIER sans jamais REECRIRE. Autrement dit,
sur un journal reel pose sur le disque, contenant des evenements ecrits
sous des empreintes anciennes :

  1. aucune ligne de `audit_event` ne doit bouger -- ni le nombre, ni les
     valeurs, ni les empreintes portees par chaque ligne. Le dump complet
     des 21 colonnes est compare avant / apres, ligne par ligne ;
  2. une ligne d'alias `ancienne -> canonique` doit apparaitre dans
     `datasource_alias` pour chaque empreinte devenue obsolete ;
  3. une lecture par l'empreinte CANONIQUE doit retrouver les evenements
     anciens : rewind (fetch + count), horizon (oldest), recherche,
     flux d'entite, et registre.

Le journal fixture rejoue le cycle utilisateur quotidien decrit dans
tx_prov_fingerprint_split : l'utilisateur edite une couche, pose un
filtre (`Couche > Filtrer`, qui ajoute `|subset=...` a la source), edite
encore, rouvre la couche autrement (`|geometrytype=`), retire le filtre
et demande un rewind. Avant le correctif, la meme couche avait TROIS
historiques disjoints. Apres, une seule empreinte canonique, deux alias.

Une quatrieme couche (`communes`) est deja canonique : elle verifie
qu'aucun alias parasite n'est ecrit pour un journal sain.

Le scenario mesure ensuite trois choses que la reconciliation NE fait
pas, et les rapporte plutot que de les supposer :
  - le dedup du rewind (`rewind_dedup._entity_key`) clef sur l'empreinte
    BRUTE de chaque evenement, pas sur l'empreinte canonique ;
  - le worker de REVIEW / Time Lens
    (`widgets.snapshot_rebuild_worker`) porte son propre SQL
    `datasource_fingerprint = ?`, hors du point d'expansion ;
  - `workflow_service.find_target_layer` compare les empreintes avec
    `==` et non `datasource_fingerprints_match`.

Enfin la reversibilite annoncee par le design est verifiee : supprimer
les deux lignes d'alias ramene EXACTEMENT le comportement d'avant.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_alias_reconciliation"
INVARIANT = "I-11"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "mig_ar_project"
_T0 = datetime(2026, 3, 2, 8, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()

# Journal fixture: (offset_s, source key, layer name, operation, entity, fid)
# The story, in order:
#   t1  edition de la couche non filtree                -> src_bare
#   t2  l'utilisateur pose un filtre, edite la meme entite -> src_filtered
#   t3  il cree une entite pendant que le filtre est pose  -> src_filtered
#   t4  il rouvre la couche en forcant le type de geometrie -> src_geomtype
#   t5  il supprime une entite dans cette ouverture-la      -> src_geomtype
#   t6  il retire le filtre et supprime l'entite creee en t3 -> src_bare
#   t7  derniere edition                                   -> src_bare
# plus deux editions sur une autre couche du meme fichier, deja canonique.
_STORY = [
    (0, "bare", "troncons", "UPDATE", "fid:1"),
    (60, "filtered", "troncons (filtre)", "UPDATE", "fid:1"),
    (120, "filtered", "troncons (filtre)", "INSERT", "fid:3"),
    (180, "geomtype", "troncons", "UPDATE", "fid:2"),
    (240, "geomtype", "troncons", "DELETE", "fid:4"),
    (300, "bare", "troncons", "DELETE", "fid:3"),
    (360, "bare", "troncons", "UPDATE", "fid:5"),
    (420, "communes", "communes", "UPDATE", "fid:9"),
    (480, "communes", "communes", "UPDATE", "fid:10"),
]


# ---------------------------------------------------------------------------
# Fixture: a real journal on disk, written at schema v5, with legacy
# fingerprints and the registry rows that were captured with them.
# ---------------------------------------------------------------------------

def _legacy_path(path: str) -> str:
    """Path exactly as the historical normaliser wrote it in a fingerprint.

    `_normalize_file_source` has always run the part before the first
    `|` through abspath + normcase + forward slashes. That part is not
    what v6 changed, so reproducing it here is enough to rebuild a
    fingerprint byte for byte as it was stored before the fix.
    """
    return os.path.normcase(os.path.abspath(path)).replace("\\", "/")


def _sources(gpkg: str) -> dict:
    """The four raw QGIS sources this fixture was captured from."""
    return {
        "bare": f"{gpkg}|layername=troncons",
        "filtered": f"{gpkg}|layername=troncons|subset=\"cat\" = 'A'",
        "geomtype": f"{gpkg}|layername=troncons|geometrytype=LineString",
        "communes": f"{gpkg}|layername=communes",
    }


def _legacy_fingerprints(gpkg: str) -> dict:
    """What the PRE-v6 `_normalize_file_source` produced for each source.

    Historical behaviour: normalise the path, then re-glue verbatim
    everything after the first `|`. A display filter therefore forked
    the identity of the layer.
    """
    base = _legacy_path(gpkg)
    return {
        "bare": f"ogr::{base}|layername=troncons",
        "filtered": f"ogr::{base}|layername=troncons|subset=\"cat\" = 'A'",
        "geomtype": f"ogr::{base}|layername=troncons|geometrytype=LineString",
        "communes": f"ogr::{base}|layername=communes",
    }


def _attrs(operation: str, idx: int) -> str:
    if operation == "UPDATE":
        return json.dumps({"changed_only": {"nom": {"old": f"n{idx}", "new": f"n{idx + 1}"}}})
    return json.dumps({"all_attributes": {"nom": f"n{idx}", "cat": "A"}})


def _seed_journal(db_path: str, fps: dict, sources: dict) -> None:
    """Write a schema-v5 journal: legacy fingerprints, registry, version."""
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    conn = sqlite3.connect(db_path)
    try:
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = "".join((
            "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
            ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
        ))
        for idx, (offset, key, layer_name, operation, entity) in enumerate(_STORY):
            conn.execute(insert, (
                _PROJECT_FP, fps[key], f"lyr_{key}", layer_name, "ogr",
                json.dumps({"fid": int(entity.split(":")[1])}),
                operation, _attrs(operation, idx),
                b"\x01wkb_old" if operation != "INSERT" else None,
                "LineString", "EPSG:2154", None, "tester", "sess_mig_ar",
                (_T0 + timedelta(seconds=offset)).isoformat(), None,
                entity, 2, None, None,
            ))
        now = _T0.isoformat()
        for key, fingerprint in fps.items():
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (fingerprint, "ogr", sources[key], key, "", "EPSG:2154",
                 "LineString", now),
            )
        # v5: everything this journal knows. v6 is what the open must add.
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (5, now, "fixture v5"),
        )
        conn.commit()
    finally:
        conn.close()


def _dump_events(db_path: str) -> list:
    """Every column of every audit_event row, comparable across opens."""
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS

    conn = sqlite3.connect(db_path)
    try:
        cols = ", ".join(AUDIT_EVENT_COLUMNS)
        rows = conn.execute(
            "SELECT " + cols + " FROM audit_event ORDER BY event_id"  # nosec B608
        ).fetchall()
    finally:
        conn.close()
    return [tuple(bytes(v) if isinstance(v, (bytes, memoryview)) else v
                  for v in row) for row in rows]


def _digest(dump: list) -> str:
    return hashlib.sha256(repr(dump).encode("utf-8", "replace")).hexdigest()[:16]


def _fingerprint_census(db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT datasource_fingerprint, COUNT(*) FROM audit_event "
            "GROUP BY datasource_fingerprint"
        ).fetchall()
    finally:
        conn.close()
    return {r[0]: r[1] for r in rows}


# ---------------------------------------------------------------------------
# Reads that must find the legacy events again
# ---------------------------------------------------------------------------

def _read_paths(conn, canonical: str) -> dict:
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, count_events_after_cutoff,
        get_oldest_event_date, fetch_entity_stream,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.search_service import (
        search_events, get_distinct_layers, summarize_scope,
    )
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.datasource_registry import lookup_datasource

    cutoff = RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True)
    events = fetch_events_after_cutoff(conn, canonical, cutoff, trace_id="mig_ar")
    criteria = SearchCriteria(
        datasource_fingerprint=canonical, layer_name=None, operation_type=None,
        user_name=None, start_date=None, end_date=None, page=1, page_size=100,
    )
    search = search_events(conn, criteria, trace_id="mig_ar")
    info = lookup_datasource(conn, canonical)
    return {
        "rewind_events": [(e.event_id, e.datasource_fingerprint, e.entity_fingerprint,
                           e.operation_type) for e in events],
        "rewind_count": count_events_after_cutoff(conn, canonical, cutoff,
                                                  trace_id="mig_ar"),
        "oldest": get_oldest_event_date(conn, canonical),
        "entity_stream_fid1": [e.event_id for e in
                               fetch_entity_stream(conn, canonical, "fid:1")],
        "search_total": search.total_count,
        "search_ids": sorted(e.event_id for e in search.events),
        "scope_summary": summarize_scope(conn, criteria).total_count,
        "distinct_layers": get_distinct_layers(conn),
        "registry_hit": None if info is None else info.source_uri,
        "raw_events": events,
    }


def _dedup_probe(events) -> dict:
    """What the rewind planner would actually do with those events.

    `rewind_dedup._entity_key` prefixes the entity key with the RAW
    `datasource_fingerprint` of each event. The alias expansion happens
    in SQL, upstream: the rows come back, but they come back carrying
    three different fingerprints. This probe collapses the very same
    events twice -- as fetched, then with the fingerprint field forced to
    the canonical value -- so any difference is imputable to that field
    and to nothing else.
    """
    from recoverland.core.rewind_dedup import collapse_rewind_events

    as_fetched = collapse_rewind_events(list(events))
    canonical = events[0].datasource_fingerprint if events else ""
    unified = collapse_rewind_events(
        [e._replace(datasource_fingerprint=canonical) for e in events])

    def _describe(collapsed):
        return sorted((e.entity_fingerprint, e.operation_type) for e in collapsed)

    return {
        "as_fetched": _describe(as_fetched),
        "unified": _describe(unified),
        "n_as_fetched": len(as_fetched),
        "n_unified": len(unified),
    }


def _review_probe(db_path: str, canonical: str) -> dict:
    """The REVIEW / Time Lens rebuild query, run exactly as the worker does."""
    try:
        from recoverland.widgets.snapshot_rebuild_worker import (
            _SQL_ALL_EVENTS_BEFORE, _SQL_DATE_RANGE,
        )
    except Exception as exc:  # noqa: BLE001 - Qt unavailable: report, do not fail
        return {"import_error": f"{type(exc).__name__}: {exc}"}
    horizon = (_T0 + timedelta(days=1)).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(_SQL_ALL_EVENTS_BEFORE, (canonical, horizon)).fetchall()
        span = conn.execute(_SQL_DATE_RANGE, (canonical,)).fetchone()
    finally:
        conn.close()
    return {"import_error": "", "n_rows": len(rows), "date_span": tuple(span or ())}


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_mig_ar_")
    gpkg = str(Path(tmpdir) / "donnees" / "reseau.gpkg")
    os.makedirs(os.path.dirname(gpkg), exist_ok=True)

    fps = _legacy_fingerprints(gpkg)
    sources = _sources(gpkg)
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _seed_journal(db_path, fps, sources)

    ctx.data.update({
        "tmpdir": tmpdir,
        "db_path": db_path,
        "gpkg": gpkg,
        "fps": fps,
        "sources": sources,
        "before_dump": _dump_events(db_path),
        "before_census": _fingerprint_census(db_path),
    })
    flog(
        f"mig_alias_reconciliation setup: trace_id={ctx.trace_id} db={db_path} "
        f"n_events={len(ctx.data['before_dump'])} "
        f"fingerprints={sorted(ctx.data['before_census'])}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema, get_schema_version
    from recoverland.core.identity import compute_fingerprint_for_source
    from recoverland.core.datasource_alias import (
        list_aliases, load_reconciliation, remove_alias,
    )

    flog(f"mig_alias_reconciliation run start: trace_id={ctx.trace_id}", "INFO")

    db_path = ctx.data["db_path"]
    sources = ctx.data["sources"]

    # The canonical form is computed by the code under test, never guessed.
    ctx.data["canonical"] = {
        key: compute_fingerprint_for_source("ogr", uri)
        for key, uri in sources.items()
    }

    conn = sqlite3.connect(db_path)
    error = ""
    try:
        initialize_schema(conn)
        ctx.data["version_after"] = get_schema_version(conn)
        ctx.data["aliases"] = list_aliases(conn)
        report = load_reconciliation(conn)
        ctx.data["report"] = None if report is None else {
            "status": report.status,
            "scanned": report.scanned,
            "aliases_created": report.aliases_created,
            "already_canonical": report.already_canonical,
            "already_aliased": report.already_aliased,
            "ambiguous": [a.fingerprint for a in report.ambiguous],
            "without_source_uri": list(report.without_source_uri),
            "is_degraded": report.is_degraded,
        }
        canonical = ctx.data["canonical"]["bare"]
        ctx.data["reads"] = _read_paths(conn, canonical)
        ctx.data["dedup"] = _dedup_probe(ctx.data["reads"]["raw_events"])
        # Reversibility: deleting the alias rows must restore the exact
        # pre-migration behaviour, which a column rewrite could never do.
        for alias_fp, _target, _created, _note in ctx.data["aliases"]:
            remove_alias(conn, alias_fp)
        ctx.data["reads_without_alias"] = _read_paths(conn, canonical)
    except Exception as exc:  # noqa: BLE001 - a failed open is the verdict
        error = f"{type(exc).__name__}: {exc}"
    finally:
        conn.commit()
        conn.close()

    ctx.data["error"] = error
    ctx.data["after_dump"] = _dump_events(db_path)
    ctx.data["after_census"] = _fingerprint_census(db_path)
    ctx.data["review"] = _review_probe(db_path, ctx.data["canonical"]["bare"])

    src = (_PLUGIN_ROOT / "core" / "workflow_service.py").read_text(
        encoding="utf-8", errors="replace")
    ctx.data["workflow_strict_eq"] = bool(re.search(
        r"compute_datasource_fingerprint\(layer\)\s*==\s*event\.datasource_fingerprint",
        src))

    reads = ctx.data.get("reads") or {}
    flog(
        f"mig_alias_reconciliation run end: trace_id={ctx.trace_id} "
        f"error={error!r} aliases={len(ctx.data.get('aliases') or [])} "
        f"rewind_events={len(reads.get('rewind_events') or [])} "
        f"dedup={ctx.data.get('dedup')} review={ctx.data.get('review')}",
        "INFO",
    )
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION

    out = []
    fps = ctx.data.get("fps") or {}
    canonical = (ctx.data.get("canonical") or {}).get("bare", "")
    aliases = ctx.data.get("aliases") or []
    alias_map = {a[0]: a[1] for a in aliases}
    reads = ctx.data.get("reads") or {}
    report = ctx.data.get("report") or {}
    before = ctx.data.get("before_dump") or []
    after = ctx.data.get("after_dump") or []

    # ===== L'ouverture ne doit jamais echouer ============================
    out.append((
        "ouverture_sans_erreur",
        not ctx.data.get("error"),
        f"erreur a l'ouverture={ctx.data.get('error')!r}, attendu aucune : "
        f"une migration qui leve prive l'utilisateur de tout son historique "
        f"au moment meme ou il met le plugin a jour",
    ))
    out.append((
        "schema_porte_a_la_version_courante",
        ctx.data.get("version_after") == CURRENT_SCHEMA_VERSION,
        f"schema_version={ctx.data.get('version_after')} "
        f"attendu={CURRENT_SCHEMA_VERSION}",
    ))

    # ===== 1. RIEN N'A ETE REECRIT ======================================
    out.append((
        "aucune_ligne_perdue_ni_ajoutee",
        len(before) == len(after) == len(_STORY),
        f"lignes avant={len(before)} apres={len(after)} attendu={len(_STORY)}",
    ))
    out.append((
        "audit_event_inchange_colonne_par_colonne",
        before == after,
        f"dump complet des 21 colonnes : sha avant={_digest(before)} "
        f"apres={_digest(after)}. La migration ne doit toucher AUCUNE ligne "
        f"de audit_event ; c'est ce qui rend la reconciliation reversible.",
    ))
    out.append((
        "aucune_empreinte_reecrite",
        ctx.data.get("before_census") == ctx.data.get("after_census"),
        f"repartition des empreintes avant={ctx.data.get('before_census')} "
        f"apres={ctx.data.get('after_census')} : chaque evenement doit "
        f"conserver l'empreinte sous laquelle il a ete capture",
    ))

    # ===== 2. LES ALIAS ATTENDUS EXISTENT ===============================
    out.append((
        "alias_pose_pour_la_couche_filtree",
        alias_map.get(fps.get("filtered")) == canonical,
        f"alias de l'empreinte filtree -> {alias_map.get(fps.get('filtered'))!r} "
        f"attendu {canonical!r}. Sans lui, tout ce que l'utilisateur a edite "
        f"filtre reste dans un historique parallele.",
    ))
    out.append((
        "alias_pose_pour_l_ouverture_geometrytype",
        alias_map.get(fps.get("geomtype")) == canonical,
        f"alias={alias_map.get(fps.get('geomtype'))!r} attendu {canonical!r}",
    ))
    out.append((
        "les_deux_formes_convergent_sur_une_empreinte",
        len({alias_map.get(fps.get("filtered")), alias_map.get(fps.get("geomtype"))}) == 1,
        f"cibles={sorted(set(alias_map.values()))} : plusieurs anciennes "
        f"empreintes d'un meme fichier doivent converger, jamais se scinder",
    ))
    out.append((
        "aucun_alias_parasite",
        len(aliases) == 2 and fps.get("communes") not in alias_map
        and fps.get("bare") not in alias_map,
        f"alias ecrits={[(a[0][-40:], a[1][-40:]) for a in aliases]} : une "
        f"source deja canonique (communes, troncons non filtre) ne doit "
        f"recevoir aucun alias",
    ))
    out.append((
        "empreinte_canonique_identique_a_l_historique_sain",
        canonical == fps.get("bare"),
        f"canonique={canonical!r} historique={fps.get('bare')!r} : pour une "
        f"source sans jeton d'affichage, la canonicalisation doit rendre la "
        f"MEME chaine, sinon tous les journaux sains auraient besoin d'alias",
    ))
    out.append((
        "compte_rendu_de_migration_coherent",
        report.get("status") == "ok" and report.get("aliases_created") == 2
        and report.get("scanned") == 4 and report.get("already_canonical") == 2
        and not report.get("ambiguous") and not report.get("without_source_uri"),
        f"compte-rendu={report} attendu status=ok scanned=4 aliases_created=2 "
        f"already_canonical=2 ambiguous=[] without_source_uri=[]",
    ))
    out.append((
        "journal_non_signale_comme_degrade",
        report.get("is_degraded") is False,
        f"is_degraded={report.get('is_degraded')} : ce journal a ete "
        f"integralement rattache, l'interface n'a rien a signaler",
    ))

    # ===== 3. LES LECTURES RETROUVENT L'HISTORIQUE ANCIEN ================
    n_troncons = sum(1 for row in _STORY if row[1] != "communes")
    rewind = reads.get("rewind_events") or []
    out.append((
        "rewind_retrouve_tout_l_historique",
        len(rewind) == n_troncons,
        f"evenements rendus par fetch_events_after_cutoff sur l'empreinte "
        f"canonique={len(rewind)} attendu={n_troncons}. C'est la preuve "
        f"centrale : sans alias, le rewind n'aurait vu que les "
        f"{sum(1 for r in _STORY if r[1] == 'bare')} evenements non filtres.",
    ))
    out.append((
        "rewind_ne_deborde_pas_sur_l_autre_couche",
        all(fp != (ctx.data.get('fps') or {}).get("communes")
            for _eid, fp, _ent, _op in rewind),
        f"empreintes rendues={sorted({fp[-30:] for _e, fp, _n, _o in rewind})} : "
        f"la couche communes ne fait pas partie de cette source",
    ))
    out.append((
        "compte_et_lecture_concordent",
        reads.get("rewind_count") == len(rewind),
        f"count_events_after_cutoff={reads.get('rewind_count')} "
        f"fetch={len(rewind)} : un compteur qui annonce moins que ce qui "
        f"sera rejoue fait mentir l'ecran de confirmation",
    ))
    out.append((
        "horizon_couvre_l_evenement_le_plus_ancien",
        reads.get("oldest") == _T0.isoformat(),
        f"get_oldest_event_date={reads.get('oldest')!r} attendu="
        f"{_T0.isoformat()!r} : le curseur de date doit descendre jusqu'a "
        f"l'evenement le plus ancien de la source, alias compris",
    ))
    out.append((
        "recherche_retrouve_les_evenements_anciens",
        reads.get("search_total") == n_troncons,
        f"search_events total={reads.get('search_total')} attendu={n_troncons}",
    ))
    out.append((
        "resume_de_portee_coherent",
        reads.get("scope_summary") == n_troncons,
        f"summarize_scope total={reads.get('scope_summary')} attendu={n_troncons}",
    ))
    out.append((
        "flux_d_entite_recolle_les_trois_ouvertures",
        len(reads.get("entity_stream_fid1") or []) == 2,
        f"fetch_entity_stream(fid:1)={reads.get('entity_stream_fid1')} "
        f"attendu 2 evenements (edition non filtree + edition filtree)",
    ))
    out.append((
        "registre_resolu_par_alias",
        bool(reads.get("registry_hit")),
        f"lookup_datasource(empreinte canonique)={reads.get('registry_hit')!r} : "
        f"le registre n'a aucune ligne pour la forme canonique, il doit la "
        f"trouver par alias, sinon un restore sur couche non chargee echoue",
    ))
    layers = reads.get("distinct_layers") or []
    out.append((
        "la_couche_n_apparait_qu_une_fois_dans_l_interface",
        len(layers) == 2,
        f"get_distinct_layers={[row.get('fingerprint', '')[-30:] for row in layers]} "
        f"attendu 2 entrees (troncons + communes) : les formes obsoletes "
        f"doivent etre masquees quand leur cible est listee, sinon "
        f"l'utilisateur choisit une entree qui cadre le rewind sur la moitie "
        f"de l'historique",
    ))

    # ===== 4. REVERSIBILITE =============================================
    plain = ctx.data.get("reads_without_alias") or {}
    n_bare = sum(1 for row in _STORY if row[1] == "bare")
    out.append((
        "suppression_des_alias_rend_l_etat_d_avant",
        len(plain.get("rewind_events") or []) == n_bare,
        f"apres remove_alias : evenements vus={len(plain.get('rewind_events') or [])} "
        f"attendu={n_bare} (l'etat pre-migration exact). Une reecriture de "
        f"colonne n'aurait pas pu etre annulee.",
    ))

    # ===== 5. CE QUE LA RECONCILIATION NE COUVRE PAS =====================
    dedup = ctx.data.get("dedup") or {}
    out.append((
        "dedup_rewind_regroupe_par_entite_et_non_par_empreinte",
        dedup.get("n_as_fetched") == dedup.get("n_unified"),
        f"collapse_rewind_events sur les evenements tels que lus="
        f"{dedup.get('n_as_fetched')} actions {dedup.get('as_fetched')}, sur "
        f"les MEMES evenements ramenes a une seule empreinte="
        f"{dedup.get('n_unified')} actions {dedup.get('unified')}. "
        f"Seule la valeur du champ datasource_fingerprint differe entre les "
        f"deux collapses : `rewind_dedup._entity_key` prefixe la cle "
        f"d'entite par `event.datasource_fingerprint` BRUT, et l'expansion "
        f"faite en SQL ramene les lignes anciennes SANS les ramener a une "
        f"identite commune. Consequence mesuree ici sur fid:3, creee pendant "
        f"que le filtre etait pose puis supprimee apres son retrait : "
        f"l'aller-retour interne INSERT+DELETE n'est plus annule "
        f"(`_cancel_internal_lifetimes`) et le rewind planifie DEUX "
        f"compensations sur une entite qui n'en demande aucune. Avant le "
        f"branchement des alias ces lignes n'etaient meme pas lues : le "
        f"defaut etait une perte silencieuse, il devient une action de trop.",
    ))
    review = ctx.data.get("review") or {}
    out.append((
        "review_time_lens_voit_l_historique_ancien",
        review.get("import_error") == "" and review.get("n_rows") == n_troncons,
        f"la requete de reconstruction de REVIEW "
        f"(widgets/snapshot_rebuild_worker._SQL_ALL_EVENTS_BEFORE) rend "
        f"{review.get('n_rows')} lignes pour l'empreinte canonique, attendu "
        f"{n_troncons} (import_error={review.get('import_error')!r}). Ce "
        f"worker porte son propre `ae.datasource_fingerprint = ?` et ne passe "
        f"pas par datasource_alias.expand_fingerprints : l'etat passe affiche "
        f"a l'utilisateur ignore tout ce qui a ete capture sous une empreinte "
        f"ancienne.",
    ))
    out.append((
        "workflow_service_compare_par_equivalence",
        ctx.data.get("workflow_strict_eq") is False,
        "core/workflow_service.find_target_layer compare "
        "`compute_datasource_fingerprint(layer) == event.datasource_fingerprint` : "
        "un evenement portant l'ancienne empreinte ne reconnait plus la couche "
        "chargee par l'utilisateur et le restore repart sur une couche "
        "temporaire reconstruite depuis le registre. "
        "identity.datasource_fingerprints_match / expand_fingerprints existent "
        "pour cette comparaison.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"datasource_alias\.reconcile: status=ok",
        name="reconciliation_journalisee",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"mig_alias_reconciliation.*trace_id={ctx.trace_id}",
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
