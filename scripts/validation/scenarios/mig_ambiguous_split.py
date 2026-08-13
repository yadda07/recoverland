"""mig_ambiguous_split - RecoverLand validation runtime.

Invariant: I-3 (une empreinte designe UNE table) et la regle d'or de la
migration : devant une ambiguite, ne pas deviner.

CE QUE CE SCENARIO PROUVE
-------------------------
Le defaut des providers DB FUSIONNE : `table="public"."routes"` etait lu
comme la table `public`, si bien que TOUTES les tables d'un schema
partageaient une empreinte. Et `datasource_registry` fait un
`ON CONFLICT(datasource_fingerprint) DO UPDATE` : pour N tables fondues
en une empreinte, UNE SEULE `source_uri` a survecu.

La canonicalisation, appliquee a cette unique URI, ne peut donc rendre
que l'une des N tables. Ecrire l'alias reviendrait a decreter que tout
l'historique du schema appartient a cette table-la : un rewind rejouerait
l'historique de `rivieres` sur `routes`. C'est le seul endroit de la
migration ou la bonne reponse est de ne rien faire.

Le journal fixture, ouvert par un vrai `JournalManager` sur un vrai
projet, contient trois sources DB :

  * `gis.public` -- DEUX tables auditees (routes, rivieres) sous une
    seule empreinte : cas ambigu, aucun alias attendu ;
  * `cadastre.public` -- UNE table (parcelles) : temoin, alias attendu,
    pour prouver que le refus est cible et non un renoncement global ;
  * `urba.public` -- UNE table dont l'utilisateur a RENOMME la couche
    dans QGIS (`zonage` puis `PLU zonage`). La sonde d'ambiguite
    interroge `layer_name_snapshot` en premier : ce renommage suffit a
    classer une source non ambigue comme ambigue. Le scenario mesure ce
    que ce refus coute a l'utilisateur.

Verifications finales : les evenements des sources refusees sont
toujours la et toujours lisibles sous leur empreinte d'origine (rien
n'est perdu, seul le rattachement automatique manque), l'empreinte
canonique d'une table ambigue ne ramene AUCUN evenement (aucune fausse
attribution), et le constat est consultable par l'interface.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_ambiguous_split"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "mig_as_project"
_T0 = datetime(2026, 5, 11, 14, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()

# The fingerprint the PRE-v6 normaliser produced: the regex
# table="([^"]*)" stopped at the first closing quote and captured the
# SCHEMA, and `schema` is not a token of a QGIS URI so it fell back to
# its default -- 'public' as well.
_FP_MERGED = "postgres::host=localhost port=5432 dbname=gis schema=public table=public"
_FP_SINGLE = "postgres::host=localhost port=5432 dbname=cadastre schema=public table=public"
_FP_RENAMED = "postgres::host=localhost port=5432 dbname=urba schema=public table=public"

# The URIs QGIS writes. For the merged fingerprint only ONE of them
# survived in the registry (ON CONFLICT DO UPDATE keeps the last).
_URI_ROUTES = ("dbname='gis' host=localhost port=5432 user='u' "
               'table="public"."routes" (geom) sql=')
_URI_RIVIERES = ("dbname='gis' host=localhost port=5432 user='u' "
                 'table="public"."rivieres" (geom) sql=')
_URI_PARCELLES = ("dbname='cadastre' host=localhost port=5432 user='u' "
                  'table="public"."parcelles" (geom) sql=')
_URI_ZONAGE = ("dbname='urba' host=localhost port=5432 user='u' "
               'table="public"."zonage" (geom) sql=')

# (offset_s, fingerprint, layer_id, layer_name, operation, entity)
_STORY = [
    (0, _FP_MERGED, "lyr_routes", "routes", "UPDATE", "pk:gid=5"),
    (60, _FP_MERGED, "lyr_routes", "routes", "DELETE", "pk:gid=7"),
    (120, _FP_MERGED, "lyr_rivieres", "rivieres", "INSERT", "pk:gid=5"),
    (180, _FP_MERGED, "lyr_rivieres", "rivieres", "UPDATE", "pk:gid=9"),
    (240, _FP_SINGLE, "lyr_parcelles", "parcelles", "UPDATE", "pk:gid=1"),
    (300, _FP_SINGLE, "lyr_parcelles", "parcelles", "UPDATE", "pk:gid=2"),
    # Same table, same QGIS layer id: the user simply renamed the layer.
    (360, _FP_RENAMED, "lyr_zonage", "zonage", "UPDATE", "pk:gid=3"),
    (420, _FP_RENAMED, "lyr_zonage", "PLU zonage", "UPDATE", "pk:gid=4"),
]

_REGISTRY = [
    (_FP_MERGED, "postgres", _URI_ROUTES, "routes"),
    (_FP_SINGLE, "postgres", _URI_PARCELLES, "parcelles"),
    (_FP_RENAMED, "postgres", _URI_ZONAGE, "PLU zonage"),
]


def _seed(db_path: str) -> None:
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
        for idx, (offset, fingerprint, layer_id, layer_name, operation,
                  entity) in enumerate(_STORY):
            attrs = (json.dumps({"changed_only": {"n": {"old": idx, "new": idx + 1}}})
                     if operation == "UPDATE"
                     else json.dumps({"all_attributes": {"n": idx}}))
            gid = entity.split("=")[1]
            conn.execute(insert, (
                _PROJECT_FP, fingerprint, layer_id, layer_name, "postgres",
                json.dumps({"fid": int(gid), "pk_field": "gid",
                            "pk_value": int(gid)}),
                operation, attrs, b"\x01wkb", "Point", "EPSG:2154", None,
                "tester", "sess_mig_as",
                (_T0 + timedelta(seconds=offset)).isoformat(), None,
                entity, 2, None, None,
            ))
        now = _T0.isoformat()
        for fingerprint, provider, uri, layer_name in _REGISTRY:
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (fingerprint, provider, uri, layer_name, "", "EPSG:2154",
                 "Point", now),
            )
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (5, now, "fixture v5"),
        )
        conn.commit()
    finally:
        conn.close()


def _dump_events(db_path: str) -> list:
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT " + ", ".join(AUDIT_EVENT_COLUMNS) +  # nosec B608
            " FROM audit_event ORDER BY event_id"
        ).fetchall()
    finally:
        conn.close()
    return [tuple(bytes(v) if isinstance(v, (bytes, memoryview)) else v
                  for v in row) for row in rows]


def _digest(dump: list) -> str:
    return hashlib.sha256(repr(dump).encode("utf-8", "replace")).hexdigest()[:16]


def _n_events(conn, fingerprint: str) -> int:
    """Events a rewind scoped on `fingerprint` would replay (alias-aware)."""
    from recoverland.core.event_stream_repository import count_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType

    return count_events_after_cutoff(
        conn, fingerprint, RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True),
        trace_id="mig_as")


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_mig_as_")
    project_path = str(Path(tmpdir) / "projet.qgz")
    Path(project_path).write_text("<qgis/>", encoding="utf-8")
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _seed(db_path)

    ctx.data.update({
        "tmpdir": tmpdir,
        "project_path": project_path,
        "db_path": db_path,
        "before_dump": _dump_events(db_path),
    })
    flog(
        f"mig_ambiguous_split setup: trace_id={ctx.trace_id} db={db_path} "
        f"n_events={len(ctx.data['before_dump'])} "
        f"empreinte_fusionnee={_FP_MERGED!r}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.identity import compute_fingerprint_for_source
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.datasource_alias import (
        list_aliases, load_reconciliation, get_ambiguous_datasources,
        describe_reconciliation,
    )

    flog(f"mig_ambiguous_split run start: trace_id={ctx.trace_id}", "INFO")

    ctx.data["canon"] = {
        "routes": compute_fingerprint_for_source("postgres", _URI_ROUTES),
        "rivieres": compute_fingerprint_for_source("postgres", _URI_RIVIERES),
        "parcelles": compute_fingerprint_for_source("postgres", _URI_PARCELLES),
        "zonage": compute_fingerprint_for_source("postgres", _URI_ZONAGE),
    }

    jm = JournalManager()
    error = ""
    try:
        jm.open_for_project(ctx.data["project_path"], ctx.data["tmpdir"])
        conn = jm.get_connection()
        ctx.data["aliases"] = list_aliases(conn)
        report = load_reconciliation(conn)
        ctx.data["report"] = None if report is None else {
            "status": report.status,
            "scanned": report.scanned,
            "aliases_created": report.aliases_created,
            "already_canonical": report.already_canonical,
            "ambiguous": [(a.fingerprint, a.provider_type, sorted(a.layer_names),
                           a.reason) for a in report.ambiguous],
            "without_source_uri": list(report.without_source_uri),
            "is_degraded": report.is_degraded,
        }
        ctx.data["ambiguous_api"] = [
            (a.fingerprint, sorted(a.layer_names), a.reason)
            for a in get_ambiguous_datasources(conn)
        ]
        ctx.data["description"] = describe_reconciliation(conn)
        ctx.data["jm_degraded"] = jm.is_reconciliation_degraded
        ctx.data["jm_warning"] = jm.reconciliation_warning()
        ctx.data["counts"] = {
            "old_merged": _n_events(conn, _FP_MERGED),
            "canon_routes": _n_events(conn, ctx.data["canon"]["routes"]),
            "canon_rivieres": _n_events(conn, ctx.data["canon"]["rivieres"]),
            "old_single": _n_events(conn, _FP_SINGLE),
            "canon_parcelles": _n_events(conn, ctx.data["canon"]["parcelles"]),
            "old_renamed": _n_events(conn, _FP_RENAMED),
            "canon_zonage": _n_events(conn, ctx.data["canon"]["zonage"]),
        }
    except Exception as exc:  # noqa: BLE001 - a failed open is the verdict
        error = f"{type(exc).__name__}: {exc}"
    finally:
        jm.close()

    ctx.data["error"] = error
    ctx.data["after_dump"] = _dump_events(ctx.data["db_path"])

    flog(
        f"mig_ambiguous_split run end: trace_id={ctx.trace_id} error={error!r} "
        f"aliases={[(a[0][-30:], a[1][-30:]) for a in ctx.data.get('aliases') or []]} "
        f"ambigus={ctx.data.get('ambiguous_api')} "
        f"comptes={ctx.data.get('counts')}",
        "INFO",
    )
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    aliases = ctx.data.get("aliases") or []
    alias_map = {a[0]: a[1] for a in aliases}
    report = ctx.data.get("report") or {}
    canon = ctx.data.get("canon") or {}
    counts = ctx.data.get("counts") or {}
    ambiguous = report.get("ambiguous") or []
    ambiguous_fps = {row[0] for row in ambiguous}

    out.append((
        "ouverture_sans_erreur",
        not ctx.data.get("error"),
        f"erreur={ctx.data.get('error')!r} : une source ambigue ne doit pas "
        f"empecher le projet de s'ouvrir",
    ))

    # ===== 1. Le defaut est bien corrige pour l'avenir ====================
    out.append((
        "canonicalisation_separe_enfin_les_tables",
        canon.get("routes") != canon.get("rivieres")
        and "table=routes" in (canon.get("routes") or "")
        and "table=rivieres" in (canon.get("rivieres") or ""),
        f"routes={canon.get('routes')!r} rivieres={canon.get('rivieres')!r} : "
        f"QgsDataSourceUri doit rendre schema=public / table=<table>, la ou "
        f"la regex historique capturait deux fois le schema",
    ))

    # ===== 2. LA MIGRATION NE DEVINE PAS ==================================
    out.append((
        "aucun_alias_pour_l_empreinte_fusionnee",
        _FP_MERGED not in alias_map,
        f"alias ecrit pour l'empreinte fusionnee={alias_map.get(_FP_MERGED)!r} "
        f"attendu aucun : le registre n'a garde que l'URI de routes ; poser "
        f"cet alias attribuerait a routes l'historique de rivieres et un "
        f"rewind rejouerait les entites d'une table sur l'autre",
    ))
    out.append((
        "ambiguite_enregistree_et_nommee",
        _FP_MERGED in ambiguous_fps,
        f"empreintes declarees ambigues={[row[0][-45:] for row in ambiguous]} : "
        f"le refus doit laisser une trace consultable, sinon l'utilisateur ne "
        f"sait pas que cette source a un historique non rattache",
    ))
    merged_entry = next((row for row in ambiguous if row[0] == _FP_MERGED), None)
    out.append((
        "les_deux_couches_sont_nommees",
        bool(merged_entry) and merged_entry[2] == ["rivieres", "routes"],
        f"couches listees pour l'empreinte fusionnee="
        f"{merged_entry[2] if merged_entry else None} attendu "
        f"['rivieres', 'routes'] : l'utilisateur doit pouvoir identifier les "
        f"sources indistinguables",
    ))
    out.append((
        "aucune_fausse_attribution",
        counts.get("canon_routes") == 0 and counts.get("canon_rivieres") == 0,
        f"evenements ramenes par l'empreinte canonique de routes="
        f"{counts.get('canon_routes')} et de rivieres="
        f"{counts.get('canon_rivieres')} attendu 0 : tant que l'ambiguite "
        f"n'est pas levee, aucune de ces deux tables ne doit recevoir "
        f"l'historique commun",
    ))
    out.append((
        "les_evenements_ambigus_restent_lisibles",
        counts.get("old_merged") == 4,
        f"evenements toujours lisibles sous l'empreinte d'origine="
        f"{counts.get('old_merged')} attendu 4 : ne pas deviner ne veut pas "
        f"dire perdre ; les lignes restent la ou elles sont",
    ))

    # ===== 3. Le refus est cible, pas un renoncement global ==============
    out.append((
        "temoin_une_seule_table_est_bien_aliasee",
        alias_map.get(_FP_SINGLE) == canon.get("parcelles"),
        f"alias du temoin={alias_map.get(_FP_SINGLE)!r} attendu "
        f"{canon.get('parcelles')!r} : une empreinte DB non ambigue doit etre "
        f"rattachee, sinon la migration ne sert a rien sur les providers DB",
    ))
    out.append((
        "temoin_rewind_retrouve_son_historique",
        counts.get("canon_parcelles") == 2,
        f"evenements vus depuis l'empreinte canonique de parcelles="
        f"{counts.get('canon_parcelles')} attendu 2",
    ))

    # ===== 4. AUCUNE LIGNE N'A BOUGE =====================================
    out.append((
        "audit_event_inchange",
        ctx.data.get("before_dump") == ctx.data.get("after_dump"),
        f"dump complet avant={_digest(ctx.data.get('before_dump') or [])} "
        f"apres={_digest(ctx.data.get('after_dump') or [])} : meme devant une "
        f"ambiguite, la migration ne touche pas une ligne",
    ))
    out.append((
        "aucune_ligne_perdue",
        len(ctx.data.get("after_dump") or []) == len(_STORY),
        f"lignes={len(ctx.data.get('after_dump') or [])} attendu={len(_STORY)}",
    ))

    # ===== 5. Le constat est consultable par l'interface ==================
    out.append((
        "journal_signale_comme_degrade",
        report.get("is_degraded") is True and ctx.data.get("jm_degraded") is True,
        f"report.is_degraded={report.get('is_degraded')} "
        f"JournalManager.is_reconciliation_degraded={ctx.data.get('jm_degraded')} : "
        f"l'interface doit pouvoir prevenir AVANT de proposer un rewind",
    ))
    out.append((
        "api_d_ambiguite_exposee",
        len(ctx.data.get("ambiguous_api") or []) == len(ambiguous),
        f"get_ambiguous_datasources={ctx.data.get('ambiguous_api')}",
    ))
    warning = ctx.data.get("jm_warning") or ""
    out.append((
        "message_utilisateur_en_francais_et_non_vide",
        bool(warning) and "source" in warning.lower(),
        f"JournalManager.reconciliation_warning()={warning!r} : le constat "
        f"doit etre disable a l'utilisateur, pas seulement lisible en base",
    ))
    out.append((
        "message_identique_a_describe_reconciliation",
        warning == (ctx.data.get("description") or ""),
        f"describe_reconciliation={ctx.data.get('description')!r}",
    ))

    # ===== 6. Le cout du refus : la couche simplement renommee ===========
    renamed_entry = next((row for row in ambiguous if row[0] == _FP_RENAMED), None)
    out.append((
        "un_simple_renommage_ne_doit_pas_valoir_ambiguite",
        _FP_RENAMED not in ambiguous_fps,
        f"la source urba.public (UNE table, UN layer_id 'lyr_zonage', mais "
        f"deux `layer_name_snapshot` parce que l'utilisateur a renomme sa "
        f"couche) est classee ambigue={_FP_RENAMED in ambiguous_fps} "
        f"raison={renamed_entry[3] if renamed_entry else None!r}. "
        f"`_ambiguity_verdict` interroge `layer_name_snapshot` en PREMIER et "
        f"ne consulte `layer_id_snapshot` -- le signal precis, ici unique -- "
        f"que si le nom parait deja unique. Consequence mesuree : un rewind "
        f"sur l'empreinte canonique de zonage ne voit que "
        f"{counts.get('canon_zonage')} de ses 2 evenements anciens. Renommer "
        f"une couche est l'action la plus courante de QGIS ; inverser l'ordre "
        f"des deux sondes rendrait ce cas non ambigu sans rien affaiblir.",
    ))
    out.append((
        "renommage_aucune_ligne_perdue_malgre_le_refus",
        counts.get("old_renamed") == 2,
        f"evenements toujours lisibles sous l'empreinte d'origine="
        f"{counts.get('old_renamed')} attendu 2 : le refus coute le "
        f"rattachement, jamais les donnees",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"JournalManager: datasource reconciliation degraded",
        name="degradation_journalisee_a_l_ouverture",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"mig_ambiguous_split.*trace_id={ctx.trace_id}",
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
