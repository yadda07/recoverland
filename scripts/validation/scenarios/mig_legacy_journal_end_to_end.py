"""mig_legacy_journal_end_to_end - RecoverLand validation runtime.

Invariant: I-JRN + I-11. Un journal ancien qu'on rouvre doit arriver a la
version courante SANS PERDRE UNE LIGNE et rester integralement rejouable.

CE QUE CE SCENARIO PROUVE
-------------------------
Il y a un journal par projet. Un utilisateur qui a dix projets a dix
journaux, a des versions differentes, dont certains n'ont pas ete
ouverts depuis des mois. Le jour ou il rouvre le vieux, tout doit
remonter : le schema, les colonnes, les index, le retro-remplissage de
`entity_fingerprint`, et le rattachement des empreintes anciennes.

Deux journaux, tous deux ecrits avec le DDL D'ORIGINE puis marques a leur
version reelle, pas simulee :

A. JOURNAL v2 AVEC REGISTRE
   `audit_event` sans `invalidated_at`, pas de table `datasource_alias`
   (elle date de v4), mais un `datasource_registry` rempli. C'est le cas
   nominal : la migration doit ajouter la colonne, creer la table,
   ecrire l'alias de la forme filtree, et le rewind doit ensuite pouvoir
   PLANIFIER sur tout l'historique (fetch -> collapse -> plan ->
   preflight), pas seulement le lire.

B. JOURNAL v1 SANS REGISTRE
   Le cas du journal anterieur au registre : `audit_event` sans
   `entity_fingerprint`, sans `new_geometry_wkb`, sans `invalidated_at`,
   et AUCUNE table `datasource_registry`. La migration doit ajouter les
   colonnes, retro-remplir `entity_fingerprint` (sans quoi tout
   l'historique ancien devient irrejouable), et surtout ne pas planter
   quand elle ne trouve aucune URI a recalculer.

   Le scenario mesure ensuite ce que ce journal-la perd, et si
   l'utilisateur en est averti.

Critere commun : le nombre de lignes, leurs horodatages et leurs
empreintes doivent etre identiques avant et apres, colonne par colonne
sur les colonnes qui existaient deja.
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

SCENARIO_ID = "mig_legacy_journal_end_to_end"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "mig_lj_project"
_T0 = datetime(2025, 11, 4, 7, 30, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()

# --- DDL of the versions this scenario reopens -----------------------------
# v1: no entity_fingerprint, no event_schema_version, no new_geometry_wkb,
# no invalidated_at, no datasource_registry, no datasource_alias.
_V1_AUDIT_EVENT = """CREATE TABLE audit_event (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_fingerprint TEXT NOT NULL,
    datasource_fingerprint TEXT NOT NULL,
    layer_id_snapshot TEXT,
    layer_name_snapshot TEXT,
    provider_type TEXT NOT NULL,
    feature_identity_json TEXT,
    operation_type TEXT NOT NULL CHECK(operation_type IN ('INSERT','UPDATE','DELETE')),
    attributes_json TEXT NOT NULL,
    geometry_wkb BLOB,
    geometry_type TEXT DEFAULT 'NoGeometry',
    crs_authid TEXT,
    field_schema_json TEXT,
    user_name TEXT NOT NULL,
    session_id TEXT,
    created_at TEXT NOT NULL,
    restored_from_event_id INTEGER
)"""

# v2: the three columns added by the v2 migration, still no invalidated_at
# (v5) and still no datasource_alias table (v4).
_V2_AUDIT_EVENT = _V1_AUDIT_EVENT.rstrip()[:-1] + """,
    entity_fingerprint TEXT,
    event_schema_version INTEGER,
    new_geometry_wkb BLOB
)"""

_SCHEMA_VERSION_DDL = """CREATE TABLE schema_version (
    version_number INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
)"""

_REGISTRY_DDL = """CREATE TABLE datasource_registry (
    datasource_fingerprint TEXT PRIMARY KEY,
    provider_type TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    layer_name TEXT,
    authcfg TEXT,
    crs_authid TEXT,
    geometry_type TEXT DEFAULT 'NoGeometry',
    last_seen_at TEXT NOT NULL
)"""

_V1_COLUMNS = (
    "project_fingerprint", "datasource_fingerprint", "layer_id_snapshot",
    "layer_name_snapshot", "provider_type", "feature_identity_json",
    "operation_type", "attributes_json", "geometry_wkb", "geometry_type",
    "crs_authid", "field_schema_json", "user_name", "session_id",
    "created_at", "restored_from_event_id",
)
_V2_COLUMNS = _V1_COLUMNS + ("entity_fingerprint", "event_schema_version",
                             "new_geometry_wkb")

# (offset_s, source key, operation, identity json, expected entity fingerprint)
# The OLDEST event was captured under the FILTERED fingerprint on purpose:
# `get_oldest_event_date` on the canonical fingerprint can only reach it
# through the alias, so the value doubles as a proof that the horizon
# offered to the user covers the history the rewind is about to replay.
_STORY = [
    (0, "filtered", "UPDATE", json.dumps({"fid": 13}), "fid:13"),
    (60, "bare", "UPDATE", json.dumps({"fid": 11}), "fid:11"),
    (120, "bare", "UPDATE", json.dumps({"pk_field": "gid", "pk_value": 12}), "pk:gid=12"),
    (180, "filtered", "DELETE", json.dumps({"fid": 14}), "fid:14"),
    (240, "bare", "INSERT", json.dumps({"fid": 15}), "fid:15"),
    # Journal written by an old build: identity unusable, kept as-is.
    (300, "bare", "UPDATE", "pas du json", None),
]
_OLDEST_BARE = (_T0 + timedelta(seconds=60)).isoformat()


def _sources(gpkg: str) -> dict:
    return {
        "bare": f"{gpkg}|layername=voirie",
        "filtered": f"{gpkg}|layername=voirie|subset=\"etat\" = 'neuf'",
    }


def _fingerprints(gpkg: str) -> dict:
    base = os.path.normcase(os.path.abspath(gpkg)).replace("\\", "/")
    return {
        "bare": f"ogr::{base}|layername=voirie",
        "filtered": f"ogr::{base}|layername=voirie|subset=\"etat\" = 'neuf'",
    }


def _seed_legacy(db_path: str, version: int, fps: dict, sources: dict) -> None:
    """Write the journal with the DDL of `version`, then stamp that version."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(_SCHEMA_VERSION_DDL)
        if version >= 2:
            conn.execute(_V2_AUDIT_EVENT)
            conn.execute(_REGISTRY_DDL)
            columns = _V2_COLUMNS
        else:
            conn.execute(_V1_AUDIT_EVENT)
            columns = _V1_COLUMNS
        insert = "".join((
            "INSERT INTO audit_event (", ", ".join(columns),
            ") VALUES (", ",".join(["?"] * len(columns)), ")",
        ))
        for idx, (offset, key, operation, identity, entity) in enumerate(_STORY):
            attrs = (json.dumps({"changed_only": {"etat": {"old": f"e{idx}", "new": f"e{idx + 1}"}}})
                     if operation == "UPDATE"
                     else json.dumps({"all_attributes": {"etat": f"e{idx}"}}))
            row = (
                _PROJECT_FP, fps[key], "lyr_voirie", "voirie", "ogr", identity,
                operation, attrs, b"\x01wkb_ancien", "LineString", "EPSG:2154",
                None, "tester", "sess_mig_lj",
                (_T0 + timedelta(seconds=offset)).isoformat(), None,
            )
            if version >= 2:
                row = row + (entity, 2, None)
            conn.execute(insert, row)
        now = _T0.isoformat()
        if version >= 2:
            for key, fingerprint in fps.items():
                conn.execute(
                    "INSERT INTO datasource_registry (datasource_fingerprint, "
                    "provider_type, source_uri, layer_name, authcfg, "
                    "crs_authid, geometry_type, last_seen_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (fingerprint, "ogr", sources[key], "voirie", "",
                     "EPSG:2154", "LineString", now),
                )
        conn.execute(
            "INSERT INTO schema_version (version_number, applied_at, description) "
            "VALUES (?, ?, ?)",
            (version, now, "fixture"),
        )
        conn.commit()
    finally:
        conn.close()


def _dump(db_path: str, columns) -> list:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT " + ", ".join(columns) +  # nosec B608
            " FROM audit_event ORDER BY event_id"
        ).fetchall()
    finally:
        conn.close()
    return [tuple(bytes(v) if isinstance(v, (bytes, memoryview)) else v
                  for v in row) for row in rows]


def _digest(dump: list) -> str:
    return hashlib.sha256(repr(dump).encode("utf-8", "replace")).hexdigest()[:16]


def _describe(db_path: str) -> dict:
    from recoverland.core.sqlite_schema import get_schema_version

    conn = sqlite3.connect(db_path)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(audit_event)").fetchall()]
        tables = sorted(r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall())
        entities = [r[0] for r in conn.execute(
            "SELECT entity_fingerprint FROM audit_event ORDER BY event_id"
        ).fetchall()] if "entity_fingerprint" in cols else []
        return {
            "version": get_schema_version(conn),
            "columns": cols,
            "tables": tables,
            "entities": entities,
            "n_events": conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0],
            "integrity": conn.execute("PRAGMA integrity_check").fetchone()[0],
        }
    finally:
        conn.close()


def _replay_probe(conn, canonical: str, layer_name: str) -> dict:
    """Can the rewind still PLAN on this migrated journal?"""
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, get_oldest_event_date,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events
    from recoverland.core.restore_planner import (
        plan_temporal_restore, preflight_check, check_retention_coverage,
    )

    cutoff = RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True)
    events = fetch_events_after_cutoff(conn, canonical, cutoff, trace_id="mig_lj")
    active = collapse_rewind_events(list(events))
    plan = plan_temporal_restore(active, canonical, layer_name, cutoff)
    oldest = get_oldest_event_date(conn, canonical)
    report = preflight_check(plan, oldest)
    # The user asks to go back to the very first event of the layer: the
    # retention gate must let him, otherwise the plugin refuses a period
    # it does hold.
    at_oldest = RestoreCutoff(CutoffType.BY_DATE, oldest or _CUTOFF, True)
    return {
        "n_fetched": len(events),
        "fingerprints_seen": sorted({e.datasource_fingerprint for e in events}),
        "n_active": len(active),
        "n_actions": len(plan.actions),
        "entity_count": plan.entity_count,
        "blocking_conflicts": [c.reason for c in plan.conflicts
                               if c.severity == "blocking"],
        "oldest": oldest,
        "retention": check_retention_coverage(at_oldest, oldest),
        "preflight": report.verdict.value if hasattr(report.verdict, "value")
        else str(report.verdict),
    }


def _case(ctx, tag: str, version: int) -> dict:
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.identity import (
        compute_fingerprint_for_source, split_fingerprint,
    )
    from recoverland.core.datasource_alias import list_aliases, load_reconciliation

    tmpdir = tempfile.mkdtemp(prefix=f"rl_mig_lj_{tag}_")
    gpkg = str(Path(tmpdir) / "voirie.gpkg")
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    fps = _fingerprints(gpkg)
    sources = _sources(gpkg)
    _seed_legacy(db_path, version, fps, sources)

    columns = _V2_COLUMNS if version >= 2 else _V1_COLUMNS
    before = _describe(db_path)
    before_dump = _dump(db_path, columns)

    canonical = compute_fingerprint_for_source("ogr", sources["bare"])
    conn = sqlite3.connect(db_path)
    error = ""
    aliases = []
    report = None
    replay = {}
    try:
        initialize_schema(conn)
        aliases = list_aliases(conn)
        report = load_reconciliation(conn)
        replay = _replay_probe(conn, canonical, "voirie")
    except Exception as exc:  # noqa: BLE001 - a failed open is the verdict
        error = f"{type(exc).__name__}: {exc}"
    finally:
        conn.commit()
        conn.close()

    # Could the mapping have been derived WITHOUT the registry? For a file
    # provider the fingerprint carries its own source: `ogr::<path>|...`.
    provider, normalized = split_fingerprint(fps["filtered"])
    derivable = compute_fingerprint_for_source(provider, normalized)

    res = {
        "tag": tag,
        "before": before,
        "after": _describe(db_path),
        "before_dump": before_dump,
        "after_dump": _dump(db_path, columns),
        "error": error,
        "aliases": {a[0]: a[1] for a in aliases},
        "report": None if report is None else {
            "status": report.status,
            "scanned": report.scanned,
            "aliases_created": report.aliases_created,
            "already_canonical": report.already_canonical,
            "ambiguous": [a.fingerprint for a in report.ambiguous],
            "without_source_uri": list(report.without_source_uri),
            "is_degraded": report.is_degraded,
        },
        "replay": replay,
        "canonical": canonical,
        "fps": fps,
        "derivable_from_fingerprint": derivable,
    }
    flog(
        f"mig_legacy_journal_end_to_end case={tag}: trace_id={ctx.trace_id} "
        f"version {before['version']}->{res['after']['version']} "
        f"lignes {before['n_events']}->{res['after']['n_events']} "
        f"error={error!r} alias={len(aliases)} replay={replay}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    flog(
        f"mig_legacy_journal_end_to_end setup: trace_id={ctx.trace_id} "
        f"n_events_par_journal={len(_STORY)}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"mig_legacy_journal_end_to_end run start: trace_id={ctx.trace_id}", "INFO")
    ctx.data["v2"] = _case(ctx, "v2_avec_registre", version=2)
    ctx.data["v1"] = _case(ctx, "v1_sans_registre", version=1)
    flog(f"mig_legacy_journal_end_to_end run end: trace_id={ctx.trace_id}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION

    out = []
    n_total = len(_STORY)
    n_bare = sum(1 for row in _STORY if row[1] == "bare")

    # ===== A. Journal v2 avec registre ===================================
    v2 = ctx.data.get("v2") or {}
    after = v2.get("after") or {}
    replay = v2.get("replay") or {}
    report = v2.get("report") or {}
    out.append((
        "v2_ouverture_sans_erreur",
        not v2.get("error"),
        f"erreur={v2.get('error')!r} : un journal laisse de cote quelques mois "
        f"doit se rouvrir, c'est la seule copie de l'historique",
    ))
    out.append((
        "v2_version_a_jour",
        after.get("version") == CURRENT_SCHEMA_VERSION,
        f"schema_version={after.get('version')} attendu={CURRENT_SCHEMA_VERSION}",
    ))
    out.append((
        "v2_colonne_invalidated_at_ajoutee",
        "invalidated_at" in (after.get("columns") or []),
        f"colonnes={after.get('columns')}",
    ))
    out.append((
        "v2_table_alias_creee",
        "datasource_alias" in (after.get("tables") or []),
        f"tables={after.get('tables')} : sans elle la reconciliation n'a nulle "
        f"part ou ecrire",
    ))
    out.append((
        "v2_aucune_ligne_perdue",
        after.get("n_events") == n_total,
        f"lignes apres migration={after.get('n_events')} attendu={n_total}",
    ))
    out.append((
        "v2_aucune_colonne_existante_modifiee",
        v2.get("before_dump") == v2.get("after_dump"),
        f"dump des colonnes v2 avant={_digest(v2.get('before_dump') or [])} "
        f"apres={_digest(v2.get('after_dump') or [])} : la migration ajoute, "
        f"elle ne recrit pas",
    ))
    out.append((
        "v2_alias_de_la_forme_filtree",
        (v2.get("aliases") or {}).get((v2.get("fps") or {}).get("filtered"))
        == v2.get("canonical"),
        f"alias={v2.get('aliases')} attendu la forme filtree -> "
        f"{v2.get('canonical')!r}",
    ))
    out.append((
        "v2_rewind_voit_tout_l_historique",
        replay.get("n_fetched") == n_total,
        f"evenements lus par le rewind={replay.get('n_fetched')} attendu="
        f"{n_total} ; empreintes rencontrees="
        f"{[fp[-28:] for fp in replay.get('fingerprints_seen') or []]}",
    ))
    out.append((
        "v2_rewind_planifie_reellement",
        replay.get("n_actions", 0) >= n_total - 1
        and replay.get("entity_count", 0) >= 4,
        f"actions planifiees={replay.get('n_actions')} entites="
        f"{replay.get('entity_count')} conflits bloquants="
        f"{replay.get('blocking_conflicts')} : lire les evenements ne suffit "
        f"pas, le plan doit se construire (1 evenement de la fixture porte "
        f"une identite illisible et ne peut pas etre planifie)",
    ))
    out.append((
        "v2_horizon_couvre_l_historique_ancien",
        replay.get("oldest") == _T0.isoformat() and replay.get("retention") is None,
        f"plus ancien evenement offert au rewind={replay.get('oldest')!r} "
        f"attendu={_T0.isoformat()!r} : cet evenement-la a ete capture sous "
        f"l'empreinte FILTREE ; sans l'alias l'horizon se serait arrete a "
        f"{_OLDEST_BARE!r} et le curseur de date aurait cache deux minutes "
        f"d'historique. Couverture de retention={replay.get('retention')!r} "
        f"attendu None, sinon le plugin refuse une periode qu'il detient.",
    ))
    out.append((
        "v2_journal_non_degrade",
        report.get("status") == "ok" and report.get("is_degraded") is False,
        f"compte-rendu={report}",
    ))
    out.append((
        "v2_base_saine",
        after.get("integrity") == "ok",
        f"PRAGMA integrity_check={after.get('integrity')!r}",
    ))

    # ===== B. Journal v1 sans registre ===================================
    v1 = ctx.data.get("v1") or {}
    after1 = v1.get("after") or {}
    replay1 = v1.get("replay") or {}
    report1 = v1.get("report") or {}
    out.append((
        "v1_ouverture_sans_erreur",
        not v1.get("error"),
        f"erreur={v1.get('error')!r} : un journal anterieur au registre ne "
        f"doit pas faire echouer la migration faute d'URI a recalculer",
    ))
    out.append((
        "v1_version_a_jour",
        after1.get("version") == CURRENT_SCHEMA_VERSION,
        f"schema_version={after1.get('version')} attendu={CURRENT_SCHEMA_VERSION}",
    ))
    out.append((
        "v1_aucune_ligne_perdue",
        after1.get("n_events") == n_total,
        f"lignes={after1.get('n_events')} attendu={n_total}",
    ))
    out.append((
        "v1_colonnes_existantes_intactes",
        v1.get("before_dump") == v1.get("after_dump"),
        f"dump des colonnes v1 avant={_digest(v1.get('before_dump') or [])} "
        f"apres={_digest(v1.get('after_dump') or [])}",
    ))
    expected_entities = [row[4] for row in _STORY]
    out.append((
        "v1_entity_fingerprint_retro_rempli",
        after1.get("entities") == expected_entities,
        f"entity_fingerprint apres migration={after1.get('entities')} "
        f"attendu={expected_entities} : c'est la cle de regroupement du "
        f"rewind ; laissee vide, tout l'historique ancien devient visible "
        f"mais irrejouable",
    ))
    out.append((
        "v1_reconciliation_degrade_proprement",
        report1.get("status") == "ok" and report1.get("aliases_created") == 0
        and len(report1.get("without_source_uri") or []) == 2,
        f"compte-rendu={report1} attendu status=ok aliases_created=0 et les 2 "
        f"empreintes listees sans URI : sans registre, la migration doit se "
        f"declarer impuissante, pas echouer",
    ))
    out.append((
        "v1_rewind_rejoue_ce_qu_il_voit",
        replay1.get("n_actions", 0) >= n_bare - 1,
        f"actions planifiees={replay1.get('n_actions')} sur les "
        f"{n_bare} evenements de l'empreinte canonique ; conflits bloquants="
        f"{replay1.get('blocking_conflicts')}",
    ))
    out.append((
        "v1_base_saine",
        after1.get("integrity") == "ok",
        f"PRAGMA integrity_check={after1.get('integrity')!r}",
    ))

    # ===== C. Ce que le journal sans registre perd, et le silence ========
    out.append((
        "v1_historique_filtre_rattache_ou_signale",
        replay1.get("n_fetched") == n_total or report1.get("is_degraded") is True,
        f"le rewind ne voit que {replay1.get('n_fetched')} des {n_total} "
        f"evenements de cette couche (les {n_total - n_bare} captures sous la "
        f"forme filtree restent detachees), et le journal se declare "
        f"is_degraded={report1.get('is_degraded')} : l'utilisateur n'est donc "
        f"averti de rien. Or le rattachement etait derivable sans registre : "
        f"pour un provider fichier l'empreinte CONTIENT sa source, et "
        f"`compute_fingerprint_for_source(*split_fingerprint(fp))` rend "
        f"{v1.get('derivable_from_fingerprint')!r}, soit exactement "
        f"{v1.get('canonical')!r}. Le repli sur l'empreinte elle-meme "
        f"couvrirait tous les journaux anterieurs au registre.",
    ))
    out.append((
        "v1_derivation_depuis_l_empreinte_est_bien_exacte",
        v1.get("derivable_from_fingerprint") == v1.get("canonical"),
        f"canonicalisation de la partie normalisee de l'empreinte filtree="
        f"{v1.get('derivable_from_fingerprint')!r} vs canonique attendue="
        f"{v1.get('canonical')!r} : preuve que l'information necessaire est "
        f"deja dans le journal",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"mig_legacy_journal_end_to_end.*trace_id={ctx.trace_id}",
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
