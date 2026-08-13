"""mig_degraded_environments - RecoverLand validation runtime.

Invariant: I-JRN. La migration peut echouer ; elle ne doit ni corrompre,
ni faire disparaitre l'historique, ni empecher le plugin de servir.

CE QUE CE SCENARIO PROUVE
-------------------------
Les journaux vivent la ou vivent les projets : partages reseau montes en
lecture seule, disques pleins, dossiers ouverts par une seconde instance
de QGIS, journaux plus vieux que le registre. Six environnements
degrades, tous mesures sur un vrai fichier.

A. JOURNAL ANCIEN EN LECTURE SEULE
   Le fichier porte l'attribut lecture seule. La migration ne peut rien
   ecrire. Exigences : ne rien inventer, ne rien abimer, et laisser
   l'historique LISIBLE -- l'utilisateur doit pouvoir consulter son
   journal meme s'il ne peut pas le faire evoluer.

B. JOURNAL DEJA A JOUR EN LECTURE SEULE
   Le cas courant du partage reseau : une fois la migration faite, la
   reouverture ne doit plus rien vouloir ecrire du tout, donc ne pas
   echouer.

C. REGISTRE VIDE
   La table existe mais aucune ligne. Aucune URI a recalculer : la passe
   doit se declarer impuissante, pas echouer.

D. SOURCE_URI ABSENTE ET PROVIDER INCOHERENT
   Une ligne de registre avec `source_uri` vide, une autre dont le
   `provider_type` ne correspond pas au prefixe de l'empreinte (la ligne
   ne decrit donc pas cette source). Aucune des deux ne doit produire
   d'alias devine.

E. BASE VERROUILLEE PAR UNE AUTRE INSTANCE
   Une seconde connexion tient une transaction ecrivain. L'ouverture doit
   echouer proprement (verrou nomme), ne rien laisser a moitie ecrit, et
   la migration doit se faire integralement des que le verrou tombe.

F. DISQUE PLEIN AU MOMENT D'ECRIRE LE COMPTE-RENDU
   Les alias sont ecrits, le marqueur ne passe pas. Le journal ne doit
   pas se croire reconcilie : la passe suivante doit repasser et ne rien
   dupliquer.

G. TABLE D'ALIAS ABSENTE (journal anterieur a v4 lu sans migration)
   Les chemins de lecture appellent `expand_fingerprints`. Sur un journal
   sans table `datasource_alias`, ils doivent degrader vers l'empreinte
   demandee et continuer a servir : un rewind incomplet est mauvais, un
   rewind qui leve est inutilisable.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_degraded_environments"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "mig_de_project"
_T0 = datetime(2026, 6, 15, 9, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()

_STORY = [
    (0, "bare", "UPDATE", "fid:21"),
    (60, "filtered", "UPDATE", "fid:22"),
    (120, "filtered", "DELETE", "fid:23"),
    (180, "bare", "UPDATE", "fid:24"),
]


def _sources(gpkg: str) -> dict:
    return {
        "bare": f"{gpkg}|layername=reseau",
        "filtered": f"{gpkg}|layername=reseau|subset=\"type\" = 'HTA'",
    }


def _fingerprints(gpkg: str) -> dict:
    base = os.path.normcase(os.path.abspath(gpkg)).replace("\\", "/")
    return {
        "bare": f"ogr::{base}|layername=reseau",
        "filtered": f"ogr::{base}|layername=reseau|subset=\"type\" = 'HTA'",
    }


def _new_journal(registry: str = "full", version: int = 5) -> tuple:
    """Seed a v5 journal. `registry` selects how broken the registry is."""
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_mig_de_")
    gpkg = str(Path(tmpdir) / "reseau.gpkg")
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    fps = _fingerprints(gpkg)
    sources = _sources(gpkg)

    conn = sqlite3.connect(db_path)
    try:
        # A real journal is in WAL as soon as `apply_pragmas` has run once.
        # Seeding in rollback mode would make the lock case fail on the
        # journal_mode PRAGMA instead of where production fails.
        conn.execute("PRAGMA journal_mode=WAL")
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = "".join((
            "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
            ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
        ))
        for idx, (offset, key, operation, entity) in enumerate(_STORY):
            attrs = (json.dumps({"changed_only": {"t": {"old": idx, "new": idx + 1}}})
                     if operation == "UPDATE"
                     else json.dumps({"all_attributes": {"t": idx}}))
            conn.execute(insert, (
                _PROJECT_FP, fps[key], "lyr_reseau", "reseau", "ogr",
                json.dumps({"fid": int(entity.split(":")[1])}),
                operation, attrs, b"\x01wkb", "LineString", "EPSG:2154", None,
                "tester", "sess_mig_de",
                (_T0 + timedelta(seconds=offset)).isoformat(), None,
                entity, 2, None, None,
            ))
        now = _T0.isoformat()
        rows = []
        if registry == "full":
            rows = [(fps["bare"], "ogr", sources["bare"]),
                    (fps["filtered"], "ogr", sources["filtered"])]
        elif registry == "broken":
            # An empty source_uri, and a row whose provider does not even
            # match the fingerprint prefix: neither describes this source.
            rows = [(fps["filtered"], "ogr", ""),
                    (fps["bare"], "postgres", "dbname='x' table=\"public\".\"y\"")]
        for fingerprint, provider, uri in rows:
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (fingerprint, provider, uri, "reseau", "", "EPSG:2154",
                 "LineString", now),
            )
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (version, now, "fixture"),
        )
        conn.commit()
    finally:
        conn.close()
    _checkpoint(db_path)
    return tmpdir, db_path, fps, sources


def _checkpoint(db_path: str) -> None:
    """Fold the WAL back into the single file, as a closed journal is."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    for suffix in ("-wal", "-shm"):
        side = db_path + suffix
        if os.path.exists(side):
            try:
                os.remove(side)
            except OSError:
                pass


def _dump(db_path: str) -> list:
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
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


def _sha_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _open(db_path: str) -> str:
    from recoverland.core.sqlite_schema import initialize_schema

    conn = sqlite3.connect(db_path)
    try:
        initialize_schema(conn)
        conn.commit()
        return ""
    except Exception as exc:  # noqa: BLE001 - the error IS the measurement
        return f"{type(exc).__name__}: {exc}"
    finally:
        conn.close()


def _aliases(db_path: str) -> list:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return sorted(conn.execute(
            "SELECT alias_fingerprint, target_fingerprint FROM datasource_alias"
        ).fetchall())
    except sqlite3.Error:
        return []
    finally:
        conn.close()


def _readonly_search(db_path: str, fingerprint: str) -> dict:
    """Can the user still consult his history on a file he cannot write?"""
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        criteria = SearchCriteria(
            datasource_fingerprint=fingerprint, layer_name=None,
            operation_type=None, user_name=None, start_date=None,
            end_date=None, page=1, page_size=50)
        result = search_events(conn, criteria, trace_id="mig_de")
        return {"total": result.total_count, "error": ""}
    except Exception as exc:  # noqa: BLE001
        return {"total": None, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        conn.close()


def _set_readonly(path: str, readonly: bool) -> None:
    os.chmod(path, stat.S_IREAD if readonly else (stat.S_IWRITE | stat.S_IREAD))


# ---------------------------------------------------------------------------
# A / B. Read-only journal
# ---------------------------------------------------------------------------

def _case_readonly(ctx, already_migrated: bool) -> dict:
    from recoverland.core.logger import flog

    tmpdir, db_path, fps, _sources_map = _new_journal()
    tag = "deja_migre" if already_migrated else "ancien"
    pre_error = ""
    if already_migrated:
        pre_error = _open(db_path)
        _checkpoint(db_path)
    before_dump = _dump(db_path)
    sha_before = _sha_file(db_path)

    _set_readonly(db_path, True)
    try:
        error = _open(db_path)
        aliases = _aliases(db_path)
        search = _readonly_search(db_path, fps["bare"])
        sha_after = _sha_file(db_path)
        after_dump = _dump(db_path)
    finally:
        _set_readonly(db_path, False)

    res = {
        "tag": tag,
        "pre_error": pre_error,
        "error": error,
        "aliases": aliases,
        "n_aliases": len(aliases),
        "search": search,
        "file_unchanged": sha_before == sha_after,
        "dump_unchanged": before_dump == after_dump,
        "n_events": len(after_dump),
    }
    flog(
        f"mig_degraded_environments case=readonly_{tag}: trace_id={ctx.trace_id} "
        f"error={error!r} alias={len(aliases)} fichier_intact={res['file_unchanged']} "
        f"recherche={search}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# C / D. Registry empty, or describing something else
# ---------------------------------------------------------------------------

def _case_registry(ctx, registry: str) -> dict:
    from recoverland.core.logger import flog
    from recoverland.core.datasource_alias import load_reconciliation

    tmpdir, db_path, fps, _sources_map = _new_journal(registry=registry)
    before_dump = _dump(db_path)
    error = _open(db_path)

    conn = sqlite3.connect(db_path)
    try:
        report = load_reconciliation(conn)
    finally:
        conn.close()

    res = {
        "registry": registry,
        "error": error,
        "aliases": _aliases(db_path),
        "report": None if report is None else {
            "status": report.status,
            "scanned": report.scanned,
            "aliases_created": report.aliases_created,
            "without_source_uri": len(report.without_source_uri),
            "ambiguous": len(report.ambiguous),
            "is_degraded": report.is_degraded,
        },
        "dump_unchanged": before_dump == _dump(db_path),
        "n_events": len(_dump(db_path)),
    }
    flog(
        f"mig_degraded_environments case=registry_{registry}: "
        f"trace_id={ctx.trace_id} error={error!r} "
        f"alias={len(res['aliases'])} report={res['report']}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# E. Another QGIS holds a writer transaction
# ---------------------------------------------------------------------------

def _case_locked(ctx) -> dict:
    from recoverland.core.logger import flog

    tmpdir, db_path, _fps, _sources_map = _new_journal()
    before_dump = _dump(db_path)

    holder = sqlite3.connect(db_path)
    holder.isolation_level = None
    holder.execute("BEGIN IMMEDIATE")
    holder.execute(
        "INSERT INTO backend_settings (setting_key, setting_value, updated_at) "
        "VALUES ('mig_de_lock', '1', ?)", (_T0.isoformat(),))

    started = time.monotonic()
    error = _open(db_path)
    waited = time.monotonic() - started
    aliases_locked = _aliases(db_path)

    holder.execute("COMMIT")
    holder.close()

    error_after = _open(db_path)
    res = {
        "error": error,
        "waited_sec": round(waited, 2),
        "aliases_while_locked": aliases_locked,
        "error_after_release": error_after,
        "aliases_after_release": _aliases(db_path),
        "dump_unchanged": before_dump == _dump(db_path),
        "n_events": len(_dump(db_path)),
    }
    flog(
        f"mig_degraded_environments case=locked: trace_id={ctx.trace_id} "
        f"error={error!r} attente={res['waited_sec']}s "
        f"alias_pendant_verrou={len(aliases_locked)} "
        f"alias_apres={len(res['aliases_after_release'])}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# F. Disk full when the report is persisted
# ---------------------------------------------------------------------------

def _case_marker_write_fails(ctx) -> dict:
    import recoverland.core.datasource_alias as da
    from recoverland.core.logger import flog

    tmpdir, db_path, _fps, _sources_map = _new_journal()
    before_dump = _dump(db_path)
    original = da._store_reconciliation

    def _failing(conn, report):
        raise sqlite3.OperationalError("database or disk is full (simule)")

    da._store_reconciliation = _failing
    try:
        error = _open(db_path)
    finally:
        da._store_reconciliation = original
    first = {"error": error, "aliases": _aliases(db_path)}

    conn = sqlite3.connect(db_path)
    try:
        marker = conn.execute(
            "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
            (da.RECONCILE_SETTING_KEY,)).fetchone()
    finally:
        conn.close()
    first["marker_present"] = marker is not None

    error_again = _open(db_path)
    conn = sqlite3.connect(db_path)
    try:
        report = da.load_reconciliation(conn)
    finally:
        conn.close()

    res = {
        "first": first,
        "error_again": error_again,
        "aliases_after": _aliases(db_path),
        "status_after": None if report is None else report.status,
        "created_after": None if report is None else report.aliases_created,
        "already_after": None if report is None else report.already_aliased,
        "dump_unchanged": before_dump == _dump(db_path),
    }
    flog(
        f"mig_degraded_environments case=marker_fail: trace_id={ctx.trace_id} "
        f"premier_passage={first} deuxieme={res['status_after']!r} "
        f"alias={len(res['aliases_after'])}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# G. No datasource_alias table at all
# ---------------------------------------------------------------------------

def _case_no_alias_table(ctx) -> dict:
    from recoverland.core.logger import flog
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, resolve_scope, get_oldest_event_date,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events, get_distinct_layers
    from recoverland.core.datasource_registry import lookup_datasource

    tmpdir, db_path, fps, _sources_map = _new_journal()
    conn = sqlite3.connect(db_path)
    conn.execute("DROP TABLE datasource_alias")
    conn.commit()

    out = {"asked": fps["bare"]}
    try:
        out["scope"] = resolve_scope(conn, fps["bare"])
        events = fetch_events_after_cutoff(
            conn, fps["bare"], RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True),
            trace_id="mig_de")
        out["n_events"] = len(events)
        out["oldest"] = get_oldest_event_date(conn, fps["bare"])
        criteria = SearchCriteria(
            datasource_fingerprint=fps["bare"], layer_name=None,
            operation_type=None, user_name=None, start_date=None,
            end_date=None, page=1, page_size=50)
        out["search_total"] = search_events(conn, criteria, trace_id="mig_de").total_count
        out["distinct_layers"] = len(get_distinct_layers(conn))
        out["registry_hit"] = lookup_datasource(conn, fps["bare"]) is not None
        out["error"] = ""
    except Exception as exc:  # noqa: BLE001 - raising here is the failure
        out["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        conn.close()

    flog(
        f"mig_degraded_environments case=no_alias_table: trace_id={ctx.trace_id} "
        f"resultat={out}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return out


def setup(ctx):
    from recoverland.core.logger import flog

    flog(
        f"mig_degraded_environments setup: trace_id={ctx.trace_id} "
        f"n_events_par_journal={len(_STORY)}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"mig_degraded_environments run start: trace_id={ctx.trace_id}", "INFO")
    ctx.data["ro_legacy"] = _case_readonly(ctx, already_migrated=False)
    ctx.data["ro_uptodate"] = _case_readonly(ctx, already_migrated=True)
    ctx.data["registry_empty"] = _case_registry(ctx, "empty")
    ctx.data["registry_broken"] = _case_registry(ctx, "broken")
    ctx.data["locked"] = _case_locked(ctx)
    ctx.data["marker_fail"] = _case_marker_write_fails(ctx)
    ctx.data["no_alias_table"] = _case_no_alias_table(ctx)
    flog(f"mig_degraded_environments run end: trace_id={ctx.trace_id}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    n_total = len(_STORY)

    # ===== A. Journal ancien en lecture seule ============================
    ro = ctx.data.get("ro_legacy") or {}
    out.append((
        "lecture_seule_aucune_ligne_perdue",
        ro.get("n_events") == n_total and ro.get("dump_unchanged") is True,
        f"lignes={ro.get('n_events')} attendu={n_total} ; contenu inchange="
        f"{ro.get('dump_unchanged')} : un journal qu'on ne peut pas migrer "
        f"reste un journal",
    ))
    out.append((
        "lecture_seule_fichier_non_modifie",
        ro.get("file_unchanged") is True,
        f"empreinte du fichier inchangee={ro.get('file_unchanged')} : sur un "
        f"partage en lecture seule, la tentative de migration ne doit laisser "
        f"aucune trace",
    ))
    out.append((
        "lecture_seule_aucun_alias_invente",
        ro.get("n_aliases") == 0,
        f"alias ecrits={ro.get('n_aliases')} attendu 0",
    ))
    out.append((
        "lecture_seule_historique_toujours_consultable",
        (ro.get("search") or {}).get("total") == 2
        and not (ro.get("search") or {}).get("error"),
        f"recherche sur le journal en lecture seule={ro.get('search')} : "
        f"attendu 2 evenements pour l'empreinte non filtree. Meme sans "
        f"migration possible, l'utilisateur doit pouvoir consulter son "
        f"historique.",
    ))
    error_ro = (ro.get("error") or "")
    out.append((
        "lecture_seule_echec_explicite_ou_absent",
        not error_ro or "readonly" in error_ro.lower() or "read-only" in error_ro.lower(),
        f"erreur remontee par l'ouverture={error_ro!r} : si l'ouverture "
        f"echoue, elle doit nommer la lecture seule pour que le plugin puisse "
        f"le dire a l'utilisateur, jamais echouer sur un effet de bord "
        f"incomprehensible",
    ))
    out.append((
        "lecture_seule_ne_bloque_pas_le_plugin",
        not error_ro,
        f"ouverture d'un journal v5 en lecture seule : erreur={error_ro!r}. "
        f"L'echec ne vient PAS de la reconciliation, qui avale tout, mais des "
        f"etapes de schema qui la precedent dans `initialize_schema` : passer "
        f"CURRENT_SCHEMA_VERSION de 5 a 6 rend TOUS les journaux existants "
        f"migrables, donc tous demandeurs d'une ecriture a la prochaine "
        f"ouverture. Le cas B ci-dessous montre qu'un journal deja a la "
        f"version courante s'ouvre en lecture seule sans erreur : avant ce "
        f"changement, un journal v5 sur un partage reseau monte en lecture "
        f"seule etait dans ce cas-la et s'ouvrait. Il ne s'ouvre plus. "
        f"L'historique reste intact et lisible (assertions ci-dessus), mais "
        f"le projet n'est plus ouvrable tant que le partage n'est pas "
        f"remonte en ecriture.",
    ))

    # ===== B. Journal deja a jour en lecture seule =======================
    ro2 = ctx.data.get("ro_uptodate") or {}
    out.append((
        "lecture_seule_journal_a_jour_s_ouvre",
        not ro2.get("error") and not ro2.get("pre_error"),
        f"journal deja migre puis passe en lecture seule : erreur="
        f"{ro2.get('error')!r} (migration prealable={ro2.get('pre_error')!r}) "
        f"attendu aucune : une fois a jour, l'ouverture ne doit plus rien "
        f"vouloir ecrire",
    ))
    out.append((
        "lecture_seule_journal_a_jour_intact",
        ro2.get("file_unchanged") is True and ro2.get("n_events") == n_total,
        f"fichier inchange={ro2.get('file_unchanged')} lignes={ro2.get('n_events')}",
    ))

    # ===== C. Registre vide =============================================
    empty = ctx.data.get("registry_empty") or {}
    report_empty = empty.get("report") or {}
    out.append((
        "registre_vide_ouverture_sans_erreur",
        not empty.get("error"),
        f"erreur={empty.get('error')!r}",
    ))
    out.append((
        "registre_vide_aucun_alias_devine",
        empty.get("aliases") == [] and report_empty.get("without_source_uri") == 2,
        f"alias={empty.get('aliases')} compte-rendu={report_empty} : sans URI, "
        f"la migration doit se declarer impuissante et lister les empreintes "
        f"concernees",
    ))
    out.append((
        "registre_vide_journal_intact",
        empty.get("dump_unchanged") is True and empty.get("n_events") == n_total,
        f"contenu inchange={empty.get('dump_unchanged')} "
        f"lignes={empty.get('n_events')}",
    ))

    # ===== D. source_uri vide / provider incoherent =====================
    broken = ctx.data.get("registry_broken") or {}
    report_broken = broken.get("report") or {}
    out.append((
        "registre_incoherent_ouverture_sans_erreur",
        not broken.get("error"),
        f"erreur={broken.get('error')!r}",
    ))
    out.append((
        "registre_incoherent_aucun_alias_devine",
        broken.get("aliases") == []
        and report_broken.get("without_source_uri") == 2,
        f"alias={broken.get('aliases')} compte-rendu={report_broken} : une URI "
        f"vide et une ligne dont le provider ne correspond pas au prefixe de "
        f"l'empreinte ne decrivent pas cette source ; en tirer un alias "
        f"rattacherait un historique a la mauvaise table",
    ))
    out.append((
        "registre_incoherent_journal_intact",
        broken.get("dump_unchanged") is True and broken.get("n_events") == n_total,
        f"contenu inchange={broken.get('dump_unchanged')} "
        f"lignes={broken.get('n_events')}",
    ))

    # ===== E. Base verrouillee ==========================================
    locked = ctx.data.get("locked") or {}
    err_lock = (locked.get("error") or "").lower()
    out.append((
        "verrou_signale_explicitement",
        "locked" in err_lock or "busy" in err_lock,
        f"erreur pendant le verrou={locked.get('error')!r} attente="
        f"{locked.get('waited_sec')}s : le refus doit nommer le verrou, "
        f"jamais passer pour un succes",
    ))
    out.append((
        "verrou_attente_effective_avant_refus",
        (locked.get("waited_sec") or 0) >= 4.0,
        f"attente reelle avant le refus={locked.get('waited_sec')}s : le "
        f"busy_timeout de 5s existe pour absorber la transaction courte d'une "
        f"autre connexion ; sans attente effective, la moindre seconde de "
        f"concurrence rend le projet non ouvrable",
    ))
    out.append((
        "verrou_aucun_alias_a_moitie_ecrit",
        locked.get("aliases_while_locked") == [],
        f"alias ecrits pendant le verrou={locked.get('aliases_while_locked')} "
        f"attendu aucun",
    ))
    out.append((
        "verrou_libere_migration_complete",
        not locked.get("error_after_release")
        and len(locked.get("aliases_after_release") or []) == 1,
        f"apres liberation : erreur={locked.get('error_after_release')!r} "
        f"alias={locked.get('aliases_after_release')} attendu 1 (la forme "
        f"filtree) : une migration empechee doit se refaire, pas se croire "
        f"faite",
    ))
    out.append((
        "verrou_journal_intact",
        locked.get("dump_unchanged") is True and locked.get("n_events") == n_total,
        f"contenu inchange={locked.get('dump_unchanged')} "
        f"lignes={locked.get('n_events')}",
    ))

    # ===== F. Disque plein a l'ecriture du compte-rendu ==================
    mf = ctx.data.get("marker_fail") or {}
    first = mf.get("first") or {}
    out.append((
        "marqueur_illisible_ne_bloque_pas_l_ouverture",
        not first.get("error"),
        f"erreur au premier passage={first.get('error')!r} : l'echec de "
        f"l'ecriture du compte-rendu ne doit pas defaire les alias ni "
        f"empecher l'ouverture",
    ))
    out.append((
        "sans_marqueur_la_passe_est_refaite",
        first.get("marker_present") is False and mf.get("status_after") == "ok",
        f"marqueur present apres le premier passage={first.get('marker_present')} "
        f"(attendu False) ; statut apres reouverture={mf.get('status_after')!r} "
        f"(attendu 'ok') : le journal ne doit pas se croire reconcilie",
    ))
    out.append((
        "reprise_sans_doublon",
        len(mf.get("aliases_after") or []) == 1 and mf.get("created_after") == 0
        and mf.get("already_after") == 1,
        f"alias apres reprise={mf.get('aliases_after')} created="
        f"{mf.get('created_after')} already={mf.get('already_after')} : l'alias "
        f"deja ecrit doit etre compte, pas reecrit",
    ))
    out.append((
        "marqueur_journal_intact",
        mf.get("dump_unchanged") is True,
        f"contenu inchange={mf.get('dump_unchanged')}",
    ))

    # ===== G. Table d'alias absente =====================================
    nat = ctx.data.get("no_alias_table") or {}
    out.append((
        "sans_table_alias_aucune_exception",
        not nat.get("error"),
        f"erreur={nat.get('error')!r} : sur un journal anterieur a v4, les "
        f"chemins de lecture doivent degrader vers l'empreinte demandee",
    ))
    out.append((
        "sans_table_alias_le_rewind_sert_encore",
        nat.get("n_events") == 2 and nat.get("search_total") == 2
        and nat.get("scope") == [nat.get("asked")],
        f"portee resolue={[fp[-25:] for fp in nat.get('scope') or []]} "
        f"evenements={nat.get('n_events')} recherche={nat.get('search_total')} "
        f"attendu 2 et 2 (les evenements de l'empreinte demandee)",
    ))
    out.append((
        "sans_table_alias_les_annexes_survivent",
        nat.get("oldest") == _T0.isoformat()
        and nat.get("registry_hit") is True
        and nat.get("distinct_layers") == 2,
        f"plus ancien={nat.get('oldest')!r} registre={nat.get('registry_hit')} "
        f"couches listees={nat.get('distinct_layers')}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"mig_degraded_environments.*trace_id={ctx.trace_id}",
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
