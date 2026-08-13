"""tx_journal_migration_locks - RecoverLand validation runtime.

Invariant: I-JRN (le stockage se repare ou se refuse, jamais il ne se vide).

Trois facons de maltraiter le stockage lui-meme, dans des dossiers
temporaires, avec le vrai code produit.

A. MIGRATION D'UN VIEUX JOURNAL
   `sqlite_schema.initialize_schema` fait tourner `_run_migrations` puis
   `_backfill_entity_fingerprint`. Le retro-remplissage n'est declenche que
   si `current_version < 2`, et il derive la cle d'entite de
   `feature_identity_json` via `identity.compute_entity_fingerprint`.
   Enjeu utilisateur : `entity_fingerprint` est la cle sur laquelle REWIND
   regroupe les evenements d'une meme entite. Un journal migre dont les
   anciennes lignes gardent `entity_fingerprint = NULL` est un journal dont
   toute la partie ancienne devient invisible pour REWIND -- l'utilisateur
   voit ses evenements dans la recherche mais aucun rewind ne les rejoue.
   Le scenario applique le DDL v1 (pas de entity_fingerprint,
   pas de new_geometry_wkb, pas de invalidated_at, pas de datasource_alias),
   force `schema_version = 1`, y met 4 lignes representatives, puis ouvre.
   Cas hostile complementaire : un journal marque `schema_version = 99`
   (ecrit par une version future du plugin) ne doit ni etre migre a
   l'envers, ni perdre de lignes.

B. CORRUPTION AU NIVEAU OCTET
   Un journal reel de plusieurs dizaines de pages, dont une page de donnees
   (pas l'en-tete) est ecrasee par des octets arbitraires -- c'est ce que
   produit un secteur defaillant ou une copie interrompue sur cle USB.
   Deux exigences : `check_journal_integrity` doit le DIRE, et ne doit RIEN
   modifier. Un outil de recuperation qui reecrit un fichier deja abime est
   la derniere chose dont l'utilisateur a besoin : la copie de secours qu'il
   fera ensuite sera celle de l'etat degrade.

C. DEUX ECRIVAINS CONCURRENTS
   C1. Deux connexions SQLite sur le meme fichier (une transaction ecrivain
       ouverte d'un cote, une insertion de l'autre). On mesure si le
       `busy_timeout=5000` de `apply_pragmas` est reellement applique et si
       l'evenement du perdant est perdu ou seulement retarde.
   C2. Le verrou multi-instances de `JournalManager._acquire_writer_lock`.
       Un VRAI processus tiers vivant est demarre, son PID est ecrit dans le
       fichier `.rlwriter` : l'ouverture doit etre refusee. Le processus est
       ensuite tue : le verrou perime doit etre recupere.

Scenario d'echec concret vise en C2 : deux QGIS ouverts sur le meme projet
reseau, les deux ecrivent dans le meme journal, les event_id s'entrelacent
et un REWIND compense des evenements que l'autre instance vient de rejouer.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess  # nosec B404: lance un processus python inerte, en local
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_journal_migration_locks"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_jml_datasource"
_T0 = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc)

# DDL du schema v1 : audit_event sans entity_fingerprint,
# event_schema_version, new_geometry_wkb ni invalidated_at.
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

_V1_SCHEMA_VERSION = """CREATE TABLE schema_version (
    version_number INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL,
    description TEXT
)"""

# (identity_json, entity_fingerprint attendu apres migration)
_LEGACY_ROWS = [
    (json.dumps({"fid": 7}), "fid:7"),
    (json.dumps({"pk_field": "gid", "pk_value": 42}), "pk:gid=42"),
    ("ceci n'est pas du json", None),
    (None, None),
]


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _drop_wal(db_path: str) -> None:
    for suffix in ("-wal", "-shm"):
        side = db_path + suffix
        if os.path.exists(side):
            try:
                os.remove(side)
            except OSError:
                pass


def _seed_legacy(db_path: str, version: int, full_schema: bool) -> None:
    """Cree un journal a l'ANCIEN schema et force sa version."""
    from recoverland.core.sqlite_schema import initialize_schema

    conn = sqlite3.connect(db_path)
    try:
        if full_schema:
            # Base "future" : schema courant, numero de version futur.
            initialize_schema(conn)
        else:
            conn.execute(_V1_SCHEMA_VERSION)
            conn.execute(_V1_AUDIT_EVENT)
        cols = (
            "project_fingerprint, datasource_fingerprint, layer_id_snapshot, "
            "layer_name_snapshot, provider_type, feature_identity_json, "
            "operation_type, attributes_json, geometry_wkb, geometry_type, "
            "crs_authid, field_schema_json, user_name, session_id, "
            "created_at, restored_from_event_id"
        )
        sql = f"INSERT INTO audit_event ({cols}) VALUES ({','.join(['?'] * 16)})"  # nosec B608
        for idx, (identity, _expected) in enumerate(_LEGACY_ROWS):
            conn.execute(sql, (
                "tx_jml_project", _DS_FP, "lyr_jml", "parcelles", "ogr",
                identity, "UPDATE",
                json.dumps({"changed_only": {"n": {"old": 1, "new": 2}}}),
                None, "NoGeometry", "EPSG:2154", None,
                "tester", "sess_jml",
                (_T0 + timedelta(seconds=idx)).isoformat(), None,
            ))
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (version, _T0.isoformat(), "fixture"),
        )
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    _drop_wal(db_path)


def _describe(db_path: str) -> dict:
    from recoverland.core.sqlite_schema import get_schema_version

    conn = sqlite3.connect(db_path)
    try:
        cols = [r[1] for r in conn.execute(
            "PRAGMA table_info(audit_event)").fetchall()]
        rows = conn.execute(
            "SELECT feature_identity_json, entity_fingerprint, created_at "
            "FROM audit_event ORDER BY event_id"
            if "entity_fingerprint" in cols else
            "SELECT feature_identity_json, NULL, created_at "
            "FROM audit_event ORDER BY event_id"
        ).fetchall()
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        return {
            "version": get_schema_version(conn),
            "columns": cols,
            "rows": [tuple(r) for r in rows],
            "tables": sorted(tables),
        }
    finally:
        conn.close()


def _migrate_case(ctx, tag: str, version: int, full_schema: bool) -> dict:
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.integrity import check_journal_integrity

    tmpdir = tempfile.mkdtemp(prefix=f"rl_tx_jml_{tag}_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    _seed_legacy(db_path, version, full_schema)
    before = _describe(db_path)

    conn = sqlite3.connect(db_path)
    error = ""
    try:
        initialize_schema(conn)
        # Deuxieme ouverture : la migration doit etre idempotente.
        initialize_schema(conn)
    except Exception as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}"
    finally:
        conn.commit()
        conn.close()

    after = _describe(db_path)
    integrity = check_journal_integrity(db_path, trace_id=ctx.trace_id)
    res = {
        "tag": tag,
        "before": before,
        "after": after,
        "error": error,
        "issues": list(integrity.issues),
        "healthy": integrity.is_healthy,
    }
    flog(
        f"tx_journal_migration_locks case={tag}: trace_id={ctx.trace_id} "
        f"version {before['version']}->{after['version']} "
        f"rows {len(before['rows'])}->{len(after['rows'])} "
        f"error={error!r} issues={res['issues']}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


_N_CORRUPT_EVENTS = 800


def _expected_attrs(i: int) -> str:
    return json.dumps({"changed_only": {"n": {"old": i, "new": i + 1}}})


def _diff_offsets(before: bytes, after: bytes) -> list:
    """Plages [debut, fin) ou les deux images du fichier different."""
    spans = []
    start = None
    for i in range(min(len(before), len(after))):
        if before[i] != after[i]:
            if start is None:
                start = i
        elif start is not None:
            spans.append([start, i])
            start = None
    if start is not None:
        spans.append([start, min(len(before), len(after))])
    return spans[:8]


def _corruption_case(ctx) -> dict:
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import (
        initialize_schema, AUDIT_EVENT_INSERT_SQL,
        AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.integrity import (
        check_journal_integrity, get_journal_health_report,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_jml_corrupt_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    conn = sqlite3.connect(db_path)
    initialize_schema(conn)
    sql = "".join((
        "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
        ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
    ))
    payload = b"\x01" + b"geom" * 60
    conn.executemany(sql, [(
        "tx_jml_project", _DS_FP, "lyr_jml", "parcelles", "ogr",
        json.dumps({"fid": i}), "UPDATE", _expected_attrs(i),
        payload, "Point", "EPSG:2154", None, "tester", "sess_jml",
        (_T0 + timedelta(seconds=i)).isoformat(), None, f"fid:{i}", 2,
        None, None,
    ) for i in range(_N_CORRUPT_EVENTS)])
    conn.commit()
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    _drop_wal(db_path)

    size_before = os.path.getsize(db_path)
    bytes_before = Path(db_path).read_bytes()
    # On vise les OCTETS REELS d'un enregistrement plutot qu'un numero de
    # page : l'espace libre d'une page ne prouverait rien. Le marqueur est
    # le contenu exact de l'evenement n 400.
    marker = _expected_attrs(400).encode("utf-8")
    offset = bytes_before.find(marker)
    with open(db_path, "r+b") as fh:
        fh.seek(offset)
        fh.write(bytes((i * 53 + 7) & 0xFF for i in range(600)))
    bytes_corrupted = Path(db_path).read_bytes()

    result = check_journal_integrity(db_path, trace_id=ctx.trace_id)
    _drop_wal(db_path)
    bytes_after = Path(db_path).read_bytes()

    # Relecture complete : combien d'evenements sont encore FIDELES ?
    readable = None
    intact = None
    read_error = ""
    try:
        c2 = sqlite3.connect(db_path)
        try:
            rows = c2.execute(
                "SELECT entity_fingerprint, attributes_json FROM audit_event"
            ).fetchall()
            readable = len(rows)
            intact = sum(
                1 for fp, attrs in rows
                if fp is not None and fp.startswith("fid:")
                and attrs == _expected_attrs(int(fp[4:]))
            )
        finally:
            c2.close()
    except sqlite3.Error as exc:
        read_error = f"{type(exc).__name__}: {exc}"

    health_error = ""
    try:
        health = get_journal_health_report(db_path).total_events
    except Exception as exc:  # noqa: BLE001
        health = None
        health_error = f"{type(exc).__name__}: {exc}"

    res = {
        "n_seeded": _N_CORRUPT_EVENTS,
        "size_before": size_before,
        "size_after": os.path.getsize(db_path),
        "sha_before": hashlib.sha256(bytes_corrupted).hexdigest(),
        "sha_after": hashlib.sha256(bytes_after).hexdigest(),
        "diff_spans": _diff_offsets(bytes_corrupted, bytes_after),
        "corruption_landed": bytes_before != bytes_corrupted and offset > 0,
        "corruption_offset": offset,
        "file_exists": os.path.isfile(db_path),
        "healthy": result.is_healthy,
        "issues": list(result.issues),
        "readable_rows": readable,
        "intact_rows": intact,
        "read_error": read_error,
        "health_total": health,
        "health_error": health_error,
    }
    flog(
        f"tx_journal_migration_locks case=corruption: trace_id={ctx.trace_id} "
        f"healthy={res['healthy']} issues={res['issues']} "
        f"size {size_before}->{res['size_after']} "
        f"sha_stable={res['sha_before'] == res['sha_after']} "
        f"diff_spans={res['diff_spans']} "
        f"readable={readable!r} intact={intact!r} read_error={read_error!r}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def _concurrent_writers_case(ctx) -> dict:
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import (
        initialize_schema, apply_pragmas, AUDIT_EVENT_INSERT_SQL,
        AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_jml_conc_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    boot = sqlite3.connect(db_path)
    initialize_schema(boot)
    boot.commit()
    boot.close()

    sql = "".join((
        "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
        ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
    ))

    def _row(tag, idx):
        return (
            "tx_jml_project", _DS_FP, "lyr_jml", tag, "ogr",
            json.dumps({"fid": idx}), "UPDATE",
            json.dumps({"changed_only": {"n": {"old": 0, "new": idx}}}),
            None, "NoGeometry", "EPSG:2154", None, "tester", "sess_jml",
            (_T0 + timedelta(seconds=idx)).isoformat(), None,
            f"fid:{idx}", 2, None, None,
        )

    a = sqlite3.connect(db_path, check_same_thread=False)
    apply_pragmas(a)
    a.isolation_level = None
    b = sqlite3.connect(db_path, check_same_thread=False)
    apply_pragmas(b)
    b.isolation_level = None

    # Instance A ouvre une transaction ecrivain et la garde.
    a.execute("BEGIN IMMEDIATE")
    a.execute(sql, _row("instance_A", 1))

    started = time.monotonic()
    blocked_error = ""
    try:
        b.execute("BEGIN IMMEDIATE")
        b.execute(sql, _row("instance_B", 2))
        b.execute("COMMIT")
    except sqlite3.Error as exc:
        blocked_error = f"{type(exc).__name__}: {exc}"
    waited = time.monotonic() - started

    a.execute("COMMIT")

    # Deuxieme tentative de B, verrou libere : l'evenement doit passer.
    retry_error = ""
    try:
        b.execute("BEGIN IMMEDIATE")
        b.execute(sql, _row("instance_B", 2))
        b.execute("COMMIT")
    except sqlite3.Error as exc:
        retry_error = f"{type(exc).__name__}: {exc}"

    names = [r[0] for r in a.execute(
        "SELECT layer_name_snapshot FROM audit_event "
        "ORDER BY event_id").fetchall()]
    busy = a.execute("PRAGMA busy_timeout").fetchone()[0]
    a.close()
    b.close()

    res = {
        "blocked_error": blocked_error,
        "waited_sec": round(waited, 2),
        "busy_timeout": busy,
        "retry_error": retry_error,
        "rows": names,
    }
    flog(
        f"tx_journal_migration_locks case=concurrent: trace_id={ctx.trace_id} "
        f"blocked={blocked_error!r} waited={res['waited_sec']}s "
        f"busy_timeout={busy} retry={retry_error!r} rows={names}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def _writer_lock_case(ctx) -> dict:
    from recoverland.core.journal_manager import (
        JournalManager, JournalLockError, _resolve_journal_path,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_jml_lock_")
    project_path = str(Path(tmpdir) / "projet.qgz")
    Path(project_path).write_text("<qgis/>", encoding="utf-8")
    journal_path = _resolve_journal_path(project_path, tmpdir, "")
    os.makedirs(os.path.dirname(journal_path), exist_ok=True)
    lock_path = journal_path + ".rlwriter"

    # Un VRAI processus tiers vivant joue la seconde instance de QGIS.
    other = subprocess.Popen(  # nosec B603: sys.executable, arguments fixes
        [sys.executable, "-c", "import time; time.sleep(120)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    Path(lock_path).write_text(
        f"{other.pid}|{socket.gethostname()}|{int(time.time())}\n",
        encoding="utf-8",
    )

    jm = JournalManager()
    refused = ""
    opened_anyway = False
    try:
        jm.open_for_project(project_path, tmpdir)
        opened_anyway = True
    except JournalLockError as exc:
        refused = str(exc)
    except Exception as exc:  # noqa: BLE001
        refused = f"AUTRE:{type(exc).__name__}: {exc}"
    finally:
        jm.close()
    journal_created_while_locked = os.path.isfile(journal_path)

    # Le tiers meurt : le verrou devient perime et doit etre recupere.
    other.terminate()
    try:
        other.wait(timeout=10)
    except Exception:  # noqa: BLE001
        other.kill()
        other.wait(timeout=10)

    jm2 = JournalManager()
    reclaim_error = ""
    reopened = False
    try:
        jm2.open_for_project(project_path, tmpdir)
        reopened = jm2.is_open
    except Exception as exc:  # noqa: BLE001
        reclaim_error = f"{type(exc).__name__}: {exc}"
    finally:
        jm2.close()

    res = {
        "dead_pid": other.pid,
        "refused": refused,
        "opened_anyway": opened_anyway,
        "journal_created_while_locked": journal_created_while_locked,
        "reopened": reopened,
        "reclaim_error": reclaim_error,
        "lock_removed_after_close": not os.path.isfile(lock_path),
    }
    from recoverland.core.logger import flog as _flog
    _flog(
        f"tx_journal_migration_locks case=writer_lock: trace_id={ctx.trace_id} "
        f"refused={refused!r} opened_anyway={opened_anyway} "
        f"reopened={reopened} reclaim_error={reclaim_error!r}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    flog(
        f"tx_journal_migration_locks setup: trace_id={ctx.trace_id} "
        f"python={sys.executable} host={socket.gethostname()}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_journal_migration_locks run start: trace_id={ctx.trace_id}", "INFO")

    ctx.data["v1"] = _migrate_case(ctx, "v1", version=1, full_schema=False)
    ctx.data["futur"] = _migrate_case(ctx, "futur", version=99, full_schema=True)
    ctx.data["corruption"] = _corruption_case(ctx)
    ctx.data["concurrent"] = _concurrent_writers_case(ctx)
    ctx.data["writer_lock"] = _writer_lock_case(ctx)

    flog(f"tx_journal_migration_locks run end: trace_id={ctx.trace_id}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION

    out = []

    # ===== A. Migration v1 -> courant ====================================
    v1 = ctx.data.get("v1") or {}
    before = v1.get("before") or {}
    after = v1.get("after") or {}
    out.append((
        "v1_migration_sans_erreur",
        not v1.get("error"),
        f"ouverture d'un journal v1 : erreur={v1.get('error')!r}, attendu "
        f"aucune. Si la migration leve, l'utilisateur qui met le plugin a "
        f"jour perd l'acces a tout son historique.",
    ))
    out.append((
        "v1_aucune_ligne_perdue",
        len(after.get("rows") or []) == len(_LEGACY_ROWS),
        f"lignes avant={len(before.get('rows') or [])} "
        f"apres={len(after.get('rows') or [])} attendu={len(_LEGACY_ROWS)} : "
        f"une migration ne doit jamais consommer d'evenements",
    ))
    out.append((
        "v1_version_a_jour",
        after.get("version") == CURRENT_SCHEMA_VERSION,
        f"schema_version apres migration={after.get('version')} "
        f"attendu={CURRENT_SCHEMA_VERSION}",
    ))
    for col in ("entity_fingerprint", "event_schema_version",
                "new_geometry_wkb", "invalidated_at"):
        out.append((
            f"v1_colonne_{col}_ajoutee",
            col in (after.get("columns") or []),
            f"colonne '{col}' presente apres migration="
            f"{col in (after.get('columns') or [])} : sans elle, toute "
            f"ecriture d'evenement echoue sur le journal migre",
        ))
    out.append((
        "v1_table_alias_creee",
        "datasource_alias" in (after.get("tables") or []),
        f"tables apres migration={after.get('tables')} : sans "
        f"datasource_alias, le garde-fou de collision d'empreinte ne peut "
        f"plus rien enregistrer",
    ))
    got_fp = [row[1] for row in (after.get("rows") or [])]
    want_fp = [expected for _identity, expected in _LEGACY_ROWS]
    out.append((
        "v1_entity_fingerprint_retro_rempli",
        got_fp == want_fp,
        f"entity_fingerprint apres migration={got_fp!r} attendu={want_fp!r} : "
        f"c'est la cle sur laquelle REWIND regroupe les evenements d'une "
        f"entite. Laissee a NULL, toute la partie ancienne du journal reste "
        f"visible dans la recherche mais devient irrejouable.",
    ))
    out.append((
        "v1_horodatages_intacts",
        [row[2] for row in (after.get("rows") or [])]
        == [row[2] for row in (before.get("rows") or [])],
        f"created_at avant={[r[2] for r in (before.get('rows') or [])]} "
        f"apres={[r[2] for r in (after.get('rows') or [])]} : un evenement "
        f"re-horodate se retrouve du mauvais cote de la date de rewind",
    ))

    # ===== A bis. Journal ecrit par une version FUTURE ===================
    futur = ctx.data.get("futur") or {}
    f_before = futur.get("before") or {}
    f_after = futur.get("after") or {}
    out.append((
        "futur_aucune_ligne_perdue",
        len(f_after.get("rows") or []) == len(f_before.get("rows") or [])
        == len(_LEGACY_ROWS),
        f"journal marque schema_version=99 : lignes "
        f"{len(f_before.get('rows') or [])} -> {len(f_after.get('rows') or [])}. "
        f"Ouvrir un journal ecrit par une version plus recente ne doit rien "
        f"detruire.",
    ))
    out.append((
        "futur_signale_a_l_utilisateur",
        any("newer" in i.lower() or "version" in i.lower()
            for i in futur.get("issues") or []),
        f"issues={futur.get('issues')} : un journal plus recent que le plugin "
        f"doit etre signale, sinon l'utilisateur ecrit dedans avec un code qui "
        f"en ignore les colonnes",
    ))

    # ===== B. Corruption au niveau octet =================================
    cor = ctx.data.get("corruption") or {}
    n_seeded = cor.get("n_seeded")
    out.append((
        "corruption_effectivement_ecrite",
        cor.get("corruption_landed") is True,
        f"ecrasement de 600 octets a l'offset {cor.get('corruption_offset')} "
        f"(octets reels d'un enregistrement) applique="
        f"{cor.get('corruption_landed')} : sans cela la fixture ne teste rien",
    ))
    out.append((
        "corruption_lignes_alterees",
        cor.get("intact_rows") != n_seeded or cor.get("readable_rows") != n_seeded,
        f"apres ecrasement : lues={cor.get('readable_rows')!r} "
        f"fideles={cor.get('intact_rows')!r} sur {n_seeded} semees "
        f"(err={cor.get('read_error')!r}). Si tout etait reste intact, "
        f"l'ecrasement serait tombe dans une zone libre et le cas ne "
        f"prouverait rien.",
    ))
    out.append((
        "corruption_detectee",
        cor.get("healthy") is False and bool(cor.get("issues")),
        f"journal dont des enregistrements ont ete ecrases : "
        f"is_healthy={cor.get('healthy')} issues={cor.get('issues')}. Une "
        f"corruption non signalee laisse l'utilisateur continuer a ecrire "
        f"dans une base qui ne se relira plus, et croire son historique "
        f"fiable jusqu'au REWIND qui echouera.",
    ))
    out.append((
        "corruption_nommee_explicitement",
        any("integrity" in i.lower() or "malformed" in i.lower()
            for i in cor.get("issues") or []),
        f"issues={cor.get('issues')} : le diagnostic doit nommer la "
        f"corruption, pas seulement echouer sur un effet de bord, sinon le "
        f"message affiche n'oriente vers aucune action",
    ))
    out.append((
        "corruption_fichier_non_supprime",
        cor.get("file_exists") is True,
        f"journal corrompu encore present apres diagnostic="
        f"{cor.get('file_exists')} : meme abime, c'est la seule copie de "
        f"l'historique de l'utilisateur",
    ))
    out.append((
        "corruption_fichier_non_reecrit",
        cor.get("sha_before") == cor.get("sha_after")
        and cor.get("size_before") == cor.get("size_after"),
        f"fichier inchange par le diagnostic : taille "
        f"{cor.get('size_before')} -> {cor.get('size_after')}, plages d'octets "
        f"modifiees={cor.get('diff_spans')} (attendu aucune). Un diagnostic "
        f"doit etre en lecture seule : reecrire un fichier deja endommage "
        f"reduit les chances d'une recuperation par un outil tiers.",
    ))

    # ===== C1. Deux ecrivains SQLite =====================================
    conc = ctx.data.get("concurrent") or {}
    out.append((
        "concurrence_verrou_signale",
        "locked" in (conc.get("blocked_error") or "").lower()
        or "busy" in (conc.get("blocked_error") or "").lower(),
        f"le second ecrivain a obtenu {conc.get('blocked_error')!r} : "
        f"l'erreur doit etre un verrou explicite, jamais un succes silencieux",
    ))
    out.append((
        "concurrence_busy_timeout_applique",
        conc.get("busy_timeout") == 5000 and (conc.get("waited_sec") or 0) >= 4.0,
        f"busy_timeout={conc.get('busy_timeout')} attente reelle="
        f"{conc.get('waited_sec')}s : sans attente effective, la moindre "
        f"seconde de concurrence renvoie l'evenement au fichier d'attente",
    ))
    out.append((
        "concurrence_aucun_evenement_perdu",
        sorted(conc.get("rows") or []) == ["instance_A", "instance_B"],
        f"lignes en base={conc.get('rows')} attendu "
        f"['instance_A', 'instance_B'] : l'evenement du perdant doit etre "
        f"retarde, pas perdu (retry_error={conc.get('retry_error')!r})",
    ))

    # ===== C2. Verrou multi-instances ====================================
    lock = ctx.data.get("writer_lock") or {}
    out.append((
        "verrou_instance_vivante_refuse",
        bool(lock.get("refused")) and lock.get("opened_anyway") is False,
        f"un processus tiers VIVANT detenait le verrou : ouverture refusee="
        f"{lock.get('refused')!r} ouverte_quand_meme={lock.get('opened_anyway')}. "
        f"Deux QGIS ecrivant le meme journal entrelacent leurs event_id et un "
        f"REWIND compense alors les evenements de l'autre session.",
    ))
    out.append((
        "verrou_refus_avant_toute_creation",
        lock.get("journal_created_while_locked") is False,
        f"fichier journal cree pendant le refus="
        f"{lock.get('journal_created_while_locked')} : un refus doit rester "
        f"sans effet de bord, sinon la seconde instance laisse derriere elle "
        f"une base vide qui sera prise pour le journal",
    ))
    out.append((
        "verrou_perime_recupere",
        lock.get("reopened") is True and not lock.get("reclaim_error"),
        f"apres la mort du processus tiers (pid={lock.get('dead_pid')}), "
        f"reouverture={lock.get('reopened')} erreur={lock.get('reclaim_error')!r} : "
        f"un QGIS qui a plante ne doit pas condamner definitivement le journal",
    ))
    out.append((
        "verrou_libere_a_la_fermeture",
        lock.get("lock_removed_after_close") is True,
        f"fichier .rlwriter supprime apres close()="
        f"{lock.get('lock_removed_after_close')} : s'il subsiste, la session "
        f"suivante devra passer par la recuperation de verrou perime, ce qui "
        f"neutralise la protection multi-instances",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"JournalManager: stale writer lock reclaimed",
        name="verrou_perime_trace",
        min_count=1,
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_migration_locks.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
