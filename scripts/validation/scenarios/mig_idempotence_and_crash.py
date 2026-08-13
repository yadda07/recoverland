"""mig_idempotence_and_crash - RecoverLand validation runtime.

Invariant: I-JRN (le journal se migre ou ne se migre pas, jamais a
moitie) + I-11.

CE QUE CE SCENARIO PROUVE
-------------------------
La migration v6 tourne A CHAQUE OUVERTURE d'un journal, et il y a un
journal par projet. Elle sera donc rejouee des dizaines de fois sur le
meme fichier, et elle sera interrompue : QGIS ferme brutalement, machine
qui s'eteint, partage reseau qui disparait. Quatre exigences, quatre cas.

A. IDEMPOTENCE
   Trois ouvertures successives du meme journal. Les lignes d'alias
   doivent etre rigoureusement les memes -- y compris leur `created_at`,
   qui prouve qu'elles n'ont pas ete reecrites par un INSERT OR REPLACE
   silencieux -- et le marqueur de reconciliation ne doit pas etre
   retouche. Une quatrieme passe est forcee (`force=True`) pour verifier
   que meme rejouee de force, la reconciliation compte les alias
   existants au lieu d'en creer de nouveaux.

B. COUPURE PENDANT L'ECRITURE DES ALIAS
   `_write_alias_row` leve au deuxieme alias (disque plein, E/S). Trois
   exigences : l'ouverture ne doit PAS lever, le journal doit rester
   coherent, et le marqueur doit enregistrer l'ECHEC -- distinct du
   numero de version -- pour que la passe soit retentee. La relance
   doit terminer le travail sans doublon.

C. COUPURE DE LA TRANSACTION DE SCHEMA
   La reconciliation est censee ecrire DANS la transaction de
   `initialize_schema`. On modelise l'abandon de cette transaction :
   reconciliation puis exception avant le commit. Exigence : zero alias,
   zero marqueur, journal identique a l'etat d'avant. C'est la
   difference entre "tout ou rien" et "a moitie migre".

D. VRAIE COUPURE DE COURANT
   Un PROCESSUS REEL ouvre le journal, demarre la migration dans une
   transaction, signale qu'il y est, et est tue (TerminateProcess) avant
   le commit. Exigences : SQLite rejoue le rollback a l'ouverture
   suivante, aucun alias fantome ne subsiste, aucune ligne d'historique
   n'est perdue, et la migration suivante fait le travail complet.

Le critere commun aux quatre : `audit_event` doit etre bit pour bit
identique du debut a la fin, et `PRAGMA integrity_check` doit rendre
'ok'.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess  # nosec B404: lance un python local avec des arguments fixes
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_idempotence_and_crash"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PROJECT_FP = "mig_ic_project"
_T0 = datetime(2026, 4, 6, 10, 0, 0, tzinfo=timezone.utc)

# Two obsolete forms of one layer (a display filter, an opening option)
# plus the canonical form: the migration must write exactly two aliases.
_STORY = [
    (0, "bare", "UPDATE", "fid:1"),
    (60, "filtered", "UPDATE", "fid:1"),
    (120, "filtered", "UPDATE", "fid:2"),
    (180, "geomtype", "DELETE", "fid:3"),
    (240, "bare", "UPDATE", "fid:4"),
    (300, "geomtype", "INSERT", "fid:5"),
]

_CHILD = '''"""Child process: starts the v6 reconciliation, then dies mid-transaction."""
import sqlite3
import sys
import time

db_path, flag_path, root, parent = sys.argv[1:5]
sys.path.insert(0, root)
sys.path.insert(0, parent)

from recoverland.core.datasource_alias import reconcile_legacy_fingerprints

conn = sqlite3.connect(db_path)
conn.isolation_level = None
conn.execute("BEGIN IMMEDIATE")
reconcile_legacy_fingerprints(conn, force=True)
with open(flag_path, "w", encoding="utf-8") as fh:
    fh.write("in_transaction")
# Never commits: the parent kills this process right here.
time.sleep(300)
'''


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

def _fingerprints(gpkg: str) -> dict:
    base = os.path.normcase(os.path.abspath(gpkg)).replace("\\", "/")
    return {
        "bare": f"ogr::{base}|layername=parcelles",
        "filtered": f"ogr::{base}|layername=parcelles|subset=\"zone\" = 2",
        "geomtype": f"ogr::{base}|layername=parcelles|geometrytype=Polygon",
    }


def _sources(gpkg: str) -> dict:
    return {
        "bare": f"{gpkg}|layername=parcelles",
        "filtered": f"{gpkg}|layername=parcelles|subset=\"zone\" = 2",
        "geomtype": f"{gpkg}|layername=parcelles|geometrytype=Polygon",
    }


def _new_journal() -> tuple:
    """A fresh schema-v5 journal on disk with legacy fingerprints."""
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_mig_ic_")
    gpkg = str(Path(tmpdir) / "cadastre.gpkg")
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    fps = _fingerprints(gpkg)
    sources = _sources(gpkg)

    conn = sqlite3.connect(db_path)
    try:
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = "".join((
            "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
            ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
        ))
        for idx, (offset, key, operation, entity) in enumerate(_STORY):
            attrs = (json.dumps({"changed_only": {"n": {"old": idx, "new": idx + 1}}})
                     if operation == "UPDATE"
                     else json.dumps({"all_attributes": {"n": idx}}))
            conn.execute(insert, (
                _PROJECT_FP, fps[key], "lyr_ic", "parcelles", "ogr",
                json.dumps({"fid": int(entity.split(":")[1])}),
                operation, attrs, b"\x01wkb", "Polygon", "EPSG:2154", None,
                "tester", "sess_mig_ic",
                (_T0 + timedelta(seconds=offset)).isoformat(), None,
                entity, 2, None, None,
            ))
        now = _T0.isoformat()
        for key, fingerprint in fps.items():
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (fingerprint, "ogr", sources[key], "parcelles", "", "EPSG:2154",
                 "Polygon", now),
            )
        conn.execute(
            "INSERT OR REPLACE INTO schema_version "
            "(version_number, applied_at, description) VALUES (?, ?, ?)",
            (5, now, "fixture v5"),
        )
        conn.commit()
    finally:
        conn.close()
    return tmpdir, db_path, fps


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


def _state(db_path: str) -> dict:
    """Everything the migration is allowed to have changed, plus the proof
    that it changed nothing else."""
    from recoverland.core.datasource_alias import (
        list_aliases, load_reconciliation, RECONCILE_SETTING_KEY,
    )
    from recoverland.core.sqlite_schema import get_schema_version

    conn = sqlite3.connect(db_path)
    try:
        aliases = sorted(list_aliases(conn))
        report = load_reconciliation(conn)
        row = conn.execute(
            "SELECT setting_value, updated_at FROM backend_settings "
            "WHERE setting_key = ?", (RECONCILE_SETTING_KEY,)).fetchone()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        version = get_schema_version(conn)
        n_events = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
    finally:
        conn.close()
    return {
        "aliases": aliases,
        "marker": (row[0] if row else None, row[1] if row else None),
        "integrity": integrity,
        "version": version,
        "n_events": n_events,
        "dump": _dump_events(db_path),
        "status": None if report is None else report.status,
        "created": None if report is None else report.aliases_created,
        "already": None if report is None else report.already_aliased,
    }


def _open(db_path: str) -> str:
    """Open the journal the way the plugin does. Returns the error, if any."""
    from recoverland.core.sqlite_schema import initialize_schema

    conn = sqlite3.connect(db_path)
    try:
        initialize_schema(conn)
        return ""
    except Exception as exc:  # noqa: BLE001 - the error IS the measurement
        return f"{type(exc).__name__}: {exc}"
    finally:
        conn.commit()
        conn.close()


# ---------------------------------------------------------------------------
# A. Idempotence
# ---------------------------------------------------------------------------

def _case_idempotence(ctx) -> dict:
    from recoverland.core.datasource_alias import reconcile_legacy_fingerprints
    from recoverland.core.logger import flog

    tmpdir, db_path, _fps = _new_journal()
    before = _dump_events(db_path)
    opens = []
    errors = []
    for _ in range(3):
        errors.append(_open(db_path))
        opens.append(_state(db_path))

    # Fourth pass, forced: even asked explicitly, it must not duplicate.
    conn = sqlite3.connect(db_path)
    try:
        forced = reconcile_legacy_fingerprints(conn, force=True)
        conn.commit()
        forced_summary = {
            "status": forced.status, "created": forced.aliases_created,
            "already": forced.already_aliased,
            "canonical": forced.already_canonical,
        }
    finally:
        conn.close()
    after_forced = _state(db_path)

    res = {
        "errors": errors,
        "opens": opens,
        "forced": forced_summary,
        "after_forced": after_forced,
        "before_dump": before,
    }
    flog(
        f"mig_idempotence_and_crash case=idempotence: trace_id={ctx.trace_id} "
        f"errors={errors} aliases={[len(o['aliases']) for o in opens]} "
        f"forced={forced_summary}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# B. Crash while writing the aliases
# ---------------------------------------------------------------------------

def _case_write_crash(ctx) -> dict:
    import recoverland.core.datasource_alias as da
    from recoverland.core.logger import flog

    tmpdir, db_path, _fps = _new_journal()
    before = _dump_events(db_path)

    calls = {"n": 0}
    original = da._write_alias_row

    def _failing(conn, alias_fp, target_fp, note):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise sqlite3.OperationalError("database or disk is full (simule)")
        return original(conn, alias_fp, target_fp, note)

    da._write_alias_row = _failing
    try:
        interrupted_error = _open(db_path)
    finally:
        da._write_alias_row = original
    interrupted = _state(db_path)

    resumed_error = _open(db_path)
    resumed = _state(db_path)

    res = {
        "n_write_calls": calls["n"],
        "interrupted_error": interrupted_error,
        "interrupted": interrupted,
        "resumed_error": resumed_error,
        "resumed": resumed,
        "before_dump": before,
    }
    flog(
        f"mig_idempotence_and_crash case=write_crash: trace_id={ctx.trace_id} "
        f"interrupted_error={interrupted_error!r} "
        f"status={interrupted['status']!r} aliases={len(interrupted['aliases'])} "
        f"-> resumed status={resumed['status']!r} "
        f"aliases={len(resumed['aliases'])}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# C. The schema transaction is abandoned before commit
# ---------------------------------------------------------------------------

class _PowerCut(RuntimeError):
    """Marks the simulated abort so nothing else is swallowed."""


def _case_transaction_cut(ctx) -> dict:
    from recoverland.core.datasource_alias import reconcile_legacy_fingerprints
    from recoverland.core.logger import flog

    tmpdir, db_path, _fps = _new_journal()
    before = _dump_events(db_path)

    conn = sqlite3.connect(db_path)
    cut = ""
    try:
        # Models `initialize_schema`'s `with conn:` block dying after the
        # reconciliation wrote its rows but before the commit.
        with conn:
            reconcile_legacy_fingerprints(conn, force=True)
            raise _PowerCut("transaction abandonnee")
    except _PowerCut as exc:
        cut = str(exc)
    finally:
        conn.close()

    after_cut = _state(db_path)
    reopen_error = _open(db_path)
    after_reopen = _state(db_path)

    res = {
        "cut": cut,
        "after_cut": after_cut,
        "reopen_error": reopen_error,
        "after_reopen": after_reopen,
        "before_dump": before,
    }
    flog(
        f"mig_idempotence_and_crash case=transaction_cut: trace_id={ctx.trace_id} "
        f"aliases_apres_coupure={len(after_cut['aliases'])} "
        f"marqueur={after_cut['marker'][0] is not None} "
        f"aliases_apres_relance={len(after_reopen['aliases'])}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


# ---------------------------------------------------------------------------
# D. A real process killed in the middle of the migration
# ---------------------------------------------------------------------------

def _case_killed_process(ctx) -> dict:
    from recoverland.core.logger import flog

    tmpdir, db_path, _fps = _new_journal()
    before = _dump_events(db_path)
    child_path = str(Path(tmpdir) / "child_migration.py")
    flag_path = str(Path(tmpdir) / "in_transaction.flag")
    Path(child_path).write_text(_CHILD, encoding="utf-8")

    proc = subprocess.Popen(  # nosec B603: sys.executable, arguments fixes
        [sys.executable, child_path, db_path, flag_path,
         str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    ready = False
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if os.path.isfile(flag_path):
            ready = True
            break
        if proc.poll() is not None:
            break
        time.sleep(0.2)

    child_error = ""
    if not ready and proc.poll() is not None:
        _out, err = proc.communicate(timeout=10)
        child_error = (err or b"").decode("utf-8", "replace")[-400:]
    proc.kill()
    try:
        proc.wait(timeout=20)
    except Exception:  # noqa: BLE001
        pass

    after_kill = _state(db_path)
    reopen_error = _open(db_path)
    after_reopen = _state(db_path)

    res = {
        "ready": ready,
        "child_error": child_error,
        "after_kill": after_kill,
        "reopen_error": reopen_error,
        "after_reopen": after_reopen,
        "before_dump": before,
    }
    flog(
        f"mig_idempotence_and_crash case=killed_process: trace_id={ctx.trace_id} "
        f"ready={ready} child_error={child_error!r} "
        f"aliases_apres_kill={len(after_kill['aliases'])} "
        f"lignes={after_kill['n_events']} integrite={after_kill['integrity']!r} "
        f"aliases_apres_relance={len(after_reopen['aliases'])}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    flog(
        f"mig_idempotence_and_crash setup: trace_id={ctx.trace_id} "
        f"python={sys.executable} n_events_par_journal={len(_STORY)}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"mig_idempotence_and_crash run start: trace_id={ctx.trace_id}", "INFO")
    ctx.data["idem"] = _case_idempotence(ctx)
    ctx.data["write_crash"] = _case_write_crash(ctx)
    ctx.data["tx_cut"] = _case_transaction_cut(ctx)
    ctx.data["killed"] = _case_killed_process(ctx)
    flog(f"mig_idempotence_and_crash run end: trace_id={ctx.trace_id}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    from recoverland.core.sqlite_schema import CURRENT_SCHEMA_VERSION

    out = []

    # ===== A. Idempotence ================================================
    idem = ctx.data.get("idem") or {}
    opens = idem.get("opens") or []
    out.append((
        "trois_ouvertures_sans_erreur",
        all(not e for e in idem.get("errors") or ["?"]),
        f"erreurs des trois ouvertures={idem.get('errors')}",
    ))
    out.append((
        "alias_identiques_a_chaque_ouverture",
        len(opens) == 3 and opens[0]["aliases"] == opens[1]["aliases"] == opens[2]["aliases"],
        f"lignes d'alias par ouverture={[len(o['aliases']) for o in opens]} ; "
        f"created_at par ouverture={[[a[2] for a in o['aliases']] for o in opens]} : "
        f"identiques horodatage compris, sinon un INSERT OR REPLACE reecrit "
        f"les alias a chaque demarrage",
    ))
    out.append((
        "deux_alias_et_pas_un_de_plus",
        bool(opens) and len(opens[-1]["aliases"]) == 2,
        f"alias apres trois ouvertures={len(opens[-1]['aliases']) if opens else None} "
        f"attendu 2 (la forme filtree et l'ouverture geometrytype)",
    ))
    out.append((
        "marqueur_non_reecrit_a_chaque_ouverture",
        len(opens) == 3 and opens[0]["marker"] == opens[2]["marker"],
        f"marqueur de reconciliation ouverture1={opens[0]['marker'][1] if opens else None} "
        f"ouverture3={opens[2]['marker'][1] if len(opens) > 2 else None} : une "
        f"passe deja faite ne doit rien ecrire du tout",
    ))
    out.append((
        "passe_forcee_ne_cree_aucun_doublon",
        (idem.get("forced") or {}).get("created") == 0
        and (idem.get("forced") or {}).get("already") == 2,
        f"passe forcee={idem.get('forced')} attendu created=0 already=2 : "
        f"un alias existant se compte, il ne se reecrit pas",
    ))
    out.append((
        "passe_forcee_ne_touche_pas_les_alias_existants",
        bool(opens) and (idem.get("after_forced") or {}).get("aliases") == opens[-1]["aliases"],
        f"alias apres passe forcee={[(a[0][-20:], a[2]) for a in (idem.get('after_forced') or {}).get('aliases') or []]} "
        f"vs avant={[(a[0][-20:], a[2]) for a in (opens[-1]['aliases'] if opens else [])]}",
    ))
    out.append((
        "idempotence_audit_event_intact",
        bool(opens) and idem.get("before_dump") == opens[-1]["dump"]
        == (idem.get("after_forced") or {}).get("dump"),
        f"audit_event sha avant={_digest(idem.get('before_dump') or [])} "
        f"apres 3 ouvertures + passe forcee="
        f"{_digest((idem.get('after_forced') or {}).get('dump') or [])}",
    ))

    # ===== B. Coupure pendant l'ecriture des alias ========================
    wc = ctx.data.get("write_crash") or {}
    interrupted = wc.get("interrupted") or {}
    resumed = wc.get("resumed") or {}
    out.append((
        "coupure_ecriture_ne_bloque_pas_l_ouverture",
        not wc.get("interrupted_error"),
        f"erreur remontee a l'ouverture={wc.get('interrupted_error')!r} : une "
        f"reconciliation qui echoue ne doit JAMAIS empecher le plugin de "
        f"s'ouvrir, sinon un disque plein rend l'historique inaccessible",
    ))
    out.append((
        "coupure_ecriture_journal_coherent",
        interrupted.get("integrity") == "ok"
        and interrupted.get("n_events") == len(_STORY)
        and interrupted.get("dump") == wc.get("before_dump"),
        f"apres coupure : integrity_check={interrupted.get('integrity')!r} "
        f"lignes={interrupted.get('n_events')} attendu={len(_STORY)} "
        f"audit_event inchange={interrupted.get('dump') == wc.get('before_dump')}",
    ))
    out.append((
        "coupure_ecriture_marquee_comme_echouee",
        interrupted.get("status") == "failed",
        f"statut enregistre={interrupted.get('status')!r} attendu 'failed' : "
        f"le marqueur est distinct du numero de version, c'est ce qui permet "
        f"de retenter la passe a l'ouverture suivante au lieu de la croire "
        f"faite",
    ))
    out.append((
        "coupure_ecriture_version_quand_meme_a_jour",
        interrupted.get("version") == CURRENT_SCHEMA_VERSION,
        f"schema_version={interrupted.get('version')} attendu="
        f"{CURRENT_SCHEMA_VERSION} : le schema lui-meme est bien migre, seule "
        f"la reconciliation reste a reprendre",
    ))
    out.append((
        "relance_termine_le_travail",
        not wc.get("resumed_error") and len(resumed.get("aliases") or []) == 2
        and resumed.get("status") == "ok",
        f"apres relance : erreur={wc.get('resumed_error')!r} "
        f"alias={len(resumed.get('aliases') or [])} attendu 2 "
        f"statut={resumed.get('status')!r} attendu 'ok'",
    ))
    out.append((
        "relance_ne_duplique_pas_l_alias_deja_ecrit",
        len({a[0] for a in resumed.get("aliases") or []}) == len(resumed.get("aliases") or []),
        f"alias apres relance={[a[0][-25:] for a in resumed.get('aliases') or []]}",
    ))
    out.append((
        "relance_audit_event_intact",
        resumed.get("dump") == wc.get("before_dump"),
        f"audit_event sha avant={_digest(wc.get('before_dump') or [])} "
        f"apres reprise={_digest(resumed.get('dump') or [])}",
    ))

    # ===== C. Transaction abandonnee =====================================
    cut = ctx.data.get("tx_cut") or {}
    after_cut = cut.get("after_cut") or {}
    after_reopen = cut.get("after_reopen") or {}
    out.append((
        "transaction_abandonnee_ne_laisse_aucun_alias",
        after_cut.get("aliases") == [],
        f"alias apres abandon de la transaction="
        f"{[a[0][-25:] for a in after_cut.get('aliases') or []]} attendu aucun : "
        f"la reconciliation doit vivre dans la transaction de schema, pas a "
        f"cote, sinon un journal peut rester a moitie migre",
    ))
    out.append((
        "transaction_abandonnee_ne_laisse_aucun_marqueur",
        (after_cut.get("marker") or (None,))[0] is None,
        f"marqueur apres abandon={(after_cut.get('marker') or (None,))[0] is not None} "
        f"attendu absent : un marqueur commite sans ses alias ferait croire "
        f"le travail fait",
    ))
    out.append((
        "transaction_abandonnee_journal_intact",
        after_cut.get("dump") == cut.get("before_dump")
        and after_cut.get("integrity") == "ok",
        f"audit_event inchange={after_cut.get('dump') == cut.get('before_dump')} "
        f"integrity_check={after_cut.get('integrity')!r}",
    ))
    out.append((
        "relance_apres_abandon_migre_completement",
        not cut.get("reopen_error") and len(after_reopen.get("aliases") or []) == 2
        and after_reopen.get("status") == "ok",
        f"apres relance : erreur={cut.get('reopen_error')!r} "
        f"alias={len(after_reopen.get('aliases') or [])} attendu 2 "
        f"statut={after_reopen.get('status')!r}",
    ))

    # ===== D. Processus tue en pleine migration ==========================
    killed = ctx.data.get("killed") or {}
    ak = killed.get("after_kill") or {}
    ar = killed.get("after_reopen") or {}
    out.append((
        "processus_tue_pendant_la_transaction",
        killed.get("ready") is True,
        f"le processus enfant a-t-il atteint la transaction avant d'etre tue ="
        f"{killed.get('ready')} (erreur enfant={killed.get('child_error')!r}) : "
        f"sans cela le cas ne prouve rien",
    ))
    out.append((
        "coupure_de_courant_aucun_alias_fantome",
        ak.get("aliases") == [],
        f"alias survivants apres kill={[a[0][-25:] for a in ak.get('aliases') or []]} "
        f"attendu aucun : une transaction non commitee doit etre annulee par "
        f"SQLite a la reouverture",
    ))
    out.append((
        "coupure_de_courant_aucune_ligne_perdue",
        ak.get("n_events") == len(_STORY) and ak.get("dump") == killed.get("before_dump"),
        f"lignes apres kill={ak.get('n_events')} attendu={len(_STORY)} ; "
        f"audit_event inchange={ak.get('dump') == killed.get('before_dump')}",
    ))
    out.append((
        "coupure_de_courant_base_saine",
        ak.get("integrity") == "ok",
        f"PRAGMA integrity_check apres kill={ak.get('integrity')!r}",
    ))
    out.append((
        "relance_apres_coupure_de_courant_complete_la_migration",
        not killed.get("reopen_error") and len(ar.get("aliases") or []) == 2
        and ar.get("status") == "ok" and ar.get("version") == CURRENT_SCHEMA_VERSION,
        f"apres relance : erreur={killed.get('reopen_error')!r} "
        f"alias={len(ar.get('aliases') or [])} statut={ar.get('status')!r} "
        f"version={ar.get('version')}",
    ))
    out.append((
        "relance_apres_coupure_audit_event_intact",
        ar.get("dump") == killed.get("before_dump"),
        f"audit_event sha avant={_digest(killed.get('before_dump') or [])} "
        f"apres={_digest(ar.get('dump') or [])}",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"mig_idempotence_and_crash.*trace_id={ctx.trace_id}",
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
