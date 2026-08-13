"""tx_life_wq_stop_restart - RecoverLand validation runtime.

Invariant: I-WQ-LIFECYCLE (le cycle de vie de la file d'ecriture ne perd
ni ne deplace jamais un evenement).

Le mecanisme
------------
`WriteQueue` est le seul chemin entre une edition capturee et le journal
SQLite. Le producteur (thread UI) appelle `enqueue(events)` et lit la
valeur de retour comme un accuse de reception : `True` veut dire
"c'est enregistre, tu peux oublier". Le consommateur est un thread
unique (`RecoverLand-Writer`) demarre par `start(db_path)` et arrete par
`stop()`.

Le probleme est que `enqueue()` ne regarde jamais si ce consommateur
existe. Elle valide les evenements, les pousse dans un `queue.Queue`
Python et renvoie `True` — que le thread ecrivain soit demarre, arrete,
ou jamais lance. Le `queue.Queue` survit a l'arret du thread : il est
cree une seule fois dans `__init__` et n'est jamais vide par `stop()`.

Les deux scenarios d'echec concrets
-----------------------------------
1. Fermeture / changement de projet. `RecoverPlugin._shutdown_local_backend`
   fait `tracker.deactivate()` puis `write_queue.stop()`. Tout evenement
   qui arrive apres ce `stop()` (commit de couche en cours de fermeture,
   callback QGIS en retard) est accepte avec `True` et reste bloque en
   RAM. Personne ne le reecrit dans le fichier `recoverland_pending.json`
   prevu exactement pour ca. L'edition est perdue sans un seul message.

2. Enchainement projet A -> projet B. `_on_project_opened` appelle
   `_shutdown_local_backend()` (stop) puis `_init_local_backend()`
   (`start(journal_de_B)`). Le thread ecrivain qui redemarre draine la
   file *existante* : les evenements du projet A restes en file sont
   ecrits dans le journal du projet B, avec les empreintes de A. Le
   journal de B contient alors un historique qui ne lui appartient pas,
   et un REWIND sur B ira chercher des entites qui n'y ont jamais existe.

Ce que ce scenario torture (journal SQLite reel, aucun objet QGIS) :
    S1  flush a l'arret : 3000 evenements en file puis stop() immediat
    S2  double start() : un seul thread ecrivain, aucun doublon
    S3  stop() sans start(), stop() deux fois
    S4  enqueue() apres stop()          <- perte silencieuse
    S5  restart sur un AUTRE journal    <- contamination inter-projets
    S6  aucune fuite de thread apres le cycle complet
    S7  enqueue() sur une file jamais demarree  <- perte silencieuse
    S8  8 producteurs concurrents : atomicite par lot + journalisation
        concurrente encore parsable par scripts/validation/parse_log.py
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

SCENARIO_ID = "tx_life_wq_stop_restart"
INVARIANT = "I-WQ-LIFECYCLE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_WRITER_THREAD_NAME = "RecoverLand-Writer"
_PENDING_FILENAME = "recoverland_pending.json"


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

def _make_journal(root: str, name: str) -> str:
    """Cree un journal SQLite initialise dans son propre sous-dossier.

    Un dossier par journal : `_save_lost_events` ecrit le fichier pending
    a cote du .sqlite, donc deux journaux dans le meme dossier
    partageraient leur fichier de secours et fausseraient le comptage.
    """
    from recoverland.core.sqlite_schema import initialize_schema

    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.close()
    return path


def _event(idx: int, datasource: str = "ds_life", session: str = "s0"):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=None,
        project_fingerprint="tx_life_project",
        datasource_fingerprint=datasource,
        layer_id_snapshot="lyr_life",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json='{"fid": %d}' % idx,
        operation_type="UPDATE",
        attributes_json='{"changed_only": {"statut": {"old": "A", "new": "B"}}}',
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:4326",
        field_schema_json='[{"name": "statut", "type": "string"}]',
        user_name="tester",
        session_id=session,
        created_at="2026-08-13T10:00:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint="fid:%d" % idx,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
    finally:
        conn.close()


def _sessions(db_path: str) -> Counter:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT session_id, COUNT(*) FROM audit_event GROUP BY session_id"
        ).fetchall()
    finally:
        conn.close()
    return Counter({row[0]: int(row[1]) for row in rows})


def _writer_threads() -> int:
    return len([t for t in threading.enumerate() if t.name == _WRITER_THREAD_NAME])


def _pending_len(db_path: str) -> int:
    import json

    path = os.path.join(os.path.dirname(db_path), _PENDING_FILENAME)
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return -1
    return len(data) if isinstance(data, list) else -1


# --------------------------------------------------------------------- #
# Scenario
# --------------------------------------------------------------------- #

def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_life_wq_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["writers_before"] = _writer_threads()
    ctx.data["db"] = {
        name: _make_journal(tmpdir, name)
        for name in ("flush", "afterstop", "projectB", "double", "flood")
    }

    flog(
        f"tx_life_wq_stop_restart setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} writers_before={ctx.data['writers_before']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.logger import flog, flog_kv

    dbs = ctx.data["db"]
    flog(f"tx_life_wq_stop_restart run start: trace_id={ctx.trace_id}", "INFO")

    # ===== S1: flush a l'arret ==========================================
    # 3000 evenements poses d'un coup puis stop() sans laisser au thread
    # le temps de tourner : c'est la fermeture de QGIS juste apres un gros
    # commit. Le contrat de stop() dit "flushing remaining events".
    wq = WriteQueue()
    wq.start(dbs["flush"])
    accepted = wq.enqueue([_event(i, session="flush") for i in range(3000)])
    wq.stop()
    ctx.data["s1_accepted"] = accepted
    ctx.data["s1_written"] = _count(dbs["flush"])
    ctx.data["s1_pending"] = _pending_len(dbs["flush"])

    # ===== S2: double start() ===========================================
    # Deux initialisations rapprochees (projet rouvert deux fois de suite).
    wq = WriteQueue()
    wq.start(dbs["double"])
    wq.start(dbs["double"])
    ctx.data["s2_writer_threads"] = _writer_threads() - ctx.data["writers_before"]
    wq.enqueue([_event(i, session="double") for i in range(200)])
    wq.stop()
    ctx.data["s2_written"] = _count(dbs["double"])

    # ===== S3: stop() sans start(), stop() deux fois ====================
    idle = WriteQueue()
    try:
        idle.stop()
        idle.stop()
        ctx.data["s3_error"] = ""
    except Exception as exc:  # noqa: BLE001
        ctx.data["s3_error"] = repr(exc)

    # ===== S7: enqueue() sur une file jamais demarree ===================
    # Cas reel : l'ouverture du journal a echoue (verrou multi-instance,
    # dossier en lecture seule) donc start() n'a jamais ete appele, mais
    # le tracker, lui, continue de capturer et d'appeler enqueue().
    ctx.data["s7_valid_accepted"] = idle.enqueue([_event(1, session="never")])
    ctx.data["s7_queued"] = idle.pending_count
    bad = _event(2, session="never")._replace(operation_type="BOGUS")
    ctx.data["s7_invalid_accepted"] = idle.enqueue([bad])
    ctx.data["s7_db_path"] = idle._db_path

    # ===== S4: enqueue() apres stop() ===================================
    wq = WriteQueue()
    wq.start(dbs["afterstop"])
    wq.stop()
    ctx.data["s4_accepted"] = wq.enqueue(
        [_event(i, session="afterstop") for i in range(25)]
    )
    time.sleep(0.3)
    ctx.data["s4_written"] = _count(dbs["afterstop"])
    ctx.data["s4_still_queued"] = wq.pending_count
    ctx.data["s4_pending"] = _pending_len(dbs["afterstop"])

    # ===== S5: le meme objet redemarre sur le journal du projet B =======
    # Exactement la sequence de RecoverPlugin._on_project_opened.
    wq.start(dbs["projectB"])
    time.sleep(0.5)
    wq.stop()
    ctx.data["s5_projectA_rows"] = _count(dbs["afterstop"])
    ctx.data["s5_projectB_rows"] = _count(dbs["projectB"])
    ctx.data["s5_projectB_sessions"] = dict(_sessions(dbs["projectB"]))

    # ===== S8: 8 producteurs concurrents ================================
    # Chaque thread pousse 40 lots de 12 evenements, chaque lot marque par
    # son propre session_id. L'atomicite promise par enqueue() est
    # "tout le lot ou rien" : un session_id present avec 11 lignes serait
    # un lot coupe en deux.
    n_threads, n_batches, batch_size = 8, 40, 12
    wq = WriteQueue()
    wq.start(dbs["flood"])
    accepted_batches = []
    accepted_lock = threading.Lock()

    def _producer(tid: int) -> None:
        for b in range(n_batches):
            tag = "T%d-B%02d" % (tid, b)
            events = [
                _event(i, datasource="ds%d" % tid, session=tag)
                for i in range(batch_size)
            ]
            ok = wq.enqueue(events)
            flog_kv(
                "INFO", "TX_LIFE_PRODUCER", module="tx_life_wq_stop_restart",
                trace=ctx.trace_id, tag=tag, accepted=ok,
                note="lot pousse depuis un thread producteur",
            )
            with accepted_lock:
                accepted_batches.append((tag, ok))

    threads = [threading.Thread(target=_producer, args=(t,), name="tx_life_prod_%d" % t)
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wq.stop()

    accepted_tags = {tag for tag, ok in accepted_batches if ok}
    ctx.data["s8_accepted_batches"] = len(accepted_tags)
    ctx.data["s8_expected_batches"] = n_threads * n_batches
    ctx.data["s8_batch_size"] = batch_size
    ctx.data["s8_written"] = _count(dbs["flood"])
    sessions = _sessions(dbs["flood"])
    ctx.data["s8_sessions_written"] = len(sessions)
    ctx.data["s8_partial_batches"] = sorted(
        tag for tag, n in sessions.items() if n != batch_size
    )
    ctx.data["s8_missing_batches"] = sorted(accepted_tags - set(sessions))
    ctx.data["s8_ghost_batches"] = sorted(set(sessions) - accepted_tags)

    # ===== S6: fuite de thread ==========================================
    time.sleep(0.3)
    ctx.data["s6_writer_threads"] = _writer_threads()

    flog(
        f"tx_life_wq_stop_restart run end: trace_id={ctx.trace_id} "
        f"s1={ctx.data['s1_written']} s4_written={ctx.data['s4_written']} "
        f"s4_queued={ctx.data['s4_still_queued']} "
        f"s5_B={ctx.data['s5_projectB_rows']} s8={ctx.data['s8_written']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    d = ctx.data

    # ===== S1 : garantie acquise ========================================
    out.append((
        "s1_shutdown_flush_writes_everything",
        d.get("s1_accepted") is True and d.get("s1_written") == 3000,
        f"accepted={d.get('s1_accepted')} ecrits={d.get('s1_written')} attendu=3000 "
        f"pending={d.get('s1_pending')} : QGIS ferme juste apres un gros commit, "
        f"les 3000 modifications doivent etre dans le journal, pas en RAM",
    ))

    # ===== S2 / S3 : garanties acquises =================================
    out.append((
        "s2_double_start_single_writer",
        d.get("s2_writer_threads") == 1 and d.get("s2_written") == 200,
        f"threads_ecrivains={d.get('s2_writer_threads')} attendu=1 "
        f"lignes={d.get('s2_written')} attendu=200 : deux start() consecutifs ne "
        f"doivent pas dupliquer les evenements ni laisser deux ecrivains sur le "
        f"meme fichier",
    ))
    out.append((
        "s3_stop_without_start_is_harmless",
        d.get("s3_error") == "",
        f"stop() sans start() a leve {d.get('s3_error')!r} : la fermeture de QGIS "
        f"apres un echec d'ouverture du journal remonterait une erreur a "
        f"l'utilisateur",
    ))

    # ===== S4 : le trou ================================================
    out.append((
        "s4_enqueue_after_stop_is_refused",
        d.get("s4_accepted") is False,
        f"enqueue() apres stop() a renvoye {d.get('s4_accepted')} : l'appelant "
        f"(EditSessionTracker) lit cette valeur comme un accuse de reception et "
        f"oublie les evenements. Il n'y a plus de thread ecrivain, personne ne "
        f"les ecrira jamais.",
    ))
    out.append((
        "s4_no_event_left_in_ram",
        d.get("s4_still_queued") == 0,
        f"{d.get('s4_still_queued')} evenement(s) restent dans la file apres "
        f"stop() : ils ne sont ni dans le journal ({d.get('s4_written')} ligne(s)) "
        f"ni dans le fichier de secours. Les editions faites pendant la fermeture "
        f"du projet disparaissent sans aucun message.",
    ))
    out.append((
        "s4_rejected_batch_lands_in_pending",
        d.get("s4_pending") == 25,
        f"fichier pending={d.get('s4_pending')} evenement(s), attendu=25 : quand "
        f"la file ne peut plus ecrire, le lot doit basculer dans "
        f"recoverland_pending.json pour etre repris au prochain demarrage",
    ))

    # ===== S5 : contamination inter-projets =============================
    out.append((
        "s5_project_b_journal_stays_clean",
        d.get("s5_projectB_rows") == 0,
        f"le journal du projet B contient {d.get('s5_projectB_rows')} ligne(s) "
        f"venues du projet A (sessions={d.get('s5_projectB_sessions')}) alors que "
        f"le projet A n'en a que {d.get('s5_projectA_rows')}. Changer de projet "
        f"deverse l'historique du projet precedent dans le journal du nouveau, "
        f"avec les empreintes de l'ancien : un REWIND sur B ciblera des entites "
        f"qui n'ont jamais existe dans B.",
    ))

    # ===== S7 : file jamais demarree ====================================
    out.append((
        "s7_enqueue_without_start_is_refused",
        d.get("s7_valid_accepted") is False,
        f"enqueue() sur une file jamais demarree a renvoye "
        f"{d.get('s7_valid_accepted')} et garde {d.get('s7_queued')} evenement(s) "
        f"en RAM (db_path={d.get('s7_db_path')!r}). C'est le cas ou l'ouverture "
        f"du journal a echoue : le plugin affiche 'enregistrement desactive' mais "
        f"accepte quand meme les evenements et les jette.",
    ))
    out.append((
        "s7_invalid_batch_reports_the_drop",
        d.get("s7_invalid_accepted") is False,
        f"enqueue() d'un lot invalide sans db_path a renvoye "
        f"{d.get('s7_invalid_accepted')} : le refus est correct, mais "
        f"_save_lost_events sort en silence quand _db_path vaut None, donc le lot "
        f"n'est nulle part",
    ))

    # ===== S8 : concurrence =============================================
    out.append((
        "s8_all_accepted_batches_persisted",
        not d.get("s8_missing_batches"),
        f"lots acceptes mais absents du journal : {d.get('s8_missing_batches')} "
        f"(acceptes={d.get('s8_accepted_batches')}/"
        f"{d.get('s8_expected_batches')}, lignes={d.get('s8_written')}). Huit "
        f"utilisateurs de couches differentes editant en meme temps perdraient "
        f"ces lots.",
    ))
    out.append((
        "s8_no_partial_batch",
        not d.get("s8_partial_batches"),
        f"lots ecrits a moitie : {d.get('s8_partial_batches')} (chaque lot doit "
        f"faire exactement {d.get('s8_batch_size')} lignes). Un lot coupe laisse "
        f"une entite avec une partie seulement de ses modifications, donc un "
        f"REWIND partiel silencieux.",
    ))
    out.append((
        "s8_no_ghost_batch",
        not d.get("s8_ghost_batches"),
        f"lots presents dans le journal alors que enqueue() les a refuses : "
        f"{d.get('s8_ghost_batches')}",
    ))
    out.append((
        "s8_exact_row_count",
        d.get("s8_written") == d.get("s8_expected_batches", 0) * d.get("s8_batch_size", 0),
        f"lignes={d.get('s8_written')} attendu="
        f"{d.get('s8_expected_batches', 0) * d.get('s8_batch_size', 0)} : ni perte "
        f"ni doublon sous 8 producteurs concurrents",
    ))

    # ===== S6 : fuite de thread =========================================
    out.append((
        "s6_no_writer_thread_leak",
        d.get("s6_writer_threads") == d.get("writers_before"),
        f"threads '{_WRITER_THREAD_NAME}' vivants={d.get('s6_writer_threads')} "
        f"avant le scenario={d.get('writers_before')} : un ecrivain orphelin "
        f"garde une connexion ouverte sur un journal que l'utilisateur croit "
        f"ferme",
    ))

    # ===== Journalisation concurrente encore lisible ====================
    # 320 lignes TX_LIFE_PRODUCER emises depuis 8 threads en parallele : le
    # handler doit les ecrire entieres, et parse_log doit retrouver chaque
    # champ. Une ligne entrelacee casserait tout l'outillage de validation.
    producer_records = [
        r for r in ctx.records
        if "TX_LIFE_PRODUCER" in r.raw and r.fields.get("trace") == ctx.trace_id
    ]
    expected_lines = d.get("s8_expected_batches", 0)
    out.append((
        "log_concurrent_lines_all_present",
        len(producer_records) == expected_lines,
        f"lignes TX_LIFE_PRODUCER retrouvees={len(producer_records)} "
        f"attendu={expected_lines} : des lignes ecrites depuis 8 threads en "
        f"parallele ont ete perdues ou tronquees dans recoverland_debug.log",
    ))
    malformed = [
        r.raw[:100] for r in producer_records
        if r.fields.get("event") != "TX_LIFE_PRODUCER"
        or r.fields.get("module") != "tx_life_wq_stop_restart"
        or not (r.fields.get("tag") or "").startswith("T")
        or r.fields.get("accepted") not in ("True", "False")
    ]
    out.append((
        "log_concurrent_lines_still_parsable",
        not malformed,
        f"{len(malformed)} ligne(s) cle=valeur illisibles apres ecriture "
        f"concurrente, ex: {malformed[:2]} : parse_log.py ne peut plus "
        f"reconstituer ce qui s'est passe, donc plus aucun diagnostic possible "
        f"sur un incident de production",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"tx_life_wq_stop_restart.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Garde de source =============================================
    # La cause racine est structurelle : enqueue() ne consulte jamais
    # l'etat du consommateur, et stop() ne vide pas la file.
    src = (_PLUGIN_ROOT / "core" / "write_queue.py").read_text(
        encoding="utf-8", errors="replace")
    enqueue_body = src[src.find("def enqueue("):src.find("def set_early_warning_callback(")]
    out.append((
        "enqueue_checks_the_writer_is_alive",
        "_running" in enqueue_body or "is_alive" in enqueue_body,
        "enqueue() doit refuser (et basculer en pending) quand aucun thread "
        "ecrivain ne consomme la file ; sans ce test elle renvoie True a un "
        "consommateur inexistant",
    ))
    stop_body = src[src.find("def stop("):src.find("def enqueue(")]
    out.append((
        "stop_drains_what_the_writer_left",
        "_drain_all" in stop_body or "save_pending" in stop_body,
        "stop() promet 'flushing remaining events' mais delegue entierement au "
        "thread ecrivain ; si celui-ci est deja mort ou si un evenement arrive "
        "apres son dernier drain, stop() rend la main en laissant les evenements "
        "en RAM",
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
