"""tx_life_wq_dead_writer - RecoverLand validation runtime.

Invariant: I-WQ-NOLOSS (une edition acceptee finit toujours dans le
journal ou dans le fichier de secours, jamais nulle part).

Le mecanisme
------------
`WriteQueue._writer_loop` est le seul consommateur de la file. Sa boucle
est protegee par un `except sqlite3.Error` et rien d'autre :

    try:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        apply_pragmas(conn)
        while not self._stop_event.is_set():
            batch = self._drain_batch()
            if batch:
                self._write_batch_with_retry(conn, batch)
            ...
    except sqlite3.Error as e:
        flog(f"WriteQueue: fatal SQLite error: {e}", "ERROR")
    finally:
        remaining = self._drain_all()
        ...

Deux portes de sortie tuent le thread pour de bon :

1. `sqlite3.Error` a l'ouverture (journal corrompu, volume reseau
   disparu, fichier verrouille par un antivirus, disque plein au moment
   du `PRAGMA journal_mode=WAL`). Le `finally` draine une file encore
   vide, donc il ne sauve rien, et la boucle ne redemarre jamais.

2. N'IMPORTE QUELLE autre exception levee dans la boucle. La seule
   remontee possible est `self._on_flush_callback(len(batch))`, appelee
   a l'interieur du `try` de `_write_batch_with_retry` juste apres le
   commit. Ce callback est `EditSessionTracker._on_flush_callback`, qui
   fait `self._commit_bridge.committed_features.emit(n_events)` — une
   emission Qt depuis le thread ecrivain vers un QObject que le plugin
   peut avoir libere (changement de projet, fermeture du dialogue,
   `unload`). Un `RuntimeError: wrapped C/C++ object has been deleted`
   n'est pas un `sqlite3.Error` : il traverse `_write_batch_with_retry`,
   traverse la boucle, et termine le thread.

Ce qui se passe ensuite est identique dans les deux cas et c'est le vrai
defaut : la `queue.Queue` reste la, sans consommateur.

    - `enqueue()` continue de renvoyer True : l'appelant croit que
      l'edition est enregistree ;
    - `pending_count` grimpe sans que personne ne regarde ;
    - `stop()` fait `join()` sur un thread deja mort, met `_running` a
      False et journalise "WriteQueue: writer thread stopped" comme si
      le flush avait eu lieu ;
    - aucun `recoverland_pending.json` n'est ecrit.

L'utilisateur travaille une journee entiere en croyant etre protege, et
il n'y a rien dans le journal apres l'incident. C'est le pire resultat
possible pour un outil de recuperation : une protection qui ment.

Scenario (journaux SQLite reels, aucun objet QGIS) :
    D1  journal corrompu -> le thread meurt a l'ouverture
    D2  20 evenements pousses ensuite : acceptes, jamais ecrits, jamais
        sauves, meme apres stop()
    D3  callback de flush qui leve : le thread meurt EN COURS DE SESSION
        apres un premier lot ecrit avec succes
    D4  30 evenements pousses ensuite : meme trou noir
    D5  contre-exemple sain : journal en lecture seule -> l'ecriture du
        lot echoue proprement et le lot part dans le fichier de secours
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess  # nosec B404: only used to flip the read-only attribute
import tempfile
import threading
import time
from pathlib import Path

SCENARIO_ID = "tx_life_wq_dead_writer"
INVARIANT = "I-WQ-NOLOSS"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_WRITER_THREAD_NAME = "RecoverLand-Writer"
_PENDING_FILENAME = "recoverland_pending.json"


def _event(idx: int, session: str):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=None,
        project_fingerprint="tx_dead_project",
        datasource_fingerprint="ds_dead",
        layer_id_snapshot="lyr_dead",
        layer_name_snapshot="reseau",
        provider_type="ogr",
        feature_identity_json='{"fid": %d}' % idx,
        operation_type="DELETE",
        attributes_json='{"full": {"nom": "poteau %d"}}' % idx,
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:2154",
        field_schema_json='[{"name": "nom", "type": "string"}]',
        user_name="tester",
        session_id=session,
        created_at="2026-08-13T11:00:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint="fid:%d" % idx,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _writer_alive() -> int:
    return len([t for t in threading.enumerate() if t.name == _WRITER_THREAD_NAME])


def _pending_len(db_path: str) -> int:
    path = os.path.join(os.path.dirname(db_path), _PENDING_FILENAME)
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return -1
    return len(data) if isinstance(data, list) else -1


def _rows(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error:
        return -1
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
    except sqlite3.Error:
        return -1
    finally:
        conn.close()


def _make_journal(root: str, name: str) -> str:
    from recoverland.core.sqlite_schema import initialize_schema

    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.close()
    return path


def _set_readonly(path: str, on: bool) -> None:
    """Windows: the read-only ATTRIBUTE, not a POSIX mode, blocks SQLite."""
    flag = "+R" if on else "-R"
    try:
        os.chmod(path, 0o444 if on else 0o666)
    except OSError:
        pass
    if os.name == "nt":
        subprocess.run(  # nosec B603 B607: fixed argv, no shell, no user input
            ["attrib", flag, path], capture_output=True, check=False,
        )


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_dead_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["writers_before"] = _writer_alive()

    # D1 : un fichier qui porte le nom d'un journal mais n'en est pas un.
    # Cas reel : coupure pendant une ecriture WAL, restauration partielle
    # d'une sauvegarde, ou synchronisation cloud qui remplace le fichier.
    corrupt = os.path.join(tmpdir, "corrupt", "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(corrupt), exist_ok=True)
    with open(corrupt, "wb") as fh:
        fh.write(b"CE FICHIER N'EST PAS UNE BASE SQLITE. " * 500)
    ctx.data["corrupt_db"] = corrupt

    ctx.data["healthy_db"] = _make_journal(tmpdir, "healthy")
    ctx.data["readonly_db"] = _make_journal(tmpdir, "readonly")

    flog(
        f"tx_life_wq_dead_writer setup: trace_id={ctx.trace_id} tmpdir={tmpdir} "
        f"corrupt={corrupt}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.integrity import check_journal_integrity
    from recoverland.core.logger import flog

    flog(f"tx_life_wq_dead_writer run start: trace_id={ctx.trace_id}", "INFO")

    corrupt = ctx.data["corrupt_db"]
    healthy = ctx.data["healthy_db"]
    readonly = ctx.data["readonly_db"]

    # ===== D0: ce que le controle d'integrite dit du journal corrompu ===
    integrity = check_journal_integrity(corrupt, trace_id=ctx.trace_id)
    ctx.data["d0_healthy"] = integrity.is_healthy
    ctx.data["d0_issues"] = list(integrity.issues)

    # ===== D1 / D2: le thread meurt a l'ouverture =======================
    wq = WriteQueue()
    wq.start(corrupt)
    time.sleep(0.4)
    ctx.data["d1_writer_alive"] = _writer_alive() - ctx.data["writers_before"]

    ctx.data["d2_accepted"] = wq.enqueue([_event(i, "corrupt") for i in range(20)])
    time.sleep(0.4)
    ctx.data["d2_queued_before_stop"] = wq.pending_count
    wq.stop()
    ctx.data["d2_queued_after_stop"] = wq.pending_count
    ctx.data["d2_rows"] = _rows(corrupt)
    ctx.data["d2_pending"] = _pending_len(corrupt)

    # ===== D3 / D4: le thread meurt EN COURS de session ==================
    # Le callback de flush simule exactement l'emission Qt de
    # EditSessionTracker._on_flush_callback vers un pont deja detruit.
    calls = []

    def _flush_callback(n_events: int) -> None:
        calls.append(n_events)
        raise RuntimeError(
            "wrapped C/C++ object of type CommitSignalBridge has been deleted"
        )

    wq = WriteQueue()
    wq.set_flush_callback(_flush_callback)
    wq.start(healthy)
    ctx.data["d3_first_accepted"] = wq.enqueue(
        [_event(i, "avant") for i in range(10)]
    )
    time.sleep(0.5)
    ctx.data["d3_rows_after_first"] = _rows(healthy)
    ctx.data["d3_callback_calls"] = len(calls)
    ctx.data["d3_writer_alive"] = _writer_alive() - ctx.data["writers_before"]

    ctx.data["d4_accepted"] = wq.enqueue([_event(i, "apres") for i in range(100, 130)])
    time.sleep(0.5)
    ctx.data["d4_queued_before_stop"] = wq.pending_count
    wq.stop()
    ctx.data["d4_queued_after_stop"] = wq.pending_count
    ctx.data["d4_rows"] = _rows(healthy)
    ctx.data["d4_pending"] = _pending_len(healthy)

    # ===== D5: contre-exemple sain ======================================
    # Journal accessible mais non inscriptible : l'echec survient a
    # l'INSERT, donc dans le chemin qui sait sauver le lot.
    _set_readonly(readonly, True)
    wq = WriteQueue()
    wq.start(readonly)
    ctx.data["d5_accepted"] = wq.enqueue([_event(i, "ro") for i in range(15)])
    time.sleep(2.0)  # 3 tentatives avec backoff 0.2 / 0.4 / 0.8 s
    wq.stop()
    ctx.data["d5_rows"] = _rows(readonly)
    ctx.data["d5_pending"] = _pending_len(readonly)
    _set_readonly(readonly, False)

    flog(
        f"tx_life_wq_dead_writer run end: trace_id={ctx.trace_id} "
        f"d2_pending={ctx.data['d2_pending']} d2_queued={ctx.data['d2_queued_after_stop']} "
        f"d4_pending={ctx.data['d4_pending']} d4_queued={ctx.data['d4_queued_after_stop']} "
        f"d5_pending={ctx.data['d5_pending']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    d = ctx.data

    # ===== D0: le diagnostic voit bien la corruption ====================
    out.append((
        "d0_corrupt_journal_is_diagnosed",
        d.get("d0_healthy") is False and bool(d.get("d0_issues")),
        f"check_journal_integrity dit healthy={d.get('d0_healthy')} "
        f"issues={d.get('d0_issues')} : un journal illisible doit etre signale "
        f"avant que la file d'ecriture ne s'y accroche",
    ))

    # ===== D1: le thread est bien mort ==================================
    out.append((
        "d1_writer_died_on_corrupt_journal",
        d.get("d1_writer_alive") == 0,
        f"threads ecrivains vivants={d.get('d1_writer_alive')} : constat de "
        f"depart du scenario, le thread sort de sa boucle des l'ouverture",
    ))

    # ===== D2: le trou noir =============================================
    out.append((
        "d2_enqueue_refuses_without_a_consumer",
        d.get("d2_accepted") is False,
        f"enqueue() a renvoye {d.get('d2_accepted')} alors que le thread "
        f"ecrivain est mort. L'appelant recoit un accuse de reception pour des "
        f"evenements que personne n'ecrira jamais.",
    ))
    out.append((
        "d2_stop_reports_the_undrained_queue",
        d.get("d2_queued_after_stop") == 0,
        f"{d.get('d2_queued_after_stop')} evenement(s) encore en file APRES "
        f"stop() (avant stop: {d.get('d2_queued_before_stop')}). stop() "
        f"journalise 'writer thread stopped' comme si le flush avait eu lieu : "
        f"le message ment a l'utilisateur.",
    ))
    out.append((
        "d2_lost_events_reach_the_pending_file",
        d.get("d2_pending") == 20,
        f"recoverland_pending.json contient {d.get('d2_pending')} evenement(s), "
        f"attendu=20 (journal={d.get('d2_rows')} ligne(s)). Le filet de secours "
        f"annonce a l'utilisateur ('evenements recuperes depuis la derniere "
        f"session') ne se declenche jamais dans ce cas : les 20 suppressions "
        f"sont definitivement perdues.",
    ))

    # ===== D3: mort en cours de session =================================
    out.append((
        "d3_first_batch_was_written",
        d.get("d3_rows_after_first") == 10 and d.get("d3_callback_calls") == 1,
        f"lignes={d.get('d3_rows_after_first')} attendu=10 "
        f"callbacks={d.get('d3_callback_calls')} attendu=1 : le premier lot passe "
        f"bien, la session demarre normalement",
    ))
    out.append((
        "d3_callback_error_does_not_kill_the_writer",
        d.get("d3_writer_alive") == 1,
        f"threads ecrivains vivants={d.get('d3_writer_alive')} apres qu'un "
        f"callback de flush a leve. Une erreur de notification (emission Qt vers "
        f"un objet detruit lors d'un changement de projet) ne doit pas arreter "
        f"l'enregistrement : le callback n'est qu'une notification, la donnee est "
        f"deja commitee.",
    ))

    # ===== D4: consequence directe ======================================
    out.append((
        "d4_enqueue_refuses_after_writer_death",
        d.get("d4_accepted") is False,
        f"enqueue() a renvoye {d.get('d4_accepted')} apres la mort du thread : "
        f"a partir de cet instant, plus une seule edition de la session n'est "
        f"enregistree et rien ne le signale",
    ))
    out.append((
        "d4_no_event_stranded_in_ram",
        d.get("d4_queued_after_stop") == 0,
        f"{d.get('d4_queued_after_stop')} evenement(s) restent en file apres "
        f"stop() ; journal={d.get('d4_rows')} ligne(s), "
        f"pending={d.get('d4_pending')}",
    ))
    out.append((
        "d4_lost_events_reach_the_pending_file",
        d.get("d4_pending") == 30,
        f"recoverland_pending.json contient {d.get('d4_pending')} evenement(s), "
        f"attendu=30 : les 30 suppressions faites apres l'incident doivent au "
        f"minimum etre recuperables au prochain demarrage",
    ))

    # ===== D5: garantie acquise =========================================
    out.append((
        "d5_readonly_journal_saves_the_batch",
        d.get("d5_pending") == 15 and d.get("d5_rows") == 0,
        f"journal en lecture seule : pending={d.get('d5_pending')} attendu=15, "
        f"lignes={d.get('d5_rows')} attendu=0. Quand l'echec survient a l'INSERT, "
        f"le lot est correctement bascule dans le fichier de secours — c'est la "
        f"preuve que le filet existe et fonctionne, et donc que son absence dans "
        f"D2/D4 est bien un trou et pas une limite de conception.",
    ))

    # ===== Traces ======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"WriteQueue: fatal SQLite error",
        name="fatal_error_is_logged",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_life_wq_dead_writer.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Gardes de source ============================================
    src = (_PLUGIN_ROOT / "core" / "write_queue.py").read_text(
        encoding="utf-8", errors="replace")

    # Fenetre exacte : du debut de la boucle de drainage jusqu'au `finally`.
    # Le `except Exception` present DANS le finally ne protege que le flush
    # d'arret, pas la boucle : le decoupage doit s'arreter avant lui.
    loop_start = src.find("while not self._stop_event.is_set():")
    loop_end = src.find("finally:", loop_start)
    loop_handlers = src[loop_start:loop_end] if loop_start >= 0 < loop_end else ""
    out.append((
        "writer_loop_catches_more_than_sqlite_errors",
        "except Exception" in loop_handlers or "except BaseException" in loop_handlers,
        f"la boucle de _writer_loop n'est protegee que par "
        f"{[ln.strip() for ln in loop_handlers.splitlines() if ln.strip().startswith('except')]} : "
        f"toute autre exception (callback de flush, objet Qt detruit, MemoryError "
        f"sur un gros WKB) termine le thread et transforme la file en trou noir "
        f"pour le reste de la session",
    ))

    retry = src[src.find("def _write_batch_with_retry("):src.find("def _save_lost_events(")]
    cb_call = retry.find("self._on_flush_callback(")
    guarded = "try:" in retry[max(cb_call - 200, 0):cb_call] and cb_call > 0
    out.append((
        "flush_callback_is_called_under_guard",
        guarded,
        "self._on_flush_callback(len(batch)) est appele sans protection, a "
        "l'interieur du try qui ne rattrape que sqlite3.Error : une notification "
        "qui echoue fait perdre l'enregistrement, alors que le lot est deja "
        "commite en base",
    ))

    stop_body = src[src.find("def stop("):src.find("def enqueue(")]
    out.append((
        "stop_verifies_the_thread_actually_flushed",
        "pending_count" in stop_body or "_drain_all" in stop_body
        or "save_pending" in stop_body,
        "stop() se contente d'un join() puis journalise 'writer thread stopped' ; "
        "il ne verifie jamais que la file est vide, donc il annonce un flush qui "
        "n'a pas eu lieu",
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
