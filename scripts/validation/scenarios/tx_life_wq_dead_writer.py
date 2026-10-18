"""tx_life_wq_dead_writer - RecoverLand validation runtime.

Invariant: I-WQ-NOLOSS (une edition acceptee finit toujours dans le
journal ou dans le fichier de secours, jamais nulle part).

Le mecanisme (avant v7)
-----------------------
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
   fait `self._commit_bridge.committed_features.emit(n_events)` : une
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

Scenario (journaux SQLite reels, aucun objet QGIS), tel que constate
avant v7 :
    D1  journal corrompu -> le thread meurt a l'ouverture
    D2  20 evenements pousses ensuite : acceptes, jamais ecrits, jamais
        sauves, meme apres stop()
    D3  callback de flush qui leve : le thread meurt EN COURS DE SESSION
        apres un premier lot ecrit avec succes
    D4  30 evenements pousses ensuite : meme trou noir
    D5  contre-exemple sain : journal en lecture seule -> l'ecriture du
        lot echoue proprement et le lot part dans le fichier de secours

Hypotheses revisees (v7)
------------------------
La file d'ecriture a ete refondue (schema v7). `enqueue()` ecrit et
fsync le lot dans l'outbox de SON journal (`<journal>.outbox/`) AVANT
tout depot dans la file en RAM, puis rend une issue explicite
(`EnqueueOutcome`). Le "fichier de secours" de l'invariant est desormais
cette outbox, rejouee au demarrage de facon idempotente (`event_uid`) ;
la file n'ecrit plus dans `recoverland_pending.json`. L'ecrivain ne meurt
plus sur une exception non SQLite. Ce que le scenario exige donc :

    D2  plus un refus (False) mais PERSISTED_FALLBACK, qui n'est honnete
        que si l'outbox contient bien les 20 evenements et si l'etat est
        FAILED ; puis, sur le chemin utilisateur (journal corrompu mis de
        cote, journal neuf cree au MEME chemin), le redemarrage
        (`restart_integrate`) les integre exactement une fois.
        stop() ne peut plus etre juge sur `pending_count` : ce compteur
        lit la generation COURANTE, que stop() abandonne, et vaut donc 0
        par construction apres stop(). Le reste non ecrit d'un ecrivain
        mort vit dans l'outbox, pas en RAM : on photographie l'outbox sur
        le disque juste avant stop() (20 evenements) et on exige que CE
        stop() annonce ce reste durable
        (`event=wq_stop_left_in_outbox n_events=20`).
    D4  l'ancien texte supposait l'ecrivain mort, ce que D3 contredit :
        l'ecrivain vivant ecrit les 30 evenements (QUEUED_DURABLE est
        alors vrai), 40 au journal exactement une fois apres redemarrage.
        "Rien de bloque en RAM" se mesure a l'arret, sur le disque :
        lignes du journal + evenements de l'outbox = 40, et 40 au journal
        puisque l'ecrivain est vivant.
    D4b l'intention d'origine de D4 (un ecrivain qui meurt EN COURS de
        session apres un premier lot reussi) reste couverte ici : un
        callback de flush leve une BaseException, qui seule tue encore
        l'ecrivain. L'appel suivant doit rendre PERSISTED_FALLBACK avec
        l'etat FAILED, le lot doit etre sur le disque des l'accuse (copie
        du journal et de son outbox rejouee a part, sans l'aide de la
        memoire du processus), puis 10 + 30 lignes exactement une fois
        apres stop() et redemarrage, outbox videe.
    D5  le lot n'est plus bascule dans `recoverland_pending.json` : il
        reste durable dans l'outbox tant que le journal est en lecture
        seule (etat DEGRADED visible), y est encore APRES stop(), puis
        est integre exactement une fois quand le journal redevient
        inscriptible.
    Gardes de source : les anciennes cherchaient du texte qui n'existe
        plus (`while not self._stop_event.is_set():`,
        `_write_batch_with_retry`, `_save_lost_events` comme borne,
        `_drain_all`/`pending_count` dans stop()). Elles epinglent
        maintenant le mecanisme v7 qui protege le meme invariant :
        outbox avant la RAM, `except Exception` autour de l'ecriture
        d'un lot, boucle qui survit a toute Exception, isolement du lot
        refuse (`_isolate_poison`), callback garde, stop() qui lit le
        reste DURABLE de l'outbox (`unacked_events`,
        `outstanding_batches`) et l'annonce.
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


def _outcome_name(res) -> str:
    return getattr(res, "name", repr(res))


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


def _session_rows(db_path: str, session: str) -> int:
    from scripts.validation.durability import count_rows
    return count_rows(db_path, "session_id = ?", (session,))


def _wait_session_rows(db_path: str, session: str, target: int,
                       timeout: float) -> int:
    """Rows of *session* once *target* is reached or *timeout* expires."""
    deadline = time.monotonic() + timeout
    n = _session_rows(db_path, session)
    while n < target and time.monotonic() < deadline:
        time.sleep(0.05)
        n = _session_rows(db_path, session)
    return n


def _init_journal(path: str) -> str:
    from recoverland.core.sqlite_schema import initialize_schema

    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
    finally:
        conn.close()
    return path


def _make_journal(root: str, name: str) -> str:
    return _init_journal(os.path.join(root, name, "recoverland_audit.sqlite"))


def _set_aside_like_the_plugin(journal_path: str) -> str:
    """What recover.py::_handle_corrupt_journal does on 'Yes'.

    The journal and its -wal/-shm sidecars are renamed aside; the outbox
    (`<journal>.outbox/`) is NOT renamed: it belongs to the path, and the
    fresh journal created at that same path is the one that replays it.
    """
    aside = journal_path + ".corrupt_tx"
    os.rename(journal_path, aside)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(journal_path + suffix):
            os.rename(journal_path + suffix, aside + suffix)
    return aside


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


def _mark(ctx, name: str) -> None:
    """Bracket a step in the log, so an assertion reads only ITS records."""
    from recoverland.core.logger import flog
    flog(f"tx_life_wq_dead_writer mark={name} trace_id={ctx.trace_id}", "INFO")


def _window(records, trace_id: str, begin: str, end: str) -> list:
    """Records strictly between the marks *begin* and *end* of this run."""
    start = stop = None
    for i, rec in enumerate(records):
        raw = getattr(rec, "raw", "")
        if f"trace_id={trace_id}" not in raw:
            continue
        if start is None and f"mark={begin} " in raw:
            start = i
        elif start is not None and f"mark={end} " in raw:
            stop = i
            break
    if start is None or stop is None:
        return []
    return list(records[start + 1:stop])


def _stop_reports(records, trace_id: str, step: str) -> list:
    """n_events of each 'wq_stop_left_in_outbox' logged by the stop() of *step*.

    Only records of the GUI thread count: stop() runs there, the writer
    thread's own shutdown line (wq_final_flush_deferred) is not stop().
    """
    import re
    pat = re.compile(r"WriteQueue: event=wq_stop_left_in_outbox n_events=(\d+)\b")
    out = []
    for rec in _window(records, trace_id, f"{step}_stop_begin", f"{step}_stop_end"):
        if getattr(rec, "thread", "").strip() != "MainThread":
            continue
        m = pat.search(getattr(rec, "raw", ""))
        if m:
            out.append(int(m.group(1)))
    return out


def _replay_disk_copy(journal_path: str, dest_dir: str, sessions=()) -> dict:
    """Replay a COPY of what is on disk now: the journal and its outbox.

    An in-process restart_integrate after stop() proves nothing about a
    crash: the process memory (RAM queue, open handles, stop() itself)
    survived until then. Copying the journal file, its sidecars and its
    `<journal>.outbox` directory right after enqueue() returned, then
    replaying the copy, measures exactly what a QGIS killed at that
    instant would find at its next start.
    """
    from recoverland.core.capture_outbox import outbox_dir_for
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events, restart_integrate,
    )
    os.makedirs(dest_dir, exist_ok=True)
    copy = os.path.join(dest_dir, os.path.basename(journal_path))
    shutil.copy2(journal_path, copy)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(journal_path + suffix):
            shutil.copy2(journal_path + suffix, copy + suffix)
    src_outbox = outbox_dir_for(journal_path)
    copied_outbox = outbox_events(journal_path)
    if os.path.isdir(src_outbox):
        shutil.copytree(src_outbox, outbox_dir_for(copy))
    rep = restart_integrate(copy)
    return {
        "outbox_events_copied": copied_outbox,
        "rows": count_rows(copy),
        "rows_by_session": {s: count_rows(copy, "session_id = ?", (s,))
                            for s in sessions},
        "dups": duplicate_uids(copy),
        "outbox_left": outbox_events(copy),
        "replay": rep.get("replay"),
    }


def _method_body(src: str, name: str) -> str:
    """Source of method *name* of write_queue.py, up to the next def."""
    start = src.find(f"def {name}(")
    if start < 0:
        return ""
    ends = [i for i in (src.find("\n    def ", start + 1),
                        src.find("\ndef ", start + 1)) if i > start]
    return src[start:min(ends)] if ends else src[start:]


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
    ctx.data["dead_mid_db"] = _make_journal(tmpdir, "dead_mid")
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
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events, restart_integrate,
    )

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

    d2 = wq.enqueue([_event(i, "corrupt") for i in range(20)])
    ctx.data["d2_accepted"] = _outcome_name(d2)
    ctx.data["d2_truthy"] = bool(d2)
    ctx.data["d2_health_state"] = wq.health().state
    time.sleep(0.4)
    # Informatif seulement : pending_count lit la generation courante, il
    # ne sert jamais de preuve (et vaut 0 par construction apres stop()).
    ctx.data["d2_queued_before_stop"] = wq.pending_count
    # Le reste non ecrit, tel qu'il est SUR LE DISQUE juste avant stop().
    ctx.data["d2_outbox_before_stop"] = outbox_events(corrupt, session="corrupt")
    _mark(ctx, "d2_stop_begin")
    wq.stop()
    _mark(ctx, "d2_stop_end")
    ctx.data["d2_rows"] = _rows(corrupt)
    ctx.data["d2_pending"] = _pending_len(corrupt)
    ctx.data["d2_outbox"] = outbox_events(corrupt, session="corrupt")

    # Chemin utilisateur au prochain demarrage : le controle d'integrite
    # voit la corruption, recover.py met le journal de cote et en cree un
    # neuf AU MEME CHEMIN, puis demarre la file, qui rejoue l'outbox.
    ctx.data["d2_user_path_error"] = ""
    try:
        _set_aside_like_the_plugin(corrupt)
        _init_journal(corrupt)
        ctx.data["d2_restart"] = restart_integrate(corrupt)
    except (OSError, sqlite3.Error) as exc:
        ctx.data["d2_user_path_error"] = f"{type(exc).__name__}: {exc}"
    ctx.data["d2_rows_after_restart"] = count_rows(
        corrupt, "session_id = ?", ("corrupt",))
    ctx.data["d2_dups_after_restart"] = duplicate_uids(corrupt)
    ctx.data["d2_outbox_after_restart"] = outbox_events(corrupt)

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

    d4 = wq.enqueue([_event(i, "apres") for i in range(100, 130)])
    ctx.data["d4_accepted"] = _outcome_name(d4)
    time.sleep(0.5)
    ctx.data["d4_queued_before_stop"] = wq.pending_count   # informatif
    # Ecrits par l'ecrivain VIVANT, avant stop() : sinon QUEUED_DURABLE
    # annoncerait un consommateur qui ne consomme pas.
    ctx.data["d4_rows_apres_live"] = _wait_session_rows(healthy, "apres", 30, 5.0)
    wq.stop()
    # A l'arret, AVANT tout redemarrage : journal et outbox sur le disque.
    ctx.data["d4_rows"] = _rows(healthy)
    ctx.data["d4_pending"] = _pending_len(healthy)
    ctx.data["d4_outbox_after_stop"] = outbox_events(healthy)
    ctx.data["d4_restart"] = restart_integrate(healthy)
    ctx.data["d4_rows_after_restart"] = count_rows(healthy)
    ctx.data["d4_rows_avant_after_restart"] = _session_rows(healthy, "avant")
    ctx.data["d4_rows_apres_after_restart"] = _session_rows(healthy, "apres")
    ctx.data["d4_dups_after_restart"] = duplicate_uids(healthy)
    ctx.data["d4_outbox_after_restart"] = outbox_events(healthy)

    # ===== D4b: le thread meurt VRAIMENT en cours de session =============
    # D3/D4 montrent que l'ecrivain survit a une Exception du callback. Une
    # BaseException (KeyboardInterrupt, SystemExit, erreur interne levee
    # depuis un slot Qt) le tue encore : c'est l'intention d'origine de D4,
    # un ecrivain mort APRES un premier lot reussi.
    dead_mid = ctx.data["dead_mid_db"]

    class _WriterKilled(BaseException):
        pass

    kills = []

    def _killing_callback(n_events: int) -> None:
        kills.append(n_events)
        raise _WriterKilled("writer killed mid-session by the flush callback")

    wq = WriteQueue()
    wq.set_flush_callback(_killing_callback)
    wq.start(dead_mid)
    own = wq._thread   # l'ecrivain de CETTE generation, pas ceux d'autres tests
    ctx.data["d4b_first"] = _outcome_name(
        wq.enqueue([_event(i, "d4b_avant") for i in range(10)]))
    deadline = time.monotonic() + 5.0
    while own is not None and own.is_alive() and time.monotonic() < deadline:
        time.sleep(0.02)
    ctx.data["d4b_thread_alive"] = bool(own is not None and own.is_alive())
    ctx.data["d4b_health_alive"] = wq.health().writer_alive
    ctx.data["d4b_kills"] = len(kills)
    ctx.data["d4b_rows_first"] = _session_rows(dead_mid, "d4b_avant")
    d4b = wq.enqueue([_event(i, "d4b_apres") for i in range(100, 130)])
    ctx.data["d4b_later"] = _outcome_name(d4b)
    ctx.data["d4b_later_truthy"] = bool(d4b)
    ctx.data["d4b_state"] = wq.health().state
    # Ce qu'un QGIS tue A CET INSTANT retrouverait : copie disque rejouee.
    ctx.data["d4b_ack_copy"] = _replay_disk_copy(
        dead_mid, os.path.join(ctx.data["tmpdir"], "d4b_ack_copy"),
        sessions=("d4b_avant", "d4b_apres"))
    ctx.data["d4b_outbox_before_stop"] = outbox_events(dead_mid, session="d4b_apres")
    _mark(ctx, "d4b_stop_begin")
    wq.stop()
    _mark(ctx, "d4b_stop_end")
    ctx.data["d4b_rows_at_stop"] = count_rows(dead_mid)
    ctx.data["d4b_outbox_after_stop"] = outbox_events(dead_mid)
    ctx.data["d4b_restart"] = restart_integrate(dead_mid)
    ctx.data["d4b_rows_after_restart"] = count_rows(dead_mid)
    ctx.data["d4b_rows_avant_after_restart"] = _session_rows(dead_mid, "d4b_avant")
    ctx.data["d4b_rows_apres_after_restart"] = _session_rows(dead_mid, "d4b_apres")
    ctx.data["d4b_dups_after_restart"] = duplicate_uids(dead_mid)
    ctx.data["d4b_outbox_after_restart"] = outbox_events(dead_mid)

    # ===== D5: contre-exemple sain ======================================
    # Journal accessible mais non inscriptible : l'echec survient a
    # l'INSERT, donc dans le chemin qui sait garder le lot.
    _set_readonly(readonly, True)
    wq = WriteQueue()
    wq.start(readonly)
    d5 = wq.enqueue([_event(i, "ro") for i in range(15)])
    ctx.data["d5_accepted"] = _outcome_name(d5)
    ctx.data["d5_truthy"] = bool(d5)
    time.sleep(2.0)  # plusieurs tentatives avec backoff 0.2 / 0.4 / 0.8 s
    h5 = wq.health()
    ctx.data["d5_health_during"] = {
        "state": h5.state, "writer_alive": h5.writer_alive,
        "journal_writable": h5.journal_writable,
        "outbox_outstanding": h5.outbox_outstanding,
        "last_persistence_error": h5.last_persistence_error,
    }
    ctx.data["d5_outbox_during"] = outbox_events(readonly, session="ro")
    _mark(ctx, "d5_stop_begin")
    wq.stop()
    _mark(ctx, "d5_stop_end")
    ctx.data["d5_rows"] = _rows(readonly)
    ctx.data["d5_pending"] = _pending_len(readonly)
    ctx.data["d5_outbox_after_stop"] = outbox_events(readonly, session="ro")
    _set_readonly(readonly, False)
    ctx.data["d5_restart"] = restart_integrate(readonly)
    ctx.data["d5_rows_after_restart"] = _session_rows(readonly, "ro")
    ctx.data["d5_dups_after_restart"] = duplicate_uids(readonly)
    ctx.data["d5_outbox_after_restart"] = outbox_events(readonly)

    flog(
        f"tx_life_wq_dead_writer run end: trace_id={ctx.trace_id} "
        f"d2={ctx.data['d2_accepted']} d2_outbox={ctx.data['d2_outbox']} "
        f"d2_after_restart={ctx.data['d2_rows_after_restart']} "
        f"d4={ctx.data['d4_accepted']} d4_after_restart={ctx.data['d4_rows_after_restart']} "
        f"d4b={ctx.data['d4b_later']} d4b_state={ctx.data['d4b_state']} "
        f"d4b_after_restart={ctx.data['d4b_rows_after_restart']} "
        f"d5={ctx.data['d5_accepted']} d5_state={h5.state} "
        f"d5_after_restart={ctx.data['d5_rows_after_restart']} "
        f"pending={ctx.data['d2_pending']}/{ctx.data['d4_pending']}/{ctx.data['d5_pending']}",
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
    # Hypothesis revised (v7): enqueue() ne renvoie plus un booleen mais un
    # EnqueueOutcome, et un lot sans ecrivain vivant n'est plus refuse : il
    # est fsync dans l'outbox du journal AVANT toute file en RAM. Le faux
    # accuse de reception que l'ancienne assertion traquait (True sans
    # consommateur) devient ici : l'issue ne doit pas etre QUEUED_DURABLE,
    # elle doit etre PERSISTED_FALLBACK, et cette issue n'est honnete que
    # si les 20 evenements sont REELLEMENT dans l'outbox nommee d'apres le
    # journal et si l'etat expose est FAILED. Plus strict que "is False" :
    # on verifie la promesse, pas seulement l'absence de promesse.
    # (ancien nom : d2_enqueue_refuses_without_a_consumer ; "refuses"
    # decrirait un comportement que v7 abandonne volontairement.)
    out.append((
        "d2_enqueue_does_not_claim_a_live_writer",
        d.get("d2_accepted") == "PERSISTED_FALLBACK" and d.get("d2_truthy") is True
        and d.get("d2_health_state") == "failed" and d.get("d2_outbox") == 20,
        f"enqueue() a renvoye {d.get('d2_accepted')} (bool={d.get('d2_truthy')}) "
        f"etat={d.get('d2_health_state')} outbox={d.get('d2_outbox')} evenement(s) "
        f"alors que le thread ecrivain est mort. Attendu : PERSISTED_FALLBACK, "
        f"etat failed, 20 evenements dans l'outbox du journal. QUEUED_DURABLE "
        f"annoncerait un consommateur qui n'existe pas ; un accuse durable sans "
        f"les 20 evenements dans l'outbox serait un mensonge.",
    ))
    # Hypothesis revised (v7): l'ancienne mesure (pending_count apres stop()
    # == 0) est une tautologie en v7 : pending_count lit la generation
    # COURANTE, stop() l'abandonne (_gen = None), la valeur est 0 par
    # construction quoi que stop() ait fait. Et un ecrivain mort n'a jamais
    # rien en RAM : enqueue() ne depose en file que sous "gen.accepting and
    # gen.alive", son reste non ecrit vit dans l'outbox. Meme intention
    # (stop() ne doit pas se taire sur ce qui n'a pas ete ecrit), mesuree la
    # ou vit ce reste : 20 evenements dans l'outbox SUR LE DISQUE juste
    # avant stop(), et CE stop() (fenetre bornee par les marques d2_stop_*,
    # thread GUI) annonce exactement ce reste durable :
    # 'event=wq_stop_left_in_outbox n_events=20'.
    d2_reports = _stop_reports(ctx.records, ctx.trace_id, "d2")
    out.append((
        "d2_stop_reports_the_undrained_queue",
        d.get("d2_outbox_before_stop") == 20 and d2_reports == [20],
        f"outbox du journal corrompu juste avant stop()="
        f"{d.get('d2_outbox_before_stop')} evenement(s) attendu=20 ; annonces "
        f"wq_stop_left_in_outbox de CE stop()={d2_reports} attendu=[20] "
        f"(pending_count avant stop, informatif: {d.get('d2_queued_before_stop')}). "
        f"Un stop() qui ne dit que 'writer thread stopped' alors que 20 "
        f"suppressions attendent dans l'outbox annonce un flush qui n'a pas eu "
        f"lieu : le message ment a l'utilisateur.",
    ))
    # Hypothesis revised (v7): la file n'ecrit plus recoverland_pending.json :
    # le filet de secours est l'outbox du journal, rejouee au demarrage. L'ancienne assertion
    # verifiait un lieu de stockage ; celle-ci verifie la propriete que ce
    # lieu devait garantir, sur le chemin reel de l'utilisateur :
    # recover.py::_handle_corrupt_journal met le journal corrompu de cote
    # et en cree un neuf au MEME chemin, puis le demarrage rejoue l'outbox.
    # Les 20 suppressions doivent y etre exactement une fois (compte exact,
    # aucun event_uid en double) et l'outbox doit etre videe. Plus strict :
    # l'ancienne se contentait de leur presence dans un fichier d'attente.
    # (ancien nom : d2_lost_events_reach_the_pending_file.)
    out.append((
        "d2_events_reach_the_fresh_journal_exactly_once",
        not d.get("d2_user_path_error")
        and d.get("d2_rows_after_restart") == 20
        and d.get("d2_dups_after_restart") == 0
        and d.get("d2_outbox_after_restart") == 0,
        f"apres mise de cote du journal corrompu et redemarrage : "
        f"journal neuf={d.get('d2_rows_after_restart')} ligne(s) attendu=20, "
        f"event_uid en double={d.get('d2_dups_after_restart')} attendu=0, "
        f"outbox restante={d.get('d2_outbox_after_restart')} attendu=0, "
        f"erreur chemin utilisateur={d.get('d2_user_path_error')!r}, "
        f"pending legacy={d.get('d2_pending')}, rejeu={d.get('d2_restart')}. "
        f"Sinon les 20 suppressions acceptees pendant l'incident sont perdues "
        f"ou dupliquees.",
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
    # Hypothesis revised (v7): l'ancienne assertion exigeait un refus "apres
    # la mort du thread", ce qui contredit D3 (le thread doit survivre au
    # callback, et il survit). Avec un ecrivain vivant, QUEUED_DURABLE est la
    # seule reponse vraie ; elle n'est vraie que si cet ecrivain ecrit
    # effectivement les 30 evenements pendant la session, avant stop().
    # Meme intention (l'issue rendue a l'appelant correspond a la realite),
    # verifiee par son effet. (ancien nom : d4_enqueue_refuses_after_writer_death.)
    out.append((
        "d4_later_edits_are_indexed_by_the_live_writer",
        d.get("d4_accepted") == "QUEUED_DURABLE"
        and d.get("d4_rows_apres_live") == 30,
        f"enqueue() a renvoye {d.get('d4_accepted')} apres le callback qui leve ; "
        f"lignes 'apres' ecrites par l'ecrivain vivant avant stop()="
        f"{d.get('d4_rows_apres_live')} attendu=30. QUEUED_DURABLE promet un "
        f"consommateur vivant : la promesse doit se verifier au journal.",
    ))
    # Hypothesis revised (v7): l'ancienne mesure (pending_count apres stop()
    # == 0) ne peut plus echouer : pending_count vaut 0 des que stop() a
    # abandonne la generation, meme si 30 evenements etaient restes en RAM
    # sans jamais etre ecrits. Meme intention (a l'arret, aucun evenement
    # accepte ne vit seulement en RAM), mesuree sur le disque a l'instant
    # de l'arret, avant tout redemarrage : lignes du journal + evenements
    # de l'outbox = 40 (10 + 30), et 40 au journal puisque l'ecrivain est
    # vivant (rien ne doit meme attendre dans l'outbox).
    d4_rows_at_stop = d.get("d4_rows")
    d4_outbox_at_stop = d.get("d4_outbox_after_stop")
    out.append((
        "d4_no_event_stranded_in_ram",
        isinstance(d4_rows_at_stop, int) and isinstance(d4_outbox_at_stop, int)
        and d4_rows_at_stop + d4_outbox_at_stop == 40
        and d4_rows_at_stop == 40,
        f"a l'arret (avant redemarrage) : journal={d4_rows_at_stop} ligne(s) + "
        f"outbox={d4_outbox_at_stop} evenement(s), attendu 40 au total et 40 "
        f"au journal (ecrivain vivant). Ce qui manque a cette somme ne vivait "
        f"qu'en RAM et disparait avec le processus ; pending legacy="
        f"{d.get('d4_pending')}, pending_count avant stop (informatif)="
        f"{d.get('d4_queued_before_stop')}",
    ))
    # Hypothesis revised (v7): les 30 evenements ne passent plus par
    # recoverland_pending.json (la file ne l'ecrit plus, et l'ecrivain n'est
    # plus mort). La propriete visee, "recuperables au prochain demarrage",
    # se mesure au journal apres un redemarrage simule : 10 + 30 = 40
    # lignes, chacune exactement une fois (le rejeu idempotent ne doit rien
    # ajouter), et plus rien dans l'outbox. Plus strict que "30 dans un
    # fichier d'attente". (ancien nom : d4_lost_events_reach_the_pending_file.)
    out.append((
        "d4_later_events_reach_the_journal_exactly_once",
        d.get("d4_rows_after_restart") == 40
        and d.get("d4_rows_avant_after_restart") == 10
        and d.get("d4_rows_apres_after_restart") == 30
        and d.get("d4_dups_after_restart") == 0
        and d.get("d4_outbox_after_restart") == 0,
        f"apres redemarrage : journal={d.get('d4_rows_after_restart')} attendu=40 "
        f"(avant={d.get('d4_rows_avant_after_restart')}/10, "
        f"apres={d.get('d4_rows_apres_after_restart')}/30), event_uid en double="
        f"{d.get('d4_dups_after_restart')} attendu=0, outbox restante="
        f"{d.get('d4_outbox_after_restart')} attendu=0 (apres stop: "
        f"{d.get('d4_outbox_after_stop')}), pending legacy={d.get('d4_pending')}, "
        f"rejeu={d.get('d4_restart')}",
    ))

    # ===== D4b: mort reelle en cours de session =========================
    # Ajout (v7) : D4 ne couvre plus un ecrivain mort, puisque l'ecrivain
    # survit desormais a une Exception du callback (D3). L'intention
    # d'origine de D4, "apres la mort du thread EN COURS de session, aucun
    # accuse normal et rien de perdu", est rejouee avec la seule cause qui
    # tue encore l'ecrivain : une BaseException levee apres un premier lot
    # reussi. PERSISTED_FALLBACK n'est honnete que si le lot est SUR LE
    # DISQUE a l'instant ou l'accuse est rendu : la copie du journal et de
    # son outbox, prise juste apres enqueue() et rejouee a part (sans la
    # memoire du processus, sans stop()), doit donner 10 + 30.
    ack = d.get("d4b_ack_copy") or {}
    ack_rows = ack.get("rows_by_session") or {}
    out.append((
        "d4b_writer_death_mid_session_is_not_acknowledged_as_live",
        d.get("d4b_first") == "QUEUED_DURABLE"
        and d.get("d4b_kills") == 1 and d.get("d4b_rows_first") == 10
        and d.get("d4b_thread_alive") is False
        and d.get("d4b_health_alive") is False
        and d.get("d4b_later") == "PERSISTED_FALLBACK"
        and d.get("d4b_later_truthy") is True
        and d.get("d4b_state") == "failed"
        and ack_rows.get("d4b_avant") == 10 and ack_rows.get("d4b_apres") == 30
        and ack.get("dups") == 0 and ack.get("outbox_left") == 0,
        f"premier lot={d.get('d4b_first')} ecrit={d.get('d4b_rows_first')}/10 "
        f"callbacks={d.get('d4b_kills')}/1 ; ecrivain vivant apres la "
        f"BaseException: thread={d.get('d4b_thread_alive')} "
        f"health={d.get('d4b_health_alive')} (attendu False/False) ; lot suivant="
        f"{d.get('d4b_later')} (bool={d.get('d4b_later_truthy')}) etat="
        f"{d.get('d4b_state')} attendu PERSISTED_FALLBACK/True/failed ; copie "
        f"disque a l'accuse rejouee: {ack} attendu avant=10 apres=30 doublons=0 "
        f"outbox restante=0. QUEUED_DURABLE annoncerait un consommateur mort ; "
        f"un accuse durable sans le lot sur le disque serait perdu par un "
        f"plantage a cet instant.",
    ))
    d4b_rows_at_stop = d.get("d4b_rows_at_stop")
    d4b_outbox_at_stop = d.get("d4b_outbox_after_stop")
    out.append((
        "d4b_later_events_reach_the_journal_exactly_once",
        isinstance(d4b_rows_at_stop, int) and isinstance(d4b_outbox_at_stop, int)
        and d4b_rows_at_stop + d4b_outbox_at_stop == 40
        and d.get("d4b_rows_after_restart") == 40
        and d.get("d4b_rows_avant_after_restart") == 10
        and d.get("d4b_rows_apres_after_restart") == 30
        and d.get("d4b_dups_after_restart") == 0
        and d.get("d4b_outbox_after_restart") == 0,
        f"a l'arret : journal={d4b_rows_at_stop} + outbox={d4b_outbox_at_stop} "
        f"attendu 40 (rien seulement en RAM) ; apres stop() et redemarrage : "
        f"journal={d.get('d4b_rows_after_restart')} attendu=40 (avant="
        f"{d.get('d4b_rows_avant_after_restart')}/10, apres="
        f"{d.get('d4b_rows_apres_after_restart')}/30), doublons="
        f"{d.get('d4b_dups_after_restart')} attendu=0, outbox restante="
        f"{d.get('d4b_outbox_after_restart')} attendu=0, rejeu="
        f"{d.get('d4b_restart')}",
    ))

    # ===== D5: garantie acquise =========================================
    # Hypothesis revised (v7): un echec d'INSERT ne bascule plus le lot dans
    # recoverland_pending.json : le lot est deja durable dans l'outbox avant
    # la tentative, l'ecrivain passe DEGRADED et reessaie, stop() laisse le
    # lot dans l'outbox. Meme intention (le lot n'est pas perdu quand le
    # journal refuse l'ecriture), verifiee de bout en bout : accuse durable,
    # 15 evenements dans l'outbox et 0 au journal tant qu'il est en lecture
    # seule, etat degraded et journal_writable=False exposes, les 15 encore
    # dans l'outbox APRES stop() (l'ancienne mesure se faisait apres l'arret :
    # le lot doit survivre a l'arret dans le stockage de secours, pas
    # seulement pendant la session), puis 15 lignes exactement une fois des
    # que le journal redevient inscriptible.
    out.append((
        "d5_readonly_journal_saves_the_batch",
        d.get("d5_truthy") is True
        and d.get("d5_outbox_during") == 15 and d.get("d5_rows") == 0
        and d.get("d5_outbox_after_stop") == 15
        and (d.get("d5_health_during") or {}).get("state") == "degraded"
        and (d.get("d5_health_during") or {}).get("journal_writable") is False
        and d.get("d5_rows_after_restart") == 15
        and d.get("d5_dups_after_restart") == 0
        and d.get("d5_outbox_after_restart") == 0,
        f"journal en lecture seule : issue={d.get('d5_accepted')} "
        f"outbox={d.get('d5_outbox_during')} attendu=15, lignes={d.get('d5_rows')} "
        f"attendu=0, outbox apres stop()={d.get('d5_outbox_after_stop')} "
        f"attendu=15, sante={d.get('d5_health_during')} (attendu degraded, "
        f"journal_writable=False). Journal redevenu inscriptible puis "
        f"redemarrage : lignes={d.get('d5_rows_after_restart')} attendu=15, "
        f"doublons={d.get('d5_dups_after_restart')} attendu=0, outbox restante="
        f"{d.get('d5_outbox_after_restart')} attendu=0, pending legacy="
        f"{d.get('d5_pending')}, rejeu={d.get('d5_restart')}.",
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
    loop = _method_body(src, "_writer_loop")
    write = _method_body(src, "_write_batches")
    stop = _method_body(src, "stop")
    final_flush = _method_body(src, "_final_flush")
    enqueue = _method_body(src, "enqueue")

    # Hypothesis revised (v7): la boucle n'est plus
    # `while not self._stop_event.is_set():` (elle lit gen.stop_event de SA
    # generation), donc l'ancienne fenetre de recherche etait vide. Le
    # mecanisme qui empeche une exception non SQLite de tuer le consommateur
    # est maintenant : l'ecriture d'un lot (insert_events_idempotent) est
    # dans un try rattrape par `except Exception` qui rend (False, False)
    # sans relancer ; la boucle reessaie puis met en quarantaine un lot
    # refuse 3 fois (_quarantine_poison) au lieu de mourir ; seule une
    # BaseException sort, et elle est enregistree (except BaseException).
    # Plus strict : on exige les trois, pas un simple `except Exception`
    # n'importe ou dans la boucle.
    # Hypothesis revised (v7): `_quarantine_poison` n'existe plus. Un lot
    # refuse 3 fois est desormais ISOLE (`_isolate_poison` : chaque lot seul,
    # puis chaque evenement seul, seuls les refuses vont en quarantaine) :
    # meme role pour cette garde (la boucle traite le lot refuse au lieu de
    # mourir), nom de la methode mis a jour. Et la boucle a gagne une
    # seconde protection que la garde exige aussi : un `except Exception`
    # DANS le while, avant `except BaseException`, qui journalise
    # wq_loop_error et continue (ni raise ni break) quand l'echec survient
    # hors de l'ecriture du lot (ack, checkpoint, quarantaine).
    w_try = write.find("try:")
    w_ins = write.find("insert_events_idempotent(")
    w_exc = write.find("except Exception", w_ins) if w_ins >= 0 else -1
    w_handler = write[w_exc:w_exc + 300] if w_exc >= 0 else ""
    w_handler = w_handler.split("return False, False")[0] if (
        "return False, False" in w_handler) else ""
    l_exc = loop.find("except Exception")
    l_base = loop.find("except BaseException")
    l_end = loop.find("clean_exit = True", l_exc) if l_exc >= 0 else -1
    l_handler = loop[l_exc:l_end] if 0 <= l_exc < l_end else ""
    loop_stays_alive = (
        0 <= l_exc < l_base and bool(l_handler)
        and "wq_loop_error" in l_handler
        and "raise" not in l_handler and "break" not in l_handler)
    out.append((
        "writer_loop_catches_more_than_sqlite_errors",
        0 <= w_try < w_ins < w_exc and bool(w_handler)
        and "raise" not in w_handler
        and "self._write_batches(" in loop
        and "self._isolate_poison(" in loop
        and "except BaseException" in loop
        and loop_stays_alive,
        f"_write_batches: try={w_try} insert={w_ins} except Exception={w_exc} "
        f"handler_sans_raise={bool(w_handler) and 'raise' not in w_handler} ; "
        f"_writer_loop: appelle _write_batches={'self._write_batches(' in loop} "
        f"isolement du lot poison={'self._isolate_poison(' in loop} "
        f"except BaseException={'except BaseException' in loop} "
        f"except Exception de boucle qui continue={loop_stays_alive}. Sans cela, "
        f"toute exception non SQLite (callback de flush, objet Qt detruit, "
        f"MemoryError sur un gros WKB) termine le thread et l'enregistrement "
        f"s'arrete pour le reste de la session",
    ))

    # Hypothesis revised (v7): _write_batch_with_retry et _save_lost_events
    # (bornes de l'ancienne fenetre) n'existent plus ; le callback est appele
    # dans _write_batches. Meme exigence, plus stricte : l'appel est APRES le
    # COMMIT et l'acquittement de l'outbox, precede d'un `try:` et suivi d'un
    # `except Exception` qui journalise sans relancer.
    cb_call = write.find("self._on_flush_callback(")
    # Hypothesis revised (v7): l'acquittement passe par `self._ack(gen, b)`,
    # qui contient l'echec d'un ack sans le laisser remonter a la boucle.
    # Equivalent a l'appel direct : on exige en plus que `_ack` appelle
    # reellement `gen.outbox.ack(`.
    ack_body = _method_body(src, "_ack")
    ack_pos = write.find("gen.outbox.ack(")
    if ack_pos < 0 and "gen.outbox.ack(" in ack_body:
        ack_pos = write.find("self._ack(")
    cb_before = write[max(cb_call - 200, 0):cb_call] if cb_call > 0 else ""
    cb_after = write[cb_call:cb_call + 300] if cb_call > 0 else ""
    out.append((
        "flush_callback_is_called_under_guard",
        cb_call > 0 and 0 <= ack_pos < cb_call
        and "try:" in cb_before and "except Exception" in cb_after,
        f"_write_batches: appel du callback={cb_call} ack outbox={ack_pos} "
        f"try avant={'try:' in cb_before} except Exception apres="
        f"{'except Exception' in cb_after}. Un callback appele sans protection "
        f"fait qu'une notification qui echoue arrete l'enregistrement, alors "
        f"que le lot est deja commite en base",
    ))

    # Hypothesis revised (v7): stop() ne contient plus pending_count,
    # _drain_all ni save_pending : ce qui reste en RAM est deja dans
    # l'outbox. Le mecanisme qui l'empeche d'annoncer un flush qui n'a pas
    # eu lieu est : stop() vide la file de SA generation (_drain_ram(gen))
    # et annonce le reste (wq_stop_left_in_outbox), annonce un arret hors
    # delai (wq_stop_timeout), et l'ecrivain annonce le lot qu'il n'a pas
    # pu ecrire a l'arret (wq_final_flush_deferred). Plus strict que la
    # garde d'origine : en plus du source, on exige la preuve a l'execution
    # que l'arret de D5 (journal en lecture seule) a bien annonce les 15
    # evenements non ecrits.
    # Hypothesis revised (v7): la garde ci-dessus ne couvrait pas le cas pour
    # lequel l'originale existait (D2 : stop() d'un ecrivain DEJA mort). Le
    # reste d'un ecrivain mort n'est jamais en RAM (enqueue ne depose en
    # file que pour un ecrivain vivant), donc _drain_ram(gen) ne le voit pas,
    # et la preuve D5 venait du thread ecrivain (_final_flush), pas de
    # stop(). La garde pre-v7 exigeait que stop() consulte le stockage des
    # evenements non ecrits ; en v7 ce stockage est l'outbox. Meme
    # intention, donc : le corps de stop() lit le reste DURABLE
    # (`.unacked_events` et `.outstanding_batches` de l'outbox, avant
    # l'annonce wq_stop_left_in_outbox), en plus de _drain_ram(gen). Preuve
    # a l'execution, sur les ecrivains morts, la ou stop() est le seul a
    # pouvoir parler : le stop() de D2 annonce n_events=20, et le stop() de
    # D4b annonce exactement ce que l'outbox garde sur le disque apres lui.
    # La preuve D5 d'origine est conservee.
    shutdown_said = assert_log_contains(
        ctx.records,
        r"event=(wq_final_flush_deferred|wq_stop_left_in_outbox) n_events=15\b",
        name="_shutdown_said", min_count=1)
    s_unacked = stop.find(".unacked_events")
    s_batches = stop.find(".outstanding_batches")
    s_report = stop.find("wq_stop_left_in_outbox")
    reads_durable_backlog = 0 <= s_unacked < s_report and 0 <= s_batches < s_report
    d4b_left = d.get("d4b_outbox_after_stop")
    d4b_reports = _stop_reports(ctx.records, ctx.trace_id, "d4b")
    d4b_stop_said = (isinstance(d4b_left, int) and (
        d4b_reports == [d4b_left] if d4b_left > 0 else d4b_reports == []))
    out.append((
        "stop_verifies_the_thread_actually_flushed",
        "self._drain_ram(gen)" in stop and "wq_stop_left_in_outbox" in stop
        and "wq_stop_timeout" in stop
        and "wq_final_flush_deferred" in final_flush
        and reads_durable_backlog
        and d2_reports == [20]
        and d4b_stop_said
        and shutdown_said[1],
        f"stop(): _drain_ram(gen)={'self._drain_ram(gen)' in stop} "
        f"wq_stop_left_in_outbox={'wq_stop_left_in_outbox' in stop} "
        f"wq_stop_timeout={'wq_stop_timeout' in stop} lit le reste durable "
        f"(unacked_events={s_unacked} outstanding_batches={s_batches} avant "
        f"l'annonce={s_report})={reads_durable_backlog} ; _final_flush: "
        f"wq_final_flush_deferred={'wq_final_flush_deferred' in final_flush} ; "
        f"annonces du stop() de D2={d2_reports} attendu=[20] ; du stop() de "
        f"D4b={d4b_reports} attendu=[{d4b_left}] (outbox sur le disque apres "
        f"stop) ; annonce a l'arret de D5 ({shutdown_said[2]}). Sinon stop() "
        f"journalise 'writer thread stopped' comme si tout avait ete ecrit",
    ))

    # Garde du mecanisme qui ferme le trou noir de D2/D4 : l'outbox (fsync)
    # est ecrite AVANT tout depot dans la file en RAM, un echec d'outbox
    # part vers le repli synchrone, et le depot en RAM n'a lieu que sous
    # accept_lock quand l'ecrivain de la generation est vivant.
    e_app = enqueue.find(".append_events(")
    e_seq = enqueue.find("if seq is None:")
    e_put = enqueue.find(".queue.put(")
    e_gate = enqueue[e_seq:e_put] if 0 <= e_seq < e_put else ""
    out.append((
        "enqueue_persists_before_any_ram_put",
        0 <= e_app < e_seq < e_put
        and "_write_sync_or_refuse(" in e_gate
        and "gen.accepting and gen.alive" in e_gate,
        f"enqueue(): append_events={e_app} if seq is None={e_seq} "
        f"queue.put={e_put} repli synchrone entre les deux="
        f"{'_write_sync_or_refuse(' in e_gate} garde ecrivain vivant="
        f"{'gen.accepting and gen.alive' in e_gate}. Un accuse de reception pour "
        f"un lot qui ne vit qu'en RAM est exactement le trou noir de D2/D4",
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
