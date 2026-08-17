"""tx_life_wq_batch_rescue - RecoverLand validation runtime.

Invariant: I-WQ-EXACTLY-ONCE (un evenement accepte finit dans le journal
OU dans le fichier d'attente, exactement une fois, jamais dans les deux
et jamais nulle part).

Pourquoi ce scenario existe
---------------------------
`tx_life_wq_dead_writer` prouve le contraire de son epoque : il decrit un
thread ecrivain qui MEURT a la premiere exception inattendue et une file
devenue un trou noir. Ce defaut est corrige — la boucle de
`WriteQueue._writer_loop` rattrape maintenant `Exception` et reste en
vie — mais la correction ouvre deux questions que ce scenario est le seul
a poser, et qui pesent plus lourd que le defaut d'origine :

1. Le LOT qui a declenche l'erreur a deja ete sorti de la file par
   `_drain_batch()` : la liste Python tenue par la boucle est la seule
   reference qui reste. Si le drainage avait lieu a l'interieur du `try`,
   survivre a l'exception detruirait justement les evenements qui l'ont
   provoquee — le thread vivant, la donnee perdue. Le drainage doit donc
   rester DEHORS, et le lot doit basculer dans
   `recoverland_pending.json`.

2. Basculer un lot deja commite serait aussi grave, dans l'autre sens :
   `_recover_pending_events` reinjecte le fichier d'attente au demarrage
   suivant, donc un lot sauve en double reapparait en double dans le
   journal. Un UPDATE compte deux fois, un REWIND rejoue deux fois, et
   `rw_dedup` voit une chaine qu'aucune edition n'a produite. C'est le
   piege du remede : `_write_batch_with_retry` sauve DEJA le lot sur ses
   propres chemins d'echec, donc la boucle ne doit pas le sauver une
   seconde fois.

Le point de rupture injecte
---------------------------
Aucun code produit n'est modifie. Le scenario pousse des evenements dont
la lecture d'un champ leve : une sous-classe de `AuditEvent` dont
`geometry_wkb` est une propriete qui leve `RuntimeError`. C'est le
`_event_to_row(e)` de `_write_batch_with_retry` qui explose, avant tout
INSERT, exactement comme un `MemoryError` sur un WKB de plusieurs dizaines
de Mo ou un objet Qt libere sous un champ. L'erreur n'est pas un
`sqlite3.Error` : elle traverse le helper d'ecriture et remonte dans la
boucle. `_asdict()` reste valide (le namedtuple s'itere par valeurs), donc
le lot est serialisable et le fichier d'attente est bien la seule bonne
destination.

Ce que ce scenario torture (journaux SQLite reels, aucun objet QGIS) :
    R1  lot sain -> journal
    R2  lot qui explose en ecriture -> ecrivain vivant, lot dans le
        fichier d'attente, UNE seule fois
    R3  lot sain pousse APRES l'incident -> toujours ecrit (la file n'est
        pas devenue un trou noir)
    R4  conservation : journal + attente == accepte, sans recouvrement
    R5  callback de notification qui leve : le lot commite N'EST PAS
        recopie dans le fichier d'attente (sinon doublon au demarrage)
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

SCENARIO_ID = "tx_life_wq_batch_rescue"
INVARIANT = "I-WQ-EXACTLY-ONCE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_WRITER_THREAD_NAME = "RecoverLand-Writer"
_PENDING_FILENAME = "recoverland_pending.json"

# Plages de fid disjointes : elles servent d'identite pour le comptage de
# conservation, un evenement ne doit apparaitre que d'un seul cote.
_R1_FIDS = range(0, 10)
_R2_FIDS = range(100, 120)
_R3_FIDS = range(200, 215)
_R5_FIRST = range(300, 312)
_R5_SECOND = range(400, 408)


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

def _event(idx: int, session: str):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=None,
        project_fingerprint="tx_rescue_project",
        datasource_fingerprint="ds_rescue",
        layer_id_snapshot="lyr_rescue",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json='{"fid": %d}' % idx,
        operation_type="UPDATE",
        attributes_json='{"changed_only": {"statut": {"old": "A", "new": "B"}}}',
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:2154",
        field_schema_json='[{"name": "statut", "type": "string"}]',
        user_name="tester",
        session_id=session,
        created_at="2026-08-17T10:00:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint="fid:%d" % idx,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _exploding(idx: int, session: str):
    """An event whose WKB cannot be read: blows up inside the write path.

    Subclassing the namedtuple shadows the field descriptor with a raising
    property. `_validate_event` (operation_type / attributes_json /
    created_at) still passes, `_asdict()` still yields the real values
    because a namedtuple serialises by iterating its tuple, and only
    `_event_to_row` — the first statement of `_write_batch_with_retry`,
    outside its own try — raises.
    """
    from recoverland.core.audit_backend import AuditEvent

    class _Exploding(AuditEvent):
        __slots__ = ()

        @property
        def geometry_wkb(self):
            raise RuntimeError(
                "simulated failure while reading a 40 MB WKB"
            )

    return _Exploding(*_event(idx, session))


def _make_journal(root: str, name: str) -> str:
    """One journal per sub-directory: the pending file sits beside it."""
    from recoverland.core.sqlite_schema import initialize_schema

    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.close()
    return path


def _writer_alive() -> int:
    return len([t for t in threading.enumerate() if t.name == _WRITER_THREAD_NAME])


def _journal_fids(db_path: str) -> list:
    """entity_fingerprint values present in the journal, as raw strings."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT entity_fingerprint FROM audit_event").fetchall()
    finally:
        conn.close()
    return [row[0] for row in rows]


def _pending_fids(db_path: str) -> list:
    """entity_fingerprint values present in the pending recovery file."""
    path = os.path.join(os.path.dirname(db_path), _PENDING_FILENAME)
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return ["<unreadable>"]
    if not isinstance(data, list):
        return ["<not-a-list>"]
    return [evt.get("entity_fingerprint") for evt in data if isinstance(evt, dict)]


def _expected(fids) -> list:
    return ["fid:%d" % i for i in fids]


def _settle(wq, limit: float = 3.0) -> None:
    """Let the writer consume what is queued before pushing the next lot.

    `_drain_batch` takes up to 500 events at once, so two enqueues in a row
    would land in ONE batch and the poisoned lot would swallow the healthy
    one. Waiting keeps each lot its own batch, which is what production
    does: commits are seconds apart.
    """
    deadline = time.monotonic() + limit
    while wq.pending_count and time.monotonic() < deadline:
        time.sleep(0.02)
    # The queue is empty, but the batch just taken is still being written
    # (executemany + commit, then the loop's 0.1 s idle wait).
    time.sleep(0.4)


# --------------------------------------------------------------------- #
# Scenario
# --------------------------------------------------------------------- #

def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_rescue_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["writers_before"] = _writer_alive()
    ctx.data["rescue_db"] = _make_journal(tmpdir, "rescue")
    ctx.data["notify_db"] = _make_journal(tmpdir, "notify")

    flog(
        f"tx_life_wq_batch_rescue setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.logger import flog

    flog(f"tx_life_wq_batch_rescue run start: trace_id={ctx.trace_id}", "INFO")

    rescue = ctx.data["rescue_db"]
    notify = ctx.data["notify_db"]

    # ===== R1 / R2 / R3 : le lot empoisonne ============================
    wq = WriteQueue()
    wq.start(rescue)

    ctx.data["r1_accepted"] = wq.enqueue([_event(i, "sain1") for i in _R1_FIDS])
    _settle(wq)
    ctx.data["r1_rows"] = len(_journal_fids(rescue))

    ctx.data["r2_accepted"] = wq.enqueue(
        [_exploding(i, "poison") for i in _R2_FIDS]
    )
    _settle(wq)
    ctx.data["r2_writer_alive"] = _writer_alive() - ctx.data["writers_before"]

    ctx.data["r3_accepted"] = wq.enqueue([_event(i, "sain2") for i in _R3_FIDS])
    _settle(wq)
    ctx.data["r3_writer_alive"] = _writer_alive() - ctx.data["writers_before"]

    wq.stop()
    ctx.data["r_journal"] = _journal_fids(rescue)
    ctx.data["r_pending"] = _pending_fids(rescue)
    ctx.data["r_queued_after_stop"] = wq.pending_count

    # ===== R5 : la notification qui leve ===============================
    # Le lot est commite AVANT l'appel du callback. Le recopier dans le
    # fichier d'attente le ferait reinjecter au demarrage suivant : le
    # journal contiendrait deux fois la meme edition.
    calls = []

    def _flush_callback(n_events: int) -> None:
        calls.append(n_events)
        raise RuntimeError(
            "wrapped C/C++ object of type CommitSignalBridge has been deleted"
        )

    wq2 = WriteQueue()
    wq2.set_flush_callback(_flush_callback)
    wq2.start(notify)
    ctx.data["r5_first_accepted"] = wq2.enqueue(
        [_event(i, "notif1") for i in _R5_FIRST]
    )
    _settle(wq2)
    ctx.data["r5_writer_alive"] = _writer_alive() - ctx.data["writers_before"]
    ctx.data["r5_second_accepted"] = wq2.enqueue(
        [_event(i, "notif2") for i in _R5_SECOND]
    )
    _settle(wq2)
    wq2.stop()
    ctx.data["r5_journal"] = _journal_fids(notify)
    ctx.data["r5_pending"] = _pending_fids(notify)
    ctx.data["r5_callback_calls"] = len(calls)

    flog(
        f"tx_life_wq_batch_rescue run end: trace_id={ctx.trace_id} "
        f"journal={len(ctx.data['r_journal'])} "
        f"pending={len(ctx.data['r_pending'])} "
        f"r5_journal={len(ctx.data['r5_journal'])} "
        f"r5_pending={len(ctx.data['r5_pending'])} "
        f"r5_callbacks={ctx.data['r5_callback_calls']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains, assert_log_absent

    out = []
    d = ctx.data

    journal = list(d.get("r_journal") or [])
    pending = list(d.get("r_pending") or [])
    written = _expected(_R1_FIDS) + _expected(_R3_FIDS)
    poisoned = _expected(_R2_FIDS)

    # ===== R1 : point de depart ========================================
    out.append((
        "r1_healthy_batch_reaches_the_journal",
        d.get("r1_accepted") is True and d.get("r1_rows") == len(_R1_FIDS),
        f"accepted={d.get('r1_accepted')} lignes={d.get('r1_rows')} "
        f"attendu={len(_R1_FIDS)} : la session demarre normalement, sinon les "
        f"mesures suivantes ne veulent rien dire",
    ))

    # ===== R2 : le lot empoisonne ======================================
    out.append((
        "r2_writer_survives_the_broken_batch",
        d.get("r2_writer_alive") == 1,
        f"threads ecrivains vivants={d.get('r2_writer_alive')} apres un lot dont "
        f"la lecture d'un champ leve. Une exception inattendue (MemoryError sur un "
        f"gros WKB, objet libere sous un champ) ne doit pas terminer le "
        f"consommateur : la file deviendrait un trou noir pour tout le reste de la "
        f"session, et enqueue() continuerait a accuser reception.",
    ))
    rescued = [fp for fp in pending if fp in poisoned]
    out.append((
        "r2_broken_batch_is_rescued_to_the_pending_file",
        sorted(rescued) == sorted(poisoned),
        f"{len(rescued)} evenement(s) du lot casse dans "
        f"recoverland_pending.json, attendu={len(poisoned)}. Le lot avait deja "
        f"quitte la file : la liste tenue par la boucle est la seule reference qui "
        f"reste. Si le drainage se faisait dans le try, survivre a l'exception "
        f"detruirait justement les evenements qui l'ont provoquee.",
    ))
    duplicated = sorted({fp for fp in pending if pending.count(fp) > 1})
    out.append((
        "r2_no_event_rescued_twice",
        not duplicated,
        f"evenement(s) presents plusieurs fois dans le fichier d'attente : "
        f"{duplicated[:5]} (n={len(pending)} pour {len(poisoned)} attendus). "
        f"_recover_pending_events reinjecte ce fichier au demarrage suivant : un "
        f"lot sauve deux fois reapparait deux fois dans le journal, et un REWIND "
        f"rejoue deux fois la meme edition.",
    ))

    # ===== R3 : la file n'est pas un trou noir =========================
    out.append((
        "r3_queue_still_accepts_after_the_incident",
        d.get("r3_accepted") is True and d.get("r3_writer_alive") == 1,
        f"accepted={d.get('r3_accepted')} ecrivains={d.get('r3_writer_alive')} : "
        f"l'edition qui suit l'incident doit continuer d'etre enregistree, sinon "
        f"l'utilisateur travaille une journee entiere en croyant etre protege",
    ))
    missing = [fp for fp in written if fp not in journal]
    out.append((
        "r3_healthy_batches_are_all_in_the_journal",
        not missing,
        f"evenement(s) sains absents du journal : {missing[:5]} "
        f"(journal={len(journal)} attendu={len(written)}). Un lot casse ne doit "
        f"pas emporter les lots voisins.",
    ))

    # ===== R4 : conservation ===========================================
    both = sorted(set(journal) & set(pending))
    out.append((
        "r4_no_event_in_both_places",
        not both,
        f"evenement(s) a la fois dans le journal et dans le fichier d'attente : "
        f"{both[:5]}. Le fichier d'attente est rejoue au demarrage : tout "
        f"recouvrement est un doublon garanti dans le journal.",
    ))
    accepted_total = len(_R1_FIDS) + len(_R2_FIDS) + len(_R3_FIDS)
    out.append((
        "r4_every_accepted_event_is_somewhere",
        len(journal) + len(pending) == accepted_total
        and d.get("r_queued_after_stop") == 0,
        f"journal={len(journal)} + attente={len(pending)} = "
        f"{len(journal) + len(pending)}, attendu={accepted_total} "
        f"(encore en file apres stop: {d.get('r_queued_after_stop')}). Trois lots "
        f"acceptes, un seul casse : la somme doit etre exacte, sans perte et sans "
        f"doublon.",
    ))

    # ===== R5 : la notification qui leve ===============================
    r5_journal = list(d.get("r5_journal") or [])
    r5_pending = list(d.get("r5_pending") or [])
    r5_expected = _expected(_R5_FIRST) + _expected(_R5_SECOND)
    out.append((
        "r5_committed_batches_are_not_copied_to_pending",
        not r5_pending,
        f"le fichier d'attente contient {len(r5_pending)} evenement(s) alors que "
        f"les deux lots sont commites en base : {r5_pending[:5]}. Le callback de "
        f"flush n'est qu'une notification (emission Qt vers un pont que le plugin "
        f"a pu detruire) appelee APRES le commit ; traiter son echec comme un "
        f"echec d'ecriture ferait reinjecter au demarrage suivant des evenements "
        f"deja presents.",
    ))
    out.append((
        "r5_both_batches_written_despite_the_failing_callback",
        sorted(r5_journal) == sorted(r5_expected)
        and d.get("r5_callback_calls") == 2
        and d.get("r5_writer_alive") == 1,
        f"journal={len(r5_journal)} attendu={len(r5_expected)} "
        f"callbacks={d.get('r5_callback_calls')} attendu=2 "
        f"ecrivains={d.get('r5_writer_alive')} attendu=1 : une notification qui "
        f"echoue a chaque lot ne doit ni arreter l'enregistrement ni faire perdre "
        f"un seul evenement",
    ))

    # ===== Traces ======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"WriteQueue: writer loop error, staying alive, "
        rf"{len(_R2_FIDS)} event\(s\) of the failed batch saved to pending",
        name="rescue_is_logged_with_the_saved_count",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"WriteQueue: flush callback failed after \d+ events were committed",
        name="callback_failure_is_logged_as_a_notification_failure",
        min_count=2,
    ))
    out.append(assert_log_absent(
        ctx.records,
        r"WriteQueue: fatal writer error",
        name="no_fatal_writer_error",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_life_wq_batch_rescue.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Gardes de source ============================================
    # Les deux proprietes structurelles derrière R2 et R5. Elles sont
    # verifiees dans le texte parce qu'une refonte de la boucle peut les
    # perdre sans qu'aucune mesure ci-dessus ne change : le lot casse
    # partirait dans le fichier d'attente par le `finally` d'arret, et le
    # doublon de R5 n'apparaitrait qu'au demarrage suivant, hors scenario.
    src = (_PLUGIN_ROOT / "core" / "write_queue.py").read_text(
        encoding="utf-8", errors="replace")
    loop_start = src.find("while not self._stop_event.is_set():")
    drain_at = src.find("batch = self._drain_batch()", loop_start)
    try_at = src.find("try:", loop_start)
    out.append((
        "batch_is_drained_outside_the_guard",
        0 < drain_at < try_at,
        f"drainage a l'offset {drain_at}, try a {try_at} (boucle a "
        f"{loop_start}) : `batch = self._drain_batch()` doit preceder le `try` de "
        f"la boucle. get_nowait() a deja retire ces evenements de la file, donc "
        f"draîner dans le try ferait detruire par le gestionnaire d'erreur le lot "
        f"meme qui l'a declenche.",
    ))
    write_at = src.find("self._write_batch_with_retry(conn, batch)", loop_start)
    clear_at = src.find("batch = []", write_at)
    except_at = src.find("except Exception as loop_err", write_at)
    out.append((
        "handed_over_batch_reference_is_dropped",
        0 < write_at < clear_at < except_at,
        f"appel a {write_at}, `batch = []` a {clear_at}, gestionnaire a "
        f"{except_at} : apres _write_batch_with_retry le lot est soit commite, "
        f"soit deja bascule en attente par le helper. Garder la reference ferait "
        f"sauver une seconde fois le meme lot si la suite de l'iteration (point "
        f"de controle WAL) echouait — donc un doublon au demarrage suivant.",
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
