# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_life_wq_batch_rescue - RecoverLand validation runtime.

Invariant: I-WQ-EXACTLY-ONCE (un evenement accepte finit dans le journal
OU dans un stockage durable qui est rejoue au demarrage (l'outbox de son
journal) ou conserve (la quarantaine), exactement une fois, jamais dans
les deux et jamais nulle part). La quarantaine est une lettre morte
conservee : elle n'est pas rejouee, donc la sante de la capture doit
l'exposer (health().quarantined_events, capture_health DEGRADED) ; sans
cela, un evenement qui y dort n'est nulle part pour l'utilisateur.

Pourquoi ce scenario existe
---------------------------
`tx_life_wq_dead_writer` prouve le contraire de son epoque : il decrit un
thread ecrivain qui MEURT a la premiere exception inattendue et une file
devenue un trou noir. Ce defaut est corrige (la boucle de
`WriteQueue._writer_loop` rattrape `Exception` autour de chaque iteration
et reste en vie), mais la correction ouvre deux questions que ce scenario
est le seul a poser, et qui pesent plus lourd que le defaut d'origine :

1. Le LOT qui a declenche l'erreur a deja ete sorti de la file : si
   survivre a l'exception le detruisait, on aurait le thread vivant et la
   donnee perdue. Le lot casse doit donc etre sauve, quelque part de
   durable, et les evenements sains qu'il contient ne doivent pas etre
   sacrifies avec lui.

2. Sauver un lot deja commite serait aussi grave, dans l'autre sens : un
   stockage rejoue au demarrage suivant ferait reapparaitre en double un
   lot sauve en double. Un UPDATE compte deux fois, un REWIND rejoue deux
   fois, et `rw_dedup` voit une chaine qu'aucune edition n'a produite.

Hypotheses revisees (v7)
------------------------
Le schema v7 a change l'endroit ou vit un lot accepte, pas la propriete
exigee. `enqueue()` ecrit (fsync) le lot dans l'outbox du journal
(`<journal>.outbox/`) AVANT de le poser dans la file en RAM et rend une
issue explicite (`EnqueueOutcome`). L'outbox est le stockage rejoue : un
lot non acquitte y est reintegre au demarrage, idempotent par
`event_uid`. Un groupe de lots que le journal refuse 3 fois de facon non
reessayable est isole (`_isolate_poison`) : chaque lot est retente seul,
puis chaque evenement seul ; les evenements acceptes vont au journal,
seuls ceux que le journal refuse sont ecrits (fsync) dans
`<journal>.outbox/quarantine`. Le lot n'est acquitte dans l'outbox que si
TOUS ses evenements sont au journal ou en quarantaine sur le disque ;
sinon il reste dans l'outbox (rejoue au prochain demarrage), il est
compte dans health().degraded_events et annonce en CRITICAL
(`wq_poison_parked`). Le fichier d'attente (`recoverland_pending.json`)
n'est plus un stockage de secours de l'ecrivain.

Les assertions qui dependaient de l'ancien stockage sont reecrites
(commentaire "Hypothesis revised (v7)") et mesurent la propriete APRES un
redemarrage simule (`durability.restart_integrate`) : les evenements
sains sont au journal exactement une fois, seuls les evenements que
l'injection rend illisibles sont en quarantaine, exactement une fois, et
rien ne reste dans l'outbox. Ce que le disque contient au moment de
l'accuse de reception est prouve sur une COPIE de l'outbox prise juste
apres le retour d'`enqueue()` : un `restart_integrate` dans le meme
processus ne le prouve pas (la memoire du processus survit).

Le point de rupture injecte
---------------------------
Aucun code produit n'est modifie. Le scenario pousse des evenements dont
la lecture d'un champ leve DANS l'ecrivain : `geometry_wkb` porte une
sous-classe de `bytes` dont `__conform__` leve `RuntimeError` au moment
ou SQLite lit la valeur pour la lier a l'INSERT, exactement comme un
`MemoryError` sur un WKB de plusieurs dizaines de Mo. L'erreur n'est pas
un `sqlite3.Error`. Tout ce qui precede l'ecrivain passe : validation
(`validate_event_row` ne lit pas la geometrie), serialisation dans
l'outbox (`event_to_record` encode les octets en base64). La copie
durable est donc saine : l'injection ne vit qu'en RAM. Avant v7, la
rupture etait une propriete qui masquait le champ du namedtuple ; la v7
construit la ligne par `event_row()` = `_asdict()`, qui ne passe jamais
par la propriete (mesure : l'ancienne injection n'explosait plus).

Le lot R2 est MIXTE, parce que l'isolement descend maintenant jusqu'a
l'evenement : 40 evenements dont les 20 de `_R2_POISON` sont illisibles
(le premier, le dernier et des voisins adjacents compris) et 20 sont
sains, avec une vraie geometrie. Un lot tout empoisonne ne distinguerait
pas "seuls les illisibles en quarantaine" de "tout le lot en quarantaine".

R7 et R8 injectent une erreur HORS de l'ecriture du lot : la methode
`ack` de l'outbox de la generation courante (l'instance, pas la classe)
est remplacee par une version qui leve une fois.

Ce que ce scenario torture (journaux SQLite reels, aucun objet QGIS) :
    R1  lot sain -> journal
    R2  lot mixte qui explose en ecriture -> ecrivain vivant ; seuls les
        evenements illisibles en quarantaine, UNE seule fois, octets
        intacts ; les evenements sains du meme lot au journal ; le lot
        etait sur le disque des l'accuse de reception ; la sante expose
        la quarantaine
    R3  lot sain pousse APRES l'incident -> toujours ecrit (la file n'est
        pas devenue un trou noir)
    R4  conservation apres redemarrage : journal == les 45 evenements
        sains, quarantaine == les 20 illisibles, sans recouvrement, sans
        doublon, rien laisse dans l'outbox ; la quarantaine est encore
        exposee apres le redemarrage
    R5  callback de notification qui leve : le lot commite N'EST PAS
        recopie dans le fichier d'attente, il est acquitte dans l'outbox,
        et un redemarrage ne le rejoue pas en double
    R6  quarantaine impossible a ecrire (un FICHIER porte son nom) : le
        lot empoisonne n'est PAS acquitte, il reste dans l'outbox et
        revient au journal au redemarrage ; il est compte degrade et
        annonce en CRITICAL ; l'ecrivain continue
    R7  exception hors de l'ecriture (ack qui leve une fois) : ecrivain
        vivant, degraded puis running, rien perdu, compteurs revenus a
        zero
    R8  la meme exception pendant l'isolement d'un lot empoisonne : rien
        perdu, et la quarantaine ne recoit pas le lot deux fois
"""
from __future__ import annotations

import ast
import base64
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import time
import traceback
from pathlib import Path

SCENARIO_ID = "tx_life_wq_batch_rescue"
INVARIANT = "I-WQ-EXACTLY-ONCE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_WRITER_THREAD_NAME = "RecoverLand-Writer"
_PENDING_FILENAME = "recoverland_pending.json"

# Plages de fid disjointes : elles servent d'identite pour le comptage de
# conservation, un evenement ne doit apparaitre que d'un seul cote.
_R1_FIDS = range(0, 10)
_R2_FIDS = range(100, 140)
# Les evenements que l'injection rend illisibles : le premier et le
# dernier du lot, des suites adjacentes (un decalage d'indice dans
# l'isolement les confondrait avec leurs voisins sains) et des isoles.
_R2_POISON = frozenset({
    100, 101, 102, 105, 107, 108, 111, 114, 115, 116,
    117, 120, 123, 126, 127, 130, 133, 136, 138, 139,
})
_R3_FIDS = range(200, 215)
_R5_FIRST = range(300, 312)
_R5_SECOND = range(400, 408)
_R6_FIDS = range(500, 512)
_R6_POISON = frozenset({500, 505, 506, 511})
_R6_FOLLOW = range(600, 606)
_R7_FIRST = range(700, 712)
_R7_SECOND = range(800, 808)
_R8_FIDS = range(900, 906)
_R8_POISON = frozenset({900, 903})

_INJECTED_ERROR = "simulated failure while reading a 40 MB WKB"
_ACK_ERROR = "simulated failure while releasing an outbox segment"


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


class _UnreadableWkb(bytes):
    """WKB whose value cannot be produced when the journal binds it.

    sqlite3 adapts every parameter that is not an exact int/float/str/
    bytearray; for an object that defines ``__conform__`` it calls it, and
    an exception other than TypeError propagates as is out of
    ``executemany``. The bytes themselves stay intact, so the outbox and
    the quarantine can still serialise them (``event_to_record`` encodes
    ``bytes(value)`` in base64).
    """

    def __conform__(self, protocol):
        raise RuntimeError(_INJECTED_ERROR)


def _wkb_payload(idx: int) -> bytes:
    return b"\x01\x03\x00\x00\x00" + idx.to_bytes(4, "little") + b"\x00" * 23


def _polygon(idx: int, session: str):
    """A healthy event with a real geometry, the neighbour of a poisoned one."""
    return _event(idx, session)._replace(
        geometry_wkb=_wkb_payload(idx), geometry_type="Polygon")


def _exploding(idx: int, session: str):
    """An event whose WKB cannot be read: blows up inside the writer only.

    ``enqueue`` validates operation_type / attributes_json / created_at and
    serialises the record through ``_asdict()`` (tuple iteration): nothing
    there reads the value the way SQLite does. The writer builds its row
    with ``event_row()`` and the INSERT binds ``geometry_wkb``: that bind
    raises RuntimeError, a non-SQLite exception, before any row commits.
    """
    return _event(idx, session)._replace(
        geometry_wkb=_UnreadableWkb(_wkb_payload(idx)),
        geometry_type="Polygon",
    )


def _mixed_batch(fids, poison, session: str) -> list:
    return [_exploding(i, session) if i in poison else _polygon(i, session)
            for i in fids]


def _make_journal(root: str, name: str) -> str:
    """One journal per sub-directory: its outbox sits beside it."""
    from recoverland.core.sqlite_schema import initialize_schema

    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.close()
    return path


def _writer_alive() -> int:
    return len([t for t in threading.enumerate() if t.name == _WRITER_THREAD_NAME])


def _own_writer_alive(wq) -> bool:
    """THIS queue's writer: another scenario may leave writers of its own."""
    own = getattr(wq, "_thread", None)
    return bool(own is not None and own.is_alive())


def _outcome_name(res) -> str:
    return getattr(res, "name", repr(res))


def _journal_fids(db_path: str) -> list:
    """entity_fingerprint values present in the journal, as raw strings."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute("SELECT entity_fingerprint FROM audit_event").fetchall()
    finally:
        conn.close()
    return [row[0] for row in rows]


def _journal_geoms(db_path: str) -> dict:
    """entity_fingerprint -> geometry_wkb bytes as stored in the journal."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT entity_fingerprint, geometry_wkb FROM audit_event").fetchall()
    finally:
        conn.close()
    return {fp: (bytes(g) if g is not None else None) for fp, g in rows}


def _geoms_intact(geoms: dict, fids) -> int:
    return sum(1 for i in fids if geoms.get("fid:%d" % i) == _wkb_payload(i))


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


def _quarantine_records(db_path: str) -> list:
    """Every event record preserved in the journal's outbox quarantine.

    Reads all the files of ``<journal>.outbox/quarantine``, whatever wrote
    them (poison batch, invalid batch, torn segment): an event counted here
    is one the plugin kept on disk instead of indexing it.
    """
    from recoverland.core.capture_outbox import QUARANTINE_DIRNAME, outbox_dir_for

    qdir = os.path.join(outbox_dir_for(db_path), QUARANTINE_DIRNAME)
    if not os.path.isdir(qdir):
        return []
    out = []
    for name in sorted(os.listdir(qdir)):
        path = os.path.join(qdir, name)
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    text = raw.strip()
                    if not text:
                        continue
                    try:
                        obj = json.loads(text)
                    except ValueError:
                        out.append({"entity_fingerprint": "<unreadable>",
                                    "_file": name})
                        continue
                    rec = obj.get("e") if isinstance(obj, dict) else None
                    if not isinstance(rec, dict):
                        out.append({"entity_fingerprint": "<no-event>",
                                    "_file": name})
                        continue
                    rec = dict(rec)
                    rec["_file"] = name
                    rec["_reason"] = obj.get("reason", "")
                    out.append(rec)
        except OSError:
            out.append({"entity_fingerprint": "<unreadable>", "_file": name})
    return out


def _outbox_fids(db_path: str) -> list:
    """entity_fingerprint of every event line still on disk in the outbox."""
    from recoverland.core.capture_outbox import list_segments, outbox_dir_for

    out = []
    for _seq, path in list_segments(outbox_dir_for(db_path)):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    try:
                        line = json.loads(raw)
                    except ValueError:
                        continue
                    if line.get("t") == "ev" and isinstance(line.get("e"), dict):
                        out.append(line["e"].get("entity_fingerprint"))
        except OSError:
            out.append("<unreadable>")
    return out


def _expected(fids) -> list:
    return ["fid:%d" % i for i in fids]


def _settle(wq, limit: float = 3.0) -> None:
    """Let the writer consume what is queued before pushing the next lot.

    `_take_batches` groups up to 500 events into one transaction, so two
    enqueues in a row would land in ONE group; waiting keeps each lot its
    own group, which is what production does: commits are seconds apart.
    `pending_count` counts the lot in flight too (queued + inflight), so
    this also waits through the retries of a failing lot until it is
    isolated.
    """
    deadline = time.monotonic() + limit
    while wq.pending_count and time.monotonic() < deadline:
        time.sleep(0.02)
    # The queue is empty, but the group just taken is still being written
    # (executemany + commit, then the loop's 0.1 s idle wait).
    time.sleep(0.4)


def _wait_state(wq, wanted: str, limit: float) -> tuple:
    """Poll the writer state until *wanted*; returns (reached, states seen)."""
    seen = []
    deadline = time.monotonic() + limit
    while True:
        state = wq.state.value
        if not seen or seen[-1] != state:
            seen.append(state)
        if state == wanted:
            return True, seen
        if time.monotonic() >= deadline:
            return False, seen
        time.sleep(0.005)


def _capture_health(health) -> dict:
    """What the status bar would show for this writer health."""
    from recoverland.core.capture_health import evaluate_capture_health

    return evaluate_capture_health(
        journal_open=True, tracker_active=True, writer=health)._asdict()


def _names_quarantine(cap: dict, n: int) -> bool:
    return any("quarantaine" in r and str(n) in r for r in (cap.get("reasons") or []))


def _fail_ack_once(outbox, calls: list) -> None:
    """Make THIS outbox instance's ack raise once, then behave normally."""
    real_ack = outbox.ack

    def _ack(seq, n_events=0):
        calls.append(int(n_events or 0))
        if len(calls) == 1:
            raise RuntimeError(_ACK_ERROR)
        return real_ack(seq, n_events)

    outbox.ack = _ack


def _mark(ctx, phase: str, edge: str) -> None:
    from recoverland.core.logger import flog

    flog(f"tx_life_wq_batch_rescue phase={phase} {edge} trace_id={ctx.trace_id}",
         "INFO")


def _window(records, trace_id: str, phase: str) -> tuple:
    """Log records between the begin and end markers of *phase*."""
    begin = f"tx_life_wq_batch_rescue phase={phase} begin trace_id={trace_id}"
    end = f"tx_life_wq_batch_rescue phase={phase} end trace_id={trace_id}"
    start = None
    for i, rec in enumerate(records):
        raw = getattr(rec, "raw", "")
        if start is None and begin in raw:
            start = i
        elif start is not None and end in raw:
            return list(records[start + 1:i]), True
    return [], False


def _in_window(records, trace_id, phase, pattern, name, min_count, max_count=None):
    from scripts.validation.assert_log import assert_log_contains

    window, found = _window(records, trace_id, phase)
    if not found:
        return (name, False, f"fenetre de log de la phase {phase} introuvable")
    label, ok, msg = assert_log_contains(window, pattern, name=name,
                                         min_count=min_count, max_count=max_count)
    return (label, ok, f"phase {phase}: {msg}")


def _absent_in_window(records, trace_id, phase, pattern, name):
    from scripts.validation.assert_log import assert_log_absent

    window, found = _window(records, trace_id, phase)
    if not found:
        return (name, False, f"fenetre de log de la phase {phase} introuvable")
    label, ok, msg = assert_log_absent(window, pattern, name=name)
    return (label, ok, f"phase {phase}: {msg}")


# --------------------------------------------------------------------- #
# Source helpers (gardes de source)
# --------------------------------------------------------------------- #

def _find_method(tree, cls: str, name: str):
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == name:
                    return item
    return None


def _method_src(src: str, tree, cls: str, name: str) -> str:
    node = _find_method(tree, cls, name)
    if node is None:
        return ""
    return ast.get_source_segment(src, node) or ""


def _handler_names(handler) -> set:
    t = handler.type
    if t is None:
        return {"<bare>"}
    elts = t.elts if isinstance(t, ast.Tuple) else [t]
    return {e.id if isinstance(e, ast.Name) else ast.unparse(e) for e in elts}


def _except_exception_returns(func) -> bool:
    """A write-path `except Exception` in *func* returns (False, False), no raise."""
    if func is None:
        return False
    for node in ast.walk(func):
        if not (isinstance(node, ast.ExceptHandler)
                and "Exception" in _handler_names(node)):
            continue
        raises = any(isinstance(n, ast.Raise) for n in ast.walk(node))
        falsy = any(
            isinstance(r, ast.Return) and isinstance(r.value, ast.Tuple)
            and r.value.elts
            and all(isinstance(v, ast.Constant) and v.value is False
                    for v in r.value.elts)
            for r in node.body)
        if falsy and not raises:
            return True
    return False


def _is_attr_call(node, attr: str) -> bool:
    return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr == attr)


def _call_is_guarded(func, attr: str) -> bool:
    """Every call to ``self.<attr>(...)`` in *func* sits in a try that catches Exception."""
    if func is None:
        return False
    calls = {id(n) for n in ast.walk(func) if _is_attr_call(n, attr)}
    guarded = set()
    for node in ast.walk(func):
        if not isinstance(node, ast.Try):
            continue
        if not any(names & {"Exception", "BaseException", "<bare>"}
                   for names in map(_handler_names, node.handlers)):
            continue
        for stmt in node.body:
            guarded.update(id(s) for s in ast.walk(stmt) if _is_attr_call(s, attr))
    return bool(calls) and calls <= guarded


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


def _run_rescue(ctx):
    """R1 / R2 / R3 on one journal, then R4 after a simulated restart."""
    from recoverland.core.capture_outbox import outbox_dir_for
    from recoverland.core.write_queue import WriteQueue
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events, pending_events,
        restart_integrate,
    )

    d = ctx.data
    rescue = d["rescue_db"]
    # Prepared before R2 so that the copy below costs only a file copy.
    disk_copy = _make_journal(d["tmpdir"], "r2_disk_copy")

    wq = WriteQueue()
    wq.start(rescue)
    try:
        d["r1_accepted"] = _outcome_name(
            wq.enqueue([_event(i, "sain1") for i in _R1_FIDS]))
        _settle(wq)
        d["r1_rows"] = len(_journal_fids(rescue))

        _mark(ctx, "R2", "begin")
        d["r2_accepted"] = _outcome_name(
            wq.enqueue(_mixed_batch(_R2_FIDS, _R2_POISON, "poison")))
        # What the disk holds at acknowledgement time: the writer cannot
        # ack this batch before three failed attempts and their backoff
        # (0.2 s + 0.4 s), a copy of the outbox takes milliseconds.
        try:
            shutil.copytree(outbox_dir_for(rescue), outbox_dir_for(disk_copy))
            d["r2_copy_error"] = ""
        except OSError as exc:
            d["r2_copy_error"] = f"{type(exc).__name__}: {exc}"
        _settle(wq, 8.0)
        _mark(ctx, "R2", "end")
        d["r2_writer_alive"] = _writer_alive() - d["writers_before"]
        d["r2_health"] = wq.health()._asdict()

        d["r3_accepted"] = _outcome_name(
            wq.enqueue([_event(i, "sain2") for i in _R3_FIDS]))
        _settle(wq)
        d["r3_writer_alive"] = _writer_alive() - d["writers_before"]
        health = wq.health()
        d["r3_health"] = health._asdict()
        d["r3_capture"] = _capture_health(health)
    finally:
        wq.stop()

    d["r_journal"] = _journal_fids(rescue)
    d["r2_healthy_geoms_intact"] = _geoms_intact(
        _journal_geoms(rescue), [i for i in _R2_FIDS if i not in _R2_POISON])
    d["r_pending"] = _pending_fids(rescue)
    d["r_outbox_after_stop"] = outbox_events(rescue)

    # Redemarrage simule : import du fichier d'attente puis rejeu de
    # l'outbox, comme au prochain lancement du plugin. C'est la que se
    # verrait un lot sauve deux fois ou un lot en quarantaine rejoue.
    d["r_restart"] = restart_integrate(rescue)
    d["r_journal_after_restart"] = _journal_fids(rescue)
    d["r_rows_after_restart"] = count_rows(rescue)
    d["r_dup_uids"] = duplicate_uids(rescue)
    d["r_outbox_after_restart"] = outbox_events(rescue)
    d["r_pending_after_restart"] = pending_events(rescue)
    qrecs = _quarantine_records(rescue)
    d["r_quarantine"] = [r.get("entity_fingerprint") for r in qrecs]
    d["r_quarantine_files"] = sorted({r.get("_file") for r in qrecs})
    d["r_quarantine_reasons"] = sorted(
        {r.get("_reason") for r in qrecs if r.get("_reason")})
    poisoned_wkb = {
        "fid:%d" % i: "b64:" + base64.b64encode(_wkb_payload(i)).decode("ascii")
        for i in _R2_POISON
    }
    d["r_quarantine_payload_intact"] = sum(
        1 for r in qrecs
        if r.get("entity_fingerprint") in poisoned_wkb
        and r.get("geometry_wkb") == poisoned_wkb[r.get("entity_fingerprint")])
    d["r_quarantine_blames_injection"] = sum(
        1 for r in qrecs
        if r.get("entity_fingerprint") in poisoned_wkb
        and _INJECTED_ERROR in str(r.get("_reason", "")))

    # The copy taken at acknowledgement time, integrated on its own.
    d["r2_copy_restart"] = restart_integrate(disk_copy)
    d["r2_copy_fids"] = _journal_fids(disk_copy)
    d["r2_copy_geoms_intact"] = _geoms_intact(_journal_geoms(disk_copy), _R2_FIDS)
    d["r2_copy_dup_uids"] = duplicate_uids(disk_copy)

    # What the next start of the plugin shows about the dead letter.
    wq_next = WriteQueue()
    wq_next.start(rescue)
    try:
        _wait_state(wq_next, "running", 3.0)
        health = wq_next.health()
        d["r_next_health"] = health._asdict()
        d["r_next_capture"] = _capture_health(health)
    finally:
        wq_next.stop()


def _run_notify(ctx):
    """R5 : la notification qui leve."""
    from recoverland.core.write_queue import WriteQueue
    from scripts.validation.durability import (
        duplicate_uids, outbox_events, restart_integrate,
    )

    d = ctx.data
    notify = d["notify_db"]
    # Le lot est commite AVANT l'appel du callback. Le recopier dans un
    # stockage rejoue au demarrage (fichier d'attente, ou outbox non
    # acquittee) le ferait reinjecter : le journal contiendrait deux fois
    # la meme edition si l'idempotence venait a manquer.
    calls = []

    def _flush_callback(n_events: int) -> None:
        calls.append(n_events)
        raise RuntimeError(
            "wrapped C/C++ object of type CommitSignalBridge has been deleted"
        )

    wq2 = WriteQueue()
    wq2.set_flush_callback(_flush_callback)
    wq2.start(notify)
    try:
        d["r5_first_accepted"] = _outcome_name(wq2.enqueue(
            [_event(i, "notif1") for i in _R5_FIRST]
        ))
        _settle(wq2)
        d["r5_writer_alive"] = _writer_alive() - d["writers_before"]
        d["r5_second_accepted"] = _outcome_name(wq2.enqueue(
            [_event(i, "notif2") for i in _R5_SECOND]
        ))
        _settle(wq2)
    finally:
        wq2.stop()
    d["r5_journal"] = _journal_fids(notify)
    d["r5_pending"] = _pending_fids(notify)
    d["r5_callback_calls"] = len(calls)
    d["r5_outbox_after_stop"] = outbox_events(notify)
    d["r5_restart"] = restart_integrate(notify)
    d["r5_journal_after_restart"] = _journal_fids(notify)
    d["r5_dup_uids"] = duplicate_uids(notify)


def _run_parked(ctx):
    """R6 : la quarantaine ne peut pas etre ecrite."""
    from recoverland.core.capture_outbox import QUARANTINE_DIRNAME, outbox_dir_for
    from recoverland.core.write_queue import WriteQueue
    from scripts.validation.durability import (
        duplicate_uids, outbox_events, restart_integrate,
    )

    d = ctx.data
    db = _make_journal(d["tmpdir"], "parked")
    odir = outbox_dir_for(db)
    os.makedirs(odir, exist_ok=True)
    blocker = os.path.join(odir, QUARANTINE_DIRNAME)
    with open(blocker, "w", encoding="utf-8") as fh:
        fh.write("a FILE where the quarantine directory should be\n")

    wq = WriteQueue()
    wq.start(db)
    _mark(ctx, "R6", "begin")
    try:
        d["r6_accepted"] = _outcome_name(
            wq.enqueue(_mixed_batch(_R6_FIDS, _R6_POISON, "parked")))
        _settle(wq, 8.0)
        health = wq.health()
        d["r6_health"] = health._asdict()
        d["r6_capture"] = _capture_health(health)
        d["r6_journal_after_park"] = _journal_fids(db)
        d["r6_follow_accepted"] = _outcome_name(
            wq.enqueue([_event(i, "parked_next") for i in _R6_FOLLOW]))
        _settle(wq)
        d["r6_follow_state"] = wq.state.value
        d["r6_writer_alive"] = _own_writer_alive(wq)
        d["r6_journal_before_stop"] = _journal_fids(db)
    finally:
        wq.stop()
        _mark(ctx, "R6", "end")
    d["r6_blocker_still_a_file"] = os.path.isfile(blocker)
    d["r6_quarantine_after_stop"] = [
        r.get("entity_fingerprint") for r in _quarantine_records(db)]
    d["r6_outbox_fids_after_stop"] = _outbox_fids(db)
    d["r6_outbox_after_stop"] = outbox_events(db)
    # The fault goes away (the injected poison lived in RAM only, the
    # blocking file is removed): the next start must bring the parked
    # batch back from the outbox.
    try:
        os.remove(blocker)
    except OSError as exc:
        d["r6_blocker_remove_error"] = str(exc)
    d["r6_restart"] = restart_integrate(db)
    d["r6_journal_after_restart"] = _journal_fids(db)
    d["r6_dup_uids"] = duplicate_uids(db)
    d["r6_outbox_after_restart"] = outbox_events(db)


def _run_ack_error(ctx):
    """R7 : une exception hors de l'ecriture du lot (ack qui leve une fois)."""
    from recoverland.core.write_queue import WriteQueue
    from scripts.validation.durability import (
        duplicate_uids, outbox_events, restart_integrate,
    )

    d = ctx.data
    db = _make_journal(d["tmpdir"], "ack_error")
    wq = WriteQueue()
    wq.start(db)
    calls = []
    _mark(ctx, "R7", "begin")
    try:
        _wait_state(wq, "running", 3.0)
        _fail_ack_once(wq._outbox, calls)
        d["r7_first_accepted"] = _outcome_name(
            wq.enqueue([_event(i, "ack1") for i in _R7_FIRST]))
        d["r7_reached_degraded"], states = _wait_state(wq, "degraded", 3.0)
        d["r7_alive_after_error"] = _own_writer_alive(wq)
        d["r7_first_in_journal"] = len(
            set(_journal_fids(db)) & set(_expected(_R7_FIRST)))
        d["r7_second_accepted"] = _outcome_name(
            wq.enqueue([_event(i, "ack2") for i in _R7_SECOND]))
        d["r7_reached_running"], states2 = _wait_state(wq, "running", 3.0)
        d["r7_states"] = states + states2[1:]
        d["r7_alive_after_second"] = _own_writer_alive(wq)
        time.sleep(0.3)
        d["r7_health_after"] = wq.health()._asdict()
        d["r7_pending_after"] = wq.pending_count
        d["r7_drained"] = wq.wait_until_drained(1.0)
        d["r7_journal_before_stop"] = _journal_fids(db)
        d["r7_ack_calls"] = list(calls)
    finally:
        wq.stop()
        _mark(ctx, "R7", "end")
    d["r7_outbox_fids_after_stop"] = _outbox_fids(db)
    d["r7_restart"] = restart_integrate(db)
    d["r7_journal_after_restart"] = _journal_fids(db)
    d["r7_dup_uids"] = duplicate_uids(db)
    d["r7_outbox_after_restart"] = outbox_events(db)


def _run_ack_error_isolation(ctx):
    """R8 : la meme exception, pendant l'isolement d'un lot empoisonne."""
    from recoverland.core.write_queue import WriteQueue
    from scripts.validation.durability import (
        duplicate_uids, outbox_events, restart_integrate,
    )

    d = ctx.data
    db = _make_journal(d["tmpdir"], "ack_error_isolation")
    wq = WriteQueue()
    wq.start(db)
    calls = []
    _mark(ctx, "R8", "begin")
    try:
        _wait_state(wq, "running", 3.0)
        # The first ack this outbox sees is the one of the isolation: the
        # poisoned batch is never acked by a group write.
        _fail_ack_once(wq._outbox, calls)
        d["r8_accepted"] = _outcome_name(
            wq.enqueue(_mixed_batch(_R8_FIDS, _R8_POISON, "ack_iso")))
        _settle(wq, 10.0)
        d["r8_alive"] = _own_writer_alive(wq)
        d["r8_health"] = wq.health()._asdict()
        d["r8_journal_before_stop"] = _journal_fids(db)
        d["r8_ack_calls"] = list(calls)
    finally:
        wq.stop()
        _mark(ctx, "R8", "end")
    d["r8_restart"] = restart_integrate(db)
    d["r8_journal_after_restart"] = _journal_fids(db)
    d["r8_dup_uids"] = duplicate_uids(db)
    d["r8_outbox_after_restart"] = outbox_events(db)
    d["r8_quarantine"] = [r.get("entity_fingerprint") for r in _quarantine_records(db)]


_CASES = (
    ("rescue", _run_rescue),
    ("notify", _run_notify),
    ("parked", _run_parked),
    ("ack_error", _run_ack_error),
    ("ack_error_isolation", _run_ack_error_isolation),
)


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_life_wq_batch_rescue run start: trace_id={ctx.trace_id}", "INFO")
    crashes = {}
    for name, fn in _CASES:
        try:
            fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            crashes[name] = (f"{type(exc).__name__}: {exc} | "
                             f"{traceback.format_exc()[-900:]}")
            flog(f"tx_life_wq_batch_rescue case={name} crashed: {exc}", "ERROR")
    ctx.data["crashes"] = crashes
    d = ctx.data
    flog(
        f"tx_life_wq_batch_rescue run end: trace_id={ctx.trace_id} "
        f"journal={len(d.get('r_journal') or [])} "
        f"journal_after_restart={len(d.get('r_journal_after_restart') or [])} "
        f"quarantine={len(d.get('r_quarantine') or [])} "
        f"pending={len(d.get('r_pending') or [])} "
        f"outbox_after_stop={d.get('r_outbox_after_stop')} "
        f"r5_journal={len(d.get('r5_journal') or [])} "
        f"r5_callbacks={d.get('r5_callback_calls')} "
        f"r6_outbox_after_stop={d.get('r6_outbox_after_stop')} "
        f"r7_states={d.get('r7_states')} "
        f"r8_quarantine={len(d.get('r8_quarantine') or [])} "
        f"crashes={sorted(crashes)}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains, assert_log_absent

    out = []
    d = ctx.data
    records = ctx.records
    tid = ctx.trace_id

    crashes = d.get("crashes") or {}
    out.append((
        "every_case_ran_to_completion",
        "crashes" in d and not crashes,
        f"cas interrompus par une exception : {crashes}",
    ))

    journal = list(d.get("r_journal") or [])
    journal_after = list(d.get("r_journal_after_restart") or [])
    quarantine = list(d.get("r_quarantine") or [])
    written = _expected(_R1_FIDS) + _expected(_R3_FIDS)
    poisoned = _expected(sorted(_R2_POISON))
    r2_healthy = _expected([i for i in _R2_FIDS if i not in _R2_POISON])
    healthy = _expected(_R1_FIDS) + r2_healthy + _expected(_R3_FIDS)
    accepted_total = len(_R1_FIDS) + len(_R2_FIDS) + len(_R3_FIDS)
    r2_health = d.get("r2_health") or {}
    r3_health = d.get("r3_health") or {}

    # ===== R1 : point de depart ========================================
    # Hypothesis revised (v7): enqueue() ne rend plus True mais une issue
    # EnqueueOutcome ; `is True` est faux par construction. On exige
    # l'issue exacte QUEUED_DURABLE (outbox fsync + ecrivain vivant), plus
    # stricte que bool(issue), qui accepterait aussi un repli sans ecrivain.
    out.append((
        "r1_healthy_batch_reaches_the_journal",
        d.get("r1_accepted") == "QUEUED_DURABLE" and d.get("r1_rows") == len(_R1_FIDS),
        f"accepted={d.get('r1_accepted')} lignes={d.get('r1_rows')} "
        f"attendu=QUEUED_DURABLE et {len(_R1_FIDS)} : la session demarre "
        f"normalement, sinon les mesures suivantes ne veulent rien dire",
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
    # Ajout (v7): ce que le disque contenait au moment ou enqueue() a
    # rendu QUEUED_DURABLE. Une copie de l'outbox prise a cet instant,
    # integree seule dans un journal vierge, doit restituer les 40
    # evenements du lot avec leurs octets : c'est la preuve d'execution de
    # "le lot est durable avant la file en RAM" (la garde de source
    # batch_is_durable_before_the_ram_queue n'en epingle que l'ordre).
    copy_fids = list(d.get("r2_copy_fids") or [])
    out.append((
        "r2_broken_batch_is_on_disk_at_acceptance",
        d.get("r2_accepted") == "QUEUED_DURABLE"
        and not d.get("r2_copy_error")
        and sorted(copy_fids) == sorted(_expected(_R2_FIDS))
        and d.get("r2_copy_geoms_intact") == len(_R2_FIDS)
        and d.get("r2_copy_dup_uids") == 0,
        f"issue={d.get('r2_accepted')} copie de l'outbox a l'accuse de "
        f"reception : {len(copy_fids)} evenement(s) restitue(s), attendu="
        f"{len(_R2_FIDS)} ; geometries intactes={d.get('r2_copy_geoms_intact')} ; "
        f"doublons={d.get('r2_copy_dup_uids')} ; erreur de copie="
        f"{d.get('r2_copy_error')!r} ; rejeu={((d.get('r2_copy_restart') or {}).get('replay'))}. "
        f"Un lot accuse QUEUED_DURABLE qui n'est pas entier sur le disque a cet "
        f"instant disparait avec un plantage de QGIS.",
    ))
    # Hypothesis revised (v7): le lot casse n'est plus sauve dans
    # recoverland_pending.json ni recopie en bloc : l'isolement descend
    # jusqu'a l'evenement et seuls les evenements que le journal refuse
    # sont ecrits (fsync) dans <journal>.outbox/quarantine. On exige,
    # APRES redemarrage simule, que la quarantaine contienne EXACTEMENT
    # les 20 evenements que l'injection rend illisibles (ni un sain de
    # plus, ni un illisible de moins), chacun avec sa geometrie intacte et
    # la raison qui designe l'injection : un sauvetage qui garderait
    # l'identite sans la donnee ne sauverait rien.
    rescued = sorted(fp for fp in quarantine if fp in poisoned)
    strays = sorted(fp for fp in quarantine if fp not in poisoned)
    out.append((
        "r2_broken_batch_is_rescued_to_the_quarantine",
        sorted(quarantine) == sorted(poisoned)
        and d.get("r_quarantine_payload_intact") == len(poisoned)
        and d.get("r_quarantine_blames_injection") == len(poisoned),
        f"{len(rescued)} evenement(s) illisible(s) dans la quarantaine apres "
        f"redemarrage, attendu={len(poisoned)} ; intrus (sains ou etrangers)="
        f"{strays[:5]} ; geometries intactes={d.get('r_quarantine_payload_intact')} ; "
        f"raison=injection pour {d.get('r_quarantine_blames_injection')} ; "
        f"fichiers={d.get('r_quarantine_files')} raisons={d.get('r_quarantine_reasons')} "
        f"etat ecrivain apres l'incident={r2_health.get('state')}. Le lot "
        f"avait deja quitte la file : si survivre a l'exception le "
        f"detruisait, les evenements qui l'ont provoquee seraient perdus.",
    ))
    # Hypothesis revised (v7): le stockage de secours du lot casse est la
    # quarantaine, plus le fichier d'attente (que la v7 n'ecrit plus :
    # l'ancienne mesure passerait a vide). Un lot mis en quarantaine a
    # chaque tentative, ou encore une fois au flush final, y figurerait
    # plusieurs fois et serait compte plusieurs fois a l'utilisateur.
    duplicated = sorted({fp for fp in quarantine if quarantine.count(fp) > 1})
    out.append((
        "r2_no_event_rescued_twice",
        not duplicated,
        f"evenement(s) presents plusieurs fois dans la quarantaine : "
        f"{duplicated[:5]} (n={len(quarantine)} pour {len(poisoned)} attendus). "
        f"Un lot sauve deux fois est un lot que toute reintegration ramene "
        f"deux fois, et un compteur qui ment a l'utilisateur.",
    ))
    # Ajout (v7): l'autre moitie de l'isolement a l'evenement. Les 20
    # evenements sains du lot casse doivent etre au journal des la fin de
    # la session (avant tout redemarrage, alors que l'outbox est deja
    # vide : c'est l'ecrivain qui les a inseres, pas un rejeu), avec leur
    # geometrie.
    r2_missing = [fp for fp in r2_healthy if fp not in journal]
    out.append((
        "r2_healthy_events_of_the_broken_batch_reach_the_journal",
        not r2_missing
        and d.get("r2_healthy_geoms_intact") == len(r2_healthy)
        and d.get("r_outbox_after_stop") == 0,
        f"evenements sains du lot casse absents du journal apres stop : "
        f"{r2_missing[:5]} ({len(r2_missing)}/{len(r2_healthy)}) ; geometries "
        f"intactes={d.get('r2_healthy_geoms_intact')} attendu={len(r2_healthy)} ; "
        f"outbox apres stop={d.get('r_outbox_after_stop')}. Un evenement "
        f"illisible ne doit pas emporter ses voisins de lot dans la "
        f"quarantaine, ou Restore et Rewind ne les verraient jamais.",
    ))
    # Ajout (v7): la quarantaine n'est "quelque part" que si la sante le
    # dit. Mesure apres R3 : l'ecrivain est revenu a running, donc un etat
    # DEGRADED ne peut venir que de la quarantaine.
    r3_cap = d.get("r3_capture") or {}
    out.append((
        "r2_quarantine_is_surfaced_by_the_health",
        r2_health.get("quarantined_events") == len(poisoned)
        and r3_health.get("quarantined_events") == len(poisoned)
        and r3_health.get("state") == "running"
        and r3_health.get("lost_events") == 0
        and r3_cap.get("state") == "DEGRADED"
        and _names_quarantine(r3_cap, len(poisoned)),
        f"quarantined_events apres R2={r2_health.get('quarantined_events')} "
        f"apres R3={r3_health.get('quarantined_events')} attendu={len(poisoned)} ; "
        f"etat ecrivain apres R3={r3_health.get('state')} ; lost_events="
        f"{r3_health.get('lost_events')} ; capture_health={r3_cap.get('state')} "
        f"raisons={r3_cap.get('reasons')}. Une lettre morte que rien ne "
        f"signale est une perte silencieuse.",
    ))

    # ===== R3 : la file n'est pas un trou noir =========================
    # Hypothesis revised (v7): enqueue() rend une issue, plus True. On
    # exige QUEUED_DURABLE, qui prouve qu'un ecrivain VIVANT et acceptant
    # a pris le lot ; PERSISTED_FALLBACK, vrai au sens booleen, voudrait
    # dire que l'ecrivain ne tourne plus et doit echouer ici.
    out.append((
        "r3_queue_still_accepts_after_the_incident",
        d.get("r3_accepted") == "QUEUED_DURABLE" and d.get("r3_writer_alive") == 1,
        f"accepted={d.get('r3_accepted')} ecrivains={d.get('r3_writer_alive')} "
        f"etat apres R3={r3_health.get('state')} : l'edition "
        f"qui suit l'incident doit continuer d'etre enregistree, sinon "
        f"l'utilisateur travaille une journee entiere en croyant etre protege",
    ))
    missing = [fp for fp in written if fp not in journal]
    out.append((
        "r3_healthy_batches_are_all_in_the_journal",
        not missing,
        f"evenement(s) des lots voisins R1/R3 absents du journal : {missing[:5]} "
        f"({len(written) - len(missing)}/{len(written)} presents, journal total="
        f"{len(journal)}). Un lot casse ne doit pas emporter les lots voisins.",
    ))

    # ===== R4 : conservation ===========================================
    # Hypothesis revised (v7): le second endroit n'est plus le fichier
    # d'attente mais la quarantaine, et l'outbox est rejouee au demarrage.
    # Mesure prise APRES redemarrage simule : si le lot en quarantaine
    # etait aussi rejoue (outbox non acquittee, ou fichier d'attente
    # alimente par un ancien chemin), il apparaitrait ici dans le journal.
    both = sorted(set(journal_after) & set(quarantine))
    out.append((
        "r4_no_event_in_both_places",
        not both,
        f"evenement(s) a la fois dans le journal (apres redemarrage) et dans "
        f"la quarantaine : {both[:5]}. Un lot sauve ET integre est un doublon "
        f"garanti des qu'on reintegre la quarantaine.",
    ))
    # Hypothesis revised (v7): "journal + fichier d'attente == accepte"
    # devient une propriete exacte apres redemarrage simule : le journal
    # contient EXACTEMENT les 45 evenements sains (R1, les 20 sains de R2,
    # R3), chacun une fois (compte de lignes == 45, ensemble egal, aucun
    # event_uid en double) ; la quarantaine contient EXACTEMENT les 20
    # illisibles ; rien ne manque ; et "encore en file apres stop"
    # (pending_count, nul par construction apres stop en v7) devient
    # "encore dans l'outbox", mesure sur le disque apres stop et apres
    # redemarrage.
    nowhere = sorted(set(healthy + poisoned) - set(journal_after) - set(quarantine))
    out.append((
        "r4_every_accepted_event_is_somewhere",
        sorted(journal_after) == sorted(healthy)
        and d.get("r_rows_after_restart") == len(healthy)
        and sorted(quarantine) == sorted(poisoned)
        and len(journal_after) + len(quarantine) == accepted_total
        and not nowhere
        and d.get("r_dup_uids") == 0
        and d.get("r_outbox_after_stop") == 0
        and d.get("r_outbox_after_restart") == 0
        and not d.get("r_pending_after_restart"),
        f"journal apres redemarrage={len(journal_after)} lignes="
        f"{d.get('r_rows_after_restart')} attendu={len(healthy)} sains ; "
        f"quarantaine={len(quarantine)} attendu={len(poisoned)} illisibles ; "
        f"total={len(journal_after) + len(quarantine)} accepte={accepted_total} ; "
        f"introuvables={nowhere[:5]} ; sains manquants="
        f"{sorted(set(healthy) - set(journal_after))[:5]} ; en trop au journal="
        f"{sorted(set(journal_after) - set(healthy))[:5]} ; doublons "
        f"event_uid={d.get('r_dup_uids')} ; encore dans l'outbox apres stop="
        f"{d.get('r_outbox_after_stop')} apres redemarrage="
        f"{d.get('r_outbox_after_restart')} ; attente apres redemarrage="
        f"{d.get('r_pending_after_restart')} ; redemarrage={d.get('r_restart')}. "
        f"Trois lots acceptes, un seul casse : le compte doit etre exact, sans "
        f"perte et sans doublon.",
    ))
    # Ajout (v7): la lettre morte doit rester visible apres un redemarrage.
    # Les compteurs de health() vivent en memoire ; les 20 evenements, eux,
    # sont toujours hors du journal. Si la sante lue au demarrage suivant
    # dit PROTECTED ("toute edition acceptee est au journal ou en route"),
    # elle affirme une chose que le disque dement.
    nxt = d.get("r_next_health") or {}
    nxt_cap = d.get("r_next_capture") or {}
    out.append((
        "r4_quarantine_is_still_surfaced_after_restart",
        nxt_cap.get("state") == "DEGRADED"
        and _names_quarantine(nxt_cap, len(poisoned)),
        f"demarrage suivant : capture_health={nxt_cap.get('state')} raisons="
        f"{nxt_cap.get('reasons')} quarantined_events={nxt.get('quarantined_events')} "
        f"dernier rejeu={nxt.get('last_replay')} ; quarantaine sur le disque="
        f"{len(quarantine)} evenement(s). Attendu : DEGRADED avec la "
        f"quarantaine nommee, tant que ces {len(poisoned)} editions ne sont pas "
        f"au journal.",
    ))

    # ===== R5 : la notification qui leve ===============================
    r5_journal = list(d.get("r5_journal") or [])
    r5_pending = list(d.get("r5_pending") or [])
    r5_expected = _expected(_R5_FIRST) + _expected(_R5_SECOND)
    out.append((
        "r5_committed_batches_are_not_copied_to_pending",
        "r5_pending" in d and not r5_pending,
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
    # Ajout (v7): l'ancien scenario notait que le doublon de R5
    # n'apparaitrait qu'au demarrage suivant, hors de sa portee. Le
    # redemarrage simule le rend mesurable : les lots commites doivent
    # etre acquittes dans l'outbox malgre le callback qui leve, et le
    # redemarrage ne doit rien rejouer en double.
    r5_after = list(d.get("r5_journal_after_restart") or [])
    out.append((
        "r5_restart_replays_nothing_twice",
        sorted(r5_after) == sorted(r5_expected)
        and d.get("r5_dup_uids") == 0
        and d.get("r5_outbox_after_stop") == 0,
        f"journal apres redemarrage={len(r5_after)} attendu={len(r5_expected)} "
        f"doublons event_uid={d.get('r5_dup_uids')} encore dans l'outbox apres "
        f"stop={d.get('r5_outbox_after_stop')} redemarrage={d.get('r5_restart')} : "
        f"un lot commite dont la notification echoue ne doit pas rester a "
        f"rejouer.",
    ))

    # ===== R6 : quarantaine impossible a ecrire ========================
    # Hypothesis revised (v7): cette assertion etait une garde de texte
    # (l'appel a la quarantaine precede l'ack dans la source). L'ordre du
    # texte restait vrai pendant que le produit acquittait un lot dont la
    # quarantaine avait echoue (perte prouvee au tour precedent). Meme
    # intention, mesuree a l'execution : la quarantaine est bloquee par un
    # FICHIER de son nom ; le lot empoisonne ne doit PAS etre acquitte :
    # ses evenements illisibles sont encore sur le disque dans l'outbox
    # apres stop, absents de la quarantaine et du journal, puis, le
    # defaut leve, le redemarrage les integre, une fois chacun.
    r6_poison = _expected(sorted(_R6_POISON))
    r6_all = _expected(_R6_FIDS) + _expected(_R6_FOLLOW)
    r6_box = list(d.get("r6_outbox_fids_after_stop") or [])
    r6_before = list(d.get("r6_journal_before_stop") or [])
    r6_after = list(d.get("r6_journal_after_restart") or [])
    out.append((
        "poison_batch_is_quarantined_before_its_ack",
        d.get("r6_accepted") == "QUEUED_DURABLE"
        and d.get("r6_blocker_still_a_file") is True
        and "r6_quarantine_after_stop" in d
        and not d.get("r6_quarantine_after_stop")
        and all(fp in r6_box for fp in r6_poison)
        and not any(fp in r6_before for fp in r6_poison)
        and sorted(r6_after) == sorted(r6_all)
        and d.get("r6_dup_uids") == 0
        and d.get("r6_outbox_after_restart") == 0,
        f"issue={d.get('r6_accepted')} quarantaine toujours bloquee="
        f"{d.get('r6_blocker_still_a_file')} ; illisibles encore dans l'outbox "
        f"apres stop={sum(1 for fp in r6_poison if fp in r6_box)}/{len(r6_poison)} "
        f"(outbox={d.get('r6_outbox_after_stop')} evenement(s)) ; illisibles au "
        f"journal avant redemarrage={[fp for fp in r6_poison if fp in r6_before]} ; "
        f"quarantaine={d.get('r6_quarantine_after_stop')} ; apres levee du "
        f"defaut et redemarrage : journal={len(r6_after)} attendu={len(r6_all)} "
        f"doublons={d.get('r6_dup_uids')} outbox={d.get('r6_outbox_after_restart')} "
        f"rejeu={((d.get('r6_restart') or {}).get('replay'))}. Un lot retire de "
        f"l'outbox sans etre ni au journal ni en quarantaine n'est nulle part.",
    ))
    r6_health = d.get("r6_health") or {}
    r6_cap = d.get("r6_capture") or {}
    out.append((
        "r6_parked_poison_is_counted_degraded",
        r6_health.get("degraded_events") == len(r6_poison)
        and r6_health.get("quarantined_events") == 0
        and r6_health.get("lost_events") == 0
        and r6_cap.get("state") == "DEGRADED",
        f"degraded_events={r6_health.get('degraded_events')} attendu="
        f"{len(r6_poison)} ; quarantined_events={r6_health.get('quarantined_events')} "
        f"attendu=0 (rien n'a pu y etre ecrit) ; lost_events="
        f"{r6_health.get('lost_events')} attendu=0 (le lot est dans l'outbox) ; "
        f"capture_health={r6_cap.get('state')} raisons={r6_cap.get('reasons')}.",
    ))
    out.append(_in_window(
        records, tid, "R6",
        r"\[CRITICAL\s*\].*WriteQueue: event=wq_poison_parked "
        rf"n_events={len(r6_poison)} batch=",
        name="r6_parked_poison_is_logged_critical", min_count=1, max_count=1))
    out.append(_absent_in_window(
        records, tid, "R6", r"event=wq_poison_batch_quarantined",
        name="r6_no_false_quarantine_claim"))
    r6_healthy = _expected([i for i in _R6_FIDS if i not in _R6_POISON])
    out.append((
        "r6_writer_goes_on_after_a_parked_batch",
        all(fp in (d.get("r6_journal_after_park") or []) for fp in r6_healthy)
        and d.get("r6_follow_accepted") == "QUEUED_DURABLE"
        and all(fp in r6_before for fp in _expected(_R6_FOLLOW))
        and d.get("r6_follow_state") == "running"
        and d.get("r6_writer_alive") is True,
        f"sains du lot parque au journal="
        f"{sum(1 for fp in r6_healthy if fp in (d.get('r6_journal_after_park') or []))}"
        f"/{len(r6_healthy)} ; lot suivant={d.get('r6_follow_accepted')} au journal="
        f"{sum(1 for fp in _expected(_R6_FOLLOW) if fp in r6_before)}/"
        f"{len(_R6_FOLLOW)} ; etat={d.get('r6_follow_state')} ecrivain vivant="
        f"{d.get('r6_writer_alive')}.",
    ))

    # ===== R7 : exception hors de l'ecriture du lot ====================
    # Ajout (v7): la boucle rattrape Exception autour de toute l'iteration
    # (ack, checkpoint, isolement), pas seulement autour de l'ecriture. Un
    # ack qui leve une fois apres le COMMIT : l'ecrivain reste vivant,
    # passe DEGRADED, revient a RUNNING au lot suivant, et rien n'est
    # perdu (mesure au journal avant stop et apres redemarrage).
    r7_all = _expected(_R7_FIRST) + _expected(_R7_SECOND)
    r7_before = list(d.get("r7_journal_before_stop") or [])
    r7_after = list(d.get("r7_journal_after_restart") or [])
    out.append((
        "r7_writer_survives_an_error_outside_the_batch_write",
        d.get("r7_first_accepted") == "QUEUED_DURABLE"
        and d.get("r7_reached_degraded") is True
        and d.get("r7_alive_after_error") is True
        and d.get("r7_second_accepted") == "QUEUED_DURABLE"
        and d.get("r7_reached_running") is True
        and d.get("r7_alive_after_second") is True,
        f"issues={d.get('r7_first_accepted')}/{d.get('r7_second_accepted')} ; "
        f"etats observes={d.get('r7_states')} (attendu degraded puis running) ; "
        f"vivant apres l'erreur={d.get('r7_alive_after_error')} apres le lot "
        f"suivant={d.get('r7_alive_after_second')} ; appels d'ack="
        f"{d.get('r7_ack_calls')}.",
    ))
    out.append((
        "r7_error_outside_the_write_loses_nothing",
        sorted(r7_before) == sorted(r7_all)
        and sorted(r7_after) == sorted(r7_all)
        and d.get("r7_dup_uids") == 0
        and d.get("r7_outbox_after_restart") == 0,
        f"journal avant stop={len(r7_before)} apres redemarrage={len(r7_after)} "
        f"attendu={len(r7_all)} ; doublons={d.get('r7_dup_uids')} ; outbox apres "
        f"redemarrage={d.get('r7_outbox_after_restart')} ; restes dans l'outbox "
        f"apres stop={len(d.get('r7_outbox_fids_after_stop') or [])} ; rejeu="
        f"{((d.get('r7_restart') or {}).get('replay'))}.",
    ))
    # Ajout (v7): "revenu a running" doit vouloir dire revenu a la normale.
    # Les deux lots sont commites ; la file doit se dire vide, sinon la
    # barriere read-your-writes (wait_until_drained) ne passe plus de la
    # session, la barre d'etat affiche "N evenement(s) en cours d'ecriture"
    # indefiniment et le chien de garde finit par crier au blocage d'un
    # ecrivain inactif.
    r7_h = d.get("r7_health_after") or {}
    out.append((
        "r7_no_phantom_inflight_after_the_error",
        d.get("r7_pending_after") == 0
        and r7_h.get("queued_events") == 0
        and d.get("r7_drained") is True,
        f"pending_count={d.get('r7_pending_after')} queued_events="
        f"{r7_h.get('queued_events')} attendu 0 alors que les "
        f"{len(r7_all)} evenements sont au journal ; wait_until_drained(1s)="
        f"{d.get('r7_drained')}.",
    ))
    # Hypothesis revised (v7): l'echec d'un ack n'est plus une erreur de
    # boucle : `_ack` le contient (l'ecrivain passe DEGRADED puis revient
    # RUNNING au lot suivant, cf. r7_writer_survives...) et le journalise
    # en ERROR sous `wq_ack_failed`. Meme exigence : l'erreur injectee est
    # dite exactement une fois dans la fenetre R7.
    out.append(_in_window(
        records, tid, "R7",
        rf"WriteQueue: event=wq_ack_failed batch=\S+ seq=\d+ "
        rf"error=RuntimeError: {_ACK_ERROR}",
        name="r7_error_outside_the_write_is_logged", min_count=1, max_count=1))

    # ===== R8 : la meme exception pendant l'isolement ==================
    # Ajout (v7): l'ack qui leve arrive cette fois apres l'ecriture de la
    # quarantaine, dans l'isolement. Le lot reste a retenter ; le
    # retenter ne doit ni perdre les sains ni recopier les illisibles.
    r8_poison = _expected(sorted(_R8_POISON))
    r8_healthy = _expected([i for i in _R8_FIDS if i not in _R8_POISON])
    r8_q = list(d.get("r8_quarantine") or [])
    r8_after = list(d.get("r8_journal_after_restart") or [])
    out.append((
        "r8_error_during_isolation_loses_nothing",
        d.get("r8_accepted") == "QUEUED_DURABLE"
        and d.get("r8_alive") is True
        and sorted(r8_after) == sorted(r8_healthy)
        and sorted(set(r8_q)) == sorted(r8_poison)
        and not (set(r8_after) & set(r8_q))
        and d.get("r8_dup_uids") == 0
        and d.get("r8_outbox_after_restart") == 0,
        f"issue={d.get('r8_accepted')} vivant={d.get('r8_alive')} ; journal "
        f"apres redemarrage={sorted(r8_after)} attendu={r8_healthy} ; "
        f"quarantaine={r8_q} ; doublons={d.get('r8_dup_uids')} ; outbox="
        f"{d.get('r8_outbox_after_restart')} ; appels d'ack={d.get('r8_ack_calls')}.",
    ))
    r8_dup = sorted({fp for fp in r8_q if r8_q.count(fp) > 1})
    r8_h = d.get("r8_health") or {}
    out.append((
        "r8_isolation_retry_does_not_quarantine_twice",
        "r8_quarantine" in d
        and not r8_dup
        and sorted(r8_q) == sorted(r8_poison)
        and r8_h.get("quarantined_events") == len(r8_poison),
        f"quarantaine={r8_q} (en double : {r8_dup}) attendu={r8_poison} une "
        f"fois chacun ; quarantined_events={r8_h.get('quarantined_events')} "
        f"attendu={len(r8_poison)}. Le meme evenement dans deux lettres mortes "
        f"est compte deux fois a l'utilisateur.",
    ))
    # Hypothesis revised (v7): meme revision que R7 (ack contenu, dit en
    # ERROR sous `wq_ack_failed`) ; le lot n'est plus re-isole, et le
    # rejeu ne remet pas au journal ce que la quarantaine detient deja
    # (r8_error_during_isolation_loses_nothing reste inchangee).
    out.append(_in_window(
        records, tid, "R8",
        rf"WriteQueue: event=wq_ack_failed batch=\S+ seq=\d+ "
        rf"error=RuntimeError: {_ACK_ERROR}",
        name="r8_error_during_isolation_is_logged", min_count=1, max_count=1))

    # ===== Traces ======================================================
    # Hypothesis revised (v7): le message "writer loop error, staying
    # alive, N event(s) of the failed batch saved to pending" n'existe
    # plus ; le sauvetage v7 s'annonce par event=wq_poison_batch_quarantined
    # avec le compte, en CRITICAL. L'isolement descendant a l'evenement,
    # la ligne porte aussi le nombre d'evenements sains inseres : on exige
    # les deux comptes exacts, le niveau, et une seule occurrence dans la
    # fenetre de R2 (un second sauvetage serait un doublon).
    out.append(_in_window(
        records, tid, "R2",
        r"\[CRITICAL\s*\].*WriteQueue: event=wq_poison_batch_quarantined "
        rf"n_events={len(poisoned)} inserted={len(r2_healthy)} ",
        name="rescue_is_logged_with_the_saved_count",
        min_count=1,
        max_count=1,
    ))
    # Hypothesis revised (v7): sentinelle de l'injection. L'echec doit
    # venir du thread ecrivain et etre non reessayable. L'isolement
    # retente maintenant chaque lot du groupe SEUL avant de descendre a
    # l'evenement : le lot R2 (seul dans son groupe) echoue donc
    # _POISON_ATTEMPTS fois en groupe puis une fois seul, et pas une fois
    # de plus (les essais par evenement ne passent pas par ce message).
    from recoverland.core import write_queue as _wqm
    attempts = int(getattr(_wqm, "_POISON_ATTEMPTS", 3))
    out.append(_in_window(
        records, tid, "R2",
        rf"\[{_WRITER_THREAD_NAME}\s*\] WriteQueue: event=wq_write_failed "
        rf"n_events={len(_R2_FIDS)} retryable=False "
        rf"error=RuntimeError: {_INJECTED_ERROR}",
        name="injected_failure_fires_inside_the_writer",
        min_count=attempts + 1,
        max_count=attempts + 1,
    ))
    out.append(assert_log_contains(
        records,
        r"WriteQueue: flush callback failed after \d+ events were committed",
        name="callback_failure_is_logged_as_a_notification_failure",
        min_count=2,
    ))
    out.append(assert_log_absent(
        records,
        r"WriteQueue: fatal writer error",
        name="no_fatal_writer_error",
    ))
    out.append(assert_log_contains(
        records,
        rf"tx_life_wq_batch_rescue.*trace_id={tid}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Gardes de source ============================================
    # Les proprietes structurelles derriere R2 et R5. Elles sont
    # verifiees dans le texte parce qu'une refonte de l'ecrivain peut les
    # perdre sans qu'aucune mesure ci-dessus ne change dans ce scenario
    # (un plantage de QGIS au mauvais moment, par exemple, n'y est pas
    # simule).
    src = (_PLUGIN_ROOT / "core" / "write_queue.py").read_text(
        encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(src)
    except SyntaxError:
        tree = ast.parse("")
    enqueue_src = _method_src(src, tree, "WriteQueue", "enqueue")
    loop_src = _method_src(src, tree, "WriteQueue", "_writer_loop")
    write_src = _method_src(src, tree, "WriteQueue", "_write_batches")
    write_node = _find_method(tree, "WriteQueue", "_write_batches")

    # Hypothesis revised (v7): `_drain_batch` n'existe plus. La propriete
    # gardee etait "le lot sorti de la file n'est pas la seule reference
    # restante". En v7 elle tient parce qu'enqueue() ecrit le lot dans
    # l'outbox (fsync) AVANT de le poser dans la file en RAM : la copie en
    # RAM n'est jamais la seule. On epingle cet ordre dans enqueue() ;
    # r2_broken_batch_is_on_disk_at_acceptance le prouve a l'execution.
    append_at = enqueue_src.find(".append_events(")
    put_at = enqueue_src.find(".queue.put(")
    durable_ret_at = enqueue_src.find("return EnqueueOutcome.QUEUED_DURABLE", put_at)
    out.append((
        "batch_is_durable_before_the_ram_queue",
        0 < append_at < put_at < durable_ret_at,
        f"append_events a {append_at}, queue.put a {put_at}, retour "
        f"QUEUED_DURABLE a {durable_ret_at} dans enqueue() : le lot doit etre "
        f"dans l'outbox avant d'entrer dans la file en RAM, sinon l'erreur qui "
        f"le fait sortir de la boucle detruirait la seule copie.",
    ))
    # Hypothesis revised (v7): meme propriete cote ecrivain. Le `try`
    # autour du drainage est remplace par : `_write_batches` rattrape
    # Exception et rend (False, False) sans relancer ; la boucle garde le
    # lot en echec (`retry = batches`) et le reprend avant tout nouveau
    # lot ; l'isolement (`_isolate_poison`, anciennement
    # `_quarantine_poison`) n'intervient qu'apres _POISON_ATTEMPTS echecs
    # non reessayables.
    take_at = loop_src.find("batches = retry or self._take_batches(")
    keep_at = loop_src.find("retry = batches")
    gate_at = loop_src.find("attempts >= _POISON_ATTEMPTS")
    quar_call_at = loop_src.find("self._isolate_poison(", gate_at)
    catches = _except_exception_returns(write_node)
    out.append((
        "writer_keeps_the_failed_batch",
        catches and 0 < take_at < keep_at < gate_at < quar_call_at,
        f"except Exception -> (False, False) sans raise={catches} ; reprise "
        f"a {take_at}, `retry = batches` a {keep_at}, seuil _POISON_ATTEMPTS "
        f"a {gate_at}, isolement a {quar_call_at} dans _writer_loop : un lot "
        f"qui leve doit rester entre les mains de l'ecrivain jusqu'au journal "
        f"ou a la quarantaine.",
    ))
    # Hypothesis revised (v7): `batch = []` apres `_write_batch_with_retry`
    # devient la remise a zero de `retry` dans la branche de succes de la
    # boucle (et apres l'isolement). Garder la reference d'un lot deja
    # commite le ferait reecrire, ou pire mettre en quarantaine alors
    # qu'il est au journal si le lot suivant echouait : un evenement aux
    # deux endroits.
    ok_at = loop_src.find("if ok:")
    clear_ok_at = loop_src.find("retry, attempts, backoff = [], 0,", ok_at)
    clear_q_at = loop_src.find("retry, attempts = [], 0", quar_call_at)
    out.append((
        "handed_over_batch_reference_is_dropped",
        0 < ok_at < clear_ok_at < keep_at and 0 < quar_call_at < clear_q_at,
        f"`if ok:` a {ok_at}, remise a zero apres succes a {clear_ok_at} "
        f"(avant `retry = batches` a {keep_at}), remise a zero apres "
        f"isolement a {clear_q_at} (appel a {quar_call_at}) : un lot commite "
        f"ou mis en quarantaine ne doit plus etre tenu par la boucle.",
    ))
    # Hypothesis revised (v7): protection structurelle de R5. L'accuse de
    # reception dans l'outbox suit le COMMIT (apres le gestionnaire
    # d'erreur d'ecriture) et precede le callback, et le callback est dans
    # un try qui rattrape Exception : son echec ne peut ni empecher l'ack
    # (le lot resterait a rejouer) ni etre pris pour un echec d'ecriture.
    wexc_at = write_src.find("except Exception")
    wack_at = write_src.find(".outbox.ack(", wexc_at)
    # Hypothesis revised (v7): l'ack passe par `self._ack(gen, b)`, qui
    # contient son propre echec (voir R7/R8). Meme exigence d'ordre, et
    # `_ack` doit appeler reellement `.outbox.ack(`.
    ack_helper_src = _method_src(src, tree, "WriteQueue", "_ack")
    if wack_at < 0 and ".outbox.ack(" in ack_helper_src:
        wack_at = write_src.find("self._ack(", wexc_at)
    cb_at = write_src.find("self._on_flush_callback(", wack_at)
    cb_guarded = _call_is_guarded(write_node, "_on_flush_callback")
    out.append((
        "flush_callback_is_guarded_after_the_ack",
        0 < wexc_at < wack_at < cb_at and cb_guarded,
        f"gestionnaire d'ecriture a {wexc_at}, outbox.ack a {wack_at}, "
        f"callback a {cb_at} dans _write_batches, callback sous try/except "
        f"Exception={cb_guarded} : une notification qui leve apres le COMMIT "
        f"ne doit ni bloquer l'ack ni etre traitee comme un echec d'ecriture.",
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
