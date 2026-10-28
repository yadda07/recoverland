# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_wq_durable_contract - RecoverLand validation runtime.

Invariant: I-WQ-01 (un evenement accepte possede toujours un chemin vers un
stockage durable) et I-WQ-02 (chaque appel a enqueue() se termine dans un
etat explicite ; aucun accuse de reception pour un evenement qui ne vit
qu'en RAM).

Le mecanisme d'avant
--------------------
`WriteQueue.enqueue` validait le lot, le posait dans une `queue.Queue` et
renvoyait True. Entre cet instant et le COMMIT du thread ecrivain, le lot
n'existait qu'en memoire : 35 ms pour un evenement, 1,3 s pour 10 000
(mesure, `bench_durability.py`), sans borne quand le journal est verrouille.
Un plantage de QGIS dans cette fenetre effacait des modifications deja
ecrites dans la donnee de l'utilisateur, et rien sur le disque ne le disait.
Un commit de plus de 50 000 entites etait REFUSE : envoye dans le fichier
d'attente, puis le suivi se desactivait tout seul pour le reste de la
session.

Ce que le scenario exige
------------------------
W1  cas nominal : issue QUEUED_DURABLE, tout est au journal apres stop(),
    rien ne reste dans l'outbox.
W2  crashs reels (processus enfant tue par os._exit) a chaque etape du
    transfert ; apres redemarrage, chaque evenement est au journal
    EXACTEMENT une fois, et une ecriture dechiree est mise en quarantaine
    et comptee, jamais effacee.
W3  thread ecrivain tue en cours de session (BaseException dans le
    callback) : l'appel suivant rend PERSISTED_FALLBACK (pas un faux
    succes), le chien de garde passe a FAILED et previent UNE fois, un
    redemarrage de l'ecrivain integre tout.
W4  journal verrouille par un autre processus : l'ecrivain passe DEGRADED,
    reessaie, puis se retablit sans perte ni doublon.
W5  outbox impossible a ecrire : repli synchrone dans le journal
    (PERSISTED_JOURNAL_SYNC).
W6  ni outbox ni journal : REFUSED_NO_DURABLE_PATH, compte dans
    lost_events et journalise en CRITICAL. C'est la seule issue ou une
    perte est possible, et elle est criee.
W7  lot malforme : REFUSED_INVALID, et le lot est en quarantaine sur le
    disque, jamais jete.
W8  un commit de 60 000 evenements est accepte et integre, pas refuse.
W9  stop() qui expire pendant que le journal est verrouille : rien n'est
    perdu, le redemarrage suivant integre le lot une seule fois.
W10 la copie d'outbox d'un lot n'est liberee qu'apres un COMMIT qui
    survit a une coupure de courant : a chaque liberation, la connexion
    de l'ecrivain est en synchronous=FULL. En WAL + NORMAL, le COMMIT
    n'atteint le disque qu'au prochain checkpoint : une coupure juste
    apres la liberation perdait la seule copie durable (un plantage de
    QGIS, lui, ne perd rien).

Verdict attendu avant correctif : FAIL (pas d'outbox, pas d'etat explicite,
W2 perd les lots en vol, W8 refuse le lot).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_wq_durable_contract"
INVARIANT = "I-WQ-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _event(idx: int, session: str, created_at: str = "2026-09-27T10:00:00+00:00"):
    from recoverland.core.audit_backend import AuditEvent
    return AuditEvent(
        event_id=None, project_fingerprint="wq_contract",
        datasource_fingerprint="ds_wq_contract", layer_id_snapshot="lyr",
        layer_name_snapshot="reseau", provider_type="ogr",
        feature_identity_json='{"fid": %d}' % idx, operation_type="DELETE",
        attributes_json='{"all_attributes": {"nom": "poteau %d"}}' % idx,
        geometry_wkb=None, geometry_type="NoGeometry", crs_authid="EPSG:2154",
        field_schema_json='[{"name": "nom", "type": "string"}]',
        user_name="tester", session_id=session, created_at=created_at,
        restored_from_event_id=None, entity_fingerprint="fid:%d" % idx,
        event_schema_version=2, new_geometry_wkb=None, invalidated_at=None,
    )


def _make_journal(root: str, name: str) -> str:
    from recoverland.core.sqlite_schema import initialize_schema
    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.close()
    return path


def _rows(db_path: str, session: str = "") -> int:
    try:
        conn = sqlite3.connect(db_path)
        try:
            if session:
                return int(conn.execute(
                    "SELECT COUNT(*) FROM audit_event WHERE session_id = ?",
                    (session,)).fetchone()[0])
            return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _dup_uids(db_path: str) -> int:
    try:
        conn = sqlite3.connect(db_path)
        try:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_event)")}
            if "event_uid" not in cols:
                return -1
            return int(conn.execute(
                "SELECT COUNT(*) FROM (SELECT event_uid FROM audit_event "
                "WHERE event_uid IS NOT NULL GROUP BY event_uid "
                "HAVING COUNT(*) > 1)").fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _outbox_segments(db_path: str) -> int:
    d = os.path.abspath(db_path) + ".outbox"
    if not os.path.isdir(d):
        return 0
    return sum(1 for n in os.listdir(d)
               if n.startswith("seg-") and os.path.getsize(os.path.join(d, n)) > 0)


def _quarantine_files(db_path: str) -> list:
    d = os.path.join(os.path.abspath(db_path) + ".outbox", "quarantine")
    return sorted(os.listdir(d)) if os.path.isdir(d) else []


def _outcome_name(res) -> str:
    return getattr(res, "name", repr(res))


def _restart_and_count(db_path: str, session: str):
    from recoverland.core.write_queue import WriteQueue
    wq = WriteQueue()
    wq.start(db_path)
    report = getattr(wq, "last_replay_report", None)
    wq.stop()
    rep = report.as_dict() if report is not None else None
    return _rows(db_path, session), _dup_uids(db_path), rep


_CHILD_W2 = """
from recoverland.core.write_queue import WriteQueue
from recoverland.core.audit_backend import AuditEvent
def ev(i):
    return AuditEvent(event_id=None, project_fingerprint="wq_contract",
        datasource_fingerprint="ds_wq_contract", layer_id_snapshot="lyr",
        layer_name_snapshot="reseau", provider_type="ogr",
        feature_identity_json='{{"fid": %d}}' % i, operation_type="DELETE",
        attributes_json='{{"all_attributes": {{"nom": "n%d"}}}}' % i,
        geometry_wkb=None, geometry_type="NoGeometry", crs_authid="EPSG:2154",
        field_schema_json="[]", user_name="tester", session_id={session!r},
        created_at="2026-09-27T10:00:00+00:00", restored_from_event_id=None,
        entity_fingerprint="fid:%d" % i, event_schema_version=2,
        new_geometry_wkb=None, invalidated_at=None)
wq = WriteQueue()
wq.start({db!r})
res = wq.enqueue([ev(i) for i in range({n})])
print("OUTCOME", getattr(res, "name", res), flush=True)
time.sleep(1.5)
os._exit(0)
"""


_CHILD_W2B = _CHILD_W2.replace("time.sleep(1.5)\n", "")


def _w2b(ctx, root):
    """QGIS dies right after enqueue() returned: no fault point needed.

    This is the defect as it stood, provable on any version: an accepted
    batch that only lived in the RAM queue vanishes with the process.
    """
    from scripts.validation.crash_harness import run_child
    db = _make_journal(root, "w2b")
    rc, so, se = run_child(_CHILD_W2B.format(db=db, n=200, session="w2b"),
                           timeout=120)
    rows, dups, rep = _restart_and_count(db, "w2b")
    return {"rc": rc, "child_outcome": [ln for ln in so.splitlines()
                                        if ln.startswith("OUTCOME")],
            "rows_after_restart": rows, "dups": dups, "replay": rep,
            "stderr_tail": se[-300:] if rc != 0 else ""}


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_wqdc_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"tx_wq_durable_contract setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def _w1(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    db = _make_journal(root, "w1")
    wq = WriteQueue()
    wq.start(db)
    res = wq.enqueue([_event(i, "w1") for i in range(25)])
    wq.stop()
    return {"outcome": _outcome_name(res), "truthy": bool(res),
            "rows": _rows(db, "w1"), "segments": _outbox_segments(db),
            "dups": _dup_uids(db)}


def _w10(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    db = _make_journal(root, "w10")
    wq = WriteQueue()
    wq.start(db)
    seen = []
    outbox = getattr(wq, "_outbox", None)
    if outbox is not None:
        original = outbox.ack

        def _spy(seq, n_events=0):
            seen.append(getattr(wq.health(), "journal_sync", None))
            return original(seq, n_events)
        outbox.ack = _spy
    res = wq.enqueue([_event(i, "w10") for i in range(30)])
    wq.stop()
    return {"outcome": _outcome_name(res), "rows": _rows(db, "w10"),
            "sync_at_release": seen, "segments": _outbox_segments(db)}


def _w2(ctx, root):
    from scripts.validation.crash_harness import run_child, FAULT_EXIT_CODE
    out = {}
    points = [
        ("outbox_torn_write", 40),
        ("outbox_after_fsync", 40),
        ("wq_after_outbox_before_queue", 40),
        ("wq_before_sqlite_commit", 40),
        ("wq_after_sqlite_commit_before_ack", 40),
    ]
    for point, n in points:
        db = _make_journal(root, f"w2_{point}")
        session = f"w2_{point}"
        rc, so, se = run_child(_CHILD_W2.format(db=db, n=n, session=session),
                               fault=point, timeout=120)
        rows, dups, rep = _restart_and_count(db, session)
        out[point] = {"rc": rc, "fault_reached": rc == FAULT_EXIT_CODE,
                      "child_outcome": [ln for ln in so.splitlines()
                                        if ln.startswith("OUTCOME")],
                      "rows_after_restart": rows, "dup_uids": dups,
                      "replay": rep, "quarantine": _quarantine_files(db),
                      "stderr_tail": se[-300:] if rc not in (0, FAULT_EXIT_CODE) else ""}
    return out


def _w3(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    db = _make_journal(root, "w3")
    wq = WriteQueue()
    alerts = []

    class _Die(BaseException):
        pass

    def _cb(n):
        raise _Die("writer killed by a BaseException from a callback")

    wq.set_flush_callback(_cb)
    if hasattr(wq, "set_failure_callback"):
        wq.set_failure_callback(lambda h: alerts.append(h))
    wq.start(db)
    first = wq.enqueue([_event(i, "w3") for i in range(10)])
    time.sleep(0.8)
    # THIS queue's writer: other scenarios of the suite may leave writers
    # of their own behind, which says nothing about this one.
    own = getattr(wq, "_thread", None)
    alive = bool(own is not None and own.is_alive())
    second = wq.enqueue([_event(i, "w3") for i in range(100, 130)])
    health1 = health2 = None
    if hasattr(wq, "watchdog_check"):
        health1 = wq.watchdog_check()._asdict()
        health2 = wq.watchdog_check()._asdict()
    restarted = False
    if hasattr(wq, "restart_writer"):
        wq.set_flush_callback(None)
        restarted = wq.restart_writer()
    wq.stop()
    rows_now = _rows(db, "w3")
    rows_after, dups, _rep = _restart_and_count(db, "w3")
    return {"first": _outcome_name(first), "writer_alive_after_kill": alive,
            "second": _outcome_name(second), "health1": health1,
            "health2": health2, "alerts": len(alerts), "restarted": restarted,
            "rows_before_restart": rows_now, "rows_after_restart": rows_after,
            "dups": dups}


def _w4(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    db = _make_journal(root, "w4")
    wq = WriteQueue()
    wq.start(db)
    locker = sqlite3.connect(db, timeout=0.1, isolation_level=None)
    locker.execute("BEGIN EXCLUSIVE")
    res = wq.enqueue([_event(i, "w4") for i in range(20)])
    # Longer than the journal's busy_timeout (5 s): the writer's INSERT
    # really fails, it is not just a slow write.
    time.sleep(6.0)
    during = wq.health()._asdict() if hasattr(wq, "health") else None
    rows_during = -2
    locker.execute("COMMIT")
    locker.close()
    deadline = time.monotonic() + 12
    while _rows(db, "w4") < 20 and time.monotonic() < deadline:
        time.sleep(0.1)
    after = wq.health()._asdict() if hasattr(wq, "health") else None
    wq.stop()
    return {"outcome": _outcome_name(res), "health_during": during,
            "health_after": after, "rows": _rows(db, "w4"),
            "dups": _dup_uids(db), "rows_during": rows_during}


def _swap_outbox(wq, new_outbox) -> None:
    gen = getattr(wq, "_gen", None)
    if gen is not None:
        gen.outbox = new_outbox
    else:
        wq._outbox = new_outbox


def _w5_w6(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    res = {}
    # W5: outbox path blocked by a FILE with the outbox's name.
    db5 = _make_journal(root, "w5")
    wq = WriteQueue()
    wq.start(db5)
    try:
        outbox = getattr(wq, "_outbox", None)
        if outbox is not None:
            outbox.close()
            shutil.rmtree(outbox.dir, ignore_errors=True)
            with open(outbox.dir, "w", encoding="utf-8") as fh:
                fh.write("not a directory")
            _swap_outbox(wq, type(outbox)(db5))
        r5 = wq.enqueue([_event(i, "w5") for i in range(7)])
    finally:
        wq.stop()
    res["w5"] = {"outcome": _outcome_name(r5), "rows": _rows(db5, "w5")}
    # W6: outbox blocked AND journal replaced by garbage.
    db6 = _make_journal(root, "w6")
    wq = WriteQueue()
    wq.start(db6)
    try:
        outbox = getattr(wq, "_outbox", None)
        if outbox is not None:
            outbox.close()
            shutil.rmtree(outbox.dir, ignore_errors=True)
            with open(outbox.dir, "w", encoding="utf-8") as fh:
                fh.write("not a directory")
            _swap_outbox(wq, type(outbox)(db6))
        wq._stop_event.set()
        if wq._thread is not None:
            wq._thread.join(timeout=5)
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(db6 + suffix)
            except OSError:
                pass
        os.makedirs(db6, exist_ok=True)   # a DIRECTORY where the journal was
        r6 = wq.enqueue([_event(i, "w6") for i in range(9)])
        h6 = wq.health()._asdict() if hasattr(wq, "health") else None
    finally:
        try:
            wq.stop()
        except Exception:  # noqa: BLE001
            pass
    res["w6"] = {"outcome": _outcome_name(r6), "truthy": bool(r6), "health": h6}
    return res


def _w7(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    db = _make_journal(root, "w7")
    wq = WriteQueue()
    wq.start(db)
    bad = [_event(1, "w7"), _event(2, "w7")._replace(operation_type="BOGUS")]
    res = wq.enqueue(bad)
    wq.stop()
    return {"outcome": _outcome_name(res), "truthy": bool(res),
            "quarantine": _quarantine_files(db), "rows": _rows(db, "w7")}


def _w8(ctx, root):
    from recoverland.core.write_queue import WriteQueue
    db = _make_journal(root, "w8")
    wq = WriteQueue()
    wq.start(db)
    t0 = time.perf_counter()
    res = wq.enqueue([_event(i, "w8") for i in range(60000)])
    ui_ms = (time.perf_counter() - t0) * 1000
    deadline = time.monotonic() + 90
    while _rows(db, "w8") < 60000 and time.monotonic() < deadline:
        time.sleep(0.2)
    wq.stop()
    return {"outcome": _outcome_name(res), "truthy": bool(res),
            "ui_ms": round(ui_ms, 1), "rows": _rows(db, "w8")}


def _w9(ctx, root):
    import recoverland.core.write_queue as wqm
    db = _make_journal(root, "w9")
    saved = wqm._FLUSH_TIMEOUT_SEC
    wqm._FLUSH_TIMEOUT_SEC = 1
    try:
        wq = wqm.WriteQueue()
        wq.start(db)
        locker = sqlite3.connect(db, timeout=0.1, isolation_level=None)
        locker.execute("BEGIN EXCLUSIVE")
        res = wq.enqueue([_event(i, "w9") for i in range(15)])
        time.sleep(0.5)
        wq.stop()
        locker.execute("COMMIT")
        locker.close()
        time.sleep(0.5)
    finally:
        wqm._FLUSH_TIMEOUT_SEC = saved
    rows_after, dups, rep = _restart_and_count(db, "w9")
    return {"outcome": _outcome_name(res), "rows_after_restart": rows_after,
            "dups": dups, "replay": rep}


def run(ctx):
    from recoverland.core.logger import flog
    root = ctx.data["tmpdir"]
    flog(f"tx_wq_durable_contract run start: trace_id={ctx.trace_id}", "INFO")
    for key, fn in (("w1", _w1), ("w2b", _w2b), ("w2", _w2), ("w3", _w3), ("w4", _w4),
                    ("w56", _w5_w6), ("w7", _w7), ("w8", _w8), ("w9", _w9),
                    ("w10", _w10)):
        try:
            ctx.data[key] = fn(ctx, root)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-1200:]}
        flog(f"tx_wq_durable_contract case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:2000]}", "INFO")
    shutil.rmtree(root, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    d = ctx.data

    w1 = d.get("w1", {})
    out.append((
        "w1_nominal_is_durable_and_explicit",
        w1.get("outcome") == "QUEUED_DURABLE" and w1.get("rows") == 25
        and w1.get("segments") == 0 and w1.get("dups") == 0,
        f"W1 {w1} : issue attendue QUEUED_DURABLE, 25 lignes, outbox vide, "
        f"aucun doublon",
    ))

    w2b = d.get("w2b", {})
    out.append((
        "w2b_crash_right_after_acceptance_loses_nothing",
        w2b.get("rc") == 0 and bool(w2b.get("child_outcome"))
        and w2b.get("rows_after_restart") == 200 and w2b.get("dups") == 0,
        f"QGIS meurt juste apres qu'enqueue() a accepte 200 evenements : {w2b} ; "
        f"attendu 200 au journal apres redemarrage. Un lot accepte qui ne "
        f"vivait qu'en RAM disparait avec le processus.",
    ))

    w2 = d.get("w2", {})
    for point in ("outbox_after_fsync", "wq_after_outbox_before_queue",
                  "wq_before_sqlite_commit", "wq_after_sqlite_commit_before_ack"):
        r = w2.get(point, {})
        out.append((
            f"w2_crash_{point}_loses_nothing",
            r.get("fault_reached") is True and r.get("rows_after_restart") == 40
            and r.get("dup_uids") == 0,
            f"crash a {point}: {r} ; attendu 40 evenements au journal apres "
            f"redemarrage, exactement une fois. Le lot etait accepte : le "
            f"plantage ne doit rien effacer ni dupliquer.",
        ))
    torn = w2.get("outbox_torn_write", {})
    rep = torn.get("replay") or {}
    out.append((
        "w2_torn_write_is_quarantined_and_counted",
        torn.get("fault_reached") is True and rep.get("torn_lines", 0) >= 1
        and len(torn.get("quarantine") or []) >= 1
        and 0 <= (torn.get("rows_after_restart") or -1) < 40
        and torn.get("dup_uids") == 0,
        f"ecriture dechiree : {torn} ; attendu : les lignes completes "
        f"integrees, la ligne dechiree en quarantaine et comptee "
        f"(torn_lines>=1). Le processus est mort AVANT l'accuse de reception : "
        f"rien n'a ete promis, mais rien ne doit etre efface sans trace.",
    ))

    w3 = d.get("w3", {})
    h1 = w3.get("health1") or {}
    out.append((
        "w3_dead_writer_is_not_acknowledged_as_normal",
        w3.get("writer_alive_after_kill") is False
        and w3.get("second") == "PERSISTED_FALLBACK",
        f"W3 : ecrivain vivant apres BaseException={w3.get('writer_alive_after_kill')} "
        f"issue suivante={w3.get('second')} (attendu PERSISTED_FALLBACK) "
        f"crash={w3.get('crash')}",
    ))
    out.append((
        "w3_watchdog_reports_failure_once",
        h1.get("state") == "failed" and w3.get("alerts") == 1,
        f"chien de garde : etat={h1.get('state')} alertes={w3.get('alerts')} "
        f"(attendu failed et UNE alerte, pas une par verification)",
    ))
    out.append((
        "w3_restart_integrates_everything_once",
        w3.get("rows_after_restart") == 40 and w3.get("dups") == 0,
        f"apres redemarrage : lignes={w3.get('rows_after_restart')} attendu 40, "
        f"doublons={w3.get('dups')}",
    ))

    w4 = d.get("w4", {})
    during = w4.get("health_during") or {}
    out.append((
        "w4_locked_journal_degrades_then_recovers",
        w4.get("outcome") == "QUEUED_DURABLE" and during.get("state") == "degraded"
        and (w4.get("health_after") or {}).get("state") == "running"
        and w4.get("rows") == 20 and w4.get("dups") == 0,
        f"W4 {w4} : verrou tenu 6 s (> busy_timeout) -> etat degraded visible, puis running, "
        f"20 lignes, aucun doublon",
    ))

    w56 = d.get("w56", {})
    w5 = w56.get("w5", {}) if isinstance(w56, dict) else {}
    w6 = w56.get("w6", {}) if isinstance(w56, dict) else {}
    out.append((
        "w5_outbox_unwritable_falls_back_to_the_journal",
        w5.get("outcome") == "PERSISTED_JOURNAL_SYNC" and w5.get("rows") == 7,
        f"W5 {w5} crash={w56.get('crash') if isinstance(w56, dict) else None}",
    ))
    out.append((
        "w6_no_durable_path_is_a_counted_loud_refusal",
        w6.get("outcome") == "REFUSED_NO_DURABLE_PATH" and w6.get("truthy") is False
        and ((w6.get("health") or {}).get("lost_events") == 9),
        f"W6 {w6} : la seule issue avec perte doit etre explicite et comptee",
    ))

    w7 = d.get("w7", {})
    out.append((
        "w7_invalid_batch_is_quarantined_not_dropped",
        w7.get("outcome") == "REFUSED_INVALID" and w7.get("truthy") is False
        and len(w7.get("quarantine") or []) == 1 and w7.get("rows") == 0,
        f"W7 {w7}",
    ))

    w8 = d.get("w8", {})
    out.append((
        "w8_mass_commit_is_accepted_and_integrated",
        w8.get("outcome") == "QUEUED_DURABLE" and w8.get("rows") == 60000,
        f"W8 {w8} : un commit de 60 000 entites ne doit ni etre refuse ni "
        f"couper le suivi",
    ))

    w9 = d.get("w9", {})
    out.append((
        "w9_stop_timeout_loses_nothing",
        w9.get("rows_after_restart") == 15 and w9.get("dups") == 0,
        f"W9 {w9}",
    ))

    w10 = d.get("w10", {})
    sync = list(w10.get("sync_at_release") or [])
    out.append((
        "w10_outbox_released_only_after_a_power_safe_commit",
        w10.get("outcome") == "QUEUED_DURABLE" and w10.get("rows") == 30
        and bool(sync) and all(m == "FULL" for m in sync)
        and w10.get("segments") == 0,
        f"W10 {w10} : a chaque liberation d'un segment, le mode synchronous de "
        f"la connexion de l'ecrivain doit etre FULL ; sinon une coupure de "
        f"courant juste apres efface la seule copie durable d'editions "
        f"annoncees enregistrees",
    ))

    out.append(assert_log_contains(
        ctx.records, r"event=wq_events_lost", name="loss_is_logged", min_count=1))
    out.append(assert_log_contains(
        ctx.records, rf"tx_wq_durable_contract.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=5))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
