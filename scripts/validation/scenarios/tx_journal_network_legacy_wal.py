"""tx_journal_network_legacy_wal - RecoverLand validation runtime.

Invariant: I-JRN (un seul ecrivain coherent par journal ; la persistance se
degrade bruyamment ; l'historique reste lisible quand il ne peut plus
evoluer).

Le mecanisme
------------
Un journal sur un partage reseau ne doit pas etre en WAL (index en memoire
partagee incoherent entre deux postes) : tx_journal_network_share_pragmas
couvre un journal NEUF. Mais tous les journaux deja poses sur un partage par
les versions precedentes du plugin SONT en WAL, et ce mode est inscrit dans
le fichier. Trois cas reels a la mise a jour du plugin :

  N1 le journal est libre : il doit quitter WAL a l'ouverture, sans rien
     perdre de son contenu ;
  N2 le journal est en LECTURE SEULE (partage monte en lecture seule,
     fichier protege) : SQLite ne peut pas changer son mode. Demander
     `journal_mode=DELETE` echoue ("disk I/O error") et faisait echouer
     TOUTE l'ouverture : l'utilisateur perdait la consultation de son
     historique, que `initialize_schema` promet pourtant de servir tel quel
     en lecture seule ;
  N3 un AUTRE poste a encore le journal ouvert en WAL (ancienne version du
     plugin, dont le verrou ne se rafraichit pas et passe pour perime) :
     ecrire a cote de lui en WAL est exactement la corruption a eviter. Il
     faut refuser, et dire pourquoi.

Le scenario d'echec concret
---------------------------
N2 : le service SIG passe le partage des projets archives en lecture seule ;
l'utilisateur ouvre un projet pour consulter ce qui a ete modifie :
"Initialisation du journal impossible", plus rien n'est lisible.
N3 : un collegue n'a pas encore mis a jour le plugin et travaille sur le
meme projet reseau ; l'ancienne version ecrase les verrous etrangers : sans
refus explicite, les deux postes ecrivent le meme journal.

Ce que le scenario execute reellement
-------------------------------------
Un vrai chemin UNC (`\\\\localhost\\C$\\...`). Le journal "ancien" est cree
par le vrai `initialize_schema` sur le chemin LOCAL du meme fichier (donc en
WAL, comme l'ancienne version), avec un evenement. Puis
`JournalManager.open_for_project` via le chemin UNC, et lecture par
`create_read_connection`.

Verdict attendu avant correctif : FAIL. Sur le code d'origine : N1 (reste en
WAL) et N3 (ouvre et ecrit en WAL a cote de l'autre poste). Sur le v7 avant
ce correctif : N2 (ouverture impossible) et le motif du refus N3.
"""
from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import stat
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_network_legacy_wal"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_MARKER = "tx_jnlw_marker"
_OLD_HOST = "RL-POSTE-ANCIENNE-VERSION"


def _unc_mirror(local_dir: str) -> str:
    drive, rest = os.path.splitdrive(os.path.abspath(local_dir))
    if len(drive) != 2 or drive[1] != ":":
        return ""
    return "\\\\localhost\\" + drive[0] + "$" + rest


def _insert_marker(conn: sqlite3.Connection, marker: str) -> None:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    sql = "".join((
        "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
        ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
    ))
    conn.execute(sql, (
        "tx_jnlw_project", "tx_jnlw_ds", "lyr_jnlw", marker, "ogr",
        json.dumps({"fid": 1}), "UPDATE",
        json.dumps({"changed_only": {"nom": {"old": "A", "new": "B"}}}),
        None, "NoGeometry", "EPSG:2154", None, "tester", None,
        "2026-09-27T10:00:00+00:00", None, "fid:1", 2, None, None,
    ))
    conn.commit()


def _legacy_project(local_base: str, unc_base: str, name: str) -> dict:
    """A project on the share whose journal the previous version left in WAL."""
    from recoverland.core.sqlite_schema import initialize_schema

    local_dir = os.path.join(local_base, name)
    os.makedirs(os.path.join(local_dir, ".recoverland"), exist_ok=True)
    Path(os.path.join(local_dir, "projet.qgz")).write_text("<qgis/>", encoding="utf-8")
    local_journal = os.path.join(local_dir, ".recoverland", "recoverland_audit.sqlite")
    conn = sqlite3.connect(local_journal)
    try:
        initialize_schema(conn)
        _insert_marker(conn, _MARKER)
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    finally:
        conn.close()
    unc_dir = os.path.join(unc_base, name)
    return {
        "local_journal": local_journal,
        "legacy_mode": mode,
        "project": os.path.join(unc_dir, "projet.qgz"),
        "journal": os.path.join(unc_dir, ".recoverland", "recoverland_audit.sqlite"),
    }


def _open_and_read(case: dict, profile: str) -> dict:
    from recoverland.core.journal_manager import JournalManager, JournalLockError

    res = {"opened": False, "refused": "", "error": "", "lock_reason": ""}
    jm = JournalManager()
    try:
        jm.open_for_project(case["project"], profile)
        res["opened"] = jm.is_open
        res["main_mode"] = str(jm.get_connection().execute(
            "PRAGMA journal_mode").fetchone()[0]).lower()
        reader = jm.create_read_connection()
        try:
            res["reader_mode"] = str(reader.execute(
                "PRAGMA journal_mode").fetchone()[0]).lower()
            res["read_back"] = int(reader.execute(
                "SELECT COUNT(*) FROM audit_event WHERE layer_name_snapshot = ?",
                (_MARKER,)).fetchone()[0])
        finally:
            reader.close()
    except JournalLockError as exc:
        res["refused"] = str(exc)
        res["lock_reason"] = getattr(exc, "reason", "")
    except Exception as exc:  # noqa: BLE001 - a failure is a result
        res["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        jm.close()
    return res


def _mode_of(path: str) -> str:
    conn = sqlite3.connect(path)
    try:
        return str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
    finally:
        conn.close()


def _n1_free(local_base, unc_base, profile) -> dict:
    case = _legacy_project(local_base, unc_base, "n1_libre")
    res = _open_and_read(case, profile)
    res["legacy_mode"] = case["legacy_mode"]
    return res


def _n2_read_only(local_base, unc_base, profile) -> dict:
    case = _legacy_project(local_base, unc_base, "n2_lecture_seule")
    os.chmod(case["local_journal"], stat.S_IREAD)
    try:
        res = _open_and_read(case, profile)
    finally:
        os.chmod(case["local_journal"], stat.S_IREAD | stat.S_IWRITE)
    res["legacy_mode"] = case["legacy_mode"]
    return res


def _n3_old_client(local_base, unc_base, profile) -> dict:
    case = _legacy_project(local_base, unc_base, "n3_ancien_poste")
    # The older client: WAL connection kept open, a lock written once an
    # hour ago and never refreshed (the old format), which the heartbeat
    # rule takes for a dead instance.
    lock_path = case["journal"] + ".rlwriter"
    ts = time.time() - 3600
    with open(lock_path, "w", encoding="utf-8") as fh:
        fh.write(f"4747|{_OLD_HOST}|{int(ts)}\n")
    os.utime(lock_path, (ts, ts))
    other = sqlite3.connect(case["journal"], check_same_thread=False)
    res = {}
    try:
        other.execute("PRAGMA journal_mode=WAL")
        before = int(other.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
        started = time.monotonic()
        res = _open_and_read(case, profile)
        res["elapsed_s"] = round(time.monotonic() - started, 1)
        _insert_marker(other, "tx_jnlw_other_after")
        res["other_rows"] = (before, int(other.execute(
            "SELECT COUNT(*) FROM audit_event").fetchone()[0]))
        res["other_mode"] = str(other.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        res["other_integrity"] = str(other.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        other.close()
    res["legacy_mode"] = case["legacy_mode"]
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    base = tempfile.mkdtemp(prefix="rl_tx_jnlw_")
    ctx.data["base"] = base
    local_base = os.path.join(base, "partage")
    os.makedirs(local_base, exist_ok=True)
    ctx.data["local_base"] = local_base
    unc_base = _unc_mirror(local_base)
    ctx.data["unc_base"] = unc_base
    ctx.data["unc_reachable"] = bool(unc_base) and os.path.isdir(unc_base)
    ctx.data["profile"] = os.path.join(base, "profil")
    os.makedirs(ctx.data["profile"], exist_ok=True)
    flog(f"tx_journal_network_legacy_wal setup: trace_id={ctx.trace_id} "
         f"base={base} unc={unc_base!r} reachable={ctx.data['unc_reachable']} "
         f"host={socket.gethostname()}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    d = ctx.data
    if d["unc_reachable"]:
        for key, fn in (("n1", _n1_free), ("n2", _n2_read_only),
                        ("n3", _n3_old_client)):
            try:
                d[key] = fn(d["local_base"], d["unc_base"], d["profile"])
            except Exception as exc:  # noqa: BLE001 - reported as a FAIL
                d[key] = {"crash": f"{type(exc).__name__}: {exc}"}
    flog(f"tx_journal_network_legacy_wal run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: d.get(k) for k in ('n1', 'n2', 'n3')}, default=str)}",
         "INFO")
    shutil.rmtree(d["base"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    n1 = d.get("n1") or {}
    n2 = d.get("n2") or {}
    n3 = d.get("n3") or {}
    out.append((
        "fixture_partage_et_journaux_anciens_en_wal",
        d.get("unc_reachable") is True
        and all(c.get("legacy_mode") == "wal" for c in (n1, n2, n3)),
        f"partage {d.get('unc_base')!r} joignable={d.get('unc_reachable')} ; "
        f"mode des journaux anciens={[c.get('legacy_mode') for c in (n1, n2, n3)]} "
        f"(attendu wal) : sans journal WAL existant, le cas de la mise a jour "
        f"ne prouve rien",
    ))
    out.append((
        "n1_journal_libre_quitte_wal_sans_perte",
        n1.get("opened") is True and n1.get("main_mode") == "delete"
        and n1.get("reader_mode") == "delete" and n1.get("read_back") == 1,
        f"journal ancien libre sur le partage : ouvert={n1.get('opened')} "
        f"mode={n1.get('main_mode')}/{n1.get('reader_mode')} relu="
        f"{n1.get('read_back')} erreur={n1.get('error') or n1.get('crash')!r} : "
        f"reste en WAL, il se corrompt des que deux postes l'ouvrent",
    ))
    out.append((
        "n2_journal_en_lecture_seule_reste_consultable",
        n2.get("opened") is True and n2.get("read_back") == 1,
        f"journal ancien en LECTURE SEULE sur le partage : ouvert="
        f"{n2.get('opened')} relu={n2.get('read_back')} erreur="
        f"{n2.get('error') or n2.get('crash')!r} : l'utilisateur doit pouvoir "
        f"consulter l'historique d'un projet archive ; rien n'y sera ecrit",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"sqlite_schema: .*network share.* stays in WAL mode.*read-only",
        name="n2_lecture_seule_en_wal_tracee",
        min_count=1,
    ))
    out.append((
        "n3_ancien_poste_en_wal_refuse",
        not n3.get("opened") and bool(n3.get("refused") or n3.get("error")),
        f"un autre poste tient le journal en WAL (ancienne version, verrou "
        f"perime) : ouvert={n3.get('opened')} refus={n3.get('refused')!r} "
        f"erreur={n3.get('error') or n3.get('crash')!r} : ecrire a cote de lui "
        f"en WAL sur un partage corrompt le journal des deux postes",
    ))
    rows = n3.get("other_rows") or (None, None)
    out.append((
        "n3_journal_de_l_autre_poste_intact",
        n3.get("other_mode") == "wal" and n3.get("other_integrity") == "ok"
        and rows[0] is not None and rows[1] == rows[0] + 1,
        f"journal de l'autre poste apres notre tentative : mode="
        f"{n3.get('other_mode')} integrite={n3.get('other_integrity')!r} lignes "
        f"avant/apres son propre commit={rows} : notre refus ne doit rien "
        f"casser chez lui",
    ))
    out.append((
        "n3_refus_nomme_la_cause",
        bool(n3.get("refused")) and "WAL" in (n3.get("refused") or "")
        and bool(n3.get("lock_reason")),
        f"refus={n3.get('refused')!r} motif={n3.get('lock_reason')!r} erreur="
        f"{n3.get('error')!r} : 'database is locked' ne dit pas a l'utilisateur "
        f"qu'un autre poste tient ce journal ni quoi faire",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"sqlite_schema: journal on a network share .* is in WAL mode and cannot leave it",
        name="n3_refus_trace",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_network_legacy_wal.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
