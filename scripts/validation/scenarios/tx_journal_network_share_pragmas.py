"""tx_journal_network_share_pragmas - RecoverLand validation runtime.

Invariant: I-JRN (la persistance se degrade bruyamment, elle ne fait pas
tomber l'application hote ; le stockage ne se corrompt pas par sa propre
configuration).

Le mecanisme
------------
`sqlite_schema.apply_pragmas` applique la meme liste a TOUTES les connexions
(celle du JournalManager, celle du thread ecrivain, celles des lectures) :

    PRAGMA journal_mode=WAL
    PRAGMA mmap_size=268435456

Or un journal vit a cote de son projet, et un projet sur un partage reseau
(`\\\\serveur\\sig\\commune\\projet.qgz`, ou un lecteur reseau `S:`) est un
emplacement normal. Deux consequences documentees par SQLite :

  * WAL repose sur un index en memoire partagee (fichier `-shm` mappe) qui
    n'est pas coherent entre deux clients reseau : deux postes qui ouvrent le
    meme journal voient chacun leur propre vue du WAL ; des commits se
    perdent ou le fichier devient illisible ("database disk image is
    malformed") ;
  * une erreur d'E/S sur une page memoire-mappee ne remonte PAS comme une
    erreur SQLite : c'est une exception materielle (EXCEPTION_IN_PAGE_ERROR
    sous Windows) qui termine le processus.

Le scenario d'echec concret
---------------------------
Un technicien travaille sur `\\\\srv\\sig\\commune\\projet.qgz`. Le VPN se
reconnecte : le partage disparait deux secondes pendant qu'une recherche
lit des pages mappees du journal. QGIS se ferme sans message ; les
modifications acceptees mais pas encore indexees ne sont plus que dans
l'outbox, et l'utilisateur a perdu sa session.

Ce que le scenario execute reellement
-------------------------------------
Un vrai chemin UNC, via le partage administratif local
(`\\\\localhost\\C$\\...`) : SQLite y passe par le redirecteur SMB comme
pour un vrai serveur. Pour un temoin local et pour le cas UNC :
JournalManager.open_for_project(), puis lecture de `PRAGMA journal_mode` et
`PRAGMA mmap_size` sur les QUATRE connexions du produit (principale,
ecrivain, lecture, lecture seule Time Lens), puis un aller-retour
ecriture/lecture pour prouver que le journal reste utilisable.

Verdict attendu avant correctif : FAIL (WAL et mmap 256 Mo sur le partage).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_journal_network_share_pragmas"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_MARKER = "tx_jnet_marker"


def _unc_mirror(local_dir: str) -> str:
    """Meme dossier, vu a travers le partage administratif SMB local."""
    drive, rest = os.path.splitdrive(os.path.abspath(local_dir))
    if len(drive) != 2 or drive[1] != ":":
        return ""
    return "\\\\localhost\\" + drive[0] + "$" + rest


def _pragmas(conn: sqlite3.Connection) -> dict:
    return {
        "journal_mode": str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
        "mmap_size": int(conn.execute("PRAGMA mmap_size").fetchone()[0]),
        "synchronous": int(conn.execute("PRAGMA synchronous").fetchone()[0]),
    }


def _insert_marker(conn: sqlite3.Connection) -> None:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    sql = "".join((
        "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
        ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
    ))
    conn.execute(sql, (
        "tx_jnet_project", "tx_jnet_ds", "lyr_jnet", _MARKER, "ogr",
        json.dumps({"fid": 1}), "UPDATE",
        json.dumps({"changed_only": {"nom": {"old": "A", "new": "B"}}}),
        None, "NoGeometry", "EPSG:2154", None, "tester", None,
        "2026-09-27T10:00:00+00:00", None, "fid:1", 2, None, None,
    ))
    conn.commit()


def _exercise(project_dir: str) -> dict:
    from recoverland.core.journal_manager import JournalManager

    res = {"project_dir": project_dir}
    os.makedirs(project_dir, exist_ok=True)
    project = os.path.join(project_dir, "projet.qgz")
    with open(project, "w", encoding="utf-8") as fh:
        fh.write("<qgis/>")
    jm = JournalManager()
    conns = []
    try:
        res["journal"] = jm.open_for_project(project, project_dir)
        main = jm.get_connection()
        res["main"] = _pragmas(main)
        writer = jm.create_write_connection()
        conns.append(writer)
        res["writer"] = _pragmas(writer)
        reader = jm.create_read_connection()
        conns.append(reader)
        res["reader"] = _pragmas(reader)
        lens = jm.open_readonly_connection()
        conns.append(lens)
        res["lens"] = _pragmas(lens)
        _insert_marker(writer)
        res["read_back"] = int(reader.execute(
            "SELECT COUNT(*) FROM audit_event WHERE layer_name_snapshot = ?",
            (_MARKER,)).fetchone()[0])
    except Exception as exc:  # noqa: BLE001 - une panne est un resultat
        res["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        for conn in conns:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        jm.close()
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    base = tempfile.mkdtemp(prefix="rl_tx_jnet_")
    ctx.data["base"] = base
    unc_base = _unc_mirror(base)
    ctx.data["unc_base"] = unc_base
    ctx.data["unc_reachable"] = bool(unc_base) and os.path.isdir(unc_base)
    flog(f"tx_journal_network_share_pragmas setup: trace_id={ctx.trace_id} "
         f"base={base} unc={unc_base!r} reachable={ctx.data['unc_reachable']}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    base = ctx.data["base"]
    ctx.data["local"] = _exercise(os.path.join(base, "local"))
    if ctx.data["unc_reachable"]:
        ctx.data["unc"] = _exercise(os.path.join(ctx.data["unc_base"], "partage"))
    flog(f"tx_journal_network_share_pragmas run: trace_id={ctx.trace_id} "
         f"local={json.dumps(ctx.data.get('local'), default=str)} "
         f"unc={json.dumps(ctx.data.get('unc'), default=str)}", "INFO")
    shutil.rmtree(base, ignore_errors=True)


_KINDS = ("main", "writer", "reader", "lens")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    local = ctx.data.get("local") or {}
    unc = ctx.data.get("unc") or {}

    out.append((
        "fixture_unc_joignable",
        ctx.data.get("unc_reachable") is True,
        f"partage administratif {ctx.data.get('unc_base')!r} joignable="
        f"{ctx.data.get('unc_reachable')} : sans chemin UNC reel, le cas "
        f"reseau ne prouve rien",
    ))
    # Hypothesis revised (v7): this witness required mmap_size > 0 on the
    # local writer, i.e. that a local journal keeps a 256 MB mapping. On
    # Windows that mapping is itself a defect, measured: VACUUM plus
    # wal_checkpoint(TRUNCATE) never shrinks a mapped journal (5 140 480
    # bytes before and after, 8 192 without mmap), and an I/O error on a
    # mapped page terminates QGIS. So the local expectation is now the
    # platform's local value (0 on Windows, > 0 elsewhere). What the witness
    # is for, "the local case keeps the LOCAL settings, not the network
    # ones", is checked more strictly than before: WAL on all FOUR
    # connections (was: the main one) and synchronous=NORMAL on the main
    # and writer connections (the network set uses FULL).
    local_mmap_ok = all(
        ((local.get(k) or {}).get("mmap_size", -1) == 0) if os.name == "nt"
        else ((local.get(k) or {}).get("mmap_size", 0) > 0)
        for k in ("main", "writer"))
    out.append((
        "local_garde_wal_et_mmap",
        not local.get("error")
        and all((local.get(k) or {}).get("journal_mode") == "wal" for k in _KINDS)
        and all((local.get(k) or {}).get("synchronous") == 1
                for k in ("main", "writer"))
        and local_mmap_ok,
        f"temoin local : {local} : un journal sur disque local garde WAL "
        f"(lectures concurrentes de l'ecriture) et synchronous=NORMAL ; "
        f"mmap_size attendu {'0 (Windows)' if os.name == 'nt' else '> 0'} ; "
        f"ce temoin prouve que le correctif ne degrade pas le cas nominal",
    ))
    out.append((
        "unc_utilisable",
        bool(unc) and not unc.get("error") and unc.get("read_back") == 1,
        f"journal sur partage : erreur={unc.get('error')!r} relu="
        f"{unc.get('read_back')} : le journal doit rester utilisable sur un "
        f"partage, sans quoi l'utilisateur perd la capture",
    ))
    modes = {k: (unc.get(k) or {}).get("journal_mode") for k in _KINDS}
    out.append((
        "unc_pas_de_wal",
        bool(unc) and all(m == "delete" for m in modes.values()),
        f"journal_mode par connexion sur le partage={modes} (attendu "
        f"'delete' partout) : l'index WAL en memoire partagee n'est pas "
        f"coherent entre deux postes ; deux techniciens sur le meme projet "
        f"reseau perdent des commits ou corrompent le journal",
    ))
    mmaps = {k: (unc.get(k) or {}).get("mmap_size") for k in _KINDS}
    out.append((
        "unc_pas_de_mmap",
        bool(unc) and all(v == 0 for v in mmaps.values()),
        f"mmap_size par connexion sur le partage={mmaps} (attendu 0 "
        f"partout) : une coupure reseau pendant la lecture d'une page mappee "
        f"termine QGIS au lieu de renvoyer une erreur",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"sqlite_schema: journal on a network share .*journal_mode=DELETE.*mmap",
        name="unc_regime_trace_dans_le_journal",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_network_share_pragmas.*trace_id={ctx.trace_id}",
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
