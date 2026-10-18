"""tx_journal_save_as_readonly_source - RecoverLand validation runtime.

Invariant: I-JRN / I-CAPTURE (une edition acceptee finit dans le journal
que le projet lira ; l'utilisateur n'est jamais rassure a tort).

Le mecanisme
------------
Quand un projet est "Enregistre sous" dans un autre dossier, l'historique
de la session suit le projet : a la fermeture du journal d'origine,
`carry_journal_history` le copie dans le journal du nouveau projet. Si ce
rattachement echoue, `open_for_project` rouvrait le journal d'ORIGINE pour
continuer la capture "la ou est l'historique", et le message disait
"L'enregistrement continue dans <origine>, sans perte".

Or le rattachement ECRIVAIT dans le journal d'origine avant de le lire
(schema v7, identifiants des lignes anciennes, rejeu de l'outbox) : sur un
journal en lecture seule (projet archive, partage monte en lecture seule)
il echouait meme quand il n'y avait rien a rattacher ("attempt to write a
readonly database"). Le repli rouvrait alors ce journal en lecture seule.

Le scenario d'echec concret
---------------------------
L'utilisateur ouvre Q, un projet archive dont le journal est en lecture
seule, et fait "Enregistrer sous" P dans son dossier de travail pour
continuer. Le message lui dit que l'enregistrement continue "sans perte".
Il modifie des parcelles : ces modifications ne sont dans AUCUN journal
(bloquees dans l'outbox du journal archive, avec une alerte qui promet une
integration "des que le journal sera accessible", jamais). Il rouvre P le
lendemain : Restaurer / Rewind ne montrent rien.

Ce que le scenario execute reellement
-------------------------------------
Partie A (VRAI RecoverPlugin, faux iface, vraie couche GeoPackage, vrais
`QgsProject.write` / `clear` / `read`) :
  A1 Q enregistre, une modification capturee, Q ferme ; le fichier journal
     de Q passe en lecture seule ; Q rouvert ; une modification faite
     pendant que Q est ouvert en lecture seule ; "Enregistrer sous" P dans
     un autre dossier.
  A2 le journal courant doit etre celui de P, aucun message "sans perte" ;
     la modification faite sur Q doit avoir suivi P.
  A3 une modification apres l'enregistrement doit etre dans le journal de
     P ; apres reouverture de P, le journal rouvert est celui de P et les
     deux modifications y sont lues par le chemin de lecture du produit.
Partie B (API) : `carry_journal_history` depuis une source en LECTURE SEULE
  qui contient des lignes anciennes (sans event_uid) et une ligne v7, puis
  depuis une source au schema v6 (colonne event_uid absente) : le
  rattachement reussit, rien n'est ecrit dans la source (octets identiques),
  et un second rattachement n'insere rien (identite deterministe).
Partie C (API + vraie seconde instance) : le journal de P est tenu par une
  autre instance ET le journal de session est en lecture seule. Le repli
  vers la source serait une capture dans un journal qui refuse tout : il
  ne doit pas avoir lieu, et aucun message ne doit dire "sans perte".
Partie D (API, deux sessions) : le journal archive de Q est en lecture
  seule. Lundi, Q est ouvert et une modification part dans son outbox.
  Mardi, Q est rouvert : une modification (avec sa ligne de commit) et une
  trace de restauration qui compense un evenement ancien de Q partent dans
  l'outbox, puis "Enregistrer sous" P. L'outbox entiere etait refusee des
  qu'une ligne venait d'une autre session : le rattachement se disait
  reussi ("0 modification(s) rattachee(s)"), l'obligation etait oubliee, et
  l'historique de P n'avait rien de mardi. Attendu : les lignes de MARDI
  seulement suivent P (la trace pointe sur la copie de l'evenement qu'elle
  compense dans le journal de P, le commit est au registre de P), celle de
  lundi reste dans l'outbox de Q, rien n'est ecrit dans la source (journal
  et outbox identiques octet pour octet), et le message le dit.
Partie E (API) : meme depart, mais un segment de l'outbox de Q est
  illisible au moment de l'enregistrement (verrou de plage d'octets tenu
  par un autre descripteur). Le rattachement doit echouer cote journal
  d'origine, rester du, et le dire ; une fois le segment lisible, la
  reouverture de P rattache les lignes de mardi.
Partie F (API) : le fichier journal de Q est inscriptible en apparence mais
  refuse toute nouvelle ligne (disque plein, partage en lecture seule par
  ses seuls droits ; simule par un declencheur qui refuse l'insertion) ; une
  modification de la session attend dans son outbox. Le rejeu dans Q
  echouait, le lot restait dans l'outbox, et P ne recevait jamais la
  modification. Attendu : elle suit P depuis l'outbox, qui reste intacte.

Verdict attendu avant correctif : FAIL (A2, A3, B, C ; D, E et F sur le
code qui refusait l'outbox entiere ou ne la lisait que pour un journal en
lecture seule).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import subprocess  # nosec B404: processus QGIS fils lance par le test
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_save_as_readonly_source"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ON_Q = "RO_EDIT_IN_Q"
_ON_RO_Q = "RO_EDIT_WHILE_ARCHIVED"
_AFTER = "RO_EDIT_AFTER_SAVE_AS"

# Another QGIS instance holding a project's journal (product code).
_HOLDER_CODE = r'''
import os, sys
sys.path.insert(0, {parent!r})
from qgis.core import QgsApplication
app = QgsApplication([], False)
app.initQgis()
from {package}.core.journal_manager import JournalManager
jm = JournalManager()
try:
    jm.open_for_project({project!r}, {profile!r})
    print("READY " + str(os.getpid()), flush=True)
except Exception as exc:
    print("FAILED " + type(exc).__name__ + ": " + str(exc), flush=True)
    sys.exit(3)
sys.stdin.readline()
jm.close()
print("CLOSED", flush=True)
'''


def _base():
    from scripts.validation.scenarios import tx_journal_follows_saved_project as base
    return base


def _norm(path) -> str:
    """One spelling per file (the extended-length prefix aside)."""
    if not path:
        return ""
    p = str(path)
    if p.startswith("\\\\?\\"):
        p = p[4:]
    return os.path.normcase(os.path.abspath(p))


def _journal_of(plugin):
    return plugin._journal.path if plugin._journal.is_open else None


def _digest(path: str) -> str:
    """Bytes and mtime of the journal file, and whether its WAL holds frames."""
    with open(path, "rb") as fh:
        data = hashlib.sha256(fh.read()).hexdigest()
    try:
        wal = os.path.getsize(path + "-wal")
    except OSError:
        wal = 0
    return f"{data}:{os.stat(path).st_mtime_ns}:wal_frames={wal > 0}"


def _writable_again(root: str) -> None:
    for folder, _dirs, files in os.walk(root):
        for name in files:
            try:
                os.chmod(os.path.join(folder, name), stat.S_IREAD | stat.S_IWRITE)
            except OSError:
                pass


# ----------------------------------------------------------------------
# Part A: the real plugin
# ----------------------------------------------------------------------

def _part_a(tmp: str, gpkg: str) -> dict:
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    b = _base()
    res = {}
    d_q = os.path.join(tmp, "archive_Q")
    d_p = os.path.join(tmp, "travail_P")
    os.makedirs(d_q, exist_ok=True)
    os.makedirs(d_p, exist_ok=True)
    q = os.path.join(d_q, "Q.qgz")
    p = os.path.join(d_p, "P.qgz")
    res["expected_p_journal"] = os.path.join(d_p, ".recoverland",
                                             "recoverland_audit.sqlite")
    QgsProject.instance().clear()
    plugin = RecoverPlugin(FakeIface())
    try:
        plugin.initGui()
        pump(0.3)
        layer = QgsVectorLayer(f"{gpkg}|layername=parcelles", "parcelles", "ogr")
        QgsProject.instance().addMapLayer(layer)
        res["write_q"] = bool(QgsProject.instance().write(q))
        pump(1.0)
        res["commit_q"] = b._rename(b._project_layer(), 0, _ON_Q)
        res["captured_q"] = b._wait_current(plugin, _ON_Q)
        j_q = _journal_of(plugin)
        res["q_journal"] = j_q
        QgsProject.instance().clear()
        pump(0.5)
        os.chmod(j_q, stat.S_IREAD)          # the archive becomes read-only
        res["read_q"] = bool(QgsProject.instance().read(q))
        pump(0.5)
        res["journal_on_ro_q"] = _journal_of(plugin)
        res["commit_on_ro_q"] = b._rename(b._project_layer(), 1, _ON_RO_Q)
        pump(2.0)
        bar0 = len(plugin.iface.bar.texts())
        res["save_as_p"] = bool(QgsProject.instance().write(p))   # "Save As"
        pump(1.5)
        res["journal_after_save_as"] = _journal_of(plugin)
        res["bar_after_save_as"] = plugin.iface.bar.texts()[bar0:]
        res["ro_edit_in_p_after_save_as"] = b._count(
            res["expected_p_journal"], _ON_RO_Q)
        res["commit_after"] = b._rename(b._project_layer(), 2, _AFTER)
        res["captured_after"] = b._wait_current(plugin, _AFTER)
        res["after_in_p"] = b._count(res["expected_p_journal"], _AFTER)
        res["bar_after_edit"] = plugin.iface.bar.texts()[bar0:]
        QgsProject.instance().clear()
        pump(0.5)
        res["reread_p"] = bool(QgsProject.instance().read(p))
        pump(1.0)
        res["reopened_journal"] = _journal_of(plugin)
        res["counts_reopened"] = b._counts_through_reader(
            plugin, (_ON_RO_Q, _AFTER))
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            res["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    return res


# ----------------------------------------------------------------------
# Part B: carry from a read-only source (API)
# ----------------------------------------------------------------------

def _v6_source(path: str) -> None:
    """A journal as the v6 plugin left it: no event_uid / commit_id column."""
    from recoverland.core import sqlite_schema
    conn = sqlite3.connect(path)
    try:
        with conn:
            for ddl in sqlite_schema._TABLE_DDL:
                conn.execute(ddl)
            conn.execute("INSERT INTO schema_version (version_number, applied_at, "
                         "description) VALUES (6, '2025-01-02T03:04:05+00:00', 'v6')")
            cols = list(sqlite_schema.AUDIT_EVENT_INSERT_COLUMNS)
            sql = (f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
                   f"VALUES ({','.join(['?'] * len(cols))})")
            for i, marker in enumerate(("V6_A", "V6_B")):
                values = {
                    "project_fingerprint": "ro_src", "datasource_fingerprint": "ro_ds",
                    "layer_id_snapshot": "lyr", "layer_name_snapshot": "parcelles",
                    "provider_type": "ogr",
                    "feature_identity_json": json.dumps({"fid": 50 + i}),
                    "operation_type": "UPDATE",
                    "attributes_json": json.dumps({"marker": marker}),
                    "geometry_wkb": None, "geometry_type": "NoGeometry",
                    "crs_authid": "EPSG:2154", "field_schema_json": None,
                    "user_name": "tester", "session_id": "s_v6",
                    "created_at": f"2026-09-0{i + 1}T10:00:00+00:00",
                    "restored_from_event_id": None,
                    "entity_fingerprint": f"fid:{50 + i}", "event_schema_version": 2,
                    "new_geometry_wkb": None, "invalidated_at": None,
                }
                conn.execute(sql, [values[c] for c in cols])
        conn.execute("PRAGMA journal_mode=DELETE")
    finally:
        conn.close()


def _markers(conn) -> dict:
    out = {}
    for (attrs,) in conn.execute(
            "SELECT attributes_json FROM audit_event WHERE project_fingerprint IN "
            "('carry_src', 'ro_src')"):
        marker = json.loads(attrs).get("marker")
        out[marker] = out.get(marker, 0) + 1
    return out


def _part_b(tmp: str) -> dict:
    from recoverland.core.journal_manager import (
        JournalManager, carry_journal_history,
    )

    b = _base()
    res = {}
    profile = os.path.join(tmp, "profil_b")
    os.makedirs(profile, exist_ok=True)
    # B1: a session journal with legacy rows and a v7 row, then read-only.
    src_jm = JournalManager()
    src = src_jm.open_for_project("", profile)
    conn = src_jm.get_connection()
    b._fixture_schema(conn)
    with conn:
        b._insert(conn, "RO_LEGACY_1", "UPDATE", 1)
        b._insert(conn, "RO_LEGACY_2", "DELETE", 2)
        b._insert(conn, "RO_V7", "INSERT", 3, uid="uid-ro-v7", commit_id="c-ro-1")
    src_jm.close()
    os.chmod(src, stat.S_IREAD)
    # B2: a v6-shaped journal, read-only.
    v6 = os.path.join(tmp, "v6", "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(v6), exist_ok=True)
    _v6_source(v6)
    os.chmod(v6, stat.S_IREAD)
    res["digest_before"] = {"b1": _digest(src), "b2": _digest(v6)}

    pdir = os.path.join(tmp, "projet_b")
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    dst_jm = JournalManager()
    try:
        dst = dst_jm.open_for_project(project, profile)
        dconn = dst_jm.get_connection()
        reports = {"b1": [], "b2": []}
        for _ in range(2):
            for key, source in (("b1", src), ("b2", v6)):
                try:
                    rep = carry_journal_history(source, dconn, dst, since_event_id=0)
                    reports[key].append(rep.as_dict())
                except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
                    reports[key].append({"ok": False, "error": repr(exc)})
        res["reports"] = reports
        res["markers"] = _markers(dconn)
    finally:
        dst_jm.close()
    res["digest_after"] = {"b1": _digest(src), "b2": _digest(v6)}
    return res


# ----------------------------------------------------------------------
# Part C: the project's journal is held AND the source is read-only
# ----------------------------------------------------------------------

def _part_c(tmp: str) -> dict:
    from recoverland.core.journal_manager import JournalManager, JournalLockError

    b = _base()
    res = {"holder_ready": "", "holder_closed": ""}
    profile = os.path.join(tmp, "profil_c")
    os.makedirs(profile, exist_ok=True)
    pdir = os.path.join(tmp, "projet_c")
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "P.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    holder_py = os.path.join(tmp, "holder_c.py")
    Path(holder_py).write_text(_HOLDER_CODE.format(
        parent=str(_PLUGIN_ROOT.parent), package=_PLUGIN_ROOT.name,
        project=project, profile=os.path.join(tmp, "profil_autre")),
        encoding="utf-8")
    proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
        [sys.executable, holder_py], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    jm = JournalManager()
    session = ""
    try:
        res["holder_ready"] = (proc.stdout.readline() or "").strip()
        session = jm.open_for_project("", profile)
        conn = jm.get_connection()
        b._fixture_schema(conn)
        with conn:
            b._insert(conn, "RO_C_SESSION", "UPDATE", 7, uid="uid-ro-c")
        res["planned"] = jm.plan_history_carry(project, profile) is not None
        # The save rewrites P (a save that leaves it untouched is a failed one).
        Path(project).write_text("<qgis version='saved'/>", encoding="utf-8")
        os.chmod(session, stat.S_IREAD)       # the session journal turns read-only
        try:
            res["opened"] = jm.open_for_project(project, profile)
        except JournalLockError as exc:
            res["refused"] = str(exc)[:300]
        except Exception as exc:  # noqa: BLE001 - reported
            res["error"] = f"{type(exc).__name__}: {exc}"
        res["journal_now"] = jm.path
        rep = jm.last_carry_report
        res["report"] = rep.as_dict() if rep else None
        res["message"] = list(rep.user_message()) if rep else None
    finally:
        try:
            jm.close()
        except Exception as exc:  # noqa: BLE001
            res["close_error"] = repr(exc)
        if session:
            try:
                os.chmod(session, stat.S_IREAD | stat.S_IWRITE)
            except OSError:
                pass
        try:
            proc.stdin.write("\n")
            proc.stdin.flush()
            res["holder_closed"] = (proc.stdout.readline() or "").strip()
            proc.wait(timeout=30)
        except Exception:  # noqa: BLE001
            pass
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    res["session_journal"] = session
    return res


# ----------------------------------------------------------------------
# Parts D and E: a read-only journal opened twice, outbox of two sessions
# ----------------------------------------------------------------------

_D = {"old": "D_ARCHIVED_OLD", "monday": "D_MONDAY_EDIT",
      "tuesday": "D_TUESDAY_EDIT", "trace": "D_TUESDAY_RESTORE_TRACE"}


def _segments(journal: str) -> dict:
    from recoverland.core.capture_outbox import list_segments, outbox_dir_for
    out = {}
    for _seq, path in list_segments(outbox_dir_for(journal)):
        with open(path, "rb") as fh:
            out[os.path.basename(path)] = hashlib.sha256(fh.read()).hexdigest()
    return out


def _two_sessions(tmp: str, name: str) -> dict:
    """Q archived (read-only journal), Monday then Tuesday in its outbox.

    Returns the open JournalManager of Tuesday, before the "Save As".
    """
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.capture_outbox import CaptureOutbox
    from scripts.validation.scenarios import tx_journal_owed_carry_bounds as ob

    fx = {}
    profile = os.path.join(tmp, f"profil_{name}")
    d1 = os.path.join(tmp, f"archive_{name}")
    d2 = os.path.join(tmp, f"travail_{name}")
    for folder in (profile, d1, d2):
        os.makedirs(folder, exist_ok=True)
    q = os.path.join(d1, "Q.qgz")
    Path(q).write_text("<qgis/>", encoding="utf-8")
    fx.update(profile=profile, q=q, p=os.path.join(d2, "P.qgz"),
              p_journal=os.path.join(d2, ".recoverland", "recoverland_audit.sqlite"))
    jm = JournalManager()
    j_q = jm.open_for_project(q, profile)
    ob._insert(jm.get_connection(), _D["old"], f"u-{name}-old")
    old_id = jm.get_connection().execute(
        "SELECT event_id FROM audit_event WHERE event_uid = ?",
        (f"u-{name}-old",)).fetchone()[0]
    jm.close()
    os.chmod(j_q, stat.S_IREAD)                    # Q is archived: read-only
    fx["q_journal"] = j_q
    # Monday: Q opened read-only, one edit waits in its outbox; QGIS closed.
    jm = JournalManager()
    jm.open_for_project(q, profile)
    box = CaptureOutbox(j_q)
    box.append_events(f"b-{name}-mon", [ob._values(_D["monday"], f"u-{name}-mon",
                                                   ob._now())])
    box.close()
    jm.close()
    time.sleep(0.05)
    # Tuesday: Q opened again; a committed edit and a restore trace.
    jm = JournalManager()
    jm.open_for_project(q, profile)
    time.sleep(0.02)
    box = CaptureOutbox(j_q)
    box.append_prepare(f"c-{name}-tue", {"layer_id": "lyr", "layer_name": "parcelles",
                                         "ds": "ds_bounds", "session_id": None,
                                         "snapshots": []})
    box.append_events(f"b-{name}-tue", [ob._values(_D["tuesday"], f"u-{name}-tue",
                                                   ob._now())],
                      commit_id=f"c-{name}-tue", commit_state="COMMITTED",
                      commit_meta={"n_events": 1, "detail": ""})
    trace = ob._values(_D["trace"], f"u-{name}-trace", ob._now())
    trace["restored_from_event_id"] = old_id
    box.append_events(f"b-{name}-trace", [trace])
    box.close()
    fx["old_id_in_q"] = old_id
    fx["jm"] = jm
    return fx


def _in_p(p_journal: str, name: str) -> dict:
    from scripts.validation.scenarios import tx_journal_owed_carry_bounds as ob
    res = {"counts": {k: (ob._count(p_journal, m) if os.path.isfile(p_journal) else -1)
                      for k, m in _D.items()}}
    if not os.path.isfile(p_journal):
        return res
    conn = sqlite3.connect(p_journal)
    try:
        row = conn.execute("SELECT restored_from_event_id FROM audit_event WHERE "
                           "event_uid = ?", (f"u-{name}-trace",)).fetchone()
        res["trace_ref"] = row[0] if row else None
        row = conn.execute("SELECT event_id FROM audit_event WHERE event_uid = ?",
                           (f"u-{name}-old",)).fetchone()
        res["old_id_in_p"] = row[0] if row else None
        try:
            row = conn.execute("SELECT state FROM capture_commit WHERE commit_id = ?",
                               (f"c-{name}-tue",)).fetchone()
            res["commit_state"] = row[0] if row else None
        except sqlite3.Error as exc:
            res["commit_state"] = repr(exc)
    finally:
        conn.close()
    return res


def _owed(profile: str) -> list:
    path = os.path.join(profile, "recoverland", "audit", "carries_owed.json")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return []


def _part_d(tmp: str) -> dict:
    fx = _two_sessions(tmp, "d")
    jm = fx.pop("jm")
    res = {"segments_before": _segments(fx["q_journal"]),
           "q_digest_before": _digest(fx["q_journal"]),
           "old_id_in_q": fx["old_id_in_q"]}
    res["planned"] = jm.plan_history_carry(fx["p"], fx["profile"],
                                           being_written=True) is not None
    Path(fx["p"]).write_text("<qgis/>", encoding="utf-8")
    jm.close()                                     # the carry
    rep = jm.last_carry_report
    res["report"] = ({k: rep.as_dict().get(k) for k in (
        "ok", "error", "warnings", "events_inserted", "failure_side", "owed",
        "source_read_only")} if rep else None)
    res["message"] = list(rep.user_message()) if rep else None
    res["owed"] = _owed(fx["profile"])
    res["segments_after"] = _segments(fx["q_journal"])
    res["q_digest_after"] = _digest(fx["q_journal"])
    res["p"] = _in_p(fx["p_journal"], "d")
    return res


def _part_e(tmp: str) -> dict:
    import msvcrt
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.capture_outbox import list_segments, outbox_dir_for

    fx = _two_sessions(tmp, "e")
    jm = fx.pop("jm")
    res = {}
    res["planned"] = jm.plan_history_carry(fx["p"], fx["profile"],
                                           being_written=True) is not None
    Path(fx["p"]).write_text("<qgis/>", encoding="utf-8")
    tuesday_seg = list_segments(outbox_dir_for(fx["q_journal"]))[-1][1]
    size = os.path.getsize(tuesday_seg)
    fd = os.open(tuesday_seg, os.O_RDWR)
    try:
        msvcrt.locking(fd, msvcrt.LK_NBLCK, size)    # the segment cannot be read now
        try:
            with open(tuesday_seg, "rb") as fh:
                fh.read()
            res["segment_unreadable"] = False
        except OSError:
            res["segment_unreadable"] = True
        jm.close()                                 # the carry
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, size)
    finally:
        os.close(fd)
    rep = jm.last_carry_report
    res["report"] = ({k: rep.as_dict().get(k) for k in (
        "ok", "error", "failure_side", "owed", "events_inserted")} if rep else None)
    res["message"] = list(rep.user_message()) if rep else None
    res["owed_after_close"] = _owed(fx["profile"])
    res["p_after_close"] = _in_p(fx["p_journal"], "e")
    # The segment is readable again: P is opened the next day.
    jm = JournalManager()
    try:
        jm.open_for_project(fx["p"], fx["profile"])
    finally:
        jm.close()
    res["owed_after_reopen"] = _owed(fx["profile"])
    res["p_after_reopen"] = _in_p(fx["p_journal"], "e")
    return res


_F = "F_SESSION_EDIT_LEFT_IN_OUTBOX"


def _part_f(tmp: str) -> dict:
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.capture_outbox import CaptureOutbox
    from scripts.validation.scenarios import tx_journal_owed_carry_bounds as ob

    res = {}
    profile = os.path.join(tmp, "profil_f")
    d1 = os.path.join(tmp, "partage_f")
    d2 = os.path.join(tmp, "travail_f")
    for folder in (profile, d1, d2):
        os.makedirs(folder, exist_ok=True)
    q = os.path.join(d1, "Q.qgz")
    p = os.path.join(d2, "P.qgz")
    Path(q).write_text("<qgis/>", encoding="utf-8")
    jm = JournalManager()
    j_q = jm.open_for_project(q, profile)
    # The file takes writes (os.access says so), the journal refuses every
    # new row: what a full disk, or a share read-only by its ACL alone, does.
    conn = jm.get_connection()
    conn.execute("CREATE TRIGGER rl_test_refuse BEFORE INSERT ON audit_event "
                 "BEGIN SELECT RAISE(ABORT, 'journal refuses writes (test)'); END")
    conn.commit()
    box = CaptureOutbox(j_q)
    box.append_events("b-f", [ob._values(_F, "u-f-edit", ob._now())])
    box.close()
    res["planned"] = jm.plan_history_carry(p, profile, being_written=True) is not None
    Path(p).write_text("<qgis/>", encoding="utf-8")
    jm.close()                                     # the carry
    rep = jm.last_carry_report
    res["report"] = ({k: rep.as_dict().get(k) for k in (
        "ok", "error", "events_inserted", "source_read_only",
        "outbox_segments_left", "session_outbox_carried", "owed")} if rep else None)
    res["message"] = list(rep.user_message()) if rep else None
    j_p = os.path.join(d2, ".recoverland", "recoverland_audit.sqlite")
    res["in_p"] = ob._count(j_p, _F) if os.path.isfile(j_p) else -1
    res["segments_left"] = len(_segments(j_q))
    return res


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jro_")
    ctx.data["tmp"] = tmp
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    gpkg = os.path.join(tmp, "data", "parcelles.gpkg")
    _base()._make_gpkg(gpkg)
    ctx.data["gpkg"] = gpkg
    flog(f"tx_journal_save_as_readonly_source setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    d = ctx.data
    for key, fn in (("a", lambda: _part_a(d["tmp"], d["gpkg"])),
                    ("b", lambda: _part_b(d["tmp"])),
                    ("c", lambda: _part_c(d["tmp"])),
                    ("d", lambda: _part_d(d["tmp"])),
                    ("e", lambda: _part_e(d["tmp"])),
                    ("f", lambda: _part_f(d["tmp"]))):
        try:
            d[key] = fn()
        except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
            import traceback
            d[key] = {"crash": f"{type(exc).__name__}: {exc}",
                      "trace": traceback.format_exc()[-1500:]}
    flog(f"tx_journal_save_as_readonly_source run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:7000]}", "INFO")
    _writable_again(d["tmp"])
    shutil.rmtree(d["tmp"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    a = d.get("a") or {}
    out.append((
        "fixture_a_q_archive_en_lecture_seule",
        a.get("write_q") is True and a.get("captured_q") == 1
        and a.get("read_q") is True and a.get("commit_on_ro_q") is True
        and a.get("save_as_p") is True
        and _norm(a.get("journal_on_ro_q")) == _norm(a.get("q_journal")),
        f"Q ecrit={a.get('write_q')} capture={a.get('captured_q')} Q relu="
        f"{a.get('read_q')} journal sur Q archive={a.get('journal_on_ro_q')} "
        f"commit sur Q archive={a.get('commit_on_ro_q')} 'Enregistrer sous' P="
        f"{a.get('save_as_p')} crash={a.get('crash')} {a.get('trace', '')[-400:]}",
    ))
    false_calm = [t for t in (a.get("bar_after_edit") or []) if "sans perte" in t]
    out.append((
        "a2_le_journal_suit_p_sans_message_rassurant_faux",
        _norm(a.get("journal_after_save_as")) == _norm(a.get("expected_p_journal"))
        and not false_calm,
        f"journal apres 'Enregistrer sous'={a.get('journal_after_save_as')} "
        f"(attendu {a.get('expected_p_journal')}) ; messages 'sans perte'="
        f"{false_calm} : l'utilisateur continuerait dans le journal archive en "
        f"lecture seule, ou rien ne s'ecrit, en lisant qu'il n'y a aucune perte",
    ))
    out.append((
        "a2_modification_faite_sur_l_archive_suit_p",
        a.get("ro_edit_in_p_after_save_as") == 1,
        f"modification faite pendant que Q archive etait ouvert, presente dans "
        f"le journal de P juste apres l'enregistrement : "
        f"{a.get('ro_edit_in_p_after_save_as')} (attendu 1) : elle resterait "
        f"bloquee dans l'outbox d'un journal qui ne peut pas la recevoir",
    ))
    counts = a.get("counts_reopened") or {}
    out.append((
        "a3_modification_apres_enregistrement_dans_le_journal_de_p",
        a.get("commit_after") is True and a.get("after_in_p") == 1
        and a.get("reread_p") is True
        and _norm(a.get("reopened_journal")) == _norm(a.get("expected_p_journal"))
        and counts.get(_AFTER) == 1 and counts.get(_ON_RO_Q) == 1,
        f"modification apres 'Enregistrer sous' : commit={a.get('commit_after')} "
        f"dans le journal de P={a.get('after_in_p')} (attendu 1) ; journal "
        f"rouvert={a.get('reopened_journal')} ; lues a la reouverture={counts} "
        f"(attendu 1 chacune) : sinon Restaurer / Rewind de P ne montrent rien "
        f"de ce qui a ete modifie apres l'enregistrement",
    ))
    bpart = d.get("b") or {}
    reps = bpart.get("reports") or {}
    marks = bpart.get("markers") or {}
    expected = {"RO_LEGACY_1": 1, "RO_LEGACY_2": 1, "RO_V7": 1, "V6_A": 1, "V6_B": 1}
    firsts = {k: (v[0] if v else {}) for k, v in reps.items()}
    seconds = {k: (v[1] if len(v) > 1 else {}) for k, v in reps.items()}
    out.append((
        "b_rattachement_depuis_une_source_en_lecture_seule",
        all(firsts.get(k, {}).get("ok") is True for k in ("b1", "b2"))
        and all(marks.get(m) == n for m, n in expected.items()),
        f"premier rattachement : ok={ {k: v.get('ok') for k, v in firsts.items()} } "
        f"erreurs={ {k: v.get('error') for k, v in firsts.items()} } ; lignes "
        f"dans la cible={marks} (attendu une de chaque) crash={bpart.get('crash')} : "
        f"un journal archive refusait tout rattachement, et chaque nouvel essai "
        f"echouait de la meme facon",
    ))
    out.append((
        "b_second_rattachement_n_insere_rien_et_la_source_est_intacte",
        all(seconds.get(k, {}).get("ok") is True
            and seconds.get(k, {}).get("events_inserted") == 0 for k in ("b1", "b2"))
        and bpart.get("digest_before") == bpart.get("digest_after")
        and bool(bpart.get("digest_before")),
        f"second rattachement : inseres="
        f"{ {k: v.get('events_inserted') for k, v in seconds.items()} } (attendu "
        f"0) ; source inchangee={bpart.get('digest_before') == bpart.get('digest_after')} "
        f": une identite tiree au hasard a chaque lecture dupliquerait les lignes "
        f"anciennes, et une source en lecture seule ne doit pas etre ecrite",
    ))
    c = d.get("c") or {}
    rep = c.get("report") or {}
    msg = (c.get("message") or ["", ""])[1] or ""
    out.append((
        "fixture_c_autre_instance_tient_p",
        (c.get("holder_ready") or "").startswith("READY") and c.get("planned") is True,
        f"autre instance={c.get('holder_ready')!r} rattachement planifie="
        f"{c.get('planned')} crash={c.get('crash')} {c.get('trace', '')[-300:]}",
    ))
    out.append((
        "c_pas_de_repli_vers_une_source_qui_refuse_l_ecriture",
        rep.get("fell_back") is not True
        and _norm(c.get("journal_now")) != _norm(c.get("session_journal"))
        and "sans perte" not in msg
        and "reste intact" in msg,
        f"repli={rep.get('fell_back')} journal courant={c.get('journal_now')} "
        f"(session en lecture seule={c.get('session_journal')}) refus du journal "
        f"de P={bool(c.get('refused'))} erreur={c.get('error')} message="
        f"{msg!r} : continuer dans un journal qui refuse toute ecriture en "
        f"disant 'sans perte' perd chaque modification suivante",
    ))
    dpart = d.get("d") or {}
    drep = dpart.get("report") or {}
    dp = dpart.get("p") or {}
    dcounts = dp.get("counts") or {}
    dmsg = (dpart.get("message") or ["", ""])[1] or ""
    out.append((
        "fixture_d_deux_sessions_dans_l_outbox",
        dpart.get("planned") is True and len(dpart.get("segments_before") or {}) == 2
        and drep.get("source_read_only") is True,
        f"rattachement planifie={dpart.get('planned')} segments avant="
        f"{sorted(dpart.get('segments_before') or {})} (attendu lundi + mardi) "
        f"source en lecture seule={drep.get('source_read_only')} crash="
        f"{dpart.get('crash')} {dpart.get('trace', '')[-500:]}",
    ))
    out.append((
        "d_les_lignes_de_cette_session_suivent_p_pas_celles_de_lundi",
        drep.get("ok") is True and dcounts.get("tuesday") == 1
        and dcounts.get("trace") == 1 and dcounts.get("monday") == 0,
        f"rapport={drep} ; journal de P : {dcounts} (attendu mardi=1, trace=1, "
        f"lundi=0) : les modifications faites sur Q archive avant 'Enregistrer "
        f"sous' manquaient a l'historique de P, et le rattachement se disait reussi",
    ))
    out.append((
        "d_trace_et_commit_de_l_outbox_coherents_dans_p",
        dp.get("old_id_in_p") is not None and dp.get("trace_ref") == dp.get("old_id_in_p")
        and dcounts.get("old") == 1 and dp.get("commit_state") == "COMMITTED",
        f"trace de restauration dans P : restored_from_event_id={dp.get('trace_ref')} "
        f"(attendu {dp.get('old_id_in_p')}, l'evenement qu'elle compense tel que "
        f"P le connait ; identifiant dans Q={dpart.get('old_id_in_q')}) ; "
        f"commit de mardi au registre "
        f"de P={dp.get('commit_state')} (attendu COMMITTED) : copiee telle quelle, "
        f"la trace designerait un evenement sans rapport dans P, et un Rewind "
        f"compenserait la mauvaise modification",
    ))
    out.append((
        "d_source_intacte_et_message_exact",
        dpart.get("segments_before") == dpart.get("segments_after")
        and dpart.get("q_digest_before") == dpart.get("q_digest_after")
        and not dpart.get("owed")
        and "0 modification(s)" not in dmsg and "autres sessions" in dmsg,
        f"outbox de Q inchangee={dpart.get('segments_before') == dpart.get('segments_after')} "
        f"journal de Q inchange={dpart.get('q_digest_before') == dpart.get('q_digest_after')} "
        f"obligation restante={dpart.get('owed')} message={dmsg!r} : le message "
        f"doit dire ce qui a suivi P et ce qui reste dans l'outbox de Q (lundi)",
    ))
    epart = d.get("e") or {}
    erep = epart.get("report") or {}
    emsg = epart.get("message") or ["", ""]
    e_close = (epart.get("p_after_close") or {}).get("counts") or {}
    e_reopen = (epart.get("p_after_reopen") or {}).get("counts") or {}
    out.append((
        "fixture_e_segment_illisible",
        epart.get("planned") is True and epart.get("segment_unreadable") is True,
        f"rattachement planifie={epart.get('planned')} segment de mardi illisible="
        f"{epart.get('segment_unreadable')} crash={epart.get('crash')} "
        f"{epart.get('trace', '')[-500:]}",
    ))
    out.append((
        "e_segment_illisible_rattachement_du_et_dit",
        erep.get("ok") is False and erep.get("failure_side") == "source"
        and erep.get("owed") is True and len(epart.get("owed_after_close") or []) == 1
        and emsg[0] == "CRITICAL" and "reste intact" in (emsg[1] or "")
        and e_close.get("tuesday") in (0, -1),
        f"rapport={erep} message={emsg} obligations={len(epart.get('owed_after_close') or [])} "
        f"journal de P apres la fermeture={e_close} : une outbox qu'on ne peut "
        f"pas lire peut contenir les modifications de la session ; conclure "
        f"'reussi' les laissait hors de l'historique de P pour toujours",
    ))
    out.append((
        "e_segment_de_nouveau_lisible_mardi_rattache_a_p",
        e_reopen.get("tuesday") == 1 and e_reopen.get("monday") == 0
        and not epart.get("owed_after_reopen"),
        f"journal de P apres reouverture={e_reopen} (attendu mardi=1, lundi=0) "
        f"obligations restantes={epart.get('owed_after_reopen')} (attendu aucune)",
    ))
    fpart = d.get("f") or {}
    frep = fpart.get("report") or {}
    fmsg = (fpart.get("message") or ["", ""])[1] or ""
    out.append((
        "f_journal_qui_refuse_le_rejeu_la_session_suit_quand_meme_p",
        fpart.get("planned") is True and frep.get("ok") is True
        and frep.get("source_read_only") is False and fpart.get("in_p") == 1
        and fpart.get("segments_left") == 1 and "rattachees au projet" in fmsg,
        f"rapport={frep} ; modification de la session dans le journal de P="
        f"{fpart.get('in_p')} (attendu 1) ; segments restes dans l'outbox de Q="
        f"{fpart.get('segments_left')} ; message={fmsg!r} crash={fpart.get('crash')} "
        f"{fpart.get('trace', '')[-400:]} : un journal inscriptible en apparence "
        f"qui refuse l'ecriture (disque plein, partage en lecture seule par ses "
        f"droits) gardait la modification dans son outbox et P ne l'avait jamais",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_save_as_readonly_source.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
