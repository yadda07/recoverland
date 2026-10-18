"""tx_journal_lock_foreign_host - RecoverLand validation runtime.

Invariant: I-JRN (un seul ecrivain par journal ; le plugin ne corrompt
jamais son propre stockage par concurrence).

Le mecanisme
------------
`JournalManager._acquire_writer_lock` ecrit `pid|hote|horodatage` dans
`<journal>.rlwriter`. Avant correctif il ne refusait l'ouverture que si le
verrou venait du MEME hote avec un PID vivant ; un verrou pose par un AUTRE
poste etait declare "perime" et ecrase, quel que soit son age (l'horodatage
etait lu puis ignore). Rien ne rafraichissait ce verrou pendant la session,
et la sequence "lire puis ecrire" n'etait pas atomique (ni O_EXCL, ni
verrou systeme).

Or un journal vit a cote de son projet, et un projet sur un partage reseau
est un emplacement normal. Scenario d'echec concret : deux techniciens
ouvrent `\\\\srv\\sig\\commune\\projet.qgz`. Le poste B ecrase le verrou du
poste A ; les deux threads ecrivains ajoutent au meme journal ; les
event_id s'entrelacent, un REWIND de A compense des evenements de B, et le
fichier finit "malformed" au prochain demarrage.

Sous-mecanismes sur le meme hote : un PID recycle par n'importe quel
processus (apres un plantage de QGIS) faisait refuser le journal pour
toujours ; a l'inverse deux QGIS lances ensemble passaient tous deux le
test "pas de verrou" avant d'ecrire le leur.

Ce que le scenario execute reellement (vrai code produit, dossiers
temporaires, vrais processus tiers) :

  F1 verrou d'un autre poste, battement de coeur recent  -> refus nomme
  F2 verrou d'un autre poste, battement vieux de 24 h     -> recupere, trace
  F3 le verrou tenu est rafraichi pendant la session (battement de coeur)
  F4 un verrou repris par un autre poste pendant la session est DETECTE
  F5 meme hote, PID vivant mais recycle par PING.EXE, battement perime -> recupere
  F6 meme hote, PID vivant d'un python (image QGIS possible), battement perime
     -> refus (on ne prend pas le risque d'ecrire a deux)
  F7 une VRAIE seconde instance (processus fils, code produit) tient le
     journal ; le contenu du fichier est falsifie en "autre poste, perime" :
     l'ouverture doit quand meme etre refusee (verrou systeme)
  F8 la seconde instance se ferme : le journal s'ouvre

Verdict attendu avant correctif : FAIL (F1, F3, F4, F5, F7).
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess  # nosec B404: processus locaux inertes lances par le test
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_lock_foreign_host"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FOREIGN_HOST = "RL-POSTE-DISTANT"
_DAY = 86400

_CHILD_CODE = r'''
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


def _project(base: str, name: str) -> tuple:
    from recoverland.core.journal_manager import _resolve_journal_path

    pdir = os.path.join(base, name)
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    journal = _resolve_journal_path(project, base, "")
    os.makedirs(os.path.dirname(journal), exist_ok=True)
    return project, journal


def _write_lock(journal: str, pid: int, host: str, ts: float) -> None:
    """A lock record as its holder left it at *ts*.

    The file date is set to *ts* as well: a lock not refreshed for a day
    has not been WRITTEN for a day either, and the file server's date is
    what a heartbeat check can trust when two machines' clocks disagree.
    """
    path = journal + ".rlwriter"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"{pid}|{host}|{int(ts)}\n")
    try:
        os.utime(path, (ts, ts))
    except OSError:
        pass


def _read_lock(journal: str) -> str:
    try:
        return Path(journal + ".rlwriter").read_text(encoding="utf-8").strip()
    except OSError as exc:
        return f"<{type(exc).__name__}>"


def _try_open(project: str, profile: str, keep: bool = False):
    from recoverland.core.journal_manager import JournalManager, JournalLockError

    jm = JournalManager()
    res = {"opened": False, "refused": "", "error": ""}
    try:
        jm.open_for_project(project, profile)
        res["opened"] = jm.is_open
    except JournalLockError as exc:
        res["refused"] = str(exc)
    except Exception as exc:  # noqa: BLE001
        res["error"] = f"{type(exc).__name__}: {exc}"
    if keep:
        return res, jm
    jm.close()
    return res, None


def _wait_until(pred, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    return pred()


def _stop(proc) -> None:
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except Exception:  # noqa: BLE001
        try:
            proc.kill()
            proc.wait(timeout=10)
        except Exception:  # noqa: BLE001
            pass
    # Les tubes du fils (stdin/stdout de la seconde instance) ne se ferment
    # pas avec lui : sans close(), ils restaient ouverts jusqu'a la collecte
    # suivante (le cadre qui tient `proc` est retenu par un cycle), pendant
    # un autre scenario.
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None:
            try:
                stream.close()
            except Exception:  # noqa: BLE001 - tube deja rompu par le fils
                pass


def _case_foreign(base: str, name: str, age: float) -> dict:
    project, journal = _project(base, name)
    _write_lock(journal, 4242, _FOREIGN_HOST, time.time() - age)
    res, _ = _try_open(project, base)
    res["journal_created"] = os.path.isfile(journal)
    res["lock_after"] = _read_lock(journal)
    return res


def _case_heartbeat(base: str) -> dict:
    import recoverland.core.journal_manager as jm_mod

    project, journal = _project(base, "heartbeat")
    saved = getattr(jm_mod, "_LOCK_HEARTBEAT_SEC", None)
    jm_mod._LOCK_HEARTBEAT_SEC = 0.2
    res, jm = _try_open(project, base, keep=True)
    try:
        me = f"{os.getpid()}|{socket.gethostname()}|"
        old_ts = int(time.time() - _DAY)
        _write_lock(journal, os.getpid(), socket.gethostname(), old_ts)

        def _refreshed():
            content = _read_lock(journal)
            if not content.startswith(me):
                return False
            try:
                return int(content.split("|")[2]) > old_ts + 3600
            except (IndexError, ValueError):
                return False

        res["refreshed"] = _wait_until(_refreshed, 4.0)
        res["lock_now"] = _read_lock(journal)
    finally:
        if jm is not None:
            jm.close()
        if saved is None:
            try:
                delattr(jm_mod, "_LOCK_HEARTBEAT_SEC")
            except AttributeError:
                pass
        else:
            jm_mod._LOCK_HEARTBEAT_SEC = saved
    return res


def _case_lock_lost(base: str) -> dict:
    import recoverland.core.journal_manager as jm_mod

    project, journal = _project(base, "lock_lost")
    saved = getattr(jm_mod, "_LOCK_HEARTBEAT_SEC", None)
    jm_mod._LOCK_HEARTBEAT_SEC = 0.2
    res, jm = _try_open(project, base, keep=True)
    try:
        _write_lock(journal, 4343, _FOREIGN_HOST, time.time())
        res["lost_seen"] = _wait_until(
            lambda: bool(getattr(jm, "is_lock_lost", False)), 4.0)
    finally:
        if jm is not None:
            jm.close()
        res["foreign_lock_kept_after_close"] = _read_lock(journal).startswith(
            f"4343|{_FOREIGN_HOST}|")
        if saved is None:
            try:
                delattr(jm_mod, "_LOCK_HEARTBEAT_SEC")
            except AttributeError:
                pass
        else:
            jm_mod._LOCK_HEARTBEAT_SEC = saved
    return res


def _case_same_host(base: str, name: str, argv: list) -> dict:
    project, journal = _project(base, name)
    proc = subprocess.Popen(  # nosec B603: arguments fixes, processus local
        argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL)
    try:
        time.sleep(0.3)
        _write_lock(journal, proc.pid, socket.gethostname(), time.time() - _DAY)
        res, _ = _try_open(project, base)
        res["pid"] = proc.pid
        res["alive_during_open"] = proc.poll() is None
    finally:
        _stop(proc)
    return res


def _case_second_instance(base: str) -> dict:
    project, journal = _project(base, "second_instance")
    package = _PLUGIN_ROOT.name
    child_py = os.path.join(base, "second_instance_child.py")
    Path(child_py).write_text(_CHILD_CODE.format(
        parent=str(_PLUGIN_ROOT.parent), package=package,
        project=project, profile=base), encoding="utf-8")
    proc = subprocess.Popen(  # nosec B603: sys.executable, script du test
        [sys.executable, child_py], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    res = {"child_ready": "", "child_closed": ""}
    try:
        res["child_ready"] = (proc.stdout.readline() or "").strip()
        if not res["child_ready"].startswith("READY"):
            return res
        # The content says "another host, dead for a day": only a lock the
        # operating system enforces can still say that the journal is held.
        _write_lock(journal, 4444, _FOREIGN_HOST, time.time() - _DAY)
        held, _ = _try_open(project, base)
        res["while_held"] = held
        proc.stdin.write("\n")
        proc.stdin.flush()
        res["child_closed"] = (proc.stdout.readline() or "").strip()
        proc.wait(timeout=30)
        after, _ = _try_open(project, base)
        res["after_release"] = after
    finally:
        _stop(proc)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    base = tempfile.mkdtemp(prefix="rl_tx_jlf_")
    ctx.data["base"] = base
    flog(f"tx_journal_lock_foreign_host setup: trace_id={ctx.trace_id} "
         f"base={base} host={socket.gethostname()}", "INFO")


def run(ctx):
    import json
    from recoverland.core.logger import flog

    base = ctx.data["base"]
    d = ctx.data
    d["f1"] = _case_foreign(base, "f1_foreign_fresh", 5)
    d["f2"] = _case_foreign(base, "f2_foreign_expired", _DAY)
    d["f3"] = _case_heartbeat(base)
    d["f4"] = _case_lock_lost(base)
    d["f5"] = _case_same_host(
        base, "f5_recycled", ["ping", "-n", "60", "127.0.0.1"])
    d["f6"] = _case_same_host(
        base, "f6_live_python",
        [sys.executable, "-c", "import time; time.sleep(60)"])
    d["f7"] = _case_second_instance(base)
    flog(f"tx_journal_lock_foreign_host run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'base'}, default=str)[:4000]}",
         "INFO")
    shutil.rmtree(base, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    f1 = d.get("f1") or {}
    out.append((
        "f1_verrou_autre_poste_recent_refuse",
        bool(f1.get("refused")) and not f1.get("opened"),
        f"verrou de {_FOREIGN_HOST} rafraichi il y a 5 s : refus="
        f"{f1.get('refused')!r} ouvert={f1.get('opened')} erreur="
        f"{f1.get('error')!r}. Deux postes qui ecrivent le meme journal sur "
        f"un partage le corrompent.",
    ))
    out.append((
        "f1_refus_nomme_le_poste",
        _FOREIGN_HOST in (f1.get("refused") or ""),
        f"message de refus={f1.get('refused')!r} : l'utilisateur doit savoir "
        f"QUEL poste tient le journal pour aller le fermer",
    ))
    out.append((
        "f1_refus_sans_effet_de_bord",
        f1.get("journal_created") is False
        and (f1.get("lock_after") or "").startswith(f"4242|{_FOREIGN_HOST}|"),
        f"journal cree={f1.get('journal_created')} verrou apres="
        f"{f1.get('lock_after')!r} : un refus ne cree rien et ne touche pas "
        f"au verrou de l'autre poste",
    ))
    f2 = d.get("f2") or {}
    out.append((
        "f2_verrou_autre_poste_perime_recupere",
        f2.get("opened") is True and not f2.get("refused"),
        f"verrou de {_FOREIGN_HOST} sans battement depuis 24 h : ouvert="
        f"{f2.get('opened')} refus={f2.get('refused')!r} : un poste qui a "
        f"plante ne doit pas condamner le journal",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"JournalManager: stale writer lock reclaimed .*host={_FOREIGN_HOST}.*"
        rf"heartbeat_age_s=",
        name="f2_recuperation_tracee_avec_age",
        min_count=1,
    ))
    f3 = d.get("f3") or {}
    out.append((
        "f3_verrou_rafraichi_pendant_la_session",
        f3.get("opened") is True and f3.get("refreshed") is True,
        f"verrou apres un battement de coeur={f3.get('lock_now')!r} "
        f"rafraichi={f3.get('refreshed')} : sans battement, un QGIS ouvert "
        f"depuis plus longtemps que la duree de vie du verrou se le fait "
        f"prendre par un autre poste",
    ))
    f4 = d.get("f4") or {}
    out.append((
        "f4_verrou_repris_detecte",
        f4.get("opened") is True and f4.get("lost_seen") is True,
        f"verrou repris par {_FOREIGN_HOST} pendant la session : detecte="
        f"{f4.get('lost_seen')} : une instance qui a perdu son verrou et "
        f"continue d'ecrire est exactement la corruption a eviter",
    ))
    out.append((
        "f4_verrou_d_autrui_non_supprime",
        f4.get("foreign_lock_kept_after_close") is True,
        f"verrou de l'autre poste encore present apres close()="
        f"{f4.get('foreign_lock_kept_after_close')} : fermer ne doit pas "
        f"deposseder l'instance qui tient le journal",
    ))
    out.append(assert_log_contains(
        ctx.records,
        r"JournalManager: writer lock .* taken over",
        name="f4_perte_de_verrou_tracee",
        min_count=1,
    ))
    f5 = d.get("f5") or {}
    out.append((
        "f5_pid_recycle_par_un_autre_programme_recupere",
        f5.get("alive_during_open") is True and f5.get("opened") is True,
        f"PID {f5.get('pid')} vivant mais tenu par PING.EXE, battement de "
        f"24 h : ouvert={f5.get('opened')} refus={f5.get('refused')!r}. Apres "
        f"un plantage, un PID recycle par n'importe quel programme coupait la "
        f"capture de ce projet sans fin.",
    ))
    f6 = d.get("f6") or {}
    out.append((
        "f6_pid_python_vivant_refuse_par_prudence",
        f6.get("alive_during_open") is True and bool(f6.get("refused"))
        and not f6.get("opened"),
        f"PID {f6.get('pid')} vivant, image python (QGIS possible), battement "
        f"perime : refus={f6.get('refused')!r} ouvert={f6.get('opened')} : "
        f"dans le doute on refuse, on n'ecrit pas a deux",
    ))
    f7 = d.get("f7") or {}
    held = f7.get("while_held") or {}
    after = f7.get("after_release") or {}
    out.append((
        "f7_fixture_seconde_instance",
        (f7.get("child_ready") or "").startswith("READY"),
        f"seconde instance (processus fils) : {f7.get('child_ready')!r}",
    ))
    out.append((
        "f7_seconde_instance_vivante_refuse_malgre_contenu_falsifie",
        bool(held.get("refused")) and not held.get("opened"),
        f"une autre instance tient le journal, contenu du fichier falsifie en "
        f"'autre poste, perime' : refus={held.get('refused')!r} ouvert="
        f"{held.get('opened')}. Sans verrou systeme, deux QGIS lances en meme "
        f"temps passent tous deux le test et ecrivent a deux.",
    ))
    out.append((
        "f8_journal_libre_apres_fermeture",
        f7.get("child_closed") == "CLOSED" and after.get("opened") is True,
        f"apres fermeture de la seconde instance ({f7.get('child_closed')!r}) "
        f": ouvert={after.get('opened')} refus={after.get('refused')!r}",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_lock_foreign_host.*trace_id={ctx.trace_id}",
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
