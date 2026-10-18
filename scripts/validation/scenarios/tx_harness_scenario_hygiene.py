"""tx_harness_scenario_hygiene - RecoverLand validation runtime.

Invariant: I-SUITE-ISOLATION (un scenario ferme ce qu'il ouvre : a sa
fin, aucune connexion SQLite ni aucun fichier qu'il a ouverts ne reste
ouvert en attendant le ramasse-miettes).

Le mecanisme
------------
Sous Python 3.12, une `sqlite3.Connection` se reference elle-meme par son
cache de requetes. Une connexion que personne ne ferme n'est donc PAS
liberee quand sa derniere reference disparait : elle reste ouverte
jusqu'au prochain passage complet du ramasse-miettes, c'est-a-dire
pendant un scenario ulterieur, dans le processus QGIS partage. Un
fichier tenu par un objet pris dans un cycle (les tubes d'un Popen dont
le cadre d'appel est retenu par la trace d'une exception gardee) fait
de meme. Pour un journal sur disque, c'est un fichier verrouille sous
Windows et une connexion qui voit un etat perime ; pour le lecteur de la
suite, ce sont des lignes LEAK qui noient celles qui comptent.

Le lanceur les signale (non bloquant) : seize scenarios de la suite
complete en produisaient, quinze pour une base `:memory:` rangee dans
`ctx.data["conn"]` sans teardown(ctx), un (tx_journal_lock_foreign_host)
pour les tubes stdin/stdout jamais fermes du processus fils qui joue la
seconde instance.

Ce que le scenario verifie : il lance le vrai `headless_run.py` de CETTE
copie du plugin, dans un processus enfant et un profil QGIS temporaire,
sur ces seize scenarios, et exige pour chacun qu'il ait tourne et
qu'aucune ligne LEAK ne porte son nom. Il ne juge pas leurs assertions
(c'est leur affaire) : seulement ce qu'ils laissent ouvert.

La mesure du lanceur depend du moment ou Python collecte : un cycle
jeune peut etre libere en silence avant elle (c'est ce qui rend la ligne
LEAK de tx_journal_lock_foreign_host intermittente). Pour ce cas-la, le
scenario rejoue aussi, ramasse-miettes coupe, le cas "seconde instance"
de ce scenario et verifie que les tubes du processus fils sont fermes a
son retour, sans attendre une collecte.
"""
from __future__ import annotations

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_harness_scenario_hygiene"
INVARIANT = "I-SUITE-ISOLATION"

_VALIDATION_DIR = Path(__file__).resolve().parents[1]
_PLUGIN_ROOT = _VALIDATION_DIR.parents[1]
_HEADLESS = _VALIDATION_DIR / "headless_run.py"

# Sous le delai par defaut d'un scenario du lanceur (300 s).
_LIMIT_S = 240.0

_TARGETS = (
    "i8_rewind_idempotence",
    "i9_cutoff_inclusivity",
    "p17_dedup_fid_collision",
    "rvw_cutoff_clock",
    "rw_dedup_post_trace_edit",
    "rw_multi_hop_navigation",
    "rw_partial_rewind_resume",
    "rw_rewind_active_trace_guard",
    "rw_rewind_fetch_limit",
    "rw_state_at_t_engine_cases",
    "rw_truncated_window_refused",
    "tx_journal_lock_foreign_host",
    "tx_rewind_broken_trace_memory",
    "tx_rewind_clock_ties",
    "tx_rewind_cutoff_edges",
    "tx_rewind_mass_and_multilayer",
)


def _python() -> str:
    exe = Path(sys.executable)
    if not exe.stem.lower().startswith("qgis"):
        return str(exe)
    for name in ("python3.exe", "python.exe", "python3", "python"):
        if exe.with_name(name).is_file():
            return str(exe.with_name(name))
    return str(exe)


def _kill_tree(proc) -> None:
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=30, check=False)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:  # noqa: BLE001
        pass
    try:
        proc.kill()
        proc.wait(timeout=30)
    except Exception:  # noqa: BLE001
        pass


def _second_instance_pipes_left() -> dict:
    """Tubes encore ouverts au retour du cas "seconde instance".

    Le cas laisse un cycle de references (la JournalLockError que garde
    le JournalManager du produit tient sa trace, donc les cadres de
    l'appelant) : seul un close() explicite libere les tubes du fils avant
    la collecte suivante. Le ramasse-miettes est coupe pendant le cas :
    la reponse ne depend pas du moment ou il aurait tourne.
    """
    import gc
    import importlib.util
    import io

    path = _VALIDATION_DIR / "scenarios" / "tx_journal_lock_foreign_host.py"
    spec = importlib.util.spec_from_file_location("zz_hygiene_jlf", str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    base = tempfile.mkdtemp(prefix="rl_hyg_jlf_")
    was_enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        before = {id(o) for o in gc.get_objects()
                  if type(o) is io.FileIO and not o.closed}
        res = mod._case_second_instance(base)
        # Les tubes d'un processus fils sont des fichiers nommes par leur
        # descripteur.
        left = [o.name for o in gc.get_objects()
                if type(o) is io.FileIO and not o.closed
                and id(o) not in before and isinstance(o.name, int)]
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()
        shutil.rmtree(base, ignore_errors=True)
    return {"child_ready": res.get("child_ready"),
            "child_closed": res.get("child_closed"), "left": left}


def setup(ctx):
    work = Path(tempfile.mkdtemp(prefix="rl_hygiene_"))
    (work / "cfg").mkdir()
    ctx.data["work"] = str(work)


def run(ctx):
    ctx.data["jlf"] = _second_instance_pipes_left()
    work = Path(ctx.data["work"])
    out_path = work / "hygiene.out.txt"
    env = os.environ.copy()
    env.pop("RECOVERLAND_SCENARIO_TIMEOUT", None)
    env["QGIS_CUSTOM_CONFIG_PATH"] = str(work / "cfg")
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    kw = {}
    if os.name != "nt":
        kw["start_new_session"] = True
    t0 = time.monotonic()
    killed = False
    try:
        with open(out_path, "wb") as fh:
            proc = subprocess.Popen(
                [_python(), str(_HEADLESS)] + list(_TARGETS),
                stdout=fh, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                env=env, cwd=str(_PLUGIN_ROOT), **kw)
            try:
                proc.wait(timeout=_LIMIT_S)
            except subprocess.TimeoutExpired:
                killed = True
                _kill_tree(proc)
        ctx.data["out"] = out_path.read_text(encoding="utf-8", errors="replace")
        ctx.data["code"] = None if killed else proc.returncode
        ctx.data["killed"] = killed
        ctx.data["elapsed"] = round(time.monotonic() - t0, 1)
        print(f"[{SCENARIO_ID}] code={ctx.data['code']} killed={killed} "
              f"elapsed={ctx.data['elapsed']} s")
    finally:
        shutil.rmtree(ctx.data["work"], ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    text = d.get("out", "")
    ran = dict(re.findall(r"^\[headless\] \[\d+/\d+\] (\S+) -> (\w+)", text,
                          re.M))
    leaks = dict(re.findall(r"^\[headless\] LEAK (\S+): (.*)$", text, re.M))
    out = [(
        "nested_run_completed",
        d.get("killed") is False and sorted(ran) == sorted(_TARGETS),
        f"tue={d.get('killed')} apres {d.get('elapsed')} s ; scenarios "
        f"passes par le lanceur={len(ran)}/{len(_TARGETS)} (manquants="
        f"{sorted(set(_TARGETS) - set(ran))}) : sans leur execution, rien "
        f"n'est prouve sur ce qu'ils laissent ouvert",
    )]
    for name in _TARGETS:
        leak = leaks.get(name)
        out.append((
            f"closes_what_it_opens_{name}",
            name in ran and leak is None,
            f"issue={ran.get(name)} ; LEAK={leak!r} : ce qu'un scenario laisse "
            f"ouvert reste ouvert (journal verrouille sous Windows) pendant "
            f"les scenarios suivants du meme QGIS, jusqu'au prochain passage "
            f"du ramasse-miettes",
        ))
    jlf = d.get("jlf") or {}
    out.append((
        "second_instance_case_closes_the_child_pipes_without_gc",
        (jlf.get("child_ready") or "").startswith("READY")
        and jlf.get("child_closed") == "CLOSED" and jlf.get("left") == [],
        f"tx_journal_lock_foreign_host, cas seconde instance : fils="
        f"{jlf.get('child_ready')!r}/{jlf.get('child_closed')!r}, tubes "
        f"encore ouverts a son retour (ramasse-miettes coupe)="
        f"{jlf.get('left')} : stdin/stdout du fils, jamais fermes, restaient "
        f"ouverts jusqu'a la collecte suivante, pendant un autre scenario",
    ))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
