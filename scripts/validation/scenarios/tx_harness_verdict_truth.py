# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_harness_verdict_truth - RecoverLand validation runtime.

Invariant: I-SUITE-EXIT-TRUTH (un verdict n'est imprime qu'une fois
l'issue du processus qui l'a produit connue ; le lanceur ne se fige
jamais apres la fin de son enfant ; ses messages disent la vraie cause ;
une option absurde est refusee avant de lancer quoi que ce soit).

Le mecanisme
------------
Le tour precedent a mis les scenarios dans un processus enfant surveille
(`suite_isolation.supervise`) : le parent ne croit le verdict de l'enfant
que si l'enfant sort ensuite avec le code que ce verdict annonce. Quatre
defauts restaient, chacun vu par qui lit la sortie d'une suite :

a) `supervise` attendait la fin du tube de sortie de l'enfant sans
   limite. Un processus lance par l'enfant (aide d'un scenario, processus
   fils du produit, `close_fds=False`, `os.system`, QProcess) herite de
   ce tube et le garde ouvert bien apres la sortie de l'enfant : le
   lanceur restait fige jusqu'a la fin de ce petit-fils (fermer le tube
   attend le verrou que tient le fil de lecture), donc la CI jusqu'a sa
   propre limite, sans verdict.
b) En mode par defaut, l'enfant imprimait "ALL GREEN" (ci_run :
   "CI VALIDATION PASSED") AVANT de savoir comment il sortirait. Un
   plantage pendant la fermeture de Python (destructeur natif, atexit)
   donnait ensuite "SUITE PROCESS ERROR" (ci_run : "ABORTED") : la sortie
   dit vert puis rouge, et le lecteur qui s'arrete au vert publie un
   plantage de fermeture (QGIS qui plante en quittant chez
   l'utilisateur).
c) La ligne LEAK d'une connexion SQLite fermee par le seul ramasse-
   miettes disait "a reference cycle kept it open". C'est faux dans le
   cas courant : sous Python 3.12 une sqlite3.Connection se reference
   elle-meme par son cache de requetes, la vraie cause est un close()
   manquant, et le message envoyait chercher un cycle qui n'existe pas.
d) `--isolate-grace 0` (ou negatif) tuait chaque enfant des le premier
   tour de surveillance et annoncait un blocage ; `--timeout -1` etait
   ramene a 0, c'est-a-dire SANS limite : une faute de frappe retirait
   en silence la protection contre les scenarios figes.

Ce que le scenario verifie, en lancant le vrai `headless_run.py` et le
vrai `ci_run.py` de CETTE copie du plugin (et `supervise` de ce
`suite_isolation`) dans des processus enfants, sur des fixtures ecrites
dans un dossier temporaire :

    V1  defaut : plantage dans la fermeture de Python apres le rapport
        -> aucune ligne "ALL GREEN", "SUITE PROCESS ERROR" nomme, exit 1
    V2  ci_run : meme plantage -> ni "CI VALIDATION PASSED" ni autre
        verdict vert, "ABORTED", exit 1 ; une liste verte normale dit
        encore "CI VALIDATION PASSED" (une fois, en dernier), exit 0
    V3  un petit-fils tient le tube de sortie de l'enfant -> supervise
        rend la main en temps borne, le dit, et le processus qui
        l'appelle se termine proprement (code 0)
    V4  connexion non fermee -> ligne LEAK "not closed (released only by
        the garbage collector)", sans "reference cycle" ; la suite reste
        verte et dit "ALL GREEN" une fois, en derniere ligne
    V5  --isolate-grace 0 / -5, --timeout -1 / nan, --leak-grace -1,
        ci_run --timeout -1 -> exit 2, option nommee, aucun scenario lance ;
        RECOVERLAND_SCENARIO_TIMEOUT=-5 -> ignoree et dite, la limite par
        defaut reste en place (elle etait ramenee a 0 : aucune limite)
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

SCENARIO_ID = "tx_harness_verdict_truth"
INVARIANT = "I-SUITE-EXIT-TRUTH"

_VALIDATION_DIR = Path(__file__).resolve().parents[1]
_PLUGIN_ROOT = _VALIDATION_DIR.parents[1]
_HEADLESS = _VALIDATION_DIR / "headless_run.py"
_CI_RUN = _VALIDATION_DIR / "ci_run.py"
_REPORTS = _VALIDATION_DIR / "reports"
_FX = "zz_vt_"  # court : le chemin du rapport reste sous 260 caracteres

# Budget total, sous le delai par defaut d'un scenario du lanceur (300 s).
_TOTAL_BUDGET_S = 240.0
_RUN_S = 45.0
_OPT_S = 30.0
# Le petit-fils garde le tube bien plus longtemps que la limite de V3 :
# sans borne, supervise ne rend la main qu'a sa mort.
_PIPE_HOLD_S = 150
_PIPE_LIMIT_S = 60.0
_PIPE_RETURN_MAX_S = 35.0


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

_TEMPLATE = '''
SCENARIO_ID = "{sid}"
INVARIANT = "fixture"


def setup(ctx):
    return


def run(ctx):
{body}


def assertions(ctx):
    return [("fixture_verdict", True, "fixture {sid}")]
'''

_PASS = "    return"
_FINAL_SEGV = ("    import atexit, faulthandler\n"
               "    # lecture a l'adresse NULL pendant la fermeture de Python\n"
               "    atexit.register(faulthandler._read_null)")
# Une connexion jamais fermee, comme `ctx.data["conn"]` d'un scenario sans
# teardown : a la sortie de run(), plus rien ne la reference que son propre
# cache de requetes. La collecte la vieillit : seul un passage complet du
# ramasse-miettes la liberera.
_UNCLOSED = ("    import gc, os, sqlite3\n"
             "    conn = sqlite3.connect(os.path.join({base!r}, "
             "\"zz_vt_unclosed.sqlite\"))\n"
             "    conn.execute(\"select 1\").fetchall()\n"
             "    gc.collect()")

_CI_PASS = "def run():\n    return {'verdict': 'PASS'}\n"
_CI_PASS_THEN_SEGV = ("def run():\n"
                      "    import atexit, faulthandler\n"
                      "    atexit.register(faulthandler._read_null)\n"
                      "    return {'verdict': 'PASS'}\n")
_CI_NAMES = ("compat_cross_version", "rv_snapshot_asof", "rv_snapshot_volume")

_PIPE_CHILD = r'''
import json, subprocess, sys, time
journal = open(sys.argv[1], "a", encoding="utf-8")


def ev(**f):
    f["t"] = time.time()
    journal.write(json.dumps(f) + "\n")
    journal.flush()


ev(ev="phase", phase="report")
# Un petit-fils qui herite de tout ce qui est heritable, dont le tube de
# sortie de ce processus (close_fds=False, comme os.system ou QProcess).
held = subprocess.Popen([sys.executable, "-c",
                         "import time; time.sleep({hold})"], close_fds=False)
with open({pidfile!r}, "w") as fh:
    fh.write(str(held.pid))
print("pipe-child: leaving; grandchild", held.pid, "keeps the pipe",
      flush=True)
ev(ev="done", code=0, blockers=[])
sys.exit(0)
'''

_PIPE_DRIVER = r'''
import os, sys, time
sys.path.insert(0, {validation!r})
import suite_isolation as iso

journal = {journal!r}
t0 = time.monotonic()


def _deadline(state, t_spawn):
    return iso.phase_deadline(state, t_spawn, 300, 40, 120)


code, killed, _elapsed, state = iso.supervise(
    [sys.executable, "-u", {child!r}, journal], journal, _deadline)
print("SUPERVISE_RETURNED after=%.1f code=%s killed=%s done=%s"
      % (time.monotonic() - t0, code, killed, state.done is not None),
      flush=True)
'''


def _write_fixture(fx_dir: Path, sid: str, body: str) -> Path:
    path = fx_dir / f"{sid}.py"
    path.write_text(_TEMPLATE.format(sid=sid, body=body), encoding="utf-8")
    return path


def _write_ci_shadow(root: Path, bodies: dict) -> Path:
    """Paquet `recoverland` fantome : la liste CI par defaut l'importe."""
    pkg = root / "recoverland" / "scripts" / "validation" / "scenarios"
    pkg.mkdir(parents=True)
    for level in (root / "recoverland", root / "recoverland" / "scripts",
                  pkg.parent, pkg):
        (level / "__init__.py").write_text("", encoding="utf-8")
    for name, src in bodies.items():
        (pkg / f"{name}.py").write_text(src, encoding="utf-8")
    return root


def _write_fixtures(work: Path) -> dict:
    fx_dir = work / "fixtures"
    fx_dir.mkdir()
    area = work / "area"
    area.mkdir()
    fx = {
        "a_pass": _write_fixture(fx_dir, _FX + "a_pass", _PASS),
        "e_py_segv": _write_fixture(fx_dir, _FX + "e_py_segv", _FINAL_SEGV),
        "u_unclosed": _write_fixture(fx_dir, _FX + "u_unclosed",
                                     _UNCLOSED.format(base=str(area))),
        "ci_segv": _write_ci_shadow(work / "ci_segv", {
            _CI_NAMES[0]: _CI_PASS, _CI_NAMES[1]: _CI_PASS_THEN_SEGV,
            _CI_NAMES[2]: _CI_PASS}),
        "ci_green": _write_ci_shadow(work / "ci_green", {
            name: _CI_PASS for name in _CI_NAMES}),
    }
    pipe = work / "pipe"
    pipe.mkdir()
    child = pipe / "pipe_child.py"
    pidfile = pipe / "grandchild.pid"
    child.write_text(_PIPE_CHILD.replace("{hold}", str(_PIPE_HOLD_S))
                     .replace("{pidfile!r}", repr(str(pidfile))),
                     encoding="utf-8")
    driver = pipe / "pipe_driver.py"
    driver.write_text(_PIPE_DRIVER.format(
        validation=str(_VALIDATION_DIR), journal=str(pipe / "child.jsonl"),
        child=str(child)), encoding="utf-8")
    fx["pipe_driver"] = driver
    fx["pipe_pidfile"] = pidfile
    return fx


# --------------------------------------------------------------------- #
# Lancement d'un enfant, borne par CE scenario
# --------------------------------------------------------------------- #

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


def _launch(ctx, tag: str, script: Path, args, limit_s: float,
            env_extra=None) -> dict:
    """Lance `script args` et rend {code, killed, elapsed, out}."""
    work = Path(ctx.data["work"])
    out_path = work / f"{tag}.out.txt"
    limit_s = max(5.0, min(limit_s, ctx.data["deadline"] - time.monotonic()))
    env = os.environ.copy()
    env.pop("RECOVERLAND_SCENARIO_TIMEOUT", None)
    env["QGIS_CUSTOM_CONFIG_PATH"] = str(work / "cfg")
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    for key, value in (env_extra or {}).items():
        if key == "PYTHONPATH" and env.get(key):
            # Prefixer, jamais remplacer : le .bat de QGIS y met qgis.core.
            value = os.pathsep.join([value, env[key]])
        env[key] = value
    kw = {}
    if os.name != "nt":
        kw["start_new_session"] = True
    t0 = time.monotonic()
    killed = False
    with open(out_path, "wb") as fh:
        proc = subprocess.Popen([_python(), str(script)] + [str(a) for a in args],
                                stdout=fh, stderr=subprocess.STDOUT,
                                stdin=subprocess.DEVNULL, env=env,
                                cwd=str(_PLUGIN_ROOT), **kw)
        try:
            proc.wait(timeout=limit_s)
        except subprocess.TimeoutExpired:
            killed = True
            _kill_tree(proc)
    elapsed = time.monotonic() - t0
    out = out_path.read_text(encoding="utf-8", errors="replace")
    res = {"code": None if killed else proc.returncode, "killed": killed,
           "elapsed": round(elapsed, 1), "limit": round(limit_s, 1), "out": out}
    print(f"[{SCENARIO_ID}] {tag}: code={res['code']} killed={killed} "
          f"elapsed={res['elapsed']} s (limit {res['limit']} s)")
    ctx.data[tag] = res
    return res


def _kill_grandchild(pidfile: Path) -> None:
    """Le petit-fils de V3 est orphelin : personne d'autre ne l'arretera."""
    try:
        pid = int(Path(pidfile).read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass


def _outcomes(out: str) -> dict:
    """{nom: (issue, detail)} depuis les lignes de progression du lanceur."""
    found = {}
    for m in re.finditer(r"^\[headless\] \[\d+/\d+\] (\S+) -> (\w+) "
                         r"\([^)]*\)(?:: (.*))?$", out, re.M):
        found[m.group(1)] = (m.group(2), m.group(3) or "")
    return found


def _lines(out: str) -> list:
    return [ln for ln in out.splitlines() if ln.strip()]


def _tail(out: str, n: int = 8) -> str:
    return " | ".join(_lines(out)[-n:])[-900:]


def _ended_alone(res: dict, code: int) -> bool:
    return res.get("killed") is False and res.get("code") == code


# --------------------------------------------------------------------- #
# Contrat
# --------------------------------------------------------------------- #

_BAD_OPTIONS = (
    ("v5_grace_zero", _HEADLESS, ["--isolate-grace", "0"], "--isolate-grace"),
    ("v5_grace_negative", _HEADLESS, ["--isolate", "--isolate-grace", "-5"],
     "--isolate-grace"),
    ("v5_timeout_negative", _HEADLESS, ["--timeout", "-1"], "--timeout"),
    ("v5_timeout_nan", _HEADLESS, ["--timeout", "nan"], "--timeout"),
    ("v5_leak_grace_negative", _HEADLESS, ["--leak-grace", "-1"],
     "--leak-grace"),
)


def setup(ctx):
    work = Path(tempfile.mkdtemp(prefix="rl_vtruth_"))
    (work / "cfg").mkdir()
    ctx.data["work"] = str(work)
    ctx.data["fx"] = _write_fixtures(work)


def run(ctx):
    fx = ctx.data["fx"]
    ctx.data["deadline"] = time.monotonic() + _TOTAL_BUDGET_S
    try:
        # V3 d'abord : le plus long a borner, et independant de QGIS.
        _launch(ctx, "v3", fx["pipe_driver"], [], _PIPE_LIMIT_S)
        # V1 : mode par defaut, plantage de la fermeture de Python.
        _launch(ctx, "v1", _HEADLESS, [fx["e_py_segv"]], _RUN_S)
        # V2 : ci_run, meme plantage ; puis une liste verte normale.
        _launch(ctx, "v2", _CI_RUN, [], _RUN_S,
                env_extra={"PYTHONPATH": str(fx["ci_segv"])})
        _launch(ctx, "v2_green", _CI_RUN, [], _RUN_S,
                env_extra={"PYTHONPATH": str(fx["ci_green"])})
        # V4 : connexion non fermee, puis un scenario propre.
        _launch(ctx, "v4", _HEADLESS, [fx["u_unclosed"], fx["a_pass"]], _RUN_S)
        # V5 : options absurdes, toutes avec une cible valide.
        for tag, script, args, _opt in _BAD_OPTIONS:
            _launch(ctx, tag, script, args + [fx["a_pass"]], _OPT_S)
        _launch(ctx, "v5_ci_timeout_negative", _CI_RUN, ["--timeout", "-1"],
                _OPT_S, env_extra={"PYTHONPATH": str(fx["ci_green"])})
        # Meme faute par l'environnement : --dry-run imprime le plan (et
        # la limite retenue) sans lancer QGIS.
        _launch(ctx, "v5_env_timeout_negative", _HEADLESS,
                ["--dry-run", fx["a_pass"]], _OPT_S,
                env_extra={"RECOVERLAND_SCENARIO_TIMEOUT": "-5"})
    finally:
        _kill_grandchild(fx["pipe_pidfile"])
        for stale in _REPORTS.glob(_FX + "*.json"):
            try:
                stale.unlink()
            except OSError:
                pass
        shutil.rmtree(ctx.data["work"], ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = []

    # ===== V1 : pas de vert avant l'issue du processus ===================
    v1 = d.get("v1") or {}
    v1_out = v1.get("out", "")
    green = [ln for ln in _lines(v1_out) if ln.startswith("[headless] ALL GREEN")]
    out.append((
        "v1_no_green_verdict_before_a_crash_in_python_shutdown",
        _ended_alone(v1, 1) and not green
        and "SUITE PROCESS ERROR" in v1_out and "Python shutdown" in v1_out,
        f"code={v1.get('code')} tue={v1.get('killed')} lignes 'ALL GREEN'="
        f"{green} (attendu : aucune, 'SUITE PROCESS ERROR' nomme, exit 1). "
        f"L'enfant imprimait son verdict vert avant de sortir ; un plantage "
        f"de la fermeture de Python venait ensuite : la sortie dit vert puis "
        f"rouge, et qui s'arrete au vert publie un QGIS qui plante en "
        f"quittant. Fin de sortie : {_tail(v1_out)}",
    ))
    o1 = _outcomes(v1_out)
    out.append((
        "v1_scenario_line_still_reports_its_own_verdict",
        o1.get(_FX + "e_py_segv", ("absent",))[0] == "PASS",
        f"ligne de progression de {_FX}e_py_segv={o1.get(_FX + 'e_py_segv')} : "
        f"les assertions du scenario ont passe ; c'est le processus, pas "
        f"le scenario, qui a plante",
    ))

    # ===== V2 : ci_run ==================================================
    v2 = d.get("v2") or {}
    v2_out = v2.get("out", "")
    out.append((
        "v2_ci_run_no_passed_verdict_before_a_crash_in_python_shutdown",
        _ended_alone(v2, 1) and "CI VALIDATION PASSED" not in v2_out
        and "CI VALIDATION ABORTED" in v2_out,
        f"ci_run : code={v2.get('code')} tue={v2.get('killed')} ; "
        f"'CI VALIDATION PASSED' present={'CI VALIDATION PASSED' in v2_out} "
        f"(attendu : absent, 'ABORTED', exit 1). Le job CI affichait vert "
        f"puis avorte. Fin de sortie : {_tail(v2_out)}",
    ))
    v2g = d.get("v2_green") or {}
    v2g_lines = _lines(v2g.get("out", ""))
    out.append((
        "v2_ci_run_green_list_still_says_passed_last",
        _ended_alone(v2g, 0)
        and sum("CI VALIDATION PASSED" in ln for ln in v2g_lines) == 1
        and bool(v2g_lines) and "CI VALIDATION PASSED" in v2g_lines[-1],
        f"ci_run vert : code={v2g.get('code')} tue={v2g.get('killed')} ; "
        f"le verdict doit rester imprime, une fois, en derniere ligne. Fin "
        f"de sortie : {_tail(v2g.get('out', ''))}",
    ))

    # ===== V3 : tube tenu par un petit-fils =============================
    v3 = d.get("v3") or {}
    v3_out = v3.get("out", "")
    m = re.search(r"SUPERVISE_RETURNED after=([\d.]+) code=(\S+) killed=(\S+)",
                  v3_out)
    after = float(m.group(1)) if m else None
    out.append((
        "v3_supervise_returns_while_a_grandchild_holds_the_pipe",
        _ended_alone(v3, 0) and m is not None
        and after < _PIPE_RETURN_MAX_S and m.group(2) == "0"
        and m.group(3) == "False",
        f"supervise rendu apres {after} s (limite {_PIPE_RETURN_MAX_S:g} s, "
        f"le petit-fils garde le tube {_PIPE_HOLD_S} s) ; processus appelant "
        f"code={v3.get('code')} tue={v3.get('killed')}. Sans borne, la suite "
        f"reste figee apres la sortie de son enfant tant qu'un processus "
        f"lance par un scenario vit, et la CI jusqu'a sa propre limite. Fin "
        f"de sortie : {_tail(v3_out)}",
    ))
    out.append((
        "v3_held_pipe_is_named",
        "still holds" in v3_out and "output pipe" in v3_out
        and "Fatal Python error" not in v3_out,
        f"le lanceur doit dire qu'un processus lance par l'enfant tient "
        f"encore son tube de sortie (et que la suite de sa sortie n'est plus "
        f"relayee), sans plantage a la fermeture. Fin de sortie : "
        f"{_tail(v3_out)}",
    ))

    # ===== V4 : le libelle LEAK dit la vraie cause ======================
    v4 = d.get("v4") or {}
    v4_out = v4.get("out", "")
    leaks = dict(re.findall(r"^\[headless\] LEAK (\S+): (.*)$", v4_out, re.M))
    u4 = leaks.get(_FX + "u_unclosed", "")
    out.append((
        "v4_leak_label_names_the_missing_close",
        "zz_vt_unclosed.sqlite" in u4
        and "not closed (released only by the garbage collector)" in u4
        and "reference cycle" not in u4,
        f"ligne LEAK de {_FX}u_unclosed={u4!r} : la cause est un close() "
        f"manquant (une sqlite3.Connection se reference elle-meme sous "
        f"Python 3.12) ; parler d'un cycle de references envoie chercher un "
        f"defaut qui n'existe pas",
    ))
    v4_lines = _lines(v4_out)
    out.append((
        "v4_green_verdict_printed_once_and_last",
        _ended_alone(v4, 0)
        and sum(ln.startswith("[headless] ALL GREEN") for ln in v4_lines) == 1
        and bool(v4_lines) and v4_lines[-1].startswith("[headless] ALL GREEN"),
        f"code={v4.get('code')} tue={v4.get('killed')} : une fuite signalee "
        f"n'est pas un echec par defaut ; le verdict vert doit etre imprime "
        f"une fois, en derniere ligne. Fin de sortie : {_tail(v4_out)}",
    ))

    # ===== V5 : options absurdes ========================================
    for tag, _script, args, opt in _BAD_OPTIONS + (
            ("v5_ci_timeout_negative", _CI_RUN, ["--timeout", "-1"],
             "--timeout"),):
        res = d.get(tag) or {}
        text = res.get("out", "")
        out.append((
            f"{tag}_refused",
            _ended_alone(res, 2) and opt in text
            and "start scenario=" not in text
            and "=== CI scenario" not in text,
            f"{' '.join(args)} : code={res.get('code')} tue={res.get('killed')} "
            f"(attendu 2, message nommant {opt}, aucun scenario lance). "
            f"Accepte, {' '.join(args)} tue chaque enfant sur-le-champ ou "
            f"retire en silence la limite de temps. Fin de sortie : "
            f"{_tail(text, 4)}",
        ))
    env = d.get("v5_env_timeout_negative") or {}
    env_out = env.get("out", "")
    out.append((
        "v5_env_timeout_negative_keeps_a_limit",
        _ended_alone(env, 0) and "timeout per scenario none" not in env_out
        and "timeout per scenario 300 s" in env_out
        and "ignoring RECOVERLAND_SCENARIO_TIMEOUT" in env_out,
        f"RECOVERLAND_SCENARIO_TIMEOUT=-5 : code={env.get('code')} ; la "
        f"valeur etait ramenee a 0, c'est-a-dire aucune limite, sans un mot : "
        f"un scenario fige tenait alors la CI jusqu'a sa propre limite. "
        f"Attendu : valeur ignoree et dite, limite par defaut (300 s). Fin "
        f"de sortie : {_tail(env_out, 4)}",
    ))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
