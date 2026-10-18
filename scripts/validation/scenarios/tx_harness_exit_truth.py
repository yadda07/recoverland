"""tx_harness_exit_truth - RecoverLand validation runtime.

Invariant: I-SUITE-EXIT-TRUTH (un verdict vert exige que le processus qui
l'a produit se soit termine comme ce verdict le dit : un processus mort,
plante ou fige pendant ou apres un scenario n'est jamais vert, et le
lanceur nomme ce qui s'est passe et ce qui n'a pas tourne).

Le mecanisme
------------
La CI lit le verdict d'une suite dans le code de sortie du processus. Ce
code ne dit la verite que s'il vient du rapport. Le lanceur du tour
precedent le laissait venir d'ailleurs dans quatre cas :

1. Mode par defaut (celui de la CI), un seul processus. Un scenario, ou un
   chemin du produit qu'il appelle, termine le processus par
   `os._exit(0)` (ou par un equivalent natif qui sort avec 0). Il n'y a
   alors plus de rapport ni de liste NON EXECUTE, les scenarios suivants
   (y compris ceux qui echouent) ne tournent jamais, et le code 0 dit
   "tout est vert". Ni `atexit` ni `finally` ne l'attrapent : seul un
   processus exterieur peut le voir.
2. `--isolate` : l'enfant ecrit son verdict, PUIS meurt en fermant QGIS
   (`exitQgis`) ou pendant la finalisation de Python (un destructeur
   natif, un `~QThread` sur un thread qui tourne). Le parent ignorait le
   code de sortie des qu'un verdict existait : PASS. De plus, l'enfant
   sortait toujours par `os._exit`, si bien que les plantages de
   finalisation ne pouvaient meme pas se produire sous test.
3. Un processus qui se fige en fermant QGIS (une QgsTask qui ne s'arrete
   pas bloque `exitQgis`) : sous `--isolate` il etait tue puis compte
   PASS avec une simple note de fuite ; en mode par defaut, la suite ne
   se terminait jamais. Pour l'utilisateur, c'est QGIS qui ne se ferme
   plus.
4. Un thread non-daemon encore vivant a la fin (demarre a
   l'initialisation, avant la premiere mesure de fuite) n'etait signale
   que par une ligne imprimee APRES "ALL GREEN", et la suite sortait 0
   meme sous `--strict-leaks`. `ci_run.py` faisait de meme ; avant, il se
   figeait (rouge a la limite du job), il sortait maintenant vert.

S'y ajoute la mesure des fuites elle-meme : le `gc.collect()` qu'elle
force entre deux scenarios fermait sans le dire une connexion SQLite
tenue par un cycle de references. Dans QGIS, cette connexion resterait
ouverte (journal verrouille) jusqu'au prochain passage du ramasse-miettes,
pendant le scenario suivant : le defaut d'ordre qu'elle provoquerait
etait cache, et aucune ligne LEAK ne le disait.

Ce que le scenario verifie, en lancant le vrai `headless_run.py` (et
`ci_run.py`) de CETTE copie du plugin dans des processus enfants, sur des
scenarios-fixtures ecrits dans un dossier temporaire :

    X1   defaut : os._exit(0) au milieu -> exit 1, coupable nomme ERROR,
         suivants NON EXECUTES, rapport present
    X2   defaut : plantage dans exitQgis apres PASS -> exit 1, nomme
    X2b  defaut : blocage dans exitQgis apres PASS -> exit 3, borne, nomme
    X3   --isolate : plantage d'exitQgis, plantage de finalisation et
         blocage a la fermeture, chacun apres PASS -> ERROR, ERROR,
         TIMEOUT ; un enfant propre reste PASS
    X4   --strict-leaks + thread non-daemon d'initialisation -> exit 1
    X5   sans --strict-leaks : ce thread est dit AVANT le verdict ; la
         connexion fermee par le seul ramasse-miettes est signalee
    X6   ci_run : os._exit(0) au milieu -> exit 1, coupable et suivants
         nommes
    X7   ci_run : thread non-daemon a la fin -> exit 1, dit avant le
         verdict

X1, X2, X6 et X7 n'utilisent que la ligne de commande d'origine
(cibles simples, liste CI par defaut via un paquet `recoverland` fantome
en tete du PYTHONPATH) : sur le code d'origine, elles echouent pour le
defaut lui-meme et non pour une option inconnue.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_harness_exit_truth"
INVARIANT = "I-SUITE-EXIT-TRUTH"

_VALIDATION_DIR = Path(__file__).resolve().parents[1]
_PLUGIN_ROOT = _VALIDATION_DIR.parents[1]
_HEADLESS = _VALIDATION_DIR / "headless_run.py"
_CI_RUN = _VALIDATION_DIR / "ci_run.py"
_REPORTS = _VALIDATION_DIR / "reports"
_FX = "zz_xt_"  # court : le chemin du rapport reste sous 260 caracteres
_INIT_THREAD = "zz-exittruth-init-nondaemon"

# Budget total du scenario, sous le delai par defaut d'un scenario du
# lanceur (300 s) : un lanceur lent fait echouer un groupe nomme, jamais
# expirer toute la suite.
_TOTAL_BUDGET_S = 270.0
_RUN_S = 45.0
_ISOLATE_S = 120.0


# --------------------------------------------------------------------- #
# Fixtures : de vrais scenarios au contrat setup/run/assertions
# --------------------------------------------------------------------- #

_TEMPLATE = '''
SCENARIO_ID = "{sid}"
INVARIANT = "fixture"


def setup(ctx):
    return


def run(ctx):
{body}


def assertions(ctx):
    return [("fixture_verdict", {ok}, "fixture {sid}")]
'''

_PASS = "    return"
_EXIT0 = ("    import os, sys\n"
          "    sys.stdout.flush()\n"
          "    os._exit(0)  # un chemin qui termine le processus avec 0")
_QGIS_EXIT_CRASH = ("    import os, sys\n"
                    "    from qgis.core import QgsApplication\n"
                    "\n"
                    "    def _crash(*_a, **_k):\n"
                    "        sys.stdout.flush()\n"
                    "        os._exit(134)  # comme un abort() natif\n"
                    "\n"
                    "    QgsApplication.exitQgis = _crash")
_QGIS_EXIT_HANG = ("    import threading\n"
                   "    from qgis.core import QgsApplication\n"
                   "    # comme une QgsTask qui ne s'arrete jamais\n"
                   "    QgsApplication.exitQgis = (\n"
                   "        lambda *_a, **_k: threading.Event().wait())")
_FINAL_SEGV = ("    import atexit, faulthandler\n"
               "    # lecture a l'adresse NULL pendant la finalisation de Python\n"
               "    atexit.register(faulthandler._read_null)")
_CYCLE_CONN = ("    import gc, os, sqlite3\n"
               "\n"
               "    class _Holder:\n"
               "        pass\n"
               "\n"
               "    h = _Holder()\n"
               "    h.conn = sqlite3.connect(os.path.join({base!r}, "
               "\"zz_exittruth_cycle.sqlite\"))\n"
               "    h.me = h  # cycle : seul le ramasse-miettes le liberera\n"
               "    gc.collect()  # vieillit le cycle : les collectes jeunes "
               "ne le touchent plus")

# Demarre un thread non-daemon dans le processus qui importe qgis.core,
# c'est-a-dire celui qui initialise QGIS, avant tout scenario.
_USERCUSTOMIZE = '''import os
import sys
import threading

if os.environ.get("ZZ_EXITTRUTH_INIT_THREAD") == "1":
    class _StartOnQgisImport:
        started = False

        def find_spec(self, name, path=None, target=None):
            if name == "qgis.core" and not self.started:
                self.started = True
                stop = threading.Event()
                threading.Thread(target=stop.wait, name={name!r},
                                 daemon=False).start()
            return None

    sys.meta_path.insert(0, _StartOnQgisImport())
'''

_CI_PASS = "def run():\n    return {'verdict': 'PASS'}\n"
_CI_FAIL = "def run():\n    return {'verdict': 'FAIL', 'failed': 1}\n"
_CI_EXIT0 = ("def run():\n    import os, sys\n    sys.stdout.flush()\n"
             "    os._exit(0)\n")
_CI_NAMES = ("compat_cross_version", "rv_snapshot_asof", "rv_snapshot_volume")


def _write_fixture(fx_dir: Path, sid: str, body: str, ok: bool = True) -> Path:
    path = fx_dir / f"{sid}.py"
    path.write_text(_TEMPLATE.format(sid=sid, body=body, ok=ok),
                    encoding="utf-8")
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
    base = work / "area"
    base.mkdir()
    fx = {}
    fx["a_pass"] = _write_fixture(fx_dir, _FX + "a_pass", _PASS)
    fx["b_exit0"] = _write_fixture(fx_dir, _FX + "b_exit0", _EXIT0)
    fx["c_fail"] = _write_fixture(fx_dir, _FX + "c_fail", _PASS, ok=False)
    fx["d_qx_crash"] = _write_fixture(
        fx_dir, _FX + "d_qx_crash", _QGIS_EXIT_CRASH)
    fx["e_py_segv"] = _write_fixture(
        fx_dir, _FX + "e_py_segv", _FINAL_SEGV)
    fx["f_qx_hang"] = _write_fixture(
        fx_dir, _FX + "f_qx_hang", _QGIS_EXIT_HANG)
    fx["g_cycle"] = _write_fixture(
        fx_dir, _FX + "g_cycle", _CYCLE_CONN.format(base=str(base)))
    site = work / "site"
    site.mkdir()
    (site / "usercustomize.py").write_text(
        _USERCUSTOMIZE.format(name=_INIT_THREAD), encoding="utf-8")
    fx["site"] = site
    fx["ci_exit0"] = _write_ci_shadow(work / "ci_exit0", {
        _CI_NAMES[0]: _CI_PASS, _CI_NAMES[1]: _CI_EXIT0,
        _CI_NAMES[2]: _CI_FAIL})
    fx["ci_green"] = _write_ci_shadow(work / "ci_green", {
        name: _CI_PASS for name in _CI_NAMES})
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
            import signal
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
    env.pop("ZZ_EXITTRUTH_INIT_THREAD", None)
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


def _outcomes(out: str) -> dict:
    """{nom: (issue, detail)} depuis les lignes de progression du lanceur."""
    found = {}
    for m in re.finditer(r"^\[headless\] \[\d+/\d+\] (\S+) -> (\w+) "
                         r"\([^)]*\)(?:: (.*))?$", out, re.M):
        found[m.group(1)] = (m.group(2), m.group(3) or "")
    return found


def _tail(out: str, n: int = 8) -> str:
    lines = [ln for ln in out.splitlines() if ln.strip()]
    return " | ".join(lines[-n:])[-900:]


def _ended_alone(res: dict, code: int) -> bool:
    return res.get("killed") is False and res.get("code") == code


# --------------------------------------------------------------------- #
# Contrat
# --------------------------------------------------------------------- #

def setup(ctx):
    work = Path(tempfile.mkdtemp(prefix="rl_exittruth_"))
    (work / "cfg").mkdir()
    ctx.data["work"] = str(work)
    ctx.data["fx"] = _write_fixtures(work)


def run(ctx):
    fx = ctx.data["fx"]
    ctx.data["deadline"] = time.monotonic() + _TOTAL_BUDGET_S
    init_thread = {"ZZ_EXITTRUTH_INIT_THREAD": "1",
                   "PYTHONPATH": str(fx["site"])}
    try:
        # X1 : mode par defaut, cibles simples (ligne de commande d'origine).
        _launch(ctx, "x1", _HEADLESS,
                [fx["a_pass"], fx["b_exit0"], fx["c_fail"]], _RUN_S)
        # X2 / X2b : fin de QGIS qui plante, puis qui se fige.
        _launch(ctx, "x2", _HEADLESS, [fx["d_qx_crash"]], _RUN_S)
        _launch(ctx, "x2b", _HEADLESS,
                ["--isolate-grace", "10", fx["f_qx_hang"]], _RUN_S)
        # X3 : un enfant par scenario.
        _launch(ctx, "x3", _HEADLESS,
                ["--isolate", "--timeout", "20", "--isolate-grace", "15",
                 fx["d_qx_crash"], fx["e_py_segv"],
                 fx["f_qx_hang"], fx["a_pass"]], _ISOLATE_S)
        # X4 / X5 : thread non-daemon demarre a l'initialisation de QGIS.
        _launch(ctx, "x4", _HEADLESS, ["--strict-leaks", fx["a_pass"]],
                _RUN_S, env_extra=init_thread)
        _launch(ctx, "x5", _HEADLESS, [fx["g_cycle"], fx["a_pass"]],
                _RUN_S, env_extra=init_thread)
        # X6 / X7 : le lanceur CI, sur sa liste par defaut.
        _launch(ctx, "x6", _CI_RUN, [], _RUN_S,
                env_extra={"PYTHONPATH": str(fx["ci_exit0"])})
        _launch(ctx, "x7", _CI_RUN, [], _RUN_S,
                env_extra={"PYTHONPATH": os.pathsep.join(
                    [str(fx["ci_green"]), str(fx["site"])]),
                    "ZZ_EXITTRUTH_INIT_THREAD": "1"})
    finally:
        for stale in _REPORTS.glob(_FX + "*.json"):
            try:
                stale.unlink()
            except OSError:
                pass
        shutil.rmtree(ctx.data["work"], ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = []

    # ===== X1 : un scenario qui termine le processus avec 0 ===============
    x1 = d.get("x1") or {}
    o1 = _outcomes(x1.get("out", ""))
    b1 = o1.get(_FX + "b_exit0", ("absent", ""))
    out.append((
        "x1_process_death_is_not_green",
        _ended_alone(x1, 1),
        f"code={x1.get('code')} tue={x1.get('killed')} (attendu 1). Un "
        f"scenario, ou un chemin du produit, qui termine le processus avec "
        f"0 laissait la suite verte, sans rapport : la CI publie comme verte "
        f"une regression que le scenario suivant aurait montree. Fin de "
        f"sortie : {_tail(x1.get('out', ''))}",
    ))
    out.append((
        "x1_culprit_named_error_with_its_exit_code",
        b1[0] == "ERROR" and "code 0" in b1[1],
        f"issue de {_FX}b_exit0={b1} : le scenario pendant lequel le "
        f"processus est mort doit etre nomme ERROR avec le code de sortie, "
        f"sinon personne ne sait ou chercher",
    ))
    out.append((
        "x1_later_scenarios_listed_not_run",
        f"  ~ {_FX}c_fail" in x1.get("out", "")
        and f"start scenario={_FX}c_fail" not in x1.get("out", ""),
        f"{_FX}c_fail (un echec) n'est pas annonce NON EXECUTE : un scenario "
        f"qui aurait echoue disparait du rapport sans que personne ne le "
        f"sache",
    ))
    out.append((
        "x1_report_keeps_the_earlier_verdict",
        "[headless] 1/2 scenario(s) PASS" in x1.get("out", ""),
        f"pas de rapport '1/2 scenario(s) PASS' : le verdict deja acquis par "
        f"{_FX}a_pass est perdu avec le processus",
    ))

    # ===== X2 / X2b : fermeture de QGIS qui plante ou se fige ============
    x2 = d.get("x2") or {}
    x2_out = x2.get("out", "")
    out.append((
        "x2_crash_in_qgis_shutdown_is_not_green_and_named",
        _ended_alone(x2, 1) and "134" in x2_out
        and "QGIS shutdown" in x2_out and "ALL GREEN" not in x2_out,
        f"code={x2.get('code')} tue={x2.get('killed')} (attendu 1, code 134 et "
        f"'QGIS shutdown' dans le rapport). Un processus qui plante en "
        f"fermant QGIS apres tous les verdicts sortait avec un code hors "
        f"contrat (134) et sans rapport : QGIS plante a la fermeture chez "
        f"l'utilisateur et personne ne sait apres quel scenario. Fin de "
        f"sortie : {_tail(x2_out)}",
    ))
    x2b = d.get("x2b") or {}
    x2b_out = x2b.get("out", "")
    out.append((
        "x2b_hang_in_qgis_shutdown_is_bounded_and_named",
        _ended_alone(x2b, 3) and "exitQgis" in x2b_out and "hung" in x2b_out,
        f"code={x2b.get('code')} tue_par_le_scenario={x2b.get('killed')} apres "
        f"{x2b.get('elapsed')} s (attendu : fin d'elle-meme, exit 3, "
        f"'exitQgis' nomme). Une fermeture de QGIS qui se fige tenait la "
        f"suite, et le job CI, jusqu'a sa limite, sans verdict : pour "
        f"l'utilisateur, QGIS ne se ferme plus. Fin de sortie : "
        f"{_tail(x2b_out)}",
    ))

    # ===== X3 : --isolate, le code de sortie apres le verdict ============
    x3 = d.get("x3") or {}
    o3 = _outcomes(x3.get("out", ""))
    d3 = o3.get(_FX + "d_qx_crash", ("absent", ""))
    e3 = o3.get(_FX + "e_py_segv", ("absent", ""))
    f3 = o3.get(_FX + "f_qx_hang", ("absent", ""))
    a3 = o3.get(_FX + "a_pass", ("absent", ""))
    out.append((
        "x3_crash_in_exitqgis_after_verdict_is_error",
        d3[0] == "ERROR" and "134" in d3[1],
        f"issue de {_FX}d_qx_crash={d3} : l'enfant a ecrit PASS puis "
        f"est mort avec le code 134 en fermant QGIS ; le parent l'affichait "
        f"PASS. Fin de sortie : {_tail(x3.get('out', ''))}",
    ))
    out.append((
        "x3_crash_in_python_shutdown_after_verdict_is_error",
        e3[0] == "ERROR" and "Python shutdown" in e3[1],
        f"issue de {_FX}e_py_segv={e3} : un acces a NULL pendant la "
        f"finalisation de Python (destructeur natif, ~QThread) doit etre un "
        f"ERROR nomme ; l'enfant sortait par os._exit et ce plantage ne "
        f"pouvait meme pas se produire sous test",
    ))
    out.append((
        "x3_hang_in_qgis_shutdown_after_verdict_is_not_green",
        f3[0] == "TIMEOUT" and "hung" in f3[1],
        f"issue de {_FX}f_qx_hang={f3} : un enfant fige en fermant "
        f"QGIS apres PASS etait tue puis compte PASS ; c'est un QGIS qui ne "
        f"se ferme plus",
    ))
    out.append((
        "x3_clean_child_stays_pass",
        a3[0] == "PASS",
        f"issue de {_FX}a_pass={a3} : un enfant qui se termine proprement, "
        f"finalisation de Python comprise, doit rester PASS (sinon le "
        f"controle invente des erreurs)",
    ))
    out.append((
        "x3_exit_code_is_timeout",
        _ended_alone(x3, 3),
        f"code={x3.get('code')} tue={x3.get('killed')} : attendu 3 (un "
        f"blocage a la fermeture ; 3 prime sur 1)",
    ))

    # ===== X4 / X5 : thread non-daemon et ramasse-miettes ===============
    x4 = d.get("x4") or {}
    x4_out = x4.get("out", "")
    out.append((
        "x4_init_nondaemon_thread_fails_strict_leaks",
        _ended_alone(x4, 1) and _INIT_THREAD in x4_out
        and "ALL GREEN" not in x4_out,
        f"code={x4.get('code')} tue={x4.get('killed')} (attendu 1 sous "
        f"--strict-leaks). Un thread non-daemon demarre a l'initialisation "
        f"retient le processus a la sortie (QGIS ne se ferme plus) ; il "
        f"n'etait dit qu'apres 'ALL GREEN' et la suite sortait 0. Fin de "
        f"sortie : {_tail(x4_out)}",
    ))
    x5 = d.get("x5") or {}
    x5_out = x5.get("out", "")
    green_at = x5_out.find("[headless] ALL GREEN")
    before, after = ((x5_out[:green_at], x5_out[green_at:]) if green_at >= 0
                     else ("", x5_out))
    out.append((
        "x5_nondaemon_thread_reported_before_the_verdict",
        x5.get("killed") is False and _INIT_THREAD in before
        and "alive forever" not in after,
        f"tue={x5.get('killed')} ; ligne du thread {_INIT_THREAD} avant le "
        f"verdict={_INIT_THREAD in before}, 'alive forever' apres le "
        f"verdict={'alive forever' in after} : une sortie qui dit ALL GREEN "
        f"puis 'vivant pour toujours' se contredit, et le lecteur s'arrete "
        f"au vert. Fin de sortie : {_tail(x5_out)}",
    ))
    leaks = dict(re.findall(r"^\[headless\] LEAK (\S+): (.*)$", x5_out, re.M))
    g5 = leaks.get(_FX + "g_cycle", "")
    out.append((
        "x5_connection_closed_only_by_gc_is_reported",
        "zz_exittruth_cycle.sqlite" in g5 and "garbage collector" in g5,
        f"ligne LEAK de {_FX}g_cycle={g5!r} : une connexion SQLite tenue "
        f"par un cycle reste ouverte, journal verrouille, jusqu'au prochain "
        f"passage du ramasse-miettes ; la mesure la fermait sans le dire et "
        f"cachait ainsi le defaut d'ordre qu'elle provoque",
    ))

    # ===== X6 / X7 : ci_run ==============================================
    x6 = d.get("x6") or {}
    x6_out = x6.get("out", "")
    out.append((
        "x6_ci_run_process_death_is_not_green",
        _ended_alone(x6, 1),
        f"ci_run : code={x6.get('code')} tue={x6.get('killed')} (attendu 1). "
        f"Un scenario CI qui termine le processus avec 0 donnait un job "
        f"vert, sans verdict pour lui ni pour les suivants. Fin de sortie : "
        f"{_tail(x6_out)}",
    ))
    out.append((
        "x6_ci_run_names_culprit_and_not_run",
        re.search(r"^\s+! \S*rv_snapshot_asof\b.*code 0", x6_out, re.M)
        is not None
        and re.search(r"^\s+~ \S*rv_snapshot_volume\b.*NOT RUN", x6_out, re.M)
        is not None
        and "=== CI scenario: recoverland.scripts.validation.scenarios."
            "rv_snapshot_volume" not in x6_out,
        f"ci_run doit nommer rv_snapshot_asof (mort, code 0) et annoncer "
        f"rv_snapshot_volume NON EXECUTE. Fin de sortie : {_tail(x6_out)}",
    ))
    x7 = d.get("x7") or {}
    x7_out = x7.get("out", "")
    verdict_at = x7_out.find("CI VALIDATION")
    out.append((
        "x7_ci_run_nondaemon_thread_is_not_green",
        _ended_alone(x7, 1) and "CI VALIDATION PASSED" not in x7_out
        and 0 <= x7_out.find(_INIT_THREAD) < verdict_at,
        f"ci_run : code={x7.get('code')} tue={x7.get('killed')} (attendu 1, "
        f"le thread {_INIT_THREAD} nomme avant le verdict). Un thread "
        f"non-daemon qui retient le processus figeait ci_run (rouge a la "
        f"limite du job) ; il sortait vert. Fin de sortie : {_tail(x7_out)}",
    ))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
