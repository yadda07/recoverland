"""tx_harness_suite_isolation - RecoverLand validation runtime.

Invariant: I-SUITE-ISOLATION (le verdict d'un scenario ne depend que de
lui : un scenario qui se bloque, plante, fuit ou depend de l'ordre ne
decide pas du sort des autres, et le lanceur le dit en le nommant).

Le mecanisme
------------
`headless_run.py` executait tous les scenarios dans un seul processus,
toujours dans le meme ordre (tri par nom), sans limite de temps et sans
rien verifier entre deux scenarios :

    for path in scenarios:
        module = _load_module(str(path))
        verdict = run_scenario(module)       # aucun delai, aucun bilan

Trois consequences concretes pour qui lit le rapport :

1. Un scenario qui se bloque (verrou jamais relache, `wait()` sans
   delai, boucle Qt jamais pompee) fige toute la suite. La CI attend
   jusqu'a sa propre limite, puis tue le job : aucun verdict, aucun nom
   de coupable, aucune pile. Les scenarios suivants ne tournent jamais et
   personne ne le sait. Meme chose pour un plantage dur (segfault de
   QGIS, `os._exit`) : tout le processus disparait.

2. L'ordre fixe cache les dependances d'ordre : un scenario qui ne passe
   que parce qu'un voisin a laisse un etat derriere lui (ou qui echoue a
   cause de lui) donne toujours le meme verdict, faux dans les deux cas.
   Sans graine imprimee, un echec observe une fois ne se reproduit pas.

3. Rien n'est libere ni mesure : un thread d'ecriture `RecoverLand-*`,
   une couche ajoutee au projet, une connexion SQLite ou un segment
   d'outbox ouverts par un scenario vivent pendant tous les suivants, qui
   mesurent alors les restes du voisin. Le contrat n'avait meme pas de
   `teardown(ctx)`.

Ce que le scenario verifie, en lancant le vrai `headless_run.py` (et
`ci_run.py`) de CETTE copie du plugin dans des processus enfants, sur des
scenarios-fixtures ecrits dans un dossier temporaire :

    R0  en processus, delai par RECOVERLAND_SCENARIO_TIMEOUT, cibles
        simples : un scenario bloque -> TIMEOUT nomme, piles, exit 3, les
        suivants annonces NON EXECUTES (au lieu d'une suite figee)
    R1  --isolate : bloque, bloque en tenant le GIL, plantage, echec ->
        TIMEOUT / TIMEOUT / ERROR / FAIL distincts, et le scenario place
        apres eux tourne quand meme
    R2  --shuffle SEED --repeat N : graine imprimee, ordre reproductible
        (--dry-run redonne l'ordre execute), autre graine = autre ordre,
        un scenario instable est signale UNSTABLE
    R3  fuites : thread RecoverLand, couche, connexion SQLite, segment
        d'outbox laisses par un scenario -> ligne LEAK a SON nom, pas a
        celui du suivant, le thread designe comme thread du produit ;
        teardown(ctx) appele et efficace ; pas un echec par defaut
    R4  --strict-leaks : la meme fuite fait echouer la suite (exit 1) et sa
        ligne de progression dit LEAK, pas PASS
    R5  ci_run.py --timeout : meme borne pour le lanceur CI (exit 3)
    R5b la meme borne sur la liste CI par defaut, avec la seule ligne de
        commande d'origine (delai par RECOVERLAND_SCENARIO_TIMEOUT, paquet
        `recoverland` fantome en tete du PYTHONPATH dont rv_snapshot_asof
        se bloque) : sur le code d'origine, elle echoue parce que ci_run se
        fige, et non parce qu'une option est refusee

R3 n'utilise, lui aussi, que la ligne de commande d'origine (cibles
simples) : sur le code d'origine, il echoue parce que teardown(ctx) n'est
pas appele et que rien n'est mesure. Le scenario tient dans un budget total
(`_TOTAL_BUDGET_S`) sous le delai par defaut d'un scenario du lanceur
(300 s) : sur une machine lente, c'est un groupe nomme qui echoue, jamais
toute la suite qui expire sur ce scenario.
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

SCENARIO_ID = "tx_harness_suite_isolation"
INVARIANT = "I-SUITE-ISOLATION"

_VALIDATION_DIR = Path(__file__).resolve().parents[1]
_PLUGIN_ROOT = _VALIDATION_DIR.parents[1]
_HEADLESS = _VALIDATION_DIR / "headless_run.py"
_CI_RUN = _VALIDATION_DIR / "ci_run.py"
_REPORTS = _VALIDATION_DIR / "reports"
_FX = "zz_suiteiso_"

# Budgets du scenario lui-meme (l'enfant ne doit jamais bloquer CE scenario).
# Chaque lancement est aussi borne par ce qui reste du budget total, lui-meme
# sous le delai par defaut d'un scenario du lanceur (300 s).
_R0_OUTER_S = 60.0
_R1_OUTER_S = 150.0
_DEFAULT_OUTER_S = 120.0
_R5B_OUTER_S = 40.0
_TOTAL_BUDGET_S = 270.0


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
_HANG = ("    import threading\n"
         "    threading.Event().wait()  # bloque pour toujours, GIL libre")
# PyDLL ne relache PAS le GIL pendant l'appel : le chien de garde Python
# du processus ne peut plus tourner, seul un tiers peut arreter l'enfant.
_HANG_GIL = ("    import ctypes, os\n"
             "    if os.name == 'nt':\n"
             "        ctypes.PyDLL('kernel32').Sleep(600000)\n"
             "    else:\n"
             "        ctypes.PyDLL(None).sleep(600)")
_CRASH = ("    import os, sys\n"
          "    sys.stdout.flush()\n"
          "    os._exit(7)  # comme un segfault : pas de finally, pas de verdict")
_FLAKY = ("    import os\n"
          "    n = int(os.environ.get('ZZ_SUITEISO_FLAKY_RUNS', '0')) + 1\n"
          "    os.environ['ZZ_SUITEISO_FLAKY_RUNS'] = str(n)\n"
          "    ctx.data['ok'] = (n == 1)")

_LEAK_BODY = '''    import os, sqlite3, threading
    from qgis.core import QgsProject, QgsVectorLayer
    base = {base!r}
    layer = QgsVectorLayer("Point?crs=EPSG:4326", "zz_suiteiso_leaked_layer",
                           "memory")
    QgsProject.instance().addMapLayer(layer)
    outbox = os.path.join(base, "leak_journal.sqlite.outbox")
    os.makedirs(outbox, exist_ok=True)
    conn = sqlite3.connect(os.path.join(base, "leak_journal.sqlite"),
                           check_same_thread=False)
    seg = open(os.path.join(outbox, "seg-00000001.jsonl"), "ab")
    stop = threading.Event()

    def _writer(conn, seg, stop):  # un writer jamais arrete
        stop.wait()

    threading.Thread(target=_writer, args=(conn, seg, stop),
                     name="RecoverLand-FixtureLeak", daemon=True).start()'''

_TEARDOWN_FIXTURE = '''
SCENARIO_ID = "zz_suiteiso_leak_c_teardown"
INVARIANT = "fixture"


def setup(ctx):
    return


def run(ctx):
    import threading
    stop = threading.Event()
    t = threading.Thread(target=stop.wait, name="RecoverLand-FixtureTeardown",
                         daemon=True)
    t.start()
    ctx.data["stop"] = stop
    ctx.data["thread"] = t


def assertions(ctx):
    return [("fixture_verdict", True, "fixture teardown")]


def teardown(ctx):
    ctx.data["stop"].set()
    ctx.data["thread"].join(5)
    with open({marker!r}, "w") as fh:
        fh.write("teardown called")
'''

_FLAKY_FIXTURE_ASSERT = '''

def assertions(ctx):  # noqa: F811 - remplace celle du gabarit
    return [("fixture_verdict", ctx.data.get("ok") is True, "flaky")]
'''

_CI_PASS = "def run():\n    return {'verdict': 'PASS'}\n"
_CI_HANG = ("def run():\n    import threading\n"
            "    threading.Event().wait()\n")
_CI_DEFAULT_LIST = ("compat_cross_version", "rv_snapshot_asof",
                    "rv_snapshot_volume")


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


def _write_fixture(fx_dir: Path, sid: str, body: str, ok: bool = True) -> Path:
    path = fx_dir / f"{sid}.py"
    path.write_text(_TEMPLATE.format(sid=sid, body=body, ok=ok),
                    encoding="utf-8")
    return path


def _write_fixtures(fx_dir: Path, marker: Path) -> dict:
    fx = {}
    fx["a_pass"] = _write_fixture(fx_dir, _FX + "a_pass", _PASS)
    fx["b_hang"] = _write_fixture(fx_dir, _FX + "b_hang", _HANG)
    fx["c_hang_gil"] = _write_fixture(fx_dir, _FX + "c_hang_gil", _HANG_GIL)
    fx["d_crash"] = _write_fixture(fx_dir, _FX + "d_crash", _CRASH)
    fx["e_fail"] = _write_fixture(fx_dir, _FX + "e_fail", _PASS, ok=False)
    fx["f_pass"] = _write_fixture(fx_dir, _FX + "f_pass", _PASS)
    for i in range(1, 5):
        fx[f"o{i}"] = _write_fixture(fx_dir, f"{_FX}o{i}", _PASS)
    flaky = _write_fixture(fx_dir, _FX + "o5_flaky", _FLAKY)
    flaky.write_text(flaky.read_text(encoding="utf-8") + _FLAKY_FIXTURE_ASSERT,
                     encoding="utf-8")
    fx["o5_flaky"] = flaky
    leak_base = fx_dir / "leak_area"
    leak_base.mkdir(exist_ok=True)
    fx["leak_a"] = _write_fixture(fx_dir, _FX + "leak_a",
                                  _LEAK_BODY.format(base=str(leak_base)))
    fx["leak_b_clean"] = _write_fixture(fx_dir, _FX + "leak_b_clean", _PASS)
    path = fx_dir / f"{_FX}leak_c_teardown.py"
    path.write_text(_TEARDOWN_FIXTURE.format(marker=str(marker)),
                    encoding="utf-8")
    fx["leak_c_teardown"] = path
    ci_dir = fx_dir / "ci_modules"
    ci_dir.mkdir(exist_ok=True)
    (ci_dir / f"{_FX}ci_pass.py").write_text(_CI_PASS, encoding="utf-8")
    (ci_dir / f"{_FX}ci_hang.py").write_text(_CI_HANG, encoding="utf-8")
    fx["ci_dir"] = ci_dir
    fx["ci_shadow_hang"] = _write_ci_shadow(fx_dir / "ci_shadow", {
        _CI_DEFAULT_LIST[0]: _CI_PASS, _CI_DEFAULT_LIST[1]: _CI_HANG,
        _CI_DEFAULT_LIST[2]: _CI_PASS})
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
    if "deadline" in ctx.data:
        limit_s = max(5.0, min(limit_s,
                               ctx.data["deadline"] - time.monotonic()))
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
           "elapsed": round(elapsed, 1), "out": out}
    print(f"[{SCENARIO_ID}] {tag}: code={res['code']} killed={killed} "
          f"elapsed={res['elapsed']} s (limit {limit_s:.1f} s)")
    ctx.data[tag] = res
    return res


def _outcomes(out: str) -> dict:
    """{nom: (issue, detail)} depuis les lignes de progression du lanceur."""
    found = {}
    for m in re.finditer(r"^\[headless\] \[\d+/\d+\] (\S+) -> (\w+) "
                         r"\([^)]*\)(?:: (.*))?$", out, re.M):
        found[m.group(1)] = (m.group(2), m.group(3) or "")
    return found


def _tail(out: str, n: int = 6) -> str:
    lines = [ln for ln in out.splitlines() if ln.strip()]
    return " | ".join(lines[-n:])[-600:]


# --------------------------------------------------------------------- #
# Contrat
# --------------------------------------------------------------------- #

def setup(ctx):
    work = Path(tempfile.mkdtemp(prefix="rl_suiteiso_"))
    (work / "cfg").mkdir()
    fx_dir = work / "fixtures"
    fx_dir.mkdir()
    marker = work / "teardown_marker.txt"
    ctx.data["work"] = str(work)
    ctx.data["marker"] = str(marker)
    ctx.data["fx"] = _write_fixtures(fx_dir, marker)


def run(ctx):
    fx = ctx.data["fx"]
    ctx.data["deadline"] = time.monotonic() + _TOTAL_BUDGET_S
    try:
        # R0 : en processus, cibles simples, delai par l'environnement.
        _launch(ctx, "r0", _HEADLESS,
                [fx["a_pass"], fx["b_hang"], fx["f_pass"]], _R0_OUTER_S,
                env_extra={"RECOVERLAND_SCENARIO_TIMEOUT": "5"})

        # R1 : un enfant par scenario.
        _launch(ctx, "r1", _HEADLESS,
                ["--isolate", "--timeout", "4", "--isolate-grace", "25",
                 fx["a_pass"], fx["b_hang"], fx["c_hang_gil"], fx["d_crash"],
                 fx["e_fail"], fx["f_pass"]], _R1_OUTER_S)

        # R2 : ordre aleatoire reproductible, repetitions.
        order_targets = [fx["o1"], fx["o2"], fx["o3"], fx["o4"], fx["o5_flaky"]]
        _launch(ctx, "r2", _HEADLESS,
                ["--shuffle", "12345", "--repeat", "2", "--timeout", "60"]
                + order_targets, _DEFAULT_OUTER_S)
        _launch(ctx, "r2_dry_same", _HEADLESS,
                ["--dry-run", "--shuffle", "12345", "--repeat", "2"]
                + order_targets, _DEFAULT_OUTER_S)
        _launch(ctx, "r2_dry_other", _HEADLESS,
                ["--dry-run", "--shuffle", "777", "--repeat", "2"]
                + order_targets, _DEFAULT_OUTER_S)
        _launch(ctx, "r2_dry_default", _HEADLESS,
                ["--dry-run"] + order_targets, _DEFAULT_OUTER_S)

        # R3 / R4 : fuites. R3 n'a que des cibles simples (ligne de commande
        # d'origine) : sur le code d'origine, il echoue parce que teardown
        # n'est pas appele et que rien n'est mesure, pas sur une option
        # refusee. Le delai de grace par defaut (1 s) remplace 0,5 s sans
        # rien changer au verdict attendu : le thread de la fixture ne
        # s'arrete jamais, il est compte apres 1 s comme apres 0,5 s.
        _launch(ctx, "r3", _HEADLESS,
                [fx["leak_a"], fx["leak_b_clean"], fx["leak_c_teardown"]],
                _DEFAULT_OUTER_S)
        _launch(ctx, "r4", _HEADLESS,
                ["--strict-leaks", "--leak-grace", "0.5", fx["leak_a"]],
                _DEFAULT_OUTER_S)

        # R5 : le lanceur CI.
        pythonpath = os.pathsep.join([str(fx["ci_dir"]),
                                      str(_PLUGIN_ROOT.parent)])
        _launch(ctx, "r5", _CI_RUN,
                ["--timeout", "4", _FX + "ci_pass", _FX + "ci_hang"],
                _R0_OUTER_S, env_extra={"PYTHONPATH": pythonpath})
        # R5b : la liste CI par defaut, ligne de commande d'origine.
        _launch(ctx, "r5b", _CI_RUN, [], _R5B_OUTER_S,
                env_extra={"PYTHONPATH": str(fx["ci_shadow_hang"]),
                           "RECOVERLAND_SCENARIO_TIMEOUT": "4"})
        ctx.data["teardown_called"] = Path(ctx.data["marker"]).is_file()
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

    # ===== R0 : un scenario bloque ne fige plus la suite =================
    r0 = d.get("r0") or {}
    o0 = _outcomes(r0.get("out", ""))
    out.append((
        "r0_hung_scenario_does_not_freeze_the_suite",
        r0.get("killed") is False and r0.get("code") == 3,
        f"code={r0.get('code')} tue_par_le_scenario={r0.get('killed')} apres "
        f"{r0.get('elapsed')} s (attendu : fin d'elle-meme, exit 3). Un "
        f"scenario bloque fige toute la suite : la CI attend jusqu'a sa propre "
        f"limite et tue le job sans verdict ni nom du coupable. Fin de "
        f"sortie : {_tail(r0.get('out', ''))}",
    ))
    out.append((
        "r0_timeout_names_the_hung_scenario_with_its_stack",
        f"[headless] TIMEOUT {_FX}b_hang" in r0.get("out", "")
        and f"{_FX}b_hang.py" in r0.get("out", "")
        and "the scenario's thread" in r0.get("out", ""),
        f"pas de ligne TIMEOUT nommant {_FX}b_hang avec sa pile : celui qui "
        f"lit le rapport ne sait ni quel scenario s'est bloque, ni ou",
    ))
    out.append((
        "r0_verdicts_before_the_hang_are_kept",
        o0.get(_FX + "a_pass", ("",))[0] == "PASS",
        f"issue de {_FX}a_pass={o0.get(_FX + 'a_pass')} : les verdicts deja "
        f"acquis avant le blocage sont perdus avec le processus",
    ))
    out.append((
        "r0_scenarios_after_the_hang_are_reported_not_run",
        f"  ~ {_FX}f_pass" in r0.get("out", "")
        and f"start scenario={_FX}f_pass" not in r0.get("out", ""),
        f"{_FX}f_pass n'est pas annonce NON EXECUTE : un scenario jamais lance "
        f"disparait du rapport sans que personne ne le sache",
    ))

    # ===== R1 : --isolate, un enfant par scenario ========================
    r1 = d.get("r1") or {}
    o1 = _outcomes(r1.get("out", ""))
    expected = {"a_pass": "PASS", "b_hang": "TIMEOUT", "c_hang_gil": "TIMEOUT",
                "d_crash": "ERROR", "e_fail": "FAIL", "f_pass": "PASS"}
    got = {k: o1.get(_FX + k, ("absent",))[0] for k in expected}
    out.append((
        "r1_isolate_gives_each_outcome_its_own_verdict",
        got == expected,
        f"issues obtenues={got} attendues={expected} : sans isolation, un "
        f"blocage ou un plantage decide du sort de tous les scenarios "
        f"suivants, et TIMEOUT, ERROR et FAIL se confondent. Fin de sortie : "
        f"{_tail(r1.get('out', ''))}",
    ))
    out.append((
        "r1_scenario_after_hang_and_crash_still_runs",
        got.get("f_pass") == "PASS"
        and f"start scenario={_FX}f_pass" in r1.get("out", ""),
        f"{_FX}f_pass (place apres le blocage et le plantage) : "
        f"{got.get('f_pass')} ; il doit tourner et passer",
    ))
    out.append((
        "r1_gil_hang_is_killed_by_the_parent",
        "killed by the parent" in o1.get(_FX + "c_hang_gil", ("", ""))[1],
        f"detail de {_FX}c_hang_gil={o1.get(_FX + 'c_hang_gil')} : un code "
        f"natif qui tient le GIL empeche tout chien de garde interne ; seul "
        f"le parent peut l'arreter, et il doit le dire",
    ))
    out.append((
        "r1_crash_is_an_error_with_its_exit_code",
        "code 7" in o1.get(_FX + "d_crash", ("", ""))[1],
        f"detail de {_FX}d_crash={o1.get(_FX + 'd_crash')} : un plantage dur "
        f"doit etre un ERROR du scenario, avec son code, pas la fin de la "
        f"suite",
    ))
    out.append((
        "r1_exit_code_is_timeout",
        r1.get("code") == 3 and r1.get("killed") is False,
        f"code={r1.get('code')} tue={r1.get('killed')} : attendu 3 (au moins "
        f"un scenario bloque ; 3 prime sur 1)",
    ))

    # ===== R2 : ordre aleatoire reproductible ============================
    r2 = d.get("r2") or {}
    starts = re.findall(r"\[validation\] start scenario=(" + _FX + r"\w+)",
                        r2.get("out", ""))
    names = sorted({_FX + f"o{i}" for i in range(1, 5)} | {_FX + "o5_flaky"})

    def _dry(tag):
        return [re.sub(r"#\d+$", "", n) for n in re.findall(
            r"^\[headless\] order \d+: (\S+)$", (d.get(tag) or {}).get("out", ""),
            re.M)]

    same, other, default = _dry("r2_dry_same"), _dry("r2_dry_other"), \
        _dry("r2_dry_default")
    out.append((
        "r2_seed_is_printed",
        "[headless] shuffle seed=12345" in r2.get("out", ""),
        "la graine n'est pas imprimee : un echec lie a l'ordre, vu une fois, "
        "ne se reproduit jamais",
    ))
    out.append((
        "r2_repeat_runs_each_scenario_n_times",
        len(starts) == 10 and all(starts.count(n) == 2 for n in names),
        f"demarrages observes={starts} : --repeat 2 doit executer chacun des "
        f"5 scenarios exactement 2 fois",
    ))
    out.append((
        "r2_same_seed_gives_the_executed_order",
        bool(starts) and same == starts,
        f"ordre execute={starts} ordre annonce pour la meme graine={same} : "
        f"rejouer la graine imprimee doit redonner exactement le meme ordre",
    ))
    out.append((
        "r2_other_seed_gives_another_order",
        bool(same) and bool(other) and same != other
        and sorted(same) == sorted(other),
        f"graine 12345={same} graine 777={other} : deux graines doivent "
        f"donner deux ordres differents du meme ensemble",
    ))
    out.append((
        "r2_default_order_unchanged",
        default == names,
        f"ordre sans --shuffle={default} attendu={names} : l'ordre des "
        f"cibles, inchange (tri par nom quand aucune cible n'est donnee)",
    ))
    out.append((
        "r2_unstable_scenario_is_named",
        f"  ~ {_FX}o5_flaky: FAIL x1, PASS x1" in r2.get("out", "")
        and r2.get("code") == 1,
        f"code={r2.get('code')} ; un scenario qui passe puis echoue entre "
        f"deux passes doit etre nomme UNSTABLE (et l'echec compter : exit 1). "
        f"Fin de sortie : {_tail(r2.get('out', ''))}",
    ))

    # ===== R3 / R4 : fuites ==============================================
    r3 = d.get("r3") or {}
    leaks = dict(re.findall(r"^\[headless\] LEAK (\S+): (.*)$",
                            r3.get("out", ""), re.M))
    leak_a = leaks.get(_FX + "leak_a", "")
    wanted = ("RecoverLand-FixtureLeak", "zz_suiteiso_leaked_layer",
              "leak_journal.sqlite", "seg-00000001.jsonl")
    out.append((
        "r3_leak_is_reported_under_the_scenario_that_caused_it",
        all(w in leak_a for w in wanted),
        f"ligne LEAK de {_FX}leak_a={leak_a!r} ; attendu : thread, couche, "
        f"connexion SQLite et segment d'outbox {wanted}. Sans elle, le thread "
        f"d'ecriture et le journal ouverts par un scenario vivent pendant "
        f"tous les suivants sans que personne ne le voie",
    ))
    out.append((
        "r3_product_thread_is_named_as_such",
        "product thread 'RecoverLand-FixtureLeak'" in leak_a,
        f"ligne LEAK de {_FX}leak_a={leak_a!r} : un thread RecoverLand-* laisse "
        f"vivant doit etre designe comme thread du produit, sinon le lecteur "
        f"ne sait pas s'il cherche dans le plugin ou dans le scenario",
    ))
    out.append((
        "r3_leak_not_blamed_on_the_next_scenario",
        (_FX + "leak_b_clean") not in leaks
        and f"start scenario={_FX}leak_b_clean" in r3.get("out", ""),
        f"fuites attribuees={sorted(leaks)} : {_FX}leak_b_clean, qui tourne "
        f"apres, ne doit pas porter la fuite de son voisin",
    ))
    out.append((
        "r3_teardown_is_called_and_releases",
        d.get("teardown_called") is True
        and (_FX + "leak_c_teardown") not in leaks,
        f"teardown appele={d.get('teardown_called')} fuites={sorted(leaks)} : "
        f"un scenario doit pouvoir liberer ce qu'il demarre",
    ))
    out.append((
        "r3_leak_is_not_a_failure_by_default",
        r3.get("code") == 0,
        f"code={r3.get('code')} : une fuite est signalee, elle ne fait pas "
        f"echouer la suite sans --strict-leaks. Fin de sortie : "
        f"{_tail(r3.get('out', ''))}",
    ))
    r4 = d.get("r4") or {}
    out.append((
        "r4_strict_leaks_fails_the_suite",
        r4.get("code") == 1
        and "counted as failures: --strict-leaks" in r4.get("out", ""),
        f"code={r4.get('code')} : avec --strict-leaks la meme fuite doit "
        f"faire echouer la suite (exit 1)",
    ))
    o4 = _outcomes(r4.get("out", ""))
    out.append((
        "r4_strict_leak_progress_line_is_not_pass",
        o4.get(_FX + "leak_a", ("absent",))[0] == "LEAK",
        f"ligne de progression de {_FX}leak_a sous --strict-leaks="
        f"{o4.get(_FX + 'leak_a')} : si elle dit PASS alors que la suite "
        f"echoue a cause de lui, celui qui suit le journal en direct ne voit "
        f"que des lignes vertes et ne sait pas quel scenario a fait echouer "
        f"le lancement",
    ))

    # ===== R5 : ci_run ===================================================
    r5 = d.get("r5") or {}
    out.append((
        "r5_ci_run_hang_is_bounded",
        r5.get("killed") is False and r5.get("code") == 3
        and f"TIMEOUT {_FX}ci_hang" in r5.get("out", "")
        and f"=== CI scenario: {_FX}ci_pass ===" in r5.get("out", ""),
        f"ci_run : code={r5.get('code')} tue={r5.get('killed')} apres "
        f"{r5.get('elapsed')} s ; un scenario CI bloque doit finir en TIMEOUT "
        f"nomme (exit 3), pas tenir le job jusqu'a sa limite. Fin de sortie : "
        f"{_tail(r5.get('out', ''))}",
    ))
    r5b = d.get("r5b") or {}
    hung = "recoverland.scripts.validation.scenarios.rv_snapshot_asof"
    out.append((
        "r5b_ci_run_default_list_hang_is_bounded",
        r5b.get("killed") is False and r5b.get("code") == 3
        and f"TIMEOUT {hung}" in r5b.get("out", ""),
        f"ci_run sur sa liste par defaut, sans option (delai par "
        f"RECOVERLAND_SCENARIO_TIMEOUT=4) : code={r5b.get('code')} "
        f"tue={r5b.get('killed')} apres {r5b.get('elapsed')} s ; le job CI "
        f"reel lance exactement cette commande, et un scenario bloque le "
        f"tenait jusqu'a sa limite, sans nom ni pile. Fin de sortie : "
        f"{_tail(r5b.get('out', ''))}",
    ))
    return out


if __name__ == "__main__":
    try:
        from scripts.validation.runner import run_scenario
    except ImportError:
        sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
        from scripts.validation.runner import run_scenario
    run_scenario(__file__)
