# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Headless runner for the setup/run/assertions scenarios (Windows + Linux).

`ci_run.py` drives the two scenarios that expose the ``run() -> dict``
contract. Everything under ``scenarios/`` uses the other contract
(``setup(ctx) / run(ctx) / assertions(ctx)`` consumed by ``runner.py``) and
had no headless entry point at all: the scenarios could only be run one by
one from the QGIS Python console, which is why several of them rotted
unnoticed. This module runs any number of them in one go.

Exit codes, kept distinct on purpose
------------------------------------
0  every scenario this runner can drive passed, and the process that ran
   them ended the way that verdict says
1  at least one scenario RAN and failed (assertion) or crashed (error); or
   the process running the scenarios died (os._exit, a native crash,
   abort()) during a scenario, between two, or while closing QGIS or
   Python; or something was left alive under ``--strict-leaks``
2  nothing was run -- unknown scenario name, no scenario found, a bad
   option, only scenarios this runner cannot drive, or QGIS never started
3  at least one scenario gave no verdict within its timeout (it hung), or
   the process hung while closing QGIS or Python

Conflating 2 with 1 is what makes a test harness untrustworthy: a typo in an
argument, or a contract this runner cannot drive, then reads exactly like a
regression in the code under test. Scenarios on the ``run() -> dict``
contract are reported as NOT RUN -- never as PASS, since unproven is not
proven, and never as FAIL, since they were never executed.

3 outranks 1: a suite with a hung scenario is incomplete (in the shared
process, every scenario after the hung one is NOT RUN), and a hang is itself
the gravest outcome for a QGIS plugin, a frozen application. The failures of
the same run are still listed.

Why a child process, always
---------------------------
CI reads the verdict in the exit code, and a process cannot report its own
sudden death. A scenario, or a product path it calls, that ended the process
with ``os._exit(0)`` used to leave a green suite with no report, and every
later scenario (failing ones included) never ran; a crash while closing
QGIS, after every verdict, ended the run with a code outside this contract
and no word on what happened. So the scenarios always run in a child
interpreter that writes an event journal as it goes (which scenario started,
which verdict it reached, which phase it is in, the code it is about to
leave with), and this process decides. The child's verdict stands only if
the child reached it AND then left with exactly that code; its final verdict
line ("ALL GREEN", "exit 3: ...") is printed by this process, once that is
known, never by the child before it. Otherwise the
scenario that was running is named (ERROR, or TIMEOUT when the child had to
be killed), every scenario after it is listed NOT RUN, and a death or a hang
while closing QGIS or Python is named as such. The child also leaves through
normal interpreter shutdown, so the crashes that happen there happen under
test instead of being skipped by ``os._exit``. The cost is one extra Python
start (not a QGIS one) per run.

Suite isolation
---------------
A shared QGIS is the realistic case (QGIS runs everything in one process),
and the default: every scenario runs in ONE child. Three things keep one
scenario from deciding another's fate:

``--timeout S`` (default 300, or RECOVERLAND_SCENARIO_TIMEOUT; 0 disables)
    bounds every scenario. In the shared child, a hang is reported as
    TIMEOUT with the stack of every thread, then the run stops with exit 3:
    a hung scenario still owns its thread, its locks and its journal, and
    nothing run after it could be trusted. When native code holds the GIL,
    the child's own watchdog cannot run: faulthandler dumps every stack,
    then this process kills the child, and it is still a TIMEOUT.
``--isolate``
    runs each scenario in its own child interpreter; the parent kills a
    child that overruns and carries on. A crash of the child (segfault,
    os._exit), before or after its verdict, is an ERROR of that scenario
    instead of the end of the suite.
``--isolate-grace S`` (default 120)
    how long a child may go without progress outside a scenario (QGIS
    start-up, leak check, QGIS and Python shutdown) before it is killed;
    half of it, at most 30 s, is what the native stack dump of a GIL-held
    hang gets after the timeout. Applies to both modes. Must be > 0; the
    durations (``--timeout``, ``--leak-grace``: >= 0) are finite numbers,
    anything else is refused with exit 2.
``--shuffle SEED`` / ``--repeat N``
    random but reproducible order (``--shuffle random`` draws a seed; the
    seed is always printed), N passes over the list. A scenario whose
    outcome differs between passes is listed as UNSTABLE. What each one
    detects: ``--repeat`` catches a scenario that is flaky or fails when
    run again in the same process (from pass 2 on, every scenario has
    already run once, so it only sees order dependence in one direction);
    dependence on a NEIGHBOUR is found by comparing two ``--shuffle``
    seeds, or the default order against one seed.
leak detection (``--leak-grace S``, ``--strict-leaks``, ``--no-leak-check``)
    after each scenario: threads still alive (the product's are named
    RecoverLand-*), running QThreads and QgsTasks, map layers left in the
    project, open sqlite connections and open files, including those that
    only the garbage collector closed. Reported per scenario as a LEAK
    line; a failure only with ``--strict-leaks``. At the end, a non-daemon
    thread that would keep the process alive for ever is listed before the
    verdict, whoever started it (a failure under ``--strict-leaks`` too).
``--dry-run``
    prints the resolved run order and exits 0 without starting QGIS.

Why the QGIS_CUSTOM_CONFIG_PATH dance
-------------------------------------
`core.logger` resolves its log file through ``QgsApplication.qgisSettingsDirPath()``
at IMPORT time, while `runner.py` reads its log window from the conventional
profile location. Without an initialised QgsApplication the logger falls
back to the plugin directory, the two paths diverge, and every log-based
assertion fails with ``n=0`` even though the code under test behaved
perfectly. So: set the config path, build QgsApplication, and only THEN
import anything from the plugin.

QGIS appends ``profiles/<name>`` to QGIS_CUSTOM_CONFIG_PATH itself, so the
variable must point at the config ROOT (``.../QGIS/QGIS4``), not at the
profile directory.

Usage (from the plugin root, with a QGIS python)::

    python-qgis.bat scripts/validation/headless_run.py                    # all
    python-qgis.bat scripts/validation/headless_run.py scenarios/rw_*.py  # subset
    python-qgis.bat scripts/validation/headless_run.py --isolate --timeout 120
    python-qgis.bat scripts/validation/headless_run.py --shuffle random --repeat 2

On Linux/CI::

    xvfb-run -a python3 scripts/validation/headless_run.py
"""
from __future__ import annotations

import json
import math
import os
import random
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PLUGIN_ROOT = _HERE.parents[1]
_SCENARIOS_DIR = _HERE / "scenarios"

try:
    from . import suite_isolation as _iso
except ImportError:  # run as a script: this directory is sys.path[0]
    if str(_HERE) not in sys.path:
        sys.path.insert(0, str(_HERE))
    import suite_isolation as _iso  # type: ignore[no-redef]

_DEFAULT_TIMEOUT_S = 300.0
_TIMEOUT_ENV = "RECOVERLAND_SCENARIO_TIMEOUT"
# After the native stack dump of a GIL-held hang, the time its lines get to
# reach the parent before the child is killed.
_KILL_SLACK_S = 10.0

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_NOTHING_RUN = 2
EXIT_TIMEOUT = 3

_RAN = ("PASS", "FAIL", "ERROR", "TIMEOUT")

# The event journal of this process when it is a supervised child.
_JOURNAL = None
# The final verdict line(s) of a supervised child's report. Never printed by
# the child: handed to the parent in the "done" event, and printed by the
# parent once the child has left with the code they announce. Printed first,
# a green verdict stood in the output even when the child then crashed in
# Python shutdown ("ALL GREEN", then "SUITE PROCESS ERROR").
_VERDICT: list = []


def _resolve_config_root() -> str:
    """Config ROOT holding ``profiles/<name>``: the plugin lives under it.

    Layout: <config_root>/profiles/<profile>/python/plugins/<plugin>.
    Walk up rather than count levels, so a non-standard checkout that is
    simply not under a profile yields "" instead of a wrong path (QGIS then
    keeps its own default and only the log-based assertions degrade).
    """
    env = os.environ.get("QGIS_CUSTOM_CONFIG_PATH")
    if env:
        return env
    for parent in _PLUGIN_ROOT.parents:
        if parent.name == "profiles" and parent.parent != parent:
            return str(parent.parent)
    return ""


def _collect(argv) -> tuple:
    """Resolve CLI targets to scenario files.

    Returns ``(paths, unresolved)``. Keeping the two apart matters: an
    unresolved target means the suite did NOT run, which is a different
    verdict from a scenario that ran and failed. Folding the first into the
    second -- as this function used to, by appending a non-existent path and
    letting run_scenario raise FileNotFoundError -- makes a typo in an
    argument look exactly like a regression in the code under test.

    A bare scenario id (``rw_apply_perf``) resolves against scenarios/, which
    is how scenarios are named everywhere else (SCENARIO_ID, reports, CI
    lists); requiring ``scenarios/rw_apply_perf.py`` from the plugin root was
    a trap, since the same word works in the QGIS console.
    """
    if not argv:
        return (sorted(p for p in _SCENARIOS_DIR.glob("*.py")
                       if not p.name.startswith("_")), [])
    out = []
    unresolved = []
    for target in argv:
        candidates = []
        raw = Path(target)
        if raw.is_absolute():
            candidates.append(raw)
        else:
            candidates.append(Path.cwd() / target)
            candidates.append(_PLUGIN_ROOT / target)
            if not target.endswith(".py"):
                candidates.append(_SCENARIOS_DIR / f"{target}.py")
                candidates.append(_SCENARIOS_DIR / target)
        for cand in candidates:
            if cand.is_dir():
                out.extend(sorted(p for p in cand.glob("*.py")
                                  if not p.name.startswith("_")))
                break
            if cand.is_file():
                out.append(cand.resolve())
                break
        else:
            unresolved.append(target)
    return (out, unresolved)


def _contract_mismatch(module) -> str:
    """Return why ``module`` is not this runner's contract, or "" if it is.

    Two scenario contracts coexist under scenarios/: ``setup(ctx)/run(ctx)/
    assertions(ctx)`` driven by runner.py, and ``run() -> dict`` driven by
    ci_run.py. This runner can only drive the first. Detecting the second
    BEFORE running it is what keeps the report honest: those scenarios used
    to be executed anyway, raise ``TypeError: run() takes 0 positional
    arguments``, and land in the FAILURES list -- which inflated 5 real
    failures into 11 and made every reader believe six subsystems were
    broken when in truth they had never been run here at all.

    Reported as "not run", never as PASS: a scenario this runner cannot drive
    is unproven, and claiming otherwise would be the same lie in the other
    direction.
    """
    import inspect

    run = getattr(module, "run", None)
    if not callable(run):
        return "no run() at all"
    if not callable(getattr(module, "assertions", None)):
        return "no assertions(ctx) -- ci_run.py contract"
    try:
        params = [
            p for p in inspect.signature(run).parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    except (TypeError, ValueError):  # builtins / C callables: let it run
        return ""
    if not params:
        return "run() takes no ctx -- ci_run.py contract"
    return ""


# --------------------------------------------------------------------- #
# Options and run plan
# --------------------------------------------------------------------- #

def _default_timeout() -> float:
    """The timeout from the environment, or the default.

    A value that is not a finite number >= 0 is ignored with a warning,
    and the default applies: clamping a negative value to 0 used to turn
    the limit OFF (0 disables it), so a typo removed the protection
    against a hung scenario without a word.
    """
    raw = os.environ.get(_TIMEOUT_ENV, "").strip()
    if not raw:
        return _DEFAULT_TIMEOUT_S
    try:
        value = float(raw)
    except ValueError:
        value = None
    if value is None or not math.isfinite(value) or value < 0:
        print(f"[headless] ignoring {_TIMEOUT_ENV}={raw!r}: not a finite "
              f"number of seconds >= 0; using {_DEFAULT_TIMEOUT_S:g} s",
              file=sys.stderr)
        return _DEFAULT_TIMEOUT_S
    return value


def _check_seconds(parser, flag: str, value, zero_ok: bool) -> None:
    """Refuse (exit 2) a duration option that cannot mean what it says.

    ``--isolate-grace 0`` killed every child at its first progress check
    and reported a hang; ``--timeout -1`` was clamped to 0, i.e. NO limit;
    ``nan`` compared false everywhere, ``inf`` overflowed the watchdog.
    Each read like a verdict on the code under test.
    """
    if value is None:
        return
    if not math.isfinite(value) or value < 0 or (value == 0 and not zero_ok):
        bound = ">= 0" if zero_ok else "> 0"
        extra = " (0 disables the limit)" if flag == "--timeout" else ""
        parser.error(f"{flag} takes a finite number of seconds {bound}"
                     f"{extra}, not {value:g}")


def _parse_args(argv):
    import argparse

    parser = argparse.ArgumentParser(
        prog="headless_run.py",
        description="Run setup/run/assertions scenarios headless. Exit codes: "
                    "0 pass, 1 fail, 2 nothing run, 3 timeout.")
    parser.add_argument("targets", nargs="*",
                        help="scenario ids, files or directories (default: all)")
    parser.add_argument("--timeout", type=float, default=None, metavar="S",
                        help=f"per-scenario timeout in seconds (default "
                             f"{_DEFAULT_TIMEOUT_S:g}, or ${_TIMEOUT_ENV}; "
                             f"0 disables)")
    parser.add_argument("--isolate", action="store_true",
                        help="run each scenario in its own child interpreter")
    parser.add_argument("--isolate-grace", type=float, default=120.0, metavar="S",
                        help="time a child may go without progress outside a "
                             "scenario (QGIS start-up, leak check, QGIS and "
                             "Python shutdown) before it is killed "
                             "(default 120)")
    parser.add_argument("--shuffle", metavar="SEED",
                        help="shuffle the order with this integer seed, or "
                             "'random' to draw one (always printed)")
    parser.add_argument("--repeat", type=int, default=1, metavar="N",
                        help="run the list N times (reshuffled each pass "
                             "under --shuffle)")
    parser.add_argument("--strict-leaks", action="store_true",
                        help="a scenario that leaves something alive fails")
    parser.add_argument("--leak-grace", type=float, default=1.0, metavar="S",
                        help="time new threads/tasks get to finish before "
                             "they count as leaked (default 1)")
    parser.add_argument("--no-leak-check", action="store_true",
                        help="do not look for leaks")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the run order and exit without running")
    # The supervised child: where it writes its events, what it is (one
    # scenario for --isolate, or the whole shared run) and its exact plan.
    parser.add_argument("--child-journal", metavar="JSONL",
                        help=argparse.SUPPRESS)
    parser.add_argument("--child-role", choices=("scenario", "suite"),
                        default="suite", help=argparse.SUPPRESS)
    parser.add_argument("--child-plan", metavar="JSON",
                        help=argparse.SUPPRESS)
    parser.add_argument("--child-sys-path", metavar="PATHS",
                        help=argparse.SUPPRESS)
    opts = parser.parse_intermixed_args(argv)
    _check_seconds(parser, "--timeout", opts.timeout, zero_ok=True)
    _check_seconds(parser, "--isolate-grace", opts.isolate_grace, zero_ok=False)
    _check_seconds(parser, "--leak-grace", opts.leak_grace, zero_ok=True)
    if opts.timeout is None:
        opts.timeout = _default_timeout()
    if opts.repeat < 1:
        parser.error("--repeat must be >= 1")
    if opts.child_journal and not opts.child_plan:
        parser.error("--child-journal needs --child-plan")
    opts.seed = None
    if opts.shuffle is not None:
        if opts.shuffle.lower() == "random":
            opts.seed = random.SystemRandom().randrange(1, 2 ** 31)
        else:
            try:
                opts.seed = int(opts.shuffle)
            except ValueError:
                parser.error(f"--shuffle takes an integer seed or 'random', "
                             f"not {opts.shuffle!r}")
    return opts


def _plan(scenarios, seed, repeat) -> list:
    """``[(pass_number, path), ...]`` in run order.

    One generator seeded once: pass k's order is a function of the seed and
    k, so the whole run is reproduced from the one printed number.
    """
    rng = random.Random(seed) if seed is not None else None
    plan = []
    for it in range(1, repeat + 1):
        order = list(scenarios)
        if rng is not None:
            rng.shuffle(order)
        plan.extend((it, path) for path in order)
    return plan


def _label(rec, repeat) -> str:
    return rec["name"] if repeat <= 1 else f"{rec['name']}#{rec['iter']}"


def _record(path, it, outcome, detail="", **extra) -> dict:
    rec = {"name": Path(path).stem, "path": str(path), "iter": it,
           "outcome": outcome, "detail": detail, "leaks": [],
           "elapsed_s": 0.0}
    rec.update(extra)
    return rec


def _print_config(opts, plan, n_scenarios) -> None:
    if opts.seed is not None:
        order = (f"shuffled (reproduce with --shuffle {opts.seed}, the same "
                 f"targets and --repeat {opts.repeat})")
    elif opts.targets:
        # _collect keeps the targets in the order they were given (a
        # directory's scenarios by name): announcing "sorted by name" here
        # would misstate the order the reader is asked to reproduce.
        order = "default (the order the targets were given in)"
    else:
        order = "default (sorted by name)"
    mode = ("one child process per scenario" if opts.isolate
            else "in-process (one shared QGIS, in one supervised child)")
    timeout = f"{opts.timeout:g} s" if opts.timeout > 0 else "none"
    leaks = ("off" if opts.no_leak_check
             else ("strict (a leak fails)" if opts.strict_leaks
                   else "on (reported, not a failure)"))
    print(f"[headless] shuffle seed={opts.seed}" if opts.seed is not None
          else "[headless] shuffle off")
    print(f"[headless] plan: {n_scenarios} scenario(s) x {opts.repeat} "
          f"pass(es) = {len(plan)} run(s); order {order}; {mode}; "
          f"timeout per scenario {timeout}; leak check {leaks}")


# --------------------------------------------------------------------- #
# In-process execution (always inside a supervised child)
# --------------------------------------------------------------------- #

def _run_one(path, it, load, run_scenario) -> dict:
    """Load and run one scenario in this process; never raises (but ^C)."""
    import traceback

    try:
        module = load(str(path))
    except AttributeError as exc:
        # A THIRD shape exists: a top-level script that runs its own
        # checks at import and prints its own verdict (i18n_runtime_qgis
        # does, 36/36, driven from the QGIS console by exec(compile(..))).
        # runner._load_module rejects it for having no run(); that is not
        # a defect, so do not call it one. Its own output stands in the
        # log next to ours.
        if "has no run" in str(exc):
            return _record(path, it, "SKIP",
                           "self-reporting script -- prints its own "
                           "verdict at import, see [i18n-runtime]-style "
                           "lines above")
        traceback.print_exc()
        return _record(path, it, "ERROR", f"load failed: {exc!r}")
    except SystemExit as exc:
        traceback.print_exc()
        return _record(path, it, "ERROR",
                       f"load called sys.exit({exc.code!r}): it would have "
                       f"ended the whole suite")
    except Exception as exc:  # noqa: BLE001 - unloadable is a real defect
        traceback.print_exc()
        return _record(path, it, "ERROR", f"load failed: {exc!r}")
    reason = _contract_mismatch(module)
    if reason:
        return _record(path, it, "SKIP", reason)
    try:
        verdict = run_scenario(module)
    except SystemExit as exc:
        traceback.print_exc()
        return _record(path, it, "ERROR",
                       f"called sys.exit({exc.code!r}): it would have ended "
                       f"the whole suite")
    except Exception as exc:  # noqa: BLE001 - a broken scenario is a defect
        traceback.print_exc()
        return _record(path, it, "ERROR", repr(exc))
    if verdict.get("verdict") != "PASS":
        return _record(path, it, "FAIL", f"{verdict.get('failed')} assertion(s)",
                       failed=verdict.get("failed"))
    return _record(path, it, "PASS")


def _print_progress(rec, idx, total, repeat, strict_leaks=False) -> None:
    label = _label(rec, repeat)
    shown = rec["outcome"]
    detail = (f": {rec['detail']}"
              if rec["outcome"] != "PASS" and rec["detail"] else "")
    if strict_leaks and shown == "PASS" and rec.get("leaks"):
        # The record stays PASS (its assertions held); the line must not,
        # or it would read PASS for a scenario that fails the run.
        shown = "LEAK"
        detail = (": its assertions passed but it left something alive, "
                  "a failure under --strict-leaks")
    print(f"[headless] [{idx}/{total}] {label} -> {shown} "
          f"({rec['elapsed_s']:.1f} s){detail}")
    if rec.get("leaks"):
        print(f"[headless] LEAK {label}: {len(rec['leaks'])} left alive: "
              + "; ".join(rec["leaks"]))
    sys.stdout.flush()


def _native_grace(opts) -> float:
    """Time after the timeout at which faulthandler dumps a GIL-held hang."""
    return min(30.0, max(1.0, opts.isolate_grace / 2))


def _run_in_process(plan, opts, journal) -> list:
    """Run the plan in this process, one shared QgsApplication.

    Every step is written to ``journal`` before and after it happens, so
    the supervising parent knows where this process was if it dies.
    """
    quiet = opts.child_role == "scenario"  # the --isolate parent prints
    journal.event("phase", phase="qgis-init")
    from qgis.core import QgsApplication

    # A hard crash (access violation, abort() from a qFatal) takes the whole
    # process with it; the parent then names the scenario from the journal.
    # The least owed to the reader on top of that is the stack of every
    # thread at the moment of the crash.
    try:
        import faulthandler
        if sys.__stderr__ is not None:
            faulthandler.enable(file=sys.__stderr__, all_threads=True)
    except Exception:  # noqa: BLE001 - no usable fd: nothing to enable
        pass

    prefix = os.environ.get("QGIS_PREFIX_PATH")
    if prefix:
        QgsApplication.setPrefixPath(prefix, True)
    app = QgsApplication([], False)
    app.initQgis()
    print(f"[headless] settings_dir={QgsApplication.qgisSettingsDirPath()}")
    journal.event("phase", phase="scenarios")

    from scripts.validation.runner import _load_module, run_scenario

    records = []
    state = {"idx": -1}

    def _on_expire(_watch_label, elapsed, hung_ident):
        # Runs on the watchdog thread while the scenario still holds the
        # main thread: report, then leave. Nothing else can be run safely.
        idx = state["idx"]
        it, path = plan[idx]
        rec = _record(path, it, "TIMEOUT",
                      f"no verdict after {elapsed:.1f} s (limit "
                      f"{opts.timeout:g} s)", elapsed_s=round(elapsed, 3))
        journal.event("record", idx=idx, rec=rec)
        label = _label(rec, opts.repeat)
        print(f"\n[headless] TIMEOUT {label}: no verdict after {elapsed:.1f} s "
              f"(limit {opts.timeout:g} s). Stack of every thread follows; "
              f"then this process stops, exit {EXIT_TIMEOUT}.")
        try:
            _iso.dump_all_stacks(sys.stdout, hung_ident)
        except Exception as exc:  # noqa: BLE001
            print(f"[headless] stack dump failed: {exc!r}")
        aborted = [_record(p, i, "ABORTED",
                           f"not run: the suite stopped when {label} timed out")
                   for (i, p) in plan[idx + 1:]]
        final = records + [rec] + aborted
        code = EXIT_TIMEOUT
        try:
            if not quiet:
                journal.event("phase", phase="report")
                code = _report(final, opts, verdict=_VERDICT)
        except Exception as exc:  # noqa: BLE001 - leaving matters more
            print(f"[headless] report after timeout failed: {exc!r}")
        try:
            journal.event("done", code=code, blockers=[
                t.name for t in _iso.non_daemon_threads()],
                verdict=list(_VERDICT))
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os._exit(code)

    # The parent kills a GIL-held hang at timeout + native grace + slack:
    # the native dump comes first, and only dumps (the parent decides).
    watchdog = _iso.ScenarioWatchdog(
        opts.timeout, _on_expire, native_exit=False,
        native_grace_s=_native_grace(opts))
    try:
        for idx, (it, path) in enumerate(plan):
            before = None
            if not opts.no_leak_check:
                before = _iso.ResourceSnapshot.baseline()
            state["idx"] = idx
            journal.event("start", idx=idx, name=Path(path).stem, iter=it)
            t0 = time.monotonic()
            watchdog.arm(str(idx))
            try:
                rec = _run_one(path, it, _load_module, run_scenario)
            finally:
                watchdog.disarm()
            rec["elapsed_s"] = round(time.monotonic() - t0, 3)
            journal.event("ran", idx=idx, outcome=rec["outcome"])
            if before is not None:
                try:
                    rec["leaks"] = _iso.measure_leaks(before, opts.leak_grace)
                except Exception as exc:  # noqa: BLE001
                    rec["leaks"] = [f"leak check failed: {exc!r}"]
            records.append(rec)
            journal.event("record", idx=idx, rec=rec)
            if not quiet:
                _print_progress(rec, idx + 1, len(plan), opts.repeat,
                                opts.strict_leaks)
    finally:
        journal.event("phase", phase="exitQgis")
        try:
            app.exitQgis()
        except Exception:  # noqa: BLE001
            pass
    return records


# --------------------------------------------------------------------- #
# Supervision: the parent side
# --------------------------------------------------------------------- #

def _child_cmd(role, plan, opts, journal) -> list:
    """Command line of a supervised child running exactly ``plan``.

    The child inherits this environment untouched: the scenarios, and the
    processes they start themselves, must see what they saw when the suite
    ran in this very process. Only the child's own streams change (``-u``
    here, UTF-8 in `_child_main`), so its lines reach the parent live.
    """
    plan_file = f"{journal}.plan.json"
    with open(plan_file, "w", encoding="utf-8") as fh:
        json.dump([[it, str(path)] for (it, path) in plan], fh)
    cmd = [_iso.python_executable(), "-u", str(Path(__file__).resolve()),
           "--child-journal", journal, "--child-role", role,
           "--child-plan", plan_file,
           "--child-sys-path",
           os.pathsep.join(p for p in sys.path if isinstance(p, str) and p),
           "--timeout", f"{opts.timeout:g}",
           "--leak-grace", f"{opts.leak_grace:g}",
           "--isolate-grace", f"{opts.isolate_grace:g}",
           "--repeat", str(opts.repeat)]
    if opts.seed is not None:
        cmd += ["--shuffle", str(opts.seed)]
    if opts.no_leak_check:
        cmd.append("--no-leak-check")
    if opts.strict_leaks:
        cmd.append("--strict-leaks")
    return cmd


def _child_deadline(opts):
    slack = _native_grace(opts) + _KILL_SLACK_S

    def _deadline(state, t_spawn):
        return _iso.phase_deadline(state, t_spawn, opts.timeout, slack,
                                   opts.isolate_grace)
    return _deadline


def _how_it_ended(code, killed, opts, state) -> str:
    pending = state.pending()
    if killed and pending is not None and pending not in state.ran:
        return ("hung in a scenario past its limit and was killed by the "
                "parent")
    if killed:
        return (f"hung (no progress for {opts.isolate_grace:g} s) and was "
                f"killed by the parent")
    return f"ended with code {_iso.describe_returncode(code)}"


def _pending_record(plan, opts, state, code, killed) -> dict:
    """The record of the scenario the child was running when it stopped."""
    idx = state.pending()
    it, path = plan[idx]
    ran = state.ran.get(idx)
    held = state.age(idx)
    if killed and ran:
        return _record(path, it, "TIMEOUT",
                       f"its verdict was {ran}, then the leak check of what "
                       f"it left behind hung; the child was killed by the "
                       f"parent after {held:.1f} s",
                       elapsed_s=round(held, 3))
    if killed:
        slack = _native_grace(opts) + _KILL_SLACK_S
        return _record(path, it, "TIMEOUT",
                       f"child killed by the parent after {held:.1f} s "
                       f"(limit {opts.timeout:g} s + {slack:g} s for the "
                       f"native stack dump): its own watchdog never fired, "
                       f"native code probably held the GIL",
                       elapsed_s=round(held, 3))
    how = _iso.describe_returncode(code)
    if ran:
        return _record(path, it, "ERROR",
                       f"its verdict was {ran}, then the process ended with "
                       f"code {how} while its leftovers were being checked "
                       f"(garbage collection, thread joins): what it left "
                       f"behind takes the process down",
                       elapsed_s=round(held, 3))
    return _record(path, it, "ERROR",
                   f"the process ended with code {how} while this scenario "
                   f"was running, before its verdict (os._exit, a native "
                   f"crash or abort() in the scenario or in the code it "
                   f"calls)", elapsed_s=round(held, 3))


def _run_supervised(plan, opts) -> int:
    """The default mode: every scenario in ONE child, judged from here."""
    import shutil
    import tempfile

    try:
        sys.stdout.reconfigure(errors="backslashreplace")
    except Exception:  # noqa: BLE001
        pass
    work = tempfile.mkdtemp(prefix="rl_headless_")
    try:
        journal = os.path.join(work, "suite.jsonl")
        cmd = _child_cmd("suite", plan, opts, journal)
        code, killed, _elapsed, state = _iso.supervise(
            cmd, journal, _child_deadline(opts))
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return _judge_suite(plan, opts, code, killed, state)


def _judge_suite(plan, opts, code, killed, state) -> int:
    """Exit code of the whole run, from how the shared child ended.

    The child's own report stands only when the child reached it and then
    left with exactly its code. Anything else is rebuilt from the journal:
    the scenario that was running is named, the rest listed NOT RUN.
    """
    done = state.done
    if done is not None and not killed and code == done.get("code"):
        # Only now is the outcome known: the child's verdict stands.
        for line in done.get("verdict") or ():
            print(line)
        return code
    how = _how_it_ended(code, killed, opts, state)
    if done is not None:
        print(f"\n[headless] SUITE PROCESS ERROR: the report above came to "
              f"exit {done.get('code')}, then the process {how} during "
              f"{state.phase_label()}: that verdict does not stand, and is "
              f"not printed")
        if killed:
            print(f"[headless] exit {EXIT_TIMEOUT}: the suite process hung")
            return EXIT_TIMEOUT
        # A hang already reported keeps its code (3 outranks 1).
        return EXIT_TIMEOUT if done.get("code") == EXIT_TIMEOUT else EXIT_FAIL
    print(f"\n[headless] the suite process never reached its own report (it "
          f"{how}); the report below is rebuilt from what it had recorded")
    records = [state.records[i] for i in sorted(state.records)]
    suite_errors = []
    pending = state.pending()
    if pending is not None:
        rec = _pending_record(plan, opts, state, code, killed)
        records.append(rec)
        _print_progress(rec, pending + 1, len(plan), opts.repeat,
                        opts.strict_leaks)
        rest = pending + 1
        why = (f"the suite process {'hung' if killed else 'died'} during "
               f"{_label(rec, opts.repeat)}")
    elif not state.started:
        suite_errors.append(f"the suite process {how} during "
                            f"{state.phase_label()}, before the first "
                            f"scenario")
        rest = 0
        why = "the suite process never reached the first scenario"
    else:
        last = max(state.records)
        last_label = _label(state.records[last], opts.repeat)
        rest = last + 1
        if rest < len(plan):
            suite_errors.append(f"after the verdict and leak check of "
                                f"{last_label}, before the next scenario "
                                f"started, the suite process {how}")
            why = (f"the suite process {'hung' if killed else 'died'} after "
                   f"{last_label}")
        else:
            suite_errors.append(f"every scenario gave its verdict, then the "
                                f"suite process {how} during "
                                f"{state.phase_label()}")
            why = ""
    records += [_record(p, i, "ABORTED", f"not run: {why}")
                for (i, p) in plan[rest:]]
    return _report(records, opts, suite_errors=suite_errors,
                   suite_hung=killed)


# --------------------------------------------------------------------- #
# One child process per scenario
# --------------------------------------------------------------------- #

def _judge_isolated(path, it, code, killed, elapsed, state, opts) -> dict:
    """The record of one --isolate child, from its journal and its exit.

    A verdict on disk is not enough: the child must then leave with the
    code that verdict implies. A crash or a hang in exitQgis or in Python
    shutdown, after PASS, is what a user meets as QGIS crashing, or never
    closing, after the operation.
    """
    rec = state.records.get(0)
    done = state.done
    if rec is None:
        if state.pending() is not None:
            return _pending_record([(it, path)], opts, state, code, killed)
        if killed:
            return _record(path, it, "ERROR",
                           f"never started: the child made no progress for "
                           f"{opts.isolate_grace:g} s during "
                           f"{state.phase_label()} and was killed by the "
                           f"parent after {elapsed:.1f} s")
        return _record(path, it, "ERROR",
                       f"child process exited with code "
                       f"{_iso.describe_returncode(code)} without a verdict "
                       f"(the interpreter or QGIS crashed during "
                       f"{state.phase_label()})")
    rec = dict(rec)
    verdict = rec.get("outcome")
    if killed:
        rec.update(outcome="TIMEOUT", verdict_before_exit=verdict,
                   detail=f"verdict {verdict} written, then the child hung "
                          f"during {state.phase_label()} and was killed by "
                          f"the parent after {elapsed:.1f} s: QGIS would not "
                          f"close")
    elif done is None or code != done.get("code"):
        leaving = ("" if done is None
                   else f"; it was leaving with {done.get('code')}")
        rec.update(outcome="ERROR", verdict_before_exit=verdict,
                   detail=f"verdict {verdict} written, then the child died "
                          f"with code {_iso.describe_returncode(code)} during "
                          f"{state.phase_label()}{leaving}")
    blockers = (done or {}).get("blockers") or []
    if blockers:
        rec["leaks"] = list(rec.get("leaks") or []) + [
            f"at exit: {len(blockers)} non-daemon thread(s) "
            f"{', '.join(repr(b) for b in blockers)} would have kept the "
            f"child alive for ever (it left hard)"]
    return rec


def _run_isolated(plan, opts) -> list:
    import shutil
    import tempfile

    try:
        sys.stdout.reconfigure(errors="backslashreplace")
    except Exception:  # noqa: BLE001
        pass
    work = tempfile.mkdtemp(prefix="rl_headless_")
    records = []
    try:
        for idx, (it, path) in enumerate(plan, 1):
            journal = os.path.join(work, f"j{idx:05d}.jsonl")
            cmd = _child_cmd("scenario", [(it, path)], opts, journal)
            code, killed, elapsed, state = _iso.supervise(
                cmd, journal, _child_deadline(opts), cwd=str(_PLUGIN_ROOT))
            rec = _judge_isolated(path, it, code, killed, elapsed, state, opts)
            rec["iter"] = it
            rec["elapsed_s"] = round(elapsed, 3)
            records.append(rec)
            _print_progress(rec, idx, len(plan), opts.repeat,
                            opts.strict_leaks)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return records


# --------------------------------------------------------------------- #
# Report
# --------------------------------------------------------------------- #

def _say_verdict(line: str, verdict) -> None:
    """Print the final verdict line, or keep it for the supervising parent."""
    if verdict is None:
        print(line)
    else:
        verdict.append(line)


def _report(records, opts, suite_errors=(), suite_hung=False,
            verdict=None) -> int:
    """Print the report and return the exit code.

    The final verdict line goes through ``verdict`` when given (a list the
    supervised child hands to its parent): it is the one line that must
    not appear before the process that produced it has ended as it says.
    """
    rep = opts.repeat
    strict = opts.strict_leaks
    ran = [r for r in records if r["outcome"] in _RAN]
    skipped = [r for r in records if r["outcome"] == "SKIP"]
    aborted = [r for r in records if r["outcome"] == "ABORTED"]
    errors = [r for r in records if r["outcome"] == "ERROR"]
    failures = [r for r in records if r["outcome"] == "FAIL"]
    timeouts = [r for r in records if r["outcome"] == "TIMEOUT"]
    leaky = [r for r in records if r.get("leaks")]
    # This process's own threads that would block its exit, wherever they
    # come from (a scenario, the product, or QGIS/plugin start-up before
    # the first leak baseline): said here, before the verdict line.
    blockers = [] if opts.no_leak_check else _iso.non_daemon_threads()
    n_pass = sum(1 for r in ran if r["outcome"] == "PASS"
                 and not (strict and r.get("leaks")))

    print()
    if opts.seed is not None:
        print(f"[headless] shuffle seed={opts.seed} (reproduce with "
              f"--shuffle {opts.seed})")
    print(f"[headless] {n_pass}/{len(ran)} scenario(s) PASS")
    if skipped:
        seen = {}
        for r in skipped:
            seen.setdefault(r["name"], r["detail"])
        print(f"[headless] {len(seen)} NOT RUN by this runner "
              f"(other contract -- see ci_run.py), so neither PASS nor FAIL:")
        for name, detail in seen.items():
            print(f"  ~ {name}: {detail}")
    if suite_errors:
        print("[headless] SUITE PROCESS ERRORS (the process running the "
              "scenarios did not end the way a verdict needs):")
        for text in suite_errors:
            print(f"  ! {text}")
    if errors:
        print("[headless] ERRORS (ran and crashed):")
        for r in errors:
            print(f"  ! {_label(r, rep)}: {r['detail']}")
    if failures:
        print("[headless] FAILURES (ran, assertions failed):")
        for r in failures:
            print(f"  - {_label(r, rep)}: {r.get('failed')} assertion(s)")
    if timeouts:
        print("[headless] TIMEOUTS (no verdict within the per-scenario "
              "timeout: hung):")
        for r in timeouts:
            print(f"  ! {_label(r, rep)}: {r['detail']}")
    if aborted:
        reason = aborted[0]["detail"].replace("not run: ", "", 1)
        print(f"[headless] {len(aborted)} NOT RUN because the suite stopped "
              f"early ({reason}); --isolate runs each scenario in its own "
              f"process and goes on past a hang or a crash:")
        for r in aborted:
            print(f"  ~ {_label(r, rep)}")
    if leaky or blockers:
        how = ("counted as failures: --strict-leaks" if strict
               else "not a failure; --strict-leaks makes it one")
        print(f"[headless] LEAKS (still alive after the scenario ended; {how}):")
        for r in leaky:
            print(f"  ~ {_label(r, rep)}: " + "; ".join(r["leaks"]))
        if blockers:
            names = ", ".join(f"'{t.name}'" for t in blockers)
            print(f"  ~ (end of run) {len(blockers)} non-daemon thread(s) "
                  f"still alive, they would keep this process alive forever "
                  f"(QGIS would not close): {names}")
    if rep > 1:
        by_name = {}
        for r in records:
            if r["outcome"] in _RAN:
                by_name.setdefault(r["name"], []).append(r["outcome"])
        unstable = {n: o for n, o in by_name.items() if len(set(o)) > 1}
        if unstable:
            print("[headless] UNSTABLE across passes (order or timing "
                  "dependent):")
            for name, outs in unstable.items():
                counts = ", ".join(f"{o} x{outs.count(o)}"
                                   for o in sorted(set(outs)))
                print(f"  ~ {name}: {counts}")

    if not ran:
        _say_verdict("[headless] nothing was run -- exit 2 (could not run), "
                     "not exit 0 (proven)", verdict)
        return EXIT_NOTHING_RUN
    if timeouts or suite_hung:
        _say_verdict(f"[headless] exit {EXIT_TIMEOUT}: at least one scenario, "
                     f"or the suite process, hung", verdict)
        return EXIT_TIMEOUT
    if failures or errors or suite_errors or (strict and (leaky or blockers)):
        return EXIT_FAIL
    note = (" (leaks reported above, not counted: --strict-leaks counts them)"
            if leaky or blockers else "")
    _say_verdict(f"[headless] ALL GREEN{note}", verdict)
    return EXIT_PASS


# --------------------------------------------------------------------- #
# Entry points
# --------------------------------------------------------------------- #

def _child_main(opts) -> int:
    """A supervised child: run the plan it was given, journal every step."""
    global _JOURNAL
    _iso.utf8_streams()
    _JOURNAL = _iso.ChildJournal(opts.child_journal)
    with open(opts.child_plan, encoding="utf-8") as fh:
        plan = [(int(it), Path(path)) for (it, path) in json.load(fh)]
    records = _run_in_process(plan, opts, _JOURNAL)
    if opts.child_role == "scenario":
        # One scenario for a parent running --isolate: the parent prints
        # the plan, the progress and the report; this code must match the
        # verdict or the parent calls the exit a crash.
        outcomes = {r["outcome"] for r in records}
        if "TIMEOUT" in outcomes:
            return EXIT_TIMEOUT
        if outcomes & {"FAIL", "ERROR"}:
            return EXIT_FAIL
        return EXIT_PASS if outcomes & set(_RAN) else EXIT_NOTHING_RUN
    _JOURNAL.event("phase", phase="report")
    return _report(records, opts, verdict=_VERDICT)


def main(argv) -> int:
    opts = _parse_args(argv)

    config_root = _resolve_config_root()
    if config_root:
        os.environ["QGIS_CUSTOM_CONFIG_PATH"] = config_root

    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)

    if opts.child_journal:
        # Import the scenarios exactly as the parent would have: its
        # sys.path first, whatever put entries there (runpy, a console).
        for entry in reversed((opts.child_sys_path or "").split(os.pathsep)):
            if entry and entry not in sys.path:
                sys.path.insert(0, entry)
        return _child_main(opts)

    scenarios, unresolved = _collect(opts.targets)
    if unresolved:
        print(f"[headless] unknown scenario(s): {', '.join(unresolved)}",
              file=sys.stderr)
        print(f"[headless] looked under {_SCENARIOS_DIR}", file=sys.stderr)
        print("[headless] nothing was run -- exit 2 (could not run), "
              "not exit 1 (ran and failed)", file=sys.stderr)
        return EXIT_NOTHING_RUN
    if not scenarios:
        print("[headless] no scenario found", file=sys.stderr)
        return EXIT_NOTHING_RUN

    plan = _plan(scenarios, opts.seed, opts.repeat)
    _print_config(opts, plan, len(scenarios))
    if opts.dry_run:
        for idx, (it, path) in enumerate(plan, 1):
            print(f"[headless] order {idx}: {Path(path).stem}"
                  + (f"#{it}" if opts.repeat > 1 else ""))
        return EXIT_PASS
    sys.stdout.flush()
    if opts.isolate:
        return _report(_run_isolated(plan, opts), opts)
    return _run_supervised(plan, opts)


def _exit(code: int) -> None:
    """Leave with ``code``, through normal interpreter shutdown if possible.

    A supervised child first tells its parent the code it is leaving with:
    a crash in the shutdown that follows (QGIS objects destroyed, atexit
    handlers) then shows as a mismatch instead of passing unseen, which is
    why a child no longer leaves by os._exit when nothing blocks it. The
    interpreter joins every non-daemon thread at exit: one left behind
    would keep this process alive forever after its verdict, so then it
    leaves hard (the report, or the parent, has said so already).
    """
    blockers = _iso.non_daemon_threads()
    if _JOURNAL is not None:
        _JOURNAL.event("done", code=code,
                       blockers=[t.name for t in blockers],
                       verdict=list(_VERDICT))
    if not blockers:
        sys.exit(code)
    names = ", ".join(f"'{t.name}'" for t in blockers)
    print(f"[headless] leaving with exit {code} without waiting for "
          f"{len(blockers)} non-daemon thread(s): {names}")
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    _exit(main(sys.argv[1:]))
