"""Headless CI runner for RecoverLand runtime validation scenarios (RL-E3-02).

Initialises a headless QgsApplication, runs each runtime scenario under
``scripts/validation/scenarios/`` and exits non-zero if ANY scenario does not
return ``verdict == "PASS"``. This is the CI counterpart of running
``scenario.run()`` in the QGIS Python console (charte AGENTS.md §4: preuve =
logs runtime QGIS, pas de mocks pytest).

Usage (inside a QGIS environment / docker image)::

    xvfb-run -a python3 -m recoverland.scripts.validation.ci_run
    xvfb-run -a python3 -m recoverland.scripts.validation.ci_run --timeout 120
    xvfb-run -a python3 -m recoverland.scripts.validation.ci_run <module> ...

The plugin package must be importable as ``recoverland`` (put the parent of the
plugin directory on PYTHONPATH and check it out into a ``recoverland`` folder).

Exit codes (the same as headless_run.py): 0 pass, 1 a scenario failed or
raised, or the process running the scenarios died or kept a non-daemon
thread alive at the end, 2 nothing was run (QGIS could not start), 3 a
scenario gave no verdict within ``--timeout`` seconds (default 300, or
RECOVERLAND_SCENARIO_TIMEOUT; 0 disables) or the process hung while closing.
A hung scenario used to hold the CI job until the job's own limit, with no
verdict and no hint of where it was stuck; it now ends the run with the
stack of every thread.

The scenarios run in ONE child process (the same shared QGIS as before),
watched from this one through an event journal (see suite_isolation). A
scenario, or a product path, that ended the process with ``os._exit(0)``
used to give a green job with no verdict for it or for the scenarios after
it: the exit code is what CI reads, and a dying process cannot report its
own death. This process now names the scenario that was running, lists the
ones that never ran, and exits 1; a child that hangs past its limits is
killed and reported (exit 3). The final verdict ("CI VALIDATION PASSED",
"FAILED", "TIMED OUT") is printed by this process once the child has left
with the code it announces, never by the child before that: a crash in the
child's Python shutdown used to follow a "PASSED" already in the job log.
A --timeout that is not a finite number >= 0 is refused (exit 2).
"""
from __future__ import annotations

import importlib
import math
import os
import sys
import traceback

try:
    from . import suite_isolation as _iso
except ImportError:  # run as a script: this directory is sys.path[0]
    import suite_isolation as _iso  # type: ignore[no-redef]

# Scenarios to run in CI. Each module must expose ``run() -> dict`` with a
# ``verdict`` key ("PASS"/"FAIL"). Add new scenarios here as they land.
_SCENARIOS = (
    # Runs on every image of the CI matrix; this is what makes the Qt5/Qt6
    # split an enforced contract rather than a claim in metadata.txt.
    "recoverland.scripts.validation.scenarios.compat_cross_version",
    "recoverland.scripts.validation.scenarios.rv_snapshot_asof",
    "recoverland.scripts.validation.scenarios.rv_snapshot_volume",
)

_DEFAULT_TIMEOUT_S = 300.0
_TIMEOUT_ENV = "RECOVERLAND_SCENARIO_TIMEOUT"
EXIT_TIMEOUT = 3
# The child's watchdog dumps a GIL-held hang at timeout + native grace; the
# parent kills it _KILL_SLACK_S later. QGIS start-up and shutdown are
# bounded by _IDLE_S without progress.
_NATIVE_GRACE_S = 30.0
_KILL_SLACK_S = 10.0
_IDLE_S = 120.0

# The event journal of this process when it is the supervised child.
_JOURNAL = None
# The child's final verdict ({"stream": ..., "text": ...}): handed to the
# parent in the "done" event and printed by the parent once the child has
# left with the code it announces. Printed by the child, "CI VALIDATION
# PASSED" stood in the job log even when the child then crashed in Python
# shutdown, and "ABORTED" came after it.
_VERDICT: dict = {}


def _init_qgis():
    """Initialise a headless QgsApplication. Returns the app instance."""
    from qgis.core import QgsApplication

    # Prefix path: honour QGIS_PREFIX_PATH when set (docker images do), else
    # fall back to the conventional /usr install.
    prefix = os.environ.get("QGIS_PREFIX_PATH", "/usr")
    QgsApplication.setPrefixPath(prefix, True)
    app = QgsApplication([], False)
    app.initQgis()
    return app


def _parse_args(argv):
    import argparse

    default = _DEFAULT_TIMEOUT_S
    raw = os.environ.get(_TIMEOUT_ENV, "").strip()
    if raw:
        # Clamping a negative value to 0 turned the limit OFF (0 disables
        # it): a value that is not a finite number >= 0 keeps the default.
        try:
            value = float(raw)
        except ValueError:
            value = None
        if value is None or not math.isfinite(value) or value < 0:
            sys.stderr.write(f"ignoring {_TIMEOUT_ENV}={raw!r}: not a finite "
                             f"number of seconds >= 0; using "
                             f"{_DEFAULT_TIMEOUT_S:g} s\n")
        else:
            default = value
    parser = argparse.ArgumentParser(
        prog="ci_run", description="Run the run() -> dict CI scenarios.")
    parser.add_argument("modules", nargs="*",
                        help="dotted module names (default: the CI list)")
    parser.add_argument("--timeout", type=float, default=default, metavar="S",
                        help="per-scenario timeout in seconds, 0 disables")
    parser.add_argument("--child-journal", metavar="JSONL",
                        help=argparse.SUPPRESS)
    parser.add_argument("--child-sys-path", metavar="PATHS",
                        help=argparse.SUPPRESS)
    opts = parser.parse_args(argv)
    if not math.isfinite(opts.timeout) or opts.timeout < 0:
        # -1 used to mean "no limit" (clamped to 0) and nan compared false
        # everywhere: refused, exit 2, before anything runs.
        parser.error(f"--timeout takes a finite number of seconds >= 0 "
                     f"(0 disables the limit), not {opts.timeout:g}")
    return opts


def main(argv=None) -> int:
    opts = _parse_args(sys.argv[1:] if argv is None else argv)
    names = tuple(opts.modules) or _SCENARIOS
    if opts.child_journal:
        global _JOURNAL
        _iso.utf8_streams()
        # Import the scenarios exactly as the parent would have: its
        # sys.path first (the parent may have found `recoverland` through
        # sys.path rather than PYTHONPATH: -m from the plugin's parent,
        # runpy, a console).
        for entry in reversed((opts.child_sys_path or "").split(os.pathsep)):
            if entry and entry not in sys.path:
                sys.path.insert(0, entry)
        _JOURNAL = _iso.ChildJournal(opts.child_journal)
        return _run(names, opts, _JOURNAL)
    return _supervise(names, opts)


def _run(names, opts, journal) -> int:
    """The supervised child: run every scenario in this one process."""
    journal.event("phase", phase="qgis-init")
    try:
        app = _init_qgis()
    except Exception:  # noqa: BLE001
        sys.stderr.write("FATAL: could not initialise QGIS headless\n")
        traceback.print_exc()
        return 2
    journal.event("phase", phase="scenarios")

    failures = []
    passed = []
    current = {"idx": None}

    def _on_expire(_watch_label, elapsed, hung_ident):
        idx = current["idx"]
        name = names[idx]
        journal.event("record", idx=idx, rec={"verdict": "TIMEOUT"})
        print(f"\n[ci_run] TIMEOUT {name}: no verdict after {elapsed:.1f} s "
              f"(limit {opts.timeout:g} s). Stack of every thread follows; "
              f"then this run stops, exit {EXIT_TIMEOUT}.")
        try:
            _iso.dump_all_stacks(sys.stdout, hung_ident)
        except Exception as exc:  # noqa: BLE001
            print(f"[ci_run] stack dump failed: {exc!r}")
        lines = ["", "CI VALIDATION TIMED OUT:",
                 f"  ! {name}: hung (no verdict after {elapsed:.1f} s)"]
        lines += [f"  - {other}: {detail}" for other, detail in failures]
        lines += [f"  ~ {other}: NOT RUN (the run stopped)"
                  for other in names[idx + 1:]]
        _VERDICT.update(stream="stderr", text="\n".join(lines) + "\n")
        try:
            journal.event("done", code=EXIT_TIMEOUT, blockers=[
                t.name for t in _iso.non_daemon_threads()],
                verdict=dict(_VERDICT))
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os._exit(EXIT_TIMEOUT)

    # The parent kills a GIL-held hang: the native timer only dumps.
    watchdog = _iso.ScenarioWatchdog(opts.timeout, _on_expire,
                                     native_grace_s=_NATIVE_GRACE_S,
                                     native_exit=False)
    try:
        for idx, name in enumerate(names):
            print(f"=== CI scenario: {name} ===", flush=True)
            current["idx"] = idx
            journal.event("start", idx=idx, name=name)
            watchdog.arm(name)
            try:
                mod = importlib.import_module(name)
                result = mod.run()
            except Exception:  # noqa: BLE001
                traceback.print_exc()
                failures.append((name, "EXCEPTION"))
                journal.event("record", idx=idx, rec={"verdict": "EXCEPTION"})
                continue
            finally:
                watchdog.disarm()
            verdict = (result or {}).get("verdict")
            if verdict != "PASS":
                failures.append((name, (result or {}).get("failed", verdict)))
            else:
                passed.append(name)
            journal.event("record", idx=idx, rec={"verdict": verdict})
    finally:
        journal.event("phase", phase="exitQgis")
        try:
            app.exitQgis()
        except Exception:  # noqa: BLE001
            pass

    journal.event("phase", phase="report")
    blockers = _iso.non_daemon_threads()
    if blockers:
        # Before the verdict, and a failure: the interpreter joins these at
        # exit, so this process (and QGIS, at close) would never end. This
        # runner used to hang here, red at the job's limit; leaving hard
        # must not turn that into a green job.
        thread_names = ", ".join(f"'{t.name}'" for t in blockers)
        print(f"[ci_run] LEAK suite-end: {len(blockers)} non-daemon thread(s) "
              f"would keep this process alive forever: {thread_names}",
              flush=True)
        failures.append(("(end of run)",
                         f"{len(blockers)} non-daemon thread(s) still alive "
                         f"({thread_names}): QGIS would not close"))
    # The verdict is the parent's to print, once this process has left
    # with the code it announces (see _VERDICT).
    if failures:
        lines = ["", "CI VALIDATION FAILED:"]
        lines += [f"  - {name}: {detail}" for name, detail in failures]
        _VERDICT.update(stream="stderr", text="\n".join(lines) + "\n")
        return 1

    _VERDICT.update(stream="stdout", text=f"\nCI VALIDATION PASSED: all "
                                          f"scenarios green ({len(passed)}).\n")
    return 0


def _supervise(names, opts) -> int:
    """Run the scenarios in one child; trust its verdict only if it ends so."""
    import shutil
    import tempfile

    try:
        sys.stdout.reconfigure(errors="backslashreplace")
    except Exception:  # noqa: BLE001
        pass
    work = tempfile.mkdtemp(prefix="rl_ci_run_")
    try:
        journal = os.path.join(work, "ci.jsonl")
        # The environment is inherited untouched (the scenarios see what
        # they saw before); only the child's own streams change: -u here,
        # UTF-8 in main(). Started by file, not by -m: -m would need the
        # package importable from the environment alone, and would fail
        # before any line of this file could say so.
        cmd = ([_iso.python_executable(), "-u", os.path.abspath(__file__),
                "--child-journal", journal, "--timeout", f"{opts.timeout:g}",
                "--child-sys-path",
                os.pathsep.join(p for p in sys.path if isinstance(p, str) and p)]
               + list(names))

        def _deadline(state, t_spawn):
            return _iso.phase_deadline(state, t_spawn, opts.timeout,
                                       _NATIVE_GRACE_S + _KILL_SLACK_S, _IDLE_S)

        code, killed, _elapsed, state = _iso.supervise(cmd, journal, _deadline)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return _judge(names, opts, code, killed, state)


def _print_verdict(verdict) -> None:
    text = (verdict or {}).get("text") or ""
    if not text:
        return
    sys.stdout.flush()
    stream = sys.stderr if verdict.get("stream") == "stderr" else sys.stdout
    stream.write(text)
    stream.flush()


def _judge(names, opts, code, killed, state) -> int:
    done = state.done
    if done is not None and not killed and code == done.get("code"):
        # Only now is the outcome known: the child's verdict stands.
        _print_verdict(done.get("verdict"))
        return code
    pending = state.pending()
    if killed and pending is not None:
        how = "hung in a scenario past its limit and was killed by the parent"
    elif killed:
        how = f"hung (no progress for {_IDLE_S:g} s) and was killed by the parent"
    else:
        how = f"ended with code {_iso.describe_returncode(code)}"
    sys.stdout.flush()
    if done is not None:
        sys.stderr.write(f"\nCI VALIDATION ABORTED: the run came to exit "
                         f"{done.get('code')}, then the process {how} during "
                         f"{state.phase_label()}: that verdict does not stand, "
                         f"and is not printed\n")
        # What the run had found stays useful; its headline does not.
        details = [ln for ln in ((done.get("verdict") or {}).get("text") or "")
                   .splitlines() if ln.startswith("  ")]
        if details:
            sys.stderr.write("what the run had listed before that:\n"
                             + "\n".join(details) + "\n")
        return EXIT_TIMEOUT if killed or done.get("code") == EXIT_TIMEOUT else 1
    sys.stderr.write(f"\nCI VALIDATION ABORTED: the process running the "
                     f"scenarios {how} before its report:\n")
    for idx, name in enumerate(names):
        rec = state.records.get(idx)
        if rec is not None:
            if rec.get("verdict") != "PASS":
                sys.stderr.write(f"  - {name}: {rec.get('verdict')}\n")
        elif idx == pending:
            if killed:
                sys.stderr.write(
                    f"  ! {name}: hung, no verdict; killed by the parent (its "
                    f"own watchdog never fired, native code probably held "
                    f"the GIL)\n")
            else:
                sys.stderr.write(
                    f"  ! {name}: the process ended with code "
                    f"{_iso.describe_returncode(code)} while it was running, "
                    f"before its verdict (os._exit, a native crash or "
                    f"abort())\n")
        else:
            sys.stderr.write(f"  ~ {name}: NOT RUN (the process "
                             f"{'hung' if killed else 'died'} before it)\n")
    if pending is None:
        sys.stderr.write(f"  ! the process {how} during "
                         f"{state.phase_label()}\n")
        if not state.started:
            return 2
    return EXIT_TIMEOUT if killed else 1


def _exit(code: int) -> None:
    """Leave with ``code``, through normal interpreter shutdown if possible.

    The child first tells its parent the code it is leaving with, so a crash
    in the shutdown that follows shows as a mismatch. The interpreter joins
    every non-daemon thread at exit: one left alive by a scenario would hold
    the CI job after its verdict, until the job's own limit, exactly the
    hang --timeout exists to bound. It was reported (and failed the run)
    before the verdict; leave without waiting for it.
    """
    blockers = _iso.non_daemon_threads()
    if _JOURNAL is not None:
        _JOURNAL.event("done", code=code, blockers=[t.name for t in blockers],
                       verdict=dict(_VERDICT))
    if not blockers:
        sys.exit(code)
    names = ", ".join(f"'{t.name}'" for t in blockers)
    print(f"[ci_run] leaving with exit {code} without waiting for "
          f"{len(blockers)} non-daemon thread(s): {names}")
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(code)


if __name__ == "__main__":
    _exit(main())
