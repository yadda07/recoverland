# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Runner for RecoverLand runtime validation scenarios.

A scenario is a Python module that exposes:

    SCENARIO_ID: str                 - unique, snake_case
    INVARIANT: str                   - "I-N" or "BL-RW-PX-NN"
    setup(ctx)                        - prepare state (idempotent)
    run(ctx)                          - perform the action under test
    assertions(ctx) -> list[Assertion]
    teardown(ctx)                     - optional: release what setup/run
                                        started; always called last

ctx is a SimpleNamespace populated by the runner:

    ctx.scenario_id, ctx.invariant, ctx.t0, ctx.trace_id
    ctx.log_path, ctx.log_offset_start, ctx.log_offset_end
    ctx.records   (filled after run, before assertions)
    ctx.log_window (how ctx.records was gathered: files, rotations, ...)
    ctx.data      (free for the scenario to stash state)
"""
from __future__ import annotations

import heapq
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from .assert_log import AssertionSummary, summarize
from .parse_log import log_anchor, log_file_size, parse_line, read_window

_HERE = Path(__file__).resolve().parent

# Scenarios import their helpers as ``scripts.validation.assert_log``, which
# needs the PLUGIN ROOT on sys.path. headless_run.py puts it there; the QGIS
# Python console does not, so calling run_scenario() from the console failed
# with ModuleNotFoundError halfway through -- after setup() and run() had
# already written to disk. Doing it here makes the runner work from any entry
# point, which is the point of a test harness.
_PLUGIN_ROOT = _HERE.parents[1]
for _extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)


def _alias_scripts_package() -> None:
    """Bind ``scripts.validation`` to this very package in sys.modules.

    Extending sys.path is not enough inside a live QGIS: something else has
    already imported a top-level ``scripts`` (QGIS ships one), so the name is
    bound in sys.modules and Python never re-resolves it -- the path entry is
    simply ignored and the scenario dies on ModuleNotFoundError, after setup()
    and run() have already written to disk.

    Aliasing is safe both ways round: when ``scripts.validation`` already
    resolves to us, nothing is touched.
    """
    import importlib

    try:
        mod = importlib.import_module("scripts.validation")
        if Path(getattr(mod, "__file__", "") or "").resolve().parent == _HERE:
            return
    except Exception:  # noqa: BLE001 - absent or foreign, both handled below
        pass
    try:
        pkg = importlib.import_module(__package__ or "recoverland.scripts.validation")
        parent_name = (__package__ or "recoverland.scripts.validation").rsplit(".", 1)[0]
        sys.modules["scripts"] = importlib.import_module(parent_name)
        sys.modules["scripts.validation"] = pkg
    except Exception as exc:  # noqa: BLE001 - never break a run over this
        print(f"[validation] could not alias scripts.validation: {exc!r}")


_alias_scripts_package()
_REPORTS_DIR = _HERE / "reports"
_REGRESSION_DIR = _HERE / "scenarios" / "regression"


def _resolve_log_path() -> Path:
    """Return the RecoverLand debug log path.

    Order matters, and the first entry is the one that makes log-based
    assertions work anywhere:

    1. ``core.logger._LOG_FILE``: the path the logger ACTUALLY opened, once
       and for all, at import time. Asking the logger removes any chance of
       the runner and the logger disagreeing, which is exactly what happened
       on a headless run: the logger resolved the profile through
       QgsApplication while the runner rebuilt a hard-coded Windows path, so
       every ``assert_log_contains`` came back with n=0 on code that had
       behaved perfectly. The same mismatch would make the suite unrunnable
       on Linux/CI, where that hard-coded path does not exist at all.
    2. the plugin's public API, when the plugin is loaded in a live QGIS.
    3. the conventional profile location, last resort for offline tooling.
    """
    try:
        from recoverland.core.logger import _LOG_FILE  # type: ignore
        if _LOG_FILE:
            return Path(_LOG_FILE)
    except Exception:
        pass
    try:
        from qgis.utils import plugins  # type: ignore
        plugin = plugins.get("recoverland")
        if plugin is not None and hasattr(plugin, "api_log_path"):
            return Path(plugin.api_log_path())
    except Exception:
        pass
    # Fallback: standard profile location for the dev workstation
    return (
        Path.home()
        / "AppData" / "Roaming" / "QGIS" / "QGIS4"
        / "profiles" / "default"
        / "recoverland" / "recoverland_debug.log"
    )


def _new_trace_id() -> str:
    return uuid.uuid4().hex[:8]


def _ensure_reports_dir() -> Path:
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    return _REPORTS_DIR


def _build_ctx(scenario_module) -> SimpleNamespace:
    scenario_id = getattr(scenario_module, "SCENARIO_ID", scenario_module.__name__)
    invariant = getattr(scenario_module, "INVARIANT", "")
    return SimpleNamespace(
        scenario_id=scenario_id,
        invariant=invariant,
        t0=datetime.now(timezone.utc),
        trace_id=_new_trace_id(),
        log_path=_resolve_log_path(),
        log_offset_start=0,
        log_offset_end=0,
        records=[],
        log_window={},
        data={},
    )


# --------------------------------------------------------------------- #
# The scenario's log window
# --------------------------------------------------------------------- #
#
# The window used to be "the bytes of recoverland_debug.log from its size
# before setup(ctx) to its end after run(ctx)". The log rotates at 5 MB:
# when it turned over during a scenario, the live file was a new, shorter
# one, the saved offset pointed past its end, and the window came back
# EMPTY. Every log assertion of that scenario then failed with n=0 on code
# that had behaved perfectly, and only when the scenario happened to cross
# the 5 MB mark, i.e. depending on the order of the suite.
#
# The window is still the LOG FILE, now read across rotations from the file
# it started in (parse_log.read_window): scenarios check what the forensic
# log holds (lines lost or mangled in it are defects some of them look
# for), including what OTHER processes wrote there (a child QGIS started by
# a scenario shares the profile's log). An in-memory handler on the
# plugin's logger, attached for the scenario's duration, keeps every record
# of THIS process as a second source. It fills in only what the files can
# no longer give: records written to a file that more rotations than kept
# backups have deleted. A record of this process that the files should
# hold and do not is NOT slipped back in (that would hide a log that loses
# lines); it is counted and said.

_FILE_LOGGER = "RecoverLand.FileDebug"
_LINE_BREAK_RE = re.compile(r"\r\n|\r|\n")


class _RecordCapture(logging.Handler):
    """Every record the plugin's debug logger emits while it is attached.

    Formatted by the file handler's own formatter: each line is, byte for
    byte, the one written to the log, and parse_log reads it the same way.
    Split on line breaks as a text-mode read of the file would.
    """

    def __init__(self, formatter):
        super().__init__(logging.DEBUG)
        self.setFormatter(formatter)
        self._lines = []
        self._open = True

    def emit(self, record):
        if not self._open:
            return
        try:
            text = self.format(record)
        except Exception:  # noqa: BLE001 - the file handler reports it
            return
        self._lines.extend(_LINE_BREAK_RE.split(text))

    def drain(self) -> list:
        """Stop capturing (a record being emitted right now is waited for)."""
        self.acquire()
        try:
            self._open = False
            lines, self._lines = self._lines, []
        finally:
            self.release()
        return lines


def _same_file(a, b) -> bool:
    return (os.path.normcase(os.path.abspath(str(a)))
            == os.path.normcase(os.path.abspath(str(b))))


class _LogWindow:
    """The log records of one scenario, from before setup(ctx) to after run(ctx)."""

    def __init__(self, log_path):
        self.log_path = log_path
        self.logger = logging.getLogger(_FILE_LOGGER)
        self.capture = None
        self.capture_off = ""
        # The anchor BEFORE the capture: every record the capture sees is
        # then written after the anchor, so one missing from a complete
        # window really is missing from the log.
        self.anchor = log_anchor(log_path)
        writer = next((h for h in list(self.logger.handlers)
                       if getattr(h, "baseFilename", None)
                       and _same_file(h.baseFilename, log_path)), None)
        if writer is None:
            # Never attach to a logger that core.logger has not set up yet:
            # it only adds its file handler to a logger WITHOUT handlers,
            # so a handler of ours would stop the debug log being written
            # at all. The file alone is then the window, as before.
            self.capture_off = (f"no handler of {_FILE_LOGGER} writes "
                                f"{log_path}: the window is the file alone")
        else:
            self.capture = _RecordCapture(writer.formatter or logging.Formatter())
            self.logger.addHandler(self.capture)
        self._closed = False

    def detach(self) -> list:
        """Remove the in-memory handler; return the lines it kept."""
        if self._closed:
            return []
        self._closed = True
        if self.capture is None:
            return []
        self.logger.removeHandler(self.capture)
        lines = self.capture.drain()
        self.capture.close()
        return lines

    def close(self) -> tuple:
        """``(records, info)`` of the window; detaches the handler."""
        mem_lines = self.detach()
        window = read_window(self.anchor)
        memory = [r for r in map(parse_line, mem_lines) if r is not None]
        records, restored, absent = _merge_window(
            window.records, memory, window.complete)
        info = {
            "files": window.files,
            "rotations": window.rotations,
            "complete": window.complete,
            "note": window.note,
            "bytes_read": window.bytes_read,
            "file_records": len(window.records),
            "memory_records": len(memory),
            "restored_from_memory": len(restored),
            "not_in_log": len(absent),
            "not_in_log_first": absent[0].raw[:200] if absent else "",
            "memory_capture": ("on" if self.capture is not None
                               else f"off: {self.capture_off}"),
        }
        return records, info


def _merge_window(file_records: list, memory_records: list,
                  complete: bool) -> tuple:
    """``(records, restored, absent)`` of a window.

    Each memory record is matched to one identical line of the files (same
    timestamp, level, thread and message). The rest is ``restored`` when
    the files that should hold it are gone (the window lost its start:
    records up to the first one the surviving files hold), and put back at
    its timestamp, after the file records of the same millisecond.
    Otherwise it is ``absent``: this process emitted it, the log does not
    have it, and the window stays what the log holds.
    """
    pending = Counter(r.raw for r in file_records)
    missing = []
    for rec in memory_records:
        if pending[rec.raw] > 0:
            pending[rec.raw] -= 1
        else:
            missing.append(rec)
    if complete or not missing:
        return list(file_records), [], missing
    if file_records:
        first_ts = file_records[0].ts
        restored = [r for r in missing if r.ts <= first_ts]
        absent = [r for r in missing if r.ts > first_ts]
    else:
        restored, absent = missing, []
    merged = list(heapq.merge(file_records, restored, key=lambda r: r.ts))
    return merged, restored, absent


def _say_window(ctx) -> None:
    """Say what a reader of the log assertions must know about the window."""
    info = ctx.log_window
    if not info.get("complete", True):
        kept = (f"{info.get('restored_from_memory')} record(s) of this "
                f"process written there are restored from memory"
                if str(info.get("memory_capture", "")).startswith("on")
                else "no in-memory capture: records written there are lost")
        print(f"[validation] log window of {ctx.scenario_id} is incomplete: "
              f"{info.get('note')}; {kept}; lines of OTHER processes written "
              f"there are lost")
    if info.get("not_in_log"):
        print(f"[validation] {info.get('not_in_log')} record(s) emitted by "
              f"this process during {ctx.scenario_id} are NOT in the debug "
              f"log {ctx.log_path} (a write that failed, or a line mangled "
              f"in the file); they are not in ctx.records either. First: "
              f"{info.get('not_in_log_first')}")


def run_scenario(scenario_or_path) -> dict:
    """Run a scenario module (or path to one) and return the verdict dict.

    The verdict is also written to `scripts/validation/reports/<id>_<ts>.json`.
    """
    scenario_module = _load_module(scenario_or_path)
    ctx = _build_ctx(scenario_module)

    print(f"[validation] start scenario={ctx.scenario_id} "
          f"invariant={ctx.invariant} trace_id={ctx.trace_id}")

    window = _LogWindow(ctx.log_path)
    ctx.log_offset_start = window.anchor.offset
    t_setup_start = time.monotonic()

    teardown = getattr(scenario_module, "teardown", None)
    try:
        setup = getattr(scenario_module, "setup", None)
        if setup is not None:
            setup(ctx)
        t_run_start = time.monotonic()
        scenario_module.run(ctx)
        t_run_end = time.monotonic()

        # Give the rotating handler a chance to flush.
        time.sleep(0.2)
        ctx.log_offset_end = log_file_size(ctx.log_path)
        ctx.records, ctx.log_window = window.close()
        _say_window(ctx)

        raw_assertions = list(scenario_module.assertions(ctx))
    except BaseException:
        # The scenario is already lost; what it started must not become the
        # next scenario's problem. Its own exception is the one reported.
        # Nor may its in-memory log handler outlive it.
        window.detach()
        _run_teardown(teardown, ctx)
        raise
    teardown_error = _run_teardown(teardown, ctx)
    if teardown_error:
        raw_assertions.append((
            "_teardown_released_everything", False,
            f"teardown(ctx) raised {teardown_error}: what this scenario "
            f"started (threads, journals, layers) may still be running "
            f"during the next one"))
    summary: AssertionSummary = summarize(raw_assertions)

    duration_ms = int((time.monotonic() - t_setup_start) * 1000)
    run_ms = int((t_run_end - t_run_start) * 1000)

    verdict = {
        "scenario": ctx.scenario_id,
        "invariant": ctx.invariant,
        "trace_id": ctx.trace_id,
        "started_at": ctx.t0.isoformat(),
        "duration_ms": duration_ms,
        "run_ms": run_ms,
        "log_path": str(ctx.log_path),
        # Bytes read across the rotation chain; the difference of two
        # offsets of a file that turned over meant nothing (it went
        # negative exactly when the window came back empty).
        "log_window_bytes": ctx.log_window.get("bytes_read", 0),
        "log_records_captured": len(ctx.records),
        "log_window": ctx.log_window,
        "assertions": [
            {"name": name, "ok": ok, "message": msg}
            for (name, ok, msg) in raw_assertions
        ],
        "verdict": summary.verdict,
        "passed": summary.passed,
        "failed": summary.failed,
        "total": summary.total,
    }

    report_path = _ensure_reports_dir() / _report_filename(ctx)
    report_path.write_text(json.dumps(verdict, indent=2), encoding="utf-8")
    print(f"[validation] verdict={verdict['verdict']} "
          f"passed={summary.passed}/{summary.total} "
          f"duration_ms={duration_ms} report={report_path}")
    for name, ok, msg in raw_assertions:
        marker = "PASS" if ok else "FAIL"
        print(f"  [{marker}] {name}: {msg}")
    return verdict


def _run_teardown(teardown, ctx) -> str:
    """Call the optional ``teardown(ctx)``; return "" or what it raised.

    Scenarios had no way to release what they start: the contract stopped
    at assertions(ctx), so a writer thread, a journal connection or a map
    layer created by one scenario lived on through every later one (the
    headless runner now reports those as LEAK). teardown(ctx) runs after
    the assertions, whatever happened before it, and is optional.
    """
    if teardown is None:
        return ""
    try:
        teardown(ctx)
    except Exception as exc:  # noqa: BLE001 - reported, never masks a verdict
        import traceback
        traceback.print_exc()
        return repr(exc)
    return ""


def run_regression_suite() -> dict:
    """Run every scenario under scenarios/regression/ and aggregate."""
    if not _REGRESSION_DIR.is_dir():
        print(f"[validation] no regression suite at {_REGRESSION_DIR}")
        return {"verdict": "FAIL", "scenarios": [], "reason": "no_regression_dir"}

    scenarios = sorted(p for p in _REGRESSION_DIR.glob("*.py")
                       if not p.name.startswith("_"))
    verdicts = []
    n_pass = 0
    n_fail = 0
    for path in scenarios:
        try:
            verdict = run_scenario(path)
        except Exception as exc:  # noqa: BLE001 - scenario errors are FAIL
            verdict = {
                "scenario": path.stem, "verdict": "FAIL",
                "error": repr(exc),
            }
            print(f"[validation] ERROR in {path.name}: {exc!r}")
        verdicts.append(verdict)
        if verdict.get("verdict") == "PASS":
            n_pass += 1
        else:
            n_fail += 1

    suite_verdict = "PASS" if n_fail == 0 and n_pass > 0 else "FAIL"
    summary_path = _ensure_reports_dir() / f"regression_{_ts()}.json"
    summary = {
        "verdict": suite_verdict,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "scenarios": verdicts,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[validation] regression suite verdict={suite_verdict} "
          f"pass={n_pass} fail={n_fail} report={summary_path}")
    return summary


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _report_filename(ctx: SimpleNamespace) -> str:
    return f"{ctx.scenario_id}_{_ts()}.json"


def _load_module(scenario_or_path):
    """Load a scenario from a module object, path, or __file__ string."""
    if hasattr(scenario_or_path, "run"):
        return scenario_or_path
    path = Path(str(scenario_or_path))
    if not path.exists():
        raise FileNotFoundError(f"scenario not found: {path}")
    import importlib.util
    spec = importlib.util.spec_from_file_location(path.stem, str(path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load scenario from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "run"):
        raise AttributeError(f"scenario {path.name} has no run(ctx)")
    return module


__all__ = ["run_scenario", "run_regression_suite"]
