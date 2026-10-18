"""Isolation tools for the headless suite: timeouts, child processes, leaks.

Four defects of the suite runners live here, each with its own tool.

1. A scenario that hangs used to block the whole suite, forever and with no
   verdict: CI waited for its own job timeout and the log said nothing about
   which scenario, or where it was stuck. `ScenarioWatchdog` bounds every
   scenario in-process (Python watchdog thread, plus faulthandler's native
   timer as a backstop when native code holds the GIL), and `supervise`
   runs the scenarios in a child interpreter so the parent can kill it and
   carry on with the next one.

2. Scenarios were always run in the same order (sorted by name), so a
   scenario that only passes because of what an earlier one left behind, or
   only fails because of it, was invisible. The shuffling itself lives in
   headless_run.py (it is three lines); this module only has to keep the
   leak attribution correct whatever the order.

3. Nothing released by a scenario was ever checked. A writer thread, a map
   layer, a QgsTask or an open journal connection left alive by scenario A
   runs, or holds a file, during scenario B: B then measures A's leftovers.
   `ResourceSnapshot` records what is alive before and after each scenario;
   the difference, once given a short grace period to wind down, is that
   scenario's leak. What only the garbage collector closes (a connection
   nobody closed: on Python 3.12 a sqlite3.Connection references itself)
   is named as such, since the collection the check forces would
   otherwise hide it.

4. The exit code of the process that ran the scenarios was read as the
   verdict even when the report never produced it. A scenario (or a product
   path) ending the process with ``os._exit(0)`` gave a green suite with no
   report; a crash or a hang while closing QGIS, after the verdicts, went
   unnoticed or ended the run with no word on what happened. Nothing inside
   a dying process can report its own death (``atexit`` and ``finally`` do
   not run), so the scenarios always run in a child that writes a
   `ChildJournal` as it goes, and the parent (`supervise`, `JournalState`)
   accepts the child's verdict only when the child reached it AND then
   left with exactly that exit code.

Everything here is best effort by construction: a probe that fails is
reported as such ("leak check failed: ...") and never breaks a run.
"""
from __future__ import annotations

import faulthandler
import gc
import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path

PRODUCT_THREAD_PREFIX = "RecoverLand"
# Threads of the harness itself: never reported as a scenario's leak.
HARNESS_THREAD_PREFIX = "headless-"


# --------------------------------------------------------------------- #
# Interpreter and process tree
# --------------------------------------------------------------------- #

def python_executable() -> str:
    """The QGIS Python interpreter to start a child with.

    Under python-qgis.bat, ``sys.executable`` is that interpreter and the
    environment the .bat prepared is inherited by the child. From the QGIS
    GUI console, ``sys.executable`` is qgis-bin.exe: look for the python
    shipped next to it instead of starting a second QGIS.
    """
    exe = Path(sys.executable)
    if not exe.stem.lower().startswith("qgis"):
        return str(exe)
    for name in ("python3.exe", "python.exe", "python3", "python"):
        cand = exe.with_name(name)
        if cand.is_file():
            return str(cand)
    return str(exe)


def kill_tree(proc: subprocess.Popen) -> None:
    """Kill ``proc`` and every process it started.

    Killing the direct child only would leave a scenario's own helpers
    (crash harnesses start interpreters too) running, holding the output
    pipe open and the journal files locked.
    """
    if proc.poll() is not None:
        return
    try:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                           timeout=30, check=False)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except Exception:  # noqa: BLE001 - fall back on the direct child
        pass
    try:
        proc.kill()
    except Exception:  # noqa: BLE001
        pass
    try:
        proc.wait(timeout=30)
    except Exception:  # noqa: BLE001
        pass


class ChildJournal:
    """Child side of a supervised run: one JSON line per event, flushed at once.

    A process can end without warning (``os._exit``, a native crash,
    ``abort()``): nothing it meant to print at the end ever comes, and
    ``atexit`` and ``finally`` do not run. What it wrote here BEFORE, and
    flushed, outlives it; the parent reads it to know which scenario was
    running, which verdicts were reached, and whether the process got as far
    as its own report. Events: ``phase`` (qgis-init, scenarios, exitQgis,
    report), ``start`` / ``ran`` / ``record`` for each scenario (``ran`` is
    the verdict before the leak check, ``record`` the full record after it),
    and ``done`` with the exit code the process is about to leave with.

    The file is opened before the first scenario, so the leak check sees it
    in every baseline and never reports it.
    """

    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._fh = open(path, "a", encoding="utf-8")

    def event(self, ev: str, **fields) -> None:
        fields["ev"] = ev
        fields["t"] = time.time()  # wall clock: comparable across processes
        line = json.dumps(fields, default=str)
        with self._lock:
            try:
                self._fh.write(line + "\n")
                self._fh.flush()
            except Exception:  # noqa: BLE001 - never break a run over this
                pass


class JournalState:
    """Parent side: what a supervised child has reached, read as it goes."""

    def __init__(self, path: str):
        self.path = path
        self._offset = 0
        self._tail = b""
        self.started = {}   # idx -> parent's monotonic time the start was seen
        self.started_at = {}  # idx -> the child's wall clock at the start
        self.ran = {}       # idx -> outcome reached before the leak check
        self.records = {}   # idx -> record written after the leak check
        self.phase = None
        self.done = None    # the "done" event: {"code": ..., "blockers": [...]}
        self.last_seen = None

    def poll(self) -> None:
        try:
            with open(self.path, "rb") as fh:
                fh.seek(self._offset)
                chunk = fh.read()
        except OSError:
            return
        if not chunk:
            return
        self._offset += len(chunk)
        lines = (self._tail + chunk).split(b"\n")
        # A line cut by the death of the child never gets its end: only
        # complete lines count.
        self._tail = lines.pop()
        now = time.monotonic()
        for raw in lines:
            try:
                ev = json.loads(raw.decode("utf-8"))
            except Exception:  # noqa: BLE001
                continue
            self._apply(ev, now)

    def _apply(self, ev: dict, now: float) -> None:
        self.last_seen = now
        kind = ev.get("ev")
        idx = ev.get("idx")
        if kind == "start":
            self.started[idx] = now
            self.started_at[idx] = ev.get("t")
        elif kind == "ran":
            self.ran[idx] = ev.get("outcome")
        elif kind == "record":
            self.records[idx] = ev.get("rec") or {}
        elif kind == "phase":
            self.phase = ev.get("phase")
        elif kind == "done":
            self.done = ev

    def pending(self):
        """Index of the scenario started and never recorded, or None."""
        open_ = [i for i in self.started if i not in self.records]
        return max(open_) if open_ else None

    def age(self, idx) -> float:
        """Seconds since scenario ``idx`` started, by the child's clock."""
        wall = self.started_at.get(idx)
        if wall is not None:
            return max(0.0, time.time() - wall)
        return max(0.0, time.monotonic() - self.started.get(idx, time.monotonic()))

    def phase_label(self) -> str:
        """Where the child was when it stopped writing, in words."""
        if self.done is not None:
            return ("Python shutdown (interpreter finalization, atexit "
                    "handlers, object destructors)")
        return {
            None: "Python start-up, before QGIS",
            "qgis-init": "QGIS start-up (initQgis)",
            "scenarios": "the run, between two scenarios",
            "exitQgis": "QGIS shutdown (exitQgis)",
            "report": "its report",
        }.get(self.phase, str(self.phase))


def phase_deadline(state: JournalState, t_spawn: float, timeout_s: float,
                   scenario_slack_s: float, idle_s: float):
    """When the parent gives up on a supervised child, or None (no limit).

    A running scenario gets its timeout plus ``scenario_slack_s``: its own
    watchdog fires first, and the slack leaves the native stack dump of a
    GIL-held hang time to reach the parent. Everything else (QGIS start-up,
    the leak check, QGIS and Python shutdown) gets ``idle_s`` from the last
    event: no phase of the child is left unbounded, and a QGIS that will not
    close is a hang like any other. ``timeout_s`` 0 disables every limit.
    """
    if not timeout_s or timeout_s <= 0:
        return None
    pending = state.pending()
    if pending is not None and pending not in state.ran:
        return state.started[pending] + timeout_s + scenario_slack_s
    return (state.last_seen or t_spawn) + idle_s


def supervise(cmd, journal_path, deadline_fn=None, env=None, cwd=None,
              out=None, drain_s: float = 10.0) -> tuple:
    """Run ``cmd`` as a supervised child writing ``journal_path``.

    Returns ``(returncode, killed, elapsed_s, state)``; ``returncode`` is
    None when the child had to be killed, ``state`` is the `JournalState`
    read to the end. ``deadline_fn(state, t_spawn)`` is asked again every
    quarter second (the limit moves with the child's phase). Output is
    forwarded line by line as it comes, so a long child is followed live
    and the lines of a killed one are not lost.

    Once the child has ended, its output gets ``drain_s`` more seconds to
    come through. The pipe only reaches its end when the LAST process
    holding it closes it, and a process the child started (a scenario's
    helper, a product subprocess, anything spawned with inherited handles)
    can hold it long after the child is gone: waiting for that end, or
    closing the pipe (which waits for the reader's lock), froze the suite
    until that process died, with no verdict. Past ``drain_s`` the fact is
    said, the rest of that output is read and dropped (the other process
    must not block on a full pipe, nor its lines land after the verdict),
    and this returns.
    """
    out = out or sys.stdout
    state = JournalState(journal_path)
    popen_kw = {}
    if os.name != "nt":
        popen_kw["start_new_session"] = True
    t0 = time.monotonic()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            stdin=subprocess.DEVNULL, env=env, cwd=cwd, **popen_kw)
    forwarding = threading.Event()
    forwarding.set()

    def _pump():
        for raw in iter(proc.stdout.readline, b""):
            if not forwarding.is_set():
                continue
            try:
                # The child's text stream already wrote "\r\n" on Windows;
                # ours translates "\n" again: one line ending, not two.
                out.write(raw.decode("utf-8", errors="replace")
                          .replace("\r\n", "\n"))
                out.flush()
            except Exception:  # noqa: BLE001 - a closed console must not kill us
                pass

    reader = threading.Thread(target=_pump, name="headless-child-reader",
                              daemon=True)
    reader.start()
    killed = False
    try:
        while True:
            try:
                proc.wait(timeout=0.25)
                break
            except subprocess.TimeoutExpired:
                pass
            state.poll()
            limit = deadline_fn(state, t0) if deadline_fn else None
            if limit is not None and time.monotonic() > limit:
                killed = True
                kill_tree(proc)
                break
    except BaseException:
        # ^C, or a bug here: never leave a QGIS child running behind us.
        kill_tree(proc)
        raise
    finally:
        _drain(proc, reader, forwarding, out, max(0.0, drain_s))
    state.poll()
    return (None if killed else proc.returncode, killed,
            time.monotonic() - t0, state)


def _drain(proc, reader, forwarding, out, drain_s: float) -> None:
    """Let the ended child's output come through, for ``drain_s`` at most."""
    reader.join(timeout=drain_s)
    if not reader.is_alive():
        try:
            proc.stdout.close()
        except Exception:  # noqa: BLE001
            pass
        return
    # Still open: another process holds the write end. Closing it from here
    # would wait on the reader's lock for as long, so the reader (a daemon
    # thread) keeps it and keeps emptying it, silently.
    forwarding.clear()
    try:
        out.write(f"[headless] the child process ended "
                  f"({describe_returncode(proc.poll())}), but a process it "
                  f"started still holds its output pipe {drain_s:g} s later "
                  f"(a scenario's helper or a subprocess of the product, "
                  f"spawned with inherited handles): it is left running, "
                  f"orphaned, and its further output is not shown\n")
        out.flush()
    except Exception:  # noqa: BLE001 - a closed console must not kill us
        pass


def utf8_streams() -> None:
    """A supervised child writes UTF-8, whatever the console code page.

    The parent decodes the child's output as UTF-8. Changing the child's
    own streams, not PYTHONIOENCODING, keeps the environment that the
    scenarios (and the processes they start) see exactly as it was.
    """
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:  # noqa: BLE001 - not a TextIOWrapper: leave it
            pass


def describe_returncode(code) -> str:
    """Render an exit code, in hex when it is a Windows NTSTATUS crash code."""
    if code is None:
        return "none (killed)"
    if code < 0 or code > 0xFFFF:
        return f"{code} (0x{code & 0xFFFFFFFF:08X})"
    return str(code)


# --------------------------------------------------------------------- #
# Per-scenario watchdog
# --------------------------------------------------------------------- #

def dump_all_stacks(out=None, hung_ident=None) -> None:
    """Print the Python stack of every thread, naming each one.

    faulthandler dumps too, but without thread names and straight to a file
    descriptor (interleaving badly with buffered output); this is the
    readable version, and the only one that says which thread is the
    scenario's.
    """
    out = out or sys.stdout
    names = {t.ident: t.name for t in threading.enumerate()}
    for ident, frame in sys._current_frames().items():
        label = names.get(ident, f"<thread {ident}>")
        if label.startswith(HARNESS_THREAD_PREFIX):
            continue
        mark = " (the scenario's thread)" if ident == hung_ident else ""
        out.write(f"[headless] stack of thread '{label}'{mark}:\n")
        for line in traceback.format_stack(frame):
            out.write("    " + line.replace("\n", "\n    ").rstrip() + "\n")
    out.flush()


class ScenarioWatchdog:
    """Fire ``on_expire(label, elapsed_s)`` when a scenario overruns.

    One daemon thread for the whole suite. ``on_expire`` runs on that
    thread and is expected not to return (it reports, then ends the
    process): an in-process scenario that hangs cannot be stopped, only
    abandoned, and the suite cannot go on sharing a process with it.

    When native code holds the GIL, this Python thread cannot run at all.
    faulthandler's timer is a C thread that can: armed at timeout +
    ``native_grace_s``, it dumps every stack and, if ``native_exit``, ends
    the process with exit code 1 (faulthandler cannot be told another one;
    its "Timeout (h:mm:ss)!" header says what happened). In a child, the
    parent kills instead, so the backstop only dumps.
    """

    def __init__(self, timeout_s: float, on_expire, native_grace_s: float = 30.0,
                 native_exit: bool = True, exit_code: int = 3):
        self.timeout_s = float(timeout_s or 0)
        self._on_expire = on_expire
        self._exit_code = exit_code
        self._native_grace_s = native_grace_s
        self._native_exit = native_exit
        self._cond = threading.Condition()
        self._token = 0
        self._armed = None  # (token, label, t0, hung_ident)
        self._thread = None
        if self.timeout_s > 0:
            self._thread = threading.Thread(target=self._loop,
                                            name="headless-watchdog",
                                            daemon=True)
            self._thread.start()

    @property
    def enabled(self) -> bool:
        return self._thread is not None

    def arm(self, label: str) -> None:
        if not self.enabled:
            return
        with self._cond:
            self._token += 1
            self._armed = (self._token, label, time.monotonic(),
                           threading.get_ident())
            self._cond.notify_all()
        self._arm_native()

    def disarm(self) -> None:
        if not self.enabled:
            return
        with self._cond:
            self._armed = None
            self._cond.notify_all()
        try:
            faulthandler.cancel_dump_traceback_later()
        except Exception:  # noqa: BLE001
            pass

    def _arm_native(self) -> None:
        stream = sys.__stderr__
        try:
            if stream is None:
                return
            stream.fileno()
            faulthandler.dump_traceback_later(
                self.timeout_s + self._native_grace_s, repeat=False,
                file=stream, exit=self._native_exit)
        except Exception:  # noqa: BLE001 - no usable fd: Python watchdog only
            pass

    def _loop(self) -> None:
        while True:
            with self._cond:
                while self._armed is None:
                    self._cond.wait()
                token, label, t0, ident = self._armed
                remaining = self.timeout_s - (time.monotonic() - t0)
                if remaining > 0:
                    self._cond.wait(timeout=remaining)
                    continue
                if self._armed is None or self._armed[0] != token:
                    continue
                self._armed = None
            try:
                self._on_expire(label, time.monotonic() - t0, ident)
            except BaseException:  # noqa: BLE001 - a hang must never outlive us
                # A reporting bug must not turn a bounded hang back into an
                # endless one: say it, then leave with the timeout code.
                try:
                    traceback.print_exc()
                    print(f"[headless] TIMEOUT {label}: reporting failed, "
                          f"exiting with {self._exit_code}", flush=True)
                finally:
                    os._exit(self._exit_code)


# --------------------------------------------------------------------- #
# Leak detection
# --------------------------------------------------------------------- #

def _raw_fileio(stream):
    """Walk TextIOWrapper -> Buffered* -> FileIO; None if not a real file."""
    seen = 0
    while stream is not None and seen < 4:
        if isinstance(stream, io.FileIO):
            return stream
        stream = getattr(stream, "buffer", None) or getattr(stream, "raw", None)
        seen += 1
    return None


def _logging_owned_files() -> set:
    """ids of the FileIO objects that belong to logging handlers.

    The plugin's rotating handlers reopen their file at every rollover; that
    new file object is the logger's, not a leak of the scenario that happened
    to be running.
    """
    import logging

    owned = set()
    for ref in list(getattr(logging, "_handlerList", [])):
        try:
            handler = ref() if callable(ref) else ref
        except Exception:  # noqa: BLE001
            continue
        raw = _raw_fileio(getattr(handler, "stream", None))
        if raw is not None:
            owned.add(id(raw))
    for std in (sys.stdin, sys.stdout, sys.stderr,
                sys.__stdin__, sys.__stdout__, sys.__stderr__):
        raw = _raw_fileio(std)
        if raw is not None:
            owned.add(id(raw))
    return owned


def _sqlite_is_open(conn) -> bool:
    try:
        conn.in_transaction  # raises ProgrammingError once closed
        return True
    except Exception:  # noqa: BLE001
        return False


def _sqlite_path(conn) -> str:
    """Main database file of a connection, without changing its state.

    ``PRAGMA database_list`` starts no transaction. A connection opened with
    check_same_thread=True by another thread refuses any call from here: it
    is then named by its owner, not by its path.
    """
    try:
        rows = conn.execute("PRAGMA database_list").fetchall()
        for row in rows:
            if row[1] == "main":
                return row[2] or ":memory:"
        return "?"
    except Exception as exc:  # noqa: BLE001
        return f"<unreadable here: {type(exc).__name__}>"


def _thread_label(t) -> str:
    """Name a leaked thread; the product's own (RecoverLand-*) say so first.

    Every new thread is reported, not only the product's: a scenario's own
    helper thread holding a journal is just as much a neighbour's problem.
    The prefix only tells the reader where to look, product or scenario.
    """
    target = getattr(t, "_target", None)
    where = getattr(target, "__qualname__", None) or type(t).__name__
    kind = "daemon" if t.daemon else "NON-daemon, blocks interpreter exit"
    owner = ("product thread" if t.name.startswith(PRODUCT_THREAD_PREFIX)
             else "thread")
    return f"{owner} '{t.name}' ({kind}, runs {where})"


class ResourceSnapshot:
    """What a scenario could leave behind, at one instant.

    A baseline must not keep anything alive: holding a journal connection
    or a file object across the next scenario would change what that
    scenario sees (a file locked on Windows, a connection that the product
    believed closed). So a baseline keeps KEYS only (`keys_only()`), and
    objects are held by the measurement snapshot for the few milliseconds
    it takes to describe them.

    Keys are identities (thread ident + name, layer id, task id, object id
    of a connection or file). An object id can be reused once its object is
    freed: a baseline connection that dies during the scenario and whose
    id is taken by a new, leaked one would be missed. That is the price of
    not holding references, and it can only hide a leak, never invent one.
    """

    KINDS = ("threads", "qthreads", "tasks", "layers", "sqlite", "files")

    def __init__(self):
        for kind in self.KINDS:
            setattr(self, kind, {})
        self.errors = []

    def keys_only(self) -> "ResourceSnapshot":
        snap = ResourceSnapshot()
        for kind in self.KINDS:
            setattr(snap, kind, dict.fromkeys(getattr(self, kind)))
        return snap

    @classmethod
    def baseline(cls) -> "ResourceSnapshot":
        return cls.take(collect=False).keys_only()

    @classmethod
    def take(cls, collect: bool = False) -> "ResourceSnapshot":
        snap = cls()
        if collect:
            gc.collect()
        main = threading.main_thread()
        for t in threading.enumerate():
            if t is main or isinstance(t, threading._DummyThread):
                continue
            if t.name.startswith(HARNESS_THREAD_PREFIX):
                continue
            snap.threads[(t.ident, getattr(t, "native_id", None), t.name)] = t
        try:
            from qgis.core import QgsApplication, QgsProject, QgsTask

            tm = QgsApplication.taskManager()
            live = (QgsTask.Queued, QgsTask.OnHold, QgsTask.Running)
            for task in tm.tasks():
                if task.status() in live:
                    snap.tasks[tm.taskId(task)] = (
                        f"QgsTask '{task.description()}' "
                        f"({type(task).__name__}, status={int(task.status())})")
            for lid, layer in QgsProject.instance().mapLayers().items():
                try:
                    prov = layer.providerType()
                except Exception:  # noqa: BLE001
                    prov = "?"
                snap.layers[lid] = f"map layer '{layer.name()}' ({prov}, id={lid})"
        except Exception as exc:  # noqa: BLE001
            snap.errors.append(f"qgis probe: {exc!r}")
        try:
            snap._scan_heap()
        except Exception as exc:  # noqa: BLE001
            snap.errors.append(f"heap scan: {exc!r}")
        return snap

    def _scan_heap(self) -> None:
        import sqlite3

        try:
            from qgis.PyQt.QtCore import QThread
        except Exception:  # noqa: BLE001
            QThread = None
        owned = _logging_owned_files()
        objs = gc.get_objects()
        try:
            for obj in objs:
                typ = type(obj)
                if typ is sqlite3.Connection:
                    if _sqlite_is_open(obj):
                        self.sqlite[id(obj)] = obj
                elif typ is io.FileIO:
                    if not obj.closed and id(obj) not in owned:
                        self.files[(id(obj), str(obj.name))] = obj
                elif QThread is not None and isinstance(obj, QThread):
                    try:
                        if obj.isRunning():
                            self.qthreads[id(obj)] = obj
                    except Exception:  # noqa: BLE001 - deleted C++ object
                        pass
        finally:
            del objs

    def release(self) -> None:
        """Drop the objects this snapshot holds (it keeps nothing alive after)."""
        for kind in self.KINDS:
            getattr(self, kind).clear()

    def new_since(self, before: "ResourceSnapshot") -> list:
        """Human-readable list of what is alive now and was not in ``before``."""
        out = []
        new_threads = [t for key, t in self.threads.items()
                       if key not in before.threads and t.is_alive()]
        # The product's threads first: they are the ones a leak report is for.
        new_threads.sort(key=lambda t: not t.name.startswith(PRODUCT_THREAD_PREFIX))
        for t in new_threads:
            out.append(_thread_label(t))
        for key, obj in self.qthreads.items():
            if key not in before.qthreads:
                out.append(f"QThread {type(obj).__name__} "
                           f"'{obj.objectName()}' still running")
        for key, label in self.tasks.items():
            if key not in before.tasks:
                out.append(label)
        for key, label in self.layers.items():
            if key not in before.layers:
                out.append(label)
        for key, conn in self.sqlite.items():
            if key not in before.sqlite:
                out.append(f"open sqlite connection on {_sqlite_path(conn)}")
        for key, fh in self.files.items():
            if key not in before.files:
                out.append(f"open file {key[1]}")
        for err in self.errors:
            out.append(f"leak check failed: {err}")
        return out

    def settling(self, before: "ResourceSnapshot") -> bool:
        """True while something new could still wind down on its own."""
        return (any(k not in before.threads and t.is_alive()
                    for k, t in self.threads.items())
                or any(k not in before.qthreads for k in self.qthreads)
                or any(k not in before.tasks for k in self.tasks))


def _closables_new_since(before: ResourceSnapshot) -> dict:
    """{(kind, object id): label} of the connections and files open now.

    Only those new since ``before``, and only as labels: nothing is held
    across the collection that follows, or it could not free them.
    """
    snap = ResourceSnapshot()
    try:
        snap._scan_heap()
        found = {}
        for key, conn in snap.sqlite.items():
            if key not in before.sqlite:
                found[("sqlite", key)] = (f"open sqlite connection on "
                                          f"{_sqlite_path(conn)}")
        for key in snap.files:
            if key not in before.files:
                found[("files", key[0])] = f"open file {key[1]}"
        return found
    finally:
        snap.release()


def _collect_naming(candidates: dict) -> list:
    """``gc.collect()``, returning the labels of the candidates only it freed.

    The collection is what closes a connection or a file that nothing
    references any more but a reference cycle. In QGIS that happens when
    Python next collects, during some later scenario, which keeps the
    journal open (locked on Windows) meanwhile. Collecting silently between
    scenarios hid exactly that order dependence. DEBUG_SAVEALL keeps what
    the collector found unreachable in ``gc.garbage`` instead of freeing it,
    so those very objects can be named; they are then freed for real.
    """
    import sqlite3

    if not candidates:
        gc.collect()
        return []
    n0 = len(gc.garbage)
    old = gc.get_debug()
    gc.set_debug(old | gc.DEBUG_SAVEALL)
    try:
        gc.collect()
    finally:
        gc.set_debug(old)
    found = []
    try:
        for obj in gc.garbage[n0:]:
            typ = type(obj)
            if typ is sqlite3.Connection:
                label = candidates.get(("sqlite", id(obj)))
            elif typ is io.FileIO:
                label = candidates.get(("files", id(obj)))
            else:
                continue
            if label:
                found.append(label)
        obj = None  # noqa: F841 - the last one must not outlive the collection
    finally:
        del gc.garbage[n0:]
    gc.collect()
    return found


def measure_leaks(before: ResourceSnapshot, grace_s: float) -> list:
    """Leaks of the scenario that ran since ``before`` was taken.

    Threads, QThreads and tasks get ``grace_s`` to finish (a writer told to
    stop is allowed to drain); layers, connections and files are what they
    are. The collection pass first closes what nobody closed and only the
    collector can release, which CPython would do anyway, sooner or later:
    without it the verdict would depend on when the collector last ran.
    What that pass closed is reported too ("not closed (released only by
    the garbage collector)"): the scenario ended with it still open.
    """
    gc_closed = _collect_naming(_closables_new_since(before))
    after = ResourceSnapshot.take(collect=False)
    deadline = time.monotonic() + max(0.0, grace_s)
    while after.settling(before) and time.monotonic() < deadline:
        for key, t in after.threads.items():
            if key not in before.threads and t.is_alive():
                t.join(timeout=max(0.0, min(0.1, deadline - time.monotonic())))
        time.sleep(0.02)
        after.release()
        after = ResourceSnapshot.take(collect=False)
    try:
        leaks = after.new_since(before)
    finally:
        after.release()
    for label in gc_closed:
        # The cause to look for is the missing close(), not a cycle of the
        # caller's making: on Python 3.12 a sqlite3.Connection references
        # itself through its statement cache, so any connection nobody
        # closes lands here. "A reference cycle kept it open" sent readers
        # looking for a cycle that was not there.
        leaks.append(f"{label}, not closed (released only by the garbage "
                     f"collector): still open when the scenario ended, it "
                     f"would stay open in QGIS until Python next collects, "
                     f"during a later scenario")
    return leaks


def non_daemon_threads() -> list:
    """Threads that would keep the interpreter alive at exit."""
    main = threading.main_thread()
    return [t for t in threading.enumerate()
            if t is not main and t.is_alive() and not t.daemon
            and not isinstance(t, threading._DummyThread)]


__all__ = [
    "PRODUCT_THREAD_PREFIX", "HARNESS_THREAD_PREFIX", "python_executable",
    "kill_tree", "ChildJournal", "JournalState", "phase_deadline",
    "supervise", "utf8_streams", "describe_returncode", "dump_all_stacks",
    "ScenarioWatchdog", "ResourceSnapshot", "measure_leaks",
    "non_daemon_threads",
]
