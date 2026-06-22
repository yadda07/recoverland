"""God Object move-only non-regression check (decision D-GOD-01).

Per mode, an ordered list of MILESTONE log patterns derived from the REAL
`recover_dialog.py` flog calls (not invented). Before/after an extraction phase
the user exercises a mode in QGIS; this asserts the milestones still fire in the
same relative order, ignoring all non-milestone noise.

Two ways to check (both assert the SAME milestone order):

  * RETROSPECTIVE (recommended, no timing): exercise the mode in the GUI, THEN
    call check_recent(mode). It anchors on the LAST occurrence of the mode's
    entry milestone, so action order / timing / plugin reload never matter.

  * MARKER (for scripted runs): mark(mode) BEFORE exercising, check_mode(mode)
    AFTER. The window is everything after the sentinel (needs correct ordering).

Usage (QGIS Python console, plugin loaded):
    from scripts.validation import god_object_check as gc
    # ... exercise the review / version / event flow in the dialog ...
    gc.check_recent('review_snapshot')   # -> PASS/FAIL on milestone order

Offline self-test (no QGIS):
    python -m scripts.validation.god_object_check --selftest
"""
from __future__ import annotations

import re
import sys
import uuid
from pathlib import Path
from typing import List, Optional

from .assert_log import assert_sequence_in_order
from .parse_log import LogRecord, parse_line, read_records

# Milestones derived from recover_dialog.py (line refs are documentation of
# provenance, matched as substrings on the raw log line; trace-id prefixes and
# values are irrelevant to the match).
MILESTONES = {
    # Full event search + restore of one event.
    # Anchored on recover_event:start (the EVENT-mode entry, post-validation),
    # NOT recover_and_load:START which fires pre-validation in ALL modes and is
    # therefore not event-specific (confirmed in runtime: 5x recover_and_load
    # START split across 2 temporal + 3 review, 0 event).
    "event_search": [
        r"recover_event: start",                    # recover_dialog.py:2806
        r"_display_search_result: total_count=",    # :3418
        r"restore_event: start",                    # :3884
        r"restore_event: done",                     # :3906
    ],
    # Version/temporal rewind to a cutoff date.
    "version_rewind": [
        r"recover_version: start",                  # :2872
        r"on_version_fetch_done: raw=",             # :2925
        r"recover_version: done",                   # :3266
    ],
    # Review snapshot session start (async overlay creation).
    "review_snapshot": [
        r"review: snapshot_mode_start",             # :2008
        r"review: snapshot_init_direct",            # :2148
        r"review: snapshot_bar_shown",              # :2195
        r"review: snapshot_ready",                  # :2207
    ],
    # Undo the last restore (revert to pre-restore state).
    "undo": [
        r"undo_last: requested",                    # :4200
        r"undo_done: trace invalidation",           # :4278
    ],
    # NOTE: "dashboard"/stats flow is intentionally NOT covered: _request_stats_refresh
    # (:535), _launch_stats_worker (:539) and _on_stats_ready (:550) emit NO flog
    # milestones (silent main-thread path). A milestone check would require adding
    # instrumentation first; tracked as a separate item, not invented here.
}

_MARK_PREFIX = "GOLDEN_MARK"


def mark(mode: str) -> str:
    """Drop a unique sentinel line in the log; call before exercising the mode."""
    if mode not in MILESTONES:
        raise KeyError(f"unknown mode {mode!r}; known={sorted(MILESTONES)}")
    from recoverland.core.logger import flog  # QGIS-only; not used offline
    token = uuid.uuid4().hex[:8]
    flog(f"{_MARK_PREFIX} id={mode} token={token}")
    print(f"[god-check] marked mode={mode} token={token} - now exercise the GUI, "
          f"then call check_mode('{mode}')")
    return token


def read_since_mark(mode: str, log_path: Optional[Path] = None) -> Optional[List[LogRecord]]:
    """Return records after the LAST sentinel for `mode`.

    Returns None if no sentinel exists (mark was never called). Returns a list
    (possibly EMPTY, when the marker is the last line) otherwise, so callers can
    distinguish "no mark" from "marked but nothing exercised yet".
    """
    from .runner import _resolve_log_path
    path = log_path or _resolve_log_path()
    records = read_records(path)
    marker = f"{_MARK_PREFIX} id={mode}"
    last = -1
    for i, rec in enumerate(records):
        if marker in rec.raw:
            last = i
    if last < 0:
        return None
    return records[last + 1:]


def check_records(mode: str, records) -> tuple:
    """Pure milestone-order assertion over given records."""
    if mode not in MILESTONES:
        raise KeyError(f"unknown mode {mode!r}; known={sorted(MILESTONES)}")
    return assert_sequence_in_order(records, MILESTONES[mode], name=f"god_object:{mode}")


def check_mode(mode: str, log_path: Optional[Path] = None) -> bool:
    """Read the window since the mark and assert milestone order. Print verdict."""
    records = read_since_mark(mode, log_path=log_path)
    if records is None:
        print(f"[god-check] mode={mode} verdict=FAIL no_mark_found "
              f"(call mark('{mode}') before exercising the GUI)")
        return False
    name, ok, msg = check_records(mode, records)
    print(f"[god-check] mode={mode} n_records={len(records)} "
          f"verdict={'PASS' if ok else 'FAIL'} {msg}")
    return ok


def find_last_run(mode: str, records: List[LogRecord]) -> Optional[List[LogRecord]]:
    """Return records from the LAST occurrence of the mode's entry milestone.

    Anchors on the mode's own first milestone instead of a manual marker, so the
    user can exercise the mode at any time and check afterwards. Returns None if
    the entry milestone never appears in the log.
    """
    if mode not in MILESTONES:
        raise KeyError(f"unknown mode {mode!r}; known={sorted(MILESTONES)}")
    entry = re.compile(MILESTONES[mode][0])
    last = -1
    for i, rec in enumerate(records):
        if entry.search(rec.raw):
            last = i
    if last < 0:
        return None
    return records[last:]


def check_recent(mode: str, log_path: Optional[Path] = None) -> bool:
    """Retrospective check (no marker): find the LAST run of `mode` and assert
    milestone order. Exercise the mode in the GUI first, then call this.
    """
    from .runner import _resolve_log_path
    path = log_path or _resolve_log_path()
    records = read_records(path)
    window = find_last_run(mode, records)
    if window is None:
        print(f"[god-check] mode={mode} verdict=FAIL entry_never_logged "
              f"/{MILESTONES[mode][0]}/ (exercise the {mode} flow in the GUI first)")
        return False
    name, ok, msg = check_records(mode, window)
    print(f"[god-check] mode={mode} n_records={len(window)} since=last_entry "
          f"verdict={'PASS' if ok else 'FAIL'} {msg}")
    return ok


# --- Antithese offline: prove the check detects regressions for every mode ---

def _synth_log(mode: str, patterns, *, stale_mark: bool = False) -> str:
    """Build a formatter-compatible log: optional stale mark, current mark,
    then one line per pattern (in order) interleaved with worker-thread noise."""
    rows = []
    n = [0]

    def line(msg: str, thread: str = "MainThread     ") -> str:
        n[0] += 1
        return (f"2026-06-21T18:{n[0] // 60:02d}:{n[0] % 60:02d}.000 "
                f"[INFO   ] [{thread}] {msg}")

    if stale_mark:
        rows.append(line(f"{_MARK_PREFIX} id={mode} token=stale"))
        rows.append(line(f"[dead0000] {patterns[0]} STALE pre-mark"))
    rows.append(line(f"{_MARK_PREFIX} id={mode} token=current"))
    for i, pat in enumerate(patterns):
        rows.append(line(f"[aa11bb22] {pat} value={i} extra"))
        rows.append(line("timeline: paint chatter n=999", "Dummy-7        "))
    return "\n".join(rows) + "\n"


def _selftest() -> int:
    import tempfile
    tmp = Path(tempfile.gettempdir()) / "god_check_selftest.log"
    cases = []

    # Per mode, for BOTH checkers: full flow in order -> PASS;
    # last run missing its final milestone -> FAIL.
    for mode, patterns in MILESTONES.items():
        tmp.write_text(_synth_log(mode, patterns, stale_mark=True), encoding="utf-8")
        cases.append((f"{mode}_check_mode_passes",
                      check_mode(mode, log_path=tmp) is True))
        cases.append((f"{mode}_check_recent_passes",
                      check_recent(mode, log_path=tmp) is True))
        tmp.write_text(_synth_log(mode, patterns[:-1]), encoding="utf-8")
        cases.append((f"{mode}_check_mode_missing_fails",
                      check_mode(mode, log_path=tmp) is False))
        cases.append((f"{mode}_check_recent_missing_fails",
                      check_recent(mode, log_path=tmp) is False))

    # check_recent with the entry milestone never logged -> FAIL (loud)
    tmp.write_text(
        "2026-06-21T18:30:00.000 [INFO   ] [MainThread     ] unrelated noise only\n",
        encoding="utf-8")
    cases.append(("check_recent_entry_never_logged_fails",
                  check_recent("event_search", log_path=tmp) is False))

    # no mark at all -> FAIL (loud, not silent pass)
    tmp.write_text(
        "2026-06-21T18:30:00.000 [INFO   ] [MainThread     ] recover_and_load: START\n",
        encoding="utf-8")
    cases.append(("no_mark_fails", check_mode("event_search", log_path=tmp) is False))

    # marked but nothing exercised after -> FAIL milestone[0], NOT no_mark
    tmp.write_text(
        "2026-06-21T18:30:00.000 [INFO   ] [MainThread     ] "
        f"{_MARK_PREFIX} id=event_search token=current\n",
        encoding="utf-8")
    win = read_since_mark("event_search", log_path=tmp)
    ok = check_mode("event_search", log_path=tmp)
    cases.append(("empty_window_is_not_no_mark",
                  win is not None and len(win) == 0 and ok is False))

    try:
        tmp.unlink()
    except OSError:
        pass

    all_ok = all(p for _, p in cases)
    for n, p in cases:
        print(f"[god-check-selftest] {'PASS' if p else 'FAIL'} {n}")
    print(f"[god-check-selftest] VERDICT={'PASS' if all_ok else 'FAIL'} "
          f"note={'check_can_fail' if all_ok else 'BROKEN'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        raise SystemExit(_selftest())
    print("usage: python -m scripts.validation.god_object_check --selftest")
    print("runtime: exercise the mode in the GUI -> gc.check_recent(mode)")
    raise SystemExit(0)
