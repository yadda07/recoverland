"""Log assertions for RecoverLand validation scenarios.

A scenario uses these helpers to express the "signature of log
expected" referenced in docs/rewind_charter.md §5 and §7.

All assertion helpers return a tuple `(name, ok, message)` so they
plug directly into the assertions list of a scenario.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional, Pattern, Sequence, Tuple

from .parse_log import LogRecord

Assertion = Tuple[str, bool, str]


def _compile(pattern: str | Pattern) -> Pattern:
    if isinstance(pattern, str):
        return re.compile(pattern)
    return pattern


def assert_log_contains(
    records: Iterable[LogRecord],
    pattern: str | Pattern,
    name: Optional[str] = None,
    min_count: int = 1,
    max_count: Optional[int] = None,
) -> Assertion:
    """Assert that `pattern` matches at least `min_count` records."""
    regex = _compile(pattern)
    matches = [r for r in records if regex.search(r.raw)]
    n = len(matches)
    ok = n >= min_count and (max_count is None or n <= max_count)
    label = name or f"contains:{regex.pattern[:60]}"
    bounds = f"min={min_count}" + (f" max={max_count}" if max_count is not None else "")
    msg = f"n={n} {bounds}"
    return (label, ok, msg)


def assert_log_absent(
    records: Iterable[LogRecord],
    pattern: str | Pattern,
    name: Optional[str] = None,
) -> Assertion:
    """Assert that `pattern` does not match any record."""
    regex = _compile(pattern)
    matches = [r for r in records if regex.search(r.raw)]
    n = len(matches)
    ok = n == 0
    label = name or f"absent:{regex.pattern[:60]}"
    extra = ""
    if not ok:
        extra = f" first_match=`{matches[0].raw[:120]}`"
    return (label, ok, f"n={n}{extra}")


def assert_no_log_between(
    records: List[LogRecord],
    marker_start: str | Pattern,
    marker_end: str | Pattern,
    forbidden: str | Pattern,
    name: Optional[str] = None,
) -> Assertion:
    """Assert no `forbidden` record appears between marker_start and marker_end.

    Looks for the FIRST occurrence of marker_start and the FIRST
    marker_end after it. Useful e.g. to prove that no event is written
    between `EditSessionTracker: suppressed` and `unsuppressed`.
    """
    start_re = _compile(marker_start)
    end_re = _compile(marker_end)
    forbidden_re = _compile(forbidden)
    label = name or f"no_between:{forbidden_re.pattern[:60]}"

    start_idx = None
    end_idx = None
    for i, rec in enumerate(records):
        if start_idx is None and start_re.search(rec.raw):
            start_idx = i
            continue
        if start_idx is not None and end_idx is None and end_re.search(rec.raw):
            end_idx = i
            break

    if start_idx is None:
        return (label, False, "marker_start not found")
    if end_idx is None:
        return (label, False, "marker_end not found after marker_start")

    window = records[start_idx + 1:end_idx]
    matches = [r for r in window if forbidden_re.search(r.raw)]
    ok = not matches
    extra = ""
    if matches:
        extra = f" first_match=`{matches[0].raw[:120]}`"
    return (label, ok, f"window_size={len(window)} forbidden_n={len(matches)}{extra}")


def assert_field_value(
    records: Iterable[LogRecord],
    event_pattern: str | Pattern,
    field_name: str,
    expected_value: str | Callable[[str], bool],
    name: Optional[str] = None,
) -> Assertion:
    """Assert that the FIRST record matching event_pattern has
    `fields[field_name]` equal to `expected_value` (or that the
    callable returns True for it)."""
    regex = _compile(event_pattern)
    label = name or f"field:{field_name}@{regex.pattern[:40]}"
    for rec in records:
        if regex.search(rec.raw):
            actual = rec.fields.get(field_name)
            if actual is None:
                return (label, False, f"field {field_name!r} missing in {rec.raw[:120]}")
            if callable(expected_value):
                ok = bool(expected_value(actual))
                detail = f"actual={actual!r} predicate_ok={ok}"
            else:
                ok = actual == str(expected_value)
                detail = f"actual={actual!r} expected={expected_value!r}"
            return (label, ok, detail)
    return (label, False, "no record matched event_pattern")


def assert_counter(
    records: Iterable[LogRecord],
    pattern: str | Pattern,
    expected: int,
    name: Optional[str] = None,
) -> Assertion:
    """Assert that pattern matches exactly `expected` records."""
    regex = _compile(pattern)
    matches = [r for r in records if regex.search(r.raw)]
    n = len(matches)
    label = name or f"counter:{regex.pattern[:60]}"
    ok = n == expected
    return (label, ok, f"n={n} expected={expected}")


def assert_sequence_in_order(
    records: Iterable[LogRecord],
    patterns: Sequence[str | Pattern],
    name: Optional[str] = None,
) -> Assertion:
    """Assert each pattern matches a record, matches occurring in increasing order.

    This is the primary God Object move-only non-regression primitive (decision
    D-GOD-01): per mode, a short ordered list of milestone patterns. Records
    between milestones are ignored, so high-frequency noise (paint logs), worker
    interleaving and log rotation do not cause false failures. Only the relative
    order of the named milestones is enforced.
    """
    rec_list = list(records)
    label = name or f"in_order:{len(patterns)}_milestones"
    search_from = 0
    matched_at: List[int] = []
    for i, pattern in enumerate(patterns):
        regex = _compile(pattern)
        found = None
        for j in range(search_from, len(rec_list)):
            if regex.search(rec_list[j].raw):
                found = j
                break
        if found is None:
            return (
                label, False,
                f"milestone[{i}] /{regex.pattern[:60]}/ not found after idx "
                f"{search_from} (matched_so_far={matched_at})",
            )
        matched_at.append(found)
        search_from = found + 1
    return (label, True, f"{len(patterns)} milestones in order at idx={matched_at}")


def diff_against_golden(
    records: Iterable[LogRecord],
    golden_id: str,
    name: Optional[str] = None,
    **extract_opts,
) -> Assertion:
    """Assert the main-thread milestone sequence matches golden_logs/<id>.golden.

    Proves behaviour preservation across a God Object extraction phase: any
    lost, extra or reordered milestone fails the assertion. `extract_opts` are
    forwarded to `golden.extract_sequence` (main_thread_only, levels, include).
    """
    from .golden import extract_sequence, read_golden, compare_sequences
    label = name or f"golden:{golden_id}"
    try:
        golden = read_golden(golden_id)
    except FileNotFoundError:
        return (label, False, f"golden missing: run capture for {golden_id}")
    current = extract_sequence(records, **extract_opts)
    ok, detail = compare_sequences(current, golden)
    return (label, ok, detail)


@dataclass
class AssertionSummary:
    total: int
    passed: int
    failed: int
    verdict: str  # "PASS" | "FAIL"


def summarize(assertions: Iterable[Assertion]) -> AssertionSummary:
    items = list(assertions)
    passed = sum(1 for _, ok, _ in items if ok)
    failed = len(items) - passed
    verdict = "PASS" if failed == 0 and items else ("FAIL" if items else "FAIL")
    return AssertionSummary(total=len(items), passed=passed, failed=failed, verdict=verdict)


__all__ = [
    "Assertion",
    "AssertionSummary",
    "assert_log_contains",
    "assert_log_absent",
    "assert_no_log_between",
    "assert_field_value",
    "assert_counter",
    "assert_sequence_in_order",
    "diff_against_golden",
    "summarize",
]


def _selftest() -> int:
    """Prove assert_sequence_in_order detects order, gaps tolerance, and misses."""
    from .parse_log import parse_line

    def mk(msgs):
        out = []
        for i, m in enumerate(msgs):
            r = parse_line(
                f"2026-06-21T17:00:0{i % 10}.000 [INFO   ] [MainThread     ] {m}")
            if r is not None:
                out.append(r)
        return out

    recs = mk([
        "recover_and_load: START",
        "timeline: paint noise",          # noise between milestones
        "_on_search_complete: 3 events",
        "restore_selected_data: applied",
        "_on_event_restore_done: ok",
    ])
    milestones = [
        r"recover_and_load: START",
        r"_on_search_complete",
        r"restore_selected_data",
        r"_on_event_restore_done",
    ]
    cases = []
    _, ok, _ = assert_sequence_in_order(recs, milestones)
    cases.append(("in_order_with_noise_passes", ok is True))
    # missing milestone -> FAIL
    _, ok, _ = assert_sequence_in_order(mk(["recover_and_load: START", "_on_event_restore_done: ok"]), milestones)
    cases.append(("missing_milestone_fails", ok is False))
    # out of order -> FAIL (expect restore before search, but log has search first)
    _, ok, _ = assert_sequence_in_order(recs, [r"restore_selected_data", r"recover_and_load: START"])
    cases.append(("out_of_order_fails", ok is False))

    all_ok = all(p for _, p in cases)
    for n, p in cases:
        print(f"[assert_log-selftest] {'PASS' if p else 'FAIL'} {n}")
    print(f"[assert_log-selftest] VERDICT={'PASS' if all_ok else 'FAIL'} "
          f"note={'assertion_can_fail' if all_ok else 'BROKEN'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        raise SystemExit(_selftest())
