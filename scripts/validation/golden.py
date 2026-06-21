"""Golden-log capture and comparison for RecoverLand validation.

BL-DIAG-P2-14 / Phase 0. The README of scripts/validation/ promised a
`diff_against_golden` helper and a versioned `golden_logs/` directory; neither
existed. This module implements both.

Principle (see docs/backlog_god_object_recover_dialog_2026-06-21.md section 7):
behaviour is the ORDERED SEQUENCE of main-thread log milestones. A golden log is
that sequence captured on a reference run; after each extraction phase the same
scenario is replayed and its sequence must match the golden byte-for-byte (after
masking volatile tokens: trace ids, uuids, durations, addresses).

No QGIS dependency: operates on parsed `LogRecord` objects, so the comparison
logic is unit-testable offline (`python -m scripts.validation.golden --selftest`).
"""
from __future__ import annotations

import difflib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from .parse_log import LogRecord, log_file_size, parse_line, read_records

_HERE = Path(__file__).resolve().parent
_GOLDEN_DIR = _HERE / "golden_logs"
_DEFAULT_LEVELS = frozenset({"INFO", "WARNING", "ERROR", "CRITICAL"})

# Volatile tokens masked before comparison. Order matters (specific first).
_MASKS: Tuple[Tuple[re.Pattern, str], ...] = (
    (re.compile(r"\[[0-9a-f]{8}\]"), "[TID]"),               # [abc12345] trace id prefix
    (re.compile(r"\b[0-9a-f]{32}\b"), "UUID"),               # bare 32-hex uuid
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f-]{27}\b"), "UUID"),  # dashed uuid
    (re.compile(r"0x[0-9a-fA-F]+"), "ADDR"),                 # memory address
    (re.compile(r"elapsed_ms=\d+"), "elapsed_ms=N"),
    (re.compile(r"\b\d+(?:\.\d+)?ms\b"), "Nms"),             # "123ms", "12.0ms"
    (re.compile(r"\bin \d+ms\b"), "in Nms"),
)


def normalize_message(message: str) -> str:
    """Mask volatile tokens so two runs of the same scenario compare equal."""
    out = message
    for pattern, repl in _MASKS:
        out = pattern.sub(repl, out)
    return out.strip()


def extract_sequence(
    records: Iterable[LogRecord],
    *,
    main_thread_only: bool = True,
    levels: frozenset = _DEFAULT_LEVELS,
    include: Optional[str] = None,
) -> List[str]:
    """Reduce records to the ordered list of normalized milestone signatures.

    Worker-thread lines are dropped by default: their interleaving is
    non-deterministic. Qt slots run on MainThread, so milestone callbacks
    (_on_stats_ready, _on_version_restore_done, ...) are preserved.
    """
    include_re = re.compile(include) if include else None
    seq: List[str] = []
    for rec in records:
        if main_thread_only and rec.thread != "MainThread":
            continue
        if rec.level not in levels:
            continue
        if include_re is not None and not include_re.search(rec.message):
            continue
        seq.append(normalize_message(rec.message))
    return seq


def golden_path(golden_id: str) -> Path:
    return _GOLDEN_DIR / f"{golden_id}.golden"


def write_golden(golden_id: str, sequence: Sequence[str]) -> Path:
    """Persist a captured sequence as the versioned reference."""
    _GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    path = golden_path(golden_id)
    header = [
        f"# golden_id={golden_id}",
        f"# captured_at={datetime.now(timezone.utc).isoformat()}",
        f"# n_events={len(sequence)}",
    ]
    path.write_text("\n".join(header + list(sequence)) + "\n", encoding="utf-8")
    return path


def read_golden(golden_id: str) -> List[str]:
    """Load a reference sequence, skipping comment/header lines."""
    path = golden_path(golden_id)
    if not path.exists():
        raise FileNotFoundError(f"golden not found: {path}")
    return [
        line.rstrip("\n")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    ]


def compare_sequences(current: Sequence[str], golden: Sequence[str]) -> Tuple[bool, str]:
    """Compare two sequences; return (ok, human-readable diff summary)."""
    if list(current) == list(golden):
        return True, f"sequences identical n={len(golden)}"

    missing = [e for e in golden if e not in current]   # in golden, lost now
    extra = [e for e in current if e not in golden]      # appeared now
    diff_lines = list(difflib.unified_diff(
        list(golden), list(current),
        fromfile="golden", tofile="current", lineterm="", n=1,
    ))
    sample = " | ".join(diff_lines[:8])
    detail = (
        f"DIVERGENCE n_golden={len(golden)} n_current={len(current)} "
        f"missing={len(missing)} extra={len(extra)}"
    )
    if missing:
        detail += f" first_missing=`{missing[0][:100]}`"
    if extra:
        detail += f" first_extra=`{extra[0][:100]}`"
    if sample:
        detail += f" diff=`{sample[:300]}`"
    return False, detail


# --- Capture API for the QGIS console (the only step that needs a live run) ---

def capture_start(log_path: Optional[Path] = None) -> int:
    """Return the current log size; pass it to capture_finish after the run."""
    from .runner import _resolve_log_path  # lazy: avoids QGIS import offline
    path = log_path or _resolve_log_path()
    return log_file_size(path)


def capture_finish(
    golden_id: str,
    start_offset: int,
    log_path: Optional[Path] = None,
    **opts,
) -> Path:
    """Read the log window since start_offset and write the golden reference."""
    from .runner import _resolve_log_path
    path = log_path or _resolve_log_path()
    records = read_records(path, start_offset=start_offset)
    sequence = extract_sequence(records, **opts)
    out = write_golden(golden_id, sequence)
    print(f"[golden] captured id={golden_id} n_events={len(sequence)} path={out}")
    return out


# --- Antithese: prove the comparator can FAIL (no QGIS) ---

def _mk_records(lines: Sequence[str]) -> List[LogRecord]:
    out = []
    for i, msg in enumerate(lines):
        raw = f"2026-06-21T16:00:0{i % 10}.000 [INFO   ] [MainThread     ] {msg}"
        rec = parse_line(raw)
        if rec is not None:
            out.append(rec)
    return out


def _selftest() -> int:
    base = [
        "[abc12345] recover_and_load: START",
        "_on_search_complete: 3 events",
        "restore_selected_data: applied in 42ms",
        "_on_event_restore_done: ok",
    ]
    golden = extract_sequence(_mk_records(base))

    cases = []
    # 1. identical run (different trace id + duration) -> PASS
    same = [
        "[def67890] recover_and_load: START",
        "_on_search_complete: 3 events",
        "restore_selected_data: applied in 99ms",
        "_on_event_restore_done: ok",
    ]
    ok, _ = compare_sequences(extract_sequence(_mk_records(same)), golden)
    cases.append(("identical_after_mask", ok is True))
    # 2. a milestone lost (regression) -> FAIL
    lost = [base[0], base[1], base[3]]
    ok, detail = compare_sequences(extract_sequence(_mk_records(lost)), golden)
    cases.append(("missing_milestone_detected", ok is False and "missing=1" in detail))
    # 3. an extra milestone (double wiring) -> FAIL
    extra = base + ["_on_event_restore_done: ok"]
    ok, detail = compare_sequences(extract_sequence(_mk_records(extra)), golden)
    cases.append(("extra_milestone_detected", ok is False and "extra=" in detail))
    # 4. reordered -> FAIL
    swapped = [base[1], base[0], base[2], base[3]]
    ok, _ = compare_sequences(extract_sequence(_mk_records(swapped)), golden)
    cases.append(("reorder_detected", ok is False))
    # 5. worker-thread noise ignored -> still PASS
    noisy = _mk_records(base)
    extra_worker = parse_line(
        "2026-06-21T16:00:05.000 [DEBUG  ] [Dummy-7        ] worker chatter n=999")
    if extra_worker:
        noisy.insert(2, extra_worker)
    ok, _ = compare_sequences(extract_sequence(noisy), golden)
    cases.append(("worker_noise_ignored", ok is True))

    all_ok = all(passed for _, passed in cases)
    for name, passed in cases:
        print(f"[golden-selftest] {'PASS' if passed else 'FAIL'} {name}")
    print(f"[golden-selftest] VERDICT={'PASS' if all_ok else 'FAIL'} "
          f"note={'comparator_can_fail' if all_ok else 'COMPARATOR_BROKEN'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        raise SystemExit(_selftest())
    print("usage: python -m scripts.validation.golden --selftest")
    print("capture is done from the QGIS console via capture_start/capture_finish")
    raise SystemExit(0)
