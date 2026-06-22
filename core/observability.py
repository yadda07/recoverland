"""Surgical observability for the rewind/restore lifecycle.

Goal: make the log self-explanatory so a reader can reconstruct what
happened without needing to read the code. Three primitives:

1. ``CycleStats`` — accumulator for stats during a rewind/undo cycle.
   Use ``record(key, value)`` or ``add(key, n=1)`` from each phase, then
   ``finalize(cycle_name, elapsed_ms)`` at the end emits one
   ``CYCLE_SUMMARY`` line followed by ``CYCLE_ANOMALY`` lines for any
   suspicious pattern.

2. ``log_state_transition`` — log every change of a critical flag in
   the dialog state machine so we correlate UI state with runtime.
   Stuck flags (``_is_recovering`` left True after crash) are visible.

3. ``assert_invariant`` — log CRITICAL when a documented invariant is
   violated (e.g. trace event without entity_fingerprint), with a short
   stack hint. Returns the condition so callers can early-return.

Discipline:
- One ``CYCLE_SUMMARY`` per cycle. Aggregates everything.
- Anomalies: one line per anomaly, grouped under ``CYCLE_ANOMALY``.
- State transitions: one line per change, prefixed ``STATE``.
- Invariant violations: prefixed ``INVARIANT_VIOLATED`` at CRITICAL.
- No per-event noise here. Per-event details stay in their own logs.
"""
from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Dict, List, Tuple

from .logger import flog


# ----------------------------- Cycle stats ----------------------------- #

# Canonical stat keys (extend as new phases are instrumented):
#   raw                  — events fetched from DB before any filtering
#   user                 — user events after splitting traces out
#   traces               — total trace events seen in fetch
#   traces_invalidated   — traces with invalidated_at != None
#   traces_active        — traces - traces_invalidated
#   dedup_active         — events surviving dedup (planner input)
#   dedup_dropped        — events neutralized by traces or fused chains
#   dedup_redundant      — chain collapses (INSERT->...->DELETE no-op)
#   plan_actions         — actions emitted by the planner
#   plan_skipped         — events the planner refused (drift, no PK, ...)
#   apply_ok             — actions that succeeded in the runner
#   apply_skipped        — actions skipped at runtime (RW-08, GEOS, ...)
#   apply_fail           — hard failures (commit, capability, exception)
#   traces_written       — trace events enqueued for the journal


class CycleStats:
    """Accumulator for one rewind/undo/event_restore cycle.

    Thread-safe additions; only one ``finalize`` per instance.
    """

    def __init__(self, trace_id: str = "") -> None:
        self.trace_id = trace_id
        self._stats: Dict[str, Any] = {}
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self._finalized = False

    def record(self, key: str, value: Any) -> None:
        """Set or replace a stat value (last writer wins)."""
        with self._lock:
            self._stats[key] = value

    def add(self, key: str, n: int = 1) -> None:
        """Increment an integer counter (defaults to 0)."""
        with self._lock:
            self._stats[key] = int(self._stats.get(key, 0)) + n

    def merge(self, **kwargs: Any) -> None:
        """Bulk record several values."""
        with self._lock:
            self._stats.update(kwargs)

    def snapshot(self) -> Dict[str, Any]:
        """Return a shallow copy of the current stats."""
        with self._lock:
            return dict(self._stats)

    def finalize(self, cycle: str, elapsed_ms: int = 0) -> Dict[str, Any]:
        """Emit ``CYCLE_SUMMARY`` + anomalies. Idempotent.

        Returns the final stats dict so callers can also propagate it.
        """
        with self._lock:
            if self._finalized:
                return dict(self._stats)
            self._finalized = True
            stats = dict(self._stats)
        if not elapsed_ms:
            elapsed_ms = int((time.monotonic() - self._t0) * 1000)
        log_cycle_summary(self.trace_id, cycle, stats, elapsed_ms)
        return stats


# ------------------------- Anomaly detection -------------------------- #


_ANOMALY_RULES: List[Tuple[str, str]] = []


def _i(stats: Dict[str, Any], key: str) -> int:
    """Coerce a stat to int, treating missing/None as 0."""
    val = stats.get(key)
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


def _detect_anomalies(
    cycle: str, stats: Dict[str, Any],
) -> List[Tuple[str, str, str]]:
    """Pattern-match suspicious cycle outcomes.

    Returns a list of (severity, code, message) tuples. Severities are
    INFO, WARNING, ERROR, CRITICAL — passed straight to flog.
    """
    anomalies: List[Tuple[str, str, str]] = []

    plan_actions = _i(stats, "plan_actions")
    apply_ok = _i(stats, "apply_ok")
    apply_skipped = _i(stats, "apply_skipped")
    apply_fail = _i(stats, "apply_fail")
    user = _i(stats, "user")
    traces = _i(stats, "traces")
    traces_invalidated = _i(stats, "traces_invalidated")
    dedup_active = _i(stats, "dedup_active")
    dedup_dropped = _i(stats, "dedup_dropped")
    traces_written = _i(stats, "traces_written")

    active_traces = max(traces - traces_invalidated, 0)

    # 1. Everything skipped: classic "rewind already applied" symptom.
    if all((cycle == "rewind", plan_actions > 0, apply_ok == 0,
            apply_skipped == plan_actions)):
        anomalies.append((
            "WARNING", "all_skipped",
            f"all {plan_actions} action(s) skipped at apply phase. "
            "Likely cause: previous rewind not undone (auto-undo skipped "
            "or failed), schema drift, or external modification of the "
            "layer between fetch and apply. Check STATE logs for "
            "_last_restore_by_ds and _is_recovering around this cycle.",
        ))

    # 2. Rewind landed in an empty plan but user events existed: dedup
    # ate everything. Either we are at target state or fingerprint drift.
    # Guard with user > 0 so cycles that never ran dedup (event_restore,
    # undo) don't trigger a false positive.
    if cycle == "rewind" and user > 0 and dedup_active == 0:
        if active_traces > 0:
            anomalies.append((
                "INFO", "already_at_target",
                f"all {user} user event(s) neutralized by "
                f"{active_traces} trace(s). System is at target "
                "state for this cutoff; nothing to do.",
            ))
        else:
            anomalies.append((
                "WARNING", "all_collapsed_no_traces",
                f"{user} user event(s) collapsed to 0 with no active "
                "traces. Possible cause: every entity has an "
                "INSERT->DELETE chain (created+destroyed after cutoff) "
                "or all events are no-ops. Verify cutoff selection.",
            ))

    # 3. Active traces exist but very few user events neutralized:
    # likely fingerprint drift between trace and event.
    if all((cycle == "rewind", active_traces >= 5,
            dedup_dropped < active_traces // 2)):
        anomalies.append((
            "WARNING", "low_neutralization",
            f"low neutralization ratio: {active_traces} active "
            f"trace(s) only neutralized {dedup_dropped} user "
            "event(s). Possible causes: entity_fingerprint drift, "
            "trace pointing at a fused/synthetic event_id no longer "
            "in the user chain, or trace cutoff filter mismatch.",
        ))

    # 4. Apply succeeded but no traces written: next rewind will see
    # the same events as un-restored.
    if cycle == "rewind" and apply_ok > 0 and traces_written == 0:
        anomalies.append((
            "ERROR", "missing_traces",
            f"{apply_ok} action(s) succeeded but ZERO trace event(s) "
            "written to the journal. The next rewind will re-attempt "
            "these events and likely produce post-state mismatches "
            "(RW-08 SKIP cascade). Check WriteQueue health and "
            "build_restore_trace_event return values.",
        ))

    # 5. Hard failures: feature-level inconsistency risk.
    if apply_fail > 0:
        anomalies.append((
            "WARNING", "apply_failures",
            f"{apply_fail} action(s) hard-failed (not skipped). "
            "Features may be in an inconsistent state. Review "
            "individual ERROR logs for this cycle.",
        ))

    # 6. Undo with partial success: we just shipped a fix for trace
    # invalidation; any partial undo deserves a reminder.
    if cycle in ("undo", "auto_undo") and apply_ok > 0 and apply_fail > 0:
        anomalies.append((
            "WARNING", "undo_partial",
            f"undo partially succeeded ({apply_ok} ok / "
            f"{apply_fail} fail). Traces for failed events are "
            "invalidated so a follow-up rewind can retry; traces "
            "for succeeded events stay active so dedup neutralizes "
            "them. Check undo_done log line for the eid lists.",
        ))

    return anomalies


# ----------------------------- Public API ----------------------------- #


_SUMMARY_KEYS_ORDER: Tuple[str, ...] = (
    "raw", "user",
    "traces", "traces_invalidated", "traces_active",
    "dedup_active", "dedup_dropped", "dedup_redundant",
    "plan_actions", "plan_skipped",
    "apply_ok", "apply_skipped", "apply_fail",
    "traces_written",
)


def log_cycle_summary(
    trace_id: str, cycle: str,
    stats: Dict[str, Any], elapsed_ms: int = 0,
) -> None:
    """Emit one ``CYCLE_SUMMARY`` line + anomalies for a cycle.

    cycle: short name ("rewind", "undo", "auto_undo", "event_restore").
    stats: bag of counters / values; only canonical keys appear in the
    summary, the rest are dropped to keep the line greppable.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    parts: List[str] = [f"cycle={cycle}"]
    for key in _SUMMARY_KEYS_ORDER:
        if key in stats:
            parts.append(f"{key}={stats[key]}")
    # Surface unknown but present keys at the tail for forward
    # compatibility (new phase instrumentation without code change).
    extra_keys = sorted(k for k in stats.keys() if k not in _SUMMARY_KEYS_ORDER)
    for key in extra_keys:
        parts.append(f"{key}={stats[key]}")
    if elapsed_ms:
        parts.append(f"elapsed_ms={elapsed_ms}")
    flog(f"{prefix}CYCLE_SUMMARY {' '.join(parts)}")

    for severity, code, msg in _detect_anomalies(cycle, stats):
        flog(f"{prefix}CYCLE_ANOMALY {cycle} code={code} {msg}", severity)


def log_state_transition(
    component: str, attr: str,
    old: Any, new: Any, **ctx: Any,
) -> None:
    """Log a state machine transition with a context payload.

    Skips no-op writes (old == new). Keep this cheap so callers can wrap
    every assignment of a critical flag without thinking about it.
    """
    if old == new:
        return
    ctx_str = " ".join(f"{k}={v}" for k, v in ctx.items())
    suffix = f" {ctx_str}" if ctx_str else ""
    flog(f"STATE {component}.{attr}: {_short(old)} -> {_short(new)}{suffix}")


def _short(value: Any) -> str:
    """Compact repr for state transitions, capping long collections."""
    if value is None:
        return "None"
    if isinstance(value, bool):
        return repr(value)
    if isinstance(value, (int, float, str)):
        return repr(value)
    if isinstance(value, dict):
        return f"dict(n={len(value)})"
    if isinstance(value, (list, tuple, set)):
        return f"{type(value).__name__}(n={len(value)})"
    return type(value).__name__


def assert_invariant(condition: bool, name: str, **ctx: Any) -> bool:
    """Log a CRITICAL line when ``condition`` is False.

    Returns the condition so callers can:

        if not assert_invariant(x is not None, "x_required", trace=tid):
            return None

    The log captures the immediate caller frame as a short location
    hint so the violation can be triaged without a full stack dump.
    """
    if condition:
        return True
    ctx_str = " ".join(f"{k}={v}" for k, v in ctx.items())
    try:
        frame = traceback.extract_stack(limit=3)[-2]
        loc = f"{frame.filename}:{frame.lineno} in {frame.name}"
    except Exception:  # pragma: no cover - defensive only
        loc = "unknown"
    suffix = f" {ctx_str}" if ctx_str else ""
    flog(f"INVARIANT_VIOLATED name={name} at={loc}{suffix}", "CRITICAL")
    return False
