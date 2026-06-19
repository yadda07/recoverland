"""log_format_audit - RecoverLand validation runtime.

Invariant: I-LOG (logs structurés key=value pour permettre le croisement
automatique entre logs et rapports).
Backlog item: BL-RW-P1-06.
Root cause: CR-7.

Pre-patch state:
    - core.logger only exposes flog(message, level). No structured API.
    - core.restore_executor.py emits free-form text like
      "BUF_INS eid=12345 fp=fid:42 ..." which has key=value-ish style
      but no uniform `level=`, `module=`, `event=` prefix.

Post-patch state:
    - core.logger exposes flog_kv(level, event, *, module, **fields).
    - The three BUF_INS / BUF_UPD / BUF_DEL emission sites in
      core/restore_executor.py use flog_kv.

Scenario (brutal, runs hors-QGIS aussi):
    1. Snapshot the log file size before the test.
    2. Try to import flog_kv. If absent, the run() returns early with
       flog_kv_exists=False -> all runtime assertions fail.
    3. Emit three structured records covering the round-trip cases:
        - basic types (int, str)
        - value containing a space (escape mandatory)
        - value containing double quotes (escape mandatory)
    4. Force the file handler to flush so the records are on disk.
    5. Re-read the log file from the saved offset, parse via
       scripts.validation.parse_log.iter_records, isolate the test
       records (event starts with TEST_KV_) and assert each round-trip.
    6. Source-level guards: ensure restore_executor.py emits each of
       BUF_INS / BUF_UPD / BUF_DEL through flog_kv.

Pre-patch verdict: FAIL.
Post-patch verdict: PASS.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

SCENARIO_ID = "log_format_audit"
INVARIANT = "I-LOG"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _flush_file_handlers() -> None:
    """Force the rotating file handler to flush so we can re-read the file."""
    file_logger = logging.getLogger("RecoverLand.FileDebug")
    for h in file_logger.handlers:
        try:
            h.flush()
        except Exception:
            pass


def setup(ctx):
    from recoverland.core.logger import _LOG_FILE, flog
    from scripts.validation.parse_log import log_file_size

    offset_pre = log_file_size(_LOG_FILE)
    ctx.data["log_file"] = _LOG_FILE
    ctx.data["offset_pre"] = offset_pre

    flog(
        f"log_format_audit setup: trace_id={ctx.trace_id} "
        f"log_file={_LOG_FILE} offset_pre={offset_pre}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.parse_log import iter_records

    flog(f"log_format_audit run start: trace_id={ctx.trace_id}", "INFO")

    # Test 1: try to import flog_kv (the API under test).
    flog_kv = None
    try:
        from recoverland.core.logger import flog_kv as _flog_kv
        flog_kv = _flog_kv
    except ImportError:
        flog_kv = None
    ctx.data["flog_kv_exists"] = flog_kv is not None

    if flog_kv is None:
        flog(
            f"log_format_audit: flog_kv NOT exported by recoverland.core.logger "
            f"trace_id={ctx.trace_id}",
            "WARNING",
        )
        ctx.data["test_records"] = []
        ctx.data["records_total"] = 0
        return

    # Test 2: emit three structured records covering escape rules.
    flog_kv("INFO", "TEST_KV_BASIC", module="log_format_audit",
            k1="v1", k2=42)
    flog_kv("INFO", "TEST_KV_SPACES", module="log_format_audit",
            path="/tmp/with space/x")
    flog_kv("WARNING", "TEST_KV_QUOTES", module="log_format_audit",
            msg='He said "hi"')

    _flush_file_handlers()

    # Test 3: re-read the log file and isolate the test records.
    all_records = list(iter_records(
        ctx.data["log_file"],
        start_offset=ctx.data["offset_pre"],
    ))
    test_records = [
        r for r in all_records
        if (r.fields.get("event") or "").startswith("TEST_KV_")
    ]
    ctx.data["test_records"] = test_records
    ctx.data["records_total"] = len(all_records)

    flog(
        f"log_format_audit run end: trace_id={ctx.trace_id} "
        f"records_total={len(all_records)} "
        f"test_records={len(test_records)} "
        f"events={[r.fields.get('event') for r in test_records]}",
        "INFO",
    )


_BUF_EVENTS = ("BUF_INS", "BUF_UPD", "BUF_DEL")


def _check_buf_event_uses_flog_kv(event_name: str) -> tuple[bool, str]:
    """Return (ok, msg). Looks for a flog_kv call carrying event=event_name.

    Accepts both styles for the event argument:
        flog_kv("INFO", "BUF_INS", ...)        # 2nd positional argument
        flog_kv("INFO", event="BUF_INS", ...)  # keyword argument
    """
    rel = Path("core/restore_executor.py")
    full = _PLUGIN_ROOT / rel
    if not full.is_file():
        return False, f"missing file: {rel}"
    text = full.read_text(encoding="utf-8", errors="replace")
    # Match flog_kv( <something> "BUF_X" or flog_kv( <something> event="BUF_X"
    regex = re.compile(
        rf'flog_kv\s*\(\s*'                       # flog_kv(
        rf'(?:["\'][^"\']+["\']|\w+)\s*,\s*'      # 1st arg (level), comma
        rf'(?:event\s*=\s*)?'                     # optional event= keyword
        rf'["\']{event_name}["\']'                # the event literal
    )
    if regex.search(text):
        return True, f"flog_kv(... event={event_name!r} ...) present in {rel}"
    return False, f"flog_kv(... event={event_name!r} ...) absent in {rel}"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []

    # ===== API surface =================================================
    out.append((
        "flog_kv_api_exists",
        ctx.data.get("flog_kv_exists") is True,
        f"flog_kv_exists={ctx.data.get('flog_kv_exists')} expected=True "
        f"(recoverland.core.logger must export flog_kv)",
    ))

    # ===== Round-trip emit -> parse ====================================
    test_records = ctx.data.get("test_records") or []
    out.append((
        "three_test_records_emitted_and_parsed",
        len(test_records) >= 3,
        f"len(test_records)={len(test_records)} expected>=3",
    ))

    by_event = {r.fields.get("event"): r for r in test_records}

    basic = by_event.get("TEST_KV_BASIC")
    out.append((
        "basic_kv_module_field",
        basic is not None and basic.fields.get("module") == "log_format_audit",
        f"basic.module={basic.fields.get('module') if basic else 'MISSING'} "
        f"expected=log_format_audit",
    ))
    out.append((
        "basic_kv_k1_value",
        basic is not None and basic.fields.get("k1") == "v1",
        f"basic.k1={basic.fields.get('k1') if basic else 'MISSING'} expected=v1",
    ))
    out.append((
        "basic_kv_k2_int_serialized",
        basic is not None and basic.fields.get("k2") == "42",
        f"basic.k2={basic.fields.get('k2') if basic else 'MISSING'} "
        f"expected='42' (int -> str round-trip)",
    ))

    spaces = by_event.get("TEST_KV_SPACES")
    spaces_path = spaces.fields.get("path") if spaces else None
    out.append((
        "spaces_path_round_trip",
        spaces_path == "/tmp/with space/x",
        f"spaces.path={spaces_path!r} expected='/tmp/with space/x' "
        f"(value with space must round-trip via quote escape)",
    ))

    quotes = by_event.get("TEST_KV_QUOTES")
    quotes_msg = quotes.fields.get("msg") if quotes else None
    out.append((
        "quotes_msg_round_trip",
        quotes_msg == 'He said "hi"',
        f"quotes.msg={quotes_msg!r} expected='He said \"hi\"' "
        f"(value with quote must round-trip via backslash escape)",
    ))

    # ===== Source guards: BUF_* sites use flog_kv =======================
    for event_name in _BUF_EVENTS:
        ok, msg = _check_buf_event_uses_flog_kv(event_name)
        out.append((f"source__{event_name}_uses_flog_kv", ok, msg))

    # ===== Trace propagation ===========================================
    out.append(assert_log_contains(
        ctx.records,
        rf"log_format_audit.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
