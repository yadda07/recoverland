# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Benchmark: what does it cost to make a captured edit durable?

Measures, on the real journal schema and realistic UPDATE events, the
main-thread cost of each way an accepted event can be handed over, and
the time until it is really durable:

  wq_async     current WriteQueue.enqueue (RAM queue + writer thread);
               ``ui_ms`` is what the GUI thread pays, ``durable_ms`` is
               the time until the rows are committed by the writer.
  sync_normal  one INSERT transaction on the calling thread, WAL +
               synchronous=NORMAL (survives a QGIS crash, not a power cut)
  sync_full    same with synchronous=FULL (survives a power cut too)
  jsonl_fsync  append-only JSON Lines outbox + flush + os.fsync
  pending_rmw  the current save_pending_events (read, rewrite, no fsync)

The journal is pre-filled so index maintenance costs what it costs on a
journal that has lived (the user's own is 208 MB).

Usage (QGIS python, no QgsApplication needed)::

    python-qgis.bat scripts/validation/bench_durability.py [--prefill 100000]

Prints a table and writes ``reports/bench_durability_<ts>.json``.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sqlite3
import statistics
import struct
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PLUGIN_ROOT = _HERE.parents[1]
for _extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)

SIZES = (1, 10, 100, 1000, 10000, 100000)


def _polygon_wkb(n_vertices: int, dx: float) -> bytes:
    pts = []
    for i in range(n_vertices):
        pts.append((700000.0 + dx + i, 6600000.0 + (i % 7)))
    pts.append(pts[0])
    out = struct.pack("<BII", 1, 3, 1) + struct.pack("<I", len(pts))
    for x, y in pts:
        out += struct.pack("<dd", x, y)
    return out


def _event(i: int, session: str, created_at: str):
    from recoverland.core.audit_backend import AuditEvent
    attrs = {
        "changed_only": {
            "nom": {"old": f"parcelle {i}", "new": f"parcelle {i} bis"},
            "surface": {"old": 1234.5 + i, "new": 1300.0 + i},
        }
    }
    schema_json = (
        '[{"name":"id","type":"int"},{"name":"nom","type":"string"},'
        '{"name":"surface","type":"double"}]'
    )
    return AuditEvent(
        event_id=None,
        project_fingerprint="bench_project",
        datasource_fingerprint="ogr::c:/bench/parcelles.gpkg|layername=parcelles",
        layer_id_snapshot="parcelles_bench",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": i, "pk": {"id": i}}),
        operation_type="UPDATE",
        attributes_json=json.dumps(attrs),
        geometry_wkb=_polygon_wkb(40, 0.0),
        geometry_type="Polygon",
        crs_authid="EPSG:2154",
        field_schema_json=schema_json,
        user_name="bench",
        session_id=session,
        created_at=created_at,
        restored_from_event_id=None,
        entity_fingerprint=f"pk:id={i}",
        event_schema_version=6,
        new_geometry_wkb=_polygon_wkb(40, 0.5),
        invalidated_at=None,
    )


def _insert_rows(conn, events) -> None:
    """Insert *events* with whatever insert path the checkout has.

    The benchmark runs on both sides of the v7 rewrite: before it, the
    write queue exposed its INSERT statement; since, rows go through
    ``journal_writes.insert_events_idempotent`` (keyed by event_uid).
    """
    try:
        from recoverland.core.write_queue import _INSERT_SQL, _event_to_row
    except ImportError:
        from recoverland.core.journal_writes import (
            ensure_v7_schema, insert_events_idempotent, event_row)
        ensure_v7_schema(conn)
        insert_events_idempotent(conn, [event_row(e) for e in events])
        return
    conn.executemany(_INSERT_SQL, [_event_to_row(e) for e in events])


def _make_journal(path: str, prefill: int) -> None:
    from recoverland.core.sqlite_schema import initialize_schema
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    if prefill:
        now = datetime.now(timezone.utc).isoformat()
        chunk = [_event(i, "prefill", now) for i in range(1000)]
        with conn:
            for _ in range(prefill // 1000):
                _insert_rows(conn, chunk)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()


def _rows(path: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
    finally:
        conn.close()


def bench_wq_async(path: str, events) -> dict:
    from recoverland.core.write_queue import WriteQueue
    before = _rows(path)
    wq = WriteQueue()
    wq.start(path)
    time.sleep(0.2)
    t0 = time.perf_counter()
    ok = wq.enqueue(events)
    t_ui = time.perf_counter() - t0
    if not ok:
        # Refused (hard limit): the batch went to the pending file and, in
        # the plugin, capture deactivates itself. Nothing will reach the
        # journal, so there is no durable time to wait for.
        wq.stop()
        try:
            os.remove(os.path.join(os.path.dirname(path), "recoverland_pending.json"))
        except OSError:
            pass
        return {"ui_ms": t_ui * 1000, "durable_ms": float("nan"), "ok": False}
    target = before + len(events)
    deadline = time.perf_counter() + 600
    while _rows(path) < target and time.perf_counter() < deadline:
        time.sleep(0.002)
    t_durable = time.perf_counter() - t0
    wq.stop()
    return {"ui_ms": t_ui * 1000, "durable_ms": t_durable * 1000, "ok": bool(ok)}


def _bench_sync(path: str, events, synchronous: str) -> dict:
    from recoverland.core.sqlite_schema import apply_pragmas
    conn = sqlite3.connect(path)
    apply_pragmas(conn)
    conn.execute(f"PRAGMA synchronous={synchronous}")
    t0 = time.perf_counter()
    with conn:
        _insert_rows(conn, events)
    dt = time.perf_counter() - t0
    conn.close()
    return {"ui_ms": dt * 1000, "durable_ms": dt * 1000, "ok": True}


def bench_sync_normal(path, events):
    return _bench_sync(path, events, "NORMAL")


def bench_sync_full(path, events):
    return _bench_sync(path, events, "FULL")


def bench_jsonl_fsync(path, events):
    out = os.path.join(os.path.dirname(path), "outbox.jsonl")
    t0 = time.perf_counter()
    lines = []
    for e in events:
        d = e._asdict()
        for k in ("geometry_wkb", "new_geometry_wkb"):
            if isinstance(d.get(k), (bytes, bytearray)):
                d[k] = "b64:" + base64.b64encode(d[k]).decode("ascii")
        lines.append(json.dumps(d, ensure_ascii=False))
    with open(out, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    dt = time.perf_counter() - t0
    return {"ui_ms": dt * 1000, "durable_ms": dt * 1000, "ok": True}


def bench_outbox_v7(path, events):
    """What the GUI thread pays in v7 before a commit is acknowledged:
    one fsync'd append of the batch to the capture outbox."""
    try:
        from recoverland.core.capture_outbox import CaptureOutbox, event_to_record
    except ImportError:
        return {"ui_ms": float("nan"), "durable_ms": float("nan"), "ok": False}
    ob = CaptureOutbox(path)
    t0 = time.perf_counter()
    ob.append_events("bench", [event_to_record(e) for e in events])
    dt = time.perf_counter() - t0
    ob.close()
    return {"ui_ms": dt * 1000, "durable_ms": dt * 1000, "ok": True}


def bench_pending_rmw(path, events):
    from recoverland.core.integrity import save_pending_events
    t0 = time.perf_counter()
    save_pending_events(path, list(events))
    dt = time.perf_counter() - t0
    try:
        os.remove(os.path.join(os.path.dirname(path), "recoverland_pending.json"))
    except OSError:
        pass
    return {"ui_ms": dt * 1000, "durable_ms": dt * 1000, "ok": True}


VARIANTS = {
    "wq_async": bench_wq_async,
    "sync_normal": bench_sync_normal,
    "sync_full": bench_sync_full,
    "jsonl_fsync": bench_jsonl_fsync,
    "outbox_v7": bench_outbox_v7,
    "pending_rmw": bench_pending_rmw,
}


def main(argv) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefill", type=int, default=100000)
    ap.add_argument("--max-size", type=int, default=100000)
    ap.add_argument("--repeat", type=int, default=5)
    ap.add_argument("--dir", default="")
    args = ap.parse_args(argv)

    root = args.dir or tempfile.mkdtemp(prefix="rl_bench_dur_")
    journal = os.path.join(root, "recoverland_audit.sqlite")
    t0 = time.perf_counter()
    _make_journal(journal, args.prefill)
    print(f"[bench] journal {journal} prefilled with {args.prefill} rows "
          f"in {time.perf_counter() - t0:.1f}s, "
          f"size={os.path.getsize(journal) / 1e6:.1f} MB", flush=True)

    created = datetime.now(timezone.utc).isoformat()
    results = []
    for size in SIZES:
        if size > args.max_size:
            continue
        events = [_event(i, f"bench-{size}", created) for i in range(size)]
        repeat = args.repeat if size <= 1000 else (2 if size <= 10000 else 1)
        for name, fn in VARIANTS.items():
            if name == "pending_rmw" and size > 10000:
                continue
            runs = [fn(journal, events) for _ in range(repeat)]
            row = {
                "variant": name, "n": size, "repeat": repeat,
                "ui_ms_median": statistics.median(r["ui_ms"] for r in runs),
                "durable_ms_median": statistics.median(r["durable_ms"] for r in runs),
                "ui_us_per_event": statistics.median(r["ui_ms"] for r in runs) * 1000 / size,
                "accepted": all(r["ok"] for r in runs),
            }
            results.append(row)
            print(f"[bench] {name:<12} n={size:>6}  ui={row['ui_ms_median']:>9.2f} ms  "
                  f"durable={row['durable_ms_median']:>9.2f} ms  "
                  f"({row['ui_us_per_event']:.1f} us/event on the calling thread)"
                  f"{'' if row['accepted'] else '  REFUSED'}", flush=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = _HERE / "reports" / f"bench_durability_{ts}.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prefill": args.prefill,
        "journal_bytes": os.path.getsize(journal),
        "python": sys.version,
        "sqlite": sqlite3.sqlite_version,
        "results": results,
    }, indent=2), encoding="utf-8")
    print(f"[bench] report={out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
