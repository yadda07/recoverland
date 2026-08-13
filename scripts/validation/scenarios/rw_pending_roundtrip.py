"""rw_pending_roundtrip - RecoverLand validation runtime.

Invariant: zero silent loss. Events the WriteQueue could not write are saved
to a pending file and MUST come back into the journal at next startup.
Root cause: they never did. Not once.

The hole this scenario proves
-----------------------------
When the queue overflows or QGIS dies, `write_queue._save_lost_events` calls
`integrity.save_pending_events`, which serialises `AuditEvent._asdict()` --
a dict whose first key is ``event_id``. At startup `check_journal_integrity`
validates each pending entry against ``_KNOWN_EVENT_KEYS``, built from
``AUDIT_EVENT_INSERT_COLUMNS``, which deliberately drops ``event_id`` (the
column is AUTOINCREMENT). Every entry therefore failed on
``unknown keys: {'event_id'}`` and was pushed back to the pending file.

Behind that, a second wall: the INSERT tuple was hand-written with 19 values
for 20 columns (it predated ``invalidated_at``), so an event surviving
validation raised ``sqlite3.Error`` and was swallowed by the
"skip pending event" branch.

Net effect: the crash safety net that the docs and the UI both advertise
("Zero silent loss: if events are lost, the user is explicitly informed and
they are re-integrated") had never put a single event back. The user is told
their edits were saved; the journal has a hole exactly where the incident
happened -- and that hole is invisible until a rewind crosses it.

Scenario layout (real journal file, real save + real recovery path):
    setup: an empty journal on disk, two events never written -- one plain,
           one carrying a geometry BLOB (exercises the base64 round-trip)
    run:   save_pending_events(), then check_journal_integrity()

Pre-patch verdict: FAIL (0 recovered, 2 rejected).
Post-patch verdict: PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_pending_roundtrip"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DS_FP = "rw_pr_datasource"
_GEOM = b"\x01\x01\x00\x00\x00pending_wkb_payload"


def _event(eid, ts, entity_fp, geom):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=eid,
        project_fingerprint="rw_pr_project",
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_pr",
        layer_name_snapshot="pr_layer",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": eid}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {"v": {"old": 1, "new": 2}}}),
        geometry_wkb=geom,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=None,
        user_name="tester",
        session_id=None,
        created_at=ts.isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=entity_fp,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_rw_pr_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    conn = sqlite3.connect(db_path)
    initialize_schema(conn)
    conn.commit()
    conn.close()

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    ctx.data["tmpdir"] = tmpdir
    ctx.data["db_path"] = db_path
    ctx.data["events"] = [
        _event(41, t0, "fid:41", None),
        _event(42, t0 + timedelta(seconds=5), "fid:42", _GEOM),
    ]
    ctx.data["created_ats"] = [e.created_at for e in ctx.data["events"]]

    flog(
        f"rw_pending_roundtrip setup: trace_id={ctx.trace_id} "
        f"db={db_path} n_events={len(ctx.data['events'])} "
        f"datasource={_DS_FP}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.integrity import (
        save_pending_events, check_journal_integrity, _get_pending_path,
    )
    from recoverland.core.logger import flog

    db_path = ctx.data["db_path"]

    flog(f"rw_pending_roundtrip run start: trace_id={ctx.trace_id}", "INFO")

    save_pending_events(db_path, ctx.data["events"])
    pending_path = _get_pending_path(db_path)
    ctx.data["pending_written"] = Path(pending_path).is_file()

    result = check_journal_integrity(db_path, trace_id=ctx.trace_id)
    ctx.data["recovered"] = result.recovered_events
    ctx.data["rejected"] = result.pending_rejected
    ctx.data["issues"] = list(result.issues)

    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT entity_fingerprint, created_at, geometry_wkb, invalidated_at "
        "FROM audit_event WHERE datasource_fingerprint = ? "
        "ORDER BY created_at",
        (_DS_FP,),
    ).fetchall()
    conn.close()
    ctx.data["rows"] = [
        (r[0], r[1], bytes(r[2]) if r[2] is not None else None, r[3])
        for r in rows
    ]

    leftover = []
    if Path(pending_path).is_file():
        try:
            leftover = json.loads(Path(pending_path).read_text(encoding="utf-8"))
        except (ValueError, OSError):
            leftover = ["<unreadable>"]
    ctx.data["leftover"] = leftover

    flog(
        f"rw_pending_roundtrip run end: trace_id={ctx.trace_id} "
        f"recovered={ctx.data['recovered']} rejected={ctx.data['rejected']} "
        f"rows={len(ctx.data['rows'])} leftover={len(leftover)}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    rows = ctx.data.get("rows") or []

    out.append((
        "pending_file_written",
        ctx.data.get("pending_written") is True,
        "save_pending_events must produce the recovery file",
    ))

    # ===== The hole ======================================================
    out.append((
        "every_event_recovered",
        ctx.data.get("recovered") == 2,
        f"recovered={ctx.data.get('recovered')} expected=2 "
        f"rejected={ctx.data.get('rejected')}: the user was told these edits "
        f"were saved; if they do not come back the journal keeps a hole "
        f"exactly where the incident happened",
    ))
    out.append((
        "nothing_rejected",
        ctx.data.get("rejected") == 0,
        f"rejected={ctx.data.get('rejected')} expected=0 "
        f"(the event_id key comes from AuditEvent._asdict and is not an error)",
    ))
    out.append((
        "rows_are_in_the_journal",
        len(rows) == 2,
        f"rows={rows!r}: recovery counted is not recovery done -- the INSERT "
        f"itself must succeed",
    ))

    # ===== Fidelity ======================================================
    if len(rows) == 2:
        out.append((
            "created_at_preserved",
            [r[1] for r in rows] == ctx.data.get("created_ats"),
            f"created_at={[r[1] for r in rows]} "
            f"expected={ctx.data.get('created_ats')}: a re-stamped event would "
            f"land at the wrong place in the timeline and be compensated by "
            f"the wrong rewind",
        ))
        out.append((
            "geometry_blob_round_tripped",
            rows[1][2] == _GEOM,
            f"geometry={rows[1][2]!r} expected={_GEOM!r} "
            f"(base64 encode/decode through the pending file)",
        ))
        out.append((
            "invalidated_at_column_bound",
            all(r[3] is None for r in rows),
            f"invalidated_at={[r[3] for r in rows]}: the column must be bound, "
            f"not missing from the value tuple",
        ))

    out.append((
        "pending_file_drained",
        not ctx.data.get("leftover"),
        f"leftover={len(ctx.data.get('leftover') or [])} entries: a recovered "
        f"event must not stay queued, or it will be inserted again at every "
        f"startup",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rw_pending_roundtrip.*trace_id={ctx.trace_id}",
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
