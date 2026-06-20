"""Scenario runtime: RL-E1-02 (Option A) volume guard + chain-correctness.

Doit etre execute dans la CONSOLE PYTHON DE QGIS (le worker derive de QThread
via qgis.PyQt, et le moteur importe qgis.core via le logger). Hors-QGIS
l'import echoue : c'est attendu (charte: preuve par logs runtime QGIS).

Lancement (console QGIS) ::

    import recoverland.scripts.validation.scenarios.rv_snapshot_volume as s
    s.run()

THESES validees :
  A1 : au-dela du budget de lignes, le worker marque result.partial=True
       (degrade EXPLICITE, jamais silencieux) ; en-dessous, partial=False.
  A2 : entite PREEXISTANTE (1er evenement = UPDATE, format production dict)
       -> attributs reconstruits = valeur NEW scalaire (pas de perte de chaine).
  A3 : chaine INSERT(geom) -> UPDATE attributs-seuls (new_geometry_wkb=None)
       -> la geometrie a T reste celle de l'INSERT (walk-back intact).

Le verdict (PASS/FAIL par assertion) est imprime ET ecrit via flog, prefixe
par le trace_id du scenario.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import uuid
from datetime import datetime, timezone

from recoverland.core.audit_backend import AuditEvent
from recoverland.core.logger import flog
from recoverland.core.serialization import compute_update_delta
from recoverland.core.sqlite_schema import (
    AUDIT_EVENT_INSERT_COLUMNS,
    initialize_schema,
)
from recoverland.core.temporal_snapshot_engine import reconstruct_snapshot_at

_DS_FP = "ds_rv_volume_test"
_GEOM0 = b"\x01geo0"


class _Results(list):
    trace_id = ""


def _check(results, name, passed, detail, brutal=False):
    tag = "PASS" if passed else "FAIL"
    kind = "BRUTAL" if brutal else "ASSERT"
    line = f"{kind} {name}: {tag} -- {detail}"
    results.append((name, passed, brutal, detail))
    flog(f"[{results.trace_id}] rv_volume: {line}", "INFO" if passed else "ERROR")
    print(line)


def _mk_event(event_id, operation_type, created_at, *, attributes_json,
              entity_fingerprint, geometry_wkb=None, new_geometry_wkb=None):
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="proj_test",
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="layer_test",
        layer_name_snapshot="couche_test",
        provider_type="memory",
        feature_identity_json="{}",
        operation_type=operation_type,
        attributes_json=attributes_json,
        geometry_wkb=geometry_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="{}",
        user_name="tester",
        session_id="sess",
        created_at=created_at,
        restored_from_event_id=None,
        entity_fingerprint=entity_fingerprint,
        new_geometry_wkb=new_geometry_wkb,
    )


# ------------------------------------------------------------------ #
# Phase 1 : invariants de chaine (moteur pur)                        #
# ------------------------------------------------------------------ #


def _run_chain_phase(results) -> None:
    cutoff = datetime(2026, 3, 1, tzinfo=timezone.utc)
    t1 = "2026-01-10T08:00:00"
    t2 = "2026-02-15T08:00:00"

    # A2 : entite PREEXISTANTE (pas d'INSERT) -> 1er evenement = UPDATE prod.
    pre_update = compute_update_delta({"name": "P0"}, {"name": "P1"}, ["name"])
    # A3 : INSERT avec geometrie, puis UPDATE attributs-seuls (geom None).
    attr_only_update = compute_update_delta({"name": "G0"}, {"name": "G1"}, ["name"])

    events = [
        _mk_event(1, "UPDATE", t2, attributes_json=pre_update,
                  entity_fingerprint="pk:gid=1"),
        _mk_event(2, "INSERT", t1,
                  attributes_json=json.dumps({"all_attributes": {"gid": 2, "name": "G0"}}),
                  entity_fingerprint="pk:gid=2", geometry_wkb=_GEOM0),
        _mk_event(3, "UPDATE", t2, attributes_json=attr_only_update,
                  entity_fingerprint="pk:gid=2", new_geometry_wkb=None),
    ]
    res = reconstruct_snapshot_at({_DS_FP: events}, cutoff, trace_id=results.trace_id)
    feats = res.features.get(_DS_FP, {})

    pre = feats.get("pk:gid=1")
    pre_attrs = json.loads(pre.attrs_json) if pre and pre.attrs_json else {}
    _check(results, "A2_preexisting_attr_new_scalar",
           pre_attrs.get("name") == "P1" and not isinstance(pre_attrs.get("name"), dict),
           f"preexisting name={pre_attrs.get('name')!r} (attendu 'P1' scalaire)",
           brutal=True)

    g = feats.get("pk:gid=2")
    _check(results, "A3_geom_only_update_keeps_geometry",
           g is not None and g.geom_wkb == _GEOM0,
           f"geom a T={g.geom_wkb!r} (attendu {_GEOM0!r} via walk-back)",
           brutal=True)

    _check(results, "A_default_not_partial",
           res.partial is False and res.partial_reason == "",
           f"partial={res.partial} reason={res.partial_reason!r} (attendu non-partiel)")


# ------------------------------------------------------------------ #
# Phase 2 : garde-fou volumetrique (worker + journal temporaire)     #
# ------------------------------------------------------------------ #


class _TmpJournal:
    """Minimal journal exposing create_read_connection over a temp sqlite file."""

    def __init__(self, path):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def _insert_events(conn, rows):
    cols = ", ".join(AUDIT_EVENT_INSERT_COLUMNS)
    ph = ",".join(["?"] * len(AUDIT_EVENT_INSERT_COLUMNS))
    sql = f"INSERT INTO audit_event ({cols}) VALUES ({ph})"
    payload = []
    for r in rows:
        payload.append(tuple(r.get(c) for c in AUDIT_EVENT_INSERT_COLUMNS))
    with conn:
        conn.executemany(sql, payload)


def _event_row(operation_type, created_at, attrs, entity_fp):
    return {
        "project_fingerprint": "proj_test",
        "datasource_fingerprint": _DS_FP,
        "layer_id_snapshot": "layer_test",
        "layer_name_snapshot": "couche_test",
        "provider_type": "memory",
        "feature_identity_json": "{}",
        "operation_type": operation_type,
        "attributes_json": attrs,
        "geometry_wkb": _GEOM0,
        "geometry_type": "Point",
        "crs_authid": "EPSG:4326",
        "field_schema_json": "{}",
        "user_name": "tester",
        "session_id": "sess",
        "created_at": created_at,
        "restored_from_event_id": None,
        "entity_fingerprint": entity_fp,
        "event_schema_version": 5,
        "new_geometry_wkb": None,
        "invalidated_at": None,
    }


def _run_worker(journal, budget):
    """Run SnapshotRebuildWorker synchronously with a custom row budget."""
    import recoverland.widgets.snapshot_rebuild_worker as wmod
    captured = {}
    worker = wmod.SnapshotRebuildWorker(
        journal,
        [{"fingerprint": _DS_FP, "layer_id": "", "layer_name": "couche_test",
          "storage_crs": "EPSG:4326"}],
        "2026-03-01T00:00:00",
        trace_id=uuid.uuid4().hex[:8],
        row_budget=budget,
    )
    worker.result_ready.connect(lambda t, r: captured.__setitem__("result", r))
    worker.error.connect(lambda t, e: captured.__setitem__("error", e))
    worker.run()
    return captured


def _run_volume_phase(results) -> None:
    try:
        from qgis.PyQt.QtCore import QThread  # noqa: F401
    except ImportError as exc:
        _check(results, "VOLUME_env", False,
               f"QGIS/PyQt indisponible: {exc!r} (phase volume sautee)")
        return

    t1 = "2026-01-10T08:00:00"
    t2 = "2026-02-15T08:00:00"
    t_after = "2026-04-20T08:00:00"
    rows = [
        _event_row("INSERT", t1,
                   json.dumps({"all_attributes": {"gid": i, "name": f"N{i}"}}),
                   f"pk:gid={i}")
        for i in range(1, 6)
    ]
    rows.append(_event_row(
        "UPDATE", t2,
        compute_update_delta({"name": "N1"}, {"name": "N1b"}, ["name"]),
        "pk:gid=1"))
    # Event after cutoff so the worker enters the reconstruction path
    # (without this, _SQL_FPS_CHANGED_AFTER returns 0 and the worker
    # short-circuits, never hitting the row budget).
    rows.append(_event_row(
        "UPDATE", t_after,
        compute_update_delta({"name": "N1b"}, {"name": "N1c"}, ["name"]),
        "pk:gid=1"))

    fd, path = tempfile.mkstemp(suffix=".sqlite", prefix="rv_volume_")
    os.close(fd)
    try:
        conn = sqlite3.connect(path)
        initialize_schema(conn)
        _insert_events(conn, rows)
        conn.close()
        journal = _TmpJournal(path)

        # Budget large -> non partiel, resultat complet.
        cap_ok = _run_worker(journal, budget=1000)
        res_ok = cap_ok.get("result")
        _check(results, "A1a_below_budget_not_partial",
               res_ok is not None and res_ok.partial is False,
               f"partial={getattr(res_ok, 'partial', 'NO_RESULT')} "
               f"n_entities={getattr(res_ok, 'n_entities', '?')}")

        # Budget minuscule -> partiel EXPLICITE.
        cap_part = _run_worker(journal, budget=2)
        res_part = cap_part.get("result")
        err_part = cap_part.get("error")
        _check(results, "A1b_over_budget_partial_explicit",
               res_part is not None and res_part.partial is True
               and res_part.partial_reason.startswith("row_budget_exceeded"),
               f"partial={getattr(res_part, 'partial', 'NO_RESULT')} "
               f"reason={getattr(res_part, 'partial_reason', '?')!r}"
               f" error={err_part!r}",
               brutal=True)
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# ------------------------------------------------------------------ #
# Entree                                                             #
# ------------------------------------------------------------------ #


def run() -> dict:
    results = _Results()
    results.trace_id = uuid.uuid4().hex[:8]
    flog(f"[{results.trace_id}] rv_volume: scenario_start", "INFO")
    print(f"=== rv_snapshot_volume trace_id={results.trace_id} ===")

    _run_chain_phase(results)
    _run_volume_phase(results)

    n_total = len(results)
    n_pass = sum(1 for _, p, _, _ in results if p)
    n_fail = n_total - n_pass
    n_brutal = sum(1 for _, _, b, _ in results if b)
    n_brutal_pass = sum(1 for _, p, b, _ in results if b and p)
    verdict = "PASS" if n_fail == 0 else "FAIL"
    synthese = (
        f"SYNTHESE: {verdict} -- {n_pass}/{n_total} assertions, "
        f"dont {n_brutal_pass}/{n_brutal} antitheses brutales."
    )
    flog(f"[{results.trace_id}] rv_volume: {synthese}",
         "INFO" if verdict == "PASS" else "ERROR")
    print(synthese)
    return {
        "verdict": verdict,
        "trace_id": results.trace_id,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "n_total": n_total,
        "failed": [name for name, p, _, _ in results if not p],
    }


if __name__ == "__main__":
    run()
