"""rw_strict_runner_end_to_end - RecoverLand validation runtime.

Invariant: I-REWIND-01 (un rewind rend l'etat a T, verifie sur le fichier,
et deux rewinds a la meme date sont idempotents) et I-CAPTURE-03 (un
rewind ne suspend la capture QUE sur la couche qu'il ecrit).

Pourquoi ce scenario
--------------------
Aucun scenario ne faisait tourner le `StrictRestoreRunner` -- le chemin que
l'interface utilise pour un REWIND -- de bout en bout sur une vraie couche :
les scenarios existants lui injectaient des rapports synthetiques. Celui-ci
enchaine vraie capture (tracker + WriteQueue + journal), vrai rewind par
ticks QTimer (un action par tick pour ouvrir des fenetres), et relit le
GeoPackage.

Cas
---
R1  rewind de la couche A a T0 apres trois commits (renommage+deplacement,
    suppression, ajout) : le fichier revient exactement a T0, les traces sont
    au journal, le rapport ne compte aucun echec.
R2  pendant R1, l'utilisateur valide une edition sur la couche B : elle
    DOIT etre journalisee. Avant : `tracker.suppress()` global pendant tout
    le run, l'edition de B etait ecrite sur disque et absente du journal.
R3  apres le run, plus aucune couche n'est suspendue.
R4  second rewind a la meme date : aucune ecriture, fichier inchange.
R5  pendant un autre rewind de A, l'utilisateur modifie A dans le tampon
    que le run tient ouvert : le run s'arrete, n'ecrit rien, et le dit.

Verdict attendu avant correctif : FAIL (R2, R5).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_strict_runner_end_to_end"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _gpkg(tmpdir, name, rows):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
                         "m", "memory")
    mem.startEditing()
    for code, nom, x, y in rows:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, nom])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, y)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    assert lyr.isValid(), path
    return path, lyr


def _state(path):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    out = {}
    for f in lyr.getFeatures():
        pt = f.geometry().asPoint()
        out[f["code"]] = (f["nom"], (round(pt.x(), 3), round(pt.y(), 3)))
    return out


def _fid_of(layer, code):
    for f in layer.getFeatures():
        if f["code"] == code:
            return f.id()
    return None


def _commit(layer, fn):
    layer.startEditing()
    fn()
    return bool(layer.commitChanges())


def _rows(journal, where, params=()):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event WHERE " + where,
                                params).fetchone()[0])
    finally:
        conn.close()


def _run_rewind(ctx, jm, wq, tracker, layers_by_fp, fp, cutoff, mid_run=None):
    """Drive a real StrictRestoreRunner, one action per tick."""
    import recoverland.restore_runner as rr
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from scripts.validation.plugin_harness import pump
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    else:
        time.sleep(1.0)
    conn = jm.create_read_connection()
    try:
        events = fetch_events_after_cutoff(conn, fp, cutoff, trace_id=ctx.trace_id)
    finally:
        conn.close()
    active, _stats = collapse_rewind_events_with_stats(events)
    # One action per tick, and the user's action injected exactly BETWEEN
    # tick 1 and tick 2, while the run holds the layer in edit mode. The
    # chunk-size constant is bound as a default argument, so the decision
    # function itself is replaced.
    saved_yield = rr._chunk_should_yield
    rr._chunk_should_yield = lambda elapsed_ms, n_done, *a, **k: n_done >= 1
    done = {}
    try:
        runner = rr.StrictRestoreRunner(
            active, lambda ev: layers_by_fp.get(ev.datasource_fingerprint),
            cutoff, write_queue=wq, tracker=tracker, trace_id=ctx.trace_id)
        runner.finished.connect(lambda res: done.setdefault("result", res))
        original_chunk = runner._process_strict_chunk
        ticks = {"n": 0}

        def _chunk_with_user_between_ticks():
            ticks["n"] += 1
            if ticks["n"] == 2 and mid_run is not None:
                mid_run()
            original_chunk()
        runner._process_strict_chunk = _chunk_with_user_between_ticks
        runner.start()
        deadline = time.monotonic() + 30
        while "result" not in done and time.monotonic() < deadline:
            pump(0.05)
        report = runner.apply_report() if hasattr(runner, "apply_report") else {}
        ctx.data.setdefault("ticks", []).append(ticks["n"])
    finally:
        rr._chunk_should_yield = saved_yield
    res = done.get("result")
    if hasattr(wq, "wait_until_drained"):
        wq.wait_until_drained(5.0)
    else:
        time.sleep(1.0)
    return {
        "n_active": len(active),
        "finished": res is not None,
        "total_ok": getattr(res, "total_ok", None),
        "total_fail": getattr(res, "total_fail", None),
        "errors": list(getattr(res, "errors", []) or [])[:6],
        "report": {k: v for k, v in (report or {}).items()
                   if k in ("applied", "apply_fail", "failed_postcommit_verification",
                            "skipped_idempotent", "plan_actions")},
    }


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_e2e_")
    flog(f"rw_strict_runner_end_to_end setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsGeometry, QgsPointXY, QgsFeature
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump

    d = ctx.data
    tmp = d["tmpdir"]
    path_a, la = _gpkg(tmp, "a", [("C001", "PONT", 1, 1), ("C002", "GARE", 2, 2),
                                  ("C003", "ECOLE", 3, 3)])
    path_b, lb = _gpkg(tmp, "b", [("B001", "BORNE", 5, 5)])
    QgsProject.instance().addMapLayers([la, lb])
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()
    tracker.connect_layer(la)
    tracker.connect_layer(lb)
    fp_a = compute_datasource_fingerprint(la)
    fp_b = compute_datasource_fingerprint(lb)
    layers = {fp_a: la, fp_b: lb}
    try:
        d["t0_state"] = _state(path_a)
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        idx_nom = la.fields().indexFromName("nom")
        f1 = _fid_of(la, "C001")
        _commit(la, lambda: (la.changeAttributeValue(f1, idx_nom, "PONT_V1"),
                             la.changeGeometry(f1, QgsGeometry.fromPointXY(QgsPointXY(11, 11)))))
        _commit(la, lambda: la.deleteFeature(_fid_of(la, "C002")))

        def _add():
            nf = QgsFeature(la.fields())
            attrs = [None] * la.fields().count()
            attrs[la.fields().indexFromName("code")] = "C004"
            attrs[idx_nom] = "NOUVEAU"
            nf.setAttributes(attrs)
            nf.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(4, 4)))
            la.addFeature(nf)
        _commit(la, _add)
        pump(0.3)
        d["before_rewind_state"] = _state(path_a)
        cutoff = RestoreCutoff(CutoffType.BY_DATE, t0)

        # R1 + R2: rewind A, the user commits on B in the middle of the run
        def _user_on_b():
            idx_b = lb.fields().indexFromName("nom")
            fb = _fid_of(lb, "B001")
            d["r2_commit"] = _commit(lb, lambda: lb.changeAttributeValue(fb, idx_b, "B_MID_RUN"))
        d["r1"] = _run_rewind(ctx, jm, wq, tracker, layers, fp_a, cutoff, mid_run=_user_on_b)
        d["r1_state"] = _state(path_a)
        d["r1_traces"] = _rows(jm.path, "restored_from_event_id IS NOT NULL "
                                        "AND datasource_fingerprint = ?", (fp_a,))
        d["r2_rows"] = _rows(jm.path, "attributes_json LIKE ?", ("%B_MID_RUN%",))
        # R3
        d["r3_suppressed_layers"] = sorted(getattr(tracker, "suppressed_layer_ids", set()))
        d["r3_globally_suppressed"] = bool(tracker.is_suppressed)
        # R4
        d["r4"] = _run_rewind(ctx, jm, wq, tracker, layers, fp_a, cutoff)
        d["r4_state"] = _state(path_a)

        # R5: two new commits on A after t1, rewind to t1, user edits A mid-run
        time.sleep(1.1)
        t1 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        f3 = _fid_of(la, "C003")
        _commit(la, lambda: la.changeAttributeValue(f3, idx_nom, "ECOLE_V1"))
        _commit(la, lambda: la.changeGeometry(f3, QgsGeometry.fromPointXY(QgsPointXY(33, 33))))
        pump(0.3)
        d["r5_before"] = _state(path_a)

        def _user_on_a():
            f_1 = _fid_of(la, "C001")
            d["r5_layer_editable_mid_run"] = la.isEditable()
            la.changeAttributeValue(f_1, idx_nom, "USER_DURING_RUN")
        d["r5"] = _run_rewind(ctx, jm, wq, tracker, layers, fp_a,
                              RestoreCutoff(CutoffType.BY_DATE, t1), mid_run=_user_on_a)
        if la.isEditable():
            la.rollBack()
        d["r5_after"] = _state(path_a)
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        tracker.deactivate()
        tracker.disconnect_all()
        wq.stop()
        jm.close()
        QgsProject.instance().clear()
    flog(f"rw_strict_runner_end_to_end run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    t0 = d.get("t0_state") or {}
    r1 = d.get("r1") or {}
    out.append((
        "r1_file_is_back_at_t0",
        d.get("r1_state") == t0 and d.get("before_rewind_state") != t0,
        f"t0={t0} avant rewind={d.get('before_rewind_state')} apres={d.get('r1_state')} "
        f"run={r1} crash={d.get('crash')} {d.get('trace', '')[-700:]}",
    ))
    out.append((
        "r1_report_has_no_failure_and_traces_are_journalled",
        r1.get("finished") is True and r1.get("total_fail") == 0
        and (d.get("r1_traces") or 0) >= r1.get("n_active", 99),
        f"run={r1} traces journalisees={d.get('r1_traces')}",
    ))
    out.append((
        "r2_user_commit_on_another_layer_is_journalled",
        d.get("r2_commit") is True and (d.get("r2_rows") or 0) >= 1,
        f"commit sur B pendant le rewind de A={d.get('r2_commit')} evenements "
        f"journalises={d.get('r2_rows')} : la capture ne doit etre suspendue que "
        f"sur la couche que le rewind ecrit",
    ))
    out.append((
        "r3_nothing_left_suppressed",
        d.get("r3_suppressed_layers") == [] and d.get("r3_globally_suppressed") is False,
        f"couches suspendues={d.get('r3_suppressed_layers')} "
        f"suspension globale={d.get('r3_globally_suppressed')}",
    ))
    r4 = d.get("r4") or {}
    out.append((
        "r4_second_rewind_is_idempotent",
        d.get("r4_state") == t0 and (r4.get("total_fail") in (0, None)),
        f"second rewind : {r4} etat={d.get('r4_state')}",
    ))
    r5 = d.get("r5") or {}
    out.append((
        "r5_foreign_edit_stops_the_run_and_writes_nothing",
        d.get("r5_after") == d.get("r5_before")
        and any("modifiee pendant la restauration" in e for e in r5.get("errors") or []),
        f"avant={d.get('r5_before')} apres={d.get('r5_after')} run={r5} "
        f"couche en edition pendant le run={d.get('r5_layer_editable_mid_run')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_strict_runner_end_to_end.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
