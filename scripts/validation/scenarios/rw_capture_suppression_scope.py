# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_capture_suppression_scope - un run ne suspend la capture d'une couche que pendant qu'il l'ecrit.

Constat couvert : revue brev2 MAJOR (suspension par couche tenue pour tout
le run, et prise sur une couche que le runner refuse).

Mecanisme
---------
Les trois runners suspendent la capture couche par couche
(`_suppress_for_run`, appele par `_resolve_runner_layer`) et ne la
relachent qu'a la toute fin du run (`_finish_runner` ->
`_release_run_suppression`). Deux consequences :

  * dans un run multi-couches, une couche deja traitee reste suspendue
    pendant que les suivantes sont ecrites ;
  * la suspension est prise AVANT le controle des editions non
    enregistrees : une couche que le runner refuse ("enregistrez ou annulez
    vos modifications") reste suspendue jusqu'a la fin du run.

Pendant ce temps chaque gestionnaire du tracker sort sur
`_layer_suppressed` : l'utilisateur, a qui on vient de dire d'enregistrer,
enregistre pendant le run, et son commit n'est jamais journalise. Perte
silencieuse d'historique : un Rewind ulterieur ne peut pas defaire ce que
le journal ne connait pas.

Ce qui est prouve, avec la vraie capture (EditSessionTracker + WriteQueue +
journal) sur de vrais GeoPackage, pour chaque runner :

  U  UndoRunner sur trois couches : A traitee, B refusee (editions non
     enregistrees), C en cours. Pendant C, l'utilisateur enregistre B et
     modifie puis enregistre A : les deux commits sont journalises.
  S  StrictRestoreRunner sur trois couches (Rewind reel) : pendant la
     deuxieme, l'utilisateur modifie la premiere (deja ecrite) et
     enregistre la troisieme (refusee) : les deux commits sont
     journalises, et les ecritures du Rewind lui-meme ne le sont pas
     comme editions de l'utilisateur.
  E  RestoreRunner (restore d'evenements), meme schema que U.
  Z  apres chaque run, plus aucune couche n'est suspendue.

Verdict avant correctif : FAIL (U, S, E). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_capture_suppression_scope"
INVARIANT = "I-CAPTURE-03"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))


def _gpkg(tmpdir, name, rows):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&field=v:string(40)",
                         "m", "memory")
    mem.startEditing()
    for code, v, x, y in rows:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, v])
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


def _fid(layer, code):
    for f in layer.getFeatures():
        if f["code"] == code:
            return f.id()
    return None


def _set(layer, code, value):
    """Change 'v' of `code` in the layer's open edit buffer."""
    layer.changeAttributeValue(_fid(layer, code), layer.fields().indexFromName("v"),
                               value)


def _commit(layer, code, value):
    layer.startEditing()
    _set(layer, code, value)
    return bool(layer.commitChanges())


def _user_rows(journal, marker):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE restored_from_event_id IS NULL"
            " AND attributes_json LIKE ?", (f"%{marker}%",)).fetchone()[0])
    finally:
        conn.close()


def _user_rows_detail(journal):
    """(layer, operation, attributes) of every user row, for the message."""
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        return [(r[0], r[1], (r[2] or "")[:90]) for r in conn.execute(
            "SELECT layer_name_snapshot, operation_type, attributes_json "
            "FROM audit_event WHERE restored_from_event_id IS NULL "
            "ORDER BY event_id")]
    finally:
        conn.close()


class _Backend:
    """Real capture: journal, write queue, tracker, on `n` fresh GeoPackages."""

    def __init__(self, tmp, prefix, n):
        from qgis.core import QgsProject
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        from recoverland.core.identity import compute_datasource_fingerprint
        # One directory per case: the journal lives next to the project, a
        # shared directory would mix the cases' rows.
        tmp = os.path.join(tmp, prefix)
        os.makedirs(tmp, exist_ok=True)
        self.paths, self.layers, self.fps = [], [], []
        for i in range(n):
            name = f"{prefix}{i}"
            path, layer = _gpkg(tmp, name, [("K1", "v0", 1, 1), ("K2", "w0", 2, 2)])
            self.paths.append(path)
            self.layers.append(layer)
        QgsProject.instance().addMapLayers(self.layers)
        project = os.path.join(tmp, f"{prefix}.qgz")
        open(project, "w").close()
        self.jm = JournalManager()
        self.jm.open_for_project(project, os.path.join(tmp, prefix + "_prof"))
        self.wq = WriteQueue()
        self.wq.start(self.jm.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()
        for layer in self.layers:
            self.tracker.connect_layer(layer)
            self.fps.append(compute_datasource_fingerprint(layer))

    def drain(self):
        from scripts.validation.plugin_harness import pump
        pump(0.3)
        self.wq.wait_until_drained(5.0)

    def close(self):
        from qgis.core import QgsProject
        for layer in self.layers:
            if layer.isEditable():
                layer.rollBack()
        self.tracker.deactivate()
        self.tracker.disconnect_all()
        self.wq.stop()
        self.jm.close()
        QgsProject.instance().removeAllMapLayers()


def _event(fp, name, eid):
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx.audit_event(
        event_id=eid, datasource_fingerprint=fp, layer_name_snapshot=name,
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {"v": {"old": "x", "new": "y"}}}))


def _case_undo(ctx, tmp, runner_kind):
    """U / E: A processed, B refused (unsaved edits), user acts during C."""
    import recoverland.restore_runner as rr
    from recoverland.core.audit_backend import RestoreReport
    from scripts.validation.plugin_harness import pump
    key = "u" if runner_kind == "undo" else "e"
    be = _Backend(tmp, key, 3)
    la, lb, lc = be.layers
    names = ["a", "b", "c"]
    by_ds = {fp: [_event(fp, names[i], 100 + i)] for i, fp in enumerate(be.fps)}
    saved = {}
    injected = {}

    def user_during_c(eid):
        if eid != 102 or injected:
            return
        injected["b_saved"] = bool(lb.commitChanges())
        injected["a_saved"] = _commit(la, "K1", f"{key.upper()}_A_USER")
        injected["suppressed_during"] = sorted(be.tracker.suppressed_layer_ids)
        injected["c_id"] = lc.id()

    try:
        # B holds unsaved user edits when the run reaches it.
        lb.startEditing()
        _set(lb, "K2", f"{key.upper()}_B_USER")
        done = []
        if runner_kind == "undo":
            from recoverland.core.restore_service import UndoBatchContext

            # Hypothesis revised (v7): the runners hand the executor the journal read
            # connection of the layer group (`journal=`, the published signature);
            # the doubles take it like the real functions, nothing else changes.
            def prepare(_layer, events, journal=None):
                return UndoBatchContext(ordered=list(reversed(events)),
                                        fid_cache={}, layer_error=None)

            def apply_one(_layer, event, _context):
                user_during_c(event.event_id)
                return {"success": True, "message": "Undo reverted"}
            saved["undo_prepare"] = rr.undo_prepare
            saved["undo_apply_one"] = rr.undo_apply_one
            rr.undo_prepare, rr.undo_apply_one = prepare, apply_one
            runner = rr.UndoRunner(
                by_ds, lambda e: be.layers[be.fps.index(e.datasource_fingerprint)],
                tracker=be.tracker)
        else:
            def batch(_layer, events, fid_cache=None, trace_id="",
                      journal=None):
                for e in events:
                    user_during_c(e.event_id)
                ids = [e.event_id for e in events]
                return RestoreReport(succeeded=ids, failed={},
                                     total_requested=len(ids), trace_events=())
            saved["restore_batch"] = rr.restore_batch
            saved["build_fid_cache"] = rr.build_fid_cache
            rr.restore_batch = batch
            rr.build_fid_cache = lambda _l, _e, journal=None: {}
            events = [e for evts in by_ds.values() for e in evts]
            runner = rr.RestoreRunner(
                events, lambda e: be.layers[be.fps.index(e.datasource_fingerprint)],
                None, tracker=be.tracker, trace_id=ctx.trace_id)
        runner.finished.connect(done.append)
        runner.start()
        deadline = time.monotonic() + 15
        while not done and time.monotonic() < deadline:
            pump(0.05)
        be.drain()
        ctx.data[f"{key}_injected"] = dict(injected)
        ctx.data[f"{key}_a_rows"] = _user_rows(be.jm.path, f"{key.upper()}_A_USER")
        ctx.data[f"{key}_b_rows"] = _user_rows(be.jm.path, f"{key.upper()}_B_USER")
        ctx.data[f"{key}_left_suppressed"] = sorted(be.tracker.suppressed_layer_ids)
        ctx.data[f"{key}_errors"] = list(getattr(done[0], "errors", []) or [])[:4] if done else None
    finally:
        for name, value in saved.items():
            setattr(rr, name, value)
        be.close()


def _case_strict(ctx, tmp):
    """S: a real three-layer Rewind; the user acts during the second layer."""
    import recoverland.restore_runner as rr
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from scripts.validation.plugin_harness import pump
    be = _Backend(tmp, "s", 3)
    l0, l1, l2 = be.layers
    saved = rr._chunk_should_yield
    injected = {}
    try:
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc).isoformat()
        time.sleep(1.1)
        for layer in be.layers:
            _commit(layer, "K1", "EDIT_AFTER_T0")
        be.drain()
        n_before = _user_rows(be.jm.path, "EDIT_AFTER_T0")
        cutoff = RestoreCutoff(CutoffType.BY_DATE, t0)
        conn = be.jm.create_read_connection()
        try:
            events = []
            for fp in be.fps:
                events.extend(fetch_events_after_cutoff(conn, fp, cutoff,
                                                        trace_id=ctx.trace_id))
        finally:
            conn.close()
        active, _stats = collapse_rewind_events_with_stats(events)
        # Groups in the order layer 0, layer 2, layer 1: when layer 1 is
        # written, layer 0 has been written and layer 2 refused. Within a
        # layer the planner orders the actions itself.
        rank = {be.fps[0]: 0, be.fps[2]: 1, be.fps[1]: 2}
        active.sort(key=lambda e: rank.get(e.datasource_fingerprint, 9))
        # Layer 2 holds unsaved user edits: refused by the run.
        l2.startEditing()
        _set(l2, "K2", "S_C_USER")
        rr._chunk_should_yield = lambda elapsed_ms, n_done, *a, **k: n_done >= 1
        done = []
        runner = rr.StrictRestoreRunner(
            active, lambda e: be.layers[be.fps.index(e.datasource_fingerprint)],
            cutoff, write_queue=be.wq, tracker=be.tracker, trace_id=ctx.trace_id)
        original = runner._process_strict_chunk

        def chunk():
            if runner._strict_layer is l1 and not injected:
                injected["a_saved"] = _commit(l0, "K2", "S_A_USER")
                injected["c_saved"] = bool(l2.commitChanges())
                injected["suppressed_during"] = sorted(be.tracker.suppressed_layer_ids)
            original()
        runner._process_strict_chunk = chunk
        runner.finished.connect(done.append)
        runner.start()
        deadline = time.monotonic() + 30
        while not done and time.monotonic() < deadline:
            pump(0.05)
        be.drain()
        ctx.data["s_injected"] = dict(injected)
        ctx.data["s_a_rows"] = _user_rows(be.jm.path, "S_A_USER")
        ctx.data["s_c_rows"] = _user_rows(be.jm.path, "S_C_USER")
        # The rewind wrote 'v0' back on K1 of layers 0 and 1: never as a
        # user edit. Every user row of the journal is accounted for: the
        # three edits after t0 and the two commits made during the run.
        ctx.data["s_user_rows_total"] = _user_rows(be.jm.path, "")
        ctx.data["s_user_rows"] = _user_rows_detail(be.jm.path)
        ctx.data["s_edits"] = (n_before, _user_rows(be.jm.path, "EDIT_AFTER_T0"))
        ctx.data["s_run_written"] = sorted(
            be.layers[be.fps.index(fp)].name()
            for fp in (getattr(done[0], "by_ds", {}) or {})) if done else None
        ctx.data["s_left_suppressed"] = sorted(be.tracker.suppressed_layer_ids)
        ctx.data["s_result"] = (getattr(done[0], "total_ok", None),
                                list(getattr(done[0], "errors", []) or [])[:3]) if done else None
    finally:
        rr._chunk_should_yield = saved
        be.close()


def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_supp_scope_")


def run(ctx):
    from recoverland.core.logger import flog
    flog(f"rw_capture_suppression_scope run start: trace_id={ctx.trace_id}")
    try:
        for label, fn in (("u", lambda: _case_undo(ctx, ctx.data["tmp"], "undo")),
                          ("s", lambda: _case_strict(ctx, ctx.data["tmp"])),
                          ("e", lambda: _case_undo(ctx, ctx.data["tmp"], "restore"))):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001 - a crash is a result
                import traceback
                ctx.data[f"{label}_crash"] = traceback.format_exc()[-1500:]
                flog(f"rw_capture_suppression_scope: case {label} crashed: {exc}",
                     "ERROR")
    finally:
        from qgis.core import QgsProject
        QgsProject.instance().clear()
        flog(f"rw_capture_suppression_scope run end: trace_id={ctx.trace_id}")
        shutil.rmtree(ctx.data["tmp"], ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = []
    for key, label in (("u", "UndoRunner"), ("e", "RestoreRunner")):
        inj = d.get(f"{key}_injected") or {}
        out += [
            (f"{key.upper()}_control_user_actions_happened",
             inj.get("b_saved") is True and inj.get("a_saved") is True
             and not d.get(f"{key}_crash"),
             f"{label} : enregistrements={inj} crash={d.get(f'{key}_crash')} "
             f"(controle)"),
            (f"{key.upper()}_refused_layer_commit_is_journalled",
             (d.get(f"{key}_b_rows") or 0) >= 1,
             f"{label} : commit de la couche refusee, fait pendant le run="
             f"{d.get(f'{key}_b_rows')} evenement(s) au journal ; couches "
             f"suspendues a ce moment={inj.get('suppressed_during')}. On a dit "
             f"a l'utilisateur d'enregistrer, il l'a fait, et son historique "
             f"est perdu."),
            (f"{key.upper()}_processed_layer_commit_is_journalled",
             (d.get(f"{key}_a_rows") or 0) >= 1,
             f"{label} : commit sur la couche deja traitee="
             f"{d.get(f'{key}_a_rows')} evenement(s) au journal : le run n'y "
             f"ecrit plus, la capture doit y etre rendue."),
            (f"{key.upper()}_only_the_written_layer_is_suppressed",
             inj.get("suppressed_during") == [inj.get("c_id")],
             f"{label} : couches suspendues pendant l'ecriture de la troisieme="
             f"{inj.get('suppressed_during')} attendu [{inj.get('c_id')}]."),
            (f"Z_{key}_nothing_left_suppressed",
             d.get(f"{key}_left_suppressed") == [],
             f"{label} : couches encore suspendues apres le run="
             f"{d.get(f'{key}_left_suppressed')}"),
        ]
    inj = d.get("s_injected") or {}
    n_before, n_after = d.get("s_edits") or (None, None)
    out += [
        ("S_control_rewind_ran_and_user_acted",
         inj.get("a_saved") is True and inj.get("c_saved") is True
         and not d.get("s_crash") and d.get("s_result") is not None,
         f"Rewind={d.get('s_result')} enregistrements={inj} "
         f"crash={d.get('s_crash')} (controle)"),
        ("S_refused_layer_commit_is_journalled",
         (d.get("s_c_rows") or 0) >= 1,
         f"Rewind : commit de la couche refusee pendant le run="
         f"{d.get('s_c_rows')} evenement(s) ; suspendues a ce moment="
         f"{inj.get('suppressed_during')}."),
        ("S_processed_layer_commit_is_journalled",
         (d.get("s_a_rows") or 0) >= 1,
         f"Rewind : commit sur la premiere couche, deja ecrite, pendant "
         f"l'ecriture de la deuxieme={d.get('s_a_rows')} evenement(s)."),
        ("S_rewind_own_writes_are_not_user_edits",
         d.get("s_run_written") == ["s0", "s1"]
         and n_before == n_after == 3 and d.get("s_user_rows_total") == 5,
         f"couches ecrites par le Rewind={d.get('s_run_written')} editions "
         f"avant/apres={(n_before, n_after)} lignes utilisateur au journal="
         f"{d.get('s_user_rows_total')} attendu 5 (3 editions + 2 commits "
         f"pendant le run) : relacher la suspension trop tot ferait capturer "
         f"les ecritures du Rewind comme des editions de l'utilisateur. "
         f"lignes={d.get('s_user_rows')}"),
        ("Z_s_nothing_left_suppressed", d.get("s_left_suppressed") == [],
         f"Rewind : couches encore suspendues={d.get('s_left_suppressed')}"),
    ]
    return out


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
