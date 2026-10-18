"""rw_undo_order_journal - un Undo defait dans l'ordre inverse de l'application, et le journal garde cet ordre.

Constat couvert : item C du lot dialogue ("pour deux UPDATE d'un meme
champ, l'annulation finit sur l'ancienne valeur NEW").

Mecanisme
---------
Sur la couche, l'Undo est deja dans le bon ordre : le Rewind compense du
plus recent au plus ancien, `undo_prepare` renverse la liste et remet donc
les evenements du plus ancien au plus recent (controle U1 ci-dessous). Le
defaut est dans ce que l'Undo ECRIT au journal : `_invalidate_trace_events`
invalide toutes les traces d'un Undo avec UN SEUL horodatage. L'ordre dans
lequel l'Undo a re-applique les evenements est perdu. Or la fenetre v7 d'un
Rewind ulterieur rejoue ces bascules du plus recent au plus ancien, et, a
horodatage egal, departage par l'identifiant de la trace, c'est-a-dire
l'ordre d'ECRITURE du Rewind annule, exactement l'inverse de l'ordre de
l'Undo.

Pour l'utilisateur : le champ passe de 'a' a 'b' (e1) puis a 'c' (e2).
Rewind avant e1 : 'a'. "Annuler le dernier restore" : 'c'. Il rembobine
ensuite a une date situee entre ce Rewind et cet Undo, ou le champ valait
'a'. La fenetre re-compense e1 avant e2 : 'c' -> 'a' puis 'a' -> 'b'. Le
champ finit sur 'b', l'ancienne valeur NEW, et le compte rendu annonce
"2 entite(s) restauree(s) avec succes".

Ce qui est prouve, de bout en bout sur un vrai GeoPackage, avec la vraie
capture (tracker, file d'ecriture, journal), la vraie lecture de la
fenetre, le vrai StrictRestoreRunner et le vrai UndoRunner, pilotes par les
vraies methodes du dialogue :

  U1  (controle) Rewind puis Undo : le champ revient a 'c' ;
  U2  l'Undo horodate ses invalidations dans l'ordre ou il a re-applique
      (e1 avant e2), strictement croissant ;
  U3  Rewind a une date entre le Rewind et l'Undo : 'a', et pas 'b' ;
  U4  Undo de ce Rewind : 'c'.
  U5  (vraies methodes du dialogue, vrai UndoRunner, fournisseur scripte)
      un Rewind aux operations melangees (INSERT, UPDATE, DELETE) : l'Undo
      parcourt l'exact inverse du plan. La reserve d'annulation etait
      re-triee par date, et `undo_prepare` la renverse : l'Undo refaisait
      les phases du Rewind dans le MEME sens.

Verdict avant correctif : FAIL (U2, U3, U5). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_undo_order_journal"
INVARIANT = "I-UNDO-ORDER"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


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


def _value(path, code="C1"):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    for f in lyr.getFeatures():
        if f["code"] == code:
            return f["v"]
    return None


def _commit(layer, code, value):
    fid = next(f.id() for f in layer.getFeatures() if f["code"] == code)
    layer.startEditing()
    layer.changeAttributeValue(fid, layer.fields().indexFromName("v"), value)
    return bool(layer.commitChanges())


def _invalidations(journal, source_ids):
    """[(source id, invalidated_at)] of the traces of `source_ids`, by stamp."""
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        ph = ",".join("?" for _ in source_ids)
        rows = conn.execute(
            "SELECT restored_from_event_id, invalidated_at FROM audit_event "
            f"WHERE restored_from_event_id IN ({ph}) "  # nosec B608
            "AND invalidated_at IS NOT NULL ORDER BY invalidated_at, event_id",
            list(source_ids)).fetchall()
        return [(int(r[0]), r[1]) for r in rows]
    finally:
        conn.close()


def _user_ids(journal):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        return [int(r[0]) for r in conn.execute(
            "SELECT event_id FROM audit_event WHERE restored_from_event_id IS NULL "
            "ORDER BY event_id")]
    finally:
        conn.close()


def _case_u5(ctx):
    """U5: the Undo walks a mixed-operation Rewind in the exact reverse of its plan.

    The planner applies a layer in phases (INSERT compensations, then UPDATE
    newest first, then DELETE); the undo state was re-sorted by date, newest
    first, which `undo_prepare` then reverses: for mixed operations the undo
    walked in the SAME order as the Rewind, not the reverse. Real dialog
    slots, real `UndoRunner`, real planner; the provider is scripted
    (`undo_prepare` reverses its input like the real one).
    """
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.workflow_service import GroupedRestoreResult
    from datetime import timedelta
    h = _h()
    fp = "ds_order_u5"
    tmp = tempfile.TemporaryDirectory(prefix="rl_undo_order_u5_")
    try:
        path = h.new_journal(tmp.name, "u5.sqlite")
        d1 = h.insert_event(path, fp, "melange", "DELETE", 100, fid=1)
        u2 = h.insert_event(path, fp, "melange", "UPDATE", 200, fid=2)
        i3 = h.insert_event(path, fp, "melange", "INSERT", 300, fid=3)
        for sec, src, op in ((400, i3, "DELETE"), (401, u2, "UPDATE"),
                             (402, d1, "INSERT")):
            h.insert_event(path, fp, "melange", op, sec, fid=1,
                           restored_from=src, schema=7)
        # The window as the Rewind got it: newest first.
        evs = h.fetch_events(path, [i3, u2, d1])
        cutoff = h.cutoff(h.T0 + timedelta(seconds=50))
        plan = plan_temporal_restore(evs, fp, "melange", cutoff)
        ctx.data["u5_applied"] = [a.event_id for a in plan.actions]
        dlg = h.build_dialog(rd, h.Journal(path), h.RecordingWriteQueue(path), [fp])
        dlg._version_restore_events = list(evs)
        dlg._version_restore_cutoff = cutoff
        undo_calls, logs, asked, done = [], [], [], []
        seams = h.Seams()
        try:
            prepare, apply_one = h.scripted_undo({}, undo_calls)
            seams.set(rr, "undo_prepare", prepare)
            seams.set(rr, "undo_apply_one", apply_one)
            seams.set(rd, "find_target_layer",
                      lambda _e, _c=None: h.FakeLayer("lyr_melange", "melange"))
            seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
            seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
            # The Rewind ends: it wrote the three, published in input order.
            dlg._on_version_restore_done(GroupedRestoreResult(
                total_ok=3, total_fail=0, errors=[], by_ds={fp: list(evs)},
                trace_events=[], applied=3))
            original = dlg._on_undo_done
            dlg._on_undo_done = lambda res, _o=original: (_o(res), done.append(res))
            dlg._undo_last_restore()
            h.pump_until(lambda: bool(done), 5.0)
            ctx.data["u5_undo_walk"] = [eid for _n, eid in undo_calls]
        finally:
            seams.restore()
            h.close_dialog(dlg)
    finally:
        tmp.cleanup()


def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_undo_order_")


def run(ctx):
    import recoverland.recover_dialog as rd
    from qgis.core import QgsProject
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump
    h = _h()
    d = ctx.data
    tmp = d["tmp"]
    flog(f"rw_undo_order_journal run start: trace_id={ctx.trace_id}")
    path, layer = _gpkg(tmp, "ordre", [("C1", "a", 1, 1), ("C2", "x", 2, 2)])
    QgsProject.instance().addMapLayers([layer])
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()
    tracker.connect_layer(layer)
    fp = compute_datasource_fingerprint(layer)
    logs, asked, fetches = [], [], []
    seams = h.Seams()
    dlg = None

    def settle(pred, timeout=20.0):
        h.pump_until(pred, timeout)
        pump(0.3)
        wq.wait_until_drained(5.0)

    def rewind(cutoff_dt):
        dlg.time_slider.cutoff_dt = cutoff_dt
        dlg._is_recovering = True
        dlg._recover_version_mode()
        settle(lambda: (not dlg._is_recovering and not dlg._restore_in_progress
                        and dlg._restore_runner is None))

    def undo():
        done = []
        original = dlg._on_undo_done
        dlg._on_undo_done = lambda res, _o=original: (_o(res), done.append(res))
        try:
            dlg._undo_last_restore()
            settle(lambda: bool(done))
        finally:
            dlg._on_undo_done = original

    try:
        time.sleep(1.1)
        t0 = datetime.now(timezone.utc)
        time.sleep(1.1)
        _commit(layer, "C1", "b")
        time.sleep(0.05)
        _commit(layer, "C1", "c")
        pump(0.3)
        wq.wait_until_drained(5.0)
        d["user_ids"] = _user_ids(jm.path)
        d["start"] = _value(path)
        dlg = h.build_dialog(rd, jm, wq, [fp], cutoff_dt=t0)
        dlg._tracker = tracker
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
        rewind(t0)
        d["after_rewind1"] = _value(path)
        time.sleep(1.1)
        t_mid = datetime.now(timezone.utc)
        time.sleep(1.1)
        undo()
        d["after_undo1"] = _value(path)
        d["stamps"] = _invalidations(jm.path, d["user_ids"])
        n_logs = len(logs)
        rewind(t_mid)
        d["after_rewind2"] = _value(path)
        d["rewind2_window"] = h.plan_marks(fetches[-1].result if fetches else [])
        d["rewind2_report"] = logs[n_logs:]
        undo()
        d["after_undo2"] = _value(path)
    except Exception:  # noqa: BLE001 - a crash is a result
        import traceback
        d["crash"] = traceback.format_exc()[-2000:]
    finally:
        seams.restore()
        if dlg is not None:
            h.close_dialog(dlg)
        tracker.deactivate()
        tracker.disconnect_all()
        wq.stop()
        jm.close()
        QgsProject.instance().clear()
        flog(f"rw_undo_order_journal run end: trace_id={ctx.trace_id}")
        shutil.rmtree(tmp, ignore_errors=True)
    try:
        _case_u5(ctx)
    except Exception:  # noqa: BLE001 - a crash is a result
        import traceback
        d["u5_crash"] = traceback.format_exc()[-1500:]


def assertions(ctx):
    d = ctx.data
    ids = d.get("user_ids") or []
    stamps = d.get("stamps") or []
    order = [sid for sid, _s in stamps]
    instants = [s for _sid, s in stamps]
    report = " || ".join(f"{lvl}:{m}" for m, lvl in d.get("rewind2_report") or [])
    return [
        ("U0_control_capture_and_first_rewind",
         not d.get("crash") and len(ids) == 2 and d.get("start") == "c"
         and d.get("after_rewind1") == "a",
         f"evenements={ids} depart={d.get('start')} apres Rewind="
         f"{d.get('after_rewind1')} crash={d.get('crash')} (controle)"),
        ("U1_undo_reverts_in_reverse_order_of_application",
         d.get("after_undo1") == "c",
         f"apres 'Annuler le dernier restore'={d.get('after_undo1')} attendu "
         f"'c' : l'Undo doit re-appliquer e1 puis e2 ; dans l'autre ordre le "
         f"champ finit sur 'b', l'ancienne valeur NEW."),
        ("U2_journal_keeps_the_order_of_the_undo",
         order == ids and len(set(instants)) == len(instants) == 2
         and instants == sorted(instants),
         f"invalidations (source, instant)={stamps} attendu e1 puis e2 a des "
         f"instants strictement croissants : un horodatage unique efface "
         f"l'ordre de l'Undo, et la fenetre le remplace par l'ordre "
         f"d'ecriture du Rewind annule, l'inverse."),
        ("U3_rewind_between_rewind_and_undo_lands_on_a",
         d.get("after_rewind2") == "a",
         f"Rewind a une date entre le Rewind et l'Undo : champ="
         f"{d.get('after_rewind2')} attendu 'a' ; fenetre="
         f"{d.get('rewind2_window')} compte rendu={report!r}. Sur 'b', le "
         f"champ porte l'ancienne valeur NEW et le compte rendu annonce un "
         f"succes."),
        ("U4_undo_of_that_rewind_lands_on_c",
         d.get("after_undo2") == "c",
         f"apres l'Undo de ce Rewind={d.get('after_undo2')} attendu 'c'."),
        ("U5_undo_walks_the_reverse_of_the_plan",
         not d.get("u5_crash") and bool(d.get("u5_applied"))
         and d.get("u5_undo_walk") == list(reversed(d.get("u5_applied") or [])),
         f"ordre applique par le Rewind={d.get('u5_applied')} ordre de "
         f"l'Undo={d.get('u5_undo_walk')} crash={d.get('u5_crash')} : l'Undo "
         f"doit parcourir l'exact inverse du plan ; re-trie par date, il "
         f"refait les phases du Rewind dans le meme sens."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
