"""rw_undo_session_compensates - "Annuler toute la session" remet les entites dans leur etat d'avant la session.

Constat couvert : item D du lot dialogue (US-4.10.01, signale au
proprietaire aux deux tours precedents).

Mecanisme
---------
`_undo_session` lisait les evenements UTILISATEUR de la session et les
passait a `UndoRunner`. Or `undo_apply_one` inverse un RESTORE, pas une
edition : sur un evenement INSERT il reinsere l'entite, sur un DELETE il
supprime une entite qui correspond a l'instantane, sur un UPDATE il
reecrit les valeurs NEW. Applique a la session elle-meme, c'est l'inverse
de ce que demande l'utilisateur : l'entite ajoutee par la session est
reinseree une seconde fois, l'entite supprimee reste supprimee, la valeur
modifiee reste modifiee, et le message annonce "Session annulee : N
evenement(s) neutralise(s)". Aucune trace n'est ecrite : le journal ne sait
rien de l'annulation.

L'annulation d'une session est une compensation de ses evenements : elle
passe desormais par le moteur du Rewind (`StrictRestoreRunner` : preuve
d'identite de la cible, atomicite par couche, relecture du fichier apres
commit, traces), sur les evenements de la session dedoublonnes par leurs
traces (`collapse_rewind_events_with_stats`). Une entite modifiee APRES la
session par une autre edition n'est pas touchee : ecrire l'ancienne valeur
de la session effacerait cette edition posterieure ; elle est refusee et
nommee.

Ce qui est prouve, sur de vrais GeoPackage, avec la vraie capture
(tracker, file d'ecriture, journal), les vraies methodes du dialogue et les
vrais runners :

  M  session = ajout de K4, modification de K1, suppression de K3 :
     apres "Annuler toute la session", le fichier est dans son etat
     d'avant la session (K4 absente, K1 d'origine, K3 revenue), les
     compensations sont journalisees, le message ne ment pas, et
     "Annuler le dernier restore" remet l'etat de fin de session.
  L  K1 modifiee par la session puis par une edition posterieure : la
     valeur posterieure reste, le refus est dit.

Verdict avant correctif : FAIL (M). Verdict apres correctif : PASS.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rw_undo_session_compensates"
INVARIANT = "US-4.10.01"

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


def _state(path):
    """{code: v} as the user reads the file back, duplicates kept visible."""
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    out = {}
    for f in lyr.getFeatures():
        key = f["code"]
        while key in out:
            key += "+dup"
        out[key] = f["v"]
    return out


def _fid(layer, code):
    return next(f.id() for f in layer.getFeatures() if f["code"] == code)


def _session_of(journal, marker):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT session_id FROM audit_event WHERE restored_from_event_id IS NULL"
            " AND attributes_json LIKE ? ORDER BY event_id LIMIT 1",
            (f"%{marker}%",)).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _traces(journal, fp):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE restored_from_event_id IS NOT NULL"
            " AND invalidated_at IS NULL AND datasource_fingerprint = ?",
            (fp,)).fetchone()[0])
    finally:
        conn.close()


def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_undo_session_")


def run(ctx):
    import recoverland.recover_dialog as rd
    from qgis.core import QgsProject, QgsFeature, QgsGeometry, QgsPointXY
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump
    h = _h()
    d = ctx.data
    tmp = d["tmp"]
    flog(f"rw_undo_session_compensates run start: trace_id={ctx.trace_id}")
    rows = [("K1", "a", 1, 1), ("K2", "w", 2, 2), ("K3", "z", 3, 3)]
    path_m, lm = _gpkg(tmp, "sess_m", rows)
    path_l, ll = _gpkg(tmp, "sess_l", rows)
    QgsProject.instance().addMapLayers([lm, ll])
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()
    tracker.connect_layer(lm)
    tracker.connect_layer(ll)
    fp_m = compute_datasource_fingerprint(lm)
    fp_l = compute_datasource_fingerprint(ll)
    layers = {fp_m: lm, fp_l: ll}
    logs, asked = [], []
    seams = h.Seams()
    dlg = None

    def settle(pred, timeout=20.0):
        h.pump_until(pred, timeout)
        pump(0.3)
        wq.wait_until_drained(5.0)

    def idle():
        return not dlg._restore_in_progress and dlg._restore_runner is None

    try:
        d["m_before"] = _state(path_m)
        idx = lm.fields().indexFromName("v")
        # Session M: one edit session, three kinds of edit.
        lm.startEditing()
        nf = QgsFeature(lm.fields())
        nf.setAttributes([None] * lm.fields().count())
        nf.setAttribute(lm.fields().indexFromName("code"), "K4")
        nf.setAttribute(idx, "n")
        nf.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(4, 4)))
        lm.addFeature(nf)
        lm.changeAttributeValue(_fid(lm, "K1"), idx, "b_SESSION_M")
        lm.deleteFeature(_fid(lm, "K3"))
        d["m_commit"] = bool(lm.commitChanges())
        # Session L, then a later edit of the same entity by another session.
        ll.startEditing()
        ll.changeAttributeValue(_fid(ll, "K1"), idx, "b_SESSION_L")
        d["l_commit"] = bool(ll.commitChanges())
        time.sleep(0.05)
        ll.startEditing()
        ll.changeAttributeValue(_fid(ll, "K1"), idx, "c_LATER")
        d["l_commit2"] = bool(ll.commitChanges())
        pump(0.3)
        wq.wait_until_drained(5.0)
        d["m_after_session"] = _state(path_m)
        sess_m = _session_of(jm.path, "b_SESSION_M")
        sess_l = _session_of(jm.path, "b_SESSION_L")
        d["sessions"] = (sess_m, sess_l)
        dlg = h.build_dialog(rd, jm, wq, [fp_m, fp_l])
        dlg._tracker = tracker
        seams.set(rd, "find_target_layer",
                  lambda evt, _c=None: layers.get(evt.datasource_fingerprint))
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        # M: the whole session is undone.
        dlg._undo_session(sess_m)
        settle(idle)
        d["m_after_undo_session"] = _state(path_m)
        d["m_traces"] = _traces(jm.path, fp_m)
        d["m_logs"] = list(logs)
        d["m_button"] = dlg.undo_last_btn.enabled
        # ... and "Annuler le dernier restore" brings the session back.
        done = []
        original = dlg._on_undo_done
        dlg._on_undo_done = lambda res, _o=original: (_o(res), done.append(res))
        dlg._undo_last_restore()
        settle(lambda: bool(done))
        dlg._on_undo_done = original
        d["m_after_undo_last"] = _state(path_m)
        # L: the session's entity was edited again afterwards.
        n_logs = len(logs)
        dlg._undo_session(sess_l)
        settle(idle)
        d["l_after_undo_session"] = _state(path_l)
        d["l_logs"] = logs[n_logs:]
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
        flog(f"rw_undo_session_compensates run end: trace_id={ctx.trace_id}")
        shutil.rmtree(tmp, ignore_errors=True)


def _msgs(logs) -> str:
    return " || ".join(f"{lvl}:{m}" for m, lvl in logs or [])


def assertions(ctx):
    d = ctx.data
    before = d.get("m_before") or {}
    after_session = d.get("m_after_session") or {}
    m_logs = d.get("m_logs") or []
    l_logs = d.get("l_logs") or []
    l_state = d.get("l_after_undo_session") or {}
    return [
        ("M_control_session_captured",
         not d.get("crash") and all(d.get("sessions") or (None,))
         and after_session == {"K1": "b_SESSION_M", "K2": "w", "K4": "n"},
         f"etat apres la session={after_session} sessions={d.get('sessions')} "
         f"crash={d.get('crash')} (controle)"),
        ("M_undo_session_restores_the_state_before_it",
         d.get("m_after_undo_session") == before,
         f"apres 'Annuler toute la session'={d.get('m_after_undo_session')} "
         f"attendu {before} : l'entite ajoutee par la session doit "
         f"disparaitre (pas etre reinseree une seconde fois), la supprimee "
         f"revenir, la modifiee retrouver sa valeur."),
        ("M_compensations_are_journalled",
         (d.get("m_traces") or 0) == 3,
         f"traces actives de la couche={d.get('m_traces')} attendu 3 : sans "
         f"trace, le Rewind suivant rejoue ces compensations."),
        ("M_message_does_not_lie",
         any(lvl == "INFO" and "3" in m and "session" in m.lower()
             for m, lvl in m_logs)
         and not any("neutralise" in m for m, _l in m_logs),
         f"messages={_msgs(m_logs)!r} : le compte rendu doit compter ce qui "
         f"a ete remis, par categorie, comme un Rewind."),
        ("M_undo_last_brings_the_session_back",
         d.get("m_button") is True
         and d.get("m_after_undo_last") == after_session,
         f"bouton={d.get('m_button')} apres 'Annuler le dernier restore'="
         f"{d.get('m_after_undo_last')} attendu {after_session} : "
         f"l'annulation de session est un run comme un autre."),
        ("L_later_edit_is_not_overwritten",
         l_state.get("K1") == "c_LATER",
         f"K1 apres l'annulation de la session L={l_state.get('K1')} attendu "
         f"'c_LATER' : l'edition posterieure a la session serait effacee "
         f"par l'ancienne valeur."),
        ("L_refusal_is_named",
         any(lvl in ("WARNING", "ERROR") and "apres la session" in m
             for m, lvl in l_logs),
         f"messages={_msgs(l_logs)!r} : l'utilisateur doit lire quelle "
         f"entite n'a pas ete annulee et pourquoi."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
