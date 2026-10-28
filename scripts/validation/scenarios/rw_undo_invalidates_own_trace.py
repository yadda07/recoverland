# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_undo_invalidates_own_trace - un Undo n'invalide que la trace ecrite par le run qu'il annule.

Constat couvert : revue du lot dialogue, BLOCKER "l'Undo invalide TOUTES
les traces actives de l'evenement".

Mecanisme
---------
"Annuler le dernier restore" remet les entites du dernier run, puis marque
au journal que la compensation de ce run est defaite : il invalide la
trace (`restored_from_event_id` = evenement compense). La requete visait
toutes les traces ACTIVES de l'evenement, pas celle du run annule. Le code
d'origine n'en produisait jamais deux a la fois (son saut avant etait un
auto-undo) ; la v7 le fait : un saut avant compense la trace par une trace
de trace et laisse la premiere active.

  e1  K1 'a' -> 'b_E1' (edition de l'utilisateur)
  R1  Rewind avant e1 : trace T2 (de e1), K1 = 'a'
  R2  saut avant, apres e1 : trace de trace T3 (de T2), T2 reste active,
      K1 = 'b_E1'
  R3  Rewind avant e1 (ou "Annuler toute la session") : trace T4 (de e1),
      K1 = 'a'
  U   "Annuler le dernier restore" : K1 = 'b_E1'

U invalidait T2 ET T4, au meme instant. Le journal affirmait alors que la
compensation de R1 avait ete defaite a cet instant, ce qui est faux, et la
relecture ("plus aucune trace active pour la source") le confirmait.

Pour l'utilisateur : un Rewind a une date entre R2 et R3, ou K1 valait
'b_E1', ecrit 'a' et annonce "2 entite(s) restauree(s) avec succes" ; un
Rewind a une date entre R3 et l'Undo, ou K1 valait 'a', est refuse
("event 1 was compensated several times after the cutoff").

Ce qui est prouve, de bout en bout sur un vrai GeoPackage, avec la vraie
capture (tracker, file d'ecriture, journal), la vraie fenetre, les vrais
StrictRestoreRunner et UndoRunner, pilotes par les vraies methodes du
dialogue ; R3 est un Rewind (cas R) ou "Annuler toute la session" (cas S) :

  *_control              R1, R2, R3 et l'Undo posent les valeurs attendues ;
  *_run_mark_read_at_launch
                         le dialogue garde avec son dernier run (R3) la
                         marque du journal lue a son lancement : au moins
                         le haut du journal avant R3, en dessous de ses
                         traces ;
  *_undo_invalidates_only_its_run_trace
                         apres l'Undo, la trace de R3 est invalidee, celles
                         de R1 et de R2 restent actives ;
  *_nav_*                Rewind a une date entre R1 et R2 ('a'), entre R2
                         et R3 ('b_E1'), entre R3 et l'Undo ('a'), puis de
                         nouveau entre R2 et R3 ('b_E1') : la valeur du
                         FICHIER, et un compte rendu sans refus ni echec.

Et sur un journal construit a la main (vraies methodes du dialogue) :

  V1  marque du run connue : seule la trace posterieure a la marque est
      invalidee, la relecture dit vrai ;
  V2  une invalidation qui emporte aussi une trace plus ancienne (l'UPDATE
      d'avant) est denoncee par la relecture : le journal n'est plus
      d'accord avec les couches, l'utilisateur doit l'apprendre ;
  V3  marque inconnue : seule la trace la plus recente de la source est
      invalidee (repli) ;
  V4  le run n'a ecrit aucune trace pour la source : rien n'est invalide
      (surtout pas la trace d'un run anterieur) et c'est dit.

Verdict avant correctif : FAIL (R/S run_mark_read_at_launch, R/S
undo_invalidates_only_its_run_trace, R/S nav entre R2 et R3 et entre R3 et
l'Undo, V1, V2, V3, V4).
Verdict apres correctif : PASS.
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

SCENARIO_ID = "rw_undo_invalidates_own_trace"
INVARIANT = "I-UNDO-OWN-TRACE"

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


def _value(path, code="K1"):
    """Valeur du champ relue du FICHIER, par une couche neuve."""
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    for f in lyr.getFeatures():
        if f["code"] == code:
            return f["v"]
    return None


def _ro(journal):
    return sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)


def _high_water(journal) -> int:
    conn = _ro(journal)
    try:
        return int(conn.execute(
            "SELECT COALESCE(MAX(event_id), 0) FROM audit_event").fetchone()[0])
    finally:
        conn.close()


def _traces(journal) -> list:
    """[(event_id, restored_from, invalidated_at)] de chaque trace, par id."""
    conn = _ro(journal)
    try:
        return [(int(r[0]), int(r[1]), r[2]) for r in conn.execute(
            "SELECT event_id, restored_from_event_id, invalidated_at "
            "FROM audit_event WHERE restored_from_event_id IS NOT NULL "
            "ORDER BY event_id")]
    finally:
        conn.close()


def _session_of(journal, marker):
    conn = _ro(journal)
    try:
        row = conn.execute(
            "SELECT session_id FROM audit_event WHERE restored_from_event_id IS NULL"
            " AND attributes_json LIKE ? ORDER BY event_id LIMIT 1",
            (f"%{marker}%",)).fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _tick():
    time.sleep(1.1)


def _now():
    return datetime.now(timezone.utc)


class _Env:
    """Un GeoPackage, sa capture reelle et le dialogue greffe dessus."""

    def __init__(self, tmp, name, rows):
        from qgis.core import QgsProject
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        from recoverland.core.identity import compute_datasource_fingerprint
        import recoverland.recover_dialog as rd
        h = _h()
        d = os.path.join(tmp, name)
        os.makedirs(d, exist_ok=True)
        self.path, self.layer = _gpkg(d, name, rows)
        QgsProject.instance().addMapLayers([self.layer])
        project = os.path.join(d, "p.qgz")
        open(project, "w").close()
        self.jm = JournalManager()
        self.jm.open_for_project(project, os.path.join(d, "prof"))
        self.wq = WriteQueue()
        self.wq.start(self.jm.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()
        self.tracker.connect_layer(self.layer)
        self.fp = compute_datasource_fingerprint(self.layer)
        self.logs, self.asked, self.fetches = [], [], []
        self.seams = h.Seams()
        self.dlg = h.build_dialog(rd, self.jm, self.wq, [self.fp])
        self.dlg._tracker = self.tracker
        layer = self.layer
        self.seams.set(rd, "find_target_layer", lambda _e, _c=None: layer)
        self.seams.set(rd, "QMessageBox", h.message_box_class(rd, self.asked))
        self.seams.set(rd, "qlog",
                       lambda msg, level="INFO": self.logs.append((str(msg), level)))
        self.seams.set(rd, "VersionFetchThread", h.real_fetch_class(self.fetches))

    def settle(self, pred, timeout=20.0):
        from scripts.validation.plugin_harness import pump
        _h().pump_until(pred, timeout)
        pump(0.3)
        self.wq.wait_until_drained(5.0)

    def idle(self):
        dlg = self.dlg
        return (not dlg._is_recovering and not dlg._restore_in_progress
                and dlg._restore_runner is None)

    def rewind(self, cutoff_dt):
        self.dlg.time_slider.cutoff_dt = cutoff_dt
        self.dlg._is_recovering = True
        self.dlg._recover_version_mode()
        self.settle(self.idle)

    def undo(self):
        done = []
        dlg = self.dlg
        original = dlg._on_undo_done
        dlg._on_undo_done = lambda res, _o=original: (_o(res), done.append(res))
        try:
            dlg._undo_last_restore()
            self.settle(lambda: bool(done))
        finally:
            dlg._on_undo_done = original
        return bool(done)

    def undo_session(self, sid):
        self.dlg._undo_session(sid)
        self.settle(self.idle)

    def close(self):
        from qgis.core import QgsProject
        self.seams.restore()
        _h().close_dialog(self.dlg)
        if self.layer.isEditable():
            self.layer.rollBack()
        self.tracker.deactivate()
        self.tracker.disconnect_all()
        self.wq.stop()
        self.jm.close()
        QgsProject.instance().removeAllMapLayers()


def _case_nav(ctx, tmp, key, third):
    """e1 ; R1 avant e1 ; R2 saut avant ; R3 (Rewind ou session) ; Undo ; navigation."""
    d = ctx.data
    env = _Env(tmp, key, [("K1", "a", 1, 1), ("K2", "w", 2, 2)])
    lyr = env.layer
    idx = lyr.fields().indexFromName("v")
    try:
        _tick()
        t_a = _now()
        _tick()
        fid = next(f.id() for f in lyr.getFeatures() if f["code"] == "K1")
        lyr.startEditing()
        lyr.changeAttributeValue(fid, idx, "b_E1")
        lyr.commitChanges()
        env.settle(lambda: True)
        _tick()
        t_b = _now()
        _tick()
        sid = _session_of(env.jm.path, "b_E1")
        marks = {"r1": _high_water(env.jm.path)}
        env.rewind(t_a)                                   # R1 -> a
        d[f"{key}_r1"] = _value(env.path)
        _tick()
        t_c = _now()
        _tick()
        marks["r2"] = _high_water(env.jm.path)
        env.rewind(t_b)                                   # R2 saut avant -> b_E1
        d[f"{key}_r2"] = _value(env.path)
        _tick()
        t_d = _now()
        _tick()
        marks["r3"] = _high_water(env.jm.path)
        if third == "session":
            env.undo_session(sid)                         # R3 -> a
        else:
            env.rewind(t_a)                               # R3 -> a
        d[f"{key}_r3"] = _value(env.path)
        # The mark the dialog kept with its last run (R3), read at launch.
        d[f"{key}_run_mark"] = getattr(env.dlg, "_last_run_mark", "absent")
        _tick()
        t_e = _now()
        _tick()
        marks["undo"] = _high_water(env.jm.path)
        n = len(env.logs)
        d[f"{key}_undo_done"] = env.undo()                # -> b_E1
        d[f"{key}_undo_logs"] = env.logs[n:]
        d[f"{key}_after_undo"] = _value(env.path)
        d[f"{key}_traces_after_undo"] = _traces(env.jm.path)
        d[f"{key}_marks"] = marks
        _tick()
        nav = []
        for label, t, expect in (("R1_R2", t_c, "a"), ("R2_R3", t_d, "b_E1"),
                                 ("R3_Undo", t_e, "a"), ("R2_R3_again", t_d, "b_E1")):
            n = len(env.logs)
            before = _value(env.path)
            env.rewind(t)
            nav.append((label, expect, before, _value(env.path),
                        _h().plan_marks(env.fetches[-1].result if env.fetches else []),
                        env.logs[n:]))
            _tick()
        d[f"{key}_nav"] = nav
        d[f"{key}_traces_end"] = _traces(env.jm.path)
    finally:
        env.close()


def _case_v(ctx):
    """V1..V4 : la selection et la relecture de l'invalidation, journal construit a la main."""
    import types
    import recoverland.recover_dialog as rd
    h = _h()
    d = ctx.data
    fp = "ds_own_trace_v"
    for key, mark, patch in (("v1", "after_t3", None), ("v2", "after_t3", "legacy"),
                             ("v3", None, None), ("v4", "after_t4", None)):
        tmp = tempfile.TemporaryDirectory(prefix="rl_own_trace_v_")
        try:
            path = h.new_journal(tmp.name, f"{key}.sqlite")
            e1 = h.insert_event(path, fp, "own", "UPDATE", 100, fid=1)
            t2 = h.insert_event(path, fp, "own", "UPDATE", 200, fid=1,
                                restored_from=e1, schema=7)       # R1
            t3 = h.insert_event(path, fp, "own", "UPDATE", 300, fid=1,
                                restored_from=t2, schema=7)       # R2, trace de trace
            t4 = h.insert_event(path, fp, "own", "UPDATE", 400, fid=1,
                                restored_from=e1, schema=7)       # R3
            dlg = h.build_dialog(rd, h.Journal(path), None, [fp])
            logs = []
            seams = h.Seams()
            try:
                seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
                # The last run is R3: its journal high-water mark was t3's id
                # when it was launched (V4: t4's, i.e. R3 wrote no trace).
                dlg._last_run_mark = {"after_t3": t3, "after_t4": t4}.get(mark)
                if patch == "legacy":
                    def legacy(self, undone_ids):
                        conn = self._journal.create_write_connection()
                        try:
                            cur = conn.executemany(
                                "UPDATE audit_event SET invalidated_at = ? "
                                "WHERE restored_from_event_id = ? "
                                "AND invalidated_at IS NULL",
                                [(_now().isoformat(), int(i)) for i in undone_ids])
                            conn.commit()
                            return cur.rowcount
                        finally:
                            conn.close()
                    dlg._invalidate_trace_events = types.MethodType(legacy, dlg)
                d[f"{key}_ok"] = dlg._invalidate_undone_traces([e1], "scenario")
            finally:
                seams.restore()
                h.close_dialog(dlg)
            d[f"{key}_ids"] = {"e1": e1, "t2": t2, "t3": t3, "t4": t4}
            d[f"{key}_traces"] = _traces(path)
        finally:
            tmp.cleanup()


def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_own_trace_")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    tmp = ctx.data["tmp"]
    flog(f"rw_undo_invalidates_own_trace run start: trace_id={ctx.trace_id}")
    for key, fn in (("r", lambda: _case_nav(ctx, tmp, "r", "rewind")),
                    ("s", lambda: _case_nav(ctx, tmp, "s", "session")),
                    ("v", lambda: _case_v(ctx))):
        try:
            fn()
        except Exception:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data[f"{key}_crash"] = traceback.format_exc()[-2500:]
    QgsProject.instance().clear()
    shutil.rmtree(tmp, ignore_errors=True)
    flog(f"rw_undo_invalidates_own_trace run end: trace_id={ctx.trace_id}")


def _said(logs) -> str:
    return " || ".join(f"{lvl}:{m[:400]}" for m, lvl in logs or [])


def _by_run(traces, marks) -> dict:
    """{run: [(trace id, invalidated_at)]} : chaque trace rangee sous le run qui l'a ecrite."""
    bounds = [("R1", marks.get("r1"), marks.get("r2")),
              ("R2", marks.get("r2"), marks.get("r3")),
              ("R3", marks.get("r3"), marks.get("undo"))]
    out = {name: [] for name, _lo, _hi in bounds}
    for tid, _src, inv in traces or ():
        for name, lo, hi in bounds:
            if lo is not None and hi is not None and lo < tid <= hi:
                out[name].append((tid, inv))
    return out


def assertions(ctx):
    d = ctx.data
    out = []
    for key, third in (("r", "un Rewind avant e1"),
                       ("s", "'Annuler toute la session'")):
        k = key.upper()
        undo_said = _said(d.get(f"{key}_undo_logs"))
        out.append((
            f"{k}_control",
            not d.get(f"{key}_crash") and d.get(f"{key}_r1") == "a"
            and d.get(f"{key}_r2") == "b_E1" and d.get(f"{key}_r3") == "a"
            and d.get(f"{key}_undo_done") and d.get(f"{key}_after_undo") == "b_E1",
            f"R1={d.get(f'{key}_r1')} (attendu a) R2={d.get(f'{key}_r2')} (b_E1) "
            f"R3={d.get(f'{key}_r3')} (a, par {third}) Undo="
            f"{d.get(f'{key}_after_undo')} (b_E1) crash={d.get(f'{key}_crash')} "
            f"(controle : sans ces valeurs le reste ne prouve rien)"))
        runs = _by_run(d.get(f"{key}_traces_after_undo"), d.get(f"{key}_marks") or {})
        mark = d.get(f"{key}_run_mark")
        r3_ids = [t for t, _inv in runs["R3"]]
        out.append((
            f"{k}_run_mark_read_at_launch",
            isinstance(mark, int) and bool(r3_ids)
            and (d.get(f"{key}_marks") or {}).get("r3", -1) <= mark < min(r3_ids),
            f"marque gardee avec le dernier run={mark} ; journal avant R3="
            f"{(d.get(f'{key}_marks') or {}).get('r3')} ; traces de R3={r3_ids} : "
            f"sans marque lue au lancement, l'Undo ne sait pas quelles traces "
            f"sont celles du run qu'il annule"))
        ok = (bool(runs["R1"]) and bool(runs["R2"]) and bool(runs["R3"])
              and all(inv is None for _t, inv in runs["R1"] + runs["R2"])
              and all(inv is not None for _t, inv in runs["R3"])
              and "ERROR:" not in undo_said)
        out.append((
            f"{k}_undo_invalidates_only_its_run_trace",
            ok,
            f"traces par run apres l'Undo={runs} (R1 et R2 doivent rester "
            f"actives, R3 seule invalidee) ; compte rendu de l'Undo : "
            f"{undo_said} : l'Undo de R3 marquait aussi la compensation de R1 "
            f"comme defaite, le journal ne decrit plus la couche et les "
            f"Rewind suivants partent d'un historique faux"))
        for label, expect, before, got, window, logs in d.get(f"{key}_nav") or []:
            said = _said(logs)
            loud = [m for m, lvl in logs or [] if lvl in ("WARNING", "ERROR")]
            out.append((
                f"{k}_nav_{label}",
                got == expect and not loud,
                f"Rewind a une date entre {label.replace('_again', '').replace('_', ' et ')} : "
                f"K1 avant={before} apres={got} attendu={expect} ; fenetre={window} ; "
                f"compte rendu : {said} : l'utilisateur rembobine a une date "
                f"connue et obtient une autre valeur (ou un refus) apres un "
                f"Undo qui a reecrit l'histoire de R1"))
    v = {k: d.get(k) for k in d if k.startswith("v")}
    out.append(("V0_no_crash", not d.get("v_crash"),
                f"crash={d.get('v_crash')}"))

    def states(key):
        ids = d.get(f"{key}_ids") or {}
        rows = {tid: inv for tid, _src, inv in d.get(f"{key}_traces") or []}
        return {name: ("active" if rows.get(tid, "x") is None else
                       "invalidated" if tid in rows else "none")
                for name, tid in ids.items() if name != "e1"}

    s1 = states("v1")
    out.append((
        "V1_known_mark_invalidates_only_the_run_trace",
        v.get("v1_ok") is True
        and s1 == {"t2": "active", "t3": "active", "t4": "invalidated"},
        f"verdict={v.get('v1_ok')} traces={s1} (attendu : t4, celle du dernier "
        f"run, invalidee ; t2 (R1) et t3 (R2) actives ; relecture vraie)"))
    s2 = states("v2")
    out.append((
        "V2_read_back_denounces_a_collateral_invalidation",
        v.get("v2_ok") is False and s2.get("t2") == "invalidated",
        f"verdict={v.get('v2_ok')} traces={s2} : une invalidation qui emporte "
        f"aussi la trace de R1 doit etre dite (relecture en echec) ; dire "
        f"'journal a jour' laisse l'utilisateur rembobiner sur un historique "
        f"faux sans avertissement"))
    s3 = states("v3")
    out.append((
        "V3_unknown_mark_falls_back_on_the_newest_trace",
        v.get("v3_ok") is True
        and s3 == {"t2": "active", "t3": "active", "t4": "invalidated"},
        f"verdict={v.get('v3_ok')} traces={s3} (attendu : sans marque, la trace "
        f"la plus recente de la source seule, t4 ; t2 reste active)"))
    s4 = states("v4")
    out.append((
        "V4_run_without_trace_invalidates_nothing_and_says_so",
        v.get("v4_ok") is False
        and s4 == {"t2": "active", "t3": "active", "t4": "active"},
        f"verdict={v.get('v4_ok')} traces={s4} (attendu : le run annule n'a "
        f"ecrit aucune trace de e1, rien n'est invalide et la relecture echoue : "
        f"invalider t2 ou t4 defairait au journal la compensation d'un autre run)"))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
