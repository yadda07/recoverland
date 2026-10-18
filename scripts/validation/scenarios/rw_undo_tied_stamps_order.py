"""rw_undo_tied_stamps_order - un journal ancien (Undo a horodatage unique) se rembobine dans l'ordre de l'Undo.

Invariant : I-UNDO-ORDER (un Rewind a une date situee entre un Rewind et
son Undo rend EXACTEMENT l'etat de cette date, y compris sur un journal
ecrit par une version qui horodatait l'Undo d'un seul instant).

Mecanisme
---------
Un Undo n'ecrit aucun evenement : il invalide les traces du Rewind qu'il
defait (`invalidated_at`). Avant cette version, `_invalidate_trace_events`
posait LE MEME instant sur toutes les traces d'un Undo. Le dialogue
horodate desormais chaque invalidation dans l'ordre de l'Undo (instants
strictement croissants, scenario rw_undo_order_journal), mais les journaux
deja ecrits gardent leurs egalites.

Un Rewind ulterieur a une date T entre ce Rewind et cet Undo doit refaire
les compensations que l'Undo a defaites apres T (elements de plan
"recompensate" de `event_stream_repository._history_plan_items`), du plus
recent au plus ancien. La fenetre les classait par (instant, identifiant
de la trace). A instant egal, c'est l'identifiant de la trace qui
tranchait, c'est-a-dire l'ordre d'ECRITURE du Rewind defait, qui est
l'inverse de l'ordre dans lequel l'Undo a re-applique les evenements (un
Rewind compense du plus recent au plus ancien, son Undo re-applique du
plus ancien au plus recent).

Pour l'utilisateur : le champ passe de 'a' a 'b' (e1) puis a 'c' (e2).
Rewind avant e1 : 'a'. Undo (ancienne version) : 'c'. Rewind a une date
entre ce Rewind et cet Undo, ou le champ valait 'a' : la fenetre refait
la compensation de e1 AVANT celle de e2 ('c' -> 'a' puis 'a' -> 'b'), le
champ finit sur 'b', l'ancienne valeur NEW, et le rapport annonce un
succes.

Correctif : une invalidation se place a (instant, -identifiant de la
trace) (`rewind_dedup.undo_position`) : a instant egal, la trace ecrite en
DERNIER a ete defaite en PREMIER. Quand l'ordre ne peut pas etre deduit
(l'ancien Undo invalidait toutes les traces actives d'un evenement, y
compris celles d'un Rewind anterieur qu'il n'avait pas defait : deux
traces du MEME evenement au meme instant), le Rewind est refuse en nommant
l'Undo et son instant.

Cas prouves
-----------
  T1  de bout en bout sur un vrai GeoPackage : vraie capture (tracker,
      file d'ecriture, journal), vraie fenetre, vrai StrictRestoreRunner,
      vrai UndoRunner ; l'Undo est horodate a l'ancienne (un seul instant,
      la requete de la version publiee). Rewind entre les deux : 'a'.
  T2  journal ecrit par la version publiee (traces de schema 6, sans
      identite exacte), meme forme : le plan compense e2 avant e1.
  T3  deux traces du meme evenement (restauration Mode A refaite apres une
      re-saisie) invalidees au meme instant par l'ancien Undo : refus qui
      nomme l'Undo et son instant, pas un refus generique.
  T4  (controle) Undo horodate a la nouvelle maniere (instants croissants
      dans l'ordre de l'Undo) : meme resultat, l'ordre vient des instants.

Egalite du meme evenement, a toutes les dates de coupure (revue w, 2e tour)
--------------------------------------------------------------------------
Le journal de T3 : e (a->b) a 100 s, t1 (restauration Mode A de e) a
200 s, e_again (re-saisie a->N) a 300 s, t2 (seconde restauration de e) a
400 s, ancien Undo de la derniere restauration a 600 s qui estampille t1 ET
t2 du meme instant. Physiquement, l'Undo n'a defait que t2 (il re-ecrit
'b') ; t1 reste ecrite sous la re-saisie. Le journal ne dit plus laquelle
des deux traces l'Undo a defaite.

La detection ne regardait que les bascules "vivantes" de la racine (une
trace creee ET invalidee apres la coupure est lue comme une paire neutre,
son ecriture et son Undo s'annulant) et n'etait appelee que dans le chemin
ecriture-par-ecriture. Le chemin "effet net", celui de tous les journaux
publies (traces de schema 6), ne la voyait jamais. Or une trace
estampillee par l'ancien Undo SANS avoir ete defaite n'est justement pas
une paire neutre.

Pour l'utilisateur, avec la coupure entre e_again et t2 (le champ valait
N) : le plan re-compense e, ecrit 'a' la ou le champ valait N, et le
rapport annonce un succes. Avec la coupure entre e et t1 (le champ valait
'b') et une re-saisie de la meme valeur 'b' : les deux traces sont des
"paires neutres", le plan compense e_again, ecrit 'a' la ou le champ
valait 'b', et le rapport annonce un succes. Dans les deux cas la valeur
lue avant ecriture est bien celle qu'attend la compensation : rien ne
l'arrete.

  T3_s6_350 / T3_s7_350  coupure entre e_again et t2, schema 6 (ce
      qu'ecrivait vraiment la version publiee) et schema 7 : refus nomme.
  T3_s6_150 / T3_s7_150  coupure entre e et t1 (les deux traces egales
      sont posterieures a la coupure), re-saisie de la meme valeur : refus
      nomme.
  T3_s6_700  (controle) coupure apres l'Undo : l'egalite est dans le
      passe de la date demandee, rien a faire, aucun refus. C'est la date
      que le refus conseille.
  T5  vrai GeoPackage, vraie capture, vrai RestoreRunner pour t1 ; t2 et
      l'Undo ecrits comme la version publiee les ecrivait (ecriture
      physique hors capture, trace copiee, estampille unique) ; puis la
      vraie fenetre, la vraie deduplication et le vrai StrictRestoreRunner
      a une date entre la re-saisie et t2 : la couche ne doit pas etre
      touchee et l'echec doit nommer l'Undo.
  T6  meme montage, coupure entre e et t1, re-saisie de la meme valeur.

Verdict avant correctif : FAIL (T1, T2, T3 ; puis, au 2e tour, T3_s6_350,
T3_s7_350, T3_s6_150, T3_s7_150, T5, T6). Apres correctif : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_undo_tied_stamps_order"
INVARIANT = "I-UNDO-ORDER"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_T0 = datetime(2026, 9, 1, 8, 0, 0, tzinfo=timezone.utc)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _iso(seconds):
    return (_T0 + timedelta(seconds=seconds)).isoformat()


# --------------------------------------------------------------------------
# T1 / T4 : la vraie pile sur un GeoPackage
# --------------------------------------------------------------------------
def _gpkg(tmpdir, name):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)"
                         "&field=v:string(40)", "m", "memory")
    mem.startEditing()
    for code, v, x in (("C1", "a", 1.0), ("C2", "x", 2.0)):
        f = QgsFeature(mem.fields())
        f.setAttributes([code, v])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, x)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
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


class _Stack:
    """Capture, journal, file d'ecriture et runners reels sur une couche."""

    def __init__(self, ctx, tmp, name):
        from qgis.core import QgsProject
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        from recoverland.core.identity import compute_datasource_fingerprint
        self.ctx = ctx
        self.dir = os.path.join(tmp, name)
        os.makedirs(self.dir, exist_ok=True)
        self.path, self.layer = _gpkg(self.dir, name)
        QgsProject.instance().addMapLayer(self.layer)
        project = os.path.join(self.dir, "p.qgz")
        open(project, "w").close()
        self.jm = JournalManager()
        self.jm.open_for_project(project, self.dir)
        self.wq = WriteQueue()
        self.wq.start(self.jm.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()
        self.tracker.connect_layer(self.layer)
        self.fp = compute_datasource_fingerprint(self.layer)

    def close(self):
        from qgis.core import QgsProject
        try:
            self.tracker.deactivate()
            self.tracker.disconnect_all()
        finally:
            self.wq.stop()
            self.jm.close()
            QgsProject.instance().removeMapLayer(self.layer.id())

    def drain(self):
        from scripts.validation.plugin_harness import pump
        pump(0.05)
        self.wq.wait_until_drained(10.0)
        pump(0.02)

    def commit(self, code, value):
        lyr = self.layer
        fid = next(f.id() for f in lyr.getFeatures() if f["code"] == code)
        lyr.startEditing()
        lyr.changeAttributeValue(fid, lyr.fields().indexFromName("v"), value)
        ok = bool(lyr.commitChanges())
        self.drain()
        return ok

    def rows(self, sql, params=()):
        conn = sqlite3.connect(f"file:{Path(self.jm.path).as_posix()}?mode=ro",
                               uri=True)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def _wait(self, runner, timeout=30.0):
        from scripts.validation.plugin_harness import pump
        done = {}
        runner.finished.connect(lambda res: done.setdefault("r", res))
        runner.start()
        deadline = time.monotonic() + timeout
        while "r" not in done and time.monotonic() < deadline:
            pump(0.02)
        self.drain()
        return done.get("r")

    def rewind(self, stamp, journal_path=None):
        """Vraie fenetre, vrai dedup, vrai StrictRestoreRunner en un passage.

        *journal_path* : le journal que le dialogue passe au runner (T5, T6).
        """
        import recoverland.restore_runner as rr
        from recoverland.core.event_stream_repository import fetch_events_after_cutoff
        from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
        from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
        cutoff = RestoreCutoff(CutoffType.BY_DATE, stamp, inclusive=True)
        self.drain()
        conn = self.jm.create_read_connection()
        try:
            window = fetch_events_after_cutoff(conn, self.fp, cutoff,
                                               trace_id=self.ctx.trace_id)
        finally:
            conn.close()
        active, _stats = collapse_rewind_events_with_stats(window)
        plan = [(e.event_id, (e.commit_id or "user").split(":")[-1].split("@")[0])
                for e in active]
        if not active:
            return None, plan
        kwargs = {"journal_path": journal_path} if journal_path else {}
        runner = rr.StrictRestoreRunner(
            active, lambda ev: self.layer if ev.datasource_fingerprint == self.fp
            else None, cutoff, write_queue=self.wq, tracker=self.tracker,
            trace_id=self.ctx.trace_id, **kwargs)
        return self._wait(runner), plan

    def undo(self, result, old_style):
        """Vrai UndoRunner, puis l'horodatage des traces defaites.

        old_style : la requete de la version publiee, un seul instant pour
        toutes les traces actives des evenements defaits. Sinon : un
        instant par trace, strictement croissant dans l'ordre de l'Undo
        (ce qu'ecrit le dialogue desormais).
        """
        import recoverland.restore_runner as rr
        by_ds = {fp: list(evts) for fp, evts in (result.by_ds or {}).items()}
        runner = rr.UndoRunner(
            by_ds, lambda ev: self.layer if ev.datasource_fingerprint == self.fp
            else None, tracker=self.tracker)
        res = self._wait(runner)
        undone = [e.event_id for evts in by_ds.values() for e in reversed(evts)]
        conn = sqlite3.connect(self.jm.path, timeout=10)
        try:
            base = datetime.now(timezone.utc)
            if old_style:
                ph = ",".join("?" for _ in undone)
                conn.execute(
                    "UPDATE audit_event SET invalidated_at = ? "  # nosec B608
                    f"WHERE restored_from_event_id IN ({ph}) "
                    "AND invalidated_at IS NULL",
                    [base.isoformat()] + undone)
            else:
                for i, sid in enumerate(undone):
                    conn.execute(
                        "UPDATE audit_event SET invalidated_at = ? "
                        "WHERE restored_from_event_id = ? "
                        "AND invalidated_at IS NULL",
                        ((base + timedelta(microseconds=i)).isoformat(), sid))
            conn.commit()
        finally:
            conn.close()
        return res


def _case_gpkg(ctx, tmp, old_style):
    from qgis.core import QgsProject
    tag = "old" if old_style else "new"
    out = {}
    s = _Stack(ctx, tmp, f"ordre_{tag}")
    try:
        time.sleep(0.05)
        t0 = _now()
        time.sleep(0.05)
        out["commits"] = [s.commit("C1", "b"), s.commit("C1", "c")]
        out["user_ids"] = [r[0] for r in s.rows(
            "SELECT event_id FROM audit_event WHERE restored_from_event_id "
            "IS NULL ORDER BY event_id")]
        res1, out["plan1"] = s.rewind(t0)
        out["after_rewind1"] = _value(s.path)
        out["rewind1_fail"] = getattr(res1, "total_fail", None)
        time.sleep(0.05)
        t_mid = _now()
        time.sleep(0.05)
        res_u = s.undo(res1, old_style)
        out["undo_fail"] = getattr(res_u, "total_fail", None)
        out["after_undo"] = _value(s.path)
        out["stamps"] = s.rows(
            "SELECT restored_from_event_id, event_id, invalidated_at FROM "
            "audit_event WHERE invalidated_at IS NOT NULL ORDER BY event_id")
        res2, out["plan2"] = s.rewind(t_mid)
        out["after_rewind2"] = _value(s.path)
        out["rewind2_fail"] = getattr(res2, "total_fail", None)
        out["rewind2_errors"] = list(getattr(res2, "errors", []) or [])[:4]
        out["c2"] = _value(s.path, "C2")
    except Exception:  # noqa: BLE001 - a crash is a result
        import traceback
        out["crash"] = traceback.format_exc()[-2000:]
    finally:
        s.close()
        QgsProject.instance().clear()
    return out


# --------------------------------------------------------------------------
# T5 / T6 : deux restaurations Mode A du meme evenement, ancien Undo
# --------------------------------------------------------------------------
def _raw_set(path, code, value):
    """Ecriture physique HORS capture (un second objet couche sur le fichier)."""
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "raw", "ogr")
    fid = next(f.id() for f in lyr.getFeatures() if f["code"] == code)
    idx = lyr.fields().indexFromName("v")
    return bool(lyr.dataProvider().changeAttributeValues({fid: {idx: value}}))


def _published_restore_again(s, model_trace):
    """Seconde restauration Mode A de e, comme l'ecrivait la version publiee.

    L'ancienne valeur de e ('a') ecrite, et une trace qui nomme e
    journalisee (copie de la premiere, a sa propre date). Retourne son id.
    """
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_SQL
    ok = _raw_set(s.path, "C1", "a")
    s.layer.reload()
    conn = sqlite3.connect(s.jm.path, timeout=10)
    try:
        cols = AUDIT_EVENT_INSERT_SQL
        conn.execute(
            f"INSERT INTO audit_event ({cols}) SELECT {cols} "  # nosec B608
            "FROM audit_event WHERE event_id = ?", (model_trace,))
        tid = conn.execute("SELECT MAX(event_id) FROM audit_event").fetchone()[0]
        conn.execute("UPDATE audit_event SET created_at = ?, event_uid = NULL, "
                     "commit_id = NULL WHERE event_id = ?", (_now(), tid))
        conn.commit()
    finally:
        conn.close()
    return ok, tid


def _published_undo(s, source_id):
    """L'Undo publie de la derniere restauration : NEW de e re-ecrit ('b'),
    et TOUTES les traces actives de e estampillees d'un seul instant (la
    requete de la version publiee). Retourne l'instant."""
    ok = _raw_set(s.path, "C1", "b")
    s.layer.reload()
    stamp = _now()
    conn = sqlite3.connect(s.jm.path, timeout=10)
    try:
        conn.execute("UPDATE audit_event SET invalidated_at = ? "
                     "WHERE restored_from_event_id IN (?) "
                     "AND invalidated_at IS NULL", (stamp, source_id))
        conn.commit()
    finally:
        conn.close()
    return ok, stamp


def _case_gpkg_same_source(ctx, tmp, cut, retyped):
    """*cut* : "before_restore1" (entre e et t1) ou "before_restore2"
    (entre la re-saisie et t2)."""
    import recoverland.restore_runner as rr
    from qgis.core import QgsProject
    from recoverland.core.event_stream_repository import fetch_events_by_ids
    out = {"cut": cut, "retyped": retyped}
    s = _Stack(ctx, tmp, f"meme_{cut}")
    try:
        time.sleep(0.05)
        out["commit_e"] = s.commit("C1", "b")
        (e_id,), = s.rows("SELECT event_id FROM audit_event "
                          "WHERE restored_from_event_id IS NULL")
        out["e"] = e_id
        time.sleep(0.05)
        if cut == "before_restore1":
            out["cutoff"] = _now()
            time.sleep(0.05)
        conn = s.jm.create_read_connection()
        try:
            events = fetch_events_by_ids(conn, [e_id])
        finally:
            conn.close()
        runner = rr.RestoreRunner(events, lambda _ev: s.layer, s.wq,
                                  tracker=s.tracker, trace_id=ctx.trace_id,
                                  journal_path=s.jm.path)
        res = s._wait(runner)
        out["restore1"] = (getattr(res, "total_ok", None),
                           getattr(res, "total_fail", None))
        out["after_restore1"] = _value(s.path)
        time.sleep(0.05)
        out["commit_e_again"] = s.commit("C1", retyped)
        time.sleep(0.05)
        if cut == "before_restore2":
            out["cutoff"] = _now()
            time.sleep(0.05)
        t1 = [r[0] for r in s.rows("SELECT event_id FROM audit_event "
                                   "WHERE restored_from_event_id = ?", (e_id,))]
        out["t1"] = t1
        out["raw_restore2"], out["t2"] = _published_restore_again(s, t1[0])
        time.sleep(0.05)
        out["raw_undo"], out["undo_at"] = _published_undo(s, e_id)
        out["before_rewind"] = _value(s.path)
        out["journal"] = s.rows(
            "SELECT event_id, restored_from_event_id, created_at, "
            "invalidated_at FROM audit_event ORDER BY event_id")
        res2, out["plan"] = s.rewind(out["cutoff"], journal_path=s.jm.path)
        out["after_rewind"] = _value(s.path)
        out["rewind_ok"] = getattr(res2, "total_ok", None)
        out["rewind_fail"] = getattr(res2, "total_fail", None)
        out["rewind_errors"] = [str(x) for x in
                                (getattr(res2, "errors", []) or [])][:4]
        out["c2"] = _value(s.path, "C2")
    except Exception:  # noqa: BLE001 - a crash is a result
        import traceback
        out["crash"] = traceback.format_exc()[-2000:]
    finally:
        s.close()
        QgsProject.instance().clear()
    return out


# --------------------------------------------------------------------------
# T2 / T3 : journaux construits ligne a ligne
# --------------------------------------------------------------------------
def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _upd(old, new):
    return {"changed_only": {"v": {"old": old, "new": new}}}


def _invalidate(path, ids, stamp):
    conn = sqlite3.connect(path)
    try:
        conn.executemany("UPDATE audit_event SET invalidated_at = ? "
                         "WHERE event_id = ?", [(stamp, i) for i in ids])
        conn.commit()
    finally:
        conn.close()


def _collapse_at(path, fp, stamp):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import plan_temporal_restore
    cutoff = RestoreCutoff(CutoffType.BY_DATE, stamp, inclusive=True)
    conn = sqlite3.connect(path)
    try:
        window = fetch_events_after_cutoff(conn, fp, cutoff)
    finally:
        conn.close()
    active, _stats = collapse_rewind_events_with_stats(window)
    plan = plan_temporal_restore(active, fp, "ordre", cutoff)
    return active, plan


def _replay(active, plan, start):
    """Valeur du champ v apres le plan : chaque compensation ecrit OLD.

    Ce que fait l'executeur sur une compensation d'UPDATE ; l'ordre est
    celui des actions du planificateur."""
    by_id = {}
    for e in active:
        by_id.setdefault(e.event_id, []).append(e)
    value = start
    for action in plan.actions:
        rows = by_id.get(action.event_id) or []
        if not rows:
            return None
        row = rows.pop(0)
        changed = (json.loads(row.attributes_json or "{}")
                   .get("changed_only") or {})
        delta = changed.get("v")
        if isinstance(delta, dict):
            value = delta.get("old")
    return value


def _case_legacy(tmp):
    """T2 : journal de la version publiee (schema 6), Undo a instant unique."""
    h = _h()
    fp = "ds_tied_legacy"
    out = {}
    path = h.new_journal(tmp, "t2.sqlite")
    e1 = h.insert_event(path, fp, "ordre", "UPDATE", 100, attrs=_upd("a", "b"),
                        schema=6)
    e2 = h.insert_event(path, fp, "ordre", "UPDATE", 200, attrs=_upd("b", "c"),
                        schema=6)
    # The published Rewind to 50 s: e2 compensated first, then e1.
    t2 = h.insert_event(path, fp, "ordre", "UPDATE", 300, restored_from=e2,
                        schema=6)
    t1 = h.insert_event(path, fp, "ordre", "UPDATE", 300, restored_from=e1,
                        schema=6)
    # Its Undo (published version): one instant for both.
    _invalidate(path, [t1, t2], _iso(500))
    active, plan = _collapse_at(path, fp, _iso(400))
    out["ids"] = dict(e1=e1, e2=e2, t1=t1, t2=t2)
    out["plan"] = [a.event_id for a in plan.actions]
    out["marks"] = h.plan_marks(active)
    out["value"] = _replay(active, plan, "c")
    out["conflicts"] = [c.reason for c in plan.conflicts]
    return out


def _case_same_source(tmp):
    """T3 : deux traces du meme evenement au meme instant d'Undo."""
    from recoverland.core.rewind_dedup import PLAN_MARK_REFUSED, plan_item_kind, refusal_reason
    h = _h()
    fp = "ds_tied_same_source"
    out = {}
    path = h.new_journal(tmp, "t3.sqlite")
    e = h.insert_event(path, fp, "ordre", "UPDATE", 100, attrs=_upd("a", "b"),
                       schema=7)
    # Mode A restore of e, the user types 'b' again, Mode A restore of e again.
    t1 = h.insert_event(path, fp, "ordre", "UPDATE", 200, restored_from=e,
                        schema=7)
    e_again = h.insert_event(path, fp, "ordre", "UPDATE", 300,
                             attrs=_upd("a", "b"), schema=7)
    t2 = h.insert_event(path, fp, "ordre", "UPDATE", 400, restored_from=e,
                        schema=7)
    # The published Undo of the last restore stamped EVERY active trace of
    # e, the older restore's included, with one instant.
    undo_at = _iso(600)
    _invalidate(path, [t1, t2], undo_at)
    active, plan = _collapse_at(path, fp, _iso(500))
    refusals = [refusal_reason(r) for r in active
                if plan_item_kind(r) == PLAN_MARK_REFUSED]
    out["ids"] = dict(e=e, t1=t1, e_again=e_again, t2=t2)
    out["undo_at"] = undo_at
    out["marks"] = h.plan_marks(active)
    out["refusals"] = refusals
    out["blocking"] = [c.reason for c in plan.conflicts
                       if c.severity == "blocking"]
    return out


def _t3_truth(cutoff_s, retyped):
    """Valeur du champ a la coupure : 'a' -100- 'b' -200- 'a' -300- N
    -400- 'a' -600- 'b' (coupures choisies hors des instants)."""
    value = "a"
    for at, v in ((100, "b"), (200, "a"), (300, retyped), (400, "a"),
                  (600, "b")):
        if at < cutoff_s:
            value = v
    return value


def _case_same_source_at(tmp, schema, cutoff_s, retyped):
    """Le journal de T3 coupe a *cutoff_s*, traces de schema *schema*.

    Le refus est lu la ou le produit le lit : le controle prealable
    (`restore_planner.preflight_check`) du plan. Un plan BLOQUE n'est pas
    execute du tout (StrictRestoreRunner refuse tout le groupe et affiche
    ses trois premiers motifs), meme s'il liste encore des actions pour
    les autres lignes de la fenetre."""
    from recoverland.core.restore_planner import preflight_check
    h = _h()
    tag = f"{schema}_{cutoff_s}_{retyped}"
    fp = f"ds_tied_{tag}"
    path = h.new_journal(tmp, f"t3_{tag}.sqlite")
    e = h.insert_event(path, fp, "ordre", "UPDATE", 100, attrs=_upd("a", "b"),
                       schema=schema)
    t1 = h.insert_event(path, fp, "ordre", "UPDATE", 200, restored_from=e,
                        schema=schema)
    e_again = h.insert_event(path, fp, "ordre", "UPDATE", 300,
                             attrs=_upd("a", retyped), schema=schema)
    t2 = h.insert_event(path, fp, "ordre", "UPDATE", 400, restored_from=e,
                        schema=schema)
    undo_at = _iso(600)
    _invalidate(path, [t1, t2], undo_at)
    active, plan = _collapse_at(path, fp, _iso(cutoff_s))
    report = preflight_check(plan)
    return {
        "ids": dict(e=e, t1=t1, e_again=e_again, t2=t2),
        "undo_at": undo_at,
        "cutoff": _iso(cutoff_s),
        "truth": _t3_truth(cutoff_s, retyped),
        "marks": h.plan_marks(active),
        "actions": [(a.event_id, a.compensatory_op) for a in plan.actions],
        "blocking": [c.reason for c in plan.conflicts
                     if c.severity == "blocking"],
        "verdict": getattr(report.verdict, "value", str(report.verdict)),
        "shown": list(report.blocking_reasons[:3]),
        "value": _replay(active, plan, "b"),
    }


_T3_VARIANTS = (
    # (cle, schema, coupure en s, valeur re-saisie)
    ("t3_s6_350", 6, 350, "x"),
    ("t3_s7_350", 7, 350, "x"),
    ("t3_s6_150", 6, 150, "b"),
    ("t3_s7_150", 7, 150, "b"),
    ("t3_s6_700", 6, 700, "x"),
)


# --------------------------------------------------------------------------
def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_tied_undo_")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    tmp = d["tmp"]
    flog(f"rw_undo_tied_stamps_order run start: trace_id={ctx.trace_id}")
    try:
        d["t1"] = _case_gpkg(ctx, tmp, old_style=True)
        d["t4"] = _case_gpkg(ctx, tmp, old_style=False)
        cases = [("t2", _case_legacy), ("t3", _case_same_source)]
        cases += [(key, (lambda t, s=schema, c=cut, r=retyped:
                         _case_same_source_at(t, s, c, r)))
                  for key, schema, cut, retyped in _T3_VARIANTS]
        for key, fn in cases:
            try:
                d[key] = fn(tmp)
            except Exception:  # noqa: BLE001 - a crash is a result
                import traceback
                d[key] = {"crash": traceback.format_exc()[-2000:]}
        d["t5"] = _case_gpkg_same_source(ctx, tmp, "before_restore2", "q")
        d["t6"] = _case_gpkg_same_source(ctx, tmp, "before_restore1", "b")
    finally:
        flog(f"rw_undo_tied_stamps_order run end: trace_id={ctx.trace_id} "
             f"t1={json.dumps(d.get('t1'), default=str)[:600]} "
             f"t2={json.dumps(d.get('t2'), default=str)[:400]} "
             f"t3={json.dumps(d.get('t3'), default=str)[:600]} "
             f"t5={json.dumps(d.get('t5'), default=str)[:800]} "
             f"t6={json.dumps(d.get('t6'), default=str)[:800]}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def _gpkg_assertions(tag, r, tied):
    stamps = r.get("stamps") or []
    instants = [s[2] for s in stamps]
    shape = (len(stamps) == 2 and len(set(instants)) == (1 if tied else 2))
    return [
        (f"{tag}_control_capture_rewind_undo",
         not r.get("crash") and r.get("commits") == [True, True]
         and len(r.get("user_ids") or []) == 2
         and r.get("after_rewind1") == "a" and r.get("rewind1_fail") == 0
         and r.get("after_undo") == "c" and r.get("undo_fail") == 0 and shape,
         f"commits={r.get('commits')} evenements={r.get('user_ids')} apres "
         f"Rewind={r.get('after_rewind1')} (echecs {r.get('rewind1_fail')}) "
         f"apres Undo={r.get('after_undo')} (echecs {r.get('undo_fail')}) "
         f"invalidations={stamps} (attendu 2, "
         f"{'un seul instant' if tied else 'deux instants'}) "
         f"crash={r.get('crash')} (controle du montage)"),
        (f"{tag}_rewind_between_rewind_and_undo_lands_on_a",
         r.get("after_rewind2") == "a" and r.get("rewind2_fail") == 0
         and r.get("c2") == "x",
         f"Rewind a une date entre le Rewind et son Undo : champ="
         f"{r.get('after_rewind2')} attendu 'a' ; plan (du plus recent au "
         f"plus ancien)={r.get('plan2')} echecs={r.get('rewind2_fail')} "
         f"{r.get('rewind2_errors')} C2={r.get('c2')}. Sur 'b', le champ "
         f"porte l'ancienne valeur NEW : les deux compensations ont ete "
         f"refaites dans l'ordre d'ecriture du Rewind defait, l'inverse de "
         f"celui de l'Undo, et le rapport annonce un succes."),
    ]


def assertions(ctx):
    d = ctx.data
    out = []
    out += _gpkg_assertions("T1_old_undo_one_instant", d.get("t1") or {}, True)
    t2 = d.get("t2") or {}
    ids = t2.get("ids") or {}
    out.append((
        "T2_published_journal_recompensates_newest_first",
        not t2.get("crash") and t2.get("plan") == [ids.get("e2"), ids.get("e1")]
        and t2.get("value") == "a" and not t2.get("conflicts"),
        f"ids={ids} ordre du plan={t2.get('plan')} attendu "
        f"[{ids.get('e2')}, {ids.get('e1')}] (e2 recompense avant e1) ; "
        f"lignes={t2.get('marks')} valeur finale={t2.get('value')} attendu "
        f"'a' ; conflits={t2.get('conflicts')} crash={t2.get('crash')}. Dans "
        f"l'autre ordre le champ finit sur 'b'."))
    t3 = d.get("t3") or {}
    undo_at = t3.get("undo_at") or "?"
    named = [r for r in t3.get("refusals") or []
             if undo_at in r and "Undo" in r]
    out.append((
        "T3_same_event_stamped_twice_refused_by_name",
        not t3.get("crash") and bool(named) and bool(t3.get("blocking")),
        f"ids={t3.get('ids')} lignes={t3.get('marks')} refus="
        f"{t3.get('refusals')} bloquants={t3.get('blocking')} crash="
        f"{t3.get('crash')} : l'ancien Undo a invalide au meme instant "
        f"({undo_at}) deux compensations du meme evenement, dont une d'une "
        f"restauration anterieure qu'il n'a pas defaite ; le journal ne dit "
        f"plus laquelle il a defaite, le Rewind doit etre refuse en nommant "
        f"cet Undo et son instant, pas par un motif generique."))
    out += _gpkg_assertions("T4_new_undo_increasing_instants",
                            d.get("t4") or {}, False)
    for key, schema, cut, retyped in _T3_VARIANTS:
        out.append(_variant_assertion(key, schema, cut, retyped,
                                      d.get(key) or {}))
    out += _same_source_gpkg_assertions("T5_real_cut_between_retype_and_restore2",
                                        d.get("t5") or {}, "q")
    out += _same_source_gpkg_assertions("T6_real_cut_between_e_and_restore1",
                                        d.get("t6") or {}, "b")
    return out


def _named(reasons, undo_at):
    return [r for r in reasons or [] if undo_at and undo_at in r and "Undo" in r]


def _variant_assertion(key, schema, cut, retyped, r):
    undo_at = r.get("undo_at") or "?"
    where = ("entre la re-saisie et la seconde restauration" if cut == 350
             else "entre e et la premiere restauration" if cut == 150
             else "apres l'ancien Undo")
    if cut > 600:
        # Control: the tie is in the past of the date asked for.
        return (
            f"T3_s{schema}_{cut}_control_after_undo_not_refused",
            not r.get("crash") and not r.get("blocking")
            and not r.get("actions") and r.get("value") == r.get("truth"),
            f"coupure {r.get('cutoff')} {where} ({undo_at}) : attendu aucun "
            f"refus, aucune ecriture, champ={r.get('truth')} ; obtenu "
            f"bloquants={r.get('blocking')} actions={r.get('actions')} "
            f"champ={r.get('value')} lignes={r.get('marks')} crash="
            f"{r.get('crash')}. Un refus ici interdirait la date meme que "
            f"le refus conseille.")
    return (
        f"T3_s{schema}_{cut}_same_event_tie_refused_by_name",
        not r.get("crash") and r.get("verdict") == "blocked"
        and bool(_named(r.get("blocking"), undo_at))
        and bool(_named(r.get("shown"), undo_at)),
        f"traces de schema {schema}, coupure {r.get('cutoff')} {where}, "
        f"re-saisie '{retyped}' : le champ valait '{r.get('truth')}' a cette "
        f"date. ids={r.get('ids')} lignes={r.get('marks')} actions="
        f"{r.get('actions')} controle prealable={r.get('verdict')} "
        f"motifs affiches={r.get('shown')} crash={r.get('crash')}. Execute, "
        f"le plan ecrirait '{r.get('value')}' : l'ancien Undo a estampille "
        f"({undo_at}) deux compensations du meme evenement dont une qu'il "
        f"n'a pas defaite, le journal ne peut plus rendre l'etat de cette "
        f"date ; le Rewind doit etre bloque avant toute ecriture par un "
        f"motif qui nomme cet Undo et son instant, au lieu d'un succes sur "
        f"une valeur fausse.")


def _same_source_gpkg_assertions(tag, r, retyped):
    undo_at = r.get("undo_at") or "?"
    journal = r.get("journal") or []
    e = r.get("e")
    tied = [row for row in journal if row[1] == e and row[3] == undo_at]
    control = (not r.get("crash") and r.get("commit_e") is True
               and r.get("restore1") == (1, 0)
               and r.get("after_restore1") == "a"
               and r.get("commit_e_again") is True
               and r.get("raw_restore2") is True and r.get("raw_undo") is True
               and r.get("before_rewind") == "b" and len(tied) == 2)
    truth = retyped if r.get("cut") == "before_restore2" else "b"
    return [
        (f"{tag}_control_setup",
         control,
         f"commit e={r.get('commit_e')} restauration 1={r.get('restore1')} "
         f"(attendu (1, 0)) champ apres={r.get('after_restore1')} (attendu "
         f"'a') re-saisie={r.get('commit_e_again')} seconde restauration "
         f"publiee={r.get('raw_restore2')} Undo publie={r.get('raw_undo')} "
         f"champ avant Rewind={r.get('before_rewind')} (attendu 'b') traces "
         f"de e estampillees {undo_at}={tied} (attendu 2) journal={journal} "
         f"crash={r.get('crash')} (controle du montage)"),
        (f"{tag}_refused_by_name_layer_untouched",
         not r.get("crash") and r.get("after_rewind") == "b"
         and r.get("rewind_ok") == 0
         and bool(_named(r.get("rewind_errors"), undo_at))
         and r.get("c2") == "x",
         f"Rewind au {r.get('cutoff')} (le champ valait '{truth}') : champ="
         f"{r.get('after_rewind')} reussites={r.get('rewind_ok')} echecs="
         f"{r.get('rewind_fail')} erreurs={r.get('rewind_errors')} plan="
         f"{r.get('plan')} C2={r.get('c2')}. Attendu : couche intacte ('b'), "
         f"aucune reussite, un echec qui nomme l'Undo de {undo_at}. Sur 'a' "
         f"avec un succes, le Rewind a ecrit une valeur qui n'a jamais ete "
         f"celle de cette date et l'a annoncee restauree."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
