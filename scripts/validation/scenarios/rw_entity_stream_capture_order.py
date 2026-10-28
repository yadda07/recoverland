# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_entity_stream_capture_order - l'historique d'une entite se lit dans l'ordre de capture, pas dans l'ordre des identifiants.

Invariant : I-REWIND-01 (les evenements d'une entite sont relus dans
l'ordre ou ils ont eu lieu : (created_at, event_id), l'identifiant ne
departageant que deux evenements du meme instant).

Mecanisme
---------
`event_id` est l'ordre d'INSERTION dans un journal, pas l'ordre de
capture. Le rattachement de l'historique a l'"Enregistrer sous"
(`journal_manager.carry_journal_history`) ajoute les lignes d'un autre
journal APRES toutes celles du journal cible : quand une autre instance a
deja ecrit la meme entite dans la cible entre-temps, la modification
rattachee, plus ancienne, recoit un identifiant plus grand. Le
rattachement le detecte et le signale (`out_of_order_entities`), mais
trois lectures de `event_stream_repository` triaient encore sur
`event_id` seul :
  - `fetch_entity_stream` (historique d'une entite, "Annuler la session") ;
  - `fetch_events_by_ids` (restauration Mode A d'une selection : le
    `RestoreRunner` applique les evenements dans l'ordre rendu) ;
  - `fetch_events_by_session` (evenements d'une session, "du plus recent
    au plus ancien" selon son contrat ; un evenement reimporte du fichier
    pending garde sa date mais recoit un identifiant recent).

Pour l'utilisateur : le champ passe de 'a' a 'b' (e1, capture dans le
journal d'origine), puis de 'b' a 'c' (e2, capture dans le journal du
projet par une autre instance). Apres le rattachement, e1 porte un
identifiant plus grand que e2. L'utilisateur selectionne e1 et e2 et
demande leur restauration : le runner compense e1 d'abord ('c' -> 'a'),
puis e2 ('a' -> 'b'). Le champ finit sur 'b', une valeur qu'il n'avait
pas avant e1, alors qu'il devait revenir a 'a'.

Cas prouves (vrai GeoPackage, vraie capture dans deux journaux, vrai
rattachement `carry_journal_history`, vrai RestoreRunner) :
  S0  (controle) le rattachement produit bien la forme : e1 a un
      identifiant plus grand que e2 dans la cible, et il le signale ;
  S1  `fetch_entity_stream` rend e1 puis e2 (ordre de capture) ;
  S2  `fetch_events_by_ids` rend e2 puis e1 (plus recent d'abord) ;
  S3  restauration Mode A de la selection {e1, e2} : 'a', sans echec ;
  S4  `fetch_events_by_session` sur une session dont une edition a ete
      reimportee du pending (date ancienne, identifiant recent) : du plus
      recent au plus ancien par date de capture.

Verdict avant correctif : FAIL (S1, S2, S3, S4). Apres correctif : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rw_entity_stream_capture_order"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))


def _gpkg(tmpdir):
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
    path = os.path.join(tmpdir, "ordre.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _value(path, code="C1"):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    for f in lyr.getFeatures():
        if f["code"] == code:
            return f["v"]
    return None


class _Journal:
    """Une instance : son projet, son journal, sa capture, sur la couche."""

    def __init__(self, directory, layer):
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        os.makedirs(directory, exist_ok=True)
        project = os.path.join(directory, "p.qgz")
        open(project, "w").close()
        self.layer = layer
        self.jm = JournalManager()
        self.jm.open_for_project(project, directory)
        self.path = self.jm.path
        self.wq = WriteQueue()
        self.wq.start(self.jm.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()
        self.tracker.connect_layer(layer)

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

    def close(self):
        try:
            self.tracker.deactivate()
            self.tracker.disconnect_all()
        finally:
            self.wq.stop()
            self.jm.close()


def _rows(path, sql, params=()):
    conn = sqlite3.connect(f"file:{Path(path).as_posix()}?mode=ro", uri=True)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _case_carry(ctx, tmp):
    import recoverland.restore_runner as rr
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.journal_manager import carry_journal_history
    from recoverland.core.event_stream_repository import (
        fetch_entity_stream, fetch_events_by_ids,
    )
    from scripts.validation.plugin_harness import pump
    out = {}
    gpkg = _gpkg(tmp)
    layer = QgsVectorLayer(f"{gpkg}|layername=pts", "ordre", "ogr")
    QgsProject.instance().addMapLayer(layer)
    fp = compute_datasource_fingerprint(layer)
    source = target = None
    try:
        # e1 in the journal the session started on (before its "Save As").
        source = _Journal(os.path.join(tmp, "origine"), layer)
        out["commit_e1"] = source.commit("C1", "b")
        src_path = source.path
        source.close()
        source = None
        time.sleep(0.05)
        # e2 in the project's journal, by another instance, meanwhile.
        target = _Journal(os.path.join(tmp, "projet"), layer)
        out["commit_e2"] = target.commit("C1", "c")
        (e2_id, e2_at), = _rows(target.path,
                                "SELECT event_id, created_at FROM audit_event")
        conn = sqlite3.connect(target.path, timeout=10)
        try:
            report = carry_journal_history(src_path, conn, target.path,
                                           since_event_id=0,
                                           trace_id=ctx.trace_id)
        finally:
            conn.close()
        out["carry_ok"] = bool(report.ok)
        out["carry_inserted"] = getattr(report, "events_inserted", None)
        out["carry_out_of_order"] = report.out_of_order_entities
        rows = _rows(target.path,
                     "SELECT event_id, created_at, entity_fingerprint FROM "
                     "audit_event WHERE restored_from_event_id IS NULL "
                     "ORDER BY event_id")
        out["journal"] = rows
        e1 = [r for r in rows if r[0] != e2_id]
        out["e1"] = e1[0][0] if len(e1) == 1 else None
        out["e2"] = e2_id
        out["e1_after_e2_by_id_before_by_date"] = (
            out["e1"] is not None and out["e1"] > e2_id and e1[0][1] < e2_at)
        entity = e1[0][2] if e1 else None
        read = target.jm.create_read_connection()
        try:
            out["entity_stream"] = [e.event_id for e in fetch_entity_stream(
                read, fp, entity)] if entity else None
            selected = fetch_events_by_ids(read, [out["e1"], e2_id])
        finally:
            read.close()
        out["by_ids"] = [e.event_id for e in selected]
        # The dialog's Mode A path: the selection as read, in that order.
        done = {}
        runner = rr.RestoreRunner(
            selected, lambda ev: layer if ev.datasource_fingerprint == fp
            else None, target.wq, tracker=target.tracker,
            trace_id=ctx.trace_id)
        runner.finished.connect(lambda res: done.setdefault("r", res))
        runner.start()
        deadline = time.monotonic() + 30
        while "r" not in done and time.monotonic() < deadline:
            pump(0.02)
        target.drain()
        res = done.get("r")
        out["restore_fail"] = getattr(res, "total_fail", None)
        out["restore_ok"] = getattr(res, "total_ok", None)
        out["restore_errors"] = list(getattr(res, "errors", []) or [])[:4]
        out["after_restore"] = _value(gpkg)
        out["c2"] = _value(gpkg, "C2")
    except Exception:  # noqa: BLE001 - a crash is a result
        import traceback
        out["crash"] = traceback.format_exc()[-2000:]
    finally:
        for inst in (source, target):
            if inst is not None:
                inst.close()
        QgsProject.instance().removeMapLayer(layer.id())
    return out


def _case_session(tmp):
    """S4 : une session dont une edition a ete reimportee du pending."""
    from scripts.validation.scenarios import _undo_dialog_harness as h
    from recoverland.core.event_stream_repository import fetch_events_by_session
    path = h.new_journal(tmp, "session.sqlite")
    sess = "sess-pending"
    first = h.insert_event(path, "ds_sess", "ordre", "UPDATE", 100, fid=1,
                           session_id=sess)
    third = h.insert_event(path, "ds_sess", "ordre", "UPDATE", 300, fid=1,
                           session_id=sess)
    # Held back in the pending file, re-imported at the next start: its
    # date is between the two, its id after both.
    second = h.insert_event(path, "ds_sess", "ordre", "UPDATE", 200, fid=1,
                            session_id=sess)
    conn = sqlite3.connect(path)
    try:
        got = [e.event_id for e in fetch_events_by_session(conn, sess)]
    finally:
        conn.close()
    return dict(ids=dict(first=first, second=second, third=third), got=got,
                expected=[third, second, first])


def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_stream_order_")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    flog(f"rw_entity_stream_capture_order run start: trace_id={ctx.trace_id}")
    d["carry"] = _case_carry(ctx, d["tmp"])
    try:
        d["session"] = _case_session(d["tmp"])
    except Exception:  # noqa: BLE001 - a crash is a result
        import traceback
        d["session"] = {"crash": traceback.format_exc()[-2000:]}
    flog(f"rw_entity_stream_capture_order run end: trace_id={ctx.trace_id} "
         f"carry={json.dumps(d['carry'], default=str)[:800]} "
         f"session={json.dumps(d['session'], default=str)[:300]}")


def teardown(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    c = ctx.data.get("carry") or {}
    s = ctx.data.get("session") or {}
    e1, e2 = c.get("e1"), c.get("e2")
    return [
        ("S0_control_carry_appends_older_change_after_newer",
         not c.get("crash") and c.get("commit_e1") and c.get("commit_e2")
         and c.get("carry_ok") and c.get("e1_after_e2_by_id_before_by_date")
         and (c.get("carry_out_of_order") or 0) >= 1,
         f"rattachement ok={c.get('carry_ok')} lignes={c.get('carry_inserted')} "
         f"entites hors ordre signalees={c.get('carry_out_of_order')} journal "
         f"cible (id, date, entite)={c.get('journal')} crash={c.get('crash')} "
         f"(controle : e1, capture avant e2, doit porter un identifiant plus "
         f"grand)"),
        ("S1_entity_stream_in_capture_order",
         c.get("entity_stream") == [e1, e2],
         f"fetch_entity_stream={c.get('entity_stream')} attendu [{e1}, {e2}] "
         f"(e1 capture avant e2) : l'historique de l'entite est relu a "
         f"l'envers, et 'Annuler la session' nomme une edition posterieure "
         f"qui n'est pas la premiere."),
        ("S2_events_by_ids_newest_capture_first",
         c.get("by_ids") == [e2, e1],
         f"fetch_events_by_ids={c.get('by_ids')} attendu [{e2}, {e1}] (plus "
         f"recent d'abord) : le runner Mode A applique dans cet ordre."),
        ("S3_mode_a_selection_restores_to_a",
         c.get("after_restore") == "a" and c.get("restore_fail") == 0
         and c.get("c2") == "x",
         f"restauration de la selection {{e1, e2}} : champ="
         f"{c.get('after_restore')} attendu 'a' ; ok={c.get('restore_ok')} "
         f"echecs={c.get('restore_fail')} {c.get('restore_errors')} C2="
         f"{c.get('c2')}. Sur 'b', e1 a ete compense avant e2 : le champ "
         f"porte une valeur qu'il n'avait pas avant les deux editions."),
        ("S4_session_newest_capture_first",
         not s.get("crash") and s.get("got") == s.get("expected"),
         f"fetch_events_by_session={s.get('got')} attendu {s.get('expected')} "
         f"ids={s.get('ids')} crash={s.get('crash')} : l'edition reimportee "
         f"du pending passait pour la plus recente de la session."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
