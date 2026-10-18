"""rw_runner_journal_locked - un journal verrouille n'autorise aucune ecriture a l'aveugle.

Invariant : I-3 (jamais d'ecriture sur une entite dont l'identite n'est pas
prouvee) et fail-loud (jamais de succes ni de refus annonce a tort, jamais
de dialogue fige en silence), quand le journal est CONNU mais ne peut pas
etre lu au moment ou l'operation le demande.

Le mecanisme
------------
Un journal sur un partage reseau est en mode DELETE : un ecrivain qui le
tient (une autre instance de QGIS, la file d'ecriture pendant une grosse
transaction, une maintenance) exclut les lecteurs, et au-dela du delai
d'attente de SQLite (5 s) la lecture echoue "database is locked".

R  RestoreRunner (restauration Mode A). Le journal fournit deux preuves a
   l'executeur : la continuite d'une cle, et le fait qu'un evenement est
   DEJA compense par une trace en vigueur (refus `already_compensated`).
   Journal illisible, le runner continuait avec journal=None : le refus
   `already_compensated` etait eteint en silence. U1 change val 10 -> 872,
   R1 le restaure (trace T1), l'utilisateur retape 872 (U2), R2 restaure
   U1 a nouveau pendant qu'un ecrivain tient le journal : R2 ecrivait 10,
   defaisait U2, journalisait une deuxieme trace nommant U1 et mettait U1
   dans le perimetre d'annulation ; le seul message disait que la
   restauration etait refusee, alors qu'elle etait ecrite. Le journal
   disait U1 restaure deux fois, U2 toujours en vigueur : la
   desynchronisation que rs_mode_a_double_restore existe pour empecher.
   StrictRestoreRunner, lui, refuse la couche dans ce cas, et l'executeur
   refuse (scan_failed) quand la meme lecture echoue en cours de route.
   Attendu : la couche refusee entiere et nommee (raison, evenements),
   rien d'ecrit, aucune trace, rien dans le perimetre d'annulation, la
   capture de la couche rendue, et la couche suivante (journal redevenu
   lisible) restauree normalement.

U  UndoRunner (annulation). L'annulation ne demande jamais
   `already_compensated` ; sans journal seule la continuite de cle manque,
   et l'executeur refuse par son nom ce qu'il ne peut plus prouver
   (key_continuity_unproven (no_journal)). L'annulation peut donc
   continuer, mais le message disait "son annulation est refusee" pour
   une couche dont l'annulation a ete ecrite. Attendu : l'entite a cle
   unique est annulee, l'entite a contenu partage est refusee par son nom,
   et l'avis dit que l'annulation a continue sans le journal.

D  Dialogue, restore_selected_data. Apres l'acceptation du preflight, le
   dialogue se met "restauration en cours" puis relit les evenements
   choisis dans le journal. Journal verrouille a ce moment : l'exception
   sortait du slot vers Qt, aucun runner, aucun message, et
   `_restore_in_progress` restait vrai : chaque action suivante etait
   refusee "une restauration est en cours sur les couches" jusqu'au
   changement de projet ; boutons grises, barre de progression affichee.
   Attendu : rien d'ecrit, la raison nommee, le dialogue rendu (drapeau,
   trace, boutons, barre, logo), et la restauration suivante acceptee.

Ce qui s'execute
----------------
R et U : vraies GeoPackage, vrai fichier journal SQLite en mode DELETE,
vrais RestoreRunner et UndoRunner, verrou reel (BEGIN EXCLUSIVE tenu au-dela
du delai d'attente). D : les vraies methodes du dialogue greffees par
_undo_dialog_harness, vrai journal en mode DELETE verrouille pendant que le
preflight est affiche.

Verdict avant correctif : FAIL (R : U2 defait, deuxieme trace sur U1, U1
dans le perimetre d'annulation ; U : avis "refusee" sur une annulation
ecrite ; D : exception hors du slot, dialogue fige).
Verdict apres correctif : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_runner_journal_locked"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_DS1 = "rw_rjl::ds_locked"
_DS2 = "rw_rjl::ds_next"
_DS3 = "rw_rjl::ds_undo"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _now():
    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------
# Real journal file (DELETE mode) and real GeoPackages
# --------------------------------------------------------------------------
def _journal_file(tmp, name):
    """A real journal in DELETE mode: a writer's lock excludes the readers."""
    from recoverland.core.sqlite_schema import initialize_schema
    path = os.path.join(tmp, name)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.commit()
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()
    return path


class _Lock:
    """A writer holding the journal, as another process would."""

    def __init__(self, path):
        self.path = path
        self.conn = None

    def take(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.path, isolation_level=None,
                                        check_same_thread=False)
            self.conn.execute("BEGIN EXCLUSIVE")

    def release(self):
        if self.conn is not None:
            try:
                self.conn.execute("ROLLBACK")
            finally:
                self.conn.close()
                self.conn = None


def _gpkg(tmp, name, rows):
    from qgis.core import (QgsVectorLayer, QgsVectorFileWriter, QgsFeature,
                           QgsCoordinateTransformContext, QgsGeometry,
                           QgsPointXY)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)"
                         "&field=val:integer", "seed", "memory")
    path = str(Path(tmp) / f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = name
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    layer = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
    feats = []
    for i, (code, val) in enumerate(rows):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("code", code)
        feat.setAttribute("val", val)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(100.0 * i, 5.0)))
        feats.append(feat)
    if not layer.dataProvider().addFeatures(feats)[0]:
        raise RuntimeError("seed impossible")
    layer.reload()
    return layer


def _event(layer, ds, op, identity, attributes):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.serialization import serialize_field_schema
    return AuditEvent(
        event_id=None, project_fingerprint="rw_rjl_project",
        datasource_fingerprint=ds, layer_id_snapshot=layer.id(),
        layer_name_snapshot=layer.name(), provider_type="ogr",
        feature_identity_json=identity, operation_type=op,
        attributes_json=json.dumps(attributes), geometry_wkb=None,
        geometry_type="Point", crs_authid="EPSG:2154",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id=None, created_at=_now(),
        restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(identity),
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)


def _journalled(path, event):
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
        AUDIT_EVENT_INSERT_COLUMNS,
    )
    from recoverland.core.search_service import get_event_by_id
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(sql, tuple(getattr(event, c)
                                      for c in AUDIT_EVENT_INSERT_COLUMNS))
        conn.commit()
        return get_event_by_id(conn, cur.lastrowid)
    finally:
        conn.close()


def _feature(layer, code):
    layer.reload()
    return next((f for f in layer.getFeatures() if f["code"] == code), None)


def _edit_val(path, layer, ds, code, new):
    """A user edit through the provider, journalled as the capture would."""
    from recoverland.core.identity import compute_feature_identity
    feat = _feature(layer, code)
    old = feat["val"]
    identity = compute_feature_identity(layer, feat)
    idx = layer.fields().indexOf("val")
    layer.dataProvider().changeAttributeValues({feat.id(): {idx: new}})
    layer.reload()
    return _journalled(path, _event(layer, ds, "UPDATE", identity, {
        "changed_only": {"val": {"old": old, "new": new}}}))


def _state(layer):
    layer.reload()
    return sorted((f["code"], f["val"]) for f in layer.getFeatures())


def _val(layer, code):
    feat = _feature(layer, code)
    return None if feat is None else feat["val"]


def _traces_on(path, eid):
    conn = sqlite3.connect(path)
    try:
        return conn.execute("SELECT COUNT(*) FROM audit_event WHERE "
                            "restored_from_event_id = ?", (eid,)).fetchone()[0]
    finally:
        conn.close()


class _Tracker:
    """Per-layer capture suppression, as EditSessionTracker exposes it."""

    def __init__(self):
        self.suppressed = []

    def suppress_layers(self, ids):
        self.suppressed.extend(ids)

    def unsuppress_layers(self, ids):
        for layer_id in ids:
            if layer_id in self.suppressed:
                self.suppressed.remove(layer_id)


def _run_to_end(runner, timeout_s=40.0):
    h = _h()
    results = []
    runner.finished.connect(results.append)
    runner.start()
    h.pump_until(lambda: bool(results), timeout_s)
    return results[0] if results else None


# --------------------------------------------------------------------------
# R: RestoreRunner, second Mode A restore while the journal is locked
# --------------------------------------------------------------------------
def _case_r(ctx, tmp):
    import recoverland.restore_runner as rr
    h = _h()
    out = {}
    jpath = _journal_file(tmp, "r.sqlite")
    lock = _Lock(jpath)
    layer1 = _gpkg(tmp, "dbl", (("A", 10), ("B", 20), ("C", 30)))
    layer2 = _gpkg(tmp, "two", (("P", 40), ("Q", 50)))
    try:
        u1 = _edit_val(jpath, layer1, _DS1, "A", 872)
        queue = h.RecordingWriteQueue(jpath)
        r1 = rr.RestoreRunner([u1], lambda _e: layer1, queue, tracker=None,
                              trace_id="rw_rjl_r1", journal_path=jpath)
        res1 = _run_to_end(r1)
        out["r1"] = {"errors": list(getattr(res1, "errors", []) or []),
                     "val": _val(layer1, "A"),
                     "traces_on_u1": _traces_on(jpath, u1.event_id)}
        u2 = _edit_val(jpath, layer1, _DS1, "A", 872)
        v = _edit_val(jpath, layer2, _DS2, "P", 555)
        out.update(u1=u1.event_id, u2=u2.event_id, v=v.event_id,
                   u2_val=_val(layer1, "A"))

        tracker = _Tracker()
        seen = {}

        def resolver(event):
            if event.datasource_fingerprint == _DS2:
                # What the run still holds on the first layer, read when
                # it takes the second one.
                seen["suppressed_at_next_layer"] = list(tracker.suppressed)
                return layer2
            return layer1

        queue.enqueued = []
        lock.take()
        r2 = rr.RestoreRunner([u1, v], resolver, queue, tracker=tracker,
                              trace_id="rw_rjl_r2", journal_path=jpath)
        # The writer lets go once the first layer is accounted (its
        # progress), before the second layer opens: its journal read
        # succeeds.
        r2.progress.connect(lambda _done, _total: lock.release())
        res2 = _run_to_end(r2)
        lock.release()
        out["r2"] = {
            "finished": res2 is not None,
            "report": dict(r2.apply_report()),
            "errors": list(getattr(res2, "errors", []) or []),
            "undo_scope": {fp: sorted(e.event_id for e in evts)
                           for fp, evts in (getattr(res2, "by_ds", {})
                                            or {}).items()},
            "enqueued": [ids for ids, _path, _stopped in queue.enqueued],
            "val_a": _val(layer1, "A"),
            "val_p": _val(layer2, "P"),
            "traces_on_u1": _traces_on(jpath, u1.event_id),
            "traces_on_v": _traces_on(jpath, v.event_id),
            "layer1_id": layer1.id(),
            "suppressed_at_next_layer": seen.get("suppressed_at_next_layer"),
            "suppressed_at_end": list(tracker.suppressed),
        }
    finally:
        lock.release()
        # The GeoPackages are released before the teardown removes them.
        layer1 = layer2 = None
    ctx.data["r"] = out


# --------------------------------------------------------------------------
# U: UndoRunner while the journal is locked
# --------------------------------------------------------------------------
def _case_u(ctx, tmp):
    import recoverland.restore_runner as rr
    h = _h()
    out = {}
    jpath = _journal_file(tmp, "u.sqlite")
    lock = _Lock(jpath)
    # A: unique content. T: two identical rows, only the journal tells them
    # apart once the restore has put the edited one back.
    layer = _gpkg(tmp, "und", (("A", 10), ("B", 20), ("T", 50), ("T", 50)))
    try:
        ua = _edit_val(jpath, layer, _DS3, "A", 872)
        ut = _edit_val(jpath, layer, _DS3, "T", 900)
        queue = h.RecordingWriteQueue(jpath)
        restore = rr.RestoreRunner([ua, ut], lambda _e: layer, queue,
                                   tracker=None, trace_id="rw_rjl_u0",
                                   journal_path=jpath)
        res0 = _run_to_end(restore)
        out["restore"] = {"errors": list(getattr(res0, "errors", []) or []),
                          "state": _state(layer),
                          "scope": {fp: sorted(e.event_id for e in evts)
                                    for fp, evts in (res0.by_ds or {}).items()}
                          if res0 is not None else None}
        out.update(ua=ua.event_id, ut=ut.event_id)
        lock.take()
        undo = rr.UndoRunner(dict(res0.by_ds), lambda _e: layer,
                             tracker=_Tracker(), journal_path=jpath)
        res = _run_to_end(undo)
        lock.release()
        out["undo"] = {"finished": res is not None,
                       "outcome": dict(undo.undo_outcome()),
                       "errors": list(getattr(res, "errors", []) or []),
                       "state": _state(layer)}
    finally:
        lock.release()
        layer = None
    ctx.data["u"] = out


# --------------------------------------------------------------------------
# D: restore_selected_data, journal locked after the preflight was accepted
# --------------------------------------------------------------------------
def _case_d(ctx, tmp):
    import recoverland.recover_dialog as rd
    h = _h()
    out = {}
    journal = h.new_journal(tmp, "d.sqlite")
    conn = sqlite3.connect(journal)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.close()
    lock = _Lock(journal)
    fp = "rw_rjl_ds_dialog"
    e1 = h.insert_event(journal, fp, "routes", "UPDATE", 10, fid=1)
    (event,) = h.fetch_events(journal, [e1])
    dlg = h.build_dialog(rd, h.Journal(journal), h.RecordingWriteQueue(journal),
                         [fp])
    dlg._search_events = [event]
    dlg.selected_rows = [0]
    logo = []
    dlg._start_logo_activity = lambda *_a, **_k: logo.append("start")
    dlg._stop_logo_activity = lambda: logo.append("stop")
    runners, logs = [], []

    class _PreflightThenLock(h.PreflightDialog):
        """The user reads the preflight; meanwhile a writer takes the journal."""

        def exec(self):
            lock.take()
            return super().exec()

    seams = h.Seams()
    escaped = None
    try:
        seams.set(rd, "RestoreRunner", h.recording_runner_class(runners))
        seams.set(rd, "RestorePreflightDialog", _PreflightThenLock)
        seams.set(rd, "find_target_layer",
                  lambda _evt, _c=None: h.FakeLayer("lyr_routes", "routes"))
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        try:
            dlg.restore_selected_data()
        except Exception as exc:  # noqa: BLE001 - what Qt would receive
            escaped = f"{type(exc).__name__}: {exc}"
        lock.release()
        out["first"] = {
            "escaped": escaped,
            "runners": len(runners),
            "restore_in_progress": bool(dlg._restore_in_progress),
            "run_in_flight": dlg._run_in_flight(),
            "trace_id": dlg._active_restore_trace_id,
            "restore_button": dlg.restore_button.enabled,
            "recover_button": dlg.recover_button.enabled,
            "progress_visible": dlg.progress_bar.visible,
            "logo": list(logo),
            "messages": list(logs),
        }
        # The next restore, the journal readable again.
        logs.clear()
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        try:
            dlg.restore_selected_data()
            escaped2 = None
        except Exception as exc:  # noqa: BLE001
            escaped2 = f"{type(exc).__name__}: {exc}"
        out["second"] = {"escaped": escaped2, "runners": len(runners),
                         "messages": list(logs)}
    finally:
        lock.release()
        seams.restore()
        h.close_dialog(dlg)
    ctx.data["d"] = out


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rw_rjl_")
    flog(f"rw_runner_journal_locked setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    tmp = ctx.data["tmp"]
    for key, case in (("r", _case_r), ("u", _case_u), ("d", _case_d)):
        try:
            case(ctx, tmp)
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data[f"{key}_crash"] = (f"{type(exc).__name__}: {exc} "
                                        f"{traceback.format_exc()[-1500:]}")
    flog(f"rw_runner_journal_locked run: trace_id={ctx.trace_id} "
         f"r={json.dumps(ctx.data.get('r'), default=str)[:1500]} "
         f"u={json.dumps(ctx.data.get('u'), default=str)[:1200]} "
         f"d={json.dumps(ctx.data.get('d'), default=str)[:1200]}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def _assert_r(ctx):
    r = ctx.data.get("r") or {}
    r1 = r.get("r1") or {}
    r2 = r.get("r2") or {}
    u1, u2, v = r.get("u1"), r.get("u2"), r.get("v")
    errors = r2.get("errors") or []
    report = r2.get("report") or {}
    out = [("r_no_crash", "r_crash" not in ctx.data,
            str(ctx.data.get("r_crash")))]
    out.append((
        "r1_control_first_restore_applies",
        not r1.get("errors") and r1.get("val") == 10
        and r1.get("traces_on_u1") == 1 and r.get("u2_val") == 872,
        f"R1={r1} val apres U2={r.get('u2_val')}: la premiere restauration, "
        f"journal lisible, doit ecrire 10 sous une trace, et la re-saisie "
        f"remettre 872.",
    ))
    notice = next((e for e in errors if "'dbl'" in e), "")
    # The events left as they are, as the notice lists them.
    listed = ""
    if "non restaure(s) : " in notice:
        listed = notice.split("non restaure(s) : ", 1)[1].split(".", 1)[0]
    out.append((
        "r2_locked_layer_refused_by_name_nothing_written",
        r2.get("finished") is True
        and r2.get("val_a") == 872
        and r2.get("traces_on_u1") == 1
        and all(u1 not in (ids or []) for ids in r2.get("enqueued") or [])
        and u1 not in (r2.get("undo_scope") or {}).get(_DS1, [])
        and "database is locked" in notice
        and str(u1) in [s.strip() for s in listed.split(",")]
        and "Rien n'a ete ecrit" in notice,
        f"valeur de A apres R2={r2.get('val_a')} (attendu 872 : la re-saisie "
        f"U2={u2} de l'utilisateur) ; traces sur U1={r2.get('traces_on_u1')} "
        f"(attendu 1) ; traces envoyees={r2.get('enqueued')} ; perimetre "
        f"d'annulation={r2.get('undo_scope')} ; message={notice!r} ; "
        f"erreurs={errors[:3]}. Journal illisible, la restauration de U1 ne "
        f"peut plus savoir que U1 est deja restaure : ecrite, elle defait "
        f"la re-saisie de l'utilisateur sous une trace qui nomme U1, et le "
        f"message qui dit 'refusee' est faux.",
    ))
    out.append((
        "r2_report_counts_the_refusal",
        report.get("plan_actions") == 2 and report.get("applied") == 1
        and report.get("failed_other") == 1
        and report.get("outcome_sum") == 2,
        f"rapport={report}: 2 planifiees, 1 restauree (la couche lisible), "
        f"1 echouee pour une autre raison (la couche au journal verrouille).",
    ))
    layer1 = r2.get("layer1_id")
    out.append((
        "r2_next_layer_restored_and_capture_given_back",
        r2.get("val_p") == 40 and r2.get("traces_on_v") == 1
        and v in (r2.get("undo_scope") or {}).get(_DS2, [])
        and r2.get("suppressed_at_next_layer") is not None
        and layer1 not in (r2.get("suppressed_at_next_layer") or [])
        and not r2.get("suppressed_at_end"),
        f"P apres R2={r2.get('val_p')} (attendu 40) traces sur V="
        f"{r2.get('traces_on_v')} perimetre={r2.get('undo_scope')} capture "
        f"suspendue a l'ouverture de la couche suivante="
        f"{r2.get('suppressed_at_next_layer')} (la couche refusee "
        f"{layer1} ne doit plus y etre) a la fin={r2.get('suppressed_at_end')}"
        f": le refus d'une couche ne doit ni arreter la suivante, ni laisser "
        f"les modifications de l'utilisateur hors du journal.",
    ))
    return out


def _assert_u(ctx):
    u = ctx.data.get("u") or {}
    undo = u.get("undo") or {}
    outcome = undo.get("outcome") or {}
    errors = undo.get("errors") or []
    ua, ut = u.get("ua"), u.get("ut")
    state = undo.get("state") or []
    out = [("u_no_crash", "u_crash" not in ctx.data,
            str(ctx.data.get("u_crash")))]
    restore = u.get("restore") or {}
    out.append((
        "u0_control_restore_applies",
        not restore.get("errors")
        and [tuple(x) for x in restore.get("state") or []]
        == [("A", 10), ("B", 20), ("T", 50), ("T", 50)],
        f"restauration temoin, journal lisible={restore}: A et le T modifie "
        f"reviennent, deux traces.",
    ))
    out.append((
        "u_unique_key_undone_shared_key_refused_by_name",
        undo.get("finished") is True
        and ua in (outcome.get("undone") or [])
        and ut in (outcome.get("failed") or [])
        and any(e.startswith(f"Evt {ut}:") and "no_journal" in e
                for e in errors)
        and ("A", 872) in [tuple(x) for x in state]
        and [tuple(x) for x in state].count(("T", 50)) == 2,
        f"issue={outcome} etat={state} erreurs={errors[:3]}: sans journal, "
        f"l'entite a contenu unique s'annule, celle dont le contenu est "
        f"partage est refusee par son nom (no_journal), rien d'autre.",
    ))
    notice = next((e for e in errors if e.startswith("Journal illisible")), "")
    out.append((
        "u_notice_says_the_undo_went_on",
        "'und'" in notice and "database is locked" in notice
        and "continue" in notice and "est refusee" not in notice,
        f"avis={notice!r} ; annulees={outcome.get('undone')}: l'annulation "
        f"de cette couche a ete ecrite pour {ua} ; un avis qui dit "
        f"'son annulation est refusee' fait croire a l'utilisateur que la "
        f"couche n'a pas change.",
    ))
    return out


def _assert_d(ctx):
    d = ctx.data.get("d") or {}
    first = d.get("first") or {}
    second = d.get("second") or {}
    msgs = first.get("messages") or []
    named = [m for m, level in msgs if level == "ERROR"
             and "database is locked" in m and "Rien n'a ete ecrit" in m]
    logo = first.get("logo") or []
    out = [("d_no_crash", "d_crash" not in ctx.data,
            str(ctx.data.get("d_crash")))]
    out.append((
        "d_no_exception_out_of_the_slot",
        first.get("escaped") is None and first.get("runners") == 0,
        f"exception={first.get('escaped')!r} runners={first.get('runners')}: "
        f"une exception qui sort du slot n'est dite a personne et laisse le "
        f"dialogue dans l'etat 'restauration en cours'.",
    ))
    out.append((
        "d_dialog_released",
        first.get("restore_in_progress") is False
        and first.get("run_in_flight") == ""
        and first.get("trace_id") == ""
        and first.get("restore_button") is True
        and first.get("recover_button") is True
        and first.get("progress_visible") is False
        and logo[-1:] == ["stop"],
        f"etat={ {k: first.get(k) for k in ('restore_in_progress', 'run_in_flight', 'trace_id', 'restore_button', 'recover_button', 'progress_visible', 'logo')} }"
        f": tant que ce drapeau reste leve, toute action suivante est "
        f"refusee 'une restauration est en cours' jusqu'au changement de "
        f"projet.",
    ))
    out.append((
        "d_reason_named_to_the_user",
        len(named) == 1,
        f"messages={msgs}: la raison (journal verrouille) et le fait que "
        f"rien n'a ete ecrit doivent etre dits.",
    ))
    busy = [m for m, _l in second.get("messages") or []
            if "est en cours" in m]
    out.append((
        "d_next_restore_accepted",
        second.get("escaped") is None and second.get("runners") == 1
        and not busy,
        f"seconde restauration: exception={second.get('escaped')!r} "
        f"runners={second.get('runners')} refus={busy}",
    ))
    return out


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    res = _assert_r(ctx) + _assert_u(ctx) + _assert_d(ctx)
    res.append(assert_log_contains(
        ctx.records, rf"rw_runner_journal_locked run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
