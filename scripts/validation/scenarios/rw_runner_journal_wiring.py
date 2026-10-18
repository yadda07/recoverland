"""rw_runner_journal_wiring - les runners donnent le journal aux preuves d'identite.

Invariant : I-3 (identite prouvee avant toute ecriture) sans refus injuste,
sur le chemin que l'interface execute vraiment.

Le mecanisme
------------
Une cle n'est une identite que si elle n'a jamais change de mains ; le
journal le prouve (`restore_service._key_continuity`) : il contient
l'evenement et, apres lui, aucune suppression ni creation de la cle. C'est
la seule corroboration d'une cle dont le CONTENU est partage par d'autres
entites (un booleen, un statut). L'executeur publie les signatures qui la
recoivent (`build_fid_cache(..., journal=)`, `restore_batch(...,
journal=)`, `undo_prepare(..., journal=)`, `_apply_via_buffer(...,
journal=)`), mais les trois runners les appelaient SANS journal : sur
l'interface, la preuve n'existait pas. Le dialogue, lui, ne donnait meme
pas le chemin du journal a l'annulation (UndoRunner n'a pas de file
d'ecriture).

Ce que l'utilisateur voyait
---------------------------
Il coche par erreur "flag" sur une entite d'une GeoPackage ou presque
toutes les entites sont cochees. Restaurer l'evenement, annuler la
restauration, ou revenir a une date anterieure : refus "Target feature
could not be verified" (value_shared), sa modification d'une seconde ne se
defait plus, alors que le journal prouve que la cle n'a jamais change de
mains. Les tests de l'executeur passaient : ils appellent les fonctions
avec le journal, pas les runners.

Ce qui est prouve
-----------------
E  De bout en bout, vraie GeoPackage, vrai journal SQLite sur disque, vrais
   runners (RestoreRunner, UndoRunner, StrictRestoreRunner) : la valeur
   partagee revient (E1 restauration, E2 annulation, E3 rewind).
L  Cycle de vie de la connexion (coutures de l'executeur qui enregistrent
   ce qu'elles recoivent) : une connexion en LECTURE SEULE sur le journal
   du run, la meme pour tous les morceaux d'une couche, une par couche,
   fermee a la fin de chaque couche, sur un crash, sur une annulation, et a
   la fin du run.
D  Le dialogue donne le chemin du journal a chaque runner qu'il lance
   (restauration, rewind, annulation, annulation de session).

Verdict avant correctif : FAIL (E1, E2, E3 : refus value_shared ; L : aucun
journal recu ; D : aucun chemin transmis).
Verdict apres correctif : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_runner_journal_wiring"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_DS = "rw_rjw_datasource"
_URI = ("Point?crs=EPSG:4326&field=etat:string(0)&field=flag:boolean"
        "&field=nb:long")
# (etat, flag, nb): C (fid 3) is the only FALSE one.
_ROWS = [("a", True, 1), ("b", True, 1), ("c", False, 1), ("d", True, 1),
         ("e", True, 1)]


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


# --------------------------------------------------------------------------
# Real GeoPackage and real journal
# --------------------------------------------------------------------------
def _gpkg(tmpdir, name, rows=_ROWS):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
        QgsFeature, QgsGeometry, QgsPointXY,
    )
    mem = QgsVectorLayer(_URI, "seed", "memory")
    path = str(Path(tmpdir) / f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = name
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
    feats = []
    for i, (etat, flag, nb) in enumerate(rows):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("etat", etat)
        feat.setAttribute("flag", flag)
        feat.setAttribute("nb", nb)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, 45.0)))
        feats.append(feat)
    ok, _ = layer.dataProvider().addFeatures(feats)
    if not ok:
        raise RuntimeError(f"GPKG seed failed: {layer.dataProvider().errors()}")
    del layer
    return path


def _open(path, name):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche invalide: {path}")
    return layer


def _flags(path, name):
    layer = _open(path, name)
    out = sorted((f.id(), f["etat"], f["flag"]) for f in layer.getFeatures())
    del layer
    return out


def _flag_of(rows, fid=3):
    return next((r[2] for r in rows or [] if r[0] == fid), "<absente>")


def _event(layer, operation, identity, attributes, minute, **extra):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.serialization import serialize_field_schema
    ident = identity if isinstance(identity, str) else json.dumps(identity)
    payload = dict(
        event_id=None, project_fingerprint="rw_rjw_project",
        datasource_fingerprint=_DS, layer_id_snapshot=layer.id(),
        layer_name_snapshot=layer.name(), provider_type="ogr",
        feature_identity_json=ident, operation_type=operation,
        attributes_json=attributes, geometry_wkb=None, geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id=None,
        created_at=f"2026-09-01T09:{minute:02d}:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(ident),
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)
    payload.update(extra)
    return AuditEvent(**payload)


def _journalled(path, event):
    """Write *event* to the journal FILE and read it back (id from SQLite)."""
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


def _edit_flag(layer, fid, value):
    from qgis.core import QgsFeatureRequest
    from recoverland.core.serialization import (
        serialize_attributes, compute_update_delta,
    )
    from recoverland.core.identity import compute_feature_identity
    names = [f.name() for f in layer.fields()]
    feat = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([fid])))
    identity = compute_feature_identity(layer, feat)
    old = serialize_attributes(feat, names)
    idx = layer.fields().indexOf("flag")
    layer.dataProvider().changeAttributeValues({fid: {idx: value}})
    layer.reload()
    feat2 = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([fid])))
    new = serialize_attributes(feat2, names)
    return compute_update_delta(old, new, ["flag"]), identity


def _new_runner(cls, *args, journal_path=None, **kwargs):
    """The runner, with the journal path when its constructor takes it."""
    import inspect
    try:
        accepts = "journal_path" in inspect.signature(cls).parameters
    except (TypeError, ValueError):
        accepts = False
    if accepts:
        kwargs["journal_path"] = journal_path
    return cls(*args, **kwargs)


def _run_to_end(runner, timeout_s=20.0):
    h = _h()
    results = []
    runner.finished.connect(results.append)
    runner.start()
    h.pump_until(lambda: bool(results), timeout_s)
    return results[0] if results else None


def _case_e(ctx, tmp):
    """E1 restore, E2 undo, E3 rewind: a key the journal proves continuous."""
    import recoverland.restore_runner as rr
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    h = _h()
    res, rows = {}, {}

    # E1: restore 'flag False -> True' of C (TRUE is carried by 4 features).
    path = _gpkg(tmp, "e1")
    journal = h.new_journal(tmp, "e1.sqlite")
    layer = _open(path, "e1")
    delta, identity = _edit_flag(layer, 3, True)
    event = _journalled(journal, _event(layer, "UPDATE", identity, delta, 0))
    rows["e1_before"] = _flags(path, "e1")
    runner = _new_runner(rr.RestoreRunner, [event], lambda _e, lyr=layer: lyr,
                         h.RecordingWriteQueue(journal), tracker=None,
                         trace_id="rw_rjw_e1", journal_path=journal)
    result = _run_to_end(runner)
    res["e1"] = {"report": dict(runner.apply_report()),
                 "errors": list(getattr(result, "errors", []) or [])[:4]}
    del layer
    rows["e1"] = _flags(path, "e1")

    # E2: undo of that restore. FALSE is carried by C and D; the journal
    # holds the edit, then the trace of its restore.
    undo_rows = [("a", True, 1), ("b", True, 1), ("c", False, 1),
                 ("d", False, 1), ("e", True, 1)]
    path = _gpkg(tmp, "e2", undo_rows)
    journal = h.new_journal(tmp, "e2.sqlite")
    layer = _open(path, "e2")
    delta, identity = _edit_flag(layer, 3, True)
    _edit_flag(layer, 3, False)  # the restore, done earlier
    event = _journalled(journal, _event(layer, "UPDATE", identity, delta, 0))
    _journalled(journal, _event(
        layer, "UPDATE", identity,
        json.dumps({"_restore_ref": event.event_id}), 10,
        restored_from_event_id=event.event_id))
    rows["e2_before"] = _flags(path, "e2")
    runner = _new_runner(rr.UndoRunner, {_DS: [event]}, lambda _e, lyr=layer: lyr,
                         tracker=None, journal_path=journal)
    result = _run_to_end(runner)
    res["e2"] = {"outcome": dict(runner.undo_outcome()),
                 "errors": list(getattr(result, "errors", []) or [])[:4]}
    del layer
    rows["e2"] = _flags(path, "e2")

    # E3: rewind to a date before the same edit.
    path = _gpkg(tmp, "e3")
    journal = h.new_journal(tmp, "e3.sqlite")
    layer = _open(path, "e3")
    delta, identity = _edit_flag(layer, 3, True)
    event = _journalled(journal, _event(layer, "UPDATE", identity, delta, 0))
    rows["e3_before"] = _flags(path, "e3")
    cutoff = RestoreCutoff(CutoffType.BY_DATE, "2026-09-01T08:59:00",
                           inclusive=True)
    runner = _new_runner(rr.StrictRestoreRunner, [event], lambda _e, lyr=layer: lyr,
                         cutoff, write_queue=h.RecordingWriteQueue(journal),
                         tracker=None, trace_id="rw_rjw_e3",
                         journal_path=journal)
    result = _run_to_end(runner)
    res["e3"] = {"report": dict(runner.apply_report()),
                 "errors": list(getattr(result, "errors", []) or [])[:4]}
    del layer
    rows["e3"] = _flags(path, "e3")
    ctx.data["e_res"] = res
    ctx.data["e_rows"] = rows


# --------------------------------------------------------------------------
# Lifecycle of the connection, observed through recording executor seams
# --------------------------------------------------------------------------
def _probe(conn):
    """What a received journal argument is, at the moment it is received."""
    if conn is None:
        return {"given": False}
    out = {"given": True, "id": id(conn)}
    try:
        out["reads"] = conn.execute(
            "SELECT COUNT(*) FROM audit_event").fetchone()[0] >= 0
    except Exception as exc:  # noqa: BLE001
        out["reads"] = f"error: {exc}"
    try:
        conn.execute("CREATE TABLE rw_rjw_probe (x INTEGER)")
        out["read_only"] = False
    except sqlite3.Error:
        out["read_only"] = True
    return out


def _closed(conn):
    try:
        conn.execute("SELECT 1")
        return False
    except sqlite3.ProgrammingError:
        return True
    except sqlite3.Error:
        return False


class _Layer:
    """Just enough of a layer for the runners' preflight and refresh."""

    def __init__(self, name):
        self._name = name

    def id(self):
        return f"lyr_{self._name}"

    def name(self):
        return self._name

    def isEditable(self):  # noqa: N802
        return False

    def isModified(self):  # noqa: N802
        return False

    def featureCount(self):  # noqa: N802
        return 10

    def reload(self):
        return None

    def triggerRepaint(self):  # noqa: N802
        return None

    def updateExtents(self):  # noqa: N802
        return None

    def dataProvider(self):  # noqa: N802
        class _P:
            def reloadData(self):  # noqa: N802
                return None
        return _P()


def _case_l(ctx, tmp):
    """Restore and undo runners over two layers, with a crash and a cancel."""
    import recoverland.restore_runner as rr
    from recoverland.core.audit_backend import RestoreReport
    from recoverland.core.restore_service import UndoBatchContext
    h = _h()
    journal = h.new_journal(tmp, "l.sqlite")
    ids_a = [h.insert_event(journal, "ds_rjw_a", "a", "UPDATE", 10 + i, fid=i)
             for i in range(7)]
    ids_b = [h.insert_event(journal, "ds_rjw_b", "b", "UPDATE", 30 + i, fid=i)
             for i in range(2)]
    events = h.fetch_events(journal, ids_a + ids_b)
    layers = {"ds_rjw_a": _Layer("a"), "ds_rjw_b": _Layer("b")}
    seen = []   # (who, layer, probe, conn)

    def fid_cache(layer, events, journal=None):
        seen.append(("cache", layer.name(), _probe(journal), journal))
        return {}

    def batch(layer, events, fid_cache=None, trace_id="", journal=None):
        seen.append(("batch", layer.name(), _probe(journal), journal))
        if crash.get(layer.name()):
            raise RuntimeError("provider failure (scripted)")
        ids = [e.event_id for e in events]
        return RestoreReport(succeeded=ids, failed={},
                             total_requested=len(ids), trace_events=())

    def prepare(layer, events, journal=None):
        seen.append(("prepare", layer.name(), _probe(journal), journal))
        return UndoBatchContext(ordered=list(reversed(events)), fid_cache={},
                                layer_error=None)

    def apply_one(layer, event, context):
        return {"success": True, "message": "Undo reverted"}

    seams = h.Seams()
    out = {}
    crash = {}
    try:
        seams.set(rr, "build_fid_cache", fid_cache)
        seams.set(rr, "restore_batch", batch)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)

        def restore_run(label, trace):
            del seen[:]
            runner = _new_runner(
                rr.RestoreRunner, list(events),
                lambda e: layers[e.datasource_fingerprint],
                h.RecordingWriteQueue(journal), tracker=None,
                trace_id=trace, journal_path=journal)
            result = _run_to_end(runner)
            conns = [c for _w, _l, _p, c in seen if c is not None]
            out[label] = {
                "finished": result is not None,
                "calls": [(w, lay, p) for w, lay, p, _c in seen],
                "all_closed": bool(conns) and all(_closed(c) for c in conns),
                "report": dict(runner.apply_report()),
                "errors": list(getattr(result, "errors", []) or [])[:3],
            }

        restore_run("l1", "rw_rjw_l1")
        crash["a"] = True
        restore_run("l2", "rw_rjw_l2")
        crash.clear()

        # L3: undo over the two layers.
        del seen[:]
        by_ds = {"ds_rjw_a": events[:7], "ds_rjw_b": events[7:]}
        runner = _new_runner(rr.UndoRunner, by_ds,
                             lambda e: layers[e.datasource_fingerprint],
                             tracker=None, journal_path=journal)
        result = _run_to_end(runner)
        conns = [c for _w, _l, _p, c in seen if c is not None]
        out["l3"] = {"finished": result is not None,
                     "calls": [(w, lay, p) for w, lay, p, _c in seen],
                     "all_closed": bool(conns) and all(_closed(c) for c in conns)}

        # L4: a cancel while the first layer of a restore is being written.
        del seen[:]
        runner = _new_runner(
            rr.RestoreRunner, list(events),
            lambda e: layers[e.datasource_fingerprint],
            h.RecordingWriteQueue(journal), tracker=None,
            trace_id="rw_rjw_l4", journal_path=journal)
        results = []
        runner.finished.connect(results.append)

        def cancel_on_first_batch(layer, evts, fid_cache=None, trace_id="",
                                  journal=None):
            runner.cancel()
            return batch(layer, evts, fid_cache, trace_id, journal)
        seams.set(rr, "restore_batch", cancel_on_first_batch)
        runner.start()
        h.pump_until(lambda: bool(results), 10.0)
        conns = [c for _w, _l, _p, c in seen if c is not None]
        out["l4"] = {"finished": bool(results),
                     "calls": [(w, lay, p) for w, lay, p, _c in seen],
                     "all_closed": bool(conns) and all(_closed(c) for c in conns)}
    finally:
        seams.restore()
    ctx.data["l"] = out


def _case_s(ctx, tmp):
    """L5: the strict runner hands the SAME connection to preflight and apply."""
    import recoverland.restore_runner as rr
    import recoverland.core.restore_planner as planner
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    h = _h()
    path = _gpkg(tmp, "s1")
    journal = h.new_journal(tmp, "s1.sqlite")
    layer = _open(path, "s1")
    delta, identity = _edit_flag(layer, 3, True)
    event = _journalled(journal, _event(layer, "UPDATE", identity, delta, 0))
    seen = []
    real_apply = rr._apply_via_buffer
    real_preflight = planner.preflight_check

    def apply(layer_, op, evt, remap=None, trace_id="", journal=None):
        seen.append(("apply", _probe(journal), journal))
        extra = {"journal": journal} if journal is not None else {}
        return real_apply(layer_, op, evt, remap, trace_id=trace_id, **extra)

    def preflight(plan, oldest_event_date=None, conn=None):
        seen.append(("preflight", _probe(conn), conn))
        return real_preflight(plan, oldest_event_date, conn=conn)

    seams = h.Seams()
    try:
        seams.set(rr, "_apply_via_buffer", apply)
        seams.set(planner, "preflight_check", preflight)
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2026-09-01T08:59:00",
                               inclusive=True)
        runner = _new_runner(rr.StrictRestoreRunner, [event], lambda _e, lyr=layer: lyr,
                             cutoff, write_queue=h.RecordingWriteQueue(journal),
                             tracker=None, trace_id="rw_rjw_s1",
                             journal_path=journal)
        result = _run_to_end(runner)
    finally:
        seams.restore()
    conns = [c for _w, _p, c in seen if c is not None]
    ctx.data["s"] = {
        "finished": result is not None,
        "calls": [(w, p) for w, p, _c in seen],
        "same_conn": len({id(c) for c in conns}) == 1 and len(conns) == len(seen),
        "all_closed": bool(conns) and all(_closed(c) for c in conns),
        "flag_after": _flag_of(_flags(path, "s1")),
    }
    del layer


# --------------------------------------------------------------------------
# The dialog hands the journal path to every runner it launches
# --------------------------------------------------------------------------
def _case_d(ctx, tmp):
    import recoverland.recover_dialog as rd
    h = _h()
    fp = "ds_rjw_d"
    journal = h.new_journal(tmp, "d.sqlite")
    e1 = h.insert_event(journal, fp, "d", "UPDATE", 10, fid=1, session_id="sess1")
    (ev,) = h.fetch_events(journal, [e1])
    created = {}

    def recording(kind):
        class Recording:
            def __init__(self, *args, **kwargs):
                created.setdefault(kind, []).append(dict(kwargs))
                self.progress = h.Signal()
                self.layer_started = h.Signal()
                self.finished = h.Signal()
                self._cancelled = False
                self._total_ok = 0
                self._by_ds = {}

            def cancel(self):
                self._cancelled = True

            def start(self):
                return None
        return Recording

    logs, asked = [], []
    seams = h.Seams()
    h.PreflightDialog.shown = []
    try:
        seams.set(rd, "RestoreRunner", recording("restore"))
        seams.set(rd, "UndoRunner", recording("undo"))
        seams.set(rd, "StrictRestoreRunner", recording("strict"))
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "find_target_layer",
                  lambda _evt, _c=None: h.FakeLayer("lyr_d", "d"))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))

        def fresh():
            dlg = h.build_dialog(rd, h.Journal(journal),
                                 h.RecordingWriteQueue(journal), [fp])
            return dlg

        # Restore (Mode A).
        dlg = fresh()
        dlg._search_events = [ev]
        dlg.selected_rows = [0]
        dlg.restore_selected_data()
        h.close_dialog(dlg)
        # Rewind.
        dlg = fresh()
        dlg._version_restore_cutoff = h.cutoff(h.T0)
        dlg._active_restore_trace_id = "rw_rjw_d"
        dlg._execute_version_restore([ev])
        h.close_dialog(dlg)
        # Undo of the last run.
        dlg = fresh()
        dlg._last_restore_by_ds = {fp: [ev]}
        dlg._last_restore_events = [ev]
        dlg._last_run_ids = {e1}
        dlg._undo_last_restore()
        h.close_dialog(dlg)
        # Undo of a whole session.
        dlg = fresh()
        dlg._undo_session("sess1")
        h.close_dialog(dlg)
    finally:
        seams.restore()
    ctx.data["d"] = {kind: [kw.get("journal_path") for kw in kws]
                     for kind, kws in created.items()}
    ctx.data["d_journal"] = journal
    ctx.data["d_logs"] = logs[-4:]


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_rjw_")
    flog(f"rw_runner_journal_wiring setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    tmp = ctx.data["tmpdir"]
    ctx.data["errors"] = {}
    for name, case in (("e", _case_e), ("l", _case_l), ("s", _case_s),
                       ("d", _case_d)):
        try:
            case(ctx, tmp)
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data["errors"][name] = (f"{type(exc).__name__}: {exc} "
                                        f"{traceback.format_exc()[-900:]}")
    flog(f"rw_runner_journal_wiring run: trace_id={ctx.trace_id} "
         f"e={ctx.data.get('e_res')} rows={ctx.data.get('e_rows')} "
         f"errors={ctx.data['errors']}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = [("scenario_ran", not d.get("errors"),
            f"crash={d.get('errors')}")]
    res = d.get("e_res") or {}
    rows = d.get("e_rows") or {}
    for name, expected, what in (
            ("e1", False, "restaurer 'flag: False -> True' (RestoreRunner)"),
            ("e2", True, "annuler cette restauration (UndoRunner)"),
            ("e3", False, "revenir avant cette edition (StrictRestoreRunner)")):
        out.append((
            f"{name}_continuous_key_proven_through_the_runner",
            _flag_of(rows.get(name)) is expected,
            f"{what} sur la cle 3, que le journal prouve jamais redonnee : "
            f"flag apres={_flag_of(rows.get(name))!r} attendu={expected!r} "
            f"(avant={_flag_of(rows.get(name + '_before'))!r}) "
            f"resultat={res.get(name)}. Sans le journal le runner refuse "
            f"(valeur partagee), et l'erreur d'un clic ne se defait plus.",
        ))

    lres = d.get("l") or {}
    for label, what in (("l1", "restauration sur deux couches"),
                        ("l2", "restauration dont la premiere couche plante"),
                        ("l3", "annulation sur deux couches"),
                        ("l4", "restauration annulee pendant la premiere "
                               "couche")):
        case = lres.get(label) or {}
        probes = [p for _w, _l, p in case.get("calls") or []]
        out.append((
            f"{label}_executor_receives_a_read_only_journal",
            bool(probes) and all(p.get("given") and p.get("reads") is True
                                 and p.get("read_only") is True
                                 for p in probes),
            f"{what} : ce que l'executeur a recu={case.get('calls')}. Chaque "
            f"appel qui decide d'une identite doit recevoir une connexion "
            f"en lecture seule sur le journal du run.",
        ))
        out.append((
            f"{label}_connections_closed_and_run_finished",
            case.get("finished") is True and case.get("all_closed") is True,
            f"{what} : fin={case.get('finished')} toutes fermees="
            f"{case.get('all_closed')}. Une connexion laissee ouverte tient "
            f"un verrou de lecture sur le journal apres le run.",
        ))
    l1 = lres.get("l1") or {}
    by_layer = {}
    for who, lay, p in l1.get("calls") or []:
        by_layer.setdefault(lay, set()).add(p.get("id"))
    out.append((
        "l1_one_connection_per_layer_group",
        set(by_layer) == {"a", "b"} and all(len(v) == 1 for v in by_layer.values())
        and len(by_layer.get("a", set()) | by_layer.get("b", set())) == 2,
        f"connexions par couche={ {k: len(v) for k, v in by_layer.items()} } "
        f"(couche a : deux morceaux de 5 et 2 evenements). Une par couche, "
        f"la meme pour tous ses morceaux : le cache de l'executeur la garde "
        f"d'un morceau a l'autre.",
    ))
    l2 = lres.get("l2") or {}
    bd = l2.get("report") or {}
    out.append((
        "l2_crash_keeps_the_run_going_and_accounted",
        l2.get("finished") is True and bd.get("failed") == 7
        and bd.get("applied") == 2,
        f"rapport={bd} erreurs={l2.get('errors')} : la couche qui plante est "
        f"comptee en echec, la suivante est ecrite avec sa propre connexion.",
    ))
    s = d.get("s") or {}
    probes = [p for _w, p in s.get("calls") or []]
    out.append((
        "l5_strict_preflight_and_apply_share_one_read_only_journal",
        s.get("finished") is True and bool(probes)
        and {w for w, _p in s.get("calls") or []} == {"preflight", "apply"}
        and all(p.get("given") and p.get("read_only") is True for p in probes)
        and s.get("same_conn") is True and s.get("all_closed") is True
        and s.get("flag_after") is False,
        f"rewind : appels={s.get('calls')} meme connexion={s.get('same_conn')} "
        f"fermee={s.get('all_closed')} flag apres={s.get('flag_after')!r}. "
        f"La garde de couverture et les preuves de cle lisent le meme "
        f"journal, ouvert une fois pour la couche et ferme avec elle.",
    ))
    dres = d.get("d") or {}
    journal = d.get("d_journal")
    for kind, what in (("restore", "restauration d'evenements"),
                       ("strict", "rewind et annulation de session"),
                       ("undo", "annulation du dernier restore")):
        paths = dres.get(kind) or []
        expected = 2 if kind == "strict" else 1
        out.append((
            f"d_dialog_hands_the_journal_to_{kind}_runner",
            len(paths) == expected and all(
                p and os.path.normcase(os.path.abspath(p))
                == os.path.normcase(os.path.abspath(journal)) for p in paths),
            f"{what} : chemins transmis={paths} journal={journal} "
            f"(messages={d.get('d_logs')}). Sans lui, l'annulation n'a aucun "
            f"journal et le rewind depend de la file d'ecriture.",
        ))
    out.append(assert_log_contains(
        ctx.records, r"journal_read opened", name="journal_read_logged",
        min_count=3))
    return out
