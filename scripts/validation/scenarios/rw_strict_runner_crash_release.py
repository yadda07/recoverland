# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_strict_runner_crash_release - un rewind qui plante rend la couche et le dialogue.

Invariant : I-CAPTURE (aucune edition de l'utilisateur perdue) et
I-RUN-GUARD (un run qui meurt ne laisse ni verrou ni dialogue occupe).

Le mecanisme
------------
`StrictRestoreRunner` ecrit une couche a la fois, en edition, sous la
suspension de la capture de CETTE couche (`_suppress_for_run`). Il rendait
la couche a la capture au tick SUIVANT (`_advance_group`), pas a la fin de
son commit ou de son rollback. Et trois endroits du run n'avaient pas de
garde :

  * l'ouverture d'une couche, lancee par un tick QTimer
    (`_advance_group` -> `_begin_strict_for_layer`) : une exception y
    sortait du slot, la chaine mourait sans `_finish` ;
  * le commit (`_commit_strict`), appele DANS la garde des morceaux, qui
    repond a tout par un rollback : un plantage APRES `commitChanges()`
    comptait en echec des ecritures deja dans le fichier et n'en
    journalisait aucune trace ;
  * le rollback lui-meme : une couche retiree du projet entre deux ticks
    faisait lever `destroyEditCommand`, hors de toute garde.

Ce que l'utilisateur voyait
---------------------------
Apres un rewind qui plante, le dialogue reste "Un Rewind est en cours" :
restauration, rewind, annulation, tout est refuse jusqu'au changement de
projet ; la couche reste en edition et sous suspension, ses enregistrements
ne sont plus journalises, sans un mot. Ou : "0 restauree(s)" alors que le
fichier a change, et le Rewind suivant rejoue la compensation par-dessus.

Cas (vraies GeoPackage, vrai StrictRestoreRunner, suivi de suspension)
---------------------------------------------------------------------
K1  deux couches : au tick qui suit le commit de la premiere, elle n'est
    plus suspendue (un enregistrement de l'utilisateur a cet instant est
    journalise).
K2  l'ouverture de la seconde couche plante apres `startEditing` : le run
    finit, la couche n'est plus en edition ni suspendue, le compte est
    juste, l'erreur est nommee.
K3  la verification du commit plante APRES `commitChanges()` : l'ecriture
    est dans le fichier, elle est journalisee (trace) et comptee non
    verifiee, jamais "restauree", jamais "0 ecrit".
K4  la couche est retiree du projet pendant le run : le run finit, le
    dialogue n'est plus occupe, la couche n'est plus suspendue.
K5  meme garde sur l'ouverture d'une couche de la restauration d'evenements
    (RestoreRunner) et, K6, de l'annulation (UndoRunner).

Verdict avant correctif : FAIL (K1 a K6). Apres : PASS.
"""
from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_strict_runner_crash_release"
INVARIANT = "I-RUN-GUARD"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_URI = "Point?crs=EPSG:4326&field=etat:string(0)&field=nb:long"
_ROWS = [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
_CUTOFF = "2026-09-01T09:00:00"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _gpkg(tmpdir, name):
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
    for i, (etat, nb) in enumerate(_ROWS):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("etat", etat)
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


def _etat(path, name, fid=3):
    layer = _open(path, name)
    value = next((f["etat"] for f in layer.getFeatures() if f.id() == fid),
                 "<absente>")
    del layer
    return value


def _edit_event(layer, fid, value, eid):
    from qgis.core import QgsFeatureRequest
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.serialization import (
        serialize_attributes, compute_update_delta, serialize_field_schema,
    )
    names = [f.name() for f in layer.fields()]
    feat = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([fid])))
    identity = compute_feature_identity(layer, feat)
    old = serialize_attributes(feat, names)
    layer.dataProvider().changeAttributeValues(
        {fid: {layer.fields().indexOf("etat"): value}})
    layer.reload()
    feat2 = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([fid])))
    delta = compute_update_delta(old, serialize_attributes(feat2, names),
                                 ["etat"])
    return AuditEvent(
        event_id=eid, project_fingerprint="rw_srcr_project",
        datasource_fingerprint=f"ds_srcr_{layer.name()}",
        layer_id_snapshot=layer.id(), layer_name_snapshot=layer.name(),
        provider_type="ogr", feature_identity_json=identity,
        operation_type="UPDATE", attributes_json=delta, geometry_wkb=None,
        geometry_type="Point", crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id="s_srcr",
        created_at="2026-09-01T09:10:00+00:00", restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(identity),
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)


class _Tracker:
    """Suppression as the edit tracker keeps it, per layer or global."""

    def __init__(self):
        self.layers = set()
        self.depth = 0

    def suppress_layers(self, ids):
        self.layers.update(ids)

    def unsuppress_layers(self, ids):
        self.layers.difference_update(ids)

    def suppress(self):
        self.depth += 1

    def unsuppress(self):
        self.depth = max(self.depth - 1, 0)

    def is_suppressed(self, layer_id):
        return layer_id in self.layers or self.depth > 0


def _two_layers(tmp, tag):
    out = []
    for i, name in enumerate((f"{tag}_a", f"{tag}_b")):
        path = _gpkg(tmp, name)
        layer = _open(path, name)
        event = _edit_event(layer, 3, "Z", 100 + i)
        out.append((path, name, layer, event))
    return out


def _runner(layers, tracker, trace):
    import recoverland.restore_runner as rr
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    by_fp = {ev.datasource_fingerprint: lyr for _p, _n, lyr, ev in layers}
    return rr.StrictRestoreRunner(
        [ev for _p, _n, _l, ev in layers],
        lambda e: by_fp.get(e.datasource_fingerprint),
        RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, inclusive=True),
        write_queue=None, tracker=tracker, trace_id=trace)


class _StartRaised:
    """What `start()` raised, recorded in place of a `finished` result."""

    def __init__(self, exc):
        self.error = repr(exc)


def _ended(res):
    """True when the runner emitted `finished` (a result, not a start crash)."""
    return res is not None and not isinstance(res, _StartRaised)


def _finish_of(runner, timeout_s=15.0):
    h = _h()
    results = []
    runner.finished.connect(results.append)
    try:
        runner.start()
    except Exception as exc:  # noqa: BLE001 - recorded, the checks decide
        results.append(_StartRaised(exc))
    h.pump_until(lambda: bool(results), timeout_s)
    h.pump_until(lambda: False, 0.2)
    return results[0] if results else None


def _case_k1(ctx, tmp):
    layers = _two_layers(tmp, "k1")
    tracker = _Tracker()
    runner = _runner(layers, tracker, "rw_srcr_k1")
    first_id = layers[0][2].id()
    seen = []
    original = runner._advance_group

    def advance(*args, **kwargs):
        # The tick after a layer ended, BEFORE it does anything: what the
        # capture would do with a commit the user makes right now.
        seen.append(tracker.is_suppressed(first_id))
        return original(*args, **kwargs)
    runner._advance_group = advance
    res = _finish_of(runner)
    ctx.data["k1"] = {
        "finished": _ended(res),
        # seen[0] is start() itself; seen[1] the tick after layer A.
        "a_suppressed_at_next_tick": seen[1] if len(seen) > 1 else None,
        "ticks": seen,
        "after": [_etat(p, n) for p, n, _l, _e in layers],
        "suppressed_after": sorted(tracker.layers), "depth": tracker.depth,
    }


def _case_k2(ctx, tmp):
    import recoverland.core.commit_guard as cg
    h = _h()
    layers = _two_layers(tmp, "k2")
    tracker = _Tracker()
    real = cg.CommitGuard

    class Exploding(real):
        def __init__(self, layer, trace_id=""):
            if layer.name().endswith("_b"):
                raise RuntimeError("commit guard failure on open (scripted)")
            super().__init__(layer, trace_id)

    seams = h.Seams()
    try:
        seams.set(cg, "CommitGuard", Exploding)
        runner = _runner(layers, tracker, "rw_srcr_k2")
        res = _finish_of(runner)
    finally:
        seams.restore()
    layer_b = layers[1][2]
    ctx.data["k2"] = {
        "finished": _ended(res),
        "b_editable": layer_b.isEditable(),
        "b_suppressed": tracker.is_suppressed(layer_b.id()),
        "report": dict(runner.apply_report()),
        "errors": list(getattr(res, "errors", []) or [])[:4],
        "after": [_etat(p, n) for p, n, _l, _e in layers],
    }


def _case_k3(ctx, tmp):
    import recoverland.core.commit_guard as cg
    h = _h()
    layers = _two_layers(tmp, "k3")[:1]
    tracker = _Tracker()
    real_finish = cg.CommitGuard.finish

    def exploding_finish(self, ok):
        raise RuntimeError("read-back verification crashed (scripted)")

    seams = h.Seams()
    try:
        seams.set(cg.CommitGuard, "finish", exploding_finish)
        runner = _runner(layers, tracker, "rw_srcr_k3")
        res = _finish_of(runner)
    finally:
        seams.restore()
    del real_finish
    path, name, layer, event = layers[0]
    traces = [getattr(t, "restored_from_event_id", None)
              for t in (getattr(res, "trace_events", None) or [])]
    ctx.data["k3"] = {
        "finished": _ended(res),
        "after": _etat(path, name),
        "event": event.event_id,
        "traces": traces,
        "report": dict(runner.apply_report()),
        "undo_scope": sorted(e.event_id for evts in
                             (getattr(res, "by_ds", None) or {}).values()
                             for e in evts),
        "errors": list(getattr(res, "errors", []) or [])[:4],
        "suppressed": tracker.is_suppressed(layer.id()),
    }


def _case_k4(ctx, tmp):
    """The layer is removed from the project between two ticks."""
    import recoverland.recover_dialog as rd
    from qgis.core import QgsProject
    h = _h()
    layers = _two_layers(tmp, "k4")
    project = QgsProject.instance()
    for _p, _n, lyr, _e in layers:
        project.addMapLayer(lyr, False)
    tracker = _Tracker()
    runner = _runner(layers, tracker, "rw_srcr_k4")
    b_id = layers[1][2].id()
    removed = []
    original_chunk = runner._process_strict_chunk

    def chunk():
        name = runner._strict_layer_name or ""
        if name.endswith("_b") and not removed:
            removed.append(True)
            project.removeMapLayer(b_id)
        return original_chunk()
    runner._process_strict_chunk = chunk
    journal = h.new_journal(tmp, "k4.sqlite")
    dlg = h.build_dialog(rd, h.Journal(journal), h.RecordingWriteQueue(journal),
                         [ev.datasource_fingerprint for _p, _n, _l, ev in layers])
    logs = []
    seams = h.Seams()
    try:
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: None)
        dlg._active_restore_trace_id = "rw_srcr_k4"
        dlg._restore_in_progress = True
        dlg._version_restore_events = [ev for _p, _n, _l, ev in layers]
        runner.finished.connect(dlg._on_version_restore_done)
        results = []
        runner.finished.connect(results.append)
        try:
            dlg._launch_runner(runner)
        except Exception as exc:  # noqa: BLE001
            results.append(_StartRaised(exc))
        h.pump_until(lambda: bool(results), 15.0)
        h.pump_until(lambda: False, 0.2)
        busy = dlg._run_in_flight()
    finally:
        seams.restore()
        h.close_dialog(dlg)
    res = results[0] if results else None
    ctx.data["k4"] = {
        "removed": bool(removed),
        "finished": _ended(res),
        "dialog_busy": busy,
        "b_suppressed": tracker.is_suppressed(b_id),
        "errors": list(getattr(res, "errors", []) or [])[:4],
        "a_after": _etat(layers[0][0], layers[0][1]),
    }
    for _p, _n, lyr, _e in layers[:1]:
        try:
            project.removeMapLayer(lyr.id())
        except RuntimeError:
            pass


def _fake_layers(h):
    return {"ds_srcr_ra": h.FakeLayer("lyr_ra", "ra"),
            "ds_srcr_rb": h.FakeLayer("lyr_rb", "rb")}


def _case_k5(ctx, tmp):
    """RestoreRunner: the second layer's resolver raises, from a tick."""
    import recoverland.restore_runner as rr
    h = _h()
    journal = h.new_journal(tmp, "k5.sqlite")
    ids = [h.insert_event(journal, "ds_srcr_ra", "ra", "UPDATE", 10, fid=1),
           h.insert_event(journal, "ds_srcr_rb", "rb", "UPDATE", 20, fid=2)]
    events = h.fetch_events(journal, ids)
    layers = _fake_layers(h)
    tracker = _Tracker()

    def resolver(event):
        if event.datasource_fingerprint == "ds_srcr_rb":
            raise RuntimeError("layer lookup failed (scripted)")
        return layers[event.datasource_fingerprint]

    calls = []
    seams = h.Seams()
    try:
        seams.set(rr, "restore_batch", h.scripted_restore_batch({}, calls))
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        runner = rr.RestoreRunner(events, resolver, None, tracker=tracker,
                                  trace_id="rw_srcr_k5")
        res = _finish_of(runner)
    finally:
        seams.restore()
    ctx.data["k5"] = {
        "finished": _ended(res), "report": dict(runner.apply_report()),
        "errors": list(getattr(res, "errors", []) or [])[:3],
        "suppressed": sorted(tracker.layers), "depth": tracker.depth,
    }


def _case_k6(ctx, tmp):
    """UndoRunner: the second layer's preparation raises, from a tick."""
    import recoverland.restore_runner as rr
    h = _h()
    journal = h.new_journal(tmp, "k6.sqlite")
    ids = [h.insert_event(journal, "ds_srcr_ra", "ra", "UPDATE", 10, fid=1),
           h.insert_event(journal, "ds_srcr_rb", "rb", "UPDATE", 20, fid=2)]
    events = h.fetch_events(journal, ids)
    layers = _fake_layers(h)
    tracker = _Tracker()
    undo_calls = []
    prepare, apply_one = h.scripted_undo({}, undo_calls)

    def exploding_prepare(layer, evts, journal=None):
        if layer.name() == "rb":
            raise RuntimeError("fid pre-resolution failed (scripted)")
        return prepare(layer, evts)

    seams = h.Seams()
    try:
        seams.set(rr, "undo_prepare", exploding_prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        runner = rr.UndoRunner(
            {e.datasource_fingerprint: [e] for e in events},
            lambda e: layers[e.datasource_fingerprint], tracker=tracker)
        res = _finish_of(runner)
    finally:
        seams.restore()
    ctx.data["k6"] = {
        "finished": _ended(res), "outcome": dict(runner.undo_outcome()),
        "errors": list(getattr(res, "errors", []) or [])[:3],
        "suppressed": sorted(tracker.layers), "depth": tracker.depth,
        "ids": ids,
    }


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_srcr_")
    flog(f"rw_strict_runner_crash_release setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    tmp = ctx.data["tmpdir"]
    ctx.data["errors"] = {}
    for name, case in (("k1", _case_k1), ("k2", _case_k2), ("k3", _case_k3),
                       ("k4", _case_k4), ("k5", _case_k5),
                       ("k6", _case_k6)):
        sub = Path(tmp) / name
        sub.mkdir(exist_ok=True)
        try:
            case(ctx, str(sub))
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data["errors"][name] = (f"{type(exc).__name__}: {exc} "
                                        f"{traceback.format_exc()[-900:]}")
    flog(f"rw_strict_runner_crash_release run: trace_id={ctx.trace_id} "
         f"k1={ctx.data.get('k1')} k2={ctx.data.get('k2')} "
         f"k3={ctx.data.get('k3')} k4={ctx.data.get('k4')} "
         f"errors={ctx.data['errors']}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", not d.get("errors"), f"crash={d.get('errors')}")]
    k1 = d.get("k1") or {}
    out.append((
        "k1_layer_given_back_when_its_commit_ends",
        k1.get("finished") is True and k1.get("a_suppressed_at_next_tick") is False
        and k1.get("after") == ["c", "c"] and not k1.get("suppressed_after"),
        f"suspension de la premiere couche au tick suivant son commit="
        f"{k1.get('a_suppressed_at_next_tick')} (ticks={k1.get('ticks')}) "
        f"etats apres={k1.get('after')}. Encore suspendue, un enregistrement "
        f"de l'utilisateur sur une couche que le run a fini d'ecrire n'est "
        f"pas journalise.",
    ))
    k2 = d.get("k2") or {}
    bd = k2.get("report") or {}
    out.append((
        "k2_open_crash_ends_the_run_and_releases_the_layer",
        k2.get("finished") is True and k2.get("b_editable") is False
        and k2.get("b_suppressed") is False,
        f"plantage a l'ouverture de la seconde couche : fin={k2.get('finished')} "
        f"en edition={k2.get('b_editable')} suspendue={k2.get('b_suppressed')}. "
        f"Sans fin de run, le dialogue refuse tout jusqu'au changement de "
        f"projet ; la couche reste en edition et hors du journal.",
    ))
    out.append((
        "k2_accounted_and_named",
        bd.get("applied") == 1 and bd.get("apply_fail") == 1
        and bd.get("plan_actions") == 2
        and any("before any write" in e for e in k2.get("errors") or [])
        and k2.get("after") == ["c", "Z"],
        f"rapport={bd} erreurs={k2.get('errors')} etats={k2.get('after')} : "
        f"la premiere couche restauree, la seconde en echec nomme, rien "
        f"d'ecrit sur elle.",
    ))
    k3 = d.get("k3") or {}
    bd = k3.get("report") or {}
    out.append((
        "k3_committed_write_is_journalled_and_undoable",
        k3.get("finished") is True and k3.get("after") == "c"
        and k3.get("event") in (k3.get("traces") or [])
        and k3.get("event") in (k3.get("undo_scope") or []),
        f"verification plantee apres le commit : fichier={k3.get('after')!r} "
        f"traces={k3.get('traces')} perimetre d'annulation="
        f"{k3.get('undo_scope')}. L'ecriture est dans le fichier : sans trace, "
        f"le Rewind suivant rejoue la compensation par-dessus.",
    ))
    out.append((
        "k3_counted_unverified_never_restored",
        bd.get("applied", 0) == 0
        and bd.get("failed_postcommit_verification") == 1
        and any("verification" in e for e in k3.get("errors") or [])
        and k3.get("suppressed") is False,
        f"rapport={bd} erreurs={k3.get('errors')} : ni 'restauree' (rien n'a "
        f"ete verifie) ni 'rien d'ecrit' (le fichier a change).",
    ))
    k4 = d.get("k4") or {}
    out.append((
        "k4_removed_layer_does_not_wedge_the_dialog",
        k4.get("removed") is True and k4.get("finished") is True
        and not k4.get("dialog_busy") and k4.get("b_suppressed") is False,
        f"couche retiree du projet pendant le run : fin={k4.get('finished')} "
        f"dialogue occupe={k4.get('dialog_busy')!r} suspendue="
        f"{k4.get('b_suppressed')} erreurs={k4.get('errors')}. Le rollback "
        f"levait sur l'objet detruit, la chaine mourait et le dialogue "
        f"restait 'Un Rewind est en cours'.",
    ))
    k5 = d.get("k5") or {}
    bd = k5.get("report") or {}
    out.append((
        "k5_restore_runner_group_open_crash_ends_the_run",
        k5.get("finished") is True and bd.get("applied") == 1
        and bd.get("apply_fail") == 1 and not k5.get("suppressed"),
        f"restauration, recherche de la seconde couche qui plante : fin="
        f"{k5.get('finished')} rapport={bd} suspendues={k5.get('suppressed')} "
        f"erreurs={k5.get('errors')}. Le tick levait hors de toute garde et "
        f"le dialogue restait occupe.",
    ))
    k6 = d.get("k6") or {}
    outcome = k6.get("outcome") or {}
    ids = k6.get("ids") or [None, None]
    out.append((
        "k6_undo_runner_group_open_crash_ends_the_run",
        k6.get("finished") is True and outcome.get("undone") == [ids[0]]
        and outcome.get("failed") == [ids[1]] and not k6.get("suppressed"),
        f"annulation, preparation de la seconde couche qui plante : fin="
        f"{k6.get('finished')} resultat={outcome} suspendues="
        f"{k6.get('suppressed')} erreurs={k6.get('errors')}. L'evenement non "
        f"defait reste annulable, nomme en echec, jamais laisse sans fin.",
    ))
    return out
