# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_rewind_coverage_journal_unreadable - pas de rewind sans l'horizon de purge durable.

Invariant : I-REWIND-01 (un rewind ne produit jamais, annonce restaure, un
etat que la couche n'a jamais eu).

Le mecanisme
------------
Une purge efface le debut de l'historique d'une couche ; le journal garde
la date jusqu'ou elle a efface (`journal_coverage`), et la garde de
couverture (`restore_planner.preflight_check(plan, conn=...)`) refuse une
date anterieure : rejouer ce qui a survecu produirait un etat que la
couche n'a jamais eu. Sans connexion au journal, seule la memoire de la
session est lue, et elle ne survit pas a un redemarrage (la purge
automatique tourne au demarrage de QGIS : c'est justement le cas ou elle
est vide).

Deux chemins du rewind lisaient cette garde sans le journal :

  * `StrictRestoreRunner` ouvrait le journal pour la garde et, si
    l'ouverture echouait (verrou, partage reseau, chemin trop long),
    ecrivait "coverage lookup failed; session horizon only" dans le log et
    CONTINUAIT : le rewind rejouait la fenetre purgee.
  * le chemin du journal venait de la file d'ecriture seule
    (`write_queue.journal_path`) : une file jamais demarree (echec
    d'initialisation apres l'ouverture du journal) ne le connait pas, et le
    rewind partait sans garde durable, en silence.

La restauration d'evenements (mode A) a ete revue : son plan n'a pas de
date de coupure (`plan_event_restore`), elle remet l'image AVANT d'un
evenement choisi, elle ne reconstitue pas un etat a une date a partir de
ce qui a survecu ; une purge anterieure ne peut pas la rendre fausse. Le
dialogue lui passe deja sa connexion (rw_dialog_scope_misc, cas E). C'est
prouve ici (C3) : pas un defaut.

Ce que l'utilisateur voyait
---------------------------
"1 entite(s) restauree(s) avec succes" pour un retour a une date que la
purge avait rendue irreconstituable : la couche reprend un etat qu'elle
n'a jamais eu.

Cas (vraie GeoPackage)
----------------------
C1  runner : le journal du run ne s'ouvre pas en lecture ; la couche doit
    etre refusee, nommee, intacte.
C2  dialogue, vrai StrictRestoreRunner : journal avec un horizon de purge
    DURABLE qui couvre la date, file d'ecriture sans chemin ; le dialogue
    doit donner le journal au runner, qui refuse la couche.
C3  mode A : plan sans date de coupure, la garde ne s'y applique pas
    (controle : pas un defaut).

Verdict avant correctif : FAIL (C1, C2 ecrivent). Verdict apres : PASS.
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_rewind_coverage_journal_unreadable"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_DS = "rw_rcju_datasource"
_URI = "Point?crs=EPSG:4326&field=etat:string(0)&field=nb:long"
_ROWS = [("a", 1), ("b", 2), ("c", 3), ("d", 4)]
_EDIT_AT = "2026-09-01T09:10:00+00:00"
_HORIZON = "2026-09-01T09:05:00+00:00"
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


def _edit_event(layer, fid, value):
    """Edit 'etat' of *fid* through the provider; the captured UPDATE."""
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
        event_id=None, project_fingerprint="rw_rcju_project",
        datasource_fingerprint=_DS, layer_id_snapshot=layer.id(),
        layer_name_snapshot=layer.name(), provider_type="ogr",
        feature_identity_json=identity, operation_type="UPDATE",
        attributes_json=delta, geometry_wkb=None, geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id="s_rcju", created_at=_EDIT_AT,
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


def _record_durable_horizon(path, datasource, through):
    """What a purge leaves in the JOURNAL, and nothing in session memory."""
    from recoverland.core.journal_writes import ensure_v7_schema
    conn = sqlite3.connect(path)
    try:
        ensure_v7_schema(conn)
        conn.execute(
            "INSERT INTO journal_coverage (datasource_fingerprint, "
            "purged_through, purged_events, updated_at) VALUES (?,?,?,?)",
            (datasource, through, 12, through))
        conn.commit()
    finally:
        conn.close()


class _TraceSink:
    """A write queue that keeps what it is given (the journal is unusable)."""

    def __init__(self, journal_path):
        self.journal_path = journal_path
        self.enqueued = []

    def enqueue(self, events, commit=None, journal_path=None):
        self.enqueued.extend(events)
        return True


def _new_runner(cls, *args, journal_path=None, **kwargs):
    import inspect
    try:
        accepts = "journal_path" in inspect.signature(cls).parameters
    except (TypeError, ValueError):
        accepts = False
    if accepts:
        kwargs["journal_path"] = journal_path
    return cls(*args, **kwargs)


def _case_c1(ctx, tmp):
    import recoverland.restore_runner as rr
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    h = _h()
    path = _gpkg(tmp, "c1")
    layer = _open(path, "c1")
    event = _edit_event(layer, 3, "Z")._replace(event_id=41)
    missing = str(Path(tmp) / "absent" / "journal_du_projet.sqlite")
    runner = _new_runner(
        rr.StrictRestoreRunner, [event], lambda _e, lyr=layer: lyr,
        RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, inclusive=True),
        write_queue=_TraceSink(missing), tracker=None,
        trace_id="rw_rcju_c1", journal_path=missing)
    results = []
    runner.finished.connect(results.append)
    before = _etat(path, "c1")
    runner.start()
    h.pump_until(lambda: bool(results), 15.0)
    res = results[0] if results else None
    ctx.data["c1"] = {
        "finished": res is not None,
        "before": before,
        "report": dict(runner.apply_report()),
        "errors": list(getattr(res, "errors", []) or [])[:3],
    }
    del layer
    ctx.data["c1"]["after"] = _etat(path, "c1")


def _case_c2(ctx, tmp):
    """The real dialog path, a durable horizon, a write queue with no path."""
    import recoverland.recover_dialog as rd
    h = _h()
    path = _gpkg(tmp, "c2")
    layer = _open(path, "c2")
    journal = h.new_journal(tmp, "c2.sqlite")
    event = _journalled(journal, _edit_event(layer, 3, "Z"))
    _record_durable_horizon(journal, _DS, _HORIZON)
    wq = h.RecordingWriteQueue(journal)
    wq.journal_path = None   # writer never started: it knows no journal
    dlg = h.build_dialog(rd, h.Journal(journal), wq, [_DS])
    logs = []
    seams = h.Seams()
    try:
        seams.set(rd, "find_target_layer", lambda _evt, _c=None, lyr=layer: lyr)
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
        dlg._version_restore_cutoff = RestoreCutoff(
            CutoffType.BY_DATE, _CUTOFF, inclusive=True)
        dlg._active_restore_trace_id = "rw_rcju_c2"
        before = _etat(path, "c2")
        dlg._execute_version_restore([event])
        h.pump_until(lambda: dlg._restore_runner is None
                     and not dlg._restore_in_progress, 15.0)
        h.pump_until(lambda: False, 0.2)
    finally:
        seams.restore()
        h.close_dialog(dlg)
    del layer
    ctx.data["c2"] = {"before": before, "after": _etat(path, "c2"),
                      "logs": [m[:400] for m, _l in logs][-3:]}


def _case_c3(ctx, tmp):
    """Mode A: no cutoff, the coverage guard does not apply (not a defect)."""
    from recoverland.core.restore_planner import (
        plan_event_restore, preflight_check,
    )
    h = _h()
    path = _gpkg(tmp, "c3")
    layer = _open(path, "c3")
    journal = h.new_journal(tmp, "c3.sqlite")
    event = _journalled(journal, _edit_event(layer, 3, "Z"))
    # A purge through a date AFTER the event: a rewind there is refused.
    _record_durable_horizon(journal, _DS, "2026-09-01T09:30:00+00:00")
    plan = plan_event_restore([event], _DS, "c3")
    conn = sqlite3.connect(journal)
    try:
        report = preflight_check(plan, conn=conn)
    finally:
        conn.close()
    del layer
    ctx.data["c3"] = {"cutoff": plan.cutoff,
                      "blocking": list(report.blocking_reasons)}


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_rcju_")
    flog(f"rw_rewind_coverage_journal_unreadable setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    tmp = ctx.data["tmpdir"]
    ctx.data["errors"] = {}
    for name, case in (("c1", _case_c1), ("c2", _case_c2), ("c3", _case_c3)):
        try:
            case(ctx, tmp)
        except Exception as exc:  # noqa: BLE001 - a crash is a result
            import traceback
            ctx.data["errors"][name] = (f"{type(exc).__name__}: {exc} "
                                        f"{traceback.format_exc()[-900:]}")
    flog(f"rw_rewind_coverage_journal_unreadable run: trace_id={ctx.trace_id} "
         f"c1={ctx.data.get('c1')} c2={ctx.data.get('c2')} "
         f"c3={ctx.data.get('c3')} errors={ctx.data['errors']}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", not d.get("errors"), f"crash={d.get('errors')}")]
    c1 = d.get("c1") or {}
    bd = c1.get("report") or {}
    out.append((
        "c1_unreadable_journal_refuses_the_layer",
        c1.get("finished") is True and c1.get("after") == "Z"
        and bd.get("applied", 0) == 0 and bd.get("apply_fail") == 1,
        f"journal illisible : etat de l'entite avant={c1.get('before')!r} "
        f"apres={c1.get('after')!r} (attendu 'Z', rien ecrit) rapport={bd}. "
        f"Sans l'horizon de purge du journal, revenir au {_CUTOFF} peut "
        f"rejouer un historique purge et annoncer restaure un etat que la "
        f"couche n'a jamais eu.",
    ))
    errors = " ".join(c1.get("errors") or [])
    out.append((
        "c1_refusal_names_the_journal_and_the_risk",
        "journal ne peut pas etre lu" in errors and "horizon de purge" in errors,
        f"message={c1.get('errors')} : le refus doit dire pourquoi (journal "
        f"illisible, horizon de purge inverifiable), pas un echec muet.",
    ))
    c2 = d.get("c2") or {}
    shown = " ".join(c2.get("logs") or [])
    out.append((
        "c2_dialog_rewind_reads_the_durable_horizon",
        c2.get("after") == "Z" and "purged" in shown,
        f"rewind lance du dialogue, file d'ecriture sans chemin : etat avant="
        f"{c2.get('before')!r} apres={c2.get('after')!r} (attendu 'Z') "
        f"messages={c2.get('logs')}. Le dialogue doit donner au runner le "
        f"journal dont il a tire le plan : la purge enregistree jusqu'au "
        f"{_HORIZON} rend le {_CUTOFF} irreconstituable.",
    ))
    c3 = d.get("c3") or {}
    out.append((
        "c3_mode_a_has_no_cutoff_not_a_defect",
        c3.get("cutoff") is None and not any(
            "Retention coverage" in r for r in c3.get("blocking") or []),
        f"plan du mode A : coupure={c3.get('cutoff')} refus={c3.get('blocking')}. "
        f"La restauration d'un evenement remet son image d'avant, elle ne "
        f"reconstitue pas un etat a une date : une purge anterieure ne la "
        f"rend pas fausse (controle, vrai avant comme apres).",
    ))
    return out
