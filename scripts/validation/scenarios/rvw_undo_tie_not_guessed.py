# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_undo_tie_not_guessed - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat a la date T, ou le dit
inconnu) et I-UNDO-ORDER (une Annulation se place comme le Rewind la
place), sur un journal ecrit par la version publiee.

Le mecanisme
------------
Une Annulation (Undo) n'ecrit aucun evenement : elle estampille
``invalidated_at`` sur les traces qu'elle a defaites. Avant la v7, elle
posait UN SEUL instant sur toutes ses traces, et sa requete
(``restored_from_event_id IN (...)``) estampillait aussi la trace d'une
restauration ANTERIEURE du meme evenement, qu'elle n'avait pas defaite.

Le moteur de Review (temporal_snapshot_engine._timeline_at_cutoff) :
  * rangeait les Annulations d'un meme instant par identifiant de trace
    croissant, c'est-a-dire dans l'ordre ou le Rewind defait les avait
    ECRITES, l'inverse de l'ordre de l'Annulation (le Rewind, lui, les
    place par rewind_dedup.undo_position : la trace ecrite en dernier est
    defaite en premier) ;
  * lisait deux traces du MEME evenement estampillees du meme instant (une
    egalite que le Rewind refuse en la nommant,
    event_stream_repository._tied_undo_groups) comme deux compensations
    toutes deux defaites : apres cet instant, il rejouait les seules
    editions de l'utilisateur, un etat que la couche n'a jamais eu. Quelle
    trace est restee en vigueur, et donc ce que cette Annulation a laisse,
    n'est pas dans le journal.

L'echec concret pour l'utilisateur
----------------------------------
(C1) Il renomme v a->b (100 s), le restaure (Mode A, 200 s : a), retape
x (300 s), restaure de nouveau (400 s : a), puis annule la derniere
restauration avec la version publiee (600 s : elle re-ecrit b et
estampille les DEUX traces), et tape z (800 s). Review a 700 s affiche
v = x ; la couche valait b. (C2) Meme histoire sur une entite supprimee
depuis hors de RecoverLand : Review affiche v = y comme connu, alors que
rien ne dit ce que l'Annulation a laisse. (C3) Un Rewind publie compense
e2 puis e1 (meme instant), son Annulation les defait d'un seul instant,
et e1 a ete purgee depuis : Review a 400 s affiche v = b comme connu ; la
couche valait a.

Ce que le scenario pilote : un vrai GeoPackage (fournisseur OGR, fid
stable), un journal forme ligne a ligne comme la version publiee
l'ecrivait (traces de schema 6, Annulation a instant unique), le vrai
SnapshotRebuildWorker lance en synchrone et la vraie complete_from_live
sur la couche. Verite physique par date :
  C1 : a -100- b -200- a -300- x -400- a -600- b -800- z
  C3 : a -100- b -200- c -300- a -500- c
Temoins (vrais avant comme apres correctif) : C1 a 150, 250, 350 et 500 s,
dates ou toutes les traces de l'egalite ont ete ecrites et aucune encore
defaite (le Rewind refuse ces dates, lui, car il doit DEFAIRE l'egalite
pour y revenir).

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rvw_undo_tie_not_guessed"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_T0 = datetime(2026, 3, 1, 8, 0, 0, tzinfo=timezone.utc)
_FP = "ogr::rvw_undo_tie"
# C1 physical truth: (instant in s, value from then on).
_C1_TRUTH = ((100, "b"), (200, "a"), (300, "x"), (400, "a"), (600, "b"),
             (800, "z"))


def _iso(seconds) -> str:
    return (_T0 + timedelta(seconds=seconds)).isoformat()


def _cut(seconds) -> str:
    """A Review date (naive UTC, as the date bar emits it)."""
    return (_T0 + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S")


def _c1_truth(seconds) -> str:
    value = "a"
    for at, v in _C1_TRUTH:
        if at <= seconds:
            value = v
    return value


def _layer(tmpdir):
    """GeoPackage with C1 (fid 1) and C3 (fid 3); C2 (fid 2) deleted since."""
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsGeometry, QgsPointXY,
        QgsProject, QgsVectorFileWriter, QgsVectorLayer,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&"
                         "field=v:string(10)&field=w:string(10)", "seed", "memory")
    feats = []
    for code, v, w, x in (("C1", "z", "p", 1.0), ("C2", "y", "q", 2.0),
                          ("C3", "c", "p", 3.0)):
        f = QgsFeature(mem.fields())
        f.setAttributes([code, v, w])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(650000.0 + x, 6860000.0)))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, "tie.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(f"{path}|layername=pts", "tie", "ogr")
    if not lyr.isValid():
        raise RuntimeError(f"invalid layer {path}")
    idents = {}
    from recoverland.core.identity import compute_feature_identity
    for f in lyr.getFeatures():
        idents[f["code"]] = compute_feature_identity(lyr, f)
    c2 = next(f.id() for f in lyr.getFeatures() if f["code"] == "C2")
    if not lyr.dataProvider().deleteFeatures([c2]):
        raise RuntimeError("could not delete C2")
    QgsProject.instance().addMapLayer(lyr)
    return lyr, idents


def _rows(idents) -> list:
    """The journal the published version wrote (schema 6, one Undo instant)."""
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS

    def row(eid, code, at, attrs, src=None, invalidated=None):
        r = dict.fromkeys(AUDIT_EVENT_COLUMNS)
        r.update({
            "event_id": eid, "project_fingerprint": "p",
            "datasource_fingerprint": _FP, "layer_id_snapshot": "l",
            "layer_name_snapshot": "tie", "provider_type": "ogr",
            "feature_identity_json": idents[code],
            "entity_fingerprint": compute_entity_fingerprint(idents[code]),
            "operation_type": "UPDATE", "geometry_type": "Point",
            "crs_authid": "EPSG:2154", "user_name": "u",
            "created_at": _iso(at), "event_schema_version": 6,
            "attributes_json": json.dumps(attrs),
            "restored_from_event_id": src, "invalidated_at": invalidated})
        return r

    def upd(field, old, new):
        return {"changed_only": {field: {"old": old, "new": new}}}

    undo = _iso(600)
    return [
        # C1: restore, re-type, restore again, published Undo, edit after it.
        row(11, "C1", 100, upd("v", "a", "b")),
        row(12, "C1", 200, {"_restore_ref": 11}, src=11, invalidated=undo),
        row(13, "C1", 300, upd("v", "a", "x")),
        row(14, "C1", 400, {"_restore_ref": 11}, src=11, invalidated=undo),
        row(15, "C1", 800, upd("v", "b", "z")),
        # C2: the same, then deleted outside RecoverLand after 800 s.
        row(21, "C2", 100, upd("v", "a", "b")),
        row(22, "C2", 200, {"_restore_ref": 21}, src=21, invalidated=undo),
        row(23, "C2", 300, upd("v", "a", "y")),
        row(24, "C2", 400, {"_restore_ref": 21}, src=21, invalidated=undo),
        row(25, "C2", 800, upd("w", "p", "q")),
        # C3: e1 (31, a->b at 100 s) purged by retention; e2 b->c; a
        # published Rewind to 50 s compensates e2 then e1 (one instant), its
        # Undo stamps both with one instant.
        row(32, "C3", 200, upd("v", "b", "c")),
        row(33, "C3", 300, {"_restore_ref": 32}, src=32, invalidated=_iso(500)),
        row(34, "C3", 300, {"_restore_ref": 31}, src=31, invalidated=_iso(500)),
    ]


def _write_journal(path, rows) -> None:
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
        ensure_v7_schema(conn)
        cols = list(AUDIT_EVENT_COLUMNS)
        with conn:
            conn.executemany(
                f"INSERT INTO audit_event ({', '.join(cols)}) VALUES "  # nosec B608
                f"({', '.join('?' for _ in cols)})",
                [tuple(r[c] for c in cols) for r in rows])
    finally:
        conn.close()


class _Journal:
    def __init__(self, path):
        self.path = path
        self.is_open = True

    def create_read_connection(self):
        return sqlite3.connect(self.path, check_same_thread=False)


def _review(journal_path, layer, iso, idents) -> dict:
    """A Review date: the real worker, then the real live completion."""
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.snapshot_overlay_session import complete_from_live
    from recoverland.widgets.snapshot_rebuild_worker import SnapshotRebuildWorker
    info = {"fingerprint": _FP, "layer_name": layer.name(), "layer_id": layer.id(),
            "storage_crs": layer.crs().authid(), "fid_stable": True}
    got = []
    worker = SnapshotRebuildWorker(_Journal(journal_path), [info], iso,
                                   trace_id=uuid.uuid4().hex[:8])
    worker.result_ready.connect(lambda _t, r: got.append(r))
    worker.error.connect(lambda _t, e: got.append(e))
    worker.run()
    if not got or isinstance(got[0], str):
        return {"error": str(got)}
    done = complete_from_live(got[0], [info], worker.trace_id)
    out = {"n_unresolved": done.n_unresolved, "entities": {}}
    codes = {compute_entity_fingerprint(ident): code
             for code, ident in idents.items()}
    for key, feat in (done.features.get(_FP) or {}).items():
        attrs = json.loads(feat.attrs_json) if feat.attrs_json else {}
        code = codes.get(key, key)
        out["entities"][code] = {"key": key, "v": attrs.get("v", "<absent>"),
                                 "w": attrs.get("w", "<absent>"),
                                 "unresolved": feat.unresolved}
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_tie_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"rvw_undo_tie_not_guessed setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    d = ctx.data
    layer = None
    try:
        layer, idents = _layer(d["tmpdir"])
        path = os.path.join(d["tmpdir"], "journal.sqlite")
        _write_journal(path, _rows(idents))
        d["live"] = {f["code"]: f["v"] for f in layer.getFeatures()}
        d["reviews"] = {}
        for seconds in (150, 250, 350, 500, 700):
            d["reviews"][str(seconds)] = _review(path, layer, _cut(seconds),
                                                 idents)
        d["reviews"]["c3_400"] = _review(path, layer, _cut(400), idents)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if layer is not None:
            QgsProject.instance().removeMapLayer(layer.id())
    flog(f"rvw_undo_tie_not_guessed run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _entity(d, date, code) -> dict:
    return (((d.get("reviews") or {}).get(date) or {}).get("entities") or {}).get(code) or {}


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    out.append((
        "setup_live_layer_after_the_published_undo",
        d.get("live") == {"C1": "z", "C3": "c"},
        f"couche={d.get('live')} (attendu C1=z, C3=c, C2 supprimee hors "
        f"RecoverLand){crash}",
    ))
    c1 = _entity(d, "700", "C1")
    out.append((
        "c1_after_the_tie_shows_what_the_layer_held_not_the_replay_of_user_edits",
        c1.get("v") == _c1_truth(700) and not c1.get("unresolved"),
        f"Review a 700 s : C1={c1 or 'ABSENTE'} (attendu v={_c1_truth(700)!r}, "
        f"lu dans le cote OLD de l'edition de 800 s : l'Annulation publiee a "
        f"estampille les deux restaurations, et rejouer les seules editions "
        f"de l'utilisateur donnait 'x', une valeur que la couche n'avait "
        f"plus){crash}",
    ))
    c2 = _entity(d, "700", "C2")
    n700 = ((d.get("reviews") or {}).get("700") or {}).get("n_unresolved")
    out.append((
        "c2_after_the_tie_with_no_later_source_is_flagged_not_guessed",
        c2.get("v") == "<absent>" and bool(c2.get("unresolved"))
        and (n700 or 0) >= 1,
        f"Review a 700 s : C2={c2 or 'ABSENTE'} n_unresolved={n700} (attendu "
        f"v absente de l'etat, entite signalee et comptee : ce que "
        f"l'Annulation a laisse n'est ni dans le journal ni dans la couche ; "
        f"afficher 'y' presentait une supposition comme un fait){crash}",
    ))
    wrong = []
    for seconds in (150, 250, 350, 500):
        got = _entity(d, str(seconds), "C1")
        if got.get("v") != _c1_truth(seconds) or got.get("unresolved"):
            wrong.append((seconds, got, _c1_truth(seconds)))
    out.append((
        "c1_before_the_tie_is_the_forward_replay",
        not wrong and all(str(s) in (d.get("reviews") or {}) for s in (150, 250, 350, 500)),
        f"ecarts avant l'Annulation={wrong} (attendu : a chaque date avant "
        f"600 s, toutes les traces de l'egalite ecrites et aucune encore "
        f"defaite, l'etat rejoue EST celui de la couche, sans signalement)"
        f"{crash}",
    ))
    c3 = _entity(d, "c3_400", "C3")
    out.append((
        "c3_one_run_undo_is_placed_like_the_rewind_places_it",
        c3.get("v") == "<absent>" and "trace_source_missing" in (c3.get("unresolved") or ""),
        f"Review a 400 s : C3={c3 or 'ABSENTE'} (attendu v absente et "
        f"trace_source_missing : l'Annulation de 500 s a defait la trace 34 "
        f"(ecrite en dernier, source purgee) AVANT la 33 ; rangee apres, la "
        f"33 donnait v='b' comme connu, la couche valait 'a'){crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, r"review_snapshot: undo_tied n_entities=2 degraded=True",
        name="the_tie_is_named_in_the_log", min_count=1))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_undo_tie_not_guessed.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
