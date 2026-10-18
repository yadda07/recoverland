"""rvw_crs_verdict_batch - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (une date de Review se construit sans figer QGIS) et
I-3 (le meme verdict de SCR que le Rewind, sur les memes preuves ; un
affichage n'ecrit jamais d'histoire pour les ecritures).

Le mecanisme
------------
Review place chaque geometrie journalisee sous un autre SCR que celui de
la couche par le verdict du Rewind (geometry_utils.crs_verdict). Pour
chaque geometrie, ce verdict cherche dans le FOURNISSEUR les entites qui
portent encore exactement une lecture capturee (brute ou reprojetee, de
la geometrie et de l'autre cote de son evenement) : jusqu'a quatre
requetes par entite, meme une fois l'histoire de la couche etablie (sa
preuve propre doit encore s'accorder avec l'histoire), une ligne WARNING
crs_decision par entite, et tout recommence a la date suivante : la vue
des histoires etait jetee avec la date, et l'affichage l'installait en
REMPLACANT le module global geometry_utils._CRS_STORIES autour de chaque
appel.

L'echec concret pour l'utilisateur
----------------------------------
Sa couche Lambert-93 avait ete declaree CC46 pendant 300 editions, puis
corrigee. Chaque date de Review demande 1200 requetes au fournisseur (des
minutes sur une base distante) et ecrit 300 lignes de journal ; puis, a
une date ou seule une entite sans preuve propre a change, Review la
refuse ("SCR non departage") alors qu'il vient d'etablir l'histoire de
cette couche a la date precedente.

Ce que le scenario pilote : un vrai GeoPackage (fournisseur OGR), un vrai
journal, le vrai SnapshotRebuildWorker et la vraie complete_from_live,
avec deux dates : D1 (300 UPDATE captures sous EPSG:3946, la couche porte
leurs NOUVELLES coordonnees brutes : un changement d'etiquette prouve par
temoin) et D2 (une seule entite, X, inseree puis supprimee, sans preuve
propre). On compte les requetes au fournisseur et les lignes crs_decision
de D1, on verifie qu'aucun appel du verdict ne voit un autre objet que la
table des histoires des ecritures, que D2 suit l'histoire etablie en D1,
qu'un changement de SCR ou de source de la couche l'oublie, que les
decisions restent celles de la verite terrain et, geometrie par
geometrie, que la recherche groupee des porteurs rend les memes preuves
que la requete par lecture (y compris un conflit, une lecture non
enregistree et une passe qui depasse son budget).

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

SCENARIO_ID = "rvw_crs_verdict_batch"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_N = 300
_SRC = "EPSG:3946"
_FP = "ogr::rvw_crs_batch"
_T0 = datetime(2026, 3, 2, 8, 0, 0, tzinfo=timezone.utc)
_Q = (655000.0, 6865000.0)       # X, raw (Lambert-93 numbers under CC46)
_PROBE = (660000.0, 6870000.0)   # carried both raw and reprojected


def _old(i):
    return (650000.0 + (i % 20) * 100.0, 6860000.0 + (i // 20) * 100.0)


def _new(i):
    x, y = _old(i)
    return (x + 7.0, y + 7.0)


def _iso(seconds) -> str:
    return (_T0 + timedelta(seconds=seconds)).isoformat()


def _cut(seconds) -> str:
    return (_T0 + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S")


def _pt(xy):
    from qgis.core import QgsGeometry, QgsPointXY
    return QgsGeometry.fromPointXY(QgsPointXY(*xy))


def _wkb(xy) -> bytes:
    return bytes(_pt(xy).asWkb())


def _transform():
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject,
    )
    return QgsCoordinateTransform(QgsCoordinateReferenceSystem(_SRC),
                                  QgsCoordinateReferenceSystem("EPSG:2154"),
                                  QgsProject.instance())


def _gpkg(tmpdir, name):
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsVectorFileWriter,
        QgsVectorLayer,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)&"
                         "field=nom:string(10)", "seed", "memory")
    feats = []
    # Reprojected the way the verdict reprojects a reading (a geometry
    # through the transform), so the probe carries it to the bit.
    probe = _pt(_PROBE)
    probe.transform(_transform())
    rows = [(f"W{i}", _pt(_new(i))) for i in range(_N)]
    rows += [("PRAW", _pt(_PROBE)), ("PREP", probe)]
    for code, geom in rows:
        f = QgsFeature(mem.fields())
        f.setAttributes([code, "n"])
        f.setGeometry(geom)
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _journal(path, layer) -> None:
    from recoverland.core.identity import (
        compute_entity_fingerprint, compute_feature_identity,
    )
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    base = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    base.update({"project_fingerprint": "p", "datasource_fingerprint": _FP,
                 "layer_id_snapshot": "l", "layer_name_snapshot": "batch",
                 "provider_type": "ogr", "geometry_type": "Point",
                 "user_name": "u", "event_schema_version": 7,
                 "crs_authid": _SRC})
    rows = []
    for f in layer.getFeatures():
        code = str(f["code"])
        if not code.startswith("W"):
            continue
        i = int(code[1:])
        ident = compute_feature_identity(layer, f)
        rows.append(dict(
            base, feature_identity_json=ident,
            entity_fingerprint=compute_entity_fingerprint(ident),
            operation_type="UPDATE", created_at=_iso(10),
            attributes_json=json.dumps({"changed_only": {"nom": {"old": "o", "new": "n"}}}),
            geometry_wkb=_wkb(_old(i)), new_geometry_wkb=_wkb(_new(i))))
    x_ident = json.dumps({"fid": 9999})
    snap = json.dumps({"all_attributes": {"code": "X", "nom": "x"}})
    for op, at in (("INSERT", 20), ("DELETE", 40)):
        rows.append(dict(
            base, feature_identity_json=x_ident, entity_fingerprint="fid:9999",
            operation_type=op, created_at=_iso(at), attributes_json=snap,
            geometry_wkb=_wkb(_Q)))
    cols = [c for c in AUDIT_EVENT_COLUMNS if c != "event_id"]
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
        ensure_v7_schema(conn)
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


class _Probe:
    """Counts provider reads and watches the story table during a completion."""

    def __init__(self):
        from qgis.core import QgsVectorDataProvider
        from recoverland.core import geometry_utils
        self.gu = geometry_utils
        self.table = geometry_utils._CRS_STORIES
        self.n_reads = 0
        self.n_verdicts = 0
        self.foreign_tables = 0
        self._cls = QgsVectorDataProvider
        # The raw class attribute (a descriptor that binds), put back as is:
        # the unbound method read through the class would no longer bind.
        self._raw_get = QgsVectorDataProvider.__dict__.get("getFeatures")
        self._orig_get = QgsVectorDataProvider.getFeatures
        self._orig_verdict = geometry_utils.crs_verdict

    def __enter__(self):
        probe = self

        def get_features(provider, *args, **kwargs):
            probe.n_reads += 1
            return probe._orig_get(provider, *args, **kwargs)

        def verdict(*args, **kwargs):
            probe.n_verdicts += 1
            if probe.gu._CRS_STORIES is not probe.table:
                probe.foreign_tables += 1
            return probe._orig_verdict(*args, **kwargs)

        self._cls.getFeatures = get_features
        self.gu.crs_verdict = verdict
        return self

    def __exit__(self, *exc):
        if self._raw_get is not None:
            setattr(self._cls, "getFeatures", self._raw_get)
        else:
            delattr(self._cls, "getFeatures")
        self.gu.crs_verdict = self._orig_verdict
        return False


def _review(journal_path, layer, iso, probe=False) -> dict:
    from recoverland.core.snapshot_overlay_session import complete_from_live
    from recoverland.widgets.snapshot_rebuild_worker import SnapshotRebuildWorker
    info = {"fingerprint": _FP, "layer_name": layer.name(), "layer_id": layer.id(),
            "storage_crs": layer.crs().authid(), "fid_stable": True}
    trace = uuid.uuid4().hex[:8]
    got = []
    worker = SnapshotRebuildWorker(_Journal(journal_path), [info], iso,
                                   trace_id=trace)
    worker.result_ready.connect(lambda _t, r: got.append(r))
    worker.error.connect(lambda _t, e: got.append(e))
    worker.run()
    if not got or isinstance(got[0], str):
        return {"error": str(got)}
    out = {"trace": trace}
    if probe:
        with _Probe() as p:
            done = complete_from_live(got[0], [info], trace)
        out.update(n_reads=p.n_reads, n_verdicts=p.n_verdicts,
                   foreign_tables=p.foreign_tables)
    else:
        done = complete_from_live(got[0], [info], trace)
    from recoverland.core.geometry_utils import rebuild_geometry
    ents = {}
    for key, feat in (done.features.get(_FP) or {}).items():
        g = rebuild_geometry(feat.geom_wkb) if feat.geom_wkb else None
        at = [g.asPoint().x(), g.asPoint().y()] if g is not None and not g.isNull() else None
        ents[key] = {"at": at, "unresolved": feat.unresolved,
                     "attrs": feat.attrs_json}
    out["entities"] = ents
    return out


def _equivalence(layer) -> dict:
    """The batched lookup against the per-reading query, reading by reading."""
    from recoverland.core import geometry_utils as gu
    carriers_cls = getattr(gu, "CaptureCarriers", None)
    if carriers_cls is None:
        return {"error": "geometry_utils.CaptureCarriers absent"}
    tr = gu._write_transform(_SRC, layer.crs())
    cases = [[_pt(_old(i)), _pt(_new(i))] for i in range(0, _N, 7)]
    cases.append([_pt(_PROBE)])                       # both readings carried
    unregistered = [_pt((123456.0, 6543210.0))]        # never registered
    carriers = carriers_cls(layer)
    for witnesses in cases:
        carriers.add_capture(witnesses[0], _SRC, witnesses[1:])
    carriers.load()
    diffs = []
    for witnesses in cases + [unregistered]:
        a, b = {}, {}
        got_batch = gu._witness_verdict(layer, witnesses, tr, a, carriers)
        got_query = gu._witness_verdict(layer, witnesses, tr, b)
        if got_batch != got_query or a != b:
            diffs.append([got_batch, got_query, a, b])
    conflict = gu._witness_verdict(layer, [_pt(_PROBE)], tr, {}, carriers)
    # Two readings at opposite corners: their union holds every feature of
    # the layer, more than a pass of two readings may read.
    dense = carriers_cls(layer)
    corners = [_pt(_new(0)), _pt(_new(_N - 1))]
    for g in corners:
        dense.add_capture(g, _SRC)
    dense.load()
    dense_diffs = []
    for g in corners:
        a, b = {}, {}
        if (gu._witness_verdict(layer, [g], tr, a, dense)
                != gu._witness_verdict(layer, [g], tr, b) or a != b):
            dense_diffs.append(g.asWkt())
    return {"n_cases": len(cases) + 1, "diffs": diffs[:3], "n_diffs": len(diffs),
            "conflict": list(conflict), "passes": carriers.n_passes,
            "direct": carriers.n_direct, "dense_passes": dense.n_passes,
            "dense_direct": dense.n_direct, "dense_diffs": dense_diffs}


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_batch_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"rvw_crs_verdict_batch setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def run(ctx):
    from qgis.core import QgsCoordinateReferenceSystem, QgsProject, QgsVectorLayer
    from recoverland.core import geometry_utils
    from recoverland.core.logger import flog
    d = ctx.data
    layer = None
    try:
        tmp = d["tmpdir"]
        path = _gpkg(tmp, "batch")
        layer = QgsVectorLayer(f"{path}|layername=pts", "batch", "ogr")
        if not layer.isValid():
            raise RuntimeError(f"invalid layer {path}")
        QgsProject.instance().addMapLayer(layer)
        jpath = os.path.join(tmp, "journal.sqlite")
        _journal(jpath, layer)
        before = dict(geometry_utils._CRS_STORIES)
        d["d1"] = _review(jpath, layer, _cut(5), probe=True)
        d["d2"] = _review(jpath, layer, _cut(30))
        # The layer is given another CRS, then its own back: what the
        # Review settled under the old pair is not trusted any more.
        layer.setCrs(QgsCoordinateReferenceSystem("EPSG:3857"))
        d["d2_other_crs"] = _review(jpath, layer, _cut(30))
        layer.setCrs(QgsCoordinateReferenceSystem("EPSG:2154"))
        d["d2_crs_back"] = _review(jpath, layer, _cut(30))
        # Settled again, then a new source behind the layer.
        d["d1_again"] = _review(jpath, layer, _cut(5))
        d["d2_again"] = _review(jpath, layer, _cut(30))
        copy = os.path.join(tmp, "batch_copy.gpkg")
        shutil.copyfile(path, copy)
        layer.setDataSource(f"{copy}|layername=pts", "batch", "ogr")
        d["d2_new_source"] = _review(jpath, layer, _cut(30))
        d["writes_table_changed"] = dict(geometry_utils._CRS_STORIES) != before
        d["equivalence"] = _equivalence(layer)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if layer is not None:
            QgsProject.instance().removeMapLayer(layer.id())
    summary = {k: v for k, v in d.items() if k != "trace"}
    for key in ("d1", "d1_again"):
        if isinstance(summary.get(key), dict):
            summary[key] = {k: v for k, v in summary[key].items() if k != "entities"}
    flog(f"rvw_crs_verdict_batch run: trace_id={ctx.trace_id} "
         f"data={json.dumps(summary, default=str)[:4000]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _x(review) -> dict:
    return ((review or {}).get("entities") or {}).get("fid:9999") or {}


def _near(a, b, tol=1e-6) -> bool:
    return (a is not None and b is not None
            and abs(a[0] - b[0]) <= tol and abs(a[1] - b[1]) <= tol)


def assertions(ctx):
    import re
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    d1 = d.get("d1") or {}
    ents = d1.get("entities") or {}
    olds = {tuple(round(v, 3) for v in _old(i)) for i in range(_N)}
    drawn = {tuple(round(v, 3) for v in e["at"]) for e in ents.values() if e.get("at")}
    out.append((
        "d1_every_entity_placed_by_its_witness_raw",
        len(ents) == _N and drawn == olds
        and not any(e.get("unresolved") for e in ents.values()),
        f"D1 : {len(ents)} entites, {len(drawn & olds)}/{_N} a leur position "
        f"brute d'avant l'edition, non resolues="
        f"{sum(1 for e in ents.values() if e.get('unresolved'))} (attendu : "
        f"toutes, brutes : la couche porte leurs nouvelles coordonnees "
        f"brutes){crash}",
    ))
    n_reads = d1.get("n_reads")
    out.append((
        "d1_reads_the_provider_a_few_times_not_per_geometry",
        n_reads is not None and n_reads <= 10,
        f"requetes au fournisseur pendant la completion de D1 = {n_reads} pour "
        f"{_N} geometries (attendu <= 10 : une passe pour les porteurs, le "
        f"reste pour les entites vivantes ; jusqu'a 4 par geometrie, a chaque "
        f"date, figeaient QGIS, des minutes sur une base distante){crash}",
    ))
    trace = d1.get("trace") or "?"
    n_lines = sum(1 for r in ctx.records
                  if re.search(rf"crs_decision .*trace_id={trace}",
                               getattr(r, "raw", str(r))))
    out.append((
        "d1_logs_the_decision_once_per_story",
        1 <= n_lines <= 2,
        f"lignes crs_decision de D1 = {n_lines} pour {_N} geometries d'une "
        f"meme histoire (attendu 1 ou 2 : une par histoire, decision et "
        f"preuve ; une ligne WARNING par geometrie noyait le journal){crash}",
    ))
    out.append((
        "the_verdict_never_sees_a_swapped_story_table",
        d1.get("n_verdicts", 0) >= _N and d1.get("foreign_tables") == 0,
        f"appels du verdict={d1.get('n_verdicts')} dont vus avec un autre "
        f"objet que la table des ecritures={d1.get('foreign_tables')} (attendu "
        f"0 : l'affichage passe sa vue en parametre, il ne remplace plus le "
        f"module global autour de chaque appel){crash}",
    ))
    x2 = _x(d.get("d2"))
    out.append((
        "d2_follows_the_story_the_review_settled_at_d1",
        _near(x2.get("at"), _Q, 1e-3) and not x2.get("unresolved"),
        f"D2 : X={x2 or 'ABSENTE'} (attendu dessinee brute a {_Q} : "
        f"l'histoire de la couche, etablie par temoins a D1, vaut pour la "
        f"couche, pas pour une date ; refusee, l'utilisateur lisait 'SCR non "
        f"departage' sur une couche que Review venait de departager){crash}",
    ))
    refused = {}
    for key in ("d2_other_crs", "d2_crs_back", "d2_new_source"):
        x = _x(d.get(key))
        refused[key] = x.get("unresolved") or x.get("at")
    out.append((
        "a_new_crs_or_source_on_the_layer_forgets_the_story",
        all("crs_ambiguous" in str(v) for v in refused.values())
        and _near(_x(d.get("d2_again")).get("at"), _Q, 1e-3),
        f"X apres un autre SCR / le SCR remis / une nouvelle source = "
        f"{refused}, re-etablie entre-temps : "
        f"{_x(d.get('d2_again'))} (attendu : refusee crs_ambiguous dans les "
        f"trois cas, l'histoire est a re-prouver){crash}",
    ))
    out.append((
        "the_review_records_no_story_for_the_writes",
        d.get("writes_table_changed") is False,
        f"table des histoires des ecritures modifiee={d.get('writes_table_changed')} "
        f"(attendu False : ce que Review a vu ne decide pas d'une ecriture){crash}",
    ))
    eq = d.get("equivalence") or {}
    out.append((
        "the_batched_carriers_are_the_same_evidence_reading_by_reading",
        not eq.get("error") and eq.get("n_diffs") == 0 and not eq.get("dense_diffs")
        and (eq.get("conflict") or [None])[0] == "conflict"
        and eq.get("passes", 0) >= 1 and eq.get("direct", 0) >= 1
        and eq.get("dense_direct", 0) >= 2,
        f"{eq} (attendu : aucun ecart entre la recherche groupee et la "
        f"requete par lecture, conflit compris ; une lecture non enregistree "
        f"et une passe au-dela de son budget retombent sur la requete){crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_crs_verdict_batch.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
