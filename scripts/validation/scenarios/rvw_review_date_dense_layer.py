# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_review_date_dense_layer - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat a une date) SANS figer QGIS,
sur une couche REELLE : les entites modifiees y sont melees a beaucoup
d'autres, que personne n'a touchees.

Le mecanisme
------------
Chaque geometrie journalisee sous un autre SCR que celui de la couche est
placee par le verdict de SCR du Rewind (geometry_utils.crs_verdict), qui
cherche dans le fournisseur les entites portant encore exactement une
lecture capturee (brute ou reprojetee, des deux cotes de l'evenement :
quatre lectures par geometrie). La recherche groupee
(geometry_utils.CaptureCarriers) lisait l'union de toutes ces lectures en
passes enchainees DANS UNE SEULE unite de travail du fil graphique, avec un
budget de 8 entites par lecture : quand les entites modifiees sont
dispersees parmi d'autres (le cas normal), l'union deborde, elle est coupee
en deux jusqu'a epuisement du budget (32 entites par geometrie, lues d'un
bloc), puis presque toutes les lectures retombent sur la requete par
lecture. Chaque date payait donc l'ancien cout PLUS une longue lecture
bloquante, et le repayait a la date suivante : rien de ce qui avait ete
prouve n'etait garde.

L'echec concret pour l'utilisateur
----------------------------------
Sa couche en degres a ete reecrite en Lambert-93 au meme chemin, puis 2000
de ses 40000 points ont ete modifies. Il deplace le curseur de date de
Review : QGIS ne repond plus pendant plus d'une demi-seconde d'un seul
tenant (la carte ne se redessine pas, les clics sont perdus), et chaque
nouvelle date redemande au fournisseur les memes preuves, pres de 4
requetes par entite modifiee (des minutes sur une base distante).

Ce que le scenario pilote
-------------------------
(A) Le vrai plugin, le vrai dialogue en mode Review, un vrai GeoPackage en
Lambert-93 de 40000 points (2000 modifies, 19 intacts autour de chacun,
dans la meme maille) et, dans le vrai journal, 2000 UPDATE captures en
EPSG:4326. L'activation de Review (sa premiere date lit les preuves des
2000 geometries pour la premiere fois), puis deux autres dates : un
battement QTimer de 5 ms mesure le plus long blocage de la boucle
d'evenements, et on compte les requetes au fournisseur de chaque etape.
(B) Le vrai SnapshotRebuildWorker et les vraies unites de
`_completion_steps`, sur une couche bien plus dense (600 modifies parmi
60000, 99 intacts autour de chacun) : entites lues par unite, entites lues
par la premiere date comparees a la requete par lecture seule (les memes
decisions, entite par entite), requetes de la deuxieme date, puis un
commit qui change la preuve d'une entite : la date suivante doit la relire
et refuser cette entite (preuves contradictoires), pas resservir l'ancienne.
Enfin un commit qui tombe entre deux passes d'un meme chargement (il s'etale
desormais sur plusieurs tours de la boucle d'evenements) : rien de ce que
les passes ont lu avant lui n'est resservi (un temoin sans memoire montre
que, sans cette regle, l'ancien etat le serait).
(C) Le parcours de (A) sur des polygones de 100 sommets, plus chers a lire :
une longue lecture d'un seul tenant s'y voit meme sur une machine au repos.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rvw_review_date_dense_layer"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_N = 2000
_EXTRA = 19
_MAX_STALL_MS = 400
# (B): the denser layer, read unit by unit.
_NB = 600
_EXTRA_B = 99
_FP_B = "ogr::rvw_dense_b"
_T0_B = datetime(2026, 3, 2, 8, 0, 0, tzinfo=timezone.utc)
# One unit may read what one bounded pass reads (about 2000 features),
# never an unbounded series of them.
_MAX_FEATURES_PER_UNIT = 2500
# What the batched lookup may read on top of the per-reading query when the
# edited features are spread among many others: a few bounded passes, not a
# budget that grows with the number of geometries (32 features each).
_MAX_EXTRA_FEATURES = 8000
_MOVED_B = 7


def _base():
    from scripts.validation.scenarios import rvw_review_date_gui_cost
    return rvw_review_date_gui_cost


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _to_l93():
    from qgis.core import (
        QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject,
    )
    return QgsCoordinateTransform(QgsCoordinateReferenceSystem("EPSG:4326"),
                                  QgsCoordinateReferenceSystem("EPSG:2154"),
                                  QgsProject.instance())


def _l93(tr, xy):
    from qgis.core import QgsPointXY
    p = tr.transform(QgsPointXY(*xy))
    return (p.x(), p.y())


def _deg_b(i):
    return (2.0 + (i % 30) * 0.01, 48.0 + (i // 30) * 0.01)


def _around(x, y, k):
    """An untouched point of the cell of (x, y): never an edited position."""
    return (x + 0.001 + 0.0004 * (k % 10), y + 0.001 + 0.0004 * (k // 10))


class _Reads:
    """Counts provider requests and, if asked, the features they return.

    Features are counted through a proxy of the iterator, only where every
    reader iterates it in Python (the completion units of (B)): a C++
    consumer of the iterator (the dialog, the canvas) would not take it.
    """

    def __init__(self, count_features: bool = False):
        from qgis.core import QgsVectorDataProvider
        self._cls = QgsVectorDataProvider
        self._raw_get = QgsVectorDataProvider.__dict__.get("getFeatures")
        self._orig_get = QgsVectorDataProvider.getFeatures
        self._count_features = count_features
        self.n_requests = 0
        self.n_features = 0

    def __enter__(self):
        reads = self

        class _Counted:
            def __init__(self, inner):
                self._inner = inner

            def __iter__(self):
                return self

            def __next__(self):
                feature = next(self._inner)
                reads.n_features += 1
                return feature

            def __getattr__(self, name):
                return getattr(self._inner, name)

        def get_features(provider, *args, **kwargs):
            reads.n_requests += 1
            it = reads._orig_get(provider, *args, **kwargs)
            return _Counted(it) if reads._count_features else it

        self._cls.getFeatures = get_features
        return self

    def __exit__(self, *exc):
        if self._raw_get is not None:
            setattr(self._cls, "getFeatures", self._raw_get)
        else:
            delattr(self._cls, "getFeatures")
        return False


# ------------------------------------------------------------ (A), (C) ---
# The same GUI run on two layers: points (A), and polygons of
# `_POLY_VERTICES` vertices (C), whose features cost more to read: a long
# read in one unit shows there even on an idle machine.

_POLY_VERTICES = 100
_POLY_RADIUS = 0.00015


def _ring():
    import math
    from qgis.core import QgsGeometry, QgsPointXY
    pts = [QgsPointXY(_POLY_RADIUS * math.cos(2 * math.pi * k / _POLY_VERTICES),
                      _POLY_RADIUS * math.sin(2 * math.pi * k / _POLY_VERTICES))
           for k in range(_POLY_VERTICES)]
    return QgsGeometry.fromPolygonXY([pts + [pts[0]]])


def _shape(kind, xy, ring=None):
    """The geometry of *kind* centred on *xy* (degrees)."""
    from qgis.core import QgsGeometry, QgsPointXY
    if kind == "pt":
        return QgsGeometry.fromPointXY(QgsPointXY(*xy))
    g = QgsGeometry(ring)
    g.translate(*xy)
    return g


def _gpkg_gui(tmpdir, kind) -> str:
    """2000 C features at their NEW position, 19 untouched U around each,
    in Lambert-93 (the file reprojected in place)."""
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsVectorFileWriter,
        QgsVectorLayer,
    )
    base, h = _base(), _h()
    tr = _to_l93()
    ring = _ring() if kind != "pt" else None
    wkb_type = "Point" if kind == "pt" else "Polygon"
    mem = QgsVectorLayer(f"{wkb_type}?crs=EPSG:2154&" + "&".join(
        f"field={n}:{t}" for n, t in h.FIELDS), "seed", "memory")
    feats = []
    for i in range(_N):
        x, y = base._deg(i)
        pts = [(f"C{i}", "new", float(i), (x + 0.0005, y + 0.0005))]
        pts += [(f"U{i}_{k}", "keep", 0.0, _around(x, y, k)) for k in range(_EXTRA)]
        for code, nom, surface, xy in pts:
            g = _shape(kind, xy, ring)
            g.transform(tr)
            f = QgsFeature(mem.fields())
            f.setAttributes([code, nom, surface])
            f.setGeometry(g)
            feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"dense_{kind}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _journal_gui(jpath, layer, kind) -> int:
    """The 2000 UPDATE rows of rvw_review_date_gui_cost (captured while the
    layer declared EPSG:4326), for the C features only."""
    from recoverland.core.identity import (
        compute_datasource_fingerprint, compute_entity_fingerprint,
        compute_feature_identity,
    )
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS
    base = _base()
    ring = _ring() if kind != "pt" else None
    fp = compute_datasource_fingerprint(layer)
    row0 = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    row0.update({"project_fingerprint": "p", "datasource_fingerprint": fp,
                 "layer_id_snapshot": layer.id(), "layer_name_snapshot": layer.name(),
                 "provider_type": "ogr",
                 "geometry_type": "Point" if kind == "pt" else "Polygon",
                 "user_name": "u", "event_schema_version": 7,
                 "crs_authid": "EPSG:4326"})
    rows = []
    t_edit = datetime.now(timezone.utc)
    for f in layer.getFeatures():
        code = str(f["code"])
        if not code.startswith("C"):
            continue
        i = int(code[1:])
        x, y = base._deg(i)
        ident = compute_feature_identity(layer, f)
        rows.append(dict(
            row0, feature_identity_json=ident,
            entity_fingerprint=compute_entity_fingerprint(ident),
            operation_type="UPDATE",
            created_at=(t_edit + timedelta(milliseconds=i)).strftime(
                "%Y-%m-%dT%H:%M:%S.%f+00:00"),
            attributes_json=json.dumps({"changed_only": {"nom": {"old": "old", "new": "new"}}}),
            geometry_wkb=bytes(_shape(kind, (x, y), ring).asWkb()),
            new_geometry_wkb=bytes(_shape(kind, (x + 0.0005, y + 0.0005), ring).asWkb())))
    cols = [c for c in AUDIT_EVENT_COLUMNS if c != "event_id"]
    conn = sqlite3.connect(jpath)
    try:
        with conn:
            conn.executemany(
                f"INSERT INTO audit_event ({', '.join(cols)}) VALUES "  # nosec B608
                f"({', '.join('?' for _ in cols)})",
                [tuple(r[c] for c in cols) for r in rows])
    finally:
        conn.close()
    return len(rows)


def _beat(fn):
    """(fn(), stalls): the longest stretch the event loop did not turn
    while *fn* pumped it (the 5 ms QTimer beat of _drive)."""
    from qgis.PyQt.QtCore import QTimer
    ticks = []
    beat = QTimer()
    beat.setInterval(5)
    beat.timeout.connect(lambda: ticks.append(time.monotonic()))
    beat.start()
    t0 = time.monotonic()
    try:
        value = fn()
    finally:
        t1 = time.monotonic()
        beat.stop()
    stamps = [t0] + ticks + [t1]
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    return value, {"total_ms": int((t1 - t0) * 1000),
                   "max_stall_ms": int(max(gaps) * 1000) if gaps else None}


def _drawn(dlg) -> dict:
    """How the C features of the date are drawn: count, misplaced (centre of
    the drawn geometry against its position at the date, in Lambert-93),
    unresolved."""
    base, h = _base(), _h()
    tr = _to_l93()
    drawn = {}
    for lyr in h.overlay_layers(dlg):
        for f in lyr.getFeatures():
            code = str(f["code"]) if "code" in f.fields().names() else ""
            if not code.startswith("C"):
                continue
            g = f.geometry()
            if g is None or g.isNull():
                drawn[code] = None
                continue
            box = g.boundingBox()
            drawn[code] = (box.center().x(), box.center().y())
    off = []
    for i in range(0, _N, 97):
        want = _l93(tr, base._deg(i))
        got = drawn.get(f"C{i}")
        if got is None or abs(got[0] - want[0]) > 1.0 or abs(got[1] - want[1]) > 1.0:
            off.append((i, got, want))
    return {"n_drawn": sum(1 for v in drawn.values() if v is not None),
            "off": off[:5],
            "unresolved": getattr(dlg._review_snap_raw_result, "n_unresolved", None)}


def _run_gui(ctx, kind, prefix) -> None:
    base = _base()
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d[f"gpkg_{kind}"], f"dense_{kind}")
        d[f"{prefix}_n_features"] = layer.featureCount()
        h.pump(0.2)
        dates = []
        for _ in range(2):
            time.sleep(1.1)
            dates.append(h.utc_now_iso())
        time.sleep(1.1)
        d[f"{prefix}_n_rows"] = _journal_gui(h.journal_path(plugin), layer, kind)
        h.set_view(iface, 600000.0, 6750000.0, 700000.0, 6850000.0)
        dlg = h.open_dialog(plugin, [layer])
        # Review on builds its first date: the first time the evidence of
        # these 2000 geometries is read. Measured like a date (the wait is
        # long: how long it takes is what the assertions judge).
        with _Reads() as reads:
            on, stall = _beat(lambda: h.review_on(dlg, 1, timeout=60.0))
        d[f"{prefix}_review_on"] = on
        d[f"{prefix}0"] = dict(stall, n_requests=reads.n_requests,
                               unresolved=getattr(dlg._review_snap_raw_result,
                                                  "n_unresolved", None))
        for name, iso in ((f"{prefix}1", dates[1]), (f"{prefix}2", dates[0])):
            with _Reads() as reads:
                run = base._drive(dlg, iso)
            run = {k: run.get(k) for k in ("shown", "total_ms", "max_stall_ms", "n_turns")}
            run["n_requests"] = reads.n_requests
            run.update(_drawn(dlg))
            d[name] = run
        h.review_off(dlg)
    finally:
        h.shutdown(plugin)


# ---------------------------------------------------------------- (B) ---

def _gpkg_b(tmpdir) -> str:
    from qgis.core import (
        QgsCoordinateTransformContext, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsVectorLayer,
    )
    tr = _to_l93()
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(12)&"
                         "field=nom:string(10)", "seed", "memory")
    feats = []
    for i in range(_NB):
        x, y = _deg_b(i)
        pts = [(f"C{i}", (x + 0.0005, y + 0.0005))]
        pts += [(f"U{i}_{k}", _around(x, y, k)) for k in range(_EXTRA_B)]
        for code, xy in pts:
            f = QgsFeature(mem.fields())
            f.setAttributes([code, "n"])
            f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*_l93(tr, xy))))
            feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, "dense_b.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _journal_b(path, layer) -> int:
    from qgis.core import QgsGeometry, QgsPointXY
    from recoverland.core.identity import (
        compute_entity_fingerprint, compute_feature_identity,
    )
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    row0 = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    row0.update({"project_fingerprint": "p", "datasource_fingerprint": _FP_B,
                 "layer_id_snapshot": "l", "layer_name_snapshot": "dense_b",
                 "provider_type": "ogr", "geometry_type": "Point",
                 "user_name": "u", "event_schema_version": 7,
                 "crs_authid": "EPSG:4326"})
    rows = []
    for f in layer.getFeatures():
        code = str(f["code"])
        if not code.startswith("C"):
            continue
        i = int(code[1:])
        x, y = _deg_b(i)
        ident = compute_feature_identity(layer, f)
        rows.append(dict(
            row0, feature_identity_json=ident,
            entity_fingerprint=compute_entity_fingerprint(ident),
            operation_type="UPDATE",
            created_at=(_T0_B + timedelta(seconds=10)).isoformat(),
            attributes_json=json.dumps({"changed_only": {"nom": {"old": "o", "new": "n"}}}),
            geometry_wkb=bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb()),
            new_geometry_wkb=bytes(QgsGeometry.fromPointXY(
                QgsPointXY(x + 0.0005, y + 0.0005)).asWkb())))
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
    return len(rows)


class _Journal:
    def __init__(self, path):
        self.path = path
        self.is_open = True

    def create_read_connection(self):
        return sqlite3.connect(self.path, check_same_thread=False)


def _worker_result(jpath, info):
    from recoverland.widgets.snapshot_rebuild_worker import SnapshotRebuildWorker
    cut = (_T0_B + timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S")
    got = []
    worker = SnapshotRebuildWorker(_Journal(jpath), [info], cut,
                                   trace_id=uuid.uuid4().hex[:8])
    worker.result_ready.connect(lambda _t, r: got.append(r))
    worker.error.connect(lambda _t, e: got.append(e))
    worker.run()
    if not got or isinstance(got[0], str):
        raise RuntimeError(f"snapshot worker: {got}")
    return got[0]


def _complete_b(jpath, info, per_reading: bool = False) -> dict:
    """One date of (B), unit by unit: what each unit reads, and the result."""
    from recoverland.core import geometry_utils
    from recoverland.core.geometry_utils import rebuild_geometry
    from recoverland.core.snapshot_overlay_session import _completion_steps
    result = _worker_result(jpath, info)
    trace = uuid.uuid4().hex[:8]
    carriers_cls = getattr(geometry_utils, "CaptureCarriers", None)
    serves = getattr(carriers_cls, "serves", None)
    if per_reading and serves is not None:
        # The per-reading query alone: no batch serves any layer.
        carriers_cls.serves = lambda self, layer: False
    units = []
    t0 = time.perf_counter()
    try:
        with _Reads(count_features=True) as reads:
            steps = _completion_steps(result, [info], trace, {"done": 0, "total": 0})
            while True:
                r0, f0, u0 = reads.n_requests, reads.n_features, time.perf_counter()
                try:
                    next(steps)
                except StopIteration as stop:
                    done = stop.value
                    units.append((reads.n_features - f0, reads.n_requests - r0,
                                  (time.perf_counter() - u0) * 1000.0))
                    break
                units.append((reads.n_features - f0, reads.n_requests - r0,
                              (time.perf_counter() - u0) * 1000.0))
    finally:
        if per_reading and serves is not None:
            carriers_cls.serves = serves
    ents = {}
    for key, feat in (done.features.get(_FP_B) or {}).items():
        g = rebuild_geometry(feat.geom_wkb) if feat.geom_wkb else None
        at = ([round(g.asPoint().x(), 3), round(g.asPoint().y(), 3)]
              if g is not None and not g.isNull() else None)
        ents[key] = {"at": at, "unresolved": feat.unresolved or ""}
    worst = max(units, key=lambda u: u[0]) if units else (0, 0, 0.0)
    return {"trace": trace, "n_units": len(units),
            "n_requests": reads.n_requests, "n_features": reads.n_features,
            "max_unit_features": worst[0], "max_unit_requests": worst[1],
            "max_unit_ms": round(max(u[2] for u in units), 1) if units else None,
            "total_ms": int((time.perf_counter() - t0) * 1000),
            "entities": ents}


def _straddle_b(layer, with_memo: bool) -> dict:
    """One load of the carriers of (B), run unit by unit; a real commit
    lands right after the pass that read the readings of C7, while other
    passes remain (a load now spans several event-loop turns). Answers
    for C7's readings are then compared with a fresh per-reading query.

    *with_memo*: the Review's own memo, wired to the layers' signals
    (snapshot_overlay_session._CARRIERS); without it, what a load across
    turns would serve if nothing told it the data changed (the hazard).
    Moves C7 between its raw numbers and its Lambert-93 position.
    """
    from qgis.core import QgsGeometry, QgsPointXY
    from recoverland.core import geometry_utils as gu
    h = _h()
    carriers_cls = getattr(gu, "CaptureCarriers", None)
    steps_of = getattr(carriers_cls, "load_steps", None)
    memo = None
    if with_memo:
        try:
            from recoverland.core.snapshot_overlay_session import _CARRIERS
        except ImportError:
            _CARRIERS = None
        memo = _CARRIERS.memo() if _CARRIERS is not None else None
    if steps_of is None or (with_memo and memo is None):
        return {"error": "no load run across units (CaptureCarriers.load_steps) "
                         "or no Review memo"}
    tr = _to_l93()
    x, y = _deg_b(_MOVED_B)
    fid = h.fid_of(layer, f"C{_MOVED_B}")
    now = layer.getFeature(fid).geometry().asPoint()
    raw_now = abs(now.x() - (x + 0.0005)) < 1e-9
    # Where the commit puts C7: the other of its two positions.
    to = (_l93(tr, (x + 0.0005, y + 0.0005)) if raw_now else (x + 0.0005, y + 0.0005))
    old, new = (_shape("pt", (x, y)), _shape("pt", (x + 0.0005, y + 0.0005)))
    transform = gu._write_transform("EPSG:4326", layer.crs())
    readings = [old, gu._reprojected(old, transform),
                new, gu._reprojected(new, transform)]
    watched = gu._finite_box(new)
    carriers = carriers_cls(layer, memo=memo) if with_memo else carriers_cls(layer)
    for i in range(_NB):
        xi, yi = _deg_b(i)
        carriers.add_capture(_shape("pt", (xi, yi)), "EPSG:4326",
                             [_shape("pt", (xi + 0.0005, yi + 0.0005))])
    committed = None
    n_units = 0
    for _unit in carriers.load_steps():
        n_units += 1
        if committed is None and watched in getattr(carriers, "_covered", ()):
            committed = {"at_unit": n_units, "ok": h.user_commit(
                layer, lambda lyr: lyr.changeGeometry(
                    fid, QgsGeometry.fromPointXY(QgsPointXY(*to))))}
    stale = []
    for reading in readings:
        got = carriers.carriers(reading)
        fresh = gu._committed_carriers(layer, reading)
        if sorted(got or []) != sorted(fresh or []):
            stale.append({"reading": reading.asWkt(3), "served": got, "fresh": fresh})
    return {"committed": committed, "n_units": n_units,
            "n_passes": carriers.n_passes, "stopped": getattr(carriers, "stopped", None),
            "stale": stale, "c7_was_raw": raw_now}


def _run_b(ctx) -> None:
    from qgis.core import QgsGeometry, QgsPointXY, QgsProject, QgsVectorLayer
    h = _h()
    d = ctx.data
    layer = None
    try:
        path = _gpkg_b(d["tmpdir"])
        layer = QgsVectorLayer(f"{path}|layername=pts", "dense_b", "ogr")
        if not layer.isValid():
            raise RuntimeError(f"invalid layer {path}")
        QgsProject.instance().addMapLayer(layer)
        d["b_n_features"] = layer.featureCount()
        jpath = os.path.join(d["tmpdir"], "journal_b.sqlite")
        d["b_n_rows"] = _journal_b(jpath, layer)
        info = {"fingerprint": _FP_B, "layer_name": layer.name(),
                "layer_id": layer.id(), "storage_crs": "EPSG:2154",
                "fid_stable": True}
        d["b_per_reading"] = _complete_b(jpath, info, per_reading=True)
        d["b1"] = _complete_b(jpath, info)
        d["b2"] = _complete_b(jpath, info)
        # C7 committed onto the raw numbers of its own new geometry: the
        # layer now also holds evidence of a relabel for that entity.
        x, y = _deg_b(_MOVED_B)
        d["b_commit"] = h.user_commit(layer, lambda lyr: lyr.changeGeometry(
            h.fid_of(lyr, f"C{_MOVED_B}"),
            QgsGeometry.fromPointXY(QgsPointXY(x + 0.0005, y + 0.0005))))
        moved_key = None
        for f in layer.getFeatures():
            if str(f["code"]) == f"C{_MOVED_B}":
                from recoverland.core.identity import (
                    compute_entity_fingerprint, compute_feature_identity,
                )
                moved_key = compute_entity_fingerprint(
                    compute_feature_identity(layer, f))
        d["b_moved_key"] = moved_key
        d["b3"] = _complete_b(jpath, info)
        d["b_straddle_control"] = _straddle_b(layer, with_memo=False)
        d["b_straddle"] = _straddle_b(layer, with_memo=True)
    finally:
        if layer is not None:
            QgsProject.instance().removeMapLayer(layer.id())


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_dense_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg_pt"] = _gpkg_gui(tmpdir, "pt")
    ctx.data["gpkg_poly"] = _gpkg_gui(tmpdir, "poly")
    flog(f"rvw_review_date_dense_layer setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    try:
        _run_gui(ctx, "pt", "a")
        _run_b(ctx)
        _run_gui(ctx, "poly", "c")
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    summary = {k: v for k, v in d.items() if k != "trace"}
    for key in ("b_per_reading", "b1", "b2", "b3"):
        if isinstance(summary.get(key), dict):
            summary[key] = {k: v for k, v in summary[key].items() if k != "entities"}
    flog(f"rvw_review_date_dense_layer run: trace_id={ctx.trace_id} "
         f"data={json.dumps(summary, default=str)[:4000]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _brief(run) -> dict:
    return {k: v for k, v in (run or {}).items() if k != "entities"}


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_absent, assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    for prefix, what in (("a", "points"), ("c", f"polygones de {_POLY_VERTICES} sommets")):
        r0, r1, r2 = (d.get(f"{prefix}0") or {}, d.get(f"{prefix}1") or {},
                      d.get(f"{prefix}2") or {})
        out.append((
            f"{prefix}_setup_review_on_2000_updates_among_40000_{'points' if prefix == 'a' else 'polygons'}",
            d.get(f"{prefix}_n_rows") == _N
            and d.get(f"{prefix}_n_features") == _N * (1 + _EXTRA)
            and d.get(f"{prefix}_review_on") is True,
            f"{what} : lignes={d.get(f'{prefix}_n_rows')} "
            f"entites={d.get(f'{prefix}_n_features')} "
            f"review_on={d.get(f'{prefix}_review_on')}{crash}",
        ))
        out.append((
            f"{prefix}_review_on_reads_the_evidence_without_holding_the_gui_thread",
            r0.get("max_stall_ms") is not None and r0["max_stall_ms"] <= _MAX_STALL_MS
            and r0.get("unresolved") == 0,
            f"{what}, Review active (premiere lecture des preuves des {_N} "
            f"geometries) : {r0} (attendu : plus long blocage <= "
            f"{_MAX_STALL_MS} ms, aucune entite non resolue ; au-dela, la carte "
            f"ne se redessine plus et les clics sont perdus pendant la "
            f"lecture){crash}",
        ))
        out.append((
            f"{prefix}_review_on_reads_the_evidence_in_passes",
            r0.get("n_requests") is not None and r0["n_requests"] <= _N // 2,
            f"{what} : requetes au fournisseur a l'activation de Review = "
            f"{r0.get('n_requests')} pour {_N} entites (attendu <= {_N // 2} : "
            f"des passes sur les entites de la zone ; ~4 par entite, c'est la "
            f"requete par lecture, des minutes sur une base distante){crash}",
        ))
        for name, run in (("first", r1), ("second", r2)):
            out.append((
                f"{prefix}_{name}_date_never_holds_the_gui_thread_in_one_piece",
                run.get("shown") is True and run.get("max_stall_ms") is not None
                and run["max_stall_ms"] <= _MAX_STALL_MS,
                f"{what} : {run} (attendu : affichee, plus long blocage <= "
                f"{_MAX_STALL_MS} ms ; au-dela, la carte ne se redessine plus et "
                f"les clics sont perdus pendant la lecture des preuves de "
                f"SCR){crash}",
            ))
            out.append((
                f"{prefix}_{name}_date_reads_nothing_already_proven",
                run.get("n_requests") is not None and run["n_requests"] <= _N // 10,
                f"{what} : requetes au fournisseur de la date = "
                f"{run.get('n_requests')} pour {_N} entites (attendu <= "
                f"{_N // 10} : les preuves lues a l'activation valent tant que "
                f"la couche n'a pas change ; seules les entites vivantes sont "
                f"relues, par lots){crash}",
            ))
        out.append((
            f"{prefix}_both_dates_drawn_reprojected_and_complete",
            all(r.get("n_drawn") == _N and not r.get("off") and r.get("unresolved") == 0
                for r in (r1, r2)),
            f"{what} : premiere={ {k: r1.get(k) for k in ('n_drawn', 'off', 'unresolved')} } "
            f"deuxieme={ {k: r2.get(k) for k in ('n_drawn', 'off', 'unresolved')} } "
            f"(attendu : les {_N} entites a leur position de cette date, "
            f"reprojetee en Lambert-93, aucune non resolue){crash}",
        ))
    per, b1, b2, b3 = (d.get("b_per_reading") or {}, d.get("b1") or {},
                       d.get("b2") or {}, d.get("b3") or {})
    out.append((
        "b_setup_600_updates_among_60000_points",
        d.get("b_n_rows") == _NB and d.get("b_n_features") == _NB * (1 + _EXTRA_B)
        and len(per.get("entities") or {}) == _NB,
        f"lignes={d.get('b_n_rows')} entites={d.get('b_n_features')} "
        f"reconstruites={len(per.get('entities') or {})}{crash}",
    ))
    ents_per, ents_b1 = per.get("entities") or {}, b1.get("entities") or {}
    diffs = [(k, ents_per.get(k), ents_b1.get(k)) for k in sorted(set(ents_per) | set(ents_b1))
             if ents_per.get(k) != ents_b1.get(k)]
    out.append((
        "b_batched_evidence_decides_like_the_per_reading_query",
        bool(ents_b1) and not diffs
        and not any(e.get("unresolved") for e in ents_b1.values()),
        f"ecarts avec la requete par lecture = {len(diffs)} {diffs[:3]} "
        f"(attendu 0, et aucune entite non resolue : la recherche groupee "
        f"lit les memes preuves, elle ne decide rien d'autre){crash}",
    ))
    worst = max((r.get("max_unit_features") or 0) for r in (b1, b2, b3)) if b1 else None
    out.append((
        "b_no_unit_reads_more_than_one_bounded_pass",
        worst is not None and worst <= _MAX_FEATURES_PER_UNIT,
        f"entites lues par la plus lourde unite = {worst} "
        f"(dates : {[_brief(r).get('max_unit_features') for r in (b1, b2, b3)]}, "
        f"ms : {[_brief(r).get('max_unit_ms') for r in (b1, b2, b3)]}) "
        f"(attendu <= {_MAX_FEATURES_PER_UNIT} : une passe bornee par unite ; "
        f"des passes enchainees dans une unite tiennent le fil graphique "
        f"d'autant, sur une base distante des secondes){crash}",
    ))
    extra = (b1.get("n_features", 0) - per.get("n_features", 0)) if b1 and per else None
    out.append((
        "b_dense_layer_stops_passing_early",
        extra is not None and extra <= _MAX_EXTRA_FEATURES,
        f"entites lues par la premiere date = {b1.get('n_features')} contre "
        f"{per.get('n_features')} par la requete par lecture, soit {extra} de "
        f"plus (attendu <= {_MAX_EXTRA_FEATURES} : une couche dense se voit "
        f"en quelques passes, puis chaque lecture est demandee seule ; un "
        f"budget de 8 entites par lecture, depense puis perdu, se repayait "
        f"a chaque date){crash}",
    ))
    out.append((
        "b_second_date_reads_nothing_already_proven",
        bool(b2) and b2.get("n_requests", 10 ** 9) <= _NB // 10
        and b2.get("entities") == ents_b1,
        f"deuxieme date : {_brief(b2)} (attendu <= {_NB // 10} requetes, les "
        f"memes decisions){crash}",
    ))
    moved = (b3.get("entities") or {}).get(d.get("b_moved_key") or "?") or {}
    others = {k: v for k, v in (b3.get("entities") or {}).items()
              if k != d.get("b_moved_key")}
    same_others = {k: v for k, v in others.items() if ents_b1.get(k) == v}
    out.append((
        "b_a_commit_forgets_what_was_proven",
        d.get("b_commit") is True and moved.get("at") is None
        and "crs_ambiguous" in (moved.get("unresolved") or "")
        and len(same_others) == _NB - 1,
        f"apres le commit de C{_MOVED_B} sur ses nombres bruts : C{_MOVED_B}="
        f"{moved or 'ABSENTE'} autres inchangees={len(same_others)}/{_NB - 1} "
        f"requetes={b3.get('n_requests')} (deuxieme date {b2.get('n_requests')}) "
        f"(attendu : preuves relues, C{_MOVED_B} refusee crs_ambiguous, ses "
        f"preuves se contredisent desormais ; resservie d'avant le commit, "
        f"elle serait dessinee comme si de rien n'etait){crash}",
    ))
    ctl, real = d.get("b_straddle_control") or {}, d.get("b_straddle") or {}
    out.append((
        "b_a_commit_between_two_passes_voids_what_they_read",
        not real.get("error") and (real.get("committed") or {}).get("ok") is True
        and not real.get("stale")
        and (ctl.get("committed") or {}).get("ok") is True and bool(ctl.get("stale")),
        f"commit entre deux passes d'un meme chargement : avec la memoire de "
        f"Review {real} ; temoin sans rien pour signaler le commit {ctl} "
        f"(attendu : avec la memoire, aucune reponse servie d'avant le commit ; "
        f"le temoin montre qu'une passe lue avant lui resservirait l'ancien "
        f"etat, maintenant qu'un chargement s'etale sur plusieurs tours de la "
        f"boucle d'evenements){crash}",
    ))
    out.append(assert_log_absent(
        ctx.records, r"Traceback|RuntimeError: wrapped C/C\+\+ object",
        name="no_exception_escapes_the_review_completion"))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_review_date_dense_layer.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
