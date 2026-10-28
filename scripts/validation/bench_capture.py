# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Benchmark: what does RecoverLand cost on a user's commit, end to end?

Real QGIS stack: a GeoPackage layer, the real EditSessionTracker, the
real WriteQueue and journal. For each commit size and each kind of edit,
measures on the GUI thread:

  commit_off_ms   layer.commitChanges() with RecoverLand not connected
  commit_on_ms    the same commit with capture connected: what the user
                  waits for when clicking "Save" (pre-image capture,
                  event generation and, in v7, the durable outbox append)
  indexed_ms      from the end of the commit until every event is a row
                  of the journal (searchable, rewindable)
  events          journal rows produced by the commit

Kinds: attr (attribute UPDATE), geom (geometry UPDATE), insert, delete.

Usage (QGIS python)::

    set QT_QPA_PLATFORM=offscreen
    python-qgis.bat scripts/validation/bench_capture.py [--sizes 1,10,100] [--kinds attr,geom]

Prints a table and writes ``reports/bench_capture_<ts>.json``. Runs on
any checkout: it only uses the public constructor, activate,
connect_layer and the journal file.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PLUGIN_ROOT = _HERE.parents[1]
for _extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
    if _extra not in sys.path:
        sys.path.insert(0, _extra)

KINDS = ("attr", "geom", "insert", "delete")
SIZES = (1, 10, 100, 1000, 10000, 100000)


def _square(x, y, n=20):
    from qgis.core import QgsGeometry, QgsPointXY
    pts = [QgsPointXY(x + (i % 5) * 0.2, y + (i // 5) * 0.2) for i in range(n)]
    return QgsGeometry.fromPolygonXY([pts + [pts[0]]]).convexHull()


def _make_gpkg(tmpdir, n):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsVectorFileWriter,
                           QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Polygon?crs=EPSG:2154&field=code:string(12)"
                         "&field=nom:string(40)&field=surface:double", "m", "memory")
    feats = []
    for i in range(n):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"P{i}", f"parcelle {i}", 1000.0 + i])
        f.setGeometry(_square(700000.0 + i * 3, 6600000.0))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"b{n}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _layer(path):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=parcelles", "parcelles", "ogr")
    assert lyr.isValid(), path
    return lyr


def _edit(lyr, kind, n):
    from qgis.core import QgsFeature
    lyr.startEditing()
    if kind == "attr":
        idx = lyr.fields().indexFromName("nom")
        for f in lyr.getFeatures():
            lyr.changeAttributeValue(f.id(), idx, f"{f['nom']} bis")
    elif kind == "geom":
        for f in lyr.getFeatures():
            g = f.geometry()
            g.translate(0.5, 0.25)
            lyr.changeGeometry(f.id(), g)
    elif kind == "insert":
        feats = []
        for i in range(n):
            f = QgsFeature(lyr.fields())
            f.setAttributes([None, f"N{i}", f"nouvelle {i}", 5.0 + i])
            f.setGeometry(_square(800000.0 + i * 3, 6700000.0))
            feats.append(f)
        lyr.addFeatures(feats)
    elif kind == "delete":
        lyr.deleteFeatures([f.id() for f in lyr.getFeatures()])


def _journal_rows(path):
    if not path or not os.path.exists(path):
        return 0
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def _pump(seconds):
    from qgis.PyQt.QtCore import QCoreApplication
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        QCoreApplication.processEvents()
        time.sleep(0.005)


def _one(tmp, n, kind, capture):
    from qgis.core import QgsProject
    path = _make_gpkg(tmp, n)
    lyr = _layer(path)
    QgsProject.instance().addMapLayer(lyr)
    stack = None
    if capture:
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker
        project = os.path.join(tmp, f"p{n}_{kind}.qgz")
        open(project, "w").close()
        jm = JournalManager()
        jm.open_for_project(project, tmp)
        wq = WriteQueue()
        wq.start(jm.path)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(lyr)
        stack = (jm, wq, tracker)
    try:
        before = _journal_rows(stack[0].path) if stack else 0
        _edit(lyr, kind, n)
        t0 = time.perf_counter()
        ok = bool(lyr.commitChanges())
        commit_ms = (time.perf_counter() - t0) * 1000.0
        out = {"ok": ok, "commit_ms": commit_ms}
        if stack:
            t1 = time.perf_counter()
            expected = n
            deadline = time.monotonic() + max(30.0, n / 200.0)
            rows = before
            while time.monotonic() < deadline:
                _pump(0.01)
                rows = _journal_rows(stack[0].path)
                if rows - before >= expected:
                    break
            out["indexed_ms"] = (time.perf_counter() - t1) * 1000.0
            out["events"] = rows - before
        return out
    finally:
        if stack:
            jm, wq, tracker = stack
            tracker.deactivate()
            tracker.disconnect_all()
            wq.stop()
            jm.close()
        QgsProject.instance().removeMapLayer(lyr.id())
        _pump(0.05)


def main(argv) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default=",".join(str(s) for s in SIZES))
    ap.add_argument("--kinds", default=",".join(KINDS))
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--label", default="")
    args = ap.parse_args(argv)
    sizes = [int(s) for s in args.sizes.split(",") if s]
    kinds = [k for k in args.kinds.split(",") if k]

    from qgis.core import QgsApplication
    app = QgsApplication([], False)
    app.initQgis()
    rows = []
    try:
        for n in sizes:
            reps = args.repeat if n <= 1000 else 1
            for kind in kinds:
                offs, ons, idx, evs, oks = [], [], [], [], []
                for _ in range(reps):
                    tmp = tempfile.mkdtemp(prefix="rl_bench_cap_")
                    try:
                        off = _one(tmp, n, kind, capture=False)
                        on = _one(tmp, n, kind, capture=True)
                    finally:
                        shutil.rmtree(tmp, ignore_errors=True)
                    offs.append(off["commit_ms"])
                    ons.append(on["commit_ms"])
                    idx.append(on.get("indexed_ms", 0.0))
                    evs.append(on.get("events", 0))
                    oks.append(off["ok"] and on["ok"])
                row = {"n": n, "kind": kind, "reps": reps,
                       "commit_off_ms": statistics.median(offs),
                       "commit_on_ms": statistics.median(ons),
                       "indexed_ms": statistics.median(idx),
                       "events": min(evs), "commits_ok": all(oks)}
                row["overhead_ms"] = row["commit_on_ms"] - row["commit_off_ms"]
                row["overhead_per_feature_us"] = row["overhead_ms"] * 1000.0 / max(n, 1)
                rows.append(row)
                print(f"n={n:>6} {kind:<6} off={row['commit_off_ms']:>9.1f} ms "
                      f"on={row['commit_on_ms']:>9.1f} ms "
                      f"overhead={row['overhead_ms']:>9.1f} ms "
                      f"({row['overhead_per_feature_us']:>7.1f} us/feat) "
                      f"indexed_after={row['indexed_ms']:>9.1f} ms "
                      f"events={row['events']} ok={row['commits_ok']}", flush=True)
    finally:
        app.exitQgis()
    reports = _HERE / "reports"
    reports.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = reports / f"bench_capture_{args.label or 'run'}_{stamp}.json"
    out.write_text(json.dumps({"root": str(_PLUGIN_ROOT), "rows": rows}, indent=1),
                   encoding="utf-8")
    print(f"report={out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
