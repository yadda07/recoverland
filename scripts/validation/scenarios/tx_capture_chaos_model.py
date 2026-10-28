# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_chaos_model - RecoverLand validation runtime.

Invariant: I-CAPTURE-01 (toute modification ecrite par le provider est au
journal, avec le bon etat d'avant) verifie contre un modele pur.

Le principe
-----------
Des sequences aleatoires mais reproductibles (graine journalisee)
d'editions passent par la VRAIE capture (EditSessionTracker + WriteQueue
+ journal SQLite) sur un vrai GeoPackage portant une contrainte
UNIQUE(code) :
  - modifications d'attributs, de geometries (y compris vers et depuis
    une geometrie NULLE), suppressions, ajouts, plusieurs par commit ;
  - ajouts en doublon de code : le commit echoue APRES que les etapes
    geometries / attributs / suppressions ont ete ecrites (commit
    partiel), puis l'utilisateur abandonne (rollBack) ou corrige et
    revalide ;
  - sessions abandonnees sans commit.

Le journal est ensuite rejoue par un modele Python independant du
produit (lecture brute des lignes, aucun helper de restauration) :
  M1  chaque etat d'avant du journal (old d'un UPDATE, image d'un DELETE)
      est l'etat que le modele connait a cet instant : le journal ne ment
      pas sur ce qu'il y avait ;
  M2  l'etat obtenu en rejouant le journal depuis l'etat initial est
      EXACTEMENT l'etat final du fichier : aucune ecriture n'a echappe a
      la capture, aucune n'a ete inventee.

Verdict attendu avant correctif : FAIL (les etapes ecrites d'un commit
partiel abandonne disparaissaient du journal).
"""
from __future__ import annotations

import json
import os
import random
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_chaos_model"
INVARIANT = "I-CAPTURE-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_SEEDS = (20260927, 7, 1234)
_ROUNDS = 22
_FIELDS = ("code", "nom", "val")


def _wkt(wkb):
    if wkb is None or wkb == b"":
        return None
    from qgis.core import QgsGeometry
    g = QgsGeometry()
    g.fromWkb(bytes(wkb))
    if g.isNull():
        return "UNDECODABLE"
    return g.asWkt(4)


def _gpkg(tmpdir, name):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)"
                         "&field=nom:string(30)&field=val:integer", "m", "memory")
    mem.startEditing()
    for i in range(12):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"K{i:03d}", f"nom{i}", i])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(float(i), float(i))))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    conn = sqlite3.connect(path)
    conn.execute("CREATE UNIQUE INDEX ux_pts_code ON pts(code)")
    conn.commit()
    conn.close()
    return path


def _file_state(path):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    out = {}
    for f in lyr.getFeatures():
        g = f.geometry()
        geom = None if (g is None or g.isNull()) else g.asWkt(4)
        out[int(f.id())] = ({k: _norm(f[k]) for k in _FIELDS}, geom)
    return out


def _norm(v):
    try:
        from qgis.PyQt.QtCore import QVariant
        if isinstance(v, QVariant) and v.isNull():
            return None
    except ImportError:
        pass
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return int(v) if float(v).is_integer() else float(v)
    s = str(v)
    return None if s == "NULL" else s


def _journal_events(journal):
    conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT event_id, operation_type, feature_identity_json, attributes_json, "
            "geometry_wkb, new_geometry_wkb FROM audit_event "
            "WHERE restored_from_event_id IS NULL ORDER BY event_id").fetchall()
    finally:
        conn.close()
    return rows


def _fid_of_identity(identity_json):
    try:
        ident = json.loads(identity_json or "{}")
    except ValueError:
        return None
    fid = ident.get("fid")
    try:
        return int(fid)
    except (TypeError, ValueError):
        return None


def _replay(initial, events):
    """Pure model: apply the journal to the initial state, checking every
    before-image on the way. Returns (state, problems)."""
    state = {fid: (dict(a), g) for fid, (a, g) in initial.items()}
    problems = []
    for eid, op, ident, attrs_json, gwkb, ngwkb in events:
        fid = _fid_of_identity(ident)
        try:
            data = json.loads(attrs_json or "{}")
        except ValueError:
            problems.append(f"e{eid}: attributes_json illisible")
            continue
        if op == "INSERT":
            attrs = data.get("all_attributes") or {}
            if fid in state:
                problems.append(f"e{eid}: INSERT fid={fid} deja present")
            state[fid] = ({k: _norm(attrs.get(k)) for k in _FIELDS}, _wkt(gwkb))
        elif op == "DELETE":
            attrs = data.get("all_attributes") or {}
            cur = state.get(fid)
            if cur is None:
                problems.append(f"e{eid}: DELETE fid={fid} absent du modele")
                continue
            before = {k: _norm(attrs.get(k)) for k in _FIELDS}
            if before != cur[0] or _wkt(gwkb) != cur[1]:
                problems.append(f"e{eid}: DELETE fid={fid} image d'avant {before} "
                                f"{_wkt(gwkb)} != modele {cur}")
            state.pop(fid, None)
        elif op == "UPDATE":
            cur = state.get(fid)
            if cur is None:
                problems.append(f"e{eid}: UPDATE fid={fid} absent du modele")
                continue
            attrs, geom = dict(cur[0]), cur[1]
            for k, change in (data.get("changed_only") or {}).items():
                if k not in _FIELDS or not isinstance(change, dict):
                    continue
                if _norm(change.get("old")) != attrs.get(k):
                    problems.append(f"e{eid}: UPDATE fid={fid} {k} old="
                                    f"{change.get('old')!r} != modele {attrs.get(k)!r}")
                attrs[k] = _norm(change.get("new"))
            if ngwkb is not None:
                if _wkt(gwkb) != geom:
                    problems.append(f"e{eid}: UPDATE fid={fid} geometrie d'avant "
                                    f"{_wkt(gwkb)} != modele {geom}")
                geom = _wkt(ngwkb)
            state[fid] = (attrs, geom)
        else:
            problems.append(f"e{eid}: operation {op}")
    return state, problems


def _one_seed(seed, tmp, ctx):
    from qgis.core import (QgsProject, QgsVectorLayer, QgsFeature, QgsGeometry,
                           QgsPointXY)
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.edit_tracker import EditSessionTracker
    from scripts.validation.plugin_harness import pump

    rng = random.Random(seed)
    # One folder per seed: the journal belongs to the project FOLDER, so
    # two projects side by side share one (by design).
    tmp = os.path.join(tmp, f"seed_{seed}")
    os.makedirs(tmp, exist_ok=True)
    path = _gpkg(tmp, f"s{seed}")
    lyr = QgsVectorLayer(f"{path}|layername=pts", f"chaos{seed}", "ogr")
    QgsProject.instance().addMapLayer(lyr)
    project = os.path.join(tmp, f"p{seed}.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()
    tracker.connect_layer(lyr)
    stats = {"commits": 0, "partial": 0, "partial_abandoned": 0, "partial_fixed": 0,
             "abandoned_sessions": 0, "ops": 0}
    initial = _file_state(path)
    counter = [100]
    try:
        fields = lyr.fields()
        i_code, i_nom, i_val = (fields.indexFromName(k) for k in _FIELDS)
        for rnd in range(_ROUNDS):
            fids = sorted(f.id() for f in lyr.getFeatures())
            codes = [f["code"] for f in lyr.getFeatures()]
            lyr.startEditing()
            touched = set()
            dup_added = []
            for _ in range(rng.randint(1, 6)):
                live = [f for f in fids if f not in touched]
                kind = rng.choice(("attr", "attr", "geom", "geom_null", "delete",
                                   "insert", "insert_dup"))
                stats["ops"] += 1
                if kind in ("attr", "geom", "geom_null", "delete") and not live:
                    kind = "insert"
                if kind == "attr":
                    fid = rng.choice(live)
                    if rng.random() < 0.5:
                        lyr.changeAttributeValue(fid, i_nom, f"n{seed}_{rnd}_{rng.randint(0, 999)}")
                    else:
                        lyr.changeAttributeValue(fid, i_val, rng.randint(-50, 5000))
                elif kind == "geom":
                    fid = rng.choice(live)
                    lyr.changeGeometry(fid, QgsGeometry.fromPointXY(
                        QgsPointXY(rng.uniform(0, 100), rng.uniform(0, 100))))
                elif kind == "geom_null":
                    fid = rng.choice(live)
                    lyr.changeGeometry(fid, QgsGeometry())
                elif kind == "delete":
                    fid = rng.choice(live)
                    lyr.deleteFeature(fid)
                    touched.add(fid)
                elif kind in ("insert", "insert_dup"):
                    f = QgsFeature(lyr.fields())
                    if kind == "insert_dup" and codes:
                        code = rng.choice(codes)
                    else:
                        counter[0] += 1
                        code = f"N{seed % 1000:03d}{counter[0]}"
                    attrs = [None] * lyr.fields().count()
                    attrs[i_code] = code
                    attrs[i_nom] = f"ins{rnd}"
                    attrs[i_val] = rng.randint(0, 99)
                    f.setAttributes(attrs)
                    if rng.random() < 0.85:
                        f.setGeometry(QgsGeometry.fromPointXY(
                            QgsPointXY(rng.uniform(0, 100), rng.uniform(0, 100))))
                    lyr.addFeature(f)
                    if kind == "insert_dup":
                        dup_added.append(code)
            if rng.random() < 0.1:
                lyr.rollBack()
                stats["abandoned_sessions"] += 1
                pump(0.02)
                continue
            ok = bool(lyr.commitChanges())
            stats["commits"] += 1
            if not ok:
                stats["partial"] += 1
                if rng.random() < 0.5:
                    lyr.rollBack()
                    stats["partial_abandoned"] += 1
                else:
                    # The user fixes the duplicates and validates again.
                    for f in list(lyr.getFeatures()):
                        if f.id() < 0 and f["code"] in dup_added:
                            counter[0] += 1
                            lyr.changeAttributeValue(
                                f.id(), i_code, f"F{seed % 1000:03d}{counter[0]}")
                    if lyr.commitChanges():
                        stats["partial_fixed"] += 1
                    else:
                        lyr.rollBack()
                        stats["partial_abandoned"] += 1
            pump(0.02)
        pump(0.3)
        if hasattr(wq, "wait_until_drained"):
            wq.wait_until_drained(10.0)
        else:
            time.sleep(2.0)
        final = _file_state(path)
        events = _journal_events(jm.path)
    finally:
        try:
            if lyr.isEditable():
                lyr.rollBack()
        except Exception:  # noqa: BLE001
            pass
        tracker.deactivate()
        tracker.disconnect_all()
        wq.stop()
        jm.close()
        QgsProject.instance().removeMapLayer(lyr.id())
    replayed, problems = _replay(initial, events)
    diff = []
    for fid in sorted(set(final) | set(replayed)):
        if final.get(fid) != replayed.get(fid):
            diff.append({"fid": fid, "fichier": final.get(fid), "journal": replayed.get(fid)})
    return {"seed": seed, "stats": stats, "n_events": len(events),
            "before_image_problems": problems[:12], "n_before_image_problems": len(problems),
            "state_diff": diff[:12], "n_state_diff": len(diff)}


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_chaos_")
    flog(f"tx_capture_chaos_model setup: trace_id={ctx.trace_id} seeds={_SEEDS}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    d["runs"] = []
    for seed in _SEEDS:
        try:
            d["runs"].append(_one_seed(seed, d["tmpdir"], ctx))
        except Exception as exc:  # noqa: BLE001
            import traceback
            d["runs"].append({"seed": seed, "crash": f"{type(exc).__name__}: {exc}",
                              "trace": traceback.format_exc()[-1000:]})
    flog(f"tx_capture_chaos_model run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d['runs'], default=str)[:4000]}", "INFO")
    shutil.rmtree(d["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    runs = ctx.data.get("runs") or []
    out = []
    out.append((
        "chaos_exercised_partial_commits",
        len(runs) == len(_SEEDS) and all("crash" not in r for r in runs)
        and sum(r.get("stats", {}).get("partial", 0) for r in runs) >= 3
        and sum(r.get("stats", {}).get("partial_abandoned", 0) for r in runs) >= 1
        and sum(r.get("stats", {}).get("partial_fixed", 0) for r in runs) >= 1,
        f"graines={[r.get('seed') for r in runs]} stats={[r.get('stats') for r in runs]} "
        f"crash={[r.get('crash') for r in runs if 'crash' in r]} "
        f"{[r.get('trace', '')[-500:] for r in runs if 'trace' in r]}",
    ))
    out.append((
        "m1_every_before_image_is_true",
        runs and all(r.get("n_before_image_problems") == 0 for r in runs),
        "; ".join(f"graine {r.get('seed')}: {r.get('n_before_image_problems')} "
                  f"ecart(s) {r.get('before_image_problems')}" for r in runs),
    ))
    out.append((
        "m2_journal_replay_equals_the_file",
        runs and all(r.get("n_state_diff") == 0 for r in runs),
        "; ".join(f"graine {r.get('seed')}: {r.get('n_state_diff')} entite(s) "
                  f"differente(s) {r.get('state_diff')}" for r in runs)
        + " : une ecriture du provider absente du journal ne pourra jamais "
          "etre defaite",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_capture_chaos_model.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
