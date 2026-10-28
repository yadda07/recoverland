# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_rewind_mass_exec_bench - RecoverLand validation runtime.

Invariant: I-REWIND-01 sous charge : un rewind de masse s'execute en
entier, juste, et dans un temps annonce.

Le mecanisme
------------
Depuis le schema 7 le rewind ne fusionne plus une chaine d'editions en un
evenement synthetique : une chaine de N editions coute N compensations,
chacune avec sa trace exacte (15 editions par entite sur 500 entites :
7500 compensations au lieu de 500). Chaque compensation d'UPDATE prouve
sa cible, et pour une seule valeur capturee la preuve parcourt la couche
pour verifier l'unicite de la valeur : le cout croit avec le produit
(nombre de compensations x nombre d'entites). Aucun scenario ne mesurait
l'execution d'un tel plan, seulement le plan lui-meme.

Ce que le scenario execute
--------------------------
Un GeoPackage de N entites, un journal de K modifications d'un champ par
entite (valeurs uniques par entite, comme en vrai), le vrai rewind : la
fenetre du journal, le dedup, puis StrictRestoreRunner en un passage.
Mesure le temps de l'execution et verifie le resultat (toutes les
compensations appliquees, chaque entite revenue a sa valeur initiale).

Mesures de reference (poste de developpement, QGIS 4.2.1) : 100 x 15 =
1500 compensations en 3,6 s ; 500 x 15 = 7500 en 43 s. Le cout est
super-lineaire (balayage d'unicite par compensation, dans l'executeur).

Echec concret pour l'utilisateur
--------------------------------
Un rewind d'une journee de saisie sur une grosse couche fige la barre de
progression pendant des minutes, ou n'aboutit pas ; ce scenario le
detecte (budget depasse) au lieu de le laisser decouvrir sur le terrain.

Verdict attendu : PASS sous le budget ; c'est une garde de performance,
pas la preuve d'un defaut existant.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rw_rewind_mass_exec_bench"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_N = int(os.environ.get("RL_BENCH_N", "300"))
_K = int(os.environ.get("RL_BENCH_K", "15"))
_BUDGET_S = float(os.environ.get("RL_BENCH_BUDGET_S", "90"))


def setup(ctx):
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_bench_")


def _make_layer(tmp):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(12)"
                         "&field=val:integer", "m", "memory")
    mem.startEditing()
    for i in range(_N):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"B{i:04d}", i * 1000 + _K])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i, i)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmp, "bench.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    return QgsVectorLayer(f"{path}|layername=pts", "bench", "ogr")


def _journal(conn, lyr, fp):
    """K single-field UPDATEs per feature, as the capture writes them."""
    from recoverland.core.serialization import serialize_field_schema
    schema = serialize_field_schema(lyr.fields())
    fids = {f["code"]: f.id() for f in lyr.getFeatures()}
    rows = []
    for k in range(_K):
        stamp = f"2026-01-01T01:{k:02d}:00+00:00"
        for i in range(_N):
            fid = fids[f"B{i:04d}"]
            identity = json.dumps({"fid": fid, "pk_field": "fid", "pk_value": fid})
            delta = {"changed_only": {"val": {"old": i * 1000 + k,
                                              "new": i * 1000 + k + 1}}}
            rows.append(("proj", fp, lyr.id(), "bench", "ogr", identity,
                         "UPDATE", json.dumps(delta), None, "Point",
                         "EPSG:2154", schema, "u", f"s{k}", stamp, None,
                         f"pk:fid={fid}", 7, None, None))
    conn.executemany(
        "INSERT INTO audit_event (project_fingerprint, datasource_fingerprint, "
        "layer_id_snapshot, layer_name_snapshot, provider_type, "
        "feature_identity_json, operation_type, attributes_json, geometry_wkb, "
        "geometry_type, crs_authid, field_schema_json, user_name, session_id, "
        "created_at, restored_from_event_id, entity_fingerprint, "
        "event_schema_version, new_geometry_wkb, invalidated_at) VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()


def run(ctx):
    from qgis.core import QgsProject
    import recoverland.restore_runner as rr
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump
    tmp = ctx.data["tmp"]
    lyr = _make_layer(tmp)
    QgsProject.instance().addMapLayer(lyr)
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    wq = WriteQueue()
    wq.start(jm.path)
    out = {}
    try:
        fp = compute_datasource_fingerprint(lyr)
        conn = sqlite3.connect(jm.path)
        try:
            _journal(conn, lyr, fp)
            cutoff = RestoreCutoff(CutoffType.BY_DATE,
                                   "2026-01-01T00:30:00+00:00", inclusive=True)
            t0 = time.monotonic()
            active, _stats = collapse_rewind_events_with_stats(
                fetch_events_after_cutoff(conn, fp, cutoff,
                                          trace_id=ctx.trace_id))
            t1 = time.monotonic()
        finally:
            conn.close()
        done = {}
        runner = rr.StrictRestoreRunner(
            active, lambda ev: lyr, cutoff, write_queue=wq,
            trace_id=ctx.trace_id)
        runner.finished.connect(lambda res: done.setdefault("r", res))
        runner.start()
        deadline = time.monotonic() + max(_BUDGET_S * 4, 120)
        while "r" not in done and time.monotonic() < deadline:
            pump(0.05)
        t2 = time.monotonic()
        res = done.get("r")
        restored = sum(1 for f in lyr.getFeatures() if f["val"] % 1000 == 0)
        out = {"plan": len(active), "collapse_s": round(t1 - t0, 2),
               "exec_s": round(t2 - t1, 2), "finished": res is not None,
               "total_fail": getattr(res, "total_fail", None),
               "restored": restored}
    finally:
        wq.stop()
        jm.close()
        QgsProject.instance().removeMapLayer(lyr.id())
    ctx.data["out"] = out
    flog(f"rw_rewind_mass_exec_bench run: trace_id={ctx.trace_id} n={_N} "
         f"k={_K} out={out}")


def teardown(ctx):
    import shutil
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = []
    res.append((
        "every_compensation_of_the_chains_is_planned",
        out.get("plan") == _N * _K,
        f"plan={out.get('plan')} attendu={_N * _K} : sans fusion, chaque "
        f"edition a sa compensation et sa trace exacte.",
    ))
    res.append((
        "mass_rewind_lands_on_the_state_at_t",
        out.get("finished") and out.get("total_fail") == 0
        and out.get("restored") == _N,
        f"{out} : chaque entite doit revenir a sa valeur de depart, toutes "
        f"les compensations appliquees.",
    ))
    res.append((
        "mass_rewind_within_budget",
        out.get("exec_s") is not None and out["exec_s"] <= _BUDGET_S,
        f"execution={out.get('exec_s')} s pour {out.get('plan')} "
        f"compensations sur {_N} entites (budget {_BUDGET_S:.0f} s) : "
        f"au-dela, un rewind de masse fige l'interface pendant des minutes.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rw_rewind_mass_exec_bench.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=1))
    return res


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
