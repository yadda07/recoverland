# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_capture_lifecycle - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (tant que QGIS est ouvert avec des couches suivies,
RecoverLand capture, ou dit clairement qu'il ne capture pas) et I-OBS-01
(l'etat affiche vient des faits d'execution, jamais d'un reglage).

Ce que le scenario pilote : le VRAI RecoverPlugin (recover.py), avec un
faux iface qui enregistre ce que l'utilisateur verrait (barre de messages,
indicateur de la barre d'etat).

Les defauts vises (audit 2026-09-27)
------------------------------------
N1  Fichier > Nouveau projet : le signal `cleared` arretait le backend et
    rien ne le relancait (`readProject` n'arrive pas pour un projet vierge).
    Toutes les editions du nouveau projet etaient perdues pour le journal,
    sous un point gris "aucun projet".
N2  Mort du thread ecrivain en cours de session : rien ne la detectait,
    rien ne l'affichait, rien ne relancait l'ecriture.
N3  Commit partiel (la source refuse une etape) : aucune alerte visible.
N4  L'indicateur lisait un reglage QgsSettings : vert "actif" alors que la
    capture etait morte.
N5  unload() doit ne laisser aucun thread ecrivain derriere lui.

Verdict attendu avant correctif : FAIL (N1, N2, N3, N4).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_plugin_capture_lifecycle"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(tmpdir: str, name: str, unique: bool = False) -> str:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i, (code, nom) in enumerate([("C001", "PONT"), ("C002", "GARE")]):
        f = QgsFeature(mem.fields())
        f.setAttributes([code, nom])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if unique:
        con = sqlite3.connect(path)
        con.execute("CREATE UNIQUE INDEX ux_pts_code ON pts(code)")
        con.commit()
        con.close()
    return path


def _layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not lyr.isValid():
        raise RuntimeError(path)
    return lyr


def _rename(layer, value: str) -> bool:
    fid = sorted(f.id() for f in layer.getFeatures())[0]
    idx = layer.fields().indexFromName("nom")
    layer.startEditing()
    layer.changeAttributeValue(fid, idx, value)
    return bool(layer.commitChanges())


def _rows_with(journal: str, value: str) -> int:
    if not journal or not os.path.isfile(journal):
        return -1
    try:
        conn = sqlite3.connect(f"file:{Path(journal).as_posix()}?mode=ro", uri=True)
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                (f"%{value}%",)).fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _wait_rows(journal: str, value: str, n: int = 1, timeout: float = 6.0) -> int:
    from scripts.validation.plugin_harness import pump
    deadline = time.monotonic() + timeout
    got = _rows_with(journal, value)
    while got < n and time.monotonic() < deadline:
        pump(0.1)
        got = _rows_with(journal, value)
    return got


def setup(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsProject
    QgsProject.instance().clear()
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_plugin_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"tx_plugin_capture_lifecycle setup: trace_id={ctx.trace_id} tmpdir={tmpdir}",
         "INFO")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump, writer_threads

    d = ctx.data
    tmp = d["tmpdir"]
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    d["writers_before"] = writer_threads()
    try:
        plugin.initGui()
        pump(0.3)
        # ---- P1: blank project at startup -------------------------------
        j0 = plugin._journal.path if plugin._journal.is_open else None
        l1 = _layer(_make_gpkg(tmp, "p1"), "p1")
        QgsProject.instance().addMapLayer(l1)
        d["p1_commit"] = _rename(l1, "P1_EDIT")
        d["p1_rows"] = _wait_rows(j0, "P1_EDIT")
        d["p1_journal"] = j0

        # ---- N1: File > New ---------------------------------------------
        QgsProject.instance().clear()
        pump(0.5)
        j1 = plugin._journal.path if plugin._journal.is_open else None
        d["n1_journal_open"] = bool(j1)
        l2 = _layer(_make_gpkg(tmp, "n1"), "n1")
        QgsProject.instance().addMapLayer(l2)
        d["n1_commit"] = _rename(l2, "N1_AFTER_NEW")
        d["n1_rows"] = _wait_rows(j1, "N1_AFTER_NEW")

        # ---- N2: writer killed mid-session ------------------------------
        class _Die(BaseException):
            pass

        def _deadly(_n):
            raise _Die("writer killed by the scenario")

        wq = plugin._write_queue
        tracker = plugin._tracker
        normal_cb = getattr(tracker, "_on_flush_callback", None)
        wq.set_flush_callback(_deadly)
        d["n2_first_commit"] = _rename(l2, "N2_BEFORE_DEATH")
        pump(0.8)
        d["n2_writer_threads_after_kill"] = writer_threads() - d["writers_before"]
        d["n2_second_commit"] = _rename(l2, "N2_AFTER_DEATH")
        pump(0.2)
        d["n2_outcome_after_death"] = getattr(
            getattr(tracker, "last_commit_outcome", None), "name", None)
        wq.set_flush_callback(normal_cb)
        tick = getattr(plugin, "_watchdog_tick", None)
        if callable(tick):
            tick()
        pump(0.5)
        d["n2_rows_after"] = _wait_rows(j1, "N2_AFTER_DEATH")
        d["n2_rows_before"] = _rows_with(j1, "N2_BEFORE_DEATH")
        d["n2_bar"] = [t for t in iface.bar.texts() if "thread" in t.lower()]
        d["n2_writer_threads_after_watchdog"] = writer_threads() - d["writers_before"]

        # ---- N3: partial commit -----------------------------------------
        l3 = _layer(_make_gpkg(tmp, "n3", unique=True), "n3")
        QgsProject.instance().addMapLayer(l3)
        from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
        fids = sorted(f.id() for f in l3.getFeatures())
        l3.startEditing()
        l3.changeAttributeValue(fids[0], l3.fields().indexFromName("nom"), "N3_WRITTEN")
        dup = QgsFeature(l3.fields())
        attrs = [None] * l3.fields().count()
        attrs[l3.fields().indexFromName("code")] = "C002"
        dup.setAttributes(attrs)
        dup.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(9, 9)))
        l3.addFeature(dup)
        d["n3_commit"] = bool(l3.commitChanges())
        l3.rollBack()
        pump(0.5)
        d["n3_rows"] = _wait_rows(j1, "N3_WRITTEN")
        d["n3_bar"] = [t for t in iface.bar.texts() if "partiel" in t.lower()]

        # ---- N4: the indicator reads runtime facts -----------------------
        ind = plugin._status_indicator
        plugin._update_status_bar()
        d["n4_state_healthy"] = getattr(ind, "_current_level", None)
        plugin._tracker.capture_failures = 1   # a commit whose capture failed
        plugin._update_status_bar()
        d["n4_state_after_failure"] = getattr(ind, "_current_level", None)
        d["n4_tooltip_after_failure"] = (getattr(ind, "_current_tooltip", "") or "")[:400]
        plugin._tracker.capture_failures = 0
        plugin._update_status_bar()
        for lyr_id in list(QgsProject.instance().mapLayers()):
            QgsProject.instance().removeMapLayer(lyr_id)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        pump(0.3)
        d["n5_writer_threads_after_unload"] = writer_threads() - d["writers_before"]
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"tx_plugin_capture_lifecycle run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    out.append((
        "p1_blank_project_at_startup_is_captured",
        d.get("p1_commit") is True and (d.get("p1_rows") or 0) >= 1,
        f"commit={d.get('p1_commit')} lignes={d.get('p1_rows')} journal="
        f"{d.get('p1_journal')} crash={d.get('crash')} {d.get('trace', '')[-600:]}",
    ))
    out.append((
        "n1_new_project_is_captured",
        d.get("n1_journal_open") is True and (d.get("n1_rows") or 0) >= 1,
        f"journal ouvert apres Nouveau projet={d.get('n1_journal_open')} "
        f"lignes={d.get('n1_rows')} : apres Fichier > Nouveau, les editions "
        f"du projet vierge doivent etre journalisees",
    ))
    out.append((
        "n2_dead_writer_is_restarted_and_nothing_is_lost",
        d.get("n2_writer_threads_after_kill") == 0
        and (d.get("n2_rows_before") or 0) >= 1 and (d.get("n2_rows_after") or 0) >= 1,
        f"ecrivains apres la mort={d.get('n2_writer_threads_after_kill')} "
        f"(attendu 0 : la mort est reelle) ; lignes avant/apres la mort="
        f"{d.get('n2_rows_before')}/{d.get('n2_rows_after')} ; issue apres la "
        f"mort={d.get('n2_outcome_after_death')} ; ecrivains apres le chien de "
        f"garde={d.get('n2_writer_threads_after_watchdog')}",
    ))
    out.append((
        "n2_outcome_after_death_is_not_normal",
        d.get("n2_outcome_after_death") == "PERSISTED_FALLBACK",
        f"issue du commit fait pendant que l'ecrivain est mort="
        f"{d.get('n2_outcome_after_death')} (attendu PERSISTED_FALLBACK : "
        f"durable, mais pas un accuse normal)",
    ))
    out.append((
        "n2_user_is_told",
        bool(d.get("n2_bar")),
        f"messages de la barre sur l'ecrivain={d.get('n2_bar')} : une panne "
        f"d'ecriture ne doit pas rester dans un fichier de log",
    ))
    out.append((
        "n3_partial_commit_is_journalled_and_told",
        d.get("n3_commit") is False and (d.get("n3_rows") or 0) >= 1
        and bool(d.get("n3_bar")),
        f"commit={d.get('n3_commit')} lignes journalisees={d.get('n3_rows')} "
        f"alertes={d.get('n3_bar')}",
    ))
    out.append((
        "n4_indicator_follows_runtime_facts",
        d.get("n4_state_healthy") in ("PROTECTED", "PERSISTING")
        and d.get("n4_state_after_failure") == "ERROR",
        f"etat sain={d.get('n4_state_healthy')} etat apres un echec de capture="
        f"{d.get('n4_state_after_failure')} infobulle="
        f"{d.get('n4_tooltip_after_failure')!r}",
    ))
    out.append((
        "n5_unload_leaves_no_writer_thread",
        d.get("n5_writer_threads_after_unload") == 0 and not d.get("unload_error"),
        f"ecrivains vivants apres unload={d.get('n5_writer_threads_after_unload')} "
        f"erreur={d.get('unload_error')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_plugin_capture_lifecycle.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
