"""tx_journal_orphans_and_storage - RecoverLand validation runtime.

Invariant: I-JRN (un evenement capture reste joignable et durable ; un
journal qui contient encore des evenements n'est jamais supprime sans que
l'utilisateur le sache).

Deux mecanismes du choix de journal, un meme resultat : de l'historique
capture que plus rien ne relit, puis qui disparait.

A. NETTOYAGE DES JOURNAUX "ORPHELINS"
   `cleanup_orphan_journals` (appele a chaque ouverture de projet) supprime
   tout `<profil>/recoverland/audit/*.sqlite` dont la date de modification a
   plus de 30 jours, sans regarder ce qu'il contient ni le dire a personne.
   Ce dossier est celui des projets jamais enregistres : chaque session de
   ce type y laisse un fichier que plus rien n'ouvre. Supprimer ces
   fichiers, c'est supprimer les seules copies des modifications capturees
   pendant ces sessions ; l'outbox d'un journal dont la session a plante
   (evenements acceptes mais pas encore indexes) part avec lui.
   Scenario d'echec : un utilisateur travaille une journee sans enregistrer
   son projet, QGIS plante ; un mois plus tard, au demarrage, le fichier qui
   contenait toutes ces modifications est efface en silence.

B. PROJET RANGE DANS UN STOCKAGE (GeoPackage, PostgreSQL)
   Pour un projet stocke dans une base, `absoluteFilePath()` est vide (API
   QGIS). `open_for_project` le traitait comme un projet jamais enregistre :
   nouvelle cle aleatoire, donc un journal NEUF et vide a chaque ouverture.
   Scenario d'echec : l'equipe range ses projets dans un GeoPackage ; chaque
   matin le projet s'ouvre sur un historique vide, alors que les
   modifications de la veille ont bien ete capturees... dans un fichier que
   rien ne relira.

Ce que le scenario execute reellement
-------------------------------------
A : un dossier d'audit de profil avec cinq journaux reels (evenements,
    vide, outbox seule, recent, illisible), dates de modification reculees
    de 40 jours ; appel du vrai `cleanup_orphan_journals`. On capte aussi le
    panneau de messages QGIS (onglet RecoverLand) : c'est la que
    l'utilisateur doit etre prevenu.
B : VRAI RecoverPlugin + faux iface : projet vierge, commit, enregistrement
    DANS un GeoPackage (`geopackage:...?projectName=`), commit, fermeture,
    reouverture, commit, fermeture, reouverture. Chaque ouverture doit
    retrouver le meme journal et toutes les modifications.

Verdict attendu avant correctif : FAIL (A et B).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_orphans_and_storage"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_OLD = 40 * 86400


def _seed_journal(path: str, n_events: int) -> None:
    from recoverland.core.sqlite_schema import (
        initialize_schema, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
        sql = "".join((
            "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
            ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
        ))
        for i in range(n_events):
            conn.execute(sql, (
                "tx_jos_project", "tx_jos_ds", "lyr", "parcelles", "ogr",
                json.dumps({"fid": i}), "UPDATE",
                json.dumps({"changed_only": {"nom": {"old": "A", "new": f"B{i}"}}}),
                None, "NoGeometry", "EPSG:2154", None, "tester", "sess",
                "2026-08-01T10:00:00+00:00", None, f"fid:{i}", 2, None, None,
            ))
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()
    for suffix in ("-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)


def _age(path: str, seconds: float) -> None:
    ts = time.time() - seconds
    os.utime(path, (ts, ts))


class _PanelRecorder:
    """What reaches the RecoverLand tab of the QGIS message log panel.

    ``QgsMessageLog.messageReceived`` is not emitted in this headless
    runtime (measured), so the scenario records at the call every
    user-facing ``qlog`` goes through, and forwards it unchanged.
    """

    def __init__(self, real):
        self.real = real
        self.messages = []

    def logMessage(self, message, tag="", *args, **kwargs):  # noqa: N802
        if tag == "RecoverLand":
            self.messages.append(str(message))
        return self.real.logMessage(message, tag, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.real, name)


def _part_a(tmp: str) -> dict:
    from recoverland.core.journal_manager import cleanup_orphan_journals

    profile = os.path.join(tmp, "profil_a")
    audit = os.path.join(profile, "recoverland", "audit")
    os.makedirs(audit, exist_ok=True)
    files = {
        "with_events": os.path.join(audit, "audit_aaaaaaaaaaaaaaa1.sqlite"),
        "empty": os.path.join(audit, "audit_aaaaaaaaaaaaaaa2.sqlite"),
        "outbox_only": os.path.join(audit, "audit_aaaaaaaaaaaaaaa3.sqlite"),
        "recent": os.path.join(audit, "audit_aaaaaaaaaaaaaaa4.sqlite"),
        "unreadable": os.path.join(audit, "audit_aaaaaaaaaaaaaaa5.sqlite"),
    }
    _seed_journal(files["with_events"], 3)
    _seed_journal(files["empty"], 0)
    _seed_journal(files["outbox_only"], 0)
    outbox = files["outbox_only"] + ".outbox"
    os.makedirs(outbox, exist_ok=True)
    with open(os.path.join(outbox, "seg-00000001.jsonl"), "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"t": "ev", "b": "b1", "e": {"event_uid": "u1"}}) + "\n")
    _seed_journal(files["recent"], 2)
    with open(files["unreadable"], "wb") as fh:
        fh.write(b"SQLite format 3\x00" + bytes(range(256)) * 16)
    for key, path in files.items():
        _age(path, 86400 if key == "recent" else _OLD)

    import recoverland.core.logger as logger_mod

    panel = _PanelRecorder(logger_mod.QgsMessageLog)
    logger_mod.QgsMessageLog = panel
    res = {"files": files}
    try:
        res["removed"] = cleanup_orphan_journals(profile, max_age_days=30,
                                                 current_path="")
    except Exception as exc:  # noqa: BLE001
        res["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        logger_mod.QgsMessageLog = panel.real
    res["exists"] = {k: os.path.isfile(p) for k, p in files.items()}
    res["outbox_exists"] = os.path.isdir(outbox)
    res["messages"] = panel.messages[-10:]
    return res


# ----------------------------------------------------------------------
# Part B: project kept in a GeoPackage project storage
# ----------------------------------------------------------------------

def _make_gpkg(path: str, layer_name: str, n: int) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i in range(n):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"C{i:03d}", f"NOM_{i:02d}"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(f)
    mem.commitChanges()
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = layer_name
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)


def _rename(index: int, value: str) -> bool:
    from qgis.core import QgsProject
    layers = QgsProject.instance().mapLayersByName("parcelles")
    if not layers:
        return False
    layer = layers[0]
    idx = layer.fields().indexFromName("nom")
    target = None
    for feat in layer.getFeatures():
        if feat["code"] == f"C{index:03d}":
            target = feat.id()
    if target is None:
        return False
    layer.startEditing()
    layer.changeAttributeValue(target, idx, value)
    return bool(layer.commitChanges())


def _count(journal: str, value: str) -> int:
    if not journal or not os.path.isfile(journal):
        return -1
    from recoverland.core.journal_manager import sqlite_readonly_uri
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(journal), uri=True)
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
                (f"%{value}%",)).fetchone()[0])
        finally:
            conn.close()
    except sqlite3.Error:
        return -1


def _wait(plugin, value: str, timeout: float = 6.0) -> int:
    from scripts.validation.plugin_harness import pump
    deadline = time.monotonic() + timeout
    got = -1
    while time.monotonic() < deadline:
        got = _count(plugin._journal.path if plugin._journal.is_open else None, value)
        if got >= 1:
            return got
        pump(0.1)
    return got


def _part_b(tmp: str) -> dict:
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    res = {}
    data = os.path.join(tmp, "data_b.gpkg")
    store = os.path.join(tmp, "stockage_projets.gpkg")
    _make_gpkg(data, "parcelles", 8)
    _make_gpkg(store, "stub", 1)
    uri = f"geopackage:{Path(store).as_posix()}?projectName=chantier"
    res["uri"] = uri
    QgsProject.instance().clear()
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    try:
        plugin.initGui()
        pump(0.3)
        layer = QgsVectorLayer(f"{data}|layername=parcelles", "parcelles", "ogr")
        QgsProject.instance().addMapLayer(layer)
        res["commit_before"] = _rename(0, "STOCK_AVANT")
        res["before_in_session"] = _wait(plugin, "STOCK_AVANT")
        res["write"] = bool(QgsProject.instance().write(uri))
        pump(0.5)
        res["absolute_file_path"] = QgsProject.instance().absoluteFilePath()
        res["commit_after"] = _rename(1, "STOCK_APRES")
        _wait(plugin, "STOCK_APRES")
        QgsProject.instance().clear()
        pump(0.3)
        res["read1"] = bool(QgsProject.instance().read(uri))
        pump(0.5)
        res["journal_open1"] = plugin._journal.path if plugin._journal.is_open else None
        res["counts_open1"] = {
            m: _count(res["journal_open1"], m) for m in ("STOCK_AVANT", "STOCK_APRES")}
        res["commit_reopened"] = _rename(2, "STOCK_REOUVERT")
        _wait(plugin, "STOCK_REOUVERT")
        QgsProject.instance().clear()
        pump(0.3)
        res["read2"] = bool(QgsProject.instance().read(uri))
        pump(0.5)
        res["journal_open2"] = plugin._journal.path if plugin._journal.is_open else None
        res["counts_open2"] = {
            m: _count(res["journal_open2"], m)
            for m in ("STOCK_AVANT", "STOCK_APRES", "STOCK_REOUVERT")}
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        res["crash"] = f"{type(exc).__name__}: {exc}"
        res["trace"] = traceback.format_exc()[-1500:]
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            res["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    return res


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog

    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jos_")
    ctx.data["tmp"] = tmp
    flog(f"tx_journal_orphans_and_storage setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    tmp = ctx.data["tmp"]
    ctx.data["a"] = _part_a(tmp)
    ctx.data["b"] = _part_b(tmp)
    flog(f"tx_journal_orphans_and_storage run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in ctx.data.items()}, default=str)[:5000]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    a = ctx.data.get("a") or {}
    exists = a.get("exists") or {}
    out.append((
        "a_orphelin_avec_evenements_conserve",
        exists.get("with_events") is True and not a.get("error"),
        f"journal orphelin de 40 jours contenant 3 evenements encore present="
        f"{exists.get('with_events')} (erreur={a.get('error')!r}) : c'est la "
        f"seule copie des modifications d'une session jamais enregistree",
    ))
    out.append((
        "a_orphelin_avec_outbox_conserve",
        exists.get("outbox_only") is True and a.get("outbox_exists") is True,
        f"journal orphelin dont l'outbox contient un evenement accepte : "
        f"journal present={exists.get('outbox_only')} outbox presente="
        f"{a.get('outbox_exists')} : supprimer le journal rend l'outbox "
        f"impossible a rejouer",
    ))
    out.append((
        "a_orphelin_illisible_conserve",
        exists.get("unreadable") is True,
        f"journal orphelin illisible present={exists.get('unreadable')} : ne "
        f"pas pouvoir prouver qu'il est vide n'autorise pas a le supprimer",
    ))
    out.append((
        "a_orphelin_recent_conserve",
        exists.get("recent") is True,
        f"journal orphelin recent present={exists.get('recent')}",
    ))
    out.append((
        "a_orphelin_vide_nettoye",
        exists.get("empty") is False,
        f"journal orphelin VIDE de 40 jours encore present="
        f"{exists.get('empty')} : le menage des fichiers sans contenu reste "
        f"fait",
    ))
    name = os.path.basename((a.get("files") or {}).get("with_events", "?"))
    out.append((
        "a_utilisateur_prevenu_dans_le_panneau_de_messages",
        any(name in m and "3" in m for m in a.get("messages") or []),
        f"messages RecoverLand du panneau QGIS={a.get('messages')} : un "
        f"historique que plus aucun projet n'ouvre doit etre signale "
        f"(fichier et nombre d'evenements), pas seulement ecrit dans un log",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"JournalManager: orphan journal kept .*{name}.*events=3",
        name="a_conservation_tracee",
        min_count=1,
    ))

    b = ctx.data.get("b") or {}
    crash = f"{b.get('crash', '')} {b.get('trace', '')[-500:]}".strip()
    out.append((
        "b_fixture_projet_stocke",
        b.get("write") is True and b.get("read1") is True
        and b.get("read2") is True and b.get("before_in_session") == 1
        and not b.get("crash"),
        f"write={b.get('write')} read1={b.get('read1')} read2={b.get('read2')} "
        f"capture initiale={b.get('before_in_session')} "
        f"absoluteFilePath={b.get('absolute_file_path')!r} crash={crash!r}",
    ))
    out.append((
        "b_meme_journal_a_chaque_ouverture",
        bool(b.get("journal_open1"))
        and b.get("journal_open1") == b.get("journal_open2"),
        f"journal a la 1re ouverture={b.get('journal_open1')} a la 2e="
        f"{b.get('journal_open2')} : un projet stocke dans une base doit "
        f"retrouver son journal, pas en recevoir un neuf et vide",
    ))
    c1 = b.get("counts_open1") or {}
    out.append((
        "b_historique_suit_l_enregistrement_en_base",
        c1.get("STOCK_AVANT") == 1 and c1.get("STOCK_APRES") == 1,
        f"lu a la 1re reouverture={c1} (attendu 1 chacune) : les "
        f"modifications faites avant et apres l'enregistrement dans le "
        f"GeoPackage doivent etre relues",
    ))
    c2 = b.get("counts_open2") or {}
    out.append((
        "b_historique_conserve_entre_ouvertures",
        all(c2.get(m) == 1 for m in ("STOCK_AVANT", "STOCK_APRES", "STOCK_REOUVERT")),
        f"lu a la 2e reouverture={c2} (attendu 1 chacune)",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_orphans_and_storage.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
