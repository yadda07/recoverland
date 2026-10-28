# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_long_path_save_follow - RecoverLand validation runtime.

Invariant: I-JRN-01 (un projet a un journal, le meme a chaque ouverture) et
I-CAPTURE-01 (une edition acceptee finit dans le journal que le projet lira).

Le mecanisme
------------
Un projet jamais enregistre journalise dans le profil. A l'enregistrement,
`RecoverPlugin._on_project_saved` (recover.py) planifie le rattachement de
l'historique au journal du projet (etape 1), puis, un tour de boucle plus
tard (etape 2), verifie que la sauvegarde a bien ecrit le fichier avant de
basculer sur le journal du projet. Cette verification etait un
`os.path.isfile(pending.project_file)` sur le chemin SIMPLE. Windows repond
False pour un fichier qui existe des que son chemin atteint 260 caracteres
(MAX_PATH, mesure) : la sauvegarde etait prise pour une sauvegarde ratee, le
rattachement annule, et la session continuait dans le journal du profil.
Le reste du code du journal (core/long_paths.py, JournalManager) sait
pourtant ouvrir et reconnaitre ce projet : seule cette verification du
plugin le trahissait.

Le scenario d'echec concret
---------------------------
L'utilisateur range son projet dans une arborescence profonde (serveur de
fichiers, dossiers par client / annee / dossier). Il edite, enregistre,
edite encore, ferme. A la reouverture, Restaurer et Rewind ne montrent rien
de sa session : l'historique est reste dans le journal du profil, que le
nettoyage des orphelins supprime 30 jours plus tard.

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin sous plugin_harness.FakeIface, vraie couche GeoPackage,
vrais `QgsProject.write` / `clear` / `read`, projet de 270 caracteres.
Mesure sur QGIS 4.2.1 : un .qgs de ce chemin s'ecrit et se relit ; un .qgz
s'ecrit mais QGIS ne sait pas le relire ("Unable to unzip file") : la
reouverture est donc prouvee sur le .qgs, le rattachement sur les deux (un
.qgz deplace plus tard vers un chemin court emporte son dossier
.recoverland, donc son historique).
  S1  (.qgs) commit dans le projet vierge ; enregistrement sous le chemin
      long : le plugin passe sur le journal du projet (a cote du projet),
      qui contient deja ce commit.
  S2  (.qgs) commit apres l'enregistrement : il arrive dans ce meme journal.
  S3  (.qgs) fermeture, reouverture : le journal rouvert est celui du projet
      et contient les deux commits, une fois chacun.
  Z1  (.qgz) meme enregistrement : le journal suit le projet et contient le
      commit fait avant.
  S4  aucun message ne dit a tort que le projet n'a pas ete enregistre.

Verdict attendu avant correctif : FAIL (S1, S2, S3, Z1).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_plugin_long_path_save_follow"
INVARIANT = "I-JRN-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PFX = "\\\\?\\"
_PROJECT_LEN = 270
_MARKERS = ("AVANT_SAUV_LONG", "APRES_SAUV_LONG")
_Z_MARKER = "AVANT_SAUV_QGZ"


def _ext(path: str) -> str:
    p = os.path.abspath(path)
    return p if p.startswith(_PFX) else _PFX + p


def _plain(path) -> str:
    if not path:
        return ""
    return os.path.normcase(os.path.abspath(str(path).replace(_PFX, "")))


def _deep_project(base: str, length: int, name: str) -> str:
    """A project path of exactly *length* characters under *base*."""
    folder = base
    need = length - len(folder) - len(name) - 1
    while need > 0:
        take = min(need - 1, 100)
        if need - 1 - take == 1:
            take -= 1
        folder = os.path.join(folder, "d" * take)
        need = length - len(folder) - len(name) - 1
    os.makedirs(_ext(folder), exist_ok=True)
    return os.path.join(folder, name)


def _project_journal(project: str) -> str:
    return os.path.join(os.path.dirname(project), ".recoverland",
                        "recoverland_audit.sqlite")


def _make_gpkg(path: str) -> None:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i in range(4):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"C{i:03d}", f"NOM_{i:02d}"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(f)
    mem.commitChanges()
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "parcelles"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)


def _layer():
    from qgis.core import QgsProject
    layers = QgsProject.instance().mapLayersByName("parcelles")
    return layers[0] if layers else None


def _rename(layer, index: int, value: str) -> bool:
    if layer is None:
        return False
    idx = layer.fields().indexFromName("nom")
    target = next((f.id() for f in layer.getFeatures()
                   if f["code"] == f"C{index:03d}"), None)
    if target is None:
        return False
    layer.startEditing()
    layer.changeAttributeValue(target, idx, value)
    return bool(layer.commitChanges())


def _counts(plugin, markers=_MARKERS) -> dict:
    """What the open journal holds, read through the product's read path."""
    if not plugin._journal.is_open:
        return {m: None for m in markers}
    plugin._write_queue.wait_until_drained(5.0)
    conn = plugin._journal.create_read_connection()
    try:
        return {m: int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE attributes_json LIKE ?",
            (f"%{m}%",)).fetchone()[0]) for m in markers}
    finally:
        conn.close()


def _wait(plugin, marker: str, timeout: float = 6.0) -> int:
    from scripts.validation.plugin_harness import pump
    deadline = time.monotonic() + timeout
    got = -1
    while time.monotonic() < deadline:
        got = (_counts(plugin, (marker,)).get(marker) or 0)
        if got >= 1:
            return got
        pump(0.1)
    return got


def _journal(plugin):
    return plugin._journal.path if plugin._journal.is_open else None


def setup(ctx):
    from qgis.core import QgsProject, QgsSettings
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    QgsSettings().setValue("RecoverLand/tracking_enabled", True)
    QgsSettings().setValue("RecoverLand/tracking_disabled_by_disk", False)
    tmp = tempfile.mkdtemp(prefix="rl_q_lps_")
    ctx.data["tmp"] = tmp
    os.makedirs(os.path.join(tmp, "data"), exist_ok=True)
    gpkg = os.path.join(tmp, "data", "parcelles.gpkg")
    _make_gpkg(gpkg)
    ctx.data["gpkg"] = gpkg
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} tmp={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    qgs = _deep_project(os.path.join(d["tmp"], "clients_qgs"), _PROJECT_LEN, "P.qgs")
    qgz = _deep_project(os.path.join(d["tmp"], "clients_qgz"), _PROJECT_LEN, "P.qgz")
    d["project_len"] = [len(qgs), len(qgz)]
    d["qgs_journal"] = _project_journal(qgs)
    d["qgz_journal"] = _project_journal(qgz)
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    try:
        plugin.initGui()
        pump(0.3)
        layer = QgsVectorLayer(f"{d['gpkg']}|layername=parcelles", "parcelles", "ogr")
        QgsProject.instance().addMapLayer(layer)
        # ---- .qgs: save, edit, reopen ---------------------------------
        d["s1_commit"] = _rename(layer, 0, _MARKERS[0])
        d["s1_in_session"] = _wait(plugin, _MARKERS[0])
        d["s1_write"] = bool(QgsProject.instance().write(qgs))
        pump(0.6)
        d["s1_file_written"] = os.path.isfile(_ext(qgs))
        d["s1_journal"] = _journal(plugin)
        d["s1_counts"] = _counts(plugin)
        d["s2_commit"] = _rename(_layer(), 1, _MARKERS[1])
        _wait(plugin, _MARKERS[1])
        d["s2_journal"] = _journal(plugin)
        d["s2_counts"] = _counts(plugin)
        QgsProject.instance().clear()
        pump(0.3)
        d["s3_read"] = bool(QgsProject.instance().read(qgs))
        pump(0.6)
        d["s3_journal"] = _journal(plugin)
        d["s3_counts"] = _counts(plugin)
        # ---- .qgz: a blank project saved under a long path --------------
        QgsProject.instance().clear()
        pump(0.4)
        layer = QgsVectorLayer(f"{d['gpkg']}|layername=parcelles", "parcelles", "ogr")
        QgsProject.instance().addMapLayer(layer)
        d["z1_commit"] = _rename(layer, 2, _Z_MARKER)
        d["z1_in_session"] = _wait(plugin, _Z_MARKER)
        d["z1_write"] = bool(QgsProject.instance().write(qgz))
        pump(0.6)
        d["z1_file_written"] = os.path.isfile(_ext(qgz))
        d["z1_journal"] = _journal(plugin)
        d["z1_counts"] = _counts(plugin, (_Z_MARKER,))
        d["bar"] = iface.bar.texts()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} data="
         f"{json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")


def teardown(ctx):
    tmp = ctx.data.get("tmp")
    if tmp:
        shutil.rmtree(_ext(tmp), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    target = _plain(d.get("qgs_journal"))
    crash = f"crash={d.get('crash')} {d.get('trace', '')}"
    out.append((
        "fixture_long_projects_saved",
        d.get("project_len") == [_PROJECT_LEN, _PROJECT_LEN]
        and d.get("s1_write") is True and d.get("s1_file_written") is True
        and d.get("z1_write") is True and d.get("z1_file_written") is True
        and d.get("s1_commit") is True and d.get("z1_commit") is True
        and (d.get("s1_in_session") or 0) >= 1 and (d.get("z1_in_session") or 0) >= 1,
        f"chemins={d.get('project_len')} caracteres, write="
        f"{d.get('s1_write')}/{d.get('z1_write')} fichiers presents="
        f"{d.get('s1_file_written')}/{d.get('z1_file_written')} commits dans le "
        f"journal de session={d.get('s1_in_session')}/{d.get('z1_in_session')} "
        f"{crash}",
    ))
    out.append((
        "s1_journal_follows_the_long_project",
        _plain(d.get("s1_journal")) == target
        and (d.get("s1_counts") or {}).get(_MARKERS[0]) == 1,
        f"journal apres l'enregistrement={d.get('s1_journal')} (attendu {target}) "
        f"contenu={d.get('s1_counts')}. La sauvegarde d'un projet a chemin long "
        f"prise pour ratee laisse l'historique dans le journal du profil",
    ))
    out.append((
        "s2_next_edit_lands_in_the_project_journal",
        _plain(d.get("s2_journal")) == target
        and (d.get("s2_counts") or {}).get(_MARKERS[1]) == 1,
        f"journal={d.get('s2_journal')} contenu={d.get('s2_counts')}",
    ))
    out.append((
        "s3_reopened_project_reads_its_whole_session",
        d.get("s3_read") is True and _plain(d.get("s3_journal")) == target
        and d.get("s3_counts") == {m: 1 for m in _MARKERS},
        f"relecture={d.get('s3_read')} journal rouvert={d.get('s3_journal')} "
        f"contenu={d.get('s3_counts')} (attendu une fois chacun). A la "
        f"reouverture, Restaurer et Rewind ne montreraient rien de la session",
    ))
    out.append((
        "z1_qgz_journal_follows_the_long_project",
        _plain(d.get("z1_journal")) == _plain(d.get("qgz_journal"))
        and (d.get("z1_counts") or {}).get(_Z_MARKER) == 1,
        f"journal apres l'enregistrement du .qgz={d.get('z1_journal')} (attendu "
        f"{d.get('qgz_journal')}) contenu={d.get('z1_counts')}",
    ))
    wrong = [t for t in d.get("bar") or []
             if "n'a pas ete enregistre" in t or "pas pu etre rattache" in t]
    out.append((
        "s4_no_false_failed_save_message",
        "bar" in d and not wrong,
        f"messages={wrong}",
    ))
    out.append((
        "unload_clean",
        not d.get("unload_error"),
        f"unload={d.get('unload_error')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID} (setup|run): trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
