# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_worker_lifecycle_crash - RecoverLand validation runtime.

Invariant: I-THREAD-JOIN (un thread de fond est arrete et attendu avant que
son proprietaire, ou sa derniere reference, ne soit detruit ; le plugin ne
fait jamais tomber QGIS).

Le mecanisme
------------
(a) Le SnapshotRebuildWorker etait cree avec parent=dialogue. Au dechargement
du plugin, `_release_dialog` appelle cleanup_resources -> _stop_snapshot_mode
-> _cancel_snap_worker, qui ne fait que lever un drapeau : personne n'attend.
Puis dlg.deleteLater() detruit le dialogue et ses enfants ; ~QThread sur un
thread qui tourne encore est un qFatal("QThread: Destroyed while thread is
still running"). Le drapeau n'etait pas lu dans la boucle de lecture des
lignes : une reconstruction sur un gros journal continue des secondes apres.
(b) Le MarkerScanWorker n'avait pas de parent ; sa seule reference forte etait
self._review_marker_worker, remise a None par son propre slot de resultat,
alors que run() etait encore dans conn.close(). Le wrapper Python part, le
QThread C++ est detruit en cours d'execution : meme qFatal, a chaque fin de
deplacement de carte.

L'echec concret pour l'utilisateur
----------------------------------
Review actif sur un gros journal ; l'utilisateur fait glisser la date puis
recharge le plugin (ou QGIS le decharge a la fermeture) : QGIS s'arrete net,
le projet non enregistre et les tampons d'edition sont perdus. Independamment,
un simple deplacement de carte peut provoquer le meme arret.

Un qFatal ne se prouve pas dans le processus de la suite (il l'emporterait) :
chaque cas tourne dans un processus QGIS enfant (crash_harness.run_child), et
l'assertion porte sur son code de sortie.

Mesure sur QGIS 4.2.1 : (a) reproduit (l'enfant meurt avec 0xC0000409 des
que le dialogue est detruit). (b) NE se reproduit PAS par ce chemin : la
trame de run() garde elle-meme une reference sur le worker jusqu'a son
retour. Le cas (b) reste ici comme garde de non-regression, pas comme
preuve du defaut.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "rvw_worker_lifecycle_crash"
INVARIANT = "I-THREAD-JOIN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Common part: a real plugin, a real GeoPackage, real captured edits, Review
# on, and a journal made big enough for a rebuild to take seconds.
_CHILD_SETUP = """
import sqlite3, tempfile
from qgis.core import QgsProject
from scripts.validation.scenarios import _rvw_harness as h
from recoverland.core.identity import compute_datasource_fingerprint
from recoverland.core.sqlite_schema import (
    AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS)

tmp = tempfile.mkdtemp(prefix="rl_rvw_life_")
rows = [("P%d" % i, "N%d" % i, float(i), float(i), float(i)) for i in range(1, 6)]
gpkg = h.make_gpkg(tmp, "cadastre", rows)
iface, plugin = h.start_plugin(tmp)
layer = h.load_layer(gpkg, "cadastre")
h.pump(0.2)
cutoff = h.utc_now_iso()
time.sleep(1.2)
h.user_commit(layer, lambda lyr: h.rename(lyr, "P1", "N1b"))
jpath = h.journal_path(plugin)
h.wait_rows(jpath, 1)
fp = compute_datasource_fingerprint(layer)
conn = sqlite3.connect(jpath)
payload = []
for i in range(6000):
    created = ("2026-01-01T00:00:%02d.%06d+00:00" % (i % 60, i)) if i < 5000 else (
        "2099-01-01T00:00:%02d.%06d+00:00" % (i % 60, i))
    payload.append((
        "proj", fp, layer.id(), "cadastre", "ogr",
        json.dumps({"fid": 100000 + i}), "UPDATE",
        json.dumps({"changed_only": {"nom": {"old": "a", "new": "b"}}}),
        None, "Point", "EPSG:2154", None, "tester", None, created, None,
        "fid:%d" % (100000 + (i % 700)), 5, None, None,
    ))
conn.executemany(
    "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
    + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")", payload)
conn.commit()
conn.close()
h.set_view(iface, 0.0, 0.0, 10.0, 10.0)
dlg = h.open_dialog(plugin, [layer])
review_ok = h.review_on(dlg, 1)
"""

_CHILD_UNLOAD = _CHILD_SETUP + """

class _SlowJournal:
    '''The real journal, on a slow disk: every statement crawls.'''
    def __init__(self, journal):
        self._journal = journal
    def __getattr__(self, name):
        return getattr(self._journal, name)
    def create_read_connection(self):
        c = self._journal.create_read_connection()
        c.set_progress_handler(lambda: (time.sleep(0.002), 0)[1], 200)
        return c

dlg._journal = _SlowJournal(plugin._journal)
dlg._on_snapshot_date_changed("2030-06-01T00:00:00")
h.pump(0.6)
worker = dlg._review_snap_worker
running_at_unload = bool(worker is not None and worker.isRunning())
t0 = time.monotonic()
plugin._release_dialog()
# QGIS runs its event loop: the dialog's deleteLater is processed right
# away. processEvents() alone never handles DeferredDelete, so force it.
from qgis.PyQt.QtCore import QCoreApplication, QEvent
QCoreApplication.sendPostedEvents(None, QEvent.Type.DeferredDelete)
h.pump(1.5)
still_running = bool(worker is not None and worker.isRunning())
print("CHILD_RESULT " + json.dumps({
    "review_ok": review_ok, "running_at_unload": running_at_unload,
    "still_running_after_release": still_running,
    "release_s": round(time.monotonic() - t0, 2)}))
plugin.unload()
h.pump(0.3)
print("CHILD_OK")
"""

_CHILD_MARKERS = _CHILD_SETUP + """

class _SlowCloseConn:
    '''A read connection whose close() takes time (network share, AV scan).'''
    def __init__(self, conn):
        self._conn = conn
    def __getattr__(self, name):
        return getattr(self._conn, name)
    def close(self):
        time.sleep(0.3)
        self._conn.close()

class _Journal:
    def __init__(self, journal):
        self._journal = journal
    def __getattr__(self, name):
        return getattr(self._journal, name)
    def create_read_connection(self):
        return _SlowCloseConn(self._journal.create_read_connection())

h.pump(0.5)
dlg._journal = _Journal(plugin._journal)
dlg._review_global_t0_iso = dlg._review_global_t0_iso or "2000-01-01T00:00:00"
dlg._review_global_t1_iso = "2100-01-01T00:00:00"
n_scans = 0
for i in range(4):
    dlg._review_marker_bbox_signature = ""
    dlg._refresh_extent_markers(dlg._compute_bbox_per_layer(), "mk%d" % i)
    n_scans += 1
    h.pump(0.7)
print("CHILD_RESULT " + json.dumps({"review_ok": review_ok, "n_scans": n_scans}))
dlg._journal = plugin._journal
h.review_off(dlg)
plugin.unload()
h.pump(0.3)
print("CHILD_OK")
"""


def _child_result(stdout: str) -> dict:
    for line in (stdout or "").splitlines():
        if line.startswith("CHILD_RESULT "):
            try:
                return json.loads(line[len("CHILD_RESULT "):])
            except ValueError:
                return {}
    return {}


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"rvw_worker_lifecycle_crash setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from scripts.validation.crash_harness import run_child
    d = ctx.data
    for key, body in (("unload", _CHILD_UNLOAD), ("markers", _CHILD_MARKERS)):
        try:
            rc, so, se = run_child(body, timeout=180.0)
        except Exception as exc:  # noqa: BLE001 - timeout is a FAIL, reported
            rc, so, se = -999, "", repr(exc)
        d[key] = {"rc": rc, "ok": "CHILD_OK" in (so or ""),
                  "result": _child_result(so),
                  "stderr_tail": (se or "")[-800:]}
    flog(f"rvw_worker_lifecycle_crash run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:3000]}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    un = d.get("unload") or {}
    mk = d.get("markers") or {}
    out = []
    out.append((
        "unload_during_rebuild_does_not_kill_qgis",
        un.get("rc") == 0 and un.get("ok") is True,
        f"code de sortie={un.get('rc')} fin atteinte={un.get('ok')} "
        f"resultat={un.get('result')} stderr={un.get('stderr_tail')!r} : un "
        f"dechargement pendant une reconstruction ne doit pas arreter QGIS",
    ))
    res = un.get("result") or {}
    out.append((
        "rebuild_was_really_running_at_unload",
        res.get("running_at_unload") is True and res.get("review_ok") is True,
        f"resultat={res} : la preuve ne vaut que si la reconstruction tournait "
        f"au moment du dechargement",
    ))
    out.append((
        "no_review_thread_left_after_release",
        res.get("still_running_after_release") is False,
        f"resultat={res} : apres le dechargement aucun thread Review ne doit "
        f"encore lire le journal",
    ))
    out.append((
        "marker_scan_result_slot_does_not_kill_qgis",
        mk.get("rc") == 0 and mk.get("ok") is True
        and (mk.get("result") or {}).get("n_scans") == 4,
        f"code de sortie={mk.get('rc')} fin atteinte={mk.get('ok')} "
        f"resultat={mk.get('result')} stderr={mk.get('stderr_tail')!r} : la "
        f"fin d'un deplacement de carte ne doit jamais arreter QGIS",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_worker_lifecycle_crash.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
