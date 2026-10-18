"""tx_maint_vacuum_completion - RecoverLand validation runtime.

Invariant: I-THREAD-01 (tout travail en arriere-plan se termine par une
issue livree a l'interface ; les objets Qt ne sont touches que depuis le
thread GUI).

Le mecanisme
------------
Le compactage du journal (bouton "Compacter" du dialogue de maintenance)
lance VACUUM dans un `threading.Thread`. A la fin, ce thread appelait
`QTimer.singleShot(0, ...)` pour prevenir le dialogue. Un thread Python
n'a pas de boucle d'evenements Qt : le minuteur est cree dans ce thread
et ne se declenche jamais. Le dialogue restait sur "Compactage en
cours...", barre de progression animee, bouton grise, jusqu'a sa
fermeture ; un echec du VACUUM (journal verrouille) n'etait jamais dit.
La connexion SQLite fuyait en cas d'erreur.

Cas
---
V1  compactage d'un journal qui a des pages libres : le dialogue recoit la
    fin (plus "en cours", bouton reactive, statut "Termine"), le fichier a
    rapetisse, plus aucun thread RecoverLand-Vacuum vivant.
V2  compactage pendant qu'une autre connexion tient le journal en
    ecriture : l'echec est dit au dialogue (statut d'echec), rien ne
    reste "en cours".
V3  l'espace annonce comme recupere (statut et boite de fin) est celui que
    le scenario mesure lui-meme sur le disque (journal + WAL + SHM) avant
    et apres, et il n'est pas nul. Sous Windows, tant qu'une connexion du
    produit mappait le fichier en memoire (mmap_size=256 Mo), VACUUM ne
    le faisait jamais rapetisser et le dialogue annoncait "Termine" sur un
    fichier inchange.

Verdict attendu avant correctif : FAIL (V1, V2 : l'issue n'arrive jamais).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

SCENARIO_ID = "tx_maint_vacuum_completion"
INVARIANT = "I-THREAD-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _fill_and_free(journal: str, n: int = 4000) -> None:
    conn = sqlite3.connect(journal)
    try:
        with conn:
            conn.executemany(
                "INSERT INTO audit_event (project_fingerprint, datasource_fingerprint, "
                "layer_id_snapshot, layer_name_snapshot, provider_type, "
                "feature_identity_json, operation_type, attributes_json, "
                "geometry_wkb, geometry_type, crs_authid, field_schema_json, "
                "user_name, session_id, created_at) VALUES "
                "('p','d','l','n','ogr','{\"fid\": 1}','UPDATE',?,?,'Point',"
                "'EPSG:2154',NULL,'t','vac','2026-09-27T10:00:00+00:00')",
                [(json.dumps({"changed_only": {"nom": {"old": "x" * 200, "new": "y" * 200}}}),
                  b"\x01" * 400) for _ in range(n)])
        with conn:
            conn.execute("DELETE FROM audit_event WHERE session_id='vac'")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def _footprint(journal: str) -> int:
    """What the journal takes on disk: file, WAL and shared memory."""
    total = 0
    for suffix in ("", "-wal", "-shm"):
        try:
            total += os.path.getsize(journal + suffix)
        except OSError:
            pass
    return total


def _vacuum_threads() -> int:
    return len([t for t in threading.enumerate() if t.name == "RecoverLand-Vacuum"])


def _drive(dialog, timeout: float = 15.0) -> dict:
    from scripts.validation.plugin_harness import pump
    from qgis.PyQt.QtWidgets import QMessageBox
    from recoverland.compat import QtCompat
    saved = (QMessageBox.question, QMessageBox.information, QMessageBox.warning,
             QMessageBox.critical)
    shown = []
    QMessageBox.question = staticmethod(lambda *a, **k: QtCompat.MSG_YES)
    QMessageBox.information = staticmethod(lambda *a, **k: shown.append(("info", a[2] if len(a) > 2 else "")))
    QMessageBox.warning = staticmethod(lambda *a, **k: shown.append(("warn", a[2] if len(a) > 2 else "")))
    QMessageBox.critical = staticmethod(lambda *a, **k: shown.append(("crit", a[2] if len(a) > 2 else "")))
    try:
        dialog._vacuum_journal()
        started = bool(dialog._vacuum_running)
        deadline = time.monotonic() + timeout
        while dialog._vacuum_running and time.monotonic() < deadline:
            pump(0.05)
        pump(0.2)
    finally:
        (QMessageBox.question, QMessageBox.information, QMessageBox.warning,
         QMessageBox.critical) = saved
    return {"started": started, "still_running": bool(dialog._vacuum_running),
            "button_enabled": bool(dialog._vacuum_btn.isEnabled()),
            "status": dialog._vacuum_status.text(),
            "progress_visible": not dialog._progress.isHidden(),
            "boxes": [(k, str(t)[:120]) for k, t in shown]}


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_vac_")
    flog(f"tx_maint_vacuum_completion setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.journal_manager import JournalManager
    from recoverland.journal_maintenance import JournalMaintenanceDialog
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump

    d = ctx.data
    tmp = d["tmpdir"]
    project = os.path.join(tmp, "p.qgz")
    open(project, "w").close()
    jm = JournalManager()
    jm.open_for_project(project, tmp)
    journal = jm.path
    dialog = None
    locker = None
    try:
        _fill_and_free(journal)
        d["size_before"] = os.path.getsize(journal)
        dialog = JournalMaintenanceDialog(jm)
        d["footprint_before"] = _footprint(journal)
        d["v1"] = _drive(dialog)
        d["footprint_after"] = _footprint(journal)
        d["size_after"] = os.path.getsize(journal)
        time.sleep(0.2)
        d["v1_threads_after"] = _vacuum_threads()

        # ---- V2: another connection holds the journal for writing ------
        _fill_and_free(journal, 2000)
        locker = sqlite3.connect(journal, timeout=0.1, isolation_level=None)
        locker.execute("BEGIN EXCLUSIVE")
        d["v2"] = _drive(dialog, timeout=20.0)
        locker.execute("ROLLBACK")
        locker.close()
        locker = None
        pump(0.2)
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    finally:
        if locker is not None:
            try:
                locker.execute("ROLLBACK")
                locker.close()
            except sqlite3.Error:
                pass
        if dialog is not None:
            dialog.close()
            dialog.deleteLater()
            pump(0.1)
        # Give a stuck worker its chance to finish before the directory goes.
        deadline = time.monotonic() + 10
        while _vacuum_threads() and time.monotonic() < deadline:
            time.sleep(0.1)
        jm.close()
    flog(f"tx_maint_vacuum_completion run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:2500]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    v1 = d.get("v1") or {}
    out.append((
        "v1_completion_reaches_the_dialog",
        v1.get("started") is True and v1.get("still_running") is False
        and v1.get("button_enabled") is True and v1.get("progress_visible") is False
        and "Termine" in (v1.get("status") or ""),
        f"V1 {v1} crash={d.get('crash')} {d.get('trace', '')[-500:]} : le "
        f"dialogue doit sortir de 'Compactage en cours...' une fois VACUUM fini",
    ))
    out.append((
        "v1_file_shrank_and_no_worker_left",
        (d.get("size_after") or 0) < (d.get("size_before") or 0)
        and d.get("v1_threads_after") == 0,
        f"taille avant={d.get('size_before')} apres={d.get('size_after')} "
        f"threads RecoverLand-Vacuum vivants={d.get('v1_threads_after')}",
    ))
    reclaimed = (d.get("footprint_before") or 0) - (d.get("footprint_after") or 0)
    try:
        from recoverland.core.health_monitor import _format_size
        shown = _format_size(max(reclaimed, 0))
    except Exception as exc:  # noqa: BLE001 - reported below
        shown = f"<{exc}>"
    infos = [t for kind, t in (v1.get("boxes") or []) if kind == "info"]
    out.append((
        "v3_espace_recupere_annonce_est_celui_mesure",
        reclaimed > 0 and shown in (v1.get("status") or "")
        and any(shown in t for t in infos),
        f"mesure sur le disque avant={d.get('footprint_before')} apres="
        f"{d.get('footprint_after')} soit {reclaimed} octets ({shown}) ; statut "
        f"du dialogue={v1.get('status')!r} ; boite de fin={infos} : le dialogue "
        f"doit annoncer ce qui a reellement ete libere",
    ))
    v2 = d.get("v2") or {}
    out.append((
        "v2_failure_is_told",
        v2.get("started") is True and v2.get("still_running") is False
        and v2.get("button_enabled") is True
        and "Termine" not in (v2.get("status") or "")
        and bool(v2.get("status")),
        f"V2 {v2} : un VACUUM qui echoue (journal verrouille) doit etre dit, "
        f"pas laisse en cours",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_maint_vacuum_completion.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
