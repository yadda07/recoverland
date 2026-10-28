# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_marker_trace_in_effect - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (la barre de dates de Review marque chaque instant
ou l'etat qu'elle dessine change ; rien de ce qui change n'y est cache),
et I-OUTCOME (un travail annule s'arrete).

Le mecanisme
------------
Les marques de la barre de Review viennent de
`widgets/marker_scan_worker.MarkerScanWorker`. Sa requete ecartait toute
ligne invalidee (`invalidated_at IS NULL`). Or Review (snapshot_rebuild
_worker, _READABLE) et le Rewind (rewind_dedup.traces_in_effect) lisent
une TRACE invalidee comme une compensation en vigueur de son created_at a
son invalidated_at : l'Undo n'ecrit aucun evenement, il tamponne les
traces qu'il annule. Review dessine donc un etat different de part et
d'autre de la compensation et de son Undo, sans aucune marque a ces deux
instants. La plage de la barre (query_snapshot_date_range) ecartait aussi
ces traces : un Undo posterieur au dernier evenement tombait hors de la
plage que la barre balaie.

Et `cancel()` ne levait qu'un drapeau lu entre deux couches : un balayage
remplace pendant qu'il lit un gros journal continuait jusqu'au bout, et
l'arret de Review (join_review_thread, borne) abandonnait l'attente avec
un fil encore en train de lire un journal sur le point d'etre ferme.

L'echec concret pour l'utilisateur
----------------------------------
Il a supprime P1, lance un Rewind qui l'a recreee, puis clique Undo. En
Review, la barre ne montre ni la recreation ni l'Undo : P1 apparait puis
disparait a des dates que rien ne signale, il ne peut pas y naviguer par
les marques. Sur un gros journal, chaque deplacement de la carte relance
un balayage que l'annulation n'arrete pas.

Ce que le scenario pilote : un vrai journal (schema du plugin), le vrai
MarkerScanWorker, la vraie plage (query_snapshot_date_range) et le vrai
SnapshotRebuildWorker pour lire l'etat que Review dessine a chaque date.
Pour l'annulation : le vrai worker sur une connexion dont la requete dure
plusieurs secondes, arrete par le vrai join_review_thread.

Verdict attendu avant correctif : FAIL. Apres : PASS.
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

SCENARIO_ID = "rvw_marker_trace_in_effect"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_FP = "ogr::rvw_marker_trace_in_effect.gpkg|layername=pts"
_DAY = "2026-03-01T"


def _ts(hhmm: str) -> str:
    return f"{_DAY}{hhmm}:00.250000+00:00"


class _Journal:
    def __init__(self, path, slow=False):
        self.path = path
        self.is_open = True
        self._slow = slow
        self.entered = threading.Event()

    def create_read_connection(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        if self._slow:
            # The same rows behind a statement that takes seconds: what a
            # large journal on a network share costs the marker scan.
            conn.execute(
                "CREATE TEMP VIEW audit_event AS SELECT * FROM main.audit_event"
                " WHERE (SELECT count(*) FROM (WITH RECURSIVE n(x) AS ("
                "SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 80000000)"
                " SELECT x FROM n)) >= 0")
        self.entered.set()
        return conn


def _point_wkb(x, y) -> bytes:
    from qgis.core import QgsGeometry, QgsPointXY
    return bytes(QgsGeometry.fromPointXY(QgsPointXY(x, y)).asWkb())


def setup(ctx):
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_marks_")
    path = os.path.join(tmpdir, "journal.sqlite")
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    ensure_v7_schema(conn)
    base = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    base.update({"project_fingerprint": "p", "datasource_fingerprint": _FP,
                 "layer_id_snapshot": "l", "layer_name_snapshot": "pts",
                 "provider_type": "ogr", "geometry_type": "Point",
                 "crs_authid": "EPSG:2154", "user_name": "u",
                 "event_schema_version": 7})

    def row(eid, fid, op, at, attrs, geom=None, new_geom=None,
            restored_from=None, invalidated_at=None):
        return dict(base, event_id=eid, feature_identity_json=json.dumps({"fid": fid}),
                    entity_fingerprint=f"fid:{fid}", operation_type=op,
                    created_at=_ts(at), attributes_json=json.dumps(attrs),
                    geometry_wkb=geom, new_geometry_wkb=new_geom,
                    restored_from_event_id=restored_from,
                    invalidated_at=_ts(invalidated_at) if invalidated_at else None)

    p1 = {"all_attributes": {"nom": "P1"}}
    rows = [
        # P1 deleted by the user.
        row(1, 1, "DELETE", "10:00", p1, geom=_point_wkb(1.0, 1.0)),
        # P3 edited, compensated by a rewind, re-applied by a forward hop
        # (an exact trace referencing the trace cancels it).
        row(2, 3, "UPDATE", "10:30", {"changed_only": {"nom": {"old": "a", "new": "b"}}}),
        row(3, 3, "UPDATE", "10:40", {"_restore_ref": 2}, restored_from=2),
        row(4, 3, "UPDATE", "10:50", {"_restore_ref": 3}, restored_from=3),
        # P2 created.
        row(5, 2, "INSERT", "11:00", {"all_attributes": {"nom": "P2"}},
            geom=_point_wkb(2.0, 2.0)),
        # A USER row invalidated: no meaning, never a mark.
        row(6, 3, "UPDATE", "11:10", {"changed_only": {"nom": {"old": "b", "new": "c"}}},
            invalidated_at="11:20"),
        # A rewind re-created P1 at 12:00; the user clicked Undo at 13:00.
        row(7, 1, "INSERT", "12:00", {"_restore_ref": 1}, restored_from=1,
            invalidated_at="13:00"),
    ]
    cols = list(AUDIT_EVENT_COLUMNS)
    with conn:
        conn.executemany(
            f"INSERT INTO audit_event ({', '.join(cols)}) VALUES "  # nosec B608
            f"({', '.join('?' for _ in cols)})",
            [tuple(r[c] for c in cols) for r in rows])
    conn.close()
    ctx.data["tmpdir"] = tmpdir
    ctx.data["path"] = path
    flog(f"rvw_marker_trace_in_effect setup: trace_id={ctx.trace_id} path={path}",
         "INFO")


def _infos():
    return [{"fingerprint": _FP, "layer_name": "pts", "layer_id": "",
             "storage_crs": "EPSG:2154", "fid_stable": True}]


def _review_keys(path, cutoff) -> list:
    """Entity keys Review draws at *cutoff* (the real rebuild worker)."""
    from recoverland.widgets.snapshot_rebuild_worker import SnapshotRebuildWorker
    got = []
    worker = SnapshotRebuildWorker(_Journal(path), _infos(), cutoff)
    worker.result_ready.connect(lambda _t, r: got.append(r))
    worker.error.connect(lambda _t, e: got.append(e))
    worker.run()
    if not got or isinstance(got[0], str):
        return [f"error:{got}"]
    return sorted(k for ents in got[0].features.values() for k in ents)


def _scan(journal, t_min, t_max):
    from qgis.core import QgsRectangle
    from recoverland.widgets.marker_scan_worker import MarkerScanWorker
    worker = MarkerScanWorker(journal, _infos(),
                              {_FP: QgsRectangle(-100, -100, 100, 100)},
                              t_min, t_max, trace_id="marks")
    got = {"markers": [], "errors": []}
    worker.markers_ready.connect(lambda _t, m: got["markers"].extend(m))
    worker.error.connect(lambda _t, e: got["errors"].append(e))
    return worker, got


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.widgets.snapshot_rebuild_worker import (
        join_review_thread, query_snapshot_date_range,
    )
    d = ctx.data
    try:
        d["review"] = {at: _review_keys(d["path"], f"{_DAY}{at}:00")
                       for at in ("11:30", "12:30", "13:30")}
        t_min, t_max = query_snapshot_date_range(_Journal(d["path"]), _infos())
        d["range"] = [t_min, t_max]
        worker, got = _scan(_Journal(d["path"]), t_min, t_max)
        worker.run()
        d["markers"] = [[m[0], m[1]] for m in got["markers"]]
        d["marker_errors"] = got["errors"]

        # Cancel: the real worker on a statement that takes seconds.
        slow = _Journal(d["path"], slow=True)
        worker, got = _scan(slow, t_min, t_max)
        worker.start()
        slow.entered.wait(10.0)
        time.sleep(0.3)
        t0 = time.monotonic()
        joined = join_review_thread(worker, 1500, "marks")
        d["cancel_joined"] = bool(joined)
        d["cancel_ms"] = int((time.monotonic() - t0) * 1000)
        # Never leave a running QThread behind, whatever the verdict.
        worker.wait(60000)
        d["cancel_total_ms"] = int((time.monotonic() - t0) * 1000)
        from qgis.PyQt.QtCore import QCoreApplication
        QCoreApplication.processEvents()
        d["cancel_outcomes"] = {"markers": len(got["markers"]),
                                "errors": list(got["errors"])}
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"rvw_marker_trace_in_effect run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _has(markers, hhmm, op) -> bool:
    return any(m[0][:16] == f"{_DAY}{hhmm}" and m[1] == op for m in markers)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    review = d.get("review") or {}
    markers = d.get("markers") or []
    out = []
    out.append((
        "setup_review_state_changes_at_the_rewind_and_its_undo",
        review.get("11:30") == [] and review.get("12:30") == ["fid:1"]
        and review.get("13:30") == [],
        f"entites dessinees par Review={review} (attendu P1 absente a 11:30, "
        f"presente a 12:30 : recreee par le rewind de 12:00, absente a 13:30 : "
        f"Undo de 13:00){crash}",
    ))
    rng = d.get("range") or ["", ""]
    out.append((
        "bar_range_reaches_the_undo",
        (rng[1] or "")[:16] >= f"{_DAY}13:00",
        f"plage de la barre={rng} : l'Undo de 13:00, posterieur au dernier "
        f"evenement, tombait hors de la plage balayee par les marques{crash}",
    ))
    out.append((
        "bar_marks_the_rewind_and_its_undo",
        _has(markers, "12:00", "INSERT") and _has(markers, "13:00", "DELETE"),
        f"marques={markers} (attendu 12:00 INSERT : la recreation, et "
        f"13:00 DELETE : l'Undo) : Review dessine P1 entre les deux sans "
        f"qu'aucune marque ne le signale{crash}",
    ))
    out.append((
        "marks_keep_user_events_and_a_cancelled_trace_drop_invalid_user_rows",
        all(_has(markers, t, op) for t, op in (
            ("10:00", "DELETE"), ("10:30", "UPDATE"), ("10:40", "UPDATE"),
            ("10:50", "UPDATE"), ("11:00", "INSERT")))
        and not any(m[0][:16] == f"{_DAY}11:10" for m in markers)
        and not any(m[0][:16] == f"{_DAY}11:20" for m in markers)
        and not d.get("marker_errors"),
        f"marques={markers} erreurs={d.get('marker_errors')} (attendu 10:00, "
        f"10:30, 10:40, 10:50, 11:00 ; rien a 11:10/11:20 : une ligne "
        f"utilisateur invalidee ne change rien){crash}",
    ))
    out.append((
        "cancel_stops_the_scan_in_flight",
        d.get("cancel_joined") is True
        and (d.get("cancel_outcomes") or {}).get("markers") == 0
        and not (d.get("cancel_outcomes") or {}).get("errors"),
        f"arret joint en 1,5 s={d.get('cancel_joined')} ({d.get('cancel_ms')} "
        f"ms, fin reelle apres {d.get('cancel_total_ms')} ms) issues="
        f"{d.get('cancel_outcomes')} : un balayage annule continuait a lire "
        f"le journal, l'arret de Review abandonnait l'attente{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_marker_trace_in_effect.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
