# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_service_history_notice - l'historique rattache par hypothese est dit, le refuse est rattachable.

Invariant : "rien n'est cache" et I-3 (un historique n'est rattache a une
base que sur une preuve, ou sur une decision de l'utilisateur).

Le mecanisme
------------
Avant v7, une empreinte de base de donnees ne notait pas le SERVICE de
connexion. La migration (`datasource_alias`) :

  * rattache l'historique au seul service connu, et le SIGNALE comme une
    hypothese (`ReconciliationReport.assumed`, `needs_notice`) ;
  * refuse de le rattacher quand plusieurs services sont connus
    (`service_history_candidates`), et sait le rattacher au service que
    l'utilisateur designe (`attach_service_history`, defait par
    `remove_alias`).

Le dialogue ne parlait que d'un rapprochement DEGRADE
(`is_reconciliation_degraded`) : l'hypothese n'etait jamais montree. Et
rien ne permettait a l'utilisateur de rattacher un historique refuse : le
message disait qu'il "peut etre rattache au service que vous designez",
sans aucun moyen de le designer. Enfin, une couche refusee comme cible
pour cette raison etait annoncee "non trouvee dans le projet", alors
qu'elle y est (`workflow_service.target_refusal_reason` savait pourquoi).

Ce que l'utilisateur voyait
---------------------------
Un rewind rejoue sur 'prod' l'historique d'avant v7 venu peut-etre de
'test', sans un mot ; ou son historique ancien reste hors de tout rewind
et il n'a aucun moyen de le rattacher ; ou "Couche 'routes' non trouvee"
devant une couche chargee.

Cas (vrai journal ouvert par JournalManager, vraie migration)
------------------------------------------------------------
S1  un seul service (prod) : la notice du projet et celle du rewind disent
    le rattachement par hypothese.
S2  deux services (prod, test) : la notice nomme l'historique refuse et
    les services connus ; le menu d'un de ses evenements propose de le
    rattacher ; le rattacher a 'prod' met ses evenements dans la portee
    d'un rewind de prod ; le menu propose alors de retirer ce
    rattachement, qui le rend de nouveau refuse et rattachable.
S3  une couche refusee comme cible est nommee avec la raison, par le
    runner comme par la verification prealable du dialogue.

Verdict avant correctif : FAIL (S1, S2, S3). Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_service_history_notice"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_T0 = datetime(2026, 9, 20, 9, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()
_LEGACY = "postgres::host= port=5432 dbname= schema=public table=routes"
_PROD = ("postgres::service=prod host= port=5432 dbname= schema=public "
         "table=routes")
_URI_PROD = "service='prod' key='gid' table=\"public\".\"routes\" (geom)"
_REFUSAL = ("la couche 'routes' passe par le service de connexion 'prod', "
            "que l'evenement (capture avant v7) n'a pas enregistre : il peut "
            "venir d'une autre base de donnees. Rattachez l'historique a ce "
            "service pour le restaurer ici")


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _seed(db_path):
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.datasource_alias import RECONCILE_SETTING_KEY
    conn = sqlite3.connect(db_path)
    try:
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
                  + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
        now = _T0.isoformat()
        for idx, gid in enumerate((5, 7, 9)):
            conn.execute(insert, (
                "proj_svc", _LEGACY, "lyr_routes", "routes", "postgres",
                json.dumps({"fid": gid, "pk_field": "gid", "pk_value": gid}),
                "UPDATE", json.dumps({"changed_only": {"nom": {
                    "old": f"n{gid}", "new": f"n{gid}b"}}}),
                None, "LineString", "EPSG:2154", None, "tester", "sess",
                (_T0 + timedelta(minutes=idx)).isoformat(), None,
                f"pk:gid={gid}", 6, None, None))
        conn.execute(
            "INSERT INTO datasource_registry (datasource_fingerprint, "
            "provider_type, source_uri, layer_name, authcfg, crs_authid, "
            "geometry_type, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
            (_LEGACY, "postgres", _URI_PROD, "routes", "", "EPSG:2154",
             "LineString", now))
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES (?, ?, ?)",
            (RECONCILE_SETTING_KEY, json.dumps({
                "status": "ok", "scanned": 1, "aliases_created": 0,
                "already_canonical": 1, "already_aliased": 0, "ambiguous": [],
                "without_source_uri": [], "completed_at": now}), now))
        conn.execute("INSERT OR REPLACE INTO schema_version (version_number, "
                     "applied_at, description) VALUES (6, ?, 'fixture v6')",
                     (now,))
        conn.commit()
    finally:
        conn.close()


def _open_case(root, name, services):
    """A project journal opened by the real JournalManager (migration runs)."""
    from recoverland.core.journal_manager import JournalManager
    case_dir = os.path.join(root, name)
    os.makedirs(os.path.join(case_dir, "sysconf"), exist_ok=True)
    service_file = os.path.join(case_dir, "pg_service.conf")
    with open(service_file, "w", encoding="utf-8") as fh:
        for section in services:
            fh.write(f"[{section}]\nhost=db-{section}.example\n"
                     f"dbname=gis\nport=5432\n\n")
    os.environ["PGSERVICEFILE"] = service_file
    os.environ["PGSYSCONFDIR"] = os.path.join(case_dir, "sysconf")
    project = os.path.join(case_dir, "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    db = os.path.join(case_dir, ".recoverland", "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db), exist_ok=True)
    _seed(db)
    jm = JournalManager()
    jm.open_for_project(project, case_dir)
    return jm


def _scope_count(jm, fingerprint):
    from recoverland.core.event_stream_repository import count_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    conn = jm.create_read_connection()
    try:
        return count_events_after_cutoff(
            conn, fingerprint, RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True),
            trace_id="rw_shn")
    finally:
        conn.close()


class _Menu:
    def __init__(self):
        self.labels = []

    def addSeparator(self):  # noqa: N802
        return None

    def addAction(self, *args):  # noqa: N802
        label = str(args[-1])
        self.labels.append(label)
        h = _h()
        action = h.Widget(label)
        action.triggered = h.Signal()
        return action


def _legacy_event(jm):
    from recoverland.core.event_stream_repository import fetch_events_by_ids
    conn = jm.create_read_connection()
    try:
        eid = conn.execute("SELECT MIN(event_id) FROM audit_event").fetchone()[0]
        return fetch_events_by_ids(conn, [eid])[0]
    finally:
        conn.close()


def _case_s1(ctx, root):
    import recoverland.recover_dialog as rd
    h = _h()
    jm = _open_case(root, "s1", ("prod",))
    dlg = h.build_dialog(rd, jm, h.RecordingWriteQueue(jm.path), [_PROD])
    logs = []
    seams = h.Seams()
    try:
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        ctx.data["s1_degraded"] = bool(jm.is_reconciliation_degraded)
        ctx.data["s1_project"] = dlg._warn_if_reconciliation_degraded("project")
        n = len(logs)
        ctx.data["s1_rewind"] = dlg._warn_if_reconciliation_degraded("rewind")
        ctx.data["s1_logs"] = [m for m, _l in logs[:n]]
        ctx.data["s1_rewind_logs"] = [m for m, _l in logs[n:]]
    finally:
        seams.restore()
        h.close_dialog(dlg)
        jm.close()


def _case_s2(ctx, root):
    import recoverland.recover_dialog as rd
    import qgis.PyQt.QtWidgets as qtw
    h = _h()
    jm = _open_case(root, "s2", ("prod", "test"))
    dlg = h.build_dialog(rd, jm, h.RecordingWriteQueue(jm.path), [_PROD])
    logs, asked, offered = [], [], []

    class _Input:
        @staticmethod
        def getItem(_parent, title, label, items, *args):  # noqa: N802
            offered.append((str(title), list(items)))
            return "prod", True

    seams = h.Seams()
    try:
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(qtw, "QInputDialog", _Input)
        event = _legacy_event(jm)
        ctx.data["s2_rewind"] = dlg._warn_if_reconciliation_degraded("rewind")
        ctx.data["s2_rewind_logs"] = [m for m, _l in logs]
        menu = _Menu()
        dlg._append_service_history_menu(menu, event)
        ctx.data["s2_menu_before"] = list(menu.labels)
        ctx.data["s2_scope_before"] = _scope_count(jm, _PROD)
        ctx.data["s2_attached"] = dlg._attach_service_history(_LEGACY)
        ctx.data["s2_offered"] = list(offered)
        ctx.data["s2_scope_after"] = _scope_count(jm, _PROD)
        menu = _Menu()
        dlg._append_service_history_menu(menu, event)
        ctx.data["s2_menu_after"] = list(menu.labels)
        ctx.data["s2_detached"] = dlg._detach_service_history(_LEGACY)
        ctx.data["s2_scope_detached"] = _scope_count(jm, _PROD)
        menu = _Menu()
        dlg._append_service_history_menu(menu, event)
        ctx.data["s2_menu_detached"] = list(menu.labels)
        ctx.data["s2_logs"] = [m[:300] for m, _l in logs]
    finally:
        seams.restore()
        h.close_dialog(dlg)
        jm.close()


def _case_s3(ctx, root):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    import recoverland.core.workflow_service as ws
    h = _h()
    journal = h.new_journal(root, "s3.sqlite")
    e1 = h.insert_event(journal, _LEGACY, "routes", "UPDATE", 10, fid=5)
    (event,) = h.fetch_events(journal, [e1])

    def refusing_find(evt, _conn=None):
        # What find_target_layer does when the loaded layer goes through a
        # service the event did not record: refused, and the reason kept.
        ws._remember_refusal(evt, [_REFUSAL])
        return None

    results = []
    runner = rr.RestoreRunner([event], refusing_find, None, tracker=None,
                              trace_id="rw_shn_s3")
    runner.finished.connect(results.append)
    runner.start()
    h.pump_until(lambda: bool(results), 5.0)
    ctx.data["s3_runner_errors"] = list(getattr(results[0], "errors", []) or []) \
        if results else None
    dlg = h.build_dialog(rd, h.Journal(journal), h.RecordingWriteQueue(journal),
                         [_LEGACY])
    seams = h.Seams()
    try:
        seams.set(rd, "find_target_layer", refusing_find)
        _warnings, blocking = rd.RecoverDialog._preflight_layer_checks(
            dlg, [event])
    finally:
        seams.restore()
        h.close_dialog(dlg)
    ctx.data["s3_preflight"] = list(blocking)
    src = (_PLUGIN_ROOT / "recover_dialog.py").read_text(encoding="utf-8")
    body = src.split("def _show_results_context_menu(", 1)[-1].split(
        "\n    def ", 1)[0]
    ctx.data["s3_menu_wired"] = "_append_service_history_menu(menu, event)" in body


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["saved_env"] = {k: os.environ.get(k)
                             for k in ("PGSERVICEFILE", "PGSYSCONFDIR")}
    ctx.data["root"] = tempfile.mkdtemp(prefix="rl_rw_shn_")
    flog(f"rw_service_history_notice setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    root = ctx.data["root"]
    ctx.data["errors"] = {}
    try:
        for name, case in (("s1", _case_s1), ("s2", _case_s2), ("s3", _case_s3)):
            try:
                case(ctx, root)
            except Exception as exc:  # noqa: BLE001 - a crash is a result
                import traceback
                ctx.data["errors"][name] = (f"{type(exc).__name__}: {exc} "
                                            f"{traceback.format_exc()[-900:]}")
    finally:
        _restore_env(ctx.data)
    flog(f"rw_service_history_notice run: trace_id={ctx.trace_id} "
         f"s1={ctx.data.get('s1_logs')} s2={ctx.data.get('s2_logs')} "
         f"errors={ctx.data['errors']}")


def _restore_env(d):
    for key, value in (d.get("saved_env") or {}).items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def teardown(ctx):
    _restore_env(ctx.data)
    shutil.rmtree(ctx.data.get("root") or "", ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", not d.get("errors"), f"crash={d.get('errors')}")]
    s1 = " ".join(d.get("s1_logs") or [])
    s1r = " ".join(d.get("s1_rewind_logs") or [])
    out.append((
        "s1_assumed_attachment_shown_at_project_open",
        d.get("s1_degraded") is False and d.get("s1_project") is True
        and "hypothese" in s1 and "prod" in s1,
        f"rapprochement non degrade={not d.get('s1_degraded')} notice="
        f"{d.get('s1_project')} message={d.get('s1_logs')} : l'historique "
        f"d'avant v7 a ete rattache a 'prod' parce que c'est le seul service "
        f"connu ; si c'est faux, un rewind rejoue sur 'prod' des editions "
        f"d'une autre base. L'utilisateur doit le lire.",
    ))
    out.append((
        "s1_rewind_notice_says_hypothesis",
        d.get("s1_rewind") is True and "rattache par hypothese" in s1r,
        f"notice au lancement d'un rewind={d.get('s1_rewind_logs')} : elle "
        f"doit dire que l'historique est rattache par hypothese (pas "
        f"'partiellement rattache', ce qui est faux ici).",
    ))
    s2r = " ".join(d.get("s2_rewind_logs") or [])
    out.append((
        "s2_refused_history_named_with_known_services",
        d.get("s2_rewind") is True and "services connus : prod, test" in s2r
        and "Rattacher cet historique a un service" in s2r,
        f"notice du rewind={d.get('s2_rewind_logs')} : elle doit nommer "
        f"l'historique refuse, les services connus, et le moyen de le "
        f"rattacher.",
    ))
    out.append((
        "s2_menu_offers_attach",
        any("Rattacher cet historique" in lbl
            for lbl in d.get("s2_menu_before") or []),
        f"menu d'un evenement de cet historique={d.get('s2_menu_before')}.",
    ))
    out.append((
        "s2_attach_puts_the_history_in_the_rewind_scope",
        d.get("s2_attached") is True and d.get("s2_scope_before") == 0
        and d.get("s2_scope_after") == 3
        and (d.get("s2_offered") or [(None, [])])[0][1] == ["prod", "test"],
        f"services proposes={d.get('s2_offered')} rattache="
        f"{d.get('s2_attached')} portee d'un rewind de prod avant="
        f"{d.get('s2_scope_before')} apres={d.get('s2_scope_after')} "
        f"(attendu 0 puis 3) messages={d.get('s2_logs')}.",
    ))
    out.append((
        "s2_attachment_can_be_taken_back",
        any("Retirer le rattachement" in lbl
            for lbl in d.get("s2_menu_after") or [])
        and d.get("s2_detached") is True and d.get("s2_scope_detached") == 0
        and any("Rattacher cet historique" in lbl
                for lbl in d.get("s2_menu_detached") or []),
        f"menu apres rattachement={d.get('s2_menu_after')} retire="
        f"{d.get('s2_detached')} portee apres retrait="
        f"{d.get('s2_scope_detached')} menu apres retrait="
        f"{d.get('s2_menu_detached')} : une decision de l'utilisateur doit "
        f"pouvoir etre defaite, et l'historique redevient rattachable.",
    ))
    runner_errors = " ".join(d.get("s3_runner_errors") or [])
    preflight = " ".join(d.get("s3_preflight") or [])
    out.append((
        "s3_refused_target_named_with_its_reason",
        "refusee comme cible" in runner_errors and "service" in runner_errors
        and "refusee comme cible" in preflight and "service" in preflight,
        f"runner={d.get('s3_runner_errors')} verification prealable="
        f"{d.get('s3_preflight')} : 'non trouvee dans le projet' envoie "
        f"l'utilisateur chercher une couche qu'il a sous les yeux.",
    ))
    out.append((
        "s3_results_menu_wired",
        d.get("s3_menu_wired") is True,
        "le menu des resultats doit appeler _append_service_history_menu "
        "(garde de source : le menu Qt ne se construit pas sur la doublure).",
    ))
    return out
