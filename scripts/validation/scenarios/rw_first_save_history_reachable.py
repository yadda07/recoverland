"""rw_first_save_history_reachable - RecoverLand validation runtime.

Invariant: I-RW-COVERAGE (un rewind refuse, en le nommant, une date que le
journal ne peut pas garantir ; il ne refuse pas une date qu'il garantit).

Le mecanisme
------------
Le premier enregistrement d'un nouveau projet rattache TOUT le journal de
session (cree par v7 pour ce projet non enregistre, qui n'a jamais tenu
que cette session : `JournalManager.plan_history_carry`, since_event_id=0)
dans un journal de projet NEUF. Ce journal neuf est "created" depuis
l'instant du rattachement : toutes les modifications de la session y sont
plus anciennes que lui. Une regle des lecteurs du plancher qui tenait
toute ligne plus ancienne que le journal pour "non garantie" (ajoutee a
`retention._legacy_floor_refusal`) ne peut pas distinguer ce cas, ou la
source garantissait tout, d'un rattachement partiel : la cible ne garde
aucune trace de ce que sa source garantissait. Elle refusait donc,
apres le premier enregistrement, ce que le journal de session acceptait
juste avant.

Le scenario d'echec concret
---------------------------
Un utilisateur cree un projet, modifie la couche L1 puis la couche L2, et
enregistre le projet pour la premiere fois. Puis :
  * "Annuler toute la session" (recover_dialog._undo_session) : UNE date
    pour toutes les couches, le plus ancien evenement de la session,
    incluse ; le StrictRestoreRunner passe chaque couche au preflight avec
    elle. L1 passe, L2 est refusee ("History of this layer before ... cannot
    be vouched for") : la session est annulee pour une couche et pas pour
    l'autre, alors qu'avant l'enregistrement les deux passaient ;
  * le curseur de date (valeur a la seconde, UTC, sans suffixe de zone,
    comme recover_dialog._recover_version_mode l'envoie) ne peut plus
    atteindre l'etat d'avant la premiere modification : une seconde avant,
    une heure avant, tout est refuse.

Ce que le scenario execute reellement
-------------------------------------
Vrai `JournalManager` : journal de session ouvert par `open_for_project`,
evenements ecrits comme la capture v7 les ecrit (event_uid, commit_id,
`journal_writes.insert_events_idempotent`, registre des commits), vrai
`plan_history_carry` puis `close()` qui fait le rattachement. Puis, dans
le journal de session (avant) et dans le journal du projet (apres), les
questions du rewind : `fetch_events_by_session` et la date de
`_undo_session` (plus ancien evenement par `chronological_key`, incluse),
`plan_temporal_restore` et `preflight_check(plan, conn=...)` par couche,
comme `StrictRestoreRunner._begin_strict_for_layer` ; et
`retention.coverage_refusal` + le meme preflight pour les valeurs du
curseur. Le plancher de Review (`query_snapshot_floor`) est journalise a
titre d'information (il garde, lui, son plancher prudent sur une ligne
plus ancienne que le journal).

Verdicts attendus
-----------------
Avant correctif (arbre de travail avec la branche finale de
`_legacy_floor_refusal` qui compare la plus ancienne ligne au debut du
journal) : FAIL sur undo_session_every_layer_after_first_save et
slider_reaches_the_state_before_the_first_edit.
Apres (branche retenue) : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_first_save_history_reachable"
INVARIANT = "I-RW-COVERAGE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_KEYS = ("coverage_recorded_since", "coverage_origin", "coverage_origin_schema")
_SESSION = "s_rw_fshr"
_L1 = "rw_fshr_l1"
_L2 = "rw_fshr_l2"
# The dialog's cutoff: UTC, to the second, no zone suffix.
_DIALOG_FORMAT = "%Y-%m-%dT%H:%M:%S"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _capture(path: str, fingerprint: str, created_at: str, new: str) -> None:
    """One UPDATE (nom A -> *new*) written as the v7 capture writes it."""
    from recoverland.core.capture_outbox import new_commit_id, new_event_uid
    from recoverland.core.journal_writes import (
        insert_events_idempotent, record_capture_commit)
    commit_id = new_commit_id()
    row = {"project_fingerprint": "", "datasource_fingerprint": fingerprint,
           "layer_id_snapshot": fingerprint, "layer_name_snapshot": fingerprint,
           "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": 1}),
           "operation_type": "UPDATE",
           "attributes_json": json.dumps(
               {"changed_only": {"nom": {"old": "A", "new": new}}}),
           "geometry_type": "NoGeometry", "user_name": "u",
           "session_id": _SESSION, "created_at": created_at,
           "entity_fingerprint": "fid:1", "event_schema_version": 2,
           "event_uid": new_event_uid(), "commit_id": commit_id}
    conn = sqlite3.connect(path)
    try:
        with conn:
            inserted, _already, rejected = insert_events_idempotent(conn, [row])
            if inserted != 1 or rejected:
                raise RuntimeError(f"capture of {fingerprint} not written: "
                                   f"inserted={inserted} rejected={rejected}")
            record_capture_commit(conn, commit_id, "COMMITTED", layer_id=fingerprint,
                                  layer_name=fingerprint,
                                  datasource_fingerprint=fingerprint,
                                  session_id=_SESSION, n_events=1)
    finally:
        conn.close()


def _stamp(path: str) -> dict:
    conn = sqlite3.connect(path)
    try:
        rows = dict(conn.execute(
            "SELECT setting_key, setting_value FROM backend_settings "
            "WHERE setting_key IN (?, ?, ?)", _KEYS))
    except sqlite3.Error as exc:
        return {"error": str(exc)}
    finally:
        conn.close()
    return {k: rows.get(k) for k in _KEYS}


def _undo_session(path: str) -> dict:
    """What "Annuler toute la session" asks, per layer, of *path*.

    The dialog's cutoff (_undo_session: the session's oldest event,
    inclusive) and the runner's preflight (_begin_strict_for_layer: the
    layer's plan, preflight_check with the journal's connection).
    """
    from recoverland.core.event_stream_repository import fetch_events_by_session
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.restore_planner import plan_temporal_restore, preflight_check
    from recoverland.core.rewind_dedup import chronological_key
    conn = sqlite3.connect(path)
    try:
        events = fetch_events_by_session(conn, _SESSION)
        oldest = min(events, key=chronological_key)
        cutoff = RestoreCutoff(CutoffType.BY_DATE, oldest.created_at or "",
                               inclusive=True)
        by_ds: dict = {}
        for e in events:
            by_ds.setdefault(e.datasource_fingerprint, []).append(e)
        layers = {}
        for fingerprint, group in sorted(by_ds.items()):
            plan = plan_temporal_restore(group, fingerprint, fingerprint, cutoff)
            report = preflight_check(plan, conn=conn)
            layers[fingerprint] = {
                "verdict": getattr(report.verdict, "name", str(report.verdict)),
                "actions": len(plan.actions),
                "blocking": [b[:220] for b in report.blocking_reasons]}
    finally:
        conn.close()
    return {"cutoff": cutoff.value, "n_events": len(events), "layers": layers}


def _slider(path: str, fingerprint: str, cutoffs: dict) -> dict:
    """coverage_refusal and the rewind's preflight for each slider value."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.restore_planner import plan_temporal_restore, preflight_check
    from recoverland.core.retention import coverage_refusal
    from recoverland.core.rewind_dedup import collapse_rewind_events
    out = {}
    conn = sqlite3.connect(path)
    try:
        for label, value in cutoffs.items():
            cutoff = RestoreCutoff(CutoffType.BY_DATE, value, inclusive=True)
            events = collapse_rewind_events(
                fetch_events_after_cutoff(conn, fingerprint, cutoff))
            plan = plan_temporal_restore(events, fingerprint, fingerprint, cutoff)
            report = preflight_check(plan, conn=conn)
            out[label] = {
                "cutoff": value,
                "refusal": coverage_refusal(conn, fingerprint, value),
                "verdict": getattr(report.verdict, "name", str(report.verdict)),
                "actions": len(plan.actions),
                "blocking": [b[:220] for b in report.blocking_reasons]}
    finally:
        conn.close()
    return out


class _Journal:
    """What query_snapshot_floor needs of a journal: a read connection."""

    def __init__(self, path: str):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_rw_fshr_")
    ctx.data["tmp"] = tmp
    flog(f"rw_first_save_history_reachable setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.journal_manager import JournalManager
    from recoverland.widgets.snapshot_rebuild_worker import (
        query_snapshot_floor, second_before)
    d = ctx.data
    tmp = d["tmp"]
    profile = os.path.join(tmp, "profil")
    os.makedirs(profile, exist_ok=True)
    jm = None
    try:
        # A new, unsaved project: its session journal.
        jm = JournalManager()
        session = jm.open_for_project("", profile)
        time.sleep(0.05)
        t1 = _now()
        _capture(session, _L1, t1, "X1")
        time.sleep(0.05)
        t2 = _now()
        _capture(session, _L2, t2, "Y1")
        first = datetime.fromisoformat(t1)
        slider = {
            "one_second_before_first_edit": second_before(t1),
            "an_hour_before_first_edit": (
                first - timedelta(hours=1)).strftime(_DIALOG_FORMAT),
        }
        d["t1"], d["t2"], d["slider_values"] = t1, t2, slider
        d["before"] = {"stamp": _stamp(session), "undo": _undo_session(session),
                       "slider": _slider(session, _L1, slider)}
        # First save: the whole session journal follows into the new
        # project's journal (plan at the save, carry at close).
        pdir = os.path.join(tmp, "nouveau_projet")
        os.makedirs(pdir, exist_ok=True)
        project = os.path.join(pdir, "projet.qgs")
        Path(project).write_text("<qgis/>", encoding="utf-8")
        carry = jm.plan_history_carry(project, profile)
        jm.close()
        report = jm.last_carry_report
        jm = None
        target = os.path.join(pdir, ".recoverland", "recoverland_audit.sqlite")
        if carry is not None:
            target = carry.target
        d["carry"] = {"planned": carry is not None,
                      "since_event_id": getattr(carry, "since_event_id", None),
                      "source_kind": getattr(carry, "source_kind", None),
                      "ok": getattr(report, "ok", None),
                      "error": getattr(report, "error", None)}
        d["after"] = {"stamp": _stamp(target), "undo": _undo_session(target),
                      "slider": _slider(target, _L1, slider),
                      "review_floor_info": {
                          fp: query_snapshot_floor(_Journal(target),
                                                   [{"fingerprint": fp}])
                          for fp in (_L1, _L2)}}
        conn = sqlite3.connect(target)
        try:
            d["after"]["rows"] = conn.execute(
                "SELECT datasource_fingerprint, created_at, event_uid FROM "
                "audit_event ORDER BY event_id").fetchall()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if jm is not None:
            try:
                jm.close()
            except Exception:  # noqa: BLE001 - teardown removes the files
                pass
    flog(f"rw_first_save_history_reachable run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    before, after = d.get("before") or {}, d.get("after") or {}
    carry = d.get("carry") or {}
    sa = after.get("stamp") or {}
    rows = after.get("rows") or []
    out = []
    out.append((
        "setup_first_save_carries_the_whole_session",
        carry.get("planned") is True and carry.get("since_event_id") == 0
        and carry.get("ok") is True and sa.get("coverage_origin") == "created"
        and (before.get("stamp") or {}).get("coverage_origin") == "created"
        and sorted(r[0] for r in rows) == sorted([_L1, _L2])
        and all(r[1] < (sa.get("coverage_recorded_since") or "") for r in rows),
        f"rattachement={carry} (attendu : planifie, since_event_id=0) ; tampon du "
        f"journal de session={before.get('stamp')} ; tampon du journal du projet="
        f"{sa} (attendu created, depuis un instant posterieur aux deux "
        f"modifications) ; lignes rattachees={rows}{crash}",
    ))
    ub, ua = before.get("undo") or {}, after.get("undo") or {}
    lb, la = ub.get("layers") or {}, ua.get("layers") or {}

    def _go(layers, fp):
        info = layers.get(fp) or {}
        return (info.get("verdict") == "GO" and info.get("actions") == 1
                and not info.get("blocking"))
    out.append((
        "undo_session_every_layer_before_save",
        ub.get("n_events") == 2 and _go(lb, _L1) and _go(lb, _L2),
        f"journal de session, 'Annuler toute la session' a {ub.get('cutoff')} "
        f"(incluse) : {lb} (attendu GO, une action, aucun blocage, pour les "
        f"deux couches){crash}",
    ))
    out.append((
        "undo_session_every_layer_after_first_save",
        ua.get("n_events") == 2 and _go(la, _L1) and _go(la, _L2),
        f"journal du projet apres le premier enregistrement, 'Annuler toute la "
        f"session' a {ua.get('cutoff')} (le plus ancien evenement de la "
        f"session, incluse) : {la}. Attendu : GO pour les deux couches, comme "
        f"dans le journal de session juste avant. Refusee pour {_L2} (modifiee "
        f"apres {_L1}), la session est annulee pour une couche et pas pour "
        f"l'autre, et l'utilisateur ne peut plus annuler sa session apres "
        f"l'avoir enregistree{crash}",
    ))
    sb, sl = before.get("slider") or {}, after.get("slider") or {}

    def _reachable(answers):
        return bool(answers) and all(
            a.get("refusal") is None and a.get("verdict") == "GO"
            and a.get("actions") == 1 and not a.get("blocking")
            for a in answers.values())
    out.append((
        "slider_reaches_the_state_before_the_first_edit",
        len(sl) == 2 and _reachable(sb) and _reachable(sl),
        f"curseur de date sur {_L1} (valeurs {d.get('slider_values')}, format du "
        f"dialogue) ; journal de session : {sb} ; journal du projet : {sl}. "
        f"Attendus : aucun refus, GO et une action, dans les deux journaux. "
        f"Refusees apres l'enregistrement, aucune valeur du curseur n'atteint "
        f"plus l'etat d'avant la premiere modification de la session, que le "
        f"journal de session rendait{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_first_save_history_reachable.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
