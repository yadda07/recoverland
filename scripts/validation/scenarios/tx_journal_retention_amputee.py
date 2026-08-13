"""tx_journal_retention_amputee - RecoverLand validation runtime.

Invariant: I-JRN / I-8 (ce que le journal ne contient plus doit etre annonce,
jamais rejoue a moitie).

Le mecanisme
------------
Trois portes par lesquelles des evenements quittent le journal sans que
personne ne le dise.

1. LA PURGE PAR EXCES COUPE LES COMMITS EN DEUX
   `retention._purge_excess` supprime les plus anciens jusqu'a redescendre
   sous `max_events` ::

       DELETE FROM audit_event WHERE event_id IN (
         SELECT event_id FROM audit_event
         ORDER BY created_at ASC, event_id ASC LIMIT ?)

   Le nombre a supprimer est `total - max_events`, un nombre d'evenements,
   sans aucun egard pour la frontiere des commits. Une session d'edition qui
   a modifie 4 entites au meme instant peut donc se retrouver avec 2
   evenements supprimes et 2 conserves. Et ce n'est pas une operation
   exotique : `recover._run_auto_purge_if_enabled` lance
   `purge_old_events` AU DEMARRAGE de QGIS, sans confirmation, des que
   l'option de purge automatique est cochee.

2. LE JOURNAL AMPUTE N'INTERDIT PLUS RIEN
   `restore_planner.check_retention_coverage` existe et fait exactement le
   bon travail : elle refuse une date de rewind anterieure au plus ancien
   evenement encore present. Elle n'est appelee NULLE PART dans le plugin
   (seulement re-exportee par `core/__init__.py`). Le pipeline de rewind
   (fetch -> plan -> preflight) accepte donc une date que le journal ne
   couvre plus, rejoue ce qui reste, et annonce un succes.

3. LA WRITEQUEUE ARRETEE ACCEPTE ENCORE
   `WriteQueue.enqueue` ne consulte jamais `self._running`. Apres `stop()`,
   ou avant tout `start()`, elle valide les evenements, les met dans une
   `queue.Queue` que plus personne ne draine, et renvoie True. Aucun
   `_save_lost_events`, aucun fichier d'attente. Les evenements sont
   acceptes puis perdus a la fermeture du processus.

Le scenario d'echec concret
---------------------------
Un utilisateur a coche la purge automatique avec un plafond d'evenements.
Un matin, QGIS purge au demarrage et coupe en deux la session ou 4
parcelles avaient ete modifiees ensemble. L'apres-midi il demande un REWIND
a la veille : le plugin annonce "restauration reussie", 2 parcelles
reviennent en arriere, les 2 autres restent modifiees. Rien ne distingue
visuellement les deux moities. C'est exactement le symptome "le rewind ne
revient pas toujours en arriere, il y a toujours des trous", mais du cote
du stockage cette fois.

Ce que le scenario execute reellement
-------------------------------------
    setup : un journal SQLite sur disque, 3 commits de 4 evenements chacun,
            horodatage identique a l'interieur d'un commit
    run A : purge_old_events(policy(max_events=6)) - le vrai chemin de la
            purge automatique au demarrage
    run B : rewind demande a une date anterieure a tout le journal restant :
            fetch -> collapse -> plan_temporal_restore -> preflight_check
    run C : WriteQueue jamais demarree, puis demarree/arretee, puis
            enqueue()
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

SCENARIO_ID = "tx_journal_retention_amputee"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_jra_datasource"
_T0 = datetime(2026, 8, 10, 8, 0, 0, tzinfo=timezone.utc)
_MAX_EVENTS = 6

# 3 commits x 4 entites. Chaque commit porte UN seul horodatage : c'est ce
# que produit une validation de couche ou une reprise depuis le fichier
# d'attente, ou tous les evenements sont ecrits d'un bloc.
_COMMITS = [
    ("sess_lundi", _T0, [101, 102, 103, 104]),
    ("sess_mardi", _T0 + timedelta(days=1), [201, 202, 203, 204]),
    ("sess_mercredi", _T0 + timedelta(days=2), [301, 302, 303, 304]),
]


def _seed(db_path: str) -> None:
    from recoverland.core.sqlite_schema import (
        initialize_schema, AUDIT_EVENT_INSERT_SQL,
        AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    conn = sqlite3.connect(db_path)
    initialize_schema(conn)
    sql = "".join((
        "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
        ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
    ))
    for session_id, ts, fids in _COMMITS:
        stamp = ts.isoformat()
        for fid in fids:
            conn.execute(sql, (
                "tx_jra_project", _DS_FP, "lyr_jra", "parcelles", "ogr",
                json.dumps({"fid": fid}), "UPDATE",
                json.dumps({"changed_only": {
                    "proprio": {"old": "DUPONT", "new": "MARTIN"}}}),
                None, "NoGeometry", "EPSG:2154", None,
                "tester", session_id, stamp, None, f"fid:{fid}", 2,
                None, None,
            ))
    conn.commit()
    conn.close()


def _snapshot(conn: sqlite3.Connection) -> dict:
    rows = conn.execute(
        "SELECT created_at, session_id, entity_fingerprint FROM audit_event "
        "ORDER BY created_at, entity_fingerprint"
    ).fetchall()
    by_commit = {}
    for created_at, session_id, fp in rows:
        by_commit.setdefault((created_at, session_id), []).append(fp)
    return {
        "total": len(rows),
        "commits": {k[1]: sorted(v) for k, v in by_commit.items()},
    }


def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_jra_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    _seed(db_path)
    conn = sqlite3.connect(db_path)
    ctx.data["tmpdir"] = tmpdir
    ctx.data["db_path"] = db_path
    ctx.data["conn"] = conn
    ctx.data["before"] = _snapshot(conn)

    flog(
        f"tx_journal_retention_amputee setup: trace_id={ctx.trace_id} "
        f"db={db_path} total={ctx.data['before']['total']} "
        f"commits={ctx.data['before']['commits']} max_events={_MAX_EVENTS}",
        "INFO",
    )


def _run_purge(ctx):
    from recoverland.core.retention import purge_old_events, RetentionPolicy
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    policy = RetentionPolicy(retention_days=365, max_events=_MAX_EVENTS)
    result = purge_old_events(conn, policy, trace_id=ctx.trace_id)
    ctx.data["purge"] = {
        "deleted_count": result.deleted_count,
        "retention_deleted": result.retention_deleted,
        "excess_deleted": result.excess_deleted,
        "error": result.error,
    }
    ctx.data["after_purge"] = _snapshot(conn)
    flog(
        f"tx_journal_retention_amputee purge: trace_id={ctx.trace_id} "
        f"result={ctx.data['purge']} after={ctx.data['after_purge']}",
        "INFO",
    )


def _run_rewind_on_amputated(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff, get_oldest_event_date,
    )
    from recoverland.core.restore_contracts import (
        RestoreCutoff, CutoffType, RestoreScope,
    )
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import (
        plan_temporal_restore, preflight_check, check_retention_coverage,
    )
    from recoverland.core.logger import flog

    conn = ctx.data["conn"]
    # Une date anterieure a TOUT le journal, y compris a ce qui a ete purge.
    cutoff_value = (_T0 - timedelta(hours=1)).isoformat()
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_value, inclusive=True)

    oldest = get_oldest_event_date(conn, _DS_FP)
    events = fetch_events_after_cutoff(
        conn, _DS_FP, cutoff, trace_id=ctx.trace_id)
    active, stats = collapse_rewind_events_with_stats(events)
    plan = plan_temporal_restore(
        active, _DS_FP, "parcelles", cutoff, scope=RestoreScope.LAYER)
    report = preflight_check(plan)

    ctx.data["rewind"] = {
        "cutoff": cutoff_value,
        "oldest_remaining": oldest,
        "coverage_reason": check_retention_coverage(cutoff, oldest),
        "n_fetched": len(events),
        "n_actions": len(plan.actions),
        "entities": sorted({
            e.entity_fingerprint for e in active if e.entity_fingerprint}),
        "verdict": str(report.verdict),
        "blocking": list(report.blocking_reasons),
        "warnings": list(report.warnings),
        "stats": dict(stats),
    }
    flog(
        f"tx_journal_retention_amputee rewind: trace_id={ctx.trace_id} "
        f"cutoff={cutoff_value} oldest={oldest} "
        f"n_fetched={len(events)} verdict={report.verdict} "
        f"blocking={report.blocking_reasons}",
        "INFO",
    )


def _event(idx: int):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=idx,
        project_fingerprint="tx_jra_project",
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="lyr_jra",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json=json.dumps({"fid": idx}),
        operation_type="UPDATE",
        attributes_json=json.dumps({"changed_only": {
            "proprio": {"old": "DUPONT", "new": "MARTIN"}}}),
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:2154",
        field_schema_json=None,
        user_name="tester",
        session_id="sess_wq",
        created_at=(_T0 + timedelta(days=10, seconds=idx)).isoformat(),
        restored_from_event_id=None,
        entity_fingerprint=f"fid:{idx}",
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _wait_drained(wq, timeout=3.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if wq.pending_count == 0:
            time.sleep(0.15)
            return True
        time.sleep(0.02)
    return False


def _count_rows(db_path: str, layer: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE session_id = 'sess_wq'"
        ).fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def _run_write_queue(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.integrity import _get_pending_path
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_jra_wq_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    _seed(db_path)
    pending_path = _get_pending_path(db_path)

    # --- C1 : jamais demarree ------------------------------------------
    never = WriteQueue()
    res_never = never.enqueue([_event(901), _event(902)])
    never_pending = never.pending_count
    never_file = os.path.isfile(pending_path)

    # --- C2 : demarree, arretee, puis sollicitee -------------------------
    wq = WriteQueue()
    wq.start(db_path)
    res_live = wq.enqueue([_event(801), _event(802)])
    drained = _wait_drained(wq)
    rows_live = _count_rows(db_path, "parcelles")
    wq.stop()

    res_after_stop = wq.enqueue([_event(811), _event(812), _event(813)])
    time.sleep(0.4)
    rows_after_stop = _count_rows(db_path, "parcelles")
    queued_after_stop = wq.pending_count
    pending_after_stop = os.path.isfile(pending_path)
    pending_entries = 0
    if pending_after_stop:
        try:
            data = json.loads(Path(pending_path).read_text(encoding="utf-8"))
            pending_entries = len(data) if isinstance(data, list) else -1
        except (ValueError, OSError):
            pending_entries = -1

    ctx.data["wq"] = {
        "never_started_result": res_never,
        "never_started_queued": never_pending,
        "never_started_pending_file": never_file,
        "live_result": res_live,
        "live_drained": drained,
        "rows_live": rows_live,
        "after_stop_result": res_after_stop,
        "rows_after_stop": rows_after_stop,
        "queued_after_stop": queued_after_stop,
        "pending_after_stop": pending_after_stop,
        "pending_entries": pending_entries,
    }
    flog(
        f"tx_journal_retention_amputee write_queue: trace_id={ctx.trace_id} "
        f"{ctx.data['wq']}",
        "INFO",
    )
    shutil.rmtree(tmpdir, ignore_errors=True)


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_journal_retention_amputee run start: trace_id={ctx.trace_id}", "INFO")

    _run_purge(ctx)
    _run_rewind_on_amputated(ctx)
    _run_write_queue(ctx)

    flog(f"tx_journal_retention_amputee run end: trace_id={ctx.trace_id}", "INFO")

    conn = ctx.data.pop("conn", None)
    if conn is not None:
        conn.close()
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _partial_commits(snapshot: dict) -> dict:
    """Commits presents en base mais amputes d'une partie de leurs evenements."""
    expected = {sid: sorted(f"fid:{f}" for f in fids)
                for sid, _ts, fids in _COMMITS}
    out = {}
    for sid, fps in (snapshot.get("commits") or {}).items():
        want = expected.get(sid, [])
        if fps and sorted(fps) != want:
            out[sid] = {"restant": sorted(fps), "attendu": want}
    return out


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    before = ctx.data.get("before") or {}
    purge = ctx.data.get("purge") or {}
    after = ctx.data.get("after_purge") or {}
    rewind = ctx.data.get("rewind") or {}
    wq = ctx.data.get("wq") or {}

    # ===== Fixture =======================================================
    out.append((
        "fixture_trois_commits_complets",
        before.get("total") == 12 and len(before.get("commits") or {}) == 3,
        f"journal de depart : total={before.get('total')} attendu=12, "
        f"commits={sorted((before.get('commits') or {}))}",
    ))

    # ===== 1. La purge par exces ========================================
    out.append((
        "purge_respecte_le_plafond",
        after.get("total") == _MAX_EVENTS,
        f"apres purge : {after.get('total')} evenements pour un plafond de "
        f"{_MAX_EVENTS} (supprimes={purge.get('deleted_count')}, "
        f"exces={purge.get('excess_deleted')}, erreur={purge.get('error')!r})",
    ))
    partiels = _partial_commits(after)
    out.append((
        "purge_ne_coupe_aucun_commit",
        not partiels,
        f"commits laisses a moitie par la purge : {partiels}. Les evenements "
        f"d'une meme validation portent le meme horodatage et forment un tout : "
        f"en garder une partie produit un journal dont aucune date ne "
        f"correspond a un etat reel de la couche. La purge automatique du "
        f"demarrage fait cela sans confirmation ni message.",
    ))
    out.append((
        "purge_signale_l_amputation",
        not partiels or bool(purge.get("error")),
        f"PurgeResult={purge} : rien dans le resultat ne dit qu'un commit a "
        f"ete tronque ; l'appelant (recover._run_auto_purge_if_enabled) ne "
        f"peut donc rien afficher a l'utilisateur",
    ))

    # ===== 2. Rewind sur un journal ampute ==============================
    out.append((
        "couverture_calculee_correctement",
        bool(rewind.get("coverage_reason")),
        f"check_retention_coverage(cutoff={rewind.get('cutoff')}, "
        f"oldest={rewind.get('oldest_remaining')}) -> "
        f"{rewind.get('coverage_reason')!r} : la fonction qui sait detecter "
        f"une date hors couverture doit repondre ici",
    ))
    out.append((
        "rewind_hors_couverture_bloque",
        "BLOCKED" in (rewind.get("verdict") or ""),
        f"verdict du preflight={rewind.get('verdict')} "
        f"blocages={rewind.get('blocking')} pour une date "
        f"({rewind.get('cutoff')}) anterieure au plus ancien evenement "
        f"conserve ({rewind.get('oldest_remaining')}). Le plugin va rejouer "
        f"ce qui reste et annoncer un succes, alors que l'etat demande n'est "
        f"plus atteignable : c'est un faux succes sur une operation de "
        f"recuperation.",
    ))
    out.append((
        "rewind_hors_couverture_au_moins_averti",
        bool(rewind.get("blocking")) or bool(rewind.get("warnings")),
        f"blocages={rewind.get('blocking')} avertissements="
        f"{rewind.get('warnings')} : si le rewind partiel est assume, il doit "
        f"au minimum etre annonce avant execution",
    ))
    entites_mardi = [f"fid:{f}" for f in _COMMITS[1][2]]
    couvertes = set(rewind.get("entities") or [])
    manquantes = [e for e in entites_mardi if e not in couvertes]
    out.append((
        "rewind_couvre_tout_le_commit_ampute",
        not manquantes,
        f"le commit '{_COMMITS[1][0]}' avait modifie {entites_mardi} ; le "
        f"rewind ne compense que {sorted(couvertes)}. Entites laissees dans "
        f"leur etat modifie sans le dire : {manquantes}. Sur le terrain, deux "
        f"parcelles reviennent en arriere et deux restent modifiees, sans "
        f"aucune difference visible.",
    ))

    # ===== 3. Le garde-fou de couverture est du code mort ================
    callers = []
    for path in _PLUGIN_ROOT.rglob("*.py"):
        parts = set(path.parts)
        if "build" in parts or "scripts" in parts or "__pycache__" in parts:
            continue
        if path.name in ("__init__.py", "restore_planner.py"):
            continue
        try:
            if "check_retention_coverage" in path.read_text(
                    encoding="utf-8", errors="replace"):
                callers.append(path.name)
        except OSError:
            continue
    out.append((
        "garde_fou_couverture_branche",
        bool(callers),
        f"aucun module du plugin n'appelle check_retention_coverage "
        f"(trouve dans {callers}) : la fonction qui sait refuser un rewind "
        f"hors couverture existe, est exportee par core/__init__.py, et n'est "
        f"jamais utilisee. Le garde-fou est du code mort.",
    ))

    # ===== 4. WriteQueue : accepter puis perdre ==========================
    out.append((
        "wq_ecriture_normale_ok",
        wq.get("live_result") is True and wq.get("rows_live") == 2,
        f"temoin : enqueue sur une file demarree -> {wq.get('live_result')}, "
        f"lignes ecrites={wq.get('rows_live')} attendu 2. Si ce cas echoue, "
        f"c'est la fixture qui est cassee.",
    ))
    out.append((
        "wq_jamais_demarree_ne_ment_pas",
        wq.get("never_started_result") is False
        or wq.get("never_started_pending_file") is True,
        f"WriteQueue jamais demarree : enqueue a repondu "
        f"{wq.get('never_started_result')}, "
        f"{wq.get('never_started_queued')} evenement(s) restent en memoire, "
        f"fichier d'attente ecrit={wq.get('never_started_pending_file')}. "
        f"Repondre True sans destinataire fait croire a l'appelant que les "
        f"modifications sont enregistrees ; elles disparaissent a la "
        f"fermeture de QGIS.",
    ))
    out.append((
        "wq_apres_stop_ne_ment_pas",
        wq.get("after_stop_result") is False,
        f"enqueue apres stop() a repondu {wq.get('after_stop_result')} : "
        f"enqueue ne consulte jamais self._running, elle accepte donc des "
        f"evenements que plus aucun thread ne drainera",
    ))
    out.append((
        "wq_apres_stop_aucun_evenement_perdu",
        wq.get("rows_after_stop") == 5 or wq.get("pending_entries") == 3,
        f"apres stop() : {wq.get('rows_after_stop')} ligne(s) en base "
        f"(attendu 5), {wq.get('queued_after_stop')} evenement(s) bloques en "
        f"memoire, fichier d'attente={wq.get('pending_after_stop')} "
        f"({wq.get('pending_entries')} entree(s)). Les 3 evenements acceptes "
        f"apres l'arret ne sont ni en base ni sur le disque : c'est "
        f"exactement la perte silencieuse que le fichier d'attente devait "
        f"empecher.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"retention: purged \d+ excess events",
        name="purge_exces_tracee",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_retention_amputee.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))

    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
