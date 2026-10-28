# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
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
    run C : WriteQueue jamais demarree (puis ouverte sur le journal d'un
            autre projet), puis demarree/arretee, puis enqueue() avec une
            copie du disque prise a l'instant de la reponse, puis ouverte
            sur le journal d'un autre projet, puis redemarrage simule
            (durability.restart_integrate)

Hypotheses revisees (v7)
------------------------
Seule la partie C (WriteQueue) est concernee ; les parties A et B sont
inchangees. Depuis le schema v7, enqueue() fsync le lot dans l'outbox de
son journal (<journal>.outbox/) AVANT toute mise en file RAM et rend une
EnqueueOutcome explicite au lieu d'un booleen ; recoverland_pending.json
n'est plus la destination de la file. Trois hypotheses du point 3 sont
donc fausses et les assertions qui en dependaient ont ete reecrites, a
exigence egale ou superieure :

* "is True / is False" epinglait l'ancienne API : on exige desormais le
  nom exact de l'issue (QUEUED_DURABLE, REFUSED_NO_DURABLE_PATH,
  PERSISTED_FALLBACK), plus precis qu'une valeur de verite.
* Une file jamais demarree n'a ni journal ni outbox : la seule reponse
  honnete est REFUSED_NO_DURABLE_PATH (fausse, comptee dans
  health().lost_events, journalisee en CRITICAL). Le refus ne doit laisser
  aucune copie cachee : quand cette meme file s'ouvre ensuite sur un
  journal, celui-ci ne recoit aucun des evenements refuses (ni ligne, ni
  outbox).
* Apres stop(), le lot n'est plus "en RAM que personne ne drainera" : il
  est durable dans l'outbox. La reponse honnete devient PERSISTED_FALLBACK
  (degradee, comptee). Ce qu'elle affirme se prouve sur le disque seul, a
  l'instant ou elle est donnee : le journal et son outbox sont copies dans
  un repertoire neuf des le retour d'enqueue(), et un redemarrage sur
  cette copie donne exactement fid:801, 802, 811, 812 et 813, une fois
  chacun. Apres un redemarrage simule sur l'original, le journal contient
  ces memes 5 entites une fois chacune et l'outbox est videe ; et le
  journal d'un autre projet ouvert entre-temps par la meme file n'en
  recoit aucune.

Deuxieme passe (v7) : pending_count et les champs queued_* de health()
lisent la generation COURANTE. Avant start() et apres stop() il n'y en a
pas : ils valent 0 par construction. Les conjonctions "rien ne reste en
RAM" qui les lisaient ne pouvaient donc pas echouer ; elles sont
remplacees par des sondes sur le disque (lignes du journal et
outbox_events). La seule consequence observable d'une copie gardee en RAM
est d'etre ecrite plus tard dans le journal que la file ouvre ensuite :
c'est ce qui est mesure. De meme, un redemarrage dans le meme processus ne
prouve pas ce que le disque contenait au moment de la reponse (la memoire
du processus survit) : d'ou la copie du disque. Les compteurs lost_events
et degraded_events, eux, appartiennent a l'instance et restent lisibles
apres stop().
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


def _outcome_name(res) -> str:
    return getattr(res, "name", repr(res))


# Ce que le journal de la partie C doit contenir apres redemarrage : les 2
# evenements ecrits par l'ecrivain vivant et les 3 acceptes apres stop(),
# chacun UNE fois ; jamais 901/902, refuses faute de journal.
_WQ_EXPECTED = {f"fid:{fid}": 1 for fid in (801, 802, 811, 812, 813)}


def _fresh_journal(root: str, name: str) -> str:
    """Journal vide (schema seul) dans son propre repertoire : un autre projet."""
    from recoverland.core.sqlite_schema import initialize_schema

    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
        conn.commit()
    finally:
        conn.close()
    return path


def _fingerprints(db_path: str, session: str = "sess_wq") -> dict:
    """entity_fingerprint -> nombre de lignes de *session* dans le journal."""
    try:
        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute(
                "SELECT entity_fingerprint, COUNT(*) FROM audit_event "
                "WHERE session_id = ? GROUP BY entity_fingerprint",
                (session,)).fetchall()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        return {"<illisible>": f"{type(exc).__name__}: {exc}"}
    return {str(fp): int(n) for fp, n in rows}


def _copy_disk_state(db_path: str, dest_root: str) -> str:
    """Copie ce qui est SUR LE DISQUE pour *db_path* : journal (+ -wal) et outbox.

    La copie vit dans un repertoire ou le processus n'a jamais ecrit : un
    redemarrage sur elle ne peut trouver que ce que le disque contenait au
    moment de la copie, pas ce que la memoire du processus garde encore.
    """
    from recoverland.core.capture_outbox import outbox_dir_for

    os.makedirs(dest_root, exist_ok=True)
    dst = os.path.join(dest_root, os.path.basename(db_path))
    for suffix in ("", "-wal"):
        if os.path.isfile(db_path + suffix):
            shutil.copy2(db_path + suffix, dst + suffix)
    src_outbox = outbox_dir_for(db_path)
    if os.path.isdir(src_outbox):
        shutil.copytree(src_outbox, outbox_dir_for(dst))
    return dst


def _run_write_queue(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.integrity import _get_pending_path
    from recoverland.core.logger import flog
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events, pending_events,
        restart_integrate,
    )

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_jra_wq_")
    db_path = str(Path(tmpdir) / "recoverland_audit.sqlite")
    _seed(db_path)
    pending_path = _get_pending_path(db_path)

    # --- C1 : jamais demarree ------------------------------------------
    never = WriteQueue()
    res_never = never.enqueue([_event(901), _event(902)])
    # lost_events est un compteur de l'INSTANCE : il reste lisible sans
    # generation. pending_count et health().queued_* lisent la generation
    # courante, absente ici : 0 par construction, ils ne prouvent rien.
    never_lost = never.health().lost_events
    never_file = os.path.isfile(pending_path)
    # Seule consequence observable d'une copie gardee en RAM malgre le
    # refus : etre ecrite dans le premier journal que cette file ouvre.
    never_next_db = _fresh_journal(tmpdir, "projet_suivant_c1")
    never.start(never_next_db)
    never.stop()
    never_next_fps = _fingerprints(never_next_db)
    never_next_outbox = outbox_events(never_next_db, "sess_wq")

    # --- C2 : demarree, arretee, puis sollicitee -------------------------
    wq = WriteQueue()
    wq.start(db_path)
    res_live = wq.enqueue([_event(801), _event(802)])
    drained = _wait_drained(wq)
    rows_live = _count_rows(db_path, "parcelles")
    wq.stop()

    res_after_stop = wq.enqueue([_event(811), _event(812), _event(813)])
    # Ce que la reponse affirme se mesure a l'instant ou elle est donnee,
    # sur le disque seul : copie du journal et de son outbox AVANT toute
    # autre action, puis redemarrage sur la copie (jamais sur l'original).
    ack_copy_db = _copy_disk_state(
        db_path, os.path.join(tmpdir, "copie_a_l_accuse"))
    outbox_after_stop = outbox_events(db_path, "sess_wq")
    health_after_stop = wq.health()
    ack_copy_outbox = outbox_events(ack_copy_db, "sess_wq")
    try:
        ack_copy_restart = restart_integrate(ack_copy_db)
    except Exception as exc:  # noqa: BLE001 - un redemarrage casse est un FAIL
        ack_copy_restart = {"crash": f"{type(exc).__name__}: {exc}"}
    ack_copy_fps = _fingerprints(ack_copy_db)
    time.sleep(0.4)
    rows_after_stop = _count_rows(db_path, "parcelles")
    pending_entries = pending_events(db_path)

    # La meme file s'ouvre ensuite sur le journal d'un autre projet : une
    # copie des 3 evenements gardee en RAM apres l'arret y serait ecrite.
    wq_next_db = _fresh_journal(tmpdir, "projet_suivant_c2")
    wq.start(wq_next_db)
    wq.stop()
    wq_next_fps = _fingerprints(wq_next_db)
    wq_next_outbox = outbox_events(wq_next_db, "sess_wq")

    # --- C3 : redemarrage simule (ce que fait le plugin au demarrage) ----
    try:
        restart = restart_integrate(db_path)
    except Exception as exc:  # noqa: BLE001 - un redemarrage casse est un FAIL
        restart = {"crash": f"{type(exc).__name__}: {exc}"}
    sess = ("session_id = ?", ("sess_wq",))
    rows_after_restart = count_rows(db_path, *sess)
    fps_after_restart = _fingerprints(db_path)
    dups_after_restart = duplicate_uids(db_path)
    outbox_after_restart = outbox_events(db_path, "sess_wq")

    ctx.data["wq"] = {
        "never_started_result": _outcome_name(res_never),
        "never_started_truthy": bool(res_never),
        "never_started_lost": never_lost,
        "never_started_pending_file": never_file,
        "never_next_fps": never_next_fps,
        "never_next_outbox": never_next_outbox,
        "live_result": _outcome_name(res_live),
        "live_drained": drained,
        "rows_live": rows_live,
        "after_stop_result": _outcome_name(res_after_stop),
        "after_stop_truthy": bool(res_after_stop),
        "after_stop_degraded": getattr(res_after_stop, "degraded", None),
        "outbox_after_stop": outbox_after_stop,
        "ack_copy_outbox": ack_copy_outbox,
        "ack_copy_restart": ack_copy_restart,
        "ack_copy_fps": ack_copy_fps,
        "rows_after_stop": rows_after_stop,
        "degraded_after_stop": health_after_stop.degraded_events,
        "lost_after_stop": health_after_stop.lost_events,
        "pending_entries": pending_entries,
        "wq_next_fps": wq_next_fps,
        "wq_next_outbox": wq_next_outbox,
        "restart": restart,
        "rows_after_restart": rows_after_restart,
        "fps_after_restart": fps_after_restart,
        "dups_after_restart": dups_after_restart,
        "outbox_after_restart": outbox_after_restart,
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
    # Hypothesis revised (v7): "exactly 6 events" cannot hold together with
    # "no commit cut" on this fixture (3 commits of 4 events, cap 6): 12-6
    # would mean deleting a commit and a half. The purge now deletes WHOLE
    # commits, oldest first, and no more than needed: at or under the cap,
    # and keeping one more commit would exceed it.
    commit_size = len(_COMMITS[0][2])
    out.append((
        "purge_respecte_le_plafond",
        (after.get("total") or 0) <= _MAX_EVENTS
        and (after.get("total") or 0) + commit_size > _MAX_EVENTS,
        f"apres purge : {after.get('total')} evenements pour un plafond de "
        f"{_MAX_EVENTS} (supprimes={purge.get('deleted_count')}, "
        f"exces={purge.get('excess_deleted')}, erreur={purge.get('error')!r}) ; "
        f"attendu : sous le plafond, par commits entiers de {commit_size}, "
        f"sans supprimer un commit de plus que necessaire",
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
    # Hypothesis revised (v7): the purge no longer amputates a commit, it
    # removes it whole. The failure this guards against (a rewind that
    # silently compensates PART of a commit) is excluded when either every
    # entity of the commit is covered, or the commit is entirely gone from
    # the journal AND the rewind to a date it covered is BLOCKED.
    mardi_restant = (after.get("commits") or {}).get(_COMMITS[1][0]) or []
    mardi_absent_et_bloque = (not mardi_restant
                              and "BLOCKED" in (rewind.get("verdict") or ""))
    out.append((
        "rewind_couvre_tout_le_commit_ampute",
        not manquantes or mardi_absent_et_bloque,
        f"le commit '{_COMMITS[1][0]}' avait modifie {entites_mardi} ; le "
        f"rewind ne compense que {sorted(couvertes)}. Entites laissees dans "
        f"leur etat modifie sans le dire : {manquantes} (commit encore "
        f"present={mardi_restant}, verdict={rewind.get('verdict')}). Sur le terrain, deux "
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
    # Hypothesis revised (v7): enqueue() ne rend plus un booleen mais une
    # EnqueueOutcome ; "is True" epinglait l'ancienne API. Le cas nominal
    # (lot dans l'outbox + ecrivain vivant) s'appelle QUEUED_DURABLE : exiger
    # ce nom exact est plus strict qu'exiger une valeur vraie, et les 2
    # lignes au journal restent exigees a l'identique.
    out.append((
        "wq_ecriture_normale_ok",
        wq.get("live_result") == "QUEUED_DURABLE" and wq.get("rows_live") == 2,
        f"temoin : enqueue sur une file demarree -> {wq.get('live_result')} "
        f"(attendu QUEUED_DURABLE), lignes ecrites={wq.get('rows_live')} "
        f"attendu 2. Si ce cas echoue, c'est la fixture qui est cassee.",
    ))
    # Hypothesis revised (v7): "False, ou bien un fichier d'attente ecrit"
    # n'existe plus sous cette forme : enqueue() rend une EnqueueOutcome et
    # la file n'ecrit plus dans recoverland_pending.json. Une file jamais
    # demarree n'a ni journal ni outbox : la seule reponse honnete est
    # REFUSED_NO_DURABLE_PATH, fausse, comptee dans health().lost_events
    # (compteur de l'instance, lisible sans generation) et criee en
    # CRITICAL. Meme exigence (ne jamais dire "enregistre" sans
    # destinataire), plus stricte : l'issue exacte, le compteur de perte et
    # le log sont exiges.
    # Hypothesis revised (v7): la premiere adaptation exigeait aussi
    # pending_count == 0 et health().queued_batches/queued_events == 0.
    # Ces lectures portent sur la generation courante, absente avant
    # start() : elles valaient 0 par construction et ne pouvaient pas
    # echouer. Elles sont remplacees par une sonde sur le disque : la file
    # s'ouvre ensuite sur le journal d'un autre projet, et ce journal ne
    # recoit aucun des 2 evenements refuses (ni ligne, ni outbox). C'est la
    # seule consequence observable d'une copie gardee en memoire ; elle
    # ferait aussi mentir l'issue REFUSED_NO_DURABLE_PATH.
    perte_criee = [
        r for r in ctx.records
        if getattr(r, "level", "") == "CRITICAL"
        and "event=wq_events_lost n_events=2 " in r.raw
        and "journal=None" in r.raw
    ]
    out.append((
        "wq_jamais_demarree_ne_ment_pas",
        wq.get("never_started_result") == "REFUSED_NO_DURABLE_PATH"
        and wq.get("never_started_truthy") is False
        and wq.get("never_started_lost") == 2
        and wq.get("never_next_fps") == {}
        and wq.get("never_next_outbox") == 0
        and len(perte_criee) >= 1,
        f"WriteQueue jamais demarree : enqueue a repondu "
        f"{wq.get('never_started_result')} (vrai={wq.get('never_started_truthy')}, "
        f"attendu REFUSED_NO_DURABLE_PATH et faux), perte comptee="
        f"{wq.get('never_started_lost')} attendu 2, log CRITICAL "
        f"wq_events_lost={len(perte_criee)} (attendu >= 1). Journal ouvert "
        f"ensuite par la meme file : lignes sess_wq="
        f"{wq.get('never_next_fps')} (attendu aucune), outbox="
        f"{wq.get('never_next_outbox')} (attendu 0). Repondre vrai sans "
        f"destinataire fait croire a l'appelant que les modifications sont "
        f"enregistrees ; les garder en memoire pour les ecrire plus tard "
        f"dans le premier journal venu les range dans un autre projet.",
    ))
    # Hypothesis revised (v7): "apres stop() le lot vit en RAM et plus aucun
    # thread ne le drainera" est faux : enqueue() fsync le lot dans l'outbox
    # du journal AVANT toute mise en file. Repondre False serait desormais
    # inexact (le lot est durable). La reponse honnete est
    # PERSISTED_FALLBACK : .degraded est vrai et health().degraded_events
    # (compteur de l'instance) la compte, donc elle ne se confond jamais
    # avec le cas nominal.
    # Hypothesis revised (v7): la premiere adaptation lisait aussi
    # pending_count apres stop() ("0 en memoire") : sans generation, cette
    # lecture vaut 0 par construction et ne prouvait rien. Ce que la
    # reponse affirme se prouve maintenant sur le disque seul, a l'instant
    # ou elle est donnee : le journal et son outbox sont copies dans un
    # repertoire neuf des le retour d'enqueue(), et un redemarrage sur cette
    # copie contient exactement fid:801, 802, 811, 812, 813, une fois
    # chacun. Un redemarrage dans le meme processus ne le prouverait pas (la
    # memoire survit). La conjonction "0 en memoire" devient une sonde sur
    # le disque : la meme file ouverte ensuite sur le journal d'un autre
    # projet n'y ecrit aucun des 3 evenements (ni ligne, ni outbox).
    ack_rep = (wq.get("ack_copy_restart") or {})
    out.append((
        "wq_apres_stop_ne_ment_pas",
        wq.get("after_stop_result") == "PERSISTED_FALLBACK"
        and wq.get("after_stop_degraded") is True
        and wq.get("outbox_after_stop") == 3
        and not ack_rep.get("crash")
        and wq.get("ack_copy_fps") == _WQ_EXPECTED
        and wq.get("wq_next_fps") == {}
        and wq.get("wq_next_outbox") == 0
        and wq.get("degraded_after_stop") == 3
        and wq.get("lost_after_stop") == 0,
        f"enqueue apres stop() a repondu {wq.get('after_stop_result')} "
        f"(degrade={wq.get('after_stop_degraded')}, attendu PERSISTED_FALLBACK "
        f"degrade) ; evenements dans l'outbox au moment de la reponse="
        f"{wq.get('outbox_after_stop')} attendu 3 (copie : "
        f"{wq.get('ack_copy_outbox')}). Redemarrage sur la copie du disque "
        f"prise a l'accuse : entites={wq.get('ack_copy_fps')} attendu "
        f"{_WQ_EXPECTED}, rejeu={(ack_rep.get('replay') or {})}, "
        f"crash={ack_rep.get('crash')}. Journal d'un autre projet ouvert "
        f"ensuite par la meme file : lignes sess_wq={wq.get('wq_next_fps')} "
        f"(attendu aucune), outbox={wq.get('wq_next_outbox')} (attendu 0). "
        f"Degrades comptes={wq.get('degraded_after_stop')} attendu 3, "
        f"perdus={wq.get('lost_after_stop')} attendu 0. Une reponse vraie "
        f"n'est honnete que si le lot est reellement sur le disque.",
    ))
    # Hypothesis revised (v7): la file ne range plus rien dans
    # recoverland_pending.json (pending_entries == 3 ne peut plus arriver)
    # et, sans ecrivain, n'indexe pas le lot au journal avant le prochain
    # demarrage (rows_after_stop reste a 2). La propriete visee, "les 3
    # evenements acceptes apres l'arret ne sont pas perdus", se mesure la ou
    # l'utilisateur la constate : apres un redemarrage simule
    # (durability.restart_integrate), le journal contient exactement les
    # entites fid:801, 802, 811, 812 et 813, chacune une fois, et l'outbox
    # est videe. Plus strict que "en base OU dans un fichier" : l'arrivee
    # effective au journal, entite par entite, est exigee. Un simple
    # decompte (5 lignes) laisserait passer un evenement perdu remplace
    # par un autre (901 refuse, ou un doublon), ou des lignes rejouees sans
    # leur entity_fingerprint. dups_after_restart == 0 est garde mais ne
    # prouve rien seul : l'index UNIQUE idx_event_uid l'impose tant qu'il
    # existe.
    restart = wq.get("restart") or {}
    replay = restart.get("replay") or {}
    out.append((
        "wq_apres_stop_aucun_evenement_perdu",
        wq.get("rows_after_restart") == 5
        and wq.get("fps_after_restart") == _WQ_EXPECTED
        and wq.get("dups_after_restart") == 0
        and wq.get("outbox_after_restart") == 0
        and replay.get("events_inserted") == 3
        and not restart.get("crash"),
        f"apres stop() : {wq.get('rows_after_stop')} ligne(s) en base avant "
        f"redemarrage, {wq.get('outbox_after_stop')} evenement(s) dans "
        f"l'outbox, fichier d'attente historique="
        f"{wq.get('pending_entries')} entree(s). Apres redemarrage simule : "
        f"{wq.get('rows_after_restart')} ligne(s) (attendu 5), entites="
        f"{wq.get('fps_after_restart')} (attendu {_WQ_EXPECTED}), event_uid "
        f"en double={wq.get('dups_after_restart')} (attendu 0), reste dans "
        f"l'outbox={wq.get('outbox_after_restart')} (attendu 0), rejoues="
        f"{replay.get('events_inserted')} (attendu 3), "
        f"crash={restart.get('crash')}. Les 3 evenements acceptes apres "
        f"l'arret doivent atteindre le journal, exactement une fois : sinon "
        f"c'est la perte silencieuse que l'outbox doit empecher.",
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
