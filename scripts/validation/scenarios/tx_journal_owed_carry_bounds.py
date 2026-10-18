"""tx_journal_owed_carry_bounds - RecoverLand validation runtime.

Invariant: I-JRN (le journal d'un projet contient son historique, pas
celui des autres ; une obligation de rattachement n'est jamais abandonnee
en silence).

Le mecanisme
------------
Un "Enregistrer sous" note durablement, dans le profil, que l'historique
de la session (du journal d'origine vers celui du nouveau projet) reste a
rattacher (carries_owed.json). Si QGIS meurt avant la fin, la prochaine
ouverture du projet s'en charge (`_settle_owed_carries`). Deux defauts :

  * l'obligation n'avait qu'une borne basse ("depuis l'evenement N"). Le
    journal d'origine d'un projet enregistre est celui de tout son DOSSIER :
    ce que les sessions suivantes y ecrivaient pour d'AUTRES projets du
    dossier partait aussi dans le journal du projet enregistre ailleurs ;
  * elle etait supprimee, sans un mot a l'utilisateur, des que le journal
    d'origine ou le fichier de projet n'etait pas trouve a cet instant (un
    partage reseau pas encore monte au demarrage), alors qu'il existe.

Le scenario d'echec concret
---------------------------
Q et Q2 sont dans le dossier D1. L'utilisateur fait "Enregistrer sous" P
(dossier D2) depuis Q ; QGIS plante. Le lendemain un collegue travaille sur
Q2 (meme journal de dossier). Quand P est ouvert, son journal recoit les
modifications de Q2, avec des numeros qui se melangent a son historique.
Autre cas : P est enregistre depuis un projet sur un partage, QGIS plante,
et au redemarrage le partage n'est pas encore monte : P n'aura jamais son
historique, et personne ne le sait.

Ce que le scenario execute reellement
-------------------------------------
S (bornes) : vrai JournalManager. Q ouvert ; une ligne ancienne ; Q rouvert,
  une modification de session, un lot capture dans l'outbox (la fin de
  session qu'un plantage laisse), plan du rattachement vers P, P ecrit,
  "plantage" (le plan en memoire est perdu, l'obligation durable reste).
  Puis Q2 est ouvert (meme journal) : l'outbox est rejouee comme au
  demarrage de la capture, puis une modification de Q2, DATEE d'avant le
  plantage (horloge d'un autre poste en retard, donnees importees avec leur
  date) : la borne ne doit pas reposer sur les dates. Enfin P est ouvert :
  il doit avoir la modification de session ET la fin rejouee, pas la ligne
  ancienne, pas la modification de Q2.
U (obligations) : deux obligations durables vers P. L'une a sa source sur
  un lecteur absent (injoignable) : elle doit etre conservee. L'autre a sa
  source supprimee d'un dossier joignable : elle est abandonnee, et
  l'utilisateur doit en etre informe.
C (fermeture, source injoignable) : le "partage" est un VRAI lecteur
  (`subst`) qui disparait, comme un partage SMB qui decroche : Windows
  repond "introuvable" pour tout ce qu'il contient, racine comprise. Q est
  ouvert sur ce lecteur, une modification de session, "Enregistrer sous" P
  en local (P ecrit), puis le lecteur disparait juste avant la bascule vers
  P. Le rattachement a la fermeture de Q concluait "journal source
  introuvable, rien a rattacher", reussi, et oubliait l'obligation ; une
  obligation conservee mais bornee a l'evenement 0 (borne lue dans un
  journal illisible) ne rattachait plus rien au nouvel essai. Attendu :
  echec dit (cote journal d'origine), obligation conservee et non bornee a
  0 ; le lecteur revient, un autre projet Q2 du meme dossier est ouvert et
  modifie, puis P est ouvert : il recoit la modification de session et pas
  celle de Q2.

Verdict attendu avant correctif : FAIL (S : la modification de Q2 est dans
P ; U : l'obligation injoignable est supprimee, rien n'est dit ; C :
l'obligation est oubliee a la fermeture, P n'aura jamais l'historique).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import string
import subprocess  # nosec B404: subst, lecteur de test
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_journal_owed_carry_bounds"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_MARKERS = ("OLD_BEFORE_OPEN", "SESSION_EDIT", "SESSION_TAIL_IN_OUTBOX",
            "LATER_OTHER_PROJECT_Q2")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _values(marker: str, uid: str, created: str) -> dict:
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": "ds_bounds",
        "layer_id_snapshot": "lyr", "layer_name_snapshot": marker,
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": 1}),
        "operation_type": "UPDATE",
        "attributes_json": json.dumps({"changed_only": {"nom": {"old": "A", "new": marker}}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": "EPSG:2154",
        "field_schema_json": None, "user_name": "tester", "session_id": None,
        "created_at": created, "restored_from_event_id": None,
        "entity_fingerprint": f"fid:{uid}", "event_schema_version": 2,
        "new_geometry_wkb": None, "invalidated_at": None,
        "event_uid": uid, "commit_id": None,
    }


def _insert(conn, marker: str, uid: str, created: str = "") -> None:
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
    cols = list(AUDIT_EVENT_INSERT_COLUMNS) + ["event_uid", "commit_id"]
    row = _values(marker, uid, created or _now())
    conn.execute(f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
                 f"VALUES ({','.join(['?'] * len(cols))})", [row[c] for c in cols])
    conn.commit()


def _count(path: str, marker: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE layer_name_snapshot = ?",
            (marker,)).fetchone()[0])
    finally:
        conn.close()


def _part_s(tmp: str) -> dict:
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.capture_outbox import CaptureOutbox, replay_outbox

    res = {}
    profile = os.path.join(tmp, "profil_s")
    os.makedirs(profile, exist_ok=True)
    d1 = os.path.join(tmp, "D1")
    d2 = os.path.join(tmp, "D2")
    os.makedirs(d1, exist_ok=True)
    os.makedirs(d2, exist_ok=True)
    q = os.path.join(d1, "Q.qgz")
    q2 = os.path.join(d1, "Q2.qgz")
    p = os.path.join(d2, "P.qgz")
    for f in (q, q2):
        Path(f).write_text("<qgis/>", encoding="utf-8")

    jm = JournalManager()
    jm.open_for_project(q, profile)
    _insert(jm.get_connection(), "OLD_BEFORE_OPEN", "u-old")
    jm.close()

    jm = JournalManager()
    j_q = jm.open_for_project(q, profile)
    _insert(jm.get_connection(), "SESSION_EDIT", "u-sess")
    # The end of the session: captured, durable in the outbox, not yet in
    # the journal when QGIS dies.
    box = CaptureOutbox(j_q)
    box.append_events("b-tail", [_values("SESSION_TAIL_IN_OUTBOX", "u-tail", _now())])
    box.close()
    carry = jm.plan_history_carry(p, profile, being_written=True)
    res["planned"] = carry is not None
    Path(p).write_text("<qgis/>", encoding="utf-8")
    jm._pending_carry = None          # QGIS dies before the close
    jm.close()
    time.sleep(0.05)

    jm = JournalManager()             # a later session, another project of D1
    jm.open_for_project(q2, profile)
    replay = replay_outbox(j_q, jm.get_connection())   # what the writer does at start
    res["tail_replayed"] = replay.events_inserted
    time.sleep(0.05)
    # Dated BEFORE the crash on purpose (a colleague's clock behind, or data
    # imported with its own dates): the bound must not rest on dates.
    _insert(jm.get_connection(), "LATER_OTHER_PROJECT_Q2", "u-later",
            created="2026-09-27T10:00:00+00:00")
    jm.close()

    jm = JournalManager()             # P is opened
    j_p = jm.open_for_project(p, profile)
    jm.close()
    res["counts"] = {m: _count(j_p, m) for m in _MARKERS}
    return res


def _free_drive() -> str:
    if os.name != "nt":
        return ""
    for letter in reversed(string.ascii_uppercase):
        root = f"{letter}:\\"
        if not os.path.exists(root):
            return root
    return ""


def _part_u(tmp: str) -> dict:
    from recoverland.core import journal_manager as jmod

    res = {}
    profile = os.path.join(tmp, "profil_u")
    audit = os.path.join(profile, "recoverland", "audit")
    os.makedirs(audit, exist_ok=True)
    pdir = os.path.join(tmp, "P_u")
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "P.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    target = os.path.join(pdir, ".recoverland", "recoverland_audit.sqlite")
    drive = _free_drive()
    res["free_drive"] = drive
    unreachable = os.path.join(drive or "\\\\rl-no-such-host\\x", "partage", "D1",
                               ".recoverland", "recoverland_audit.sqlite")
    gone_dir = os.path.join(tmp, "D_gone", ".recoverland")
    os.makedirs(gone_dir, exist_ok=True)
    gone = os.path.join(gone_dir, "recoverland_audit.sqlite")   # never created

    def entry(source: str) -> dict:
        return {"source": source, "source_kind": "saved", "source_session_key": "",
                "target": target, "target_kind": "saved", "since_event_id": 0,
                "opened_at": "2026-09-27T08:00:00+00:00", "mark_source": False,
                "project_file": project, "profile_path": profile,
                "planned_at": "2026-09-27T08:05:00+00:00"}

    owed = os.path.join(audit, "carries_owed.json")
    with open(owed, "w", encoding="utf-8") as fh:
        json.dump([entry(unreachable), entry(gone)], fh)
    told = []
    saved_qlog = jmod.qlog
    jmod.qlog = lambda msg, level="INFO": told.append((str(msg), level))
    try:
        jm = jmod.JournalManager()
        try:
            jm.open_for_project(project, profile)
        finally:
            jm.close()
    finally:
        jmod.qlog = saved_qlog
    try:
        with open(owed, "r", encoding="utf-8") as fh:
            left = json.load(fh)
    except FileNotFoundError:
        left = []
    res["left_sources"] = [e.get("source") for e in left]
    res["unreachable"] = unreachable
    res["gone"] = gone
    res["told"] = told
    return res


_C_MARKERS = ("C_SESSION_EDIT_ON_SHARE", "C_LATER_OTHER_PROJECT_Q2")


def _subst(letter: str, folder: str = "") -> int:
    """Map *letter* on *folder* (a drive that can vanish), or unmap it."""
    args = ["subst", letter, folder] if folder else ["subst", letter, "/D"]
    return subprocess.run(args, capture_output=True, text=True,  # nosec B603 B607
                          timeout=30).returncode


def _owed_entries(profile: str) -> list:
    path = os.path.join(profile, "recoverland", "audit", "carries_owed.json")
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return []


def _part_c(tmp: str) -> dict:
    from recoverland.core import journal_manager as jmod

    res = {}
    if os.name != "nt":
        res["skipped"] = "subst is Windows only"
        return res
    profile = os.path.join(tmp, "profil_c")
    share = os.path.join(tmp, "partage_D1")
    d2 = os.path.join(tmp, "local_D2")
    for folder in (profile, share, d2):
        os.makedirs(folder, exist_ok=True)
    drive = _free_drive()
    letter = drive[:2]
    res["letter"] = letter
    res["mapped"] = _subst(letter, share)
    try:
        q = os.path.join(drive, "Q.qgz")
        q2 = os.path.join(drive, "Q2.qgz")
        p = os.path.join(d2, "P.qgz")
        for f in (q, q2):
            Path(f).write_text("<qgis/>", encoding="utf-8")
        jm = jmod.JournalManager()
        j_q = jm.open_for_project(q, profile)
        res["q_journal"] = j_q
        _insert(jm.get_connection(), _C_MARKERS[0], "u-c-share")
        res["planned"] = jm.plan_history_carry(p, profile, being_written=True) is not None
        Path(p).write_text("<qgis/>", encoding="utf-8")   # the local save succeeded
        res["dropped"] = _subst(letter)                    # the share drops
        res["q_journal_seen_while_dropped"] = os.path.isfile(j_q)
        told = []
        saved_qlog = jmod.qlog
        jmod.qlog = lambda msg, level="INFO": told.append((str(msg), level))
        try:
            # The switch to P, as the plugin does after the save: closing Q
            # runs the carry, then P's journal opens (and retries what is owed).
            res["p_journal"] = jm.open_for_project(p, profile)
            rep = jm.last_carry_report
            res["report"] = ({k: rep.as_dict().get(k) for k in (
                "ok", "error", "warnings", "failure_side", "owed",
                "events_inserted")} if rep else None)
            res["message"] = list(rep.user_message()) if rep else None
            res["owed_after_switch"] = _owed_entries(profile)
            res["in_p_while_dropped"] = _count(res["p_journal"], _C_MARKERS[0])
            jm.close()
        finally:
            jmod.qlog = saved_qlog
        res["told"] = told
        res["remapped"] = _subst(letter, share)            # the share is back
        jm = jmod.JournalManager()
        jm.open_for_project(q2, profile)                   # another project of D1
        _insert(jm.get_connection(), _C_MARKERS[1], "u-c-later")
        jm.close()
        jm = jmod.JournalManager()
        j_p = jm.open_for_project(p, profile)              # P, the next day
        jm.close()
        res["counts"] = {m: _count(j_p, m) for m in _C_MARKERS}
        res["owed_at_end"] = _owed_entries(profile)
    finally:
        _subst(letter)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_tx_jocb_")
    flog(f"tx_journal_owed_carry_bounds setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    d = ctx.data
    for key, fn in (("s", _part_s), ("u", _part_u), ("c", _part_c)):
        try:
            d[key] = fn(d["tmp"])
        except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
            import traceback
            d[key] = {"crash": f"{type(exc).__name__}: {exc}",
                      "trace": traceback.format_exc()[-1500:]}
    flog(f"tx_journal_owed_carry_bounds run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:5000]}", "INFO")
    shutil.rmtree(d["tmp"], ignore_errors=True)


def _norm(path) -> str:
    return os.path.normcase(os.path.abspath(str(path or "")))


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    s = d.get("s") or {}
    counts = s.get("counts") or {}
    out.append((
        "fixture_s_plantage_apres_enregistrement",
        s.get("planned") is True and s.get("tail_replayed") == 1,
        f"rattachement planifie={s.get('planned')} fin de session rejouee depuis "
        f"l'outbox={s.get('tail_replayed')} crash={s.get('crash')} "
        f"{s.get('trace', '')[-500:]}",
    ))
    out.append((
        "s_le_projet_recoit_sa_session_et_rien_d_autre",
        counts.get("SESSION_EDIT") == 1 and counts.get("SESSION_TAIL_IN_OUTBOX") == 1
        and counts.get("OLD_BEFORE_OPEN") == 0
        and counts.get("LATER_OTHER_PROJECT_Q2") == 0,
        f"journal de P : {counts} (attendu session=1, fin rejouee=1, ancienne=0, "
        f"Q2=0) : les modifications d'un autre projet du dossier arrivaient dans "
        f"l'historique de P, et un Rewind de P les rejouerait",
    ))
    u = d.get("u") or {}
    left = [_norm(x) for x in (u.get("left_sources") or [])]
    told = [m for m, _lvl in (u.get("told") or []) if "abandonne" in m]
    out.append((
        "u_obligation_injoignable_conservee",
        not u.get("crash") and _norm(u.get("unreachable")) in left,
        f"obligations restantes={u.get('left_sources')} (source injoignable="
        f"{u.get('unreachable')}, lecteur libre={u.get('free_drive')!r}) crash="
        f"{u.get('crash')} {u.get('trace', '')[-400:]} : un partage pas encore "
        f"monte n'est pas un journal supprime",
    ))
    out.append((
        "u_obligation_abandonnee_dite_a_l_utilisateur",
        not u.get("crash") and _norm(u.get("gone")) not in left and len(told) == 1,
        f"obligation dont la source n'existe plus : conservee="
        f"{_norm(u.get('gone')) in left} ; messages={u.get('told')} (attendu un "
        f"message d'abandon) : une obligation abandonnee sans un mot laisse "
        f"croire que l'historique du projet est complet",
    ))
    c = d.get("c") or {}
    rep = c.get("report") or {}
    msg = c.get("message") or ["", ""]
    q_key = _norm(str(c.get("q_journal") or "").replace("\\\\?\\", ""))
    kept = [e for e in c.get("owed_after_switch") or []
            if _norm(e.get("source")) == q_key]
    out.append((
        "fixture_c_lecteur_qui_disparait",
        not c.get("crash") and c.get("mapped") == 0 and c.get("planned") is True
        and c.get("dropped") == 0 and c.get("q_journal_seen_while_dropped") is False
        and c.get("remapped") == 0,
        f"lecteur {c.get('letter')} monte={c.get('mapped')} rattachement planifie="
        f"{c.get('planned')} demonte={c.get('dropped')} journal de Q visible "
        f"pendant la coupure={c.get('q_journal_seen_while_dropped')} remonte="
        f"{c.get('remapped')} crash={c.get('crash')} {c.get('trace', '')[-500:]}",
    ))
    out.append((
        "c_source_injoignable_a_la_fermeture_obligation_conservee_et_dite",
        rep.get("ok") is False and rep.get("failure_side") == "source"
        and rep.get("owed") is True and len(kept) == 1
        and kept[0].get("until_event_id") != 0
        and msg[0] == "CRITICAL" and "reste intact" in (msg[1] or "")
        and c.get("in_p_while_dropped") == 0,
        f"rapport a la fermeture de Q={rep} ; message={msg} ; obligations "
        f"restantes vers P={kept} (attendu une, non bornee a 0) : un partage "
        f"qui decroche au moment de la bascule effacait l'obligation en disant "
        f"'rien a rattacher', et P n'aurait jamais l'historique de la session",
    ))
    counts_c = c.get("counts") or {}
    left_c = [e for e in c.get("owed_at_end") or [] if _norm(e.get("source")) == q_key]
    out.append((
        "c_partage_revenu_l_historique_arrive_sans_celui_de_q2",
        counts_c.get(_C_MARKERS[0]) == 1 and counts_c.get(_C_MARKERS[1]) == 0
        and not left_c,
        f"journal de P une fois le partage revenu : {counts_c} (attendu session=1, "
        f"Q2=0) ; obligations restantes={left_c} (attendu aucune) : "
        f"l'historique capture sur le partage ne suivait jamais le projet",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_owed_carry_bounds.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
