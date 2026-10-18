"""rw_floor_carried_history - RecoverLand validation runtime.

Invariant: I-RW-COVERAGE (un rewind refuse, en le nommant, une date que le
journal ne peut pas garantir ; il ne refuse pas une date qu'il garantit).

Le mecanisme
------------
Le tampon de couverture (`journal_writes.coverage_origin`) dit depuis
quand un journal enregistre toute purge (`since`) et ce qu'il etait avant
(`created` : aucune vie anterieure ; `migrated` ou '' : il a vecu sous une
version qui purgeait sans trace). "created" ne garantit que les lignes
ecrites DANS ce journal apres `since`. Un rattachement d'historique
("Enregistrer sous", journal de session vers journal de projet :
`journal_manager._carry_into_journal`) cree un journal NEUF (created,
since = l'instant du rattachement) et y recopie des lignes plus anciennes
que `since`, venues d'un autre journal ; ce que cet autre journal a pu
perdre sous sa plus ancienne ligne (purge silencieuse d'une ancienne
version, s'il etait migre) n'est dit nulle part dans la cible.

Review compare deja la plus ancienne ligne de la couche a la creation du
journal (`snapshot_rebuild_worker._layer_floor` : une ligne plus ancienne
que le journal garde son plancher). Le rewind
(`retention._legacy_floor_refusal`) ne le fait pas : journal "cree par
v7" -> aucune garde, quelle que soit l'origine des lignes.

Un "Enregistrer sous" d'un projet DEJA enregistre vers un autre dossier ne
rattache que les lignes de cette session (`plan_history_carry` :
since_event_id = l'evenement a l'ouverture) : l'historique plus ancien de
la couche reste dans le journal d'origine. Dans un dossier neuf, la cible
est plus recente que les lignes rattachees ; dans un dossier qui a deja un
journal (tous les projets d'un dossier le partagent), elle est plus
ancienne qu'elles et rien, ni son age ni celui des lignes, ne dit que
l'historique est ailleurs.

Le scenario d'echec concret
---------------------------
Un utilisateur ouvre son projet enregistre, modifie une couche deja
modifiee lors d'une session precedente, puis fait "Enregistrer sous" vers
un autre dossier : seule cette session suit (cas P). Il demande dans le
nouveau projet un rewind a une date anterieure a la session precedente :
le rewind l'accepte, n'annule que la modification de cette session et
annonce comme etat de cette date la valeur d'apres la session precedente.
Le meme rewind dans l'ancien projet annule les deux. Meme chose quand
l'historique entier d'un journal migre est rattache (cas K : la
fonction de rattachement avec since_event_id=0) : sous sa plus ancienne
ligne, l'ancienne version a pu purger sans trace ; l'ancien journal le
refuse, la cible l'accepte.

Ce que le scenario execute reellement
-------------------------------------
Vrais journaux, vrai ouvreur (`JournalManager.open_for_project`), vrai
rattachement (`_carry_into_journal`), vrais lecteurs du plancher :
`retention.coverage_refusal`, `restore_planner.preflight_check(plan,
conn=...)` sur un plan construit par la vraie fenetre et le vrai dedup, et
`snapshot_rebuild_worker.query_snapshot_floor` (Review).
  K journal au schema 6 (horloge juste) ouvert (migre), puis rattache a un
    nouveau projet :
      K1 la cible refuse un rewind sous la plus ancienne ligne rattachee
         (2025-02-01 sous 2025-03-01T10:00), en nommant la date disponible,
         et le preflight du rewind le bloque ;
      K2 l'historique rattache reste atteignable : entre la plus ancienne
         ligne rattachee et `since` (2025-03-03), accepte, et le preflight
         a une action ;
      K3 Review sur la cible pose son plancher a la seconde avant la plus
         ancienne ligne rattachee (garde de parite) ;
      K4 une couche modifiee dans la cible apres sa creation : ni refus
         (2020-01-01) ni plancher (garde contre le sur-refus) ;
      K5 la source migree refuse toujours sous sa plus ancienne ligne
         (garde).
  N journal v7 SANS tampon (cree par une v7 anterieure au tampon : les
    lecteurs deduisent de schema_version), une ligne plus ancienne que le
    journal (importee) et une ligne ecrite apres sa creation : le rewind
    refuse sous la premiere, accepte sous la seconde, comme Review.
  P "Enregistrer sous" reel d'un projet enregistre (`JournalManager` :
    `open_for_project`, `plan_history_carry`, `close`), rattachement
    partiel ; la source garde la modification E0 (2026-06-01) de la
    couche, la session ajoute E1 :
      P_B vers un dossier neuf, P_C vers un dossier dont le journal existe
      deja : la cible refuse un rewind au 2026-05-01 (la source, elle, a
      besoin de deux actions pour cette date ; la cible n'en a qu'une et
      ecrirait l'etat d'apres E0 comme etat du 2026-05-01), en nommant E1,
      et Review pose son plancher a la seconde avant E1.

Verdicts attendus
-----------------
SCENARIO DE VERROUILLAGE, ATTENDU EN ECHEC sur
k1_rewind_refuses_below_the_oldest_carried_row,
n_unstamped_journal_refuses_below_an_imported_row,
p_b_new_folder_refuses_below_the_carried_rows et
p_c_existing_folder_refuses_below_the_carried_rows, tant que le
rattachement n'ecrit pas, dans la
cible et par source de donnees rattachee, ce que sa source garantissait
sous la plus ancienne ligne rattachee (zone stockage :
`journal_manager.carry_journal_history` / `_carry_coverage`). Les lecteurs
du plancher ne peuvent pas le deduire : une regle qui comparait l'age de
la plus ancienne ligne au debut du journal faisait passer K1, N et P_B,
mais refusait aussi le premier enregistrement de tout nouveau projet (la
source, journal de session neuf, garantissait tout) et cassait "Annuler
toute la session" (rw_first_save_history_reachable) ; elle ne voyait pas
P_C. Elle a ete retenue. Les autres verifications (setup, K2 a K5,
n_unstamped_journal_own_rows_unbounded, p_setup_*) passent : elles gardent
de tout sur-refus le correctif a venir (un plancher pose a `since`
rendrait l'historique rattache inatteignable) et prouvent que le cas P est
bien partiel.

Cas S (information dans le journal du run, non verifie ici) : premier
enregistrement d'un projet ; rw_first_save_history_reachable le verifie.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rw_floor_carried_history"
INVARIANT = "I-RW-COVERAGE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_KEYS = ("coverage_recorded_since", "coverage_origin", "coverage_origin_schema")

_DS = "rw_fch_ds"
_OLDEST = "2025-03-01T10:00:00+00:00"
_LATER = "2025-03-05T10:00:00+00:00"
_BELOW = "2025-02-01T00:00:00+00:00"
_BETWEEN = "2025-03-03T00:00:00+00:00"
_FLOOR = "2025-03-01T09:59:59"
_OWN = "rw_fch_own"
_LONG_AGO = "2020-01-01T00:00:00+00:00"
# N: a row older than the journal (imported), asked below it.
_IMP = "rw_fch_imp"
_IMP_AT = "2026-03-01T10:00:00+00:00"
_IMP_BELOW = "2026-02-01T00:00:00+00:00"
_IMP_FLOOR = "2026-03-01T09:59:59"
# P: a Save As of a saved project (partial carry). The source journal was
# born on _P_BORN and holds E0 (last session) of _PART; the session adds E1.
_PART = "rw_fch_part"
_P_BORN = "2026-01-01T00:00:00+00:00"
_P_E0 = "2026-06-01T09:00:00+00:00"
_P_BELOW = "2026-05-01T00:00:00+00:00"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _capture(path: str, fingerprint: str, created_at: str, new: str) -> None:
    """One UPDATE (nom A -> *new*) written as the v7 capture writes it."""
    from recoverland.core.capture_outbox import new_commit_id, new_event_uid
    from recoverland.core.journal_writes import (
        insert_events_idempotent, record_capture_commit)
    commit_id = new_commit_id()
    row = {"project_fingerprint": "p", "datasource_fingerprint": fingerprint,
           "layer_id_snapshot": "l", "layer_name_snapshot": "parcelles",
           "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": 1}),
           "operation_type": "UPDATE",
           "attributes_json": json.dumps(
               {"changed_only": {"nom": {"old": "A", "new": new}}}),
           "geometry_type": "NoGeometry", "user_name": "u",
           "session_id": "s_fch_p", "created_at": created_at,
           "entity_fingerprint": "fid:1", "event_schema_version": 2,
           "event_uid": new_event_uid(), "commit_id": commit_id}
    conn = sqlite3.connect(path)
    try:
        with conn:
            inserted, _already, rejected = insert_events_idempotent(conn, [row])
            if inserted != 1 or rejected:
                raise RuntimeError(f"capture of {fingerprint} not written: "
                                   f"inserted={inserted} rejected={rejected}")
            record_capture_commit(conn, commit_id, "COMMITTED",
                                  datasource_fingerprint=fingerprint,
                                  session_id="s_fch_p", n_events=1)
    finally:
        conn.close()


def _born_at(path: str, instant: str) -> None:
    """Date a v7 journal's birth back to *instant* (a journal kept for months).

    Both records of the birth: the schema_version stamps and
    coverage_recorded_since ("created" kept).
    """
    conn = sqlite3.connect(path)
    try:
        with conn:
            conn.execute("UPDATE schema_version SET applied_at = ?", (instant,))
            conn.execute("UPDATE backend_settings SET setting_value = ? "
                         "WHERE setting_key = 'coverage_recorded_since'", (instant,))
    finally:
        conn.close()


def _saved_project(tmp: str, name: str, file_name: str = "projet.qgs") -> tuple:
    """A saved project file (.qgs: written before projectSaved, see
    HistoryCarry.project_stamp) and the journal of its folder."""
    pdir = os.path.join(tmp, name)
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, file_name)
    Path(project).write_text("<qgis/>", encoding="utf-8")
    return project, os.path.join(pdir, ".recoverland", "recoverland_audit.sqlite")


def _partial_carry(tmp: str, profile: str, case: str, folder_has_journal: bool) -> dict:
    """"Save As" of a saved project into another folder, through the real
    JournalManager; what the source and the target answer below E0."""
    from recoverland.core.journal_manager import JournalManager
    src_project, src = _saved_project(tmp, f"{case}_ancien")
    jm = JournalManager()
    jm.open_for_project(src_project, profile)
    jm.close()
    _born_at(src, _P_BORN)
    _capture(src, _PART, _P_E0, "B1")
    dst_project, dst = _saved_project(tmp, f"{case}_nouveau")
    if folder_has_journal:
        # Another project of that folder, saved and journaled for months.
        other, _same = _saved_project(tmp, f"{case}_nouveau", "autre.qgs")
        jm = JournalManager()
        jm.open_for_project(other, profile)
        jm.close()
        _born_at(dst, _P_BORN)
    # This session: the saved project opened, _PART edited, Save As.
    jm = JournalManager()
    jm.open_for_project(src_project, profile)
    e1 = _now()
    _capture(src, _PART, e1, "B2")
    carry = jm.plan_history_carry(dst_project, profile)
    jm.close()
    rep = jm.last_carry_report
    conn = sqlite3.connect(dst)
    try:
        rows = [r[0] for r in conn.execute(
            "SELECT created_at FROM audit_event WHERE datasource_fingerprint = ? "
            "ORDER BY event_id", (_PART,))]
    finally:
        conn.close()
    return {"since_event_id": getattr(carry, "since_event_id", None),
            "carry_ok": getattr(rep, "ok", None), "error": getattr(rep, "error", None),
            "e1": e1, "floor_expected": _second_before(e1), "target_rows": rows,
            "target_stamp": _stamp(dst),
            "source": _readers(src, (), ((_PART, _P_BELOW),), ()),
            "target": _readers(dst, ((_PART, _P_BELOW),), ((_PART, _P_BELOW),),
                               (_PART,))}


def _second_before(iso: str) -> str:
    """The date-bar value Review floors a row at (last second before it)."""
    from recoverland.widgets.snapshot_rebuild_worker import second_before
    return second_before(iso)


def _insert_event(conn: sqlite3.Connection, fingerprint: str, created_at: str,
                  fid: int = 1, new: str = "B") -> None:
    """One UPDATE of *fingerprint* (nom A -> *new*), in the caller's tx."""
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
    cols = list(AUDIT_EVENT_INSERT_COLUMNS)
    row = dict.fromkeys(cols)
    row.update({"project_fingerprint": "p", "datasource_fingerprint": fingerprint,
                "layer_id_snapshot": "l", "layer_name_snapshot": "parcelles",
                "provider_type": "ogr",
                "feature_identity_json": json.dumps({"fid": fid}),
                "operation_type": "UPDATE",
                "attributes_json": json.dumps(
                    {"changed_only": {"nom": {"old": "A", "new": new}}}),
                "geometry_type": "NoGeometry", "user_name": "u",
                "session_id": "s_fch", "created_at": created_at,
                "entity_fingerprint": f"fid:{fid}", "event_schema_version": 2})
    conn.execute(f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
                 f"VALUES ({','.join(['?'] * len(cols))})",
                 [row[c] for c in cols])


def _legacy_journal(path: str) -> None:
    """A journal shaped like the shipped schema 6, clock right (2025).

    Two events of _DS; nothing says what the old release purged before
    the first one.
    """
    from recoverland.core.sqlite_schema import initialize_schema
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
        with conn:
            conn.execute("DELETE FROM schema_version WHERE version_number >= 7")
            conn.execute("UPDATE schema_version SET applied_at = ?",
                         ("2025-01-15T09:00:00+00:00",))
            for table in ("journal_coverage", "journal_coverage_gap", "capture_commit"):
                conn.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            conn.execute("DELETE FROM backend_settings WHERE setting_key IN (?, ?, ?)",
                         _KEYS)
            _insert_event(conn, _DS, _OLDEST, new="B1")
            _insert_event(conn, _DS, _LATER, new="B2")
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


class _Journal:
    """What query_snapshot_floor needs of a journal: a read connection."""

    def __init__(self, path: str):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def _preflight(conn, ds: str, cutoff_value: str) -> dict:
    """The rewind's own question: window, dedup, plan, preflight(conn)."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events
    from recoverland.core.restore_planner import plan_temporal_restore, preflight_check
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_value, inclusive=True)
    events = collapse_rewind_events(fetch_events_after_cutoff(conn, ds, cutoff))
    plan = plan_temporal_restore(events, ds, ds, cutoff)
    report = preflight_check(plan, conn=conn)
    coverage = [r for r in report.blocking_reasons if "Retention coverage" in r]
    return {"verdict": str(report.verdict), "actions": len(plan.actions),
            "coverage": coverage}


def _readers(path: str, refusals: tuple, preflights: tuple, floors: tuple) -> dict:
    """What the real floor readers answer on *path*.

    ``refusal[fp@cutoff]``: retention.coverage_refusal, None when the date
    is accepted. ``preflight[fp@cutoff]``: restore_planner.preflight_check
    on the real plan. ``floor[fp]``: query_snapshot_floor (Review), ""
    when unbounded. A reader that raises is listed in ``errors`` (never
    read as a refusal).
    """
    out = {"refusal": {}, "preflight": {}, "floor": {}, "errors": []}
    conn = sqlite3.connect(path)
    try:
        from recoverland.core.retention import coverage_refusal
        for fingerprint, cutoff in refusals:
            try:
                out["refusal"][f"{fingerprint}@{cutoff}"] = coverage_refusal(
                    conn, fingerprint, cutoff)
            except Exception as exc:  # noqa: BLE001 - reported, never a refusal
                out["errors"].append(f"coverage_refusal {fingerprint}: "
                                     f"{type(exc).__name__}: {exc}")
        for fingerprint, cutoff in preflights:
            try:
                out["preflight"][f"{fingerprint}@{cutoff}"] = _preflight(
                    conn, fingerprint, cutoff)
            except Exception as exc:  # noqa: BLE001
                out["errors"].append(f"preflight {fingerprint}: "
                                     f"{type(exc).__name__}: {exc}")
    finally:
        conn.close()
    from recoverland.widgets.snapshot_rebuild_worker import query_snapshot_floor
    for fingerprint in floors:
        try:
            out["floor"][fingerprint] = query_snapshot_floor(
                _Journal(path), [{"fingerprint": fingerprint}])
        except Exception as exc:  # noqa: BLE001
            out["errors"].append(f"query_snapshot_floor {fingerprint}: "
                                 f"{type(exc).__name__}: {exc}")
    return out


def _project(tmp: str, name: str) -> tuple:
    pdir = os.path.join(tmp, name)
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    return project, os.path.join(pdir, ".recoverland", "recoverland_audit.sqlite")


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_rw_fch_")
    ctx.data["tmp"] = tmp
    flog(f"rw_floor_carried_history setup: trace_id={ctx.trace_id} tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.journal_manager import (
        HistoryCarry, JournalManager, _carry_into_journal)
    d = ctx.data
    tmp = d["tmp"]
    profile = os.path.join(tmp, "profil")
    os.makedirs(profile, exist_ok=True)
    try:
        # K: the old project's schema 6 journal, opened (migrated) ...
        project_old, j_old = _project(tmp, "ancien")
        _legacy_journal(j_old)
        jm = JournalManager()
        src = jm.open_for_project(project_old, profile)
        jm.close()
        d["src"] = {"path": src, "stamp": _stamp(src),
                    "readers": _readers(src, ((_DS, _BELOW),), (), ())}
        # ... then "Save As" to a new folder: the history follows into a
        # new project journal.
        project_new, j_new = _project(tmp, "nouveau")
        carry = HistoryCarry(source=src, source_kind="saved",
                             source_session_key="", target=j_new,
                             target_kind="saved", since_event_id=0,
                             opened_at="", mark_source=False,
                             project_file=project_new, profile_path="")
        rep = _carry_into_journal(carry)
        d["carry"] = {"ok": rep.ok, "error": rep.error,
                      "inserted": getattr(rep, "events_inserted", None)}
        # A layer first edited in the new project, after its creation.
        conn = sqlite3.connect(j_new)
        try:
            with conn:
                _insert_event(conn, _OWN, _now())
        finally:
            conn.close()
        d["dst"] = {"path": j_new, "stamp": _stamp(j_new),
                    "readers": _readers(
                        j_new,
                        ((_DS, _BELOW), (_DS, _BETWEEN), (_OWN, _LONG_AGO)),
                        ((_DS, _BELOW), (_DS, _BETWEEN)),
                        (_DS, _OWN))}
        # N: a v7 journal from before the stamp (the keys removed: the
        # readers deduce from schema_version), a row older than the
        # journal and a row written after its creation.
        from recoverland.core.sqlite_schema import initialize_schema
        j_n = os.path.join(tmp, "n", "recoverland_audit.sqlite")
        os.makedirs(os.path.dirname(j_n), exist_ok=True)
        conn = sqlite3.connect(j_n)
        try:
            initialize_schema(conn)
            with conn:
                conn.execute("DELETE FROM backend_settings WHERE setting_key IN "
                             "(?, ?, ?)", _KEYS)
                _insert_event(conn, _IMP, _IMP_AT)
                _insert_event(conn, _OWN, _now())
        finally:
            conn.close()
        d["n"] = {"stamp": _stamp(j_n),
                  "readers": _readers(j_n, ((_IMP, _IMP_BELOW), (_OWN, _LONG_AGO)),
                                      ((_IMP, _IMP_BELOW),), (_OWN, _IMP))}
        # S (information, not asserted here): a fresh session journal
        # (created) whose row is carried into a new project journal, the
        # first save of a project. The source vouched for it; the target
        # cannot tell, and must not refuse it (checked by
        # rw_first_save_history_reachable).
        jm = JournalManager()
        s_src = jm.open_for_project("", profile)
        jm.close()
        conn = sqlite3.connect(s_src)
        try:
            with conn:
                _insert_event(conn, _OWN, _now())
        finally:
            conn.close()
        project_s, j_s = _project(tmp, "premier_enregistrement")
        rep = _carry_into_journal(HistoryCarry(
            source=s_src, source_kind="session", source_session_key="",
            target=j_s, target_kind="saved", since_event_id=0, opened_at="",
            mark_source=False, project_file=project_s, profile_path=""))
        d["s_info"] = {"carry_ok": rep.ok, "source_stamp": _stamp(s_src),
                       "target_stamp": _stamp(j_s),
                       "readers": _readers(j_s, ((_OWN, _LONG_AGO),), (), (_OWN,))}
        # P: "Save As" of a saved project, into a new folder and into a
        # folder whose journal already exists.
        d["p_b"] = _partial_carry(tmp, profile, "pb", folder_has_journal=False)
        d["p_c"] = _partial_carry(tmp, profile, "pc", folder_has_journal=True)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"rw_floor_carried_history run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    src, dst, n = d.get("src") or {}, d.get("dst") or {}, d.get("n") or {}
    carry = d.get("carry") or {}
    sd = dst.get("stamp") or {}
    rd, rs, rn = dst.get("readers") or {}, src.get("readers") or {}, n.get("readers") or {}
    ref_d, pre_d, floor_d = (rd.get("refusal") or {}, rd.get("preflight") or {},
                             rd.get("floor") or {})
    below, between = f"{_DS}@{_BELOW}", f"{_DS}@{_BETWEEN}"
    own = f"{_OWN}@{_LONG_AGO}"
    out = []
    out.append((
        "setup_history_carried_into_a_created_journal",
        carry.get("ok") is True and sd.get("coverage_origin") == "created"
        and (src.get("stamp") or {}).get("coverage_origin") == "migrated",
        f"rattachement={carry} ; tampon de la cible={sd} (attendu created) ; "
        f"tampon de la source={src.get('stamp')} (attendu migrated){crash}",
    ))
    k1 = ref_d.get(below)
    k1_pre = pre_d.get(below) or {}
    out.append((
        "k1_rewind_refuses_below_the_oldest_carried_row",
        not rd.get("errors") and bool(k1) and _OLDEST in str(k1)
        and bool(k1_pre.get("coverage")),
        f"journal du nouveau projet (created depuis {sd.get('coverage_recorded_since')}), "
        f"historique rattache d'un journal migre : rewind {below} -> {k1!r} ; "
        f"preflight -> {k1_pre}. Accepte, le rewind rejoue ce qui a survecu "
        f"dans l'ancien journal et annonce comme etat du {_BELOW[:10]} l'etat a "
        f"la ligne d'une purge silencieuse de l'ancienne version ; le refus doit "
        f"nommer la plus ancienne date disponible ({_OLDEST}). "
        f"Erreurs={rd.get('errors')}{crash}",
    ))
    k2 = ref_d.get(between, "absent")
    k2_pre = pre_d.get(between) or {}
    out.append((
        "k2_carried_history_stays_reachable",
        not rd.get("errors") and k2 is None and not k2_pre.get("coverage")
        and k2_pre.get("actions", 0) >= 1,
        f"rewind {between} (entre la plus ancienne ligne rattachee et le debut "
        f"du journal) -> {k2!r} ; preflight -> {k2_pre} (attendus None, aucune "
        f"raison de couverture, au moins une action). Un plancher pose au debut "
        f"du journal rendrait tout l'historique rattache inatteignable : "
        f"l'utilisateur ne pourrait plus revenir a une date d'avant son "
        f"'Enregistrer sous'. Erreurs={rd.get('errors')}{crash}",
    ))
    out.append((
        "k3_review_agrees_on_the_carried_floor",
        not rd.get("errors") and floor_d.get(_DS) == _FLOOR,
        f"plancher Review de {_DS} dans la cible={floor_d.get(_DS)!r} (attendu "
        f"{_FLOOR!r}, la seconde avant la plus ancienne ligne rattachee) : "
        f"Review et le rewind doivent dire la meme chose{crash}",
    ))
    out.append((
        "k4_layer_edited_in_the_target_has_no_floor",
        not rd.get("errors") and own in ref_d and ref_d[own] is None
        and floor_d.get(_OWN) == "",
        f"couche modifiee dans le nouveau projet apres sa creation : rewind "
        f"{own} -> {ref_d.get(own, 'absent')!r} ; plancher Review="
        f"{floor_d.get(_OWN, 'absent')!r} (attendus None et '') : rien n'a pu "
        f"etre purge sans trace, un refus ici interdirait de revenir avant la "
        f"premiere modification{crash}",
    ))
    k5 = (rs.get("refusal") or {}).get(below)
    out.append((
        "k5_migrated_source_keeps_the_legacy_guard",
        not rs.get("errors") and bool(k5),
        f"ancien journal (migre) : rewind {below} -> {k5!r} (attendu un refus : "
        f"l'ancienne version a pu purger sans trace sous sa plus ancienne "
        f"ligne). Erreurs={rs.get('errors')}{crash}",
    ))
    ref_n, pre_n, floor_n = (rn.get("refusal") or {}, rn.get("preflight") or {},
                             rn.get("floor") or {})
    imp = f"{_IMP}@{_IMP_BELOW}"
    n_imp, n_pre = ref_n.get(imp), pre_n.get(imp) or {}
    out.append((
        "n_unstamped_journal_refuses_below_an_imported_row",
        not rn.get("errors") and bool(n_imp) and _IMP_AT in str(n_imp)
        and bool(n_pre.get("coverage")) and floor_n.get(_IMP) == _IMP_FLOOR,
        f"journal v7 sans tampon (tampon={n.get('stamp')}), ligne plus ancienne "
        f"que le journal : rewind {imp} -> {n_imp!r} ; preflight -> {n_pre} ; "
        f"plancher Review={floor_n.get(_IMP)!r} (attendu {_IMP_FLOOR!r}). "
        f"Review pose ce plancher ; le rewind qui accepte rend l'etat a la "
        f"plus ancienne ligne importee comme etat de la date demandee. "
        f"Erreurs={rn.get('errors')}{crash}",
    ))
    out.append((
        "n_unstamped_journal_own_rows_unbounded",
        not rn.get("errors") and own in ref_n and ref_n[own] is None
        and floor_n.get(_OWN) == "",
        f"meme journal, ligne ecrite apres sa creation : rewind {own} -> "
        f"{ref_n.get(own, 'absent')!r} ; plancher Review={floor_n.get(_OWN, 'absent')!r} "
        f"(attendus None et ''){crash}",
    ))
    below_p = f"{_PART}@{_P_BELOW}"
    p_setup = []
    for case in ("p_b", "p_c"):
        p = d.get(case) or {}
        src_pre = ((p.get("source") or {}).get("preflight") or {}).get(below_p) or {}
        dst_pre = ((p.get("target") or {}).get("preflight") or {}).get(below_p) or {}
        p_setup.append(
            p.get("carry_ok") is True and (p.get("since_event_id") or 0) >= 1
            and p.get("target_rows") == [p.get("e1")]
            and src_pre.get("actions") == 2 and not src_pre.get("coverage")
            and dst_pre.get("actions") == 1
            and (p.get("target_stamp") or {}).get("coverage_origin") == "created")
    out.append((
        "p_setup_save_as_carries_only_this_session",
        all(p_setup),
        f"'Enregistrer sous' d'un projet enregistre : dossier neuf={p_setup[0]}, "
        f"dossier avec journal={p_setup[1]} (attendus : rattachement reussi, "
        f"partiel, la cible ne tient que E1, la source a besoin de deux actions "
        f"au {_P_BELOW[:10]} et la cible n'en a qu'une). p_b={d.get('p_b')} "
        f"p_c={d.get('p_c')}{crash}",
    ))
    for case, where in (("p_b", "un dossier neuf"),
                        ("p_c", "un dossier dont le journal existe deja")):
        p = d.get(case) or {}
        tgt = p.get("target") or {}
        refusal = (tgt.get("refusal") or {}).get(below_p)
        pre = (tgt.get("preflight") or {}).get(below_p) or {}
        floor = (tgt.get("floor") or {}).get(_PART)
        name = ("p_b_new_folder_refuses_below_the_carried_rows" if case == "p_b"
                else "p_c_existing_folder_refuses_below_the_carried_rows")
        out.append((
            name,
            not tgt.get("errors") and bool(refusal) and str(p.get("e1")) in str(refusal)
            and bool(pre.get("coverage")) and floor == p.get("floor_expected"),
            f"'Enregistrer sous' vers {where} : l'historique de {_PART} avant cette "
            f"session (E0, {_P_E0}) est reste dans le journal d'origine. Rewind "
            f"{below_p} dans la cible -> {refusal!r} ; preflight -> {pre} ; "
            f"plancher Review={floor!r} (attendus : un refus nommant E1 "
            f"{p.get('e1')}, le preflight bloque, plancher "
            f"{p.get('floor_expected')!r}). Accepte, le rewind ecrit la valeur "
            f"d'apres E0 et l'annonce comme l'etat du {_P_BELOW[:10]}, ou elle "
            f"valait A. ATTENDU EN ECHEC tant que le rattachement ne dit pas a la "
            f"cible que l'historique anterieur est reste ailleurs (zone "
            f"stockage). Erreurs={tgt.get('errors')}{crash}",
        ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_floor_carried_history.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
