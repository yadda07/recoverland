"""tx_journal_coverage_origin_stamp - RecoverLand validation runtime.

Invariant: I-RVW-ASOF / I-RW-COVERAGE (Review et le rewind montrent l'etat
a la date T ou disent qu'ils ne le peuvent pas ; le plancher d'un journal
jamais purge est un fait enregistre, pas une deduction).

Le mecanisme
------------
Depuis v7 toute purge laisse une trace durable (`journal_coverage`,
`journal_coverage_gap`). Un journal qui a vecu sous une version plus
ancienne a pu etre purge sans trace : sous le plus ancien evenement
survivant, rien n'est garanti. Pour savoir dans quel cas on est, Review
(`snapshot_rebuild_worker.recording_journal_creation`) et le rewind
(`retention._journal_predates_v7`) DEDUISENT l'histoire du fichier des
horodatages de `schema_version` : versions < 7 et version 7 ecrites dans
la meme minute -> "journal cree par v7, jamais purge sans trace". Rien
dans le journal ne le dit explicitement ; une horloge fausse au moment
ou une version ancienne a cree le fichier (horodatages < 7 dans le futur)
suffit a faire passer un journal migre, peut-etre purge en silence, pour
un journal neuf.

Le scenario d'echec concret
---------------------------
Un poste dont l'horloge etait en avance d'un an a cree le journal avec la
version livree, qui a purge au demarrage (horizon garde en memoire
seulement). Horloge corrigee, passage en v7 : le rewind a une date
anterieure a cette purge est accepte (le journal passe pour neuf) et
annonce comme etat de cette date l'etat a la ligne de purge.

Ce que le scenario execute reellement
-------------------------------------
Vrai `JournalManager.open_for_project`, vrais fichiers :
  A journal de session neuf : il doit dire lui-meme quand il a commence a
    enregistrer toute purge (`coverage_recorded_since`) et qu'il n'a pas
    de vie anterieure (`coverage_origin` = created, schema anterieur 0).
  B journal de projet au schema 6 (horodatages de 2025) ouvert : migre, il
    doit le dire (origin = migrated, schema anterieur 6, depuis = la
    migration).
  C meme chose avec des horodatages < 7 dans le FUTUR (horloge fausse) :
    migrated malgre tout. Une couche de plus (`tx_jcos_late`) porte une
    ligne ecrite sous cette horloge (2027-10-01), apres la creation que
    disent les horodatages : c'est la ligne qu'une purge silencieuse de
    l'ancienne version laisse en premier.
  D reouverture de A : le tampon ne bouge pas.
  E journal v7 anterieur a ce tampon (table de couverture presente, pas
    de tampon) : rien n'est invente, les lecteurs gardent la deduction.
  F journal de projet cree par un rattachement d'historique a la
    fermeture : created lui aussi.
  G fichier neuf dont le schema est initialise SANS l'ouvreur (ce que
    laisse un arret brutal entre le commit de initialize_schema et
    l'ecriture de l'origine, qui se font en deux transactions ; c'est
    aussi le journal de tout outil qui initialise le schema seul) : il
    doit etre created. Sans origine il est lu comme migre, pour toujours.
  H journal au schema 6 dont l'ancienne version a purge TOUTES les lignes
    (table vide, mais `sqlite_sequence` garde le dernier event_id) :
    migrated a l'ouverture, et jamais created meme sans l'ouvreur. Garde
    contre le sous-refus : vide ne veut pas dire neuf.
  I journal au schema 6 qui n'a JAMAIS contenu de ligne (aucune entree
    `sqlite_sequence`) : rien n'a pu y etre purge, il est created (schema
    anterieur 6). La premiere modification apres la mise a jour doit
    pouvoir etre defaite : ni refus du rewind ni plancher Review sous
    elle.

Ecrire le tampon ne suffit pas : ce sont les LECTEURS du plancher qui
decident. Le scenario interroge donc les vrais lecteurs sur les memes
fichiers :
  * le rewind, `retention.coverage_refusal` (zone rewind : il passe par
    `retention._journal_predates_v7`) ;
  * Review, `snapshot_rebuild_worker.query_snapshot_floor` (zone review :
    il passe par `recording_journal_creation`).
  C : une date sous la plus ancienne ligne survivante d'une couche doit
    etre refusee par le rewind (2025-02-01 sous 2025-03-01 ; 2027-09-15
    sous 2027-10-01) et Review doit poser son plancher a la seconde avant
    2027-10-01 pour `tx_jcos_late`. Tant que ces lecteurs deduisent de
    l'horloge, le rewind accepte les deux dates et Review n'a aucun
    plancher : l'etat a la ligne de purge est montre comme l'etat demande.
  I : sous la premiere modification apres la mise a jour d'un journal qui
    n'a jamais contenu de ligne, ni refus ni plancher. La deduction y voit
    des horodatages < 7 anciens et pose la garde historique : "History
    purged before this date by an older version", sur un journal ou rien
    n'a jamais ete purge, et l'utilisateur ne peut pas revenir avant sa
    premiere modification.
  B (garde) : le journal migre a horloge juste garde ce refus et ce
    plancher (2025-03-01T09:59:59).
  A (garde) : dans le journal cree, une ligne ecrite apres sa creation ne
    donne ni refus ni plancher (aucune vie anterieure a garder).

Verdicts attendus
-----------------
Avant le tampon : FAIL (A, B, C, D, F : aucun tampon).
Origine ecrite par l'ouvreur seul, dans sa propre transaction : FAIL sur
G (aucune origine) et I (migrated).
Tampon complet, lecteurs encore sur la deduction (etat de la zone stockage
seule) : FAIL sur `c_rewind_refuses_below_surviving_rows`,
`c_review_floors_below_surviving_rows` et
`i_readers_let_the_user_go_before_the_first_edit`. Ils passent quand
`retention._journal_predates_v7` et `recording_journal_creation` lisent
d'abord `journal_writes.coverage_origin` (created -> jamais purge sans
trace, depuis `since` ; migrated ou "" -> garde historique ; None ->
deduction), correctif des zones rewind et review.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_journal_coverage_origin_stamp"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_KEYS = ("coverage_recorded_since", "coverage_origin", "coverage_origin_schema")
# The only row of tx_jcos_ds (B and C): 2025-03-01T10:00:00. Asked below it.
_BELOW_DS = ("tx_jcos_ds", "2025-02-01T00:00:00+00:00")
# C: a row the old release wrote under the clock ahead, after the creation
# its schema_version stamps claim (2027-09-01). Asked below it.
_LATE = ("tx_jcos_late", "2027-10-01T10:00:00+00:00")
_BELOW_LATE = ("tx_jcos_late", "2027-09-15T00:00:00+00:00")
# Review's floor: the last second strictly before the oldest row.
_FLOOR_DS = "2025-03-01T09:59:59"
_FLOOR_LATE = "2027-10-01T09:59:59"
# A: a row journaled after the creation, asked long before it.
_BELOW_NEW = ("tx_jcos_new", "2020-01-01T00:00:00+00:00")
# I: the first row ever journaled in a legacy journal, after its upgrade.
_BELOW_FIRST = ("tx_jcos_first", "2020-01-01T00:00:00+00:00")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _inferences(path: str) -> dict:
    """What the clock-based inferences say about the file (information)."""
    out = {}
    conn = sqlite3.connect(path)
    try:
        try:
            from recoverland.core.retention import _journal_predates_v7
            out["rewind_predates_v7"] = _journal_predates_v7(conn)
        except Exception as exc:  # noqa: BLE001
            out["rewind_predates_v7"] = f"{type(exc).__name__}"
        try:
            from recoverland.widgets.snapshot_rebuild_worker import (
                recording_journal_creation)
            created = recording_journal_creation(conn)
            out["review_created"] = created.isoformat() if created else None
        except Exception as exc:  # noqa: BLE001
            out["review_created"] = f"{type(exc).__name__}"
        try:
            from recoverland.core.journal_writes import coverage_origin
            out["helper"] = coverage_origin(conn)
        except Exception as exc:  # noqa: BLE001 - pre-fix: no helper
            out["helper"] = f"{type(exc).__name__}"
    finally:
        conn.close()
    return out


def _insert_event(conn: sqlite3.Connection, fingerprint: str, created_at: str) -> None:
    """One UPDATE event of *fingerprint* at *created_at*, in the caller's tx."""
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
    cols = list(AUDIT_EVENT_INSERT_COLUMNS)
    row = dict.fromkeys(cols)
    row.update({"project_fingerprint": "p", "datasource_fingerprint": fingerprint,
                "layer_id_snapshot": "l", "layer_name_snapshot": "l",
                "provider_type": "ogr", "feature_identity_json": '{"fid": 1}',
                "operation_type": "UPDATE", "attributes_json": '{"changed_only": {}}',
                "geometry_type": "Point", "user_name": "u",
                "created_at": created_at,
                "entity_fingerprint": "fid:1", "event_schema_version": 2})
    conn.execute(f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
                 f"VALUES ({','.join(['?'] * len(cols))})",
                 [row[c] for c in cols])


def _legacy_journal(path: str, old_stamp: str, extra: tuple = (),
                    base: bool = True) -> None:
    """A journal shaped like the shipped schema 6: no v7 tables, no stamp.

    One event of tx_jcos_ds at 2025-03-01 (none when *base* is False),
    plus *extra* (fingerprint, created_at) events.
    """
    from recoverland.core.sqlite_schema import initialize_schema
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
        with conn:
            conn.execute("DELETE FROM schema_version WHERE version_number >= 7")
            conn.execute("UPDATE schema_version SET applied_at = ?", (old_stamp,))
            for table in ("journal_coverage", "journal_coverage_gap", "capture_commit"):
                conn.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
            conn.execute("DELETE FROM backend_settings WHERE setting_key IN (?, ?, ?)",
                         _KEYS)
            if base:
                _insert_event(conn, "tx_jcos_ds", "2025-03-01T10:00:00+00:00")
            for fingerprint, created_at in extra:
                _insert_event(conn, fingerprint, created_at)
    finally:
        conn.close()


def _events_and_sequence(path: str) -> dict:
    """Rows of audit_event, and the largest event_id SQLite ever handed out."""
    conn = sqlite3.connect(path)
    try:
        rows = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
        seq = conn.execute("SELECT seq FROM sqlite_sequence "
                           "WHERE name = 'audit_event'").fetchone()
    except sqlite3.Error as exc:
        return {"error": str(exc)}
    finally:
        conn.close()
    return {"rows": rows, "seq": seq[0] if seq else None}


def _schema_alone(path: str) -> None:
    """initialize_schema on *path* WITHOUT the opener's origin write.

    The file a kill between initialize_schema's commit and the origin
    write leaves, and the journal of any tool that initializes the schema
    alone.
    """
    from recoverland.core.sqlite_schema import initialize_schema
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        initialize_schema(conn)
    finally:
        conn.close()


class _Journal:
    """What query_snapshot_floor needs of a journal: a read connection."""

    def __init__(self, path: str):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def _readers(path: str, refusals: tuple, floors: tuple) -> dict:
    """What the real floor readers answer on *path*.

    ``refusal[fp@cutoff]``: retention.coverage_refusal (the rewind
    preflight), None when the date is accepted. ``floor[fp]``:
    snapshot_rebuild_worker.query_snapshot_floor (Review), "" when
    unbounded. A reader that raises is listed in ``errors`` (never read
    as a refusal).
    """
    out = {"refusal": {}, "floor": {}, "errors": []}
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
    finally:
        conn.close()
    for fingerprint in floors:
        try:
            from recoverland.widgets.snapshot_rebuild_worker import query_snapshot_floor
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


def _open(project: str, profile: str) -> tuple:
    from recoverland.core.journal_manager import JournalManager
    jm = JournalManager()
    before = _now()
    path = jm.open_for_project(project, profile)
    after = _now()
    jm.close()
    return path, before, after


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    tmp = tempfile.mkdtemp(prefix="rl_tx_jcos_")
    ctx.data["tmp"] = tmp
    flog(f"tx_journal_coverage_origin_stamp setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    tmp = d["tmp"]
    profile = os.path.join(tmp, "profil")
    os.makedirs(profile, exist_ok=True)
    try:
        # A: a fresh session journal.
        a_path, before, after = _open("", profile)
        d["a"] = {"path": a_path, "before": before, "after": after,
                  "stamp": _stamp(a_path), "inferred": _inferences(a_path)}
        # B: a schema 6 journal of a saved project, then opened.
        project_b, jb = _project(tmp, "projet_b")
        _legacy_journal(jb, "2025-01-15T09:00:00+00:00")
        b_path, before, after = _open(project_b, profile)
        d["b"] = {"path": b_path, "same": os.path.normcase(b_path) == os.path.normcase(jb),
                  "before": before, "after": after, "stamp": _stamp(b_path),
                  "inferred": _inferences(b_path),
                  "readers": _readers(b_path, (_BELOW_DS,), (_BELOW_DS[0],))}
        # C: the same, created under a clock one year ahead.
        project_c, jc = _project(tmp, "projet_c")
        _legacy_journal(jc, "2027-09-01T09:00:00+00:00", extra=(_LATE,))
        c_path, before, after = _open(project_c, profile)
        d["c"] = {"path": c_path, "before": before, "after": after,
                  "stamp": _stamp(c_path), "inferred": _inferences(c_path),
                  "readers": _readers(c_path, (_BELOW_DS, _BELOW_LATE), (_LATE[0],))}
        # D: A reopened (the session journal of an unsaved project is keyed
        # at random: reopen the file itself as the project's journal would).
        from recoverland.core.journal_manager import JournalManager
        jm = JournalManager()
        jm._open_connection(a_path)
        jm._conn.close()
        jm._conn = None
        d["d"] = {"stamp": _stamp(a_path)}
        # E: a v7 journal from before the stamp: the table is there, no stamp.
        project_e, je = _project(tmp, "projet_e")
        os.makedirs(os.path.dirname(je), exist_ok=True)
        from recoverland.core.sqlite_schema import initialize_schema
        conn = sqlite3.connect(je)
        try:
            initialize_schema(conn)
            with conn:
                conn.execute("DELETE FROM backend_settings WHERE setting_key IN "
                             "(?, ?, ?)", _KEYS)
        finally:
            conn.close()
        e_path, _b, _a = _open(project_e, profile)
        d["e"] = {"stamp": _stamp(e_path), "inferred": _inferences(e_path)}
        # F: a project journal created by a history carry at close.
        from recoverland.core.journal_manager import HistoryCarry, _carry_into_journal
        project_f, jf = _project(tmp, "projet_f")
        carry = HistoryCarry(source=a_path, source_kind="session",
                             source_session_key="", target=jf,
                             target_kind="saved", since_event_id=0,
                             opened_at="", mark_source=False,
                             project_file=project_f, profile_path="")
        before = _now()
        rep = _carry_into_journal(carry)
        after = _now()
        d["f"] = {"ok": rep.ok, "error": rep.error, "before": before, "after": after,
                  "stamp": _stamp(jf) if os.path.isfile(jf) else {}}
        # G: a fresh file, schema initialized without the opener.
        jg = os.path.join(tmp, "g", "recoverland_audit.sqlite")
        _schema_alone(jg)
        d["g"] = {"stamp": _stamp(jg)}
        # H: a schema 6 journal whose rows the old release ALL purged
        # (DELETE: sqlite_sequence still holds the last event_id). Opened
        # by the real opener, and a copy initialized without it.
        project_h, jh = _project(tmp, "projet_h")
        _legacy_journal(jh, "2025-01-15T09:00:00+00:00")
        conn = sqlite3.connect(jh)
        try:
            with conn:
                conn.execute("DELETE FROM audit_event")
        finally:
            conn.close()
        jh_alone = os.path.join(tmp, "h_alone", "recoverland_audit.sqlite")
        os.makedirs(os.path.dirname(jh_alone), exist_ok=True)
        shutil.copyfile(jh, jh_alone)
        seq_h = _events_and_sequence(jh)
        h_path, _b, _a = _open(project_h, profile)
        _schema_alone(jh_alone)
        d["h"] = {"before_open": seq_h, "stamp": _stamp(h_path),
                  "alone": _stamp(jh_alone)}
        # I: a schema 6 journal that never held a row, opened; then the
        # first edit after the upgrade, and the readers below it.
        project_i, ji = _project(tmp, "projet_i")
        _legacy_journal(ji, "2025-01-15T09:00:00+00:00", base=False)
        seq_i = _events_and_sequence(ji)
        i_path, _b, _a = _open(project_i, profile)
        conn = sqlite3.connect(i_path)
        try:
            with conn:
                _insert_event(conn, _BELOW_FIRST[0], _now())
        finally:
            conn.close()
        d["i"] = {"before_open": seq_i, "stamp": _stamp(i_path),
                  "inferred": _inferences(i_path),
                  "readers": _readers(i_path, (_BELOW_FIRST,), (_BELOW_FIRST[0],))}
        # A again, last (F carried it as it was): a row journaled after its
        # creation, then what the readers answer below it.
        conn = sqlite3.connect(a_path)
        try:
            with conn:
                _insert_event(conn, _BELOW_NEW[0], _now())
        finally:
            conn.close()
        d["a"]["readers"] = _readers(a_path, (_BELOW_NEW,), (_BELOW_NEW[0],))
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"tx_journal_coverage_origin_stamp run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp", ""), ignore_errors=True)


def _within(stamp: dict, case: dict) -> bool:
    since = (stamp or {}).get("coverage_recorded_since") or ""
    return bool(since) and case.get("before", "") <= since <= case.get("after", "~")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    a, b, c = d.get("a") or {}, d.get("b") or {}, d.get("c") or {}
    dd, e, f = d.get("d") or {}, d.get("e") or {}, d.get("f") or {}
    sa, sb, sc = a.get("stamp") or {}, b.get("stamp") or {}, c.get("stamp") or {}
    out = []
    out.append((
        "a_fresh_journal_states_its_origin",
        _within(sa, a) and sa.get("coverage_origin") == "created"
        and sa.get("coverage_origin_schema") == "0",
        f"journal neuf : tampon={sa} ouverture entre {a.get('before')} et "
        f"{a.get('after')} ; attendu created, schema anterieur 0, depuis "
        f"l'ouverture. Sans lui, Review et le rewind devinent son histoire "
        f"d'apres des horodatages{crash}",
    ))
    out.append((
        "b_migrated_journal_states_its_migration",
        b.get("same") is True and _within(sb, b)
        and sb.get("coverage_origin") == "migrated"
        and sb.get("coverage_origin_schema") == "6",
        f"journal au schema 6 ouvert : tampon={sb} ; attendu migrated, schema "
        f"anterieur 6, depuis la migration ({b.get('before')} .. "
        f"{b.get('after')}){crash}",
    ))
    # Named after what it checks: the stamp. Whether the readers still take
    # this journal for a fresh one is checked on its own below (c_rewind_*,
    # c_review_*); the former name claimed that and it was not checked.
    out.append((
        "c_clock_ahead_journal_stamped_migrated",
        _within(sc, c) and sc.get("coverage_origin") == "migrated",
        f"journal au schema 6 cree sous une horloge en avance : tampon={sc} "
        f"(attendu migrated). Deductions des lecteurs : {c.get('inferred')}"
        f"{crash}",
    ))
    rc, rb, ra = c.get("readers") or {}, b.get("readers") or {}, a.get("readers") or {}
    ref_c = rc.get("refusal") or {}
    below_ds, below_late = "@".join(_BELOW_DS), "@".join(_BELOW_LATE)
    out.append((
        "c_rewind_refuses_below_surviving_rows",
        not rc.get("errors") and below_ds in ref_c and below_late in ref_c
        and bool(ref_c[below_ds]) and bool(ref_c[below_late]),
        f"journal migre (tampon {sc.get('coverage_origin')!r}) dont les "
        f"horodatages < 7 sont en avance : refus du rewind sous la plus "
        f"ancienne ligne survivante, {below_ds} -> {ref_c.get(below_ds)!r} ; "
        f"{below_late} -> {ref_c.get(below_late)!r}. None = date acceptee et "
        f"annoncee exacte alors que l'ancienne version a pu purger sans trace "
        f"ce qui precede cette ligne : l'utilisateur recoit l'etat a la ligne "
        f"de purge comme l'etat demande. Correctif zone rewind : "
        f"retention._journal_predates_v7 lit journal_writes.coverage_origin "
        f"d'abord (migrated ou '' -> True). Erreurs={rc.get('errors')}{crash}",
    ))
    floor_late = (rc.get("floor") or {}).get(_LATE[0])
    out.append((
        "c_review_floors_below_surviving_rows",
        not rc.get("errors") and floor_late == _FLOOR_LATE,
        f"plancher Review de {_LATE[0]} dans ce journal migre = {floor_late!r} "
        f"(attendu {_FLOOR_LATE!r}, la seconde avant sa plus ancienne ligne). "
        f"'' = aucun plancher : Review rebatit une date anterieure a une purge "
        f"silencieuse et l'affiche comme l'etat de cette date. Correctif zone "
        f"review : snapshot_rebuild_worker.recording_journal_creation lit "
        f"journal_writes.coverage_origin d'abord (migrated ou '' -> None). "
        f"Erreurs={rc.get('errors')}{crash}",
    ))
    ref_b = (rb.get("refusal") or {}).get(below_ds)
    floor_b = (rb.get("floor") or {}).get(_BELOW_DS[0])
    out.append((
        "b_readers_keep_the_legacy_guard",
        not rb.get("errors") and bool(ref_b) and floor_b == _FLOOR_DS,
        f"journal migre a horloge juste : refus du rewind {below_ds} -> "
        f"{ref_b!r} ; plancher Review = {floor_b!r} (attendu {_FLOOR_DS!r}). "
        f"Sans eux, une date d'avant une purge silencieuse de l'ancienne "
        f"version serait rendue comme exacte. Erreurs={rb.get('errors')}{crash}",
    ))
    below_new = "@".join(_BELOW_NEW)
    ref_a = ra.get("refusal") or {}
    floor_a = (ra.get("floor") or {}).get(_BELOW_NEW[0], "absent")
    out.append((
        "a_readers_add_no_floor_to_a_created_journal",
        not ra.get("errors") and below_new in ref_a and ref_a[below_new] is None
        and floor_a == "",
        f"journal cree (tampon {sa.get('coverage_origin')!r}), ligne ecrite "
        f"apres sa creation : refus du rewind {below_new} -> "
        f"{ref_a.get(below_new, 'absent')!r} ; plancher Review = {floor_a!r} "
        f"(attendus None et '') : rien n'a pu etre purge sans trace, un refus "
        f"ici interdirait de revenir avant la premiere modification. "
        f"Erreurs={ra.get('errors')}{crash}",
    ))
    out.append((
        "d_stamp_is_stable_across_reopen",
        bool(sa.get("coverage_recorded_since")) and dd.get("stamp") == sa,
        f"tampon a la creation={sa} ; a la reouverture={dd.get('stamp')} : le "
        f"depart de l'enregistrement des purges ne se deplace jamais",
    ))
    out.append((
        "e_unstamped_v7_journal_left_to_the_fallback",
        (e.get("stamp") or {}) == dict.fromkeys(_KEYS),
        f"journal v7 anterieur au tampon : tampon apres ouverture="
        f"{e.get('stamp')} (attendu aucun : rien ne prouve son histoire, les "
        f"lecteurs gardent la deduction)",
    ))
    sf = f.get("stamp") or {}
    out.append((
        "f_carry_created_journal_states_its_origin",
        f.get("ok") is True and _within(sf, f)
        and sf.get("coverage_origin") == "created",
        f"journal de projet cree par le rattachement : ok={f.get('ok')} "
        f"erreur={f.get('error')!r} tampon={sf}{crash}",
    ))
    g, h, i = d.get("g") or {}, d.get("h") or {}, d.get("i") or {}
    sg = g.get("stamp") or {}
    out.append((
        "g_schema_alone_states_a_fresh_file_created",
        bool(sg.get("coverage_recorded_since")) and sg.get("coverage_origin") == "created",
        f"fichier neuf, schema initialise sans l'ouvreur (ce que laisse un arret "
        f"brutal entre le commit du schema et l'ecriture de l'origine) : "
        f"tampon={sg} ; attendu created. Sans origine, le journal est lu comme "
        f"migre pour toujours : revenir avant sa premiere modification est "
        f"refuse alors que rien n'y a jamais ete purge{crash}",
    ))
    sh, sh_alone, seq_h = h.get("stamp") or {}, h.get("alone") or {}, h.get("before_open") or {}
    out.append((
        "h_fully_purged_legacy_journal_never_stamped_created",
        seq_h.get("rows") == 0 and (seq_h.get("seq") or 0) > 0
        and sh.get("coverage_origin") == "migrated"
        and sh.get("coverage_origin_schema") == "6"
        and bool(sh_alone.get("coverage_recorded_since"))
        and sh_alone.get("coverage_origin") != "created",
        f"journal au schema 6 dont l'ancienne version a purge toutes les lignes "
        f"(avant ouverture : {seq_h}) : tampon a l'ouverture={sh} (attendu "
        f"migrated, 6) ; schema initialise sans l'ouvreur={sh_alone} (attendu "
        f"tout sauf created). Un journal vide n'est pas un journal neuf : pris "
        f"pour neuf, une date d'avant ces purges serait rebatie et annoncee "
        f"exacte{crash}",
    ))
    si, seq_i = i.get("stamp") or {}, i.get("before_open") or {}
    out.append((
        "i_legacy_journal_that_never_held_a_row_is_created",
        seq_i.get("rows") == 0 and not seq_i.get("seq")
        and si.get("coverage_origin") == "created"
        and si.get("coverage_origin_schema") == "6",
        f"journal au schema 6 qui n'a jamais contenu de ligne (avant ouverture : "
        f"{seq_i}) : tampon={si} ; attendu created, schema anterieur 6. Rien "
        f"n'a pu y etre purge ; lu comme migre, il interdirait a l'utilisateur "
        f"de revenir avant sa premiere modification apres la mise a jour{crash}",
    ))
    ri = i.get("readers") or {}
    below_first = "@".join(_BELOW_FIRST)
    ref_i = ri.get("refusal") or {}
    floor_i = (ri.get("floor") or {}).get(_BELOW_FIRST[0], "absent")
    out.append((
        "i_readers_let_the_user_go_before_the_first_edit",
        not ri.get("errors") and below_first in ref_i and ref_i[below_first] is None
        and floor_i == "",
        f"meme journal, premiere modification apres la mise a jour : refus du "
        f"rewind {below_first} -> {ref_i.get(below_first, 'absent')!r} ; "
        f"plancher Review = {floor_i!r} (attendus None et ''). Deductions des "
        f"lecteurs : {i.get('inferred')}. Un refus ici empeche de defaire la "
        f"premiere journee de travail apres la mise a jour. Correctif zones "
        f"rewind et review : lire journal_writes.coverage_origin d'abord "
        f"(created -> jamais purge sans trace). Erreurs={ri.get('errors')}{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_journal_coverage_origin_stamp.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
