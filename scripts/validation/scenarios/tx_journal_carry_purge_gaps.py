"""tx_journal_carry_purge_gaps - RecoverLand validation runtime.

Invariant: I-RW-COVERAGE (un rewind refuse une date que le journal ne
peut plus reconstruire, et le dit ; aucun etat invente presente comme
l'etat demande).

Le mecanisme
------------
Une purge LOGIQUE (paires INSERT/DELETE, traces invalidees) supprime
l'historique d'un INTERVALLE de dates, pas tout ce qui precede. Depuis v7
elle l'inscrit dans le journal, table `journal_coverage_gap` (une ligne
par membre de la famille d'alias de la source), et le preflight du rewind
refuse toute date tombant dans l'intervalle (`retention.coverage_refusal`
: "History of this layer purged between ..."). Review lit la meme table
(`retention.purged_horizon`, `journal_coverage`).

Quand l'historique suit un projet enregistre (`carry_journal_history` :
journal de session vers journal du projet, "Enregistrer sous", rattachement
du a l'ouverture suivante), `_carry_coverage` recopiait l'horizon de purge
(`journal_coverage`) mais PAS les intervalles : le journal du projet
recevait les evenements survivants et rien ne disait qu'un intervalle
avait ete purge. Pour un rattachement partiel (depuis l'ouverture de la
session), le filtre sur les empreintes des evenements rattaches ignorait
en plus les lignes tenues par un autre membre de la famille d'alias.

Le scenario d'echec concret
---------------------------
L'utilisateur edite une couche dans un projet pas encore enregistre ; la
maintenance purge une paire INSERT/DELETE (le 3 au 5 mars). Il enregistre
le projet : l'historique part dans le journal du projet. Le lendemain il
demande un rewind au 4 mars : le preflight l'accepte, le rewind rejoue ce
qui a survecu et annonce "etat au 4 mars" alors que l'entite creee puis
supprimee dans l'intervalle n'y apparait pas. Review au 4 mars affiche le
meme etat faux, sans avertissement.

Ce que le scenario execute reellement
-------------------------------------
Vrais journaux (`initialize_schema`), vrai `carry_journal_history`, vrai
`retention.coverage_refusal`. Le journal source tient des evenements de la
couche, un alias (ancienne empreinte -> empreinte actuelle), et deux
intervalles purges tels qu'une session precedente les a ecrits : l'un
tenu par l'empreinte actuelle, l'autre par l'ancienne seule (inscrit avant
que l'alias n'existe).
  A rattachement complet (journal de session -> journal de projet), DEUX
    fois : les intervalles sont dans la cible, sans doublon ; la cible
    refuse un rewind dans chacun, accepte une date hors intervalle ; une
    ligne de la cible pour une autre couche n'est pas touchee.
  B rattachement partiel (depuis un evenement, "Enregistrer sous") vers un
    autre projet : les deux intervalles de la couche rattachee suivent,
    y compris celui tenu par l'ancienne empreinte.

Verdict attendu avant correctif : FAIL (A et B : aucun intervalle dans la
cible, le rewind au 4 mars est accepte).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_journal_carry_purge_gaps"
INVARIANT = "I-RW-COVERAGE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Same shape as retention._GAP_DDL: rows as a purge of an earlier session
# wrote them (the scenario does not call the purge, whose session memory
# would answer for the journal).
_GAP_DDL = (
    "CREATE TABLE IF NOT EXISTS journal_coverage_gap ("
    "datasource_fingerprint TEXT NOT NULL, "
    "gap_from TEXT NOT NULL, "
    "gap_through TEXT NOT NULL, "
    "kind TEXT, "
    "recorded_at TEXT NOT NULL, "
    "PRIMARY KEY (datasource_fingerprint, gap_from, gap_through))"
)

_GAP_CUR = ("2026-03-03T08:00:00+00:00", "2026-03-05T18:00:00+00:00")
_GAP_OLD = ("2026-03-06T00:00:00+00:00", "2026-03-07T00:00:00+00:00")
_IN_CUR = "2026-03-04T12:00:00+00:00"
_IN_OLD = "2026-03-06T12:00:00+00:00"
_OUTSIDE = "2026-03-08T12:00:00+00:00"


def _names(trace_id: str) -> dict:
    tag = (trace_id or "x").replace("-", "")[:10]
    return {"cur": f"tx_jcpg_ds_{tag}", "old": f"tx_jcpg_ds_old_{tag}",
            "other": f"tx_jcpg_other_{tag}"}


def _new_journal(path: str) -> sqlite3.Connection:
    from recoverland.core.sqlite_schema import initialize_schema
    conn = sqlite3.connect(path, check_same_thread=False)
    initialize_schema(conn)
    return conn


def _insert(conn, ds: str, fid: int, created: str, uid: str) -> int:
    from recoverland.core.sqlite_schema import AUDIT_EVENT_INSERT_COLUMNS
    cols = list(AUDIT_EVENT_INSERT_COLUMNS) + ["event_uid", "commit_id"]
    values = {
        "project_fingerprint": "tx_jcpg", "datasource_fingerprint": ds,
        "layer_id_snapshot": "lyr", "layer_name_snapshot": "parcelles",
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": "UPDATE",
        "attributes_json": json.dumps({"changed_only": {"nom": {"old": "A", "new": uid}}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": "EPSG:2154",
        "field_schema_json": None, "user_name": "tester", "session_id": "s_jcpg",
        "created_at": created, "restored_from_event_id": None,
        "entity_fingerprint": f"fid:{fid}", "event_schema_version": 2,
        "new_geometry_wkb": None, "invalidated_at": None,
        "event_uid": uid, "commit_id": None,
    }
    cur = conn.execute(
        f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
        f"VALUES ({','.join(['?'] * len(cols))})", [values[c] for c in cols])
    return int(cur.lastrowid)


def _gap_rows(conn) -> list:
    try:
        return sorted(tuple(r) for r in conn.execute(
            "SELECT datasource_fingerprint, gap_from, gap_through, kind "
            "FROM journal_coverage_gap"))
    except sqlite3.Error:
        return []


def _source(path: str, n: dict) -> dict:
    conn = _new_journal(path)
    try:
        with conn:
            first = _insert(conn, n["cur"], 1, "2026-03-01T10:00:00+00:00", "u-jcpg-1")
            _insert(conn, n["cur"], 2, "2026-03-02T10:00:00+00:00", "u-jcpg-2")
            _insert(conn, n["cur"], 1, "2026-03-09T10:00:00+00:00", "u-jcpg-3")
            conn.execute(
                "INSERT INTO datasource_alias (alias_fingerprint, "
                "target_fingerprint, created_at, note) VALUES (?, ?, ?, ?)",
                (n["old"], n["cur"], "2026-03-08T00:00:00+00:00", "tx_jcpg"))
            conn.execute(_GAP_DDL)
            conn.executemany(
                "INSERT INTO journal_coverage_gap (datasource_fingerprint, "
                "gap_from, gap_through, kind, recorded_at) VALUES (?,?,?,?,?)",
                [(n["cur"], _GAP_CUR[0], _GAP_CUR[1], "insert_delete_pairs",
                  "2026-03-06T09:00:00+00:00"),
                 # Written before the alias existed: held by the old
                 # fingerprint alone.
                 (n["old"], _GAP_OLD[0], _GAP_OLD[1], "invalidated",
                  "2026-03-07T09:00:00+00:00")])
        return {"first_event_id": first, "gaps": _gap_rows(conn)}
    finally:
        conn.close()


def _refusals(conn, ds: str) -> dict:
    from recoverland.core.retention import coverage_refusal, journal_coverage
    out = {}
    for label, date in (("in_cur", _IN_CUR), ("in_old", _IN_OLD),
                        ("outside", _OUTSIDE)):
        try:
            out[label] = coverage_refusal(conn, ds, date)
        except Exception as exc:  # noqa: BLE001 - reported as a FAIL
            out[label] = f"CRASH {type(exc).__name__}: {exc}"
    try:
        cov = journal_coverage(conn, ds)
        out["review_gaps"] = [list(g) for g in cov.get("gaps") or []]
        out["review_coverage_gap"] = bool(cov.get("coverage_gap"))
    except Exception as exc:  # noqa: BLE001
        out["review_gaps"] = f"CRASH {type(exc).__name__}: {exc}"
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmp = tempfile.mkdtemp(prefix="rl_tx_jcpg_")
    ctx.data["tmp"] = tmp
    flog(f"tx_journal_carry_purge_gaps setup: trace_id={ctx.trace_id} "
         f"tmp={tmp}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.journal_manager import carry_journal_history

    d = ctx.data
    n = _names(ctx.trace_id)
    d["names"] = n
    tmp = d["tmp"]
    try:
        src = os.path.join(tmp, "session.sqlite")
        info = _source(src, n)
        d["source_gaps"] = [list(g) for g in info["gaps"]]

        # A: full carry, twice, into a project journal that already holds a
        # purged interval of another layer.
        dst = os.path.join(tmp, "projet_a", "recoverland_audit.sqlite")
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        dconn = _new_journal(dst)
        try:
            with dconn:
                dconn.execute(_GAP_DDL)
                dconn.execute(
                    "INSERT INTO journal_coverage_gap VALUES (?,?,?,?,?)",
                    (n["other"], "2026-01-01T00:00:00+00:00",
                     "2026-01-02T00:00:00+00:00", "invalidated",
                     "2026-01-03T00:00:00+00:00"))
            reports = []
            for _ in range(2):
                rep = carry_journal_history(src, dconn, dst, since_event_id=0,
                                            mark_source=False)
                reports.append({"ok": rep.ok, "error": rep.error})
                d.setdefault("a_gaps_after", []).append(
                    [list(g) for g in _gap_rows(dconn)])
            d["a_reports"] = reports
            d["a_refusals"] = _refusals(dconn, n["cur"])
        finally:
            dconn.close()

        # B: partial carry (the events since the first one, as a "Save As"
        # of a session opened after it) into another project journal.
        dst_b = os.path.join(tmp, "projet_b", "recoverland_audit.sqlite")
        os.makedirs(os.path.dirname(dst_b), exist_ok=True)
        bconn = _new_journal(dst_b)
        try:
            rep = carry_journal_history(src, bconn, dst_b,
                                        since_event_id=info["first_event_id"],
                                        mark_source=False)
            d["b_report"] = {"ok": rep.ok, "error": rep.error,
                             "inserted": rep.events_inserted}
            d["b_gaps"] = [list(g) for g in _gap_rows(bconn)]
            d["b_refusals"] = _refusals(bconn, n["cur"])
        finally:
            bconn.close()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"tx_journal_carry_purge_gaps run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3500]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    n = d.get("names") or {}
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-400:]}" if d.get("crash") else ""
    src_gaps = [tuple(g) for g in d.get("source_gaps") or []]
    after = [[tuple(g) for g in rows] for rows in d.get("a_gaps_after") or []]
    reports = d.get("a_reports") or []
    ref_a = d.get("a_refusals") or {}
    ref_b = d.get("b_refusals") or {}
    b_gaps = [tuple(g) for g in d.get("b_gaps") or []]
    other = (n.get("other"), "2026-01-01T00:00:00+00:00",
             "2026-01-02T00:00:00+00:00", "invalidated")
    out = []
    out.append((
        "setup_source_holds_two_purged_intervals",
        len(src_gaps) == 2 and not d.get("crash"),
        f"intervalles du journal de session={src_gaps}{crash}",
    ))
    out.append((
        "a_carry_succeeds_twice",
        len(reports) == 2 and all(r.get("ok") for r in reports),
        f"rattachements={reports}{crash}",
    ))
    out.append((
        "a_purged_intervals_follow_the_history",
        len(after) == 2 and all(g in after[0] for g in src_gaps),
        f"intervalles du journal du projet apres le rattachement="
        f"{after[0] if after else None} ; attendu en plus : {src_gaps}. "
        f"Sans eux le journal du projet ne sait pas que l'historique de "
        f"ces dates a ete purge{crash}",
    ))
    out.append((
        "a_second_carry_adds_no_duplicate",
        len(after) == 2 and after[0] == after[1] and len(after[1]) == len(set(after[1])),
        f"apres le 1er rattachement={after[0] if after else None} ; apres le "
        f"2e={after[1] if len(after) > 1 else None} : un rattachement rejoue "
        f"(obligation durable apres un plantage) ne doit rien dupliquer",
    ))
    out.append((
        "a_target_own_interval_untouched",
        len(after) == 2 and other in after[1],
        f"intervalle propre au journal du projet {other} present apres les "
        f"rattachements={bool(after) and other in after[-1]}",
    ))
    out.append((
        "a_rewind_inside_a_purged_interval_is_refused",
        bool(ref_a.get("in_cur")) and "purged between" in str(ref_a.get("in_cur"))
        and bool(ref_a.get("in_old")) and "purged between" in str(ref_a.get("in_old")),
        f"preflight du rewind au {_IN_CUR} : {ref_a.get('in_cur')!r} ; au "
        f"{_IN_OLD} (intervalle tenu par l'ancienne empreinte) : "
        f"{ref_a.get('in_old')!r}. Accepte, le rewind rejoue ce qui a survecu "
        f"et annonce un etat qui n'a jamais existe{crash}",
    ))
    out.append((
        "a_date_outside_the_intervals_stays_reachable",
        ref_a.get("outside") is None and "outside" in ref_a,
        f"preflight du rewind au {_OUTSIDE} : {ref_a.get('outside')!r} "
        f"(attendu : accepte, rien n'a ete purge a cette date)",
    ))
    out.append((
        "a_review_sees_the_intervals",
        ref_a.get("review_coverage_gap") is True
        and isinstance(ref_a.get("review_gaps"), list) and len(ref_a["review_gaps"]) >= 2,
        f"journal_coverage (lu par Review et le rapport) : gaps="
        f"{ref_a.get('review_gaps')} coverage_gap={ref_a.get('review_coverage_gap')}",
    ))
    out.append((
        "b_partial_carry_keeps_every_interval_of_the_layer",
        (d.get("b_report") or {}).get("ok") is True
        and all(g in b_gaps for g in src_gaps)
        and bool(ref_b.get("in_cur")) and bool(ref_b.get("in_old")),
        f"rattachement partiel={d.get('b_report')} ; intervalles de la cible="
        f"{b_gaps} (attendu : {src_gaps}) ; preflight au {_IN_CUR}="
        f"{ref_b.get('in_cur')!r}, au {_IN_OLD}={ref_b.get('in_old')!r}. "
        f"L'intervalle tenu par l'ancienne empreinte de la couche doit "
        f"suivre aussi{crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_journal_carry_purge_gaps.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
