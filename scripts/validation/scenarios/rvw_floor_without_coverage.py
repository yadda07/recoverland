# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rvw_floor_without_coverage - RecoverLand validation runtime.

Invariant: I-RVW-ASOF (Review montre l'etat a la date T ou dit qu'il ne
peut pas ; il ne presente jamais l'etat a la ligne de purge comme l'etat a
une date anterieure).

Le mecanisme
------------
Sous la ligne d'une purge, les evenements ont ete supprimes : l'image avant
du premier evenement SURVIVANT decrit l'etat a la ligne de purge, pas l'etat
a une date anterieure. Le plancher de Review (widgets/snapshot_rebuild_worker
.query_snapshot_floor) ne lit plus que l'horizon de purge durable
(journal_coverage) et vaut "" (aucune limite) quand cette table n'a pas de
ligne pour la couche. Or avant v7 l'horizon de purge ne vivait qu'en memoire
de session (retention.purged_horizon : "Session memory, not journal memory
... The durable guard is the oldest surviving event"), et v7 ne le
reconstitue pas : tout journal purge par une version precedente n'a aucun
horizon durable. Le Rewind, lui, refuse la meme date
(check_journal_coverage : "History purged before this date").

L'echec concret pour l'utilisateur
----------------------------------
La retention d'une version precedente a purge la premiere edition d'une
parcelle (P1 : Alpha -> Alpha1). Review a une date anterieure : P1 est
affichee "Alpha1" (l'etat a la ligne de purge) comme etat de cette date, qui
etait "Alpha", sans aucun avertissement ; l'export fait de meme.

Ce que le scenario pilote : le vrai plugin, un vrai GeoPackage, de vraies
editions capturees, un journal dont l'historique de schema commence avant
v7 (celui d'un utilisateur de la version livree, mis a jour : tampon de
couverture "migrated" depuis le schema 6, comme l'ecrit l'ouvreur), puis la purge
telle que l'ecrivait cette version (lignes supprimees, aucun horizon
durable), et le vrai dialogue en mode Review a une date anterieure a la
purge. En garde : avec un horizon durable, le plancher est cet horizon
(arrondi a la seconde superieure) ; un journal CREE en v7, ou toute purge
laisse son horizon, n'a pas de plancher sans horizon (rien n'y a ete
purge) ; le meme journal avec un historique d'avant v7 (tampon "migrated")
a le plancher du plus ancien evenement survivant.

Verdict attendu avant correctif : FAIL. Apres : PASS.

Hypothese revisee (v7) : le journal "d'avant v7" etait simule en vieillissant
les seuls horodatages de schema_version d'un journal cree par le plugin.
Depuis le tampon de couverture, ce journal dit lui-meme qu'il est "created"
et les lecteurs du plancher le lisent avant l'horloge : la simulation doit
aussi ecrire ce que la migration d'un journal au schema 6 enregistre
("migrated", schema 6). Les verifications sont inchangees.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_floor_without_coverage"
INVARIANT = "I-RVW-ASOF"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS = [
    ("P1", "Alpha", 10.0, 1.0, 1.0),
    ("P2", "Beta", 20.0, 2.0, 2.0),
]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def _second(iso: str) -> str:
    return (iso or "")[:19]


def _record_migrated_from_v6(conn) -> None:
    """What the opener of a schema 6 journal records after its migration.

    journal_writes.record_coverage_origin: coverage_origin "migrated" and
    coverage_origin_schema "6", beside the coverage_recorded_since the
    schema transaction wrote. In the caller's transaction. A journal with
    no backend_settings (older code) has no stamp to correct.
    """
    try:
        for key, value in (("coverage_origin", "migrated"),
                           ("coverage_origin_schema", "6")):
            conn.execute(
                "INSERT OR REPLACE INTO backend_settings (setting_key, "
                "setting_value, updated_at) SELECT ?, ?, COALESCE((SELECT "
                "setting_value FROM backend_settings WHERE setting_key = "
                "'coverage_recorded_since'), ?)",
                (key, value, "2026-01-01T00:00:00+00:00"))
    except sqlite3.Error:
        pass


class _Journal:
    def __init__(self, path):
        self._path = path

    def create_read_connection(self):
        return sqlite3.connect(self._path)


def _coverage_guard(tmpdir: str) -> dict:
    """query_snapshot_floor on a journal created at v7, then on the same
    journal with a schema history from before v7.

    ds_guard: a durable horizon. ds_nohz: no horizon, rows journaled after
    the journal was created. ds_old: no horizon, a row OLDER than the
    journal (brought in from elsewhere: its history is unknown), the shape
    of the adversarial review's probe B.
    """
    from recoverland.core.journal_writes import ensure_v7_schema
    from recoverland.core.sqlite_schema import AUDIT_EVENT_COLUMNS, initialize_schema
    from recoverland.widgets.snapshot_rebuild_worker import query_snapshot_floor
    path = os.path.join(tmpdir, "guard.sqlite")
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    ensure_v7_schema(conn)
    row = dict.fromkeys(AUDIT_EVENT_COLUMNS)
    row.update({"event_id": 1, "project_fingerprint": "p",
                "datasource_fingerprint": "ds_guard", "layer_id_snapshot": "l",
                "layer_name_snapshot": "l", "provider_type": "ogr",
                "feature_identity_json": '{"fid": 1}', "operation_type": "UPDATE",
                "attributes_json": '{"changed_only": {}}', "geometry_type": "Point",
                "user_name": "u", "created_at": "2026-03-01T10:00:00+00:00",
                "entity_fingerprint": "fid:1", "event_schema_version": 7})
    cols = ", ".join(AUDIT_EVENT_COLUMNS)
    ph = ",".join("?" for _ in AUDIT_EVENT_COLUMNS)
    from datetime import datetime, timedelta, timezone
    after_creation = (datetime.now(timezone.utc) + timedelta(minutes=5)).replace(
        microsecond=0)
    out = {"nohz_event": after_creation.isoformat(),
           "nohz_floor_expected": (after_creation - timedelta(seconds=1)).strftime(
               "%Y-%m-%dT%H:%M:%S")}
    with conn:
        conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({ph})",  # nosec B608
                     tuple(row[c] for c in AUDIT_EVENT_COLUMNS))
        conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({ph})",  # nosec B608
                     tuple(dict(row, event_id=2, datasource_fingerprint="ds_nohz",
                                created_at=out["nohz_event"])[c]
                           for c in AUDIT_EVENT_COLUMNS))
        conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({ph})",  # nosec B608
                     tuple(dict(row, event_id=3, datasource_fingerprint="ds_old",
                                created_at="2026-03-01T00:00:00+00:00")[c]
                           for c in AUDIT_EVENT_COLUMNS))
        conn.execute("INSERT INTO journal_coverage (datasource_fingerprint, "
                     "purged_through, purged_events, updated_at) VALUES (?,?,?,?)",
                     ("ds_guard", "2026-02-10T08:30:00.250000+00:00", 3,
                      "2026-02-11T00:00:00+00:00"))
    conn.close()
    out.update({
        "with_horizon": query_snapshot_floor(_Journal(path), [{"fingerprint": "ds_guard"}]),
        "no_event": query_snapshot_floor(_Journal(path), [{"fingerprint": "ds_empty"}]),
        # Created at v7: every purge would have left a horizon.
        "fresh_no_horizon": query_snapshot_floor(_Journal(path), [{"fingerprint": "ds_nohz"}]),
        "row_older_than_journal": query_snapshot_floor(
            _Journal(path), [{"fingerprint": "ds_old"}]),
    })
    conn = sqlite3.connect(path)
    with conn:
        conn.execute("UPDATE schema_version SET applied_at = ? WHERE version_number < 7",
                     ("2025-01-15T09:00:00+00:00",))
        # Hypothesis revised (v7): same as in run(). This journal was created
        # fresh above and its coverage stamp says "created", which Review
        # now reads before the clock stamps; the journal "created before v7"
        # this probe stands for is the one whose migration recorded
        # "migrated" from schema 6. Same journal, same expected floor.
        _record_migrated_from_v6(conn)
    conn.close()
    out["legacy_no_horizon"] = query_snapshot_floor(
        _Journal(path), [{"fingerprint": "ds_nohz"}])
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_floor_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", _ROWS)
    flog(f"rvw_floor_without_coverage setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)
        # The date the user will ask for: P1 is still "Alpha".
        asked = h.utc_now_iso()
        d["asked"] = asked
        time.sleep(1.3)
        d["commits"] = [h.user_commit(layer, lambda lyr: h.rename(lyr, "P1", "Alpha1"))]
        h.wait_rows(jpath, rows0 + 1)
        # Wide gap: the floor is one second before the oldest surviving
        # event, and this edit must stay on the other side of it.
        time.sleep(2.5)
        d["commits"].append(h.user_commit(layer, lambda lyr: h.rename(lyr, "P1", "Alpha2")))
        time.sleep(1.2)
        d["commits"].append(h.user_commit(layer, lambda lyr: h.rename(lyr, "P2", "Beta2")))
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 3) - rows0

        # The journal of a user of the shipped version (schema 6), upgraded
        # to v7: its schema history starts long before v7. Then the purge of
        # that older version: rows below a date deleted, the horizon kept in
        # that session's memory only (nothing in the file).
        conn = sqlite3.connect(jpath)
        try:
            with conn:
                conn.execute("UPDATE schema_version SET applied_at = ? "
                             "WHERE version_number < 7",
                             ("2025-01-15T09:00:00+00:00",))
                # Hypothesis revised (v7): the file is created fresh by the
                # plugin, and its coverage stamp truthfully says "created"
                # (journal_writes.coverage_origin), which the floor readers
                # now consult before the schema_version clock stamps. Aging
                # those stamps alone no longer makes it the journal of a
                # user of the shipped version; the stamp the migration of
                # such a journal records does (record_coverage_origin:
                # "migrated", schema seen 6). The journal under test is the
                # same upgraded schema 6 journal, now described by both
                # records instead of one; every check below is unchanged.
                _record_migrated_from_v6(conn)
            dates = [r[0] for r in conn.execute(
                "SELECT created_at FROM audit_event ORDER BY created_at")]
            d["purged_event_at"] = dates[0] if dates else None
            d["oldest_surviving"] = dates[1] if len(dates) > 1 else None
            with conn:
                d["purged_rows"] = conn.execute(
                    "DELETE FROM audit_event WHERE created_at = ?",
                    (d["purged_event_at"],)).rowcount
            try:
                d["coverage_rows"] = conn.execute(
                    "SELECT COUNT(*) FROM journal_coverage").fetchone()[0]
            except sqlite3.Error:
                d["coverage_rows"] = 0
        finally:
            conn.close()

        h.set_view(iface, 0.0, 0.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layer])
        # From the start of the session: the bar opens on a date of its own,
        # which the floor may already bring forward.
        n_msgs = len(iface.bar.messages)
        d["review_on"] = h.review_on(dlg, 1)
        # The user moves the date bar to the date, which then asks for it.
        if dlg._review_date_bar is not None:
            dlg._review_date_bar.set_value_iso(asked)
        d["review_date"] = h.review_date(dlg, asked)
        d["answered_date"] = dlg._review_snap_raw_iso
        bar = dlg._review_date_bar
        d["bar_date"] = bar.current_date_iso() if bar is not None else None
        d["overlay"] = h.overlay_rows(dlg)
        d["messages"] = [f"{t} | {x}" for t, x, _l, _dur in iface.bar.messages[n_msgs:]]
        h.review_off(dlg)
        d["guard"] = _coverage_guard(d["tmpdir"])
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_floor_without_coverage run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:3000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    answered = d.get("answered_date") or ""
    oldest = _second(d.get("oldest_surviving") or "")
    purged = _second(d.get("purged_event_at") or "")
    p1 = (d.get("overlay") or {}).get("P1") or {}
    out = []
    out.append((
        "setup_legacy_purge_without_durable_horizon",
        d.get("commits") == [True, True, True] and d.get("purged_rows") == 1
        and d.get("coverage_rows") == 0 and d.get("review_on") is True,
        f"commits={d.get('commits')} lignes purgees={d.get('purged_rows')} "
        f"lignes journal_coverage={d.get('coverage_rows')} review_on="
        f"{d.get('review_on')}{crash}",
    ))
    out.append((
        "review_does_not_answer_a_date_before_the_purge",
        bool(answered) and answered != d.get("asked") and answered > purged,
        f"date demandee={d.get('asked')} date reconstruite={answered!r} "
        f"evenement purge a {purged} : l'etat a la ligne de purge ne doit pas "
        f"etre presente comme l'etat de la date demandee{crash}",
    ))
    out.append((
        "clamp_is_strictly_before_the_oldest_surviving_event",
        bool(answered) and answered < oldest and p1.get("nom") == "Alpha1",
        f"date reconstruite={answered!r} plus ancien evenement survivant="
        f"{oldest} P1={p1 or 'ABSENTE'} (attendu nom=Alpha1 : la premiere "
        f"edition survivante ne doit pas apparaitre deja faite){crash}",
    ))
    out.append((
        "bar_shows_the_date_really_answered",
        d.get("bar_date") == answered,
        f"barre={d.get('bar_date')!r} date reconstruite={answered!r} : "
        f"l'utilisateur doit lire la date de l'etat affiche",
    ))
    out.append((
        "user_is_told_the_history_was_purged",
        any("purg" in m for m in (d.get("messages") or [])),
        f"messages={d.get('messages')} : ramener la date sans le dire cache "
        f"que l'historique anterieur n'existe plus",
    ))
    g = d.get("guard") or {}
    out.append((
        "durable_horizon_floor_rounded_up_to_the_second",
        g.get("with_horizon") == "2026-02-10T08:30:01" and g.get("no_event") == "",
        f"plancher avec horizon durable={g.get('with_horizon')!r} (attendu "
        f"'2026-02-10T08:30:01', l'horizon arrondi a la seconde superieure) ; "
        f"sans evenement={g.get('no_event')!r} (attendu ''){crash}",
    ))
    out.append((
        "legacy_journal_without_horizon_guarded_fresh_one_unbounded",
        bool(g.get("nohz_floor_expected"))
        and g.get("legacy_no_horizon") == g.get("nohz_floor_expected")
        and g.get("fresh_no_horizon") == ""
        and g.get("row_older_than_journal") == "2026-02-28T23:59:59",
        f"journal cree avant v7 sans horizon : plancher="
        f"{g.get('legacy_no_horizon')!r} (attendu {g.get('nohz_floor_expected')!r}, "
        f"la seconde avant le plus ancien evenement survivant) ; journal cree "
        f"en v7 (toute purge y laisse son horizon), lignes toutes posterieures "
        f"a sa creation : plancher={g.get('fresh_no_horizon')!r} (attendu '', "
        f"rien n'a ete purge) ; ligne plus ancienne que le journal (importee, "
        f"sonde B de la revue) : plancher={g.get('row_older_than_journal')!r} "
        f"(attendu '2026-02-28T23:59:59'){crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_floor_without_coverage.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
