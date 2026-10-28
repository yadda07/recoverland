# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""maint_purge_interval_coverage - RecoverLand validation runtime.

Invariant: I-8 (ce que le journal ne peut plus reconstruire est refuse en
le nommant) SANS refuser ce qu'il reconstruit encore ; et "jamais de
limite absente" sur un journal purge par une ancienne version.

Le mecanisme
------------
A3. Chaque purge logique (paires INSERT/DELETE, traces invalidees)
    enregistrait UN horizon par source de donnees, et le preflight
    refusait toute date inferieure ou egale. Or une paire (t1, t2) purgee
    ne rendait impossible que l'intervalle [t1, t2] (l'entite existait
    entre les deux) ; une trace en vigueur de c a i (annulee par un Undo)
    que l'intervalle [c, i]. Avant et apres, la paire s'annule et la
    trace n'a jamais compte. Un clic de maintenance (cases cochees par
    defaut) desactivait durablement presque tout l'historique de rewind
    de la couche. Les traces orphelines, elles, disent que TOUT ce qui
    precede exige un evenement deja purge : horizon plancher, conserve.

C.  Un journal purge par une version anterieure a v7 (horizon garde en
    memoire de session seulement) n'a aucune ligne journal_coverage pour
    la couche. Le plancher durable retombait sur "pas de limite" : un
    rewind vers une date anterieure au plus ancien evenement survivant
    rejouait ce qui reste et annoncait un succes. Il doit retomber sur la
    garde historique (le plus ancien evenement survivant de la couche).
    Un journal NE sous v7 sans purge garde son comportement : une date
    avant la premiere capture est annoncee, pas refusee.

Ce que le scenario execute
--------------------------
Un vrai journal (schema du plugin), la vraie purge
(`purge_old_events_with_options`, options de la maintenance), puis les
questions du rewind : `retention.coverage_refusal` et
`restore_planner.preflight_check(plan, conn=...)` sur un plan construit
par la vraie fenetre et le vrai dedup. Puis le meme journal relu sans la
memoire de session (redemarrage de QGIS). Enfin un second journal vieilli
(tampons schema_version anterieurs a v7, tampon de couverture "migrated"
depuis le schema 6) sans ligne de couverture.

Echec concret pour l'utilisateur (avant correctif)
--------------------------------------------------
A3 : apres une purge de maintenance, un rewind de la couche a hier matin
(avant la paire purgee) est refuse "History purged", pour toujours. C :
sur un journal migre, un rewind avant le plus ancien evenement restant
est accepte et annonce un etat qui n'a jamais existe.

Verdict attendu avant correctif : FAIL.

Hypothese revisee (v7) : les deux journaux etaient crees aujourd'hui.
Leur tampon de couverture dit "created" depuis aujourd'hui, et les
lecteurs du plancher le lisent avant l'horloge de schema_version : le
journal "d'avant v7" doit aussi porter ce que sa migration enregistre
("migrated", schema 6), et le journal "ne sous v7" doit etre ne avant
les lignes qu'il a capturees (une ligne plus ancienne que le journal
vient d'ailleurs et garde son plancher). Les verifications sont
inchangees.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "maint_purge_interval_coverage"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _row(ds, op, fid, ts, session=None, restored_from=None,
         invalidated_at=None, attrs=None):
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": ds,
        "layer_id_snapshot": ds, "layer_name_snapshot": ds,
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": op,
        "attributes_json": json.dumps(attrs or {"all_attributes": {"nom": f"n{fid}"}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": None,
        "field_schema_json": None, "user_name": "u", "session_id": session,
        "created_at": ts, "restored_from_event_id": restored_from,
        "entity_fingerprint": f"fid:{fid}", "event_schema_version": 7,
        "new_geometry_wkb": None, "invalidated_at": invalidated_at,
    }


def _insert(conn, row) -> int:
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    return conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({marks})",
                        tuple(row.values())).lastrowid


def _born_at(conn, instant: str) -> None:
    """Date the creation of a fresh v7 journal back to *instant*.

    Both records of the birth: every schema_version stamp (the inference)
    and coverage_recorded_since (the coverage stamp, "created" kept). A
    journal with no backend_settings (older code) keeps the first only.
    """
    conn.execute("UPDATE schema_version SET applied_at = ?", (instant,))
    try:
        conn.execute("UPDATE backend_settings SET setting_value = ?, updated_at = ? "
                     "WHERE setting_key = 'coverage_recorded_since'",
                     (instant, instant))
    except sqlite3.Error:
        pass


def _record_migrated_from_v6(conn) -> None:
    """What the opener of a schema 6 journal records after its migration.

    journal_writes.record_coverage_origin: coverage_origin "migrated" and
    coverage_origin_schema "6", beside the coverage_recorded_since the
    schema transaction wrote. A journal with no backend_settings (older
    code) has no stamp to correct.
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


def _upd(ds, fid, ts):
    return _row(ds, "UPDATE", fid, ts, "s-u",
                attrs={"changed_only": {"nom": {"old": "a", "new": "b"}}})


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog
    paths = {}
    for name in ("main", "legacy"):
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        paths[name] = path
    conn = sqlite3.connect(paths["main"])
    initialize_schema(conn)
    # Hypothesis revised (v7): this journal stands for one BORN under v7
    # that captured every event below (the N layer "captured from
    # 2026-03-06 only"). Created today, it held rows dated months before
    # its own birth, which no journal can have captured: rows brought in
    # (carried or imported), below which Review does not vouch for
    # anything (snapshot_rebuild_worker._layer_floor). Its birth is set
    # before those rows, in both records of it
    # (schema_version stamps and the coverage stamp): the journal the
    # checks were written for.
    _born_at(conn, "2026-02-01T00:00:00+00:00")
    # A pair of layer P (09:00-09:30), the layer edited before and after.
    _insert(conn, _upd("ic_ds_p", 1, "2026-03-03T07:00:00+00:00"))
    _insert(conn, _row("ic_ds_p", "INSERT", 9, "2026-03-03T09:00:00+00:00", "s-p"))
    _insert(conn, _row("ic_ds_p", "DELETE", 9, "2026-03-03T09:30:00+00:00", "s-p"))
    _insert(conn, _upd("ic_ds_p", 1, "2026-03-03T12:00:00+00:00"))
    # An undone compensation of layer I: in effect from 10:00 to 11:00.
    i_del = _insert(conn, _row("ic_ds_i", "DELETE", 6, "2026-03-05T09:00:00+00:00", "s-i"))
    _insert(conn, _row("ic_ds_i", "INSERT", 6, "2026-03-05T10:00:00+00:00",
                       restored_from=i_del,
                       invalidated_at="2026-03-05T11:00:00+00:00"))
    _insert(conn, _upd("ic_ds_i", 2, "2026-03-05T12:00:00+00:00"))
    # A real orphan of layer O at 10:00.
    _insert(conn, _row("ic_ds_o", "INSERT", 4, "2026-03-04T10:00:00+00:00",
                       restored_from=999999))
    _insert(conn, _upd("ic_ds_o", 3, "2026-03-04T12:00:00+00:00"))
    # Layer N: never purged, captured from 2026-03-06 only.
    _insert(conn, _upd("ic_ds_n", 5, "2026-03-06T09:00:00+00:00"))
    conn.commit()
    conn.close()

    # A journal that lived before v7 (older schema stamps), purged by that
    # release: no coverage row, history of layer L starts on 2026-06-01.
    conn = sqlite3.connect(paths["legacy"])
    initialize_schema(conn)
    conn.execute("UPDATE schema_version SET applied_at = "
                 "'2025-01-01T00:00:00+00:00' WHERE version_number < 7")
    # Hypothesis revised (v7): created fresh, this file's coverage stamp
    # truthfully says "created" (journal_writes.coverage_origin), and the
    # floor readers consult it before the schema_version clock stamps:
    # aging those alone no longer makes it a journal that lived before v7.
    # The migration of such a journal records "migrated" from schema 6
    # (record_coverage_origin); written here, it is the journal the checks
    # below were written for, now described by both records.
    _record_migrated_from_v6(conn)
    _insert(conn, _upd("ic_ds_l", 1, "2026-06-01T09:00:00+00:00"))
    _insert(conn, _upd("ic_ds_l", 1, "2026-06-02T09:00:00+00:00"))
    _insert(conn, _upd("ic_ds_k", 1, "2026-06-01T09:00:00+00:00"))
    conn.commit()
    conn.close()
    ctx.data["paths"] = paths
    flog(f"maint_purge_interval_coverage setup: trace_id={ctx.trace_id} paths={paths}")


def _refusal(conn, ds, cutoff):
    from recoverland.core import retention
    if hasattr(retention, "coverage_refusal"):
        return retention.coverage_refusal(conn, ds, cutoff)
    horizon = retention.purged_horizon(ds)  # older API: session memory only
    return f"purged up to {horizon}" if horizon and cutoff <= horizon else None


def _forget_session():
    """What a QGIS restart does to the purge memory of the process."""
    from recoverland.core import retention
    for name in ("_purge_horizon", "_purge_gaps"):
        memory = getattr(retention, name, None)
        if isinstance(memory, dict):
            memory.clear()


def _preflight(conn, ds, cutoff_value):
    """The rewind's own question: window, dedup, plan, preflight(conn)."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events
    from recoverland.core.restore_planner import plan_temporal_restore, preflight_check
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_value, inclusive=True)
    events = collapse_rewind_events(fetch_events_after_cutoff(conn, ds, cutoff))
    plan = plan_temporal_restore(events, ds, ds, cutoff)
    try:
        report = preflight_check(plan, conn=conn)
    except TypeError:
        report = preflight_check(plan)
    coverage = [r for r in report.blocking_reasons if "Retention coverage" in r]
    return {"verdict": str(report.verdict), "actions": len(plan.actions),
            "coverage": coverage}


_CHECKS = {
    "p_before_pair": ("ic_ds_p", "2026-03-03T08:00:00+00:00", False),
    "p_pair_start": ("ic_ds_p", "2026-03-03T09:00:00+00:00", True),
    "p_inside_pair": ("ic_ds_p", "2026-03-03T09:15:00+00:00", True),
    "p_pair_end": ("ic_ds_p", "2026-03-03T09:30:00+00:00", True),
    "p_after_pair": ("ic_ds_p", "2026-03-03T10:00:00+00:00", False),
    "i_before_trace": ("ic_ds_i", "2026-03-05T09:30:00+00:00", False),
    "i_while_in_effect": ("ic_ds_i", "2026-03-05T10:30:00+00:00", True),
    "i_after_undo": ("ic_ds_i", "2026-03-05T11:30:00+00:00", False),
    "o_before_orphan": ("ic_ds_o", "2026-03-04T09:00:00+00:00", True),
    "o_after_orphan": ("ic_ds_o", "2026-03-04T10:30:00+00:00", False),
    "n_before_first_capture": ("ic_ds_n", "2026-03-01T00:00:00+00:00", False),
}


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import apply_pragmas
    from recoverland.core.retention import (
        PurgeOptions, RetentionPolicy, purge_old_events_with_options,
        purged_horizon,
    )
    paths = ctx.data["paths"]
    _forget_session()
    conn = sqlite3.connect(paths["main"])
    apply_pragmas(conn)
    result = purge_old_events_with_options(
        conn, RetentionPolicy(retention_days=36500, max_events=1_000_000),
        PurgeOptions(retention=False, invalidated=True,
                     insert_delete_pairs=True, orphan_traces=True))
    ctx.data["result"] = result._asdict()
    ctx.data["session"] = {k: _refusal(conn, ds, c) for k, (ds, c, _x) in _CHECKS.items()}
    ctx.data["preflight"] = {
        "before_pair": _preflight(conn, "ic_ds_p", "2026-03-03T08:00:00+00:00"),
        "inside_pair": _preflight(conn, "ic_ds_p", "2026-03-03T09:15:00+00:00"),
    }
    try:
        ctx.data["review_floor_p"] = purged_horizon("ic_ds_p", conn)
    except TypeError:  # older API: session memory only
        ctx.data["review_floor_p"] = purged_horizon("ic_ds_p")
    conn.close()
    _forget_session()
    conn = sqlite3.connect(paths["main"])
    ctx.data["restart"] = {k: _refusal(conn, ds, c) for k, (ds, c, _x) in _CHECKS.items()}
    ctx.data["preflight_restart"] = _preflight(
        conn, "ic_ds_p", "2026-03-03T08:00:00+00:00")
    conn.close()

    conn = sqlite3.connect(paths["legacy"])
    ctx.data["legacy"] = {
        "before_oldest": _refusal(conn, "ic_ds_l", "2026-05-01T00:00:00+00:00"),
        "after_oldest": _refusal(conn, "ic_ds_l", "2026-06-01T12:00:00+00:00"),
        "preflight_before_oldest": _preflight(conn, "ic_ds_l", "2026-05-01T00:00:00+00:00"),
    }
    # The same journal once a v7 purge recorded the layer K: the record
    # answers, not the guess.
    conn.execute("CREATE TABLE IF NOT EXISTS journal_coverage ("
                 "datasource_fingerprint TEXT PRIMARY KEY, purged_through TEXT "
                 "NOT NULL, purged_events INTEGER NOT NULL DEFAULT 0, "
                 "updated_at TEXT NOT NULL)")
    conn.execute("INSERT OR REPLACE INTO journal_coverage VALUES "
                 "('ic_ds_k', '2026-05-15T00:00:00+00:00', 3, "
                 "'2026-05-16T00:00:00+00:00')")
    conn.commit()
    ctx.data["legacy"]["recorded_after_row"] = _refusal(
        conn, "ic_ds_k", "2026-05-20T00:00:00+00:00")
    ctx.data["legacy"]["recorded_before_row"] = _refusal(
        conn, "ic_ds_k", "2026-05-10T00:00:00+00:00")
    conn.close()
    flog(f"maint_purge_interval_coverage run: trace_id={ctx.trace_id} "
         f"result={ctx.data['result']} session={ctx.data['session']} "
         f"restart={ctx.data['restart']} preflight={ctx.data['preflight']} "
         f"legacy={ctx.data['legacy']}")


def teardown(ctx):
    for path in (ctx.data.get("paths") or {}).values():
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    for phase in ("session", "restart"):
        got = ctx.data.get(phase) or {}
        wrong = {k: got.get(k) for k, (_ds, _c, refuse) in _CHECKS.items()
                 if bool(got.get(k)) != refuse}
        out.append((
            f"{phase}_refuses_exactly_what_was_lost",
            bool(got) and not wrong,
            f"ecarts (date: refus obtenu)={wrong} ; attendu : refus DANS la "
            f"paire purgee [09:00, 09:30] et pendant la compensation annulee "
            f"[10:00, 11:00], refus avant l'orpheline ; acceptation avant et "
            f"apres ces intervalles. Refuser hors intervalle desactive "
            f"l'historique de la couche ; accepter dedans rend un etat faux.",
        ))
    pre = ctx.data.get("preflight") or {}
    before, inside = pre.get("before_pair") or {}, pre.get("inside_pair") or {}
    out.append((
        "rewind_preflight_accepts_a_date_before_the_purged_pair",
        not before.get("coverage") and before.get("actions", 0) >= 1,
        f"preflight a 08:00 (avant la paire purgee)={before} : la paire "
        f"s'annule avant son INSERT, rien n'est perdu a cette date.",
    ))
    out.append((
        "rewind_preflight_refuses_inside_the_purged_pair_by_name",
        bool(inside.get("coverage")) and "09:30" in " ".join(inside["coverage"]),
        f"preflight a 09:15 (dans la paire purgee)={inside} : l'entite "
        f"existait, le journal ne l'a plus ; le refus doit nommer l'intervalle.",
    ))
    restart = ctx.data.get("preflight_restart") or {}
    out.append((
        "after_restart_the_date_before_the_pair_stays_reachable",
        not restart.get("coverage"),
        f"preflight apres redemarrage={restart}",
    ))
    floor = ctx.data.get("review_floor_p") or ""
    out.append((
        "review_single_date_floor_never_below_the_loss",
        floor >= "2026-03-03T09:30:00+00:00",
        f"purged_horizon(P)={floor!r} : un appelant qui ne tient qu'une date "
        f"(plancher de Review) doit refuser au moins tout l'intervalle perdu.",
    ))
    leg = ctx.data.get("legacy") or {}
    out.append((
        "legacy_journal_without_record_falls_back_to_the_oldest_event",
        bool(leg.get("before_oldest")) and not leg.get("after_oldest")
        and bool((leg.get("preflight_before_oldest") or {}).get("coverage")),
        f"{leg} : un journal purge par une version d'avant v7 n'a pas de "
        f"trace de la purge ; sans la garde historique, un rewind avant le "
        f"plus ancien evenement restant rejoue ce qui reste et annonce un "
        f"succes.",
    ))
    out.append((
        "a_recorded_purge_answers_instead_of_the_guess",
        not leg.get("recorded_after_row") and bool(leg.get("recorded_before_row")),
        f"apres={leg.get('recorded_after_row')!r} avant={leg.get('recorded_before_row')!r} "
        f": une ligne de couverture dit exactement ce qui a ete purge.",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"maint_purge_interval_coverage.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
