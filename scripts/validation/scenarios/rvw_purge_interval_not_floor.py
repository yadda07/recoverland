"""rvw_purge_interval_not_floor - RecoverLand validation runtime.

Invariant: I-8 (ce que le journal ne peut plus reconstruire est refuse en
le nommant) SANS refuser ce qu'il reconstruit encore, dans Review comme
dans le Rewind.

Le mecanisme
------------
Une purge logique de maintenance (paire INSERT/DELETE, compensation annulee
par un Undo) ne retire l'historique que d'UN intervalle de la couche : une
paire (t1, t2) ne reconstruisait que les dates entre ses deux evenements.
Le Rewind le sait : `retention.coverage_refusal` refuse les dates DANS
l'intervalle et accepte celles d'avant et d'apres. Le plancher de Review
(`widgets/snapshot_rebuild_worker.query_snapshot_floor`) lisait encore
`retention.purged_horizon`, date unique qui inclut la FIN de chaque
intervalle ("refuses more than was lost") : toute date de la couche
anterieure a la fin de la paire purgee etait ramenee a cette fin.

L'echec concret pour l'utilisateur
----------------------------------
Il renomme la parcelle P1 (Alpha -> Alpha0), cree puis supprime une
parcelle P9, renomme P1 encore (Alpha1), puis lance la maintenance
(purge des paires INSERT/DELETE, cochee par defaut). Review a une date
d'avant la creation de P9 : la date est ramenee apres la suppression de
P9, P1 est affichee "Alpha0" au lieu de "Alpha", et un message pretend que
l'historique anterieur a ete purge ; le Rewind, lui, accepte cette date.
Review a une date ou P9 existait : l'etat d'une autre date est dessine
sous le libelle "date ramenee", au lieu d'un refus qui nomme l'intervalle.

Ce que le scenario pilote : le vrai plugin, un vrai GeoPackage, de vraies
editions capturees, la vraie purge de maintenance
(`purge_old_events_with_options`), puis le vrai dialogue en mode Review
aux trois dates (avant, dans, apres l'intervalle purge), et la question du
Rewind (`coverage_refusal`) aux memes dates, pour comparaison.

Verdict attendu avant correctif : FAIL. Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rvw_purge_interval_not_floor"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_ROWS = [
    ("P1", "Alpha", 10.0, 1.0, 1.0),
    ("P2", "Beta", 20.0, 2.0, 2.0),
]


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_rvw_gap_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = _h().make_gpkg(tmpdir, "cadastre", _ROWS)
    flog(f"rvw_purge_interval_not_floor setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _purge_logical_garbage(jpath: str) -> dict:
    """The maintenance purge with the options checked by default."""
    from recoverland.core.retention import (
        PurgeOptions, RetentionPolicy, purge_old_events_with_options,
    )
    from recoverland.core.sqlite_schema import apply_pragmas
    conn = sqlite3.connect(jpath)
    try:
        apply_pragmas(conn)
        result = purge_old_events_with_options(
            conn, RetentionPolicy(retention_days=36500, max_events=1_000_000),
            PurgeOptions(retention=False, invalidated=True,
                         insert_delete_pairs=True, orphan_traces=True))
        conn.commit()
    finally:
        conn.close()
    return result._asdict()


def _review_at(dlg, iface, asked: str) -> dict:
    h = _h()
    n_msgs = len(iface.bar.messages)
    bar = dlg._review_date_bar
    if bar is not None:
        bar.set_value_iso(asked)
    idle = h.review_date(dlg, asked)
    bar = dlg._review_date_bar
    return {
        "idle": idle,
        "answered": dlg._review_snap_raw_iso,
        "cached": dlg._review_snap_raw_result is not None,
        "bar_date": bar.current_date_iso() if bar is not None else None,
        "bar_state": bar.status_state() if bar is not None else None,
        "bar_tip": bar._lbl_status.toolTip() if bar is not None else "",
        "overlay": h.overlay_rows(dlg),
        "messages": [f"{t} | {x}" for t, x, _l, _d in iface.bar.messages[n_msgs:]],
    }


def run(ctx):
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    from recoverland.core.retention import coverage_refusal
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)
        commits = []
        d["asked_before"] = h.utc_now_iso()          # P1 Alpha, no P9
        time.sleep(1.3)
        commits.append(h.user_commit(layer, lambda lyr: h.rename(lyr, "P1", "Alpha0")))
        time.sleep(1.3)
        # One edit session, two saves (the user clicks "Save layer edits"
        # and keeps editing): the pair the maintenance purge annuls.
        layer.startEditing()
        h.add(layer, "P9", "Neuf", 9.0, 5.0, 5.0)
        commits.append(bool(layer.commitChanges(False)))
        time.sleep(1.3)
        d["asked_inside"] = h.utc_now_iso()          # P9 exists
        time.sleep(1.3)
        h.delete(layer, "P9")
        commits.append(bool(layer.commitChanges()))
        time.sleep(1.3)
        d["asked_after"] = h.utc_now_iso()           # P9 gone, P1 Alpha0
        time.sleep(1.3)
        commits.append(h.user_commit(layer, lambda lyr: h.rename(lyr, "P1", "Alpha1")))
        d["commits"] = commits
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 4) - rows0
        d["purge"] = _purge_logical_garbage(jpath)
        conn = sqlite3.connect(jpath)
        try:
            d["rows_left"] = [r[0] for r in conn.execute(
                "SELECT operation_type FROM audit_event ORDER BY created_at")]
            fp = compute_datasource_fingerprint(layer)
            d["rewind_refusal"] = {
                k: coverage_refusal(conn, fp, d[k] + "+00:00")
                for k in ("asked_before", "asked_inside", "asked_after")}
        finally:
            conn.close()

        h.set_view(iface, 0.0, 0.0, 30.0, 30.0)
        dlg = h.open_dialog(plugin, [layer])
        d["review_on"] = h.review_on(dlg, 1)
        d["floor"] = getattr(dlg, "_review_floor_iso", None)
        for key in ("asked_before", "asked_inside", "asked_after"):
            d[key.replace("asked", "at")] = _review_at(dlg, iface, d[key])
        h.review_off(dlg)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        h.shutdown(plugin)
    flog(f"rvw_purge_interval_not_floor run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-500:]}" if d.get("crash") else ""
    out = []
    purge = d.get("purge") or {}
    refusal = d.get("rewind_refusal") or {}
    out.append((
        "setup_pair_purged_rewind_refuses_only_inside",
        d.get("commits") == [True] * 4 and purge.get("pair_deleted") == 2
        and d.get("review_on") is True
        and not refusal.get("asked_before") and bool(refusal.get("asked_inside"))
        and not refusal.get("asked_after"),
        f"commits={d.get('commits')} purge={purge} lignes restantes="
        f"{d.get('rows_left')} refus du Rewind={refusal} review_on="
        f"{d.get('review_on')}{crash}",
    ))
    before = d.get("at_before") or {}
    p1 = (before.get("overlay") or {}).get("P1") or {}
    out.append((
        "review_answers_a_date_before_the_purged_interval",
        before.get("answered") == d.get("asked_before")
        and before.get("bar_date") == d.get("asked_before")
        and p1.get("nom") == "Alpha" and "P9" not in (before.get("overlay") or {}),
        f"date demandee={d.get('asked_before')} reconstruite="
        f"{before.get('answered')!r} barre={before.get('bar_date')!r} "
        f"P1={p1 or 'ABSENTE'} (attendu nom=Alpha) plancher={d.get('floor')!r} : "
        f"le Rewind accepte cette date, Review la ramenait apres la paire "
        f"purgee et montrait l'etat d'une autre date{crash}",
    ))
    out.append((
        "no_false_purged_history_message_before_the_interval",
        not any("purg" in m for m in before.get("messages") or []),
        f"messages={before.get('messages')} : l'historique anterieur a la "
        f"paire n'a pas ete purge, le dire est faux",
    ))
    inside = d.get("at_inside") or {}
    named = [m for m in inside.get("messages") or []
             if "purg" in m and d.get("asked_inside", "?") in m]
    out.append((
        "review_refuses_a_date_inside_the_purged_interval_by_name",
        inside.get("bar_state") == "error" and not inside.get("overlay")
        and not inside.get("cached") and bool(named),
        f"etat barre={inside.get('bar_state')!r} entites dessinees="
        f"{sorted((inside.get('overlay') or {}).keys())} resultat en cache="
        f"{inside.get('cached')} messages={inside.get('messages')} : dans "
        f"l'intervalle purge le journal ne sait plus l'etat (P9 existait) ; "
        f"il faut un refus qui nomme l'intervalle, jamais l'etat d'une autre "
        f"date dessine{crash}",
    ))
    after = d.get("at_after") or {}
    p1a = (after.get("overlay") or {}).get("P1") or {}
    out.append((
        "review_answers_a_date_after_the_purged_interval",
        after.get("answered") == d.get("asked_after")
        and after.get("bar_state") != "error"
        and p1a.get("nom") == "Alpha0" and "P9" not in (after.get("overlay") or {}),
        f"date demandee={d.get('asked_after')} reconstruite="
        f"{after.get('answered')!r} etat barre={after.get('bar_state')!r} "
        f"P1={p1a or 'ABSENTE'} (attendu nom=Alpha0){crash}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rvw_purge_interval_not_floor.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
