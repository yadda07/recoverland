"""maint_logical_purge_keeps_traces - RecoverLand validation runtime.

Invariant: I-8 (les traces de restauration en vigueur sont la memoire du
rewind ; elles ne sont jamais des dechets) et "aucune perte silencieuse" :
une purge qui retire de l'historique doit dire jusqu'a quelle date le
journal ne peut plus reconstruire l'etat.

Le mecanisme
------------
La maintenance propose trois purges "logiques", cochees par defaut dans
l'interface (decochees depuis, avec les dates qu'elles retirent affichees :
maint_purge_dialog_defaults) :

- paires INSERT/DELETE : ``LEAD() OVER (PARTITION BY entity_fingerprint,
  session_id ORDER BY event_id)``. Pas de datasource dans la cle, pas de
  filtre sur les traces. Toutes les traces ont session_id NULL : les
  traces de 'fid:5' de TOUTES les couches tombent dans la meme partition,
  et une trace INSERT (couche A) suivie d'une trace DELETE (couche B) est
  supprimee comme une "paire".
- traces orphelines : ``restored_from_event_id NOT IN (evenements
  utilisateur)``. Une trace qui annule une trace (le saut en avant d'un
  rewind, schema 7) est prise pour une orpheline et supprimee : la
  compensation annulee redevient active dans le journal.
- toutes ces suppressions (et celle des evenements invalides) retirent de
  l'historique ce qui permet de reconstruire certaines dates, sans
  enregistrer d'horizon : un rewind vers ces dates rend un etat faux au
  lieu d'etre refuse.

Echec concret pour l'utilisateur
--------------------------------
Couche A : DELETE fid:5 ; couche B : INSERT fid:5 ; un rewind multi-couche
ecrit la trace A (INSERT) et la trace B (DELETE). La purge par defaut
supprime les deux traces. Le rewind suivant a la meme date re-insere
l'entite de A une seconde fois (doublon) et echoue sur B (target_absent) ;
le saut en avant ne peut plus defaire ni l'un ni l'autre.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "maint_logical_purge_keeps_traces"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _row(ds, op, fid, ts, session=None, restored_from=None,
         invalidated_at=None, schema=7, attrs=None):
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": ds,
        "layer_id_snapshot": ds, "layer_name_snapshot": ds,
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": fid}),
        "operation_type": op,
        "attributes_json": json.dumps(attrs or {"all_attributes": {"nom": "x"}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": None,
        "field_schema_json": None, "user_name": "u", "session_id": session,
        "created_at": ts, "restored_from_event_id": restored_from,
        "entity_fingerprint": f"fid:{fid}", "event_schema_version": schema,
        "new_geometry_wkb": None, "invalidated_at": invalidated_at,
    }


def _insert(conn, row) -> int:
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    return conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({marks})",
                        tuple(row.values())).lastrowid


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    ctx.data["path"] = path
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    ids = {}
    # Two layers, same fid, one multi-layer rewind (the finding's journal).
    ids["a_delete"] = _insert(conn, _row("mlp_ds_a", "DELETE", 5, "2026-03-01T09:00:00+00:00", "s-a"))
    ids["b_insert"] = _insert(conn, _row("mlp_ds_b", "INSERT", 5, "2026-03-01T09:05:00+00:00", "s-b"))
    ids["a_trace"] = _insert(conn, _row("mlp_ds_a", "INSERT", 5, "2026-03-01T10:00:00+00:00",
                                        restored_from=ids["a_delete"]))
    ids["b_trace"] = _insert(conn, _row("mlp_ds_b", "DELETE", 5, "2026-03-01T10:00:01+00:00",
                                        restored_from=ids["b_insert"]))
    # A forward hop: a trace that cancels a trace (schema 7).
    ids["c_update"] = _insert(conn, _row("mlp_ds_c", "UPDATE", 8, "2026-03-02T09:00:00+00:00", "s-c",
                                         attrs={"changed_only": {"a": {"old": 0, "new": 1}}}))
    ids["c_trace"] = _insert(conn, _row("mlp_ds_c", "UPDATE", 8, "2026-03-02T10:00:00+00:00",
                                        restored_from=ids["c_update"]))
    ids["c_cancel"] = _insert(conn, _row("mlp_ds_c", "UPDATE", 8, "2026-03-02T11:00:00+00:00",
                                         restored_from=ids["c_trace"]))
    # A genuine user pair: created then deleted in one edit session.
    ids["p_insert"] = _insert(conn, _row("mlp_ds_p", "INSERT", 9, "2026-03-03T09:00:00+00:00", "s-p"))
    ids["p_delete"] = _insert(conn, _row("mlp_ds_p", "DELETE", 9, "2026-03-03T09:30:00+00:00", "s-p"))
    # A real orphan: its source is gone.
    ids["o_trace"] = _insert(conn, _row("mlp_ds_o", "INSERT", 4, "2026-03-04T10:00:00+00:00",
                                        restored_from=999999))
    # An undone compensation (Undo): invalidated.
    ids["i_delete"] = _insert(conn, _row("mlp_ds_i", "DELETE", 6, "2026-03-05T09:00:00+00:00", "s-i"))
    ids["i_trace"] = _insert(conn, _row("mlp_ds_i", "INSERT", 6, "2026-03-05T10:00:00+00:00",
                                        restored_from=ids["i_delete"],
                                        invalidated_at="2026-03-05T11:00:00+00:00"))
    conn.commit()
    conn.close()
    ctx.data["ids"] = ids
    flog(f"maint_logical_purge_keeps_traces setup: trace_id={ctx.trace_id} ids={ids}")


def _refusal(conn, ds, cutoff):
    """What the rewind preflight answers for *cutoff* on *ds*."""
    from recoverland.core import retention
    if hasattr(retention, "coverage_refusal"):
        return retention.coverage_refusal(conn, ds, cutoff)
    horizon = retention.purged_horizon(ds)  # older API: session memory only
    return f"purged up to {horizon}" if horizon and cutoff <= horizon else None


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import apply_pragmas
    from recoverland.core.retention import (
        PurgeOptions, RetentionPolicy, count_logical_garbage_events,
        purge_old_events_with_options,
    )
    coverage_refusal = _refusal
    conn = sqlite3.connect(ctx.data["path"])
    apply_pragmas(conn)
    ctx.data["garbage_before"] = tuple(count_logical_garbage_events(conn))
    # The maintenance defaults: every logical purge checked, no date purge.
    result = purge_old_events_with_options(
        conn, RetentionPolicy(retention_days=36500, max_events=1_000_000),
        PurgeOptions(retention=False, invalidated=True,
                     insert_delete_pairs=True, orphan_traces=True))
    ctx.data["result"] = result._asdict()
    ctx.data["surviving"] = sorted(
        r[0] for r in conn.execute("SELECT event_id FROM audit_event"))
    ctx.data["refusals"] = {
        "pair_before_delete": coverage_refusal(conn, "mlp_ds_p", "2026-03-03T09:15:00+00:00"),
        "orphan_before_trace": coverage_refusal(conn, "mlp_ds_o", "2026-03-04T09:00:00+00:00"),
        "undo_before_undo": coverage_refusal(conn, "mlp_ds_i", "2026-03-05T10:30:00+00:00"),
        "untouched_layer": coverage_refusal(conn, "mlp_ds_a", "2026-03-01T08:00:00+00:00"),
        # Before the purged pair's INSERT: the pair cancels out there.
        "before_pair_insert": coverage_refusal(conn, "mlp_ds_p", "2026-03-03T08:00:00+00:00"),
        "before_undone_trace": coverage_refusal(conn, "mlp_ds_i", "2026-03-05T09:30:00+00:00"),
    }
    conn.close()
    flog(f"maint_logical_purge_keeps_traces run: trace_id={ctx.trace_id} "
         f"result={ctx.data['result']} surviving={ctx.data['surviving']} "
         f"refusals={ctx.data['refusals']}")


def teardown(ctx):
    path = ctx.data.get("path")
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    ids = ctx.data.get("ids") or {}
    alive = set(ctx.data.get("surviving") or [])
    ref = ctx.data.get("refusals") or {}
    out = []
    out.append((
        "multi_layer_traces_survive_the_pair_purge",
        {ids.get("a_trace"), ids.get("b_trace")} <= alive,
        f"traces restantes={sorted(alive)} traces A={ids.get('a_trace')} "
        f"B={ids.get('b_trace')} : supprimees, le rewind suivant reinsere "
        f"l'entite de A en double et echoue sur B.",
    ))
    out.append((
        "users_events_of_two_layers_are_not_a_pair",
        {ids.get("a_delete"), ids.get("b_insert")} <= alive,
        f"restants={sorted(alive)} : un DELETE de A et un INSERT de B ne sont "
        f"pas le meme objet.",
    ))
    out.append((
        "a_cancelling_trace_is_not_an_orphan",
        {ids.get("c_trace"), ids.get("c_cancel")} <= alive,
        f"restants={sorted(alive)} trace={ids.get('c_trace')} annulation="
        f"{ids.get('c_cancel')} : supprimer l'annulation reactive la "
        f"compensation, l'edition re-appliquee est a nouveau comptee comme "
        f"defaite.",
    ))
    out.append((
        "genuine_garbage_is_still_purged",
        ids.get("p_insert") not in alive and ids.get("p_delete") not in alive
        and ids.get("o_trace") not in alive and ids.get("i_trace") not in alive,
        f"restants={sorted(alive)} : la paire utilisateur, l'orpheline reelle "
        f"et la trace invalidee restent purgeables (bouton de maintenance).",
    ))
    out.append((
        "purges_record_what_they_made_unrebuildable",
        all(ref.get(k) for k in ("pair_before_delete", "orphan_before_trace",
                                 "undo_before_undo")),
        f"refus du rewind apres purge={ref} : sans horizon, un rewind vers une "
        f"date que la purge rend impossible a reconstruire rend un etat faux "
        f"annonce comme un succes.",
    ))
    out.append((
        "untouched_layers_keep_their_history",
        ref.get("untouched_layer") is None,
        f"refus sur une couche que la purge n'a pas touchee="
        f"{ref.get('untouched_layer')!r}",
    ))
    out.append((
        "dates_before_a_purged_interval_stay_rewindable",
        ref.get("before_pair_insert") is None
        and ref.get("before_undone_trace") is None,
        f"avant l'INSERT de la paire purgee={ref.get('before_pair_insert')!r} "
        f"avant la compensation annulee={ref.get('before_undone_trace')!r} : "
        f"la paire ne rendait impossible que [INSERT, DELETE], la trace "
        f"invalidee que [creation, Undo] ; refuser toute date anterieure "
        f"desactive l'historique de la couche apres un clic de maintenance.",
    ))
    before = ctx.data.get("garbage_before") or ()
    out.append((
        "counters_match_the_purge",
        len(before) >= 4 and before[2] == 1 and before[3] == 1,
        f"compteurs avant purge (old, invalides, paires, orphelines, total)="
        f"{before} attendu paires=1 orphelines=1 : le compteur affiche doit "
        f"annoncer ce que la purge supprimera.",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"maint_logical_purge_keeps_traces.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
