"""rw_rewind_untrusted_identity - RecoverLand validation runtime.

Invariant: I-3 (une cle d'entite designe UNE entite) et I-REWIND-01 (le
rewind ne neutralise jamais en silence une edition qu'aucune trace ne
compense ; ce qu'il ne peut pas prouver, il le refuse en le nommant).

Trois lectures du journal que le rewind devinait au lieu de prouver
-------------------------------------------------------------------
B. Cle composite journalisee avant v7. Une couche a cle (zone_id,
   parcel_no) etait journalisee avec la PREMIERE colonne seule :
   ``pk:zone_id=12`` designe toutes les parcelles de la zone 12. Le
   rewind range l'INSERT de la parcelle (12,9) et le DELETE de la parcelle
   (12,7) dans le meme seau et les annule l'un par l'autre : ni l'un ni
   l'autre n'est compense, le rewind annonce un succes, (12,9) reste et
   (12,7) manque. Deux UPDATE de parcelles differentes partagent aussi la
   cle que l'executeur utilise pour retrouver une entite re-inseree
   (fid_remap) : la compensation de l'une peut viser l'autre.

A4. Trace ancienne a forme ambigue. Une trace d'avant v7 de meme
   operation que l'evenement qu'elle nomme, suivie de 9 editions de
   l'entite qu'aucune trace ne nomme : un rewind v6 fusionnait une chaine
   longue en un seul evenement (la trace couvrait tout), mais une
   restauration Mode A d'UNE edition suivie de 9 editions a la meme forme.
   Le rewind devinait "fusionne" et neutralisait les 9 editions en
   silence : elles n'etaient jamais compensees, le rewind annoncait un
   succes sur une couche qui n'etait pas a la date T.

A5. Trace ancienne qui designe une trace. Une trace legacy C reference
   une trace R qui existe mais n'est pas dans la fenetre (R restaure un
   evenement anterieur a T). La fenetre prend C (R est posterieure a T) ;
   le rewind ne trouvait pas R dans la fenetre et lisait C comme une
   orpheline purgee : un intervalle s'ouvrait depuis le debut de la
   fenetre et neutralisait en silence les editions de l'entite faites
   apres T.

Ce que le scenario execute
--------------------------
Un vrai journal SQLite (schema du plugin), les vraies fonctions
``fetch_events_after_cutoff`` et ``collapse_rewind_events_with_stats``.

Echec concret pour l'utilisateur (avant correctif)
--------------------------------------------------
B : une parcelle creee apres T reste, une parcelle supprimee apres T ne
revient pas, rapport sans echec. A4/A5 : des modifications faites apres T
restent en place apres un rewind annonce reussi.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_rewind_untrusted_identity"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_CUTOFF = "2026-05-01T12:00:00+00:00"


def _ts(minute, second=0):
    return f"2026-05-01T{12 + minute // 60:02d}:{minute % 60:02d}:{second:02d}+00:00"


def _row(ds, op, identity, entity, ts, attrs, schema=7, restored_from=None,
         session="s1"):
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": ds,
        "layer_id_snapshot": ds, "layer_name_snapshot": ds,
        "provider_type": "postgres", "feature_identity_json": json.dumps(identity),
        "operation_type": op, "attributes_json": json.dumps(attrs),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": None,
        "field_schema_json": None, "user_name": "u",
        "session_id": None if restored_from else session,
        "created_at": ts, "restored_from_event_id": restored_from,
        "entity_fingerprint": entity, "event_schema_version": schema,
        "new_geometry_wkb": None, "invalidated_at": None,
    }


def _insert(conn, row) -> int:
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    return conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({marks})",
                        tuple(row.values())).lastrowid


def _legacy(zone, fid):
    return {"fid": fid, "pk_field": "zone_id", "pk_value": zone}


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    ids = {}
    # --- B: composite key (zone_id, parcel_no) --------------------------
    # A v7 row of the layer, BEFORE the cutoff: only the journal as a
    # whole says what the key columns are.
    ids["b_v7"] = _insert(conn, _row(
        "cpk_ds", "UPDATE",
        {"fid": 5, "pk_fields": ["zone_id", "parcel_no"], "pk_values": [12, 5]},
        "pkc:zone_id=12|parcel_no=5", "2026-05-01T08:00:00+00:00",
        {"changed_only": {"nom": {"old": "a", "new": "b"}}}))
    ids["b_ins_129"] = _insert(conn, _row(
        "cpk_ds", "INSERT", _legacy(12, 9), "pk:zone_id=12", _ts(1),
        {"all_attributes": {"zone_id": 12, "parcel_no": 9, "nom": "neuf"}}, schema=6))
    ids["b_del_127"] = _insert(conn, _row(
        "cpk_ds", "DELETE", _legacy(12, 7), "pk:zone_id=12", _ts(2),
        {"all_attributes": {"zone_id": 12, "parcel_no": 7, "nom": "sept"}}, schema=6))
    ids["b_upd_121"] = _insert(conn, _row(
        "cpk_ds", "UPDATE", _legacy(12, 1), "pk:zone_id=12", _ts(3),
        {"changed_only": {"nom": {"old": "un", "new": "UN"}}}, schema=6))
    ids["b_upd_123"] = _insert(conn, _row(
        "cpk_ds", "UPDATE", _legacy(12, 3), "pk:zone_id=12", _ts(4),
        {"changed_only": {"nom": {"old": "trois", "new": "TROIS"}}}, schema=6))
    # Control: a single-column key, legacy rows, same feature created and
    # deleted after T: still one lifetime, cancelled.
    ids["k_v7"] = _insert(conn, _row(
        "one_ds", "UPDATE", {"fid": 1, "pk_field": "gid", "pk_value": 1},
        "pk:gid=1", "2026-05-01T08:00:00+00:00",
        {"changed_only": {"nom": {"old": "a", "new": "b"}}}))
    ids["k_ins"] = _insert(conn, _row(
        "one_ds", "INSERT", {"fid": 40, "pk_field": "gid", "pk_value": 40},
        "pk:gid=40", _ts(1), {"all_attributes": {"gid": 40, "nom": "x"}}, schema=6))
    ids["k_del"] = _insert(conn, _row(
        "one_ds", "DELETE", {"fid": 40, "pk_field": "gid", "pk_value": 40},
        "pk:gid=40", _ts(2), {"all_attributes": {"gid": 40, "nom": "x"}}, schema=6))

    # --- A4: ambiguous legacy trace --------------------------------------
    fid3 = {"fid": 3}
    ids["a4_e1"] = _insert(conn, _row(
        "amb_ds", "UPDATE", fid3, "fid:3", _ts(10),
        {"changed_only": {"val": {"old": 0, "new": 1}}}, schema=6))
    for i in range(9):
        ids[f"a4_u{i}"] = _insert(conn, _row(
            "amb_ds", "UPDATE", fid3, "fid:3", _ts(11 + i),
            {"changed_only": {"val": {"old": i + 1, "new": i + 2}}}, schema=6))
    ids["a4_trace"] = _insert(conn, _row(
        "amb_ds", "UPDATE", fid3, "fid:3", _ts(30),
        {"_restore_ref": ids["a4_e1"]}, schema=6, restored_from=ids["a4_e1"]))

    # --- A5: legacy trace referencing a trace outside the window --------
    fid4 = {"fid": 4}
    ids["a5_src"] = _insert(conn, _row(
        "t2t_ds", "UPDATE", fid4, "fid:4", "2026-05-01T09:00:00+00:00",
        {"changed_only": {"nom": {"old": "p", "new": "q"}}}))
    ids["a5_r"] = _insert(conn, _row(
        "t2t_ds", "UPDATE", fid4, "fid:4", _ts(40),
        {"_restore_ref": ids["a5_src"]}, restored_from=ids["a5_src"]))
    ids["a5_u"] = _insert(conn, _row(
        "t2t_ds", "UPDATE", fid4, "fid:4", _ts(41),
        {"changed_only": {"val": {"old": 5, "new": 6}}}))
    ids["a5_c"] = _insert(conn, _row(
        "t2t_ds", "UPDATE", fid4, "fid:4", _ts(42),
        {"_restore_ref": ids["a5_r"]}, schema=6, restored_from=ids["a5_r"]))
    conn.commit()
    conn.close()
    ctx.data["path"] = path
    ctx.data["ids"] = ids
    flog(f"rw_rewind_untrusted_identity setup: trace_id={ctx.trace_id} ids={ids}")


def _window(conn, ds):
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    return fetch_events_after_cutoff(
        conn, ds, RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, inclusive=True))


def _collapse(events, **kw):
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    try:
        return collapse_rewind_events_with_stats(events, **kw)
    except TypeError:
        # A collapse that cannot take the live layer's key columns.
        return collapse_rewind_events_with_stats(events)


def _dedup_helper(name, default):
    """A helper of rewind_dedup, or *default* on a version without it."""
    from recoverland.core import rewind_dedup
    return getattr(rewind_dedup, name, default)


def _plan(active):
    plan_item_kind = _dedup_helper("plan_item_kind", lambda e: None)
    return [(e.event_id, e.operation_type, e.entity_fingerprint,
             (plan_item_kind(e) or "user")) for e in active]


def run(ctx):
    from recoverland.core.logger import flog
    refusal_reason = _dedup_helper("refusal_reason", lambda e: "")
    conn = sqlite3.connect(ctx.data["path"])
    out = {}
    try:
        active, stats = _collapse(_window(conn, "cpk_ds"))
        out["b_plan"] = _plan(active)
        active, _s = _collapse(_window(conn, "one_ds"))
        out["k_plan"] = _plan(active)
        # B through the collapse alone, legacy rows only, the live layer's
        # key columns handed over (the window above had the journal's).
        legacy_only = [e for e in _window(conn, "cpk_ds")
                       if (e.event_schema_version or 0) < 7]
        legacy_only = [e._replace(entity_fingerprint="pk:zone_id=12")
                       for e in legacy_only]
        active, _s = _collapse(
            legacy_only,
            key_columns=lambda ds: ["zone_id", "parcel_no"] if ds == "cpk_ds" else None)
        out["b3_plan"] = _plan(active)
        active, stats = _collapse(_window(conn, "amb_ds"))
        out["a4_plan"] = _plan(active)
        out["a4_refusals"] = [refusal_reason(e) for e in active if refusal_reason(e)]
        out["a4_stats"] = stats
        window = _window(conn, "t2t_ds")
        # The TRACES the window hands over (a refusal row it adds may
        # carry the id of the trace it names).
        out["a5_window"] = [e.event_id for e in window
                            if e.restored_from_event_id is not None]
        active, stats = _collapse(window)
        out["a5_plan"] = _plan(active)
        out["a5_refusals"] = [refusal_reason(e) for e in active if refusal_reason(e)]
    finally:
        conn.close()
    ctx.data["out"] = out
    flog(f"rw_rewind_untrusted_identity run: trace_id={ctx.trace_id} "
         f"out={json.dumps(out, default=str)[:3000]}")


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
    out = ctx.data.get("out") or {}
    res = []
    b = out.get("b_plan") or []
    b_ids = {row[0] for row in b}
    res.append((
        "b_composite_insert_and_delete_both_compensated",
        {ids.get("b_ins_129"), ids.get("b_del_127")} <= b_ids,
        f"plan={b} : l'INSERT de (12,9) et le DELETE de (12,7) sont deux "
        f"parcelles ; annules l'un par l'autre, (12,9) reste et (12,7) ne "
        f"revient pas, rewind annonce reussi.",
    ))
    fps = {row[0]: row[2] for row in b}
    res.append((
        "b_rows_of_different_parcels_never_share_a_key",
        len({
            fps.get(ids.get(k))
            for k in ("b_ins_129", "b_del_127", "b_upd_121", "b_upd_123")
        }) == 4,
        f"cles={fps} : deux parcelles sous une meme cle, et la compensation "
        f"de l'une peut viser l'autre (fid_remap, seau de dedup).",
    ))
    res.append((
        "b_rebuilt_keys_name_the_parcel",
        fps.get(ids.get("b_ins_129")) == 'pkc:zone_id=12|parcel_no=9'
        and fps.get(ids.get("b_del_127")) == 'pkc:zone_id=12|parcel_no=7',
        f"cles={fps} : un INSERT ou un DELETE porte toute la ligne, sa cle "
        f"composite se relit de son instantane.",
    ))
    b3 = out.get("b3_plan") or []
    res.append((
        "b_live_key_columns_prove_it_without_a_v7_row",
        {ids.get("b_ins_129"), ids.get("b_del_127")} <= {row[0] for row in b3},
        f"plan={b3} : sans ligne v7 dans la fenetre, les colonnes de la cle "
        f"de la couche vivante suffisent a separer les parcelles.",
    ))
    k = out.get("k_plan") or []
    res.append((
        "control_single_key_lifetime_still_cancels",
        not [row for row in k if row[0] in (ids.get("k_ins"), ids.get("k_del"))],
        f"plan={k} : une cle a une colonne reste une identite prouvee ; "
        f"creee puis supprimee apres T, l'entite n'a rien a compenser.",
    ))
    a4 = out.get("a4_plan") or []
    a4_ids = {row[0] for row in a4}
    edits = {ids.get(f"a4_u{i}") for i in range(9)}
    res.append((
        "a4_ambiguous_legacy_trace_is_refused_by_name",
        bool(out.get("a4_refusals")) and edits <= a4_ids,
        f"refus={out.get('a4_refusals')} plan={[r[0] for r in a4]} "
        f"editions={sorted(edits)} : les 9 editions posterieures a T "
        f"etaient neutralisees en silence (couverture devinee) ; le rewind "
        f"doit refuser en nommant la trace, pas annoncer un succes.",
    ))
    a5 = out.get("a5_plan") or []
    res.append((
        "a5_trace_to_trace_is_not_read_as_an_orphan",
        ids.get("a5_u") in {row[0] for row in a5}
        and ids.get("a5_c") not in (out.get("a5_window") or []),
        f"fenetre={out.get('a5_window')} plan={a5} refus={out.get('a5_refusals')} "
        f": l'edition {ids.get('a5_u')} faite apres T etait neutralisee par "
        f"un intervalle 'orphelin' ouvert pour une trace dont la reference "
        f"existe ; elle n'etait jamais compensee.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rw_rewind_untrusted_identity.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return res


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
