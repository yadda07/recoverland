# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_rewind_legacy_key_records - RecoverLand validation runtime.

Invariant: I-3 (une cle d'entite designe UNE entite) et I-REWIND-01 (le
rewind ne modifie jamais une entite dont l'identite n'est pas prouvee ;
ce qu'il ne peut pas prouver, il le refuse en le nommant).

Le mecanisme
------------
Avant v7, une couche a cle composite (zone_id, parcel_no) etait
journalisee avec la PREMIERE colonne seule : ``pk:zone_id=12`` designe
toutes les parcelles de la zone 12. Le rewind reconstruit la cle d'un
INSERT ou d'un DELETE depuis son instantane (``pkc:...``). Un UPDATE ne
porte que les champs modifies : il ne peut pas nommer sa parcelle.

1. UPDATE orphelin (review composite-orphan-update). La parcelle (12,9)
   est creee, renommee "Parcelle Dupont" puis supprimee apres T, dans une
   seule session d'edition. L'INSERT et le DELETE recoivent la cle
   reconstruite et s'annulent ; l'UPDATE, garde sur une cle a lui,
   restait SEUL dans le plan : la compensation d'une parcelle qui
   n'existe ni a T ni maintenant. L'executeur retirait la cle non
   prouvee, cherchait la parcelle par ses valeurs NOUVELLES et trouvait
   (12,10), qui porte justement "Parcelle Dupont" : il y ecrivait
   "neuf". Correction : l'UPDATE est rattache a la cle de l'INSERT et du
   DELETE du meme enregistrement (meme cle journalisee, meme FID, meme
   session d'edition, valeurs qui s'enchainent) ; sinon il est refuse
   par son nom.

2. Colonnes de cle inconnues (review, item B). Quand le journal ne
   contient aucune ligne v7 de la couche et qu'aucune couche n'est
   donnee, la colonne journalisee peut etre la cle entiere ou la
   premiere de plusieurs. L'INSERT de (12,9) et le DELETE de (12,7)
   s'annulaient encore l'un par l'autre en silence. Maintenant une cle
   sous laquelle le plan tient plusieurs enregistrements (FID) est
   refusee par son nom ; une cle a une seule colonne dont l'entite est
   creee, modifiee puis supprimee dans trois sessions reste annulee (la
   separer par FID et session, comme propose, aurait mis la re-insertion
   et la suppression de la meme entite dans deux seaux : le chemin de
   resurrection de l'executeur).

Ce que le scenario execute
--------------------------
Un vrai journal SQLite (schema du plugin), les vraies fonctions
``fetch_events_after_cutoff``, ``collapse_rewind_events_with_stats``,
``plan_temporal_restore`` et ``execute_restore_plan`` (preflight compris),
sur une couche memoire presentee comme PostgreSQL a cle (zone_id,
parcel_no) (fixture de rw_composite_pk_identity).

Echec concret pour l'utilisateur (avant correctif)
--------------------------------------------------
1 : la parcelle (12,10) "Parcelle Dupont" devient "neuf" apres un rewind
qui ne devait rien ecrire. 2 : (12,9) reste, (12,7) ne revient pas,
rewind annonce reussi.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_rewind_legacy_key_records"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_CUTOFF = "2026-05-01T12:00:00+00:00"
_DS = "postgres::host=h port=5432 dbname=cad schema=public table=parcelles"
_DS_ONE = "postgres::host=h port=5432 dbname=cad schema=public table=bornes"


def _ts(minute):
    return f"2026-05-01T12:{minute:02d}:00+00:00"


def _row(ds, op, identity, entity, ts, attrs, schema=6, session="s1"):
    return {
        "project_fingerprint": "proj", "datasource_fingerprint": ds,
        "layer_id_snapshot": "lyr", "layer_name_snapshot": "parcelles",
        "provider_type": "postgres", "feature_identity_json": json.dumps(identity),
        "operation_type": op, "attributes_json": json.dumps(attrs),
        "geometry_wkb": None, "geometry_type": "Point", "crs_authid": "EPSG:2154",
        "field_schema_json": None, "user_name": "u", "session_id": session,
        "created_at": ts, "restored_from_event_id": None,
        "entity_fingerprint": entity, "event_schema_version": schema,
        "new_geometry_wkb": None, "invalidated_at": None,
    }


def _insert(conn, row):
    cols = ", ".join(row)
    marks = ", ".join("?" for _ in row)
    return conn.execute(f"INSERT INTO audit_event ({cols}) VALUES ({marks})",
                        tuple(row.values())).lastrowid


def _make_layer():
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=zone_id:integer"
                         "&field=parcel_no:integer&field=nom:string(40)"
                         "&field=usage:string(20)", "parcelles", "memory")
    feats = []
    for zone, parcel, nom, usage, x in (
            (12, 1, "parcelle un", "libre", 1.0),
            (12, 5, "b", "libre", 5.0),
            (12, 7, "parcelle sept", "libre", 7.0),
            (12, 10, "Parcelle Dupont", "bati", 10.0)):
        f = QgsFeature(mem.fields())
        f.setAttributes([zone, parcel, nom, usage])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, 100.0)))
        feats.append(f)
    mem.dataProvider().addFeatures(feats)
    return mem


def _rows(layer):
    return sorted((f["zone_id"], f["parcel_no"], f["nom"], f["usage"])
                  for f in layer.getFeatures())


def _journal(case):
    """A journal for *case*; returns its path."""
    from recoverland.core.sqlite_schema import initialize_schema
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    if case.startswith("orphan") or case == "untied_update":
        # One v7 row of the layer, before T: the journal knows the key.
        _insert(conn, _row(_DS, "UPDATE", {"fid": 2, "pk_fields": ["zone_id", "parcel_no"],
                                           "pk_values": [12, 5]},
                           "pkc:zone_id=12|parcel_no=5", "2026-05-01T08:00:00+00:00",
                           {"changed_only": {"nom": {"old": "a", "new": "b"}}}, schema=7))
    if case in ("orphan_two_fields", "orphan_one_field", "untied_update"):
        leg = {"fid": 9, "pk_field": "zone_id", "pk_value": 12}
        _insert(conn, _row(_DS, "INSERT", leg, "pk:zone_id=12", _ts(1),
                           {"all_attributes": {"zone_id": 12, "parcel_no": 9,
                                               "nom": "neuf", "usage": "?"}}))
        changed = {"nom": {"old": "neuf", "new": "Parcelle Dupont"}}
        after = {"zone_id": 12, "parcel_no": 9, "nom": "Parcelle Dupont", "usage": "?"}
        if case != "orphan_one_field":
            changed["usage"] = {"old": "?", "new": "bati"}
            after["usage"] = "bati"
        # untied_update: the UPDATE comes from another edit session, where
        # FID 9 may name another parcel; nothing ties it to (12,9).
        _insert(conn, _row(_DS, "UPDATE", leg, "pk:zone_id=12", _ts(2),
                           {"changed_only": changed},
                           session="s2" if case == "untied_update" else "s1"))
        _insert(conn, _row(_DS, "DELETE", leg, "pk:zone_id=12", _ts(3),
                           {"all_attributes": after}))
    if case == "unknown_two_parcels":
        # No v7 row anywhere: the key columns are unknown.
        _insert(conn, _row(_DS, "INSERT", {"fid": 9, "pk_field": "zone_id", "pk_value": 12},
                           "pk:zone_id=12", _ts(1),
                           {"all_attributes": {"zone_id": 12, "parcel_no": 9,
                                               "nom": "neuf", "usage": "?"}}))
        _insert(conn, _row(_DS, "DELETE", {"fid": 7, "pk_field": "zone_id", "pk_value": 12},
                           "pk:zone_id=12", _ts(2),
                           {"all_attributes": {"zone_id": 12, "parcel_no": 7,
                                               "nom": "parcelle sept", "usage": "libre"}}))
    if case == "unknown_single_lifetime":
        one = {"fid": 40, "pk_field": "gid", "pk_value": 40}
        _insert(conn, _row(_DS_ONE, "INSERT", one, "pk:gid=40", _ts(1),
                           {"all_attributes": {"gid": 40, "nom": "x"}}, session="s1"))
        _insert(conn, _row(_DS_ONE, "UPDATE", one, "pk:gid=40", _ts(2),
                           {"changed_only": {"nom": {"old": "x", "new": "y"}}},
                           session="s2"))
        _insert(conn, _row(_DS_ONE, "DELETE", one, "pk:gid=40", _ts(3),
                           {"all_attributes": {"gid": 40, "nom": "y"}}, session="s3"))
    conn.commit()
    conn.close()
    return path


_CASES = ("orphan_two_fields", "orphan_one_field", "untied_update",
          "unknown_two_parcels", "unknown_single_lifetime")


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["paths"] = {case: _journal(case) for case in _CASES}
    flog(f"rw_rewind_legacy_key_records setup: trace_id={ctx.trace_id} "
         f"cases={list(_CASES)}")


def _one(ctx, case):
    from scripts.validation.scenarios.rw_composite_pk_identity import PkLayer
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.restore_executor import execute_restore_plan
    from recoverland.core import rewind_dedup
    refusal_reason = getattr(rewind_dedup, "refusal_reason", lambda e: "")
    base = _make_layer()
    layer = PkLayer(base)
    out = {"before": _rows(base)}
    ds = _DS_ONE if case == "unknown_single_lifetime" else _DS
    conn = sqlite3.connect(ctx.data["paths"][case])
    try:
        cutoff = RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, inclusive=True)
        window = fetch_events_after_cutoff(conn, ds, cutoff)
        active, stats = collapse_rewind_events_with_stats(window)
        out["plan"] = [(e.event_id, e.operation_type, e.entity_fingerprint)
                       for e in active]
        out["refusals"] = [refusal_reason(e) for e in active if refusal_reason(e)]
        if active and ds == _DS:
            plan = plan_temporal_restore(active, ds, "parcelles", cutoff)
            by_id = {e.event_id: e for e in active}
            try:
                rep = execute_restore_plan(plan, by_id, layer,
                                           trace_id=ctx.trace_id, conn=conn)
            except TypeError:
                # An executor that takes no journal connection.
                rep = execute_restore_plan(plan, by_id, layer,
                                           trace_id=ctx.trace_id)
            out["succeeded"] = [getattr(r, "event_id", r) for r in rep.succeeded]
            out["failed"] = {str(k): str(v)[:300] for k, v in (rep.failed or {}).items()}
    finally:
        conn.close()
    out["after"] = _rows(base)
    return out


def run(ctx):
    from recoverland.core.logger import flog
    res = {}
    for case in _CASES:
        try:
            res[case] = _one(ctx, case)
        except Exception:  # noqa: BLE001 - reported by the assertions
            import traceback
            res[case] = {"crash": traceback.format_exc()[-1500:]}
        flog(f"rw_rewind_legacy_key_records case={case} trace_id={ctx.trace_id} "
             f"{json.dumps(res[case], default=str)[:2500]}")
    ctx.data["res"] = res


def teardown(ctx):
    for path in (ctx.data.get("paths") or {}).values():
        try:
            os.remove(path)
        except OSError:
            pass


def _refused_by_name(r):
    failed = " ".join((r.get("failed") or {}).values())
    return (bool(r.get("refusals")) and not r.get("succeeded")
            and "unrebuildable_state" in failed)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    res = ctx.data.get("res") or {}
    out = []
    for case in ("orphan_two_fields", "orphan_one_field"):
        r = res.get(case) or {}
        out.append((
            f"{case}_writes_nothing",
            not r.get("crash") and r.get("plan") == [] and r.get("after") == r.get("before"),
            f"{json.dumps(r, default=str)[:2500]} : la parcelle (12,9) a ete creee, "
            f"renommee et supprimee apres T dans une seule session ; a T comme "
            f"maintenant elle n'existe pas, le rewind ne doit rien ecrire. "
            f"L'UPDATE laisse seul dans le plan etait ecrit sur (12,10), "
            f"qui porte les memes valeurs.",
        ))
    r = res.get("untied_update") or {}
    out.append((
        "untied_update_refused_by_name_layer_untouched",
        not r.get("crash") and _refused_by_name(r) and r.get("after") == r.get("before")
        and any("first column of the layer's key" in x for x in r.get("refusals") or []),
        f"{json.dumps(r, default=str)[:2500]} : un UPDATE d'une autre session "
        f"d'edition, que rien ne rattache a la parcelle (12,9), ne doit pas "
        f"atteindre l'executeur : refus nomme, couche intacte.",
    ))
    r = res.get("unknown_two_parcels") or {}
    out.append((
        "unknown_key_columns_two_parcels_never_cancelled_in_silence",
        not r.get("crash") and _refused_by_name(r) and r.get("after") == r.get("before")
        and any("key columns of this layer are unknown" in x
                for x in r.get("refusals") or []),
        f"{json.dumps(r, default=str)[:2500]} : sans colonne de cle connue, "
        f"l'INSERT de (12,9) et le DELETE de (12,7) etaient annules l'un par "
        f"l'autre ; (12,9) restait, (12,7) ne revenait pas, rewind annonce "
        f"reussi. Attendu : refus nomme, couche intacte.",
    ))
    r = res.get("unknown_single_lifetime") or {}
    out.append((
        "unknown_key_columns_single_key_lifetime_still_cancels",
        not r.get("crash") and r.get("plan") == [],
        f"{json.dumps(r, default=str)[:1500]} : une entite a cle simple creee, "
        f"modifiee puis supprimee apres T (trois sessions) n'a rien a "
        f"compenser ; separee par session, sa re-insertion et sa suppression "
        f"tomberaient dans deux seaux et l'executeur la ressusciterait.",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_rewind_legacy_key_records.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
