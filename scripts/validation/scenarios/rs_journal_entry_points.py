# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_journal_entry_points - RecoverLand validation runtime.

Invariant: I-3 sans refus injustifie, sur CHAQUE point d'entree: la
preuve de continuite d'une cle par le journal (rs_key_continuity_journal)
doit pouvoir atteindre tous les chemins qui decident d'une identite.
Constat resolu: item 1 du tour executeur (et la defaillance de suite
`tx_serial_null_sentinels::gpkg_false_restored_as_false`).

Le mecanisme
------------
Une cle GeoPackage n'est une identite que si le contenu la corrobore,
ou si le journal prouve qu'elle n'a jamais change de mains. Le tour
precedent a construit cette preuve (`restore_service._key_continuity`),
mais elle ne sert que si l'appelant passe la connexion de lecture du
journal. Plusieurs points d'entree publics ne l'acceptaient pas
(`restore_deleted_feature`, `undo_apply_one`): un runner qui tient le
journal ne pouvait pas le leur donner, et 'flag: False -> True' sur une
couche ou toutes les autres lignes sont a True restait refuse
"value_shared:flag ... key_continuity_unproven (no_journal)" sur ces
chemins, alors que la cle n'a jamais change de mains.

Ce que l'utilisateur voit
-------------------------
Il coche "flag" par erreur et ne peut pas revenir en arriere: "Target
feature could not be verified", selon le bouton qu'il a presse.

Cas (GeoPackage, cle fid, journal SQLite reel; C, fid 3, seule a False)
----------------------------------------------------------------------
    E0  chaque point d'entree public qui decide d'une identite accepte
        la connexion du journal (mot-cle, None par defaut)
    E1  restore_updated_feature(journal=): FAUX revient
    E2  restore_batch(journal=): FAUX revient
    E3  execute_restore_plan(plan Mode A, conn=): FAUX revient
    E4  execute_restore_plan(plan temporel strict, conn=): FAUX revient
    E5  undo_prepare sans journal + undo_apply_one(journal=): l'annulation
        d'une restauration (FAUX partage par deux lignes) aboutit
    E6  restore_deleted_feature(journal=): la ligne supprimee revient
    N1..N4  temoins sans journal: E1 a E4 refuses NOMMES
        (key_continuity_unproven), couche intacte

Verdict avant correctif: FAIL (E0: restore_deleted_feature et
undo_apply_one n'acceptent pas le journal; E5 plante). Apres: PASS.
"""
from __future__ import annotations

import inspect
import json
import shutil
import tempfile

SCENARIO_ID = "rs_journal_entry_points"
INVARIANT = "I-3"


def _kcj():
    from scripts.validation.scenarios import rs_key_continuity_journal as kcj
    return kcj


def _entry_points():
    from recoverland.core import restore_service as rs
    from recoverland.core import restore_executor as rx
    return {
        "restore_service.restore_updated_feature": (rs.restore_updated_feature, "journal"),
        "restore_service.restore_inserted_feature": (rs.restore_inserted_feature, "journal"),
        "restore_service.restore_deleted_feature": (rs.restore_deleted_feature, "journal"),
        "restore_service.restore_batch": (rs.restore_batch, "journal"),
        "restore_service.build_fid_cache": (rs.build_fid_cache, "journal"),
        "restore_service.undo_prepare": (rs.undo_prepare, "journal"),
        "restore_service.undo_apply_one": (rs.undo_apply_one, "journal"),
        "restore_service.undo_restore_batch": (rs.undo_restore_batch, "journal"),
        "restore_executor.execute_restore_plan": (rx.execute_restore_plan, "conn"),
        "restore_executor._apply_via_buffer": (rx._apply_via_buffer, "journal"),
    }


def _flag_case(tmp, name):
    """GPKG of rs_key_continuity_journal, 'flag' of C edited False -> True."""
    kcj = _kcj()
    path = kcj._gpkg(tmp, name)
    layer = kcj._open(path, name)
    delta, identity = kcj._edit_flag(layer, 3, True)
    conn = kcj._journal()
    event = kcj._journalled(conn, kcj._event(layer, "UPDATE", identity,
                                             delta, 0))
    return path, layer, conn, event


def _flag_of_c(path, name):
    rows = _kcj()._flags(path, name)
    return next((flag for fid, _etat, flag in rows if fid == 3), "<absent>")


def _report(rep):
    if isinstance(rep, dict):
        return {k: v for k, v in rep.items() if k in (
            "success", "reason_code", "message", "identity_level", "crash")}
    return {"succeeded": sorted(rep.succeeded),
            "failed": {int(k): str(v)[:240] for k, v in rep.failed.items()}}


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rs_jep_")
    flog(f"rs_journal_entry_points setup: trace_id={ctx.trace_id}")


def _run_path(tmp, name, kind, with_journal):
    from recoverland.core import restore_service as rs
    from recoverland.core import restore_executor as rx
    from recoverland.core.restore_planner import (plan_event_restore,
                                                  plan_temporal_restore)
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    path, layer, conn, event = _flag_case(tmp, name)
    journal = conn if with_journal else None
    try:
        if kind == "updated":
            out = rs.restore_updated_feature(layer, event, journal=journal)
        elif kind == "batch":
            out = rs.restore_batch(layer, [event], journal=journal)
        elif kind == "plan_a":
            plan = plan_event_restore([event], event.datasource_fingerprint,
                                      name)
            out = rx.execute_restore_plan(plan, {event.event_id: event},
                                          layer, conn=journal)
        else:
            # At the event's own date: a journal of this fixture carries no
            # v7 purge record, and a date before its oldest event would be
            # refused by the coverage guard (not what E4 is about).
            cutoff = RestoreCutoff(CutoffType.BY_DATE, event.created_at,
                                   inclusive=True)
            plan = plan_temporal_restore([event], event.datasource_fingerprint,
                                         name, cutoff)
            out = rx.execute_restore_plan(plan, {event.event_id: event},
                                          layer, conn=journal)
        result = _report(out)
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        result = {"success": None, "crash": repr(exc)}
    del layer
    conn.close()
    return result, _flag_of_c(path, name)


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core import restore_service as rs
    kcj = _kcj()
    tmp = ctx.data["tmp"]
    out = {"signatures": {}}
    for label, (fn, kw) in _entry_points().items():
        try:
            params = inspect.signature(fn).parameters
            out["signatures"][label] = (
                kw in params and params[kw].default is None)
        except (TypeError, ValueError):
            out["signatures"][label] = False
    try:
        for kind in ("updated", "batch", "plan_a", "plan_t"):
            out[f"e_{kind}"] = _run_path(tmp, f"e_{kind}", kind, True)
            out[f"n_{kind}"] = _run_path(tmp, f"n_{kind}", kind, False)

        # E5: undo of the restore (C back to False, D False too), the
        # journal handed to undo_apply_one only.
        undo_rows = [("a", True, 1), ("b", True, 1), ("c", False, 1),
                     ("d", False, 1), ("e", True, 1)]
        path = kcj._gpkg(tmp, "e5", undo_rows)
        layer = kcj._open(path, "e5")
        delta, identity = kcj._edit_flag(layer, 3, True)
        kcj._edit_flag(layer, 3, False)
        conn = kcj._journal()
        event = kcj._journalled(conn, kcj._event(layer, "UPDATE", identity,
                                                 delta, 0))
        kcj._journalled(conn, kcj._event(
            layer, "UPDATE", identity,
            json.dumps({"_restore_ref": event.event_id}), 10,
            restored_from_event_id=event.event_id))
        try:
            context = rs.undo_prepare(layer, [event])
            out["e5"] = _report(rs.undo_apply_one(layer, event, context,
                                                  journal=conn))
        except Exception as exc:  # noqa: BLE001
            out["e5"] = {"success": None, "crash": repr(exc)}
        del layer
        conn.close()
        out["e5_flag"] = _flag_of_c(path, "e5")

        # E6: restore of a DELETE, the journal accepted.
        path = kcj._gpkg(tmp, "e6")
        layer = kcj._open(path, "e6")
        from qgis.core import QgsFeatureRequest
        from recoverland.core.identity import compute_feature_identity
        from recoverland.core.serialization import serialize_attributes
        feat = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([2])))
        identity = compute_feature_identity(layer, feat)
        snap = serialize_attributes(feat, [f.name() for f in layer.fields()])
        wkb = bytes(feat.geometry().asWkb())
        layer.dataProvider().deleteFeatures([2])
        layer.reload()
        conn = kcj._journal()
        event = kcj._journalled(conn, kcj._event(
            layer, "DELETE", identity, json.dumps({"all_attributes": snap}),
            0, geometry_wkb=wkb))
        try:
            out["e6"] = _report(rs.restore_deleted_feature(layer, event,
                                                           journal=conn))
        except Exception as exc:  # noqa: BLE001
            out["e6"] = {"success": None, "crash": repr(exc)}
        del layer
        conn.close()
        out["e6_rows"] = kcj._flags(path, "e6")
    except Exception as exc:  # noqa: BLE001
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    ctx.data["out"] = out
    flog(f"rs_journal_entry_points run: trace_id={ctx.trace_id} "
         f"{ {k: v for k, v in out.items() if k != 'trace'} }")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def _succeeded(result) -> bool:
    if "succeeded" in result:
        return bool(result["succeeded"]) and not result.get("failed")
    return result.get("success") is True


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = [("no_crash", "error" not in out,
            f"{out.get('error')} {out.get('trace', '')}")]
    sigs = out.get("signatures") or {}
    missing = sorted(k for k, ok in sigs.items() if not ok)
    res.append((
        "e0_every_identity_entry_point_accepts_the_journal", not missing and bool(sigs),
        f"sans parametre journal (mot-cle, None par defaut): {missing}. Un "
        f"runner qui tient le journal ne peut pas le leur donner: la cle "
        f"que le journal prouve continue y reste refusee.",
    ))
    labels = {"updated": "restore_updated_feature",
              "batch": "restore_batch",
              "plan_a": "execute_restore_plan (Mode A)",
              "plan_t": "execute_restore_plan (strict)"}
    for kind, label in labels.items():
        result, flag = out.get(f"e_{kind}") or ({}, None)
        res.append((
            f"e_{kind}_false_comes_back_with_the_journal",
            _succeeded(result) and flag is False,
            f"{label} avec le journal: {result} flag de C={flag!r}. La cle "
            f"n'a jamais change de mains, le journal le prouve: FAUX doit "
            f"revenir.",
        ))
        result, flag = out.get(f"n_{kind}") or ({}, None)
        text = json.dumps(result)
        res.append((
            f"n_{kind}_without_journal_refusal_is_kept",
            not _succeeded(result) and flag is True
            and "key_continuity_unproven" in text,
            f"{label} sans journal: {result} flag de C={flag!r}. Sans "
            f"journal rien ne distingue cette cle d'une cle redonnee: le "
            f"refus nomme doit rester.",
        ))
    res.append((
        "e5_undo_apply_one_uses_the_journal_it_is_given",
        (out.get("e5") or {}).get("success") is True
        and out.get("e5_flag") is True,
        f"{out.get('e5')} flag de C apres={out.get('e5_flag')!r}: l'etat "
        f"ecrit par la restauration (FAUX) est partage par deux lignes; le "
        f"journal donne a undo_apply_one prouve la cle.",
    ))
    res.append((
        "e6_restore_deleted_feature_accepts_the_journal",
        (out.get("e6") or {}).get("success") is True
        and any(fid == 2 for fid, _e, _f in (out.get("e6_rows") or [])),
        f"{out.get('e6')} lignes={out.get('e6_rows')}",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_journal_entry_points run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
