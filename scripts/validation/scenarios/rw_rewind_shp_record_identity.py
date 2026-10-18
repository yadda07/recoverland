"""rw_rewind_shp_record_identity - RecoverLand validation runtime.

Invariant: I-REWIND-01 (apres un rewind a la date T, le fichier contient
EXACTEMENT l'etat qu'il avait a T, ou le rewind est refuse par son nom,
fichier intact ; jamais un etat faux annonce reussi) sur un shapefile, ou
un FID nomme un ENREGISTREMENT et non une entite.

Le mecanisme
------------
Un shapefile renumerote ses enregistrements quand se termine une session
d'edition qui en a supprime un. Le rewind doit donc prouver que deux
lignes du journal portant le meme ``fid:N`` sont la meme entite, avec
des valeurs que le fournisseur a lui-meme rendues : l'instantane d'un
INSERT (relu apres le commit), celui d'un DELETE et le cote ANCIEN d'un
UPDATE (lus avant). Le cote NOUVEAU d'un UPDATE vient du tampon
d'edition : le fournisseur arrondit ensuite un reel a la precision du
champ (1234.56789 stocke 1234.57) et coupe une chaine a 254 caracteres.

  A. Modification hors capture (review external-edit-lifetime). Une
     entite creee apres T, modifiee par un autre programme, supprimee :
     l'INSERT et le DELETE ne portent pas le meme etat. La paire etait
     gardee : le rewind re-inserait l'entite puis ne pouvait pas la
     supprimer (l'executeur epargne ce qu'il vient de re-inserer),
     entite ressuscitee, rewind annonce reussi. Attendu : annulee, ou
     refus nomme ; jamais les deux moities.
  B. Precision et troncature (review shp-split-precision). Une entite
     modifiee (reel N(12,2) ou chaine de plus de 254 caracteres) puis
     supprimee etait lue comme DEUX entites (cote NOUVEAU contre
     instantane du DELETE) : cle coupee en deux, entite creee ressuscitee
     ou valeur d'apres T laissee. Attendu : le rewind retombe exactement
     sur l'etat a T.
  C. Renumerotation reelle (garde conservee). C003 modifiee, C001
     supprimee (le shapefile renumerote), puis suppression de l'entite
     qui a pris le numero de C003 : le rewind ne doit jamais ecrire les
     anciennes valeurs de C003 sur l'entite re-inseree. Attendu : etat a
     T exact, ou refus nomme.
  D. Marches aleatoires epinglees (36 pas, shapefile). Graine 113 (la
     review) : une duree de vie etalee sur trois FID revenait en silence.
     Graine 109 (exploration de ce tour, deja fausse avant) : la
     re-application d'une edition de C003 anterieure a T porte le numero
     d'enregistrement de son temps ; C000, re-inseree sous ce numero apres
     une renumerotation, recevait la valeur de C003. Attendu : aucun etat
     faux silencieux (refus nomme ou echec annonce tolere, comme dans
     rw_state_at_t_toggles).
  E. Un enregistrement supprime deux fois sans re-insertion entre les
     deux (C001 modifiee puis supprimee, puis C002 qui a pris son
     numero) : ce sont deux entites. Sous une seule cle, le rewind
     re-inserait les deux et l'executeur, suivant la cle vers la derniere
     re-inseree, pouvait ecrire les anciennes valeurs de C001 sur C002.
     Attendu : etat a T exact.
  F. Une duree de vie sur trois enregistrements (review, A1) : creee sur
     le 4, modifiee sur le 3, supprimee sur le 2 apres deux
     renumerotations. L'appariement ne relie que deux FID : la
     re-insertion (compensation du DELETE) et la suppression
     (compensation de l'INSERT) restaient dans deux seaux. Attendu : etat
     a T exact, ou refus nomme ; jamais l'entite ressuscitee.

Ce que le scenario execute
--------------------------
La vraie pile de rw_state_at_t_property / rw_state_at_t_toggles (capture
reelle, fenetre du journal, dedup, StrictRestoreRunner en un passage) sur
un shapefile, une couche neuve par cas ; un GeoPackage en controle pour A.

Echec concret pour l'utilisateur (avant correctif)
--------------------------------------------------
A, B : une entite creee puis supprimee apres T est de retour apres le
rewind, ou une entite supprimee revient avec sa valeur d'apres T, et le
rapport annonce le succes ou accuse une "entite absente".

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "rw_rewind_shp_record_identity"
INVARIANT = "I-REWIND-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_LONG = ("Chemin rural numero quarante deux dit des Tilleuls " * 6).strip()
_SURF = 1234.56789
# Shapefile walks of rw_state_at_t_toggles (36 steps) that ended on a wrong
# state reported without error, and what the user saw.
_WALK_SEEDS = {
    113: "la marche de la review ressuscitait C002 sans erreur (duree de vie "
         "sur trois FID).",
    109: "la re-application d'une edition de C003 d'avant T, portant le numero "
         "d'enregistrement de son temps, ecrivait la valeur de C003 sur C000 "
         "(re-inseree sous ce numero), sans erreur.",
}


def _make_shp_surf(tmpdir, name):
    """The shapefile of rw_state_at_t_toggles plus a real N(12,2) field."""
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    from scripts.validation.scenarios import rw_state_at_t_property as prop
    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(12)&field=nom:string(254)"
        "&field=val:integer&field=surf:double(12,2)", "m", "memory")
    mem.startEditing()
    seed = {}
    for i in range(4):
        code = f"C{i:03d}"
        f = QgsFeature(mem.fields())
        f.setAttributes([code, prop._WORDS[i], i * 10, 100.25 * i])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(10.0 * i, 5.0 * i)))
        mem.addFeature(f)
        seed[code] = (prop._WORDS[i], i * 10, 10.0 * i, 5.0 * i)
    mem.commitChanges()
    path = os.path.join(tmpdir, "pts.shp")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "ESRI Shapefile"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    lyr = QgsVectorLayer(path, name, "ogr")
    assert lyr.isValid(), path
    return path, lyr, seed


def _run_for(ctx, seed, provider, surf=False):
    from scripts.validation.scenarios import rw_state_at_t_toggles as tog
    if tog._ToggleRun is None:
        tog._ToggleRun = tog._toggle_run_class()
    original = tog._make_shp
    if surf:
        tog._make_shp = _make_shp_surf
    try:
        r = tog._new_run(ctx, seed, ctx.data["tmpdir"], provider)
    finally:
        tog._make_shp = original
    r.provider = provider
    r.allow_refusal = True
    r.refusals, r.loud = [], []
    return r


def _surfs(r):
    return {f["code"]: f["surf"] for f in r.layer.getFeatures()}


def _rewind(r, idx):
    """The harness rewind, measured: plan, refusals, report, file."""
    import recoverland.restore_runner as rr
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core import rewind_dedup
    from scripts.validation.plugin_harness import pump
    from scripts.validation.scenarios.rw_state_at_t_property import (
        _file_state, _model_rows)
    refusal_reason = getattr(rewind_dedup, "refusal_reason", lambda e: "")
    stamp, expected_model, label = r.history[idx]
    before = _file_state(r.path)
    r._drain()
    conn = r.jm.create_read_connection()
    try:
        events = fetch_events_after_cutoff(
            conn, r.fp, RestoreCutoff(CutoffType.BY_DATE, stamp, inclusive=True),
            trace_id=r.ctx.trace_id)
    finally:
        conn.close()
    active, _stats = collapse_rewind_events_with_stats(events)
    res = None
    if active:
        done = {}
        runner = rr.StrictRestoreRunner(
            active, lambda ev: r.layer if ev.datasource_fingerprint == r.fp else None,
            RestoreCutoff(CutoffType.BY_DATE, stamp, inclusive=True),
            write_queue=r.wq, tracker=r.tracker, trace_id=r.ctx.trace_id)
        runner.finished.connect(lambda x: done.setdefault("r", x))
        runner.start()
        deadline = time.monotonic() + 30
        while "r" not in done and time.monotonic() < deadline:
            pump(0.02)
        res = done.get("r")
        r._drain()
    after = _file_state(r.path)
    errors = [str(x) for x in (getattr(res, "errors", None) or [])]
    return {
        "label": label,
        "plan": [(e.event_id, e.operation_type,
                  (getattr(e, "commit_id", None) or "user").split(":")[-1].split("@")[0])
                 for e in active],
        "refusals": [refusal_reason(e)[:300] for e in active if refusal_reason(e)],
        "errors": errors[:4],
        "total_fail": getattr(res, "total_fail", None),
        "before": before, "after": after,
        "expected": _model_rows(dict(expected_model)),
        "log": r.log[-6:],
    }


def _verdict(out):
    """landed / refused / WRONG (anything else)."""
    if out.get("crash"):
        return "WRONG"
    if (out["after"] == out["expected"] and not out["refusals"]
            and not out["errors"] and not out.get("total_fail")):
        return "landed"
    if (out["refusals"] and out["after"] == out["before"]
            and any("unrebuildable_state" in e for e in out["errors"])):
        return "refused"
    return "WRONG"


def _external_set_val(r, code, value):
    """A write the capture does not see: the provider, no edit buffer."""
    fid = r._fid_of(code)
    idx = r.layer.fields().indexFromName("val")
    ok = r.layer.dataProvider().changeAttributeValues({fid: {idx: value}})
    r.layer.reload()
    nom, _val, x, y = r.model[code]
    r.model[code] = (nom, value, x, y)
    r.log.append(f"EXTERNAL val={value} on {code} fid={fid} ok={ok}")
    r._new_user_eids()
    r._gap("external edit")


def _set(r, code, field, value):
    fid = r._fid_of(code)
    idx = r.layer.fields().indexFromName(field)
    if not r._commit(lambda: r.layer.changeAttributeValue(fid, idx, value)):
        return r._fail("commit", op="UPDATE", code=code)
    eids = r._new_user_eids()
    if field == "nom":
        stored = next((f["nom"] for f in r.layer.getFeatures()
                       if f["code"] == code), value)
        nom, val, x, y = r.model[code]
        r.model[code] = (stored, val, x, y)
    r.log.append(f"SET {field} {code} eids={eids}")
    r._gap(f"SET {field} {code}")


def _case_external(r):
    base = len(r.history) - 1
    r.act_insert()
    code = r.edits[-1]["code"]
    _external_set_val(r, code, 777)
    r.act_delete(code)
    return _rewind(r, base)


def _case_created(field, value):
    def fn(r):
        base = len(r.history) - 1
        r.act_insert()
        code = r.edits[-1]["code"]
        _set(r, code, field, value)
        r.act_delete(code)
        return _rewind(r, base)
    return fn


def _case_existing(field, value):
    def fn(r):
        base = len(r.history) - 1
        seed_surf = _surfs(r).get("C001")
        _set(r, "C001", field, value)
        r.act_delete("C001")
        out = _rewind(r, base)
        out["c001_surf"] = (seed_surf, _surfs(r).get("C001"))
        return out
    return fn


def _case_renumbered(r):
    r.act_insert()                       # N at the last record number
    base = len(r.history) - 1
    newcode = r.edits[-1]["code"]
    r.act_update("C003", "nom")          # attributes only, record 3
    r.act_delete("C001")                 # the shapefile renumbers
    r.act_delete(newcode)                # now on C003's former number
    return _rewind(r, base)


def _case_three_records(r):
    base = len(r.history) - 1
    r.act_insert()                       # N on record 4
    code = r.edits[-1]["code"]
    r.act_delete("C000")                 # renumbered: N on record 3
    r.act_update(code, "val")            # edited on record 3
    r.act_delete("C001")                 # renumbered: N on record 2
    r.act_delete(code)                   # deleted on record 2
    return _rewind(r, base)


def _case_deleted_twice(r):
    base = len(r.history) - 1
    r.act_update("C001", "val")          # attributes only, record 1
    r.act_delete("C001")                 # record 1; the shapefile renumbers
    r.act_delete("C002")                 # C002 now holds record 1
    return _rewind(r, base)


_CASES = (
    ("A_external_edit_lifetime", "shp", False, _case_external),
    ("A_control_gpkg", "gpkg", False, _case_external),
    ("B_created_precision", "shp", True, _case_created("surf", _SURF)),
    ("B_existing_precision", "shp", True, _case_existing("surf", _SURF)),
    ("B_created_truncated", "shp", True, _case_created("nom", _LONG)),
    ("B_existing_truncated", "shp", True, _case_existing("nom", _LONG)),
    ("C_renumbered_update_then_delete", "shp", False, _case_renumbered),
    ("E_record_deleted_twice", "shp", False, _case_deleted_twice),
    ("F_lifetime_over_three_records", "shp", False, _case_three_records),
)


def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_shpid_")
    flog(f"rw_rewind_shp_record_identity setup: trace_id={ctx.trace_id}")


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog
    from scripts.validation.scenarios import rw_state_at_t_toggles as tog
    tog._ToggleRun = tog._toggle_run_class()
    res = {}
    for n, (name, provider, surf, fn) in enumerate(_CASES):
        r = None
        try:
            r = _run_for(ctx, 8600 + n, provider, surf)
            out = fn(r)
            out["harness_failure"] = r.failure
        except Exception:  # noqa: BLE001 - reported by the assertions
            import traceback
            out = {"crash": traceback.format_exc()[-1500:]}
        finally:
            if r is not None:
                r.close()
        out["verdict"] = _verdict(out) if "after" in out else "WRONG"
        res[name] = out
        flog(f"rw_rewind_shp_record_identity case={name} trace_id={ctx.trace_id} "
             f"{json.dumps(out, default=str)[:2500]}")
    # D. Shapefile walks that ended on a silent wrong state, pinned.
    for seed in _WALK_SEEDS:
        walk = {}
        r = None
        try:
            r = _run_for(ctx, seed, "shp")
            for _ in range(36):
                if r.failure or r.stopped:
                    break
                r.step()
            walk = dict(failure=r.failure, checked=r.checked, refused=r.refused,
                        refusals=list(r.refusals)[:3], loud=list(r.loud)[:3],
                        log=r.log[-10:])
        except Exception:  # noqa: BLE001
            import traceback
            walk = {"crash": traceback.format_exc()[-1500:]}
        finally:
            if r is not None:
                r.close()
        res[f"D_walk_seed_{seed}"] = walk
        flog(f"rw_rewind_shp_record_identity case=D_walk_seed_{seed} "
             f"trace_id={ctx.trace_id} {json.dumps(walk, default=str)[:2500]}")
    ctx.data["res"] = res
    QgsProject.instance().clear()
    shutil.rmtree(ctx.data["tmpdir"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    res = ctx.data.get("res") or {}
    out = []

    def show(name):
        return json.dumps(res.get(name) or {}, default=str)[:2800]

    a = res.get("A_external_edit_lifetime") or {}
    out.append((
        "A_external_edit_lifetime_cancelled_or_refused_never_resurrected",
        a.get("verdict") in ("landed", "refused"),
        f"{show('A_external_edit_lifetime')} : entite creee apres T, modifiee "
        f"hors capture, supprimee ; le rewind avant sa creation la faisait "
        f"revenir (re-insertion puis suppression impossible), succes annonce.",
    ))
    out.append((
        "A_control_gpkg_lands",
        (res.get("A_control_gpkg") or {}).get("verdict") == "landed",
        f"{show('A_control_gpkg')} : sur GeoPackage la cle est la cle "
        f"primaire, la paire s'annule.",
    ))
    for name in ("B_created_precision", "B_created_truncated"):
        out.append((
            f"{name}_lands",
            (res.get(name) or {}).get("verdict") == "landed",
            f"{show(name)} : l'entite creee, modifiee (valeur arrondie ou "
            f"coupee par le shapefile) puis supprimee apres T doit rester "
            f"absente ; lue comme deux entites, elle revenait.",
        ))
    for name, check in (("B_existing_precision", lambda s: s[1] == s[0]),
                        ("B_existing_truncated", lambda s: True)):
        r = res.get(name) or {}
        out.append((
            f"{name}_lands",
            r.get("verdict") == "landed" and check(r.get("c001_surf") or (0, 1)),
            f"{show(name)} : C001 modifiee (valeur arrondie ou coupee) puis "
            f"supprimee doit revenir a son etat a T ; coupee en deux cles, "
            f"elle revenait avec la valeur d'apres T.",
        ))
    c = res.get("C_renumbered_update_then_delete") or {}
    out.append((
        "C_renumbered_record_never_receives_another_feature_values",
        c.get("verdict") in ("landed", "refused"),
        f"{show('C_renumbered_update_then_delete')} : apres renumerotation, "
        f"les anciennes valeurs de C003 ne doivent jamais etre ecrites sur "
        f"l'entite re-inseree qui a pris son numero.",
    ))
    out.append((
        "E_record_deleted_twice_lands",
        (res.get("E_record_deleted_twice") or {}).get("verdict") == "landed",
        f"{show('E_record_deleted_twice')} : C001 modifiee puis supprimee, "
        f"puis C002, qui a pris son numero ; deux suppressions sans "
        f"re-insertion entre elles sont deux entites. Sous une seule cle, "
        f"le rewind re-inserait les deux et ecrivait les anciennes valeurs "
        f"de C001 sur C002.",
    ))
    out.append((
        "F_lifetime_over_three_records_never_resurrected",
        (res.get("F_lifetime_over_three_records") or {}).get("verdict")
        in ("landed", "refused"),
        f"{show('F_lifetime_over_three_records')} : une entite creee apres T "
        f"sur l'enregistrement 4, modifiee sur le 3 et supprimee sur le 2 "
        f"(deux renumerotations) ; ni appariee ni distinguee, le plan la "
        f"re-inserait puis ne pouvait plus la supprimer.",
    ))
    for seed, what in _WALK_SEEDS.items():
        d = res.get(f"D_walk_seed_{seed}") or {}
        out.append((
            f"D_walk_seed_{seed}_no_silent_wrong_state",
            not d.get("crash") and d.get("failure") is None and d.get("checked", 0) >= 3,
            f"{show(f'D_walk_seed_{seed}')} : {what}",
        ))
    out.append(assert_log_contains(
        ctx.records, rf"rw_rewind_shp_record_identity.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
