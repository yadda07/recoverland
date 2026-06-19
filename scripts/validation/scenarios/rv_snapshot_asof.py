"""Scenario runtime: ReView Snapshot "as-of T" completeness + dedup.

Doit etre execute dans la CONSOLE PYTHON DE QGIS (le moteur importe qgis.core
via le logger). Hors-QGIS l'import echoue : c'est attendu (charte: preuve par
logs runtime QGIS, pas de mocks CLI).

Lancement (console QGIS) ::

    import recoverland.scripts.validation.scenarios.rv_snapshot_asof as s
    s.run()

Ou bien ::

    exec(open(r"<plugin>/scripts/validation/scenarios/rv_snapshot_asof.py").read())

THESE validee : a une date T, le mode Snapshot affiche l'etat fidele =
reconstruction(entites tracees a T) FUSIONNEE avec etat courant(entites non
tracees), sans doublon ; les entites tracees creees apres T ou supprimees avant
T sont absentes.

Le verdict reel (PASS/FAIL par assertion) est imprime ET ecrit dans
recoverland_debug.log via flog, prefixe par le trace_id du scenario.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from recoverland.core.audit_backend import AuditEvent
from recoverland.core.logger import flog
from recoverland.core.serialization import compute_update_delta
from recoverland.core.temporal_snapshot_engine import (
    SnapshotResult,
    reconstruct_snapshot_at,
)

_DS_FP = "ds_rv_asof_test"


def _mk_event(
    event_id,
    operation_type,
    created_at,
    *,
    entity_fingerprint=None,
    feature_identity_json="{}",
    attributes_json="{}",
    geometry_wkb=b"\x00",
    new_geometry_wkb=None,
):
    """Build a minimal AuditEvent for the engine. Only engine-read fields matter."""
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="proj_test",
        datasource_fingerprint=_DS_FP,
        layer_id_snapshot="layer_test",
        layer_name_snapshot="couche_test",
        provider_type="memory",
        feature_identity_json=feature_identity_json,
        operation_type=operation_type,
        attributes_json=attributes_json,
        geometry_wkb=geometry_wkb,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json="{}",
        user_name="tester",
        session_id="sess",
        created_at=created_at,
        restored_from_event_id=None,
        entity_fingerprint=entity_fingerprint,
        new_geometry_wkb=new_geometry_wkb,
    )


def _insert_attrs(**kv):
    return json.dumps({"all_attributes": kv}, ensure_ascii=False)


def _update_attrs(**field_old_new):
    """Legacy LIST-form delta ``{"changed_only": {field: [old, new]}}``.

    Conserve pour couvrir la RETRO-COMPATIBILITE des journaux historiques
    (anterieurs au format de production dict). NE PAS utiliser pour simuler
    une ecriture de production : preferer ``_update_attrs_prod``.
    """
    return json.dumps({"changed_only": field_old_new}, ensure_ascii=False)


def _update_attrs_prod(**field_old_new):
    """PRODUCTION-form delta, genere par le VRAI ecrivain du journal.

    Chaque valeur est une paire ``(old, new)``. On passe par
    ``compute_update_delta`` afin que le test exerce exactement le format
    ecrit en production (dict ``{"old": .., "new": ..}``), et non une forme
    fabriquee a la main qui masquerait l'incoherence producteur/consommateur.
    """
    old_attrs = {field: pair[0] for field, pair in field_old_new.items()}
    new_attrs = {field: pair[1] for field, pair in field_old_new.items()}
    return compute_update_delta(old_attrs, new_attrs, list(field_old_new.keys()))


def _check(results, name, passed, detail, brutal=False):
    tag = "PASS" if passed else "FAIL"
    kind = "BRUTAL" if brutal else "ASSERT"
    line = f"{kind} {name}: {tag} -- {detail}"
    results.append((name, passed, brutal, detail))
    flog(f"[{results.trace_id}] rv_asof: {line}", "INFO" if passed else "ERROR")
    print(line)


class _Results(list):
    trace_id = ""


# ------------------------------------------------------------------ #
# Phase 1 : moteur de reconstruction (pur, sans couche QGIS)          #
# ------------------------------------------------------------------ #


def _run_engine_phase(results) -> SnapshotResult:
    """Reconstruct at T and assert as-of-T semantics + F-1 canonical key."""
    # T = 2026-03-01. Lignes de vie:
    #  A (pk:gid=1) INSERT t1 -> UPDATE t2  (present a T)
    #  B (pk:gid=2) INSERT t_after          (cree APRES T -> absent)
    #  C (pk:gid=3) INSERT t1 -> DELETE t2  (supprime avant T -> absent)
    #  D (fingerprint NULL, identity {"fid":77}) INSERT t1 (present, cle canonique)
    #  E (pk:gid=5) INSERT t1 -> UPDATE t2 FORME LISTE  (retro-compat journaux)
    cutoff = datetime(2026, 3, 1, tzinfo=timezone.utc)
    t_mid = datetime(2026, 2, 1, tzinfo=timezone.utc)  # entre t1 et t2
    t1 = "2026-01-10T08:00:00"
    t2 = "2026-02-15T08:00:00"
    t_after = "2026-04-20T08:00:00"

    events = [
        _mk_event(1, "INSERT", t1, entity_fingerprint="pk:gid=1",
                  feature_identity_json='{"fid":1,"pk_field":"gid","pk_value":1}',
                  attributes_json=_insert_attrs(gid=1, name="A0")),
        # UPDATE au FORMAT DE PRODUCTION (dict {"old","new"}) via le vrai ecrivain.
        _mk_event(2, "UPDATE", t2, entity_fingerprint="pk:gid=1",
                  feature_identity_json='{"fid":1,"pk_field":"gid","pk_value":1}',
                  attributes_json=_update_attrs_prod(name=("A0", "A1")),
                  new_geometry_wkb=b"\x01"),
        _mk_event(3, "INSERT", t_after, entity_fingerprint="pk:gid=2",
                  feature_identity_json='{"fid":2,"pk_field":"gid","pk_value":2}',
                  attributes_json=_insert_attrs(gid=2, name="B0")),
        _mk_event(4, "INSERT", t1, entity_fingerprint="pk:gid=3",
                  feature_identity_json='{"fid":3,"pk_field":"gid","pk_value":3}',
                  attributes_json=_insert_attrs(gid=3, name="C0")),
        _mk_event(5, "DELETE", t2, entity_fingerprint="pk:gid=3",
                  feature_identity_json='{"fid":3,"pk_field":"gid","pk_value":3}',
                  attributes_json=_insert_attrs(gid=3, name="C0")),
        # D : entity_fingerprint absent -> teste la cle de fallback (F-1).
        _mk_event(6, "INSERT", t1, entity_fingerprint=None,
                  feature_identity_json='{"fid":77}',
                  attributes_json=_insert_attrs(fid=77, name="D0")),
        # E : UPDATE en FORME LISTE [old, new] -> retro-compatibilite lecture.
        _mk_event(7, "INSERT", t1, entity_fingerprint="pk:gid=5",
                  feature_identity_json='{"fid":5,"pk_field":"gid","pk_value":5}',
                  attributes_json=_insert_attrs(gid=5, name="E0")),
        _mk_event(8, "UPDATE", t2, entity_fingerprint="pk:gid=5",
                  feature_identity_json='{"fid":5,"pk_field":"gid","pk_value":5}',
                  attributes_json=_update_attrs(name=["E0", "E1"])),
    ]

    result = reconstruct_snapshot_at({_DS_FP: events}, cutoff,
                                     trace_id=results.trace_id)
    ds_feats = result.features.get(_DS_FP, {})
    keys = set(ds_feats.keys())

    _check(results, "A2_modified_present",
           "pk:gid=1" in keys and ds_feats["pk:gid=1"].last_op == "UPDATE",
           f"pk:gid=1 present last_op={ds_feats.get('pk:gid=1') and ds_feats['pk:gid=1'].last_op}")

    # A2b (BRUTAL) : la valeur reconstruite a T doit etre la valeur NEW scalaire
    # "A1", jamais le dict {"old","new"} (symptome de l'incoherence de format).
    a_feat = ds_feats.get("pk:gid=1")
    a_attrs = json.loads(a_feat.attrs_json) if a_feat and a_feat.attrs_json else {}
    _check(results, "A2b_attr_value_new_scalar",
           a_attrs.get("name") == "A1" and not isinstance(a_attrs.get("name"), dict),
           f"name reconstruit a T={a_attrs.get('name')!r} (attendu 'A1' scalaire)",
           brutal=True)

    # A2c (BRUTAL) : a un cutoff ANTERIEUR a l'UPDATE, on lit la valeur AVANT.
    mid = reconstruct_snapshot_at({_DS_FP: events}, t_mid,
                                  trace_id=results.trace_id)
    mid_feat = mid.features.get(_DS_FP, {}).get("pk:gid=1")
    mid_attrs = json.loads(mid_feat.attrs_json) if mid_feat and mid_feat.attrs_json else {}
    _check(results, "A2c_attr_value_old_before_update",
           mid_attrs.get("name") == "A0",
           f"name a T_mid={mid_attrs.get('name')!r} (attendu 'A0' avant UPDATE)",
           brutal=True)

    # A2d : retro-compat de la FORME LISTE [old, new] (journaux historiques).
    e_feat = ds_feats.get("pk:gid=5")
    e_attrs = json.loads(e_feat.attrs_json) if e_feat and e_feat.attrs_json else {}
    _check(results, "A2d_legacy_list_form_new",
           e_attrs.get("name") == "E1" and not isinstance(e_attrs.get("name"), (list, dict)),
           f"name forme-liste reconstruit={e_attrs.get('name')!r} (attendu 'E1' scalaire)")

    _check(results, "A3_created_after_T_absent",
           "pk:gid=2" not in keys and result.n_unknown >= 1,
           f"pk:gid=2 absent n_unknown={result.n_unknown}", brutal=True)

    _check(results, "A4_deleted_before_T_absent",
           "pk:gid=3" not in keys and result.n_absent >= 1,
           f"pk:gid=3 absent n_absent={result.n_absent}", brutal=True)

    # F-1 : la cle de fallback doit etre canonique "fid:77", pas le JSON brut.
    raw_key = 'fid:{"fid":77}'
    _check(results, "A1_null_fp_canonical_key",
           "fid:77" in keys and raw_key not in keys,
           f"keys_fid={[k for k in keys if k.startswith('fid')]}", brutal=True)

    return result


# ------------------------------------------------------------------ #
# Phase 2 : fusion baseline (necessite une couche memoire QGIS)       #
# ------------------------------------------------------------------ #


def _run_merge_phase(results, engine_result) -> None:
    """Build a memory layer + a SnapshotResult, run merge_untracked_base, assert."""
    try:
        from qgis.core import (
            QgsFeature,
            QgsGeometry,
            QgsProject,
            QgsVectorLayer,
        )
    except ImportError as exc:
        _check(results, "MERGE_env", False,
               f"QGIS indisponible: {exc!r} (phase merge sautee)")
        return

    from recoverland.widgets.snapshot_rebuild_worker import (
        _feature_entity_fp,
        _resolve_pk_field,
        merge_untracked_base,
    )
    from recoverland.core.temporal_snapshot_engine import SnapshotFeature

    # Champs declares via l'URI memoire: version-proof (pas de divergence
    # QVariant 3.44/Qt5 vs QMetaType 4.0/Qt6).
    lyr = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=gid:integer&field=name:string",
        "__rv_asof_src", "memory",
    )
    dp = lyr.dataProvider()

    def _feat(gid, name, x, y):
        f = QgsFeature(lyr.fields())
        f.setAttribute("gid", gid)
        f.setAttribute("name", name)
        f.setGeometry(QgsGeometry.fromWkt(f"POINT({x} {y})"))
        return f

    # Roles: A tracee+presente a T (reconstruite); B tracee creee-apres-T;
    # C tracee supprimee-avant-T (mais encore live); U non tracee (jamais editee).
    dp.addFeatures([_feat(1, "A_now", 1, 1), _feat(2, "B_now", 2, 2),
                    _feat(3, "C_now", 3, 3), _feat(10, "U_now", 10, 10)])
    lyr.updateExtents()
    QgsProject.instance().addMapLayer(lyr, False)

    try:
        # Cles calculees par la VRAIE fonction d'identite (independante de la
        # presence d'un PK: couche memoire -> fid:<id>). Le scenario teste la
        # logique de dedup, pas le format de cle.
        pk_field = _resolve_pk_field(lyr)
        by_name = {f["name"]: f for f in lyr.getFeatures()}
        key = {n: _feature_entity_fp(f, pk_field) for n, f in by_name.items()}
        flog(
            f"[{results.trace_id}] rv_asof: merge_keys pk_field={pk_field} "
            f"A={key['A_now']} B={key['B_now']} C={key['C_now']} U={key['U_now']}",
            "INFO",
        )

        layer_infos = [{
            "fingerprint": _DS_FP,
            "layer_id": lyr.id(),
            "layer_name": "couche_test",
            "storage_crs": "EPSG:4326",
        }]
        # tracked_fps : toutes les entites ayant >=1 evenement (A, B, C).
        tracked_fps = {_DS_FP: {key["A_now"], key["B_now"], key["C_now"]}}
        # features reconstruites a T : seulement A (present).
        recon_feat = SnapshotFeature(
            entity_fp=key["A_now"], geom_wkb=b"", attrs_json='{"gid":1,"name":"A1"}',
            crs_authid="EPSG:4326", last_event_id=2, last_op="UPDATE",
            last_created_at="2026-02-15T08:00:00",
        )
        base_result = SnapshotResult(
            features={_DS_FP: {key["A_now"]: recon_feat}},
            cutoff_dt=datetime(2026, 3, 1, tzinfo=timezone.utc),
            n_fps=1, n_entities=1, n_absent=1, n_unknown=1, elapsed_ms=0,
            trace_id=results.trace_id, all_event_markers=(), tracked_fps=tracked_fps,
        )

        merged = merge_untracked_base(base_result, layer_infos, None,
                                      trace_id=results.trace_id)
        mfeats = merged.features.get(_DS_FP, {})
        mkeys = set(mfeats.keys())

        _check(results, "A5_untracked_added_unchanged",
               key["U_now"] in mkeys and mfeats[key["U_now"]].last_op == "UNCHANGED",
               f"U={key['U_now']} present last_op="
               f"{mfeats.get(key['U_now']) and mfeats[key['U_now']].last_op}")

        _check(results, "A6_reconstructed_not_duplicated",
               key["A_now"] in mkeys and mfeats[key["A_now"]].last_op == "UPDATE",
               f"A={key['A_now']} last_op={mfeats.get(key['A_now']) and mfeats[key['A_now']].last_op} "
               f"(reconstruit, non ecrase par le live)", brutal=True)

        _check(results, "A7_tracked_absent_not_merged",
               key["B_now"] not in mkeys and key["C_now"] not in mkeys,
               f"B/C (tracees mais absentes a T) non re-ajoutees par le merge; "
               f"mkeys={sorted(mkeys)}", brutal=True)

        # A8 (RL-E1-03) : cutoff ANTERIEUR a la baseline T0 (debut du suivi)
        # -> les entites non suivies ne sont PAS presumees presentes, et la
        # couche est signalee dans baseline_missing_layers.
        pre_baseline_result = SnapshotResult(
            features={_DS_FP: {key["A_now"]: recon_feat}},
            cutoff_dt=datetime(2025, 12, 1, tzinfo=timezone.utc),  # avant T0
            n_fps=1, n_entities=1, n_absent=0, n_unknown=0, elapsed_ms=0,
            trace_id=results.trace_id, all_event_markers=(),
            tracked_fps=tracked_fps,
            layer_baseline={_DS_FP: "2026-01-10T08:00:00"},  # T0 > cutoff
        )
        merged_pre = merge_untracked_base(pre_baseline_result, layer_infos, None,
                                          trace_id=results.trace_id)
        pre_keys = set(merged_pre.features.get(_DS_FP, {}).keys())
        _check(results, "A8_before_baseline_untracked_not_present",
               key["U_now"] not in pre_keys
               and "couche_test" in merged_pre.baseline_missing_layers,
               f"U non presume present avant T0; "
               f"baseline_missing={merged_pre.baseline_missing_layers}",
               brutal=True)

        # A9 (RL-E1-04) : couche memoire sans PK -> identity=fid-only WEAK
        # -> la couche doit etre signalee dans fid_only_layers pour avertir
        # l'utilisateur du risque de renumerotation FID.
        _check(results, "A9_fid_only_layer_warned",
               "couche_test" in merged.fid_only_layers,
               f"fid_only_layers={merged.fid_only_layers} "
               f"(attendu 'couche_test' car memory provider sans PK)",
               brutal=True)
    finally:
        QgsProject.instance().removeMapLayer(lyr.id())


# ------------------------------------------------------------------ #
# Entree                                                              #
# ------------------------------------------------------------------ #


def run() -> dict:
    """Run the full scenario. Returns a verdict dict and prints a SYNTHESE."""
    results = _Results()
    results.trace_id = uuid.uuid4().hex[:8]
    flog(f"[{results.trace_id}] rv_asof: scenario_start", "INFO")
    print(f"=== rv_snapshot_asof trace_id={results.trace_id} ===")

    engine_result = _run_engine_phase(results)
    _run_merge_phase(results, engine_result)

    n_total = len(results)
    n_pass = sum(1 for _, p, _, _ in results if p)
    n_fail = n_total - n_pass
    n_brutal = sum(1 for _, _, b, _ in results if b)
    n_brutal_pass = sum(1 for _, p, b, _ in results if b and p)
    verdict = "PASS" if n_fail == 0 else "FAIL"
    synthese = (
        f"SYNTHESE: {verdict} -- {n_pass}/{n_total} assertions, "
        f"dont {n_brutal_pass}/{n_brutal} antitheses brutales."
    )
    flog(f"[{results.trace_id}] rv_asof: {synthese}", "INFO" if verdict == "PASS" else "ERROR")
    print(synthese)
    return {
        "verdict": verdict,
        "trace_id": results.trace_id,
        "n_pass": n_pass,
        "n_fail": n_fail,
        "n_total": n_total,
        "failed": [name for name, p, _, _ in results if not p],
    }


if __name__ == "__main__":
    run()
