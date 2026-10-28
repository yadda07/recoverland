# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_datetime_instant_proof - RecoverLand validation runtime.

Invariant: I-3 (la preuve d'identite compare des VALEURS, pas des chiffres)
et fail-loud (aucun "deja fait" sur une valeur alteree).
Constat resolu: item D, `restore_service._qgis_vals_equal` comparait une
date-heure vivante au texte du journal par TROIS rendus (chiffres naifs,
chiffres UTC, chiffres locaux) et acceptait le premier qui collait.

Le mecanisme
------------
Le journal ecrit une date-heure en texte. Selon la generation de
l'evenement, ces chiffres designent l'heure UTC (schema 6) ou l'heure
murale locale (schema <= 5); v7 ecrit un suffixe explicite. Le
comparateur des preuves ignorait la generation: il acceptait le texte des
qu'il coincidait avec les chiffres NAIFS de la valeur vivante, ou ses
chiffres UTC, ou ses chiffres locaux. Une valeur dont le fuseau differe
(une date-heure PostGIS a decalage, une valeur restauree decalee par un
ancien bug) a les MEMES chiffres naifs qu'un autre instant: la preuve
passait sur un instant faux de plusieurs heures. `serialization.
journal_datetime_matches` lit le texte une seule fois, selon la
generation de l'evenement, et compare des instants a la milliseconde.

Ce que l'utilisateur voit
-------------------------
Un constat horodate 08:00 (+05:00), soit 03:00 UTC. Le journal (schema 6)
dit qu'avant l'edition il valait 08:00 UTC. Le rewind annonce "deja a
l'etat d'avant l'edition, rien a faire": la valeur reste fausse de cinq
heures, et la trace de restauration l'enterine. Dans l'autre sens, la
restauration d'un constat supprime repond "Already exists" parce qu'un
AUTRE constat a les memes chiffres a une autre heure: le constat
supprime ne revient jamais.

Decor: couche memoire (le seul fournisseur qui garde tel quel le fuseau
d'une date-heure), chemin REWIND (`_apply_via_buffer`) et restauration
d'un DELETE (`restore_deleted_feature`), qui ne dependent pas du fuseau
de la machine.

Cas
---
    D1  rewind, la valeur vivante a les chiffres de l'etat OLD dans un
        autre fuseau: pas de saut "already_at_old", refus nomme
    D2  rewind, la valeur vivante a les chiffres de l'etat NEW dans un
        autre fuseau: pas d'ecriture, refus nomme
    D3  temoin: la valeur vivante EST l'instant NEW, exprime a +05:00:
        prouvee, l'etat OLD (08:00 UTC) est ecrit
    D4  restauration d'un DELETE (schema 6, 10:00 UTC), un autre constat
        a 10:00 +05:00 au meme endroit: le constat supprime revient

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "rs_datetime_instant_proof"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_URI = ("Point?crs=EPSG:4326&field=label:string(20)&field=dtm:datetime")


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _dt(hour, offset_h):
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    factory = getattr(QTimeZone, "fromSecondsAheadOfUtc", None)
    zone = (factory(offset_h * 3600) if factory is not None
            else QTimeZone(offset_h * 3600))
    return QDateTime(QDate(2026, 1, 15), QTime(hour, 0), zone)


def _layer(rows):
    """Couche memoire [(label, QDateTime, x)]; renvoie (couche, {label: fid})."""
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY
    layer = QgsVectorLayer(_URI, "constats", "memory")
    feats = []
    for label, value, x in rows:
        feat = QgsFeature(layer.fields())
        feat.setAttribute("label", label)
        feat.setAttribute("dtm", value)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, 1.0)))
        feats.append(feat)
    layer.dataProvider().addFeatures(feats)
    return layer, {f["label"]: f.id() for f in layer.getFeatures()}


def _instants(layer):
    """{label: [instants UTC ISO]}, ce que la couche tient vraiment."""
    out = {}
    for feat in layer.getFeatures():
        value = feat["dtm"]
        text = (value.toUTC().toString("yyyy-MM-ddTHH:mm:ss")
                if hasattr(value, "toUTC") else repr(value))
        out.setdefault(feat["label"], []).append(text)
    return {k: sorted(v) for k, v in out.items()}


def _update(layer, fid, old, new, eid):
    from recoverland.core.serialization import serialize_field_schema
    return _fx().audit_event(
        event_id=eid, operation_type="UPDATE",
        feature_identity_json=json.dumps({"fid": fid}),
        entity_fingerprint=f"fid:{fid}",
        field_schema_json=serialize_field_schema(layer.fields()),
        attributes_json=json.dumps({"changed_only": {
            "dtm": {"old": old, "new": new}}}),
        event_schema_version=6)


def _rewind(layer, event, tid):
    from recoverland.core.restore_executor import _apply_via_buffer
    layer.startEditing()
    try:
        result = dict(_apply_via_buffer(layer, "UPDATE", event, {},
                                        trace_id=tid))
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        result = {"success": None, "crash": repr(exc)}
    if not layer.commitChanges():
        layer.rollBack()
    return result


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"rs_datetime_instant_proof setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import restore_deleted_feature
    from recoverland.core.serialization import (
        serialize_field_schema, build_full_snapshot,
    )
    d = ctx.data
    tid = ctx.trace_id
    res = {}
    flog(f"rs_datetime_instant_proof run start: trace_id={tid}", "INFO")
    try:
        # D1: 08:00 +05:00 is 03:00 UTC; OLD is 08:00 UTC (schema 6).
        layer, fids = _layer([("constat", _dt(8, 5), 1.0),
                              ("autre", _dt(12, 0), 2.0)])
        d["d1_before"] = _instants(layer)
        res["d1"] = _rewind(layer, _update(
            layer, fids["constat"], "2026-01-15T08:00:00",
            "2026-01-15T10:00:00", 901), tid)
        d["d1_after"] = _instants(layer)

        # D2: 10:00 +05:00 is 05:00 UTC; NEW is 10:00 UTC.
        layer, fids = _layer([("constat", _dt(10, 5), 1.0),
                              ("autre", _dt(12, 0), 2.0)])
        d["d2_before"] = _instants(layer)
        res["d2"] = _rewind(layer, _update(
            layer, fids["constat"], "2026-01-15T08:00:00",
            "2026-01-15T10:00:00", 902), tid)
        d["d2_after"] = _instants(layer)

        # D3: 15:00 +05:00 IS 10:00 UTC, the NEW instant.
        layer, fids = _layer([("constat", _dt(15, 5), 1.0),
                              ("autre", _dt(12, 0), 2.0)])
        res["d3"] = _rewind(layer, _update(
            layer, fids["constat"], "2026-01-15T08:00:00",
            "2026-01-15T10:00:00", 903), tid)
        d["d3_after"] = _instants(layer)

        # D4: the deleted constat was 10:00 UTC; another one sits at the
        # same place at 10:00 +05:00 (05:00 UTC).
        layer, _fids = _layer([("constat", _dt(10, 5), 1.0)])
        event = _fx().audit_event(
            event_id=904, operation_type="DELETE",
            feature_identity_json=json.dumps({"fid": 42}),
            entity_fingerprint="fid:42",
            field_schema_json=serialize_field_schema(layer.fields()),
            attributes_json=build_full_snapshot(
                {"label": "constat", "dtm": "2026-01-15T10:00:00"}),
            geometry_wkb=_fx().wkb_wkt("Point (1 1)"),
            event_schema_version=6)
        try:
            res["d4"] = dict(restore_deleted_feature(layer, event))
        except Exception as exc:  # noqa: BLE001
            res["d4"] = {"success": None, "crash": repr(exc)}
        d["d4_after"] = _instants(layer)
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    d["res"] = res
    flog(f"rs_datetime_instant_proof run end: trace_id={tid} res={res} "
         f"crash={d.get('crash')}", "INFO")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    res = d.get("res") or {}
    out.append(("scenario_ran", d.get("crash") is None,
                f"crash={d.get('crash')} trace={d.get('trace')}"))

    d1 = res.get("d1") or {}
    out.append((
        "d1_shifted_value_not_already_at_old",
        not d1.get("skipped") and d1.get("reason_code") != "already_at_old"
        and d1.get("success") is False,
        f"resultat={d1}. La valeur vaut 03:00 UTC (08:00 +05:00), l'etat "
        f"d'avant l'edition est 08:00 UTC: annoncer 'deja a l'etat d'avant' "
        f"laisse une heure fausse de cinq heures et une trace qui l'enterine.",
    ))
    out.append((
        "d1_value_untouched",
        d.get("d1_after") == d.get("d1_before"),
        f"avant={d.get('d1_before')} apres={d.get('d1_after')}",
    ))
    d2 = res.get("d2") or {}
    out.append((
        "d2_shifted_value_not_proven_new",
        d2.get("success") is False,
        f"resultat={d2}. La valeur vaut 05:00 UTC, l'etat NEW est 10:00 UTC: "
        f"les memes chiffres dans un autre fuseau ne prouvent pas que "
        f"l'entite porte l'etat de l'evenement.",
    ))
    out.append((
        "d2_value_untouched",
        d.get("d2_after") == d.get("d2_before"),
        f"avant={d.get('d2_before')} apres={d.get('d2_after')}",
    ))
    d3 = res.get("d3") or {}
    out.append((
        "d3_control_same_instant_other_zone_restored",
        d3.get("success") is True and not d3.get("skipped")
        and (d.get("d3_after") or {}).get("constat")
        == ["2026-01-15T08:00:00"],
        f"temoin: resultat={d3} apres={d.get('d3_after')}. 15:00 +05:00 EST "
        f"10:00 UTC, l'etat NEW: la preuve par instant doit passer et rendre "
        f"08:00 UTC.",
    ))
    d4 = res.get("d4") or {}
    out.append((
        "d4_deleted_constat_really_restored",
        d4.get("success") is True and not d4.get("skipped")
        and "2026-01-15T10:00:00" in ((d.get("d4_after") or {})
                                      .get("constat") or []),
        f"resultat={d4} apres={d.get('d4_after')}. Le constat supprime valait "
        f"10:00 UTC; celui qui reste vaut 05:00 UTC. 'Already exists' sur les "
        f"seuls chiffres est un succes pour une restauration qui n'a pas eu "
        f"lieu.",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"rs_datetime_instant_proof.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
