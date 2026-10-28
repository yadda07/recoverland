# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_mode_a_double_restore - RecoverLand validation runtime.

Invariant: I-3 et fail-loud (une restauration ne defait que l'evenement
qu'elle nomme; un journal ne dit jamais qu'une ecriture a compense un
evenement qu'elle n'a pas compense).
Constat resolu: item 5 du tour executeur. Une deuxieme restauration
(Mode A) d'un evenement DEJA compense ecrivait une trace qui defaisait
physiquement une modification posterieure.

Le mecanisme
------------
U: 'val: 10 -> 872'. L'utilisateur restaure U (Mode A): la couche revient
a 10, une trace T1 (restored_from_event_id = U) est journalisee. Il
retape 872 (evenement U2: 10 -> 872), puis restaure U une seconde fois.
La preuve de cible exige que l'entite porte l'etat d'APRES de U (872):
elle le porte, grace a U2. La restauration ecrit 10 et journalise une
trace T2... sur U. Physiquement, c'est U2 qui vient d'etre defait; le
journal dit que U a ete restaure deux fois et que U2 est toujours en
vigueur. Le rewind qui lit ce journal doit alors deviner laquelle des
deux traces compte, et une restauration de U2 (legitime) trouve la
couche deja a 10 et ne fait rien. Meme chose pour une suppression:
supprimer, restaurer, supprimer a nouveau, restaurer encore la premiere
suppression ressuscite l'entite sous une trace qui ne la concerne pas.

Ce que l'utilisateur voit
-------------------------
La deuxieme restauration de U repond "Reverted" alors qu'elle a annule
sa re-saisie, et son historique affiche U restaure deux fois, U2 jamais.

Ce qui est attendu: le refus NOMME (reason_code already_compensated), qui
cite la trace en vigueur et la modification posterieure, rien d'ecrit,
aucune trace. La restauration de U2 reste possible et juste; une
deuxieme restauration sans modification posterieure reste un saut
idempotent. Sans journal, l'executeur ne peut pas savoir (limite
nommee: le runner doit passer le journal).

Cas (GeoPackage, cle fid, journal SQLite reel)
----------------------------------------------
    A1  restauration de U: appliquee, trace T1
    A2  re-saisie 872 (U2) puis nouvelle restauration de U: refus
        already_compensated, 872 reste, aucune trace nouvelle; une seule
        trace sur U dans le journal
    A3  restauration de U2: appliquee (10), trace sur U2
    A4  troisieme restauration de U, rien n'a change depuis A3: saut
        idempotent (already_at_old), aucune trace
    D1  suppression de B, restauration (B revient, trace), nouvelle
        suppression (D2), nouvelle restauration de D: refus
        already_compensated, B reste absente, une seule trace sur D

Verdict avant correctif: FAIL (A2 et D1 ecrivent et journalisent une
seconde trace). Apres: PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "rs_mode_a_double_restore"
INVARIANT = "I-3"

_DS = "rs_mode_a_double_restore::ds"


def _now():
    return datetime.now(timezone.utc).isoformat()


def _gpkg(tmp):
    from qgis.core import (QgsVectorLayer, QgsVectorFileWriter, QgsFeature,
                           QgsCoordinateTransformContext, QgsGeometry,
                           QgsPointXY)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=code:string(10)"
                         "&field=val:integer", "seed", "memory")
    path = str(Path(tmp) / "dbl.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "dbl"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    layer = QgsVectorLayer(f"{path}|layername=dbl", "dbl", "ogr")
    feats = []
    for i, (code, val) in enumerate((("A", 10), ("B", 20), ("C", 30))):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("code", code)
        feat.setAttribute("val", val)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(100.0 * i, 5.0)))
        feats.append(feat)
    if not layer.dataProvider().addFeatures(feats)[0]:
        raise RuntimeError("seed impossible")
    layer.reload()
    return path, layer


def _journal():
    from recoverland.core.sqlite_schema import initialize_schema
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _journalled(conn, event):
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
        AUDIT_EVENT_INSERT_COLUMNS,
    )
    from recoverland.core.search_service import get_event_by_id
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    cur = conn.execute(sql, tuple(getattr(event, c)
                                  for c in AUDIT_EVENT_INSERT_COLUMNS))
    conn.commit()
    return get_event_by_id(conn, cur.lastrowid)


def _event(layer, op, identity, attributes, geom=None):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.serialization import serialize_field_schema
    return AuditEvent(
        event_id=None, project_fingerprint="rs_madr_project",
        datasource_fingerprint=_DS, layer_id_snapshot=layer.id(),
        layer_name_snapshot=layer.name(), provider_type="ogr",
        feature_identity_json=identity, operation_type=op,
        attributes_json=json.dumps(attributes), geometry_wkb=geom,
        geometry_type="Point", crs_authid="EPSG:2154",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id=None, created_at=_now(),
        restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(identity),
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)


def _feature(layer, code):
    layer.reload()
    return next((f for f in layer.getFeatures() if f["code"] == code), None)


def _edit_val(conn, layer, code, new):
    """User edit through the provider, journalled as the capture would."""
    from recoverland.core.identity import compute_feature_identity
    feat = _feature(layer, code)
    old = feat["val"]
    identity = compute_feature_identity(layer, feat)
    idx = layer.fields().indexOf("val")
    layer.dataProvider().changeAttributeValues({feat.id(): {idx: new}})
    layer.reload()
    return _journalled(conn, _event(layer, "UPDATE", identity, {
        "changed_only": {"val": {"old": old, "new": new}}}))


def _delete(conn, layer, code):
    from recoverland.core.identity import compute_feature_identity
    from recoverland.core.serialization import serialize_attributes
    feat = _feature(layer, code)
    identity = compute_feature_identity(layer, feat)
    snap = serialize_attributes(feat, [f.name() for f in layer.fields()])
    wkb = bytes(feat.geometry().asWkb())
    layer.dataProvider().deleteFeatures([feat.id()])
    layer.reload()
    return _journalled(conn, _event(layer, "DELETE", identity,
                                    {"all_attributes": snap}, geom=wkb))


def _restore(conn, layer, event):
    """Mode A: restore_batch with the journal, traces journalled."""
    from recoverland.core.restore_service import restore_batch
    report = restore_batch(layer, [event], journal=conn)
    for trace in report.trace_events:
        _journalled(conn, trace)
    layer.reload()
    return {"succeeded": list(report.succeeded),
            "failed": {int(k): str(v) for k, v in report.failed.items()},
            "skipped": list(report.skipped_idempotent),
            "traces": len(report.trace_events)}


def _traces_on(conn, eid):
    return conn.execute("SELECT COUNT(*) FROM audit_event WHERE "
                        "restored_from_event_id = ?", (eid,)).fetchone()[0]


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_rs_madr_")
    flog(f"rs_mode_a_double_restore setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    out = {}
    conn = _journal()
    try:
        _path, layer = _gpkg(ctx.data["tmp"])
        u1 = _edit_val(conn, layer, "A", 872)
        out["u1"] = u1.event_id
        out["a1"] = _restore(conn, layer, u1)
        out["a1_val"] = _feature(layer, "A")["val"]
        u2 = _edit_val(conn, layer, "A", 872)
        out["u2"] = u2.event_id
        out["a2"] = _restore(conn, layer, u1)
        out["a2_val"] = _feature(layer, "A")["val"]
        out["a2_traces_on_u1"] = _traces_on(conn, u1.event_id)
        out["a3"] = _restore(conn, layer, u2)
        out["a3_val"] = _feature(layer, "A")["val"]
        out["a4"] = _restore(conn, layer, u1)
        out["a4_val"] = _feature(layer, "A")["val"]
        out["a4_traces_on_u1"] = _traces_on(conn, u1.event_id)

        d1 = _delete(conn, layer, "B")
        out["d1"] = d1.event_id
        out["d1_first"] = _restore(conn, layer, d1)
        out["d1_back"] = _feature(layer, "B") is not None
        d2 = _delete(conn, layer, "B")
        out["d2"] = d2.event_id
        out["d1_second"] = _restore(conn, layer, d1)
        out["d1_second_present"] = _feature(layer, "B") is not None
        out["d1_traces"] = _traces_on(conn, d1.event_id)
        del layer
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        import traceback
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["trace"] = traceback.format_exc()[-1500:]
    finally:
        conn.close()
    ctx.data["out"] = out
    flog(f"rs_mode_a_double_restore run: trace_id={ctx.trace_id} "
         f"out={ {k: v for k, v in out.items() if k != 'trace'} }")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmp") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = ctx.data.get("out") or {}
    res = [("no_crash", "error" not in out,
            f"{out.get('error')} {out.get('trace', '')}")]
    u1, u2 = out.get("u1"), out.get("u2")
    a1 = out.get("a1") or {}
    res.append(("a1_first_restore_applies",
                a1.get("succeeded") == [u1] and a1.get("traces") == 1
                and out.get("a1_val") == 10,
                f"{a1} val={out.get('a1_val')}"))
    a2 = out.get("a2") or {}
    msg = (a2.get("failed") or {}).get(u1, "")
    res.append((
        "a2_second_restore_of_a_compensated_event_is_refused_by_name",
        u1 in (a2.get("failed") or {}) and "already_compensated" in msg
        and str(u2) in msg,
        f"deuxieme restauration de U={a2}: U est deja compense (trace en "
        f"vigueur) et U2 l'a re-edite depuis; ecrire 10 defait U2 sous une "
        f"trace qui nomme U. Le refus doit nommer already_compensated et "
        f"l'evenement {u2}.",
    ))
    res.append((
        "a2_nothing_written_no_trace",
        out.get("a2_val") == 872 and a2.get("traces") == 0
        and out.get("a2_traces_on_u1") == 1,
        f"val={out.get('a2_val')} (attendu 872, la re-saisie) traces "
        f"ecrites={a2.get('traces')} traces sur U dans le journal="
        f"{out.get('a2_traces_on_u1')} (attendu 1): le journal doit rester "
        f"sans ambiguite pour le rewind.",
    ))
    a3 = out.get("a3") or {}
    res.append(("a3_restore_of_the_later_edit_applies",
                a3.get("succeeded") == [u2] and a3.get("traces") == 1
                and out.get("a3_val") == 10,
                f"{a3} val={out.get('a3_val')}: la restauration de U2 est "
                f"la bonne, elle doit rester possible."))
    a4 = out.get("a4") or {}
    res.append(("a4_unchanged_since_is_an_idempotent_skip",
                a4.get("skipped") == [u1] and a4.get("traces") == 0
                and out.get("a4_val") == 10
                and out.get("a4_traces_on_u1") == 1,
                f"{a4} val={out.get('a4_val')} traces sur U="
                f"{out.get('a4_traces_on_u1')}: rien n'a change depuis, "
                f"rien a ecrire."))
    d1s = out.get("d1_second") or {}
    d1, d2 = out.get("d1"), out.get("d2")
    dmsg = (d1s.get("failed") or {}).get(d1, "")
    res.append((
        "d1_second_restore_of_a_compensated_delete_is_refused",
        (out.get("d1_first") or {}).get("succeeded") == [d1]
        and out.get("d1_back") is True
        and "already_compensated" in dmsg and str(d2) in dmsg
        and out.get("d1_second_present") is False
        and d1s.get("traces") == 0 and out.get("d1_traces") == 1,
        f"premiere={out.get('d1_first')} seconde={d1s} B presente apres="
        f"{out.get('d1_second_present')} traces sur D={out.get('d1_traces')}"
        f": re-inserer B defait la suppression {d2}, pas {d1}.",
    ))
    res.append(assert_log_contains(
        ctx.records, rf"rs_mode_a_double_restore run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return res


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
