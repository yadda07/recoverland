# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_composite_key_restore - RecoverLand validation runtime.

Invariant: I-3 (une cle designe UNE entite, prouvee avant toute ecriture)
et fail-loud (aucun "deja present" pour une restauration qui n'a pas eu
lieu).
Constats resolus: item E (cles composites cote restauration) et le BLOCKER
de la revue identite "une identite composite ne porte pas de pk_field:
`_reinsert_feature` saute le controle du detenteur de cle, le balayage
anti-doublon laisse toutes les colonnes de cle de cote, trouve une autre
ligne, succes alors que rien n'est restaure".

Le mecanisme
------------
Depuis v7 la capture journalise une cle (zone_id, parcel_no) en entier:
``pk_fields`` / ``pk_values``, sans ``pk_field``. Les chemins de
restauration ne lisaient que ``pk_field``: pour eux une identite composite
n'avait PAS de cle. Restaurer un DELETE sautait donc le controle du
detenteur de cle, et le balayage anti-doublon, qui ecarte les colonnes de
cle (``_comparable_fields``), prenait pour "deja restauree" une AUTRE
parcelle au meme contenu: "Already exists (skip)", succes, trace ecrite,
parcelle absente pour toujours. Une cle deja tenue par une autre parcelle
laissait inserer un doublon de cle. Un UPDATE ou un INSERT dont le FID
avait change n'etaient plus retrouves par leur cle. Enfin les evenements
anterieurs a v7 portent la PREMIERE colonne seule (zone_id=12): une paire
que `identity.effective_identity` retire, et que la restauration ne lui
faisait pas retirer.

Ce que l'utilisateur voit
-------------------------
Il restaure la parcelle (12,7) supprimee par erreur. Une parcelle jumelle
(12,8), meme nom, meme contour, existe: "Deja presente", la (12,7) ne
revient jamais et le journal dit qu'elle est restauree. Si la cle (12,7) a
ete redonnee, il obtient deux (12,7). Les evenements journalises avant v7
sont tous refuses "cle tenue par plusieurs entites" au lieu d'etre prouves
par le FID et le contenu.

Decor: une vraie couche QGIS (fournisseur memoire) presentee comme une
table PostGIS a cle (zone_id, parcel_no), comme dans
rw_composite_pk_identity.

Cas
---
    K1  DELETE (12,7), jumelle (12,8) meme contenu: (12,7) revient
    K2  rejeu de K1: saut prouve par la cle, pas de copie
    K3  cle (12,7) tenue par une autre parcelle: pk_conflict, rien insere
    K4  UPDATE (12,5), FID perime vers (12,1): ecrit sur (12,5) par la cle
    K5  cle (12,5) tenue par deux lignes (vue): target_ambiguous
    K6  INSERT (12,9), FID perime, jumelle (13,9): (12,9) supprimee seule,
        la continuite de la cle prouvee par le journal (la jumelle porte le
        meme contenu: seule l'histoire de la cle la distingue); K6c sans
        journal: rien n'est supprime, refus nomme
    K7  DELETE journalise avant v7 (zone_id seul): (12,7) revient
    K8  UPDATE journalise avant v7 (zone_id seul): ecrit sur (12,5)
    K9  rewind UPDATE (12,5), FID perime: ecrit sur (12,5) par la cle

Revue suivante: la meme faute pour une cle SIMPLE
-------------------------------------------------
Le correctif ne gardait comme preuve que les colonnes d'une cle
COMPOSITE. Pour une cle d'une seule colonne (le fid d'un GeoPackage, le
gid d'une table PostGIS), la colonne restait ecartee du balayage
anti-doublon, sur la foi d'une premisse fausse ("la reinsertion recoit un
fid NEUF"): QGIS 4.2.1 garde la cle ecrite, et la relecture
(`_readback_written`) refuse deja toute reinsertion dont la cle n'a pas
ete gardee. Une jumelle sous une autre cle etait prise pour l'entite
restauree: "Already exists (skip)", restore_batch la compte reussie,
aucune trace, et l'entite 3 ne revient jamais.

    T1  GeoPackage, DELETE du fid 3, jumelle fid 7 (meme nom, code,
        point), par restore_batch: le fid 3 revient, compte applique,
        une trace
    T2  temoin sans jumelle: le fid 3 revient
    T3  rejeu de T1: saut prouve par la cle, pas de copie
    T4  meme cas, identite journalisee par le FID seul (evenement ancien):
        la colonne fid, capturee et reecrite telle quelle, est aussi une
        preuve; la jumelle ne repond pas "deja la"

Mesure avant ce correctif: code du tour precedent 13/16 (T1 et T4
"Already exists" sur la jumelle, T3 en decoule), code d'origine 3/17.

Verdict avant correctif: FAIL. Apres: PASS.
"""
from __future__ import annotations

import json
from pathlib import Path

SCENARIO_ID = "rs_composite_key_restore"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_URI = ("Point?crs=EPSG:2154&field=zone_id:integer&field=parcel_no:integer"
        "&field=nom:string(40)")
_ROWS = [(12, 1, "parcelle un", 1.0), (12, 5, "b", 5.0),
         (12, 7, "parcelle sept", 7.0), (12, 9, "parcelle neuf", 9.0)]


def _cpk():
    from scripts.validation.scenarios import rw_composite_pk_identity as cpk
    return cpk


def _layer(rows):
    """(couche reelle, couche vue comme PostGIS a cle composite)."""
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY
    base = QgsVectorLayer(_URI, "parcelles_zone", "memory")
    feats = []
    for zone, parcel, nom, x in rows:
        feat = QgsFeature(base.fields())
        feat.setAttributes([zone, parcel, nom])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, 100.0)))
        feats.append(feat)
    base.dataProvider().addFeatures(feats)
    return base, _cpk().PkLayer(base)


def _rows(base):
    return sorted((f["zone_id"], f["parcel_no"], f["nom"])
                  for f in base.getFeatures())


def _legacy(event, identity):
    from recoverland.core.identity import compute_entity_fingerprint
    ident = json.dumps(identity)
    return event._replace(feature_identity_json=ident,
                          entity_fingerprint=compute_entity_fingerprint(ident),
                          event_schema_version=5)


def _with_fid(event, fid):
    identity = json.loads(event.feature_identity_json)
    identity["fid"] = fid
    return event._replace(feature_identity_json=json.dumps(identity))


def _journal():
    import sqlite3
    from recoverland.core.sqlite_schema import initialize_schema
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _journalled(conn, event):
    """Ecrit *event* dans le journal *conn* et le relit tel que stocke."""
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
        AUDIT_EVENT_INSERT_COLUMNS,
    )
    from recoverland.core.search_service import get_event_by_id
    cur = conn.execute(
        "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")",
        tuple(getattr(event, c) for c in AUDIT_EVENT_INSERT_COLUMNS))
    conn.commit()
    return get_event_by_id(conn, cur.lastrowid)


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"rs_composite_key_restore setup: trace_id={ctx.trace_id}", "INFO")


def _run_single_key_twin(res, rows, tid):
    """T1 a T4: cle d'une colonne (fid GeoPackage), jumelle sous le fid 7."""
    import shutil
    import tempfile
    from osgeo import ogr
    from scripts.validation.scenarios import _restore_fixtures as fx
    from recoverland.core import restore_service as rs

    tmpdir = tempfile.mkdtemp(prefix="rl_rs_cpk_t_")
    fields = [("nom", ogr.OFTString), ("code", ogr.OFTString)]
    twin = [(1, ["A", "K1"], "POINT (1 1)"),
            (7, ["ORIG", "K3"], "POINT (3 3)")]

    def _delete_event(eid, identity):
        return fx.audit_event(
            event_id=eid, operation_type="DELETE",
            field_schema_json=fx.schema(("fid", "integer"), ("nom", "string"),
                                        ("code", "string")),
            feature_identity_json=json.dumps(identity),
            entity_fingerprint=(f"pk:fid={identity['pk_value']}"
                                if "pk_value" in identity
                                else f"fid:{identity['fid']}"),
            attributes_json=json.dumps({"all_attributes": {
                "fid": 3, "nom": "ORIG", "code": "K3"}}),
            geometry_wkb=fx.wkb_wkt("POINT (3 3)"), geometry_type="Point",
            event_schema_version=7)

    def _report(rep):
        return {"succeeded": sorted(rep.succeeded),
                "skipped": sorted(rep.skipped_idempotent or []),
                "failed": {int(k): str(v)[:160] for k, v in rep.failed.items()},
                "traces": len(rep.trace_events)}

    pk3 = {"fid": 3, "pk_field": "fid", "pk_value": 3}
    try:
        for key, base_rows, identity, events in (
                ("t1", twin, pk3, [801, 802]),
                ("t2", twin[:1], pk3, [803]),
                ("t4", twin, {"fid": 3}, [804])):
            path = fx.make_gpkg(tmpdir, key, ogr.wkbPoint, fields, base_rows)
            layer = fx.open_layer(path, key)
            for n, eid in enumerate(events):
                tag = key if n == 0 else "t3"
                try:
                    res[tag] = _report(rs.restore_batch(
                        layer, [_delete_event(eid, identity)], trace_id=tid))
                except Exception as exc:  # noqa: BLE001
                    res[tag] = {"crash": repr(exc)}
                rows[tag] = fx.file_rows(path, key, with_fid=True)
            del layer
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run(ctx):
    from recoverland.core import restore_service as rs
    from recoverland.core.restore_executor import _apply_via_buffer
    from recoverland.core.serialization import compute_update_delta
    from recoverland.core.logger import flog
    cpk = _cpk()
    d = ctx.data
    tid = ctx.trace_id
    res = {}
    rows = {}
    d["error"] = None

    def _safe(fn, *args):
        try:
            return dict(fn(*args))
        except Exception as exc:  # noqa: BLE001 - un crash est un resultat
            return {"success": None, "crash": repr(exc)}

    try:
        # K1 / K2: the twin (12,8) carries the same name and outline.
        base, layer = _layer(_ROWS + [(12, 8, "parcelle sept", 7.0)])
        f7 = cpk._feature(base, 12, 7)
        dele = cpk._event(layer, f7, "DELETE", cpk._snapshot_json(f7), 0, 1001)
        d["identity_12_7"] = json.loads(dele.feature_identity_json)
        base.dataProvider().deleteFeatures([f7.id()])
        res["k1"] = _safe(rs.restore_deleted_feature, layer, dele)
        rows["k1"] = _rows(base)
        res["k2"] = _safe(rs.restore_deleted_feature, layer,
                          dele._replace(event_id=1002))
        rows["k2"] = _rows(base)

        # K3: the key (12,7) was given to another parcel.
        base, layer = _layer(_ROWS)
        f7 = cpk._feature(base, 12, 7)
        dele = cpk._event(layer, f7, "DELETE", cpk._snapshot_json(f7), 0, 1003)
        base.dataProvider().deleteFeatures([f7.id()])
        intrus = cpk._feature(base, 12, 9)
        base.dataProvider().changeAttributeValues(
            {intrus.id(): {1: 7, 2: "INTRUS"}})
        rows["k3_before"] = _rows(base)
        res["k3"] = _safe(rs.restore_deleted_feature, layer, dele)
        rows["k3"] = _rows(base)

        # K4: the UPDATE of (12,5), journalled with a FID that now names
        # (12,1) (another PostGIS session renumbered).
        base, layer = _layer(_ROWS)
        f5 = cpk._feature(base, 12, 5)
        upd = cpk._event(layer, f5, "UPDATE",
                         compute_update_delta({"nom": "avant"}, {"nom": "b"},
                                              ["nom"]), 0, 1004,
                         geometry=False)
        stale = _with_fid(upd, cpk._feature(base, 12, 1).id())
        res["k4"] = _safe(rs.restore_updated_feature, layer, stale)
        rows["k4"] = _rows(base)

        # K5: a view whose "key" holds two rows (12,5).
        base, layer = _layer(_ROWS + [(12, 5, "b", 6.0)])
        f5 = cpk._feature(base, 12, 5)
        upd5 = cpk._event(layer, f5, "UPDATE",
                          compute_update_delta({"nom": "avant"}, {"nom": "b"},
                                               ["nom"]), 0, 1005,
                          geometry=False)
        rows["k5_before"] = _rows(base)
        res["k5"] = _safe(rs.restore_updated_feature, layer, upd5)
        rows["k5"] = _rows(base)

        # K6: undo the INSERT of (12,9); FID stale, twin (13,9).
        # Hypothesis revised (v7): the twin carries the same name AND the
        # same point, so the content of (12,9) corroborates its key no more
        # than a value the whole layer shares (review: a key is STRONG only
        # when no other feature carries its evidence, or when the journal
        # proves it never changed hands). The deletion of (12,9) alone is
        # therefore asserted WITH the journal of the INSERT (K6), and the
        # same undo without that evidence must delete nothing (K6c):
        # stronger than before, since it also proves the composite key
        # through its history.
        for tag, with_journal in (("k6", True), ("k6c", False)):
            base, layer = _layer(_ROWS + [(13, 9, "parcelle neuf", 9.0)])
            f9 = cpk._feature(base, 12, 9)
            ins = cpk._event(layer, f9, "INSERT", cpk._snapshot_json(f9), 0,
                             1006)
            ins = _with_fid(ins, cpk._feature(base, 12, 1).id())
            conn = _journal()
            ins = _journalled(conn, ins)
            rows[f"{tag}_before"] = _rows(base)
            if with_journal:
                res[tag] = _safe(lambda lyr, ev: rs.restore_inserted_feature(
                    lyr, ev, journal=conn), layer, ins)
            else:
                res[tag] = _safe(rs.restore_inserted_feature, layer, ins)
            conn.close()
            rows[tag] = _rows(base)

        # K7 / K8: events journalled before v7, first key column only.
        base, layer = _layer(_ROWS)
        f7 = cpk._feature(base, 12, 7)
        f5 = cpk._feature(base, 12, 5)
        dele = _legacy(cpk._event(layer, f7, "DELETE", cpk._snapshot_json(f7),
                                  0, 1007),
                       {"fid": f7.id(), "pk_field": "zone_id", "pk_value": 12})
        base.dataProvider().deleteFeatures([f7.id()])
        res["k7"] = _safe(rs.restore_deleted_feature, layer, dele)
        rows["k7"] = _rows(base)
        upd = _legacy(cpk._event(layer, f5, "UPDATE",
                                 compute_update_delta({"nom": "avant"},
                                                      {"nom": "b"}, ["nom"]),
                                 0, 1008, geometry=False),
                      {"fid": f5.id(), "pk_field": "zone_id", "pk_value": 12})
        res["k8"] = _safe(rs.restore_updated_feature, layer, upd)
        rows["k8"] = _rows(base)

        # K9: the rewind compensates the UPDATE of (12,5), stale FID.
        base, layer = _layer(_ROWS)
        f5 = cpk._feature(base, 12, 5)
        upd = cpk._event(layer, f5, "UPDATE",
                         compute_update_delta({"nom": "avant"}, {"nom": "b"},
                                              ["nom"]), 0, 1009,
                         geometry=False)
        stale = _with_fid(upd, cpk._feature(base, 12, 1).id())
        base.startEditing()
        try:
            res["k9"] = dict(_apply_via_buffer(layer, "UPDATE", stale, {},
                                               trace_id=tid))
        except Exception as exc:  # noqa: BLE001
            res["k9"] = {"success": None, "crash": repr(exc)}
        d["k9_commit"] = bool(base.commitChanges())
        rows["k9"] = _rows(base)

        _run_single_key_twin(res, rows, tid)
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["error"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    d["res"] = res
    d["rows"] = rows
    summary = {key: (value.get("success"), value.get("skipped"),
                     value.get("reason_code"), value.get("identity_level"))
               for key, value in res.items()}
    flog(f"rs_composite_key_restore run: trace_id={tid} results={summary} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    res = d.get("res") or {}
    rows = d.get("rows") or {}
    out = [("scenario_ran", d.get("error") is None,
            f"crash={d.get('error')} trace={d.get('trace')}")]
    ident = d.get("identity_12_7") or {}
    out.append((
        "fixture_identity_is_composite",
        ident.get("pk_fields") == ["zone_id", "parcel_no"]
        and "pk_field" not in ident,
        f"identite journalisee de (12,7)={ident}",
    ))

    k1 = res.get("k1") or {}
    out.append((
        "k1_twin_is_not_already_exists",
        not k1.get("skipped") and k1.get("success") is True
        and (12, 7, "parcelle sept") in (rows.get("k1") or []),
        f"restauration du DELETE de (12,7): resultat={k1} lignes="
        f"{rows.get('k1')}. La jumelle (12,8) porte le meme nom et le meme "
        f"contour, mais une autre cle: 'Already exists' laisse (12,7) absente "
        f"pour toujours, sous une trace qui dit le contraire.",
    ))
    k2 = res.get("k2") or {}
    out.append((
        "k2_replay_skips_without_copy",
        k2.get("success") is True and k2.get("skipped") is True
        and (rows.get("k2") or []).count((12, 7, "parcelle sept")) == 1,
        f"rejeu: resultat={k2} lignes={rows.get('k2')}. (12,7) est revenue: "
        f"sa cle et son contenu le prouvent, le rejeu ne doit pas ajouter de "
        f"copie.",
    ))
    k3 = res.get("k3") or {}
    out.append((
        "k3_taken_key_is_refused",
        k3.get("success") is False and k3.get("reason_code") == "pk_conflict"
        and rows.get("k3") == rows.get("k3_before"),
        f"cle (12,7) redonnee a INTRUS: resultat={k3} lignes avant="
        f"{rows.get('k3_before')} apres={rows.get('k3')}. Reinserer sous une "
        f"cle deja tenue cree deux (12,7); le refus doit nommer le conflit.",
    ))
    k4 = res.get("k4") or {}
    out.append((
        "k4_update_lands_on_its_key",
        k4.get("success") is True
        and (12, 5, "avant") in (rows.get("k4") or [])
        and (12, 1, "parcelle un") in (rows.get("k4") or []),
        f"UPDATE de (12,5), FID perime vers (12,1): resultat={k4} lignes="
        f"{rows.get('k4')}. La cle complete designe (12,5), qui porte l'etat "
        f"NEW: l'ancien nom doit y revenir, et (12,1) rester intacte.",
    ))
    k5 = res.get("k5") or {}
    out.append((
        "k5_duplicated_key_is_ambiguous",
        k5.get("success") is False
        and k5.get("reason_code") == "target_ambiguous"
        and rows.get("k5") == rows.get("k5_before"),
        f"cle (12,5) tenue par deux lignes: resultat={k5} lignes="
        f"{rows.get('k5')}. Une cle qui designe deux lignes ne designe "
        f"personne: refus nomme 'ambigu'.",
    ))
    k6 = res.get("k6") or {}
    out.append((
        "k6_insert_undone_on_its_key_only",
        k6.get("success") is True
        and (12, 9, "parcelle neuf") not in (rows.get("k6") or [])
        and (13, 9, "parcelle neuf") in (rows.get("k6") or []),
        f"annulation de l'INSERT de (12,9), FID perime, jumelle (13,9), "
        f"journal de l'INSERT fourni: resultat={k6} lignes={rows.get('k6')}. "
        f"La cle complete designe (12,9), le journal prouve qu'elle n'a pas "
        f"change de mains; la jumelle ne doit pas bouger.",
    ))
    k6c = res.get("k6c") or {}
    out.append((
        "k6c_twin_without_history_deletes_nothing",
        k6c.get("success") is False
        and k6c.get("reason_code") in (
            "target_unverifiable", "target_ambiguous",
        )
        and rows.get("k6c") == rows.get("k6c_before"),
        f"la meme annulation sans journal: resultat={k6c} lignes="
        f"{rows.get('k6c')}. La jumelle (13,9) porte le meme nom et le meme "
        f"point: le contenu ne distingue pas (12,9) d'une entite qui aurait "
        f"repris sa cle; sans l'histoire de la cle, rien n'est supprime et "
        f"le refus est nomme.",
    ))
    k7 = res.get("k7") or {}
    out.append((
        "k7_legacy_delete_restored_by_fid_and_content",
        k7.get("success") is True and not k7.get("skipped")
        and (12, 7, "parcelle sept") in (rows.get("k7") or []),
        f"DELETE journalise avant v7 (zone_id=12 seul): resultat={k7} "
        f"lignes={rows.get('k7')}. La paire zone_id=12 designe toute la zone "
        f"et doit etre retiree (effective_identity); le FID et l'instantane "
        f"prouvent que (12,7) manque.",
    ))
    k8 = res.get("k8") or {}
    out.append((
        "k8_legacy_update_restored_by_fid_and_content",
        k8.get("success") is True
        and (12, 5, "avant") in (rows.get("k8") or [])
        and (12, 1, "parcelle un") in (rows.get("k8") or []),
        f"UPDATE journalise avant v7 (zone_id=12 seul): resultat={k8} "
        f"lignes={rows.get('k8')}",
    ))
    k9 = res.get("k9") or {}
    out.append((
        "k9_rewind_update_lands_on_its_key",
        k9.get("success") is True and d.get("k9_commit") is True
        and (12, 5, "avant") in (rows.get("k9") or [])
        and (12, 1, "parcelle un") in (rows.get("k9") or []),
        f"rewind de l'UPDATE de (12,5), FID perime: resultat={k9} "
        f"commit={d.get('k9_commit')} lignes={rows.get('k9')}",
    ))

    back = (3, 3, "ORIG", "K3", "Point (3 3)")
    twin = (7, 7, "ORIG", "K3", "Point (3 3)")
    for key, label in (("t1", "identite par la cle fid=3"),
                       ("t4", "identite par le FID seul (evenement ancien)")):
        rep = res.get(key) or {}
        got = rows.get(key) or []
        out.append((
            f"{key}_single_key_twin_is_not_already_exists",
            rep.get("succeeded") == [801 if key == "t1" else 804]
            and not rep.get("skipped") and rep.get("traces") == 1
            and back in got and twin in got,
            f"GeoPackage, restauration du DELETE du fid 3 ({label}), jumelle "
            f"fid 7 au meme nom, code et point: rapport={rep} fichier={got}. "
            f"La jumelle porte une AUTRE cle: 'Already exists' compte la "
            f"restauration reussie, sans trace, et le fid 3 ne revient "
            f"jamais.",
        ))
    t2 = res.get("t2") or {}
    out.append((
        "t2_control_no_twin_restored",
        t2.get("succeeded") == [803] and not t2.get("skipped")
        and back in (rows.get("t2") or []),
        f"temoin sans jumelle: rapport={t2} fichier={rows.get('t2')}: le "
        f"fournisseur garde la cle ecrite, le fid 3 revient.",
    ))
    t3 = res.get("t3") or {}
    out.append((
        "t3_replay_skips_without_copy",
        t3.get("succeeded") == [802] and t3.get("skipped") == [802]
        and t3.get("traces") == 0
        and (rows.get("t3") or []).count(back) == 1,
        f"rejeu de T1: rapport={t3} fichier={rows.get('t3')}. Le fid 3 est "
        f"revenu, sa cle et son contenu le prouvent: saut, aucune copie, "
        f"aucune trace.",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"rs_composite_key_restore run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
