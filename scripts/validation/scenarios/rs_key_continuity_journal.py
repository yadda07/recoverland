# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rs_key_continuity_journal - RecoverLand validation runtime.

Invariant: I-3 (identite prouvee avant toute ecriture) sans refus
injustifie: une cle que le journal prouve continue designe l'entite.
Constat resolu: revue du tour v7, MAJOR "une valeur partagee seule sur une
cle est WEAK": `tx_serial_null_sentinels::gpkg_false_restored_as_false`
echoue, la restauration de 'flag: False -> True' sur une cle GeoPackage
jamais redonnee est refusee (value_shared:flag).

Le mecanisme
------------
Une cle n'est une identite que si elle n'a pas change de mains: INTEGER
PRIMARY KEY sans AUTOINCREMENT redonne le plus grand id, une sequence se
remet a zero, un import garde ses fid. Le tour precedent a donc exige que
le CONTENU corrobore la cle, et une valeur que d'autres entites portent
(un booleen, un statut, une enumeration) ne corrobore rien: refus. C'est
juste quand la cle a ete redonnee (INTRUS), faux quand elle ne l'a jamais
ete. Or ce qui les distingue n'est pas dans la couche -- les deux couches
sont identiques octet pour octet -- mais dans l'HISTOIRE: une cle ne
change de mains qu'apres la suppression de son entite (ou l'edition de la
cle). Le journal les enregistre. S'il contient l'evenement et, apres lui,
aucune suppression ni creation de cette cle et aucune edition d'une
colonne de cle sur la source, la cle est continue: elle designe l'entite
de l'evenement, et une concordance non discriminante (une valeur
partagee, des NULL) suffit alors a verifier que l'entite porte encore
l'etat de l'evenement. Sans journal, ou quand il ne peut pas repondre,
rien ne change: la cle reste a prouver par un contenu rare.

Ce que l'utilisateur voit
-------------------------
Il coche par erreur "flag" sur une entite d'une GeoPackage ou presque
toutes les entites sont cochees, et veut revenir en arriere: refus
"Target feature could not be verified", sa modification d'une seconde
n'est plus annulable. Idem pour l'annulation d'une restauration, pour le
rewind, et pour l'annulation d'un ajout d'entite vide.

Cas (GeoPackage, cle fid, journal SQLite reel)
----------------------------------------------
    J1  restauration 'flag: False -> True', le journal prouve la cle
        continue: FAUX revient (STRONG)
    J2  la meme sans journal: refus nomme, couche intacte (rien ne
        distingue cette couche de celle de J3)
    J3  le journal contient, apres l'evenement, la suppression de la cle
        3 puis sa creation par INTRUS: refus, INTRUS intacte
    J4  le journal contient, apres l'evenement, une edition de la colonne
        de cle: refus
    J5  l'evenement n'est pas dans le journal fourni: refus
    J6  annulation de la restauration (FAUX partage), journal continu
        (seule la trace de la restauration suit l'evenement): VRAI revient;
        J6c le meme sans journal: refus
    J7  rewind de la meme modification, journal continu: APPLIED, FAUX
        revient; J7c sans journal: refus
    J8  annulation d'un INSERT dont l'instantane est tout-NULL, journal
        continu: l'entite est supprimee; J8c le journal montre la cle
        supprimee puis recreee: refus

Verdict avant correctif: FAIL. Tour precedent 9/13 (J1, J6, J7, J8
refuses: value_shared, nulls_only); code d'origine 5/13 (J2 a J5, J6c,
J7c et J8c ecrivent sans preuve, J3 renomme INTRUS). Apres: PASS 13/13.

Revue du tour executeur (MAJOR): les lignes de plan du rewind
--------------------------------------------------------------
La preuve de continuite exigeait que le journal contienne l'evenement
LUI-MEME (meme operation, entite, date et contenu). Une ligne de plan du
rewind (`rewind_dedup.build_reapply_item`, `build_recompensate_item`)
n'est jamais cette ligne: elle porte l'id de la trace qu'elle defait (ou
de sa source), la date du basculement, et une re-application echange
l'avant et l'apres. Resultat: "key_continuity_unproven
(event_not_in_journal)" pour chaque aller-retour d'une modification dont
la valeur est partagee, journal branche ou non: la marche avant apres un
rewind, puis le re-rewind, etaient refuses sur toute GeoPackage.

La preuve s'ancre desormais sur l'evenement SOURCE: la ligne du journal
que la ligne de plan nomme (trace ou source), la chaine
restored_from_event_id jusqu'a l'evenement utilisateur, puis la
verification que la ligne de plan est exactement ce que le journal en
fait (meme constructeur, meme operation, meme contenu, meme geometrie,
meme entite, profondeur de la trace coherente avec le genre de ligne).
La recherche d'une suppression, d'une creation ou d'une edition de cle
part de la SOURCE, pas de la trace: une cle donnee a une autre entite
entre les deux casse la continuite (J12).

    J9  marche avant (re-application de 'flag: False -> True' apres un
        rewind), FAUX porte aussi par D, journal continu: VRAI revient;
        J9c sans journal: refus
    J10 re-rewind par une ligne de recompensation via la trace de la
        marche avant: FAUX revient
    J11 recompensation directe (la compensation a ete annulee par un
        Undo): FAUX revient
    J12 la cle 3 est supprimee puis donnee a INTRUS entre la source et
        la trace: refus, INTRUS intacte (la trace seule, sans rien
        apres elle, aurait prouve la cle)
    J13 ligne de re-application qui nomme la trace d'un AUTRE evenement:
        refus

Verdict avant correctif (tour de revue): FAIL (J9, J10, J11 refuses
event_not_in_journal). J9c, J12 et J13 passent avant et doivent le
rester. Apres: PASS.
"""
from __future__ import annotations

import inspect
import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rs_key_continuity_journal"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS = "rs_kcj_datasource"
_URI = ("Point?crs=EPSG:4326&field=etat:string(0)&field=flag:boolean"
        "&field=nb:long")
# (etat, flag, nb): C (fid 3) is the only FALSE one.
_ROWS = [("a", True, 1), ("b", True, 1), ("c", False, 1), ("d", True, 1),
         ("e", True, 1)]


def _gpkg(tmpdir, name, rows=_ROWS):
    """GeoPackage ecrite par QGIS (fid 1..n), comme tx_serial_null_sentinels."""
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
        QgsFeature, QgsGeometry, QgsPointXY,
    )
    mem = QgsVectorLayer(_URI, "seed", "memory")
    path = str(Path(tmpdir) / f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = name
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
    feats = []
    for i, (etat, flag, nb) in enumerate(rows):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("etat", etat)
        feat.setAttribute("flag", flag)
        feat.setAttribute("nb", nb)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, 45.0)))
        feats.append(feat)
    ok, _ = layer.dataProvider().addFeatures(feats)
    if not ok:
        raise RuntimeError(f"GPKG seed failed: {layer.dataProvider().errors()}")
    del layer
    return path


def _open(path, name):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername={name}", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche invalide: {path}")
    return layer


def _flags(path, name):
    layer = _open(path, name)
    out = sorted((f.id(), f["etat"], f["flag"]) for f in layer.getFeatures())
    del layer
    return out


def _journal():
    from recoverland.core.sqlite_schema import initialize_schema
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _journalled(conn, event):
    """Ecrit *event* au journal et le relit (event_id attribue par SQLite)."""
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


def _event(layer, operation, identity, attributes, minute, **extra):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.identity import compute_entity_fingerprint
    from recoverland.core.serialization import serialize_field_schema
    ident = identity if isinstance(identity, str) else json.dumps(identity)
    payload = dict(
        event_id=None, project_fingerprint="rs_kcj_project",
        datasource_fingerprint=_DS, layer_id_snapshot=layer.id(),
        layer_name_snapshot=layer.name(), provider_type="ogr",
        feature_identity_json=ident, operation_type=operation,
        attributes_json=attributes, geometry_wkb=None, geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="tester", session_id=None,
        created_at=f"2026-09-01T09:{minute:02d}:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(ident),
        event_schema_version=7, new_geometry_wkb=None, invalidated_at=None)
    payload.update(extra)
    return AuditEvent(**payload)


def _edit_flag(layer, fid, value):
    """Edite 'flag' par le fournisseur et rend (delta, identite) captures."""
    from qgis.core import QgsFeatureRequest
    from recoverland.core.serialization import (
        serialize_attributes, compute_update_delta,
    )
    from recoverland.core.identity import compute_feature_identity
    names = [f.name() for f in layer.fields()]
    feat = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([fid])))
    identity = compute_feature_identity(layer, feat)
    old = serialize_attributes(feat, names)
    idx = layer.fields().indexOf("flag")
    layer.dataProvider().changeAttributeValues({fid: {idx: value}})
    layer.reload()
    feat2 = next(layer.getFeatures(QgsFeatureRequest().setFilterFids([fid])))
    new = serialize_attributes(feat2, names)
    return compute_update_delta(old, new, ["flag"]), identity


def _call(fn, *args, journal=None):
    """Appelle *fn* avec le journal quand il l'accepte (avant correctif, non)."""
    try:
        accepts = "journal" in inspect.signature(fn).parameters
    except (TypeError, ValueError):
        accepts = False
    try:
        if accepts and journal is not None:
            out = fn(*args, journal=journal)
        else:
            out = fn(*args)
    except Exception as exc:  # noqa: BLE001 - un crash est un resultat
        return {"success": None, "crash": repr(exc)}, accepts
    return (dict(out) if isinstance(out, dict) else out), accepts


def _report(rep):
    if isinstance(rep, dict):
        return rep
    return {"succeeded": sorted(rep.succeeded),
            "failed": {int(k): str(v)[:200] for k, v in rep.failed.items()}}


def _trace(conn, layer, identity, source, minute, **extra):
    """Trace journalisee d'une ecriture du rewind sur *source*."""
    return _journalled(conn, _event(
        layer, "UPDATE", identity,
        json.dumps({"_restore_ref": source.event_id}), minute,
        restored_from_event_id=source.event_id, **extra))


def _intrus_at_3(layer, conn, identity):
    """La cle 3 est supprimee puis donnee a INTRUS (FAUX, comme D)."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    layer.dataProvider().deleteFeatures([3])
    intrus = QgsFeature(layer.fields())
    intrus.setAttribute("fid", 3)
    intrus.setAttribute("etat", "INTRUS")
    intrus.setAttribute("flag", False)
    intrus.setAttribute("nb", 9)
    intrus.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(9.0, 45.0)))
    layer.dataProvider().addFeatures([intrus])
    layer.reload()
    _journalled(conn, _event(layer, "DELETE", identity, json.dumps(
        {"all_attributes": {"fid": 3, "etat": "c", "flag": True,
                            "nb": 1}}), 5))
    _journalled(conn, _event(layer, "INSERT", identity, json.dumps(
        {"all_attributes": {"fid": 3, "etat": "INTRUS", "flag": False,
                            "nb": 9}}), 6))


def _plan_story(kind):
    """*story(layer, conn)* de J9 a J13: edite la couche, journalise
    l'histoire et rend la ligne de plan du rewind a compenser."""
    from recoverland.core.rewind_dedup import (
        build_reapply_item, build_recompensate_item,
    )

    def story(layer, conn):
        delta, identity = _edit_flag(layer, 3, True)  # U, the user's edit
        u = _journalled(conn, _event(layer, "UPDATE", identity, delta, 0))
        if kind == "reapply":
            _edit_flag(layer, 3, False)  # the rewind to before U (R1)
            r1 = _trace(conn, layer, identity, u, 10)
            return build_reapply_item(u, r1, (r1.created_at, r1.event_id))
        if kind == "recompensate_via":
            _edit_flag(layer, 3, False)  # R1: rewind to before U
            _edit_flag(layer, 3, True)   # R2: forward hop, U re-applied
            r1 = _trace(conn, layer, identity, u, 10)
            r2 = _trace(conn, layer, identity, r1, 20)
            return build_recompensate_item(
                u, (r2.created_at, r2.event_id), via=r2)
        if kind == "recompensate_direct":
            # R1 wrote FALSE, then an Undo put TRUE back (no row written,
            # R1 stamped invalidated).
            undo_at = "2026-09-01T09:15:00+00:00"
            r1 = _trace(conn, layer, identity, u, 10, invalidated_at=undo_at)
            return build_recompensate_item(u, (undo_at, r1.event_id))
        if kind == "key_given_away":
            _intrus_at_3(layer, conn, identity)
            r1 = _trace(conn, layer, identity, u, 10)
            return build_reapply_item(u, r1, (r1.created_at, r1.event_id))
        if kind == "foreign_trace":
            _edit_flag(layer, 3, False)  # C back to FALSE
            delta2, ident1 = _edit_flag(layer, 1, False)  # U2 on A
            _edit_flag(layer, 1, True)                     # R2 undid U2
            u2 = _journalled(conn, _event(layer, "UPDATE", ident1, delta2, 2))
            r2 = _trace(conn, layer, ident1, u2, 12)
            return build_reapply_item(u, r2, (r2.created_at, r2.event_id))
        raise ValueError(kind)
    return story


def _plan_row_case(tmp, name, res, rows, tid, kind, with_journal=True):
    """J9 a J13: une ligne de plan du rewind appliquee dans le tampon.

    FAUX est porte par C (cle 3) et D: seule la continuite de la cle
    peut corroborer une ecriture sur la cle 3.
    """
    from recoverland.core.restore_executor import _apply_via_buffer
    shared = [("a", True, 1), ("b", True, 1), ("c", False, 1),
              ("d", False, 1), ("e", True, 1)]
    path = _gpkg(tmp, name, shared)
    layer = _open(path, name)
    conn = _journal()
    row = _plan_story(kind)(layer, conn)
    rows[f"{name}_before"] = _flags(path, name)
    layer.startEditing()
    result, res[f"{name}_api"] = _call(
        _apply_via_buffer, layer, "UPDATE", row, {}, tid,
        journal=conn if with_journal else None)
    res[name] = result
    res[f"{name}_commit"] = bool(layer.commitChanges())
    if not res[f"{name}_commit"]:
        layer.rollBack()
    del layer
    conn.close()
    rows[name] = _flags(path, name)


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rs_kcj_")
    flog(f"rs_key_continuity_journal setup: trace_id={ctx.trace_id} "
         f"tmpdir={ctx.data['tmpdir']}", "INFO")


def _restore_case(tmp, name, res, rows, journal_kind):
    """J1 / J2 / J3 / J4 / J5: la restauration de 'flag: False -> True'."""
    from recoverland.core.restore_service import restore_updated_feature
    path = _gpkg(tmp, name)
    layer = _open(path, name)
    delta, identity = _edit_flag(layer, 3, True)
    conn = _journal()
    event = _journalled(conn, _event(layer, "UPDATE", identity, delta, 0))
    if journal_kind == "reused":
        # The key 3 is deleted, then given to INTRUS (explicit fid).
        from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
        layer.dataProvider().deleteFeatures([3])
        intrus = QgsFeature(layer.fields())
        intrus.setAttribute("fid", 3)
        intrus.setAttribute("etat", "INTRUS")
        intrus.setAttribute("flag", True)
        intrus.setAttribute("nb", 9)
        intrus.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(9.0, 45.0)))
        layer.dataProvider().addFeatures([intrus])
        layer.reload()
        _journalled(conn, _event(layer, "DELETE", identity, json.dumps(
            {"all_attributes": {"fid": 3, "etat": "c", "flag": True,
                                "nb": 1}}), 5))
        _journalled(conn, _event(layer, "INSERT", identity, json.dumps(
            {"all_attributes": {"fid": 3, "etat": "INTRUS", "flag": True,
                                "nb": 9}}), 6))
    elif journal_kind == "key_edited":
        other = json.dumps({"fid": 9, "pk_field": "fid", "pk_value": 9})
        _journalled(conn, _event(layer, "UPDATE", other, json.dumps(
            {"changed_only": {"fid": {"old": 9, "new": 3}}}), 5))
    elif journal_kind == "foreign":
        conn.close()
        conn = _journal()
        _journalled(conn, _event(layer, "UPDATE", identity, delta, 1))
    rows[f"{name}_before"] = _flags(path, name)
    given = None if journal_kind == "none" else conn
    res[name], res[f"{name}_api"] = _call(restore_updated_feature, layer,
                                          event, journal=given)
    del layer
    conn.close()
    rows[name] = _flags(path, name)


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.restore_service import (
        undo_restore_batch, restore_inserted_feature,
    )
    from recoverland.core.restore_executor import _apply_via_buffer
    tmp = ctx.data["tmpdir"]
    tid = ctx.trace_id
    res, rows = {}, {}
    ctx.data["error"] = None
    try:
        for name, kind in (("j1", "continuous"), ("j2", "none"),
                           ("j3", "reused"), ("j4", "key_edited"),
                           ("j5", "foreign")):
            _restore_case(tmp, name, res, rows, kind)

        # J6: the restore already wrote FALSE back on C; D is FALSE too.
        # The journal holds the edit, then the trace of its restore.
        for name, with_journal in (("j6", True), ("j6c", False)):
            undo_rows = [("a", True, 1), ("b", True, 1), ("c", False, 1),
                         ("d", False, 1), ("e", True, 1)]
            path = _gpkg(tmp, name, undo_rows)
            layer = _open(path, name)
            delta, identity = _edit_flag(layer, 3, True)
            _edit_flag(layer, 3, False)  # the restore, done earlier
            conn = _journal()
            event = _journalled(conn, _event(layer, "UPDATE", identity,
                                             delta, 0))
            _journalled(conn, _event(
                layer, "UPDATE", identity,
                json.dumps({"_restore_ref": event.event_id}), 10,
                restored_from_event_id=event.event_id))
            rows[f"{name}_before"] = _flags(path, name)
            rep, res[f"{name}_api"] = _call(
                undo_restore_batch, layer, [event],
                journal=conn if with_journal else None)
            res[name] = _report(rep)
            del layer
            conn.close()
            rows[name] = _flags(path, name)

        # J7: the rewind of the same edit, through the edit buffer.
        for name, with_journal in (("j7", True), ("j7c", False)):
            path = _gpkg(tmp, name)
            layer = _open(path, name)
            delta, identity = _edit_flag(layer, 3, True)
            conn = _journal()
            event = _journalled(conn, _event(layer, "UPDATE", identity,
                                             delta, 0))
            rows[f"{name}_before"] = _flags(path, name)
            layer.startEditing()
            result, res[f"{name}_api"] = _call(
                _apply_via_buffer, layer, "UPDATE", event, {}, tid,
                journal=conn if with_journal else None)
            res[name] = result
            res[f"{name}_commit"] = bool(layer.commitChanges())
            if not res[f"{name}_commit"]:
                layer.rollBack()
            del layer
            conn.close()
            rows[name] = _flags(path, name)

        # J8: undo of the INSERT of an empty feature (fid 6, all NULL, no
        # geometry). J8c: the journal shows key 6 deleted then re-created.
        for name, reused in (("j8", False), ("j8c", True)):
            from qgis.core import QgsFeature
            path = _gpkg(tmp, name)
            layer = _open(path, name)
            empty = QgsFeature(layer.fields())
            empty.setAttribute("fid", 6)
            layer.dataProvider().addFeatures([empty])
            layer.reload()
            ident6 = {"fid": 6, "pk_field": "fid", "pk_value": 6}
            snap = json.dumps({"all_attributes": {
                "fid": 6, "etat": None, "flag": None, "nb": None}})
            conn = _journal()
            event = _journalled(conn, _event(layer, "INSERT", ident6, snap, 0))
            if reused:
                _journalled(conn, _event(layer, "DELETE", ident6, snap, 5))
                _journalled(conn, _event(layer, "INSERT", ident6, snap, 6))
            rows[f"{name}_before"] = _flags(path, name)
            res[name], res[f"{name}_api"] = _call(
                restore_inserted_feature, layer, event, journal=conn)
            del layer
            conn.close()
            rows[name] = _flags(path, name)

        # J9 to J13: rows of a rewind plan (forward hop, re-rewind).
        for name, kind, with_journal in (
                ("j9", "reapply", True), ("j9c", "reapply", False),
                ("j10", "recompensate_via", True),
                ("j11", "recompensate_direct", True),
                ("j12", "key_given_away", True),
                ("j13", "foreign_trace", True)):
            _plan_row_case(tmp, name, res, rows, tid, kind, with_journal)
    except Exception as exc:  # noqa: BLE001
        import traceback
        ctx.data["error"] = f"{type(exc).__name__}: {exc}"
        ctx.data["trace"] = traceback.format_exc()[-1500:]
    ctx.data["res"] = res
    ctx.data["rows"] = rows
    summary = {k: ((v.get("success"), v.get("reason_code"),
                    v.get("identity_level")) if "success" in v else v)
               for k, v in res.items() if isinstance(v, dict)}
    flog(f"rs_key_continuity_journal run: trace_id={tid} results={summary} "
         f"error={ctx.data.get('error')}",
         "INFO" if ctx.data.get("error") is None else "ERROR")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    res = d.get("res") or {}
    rows = d.get("rows") or {}
    out = [("scenario_ran", d.get("error") is None,
            f"crash={d.get('error')} trace={d.get('trace')}")]
    refusals = ("target_unverifiable", "target_ambiguous")

    def flag_of(name, fid=3):
        return next((r[2] for r in rows.get(name) or [] if r[0] == fid),
                    "<absente>")

    j1 = res.get("j1") or {}
    out.append((
        "j1_continuous_key_restores_shared_value",
        j1.get("success") is True and j1.get("identity_level") == "STRONG"
        and flag_of("j1") is False,
        f"restauration de 'flag: False -> True' sur la cle 3, jamais "
        f"redonnee (le journal le prouve): resultat={j1} flag apres="
        f"{flag_of('j1')!r} journal accepte={res.get('j1_api')}. VRAI est "
        f"porte par 4 entites, mais la cle designe l'entite: refuser ici "
        f"rend l'erreur d'un clic impossible a annuler.",
    ))
    for name, label in (
            ("j2", "sans journal (rien ne distingue cette couche de J3)"),
            ("j3", "le journal montre la cle 3 supprimee puis donnee a "
                   "INTRUS"),
            ("j4", "le journal montre une edition de la colonne de cle"),
            ("j5", "l'evenement n'est pas dans le journal fourni")):
        result = res.get(name) or {}
        out.append((
            f"{name}_unproven_continuity_is_refused",
            result.get("success") is False
            and result.get("reason_code") in refusals
            and rows.get(name) == rows.get(f"{name}_before"),
            f"{label}: resultat={result} couche avant="
            f"{rows.get(name + '_before')} apres={rows.get(name)}. Une valeur "
            f"partagee ne prouve pas une cle dont la continuite n'est pas "
            f"etablie: l'entite qui tient la cle peut etre une autre.",
        ))
    j6 = res.get("j6") or {}
    out.append((
        "j6_undo_with_continuous_key_applied",
        not j6.get("failed") and j6.get("succeeded") and flag_of("j6") is True,
        f"annulation de la restauration (FAUX, porte aussi par D), journal "
        f"continu: rapport={j6} flag apres={flag_of('j6')!r} journal "
        f"accepte={res.get('j6_api')}",
    ))
    j6c = res.get("j6c") or {}
    out.append((
        "j6c_undo_without_journal_refused",
        bool(j6c.get("failed")) and rows.get("j6c") == rows.get("j6c_before"),
        f"la meme annulation sans journal: rapport={j6c} apres="
        f"{rows.get('j6c')}",
    ))
    j7 = res.get("j7") or {}
    out.append((
        "j7_rewind_with_continuous_key_applied",
        j7.get("success") is True and not j7.get("skipped")
        and res.get("j7_commit") is True and flag_of("j7") is False,
        f"rewind de 'flag: False -> True', journal continu: resultat={j7} "
        f"commit={res.get('j7_commit')} flag apres={flag_of('j7')!r} journal "
        f"accepte={res.get('j7_api')}",
    ))
    j7c = res.get("j7c") or {}
    out.append((
        "j7c_rewind_without_journal_refused",
        j7c.get("success") is False and j7c.get("reason_code") in refusals
        and rows.get("j7c") == rows.get("j7c_before"),
        f"le meme rewind sans journal: resultat={j7c} apres={rows.get('j7c')}",
    ))
    j8 = res.get("j8") or {}
    out.append((
        "j8_insert_undo_with_continuous_key_applied",
        j8.get("success") is True and not j8.get("skipped")
        and flag_of("j8", 6) == "<absente>" and len(rows.get("j8") or []) == 5,
        f"annulation de l'INSERT d'une entite vide (fid 6), journal continu: "
        f"resultat={j8} lignes apres={rows.get('j8')} journal accepte="
        f"{res.get('j8_api')}",
    ))
    j8c = res.get("j8c") or {}
    out.append((
        "j8c_insert_undo_reused_key_refused",
        j8c.get("success") is False and j8c.get("reason_code") in refusals
        and rows.get("j8c") == rows.get("j8c_before"),
        f"le journal montre la cle 6 supprimee puis recreee: resultat={j8c} "
        f"apres={rows.get('j8c')}. L'entite vide qui tient la cle peut etre "
        f"une autre: la supprimer n'est pas prouve.",
    ))
    for name, want, label in (
            ("j9", True, "marche avant apres un rewind (re-application de "
                         "'flag: False -> True')"),
            ("j10", False, "re-rewind par la recompensation via la trace "
                           "de la marche avant"),
            ("j11", False, "recompensation directe apres l'Undo de la "
                           "compensation")):
        result = res.get(name) or {}
        out.append((
            f"{name}_plan_row_with_continuous_key_applied",
            result.get("success") is True and not result.get("skipped")
            and res.get(f"{name}_commit") is True and flag_of(name) is want,
            f"{label}, FAUX porte aussi par D, journal continu (la source "
            f"et ses traces seules touchent la cle 3): resultat={result} "
            f"commit={res.get(name + '_commit')} flag apres="
            f"{flag_of(name)!r} (attendu {want!r}). Refuser ici rend tout "
            f"aller-retour d'une modification a valeur partagee impossible "
            f"sur une GeoPackage.",
        ))
    for name, label in (
            ("j9c", "la meme marche avant sans journal"),
            ("j12", "la cle 3 a ete donnee a INTRUS entre la source et la "
                    "trace: ancree sur la trace, la preuve ne voit rien"),
            ("j13", "la ligne de plan nomme la trace d'un autre evenement "
                    "(A): ce n'est pas la ligne que le journal construit")):
        result = res.get(name) or {}
        out.append((
            f"{name}_plan_row_unproven_continuity_refused",
            result.get("success") is False
            and result.get("reason_code") in refusals
            and rows.get(name) == rows.get(f"{name}_before"),
            f"{label}: resultat={result} couche avant="
            f"{rows.get(name + '_before')} apres={rows.get(name)}",
        ))
    out.append(assert_log_contains(
        ctx.records, rf"rs_key_continuity_journal run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
