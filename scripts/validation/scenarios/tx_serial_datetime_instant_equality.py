# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_serial_datetime_instant_equality - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (une valeur restauree est IDENTIQUE a la valeur
capturee ; aucun succes n'est annonce sur une valeur alteree, aucun echec
sur une valeur exacte).

Le trou que ce scenario prouve
------------------------------
Depuis v7, le texte journal d'une date-heure garde son type de fuseau
(``...14:30:00.000`` heure locale, ``...13:30:00.000Z`` UTC). Or
`commit_guard.values_equal`, le comparateur de TOUTES les relectures
(relecture d'une restauration, CommitGuard du rewind strict, reconciliation
apres crash), decidait l'egalite en comparant ces textes. Une GeoPackage
stocke l'heure locale saisie en UTC et la relit en UTC (mesure sous QGIS
4.2.1 : 14:30 locales ecrites -> ``13:30:00Z`` sur disque, relues UTC) : le
MEME instant donnait deux textes differents.

Consequences utilisateur, mesurees ici sur une vraie GeoPackage :

  * la restauration d'un DELETE ou d'un UPDATE journalise par 6.x (le
    journal de tout utilisateur actuel) ecrit la bonne valeur, puis
    l'annonce en echec ``failed_postcommit_verification`` ;
  * le CommitGuard du rewind strict classe FAILED_POSTCOMMIT_VERIFICATION
    une compensation qui a ecrit une heure locale exacte ;
  * la reconciliation apres crash ne retrouve pas un INSERT qui a bien
    atterri (instantane pris dans le tampon d'edition, heure locale) : il
    est classe "non applique" et n'est JAMAIS journalise, trou silencieux
    dans l'historique.

Une date-heure se compare comme un INSTANT, a la milliseconde, quel que
soit son texte ou son type de fuseau ; deux instants differents ne sont
jamais egaux, meme a chiffres identiques.

Second tour : a la RESOLUTION que porte la valeur stockee. v7 journalise
les millisecondes (un ``now()`` par defaut dans un formulaire), mais une
SpatiaLite stocke a la seconde (mesure : 14:30:12.517 ecrit, 14:30:12.000
relu), comme une colonne PostgreSQL ``timestamp(0)`` (qui arrondit) ou une
``DATE`` Oracle. Compare a la milliseconde, l'INSERT atterri avant un
crash n'etait pas retrouve (classe "non applique", jamais journalise : un
trou silencieux) et l'annulation de cet INSERT etait refusee pour toujours
(``differs=['dtm']``). Regle : egalite a la milliseconde, ou, quand la
valeur STOCKEE (le premier argument de `values_equal`, la valeur vivante
de `journal_datetime_matches`) n'a pas de fraction alors que la valeur
attendue en a une, moins d'une seconde d'ecart (troncature ou arrondi).
Un provider n'invente jamais de millisecondes : une valeur stockee a
fraction face a une valeur attendue entiere reste une difference.

Les dates d'essai (15 janvier, 15 juillet) doivent tomber a un decalage UTC
non nul dans le fuseau du poste : sinon heure locale et UTC coincident et
le scenario ne prouverait rien. La precondition est verifiee et echoue
bruyamment.

Verdict attendu avant correctif (worktree v7 avant ce correctif) : FAIL.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_serial_datetime_instant_equality"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _qt():
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    return QDate, QDateTime, QTime, QTimeZone


def _local(y, mo, d, h, mi, s=0, ms=0):
    QDate, QDateTime, QTime, _z = _qt()
    return QDateTime(QDate(y, mo, d), QTime(h, mi, s, ms))


def _utc(y, mo, d, h, mi, s=0, ms=0):
    QDate, QDateTime, QTime, QTimeZone = _qt()
    return QDateTime(QDate(y, mo, d), QTime(h, mi, s, ms),
                     QTimeZone(QTimeZone.Initialization.UTC))


def _offset(y, mo, d, h, mi, seconds):
    QDate, QDateTime, QTime, QTimeZone = _qt()
    return QDateTime(QDate(y, mo, d), QTime(h, mi),
                     QTimeZone.fromSecondsAheadOfUtc(seconds))


def _make_gpkg(tmpdir, name, geometry=False):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    head = "Point?crs=EPSG:4326&" if geometry else "NoGeometry?"
    mem = QgsVectorLayer(head + "field=label:string(20)&field=dtm:datetime",
                         "seed", "memory")
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "t"
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername=t", "t", "ogr")
    if not layer.isValid():
        raise RuntimeError("couche GPKG invalide")
    return path, layer


def _event(layer, op, attrs_json, version, fid=1, geometry_type="NoGeometry"):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.serialization import serialize_field_schema
    return AuditEvent(
        event_id=fid, project_fingerprint="p", datasource_fingerprint="ds",
        layer_id_snapshot=layer.id(), layer_name_snapshot="t",
        provider_type="ogr", feature_identity_json=json.dumps({"fid": fid}),
        operation_type=op, attributes_json=attrs_json,
        geometry_wkb=None, geometry_type=geometry_type, crs_authid="",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="t", session_id="s", created_at="2026-02-01T10:00:00",
        restored_from_event_id=None, entity_fingerprint=f"fid:{fid}",
        event_schema_version=version, new_geometry_wkb=None,
    )


def _raw(path):
    con = sqlite3.connect(path)
    try:
        return con.execute("SELECT label, dtm FROM t ORDER BY fid").fetchall()
    finally:
        con.close()


def _safe(fn, *args):
    try:
        res = fn(*args)
    except Exception as exc:  # noqa: BLE001 - measured, not hidden
        return {"exception": repr(exc)}
    return {
        k: res.get(k)
        for k in ("success", "reason_code", "message", "landed", "skipped")
    }


def _phase_precondition(d):
    for label, (mo, day) in (("hiver", (1, 15)), ("ete", (7, 15))):
        d[f"offset_{label}"] = int(_local(2026, mo, day, 12, 0).offsetFromUtc())


def _phase_comparator(d):
    from recoverland.core.commit_guard import values_equal
    from datetime import datetime, timezone
    # (valeur stockee, valeur attendue, attendu, attendu en sens inverse)
    directional = {
        # Hypothesis revised (v7): "milliseconds_differ" exigeait False dans
        # les deux sens. Dans le sens (stockee 14:30:00.000, attendue .517)
        # c'est l'ecriture exacte d'une valeur par un provider a la seconde
        # (SpatiaLite mesuree), que le comparateur doit reconnaitre ; le
        # sens (stockee .517, attendue .000) reste une difference, et une
        # seconde d'ecart ou plus en reste une dans les deux sens (ci-dessous).
        "milliseconds_differ": (_local(2026, 1, 15, 14, 30, 0, 517),
                                _local(2026, 1, 15, 14, 30, 0, 0),
                                False, True),
        "second_resolution_truncated": (_local(2026, 1, 15, 14, 30, 12, 0),
                                        _local(2026, 1, 15, 14, 30, 12, 517),
                                        True, False),
        "second_resolution_rounded": (_local(2026, 1, 15, 14, 30, 13, 0),
                                      _local(2026, 1, 15, 14, 30, 12, 517),
                                      True, False),
        "second_resolution_never_hides_a_second": (
            _local(2026, 1, 15, 14, 30, 14, 0),
            _local(2026, 1, 15, 14, 30, 12, 517), False, False),
        "both_sub_second_compared_to_the_ms": (
            _local(2026, 1, 15, 14, 30, 12, 300),
            _local(2026, 1, 15, 14, 30, 12, 517), False, False),
        "stored_whole_second_vs_v7_ms_text": (
            _utc(2026, 1, 15, 13, 30, 12, 0), "2026-01-15T14:30:12.517",
            True, False),
        "stored_whole_second_vs_text_two_seconds_away": (
            _utc(2026, 1, 15, 13, 30, 12, 0), "2026-01-15T14:30:14.517",
            False, False),
    }
    checks = {
        "utc_vs_local_same_instant": (_utc(2026, 1, 15, 13, 30),
                                      _local(2026, 1, 15, 14, 30), True),
        "utc_vs_local_same_digits": (_utc(2026, 1, 15, 13, 30),
                                     _local(2026, 1, 15, 13, 30), False),
        "offset_vs_utc_same_instant": (_offset(2026, 1, 15, 15, 30, 7200),
                                       _utc(2026, 1, 15, 13, 30), True),
        "summer_utc_vs_local": (_utc(2026, 7, 15, 12, 30),
                                _local(2026, 7, 15, 14, 30), True),
        "live_utc_vs_v7_naive_text": (_utc(2026, 1, 15, 13, 30),
                                      "2026-01-15T14:30:00.000", True),
        "live_utc_vs_v7_utc_text": (_utc(2026, 1, 15, 13, 30),
                                    "2026-01-15T13:30:00.000Z", True),
        "live_utc_vs_shifted_naive_text": (_utc(2026, 1, 15, 13, 30),
                                           "2026-01-15T13:30:00.000", False),
        "python_aware_vs_qt": (datetime(2026, 1, 15, 13, 30,
                                        tzinfo=timezone.utc),
                               _local(2026, 1, 15, 14, 30), True),
    }
    d["comparator"] = {}
    for name, (a, b, expected) in checks.items():
        directional[name] = (a, b, expected, expected)
    for name, (a, b, expected, expected_back) in directional.items():
        try:
            got = values_equal(a, b)
            back = values_equal(b, a)
        except Exception as exc:  # noqa: BLE001
            got = back = repr(exc)
        d["comparator"][name] = (got, back, expected, expected_back)


def _phase_v6_restores(d, tmpdir):
    from qgis.core import QgsFeature
    from recoverland.core.restore_service import (
        restore_deleted_feature, restore_updated_feature,
    )
    from recoverland.core.serialization import build_full_snapshot
    # 6.x DELETE : 14:30 locales (Paris, hiver) journalisees en chiffres UTC.
    path, layer = _make_gpkg(tmpdir, "v6_delete")
    d["v6_delete"] = _safe(restore_deleted_feature, layer, _event(
        layer, "DELETE", build_full_snapshot(
            {"fid": 1, "label": "orig", "dtm": "2026-01-15T13:30:00"}), 6))
    d["v6_delete_raw"] = _raw(path)
    # 6.x UPDATE : label et dtm changes ; la restauration remet l'ancien dtm.
    path2, layer2 = _make_gpkg(tmpdir, "v6_update")
    feat = QgsFeature(layer2.fields())
    feat.setAttribute("label", "after")
    feat.setAttribute("dtm", _local(2026, 1, 15, 16, 0))
    layer2.dataProvider().addFeatures([feat])
    layer2.reload()
    fid = next(layer2.getFeatures()).id()
    delta = json.dumps({"changed_only": {
        "label": {"old": "before", "new": "after"},
        "dtm": {"old": "2026-01-15T13:30:00", "new": "2026-01-15T15:00:00"}}})
    d["v6_update"] = _safe(restore_updated_feature, layer2,
                           _event(layer2, "UPDATE", delta, 6, fid))
    d["v6_update_raw"] = _raw(path2)


def _phase_commit_guard(d, tmpdir):
    """Le CommitGuard du rewind strict verifie une heure locale ecrite."""
    from qgis.core import QgsFeature
    from recoverland.core.commit_guard import (
        CommitGuard, EV_APPLIED_VERIFIED,
    )
    path, layer = _make_gpkg(tmpdir, "guard")
    feat = QgsFeature(layer.fields())
    feat.setAttribute("label", "g")
    feat.setAttribute("dtm", _local(2026, 1, 15, 16, 0))
    layer.dataProvider().addFeatures([feat])
    layer.reload()
    fid = next(layer.getFeatures()).id()
    layer.startEditing()
    guard = CommitGuard(layer, trace_id="dtguard")
    guard.begin_action(7)
    ok = layer.changeAttributeValue(fid, layer.fields().indexOf("dtm"),
                                    _local(2026, 1, 15, 14, 30))
    guard.end_action(7, extra_fids=[fid])
    verdict = guard.commit()
    d["guard_change_ok"] = bool(ok)
    d["guard_state"] = verdict.state
    d["guard_event"] = verdict.per_event.get(7)
    d["guard_detail"] = verdict.per_event_detail.get(7)
    d["guard_expected"] = EV_APPLIED_VERIFIED
    d["guard_raw"] = _raw(path)


def _phase_crash_reconcile(d, tmpdir):
    """Un INSERT atterri avant le crash, heure saisie dans le formulaire."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    from recoverland.core.crash_reconcile import _content_matches
    from recoverland.core.serialization import serialize_attributes
    from recoverland.core.geometry_utils import geometry_to_wkb
    path, layer = _make_gpkg(tmpdir, "crash", geometry=True)
    feat = QgsFeature(layer.fields())
    feat.setAttribute("label", "nouveau")
    feat.setAttribute("dtm", _local(2026, 1, 15, 14, 30))
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.35, 48.85)))
    snapshot = serialize_attributes(feat, ["label", "dtm"])
    wkb = geometry_to_wkb(feat.geometry())
    layer.dataProvider().addFeatures([feat])
    layer.reload()
    d["crash_snapshot"] = snapshot
    d["crash_raw"] = _raw(path)
    d["crash_matches"] = len(_content_matches(layer, snapshot, wkb))


def _spatialite_layer(tmpdir):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
        QgsDataSourceUri,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=label:string(20)"
                         "&field=dtm:datetime", "seed", "memory")
    path = os.path.join(tmpdir, "sl.sqlite")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "SpatiaLite"
    opts.layerName = "t"
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"SpatiaLite write failed: {err}")
    uri = QgsDataSourceUri()
    uri.setDatabase(path)
    uri.setDataSource("", "t", "GEOMETRY")
    layer = QgsVectorLayer(uri.uri(), "t", "spatialite")
    if not layer.isValid():
        raise RuntimeError("couche SpatiaLite invalide")
    return layer


def _phase_second_resolution(d, tmpdir):
    """SpatiaLite stocke a la seconde : INSERT a millisecondes, crash, annulation."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.crash_reconcile import _content_matches
    from recoverland.core.restore_service import restore_inserted_feature
    from recoverland.core.serialization import (
        serialize_attributes, build_full_snapshot, serialize_field_schema,
    )
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.geometry_utils import geometry_to_wkb
    layer = _spatialite_layer(tmpdir)
    feat = QgsFeature(layer.fields())
    feat.setAttribute("label", "nouveau")
    # Un now() par defaut dans le formulaire : des millisecondes.
    feat.setAttribute("dtm", _local(2026, 1, 15, 14, 30, 12, 517))
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.35, 48.85)))
    snapshot = serialize_attributes(feat, ["label", "dtm"])
    wkb = geometry_to_wkb(feat.geometry())
    layer.dataProvider().addFeatures([feat])
    layer.reload()
    stored = next(f for f in layer.dataProvider().getFeatures()
                  if f["label"] == "nouveau")
    d["sl_snapshot"] = snapshot
    d["sl_stored"] = stored["dtm"].toString("yyyy-MM-ddTHH:mm:ss.zzz")
    d["sl_crash_matches"] = len(_content_matches(layer, snapshot, wkb))
    # Temoin : une seconde et demie d'ecart n'est pas la meme saisie.
    far = dict(snapshot, dtm="2026-01-15T14:30:13.517")
    d["sl_crash_matches_far"] = len(_content_matches(layer, far, wkb))
    identity = compute_feature_identity(layer, stored)
    event = AuditEvent(
        event_id=77, project_fingerprint="p", datasource_fingerprint="ds",
        layer_id_snapshot=layer.id(), layer_name_snapshot="t",
        provider_type="spatialite", feature_identity_json=identity,
        operation_type="INSERT", attributes_json=build_full_snapshot(snapshot),
        geometry_wkb=wkb, geometry_type="Point", crs_authid="EPSG:4326",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="t", session_id="s", created_at="2026-09-01T10:00:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint=compute_entity_fingerprint(identity),
        event_schema_version=7, new_geometry_wkb=wkb)
    d["sl_undo"] = _safe(restore_inserted_feature, layer, event)
    layer.reload()
    d["sl_rows_after_undo"] = [f["label"] for f in
                               layer.dataProvider().getFeatures()]


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_datetime_instant_equality setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from qgis.PyQt.QtCore import QTimeZone
    from recoverland.core.logger import flog
    d = ctx.data
    d["error"] = None
    d["zone"] = bytes(QTimeZone.systemTimeZoneId()).decode("ascii", "replace")
    tmpdir = tempfile.mkdtemp(prefix="rl_dtinst_")
    try:
        _phase_precondition(d)
        _phase_comparator(d)
        for phase in (_phase_v6_restores, _phase_commit_guard,
                      _phase_crash_reconcile, _phase_second_resolution):
            try:
                phase(d, tmpdir)
            except Exception as exc:  # noqa: BLE001
                d.setdefault("phase_errors", {})[phase.__name__] = repr(exc)
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    flog(f"tx_serial_datetime_instant_equality run: trace_id={ctx.trace_id} "
         f"zone={d.get('zone')} v6_delete={d.get('v6_delete')} "
         f"v6_update={d.get('v6_update')} guard={d.get('guard_event')} "
         f"crash_matches={d.get('crash_matches')} "
         f"sl_stored={d.get('sl_stored')} "
         f"sl_crash_matches={d.get('sl_crash_matches')} "
         f"sl_undo={d.get('sl_undo')} "
         f"phase_errors={d.get('phase_errors')} error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = [
        ("scenario_ran", d.get("error") is None and not d.get("phase_errors"),
         f"le scenario a plante : {d.get('error')} {d.get('phase_errors')}"),
        ("precondition_zone_has_an_offset",
         d.get("offset_hiver") not in (None, 0)
         and d.get("offset_ete") not in (None, 0),
         f"fuseau du poste {d.get('zone')!r} : decalage UTC hiver="
         f"{d.get('offset_hiver')!r} s, ete={d.get('offset_ete')!r} s. A "
         f"decalage nul, heure locale et UTC coincident et ce scenario ne "
         f"prouve rien : lancez-le dans un fuseau decale (Europe/Paris)."),
    ]
    for name, (got, back, expected, expected_back) in (
            d.get("comparator") or {}).items():
        out.append((
            f"values_equal_{name}",
            got is expected and back is expected_back,
            f"values_equal(stockee, attendue) -> {got!r}, attendu "
            f"{expected!r} ; sens inverse -> {back!r}, attendu "
            f"{expected_back!r} : deux date-heures se comparent comme des "
            f"instants, a la resolution que porte la valeur stockee. Un meme "
            f"instant juge different fait annoncer en echec une restauration "
            f"exacte ; deux instants differents juges egaux laissent passer "
            f"une valeur decalee."))
    right = ("orig", "2026-01-15T13:30:00Z")
    out.append((
        "v6_delete_restore_reported_success",
        (d.get("v6_delete") or {}).get("success") is True
        and right in (d.get("v6_delete_raw") or []),
        f"restauration d'un DELETE journalise par 6.x : resultat="
        f"{d.get('v6_delete')} fichier={d.get('v6_delete_raw')} (attendu la "
        f"ligne {right}). La bonne valeur ecrite puis annoncee en echec : "
        f"l'utilisateur croit sa restauration ratee et recommence."))
    out.append((
        "v6_update_restore_reported_success",
        (d.get("v6_update") or {}).get("success") is True
        and ("before", "2026-01-15T13:30:00Z") in (d.get("v6_update_raw") or []),
        f"restauration d'un UPDATE journalise par 6.x : resultat="
        f"{d.get('v6_update')} fichier={d.get('v6_update_raw')}."))
    out.append((
        "strict_guard_verifies_a_local_time",
        d.get("guard_change_ok") is True
        and d.get("guard_event") == d.get("guard_expected")
        and ("g", "2026-01-15T13:30:00Z") in (d.get("guard_raw") or []),
        f"CommitGuard apres ecriture de 14:30 locales dans une GeoPackage : "
        f"etat={d.get('guard_state')!r} evenement={d.get('guard_event')!r} "
        f"detail={d.get('guard_detail')!r} fichier={d.get('guard_raw')}. Une "
        f"compensation exacte classee FAILED_POSTCOMMIT_VERIFICATION."))
    out.append((
        "crash_reconcile_finds_the_landed_insert",
        d.get("crash_matches") == 1,
        f"INSERT atterri avant le crash, instantane {d.get('crash_snapshot')} "
        f"fichier={d.get('crash_raw')} : {d.get('crash_matches')!r} "
        f"correspondance(s), attendu 1. Non retrouve, il est classe non "
        f"applique et jamais journalise : un trou silencieux dans "
        f"l'historique."))
    out.append((
        "spatialite_crash_reconcile_finds_the_landed_insert",
        d.get("sl_crash_matches") == 1 and d.get("sl_crash_matches_far") == 0,
        f"SpatiaLite (stockage a la seconde) : INSERT atterri avant le crash, "
        f"instantane {d.get('sl_snapshot')} relu {d.get('sl_stored')!r} : "
        f"{d.get('sl_crash_matches')!r} correspondance(s), attendu 1 ; a "
        f"1,5 s d'ecart : {d.get('sl_crash_matches_far')!r}, attendu 0. Non "
        f"retrouve, l'INSERT est classe non applique et jamais journalise : "
        f"un trou silencieux dans l'historique."))
    undo = d.get("sl_undo") or {}
    out.append((
        "spatialite_insert_with_milliseconds_can_be_undone",
        undo.get("success") is True
        and d.get("sl_rows_after_undo") is not None
        and "nouveau" not in d.get("sl_rows_after_undo"),
        f"annulation de l'INSERT (saisie a 14:30:12.517, stockee "
        f"{d.get('sl_stored')!r}) : resultat={undo} lignes restantes="
        f"{d.get('sl_rows_after_undo')}. Refusee pour une fraction de "
        f"seconde que le provider ne stocke pas, l'entite ne peut jamais "
        f"etre annulee."))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_datetime_instant_equality run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
