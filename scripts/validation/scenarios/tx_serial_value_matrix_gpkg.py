# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_serial_value_matrix_gpkg - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (une valeur restauree est IDENTIQUE a la valeur que
la source contenait ; I-1 : toute modification acceptee par le provider
atteint le journal).

Matrice d'aller-retour de bout en bout, sur une vraie GeoPackage, par les
vrais composants :

    valeur QGIS -> tampon d'edition -> commit -> capture du tracker
      (serialisation) -> file d'ecriture -> journal SQLite -> relecture de
      l'evenement -> deserialisation -> restore_deleted_feature -> relecture

La reference n'est pas ce que le scenario a VOULU ecrire, c'est ce que la
GeoPackage a reellement STOCKE apres le commit de l'utilisateur (NaN devient
NULL dans SQLite, une heure locale devient UTC, un demi-caractere devient
U+FFFD) : on compare la restauration a l'etat que l'utilisateur avait sous
les yeux, sans indulgence et sans faux positif.

Classes de valeurs : NULL reel, texte 'NULL', chaine vide, booleens,
bornes int32 et int64, flottant binaire 0.1+0.2, NaN, +/-infini, date,
heure avec millisecondes, date-heure naive, UTC, a decalage, instants du
changement d'heure Europe/Paris (printemps et automne), binaire, texte au
format UUID, objet JSON, liste de chaines, liste mixte, unicode (accents,
ideogrammes, droite-a-gauche, emoji, retours a la ligne, tabulation,
guillemets), texte tres long, et un demi-caractere UTF-16 produit par une
valeur par defaut QGIS (``'abc' || char(55357)``).

Scenario utilisateur : il saisit une entite (INSERT capture), la supprime
(DELETE capture), puis la restaure. Chaque champ doit revenir identique,
et l'insertion elle-meme doit etre journalisee malgre le demi-caractere.

Verdict attendu avant correctif : FAIL (millisecondes, spec des
date-heures, INSERT perdu par l'erreur d'encodage du demi-caractere).
"""
from __future__ import annotations

import math
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_serial_value_matrix_gpkg"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_UNICODE = ("accents éàüçÑ | 漢字カナ | مرحبا שלום | emoji \U0001F30D\U0001F9EE"
            "\nligne2\tTAB 'simple' \"double\" \\ fin")
_LONG = "échantillon-" * 6000
_UUID = "550e8400-e29b-41d4-a716-446655440000"
_BLOB = bytes(range(256)) + b"\x00\xff\x00"


def _field_defs():
    from qgis.PyQt.QtCore import QMetaType
    t = QMetaType.Type
    return [
        ("t_null", t.QString, ""), ("t_nullstr", t.QString, ""),
        ("t_empty", t.QString, ""), ("t_unicode", t.QString, ""),
        ("t_long", t.QString, ""), ("t_uuid", t.QString, ""),
        ("t_surr", t.QString, ""),
        ("b_true", t.Bool, ""), ("b_false", t.Bool, ""),
        ("i32max", t.Int, ""), ("i32min", t.Int, ""),
        ("i64max", t.LongLong, ""), ("i64min", t.LongLong, ""),
        ("f_dec", t.Double, ""), ("f_nan", t.Double, ""),
        ("f_inf", t.Double, ""), ("f_ninf", t.Double, ""),
        ("d_date", t.QDate, ""), ("tm_time", t.QTime, ""),
        ("dt_naive", t.QDateTime, ""), ("dt_utc", t.QDateTime, ""),
        ("dt_offset", t.QDateTime, ""), ("dt_dst_spring", t.QDateTime, ""),
        ("dt_dst_autumn", t.QDateTime, ""),
        ("bin", t.QByteArray, ""),
        ("js", t.QVariantMap, "JSON"), ("sl", t.QStringList, ""),
        ("mixed", t.QVariantList, ""),
    ]


def _values():
    from qgis.PyQt.QtCore import QByteArray, QDate, QDateTime, QTime, QTimeZone
    utc = QTimeZone(QTimeZone.Initialization.UTC)
    return {
        "t_null": None, "t_nullstr": "NULL", "t_empty": "",
        "t_unicode": _UNICODE, "t_long": _LONG, "t_uuid": _UUID,
        "b_true": True, "b_false": False,
        "i32max": 2147483647, "i32min": -2147483648,
        "i64max": 9223372036854775807, "i64min": -9223372036854775808,
        "f_dec": 0.1 + 0.2, "f_nan": float("nan"),
        "f_inf": float("inf"), "f_ninf": float("-inf"),
        "d_date": QDate(1899, 12, 31), "tm_time": QTime(10, 15, 42, 517),
        "dt_naive": QDateTime(QDate(2026, 7, 1), QTime(9, 0, 0, 250)),
        "dt_utc": QDateTime(QDate(2026, 1, 15), QTime(10, 15, 42, 517), utc),
        "dt_offset": QDateTime(QDate(2026, 1, 15), QTime(14, 30, 0, 517),
                               QTimeZone.fromSecondsAheadOfUtc(5 * 3600 + 1800)),
        # 02:00 Paris n'existe pas ce jour-la : l'instant 01:30Z est 03:30.
        "dt_dst_spring": QDateTime(QDate(2026, 3, 29), QTime(1, 30, 0, 1), utc),
        # 02:30 Paris existe deux fois ce jour-la : 00:30Z est la premiere.
        "dt_dst_autumn": QDateTime(QDate(2026, 10, 25), QTime(0, 30, 0, 2), utc),
        "bin": QByteArray(_BLOB),
        "js": {"a": 1, "b": [1, 2, {"c": "é"}], "n": None, "s": "NULL"},
        "sl": ["x", "é", "", "NULL"],
        "mixed": [1, 2.5, "s"],
    }


def _make_gpkg(tmpdir):
    from qgis.core import (
        QgsVectorLayer, QgsField, QgsVectorFileWriter,
        QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326", "seed", "memory")
    mem.dataProvider().addAttributes(
        [QgsField(n, t, tn) for n, t, tn in _field_defs()])
    mem.updateFields()
    path = os.path.join(tmpdir, "matrice.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "matrice"
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername=matrice", "matrice", "ogr")
    if not layer.isValid():
        raise RuntimeError("couche GPKG invalide")
    return layer


def _norm(value):
    """Forme comparable d'une valeur relue : chiffres, spec, ms."""
    try:
        from qgis.PyQt.QtCore import QByteArray, QDate, QDateTime, QTime
    except ImportError:
        return value
    if isinstance(value, QByteArray):
        return ("bytes", bytes(value))
    if isinstance(value, QDateTime):
        return ("datetime", value.toMSecsSinceEpoch(), value.timeSpec().name,
                value.toString("yyyy-MM-ddTHH:mm:ss.zzz"))
    if isinstance(value, QDate):
        return ("date", value.toString("yyyy-MM-dd"))
    if isinstance(value, QTime):
        return ("time", value.toString("HH:mm:ss.zzz"))
    if isinstance(value, float) and math.isnan(value):
        return ("nan",)
    return value


def _snapshot(layer, names):
    feat = next(layer.getFeatures(), None)
    if feat is None:
        return None
    return {name: _norm(feat[name]) for name in names}


def _short(value, limit=90):
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + f"...(+{len(text) - limit})"


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_value_matrix_gpkg setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    import json
    from qgis.core import (
        QgsDefaultValue, QgsVectorLayerUtils, QgsGeometry, QgsPointXY,
    )
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_deleted_feature
    from recoverland.core.logger import flog
    from scripts.validation.scenarios.tx_prov_fingerprint_split import (
        _open_journal, _WriteQueueBarrier,
    )

    d = ctx.data
    d["error"] = None
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_matrix_")
    jrnl = tempfile.mkdtemp(prefix="rl_tx_matrix_j_")
    jm = wq = None
    try:
        layer = _make_gpkg(tmpdir)
        names = [n for n, _t, _tn in _field_defs()]
        d["types"] = {f.name(): f.typeName() for f in layer.fields()}
        layer.setDefaultValueDefinition(
            layer.fields().indexOf("t_surr"),
            QgsDefaultValue("'abc' || char(55357)"))

        jm, wq = _open_journal(jrnl)
        barrier = _WriteQueueBarrier(wq)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        d["connected"] = layer.id() in tracker._connected_layers

        # 1. l'utilisateur saisit l'entite (valeur par defaut evaluee en C++)
        values = _values()
        attrs = {layer.fields().indexOf(k): v for k, v in values.items()}
        feat = QgsVectorLayerUtils.createFeature(
            layer, QgsGeometry.fromPointXY(QgsPointXY(3.25, 45.75)), attrs,
            layer.createExpressionContext())
        layer.startEditing()
        layer.addFeature(feat)
        d["insert_commit"] = bool(layer.commitChanges())
        barrier.wait(jm, timeout_s=6.0)
        layer.reload()
        d["reference"] = _snapshot(layer, names)

        # 2. l'utilisateur la supprime
        fid = next(layer.getFeatures()).id()
        layer.startEditing()
        layer.deleteFeature(fid)
        d["delete_commit"] = bool(layer.commitChanges())
        barrier.wait(jm, timeout_s=6.0)
        layer.reload()
        d["count_after_delete"] = layer.featureCount()

        # 3. il la restaure depuis le journal
        fp = compute_datasource_fingerprint(layer)
        conn = jm.create_read_connection()
        try:
            rows = conn.execute(
                "SELECT event_id, operation_type, attributes_json FROM "
                "audit_event WHERE datasource_fingerprint = ? "
                "ORDER BY event_id", (fp,)).fetchall()
            d["journal_ops"] = [r[1] for r in rows]
            inserts = [json.loads(r[2]) for r in rows if r[1] == "INSERT"]
            d["insert_t_surr"] = ((inserts[0].get("all_attributes") or {})
                                  .get("t_surr", "<absent>")
                                  if inserts else "<pas d'INSERT>")
            deletes = [r[0] for r in rows if r[1] == "DELETE"]
            event = get_event_by_id(conn, deletes[-1]) if deletes else None
        finally:
            conn.close()
        with tracker.suppressed():
            d["restore_result"] = (restore_deleted_feature(layer, event)
                                   if event is not None else {"success": None})
        layer.reload()
        d["restored"] = _snapshot(layer, names)
        try:
            tracker.disconnect_all()
        except Exception:  # noqa: BLE001
            pass
        layer = None
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        try:
            if wq is not None:
                wq.stop()
            if jm is not None and jm.is_open:
                jm.close()
        except Exception:  # noqa: BLE001
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(jrnl, ignore_errors=True)
    flog(f"tx_serial_value_matrix_gpkg run: trace_id={ctx.trace_id} "
         f"ops={d.get('journal_ops')} restore={d.get('restore_result')} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


_MESSAGES = {
    "t_null": "un NULL reel doit rester NULL, pas devenir une chaine",
    "t_nullstr": "le TEXTE 'NULL' est une valeur saisie, pas une absence",
    "t_empty": "vide et NULL sont deux informations differentes",
    "t_unicode": "accents, ideogrammes, emoji, retours a la ligne intacts",
    "t_long": "un texte long doit revenir complet",
    "t_uuid": "un identifiant texte au format UUID ne doit pas etre transforme",
    "t_surr": "la valeur que la source contient (U+FFFD) doit revenir",
    "b_true": "booleen vrai", "b_false": "booleen faux, pas zero ni NULL",
    "i32max": "borne haute int32", "i32min": "borne basse int32",
    "i64max": "borne haute int64", "i64min": "borne basse int64",
    "f_dec": "precision binaire 0.1+0.2 au dernier bit",
    "f_nan": "NaN (que SQLite stocke NULL) doit revenir tel que stocke",
    "f_inf": "infini positif", "f_ninf": "infini negatif",
    "d_date": "date ancienne 1899-12-31",
    "tm_time": "heure avec millisecondes",
    "dt_naive": "date-heure naive (la GeoPackage la stocke en UTC)",
    "dt_utc": "date-heure UTC avec millisecondes",
    "dt_offset": "date-heure a decalage +05:30, spec comprise",
    "dt_dst_spring": "instant du passage a l'heure d'ete a Paris",
    "dt_dst_autumn": "instant de l'heure doublee du retour a l'heure d'hiver",
    "bin": "piece jointe binaire octet pour octet",
    "js": "objet JSON imbrique", "sl": "liste de chaines",
    "mixed": "liste de valeurs mixtes",
}


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    ref = d.get("reference") or {}
    got = d.get("restored") or {}
    out = [
        ("scenario_ran", d.get("error") is None,
         f"le scenario a plante : {d.get('error')}"),
        ("fixture_committed",
         d.get("connected") is True and d.get("insert_commit") is True
         and d.get("delete_commit") is True
         and d.get("count_after_delete") == 0,
         f"connected={d.get('connected')} insert={d.get('insert_commit')} "
         f"delete={d.get('delete_commit')} restant={d.get('count_after_delete')}"),
        ("insert_with_lone_surrogate_is_journaled",
         "INSERT" in (d.get("journal_ops") or [])
         and d.get("insert_t_surr") == "abc�",
         f"operations journalisees={d.get('journal_ops')} t_surr de l'INSERT="
         f"{d.get('insert_t_surr')!r} : la saisie de l'utilisateur, acceptee "
         f"par la source, doit etre dans le journal avec la valeur stockee."),
        ("restore_reported_success",
         (d.get("restore_result") or {}).get("success") is True,
         f"restore_deleted_feature -> {d.get('restore_result')}"),
    ]
    for name, _t, _tn in _field_defs():
        b, a = ref.get(name, "<absent>"), got.get(name, "<absent>")
        out.append((
            f"matrix_{name}", b == a and b != "<absent>",
            f"{_MESSAGES.get(name, name)} : stocke avant={_short(b)} "
            f"restaure={_short(a)} (type {d.get('types', {}).get(name)})"))
    out.append(assert_log_contains(
        ctx.records, rf"tx_serial_value_matrix_gpkg run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
