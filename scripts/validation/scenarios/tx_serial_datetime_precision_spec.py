# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_serial_datetime_precision_spec - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (aller-retour exact d'une valeur, independant du
poste qui capture et de celui qui restaure).

Les trous que ce scenario prouve
--------------------------------
1. Millisecondes. `_serialize_qt_type` ecrivait
   ``toUTC().toString("yyyy-MM-ddTHH:mm:ss")`` et ``"HH:mm:ss"`` : les
   millisecondes etaient perdues. Une colonne ``maj DEFAULT now()`` a
   10:15:42.517 revenait a 10:15:42.000 apres suppression puis rewind,
   annoncee APPLIED ; une cle temporelle (id, valid_from) ne correspond
   plus. Et une edition qui ne change QUE les millisecondes produisait deux
   textes identiques : aucun delta, aucun evenement, rien a restaurer.

2. Spec de temps. Toute valeur etait convertie en UTC et le suffixe
   jete ; la restauration reconstruisait une heure LOCALE. Une valeur UTC
   ou a decalage revenait donc avec une autre spec (la GeoPackage stocke
   alors un autre texte), et une valeur NAIVE dependait du fuseau du poste
   a la capture puis a la restauration : un journal capture dans une
   session distante en UTC et restaure sur un portable a Paris decale
   toutes les valeurs naives de l'offset.

Dispositif : une vraie GeoPackage, le vrai tracker pour l'edition de
millisecondes, le vrai chemin de restauration (`restore_deleted_feature`)
pour l'aller-retour, et le texte brut relu dans le fichier SQLite.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_serial_datetime_precision_spec"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _make_gpkg(tmpdir):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=label:string(10)"
                         "&field=maj:datetime", "seed", "memory")
    path = os.path.join(tmpdir, "precision.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "zones"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    layer = QgsVectorLayer(f"{path}|layername=zones", "zones_precision", "ogr")
    if not layer.isValid():
        raise RuntimeError("couche GPKG invalide")
    return layer, path


def _seed(layer):
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    utc = QDateTime(QDate(2026, 1, 15), QTime(10, 15, 42, 517),
                    QTimeZone(QTimeZone.Initialization.UTC))
    off = QDateTime(QDate(2026, 1, 15), QTime(14, 30, 0, 517),
                    QTimeZone.fromSecondsAheadOfUtc(3600))
    feats = []
    for label, value, x in (("utc", utc, 1.0), ("offset", off, 2.0)):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("label", label)
        feat.setAttribute("maj", value)
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, x)))
        feats.append(feat)
    ok, _ = layer.dataProvider().addFeatures(feats)
    if not ok:
        raise RuntimeError("seed impossible")
    layer.reload()


def _raw(path):
    conn = sqlite3.connect(path)
    try:
        return dict(conn.execute("SELECT label, maj FROM zones").fetchall())
    finally:
        conn.close()


def _live(layer):
    out = {}
    for feat in layer.getFeatures():
        value = feat["maj"]
        out[feat["label"]] = (value.toMSecsSinceEpoch(), value.timeSpec().name)
    return out


def _delete_and_restore(layer, conn):
    from recoverland.core.serialization import (
        serialize_attributes, serialize_field_schema, build_full_snapshot,
    )
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.geometry_utils import geometry_to_wkb
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
        CURRENT_SCHEMA_VERSION,
    )
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import restore_deleted_feature

    names = [f.name() for f in layer.fields()]
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    captured, results = {}, {}
    for feat in list(layer.getFeatures()):
        attrs = serialize_attributes(feat, names)
        captured[feat["label"]] = attrs.get("maj")
        identity = compute_feature_identity(layer, feat)
        cur = conn.execute(sql, (
            "p", "ds", layer.id(), "zones", "ogr", identity, "DELETE",
            build_full_snapshot(attrs), geometry_to_wkb(feat.geometry()),
            "Point", "EPSG:4326", serialize_field_schema(layer.fields()),
            "tester", None, datetime.now(timezone.utc).isoformat(), None,
            compute_entity_fingerprint(identity), CURRENT_SCHEMA_VERSION,
            None, None))
        conn.commit()
        layer.dataProvider().deleteFeatures([feat.id()])
        layer.reload()
        event = get_event_by_id(conn, cur.lastrowid)
        results[feat["label"]] = restore_deleted_feature(layer, event)
        layer.reload()
    return captured, results


def _ms_only_edit(d, layer):
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from scripts.validation.scenarios.tx_prov_fingerprint_split import (
        _open_journal, _WriteQueueBarrier,
    )
    jrnl = tempfile.mkdtemp(prefix="rl_prec_j_")
    jm = wq = None
    try:
        jm, wq = _open_journal(jrnl)
        barrier = _WriteQueueBarrier(wq)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        fid = next(f.id() for f in layer.getFeatures() if f["label"] == "utc")
        idx = layer.fields().indexOf("maj")
        layer.startEditing()
        layer.changeAttributeValue(fid, idx, QDateTime(
            QDate(2026, 1, 15), QTime(10, 15, 42, 900),
            QTimeZone(QTimeZone.Initialization.UTC)))
        d["ms_commit"] = bool(layer.commitChanges())
        barrier.wait(jm, timeout_s=5.0)
        fp = compute_datasource_fingerprint(layer)
        rconn = jm.create_read_connection()
        try:
            rows = rconn.execute(
                "SELECT operation_type, attributes_json FROM audit_event "
                "WHERE datasource_fingerprint = ?", (fp,)).fetchall()
        finally:
            rconn.close()
        d["ms_events"] = [(op, json.loads(a)) for op, a in rows]
        tracker.disconnect_all()
    finally:
        try:
            if wq is not None:
                wq.stop()
            if jm is not None and jm.is_open:
                jm.close()
        except Exception:  # noqa: BLE001
            pass
        shutil.rmtree(jrnl, ignore_errors=True)


def _spec_roundtrip(d):
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    from recoverland.core.serialization import serialize_value, deserialize_value
    values = {
        "naive": QDateTime(QDate(2026, 7, 1), QTime(9, 0, 0, 250)),
        "utc": QDateTime(QDate(2026, 7, 1), QTime(9, 0, 0, 250),
                         QTimeZone(QTimeZone.Initialization.UTC)),
        "offset": QDateTime(QDate(2026, 7, 1), QTime(9, 0, 0, 250),
                            QTimeZone.fromSecondsAheadOfUtc(-5 * 3600)),
    }
    out = {}
    for label, value in values.items():
        text = serialize_value(value)
        back = deserialize_value(text, "DateTime")
        out[label] = {
            "text": text,
            "same_instant": back == value,
            "same_spec": back.timeSpec() == value.timeSpec(),
            "same_digits": back.toString("yyyy-MM-ddTHH:mm:ss.zzz")
            == value.toString("yyyy-MM-ddTHH:mm:ss.zzz"),
        }
    d["spec"] = out
    d["time_text"] = serialize_value(QTime(10, 15, 42, 517))
    back = deserialize_value(d["time_text"], "Time")
    d["time_msec"] = back.msec() if hasattr(back, "msec") else None


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_datetime_precision_spec setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema
    d = ctx.data
    d["errors"] = {}
    tmpdir = tempfile.mkdtemp(prefix="rl_prec_")
    try:
        layer, path = _make_gpkg(tmpdir)
        _seed(layer)
        d["raw_before"] = _raw(path)
        d["live_before"] = _live(layer)
        conn = sqlite3.connect(":memory:")
        initialize_schema(conn)
        d["captured"], d["results"] = _delete_and_restore(layer, conn)
        conn.close()
        d["raw_after"] = _raw(path)
        d["live_after"] = _live(layer)
        try:
            _ms_only_edit(d, layer)
        except Exception as exc:  # noqa: BLE001
            d["errors"]["ms_edit"] = repr(exc)
        layer = None
    except Exception as exc:  # noqa: BLE001
        d["errors"]["roundtrip"] = repr(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    try:
        _spec_roundtrip(d)
    except Exception as exc:  # noqa: BLE001
        d["errors"]["spec"] = repr(exc)
    flog(f"tx_serial_datetime_precision_spec run: trace_id={ctx.trace_id} "
         f"captured={d.get('captured')} raw_before={d.get('raw_before')} "
         f"raw_after={d.get('raw_after')} errors={d['errors']}",
         "INFO" if not d["errors"] else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    before = d.get("live_before") or {}
    after = d.get("live_after") or {}
    raw_b = d.get("raw_before") or {}
    raw_a = d.get("raw_after") or {}
    results = d.get("results") or {}
    out = [("phases_ran", not d.get("errors"),
            f"phases en erreur : {d.get('errors')}"),
           ("restores_reported_success",
            all((r or {}).get("success") is True for r in results.values())
            and len(results) == 2,
            f"resultats={results} : la suite mesure ce que l'utilisateur "
            f"recupere quand le plugin annonce avoir reussi.")]
    for label in ("utc", "offset"):
        b, a = before.get(label), after.get(label)
        out.append((
            f"milliseconds_survive_{label}",
            bool(b) and bool(a) and b[0] == a[0],
            f"valeur {label} : instant avant={b} apres={a} (ms depuis "
            f"l'epoque, spec) ; texte journalise={(d.get('captured') or {}).get(label)!r}. "
            f"Une horodate DEFAULT now() revient tronquee a la seconde."))
        out.append((
            f"stored_text_identical_{label}",
            raw_b.get(label) is not None and raw_b.get(label) == raw_a.get(label),
            f"texte GeoPackage de {label} avant={raw_b.get(label)!r} apres="
            f"{raw_a.get(label)!r} : la valeur doit revenir dans sa spec "
            f"d'origine, pas convertie."))
    events = d.get("ms_events") or []
    changed = [(op, (a.get("changed_only") or {}).get("maj")) for op, a in events]
    out.append((
        "millisecond_only_edit_is_journaled",
        d.get("ms_commit") is True and any(
            op == "UPDATE" and c for op, c in changed),
        f"edition 10:15:42.517 -> 10:15:42.900 : evenements={changed}. Une "
        f"modification que le provider a enregistree doit etre journalisee, "
        f"sinon aucun rewind ne peut la defaire."))
    spec = d.get("spec") or {}
    for label in ("naive", "utc", "offset"):
        item = spec.get(label) or {}
        out.append((
            f"spec_roundtrip_{label}",
            item.get("same_instant") and item.get("same_spec")
            and item.get("same_digits"),
            f"valeur {label} : texte={item.get('text')!r} instant="
            f"{item.get('same_instant')} spec={item.get('same_spec')} chiffres="
            f"{item.get('same_digits')}. La valeur restauree doit avoir la "
            f"spec et les chiffres de la valeur capturee."))
    out.append((
        "naive_digits_not_converted",
        (spec.get("naive") or {}).get("text") == "2026-07-01T09:00:00.250",
        f"texte journalise d'une valeur naive 09:00:00.250 = "
        f"{(spec.get('naive') or {}).get('text')!r} : convertie avec le fuseau "
        f"du poste, elle revient decalee sur un poste d'un autre fuseau."))
    out.append((
        "time_milliseconds_survive",
        d.get("time_msec") == 517,
        f"QTime 10:15:42.517 -> {d.get('time_text')!r} -> msec="
        f"{d.get('time_msec')!r}"))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_datetime_precision_spec run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
