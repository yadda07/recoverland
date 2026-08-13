"""tx_serial_gpkg_value_roundtrip - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (une valeur restauree doit etre IDENTIQUE a la
valeur capturee).
Cause racine: `core/serialization.py` expose `deserialize_value()` et
`_deserialize_blob()`, mais AUCUN site du chemin de restauration ne les
appelle. `restore_service.iter_mapped_attributes` recopie telle quelle la
valeur JSON du journal dans la couche.

Le trou que ce scenario prouve
------------------------------
La chaine reelle d'un rewind est :

    QgsFeature -> serialize_attributes()  -> JSON -> SQLite (audit_event)
              -> reconstruct_attributes() -> provider.addFeatures()
              -> relecture par l'utilisateur

L'aller (serialisation) est typé : un QByteArray devient la chaine
``"b64:<base64>"``, un QDateTime devient ``"yyyy-MM-ddTHH:mm:ss"``. Le
retour, lui, n'est PAS typé : la chaine est ecrite telle quelle dans le
champ. Deux consequences mesurees ici sur un vrai GeoPackage :

  1. BLOB. Le champ binaire (photo, signature, PDF, geometrie raster)
     revient dans la couche avec pour contenu les octets ASCII de sa
     propre etiquette base64 : ``b'b64:AAEC+vv/Cg0a'`` au lieu de
     ``b'\\x00\\x01\\x02\\xfa\\xfb\\xff\\n\\r\\x1a'``. La piece jointe est
     detruite ET le journal declare la restauration reussie.
  2. DATETIME. `_serialize_qt_type` ecrit ``toString("yyyy-MM-ddTHH:mm:ss")``
     sans fuseau. Le provider a rendu l'instant en UTC ; la chaine sans
     suffixe est relue comme une heure LOCALE et reconvertie en UTC. Chaque
     restauration retranche donc l'offset local (Europe/Paris : -1 h en
     hiver, -2 h en ete). La derive est CUMULATIVE : trois rewinds sur la
     meme entite = trois heures perdues, sans plafond et sans alerte.

Pourquoi c'est le pire cas pour un outil de recuperation : l'utilisateur
declenche le rewind precisement parce qu'il ne fait plus confiance a
l'etat courant. Un rewind qui echoue est reparable ; un rewind qui rend
une valeur alteree en annoncant "Restaure" est indetectable.

Scenario (vrai GeoPackage dans un dossier temporaire, vrai journal SQLite):
    setup:
        - GPKG "torture" a 17 champs typés (texte unicode, texte tres long,
          caracteres de controle, entiers 64 bits aux bornes, flottants
          extremes, booleens, date, datetime, BLOB, chaine vide, NULL)
        - une entite ecrite via le provider, puis RELUE : ce que le provider
          a reellement stocke est la reference. On ne compare pas a ce qu'on
          a voulu ecrire, on compare a ce que la couche contenait avant le
          cycle. Aucune indulgence, aucun faux positif non plus.
    run:
        - capture (serialize_attributes + build_full_snapshot + identite +
          schema reels), ecriture d'un evenement DELETE dans un vrai
          journal SQLite
        - suppression de l'entite dans le GPKG
        - relecture de l'evenement (get_event_by_id) puis
          restore_deleted_feature() : le vrai chemin de restauration
        - relecture de l'entite restauree
        - 2 cycles supplementaires pour mesurer si la derive s'accumule
    assertions: une par classe de valeur.

Verdict attendu avant correctif : FAIL (BLOB detruit, datetime derivant).
Verdict attendu apres correctif : PASS.
"""
from __future__ import annotations

import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_serial_gpkg_value_roundtrip"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LAYER_NAME = "torture"
_DS_FP = "tx_sgvr_datasource"
_PROJ_FP = "tx_sgvr_project"

# Champs typés du GeoPackage. GPKG est le provider au typage le plus riche
# disponible sans serveur : Integer64, Real, Boolean, Date, DateTime, Binary.
_URI = ("Point?crs=EPSG:4326"
        "&field=txt:string(0)"
        "&field=lgtxt:string(0)"
        "&field=ctrltxt:string(0)"
        "&field=nultxt:string(0)"
        "&field=i64max:long"
        "&field=i64min:long"
        "&field=ineg:long"
        "&field=izero:long"
        "&field=dbl:double"
        "&field=dtiny:double"
        "&field=dhuge:double"
        "&field=boot:boolean"
        "&field=boof:boolean"
        "&field=dat:date"
        "&field=dtm:datetime"
        "&field=bin:binary"
        "&field=estr:string(0)")

# Chaine de torture : accents, ideogrammes, droite-a-gauche, emoji hors BMP,
# retour a la ligne, tabulation, guillemets simples et doubles, antislash,
# caracteres de controle.
_TXT = (
    "accents: éàüçÑ | ideo: 漢字カナ"
    " | rtl: مرحبا שלום"
    " | emoji: \U0001F30D\U0001F9EE"
    "\nligne2\tTAB 'simple' \"double\" \\antislash %s ; DROP TABLE audit_event;--"
)
_CTRL = "".join(chr(c) for c in (1, 2, 7, 8, 11, 12, 27, 31, 127)) + "fin"
_NULTXT = "avant\x00apres"
_LONG = ("échantillon-" * 4000)  # ~52 000 caracteres
_BLOB = bytes([0, 1, 2, 250, 251, 255, 10, 13, 26, 34, 39, 92]) + bytes(range(256))

_I64_MAX = 9223372036854775807
_I64_MIN = -9223372036854775808
_DBL = 0.1 + 0.2                      # 0.30000000000000004
_DTINY = 5e-324                       # plus petit denormal
_DHUGE = 1.7976931348623157e308       # plus grand double fini


def _norm(value):
    """Forme Python comparable d'une valeur lue dans une couche QGIS."""
    try:
        from qgis.PyQt.QtCore import QByteArray
        if isinstance(value, QByteArray):
            return bytes(value)
    except ImportError:
        pass
    return value


def _short(value, limit: int = 70) -> str:
    text = repr(_norm(value))
    if len(text) <= limit:
        return text
    return text[:limit] + f"...(+{len(text) - limit})"


def _make_gpkg(tmpdir: str):
    """Cree un GeoPackage vide a partir du schema typé, puis le recharge."""
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(_URI, "seed", "memory")
    if not mem.isValid():
        raise RuntimeError("memory schema layer invalid")

    path = str(Path(tmpdir) / "torture.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = _LAYER_NAME
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")

    layer = QgsVectorLayer(f"{path}|layername={_LAYER_NAME}", _LAYER_NAME, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"gpkg layer invalid: {path}")
    return layer


def _seed_feature(layer) -> None:
    """Ecrit l'entite de torture via le provider (pas via le buffer memoire)."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    from qgis.PyQt.QtCore import QByteArray, QDate, QDateTime, QTime

    feat = QgsFeature(layer.fields())
    feat.setAttribute("txt", _TXT)
    feat.setAttribute("lgtxt", _LONG)
    feat.setAttribute("ctrltxt", _CTRL)
    feat.setAttribute("nultxt", _NULTXT)
    feat.setAttribute("i64max", _I64_MAX)
    feat.setAttribute("i64min", _I64_MIN)
    feat.setAttribute("ineg", -1)
    feat.setAttribute("izero", 0)
    feat.setAttribute("dbl", _DBL)
    feat.setAttribute("dtiny", _DTINY)
    feat.setAttribute("dhuge", _DHUGE)
    feat.setAttribute("boot", True)
    feat.setAttribute("boof", False)
    feat.setAttribute("dat", QDate(1899, 12, 31))
    feat.setAttribute("dtm", QDateTime(QDate(2026, 1, 15), QTime(14, 30, 0)))
    feat.setAttribute("bin", QByteArray(_BLOB))
    feat.setAttribute("estr", "")
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(3.25, 45.75)))

    ok, _added = layer.dataProvider().addFeatures([feat])
    if not ok:
        raise RuntimeError(f"seed insert failed: {layer.dataProvider().errors()}")
    layer.reload()


def _snapshot(layer, names) -> dict:
    feat = next(layer.getFeatures(), None)
    if feat is None:
        return {}
    out = {name: _norm(feat[name]) for name in names}
    geom = feat.geometry()
    out["__wkt__"] = geom.asWkt(6) if geom is not None and not geom.isNull() else None
    return out


def _capture_delete_event(conn, layer, names) -> int:
    """Capture l'entite vivante et ecrit un vrai evenement DELETE au journal."""
    from recoverland.core.serialization import (
        serialize_attributes, serialize_field_schema, build_full_snapshot,
    )
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.geometry_utils import geometry_to_wkb
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )

    feat = next(layer.getFeatures())
    attrs = serialize_attributes(feat, names)
    identity = compute_feature_identity(layer, feat)
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    row = (
        _PROJ_FP, _DS_FP, layer.id(), _LAYER_NAME, "ogr",
        identity, "DELETE", build_full_snapshot(attrs),
        geometry_to_wkb(feat.geometry()), "Point", "EPSG:4326",
        serialize_field_schema(layer.fields()), "tester", None,
        datetime.now(timezone.utc).isoformat(), None,
        compute_entity_fingerprint(identity), 5, None, None,
    )
    cur = conn.execute(sql, row)
    conn.commit()
    return cur.lastrowid, feat.id(), attrs


def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_sgvr_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_gpkg(tmpdir)
    _seed_feature(layer)

    names = [f.name() for f in layer.fields()]
    ctx.data["layer"] = layer
    ctx.data["names"] = names
    ctx.data["types"] = {f.name(): f.typeName() for f in layer.fields()}
    ctx.data["before"] = _snapshot(layer, names)

    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ctx.data["conn"] = conn

    flog(
        f"tx_serial_gpkg_value_roundtrip setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} fields={len(names)} "
        f"types={ctx.data['types']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.search_service import (
        get_event_by_id, reconstruct_attributes,
    )
    from recoverland.core.restore_service import restore_deleted_feature

    layer = ctx.data["layer"]
    names = ctx.data["names"]
    conn = ctx.data["conn"]
    provider = layer.dataProvider()

    flog(f"tx_serial_gpkg_value_roundtrip run start: trace_id={ctx.trace_id}",
         "INFO")

    # ---- cycle 1 : le cycle de reference, mesure par classe de valeur -----
    eid, fid, serialized = _capture_delete_event(conn, layer, names)
    ctx.data["serialized"] = {k: _short(v, 90) for k, v in serialized.items()}
    ctx.data["serialized_raw_bin"] = serialized.get("bin")
    ctx.data["event_id"] = eid

    event = get_event_by_id(conn, eid)
    ctx.data["reconstructed"] = {
        k: _short(v, 90) for k, v in reconstruct_attributes(event).items()
    }
    ctx.data["reconstructed_bin_type"] = type(
        reconstruct_attributes(event).get("bin")).__name__

    provider.deleteFeatures([fid])
    layer.reload()
    ctx.data["count_after_delete"] = layer.featureCount()

    ctx.data["restore_result"] = restore_deleted_feature(layer, event)
    layer.reload()
    ctx.data["after"] = _snapshot(layer, names)

    # ---- cycles 2 et 3 : la derive s'accumule-t-elle ? -------------------
    drift_track = [ctx.data["before"].get("dtm"), ctx.data["after"].get("dtm")]
    for _ in range(2):
        eid_n, fid_n, _attrs = _capture_delete_event(conn, layer, names)
        provider.deleteFeatures([fid_n])
        layer.reload()
        restore_deleted_feature(layer, get_event_by_id(conn, eid_n))
        layer.reload()
        drift_track.append(_snapshot(layer, names).get("dtm"))
    ctx.data["dtm_track"] = [
        v.toString("yyyy-MM-ddTHH:mm:ss") if hasattr(v, "toString") else repr(v)
        for v in drift_track
    ]

    flog(
        f"tx_serial_gpkg_value_roundtrip run end: trace_id={ctx.trace_id} "
        f"restore={ctx.data['restore_result']} "
        f"dtm_track={ctx.data['dtm_track']}",
        "INFO",
    )

    # Le runner n'a pas de hook de teardown : on libere ici. OGR peut encore
    # tenir le fichier ouvert sous Windows, d'ou ignore_errors.
    ctx.data["layer"] = None
    conn.close()
    ctx.data["conn"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _cmp(ctx, field: str):
    before = (ctx.data.get("before") or {}).get(field)
    after = (ctx.data.get("after") or {}).get(field)
    return before, after, before == after


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    result = ctx.data.get("restore_result") or {}

    # ===== Sanite du dispositif ==========================================
    out.append((
        "fixture_typed_gpkg",
        ctx.data.get("types", {}).get("bin") == "Binary"
        and ctx.data.get("types", {}).get("dtm") == "DateTime",
        f"types={ctx.data.get('types')}: le scenario a besoin d'un GPKG au "
        f"typage riche (Binary + DateTime), sinon il ne teste rien",
    ))
    out.append((
        "feature_was_really_deleted",
        ctx.data.get("count_after_delete") == 0,
        f"count_after_delete={ctx.data.get('count_after_delete')} attendu=0 : "
        f"sans suppression prealable la restauration serait un no-op et le "
        f"test serait truque",
    ))
    out.append((
        "restore_reported_success",
        result.get("success") is True,
        f"restore_deleted_feature a repondu {result}. Tout le reste du "
        f"scenario mesure ce que l'utilisateur recupere QUAND le plugin "
        f"annonce avoir reussi.",
    ))

    # ===== Chaines =======================================================
    b, a, ok = _cmp(ctx, "txt")
    out.append((
        "text_unicode_identical", ok,
        f"le champ texte revient altere : avant={_short(b)} apres={_short(a)}. "
        f"Accents, ideogrammes, ecriture droite-a-gauche, emoji hors BMP, "
        f"guillemets et antislash doivent traverser le journal intacts.",
    ))
    b, a, ok = _cmp(ctx, "lgtxt")
    out.append((
        "text_very_long_identical", ok,
        f"texte long tronque ou altere : longueur avant="
        f"{len(b) if isinstance(b, str) else None} apres="
        f"{len(a) if isinstance(a, str) else None}. Un champ de description "
        f"volumineux doit revenir complet.",
    ))
    b, a, ok = _cmp(ctx, "ctrltxt")
    out.append((
        "text_control_chars_identical", ok,
        f"caracteres de controle alteres : avant={_short(b)} apres={_short(a)}",
    ))
    b, a, ok = _cmp(ctx, "nultxt")
    out.append((
        "text_embedded_nul_identical", ok,
        f"chaine a octet NUL alteree : avant={_short(b)} apres={_short(a)}",
    ))
    b, a, ok = _cmp(ctx, "estr")
    out.append((
        "empty_string_stays_empty_string", ok,
        f"la chaine vide n'est pas revenue telle quelle : avant={b!r} "
        f"apres={a!r}. Vide et NULL sont deux informations differentes : "
        f"'renseigne mais vide' n'est pas 'non renseigne'.",
    ))

    # ===== Entiers 64 bits ===============================================
    for field, label in (("i64max", "borne haute 2^63-1"),
                         ("i64min", "borne basse -2^63"),
                         ("ineg", "negatif"), ("izero", "zero")):
        b, a, ok = _cmp(ctx, field)
        out.append((
            f"int64_{field}_identical", ok,
            f"entier ({label}) altere : avant={b!r} apres={a!r}",
        ))

    # ===== Flottants =====================================================
    for field, label in (("dbl", "precision binaire 0.1+0.2"),
                         ("dtiny", "denormal 5e-324"),
                         ("dhuge", "max double 1.797e308")):
        b, a, ok = _cmp(ctx, field)
        out.append((
            f"float_{field}_identical", ok,
            f"flottant ({label}) altere : avant={b!r} apres={a!r}. Une "
            f"superficie ou une altitude qui bouge au dernier bit fausse "
            f"tout recalcul en aval.",
        ))

    # ===== Booleens ======================================================
    b, a, ok = _cmp(ctx, "boot")
    out.append((
        "bool_true_identical",
        ok and a is True,
        f"booleen VRAI altere : avant={b!r} apres={a!r}",
    ))
    b, a, ok = _cmp(ctx, "boof")
    out.append((
        "bool_false_stays_false",
        ok and a is False,
        f"booleen FAUX altere : avant={b!r} apres={a!r}. Faux doit rester "
        f"faux et ne jamais devenir NULL : 'non' et 'inconnu' sont deux "
        f"reponses differentes.",
    ))

    # ===== Date / DateTime ===============================================
    b, a, ok = _cmp(ctx, "dat")
    out.append((
        "date_identical", ok,
        f"date alteree : avant={_short(b)} apres={_short(a)}",
    ))
    b, a, ok = _cmp(ctx, "dtm")
    out.append((
        "datetime_identical", ok,
        f"HORODATAGE DEPLACE par la restauration : avant={_short(b)} "
        f"apres={_short(a)}. La serialisation ecrit "
        f"toString('yyyy-MM-ddTHH:mm:ss') sans fuseau ; la relecture "
        f"reinterprete ces chiffres comme une heure locale. Une date de "
        f"depot, de constat ou de signature change d'heure a chaque "
        f"restauration.",
    ))
    track = ctx.data.get("dtm_track") or []
    unique = len(set(track))
    out.append((
        "datetime_drift_does_not_accumulate",
        unique <= 1,
        f"la derive s'accumule a chaque cycle : dtm={track} "
        f"({unique} valeurs distinctes sur {len(track)} mesures). Il n'y a "
        f"aucun plafond : N rewinds sur la meme entite = N fois l'offset "
        f"local retranche, silencieusement.",
    ))

    # ===== BLOB ==========================================================
    b, a, ok = _cmp(ctx, "bin")
    a_bytes = a if isinstance(a, (bytes, bytearray)) else b""
    out.append((
        "blob_returns_as_bytes_identical", ok,
        f"LE CHAMP BINAIRE EST DETRUIT : avant={_short(b)} apres={_short(a)}. "
        f"La restauration ecrit la chaine 'b64:...' au lieu de la decoder : "
        f"la piece jointe (photo, signature, PDF) est remplacee par les "
        f"octets ASCII de sa propre etiquette base64, et le plugin annonce "
        f"'Restaure'.",
    ))
    out.append((
        "blob_is_not_the_b64_label",
        not bytes(a_bytes).startswith(b"b64:"),
        f"le contenu binaire restaure commence par le marqueur base64 : "
        f"{_short(a)}. C'est la preuve directe que _deserialize_blob() n'a "
        f"jamais ete appele sur ce chemin.",
    ))

    # ===== Geometrie (temoin) ============================================
    b, a, ok = _cmp(ctx, "__wkt__")
    out.append((
        "geometry_identical", ok,
        f"geometrie alteree : avant={b!r} apres={a!r}",
    ))

    # ===== Preuve de mecanisme dans le code ==============================
    src_restore = (_PLUGIN_ROOT / "core" / "restore_service.py").read_text(
        encoding="utf-8", errors="replace")
    src_serial = (_PLUGIN_ROOT / "core" / "serialization.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "restore_path_decodes_values",
        "deserialize_value" in src_restore or "_deserialize_blob" in src_restore,
        "restore_service.py n'appelle nulle part deserialize_value() : la "
        "valeur JSON du journal est recopiee brute dans la couche. La "
        "fonction de decodage existe et est exportee par core/__init__.py, "
        "mais elle est morte sur le chemin de restauration.",
    ))
    out.append((
        "iter_mapped_attributes_decodes",
        "deserialize" in src_serial.split("def iter_mapped_attributes")[-1],
        "iter_mapped_attributes() est le point de passage unique de toutes "
        "les valeurs restaurees (5 sites d'appel) : c'est la qu'un decodage "
        "typé doit avoir lieu, sinon chaque appelant doit y penser seul.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"restore_deleted\[\d+\]: OK new_fid=",
        name="restore_logged_ok",
        min_count=3,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_gpkg_value_roundtrip.*trace_id={ctx.trace_id}",
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
