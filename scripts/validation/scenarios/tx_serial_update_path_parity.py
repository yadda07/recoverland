"""tx_serial_update_path_parity - RecoverLand validation runtime.

Invariant: BL-SER-P0-04 (les deux sens du rewind ecrivent des valeurs
typees : revenir en arriere, puis annuler ce retour, doit rendre l'entite
exactement a son etat de depart).
Cause racine: identique a BL-SER-P0-01 - `iter_mapped_attributes` est le
goulot unique des cinq sites d'ecriture et ne decode rien.

Ce que ce scenario ajoute a tx_serial_gpkg_value_roundtrip
----------------------------------------------------------
Le premier scenario prouve la corruption sur la reinsertion d'une entite
supprimee (`restore_deleted_feature`). Or la fonction phare du plugin
n'est pas celle-la : c'est le REWIND, qui passe par le delta d'un
evenement UPDATE, et son bouton d'annulation, qui repasse par le meme
delta dans l'autre sens.

    restore_updated_feature   -> ecrit le cote "old" du delta  (rewind)
    _undo_update_restore      -> ecrit le cote "new" du delta  (annuler)

Les deux consomment `iter_mapped_attributes`. La corruption est donc
symetrique et se cumule : un aller-retour rewind + annulation ne rend pas
l'entite a son etat de depart, il l'eloigne deux fois.

Mesure faite ici sur un vrai GeoPackage :
  - BLOB : ``b'\\x00\\x01\\x02\\xfa\\xff'`` -> ``b'b64:AAEC+v8='`` a
    l'aller, puis ``b'b64:WFla'`` au retour. L'utilisateur qui annule sa
    restauration ne recupere pas sa piece jointe : il en obtient une
    deuxieme etiquette base64.
  - DATETIME : l'offset local est retranche a chaque ecriture, dans les
    deux sens. Aller-retour = deux heures perdues (Europe/Paris, hiver),
    et le plugin annonce deux succes.

Garantie acquise verifiee au passage : les champs de type liste exposes
par GPKG en sous-type JSON (`stringlist` -> JSON, `integerlist` -> JSON)
traversent parfaitement la chaine dans les deux sens. `serialize_value`
descend recursivement dans les listes et le provider relit du JSON : rien
a corriger de ce cote, et le correctif du BLOB ne doit pas le casser.

Scenario (vrai GeoPackage en dossier temporaire, vrai journal SQLite):
    setup: lst(tags JSON, nums JSON, bin Binary, dtm DateTime, nom String)
           + 1 entite
    run:
        - capture de l'etat AVANT (serialize_attributes)
        - edition de tous les champs par le provider, capture de l'APRES
        - compute_update_delta reel -> evenement UPDATE au journal SQLite
        - restore_updated_feature()  : doit rendre l'etat AVANT
        - undo_restore_batch()       : doit rendre l'etat APRES
    Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_serial_update_path_parity"
INVARIANT = "BL-SER-P0-04"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LAYER_NAME = "lst"
_DS_FP = "tx_supp_datasource"
_PROJ_FP = "tx_supp_project"

_URI = ("Point?crs=EPSG:4326"
        "&field=tags:stringlist"
        "&field=nums:integerlist"
        "&field=bin:binary"
        "&field=dtm:datetime"
        "&field=nom:string(0)")

_TAGS_OLD = ["alpha", "bêta", "gamma"]
_TAGS_NEW = ["delta"]
_NUMS_OLD = [1, 2, 3]
_NUMS_NEW = [9]
_BLOB_OLD = bytes([0, 1, 2, 250, 255])
_BLOB_NEW = b"XYZ"


def _norm(value):
    try:
        from qgis.PyQt.QtCore import QByteArray
        if isinstance(value, QByteArray):
            return bytes(value)
    except ImportError:
        pass
    return value


def _short(value, limit: int = 60) -> str:
    text = repr(_norm(value))
    return text if len(text) <= limit else text[:limit] + "..."


def _make_gpkg(tmpdir: str):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(_URI, "seed", "memory")
    if not mem.isValid():
        raise RuntimeError("memory schema layer invalid")
    path = str(Path(tmpdir) / "lst.gpkg")
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


def _seed(layer):
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    from qgis.PyQt.QtCore import QByteArray, QDate, QDateTime, QTime

    feat = QgsFeature(layer.fields())
    feat.setAttribute("tags", list(_TAGS_OLD))
    feat.setAttribute("nums", list(_NUMS_OLD))
    feat.setAttribute("bin", QByteArray(_BLOB_OLD))
    feat.setAttribute("dtm", QDateTime(QDate(2026, 1, 15), QTime(14, 30, 0)))
    feat.setAttribute("nom", "AVANT")
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(3.0, 45.0)))
    ok, _ = layer.dataProvider().addFeatures([feat])
    if not ok:
        raise RuntimeError(f"seed failed: {layer.dataProvider().errors()}")
    layer.reload()


def _snapshot(layer, names) -> dict:
    feat = next(layer.getFeatures(), None)
    if feat is None:
        return {}
    return {n: _norm(feat[n]) for n in names}


def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_supp_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_gpkg(tmpdir)
    _seed(layer)
    names = [f.name() for f in layer.fields()]
    ctx.data["layer"] = layer
    ctx.data["names"] = names
    ctx.data["subtypes"] = {f.name(): f.typeName() for f in layer.fields()}
    ctx.data["state_before"] = _snapshot(layer, names)

    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ctx.data["conn"] = conn

    flog(
        f"tx_serial_update_path_parity setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} subtypes={ctx.data['subtypes']} "
        f"state_before={{k: _short(v, 40) for k, v in ctx.data['state_before'].items()}}",
        "INFO",
    )


def run(ctx):
    from qgis.PyQt.QtCore import QByteArray, QDate, QDateTime, QTime
    from recoverland.core.logger import flog
    from recoverland.core.serialization import (
        serialize_attributes, serialize_field_schema, compute_update_delta,
    )
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.search_service import get_event_by_id
    from recoverland.core.restore_service import (
        restore_updated_feature, undo_restore_batch,
    )

    layer = ctx.data["layer"]
    names = ctx.data["names"]
    conn = ctx.data["conn"]
    provider = layer.dataProvider()

    flog(f"tx_serial_update_path_parity run start: trace_id={ctx.trace_id}",
         "INFO")

    feat = next(layer.getFeatures())
    fid = feat.id()
    old_attrs = serialize_attributes(feat, names)

    # ---- l'utilisateur modifie tout -------------------------------------
    idx = {n: layer.fields().indexOf(n) for n in names}
    provider.changeAttributeValues({fid: {
        idx["tags"]: list(_TAGS_NEW),
        idx["nums"]: list(_NUMS_NEW),
        idx["bin"]: QByteArray(_BLOB_NEW),
        idx["dtm"]: QDateTime(QDate(2020, 6, 1), QTime(8, 15, 0)),
        idx["nom"]: "APRES",
    }})
    layer.reload()
    feat2 = next(layer.getFeatures())
    new_attrs = serialize_attributes(feat2, names)
    ctx.data["state_after_edit"] = _snapshot(layer, names)

    delta = compute_update_delta(old_attrs, new_attrs, names)
    ctx.data["delta"] = delta
    ctx.data["delta_fields"] = sorted(
        (__import__("json").loads(delta).get("changed_only") or {}).keys()
    ) if delta else []

    identity_json = compute_feature_identity(layer, feat2)
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    row = (
        _PROJ_FP, _DS_FP, layer.id(), _LAYER_NAME, "ogr",
        identity_json, "UPDATE", delta, None, "Point", "EPSG:4326",
        serialize_field_schema(layer.fields()), "tester", None,
        datetime.now(timezone.utc).isoformat(), None,
        compute_entity_fingerprint(identity_json), 5, None, None,
    )
    cur = conn.execute(sql, row)
    eid = cur.lastrowid
    conn.commit()
    event = get_event_by_id(conn, eid)

    # ---- sens 1 : le rewind ---------------------------------------------
    ctx.data["rewind_result"] = restore_updated_feature(layer, event)
    layer.reload()
    ctx.data["state_after_rewind"] = _snapshot(layer, names)

    # ---- sens 2 : annuler la restauration -------------------------------
    report = undo_restore_batch(layer, [event])
    ctx.data["undo_report"] = {
        "succeeded": list(report.succeeded), "failed": dict(report.failed),
    }
    layer.reload()
    ctx.data["state_after_undo"] = _snapshot(layer, names)

    flog(
        f"tx_serial_update_path_parity run end: trace_id={ctx.trace_id} "
        f"rewind={ctx.data['rewind_result']} undo={ctx.data['undo_report']} "
        f"bin_after_rewind={_short(ctx.data['state_after_rewind'].get('bin'))} "
        f"bin_after_undo={_short(ctx.data['state_after_undo'].get('bin'))}",
        "INFO",
    )

    ctx.data["layer"] = None
    conn.close()
    ctx.data["conn"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    before = ctx.data.get("state_before") or {}
    edited = ctx.data.get("state_after_edit") or {}
    rewound = ctx.data.get("state_after_rewind") or {}
    undone = ctx.data.get("state_after_undo") or {}
    rewind_res = ctx.data.get("rewind_result") or {}
    undo_rep = ctx.data.get("undo_report") or {}

    # ===== Sanite du dispositif ==========================================
    out.append((
        "fixture_lists_are_json_backed",
        ctx.data.get("subtypes", {}).get("tags") == "JSON"
        and ctx.data.get("subtypes", {}).get("nums") == "JSON",
        f"subtypes={ctx.data.get('subtypes')} : GPKG doit exposer les champs "
        f"liste en sous-type JSON, sinon le scenario ne teste pas les listes",
    ))
    out.append((
        "delta_covers_every_edited_field",
        set(ctx.data.get("delta_fields") or []) == {
            "tags", "nums", "bin", "dtm", "nom"},
        f"le delta ne couvre que {ctx.data.get('delta_fields')} : les cinq "
        f"champs ont ete modifies, tous doivent etre journalises sinon le "
        f"reste de la mesure ne veut rien dire",
    ))
    out.append((
        "rewind_reported_success",
        rewind_res.get("success") is True,
        f"restore_updated_feature a repondu {rewind_res}",
    ))
    out.append((
        "undo_reported_success",
        bool(undo_rep.get("succeeded")) and not undo_rep.get("failed"),
        f"undo_restore_batch a repondu {undo_rep}",
    ))

    # ===== Sens 1 : le rewind doit rendre l'etat AVANT ===================
    for field, label in (("nom", "texte"), ("tags", "liste de chaines"),
                         ("nums", "liste d'entiers")):
        out.append((
            f"rewind_restores_{field}",
            rewound.get(field) == before.get(field),
            f"champ {field} ({label}) non rendu par le rewind : "
            f"attendu={_short(before.get(field))} obtenu={_short(rewound.get(field))}",
        ))
    out.append((
        "rewind_restores_bin",
        rewound.get("bin") == before.get("bin"),
        f"LE REWIND DETRUIT LE CHAMP BINAIRE : attendu="
        f"{_short(before.get('bin'))} obtenu={_short(rewound.get('bin'))}. "
        f"C'est la fonction principale du plugin, sur son chemin principal : "
        f"l'utilisateur revient en arriere et perd sa piece jointe.",
    ))
    out.append((
        "rewind_restores_dtm",
        rewound.get("dtm") == before.get("dtm"),
        f"horodatage deplace par le rewind : attendu="
        f"{_short(before.get('dtm'))} obtenu={_short(rewound.get('dtm'))}",
    ))

    # ===== Sens 2 : annuler doit rendre l'etat APRES =====================
    for field, label in (("nom", "texte"), ("tags", "liste de chaines"),
                         ("nums", "liste d'entiers")):
        out.append((
            f"undo_restores_{field}",
            undone.get(field) == edited.get(field),
            f"champ {field} ({label}) non rendu par l'annulation : "
            f"attendu={_short(edited.get(field))} obtenu={_short(undone.get(field))}",
        ))
    out.append((
        "undo_restores_bin",
        undone.get("bin") == edited.get("bin"),
        f"L'ANNULATION DETRUIT AUSSI LE CHAMP BINAIRE : attendu="
        f"{_short(edited.get('bin'))} obtenu={_short(undone.get('bin'))}. "
        f"L'utilisateur qui se ravise et annule sa restauration ne recupere "
        f"pas sa donnee : il obtient une deuxieme etiquette base64. La "
        f"corruption est symetrique, donc aucun aller-retour ne repare rien.",
    ))
    out.append((
        "undo_restores_dtm",
        undone.get("dtm") == edited.get("dtm"),
        f"horodatage deplace par l'annulation : attendu="
        f"{_short(edited.get('dtm'))} obtenu={_short(undone.get('dtm'))}. "
        f"L'offset local est retranche a CHAQUE ecriture, dans les deux "
        f"sens : un aller-retour eloigne l'entite deux fois de son etat "
        f"reel au lieu de l'y ramener.",
    ))

    # ===== Le point de passage unique ====================================
    src = (_PLUGIN_ROOT / "core" / "serialization.py").read_text(
        encoding="utf-8", errors="replace")
    body = src.split("def iter_mapped_attributes")[-1]
    out.append((
        "single_write_choke_point_decodes",
        "deserialize" in body,
        "les deux sens (restore_updated_feature et _undo_update_restore) "
        "traversent iter_mapped_attributes : un decodage typé pose la, "
        "guide par field_schema_json, corrige les cinq sites d'ecriture "
        "d'un coup. Aucun autre endroit ne peut le faire une seule fois.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"restore_updated\[\d+\]: OK fid=",
        name="rewind_logged_ok",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_update_path_parity.*trace_id={ctx.trace_id}",
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
