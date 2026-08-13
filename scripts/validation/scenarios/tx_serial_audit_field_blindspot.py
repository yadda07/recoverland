"""tx_serial_audit_field_blindspot - RecoverLand validation runtime.

Invariant: BL-SER-P0-03 (le filtre de champs d'audit ne doit jamais
avaler une colonne metier, et surtout pas la cle primaire).
Cause racine: `core/audit_field_policy.is_layer_audit_field` classe comme
"metadonnee d'audit" tout nom qui, une fois debarrasse de ses separateurs
et de ses accents, tombe dans une liste fermee - laquelle contient ``gid``
et ``username``.

Le trou que ce scenario prouve
------------------------------
``gid`` est le nom de cle primaire produit par defaut par shp2pgsql et par
la quasi-totalite des imports PostGIS ; c'est aussi l'exemple utilise par
la docstring de `identity.compute_feature_identity` :

    Returns JSON string like: {"fid": 42, "pk_field": "gid", "pk_value": 42}

Le plugin identifie donc les entites par ``gid``, et refuse en meme temps
de le restaurer. Sur un GeoPackage dont la colonne FID s'appelle ``gid``
(equivalent local d'une table PostGIS importee), la chaine complete
donne :

    capture   : le journal contient bien gid=4242, username='DUPONT Jean'
    mapping   : safe_field_mapping renvoie bien 'gid' -> 'gid'
    ecriture  : iter_mapped_attributes ecarte gid et username
    resultat  : l'entite reinseree porte gid=4243 (FID neuf attribue par
                OGR) et username=NULL

Deux degats, dont un irreversible :

  1. Perte de donnee metier. Le titulaire (``username``) et la date
     metier sont vides. Le journal les contenait : l'information n'est pas
     perdue a la capture, elle est jetee a l'ecriture.
  2. Perte d'IDENTITE, en cascade. L'entite ne repond plus a sa propre
     cle primaire : `_find_target_feature` interroge gid=4242 et ne trouve
     rien. TOUS les autres evenements du journal qui designent cette
     entite deviennent irrecuperables. Une seule restauration suffit a
     couper definitivement l'entite de son historique.

Et en amont, `compute_update_delta` ignore ces memes champs : modifier un
``gid`` ou un ``username`` n'ecrit AUCUN evenement. La modification est
invisible, donc non restaurable - il n'y a meme pas de trace a rejouer.

Ce que la politique protege legitimement (verifie ici aussi) : un vrai
champ d'horodatage de ligne comme ``date_modif``, alimente par un trigger,
doit bien rester hors du delta et hors de la restauration, sinon chaque
rewind se battrait contre le trigger.

Scenario (vrai GeoPackage en dossier temporaire, colonne FID = gid):
    setup: parcelles(gid PK, username, nom, surface,
           date_modification_parcelle, date_modif) + 1 entite
    run:
        - capture reelle (serialize_attributes / safe_field_mapping)
        - evenement DELETE dans un vrai journal SQLite
        - suppression puis restore_deleted_feature()
        - relecture + tentative de re-resolution par la cle primaire
          d'origine
        - deltas sur gid et username
    Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_serial_audit_field_blindspot"
INVARIANT = "BL-SER-P0-03"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_LAYER_NAME = "parcelles"
_DS_FP = "tx_safb_datasource"
_PROJ_FP = "tx_safb_project"

_GID = 4242
_USERNAME = "DUPONT Jean"
_NOM = "PARCELLE AB-12"
_SURFACE = 1234.5
_DATE_METIER = "2024-06-01"
_DATE_AUDIT = "2026-08-13T09:00:00"

# La colonne FID du GeoPackage s'appelle gid : c'est la forme locale d'une
# table PostGIS importee par shp2pgsql, cas d'usage central du plugin.
_URI = ("Point?crs=EPSG:4326"
        "&field=username:string(0)"
        "&field=nom:string(0)"
        "&field=surface:double"
        "&field=date_modification_parcelle:string(0)"
        "&field=date_modif:string(0)")


def _make_gpkg(tmpdir: str):
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(_URI, "seed", "memory")
    if not mem.isValid():
        raise RuntimeError("memory schema layer invalid")
    path = str(Path(tmpdir) / "parcelles.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = _LAYER_NAME
    opts.layerOptions = ["FID=gid"]
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername={_LAYER_NAME}", _LAYER_NAME, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"gpkg layer invalid: {path}")
    return layer


def _seed(layer) -> None:
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    feat = QgsFeature(layer.fields())
    feat.setAttribute("gid", _GID)
    feat.setAttribute("username", _USERNAME)
    feat.setAttribute("nom", _NOM)
    feat.setAttribute("surface", _SURFACE)
    feat.setAttribute("date_modification_parcelle", _DATE_METIER)
    feat.setAttribute("date_modif", _DATE_AUDIT)
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(3.0, 45.0)))
    ok, _ = layer.dataProvider().addFeatures([feat])
    if not ok:
        raise RuntimeError(f"seed failed: {layer.dataProvider().errors()}")
    layer.reload()


def _snapshot(layer) -> dict:
    feat = next(layer.getFeatures(), None)
    if feat is None:
        return {}
    return {f.name(): feat[f.name()] for f in layer.fields()}


def setup(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.sqlite_schema import initialize_schema

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_safb_")
    ctx.data["tmpdir"] = tmpdir
    layer = _make_gpkg(tmpdir)
    _seed(layer)
    ctx.data["layer"] = layer
    ctx.data["pk_indexes"] = list(layer.dataProvider().pkAttributeIndexes())
    ctx.data["before"] = _snapshot(layer)

    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    ctx.data["conn"] = conn

    flog(
        f"tx_serial_audit_field_blindspot setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} pk_indexes={ctx.data['pk_indexes']} "
        f"before={ctx.data['before']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.serialization import (
        serialize_attributes, serialize_field_schema, build_full_snapshot,
        compute_update_delta, iter_mapped_attributes,
    )
    from recoverland.core.identity import (
        compute_feature_identity, compute_entity_fingerprint,
    )
    from recoverland.core.geometry_utils import geometry_to_wkb
    from recoverland.core.schema_drift import safe_field_mapping
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.search_service import (
        get_event_by_id, reconstruct_attributes,
    )
    from recoverland.core.restore_service import (
        restore_deleted_feature, _find_target_feature,
    )
    from recoverland.core.audit_field_policy import is_layer_audit_field

    layer = ctx.data["layer"]
    conn = ctx.data["conn"]
    provider = layer.dataProvider()
    names = [f.name() for f in layer.fields()]

    flog(f"tx_serial_audit_field_blindspot run start: trace_id={ctx.trace_id}",
         "INFO")

    # ---- classification brute des noms de colonnes ----------------------
    ctx.data["classification"] = {
        name: is_layer_audit_field(name)
        for name in ("gid", "GID", "g_i_d", "username", "USERNAME",
                     "user_name", "date_modification_parcelle", "date_modif",
                     "nom", "surface", "fid")
    }

    # ---- capture reelle -------------------------------------------------
    feat = next(layer.getFeatures())
    attrs = serialize_attributes(feat, names)
    identity_json = compute_feature_identity(layer, feat)
    ctx.data["identity_json"] = identity_json
    ctx.data["captured_attrs"] = dict(attrs)

    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    row = (
        _PROJ_FP, _DS_FP, layer.id(), _LAYER_NAME, "ogr",
        identity_json, "DELETE", build_full_snapshot(attrs),
        geometry_to_wkb(feat.geometry()), "Point", "EPSG:4326",
        serialize_field_schema(layer.fields()), "tester", None,
        datetime.now(timezone.utc).isoformat(), None,
        compute_entity_fingerprint(identity_json), 5, None, None,
    )
    cur = conn.execute(sql, row)
    eid = cur.lastrowid
    conn.commit()

    event = get_event_by_id(conn, eid)
    ctx.data["journal_attrs"] = dict(reconstruct_attributes(event))
    ctx.data["mapping"] = dict(safe_field_mapping(event))

    # Ce que le chemin d'ecriture retient reellement du journal.
    fields = layer.fields()
    written = {}
    for idx, value in iter_mapped_attributes(
            ctx.data["mapping"], ctx.data["journal_attrs"], fields):
        written[fields.at(idx).name()] = value
    ctx.data["written_by_iter"] = written

    # ---- suppression puis restauration reelle ---------------------------
    provider.deleteFeatures([feat.id()])
    layer.reload()
    ctx.data["count_after_delete"] = layer.featureCount()

    ctx.data["restore_result"] = restore_deleted_feature(layer, event)
    layer.reload()
    ctx.data["after"] = _snapshot(layer)

    # ---- l'entite repond-elle encore a sa cle primaire d'origine ? ------
    ctx.data["refound_fid"] = _find_target_feature(
        layer, json.loads(identity_json))

    # ---- deltas sur les champs avales -----------------------------------
    ctx.data["delta_gid"] = compute_update_delta(
        {"gid": _GID, "nom": _NOM}, {"gid": 9999, "nom": _NOM}, ["gid"])
    ctx.data["delta_username"] = compute_update_delta(
        {"username": _USERNAME}, {"username": "MARTIN Paul"}, ["username"])
    ctx.data["delta_nom"] = compute_update_delta(
        {"nom": _NOM}, {"nom": "PARCELLE AB-13"}, ["nom"])

    flog(
        f"tx_serial_audit_field_blindspot run end: trace_id={ctx.trace_id} "
        f"after={ctx.data['after']} refound_fid={ctx.data['refound_fid']} "
        f"delta_gid={ctx.data['delta_gid']} "
        f"delta_username={ctx.data['delta_username']}",
        "INFO",
    )

    ctx.data["layer"] = None
    conn.close()
    ctx.data["conn"] = None
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    cls = ctx.data.get("classification") or {}
    before = ctx.data.get("before") or {}
    after = ctx.data.get("after") or {}
    journal = ctx.data.get("journal_attrs") or {}
    written = ctx.data.get("written_by_iter") or {}
    result = ctx.data.get("restore_result") or {}

    # ===== Sanite du dispositif ==========================================
    out.append((
        "fixture_pk_is_named_gid",
        ctx.data.get("pk_indexes") == [0] and "gid" in before,
        f"pk_indexes={ctx.data.get('pk_indexes')} champs={list(before)} : le "
        f"scenario a besoin d'une couche dont la cle primaire s'appelle gid, "
        f"cas produit par tout import shp2pgsql",
    ))
    out.append((
        "identity_uses_gid_as_primary_key",
        '"pk_field": "gid"' in (ctx.data.get("identity_json") or ""),
        f"identity={ctx.data.get('identity_json')!r} : le plugin doit bien "
        f"identifier l'entite par gid, sinon la demonstration ne porte pas",
    ))
    out.append((
        "feature_was_really_deleted",
        ctx.data.get("count_after_delete") == 0,
        f"count_after_delete={ctx.data.get('count_after_delete')} attendu=0",
    ))
    out.append((
        "restore_reported_success",
        result.get("success") is True,
        f"restore_deleted_feature a repondu {result} : tout ce qui suit "
        f"decrit ce que l'utilisateur obtient quand le plugin annonce "
        f"avoir reussi",
    ))

    # ===== Classification des noms de colonnes ===========================
    out.append((
        "gid_is_not_an_audit_field",
        cls.get("gid") is False,
        f"is_layer_audit_field('gid')={cls.get('gid')}. gid est le nom de cle "
        f"primaire genere par defaut par shp2pgsql et l'exemple meme de la "
        f"docstring de compute_feature_identity. Le classer en metadonnee "
        f"d'audit revient a exclure la cle primaire de toute restauration.",
    ))
    out.append((
        "username_is_not_an_audit_field",
        cls.get("username") is False and cls.get("user_name") is False,
        f"is_layer_audit_field('username')={cls.get('username')} "
        f"'user_name'={cls.get('user_name')}. Sur une couche de concessions, "
        f"de baux ou de comptes, 'username' est le titulaire : une donnee "
        f"metier, jamais restauree.",
    ))
    out.append((
        "normalisation_does_not_swallow_separated_names",
        cls.get("g_i_d") is False,
        f"is_layer_audit_field('g_i_d')={cls.get('g_i_d')} : la normalisation "
        f"supprime accents et separateurs avant de comparer, donc 'g_i_d', "
        f"'G-I-D' et 'gID' tombent tous dans le meme piege que 'gid'. Aucune "
        f"convention de nommage ne met une colonne metier a l'abri.",
    ))
    out.append((
        "business_columns_stay_out_of_the_policy",
        cls.get("nom") is False and cls.get("surface") is False
        and cls.get("fid") is False,
        f"des colonnes ordinaires sont classees en audit : "
        f"nom={cls.get('nom')} surface={cls.get('surface')} "
        f"fid={cls.get('fid')}",
    ))
    out.append((
        "policy_still_catches_a_real_audit_column",
        cls.get("date_modif") is True,
        f"is_layer_audit_field('date_modif')={cls.get('date_modif')} : la "
        f"politique doit continuer d'ecarter un vrai horodatage de ligne "
        f"alimente par trigger, sinon chaque rewind se battrait contre le "
        f"trigger. C'est sa raison d'etre et elle doit survivre au correctif.",
    ))

    # ===== L'information EST dans le journal =============================
    out.append((
        "journal_did_capture_gid_and_username",
        journal.get("gid") == _GID and journal.get("username") == _USERNAME,
        f"le journal relu contient gid={journal.get('gid')!r} "
        f"username={journal.get('username')!r} : la capture fonctionne. "
        f"Le defaut est donc uniquement dans l'ecriture, et le correctif ne "
        f"demande aucune remigration du journal existant.",
    ))
    out.append((
        "write_path_keeps_gid_and_username",
        "gid" in written and "username" in written,
        f"iter_mapped_attributes ne retient que {sorted(written)} : gid et "
        f"username sont ecartes juste avant l'ecriture, alors que "
        f"safe_field_mapping les avait bien apparies "
        f"({sorted(ctx.data.get('mapping') or {})}).",
    ))

    # ===== Ce que l'utilisateur retrouve dans sa couche ==================
    out.append((
        "restored_gid_is_the_original_one",
        after.get("gid") == before.get("gid"),
        f"la cle primaire a change : avant gid={before.get('gid')!r}, apres "
        f"gid={after.get('gid')!r}. OGR a attribue un FID neuf parce que la "
        f"valeur d'origine n'a pas ete reecrite.",
    ))
    out.append((
        "restored_username_is_not_lost",
        after.get("username") == before.get("username"),
        f"titulaire perdu : avant={before.get('username')!r} "
        f"apres={after.get('username')!r}. La valeur etait dans le journal ; "
        f"elle a ete jetee a l'ecriture.",
    ))
    out.append((
        "restored_business_date_is_not_lost",
        after.get("date_modification_parcelle")
        == before.get("date_modification_parcelle"),
        f"date metier perdue : avant="
        f"{before.get('date_modification_parcelle')!r} apres="
        f"{after.get('date_modification_parcelle')!r}. Le filtre s'applique "
        f"par prefixe : toute colonne dont le nom commence par un jeton "
        f"d'audit est avalee, quelle que soit la suite.",
    ))
    out.append((
        "restored_ordinary_fields_survive",
        after.get("nom") == before.get("nom")
        and after.get("surface") == before.get("surface"),
        f"les colonnes ordinaires ne reviennent pas non plus : "
        f"nom {before.get('nom')!r}->{after.get('nom')!r} "
        f"surface {before.get('surface')!r}->{after.get('surface')!r}",
    ))
    out.append((
        "true_audit_column_stays_excluded",
        after.get("date_modif") is None,
        f"date_modif={after.get('date_modif')!r} : un vrai champ d'audit de "
        f"ligne doit rester non restaure (le trigger le reecrira)",
    ))

    # ===== Le degat irreversible : l'entite perd son identite ============
    out.append((
        "entity_still_answers_to_its_primary_key",
        ctx.data.get("refound_fid") is not None,
        f"_find_target_feature(identity d'origine) renvoie "
        f"{ctx.data.get('refound_fid')!r}. L'entite ne repond plus a sa "
        f"propre cle primaire : TOUS les autres evenements du journal qui la "
        f"designent deviennent irrecuperables. Une seule restauration suffit "
        f"a couper definitivement l'entite de son historique.",
    ))

    # ===== En amont : la modification n'est meme pas journalisee =========
    out.append((
        "a_change_of_gid_is_journalled",
        ctx.data.get("delta_gid") is not None,
        f"compute_update_delta sur gid renvoie {ctx.data.get('delta_gid')!r} : "
        f"changer une cle primaire n'ecrit AUCUN evenement. Il n'y a donc "
        f"rien a restaurer plus tard, et rien a afficher dans l'historique.",
    ))
    out.append((
        "a_change_of_username_is_journalled",
        ctx.data.get("delta_username") is not None,
        f"compute_update_delta sur username renvoie "
        f"{ctx.data.get('delta_username')!r} : le changement de titulaire est "
        f"invisible pour l'audit.",
    ))
    out.append((
        "a_change_of_an_ordinary_field_is_journalled",
        ctx.data.get("delta_nom") is not None,
        f"temoin : delta sur 'nom' = {ctx.data.get('delta_nom')!r} (doit "
        f"exister, sinon le dispositif de mesure est casse)",
    ))

    # ===== Preuve de mecanisme ===========================================
    src_identity = (_PLUGIN_ROOT / "core" / "identity.py").read_text(
        encoding="utf-8", errors="replace")
    src_policy = (_PLUGIN_ROOT / "core" / "audit_field_policy.py").read_text(
        encoding="utf-8", errors="replace")
    contradiction = ('"pk_field": "gid"' in src_identity
                     and '"gid"' in src_policy)
    out.append((
        "no_contradiction_between_identity_and_policy",
        not contradiction,
        "identity.py documente gid comme exemple de cle primaire "
        "('pk_field: gid') pendant que audit_field_policy.py le liste dans "
        "_LAYER_AUDIT_FIELD_NAMES. Les deux ne peuvent pas etre vrais en "
        "meme temps : ou gid identifie l'entite, ou c'est du bruit d'audit.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        r"restore_deleted\[\d+\]: OK new_fid=",
        name="restore_logged_ok",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_audit_field_blindspot.*trace_id={ctx.trace_id}",
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
