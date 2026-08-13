"""
stress_edit.py  -  Stress-test éditions aléatoires pour RecoverLand Rewind
==========================================================================
Lance depuis la console Python QGIS :

    exec(open('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default/python/plugins/recoverland/scripts/stress_edit.py').read())

Ou copie-colle le contenu dans la console.

Paramétrage rapide (modifier les constantes ci-dessous) :
    ROUNDS          = nombre de passes (chaque passe = 1 session d'édition par couche)
    MAX_LAYERS      = nombre max de couches traitées par passe
    MAX_INSERTS     = insertions max par couche par passe
    MAX_UPDATES     = updates (attrs + geom) max par couche par passe
    MAX_DELETES     = suppressions max par couche par passe
    DELETE_CAP_PCT  = % max de features supprimables (sécurité)
    GEOM_JITTER_M   = amplitude du déplacement géométrique en mètres
"""

import json
import os
import random
import math
import uuid
from datetime import datetime

from qgis.core import (
    QgsProject,
    QgsVectorLayer,
    QgsFeature,
    QgsFeatureRequest,
    QgsGeometry,
    QgsPointXY,
    QgsCoordinateTransform,
    Qgis,
)
from qgis.PyQt.QtCore import QVariant
from qgis.utils import iface

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# --- Passe de DIAGNOSTIC, pas de demonstration -----------------------------
# Ces valeurs isolent UNE hypothese, elles ne menagent pas le plugin.
#
# Les logs du 13/08 montrent que le rewind invalide ses propres cibles: en
# supprimant une entite il fait renumeroter les FID par le provider, si bien
# que les compensations suivantes visent un FID qui ne designe plus la meme
# entite ("post_geom_mismatch", "FID occupant unverifiable"). MAX_DELETES = 0
# retire cette seule variable. Si le rewind devient exact, l'hypothese est
# confirmee et le correctif est cible. S'il reste faux, la cause est ailleurs
# et il faut chercher autre chose.
#
# CE REGLAGE N'EST PAS UN VERDICT. Un rewind exact sans suppressions ne dit
# rien du cas reel, ou l'utilisateur supprime. Une fois l'hypothese tranchee,
# REMETTRE MAX_DELETES a 8 et relancer: c'est ce chiffre-la qui compte.
ROUNDS = 1
MAX_LAYERS = 2
MAX_INSERTS = 5
MAX_UPDATES = 8
MAX_DELETES = 0        # <-- variable isolee. Remettre a 8 apres diagnostic.
DELETE_CAP_PCT = 15
GEOM_JITTER_M = 100.0
DRY_RUN = False
# True = snapshot l'état actuel même s'il contient des 'stress_*' (test
# de la capacité de rewind sur baseline pollué). False = abort si pollué.
ALLOW_DIRTY_BASELINE = True

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
_LOG_LINES = []
_RUN_GUARD_KEY = "_STRESS_EDIT_LAST_RUN_TS"


def _log(level, msg):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {level:7s}  {msg}"
    _LOG_LINES.append(line)
    print(line)


def _info(msg):
    _log("INFO", msg)


def _warn(msg):
    _log("WARNING", msg)


def _detail(msg):
    _log("DETAIL", msg)


# ---------------------------------------------------------------------------
# SNAPSHOT (ground-truth avant stress)
# ---------------------------------------------------------------------------

def _snapshot_dir():
    """Répertoire de sauvegarde du snapshot : scripts/ du plugin (lisible par Cascade)."""
    d = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(d, exist_ok=True)
    return d


def _feat_to_dict(feat):
    """Sérialise une feature en dict JSON-compatible."""
    geom = feat.geometry()
    attrs = {}
    for field in feat.fields():
        val = feat[field.name()]
        if hasattr(val, 'isNull') and val.isNull():
            attrs[field.name()] = None
        elif isinstance(val, (int, float, str, bool, type(None))):
            attrs[field.name()] = val
        else:
            attrs[field.name()] = str(val)
    return {
        "fid": feat.id(),
        "attrs": attrs,
        "geom_wkt": geom.asWkt(6) if geom and not geom.isNull() else None,
    }


def _save_snapshot(layers, canvas_extent, canvas_crs, config):
    """Sauvegarde l'état courant de toutes les couches dans un JSON.

    Retourne le chemin du fichier créé.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snap = {
        "snapshot_time": datetime.now().isoformat(timespec="seconds"),
        "config": config,
        "layers": {},
    }
    for layer in layers:
        name = layer.name()
        features = {}
        for feat in layer.getFeatures():
            d = _feat_to_dict(feat)
            features[str(d["fid"])] = d
        snap["layers"][name] = {
            "source": layer.source(),
            "feature_count": len(features),
            "features": features,
        }
    d = _snapshot_dir()
    path = os.path.join(d, f"stress_snapshot_{ts}.json")
    latest = os.path.join(d, "stress_snapshot_latest.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(snap, fh, ensure_ascii=False, indent=2)
    with open(latest, "w", encoding="utf-8") as fh:
        json.dump(snap, fh, ensure_ascii=False, indent=2)
    _info(f"Snapshot sauvegardé : {path}")
    _info(f"  {len(snap['layers'])} couches, {sum(v['feature_count'] for v in snap['layers'].values())} features total")
    return path


def _get_canvas_extent():
    """Retourne l'emprise courante du canevas en CRS du projet."""
    canvas = iface.mapCanvas()
    return canvas.extent(), canvas.mapSettings().destinationCrs()


def _get_visible_fids(layer, canvas_extent, canvas_crs):
    """Retourne les FIDs de la couche qui intersectent l'emprise visible."""
    layer_crs = layer.crs()
    if layer_crs != canvas_crs:
        xform = QgsCoordinateTransform(
            canvas_crs, layer_crs, QgsProject.instance()
        )
        extent_in_layer = xform.transformBoundingBox(canvas_extent)
    else:
        extent_in_layer = canvas_extent

    request = QgsFeatureRequest()
    request.setFilterRect(extent_in_layer)
    request.setNoAttributes()
    return [f.id() for f in layer.getFeatures(request)]


def _get_editable_layers():
    """Retourne les couches vectorielles éditables (non raster, non WMS)."""
    layers = []
    for layer in QgsProject.instance().mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue
        dp = layer.dataProvider()
        if dp is None:
            continue
        caps = dp.capabilities()
        can_edit = (
            caps & dp.AddFeatures
            and caps & dp.ChangeAttributeValues
            and caps & dp.DeleteFeatures
        )
        if not can_edit:
            continue
        if layer.featureCount() == 0 and not (caps & dp.AddFeatures):
            continue
        layers.append(layer)
    return layers


def _ensure_pristine_state(layers):
    """Cleanup auto avant snapshot.

    1. Rollback les buffers d'édition non-commités (cas simple).
    2. Scan les attributs texte de chaque couche pour 'stress_*'.
       Si trouvé, la couche a été modifiée par un run précédent qui
       n'a pas été rewind → retourne False (abort).

    Retourne True si l'état est pristine après cleanup, False sinon.
    """
    n_rollback = 0
    for lyr in layers:
        if lyr.isEditable():
            try:
                if lyr.rollBack():
                    n_rollback += 1
                    _info(f"  cleanup: {lyr.name()} rollback buffer OK")
                else:
                    _warn(f"  cleanup: {lyr.name()} rollback REFUSÉ")
            except Exception as exc:
                _warn(f"  cleanup: {lyr.name()} rollback err={exc}")
    if n_rollback:
        _info(f"cleanup: {n_rollback} buffer(s) rollback")

    contaminated = []
    for lyr in layers:
        n_dirty = _count_stress_attrs(lyr, sample_limit=200)
        if n_dirty > 0:
            contaminated.append((lyr.name(), n_dirty))

    if contaminated:
        _warn(f"cleanup: {len(contaminated)} couche(s) contaminée(s) par 'stress_*' "
              f"→ purge in-place sans pollution du journal")
        for name, n in contaminated:
            _info(f"  - {name}: {n} feature(s) à purger")
        scrubbed = _scrub_stress_values(layers)
        if scrubbed["fail_layers"]:
            _warn(f"cleanup: échec sur {len(scrubbed['fail_layers'])} couche(s): "
                  f"{', '.join(scrubbed['fail_layers'])}")
            if not ALLOW_DIRTY_BASELINE:
                return False
            _warn("ALLOW_DIRTY_BASELINE=True → on continue malgré l'échec partiel")
        _info(f"cleanup: purgé {scrubbed['n_feats']} feature(s) "
              f"sur {scrubbed['n_layers']} couche(s) en {scrubbed['elapsed_ms']}ms")
        return True

    _info(f"cleanup: {len(layers)} couche(s) vérifiée(s) pristine ✓")
    return True


def _get_recoverland_tracker():
    """Retourne l'EditSessionTracker du plugin RecoverLand, ou None."""
    try:
        import qgis.utils
        for name, plugin in dict(qgis.utils.plugins).items():
            if name == 'recoverland' or type(plugin).__name__ == 'RecoverPlugin':
                tracker = getattr(plugin, '_tracker', None)
                if tracker is not None:
                    return tracker
    except Exception as exc:
        _warn(f"cleanup: tracker introspect err={exc}")
    return None


def _scrub_stress_values(layers):
    """Purge in-place toutes les valeurs 'stress_*' des couches éditables.

    Performance: utilise un QgsFeatureRequest avec expression filter pour
    ne récupérer QUE les features pollées (au lieu d'itérer toute la couche).
    Suppression du tracker RecoverLand pendant l'opération pour ne pas
    écrire d'events parasites dans le journal d'audit.
    """
    import sys as _sys
    import time as _time
    from qgis.core import QgsFeatureRequest, QgsExpression

    t0 = _time.time()
    tracker = _get_recoverland_tracker()
    if tracker is not None:
        tracker.suppress()
        _info("cleanup: RecoverLand tracker suppressed")
    else:
        _warn("cleanup: RecoverLand tracker introuvable → cleanup écrira des events")
    _sys.stdout.flush()

    n_feats_total = 0
    n_layers_total = 0
    fail_layers = []
    try:
        for lyr in layers:
            t_layer = _time.time()
            field_names = [f.name() for f in lyr.fields()
                           if f.typeName().lower() in ("string", "text", "varchar")]
            if not field_names:
                continue
            # Filtre côté provider: ne récupère que les features avec au moins un
            # champ texte commençant par 'stress_'. Évite d'itérer la couche entière.
            expr_parts = [f'substr("{fname}", 1, 7) = \'stress_\''
                          for fname in field_names]
            expr_str = " OR ".join(expr_parts)
            req = QgsFeatureRequest(QgsExpression(expr_str))
            req.setSubsetOfAttributes(field_names, lyr.fields())
            try:
                req.setFlags(QgsFeatureRequest.NoGeometry)
            except Exception:
                pass
            dirty_feats = []
            for feat in lyr.getFeatures(req):
                changes = {}
                for fname in field_names:
                    try:
                        val = feat[fname]
                    except (KeyError, IndexError):
                        continue
                    if isinstance(val, str) and val.startswith("stress_"):
                        idx = lyr.fields().indexOf(fname)
                        if idx >= 0:
                            changes[idx] = None
                if changes:
                    dirty_feats.append((feat.id(), changes))
            if not dirty_feats:
                continue
            if not lyr.startEditing():
                fail_layers.append(lyr.name())
                _warn(f"  scrub {lyr.name()}: startEditing REFUSED")
                _sys.stdout.flush()
                continue
            ok = True
            for fid, changes in dirty_feats:
                for idx, value in changes.items():
                    if not lyr.changeAttributeValue(fid, idx, value):
                        ok = False
            if not ok or not lyr.commitChanges():
                lyr.rollBack()
                fail_layers.append(lyr.name())
                _warn(f"  scrub {lyr.name()}: commit FAILED")
                _sys.stdout.flush()
                continue
            n_feats_total += len(dirty_feats)
            n_layers_total += 1
            ms = int((_time.time() - t_layer) * 1000)
            _info(f"  scrubbed {lyr.name()}: {len(dirty_feats)} feature(s) ({ms}ms)")
            _sys.stdout.flush()
    finally:
        if tracker is not None:
            tracker.unsuppress()
            _info("cleanup: RecoverLand tracker unsuppressed")
        _sys.stdout.flush()

    elapsed_ms = int((_time.time() - t0) * 1000)
    return {
        "n_feats": n_feats_total,
        "n_layers": n_layers_total,
        "fail_layers": fail_layers,
        "elapsed_ms": elapsed_ms,
    }


def _count_stress_attrs(layer, sample_limit=200):
    """Compte les valeurs d'attributs préfixées 'stress_' sur les N premières
    features. Limite stricte pour rester rapide sur grosses couches."""
    field_names = [f.name() for f in layer.fields()
                   if f.typeName().lower() in ("string", "text", "varchar")]
    if not field_names:
        return 0
    n = 0
    seen = 0
    for feat in layer.getFeatures():
        seen += 1
        if seen > sample_limit:
            break
        for fname in field_names:
            try:
                val = feat[fname]
            except (KeyError, IndexError):
                continue
            if isinstance(val, str) and val.startswith("stress_"):
                n += 1
                break  # une seule détection par feature suffit
    return n


def _random_value_for_field(field):
    """Génère une valeur aléatoire cohérente avec le type du champ."""
    ft = field.type()
    if ft in (QVariant.Int, QVariant.LongLong):
        return random.randint(1, 9999)
    if ft == QVariant.Double:
        return round(random.uniform(0.1, 999.9), 2)
    if ft == QVariant.String:
        max_len = field.length() if field.length() > 0 else 20
        tag = uuid.uuid4().hex[:min(8, max_len)]
        return f"stress_{tag}"
    if ft == QVariant.Bool:
        return random.choice([True, False])
    return None


def _pick_updatable_fields(layer):
    """Retourne les champs modifiables (non PK, non auto-increment)."""
    fields = []
    pk_indices = set(layer.dataProvider().pkAttributeIndexes())
    for i, field in enumerate(layer.fields()):
        if i in pk_indices:
            continue
        if field.isReadOnly():
            continue
        fields.append((i, field))
    return fields


def _jitter_point(pt, amplitude):
    """Déplace un point de façon aléatoire dans un rayon donné."""
    angle = random.uniform(0, 2 * math.pi)
    dist = random.uniform(1, amplitude)
    return QgsPointXY(
        pt.x() + dist * math.cos(angle),
        pt.y() + dist * math.sin(angle),
    )


def _jitter_geometry(geom, amplitude):
    """Perturbe la géométrie (point, ligne, polygone) de façon aléatoire."""
    if geom is None or geom.isEmpty():
        return None

    geom_type = geom.type()

    if geom_type == Qgis.GeometryType.Point:
        pt = geom.asPoint()
        new_pt = _jitter_point(pt, amplitude)
        return QgsGeometry.fromPointXY(new_pt)

    if geom_type == Qgis.GeometryType.Line:
        if geom.isMultipart():
            parts = geom.asMultiPolyline()
            new_parts = []
            for part in parts:
                idx = random.randint(0, max(0, len(part) - 1))
                new_part = list(part)
                new_part[idx] = _jitter_point(part[idx], amplitude)
                new_parts.append(new_part)
            return QgsGeometry.fromMultiPolylineXY(new_parts)
        else:
            pts = geom.asPolyline()
            if not pts:
                return None
            idx = random.randint(0, len(pts) - 1)
            new_pts = list(pts)
            new_pts[idx] = _jitter_point(pts[idx], amplitude)
            return QgsGeometry.fromPolylineXY(new_pts)

    if geom_type == Qgis.GeometryType.Polygon:
        if geom.isMultipart():
            parts = geom.asMultiPolygon()
            new_parts = []
            for poly in parts:
                new_rings = []
                for ring in poly:
                    idx = random.randint(0, max(0, len(ring) - 2))
                    new_ring = list(ring)
                    new_ring[idx] = _jitter_point(ring[idx], amplitude)
                    if len(new_ring) > 1 and new_ring[0] != new_ring[-1]:
                        new_ring[-1] = new_ring[0]
                    new_rings.append(new_ring)
                new_parts.append(new_rings)
            return QgsGeometry.fromMultiPolygonXY(new_parts)
        else:
            rings = geom.asPolygon()
            if not rings:
                return None
            new_rings = []
            for ring in rings:
                idx = random.randint(0, max(0, len(ring) - 2))
                new_ring = list(ring)
                new_ring[idx] = _jitter_point(ring[idx], amplitude)
                if len(new_ring) > 1 and new_ring[0] != new_ring[-1]:
                    new_ring[-1] = new_ring[0]
                new_rings.append(new_ring)
            return QgsGeometry.fromPolygonXY(new_rings)

    return None


def _clone_feature_nearby(layer, source_feat, amplitude):
    """Clone une feature existante avec géométrie décalée et attributs légèrement modifiés."""
    new_feat = QgsFeature(layer.fields())
    src_geom = source_feat.geometry()
    if src_geom and not src_geom.isEmpty():
        new_geom = _jitter_geometry(src_geom, amplitude * 2)
        if new_geom:
            new_feat.setGeometry(new_geom)

    updatable = _pick_updatable_fields(layer)
    for idx, field in updatable:
        src_val = source_feat.attribute(idx)
        if random.random() < 0.3:
            new_val = _random_value_for_field(field)
            if new_val is not None:
                new_feat.setAttribute(idx, new_val)
                continue
        new_feat.setAttribute(idx, src_val)

    return new_feat


# ---------------------------------------------------------------------------
# OPERATIONS
# ---------------------------------------------------------------------------
def do_inserts(layer, n, all_fids):
    """Insère n features clonées depuis des features existantes."""
    if not all_fids:
        _warn(f"  [{layer.name()}] INSERT skip: aucune feature source")
        return 0

    count = 0
    for _ in range(n):
        src_fid = random.choice(all_fids)
        src_feat = layer.getFeature(src_fid)
        if not src_feat.isValid():
            continue
        new_feat = _clone_feature_nearby(layer, src_feat, GEOM_JITTER_M)
        if DRY_RUN:
            _detail(f"  [{layer.name()}] INSERT (dry) clone de fid={src_fid}")
            count += 1
            continue
        ok = layer.addFeature(new_feat)
        if ok:
            count += 1
            _detail(f"  [{layer.name()}] INSERT clone de fid={src_fid}")
        else:
            _warn(f"  [{layer.name()}] INSERT échoué pour clone de fid={src_fid}")
    return count


def do_updates(layer, n, all_fids):
    """Met à jour attributs et/ou géométrie de n features."""
    if not all_fids:
        _warn(f"  [{layer.name()}] UPDATE skip: aucune feature")
        return 0

    updatable_fields = _pick_updatable_fields(layer)
    count = 0
    fids_to_update = random.sample(all_fids, min(n, len(all_fids)))

    for fid in fids_to_update:
        feat = layer.getFeature(fid)
        if not feat.isValid():
            continue

        changes = []

        if updatable_fields and random.random() < 0.7:
            n_fields = random.randint(1, min(3, len(updatable_fields)))
            chosen = random.sample(updatable_fields, n_fields)
            for idx, field in chosen:
                new_val = _random_value_for_field(field)
                if new_val is not None:
                    if DRY_RUN:
                        changes.append(f"{field.name()}={new_val}")
                    else:
                        layer.changeAttributeValue(fid, idx, new_val)
                        changes.append(f"{field.name()}={new_val}")

        if random.random() < 0.5:
            new_geom = _jitter_geometry(feat.geometry(), GEOM_JITTER_M)
            if new_geom and not new_geom.isEmpty():
                if not DRY_RUN:
                    layer.changeGeometry(fid, new_geom)
                changes.append("geom_moved")

        if changes:
            count += 1
            _detail(f"  [{layer.name()}] UPDATE fid={fid}  {', '.join(changes)}")

    return count


def do_deletes(layer, n, all_fids):
    """Supprime n features (borné par DELETE_CAP_PCT)."""
    if not all_fids:
        _warn(f"  [{layer.name()}] DELETE skip: aucune feature")
        return 0

    max_allowed = max(1, int(len(all_fids) * DELETE_CAP_PCT / 100))
    actual_n = min(n, max_allowed)

    if actual_n < n:
        _warn(
            f"  [{layer.name()}] DELETE borné: {n} demandé → {actual_n} "
            f"(cap {DELETE_CAP_PCT}% de {len(all_fids)} features)"
        )

    fids_to_delete = random.sample(all_fids, actual_n)
    count = 0
    for fid in fids_to_delete:
        if DRY_RUN:
            _detail(f"  [{layer.name()}] DELETE (dry) fid={fid}")
            count += 1
            continue
        ok = layer.deleteFeature(fid)
        if ok:
            count += 1
            _detail(f"  [{layer.name()}] DELETE fid={fid}")
        else:
            _warn(f"  [{layer.name()}] DELETE échoué fid={fid}")
    return count


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def run_stress_edit():
    import time as _time
    last_run = globals().get(_RUN_GUARD_KEY)
    if last_run and (_time.time() - last_run) < 30:
        print("[GUARD] stress_edit déjà exécuté il y a < 30s. Ignoré. "
              "Attendre 30s ou relancer QGIS pour forcer.")
        return
    globals()[_RUN_GUARD_KEY] = _time.time()

    _LOG_LINES.clear()
    _info("=== stress_edit START ===")
    _info(
        f"config: rounds={ROUNDS} max_layers={MAX_LAYERS} "
        f"inserts={MAX_INSERTS} updates={MAX_UPDATES} deletes={MAX_DELETES} "
        f"delete_cap={DELETE_CAP_PCT}% jitter={GEOM_JITTER_M}m dry_run={DRY_RUN}"
    )

    canvas_extent, canvas_crs = _get_canvas_extent()
    _info(
        f"Emprise visible: {canvas_extent.xMinimum():.1f},{canvas_extent.yMinimum():.1f}"
        f" -> {canvas_extent.xMaximum():.1f},{canvas_extent.yMaximum():.1f}"
        f"  CRS={canvas_crs.authid()}"
    )

    all_layers = _get_editable_layers()
    if not all_layers:
        _warn("Aucune couche vectorielle éditable trouvée. Abandon.")
        return

    _info(f"Couches éditables trouvées: {len(all_layers)}")
    for lyr in all_layers:
        n_visible = len(_get_visible_fids(lyr, canvas_extent, canvas_crs))
        _info(f"  - {lyr.name()} [{n_visible} visibles / {lyr.featureCount()} total] ({lyr.geometryType().name})")

    # Cleanup auto: rollback edit buffers + detect persistent contamination
    if not _ensure_pristine_state(all_layers):
        _warn("ABORT: couche(s) contaminée(s) par un run précédent. "
              "Faire un Rewind RecoverLand complet avant relance, "
              "ou recharger le projet depuis le disque.")
        globals()[_RUN_GUARD_KEY] = 0  # libère le guard pour pouvoir retenter après cleanup
        return

    # Le jeu de couches sous test est fige AVANT le snapshot, et c'est
    # exactement lui qui est photographie. Le script photographiait les 35
    # couches du projet (193 Mo) alors qu'il n'en stressait que quelques-unes
    # tirees au sort a chaque passe: la verification portait donc sur autre
    # chose que ce qui etait teste, et le fichier etait trop gros pour etre
    # relu. Restreindre le nombre de COUCHES, jamais le nombre d'entites: une
    # couche sous test est photographiee entierement, sinon un degat
    # collateral hors emprise visible passerait inapercu.
    n_layers = random.randint(1, min(MAX_LAYERS, len(all_layers)))
    test_layers = random.sample(all_layers, n_layers)
    _info(f"Couches sous test ({len(test_layers)}): "
          f"{', '.join(lyr.name() for lyr in test_layers)}")
    _info("Les autres couches du projet ne sont ni stressees ni photographiees.")

    _config = {
        "rounds": ROUNDS, "max_layers": MAX_LAYERS,
        "max_inserts": MAX_INSERTS, "max_updates": MAX_UPDATES,
        "max_deletes": MAX_DELETES, "geom_jitter_m": GEOM_JITTER_M,
        "test_layers": [lyr.name() for lyr in test_layers],
    }
    if not DRY_RUN:
        _save_snapshot(test_layers, canvas_extent, canvas_crs, _config)

    total_stats = {"inserts": 0, "updates": 0, "deletes": 0, "commits": 0, "errors": 0}

    for round_idx in range(1, ROUNDS + 1):
        _info(f"--- ROUND {round_idx}/{ROUNDS} ---")

        # Toujours le meme jeu que celui photographie: sans cela, une passe
        # pourrait editer une couche absente du snapshot et la verification
        # serait aveugle a ce qu'elle a fait.
        for layer in test_layers:
            layer_name = layer.name()
            feat_count = layer.featureCount()
            _info(f"  Couche: {layer_name} ({feat_count} features)")

            visible_fids = _get_visible_fids(layer, canvas_extent, canvas_crs)
            n_visible = len(visible_fids)
            _info(f"  [{layer_name}] {n_visible} features visibles dans l'emprise")

            if n_visible == 0:
                _info(f"  [{layer_name}] aucune feature visible, skip")
                continue

            all_fids = visible_fids

            was_editing = layer.isEditable()
            if not was_editing:
                if not layer.startEditing():
                    _warn(f"  [{layer_name}] startEditing() échoué, skip")
                    total_stats["errors"] += 1
                    continue

            _info(f"  [{layer_name}] session d'édition ouverte")

            n_ins = random.randint(0, MAX_INSERTS)
            n_upd = random.randint(1, MAX_UPDATES)
            n_del = random.randint(0, MAX_DELETES)

            ops = (
                [("insert", n_ins)] * (1 if n_ins > 0 else 0)
                + [("update", n_upd)]
                + [("delete", n_del)] * (1 if n_del > 0 else 0)
            )
            random.shuffle(ops)

            round_ins = round_upd = round_del = 0
            for op_type, op_count in ops:
                if op_type == "insert":
                    round_ins += do_inserts(layer, op_count, all_fids)
                elif op_type == "update":
                    round_upd += do_updates(layer, op_count, all_fids)
                elif op_type == "delete":
                    round_del += do_deletes(layer, op_count, all_fids)

            total_stats["inserts"] += round_ins
            total_stats["updates"] += round_upd
            total_stats["deletes"] += round_del

            if DRY_RUN:
                if not was_editing:
                    layer.rollBack()
                _info(
                    f"  [{layer_name}] DRY RUN: +{round_ins} ins, "
                    f"~{round_upd} upd, -{round_del} del (rollback)"
                )
                continue

            ok = layer.commitChanges()
            if ok:
                total_stats["commits"] += 1
                _info(
                    f"  [{layer_name}] COMMIT OK: +{round_ins} ins, "
                    f"~{round_upd} upd, -{round_del} del"
                )
            else:
                errors = layer.commitErrors()
                _warn(
                    f"  [{layer_name}] COMMIT ECHOUÉ: {errors}"
                )
                layer.rollBack()
                total_stats["errors"] += 1

    _info("=== stress_edit TERMINÉ ===")
    _info(
        f"BILAN: {total_stats['inserts']} inserts, "
        f"{total_stats['updates']} updates, "
        f"{total_stats['deletes']} deletes, "
        f"{total_stats['commits']} commits, "
        f"{total_stats['errors']} erreurs"
    )


# ---------------------------------------------------------------------------
# AUTO-RUN
# ---------------------------------------------------------------------------
run_stress_edit()
