"""tx_prov_join_and_repack - RecoverLand validation runtime.

Invariant: I-3 (l'identite d'une entite au moment d'ECRIRE) et I-2 (ne
capturer que ce que l'utilisateur a reellement modifie).

Deux tortures sur des couches OGR reelles, dans un dossier temporaire.

Phase A - la couche jointe fabrique de faux changements d'attributs
------------------------------------------------------------------
`edit_tracker._capture_edit_buffer_state` construit sa liste de champs
depuis `layer.fields()`, qui inclut les champs JOINTS, puis lit l'etat
AVANT depuis `provider.getFeatures()`, qui ne les connait pas :

    field_names = [f.name() for f in layer.fields()]     # avec j_extra
    ...
    for feature in provider.getFeatures(request):        # sans j_extra
        snapshot = create_snapshot_from_feature(feature, field_names)

`serialize_attributes` avale le `KeyError` et ecrit `None`. L'etat APRES,
lui, est lu via `layer.getFeatures()` (`_capture_new_state`) et contient
la vraie valeur jointe. Pour une modification PUREMENT geometrique,
`_generate_events` retombe sur

    changed_field_names = snap.changed_field_names or snap.field_names

c'est-a-dire TOUS les champs, champs joints compris. Le delta enregistre
alors `j_extra: None -> "EXTRA_..."`, un changement que l'utilisateur n'a
jamais fait.

Ce que ca casse pour lui : l'onglet historique lui montre une
modification d'attribut fantome sur une entite ou il n'a fait que
deplacer un point. Et surtout, au rewind, la compensation essaie de
reecrire `None` dans ce champ. `_build_attribute_changes` /
`iter_mapped_attributes` resolvent l'index via `layer.fields()`, qui
numerote le champ joint APRES les champs du provider : l'index envoye au
provider est hors de sa table.

Scenario d'echec concret : une couche communale jointe a un tableau
Excel de population. L'utilisateur corrige le trace d'une limite. Le
journal enregistre une modification de la population. Un rewind tente de
la remettre a NULL.

Phase B - shapefile recompacte : le rewind ecrase la mauvaise entite
-------------------------------------------------------------------
Sur un shapefile le FID est un numero d'enregistrement. Apres un REPACK
(que QGIS declenche lui-meme a la suppression d'entites, et que tout
autre outil SIG peut declencher), les enregistrements sont renumerotes.

`restore_service.restore_updated_feature` a ete durci : il exige une
preuve avant d'ecrire et refuse sinon (`target_unverifiable`, cf.
`rs_update_target_proof`). Le chemin REWIND, lui, passe par
`restore_executor._buffer_update`, qui se termine ainsi quand la preuve
echoue ET que le rattrapage par etat-apres ne trouve rien :

    flog(f"BUF_UPD eid=... post_chk=fail no_fallback → FORCE_APPLY "
         f"(FID exists, apply comp RW-12)", "WARNING")

Il applique quand meme. Le scenario reproduit la sequence complete :

    t1  RecoverLand capture : GAMMA (fid=2) renommee "MODIFIE"
    t2  RecoverLand n'est plus la (plugin desactive, QGIS redemarre) et
        l'entite MODIFIE est supprimee du shapefile, qui est recompacte
    t3  a la session suivante l'utilisateur demande un rewind avant t1

Le FID 2 existe toujours mais il porte maintenant DELTA. L'etat-apres
capture ("MODIFIE", geometrie (3 3)) n'existe plus nulle part, donc le
rattrapage echoue. FORCE_APPLY ecrit alors le nom ET la geometrie de
GAMMA sur DELTA. Une entite etrangere a l'operation est detruite, le
rapport annonce un succes, et le journal enregistre la destruction comme
une restauration legitime.

Verdict attendu avant correctif : FAIL sur les deux phases.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_prov_join_and_repack"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Quatre entites, geometries toutes distinctes pour que la preuve par
# etat-apres soit discriminante.
_SEED = [
    ("ALPHA", (1.0, 1.0)),
    ("BETA", (2.0, 2.0)),
    ("GAMMA", (3.0, 3.0)),
    ("DELTA", (4.0, 4.0)),
]
_JOIN_VALUES = {
    "ALPHA": "EXTRA_ALPHA",
    "BETA": "EXTRA_BETA",
    "GAMMA": "EXTRA_GAMMA",
    "DELTA": "EXTRA_DELTA",
}
_JOIN_PREFIX = "j_"
_MOVED_XY = (7.5, 7.5)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _seed_memory():
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY

    mem = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=nom:string(20)", "seed", "memory")
    mem.startEditing()
    for nom, xy in _SEED:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([nom])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*xy)))
        mem.addFeature(feat)
    mem.commitChanges()
    return mem


def _write(mem, path: str, driver: str, layer_name: str = ""):
    from qgis.core import QgsVectorFileWriter, QgsCoordinateTransformContext
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = driver
    if layer_name:
        opts.layerName = layer_name
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)


def _make_gpkg(tmpdir: str):
    from qgis.core import QgsVectorLayer
    path = os.path.join(tmpdir, "join.gpkg")
    _write(_seed_memory(), path, "GPKG", layer_name="communes")
    layer = QgsVectorLayer(f"{path}|layername=communes", "tx_join", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"gpkg invalide: {path}")
    return layer, path


def _make_shapefile(tmpdir: str, name: str = "repack"):
    from qgis.core import QgsVectorLayer
    path = os.path.join(tmpdir, f"{name}.shp")
    if not os.path.exists(path):
        _write(_seed_memory(), path, "ESRI Shapefile")
    layer = QgsVectorLayer(path, "tx_repack", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"shapefile invalide: {path}")
    return layer, path


def _make_join_source():
    """Table sans geometrie qui apporte le champ `extra`."""
    from qgis.core import QgsVectorLayer, QgsFeature

    src = QgsVectorLayer(
        "None?field=nom:string(20)&field=extra:string(30)",
        "tx_join_source", "memory")
    src.startEditing()
    for nom, extra in _JOIN_VALUES.items():
        feat = QgsFeature(src.fields())
        feat.setAttributes([nom, extra])
        src.addFeature(feat)
    src.commitChanges()
    return src


def _state(layer):
    rows = []
    for feat in layer.getFeatures():
        geom = feat.geometry()
        pt = geom.asPoint() if (geom is not None and not geom.isNull()) else None
        rows.append((
            str(feat["nom"]),
            (round(pt.x(), 4), round(pt.y(), 4)) if pt else None,
        ))
    return sorted(rows)


def _fid_by_nom(layer, nom_value: str):
    for feat in layer.getFeatures():
        if str(feat["nom"]) == nom_value:
            return feat.id()
    return None


# ---------------------------------------------------------------------------
# Journal
# ---------------------------------------------------------------------------

def _open_journal(tmpdir: str):
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue

    jm = JournalManager()
    jm.open_for_project(project_path=os.path.join(tmpdir, "audit.db"),
                        profile_path=tmpdir)
    wq = WriteQueue()
    wq.start(jm.path)
    return jm, wq


def _events_for(jm, fingerprint: str, expected_min: int = 1,
                timeout_s: float = 6.0):
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events

    deadline = time.monotonic() + timeout_s
    found = []
    while True:
        try:
            conn = jm.create_read_connection()
            try:
                criteria = SearchCriteria(
                    datasource_fingerprint=fingerprint,
                    layer_name=None, operation_type=None, user_name=None,
                    start_date=None, end_date=None, page=1, page_size=1000,
                )
                found = list(getattr(search_events(conn, criteria),
                                     "events", []) or [])
            finally:
                conn.close()
        except Exception:
            found = []
        if len(found) >= expected_min or time.monotonic() > deadline:
            return found
        time.sleep(0.05)


def _rewind(jm, layer, fingerprint: str, cutoff_iso: str, trace_id: str) -> dict:
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.restore_executor import execute_restore_plan

    out = {"fetched": 0, "active": 0, "succeeded": 0, "failed": {},
           "error": None}
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)
    conn = jm.create_read_connection()
    try:
        events = fetch_events_after_cutoff(
            conn, fingerprint, cutoff, trace_id=trace_id)
    finally:
        conn.close()
    out["fetched"] = len(events)
    active, _stats = collapse_rewind_events_with_stats(events)
    out["active"] = len(active)
    if not active:
        return out
    plan = plan_temporal_restore(active, fingerprint, layer.name(), cutoff)
    try:
        report = execute_restore_plan(
            plan, {e.event_id: e for e in active}, layer, trace_id=trace_id)
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
        return out
    out["succeeded"] = len(report.succeeded)
    out["failed"] = {k: str(v)[:160] for k, v in (report.failed or {}).items()}
    return out


# ---------------------------------------------------------------------------
# Phase A : couche jointe
# ---------------------------------------------------------------------------

def _phase_join(ctx) -> dict:
    from qgis.core import (
        QgsProject, QgsVectorLayerJoinInfo, QgsGeometry, QgsPointXY,
    )
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog

    out = {"error": None}
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_join_fix_")
    jrnl = tempfile.mkdtemp(prefix="rl_tx_join_jrnl_")
    jm = wq = None
    layer = src = None
    project = QgsProject.instance()
    try:
        layer, _path = _make_gpkg(tmpdir)
        src = _make_join_source()
        project.addMapLayer(src, False)
        project.addMapLayer(layer, False)

        info = QgsVectorLayerJoinInfo()
        info.setJoinLayer(src)
        info.setJoinFieldName("nom")
        info.setTargetFieldName("nom")
        info.setUsingMemoryCache(True)
        info.setPrefix(_JOIN_PREFIX)
        out["join_added"] = bool(layer.addJoin(info))

        out["layer_fields"] = [f.name() for f in layer.fields()]
        out["provider_fields"] = [f.name() for f in layer.dataProvider().fields()]
        out["join_field"] = f"{_JOIN_PREFIX}extra"
        out["join_visible"] = out["join_field"] in out["layer_fields"]
        out["join_absent_from_provider"] = (
            out["join_field"] not in out["provider_fields"])

        jm, wq = _open_journal(jrnl)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        out["connected"] = layer.id() in tracker._connected_layers

        cutoff_iso = (datetime.now(timezone.utc)
                      - timedelta(seconds=2)).isoformat()
        out["state_before"] = _state(layer)

        # --- modification PUREMENT geometrique --------------------------
        fid = _fid_by_nom(layer, "BETA")
        out["target_fid"] = fid
        if layer.startEditing():
            geom = QgsGeometry.fromPointXY(QgsPointXY(*_MOVED_XY))
            out["geom_changed"] = bool(layer.changeGeometry(fid, geom))
            out["commit_ok"] = bool(layer.commitChanges())
            if not out["commit_ok"]:
                out["commit_errors"] = list(layer.commitErrors())
                layer.rollBack()
        else:
            out["geom_changed"] = False
            out["commit_ok"] = False

        fp = compute_datasource_fingerprint(layer)
        events = _events_for(jm, fp, expected_min=1)
        out["captured"] = len(events)
        deltas = []
        for evt in events:
            raw = getattr(evt, "attributes_json", None)
            try:
                parsed = json.loads(raw) if raw else {}
            except (TypeError, ValueError):
                parsed = {"_unparseable": str(raw)[:120]}
            deltas.append({
                "op": getattr(evt, "operation_type", "?"),
                "changed_only": parsed.get("changed_only", parsed),
            })
        out["deltas"] = deltas
        changed_keys = set()
        for d in deltas:
            co = d.get("changed_only")
            if isinstance(co, dict):
                changed_keys.update(co.keys())
        out["changed_keys"] = sorted(changed_keys)
        out["phantom_join_change"] = out["join_field"] in changed_keys

        # --- rewind : la compensation touche-t-elle le champ joint ? -----
        with tracker.suppressed():
            out["rewind"] = _rewind(jm, layer, fp, cutoff_iso, ctx.trace_id)
            layer.reload()
            out["state_after_rewind"] = _state(layer)
        out["geom_restored"] = out["state_after_rewind"] == out["state_before"]

        # La table source de la jointure ne doit pas avoir bouge.
        out["join_source_after"] = sorted(
            (str(f["nom"]), str(f["extra"])) for f in src.getFeatures())
        out["join_source_intact"] = (
            out["join_source_after"] == sorted(_JOIN_VALUES.items()))

        try:
            tracker.disconnect_all()
        except Exception:
            pass
        flog(
            f"tx_prov_join_and_repack phase_join: connected={out['connected']} "
            f"captured={out['captured']} changed_keys={out['changed_keys']} "
            f"phantom={out['phantom_join_change']} "
            f"rewind={out.get('rewind')} geom_restored={out['geom_restored']} "
            f"trace_id={ctx.trace_id}",
            "INFO" if not out["phantom_join_change"] else "WARNING",
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
        flog(
            f"tx_prov_join_and_repack phase_join: exception={exc!r} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        for lyr in (layer, src):
            try:
                if lyr is not None:
                    project.removeMapLayer(lyr.id())
            except Exception:
                pass
        layer = src = None
        try:
            if wq is not None:
                wq.stop()
        except Exception:
            pass
        try:
            if jm is not None and jm.is_open:
                jm.close()
        except Exception:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(jrnl, ignore_errors=True)
    return out


# ---------------------------------------------------------------------------
# Phase B : shapefile recompacte hors surveillance
# ---------------------------------------------------------------------------

def _external_delete_and_repack(path: str, nom_value: str) -> dict:
    """Supprime une entite et recompacte, comme le ferait un autre outil."""
    from osgeo import ogr

    info = {"deleted_fids": [], "repack": None, "error": None}
    try:
        ds = ogr.Open(path, 1)
        if ds is None:
            info["error"] = "ogr.Open en ecriture a echoue"
            return info
        lyr = ds.GetLayer(0)
        lyr.SetAttributeFilter(f"nom = '{nom_value}'")
        fids = []
        feat = lyr.GetNextFeature()
        while feat is not None:
            fids.append(feat.GetFID())
            feat = lyr.GetNextFeature()
        lyr.SetAttributeFilter(None)
        for fid in fids:
            lyr.DeleteFeature(fid)
        info["deleted_fids"] = fids
        name = lyr.GetName()
        ds.ExecuteSQL(f"REPACK {name}")
        ds.FlushCache()
        info["repack"] = name
        ds = None
    except Exception as exc:  # noqa: BLE001
        info["error"] = repr(exc)
    return info


def _phase_repack(ctx) -> dict:
    from qgis.core import QgsVectorLayer
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog

    out = {"error": None}
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_repack_fix_")
    jrnl = tempfile.mkdtemp(prefix="rl_tx_repack_jrnl_")
    jm = wq = None
    layer = None
    try:
        layer, path = _make_shapefile(tmpdir)
        out["shp_path"] = path
        out["state_before"] = _state(layer)
        out["fids_before"] = sorted(f.id() for f in layer.getFeatures())

        jm, wq = _open_journal(jrnl)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        out["connected"] = layer.id() in tracker._connected_layers

        cutoff_iso = (datetime.now(timezone.utc)
                      - timedelta(seconds=2)).isoformat()
        fp = compute_datasource_fingerprint(layer)

        # --- t1 : RecoverLand capture le renommage de GAMMA -------------
        idx = layer.fields().indexOf("nom")
        fid_gamma = _fid_by_nom(layer, "GAMMA")
        out["fid_gamma"] = fid_gamma
        if layer.startEditing():
            out["edit_ok"] = bool(
                layer.changeAttributeValue(fid_gamma, idx, "MODIFIE"))
            out["commit_ok"] = bool(layer.commitChanges())
        else:
            out["edit_ok"] = out["commit_ok"] = False
        events = _events_for(jm, fp, expected_min=1)
        out["captured"] = len(events)
        out["captured_fids"] = []
        for evt in events:
            try:
                out["captured_fids"].append(
                    json.loads(evt.feature_identity_json or "{}").get("fid"))
            except (TypeError, ValueError):
                out["captured_fids"].append(None)

        # --- t2 : RecoverLand n'est plus la, le shapefile est nettoye ---
        tracker.disconnect_all()
        try:
            layer.setDataSource("", "closed", "memory")
        except Exception:
            pass
        layer = None
        out["external"] = _external_delete_and_repack(path, "MODIFIE")

        layer = QgsVectorLayer(path, "tx_repack_session2", "ogr")
        if not layer.isValid():
            raise RuntimeError("shapefile invalide apres REPACK")
        out["state_after_repack"] = _state(layer)
        out["fids_after_repack"] = sorted(f.id() for f in layer.getFeatures())
        out["fid_reuse"] = fid_gamma in out["fids_after_repack"]
        occupant = None
        for feat in layer.getFeatures():
            if feat.id() == fid_gamma:
                occupant = str(feat["nom"])
        out["occupant_of_old_fid"] = occupant

        # --- t3 : session suivante, rewind ------------------------------
        tracker2 = EditSessionTracker(wq, jm)
        tracker2.activate()
        with tracker2.suppressed():
            out["rewind"] = _rewind(jm, layer, fp, cutoff_iso, ctx.trace_id)
            layer.reload()
            out["state_after_rewind"] = _state(layer)

        expected_untouched = [
            (nom, xy) for nom, xy in _SEED if nom != "GAMMA"
        ]
        out["expected_untouched"] = sorted(expected_untouched)
        out["bystanders_intact"] = all(
            row in out["state_after_rewind"] for row in expected_untouched)
        try:
            tracker2.disconnect_all()
        except Exception:
            pass
        flog(
            f"tx_prov_join_and_repack phase_repack: captured={out['captured']} "
            f"captured_fids={out['captured_fids']} "
            f"occupant_of_old_fid={out['occupant_of_old_fid']!r} "
            f"rewind={out.get('rewind')} "
            f"bystanders_intact={out['bystanders_intact']} "
            f"trace_id={ctx.trace_id}",
            "INFO" if out["bystanders_intact"] else "ERROR",
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
        flog(
            f"tx_prov_join_and_repack phase_repack: exception={exc!r} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        layer = None
        try:
            if wq is not None:
                wq.stop()
        except Exception:
            pass
        try:
            if jm is not None and jm.is_open:
                jm.close()
        except Exception:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(jrnl, ignore_errors=True)
    return out


# ---------------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    flog(
        f"tx_prov_join_and_repack setup: trace_id={ctx.trace_id} "
        f"join_prefix={_JOIN_PREFIX!r}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_prov_join_and_repack run start: trace_id={ctx.trace_id}", "INFO")
    ctx.data["join"] = _phase_join(ctx)
    ctx.data["repack"] = _phase_repack(ctx)
    flog(
        f"tx_prov_join_and_repack run end: trace_id={ctx.trace_id} "
        f"phantom={ctx.data['join'].get('phantom_join_change')} "
        f"bystanders_intact={ctx.data['repack'].get('bystanders_intact')}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    jn = ctx.data.get("join") or {}
    rp = ctx.data.get("repack") or {}

    # ===== Phase A : sanite =============================================
    out.append((
        "join_phase_ran",
        jn.get("error") is None,
        f"la phase jointure a plante : {jn.get('error')}",
    ))
    out.append((
        "join_fixture_is_a_real_join",
        (jn.get("join_added") is True and jn.get("join_visible") is True
         and jn.get("join_absent_from_provider") is True),
        f"join_added={jn.get('join_added')} "
        f"champs couche={jn.get('layer_fields')} "
        f"champs provider={jn.get('provider_fields')} : la fixture doit "
        f"presenter un champ joint visible cote couche et absent cote "
        f"provider, sinon la torture ne porte sur rien.",
    ))
    out.append((
        "joined_layer_is_captured",
        jn.get("connected") is True and jn.get("commit_ok") is True
        and jn.get("captured", 0) >= 1,
        f"connected={jn.get('connected')} commit_ok={jn.get('commit_ok')} "
        f"captured={jn.get('captured')} : une couche jointe reste une "
        f"couche OGR editable, elle doit etre capturee normalement.",
    ))

    # ===== Phase A : le trou ============================================
    out.append((
        "geometry_only_edit_records_no_attribute_change",
        jn.get("changed_keys") == [],
        f"champs marques comme modifies dans le journal = "
        f"{jn.get('changed_keys')} alors que l'utilisateur n'a fait que "
        f"DEPLACER un point. Delta enregistre = {jn.get('deltas')}. "
        f"L'historique montre a l'utilisateur des modifications "
        f"d'attributs qu'il n'a jamais faites.",
    ))
    out.append((
        "no_phantom_change_on_the_joined_field",
        jn.get("phantom_join_change") is False,
        f"le champ joint {jn.get('join_field')!r} apparait dans le delta "
        f"capture ({jn.get('deltas')}). L'etat AVANT est lu depuis le "
        f"provider, qui ignore les champs joints et renvoie None ; l'etat "
        f"APRES est lu depuis la couche, qui les resout. La difference est "
        f"un artefact de lecture, pas une modification. Au rewind, la "
        f"compensation tentera de reecrire None dans un champ qui "
        f"n'appartient meme pas a la table.",
    ))
    out.append((
        "join_geometry_rewind_returns_the_point",
        jn.get("geom_restored") is True,
        f"avant={jn.get('state_before')} "
        f"apres rewind={jn.get('state_after_rewind')} "
        f"rewind={jn.get('rewind')} : le deplacement doit etre annule sur "
        f"une couche jointe comme sur une autre.",
    ))
    out.append((
        "joined_layer_rewind_is_not_blocked_by_the_phantom_field",
        not any("buffer_refused" in str(v) or "attribute changes rejected"
                in str(v)
                for v in ((jn.get("rewind") or {}).get("failed") or {}).values()),
        f"echecs du rejeu inverse = "
        f"{(jn.get('rewind') or {}).get('failed')}. Le SEUL changement "
        f"d'attribut du delta est le champ joint fantome ; le provider le "
        f"refuse (son index depasse ses propres champs), "
        f"`_buffer_update` compte attr_ok=0 / attr_fail>0 et renvoie "
        f"reason_code=buffer_refused AVANT d'avoir restaure la geometrie. "
        f"En mode STRICT l'echec d'une action annule tout le plan : une "
        f"couche portant une jointure n'est donc pas rembobinable du tout, "
        f"et c'est une configuration QGIS parfaitement ordinaire.",
    ))
    out.append((
        "join_source_table_untouched",
        jn.get("join_source_intact") is True,
        f"table source de la jointure apres rewind="
        f"{jn.get('join_source_after')} : le rewind d'une couche ne doit "
        f"jamais ecrire dans la table jointe, qui n'a jamais ete auditee.",
    ))

    # ===== Phase B : sanite =============================================
    out.append((
        "repack_phase_ran",
        rp.get("error") is None,
        f"la phase recompactage a plante : {rp.get('error')}",
    ))
    out.append((
        "repack_fixture_captured_the_edit",
        rp.get("connected") is True and rp.get("commit_ok") is True
        and rp.get("captured", 0) >= 1,
        f"connected={rp.get('connected')} commit_ok={rp.get('commit_ok')} "
        f"captured={rp.get('captured')} fids captures="
        f"{rp.get('captured_fids')}",
    ))
    out.append((
        "repack_really_renumbered_the_records",
        (rp.get("fid_reuse") is True
         and rp.get("occupant_of_old_fid") not in (None, "MODIFIE")),
        f"fids avant={rp.get('fids_before')} apres REPACK="
        f"{rp.get('fids_after_repack')} ; occupant du FID historique "
        f"{rp.get('fid_gamma')} = {rp.get('occupant_of_old_fid')!r} "
        f"(external={rp.get('external')}). Sans reutilisation de FID la "
        f"torture ne prouve rien.",
    ))

    # ===== Phase B : le trou ============================================
    out.append((
        "rewind_does_not_destroy_an_unrelated_feature",
        rp.get("bystanders_intact") is True,
        f"etat avant       = {rp.get('state_before')}\n"
        f"      apres REPACK externe = {rp.get('state_after_repack')}\n"
        f"      apres rewind         = {rp.get('state_after_rewind')}\n"
        f"      rewind={rp.get('rewind')}\n"
        f"Les entites qui n'ont jamais fait partie de l'operation "
        f"({rp.get('expected_untouched')}) doivent toutes se retrouver "
        f"intactes. L'entite qui occupe aujourd'hui le FID historique a "
        f"recu les attributs d'une autre (l'evenement etant attributaire, "
        f"seule la geometrie de la victime survit) : l'utilisateur perd une "
        f"donnee qu'il n'a jamais touchee, l'operation est rapportee comme "
        f"reussie, et la destruction est journalisee comme une restauration "
        f"legitime.",
    ))
    out.append((
        "rewind_reports_the_unprovable_target",
        (rp.get("rewind") or {}).get("succeeded", 0) == 0
        or rp.get("bystanders_intact") is True,
        f"rewind={rp.get('rewind')} : quand la cible ne peut pas etre "
        f"prouvee, le rejeu doit compter un echec explicite plutot que "
        f"d'annoncer un succes. `restore_service.restore_updated_feature` "
        f"refuse deja dans ce cas (reason_code=target_unverifiable) ; "
        f"`restore_executor._buffer_update` applique quand meme via sa "
        f"branche FORCE_APPLY.",
    ))

    # ===== Trace du FORCE_APPLY =========================================
    out.append((
        "no_force_apply_in_the_rewind_path",
        not any("FORCE_APPLY" in (getattr(r, "message", "") or "")
                for r in (ctx.records or [])),
        "la ligne 'BUF_UPD ... post_chk=fail no_fallback -> FORCE_APPLY' a "
        "ete emise : le rejeu inverse a ecrit sur une entite dont il "
        "n'avait pas pu prouver l'identite. Un outil de recuperation ne "
        "doit jamais ecrire sans preuve ; refuser coute une restauration, "
        "appliquer detruit une entite etrangere.",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"tx_prov_join_and_repack.*trace_id={ctx.trace_id}",
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
