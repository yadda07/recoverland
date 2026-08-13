"""tx_prov_full_cycle_matrix - RecoverLand validation runtime.

Invariant: I-10 (un provider annonce comme supporte doit boucler le cycle
complet : capturer, rejouer en inverse, et RENDRE les donnees d'origine).

Le trou que ce scenario sonde
-----------------------------
Les scenarios `providers/provider_*.py` existants s'arretent a la capture :
ils comptent les evenements tombes dans le journal et concluent
`score=100`. Personne ne verifie que le REWIND ramene reellement les
donnees. Or c'est la seule chose que l'utilisateur constate : il clique
"revenir au 12/08 09:00", le plugin affiche "restauration terminee", et
il rouvre la table attributaire. Si la couche n'est pas revenue, le
plugin a menti.

Ce scenario fait, pour chaque provider testable sans serveur, un cycle
COMPLET sur une vraie couche QGIS ecrite dans un dossier temporaire :

    1. creation de la couche + 3 entites  -> etat S0 relu depuis le disque
    2. tracker actif, connect_layer
    3. UNE transaction Qt qui melange les trois operations :
         - UPDATE attributaire sur E1 (nom -> "MODIFIE")
         - UPDATE geometrique  sur E2 (deplacee en (9 9))
         - DELETE de E3
         - INSERT de E4
    4. attente que les evenements soient ecrits dans le journal SQLite
    5. rewind reel : fetch_events_after_cutoff -> collapse_rewind_events
       -> plan_temporal_restore -> execute_restore_plan (chemin STRICT,
       celui du StrictRestoreRunner de production, sans la machine Qt)
    6. layer.reload() puis relecture du disque -> etat S1
    7. S1 doit etre EGAL a S0, contenu par contenu (nom, code, geometrie
       arrondie), independamment des FID qui, eux, ont le droit de bouger.

Providers couverts (`_PROVIDER_BUILDERS`) :
    memory, ogr/GPKG, ogr/GPKG NoGeometry, ogr/ESRI Shapefile,
    ogr/GeoJSON, ogr/CSV, delimitedtext/CSV.

Le journal est UNIQUE pour toute la matrice : chaque provider doit se
retrouver sur sa propre empreinte de datasource. Un provider qui
contaminerait le rewind d'un autre le ferait echouer ici.

Point de contrat verifie en plus du cycle
-----------------------------------------
`support_policy` publie `is_capture_supported(layer)`. Pour une couche
memoire elle repond True (`capture=True` dans `_PROVIDER_POLICIES`),
alors que `EditSessionTracker.connect_layer` refuse la couche parce que
`identity_strength == NONE`. Les deux affirmations ne peuvent pas etre
vraies en meme temps. La question posee par l'assertion
`memory_policy_matches_reality` est : quelle est la source de verite que
l'utilisateur peut consulter ? Aujourd'hui la seule trace du refus est
une ligne WARNING dans `recoverland_debug.log`.

Scenario d'echec concret : un utilisateur travaille sur une couche
temporaire (resultat d'un buffer, d'une extraction), edite pendant une
heure, ferme QGIS. RecoverLand n'a rien capture et ne le lui a jamais
dit dans l'interface.
"""
from __future__ import annotations

import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_prov_full_cycle_matrix"
INVARIANT = "I-10"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Etat initial commun a toutes les fixtures : 3 entites.
_SEED = [
    ("ALPHA", "C001", (1.0, 1.0)),
    ("BETA", "C002", (2.0, 2.0)),
    ("GAMMA", "C003", (3.0, 3.0)),
]
_MOVED_XY = (9.0, 9.0)


# ---------------------------------------------------------------------------
# Fixtures : une couche par provider, chacune dans son propre dossier temp.
# ---------------------------------------------------------------------------

def _seed_memory_layer(name: str, with_geom: bool = True):
    from qgis.core import QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY

    head = "Point?crs=EPSG:4326" if with_geom else "None"
    uri = f"{head}&field=nom:string(20)&field=code:string(20)"
    mem = QgsVectorLayer(uri, name, "memory")
    if not mem.isValid():
        raise RuntimeError(f"memory seed layer invalid: {uri}")
    mem.startEditing()
    for nom, code, xy in _SEED:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([nom, code])
        if with_geom:
            feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*xy)))
        mem.addFeature(feat)
    if not mem.commitChanges():
        raise RuntimeError(f"memory seed commit failed: {mem.commitErrors()}")
    return mem


def _write_with_ogr(mem, path: str, driver: str, layer_name: str = ""):
    from qgis.core import (
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = driver
    if layer_name:
        opts.layerName = layer_name
    res = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    code = res[0] if isinstance(res, (tuple, list)) else res
    if code != QgsVectorFileWriter.NoError:
        raise RuntimeError(f"writer {driver} failed on {path}: {res}")


def build_memory(tmpdir: str):
    return _seed_memory_layer("tx_mem"), True


def build_gpkg(tmpdir: str):
    from qgis.core import QgsVectorLayer
    mem = _seed_memory_layer("tx_gpkg_seed")
    path = os.path.join(tmpdir, "matrix.gpkg")
    _write_with_ogr(mem, path, "GPKG", layer_name="zones")
    layer = QgsVectorLayer(f"{path}|layername=zones", "tx_gpkg", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"gpkg layer invalid: {path}")
    return layer, True


def build_gpkg_nogeom(tmpdir: str):
    """Table attributaire GPKG (wkbNone) : le cas 'couche NoGeometry'."""
    from osgeo import ogr
    from qgis.core import QgsVectorLayer

    path = os.path.join(tmpdir, "attrs.gpkg")
    drv = ogr.GetDriverByName("GPKG")
    ds = drv.CreateDataSource(path)
    lyr = ds.CreateLayer("registre", geom_type=ogr.wkbNone)
    lyr.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    for nom, code, _xy in _SEED:
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("nom", nom)
        feat.SetField("code", code)
        lyr.CreateFeature(feat)
        feat = None
    ds.FlushCache()
    ds = None
    layer = QgsVectorLayer(f"{path}|layername=registre", "tx_gpkg_nogeom", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"gpkg nogeom layer invalid: {path}")
    return layer, False


def build_shapefile(tmpdir: str):
    from qgis.core import QgsVectorLayer
    mem = _seed_memory_layer("tx_shp_seed")
    path = os.path.join(tmpdir, "matrix.shp")
    _write_with_ogr(mem, path, "ESRI Shapefile")
    layer = QgsVectorLayer(path, "tx_shp", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"shapefile layer invalid: {path}")
    return layer, True


def build_geojson(tmpdir: str):
    from qgis.core import QgsVectorLayer
    mem = _seed_memory_layer("tx_gj_seed")
    path = os.path.join(tmpdir, "matrix.geojson")
    _write_with_ogr(mem, path, "GeoJSON")
    layer = QgsVectorLayer(path, "tx_geojson", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"geojson layer invalid: {path}")
    return layer, True


def build_csv_ogr(tmpdir: str):
    """CSV lu par le provider OGR (policy: FULL, identity WEAK)."""
    from qgis.core import QgsVectorLayer
    path = os.path.join(tmpdir, "matrix.csv")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("nom,code\n")
        for nom, code, _xy in _SEED:
            fh.write(f"{nom},{code}\n")
    layer = QgsVectorLayer(path, "tx_csv_ogr", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"csv/ogr layer invalid: {path}")
    return layer, False


def build_csv_delimitedtext(tmpdir: str):
    """Meme fichier CSV, lu par le provider natif delimitedtext."""
    from qgis.core import QgsVectorLayer
    path = os.path.join(tmpdir, "delim.csv")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("nom,code,x,y\n")
        for nom, code, xy in _SEED:
            fh.write(f"{nom},{code},{xy[0]},{xy[1]}\n")
    uri = ("file:///" + path.replace("\\", "/")
           + "?type=csv&xField=x&yField=y&crs=EPSG:4326")
    layer = QgsVectorLayer(uri, "tx_csv_delim", "delimitedtext")
    if not layer.isValid():
        raise RuntimeError(f"delimitedtext layer invalid: {uri}")
    return layer, True


_PROVIDER_BUILDERS = [
    ("memory", build_memory),
    ("ogr_gpkg", build_gpkg),
    ("ogr_gpkg_nogeom", build_gpkg_nogeom),
    ("ogr_shapefile", build_shapefile),
    ("ogr_geojson", build_geojson),
    ("ogr_csv", build_csv_ogr),
    ("delimitedtext_csv", build_csv_delimitedtext),
]


# ---------------------------------------------------------------------------
# Lecture d'etat : contenu seul, jamais les FID.
# ---------------------------------------------------------------------------

def _read_state(layer, has_geom: bool):
    """Etat lisible d'une couche : liste triee de (nom, code, xy)."""
    rows = []
    for feat in layer.getFeatures():
        try:
            nom = feat["nom"]
        except (KeyError, IndexError):
            nom = None
        try:
            code = feat["code"]
        except (KeyError, IndexError):
            code = None
        xy = None
        if has_geom:
            geom = feat.geometry()
            if geom is not None and not geom.isNull():
                pt = geom.asPoint()
                xy = (round(pt.x(), 4), round(pt.y(), 4))
        rows.append((
            None if nom is None else str(nom),
            None if code is None else str(code),
            xy,
        ))
    return sorted(rows, key=lambda r: (str(r[0]), str(r[1]), str(r[2])))


def _fid_by_nom(layer, nom_value: str):
    for feat in layer.getFeatures():
        try:
            if str(feat["nom"]) == nom_value:
                return feat.id()
        except (KeyError, IndexError):
            continue
    return None


# ---------------------------------------------------------------------------
# Journal partage
# ---------------------------------------------------------------------------

def _open_journal(tmpdir: str):
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.write_queue import WriteQueue

    journal_path = os.path.join(tmpdir, "audit.db")
    jm = JournalManager()
    jm.open_for_project(project_path=journal_path, profile_path=tmpdir)
    wq = WriteQueue()
    wq.start(jm.path)
    return jm, wq


def _wait_for_events(jm, datasource_fp: str, expected_min: int,
                     timeout_s: float = 6.0):
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events

    deadline = time.monotonic() + timeout_s
    found = []
    while time.monotonic() < deadline:
        try:
            conn = jm.create_read_connection()
            try:
                criteria = SearchCriteria(
                    datasource_fingerprint=datasource_fp,
                    layer_name=None, operation_type=None, user_name=None,
                    start_date=None, end_date=None, page=1, page_size=1000,
                )
                found = list(getattr(search_events(conn, criteria),
                                     "events", []) or [])
            finally:
                conn.close()
        except Exception:
            found = []
        if len(found) >= expected_min:
            return found
        time.sleep(0.05)
    return found


# ---------------------------------------------------------------------------
# Le cycle
# ---------------------------------------------------------------------------

def _mutate(layer, has_geom: bool) -> dict:
    """UNE transaction Qt : UPDATE attr + UPDATE geom + DELETE + INSERT."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    info = {"started": False, "commit_ok": False, "commit_errors": [],
            "ops": []}
    if not layer.startEditing():
        info["commit_errors"] = ["startEditing returned False"]
        return info
    info["started"] = True

    idx_nom = layer.fields().indexOf("nom")
    fid_a = _fid_by_nom(layer, "ALPHA")
    fid_b = _fid_by_nom(layer, "BETA")
    fid_c = _fid_by_nom(layer, "GAMMA")

    if fid_a is not None and idx_nom >= 0:
        if layer.changeAttributeValue(fid_a, idx_nom, "MODIFIE"):
            info["ops"].append("update_attr")
    if has_geom and fid_b is not None:
        geom = QgsGeometry.fromPointXY(QgsPointXY(*_MOVED_XY))
        if layer.changeGeometry(fid_b, geom):
            info["ops"].append("update_geom")
    if fid_c is not None:
        if layer.deleteFeature(fid_c):
            info["ops"].append("delete")

    feat = QgsFeature(layer.fields())
    feat.setAttribute(layer.fields().indexOf("nom"), "DELTA")
    idx_code = layer.fields().indexOf("code")
    if idx_code >= 0:
        feat.setAttribute(idx_code, "C004")
    if has_geom:
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(4.0, 4.0)))
    if layer.addFeature(feat):
        info["ops"].append("insert")

    info["commit_ok"] = bool(layer.commitChanges())
    if not info["commit_ok"]:
        info["commit_errors"] = list(layer.commitErrors())
        layer.rollBack()
    return info


def _rewind(jm, layer, datasource_fp: str, cutoff_iso: str, trace_id: str) -> dict:
    """Rejoue le journal en inverse, chemin STRICT de production."""
    from recoverland.core.event_stream_repository import fetch_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.rewind_dedup import collapse_rewind_events_with_stats
    from recoverland.core.restore_planner import plan_temporal_restore
    from recoverland.core.restore_executor import execute_restore_plan

    out = {"fetched": 0, "active": 0, "planned": 0, "conflicts": [],
           "succeeded": 0, "failed": {}, "error": None}
    cutoff = RestoreCutoff(CutoffType.BY_DATE, cutoff_iso, inclusive=True)
    conn = jm.create_read_connection()
    try:
        events = fetch_events_after_cutoff(
            conn, datasource_fp, cutoff, trace_id=trace_id)
    finally:
        conn.close()
    out["fetched"] = len(events)
    active, _stats = collapse_rewind_events_with_stats(events)
    out["active"] = len(active)
    if not active:
        return out

    plan = plan_temporal_restore(
        active, datasource_fp, layer.name(), cutoff)
    out["planned"] = len(plan.actions)
    out["conflicts"] = [f"{c.event_id}:{c.reason}" for c in plan.conflicts]
    try:
        report = execute_restore_plan(
            plan, {e.event_id: e for e in active}, layer, trace_id=trace_id)
    except Exception as exc:  # noqa: BLE001 - a crashing rewind is a result
        out["error"] = repr(exc)
        return out
    out["succeeded"] = len(report.succeeded)
    out["failed"] = {k: str(v)[:160] for k, v in (report.failed or {}).items()}
    return out


def _run_one(ctx, label: str, builder, jm, tracker) -> dict:
    from recoverland.core.identity import (
        compute_datasource_fingerprint, get_identity_strength_for_layer,
    )
    from recoverland.core.support_policy import (
        evaluate_layer_support, is_capture_supported,
    )
    from recoverland.core.logger import flog

    res = {
        "provider": label, "error": None,
        "policy_capture": None, "policy_support": None,
        "policy_strength": None, "is_capture_supported": None,
        "connected": None, "commit_ok": None, "commit_errors": [],
        "ops": [], "captured": 0, "ops_captured": [],
        "state_before": None, "state_after_edit": None,
        "state_after_rewind": None,
        "rewind": {}, "restored": None, "tmpdir": None,
    }
    tmpdir = tempfile.mkdtemp(prefix=f"rl_tx_{label}_")
    res["tmpdir"] = tmpdir
    layer = None
    try:
        layer, has_geom = builder(tmpdir)
        res["provider_name"] = layer.dataProvider().name()
        policy = evaluate_layer_support(layer)
        res["policy_capture"] = bool(policy.capture)
        res["policy_support"] = policy.support_level.value
        res["policy_strength"] = policy.identity_strength.value
        res["is_capture_supported"] = bool(is_capture_supported(layer))
        res["raw_strength"] = get_identity_strength_for_layer(layer).value
        fp = compute_datasource_fingerprint(layer)
        res["fingerprint"] = fp

        res["state_before"] = _read_state(layer, has_geom)

        tracker.connect_layer(layer)
        res["connected"] = layer.id() in tracker._connected_layers

        cutoff_iso = (datetime.now(timezone.utc)
                      - timedelta(seconds=2)).isoformat()

        mut = _mutate(layer, has_geom)
        res["commit_ok"] = mut["commit_ok"]
        res["commit_errors"] = mut["commit_errors"]
        res["ops"] = mut["ops"]
        layer.reload()
        res["state_after_edit"] = _read_state(layer, has_geom)

        if res["connected"] and mut["commit_ok"]:
            events = _wait_for_events(jm, fp, expected_min=len(mut["ops"]))
            res["captured"] = len(events)
            res["ops_captured"] = sorted(
                str(getattr(e, "operation_type", "?")) for e in events)

        with tracker.suppressed():
            res["rewind"] = _rewind(
                jm, layer, fp, cutoff_iso, ctx.trace_id)
            layer.reload()
            res["state_after_rewind"] = _read_state(layer, has_geom)

        res["restored"] = res["state_after_rewind"] == res["state_before"]
        flog(
            f"tx_prov_full_cycle_matrix: provider={label} "
            f"connected={res['connected']} captured={res['captured']} "
            f"rewind={res['rewind']} restored={res['restored']} "
            f"trace_id={ctx.trace_id}",
            "INFO" if res["restored"] else "WARNING",
        )
    except Exception as exc:  # noqa: BLE001 - a broken provider is a result
        res["error"] = repr(exc)
        flog(
            f"tx_prov_full_cycle_matrix: provider={label} exception={exc!r} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        if layer is not None:
            try:
                tracker.disconnect_layer(layer)
            except Exception:
                pass
        layer = None
        shutil.rmtree(tmpdir, ignore_errors=True)
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["journal_dir"] = tempfile.mkdtemp(prefix="rl_tx_matrix_jrnl_")
    jm, wq = _open_journal(ctx.data["journal_dir"])
    ctx.data["jm"] = jm
    ctx.data["wq"] = wq
    flog(
        f"tx_prov_full_cycle_matrix setup: trace_id={ctx.trace_id} "
        f"journal={jm.path} providers={[p for p, _ in _PROVIDER_BUILDERS]}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog

    jm = ctx.data["jm"]
    wq = ctx.data["wq"]
    tracker = EditSessionTracker(wq, jm)
    tracker.activate()

    flog(f"tx_prov_full_cycle_matrix run start: trace_id={ctx.trace_id}", "INFO")

    results = {}
    for label, builder in _PROVIDER_BUILDERS:
        results[label] = _run_one(ctx, label, builder, jm, tracker)
    ctx.data["results"] = results

    flog(
        "tx_prov_full_cycle_matrix run end: trace_id={t} summary={s}".format(
            t=ctx.trace_id,
            s={k: (v["connected"], v["captured"], v["restored"], v["error"])
               for k, v in results.items()},
        ),
        "INFO",
    )

    try:
        tracker.disconnect_all()
    except Exception:
        pass
    try:
        wq.stop()
    except Exception:
        pass
    try:
        if jm.is_open:
            jm.close()
    except Exception:
        pass
    ctx.data["jm"] = None
    ctx.data["wq"] = None
    shutil.rmtree(ctx.data.get("journal_dir", ""), ignore_errors=True)


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def _cycle_assertions(out, res, label, expect_cycle: bool):
    """Assertions communes a un provider dont on attend le cycle complet."""
    err = res.get("error")
    rw = res.get("rewind") or {}

    out.append((
        f"{label}_fixture_built",
        err is None,
        f"la fixture {label} n'a pas pu etre construite/exercee : {err}. "
        f"Sans fixture, aucune conclusion possible sur ce provider.",
    ))
    if err is not None:
        return

    out.append((
        f"{label}_edit_committed",
        res.get("commit_ok") is True,
        f"commitChanges a echoue sur {label} (errors={res.get('commit_errors')}). "
        f"Le provider refuse l'ecriture : ni capture ni rewind n'ont de sens.",
    ))

    out.append((
        f"{label}_tracker_connected",
        res.get("connected") is True,
        f"le tracker n'a pas connecte la couche {label} "
        f"(support={res.get('policy_support')} "
        f"strength={res.get('policy_strength')}). "
        f"L'utilisateur edite sans filet : aucune modification n'est "
        f"journalisee et rien ne l'en avertit dans l'interface.",
    ))

    out.append((
        f"{label}_all_ops_captured",
        res.get("captured", 0) >= len(res.get("ops") or []) > 0,
        f"captured={res.get('captured')} pour ops={res.get('ops')} "
        f"(types captures={res.get('ops_captured')}). Un evenement manquant "
        f"est une modification que le rewind ne pourra jamais annuler.",
    ))

    out.append((
        f"{label}_rewind_produced_actions",
        rw.get("planned", 0) > 0,
        f"rewind={rw} : le planificateur n'a produit aucune action alors que "
        f"la couche a bien ete modifiee. L'utilisateur verra "
        f"'Aucun evenement apres cette date. Rien a restaurer.' devant une "
        f"couche pourtant sale.",
    ))

    out.append((
        f"{label}_rewind_reported_no_failure",
        rw.get("error") is None and not rw.get("failed"),
        f"rewind={rw} : le rejeu inverse a signale des echecs. "
        f"La couche reste dans un etat intermediaire, ni avant ni apres.",
    ))

    if expect_cycle:
        out.append((
            f"{label}_data_actually_returned",
            res.get("restored") is True,
            f"apres rewind la couche {label} n'est PAS revenue a son etat "
            f"d'origine.\n      avant  = {res.get('state_before')}\n"
            f"      apres edition = {res.get('state_after_edit')}\n"
            f"      apres rewind  = {res.get('state_after_rewind')}\n"
            f"      rewind={rw}\n"
            f"C'est exactement ce que l'utilisateur constate en rouvrant la "
            f"table attributaire : le plugin annonce une restauration reussie "
            f"et les donnees ne sont pas revenues.",
        ))


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    results = ctx.data.get("results") or {}

    # ===== Sanite : la matrice a bien tourne ============================
    out.append((
        "matrix_ran_every_provider",
        set(results.keys()) == {p for p, _ in _PROVIDER_BUILDERS},
        f"providers executes={sorted(results.keys())} "
        f"attendus={sorted(p for p, _ in _PROVIDER_BUILDERS)}",
    ))

    # ===== memoire : la policy dit capture=True, la realite dit non =====
    mem = results.get("memory") or {}
    out.append((
        "memory_tracker_refuses_capture",
        mem.get("connected") is False,
        f"connected={mem.get('connected')} : le tracker doit refuser une "
        f"couche memoire (identity_strength=none), sinon il journalise des "
        f"FID qu'aucun rewind ne pourra retrouver.",
    ))
    out.append((
        "memory_policy_matches_reality",
        mem.get("is_capture_supported") is False,
        f"is_capture_supported(couche memoire)={mem.get('is_capture_supported')} "
        f"alors que le tracker a refuse la couche "
        f"(connected={mem.get('connected')}). L'API publique de "
        f"support_policy affirme que la capture est supportee ; le seul "
        f"endroit ou l'utilisateur peut apprendre le contraire est une ligne "
        f"WARNING dans recoverland_debug.log. Aucun appelant du plugin "
        f"n'utilise is_capture_supported / format_support_message : rien "
        f"dans l'interface ne signale que la couche n'est pas protegee.",
    ))
    out.append((
        "memory_no_event_captured",
        mem.get("captured", 0) == 0,
        f"captured={mem.get('captured')} : une couche refusee ne doit "
        f"produire aucun evenement, sinon le journal contient des "
        f"enregistrements irrecuperables.",
    ))

    # ===== providers fichier : cycle complet attendu =====================
    for label in ("ogr_gpkg", "ogr_gpkg_nogeom", "ogr_shapefile",
                  "ogr_geojson", "ogr_csv"):
        res = results.get(label) or {}
        _cycle_assertions(out, res, label, expect_cycle=True)

    # ===== delimitedtext : provider en lecture seule =====================
    delim = results.get("delimitedtext_csv") or {}
    out.append((
        "delimitedtext_refusal_is_coherent",
        (delim.get("connected") is False
         and delim.get("policy_support") == "refused"),
        f"connected={delim.get('connected')} "
        f"support={delim.get('policy_support')} "
        f"policy_capture={delim.get('policy_capture')} : le provider "
        f"delimitedtext est en lecture seule, la couche doit etre refusee et "
        f"la policy doit le dire. Une policy statique annoncant "
        f"capture=True pour delimitedtext ferait croire a une protection "
        f"inexistante.",
    ))
    out.append((
        "delimitedtext_edit_is_impossible",
        delim.get("commit_ok") is not True
        or not (delim.get("ops") or []),
        f"commit_ok={delim.get('commit_ok')} ops={delim.get('ops')} : si "
        f"le provider accepte finalement des ecritures, le refus de capture "
        f"devient un trou net (l'utilisateur edite, rien n'est journalise).",
    ))

    # ===== isolation des empreintes ======================================
    fps = {k: v.get("fingerprint") for k, v in results.items()
           if v.get("fingerprint")}
    out.append((
        "each_provider_has_its_own_fingerprint",
        len(set(fps.values())) == len(fps),
        f"empreintes={fps} : deux providers partageant une empreinte "
        f"melangeraient leurs historiques et un rewind sur l'un "
        f"toucherait l'autre.",
    ))

    # ===== traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_prov_full_cycle_matrix.*trace_id={ctx.trace_id}",
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
