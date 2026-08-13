"""tx_prov_fingerprint_split - RecoverLand validation runtime.

Invariant: I-11 (une empreinte de datasource designe UN fichier de donnees,
quelle que soit la facon dont l'utilisateur l'a ouvert ou filtre).

Le trou que ce scenario prouve
------------------------------
`identity._normalize_file_source` normalise le CHEMIN puis recolle tel quel
tout ce qui suit le premier `|` :

    path = raw.split("|")[0] ; ... ; suffix = "|" + raw.split("|", 1)[1]

Or QGIS met dans ce suffixe non seulement `layername=`, qui fait bien
partie de l'identite de la table, mais aussi `subset=`, qui n'est qu'un
FILTRE D'AFFICHAGE. Poser un filtre sur une couche (clic droit >
Filtrer..., ou `setSubsetString`) change donc son empreinte de datasource
alors que le fichier, la table et les entites sont rigoureusement les
memes.

Consequence, deroulee ici de bout en bout sur un vrai GeoPackage :

    t1  l'utilisateur edite la couche non filtree      -> journal sous FP_A
    t2  il pose un filtre pour travailler sur un sous-ensemble
    t3  il edite de nouveau                            -> journal sous FP_B
    t4  il enleve le filtre et demande un rewind avant t1

Le rewind ne connait que FP_A. Les evenements de t3 sont dans une autre
"couche" du point de vue du journal : ils ne sont ni rejoues, ni
comptes, ni signales. Le plugin annonce une restauration reussie et la
couche reste sale. C'est un cas d'usage quotidien de QGIS, pas une
manipulation exotique.

Trois autres facons d'ecrire le meme chemin sont verifiees dans la
foulee (absolu vs relatif, casse Windows `C:` vs `c:`, separateurs
melanges) : celles-la doivent donner la MEME empreinte.

Enfin, la stabilite au repertoire courant. `_normalize_file_source`
applique `os.path.abspath` a tout ce qui precede le premier `|`. Pour
`ogr` cette portion est un vrai chemin, tout va bien. Pour les deux
autres providers declares "fichier" dans `_FILE_PROVIDERS` ce n'en est
pas un :

    delimitedtext : 'file:///C:/.../t.csv?type=csv&xField=x...'
    spatialite    : 'dbname=\\'C:/.../db.sqlite\\' table="pts" (geom)'

`abspath` ne reconnait pas ces formes comme absolues et les colle
derriere le repertoire courant du processus. L'empreinte obtenue depend
alors du dossier depuis lequel QGIS a ete lance. Meme fichier, meme
projet, deux sessions : deux historiques. Pour `spatialite`
(SupportLevel.FULL, IdentityStrength.STRONG, capture ET restore
autorises) c'est une perte d'historique silencieuse sur un provider
pleinement supporte.

Le provider spatialite n'a pas pu etre instancie dans le harnais
headless (aucune couche valide construite depuis le fichier SQLite ecrit
par QgsVectorFileWriter) ; la demonstration porte donc sur
`_normalize_source_uri('spatialite', uri)`, la fonction pure qui calcule
l'empreinte, appelee depuis deux repertoires courants differents.

Verdict attendu avant correctif : FAIL (scission par le filtre, plus les
deux empreintes dependantes du repertoire courant).
"""
from __future__ import annotations

import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_prov_fingerprint_split"
INVARIANT = "I-11"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SEED = [
    ("ALPHA", "x", (1.0, 1.0)),
    ("BETA", "y", (2.0, 2.0)),
    ("GAMMA", "x", (3.0, 3.0)),
    ("DELTA", "y", (4.0, 4.0)),
]
_SUBSET = "cat = 'x'"


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

def _make_gpkg(tmpdir: str):
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        "Point?crs=EPSG:4326&field=nom:string(20)&field=cat:string(5)",
        "seed", "memory")
    mem.startEditing()
    for nom, cat, xy in _SEED:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([nom, cat])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(*xy)))
        mem.addFeature(feat)
    mem.commitChanges()

    path = os.path.join(tmpdir, "split.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "zones"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)

    layer = QgsVectorLayer(f"{path}|layername=zones", "tx_split", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"gpkg invalide: {path}")
    return layer, path


def _state(layer):
    rows = []
    for feat in layer.getFeatures():
        geom = feat.geometry()
        pt = geom.asPoint() if (geom is not None and not geom.isNull()) else None
        rows.append((
            str(feat["nom"]), str(feat["cat"]),
            (round(pt.x(), 4), round(pt.y(), 4)) if pt else None,
        ))
    return sorted(rows)


def _fid_by_nom(layer, nom_value: str):
    for feat in layer.getFeatures():
        if str(feat["nom"]) == nom_value:
            return feat.id()
    return None


def _rename(layer, old_nom: str, new_nom: str) -> bool:
    idx = layer.fields().indexOf("nom")
    fid = _fid_by_nom(layer, old_nom)
    if fid is None or idx < 0:
        return False
    if not layer.startEditing():
        return False
    if not layer.changeAttributeValue(fid, idx, new_nom):
        layer.rollBack()
        return False
    ok = bool(layer.commitChanges())
    if not ok:
        layer.rollBack()
    return ok


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


def _events_for(jm, fingerprint: str, expected_min: int = 0,
                timeout_s: float = 5.0):
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
# Phases
# ---------------------------------------------------------------------------

def _phase_filter_split(ctx) -> dict:
    """Cycle reel : edition non filtree, filtre, edition filtree, rewind."""
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import (
        compute_datasource_fingerprint, datasource_fingerprints_match,
    )
    from recoverland.core.logger import flog

    out = {"error": None}
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_split_fix_")
    jrnl = tempfile.mkdtemp(prefix="rl_tx_split_jrnl_")
    jm = wq = None
    layer = None
    try:
        layer, path = _make_gpkg(tmpdir)
        out["gpkg_path"] = path
        out["state_before"] = _state(layer)

        jm, wq = _open_journal(jrnl)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        out["connected"] = layer.id() in tracker._connected_layers

        cutoff_iso = (datetime.now(timezone.utc)
                      - timedelta(seconds=2)).isoformat()
        out["cutoff"] = cutoff_iso

        # --- t1 : edition sur la couche NON filtree ----------------------
        fp_bare = compute_datasource_fingerprint(layer)
        out["fp_bare"] = fp_bare
        out["edit_bare_ok"] = _rename(layer, "ALPHA", "EDIT_NU")
        out["events_bare_after_t1"] = len(
            _events_for(jm, fp_bare, expected_min=1))

        # --- t2 : l'utilisateur pose un filtre ---------------------------
        out["subset_applied"] = bool(layer.setSubsetString(_SUBSET))
        fp_filtered = compute_datasource_fingerprint(layer)
        out["fp_filtered"] = fp_filtered
        out["fp_identical"] = fp_bare == fp_filtered
        out["fp_match_helper"] = bool(
            datasource_fingerprints_match(fp_bare, fp_filtered))
        out["visible_after_subset"] = layer.featureCount()

        # --- t3 : edition sur la couche filtree --------------------------
        out["edit_filtered_ok"] = _rename(layer, "GAMMA", "EDIT_FILTRE")
        out["events_filtered_fp"] = len(
            _events_for(jm, fp_filtered,
                        expected_min=0 if out["fp_identical"] else 1))
        out["events_bare_fp"] = len(_events_for(jm, fp_bare))

        # --- t4 : l'utilisateur enleve le filtre et rembobine ------------
        layer.setSubsetString("")
        out["state_after_edits"] = _state(layer)
        with tracker.suppressed():
            out["rewind"] = _rewind(
                jm, layer, fp_bare, cutoff_iso, ctx.trace_id)
            layer.reload()
            out["state_after_rewind"] = _state(layer)
        out["fully_restored"] = out["state_after_rewind"] == out["state_before"]

        try:
            tracker.disconnect_all()
        except Exception:
            pass
        flog(
            f"tx_prov_fingerprint_split phase_filter: "
            f"fp_identical={out['fp_identical']} "
            f"events_bare={out['events_bare_fp']} "
            f"events_filtered={out['events_filtered_fp']} "
            f"rewind={out.get('rewind')} restored={out['fully_restored']} "
            f"trace_id={ctx.trace_id}",
            "INFO" if out["fully_restored"] else "WARNING",
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
        flog(
            f"tx_prov_fingerprint_split phase_filter: exception={exc!r} "
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


def _phase_path_writings(ctx) -> dict:
    """Meme fichier, quatre facons de l'ecrire + un |subset= a l'ouverture."""
    from qgis.core import QgsVectorLayer
    from recoverland.core.identity import (
        compute_datasource_fingerprint, datasource_fingerprints_match,
    )
    from recoverland.core.logger import flog

    out = {"error": None, "fps": {}}
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_paths_")
    cwd0 = os.getcwd()
    try:
        base, path = _make_gpkg(tmpdir)
        out["fps"]["absolu"] = compute_datasource_fingerprint(base)

        # casse Windows : C: -> c:
        swapped = path
        if len(path) > 1 and path[1] == ":":
            swapped = path[0].swapcase() + path[1:]
        lay_case = QgsVectorLayer(f"{swapped}|layername=zones", "case", "ogr")
        out["case_layer_valid"] = lay_case.isValid()
        out["fps"]["casse_inversee"] = compute_datasource_fingerprint(lay_case)

        # separateurs melanges
        mixed = path.replace("\\", "/")
        if "/" in mixed:
            head, _, tail = mixed.rpartition("/")
            mixed = head + "\\" + tail
        lay_mixed = QgsVectorLayer(f"{mixed}|layername=zones", "mixed", "ogr")
        out["mixed_layer_valid"] = lay_mixed.isValid()
        out["fps"]["separateurs_melanges"] = compute_datasource_fingerprint(
            lay_mixed)

        # chemin relatif au repertoire courant
        os.chdir(tmpdir)
        lay_rel = QgsVectorLayer("split.gpkg|layername=zones", "rel", "ogr")
        out["rel_layer_valid"] = lay_rel.isValid()
        out["rel_source"] = lay_rel.source()
        out["fps"]["relatif"] = compute_datasource_fingerprint(lay_rel)
        os.chdir(cwd0)

        # |subset= pose des l'ouverture (deuxieme couche sur le meme fichier)
        lay_sub = QgsVectorLayer(
            f"{path}|layername=zones|subset={_SUBSET}", "sub", "ogr")
        out["subset_layer_valid"] = lay_sub.isValid()
        out["fp_uri_subset"] = compute_datasource_fingerprint(lay_sub)

        ref = out["fps"]["absolu"]
        out["all_paths_equal"] = all(v == ref for v in out["fps"].values())
        out["all_paths_match_helper"] = all(
            datasource_fingerprints_match(ref, v) for v in out["fps"].values())
        out["uri_subset_equals_bare"] = out["fp_uri_subset"] == ref

        flog(
            f"tx_prov_fingerprint_split phase_paths: fps={out['fps']} "
            f"uri_subset={out['fp_uri_subset']!r} "
            f"all_equal={out['all_paths_equal']} trace_id={ctx.trace_id}",
            "INFO",
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
        flog(
            f"tx_prov_fingerprint_split phase_paths: exception={exc!r} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        try:
            os.chdir(cwd0)
        except Exception:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
    return out


def _phase_cwd_stability(ctx) -> dict:
    """L'empreinte doit etre independante du repertoire courant du processus."""
    from qgis.core import QgsVectorLayer
    from recoverland.core.identity import (
        compute_datasource_fingerprint, _normalize_source_uri,
    )
    from recoverland.core.logger import flog

    out = {"error": None}
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_cwd_")
    other = tempfile.mkdtemp(prefix="rl_tx_cwd_other_")
    cwd0 = os.getcwd()
    try:
        gpkg_layer, _path = _make_gpkg(tmpdir)

        csv_path = os.path.join(tmpdir, "t.csv")
        with open(csv_path, "w", encoding="utf-8", newline="") as fh:
            fh.write("nom,x,y\nALPHA,1,1\nBETA,2,2\n")
        csv_uri = ("file:///" + csv_path.replace("\\", "/")
                   + "?type=csv&xField=x&yField=y&crs=EPSG:4326")
        csv_layer = QgsVectorLayer(csv_uri, "csv", "delimitedtext")
        out["csv_valid"] = csv_layer.isValid()

        spatialite_uri = "dbname='{p}' table=\"zones\" (geom)".format(
            p=os.path.join(tmpdir, "db.sqlite").replace("\\", "/"))

        os.chdir(tmpdir)
        fp_ogr_a = compute_datasource_fingerprint(gpkg_layer)
        fp_csv_a = compute_datasource_fingerprint(csv_layer)
        fp_spl_a = _normalize_source_uri("spatialite", spatialite_uri)
        os.chdir(other)
        fp_ogr_b = compute_datasource_fingerprint(gpkg_layer)
        fp_csv_b = compute_datasource_fingerprint(csv_layer)
        fp_spl_b = _normalize_source_uri("spatialite", spatialite_uri)
        os.chdir(cwd0)

        out["ogr_stable"] = fp_ogr_a == fp_ogr_b
        out["csv_stable"] = fp_csv_a == fp_csv_b
        out["spatialite_stable"] = fp_spl_a == fp_spl_b
        out["fp_ogr"] = fp_ogr_a
        out["fp_csv_a"] = fp_csv_a
        out["fp_csv_b"] = fp_csv_b
        out["fp_spl_a"] = fp_spl_a
        out["fp_spl_b"] = fp_spl_b

        flog(
            f"tx_prov_fingerprint_split phase_cwd: ogr_stable={out['ogr_stable']} "
            f"csv_stable={out['csv_stable']} "
            f"spatialite_stable={out['spatialite_stable']} "
            f"trace_id={ctx.trace_id}",
            "INFO",
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = repr(exc)
        flog(
            f"tx_prov_fingerprint_split phase_cwd: exception={exc!r} "
            f"trace_id={ctx.trace_id}",
            "ERROR",
        )
    finally:
        try:
            os.chdir(cwd0)
        except Exception:
            pass
        shutil.rmtree(tmpdir, ignore_errors=True)
        shutil.rmtree(other, ignore_errors=True)
    return out


def setup(ctx):
    from recoverland.core.logger import flog
    flog(
        f"tx_prov_fingerprint_split setup: trace_id={ctx.trace_id} "
        f"subset={_SUBSET!r}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_prov_fingerprint_split run start: trace_id={ctx.trace_id}", "INFO")
    ctx.data["filter_split"] = _phase_filter_split(ctx)
    ctx.data["paths"] = _phase_path_writings(ctx)
    ctx.data["cwd"] = _phase_cwd_stability(ctx)
    flog(
        f"tx_prov_fingerprint_split run end: trace_id={ctx.trace_id} "
        f"fp_identical={ctx.data['filter_split'].get('fp_identical')} "
        f"restored={ctx.data['filter_split'].get('fully_restored')} "
        f"paths_equal={ctx.data['paths'].get('all_paths_equal')} "
        f"cwd={{k: v for k, v in ctx.data['cwd'].items() if k.endswith('stable')}}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    fs = ctx.data.get("filter_split") or {}
    paths = ctx.data.get("paths") or {}
    cwd = ctx.data.get("cwd") or {}

    # ===== Sanite ========================================================
    out.append((
        "filter_phase_ran",
        fs.get("error") is None,
        f"la phase filtre a plante : {fs.get('error')}",
    ))
    out.append((
        "fixture_connected_and_edited",
        fs.get("connected") is True and fs.get("edit_bare_ok") is True
        and fs.get("edit_filtered_ok") is True
        and fs.get("subset_applied") is True,
        f"connected={fs.get('connected')} edit_nu={fs.get('edit_bare_ok')} "
        f"subset={fs.get('subset_applied')} "
        f"edit_filtre={fs.get('edit_filtered_ok')} : sans ces quatre etapes "
        f"la demonstration ne porte sur rien.",
    ))
    out.append((
        "subset_really_filters",
        fs.get("visible_after_subset") == 2,
        f"entites visibles apres subset={fs.get('visible_after_subset')} "
        f"attendu=2 : le filtre doit reellement s'appliquer.",
    ))

    # ===== Le trou : le filtre scinde l'identite ========================
    out.append((
        "subset_does_not_change_the_fingerprint",
        fs.get("fp_identical") is True,
        f"empreinte sans filtre = {fs.get('fp_bare')!r}\n"
        f"      empreinte avec filtre = {fs.get('fp_filtered')!r}\n"
        f"Un filtre d'affichage n'est pas un changement de source de "
        f"donnees. Tant que les deux empreintes different, poser un filtre "
        f"ouvre un second historique pour le meme fichier et le rewind ne "
        f"voit plus qu'une moitie de ce que l'utilisateur a fait.",
    ))
    out.append((
        "fingerprints_match_helper_reconciles_the_filter",
        fs.get("fp_match_helper") is True,
        f"datasource_fingerprints_match(nu, filtre)="
        f"{fs.get('fp_match_helper')} : meme si les empreintes brutes "
        f"different, le comparateur utilise pour retrouver la couche cible "
        f"devrait les reconcilier. Il ne traite aujourd'hui que "
        f"l'ecart absolu/relatif.",
    ))
    out.append((
        "all_edits_land_under_one_fingerprint",
        fs.get("events_bare_fp", 0) >= 2,
        f"evenements sous l'empreinte non filtree="
        f"{fs.get('events_bare_fp')} (attendu 2 : l'edition avant filtre et "
        f"celle apres) ; evenements ranges sous l'empreinte filtree="
        f"{fs.get('events_filtered_fp')}. Les evenements produits pendant "
        f"que le filtre etait pose sont invisibles pour un rewind lance "
        f"filtre enleve.",
    ))

    # ===== Consequence utilisateur ======================================
    rw = fs.get("rewind") or {}
    out.append((
        "rewind_sees_the_whole_history",
        rw.get("fetched", 0) >= 2,
        f"rewind={rw} : le rewind lance sur la couche non filtree n'a "
        f"trouve que {rw.get('fetched')} evenement(s) alors que "
        f"l'utilisateur en a produit 2. Il n'aura aucun avertissement : "
        f"les evenements manquants ne sont pas 'ignores', ils sont "
        f"attribues a une autre couche.",
    ))
    out.append((
        "data_fully_returned_after_rewind",
        fs.get("fully_restored") is True,
        f"avant  = {fs.get('state_before')}\n"
        f"      apres editions = {fs.get('state_after_edits')}\n"
        f"      apres rewind   = {fs.get('state_after_rewind')}\n"
        f"      rewind={rw}\n"
        f"La modification faite pendant que le filtre etait pose survit au "
        f"rewind. L'utilisateur croit sa couche revenue a la date choisie ; "
        f"elle ne l'est pas, et rien ne le lui dit.",
    ))

    # ===== Ecritures equivalentes du meme chemin =========================
    out.append((
        "path_phase_ran",
        paths.get("error") is None,
        f"la phase chemins a plante : {paths.get('error')}",
    ))
    out.append((
        "same_file_written_four_ways_same_fingerprint",
        paths.get("all_paths_equal") is True,
        f"empreintes={paths.get('fps')} : chemin absolu, casse Windows "
        f"inversee, separateurs melanges et chemin relatif designent le "
        f"meme GeoPackage et doivent donner une empreinte identique, sinon "
        f"un projet deplace ou rouvert autrement perd son historique.",
    ))
    out.append((
        "uri_subset_at_open_time_is_the_same_source",
        paths.get("uri_subset_equals_bare") is True,
        f"empreinte avec |subset= a l'ouverture="
        f"{paths.get('fp_uri_subset')!r} vs sans="
        f"{(paths.get('fps') or {}).get('absolu')!r} : ouvrir deux fois le "
        f"meme GeoPackage, une fois nu et une fois filtre, produit deux "
        f"historiques distincts pour un seul fichier.",
    ))

    # ===== Stabilite au repertoire courant ==============================
    out.append((
        "cwd_phase_ran",
        cwd.get("error") is None,
        f"la phase repertoire courant a plante : {cwd.get('error')}",
    ))
    out.append((
        "ogr_fingerprint_independent_of_cwd",
        cwd.get("ogr_stable") is True,
        f"empreinte ogr instable selon le repertoire courant "
        f"({cwd.get('fp_ogr')!r}).",
    ))
    out.append((
        "delimitedtext_fingerprint_independent_of_cwd",
        cwd.get("csv_stable") is True,
        f"empreinte delimitedtext depuis deux repertoires courants :\n"
        f"      {cwd.get('fp_csv_a')!r}\n"
        f"      {cwd.get('fp_csv_b')!r}\n"
        f"L'URI 'file:///...' n'est pas un chemin : os.path.abspath la colle "
        f"derriere le repertoire de travail du processus. Deux lancements de "
        f"QGIS depuis deux dossiers = deux historiques pour le meme fichier.",
    ))
    out.append((
        "spatialite_fingerprint_independent_of_cwd",
        cwd.get("spatialite_stable") is True,
        f"empreinte spatialite depuis deux repertoires courants :\n"
        f"      {cwd.get('fp_spl_a')!r}\n"
        f"      {cwd.get('fp_spl_b')!r}\n"
        f"spatialite est declare SupportLevel.FULL / IdentityStrength.STRONG "
        f"avec capture ET restore autorises : c'est le provider le mieux note "
        f"de la matrice apres postgres. Son URI ('dbname=... table=...') n'est "
        f"pas un chemin, donc son empreinte depend du repertoire de lancement "
        f"de QGIS et son historique se scinde silencieusement d'une session a "
        f"l'autre.",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_prov_fingerprint_split.*trace_id={ctx.trace_id}",
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
