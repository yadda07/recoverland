# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_prov_spatialite_filter_split - RecoverLand validation runtime.

Invariant: I-11 (une empreinte de datasource designe UNE table, quel que
soit le filtre d'affichage ; une couche suivie n'arrete jamais d'etre
capturee en silence).

Le trou que ce scenario prouve
------------------------------
Une source spatialite est une chaine de connexion ``dbname='...'
table="zones" (geometry)``. `identity._normalize_file_source` la gardait
TELLE QUELLE, y compris ``sql=<filtre>`` que QGIS y ajoute des qu'un filtre
est pose (`setSubsetString`, Couche > Filtrer), ``key=`` et l'ordre dans
lequel `QgsDataSourceUri` re-serialise. La v6 avait corrige ce defaut pour
les fichiers OGR (``|subset=``), pas pour spatialite.

Consequences utilisateur, deroulees sur une vraie base spatialite :

  * t1 edition sans filtre, t2 filtre ``commune = 'X'``, t3 edition, t4
    filtre retire et rewind avant t1 : l'edition de t3 est rangee sous une
    autre empreinte, le rewind ne la voit pas et annonce un succes ;
  * avec une selection de suivi, le projet rouvert avec son filtre
    enregistre donne une couche dont l'empreinte n'est plus dans la
    selection : elle n'est plus capturee du tout, sans un mot ;
  * un journal 6.x qui contient deja des evenements sous l'empreinte
    filtree doit les voir rattaches a la table par la reconciliation.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_prov_spatialite_filter_split"
INVARIANT = "I-11"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_SEED = [("ALPHA", "X", 1.0), ("BETA", "Y", 2.0), ("GAMMA", "X", 3.0),
         ("DELTA", "Y", 4.0)]
_SUBSET = "commune = 'X'"


def _make_spatialite(tmpdir, name="zones.sqlite"):
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    mem = QgsVectorLayer("Point?crs=EPSG:4326&field=nom:string(20)"
                         "&field=commune:string(5)", "seed", "memory")
    mem.startEditing()
    for nom, commune, x in _SEED:
        feat = QgsFeature(mem.fields())
        feat.setAttributes([nom, commune])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, x)))
        mem.addFeature(feat)
    mem.commitChanges()
    path = os.path.join(tmpdir, name).replace("\\", "/")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "SQLite"
    opts.layerName = "zones"
    opts.datasourceOptions = ["SPATIALITE=YES"]
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"ecriture spatialite impossible: {err}")
    return path


def _open(path, subset=None, name="zones"):
    from qgis.core import QgsVectorLayer
    source = f"dbname='{path}' table=\"zones\" (geometry)"
    if subset:
        source += f" sql={subset}"
    layer = QgsVectorLayer(source, name, "spatialite")
    if not layer.isValid():
        raise RuntimeError(f"couche spatialite invalide: {source}")
    return layer


def _state(layer):
    return sorted((f["nom"], f["commune"]) for f in layer.getFeatures())


def _rename(layer, old, new):
    idx = layer.fields().indexOf("nom")
    fid = next((f.id() for f in layer.getFeatures() if f["nom"] == old), None)
    if fid is None or not layer.startEditing():
        return False
    if not layer.changeAttributeValue(fid, idx, new):
        layer.rollBack()
        return False
    return bool(layer.commitChanges())


def _phase_cycle(ctx, d):
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import (
        compute_datasource_fingerprint, datasource_fingerprints_match,
    )
    from scripts.validation.scenarios.tx_prov_fingerprint_split import (
        _open_journal, _WriteQueueBarrier, _events_for, _rewind,
    )
    tmpdir = tempfile.mkdtemp(prefix="rl_spl_cyc_")
    jrnl = tempfile.mkdtemp(prefix="rl_spl_cyc_j_")
    jm = wq = None
    try:
        path = _make_spatialite(tmpdir)
        layer = _open(path)
        d["state_before"] = _state(layer)
        jm, wq = _open_journal(jrnl)
        barrier = _WriteQueueBarrier(wq)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.connect_layer(layer)
        d["connected"] = layer.id() in tracker._connected_layers
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()

        fp_bare = compute_datasource_fingerprint(layer)
        d["fp_bare"] = fp_bare
        d["edit_bare"] = _rename(layer, "ALPHA", "EDIT_NU")
        barrier.wait(jm)
        d["subset_applied"] = bool(layer.setSubsetString(_SUBSET))
        d["source_filtered"] = layer.source()
        d["fp_filtered"] = compute_datasource_fingerprint(layer)
        d["fp_match"] = datasource_fingerprints_match(fp_bare, d["fp_filtered"])
        d["visible_filtered"] = layer.featureCount()
        d["edit_filtered"] = _rename(layer, "GAMMA", "EDIT_FILTRE")
        barrier.wait(jm)
        d["events_bare_fp"] = len(_events_for(jm, fp_bare, expected_min=2,
                                              timeout_s=3.0))
        layer.setSubsetString("")
        with tracker.suppressed():
            d["rewind"] = _rewind(jm, layer, fp_bare, cutoff, ctx.trace_id)
            layer.reload()
            d["state_after_rewind"] = _state(layer)
        try:
            tracker.disconnect_all()
        except Exception:  # noqa: BLE001
            pass
        layer = None
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


def _phase_selection(ctx, d):
    """Projet rouvert avec son filtre enregistre, selection de suivi 6.x."""
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from scripts.validation.scenarios.tx_prov_fingerprint_split import (
        _open_journal, _WriteQueueBarrier, _journal_row_count,
    )
    tmpdir = tempfile.mkdtemp(prefix="rl_spl_sel_")
    jrnl = tempfile.mkdtemp(prefix="rl_spl_sel_j_")
    jm = wq = None
    try:
        path = _make_spatialite(tmpdir)
        # La selection a ete cochee sans filtre : forme verbatim non filtree.
        selection = {"spatialite::" + _open(path, name="probe").source()}
        filtered = _open(path, subset=_SUBSET, name="zones_filtre")
        jm, wq = _open_journal(jrnl)
        barrier = _WriteQueueBarrier(wq)
        tracker = EditSessionTracker(wq, jm)
        tracker.activate()
        tracker.set_filter(selection)
        tracker.connect_layer(filtered)
        d["sel_connected"] = filtered.id() in tracker._connected_layers
        d["sel_edit"] = _rename(filtered, "GAMMA", "EDIT_SEL")
        barrier.wait(jm, timeout_s=5.0)
        d["sel_rows"] = _journal_row_count(jm)
        # Selection cochee pendant qu'un filtre etait pose (forme 6.x).
        selection_f = {"spatialite::" + filtered.source()}
        tracker2 = EditSessionTracker(wq, jm)
        tracker2.set_filter(selection_f)
        bare = _open(path, name="zones_nu")
        d["sel_bare_tracked"] = tracker2._is_tracked(
            compute_datasource_fingerprint(bare))
        try:
            tracker.disconnect_all()
            tracker2.disconnect_all()
        except Exception:  # noqa: BLE001
            pass
        filtered = bare = None
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


def _phase_legacy_journal(ctx, d):
    """Journal 6.x : evenements deja ranges sous l'empreinte filtree."""
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.datasource_alias import (
        RECONCILE_SETTING_KEY, list_aliases,
    )
    from recoverland.core.identity import compute_fingerprint_for_source
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.event_stream_repository import count_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType

    tmpdir = tempfile.mkdtemp(prefix="rl_spl_leg_")
    try:
        dbfile = "C:/data/cadastre.sqlite"
        uri_bare = f"dbname='{dbfile}' table=\"zones\" (geometry)"
        uri_filtered = uri_bare + " sql=" + _SUBSET
        fp_bare_v6 = "spatialite::" + uri_bare
        fp_filtered_v6 = "spatialite::" + uri_filtered
        project_path = str(Path(tmpdir) / "projet.qgz")
        Path(project_path).write_text("<qgis/>", encoding="utf-8")
        db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        t0 = datetime(2026, 9, 10, 8, 0, tzinfo=timezone.utc)
        conn = sqlite3.connect(db_path)
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
                  + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
        for i, fp in enumerate((fp_bare_v6, fp_filtered_v6, fp_filtered_v6)):
            conn.execute(insert, (
                "p", fp, "lyr_z", "zones", "spatialite",
                json.dumps({"fid": i + 1, "pk_field": "ogc_fid",
                            "pk_value": i + 1}),
                "UPDATE", json.dumps({"changed_only": {"nom": {
                    "old": "a", "new": "b"}}}),
                None, "Point", "EPSG:4326", None, "tester", "s",
                (t0 + timedelta(minutes=i)).isoformat(), None,
                f"pk:ogc_fid={i + 1}", 6, None, None))
        now = t0.isoformat()
        for fp, uri in ((fp_bare_v6, uri_bare), (fp_filtered_v6, uri_filtered)):
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
                (fp, "spatialite", uri, "zones", "", "EPSG:4326", "Point", now))
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES (?, ?, ?)",
            (RECONCILE_SETTING_KEY, json.dumps({
                "status": "ok", "scanned": 2, "aliases_created": 0,
                "already_canonical": 2, "already_aliased": 0, "ambiguous": [],
                "without_source_uri": [], "completed_at": now}), now))
        conn.execute("INSERT OR REPLACE INTO schema_version (version_number, "
                     "applied_at, description) VALUES (6, ?, 'fixture v6')",
                     (now,))
        conn.commit()
        conn.close()

        canonical = compute_fingerprint_for_source("spatialite", uri_bare)
        d["legacy_canonical_is_v6_bare"] = canonical == fp_bare_v6
        jm = JournalManager()
        try:
            jm.open_for_project(project_path, tmpdir)
            rconn = jm.get_connection()
            d["legacy_aliases"] = [(a[0], a[1]) for a in list_aliases(rconn)]
            d["legacy_count"] = count_events_after_cutoff(
                rconn, canonical,
                RestoreCutoff(CutoffType.BY_DATE,
                              (t0 - timedelta(hours=1)).isoformat(), True),
                trace_id="spl")
        finally:
            jm.close()
        d["legacy_fp_filtered"] = fp_filtered_v6
        d["legacy_fp_canonical"] = canonical
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_prov_spatialite_filter_split setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    d["errors"] = {}
    for name, phase in (("cycle", _phase_cycle), ("selection", _phase_selection),
                        ("legacy", _phase_legacy_journal)):
        try:
            phase(ctx, d)
        except Exception as exc:  # noqa: BLE001
            d["errors"][name] = repr(exc)
    flog(f"tx_prov_spatialite_filter_split run: trace_id={ctx.trace_id} "
         f"fp_bare={d.get('fp_bare')!r} fp_filtered={d.get('fp_filtered')!r} "
         f"rewind={d.get('rewind')} sel_connected={d.get('sel_connected')} "
         f"legacy_aliases={d.get('legacy_aliases')} errors={d['errors']}",
         "INFO" if not d["errors"] else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    rw = d.get("rewind") or {}
    return [
        ("phases_ran", not d.get("errors"),
         f"phases en erreur : {d.get('errors')}"),
        ("fixture_filters_really",
         d.get("connected") is True and d.get("subset_applied") is True
         and d.get("visible_filtered") == 2
         and d.get("edit_bare") is True and d.get("edit_filtered") is True,
         f"connected={d.get('connected')} subset={d.get('subset_applied')} "
         f"visibles={d.get('visible_filtered')} editions="
         f"{d.get('edit_bare')}/{d.get('edit_filtered')} : sans ces etapes le "
         f"scenario ne prouve rien."),
        ("filter_does_not_change_the_fingerprint",
         d.get("fp_bare") == d.get("fp_filtered"),
         f"empreinte sans filtre={d.get('fp_bare')!r}\n      avec filtre="
         f"{d.get('fp_filtered')!r} (source={d.get('source_filtered')!r}) : "
         f"un filtre d'affichage ouvre un second historique pour la meme "
         f"table spatialite."),
        ("fingerprints_match_helper_reconciles_the_filter",
         d.get("fp_match") is True,
         f"datasource_fingerprints_match(nu, filtre)={d.get('fp_match')}"),
        ("rewind_sees_both_edits",
         rw.get("fetched", 0) >= 2 and d.get("events_bare_fp", 0) >= 2,
         f"evenements sous l'empreinte non filtree={d.get('events_bare_fp')} "
         f"rewind={rw} : l'edition faite filtre pose est invisible pour un "
         f"rewind lance filtre retire."),
        ("data_fully_returned_after_rewind",
         d.get("state_after_rewind") == d.get("state_before"),
         f"avant={d.get('state_before')}\n      apres rewind="
         f"{d.get('state_after_rewind')} : l'utilisateur croit sa couche "
         f"revenue a la date choisie, elle ne l'est pas."),
        ("tracked_layer_reopened_with_filter_is_captured",
         d.get("sel_connected") is True and d.get("sel_edit") is True
         and (d.get("sel_rows") or 0) >= 1,
         f"couche rouverte avec son filtre, selection de suivi 6.x : "
         f"connectee={d.get('sel_connected')} edition={d.get('sel_edit')} "
         f"evenements journalises={d.get('sel_rows')} : une couche suivie "
         f"n'est plus capturee du tout et rien ne le signale."),
        ("selection_saved_with_filter_still_tracks_the_table",
         d.get("sel_bare_tracked") is True,
         f"selection enregistree filtre pose, couche ouverte sans filtre -> "
         f"suivie={d.get('sel_bare_tracked')!r}"),
        ("unfiltered_fingerprint_kept_byte_for_byte",
         d.get("legacy_canonical_is_v6_bare") is True,
         f"empreinte canonique={d.get('legacy_fp_canonical')!r} : une source "
         f"spatialite sans filtre doit garder son empreinte 6.x, sinon tout "
         f"son historique a besoin d'un alias."),
        ("legacy_filtered_history_attached",
         (d.get("legacy_fp_filtered"), d.get("legacy_fp_canonical"))
         in [tuple(a) for a in d.get("legacy_aliases") or []]
         and d.get("legacy_count") == 3,
         f"alias={d.get('legacy_aliases')} evenements dans la portee de la "
         f"table={d.get('legacy_count')} (attendu 3) : les editions faites "
         f"filtre pose sous 6.x doivent rejoindre l'historique de la table."),
        assert_log_contains(
            ctx.records,
            rf"tx_prov_spatialite_filter_split run: trace_id={ctx.trace_id}",
            name="trace_id_propagated"),
    ]


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
