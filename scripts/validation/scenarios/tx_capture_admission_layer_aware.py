# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_admission_layer_aware - RecoverLand validation runtime.

Invariant: I-CAPTURE-03 (une couche que la selection de suivi designe est
capturee ; une couche qu'elle ne designe pas ne l'est pas, et l'utilisateur
le sait).

Le mecanisme
------------
La selection des couches suivies vit dans QgsSettings sous la forme
d'empreinte en vigueur le jour ou l'utilisateur a coche la case : forme
historique (5.x), forme v6, forme canonique v7. La boite de dialogue decide
"cochee" avec `identity.layer_is_selected(layer, selection)`, qui recalcule
ces formes DEPUIS LA COUCHE. La capture, elle, decidait dans
`EditSessionTracker.connect_layer` avec `_is_tracked(empreinte)`, qui ne
voit qu'une chaine : il ne peut pas retrouver une forme historique qui ne
se deduit pas de la forme canonique. Deux consequences :

  * une couche Spatialite cochee sous 5.x est affichee cochee et n'est
    JAMAIS branchee : ses modifications ne sont pas capturees, sans
    message (la boite dit "suivie") ;
  * la seule chose qui fait encore passer une couche PostgreSQL atteinte
    par un SERVICE et cochee sous 6.x (forme sans service) est la
    tolerance "service" de `datasource_fingerprints_match`. Cette tolerance
    doit disparaitre (elle laisse aussi ecrire des evenements d'une autre
    base) : ce jour-la la capture de ces couches devient muette. Le cas A2
    simule ce retrait.

Meme defaut quand la source d'une couche branchee change
(`dataSourceChanged`) : l'empreinte etait remplacee sans nouvelle
admission. Repointee vers une source hors selection, la couche restait
"branchee" mais chaque signal la refusait : rien capture, et l'alerte
"cette couche n'est pas suivie" ne partait jamais (elle ne concerne que
les couches non branchees). Repointee vers une source cochee sous une
forme historique, ses modifications etaient perdues en silence.

Cas
---
A1  Spatialite, selection = forme historique 5.x : branchee, et une
    modification est journalisee.
A2  PostgreSQL par service (identite simulee sur un GeoPackage), selection
    = forme v6 sans service, tolerance "service" retiree : branchee, et une
    modification est journalisee.
A3  temoin : couche hors selection, jamais branchee, alerte a l'edition.
A4  couche branchee repointee vers une source hors selection : debranchee,
    alerte a l'edition, rien journalise ; ramenee sur sa source : de
    nouveau capturee.
A5  couche branchee repointee vers une source cochee sous sa forme
    historique : capturee.

Verdict attendu avant correctif : FAIL (A1, A2, A4, A5).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_admission_layer_aware"
INVARIANT = "I-CAPTURE-03"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_URI_SERVICE = "service='prod' key='fid' table=\"public\".\"routes\" (geom)"
_FP_V6_SERVICELESS = "postgres::host= port=5432 dbname= schema=public table=routes"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_gpkg(path: str) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("nom", ogr.OFTString))
    for i, nom in enumerate(("PONT", "GARE")):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("nom", nom)
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i + 1} {i + 1})"))
        lyr.CreateFeature(feat)
    ds = None


def _make_spatialite(path: str) -> None:
    from osgeo import ogr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("SQLite").CreateDataSource(
        path, options=["SPATIALITE=YES"])
    ds.ExecuteSQL("CREATE TABLE pts (pk INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT)")
    ds.ExecuteSQL("SELECT AddGeometryColumn('pts', 'geom', 2154, 'POINT', 'XY')")
    ds.ExecuteSQL("INSERT INTO pts (nom, geom) VALUES ('PONT', MakePoint(1, 1, 2154))")
    ds.ExecuteSQL("INSERT INTO pts (nom, geom) VALUES ('GARE', MakePoint(2, 2, 2154))")
    ds = None


def _spatialite_uri(path: str) -> str:
    from qgis.core import QgsDataSourceUri
    uri = QgsDataSourceUri()
    uri.setDatabase(path)
    uri.setDataSource("", "pts", "geom", "", "pk")
    return uri.uri()


def _gpkg_layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


def _spatialite_layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(_spatialite_uri(path), name, "spatialite")
    if not layer.isValid():
        raise RuntimeError(f"invalid spatialite layer {path}")
    return layer


class _ProviderAs:
    """The real provider, answering another provider name."""

    def __init__(self, real, name):
        self._real = real
        self._name = name

    def name(self):
        return self._name

    def __getattr__(self, attr):
        return getattr(self._real, attr)


def _service_layer(path: str, name: str):
    """A GeoPackage layer whose Python-visible identity is a PostgreSQL table
    reached through a connection service (no server needed)."""
    from qgis.core import QgsVectorLayer

    class _ServiceLayer(QgsVectorLayer):
        def source(self):  # noqa: D401 - Python-side override
            return _URI_SERVICE

        def dataProvider(self):  # noqa: N802
            return _ProviderAs(QgsVectorLayer.dataProvider(self), "postgres")

    layer = _ServiceLayer(f"{path}|layername=pts", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


class _Stack:
    def __init__(self, workdir: str):
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker

        os.makedirs(workdir, exist_ok=True)
        self.jm = JournalManager()
        self.jm.open_for_project(
            project_path=os.path.join(workdir, "audit.qgz"), profile_path=workdir)
        self.path = self.jm.path
        self.wq = WriteQueue()
        self.wq.start(self.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.statuses = []
        if hasattr(self.tracker, "set_capture_status_callback"):
            self.tracker.set_capture_status_callback(
                lambda kind, detail: self.statuses.append((kind, dict(detail))))
        self.tracker.activate()

    def connected(self, layer) -> bool:
        return layer.id() in self.tracker._connected_layers

    def shutdown(self) -> None:
        try:
            self.tracker.deactivate()
            self.tracker.disconnect_all()
        finally:
            self.wq.stop()
            self.jm.close()


def _pump(seconds: float = 0.0) -> None:
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + seconds
    while True:
        QCoreApplication.processEvents()
        if time.monotonic() >= deadline:
            return
        time.sleep(0.02)


def _rename_first(layer, new_name: str) -> bool:
    fid = min(f.id() for f in layer.getFeatures())
    layer.startEditing()
    layer.changeAttributeValue(fid, layer.fields().indexFromName("nom"), new_name)
    ok = bool(layer.commitChanges())
    _pump(0.2)
    return ok


def _journal_updates(journal_path: str) -> list:
    conn = sqlite3.connect(f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT datasource_fingerprint, attributes_json FROM audit_event "
            "WHERE operation_type = 'UPDATE' ORDER BY event_id").fetchall()
    finally:
        conn.close()
    out = []
    for fp, attrs in rows:
        try:
            changed = (json.loads(attrs or "{}") or {}).get("changed_only") or {}
        except ValueError:
            changed = {}
        out.append({"fp": fp, "nom": (changed.get("nom") or {}).get("new")})
    return out


def _untracked(statuses, layer_id) -> list:
    return [d.get("reason") for k, d in statuses
            if k == "untracked_edit" and d.get("layer_id") == layer_id]


def _strict_service_matching():
    """Simulate the planned removal of the "service" leniency."""
    from recoverland.core import identity as ident
    from recoverland.core import edit_tracker as et
    orig = ident.datasource_fingerprints_match
    unproven = getattr(ident, "service_match_unproven", None)

    def strict(stored, current):
        if unproven is not None and unproven(stored, current):
            return False
        return orig(stored, current)

    ident.datasource_fingerprints_match = strict
    et.datasource_fingerprints_match = strict

    def restore():
        ident.datasource_fingerprints_match = orig
        et.datasource_fingerprints_match = orig
    return restore


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def _case_a1(ctx) -> dict:
    from recoverland.core import identity as ident
    work = os.path.join(ctx.data["tmpdir"], "a1")
    stack = _Stack(work)
    db = os.path.join(work, "histo.sqlite")
    _make_spatialite(db)
    layer = _spatialite_layer(db, "a1")
    legacy = ident.legacy_fingerprint_for_source("spatialite", layer.source())
    canonical = ident.compute_datasource_fingerprint(layer)
    stack.tracker.set_filter({legacy})
    res = {"legacy": legacy[-90:], "canonical": canonical[-90:],
           "forms_differ": legacy != canonical,
           "dialog_shows_ticked": ident.layer_is_selected(layer, {legacy}),
           "string_gate_matches": stack.tracker._is_tracked(canonical)}
    stack.tracker.connect_layer(layer)
    res["connected"] = stack.connected(layer)
    res["commit_ok"] = _rename_first(layer, "PONT_A1")
    stack.shutdown()
    res["updates"] = _journal_updates(stack.path)
    return res


def _case_a2(ctx) -> dict:
    from recoverland.core import identity as ident
    work = os.path.join(ctx.data["tmpdir"], "a2")
    path = os.path.join(work, "service.gpkg")
    os.makedirs(work, exist_ok=True)
    _make_gpkg(path)
    restore = _strict_service_matching()
    try:
        stack = _Stack(work)
        layer = _service_layer(path, "a2")
        fp = ident.compute_datasource_fingerprint(layer)
        v6 = getattr(ident, "v6_fingerprint_for_source", None)
        selection = {v6("postgres", _URI_SERVICE) if v6 else _FP_V6_SERVICELESS}
        stack.tracker.set_filter(selection)
        res = {"fp": fp, "selection": sorted(selection),
               "dialog_shows_ticked": ident.layer_is_selected(layer, selection),
               "leniency_removed": not ident.datasource_fingerprints_match(
                   next(iter(selection)), fp)}
        stack.tracker.connect_layer(layer)
        res["connected"] = stack.connected(layer)
        res["commit_ok"] = _rename_first(layer, "PONT_A2")
        stack.shutdown()
    finally:
        restore()
    res["updates"] = _journal_updates(stack.path)
    return res


def _case_a3(ctx) -> dict:
    from recoverland.core import identity as ident
    work = os.path.join(ctx.data["tmpdir"], "a3")
    stack = _Stack(work)
    tracked = os.path.join(work, "suivie.gpkg")
    other = os.path.join(work, "autre.gpkg")
    _make_gpkg(tracked)
    _make_gpkg(other)
    stack.tracker.set_filter({ident.compute_datasource_fingerprint(
        _gpkg_layer(tracked, "a3_sel"))})
    layer = _gpkg_layer(other, "a3")
    stack.tracker.connect_layer(layer)
    res = {"connected": stack.connected(layer),
           "dialog_shows_ticked": ident.layer_is_selected(
               layer, stack.tracker._allowed_layer_fingerprints)}
    res["commit_ok"] = _rename_first(layer, "PONT_A3")
    res["untracked"] = _untracked(stack.statuses, layer.id())
    stack.shutdown()
    res["updates"] = _journal_updates(stack.path)
    return res


def _case_a4(ctx) -> dict:
    from recoverland.core import identity as ident
    work = os.path.join(ctx.data["tmpdir"], "a4")
    stack = _Stack(work)
    src_a = os.path.join(work, "a.gpkg")
    src_b = os.path.join(work, "b.gpkg")
    _make_gpkg(src_a)
    _make_gpkg(src_b)
    layer = _gpkg_layer(src_a, "a4")
    stack.tracker.set_filter({ident.compute_datasource_fingerprint(layer)})
    stack.tracker.connect_layer(layer)
    res = {"connected_on_a": stack.connected(layer)}
    layer.setDataSource(f"{src_b}|layername=pts", "a4", "ogr")
    _pump(0.1)
    res["source_b"] = layer.source().endswith("b.gpkg|layername=pts")
    res["connected_on_b"] = stack.connected(layer)
    res["commit_b_ok"] = _rename_first(layer, "PONT_B")
    res["untracked_on_b"] = _untracked(stack.statuses, layer.id())
    layer.setDataSource(f"{src_a}|layername=pts", "a4", "ogr")
    _pump(0.1)
    res["connected_back_on_a"] = stack.connected(layer)
    res["commit_a_ok"] = _rename_first(layer, "PONT_A")
    stack.shutdown()
    res["updates"] = _journal_updates(stack.path)
    return res


def _case_a5(ctx) -> dict:
    from recoverland.core import identity as ident
    work = os.path.join(ctx.data["tmpdir"], "a5")
    stack = _Stack(work)
    src_a = os.path.join(work, "a.gpkg")
    db = os.path.join(work, "histo.sqlite")
    _make_gpkg(src_a)
    _make_spatialite(db)
    layer = _gpkg_layer(src_a, "a5")
    legacy = ident.legacy_fingerprint_for_source("spatialite", _spatialite_uri(db))
    stack.tracker.set_filter({ident.compute_datasource_fingerprint(layer), legacy})
    stack.tracker.connect_layer(layer)
    res = {"connected_on_a": stack.connected(layer)}
    layer.setDataSource(_spatialite_uri(db), "a5", "spatialite")
    _pump(0.1)
    res["provider_now"] = layer.dataProvider().name()
    res["dialog_shows_ticked"] = ident.layer_is_selected(
        layer, stack.tracker._allowed_layer_fingerprints)
    res["connected_on_s"] = stack.connected(layer)
    res["commit_ok"] = _rename_first(layer, "PONT_A5")
    stack.shutdown()
    res["updates"] = _journal_updates(stack.path)
    return res


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_k_adm_")
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    for key, fn in (("a1", _case_a1), ("a2", _case_a2), ("a3", _case_a3),
                    ("a4", _case_a4), ("a5", _case_a5)):
        try:
            ctx.data[key] = fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-900:]}
        flog(f"{SCENARIO_ID} case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:2000]}", "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _noms(res) -> list:
    return [u.get("nom") for u in res.get("updates") or []]


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    out = []
    a1, a2, a3 = ctx.data.get("a1", {}), ctx.data.get("a2", {}), ctx.data.get("a3", {})
    a4, a5 = ctx.data.get("a4", {}), ctx.data.get("a5", {})

    # ===== A1: Spatialite ticked under 5.x =============================
    out.append((
        "a1_fixture_historical_form_only_the_layer_can_rebuild",
        a1.get("forms_differ") is True and a1.get("dialog_shows_ticked") is True
        and a1.get("string_gate_matches") is False,
        f"formes={a1.get('legacy')} / {a1.get('canonical')} "
        f"dialogue_coche={a1.get('dialog_shows_ticked')} "
        f"test_sur_chaine={a1.get('string_gate_matches')} crash={a1.get('crash')} "
        f"{a1.get('trace', '')}",
    ))
    out.append((
        "a1_layer_ticked_under_5x_is_captured",
        a1.get("connected") is True and a1.get("commit_ok") is True
        and _noms(a1) == ["PONT_A1"],
        f"branchee={a1.get('connected')} commit={a1.get('commit_ok')} "
        f"UPDATE journalises={_noms(a1)} attendu ['PONT_A1']. La boite dit "
        f"'suivie', la modification n'est pas au journal",
    ))

    # ===== A2: service layer, leniency removed ==========================
    out.append((
        "a2_fixture_v6_selection_and_no_leniency",
        a2.get("dialog_shows_ticked") is True and a2.get("leniency_removed") is True,
        f"selection={a2.get('selection')} empreinte={a2.get('fp')} "
        f"dialogue_coche={a2.get('dialog_shows_ticked')} "
        f"tolerance_retiree={a2.get('leniency_removed')} crash={a2.get('crash')} "
        f"{a2.get('trace', '')}",
    ))
    out.append((
        "a2_service_layer_ticked_under_6x_stays_captured",
        a2.get("connected") is True and a2.get("commit_ok") is True
        and _noms(a2) == ["PONT_A2"],
        f"branchee={a2.get('connected')} commit={a2.get('commit_ok')} "
        f"UPDATE={_noms(a2)} attendu ['PONT_A2'] : sans la tolerance 'service' "
        f"la capture de cette couche deviendrait muette",
    ))

    # ===== A3: control, not in the selection ===========================
    out.append((
        "a3_unselected_layer_is_not_captured_and_the_user_is_told",
        a3.get("connected") is False and a3.get("dialog_shows_ticked") is False
        and a3.get("commit_ok") is True and _noms(a3) == []
        and "not_in_tracking_selection" in (a3.get("untracked") or []),
        f"branchee={a3.get('connected')} coche={a3.get('dialog_shows_ticked')} "
        f"UPDATE={_noms(a3)} alertes={a3.get('untracked')} crash={a3.get('crash')} "
        f"{a3.get('trace', '')} : l'admission ne doit pas elargir la selection",
    ))

    # ===== A4: repointed outside the selection =========================
    out.append((
        "a4_fixture_repointed",
        a4.get("connected_on_a") is True and a4.get("source_b") is True,
        f"branchee_sur_a={a4.get('connected_on_a')} source_b={a4.get('source_b')} "
        f"crash={a4.get('crash')} {a4.get('trace', '')}",
    ))
    out.append((
        "a4_repointed_outside_selection_is_released_and_announced",
        a4.get("connected_on_b") is False
        and "not_in_tracking_selection" in (a4.get("untracked_on_b") or []),
        f"branchee_sur_b={a4.get('connected_on_b')} alertes={a4.get('untracked_on_b')}"
        f" : la couche restait 'branchee' sans rien capturer, et l'alerte "
        f"'non suivie' ne partait jamais",
    ))
    out.append((
        "a4_only_the_selected_source_is_journalled",
        a4.get("commit_b_ok") is True and a4.get("commit_a_ok") is True
        and a4.get("connected_back_on_a") is True and _noms(a4) == ["PONT_A"],
        f"commits b/a={a4.get('commit_b_ok')}/{a4.get('commit_a_ok')} "
        f"rebranchee_sur_a={a4.get('connected_back_on_a')} UPDATE={_noms(a4)} "
        f"attendu ['PONT_A']",
    ))

    # ===== A5: repointed to a source ticked under its historical form ==
    out.append((
        "a5_repointed_to_a_historically_ticked_source_is_captured",
        a5.get("connected_on_a") is True and a5.get("provider_now") == "spatialite"
        and a5.get("dialog_shows_ticked") is True
        and a5.get("commit_ok") is True and _noms(a5) == ["PONT_A5"],
        f"branchee_sur_a={a5.get('connected_on_a')} provider={a5.get('provider_now')} "
        f"coche={a5.get('dialog_shows_ticked')} commit={a5.get('commit_ok')} "
        f"UPDATE={_noms(a5)} attendu ['PONT_A5'] crash={a5.get('crash')} "
        f"{a5.get('trace', '')}",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID}.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=5))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT))
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
