# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_capture_schema_change_same_session - RecoverLand validation runtime.

Invariant: I-CAPTURE-01 (toute modification ecrite par le provider est
journalisee avec la bonne valeur d'avant et le bon nom de champ) et
I-CAPTURE-04 (une couche editee sans etre capturee le dit).

Le mecanisme
------------
Le tampon d'edition indexe ses changements de valeurs sur les champs
COURANTS de la couche. `EditSessionTracker` traduisait ces index avec les
noms des champs du PROVIDER avant commit. Une suppression de champ en
attente decale tous les index suivants ; un renommage en attente change le
nom. Le delta etait calcule sur de mauvais noms, revenait vide, et l'UPDATE
etait jete comme un "faux commit" alors que le fichier changeait.

Cas
---
S1  suppression du champ `a` + modification de `c` dans la meme session :
    l'UPDATE de `c` (ancienne -> nouvelle valeur) est journalise.
S2  renommage de `nom` en `libelle` + modification de sa valeur : l'UPDATE
    est journalise sous le nouveau nom avec l'ancienne valeur.
S3  ajout d'un champ `x` + valeur sur une entite : l'UPDATE porte x
    NULL -> valeur.
S4  une couche refusee au chargement (source absente) puis reparee
    (setDataSource) est capturee ensuite, sans rouvrir la fenetre.
S5  une couche exclue de la selection suivie passe en edition : une alerte
    part (statut `untracked_edit`), l'utilisateur sait que rien ne sera
    enregistre.

Verdict attendu avant correctif : FAIL (S1, S2, S4, S5).
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_capture_schema_change_same_session"
INVARIANT = "I-CAPTURE-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _gpkg(tmpdir, name):
    from qgis.core import (QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
                           QgsVectorFileWriter, QgsCoordinateTransformContext)
    mem = QgsVectorLayer("Point?crs=EPSG:2154&field=a:string(10)&field=nom:string(20)"
                         "&field=c:string(20)", "m", "memory")
    mem.startEditing()
    for i in range(3):
        f = QgsFeature(mem.fields())
        f.setAttributes([f"A{i}", f"NOM{i}", f"C{i}"])
        f.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i, i)))
        mem.addFeature(f)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(mem, path, QgsCoordinateTransformContext(), opts)
    return path


def _layer(path, name):
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    return lyr


def _deltas(wq_events, op="UPDATE"):
    out = []
    for e in wq_events:
        if e.operation_type != op:
            continue
        try:
            out.append((json.loads(e.attributes_json or "{}").get("changed_only") or {}))
        except ValueError:
            out.append({"raw": e.attributes_json})
    return out


class _CapturingWQ:
    """In-memory write queue: this scenario is about WHAT is captured."""

    def __init__(self):
        self.events = []

    def enqueue(self, events, commit=None, journal_path=None):
        self.events.extend(events)
        return True

    def prepare_commit(self, commit_id, payload, journal_path=None):
        return True


class _DummyJM:
    def get_connection(self):
        raise RuntimeError("no journal in this scenario")


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_tx_schema_")
    flog(f"tx_capture_schema_change_same_session setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsField, QgsProject
    from qgis.PyQt.QtCore import QMetaType
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog
    from scripts.validation.plugin_harness import pump

    d = ctx.data
    tmp = d["tmpdir"]
    wq = _CapturingWQ()
    tracker = EditSessionTracker(wq, _DummyJM())
    statuses = []
    if hasattr(tracker, "set_capture_status_callback"):
        tracker.set_capture_status_callback(lambda k, det: statuses.append((k, det)))
    tracker.activate()
    try:
        # ---- S1: delete field a + change c -----------------------------
        l1 = _layer(_gpkg(tmp, "s1"), "s1")
        tracker.connect_layer(l1)
        fid = sorted(f.id() for f in l1.getFeatures())[0]
        l1.startEditing()
        l1.deleteAttribute(l1.fields().indexFromName("a"))
        l1.changeAttributeValue(fid, l1.fields().indexFromName("c"), "C_NEW")
        mark = len(wq.events)
        d["s1_commit"] = bool(l1.commitChanges())
        pump(0.2)
        d["s1_deltas"] = _deltas(wq.events[mark:])

        # ---- S2: rename nom -> libelle + change it ---------------------
        l2 = _layer(_gpkg(tmp, "s2"), "s2")
        tracker.connect_layer(l2)
        fid = sorted(f.id() for f in l2.getFeatures())[0]
        l2.startEditing()
        l2.renameAttribute(l2.fields().indexFromName("nom"), "libelle")
        l2.changeAttributeValue(fid, l2.fields().indexFromName("libelle"), "LIB_NEW")
        mark = len(wq.events)
        d["s2_commit"] = bool(l2.commitChanges())
        pump(0.2)
        d["s2_deltas"] = _deltas(wq.events[mark:])

        # ---- S3: add field x + value -----------------------------------
        l3 = _layer(_gpkg(tmp, "s3"), "s3")
        tracker.connect_layer(l3)
        fid = sorted(f.id() for f in l3.getFeatures())[0]
        l3.startEditing()
        l3.addAttribute(QgsField("x", QMetaType.Type.QString))
        l3.changeAttributeValue(fid, l3.fields().indexFromName("x"), "X_NEW")
        mark = len(wq.events)
        d["s3_commit"] = bool(l3.commitChanges())
        pump(0.2)
        d["s3_deltas"] = _deltas(wq.events[mark:])

        # ---- S4: layer broken at load, repaired later -------------------
        good = _gpkg(tmp, "s4")
        missing = os.path.join(tmp, "missing_s4.gpkg")
        l4 = _layer(missing, "s4")
        QgsProject.instance().addMapLayer(l4)
        tracker.connect_layer(l4)
        d["s4_connected_while_broken"] = l4.id() in tracker._connected_layers
        l4.setDataSource(f"{good}|layername=pts", "s4", "ogr")
        pump(0.2)
        d["s4_connected_after_repair"] = l4.id() in tracker._connected_layers
        fid = sorted(f.id() for f in l4.getFeatures())[0] if l4.isValid() else None
        mark = len(wq.events)
        if fid is not None:
            l4.startEditing()
            l4.changeAttributeValue(fid, l4.fields().indexFromName("c"), "S4_AFTER_REPAIR")
            d["s4_commit"] = bool(l4.commitChanges())
        pump(0.2)
        d["s4_deltas"] = _deltas(wq.events[mark:])

        # ---- S5: untracked layer starts editing -------------------------
        l5 = _layer(_gpkg(tmp, "s5"), "s5")
        tracker.connect_layer(l5)
        from recoverland.core.identity import compute_datasource_fingerprint
        tracker.set_filter({compute_datasource_fingerprint(l1)})   # l5 excluded
        d["s5_connected"] = l5.id() in tracker._connected_layers
        l5.startEditing()
        pump(0.1)
        d["s5_statuses"] = [k for k, _det in statuses if k == "untracked_edit"]
        l5.rollBack()
    except Exception as exc:  # noqa: BLE001
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1200:]
    finally:
        tracker.deactivate()
        tracker.disconnect_all()
        QgsProject.instance().clear()
    flog(f"tx_capture_schema_change_same_session run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:2500]}",
         "INFO")
    shutil.rmtree(tmp, ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    s1 = d.get("s1_deltas") or []
    out.append((
        "s1_value_change_survives_a_field_deletion",
        d.get("s1_commit") is True and any(
            (dl.get("c") or {}).get("old") == "C0" and (dl.get("c") or {}).get("new") == "C_NEW"
            for dl in s1),
        f"commit={d.get('s1_commit')} deltas={s1} attendu c: C0 -> C_NEW ; "
        f"crash={d.get('crash')} {d.get('trace', '')[-500:]}",
    ))
    s2 = d.get("s2_deltas") or []
    out.append((
        "s2_value_change_survives_a_rename",
        d.get("s2_commit") is True and any(
            (dl.get("libelle") or {}).get("old") == "NOM0"
            and (dl.get("libelle") or {}).get("new") == "LIB_NEW" for dl in s2),
        f"commit={d.get('s2_commit')} deltas={s2} attendu libelle: NOM0 -> LIB_NEW",
    ))
    s3 = d.get("s3_deltas") or []
    out.append((
        "s3_value_in_an_added_field_is_captured",
        d.get("s3_commit") is True and any(
            (dl.get("x") or {}).get("new") == "X_NEW" for dl in s3),
        f"commit={d.get('s3_commit')} deltas={s3} attendu x: NULL -> X_NEW",
    ))
    s4 = d.get("s4_deltas") or []
    out.append((
        "s4_repaired_layer_is_captured",
        d.get("s4_connected_while_broken") is False
        and d.get("s4_connected_after_repair") is True
        and any((dl.get("c") or {}).get("new") == "S4_AFTER_REPAIR" for dl in s4),
        f"branchee cassee={d.get('s4_connected_while_broken')} apres reparation="
        f"{d.get('s4_connected_after_repair')} deltas={s4}",
    ))
    out.append((
        "s5_untracked_layer_editing_is_announced",
        d.get("s5_connected") is False and bool(d.get("s5_statuses")),
        f"couche exclue branchee={d.get('s5_connected')} alertes={d.get('s5_statuses')} : "
        f"passer en edition une couche que RecoverLand ne suit pas doit etre dit",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_capture_schema_change_same_session.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
