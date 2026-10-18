"""tx_capture_partial_commit - RecoverLand validation runtime.

Invariant: I-CAPTURE-01 (aucune modification validee par le provider ne
disparait du journal, meme quand le commit QGIS echoue a moitie).

Le mecanisme
------------
`QgsVectorLayer.commitChanges()` n'est pas une transaction : le tampon
d'edition pousse ses changements au provider par etapes (geometries,
attributs, suppressions, ajouts), et chaque etape reussie est ECRITE dans
le fichier et retiree du tampon. Mesure sur QGIS 4.2.1, GeoPackage avec un
index UNIQUE sur `code`, une session qui deplace et renomme fid 1,
supprime fid 2 et ajoute un doublon de code :

    commitChanges() -> False
    commitErrors: SUCCESS: 1 geometries were changed.
                  SUCCESS: 1 attribute value(s) changed.
                  SUCCESS: 1 feature(s) deleted.
                  ERROR: 1 feature(s) not added.
    signaux: beforeCommitChanges, committedGeometriesChanges,
             committedAttributeValuesChanges, committedFeaturesRemoved
             (PAS de afterCommitChanges)
    fichier apres coup: fid 1 deplace et renomme, fid 2 supprime

`EditSessionTracker` ne produit ses evenements que dans
`_on_after_commit`. Ce signal n'arrive jamais sur un commit partiel. Le
tampon de capture reste en memoire, et `_on_rollback` le VIDE si
l'utilisateur abandonne ses modifications restantes -- ce que fait tout
utilisateur devant "le commit a echoue". Trois modifications reellement
ecrites dans son fichier n'ont alors aucune trace au journal : aucun
restore, aucun rewind ne pourra jamais les defaire.

Cas
---
K1  commit partiel puis rollBack : l'UPDATE (attribut + geometrie) de
    fid 1 et le DELETE de fid 2 doivent etre au journal, avec l'etat
    d'avant et l'etat reellement ecrit ; aucun INSERT (il n'a pas eu lieu).
K2  commit partiel, correction, nouveau commit reussi : chaque evenement
    exactement une fois (UPDATE, DELETE, INSERT), pas de doublon.
K3  commit partiel, l'utilisateur ne fait RIEN (la couche reste en
    edition) : ce qui a ete ecrit doit etre durable sans attendre une
    action ulterieure. QGIS peut planter dans la minute.
K4  l'etape attributs echoue (un declencheur refuse la valeur) apres
    l'etape geometries : l'UPDATE journalise ne doit porter QUE le
    deplacement. Journaliser le renommage projete par le tampon, qui n'a
    jamais atteint le disque, serait un faux souvenir que le restore
    "remettrait" ensuite.

Verdict attendu avant correctif : FAIL (K1, K3, K4 sans evenement).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_capture_partial_commit"
INVARIANT = "I-CAPTURE-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PENDING_FILENAME = "recoverland_pending.json"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_gpkg(tmpdir: str, name: str, with_trigger: bool = False) -> str:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsVectorFileWriter, QgsCoordinateTransformContext,
    )

    mem = QgsVectorLayer(
        "Point?crs=EPSG:2154&field=code:string(10)&field=nom:string(40)",
        "seed", "memory")
    mem.startEditing()
    for i, (code, nom) in enumerate([("C001", "PONT"), ("C002", "GARE"),
                                     ("C003", "ECOLE")]):
        feat = QgsFeature(mem.fields())
        feat.setAttributes([code, nom])
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(i + 1.0, i + 1.0)))
        mem.addFeature(feat)
    mem.commitChanges()
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "pts"
    QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    con = sqlite3.connect(path)
    con.execute("CREATE UNIQUE INDEX ux_pts_code ON pts(code)")
    if with_trigger:
        con.execute(
            "CREATE TRIGGER trg_nom_interdit BEFORE UPDATE OF nom ON pts "
            "WHEN NEW.nom = 'INTERDIT' "
            "BEGIN SELECT RAISE(ABORT, 'nom interdit'); END")
    con.commit()
    con.close()
    return path


class _Stack:
    """Real journal + real WriteQueue + real tracker, one per case."""

    def __init__(self, tmpdir: str, case: str):
        from recoverland.core.journal_manager import JournalManager
        from recoverland.core.write_queue import WriteQueue
        from recoverland.core.edit_tracker import EditSessionTracker

        self.dir = os.path.join(tmpdir, case)
        os.makedirs(self.dir, exist_ok=True)
        self.jm = JournalManager()
        self.jm.open_for_project(
            project_path=os.path.join(self.dir, "audit.qgz"),
            profile_path=self.dir)
        self.path = self.jm.path
        self.wq = WriteQueue()
        self.wq.start(self.path)
        self.tracker = EditSessionTracker(self.wq, self.jm)
        self.tracker.activate()

    def connect(self, layer) -> bool:
        self.tracker.connect_layer(layer)
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


def _durable_events(journal_path: str) -> list:
    """Every event that survives a crash: journal rows + pending file."""
    out = []
    try:
        conn = sqlite3.connect(
            f"file:{Path(journal_path).as_posix()}?mode=ro", uri=True)
        try:
            for row in conn.execute(
                    "SELECT operation_type, feature_identity_json, "
                    "attributes_json, geometry_wkb, new_geometry_wkb "
                    "FROM audit_event WHERE restored_from_event_id IS NULL "
                    "ORDER BY event_id"):
                out.append({"src": "journal", "op": row[0],
                            "identity": row[1], "attrs": row[2],
                            "old_wkb": row[3], "new_wkb": row[4]})
        finally:
            conn.close()
    except sqlite3.Error as exc:
        out.append({"src": "journal_error", "op": "?", "error": str(exc)})
    pending = os.path.join(os.path.dirname(journal_path), _PENDING_FILENAME)
    if os.path.isfile(pending):
        try:
            with open(pending, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for evt in data if isinstance(data, list) else []:
                out.append({"src": "pending", "op": evt.get("operation_type"),
                            "identity": evt.get("feature_identity_json"),
                            "attrs": evt.get("attributes_json"),
                            "old_wkb": evt.get("geometry_wkb"),
                            "new_wkb": evt.get("new_geometry_wkb")})
        except (OSError, ValueError) as exc:
            out.append({"src": "pending_error", "op": "?", "error": str(exc)})
    return out


def _point_of(wkb):
    from qgis.core import QgsGeometry
    if wkb is None:
        return None
    if isinstance(wkb, str) and wkb.startswith("b64:"):
        import base64
        wkb = base64.b64decode(wkb[4:])
    geom = QgsGeometry()
    geom.fromWkb(bytes(wkb))
    if geom.isNull():
        return None
    pt = geom.asPoint()
    return (round(pt.x(), 6), round(pt.y(), 6))


def _describe(events) -> list:
    out = []
    for e in events:
        try:
            attrs = json.loads(e.get("attrs") or "{}")
        except ValueError:
            attrs = {"raw": e.get("attrs")}
        out.append({
            "src": e.get("src"), "op": e.get("op"),
            "identity": e.get("identity"),
            "changed": attrs.get("changed_only"),
            "full_nom": (attrs.get("all_attributes") or {}).get("nom")
            if isinstance(attrs.get("all_attributes"), dict) else None,
            "old_xy": _point_of(e.get("old_wkb")),
            "new_xy": _point_of(e.get("new_wkb")),
        })
    return out


def _provider_state(path: str) -> list:
    from qgis.core import QgsVectorLayer
    lyr = QgsVectorLayer(f"{path}|layername=pts", "probe", "ogr")
    state = []
    for f in lyr.getFeatures():
        pt = f.geometry().asPoint()
        state.append((f.id(), f["code"], f["nom"], (pt.x(), pt.y())))
    return sorted(state)


def _open_layer(path: str, name: str):
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=pts", name, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"invalid layer {path}")
    return layer


def _edit_session(layer):
    """fid1: rename + move ; fid2: delete ; add a UNIQUE-violating row."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    fids = sorted(f.id() for f in layer.getFeatures())
    idx_nom = layer.fields().indexFromName("nom")
    idx_code = layer.fields().indexFromName("code")
    layer.startEditing()
    layer.changeAttributeValue(fids[0], idx_nom, "PONT_EDIT")
    layer.changeGeometry(fids[0], QgsGeometry.fromPointXY(QgsPointXY(11.0, 11.0)))
    layer.deleteFeature(fids[1])
    dup = QgsFeature(layer.fields())
    attrs = [None] * layer.fields().count()
    attrs[idx_code] = "C003"
    attrs[idx_nom] = "DOUBLON"
    dup.setAttributes(attrs)
    dup.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(9.0, 9.0)))
    layer.addFeature(dup)
    return fids, dup.id(), idx_code


# ---------------------------------------------------------------------------
# Hooks
# ---------------------------------------------------------------------------

def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_partial_")
    ctx.data["tmpdir"] = tmpdir
    flog(f"tx_capture_partial_commit setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _case_k1(ctx):
    stack = _Stack(ctx.data["tmpdir"], "k1")
    path = _make_gpkg(stack.dir, "k1")
    layer = _open_layer(path, "k1")
    res = {"connected": stack.connect(layer)}
    _edit_session(layer)
    res["commit_ok"] = bool(layer.commitChanges())
    res["commit_errors"] = list(layer.commitErrors())
    layer.rollBack()
    _pump(0.3)
    stack.shutdown()
    res["provider"] = _provider_state(path)
    res["events"] = _describe(_durable_events(stack.path))
    return res


def _case_k2(ctx):
    stack = _Stack(ctx.data["tmpdir"], "k2")
    path = _make_gpkg(stack.dir, "k2")
    layer = _open_layer(path, "k2")
    res = {"connected": stack.connect(layer)}
    _fids, dup_fid, idx_code = _edit_session(layer)
    res["commit1_ok"] = bool(layer.commitChanges())
    _pump(0.3)
    layer.changeAttributeValue(dup_fid, idx_code, "C004")
    res["commit2_ok"] = bool(layer.commitChanges())
    _pump(0.3)
    stack.shutdown()
    res["provider"] = _provider_state(path)
    res["events"] = _describe(_durable_events(stack.path))
    return res


def _case_k3(ctx):
    stack = _Stack(ctx.data["tmpdir"], "k3")
    path = _make_gpkg(stack.dir, "k3")
    layer = _open_layer(path, "k3")
    res = {"connected": stack.connect(layer)}
    _edit_session(layer)
    res["commit_ok"] = bool(layer.commitChanges())
    # The user reads the error and does nothing yet. The layer stays in
    # edit mode, the plugin keeps running. What is durable NOW is what a
    # crash in the next minute would leave.
    _pump(2.0)
    res["still_editing"] = bool(layer.isEditable())
    res["events_while_editing"] = _describe(_durable_events(stack.path))
    layer.rollBack()
    _pump(0.2)
    stack.shutdown()
    res["provider"] = _provider_state(path)
    return res


def _case_k4(ctx):
    from qgis.core import QgsGeometry, QgsPointXY
    stack = _Stack(ctx.data["tmpdir"], "k4")
    path = _make_gpkg(stack.dir, "k4", with_trigger=True)
    layer = _open_layer(path, "k4")
    res = {"connected": stack.connect(layer)}
    fids = sorted(f.id() for f in layer.getFeatures())
    idx_nom = layer.fields().indexFromName("nom")
    layer.startEditing()
    layer.changeGeometry(fids[0], QgsGeometry.fromPointXY(QgsPointXY(21.0, 21.0)))
    layer.changeAttributeValue(fids[0], idx_nom, "INTERDIT")
    res["commit_ok"] = bool(layer.commitChanges())
    res["commit_errors"] = list(layer.commitErrors())
    layer.rollBack()
    _pump(0.3)
    stack.shutdown()
    res["provider"] = _provider_state(path)
    res["events"] = _describe(_durable_events(stack.path))
    return res


def run(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_capture_partial_commit run start: trace_id={ctx.trace_id}", "INFO")
    for key, fn in (("k1", _case_k1), ("k2", _case_k2),
                    ("k3", _case_k3), ("k4", _case_k4)):
        try:
            ctx.data[key] = fn(ctx)
        except Exception as exc:  # noqa: BLE001 - a crashed case is a FAIL
            import traceback
            ctx.data[key] = {"crash": f"{type(exc).__name__}: {exc}",
                             "trace": traceback.format_exc()[-800:]}
        flog(f"tx_capture_partial_commit case={key} trace_id={ctx.trace_id} "
             f"result={json.dumps(ctx.data[key], default=str)[:1500]}", "INFO")
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _ops(events):
    return sorted(e.get("op") for e in events)


def _update_of_fid1(events):
    ups = [e for e in events if e.get("op") == "UPDATE"]
    return ups[0] if len(ups) == 1 else None


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    k1 = ctx.data.get("k1", {})
    k2 = ctx.data.get("k2", {})
    k3 = ctx.data.get("k3", {})
    k4 = ctx.data.get("k4", {})

    # ===== Preconditions: the fixture really produces a partial commit ===
    prov1 = k1.get("provider") or []
    out.append((
        "k1_fixture_partial_commit_reached_the_file",
        k1.get("connected") is True and k1.get("commit_ok") is False
        and any(r[2] == "PONT_EDIT" for r in prov1)
        and not any(r[1] == "C002" for r in prov1),
        f"connected={k1.get('connected')} commit_ok={k1.get('commit_ok')} "
        f"provider={prov1} crash={k1.get('crash')} : sans commit partiel "
        f"reel le reste du scenario ne prouve rien",
    ))

    # ===== K1: rollback after a partial commit ==========================
    ev1 = k1.get("events") or []
    up1 = _update_of_fid1(ev1)
    out.append((
        "k1_applied_changes_are_journalled_after_rollback",
        _ops(ev1) == ["DELETE", "UPDATE"],
        f"evenements durables={_ops(ev1)} attendu ['DELETE', 'UPDATE'] ; "
        f"detail={ev1}. Le fichier contient fid 1 renomme et deplace, fid 2 "
        f"supprime ; l'utilisateur a abandonne le reste et le journal ne "
        f"garde rien de ce qui a ete ecrit.",
    ))
    out.append((
        "k1_update_carries_before_and_written_state",
        up1 is not None
        and (up1.get("changed") or {}).get("nom", {}).get("old") == "PONT"
        and (up1.get("changed") or {}).get("nom", {}).get("new") == "PONT_EDIT"
        and up1.get("old_xy") == (1.0, 1.0) and up1.get("new_xy") == (11.0, 11.0),
        f"UPDATE fid1={up1} attendu nom PONT->PONT_EDIT et (1,1)->(11,11)",
    ))
    dels = [e for e in ev1 if e.get("op") == "DELETE"]
    out.append((
        "k1_delete_carries_the_full_before_state",
        len(dels) == 1 and dels[0].get("full_nom") == "GARE"
        and dels[0].get("old_xy") == (2.0, 2.0),
        f"DELETE={dels} attendu l'etat complet de GARE en (2,2) : sans lui "
        f"la suppression est irreversible",
    ))

    # ===== K2: retry after fixing, exactly once ========================
    ev2 = k2.get("events") or []
    out.append((
        "k2_retry_journals_each_change_exactly_once",
        k2.get("commit1_ok") is False and k2.get("commit2_ok") is True
        and _ops(ev2) == ["DELETE", "INSERT", "UPDATE"],
        f"commit1={k2.get('commit1_ok')} commit2={k2.get('commit2_ok')} "
        f"evenements={_ops(ev2)} attendu ['DELETE', 'INSERT', 'UPDATE'] "
        f"detail={ev2} crash={k2.get('crash')}",
    ))

    # ===== K3: durable without waiting for the user ====================
    ev3 = k3.get("events_while_editing") or []
    out.append((
        "k3_written_part_is_durable_before_any_user_action",
        k3.get("still_editing") is True and _ops(ev3) == ["DELETE", "UPDATE"],
        f"couche encore en edition={k3.get('still_editing')} ; evenements "
        f"durables 2 s apres le commit refuse={_ops(ev3)} attendu "
        f"['DELETE', 'UPDATE']. Un plantage de QGIS a cet instant effacerait "
        f"toute trace de modifications deja ecrites sur le disque.",
    ))

    # ===== K4: only what reached the provider ==========================
    prov4 = k4.get("provider") or []
    ev4 = k4.get("events") or []
    up4 = _update_of_fid1(ev4)
    out.append((
        "k4_fixture_geometry_landed_attribute_refused",
        k4.get("commit_ok") is False
        and any(r[3] == (21.0, 21.0) and r[2] == "PONT" for r in prov4),
        f"commit_ok={k4.get('commit_ok')} provider={prov4} "
        f"errors={k4.get('commit_errors')} crash={k4.get('crash')}",
    ))
    out.append((
        "k4_update_records_only_what_was_written",
        up4 is not None and up4.get("new_xy") == (21.0, 21.0)
        and "nom" not in (up4.get("changed") or {}),
        f"UPDATE fid1={up4} ; tous={ev4}. Le deplacement a ete ecrit, le "
        f"renommage refuse par le fichier : journaliser nom=INTERDIT serait "
        f"enregistrer une valeur que la donnee n'a jamais eue.",
    ))

    out.append(assert_log_contains(
        ctx.records, rf"tx_capture_partial_commit.*trace_id={ctx.trace_id}",
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
