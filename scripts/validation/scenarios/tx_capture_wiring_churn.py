"""tx_capture_wiring_churn - RecoverLand validation runtime.

Invariant: I-1 (fidelite de capture) face au va-et-vient des couches : une
couche branchee deux fois ne doit pas doubler les evenements, une couche
debranchee puis rebranchee en pleine edition ne doit pas en perdre, et une
couche retiree du projet ne doit rien laisser derriere elle.

Le mecanisme
------------
`EditSessionTracker` ne surveille que les couches presentes dans
`_connected_layers`, avec pour chacune huit connexions de signaux Qt
(`_bind_signals`). Trois evenements exterieurs rebattent ces cartes en
permanence, souvent pendant que l'utilisateur edite :

    connect_layer / disconnect_layer   quand le projet gagne ou perd une couche
    set_filter(...)                    quand l'utilisateur change la liste des
                                       couches suivies dans la fenetre
    disconnect_layer_by_id             sur le signal layersRemoved du projet

`set_filter` est le plus violent : il debranche tout ce qui sort du filtre -
et `disconnect_layer` DETRUIT le tampon de la session d'edition en cours -
puis reparcourt le projet pour rebrancher. Une session d'edition ouverte
traverse donc ce cycle sans que l'utilisateur en sache rien.

Scenarios d'echec concrets couverts
-----------------------------------
W1 Une couche branchee deux fois (deux appels a connect_layer, cas courant :
   le signal layersAdded et le balayage initial du projet se croisent) doit
   produire UN evenement par modification. Deux jeux de signaux et le
   journal compte double : le rewind applique alors deux fois la meme
   compensation.
W2 L'utilisateur ouvre l'edition, modifie, puis va cocher/decocher des
   couches dans la fenetre RecoverLand avant d'enregistrer. Le tampon est
   detruit au passage. Le filet de securite est la "late session start" de
   `_on_before_commit` : sans elle, la modification est perdue alors que
   QGIS l'ecrit sur le disque.
W3 Une couche hors filtre ne doit produire aucun evenement, et ne doit pas
   entrainer avec elle les couches qui restent suivies.
W4 Retour au suivi complet : la couche precedemment exclue doit etre
   reprise sans intervention supplementaire.
W5 Une couche retiree du projet pendant une session d'edition ouverte : le
   tracker garde une reference vers un objet C++ detruit. Il doit s'en
   defaire sans lever, sans laisser de tampon en memoire, et continuer a
   suivre les autres couches.
W6 Le meme fichier ouvert deux fois dans le projet (deux couches, une seule
   source) : une modification faite par l'une ne doit etre journalisee
   qu'une fois, sinon l'historique de cette source compte double.

Montage : deux GPKG dans un dossier temporaire, tous deux inscrits dans
QgsProject (indispensable : `set_filter` rebranche en parcourant le projet),
et le meme cablage que le produit pour layersRemoved.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_capture_wiring_churn"
INVARIANT = "I-1"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = ["nom", "etat"]
_N_SIGNALS = 8


# --------------------------------------------------------------------------
# Fixture
# --------------------------------------------------------------------------
def _make_gpkg(tmpdir: str, stem: str) -> str:
    from osgeo import ogr

    path = os.path.join(tmpdir, f"{stem}.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("driver GPKG indisponible")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer(stem, geom_type=ogr.wkbPoint)
    for name in _FIELDS:
        lyr.CreateField(ogr.FieldDefn(name, ogr.OFTString))
    ds.FlushCache()
    ds = None
    return path


def _load_layer(path: str, stem: str, alias: str):
    from qgis.core import QgsVectorLayer

    layer = QgsVectorLayer(f"{path}|layername={stem}", alias, "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {path!r}")
    return layer


def _seed(layer, tag: str) -> list:
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    layer.startEditing()
    for k in range(2):
        feat = QgsFeature(layer.fields())
        feat.setAttribute("nom", f"{tag}_{k}")
        feat.setAttribute("etat", "initial")
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(float(k), float(k))))
        layer.addFeature(feat)
    layer.commitChanges()
    layer.reload()
    return [f.id() for f in layer.getFeatures()]


class _CapturingWQ:
    def __init__(self):
        self.events: list = []

    def enqueue(self, events) -> bool:
        self.events.extend(events)
        return True

    def set_flush_callback(self, callback) -> None:
        self._cb = callback


class _DummyJM:
    def get_connection(self):
        raise RuntimeError("tx_capture: pas de journal reel dans ce scenario")


def _delta(event) -> dict:
    try:
        return json.loads(event.attributes_json or "{}").get("changed_only", {})
    except (TypeError, ValueError):
        return {}


def _describe(events) -> list:
    return [{"op": e.operation_type, "layer": e.layer_name_snapshot,
             "delta": _delta(e)} for e in events]


def _edit(layer, fid: int, value: str) -> bool:
    idx = layer.fields().indexFromName("etat")
    if not layer.startEditing():
        return False
    layer.changeAttributeValue(fid, idx, value)
    return bool(layer.commitChanges())


# --------------------------------------------------------------------------
# Scenario
# --------------------------------------------------------------------------
def setup(ctx):
    from qgis.core import QgsProject
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_churn_")
    path_a = _make_gpkg(tmpdir, "parcelles")
    path_b = _make_gpkg(tmpdir, "reseau")
    layer_a = _load_layer(path_a, "parcelles", "parcelles")
    layer_b = _load_layer(path_b, "reseau", "reseau")
    fids_a = _seed(layer_a, "parc")
    fids_b = _seed(layer_b, "res")

    project = QgsProject.instance()
    project.removeAllMapLayers()
    project.addMapLayer(layer_a)
    project.addMapLayer(layer_b)

    wq = _CapturingWQ()
    tracker = EditSessionTracker(wq, _DummyJM())
    tracker.activate()
    tracker.connect_layer(layer_a)
    tracker.connect_layer(layer_b)

    # Meme cablage que le produit (recover.RecoverPlugin._on_layers_removed).
    def _on_layers_removed(layer_ids):
        for lid in layer_ids:
            tracker.disconnect_layer_by_id(lid)

    project.layersRemoved.connect(_on_layers_removed)

    ctx.data["tmpdir"] = tmpdir
    ctx.data["path_a"] = path_a
    ctx.data["layer_a"] = layer_a
    ctx.data["layer_b"] = layer_b
    ctx.data["layer_b_id"] = layer_b.id()
    ctx.data["fids_a"] = fids_a
    ctx.data["fids_b"] = fids_b
    ctx.data["fp_a"] = compute_datasource_fingerprint(layer_a)
    ctx.data["fp_b"] = compute_datasource_fingerprint(layer_b)
    ctx.data["wq"] = wq
    ctx.data["tracker"] = tracker
    ctx.data["_removed_slot"] = _on_layers_removed
    ctx.data["phases"] = {}
    ctx.data["connected_both"] = (
        layer_a.id() in tracker._connected_layers
        and layer_b.id() in tracker._connected_layers)

    flog(
        f"tx_capture_wiring_churn setup: trace_id={ctx.trace_id} "
        f"fp_a={ctx.data['fp_a']} fp_b={ctx.data['fp_b']} "
        f"connected_both={ctx.data['connected_both']}",
        "INFO",
    )


def _phase(ctx, name, body):
    from recoverland.core.logger import flog

    wq = ctx.data["wq"]
    mark = len(wq.events)
    body()
    produced = wq.events[mark:]
    ctx.data["phases"][name] = produced
    flog(
        f"tx_capture_wiring_churn phase={name} trace_id={ctx.trace_id} "
        f"events={len(produced)} detail={_describe(produced)}",
        "INFO",
    )
    return produced


def run(ctx):
    from qgis.core import QgsProject
    from recoverland.core.logger import flog

    tracker = ctx.data["tracker"]
    layer_a = ctx.data["layer_a"]
    layer_b = ctx.data["layer_b"]
    fid_a = ctx.data["fids_a"][0]
    fid_b = ctx.data["fids_b"][0]

    flog(f"tx_capture_wiring_churn run start: trace_id={ctx.trace_id}", "INFO")

    # ===== W1 : double branchement ======================================
    tracker.connect_layer(layer_a)
    tracker.connect_layer(layer_a)
    ctx.data["w1_signal_slots"] = len(
        tracker._signal_connections.get(layer_a.id(), []))

    def _w1():
        _edit(layer_a, fid_a, "W1")

    _phase(ctx, "W1_double_connect", _w1)
    layer_a.reload()

    # ===== W2 : debranchement/rebranchement en pleine edition ===========
    def _w2():
        idx = layer_a.fields().indexFromName("etat")
        layer_a.startEditing()
        layer_a.changeAttributeValue(fid_a, idx, "W2")
        ctx.data["w2_buffer_before"] = layer_a.id() in tracker._buffers
        tracker.disconnect_layer(layer_a)
        ctx.data["w2_buffer_after_disconnect"] = layer_a.id() in tracker._buffers
        tracker.connect_layer(layer_a)
        layer_a.commitChanges()

    _phase(ctx, "W2_disconnect_mid_session", _w2)
    layer_a.reload()

    # ===== W3 : couche hors filtre ======================================
    tracker.set_filter({ctx.data["fp_b"]})
    ctx.data["w3_a_connected"] = layer_a.id() in tracker._connected_layers
    ctx.data["w3_b_connected"] = layer_b.id() in tracker._connected_layers

    def _w3_a():
        ctx.data["w3_commit_a"] = _edit(layer_a, fid_a, "W3_HORS_FILTRE")

    _phase(ctx, "W3_excluded_layer", _w3_a)
    layer_a.reload()
    ctx.data["w3_disk_a"] = layer_a.getFeature(fid_a)["etat"]

    def _w3_b():
        ctx.data["w3_commit_b"] = _edit(layer_b, fid_b, "W3_DANS_FILTRE")

    _phase(ctx, "W3_kept_layer", _w3_b)
    layer_b.reload()

    # ===== W4 : retour au suivi complet =================================
    tracker.set_filter(set())
    ctx.data["w4_a_connected"] = layer_a.id() in tracker._connected_layers

    def _w4():
        _edit(layer_a, fid_a, "W4_REPRIS")

    _phase(ctx, "W4_filter_cleared", _w4)
    layer_a.reload()

    # ===== W6 : le meme fichier ouvert deux fois ========================
    twin = _load_layer(ctx.data["path_a"], "parcelles", "parcelles_bis")
    QgsProject.instance().addMapLayer(twin)
    tracker.connect_layer(twin)
    ctx.data["w6_twin_connected"] = twin.id() in tracker._connected_layers
    ctx.data["w6_same_fingerprint"] = (
        tracker._layer_fingerprints.get(twin.id())
        == tracker._layer_fingerprints.get(layer_a.id()))

    def _w6():
        _edit(layer_a, fid_a, "W6_UNE_SEULE_FOIS")

    _phase(ctx, "W6_same_source_twice", _w6)
    layer_a.reload()
    QgsProject.instance().removeMapLayer(twin.id())
    twin = None

    # ===== W5 : couche retiree du projet en pleine edition ==============
    layer_b_id = ctx.data["layer_b_id"]
    idx_b = layer_b.fields().indexFromName("etat")
    layer_b.startEditing()
    layer_b.changeAttributeValue(fid_b, idx_b, "W5_JAMAIS_ENREGISTRE")
    ctx.data["w5_buffer_before"] = layer_b_id in tracker._buffers
    ctx.data["layer_b"] = None
    removal_error = None
    try:
        QgsProject.instance().removeMapLayer(layer_b_id)
    except Exception as exc:  # noqa: BLE001 - c'est justement ce qu'on mesure
        removal_error = f"{type(exc).__name__}: {exc}"
    layer_b = None
    ctx.data["w5_removal_error"] = removal_error
    ctx.data["w5_still_connected"] = layer_b_id in tracker._connected_layers
    ctx.data["w5_buffer_left"] = layer_b_id in tracker._buffers
    ctx.data["w5_fingerprint_left"] = layer_b_id in tracker._layer_fingerprints

    # La couche restante doit continuer a etre suivie normalement.
    def _w5_after():
        _edit(layer_a, fid_a, "W5_APRES_RETRAIT")

    _phase(ctx, "W5_survivor_still_tracked", _w5_after)
    layer_a.reload()

    teardown_error = None
    try:
        tracker.disconnect_all()
    except Exception as exc:  # noqa: BLE001
        teardown_error = f"{type(exc).__name__}: {exc}"
    ctx.data["w5_teardown_error"] = teardown_error

    flog(
        f"tx_capture_wiring_churn run end: trace_id={ctx.trace_id} "
        f"phases={{k: len(v) for k, v in ctx.data['phases'].items()}} "
        f"w5_removal_error={removal_error!r} teardown={teardown_error!r}",
        "INFO",
    )

    ctx.data["layer_a"] = None
    try:
        QgsProject.instance().layersRemoved.disconnect(ctx.data["_removed_slot"])
    except (TypeError, RuntimeError):
        pass
    QgsProject.instance().removeAllMapLayers()
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


# --------------------------------------------------------------------------
# Assertions
# --------------------------------------------------------------------------
def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    ph = ctx.data.get("phases") or {}

    def phase(name):
        return ph.get(name) or []

    out.append((
        "fixture_two_layers_tracked",
        ctx.data.get("connected_both") is True,
        f"connected_both={ctx.data.get('connected_both')} : les deux couches "
        f"doivent etre suivies au depart",
    ))

    # ===== W1 : double branchement ======================================
    w1 = phase("W1_double_connect")
    out.append((
        "W1_no_duplicate_signal_binding",
        ctx.data.get("w1_signal_slots") == _N_SIGNALS,
        f"connexions de signaux={ctx.data.get('w1_signal_slots')} attendu="
        f"{_N_SIGNALS} : un second jeu de connexions ferait tout capturer en "
        f"double",
    ))
    out.append((
        "W1_one_event_per_edit",
        len(w1) == 1 and w1[0].operation_type == "UPDATE",
        f"evenements={_describe(w1)} attendu=1 UPDATE : deux evenements "
        f"identiques dans le journal font appliquer deux fois la meme "
        f"compensation au rewind, l'entite finit dans un etat qu'elle n'a "
        f"jamais eu",
    ))

    # ===== W2 : debranchement en pleine edition =========================
    w2 = phase("W2_disconnect_mid_session")
    w2_delta = _delta(w2[0]) if w2 else {}
    out.append((
        "W2_disconnect_really_dropped_the_buffer",
        ctx.data.get("w2_buffer_before") is True
        and ctx.data.get("w2_buffer_after_disconnect") is False,
        f"tampon avant={ctx.data.get('w2_buffer_before')} "
        f"apres deconnexion={ctx.data.get('w2_buffer_after_disconnect')} : la "
        f"phase doit vraiment detruire le tampon de la session en cours pour "
        f"tester le filet de securite",
    ))
    out.append((
        "W2_edit_survives_the_reconnection",
        len(w2) == 1 and w2_delta.get("etat", {}).get("new") == "W2",
        f"evenements={_describe(w2)} attendu=1 UPDATE etat -> 'W2' : "
        f"l'utilisateur a modifie sa donnee puis a touche a la liste des "
        f"couches suivies avant d'enregistrer. QGIS ecrit la modification sur "
        f"le disque ; si le tracker la perd, elle n'existe dans aucun "
        f"historique et aucun rewind ne pourra l'annuler.",
    ))
    out.append((
        "W2_old_value_is_correct",
        w2_delta.get("etat", {}).get("old") == "W1",
        f"delta={w2_delta} attendu old='W1' : le filet de securite "
        f"(late session start) doit relire l'etat AVANT depuis le provider, "
        f"pas inventer une valeur",
    ))

    # ===== W3 : hors filtre =============================================
    out.append((
        "W3_excluded_layer_is_unplugged",
        ctx.data.get("w3_a_connected") is False
        and ctx.data.get("w3_b_connected") is True,
        f"A branchee={ctx.data.get('w3_a_connected')} "
        f"B branchee={ctx.data.get('w3_b_connected')} attendu=False/True",
    ))
    out.append((
        "W3_excluded_layer_produces_nothing",
        ctx.data.get("w3_commit_a") is True
        and len(phase("W3_excluded_layer")) == 0,
        f"commit={ctx.data.get('w3_commit_a')} "
        f"evenements={_describe(phase('W3_excluded_layer'))} "
        f"valeur ecrite={ctx.data.get('w3_disk_a')!r} attendu=aucun "
        f"evenement : une couche que l'utilisateur a retiree du suivi ne doit "
        f"rien ecrire dans le journal, sinon le filtre ne sert a rien et "
        f"l'historique se remplit de couches non demandees",
    ))
    out.append((
        "W3_kept_layer_still_captured",
        ctx.data.get("w3_commit_b") is True
        and len(phase("W3_kept_layer")) == 1,
        f"commit={ctx.data.get('w3_commit_b')} "
        f"evenements={_describe(phase('W3_kept_layer'))} attendu=1 : exclure "
        f"une couche ne doit pas aveugler les autres",
    ))

    # ===== W4 : retour au suivi complet =================================
    out.append((
        "W4_layer_is_picked_up_again",
        ctx.data.get("w4_a_connected") is True
        and len(phase("W4_filter_cleared")) == 1,
        f"branchee={ctx.data.get('w4_a_connected')} "
        f"evenements={_describe(phase('W4_filter_cleared'))} attendu=True/1 : "
        f"remettre le suivi complet doit rebrancher les couches deja "
        f"presentes dans le projet, sans que l'utilisateur ait a les "
        f"recharger",
    ))

    # ===== W6 : meme source, deux couches ===============================
    w6 = phase("W6_same_source_twice")
    out.append((
        "W6_twin_layer_shares_the_fingerprint",
        ctx.data.get("w6_twin_connected") is True
        and ctx.data.get("w6_same_fingerprint") is True,
        f"jumelle branchee={ctx.data.get('w6_twin_connected')} "
        f"meme empreinte={ctx.data.get('w6_same_fingerprint')} : la phase "
        f"exige deux couches distinctes pointant la meme source",
    ))
    out.append((
        "W6_single_event_for_a_single_edit",
        len(w6) == 1,
        f"evenements={_describe(w6)} attendu=1 : le meme fichier ouvert deux "
        f"fois dans le projet ne doit pas doubler l'historique de cette "
        f"source, sinon chaque rewind compense deux fois",
    ))

    # ===== W5 : couche retiree du projet ================================
    out.append((
        "W5_removal_does_not_raise",
        ctx.data.get("w5_removal_error") is None
        and ctx.data.get("w5_teardown_error") is None,
        f"erreur au retrait={ctx.data.get('w5_removal_error')!r} "
        f"erreur au demontage={ctx.data.get('w5_teardown_error')!r} : retirer "
        f"une couche en cours d'edition ne doit pas faire remonter "
        f"d'exception dans QGIS",
    ))
    out.append((
        "W5_no_dangling_reference_left",
        ctx.data.get("w5_still_connected") is False
        and ctx.data.get("w5_buffer_left") is False
        and ctx.data.get("w5_fingerprint_left") is False,
        f"encore branchee={ctx.data.get('w5_still_connected')} "
        f"tampon restant={ctx.data.get('w5_buffer_left')} "
        f"empreinte restante={ctx.data.get('w5_fingerprint_left')} "
        f"attendu=False partout : une reference vers un objet C++ detruit et "
        f"le tampon de sa session (potentiellement des centaines de Mo) ne "
        f"doivent pas survivre au retrait de la couche",
    ))
    out.append((
        "W5_survivor_still_tracked",
        len(phase("W5_survivor_still_tracked")) == 1,
        f"evenements={_describe(phase('W5_survivor_still_tracked'))} "
        f"attendu=1 : le retrait d'une couche ne doit pas emporter le suivi "
        f"des autres",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"late session start",
        name="late_session_start_logged",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_capture_wiring_churn.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=3,
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
