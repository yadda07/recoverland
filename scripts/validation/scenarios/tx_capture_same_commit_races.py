"""tx_capture_same_commit_races - RecoverLand validation runtime.

Invariant: I-1 (fidelite de capture) sous la contrainte la plus dure : un
SEUL commit qui contient plusieurs operations contradictoires sur la meme
entite, puis un commit de masse, puis le depassement du budget memoire du
tampon de capture.

Le mecanisme
------------
`EditSessionBuffer.compute_net_effect` arbitre les operations qui se
neutralisent dans un meme commit :

    ajoutee puis supprimee   -> aucun evenement (elle n'a jamais existe)
    modifiee puis supprimee  -> DELETE seul, portant l'etat d'AVANT
    supprimee puis recreee   -> DELETE + INSERT

et `EditSessionTracker._capture_edit_buffer_state` capture en trois temps :

    _capture_deletions      -> snapshots des entites supprimees
    _check_buffer_pressure  -> STOPPE tout si le budget memoire est depasse
    _capture_modifications  -> etat OLD des entites modifiees
    _capture_additions      -> fid des entites ajoutees

Scenarios d'echec concrets couverts
-----------------------------------
P1  L'utilisateur corrige un attribut puis se rend compte que la parcelle
    est en trop et la supprime, le tout avant d'enregistrer. Le journal doit
    contenir UNE suppression portant l'etat d'ORIGINE. S'il porte l'etat
    corrige, la restauration de la parcelle la fait revenir avec une valeur
    que la base n'a jamais eue ; s'il contient aussi un UPDATE, le rewind
    tente de modifier une entite deja supprimee.
P2  Suppression puis recreation dans le meme enregistrement (le geste
    classique de "je refais l'entite proprement"). Il faut les deux
    evenements, sinon soit l'ancienne version est irrecuperable, soit la
    nouvelle entite n'existe pas dans l'historique.
P3  Une entite ajoutee puis supprimee avant l'enregistrement n'a jamais
    touche la base : elle ne doit produire aucun evenement, mais elle ne
    doit pas non plus faire disparaitre l'ajout voisin du meme commit.
P4  Import massif (1200 entites) puis suppression massive : chaque entite
    doit avoir son evenement avec son instantane complet, et l'alerte de
    suppression massive doit partir avec le bon compte.
P5  Depassement du budget memoire EN PLEIN COMMIT. `_capture_edit_buffer_state`
    fait `return` des que `_check_buffer_pressure` est vrai, apres la phase
    des suppressions : les modifications et les ajouts ne sont alors JAMAIS
    captures. Les INSERT sont rattrapes par le signal committedFeaturesAdded,
    mais les UPDATE - les seuls evenements dont l'etat OLD n'existe nulle
    part ailleurs - sont perdus. Le commit reussit cote QGIS, la donnee est
    modifiee sur le disque, et le plugin annonce a l'utilisateur un nombre
    d'evenements captures qui ne mentionne pas le trou.

Note sur P5 : le budget reel est de 500 Mo, inatteignable dans un test. On
abaisse le seuil sur L'INSTANCE du tracker (`tracker._MEMORY_HARD_LIMIT_MB`)
pour atteindre la branche ; aucun fichier produit n'est modifie et la valeur
d'origine est remise avant la phase P6, qui verifie que la capture
redevient normale.

Montage (vrai GPKG dans un dossier temporaire, vrais signaux Qt) :
    - GPKG `parcelles` : 4 champs texte + 6 entites
    - EditSessionTracker branche via connect_layer, WriteQueue de capture
    - callback de commit enregistre pour lire l'alerte de suppression massive
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_capture_same_commit_races"
INVARIANT = "I-1"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = ["nom", "code", "etat", "note"]
_N_SEED = 6
_MASS_N = 1200


# --------------------------------------------------------------------------
# Fixture
# --------------------------------------------------------------------------
def _make_gpkg(tmpdir: str) -> str:
    from osgeo import ogr

    path = os.path.join(tmpdir, "parcelles.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("driver GPKG indisponible")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer("parcelles", geom_type=ogr.wkbPoint)
    for name in _FIELDS:
        lyr.CreateField(ogr.FieldDefn(name, ogr.OFTString))
    ds.FlushCache()
    ds = None
    return path


def _load_layer(path: str):
    from qgis.core import QgsVectorLayer

    uri = f"{path}|layername=parcelles"
    layer = QgsVectorLayer(uri, "parcelles", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"couche GPKG invalide: {uri!r}")
    return layer


def _new_feature(layer, tag: str, x: float, y: float):
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    feat = QgsFeature(layer.fields())
    feat.setAttribute("nom", f"nom_{tag}")
    feat.setAttribute("code", f"code_{tag}")
    feat.setAttribute("etat", "initial")
    feat.setAttribute("note", f"note_{tag}")
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(x, y)))
    return feat


def _seed(layer) -> list:
    layer.startEditing()
    for k in range(_N_SEED):
        layer.addFeature(_new_feature(layer, "s%d" % k, float(k), float(k)))
    layer.commitChanges()
    layer.reload()
    return [f.id() for f in layer.getFeatures()]


class _CapturingWQ:
    def __init__(self):
        self.events: list = []
        self.batches: list = []

    def enqueue(self, events) -> bool:
        self.batches.append(len(events))
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


def _snapshot(event) -> dict:
    try:
        return json.loads(event.attributes_json or "{}").get("all_attributes", {})
    except (TypeError, ValueError):
        return {}


def _ops(events) -> list:
    return sorted(e.operation_type for e in events)


def _describe(events, limit: int = 6) -> list:
    out = []
    for ev in events[:limit]:
        out.append({
            "op": ev.operation_type,
            "identity": ev.feature_identity_json,
            "delta": _delta(ev) if ev.operation_type == "UPDATE" else None,
            "snapshot": (_snapshot(ev) if ev.operation_type != "UPDATE" else None),
        })
    if len(events) > limit:
        out.append(f"... (+{len(events) - limit})")
    return out


# --------------------------------------------------------------------------
# Scenario
# --------------------------------------------------------------------------
def setup(ctx):
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_races_")
    path = _make_gpkg(tmpdir)
    layer = _load_layer(path)
    fids = _seed(layer)

    wq = _CapturingWQ()
    tracker = EditSessionTracker(wq, _DummyJM())
    commit_calls: list = []

    def _on_commit(*args):
        commit_calls.append(args)

    tracker.set_commit_callback(_on_commit)
    tracker.activate()
    tracker.connect_layer(layer)

    ctx.data["tmpdir"] = tmpdir
    ctx.data["layer"] = layer
    ctx.data["fids"] = fids
    ctx.data["wq"] = wq
    ctx.data["tracker"] = tracker
    ctx.data["commit_calls"] = commit_calls
    ctx.data["connected"] = layer.id() in tracker._connected_layers
    ctx.data["phases"] = {}

    flog(
        f"tx_capture_same_commit_races setup: trace_id={ctx.trace_id} "
        f"gpkg={path} fids={fids} connected={ctx.data['connected']}",
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
        f"tx_capture_same_commit_races phase={name} trace_id={ctx.trace_id} "
        f"events={len(produced)} ops={_ops(produced)[:8]} "
        f"detail={_describe(produced, 3)}",
        "INFO",
    )
    return produced


def run(ctx):
    from recoverland.core.logger import flog

    layer = ctx.data["layer"]
    tracker = ctx.data["tracker"]
    fids = ctx.data["fids"]
    idx_etat = layer.fields().indexFromName("etat")
    idx_nom = layer.fields().indexFromName("nom")

    flog(f"tx_capture_same_commit_races run start: trace_id={ctx.trace_id}", "INFO")

    # ===== P1 : modifiee PUIS supprimee dans le meme commit =============
    target = fids[0]
    ctx.data["p1_fid"] = target
    ctx.data["p1_original_etat"] = layer.getFeature(target)["etat"]

    def _p1():
        layer.startEditing()
        layer.changeAttributeValue(target, idx_etat, "MODIFIE_AVANT_SUPPRESSION")
        layer.deleteFeature(target)
        layer.commitChanges()

    _phase(ctx, "P1_modify_then_delete", _p1)
    layer.reload()
    ctx.data["p1_gone"] = layer.getFeature(target) is None or not layer.getFeature(target).isValid()

    # ===== P2 : supprimee PUIS recreee dans le meme commit ==============
    victim = fids[1]
    ctx.data["p2_victim_fid"] = victim
    ctx.data["p2_victim_nom"] = layer.getFeature(victim)["nom"]

    def _p2():
        layer.startEditing()
        layer.deleteFeature(victim)
        layer.addFeature(_new_feature(layer, "recreee", 50.0, 50.0))
        layer.commitChanges()

    _phase(ctx, "P2_delete_then_recreate", _p2)
    layer.reload()

    # ===== P3 : ajoutee puis supprimee avant enregistrement ==============
    def _p3():
        from qgis.core import QgsFeature
        layer.startEditing()
        ghost: QgsFeature = _new_feature(layer, "fantome", 60.0, 60.0)
        layer.addFeature(ghost)
        ctx.data["p3_ghost_temp_fid"] = ghost.id()
        survivor = _new_feature(layer, "survivante", 61.0, 61.0)
        layer.addFeature(survivor)
        layer.deleteFeature(ghost.id())
        layer.commitChanges()

    _phase(ctx, "P3_add_then_delete_same_commit", _p3)
    layer.reload()
    ctx.data["p3_names"] = sorted(
        f["nom"] for f in layer.getFeatures() if f["nom"] is not None)

    # ===== P4 : import massif puis suppression massive ===================
    def _p4_add():
        layer.startEditing()
        for k in range(_MASS_N):
            layer.addFeature(_new_feature(layer, "mass%04d" % k,
                                          float(k % 90), float(k % 45)))
        layer.commitChanges()

    p4 = _phase(ctx, "P4_mass_insert", _p4_add)
    layer.reload()
    mass_fids = [f.id() for f in layer.getFeatures()
                 if (f["nom"] or "").startswith("nom_mass")]
    ctx.data["p4_mass_fids_count"] = len(mass_fids)
    ctx.data["p4_distinct_fingerprints"] = len(
        {e.entity_fingerprint for e in p4})
    ctx.data["p4_snapshots_complete"] = sum(
        1 for e in p4
        if set(_snapshot(e).keys()) >= set(_FIELDS) and _snapshot(e).get("nom"))
    ctx.data["p4_with_geometry"] = sum(1 for e in p4 if e.geometry_wkb)

    def _p4_del():
        layer.startEditing()
        layer.deleteFeatures(mass_fids)
        layer.commitChanges()

    _phase(ctx, "P4_mass_delete", _p4_del)
    layer.reload()
    ctx.data["commit_calls_snapshot"] = [
        {"n_events": c[0], "layer": c[1], "mass": c[2], "deletes": c[3]}
        for c in ctx.data["commit_calls"]
    ]

    # ===== P5 : budget memoire depasse EN PLEIN COMMIT ==================
    remaining = [f.id() for f in layer.getFeatures()]
    remaining.sort()
    ctx.data["p5_remaining"] = list(remaining)
    to_modify = remaining[:2]
    to_delete = remaining[2:4]
    ctx.data["p5_to_modify"] = to_modify
    ctx.data["p5_to_delete"] = to_delete
    original_limit = tracker._MEMORY_HARD_LIMIT_MB

    def _p5():
        # Budget ramene a zero sur l'instance : la premiere verification de
        # pression, juste apres la capture des suppressions, sera vraie.
        tracker._MEMORY_HARD_LIMIT_MB = 0.0
        layer.startEditing()
        for fid in to_modify:
            layer.changeAttributeValue(fid, idx_nom, "P5_MODIFIE")
        layer.deleteFeatures(to_delete)
        layer.addFeature(_new_feature(layer, "p5a", 70.0, 70.0))
        layer.addFeature(_new_feature(layer, "p5b", 71.0, 71.0))
        layer.commitChanges()

    p5 = _phase(ctx, "P5_buffer_pressure", _p5)
    layer.reload()
    ctx.data["p5_ops"] = _ops(p5)
    ctx.data["p5_tracker_still_active"] = tracker.is_active
    ctx.data["p5_reported_event_count"] = (
        ctx.data["commit_calls"][-1][0] if ctx.data["commit_calls"] else None)
    ctx.data["p5_disk_values"] = sorted(
        (f.id(), f["nom"]) for f in layer.getFeatures() if f.id() in to_modify)

    # ===== P6 : la capture redevient normale ============================
    tracker._MEMORY_HARD_LIMIT_MB = original_limit

    def _p6():
        layer.startEditing()
        layer.changeAttributeValue(to_modify[0], idx_nom, "P6_APRES_PRESSION")
        layer.commitChanges()

    _phase(ctx, "P6_recovery", _p6)
    layer.reload()

    flog(
        f"tx_capture_same_commit_races run end: trace_id={ctx.trace_id} "
        f"total={len(ctx.data['wq'].events)} "
        f"phases={{k: len(v) for k, v in ctx.data['phases'].items()}}",
        "INFO",
    )

    ctx.data["layer"] = None
    tracker.disconnect_all()
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
        "fixture_layer_tracked",
        ctx.data.get("connected") is True,
        f"connected={ctx.data.get('connected')} : sans couche suivie le "
        f"scenario ne teste rien",
    ))

    # ===== P1 : modifiee puis supprimee =================================
    p1 = phase("P1_modify_then_delete")
    p1_del = [e for e in p1 if e.operation_type == "DELETE"]
    p1_upd = [e for e in p1 if e.operation_type == "UPDATE"]
    out.append((
        "P1_single_delete_event",
        _ops(p1) == ["DELETE"],
        f"ops={_ops(p1)} attendu=['DELETE'] detail={_describe(p1)} : "
        f"l'entite a ete corrigee puis supprimee avant l'enregistrement ; un "
        f"UPDATE en plus ferait tenter au rewind la modification d'une entite "
        f"qui n'existe plus, et compterait deux fois la meme action dans "
        f"l'historique",
    ))
    out.append((
        "P1_delete_carries_the_pre_edit_state",
        bool(p1_del)
        and _snapshot(p1_del[0]).get("etat") == ctx.data.get("p1_original_etat"),
        f"snapshot={_snapshot(p1_del[0]) if p1_del else None} attendu "
        f"etat={ctx.data.get('p1_original_etat')!r} : la suppression doit "
        f"conserver l'etat d'ORIGINE. Si elle porte "
        f"'MODIFIE_AVANT_SUPPRESSION', restaurer la parcelle la fait revenir "
        f"avec une valeur que la base n'a jamais contenue",
    ))
    out.append((
        "P1_no_update_event",
        len(p1_upd) == 0,
        f"updates={_describe(p1_upd)} attendu=aucun",
    ))
    out.append((
        "P1_feature_really_gone",
        ctx.data.get("p1_gone") is True,
        f"p1_gone={ctx.data.get('p1_gone')} : la suppression doit avoir eu "
        f"lieu cote donnee, sinon la phase ne prouve rien",
    ))

    # ===== P2 : supprimee puis recreee ==================================
    p2 = phase("P2_delete_then_recreate")
    p2_del = [e for e in p2 if e.operation_type == "DELETE"]
    p2_ins = [e for e in p2 if e.operation_type == "INSERT"]
    out.append((
        "P2_delete_and_insert_both_journalled",
        _ops(p2) == ["DELETE", "INSERT"],
        f"ops={_ops(p2)} attendu=['DELETE', 'INSERT'] detail={_describe(p2)} : "
        f"sans le DELETE l'ancienne version est irrecuperable, sans l'INSERT "
        f"la nouvelle entite est invisible dans l'historique",
    ))
    out.append((
        "P2_delete_snapshot_is_the_old_feature",
        bool(p2_del)
        and _snapshot(p2_del[0]).get("nom") == ctx.data.get("p2_victim_nom"),
        f"snapshot={_snapshot(p2_del[0]) if p2_del else None} attendu "
        f"nom={ctx.data.get('p2_victim_nom')!r}",
    ))
    out.append((
        "P2_insert_snapshot_is_the_new_feature",
        bool(p2_ins) and _snapshot(p2_ins[0]).get("nom") == "nom_recreee",
        f"snapshot={_snapshot(p2_ins[0]) if p2_ins else None} attendu "
        f"nom='nom_recreee'",
    ))
    out.append((
        "P2_identities_are_distinct",
        bool(p2_del) and bool(p2_ins)
        and p2_del[0].entity_fingerprint != p2_ins[0].entity_fingerprint,
        f"delete_fp={p2_del[0].entity_fingerprint if p2_del else None} "
        f"insert_fp={p2_ins[0].entity_fingerprint if p2_ins else None} : deux "
        f"entites differentes ne doivent pas partager la meme empreinte, "
        f"sinon le rewind les confond et n'en restaure qu'une",
    ))

    # ===== P3 : ajoutee puis supprimee ==================================
    p3 = phase("P3_add_then_delete_same_commit")
    out.append((
        "P3_only_the_survivor_is_journalled",
        _ops(p3) == ["INSERT"]
        and _snapshot(p3[0]).get("nom") == "nom_survivante",
        f"ops={_ops(p3)} detail={_describe(p3)} attendu=1 INSERT "
        f"nom='nom_survivante' : l'entite creee puis supprimee avant "
        f"enregistrement n'a jamais existe dans la base, elle ne doit pas "
        f"apparaitre dans l'historique ; l'ajout voisin, lui, doit y etre",
    ))
    out.append((
        "P3_disk_matches_the_journal",
        "nom_survivante" in (ctx.data.get("p3_names") or [])
        and "nom_fantome" not in (ctx.data.get("p3_names") or []),
        f"noms sur disque={ctx.data.get('p3_names')} : le journal doit "
        f"decrire exactement ce que le provider a ecrit",
    ))

    # ===== P4 : masse ===================================================
    p4_add = phase("P4_mass_insert")
    p4_del = phase("P4_mass_delete")
    out.append((
        "P4_every_inserted_feature_has_its_event",
        len(p4_add) == _MASS_N and _ops(p4_add) == ["INSERT"] * _MASS_N,
        f"events={len(p4_add)} attendu={_MASS_N} ops_distinctes="
        f"{sorted(set(_ops(p4_add)))} : un import massif partiellement "
        f"journalise laisse des entites impossibles a annuler",
    ))
    out.append((
        "P4_insert_events_have_distinct_identities",
        ctx.data.get("p4_distinct_fingerprints") == _MASS_N,
        f"empreintes distinctes={ctx.data.get('p4_distinct_fingerprints')} "
        f"attendu={_MASS_N} : des empreintes dupliquees font que le rewind "
        f"n'annule qu'une entite sur N",
    ))
    out.append((
        "P4_insert_snapshots_are_complete",
        ctx.data.get("p4_snapshots_complete") == _MASS_N
        and ctx.data.get("p4_with_geometry") == _MASS_N,
        f"snapshots complets={ctx.data.get('p4_snapshots_complete')} "
        f"avec geometrie={ctx.data.get('p4_with_geometry')} attendu="
        f"{_MASS_N}/{_MASS_N} : un INSERT sans instantane ni geometrie ne "
        f"permet ni de revoir ni de recreer l'entite",
    ))
    out.append((
        "P4_every_deleted_feature_has_its_event",
        len(p4_del) == _MASS_N and set(_ops(p4_del)) == {"DELETE"},
        f"events={len(p4_del)} attendu={_MASS_N} ops={sorted(set(_ops(p4_del)))} : "
        f"une suppression massive non integralement journalisee est une perte "
        f"de donnee definitive",
    ))
    calls = ctx.data.get("commit_calls_snapshot") or []
    mass_calls = [c for c in calls if c["mass"]]
    out.append((
        "P4_mass_delete_alert_is_raised",
        len(mass_calls) == 1 and mass_calls[0]["deletes"] == _MASS_N,
        f"appels avec alerte={mass_calls} attendu 1 appel avec "
        f"deletes={_MASS_N} : l'utilisateur doit etre prevenu qu'il vient "
        f"d'effacer {_MASS_N} entites",
    ))

    # ===== P5 : budget memoire depasse en plein commit ==================
    p5 = phase("P5_buffer_pressure")
    p5_upd = [e for e in p5 if e.operation_type == "UPDATE"]
    p5_del = [e for e in p5 if e.operation_type == "DELETE"]
    p5_ins = [e for e in p5 if e.operation_type == "INSERT"]
    n_mod = len(ctx.data.get("p5_to_modify") or [])
    out.append((
        "P5_disk_really_changed",
        all(nom == "P5_MODIFIE"
            for _fid, nom in (ctx.data.get("p5_disk_values") or [])),
        f"valeurs relues={ctx.data.get('p5_disk_values')} attendu 'P5_MODIFIE' "
        f"partout : le commit QGIS a bien ecrit sur le disque, c'est ce qui "
        f"rend la perte de capture grave",
    ))
    out.append((
        "P5_updates_survive_buffer_pressure",
        len(p5_upd) == n_mod,
        f"UPDATE captures={len(p5_upd)} attendu={n_mod} "
        f"(DELETE={len(p5_del)} INSERT={len(p5_ins)}) : quand le budget "
        f"memoire saute, `_capture_edit_buffer_state` fait return juste apres "
        f"les suppressions. Les INSERT sont sauves par le signal "
        f"committedFeaturesAdded, mais l'etat AVANT des entites modifiees "
        f"n'existe nulle part ailleurs : ces modifications sont ecrites sur "
        f"le disque et definitivement absentes du journal. Le rewind ne "
        f"pourra jamais les annuler.",
    ))
    out.append((
        "P5_truncation_is_not_silent",
        ctx.data.get("p5_tracker_still_active") is False
        or ctx.data.get("p5_reported_event_count") == len(p5) + n_mod,
        f"tracker_actif={ctx.data.get('p5_tracker_still_active')} "
        f"evenements annonces a l'interface="
        f"{ctx.data.get('p5_reported_event_count')} reellement captures="
        f"{len(p5)} : le tracker continue et annonce un commit capture sans "
        f"signaler le trou. Le depassement de la file d'ecriture, lui, "
        f"desactive le suivi et declenche `_on_overflow_callback` ; la "
        f"troncature de capture, plus dangereuse, ne previent personne. "
        f"L'utilisateur croit son historique complet.",
    ))

    # ===== P6 : retour a la normale =====================================
    p6 = phase("P6_recovery")
    out.append((
        "P6_capture_recovers_after_pressure",
        len(p6) == 1 and p6[0].operation_type == "UPDATE"
        and _delta(p6[0]).get("nom", {}).get("new") == "P6_APRES_PRESSION",
        f"evenements={_describe(p6)} attendu=1 UPDATE nom -> "
        f"'P6_APRES_PRESSION' : une fois la pression retombee le suivi doit "
        f"reprendre normalement, sinon un seul pic memoire aveugle le plugin "
        f"pour le reste de la session",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"buffer hard limit reached",
        name="pressure_logged_as_error",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_capture_same_commit_races.*trace_id={ctx.trace_id}",
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
