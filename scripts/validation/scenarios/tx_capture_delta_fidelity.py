"""tx_capture_delta_fidelity - RecoverLand validation runtime.

Invariant: I-1 (fidelite de capture : ce que le journal enregistre doit etre
exactement ce que l'utilisateur a fait, ni plus, ni moins).

Le mecanisme
------------
`EditSessionTracker` observe un commit QGIS en trois temps :

    beforeCommitChanges       -> lecture de l'etat OLD depuis le provider
    committed*Changes         -> etat NEW authoritatif confirme par le provider
    afterCommitChanges        -> `_generate_events` fabrique les AuditEvent

Le delta d'un UPDATE est calcule par `serialization.compute_update_delta`
uniquement sur `changed_field_names`, et chaque valeur passe par
`serialization.serialize_value`. Tout ecart entre la valeur reelle stockee
par le provider et la valeur serialisee devient une erreur DEFINITIVE : le
journal est la seule memoire du plugin, un rewind rejouera ce qui est ecrit
la, pas ce que l'utilisateur avait vraiment.

Deux causes racines mises a nu par ce scenario
----------------------------------------------
1. `_capture_modifications` lit l'etat OLD des entites "attributs seuls"
   avec `request.setFlags(QgisCompat.NO_GEOMETRY)` : le snapshot OLD sort
   donc avec `geometry_wkb=None`. L'etat NEW, lui, est lu par
   `_capture_new_state` AVEC la geometrie. `_make_update_event` compare
   ensuite les deux via `geometries_equal(None, wkb_courant)` -> toujours
   different -> `geom_changed=True`. Consequences en chaine : le garde-fou
   `if delta_json is None and not geom_changed: return None` (filtre du faux
   commit) est mort pour toute edition d'attribut, et chaque UPDATE
   attributaire porte une fausse geometrie NEW.
2. `serialize_value` traite toute chaine egale a
   `QgsApplication.nullRepresentation()` comme une absence de valeur.

Scenarios d'echec concrets couverts
-----------------------------------
A/B  L'utilisateur annule ses modifications (rollBack) puis en refait une
     autre : si le buffer de la session annulee survivait, le journal
     enregistrerait une suppression jamais effectuee et un OLD faux, donc
     un rewind restaurerait une entite que l'utilisateur avait gardee.
C    L'utilisateur ouvre l'edition, re-saisit la meme valeur et enregistre :
     le journal ne doit pas se remplir d'evenements fantomes qui polluent
     l'historique et font croire a des modifications inexistantes.
D    Un seul champ modifie sur vingt : si le delta embarque les vingt champs,
     un rewind ecrase dix-neuf valeurs que personne n'avait touchees.
E/F  Vidage d'un champ (valeur -> NULL) et remplissage (NULL -> valeur) :
     les deux sens doivent etre journalises, sinon un rewind ne sait pas
     re-vider ou re-remplir la cellule.
G/H  Le texte litteral egal a `QgsApplication.nullRepresentation()` ("NULL"
     par defaut) : `serialize_value` le confond avec l'absence de valeur.
     Une parcelle dont le champ vaut reellement la chaine "NULL" est
     journalisee comme vide ; le rewind vide alors une cellule qui
     contenait du texte. Perte de donnee silencieuse a la capture.
I    Deplacement pur (geometrie seule, aucun attribut) : l'evenement doit
     porter OLD et NEW geometrie, sinon le rewind d'un deplacement ne
     remet jamais l'entite a sa place.

Montage (vrai GPKG dans un dossier temporaire, vrais signaux Qt) :
    - GPKG `parcelles` : 20 champs texte f00..f19 + 3 entites
    - EditSessionTracker branche sur la couche via connect_layer
    - WriteQueue de capture qui memorise chaque lot enqueue
    - chaque phase = un vrai commit QGIS, ses evenements sont isoles
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_capture_delta_fidelity"
INVARIANT = "I-1"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_N_FIELDS = 20
_N_FEATURES = 3


# --------------------------------------------------------------------------
# Fixture
# --------------------------------------------------------------------------
def _make_gpkg(tmpdir: str) -> str:
    """Cree un GPKG a 20 champs texte, sans entite."""
    from osgeo import ogr

    path = os.path.join(tmpdir, "parcelles.gpkg")
    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("driver GPKG indisponible")
    ds = driver.CreateDataSource(path)
    lyr = ds.CreateLayer("parcelles", geom_type=ogr.wkbPoint)
    for i in range(_N_FIELDS):
        lyr.CreateField(ogr.FieldDefn("f%02d" % i, ogr.OFTString))
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


def _seed(layer) -> list:
    """Insere 3 entites avant tout tracking. Retourne les fid reels."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY

    layer.startEditing()
    for k in range(_N_FEATURES):
        feat = QgsFeature(layer.fields())
        for i in range(_N_FIELDS):
            feat.setAttribute("f%02d" % i, "v%d_%02d" % (k, i))
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(float(k), float(k))))
        layer.addFeature(feat)
    layer.commitChanges()
    layer.reload()
    # OGR numerote les GPKG a partir de 1 : on lit les vrais fid.
    return [f.id() for f in layer.getFeatures()]


class _CapturingWQ:
    """WriteQueue de capture : memorise tous les lots enqueues."""

    def __init__(self):
        self.batches: list = []
        self.events: list = []

    def enqueue(self, events) -> bool:
        self.batches.append(list(events))
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


def _snapshot_attrs(event) -> dict:
    try:
        return json.loads(event.attributes_json or "{}").get("all_attributes", {})
    except (TypeError, ValueError):
        return {}


def _point_of(wkb):
    """Retourne (x, y) arrondis a partir d'un WKB, ou None."""
    if not wkb:
        return None
    from qgis.core import QgsGeometry

    geom = QgsGeometry()
    geom.fromWkb(wkb)
    if geom.isNull() or geom.isEmpty():
        return None
    pt = geom.asPoint()
    return (round(pt.x(), 6), round(pt.y(), 6))


def _describe(events) -> list:
    """Resume lisible d'un lot d'evenements, pour les messages d'assertion."""
    out = []
    for ev in events:
        out.append({
            "op": ev.operation_type,
            "identity": ev.feature_identity_json,
            "delta": _delta(ev) if ev.operation_type == "UPDATE" else None,
            "geom_old": _point_of(ev.geometry_wkb),
            "geom_new": _point_of(ev.new_geometry_wkb),
        })
    return out


# --------------------------------------------------------------------------
# Scenario
# --------------------------------------------------------------------------
def setup(ctx):
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog
    from qgis.core import QgsApplication

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_delta_")
    path = _make_gpkg(tmpdir)
    layer = _load_layer(path)
    fids = _seed(layer)

    wq = _CapturingWQ()
    tracker = EditSessionTracker(wq, _DummyJM())
    tracker.activate()
    tracker.connect_layer(layer)

    ctx.data["tmpdir"] = tmpdir
    ctx.data["layer"] = layer
    ctx.data["fids"] = fids
    ctx.data["wq"] = wq
    ctx.data["tracker"] = tracker
    ctx.data["connected"] = layer.id() in tracker._connected_layers
    ctx.data["null_repr"] = QgsApplication.nullRepresentation()
    ctx.data["phases"] = {}

    flog(
        f"tx_capture_delta_fidelity setup: trace_id={ctx.trace_id} "
        f"gpkg={path} fids={fids} connected={ctx.data['connected']} "
        f"null_repr={ctx.data['null_repr']!r}",
        "INFO",
    )


def _phase(ctx, name, body):
    """Execute une phase et isole les evenements qu'elle a produits."""
    from recoverland.core.logger import flog

    wq = ctx.data["wq"]
    mark = len(wq.events)
    body()
    produced = wq.events[mark:]
    ctx.data["phases"][name] = produced
    flog(
        f"tx_capture_delta_fidelity phase={name} trace_id={ctx.trace_id} "
        f"events={len(produced)} detail={_describe(produced)}",
        "INFO",
    )
    return produced


def run(ctx):
    from recoverland.core.logger import flog
    from qgis.core import QgsGeometry, QgsPointXY

    layer = ctx.data["layer"]
    fids = ctx.data["fids"]
    fid_a, fid_b, fid_c = fids[0], fids[1], fids[2]
    idx = {name: layer.fields().indexFromName(name)
           for name in ("f03", "f05", "f11", "f12")}
    ctx.data["field_idx"] = dict(idx)

    flog(f"tx_capture_delta_fidelity run start: trace_id={ctx.trace_id}", "INFO")

    # ===== A : modifications puis rollBack ==============================
    def _a():
        layer.startEditing()
        layer.changeAttributeValue(fid_a, idx["f03"], "ANNULE")
        layer.changeGeometry(fid_b, QgsGeometry.fromPointXY(QgsPointXY(99.0, 99.0)))
        layer.deleteFeature(fid_c)
        ctx.data["a_buffer_had_changes"] = bool(
            layer.editBuffer().changedAttributeValues()
            or layer.editBuffer().deletedFeatureIds())
        layer.rollBack()

    _phase(ctx, "A_rollback", _a)
    layer.reload()
    ctx.data["a_buffer_dropped"] = layer.id() not in ctx.data["tracker"]._buffers
    ctx.data["a_feature_count"] = layer.featureCount()
    ctx.data["a_value_f03"] = layer.getFeature(fid_a)["f03"]

    # ===== B : une vraie modification apres le rollback ==================
    def _b():
        layer.startEditing()
        layer.changeAttributeValue(fid_a, idx["f03"], "B_NEW")
        layer.commitChanges()

    _phase(ctx, "B_after_rollback", _b)
    layer.reload()

    # ===== C : commit sans changement reel (meme valeur) =================
    def _c():
        layer.startEditing()
        current = layer.getFeature(fid_a)["f05"]
        ctx.data["c_same_value"] = current
        layer.changeAttributeValue(fid_a, idx["f05"], current)
        ctx.data["c_buffer_not_empty"] = bool(
            layer.editBuffer().changedAttributeValues())
        layer.commitChanges()

    _phase(ctx, "C_false_commit", _c)
    layer.reload()

    # ===== D : un champ sur vingt =======================================
    def _d():
        layer.startEditing()
        layer.changeAttributeValue(fid_b, idx["f11"], "D_NEW")
        layer.commitChanges()

    _phase(ctx, "D_single_field", _d)
    layer.reload()

    # ===== E : valeur -> NULL ===========================================
    def _e():
        layer.startEditing()
        layer.changeAttributeValue(fid_b, idx["f11"], None)
        layer.commitChanges()

    _phase(ctx, "E_to_null", _e)
    layer.reload()
    ctx.data["e_stored"] = layer.getFeature(fid_b)["f11"]

    # ===== F : NULL -> valeur ===========================================
    def _f():
        layer.startEditing()
        layer.changeAttributeValue(fid_b, idx["f11"], "F_BACK")
        layer.commitChanges()

    _phase(ctx, "F_from_null", _f)
    layer.reload()

    # ===== G : ecriture du texte litteral "NULL" ========================
    null_repr = ctx.data["null_repr"]

    def _g():
        layer.startEditing()
        layer.changeAttributeValue(fid_c, idx["f12"], null_repr)
        layer.commitChanges()

    _phase(ctx, "G_literal_null_repr", _g)
    layer.reload()
    ctx.data["g_stored"] = layer.getFeature(fid_c)["f12"]

    # ===== H : on quitte le texte litteral "NULL" =======================
    def _h():
        layer.startEditing()
        layer.changeAttributeValue(fid_c, idx["f12"], "H_NEW")
        layer.commitChanges()

    _phase(ctx, "H_leaving_literal_null", _h)
    layer.reload()

    # ===== I : deplacement pur (geometrie seule) ========================
    def _i():
        layer.startEditing()
        layer.changeGeometry(
            fid_a, QgsGeometry.fromPointXY(QgsPointXY(42.5, -7.25)))
        layer.commitChanges()

    _phase(ctx, "I_geometry_only", _i)
    layer.reload()

    ctx.data["total_events"] = len(ctx.data["wq"].events)
    flog(
        f"tx_capture_delta_fidelity run end: trace_id={ctx.trace_id} "
        f"total_events={ctx.data['total_events']} "
        f"phases={{k: len(v) for k, v in ctx.data['phases'].items()}}",
        "INFO",
    )

    # Le runner n'a pas de teardown : on libere ici. OGR peut encore tenir
    # le fichier ouvert sous Windows, d'ou ignore_errors.
    ctx.data["layer"] = None
    ctx.data["tracker"].disconnect_all()
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


# --------------------------------------------------------------------------
# Assertions
# --------------------------------------------------------------------------
def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    ph = ctx.data.get("phases") or {}
    null_repr = ctx.data.get("null_repr")

    def phase(name):
        return ph.get(name) or []

    # ===== Montage ======================================================
    out.append((
        "fixture_layer_tracked",
        ctx.data.get("connected") is True,
        f"connected={ctx.data.get('connected')} : la couche GPKG doit etre "
        f"acceptee par connect_layer, sinon le scenario ne teste rien",
    ))

    # ===== A : rollBack =================================================
    out.append((
        "A_rollback_had_real_pending_changes",
        ctx.data.get("a_buffer_had_changes") is True,
        f"a_buffer_had_changes={ctx.data.get('a_buffer_had_changes')} : le "
        f"tampon d'edition doit vraiment contenir des changements avant le "
        f"rollBack, sinon la phase ne prouve rien",
    ))
    out.append((
        "A_rollback_journals_nothing",
        len(phase("A_rollback")) == 0,
        f"evenements={_describe(phase('A_rollback'))} : l'utilisateur a annule "
        f"ses modifications, le journal doit rester vide. Un evenement ici "
        f"ferait apparaitre dans l'historique une suppression et une "
        f"modification qui n'ont jamais eu lieu.",
    ))
    out.append((
        "A_rollback_keeps_the_data",
        ctx.data.get("a_feature_count") == _N_FEATURES
        and ctx.data.get("a_value_f03") == "v0_03",
        f"features={ctx.data.get('a_feature_count')} f03="
        f"{ctx.data.get('a_value_f03')!r} attendu={_N_FEATURES} et 'v0_03' : "
        f"le rollBack doit rendre la couche intacte",
    ))
    out.append((
        "A_rollback_drops_the_buffer",
        ctx.data.get("a_buffer_dropped") is True,
        f"a_buffer_dropped={ctx.data.get('a_buffer_dropped')} : le tampon de "
        f"la session annulee doit etre detruit, sinon ses snapshots polluent "
        f"le commit suivant (OLD errone donc rewind faux)",
    ))

    # ===== B : la modification suivante est propre ======================
    b_events = phase("B_after_rollback")
    b_delta = _delta(b_events[0]) if b_events else {}
    out.append((
        "B_one_update_only",
        len(b_events) == 1 and b_events[0].operation_type == "UPDATE",
        f"evenements={_describe(b_events)} attendu=1 UPDATE : apres un "
        f"rollback, seule la nouvelle modification doit etre journalisee",
    ))
    out.append((
        "B_old_value_is_the_original",
        b_delta.get("f03", {}).get("old") == "v0_03"
        and b_delta.get("f03", {}).get("new") == "B_NEW",
        f"delta={b_delta} attendu f03 old='v0_03' new='B_NEW' : si le OLD "
        f"portait 'ANNULE' (valeur annulee par le rollback), un rewind "
        f"restaurerait une valeur que la donnee n'a jamais eue",
    ))
    out.append((
        "B_identity_carries_the_primary_key",
        bool(b_events) and '"pk_field"' in (b_events[0].feature_identity_json or ""),
        f"identity={b_events[0].feature_identity_json if b_events else None} : "
        f"sur GPKG l'identite doit porter la cle primaire, sinon la "
        f"restauration se rabat sur un FID qui peut avoir ete recycle",
    ))

    # ===== C : faux commit ==============================================
    out.append((
        "C_edit_buffer_was_not_empty",
        ctx.data.get("c_buffer_not_empty") is True,
        f"c_buffer_not_empty={ctx.data.get('c_buffer_not_empty')} : QGIS doit "
        f"bien avoir enregistre un changement (meme valeur) pour que la "
        f"phase teste le filtrage du faux commit",
    ))
    out.append((
        "C_identical_value_produces_no_event",
        len(phase("C_false_commit")) == 0,
        f"evenements={_describe(phase('C_false_commit'))} attendu=0 : "
        f"re-saisir la meme valeur ne modifie rien ; un evenement fantome ici "
        f"gonfle l'historique et fait croire a l'utilisateur qu'il a modifie "
        f"la donnee. Le delta est bien vide, mais l'evenement passe quand meme "
        f"parce que le OLD est lu sans geometrie (NO_GEOMETRY) et le NEW avec : "
        f"geom_changed est vrai a tort, donc le filtre du faux commit ne "
        f"declenche jamais sur une edition d'attributs.",
    ))

    # ===== D : un champ sur vingt =======================================
    d_events = phase("D_single_field")
    d_delta = _delta(d_events[0]) if d_events else {}
    out.append((
        "D_single_update_event",
        len(d_events) == 1 and d_events[0].operation_type == "UPDATE",
        f"evenements={_describe(d_events)} attendu=1 UPDATE",
    ))
    out.append((
        "D_delta_holds_only_the_edited_field",
        list(d_delta.keys()) == ["f11"],
        f"delta={d_delta} attendu uniquement 'f11' sur {_N_FIELDS} champs : "
        f"un delta trop large fait ecraser par le rewind 19 valeurs que "
        f"l'utilisateur n'avait pas touchees",
    ))
    out.append((
        "D_no_phantom_geometry",
        bool(d_events)
        and d_events[0].geometry_wkb is None
        and d_events[0].new_geometry_wkb is None,
        f"geom_old={_point_of(d_events[0].geometry_wkb) if d_events else None} "
        f"geom_new={_point_of(d_events[0].new_geometry_wkb) if d_events else None} "
        f"attendu=None/None : aucune geometrie n'a ete touchee. L'evenement "
        f"pretend pourtant que la geometrie est passee de rien a sa position "
        f"actuelle. Sur une couche sans cle primaire (shapefile, GeoJSON), "
        f"restore_service._verify_update_target voit un new_geometry_wkb et "
        f"exige que l'entite porte encore CETTE geometrie : il suffit que "
        f"l'utilisateur ait deplace l'entite depuis pour que la restauration "
        f"d'une simple correction de texte soit refusee (target_unverifiable) "
        f"alors qu'elle ne touche pas a la geometrie.",
    ))

    # ===== E / F : les deux sens du NULL ================================
    e_events = phase("E_to_null")
    e_delta = _delta(e_events[0]) if e_events else {}
    out.append((
        "E_value_to_null_is_captured",
        len(e_events) == 1
        and e_delta.get("f11", {}).get("old") == "D_NEW"
        and "new" in e_delta.get("f11", {})
        and e_delta.get("f11", {}).get("new") is None,
        f"evenements={_describe(e_events)} delta={e_delta} attendu f11 "
        f"old='D_NEW' new=None : vider une cellule est une modification comme "
        f"une autre ; non capturee, elle est irrecuperable",
    ))

    f_events = phase("F_from_null")
    f_delta = _delta(f_events[0]) if f_events else {}
    out.append((
        "F_null_to_value_is_captured",
        len(f_events) == 1
        and f_delta.get("f11", {}).get("old") is None
        and f_delta.get("f11", {}).get("new") == "F_BACK",
        f"evenements={_describe(f_events)} delta={f_delta} attendu f11 "
        f"old=None new='F_BACK' : sans le OLD=None, un rewind ne saurait pas "
        f"re-vider la cellule",
    ))

    # ===== G / H : le texte litteral egal a la representation du NULL ===
    g_events = phase("G_literal_null_repr")
    g_delta = _delta(g_events[0]) if g_events else {}
    out.append((
        "G_provider_really_stored_the_text",
        ctx.data.get("g_stored") == null_repr,
        f"valeur relue={ctx.data.get('g_stored')!r} attendu={null_repr!r} : "
        f"le provider stocke bien le texte, c'est donc de la vraie donnee "
        f"utilisateur",
    ))
    out.append((
        "G_literal_null_text_is_journalled_verbatim",
        g_delta.get("f12", {}).get("new") == null_repr,
        f"delta={g_delta} attendu f12 new={null_repr!r} : le champ contient le "
        f"TEXTE {null_repr!r}, pas une absence de valeur. Journalise comme "
        f"NULL, l'historique affiche une cellule vide et un rewind qui "
        f"repasse par cet evenement efface le texte de l'utilisateur.",
    ))

    h_events = phase("H_leaving_literal_null")
    h_delta = _delta(h_events[0]) if h_events else {}
    out.append((
        "H_old_literal_null_text_is_preserved",
        h_delta.get("f12", {}).get("old") == null_repr,
        f"delta={h_delta} attendu f12 old={null_repr!r} : c'est le sens "
        f"destructeur. L'etat AVANT modification etait le texte {null_repr!r} ; "
        f"capture comme None, le rewind videra la cellule au lieu d'y "
        f"remettre le texte. La donnee est perdue definitivement.",
    ))

    # ===== I : deplacement pur ==========================================
    i_events = phase("I_geometry_only")
    i_first = i_events[0] if i_events else None
    out.append((
        "I_geometry_only_update_exists",
        len(i_events) == 1 and i_first.operation_type == "UPDATE",
        f"evenements={_describe(i_events)} attendu=1 UPDATE : deplacer une "
        f"entite sans toucher aux attributs reste une modification",
    ))
    out.append((
        "I_both_geometries_captured",
        i_first is not None
        and _point_of(i_first.geometry_wkb) == (0.0, 0.0)
        and _point_of(i_first.new_geometry_wkb) == (42.5, -7.25),
        f"geom_old={_point_of(i_first.geometry_wkb) if i_first else None} "
        f"geom_new={_point_of(i_first.new_geometry_wkb) if i_first else None} "
        f"attendu (0.0, 0.0) -> (42.5, -7.25) : sans la geometrie OLD le "
        f"rewind ne peut pas remettre l'entite a sa place",
    ))
    out.append((
        "I_attribute_delta_is_empty",
        i_first is not None and _delta(i_first) == {},
        f"delta={_delta(i_first) if i_first else None} attendu vide : aucun "
        f"attribut n'a bouge, en inventer un ferait ecraser des valeurs "
        f"valides au rewind",
    ))

    # ===== Traces =======================================================
    out.append(assert_log_contains(
        ctx.records,
        r"produced 0 real events",
        name="false_commit_guard_is_reachable",
        min_count=1,
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_capture_delta_fidelity.*trace_id={ctx.trace_id}",
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
