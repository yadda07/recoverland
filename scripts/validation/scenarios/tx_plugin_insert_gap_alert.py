"""tx_plugin_insert_gap_alert - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (tant que RecoverLand ne dit rien, il capture en
entier ; sinon l'utilisateur est prevenu et l'indicateur le montre).

Le mecanisme
------------
Le formulaire de saisie de QGIS laisse a la source les champs qu'elle
remplit elle-meme (DEFAULT d'une colonne, cle auto-incrementee) :
`committedFeaturesAdded` rend ces champs en `QgsUnsetAttributeValue`, pas
la valeur ecrite. `EditSessionTracker._on_committed_features_added` relit
donc les entites ajoutees chez le provider. Quand la relecture ne les rend
pas (lecture echouee, provider qui applique le filtre de la couche a une
lecture par FID), l'instantane de l'INSERT omet ces champs (valeur
inconnue, jamais inventee) et le suivi emet le statut
``insert_values_unverified`` (couche, FID, champs).

`RecoverPlugin._on_capture_status` (recover.py) n'avait aucune branche pour
ce statut : il tombait au bout de la chaine de `elif`, seul
`_update_status_bar` s'executait. Le trou n'atteignait que le journal
technique ; la barre de messages ne disait rien et l'indicateur restait
vert "protege".

Le scenario d'echec concret
---------------------------
L'utilisateur saisit deux points sur une couche GeoPackage dont la colonne
`statut` vaut DEFAULT 'AUTO'. La relecture echoue : l'historique de ces
deux entites ne connait pas leur `statut`, ni `cree_le`, ni `n`. Rien ne le
lui dit ; il croit ses ajouts journalises en entier, et decouvre le trou le
jour ou il en a besoin (fiche d'entite, Restore, Rewind).

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin sous plugin_harness.FakeIface, VRAI suivi branche par
`_init_local_backend`, vraies couches GeoPackage a DEFAULT, saisie comme par
le formulaire (`QgsVectorLayerUtils.createFeature`). Seule la relecture est
simulee vide (`EditSessionTracker._read_back_committed` rend {}), comme dans
tx_capture_insert_provider_values P5.
  G1  couche A, commit de deux ajouts : une alerte CRITIQUE persistante
      (duree 0) nomme la couche, les deux FID et les champs manquants.
  G2  couche A, second commit d'un ajout moins de 120 s apres : son alerte
      atteint aussi la barre (cle par commit, pas etouffee par la
      precedente) et nomme son FID.
  G3  couche B, un ajout : alerte nommant B.
  G4  la sante de capture n'est plus PROTEGE : une raison dit que 4 entites
      ajoutees ont un historique incomplet et nomme A et B et les champs ;
      l'infobulle de l'indicateur la montre.
  G5  relecture retablie, un ajout complet sur A : aucune alerte de trou.
  G6  le journal porte bien les INSERT sans `statut` (le trou est reel,
      pas une fausse alerte).
  G7  projet ferme (Fichier > Nouveau) : la sante du projet suivant ne
      reprend pas le trou d'un autre projet.

Verdict attendu avant correctif : FAIL (G1, G2, G3, G4).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_plugin_insert_gap_alert"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_LAYER_A = "couche_trou_a"
_LAYER_B = "couche_trou_b"
_MISSING = ("statut", "cree_le", "n")


def _make_defaults_gpkg(path: str) -> None:
    """fid AUTOINCREMENT, statut DEFAULT 'AUTO', cree_le DEFAULT now, n DEFAULT 7."""
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("code", ogr.OFTString))
    statut = ogr.FieldDefn("statut", ogr.OFTString)
    statut.SetDefault("'AUTO'")
    lyr.CreateField(statut)
    cree = ogr.FieldDefn("cree_le", ogr.OFTDateTime)
    cree.SetDefault("CURRENT_TIMESTAMP")
    lyr.CreateField(cree)
    n = ogr.FieldDefn("n", ogr.OFTInteger)
    n.SetDefault("7")
    lyr.CreateField(n)
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("code", "SEED")
    feat.SetGeometry(ogr.CreateGeometryFromWkt("POINT (1 1)"))
    lyr.CreateFeature(feat)
    feat = None
    ds = None


def _form_insert(layer, codes) -> bool:
    """What the digitising form does, one commit for all *codes*."""
    from qgis.core import QgsGeometry, QgsPointXY, QgsVectorLayerUtils
    idx = layer.fields().indexFromName("code")
    layer.startEditing()
    for i, code in enumerate(codes):
        feat = QgsVectorLayerUtils.createFeature(
            layer, QgsGeometry.fromPointXY(QgsPointXY(5.0 + i, 5.0 + i)),
            {idx: code})
        if not layer.addFeature(feat):
            layer.rollBack()
            return False
    return bool(layer.commitChanges())


def _fids(layer, codes) -> dict:
    return {f["code"]: f.id() for f in layer.dataProvider().getFeatures()
            if f["code"] in codes}


def _bar_since(iface, start: int) -> list:
    from qgis.core import Qgis
    out = []
    for _title, text, level, duration in iface.bar.messages[start:]:
        out.append({"text": text, "critical": level == Qgis.MessageLevel.Critical,
                    "duration": duration})
    return out


def _gap_messages(bar, layer_name: str) -> list:
    return [m for m in bar if layer_name in m["text"]
            and any(f in m["text"] for f in _MISSING)]


def _insert_attrs(journal: str) -> dict:
    """code -> attributes the journal's INSERT carries."""
    if not journal:
        return {}
    from recoverland.core.long_paths import when_needed
    conn = sqlite3.connect(when_needed(journal))
    try:
        rows = conn.execute(
            "SELECT attributes_json FROM audit_event WHERE operation_type = "
            "'INSERT' AND restored_from_event_id IS NULL").fetchall()
    finally:
        conn.close()
    out = {}
    for (raw,) in rows:
        try:
            attrs = (json.loads(raw or "{}") or {}).get("all_attributes") or {}
        except ValueError:
            continue
        if str(attrs.get("code", "")).startswith("TROU") or attrs.get("code") == "COMPLET":
            out[attrs["code"]] = attrs
    return out


def setup(ctx):
    from qgis.core import QgsProject, QgsSettings
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    QgsSettings().setValue("RecoverLand/tracking_enabled", True)
    QgsSettings().setValue("RecoverLand/tracking_disabled_by_disk", False)
    tmp = tempfile.mkdtemp(prefix="rl_q_gap_")
    ctx.data["tmp"] = tmp
    for name in ("a", "b"):
        _make_defaults_gpkg(os.path.join(tmp, f"{name}.gpkg"))
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} tmp={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsProject, QgsVectorLayer
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    tmp = d["tmp"]
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    had = "_read_back_committed" in EditSessionTracker.__dict__
    saved = EditSessionTracker.__dict__.get("_read_back_committed")
    try:
        plugin.initGui()
        pump(0.3)
        d["journal"] = plugin._journal.path if plugin._journal.is_open else None
        la = QgsVectorLayer(f"{os.path.join(tmp, 'a.gpkg')}|layername=pts", _LAYER_A, "ogr")
        lb = QgsVectorLayer(f"{os.path.join(tmp, 'b.gpkg')}|layername=pts", _LAYER_B, "ogr")
        QgsProject.instance().addMapLayers([la, lb])
        pump(0.2)
        d["health_before"] = plugin._capture_health().state
        # The provider does not return the added features on read-back.
        EditSessionTracker._read_back_committed = lambda self, lyr, feats: {}
        start = len(iface.bar.messages)
        d["g1_commit"] = _form_insert(la, ["TROU_A1", "TROU_A2"])
        pump(0.3)
        d["g1_bar"] = _bar_since(iface, start)
        d["g1_fids"] = _fids(la, ("TROU_A1", "TROU_A2"))
        start = len(iface.bar.messages)
        d["g2_commit"] = _form_insert(la, ["TROU_A3"])
        pump(0.3)
        d["g2_bar"] = _bar_since(iface, start)
        d["g2_fids"] = _fids(la, ("TROU_A3",))
        start = len(iface.bar.messages)
        d["g3_commit"] = _form_insert(lb, ["TROU_B1"])
        pump(0.3)
        d["g3_bar"] = _bar_since(iface, start)
        health = plugin._capture_health()
        d["g4_state"] = health.state
        d["g4_reasons"] = list(health.reasons)
        plugin._update_status_bar()
        ind = plugin._status_indicator
        d["g4_tooltip"] = ind.toolTip() if ind is not None else None
        # Read-back back to normal: a complete addition says nothing.
        if had:
            EditSessionTracker._read_back_committed = saved
        else:
            del EditSessionTracker._read_back_committed
        had = None
        start = len(iface.bar.messages)
        d["g5_commit"] = _form_insert(la, ["COMPLET"])
        pump(0.3)
        d["g5_bar"] = _bar_since(iface, start)
        plugin._write_queue.wait_until_drained(5.0)
        d["g6_attrs"] = _insert_attrs(d["journal"])
        # File > New: the next project's health is about its own journal.
        QgsProject.instance().clear()
        pump(0.4)
        d["g7_reasons"] = list(plugin._capture_health().reasons)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if had is not None:
            if had:
                EditSessionTracker._read_back_committed = saved
            elif "_read_back_committed" in EditSessionTracker.__dict__:
                del EditSessionTracker._read_back_committed
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} data="
         f"{json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")


def teardown(ctx):
    tmp = ctx.data.get("tmp")
    if tmp:
        shutil.rmtree(tmp, ignore_errors=True)


def _names_fids(text: str, fids) -> bool:
    """The FIDs, as the list the alert gives them ("FID 2, 3")."""
    return bool(fids) and (
        "FID " + ", ".join(str(fid) for fid in sorted(fids))) in text


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    crash = f"crash={d.get('crash')} {d.get('trace', '')}"
    out.append((
        "fixture_commits_landed",
        not d.get("crash") and all(d.get(k) for k in (
            "g1_commit", "g2_commit", "g3_commit", "g5_commit"))
        and len(d.get("g1_fids") or {}) == 2 and len(d.get("g2_fids") or {}) == 1,
        f"commits={[d.get(k) for k in ('g1_commit', 'g2_commit', 'g3_commit', 'g5_commit')]} "
        f"fids={d.get('g1_fids')} {d.get('g2_fids')} {crash}",
    ))
    attrs = d.get("g6_attrs") or {}
    gap_rows = [attrs.get(c) for c in ("TROU_A1", "TROU_A2", "TROU_A3", "TROU_B1")]
    out.append((
        "g6_the_gap_is_real",
        all(r is not None and "statut" not in r for r in gap_rows)
        and (attrs.get("COMPLET") or {}).get("statut") == "AUTO",
        f"INSERT journalises={attrs}. Sans ce trou reel, les alertes ci-dessous "
        f"ne prouveraient rien",
    ))
    fids1 = list((d.get("g1_fids") or {}).values())
    g1 = [m for m in _gap_messages(d.get("g1_bar") or [], _LAYER_A)
          if m["critical"] and m["duration"] == 0 and _names_fids(m["text"], fids1)
          and all(f in m["text"] for f in _MISSING)]
    out.append((
        "g1_gap_alert_critical_persistent_names_layer_fids_fields",
        bool(g1),
        f"barre apres le commit de deux ajouts sans relecture={d.get('g1_bar')} "
        f"(FID {fids1}, champs {list(_MISSING)}). Sans alerte critique qui "
        f"reste affichee et qui nomme la couche, les entites et les champs, "
        f"l'utilisateur croit ses ajouts journalises en entier",
    ))
    fids2 = list((d.get("g2_fids") or {}).values())
    g2 = [m for m in _gap_messages(d.get("g2_bar") or [], _LAYER_A)
          if m["critical"] and _names_fids(m["text"], fids2)]
    out.append((
        "g2_second_commit_gap_not_swallowed",
        bool(g2),
        f"barre apres un second commit sur la meme couche={d.get('g2_bar')} "
        f"(FID {fids2}). Une alerte etouffee par la precedente laisse ce "
        f"nouveau trou sans nom",
    ))
    g3 = [m for m in _gap_messages(d.get("g3_bar") or [], _LAYER_B) if m["critical"]]
    out.append((
        "g3_other_layer_named",
        bool(g3),
        f"barre apres un ajout sans relecture sur {_LAYER_B}={d.get('g3_bar')}",
    ))
    reasons = d.get("g4_reasons") or []
    gap_reason = [r for r in reasons if "incomplet" in r and _LAYER_A in r
                  and _LAYER_B in r and r.startswith("4 ")
                  and all(f in r for f in _MISSING)]
    out.append((
        "g4_capture_health_shows_incomplete_history",
        d.get("g4_state") not in ("PROTECTED", "PERSISTING", None) and bool(gap_reason),
        f"etat={d.get('g4_state')} (avant : {d.get('health_before')}) "
        f"raisons={reasons}. Un indicateur vert 'protege' au-dessus de quatre "
        f"ajouts dont l'historique est incomplet dit le contraire des faits",
    ))
    tip = d.get("g4_tooltip") or ""
    out.append((
        "g4_indicator_tooltip_shows_it",
        bool(gap_reason) and gap_reason[0] in tip,
        f"infobulle={tip!r}",
    ))
    g5 = _gap_messages(d.get("g5_bar") or [], _LAYER_A)
    out.append((
        "g5_complete_insert_raises_no_gap_alert",
        d.get("g5_commit") is True and not g5,
        f"barre apres un ajout relu normalement={d.get('g5_bar')} : une fausse "
        f"alerte apprend a ignorer les vraies",
    ))
    g7 = [r for r in d.get("g7_reasons") or [] if _LAYER_A in r or _LAYER_B in r]
    out.append((
        "g7_next_project_not_blamed",
        "g7_reasons" in d and not g7,
        f"raisons apres Fichier > Nouveau={d.get('g7_reasons')} : le projet "
        f"suivant ne porte pas le trou d'un autre journal",
    ))
    out.append((
        "unload_clean",
        not d.get("unload_error"),
        f"unload={d.get('unload_error')}",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"{SCENARIO_ID} (setup|run): trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
