# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_plugin_update_gap_alert - RecoverLand validation runtime.

Invariant: I-CAPTURE-02 (tant que RecoverLand ne dit rien, il capture en
entier ; sinon l'utilisateur est prevenu et l'indicateur le montre).

Le mecanisme
------------
Le cote NEW d'un UPDATE vient des signaux ``committedAttributeValuesChanges``
et ``committedGeometriesChanges`` : ils portent ce que le tampon d'edition a
ENVOYE, pas ce que la source a ecrit (un champ N(12,2) de Shapefile garde
3.33 pour le 3.3333333333333335 du calculateur de champ).
`EditSessionTracker._on_committed_attribute_values_changes` (et son pendant
geometrie) relit donc les entites modifiees chez le provider. Quand la
relecture ne les rend pas (lecture echouee, provider qui applique le filtre
de la couche a une lecture par FID), le cote NEW garde la valeur envoyee et
le suivi emet le statut ``update_values_unverified`` (couche, FID, champs ;
la geometrie y est nommee "(geometry)").

`RecoverPlugin._on_capture_status` (recover.py) n'avait aucune branche pour
ce statut, ajoute dans le suivi en meme temps que la branche des ajouts
(``insert_values_unverified``) : il tombait au bout de la chaine de `elif`,
seul `_update_status_bar` s'executait. Le trou n'atteignait que le journal
technique ; la barre de messages ne disait rien, l'indicateur restait vert
"protege". Tout statut que le suivi ajoute sans branche dans le plugin
subit le meme sort : le scenario verifie aussi, sur le source, que chaque
statut emis par le suivi a sa branche, et qu'un statut inconnu du plugin
est quand meme dit a l'utilisateur.

Le scenario d'echec concret
---------------------------
L'utilisateur divise par 3 le champ `val` d'une couche Shapefile au
calculateur de champ. La relecture echoue : l'historique garde 3.3333...
alors que le fichier tient 3.33. Rien ne le lui dit. Le jour ou il veut
annuler cette modification (Restore) ou revenir avant elle (Rewind), le
moteur prouve sa cible par ce cote NEW, ne le retrouve pas dans la source
et refuse (``post_attr_mismatch``), sans que rien ne l'ait prevenu ; ou
l'historique lui montre une valeur que ses donnees n'ont jamais tenue, sous
un indicateur qui dit "protege".

Ce que le scenario execute reellement
-------------------------------------
VRAI RecoverPlugin sous plugin_harness.FakeIface, VRAI suivi branche par
`_init_local_backend`, vraies couches (Shapefile N(12,2), GeoPackage), vrais
commits. Seule la relecture est simulee vide
(`EditSessionTracker._read_back_updated` rend {}), comme dans
tx_capture_update_provider_values U5.
  M1  couche A (Shapefile), calculateur de champ sur 3 entites, un commit :
      une alerte CRITIQUE persistante (duree 0) nomme la couche, les FID,
      le champ `val`, dit que la valeur journalisee est celle ENVOYEE et
      ce que cela coute (Restore / Rewind).
  M2  couche A, un commit modifie la valeur ET la geometrie d'une entite
      (deux statuts du meme commit) : les deux atteignent la barre (la
      geometrie n'est pas etouffee par l'alerte des attributs du meme
      commit), chacune nomme le FID.
  M3  couche B (GeoPackage), une modification : alerte nommant B.
  M4  la sante de capture n'est plus PROTEGE : une raison dit que 5
      modifications (3 + 1 + 1 : les deux statuts de M2 portent sur la
      meme entite du meme commit) ont un cote NEW non relu, nomme A et B,
      `val` et la geometrie ; l'infobulle de l'indicateur la montre.
  M5  relecture retablie, une modification sur B : aucune alerte de trou.
  M6  le journal porte bien, cote NEW, la valeur envoyee (3.3333...) que
      le fichier ne tient pas (3.33) : le trou est reel. Le Restore de
      cette modification est ensuite tente, plugin decharge, et son
      resultat note dans le journal du run (mesure du cout annonce ; non
      asserte, le moteur de restauration est d'une autre zone).
  M7  un statut que le plugin ne connait pas atteint quand meme la barre,
      en le nommant (plus aucun statut du suivi ne finit en silence).
  M8  projet ferme (Fichier > Nouveau) : la sante du projet suivant ne
      reprend pas le trou d'un autre projet.
  S1  (source) chaque statut emis par le suivi (`_notify_status("...")`
      dans core/) a sa branche dans `RecoverPlugin._on_capture_status`.

Verdict attendu avant correctif : FAIL (M1, M2, M3, M4, M7, S1).
"""
from __future__ import annotations

import ast
import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_plugin_update_gap_alert"
INVARIANT = "I-CAPTURE-02"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
# Repointable (pre-fix measurement on a copy of recover.py).
_RECOVER_SOURCE = _PLUGIN_ROOT / "recover.py"
_CORE_DIR = _PLUGIN_ROOT / "core"
_LAYER_A = "couche_maj_a"
_LAYER_B = "couche_maj_b"
_FUTURE_KIND = "statut_futur_du_suivi"
_SENT_M1 = 10.0 / 3.0


def _make_shp(path: str) -> None:
    """val N(12,2) = 10 + i, lbl 'L<i>', point (i, i), 3 features."""
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("n", srs, ogr.wkbPoint)
    fd = ogr.FieldDefn("val", ogr.OFTReal)
    fd.SetWidth(12)
    fd.SetPrecision(2)
    lyr.CreateField(fd)
    fd = ogr.FieldDefn("lbl", ogr.OFTString)
    fd.SetWidth(8)
    lyr.CreateField(fd)
    for i in range(3):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("val", 10.0 + i)
        feat.SetField("lbl", f"L{i}")
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(feat)
    ds = None


def _make_gpkg(path: str) -> None:
    from osgeo import ogr, osr
    ogr.UseExceptions()
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(path)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(2154)
    lyr = ds.CreateLayer("pts", srs, ogr.wkbPoint)
    lyr.CreateField(ogr.FieldDefn("lbl", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("val", ogr.OFTReal))
    for i in range(2):
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("lbl", f"B{i}")
        feat.SetField("val", 1.0 + i)
        feat.SetGeometry(ogr.CreateGeometryFromWkt(f"POINT ({i} {i})"))
        lyr.CreateFeature(feat)
    ds = None


def _field_calculator(layer, expression: str, field: str) -> int:
    """What the field calculator does: evaluate, then changeAttributeValue."""
    from qgis.core import QgsExpression, QgsExpressionContext, \
        QgsExpressionContextUtils
    expr = QgsExpression(expression)
    context = QgsExpressionContext(
        QgsExpressionContextUtils.globalProjectLayerScopes(layer))
    expr.prepare(context)
    idx = layer.fields().indexFromName(field)
    n = 0
    for feat in layer.getFeatures():
        context.setFeature(feat)
        layer.changeAttributeValue(feat.id(), idx, expr.evaluate(context))
        n += 1
    return n


def _fid_of(layer, lbl: str):
    return next((f.id() for f in layer.dataProvider().getFeatures()
                 if f["lbl"] == lbl), None)


def _bar_since(iface, start: int) -> list:
    from qgis.core import Qgis
    out = []
    for _title, text, level, duration in iface.bar.messages[start:]:
        out.append({"text": text, "critical": level == Qgis.MessageLevel.Critical,
                    "duration": duration})
    return out


def _gap_messages(bar, layer_name: str) -> list:
    """Messages that name the layer and say the NEW value is the one sent."""
    return [m for m in bar if layer_name in m["text"] and "envoy" in m["text"]]


def _journal_new_values(journal: str, layer_name: str) -> list:
    """NEW side of 'val' in the journal's UPDATEs of *layer_name*."""
    if not journal:
        return []
    from recoverland.core.long_paths import when_needed
    conn = sqlite3.connect(when_needed(journal))
    try:
        rows = conn.execute(
            "SELECT attributes_json FROM audit_event WHERE operation_type = "
            "'UPDATE' AND restored_from_event_id IS NULL "
            "AND layer_name_snapshot = ?", (layer_name,)).fetchall()
    finally:
        conn.close()
    out = []
    for (raw,) in rows:
        try:
            delta = (json.loads(raw or "{}") or {}).get("changed_only") or {}
        except ValueError:
            continue
        if "val" in delta:
            out.append((delta["val"] or {}).get("new"))
    return out


def _restore_fid0(journal: str, shp: str) -> dict:
    """Restore the M1 UPDATE of 'L0' (10 -> 10/3), plugin unloaded."""
    from qgis.core import QgsVectorLayer
    from recoverland.core.long_paths import when_needed
    from recoverland.core.restore_service import restore_updated_feature
    from recoverland.core.search_service import get_event_by_id
    if not journal:
        return {}
    conn = sqlite3.connect(when_needed(journal))
    try:
        eid = None
        for row_id, raw in conn.execute(
                "SELECT event_id, attributes_json FROM audit_event WHERE "
                "operation_type = 'UPDATE' AND layer_name_snapshot = ? "
                "ORDER BY event_id", (_LAYER_A,)).fetchall():
            delta = (json.loads(raw or "{}") or {}).get("changed_only") or {}
            if (delta.get("val") or {}).get("new") == _SENT_M1:
                eid = row_id
                break
        event = get_event_by_id(conn, eid) if eid is not None else None
    finally:
        conn.close()
    if event is None:
        return {"event": None}
    layer = QgsVectorLayer(shp, "m6_restore", "ogr")
    res = restore_updated_feature(layer, event)
    after = next((f["val"] for f in QgsVectorLayer(shp, "m6_after", "ogr").getFeatures()
                  if f["lbl"] == "L0"), None)
    return {"event": eid, "file_after": after,
            **{k: res.get(k) for k in ("success", "reason_code", "message")}}


def _emitted_statuses() -> dict:
    """kind -> 'file:line' for every _notify_status("kind", ...) in core/."""
    out = {}
    for path in sorted(_CORE_DIR.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "_notify_status"
                    and node.args and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)):
                out.setdefault(node.args[0].value,
                               f"{path.relative_to(_PLUGIN_ROOT).as_posix()}:{node.lineno}")
    return out


def _handled_statuses(source: Path) -> set:
    """Kinds compared with `kind == "..."` in RecoverPlugin._on_capture_status."""
    tree = ast.parse(source.read_text(encoding="utf-8"))
    handled = set()
    for cls in tree.body:
        if not (isinstance(cls, ast.ClassDef) and cls.name == "RecoverPlugin"):
            continue
        for fn in cls.body:
            if not (isinstance(fn, ast.FunctionDef) and fn.name == "_on_capture_status"):
                continue
            for node in ast.walk(fn):
                if (isinstance(node, ast.Compare)
                        and isinstance(node.left, ast.Name) and node.left.id == "kind"
                        and all(isinstance(op, ast.Eq) for op in node.ops)):
                    for comp in node.comparators:
                        if isinstance(comp, ast.Constant) and isinstance(comp.value, str):
                            handled.add(comp.value)
    return handled


def setup(ctx):
    from qgis.core import QgsProject, QgsSettings
    from recoverland.core.logger import flog
    QgsProject.instance().clear()
    QgsSettings().setValue("RecoverLand/tracking_enabled", True)
    QgsSettings().setValue("RecoverLand/tracking_disabled_by_disk", False)
    tmp = tempfile.mkdtemp(prefix="rl_q_upd_")
    ctx.data["tmp"] = tmp
    _make_shp(os.path.join(tmp, "a.shp"))
    _make_gpkg(os.path.join(tmp, "b.gpkg"))
    flog(f"{SCENARIO_ID} setup: trace_id={ctx.trace_id} tmp={tmp}", "INFO")


def run(ctx):
    from qgis.core import QgsGeometry, QgsProject, QgsVectorLayer
    from recoverland.core.edit_tracker import EditSessionTracker
    from recoverland.core.logger import flog
    from recoverland.recover import RecoverPlugin
    from scripts.validation.plugin_harness import FakeIface, pump

    d = ctx.data
    tmp = d["tmp"]
    try:
        d["s1_emitted"] = _emitted_statuses()
        d["s1_handled"] = sorted(_handled_statuses(_RECOVER_SOURCE))
    except Exception as exc:  # noqa: BLE001 - reported by the assertion
        d["s1_error"] = f"{type(exc).__name__}: {exc}"
    iface = FakeIface()
    plugin = RecoverPlugin(iface)
    had = "_read_back_updated" in EditSessionTracker.__dict__
    saved = EditSessionTracker.__dict__.get("_read_back_updated")
    try:
        plugin.initGui()
        pump(0.3)
        d["journal"] = plugin._journal.path if plugin._journal.is_open else None
        la = QgsVectorLayer(os.path.join(tmp, "a.shp"), _LAYER_A, "ogr")
        lb = QgsVectorLayer(f"{os.path.join(tmp, 'b.gpkg')}|layername=pts", _LAYER_B, "ogr")
        QgsProject.instance().addMapLayers([la, lb])
        pump(0.2)
        d["health_before"] = plugin._capture_health().state
        d["m1_fids"] = sorted(f.id() for f in la.dataProvider().getFeatures())
        # The provider does not return the changed features on read-back.
        EditSessionTracker._read_back_updated = lambda self, lyr, fids, **kw: {}
        start = len(iface.bar.messages)
        la.startEditing()
        d["m1_edited"] = _field_calculator(la, '"val" / 3', "val")
        d["m1_commit"] = bool(la.commitChanges())
        pump(0.3)
        d["m1_bar"] = _bar_since(iface, start)
        # One commit, the value AND the geometry of one feature.
        fid_l1 = _fid_of(la, "L1")
        d["m2_fid"] = fid_l1
        start = len(iface.bar.messages)
        la.startEditing()
        la.changeAttributeValue(fid_l1, la.fields().indexFromName("val"), 99.5)
        la.changeGeometry(fid_l1, QgsGeometry.fromWkt("POINT (7 8)"))
        d["m2_commit"] = bool(la.commitChanges())
        pump(0.3)
        d["m2_bar"] = _bar_since(iface, start)
        fid_b0 = _fid_of(lb, "B0")
        d["m3_fid"] = fid_b0
        start = len(iface.bar.messages)
        lb.startEditing()
        lb.changeAttributeValue(fid_b0, lb.fields().indexFromName("val"), 12.25)
        d["m3_commit"] = bool(lb.commitChanges())
        pump(0.3)
        d["m3_bar"] = _bar_since(iface, start)
        health = plugin._capture_health()
        d["m4_state"] = health.state
        d["m4_reasons"] = list(health.reasons)
        plugin._update_status_bar()
        ind = plugin._status_indicator
        d["m4_tooltip"] = ind.toolTip() if ind is not None else None
        # Read-back back to normal: a verified change says nothing.
        if had:
            EditSessionTracker._read_back_updated = saved
        else:
            del EditSessionTracker._read_back_updated
        had = None
        fid_b1 = _fid_of(lb, "B1")
        start = len(iface.bar.messages)
        lb.startEditing()
        lb.changeAttributeValue(fid_b1, lb.fields().indexFromName("val"), 21.5)
        d["m5_commit"] = bool(lb.commitChanges())
        pump(0.3)
        d["m5_bar"] = _bar_since(iface, start)
        plugin._write_queue.wait_until_drained(5.0)
        d["m6_journal_new"] = _journal_new_values(d["journal"], _LAYER_A)
        d["m6_file_fid0"] = next((f["val"] for f in la.dataProvider().getFeatures()
                                  if f["lbl"] == "L0"), None)
        # A status the plugin has no branch for (a later tracker version).
        start = len(iface.bar.messages)
        plugin._on_capture_status(_FUTURE_KIND, {
            "layer_id": lb.id(), "layer_name": _LAYER_B, "commit_id": "c-futur"})
        pump(0.1)
        d["m7_bar"] = _bar_since(iface, start)
        # File > New: the next project's health is about its own journal.
        QgsProject.instance().clear()
        pump(0.4)
        d["m8_reasons"] = list(plugin._capture_health().reasons)
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        if had is not None:
            if had:
                EditSessionTracker._read_back_updated = saved
            elif "_read_back_updated" in EditSessionTracker.__dict__:
                del EditSessionTracker._read_back_updated
        try:
            plugin.unload()
        except Exception as exc:  # noqa: BLE001
            d["unload_error"] = repr(exc)
        QgsProject.instance().clear()
        pump(0.2)
    # What the alert announces, measured (recorded, not asserted: the alert
    # says "peut etre refuse", and the restore engine is another area's).
    try:
        d["m6_restore"] = _restore_fid0(d.get("journal"), os.path.join(tmp, "a.shp"))
    except Exception as exc:  # noqa: BLE001 - recorded only
        d["m6_restore"] = f"{type(exc).__name__}: {exc}"
    flog(f"{SCENARIO_ID} run: trace_id={ctx.trace_id} data="
         f"{json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:6000]}",
         "INFO")


def teardown(ctx):
    tmp = ctx.data.get("tmp")
    if tmp:
        shutil.rmtree(tmp, ignore_errors=True)


def _names_fids(text: str, fids) -> bool:
    """The FIDs, as the list the alert gives them ("FID 0, 1, 2")."""
    return bool(fids) and (
        "FID " + ", ".join(str(fid) for fid in sorted(fids))) in text


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = []
    crash = f"crash={d.get('crash')} {d.get('trace', '')}"
    out.append((
        "fixture_commits_landed",
        not d.get("crash") and d.get("m1_edited") == 3
        and all(d.get(k) for k in ("m1_commit", "m2_commit", "m3_commit", "m5_commit"))
        and d.get("m2_fid") is not None and d.get("m3_fid") is not None,
        f"commits={[d.get(k) for k in ('m1_commit', 'm2_commit', 'm3_commit', 'm5_commit')]} "
        f"edites={d.get('m1_edited')} {crash}",
    ))
    news = [v for v in d.get("m6_journal_new") or [] if isinstance(v, (int, float))]
    out.append((
        "m6_the_gap_is_real",
        any(abs(v - _SENT_M1) < 1e-12 for v in news)
        and d.get("m6_file_fid0") is not None
        and abs(float(d["m6_file_fid0"]) - 3.33) < 1e-9,
        f"cote NEW journalise de 'val' sur {_LAYER_A}={d.get('m6_journal_new')}, "
        f"fichier={d.get('m6_file_fid0')}. Sans cet ecart reel (valeur envoyee "
        f"3.3333... journalisee, 3.33 stockee), les alertes ci-dessous ne "
        f"prouveraient rien",
    ))
    fids1 = d.get("m1_fids") or []
    g1 = [m for m in _gap_messages(d.get("m1_bar") or [], _LAYER_A)
          if m["critical"] and m["duration"] == 0 and _names_fids(m["text"], fids1)
          and "val" in m["text"]
          and ("Restore" in m["text"] or "Rewind" in m["text"])]
    out.append((
        "m1_gap_alert_critical_persistent_names_layer_fids_field_and_cost",
        bool(g1),
        f"barre apres un calcul de champ sans relecture={d.get('m1_bar')} "
        f"(FID {fids1}, champ val). Sans alerte critique qui reste affichee, "
        f"nomme la couche, les entites et le champ, dit que la valeur "
        f"journalisee est celle envoyee et ce que cela coute, l'utilisateur "
        f"decouvre le trou quand un Restore ou un Rewind est refuse",
    ))
    fid2 = d.get("m2_fid")
    bar2 = _gap_messages(d.get("m2_bar") or [], _LAYER_A)
    attr2 = [m for m in bar2 if m["critical"] and _names_fids(m["text"], [fid2])
             and "val" in m["text"]]
    geom2 = [m for m in bar2 if m["critical"] and _names_fids(m["text"], [fid2])
             and "geometrie" in m["text"]]
    out.append((
        "m2_attribute_and_geometry_gaps_of_one_commit_both_shown",
        bool(attr2) and bool(geom2),
        f"barre apres un commit valeur + geometrie sans relecture={d.get('m2_bar')} "
        f"(FID {fid2}). Une alerte etouffee par celle du meme commit laisse "
        f"la geometrie non verifiee sans nom",
    ))
    g3 = [m for m in _gap_messages(d.get("m3_bar") or [], _LAYER_B)
          if m["critical"] and _names_fids(m["text"], [d.get("m3_fid")])]
    out.append((
        "m3_other_layer_named",
        bool(g3),
        f"barre apres une modification sans relecture sur {_LAYER_B}={d.get('m3_bar')}",
    ))
    reasons = d.get("m4_reasons") or []
    gap_reason = [r for r in reasons if "envoy" in r and r.startswith("5 ")
                  and _LAYER_A in r and _LAYER_B in r and "val" in r
                  and "geometrie" in r]
    out.append((
        "m4_capture_health_shows_unverified_updates",
        d.get("m4_state") not in ("PROTECTED", "PERSISTING", None) and bool(gap_reason),
        f"etat={d.get('m4_state')} (avant : {d.get('health_before')}) "
        f"raisons={reasons}. Attendu : une raison '5 modification(s)' (3 + 1 + 1, "
        f"les deux statuts de M2 visent la meme entite du meme commit) nommant "
        f"les deux couches, val et la geometrie. Un indicateur vert 'protege' "
        f"au-dessus de modifications journalisees avec une valeur non relue "
        f"dit le contraire des faits",
    ))
    tip = d.get("m4_tooltip") or ""
    out.append((
        "m4_indicator_tooltip_shows_it",
        bool(gap_reason) and gap_reason[0] in tip,
        f"infobulle={tip!r}",
    ))
    g5 = _gap_messages(d.get("m5_bar") or [], _LAYER_B)
    out.append((
        "m5_verified_update_raises_no_gap_alert",
        d.get("m5_commit") is True and not g5,
        f"barre apres une modification relue normalement={d.get('m5_bar')} : une "
        f"fausse alerte apprend a ignorer les vraies",
    ))
    g7 = [m for m in d.get("m7_bar") or [] if _FUTURE_KIND in m["text"]
          and _LAYER_B in m["text"]]
    out.append((
        "m7_unknown_status_still_said",
        bool(g7),
        f"barre apres un statut du suivi que le plugin ne connait pas="
        f"{d.get('m7_bar')}. Sans branche, ce statut finissait dans le seul "
        f"journal technique : le prochain statut ajoute au suivi serait tu",
    ))
    g8 = [r for r in d.get("m8_reasons") or [] if _LAYER_A in r or _LAYER_B in r]
    out.append((
        "m8_next_project_not_blamed",
        "m8_reasons" in d and not g8,
        f"raisons apres Fichier > Nouveau={d.get('m8_reasons')} : le projet "
        f"suivant ne porte pas le trou d'un autre journal",
    ))
    emitted = d.get("s1_emitted") or {}
    handled = set(d.get("s1_handled") or [])
    missing = {k: v for k, v in emitted.items() if k not in handled}
    out.append((
        "s1_every_tracker_status_has_a_plugin_branch",
        not d.get("s1_error") and "update_values_unverified" in emitted and not missing,
        f"statuts emis sans branche dans RecoverPlugin._on_capture_status="
        f"{missing} (source {_RECOVER_SOURCE.name}) {d.get('s1_error') or ''}. "
        f"Un statut sans branche n'atteint que le journal technique",
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
