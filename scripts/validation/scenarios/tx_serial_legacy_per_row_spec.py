# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_serial_legacy_per_row_spec - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (une valeur restauree est IDENTIQUE a la valeur
capturee ; jamais de decalage silencieux, jamais de succes annonce sur une
valeur alteree).

Le trou que ce scenario prouve
------------------------------
Un evenement <= 5.x a journalise une date-heure sans son type de fuseau :
``value.toString('yyyy-MM-ddTHH:mm:ss')`` de la valeur que le provider
rendait. Le correctif precedent retrouvait ce type en mesurant les valeurs
vivantes DU CHAMP, sur un echantillon de 64 lignes non nulles. Deux
erreurs, mesurees sous QGIS 4.2.1 :

  * sur une GeoPackage, et sur tout provider qui stocke une date-heure
    comme un texte par ligne (SQLite OGR, GeoJSON, FlatGeobuf, SpatiaLite,
    memoire), le type de fuseau appartient a CHAQUE valeur stockee, pas au
    champ. "Exporter / Enregistrer sous" (QgsVectorFileWriter) ecrit un
    texte naif (``13:30:00``, relu en heure locale) ; une saisie validee
    par le provider ecrit ``13:30:00Z`` (relue en UTC). Une GeoPackage
    exportee puis editee melange les deux. Or la ligne supprimee, ou
    l'ancienne valeur d'un UPDATE, n'est plus stockee : les lignes
    restantes ne disent rien de SA forme ;
  * l'echantillon borne ne voyait pas le melange quand les 64 premieres
    lignes partageaient une forme.

Consequence utilisateur : GeoPackage exportee (lignes naives), une entite
ajoutee dans QGIS a 14:30 puis supprimee par erreur sous 5.x (journalise
``13:30:00``, les chiffres UTC que la GeoPackage rendait). Restauree en
v7 : les lignes restantes sont toutes naives, le champ est mesure "heure
locale", l'entite revient a 12:30Z, une heure trop tot (deux en ete), et
la restauration annonce "Restored".

Comment le scenario le prouve : par MONDES JUMEAUX. Deux couches dont les
lignes restantes sont identiques (memes formes stockees, memes valeurs) et
dont l'evenement 5.x porte le MEME texte, mais dont la ligne supprimee
avait l'autre forme : une saisie QGIS (``13:30:00Z``) dans l'un, une ligne
exportee (``13:30:00`` naive) dans l'autre. Aucune regle ne peut
restaurer les deux exactement : ses entrees sont les memes, les verites
different d'une heure. Chaque monde exige donc "restaure exact, ou refuse
en nommant le champ sans rien ecrire" ; seul le refus satisfait les deux.

  * B / B' : 10 lignes exportees ; la ligne supprimee est la seule saisie
    (B) ou une ligne exportee de plus (B') ;
  * C / C' : 2 saisies ; la ligne supprimee est une saisie (C) ou la
    seule ligne exportee (C') ;
  * D / D' : SQLite OGR (le type ecrit est garde), ancienne valeur d'un
    UPDATE 5.x : saisie locale (D) ou ligne importee en UTC (D') ;
  * A : 70 lignes exportees puis 2 saisies, une saisie supprimee : le
    melange est au-dela des 64 premieres valeurs ;
  * E / F : provider a TYPE DE COLONNE (PostgreSQL : le type de la
    colonne et le fuseau de la session fixent le fuseau rendu, jamais la
    ligne). Simule par une couche memoire presentee sous le nom
    ``postgres`` (dispositif de rw_composite_pk_identity) : c'est la regle
    de mesure qui est prouvee, pas le serveur. E (valeurs concordantes)
    doit etre restaure exact : c'est le temoin qui interdit de tout
    refuser. F (70 valeurs locales puis 2 UTC, une UTC supprimee) doit
    voir le melange au-dela de 64 et refuser.

Precondition : decalage UTC non nul le 15 janvier dans le fuseau du poste
(verifie, echec bruyant sinon).

Verdict attendu avant correctif (worktree v7 avant ce correctif) : FAIL
(A, B, C', D', F restaures decales et annonces reussis).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_serial_legacy_per_row_spec"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = "NoGeometry?field=label:string(20)&field=dtm:datetime"


def _qdt(h, mi, utc=False, day=15):
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    if utc:
        return QDateTime(QDate(2026, 1, day), QTime(h, mi),
                         QTimeZone(QTimeZone.Initialization.UTC))
    return QDateTime(QDate(2026, 1, day), QTime(h, mi))


def _memory(rows):
    from qgis.core import QgsVectorLayer, QgsFeature
    mem = QgsVectorLayer(_FIELDS, "seed", "memory")
    feats = []
    for label, value in rows:
        feat = QgsFeature(mem.fields())
        feat.setAttribute("label", label)
        feat.setAttribute("dtm", value)
        feats.append(feat)
    if feats and not mem.dataProvider().addFeatures(feats)[0]:
        raise RuntimeError("seed memoire refuse")
    return mem


def _export(tmpdir, name, driver, rows):
    """Ce que produit "Exporter / Enregistrer sous" (QgsVectorFileWriter)."""
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
    )
    path = os.path.join(tmpdir, name)
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = driver
    opts.layerName = "t"
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        _memory(rows), path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"{driver} export failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername=t", "t", "ogr")
    if not layer.isValid():
        raise RuntimeError(f"{name}: couche invalide")
    return path, layer


def _type_in(layer, rows):
    """Saisies de l'utilisateur, validees par le provider."""
    from qgis.core import QgsFeature
    feats = []
    for label, value in rows:
        feat = QgsFeature(layer.fields())
        feat.setAttribute("label", label)
        feat.setAttribute("dtm", value)
        feats.append(feat)
    if not layer.dataProvider().addFeatures(feats)[0]:
        raise RuntimeError("saisie refusee")
    layer.reload()


def _import_utc(path, label, h, mi):
    """Une ligne ecrite en UTC par un autre outil (OGR, TZFlag 100)."""
    from osgeo import ogr
    ogr.UseExceptions()
    ds = ogr.Open(path, 1)
    lyr = ds.GetLayerByName("t")
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("label", label)
    feat.SetField("dtm", 2026, 1, 15, h, mi, 0, 100)
    lyr.CreateFeature(feat)
    feat = None
    ds = None


def _find(layer, label):
    for feat in layer.dataProvider().getFeatures():
        if feat["label"] == label:
            return feat
    return None


def _forms(path, exclude=()):
    """Formes stockees du champ, lignes *exclude* omises : (naives, Z)."""
    con = sqlite3.connect(path)
    try:
        rows = con.execute("SELECT label, dtm FROM t").fetchall()
    finally:
        con.close()
    kept = [str(text) for label, text in rows
            if label not in exclude and text is not None]
    return (sorted(t for t in kept if not t.endswith("Z")),
            sorted(t for t in kept if t.endswith("Z")))


def _event(layer, op, attrs_json, fid, provider="ogr"):
    from recoverland.core.audit_backend import AuditEvent
    from recoverland.core.serialization import serialize_field_schema
    return AuditEvent(
        event_id=fid + 900, project_fingerprint="p",
        datasource_fingerprint="ds", layer_id_snapshot=layer.id(),
        layer_name_snapshot="t", provider_type=provider,
        feature_identity_json=json.dumps({"fid": fid}),
        operation_type=op, attributes_json=attrs_json,
        geometry_wkb=None, geometry_type="NoGeometry", crs_authid="",
        field_schema_json=serialize_field_schema(layer.fields()),
        user_name="t", session_id="s", created_at="2026-02-01T10:00:00",
        restored_from_event_id=None, entity_fingerprint=f"fid:{fid}",
        event_schema_version=5, new_geometry_wkb=None,
    )


def _safe(fn, *args):
    try:
        res = fn(*args)
    except Exception as exc:  # noqa: BLE001 - measured, not hidden
        return {"exception": repr(exc)}
    return {
        k: res.get(k)
        for k in ("success", "reason_code", "message", "refused_fields", "skipped")
    }


def _delete_world(layer, victim_label, provider="ogr", reader=None,
                  path=None):
    """Supprime *victim_label*, puis restaure le DELETE journalise par 5.x.

    Les formes stockees restantes (*path*) sont relevees entre la
    suppression et la restauration : ce que la restauration peut lire.
    """
    from recoverland.core.restore_service import restore_deleted_feature
    from recoverland.core.serialization import build_full_snapshot
    reader = reader or layer
    victim = _find(reader, victim_label)
    value = victim["dtm"]
    out = {"v5_text": value.toString("yyyy-MM-ddTHH:mm:ss"),
           "truth_ms": value.toMSecsSinceEpoch(),
           "victim_spec": value.timeSpec().name}
    reader.dataProvider().deleteFeatures([victim.id()])
    reader.reload()
    if path is not None:
        out["remaining_forms"] = _forms(path)
    event = _event(layer, "DELETE", build_full_snapshot(
        {"label": victim_label, "dtm": out["v5_text"]}), victim.id(), provider)
    out["result"] = _safe(restore_deleted_feature, layer, event)
    reader.reload()
    back = [f["dtm"] for f in reader.dataProvider().getFeatures()
            if f["label"] == victim_label]
    out["written"] = [v.toMSecsSinceEpoch() for v in back
                      if hasattr(v, "toMSecsSinceEpoch")]
    return out


def _update_world(path, layer, label, old_value_writer):
    """Ancienne valeur d'un UPDATE 5.x (dtm -> 16:00 locales) a restaurer."""
    from recoverland.core.restore_service import restore_updated_feature
    old_value_writer()
    layer.reload()
    feat = _find(layer, label)
    value = feat["dtm"]
    out = {"v5_text": value.toString("yyyy-MM-ddTHH:mm:ss"),
           "truth_ms": value.toMSecsSinceEpoch(),
           "victim_spec": value.timeSpec().name}
    idx = layer.fields().indexOf("dtm")
    layer.dataProvider().changeAttributeValues({feat.id(): {idx: _qdt(16, 0)}})
    layer.reload()
    out["after_edit_ms"] = _find(layer, label)["dtm"].toMSecsSinceEpoch()
    out["remaining_forms"] = _forms(path)
    delta = json.dumps({"changed_only": {"dtm": {
        "old": out["v5_text"], "new": "2026-01-15T16:00:00"}}})
    out["result"] = _safe(restore_updated_feature, layer,
                          _event(layer, "UPDATE", delta, feat.id()))
    layer.reload()
    out["written"] = [_find(layer, label)["dtm"].toMSecsSinceEpoch()]
    return out


def _phase_gpkg(d, tmpdir):
    naive10 = [(f"exporte{i}", _qdt(10, i)) for i in range(10)]
    typed = [(f"saisie{i}", _qdt(14, 30)) for i in range(3)]
    worlds = {}
    # B : 10 exportees + LA saisie, supprimee.
    path, layer = _export(tmpdir, "b.gpkg", "GPKG", naive10)
    _type_in(layer, typed[:1])
    worlds["B"] = _delete_world(layer, "saisie0", path=path)
    # B' : 10 exportees + une exportee de plus (13:30 locales), supprimee.
    path, layer = _export(tmpdir, "b2.gpkg", "GPKG",
                          naive10 + [("saisie0", _qdt(13, 30))])
    worlds["B'"] = _delete_world(layer, "saisie0", path=path)
    # C : 3 saisies, une supprimee.
    path, layer = _export(tmpdir, "c.gpkg", "GPKG", [])
    _type_in(layer, typed)
    worlds["C"] = _delete_world(layer, "saisie0", path=path)
    # C' : la seule ligne exportee (13:30 locales) + 2 saisies ; l'exportee
    # est supprimee.
    path, layer = _export(tmpdir, "c2.gpkg", "GPKG",
                          [("saisie0", _qdt(13, 30))])
    _type_in(layer, typed[1:])
    worlds["C'"] = _delete_world(layer, "saisie0", path=path)
    # A : 70 exportees puis 2 saisies ; une saisie supprimee.
    path, layer = _export(tmpdir, "a.gpkg", "GPKG",
                          [(f"exporte{i}", _qdt(8 + i // 60, i % 60))
                           for i in range(70)])
    _type_in(layer, typed[:2])
    worlds["A"] = _delete_world(layer, "saisie0", path=path)
    d["worlds"].update(worlds)


def _phase_sqlite(d, tmpdir):
    worlds = {}
    # D : l'ancienne valeur est une saisie locale (13:30, stockee naive).
    path, layer = _export(tmpdir, "d.sqlite", "SQLite", [])
    _type_in(layer, [("voisin", _qdt(9, 0))])
    worlds["D"] = _update_world(path, layer, "cible", lambda: _type_in(
        layer, [("cible", _qdt(13, 30))]))
    # D' : l'ancienne valeur a ete importee en UTC (13:30Z) par un autre
    # outil, avant toute ecriture de QGIS ; 5.x la lisait en UTC, meme
    # texte que D.
    path, layer = _export(tmpdir, "d2.sqlite", "SQLite", [])
    _import_utc(path, "cible", 13, 30)
    layer.reload()
    _type_in(layer, [("voisin", _qdt(9, 0))])
    worlds["D'"] = _update_world(path, layer, "cible", lambda: None)
    d["worlds"].update(worlds)


def _phase_column_typed(d):
    from scripts.validation.scenarios.rw_composite_pk_identity import PkLayer
    worlds = {}
    base = _memory([(f"local{i}", _qdt(10, i)) for i in range(5)])
    worlds["E"] = _delete_world(PkLayer(base, pk_indexes=()), "local2",
                                provider="postgres", reader=base)
    base = _memory([(f"local{i}", _qdt(8 + i // 60, i % 60))
                    for i in range(70)]
                   + [(f"utc{i}", _qdt(13, 30, utc=True)) for i in range(2)])
    worlds["F"] = _delete_world(PkLayer(base, pk_indexes=()), "utc0",
                                provider="postgres", reader=base)
    d["worlds"].update(worlds)


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_legacy_per_row_spec setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from qgis.PyQt.QtCore import QTimeZone
    from recoverland.core.logger import flog
    d = ctx.data
    d["error"] = None
    d["zone"] = bytes(QTimeZone.systemTimeZoneId()).decode("ascii", "replace")
    d["offset_s"] = int(_qdt(14, 30).offsetFromUtc())
    d["worlds"] = {}
    tmpdir = tempfile.mkdtemp(prefix="rl_legrow_")
    try:
        for phase, args in ((_phase_gpkg, (d, tmpdir)),
                            (_phase_sqlite, (d, tmpdir)),
                            (_phase_column_typed, (d,))):
            try:
                phase(*args)
            except Exception as exc:  # noqa: BLE001
                d.setdefault("phase_errors", {})[phase.__name__] = repr(exc)
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    flog(f"tx_serial_legacy_per_row_spec run: trace_id={ctx.trace_id} "
         f"zone={d.get('zone')} worlds={json.dumps(d.get('worlds'), default=str)[:3000]} "
         f"phase_errors={d.get('phase_errors')} error={d.get('error')}",
         "INFO" if d.get("error") is None and not d.get("phase_errors")
         else "ERROR")


def _refused_by_name(world):
    res = world.get("result") or {}
    return (res.get("success") is not True
            and res.get("reason_code") == "legacy_datetime_refused"
            and "dtm" in (res.get("refused_fields") or {}))


def _outcome(world, update=False):
    """'exact', 'refused' (nomme, rien ecrit) ou 'wrong'."""
    res = world.get("result") or {}
    written = world.get("written") or []
    truth = world.get("truth_ms")
    if res.get("success") is True and written == [truth]:
        return "exact"
    untouched = (written == [world.get("after_edit_ms")] if update
                 else written == [])
    if _refused_by_name(world) and untouched:
        return "refused"
    return "wrong"


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    worlds = d.get("worlds") or {}
    out = [
        ("scenario_ran", d.get("error") is None and not d.get("phase_errors"),
         f"le scenario a plante : {d.get('error')} {d.get('phase_errors')}"),
        ("precondition_zone_has_an_offset", d.get("offset_s") not in (None, 0),
         f"fuseau du poste {d.get('zone')!r}, decalage UTC le 15 janvier="
         f"{d.get('offset_s')!r} s : a decalage nul, heure locale et UTC "
         f"coincident et ce scenario ne prouve rien."),
    ]
    for twin, other in (("B", "B'"), ("C", "C'"), ("D", "D'")):
        a, b = worlds.get(twin) or {}, worlds.get(other) or {}
        out.append((
            f"twin_worlds_{twin}_{other}_are_indistinguishable",
            bool(a) and bool(b) and a.get("v5_text") == b.get("v5_text")
            and a.get("remaining_forms") == b.get("remaining_forms")
            and a.get("truth_ms") != b.get("truth_ms"),
            f"dispositif : {twin} texte 5.x={a.get('v5_text')!r} formes "
            f"restantes={a.get('remaining_forms')} ligne supprimee de type "
            f"{a.get('victim_spec')!r} ; {other} texte={b.get('v5_text')!r} "
            f"formes={b.get('remaining_forms')} type {b.get('victim_spec')!r}. "
            f"Memes entrees, verites differentes : sinon le scenario ne "
            f"prouve pas que la forme d'une ligne disparue est indecidable."))
    naive, zulu = (worlds.get("A") or {}).get("remaining_forms") or ([], [])
    out.append((
        "fixture_A_mix_beyond_the_first_64",
        len(naive) == 70 and len(zulu) == 1,
        f"dispositif A : {len(naive)} textes naifs (exportes) et {len(zulu)} "
        f"texte(s) 'Z' (saisie) restent ; attendu 70 et 1."))
    for name, story in (
            ("A", "70 lignes exportees puis 2 saisies, une saisie supprimee"),
            ("B", "10 lignes exportees, la seule saisie supprimee"),
            ("B'", "10 lignes exportees, une exportee supprimee"),
            ("C", "saisies seules, une supprimee"),
            ("C'", "2 saisies, la seule ligne exportee supprimee"),
            ("D", "SQLite OGR, ancienne valeur saisie en heure locale"),
            ("D'", "SQLite OGR, ancienne valeur importee en UTC"),
            ("F", "colonne typee, melange au-dela des 64 premieres valeurs")):
        world = worlds.get(name) or {}
        outcome = _outcome(world, update=name.startswith("D"))
        out.append((
            f"world_{name}_never_written_shifted",
            outcome in ("exact", "refused"),
            f"{story} : texte 5.x {world.get('v5_text')!r}, resultat="
            f"{world.get('result')}, ecrit={world.get('written')} (instant "
            f"capture {world.get('truth_ms')}) -> {outcome}. Ecrire un "
            f"instant decale de l'offset UTC en annoncant le succes est une "
            f"donnee faussee que rien ne signalera : restaurer exact, ou "
            f"refuser en nommant le champ sans rien ecrire."))
    e_world = worlds.get("E") or {}
    out.append((
        "column_typed_uniform_restored_exact",
        _outcome(e_world) == "exact",
        f"temoin : provider a type de colonne, valeurs concordantes, texte "
        f"5.x {e_world.get('v5_text')!r} -> resultat={e_world.get('result')} "
        f"ecrit={e_world.get('written')} attendu [{e_world.get('truth_ms')}]. "
        f"La ou le fuseau est etabli, la date doit revenir : tout refuser "
        f"n'est pas une correction."))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_legacy_per_row_spec run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
