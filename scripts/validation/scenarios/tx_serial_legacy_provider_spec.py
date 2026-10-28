# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_serial_legacy_provider_spec - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (une valeur restauree est IDENTIQUE a la valeur
capturee ; jamais de decalage silencieux).

Le trou que ce scenario prouve
------------------------------
Jusqu'au schema 5 (plugin <= 5.0.x), une date-heure etait journalisee par
``value.toString('yyyy-MM-ddTHH:mm:ss')`` de la valeur TELLE QUE LE
PROVIDER LA RENDAIT : les chiffres sont ceux du type de fuseau de cette
valeur, que le texte ne dit pas. Mesure sous QGIS 4.2.1 (fuseau du poste) :

  * GeoPackage : QGIS convertit en UTC ce qu'il ecrit (14:30 locales ->
    ``13:30:00Z`` sur disque) et le relit en UTC ; un texte sans suffixe
    ecrit par un autre outil est relu en heure locale ;
  * memoire, GeoJSON, FlatGeobuf, SQLite (OGR), SpatiaLite : le type donne
    a l'ecriture est garde (une heure locale reste locale) ;
  * Shapefile : pas de date-heure, un texte (aucun decodage).

Le correctif precedent lisait TOUT chiffre 5.x comme une heure locale :
sur une GeoPackage, un DELETE 5.x de 14:30 locales (journalise
``13:30:00``) revenait a 12:30Z, decale d'une heure en hiver, deux en ete,
et, une fois les instants compares correctement, annonce reussi.

Ce que le correctif doit garantir, mesure sur de vraies couches :

  * le texte 5.x est relu dans le type de fuseau que le provider rend pour
    ce champ (mesure sur ses valeurs vivantes) : l'instant restaure est
    l'instant capture, sur chaque provider ;
  * quand ce type ne peut pas etre etabli (aucune valeur vivante, types
    melanges, valeur saisie relue par une GeoPackage qui l'a convertie),
    le champ est REFUSE et nomme, jamais devine ;
  * un appelant qui ne fournit pas le type du provider n'ecrit rien : le
    decodage leve une erreur nommee au lieu d'ecrire une valeur decalee ;
  * le comparateur des preuves reconnait la valeur exacte et rejette la
    valeur decalee de l'offset.

Precondition : les dates d'essai tombent a un decalage UTC non nul dans le
fuseau du poste (verifiee, echec bruyant sinon).

Verdict attendu avant correctif (worktree v7 avant ce correctif) : FAIL
(GeoPackage restauree decalee, aucun refus nomme).

Hypothesis revised (v7), second tour : "le texte 5.x est relu dans le type
de fuseau que le provider rend pour ce champ, mesure sur ses valeurs
vivantes" supposait que ce type appartient au CHAMP. Mesure par les
mondes jumeaux de `tx_serial_legacy_per_row_spec` : sur GeoPackage,
GeoJSON, SQLite (OGR), SpatiaLite et memoire il appartient a CHAQUE valeur
stockee (une GeoPackage exportee stocke un texte naif, une saisie QGIS un
texte ``Z`` ; les autres gardent le type de chaque ecriture). Les memes
lignes restantes et le meme texte 5.x viennent d'une ligne supprimee de
l'une ou l'autre forme, a une heure d'ecart : aucune lecture ne peut
etre juste dans les deux cas. Sur ces cinq providers, l'assertion
"restaure a l'instant capture" devient donc "jamais ecrit decale" :
restaure exact, ou champ refuse et nomme, rien ecrit dans ce champ. Le
temoin "valeur saisie resolue sur GeoJSON" devient un refus (le type
d'une saisie n'est journalise nulle part). La restauration exacte reste
exigee la ou le type appartient a la colonne : un provider PostgreSQL,
simule par une couche memoire presentee sous le nom ``postgres``
(``colonne_postgres``), et un texte v6 n'est toujours jamais refuse.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_serial_legacy_provider_spec"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_FIELDS = "field=label:string(20)&field=dtm:datetime"
# Ce que l'utilisateur a saisi (heure murale locale), par ligne.
_ROWS = (("a", (2026, 1, 15, 14, 30)), ("b", (2026, 7, 15, 9, 0)))


def _local(y, mo, d, h, mi):
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime
    return QDateTime(QDate(y, mo, d), QTime(h, mi))


def _seed():
    from qgis.core import QgsVectorLayer
    return QgsVectorLayer("Point?crs=EPSG:4326&" + _FIELDS, "seed", "memory")


def _write(tmpdir, driver, name, layer_name=None):
    from qgis.core import (
        QgsVectorFileWriter, QgsCoordinateTransformContext, QgsFeature,
        QgsGeometry, QgsPointXY,
    )
    mem = _seed()
    # GeoJSON perd ses champs sans une entite : une ligne sacrificielle.
    feat = QgsFeature(mem.fields())
    feat.setAttribute("label", "seed")
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(0.0, 0.0)))
    mem.dataProvider().addFeatures([feat])
    path = os.path.join(tmpdir, name)
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = driver
    if layer_name:
        opts.layerName = layer_name
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"{driver} write failed: {err}")
    return path


def _open(kind, tmpdir):
    from qgis.core import QgsVectorLayer, QgsDataSourceUri
    if kind == "memory":
        return _seed()
    if kind == "colonne_postgres":
        from scripts.validation.scenarios.rw_composite_pk_identity import (
            PkLayer,
        )
        return PkLayer(_seed(), pk_indexes=(), name="postgres")
    if kind == "gpkg":
        path = _write(tmpdir, "GPKG", "t.gpkg", "t")
        return QgsVectorLayer(f"{path}|layername=t", "t", "ogr")
    if kind == "geojson":
        path = _write(tmpdir, "GeoJSON", "t.geojson")
        return QgsVectorLayer(path, "t", "ogr")
    if kind == "sqlite_ogr":
        path = _write(tmpdir, "SQLite", "t.sqlite", "t")
        return QgsVectorLayer(f"{path}|layername=t", "t", "ogr")
    if kind == "spatialite":
        path = _write(tmpdir, "SpatiaLite", "sl.sqlite", "t")
        uri = QgsDataSourceUri()
        uri.setDatabase(path)
        uri.setDataSource("", "t", "GEOMETRY")
        return QgsVectorLayer(uri.uri(), "t", "spatialite")
    raise ValueError(kind)


def _populate(layer):
    """Les saisies de l'utilisateur, ecrites par le provider (commit QGIS)."""
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    provider = layer.dataProvider()
    seeds = [f.id() for f in provider.getFeatures() if f["label"] == "seed"]
    if seeds:
        provider.deleteFeatures(seeds)
    feats = []
    for label, parts in _ROWS:
        feat = QgsFeature(layer.fields())
        feat.setAttribute("label", label)
        feat.setAttribute("dtm", _local(*parts))
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.35, 48.85)))
        feats.append(feat)
    ok, _ = provider.addFeatures(feats)
    if not ok:
        raise RuntimeError(f"addFeatures refused: {provider.errors()[:2]}")
    layer.reload()


def _live(layer, label):
    for feat in layer.dataProvider().getFeatures():
        if feat["label"] == label:
            return feat
    return None


def _v5_text(value):
    """Exactement ce que 5.x ecrivait dans le journal pour cette valeur."""
    return value.toString("yyyy-MM-ddTHH:mm:ss")


def _iter_with_version(ser, mapping, attrs, fields, version):
    import inspect
    params = inspect.signature(ser.iter_mapped_attributes).parameters
    if "event_schema_version" not in params:
        # Code 6.x : pas de version, tout texte sans suffixe est lu en UTC.
        return dict(ser.iter_mapped_attributes(mapping, attrs, fields))
    return dict(ser.iter_mapped_attributes(mapping, attrs, fields, version))


def _decode(ser, layer, attrs, version, wired):
    """``(valeurs decodees par index, {champ refuse: raison})``."""
    mapping = {name: name for name in attrs}
    fields = layer.fields()
    if wired:
        factory = getattr(ser, "provider_time_specs", None)
        if factory is None:
            # Code sans lecture du fuseau du provider : il decode comme il
            # l'a toujours fait, et c'est precisement ce qui est mesure.
            return _iter_with_version(ser, mapping, attrs, fields,
                                      version), {}
        specs = factory(layer)
        refused = ser.legacy_datetime_refusals(mapping, attrs, fields,
                                               version, specs)
        mapping = {h: c for h, c in mapping.items() if c not in refused}
        return dict(ser.iter_mapped_attributes(mapping, attrs, fields,
                                               version, specs)), refused
    return _iter_with_version(ser, mapping, attrs, fields, version), {}


def _restore(layer, decoded):
    from qgis.core import QgsFeature, QgsGeometry, QgsPointXY
    feat = QgsFeature(layer.fields())
    for idx, value in decoded.items():
        feat.setAttribute(idx, value)
    feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(2.35, 48.85)))
    ok, added = layer.dataProvider().addFeatures([feat])
    layer.reload()
    return ok


def _phase_provider(d, kind, tmpdir):
    from recoverland.core import serialization as ser
    out = {}
    layer = _open(kind, tmpdir)
    if not layer.isValid():
        raise RuntimeError(f"{kind}: couche invalide")
    _populate(layer)
    live = _live(layer, "a")
    value = live["dtm"]
    out["live_spec"] = value.timeSpec().name
    out["v5_text"] = _v5_text(value)
    want = value.toMSecsSinceEpoch()
    matcher = getattr(ser, "journal_datetime_matches", None)
    if matcher is not None:
        shifted = value.addSecs(int(value.toLocalTime().offsetFromUtc()))
        out["match_exact"] = matcher(value, out["v5_text"], 5)
        out["match_shifted"] = matcher(shifted, out["v5_text"], 5)
    layer.dataProvider().deleteFeatures([live.id()])
    layer.reload()
    attrs = {"label": "a_restored", "dtm": out["v5_text"]}
    for version in (5, None):
        tag = "v5" if version == 5 else "vnull"
        try:
            decoded, refused = _decode(ser, layer, attrs, version,
                                       wired=True)
            out[f"{tag}_refused"] = dict(refused)
            out[f"{tag}_decoded_fields"] = sorted(decoded)
            _restore(layer, decoded)
            back = _live(layer, "a_restored")
            got = back["dtm"] if back is not None else None
            out[f"{tag}_restored_ok"] = (
                got is not None and hasattr(got, "toMSecsSinceEpoch")
                and got.toMSecsSinceEpoch() == want)
            out[f"{tag}_restored"] = (
                got.toUTC().toString("yyyy-MM-ddTHH:mm:ssZ")
                if hasattr(got, "toUTC") else repr(got))
            out[f"{tag}_restored_null"] = back is not None and (
                got is None or (hasattr(got, "isNull") and got.isNull()))
            if back is not None:
                layer.dataProvider().deleteFeatures([back.id()])
                layer.reload()
        except Exception as exc:  # noqa: BLE001 - measured, not hidden
            out[f"{tag}_error"] = repr(exc)
    out["expected_utc"] = value.toUTC().toString("yyyy-MM-ddTHH:mm:ssZ")
    # Un appelant qui ne donne PAS le fuseau du provider.
    try:
        decoded, _refused = _decode(ser, layer, attrs, 5, wired=False)
        refused_cls = getattr(ser, "LegacyDateTimeRefused", None)
        out["unwired"] = ("decoded", repr(decoded.get(
            layer.fields().indexOf("dtm"))), refused_cls is not None)
    except Exception as exc:  # noqa: BLE001
        out["unwired"] = ("refused", type(exc).__name__, str(exc))
    d["providers"][kind] = out


def _phase_unresolvable(d, tmpdir):
    """Trois cas ou le type de fuseau ne peut pas etre etabli."""
    from osgeo import ogr
    from recoverland.core import serialization as ser
    factory = getattr(ser, "provider_time_specs", None)
    if factory is None:
        d["unresolvable"] = "absent"
        return
    out = {}
    attrs = {"label": "x", "dtm": "2026-01-15T13:30:00"}
    mapping = {"label": "label", "dtm": "dtm"}
    # 1. Types melanges : lignes ecrites par QGIS (UTC) + une ligne ecrite
    #    sans fuseau par un autre outil (relue en heure locale).
    layer = _open("gpkg", tmpdir)
    _populate(layer)
    path = layer.source().split("|")[0]
    layer = None
    ds = ogr.Open(path, 1)
    lyr = ds.GetLayerByName("t")
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetField("label", "outil")
    feat.SetField("dtm", 2026, 1, 16, 10, 0, 0, 0)
    feat.SetGeometry(ogr.CreateGeometryFromWkt("POINT (2 48)"))
    lyr.CreateFeature(feat)
    feat = None
    ds = None
    from qgis.core import QgsVectorLayer
    layer = QgsVectorLayer(f"{path}|layername=t", "t", "ogr")
    out["mixed_specs"] = sorted({f["dtm"].timeSpec().name
                                 for f in layer.getFeatures()})
    out["mixed"] = ser.legacy_datetime_refusals(
        mapping, attrs, layer.fields(), 5, factory(layer))
    # 2. Aucune valeur vivante dans le champ.
    empty = _open("gpkg", os.path.join(tmpdir, "empty"))
    out["empty"] = ser.legacy_datetime_refusals(
        mapping, attrs, empty.fields(), 5, factory(empty))
    # 3. Valeur saisie (tampon d'edition) relue par une GeoPackage : elle a
    #    ete convertie en UTC, le type de la saisie est perdu.
    gpkg = _open("gpkg", os.path.join(tmpdir, "edit"))
    _populate(gpkg)
    edit_role = getattr(ser, "CAPTURED_FROM_EDIT_BUFFER", "edit_buffer")
    out["edit_buffer_gpkg"] = ser.legacy_datetime_refusals(
        mapping, attrs, gpkg.fields(), 5, factory(gpkg, edit_role))
    # Temoin : la meme saisie sur GeoJSON (type garde) est etablie.
    gj = _open("geojson", os.path.join(tmpdir, "gj"))
    _populate(gj)
    out["edit_buffer_geojson"] = ser.legacy_datetime_refusals(
        mapping, attrs, gj.fields(), 5, factory(gj, edit_role))
    # Un texte v6 / v7 n'est jamais refuse : il se decrit (v7) ou il est UTC.
    out["v6_text"] = ser.legacy_datetime_refusals(
        mapping, attrs, empty.fields(), 6, factory(empty))
    d["unresolvable"] = out


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_legacy_provider_spec setup: trace_id={ctx.trace_id}",
         "INFO")


def run(ctx):
    from qgis.PyQt.QtCore import QTimeZone
    from recoverland.core.logger import flog
    d = ctx.data
    d["error"] = None
    d["zone"] = bytes(QTimeZone.systemTimeZoneId()).decode("ascii", "replace")
    d["offsets"] = [int(_local(*parts).offsetFromUtc()) for _l, parts in _ROWS]
    d["providers"] = {}
    tmpdir = tempfile.mkdtemp(prefix="rl_legspec_")
    try:
        for kind in ("gpkg", "geojson", "sqlite_ogr", "spatialite", "memory",
                     "colonne_postgres"):
            sub = os.path.join(tmpdir, kind)
            os.makedirs(sub, exist_ok=True)
            try:
                _phase_provider(d, kind, sub)
            except Exception as exc:  # noqa: BLE001
                d["providers"][kind] = {"error": repr(exc)}
        for sub in ("empty", "edit", "gj"):
            os.makedirs(os.path.join(tmpdir, sub), exist_ok=True)
        try:
            _phase_unresolvable(d, tmpdir)
        except Exception as exc:  # noqa: BLE001
            d["unresolvable"] = {"error": repr(exc)}
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    flog(f"tx_serial_legacy_provider_spec run: trace_id={ctx.trace_id} "
         f"zone={d.get('zone')} providers={json.dumps(d.get('providers'), default=str)[:1500]} "
         f"unresolvable={d.get('unresolvable')} error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    out = [
        ("scenario_ran", d.get("error") is None,
         f"le scenario a plante : {d.get('error')}"),
        ("precondition_zone_has_an_offset",
         all(o != 0 for o in d.get("offsets") or [0]),
         f"fuseau du poste {d.get('zone')!r}, decalages aux dates d'essai="
         f"{d.get('offsets')} s : a decalage nul, heure locale et UTC "
         f"coincident et ce scenario ne prouve rien."),
    ]
    for kind, res in (d.get("providers") or {}).items():
        for tag, label in (("v5", "schema 5"), ("vnull", "version NULL")):
            if kind != "colonne_postgres":
                # Hypothesis revised (v7): voir la docstring. Le type de
                # fuseau de ces providers appartient a chaque valeur : exact,
                # ou refus nomme sans rien ecrire dans le champ.
                refused = res.get(f"{tag}_refused") or {}
                out.append((
                    f"{kind}_{tag}_never_written_shifted",
                    res.get(f"{tag}_restored_ok") is True
                    or (bool(refused.get("dtm"))
                        and res.get(f"{tag}_restored_null") is True),
                    f"{kind} : texte 5.x {res.get('v5_text')!r} (valeur "
                    f"vivante de type {res.get('live_spec')!r}), evenement "
                    f"{label} restaure a {res.get(f'{tag}_restored')!r}, "
                    f"attendu {res.get('expected_utc')!r} ou un refus nomme "
                    f"sans rien ecrire ; refus={refused!r} erreur="
                    f"{res.get(f'{tag}_error')!r}. Une date restauree "
                    f"decalee de l'offset UTC est une donnee faussee que "
                    f"rien ne signale."))
                continue
            out.append((
                f"{kind}_{tag}_restored_at_the_captured_instant",
                res.get(f"{tag}_restored_ok") is True,
                f"{kind} : texte 5.x {res.get('v5_text')!r} (valeur vivante de "
                f"type {res.get('live_spec')!r}), evenement {label} restaure a "
                f"{res.get(f'{tag}_restored')!r}, attendu "
                f"{res.get('expected_utc')!r} ; erreur={res.get(f'{tag}_error')!r}. "
                f"Une date restauree decalee de l'offset UTC est une donnee "
                f"faussee que rien ne signale."))
        unwired = res.get("unwired") or ()
        out.append((
            f"{kind}_caller_without_provider_spec_writes_nothing",
            bool(unwired) and unwired[0] == "refused"
            and unwired[1] == "LegacyDateTimeRefused" and "dtm" in unwired[2],
            f"{kind} : decodage d'un texte 5.x sans le type de fuseau du "
            f"provider -> {unwired!r}. Il doit refuser en nommant le champ ; "
            f"rendre une valeur, c'est deviner le fuseau et ecrire decale."))
        out.append((
            f"{kind}_matcher_exact_vs_shifted",
            res.get("match_exact") is True and res.get("match_shifted") is False,
            f"{kind} : comparateur des preuves, valeur exacte -> "
            f"{res.get('match_exact')!r}, valeur decalee de l'offset -> "
            f"{res.get('match_shifted')!r}. La valeur decalee ne doit passer "
            f"aucune preuve, la valeur exacte toutes."))
    unres = d.get("unresolvable")
    unres = unres if isinstance(unres, dict) else {}
    for case, story in (
            ("mixed", "types de fuseau melanges dans le champ "
                      f"({unres.get('mixed_specs')})"),
            ("empty", "aucune valeur vivante dans le champ"),
            ("edit_buffer_gpkg", "valeur saisie relue par une GeoPackage "
                                 "qui l'a convertie en UTC")):
        refused = unres.get(case)
        out.append((
            f"unresolvable_{case}_refused_and_named",
            isinstance(refused, dict) and bool(refused.get("dtm")),
            f"{story} : refus={refused!r} (resultat complet {d.get('unresolvable')!r}). "
            f"Le type de fuseau ne peut pas etre etabli : le champ doit etre "
            f"refuse avec sa raison, jamais devine."))
    # Hypothesis revised (v7): une valeur saisie sur GeoJSON etait tenue
    # pour etablie par les autres valeurs du champ ; le type d'une saisie
    # n'est journalise nulle part et le champ ne le prouve pas (voir la
    # docstring). Le ciblage du refus reste exige : un texte v6 n'est
    # jamais refuse, et la colonne typee restaure exact (plus haut).
    out.append((
        "edit_buffer_refused_v6_text_never",
        bool((unres.get("edit_buffer_geojson") or {}).get("dtm"))
        and unres.get("v6_text") == {},
        f"GeoJSON, valeur saisie -> {unres.get('edit_buffer_geojson')!r} "
        f"(attendu un refus nomme), texte v6 -> {unres.get('v6_text')!r} "
        f"(attendu aucun refus) : le refus doit rester cible."))
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_serial_legacy_provider_spec run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
