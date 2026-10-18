"""tx_serial_legacy_datetime - RecoverLand validation runtime.

Invariant: BL-SER-P0-01 (une valeur restauree est IDENTIQUE a la valeur
capturee ; aucun succes n'est annonce sur une valeur alteree).

Le trou que ce scenario prouve
------------------------------
Le journal a connu deux significations pour le MEME texte de date-heure
``yyyy-MM-ddTHH:mm:ss`` :

  * jusqu'au schema 5 (plugin 4.9 / 5.0.x), les chiffres du provider :
    l'heure murale locale d'une colonne PostgreSQL ``timestamp``, d'une
    date texte spatialite, d'un ``datetime`` MSSQL. La restauration de
    l'epoque ecrivait ce texte tel quel, donc relu comme heure LOCALE ;
  * depuis le schema 6 (plugin 6.0.0), les chiffres UTC.

Aucune migration n'a reecrit les anciens evenements, et
`_deserialize_datetime` lit TOUT texte sans suffixe comme de l'UTC. Seul
`event_schema_version` distingue les deux, et personne ne le lisait. Un
evenement 5.x est donc restaure decale de l'offset UTC de sa date : une
heure en hiver, deux en ete a Paris. Le comparateur des preuves
(`_qdatetime_matches_str`) accepte ensuite le texte s'il egale la forme
naive, UTC OU locale de la valeur vivante : la valeur decalee passe toutes
les preuves et tous les controles d'idempotence.

Consequence utilisateur : journal cree en 5.0.1 a Paris, une colonne
``date_constat`` a 14:30 le 15 janvier, entite supprimee. Apres mise a
jour, le rewind la recree a 15:30 et annonce APPLIED. Rien ne le signalera
jamais.

Ce scenario, dans le fuseau du poste (Europe/Paris sur la machine de
reference), ecrit des evenements de chaque generation dans une vraie
GeoPackage via le decodeur de restauration (`iter_mapped_attributes` avec
la version de l'evenement), relit l'heure murale, et interroge le
comparateur strict `journal_datetime_matches`.

Hypothesis revised (v7): la premiere version de ce scenario journalisait
``14:30:00`` pour 14:30 locales en 5.x sur une GeoPackage, pour toute
ligne. C'est faux, et mesure : 5.x ecrivait
``value.toString('yyyy-MM-ddTHH:mm:ss')`` de la valeur que le PROVIDER
rendait, et une GeoPackage la rend dans la forme stockee de CHAQUE ligne :
une saisie validee par QGIS est stockee ``13:30:00Z`` et relue en UTC
(texte 5.x ``13:30:00``) ; une ligne ecrite par "Exporter / Enregistrer
sous" ou par un autre outil est stockee sans fuseau et relue en heure
locale (texte 5.x ``14:30:00``). Les deux formes coexistent dans une
GeoPackage exportee puis editee, et le texte ne dit pas laquelle il vient
de lire.

Second tour : le correctif precedent retrouvait cette forme en mesurant
les autres lignes du champ ; or la ligne supprimee n'est plus stockee et
les lignes restantes ne prouvent rien de SA forme (mondes jumeaux de
`tx_serial_legacy_per_row_spec` : memes entrees, verites a une heure
d'ecart). Les cas 5.x sur GeoPackage, saisie QGIS comme ligne exportee
(cas ``v5_exporte`` ajoute), doivent donc etre REFUSES en nommant le
champ, sans rien ecrire : l'ancienne exigence "14:30 restaurees" ne peut
etre tenue que par une lecture qui, sur les memes entrees, restaure
l'autre forme decalee d'une heure. Les cas v6 et v7 gardent leur
exigence inchangee (14:30 restaurees). La restauration exacte d'un texte
5.x la ou le fuseau appartient a la colonne est exigee par
`tx_serial_legacy_per_row_spec` et `tx_serial_legacy_provider_spec`.

Le passage de `event_schema_version` par les chemins de restauration de
`restore_service` / `restore_executor` et le remplacement de
`_qdatetime_matches_str` relevent de ces modules ; leur preuve de bout en
bout est dans `rw_identity_serial_consumer_wiring`.

Verdict attendu avant correctif : FAIL (13:30 au lieu de 14:30 pour les
evenements 5.x, lus comme heure locale ; puis, au second tour, cas 5.x
ecrits au lieu d'etre refuses).
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

SCENARIO_ID = "tx_serial_legacy_datetime"
INVARIANT = "BL-SER-P0-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Texte capture comme 5.x le faisait : la valeur ecrite par le provider,
# relue, formatee sans fuseau (voir _capture_5x).
_CAPTURED_5X = object()
# Meme chose pour une ligne produite par "Exporter / Enregistrer sous"
# (texte stocke sans fuseau, relu en heure locale).
_EXPORTED_5X = object()
# Attendu d'un cas 5.x sur GeoPackage : refus nomme, rien ecrit.
_REFUSED = "refus nomme"

# (label, texte journalise, event_schema_version, heure murale attendue)
_CASES = [
    # Hypothesis revised (v7): texte 5.x reel d'une GeoPackage (chiffres
    # rendus par le provider dans la forme stockee de la ligne), et, au
    # second tour, refus nomme : la forme d'une ligne disparue n'est pas
    # etablie (voir la docstring).
    ("v5_hiver", _CAPTURED_5X, 5, _REFUSED),
    ("v5_ete", _CAPTURED_5X, 5, _REFUSED),
    ("version_nulle", _CAPTURED_5X, None, _REFUSED),
    ("v5_exporte", _EXPORTED_5X, 5, _REFUSED),
    # 6.0.x : 14:30 locales d'hiver = 13:30 UTC.
    ("v6_utc", "2026-01-15T13:30:00", 6, "2026-01-15T14:30:00"),
    # v7 : la valeur se decrit elle-meme, la version ne compte plus.
    ("v7_naive", "2026-01-15T14:30:00.000", 7, "2026-01-15T14:30:00"),
    ("v7_utc", "2026-01-15T13:30:00.000Z", 7, "2026-01-15T14:30:00"),
]


def _make_gpkg(tmpdir, rows=(), name="legacy_dt"):
    """GeoPackage produite par QgsVectorFileWriter, *rows* exportees."""
    from qgis.core import (
        QgsVectorLayer, QgsVectorFileWriter, QgsCoordinateTransformContext,
        QgsFeature,
    )
    from qgis.PyQt.QtCore import QDateTime
    mem = QgsVectorLayer("NoGeometry?field=label:string(20)&field=dtm:datetime",
                         "seed", "memory")
    feats = []
    for label, wall_clock in rows:
        feat = QgsFeature(mem.fields())
        feat.setAttribute("label", label)
        feat.setAttribute("dtm", QDateTime.fromString(
            wall_clock, "yyyy-MM-ddTHH:mm:ss"))
        feats.append(feat)
    if feats:
        mem.dataProvider().addFeatures(feats)
    path = os.path.join(tmpdir, f"{name}.gpkg")
    opts = QgsVectorFileWriter.SaveVectorOptions()
    opts.driverName = "GPKG"
    opts.layerName = "constats"
    err = QgsVectorFileWriter.writeAsVectorFormatV3(
        mem, path, QgsCoordinateTransformContext(), opts)
    if err[0] != 0:
        raise RuntimeError(f"GPKG write failed: {err}")
    layer = QgsVectorLayer(f"{path}|layername=constats", "constats", "ogr")
    if not layer.isValid():
        raise RuntimeError("couche GPKG invalide")
    return layer


def _capture_5x(layer, label, wall_clock):
    """Le texte que 5.x journalisait pour cette saisie sur cette couche.

    La saisie (heure murale locale) est ecrite par le provider, relue par
    le provider, et formatee comme 5.x : ``toString`` sans fuseau.
    """
    from qgis.core import QgsFeature
    from qgis.PyQt.QtCore import QDateTime
    feat = QgsFeature(layer.fields())
    feat.setAttribute("label", f"capture_{label}")
    feat.setAttribute("dtm", QDateTime.fromString(wall_clock,
                                                  "yyyy-MM-ddTHH:mm:ss"))
    layer.dataProvider().addFeatures([feat])
    layer.reload()
    for live in layer.dataProvider().getFeatures():
        if live["label"] == f"capture_{label}":
            return live["dtm"].toString("yyyy-MM-ddTHH:mm:ss")
    raise RuntimeError(f"capture {label} introuvable")


def _exported_5x(tmpdir, label, wall_clock):
    """Texte 5.x d'une ligne exportee : relue en heure locale, sans fuseau."""
    layer = _make_gpkg(tmpdir, [(f"capture_{label}", wall_clock)],
                       name=f"exporte_{label}")
    for live in layer.dataProvider().getFeatures():
        if live["label"] == f"capture_{label}":
            return layer, live["dtm"].toString("yyyy-MM-ddTHH:mm:ss")
    raise RuntimeError(f"export {label} introuvable")


def _decode_row(layer, attrs, version):
    """Ce que la restauration ecrit : iter_mapped_attributes + la version."""
    from recoverland.core import serialization as ser
    mapping = {name: name for name in attrs}
    fields = layer.fields()
    factory = getattr(ser, "provider_time_specs", None)
    if factory is not None:
        # Le decodeur cable : fuseau que le provider rend pour ce champ.
        return dict(ser.iter_mapped_attributes(mapping, attrs, fields,
                                               version, factory(layer)))
    try:
        return dict(ser.iter_mapped_attributes(mapping, attrs, fields,
                                               event_schema_version=version))
    except TypeError:
        # Code sans parametre de version : il decode comme il l'a toujours
        # fait, et c'est precisement ce qui est mesure.
        return dict(ser.iter_mapped_attributes(mapping, attrs, fields))


def setup(ctx):
    from recoverland.core.logger import flog
    flog(f"tx_serial_legacy_datetime setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from qgis.core import QgsFeature
    from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    from recoverland.core import serialization as ser
    from recoverland.core.logger import flog

    d = ctx.data
    d["error"] = None
    d["zone"] = bytes(QTimeZone.systemTimeZoneId()).decode("ascii", "replace")
    d["offsets"] = [int(QDateTime(QDate(2026, month, 15), QTime(12, 0))
                        .offsetFromUtc()) for month in (1, 7)]
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_sld_")
    try:
        layer = _make_gpkg(tmpdir)
        d["texts"] = {}
        d["refused"] = {}
        targets = {}
        for label, text, _version, _expected in _CASES:
            # La saisie de chaque cas : 14:30 locales, en hiver ou en ete.
            wall_clock = ("2026-07-15T14:30:00" if label == "v5_ete"
                          else "2026-01-15T14:30:00")
            if text is _CAPTURED_5X:
                d["texts"][label] = _capture_5x(layer, label, wall_clock)
            elif text is _EXPORTED_5X:
                targets[label], d["texts"][label] = _exported_5x(
                    tmpdir, label, wall_clock)
        feats = []
        for label, text, version, _expected in _CASES:
            text = d["texts"].get(label, text)
            target = targets.get(label, layer)
            try:
                decoded = _decode_row(target, {"label": label, "dtm": text},
                                      version)
            except ser.LegacyDateTimeRefused as exc:
                # Refus nomme : rien n'est ecrit pour ce cas.
                d["refused"][label] = dict(exc.fields)
                continue
            feat = QgsFeature(layer.fields())
            for idx, value in decoded.items():
                feat.setAttribute(idx, value)
            feats.append(feat)
        ok, _ = layer.dataProvider().addFeatures(feats)
        d["write_ok"] = bool(ok)
        layer.reload()
        d["restored"] = {}
        for feat in layer.getFeatures():
            value = feat["dtm"]
            d["restored"][feat["label"]] = (
                value.toLocalTime().toString("yyyy-MM-ddTHH:mm:ss")
                if hasattr(value, "toLocalTime") else repr(value))

        matcher = getattr(ser, "journal_datetime_matches", None)
        d["matcher_available"] = matcher is not None
        if matcher is not None:
            shifted = QDateTime(QDate(2026, 1, 15), QTime(15, 30))
            exact = QDateTime(QDate(2026, 1, 15), QTime(14, 30))
            d["match_shifted_v5"] = matcher(shifted, "2026-01-15T14:30:00", 5)
            d["match_exact_v5"] = matcher(exact, "2026-01-15T14:30:00", 5)
            d["match_exact_v6"] = matcher(exact, "2026-01-15T13:30:00", 6)
            d["match_shifted_v6"] = matcher(exact, "2026-01-15T14:30:00", 6)
            d["match_ms_differ"] = matcher(
                QDateTime(QDate(2026, 1, 15), QTime(14, 30, 0, 517)),
                "2026-01-15T14:30:00.000", 7)
        layer = None
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    flog(f"tx_serial_legacy_datetime run: trace_id={ctx.trace_id} "
         f"zone={d.get('zone')} restored={d.get('restored')} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = [("scenario_ran", d.get("error") is None,
            f"le scenario a plante : {d.get('error')}"),
           ("gpkg_write_ok", d.get("write_ok") is True,
            "l'ecriture dans la GeoPackage a echoue : rien n'est mesure"),
           # A decalage nul, heure locale et UTC coincident : toutes les
           # lectures donnent le meme resultat et rien n'est prouve.
           ("precondition_zone_has_an_offset",
            all(o not in (None, 0) for o in d.get("offsets") or [0]),
            f"fuseau du poste {d.get('zone')!r}, decalages UTC aux dates "
            f"d'essai={d.get('offsets')} s : lancez ce scenario dans un "
            f"fuseau decale (Europe/Paris), sinon il ne prouve rien.")]
    restored = d.get("restored") or {}
    texts = d.get("texts") or {}
    refused = d.get("refused") or {}
    for label, text, version, expected in _CASES:
        text = texts.get(label, text)
        got = restored.get(label)
        if expected is _REFUSED:
            # Hypothesis revised (v7): voir la docstring. Le champ est
            # refuse en le nommant et rien n'est ecrit pour ce cas.
            out.append((
                f"refused_by_name_{label}",
                got is None and bool((refused.get(label) or {}).get("dtm")),
                f"evenement {label} (schema={version}) journalise {text!r} "
                f"sur une GeoPackage : ecrit {got!r}, refus="
                f"{refused.get(label)!r}. La forme stockee de la ligne "
                f"disparue n'est pas etablie : ecrire, c'est deviner le "
                f"fuseau, et la meme lecture restaure l'autre forme decalee "
                f"d'une heure en annoncant APPLIED.",
            ))
            continue
        out.append((
            f"restored_wall_clock_{label}",
            got == expected,
            f"evenement {label} (schema={version}) journalise {text!r} : "
            f"relu {got!r} apres restauration, attendu {expected!r} "
            f"(fuseau {d.get('zone')}). Une date de constat restauree "
            f"decalee de l'offset UTC, annoncee APPLIED, est une donnee "
            f"faussee que rien ne signalera.",
        ))
    out.append((
        "strict_matcher_available",
        d.get("matcher_available") is True,
        "serialization.journal_datetime_matches absent : le comparateur des "
        "preuves n'a qu'une lecture laxiste a trois rendus.",
    ))
    out.append((
        "shifted_value_is_not_the_captured_one",
        d.get("match_shifted_v5") is False,
        f"15:30 compare a l'evenement 5.x '14:30:00' -> "
        f"{d.get('match_shifted_v5')!r} : la valeur decalee d'une heure ne "
        f"doit passer aucune preuve.",
    ))
    out.append((
        "exact_value_matches_its_generation",
        d.get("match_exact_v5") is True and d.get("match_exact_v6") is True,
        f"14:30 locale vs v5 '14:30:00' -> {d.get('match_exact_v5')!r}, vs "
        f"v6 '13:30:00' -> {d.get('match_exact_v6')!r} : la bonne valeur "
        f"doit etre reconnue, sinon chaque restauration saine est refusee.",
    ))
    out.append((
        "one_reading_per_generation",
        d.get("match_shifted_v6") is False,
        f"14:30 locale vs v6 '14:30:00' (donc 15:30 locale) -> "
        f"{d.get('match_shifted_v6')!r} : une seule lecture par generation, "
        f"pas trois.",
    ))
    out.append((
        "milliseconds_are_compared",
        d.get("match_ms_differ") is False,
        f"14:30:00.517 vs '14:30:00.000' -> {d.get('match_ms_differ')!r} : "
        f"une difference de millisecondes est une difference.",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_serial_legacy_datetime run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
