"""tx_geom_wkb_roundtrip_types - RecoverLand validation runtime.

Invariant: I-8 (la geometrie relue doit etre la geometrie capturee).

Le mecanisme
------------
La chaine geometrique du journal est: `geometry_utils.geometry_to_wkb`
(capture) -> BLOB SQLite (`audit_event.geometry_wkb`) -> lecture par
`event_stream_repository` -> `geometry_utils.rebuild_geometry`
(restauration). Aucune de ces etapes ne connait le type de geometrie:
tout repose sur le fait que le WKB est stocke et relu octet a octet.

Ce scenario passe un catalogue de 18 geometries dans cette chaine reelle
et verifie trois choses pour chacune:
  - les octets ressortent identiques (aucune perte au passage BLOB);
  - le type WKB survit, suffixe Z/M/ZM compris (une PointZM qui revient
    en Point perd son altitude et sa mesure silencieusement);
  - les courbes restent des courbes (une CircularString relinearisee en
    LineString change de forme sans prevenir);
  - l'aire / la longueur sont conservees.

Deux limites connues sont attaquees en plus:

1. VIDE vs NULL. `geometry_to_wkb` s'appuie sur `is_geometry_present`, qui
   fusionne `isNull()` et `isEmpty()` dans un seul verdict et renvoie None
   dans les deux cas. Une feature dont la geometrie etait VIDE est donc
   journalisee comme une feature SANS geometrie. Cote restauration,
   `restore_updated_feature` teste `has_geom = event.geometry_wkb is not
   None`: la geometrie n'est pas retablie du tout, la feature garde celle
   qu'elle a aujourd'hui, et la restauration est rapportee "Reverted".

2. Enveloppe WKB des GEOMETRYCOLLECTION. `wkb_envelope.parse_envelope`
   documente qu'un type non supporte doit renvoyer None (le lecteur reste
   conservateur et garde l'evenement). C'est vrai pour une CircularString
   isolee. Mais pour une GEOMETRYCOLLECTION, `_read_multi_env` boucle sur
   les enfants: quand un enfant non supporte renvoie None, le curseur du
   lecteur est reste plante au milieu de cet enfant, et les enfants
   suivants sont decodes a partir d'octets arbitraires. La fonction rend
   alors une enveloppe FAUSSE au lieu de None. `fetch_events_in_zone`
   (filtre spatial du Time Lens) croit cette enveloppe: l'evenement
   disparait de la fenetre ou y apparait sans raison.

Plan du scenario (vrai journal SQLite, vrai fetch, geometries QGIS):
    setup : base en memoire au schema RecoverLand, un evenement INSERT par
            geometrie du catalogue.
    run   : fetch_events_after_cutoff -> rebuild_geometry -> comparaison
            octets / type / mesure, puis les deux sondes VIDE-vs-NULL et
            enveloppe.

Verdict attendu si le produit est sain: PASS.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_geom_wkb_roundtrip_types"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_DATASOURCE_FP = "tx_wrt_datasource"
_PROJECT_FP = "tx_wrt_project"

# (cle, WKT, famille) ; famille pilote les mesures comparees.
#   "surface" -> area(), "linear" -> length(), "punctual" -> aucune mesure
_CATALOGUE = [
    ("point", "POINT(2.5 48.5)", "punctual"),
    ("linestring", "LINESTRING(0 0, 10 0, 10 10)", "linear"),
    ("polygon_hole",
     "POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,4 2,4 4,2 4,2 2))", "surface"),
    ("multipoint", "MULTIPOINT((0 0),(1 1),(2 2))", "punctual"),
    ("multilinestring", "MULTILINESTRING((0 0,1 1),(5 5,6 6))", "linear"),
    ("multipolygon",
     "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((5 5,6 5,6 6,5 6,5 5)))",
     "surface"),
    ("point_z", "POINT Z (1 2 3)", "punctual"),
    ("point_m", "POINT M (1 2 4)", "punctual"),
    ("point_zm", "POINT ZM (1 2 3 4)", "punctual"),
    ("polygon_z", "POLYGON Z ((0 0 5,10 0 5,10 10 5,0 10 5,0 0 5))",
     "surface"),
    ("polygon_m", "POLYGON M ((0 0 1,10 0 2,10 10 3,0 10 4,0 0 1))",
     "surface"),
    ("linestring_zm", "LINESTRING ZM (0 0 1 10, 5 5 2 20)", "linear"),
    ("multipoint_z", "MULTIPOINT Z ((0 0 1),(1 1 2))", "punctual"),
    ("circularstring", "CIRCULARSTRING(0 0, 5 5, 10 0)", "linear"),
    ("compoundcurve",
     "COMPOUNDCURVE((0 0, 5 0), CIRCULARSTRING(5 0, 7 2, 10 0))", "linear"),
    ("curvepolygon",
     "CURVEPOLYGON(CIRCULARSTRING(0 0, 10 0, 0 0))", "surface"),
    ("huge_coords", "POINT(12345678901.234567 -9876543210.987654)",
     "punctual"),
    ("high_precision", "POINT(2.1234567890123456 48.9876543210987654)",
     "punctual"),
]

# Geometries dont l'enveloppe n'est PAS calculable par le parseur pur
# Python (types courbes hors 1..7): il doit renvoyer None, pas une valeur
# approximative.
_CURVE_KEYS = {"circularstring", "compoundcurve", "curvepolygon"}


def _t_iso(t: datetime) -> str:
    return t.isoformat()


def _geom_signature(geom) -> dict:
    """Description comparable d'une QgsGeometry (type, mesures, wkt court)."""
    from qgis.core import QgsWkbTypes

    if geom is None:
        return {"present": False}
    try:
        null = bool(geom.isNull())
        empty = bool(geom.isEmpty())
    except Exception:  # noqa: BLE001
        return {"present": False}
    sig = {
        "present": not (null or empty),
        "null": null,
        "empty": empty,
        "wkb_type": int(geom.wkbType()),
        "type_name": QgsWkbTypes.displayString(geom.wkbType()),
    }
    try:
        sig["area"] = round(geom.area(), 9)
        sig["length"] = round(geom.length(), 9)
    except Exception:  # noqa: BLE001
        sig["area"] = None
        sig["length"] = None
    try:
        bb = geom.boundingBox()
        sig["bbox"] = (bb.xMinimum(), bb.yMinimum(), bb.xMaximum(),
                       bb.yMaximum())
    except Exception:  # noqa: BLE001
        sig["bbox"] = None
    try:
        sig["wkt"] = geom.asWkt(6)[:90]
    except Exception:  # noqa: BLE001
        sig["wkt"] = ""
    return sig


def _seed(conn, t0):
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.geometry_utils import geometry_to_wkb
    from qgis.core import QgsGeometry

    sql = (
        "INSERT INTO audit_event ("
        + AUDIT_EVENT_INSERT_SQL + ") VALUES ("
        + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")"
    )

    seeded = {}
    for i, (key, wkt, family) in enumerate(_CATALOGUE):
        geom = QgsGeometry.fromWkt(wkt)
        wkb = geometry_to_wkb(geom)
        row = (
            _PROJECT_FP, _DATASOURCE_FP, "lyr_wrt", "wrt_layer", "ogr",
            json.dumps({"fid": i + 1}),
            "INSERT",
            json.dumps({"all_attributes": {"nom": key}}),
            wkb, "Unknown", "EPSG:4326",
            json.dumps([{"name": "nom", "type": "String"}]),
            "tester", None,
            _t_iso(t0 + timedelta(seconds=i + 1)), None,
            f"fid:{i + 1}", 2, None, None,
        )
        cur = conn.execute(sql, row)
        seeded[key] = {
            "event_id": cur.lastrowid,
            "wkt": wkt,
            "family": family,
            "wkb_in": wkb,
            "wkb_len": len(wkb) if wkb else 0,
            "sig_in": _geom_signature(geom),
        }
    conn.commit()
    return seeded


def setup(ctx):
    from recoverland.core.sqlite_schema import initialize_schema
    from recoverland.core.logger import flog

    t0 = datetime(2026, 8, 12, 9, 0, 0, tzinfo=timezone.utc)
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    seeded = _seed(conn, t0)

    ctx.data["conn"] = conn
    ctx.data["t0"] = t0
    ctx.data["seeded"] = seeded

    lens = {k: v["wkb_len"] for k, v in seeded.items()}
    flog(
        f"tx_geom_wkb_roundtrip_types setup: trace_id={ctx.trace_id} "
        f"n_geoms={len(seeded)} wkb_lens={lens}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.event_stream_repository import (
        fetch_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.geometry_utils import (
        geometry_to_wkb, rebuild_geometry,
    )
    from recoverland.core.wkb_envelope import parse_envelope
    from recoverland.core.logger import flog
    from qgis.core import QgsGeometry

    conn = ctx.data["conn"]
    seeded = ctx.data["seeded"]

    flog(f"tx_geom_wkb_roundtrip_types run start: trace_id={ctx.trace_id}",
         "INFO")

    cutoff = RestoreCutoff(
        CutoffType.BY_DATE, _t_iso(ctx.data["t0"]), inclusive=True,
    )
    events = fetch_events_after_cutoff(
        conn, _DATASOURCE_FP, cutoff, trace_id=ctx.trace_id,
    )
    by_eid = {e.event_id: e for e in events}
    ctx.data["fetched_count"] = len(events)

    results = {}
    for key, info in seeded.items():
        event = by_eid.get(info["event_id"])
        entry = {"fetched": event is not None}
        if event is not None:
            wkb_out = event.geometry_wkb
            wkb_out = bytes(wkb_out) if wkb_out is not None else None
            entry["bytes_identical"] = (wkb_out == info["wkb_in"])
            rebuilt = rebuild_geometry(wkb_out)
            entry["sig_out"] = _geom_signature(rebuilt)
            # Re-serialisation: la geometrie reconstruite doit produire le
            # meme WKB, sinon la prochaine capture divergera.
            entry["reserialised_identical"] = (
                geometry_to_wkb(rebuilt) == info["wkb_in"]
            )
            # Enveloppe pure-Python vs enveloppe QGIS.
            entry["envelope"] = parse_envelope(wkb_out)
        results[key] = entry
    ctx.data["results"] = results

    # --- sonde 1: geometrie VIDE vs geometrie NULL -----------------------
    null_geom = QgsGeometry()
    empty_poly = QgsGeometry.fromWkt("POLYGON EMPTY")
    ctx.data["probe_empty"] = {
        "null_is_null": bool(null_geom.isNull()),
        "empty_is_null": bool(empty_poly.isNull()),
        "empty_is_empty": bool(empty_poly.isEmpty()),
        "wkb_from_null": geometry_to_wkb(null_geom),
        "wkb_from_empty": geometry_to_wkb(empty_poly),
        # Le WKB d'une POLYGON EMPTY existe pourtant bel et bien.
        "raw_wkb_empty_len": len(bytes(empty_poly.asWkb())),
    }

    # --- sonde 2: enveloppe d'une GEOMETRYCOLLECTION a enfant courbe -----
    gc_mixed = QgsGeometry.fromWkt(
        "GEOMETRYCOLLECTION(CIRCULARSTRING(0 0, 5 5, 10 0), POINT(100 100))")
    gc_plain = QgsGeometry.fromWkt(
        "GEOMETRYCOLLECTION(LINESTRING(0 0, 10 0), POINT(100 100))")
    probe = {}
    for name, g in (("mixed", gc_mixed), ("plain", gc_plain)):
        wkb = geometry_to_wkb(g)
        bb = g.boundingBox()
        probe[name] = {
            "qgis_bbox": (round(bb.xMinimum(), 6), round(bb.yMinimum(), 6),
                          round(bb.xMaximum(), 6), round(bb.yMaximum(), 6)),
            "parsed": parse_envelope(wkb),
            "wkb_len": len(wkb) if wkb else 0,
        }
    ctx.data["probe_collection"] = probe

    flog(
        f"tx_geom_wkb_roundtrip_types run end: trace_id={ctx.trace_id} "
        f"fetched={ctx.data['fetched_count']} "
        f"probe_empty={ctx.data['probe_empty']} "
        f"probe_collection={probe}",
        "INFO",
    )

    conn.close()
    ctx.data["conn"] = None


def _bbox_close(a, b, tol=1e-6):
    if a is None or b is None:
        return False
    return all(abs(x - y) <= tol for x, y in zip(a, b))


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    seeded = ctx.data.get("seeded") or {}
    results = ctx.data.get("results") or {}

    out.append((
        "all_events_fetched",
        ctx.data.get("fetched_count") == len(_CATALOGUE),
        f"fetched={ctx.data.get('fetched_count')} attendu={len(_CATALOGUE)}: "
        f"le journal doit rendre tous les evenements geometriques",
    ))

    # ===== Aller-retour par geometrie ===================================
    for key, _wkt, family in _CATALOGUE:
        info = seeded.get(key, {})
        res = results.get(key, {})
        sig_in = info.get("sig_in") or {}
        sig_out = res.get("sig_out") or {}

        out.append((
            f"{key}__captured",
            info.get("wkb_len", 0) > 0,
            f"{key}: geometry_to_wkb a renvoye {info.get('wkb_len')} octets "
            f"pour {info.get('wkt')} - une geometrie valide n'a pas ete "
            f"capturee, l'evenement est journalise sans geometrie",
        ))
        out.append((
            f"{key}__bytes_survive_the_journal",
            res.get("bytes_identical") is True,
            f"{key}: les octets relus du BLOB different de ceux ecrits "
            f"(len_in={info.get('wkb_len')}); la geometrie restauree ne sera "
            f"pas celle qui a ete capturee",
        ))
        out.append((
            f"{key}__wkb_type_preserved",
            sig_out.get("wkb_type") == sig_in.get("wkb_type"),
            f"{key}: type WKB {sig_in.get('type_name')} -> "
            f"{sig_out.get('type_name')}. Un changement de type fait perdre "
            f"Z/M (altitude, mesure) ou transforme une courbe en polyligne; "
            f"la restauration ecrit alors une autre geometrie que l'originale",
        ))
        if family == "surface":
            out.append((
                f"{key}__area_preserved",
                sig_out.get("area") == sig_in.get("area"),
                f"{key}: aire {sig_in.get('area')} -> {sig_out.get('area')} "
                f"(wkt_out={sig_out.get('wkt')})",
            ))
        elif family == "linear":
            out.append((
                f"{key}__length_preserved",
                sig_out.get("length") == sig_in.get("length"),
                f"{key}: longueur {sig_in.get('length')} -> "
                f"{sig_out.get('length')} (wkt_out={sig_out.get('wkt')})",
            ))
        out.append((
            f"{key}__reserialisation_is_stable",
            res.get("reserialised_identical") is True,
            f"{key}: re-serialiser la geometrie reconstruite ne redonne pas "
            f"le WKB d'origine; une capture -> restauration -> capture "
            f"derive a chaque tour",
        ))

        # Enveloppe: soit exacte, soit None (inconnu = garde conservatrice).
        env = res.get("envelope")
        if key in _CURVE_KEYS:
            ok_env = env is None
            msg_env = (
                f"{key}: parse_envelope a renvoye {env} pour un type courbe "
                f"non supporte. La doc du module impose None (enveloppe "
                f"inconnue -> evenement conserve); une valeur approximative "
                f"fait disparaitre l'evenement du filtre spatial du Time Lens"
            )
        else:
            ok_env = _bbox_close(env, sig_in.get("bbox"))
            msg_env = (
                f"{key}: enveloppe pure-Python {env} != bbox QGIS "
                f"{sig_in.get('bbox')}; le filtre spatial du Time Lens "
                f"selectionne les mauvais evenements"
            )
        out.append((f"{key}__envelope_matches_or_unknown", ok_env, msg_env))

    # ===== Sonde 1: VIDE vs NULL ========================================
    probe = ctx.data.get("probe_empty") or {}
    out.append((
        "empty_geometry_is_distinguishable_from_null",
        not (probe.get("wkb_from_null") is None
             and probe.get("wkb_from_empty") is None),
        f"geometry_to_wkb(NULL)={probe.get('wkb_from_null')!r} et "
        f"geometry_to_wkb(POLYGON EMPTY)={probe.get('wkb_from_empty')!r}: "
        f"les deux etats sont journalises a l'identique alors que le WKB "
        f"d'une geometrie vide existe ({probe.get('raw_wkb_empty_len')} "
        f"octets). Consequence: restaurer un evenement dont la geometrie "
        f"etait vide ne remet pas la feature dans cet etat - "
        f"restore_updated_feature teste `event.geometry_wkb is not None`, "
        f"saute l'ecriture et rapporte quand meme 'Reverted'.",
    ))

    # ===== Sonde 2: enveloppe des collections ===========================
    coll = ctx.data.get("probe_collection") or {}
    plain = coll.get("plain") or {}
    mixed = coll.get("mixed") or {}
    out.append((
        "collection_envelope_is_exact_when_children_are_supported",
        _bbox_close(plain.get("parsed"), plain.get("qgis_bbox")),
        f"GEOMETRYCOLLECTION(LINESTRING, POINT): enveloppe "
        f"{plain.get('parsed')} != bbox QGIS {plain.get('qgis_bbox')}",
    ))
    mixed_env = mixed.get("parsed")
    out.append((
        "collection_with_curve_child_is_not_silently_wrong",
        mixed_env is None or _bbox_close(mixed_env, mixed.get("qgis_bbox")),
        f"GEOMETRYCOLLECTION(CIRCULARSTRING, POINT): parse_envelope rend "
        f"{mixed_env} alors que la vraie emprise est {mixed.get('qgis_bbox')}. "
        f"L'enfant courbe n'est pas decode, mais le curseur du lecteur reste "
        f"plante au milieu de ses octets, donc les enfants suivants sont lus "
        f"n'importe ou. Le filtre spatial du Time Lens croit cette emprise: "
        f"l'evenement disparait de la zone ou y apparait a tort.",
    ))

    # ===== Propagation du trace_id ======================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_geom_wkb_roundtrip_types.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
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
