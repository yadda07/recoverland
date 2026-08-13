"""stress_verify.py - Did the rewind actually put the data back?

Companion to stress_edit.py, which writes a full ground truth of every
layer to ``stress_snapshot_latest.json`` BEFORE it starts breaking things.
This script answers the only question that matters after a Rewind, and the
one the plugin cannot answer today:

    is the data identical to what it was before the stress?

The plugin counts operations ("126 restaurees, 24 echouees"). That number
says how many compensations were applied, not whether the layer came back.
A rewind can report a hundred successes and still leave an intermediate
state. This compares content, feature by feature.

Run it from the QGIS Python console, after a Rewind:

    exec(open('C:/Users/yadda/AppData/Roaming/QGIS/QGIS4/profiles/default'
              '/python/plugins/recoverland/scripts/stress_verify.py').read())

Matching is by CONTENT, never by FID: OGR renumbers records after a
deletion, so a feature that came back correctly usually carries a
different FID. Two features are the same when their geometry and their
compared attributes match. Fields listed in IGNORE_FIELDS are excluded --
put your primary keys and any provider-managed column there.
"""

import json
import os
from datetime import datetime

from qgis.core import QgsProject, QgsVectorLayer

# --------------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------------
# Columns never compared: primary keys, provider-managed values, anything
# the database rewrites on its own. A restored feature legitimately gets a
# new gid, and comparing it would flag every single row as different.
IGNORE_FIELDS = {"gid", "fid", "ogc_fid", "objectid", "id"}

# Coordinate tolerance in layer units. Geometries are compared on their WKT
# rounded to this many decimals; 6 is ~0.1 mm in a metric CRS.
GEOM_DECIMALS = 6

# Show at most this many differing features per layer.
MAX_SAMPLES = 5

SNAPSHOT_NAME = "stress_snapshot_latest.json"

# Above this, the snapshot is not loaded at all. Stress fewer
# LAYERS instead of comparing fewer features.
MAX_SNAPSHOT_MB = 40


def _snapshot_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), SNAPSHOT_NAME)


def _norm_value(val):
    """Comparable form of an attribute value.

    NULL, empty string and None all read the same on screen but are three
    different things in a provider; they are kept distinct here, because a
    restore that turns a value into NULL is exactly the kind of loss this
    script exists to catch.
    """
    if val is None:
        return None
    if hasattr(val, "isNull") and val.isNull():
        return None
    if isinstance(val, float):
        return round(val, GEOM_DECIMALS)
    if isinstance(val, (int, str, bool)):
        return val
    return str(val)


def _feature_key(attrs, geom_wkt, compared_fields):
    """Content signature of a feature, independent of its FID."""
    parts = [f"{name}={_norm_value(attrs.get(name))!r}"
             for name in compared_fields]
    parts.append(f"geom={geom_wkt or ''}")
    return "|".join(parts)


def _live_features(layer, compared_fields):
    out = {}
    for feat in layer.getFeatures():
        attrs = {f.name(): feat[f.name()] for f in feat.fields()}
        geom = feat.geometry()
        wkt = geom.asWkt(GEOM_DECIMALS) if geom and not geom.isNull() else None
        key = _feature_key(attrs, wkt, compared_fields)
        out.setdefault(key, []).append(feat.id())
    return out


def _snapshot_features(entry, compared_fields):
    out = {}
    for raw in entry.get("features", {}).values():
        key = _feature_key(raw.get("attrs", {}), raw.get("geom_wkt"),
                           compared_fields)
        out.setdefault(key, []).append(raw.get("fid"))
    return out


def _describe(key, limit=160):
    return key[:limit] + ("..." if len(key) > limit else "")


def verify():
    path = _snapshot_path()
    if not os.path.isfile(path):
        print(f"[verify] no snapshot at {path}")
        print("[verify] run stress_edit.py first: it writes the ground truth.")
        return

    # json.load builds the whole file as Python objects, several times its
    # size in RAM, inside the QGIS process. A 193 MB snapshot -- what you get
    # when every layer of a real project is photographed -- freezes QGIS
    # before printing a single line. Refuse rather than hang, and say what to
    # do about it: the fix is to stress FEWER LAYERS, never to compare fewer
    # features, which would hide exactly what this script exists to find.
    size_mb = os.path.getsize(path) / (1024 * 1024)
    if size_mb > MAX_SNAPSHOT_MB:
        print(f"[verify] snapshot is {size_mb:.0f} MB "
              f"(limit {MAX_SNAPSHOT_MB} MB) -- NOT loading it.")
        print("[verify] loading it would take gigabytes of RAM in QGIS.")
        print("[verify] Lower MAX_LAYERS in stress_edit.py and run the whole")
        print("[verify] cycle again: snapshot, stress, rewind, verify.")
        print("[verify] Do NOT raise this limit to force it through: a")
        print("[verify] verification that freezes QGIS proves nothing.")
        return
    print(f"[verify] reading snapshot ({size_mb:.1f} MB)...")
    with open(path, "r", encoding="utf-8") as fh:
        snap = json.load(fh)

    print("=" * 72)
    print(f"[verify] snapshot taken {snap.get('snapshot_time', '?')}")
    print(f"[verify] checked at     {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 72)

    by_name = {}
    for layer in QgsProject.instance().mapLayers().values():
        if isinstance(layer, QgsVectorLayer):
            by_name[layer.name()] = layer

    total_missing = total_extra = 0
    perfect, degraded, absent = [], [], []

    for name, entry in sorted(snap.get("layers", {}).items()):
        layer = by_name.get(name)
        if layer is None:
            absent.append(name)
            print(f"\n  {name}: LAYER NOT LOADED, skipped")
            continue

        compared = sorted(
            f.name() for f in layer.fields()
            if f.name().lower() not in IGNORE_FIELDS
        )
        live = _live_features(layer, compared)
        want = _snapshot_features(entry, compared)

        missing, extra = [], []
        for key, fids in want.items():
            gap = len(fids) - len(live.get(key, []))
            if gap > 0:
                missing.append((key, gap))
        for key, fids in live.items():
            gap = len(fids) - len(want.get(key, []))
            if gap > 0:
                extra.append((key, gap))

        n_missing = sum(n for _k, n in missing)
        n_extra = sum(n for _k, n in extra)
        total_missing += n_missing
        total_extra += n_extra

        expected = entry.get("feature_count", 0)
        got = layer.featureCount()
        if n_missing == 0 and n_extra == 0:
            perfect.append(name)
            print(f"\n  {name}: OK  {got} feature(s), identical to the snapshot")
            continue

        degraded.append(name)
        print(f"\n  {name}: DIFFERS  snapshot={expected} now={got}")
        print(f"      {n_missing} feature(s) NOT back as they were")
        print(f"      {n_extra} feature(s) present that should not be")
        for key, n in missing[:MAX_SAMPLES]:
            print(f"      - missing x{n}: {_describe(key)}")
        for key, n in extra[:MAX_SAMPLES]:
            print(f"      + extra   x{n}: {_describe(key)}")
        if len(missing) > MAX_SAMPLES or len(extra) > MAX_SAMPLES:
            print(f"      ... ({len(missing)} missing / {len(extra)} extra "
                  f"distinct signatures)")

    print("\n" + "=" * 72)
    if not degraded and not absent:
        print("[verify] VERDICT: the rewind put every layer back exactly.")
    else:
        print(f"[verify] VERDICT: {len(perfect)} layer(s) restored exactly, "
              f"{len(degraded)} NOT restored, {len(absent)} not loaded.")
        print(f"[verify] {total_missing} feature(s) missing or altered, "
              f"{total_extra} feature(s) in excess.")
        if degraded:
            print(f"[verify] look at: {', '.join(degraded)}")
        print("[verify] A layer listed here did NOT come back to its previous")
        print("[verify] state, whatever the plugin reported. Send this output")
        print("[verify] with the debug log: the pair identifies the mechanism.")
    print("=" * 72)


verify()
