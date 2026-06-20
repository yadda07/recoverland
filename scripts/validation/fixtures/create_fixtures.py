"""Regenerate the deterministic validation fixtures (BL-RW-P2-11).

This script is the authoritative source of truth for the test dataset
shape. It writes:

  - `shapefile/points5.shp`    : 5 point features, attrs (name, value).
  - `shapefile/polygons3.shp`  : 3 polygons, one with a hole.
  - `gpkg/test.gpkg`            : same data, both layers, single GPKG.
  - `postgres/init.sql`         : DDL stub (no runtime; cf. P2-10 followup).
  - `golden/*.txt`              : pattern files per scenario (placeholder).

Determinism: the produced files have the same FEATURE CONTENT across
runs (count, attrs, WKT). Binary equality across machines/GDAL versions
is NOT guaranteed and is not a goal.

Usage:
    python -m scripts.validation.fixtures.create_fixtures
or
    python scripts/validation/fixtures/create_fixtures.py

The script must be runnable both with `python -m` (relative imports
allowed) and as a standalone file (direct invocation).
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
SHAPEFILE_DIR = _HERE / "shapefile"
GPKG_DIR = _HERE / "gpkg"
POSTGRES_DIR = _HERE / "postgres"
GOLDEN_DIR = _HERE / "golden"


# === Authoritative specs =====================================================

POINTS_SPEC = [
    # (fid_hint, name, value, wkt)
    (1, "p_alpha",   10, "POINT(0 0)"),
    (2, "p_beta",    20, "POINT(1 0)"),
    (3, "p_gamma",   30, "POINT(0 1)"),
    (4, "p_delta",   40, "POINT(2 2)"),
    (5, "p_epsilon", 50, "POINT(-1 -1)"),
]

POLYGONS_SPEC = [
    (1, "poly_square", 100,
        "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
    (2, "poly_with_hole", 200,
        "POLYGON((0 0, 20 0, 20 20, 0 20, 0 0),"
        "(5 5, 15 5, 15 15, 5 15, 5 5))"),
    (3, "poly_triangle", 300,
        "POLYGON((30 0, 40 0, 35 10, 30 0))"),
]


# === Writers =================================================================

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_shp(path: Path, geom_type, layer_name: str, spec) -> None:
    from osgeo import ogr, osr

    _ensure_dir(path.parent)
    if path.exists():
        # Remove any stale .shp + sidecars so the writer starts fresh.
        for ext in (".shp", ".shx", ".dbf", ".prj", ".cpg"):
            p = path.with_suffix(ext)
            if p.exists():
                p.unlink()

    driver = ogr.GetDriverByName("ESRI Shapefile")
    if driver is None:
        raise RuntimeError("ESRI Shapefile driver not available")
    ds = driver.CreateDataSource(str(path))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    lyr = ds.CreateLayer(layer_name, srs=srs, geom_type=geom_type)
    lyr.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("value", ogr.OFTInteger))
    for fid_hint, name, value, wkt in spec:
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetField("name", name)
        feat.SetField("value", int(value))
        feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
        lyr.CreateFeature(feat)
        feat = None
    ds.FlushCache()
    ds = None


def _write_gpkg(path: Path) -> None:
    from osgeo import ogr, osr

    _ensure_dir(path.parent)
    if path.exists():
        path.unlink()

    driver = ogr.GetDriverByName("GPKG")
    if driver is None:
        raise RuntimeError("GPKG driver not available")
    ds = driver.CreateDataSource(str(path))
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    lyr_pts = ds.CreateLayer("points5", srs=srs, geom_type=ogr.wkbPoint)
    lyr_pts.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    lyr_pts.CreateField(ogr.FieldDefn("value", ogr.OFTInteger))
    for _, name, value, wkt in POINTS_SPEC:
        feat = ogr.Feature(lyr_pts.GetLayerDefn())
        feat.SetField("name", name)
        feat.SetField("value", int(value))
        feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
        lyr_pts.CreateFeature(feat)
        feat = None

    lyr_poly = ds.CreateLayer("polygons3", srs=srs, geom_type=ogr.wkbPolygon)
    lyr_poly.CreateField(ogr.FieldDefn("name", ogr.OFTString))
    lyr_poly.CreateField(ogr.FieldDefn("value", ogr.OFTInteger))
    for _, name, value, wkt in POLYGONS_SPEC:
        feat = ogr.Feature(lyr_poly.GetLayerDefn())
        feat.SetField("name", name)
        feat.SetField("value", int(value))
        feat.SetGeometry(ogr.CreateGeometryFromWkt(wkt))
        lyr_poly.CreateFeature(feat)
        feat = None

    ds.FlushCache()
    ds = None


def _write_postgres_init() -> None:
    _ensure_dir(POSTGRES_DIR)
    target = POSTGRES_DIR / "init.sql"
    sql = (
        "-- BL-RW-P2-11 / BL-RW-P2-10-DB-FOLLOWUP\n"
        "-- DDL stub for the Postgres runtime validation scenario.\n"
        "-- Apply against an empty test database before running scenarios:\n"
        "--   psql -h <host> -U <user> -d <db> -f init.sql\n"
        "-- Then set RECOVERLAND_TEST_PG_URI in your shell.\n"
        "\n"
        "CREATE EXTENSION IF NOT EXISTS postgis;\n"
        "\n"
        "DROP TABLE IF EXISTS recoverland_test.points5 CASCADE;\n"
        "DROP TABLE IF EXISTS recoverland_test.polygons3 CASCADE;\n"
        "DROP SCHEMA IF EXISTS recoverland_test CASCADE;\n"
        "\n"
        "CREATE SCHEMA recoverland_test;\n"
        "\n"
        "CREATE TABLE recoverland_test.points5 (\n"
        "    fid SERIAL PRIMARY KEY,\n"
        "    name TEXT NOT NULL,\n"
        "    value INTEGER NOT NULL,\n"
        "    geom geometry(Point, 4326)\n"
        ");\n"
        "\n"
        "CREATE TABLE recoverland_test.polygons3 (\n"
        "    fid SERIAL PRIMARY KEY,\n"
        "    name TEXT NOT NULL,\n"
        "    value INTEGER NOT NULL,\n"
        "    geom geometry(Polygon, 4326)\n"
        ");\n"
        "\n"
        "INSERT INTO recoverland_test.points5 (name, value, geom) VALUES\n"
    )
    for _, name, value, wkt in POINTS_SPEC:
        sql += (
            f"    ('{name}', {value}, ST_GeomFromText('{wkt}', 4326)),\n"
        )
    sql = sql.rstrip(",\n") + ";\n\n"
    sql += "INSERT INTO recoverland_test.polygons3 (name, value, geom) VALUES\n"
    for _, name, value, wkt in POLYGONS_SPEC:
        sql += (
            f"    ('{name}', {value}, ST_GeomFromText('{wkt}', 4326)),\n"
        )
    sql = sql.rstrip(",\n") + ";\n"
    target.write_text(sql, encoding="utf-8")


def _write_golden_logs() -> None:
    """Write per-scenario golden log pattern files.

    Each pattern is one regex per line. Lines starting with `#` are
    comments. Scenarios assert that each non-comment pattern matches at
    least once in their captured log slice (trace_id is asserted separately).
    """
    _ensure_dir(GOLDEN_DIR)
    patterns = {
        "provider_memory.txt": [
            r"EditSessionTracker\.connect_layer:.*provider=memory.*"
            r"action=refused.*reason=no_stable_identity",
            r"validate_rewind:\s+layer=\S+\s+provider=memory\s+driver=memory"
            r"\s+identity_strength=none\s+score=100",
        ],
        "provider_gpkg.txt": [
            r"EditSessionTracker\.connect_layer:.*driver=GPKG.*"
            r"action=accepted",
            r"validate_rewind:\s+layer=\S+\s+provider=ogr\s+driver=GPKG"
            r"\s+identity_strength=strong\s+score=100",
        ],
        "provider_shp.txt": [
            r"EditSessionTracker\.connect_layer:.*driver=ESRI Shapefile.*"
            r"action=accepted_untested",
            r"validate_rewind:\s+layer=\S+\s+provider=ogr\s+driver=ESRI "
            r"Shapefile\s+identity_strength=medium\s+score=100",
        ],
        "i2_tracker_suppress.txt": [
            r"EditSessionTracker:\s+signal=\S+\s+ignored=suppressed",
        ],
        "i8_rewind_idempotence.txt": [
            r"rewind:.*idempotent",
        ],
        "i9_cutoff_inclusivity.txt": [
            r"event_stream_repository.*inclusive=True",
        ],
        "log_format_audit.txt": [
            r"BUF_INS\s+layer_id=\S+",
            r"BUF_DEL\s+layer_id=\S+",
            r"BUF_UPD\s+layer_id=\S+",
        ],
        "p13_fingerprint_portable.txt": [
            r"compute_datasource_fingerprint.*mode=(absolute|relative)",
        ],
        "p17_dedup_fid_collision.txt": [
            r"collapse_rewind_events_with_stats.*recycled_fids",
        ],
        "p18_makevalid_drift.txt": [
            r"makevalid.*drift_linf=\S+",
        ],
    }
    for name, lines in patterns.items():
        target = GOLDEN_DIR / name
        body = ["# RecoverLand golden log patterns — BL-RW-P2-11"]
        body.append(
            "# One regex per line, must match at least once in the "
            "scenario log slice."
        )
        body.append(
            "# Trace_id is volatile; assert it separately, not here."
        )
        body.append("")
        body.extend(lines)
        target.write_text("\n".join(body) + "\n", encoding="utf-8")


# === Determinism antithese ===================================================

def _antithese_determinism_check() -> dict:
    """Run create_fixtures twice in temp scratch dirs and compare CONTENT.

    Returns a dict with the antithese verdict. NOT bit-equality:
    we compare the feature payload (counts + attrs + WKT) since binary
    determinism depends on GDAL/timestamps which we deliberately tolerate.
    """
    import tempfile

    from osgeo import ogr

    out = {
        "passes": 0,
        "fails": 0,
        "details": [],
    }

    def _read_features(shp_path):
        ds = ogr.Open(shp_path)
        lyr = ds.GetLayer(0)
        rows = []
        for feat in lyr:
            geom = feat.GetGeometryRef()
            rows.append((
                feat.GetField("name"),
                feat.GetField("value"),
                geom.ExportToWkt() if geom is not None else None,
            ))
        ds = None
        return rows

    with tempfile.TemporaryDirectory(prefix="rl_fix_antith_") as tmp:
        for run_label in ("run1", "run2"):
            shp_pts = Path(tmp) / run_label / "points5.shp"
            _write_shp(shp_pts, ogr.wkbPoint, "points5", POINTS_SPEC)
            shp_poly = Path(tmp) / run_label / "polygons3.shp"
            _write_shp(shp_poly, ogr.wkbPolygon, "polygons3", POLYGONS_SPEC)
        rows1_p = _read_features(str(Path(tmp) / "run1" / "points5.shp"))
        rows2_p = _read_features(str(Path(tmp) / "run2" / "points5.shp"))
        rows1_g = _read_features(str(Path(tmp) / "run1" / "polygons3.shp"))
        rows2_g = _read_features(str(Path(tmp) / "run2" / "polygons3.shp"))

    if rows1_p == rows2_p:
        out["passes"] += 1
        out["details"].append("points5: identical content across runs")
    else:
        out["fails"] += 1
        out["details"].append(
            f"points5: DRIFT — run1={rows1_p!r} run2={rows2_p!r}"
        )
    if rows1_g == rows2_g:
        out["passes"] += 1
        out["details"].append("polygons3: identical content across runs")
    else:
        out["fails"] += 1
        out["details"].append(
            f"polygons3: DRIFT — run1={rows1_g!r} run2={rows2_g!r}"
        )

    return out


# === Main entry point ========================================================

def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    try:
        from osgeo import ogr  # noqa: F401  (probe)
    except ImportError as exc:
        print(f"ERROR: GDAL Python bindings not available: {exc}",
              file=sys.stderr)
        return 2

    print(f"[create_fixtures] target dir: {_HERE}")
    _ensure_dir(SHAPEFILE_DIR)
    _ensure_dir(GPKG_DIR)

    print("[create_fixtures] writing shapefile/points5.shp ...")
    _write_shp(SHAPEFILE_DIR / "points5.shp",
               ogr.wkbPoint, "points5", POINTS_SPEC)
    print("[create_fixtures] writing shapefile/polygons3.shp ...")
    _write_shp(SHAPEFILE_DIR / "polygons3.shp",
               ogr.wkbPolygon, "polygons3", POLYGONS_SPEC)
    print("[create_fixtures] writing gpkg/test.gpkg ...")
    _write_gpkg(GPKG_DIR / "test.gpkg")
    print("[create_fixtures] writing postgres/init.sql ...")
    _write_postgres_init()
    print("[create_fixtures] writing golden/*.txt ...")
    _write_golden_logs()

    if "--no-antithese" not in argv:
        print("[create_fixtures] running antithese determinism check ...")
        verdict = _antithese_determinism_check()
        print(f"[create_fixtures] antithese: passes={verdict['passes']} "
              f"fails={verdict['fails']}")
        for line in verdict["details"]:
            print(f"  - {line}")
        if verdict["fails"] > 0:
            print("ERROR: determinism antithese FAILED", file=sys.stderr)
            return 1

    print("[create_fixtures] DONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
