"""Canonical paths for the validation fixtures.

Scenarios should never hard-code paths. Always go through
`from scripts.validation.fixtures import paths` and use the constants
below.
"""
from __future__ import annotations

from pathlib import Path

_HERE = Path(__file__).resolve().parent

SHAPEFILE_DIR = _HERE / "shapefile"
GPKG_DIR = _HERE / "gpkg"
POSTGRES_DIR = _HERE / "postgres"
GOLDEN_DIR = _HERE / "golden"

SHP_POINTS = str(SHAPEFILE_DIR / "points5.shp")
SHP_POLYGONS = str(SHAPEFILE_DIR / "polygons3.shp")

GPKG_PATH = str(GPKG_DIR / "test.gpkg")
GPKG_POINTS_URI = f"{GPKG_PATH}|layername=points5"
GPKG_POLYGONS_URI = f"{GPKG_PATH}|layername=polygons3"

POSTGRES_INIT_SQL = str(POSTGRES_DIR / "init.sql")


def fixtures_present() -> bool:
    """Return True if all expected fixture files are present.

    Scenarios should call this and skip with a clear message if False,
    rather than crashing on a missing file.
    """
    required = [
        SHAPEFILE_DIR / "points5.shp",
        SHAPEFILE_DIR / "points5.dbf",
        SHAPEFILE_DIR / "polygons3.shp",
        SHAPEFILE_DIR / "polygons3.dbf",
        GPKG_DIR / "test.gpkg",
    ]
    return all(p.exists() for p in required)
