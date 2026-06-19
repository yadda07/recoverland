# RecoverLand validation fixtures (LOCAL ONLY)

This directory is `.gitignore`d on purpose. Fixtures are regenerated on
demand from `create_fixtures.py`, which is the authoritative source of
truth for the dataset shape.

## Tested versions

- GDAL / OGR : 3.8+
- QGIS : 3.40 LTR, 4.x stable
- SQLite : 3.40+

Running on older GDAL may produce binary-different fixtures; that's
expected. Scenarios consume the fixtures by content, not by hash.

## Layout

- `shapefile/points5.shp[+ .shx, .dbf, .prj]` : 5 point features, 2 attrs.
- `shapefile/polygons3.shp[...]` : 3 polygons including one with hole.
- `gpkg/test.gpkg`                              : same data, single GPKG.
- `postgres/init.sql`                           : DDL stub for runtime PG tests.
- `golden/`                                      : golden log patterns per scenario.

## How to (re)create

```python
python scripts/validation/fixtures/create_fixtures.py
```

The script is deterministic by-content (same seeds + same WKT specs).
Binary differences across machines/GDAL versions are tolerated; scenarios
compare features, not bytes.

## How to run a scenario consuming fixtures

```python
from scripts.validation.fixtures import paths
layer = QgsVectorLayer(paths.GPKG_POINTS, "p11_points", "ogr")
# ...
```

## Golden log patterns

Each entry in `golden/<scenario_id>.txt` is one regex pattern that must
match at least once in the scenario's recoverland_debug.log slice. Patterns
explicitly avoid `trace_id` and other volatile fields.

## Postgres runtime

`postgres/init.sql` is a DDL stub; runtime tests are deferred to
`BL-RW-P2-10-DB-FOLLOWUP`. To run locally, point a Postgres connection at
the schema defined there and set the env var `RECOVERLAND_TEST_PG_URI`.

## Antithese guarantees

- **Determinism by content** : running the script twice in a row produces
  the same feature set (count, attr values, WKT). Binary equality NOT
  guaranteed (timestamps, internal ordering).
- **No trace_id in golden patterns** : golden patterns contain only stable
  prefixes/keys. Trace_id is asserted separately by the scenario.
- **Schema drift sentinel** : if `create_fixtures.py` is run with a
  GDAL/QGIS combination that produces a fundamentally different schema
  (e.g. GPKG version bump), the README must be updated explicitly.
