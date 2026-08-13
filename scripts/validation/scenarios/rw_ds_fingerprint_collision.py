"""rw_ds_fingerprint_collision - RecoverLand validation runtime.

Invariant: I-3. One datasource fingerprint names one table.
Root cause: `identity._normalize_db_source` captures the SCHEMA where it
means the TABLE.

The hole this scenario proves
-----------------------------
QGIS writes a PostGIS source as::

    ... dbname='gis' table="public"."routes" (geom) sql=

`_normalize_db_source` looks for ``table="([^"]*)"`` and stops at the first
closing quote, so it captures ``public``. And ``schema`` is not a separate
token in a QGIS URI, so it falls back to its default -- also ``public``.
Every table of a schema therefore collapses onto one fingerprint:

    postgres::host=... schema=public table=public     <- routes
    postgres::host=... schema=public table=public     <- rivieres

Consequences, all silent:
  * `rewind_dedup._entity_key` is ``datasource::entity``, so a feature
    ``pk:gid=5`` in routes and another ``pk:gid=5`` in rivieres share a dedup
    bucket. An INSERT in one and a DELETE in the other read as one entity
    created then destroyed -- a net no-op -- and NEITHER is restored;
  * `datasource_registry.register_datasource` upserts on the fingerprint, so
    one row survives for N tables and a layer rebuilt from the registry can
    be the wrong table;
  * the Version tab lists one line for N tables.

Correcting the fingerprint rewrites a value stored in every journal row, so
it needs a migration and is deliberately NOT done here. What ships now is the
guard: `identity.find_fingerprint_collisions` plus a refusal in the rewind
entry point, so the engine never operates on a merged identity.

Scenario layout (no database needed -- the defect is in string
normalisation, so real QGIS URIs are enough):
    case 1: two PostGIS URIs, two tables, one schema -> same fingerprint
    case 2: the same for Oracle and MSSQL
    case 3: two file layers with distinct sources -> no collision (the guard
            must not fire on healthy projects)

Pre-patch verdict: FAIL (no detector, no refusal).
Post-patch verdict: PASS -- and case 1 still shows the collision, because
the fingerprint itself is not fixed yet. That assertion is the reminder.
"""
from __future__ import annotations

import re
from pathlib import Path

SCENARIO_ID = "rw_ds_fingerprint_collision"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_PG_ROUTES = (
    "dbname='gis' host=localhost port=5432 user='u' "
    'table="public"."routes" (geom) sql='
)
_PG_RIVIERES = (
    "dbname='gis' host=localhost port=5432 user='u' "
    'table="public"."rivieres" (geom) sql='
)
_ORA_A = 'host=srv port=1521 dbname=ORCL table="GEO"."PARCELLE" (GEOM)'
_ORA_B = 'host=srv port=1521 dbname=ORCL table="GEO"."BATIMENT" (GEOM)'


class _FakeLayer:
    """Minimal stand-in: fingerprinting only reads provider name + source."""

    def __init__(self, provider: str, source: str, name: str):
        self._provider = provider
        self._source = source
        self._name = name

    def dataProvider(self):
        outer = self

        class _P:
            def name(self_inner):
                return outer._provider
        return _P()

    def source(self):
        return self._source

    def name(self):
        return self._name


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["pg_layers"] = [
        _FakeLayer("postgres", _PG_ROUTES, "routes"),
        _FakeLayer("postgres", _PG_RIVIERES, "rivieres"),
    ]
    ctx.data["ora_layers"] = [
        _FakeLayer("oracle", _ORA_A, "PARCELLE"),
        _FakeLayer("oracle", _ORA_B, "BATIMENT"),
    ]
    ctx.data["file_layers"] = [
        _FakeLayer("ogr", "C:/data/a.gpkg|layername=routes", "routes"),
        _FakeLayer("ogr", "C:/data/a.gpkg|layername=rivieres", "rivieres"),
    ]
    flog(
        f"rw_ds_fingerprint_collision setup: trace_id={ctx.trace_id} "
        f"pg=2 oracle=2 file=2",
        "INFO",
    )


def run(ctx):
    from recoverland.core.identity import (
        compute_datasource_fingerprint, find_fingerprint_collisions,
    )
    from recoverland.core.logger import flog

    flog(f"rw_ds_fingerprint_collision run start: trace_id={ctx.trace_id}", "INFO")

    for key in ("pg_layers", "ora_layers", "file_layers"):
        layers = ctx.data[key]
        ctx.data[f"{key}_fps"] = [compute_datasource_fingerprint(x) for x in layers]
        ctx.data[f"{key}_collisions"] = find_fingerprint_collisions(layers)

    flog(
        f"rw_ds_fingerprint_collision run end: trace_id={ctx.trace_id} "
        f"pg_fps={ctx.data['pg_layers_fps']} "
        f"ora_fps={ctx.data['ora_layers_fps']} "
        f"file_collisions={ctx.data['file_layers_collisions']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    pg_fps = ctx.data.get("pg_layers_fps") or []
    ora_fps = ctx.data.get("ora_layers_fps") or []

    # ===== The defect, still present and now documented ==================
    out.append((
        "postgis_tables_still_share_a_fingerprint",
        len(pg_fps) == 2 and pg_fps[0] == pg_fps[1],
        f"fingerprints={pg_fps}: public.routes and public.rivieres normalise "
        f"to the same value. Kept as an assertion so the day the fingerprint "
        f"is fixed (with its migration) this scenario fails and forces the "
        f"guard and the docs to be revisited.",
    ))
    out.append((
        "fingerprint_captures_the_schema_not_the_table",
        bool(pg_fps) and "table=public" in pg_fps[0],
        f"fingerprint={pg_fps[0] if pg_fps else None!r}: the captured table is "
        f"'public', i.e. the schema -- the regex stops at the first closing "
        f"double quote of table=\"public\".\"routes\"",
    ))
    out.append((
        "oracle_is_affected_too",
        len(ora_fps) == 2 and ora_fps[0] == ora_fps[1],
        f"fingerprints={ora_fps}: GEO.PARCELLE and GEO.BATIMENT collide",
    ))

    # ===== The guard =====================================================
    pg_col = ctx.data.get("pg_layers_collisions") or {}
    out.append((
        "collision_detected_on_postgis",
        len(pg_col) == 1 and sorted(next(iter(pg_col.values()))) == ["rivieres", "routes"],
        f"collisions={pg_col}: the detector must name both layers so the user "
        f"can tell which ones are indistinguishable",
    ))
    out.append((
        "collision_detected_on_oracle",
        len(ctx.data.get("ora_layers_collisions") or {}) == 1,
        f"collisions={ctx.data.get('ora_layers_collisions')}",
    ))
    out.append((
        "no_false_positive_on_healthy_layers",
        ctx.data.get("file_layers_collisions") == {},
        f"collisions={ctx.data.get('file_layers_collisions')}: two layers of "
        f"one GeoPackage have distinct sources and must NOT be flagged, or the "
        f"guard blocks ordinary projects",
    ))

    # ===== The rewind entry point must actually refuse ===================
    src = (_PLUGIN_ROOT / "recover_dialog.py").read_text(
        encoding="utf-8", errors="replace")
    out.append((
        "dialog_defines_the_refusal",
        bool(re.search(r"def\s+_refuse_fingerprint_collision\b", src)),
        "recover_dialog.py must define _refuse_fingerprint_collision",
    ))
    start = src.find("def _recover_version_mode(")
    fetch = src.find("VersionFetchThread(", start)
    body = src[start:fetch] if start >= 0 and fetch > start else ""
    out.append((
        "refusal_runs_before_the_fetch",
        "_refuse_fingerprint_collision" in body,
        "the check must run before the rewind fetch is launched; refusing "
        "after the events are read would still have planned the work",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"rw_ds_fingerprint_collision.*trace_id={ctx.trace_id}",
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
