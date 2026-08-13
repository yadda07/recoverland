"""rw_ds_fingerprint_collision - RecoverLand validation runtime.

Invariant: I-3. One datasource fingerprint names one table.

REECRIT AVEC LA MIGRATION v6
----------------------------
La version precedente de ce scenario affirmait, par construction, que le
defaut d'empreinte etait TOUJOURS present : deux tables d'un meme schema
PostGIS partageaient une empreinte parce que `_normalize_db_source`
lisait ``table="([^"]*)"`` et capturait ``public``, c'est-a-dire le
SCHEMA. Sa docstring demandait sa reecriture le jour ou l'empreinte
serait corrigee. Ce jour est arrive avec la v6 : l'empreinte passe par
`QgsDataSourceUri` (host / port / database / schema / table) et la
migration rattache les anciennes empreintes par alias, sans reecrire une
seule ligne de `audit_event`.

Ce que ce scenario prouve maintenant
------------------------------------
1. LA FUSION EST FINIE. `public.routes` et `public.rivieres` ne
   normalisent plus vers la meme valeur ; le jeton `table=` porte la
   TABLE et le jeton `schema=` porte le schema. Idem pour Oracle, ou le
   profil gagne un `schema` : `GEO.PARCELLE` et `AUTRE.PARCELLE` sont
   deux tables differentes, et les confondre revenait a rejouer
   l'historique d'un proprietaire sur un autre.

2. LE GARDE-FOU SERT ENCORE, ET SUR LE BON CAS. Deux couches QGIS qui
   pointent la MEME table partagent une empreinte -- c'est voulu, elles
   partagent aussi leur historique. Mais un rewind cadre sur l'une
   agirait sur les evenements captures via l'autre :
   `find_fingerprint_collisions` doit les nommer toutes les deux et
   `recover_dialog` doit refuser AVANT de lancer la lecture. Trois
   formes de ce cas sont mesurees : un filtre serveur PostGIS (`sql=`),
   un filtre d'affichage sur un GeoPackage (`|subset=`), et une
   deuxieme ouverture de la meme table sous un autre nom de couche.

3. AUCUN FAUX POSITIF. Deux couches d'un meme GeoPackage, deux tables
   d'un meme schema, deux tables de deux proprietaires Oracle : rien de
   tout cela ne doit declencher le refus, ou le garde-fou bloque les
   projets ordinaires.

Verdict attendu apres v6 : PASS.
"""
from __future__ import annotations

import re
from pathlib import Path

SCENARIO_ID = "rw_ds_fingerprint_collision"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Deux tables d'un meme schema : la fusion historique.
_PG_ROUTES = (
    "dbname='gis' host=localhost port=5432 user='u' "
    'table="public"."routes" (geom) sql='
)
_PG_RIVIERES = (
    "dbname='gis' host=localhost port=5432 user='u' "
    'table="public"."rivieres" (geom) sql='
)
# La MEME table, ouverte une seconde fois avec un filtre serveur.
_PG_ROUTES_FILTREE = (
    "dbname='gis' host=localhost port=5432 user='u' "
    'table="public"."routes" (geom) sql="cat" = \'A\''
)
# Deux proprietaires Oracle, un meme nom de table.
_ORA_A = 'host=srv port=1521 dbname=ORCL table="GEO"."PARCELLE" (GEOM)'
_ORA_B = 'host=srv port=1521 dbname=ORCL table="GEO"."BATIMENT" (GEOM)'
_ORA_C = 'host=srv port=1521 dbname=ORCL table="AUTRE"."PARCELLE" (GEOM)'

_GPKG = "C:/data/a.gpkg"


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
        _FakeLayer("oracle", _ORA_C, "PARCELLE (AUTRE)"),
    ]
    ctx.data["file_layers"] = [
        _FakeLayer("ogr", f"{_GPKG}|layername=routes", "routes"),
        _FakeLayer("ogr", f"{_GPKG}|layername=rivieres", "rivieres"),
    ]
    # Les trois facons d'ouvrir DEUX FOIS la meme table.
    ctx.data["same_table_pg"] = [
        _FakeLayer("postgres", _PG_ROUTES, "routes"),
        _FakeLayer("postgres", _PG_ROUTES_FILTREE, "routes (cat=A)"),
    ]
    ctx.data["same_table_file"] = [
        _FakeLayer("ogr", f"{_GPKG}|layername=routes", "routes"),
        _FakeLayer("ogr", f"{_GPKG}|layername=routes|subset=\"cat\" = 'A'",
                   "routes filtrees"),
    ]
    flog(
        f"rw_ds_fingerprint_collision setup: trace_id={ctx.trace_id} "
        f"pg=2 oracle=3 file=2 same_table_pg=2 same_table_file=2",
        "INFO",
    )


def run(ctx):
    from recoverland.core.identity import (
        compute_datasource_fingerprint, compute_fingerprint_for_source,
        find_fingerprint_collisions,
    )
    from recoverland.core.logger import flog

    flog(f"rw_ds_fingerprint_collision run start: trace_id={ctx.trace_id}", "INFO")

    for key in ("pg_layers", "ora_layers", "file_layers",
                "same_table_pg", "same_table_file"):
        layers = ctx.data[key]
        ctx.data[f"{key}_fps"] = [compute_datasource_fingerprint(x) for x in layers]
        ctx.data[f"{key}_collisions"] = find_fingerprint_collisions(layers)

    # La fonction unique partagee avec la migration doit rendre la meme
    # valeur que le chemin de capture, sinon les alias poses par la
    # migration ne designeraient pas ce que la capture ecrit.
    ctx.data["shared_entry_point"] = [
        compute_fingerprint_for_source("postgres", _PG_ROUTES),
        compute_fingerprint_for_source("oracle", _ORA_A),
        compute_fingerprint_for_source("ogr", f"{_GPKG}|layername=routes"),
    ]

    flog(
        f"rw_ds_fingerprint_collision run end: trace_id={ctx.trace_id} "
        f"pg_fps={ctx.data['pg_layers_fps']} "
        f"ora_fps={ctx.data['ora_layers_fps']} "
        f"same_table_pg={ctx.data['same_table_pg_collisions']} "
        f"file_collisions={ctx.data['file_layers_collisions']}",
        "INFO",
    )


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    pg_fps = ctx.data.get("pg_layers_fps") or []
    ora_fps = ctx.data.get("ora_layers_fps") or []
    file_fps = ctx.data.get("file_layers_fps") or []

    # ===== 1. La fusion est finie =======================================
    out.append((
        "postgis_tables_no_longer_share_a_fingerprint",
        len(pg_fps) == 2 and pg_fps[0] != pg_fps[1],
        f"fingerprints={pg_fps}: public.routes and public.rivieres must "
        f"normalise to two distinct values. While they collided, a feature "
        f"pk:gid=5 of routes and another of rivieres shared one dedup bucket: "
        f"an INSERT in one and a DELETE in the other read as a net no-op and "
        f"NEITHER was restored.",
    ))
    out.append((
        "fingerprint_captures_the_table_not_the_schema",
        bool(pg_fps) and "schema=public" in pg_fps[0] and "table=routes" in pg_fps[0],
        f"fingerprint={pg_fps[0] if pg_fps else None!r}: QgsDataSourceUri must "
        f"yield schema=public and table=routes, where the historical regex "
        f'stopped at the first closing quote of table="public"."routes" and '
        f"captured the schema twice.",
    ))
    out.append((
        "oracle_tables_of_one_owner_are_distinct",
        len(ora_fps) == 3 and ora_fps[0] != ora_fps[1],
        f"fingerprints={ora_fps[:2]}: GEO.PARCELLE and GEO.BATIMENT must not "
        f"collide either",
    ))
    out.append((
        "oracle_owner_is_part_of_the_identity",
        len(ora_fps) == 3 and ora_fps[0] != ora_fps[2]
        and "schema=GEO" in ora_fps[0] and "schema=AUTRE" in ora_fps[2],
        f"GEO.PARCELLE={ora_fps[0] if ora_fps else None!r} vs "
        f"AUTRE.PARCELLE={ora_fps[2] if len(ora_fps) > 2 else None!r}: the "
        f"oracle profile gained a `schema` token; without it two owners "
        f"holding a table of the same name shared one history.",
    ))
    out.append((
        "one_entry_point_for_capture_and_migration",
        ctx.data.get("shared_entry_point") == [pg_fps[0] if pg_fps else None,
                                               ora_fps[0] if ora_fps else None,
                                               file_fps[0] if file_fps else None],
        f"compute_fingerprint_for_source={ctx.data.get('shared_entry_point')} "
        f"vs compute_datasource_fingerprint={[pg_fps[:1], ora_fps[:1], file_fps[:1]]}: "
        f"the migration recomputes the canonical form from the registry URI "
        f"with the SAME function the capture uses, or the aliases it writes "
        f"would point at a value nothing ever produces.",
    ))

    # ===== 2. Le garde-fou sert encore, sur le bon cas ===================
    pg_same = ctx.data.get("same_table_pg_collisions") or {}
    out.append((
        "two_layers_on_one_postgis_table_still_collide",
        len(pg_same) == 1
        and sorted(next(iter(pg_same.values()))) == ["routes", "routes (cat=A)"],
        f"collisions={pg_same}: a server-side filter (`sql=`) is not another "
        f"table. Both layers write to public.routes and share one history, so "
        f"a rewind scoped on one of them would act on events captured through "
        f"the other. The detector must name both layers.",
    ))
    file_same = ctx.data.get("same_table_file_collisions") or {}
    out.append((
        "two_layers_on_one_gpkg_table_still_collide",
        len(file_same) == 1
        and sorted(next(iter(file_same.values()))) == ["routes", "routes filtrees"],
        f"collisions={file_same}: since v6 a display filter (`|subset=`) no "
        f"longer forks the identity of a file -- which is exactly why two "
        f"layers of the same table now meet in one fingerprint and must be "
        f"reported.",
    ))

    # ===== 3. Aucun faux positif ========================================
    out.append((
        "no_false_positive_on_two_tables_of_one_schema",
        ctx.data.get("pg_layers_collisions") == {},
        f"collisions={ctx.data.get('pg_layers_collisions')}: two tables of one "
        f"schema are two sources; flagging them would have blocked every "
        f"PostGIS project",
    ))
    out.append((
        "no_false_positive_on_oracle_owners",
        ctx.data.get("ora_layers_collisions") == {},
        f"collisions={ctx.data.get('ora_layers_collisions')}",
    ))
    out.append((
        "no_false_positive_on_healthy_file_layers",
        ctx.data.get("file_layers_collisions") == {},
        f"collisions={ctx.data.get('file_layers_collisions')}: two layers of "
        f"one GeoPackage have distinct sources and must NOT be flagged",
    ))

    # ===== 4. Le point d'entree du rewind refuse toujours ================
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
