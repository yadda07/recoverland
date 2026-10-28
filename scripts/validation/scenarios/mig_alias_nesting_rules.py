# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""mig_alias_nesting_rules - RecoverLand validation runtime.

Invariant: I-3 et la regle d'or de la migration, dans les DEUX sens :
l'historique d'une table n'est jamais attribue a une autre, ET
l'historique d'une table saine n'en est jamais detache sans preuve.

Le trou que ce scenario prouve
------------------------------
La reconciliation a deux regles qui ne disaient pas la meme chose :

  * pour poser un NOUVEL alias, elle refusait des que les evenements
    d'une empreinte portaient deux textes `field_schema_json` differents.
    Ce texte est `serialize_field_schema(layer.fields())` : jointures et
    champs virtuels compris, longueurs et types compris. Une colonne
    ajoutee, une jointure ajoutee, et un journal 5.x migre directement en
    v7 perd la portee de rewind de sa table, alors que le meme journal
    passe par 6.x la garde ;
  * pour RETIRER un alias pose par 6.x, elle se contentait de listes de
    champs qui ne s'emboitent pas, sous la precondition "le jeton table=
    change", vraie pour TOUTE empreinte PostGIS historique (le jeton
    contenait le schema). Une colonne renommee, une jointure remplacee par
    une autre, et l'alias d'une table saine est retire : son historique
    sort du rewind, sans interface pour le rattacher.

Regles attendues : un nouvel alias exige des listes de champs qui
s'emboitent (colonnes seulement ajoutees ou seulement retirees) et une
seule famille de geometrie ; un alias deja pose n'est retire que sur une
preuve independante du jeton : familles de geometrie differentes, ou
listes de champs qui ne partagent rien au-dela de la cle.

Deux journaux fixtures (vrai JournalManager, vraie migration) :

  * journal 5.x : ``grow`` (colonne ajoutee), ``join`` (jointure
    ajoutee) doivent etre RATTACHES ; ``repoint`` (routes -> rivieres) reste
    refuse, temoin ;
  * journal 6.x (alias deja poses, marqueur "ok") : ``rename6`` (colonne
    renommee) et ``joinswap6`` (jointure remplacee) GARDENT leur alias ;
    ``repoint6`` (ne partagent que la cle) et ``geomflip6`` (lignes puis
    polygones) le perdent, avec leur raison.

Verdict attendu avant correctif : FAIL (grow/join refuses, rename6/joinswap6
retires).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_alias_nesting_rules"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_T0 = datetime(2026, 5, 20, 9, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()
_NOTE_V6 = "migration v6: empreinte canonique"


def _legacy_fp(db):
    return (f"postgres::host=localhost port=5432 dbname={db} "
            f"schema=public table=public")


def _target_fp(db, table):
    return (f"postgres::host=localhost port=5432 dbname={db} "
            f"schema=public table={table}")


def _uri(db, table):
    return (f"dbname='{db}' host=localhost port=5432 user='u' "
            f"table=\"public\".\"{table}\" (geom) sql=")


def _schema(*names):
    return json.dumps([{"name": n, "type": "text", "length": 0,
                        "precision": 0} for n in names])


# (db, layer_id, [(schema, geometrie), ...], table enregistree, geometrie
#  enregistree)
_V5 = [
    ("grow", "G1", [(_schema("gid", "nom"), "Polygon"),
                    (_schema("gid", "nom", "surface"), "Polygon")],
     "parcelles", "Polygon"),
    ("join", "J1", [(_schema("gid", "nom"), "Point"),
                    (_schema("gid", "nom", "communes_nom"), "Point")],
     "arbres", "Point"),
    ("repoint", "R1", [(_schema("gid", "nom"), "LineString"),
                       (_schema("gid", "debit"), "LineString")],
     "rivieres", "LineString"),
]
_V6 = [
    ("rename6", "N6", [(_schema("gid", "nom", "code"), "Polygon"),
                       (_schema("gid", "name", "code"), "Polygon")],
     "parcelles", "Polygon"),
    ("joinswap6", "S6", [(_schema("gid", "nom", "zone_a_libelle"), "Point"),
                         (_schema("gid", "nom", "zone_b_libelle"), "Point")],
     "arbres", "Point"),
    ("repoint6", "P6", [(_schema("gid", "nom"), "LineString"),
                        (_schema("gid", "debit"), "LineString")],
     "rivieres", "LineString"),
    ("geomflip6", "F6", [(_schema("gid", "code"), "LineString"),
                         (_schema("gid", "code"), "Polygon")],
     "surfaces", "Polygon"),
]


def _seed(db_path, story, version, with_aliases):
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    from recoverland.core.datasource_alias import RECONCILE_SETTING_KEY
    conn = sqlite3.connect(db_path)
    try:
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
                  + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
        now = _T0.isoformat()
        gid = 0
        for db, layer_id, steps, table, geom_reg in story:
            for schema, geom in steps:
                for _ in range(2):
                    gid += 1
                    conn.execute(insert, (
                        "proj_nest", _legacy_fp(db), layer_id,
                        f"couche_{layer_id}", "postgres",
                        json.dumps({"fid": gid, "pk_field": "gid",
                                    "pk_value": gid}),
                        "UPDATE", json.dumps({"changed_only": {"gid": {
                            "old": gid, "new": gid}}}),
                        None, geom, "EPSG:2154", schema, "tester", "sess",
                        (_T0 + timedelta(minutes=gid)).isoformat(), None,
                        f"pk:gid={gid}", version, None, None))
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
                (_legacy_fp(db), "postgres", _uri(db, table), table, "",
                 "EPSG:2154", geom_reg, now))
            if with_aliases:
                conn.execute(
                    "INSERT INTO datasource_alias (alias_fingerprint, "
                    "target_fingerprint, created_at, note) VALUES (?,?,?,?)",
                    (_legacy_fp(db), _target_fp(db, table), now, _NOTE_V6))
        if with_aliases:
            conn.execute(
                "INSERT OR REPLACE INTO backend_settings (setting_key, "
                "setting_value, updated_at) VALUES (?, ?, ?)",
                (RECONCILE_SETTING_KEY, json.dumps({
                    "status": "ok", "scanned": len(story),
                    "aliases_created": len(story), "already_canonical": 0,
                    "already_aliased": 0, "ambiguous": [],
                    "without_source_uri": [], "completed_at": now}), now))
        conn.execute("INSERT OR REPLACE INTO schema_version (version_number, "
                     "applied_at, description) VALUES (?, ?, 'fixture')",
                     (version, now))
        conn.commit()
    finally:
        conn.close()


def _journal(prefix, story, version, with_aliases):
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    project = str(Path(tmpdir) / "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    db = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db), exist_ok=True)
    _seed(db, story, version, with_aliases)
    return tmpdir, project


def _open_and_read(tmpdir, project, story):
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.datasource_alias import (
        list_aliases, get_ambiguous_datasources, describe_reconciliation,
    )
    from recoverland.core.event_stream_repository import (
        count_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    jm = JournalManager()
    try:
        jm.open_for_project(project, tmpdir)
        conn = jm.get_connection()
        cutoff = RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True)
        return {
            "aliases": {a[0]: a[1] for a in list_aliases(conn)},
            "ambiguous": {a.fingerprint: a.reason
                          for a in get_ambiguous_datasources(conn)},
            "description": describe_reconciliation(conn),
            "in_scope": {db: count_events_after_cutoff(
                conn, _target_fp(db, table), cutoff, trace_id="nest")
                for db, _l, _s, table, _g in story},
        }
    finally:
        jm.close()
        shutil.rmtree(tmpdir, ignore_errors=True)


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["j5"] = _journal("rl_mig_nest5_", _V5, 5, False)
    ctx.data["j6"] = _journal("rl_mig_nest6_", _V6, 6, True)
    flog(f"mig_alias_nesting_rules setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    d["error"] = None
    try:
        d["r5"] = _open_and_read(*d["j5"], _V5)
        d["r6"] = _open_and_read(*d["j6"], _V6)
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    flog(f"mig_alias_nesting_rules run: trace_id={ctx.trace_id} "
         f"r5={d.get('r5')} r6={d.get('r6')} error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def teardown(ctx):
    for key in ("j5", "j6"):
        pair = ctx.data.get(key)
        if pair:
            shutil.rmtree(pair[0], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    r5 = d.get("r5") or {}
    r6 = d.get("r6") or {}
    a5, amb5, n5 = (r5.get("aliases") or {}, r5.get("ambiguous") or {},
                    r5.get("in_scope") or {})
    a6, amb6, n6 = (r6.get("aliases") or {}, r6.get("ambiguous") or {},
                    r6.get("in_scope") or {})
    out = [("scenario_ran", d.get("error") is None,
            f"le scenario a plante : {d.get('error')}")]
    for db, table, story in (
            ("grow", "parcelles", "une colonne ajoutee a la table"),
            ("join", "arbres", "une jointure ajoutee a la couche")):
        fp = _legacy_fp(db)
        out.append((
            f"new_alias_kept_{db}",
            a5.get(fp) == _target_fp(db, table) and n5.get(db) == 4,
            f"journal 5.x, {story} : alias={a5.get(fp)!r} refus="
            f"{amb5.get(fp)!r} evenements dans la portee du rewind="
            f"{n5.get(db)!r} (attendu 4). Des listes de champs qui "
            f"s'emboitent decrivent UNE table : refuser detache son "
            f"historique du rewind, alors que le meme journal passe par 6.x "
            f"le garde."))
    fp = _legacy_fp("repoint")
    out.append((
        "new_alias_refused_repoint",
        fp not in a5
        and amb5.get(fp) == "plusieurs_schemas_de_champs_sous_une_empreinte",
        f"temoin, couche repointee de routes vers rivieres : alias="
        f"{a5.get(fp)!r} raison={amb5.get(fp)!r}. Le refus doit rester."))
    for db, table, story in (
            ("rename6", "parcelles", "une colonne renommee"),
            ("joinswap6", "arbres", "une jointure remplacee par une autre")):
        fp = _legacy_fp(db)
        out.append((
            f"v6_alias_kept_{db}",
            a6.get(fp) == _target_fp(db, table) and n6.get(db) == 4
            and fp not in amb6,
            f"journal 6.x, {story} : alias apres ouverture={a6.get(fp)!r} "
            f"raison={amb6.get(fp)!r} evenements dans la portee="
            f"{n6.get(db)!r} (attendu 4). Retirer cet alias sort l'historique "
            f"d'une table saine du rewind, sans interface pour le rattacher."))
    for db, reason, story in (
            ("repoint6", "alias_v6_retire_plusieurs_schemas_de_champs_"
                         "sous_une_empreinte",
             "listes de champs qui ne partagent que la cle"),
            ("geomflip6", "alias_v6_retire_plusieurs_types_de_geometrie_"
                          "sous_une_empreinte",
             "lignes puis polygones")):
        fp = _legacy_fp(db)
        out.append((
            f"v6_alias_withdrawn_{db}",
            fp not in a6 and amb6.get(fp) == reason and n6.get(db) == 0,
            f"journal 6.x, {story} : alias={a6.get(fp)!r} raison="
            f"{amb6.get(fp)!r} (attendue {reason!r}) portee={n6.get(db)!r}. "
            f"Deux tables sous une empreinte : l'alias rejouerait l'historique "
            f"de l'une sur les lignes de l'autre."))
    out.append(assert_log_contains(
        ctx.records, rf"mig_alias_nesting_rules run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
