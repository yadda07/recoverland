# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""mig_repointed_layer_alias - RecoverLand validation runtime.

Invariant: I-3 et la regle d'or de la migration : devant une ambiguite,
ne pas deviner ; l'historique d'une table n'est jamais attribue a une
autre.

Le trou que ce scenario prouve
------------------------------
La reconciliation v6 rattache une empreinte DB historique (dont le jeton
``table=`` contenait le SCHEMA, si bien que toutes les tables d'un schema
la partageaient) a la table que nomme la derniere URI du registre, des que
ses evenements ne portent qu'UN `layer_id_snapshot`. Or un identifiant de
couche QGIS n'est pas une table : "Changer la source de donnees" repointe
la couche L1 de ``public.routes`` vers ``public.rivieres`` en gardant son
identifiant. L'empreinte historique ne change pas (meme schema), l'upsert
du registre garde ``rivieres``, et la migration voit une seule couche :
alias ``historique -> rivieres``. Un rewind sur rivieres rejoue alors les
evenements de routes, par gid, sur les lignes de rivieres (cle primaire
de confiance) et annonce APPLIED.

Ce que les evenements ont capture tranche pourtant : une table a UN type
de geometrie et, pour ce journal, UN schema de champs. Le journal fixture
(vrai JournalManager, vraie migration) contient :

  * ``gis``    : L1 edite sur routes (gid, nom) puis sur rivieres (gid,
    debit), meme geometrie ligne, registre sur rivieres ;
  * ``gis2``   : L2 edite sur une table lignes puis une table polygones,
    meme schema de champs ;
  * ``gis3``   : L3, evenements ponctuels, registre sur une table de
    polygones ;
  * ``cad``    : temoin sain, une couche, un schema, une geometrie : l'alias
    DOIT etre pose, le refus doit rester cible.

Les journaux deja reconcilies par 6.x portent DEJA l'alias fautif : ne
plus le creer ne les protege pas. Un second journal fixture (schema 6,
marqueur "ok" pose sous les regles 6.x) le verifie :

  * ``gis6`` : alias de migration pose sur une couche repointee de routes
    vers rivieres : il doit etre RETIRE et signale ;
  * ``cad6`` : alias de migration d'une table a laquelle une colonne a ete
    ajoutee (schemas emboites, meme geometrie) : il doit rester ;
  * ``man6`` : meme situation que gis6, mais alias pose a la main par
    l'utilisateur : on n'y touche jamais.

Verdict attendu avant correctif : FAIL (alias poses sur les trois cas).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_repointed_layer_alias"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_T0 = datetime(2026, 5, 20, 9, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()


def _legacy_fp(db):
    return (f"postgres::host=localhost port=5432 dbname={db} "
            f"schema=public table=public")


def _uri(db, table):
    return (f"dbname='{db}' host=localhost port=5432 user='u' "
            f"table=\"public\".\"{table}\" (geom) sql=")


def _schema(*names):
    return json.dumps([{"name": n, "type": "text", "length": 0,
                        "precision": 0} for n in names])


_ROUTES = _schema("gid", "nom")
_RIVIERES = _schema("gid", "debit")
_LIGNES = _schema("gid", "code")

# (db, layer_id, field_schema_json, geometry_type)
_STORY = [
    ("gis", "L1", _ROUTES, "LineString"),
    ("gis", "L1", _ROUTES, "LineString"),
    ("gis", "L1", _RIVIERES, "LineString"),
    ("gis", "L1", _RIVIERES, "LineString"),
    ("gis2", "L2", _LIGNES, "LineString"),
    ("gis2", "L2", _LIGNES, "Polygon"),
    ("gis3", "L3", _LIGNES, "Point"),
    ("gis3", "L3", _LIGNES, "Point"),
    ("cad", "L4", _LIGNES, "MultiPolygon"),
    ("cad", "L4", _LIGNES, "MultiPolygon"),
]
# (db, table enregistree, geometrie enregistree)
_REGISTRY = [("gis", "rivieres", "LineString"), ("gis2", "surfaces", "Polygon"),
             ("gis3", "parcelles", "Polygon"), ("cad", "parcelles", "Polygon")]

# Journal 6.x : (db, layer_id, schemas successifs, geometrie, table du
# registre, note de l'alias deja pose).
_NOTE_V6 = "migration v6: empreinte canonique"
_STORY_V6 = [
    ("gis6", "L6", (_ROUTES, _RIVIERES), "LineString", "rivieres", _NOTE_V6),
    ("cad6", "L7", (_LIGNES, _schema("gid", "code", "surface")), "Polygon",
     "parcelles", _NOTE_V6),
    ("man6", "L8", (_ROUTES, _RIVIERES), "LineString", "rivieres",
     "rattachement manuel"),
]


def _seed(db_path):
    from recoverland.core.sqlite_schema import (
        get_all_ddl, AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    conn = sqlite3.connect(db_path)
    try:
        for ddl in get_all_ddl():
            conn.execute(ddl)
        insert = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
                  + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
        for idx, (db, layer_id, schema, geom) in enumerate(_STORY):
            gid = idx + 1
            conn.execute(insert, (
                "proj_rep", _legacy_fp(db), layer_id, f"couche_{layer_id}",
                "postgres",
                json.dumps({"fid": gid, "pk_field": "gid", "pk_value": gid}),
                "UPDATE", json.dumps({"changed_only": {"gid": {
                    "old": gid, "new": gid}}}),
                None, geom, "EPSG:2154", schema, "tester", "sess_rep",
                (_T0 + timedelta(minutes=idx)).isoformat(), None,
                f"pk:gid={gid}", 5, None, None))
        now = _T0.isoformat()
        for db, table, geom in _REGISTRY:
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
                (_legacy_fp(db), "postgres", _uri(db, table), table, "",
                 "EPSG:2154", geom, now))
        conn.execute("INSERT OR REPLACE INTO schema_version (version_number, "
                     "applied_at, description) VALUES (5, ?, 'fixture v5')",
                     (now,))
        conn.commit()
    finally:
        conn.close()


def _seed_v6(db_path):
    """Journal 6.x : evenements legacy, registre, alias DEJA poses, marqueur ok."""
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
        for db, layer_id, schemas, geom, table, note in _STORY_V6:
            for schema in schemas:
                for _ in range(2):
                    gid += 1
                    conn.execute(insert, (
                        "proj_rep6", _legacy_fp(db), layer_id,
                        f"couche_{layer_id}", "postgres",
                        json.dumps({"fid": gid, "pk_field": "gid",
                                    "pk_value": gid}),
                        "UPDATE", json.dumps({"changed_only": {"gid": {
                            "old": gid, "new": gid}}}),
                        None, geom, "EPSG:2154", schema, "tester", "sess_rep6",
                        (_T0 + timedelta(minutes=gid)).isoformat(), None,
                        f"pk:gid={gid}", 5, None, None))
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
                (_legacy_fp(db), "postgres", _uri(db, table), table, "",
                 "EPSG:2154", geom, now))
            conn.execute(
                "INSERT INTO datasource_alias (alias_fingerprint, "
                "target_fingerprint, created_at, note) VALUES (?, ?, ?, ?)",
                (_legacy_fp(db), _target_fp(db, table), now, note))
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES (?, ?, ?)",
            (RECONCILE_SETTING_KEY, json.dumps({
                "status": "ok", "scanned": 3, "aliases_created": 3,
                "already_canonical": 0, "already_aliased": 0,
                "ambiguous": [], "without_source_uri": [],
                "completed_at": now}), now))
        conn.execute("INSERT OR REPLACE INTO schema_version (version_number, "
                     "applied_at, description) VALUES (6, ?, 'fixture v6')",
                     (now,))
        conn.commit()
    finally:
        conn.close()


def _target_fp(db, table):
    return (f"postgres::host=localhost port=5432 dbname={db} "
            f"schema=public table={table}")


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir6 = tempfile.mkdtemp(prefix="rl_mig_rep6_")
    project6 = str(Path(tmpdir6) / "projet.qgz")
    Path(project6).write_text("<qgis/>", encoding="utf-8")
    db6 = str(Path(tmpdir6) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db6), exist_ok=True)
    _seed_v6(db6)
    ctx.data.update({"tmpdir6": tmpdir6, "project6": project6})
    tmpdir = tempfile.mkdtemp(prefix="rl_mig_rep_")
    project_path = str(Path(tmpdir) / "projet.qgz")
    Path(project_path).write_text("<qgis/>", encoding="utf-8")
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _seed(db_path)
    ctx.data.update({"tmpdir": tmpdir, "project_path": project_path})
    flog(f"mig_repointed_layer_alias setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.datasource_alias import (
        list_aliases, get_ambiguous_datasources,
    )
    from recoverland.core.event_stream_repository import count_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    from recoverland.core.logger import flog

    d = ctx.data
    d["error"] = None
    jm = JournalManager()
    try:
        jm.open_for_project(d["project_path"], d["tmpdir"])
        conn = jm.get_connection()
        d["aliases"] = {a[0]: a[1] for a in list_aliases(conn)}
        d["ambiguous"] = {a.fingerprint: a.reason
                          for a in get_ambiguous_datasources(conn)}
        cutoff = RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True)
        d["counts"] = {db: count_events_after_cutoff(
            conn, _legacy_fp(db), cutoff, trace_id="rep")
            for db in ("gis", "gis2", "gis3", "cad")}
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        jm.close()
        shutil.rmtree(d["tmpdir"], ignore_errors=True)
    jm6 = JournalManager()
    try:
        jm6.open_for_project(d["project6"], d["tmpdir6"])
        conn = jm6.get_connection()
        d["aliases6"] = {a[0]: a[1] for a in list_aliases(conn)}
        d["ambiguous6"] = {a.fingerprint: a.reason
                           for a in get_ambiguous_datasources(conn)}
        from recoverland.core.datasource_alias import describe_reconciliation
        d["description6"] = describe_reconciliation(conn)
        cutoff = RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True)
        d["counts6"] = {db: count_events_after_cutoff(
            conn, _target_fp(db, table), cutoff, trace_id="rep6")
            for db, _l, _s, _g, table, _n in _STORY_V6}
    except Exception as exc:  # noqa: BLE001
        d["error"] = (d.get("error") or "") + " v6:" + repr(exc)
    finally:
        jm6.close()
        shutil.rmtree(d["tmpdir6"], ignore_errors=True)
    flog(f"mig_repointed_layer_alias run: trace_id={ctx.trace_id} "
         f"aliases={d.get('aliases')} ambiguous={d.get('ambiguous')} "
         f"aliases6={d.get('aliases6')} ambiguous6={d.get('ambiguous6')} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    aliases = d.get("aliases") or {}
    amb = d.get("ambiguous") or {}
    counts = d.get("counts") or {}
    out = [("scenario_ran", d.get("error") is None,
            f"le scenario a plante : {d.get('error')}")]
    for db, reason, story in (
            ("gis", "plusieurs_schemas_de_champs_sous_une_empreinte",
             "L1 repointee de routes vers rivieres"),
            ("gis2", "plusieurs_types_de_geometrie_sous_une_empreinte",
             "L2 repointee d'une table de lignes vers une table de polygones"),
            ("gis3", "geometrie_differente_de_la_table_enregistree",
             "evenements ponctuels, registre sur une table de polygones")):
        fp = _legacy_fp(db)
        out.append((
            f"no_alias_{db}",
            fp not in aliases,
            f"{story} : alias pose {fp!r} -> {aliases.get(fp)!r}. Un rewind "
            f"sur la table enregistree rejouerait par gid l'historique d'une "
            f"AUTRE table sur ses lignes, en annoncant APPLIED."))
        out.append((
            f"reported_{db}",
            amb.get(fp) == reason,
            f"{story} : raison signalee={amb.get(fp)!r}, attendue {reason!r}. "
            f"Le refus doit etre nomme precisement pour que l'utilisateur "
            f"sache quoi faire."))
    cad = _legacy_fp("cad")
    out.append((
        "healthy_source_still_aliased",
        cad in aliases and "parcelles" in (aliases.get(cad) or ""),
        f"temoin sain : alias={aliases.get(cad)!r}. Le refus doit rester "
        f"cible, sinon toute source ancienne perd son historique."))
    out.append((
        "nothing_lost",
        counts == {"gis": 4, "gis2": 2, "gis3": 2, "cad": 2},
        f"evenements toujours lisibles sous leur empreinte d'origine={counts}"))

    aliases6 = d.get("aliases6") or {}
    amb6 = d.get("ambiguous6") or {}
    counts6 = d.get("counts6") or {}
    gis6 = _legacy_fp("gis6")
    out.append((
        "v6_disproved_alias_withdrawn",
        gis6 not in aliases6 and counts6.get("gis6") == 0,
        f"journal 6.x, L6 repointee de routes vers rivieres, alias deja pose "
        f"par la migration 6.x : alias apres ouverture={aliases6.get(gis6)!r}, "
        f"evenements dans la portee d'un rewind de rivieres="
        f"{counts6.get('gis6')!r} (attendu 0). Laisse en place, il fait "
        f"rejouer par gid l'historique de routes sur les lignes de rivieres."))
    out.append((
        "v6_withdrawal_reported",
        amb6.get(gis6) == ("alias_v6_retire_plusieurs_schemas_de_champs_"
                           "sous_une_empreinte")
        and "retires" in (d.get("description6") or ""),
        f"raison signalee={amb6.get(gis6)!r} message="
        f"{d.get('description6')!r} : l'utilisateur doit savoir que "
        f"l'historique ancien de cette couche n'est plus rattache, et pourquoi."))
    cad6, man6 = _legacy_fp("cad6"), _legacy_fp("man6")
    out.append((
        "v6_nested_schema_alias_kept",
        aliases6.get(cad6) == _target_fp("cad6", "parcelles")
        and counts6.get("cad6") == 4,
        f"table a laquelle une colonne a ete ajoutee : alias="
        f"{aliases6.get(cad6)!r} evenements dans la portee="
        f"{counts6.get('cad6')!r} (attendu 4). Retirer cet alias detacherait "
        f"sans raison l'historique d'une table saine."))
    out.append((
        "v6_manual_alias_untouched",
        aliases6.get(man6) == _target_fp("man6", "rivieres"),
        f"alias pose a la main par l'utilisateur : {aliases6.get(man6)!r}. "
        f"La migration ne defait jamais une decision de l'utilisateur."))
    out.append(assert_log_contains(
        ctx.records, rf"mig_repointed_layer_alias run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
