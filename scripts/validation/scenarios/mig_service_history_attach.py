"""mig_service_history_attach - RecoverLand validation runtime.

Invariant: I-3 (une empreinte designe une table dans une base) ET "rien
n'est cache, rien n'est perdu" : l'historique d'avant v7 d'une couche
atteinte par un service de connexion n'est ni attribue a une autre base,
ni retire en silence du rewind.

Le trou que ce scenario prouve
------------------------------
Avant v7, une empreinte DB ne notait pas le SERVICE de connexion
(pg_service.conf, DSN ODBC) : ``service='prod'`` et ``service='test'``
donnaient la meme empreinte. v7 ajoute le service ; l'ancienne empreinte
ne peut donc plus etre rattachee a coup sur. Le correctif precedent la
declarait ambigue dans TOUS les cas : l'utilisateur qui n'a jamais eu
qu'UN service, la configuration courante en entreprise, perdait le rewind
de tout son historique anterieur, avec un avertissement generique et
aucun moyen de le rattacher.

Regle attendue, compatible et prudente :

  * un seul service connu (du journal : registre, empreintes v7 ; et du
    poste : fichiers de services libpq) -> l'historique est rattache a ce
    service, et le rattachement est SIGNALE comme une hypothese ;
  * plusieurs services connus -> refus, nomme, avec la liste des services ;
  * liste des services du poste non etablie (le service du registre n'est
    dans aucun fichier lu) -> refus nomme, jamais une supposition ;
  * l'utilisateur peut rattacher lui-meme l'historique a un service
    nomme (`attach_service_history`), et seulement a la meme table.

Rattachement manuel d'une empreinte d'avant v6 (second tour)
------------------------------------------------------------
Avant v6, le jeton ``table=`` d'une empreinte PostGIS portait le SCHEMA
(``schema=public table=public``). `attach_service_history` fabriquait sa
cible en ajoutant le service au corps stocke : ``service=prod ...
table=public``, une table qui n'existe pas. Il repondait "historique
rattache au service 'prod'", la portee du rewind de prod restait a 0
evenement, et le candidat disparaissait du rapport (statut ok, jamais
reexamine) : l'historique etait detache pour toujours, apres un succes
annonce. Attendu : la cible est la table que le registre nomme pour
cette empreinte, atteinte par le service choisi (``table=routes``), sous
la meme preuve "une seule table" que le rattachement automatique ; le
rattachement n'est annonce que si la portee du rewind contient bien les
evenements ; sinon refus nomme, rien retire du rapport.

Les fichiers de services sont fixes par le scenario (PGSERVICEFILE,
PGSYSCONFDIR vers des fichiers temporaires), restaures ensuite.

Verdict attendu avant correctif : FAIL (service unique refuse, refus sans
liste de services, aucune API de rattachement ; au second tour, jeton de
schema rattache a une table inexistante et annonce reussi).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "mig_service_history_attach"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_T0 = datetime(2026, 9, 20, 9, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()

_LEGACY = "postgres::host= port=5432 dbname= schema=public table=routes"
_PROD = ("postgres::service=prod host= port=5432 dbname= schema=public "
         "table=routes")
_URI_PROD = "service='prod' key='gid' table=\"public\".\"routes\" (geom)"
_LEGACY_RIV = "postgres::host= port=5432 dbname= schema=public table=rivieres"
# Forme d'avant v6 : le jeton table= portait le SCHEMA.
_LEGACY_SCHEMA_TOKEN = ("postgres::host= port=5432 dbname= schema=public "
                        "table=public")
_URI_TEST_RIV = "service='test' key='gid' table=\"public\".\"rivieres\" (geom)"

# (cas, sections du fichier de services, registre supplementaire)
_CASES = [
    ("unique", ("prod",), ()),
    ("deux_services_poste", ("prod", "test"), ()),
    ("non_enumerable", ("autre",), ()),
    ("deux_services_journal", ("prod",), ((_LEGACY_RIV, _URI_TEST_RIV),)),
]


def _seed(db_path, extra_registry, legacy=None, layer_ids=None):
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
        legacy = legacy or _LEGACY
        layer_ids = layer_ids or ("lyr_routes",) * 3
        for idx, gid in enumerate((5, 7, 9)):
            conn.execute(insert, (
                "proj_svc", legacy, layer_ids[idx], "routes", "postgres",
                json.dumps({"fid": gid, "pk_field": "gid", "pk_value": gid}),
                "UPDATE", json.dumps({"changed_only": {"nom": {
                    "old": f"n{gid}", "new": f"n{gid}b"}}}),
                None, "LineString", "EPSG:2154", None, "tester", "sess",
                (_T0 + timedelta(minutes=idx)).isoformat(), None,
                f"pk:gid={gid}", 6, None, None))
        rows = [(legacy, _URI_PROD)] + list(extra_registry)
        for fp, uri in rows:
            conn.execute(
                "INSERT INTO datasource_registry (datasource_fingerprint, "
                "provider_type, source_uri, layer_name, authcfg, crs_authid, "
                "geometry_type, last_seen_at) VALUES (?,?,?,?,?,?,?,?)",
                (fp, "postgres", uri, fp.rsplit("=", 1)[-1], "", "EPSG:2154",
                 "LineString", now))
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES (?, ?, ?)",
            (RECONCILE_SETTING_KEY, json.dumps({
                "status": "ok", "scanned": 1, "aliases_created": 0,
                "already_canonical": 1, "already_aliased": 0, "ambiguous": [],
                "without_source_uri": [], "completed_at": now}), now))
        conn.execute("INSERT OR REPLACE INTO schema_version (version_number, "
                     "applied_at, description) VALUES (6, ?, 'fixture v6')",
                     (now,))
        conn.commit()
    finally:
        conn.close()


def _count(conn, fingerprint):
    from recoverland.core.event_stream_repository import (
        count_events_after_cutoff,
    )
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    return count_events_after_cutoff(
        conn, fingerprint, RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True),
        trace_id="svcatt")


def _run_case(d, name, sections, extra_registry, root, legacy=None,
              layer_ids=None):
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core import datasource_alias as da
    from recoverland.core.datasource_registry import lookup_datasource
    case_dir = os.path.join(root, name)
    os.makedirs(os.path.join(case_dir, "sysconf"), exist_ok=True)
    service_file = os.path.join(case_dir, "pg_service.conf")
    with open(service_file, "w", encoding="utf-8") as fh:
        for section in sections:
            fh.write(f"[{section}]\nhost=db-{section}.example\n"
                     f"dbname=gis\nport=5432\n\n")
    os.environ["PGSERVICEFILE"] = service_file
    os.environ["PGSYSCONFDIR"] = os.path.join(case_dir, "sysconf")
    project = os.path.join(case_dir, "projet.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    db = os.path.join(case_dir, ".recoverland", "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db), exist_ok=True)
    _seed(db, extra_registry, legacy, layer_ids)
    out = {}
    jm = JournalManager()
    try:
        jm.open_for_project(project, case_dir)
        conn = jm.get_connection()
        out["aliases"] = {a[0]: (a[1], a[3]) for a in da.list_aliases(conn)}
        report = da.load_reconciliation(conn)
        out["ambiguous"] = {a.fingerprint: (a.reason, getattr(a, "detail", ""))
                            for a in (report.ambiguous if report else ())}
        out["assumed"] = [
            (a.fingerprint, a.target, a.detail)
            for a in (getattr(report, "assumed", ()) if report else ())]
        out["description"] = da.describe_reconciliation(conn)
        out["n_prod"] = _count(conn, _PROD)
        out["n_legacy"] = _count(conn, _LEGACY)
        info = lookup_datasource(conn, _PROD)
        out["registry_prod"] = info.source_uri if info else None
        attach = getattr(da, "attach_service_history", None)
        out["attach_api"] = attach is not None
        if attach is not None and name.startswith("attach_"):
            candidates = getattr(da, "service_history_candidates", None)
            out["candidates_before"] = [
                c.fingerprint for c in (candidates(conn) if candidates else [])]
            out["attach_prod"] = attach(conn, legacy, "prod")
            out["aliases_after_attach"] = {
                a[0]: a[1] for a in da.list_aliases(conn)}
            out["n_prod_after_attach"] = _count(conn, _PROD)
            out["candidates_after"] = [
                c.fingerprint for c in (candidates(conn) if candidates else [])]
            out["description_after"] = da.describe_reconciliation(conn)
        if attach is not None and name == "deux_services_poste":
            out["attach_other_provider"] = attach(
                conn, "ogr::c:/data/x.gpkg|layername=routes", "prod")
            out["attach_empty_service"] = attach(conn, _LEGACY, "")
            out["attach_prod"] = attach(conn, _LEGACY, "prod")
            out["aliases_after_attach"] = {
                a[0]: a[1] for a in da.list_aliases(conn)}
            out["n_prod_after_attach"] = _count(conn, _PROD)
    finally:
        jm.close()
    d["cases"][name] = out


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["saved_env"] = {k: os.environ.get(k)
                             for k in ("PGSERVICEFILE", "PGSYSCONFDIR")}
    ctx.data["root"] = tempfile.mkdtemp(prefix="rl_svcatt_")
    flog(f"mig_service_history_attach setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog
    d = ctx.data
    d["error"] = None
    d["cases"] = {}
    try:
        for name, sections, extra in _CASES:
            try:
                _run_case(d, name, sections, extra, d["root"])
            except Exception as exc:  # noqa: BLE001
                d["cases"][name] = {"error": repr(exc)}
        # Un seul service, mais une empreinte d'avant v6 (jeton table=
        # porteur du SCHEMA) sous laquelle deux couches ont ecrit : le
        # service designe la base, pas la table.
        try:
            _run_case(d, "jeton_schema_deux_couches", ("prod",), (),
                      d["root"], legacy=_LEGACY_SCHEMA_TOKEN,
                      layer_ids=("lyr_routes", "lyr_rivieres", "lyr_routes"))
        except Exception as exc:  # noqa: BLE001
            d["cases"]["jeton_schema_deux_couches"] = {"error": repr(exc)}
        # Rattachement MANUEL d'une empreinte d'avant v6 (jeton de schema),
        # refusee parce que le poste connait prod et test : une couche (la
        # cible est routes, que nomme le registre), puis deux couches.
        for name, layer_ids in (
                ("attach_jeton_schema", ("lyr_routes",) * 3),
                ("attach_jeton_schema_deux_couches",
                 ("lyr_routes", "lyr_rivieres", "lyr_routes"))):
            try:
                _run_case(d, name, ("prod", "test"), (), d["root"],
                          legacy=_LEGACY_SCHEMA_TOKEN, layer_ids=layer_ids)
            except Exception as exc:  # noqa: BLE001
                d["cases"][name] = {"error": repr(exc)}
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        _restore_env(d)
    flog(f"mig_service_history_attach run: trace_id={ctx.trace_id} "
         f"cases={d.get('cases')} error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def _restore_env(d):
    for key, value in (d.get("saved_env") or {}).items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def teardown(ctx):
    _restore_env(ctx.data)
    shutil.rmtree(ctx.data.get("root") or "", ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    cases = d.get("cases") or {}
    uniq = cases.get("unique") or {}
    out = [("scenario_ran",
            d.get("error") is None
            and not any("error" in c for c in cases.values()),
            f"le scenario a plante : {d.get('error')} "
            f"{ {k: v.get('error') for k, v in cases.items() if 'error' in v} }")]
    alias = (uniq.get("aliases") or {}).get(_LEGACY)
    out.append((
        "single_service_history_attached",
        alias is not None and alias[0] == _PROD and uniq.get("n_prod") == 3,
        f"un seul service connu (prod) : alias={alias!r}, evenements dans la "
        f"portee d'un rewind de prod={uniq.get('n_prod')!r} (attendu 3). "
        f"Laisse detache, tout l'historique d'avant v7 sort du rewind."))
    out.append((
        "single_service_attachment_said",
        any(a[0] == _LEGACY and a[1] == _PROD and "prod" in (a[2] or "")
            for a in uniq.get("assumed") or [])
        and "prod" in (uniq.get("description") or ""),
        f"rattachement par hypothese : rapport={uniq.get('assumed')!r} "
        f"message={uniq.get('description')!r}. L'utilisateur doit savoir que "
        f"l'historique a ete rattache a 'prod' parce que c'est le seul "
        f"service connu."))
    out.append((
        "single_service_registry_usable",
        uniq.get("registry_prod") == _URI_PROD,
        f"lookup_datasource(prod) -> {uniq.get('registry_prod')!r} : une fois "
        f"l'historique rattache a prod, le registre de prod doit servir."))
    for name, story, services in (
            ("deux_services_poste", "le poste connait prod et test",
             ("prod", "test")),
            ("deux_services_journal", "le journal a deja vu prod et test",
             ("prod", "test")),
            ("non_enumerable", "le service du registre (prod) n'est dans "
                               "aucun fichier de services lu", ("prod",))):
        res = cases.get(name) or {}
        amb = (res.get("ambiguous") or {}).get(_LEGACY) or ("", "")
        out.append((
            f"refused_named_{name}",
            _LEGACY not in (res.get("aliases") or {})
            and amb[0] == "service_de_connexion_non_enregistre"
            and all(s in (amb[1] or "") for s in services)
            and res.get("n_prod") == 0 and res.get("n_legacy") == 3,
            f"{story} : alias={(res.get('aliases') or {}).get(_LEGACY)!r} "
            f"refus={amb!r} portee prod={res.get('n_prod')!r} evenements "
            f"lisibles={res.get('n_legacy')!r}. Le refus doit nommer les "
            f"services en cause ({', '.join(services)}), et rien ne doit etre "
            f"perdu."))
    tok = cases.get("jeton_schema_deux_couches") or {}
    tok_amb = (tok.get("ambiguous") or {}).get(_LEGACY_SCHEMA_TOKEN) or ("", "")
    out.append((
        "single_service_never_attaches_two_tables",
        _LEGACY_SCHEMA_TOKEN not in (tok.get("aliases") or {})
        and tok_amb[0] == "plusieurs_couches_sous_une_empreinte",
        f"un seul service connu, mais une empreinte d'avant v6 (schema dans "
        f"le jeton table=) sous laquelle deux couches ont ecrit : alias="
        f"{(tok.get('aliases') or {}).get(_LEGACY_SCHEMA_TOKEN)!r} refus="
        f"{tok_amb!r}. Le service designe la base, pas la table : rattacher "
        f"ferait rejouer l'historique de rivieres sur routes."))
    two = cases.get("deux_services_poste") or {}

    def _ok(value):
        return value[0] if isinstance(value, tuple) else value

    out.append((
        "manual_attach_api",
        two.get("attach_api") is True
        and _ok(two.get("attach_other_provider")) is False,
        f"API de rattachement : presente={two.get('attach_api')!r}, empreinte "
        f"non DB -> {two.get('attach_other_provider')!r} (doit etre refusee)."))
    ok_attach = _ok(two.get("attach_prod"))
    bad_empty = _ok(two.get("attach_empty_service"))
    out.append((
        "manual_attach_to_a_named_service",
        ok_attach is True and bad_empty is False
        and (two.get("aliases_after_attach") or {}).get(_LEGACY) == _PROD
        and two.get("n_prod_after_attach") == 3,
        f"rattachement choisi par l'utilisateur : prod -> {two.get('attach_prod')!r}, "
        f"service vide -> {two.get('attach_empty_service')!r}, alias="
        f"{(two.get('aliases_after_attach') or {}).get(_LEGACY)!r}, portee="
        f"{two.get('n_prod_after_attach')!r}. Sans ce moyen, un historique "
        f"refuse reste hors du rewind pour toujours."))
    one = cases.get("attach_jeton_schema") or {}
    out.append((
        "manual_attach_schema_token_reaches_the_registered_table",
        _LEGACY_SCHEMA_TOKEN in (one.get("candidates_before") or [])
        and _ok(one.get("attach_prod")) is True
        and (one.get("aliases_after_attach") or {}).get(_LEGACY_SCHEMA_TOKEN)
        == _PROD
        and one.get("n_prod_after_attach") == 3,
        f"empreinte d'avant v6 (jeton table=public), une couche, poste "
        f"prod+test : candidats={one.get('candidates_before')} ; rattachement "
        f"a prod -> {one.get('attach_prod')!r} alias="
        f"{(one.get('aliases_after_attach') or {}).get(_LEGACY_SCHEMA_TOKEN)!r} "
        f"(attendu {_PROD!r}) portee du rewind de prod="
        f"{one.get('n_prod_after_attach')!r} (attendu 3). Rattache a "
        f"'table=public', une table qui n'existe pas, l'historique n'entre "
        f"jamais dans le rewind alors que le succes est annonce."))
    two_l = cases.get("attach_jeton_schema_deux_couches") or {}
    out.append((
        "manual_attach_never_announces_what_it_did_not_do",
        _ok(two_l.get("attach_prod")) is False
        and _LEGACY_SCHEMA_TOKEN not in (two_l.get("aliases_after_attach") or {})
        and two_l.get("n_prod_after_attach") == 0
        and _LEGACY_SCHEMA_TOKEN in (two_l.get("candidates_after") or [])
        and bool(two_l.get("description_after")),
        f"empreinte d'avant v6 sous laquelle deux couches ont ecrit, "
        f"rattachement a prod -> {two_l.get('attach_prod')!r} alias="
        f"{(two_l.get('aliases_after_attach') or {}).get(_LEGACY_SCHEMA_TOKEN)!r} "
        f"portee={two_l.get('n_prod_after_attach')!r} candidats apres="
        f"{two_l.get('candidates_after')} avertissement="
        f"{bool(two_l.get('description_after'))}. Le service designe la "
        f"base, pas la table : refus nomme, et l'avertissement comme le "
        f"candidat doivent rester, sinon l'historique est detache sans "
        f"retour."))
    out.append(assert_log_contains(
        ctx.records, rf"mig_service_history_attach run: trace_id={ctx.trace_id}",
        name="trace_id_propagated"))
    return out


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
