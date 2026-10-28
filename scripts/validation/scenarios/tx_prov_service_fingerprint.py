# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_prov_service_fingerprint - RecoverLand validation runtime.

Invariant: I-3 (une empreinte de datasource designe UNE table dans UNE
base de donnees).

Le trou que ce scenario prouve
------------------------------
Le profil d'empreinte des providers DB ne retenait que host, port, dbname,
schema et table. Une connexion par SERVICE (``service='prod'``, fichier
pg_service.conf ; un DSN ODBC pour MSSQL) laisse host et dbname vides :
TOUS les services donnaient ``postgres::host= port=5432 dbname=
schema=public table=routes``. Deux bases atteintes par deux services
partageaient donc un historique, une ligne de registre (l'upsert garde la
DERNIERE URI vue) et une portee de rewind.

Consequence utilisateur : un projet developpe sur le service ``test`` est
repointe sur ``prod``. L'utilisateur rembobine prod a hier : les editions
faites sur test apres la date sont "compensees" sur les lignes de prod de
meme gid, annoncees APPLIED. Et restaurer un evenement de test quand
aucune couche n'est chargee reconstruit la couche depuis le registre...
qui pointe sur prod.

Ce que le correctif doit garantir, verifie ici :
  * deux services donnent deux empreintes ; une URI host/dbname garde son
    empreinte octet pour octet (aucun historique sain ne bouge), meme
    quand elle nomme aussi un service (les parametres explicites priment
    sur ceux du service, la base est designee sans lui) ;
  * une selection de suivi enregistree par 6.x (forme sans service) designe
    toujours la couche : la capture ne doit pas devenir muette ;
  * un journal 6.x deja reconcilie (marqueur "ok") est reexamine : son
    empreinte sans service n'est PAS rattachee automatiquement a prod
    (on ne sait pas de quel service venaient ses evenements), elle est
    signalee ambigue avec sa raison, le registre refuse de reconstruire une
    couche depuis elle, et le rewind de prod ne rejoue aucun evenement de
    test. Rien n'est perdu : les evenements restent lisibles sous leur
    empreinte d'origine.

Hypothesis revised (v7): la regle "une empreinte sans service n'est
JAMAIS rattachee" est devenue "rattachee seulement quand un seul service
est connu (du journal et du poste), refusee et nommee sinon" : refuser
toujours faisait perdre le rewind de tout l'historique anterieur a
l'utilisateur qui n'a jamais eu qu'un service (cas prouve par
`mig_service_history_attach`). L'histoire de ce scenario (un projet
developpe sur ``test`` puis repointe sur ``prod``) suppose un poste qui
connait les DEUX services : la fixture le declare maintenant
explicitement (PGSERVICEFILE vers un fichier ``[test]`` + ``[prod]``,
PGSYSCONFDIR vers un dossier vide, restaures ensuite), au lieu de
dependre des fichiers de services du poste qui lance le scenario. Les
assertions de refus sont inchangees. La revision attendue du marqueur
suit la constante `RECONCILE_REVISION` (3 depuis que les regles ont
change) : l'intention, "reexamine sous les regles courantes", est la meme.

Verdict attendu avant correctif : FAIL.
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "tx_prov_service_fingerprint"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_URI_PROD = "service='prod' key='gid' table=\"public\".\"routes\" (geom)"
_URI_TEST = "service='test' key='gid' table=\"public\".\"routes\" (geom)"
_URI_HOST = ("dbname='gis' host=localhost port=5432 user='u' "
             "table=\"public\".\"routes\" (geom) sql=")
_FP_HOST_V6 = ("postgres::host=localhost port=5432 dbname=gis "
               "schema=public table=routes")
# Un service ET un hote/une base explicites : les parametres explicites
# priment sur ceux du service (libpq), la base est designee sans lui.
_URI_SVC_HOST = ("service='prod' dbname='gis' host=localhost port=5432 "
                 "table=\"public\".\"routes\" (geom)")
_FP_SERVICELESS = "postgres::host= port=5432 dbname= schema=public table=routes"
_URI_MSSQL_A = "service='dsn_a' dbname='x' table=\"dbo\".\"t\" (geom)"
_URI_MSSQL_B = "service='dsn_b' dbname='x' table=\"dbo\".\"t\" (geom)"

_T0 = datetime(2026, 9, 20, 9, 0, 0, tzinfo=timezone.utc)
_CUTOFF = (_T0 - timedelta(hours=1)).isoformat()


class _Provider:
    def __init__(self, name):
        self._name = name

    def name(self):
        return self._name


class _Layer:
    def __init__(self, provider, source):
        self._provider = _Provider(provider)
        self._source = source

    def dataProvider(self):
        return self._provider

    def source(self):
        return self._source


def _seed(db_path):
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
        for idx, gid in enumerate((5, 7, 9)):
            conn.execute(insert, (
                "proj_svc", _FP_SERVICELESS, "lyr_routes", "routes", "postgres",
                json.dumps({"fid": gid, "pk_field": "gid", "pk_value": gid}),
                "UPDATE",
                json.dumps({"changed_only": {"nom": {"old": f"test_{gid}",
                                                     "new": f"test_{gid}b"}}}),
                None, "LineString", "EPSG:2154", None, "tester", "sess_test",
                (_T0 + timedelta(minutes=idx)).isoformat(), None,
                f"pk:gid={gid}", 6, None, None,
            ))
        now = _T0.isoformat()
        # La couche a ete repointee sur prod : l'upsert a garde cette URI.
        conn.execute(
            "INSERT INTO datasource_registry (datasource_fingerprint, "
            "provider_type, source_uri, layer_name, authcfg, crs_authid, "
            "geometry_type, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (_FP_SERVICELESS, "postgres", _URI_PROD, "routes", "",
             "EPSG:2154", "LineString", now))
        conn.execute(
            "INSERT INTO datasource_registry (datasource_fingerprint, "
            "provider_type, source_uri, layer_name, authcfg, crs_authid, "
            "geometry_type, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (_FP_HOST_V6, "postgres", _URI_SVC_HOST, "routes", "",
             "EPSG:2154", "LineString", now))
        # Journal 6.x deja reconcilie : marqueur "ok", sans revision.
        conn.execute(
            "INSERT OR REPLACE INTO backend_settings (setting_key, "
            "setting_value, updated_at) VALUES (?, ?, ?)",
            (RECONCILE_SETTING_KEY, json.dumps({
                "status": "ok", "scanned": 1, "aliases_created": 0,
                "already_canonical": 1, "already_aliased": 0, "ambiguous": [],
                "without_source_uri": [], "completed_at": now}), now))
        conn.execute(
            "INSERT OR REPLACE INTO schema_version (version_number, "
            "applied_at, description) VALUES (?, ?, ?)", (6, now, "fixture v6"))
        conn.commit()
    finally:
        conn.close()


def _n_events(conn, fingerprint):
    from recoverland.core.event_stream_repository import count_events_after_cutoff
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    return count_events_after_cutoff(
        conn, fingerprint, RestoreCutoff(CutoffType.BY_DATE, _CUTOFF, True),
        trace_id="svc")


def setup(ctx):
    from recoverland.core.logger import flog
    tmpdir = tempfile.mkdtemp(prefix="rl_tx_svc_")
    project_path = str(Path(tmpdir) / "projet.qgz")
    Path(project_path).write_text("<qgis/>", encoding="utf-8")
    db_path = str(Path(tmpdir) / ".recoverland" / "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    _seed(db_path)
    ctx.data.update({"tmpdir": tmpdir, "project_path": project_path})
    # Le poste de l'histoire connait test ET prod (voir le docstring).
    ctx.data["saved_env"] = {k: os.environ.get(k)
                             for k in ("PGSERVICEFILE", "PGSYSCONFDIR")}
    service_file = str(Path(tmpdir) / "pg_service.conf")
    Path(service_file).write_text(
        "[test]\nhost=db-test\ndbname=gis\n\n[prod]\nhost=db-prod\n"
        "dbname=gis\n", encoding="utf-8")
    sysconf = str(Path(tmpdir) / "sysconf")
    os.makedirs(sysconf, exist_ok=True)
    os.environ["PGSERVICEFILE"] = service_file
    os.environ["PGSYSCONFDIR"] = sysconf
    flog(f"tx_prov_service_fingerprint setup: trace_id={ctx.trace_id}", "INFO")


def _restore_env(ctx):
    for key, value in (ctx.data.get("saved_env") or {}).items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def teardown(ctx):
    _restore_env(ctx)
    shutil.rmtree(ctx.data.get("tmpdir") or "", ignore_errors=True)


def run(ctx):
    from recoverland.core import identity as ident
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core import datasource_alias as da
    from recoverland.core.datasource_registry import lookup_datasource
    from recoverland.core.logger import flog

    d = ctx.data
    d["error"] = None
    fp = ident.compute_fingerprint_for_source
    d["fp_prod"] = fp("postgres", _URI_PROD)
    d["fp_test"] = fp("postgres", _URI_TEST)
    d["fp_host"] = fp("postgres", _URI_HOST)
    d["fp_svc_host"] = fp("postgres", _URI_SVC_HOST)
    d["fp_mssql_a"] = fp("mssql", _URI_MSSQL_A)
    d["fp_mssql_b"] = fp("mssql", _URI_MSSQL_B)
    d["selected_v6"] = ident.layer_is_selected(
        _Layer("postgres", _URI_PROD), {_FP_SERVICELESS})
    d["selected_other_table"] = ident.layer_is_selected(
        _Layer("postgres", _URI_PROD),
        {"postgres::host= port=5432 dbname= schema=public table=rivieres"})
    # Le vrai portail de capture : EditSessionTracker._is_tracked, avec la
    # selection telle que 6.x l'a enregistree.
    from recoverland.core.edit_tracker import EditSessionTracker
    tracker = EditSessionTracker(None, None)
    tracker.set_filter({_FP_SERVICELESS})
    d["tracked_prod_layer"] = tracker._is_tracked(d["fp_prod"])
    d["tracked_other_table"] = tracker._is_tracked(
        fp("postgres", _URI_PROD.replace('"routes"', '"rivieres"')))
    unproven = getattr(ident, "service_match_unproven", None)
    d["unproven_available"] = unproven is not None
    if unproven is not None:
        d["unproven_old_vs_prod"] = unproven(_FP_SERVICELESS, d["fp_prod"])
        d["unproven_test_vs_prod"] = unproven(d["fp_test"], d["fp_prod"])
    d["match_test_vs_prod"] = ident.datasource_fingerprints_match(
        d["fp_test"], d["fp_prod"])

    jm = JournalManager()
    try:
        jm.open_for_project(d["project_path"], d["tmpdir"])
        conn = jm.get_connection()
        d["aliases"] = [(a[0], a[1]) for a in da.list_aliases(conn)]
        report = da.load_reconciliation(conn)
        d["revision"] = getattr(report, "revision", None) if report else None
        d["current_revision"] = getattr(da, "RECONCILE_REVISION", None)
        d["ambiguous"] = [(a.fingerprint, a.reason)
                          for a in da.get_ambiguous_datasources(conn)]
        d["description"] = da.describe_reconciliation(conn)
        d["registry_hit"] = lookup_datasource(conn, _FP_SERVICELESS) is not None
        d["registry_hit_host"] = lookup_datasource(conn, _FP_HOST_V6) is not None
        d["n_prod"] = _n_events(conn, d["fp_prod"])
        d["n_serviceless"] = _n_events(conn, _FP_SERVICELESS)
    except Exception as exc:  # noqa: BLE001
        d["error"] = repr(exc)
    finally:
        jm.close()
        shutil.rmtree(d["tmpdir"], ignore_errors=True)
    flog(f"tx_prov_service_fingerprint run: trace_id={ctx.trace_id} "
         f"fp_prod={d.get('fp_prod')!r} fp_test={d.get('fp_test')!r} "
         f"aliases={d.get('aliases')} ambiguous={d.get('ambiguous')} "
         f"registry_hit={d.get('registry_hit')} n_prod={d.get('n_prod')} "
         f"error={d.get('error')}",
         "INFO" if d.get("error") is None else "ERROR")


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    amb = dict(d.get("ambiguous") or [])
    return [
        ("scenario_ran", d.get("error") is None,
         f"le scenario a plante : {d.get('error')}"),
        ("two_services_two_fingerprints",
         d.get("fp_prod") != d.get("fp_test"),
         f"service prod -> {d.get('fp_prod')!r}, service test -> "
         f"{d.get('fp_test')!r} : deux bases differentes partagent un "
         f"historique, et le rewind de l'une rejoue les editions de l'autre."),
        ("two_odbc_dsn_two_fingerprints",
         d.get("fp_mssql_a") != d.get("fp_mssql_b"),
         f"DSN a -> {d.get('fp_mssql_a')!r}, DSN b -> {d.get('fp_mssql_b')!r}"),
        ("host_uri_fingerprint_unchanged",
         d.get("fp_host") == _FP_HOST_V6,
         f"URI host/dbname -> {d.get('fp_host')!r}, attendu {_FP_HOST_V6!r} : "
         f"une source saine ne doit pas changer d'empreinte, sinon son "
         f"historique se detache."),
        ("explicit_host_with_service_unchanged",
         d.get("fp_svc_host") == _FP_HOST_V6
         and d.get("registry_hit_host") is True
         and _FP_HOST_V6 not in amb,
         f"URI service + hote + base explicites -> {d.get('fp_svc_host')!r}, "
         f"attendu {_FP_HOST_V6!r} ; registre -> {d.get('registry_hit_host')!r}"
         f" ; ambigus={d.get('ambiguous')} : les parametres explicites "
         f"designent deja la base, le service n'ajoute rien ; changer "
         f"l'empreinte detacherait un historique sain."),
        ("v6_tracking_selection_still_matches",
         d.get("selected_v6") is True and d.get("selected_other_table") is False,
         f"selection 6.x (forme sans service) -> {d.get('selected_v6')!r}, "
         f"autre table -> {d.get('selected_other_table')!r} : une couche "
         f"suivie qui ne reconnait plus sa selection n'est plus capturee, "
         f"sans un mot."),
        ("capture_continues_on_a_6x_selection",
         d.get("tracked_prod_layer") is True
         and d.get("tracked_other_table") is False,
         f"_is_tracked(couche prod) avec la selection 6.x -> "
         f"{d.get('tracked_prod_layer')!r}, autre table -> "
         f"{d.get('tracked_other_table')!r} : la couche suivie doit rester "
         f"capturee apres la mise a jour, sans capturer une table non "
         f"cochee."),
        ("new_events_of_two_services_never_match",
         d.get("match_test_vs_prod") is False,
         f"datasource_fingerprints_match(test, prod) -> "
         f"{d.get('match_test_vs_prod')!r} : un evenement capture sur test ne "
         f"doit designer aucune couche prod."),
        ("old_match_flagged_unproven_for_writers",
         d.get("unproven_available") is True
         and d.get("unproven_old_vs_prod") is True
         and d.get("unproven_test_vs_prod") is False,
         f"service_match_unproven(sans service, prod) -> "
         f"{d.get('unproven_old_vs_prod')!r}, (test, prod) -> "
         f"{d.get('unproven_test_vs_prod')!r} : celui qui va ECRIRE un ancien "
         f"evenement sur une couche doit pouvoir savoir que la correspondance "
         f"n'est pas prouvee."),
        # Hypothesis revised (v7): la revision suit RECONCILE_REVISION.
        ("reconciliation_reexamined",
         d.get("revision") == d.get("current_revision")
         and (d.get("current_revision") or 0) >= 2,
         f"revision du marqueur apres ouverture={d.get('revision')!r} : un "
         f"journal 6.x marque 'ok' doit etre reexamine sous les regles v7, "
         f"sinon ses empreintes ambigues ne sont jamais signalees."),
        ("serviceless_fingerprint_never_aliased",
         not any(a == _FP_SERVICELESS for a, _t in d.get("aliases") or []),
         f"alias={d.get('aliases')} : rattacher l'empreinte sans service a "
         f"prod attribuerait a prod les editions faites sur test."),
        ("serviceless_fingerprint_reported",
         amb.get(_FP_SERVICELESS) == "service_de_connexion_non_enregistre"
         and "service de connexion" in (d.get("description") or ""),
         f"ambigus={d.get('ambiguous')} message={d.get('description')!r} : "
         f"l'utilisateur doit savoir que l'historique ancien de cette couche "
         f"n'est pas rattache, et pourquoi."),
        ("registry_refuses_the_ambiguous_uri",
         d.get("registry_hit") is False,
         f"lookup_datasource(empreinte sans service) -> "
         f"{d.get('registry_hit')!r} : le registre reconstruirait une couche "
         f"sur prod pour y restaurer des evenements de test."),
        ("prod_rewind_replays_nothing_from_test",
         d.get("n_prod") == 0,
         f"evenements dans la portee d'un rewind de prod={d.get('n_prod')!r} "
         f"(attendu 0) : les editions de test seraient compensees sur les "
         f"lignes de prod."),
        ("nothing_lost",
         d.get("n_serviceless") == 3,
         f"evenements encore lisibles sous l'empreinte d'origine="
         f"{d.get('n_serviceless')!r} (attendu 3)."),
        assert_log_contains(
            ctx.records, rf"tx_prov_service_fingerprint run: trace_id={ctx.trace_id}",
            name="trace_id_propagated"),
    ]


if __name__ == "__main__":
    import sys
    if str(_PLUGIN_ROOT.parent) not in sys.path:
        sys.path.insert(0, str(_PLUGIN_ROOT.parent))
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
