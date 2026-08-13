"""tx_journal_path_hostile - RecoverLand validation runtime.

Invariant: I-JRN (le journal ecrit et le journal relu sont le MEME fichier).

Le mecanisme
------------
`JournalManager` ouvre le journal en ECRITURE avec un chemin brut::

    sqlite3.connect(self._path, check_same_thread=False)

mais toutes les connexions de LECTURE passent par une URI SQLite::

    sqlite3.connect(f"file:{self._path}?mode=ro", uri=True)

(`create_read_connection`, `open_readonly_connection`, et
`integrity.get_journal_health_report`).

Une URI n'est pas un chemin. SQLite decoupe `file:<chemin>?<params>#<ancre>`
et decode les sequences `%XX` du chemin. Consequence directe pour un chemin
Windows qui n'a jamais ete echappe :

  * un `#` dans un nom de dossier coupe l'URI : tout ce qui suit devient une
    ancre, donc le chemin est TRONQUE et le `?mode=ro` disparait avec. SQLite
    ouvre alors un AUTRE fichier, en lecture/ecriture, et le CREE s'il
    n'existe pas ;
  * une sequence `%41` dans un nom de dossier est decodee en `A`, donc le
    chemin vise ne designe plus le meme dossier.

Le scenario d'echec concret
---------------------------
Un projet range dans `D:\\SIG\\Lot #12 revision\\projet.qgz`. Le suivi des
modifications fonctionne : les evenements partent bien dans
`Lot #12 revision\\.recoverland\\recoverland_audit.sqlite` (connexion
ECRITURE, chemin brut). Mais REWIND et REVIEW lisent via
`VersionFetchThread` -> `journal.create_read_connection()`, c'est-a-dire via
l'URI tronquee. L'utilisateur voit donc :

  * soit "Aucun evenement apres cette date. Rien a restaurer." alors que son
    journal est plein ;
  * soit une erreur `no such table: audit_event` ;
  * et dans tous les cas un fichier fantome vide cree a cote de son projet.

Autrement dit : l'audit tourne, la recuperation est impossible, et rien ne
le signale. Pour un plugin de recuperation c'est la panne silencieuse la
plus grave apres la perte d'ecriture.

Ce que le scenario execute reellement
-------------------------------------
Pour chaque nom de dossier hostile (temoin ASCII, espace + accents, `#`,
sequence `%XX`, chemin tres long) :
    setup : un dossier temporaire + un faux .qgz dedans
    run   : JournalManager.open_for_project(), UN evenement insere par la
            connexion ECRITURE, puis relecture par les TROIS chemins de
            lecture du produit (create_read_connection,
            open_readonly_connection, get_journal_health_report) et
            inventaire des fichiers avant/apres pour detecter un fantome.

Verdict attendu si le produit est sain : les trois lectures retrouvent
l'evenement pour chaque cas, et aucun fichier n'apparait.

Note d'execution : le caractere `?` est interdit dans un nom de fichier
Windows, il ne peut donc pas etre teste ici (voir assertion
`case_interro_non_testable`).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCENARIO_ID = "tx_journal_path_hostile"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_DS_FP = "tx_jph_datasource"
_MARKER = "tx_jph_marker"

# (cle, nom de dossier). Le premier est le temoin : s'il echoue, ce n'est
# pas l'URI qui est en cause mais la fixture.
_CASES = [
    ("ascii", "projet_simple"),
    ("espace_accent", "Donnees du Reseau 2026 - releve terrain"),
    ("diese", "Lot #12 revision"),
    ("pourcent", "taux_%41_final"),
    ("long", None),  # construit dans setup()
]


def _long_dir_name() -> str:
    """Nom de dossier profond mais sous la limite MAX_PATH de Windows."""
    return "/".join(["dossier_de_travail_tres_long_%02d" % i for i in range(4)])


def _insert_event(conn: sqlite3.Connection, ts: str) -> int:
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    sql = "".join((
        "INSERT INTO audit_event (", AUDIT_EVENT_INSERT_SQL,
        ") VALUES (", AUDIT_EVENT_INSERT_PLACEHOLDERS, ")",
    ))
    row = (
        "tx_jph_project", _DS_FP, "lyr_jph", _MARKER, "ogr",
        json.dumps({"fid": 7}), "UPDATE",
        json.dumps({"changed_only": {"nom": {"old": "A", "new": "B"}}}),
        None, "NoGeometry", "EPSG:4326", None,
        "tester", None, ts, None, "fid:7", 2, None, None,
    )
    cur = conn.execute(sql, row)
    conn.commit()
    return cur.lastrowid


def _tree(root: str) -> set:
    out = set()
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            full = os.path.join(dirpath, name)
            out.add(os.path.relpath(full, root))
    return out


def _count_marker(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM audit_event WHERE layer_name_snapshot = ?",
        (_MARKER,),
    ).fetchone()
    return int(row[0] if row else 0)


def _safe(fn):
    """Execute fn() et renvoie (valeur, erreur_str)."""
    try:
        return fn(), ""
    except Exception as exc:  # noqa: BLE001 - toute panne est un resultat
        return None, f"{type(exc).__name__}: {exc}"


def setup(ctx):
    from recoverland.core.logger import flog

    base = tempfile.mkdtemp(prefix="rl_tx_jph_")
    cases = []
    for key, name in _CASES:
        dir_name = _long_dir_name() if name is None else name
        case_dir = os.path.join(base, dir_name)
        try:
            os.makedirs(case_dir, exist_ok=True)
            project_path = os.path.join(case_dir, "projet.qgz")
            with open(project_path, "w", encoding="utf-8") as fh:
                fh.write("<qgis/>")
            creatable = True
            create_err = ""
        except OSError as exc:
            project_path = ""
            creatable = False
            create_err = f"{type(exc).__name__}: {exc}"
        cases.append({
            "key": key,
            "dir_name": dir_name,
            "project_path": project_path,
            "creatable": creatable,
            "create_error": create_err,
        })

    # Le `?` est un caractere interdit par le systeme de fichiers Windows :
    # on le prouve plutot que de l'affirmer.
    interro_dir = os.path.join(base, "question ? ouverte")
    interro_creatable, interro_err = _safe(
        lambda: os.makedirs(interro_dir, exist_ok=True))
    ctx.data["interro_error"] = interro_err

    ctx.data["base"] = base
    ctx.data["cases"] = cases
    ctx.data["profile_dir"] = base

    flog(
        f"tx_journal_path_hostile setup: trace_id={ctx.trace_id} base={base} "
        f"n_cases={len(cases)} interro_err={interro_err!r}",
        "INFO",
    )


def _exercise_case(ctx, case: dict) -> dict:
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.integrity import get_journal_health_report
    from recoverland.core.logger import flog

    base = ctx.data["base"]
    res = {"key": case["key"], "dir_name": case["dir_name"]}
    if not case["creatable"]:
        res["skipped"] = case["create_error"]
        return res

    jm = JournalManager()
    ts = datetime(2026, 8, 13, 9, 0, 0, tzinfo=timezone.utc).isoformat()
    try:
        path = jm.open_for_project(case["project_path"], ctx.data["profile_dir"])
        res["journal_path"] = path
        res["journal_exists"] = os.path.isfile(path)

        eid = _insert_event(jm.get_connection(), ts)
        res["event_id"] = eid
        res["rw_count"] = _count_marker(jm.get_connection())

        tree_before = _tree(base)

        # --- 1. connexion de lecture des threads de recherche / rewind ----
        def _read_conn():
            conn = jm.create_read_connection()
            try:
                return _count_marker(conn)
            finally:
                conn.close()
        res["ro_count"], res["ro_error"] = _safe(_read_conn)

        # --- 2. connexion Review / Time Lens ------------------------------
        def _lens_conn():
            conn = jm.open_readonly_connection()
            try:
                return _count_marker(conn)
            finally:
                conn.close()
        res["lens_count"], res["lens_error"] = _safe(_lens_conn)

        # --- 3. rapport de sante (meme construction d'URI) -----------------
        def _health():
            return get_journal_health_report(path).total_events
        res["health_count"], res["health_error"] = _safe(_health)

        tree_after = _tree(base)
        res["new_files"] = sorted(tree_after - tree_before)
    finally:
        jm.close()

    flog(
        f"tx_journal_path_hostile case={case['key']}: trace_id={ctx.trace_id} "
        f"rw={res.get('rw_count')} ro={res.get('ro_count')!r} "
        f"lens={res.get('lens_count')!r} health={res.get('health_count')!r} "
        f"new_files={res.get('new_files')} "
        f"ro_error={res.get('ro_error')!r} lens_error={res.get('lens_error')!r}",
        "INFO",
    )
    return res


def run(ctx):
    from recoverland.core.logger import flog

    flog(f"tx_journal_path_hostile run start: trace_id={ctx.trace_id}", "INFO")

    results = {}
    for case in ctx.data["cases"]:
        results[case["key"]] = _exercise_case(ctx, case)
    ctx.data["results"] = results

    flog(
        f"tx_journal_path_hostile run end: trace_id={ctx.trace_id} "
        f"cases={sorted(results)}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("base", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    results = ctx.data.get("results") or {}

    # ===== Temoin : la fixture elle-meme est saine =======================
    ascii_res = results.get("ascii") or {}
    out.append((
        "temoin_ascii_lisible",
        ascii_res.get("rw_count") == 1 and ascii_res.get("ro_count") == 1,
        f"cas temoin (dossier ASCII) : ecriture={ascii_res.get('rw_count')} "
        f"lecture={ascii_res.get('ro_count')!r} attendu 1/1. Si ce cas echoue, "
        f"c'est la fixture qui est cassee, pas le produit "
        f"(err={ascii_res.get('ro_error')!r})",
    ))

    # ===== Le trou : lecture et ecriture doivent viser le meme fichier ====
    for key in ("espace_accent", "diese", "pourcent", "long"):
        res = results.get(key) or {}
        if res.get("skipped"):
            out.append((
                f"{key}_dossier_creable",
                False,
                f"le dossier '{res.get('dir_name')}' n'a pas pu etre cree : "
                f"{res['skipped']} - cas non teste",
            ))
            continue

        rw = res.get("rw_count")
        out.append((
            f"{key}_ecriture_ok",
            rw == 1,
            f"dossier '{res.get('dir_name')}' : la connexion ECRITURE devrait "
            f"contenir 1 evenement, elle en contient {rw!r}. "
            f"journal={res.get('journal_path')!r}",
        ))
        out.append((
            f"{key}_relecture_voit_l_evenement",
            res.get("ro_count") == 1,
            f"dossier '{res.get('dir_name')}' : create_read_connection lit "
            f"{res.get('ro_count')!r} evenement(s), attendu 1 "
            f"(err={res.get('ro_error')!r}). C'est le chemin utilise par "
            f"VersionFetchThread : s'il ne retrouve pas l'evenement, REWIND "
            f"annonce 'rien a restaurer' sur un journal pourtant plein.",
        ))
        out.append((
            f"{key}_review_voit_l_evenement",
            res.get("lens_count") == 1,
            f"dossier '{res.get('dir_name')}' : open_readonly_connection "
            f"(mode REVIEW / Time Lens) lit {res.get('lens_count')!r} "
            f"evenement(s), attendu 1 (err={res.get('lens_error')!r}). "
            f"Sans cette lecture, l'utilisateur ne peut pas consulter son "
            f"historique.",
        ))
        out.append((
            f"{key}_sante_coherente",
            res.get("health_count") == 1,
            f"dossier '{res.get('dir_name')}' : get_journal_health_report "
            f"annonce {res.get('health_count')!r} evenement(s), attendu 1 "
            f"(err={res.get('health_error')!r}). Un compte faux fait afficher "
            f"un journal vide dans la fenetre de maintenance et invite a "
            f"purger un fichier qui contient tout.",
        ))
        out.append((
            f"{key}_aucun_fichier_fantome",
            not res.get("new_files"),
            f"dossier '{res.get('dir_name')}' : ouvrir le journal en LECTURE a "
            f"cree {res.get('new_files')!r}, attendu aucun fichier. Une "
            f"lecture ne doit jamais creer de base a cote du projet : ce "
            f"fichier vide sera pris pour le journal aux ouvertures suivantes.",
        ))

    # ===== Limite du banc : le '?' n'est pas testable sur Windows =========
    out.append((
        "case_interro_non_testable",
        bool(ctx.data.get("interro_error")),
        f"le caractere '?' devait etre rejete par le systeme de fichiers pour "
        f"justifier son absence du banc ; erreur observee="
        f"{ctx.data.get('interro_error')!r} (vide = le dossier a ete cree, "
        f"donc le cas '?' aurait du etre teste)",
    ))

    # ===== Traces ========================================================
    out.append(assert_log_contains(
        ctx.records,
        rf"tx_journal_path_hostile.*trace_id={ctx.trace_id}",
        name="trace_id_propage",
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
