# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""tx_life_wq_stop_restart - RecoverLand validation runtime.

Invariant: I-WQ-LIFECYCLE (le cycle de vie de la file d'ecriture ne perd
ni ne deplace jamais un evenement).

Le mecanisme (avant v7)
-----------------------
`WriteQueue` est le seul chemin entre une edition capturee et le journal
SQLite. Le producteur (thread UI) appelle `enqueue(events)` et lit la
valeur de retour comme un accuse de reception : `True` veut dire
"c'est enregistre, tu peux oublier". Le consommateur est un thread
unique (`RecoverLand-Writer`) demarre par `start(db_path)` et arrete par
`stop()`.

Le probleme est que `enqueue()` ne regarde jamais si ce consommateur
existe. Elle valide les evenements, les pousse dans un `queue.Queue`
Python et renvoie `True`, que le thread ecrivain soit demarre, arrete,
ou jamais lance. Le `queue.Queue` survit a l'arret du thread : il est
cree une seule fois dans `__init__` et n'est jamais vide par `stop()`.

Les deux scenarios d'echec concrets
-----------------------------------
1. Fermeture / changement de projet. `RecoverPlugin._shutdown_local_backend`
   fait `tracker.deactivate()` puis `write_queue.stop()`. Tout evenement
   qui arrive apres ce `stop()` (commit de couche en cours de fermeture,
   callback QGIS en retard) est accepte avec `True` et reste bloque en
   RAM. Personne ne le reecrit dans le fichier `recoverland_pending.json`
   prevu exactement pour ca. L'edition est perdue sans un seul message.

2. Enchainement projet A -> projet B. `_on_project_opened` appelle
   `_shutdown_local_backend()` (stop) puis `_init_local_backend()`
   (`start(journal_de_B)`). Le thread ecrivain qui redemarre draine la
   file *existante* : les evenements du projet A restes en file sont
   ecrits dans le journal du projet B, avec les empreintes de A. Le
   journal de B contient alors un historique qui ne lui appartient pas,
   et un REWIND sur B ira chercher des entites qui n'y ont jamais existe.

Hypotheses revisees (v7)
------------------------
Depuis le schema v7, `enqueue()` ecrit (fsync) le lot dans l'outbox de son
journal (`<journal>.outbox/`) AVANT toute mise en file RAM, et rend une
issue explicite `EnqueueOutcome` au lieu d'un booleen ; `bool(issue)` ne
vaut True que pour les trois issues durables. Chaque `start()` cree une
generation isolee (file, outbox, journal, thread). Ce qui change ici :

* Les tests d'identite `is True` / `is False` visaient l'ancienne API
  booleenne. Ils comparent maintenant le NOM exact de l'issue, ce qui est
  plus strict : QUEUED_DURABLE et PERSISTED_FALLBACK sont tous deux vrais
  mais ne disent pas la meme chose a l'utilisateur.
* S4 : apres `stop()`, le lot n'est plus refuse, il est durable dans
  l'outbox du journal A (PERSISTED_FALLBACK, ligne ERROR
  `event=wq_enqueue_no_writer`). La preuve n'est plus "25 entrees dans
  recoverland_pending.json" (le chemin d'ecriture ne s'en sert plus) mais
  la propriete elle-meme, mesuree a deux instants :
  - A L'ACCUSE DE RECEPTION, avant toute autre etape : l'outbox de A
    contient les 25 evenements, et une COPIE DISQUE du dossier du journal
    (fichier SQLite + `<journal>.outbox/`) prise des le retour
    d'`enqueue()` est integree par `restart_integrate` dans un PROCESSUS
    ENFANT (`crash_harness.run_child`) : 25 evenements, une seule fois.
    Rien de la memoire du processus de test ne peut atteindre cette
    copie : c'est ce que trouverait un redemarrage apres un plantage a
    cet instant precis. Un `restart_integrate` dans le meme processus ne
    le prouverait pas (la RAM survit au "redemarrage").
  - apres S5, deux redemarrages sur le journal A reel : 25 evenements
    exactement une fois, puis aucun changement (idempotence).
* `pending_count` et `health()` lisent la generation COURANTE : apres
  `stop()` (ou sans `start()`) ils valent 0 par construction. Ils ne
  servent plus de preuve "rien ne reste en RAM" ; les sondes disque
  (copie a l'accuse, `outbox_events`, `count_rows`) les remplacent.
* S7 : une file jamais demarree n'a aucun chemin durable. L'issue est
  REFUSED_NO_DURABLE_PATH, comptee dans `lost_events` et criee en
  CRITICAL (`event=wq_events_lost`). Preuve disque qu'aucun evenement
  refuse n'est garde en RAM : le MEME objet est ensuite demarre sur un
  journal neuf, et aucun evenement refuse n'y apparait (ni journal, ni
  outbox). Un lot invalide sans journal rend REFUSED_INVALID avec une
  ligne ERROR `event=wq_enqueue_rejected` ; comme aucune quarantaine ne
  peut etre ecrite, il est compte PERDU (`lost_events` +1, ligne CRITICAL
  `event=wq_events_lost`) et jamais compte comme preserve
  (`quarantined_events` inchange).
* S8 : les producteurs journalisaient la valeur de retour brute
  (`accepted=EnqueueOutcome.QUEUED_DURABLE`), si bien que le controle
  `accepted in ("True", "False")` echouait sur les 320 lignes. Les lignes
  etaient pourtant entieres : pas de rotation, pas d'entrelacement ; c'est
  l'extrait `raw[:100]` du diagnostic qui tombait pile apres `event=`. Le
  scenario journalise maintenant `accepted=bool(issue)` et
  `outcome=<nom>`, le controle verifie les deux et le diagnostic montre
  les champs fautifs au lieu d'une coupe a 100 caracteres.
* Les gardes de source cherchaient des noms prives disparus (`_running`,
  `_drain_all`, `save_pending`). Elles epinglent le nouveau mecanisme, par
  l'AST (commentaires et chaines ne comptent pas) : aucune issue durable
  (QUEUED_DURABLE, PERSISTED_FALLBACK, PERSISTED_JOURNAL_SYNC) n'est
  nommee ni rendue avant le premier `append_events` d'`enqueue()`, sauf
  le retour du lot vide ; file RAM seulement apres l'outbox et seulement
  si l'ecrivain est vivant et accepte, sinon PERSISTED_FALLBACK ;
  `stop()` vide la RAM et dit ce qui reste dans l'outbox ; chaque sortie
  de la boucle de l'ecrivain est soit la sortie propre (arret demande,
  aucun lot a reessayer, file vide), soit couverte par le dernier flush
  du `finally`, qui recoit les lots a reessayer et vide la file RAM.

Ce que ce scenario torture (journal SQLite reel, aucun objet QGIS) :
    S1  flush a l'arret : 3000 evenements en file puis stop() immediat
    S2  double start() : un seul thread ecrivain, aucun doublon
    S3  stop() sans start(), stop() deux fois
    S4  enqueue() apres stop()          <- issue explicite, sur disque des
                                           l'accuse, integre une seule fois
                                           au redemarrage
    S5  restart sur un AUTRE journal    <- contamination inter-projets
    S6  aucune fuite de thread apres le cycle complet
    S7  enqueue() sur une file jamais demarree  <- refus compte et crie
    S8  8 producteurs concurrents : atomicite par lot + journalisation
        concurrente encore parsable par scripts/validation/parse_log.py
"""
from __future__ import annotations

import os
import re
import shutil
import sqlite3
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path

SCENARIO_ID = "tx_life_wq_stop_restart"
INVARIANT = "I-WQ-LIFECYCLE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

_WRITER_THREAD_NAME = "RecoverLand-Writer"
_PENDING_FILENAME = "recoverland_pending.json"


# --------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------- #

def _make_journal(root: str, name: str) -> str:
    """Cree un journal SQLite initialise dans son propre sous-dossier.

    Un dossier par journal : le fichier pending historique (encore importe
    au demarrage par core/integrity.py) vit a cote du .sqlite, donc deux
    journaux dans le meme dossier partageraient leur fichier de secours et
    fausseraient le comptage apres redemarrage.
    """
    from recoverland.core.sqlite_schema import initialize_schema

    path = os.path.join(root, name, "recoverland_audit.sqlite")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.close()
    return path


def _event(idx: int, datasource: str = "ds_life", session: str = "s0"):
    from recoverland.core.audit_backend import AuditEvent

    return AuditEvent(
        event_id=None,
        project_fingerprint="tx_life_project",
        datasource_fingerprint=datasource,
        layer_id_snapshot="lyr_life",
        layer_name_snapshot="parcelles",
        provider_type="ogr",
        feature_identity_json='{"fid": %d}' % idx,
        operation_type="UPDATE",
        attributes_json='{"changed_only": {"statut": {"old": "A", "new": "B"}}}',
        geometry_wkb=None,
        geometry_type="NoGeometry",
        crs_authid="EPSG:4326",
        field_schema_json='[{"name": "statut", "type": "string"}]',
        user_name="tester",
        session_id=session,
        created_at="2026-08-13T10:00:00+00:00",
        restored_from_event_id=None,
        entity_fingerprint="fid:%d" % idx,
        event_schema_version=2,
        new_geometry_wkb=None,
        invalidated_at=None,
    )


def _count(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0])
    finally:
        conn.close()


def _sessions(db_path: str) -> Counter:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT session_id, COUNT(*) FROM audit_event GROUP BY session_id"
        ).fetchall()
    finally:
        conn.close()
    return Counter({row[0]: int(row[1]) for row in rows})


def _writer_threads() -> int:
    return len([t for t in threading.enumerate() if t.name == _WRITER_THREAD_NAME])


def _pending_len(db_path: str) -> int:
    import json

    path = os.path.join(os.path.dirname(db_path), _PENDING_FILENAME)
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return -1
    return len(data) if isinstance(data, list) else -1


def _outcome_name(res) -> str:
    """Nom de l'issue d'enqueue() (EnqueueOutcome), repr sinon."""
    return getattr(res, "name", repr(res))


def _health(wq) -> dict:
    return wq.health()._asdict() if hasattr(wq, "health") else {}


def _log_hits(records, pattern: str, level: str) -> list:
    rx = re.compile(pattern)
    return [r for r in records if r.level == level and rx.search(r.raw)]


# --------------------------------------------------------------------- #
# Preuve disque a l'accuse de reception
# --------------------------------------------------------------------- #

_CHILD_INTEGRATE = """
from recoverland.scripts.validation.durability import restart_integrate
res = restart_integrate({db!r})
print("RESULT " + json.dumps(res, default=str), flush=True)
os._exit(0)
"""


def _snapshot_journal(db_path: str, dest_dir: str) -> str:
    """Copie disque du dossier du journal, tel qu'il est a cet instant.

    Le dossier entier : fichier SQLite (et -wal s'il existe),
    `<journal>.outbox/`, fichier pending historique. C'est exactement ce
    qu'un redemarrage apres un plantage trouverait ; rien de la memoire du
    processus ne passe dans la copie. Retourne le chemin du journal copie.
    """
    shutil.copytree(os.path.dirname(os.path.abspath(db_path)), dest_dir)
    return os.path.join(dest_dir, os.path.basename(db_path))


def _integrate_in_child(db_path: str, session: str, workroot: str) -> dict:
    """Redemarrage du plugin sur *db_path* dans un processus neuf, puis comptage.

    Le processus enfant (crash_harness) n'herite d'aucun etat du processus
    de test : ce qu'il integre etait sur le disque. Sa config QGIS est
    isolee pour que ses lignes de log ne se melent pas a la fenetre de log
    du scenario.
    """
    from scripts.validation.crash_harness import run_child
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events,
    )

    out = {"outbox_before": outbox_events(db_path, session)}
    cfg = os.path.join(workroot, "child_cfg")
    work = os.path.join(workroot, "child_work")
    os.makedirs(cfg, exist_ok=True)
    os.makedirs(work, exist_ok=True)
    try:
        rc, so, se = run_child(_CHILD_INTEGRATE.format(db=db_path), timeout=180,
                               extra_env={"QGIS_CUSTOM_CONFIG_PATH": cfg},
                               workdir=work)
    except Exception as exc:  # noqa: BLE001 - a child that cannot run is a FAIL
        out.update(rc=None, error=f"{type(exc).__name__}: {exc}")
        return out
    import json

    replay = None
    for line in so.splitlines():
        if line.startswith("RESULT "):
            try:
                replay = (json.loads(line[len("RESULT "):]) or {}).get("replay")
            except ValueError:
                replay = "unparsable"
    out.update(
        rc=rc,
        rows=count_rows(db_path, "session_id = ?", (session,)),
        total=count_rows(db_path),
        dups=duplicate_uids(db_path),
        outbox_left=outbox_events(db_path),
        replay=replay,
    )
    if rc != 0:
        out["stderr_tail"] = (se or "")[-400:]
    return out


# --------------------------------------------------------------------- #
# Gardes de source (AST)
# --------------------------------------------------------------------- #

def _method_node(src: str, cls: str, name: str):
    """Noeud AST exact d'une methode (pas de coupe de texte par voisinage)."""
    import ast

    for node in ast.parse(src).body:
        if isinstance(node, ast.ClassDef) and node.name == cls:
            for item in node.body:
                if (isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and item.name == name):
                    return item
    return None


def _pos(node) -> tuple:
    return (node.lineno, node.col_offset)


def _calls(root, attr: str) -> list:
    """Appels `<x>.<attr>(...)` sous *root*, dans l'ordre du source."""
    import ast

    return sorted((n for n in ast.walk(root)
                   if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                   and n.func.attr == attr), key=_pos)


def _parents(root) -> dict:
    import ast

    return {id(child): parent for parent in ast.walk(root)
            for child in ast.iter_child_nodes(parent)}


def _attrs_read(node) -> set:
    import ast

    return {n.attr for n in ast.walk(node) if isinstance(n, ast.Attribute)}


def _enqueue_guard(src: str) -> tuple:
    """Controles AST sur WriteQueue.enqueue ; retourne (checks, details)."""
    import ast
    from recoverland.core.write_queue import EnqueueOutcome

    durable = {o.name for o in EnqueueOutcome if bool(o)}
    fn = _method_node(src, "WriteQueue", "enqueue")
    if fn is None:
        return {"enqueue_found": False}, {}
    appends = _calls(fn, "append_events")
    first = _pos(appends[0]) if appends else None

    def _before_append(node) -> bool:
        return first is None or _pos(node) < first

    # Seule exception admise : `if not events and commit is None:` dont le
    # corps est exactement `return EnqueueOutcome.QUEUED_DURABLE` (un lot
    # vide sans commit n'a rien a rendre durable).
    exempt = set()
    for n in ast.walk(fn):
        if (isinstance(n, ast.If) and not n.orelse and len(n.body) == 1
                and ast.unparse(n.test) == "not events and commit is None"
                and isinstance(n.body[0], ast.Return)
                and n.body[0].value is not None
                and ast.unparse(n.body[0].value) == "EnqueueOutcome.QUEUED_DURABLE"):
            exempt.update(id(x) for x in ast.walk(n.body[0]))

    def _outcome_attr(node):
        if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                and node.value.id == "EnqueueOutcome"):
            return node.attr
        return None

    early_durable = sorted(
        n.lineno for n in ast.walk(fn)
        if _outcome_attr(n) in durable and id(n) not in exempt and _before_append(n))
    early_returns = sorted(
        n.lineno for n in ast.walk(fn)
        if isinstance(n, ast.Return) and id(n) not in exempt and _before_append(n)
        and not ((_outcome_attr(n.value) or "").startswith("REFUSED_")))

    parents = _parents(fn)
    puts = [c for c in _calls(fn, "put")
            if isinstance(c.func.value, ast.Attribute) and c.func.value.attr == "queue"]

    def _guarded_by_live_writer(call) -> bool:
        node = parents.get(id(call))
        while node is not None and not isinstance(node, ast.If):
            node = parents.get(id(node))
        if node is None:
            return False
        if {"alive", "accepting"} <= _attrs_read(node.test):
            return True
        if isinstance(node.test, ast.Name):
            assigns = [a for a in ast.walk(fn)
                       if isinstance(a, ast.Assign) and _pos(a) < _pos(node)
                       and any(isinstance(t, ast.Name) and t.id == node.test.id
                               for t in a.targets)]
            if assigns:
                last = max(assigns, key=_pos)
                return {"alive", "accepting"} <= _attrs_read(last.value)
        return False

    fallback_after = [n.lineno for n in ast.walk(fn)
                      if isinstance(n, ast.Return)
                      and _outcome_attr(n.value) == "PERSISTED_FALLBACK"
                      and not _before_append(n)]
    checks = {
        "append_events_called": bool(appends),
        "no_durable_outcome_before_append": bool(appends) and not early_durable,
        "only_refusals_returned_before_append": bool(appends) and not early_returns,
        "ram_put_after_append": bool(puts) and bool(appends)
        and all(_pos(p) > first for p in puts),
        "ram_put_only_if_writer_alive_and_accepting": bool(puts)
        and all(_guarded_by_live_writer(p) for p in puts),
        "explicit_fallback_after_append": bool(fallback_after),
    }
    details = {
        "first_append_line": first[0] if first else None,
        "durable_refs_before_append": early_durable,
        "non_refusal_returns_before_append": early_returns,
        "queue_put_lines": [p.lineno for p in puts],
        "fallback_return_lines": fallback_after,
        "empty_batch_exemption_found": bool(exempt),
    }
    return checks, details


def _stop_guard(src: str) -> tuple:
    """Controles AST sur stop(), la boucle de l'ecrivain et le dernier flush."""
    import ast

    stop_fn = _method_node(src, "WriteQueue", "stop")
    loop_fn = _method_node(src, "WriteQueue", "_writer_loop")
    flush_fn = _method_node(src, "WriteQueue", "_final_flush")
    if stop_fn is None or loop_fn is None or flush_fn is None:
        return {"methods_found": False}, {}

    def _is_flog_marker(node, marker: str) -> bool:
        return (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "flog"
                and any(isinstance(c, ast.Constant) and isinstance(c.value, str)
                        and marker in c.value
                        for a in node.args for c in ast.walk(a)))

    def _is_call_on(node, obj_attr: str, meth: str) -> bool:
        return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == meth
                and isinstance(node.func.value, ast.Attribute)
                and node.func.value.attr == obj_attr)

    def _clean_exit_test(test) -> bool:
        # `gen.stop_event.is_set() and not retry and gen.queue.empty()`
        if not (isinstance(test, ast.BoolOp) and isinstance(test.op, ast.And)):
            return False
        parts = test.values
        return (any(_is_call_on(p, "stop_event", "is_set") for p in parts)
                and any(isinstance(p, ast.UnaryOp) and isinstance(p.op, ast.Not)
                        and isinstance(p.operand, ast.Name) and p.operand.id == "retry"
                        for p in parts)
                and any(_is_call_on(p, "queue", "empty") for p in parts))

    loops = sorted((n for n in ast.walk(loop_fn) if isinstance(n, ast.While)), key=_pos)
    loop = loops[0] if loops else None
    parents = _parents(loop_fn)
    exits = []   # (ligne, nature, classe)
    if loop is not None:
        if not (isinstance(loop.test, ast.Constant) and loop.test.value is True):
            exits.append((loop.lineno, "while-test",
                          "clean" if _clean_exit_test(loop.test) else "other"))
        # break : ceux qui appartiennent a CETTE boucle (pas a une boucle
        # imbriquee) ; return : n'importe ou dans la boucle.
        stack = list(loop.body)
        while stack:
            n = stack.pop()
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda,
                              ast.ClassDef)):
                continue
            if isinstance(n, ast.Break):
                par = parents.get(id(n))
                clean = (isinstance(par, ast.If) and n in par.body
                         and _clean_exit_test(par.test))
                exits.append((n.lineno, "break", "clean" if clean else "other"))
            if isinstance(n, ast.Return):
                exits.append((n.lineno, "return", "other"))
            for child in ast.iter_child_nodes(n):
                if isinstance(child, (ast.For, ast.While, ast.AsyncFor)):
                    stack.extend(r for r in ast.walk(child)
                                 if isinstance(r, ast.Return))
                    continue
                stack.append(child)
    exits.sort()

    # Le finally qui entoure la boucle appelle _final_flush avec les lots a
    # reessayer (`retry`), et _final_flush ecrit ces lots ET vide la RAM.
    finally_flush = False
    node = parents.get(id(loop)) if loop is not None else None
    while node is not None:
        if isinstance(node, ast.Try) and node.finalbody and any(
                loop is x for b in node.body for x in ast.walk(b)):
            finally_flush = finally_flush or any(
                isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute)
                and c.func.attr == "_final_flush"
                and any(isinstance(a, ast.Name) and a.id == "retry" for a in c.args)
                for s in node.finalbody for c in ast.walk(s))
        node = parents.get(id(node))
    params = [a.arg for a in flush_fn.args.args]
    retry_param = params[3] if len(params) > 3 else None
    flush_drains = (
        retry_param is not None
        and any(isinstance(n, ast.Name) and n.id == retry_param
                and isinstance(n.ctx, ast.Load) for n in ast.walk(flush_fn))
        and bool(_calls(flush_fn, "_drain_ram"))
        and bool(_calls(flush_fn, "_write_batches")))
    others = [e for e in exits if e[2] != "clean"]
    checks = {
        "stop:_drain_ram": bool(_calls(stop_fn, "_drain_ram")),
        "stop:wq_stop_left_in_outbox": any(
            _is_flog_marker(n, "event=wq_stop_left_in_outbox") for n in ast.walk(stop_fn)),
        "loop:clean_exit_on_stop_no_retry_empty_queue": any(
            e[2] == "clean" for e in exits),
        "loop:finally_final_flush_gets_retry": finally_flush,
        "final_flush:writes_retry_and_drains_ram": flush_drains,
        "loop:every_other_exit_covered_by_final_flush": (
            not others or (finally_flush and flush_drains)),
    }
    return checks, {"loop_line": loop.lineno if loop is not None else None,
                    "exits": exits}


# --------------------------------------------------------------------- #
# Scenario
# --------------------------------------------------------------------- #

def setup(ctx):
    from recoverland.core.logger import flog

    tmpdir = tempfile.mkdtemp(prefix="rl_tx_life_wq_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["writers_before"] = _writer_threads()
    ctx.data["db"] = {
        name: _make_journal(tmpdir, name)
        for name in ("flush", "afterstop", "projectB", "double", "flood",
                     "after_never")
    }

    flog(
        f"tx_life_wq_stop_restart setup: trace_id={ctx.trace_id} "
        f"tmpdir={tmpdir} writers_before={ctx.data['writers_before']}",
        "INFO",
    )


def run(ctx):
    from recoverland.core.write_queue import WriteQueue
    from recoverland.core.logger import flog, flog_kv
    from scripts.validation.durability import (
        count_rows, duplicate_uids, outbox_events, restart_integrate,
    )

    dbs = ctx.data["db"]
    flog(f"tx_life_wq_stop_restart run start: trace_id={ctx.trace_id}", "INFO")

    # ===== S1: flush a l'arret ==========================================
    # 3000 evenements poses d'un coup puis stop() sans laisser au thread
    # le temps de tourner : c'est la fermeture de QGIS juste apres un gros
    # commit. Le contrat de stop() dit "flushing remaining events".
    wq = WriteQueue()
    wq.start(dbs["flush"])
    accepted = wq.enqueue([_event(i, session="flush") for i in range(3000)])
    wq.stop()
    ctx.data["s1_accepted"] = _outcome_name(accepted)
    ctx.data["s1_written"] = _count(dbs["flush"])
    ctx.data["s1_pending"] = _pending_len(dbs["flush"])
    ctx.data["s1_dup_uids"] = duplicate_uids(dbs["flush"])
    ctx.data["s1_outbox_left"] = outbox_events(dbs["flush"])

    # ===== S2: double start() ===========================================
    # Deux initialisations rapprochees (projet rouvert deux fois de suite).
    wq = WriteQueue()
    wq.start(dbs["double"])
    wq.start(dbs["double"])
    ctx.data["s2_writer_threads"] = _writer_threads() - ctx.data["writers_before"]
    wq.enqueue([_event(i, session="double") for i in range(200)])
    wq.stop()
    ctx.data["s2_written"] = _count(dbs["double"])

    # ===== S3: stop() sans start(), stop() deux fois ====================
    idle = WriteQueue()
    try:
        idle.stop()
        idle.stop()
        ctx.data["s3_error"] = ""
    except Exception as exc:  # noqa: BLE001
        ctx.data["s3_error"] = repr(exc)

    # ===== S7: enqueue() sur une file jamais demarree ===================
    # Cas reel : l'ouverture du journal a echoue (verrou multi-instance,
    # dossier en lecture seule) donc start() n'a jamais ete appele, mais
    # le tracker, lui, continue de capturer et d'appeler enqueue().
    valid = idle.enqueue([_event(1, session="never")])
    ctx.data["s7_valid_accepted"] = _outcome_name(valid)
    ctx.data["s7_valid_truthy"] = bool(valid)
    ctx.data["s7_health_after_valid"] = _health(idle)
    bad = _event(2, session="never")._replace(operation_type="BOGUS")
    invalid = idle.enqueue([bad])
    ctx.data["s7_invalid_accepted"] = _outcome_name(invalid)
    ctx.data["s7_invalid_truthy"] = bool(invalid)
    ctx.data["s7_health_after_invalid"] = _health(idle)
    ctx.data["s7_db_path"] = idle._db_path
    # Sonde disque "rien de refuse n'est garde en RAM" : pending_count vaut 0
    # par construction sans generation, il ne prouve rien. Le MEME objet est
    # demarre sur un journal neuf ; un evenement refuse mais garde en memoire
    # y reapparaitrait (journal ou outbox), dans le journal d'un autre projet.
    idle.start(dbs["after_never"])
    idle.stop()
    ctx.data["s7_resurfaced_rows"] = count_rows(
        dbs["after_never"], "session_id = ?", ("never",))
    ctx.data["s7_resurfaced_outbox"] = outbox_events(dbs["after_never"], "never")

    # ===== S4: enqueue() apres stop() ===================================
    wq = WriteQueue()
    wq.start(dbs["afterstop"])
    wq.stop()
    after_stop = wq.enqueue(
        [_event(i, session="afterstop") for i in range(25)]
    )
    # Instant de l'accuse de reception : rien d'autre ne tourne avant ces
    # deux mesures. La copie disque fige ce qu'un plantage laisserait.
    ack_snapshot_db = _snapshot_journal(
        dbs["afterstop"], os.path.join(ctx.data["tmpdir"], "s4_ack_snapshot"))
    ctx.data["s4_outbox_after_enqueue"] = outbox_events(dbs["afterstop"], "afterstop")
    ctx.data["s4_accepted"] = _outcome_name(after_stop)
    ctx.data["s4_truthy"] = bool(after_stop)
    ctx.data["s4_degraded"] = bool(getattr(after_stop, "degraded", False))
    ctx.data["s4_ack_snapshot"] = _integrate_in_child(
        ack_snapshot_db, "afterstop", os.path.join(ctx.data["tmpdir"], "s4_child"))
    time.sleep(0.3)
    ctx.data["s4_written"] = _count(dbs["afterstop"])
    ctx.data["s4_pending"] = _pending_len(dbs["afterstop"])

    # ===== S5: le meme objet redemarre sur le journal du projet B =======
    # Exactement la sequence de RecoverPlugin._on_project_opened.
    wq.start(dbs["projectB"])
    time.sleep(0.5)
    wq.stop()
    ctx.data["s5_projectA_rows"] = _count(dbs["afterstop"])
    ctx.data["s5_projectA_outbox"] = outbox_events(dbs["afterstop"], "afterstop")
    ctx.data["s5_projectB_rows"] = _count(dbs["projectB"])
    ctx.data["s5_projectB_sessions"] = dict(_sessions(dbs["projectB"]))

    # ===== S4 (suite) : QGIS redemarre sur le projet A ==================
    # Ce que fait le plugin au demarrage (import pending puis rejeu de
    # l'outbox), deux fois : le second passage prouve l'idempotence.
    try:
        first = restart_integrate(dbs["afterstop"])
        ctx.data["s4_rows_after_restart"] = count_rows(
            dbs["afterstop"], "session_id = ?", ("afterstop",))
        restart_integrate(dbs["afterstop"])
        ctx.data["s4_rows_after_second_restart"] = count_rows(
            dbs["afterstop"], "session_id = ?", ("afterstop",))
        ctx.data["s4_total_rows_after_restart"] = count_rows(dbs["afterstop"])
        ctx.data["s4_dup_uids"] = duplicate_uids(dbs["afterstop"])
        ctx.data["s4_outbox_after_restart"] = outbox_events(dbs["afterstop"])
        ctx.data["s4_replay"] = first.get("replay")
        ctx.data["s4_restart_error"] = ""
    except Exception as exc:  # noqa: BLE001 - a crashed restart is a FAIL
        ctx.data["s4_restart_error"] = f"{type(exc).__name__}: {exc}"

    # ===== S8: 8 producteurs concurrents ================================
    # Chaque thread pousse 40 lots de 12 evenements, chaque lot marque par
    # son propre session_id. L'atomicite promise par enqueue() est
    # "tout le lot ou rien" : un session_id present avec 11 lignes serait
    # un lot coupe en deux.
    n_threads, n_batches, batch_size = 8, 40, 12
    wq = WriteQueue()
    wq.start(dbs["flood"])
    accepted_batches = []
    accepted_lock = threading.Lock()

    def _producer(tid: int) -> None:
        for b in range(n_batches):
            tag = "T%d-B%02d" % (tid, b)
            events = [
                _event(i, datasource="ds%d" % tid, session=tag)
                for i in range(batch_size)
            ]
            ok = wq.enqueue(events)
            flog_kv(
                "INFO", "TX_LIFE_PRODUCER", module="tx_life_wq_stop_restart",
                trace=ctx.trace_id, tag=tag, accepted=bool(ok),
                outcome=_outcome_name(ok),
                note="lot pousse depuis un thread producteur",
            )
            with accepted_lock:
                accepted_batches.append((tag, ok))

    threads = [threading.Thread(target=_producer, args=(t,), name="tx_life_prod_%d" % t)
               for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wq.stop()

    accepted_tags = {tag for tag, ok in accepted_batches if ok}
    ctx.data["s8_accepted_batches"] = len(accepted_tags)
    ctx.data["s8_expected_batches"] = n_threads * n_batches
    ctx.data["s8_batch_size"] = batch_size
    ctx.data["s8_outcomes"] = dict(Counter(_outcome_name(ok) for _t, ok in accepted_batches))
    ctx.data["s8_written"] = _count(dbs["flood"])
    sessions = _sessions(dbs["flood"])
    ctx.data["s8_sessions_written"] = len(sessions)
    ctx.data["s8_partial_batches"] = sorted(
        tag for tag, n in sessions.items() if n != batch_size
    )
    ctx.data["s8_missing_batches"] = sorted(accepted_tags - set(sessions))
    ctx.data["s8_ghost_batches"] = sorted(set(sessions) - accepted_tags)

    # ===== S6: fuite de thread ==========================================
    time.sleep(0.3)
    ctx.data["s6_writer_threads"] = _writer_threads()

    flog(
        f"tx_life_wq_stop_restart run end: trace_id={ctx.trace_id} "
        f"s1={ctx.data['s1_written']} s4_written={ctx.data['s4_written']} "
        f"s4_outbox_at_ack={ctx.data['s4_outbox_after_enqueue']} "
        f"s4_ack_snapshot_rows={(ctx.data.get('s4_ack_snapshot') or {}).get('rows')} "
        f"s4_after_restart={ctx.data.get('s4_rows_after_restart')} "
        f"s5_B={ctx.data['s5_projectB_rows']} s8={ctx.data['s8_written']}",
        "INFO",
    )

    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    out = []
    d = ctx.data

    # ===== S1 : garantie acquise ========================================
    # Hypothesis revised (v7): enqueue() ne rend plus un booleen mais une
    # EnqueueOutcome, donc `is True` echouait sur un succes reel. L'issue
    # attendue est QUEUED_DURABLE (ecrivain vivant, plus strict que "vrai",
    # qui accepterait aussi un repli degrade). Le controle "ecrits == 3000"
    # est inchange ; s'y ajoutent aucun doublon (event_uid) et une outbox
    # vide : les 3000 sont bien indexes au journal a l'arret, pas laisses
    # a un rejeu ulterieur.
    out.append((
        "s1_shutdown_flush_writes_everything",
        d.get("s1_accepted") == "QUEUED_DURABLE" and d.get("s1_written") == 3000
        and d.get("s1_dup_uids") == 0 and d.get("s1_outbox_left") == 0,
        f"accepted={d.get('s1_accepted')} ecrits={d.get('s1_written')} attendu=3000 "
        f"doublons={d.get('s1_dup_uids')} outbox_restante={d.get('s1_outbox_left')} "
        f"pending={d.get('s1_pending')} : QGIS ferme juste apres un gros commit, "
        f"les 3000 modifications doivent etre dans le journal, pas en RAM",
    ))

    # ===== S2 / S3 : garanties acquises =================================
    out.append((
        "s2_double_start_single_writer",
        d.get("s2_writer_threads") == 1 and d.get("s2_written") == 200,
        f"threads_ecrivains={d.get('s2_writer_threads')} attendu=1 "
        f"lignes={d.get('s2_written')} attendu=200 : deux start() consecutifs ne "
        f"doivent pas dupliquer les evenements ni laisser deux ecrivains sur le "
        f"meme fichier",
    ))
    out.append((
        "s3_stop_without_start_is_harmless",
        d.get("s3_error") == "",
        f"stop() sans start() a leve {d.get('s3_error')!r} : la fermeture de QGIS "
        f"apres un echec d'ouverture du journal remonterait une erreur a "
        f"l'utilisateur",
    ))

    # ===== S4 : le trou ================================================
    # Hypothesis revised (v7): ancien nom s4_enqueue_after_stop_is_refused,
    # ancien controle `s4_accepted is False`. Il supposait qu'apres stop()
    # personne n'ecrirait jamais le lot, donc que tout accuse de reception
    # mentait. En v7 le lot est fsync dans l'outbox du journal A avant tout
    # retour, et rejoue au prochain demarrage : un refus serait faux. Ce qui
    # protege l'utilisateur est que l'issue DISE qu'aucun ecrivain ne tourne :
    # PERSISTED_FALLBACK exactement (pas QUEUED_DURABLE), marquee degradee,
    # et une ligne ERROR event=wq_enqueue_no_writer pour ces 25 evenements.
    # La preuve que ce n'est pas un faux succes est l'assertion
    # s4_after_stop_batch_integrated_once_after_restart ci-dessous.
    s4_no_writer = _log_hits(
        ctx.records, r"event=wq_enqueue_no_writer n_events=25 .*afterstop", "ERROR")
    out.append((
        "s4_enqueue_after_stop_is_explicit_fallback",
        d.get("s4_accepted") == "PERSISTED_FALLBACK" and d.get("s4_degraded") is True
        and len(s4_no_writer) == 1,
        f"enqueue() apres stop() a renvoye {d.get('s4_accepted')} "
        f"(degrade={d.get('s4_degraded')}, lignes ERROR wq_enqueue_no_writer="
        f"{len(s4_no_writer)}) ; attendu PERSISTED_FALLBACK et une ligne ERROR. "
        f"L'appelant (EditSessionTracker) ne doit pas recevoir un accuse "
        f"'ecrivain vivant' alors qu'il n'y a plus de thread ecrivain.",
    ))
    # Hypothesis revised (v7): ancien controle `wq.pending_count == 0` apres
    # stop(). Avant v7 la file RAM survivait a stop() et ce compteur la
    # mesurait ; en v7 pending_count lit la generation COURANTE, qui n'existe
    # plus apres stop() : il vaut 0 par construction, meme si un evenement
    # accepte ne vit qu'en memoire (mutant "repli en RAM" de la revue : 0
    # ici, et 19/19). Le nom est garde, l'intention est identique : aucun
    # evenement accepte ne doit vivre seulement en RAM. La mesure devient
    # physique : une copie DISQUE du dossier du journal A, prise des le
    # retour d'enqueue(), est integree par restart_integrate dans un
    # PROCESSUS ENFANT (aucune memoire heritee) ; les 25 evenements doivent
    # y etre, une seule fois, et l'outbox de la copie doit etre videe.
    snap = d.get("s4_ack_snapshot") or {}
    out.append((
        "s4_no_event_left_in_ram",
        snap.get("rc") == 0
        and snap.get("rows") == 25
        and snap.get("total") == 25
        and snap.get("dups") == 0
        and snap.get("outbox_left") == 0,
        f"copie disque du journal A prise a l'accuse de reception, integree dans "
        f"un processus neuf : lignes={snap.get('rows')} total={snap.get('total')} "
        f"attendu=25, doublons={snap.get('dups')}, outbox restante="
        f"{snap.get('outbox_left')} (avant integration: {snap.get('outbox_before')}), "
        f"rc={snap.get('rc')} rejeu={snap.get('replay')} "
        f"erreur={snap.get('error')!r} stderr={snap.get('stderr_tail')!r}. "
        f"Ce qui manque ici n'existait qu'en RAM au moment ou enqueue() a "
        f"accuse reception : un plantage de QGIS a cet instant l'efface, et les "
        f"editions faites pendant la fermeture du projet disparaissent sans "
        f"aucun message.",
    ))
    # Hypothesis revised (v7): ancien nom s4_rejected_batch_lands_in_pending,
    # ancien controle `pending == 25` dans recoverland_pending.json. Le chemin
    # d'ecriture ne met plus rien dans ce fichier : le lot attend dans
    # l'outbox de SON journal. L'ancien controle mesurait un emplacement de
    # stockage ; celui-ci mesure ce qu'il etait cense garantir, "repris au
    # prochain demarrage" : apres restart_integrate (import pending + rejeu
    # outbox, comme le plugin), les 25 evenements sont au journal A, rien
    # d'autre n'y est, aucun event_uid en double, l'outbox est videe, et un
    # second redemarrage ne change rien. Plus strict : un fichier pending
    # bien rempli mais jamais reimporte passait l'ancien controle.
    # Hypothesis revised (v7), 2e passe : l'ancien controle mesurait le
    # stockage A L'ACCUSE (pending == 25 juste apres enqueue, avant S5) ;
    # la 1re adaptation ne regardait plus qu'apres un restart_integrate dans
    # le meme processus, que la RAM traverse. L'equivalent exact de
    # l'ancienne mesure est remis en tete : l'outbox du journal A contient
    # les 25 evenements des le retour d'enqueue() (mesure faite avant toute
    # autre etape, meme avant la pause de 0,3 s : un lot ecrit "plus tard"
    # par un fil de fond ne passe pas). La preuve de redemarrage sur la copie
    # disque est dans s4_no_event_left_in_ram.
    out.append((
        "s4_after_stop_batch_integrated_once_after_restart",
        d.get("s4_outbox_after_enqueue") == 25
        and d.get("s4_restart_error") == ""
        and d.get("s4_rows_after_restart") == 25
        and d.get("s4_rows_after_second_restart") == 25
        and d.get("s4_total_rows_after_restart") == 25
        and d.get("s4_dup_uids") == 0
        and d.get("s4_outbox_after_restart") == 0,
        f"outbox du journal A a l'accuse de reception="
        f"{d.get('s4_outbox_after_enqueue')} attendu=25 (pending="
        f"{d.get('s4_pending')}) ; "
        f"apres redemarrage du projet A : lignes={d.get('s4_rows_after_restart')} "
        f"apres un 2e redemarrage={d.get('s4_rows_after_second_restart')} "
        f"total={d.get('s4_total_rows_after_restart')} attendu=25, "
        f"doublons={d.get('s4_dup_uids')}, outbox restante="
        f"{d.get('s4_outbox_after_restart')}, "
        f"rejeu={d.get('s4_replay')} erreur={d.get('s4_restart_error')!r} : "
        f"les editions faites pendant la fermeture du projet doivent revenir "
        f"au prochain demarrage, une seule fois",
    ))

    # ===== S5 : contamination inter-projets =============================
    out.append((
        "s5_project_b_journal_stays_clean",
        d.get("s5_projectB_rows") == 0,
        f"le journal du projet B contient {d.get('s5_projectB_rows')} ligne(s) "
        f"venues du projet A (sessions={d.get('s5_projectB_sessions')}) alors que "
        f"le projet A n'en a que {d.get('s5_projectA_rows')} "
        f"(+{d.get('s5_projectA_outbox')} en attente dans son outbox). Changer de projet "
        f"deverse l'historique du projet precedent dans le journal du nouveau, "
        f"avec les empreintes de l'ancien : un REWIND sur B ciblera des entites "
        f"qui n'ont jamais existe dans B.",
    ))

    # ===== S7 : file jamais demarree ====================================
    # Hypothesis revised (v7): ancien controle `s7_valid_accepted is False`,
    # qui visait l'ancien booleen (une EnqueueOutcome n'est jamais `False`).
    # Le nom est garde, l'intention est identique : sans journal ni outbox
    # il n'existe aucun chemin durable, donc refus. Plus strict qu'avant :
    # l'issue doit etre REFUSED_NO_DURABLE_PATH (fausse), la perte est
    # comptee (lost_events == 1) et criee par UNE ligne CRITICAL
    # event=wq_events_lost pour cet evenement.
    # Hypothesis revised (v7), 2e passe : le conjoint `pending_count == 0`
    # ajoute a la 1re passe est retire, il etait vide de sens (sans
    # generation, pending_count vaut 0 par construction, meme si
    # l'evenement etait garde en memoire). "Rien n'est garde en RAM" est
    # maintenant prouve sur disque : le MEME objet est ensuite demarre sur un
    # journal neuf, et l'evenement refuse n'y apparait ni en ligne ni dans
    # l'outbox (il n'y ressurgit pas plus tard, dans le journal d'un autre
    # projet, comme la file d'avant v7 le faisait).
    s7_lost_lines = _log_hits(
        ctx.records, r"event=wq_events_lost n_events=1 .*journal=None", "CRITICAL")
    h7v = d.get("s7_health_after_valid") or {}
    out.append((
        "s7_enqueue_without_start_is_refused",
        d.get("s7_valid_accepted") == "REFUSED_NO_DURABLE_PATH"
        and d.get("s7_valid_truthy") is False
        and h7v.get("lost_events") == 1
        and len(s7_lost_lines) == 1
        and d.get("s7_resurfaced_rows") == 0
        and d.get("s7_resurfaced_outbox") == 0,
        f"enqueue() sur une file jamais demarree a renvoye "
        f"{d.get('s7_valid_accepted')} (vrai={d.get('s7_valid_truthy')}) "
        f"(db_path={d.get('s7_db_path')!r}), lost_events={h7v.get('lost_events')} "
        f"attendu=1, lignes CRITICAL wq_events_lost={len(s7_lost_lines)} attendu=1 ; "
        f"le meme objet demarre ensuite sur un journal neuf y fait reapparaitre "
        f"{d.get('s7_resurfaced_rows')} ligne(s) et {d.get('s7_resurfaced_outbox')} "
        f"evenement(s) d'outbox refuses (attendu 0 et 0). C'est le cas ou "
        f"l'ouverture du journal a echoue : le plugin ne doit ni accepter ces "
        f"evenements, ni les perdre en silence, ni les garder en memoire pour "
        f"les verser plus tard dans le journal d'un autre projet.",
    ))
    # Hypothesis revised (v7): ancien controle `s7_invalid_accepted is False`
    # (ancien booleen). Le nom est garde : il s'agit toujours de prouver que
    # le rejet d'un lot invalide sans journal n'est pas silencieux. L'issue
    # doit etre REFUSED_INVALID (fausse) ET une ligne ERROR
    # event=wq_enqueue_rejected doit nommer le defaut (BOGUS) : c'est
    # precisement le silence que l'ancien message reprochait a
    # _save_lost_events.
    # Hypothesis revised (v7), 2e passe : sans journal, aucune quarantaine ne
    # peut etre ecrite ; le lot n'existe donc nulle part et doit etre compte
    # PERDU, pas preserve. La 1re passe laissait passer un health() qui le
    # disait mis en quarantaine (quarantined_events=1, lost_events inchange,
    # aucun fichier) : c'est le mensonge que l'ancien message reprochait.
    # Exige en plus : lost_events augmente d'exactement 1 entre les deux
    # enqueue(), quarantined_events n'augmente pas (et reste 0), et UNE ligne
    # CRITICAL event=wq_events_lost pour ce lot invalide.
    s7_rejected_lines = _log_hits(
        ctx.records, r"event=wq_enqueue_rejected n_events=1 n_invalid=1 .*BOGUS",
        "ERROR")
    s7_invalid_lost_lines = _log_hits(
        ctx.records,
        r"event=wq_events_lost n_events=1 reason=invalid_batch_without_journal",
        "CRITICAL")
    h7i = d.get("s7_health_after_invalid") or {}
    lost_delta = quar_delta = None
    if isinstance(h7i.get("lost_events"), int) and isinstance(h7v.get("lost_events"), int):
        lost_delta = h7i["lost_events"] - h7v["lost_events"]
    if (isinstance(h7i.get("quarantined_events"), int)
            and isinstance(h7v.get("quarantined_events"), int)):
        quar_delta = h7i["quarantined_events"] - h7v["quarantined_events"]
    out.append((
        "s7_invalid_batch_reports_the_drop",
        d.get("s7_invalid_accepted") == "REFUSED_INVALID"
        and d.get("s7_invalid_truthy") is False
        and len(s7_rejected_lines) == 1
        and lost_delta == 1
        and quar_delta == 0
        and h7i.get("quarantined_events") == 0
        and len(s7_invalid_lost_lines) == 1,
        f"enqueue() d'un lot invalide sans db_path a renvoye "
        f"{d.get('s7_invalid_accepted')} (vrai={d.get('s7_invalid_truthy')}), "
        f"lignes ERROR wq_enqueue_rejected={len(s7_rejected_lines)} attendu=1, "
        f"lost_events +{lost_delta} attendu +1 (={h7i.get('lost_events')}), "
        f"quarantined_events +{quar_delta} attendu +0 "
        f"(={h7i.get('quarantined_events')}), lignes CRITICAL wq_events_lost "
        f"reason=invalid_batch_without_journal={len(s7_invalid_lost_lines)} "
        f"attendu=1 : sans journal le lot n'est nulle part ; health() ne doit "
        f"pas le dire preserve, il doit le compter perdu et le crier",
    ))

    # ===== S8 : concurrence =============================================
    out.append((
        "s8_all_accepted_batches_persisted",
        not d.get("s8_missing_batches"),
        f"lots acceptes mais absents du journal : {d.get('s8_missing_batches')} "
        f"(acceptes={d.get('s8_accepted_batches')}/"
        f"{d.get('s8_expected_batches')}, lignes={d.get('s8_written')}). Huit "
        f"utilisateurs de couches differentes editant en meme temps perdraient "
        f"ces lots.",
    ))
    out.append((
        "s8_no_partial_batch",
        not d.get("s8_partial_batches"),
        f"lots ecrits a moitie : {d.get('s8_partial_batches')} (chaque lot doit "
        f"faire exactement {d.get('s8_batch_size')} lignes). Un lot coupe laisse "
        f"une entite avec une partie seulement de ses modifications, donc un "
        f"REWIND partiel silencieux.",
    ))
    out.append((
        "s8_no_ghost_batch",
        not d.get("s8_ghost_batches"),
        f"lots presents dans le journal alors que enqueue() les a refuses : "
        f"{d.get('s8_ghost_batches')}",
    ))
    out.append((
        "s8_exact_row_count",
        d.get("s8_written") == d.get("s8_expected_batches", 0) * d.get("s8_batch_size", 0),
        f"lignes={d.get('s8_written')} attendu="
        f"{d.get('s8_expected_batches', 0) * d.get('s8_batch_size', 0)} : ni perte "
        f"ni doublon sous 8 producteurs concurrents",
    ))

    # ===== S6 : fuite de thread =========================================
    out.append((
        "s6_no_writer_thread_leak",
        d.get("s6_writer_threads") == d.get("writers_before"),
        f"threads '{_WRITER_THREAD_NAME}' vivants={d.get('s6_writer_threads')} "
        f"avant le scenario={d.get('writers_before')} : un ecrivain orphelin "
        f"garde une connexion ouverte sur un journal que l'utilisateur croit "
        f"ferme",
    ))

    # ===== Journalisation concurrente encore lisible ====================
    # 320 lignes TX_LIFE_PRODUCER emises depuis 8 threads en parallele : le
    # handler doit les ecrire entieres, et parse_log doit retrouver chaque
    # champ. Une ligne entrelacee casserait tout l'outillage de validation.
    producer_records = [
        r for r in ctx.records
        if "TX_LIFE_PRODUCER" in r.raw and r.fields.get("trace") == ctx.trace_id
    ]
    expected_lines = d.get("s8_expected_batches", 0)
    out.append((
        "log_concurrent_lines_all_present",
        len(producer_records) == expected_lines,
        f"lignes TX_LIFE_PRODUCER retrouvees={len(producer_records)} "
        f"attendu={expected_lines} : des lignes ecrites depuis 8 threads en "
        f"parallele ont ete perdues ou tronquees dans recoverland_debug.log",
    ))
    # Hypothesis revised (v7): le producteur journalisait `accepted=ok` avec
    # ok = valeur de retour d'enqueue(). En v7 c'est une EnqueueOutcome, dont
    # str() vaut "EnqueueOutcome.QUEUED_DURABLE" : le champ `accepted` sortait
    # donc de ("True", "False") sur les 320 lignes, alors que chaque ligne
    # etait entiere et parsable (verifie : 320 lignes de meme longueur, un
    # seul fichier, aucune rotation ; event, module et tag corrects partout).
    # Le "event=" final des exemples venait de l'extrait `raw[:100]`, qui
    # coupe pile a la colonne 100. Le producteur journalise maintenant
    # `accepted=bool(ok)` et `outcome=<nom>`. Le controle garde ses quatre
    # champs a l'identique et exige en plus un `outcome` qui soit un nom
    # d'EnqueueOutcome coherent avec `accepted` : plus strict. Le diagnostic
    # montre les champs fautifs au lieu d'une coupe a 100 caracteres.
    from recoverland.core.write_queue import EnqueueOutcome
    outcome_truth = {o.name: str(bool(o)) for o in EnqueueOutcome}
    malformed = [
        {k: r.fields.get(k) for k in ("event", "module", "tag", "accepted", "outcome")}
        for r in producer_records
        if r.fields.get("event") != "TX_LIFE_PRODUCER"
        or r.fields.get("module") != "tx_life_wq_stop_restart"
        or not (r.fields.get("tag") or "").startswith("T")
        or r.fields.get("accepted") not in ("True", "False")
        or outcome_truth.get(r.fields.get("outcome")) != r.fields.get("accepted")
    ]
    out.append((
        "log_concurrent_lines_still_parsable",
        not malformed,
        f"{len(malformed)} ligne(s) cle=valeur illisibles apres ecriture "
        f"concurrente, ex: {malformed[:2]} : parse_log.py ne peut plus "
        f"reconstituer ce qui s'est passe, donc plus aucun diagnostic possible "
        f"sur un incident de production (issues S8={d.get('s8_outcomes')})",
    ))

    out.append(assert_log_contains(
        ctx.records,
        rf"tx_life_wq_stop_restart.*trace_id={ctx.trace_id}",
        name="trace_id_propagated",
        min_count=2,
    ))

    # ===== Garde de source =============================================
    # La cause racine etait structurelle : enqueue() ne consultait jamais
    # l'etat du consommateur, et stop() ne vidait pas la file.
    src = (_PLUGIN_ROOT / "core" / "write_queue.py").read_text(
        encoding="utf-8", errors="replace")
    # Hypothesis revised (v7): l'ancienne garde cherchait `_running` ou
    # `is_alive` dans une tranche de texte allant de `def enqueue(` a
    # `def set_early_warning_callback(`. En v7 enqueue() lit `gen.alive` et
    # `gen.accepting`, et la tranche couvrait aussi d'autres methodes. La
    # garde prend la methode exacte (AST) et epingle le mecanisme qui
    # protege le meme invariant : le lot est ecrit dans l'outbox AVANT la
    # file RAM ; la mise en file RAM depend d'un ecrivain vivant qui accepte ;
    # sinon l'issue est PERSISTED_FALLBACK. Plus strict : l'ancienne garde
    # passait avec un simple mot-cle n'importe ou dans la tranche.
    # Hypothesis revised (v7), 2e passe : la garde de la 1re passe restait
    # textuelle (positions de sous-chaines) et ne regardait que la file
    # `gen.queue` : un enqueue() qui rendait PERSISTED_FALLBACK AVANT
    # d'ecrire l'outbox, en gardant le lot dans une autre structure en
    # memoire, la passait (mutant de la revue). L'intention est identique
    # (aucun accuse de reception pour un evenement qui ne vit qu'en RAM) ;
    # la garde devient un controle AST de WriteQueue.enqueue :
    # - aucune issue durable (QUEUED_DURABLE, PERSISTED_FALLBACK,
    #   PERSISTED_JOURNAL_SYNC) n'est nommee avant le premier appel
    #   append_events, sauf le `return EnqueueOutcome.QUEUED_DURABLE` seul
    #   sous `if not events and commit is None:` (lot vide) ;
    # - avant ce premier append_events, tout `return` rend un REFUSED_*
    #   (meme exception) : pas d'issue durable passee par une variable ;
    # - chaque `.queue.put(` vient apres l'append, sous un `if` qui lit
    #   `.alive` et `.accepting` (directement ou via la variable testee) ;
    # - un `return EnqueueOutcome.PERSISTED_FALLBACK` existe apres l'append.
    # Les commentaires et les chaines ne comptent plus.
    enq_checks, enq_details = _enqueue_guard(src)
    out.append((
        "enqueue_checks_the_writer_is_alive",
        bool(enq_checks) and all(enq_checks.values()),
        f"{enq_checks} {enq_details} : enqueue() ne doit rendre aucune issue "
        f"durable avant d'avoir ecrit le lot dans l'outbox (append_events), ne "
        f"le mettre en file RAM qu'ensuite et seulement si l'ecrivain est vivant "
        f"et accepte, et sinon rendre PERSISTED_FALLBACK ; sans cela elle accuse "
        f"reception d'un evenement qui ne vit qu'en memoire, ou aupres d'un "
        f"consommateur inexistant",
    ))
    # Hypothesis revised (v7): l'ancienne garde cherchait `_drain_all` ou
    # `save_pending` dans stop(). En v7 la file RAM ne porte que des copies
    # de lots deja dans l'outbox ; ce qui protege le meme invariant est que
    # stop() vide la RAM de SA generation (_drain_ram) et dit ce qui reste a
    # rejouer (wq_stop_left_in_outbox), que l'ecrivain ne sorte de sa boucle
    # que file vide, et qu'il termine par un dernier flush (_final_flush)
    # dans son finally.
    # Hypothesis revised (v7), 2e passe : la sous-garde
    # "loop:break_only_if_queue_empty" ne prouvait que l'EXISTENCE d'un break
    # conditionne par queue.empty(), pas qu'il etait la seule sortie (la
    # boucle en a une seconde, `if gen.stop_event.is_set(): break` sur le
    # chemin de reessai), et "_final_flush(" dans le texte du finally ne
    # disait pas ce que ce flush recoit. Meme intention, controle AST :
    # chaque sortie de la boucle de l'ecrivain (break de CETTE boucle,
    # return, test du while) est soit la sortie propre (`stop_event.is_set()
    # and not retry and queue.empty()`), soit couverte par le finally qui
    # appelle _final_flush(..., retry) ; _final_flush lit ces lots a
    # reessayer, vide la RAM (_drain_ram) et les ecrit (_write_batches). La
    # sortie propre doit exister. Les cles disent ce qui est verifie.
    stop_checks, stop_details = _stop_guard(src)
    out.append((
        "stop_drains_what_the_writer_left",
        bool(stop_checks) and all(stop_checks.values()),
        f"{stop_checks} {stop_details} : stop() doit vider la file RAM de sa "
        f"generation et dire ce qui reste dans l'outbox ; l'ecrivain ne doit "
        f"sortir de sa boucle que file vide, ou par un dernier flush qui ecrit "
        f"les lots a reessayer et vide la RAM ; sinon un evenement arrive apres "
        f"son dernier drain resterait en RAM",
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
