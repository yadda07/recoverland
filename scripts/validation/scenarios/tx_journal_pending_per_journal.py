"""tx_journal_pending_per_journal - RecoverLand validation runtime.

Invariant: I-JRN (une edition finit dans le journal du projet qui l'a
produite, jamais dans celui d'un autre ; un fichier d'attente n'est jamais
perdu).

Le mecanisme
------------
Le fichier d'attente historique (`recoverland_pending.json`, ecrit par
`integrity.save_pending_events` quand le journal ne pouvait pas etre ecrit,
relu au demarrage par `check_journal_integrity`) etait range dans le
DOSSIER du journal. Pour un projet enregistre, ce dossier
(`<projet>/.recoverland/`) n'a qu'un journal. Mais tous les projets jamais
enregistres d'un profil ont leur journal dans le meme dossier
(`<profil>/recoverland/audit/audit_<cle>.sqlite`) : ils partageaient UN
fichier d'attente. Les evenements en attente d'une session etaient importes
dans le journal du PROCHAIN projet non enregistre ouvert, puis le fichier
etait supprime.

Le scenario d'echec concret
---------------------------
QGIS plante pendant une session sur un projet non enregistre ; ses
modifications partent dans le fichier d'attente. Le lendemain, l'utilisateur
ouvre un NOUVEAU projet vierge : les modifications de la veille sont
importees dans l'historique de ce nouveau projet (qui ne contient pas ces
donnees), et un Rewind du nouveau projet les propose. Le journal de la
session d'hier, lui, ne les aura jamais.

Ce que le scenario execute reellement
-------------------------------------
Vrais `JournalManager`, `save_pending_events`, `check_journal_integrity`,
`cleanup_orphan_journals`, dans un profil temporaire :
  P1 un fichier d'attente PARTAGE (ancien format) dans le dossier du profil :
     le controle d'un autre journal non enregistre ne l'importe pas, le
     garde, et le signale.
  P2 `save_pending_events` pour le journal A : le controle du journal B
     n'importe rien ; celui du journal A importe tout.
  P3 un projet enregistre (son dossier n'a que son journal) : son fichier
     d'attente historique est importe comme avant.
  P4 un journal non enregistre ancien, vide, dont le fichier d'attente
     existe : le nettoyage des orphelins le garde, fichier d'attente compris.

Verdict attendu avant correctif : FAIL (P1, P2, P4).
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

SCENARIO_ID = "tx_journal_pending_per_journal"
INVARIANT = "I-JRN"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]


def _event(marker: str, idx: int) -> dict:
    return {
        "project_fingerprint": "tx_jppj_project", "datasource_fingerprint": "tx_jppj_ds",
        "layer_id_snapshot": "lyr", "layer_name_snapshot": marker,
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": idx}),
        "operation_type": "UPDATE",
        "attributes_json": json.dumps({"changed_only": {"nom": {"old": "A", "new": marker}}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": "EPSG:2154",
        "field_schema_json": None, "user_name": "tester", "session_id": "s_jppj",
        "created_at": f"2026-09-27T10:00:{idx:02d}+00:00",
        "restored_from_event_id": None, "entity_fingerprint": f"fid:{idx}",
        "event_schema_version": 2, "new_geometry_wkb": None, "invalidated_at": None,
        "event_uid": f"u-{marker}-{idx}", "commit_id": None,
    }


def _count(journal: str, marker: str) -> int:
    conn = sqlite3.connect(journal)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE layer_name_snapshot = ?",
            (marker,)).fetchone()[0])
    finally:
        conn.close()


def _new_unsaved_journal(profile: str) -> str:
    from recoverland.core.journal_manager import JournalManager
    jm = JournalManager()
    try:
        return jm.open_for_project("", profile)
    finally:
        jm.close()


def _p1_shared_legacy(tmp: str) -> dict:
    from recoverland.core.integrity import check_journal_integrity

    res = {}
    profile = os.path.join(tmp, "profil_p1")
    journal_b = _new_unsaved_journal(profile)
    shared = os.path.join(os.path.dirname(journal_b), "recoverland_pending.json")
    with open(shared, "w", encoding="utf-8") as fh:
        json.dump([_event("P1_SESSION_OF_YESTERDAY", i) for i in range(3)], fh)
    result = check_journal_integrity(journal_b)
    res["imported_into_b"] = _count(journal_b, "P1_SESSION_OF_YESTERDAY")
    res["shared_kept"] = os.path.isfile(shared)
    res["issues"] = list(result.issues)
    res["shared"] = shared
    return res


def _p2_named_after_journal(tmp: str) -> dict:
    from recoverland.core.integrity import check_journal_integrity, save_pending_events

    res = {}
    profile = os.path.join(tmp, "profil_p2")
    journal_a = _new_unsaved_journal(profile)
    journal_b = _new_unsaved_journal(profile)
    res["distinct_journals"] = journal_a != journal_b
    save_pending_events(journal_a, [_event("P2_OF_A", i) for i in range(2)])
    check_journal_integrity(journal_b)
    res["in_b"] = _count(journal_b, "P2_OF_A")
    result_a = check_journal_integrity(journal_a)
    res["in_a"] = _count(journal_a, "P2_OF_A")
    res["recovered_a"] = result_a.recovered_events
    res["left_in_audit_dir"] = sorted(
        n for n in os.listdir(os.path.dirname(journal_a)) if "pending" in n)
    return res


def _p3_saved_project(tmp: str) -> dict:
    from recoverland.core.journal_manager import JournalManager
    from recoverland.core.integrity import check_journal_integrity

    res = {}
    pdir = os.path.join(tmp, "projet_p3")
    os.makedirs(pdir, exist_ok=True)
    project = os.path.join(pdir, "P.qgz")
    Path(project).write_text("<qgis/>", encoding="utf-8")
    jm = JournalManager()
    try:
        journal = jm.open_for_project(project, os.path.join(tmp, "profil_p3"))
    finally:
        jm.close()
    legacy = os.path.join(os.path.dirname(journal), "recoverland_pending.json")
    with open(legacy, "w", encoding="utf-8") as fh:
        json.dump([_event("P3_SAVED", i) for i in range(2)], fh)
    result = check_journal_integrity(journal)
    res["imported"] = _count(journal, "P3_SAVED")
    res["recovered"] = result.recovered_events
    res["legacy_left"] = os.path.isfile(legacy)
    return res


def _p4_orphan_with_pending(tmp: str) -> dict:
    from recoverland.core.journal_manager import cleanup_orphan_journals
    from recoverland.core.integrity import save_pending_events

    res = {}
    profile = os.path.join(tmp, "profil_p4")
    orphan = _new_unsaved_journal(profile)
    save_pending_events(orphan, [_event("P4_ORPHAN", 1)])
    audit = os.path.dirname(orphan)
    before = sorted(os.listdir(audit))
    old = time.time() - 40 * 86400
    for name in before:
        os.utime(os.path.join(audit, name), (old, old))
    removed = cleanup_orphan_journals(profile, max_age_days=30)
    after = sorted(os.listdir(audit))
    res["removed"] = removed
    res["orphan_kept"] = os.path.isfile(orphan)
    res["pending_files_after"] = [n for n in after if "pending" in n]
    res["pending_files_before"] = [n for n in before if "pending" in n]
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_tx_jppj_")
    flog(f"tx_journal_pending_per_journal setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    d = ctx.data
    for key, fn in (("p1", _p1_shared_legacy), ("p2", _p2_named_after_journal),
                    ("p3", _p3_saved_project), ("p4", _p4_orphan_with_pending)):
        try:
            d[key] = fn(d["tmp"])
        except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
            import traceback
            d[key] = {"crash": f"{type(exc).__name__}: {exc}",
                      "trace": traceback.format_exc()[-1500:]}
    flog(f"tx_journal_pending_per_journal run: trace_id={ctx.trace_id} "
         f"data={json.dumps(d, default=str)[:5000]}", "INFO")
    shutil.rmtree(d["tmp"], ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    out = []
    p1 = d.get("p1") or {}
    named = [i for i in p1.get("issues") or [] if "recoverland_pending.json" in i]
    out.append((
        "p1_fichier_partage_ni_importe_ni_perdu_et_signale",
        p1.get("imported_into_b") == 0 and p1.get("shared_kept") is True
        and len(named) == 1,
        f"evenements de la session d'hier importes dans un AUTRE projet="
        f"{p1.get('imported_into_b')} (attendu 0) ; fichier conserve="
        f"{p1.get('shared_kept')} ; signalement={named or p1.get('issues')} "
        f"crash={p1.get('crash')} {p1.get('trace', '')[-400:]} : l'historique "
        f"d'un projet finissait dans celui du suivant",
    ))
    p2 = d.get("p2") or {}
    out.append((
        "p2_chaque_journal_retrouve_ses_seuls_evenements",
        p2.get("distinct_journals") is True and p2.get("in_b") == 0
        and p2.get("in_a") == 2 and p2.get("recovered_a") == 2
        and not p2.get("left_in_audit_dir"),
        f"evenements de A importes dans B={p2.get('in_b')} (attendu 0) ; dans A="
        f"{p2.get('in_a')} (attendu 2) ; fichiers d'attente restants="
        f"{p2.get('left_in_audit_dir')} crash={p2.get('crash')} "
        f"{p2.get('trace', '')[-400:]}",
    ))
    p3 = d.get("p3") or {}
    out.append((
        "p3_projet_enregistre_inchange",
        p3.get("imported") == 2 and p3.get("recovered") == 2
        and p3.get("legacy_left") is False,
        f"projet enregistre : importes={p3.get('imported')} recuperes="
        f"{p3.get('recovered')} fichier restant={p3.get('legacy_left')} crash="
        f"{p3.get('crash')} {p3.get('trace', '')[-400:]}",
    ))
    p4 = d.get("p4") or {}
    out.append((
        "p4_orphelin_avec_fichier_d_attente_garde",
        p4.get("orphan_kept") is True and p4.get("removed") == 0
        and bool(p4.get("pending_files_after"))
        and p4.get("pending_files_after") == p4.get("pending_files_before"),
        f"journal orphelin garde={p4.get('orphan_kept')} supprimes="
        f"{p4.get('removed')} fichiers d'attente avant={p4.get('pending_files_before')} "
        f"apres={p4.get('pending_files_after')} crash={p4.get('crash')} "
        f"{p4.get('trace', '')[-400:]} : un journal vide dont les evenements "
        f"attendent ailleurs n'est pas un orphelin a supprimer",
    ))
    out.append(assert_log_contains(
        ctx.records, rf"tx_journal_pending_per_journal.*trace_id={ctx.trace_id}",
        name="trace_id_propage", min_count=2))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
