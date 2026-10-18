"""tx_journal_long_path_companions - RecoverLand validation runtime.

Invariant: I-JRN-01 (un projet a un journal, le meme a chaque ouverture) et
zero perte silencieuse autour de ce journal.

Le mecanisme
------------
tx_journal_long_path prouve qu'un journal au-dela de MAX_PATH (260
caracteres) s'ouvre et recoit la capture, sous sa forme etendue (deux
barres obliques inverses, un point d'interrogation, une barre oblique
inverse, puis le chemin absolu). Autour du journal vivent d'autres fichiers
et d'autres controles, qui recoivent son chemin sous l'une ou l'autre
ecriture : le fichier d'attente et le controle d'integrite du demarrage, le
verrou d'ecrivain, la taille affichee dans la maintenance, la surveillance
du disque, le nettoyage des journaux orphelins et le registre des
rattachements d'historique d'un profil profond. Un seul d'entre eux qui
prend le chemin long pour un fichier absent suffit a perdre ou a masquer
quelque chose : "Journal file not found" au demarrage (le fichier d'attente
n'est jamais relu), un disque annonce inaccessible (capture coupee), un
verrou que personne ne voit (deux ecrivains), un orphelin jamais nettoye ou
un rattachement jamais enregistre.

Le scenario d'echec concret
---------------------------
Un projet range dans une arborescence profonde d'un serveur de fichiers :
au redemarrage apres un plantage, les modifications du fichier d'attente
ne sont pas reintegrees ; la barre d'etat dit le disque inaccessible ; un
projet jamais enregistre dans un profil profond n'a aucun journal.

Ce que le scenario execute reellement
-------------------------------------
Un projet enregistre sous ~280 caracteres, un profil sous ~240 :
  C1 `save_pending_events` puis `check_journal_integrity` sur le journal
     profond (forme etendue ET forme simple) : l'evenement est reintegre.
  C2 `get_journal_size_bytes` (forme simple) > 0.
  C3 `writer_lock_holder` voit le verrou de cette instance sous les deux
     ecritures.
  C4 `check_disk_for_path` mesure le volume, nomme sans prefixe etendu.
  C5 profil profond : un journal orphelin ancien et vide est supprime, un
     orphelin qui contient un evenement est garde.
  C6 profil profond : un projet vierge capture, "Enregistrer sous" vers le
     projet profond (plan durable), fermeture : l'evenement est dans le
     journal du projet profond.

Verdict attendu avant correctif : FAIL (C5, C6 : aucun journal ne s'ouvre
dans un profil profond ; C1 a C4 sans objet sur le journal range a tort
dans le profil).
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

SCENARIO_ID = "tx_journal_long_path_companions"
INVARIANT = "I-JRN-01"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
_PFX = "\\\\?\\"


def _ext(path: str) -> str:
    p = os.path.abspath(path)
    return p if p.startswith(_PFX) else _PFX + p


def _plain(path: str) -> str:
    return path[len(_PFX):] if path and path.startswith(_PFX) else path


def _deep(base: str, total: int) -> str:
    deep = base
    while len(deep) < total:
        deep = os.path.join(deep, "dossier_de_projet_au_nom_tres_long")
    os.makedirs(_ext(deep), exist_ok=True)
    return deep


def _event(marker: str, idx: int) -> dict:
    return {
        "project_fingerprint": "tx_jlpc", "datasource_fingerprint": "tx_jlpc_ds",
        "layer_id_snapshot": "lyr", "layer_name_snapshot": marker,
        "provider_type": "ogr", "feature_identity_json": json.dumps({"fid": idx}),
        "operation_type": "UPDATE",
        "attributes_json": json.dumps({"changed_only": {"nom": {"old": "A", "new": marker}}}),
        "geometry_wkb": None, "geometry_type": "NoGeometry", "crs_authid": "EPSG:2154",
        "field_schema_json": None, "user_name": "tester", "session_id": "s_jlpc",
        "created_at": f"2026-09-27T10:00:{idx:02d}+00:00",
        "restored_from_event_id": None, "entity_fingerprint": f"fid:{idx}",
        "event_schema_version": 2, "new_geometry_wkb": None, "invalidated_at": None,
        "event_uid": f"u-{marker}-{idx}", "commit_id": None,
    }


def _insert(conn, marker: str, idx: int) -> None:
    ev = _event(marker, idx)
    cols = list(ev)
    conn.execute(f"INSERT INTO audit_event ({', '.join(cols)}) "  # nosec B608
                 f"VALUES ({','.join(['?'] * len(cols))})", [ev[c] for c in cols])
    conn.commit()


def _count(journal: str, marker: str) -> int:
    try:
        conn = sqlite3.connect(_ext(journal))
    except sqlite3.Error:
        return -1
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE layer_name_snapshot = ?",
            (marker,)).fetchone()[0])
    except sqlite3.Error:
        return -1
    finally:
        conn.close()


def _saved_part(tmp: str) -> dict:
    from recoverland.core.journal_manager import (
        JournalManager, get_journal_size_bytes, writer_lock_holder,
    )
    from recoverland.core.integrity import check_journal_integrity, save_pending_events
    from recoverland.core.disk_monitor import check_disk_for_path

    res = {}
    proj_dir = _deep(os.path.join(tmp, "projets"), 250)
    project = os.path.join(proj_dir, "mon_projet.qgz")
    with open(_ext(project), "w") as fh:
        fh.write("x")
    res["project_len"] = len(project)
    profile = os.path.join(tmp, "profil")
    jm = JournalManager()
    try:
        journal = jm.open_for_project(project, profile)
        res["journal"] = journal
        res["next_to_project"] = os.path.normcase(_plain(journal)).startswith(
            os.path.normcase(proj_dir))
        holder = writer_lock_holder(journal) or {}
        holder_plain = writer_lock_holder(_plain(journal)) or {}
        res["c3_holder"] = holder.get("pid") == os.getpid()
        res["c3_holder_plain"] = holder_plain.get("pid") == os.getpid()
        res["c2_size_plain"] = get_journal_size_bytes(_plain(journal))
        status = check_disk_for_path(journal)
        res["c4_total"] = status.total_bytes
        res["c4_volume"] = status.volume_path
    finally:
        jm.close()
    save_pending_events(journal, [_event("C1_EXT", 1)])
    r_ext = check_journal_integrity(journal)
    save_pending_events(_plain(journal), [_event("C1_PLAIN", 2)])
    r_plain = check_journal_integrity(_plain(journal))
    res["c1_recovered"] = [r_ext.recovered_events, r_plain.recovered_events]
    res["c1_issues"] = list(r_ext.issues) + list(r_plain.issues)
    res["c1_rows"] = [_count(journal, "C1_EXT"), _count(journal, "C1_PLAIN")]
    res["project"] = project
    return res


def _deep_profile_part(tmp: str, project: str) -> dict:
    from recoverland.core.journal_manager import JournalManager, cleanup_orphan_journals

    res = {}
    profile = _deep(os.path.join(tmp, "profil_profond"), 240)
    # C5: two old orphans, one empty, one holding an event.
    paths = []
    for marker in ("", "C5_KEPT"):
        jm = JournalManager()
        try:
            path = jm.open_for_project("", profile)
            if marker:
                _insert(jm.get_connection(), marker, 5)
        finally:
            jm.close()
        paths.append(path)
    old = time.time() - 40 * 86400
    for path in paths:
        os.utime(_ext(path), (old, old))
    res["c5_removed"] = cleanup_orphan_journals(profile, max_age_days=30)
    res["c5_empty_gone"] = not os.path.isfile(_ext(paths[0]))
    res["c5_holding_kept"] = os.path.isfile(_ext(paths[1]))
    # C6: an unsaved session in the deep profile, "Save As" the deep project.
    jm = JournalManager()
    try:
        jm.open_for_project("", profile)
        _insert(jm.get_connection(), "C6_SESSION", 6)
        carry = jm.plan_history_carry(project, profile, being_written=True)
        res["c6_planned"] = carry is not None
        owed = os.path.join(profile, "recoverland", "audit", "carries_owed.json")
        res["c6_durable"] = os.path.isfile(_ext(owed))
        with open(_ext(project), "w") as fh:     # the save writes the project
            fh.write("saved")
        target = jm.open_for_project(project, profile)
        rep = jm.last_carry_report
        res["c6_report"] = rep.as_dict() if rep else None
    finally:
        jm.close()
    res["c6_rows"] = _count(target, "C6_SESSION")
    return res


def setup(ctx):
    from recoverland.core.logger import flog

    ctx.data["tmp"] = tempfile.mkdtemp(prefix="rl_lpc_")
    flog(f"tx_journal_long_path_companions setup: trace_id={ctx.trace_id}", "INFO")


def run(ctx):
    from recoverland.core.logger import flog

    d = ctx.data
    try:
        d["saved"] = _saved_part(d["tmp"])
        d["deep"] = _deep_profile_part(d["tmp"], d["saved"]["project"])
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    flog(f"tx_journal_long_path_companions run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")
    shutil.rmtree(_ext(d["tmp"]), ignore_errors=True)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains

    d = ctx.data
    s = d.get("saved") or {}
    p = d.get("deep") or {}
    crash = f"{d.get('crash')} {d.get('trace', '')[-600:]}"
    out = [(
        "fixture_projet_profond_a_cote_du_projet",
        s.get("next_to_project") is True and (s.get("project_len") or 0) > 260,
        f"chemin du projet={s.get('project_len')} car. journal={s.get('journal')} "
        f"a cote du projet={s.get('next_to_project')} crash={crash}",
    ), (
        "c1_fichier_d_attente_reintegre_sous_les_deux_ecritures",
        s.get("c1_recovered") == [1, 1] and s.get("c1_rows") == [1, 1]
        and not s.get("c1_issues"),
        f"recuperes (forme etendue, forme simple)={s.get('c1_recovered')} lignes="
        f"{s.get('c1_rows')} problemes={s.get('c1_issues')} : un journal pris pour "
        f"absent ne relit jamais les modifications en attente",
    ), (
        "c2_taille_affichee",
        (s.get("c2_size_plain") or 0) > 0,
        f"taille du journal (forme simple)={s.get('c2_size_plain')} octets",
    ), (
        "c3_verrou_vu_sous_les_deux_ecritures",
        s.get("c3_holder") is True and s.get("c3_holder_plain") is True,
        f"verrou de cette instance vu : forme etendue={s.get('c3_holder')} forme "
        f"simple={s.get('c3_holder_plain')} : un verrou invisible laisse ecrire "
        f"une seconde instance",
    ), (
        "c4_disque_mesure_et_nomme_simplement",
        (s.get("c4_total") or 0) > 0
        and not str(s.get("c4_volume") or "").startswith(_PFX),
        f"volume={s.get('c4_volume')!r} taille totale={s.get('c4_total')} : un "
        f"volume non mesure coupe la capture",
    ), (
        "c5_orphelins_du_profil_profond",
        p.get("c5_empty_gone") is True and p.get("c5_holding_kept") is True
        and p.get("c5_removed") == 1,
        f"orphelin vide supprime={p.get('c5_empty_gone')} orphelin avec "
        f"historique garde={p.get('c5_holding_kept')} supprimes={p.get('c5_removed')} "
        f"crash={crash}",
    ), (
        "c6_rattachement_depuis_un_profil_profond",
        p.get("c6_planned") is True and p.get("c6_durable") is True
        and p.get("c6_rows") == 1,
        f"plan={p.get('c6_planned')} enregistre durablement={p.get('c6_durable')} "
        f"evenement dans le journal du projet profond={p.get('c6_rows')} rapport="
        f"{(p.get('c6_report') or {}).get('error')!r} crash={crash}",
    )]
    out.append(assert_log_contains(
        ctx.records, rf"tx_journal_long_path_companions.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
