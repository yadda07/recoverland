"""rw_barrier_shared_pending - la barriere du journal lit aussi le fichier de secours partage.

Invariant : I-PLAN-COMPLETE (on ne planifie pas une restauration, un rewind
ou une annulation sur un journal auquel il manque des editions connues).

Le mecanisme
------------
Avant de planifier, le dialogue exige que le journal tienne toutes les
editions deja faites (`_journal_barrier`) : la file d'ecriture videe, la
boite d'envoi indexee, et aucun evenement du perimetre en attente dans un
fichier de secours (ecrit par un processus qui meurt). Il ne lisait que le
fichier PROPRE du journal (`integrity._get_pending_path`). Les journaux des
projets non enregistres vivent tous dans le dossier d'audit du profil, ou
un ancien fichier de secours PARTAGE (`recoverland_pending.json`) peut
encore contenir des editions de ces couches ; il n'est jamais reintegre
automatiquement (le journal de ses evenements ne peut pas etre prouve,
`integrity._report_shared_pending`). `integrity.pending_files_for` les
nomme tous les deux.

Ce que l'utilisateur voyait
---------------------------
Un rewind planifie sans ces editions, annonce complet : l'etat produit
ignore des modifications qu'il a faites. Et quand un refus venait du
fichier partage, le message promettait une reintegration au prochain
demarrage qui n'arrive jamais.

Cas (journal d'un projet non enregistre, dans <profil>/recoverland/audit/)
-------------------------------------------------------------------------
P1  le fichier partage tient une edition du perimetre : refus, qui nomme ce
    fichier et dit qu'un redemarrage ne le reintegrera pas ; aussi par le
    vrai point d'entree de la restauration (aucun runner cree).
P2  le fichier partage ne tient que des editions d'une autre couche : la
    planification continue (controle).
P3  le fichier propre du journal tient une edition du perimetre : refus
    "reintegre au prochain demarrage" (controle, comme avant).

Verdict avant correctif : FAIL (P1). Apres : PASS.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_barrier_shared_pending"
INVARIANT = "I-PLAN-COMPLETE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_barrier_shared_routes"
_OTHER = "ds_barrier_shared_bati"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _audit_journal(root, name):
    """A journal of the profile's shared audit directory."""
    h = _h()
    audit = Path(root) / name / "recoverland" / "audit"
    audit.mkdir(parents=True, exist_ok=True)
    return h.new_journal(str(audit), "unsaved_1a2b3c4d.sqlite")


def _write(path, fps):
    events = [{"datasource_fingerprint": fp, "operation_type": "UPDATE",
               "created_at": "2026-09-01T10:00:00+00:00",
               "feature_identity_json": json.dumps({"fid": 1})}
              for fp in fps]
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(events, fh)
    return path


def _barrier(journal, fps, action="Rewind"):
    import recoverland.recover_dialog as rd
    h = _h()
    dlg = h.build_dialog(rd, h.Journal(journal), h.RecordingWriteQueue(journal),
                         [_FP])
    logs = []
    seams = h.Seams()
    try:
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        ok = dlg._journal_barrier(action, fps, "rw_bsp")
    finally:
        seams.restore()
        h.close_dialog(dlg)
    return ok, [m for m, _l in logs]


def _entry_point(journal):
    """The real restore entry point, the shared file holding the scope."""
    import recoverland.recover_dialog as rd
    h = _h()
    e1 = h.insert_event(journal, _FP, "routes", "UPDATE", 10, fid=1)
    (ev,) = h.fetch_events(journal, [e1])
    dlg = h.build_dialog(rd, h.Journal(journal), h.RecordingWriteQueue(journal),
                         [_FP])
    dlg._search_events = [ev]
    dlg.selected_rows = [0]
    runners, logs = [], []
    seams = h.Seams()
    h.PreflightDialog.shown = []
    try:
        seams.set(rd, "RestoreRunner", h.recording_runner_class(runners))
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        seams.set(rd, "find_target_layer",
                  lambda _evt, _c=None: h.FakeLayer("lyr_routes", "routes"))
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))
        dlg.restore_selected_data()
    finally:
        seams.restore()
        h.close_dialog(dlg)
    return len(runners), [m for m, _l in logs]


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_bsp_")
    flog(f"rw_barrier_shared_pending setup: trace_id={ctx.trace_id}")


def run(ctx):
    from recoverland.core.logger import flog
    from recoverland.core.integrity import _get_pending_path
    tmp = ctx.data["tmpdir"]
    ctx.data["error"] = None
    try:
        # P1: the shared legacy file holds an edit of the scope.
        journal = _audit_journal(tmp, "p1")
        shared = _write(os.path.join(os.path.dirname(journal),
                                     "recoverland_pending.json"), [_FP])
        ctx.data["p1_shared"] = shared
        ctx.data["p1"] = _barrier(journal, [_FP])
        ctx.data["p1_entry"] = _entry_point(journal)
        # P2: the shared file holds another layer's edit only.
        journal = _audit_journal(tmp, "p2")
        _write(os.path.join(os.path.dirname(journal),
                            "recoverland_pending.json"), [_OTHER])
        ctx.data["p2"] = _barrier(journal, [_FP])
        # P3: the journal's own pending file holds an edit of the scope.
        journal = _audit_journal(tmp, "p3")
        own = _write(_get_pending_path(journal), [_FP])
        ctx.data["p3_own"] = own
        ctx.data["p3"] = _barrier(journal, [_FP])
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        import traceback
        ctx.data["error"] = f"{exc!r} {traceback.format_exc()[-900:]}"
    flog(f"rw_barrier_shared_pending run: trace_id={ctx.trace_id} "
         f"p1={ctx.data.get('p1')} p1_entry={ctx.data.get('p1_entry')} "
         f"p2={ctx.data.get('p2')} p3={ctx.data.get('p3')} "
         f"error={ctx.data.get('error')}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", d.get("error") is None, f"crash={d.get('error')}")]
    ok, logs = d.get("p1") or (None, [])
    msg = " ".join(logs)
    out.append((
        "p1_shared_pending_file_blocks_planning",
        ok is False and "recoverland_pending.json" in msg,
        f"barriere={ok} message={logs} : une edition de ces couches attend "
        f"dans le fichier de secours partage du profil ; planifier sans elle "
        f"annonce un etat qui l'ignore.",
    ))
    out.append((
        "p1_message_says_no_restart_will_bring_it_back",
        "jamais reintegre" in msg and "redemarrage ne les reintegrera pas" in msg,
        f"message={logs} : ce fichier n'est jamais reintegre ; promettre une "
        f"reintegration au prochain demarrage laisse l'utilisateur attendre "
        f"une correction qui ne vient pas.",
    ))
    n_runners, entry_logs = d.get("p1_entry") or (None, [])
    out.append((
        "p1_restore_entry_point_refused",
        n_runners == 0 and any("recoverland_pending.json" in m
                               for m in entry_logs),
        f"restauration lancee : runners crees={n_runners} messages="
        f"{entry_logs} : rien ne doit etre planifie ni ecrit.",
    ))
    ok2, logs2 = d.get("p2") or (None, [])
    out.append((
        "p2_other_layer_in_shared_file_does_not_block",
        ok2 is True and not logs2,
        f"barriere={ok2} messages={logs2} : une edition d'une autre couche ne "
        f"concerne pas ce plan (controle).",
    ))
    ok3, logs3 = d.get("p3") or (None, [])
    out.append((
        "p3_own_pending_file_still_blocks",
        ok3 is False and any("reintegre au prochain demarrage" in m
                             for m in logs3),
        f"barriere={ok3} messages={logs3} : le fichier propre du journal "
        f"bloque toujours, avec sa promesse exacte (controle).",
    ))
    return out
