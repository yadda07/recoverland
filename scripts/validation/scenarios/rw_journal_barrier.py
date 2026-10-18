"""rw_journal_barrier - aucun Restore, Rewind ou Undo ne se planifie sur un journal incomplet.

Constat couvert : unflushed-and-pending-events-invisible (HIGH).

Mecanisme
---------
La lecture du Rewind (`VersionFetchThread`) ne voit que les lignes deja
commitees dans le journal. Rien n'attendait la file d'ecriture, et rien ne
regardait le fichier de secours (`recoverland_pending.json`) ni la boite
d'envoi durable (outbox v7) d'un ecrivain mort : ce sont pourtant des
editions de l'utilisateur, simplement pas encore indexees. Le Rewind les
ignorait, ne les compensait pas, et s'annoncait reussi. Au demarrage
suivant elles etaient importees avec leur `created_at` d'origine, donc a
l'interieur de l'intervalle d'une trace deja ecrite pour la meme entite, et
restaient neutralisees : "Rien a restaurer" alors que la valeur est fausse.

Ce qui est exige (et prouve ici sur les vraies methodes du dialogue) :
avant de planifier un Rewind, un restore d'evenements, un Undo ou un Undo
de session, le dialogue appelle `write_queue.wait_until_drained(timeout)` et
refuse EXPLICITEMENT quand la barriere n'est pas atteinte ; il refuse aussi
quand des evenements du perimetre attendent dans la boite d'envoi ou dans
le fichier de secours.

  B1..B4  barriere non atteinte : Rewind, restore, Undo, Undo de session
          refuses, sans boite de confirmation, message nommant le journal ;
  B5      barriere atteinte mais lots durables non indexes (ecrivain mort) ;
  B6      fichier de secours contenant un evenement du perimetre ;
  B7/B8   antitheses : journal a jour (et secours d'une AUTRE couche) ->
          Rewind et Undo partent normalement.

Verdict pre-correctif : FAIL. Verdict post-correctif : PASS.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

SCENARIO_ID = "rw_journal_barrier"
INVARIANT = "I-READ-YOUR-WRITES"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_barrier_reseau"
_OTHER_FP = "ds_barrier_autre"
_SESSION = "sess-barrier-0001"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _seed(h, directory, name):
    path = h.new_journal(directory, name)
    e1 = h.insert_event(path, _FP, "reseau", "UPDATE", 100, fid=1,
                        session_id=_SESSION)
    e2 = h.insert_event(path, _FP, "reseau", "UPDATE", 110, fid=2,
                        session_id=_SESSION)
    h.insert_event(path, _FP, "reseau", "UPDATE", 400, fid=1, restored_from=e1)
    return path, {"e1": e1, "e2": e2}


def _write_pending(path, fp):
    pending = os.path.join(os.path.dirname(path), "recoverland_pending.json")
    with open(pending, "w", encoding="utf-8") as fh:
        json.dump([{"datasource_fingerprint": fp, "operation_type": "UPDATE",
                    "created_at": "2026-09-01T08:05:00+00:00",
                    "layer_name_snapshot": "reseau"}], fh)
    return pending


class _Probe:
    """Everything a refused entry point must NOT have done, plus what it said."""

    def __init__(self, h, rd, rr):
        self.h, self.rd, self.rr = h, rd, rr
        self.fetches, self.runners, self.stricts = [], [], []
        self.asked, self.logs = [], []
        self.seams = h.Seams()
        layer = h.FakeLayer("lyr_reseau", "reseau")
        h.PreflightDialog.shown = []
        s = self.seams
        s.set(rd, "VersionFetchThread", h.fetch_thread_class([], self.fetches))
        s.set(rd, "RestoreRunner", h.recording_runner_class(self.runners))
        s.set(rd, "UndoRunner", h.recording_runner_class(self.runners))
        s.set(rd, "StrictRestoreRunner", h.strict_runner_class(self.stricts))
        s.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        s.set(rd, "QMessageBox", h.message_box_class(rd, self.asked))
        s.set(rd, "find_target_layer", lambda _evt, _conn=None: layer)
        s.set(rd, "qlog", lambda msg, level="INFO": self.logs.append((str(msg), level)))

    def close(self):
        self.seams.restore()

    def said(self, *needles):
        return [(m, lvl) for m, lvl in self.logs
                if lvl in ("WARNING", "ERROR")
                and all(n.lower() in m.lower() for n in needles)]


def _dialog(h, rd, path, ids, wq):
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP],
                         cutoff_dt=h.T0 + timedelta(seconds=50))
    ev = h.fetch_events(path, [ids["e1"], ids["e2"]])
    dlg._search_events = list(ev)
    dlg.selected_rows = [0]
    dlg._last_restore_by_ds = {_FP: [ev[0]]}
    dlg._last_restore_events = [ev[0]]
    # The previous restore (the last run) wrote e1: what the Undo button
    # reverts (`_manual_undo_scope`).
    dlg._last_run_ids = {ev[0].event_id}
    return dlg


def _entry_points(ctx, h, rd, rr, prefix, *, drained=True, outstanding=0,
                  pending_fp=None, which=("rewind", "restore", "undo", "session")):
    path, ids = _seed(h, ctx.data["tmp"].name, f"{prefix}.sqlite")
    if pending_fp:
        _write_pending(path, pending_fp)
    out = {}
    for entry in which:
        wq = h.RecordingWriteQueue(path)
        wq.drained = drained
        wq.outbox_outstanding = outstanding
        dlg = _dialog(h, rd, path, ids, wq)
        probe = _Probe(h, rd, rr)
        try:
            if entry == "rewind":
                dlg._recover_version_mode()
            elif entry == "restore":
                dlg.restore_selected_data()
            elif entry == "undo":
                dlg._undo_last_restore()
            else:
                dlg._undo_session(_SESSION)
            out[entry] = {
                "barrier_calls": len(wq.barrier_calls),
                "fetches": len(probe.fetches),
                "runners": len(probe.runners),
                "asked": len(probe.asked),
                "preflight_shown": len(h.PreflightDialog.shown),
                "refusal": probe.said("journal"),
                "logs": list(probe.logs),
            }
        finally:
            probe.close()
            h.close_dialog(dlg)
    ctx.data[prefix] = out


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog

    h = _h()
    ctx.data["tmp"] = tempfile.TemporaryDirectory(prefix="rl_barrier_")
    flog(f"rw_journal_barrier run start: trace_id={ctx.trace_id}")
    try:
        _entry_points(ctx, h, rd, rr, "b_undrained", drained=False)
        _entry_points(ctx, h, rd, rr, "b_outbox", outstanding=2,
                      which=("rewind", "undo"))
        _entry_points(ctx, h, rd, rr, "b_pending", pending_fp=_FP,
                      which=("rewind", "undo"))
        _entry_points(ctx, h, rd, rr, "b_clean", pending_fp=_OTHER_FP,
                      which=("rewind", "undo"))
    finally:
        flog(f"rw_journal_barrier run end: trace_id={ctx.trace_id}")
        ctx.data["tmp"].cleanup()


def _refused(case):
    return (case.get("fetches") == 0 and case.get("runners") == 0
            and case.get("asked") == 0 and case.get("preflight_shown") == 0
            and bool(case.get("refusal")))


def assertions(ctx):
    und = ctx.data.get("b_undrained") or {}
    outb = ctx.data.get("b_outbox") or {}
    pend = ctx.data.get("b_pending") or {}
    clean = ctx.data.get("b_clean") or {}
    out = []
    labels = {
        "rewind": "le Rewind",
        "restore": "le restore d'evenements",
        "undo": "l'Undo",
        "session": "l'Undo de session",
    }
    for entry, label in labels.items():
        case = und.get(entry, {})
        out.append((
            f"B_undrained_{entry}_asks_the_barrier",
            case.get("barrier_calls", 0) >= 1,
            f"{label} n'a pas appele wait_until_drained "
            f"(appels={case.get('barrier_calls')}) : il planifie sur ce que "
            f"le journal contient deja, sans les editions encore en file."))
        out.append((
            f"B_undrained_{entry}_refuses_explicitly",
            _refused(case),
            f"{label} avec barriere non atteinte : relectures="
            f"{case.get('fetches')} runners={case.get('runners')} "
            f"questions={case.get('asked')} preflight="
            f"{case.get('preflight_shown')} refus={case.get('refusal')} ; "
            f"il doit refuser, avant toute question, en nommant le journal. "
            f"messages={case.get('logs')}"))
    for entry in ("rewind", "undo"):
        case = outb.get(entry, {})
        out.append((
            f"B_outbox_backlog_{entry}_refused",
            _refused(case),
            f"{labels[entry]} avec 2 lots durables non indexes (ecrivain "
            f"mort) : relectures={case.get('fetches')} runners="
            f"{case.get('runners')} refus={case.get('refusal')} ; ces "
            f"editions existent mais le Rewind ne les voit pas."))
        case = pend.get(entry, {})
        out.append((
            f"B_pending_file_{entry}_refused",
            _refused(case)
            and any("recoverland_pending.json" in m for m, _l in case.get("refusal", [])),
            f"{labels[entry]} avec un evenement du perimetre dans le fichier "
            f"de secours : relectures={case.get('fetches')} runners="
            f"{case.get('runners')} refus={case.get('refusal')} ; le refus "
            f"doit nommer le fichier pour que l'utilisateur puisse agir."))
    case = clean.get("rewind", {})
    out.append((
        "B_clean_rewind_proceeds",
        case.get("fetches") == 1 and not case.get("refusal"),
        f"journal a jour, secours d'une autre couche : relectures="
        f"{case.get('fetches')} refus={case.get('refusal')} ; le Rewind "
        f"doit partir normalement (antithese)."))
    case = clean.get("undo", {})
    out.append((
        "B_clean_undo_proceeds",
        case.get("runners") == 1 and case.get("asked") == 1,
        f"journal a jour : runners={case.get('runners')} questions="
        f"{case.get('asked')} ; l'Undo doit partir normalement (antithese)."))
    return out


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
