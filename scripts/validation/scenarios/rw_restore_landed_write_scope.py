# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_restore_landed_write_scope - une ecriture qui a atterri est journalisee et reste annulable, meme quand l'evenement echoue.

Constat couvert : suite de event-restore-undo-scope-unfiltered (CRITICAL),
trouvee par la revue adversariale du premier correctif.

Mecanisme
---------
Le premier correctif restreint le perimetre d'annulation et les traces
d'un restore d'evenements (`RestoreRunner._finish`) aux evenements du seau
`applied` : un saut ou un refus n'a rien ecrit, il n'y a rien a defaire ni
a journaliser. Mais `restore_batch` (v7) connait un troisieme cas : une
ecriture qui a ATTERRI dans le fichier alors que l'evenement est declare en
echec (`failed_postcommit_verification` avec ``landed``: la relecture
contredit ce qui a ete ecrit, ou une ecriture refusee n'a pas pu etre
reprise). Il construit pour elle une trace, parce que les donnees ont
change. Le filtre sur `applied` jetait cette trace et retirait l'evenement
du perimetre d'annulation.

Pour l'utilisateur : le compte rendu dit "1 echouee" (juste), mais le
journal ignore que l'entite a ete ecrite. Le Rewind suivant la tient pour
non compensee et rejoue sa compensation par-dessus (pour un DELETE : un
addFeature de plus, l'entite en double), et "Annuler le dernier restore"
ne peut pas la retirer. StrictRestoreRunner applique deja la regle
inverse : ce qui a atterri est journalise ET garde dans le perimetre
d'annulation.

Ce qui est prouve, sur le vrai `restore_batch` (seul son aiguillage vers le
fournisseur est scripte), le vrai `RestoreRunner`, le vrai `UndoRunner` et
les vraies methodes du dialogue :

  L  trois evenements : un applique, un echoue dont l'ecriture a atterri,
     un refuse sans ecriture. Les deux ecritures sont journalisees et
     annulables, le refus ne l'est pas, et les compteurs du compte rendu
     ne bougent pas (l'ecriture atterrie reste un echec).

Verdict avant ce correctif (arbre de travail v7) : FAIL. Sur le code
d'origine : FAIL aussi (restore_batch ne tracait pas l'ecriture atterrie,
et le perimetre publie contenait le refus).
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_restore_landed_write_scope"
INVARIANT = "I-UNDO-SCOPE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_landed_routes"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def setup(ctx):
    return None


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    import recoverland.core.restore_service as rs
    from recoverland.core.logger import flog

    h = _h()
    tmp = tempfile.TemporaryDirectory(prefix="rl_landed_scope_")
    flog(f"rw_restore_landed_write_scope run start: trace_id={ctx.trace_id}")
    try:
        path = h.new_journal(tmp.name, "landed.sqlite")
        e_ok = h.insert_event(path, _FP, "routes", "DELETE", 100, fid=1)
        e_landed = h.insert_event(path, _FP, "routes", "DELETE", 110, fid=2)
        e_refused = h.insert_event(path, _FP, "routes", "DELETE", 120, fid=3)
        ctx.data["ids"] = (e_ok, e_landed, e_refused)
        events = h.fetch_events(path, [e_ok, e_landed, e_refused])
        wq = h.RecordingWriteQueue(path)
        dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP])
        layer = h.FakeLayer("lyr_routes", "routes")

        def handler(_layer, event, _fid_cache):
            if event.event_id == e_landed:
                return {"success": False,
                        "reason_code": "failed_postcommit_verification",
                        "landed": True, "fid": 42,
                        "message": "Inserted, but the provider holds "
                                   "something else (read back)"}
            if event.event_id == e_refused:
                return {"success": False, "reason_code": "provider_refused",
                        "message": "Write refused by the provider"}
            return {"success": True, "message": "Restored", "fid": 41}

        undo_calls, logs, asked = [], [], []
        seams = h.Seams()
        try:
            seams.set(rs, "_restore_dispatch",
                      lambda: {"DELETE": handler, "INSERT": handler,
                               "UPDATE": handler})
            seams.set(rs, "validate_restore_layer_state", lambda _l: None)
            seams.set(rs, "build_restore_trace_event",
                      lambda ev, _l: h.trace_for(ev))
            # Hypothesis revised (v7): the runners hand the executor the journal read
            # connection of the layer group (`journal=`, the published signature);
            # the doubles take it like the real functions, nothing else changes.
            seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
            prepare, apply_one = h.scripted_undo({}, undo_calls)
            seams.set(rr, "undo_prepare", prepare)
            seams.set(rr, "undo_apply_one", apply_one)
            seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
            seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
            seams.set(rd, "qlog",
                      lambda msg, level="INFO": logs.append((str(msg), level)))

            # Control: what the real restore_batch hands the runner.
            rep = rs.restore_batch(layer, events, fid_cache={})
            ctx.data["batch_traces"] = sorted(
                t.restored_from_event_id for t in rep.trace_events)
            ctx.data["batch_failed"] = sorted(rep.failed)

            runner = rr.RestoreRunner(events, lambda _e: layer, wq,
                                      tracker=None, trace_id=ctx.trace_id)
            results = []
            runner.finished.connect(results.append)
            runner.finished.connect(dlg._on_event_restore_done)
            dlg._event_restore_events = events
            dlg._restore_runner = runner
            runner.start()
            h.pump_until(lambda: bool(results), 10.0)
            ctx.data["finished"] = bool(results)
            ctx.data["report"] = dict(runner.apply_report())
            ctx.data["journalled"] = sorted(
                i for ids_, _jp, _s in wq.enqueued for i in ids_ if i)
            ctx.data["scope"] = (sorted(h.all_ids(results[0].by_ds))
                                 if results else None)
            ctx.data["undo_state"] = sorted(h.all_ids(dlg._last_restore_by_ds))

            done = []
            original = dlg._on_undo_done

            def _record(res, _orig=original):
                _orig(res)
                done.append(res)
            dlg._on_undo_done = _record
            dlg._undo_last_restore()
            h.pump_until(lambda: bool(done), 10.0)
            ctx.data["undo_calls"] = sorted(e for _n, e in undo_calls)
            ctx.data["traces_after_undo"] = h.trace_state(
                path, [e_ok, e_landed, e_refused])
            ctx.data["errors"] = [m for m, level in logs if level == "ERROR"]
        finally:
            seams.restore()
            h.close_dialog(dlg)
    finally:
        flog(f"rw_restore_landed_write_scope run end: trace_id={ctx.trace_id}")
        tmp.cleanup()


def assertions(ctx):
    e_ok, e_landed, e_refused = ctx.data.get("ids") or (None, None, None)
    report = ctx.data.get("report") or {}
    after = ctx.data.get("traces_after_undo") or {}
    return [
        ("L_control_restore_batch_traces_the_landed_write",
         ctx.data.get("batch_traces") == sorted([e_ok, e_landed])
         and ctx.data.get("batch_failed") == sorted([e_landed, e_refused]),
         f"restore_batch traces={ctx.data.get('batch_traces')} echecs="
         f"{ctx.data.get('batch_failed')} (controle : c'est le contrat v7 de "
         f"core.restore_service que le runner doit respecter)"),
        ("L_run_finished", ctx.data.get("finished") is True,
         "le RestoreRunner doit emettre finished"),
        ("L_landed_write_is_journalled",
         ctx.data.get("journalled") == sorted([e_ok, e_landed]),
         f"traces envoyees au journal={ctx.data.get('journalled')} attendu "
         f"{sorted([e_ok, e_landed])} : l'ecriture de {e_landed} a atterri ; "
         f"sans trace, le Rewind suivant rejoue sa compensation par-dessus "
         f"(entite en double). Le refus {e_refused} n'a rien ecrit."),
        ("L_landed_write_stays_revertible",
         ctx.data.get("scope") == sorted([e_ok, e_landed])
         and ctx.data.get("undo_state") == sorted([e_ok, e_landed]),
         f"perimetre publie={ctx.data.get('scope')} etat du dialogue="
         f"{ctx.data.get('undo_state')} attendu {sorted([e_ok, e_landed])} : "
         f"ce qui a ete ecrit doit pouvoir etre annule, et rien d'autre."),
        # Hypothesis revised (v7): this check read the landed failure in the
        # residual bucket `failed` ("pour une autre raison"). The dialog
        # round 3 (revue brev2, MINOR) gives it its own bucket,
        # `failed_postcommit_verification`, so the report says the file WAS
        # changed. The invariant is unchanged and checked more strictly: the
        # landed write is still a failure (failure total `apply_fail` == 2,
        # as `failed` was), only one entity was applied, and the landed one
        # is now counted under its own name, never under "other reason".
        ("L_counters_unchanged_landed_is_a_failure",
         report.get("applied") == 1 and report.get("apply_fail") == 2
         and report.get("failed", 0)
         + report.get("failed_postcommit_verification", 0) == 2
         and report.get("failed_postcommit_verification") == 1,
         f"apply_report={report} : l'ecriture atterrie reste un echec pour "
         f"l'utilisateur (la relecture la contredit), comptee dans son seau "
         f"'ecrite mais contredite' ; seul le perimetre d'annulation et le "
         f"journal changent."),
        ("L_undo_reverts_both_writes_and_nothing_else",
         ctx.data.get("undo_calls") == sorted([e_ok, e_landed]),
         f"inverses={ctx.data.get('undo_calls')} : l'undo doit viser les deux "
         f"ecritures, jamais le refus {e_refused}."),
        ("L_journal_after_undo_matches_the_data",
         after.get(e_ok) == "invalidated" and after.get(e_landed) == "invalidated"
         and after.get(e_refused) == "none" and not ctx.data.get("errors"),
         f"traces apres undo={after} erreurs={ctx.data.get('errors')} : les "
         f"deux traces defaites sont invalidees, le refus n'en a jamais eu."),
    ]


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
