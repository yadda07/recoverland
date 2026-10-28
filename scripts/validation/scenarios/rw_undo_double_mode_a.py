# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""rw_undo_double_mode_a - l'Undo d'une seconde restauration du meme evenement.

Invariant : I-UNDO-TRACE (l'Undo n'invalide que les traces du run qu'il
defait ; le journal decrit toujours les couches).

Le mecanisme
------------
"Annuler le dernier restore" remet les entites du dernier run, puis marque
au journal que sa compensation est defaite : il invalide la trace de
l'evenement (`restored_from_event_id`). La requete d'origine visait TOUTES
les traces actives de l'evenement. Or le mode A permet de restaurer deux
fois le meme evenement :

  E   edition de l'entite (valeur V1 -> V2)
  R1  restauration de E : l'entite reprend V1, trace T1 (active)
  U   l'utilisateur reedite l'entite (V1 -> V3), evenement journalise
  R2  nouvelle restauration de E : l'entite reprend V1, trace T2
  Un  "Annuler le dernier restore" : l'Undo defait R2, l'entite reprend V3

L'Undo ne defait que R2. Invalider T1 aussi dit au journal que la
compensation de R1 n'a jamais eu lieu : un Rewind a une date entre R1 et
U (ou l'entite valait V1) rejoue E comme non compensee, et compte faux.

Depuis v7, quand le runner lui donne le journal, l'executeur refuse R2
par son nom (`already_compensated`, scenario rs_mode_a_double_restore).
Deux traces actives de E restent pourtant possibles : un journal ecrit
par une version precedente (son RestoreRunner ne donnait pas le journal a
l'executeur). Un R2 lance quand le journal ne peut pas etre lu est
refuse lui aussi, couche entiere (rw_runner_journal_locked). L'ecriture de
couche est donc scriptee ici (R2 ecrit), pour eprouver l'Undo sur le
journal d'une version precedente.

Ce qui est prouve, avec les vraies methodes du dialogue
(`restore_selected_data`, `_undo_last_restore` et leurs slots de fin),
les vrais RestoreRunner et UndoRunner, un vrai journal SQLite ; seules les
ecritures de couche sont scriptees :

  *_second_run_trace_invalidated  T2 est invalidee ;
  *_first_run_trace_kept_active   T1 reste active ;
  *_journal_verdict_ok            la relecture du dialogue ne signale
                                  aucune desynchronisation.

Verdict avant correctif (code d'origine) : FAIL (T1 invalidee avec T2).
Verdict apres correctif : PASS.
"""
from __future__ import annotations

import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_undo_double_mode_a"
INVARIANT = "I-UNDO-TRACE"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_double_mode_a"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _traces(path, source):
    conn = sqlite3.connect(path)
    try:
        return [(int(tid), inv) for tid, inv in conn.execute(
            "SELECT event_id, invalidated_at FROM audit_event "
            "WHERE restored_from_event_id = ? ORDER BY event_id",
            (source,)).fetchall()]
    finally:
        conn.close()


def _restore(dlg, h, rd, results_attr="_restore_runner"):
    dlg.restore_selected_data()
    h.pump_until(lambda: getattr(dlg, results_attr, None) is None
                 and not dlg._restore_in_progress, 10.0)
    h.pump_until(lambda: False, 0.1)


def setup(ctx):
    from recoverland.core.logger import flog
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_rw_dma_")
    flog(f"rw_undo_double_mode_a setup: trace_id={ctx.trace_id}")


def run(ctx):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    from recoverland.core.logger import flog
    h = _h()
    tmp = ctx.data["tmpdir"]
    path = h.new_journal(tmp, "dma.sqlite")
    e = h.insert_event(path, _FP, "parcelles", "UPDATE", 100, fid=1,
                       attrs={"changed_only": {"v": {"old": "V1", "new": "V2"}}})
    ctx.data["e"] = e
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP])
    layer = h.FakeLayer("lyr_parcelles", "parcelles")
    restore_calls, undo_calls, logs, asked = [], [], [], []
    seams = h.Seams()
    ctx.data["error"] = None
    try:
        seams.set(rr, "restore_batch", h.scripted_restore_batch({}, restore_calls))
        seams.set(rr, "build_fid_cache", lambda _l, _e, journal=None: {})
        prepare, apply_one = h.scripted_undo({}, undo_calls)
        seams.set(rr, "undo_prepare", prepare)
        seams.set(rr, "undo_apply_one", apply_one)
        seams.set(rd, "RestorePreflightDialog", h.PreflightDialog)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "qlog",
                  lambda msg, level="INFO": logs.append((str(msg), level)))

        (ev,) = h.fetch_events(path, [e])
        dlg._search_events = [ev]
        dlg.selected_rows = [0]
        # R1: first restore of E.
        _restore(dlg, h, rd)
        ctx.data["after_r1"] = _traces(path, e)
        # U: the user edits the entity again; the capture journals it and
        # the plugin tells the dialog.
        h.insert_event(path, _FP, "parcelles", "UPDATE", 300, fid=1,
                       attrs={"changed_only": {"v": {"old": "V1", "new": "V3"}}})
        notify = getattr(dlg, "on_events_committed", None)
        if callable(notify):
            notify(_FP)
        # R2: E restored again.
        dlg._search_events = [ev]
        dlg.selected_rows = [0]
        _restore(dlg, h, rd)
        ctx.data["after_r2"] = _traces(path, e)
        # Un: undo of the last restore (R2).
        n_logs = len(logs)
        dlg._undo_last_restore()
        h.pump_until(lambda: dlg._restore_runner is None
                     and not dlg._restore_in_progress, 10.0)
        h.pump_until(lambda: False, 0.1)
        ctx.data["after_undo"] = _traces(path, e)
        ctx.data["undo_calls"] = list(undo_calls)
        ctx.data["undo_logs"] = [(m[:300], lvl) for m, lvl in logs[n_logs:]]
        ctx.data["restore_calls"] = list(restore_calls)
    except Exception as exc:  # noqa: BLE001 - a crash is a result
        import traceback
        ctx.data["error"] = f"{exc!r} {traceback.format_exc()[-900:]}"
    finally:
        seams.restore()
        h.close_dialog(dlg)
    flog(f"rw_undo_double_mode_a run: trace_id={ctx.trace_id} "
         f"r1={ctx.data.get('after_r1')} r2={ctx.data.get('after_r2')} "
         f"undo={ctx.data.get('after_undo')} logs={ctx.data.get('undo_logs')} "
         f"error={ctx.data.get('error')}")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def assertions(ctx):
    d = ctx.data
    out = [("scenario_ran", d.get("error") is None, f"crash={d.get('error')}")]
    r1 = d.get("after_r1") or []
    r2 = d.get("after_r2") or []
    after = dict(d.get("after_undo") or [])
    t1 = r1[0][0] if len(r1) == 1 else None
    t2 = next((tid for tid, _inv in r2 if tid != t1), None)
    out.append((
        "setup_two_runs_two_traces",
        t1 is not None and t2 is not None and len(r2) == 2
        and all(inv is None for _t, inv in r2),
        f"traces de E apres R1={r1} apres R2={r2} : chaque restauration de E "
        f"doit laisser sa propre trace active, sinon le scenario ne teste rien.",
    ))
    out.append((
        "undo_undid_the_second_run",
        [eid for _layer, eid in d.get("undo_calls") or []] == [d.get("e")],
        f"inverses appliques={d.get('undo_calls')} : l'Undo defait R2, "
        f"l'evenement E.",
    ))
    out.append((
        "second_run_trace_invalidated",
        t2 is not None and after.get(t2) is not None,
        f"trace de R2 T{t2} apres l'Undo : invalidee_a={after.get(t2)!r}. "
        f"Restee active, le journal dirait R2 toujours en place alors que "
        f"l'entite a repris la valeur de l'utilisateur.",
    ))
    out.append((
        "first_run_trace_kept_active",
        t1 is not None and t1 in after and after.get(t1) is None,
        f"trace de R1 T{t1} apres l'Undo : invalidee_a={after.get(t1)!r}. "
        f"L'Undo n'a pas defait R1 : invalider sa trace fait croire au "
        f"journal que E n'a jamais ete compense, et un Rewind a une date "
        f"entre R1 et la reedition rejoue E sur une entite qui valait deja "
        f"V1, en comptant faux.",
    ))
    errors = [m for m, lvl in d.get("undo_logs") or [] if lvl == "ERROR"]
    out.append((
        "journal_verdict_ok",
        not errors,
        f"messages d'erreur apres l'Undo={errors} : la relecture du journal "
        f"doit confirmer l'invalidation, sans desynchronisation.",
    ))
    return out
