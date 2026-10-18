"""rw_undo_keeps_later_edit - l'annulation d'un restore du mode A n'ecrase jamais une valeur posterieure.

Constat couvert : autoundo-modeA-overwrites-later-edits (HIGH), de bout en
bout, sur un vrai GeoPackage.

Mecanisme
---------
Le constat a deux moities. La premiere (le perimetre d'annulation d'un
restore d'evenements portait aussi ses evenements refuses) est prouvee par
`rw_event_restore_undo_scope`. La seconde : `_undo_update_restore` ecrivait
les valeurs NEW de l'evenement sans verifier que l'entite portait encore
ce que le restore avait ecrit. Une valeur saisie apres le restore, hors de
la capture (suivi desactive, fichier modifie hors de QGIS), etait ecrasee
par l'Undo manuel ou par l'auto-undo silencieux qui precede le Rewind
suivant, sans evenement (le suivi est suspendu pendant l'annulation), et
le dialogue annoncait "1 entite(s) remise(s) dans leur etat initial".

Pour l'utilisateur : la parcelle P passe de 'a' a 'b' (evenement E1). Il
restaure E1 en mode A : P revient a 'a'. Il corrige ensuite P a la main
en 'c', suivi desactive. Plus tard il clique "Annuler le dernier restore",
ou deplace le curseur a la date du jour pour "revenir a l'etat actuel" :
P repasse a 'b', sa correction a disparu, et rien dans le journal ne le
dit.

`rs_undo_target_proof` (cas V2) prouve le refus au niveau du service. Ce
scenario prouve la chaine livree : le vrai `RestoreRunner` et le vrai
`restore_batch` ecrivent sur le fichier, la vraie `UndoRunner` et le vrai
`undo_apply_one` annulent, le dialogue compte et invalide ; seuls la
resolution de couche, les boites et le thread de lecture sont scriptes.

  M  Undo manuel apres une correction hors capture : 'c' reste, le refus
     est dit, la trace du restore reste active.
  A  meme chose par l'auto-undo de "revenir a l'etat actuel".
     v7 (lot dialogue, tour 3) : cet auto-undo n'existe plus. Un Rewind au
     curseur de la date du jour (apres la trace du restore) lit la vraie
     fenetre, la trouve vide, n'ecrit rien et le dit ; 'c' reste.

Verdict pre-correctif : FAIL (l'undo ecrit 'b'). Verdict post-correctif :
PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCENARIO_ID = "rw_undo_keeps_later_edit"
INVARIANT = "I-3"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]
if str(_PLUGIN_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PLUGIN_ROOT.parent))

_FP = "ds_later_edit_parcelles"


def _h():
    from scripts.validation.scenarios import _undo_dialog_harness as h
    return h


def _fx():
    from scripts.validation.scenarios import _restore_fixtures as fx
    return fx


def _fields():
    from osgeo import ogr
    return [("nom", ogr.OFTString), ("code", ogr.OFTString)]


def _journal_event(path, name):
    """E1 (nom: a -> b on the parcel of key 1), stored in the real journal."""
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_COLUMNS, AUDIT_EVENT_INSERT_SQL,
        AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    h, fx = _h(), _fx()
    event = fx.audit_event(
        event_id=None, datasource_fingerprint=_FP,
        layer_id_snapshot=f"lyr_{name}", layer_name_snapshot=name,
        operation_type="UPDATE",
        feature_identity_json=json.dumps(
            {"fid": 1, "pk_field": "fid", "pk_value": 1}),
        entity_fingerprint="pk:fid=1",
        attributes_json=json.dumps(
            {"changed_only": {"nom": {"old": "a", "new": "b"}}}),
        field_schema_json=fx.schema(("fid", "integer"), ("nom", "string"),
                                    ("code", "string")),
        created_at=(h.T0 + timedelta(seconds=100)).isoformat())
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(
            "INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
            + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")",
            [getattr(event, col, None) for col in AUDIT_EVENT_INSERT_COLUMNS])
        conn.commit()
        eid = cur.lastrowid
    finally:
        conn.close()
    return h.fetch_events(path, [eid])[0]


def _names(gpkg, name):
    """The 'nom' column as the user reads it back: a fresh layer, no cache."""
    layer = _fx().open_layer(gpkg, name)
    try:
        return sorted(str(f["nom"]) for f in layer.getFeatures())
    finally:
        del layer


def _restore_then_edit(ctx, h, rd, rr, dlg, wq, layer, gpkg, name, event, key):
    """The real mode A restore of E1, then the user's out-of-capture edit."""
    runner = rr.RestoreRunner([event], lambda _e: layer, wq, tracker=None,
                              trace_id=ctx.trace_id)
    results = []
    runner.finished.connect(results.append)
    runner.finished.connect(dlg._on_event_restore_done)
    dlg._event_restore_events = [event]
    dlg._restore_runner = runner
    runner.start()
    h.pump_until(lambda: bool(results), 10.0)
    ctx.data[f"{key}_after_restore"] = _names(gpkg, name)
    ctx.data[f"{key}_armed"] = sorted(h.all_ids(dlg._last_restore_by_ds))
    # Tracking off (or another program): the provider is written directly.
    idx = layer.fields().indexOf("nom")
    layer.dataProvider().changeAttributeValues({1: {idx: "c"}})
    layer.reload()
    ctx.data[f"{key}_after_edit"] = _names(gpkg, name)


def _case(ctx, key, auto):
    import recoverland.recover_dialog as rd
    import recoverland.restore_runner as rr
    h, fx = _h(), _fx()
    from osgeo import ogr
    tmpdir = ctx.data["tmpdir"]
    name = f"parcelles_{key}"
    gpkg = fx.make_gpkg(tmpdir, name, ogr.wkbPoint, _fields(), [
        (1, ["b", "K1"], "POINT (1 1)"), (2, ["AUTRE", "K2"], "POINT (2 2)")])
    path = h.new_journal(tmpdir, f"{key}.sqlite")
    event = _journal_event(path, name)
    ctx.data[f"{key}_eid"] = event.event_id
    now = h.T0 + timedelta(days=1)
    wq = h.RecordingWriteQueue(path)
    dlg = h.build_dialog(rd, h.Journal(path), wq, [_FP], cutoff_dt=now)
    layer = fx.open_layer(gpkg, name)
    logs, asked, fetches, stricts, done = [], [], [], [], []
    seams = h.Seams()
    try:
        seams.set(rd, "find_target_layer", lambda _evt, _c=None: layer)
        seams.set(rd, "QMessageBox", h.message_box_class(rd, asked))
        seams.set(rd, "qlog", lambda msg, level="INFO": logs.append((str(msg), level)))
        # Hypothesis revised (v7): the fetch was scripted ([]) at a cutoff
        # (T0 + 1 day) BEFORE the restore trace, which the harness writes at
        # the real clock: the real window would have carried a re-application
        # there, not "revenir a l'etat actuel" (revue brev2). The REAL window
        # is read now, and case A puts the cursor after the trace.
        seams.set(rd, "VersionFetchThread", h.real_fetch_class(fetches))
        seams.set(rd, "StrictRestoreRunner", h.strict_runner_class(stricts))
        _restore_then_edit(ctx, h, rd, rr, dlg, wq, layer, gpkg, name, event, key)
        if auto:
            # The slider at today's date, after the restore's trace:
            # "revenir a l'etat actuel". There is no auto-undo any more: the
            # Rewind starts from its entry point and ends on its own.
            dlg.time_slider.cutoff_dt = (datetime.now(timezone.utc)
                                         + timedelta(minutes=1))
            n_logs = len(logs)
            dlg._is_recovering = True
            dlg._recover_version_mode()
            h.pump_until(lambda: (not dlg._is_recovering
                                  and not dlg._restore_in_progress
                                  and dlg._restore_runner is None), 10.0)
            done.append(True)
            ctx.data[f"{key}_runs"] = [len(s.events) for s in stricts]
            ctx.data[f"{key}_rewind_logs"] = logs[n_logs:]
        else:
            original = dlg._on_undo_done

            def _record(res, _orig=original):
                _orig(res)
                done.append(res)
            dlg._on_undo_done = _record
            dlg._undo_last_restore()
        h.pump_until(lambda: bool(done), 10.0)
        h.pump_until(lambda: False, 0.2)
        ctx.data[f"{key}_undo_ran"] = bool(done)
        ctx.data[f"{key}_after_undo"] = _names(gpkg, name)
        ctx.data[f"{key}_trace"] = h.trace_state(path, [event.event_id]).get(
            event.event_id)
        ctx.data[f"{key}_logs"] = list(logs)
    finally:
        seams.restore()
        h.close_dialog(dlg)


def setup(ctx):
    ctx.data["tmpdir"] = tempfile.mkdtemp(prefix="rl_undo_later_edit_")


def run(ctx):
    from recoverland.core.logger import flog
    flog(f"rw_undo_keeps_later_edit run start: trace_id={ctx.trace_id}")
    try:
        _case(ctx, "m", auto=False)
        _case(ctx, "a", auto=True)
    finally:
        flog(f"rw_undo_keeps_later_edit run end: trace_id={ctx.trace_id}")
        shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _says_not_undone(logs) -> bool:
    """A message, WARNING or ERROR, that says the entity was NOT put back."""
    for msg, level in logs:
        if level in ("WARNING", "ERROR") and (
                "aucune entite remise" in msg or "echec" in msg
                or "incomplete" in msg or "non remise" in msg):
            return True
    return False


def _claims_clean_undo(logs) -> bool:
    return any(level == "INFO" and "remise(s) dans leur etat initial" in msg
               for msg, level in logs)


def assertions(ctx):
    out = []
    for key, label in (("m", "Undo manuel"), ("a", "auto-undo")):
        logs = ctx.data.get(f"{key}_logs") or []
        out += [
            (f"{key.upper()}_restore_wrote_old_then_user_typed_c",
             ctx.data.get(f"{key}_after_restore") == ["AUTRE", "a"]
             and ctx.data.get(f"{key}_after_edit") == ["AUTRE", "c"]
             and ctx.data.get(f"{key}_armed") == [ctx.data.get(f"{key}_eid")]
             and ctx.data.get(f"{key}_undo_ran") is True,
             f"apres restore={ctx.data.get(f'{key}_after_restore')} apres "
             f"saisie={ctx.data.get(f'{key}_after_edit')} perimetre="
             f"{ctx.data.get(f'{key}_armed')} undo="
             f"{ctx.data.get(f'{key}_undo_ran')} (controle : le vrai restore "
             f"a ecrit 'a', l'utilisateur a saisi 'c')"),
            (f"{key.upper()}_later_edit_survives_the_{'auto_' if key == 'a' else ''}undo",
             ctx.data.get(f"{key}_after_undo") == ["AUTRE", "c"],
             f"{label} : fichier={ctx.data.get(f'{key}_after_undo')} attendu "
             f"['AUTRE', 'c'] : la valeur 'c' saisie APRES le restore a ete "
             f"remplacee par 'b', la valeur NEW de E1, sans evenement."),
            # Hypothesis revised (v7), case A only: the auto-undo that had to
            # refuse and say so does not exist any more; a Rewind to "now"
            # writes nothing. Checked as strictly: no run was launched, the
            # message says nothing was changed, and never claims a restore
            # or a clean undo. Case M is unchanged.
            (f"{key.upper()}_refusal_is_said",
             (_says_not_undone(logs) and not _claims_clean_undo(logs))
             if key == "m" else
             (ctx.data.get("a_runs") == []
              and any("rien n'a ete modifie" in m
                      for m, _l in ctx.data.get("a_rewind_logs") or [])
              and not _claims_clean_undo(logs)
              and not any("restauree" in m
                          for m, _l in ctx.data.get("a_rewind_logs") or [])),
             f"{label} : messages={logs} runs={ctx.data.get(f'{key}_runs')} : "
             f"l'utilisateur doit lire que l'entite n'a pas ete remise (ou, "
             f"pour un Rewind a la date du jour, que rien n'a change), jamais "
             f"'1 entite(s) remise(s) dans leur etat initial'."),
            (f"{key.upper()}_restore_trace_stays_active",
             ctx.data.get(f"{key}_trace") == "active",
             f"{label} : trace du restore={ctx.data.get(f'{key}_trace')} : "
             f"l'undo n'a rien defait ; invalider la trace ferait rejouer "
             f"la compensation de E1 au Rewind suivant."),
        ]
    return out


if __name__ == "__main__":
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
