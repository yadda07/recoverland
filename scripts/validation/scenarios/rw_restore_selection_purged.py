"""rw_restore_selection_purged - RecoverLand validation runtime.

Invariant: I-8 (un refus dit sa vraie raison) et fail-safe (jamais une
autre restauration que celle que l'utilisateur a confirmee), pour la
restauration des lignes choisies dans le tableau Restore.

Le mecanisme
------------
Le tableau Restore affiche des lignes LEGERES (search_events sans BLOB :
la geometrie n'y est qu'un marqueur d'un octet). Au clic sur Restaurer, le
dialogue calcule et montre le preflight sur ces lignes, puis, une fois le
preflight accepte, relit chaque evenement choisi en entier dans le journal
(event_stream_repository.fetch_events_by_ids) et donne CETTE liste au
runner. La relecture est un `SELECT ... WHERE event_id IN (...)` : un
evenement que le journal ne contient plus n'est simplement pas rendu, et
rien ne le disait. Entre la recherche et le clic, une purge de maintenance
(Maintenance du journal, plafond du nombre d'evenements : les commits les
plus anciens partent en premier) retire des lignes que le tableau affiche
encore : _open_maintenance ne relance pas la recherche.

Le runner recevait alors MOINS d'evenements que le preflight n'en avait
montres, la restauration partielle etait annoncee comme un succes, et les
evenements manquants n'etaient nommes nulle part.

Sans connexion de lecture du journal (create_read_connection qui echoue :
partage reseau coupe, fichier tenu), le dialogue ne relisait rien et
donnait au runner les lignes LEGERES elles-memes : une geometrie qui n'est
qu'un marqueur d'un octet, a la place de la geometrie journalisee.

L'echec concret pour l'utilisateur
----------------------------------
P  La parcelle B s'appelait 'a' ; il la renomme 'b', puis 'c'. Il renomme
   aussi D 'd0' -> 'd1'. Dans Restore il cherche, voit les trois lignes,
   ouvre la Maintenance et purge au plafond (la plus ancienne edition,
   'a' -> 'b', disparait), revient, choisit les trois lignes et clique
   Restaurer. Le preflight annonce 3 evenements ; il confirme. La
   restauration ecrivait B = 'b' (ni 'a' qu'il attendait, ni 'c' qu'il
   avait) et D = 'd0', annoncait deux restaurations reussies, et ne
   disait jamais qu'une edition choisie n'existait plus. Attendu : rien
   d'ecrit (le plan confirme n'est plus executable tel quel ; en executer
   une partie laisse une entite dans un etat intermediaire), l'evenement
   manquant nomme par son numero, la raison dite, et le dialogue rendu.
N  Meme clic quand la connexion de lecture du journal ne s'ouvre plus :
   le runner recevait les lignes legeres (geometrie = marqueur). Attendu :
   rien d'ecrit, la raison nommee, le dialogue rendu.
F  fetch_events_by_ids lui-meme : un identifiant absent du journal est
   nomme (journal), et `require_all=True` le refuse par son numero.

Ce que le scenario pilote : le vrai RecoverPlugin sous FakeIface, une vraie
GeoPackage, de vraies editions capturees par le vrai EditSessionTracker, le
vrai journal, la vraie purge de la maintenance (purge_old_events_with_options
sur la connexion du journal, appelee par le vrai _open_maintenance du
dialogue), la vraie methode restore_selected_data et le vrai RestoreRunner.
Seule la boite du preflight (modale) est remplacee par une boite acceptee
qui enregistre ce qu'elle affiche.

Verdict avant correctif : FAIL (P : B = 'b', D = 'd0', 2 traces, aucun
message ne nomme l'evenement purge ; N : le runner recoit les lignes
legeres ; F : aucun nom, pas de refus). Apres : PASS.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path

SCENARIO_ID = "rw_restore_selection_purged"
INVARIANT = "I-8"

_PLUGIN_ROOT = Path(__file__).resolve().parents[3]

# Qgis.MessageLevel.Critical: what qlog(..., "ERROR") writes to the log panel.
_CRITICAL = 2


def _h():
    from scripts.validation.scenarios import _rvw_harness
    return _rvw_harness


def setup(ctx):
    from recoverland.core.logger import flog
    h = _h()
    tmpdir = tempfile.mkdtemp(prefix="rl_rw_rsp_")
    ctx.data["tmpdir"] = tmpdir
    ctx.data["gpkg"] = h.make_gpkg(tmpdir, "cadastre", [
        ("B", "a", 1.0, 651000.0, 6861000.0),
        ("C", "c0", 1.0, 652000.0, 6862000.0),
        ("D", "d0", 1.0, 653000.0, 6863000.0),
    ])
    flog(f"rw_restore_selection_purged setup: trace_id={ctx.trace_id} "
         f"tmpdir={tmpdir}", "INFO")


def _lightweight_rows(jpath, fingerprint):
    """The rows of the Restore table, fetched as LocalSearchThread fetches them."""
    from recoverland.core.audit_backend import SearchCriteria
    from recoverland.core.search_service import search_events
    conn = sqlite3.connect(jpath)
    try:
        crit = SearchCriteria(datasource_fingerprint=fingerprint, layer_name=None,
                              operation_type=None, user_name=None,
                              start_date=None, end_date=None, page=1,
                              page_size=100)
        return list(search_events(conn, crit, exclude_blobs=True).events)
    finally:
        conn.close()


def _state(layer):
    from qgis.core import QgsVectorLayer
    probe = QgsVectorLayer(layer.source(), "probe", "ogr")
    out = {}
    for f in probe.getFeatures():
        g = f.geometry()
        pt = g.asPoint() if g is not None and not g.isNull() else None
        out[str(f["code"])] = [str(f["nom"]),
                               [pt.x(), pt.y()] if pt is not None else None]
    del probe
    return out


def _traces(jpath):
    conn = sqlite3.connect(jpath)
    try:
        return int(conn.execute(
            "SELECT COUNT(*) FROM audit_event "
            "WHERE restored_from_event_id IS NOT NULL").fetchone()[0])
    finally:
        conn.close()


def _event_ids(jpath):
    conn = sqlite3.connect(jpath)
    try:
        return sorted(int(r[0]) for r in conn.execute(
            "SELECT event_id FROM audit_event"))
    finally:
        conn.close()


class _Preflight:
    """RestorePreflightDialog accepted without a modal box; records what it showed."""

    shown = []

    def __init__(self, _parent=None):
        self.data = None

    def set_preflight_data(self, verdict, summary, detail, blocked):
        self.data = {"verdict": verdict, "summary": str(summary)[:400],
                     "blocked": bool(blocked)}
        _Preflight.shown.append(self.data)

    def exec(self):
        from qgis.PyQt.QtWidgets import QDialog
        return QDialog.DialogCode.Accepted


def _maintenance_class(journal, cap):
    """The maintenance dialog: the user purges down to *cap* events."""

    class _Maintenance:
        purged = []

        def __init__(self, _journal, parent=None):
            self._journal = _journal

        def exec(self):
            # What JournalMaintenanceDialog._purge runs: the journal's own
            # connection, the retention policy of the settings, the scope
            # the user ticked (retention: date AND cap).
            from recoverland.core.retention import (
                PurgeOptions, RetentionPolicy, purge_old_events_with_options,
            )
            conn = self._journal.get_connection()
            res = purge_old_events_with_options(
                conn, RetentionPolicy(retention_days=3650, max_events=cap),
                PurgeOptions(retention=True))
            _Maintenance.purged.append(int(res.deleted_count))
            return 0

    return _Maintenance


def _restore_click(h, dlg, rows, timeout=25.0):
    """The user selects *rows* and clicks Restaurer; waits for the run's end."""
    dlg.selected_rows = list(rows)
    escaped = None
    try:
        dlg.restore_selected_data()
    except Exception as exc:  # noqa: BLE001 - what Qt would receive
        escaped = f"{type(exc).__name__}: {exc}"
    finished = h.pump_until(
        lambda: not dlg._restore_in_progress and dlg._restore_runner is None,
        timeout)
    # The feedback of a run is deferred (minimum animation time).
    h.pump(2.3)
    return escaped, finished


def _dialog_state(dlg):
    return {
        "restore_in_progress": bool(dlg._restore_in_progress),
        "runner": dlg._restore_runner is not None,
        "trace_id": dlg._active_restore_trace_id,
        "restore_button": bool(dlg.restore_button.isEnabled()),
        "recover_button": bool(dlg.recover_button.isEnabled()),
        "progress_visible": bool(dlg.progress_bar.isVisible()),
    }


class _PanelRecorder:
    """What reaches the RecoverLand tab of the QGIS message log panel.

    ``QgsMessageLog.messageReceived`` is not emitted in this headless
    runtime (measured by tx_journal_lock_no_retry): every user-facing
    ``qlog`` is recorded at the call it goes through, and forwarded.
    """

    def __init__(self, real):
        self.real = real
        self.messages = []

    def logMessage(self, message, tag="", level=None, *args, **kwargs):  # noqa: N802
        if tag == "RecoverLand":
            self.messages.append(
                (str(message), int(getattr(level, "value", level) or 0)))
        return self.real.logMessage(message, tag, level, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self.real, name)


def run(ctx):
    import recoverland.core.logger as logger_mod
    import recoverland.recover_dialog as rd
    from recoverland.core.identity import compute_datasource_fingerprint
    from recoverland.core.logger import flog
    h = _h()
    d = ctx.data
    iface, plugin = h.start_plugin(d["tmpdir"])
    real_log = logger_mod.QgsMessageLog
    panel = _PanelRecorder(real_log)
    logger_mod.QgsMessageLog = panel
    logs = panel.messages
    saved = {n: getattr(rd, n) for n in ("RestorePreflightDialog",
                                         "JournalMaintenanceDialog")}
    try:
        layer = h.load_layer(d["gpkg"], "cadastre")
        h.pump(0.2)
        jpath = h.journal_path(plugin)
        rows0 = h.journal_rows(jpath)
        commits = [
            h.user_commit(layer, lambda lyr: h.rename(lyr, "B", "b")),
            h.user_commit(layer, lambda lyr: h.rename(lyr, "B", "c")),
            h.user_commit(layer, lambda lyr: h.rename(lyr, "D", "d1")),
            h.user_commit(layer, lambda lyr: h.move(lyr, "C", 652500.0, 6862500.0)),
        ]
        d["commits"] = commits
        d["journal_rows"] = h.wait_rows(jpath, rows0 + 4) - rows0
        fp = compute_datasource_fingerprint(layer)
        dlg = h.open_dialog(plugin, [layer])
        events = _lightweight_rows(jpath, fp)
        d["rows"] = [[e.event_id, e.operation_type, e.created_at] for e in events]
        dlg._search_events = events
        by_id = {e.event_id: i for i, e in enumerate(events)}
        ids = sorted(by_id)  # capture order: E1 (a->b), E2, E3 (D), E4 (C)
        d["ids"] = ids
        rd.RestorePreflightDialog = _Preflight

        # ---- P: maintenance purge after the search, then Restore -------
        if len(ids) == 4:
            e1, e2, e3, _e4 = ids
            d["p_before"] = _state(layer)
            rd.JournalMaintenanceDialog = _maintenance_class(
                plugin._journal, cap=len(_event_ids(jpath)) - 1)
            dlg._open_maintenance()
            d["p_purged"] = list(rd.JournalMaintenanceDialog.purged)
            d["p_journal_after_purge"] = _event_ids(jpath)
            n_logs = len(logs)
            _Preflight.shown.clear()
            traces0 = _traces(jpath)
            escaped, finished = _restore_click(
                h, dlg, [by_id[e1], by_id[e2], by_id[e3]])
            d["p"] = {
                "escaped": escaped, "finished": finished,
                "preflight": list(_Preflight.shown),
                "state": _state(layer),
                "traces": _traces(jpath) - traces0,
                "dialog": _dialog_state(dlg),
                "logs": logs[n_logs:],
                "missing_id": e1,
            }

            # ---- N: the journal can no longer be opened for reading ----
            e4 = ids[3]
            d["n_before"] = _state(layer)
            journal = plugin._journal
            real_open = journal.create_read_connection

            def _unreadable(*_a, **_k):
                raise sqlite3.OperationalError("unable to open database file")

            dlg._close_dialog_read_conn()
            journal.create_read_connection = _unreadable
            n_logs = len(logs)
            _Preflight.shown.clear()
            traces0 = _traces(jpath)
            handed = []
            real_runner = rd.RestoreRunner

            def _recording_runner(events_arg, *a, **k):
                handed.extend(
                    [e.event_id, bytes(e.geometry_wkb or b"")[:2].hex()]
                    for e in events_arg)
                return real_runner(events_arg, *a, **k)

            rd.RestoreRunner = _recording_runner
            try:
                escaped, finished = _restore_click(h, dlg, [by_id[e4]])
            finally:
                rd.RestoreRunner = real_runner
                journal.create_read_connection = real_open
            d["n"] = {
                "escaped": escaped, "finished": finished,
                "handed": handed,
                "state": _state(layer),
                "traces": _traces(jpath) - traces0,
                "dialog": _dialog_state(dlg),
                "logs": logs[n_logs:],
            }

            # ---- F: the function itself --------------------------------
            conn = sqlite3.connect(jpath)
            try:
                from recoverland.core.event_stream_repository import (
                    fetch_events_by_ids,
                )
                got = fetch_events_by_ids(conn, [e1, e2, e3])
                d["f_lenient"] = sorted(e.event_id for e in got)
                try:
                    strict = fetch_events_by_ids(conn, [e1, e2, e3],
                                                 require_all=True)
                    d["f_strict"] = {"returned": sorted(e.event_id for e in strict)}
                except Exception as exc:  # noqa: BLE001 - the refusal, or a TypeError
                    d["f_strict"] = {
                        "type": type(exc).__name__, "text": str(exc)[:300],
                        "missing": list(getattr(exc, "missing_ids", ()) or ()),
                    }
            finally:
                conn.close()
            d["f_missing_id"] = e1
        dlg.close()
    except Exception as exc:  # noqa: BLE001 - a crash is a FAIL, reported
        import traceback
        d["crash"] = f"{type(exc).__name__}: {exc}"
        d["trace"] = traceback.format_exc()[-1500:]
    finally:
        for name, value in saved.items():
            setattr(rd, name, value)
        logger_mod.QgsMessageLog = real_log
        h.shutdown(plugin)
    flog(f"rw_restore_selection_purged run: trace_id={ctx.trace_id} "
         f"data={json.dumps({k: v for k, v in d.items() if k != 'trace'}, default=str)[:4000]}",
         "INFO")


def teardown(ctx):
    shutil.rmtree(ctx.data.get("tmpdir", ""), ignore_errors=True)


def _released(dialog) -> bool:
    return (dialog.get("restore_in_progress") is False
            and dialog.get("runner") is False
            and dialog.get("trace_id") == ""
            and dialog.get("restore_button") is True
            and dialog.get("recover_button") is True
            and dialog.get("progress_visible") is False)


def assertions(ctx):
    from scripts.validation.assert_log import assert_log_contains
    d = ctx.data
    crash = f" crash={d.get('crash')} {d.get('trace', '')[-600:]}" if d.get("crash") else ""
    out = [("no_crash", "crash" not in d, crash)]
    ids = d.get("ids") or []
    out.append((
        "setup_four_edits_captured_and_one_purged",
        d.get("commits") == [True] * 4 and len(ids) == 4
        and d.get("p_purged") == [1]
        and ids[0] not in (d.get("p_journal_after_purge") or [])
        and all(i in (d.get("p_journal_after_purge") or []) for i in ids[1:]),
        f"commits={d.get('commits')} lignes={d.get('rows')} "
        f"purge={d.get('p_purged')} journal apres purge="
        f"{d.get('p_journal_after_purge')} (attendu : seule la plus ancienne "
        f"edition, 'a' -> 'b', purgee){crash}",
    ))

    p = d.get("p") or {}
    before = d.get("p_before") or {}
    state = p.get("state") or {}
    out.append((
        "p_nothing_written_when_a_selected_event_is_gone",
        bool(state) and state.get("B") == before.get("B")
        and state.get("D") == before.get("D") and p.get("traces") == 0,
        f"avant={before} apres={state} traces={p.get('traces')} "
        f"preflight={p.get('preflight')} : l'utilisateur a confirme la "
        f"restauration de 3 evenements ; en executer 2 ecrit B = 'b', un "
        f"etat qu'il n'a ni demande ni vu annoncer, et D = 'd0'{crash}",
    ))
    missing = p.get("missing_id")
    named = [m for m, level in p.get("logs") or []
             if level == _CRITICAL and missing is not None
             and f"#{missing}" in m and "Rien n'a ete ecrit" in m]
    out.append((
        "p_missing_event_named_to_the_user",
        len(named) == 1,
        f"messages={p.get('logs')} : l'evenement #{missing}, choisi et "
        f"confirme puis purge du journal, doit etre nomme par son numero, "
        f"avec la raison et le fait que rien n'a ete ecrit{crash}",
    ))
    out.append((
        "p_dialog_released",
        p.get("escaped") is None and _released(p.get("dialog") or {}),
        f"exception={p.get('escaped')!r} etat={p.get('dialog')} : un refus "
        f"doit rendre le dialogue (sinon toute action suivante est refusee "
        f"'une restauration est en cours'){crash}",
    ))

    n = ctx.data.get("n") or {}
    nb = d.get("n_before") or {}
    ns = n.get("state") or {}
    marker = [e for e in n.get("handed") or [] if e[1] == "00"]
    out.append((
        "n_no_lightweight_row_handed_to_the_runner",
        not marker and bool(ns) and ns.get("C") == nb.get("C")
        and n.get("traces") == 0,
        f"remis au runner={n.get('handed')} avant={nb.get('C')} "
        f"apres={ns.get('C')} traces={n.get('traces')} : sans relecture, le "
        f"runner recevait la ligne legere du tableau (geometrie = marqueur "
        f"d'un octet) au lieu de l'evenement journalise{crash}",
    ))
    named_n = [m for m, level in n.get("logs") or []
               if level == _CRITICAL and "Rien n'a ete ecrit" in m
               and "relus" in m]
    out.append((
        "n_reason_named_and_dialog_released",
        len(named_n) == 1 and n.get("escaped") is None
        and _released(n.get("dialog") or {}),
        f"messages={n.get('logs')} exception={n.get('escaped')!r} "
        f"etat={n.get('dialog')}{crash}",
    ))

    f_missing = d.get("f_missing_id")
    strict = d.get("f_strict") or {}
    out.append((
        "f_require_all_refuses_by_number",
        strict.get("type") == "EventsNotInJournal"
        and strict.get("missing") == [f_missing]
        and f"{f_missing}" in (strict.get("text") or ""),
        f"fetch_events_by_ids(require_all=True) -> {strict} : l'evenement "
        f"{f_missing} absent du journal doit etre refuse par son numero",
    ))
    out.append((
        "f_lenient_read_returns_what_exists",
        d.get("f_lenient") == sorted(ids[1:3]),
        f"fetch_events_by_ids sans require_all -> {d.get('f_lenient')} "
        f"(attendu {sorted(ids[1:3])}) : le mode par defaut rend ce qui "
        f"existe, et le nomme au journal (assertion suivante)",
    ))
    out.append(assert_log_contains(
        ctx.records,
        rf"fetch_events_by_ids: .*not in the journal.*ids \[{f_missing}\] "
        r"require_all=False",
        name="f_missing_ids_logged", min_count=1))
    out.append(assert_log_contains(
        ctx.records, rf"rw_restore_selection_purged.*trace_id={ctx.trace_id}",
        name="trace_id_propagated", min_count=2))
    return out


if __name__ == "__main__":
    import sys
    for extra in (str(_PLUGIN_ROOT), str(_PLUGIN_ROOT.parent)):
        if extra not in sys.path:
            sys.path.insert(0, extra)
    from scripts.validation.runner import run_scenario
    run_scenario(__file__)
