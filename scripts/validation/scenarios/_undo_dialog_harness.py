"""Harness partage des scenarios d'orchestration Undo / Rewind du dialogue.

Ce n'est pas un scenario (le tiret bas initial le fait ignorer par le
runner headless). Il sert a `rw_event_restore_undo_scope`,
`rw_undo_trace_invalidation_truth`, `rw_autoundo_layer_scope`,
`rw_journal_barrier`, `rw_project_switch_quiesce`, `rw_undo_last_run_scope`,
`rw_restore_landed_write_scope`, `rw_undo_keeps_later_edit`,
`rw_rewind_no_autoundo`, `rw_undo_button_run_guard`, `rw_undo_order_journal`,
`rw_undo_session_compensates`, `rw_dialog_scope_misc` et
`rw_multi_hop_navigation`.

`real_fetch_class` lit la VRAIE fenetre du journal : un fetch scripte
repondait ce que le scenario croyait, pas ce que le coeur v7 renvoie (c'est
ce qui a cache la re-compensation apres l'auto-undo, revue brev2).

Pourquoi un greffon et pas le vrai dialogue
-------------------------------------------
`RecoverDialog` ne se construit pas sans toute son interface, et une boite
modale fige un run headless. Les scenarios greffent donc les VRAIES
methodes, non modifiees, de `RecoverDialog` sur `DialogStub`, un objet
ordinaire qui ne porte que des doublures de widgets. Ce qui s'execute est
le code d'orchestration livre : les runners sont les vraies classes de
`restore_runner`, le journal est un vrai fichier SQLite, et seules les
coutures au niveau du fournisseur (`restore_batch`, `undo_prepare`,
`undo_apply_one`), la resolution de couche QGIS, les boites de dialogue et
le thread de lecture sont scriptes.
"""
from __future__ import annotations

import inspect
import json
import os
import sqlite3
import time
import types
from datetime import datetime, timedelta, timezone

T0 = datetime(2026, 9, 1, 8, 0, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------
# Signaux et widgets de substitution
# --------------------------------------------------------------------------
class Signal:
    """Signal synchrone minimal (connect / disconnect / emit)."""

    def __init__(self):
        self._slots = []

    def connect(self, slot):
        self._slots.append(slot)

    def disconnect(self, slot=None):
        if slot is None:
            self._slots = []
        else:
            self._slots = [s for s in self._slots if s != slot]

    def emit(self, *args):
        for slot in list(self._slots):
            slot(*args)


class Widget:
    """Doublure permissive de tout widget que l'orchestration touche."""

    def __init__(self, name=""):
        self.name = name
        self.enabled = True
        self.visible = False
        self.text = ""

    def setEnabled(self, value):  # noqa: N802
        self.enabled = bool(value)

    def isEnabled(self):  # noqa: N802
        return self.enabled

    def setVisible(self, value):  # noqa: N802
        self.visible = bool(value)

    def isVisible(self):  # noqa: N802
        return self.visible

    def setText(self, value):  # noqa: N802
        self.text = str(value)

    def isChecked(self):  # noqa: N802
        return False

    def isActive(self):  # noqa: N802
        return False

    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(name)
        return lambda *args, **kwargs: None


class Canvas:
    def __init__(self):
        self.refreshes = 0

    def freeze(self, _active):
        return None

    def refresh(self):
        self.refreshes += 1

    def refreshAllLayers(self):  # noqa: N802
        self.refreshes += 1


class Iface:
    def __init__(self):
        self._canvas = Canvas()

    def mapCanvas(self):  # noqa: N802
        return self._canvas

    def showAttributeTable(self, _layer):  # noqa: N802
        return None


class TimeSlider:
    def __init__(self, cutoff_dt: datetime):
        self.cutoff_dt = cutoff_dt
        self.event_count = None

    def cutoff_datetime(self):
        from qgis.PyQt.QtCore import QDateTime
        return QDateTime.fromMSecsSinceEpoch(
            int(self.cutoff_dt.timestamp() * 1000))

    def set_event_count(self, n):
        self.event_count = n


# --------------------------------------------------------------------------
# Journal SQLite reel
# --------------------------------------------------------------------------
class _LockingConnection:
    """Connexion d'ecriture dont chaque UPDATE echoue comme sous verrou."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, *args):
        if sql.lstrip().upper().startswith("UPDATE"):
            raise sqlite3.OperationalError("database is locked")
        return self._conn.execute(sql, *args)

    def executemany(self, sql, *args):
        if sql.lstrip().upper().startswith("UPDATE"):
            raise sqlite3.OperationalError("database is locked")
        return self._conn.executemany(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class Journal:
    """Le strict necessaire de JournalManager, sur un vrai fichier SQLite."""

    def __init__(self, path: str):
        self.path = path
        self.is_open = True
        self.is_reconciliation_degraded = False
        self.lock_writes = False
        self.write_connections = 0

    def reconciliation_warning(self):
        return ""

    def create_read_connection(self):
        return sqlite3.connect(self.path)

    def create_write_connection(self):
        self.write_connections += 1
        conn = sqlite3.connect(self.path)
        if self.lock_writes:
            return _LockingConnection(conn)
        return conn


def new_journal(directory: str, name: str = "journal.sqlite") -> str:
    from recoverland.core.sqlite_schema import initialize_schema
    path = os.path.join(directory, name)
    conn = sqlite3.connect(path)
    initialize_schema(conn)
    conn.commit()
    conn.close()
    return path


def insert_event(path, ds_fp, layer_name, op, seconds, *, fid=1,
                 attrs=None, restored_from=None, session_id=None, schema=2):
    """Insere un evenement (utilisateur, ou trace si restored_from).

    `schema` : event_schema_version de la ligne. 2 par defaut (trace
    heritee, comme les scenarios d'origine) ; 7 pour une trace exacte, celle
    qu'ecrit un Rewind v7.
    """
    from recoverland.core.sqlite_schema import (
        AUDIT_EVENT_INSERT_SQL, AUDIT_EVENT_INSERT_PLACEHOLDERS,
    )
    if attrs is None:
        attrs = ({"_restore_ref": restored_from} if restored_from
                 else {"changed_only": {"v": {"old": "a", "new": "b"}}})
    ts = (T0 + timedelta(seconds=seconds)).isoformat()
    sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
           + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
    conn = sqlite3.connect(path)
    try:
        cur = conn.execute(sql, (
            "proj_undo_dialog", ds_fp, f"lyr_{layer_name}", layer_name, "ogr",
            json.dumps({"fid": fid}), op, json.dumps(attrs),
            None, "NoGeometry", "EPSG:2154", None,
            "tester", session_id,
            ts, restored_from,
            f"fid:{fid}", int(schema), None, None,
        ))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def fetch_events(path, ids):
    """Vrais AuditEvent relus du journal, dans l'ordre des ids demandes."""
    from recoverland.core.event_stream_repository import fetch_events_by_ids
    conn = sqlite3.connect(path)
    try:
        by_id = {e.event_id: e for e in fetch_events_by_ids(conn, list(ids))}
    finally:
        conn.close()
    return [by_id[i] for i in ids]


def trace_state(path, source_ids):
    """{source_eid: 'active' | 'invalidated' | 'none'} pour chaque id."""
    conn = sqlite3.connect(path)
    try:
        out = {}
        for sid in source_ids:
            rows = conn.execute(
                "SELECT invalidated_at FROM audit_event "
                "WHERE restored_from_event_id = ?", (sid,)).fetchall()
            if not rows:
                out[sid] = "none"
            elif any(r[0] is None for r in rows):
                out[sid] = "active"
            else:
                out[sid] = "invalidated"
        return out
    finally:
        conn.close()


# --------------------------------------------------------------------------
# File d'ecriture, couches, thread de lecture, runner de rewind
# --------------------------------------------------------------------------
class RecordingWriteQueue:
    """File d'ecriture enregistreuse : barriere et backlog pilotables.

    Tant qu'elle n'est pas arretee, elle ecrit vraiment les traces recues
    dans le journal SQLite, comme le ferait l'ecrivain : l'undo qui suit
    trouve donc les traces a invalider la ou le produit les chercherait.
    """

    def __init__(self, journal_path: str):
        self.journal_path = journal_path
        self.drained = True
        self.outbox_outstanding = 0
        self.barrier_calls = []
        self.enqueued = []   # (restored_from ids, journal_path, stopped)
        self.stopped = False
        self.pending_count = 0

    def wait_until_drained(self, timeout: float = 10.0) -> bool:
        self.barrier_calls.append(timeout)
        if not self.drained and not self.pending_count:
            self.pending_count = 3
        return self.drained

    def _persist(self, events):
        from recoverland.core.sqlite_schema import (
            AUDIT_EVENT_INSERT_COLUMNS, AUDIT_EVENT_INSERT_SQL,
            AUDIT_EVENT_INSERT_PLACEHOLDERS,
        )
        sql = ("INSERT INTO audit_event (" + AUDIT_EVENT_INSERT_SQL
               + ") VALUES (" + AUDIT_EVENT_INSERT_PLACEHOLDERS + ")")
        conn = sqlite3.connect(self.journal_path)
        try:
            for e in events:
                conn.execute(sql, [getattr(e, col, None)
                                   for col in AUDIT_EVENT_INSERT_COLUMNS])
            conn.commit()
        finally:
            conn.close()

    def health(self):
        return types.SimpleNamespace(
            outbox_outstanding=self.outbox_outstanding,
            open_prepares=0,
            queued_events=self.pending_count,
            writer_alive=not self.stopped,
            state="stopped" if self.stopped else "running",
            journal_path=self.journal_path,
        )

    def enqueue(self, events, commit=None, journal_path=None):
        events = list(events)
        ids = [getattr(e, "restored_from_event_id", None) for e in events]
        self.enqueued.append((ids, journal_path, self.stopped))
        if not self.stopped:
            self._persist(events)
        return True

    def stop(self):
        self.stopped = True


class FakeProvider:
    def name(self):
        return "ogr"

    def reloadData(self):  # noqa: N802
        return None

    def capabilities(self):
        return 0


class FakeLayer:
    def __init__(self, layer_id: str, name: str, count: int = 10):
        self._id = layer_id
        self._name = name
        self.count = count

    def id(self):
        return self._id

    def name(self):
        return self._name

    def isValid(self):  # noqa: N802
        return True

    def isEditable(self):  # noqa: N802
        return False

    def isModified(self):  # noqa: N802
        return False

    def featureCount(self):  # noqa: N802
        return self.count

    def reload(self):
        return None

    def triggerRepaint(self):  # noqa: N802
        return None

    def updateExtents(self):  # noqa: N802
        return None

    def dataProvider(self):  # noqa: N802
        return FakeProvider()

    def commitChanges(self):  # noqa: N802
        return True


def fetch_thread_class(responses: list, created: list):
    """VersionFetchThread scripte : chaque start() emet la reponse suivante."""

    class FakeFetchThread:
        def __init__(self, journal, fps, cutoff, trace_id="",
                     include_traces=True):
            self.fps = list(fps)
            self.cutoff = cutoff
            self.count_ready = Signal()
            self.events_ready = Signal()
            self.error_occurred = Signal()
            created.append(self)

        def start(self):
            events = responses.pop(0) if responses else []
            self.count_ready.emit(len(events))
            self.events_ready.emit(list(events))

        def isRunning(self):  # noqa: N802
            return False

        def stop(self):
            return None

        def wait(self, _ms=0):
            return True

    return FakeFetchThread


def real_fetch_class(created: list):
    """VersionFetchThread synchrone qui lit le VRAI journal.

    Meme code de lecture que le thread livre (`version_fetch_thread._run_fetch`,
    donc la vraie fenetre v7 : evenements apres la date, traces, et elements
    de plan de l'historique), execute dans `start()` au lieu d'un QThread.
    Les scenarios qui scriptaient la reponse du fetch ne voyaient pas ce que
    la vraie fenetre renvoie apres un auto-undo (revue brev2, sonde 3).
    """
    from recoverland.version_fetch_thread import _run_fetch

    class RealFetchThread:
        def __init__(self, journal, fps, cutoff, trace_id="",
                     include_traces=True):
            self.journal = journal
            self.fps = list(fps)
            self.cutoff = cutoff
            self.trace_id = trace_id
            self.include_traces = include_traces
            self.count_ready = Signal()
            self.events_ready = Signal()
            self.error_occurred = Signal()
            self.result = None
            created.append(self)

        def start(self):
            events = _run_fetch(self.journal, self.fps, self.cutoff,
                                self.trace_id, self.count_ready.emit,
                                lambda: False,
                                include_traces=self.include_traces)
            self.result = list(events or [])
            self.events_ready.emit(list(events or []))

        def isRunning(self):  # noqa: N802
            return False

        def stop(self):
            return None

        def wait(self, _ms=0):
            return True

    return RealFetchThread


def plan_marks(events) -> list:
    """(kind, event_id) de chaque ligne : 'user' ou le type d'element de plan."""
    out = []
    for e in events or ():
        mark = getattr(e, "commit_id", None) or ""
        kind = (mark.split(":", 1)[1].split("@")[0]
                if mark.startswith("rewind-plan:") else "user")
        out.append((kind, e.event_id))
    return out


def strict_runner_class(created: list):
    """StrictRestoreRunner enregistreur : applique tout, ne touche a rien."""
    from recoverland.core.workflow_service import GroupedRestoreResult

    class RecordingStrictRunner:
        # Hypothesis revised (v7): the dialog hands every runner the
        # journal it planned from (`journal_path`, read by the coverage
        # guard and the identity proofs). The double takes it like the real
        # constructor and keeps it for the scenarios that check it.
        def __init__(self, events, resolver, cutoff, write_queue=None,
                     tracker=None, trace_id="", parent=None,
                     extra_stats=None, journal_path=None):
            self.journal_path = journal_path
            self.events = list(events)
            self.progress = Signal()
            self.layer_started = Signal()
            self.finished = Signal()
            self._cancelled = False
            self._total_ok = 0
            self._by_ds = {}
            created.append(self)

        def cancel(self):
            self._cancelled = True

        def start(self):
            by_ds = {}
            for e in self.events:
                by_ds.setdefault(e.datasource_fingerprint, []).append(e)
            self._total_ok = len(self.events)
            self.finished.emit(GroupedRestoreResult(
                total_ok=len(self.events), total_fail=0, errors=[],
                by_ds=by_ds, trace_events=[], applied=len(self.events)))

    return RecordingStrictRunner


def recording_runner_class(created: list):
    """Runner inerte : prouve seulement qu'un runner a ete (ou non) cree."""

    class RecordingRunner:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.progress = Signal()
            self.layer_started = Signal()
            self.finished = Signal()
            self._cancelled = False
            self._total_ok = 0
            self._by_ds = {}
            created.append(self)

        def cancel(self):
            self._cancelled = True

        def start(self):
            return None

    return RecordingRunner


class PreflightDialog:
    """RestorePreflightDialog accepte sans boite modale."""

    shown = []

    def __init__(self, _parent=None):
        PreflightDialog.shown.append(self)

    def set_preflight_data(self, *args, **kwargs):
        return None

    def exec(self):
        from qgis.PyQt.QtWidgets import QDialog
        return QDialog.DialogCode.Accepted


def message_box_class(rd, asked: list, answer_yes: bool = True):
    class FakeMessageBox:
        @staticmethod
        def question(_parent, title, text, *args, **kwargs):
            asked.append((str(title), str(text)))
            return rd.QtCompat.MSG_YES if answer_yes else rd.QtCompat.MSG_NO

        @staticmethod
        def warning(_parent, title, text, *args, **kwargs):
            asked.append((str(title), str(text)))

        information = warning
        critical = warning

    return FakeMessageBox


# --------------------------------------------------------------------------
# Coutures du fournisseur
# --------------------------------------------------------------------------
def trace_for(event):
    inverse = {"DELETE": "INSERT", "INSERT": "DELETE"}.get(
        event.operation_type, "UPDATE")
    return event._replace(
        event_id=None, restored_from_event_id=event.event_id,
        operation_type=inverse,
        created_at=datetime.now(timezone.utc).isoformat())


def scripted_restore_batch(outcomes: dict, calls: list, on_call=None):
    """restore_batch scripte, fidele au contrat du vrai.

    Comme le vrai `restore_batch`, il emet une trace pour CHAQUE succes, y
    compris les sauts idempotents : c'est ce que le runner doit filtrer.
    """
    from recoverland.core.audit_backend import RestoreReport

    # Hypothesis revised (v7): the runners hand the executor the journal read
    # connection of the layer group (`journal=`, the published signature);
    # the doubles take it like the real functions, nothing else changes.
    def restore_batch(layer, events, fid_cache=None, trace_id="",
                      journal=None):
        if on_call is not None:
            on_call(len(calls))
        succeeded, failed, traces = [], {}, []
        skipped, absent, drift = [], [], []
        for event in events:
            eid = event.event_id
            calls.append(eid)
            kind = outcomes.get(eid, "applied")
            if kind == "applied":
                succeeded.append(eid)
            elif kind == "skipped":
                succeeded.append(eid)
                skipped.append(eid)
            elif kind == "target_absent":
                failed[eid] = ("Target feature absent or identity mismatch "
                               "(scripted): FID now holds another feature")
                absent.append(eid)
            elif kind == "drift":
                failed[eid] = "Geometry drift beyond tolerance (scripted)"
                drift.append(eid)
            else:
                failed[eid] = "Write refused (scripted)"
            if eid in succeeded:
                traces.append(trace_for(event))
        return RestoreReport(
            succeeded=succeeded, failed=failed,
            total_requested=len(events), trace_events=tuple(traces),
            skipped_idempotent=skipped, failed_target_absent=absent,
            failed_geometry_drift=drift)

    return restore_batch


def scripted_undo(outcomes: dict, calls: list, on_apply=None):
    """(undo_prepare, undo_apply_one) scriptes ; enregistre chaque inverse."""
    from recoverland.core.restore_service import UndoBatchContext

    def undo_prepare(layer, events, journal=None):
        return UndoBatchContext(ordered=list(reversed(events)), fid_cache={},
                                layer_error=None)

    def undo_apply_one(layer, event, context):
        eid = event.event_id
        calls.append((layer.name(), eid))
        if on_apply is not None:
            on_apply(eid)
        kind = outcomes.get(eid, "undone")
        if kind == "failed":
            return {"success": False,
                    "message": "Delete failed (scripted provider refusal)"}
        if kind == "skipped":
            return {"success": True, "skipped": True,
                    "message": "Already absent (no snapshot match)"}
        return {"success": True, "message": "Undo reverted"}

    return undo_prepare, undo_apply_one


class Seams:
    """Pose puis retire toutes les coutures, quoi qu'il arrive."""

    def __init__(self):
        self._saved = []

    def set(self, module, name, value):
        self._saved.append((module, name, getattr(module, name)))
        setattr(module, name, value)

    def restore(self):
        for module, name, value in reversed(self._saved):
            setattr(module, name, value)
        self._saved = []


def pump_until(predicate, timeout_s: float = 10.0) -> bool:
    from qgis.PyQt.QtCore import QCoreApplication
    deadline = time.monotonic() + timeout_s
    while not predicate():
        QCoreApplication.processEvents()
        if time.monotonic() > deadline:
            return False
        time.sleep(0.002)
    return True


# --------------------------------------------------------------------------
# Le dialogue greffe
# --------------------------------------------------------------------------
def dialog_class(rd):
    """Classe portant les methodes reelles de RecoverDialog.

    Seules les methodes d'affichage pur sont redefinies ici ; tout le reste
    (orchestration, scopes, invalidation, barriere) est greffe tel quel.
    C'est un QObject : les runners sont crees avec ``parent=self``.
    """
    from qgis.PyQt.QtCore import QObject

    class DialogStub(QObject):
        def tr(self, text):
            return text

        def update_phase(self, text):
            self.phases.append(str(text))

        def _start_logo_activity(self, *_args, **_kwargs):
            return None

        def _stop_logo_activity(self):
            return None

        def _freeze_canvas_for_write(self, active):
            self.frozen = bool(active)

        def _smooth_set_progress(self, value):
            self.progress = value

        def _get_version_checked_fingerprints(self):
            return list(self.checked_fps)

        def _refresh_layers_panel(self):
            return None

        def _refresh_journal_status(self):
            return None

        def _open_attribute_tables_if_requested(self, _by_ds, _source):
            return None

        def _preflight_layer_checks(self, _events):
            return [], []

        def _ambiguous_datasource_names(self):
            return []

        def reject(self):
            self.rejected = True

        def isVisible(self):  # noqa: N802
            return False

    for name, member in vars(rd.RecoverDialog).items():
        if name.startswith("__") or name in vars(DialogStub):
            continue
        if inspect.isfunction(member) or isinstance(
                member, (staticmethod, classmethod)):
            setattr(DialogStub, name, member)
    return DialogStub


def build_dialog(rd, journal, write_queue, checked_fps, cutoff_dt=None):
    dlg = dialog_class(rd)()
    dlg.iface = Iface()
    dlg._journal = journal
    dlg._tracker = None
    dlg._write_queue = write_queue
    dlg.checked_fps = list(checked_fps)
    dlg.phases = []
    for widget in ("progress_bar", "recover_button", "undo_last_btn",
                   "restore_button", "progress_phase_label", "progress_timer",
                   "cancel_button", "layer_input", "operation_input",
                   "start_input", "end_input", "open_attribute_check",
                   "logo_label", "table_widget", "restore_mode_selector"):
        setattr(dlg, widget, Widget(widget))
    dlg.time_slider = TimeSlider(cutoff_dt or T0)
    dlg._glow_color = None
    dlg._is_recovering = False
    dlg._restore_in_progress = False
    dlg._restore_runner = None
    dlg._cancelled_restore_runner = None
    dlg._last_restore_events = None
    dlg._last_restore_by_ds = None
    dlg._last_run_ids = set()
    dlg._pending_cycle_stats = None
    dlg._state_mismatches = []
    dlg._active_search_trace_id = ""
    dlg._active_restore_trace_id = ""
    dlg._dialog_read_conn = None
    dlg._reconciliation_notified = set()
    dlg._version_fetch_thread = None
    dlg._version_restore_cutoff = None
    dlg._version_fetch_total = 0
    dlg._restore_started_at = 0.0
    dlg._recover_started_at = 0.0
    dlg._pending_restore_feedback = None
    dlg._pending_search_result = None
    dlg._event_restore_events = None
    dlg._version_restore_events = None
    dlg._undo_counts_before = {}
    dlg._restore_counts_before = {}
    dlg._canvas_frozen_by_run = False
    dlg.worker_thread = None
    dlg.selected_rows = []
    dlg._search_events = []
    return dlg


def close_dialog(dlg):
    # A stub left connected to the project signals would be woken by the
    # next scenario that clears the project.
    if getattr(dlg, "_project_clear_guard_armed", False):
        from qgis.core import QgsProject
        try:
            QgsProject.instance().aboutToBeCleared.disconnect(
                dlg._on_project_about_to_clear)
        except (TypeError, RuntimeError):
            pass
        dlg._project_clear_guard_armed = False
    conn = getattr(dlg, "_dialog_read_conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:
            pass
        dlg._dialog_read_conn = None


def cutoff(iso_dt: datetime):
    from recoverland.core.restore_contracts import RestoreCutoff, CutoffType
    return RestoreCutoff(CutoffType.BY_DATE,
                         iso_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                         inclusive=True)


def by_ds_ids(by_ds) -> dict:
    return {fp: sorted(e.event_id for e in evts)
            for fp, evts in (by_ds or {}).items()}


def all_ids(by_ds) -> set:
    return {e.event_id for evts in (by_ds or {}).values() for e in evts}
