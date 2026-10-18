"""Edit session tracker for RecoverLand (RLU-020 to RLU-026).

Listens to QGIS editing signals on all editable layers.
Captures initial feature state before commit, generates audit events
after successful commit, discards on rollback.

All QGIS object access happens on the main thread.
Serialized data is passed to the write queue for async persistence.
"""
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional, List

from qgis.PyQt.QtCore import QObject, pyqtSignal, QTimer

from .audit_backend import AuditEvent
from .edit_buffer import (
    EditSessionBuffer, FeatureSnapshot, create_snapshot_from_feature,
)
from .identity import (
    compute_datasource_fingerprint, compute_feature_identity,
    compute_project_fingerprint, extract_layer_name,
    compute_entity_fingerprint, canonicalize_fingerprint,
    datasource_fingerprints_match, layer_is_selected,
)
from .sqlite_schema import CURRENT_SCHEMA_VERSION
from .geometry_utils import geometries_equal, geometry_to_wkb, wkb_short_repr
from .serialization import (
    serialize_attributes, serialize_field_schema,
    compute_update_delta, build_full_snapshot,
)
from .support_policy import (
    evaluate_layer_support, SupportLevel, IdentityStrength,
)
from .user_identity import resolve_user_name
from .logger import flog


# Sentinel returned by dict.get to distinguish "fid not in committed_geom_changes"
# (provider did not change geometry) from "fid present with explicit None geom"
# (geometry was cleared by the commit).
_SENTINEL = object()


class _CommitSignalBridge(QObject):
    """QObject bridge for Qt signals from EditSessionTracker.

    EditSessionTracker remains a pure Python class (no Qt dependency in its core logic).
    This bridge is owned by the tracker and lives on the main thread (created in __init__).
    It emits committed_features(n_events) when the WriteQueue flush callback fires.
    """
    committed_features = pyqtSignal(int)

    def __init__(self, parent=None):
        super().__init__(parent)


class _CommitAttempt:
    """One QGIS commit attempt on one layer, from beforeCommitChanges to its end.

    QgsVectorLayer.commitChanges() is not a transaction: the edit buffer
    pushes geometries, attributes, deletions and additions to the provider
    one stage at a time, and every stage that succeeds is WRITTEN before a
    later stage fails (measured on QGIS 4.2.1: a UNIQUE violation at the
    additions stage leaves the geometry, the attribute and the deletion in
    the file, commitChanges() returns False and afterCommitChanges never
    comes). An attempt therefore always ends in one explicit state:

    COMMITTED   afterCommitChanges fired: the whole buffer reached the provider.
    PARTIAL     no afterCommitChanges, but committed* signals prove some
                stages were written: those, and only those, are journalled.
    NO_WRITE    no afterCommitChanges and no committed* signal: nothing
                reached the provider, the pre-commit capture is discarded.
    GENERATION_FAILED  the events could not be built; the pre-commit capture
                is kept in the journal ledger instead of being lost.

    It carries everything event generation needs, captured while the layer
    was alive, so an attempt can still be finalised after the layer is gone.
    """

    __slots__ = ("commit_id", "layer_id", "session_id", "buf", "meta",
                 "prepared_at", "prepared_durable", "finalized",
                 "got_committed_signal", "state")

    def __init__(self, commit_id, layer_id, session_id, buf, meta, prepared_at):
        self.commit_id = commit_id
        self.layer_id = layer_id
        self.session_id = session_id
        self.buf = buf
        self.meta = meta
        self.prepared_at = prepared_at
        self.prepared_durable = False
        self.finalized = False
        self.got_committed_signal = False
        self.state = "PREPARED"


class EditSessionTracker:
    """Tracks editing sessions across all layers in a QGIS project.

    Lifecycle: created when plugin starts, disposed when plugin unloads.
    One tracker instance per plugin lifetime.

    Commit state machine (per layer), see `_CommitAttempt`::

        editingStarted -> beforeCommitChanges [prepare: pre-edit state made
        durable in the outbox] -> committed* signals -> afterCommitChanges
        [COMMITTED] | zero-timer after commitChanges() returned, rollback,
        next beforeCommitChanges, disconnect, deactivate [PARTIAL/NO_WRITE]
    """

    _MASS_DELETE_THRESHOLD = 100

    def __init__(self, write_queue, journal_manager):
        self._write_queue = write_queue
        self._journal_manager = journal_manager
        self._buffers: Dict[str, EditSessionBuffer] = {}
        self._connected_layers: Dict[str, object] = {}
        self._layer_fingerprints: Dict[str, str] = {}
        self._signal_connections: Dict[str, List] = {}
        self._active = False
        self._suppress_depth = 0
        # True while capture is paused because a commit blew the buffer
        # memory budget. Cleared on the next editing session.
        self._pressure_paused = False
        self._allowed_layer_fingerprints: set = set()  # empty = track all
        # `_allowed_layer_fingerprints` as the user persisted it, PLUS the
        # canonical form of each entry and every alias the journal knows.
        # See `_is_tracked`.
        self._tracked_scope: set = set()
        self._session_event_count = 0
        self._on_commit_callback = None
        self._on_overflow_callback = None
        self._on_capture_status_callback = None
        self._commit_bridge = _CommitSignalBridge()
        # One open commit attempt per layer (see _CommitAttempt).
        self._attempts: Dict[str, _CommitAttempt] = {}
        # Editing session id per layer, from editingStarted to the next one.
        self._sessions: Dict[str, str] = {}
        # Per-layer suppression (restore runs write on THEIR layers only):
        # layer_id -> depth. The global depth above stays for legacy callers.
        self._suppressed_layers: Dict[str, int] = {}
        # Layers whose current session cannot be captured (automatic
        # transaction groups: the edit buffer writes straight through).
        self._uncapturable_sessions: Dict[str, str] = {}
        self.capture_failures = 0
        self.partial_commits = 0
        self.last_commit_outcome = None

    @property
    def is_active(self) -> bool:
        return self._active

    @property
    def is_suppressed(self) -> bool:
        return self._suppress_depth > 0

    def suppress(self) -> None:
        """Temporarily suppress event capture (e.g. during restore).

        Reentrant: each suppress() must be paired with unsuppress().
        The tracker remains suppressed until all callers have unsuppressed.
        """
        self._suppress_depth += 1
        flog(f"EditSessionTracker: suppressed (depth={self._suppress_depth})")

    def unsuppress(self) -> None:
        """Re-enable event capture after restore.

        Reentrant: only actually unsuppresses when depth reaches 0.
        """
        if self._suppress_depth > 0:
            self._suppress_depth -= 1
        flog(f"EditSessionTracker: unsuppressed (depth={self._suppress_depth})")

    def suppressed(self):
        """Context manager for safe suppress/unsuppress pairing.

        Usage: with tracker.suppressed(): ...
        Guarantees unsuppress even if the body raises.
        """
        from contextlib import contextmanager

        @contextmanager
        def _ctx():
            self.suppress()
            try:
                yield
            finally:
                self.unsuppress()
        return _ctx()

    def force_unsuppress(self) -> None:
        """Force-reset suppress depth to 0 (cleanup/teardown only)."""
        prev = self._suppress_depth
        self._suppress_depth = 0
        layers = dict(self._suppressed_layers)
        self._suppressed_layers.clear()
        self.__dict__.get("_runner_managed_layers", set()).clear()
        self.__dict__.get("_runner_step_layers", {}).clear()
        if prev > 0 or layers:
            flog(f"EditSessionTracker: force_unsuppress (was depth={prev}, "
                 f"layers={layers})")

    def suppress_layers(self, layer_ids) -> None:
        """Suppress capture on THESE layers only (restore/rewind writes).

        Global suppression during a minutes-long, non-modal restore run
        dropped every commit the user made meanwhile on any other layer.
        A restore only needs its own writes kept out of the journal.
        Reentrant per layer; pair with unsuppress_layers.
        """
        for lid in layer_ids or ():
            if lid:
                self._suppressed_layers[lid] = self._suppressed_layers.get(lid, 0) + 1
        flog(f"EditSessionTracker: layers suppressed {sorted(self._suppressed_layers)}")

    def suppress_runner_layer(self, layer_id: str) -> None:
        """Suppress capture on *layer_id* for a chunked runner (only during steps)."""
        if not layer_id:
            return
        self.suppress_layers([layer_id])
        self.__dict__.setdefault("_runner_managed_layers", set()).add(layer_id)

    def unsuppress_layers(self, layer_ids) -> None:
        managed = self.__dict__.get("_runner_managed_layers", set())
        steps = self.__dict__.get("_runner_step_layers", {})
        for lid in layer_ids or ():
            depth = self._suppressed_layers.get(lid, 0) - 1
            if depth > 0:
                self._suppressed_layers[lid] = depth
            else:
                self._suppressed_layers.pop(lid, None)
                managed.discard(lid)
                steps.pop(lid, None)
        flog(f"EditSessionTracker: layers suppressed now {sorted(self._suppressed_layers)}")

    def enter_runner_step(self, layer_id: str) -> None:
        """Mark *layer_id* as actively mutated by a runner on the current stack."""
        if not layer_id:
            return
        steps = self.__dict__.setdefault("_runner_step_layers", {})
        steps[layer_id] = steps.get(layer_id, 0) + 1

    def leave_runner_step(self, layer_id: str) -> None:
        """Release active runner step mark for *layer_id*."""
        if not layer_id:
            return
        steps = self.__dict__.get("_runner_step_layers", {})
        depth = steps.get(layer_id, 0) - 1
        if depth > 0:
            steps[layer_id] = depth
        else:
            steps.pop(layer_id, None)

    def _layer_suppressed(self, layer_id) -> bool:
        if self._suppressed_layers.get(layer_id, 0) <= 0:
            return False
        managed = self.__dict__.get("_runner_managed_layers", set())
        if layer_id in managed:
            steps = self.__dict__.get("_runner_step_layers", {})
            return steps.get(layer_id, 0) > 0
        return True

    @property
    def suppressed_layer_ids(self) -> set:
        return set(self._suppressed_layers)

    @property
    def session_event_count(self) -> int:
        return self._session_event_count

    @property
    def committed_features(self):
        """Qt signal emitted after WriteQueue flush (n_events persisted).

        Returns the bound pyqtSignal from the internal QObject bridge.
        This property exists so existing connect/disconnect code in recover_dialog.py
        works without modification.
        """
        return self._commit_bridge.committed_features

    def set_commit_callback(self, callback) -> None:
        """Set callback(event_count, layer_name, is_mass_delete, delete_count)."""
        self._on_commit_callback = callback

    def set_overflow_callback(self, callback) -> None:
        """Set callback() invoked when a commit's events reached NO durable storage.

        Legacy name. The write queue no longer refuses on volume; this now
        fires only for the outcomes that lose or set aside data
        (REFUSED_NO_DURABLE_PATH, REFUSED_INVALID).
        """
        self._on_overflow_callback = callback

    def set_capture_status_callback(self, callback) -> None:
        """Set callback(kind, detail: dict) for anything but a normal capture.

        kind: "partial_commit", "degraded", "lost", "generation_failed",
        "uncapturable", "prepare_not_durable", "slot_failed",
        "insert_values_unverified", "update_values_unverified". Called on
        the GUI thread.
        """
        self._on_capture_status_callback = callback

    def _notify_status(self, kind: str, **detail) -> None:
        level = "WARNING" if kind == "partial_commit" else "ERROR"
        flog(f"EditSessionTracker: event=capture_status kind={kind} "
             f"detail={detail}", level)
        cb = self._on_capture_status_callback
        if cb is not None:
            try:
                cb(kind, detail)
            except Exception as exc:  # noqa: BLE001 - a notification
                flog(f"EditSessionTracker: capture status callback failed: "
                     f"{exc}", "ERROR")

    def _on_flush_callback(self, n_events: int) -> None:
        """Internal callback from WriteQueue after successful batch write.

        Emits the Qt signal to notify that events have been persisted.
        Race-free by construction: emission happens after persistence,
        and the signal has no shared state (simple notification).
        """
        flog(
            f"EditSessionTracker: flush_callback n_events={n_events} emitting_signal",
            "DEBUG",
        )
        self._commit_bridge.committed_features.emit(n_events)

    def reset_session_count(self) -> None:
        self._session_event_count = 0

    def activate(self) -> None:
        self._active = True
        if hasattr(self._write_queue, 'set_flush_callback'):
            self._write_queue.set_flush_callback(self._on_flush_callback)
            flog("EditSessionTracker: activated flush_callback=registered")
        else:
            flog("EditSessionTracker: activated flush_callback=unavailable", "WARNING")
        for lid, lyr in list(self._connected_layers.items()):
            if _layer_gone(lyr) or self._layer_suppressed(lid):
                continue
            if not self._is_tracked(self._layer_fingerprints.get(lid)):
                continue
            try:
                if hasattr(lyr, "isEditable") and lyr.isEditable():
                    self._check_capturable(lid)
            except RuntimeError:
                pass

    def deactivate(self) -> None:
        # A commit that already wrote to the provider must be journalled
        # before capture goes quiet (disk monitor, user toggle, shutdown).
        self._finalize_all_unfinished("deactivate")
        self._active = False
        flog("EditSessionTracker: deactivated")

    def set_filter(self, layer_fingerprints: set) -> None:
        """Restrict tracking to these datasource fingerprints. Empty set = track all.

        The list comes from `QgsSettings` (`RecoverLand/tracked_layer_fingerprints`),
        written once and reread at every start, so it holds fingerprints
        computed by OLDER rules. Testing membership by strict equality
        after the v6 canonicalisation matched nothing at all and capture
        stopped silently -- the worst failure this plugin can have. The
        set is therefore widened once here (canonical forms + journal
        aliases), every membership test goes through `_is_tracked`, and
        admitting a layer through `admit_layer`, which also knows the layer.
        """
        self._allowed_layer_fingerprints = {
            fp for fp in (layer_fingerprints or set()) if fp
        }
        self._tracked_scope = self._expand_tracked(
            self._allowed_layer_fingerprints)
        for layer_id in list(self._connected_layers.keys()):
            layer_fp = self._layer_fingerprints.get(layer_id)
            layer = self._connected_layers.get(layer_id)
            if layer is not None and _layer_gone(layer):
                # Destroyed without layersRemoved: released by id, never
                # asked anything (admit_layer reads the layer).
                self.disconnect_layer_by_id(layer_id)
                continue
            admitted = (self._is_tracked(layer_fp) if layer is None
                        else self.admit_layer(layer, layer_fp))
            if not admitted:
                if layer is not None:
                    try:
                        self.disconnect_layer(layer)
                    except RuntimeError:
                        pass
        n_candidates = 0
        try:
            from qgis.core import QgsProject, QgsVectorLayer
            for layer in QgsProject.instance().mapLayers().values():
                if isinstance(layer, QgsVectorLayer):
                    n_candidates += 1
                    self.connect_layer(layer)
        except (ImportError, RuntimeError) as exc:
            # Benign during shutdown or in unit test harness without QgsProject.
            flog(f"EditSessionTracker.set_filter: reconnect skipped: {exc}", "DEBUG")
        flog(f"EditSessionTracker: filter set, "
             f"{len(self._allowed_layer_fingerprints)} layer(s) asked, "
             f"{len(self._tracked_scope)} fingerprint(s) accepted "
             f"(canonical forms and journal aliases included), "
             f"{len(self._connected_layers)} of {n_candidates} vector layer(s) "
             f"connected")
        if (self._allowed_layer_fingerprints and n_candidates
                and not self._connected_layers):
            # Mute plugin: the selection matches no layer of this project.
            # Loud on purpose -- capturing nothing looks exactly like having
            # nothing to capture, and the difference only shows up the day
            # the user needs the history back.
            flog(
                f"EditSessionTracker: the tracking selection matches NONE of "
                f"the {n_candidates} vector layer(s) of this project; no edit "
                f"will be captured. Selected fingerprints="
                f"{sorted(fp[-60:] for fp in self._allowed_layer_fingerprints)}",
                "ERROR",
            )

    def _expand_tracked(self, fingerprints: set) -> set:
        """Widen the persisted selection to every equivalent fingerprint."""
        scope = set()
        for fingerprint in fingerprints:
            scope.add(fingerprint)
            try:
                scope.add(canonicalize_fingerprint(fingerprint))
            except Exception as exc:  # noqa: BLE001 - never lose capture
                flog(f"EditSessionTracker: cannot canonicalise a tracked "
                     f"fingerprint ({exc})", "WARNING")
        try:
            conn = self._journal_manager.get_connection()
            from .datasource_alias import expand_fingerprints
            for fingerprint in list(scope):
                scope.update(expand_fingerprints(conn, fingerprint))
        except Exception as exc:  # noqa: BLE001 - journal not open yet
            flog(f"EditSessionTracker: alias expansion of the tracked list "
                 f"skipped ({type(exc).__name__}: {exc})", "DEBUG")
        return {fp for fp in scope if fp}

    def admit_layer(self, layer, layer_fp) -> bool:
        """Decide whether *layer* is in the tracked selection, and remember it.

        The selection persisted in QgsSettings holds whatever fingerprint form
        was current when the user ticked the box. After the v6 canonicalisation
        a DB layer's canonical form no longer equals its stored one, and the
        table token cannot be derived backwards, so no amount of canonicalising
        `layer_fp` recovers the match: it has to be recomputed FROM THE LAYER.

        Resolving it through the journal's alias table was not an option --
        capture would then depend on a row that may not exist (no registry
        entry, an ambiguous source we refuse to guess, a read-only journal, a
        migration that has not run). Capture must never go silent because of a
        missing row.

        When a layer is admitted on its historical form, its canonical
        fingerprint joins the scope, so the per-signal guards downstream --
        which only ever see a fingerprint string -- keep agreeing with this
        decision.

        This is THE admission test: `connect_layer` and a data source change
        both go through it. `_is_tracked` alone (a string) cannot rebuild a
        historical form, so a layer the tracking dialog shows ticked (it asks
        `layer_is_selected` too) was never connected, in silence.
        """
        if self._is_tracked(layer_fp):
            return True
        if not self._selected_from_layer(layer, layer_fp):
            return False
        if layer_fp:
            self._tracked_scope.add(layer_fp)
            flog(f"EditSessionTracker: layer admitted on its historical "
                 f"fingerprint, canonical form added to scope "
                 f"{str(layer_fp)[-60:]}")
        return True

    def _selected_from_layer(self, layer, layer_fp) -> bool:
        """`layer_is_selected`, every fingerprint form rebuilt from the layer."""
        try:
            return layer_is_selected(layer, self._allowed_layer_fingerprints)
        except Exception as exc:  # noqa: BLE001 - a doubt never stops capture
            flog(f"EditSessionTracker: selection test degraded for "
                 f"{str(layer_fp)[-60:]}: {exc}", "WARNING")
            return True

    def _is_tracked(self, layer_fp) -> bool:
        """True when this datasource is in the user's tracking selection.

        Equivalence, never string equality: the same file opened with a
        display filter, a path written relative instead of absolute, or a
        fingerprint captured before v6 all denote the same source. Any of
        them failing to match means the layer is no longer audited and
        the user is told nothing.
        """
        if not self._allowed_layer_fingerprints:
            return True
        if not layer_fp:
            return False
        if layer_fp in self._tracked_scope:
            return True
        try:
            if canonicalize_fingerprint(layer_fp) in self._tracked_scope:
                return True
            return any(datasource_fingerprints_match(tracked, layer_fp)
                       for tracked in self._tracked_scope)
        except Exception as exc:  # noqa: BLE001 - a doubt never stops capture
            flog(f"EditSessionTracker: tracking test degraded for "
                 f"{layer_fp[:60]!r}: {exc}", "WARNING")
            return True

    def connect_layer(self, layer) -> None:
        """Start monitoring a layer for edit events.

        Gate by `identity_strength` so the user has a structured log of
        capture decisions per layer. Behaviour by strength:

        * STRONG  -> capture, INFO
        * MEDIUM  -> capture, INFO action=accepted_untested
        * WEAK    -> capture, WARNING action=warned (recommend a PK)
        * NONE    -> refuse,  WARNING action=refused

        Providers refused at the support level (REFUSED) are blocked
        upstream. Providers whose policy disables capture (capture=False)
        are refused with reason=capture_disabled.
        """
        layer_id = layer.id()
        # Skip RecoverLand's own ephemeral overlay layers (Review snapshot /
        # lens). They are pure visualisation, are always refused below anyway,
        # and during Review entry dozens are added in one batch: running the
        # full identity/support evaluation on each was pure overhead. Marked
        # via custom property before they ever reach the project registry.
        try:
            if (layer.customProperty("_rl_snap_managed") == "1" or layer.customProperty("_rl_lens_managed") == "1"):
                return
        except (RuntimeError, AttributeError):
            pass
        self._watch_layer(layer)
        try:
            layer_fp = compute_datasource_fingerprint(layer)
        except Exception as exc:  # noqa: BLE001 - one bad layer, not the loop
            # A layer without a data provider (broken source, plugin layer)
            # used to raise here and abort the loop that wires every other
            # layer of the project.
            flog(f"EditSessionTracker.connect_layer: layer_id={layer_id} "
                 f"action=refused reason=fingerprint_failed "
                 f"error={type(exc).__name__}: {exc}", "WARNING")
            return
        if not self.admit_layer(layer, layer_fp):
            return
        if layer_id in self._connected_layers:
            return

        try:
            policy = evaluate_layer_support(layer)
            provider = layer.dataProvider()
            provider_name = provider.name() if provider is not None else "unknown"
            if provider_name == "ogr" and provider is not None:
                driver_name = provider.storageType() or "unknown"
            else:
                driver_name = provider_name
        except (RuntimeError, AttributeError) as exc:
            flog(
                f"EditSessionTracker.connect_layer: layer={layer.name()!r} "
                f"layer_id={layer_id} action=refused reason=policy_eval_failed "
                f"error={type(exc).__name__} msg={exc}",
                "WARNING",
            )
            return

        strength = policy.identity_strength
        support = policy.support_level
        base = (
            f"EditSessionTracker.connect_layer: layer={layer.name()!r} "
            f"provider={provider_name} driver={driver_name} "
            f"identity_strength={strength.value} support_level={support.value}"
        )

        if support == SupportLevel.REFUSED:
            flog(
                f"{base} action=refused reason=provider_refused "
                f"policy_reason={policy.reason!r}",
                "WARNING",
            )
            return

        if strength == IdentityStrength.NONE:
            flog(
                f"{base} action=refused reason=no_stable_identity",
                "WARNING",
            )
            return

        if not policy.capture:
            flog(
                f"{base} action=refused reason=capture_disabled",
                "INFO",
            )
            return

        if strength == IdentityStrength.WEAK:
            level = "WARNING"
            tail = ' action=warned recommendation="add a PK column"'
        elif strength == IdentityStrength.MEDIUM:
            level = "INFO"
            tail = " action=accepted_untested"
        else:
            level = "INFO"
            tail = " action=accepted"

        self._bind_signals(layer)
        self._connected_layers[layer_id] = layer
        self._layer_fingerprints[layer_id] = layer_fp
        flog(f"{base}{tail} layer_id={layer_id}", level)
        if (self._active and not self.is_suppressed
                and not self._layer_suppressed(layer_id)):
            try:
                if hasattr(layer, "isEditable") and layer.isEditable():
                    self._check_capturable(layer_id)
            except RuntimeError:
                pass
        self._reconcile_interrupted(layer, layer_fp)

    def _watch_layer(self, layer) -> None:
        """Signals kept on EVERY vector layer, connected or not.

        dataSourceChanged: a layer refused at load (source offline, file
        missing) and repaired later keeps its id and emits only this
        signal; nobody listened, and the repaired layer was edited all
        afternoon with no capture. editingStarted on a layer the tracking
        selection excludes: the user must know that what he is about to
        edit will not be recorded.
        """
        try:
            layer_id = layer.id()
        except (RuntimeError, AttributeError):
            return
        watched = self.__dict__.setdefault("_watched", {})
        entry = watched.get(layer_id)
        if entry is not None:
            if not _layer_gone(entry[0]):
                return
            # The layer this id named was destroyed without layersRemoved
            # (deleted behind the project's back: QgsMapLayerStore only
            # forgets it). Kept, its entries made connect_layer skip the
            # new layer that carries the id: edited, never captured.
            flog(f"EditSessionTracker: layer {layer_id} was destroyed "
                 f"without leaving the project, its tracking is released "
                 f"before the new layer with that id is connected", "WARNING")
            self.disconnect_layer_by_id(layer_id)
        slots = []
        try:
            slot_src = self._guarded(
                layer.dataSourceChanged,
                lambda *_a, lid=layer_id: self._on_datasource_changed(lid))
            layer.dataSourceChanged.connect(slot_src)
            slots.append((layer.dataSourceChanged, slot_src))
            slot_edit = self._guarded(
                layer.editingStarted,
                lambda *_a, lid=layer_id: self._on_watched_editing(lid))
            layer.editingStarted.connect(slot_edit)
            slots.append((layer.editingStarted, slot_edit))
        except (AttributeError, RuntimeError, TypeError):
            pass
        watched[layer_id] = (layer, slots)

    def _unwatch_layer(self, layer_id: str) -> None:
        entry = self.__dict__.get("_watched", {}).pop(layer_id, None)
        if entry is None:
            return
        if _layer_gone(entry[0]):
            # A destroyed QObject took its connections with it. Its stored
            # bound signals still hold the raw C++ pointer: disconnecting
            # through them read freed memory (0xC0000005 at File > New).
            return
        for sig, slot in entry[1]:
            try:
                sig.disconnect(slot)
            except (TypeError, RuntimeError):
                pass

    def _on_datasource_changed(self, layer_id: str) -> None:
        entry = self.__dict__.get("_watched", {}).get(layer_id)
        if entry is None:
            return
        layer = entry[0]
        try:
            new_fp = compute_datasource_fingerprint(layer)
        except Exception as exc:  # noqa: BLE001
            flog(f"EditSessionTracker: data source changed on {layer_id}, "
                 f"fingerprint failed ({exc})", "WARNING")
            return
        if layer_id in self._connected_layers:
            old = self._layer_fingerprints.get(layer_id)
            if new_fp != old:
                self._finalize_unfinished(layer_id, "datasource_changed")
                if not self.admit_layer(layer, new_fp):
                    # Repointed outside the tracking selection. Swapping the
                    # fingerprint alone left the layer "connected" while
                    # every signal refused it: nothing captured, and the
                    # "not tracked" alert, which only watches unconnected
                    # layers, never went off. Released, it gets that alert.
                    self.disconnect_layer(layer)
                    flog(f"EditSessionTracker: data source of {layer_id} "
                         f"changed to a source outside the tracking "
                         f"selection ({new_fp[-50:]}), capture released",
                         "WARNING")
                    return
                self._layer_fingerprints[layer_id] = new_fp
                flog(f"EditSessionTracker: data source of {layer_id} changed, "
                     f"fingerprint updated ({str(old)[-50:]} -> {new_fp[-50:]})")
            return
        flog(f"EditSessionTracker: data source of {layer_id} changed, "
             f"retrying the connection")
        self.connect_layer(layer)

    def _on_watched_editing(self, layer_id: str) -> None:
        if not self._active or layer_id in self._connected_layers:
            return
        entry = self.__dict__.get("_watched", {}).get(layer_id)
        if entry is None:
            return
        layer = entry[0]
        try:
            layer_fp = compute_datasource_fingerprint(layer)
        except Exception:  # noqa: BLE001
            layer_fp = None
        selected = bool(layer_fp) and (self._is_tracked(layer_fp)
                                       or self._selected_from_layer(layer, layer_fp))
        reason = ("not_in_tracking_selection" if layer_fp and not selected
                  else "refused_by_support_policy")
        self._notify_status("untracked_edit", layer_id=layer_id,
                            layer_name=_safe_layer_name(layer), reason=reason)

    def _reconcile_interrupted(self, layer, layer_fp) -> None:
        """Resolve, against the provider, commits a crash left INTERRUPTED."""
        journal = getattr(self._write_queue, "journal_path", None)
        if not journal:
            return
        try:
            from .crash_reconcile import reconcile_layer
            totals = reconcile_layer(layer, self._write_queue, journal, layer_fp)
        except Exception as exc:  # noqa: BLE001 - never blocks the connection
            flog(f"EditSessionTracker: crash reconciliation failed for "
                 f"{_safe_layer_name(layer)!r}: {exc}", "ERROR")
            return
        if totals.get("commits"):
            self._notify_status("reconciled", layer_id=layer.id(),
                                layer_name=_safe_layer_name(layer), **totals)

    def disconnect_layer(self, layer) -> None:
        """Stop monitoring a layer."""
        layer_id = layer.id()
        self._finalize_unfinished(layer_id, "disconnect")
        self._unbind_signals(layer_id, layer)
        self._buffers.pop(layer_id, None)
        self._connected_layers.pop(layer_id, None)
        self._layer_fingerprints.pop(layer_id, None)
        self._sessions.pop(layer_id, None)

    def disconnect_layer_by_id(self, layer_id: str) -> None:
        """Stop monitoring a layer by its ID (for layersRemoved signal).

        At layersRemoved the project has ALREADY destroyed every layer it
        owned (removeMapLayer, File > New, another project opened), while
        a layer taken out alive (takeMapLayer) is still there. Nothing here
        goes through a destroyed layer: its entries are dropped by id, and
        only a living layer has its signals disconnected.
        """
        self._forget_destroyed(layer_id)
        self._finalize_unfinished(layer_id, "layer_removed")
        self._unwatch_layer(layer_id)
        self._sessions.pop(layer_id, None)
        self._unbind_signals(layer_id, self._connected_layers.get(layer_id))
        self._buffers.pop(layer_id, None)
        self._connected_layers.pop(layer_id, None)
        self._layer_fingerprints.pop(layer_id, None)

    def disconnect_all(self) -> None:
        for layer_id in list(self._connected_layers.keys()):
            self._forget_destroyed(layer_id)
        self._finalize_all_unfinished("disconnect_all")
        self._sessions.clear()
        for lid in list(self.__dict__.get("_watched", {}).keys()):
            self._unwatch_layer(lid)
        # Every stored connection, not only those of the layers still
        # connected: a leftover entry is a bound signal that holds a raw
        # C++ pointer.
        for layer_id in (set(self._signal_connections)
                         | set(self._connected_layers)):
            self._unbind_signals(layer_id, self._connected_layers.get(layer_id))
        self._connected_layers.clear()
        self._layer_fingerprints.clear()
        self._buffers.clear()

    def _forget_destroyed(self, layer_id: str) -> None:
        """Drop a destroyed layer from `_connected_layers`, touching nothing.

        Run before an open commit attempt of that layer is finalised: the
        finalisation reaches the connected layer (`_after_enqueue`
        registers its data source), and a destroyed one is freed memory.
        The attempt itself carries everything it needs (`_CommitAttempt`).
        """
        layer = self._connected_layers.get(layer_id)
        if layer is None or not _layer_gone(layer):
            return
        del self._connected_layers[layer_id]
        attempt = self._attempts.get(layer_id)
        if attempt is not None and not attempt.finalized:
            flog(f"EditSessionTracker: layer {layer_id} destroyed with commit "
                 f"{attempt.commit_id} still open; it is finalised from what "
                 f"was captured, its data source is not registered from it",
                 "WARNING")

    def _bind_signals(self, layer) -> None:
        """Wire all six QgsVectorLayer commit signals so the tracker can
        observe both projected pre-commit state and authoritative post-commit
        state (per QGIS API: commit happens in stages and each stage emits a
        committed*Changes signal once the provider has accepted it)."""
        layer_id = layer.id()
        slots = [
            (layer.editingStarted,
             lambda *_, lid=layer_id: self._on_editing_started(lid)),
            (layer.beforeCommitChanges,
             lambda *_, lid=layer_id: self._on_before_commit(lid)),
            (layer.committedFeaturesAdded,
             lambda _lid, feats, lid=layer_id:
                 self._on_committed_features_added(lid, feats)),
            (layer.committedFeaturesRemoved,
             lambda _lid, fids, lid=layer_id:
                 self._on_committed_features_removed(lid, fids)),
            (layer.committedAttributeValuesChanges,
             lambda _lid, changes, lid=layer_id:
                 self._on_committed_attribute_values_changes(lid, changes)),
            (layer.committedGeometriesChanges,
             lambda _lid, changes, lid=layer_id:
                 self._on_committed_geometries_changes(lid, changes)),
            (layer.afterCommitChanges,
             lambda *_, lid=layer_id: self._on_after_commit(lid)),
            (layer.afterRollBack,
             lambda *_, lid=layer_id: self._on_rollback(lid)),
        ]
        slots = [(signal, self._guarded(signal, slot)) for signal, slot in slots]
        for signal, slot in slots:
            signal.connect(slot)
        self._signal_connections[layer_id] = slots

    def _guarded(self, signal, fn):
        """A slot that never lets an exception escape to Qt.

        Raised out of a slot called by Qt, an exception either aborts the
        process (PyQt6 without a global hook) or reaches the QGIS error
        dialog: the slot stops half-way, the commit goes on, the data is
        written and the history never hears of it while the indicator
        stays green. Here the failure is logged with its traceback,
        counted (the indicator turns to ERROR) and announced.
        """
        name = getattr(signal, "signal", None) or repr(signal)

        def slot(*args):
            try:
                fn(*args)
            except Exception as exc:  # noqa: BLE001 - a Qt slot must not raise
                import traceback
                self.capture_failures += 1
                flog(f"EditSessionTracker: event=slot_failed slot={name} "
                     f"error={type(exc).__name__}: {exc} "
                     f"trace={traceback.format_exc()[-800:]!r}", "CRITICAL")
                self._notify_status("slot_failed", slot=str(name),
                                    error=f"{type(exc).__name__}: {exc}")
        return slot

    def _unbind_signals(self, layer_id: str, layer) -> None:
        """Disconnect the commit signals stored for *layer_id*.

        By id, never through ``layer.id()``: on a destroyed layer that call
        raised, and the exception skipped the pop below, so eight bound
        signals holding a freed pointer stayed stored for good. The entry
        is always dropped; its signals are disconnected only while the
        layer lives (a destroyed QObject already dropped its connections,
        and a bound signal disconnects through its raw C++ pointer).
        """
        slots = self._signal_connections.pop(layer_id, [])
        if _layer_gone(layer):
            return
        for signal, slot in slots:
            try:
                signal.disconnect(slot)
            except (TypeError, RuntimeError):
                pass

    def _create_buffer(self, layer_id: str) -> EditSessionBuffer:
        session_id = self._sessions.get(layer_id) or str(uuid.uuid4())
        self._sessions[layer_id] = session_id
        buf = EditSessionBuffer(layer_id, session_id)
        self._buffers[layer_id] = buf
        return buf

    def _on_editing_started(self, layer_id: str) -> None:
        if self._pressure_paused and not self.is_suppressed:
            self._resume_after_buffer_pressure()
        if not self._active or self.is_suppressed:
            return
        if self._layer_suppressed(layer_id):
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if not self._is_tracked(layer_fp):
            return
        # A leftover attempt from the previous session (commit failed,
        # editing stopped) is resolved before the new session starts.
        self._finalize_unfinished(layer_id, "editing_restarted")
        self._sessions[layer_id] = str(uuid.uuid4())
        self._check_capturable(layer_id)
        flog(f"EditSessionTracker: editing started on {layer_id} "
             f"session={self._sessions[layer_id]}")

    def _check_capturable(self, layer_id: str) -> bool:
        """Detect edit sessions whose pre-edit state cannot be captured.

        In an automatic transaction group the edit buffer is a passthrough:
        every edit is written to the database at once, inside a transaction.
        At beforeCommitChanges the provider already returns the NEW values,
        so the pre-edit state RecoverLand reads there is the post-edit one
        and every commit looks empty: capture would be silently dead. Say
        it, instead of journalling nothing.
        """
        layer = self._connected_layers.get(layer_id)
        if layer is None:
            return True
        try:
            from qgis.core import QgsVectorLayerEditPassthrough
            passthrough = isinstance(layer.editBuffer(), QgsVectorLayerEditPassthrough)
        except (ImportError, RuntimeError, AttributeError):
            passthrough = False
        if passthrough:
            self._uncapturable_sessions[layer_id] = self._sessions.get(layer_id, "")
            self._notify_status(
                "uncapturable", layer_id=layer_id,
                layer_name=_safe_layer_name(layer),
                reason="automatic_transaction_group")
            return False
        self._uncapturable_sessions.pop(layer_id, None)
        return True

    def _on_before_commit(self, layer_id: str) -> None:
        """Open a commit attempt and make its pre-edit state durable.

        Everything the provider is about to overwrite is read HERE, before
        it writes anything, and handed to the outbox (`prepare_commit`)
        before this slot returns. A crash anywhere after this point leaves
        a durable record of what the features looked like before the
        commit; a crash before it happens before the provider wrote.
        """
        if not self._active or self.is_suppressed:
            return
        if self._layer_suppressed(layer_id):
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if not self._is_tracked(layer_fp):
            return
        layer = self._connected_layers.get(layer_id)
        if layer is None:
            return
        # A previous attempt that never saw afterCommitChanges (the user is
        # retrying after a refused commit) is resolved first: what it wrote
        # is journalled, what it did not is dropped, and its stale
        # snapshots can never leak into this attempt.
        self._finalize_unfinished(layer_id, "retry")
        if layer_id in self._uncapturable_sessions:
            self._notify_status(
                "uncapturable", layer_id=layer_id,
                layer_name=_safe_layer_name(layer),
                reason="automatic_transaction_group")
            return
        if not self._check_capturable(layer_id):
            return
        if layer_id not in self._sessions:
            self._sessions[layer_id] = str(uuid.uuid4())
            flog(f"EditSessionTracker: late session start on {layer_id}")
        buf = self._create_buffer(layer_id)
        from .capture_outbox import new_commit_id
        attempt = _CommitAttempt(
            new_commit_id(), layer_id, buf.session_id, buf,
            self._capture_layer_meta(layer), datetime.now(timezone.utc).isoformat())
        self._attempts[layer_id] = attempt
        added_payload = self._capture_edit_buffer_state(layer, buf)
        view = getattr(buf, "schema_view", None)
        if view and view.get("pending"):
            attempt.meta["field_names"] = list(view["final_names"])
        self._prepare_attempt(attempt, added_payload, layer)
        from .capture_outbox import fault_point
        fault_point("capture_after_prepare")
        # commitChanges() is synchronous: this zero-delay timer runs once it
        # has returned to the event loop. If afterCommitChanges has not
        # finalised the attempt by then, the commit failed, partly or wholly.
        commit_id = attempt.commit_id
        QTimer.singleShot(
            0, lambda lid=layer_id, cid=commit_id:
                self._finalize_unfinished(lid, "commit_returned", cid))

    def _capture_layer_meta(self, layer) -> dict:
        """Everything event generation needs from the layer, read while alive."""
        geom_type, crs = self._get_layer_geometry_info(layer)
        provider = layer.dataProvider()
        return {
            "layer_id": layer.id(),
            "ds_fp": compute_datasource_fingerprint(layer),
            "proj_fp": compute_project_fingerprint(),
            "layer_name": extract_layer_name(layer),
            "provider_type": provider.name() if provider is not None else "unknown",
            "user": resolve_user_name(),
            "field_schema": serialize_field_schema(layer.fields()),
            "geom_type": geom_type,
            "crs": crs,
            "field_names": self._capture_field_names(layer),
        }

    def _prepare_attempt(self, attempt: _CommitAttempt, added_payload, layer) -> None:
        """Hand the pre-edit state to the durable outbox before the provider writes."""
        buf = attempt.buf
        if not (buf.get_deleted_snapshots() or buf.get_modified_snapshots()
                or added_payload):
            return
        prepare = getattr(self._write_queue, "prepare_commit", None)
        if prepare is None:
            return
        if added_payload:
            self._mark_unproven_additions(layer, added_payload, buf)
        meta = attempt.meta
        geom_fids = getattr(buf, "geometry_changed_fids", None)
        added, unproven = [], []
        for entry in added_payload or []:
            record = {k: v for k, v in entry.items() if not k.startswith("_")}
            if "unproven" in record:
                # Not in "added": its content designates an existing
                # feature (or could not be checked). The reconciliation
                # sets the recorded twins aside by FID before it judges
                # what remains (crash_reconcile._addition_verdict).
                record.update(kind=UNPROVEN_INSERT_KIND, fid=None)
                unproven.append(record)
            else:
                added.append(record)
        payload = {
            "layer_id": attempt.layer_id,
            "layer_name": meta.get("layer_name"),
            "ds": meta.get("ds_fp"),
            "session_id": attempt.session_id,
            "provider": meta.get("provider_type"),
            "snapshots": (
                [_snapshot_record("DELETE", sn) for sn in buf.get_deleted_snapshots().values()]
                + [_snapshot_record("UPDATE", sn, geom_fids)
                   for sn in buf.get_modified_snapshots().values()]
                + unproven),
            "added": added,
        }
        try:
            attempt.prepared_durable = bool(prepare(attempt.commit_id, payload))
        except Exception as exc:  # noqa: BLE001 - the commit goes on anyway
            flog(f"EditSessionTracker: prepare_commit raised {exc!r}", "ERROR")
            attempt.prepared_durable = False
        if not attempt.prepared_durable:
            self._notify_status(
                "prepare_not_durable", layer_id=attempt.layer_id,
                commit_id=attempt.commit_id,
                layer_name=meta.get("layer_name"))

    def _on_after_commit(self, layer_id: str) -> None:
        """The whole edit buffer reached the provider."""
        if not self._active or self.is_suppressed:
            return
        if self._layer_suppressed(layer_id):
            return
        layer_fp = self._layer_fingerprints.get(layer_id)
        if not self._is_tracked(layer_fp):
            return
        attempt = self._attempts.get(layer_id)
        if attempt is None or attempt.finalized:
            return
        from .capture_outbox import fault_point
        fault_point("capture_after_provider_commit")
        self._finalize_attempt(attempt, committed=True, reason="after_commit")

    def _finalize_unfinished(self, layer_id: str, reason: str,
                             commit_id: Optional[str] = None) -> None:
        """Resolve an attempt that afterCommitChanges did not finalise."""
        attempt = self._attempts.get(layer_id)
        if attempt is None or attempt.finalized:
            return
        if commit_id is not None and attempt.commit_id != commit_id:
            return
        self._finalize_attempt(attempt, committed=False, reason=reason)

    def _finalize_all_unfinished(self, reason: str) -> None:
        for layer_id in list(self._attempts.keys()):
            self._finalize_unfinished(layer_id, reason)

    def _finalize_attempt(self, attempt: _CommitAttempt, committed: bool,
                          reason: str) -> None:
        """Turn one commit attempt into journal events, exactly once.

        committed=False means afterCommitChanges never came: only what the
        provider CONFIRMED through its committed* signals is journalled.
        The pre-commit projection of the edit buffer is a wish, not a fact.
        """
        attempt.finalized = True
        layer_id = attempt.layer_id
        if self._attempts.get(layer_id) is attempt:
            del self._attempts[layer_id]
        if self._buffers.get(layer_id) is attempt.buf:
            self._buffers.pop(layer_id, None)
        buf = attempt.buf
        if not committed and not attempt.got_committed_signal:
            attempt.state = "NO_WRITE"
            if attempt.prepared_durable:
                self._enqueue_commit([], attempt, "NO_WRITE",
                                     f"nothing reached the provider ({reason})")
            flog(f"EditSessionTracker: commit {attempt.commit_id} on {layer_id} "
                 f"wrote nothing (reason={reason}), pre-commit capture dropped")
            return
        state = "COMMITTED" if committed else "PARTIAL"
        try:
            events = self._generate_events(attempt, authoritative_only=not committed)
        except Exception as exc:  # noqa: BLE001 - the capture must survive
            import traceback
            self.capture_failures += 1
            attempt.state = "GENERATION_FAILED"
            snapshots = [_snapshot_record("DELETE", sn) for sn in buf.get_deleted_snapshots().values()]
            snapshots += [_snapshot_record("UPDATE", sn, getattr(buf, "geometry_changed_fids", None))
                          for sn in buf.get_modified_snapshots().values()]
            import json as _json
            detail = (f"{type(exc).__name__}: {exc} | "
                      f"{traceback.format_exc()[-600:]}")
            self._enqueue_commit(
                [], attempt, "GENERATION_FAILED", detail,
                snapshots_json=_json.dumps(snapshots, ensure_ascii=False, default=str))
            flog(f"EditSessionTracker: event=capture_generation_failed "
                 f"commit={attempt.commit_id} layer={layer_id} error={detail}",
                 "CRITICAL")
            self._notify_status("generation_failed", layer_id=layer_id,
                                commit_id=attempt.commit_id,
                                layer_name=attempt.meta.get("layer_name"),
                                error=f"{type(exc).__name__}: {exc}")
            return
        attempt.state = state
        if not committed:
            self.partial_commits += 1
        if not events:
            flog(f"EditSessionTracker: commit on {layer_id} produced 0 real events "
                 f"(state={state}; false commit: edit buffer had changes but "
                 f"values were identical)")
            if attempt.prepared_durable:
                self._enqueue_commit([], attempt, state, "no effective change")
            return
        outcome = self._enqueue_commit(events, attempt, state, reason)
        self._after_enqueue(attempt, events, outcome, state)
        if buf.over_budget:
            # No pause any more. The capture of this commit is complete and
            # handed to the durable outbox, its memory released with the
            # attempt. Pausing afterwards guaranteed the loss of every
            # later commit of a layer kept in edit mode ("Save Layer Edits"
            # keeps editing), under a status that still said "active".
            self._notify_status("large_commit", layer_id=buf.layer_id,
                                layer_name=attempt.meta.get("layer_name"),
                                memory_mb=round(buf.approx_memory_mb, 1),
                                features=buf.total_tracked)

    def _enqueue_commit(self, events, attempt: _CommitAttempt, state: str,
                        detail: str, snapshots_json: Optional[str] = None):
        from .write_queue import CommitInfo
        meta = attempt.meta
        info = CommitInfo(
            commit_id=attempt.commit_id, state=state, layer_id=attempt.layer_id,
            layer_name=meta.get("layer_name"),
            datasource_fingerprint=meta.get("ds_fp"),
            session_id=attempt.session_id, prepared_at=attempt.prepared_at,
            detail=detail, snapshots_json=snapshots_json)
        try:
            try:
                return self._write_queue.enqueue(events, commit=info)
            except TypeError:
                # A write queue without commit support (test doubles, old API).
                if not events:
                    return True
                return self._write_queue.enqueue(events)
        except Exception as exc:  # noqa: BLE001 - never lose a commit in a slot
            # An exception escaping here used to unwind into the Qt slot: the
            # attempt was already finalised, the events existed nowhere.
            flog(f"EditSessionTracker: event=enqueue_raised commit="
                 f"{attempt.commit_id} n_events={len(events)} "
                 f"error={type(exc).__name__}: {exc}", "CRITICAL")
            return False

    def _after_enqueue(self, attempt: _CommitAttempt, events, outcome,
                       state: str) -> None:
        self.last_commit_outcome = outcome
        meta = attempt.meta
        layer_id = attempt.layer_id
        try:
            from .logger import jlog
            ops = {}
            for e in events:
                ops[e.operation_type] = ops.get(e.operation_type, 0) + 1
            jlog("capture_commit", "INFO" if outcome else "CRITICAL",
                 component="tracker", commit_id=attempt.commit_id,
                 layer=meta.get("layer_name"), layer_id=layer_id,
                 datasource=meta.get("ds_fp"), state=state,
                 event_count=len(events), ops=ops,
                 outcome=getattr(outcome, "value", str(outcome)),
                 prepared_durable=attempt.prepared_durable)
        except Exception:  # noqa: BLE001 # nosec B110 - forensic log never breaks capture
            pass
        if not outcome:
            # Nothing durable could be written, or the batch was set aside
            # as malformed. The data is already in the provider: say so,
            # and keep capturing (the next commit may find the disk back).
            self.capture_failures += 1
            flog(f"EditSessionTracker: event=capture_not_durable "
                 f"commit={attempt.commit_id} n_events={len(events)} "
                 f"outcome={getattr(outcome, 'value', outcome)}", "CRITICAL")
            self._notify_status("lost", layer_id=layer_id,
                                commit_id=attempt.commit_id,
                                layer_name=meta.get("layer_name"),
                                n_events=len(events),
                                outcome=getattr(outcome, "value", str(outcome)))
            if self._on_overflow_callback is not None:
                try:
                    self._on_overflow_callback()
                except Exception as exc:  # noqa: BLE001 - a notification
                    flog(f"EditSessionTracker: overflow callback failed: {exc}",
                         "ERROR")
            return
        if getattr(outcome, "degraded", False):
            self._notify_status("degraded", layer_id=layer_id,
                                commit_id=attempt.commit_id,
                                layer_name=meta.get("layer_name"),
                                n_events=len(events),
                                outcome=getattr(outcome, "value", str(outcome)))
        if state == "PARTIAL":
            self._notify_status("partial_commit", layer_id=layer_id,
                                commit_id=attempt.commit_id,
                                layer_name=meta.get("layer_name"),
                                n_events=len(events))
        layer = self._connected_layers.get(layer_id)
        if layer is not None:
            self._register_datasource(layer)
        self._session_event_count += len(events)
        ops = [e.operation_type for e in events]
        flog(f"EditSessionTracker: {len(events)} events for {layer_id} "
             f"commit={attempt.commit_id} state={state} ops={ops[:20]} "
             f"created_at={events[0].created_at} "
             f"outcome={getattr(outcome, 'value', outcome)}")
        layer_name = events[0].layer_name_snapshot or layer_id
        delete_count = sum(1 for e in events if e.operation_type == "DELETE")
        is_mass_delete = delete_count >= self._MASS_DELETE_THRESHOLD
        if self._on_commit_callback is not None:
            self._on_commit_callback(
                len(events), layer_name, is_mass_delete, delete_count,
                events[0].datasource_fingerprint)

    def _pause_after_buffer_pressure(self, buf: EditSessionBuffer) -> None:
        """Stop capture after a commit whose capture blew the memory budget.

        The events of the commit that blew the budget have already been
        handed to the durable outbox, so nothing is lost by stopping here.
        The pause is reported as a capture status, never left to the log.
        """
        self._pressure_paused = True
        self._finalize_all_unfinished("buffer_pressure")
        self._active = False
        flog(f"EditSessionTracker: capture paused after buffer pressure on "
             f"{buf.layer_id} ({buf.approx_memory_mb:.0f} MB, "
             f"{buf.total_tracked} features), re-arms on the next "
             f"editing session", "ERROR")
        self._notify_status("paused_buffer_pressure", layer_id=buf.layer_id,
                            memory_mb=round(buf.approx_memory_mb, 1),
                            features=buf.total_tracked)

    def _resume_after_buffer_pressure(self) -> None:
        """Re-arm capture that a memory spike had paused."""
        self._pressure_paused = False
        self.activate()
        flog("EditSessionTracker: capture re-armed, buffer pressure cleared")

    def _register_datasource(self, layer) -> None:
        """Store the layer's source URI in the journal registry for future restore."""
        if _layer_gone(layer):
            flog("EditSessionTracker: datasource not registered, its layer "
                 "was destroyed", "WARNING")
            return
        try:
            conn = self._journal_manager.get_connection()
            from .datasource_registry import register_datasource
            register_datasource(conn, layer)
        except Exception as e:
            flog(f"EditSessionTracker: datasource register failed: {e}", "WARNING")

    def _on_committed_features_added(self, layer_id: str, features) -> None:
        """Capture the added features AS THE PROVIDER STORED THEM.

        Fires during commit, once the provider has written the additions
        and assigned their FIDs. The features are read back by those FIDs
        (see `_read_back_committed`: the signal's own attributes are not
        what was written) and serialised here, so that _generate_events
        can build INSERT events without any later provider lookup.
        """
        if not self._active or self.is_suppressed:
            flog(
                f"EditSessionTracker: signal=committedFeaturesAdded "
                f"ignored=suppressed layer_id={layer_id}",
                "DEBUG",
            )
            return
        if self._layer_suppressed(layer_id):
            return
        buf = self._buffers.get(layer_id)
        attempt = self._attempts.get(layer_id)
        if attempt is not None and attempt.buf is buf:
            attempt.got_committed_signal = True
        if buf is None:
            flog(f"EditSessionTracker: committedFeaturesAdded but no buffer "
                 f"for {layer_id}", "WARNING")
            return
        layer = self._connected_layers.get(layer_id)
        if layer is None:
            flog(f"EditSessionTracker: committedFeaturesAdded but no layer "
                 f"for {layer_id}", "WARNING")
            return
        field_names = self._capture_field_names(layer)
        features = list(features)
        unverified: Dict[int, List[str]] = {}
        n_read = 0
        # One chunk at a time: a pasted batch of 100 000 features is never
        # held twice in memory, nor read through one 100 000-term filter.
        for start in range(0, len(features), self._READBACK_CHUNK):
            chunk = features[start:start + self._READBACK_CHUNK]
            stored = self._read_back_committed(layer, chunk)
            n_read += len(stored)
            for feature in chunk:
                source = stored.get(feature.id())
                unknown: List[str] = []
                if source is None:
                    source = _UnsetMasked(feature)
                    unknown = source.unset_names(field_names)
                    if unknown:
                        unverified[feature.id()] = unknown
                attrs = serialize_attributes(source, field_names)
                for name in unknown:
                    attrs.pop(name, None)
                wkb = geometry_to_wkb(source.geometry())
                identity = compute_feature_identity(layer, source)
                buf.record_committed_addition({
                    "fid": feature.id(),
                    "attrs_json": build_full_snapshot(attrs),
                    "geometry_wkb": wkb,
                    "identity_json": identity,
                    "entity_fingerprint": compute_entity_fingerprint(identity),
                })
        flog(f"EditSessionTracker: committedFeaturesAdded "
             f"{len(features)} features captured on {layer_id} "
             f"(read back from the provider: {n_read})")
        if unverified:
            fields = sorted({n for names in unverified.values() for n in names})
            flog(f"EditSessionTracker: event=insert_values_unverified "
                 f"layer={layer_id} fids={sorted(unverified)[:20]} "
                 f"(total={len(unverified)}) fields={fields}: the provider "
                 f"chose these values and did not return the features on "
                 f"read-back; the INSERT snapshots leave them out", "ERROR")
            self._notify_status(
                "insert_values_unverified", layer_id=layer_id,
                layer_name=_safe_layer_name(layer),
                commit_id=getattr(attempt, "commit_id", None),
                fids=sorted(unverified)[:50], n_features=len(unverified),
                fields=fields)

    _READBACK_CHUNK = 1000

    def _read_back_committed(self, layer, features) -> Dict[int, object]:
        """The committed features as the provider stored them, by FID.

        The features of committedFeaturesAdded are not what was written:
        every field the digitising form left to the source (a column
        DEFAULT, an auto-increment key) still holds a QgsUnsetAttributeValue
        (measured on QGIS 4.2.1: the GeoPackage file holds 'AUTO' and a
        timestamp, the signal '<QgsUnsetAttributeValue: AUTO>'; on
        Spatialite the key itself is '<...: Autogenerate>'), and a trigger
        or a type coercion is invisible there too. Serialised as is, the
        INSERT snapshot named values the data never had: undoing it fell to
        a WEAK proof and was refused, and on Spatialite every inserted
        feature shared one bogus key.

        Read on the provider (not the layer: no edit buffer, no joined or
        virtual field), by the FIDs the provider itself assigned (the caller
        passes one chunk at a time). Spatialite (measured) and PostgreSQL
        apply the layer filter even to a read by FID: an addition outside
        the filter is read again without it (`_read_back_unfiltered`). A
        feature neither read returns keeps what the signal carried minus
        the fields left to the source: their value is unknown, so it is
        left out of the snapshot, never guessed, and the gap is announced.
        Measured cost on a GeoPackage: about 65 us per added feature.
        """
        from qgis.core import QgsFeatureRequest
        out: Dict[int, object] = {}
        provider = layer.dataProvider()
        fids = [f.id() for f in features]
        if provider is None or not fids:
            return out
        try:
            request = QgsFeatureRequest().setFilterFids(fids)
            for stored in provider.getFeatures(request):
                out[stored.id()] = stored
        except Exception as exc:  # noqa: BLE001 - the capture must survive
            flog(f"EditSessionTracker: read-back of the committed additions "
                 f"failed on {layer.id()} after {len(out)}/{len(fids)} "
                 f"feature(s): {type(exc).__name__}: {exc}", "ERROR")
        missed = [f for f in features if f.id() not in out]
        if missed and (provider.subsetString() or ""):
            out.update(self._read_back_unfiltered(layer, missed))
        return out

    @staticmethod
    def _read_back_unfiltered(layer, features) -> Dict[int, object]:
        """Additions the filtered provider hid, read without the filter.

        A second provider on the same source (`_unfiltered_provider`) is not
        the layer's: its FIDs must mean the same rows. A feature it returns
        is kept only if both hold:
          * its FID is the stored key: the provider's single key field holds
            the FID itself (Spatialite INTEGER PRIMARY KEY, PostgreSQL
            integer key), or, with no key field, an SQLite rowid (OGR,
            Spatialite). A FID the provider made up for the session (a
            PostgreSQL text or composite key) could name another row;
          * it holds what the user entered: every field the signal carries
            a value for, and the geometry, are equal.
        Anything else is left to the announced fallback of the caller.
        """
        from qgis.core import QgsFeatureRequest
        from .commit_guard import values_equal, geometry_equal
        from .crash_reconcile import fid_is_stored_key
        out: Dict[int, object] = {}
        other = _unfiltered_provider(layer)
        if other is None:
            return out
        refused = []
        try:
            by_fid = {f.id(): f for f in features}
            pk = list(other.pkAttributeIndexes())
            names = [f.name() for f in other.fields()]
            rowid_fids = layer.providerType() in ("ogr", "spatialite")
            request = QgsFeatureRequest().setFilterFids(list(by_fid))
            for stored in other.getFeatures(request):
                signal = by_fid.get(stored.id())
                if signal is None:
                    continue
                key_ok = fid_is_stored_key(stored, pk, rowid_fids)
                same = key_ok and geometry_equal(stored.geometry(), signal.geometry())
                for name in names if same else ():
                    try:
                        entered = signal.attribute(name)
                    except KeyError:
                        continue
                    if not _is_unset(entered) and not values_equal(
                            stored.attribute(name), entered):
                        same = False
                        break
                if same:
                    out[stored.id()] = stored
                else:
                    refused.append(stored.id())
        except Exception as exc:  # noqa: BLE001 - the capture must survive
            flog(f"EditSessionTracker: unfiltered read-back failed on "
                 f"{layer.id()}: {type(exc).__name__}: {exc}", "ERROR")
        finally:
            del other
        flog(f"EditSessionTracker: {len(out)} committed addition(s) outside "
             f"the filter of {layer.id()} read back without it"
             + (f"; refused (FID not the stored key, or content differs): "
                f"{sorted(refused)[:20]}" if refused else ""),
             "WARNING" if refused else "INFO")
        return out

    def _on_committed_features_removed(self, layer_id: str, fids) -> None:
        """Authoritative confirmation of deletes that the provider applied.

        Without this signal the tracker would log a DELETE event for
        every feature the user marked as deleted in the buffer, even if
        the provider rejected the delete (constraint, referential check).
        """
        if not self._active or self.is_suppressed:
            flog(
                f"EditSessionTracker: signal=committedFeaturesRemoved "
                f"ignored=suppressed layer_id={layer_id}",
                "DEBUG",
            )
            return
        if self._layer_suppressed(layer_id):
            return
        buf = self._buffers.get(layer_id)
        attempt = self._attempts.get(layer_id)
        if attempt is not None and attempt.buf is buf:
            attempt.got_committed_signal = True
        if buf is None:
            return
        for fid in fids:
            buf.record_committed_deletion(int(fid))
        flog(f"EditSessionTracker: committedFeaturesRemoved "
             f"layer={layer_id} fids={sorted(int(f) for f in fids)[:20]} "
             f"(total={len(fids)})")

    def _on_committed_attribute_values_changes(self, layer_id: str,
                                               changed_attrs) -> None:
        """Attribute changes accepted by the provider, AS THE PROVIDER STORED THEM.

        QGIS emits ``{fid_post_commit: {idx: value}}`` once the provider
        accepted the attribute writes. Those are the values the edit buffer
        SENT, not the values written (measured on QGIS 4.2.1): a Shapefile
        N(12,2) column stores 3.33 for the 3.3333333333333335 of the field
        calculator, a GeoPackage stores 12:30Z for a local 14:30. Journalled
        as sent, the NEW side of the UPDATE disagreed with the data, and a
        restore or a rewind, which proves its target by that NEW side,
        refused the edit for good (post_attr_mismatch). The changed values
        are read back here (`_read_back_changed_values`), during the commit,
        before the deletion stage that renumbers a Shapefile. A feature the
        read does not return keeps the value sent, announced as
        ``update_values_unverified``.
        """
        if not self._active or self.is_suppressed:
            flog(
                f"EditSessionTracker: signal=committedAttributeValuesChanges "
                f"ignored=suppressed layer_id={layer_id}",
                "DEBUG",
            )
            return
        if self._layer_suppressed(layer_id):
            return
        buf = self._buffers.get(layer_id)
        attempt = self._attempts.get(layer_id)
        if attempt is not None and attempt.buf is buf:
            attempt.got_committed_signal = True
        if buf is None:
            return
        changes: Dict[int, dict] = {}
        for fid, idx_to_value in changed_attrs.items():
            try:
                fid_i = int(fid)
            except (TypeError, ValueError):
                flog(f"EditSessionTracker: committedAttributeValuesChanges "
                     f"unparseable fid={fid!r}, skipped", "WARNING")
                continue
            changes[fid_i] = dict(idx_to_value)
        layer = self._connected_layers.get(layer_id)
        field_names = ((attempt.meta.get("field_names")
                        if attempt is not None and attempt.buf is buf else None)
                       or (self._capture_field_names(layer) if layer is not None else []))
        unverified = self._read_back_changed_values(layer, changes, field_names)
        for fid_i, values in changes.items():
            buf.record_committed_attr_change(fid_i, values)
        flog(f"EditSessionTracker: committedAttributeValuesChanges "
             f"layer={layer_id} fids={sorted(changes)[:20]} "
             f"(total={len(changes)}, read back from the provider: "
             f"{len(changes) - len(unverified)})")
        if unverified:
            self._announce_unverified_update(layer, layer_id, attempt, unverified)

    def _on_committed_geometries_changes(self, layer_id: str,
                                         changed_geoms) -> None:
        """Geometry changes accepted by the provider, AS THE PROVIDER STORED THEM.

        QGIS emits ``{fid_post_commit: QgsGeometry}`` once the provider
        accepted each geometry write: the geometry SENT. A 2D Shapefile
        stores 'Point (7 8)' for a digitised 'Point Z (7 8 9)', a
        single-part GeoPackage layer the one polygon of a 'MultiPolygon'
        (measured on QGIS 4.2.1). Read back like the attributes (see
        `_on_committed_attribute_values_changes`).
        """
        if not self._active or self.is_suppressed:
            flog(
                f"EditSessionTracker: signal=committedGeometriesChanges "
                f"ignored=suppressed layer_id={layer_id}",
                "DEBUG",
            )
            return
        if self._layer_suppressed(layer_id):
            return
        buf = self._buffers.get(layer_id)
        attempt = self._attempts.get(layer_id)
        if attempt is not None and attempt.buf is buf:
            attempt.got_committed_signal = True
        if buf is None:
            return
        sent = {}
        for fid, geom in changed_geoms.items():
            try:
                fid_i = int(fid)
            except (TypeError, ValueError):
                flog(f"EditSessionTracker: committedGeometriesChanges "
                     f"unparseable fid={fid!r}, skipped", "WARNING")
                continue
            sent[fid_i] = geom
        layer = self._connected_layers.get(layer_id)
        unverified: Dict[int, List[str]] = {}
        fids = list(sent)
        # One chunk at a time: the geometries read back are serialised and
        # released before the next chunk is read.
        for start in range(0, len(fids), self._READBACK_CHUNK):
            chunk = fids[start:start + self._READBACK_CHUNK]
            stored = (self._read_back_updated(layer, chunk, with_geometry=True, names=[])
                      if layer is not None else {})
            for fid_i in chunk:
                feature = stored.get(fid_i)
                if feature is None:
                    unverified[fid_i] = [_GEOMETRY_FIELD_LABEL]
                    wkb = geometry_to_wkb(sent[fid_i])
                else:
                    wkb = geometry_to_wkb(feature.geometry())
                buf.record_committed_geom_change(fid_i, wkb)
        flog(f"EditSessionTracker: committedGeometriesChanges "
             f"layer={layer_id} fids={sorted(sent)[:20]} "
             f"(total={len(sent)}, read back from the provider: "
             f"{len(sent) - len(unverified)})")
        if unverified:
            self._announce_unverified_update(layer, layer_id, attempt, unverified)

    def _read_back_changed_values(self, layer, changes: Dict[int, dict],
                                  field_names: List[str]) -> Dict[int, List[str]]:
        """Replace, in *changes* ``{fid: {idx: value}}``, each value sent by the
        value the provider stored; return ``{fid: [names left as sent]}``.

        An index past *field_names* (a joined or virtual field) is left as
        is: no event ever journals it (`_resolve_new_attrs`).
        """
        unverified: Dict[int, List[str]] = {}
        fids = list(changes)
        for start in range(0, len(fids), self._READBACK_CHUNK):
            chunk = fids[start:start + self._READBACK_CHUNK]
            names = sorted({field_names[i] for fid in chunk for i in changes[fid]
                            if 0 <= i < len(field_names)})
            stored = (self._read_back_updated(layer, chunk, names=names)
                      if layer is not None and names else {})
            for fid in chunk:
                feature = stored.get(fid)
                for idx in list(changes[fid]):
                    if not 0 <= idx < len(field_names):
                        continue
                    name = field_names[idx]
                    try:
                        if feature is None:
                            raise KeyError(name)
                        changes[fid][idx] = feature.attribute(name)
                    except KeyError:
                        unverified.setdefault(fid, []).append(name)
        return unverified

    def _read_back_updated(self, layer, fids, with_geometry: bool = False,
                           names=None) -> Dict[int, object]:
        """The features *fids* as the provider stored them, by FID.

        Read on the provider (no edit buffer, no joined or virtual field),
        without geometry unless *with_geometry*, and only the fields *names*
        when given ([]: none). The caller passes one chunk at a time.
        Spatialite and PostgreSQL apply the layer filter even to a read by
        FID: an edit that moves a feature out of the filter hides it here;
        it is read again through a provider without the filter
        (`_read_back_updated_unfiltered`). Whatever neither read returns is
        left out: the caller keeps the value sent and says so. Measured
        cost: see scripts/validation/bench_capture.py (kind attr).
        """
        out: Dict[int, object] = {}
        provider = layer.dataProvider()
        if provider is None or not fids:
            return out
        try:
            request = _read_back_request(fids, with_geometry, names, provider.fields())
            for stored in provider.getFeatures(request):
                out[stored.id()] = stored
        except Exception as exc:  # noqa: BLE001 - the capture must survive
            flog(f"EditSessionTracker: read-back of the committed changes "
                 f"failed on {layer.id()} after {len(out)}/{len(fids)} "
                 f"feature(s): {type(exc).__name__}: {exc}", "ERROR")
        missed = [fid for fid in fids if fid not in out]
        if missed and (provider.subsetString() or ""):
            out.update(self._read_back_updated_unfiltered(layer, missed, with_geometry))
        return out

    @staticmethod
    def _read_back_updated_unfiltered(layer, fids, with_geometry: bool) -> Dict[int, object]:
        """Changed features the filtered provider hid, read without the filter.

        The second provider (`_unfiltered_provider`) is not the layer's: a
        feature it returns is kept only when its FID is the stored key
        (`crash_reconcile.fid_is_stored_key`), so that it is the row the
        commit wrote and not one a session numbering maps to the same FID.
        """
        from .crash_reconcile import fid_is_stored_key
        out: Dict[int, object] = {}
        other = _unfiltered_provider(layer)
        if other is None:
            return out
        refused = []
        try:
            pk = list(other.pkAttributeIndexes())
            rowid_fids = layer.providerType() in ("ogr", "spatialite")
            request = _read_back_request(fids, with_geometry, None, other.fields())
            for stored in other.getFeatures(request):
                if fid_is_stored_key(stored, pk, rowid_fids):
                    out[stored.id()] = stored
                else:
                    refused.append(stored.id())
        except Exception as exc:  # noqa: BLE001 - the capture must survive
            flog(f"EditSessionTracker: unfiltered read-back of the committed "
                 f"changes failed on {layer.id()}: {type(exc).__name__}: {exc}",
                 "ERROR")
        finally:
            del other
        flog(f"EditSessionTracker: {len(out)} changed feature(s) outside the "
             f"filter of {layer.id()} read back without it"
             + (f"; refused (FID not the stored key): {sorted(refused)[:20]}"
                if refused else ""),
             "WARNING" if refused else "INFO")
        return out

    def _announce_unverified_update(self, layer, layer_id: str, attempt,
                                    unverified: Dict[int, List[str]]) -> None:
        """Say which NEW values of which UPDATEs are the values sent, not read."""
        fields = sorted({n for names in unverified.values() for n in names})
        name = _safe_layer_name(layer) if layer is not None else layer_id
        flog(f"EditSessionTracker: event=update_values_unverified "
             f"layer={name!r} layer_id={layer_id} fids={sorted(unverified)[:20]} "
             f"(total={len(unverified)}) fields={fields}: the provider did not "
             f"return these features on read-back; the NEW side of their "
             f"UPDATE keeps what the edit buffer sent, which the source may "
             f"have stored differently", "ERROR")
        self._notify_status(
            "update_values_unverified", layer_id=layer_id, layer_name=name,
            commit_id=getattr(attempt, "commit_id", None),
            fids=sorted(unverified)[:50], n_features=len(unverified),
            fields=fields)

    def _on_rollback(self, layer_id: str) -> None:
        # When suppressed we deliberately keep the buffer in place: a
        # restore-driven rollback should not lose user data captured
        # before suppress() was called. The buffer is reaped on the
        # next session reset (force_unsuppress / disconnect_all).
        if not self._active or self.is_suppressed:
            flog(
                f"EditSessionTracker: signal=rollback "
                f"ignored=suppressed layer_id={layer_id}",
                "DEBUG",
            )
            return
        if self._layer_suppressed(layer_id):
            return
        # Rolling back after a refused commit only discards what is still in
        # the edit buffer. The stages the provider already wrote are on disk:
        # they are journalled here, never cleared with the rest.
        self._finalize_unfinished(layer_id, "rollback")
        buf = self._buffers.pop(layer_id, None)
        if buf is not None:
            buf.clear()
            flog(f"EditSessionTracker: rollback, buffer cleared for {layer_id}")

    _MEMORY_HARD_LIMIT_MB = 500

    def _capture_edit_buffer_state(self, layer, buf: EditSessionBuffer) -> list:
        """Read the edit buffer and capture originals from the provider.

        Guards against empty commits: if the edit buffer reports no deletions,
        no attribute changes, no geometry changes, and no additions, skip
        capture entirely. This avoids recording phantom events when a user
        opens edit mode and commits without modifying anything.

        Memory budget: the capture of the commit in progress always runs to
        completion. Cutting it short used to drop the modifications phase,
        and the pre-edit state of a modified feature exists nowhere else:
        the commit landed on disk with no way back. Blowing the budget is
        recorded on the buffer instead, and the tracker pauses once the
        events are out (see _pause_after_buffer_pressure).
        """
        edit_buf = layer.editBuffer()
        if edit_buf is None:
            return []
        self._warn_schema_destruction(layer, edit_buf)
        deleted_fids = edit_buf.deletedFeatureIds()
        changed_attrs = edit_buf.changedAttributeValues()
        changed_geoms = edit_buf.changedGeometries()
        added_feats = edit_buf.addedFeatures()
        flog(f"EditSessionTracker: edit buffer state on {buf.layer_id}: "
             f"deletes={len(deleted_fids)} "
             f"attr_changes={len(changed_attrs)} "
             f"geom_changes={len(changed_geoms)} "
             f"additions={len(added_feats)}")
        if not (deleted_fids or changed_attrs or changed_geoms or added_feats):
            flog(f"EditSessionTracker: empty edit buffer on {buf.layer_id}, "
                 "commit without modifications, skipping capture")
            return []
        provider = layer.dataProvider()
        field_names = self._capture_field_names(layer)
        self._capture_deletions(provider, edit_buf, buf, field_names)
        over_budget = self._check_buffer_pressure(buf)
        view = self._schema_view(layer, edit_buf, field_names)
        buf.schema_view = view
        self._capture_modifications(layer, provider, edit_buf, buf, field_names, view)
        over_budget = self._check_buffer_pressure(buf) or over_budget
        self._capture_additions(edit_buf, buf)
        if over_budget:
            buf.over_budget = True
        if view.get("pending"):
            self._rekey_snapshots(buf, view)
        # Which UPDATE snapshots read their geometry (the others were read
        # with NO_GEOMETRY): the prepare record must not pass "not read"
        # off as "no geometry".
        buf.geometry_changed_fids = set(changed_geoms.keys())
        return self._added_payload(added_feats, view.get("final_names") or field_names)

    @staticmethod
    def _origin_code(origin) -> int:
        value = getattr(origin, "value", origin)
        try:
            return int(value)
        except (TypeError, ValueError):
            return -1

    def _schema_view(self, layer, edit_buf, provider_names) -> dict:
        """How the edit buffer's field indexes map onto names, before commit.

        index_names  layer field names by LAYER index (what the edit buffer's
                     changedAttributeValues keys refer to)
        final_names  the provider schema once this commit lands (provider
                     fields minus pending deletions, renamed in place, plus
                     pending additions): the names events are keyed by
        name_map     pre-commit provider name -> final name
        pending      True when a deletion, rename or addition is pending

        A pending field deletion shifts every later index: mapping them
        onto the pre-commit provider names recorded the wrong field, and
        the delta over those names came back empty, so the UPDATE was
        dropped as a "false commit" while the file changed.
        """
        view = {"index_names": list(provider_names), "final_names": list(provider_names),
                "name_map": {n: n for n in provider_names}, "pending": False}
        # renamedAttributes() is not exposed to Python: renames are read
        # from the field origins instead (a provider field whose name
        # differs from the provider's name at its origin index).
        try:
            deleted = list(edit_buf.deletedAttributeIds())
        except (AttributeError, RuntimeError):
            deleted = []
        try:
            added = list(edit_buf.addedAttributes())
        except (AttributeError, RuntimeError):
            added = []
        try:
            fields = layer.fields()
            prov_fields = layer.dataProvider().fields()
        except (AttributeError, RuntimeError):
            return view
        index_names, final_names, name_map = [], [], {}
        renamed = {}
        for i in range(fields.count()):
            name = fields.at(i).name()
            index_names.append(name)
            origin = self._origin_code(fields.fieldOrigin(i))
            if origin == 1:       # provider field (possibly renamed)
                pidx = fields.fieldOriginIndex(i)
                if 0 <= pidx < prov_fields.count():
                    pname = prov_fields.at(pidx).name()
                    name_map[pname] = name
                    if pname != name:
                        renamed[pname] = name
                final_names.append(name)
            elif origin == 3:     # added in this edit session
                final_names.append(name)
        if not (deleted or added or renamed):
            return view
        view.update(index_names=index_names, final_names=final_names,
                    name_map=name_map, pending=True)
        flog(f"EditSessionTracker: pending schema change on {layer.id()}: "
             f"deleted={deleted} renamed={renamed} added={len(added)} "
             f"final_names={final_names}")
        return view

    @staticmethod
    def _rekey_snapshots(buf, view) -> None:
        """Key every OLD snapshot by the post-commit field names."""
        name_map = view["name_map"]
        final = view["final_names"]
        for snap in list(buf.get_modified_snapshots().values()) + \
                list(buf.get_deleted_snapshots().values()):
            rekeyed = {}
            for old_name, value in (snap.attributes or {}).items():
                new_name = name_map.get(old_name)
                if new_name is not None:
                    rekeyed[new_name] = value
            for name in final:
                rekeyed.setdefault(name, None)
            snap.attributes = rekeyed
            snap.field_names = list(final)

    @staticmethod
    def _added_payload(added_feats, field_names) -> list:
        """Content of the features about to be inserted, for the prepare record.

        Their final FID is only known after the commit; the content is what
        lets a post-crash reconciliation recognise them in the provider.
        A field the form left to the source (QgsUnsetAttributeValue) is
        recorded as None, which the reconciliation does not compare (its
        placeholder text matched no feature, and an insert that had landed
        was declared "not applied"), and named in ``unset_fields``: None
        alone cannot tell "left to the source" from a NULL the user typed.
        What the remaining content proves is decided by
        `_mark_unproven_additions`.
        """
        out = []
        for tmp_fid, feat in added_feats.items():
            try:
                wkb = geometry_to_wkb(feat.geometry())
                masked = _UnsetMasked(feat)
                out.append({
                    "tmp_fid": int(tmp_fid),
                    "attributes": serialize_attributes(masked, field_names),
                    "unset_fields": masked.unset_names(field_names),
                    "geometry_wkb": _b64(wkb),
                    "_wkb": wkb,
                })
            except Exception as exc:  # noqa: BLE001 - never blocks the commit
                out.append({"tmp_fid": int(tmp_fid), "error": str(exc),
                            "unproven": "payload_error"})
        return out

    # Budget of the twin search, per commit (see _mark_unproven_additions).
    # Measured on a 100 000-point GeoPackage: ~270 us per bounding-box query,
    # ~160 ms for a full scan; a batch of additions is read in one query on
    # the union of their boxes when it holds few enough features. Past the
    # budget an addition is left unchecked, which makes it unprovable after
    # a crash, never matched.
    _TWIN_BBOX_QUERIES = 500
    _TWIN_UNION_MIN = 4
    _TWIN_UNION_CANDIDATES = 20000
    _TWIN_SCAN_FEATURES = 20000
    _TWIN_POST_COMPARISONS = 50000
    _TWIN_FIDS_KEPT = 20

    def _mark_unproven_additions(self, layer, entries, buf) -> None:
        """Flag each addition whose content does not designate it alone.

        After a crash, `crash_reconcile` recognises an addition in the
        provider by its content (attributes, geometry): the FID it will get
        is unknown before the commit. A pasted or duplicated feature has
        the content of a feature that ALREADY exists (the fields left to
        the source, the key among them, are not compared): if QGIS stops
        before the provider writes, the one feature holding that content
        is the original, the reconciliation took it for the insert that
        landed, and undoing that INSERT deleted the user's original
        (measured: STRONG, 'Deleted (undo insert)').

        Here, before the provider writes, every addition is searched for in
        what exists: the provider features (through a provider without the
        layer filter: a twin hidden by the filter today is visible after a
        restart without it) and the features this commit modifies, in the
        state the commit gives them. The reconciliation matches an addition
        as the source stores it (a number rounded to the field, a text
        without its trailing spaces); the comparison here is wider than its
        own (`crash_reconcile.twin_matcher`), so a twin it would match is
        found here, a feature this commit is about to write included (its
        value is stored the same way). Every checked addition gets
        ``twins`` (their FIDs, the first _TWIN_FIDS_KEPT), ``n_twins`` (how
        many) and ``rounding`` (the fields the source will round, which
        also says the twins were searched this way); one with a twin, or
        that could not be checked in the budget, also gets ``unproven``
        (reason; ``twins`` None when unchecked). The reconciliation sets
        the listed twins aside by FID and judges what remains; an
        unchecked or cut list can never make it say "landed".
        """
        live = [e for e in entries if "unproven" not in e]
        if not live:
            return
        provider = None
        owned = False
        try:
            from .crash_reconcile import (
                rounding_rules, storage_decimals, twin_matcher,
            )
            base = layer.dataProvider()
            provider = base
            if base is not None and (base.subsetString() or ""):
                provider, owned = _unfiltered_provider(layer), True
            if provider is None:
                raise RuntimeError("no provider without the layer filter")
            decimals = storage_decimals(provider.fields())
            self._search_twins(provider, live, buf, twin_matcher(decimals))
            for entry in live:
                if isinstance(entry.get("twins"), list):
                    entry["rounding"] = rounding_rules(entry.get("attributes"),
                                                       decimals)
        except Exception as exc:  # noqa: BLE001 - never blocks the commit
            flog(f"EditSessionTracker: twin search for the additions on "
                 f"{_safe_layer_name(layer)!r} failed ({type(exc).__name__}: "
                 f"{exc}); they are recorded as unprovable after a crash",
                 "WARNING")
            for entry in live:
                entry.setdefault("unproven", "twin_check_failed")
                entry.setdefault("twins", None)
        finally:
            if owned:
                del provider
        n_unproven = sum(1 for e in entries if "unproven" in e)
        if n_unproven:
            flog(f"EditSessionTracker: event=additions_unprovable_by_content "
                 f"layer={_safe_layer_name(layer)!r} n={n_unproven}/{len(entries)} "
                 f"reasons={sorted({e['unproven'] for e in entries if 'unproven' in e})}: "
                 f"a crash during this commit leaves them unresolved, never "
                 f"attributed to an existing feature", "INFO")

    def _search_twins(self, provider, entries, buf, same) -> None:
        """Fill ``twins`` / ``unproven`` on *entries* (see caller)."""
        from qgis.core import QgsFeatureRequest, QgsGeometry
        geom_fids = getattr(buf, "geometry_changed_fids", set()) or set()
        post_attrs, post_moved = {}, []
        for snap in buf.get_modified_snapshots().values():
            attrs = snap.new_attributes if snap.new_attributes is not None \
                else snap.attributes
            post_attrs[snap.fid] = attrs
            if snap.fid in geom_fids:
                post_moved.append(_PostState(snap.fid, attrs, snap.new_geometry_wkb))

        def matches(feat, known, wkb) -> bool:
            if same(feat, known, wkb):
                return True
            fid = feat.id()
            # Modified by this commit, geometry untouched: its state after
            # the commit is the provider geometry with the new attributes.
            return (fid in post_attrs and fid not in geom_fids
                    and same(_PostState(fid, post_attrs[fid], feat.geometry()),
                             known, wkb))

        geo, scan_entries = [], []
        for entry in entries:
            entry["_known"] = {k: v for k, v in (entry.get("attributes") or {}).items()
                               if v is not None}
            entry["_twins"] = set()
            wkb = entry.get("_wkb")
            geom = QgsGeometry()
            if wkb:
                geom.fromWkb(bytes(wkb))
            if geom.isNull():
                scan_entries.append(entry)
                continue
            # Wider than the reconciliation's own box: a superset of what
            # it can match.
            bb = geom.boundingBox()
            bb.grow(max(bb.width(), bb.height(), 1.0) * 1e-3 + 1e-6)
            geo.append((entry, bb))
        if not (len(geo) >= self._TWIN_UNION_MIN
                and self._twins_in_union(provider, geo, matches)):
            for n, (entry, bb) in enumerate(geo):
                if n >= self._TWIN_BBOX_QUERIES:
                    entry["unproven"], entry["twins"] = "twin_check_budget", None
                    continue
                for feat in provider.getFeatures(QgsFeatureRequest().setFilterRect(bb)):
                    if matches(feat, entry["_known"], entry["_wkb"]):
                        entry["_twins"].add(feat.id())
        if scan_entries:
            count = provider.featureCount()
            if count < 0 or count * len(scan_entries) > self._TWIN_SCAN_FEATURES:
                for entry in scan_entries:
                    entry["unproven"], entry["twins"] = "twin_check_budget", None
            else:
                for feat in provider.getFeatures(QgsFeatureRequest()):
                    for entry in scan_entries:
                        if matches(feat, entry["_known"], None):
                            entry["_twins"].add(feat.id())
        checked = [e for e in entries if "_twins" in e and "unproven" not in e]
        # Modified by this commit AND moved: their new geometry is only in
        # the edit buffer, compared in memory.
        if post_moved and len(post_moved) * len(checked) > self._TWIN_POST_COMPARISONS:
            for entry in checked:
                entry["unproven"], entry["twins"] = "twin_check_budget", None
            checked = []
        for entry in checked:
            for post in post_moved:
                if same(post, entry["_known"], entry.get("_wkb")):
                    entry["_twins"].add(post.id())
            twins = sorted(entry["_twins"])
            entry["twins"] = twins[:self._TWIN_FIDS_KEPT]
            # The reconciliation sets a twin aside by its FID: a cut list
            # must say so, an unlisted twin would pass for the addition.
            entry["n_twins"] = len(twins)
            if twins:
                entry["unproven"] = "content_matches_existing_features"

    def _twins_in_union(self, provider, geo, matches) -> bool:
        """One provider read for a batch of additions (a paste, a split).

        The features in the union of their boxes are read once and each is
        compared only with the additions whose box it meets (spatial index
        of the additions). False, nothing recorded, when that union holds
        more than _TWIN_UNION_CANDIDATES features (additions spread over the
        whole layer): the caller then queries addition by addition.
        """
        from qgis.core import QgsFeatureRequest, QgsRectangle, QgsSpatialIndex
        union = QgsRectangle(geo[0][1])
        index = QgsSpatialIndex()
        for i, (_entry, bb) in enumerate(geo):
            union.combineExtentWith(bb)
            index.addFeature(i, bb)
        found = []
        request = QgsFeatureRequest().setFilterRect(union)
        for n, feat in enumerate(provider.getFeatures(request)):
            if n >= self._TWIN_UNION_CANDIDATES:
                return False
            geom = feat.geometry()
            if geom is None or geom.isNull():
                continue
            for i in index.intersects(geom.boundingBox()):
                entry = geo[i][0]
                if matches(feat, entry["_known"], entry["_wkb"]):
                    found.append((entry, feat.id()))
        for entry, fid in found:
            entry["_twins"].add(fid)
        return True

    def _warn_schema_destruction(self, layer, edit_buf) -> None:
        """A field deletion destroys a column of values RecoverLand cannot restore.

        The journal records feature operations, not schema operations: the
        values of a deleted field are gone from the file once the commit
        lands, and no event carries them. That is a limit of the product,
        and it must be said at the moment it bites, not discovered the day
        a rewind comes back without the column.
        """
        try:
            deleted = list(edit_buf.deletedAttributeIds())
        except (AttributeError, RuntimeError):
            return
        if not deleted:
            return
        try:
            provider_fields = layer.dataProvider().fields()
            names = [provider_fields.at(i).name() for i in deleted
                     if 0 <= i < provider_fields.count()]
        except (AttributeError, RuntimeError):
            names = [str(i) for i in deleted]
        self._notify_status("schema_change_not_journaled",
                            layer_id=layer.id(), layer_name=_safe_layer_name(layer),
                            fields=names)

    @staticmethod
    def _capture_field_names(layer) -> List[str]:
        """Field names to capture: the PROVIDER's, never the layer's.

        ``layer.fields()`` also carries joined and virtual fields, which
        the datasource does not store. The OLD state is read from the
        provider (no value for them) and the NEW state from the layer
        (which resolves them), so capturing them turns a plain vertex
        move into a phantom attribute change, and the restore then
        computes a field index the provider does not have.
        """
        provider = layer.dataProvider()
        if provider is None:
            return [f.name() for f in layer.fields()]
        return [f.name() for f in provider.fields()]

    def _check_buffer_pressure(self, buf: EditSessionBuffer) -> bool:
        """Log warning if buffer exceeds soft threshold; return True if hard limit hit."""
        if buf.approx_memory_mb > self._MEMORY_HARD_LIMIT_MB:
            flog(f"EditSessionTracker: buffer hard limit reached "
                 f"({buf.approx_memory_mb:.0f} MB, {buf.total_tracked} features) "
                 f"on {buf.layer_id}, finishing this commit then pausing "
                 f"capture", "ERROR")
            return True
        if buf.needs_flush():
            flog(f"EditSessionTracker: buffer pressure warning "
                 f"({buf.approx_memory_mb:.0f} MB, {buf.total_tracked} features) "
                 f"on {buf.layer_id}", "WARNING")
        return False

    def _capture_deletions(self, provider, edit_buf, buf, field_names) -> None:
        from qgis.core import QgsFeatureRequest
        deleted_fids = list(edit_buf.deletedFeatureIds())
        if not deleted_fids:
            return
        flog(f"EditSessionTracker: capturing {len(deleted_fids)} deletions, "
             f"FIDs={deleted_fids[:20]}")
        request = QgsFeatureRequest().setFilterFids(deleted_fids)
        captured = 0
        layer = self._connected_layers.get(buf.layer_id)
        for feature in provider.getFeatures(request):
            snapshot = create_snapshot_from_feature(feature, field_names)
            if layer is not None:
                # Same stub-based identity the DELETE event always used,
                # computed now: the layer may be gone when the attempt is
                # finalised (layer removed right after a refused commit).
                snapshot.identity_json = compute_feature_identity(
                    layer, _stub_feature(snapshot))
            buf.record_deletion(snapshot)
            captured += 1
        if captured != len(deleted_fids):
            flog(f"EditSessionTracker: deletion capture mismatch: "
                 f"expected={len(deleted_fids)} captured={captured}", "WARNING")

    def _capture_modifications(self, layer, provider, edit_buf, buf, field_names,
                               view=None) -> None:
        """Capture OLD state from the provider (pre-commit).

        NEW state is filled later from the authoritative QGIS post-commit
        signals (committedAttributeValuesChanges, committedGeometriesChanges)
        in _generate_events. We keep a projected NEW from layer+buffer as a
        fallback in case a provider does not emit those signals (some
        non-spatial providers historically did not).

        Newly-added features (with temporary FIDs not yet known to the
        provider) are excluded here: they will be recorded as INSERT events
        from committedFeaturesAdded which carries the post-commit fid.
        """
        from qgis.core import QgsFeatureRequest
        changed_attrs = edit_buf.changedAttributeValues()
        changed_geoms = edit_buf.changedGeometries()
        deleted_set = set(edit_buf.deletedFeatureIds())
        added_set = set(edit_buf.addedFeatures().keys())

        geom_fids = set(changed_geoms.keys()) - deleted_set - added_set
        attr_only_fids = (set(changed_attrs.keys()) - deleted_set - added_set - geom_fids)

        excluded_added = (set(changed_geoms) | set(changed_attrs)) & added_set
        excluded_deleted = (set(changed_geoms) | set(changed_attrs)) & deleted_set
        flog(f"EditSessionTracker: capture_modifications {buf.layer_id} "
             f"geom_fids={sorted(geom_fids)} "
             f"attr_only_fids={sorted(attr_only_fids)} "
             f"excluded_added={sorted(excluded_added)} "
             f"excluded_deleted={sorted(excluded_deleted)}")

        # NEW geometry is read only for the features whose geometry the
        # edit buffer actually changed. Attribute-only features get their
        # OLD state read with NO_GEOMETRY below; reading a NEW geometry
        # for them made _make_update_event compare None against the
        # current position, so every attribute edit was journalled as a
        # geometry change that never happened - and the false-commit
        # filter, which needs geom_changed to be False, never fired.
        index_names = (view or {}).get("index_names") or field_names
        new_names = (view or {}).get("final_names") or field_names
        new_state_by_fid = self._capture_new_state(
            layer, geom_fids, new_names)
        new_state_by_fid.update(self._capture_new_state(
            layer, attr_only_fids, new_names, with_geometry=False))

        captured_geom: set = set()
        if geom_fids:
            request = QgsFeatureRequest().setFilterFids(list(geom_fids))
            for feature in provider.getFeatures(request):
                captured_geom.add(feature.id())
                changed_names = self._extract_changed_field_names(
                    feature.id(), changed_attrs, index_names)
                snapshot = create_snapshot_from_feature(
                    feature, field_names, changed_names)
                self._attach_new_state_and_identity(
                    snapshot, layer, feature, new_state_by_fid)
                buf.record_modification(snapshot)
                new_attrs_proj, new_wkb_proj = new_state_by_fid.get(
                    feature.id(), (None, None))
                flog(f"TRACE_CAPTURE: layer={buf.layer_id} fid={feature.id()} "
                     f"OLD_geom={wkb_short_repr(snapshot.geometry_wkb)} "
                     f"NEW_geom_projected={wkb_short_repr(new_wkb_proj)}")
            missing = geom_fids - captured_geom
            if missing:
                flog(f"EditSessionTracker: geom-modification OLD UNCAPTURED "
                     f"layer={buf.layer_id} fids={sorted(missing)} "
                     f"(provider lookup returned no row; UPDATE events for "
                     f"these fids cannot be generated)", "ERROR")

        captured_attr: set = set()
        if attr_only_fids:
            from ..compat import QgisCompat
            request = QgsFeatureRequest().setFilterFids(list(attr_only_fids))
            request.setFlags(QgisCompat.NO_GEOMETRY)
            for feature in provider.getFeatures(request):
                captured_attr.add(feature.id())
                changed_names = self._extract_changed_field_names(
                    feature.id(), changed_attrs, index_names)
                snapshot = create_snapshot_from_feature(
                    feature, field_names, changed_names)
                self._attach_new_state_and_identity(
                    snapshot, layer, feature, new_state_by_fid)
                buf.record_modification(snapshot)
            missing = attr_only_fids - captured_attr
            if missing:
                flog(f"EditSessionTracker: attr-modification OLD UNCAPTURED "
                     f"layer={buf.layer_id} fids={sorted(missing)} "
                     f"(provider lookup returned no row; UPDATE events for "
                     f"these fids cannot be generated)", "ERROR")

    @staticmethod
    def _capture_new_state(layer, fids, field_names,
                           with_geometry: bool = True) -> Dict[int, tuple]:
        """Return {fid: (new_attrs_dict, new_geom_wkb)} read from layer+buffer.

        ``with_geometry=False`` leaves the NEW geometry at None for the
        features whose geometry the user never touched, so the UPDATE
        event carries no geometry at all instead of a fake OLD=None ->
        NEW=current transition.
        """
        if not fids:
            return {}
        from qgis.core import QgsFeatureRequest
        result: Dict[int, tuple] = {}
        request = QgsFeatureRequest().setFilterFids(list(fids))
        if not with_geometry:
            from ..compat import QgisCompat
            request.setFlags(QgisCompat.NO_GEOMETRY)
        for feature in layer.getFeatures(request):
            attrs = serialize_attributes(feature, field_names)
            wkb = geometry_to_wkb(feature.geometry()) if with_geometry else None
            result[feature.id()] = (attrs, wkb)
        missing = set(fids) - set(result.keys())
        if missing:
            flog(f"EditSessionTracker: _capture_new_state missing fids "
                 f"layer={layer.id()} fids={sorted(missing)} "
                 f"(post-commit lookup will fail; identity may be stale)",
                 "WARNING")
        return result

    @staticmethod
    def _attach_new_state_and_identity(snapshot, layer, old_feature,
                                       new_state_by_fid) -> None:
        """Populate snapshot.new_* and snapshot.identity_json.

        Identity is computed from the OLD feature (pre-commit). For PK-stable
        providers it carries the PK which survives commit. For FID-only
        providers it carries the pre-commit FID; restore falls back to
        post-state geometry lookup when the FID is unstable.
        """
        new_attrs, new_wkb = new_state_by_fid.get(snapshot.fid, (None, None))
        snapshot.new_attributes = new_attrs
        snapshot.new_geometry_wkb = new_wkb
        snapshot.identity_json = compute_feature_identity(layer, old_feature)

    @staticmethod
    def _extract_changed_field_names(fid: int, changed_attrs: Dict,
                                     field_names: List[str]) -> List[str]:
        changed = changed_attrs.get(fid, {})
        names = []
        for idx in sorted(changed.keys()):
            if 0 <= idx < len(field_names):
                names.append(field_names[idx])
        return names

    @staticmethod
    def _capture_additions(edit_buf, buf) -> None:
        for fid in edit_buf.addedFeatures().keys():
            buf.record_addition(fid)

    def _generate_events(self, attempt, authoritative_only: bool = False) -> List[AuditEvent]:
        """Build AuditEvent objects for one commit attempt.

        ``authoritative_only`` (a commit that did not reach
        afterCommitChanges): an event is emitted only for what the provider
        CONFIRMED through committedFeaturesRemoved,
        committedAttributeValuesChanges, committedGeometriesChanges and
        committedFeaturesAdded. The pre-commit projection read from the edit
        buffer describes what the user asked for, not what was written: on
        a partial commit the two differ, and journalling the projection
        would record values the data never had.

        Otherwise (full success):
          * DELETE events are emitted only for fids confirmed by
            committedFeaturesRemoved (when at least one such signal was
            received; otherwise we trust the pre-commit deletion buffer).
          * UPDATE events use NEW state from committedAttributeValuesChanges
            plus committedGeometriesChanges when present, falling back to
            the pre-commit projection captured via layer.getFeatures.
        """
        buf = attempt.buf
        meta = attempt.meta
        layer_id = meta["layer_id"]
        net = buf.compute_net_effect()
        events: List[AuditEvent] = []
        ds_fp = meta["ds_fp"]
        proj_fp = meta["proj_fp"]
        layer_name = meta["layer_name"]
        provider_type = meta["provider_type"]
        user = meta["user"]
        now = datetime.now(timezone.utc).isoformat()
        field_schema = meta["field_schema"]
        geom_type, crs = meta["geom_type"], meta["crs"]
        field_names = meta["field_names"]
        session_id = buf.session_id
        commit_id = attempt.commit_id

        committed_deletions = buf.get_committed_deletions()
        committed_geom_changes = buf.get_committed_geom_changes()
        committed_attr_changes = buf.get_committed_attr_changes()

        if authoritative_only:
            deleted_to_emit = net["deleted"] & committed_deletions
        else:
            deleted_to_emit = (net["deleted"] & committed_deletions
                               if committed_deletions else net["deleted"])
        rejected_deletions = net["deleted"] - deleted_to_emit
        if rejected_deletions:
            flog(f"EditSessionTracker: deletions not confirmed by the provider "
                 f"layer={buf.layer_id} fids={sorted(rejected_deletions)[:50]} "
                 f"(buffer requested but no committedFeaturesRemoved fired)",
                 "WARNING")

        for fid in deleted_to_emit:
            snap = buf.get_deleted_snapshots().get(fid)
            if snap is None:
                continue
            events.append(self._make_delete_event(
                snap, ds_fp, proj_fp, layer_id, layer_name,
                provider_type, user, now, field_schema, geom_type, crs,
                session_id,
            ))

        for fid in net["modified"]:
            snap = buf.get_modified_snapshots().get(fid)
            if snap is None:
                continue
            event = self._make_update_event(
                snap, layer_id, fid, ds_fp, proj_fp, layer_name,
                provider_type, user, now, field_schema, geom_type, crs,
                session_id,
                field_names,
                committed_geom_changes.get(fid, _SENTINEL),
                committed_attr_changes.get(fid),
                authoritative_only=authoritative_only,
            )
            if event is not None:
                events.append(event)

        committed = buf.get_committed_additions()
        flog(f"EditSessionTracker: generating events: commit={commit_id} "
             f"authoritative_only={authoritative_only} "
             f"net_deleted={len(net['deleted'])} "
             f"committed_deletions={len(committed_deletions)} "
             f"emitting_deletes={len(deleted_to_emit)} "
             f"net_modified={len(net['modified'])} "
             f"committed_geom_changes={len(committed_geom_changes)} "
             f"committed_attr_changes={len(committed_attr_changes)} "
             f"net_added={len(net['added'])} "
             f"committed_additions={len(committed)}")
        if committed:
            for ca in committed:
                events.append(self._make_insert_event_from_committed(
                    ca, ds_fp, proj_fp, layer_id, layer_name,
                    provider_type, user, now, field_schema, geom_type, crs,
                    session_id,
                ))
        elif not authoritative_only:
            layer = self._connected_layers.get(layer_id)
            for fid in net["added"]:
                event = None
                if layer is not None:
                    event = self._make_insert_event(
                        layer, fid, ds_fp, proj_fp, layer_name,
                        provider_type, user, now, field_schema, geom_type, crs,
                        session_id,
                    )
                if event is not None:
                    events.append(event)
                else:
                    flog(f"EditSessionTracker: INSERT event dropped for "
                         f"FID={fid}, feature not found", "WARNING")

        return [e._replace(commit_id=commit_id) for e in events]

    @staticmethod
    def _get_layer_geometry_info(layer):
        from .geometry_utils import extract_geometry_type, extract_crs_authid
        return extract_geometry_type(layer), extract_crs_authid(layer)

    @staticmethod
    def _make_delete_event(snap, ds_fp, proj_fp, layer_id, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id) -> AuditEvent:
        attrs_json = build_full_snapshot(snap.attributes)
        identity = snap.identity_json or f'{{"fid": {snap.fid}}}'
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer_id,
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=identity, operation_type="DELETE",
            attributes_json=attrs_json, geometry_wkb=snap.geometry_wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=None,
        )

    @staticmethod
    def _make_update_event(snap, layer_id, fid, ds_fp, proj_fp, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id,
                           field_names: List[str],
                           committed_geom,
                           committed_attrs,
                           authoritative_only: bool = False) -> Optional[AuditEvent]:
        """Build an UPDATE event from OLD snapshot + NEW state.

        Sources for NEW (in priority order):
          1. Authoritative committed_geom / committed_attrs from QGIS post-
             commit signals when present (provider confirmed the change).
          2. Pre-commit projection from snap.new_attributes / new_geometry_wkb
             (read from layer+buffer before commit) as a fallback for
             providers that do not emit committed*Changes signals.

        Returns None if neither source provides any NEW state (the buffer
        thought the feature was modified but the provider applied nothing).
        """
        if authoritative_only:
            # Partial commit: a stage the provider did not confirm did not
            # happen. Unchanged geometry and unchanged attributes, not the
            # projection of what the user had asked for.
            if committed_geom is _SENTINEL:
                new_wkb, geom_authoritative = snap.geometry_wkb, False
            else:
                new_wkb, geom_authoritative = committed_geom, True
            if committed_attrs:
                new_attrs, attrs_authoritative = EditSessionTracker._resolve_new_attrs(
                    snap, committed_attrs, field_names)
            else:
                new_attrs, attrs_authoritative = dict(snap.attributes or {}), False
        else:
            new_wkb, geom_authoritative = EditSessionTracker._resolve_new_geom(
                snap, committed_geom)
            new_attrs, attrs_authoritative = EditSessionTracker._resolve_new_attrs(
                snap, committed_attrs, field_names)

        if new_attrs is None and new_wkb is None and not geom_authoritative:
            flog(f"_make_update_event: no NEW state for fid={fid} "
                 f"(neither post-commit signal nor pre-commit projection), "
                 f"dropping UPDATE event", "WARNING")
            return None

        old_attrs = snap.attributes
        new_attrs = new_attrs if new_attrs is not None else old_attrs
        changed_field_names = snap.changed_field_names or snap.field_names
        delta_json = compute_update_delta(
            old_attrs, new_attrs, changed_field_names)
        geom_changed = not geometries_equal(snap.geometry_wkb, new_wkb)

        if delta_json is None and not geom_changed:
            return None
        if delta_json is None:
            delta_json = '{"changed_only": {}}'
        identity = snap.identity_json or f'{{"fid": {fid}}}'

        flog(f"TRACE_EVENT: layer={layer_id} fid={fid} op=UPDATE "
             f"geom_source={'authoritative' if geom_authoritative else 'projected'} "
             f"attrs_source={'authoritative' if attrs_authoritative else 'projected'} "
             f"geom_changed={geom_changed} "
             f"OLD={wkb_short_repr(snap.geometry_wkb)} "
             f"NEW={wkb_short_repr(new_wkb)} "
             f"identity={identity}")

        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer_id,
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=identity, operation_type="UPDATE",
            attributes_json=delta_json,
            geometry_wkb=(_null_marked(snap.geometry_wkb) if geom_changed else None),
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=(_null_marked(new_wkb) if geom_changed else None),
        )

    @staticmethod
    def _resolve_new_geom(snap, committed_geom):
        """Pick the authoritative NEW geometry WKB if available.

        Returns (wkb_or_None, is_authoritative).
        """
        if committed_geom is not _SENTINEL:
            return committed_geom, True
        return snap.new_geometry_wkb, False

    @staticmethod
    def _resolve_new_attrs(snap, committed_attrs,
                           field_names: List[str]):
        """Compose NEW attribute dict from OLD + authoritative committed deltas.

        committed_attrs is {idx: value}. We start from the OLD attrs and
        overlay each idx with its committed value. When the post-commit
        signal does not carry attribute changes for this fid we fall back
        to snap.new_attributes captured pre-commit (may be None).

        Returns (attrs_dict_or_None, is_authoritative).
        """
        if not committed_attrs:
            return snap.new_attributes, False
        merged = dict(snap.attributes or {})
        for idx, value in committed_attrs.items():
            if 0 <= idx < len(field_names):
                from .serialization import serialize_value
                merged[field_names[idx]] = serialize_value(value)
        return merged, True

    @staticmethod
    def _make_insert_event(layer, fid, ds_fp, proj_fp, layer_name,
                           provider_type, user, now, field_schema,
                           geom_type, crs, session_id) -> Optional[AuditEvent]:
        from qgis.core import QgsFeatureRequest
        request = QgsFeatureRequest(fid)
        feature = next(layer.dataProvider().getFeatures(request), None)
        if feature is None:
            return None
        field_names = EditSessionTracker._capture_field_names(layer)
        attrs = serialize_attributes(feature, field_names)
        attrs_json = build_full_snapshot(attrs)
        wkb = geometry_to_wkb(feature.geometry())
        identity = compute_feature_identity(layer, feature)
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer.id(),
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=identity, operation_type="INSERT",
            attributes_json=attrs_json, geometry_wkb=wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=compute_entity_fingerprint(identity),
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=wkb,
        )

    @staticmethod
    def _make_insert_event_from_committed(ca, ds_fp, proj_fp, layer_id,
                                          layer_name, provider_type, user,
                                          now, field_schema, geom_type,
                                          crs, session_id) -> AuditEvent:
        """Build INSERT event from data captured by committedFeaturesAdded."""
        wkb = ca["geometry_wkb"]
        return AuditEvent(
            event_id=None, project_fingerprint=proj_fp,
            datasource_fingerprint=ds_fp, layer_id_snapshot=layer_id,
            layer_name_snapshot=layer_name, provider_type=provider_type,
            feature_identity_json=ca["identity_json"],
            operation_type="INSERT",
            attributes_json=ca["attrs_json"], geometry_wkb=wkb,
            geometry_type=geom_type, crs_authid=crs,
            field_schema_json=field_schema, user_name=user,
            session_id=session_id, created_at=now,
            restored_from_event_id=None,
            entity_fingerprint=ca["entity_fingerprint"],
            event_schema_version=CURRENT_SCHEMA_VERSION,
            new_geometry_wkb=wkb,
        )


class _StubFeature:
    """Minimal stand-in for compute_feature_identity when using a snapshot."""

    def __init__(self, fid, attrs):
        self._fid = fid
        self._attrs = attrs

    def id(self):
        return self._fid

    def geometry(self):
        return None

    def __getitem__(self, key):
        return self._attrs.get(key)


def _stub_feature(snap: FeatureSnapshot):
    return _StubFeature(snap.fid, snap.attributes)


def _is_unset(value) -> bool:
    """True for QgsUnsetAttributeValue: "the provider decides this value"."""
    return type(value).__name__ == "QgsUnsetAttributeValue"


class _UnsetMasked:
    """A feature read through, its QgsUnsetAttributeValue fields as NULL.

    Used where the provider's choice for those fields is not known: the
    placeholder must never reach a snapshot or an identity (a key reading
    '<QgsUnsetAttributeValue: Autogenerate>' designates nothing). Reading
    NULL keeps it out of the identity; the caller decides whether the
    field is left out of the snapshot (INSERT) or not compared (prepare).
    """

    def __init__(self, feature):
        self._feature = feature

    def id(self):
        return self._feature.id()

    def geometry(self):
        return self._feature.geometry()

    def attribute(self, name):
        value = self._feature.attribute(name)
        return None if _is_unset(value) else value

    def __getitem__(self, name):
        return self.attribute(name)

    def unset_names(self, field_names) -> List[str]:
        out = []
        for name in field_names:
            try:
                if _is_unset(self._feature.attribute(name)):
                    out.append(name)
            except (KeyError, IndexError):
                continue
        return out


def _null_marked(wkb):
    """A changed geometry side that is NULL is stored as the explicit marker."""
    from .constants import GEOMETRY_NULL_MARKER
    return GEOMETRY_NULL_MARKER if wkb is None else wkb


def _b64(data) -> Optional[str]:
    if data is None:
        return None
    import base64
    return "b64:" + base64.b64encode(bytes(data)).decode("ascii")


# How an unverified geometry is named among the fields of the
# update_values_unverified status (no field can hold this name).
_GEOMETRY_FIELD_LABEL = "(geometry)"


def _read_back_request(fids, with_geometry: bool, names, fields):
    """Provider request for the read-back of committed changes.

    *names* None: every field; [] none; otherwise those fields only.
    """
    from qgis.core import QgsFeatureRequest
    request = QgsFeatureRequest().setFilterFids(list(fids))
    if not with_geometry:
        from ..compat import QgisCompat
        request.setFlags(QgisCompat.NO_GEOMETRY)
    if names is not None:
        if names:
            request.setSubsetOfAttributes(list(names), fields)
        else:
            request.setNoAttributes()
    return request


def _snapshot_record(kind: str, snap: FeatureSnapshot, geometry_changed_fids=None) -> dict:
    """JSON-safe pre-edit state of one feature, for the prepare record.

    An UPDATE whose geometry the commit changes follows the event rule of
    schema 7: a NULL side is the explicit GEOMETRY_NULL_MARKER, not None,
    which reads "geometry unchanged" (a reconciled "geometry added" whose
    old side was None could not be undone: 'Reverted', and the geometry
    stayed). An UPDATE of attributes only never read its geometry
    (NO_GEOMETRY): ``geometry_captured`` False says so, its None is "not
    read", not "no geometry". *geometry_changed_fids* None: not known.
    """
    old_wkb, new_wkb = snap.geometry_wkb, snap.new_geometry_wkb
    record = {
        "kind": kind,
        "fid": snap.fid,
        "identity": snap.identity_json,
        "attributes": snap.attributes,
        "changed_fields": list(snap.changed_field_names or []),
        "new_attributes": snap.new_attributes,
    }
    if kind == "UPDATE" and geometry_changed_fids is not None:
        changed = snap.fid in geometry_changed_fids
        record["geometry_captured"] = changed
        if changed:
            old_wkb = _null_marked(old_wkb)
            # The projected NEW side exists only if it was read.
            if snap.new_attributes is not None:
                new_wkb = _null_marked(new_wkb)
    record["geometry_wkb"] = _b64(old_wkb)
    record["new_geometry_wkb"] = _b64(new_wkb)
    return record


# Prepare-record kind of an addition that its content does not designate
# alone (see EditSessionTracker._mark_unproven_additions).
UNPROVEN_INSERT_KIND = "INSERT_UNPROVEN"


class _PostState:
    """A feature as a commit will leave it, for the twin search.

    Answers what `crash_reconcile.twin_matcher` asks of a feature: its id, a
    value by field name (KeyError when the name is unknown: not compared)
    and its geometry (WKB, a QgsGeometry, or None for none).
    """

    def __init__(self, fid, attributes, geometry):
        self._fid = fid
        self._attrs = attributes or {}
        self._geometry = geometry

    def id(self):
        return self._fid

    def __getitem__(self, name):
        return self._attrs[name]

    def geometry(self):
        from qgis.core import QgsGeometry
        g = self._geometry
        if isinstance(g, QgsGeometry):
            return g
        out = QgsGeometry()
        if g:
            out.fromWkb(bytes(g))
        return out


def _unfiltered_provider(layer):
    """A second provider on *layer*'s source without its filter, or None.

    Most providers apply the layer's subset string to every read, a read
    by FID included (Spatialite, measured; PostgreSQL builds the same
    WHERE); OGR does not for a read by FID (measured). What the filter
    hides still exists. Built from the layer's own source through the
    provider registry, minus the subset; None when the source cannot be
    rebuilt without it. The caller drops it right after use.
    """
    try:
        provider = layer.dataProvider()
        if provider is None or not (provider.subsetString() or ""):
            return None
        from qgis.core import QgsProviderRegistry, QgsDataProvider
        registry = QgsProviderRegistry.instance()
        key = layer.providerType()
        parts = dict(registry.decodeUri(key, layer.source()) or {})
        cleared = [k for k in ("subset", "sql") if parts.get(k)]
        if not cleared:
            return None
        for k in cleared:
            parts[k] = ""
        other = registry.createProvider(key, registry.encodeUri(key, parts),
                                        QgsDataProvider.ProviderOptions())
        if other is None or not other.isValid() or (other.subsetString() or ""):
            return None
        return other
    except Exception as exc:  # noqa: BLE001 - callers fall back, loudly
        flog(f"EditSessionTracker: no unfiltered provider for "
             f"{_safe_layer_name(layer)!r}: {type(exc).__name__}: {exc}", "WARNING")
        return None


def _safe_layer_name(layer) -> str:
    try:
        return layer.name()
    except (RuntimeError, AttributeError):
        return "?"


def _layer_gone(layer) -> bool:
    """True when *layer* is None or its C++ object has been destroyed.

    Asked of the wrapper only (sip keeps the flag on the Python side), so
    the answer never goes through the freed object. Measured on QGIS
    4.2.1: the flag is set for layers made in Python and for layers QGIS
    created itself (project read) alike, and it is already set when
    layersRemoved is emitted. An object sip does not wrap (a test double)
    is taken as alive.
    """
    if layer is None:
        return True
    try:
        from qgis.PyQt import sip
        return bool(sip.isdeleted(layer))
    except (ImportError, TypeError):
        return False
