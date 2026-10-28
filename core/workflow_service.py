# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Workflow service for RecoverLand restore operations.

Centralizes the grouped-restore and undo-restore logic that was
previously duplicated in the dialog. Pure business logic; no Qt widgets.
"""
from collections import defaultdict
from typing import List, Dict, Optional, Callable, NamedTuple

from .audit_backend import AuditEvent
from .restore_service import restore_batch, undo_restore_batch
from .logger import flog


_temp_layer_ids: List[str] = []


def cleanup_temp_layers() -> int:
    """Remove temporary layers added during restore (RW-15).

    Returns number of layers removed.
    """
    if not _temp_layer_ids:
        return 0
    try:
        from qgis.core import QgsProject
        project = QgsProject.instance()
        removed = 0
        for lid in list(_temp_layer_ids):
            if project.mapLayer(lid) is not None:
                project.removeMapLayer(lid)
                removed += 1
        flog(f"cleanup_temp_layers: removed={removed}/{len(_temp_layer_ids)}")
        _temp_layer_ids.clear()
        return removed
    except Exception as exc:
        flog(f"cleanup_temp_layers: failed: {exc}", "WARNING")
        _temp_layer_ids.clear()
        return 0


class GroupedRestoreResult(NamedTuple):
    total_ok: int
    total_fail: int
    errors: List[str]
    by_ds: Dict[str, list]
    trace_events: list
    failed_eids: List[int] = []
    # BL-RW-P3-18: per-category breakdown propagated by the runners.
    # `applied` is a strict subset of `total_ok`: it excludes events the
    # executor short-circuited as `skipped_idempotent`, `target_absent`
    # or `geometry_drift`, and (RLU-053) those written without every
    # captured field. Conservation invariant, on the runner side:
    #   total_ok + total_fail == applied + applied_partial
    #                          + skipped_idempotent
    #                          + failed + failed_target_absent
    #                          + failed_geometry_drift
    #                          (+ cancelled, actions never attempted)
    # `applied_partial` and `cancelled` have no field here: the strict
    # runner answers `partial_restore_report()` for the first, and both
    # are published on its CYCLE_SUMMARY line. Do not reconstruct the
    # partial count as `total_ok - applied - skipped_idempotent`.
    #
    # RLU-054, read this before displaying anything: `failed` is the
    # RESIDUAL failure bucket ("no more precise reason"), NOT the failure
    # total. A rewind that failed 59 times on absent targets carries
    # failed=0 here. The total is
    #   failed + failed_target_absent + failed_geometry_drift
    # and that is what the runner publishes as `apply_fail` / `failed`.
    applied: int = 0
    skipped_idempotent: int = 0
    failed: int = 0
    failed_target_absent: int = 0
    failed_geometry_drift: int = 0
    # The commit was accepted but reading the feature back from the
    # provider contradicts what was written (core.commit_guard).
    failed_postcommit_verification: int = 0


def execute_grouped_restore(
    events: List[AuditEvent],
    find_layer_fn: Callable[[AuditEvent], object],
    on_group_done: Optional[Callable[[int, int], None]] = None,
    trace_id: str = "",
    journal_path: Optional[str] = None,
) -> GroupedRestoreResult:
    """Execute restore grouped by datasource fingerprint.

    Args:
        events: audit events to restore (in order).
        find_layer_fn: callable(event) -> QgsVectorLayer or None.
        on_group_done: optional callback(processed_count, total_count)
            for progress updates.
        trace_id: BL-RW-P3-17: propagated to restore_batch so executor
            logs share the rewind chain prefix.
        journal_path: the journal *events* come from. ONE read-only
            connection on it per group (`_open_group_journal`), handed to
            restore_batch and closed on every path. The executor reads it
            for two proofs: a key the journal proves continuous (without
            it a key whose content other features share is refused
            "key_continuity_unproven (no_journal)"), and an event a trace
            already compensates (without it a second restore is written
            over a later edit). A group whose journal is not given or
            cannot be read is refused, named, before its layer is even
            looked up: nothing is written.

    Returns:
        GroupedRestoreResult with totals, errors, by_ds map, and trace events.
    """
    by_ds: Dict[str, list] = defaultdict(list)
    for event in events:
        by_ds[event.datasource_fingerprint].append(event)

    total_ok, total_fail = 0, 0
    errors: List[str] = []
    all_traces: list = []
    processed = 0

    for fp, group in by_ds.items():
        journal, unreadable = _open_group_journal(journal_path, group[0],
                                                  trace_id)
        try:
            if journal is None:
                errors.append(_journal_refusal(group, fp, journal_path,
                                               unreadable, "restaure"))
                total_fail += len(group)
            else:
                layer = find_layer_fn(group[0])
                if layer is None:
                    errors.append(_missing_layer_message(group[0], fp))
                    total_fail += len(group)
                else:
                    report = restore_batch(layer, group, trace_id=trace_id,
                                           journal=journal)
                    total_ok += len(report.succeeded)
                    total_fail += len(report.failed)
                    for eid, msg in report.failed.items():
                        errors.append(f"Evt {eid}: {msg}")
                    all_traces.extend(report.trace_events)

                    if report.succeeded:
                        layer.reload()
                    layer.triggerRepaint()
        finally:
            _close_group_journal(journal)

        processed += len(group)
        if on_group_done is not None:
            on_group_done(processed, len(events))

    return GroupedRestoreResult(
        total_ok=total_ok,
        total_fail=total_fail,
        errors=errors,
        by_ds=dict(by_ds),
        trace_events=all_traces,
    )


def execute_grouped_undo(
    by_ds: Dict[str, list],
    find_layer_fn: Callable[[AuditEvent], object],
    journal_path: Optional[str] = None,
) -> GroupedRestoreResult:
    """Undo a previous grouped restore.

    Args:
        by_ds: dict mapping datasource fingerprints to event lists
            (as returned by execute_grouped_restore).
        find_layer_fn: callable(event) -> QgsVectorLayer or None.
        journal_path: the journal the events come from. ONE read-only
            connection on it per group, handed to undo_restore_batch and
            closed on every path: the evidence that a key never changed
            hands (`restore_service._key_continuity`). A journal given
            that cannot be read refuses the group, named, before its
            layer is looked up. None keeps the undo without it, as
            `UndoRunner` does when given no journal: an undo never asks
            `already_compensated`, and the executor refuses by name every
            key it cannot prove by content alone.

    Returns:
        GroupedRestoreResult (trace_events always empty for undo).
    """
    total_ok, total_fail = 0, 0
    errors: List[str] = []

    for fp, group in by_ds.items():
        journal, unreadable = (_open_group_journal(journal_path, group[0])
                               if journal_path else (None, ""))
        try:
            if journal_path and journal is None:
                errors.append(_journal_refusal(group, fp, journal_path,
                                               unreadable, "annule"))
                total_fail += len(group)
                continue
            layer = find_layer_fn(group[0])
            if layer is None:
                errors.append(_missing_layer_message(group[0], fp))
                total_fail += len(group)
                continue

            report = undo_restore_batch(layer, group, journal=journal)
            total_ok += len(report.succeeded)
            total_fail += len(report.failed)
            for eid, msg in report.failed.items():
                errors.append(f"Evt {eid}: {msg}")

            if report.succeeded:
                layer.reload()
            layer.triggerRepaint()
        finally:
            _close_group_journal(journal)

    return GroupedRestoreResult(
        total_ok=total_ok,
        total_fail=total_fail,
        errors=errors,
        by_ds=dict(by_ds),
        trace_events=[],
    )


def _open_group_journal(journal_path: Optional[str], event: AuditEvent,
                        trace_id: str = ""):
    """ONE read-only connection on the journal, for the group starting.

    Same contract as `restore_runner._open_group_journal`: ``(conn, "")``
    when the journal answers, ``(None, reason)`` when it is not given or
    cannot be read. Opening a URI proves nothing about its content: the
    table the proofs read must answer. The caller closes the connection
    (`_close_group_journal`) on every path, exceptions included.
    """
    prefix = f"[{trace_id}] " if trace_id else ""
    name = event.layer_name_snapshot or event.datasource_fingerprint
    if not journal_path:
        flog(f"{prefix}workflow_service: no journal given for layer "
             f"{name!r}; group refused, nothing written", "ERROR")
        return None, "aucun journal fourni"
    import sqlite3
    from .journal_manager import sqlite_readonly_uri
    conn = None
    try:
        conn = sqlite3.connect(sqlite_readonly_uri(journal_path), uri=True)
        conn.execute("SELECT 1 FROM audit_event LIMIT 1").fetchall()
    except Exception as exc:  # noqa: BLE001 - said to the caller, never silent
        _close_group_journal(conn)
        flog(f"{prefix}workflow_service: journal_read unavailable layer="
             f"{name!r} ({type(exc).__name__}: {exc}); group refused, "
             f"nothing written", "ERROR")
        return None, f"{type(exc).__name__}: {exc}"
    flog(f"{prefix}workflow_service: journal_read opened layer={name!r}")
    return conn, ""


def _close_group_journal(conn) -> None:
    """Close a group's journal connection, if any. Never raises."""
    if conn is None:
        return
    try:
        conn.close()
    except Exception as exc:  # noqa: BLE001 - a close never stops the run
        flog(f"workflow_service: journal read connection close failed "
             f"({exc!r})", "WARNING")


def _journal_refusal(group: list, fp: str, journal_path: Optional[str],
                     reason: str, done: str) -> str:
    """Why a group was refused for its journal: layer, reason, events."""
    name = group[0].layer_name_snapshot or fp
    ids = ", ".join(str(e.event_id) for e in group[:10])
    more = f" (+{len(group) - 10})" if len(group) > 10 else ""
    if journal_path:
        why = (f"le journal des evenements ({journal_path}) n'a pas pu etre "
               f"lu ({reason})")
    else:
        why = "aucun journal des evenements n'a ete fourni"
    return (f"Couche '{name}' refusee : {why}. Ses preuves d'identite (cle "
            f"jamais passee a une autre entite, evenement deja restaure) "
            f"manquent : {len(group)} evenement(s) non {done}(s) "
            f"[{ids}{more}], rien d'ecrit.")


def find_target_layer(event: AuditEvent, read_conn=None) -> object:
    """Find the QGIS layer matching an audit event.

    Search order:
    1. Layer ID match in loaded project layers, CONFIRMED by the source.
    2. Datasource EQUIVALENCE with a loaded layer.
    3. Recreate from datasource registry (if read_conn provided).

    Step 1 no longer trusts the id alone: "Change Data Source" repoints a
    layer to another table (another file, a backup copy, another service)
    and keeps its id, so the event of ``routes`` was written into
    ``rivieres``. The id is accepted only when the layer's current source
    is the event's, the alias scope of the journal included; otherwise
    the search goes on and may find the layer that does hold that source.

    Step 2 compares by equivalence, not by string equality: an event
    captured before the v6 canonical form (a display filter in the
    source, a path written the other way round) carries a fingerprint
    that no longer equals the one the loaded layer computes today. With
    a strict `==` the layer the user has in front of him is not
    recognised and the restore silently falls back to a temporary layer
    rebuilt from the registry -- writing to a copy while he watches the
    original. The equivalence stays lenient for a fingerprint written
    before v7 without its connection service, so that capture keeps
    following a layer tracked under a 6.x selection; a WRITE on that
    ground alone is refused (`identity.service_match_unproven`): the
    events may come from another database. Attaching that history
    (`datasource_alias.attach_service_history`, or the reconciliation
    when one service only is known) puts the service fingerprint in the
    alias scope, and the layer is then found.

    A refusal is remembered for `target_refusal_reason`, so the caller
    can say WHY instead of "layer not found".

    Returns QgsVectorLayer or None.
    """
    from qgis.core import QgsProject, QgsVectorLayer

    scope = _event_scope(event, read_conn)
    refusals: List[str] = []
    layers = [layer for layer in QgsProject.instance().mapLayers().values()
              if isinstance(layer, QgsVectorLayer)]

    for layer in layers:
        if layer.id() != event.layer_id_snapshot:
            continue
        proven, why = _source_matches(layer, scope)
        if proven:
            _forget_refusal(event)
            return layer
        refusals.append(why or _repointed_reason(layer, event))
        flog(f"find_target_layer: layer id {layer.id()} matches the event but "
             f"not its source ({refusals[-1]}); id not trusted", "WARNING")
        break

    for layer in layers:
        proven, why = _source_matches(layer, scope)
        if proven:
            _forget_refusal(event)
            return layer
        if why and why not in refusals:
            refusals.append(why)

    if refusals:
        flog(f"find_target_layer: eid={event.event_id} refused on "
             f"{len(refusals)} loaded layer(s): {refusals[:3]}", "WARNING")
    if read_conn is not None:
        layer = _try_restore_from_registry(event, read_conn)
        if layer is not None:
            _forget_refusal(event)
            return layer
    _remember_refusal(event, refusals)
    return None


def _source_matches(layer, scope: set):
    """``(proven, reason)``: does *layer* hold one of the *scope* sources?

    *reason* is only set for a near miss worth telling the user: the same
    table reached through a service the events did not record.
    """
    from .identity import (
        compute_datasource_fingerprint, datasource_fingerprints_match,
        fingerprint_service, service_match_unproven,
    )
    try:
        current = compute_datasource_fingerprint(layer)
    except Exception as exc:  # noqa: BLE001
        # An unfingerprintable layer cannot be matched, and skipping it
        # lets the search reach the next candidate. No layer.id() call
        # here on purpose: the usual cause is a deleted C++ wrapper, whose
        # id() would raise too and turn this handler into the crash it
        # exists to absorb. exc!r already names that case.
        flog(f"find_target_layer: datasource fingerprint failed, "
             f"candidate layer skipped ({exc!r})", "DEBUG")
        return False, ""
    if current in scope:
        return True, ""
    unproven = ""
    for fp in scope:
        if not datasource_fingerprints_match(fp, current):
            continue
        if service_match_unproven(fp, current):
            unproven = (
                f"la couche '{_layer_label(layer)}' passe par le service de "
                f"connexion '{fingerprint_service(current)}', que "
                f"l'evenement (capture avant v7) n'a pas enregistre : il "
                f"peut venir d'une autre base de donnees. Rattachez "
                f"l'historique a ce service pour le restaurer ici")
            continue
        return True, ""
    return False, unproven


def _repointed_reason(layer, event: AuditEvent) -> str:
    try:
        from .identity import compute_datasource_fingerprint
        current = compute_datasource_fingerprint(layer)
    except Exception:  # noqa: BLE001 - the reason stays generic
        current = "?"
    return (f"la couche '{_layer_label(layer)}' porte l'identifiant de la "
            f"couche d'origine mais pointe desormais vers une autre source "
            f"({current}) que celle des evenements "
            f"({event.datasource_fingerprint})")


def _layer_label(layer) -> str:
    try:
        return layer.name() or "?"
    except Exception:  # noqa: BLE001 - deleted wrapper
        return "?"


# Last refusal of find_target_layer per event, for the caller's message.
# Bounded: only the most recent searches matter (GUI thread only).
_REFUSALS: Dict[tuple, str] = {}
_MAX_REFUSALS = 256


def _refusal_key(event: AuditEvent) -> tuple:
    return (event.event_id, event.datasource_fingerprint or "")


def _remember_refusal(event: AuditEvent, refusals: List[str]) -> None:
    key = _refusal_key(event)
    _REFUSALS.pop(key, None)
    if not refusals:
        return
    if len(_REFUSALS) >= _MAX_REFUSALS:
        _REFUSALS.pop(next(iter(_REFUSALS)))
    _REFUSALS[key] = "; ".join(refusals[:3])


def _forget_refusal(event: AuditEvent) -> None:
    _REFUSALS.pop(_refusal_key(event), None)


def target_refusal_reason(event: AuditEvent) -> str:
    """Why the last `find_target_layer` for *event* found no layer.

    '' when no loaded layer was refused, i.e. the source is simply not
    loaded in the project.
    """
    return _REFUSALS.get(_refusal_key(event), "")


def _missing_layer_message(event: AuditEvent, fp: str) -> str:
    name = event.layer_name_snapshot or fp
    reason = target_refusal_reason(event)
    if reason:
        return f"Couche '{name}' refusee comme cible : {reason}."
    return f"Couche '{name}' non trouvee dans le projet."


def _event_scope(event: AuditEvent, read_conn=None) -> set:
    """The event's fingerprint plus every form aliased to it."""
    fingerprint = event.datasource_fingerprint or ""
    scope = {fingerprint} if fingerprint else set()
    if read_conn is None or not fingerprint:
        return scope
    try:
        from .datasource_alias import expand_fingerprints
        scope.update(expand_fingerprints(read_conn, fingerprint))
    except Exception as exc:  # noqa: BLE001 - degrade to the raw fingerprint
        flog(f"find_target_layer: alias expansion failed: {exc}", "WARNING")
    return scope


def _try_restore_from_registry(event: AuditEvent, read_conn) -> object:
    """Recreate a layer from the datasource registry.

    For DB-backed layers (postgres, mssql, oracle): resolves credentials
    from authcfg or QGIS saved connections. Falls back to None if the
    connection cannot be established.
    """
    try:
        from qgis.core import QgsProject
        from .datasource_registry import (
            lookup_datasource, create_layer_from_registry, _DB_PROVIDERS,
        )

        info = lookup_datasource(read_conn, event.datasource_fingerprint)
        if info is None:
            flog(f"find_target_layer: no registry for {event.datasource_fingerprint}")
            return None

        flog(f"find_target_layer: registry hit provider={info.provider_type} "
             f"layer={info.layer_name}")

        layer = create_layer_from_registry(info)
        if layer is not None and layer.isValid():
            QgsProject.instance().addMapLayer(layer, False)
            _temp_layer_ids.append(layer.id())
            flog(f"find_target_layer: temp layer added for restore "
                 f"provider={info.provider_type} id={layer.id()}")
            return layer

        if info.provider_type in _DB_PROVIDERS:
            flog(f"find_target_layer: DB layer '{info.layer_name}' could not "
                 f"reconnect; load it manually in the project first", "WARNING")
        return None
    except Exception as e:
        flog(f"find_target_layer: registry fallback failed: {e}", "WARNING")
        return None


# ----- Overlay cleanup ---------------------------------------------------


_LENS_LAYER_PREFIX = "__rl_lens_"


def purge_lens_overlays(context: str = "manual") -> int:
    """Remove every map layer whose name starts with the Time Lens prefix.

    Called from `recover.initGui` (context="startup"), `recover.unload`
    (context="shutdown"), and `execute_grouped_lens_view` (context=
    "refresh") to enforce acceptance section 1, 2 and 3 of BL-IL-P0-09.
    Loops over ``QgsProject.instance().mapLayers()`` and matches by
    ``layer.name().startswith(_LENS_LAYER_PREFIX)`` so the purge also
    catches layers persisted in a saved .qgs file (CR-IL-6 antithese
    A2: a project saved with Lens active is cleaned at next open).

    Args:
        context: free-form tag carried into the log signature
            (``startup``, ``shutdown``, ``refresh``, ``manual``...).

    Returns:
        Number of layers actually removed.
    """
    try:
        from qgis.core import QgsProject  # noqa: PLC0415
        project = QgsProject.instance()
        ids = [
            lyr.id() for lyr in project.mapLayers().values()
            if any((
                lyr.name().startswith(_LENS_LAYER_PREFIX),
                lyr.name().startswith("__rl_snap_"),
                lyr.customProperty("_rl_lens_managed") == "1",
                lyr.customProperty("_rl_snap_managed") == "1",
            ))
        ]
        if ids:
            project.removeMapLayers(ids)
        flog(
            f"lens_lifecycle event={context}_cleanup "
            f"n_orphan_layers={len(ids)}",
            "INFO" if ids else "DEBUG",
        )
        return len(ids)
    except Exception as exc:  # noqa: BLE001
        flog(
            f"lens_lifecycle event={context}_cleanup_failed "
            f"type={type(exc).__name__}",
            "WARNING",
        )
        return 0
