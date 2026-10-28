# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""What the user must be able to read at a glance: is my work protected?

Pure logic, no Qt. The status bar used to show a green dot computed from
a QgsSettings flag: capture could be dead (writer thread gone, a journal
the queue could not write, a paused tracker) under a dot that said
"actif". This module derives the state from the runtime facts only.

States, in the order they win:

``LOCKED``      no journal is open because another QGIS instance holds the
                project's journal: edits made here are NOT recorded; the
                plugin retries on its own.
``NO_PROJECT``  no journal is open, nothing can be captured.
``ERROR``       edits are, or were this session, NOT recorded anywhere, or
                may not be: writer dead, events lost, capture cut by the
                disk monitor, event generation failed, the journal's lock
                taken over by another instance (two writers), an internal
                failure of the plugin that may have left capture off, the
                writer's state unreadable (nothing can tell whether it is
                alive). With no journal open, an internal failure also wins
                over NO_PROJECT: it may be why nothing is open.
``PAUSED``      capture is off (user toggle, memory pressure pause).
``DEGRADED``    edits are durable but not in the normal path: journal
                unwritable and retried, outbox fallback used, low disk,
                pre-commit capture not durable, layers edited that cannot
                be captured, history kept in the session journal after a
                save whose carry failed, an internal failure of the plugin
                that leaves capture running, additions of this project
                journaled without the values their source chose for them
                (the read-back after the commit did not return them: the
                history of those features is incomplete), changes of this
                project whose NEW side is the value sent, not read back
                from the source (a Restore or a Rewind that proves its
                target by it may be refused).
``PERSISTING``  healthy, the writer is catching up on queued events.
``PROTECTED``   every accepted edit is in the journal or on its way, durably.
"""
from __future__ import annotations

from typing import List, NamedTuple, Sequence, Tuple

LOCKED = "LOCKED"
NO_PROJECT = "NO_PROJECT"
ERROR = "ERROR"
PAUSED = "PAUSED"
DEGRADED = "DEGRADED"
PERSISTING = "PERSISTING"
PROTECTED = "PROTECTED"

DISK_HEALTHY = "HEALTHY"
DISK_LOW = "LOW_SPACE"
DISK_CRITICAL = "CRITICAL_SPACE"
DISK_UNREACHABLE = "UNREACHABLE"
DISK_UNKNOWN = "UNKNOWN"


def disk_state(status) -> str:
    """Classify a core.disk_monitor.DiskStatus."""
    if status is None:
        return DISK_UNKNOWN
    if not status.volume_path and status.total_bytes <= 0 and not status.is_critical:
        return DISK_UNKNOWN
    if status.total_bytes <= 0:
        return DISK_UNREACHABLE
    if status.is_critical:
        return DISK_CRITICAL
    if status.is_low:
        return DISK_LOW
    return DISK_HEALTHY


class CaptureHealth(NamedTuple):
    state: str
    reasons: List[str]
    queued_events: int
    last_successful_flush: str
    lost_events: int
    degraded_events: int
    disk: str


def _unverified_inserts_reason(
        entries: Sequence[Tuple[str, int, Sequence[str]]]) -> str:
    """One line for every addition journaled without its source values.

    ``entries``: (layer name, features, field names) per layer. One reason,
    however many layers: the tooltip shows a handful of reasons, and a
    layer per line would push the others out of it.
    """
    total = sum(int(count or 0) for _name, count, _fields in entries)
    parts = [f"'{name}' : {', '.join(fields) or '?'}"
             for name, _count, fields in list(entries)[:3]]
    if len(entries) > 3:
        parts.append(f"{len(entries) - 3} autre(s) couche(s)")
    return (f"{total} entite(s) ajoutee(s) journalisee(s) sans les valeurs "
            f"choisies par la source (champs {' ; '.join(parts)}) : leur "
            f"historique est incomplet")


def _unverified_updates_reason(
        entries: Sequence[Tuple[str, int, Sequence[str]]]) -> str:
    """One line for every change whose NEW side was not read back.

    ``entries``: (layer name, changes, field names) per layer, bounded like
    `_unverified_inserts_reason`.
    """
    total = sum(int(count or 0) for _name, count, _fields in entries)
    parts = [f"'{name}' : {', '.join(fields) or '?'}"
             for name, _count, fields in list(entries)[:3]]
    if len(entries) > 3:
        parts.append(f"{len(entries) - 3} autre(s) couche(s)")
    return (f"{total} modification(s) journalisee(s) avec la valeur envoyee, "
            f"non relue dans la source ({' ; '.join(parts)}) : un Restore ou "
            f"un Rewind de ces modifications peut etre refuse")


def _internal_reason(text: str, at_risk: bool) -> str:
    if at_risk:
        return (f"defaillance interne de RecoverLand : {text} ; la capture "
                f"peut etre coupee ou incomplete")
    return f"defaillance interne de RecoverLand : {text} (la capture continue)"


def evaluate_capture_health(
    journal_open: bool,
    tracker_active: bool,
    writer=None,
    disk: str = DISK_UNKNOWN,
    disabled_by_disk: bool = False,
    pressure_paused: bool = False,
    capture_failures: int = 0,
    uncapturable_layers: int = 0,
    prepare_failures: int = 0,
    journal_locked: str = "",
    lock_lost: bool = False,
    carry_fallback: str = "",
    internal_failures: Sequence[Tuple[str, bool]] = (),
    writer_error: str = "",
    unverified_inserts: Sequence[Tuple[str, int, Sequence[str]]] = (),
    unverified_updates: Sequence[Tuple[str, int, Sequence[str]]] = (),
) -> CaptureHealth:
    """``writer`` is a core.write_queue.WriterHealth (or None).

    ``journal_locked``: why no journal is open when another instance holds
    it ('' when that is not the case). ``lock_lost``: the open journal's
    writer lock was taken over by another instance. ``carry_fallback``:
    what the last save could not carry, while capture stays in the source
    journal ('' when nothing is pending). ``internal_failures``: (text,
    capture_at_risk) for each plugin slot that failed and was not
    recovered since. ``writer_error``: why the writer's health could not be
    read ('' when it was). A journal open with a writer nobody can read is
    ERROR: ``writer=None`` alone used to end in PROTECTED, a green dot over
    a writer thread that may be dead. ``unverified_inserts``: (layer name,
    features, field names) for the additions of this project journaled
    without the values their source chose for those fields (the tracker's
    status 'insert_values_unverified'). Capture goes on and every event is
    durable, but the history of those features lacks those values: DEGRADED,
    never PROTECTED, until another project is opened. ``unverified_updates``:
    (layer name, changes, field names) for the changes of this project whose
    NEW side keeps the value the edit buffer sent, because the read-back did
    not return them (status 'update_values_unverified'); the source may have
    stored another value, and a Restore or a Rewind proves its target by
    that side: DEGRADED on the same terms.
    """
    reasons: List[str] = []
    queued = int(getattr(writer, "queued_events", 0) or 0)
    last_flush = str(getattr(writer, "last_successful_flush", "") or "")
    lost = int(getattr(writer, "lost_events", 0) or 0)
    degraded_events = int(getattr(writer, "degraded_events", 0) or 0)
    at_risk = [text for text, risk in internal_failures if risk]
    harmless = [text for text, risk in internal_failures if not risk]

    def _result(state: str) -> CaptureHealth:
        return CaptureHealth(state, reasons, queued, last_flush, lost,
                             degraded_events, disk)

    if not journal_open:
        if journal_locked:
            reasons.append(f"enregistrement inactif (journal verrouille) : "
                           f"{journal_locked}")
            reasons.extend(_internal_reason(t, True) for t in at_risk)
            return _result(LOCKED)
        if at_risk:
            reasons.extend(_internal_reason(t, True) for t in at_risk)
            reasons.append("aucun journal ouvert : les modifications ne sont "
                           "PAS enregistrees")
            return _result(ERROR)
        reasons.append("aucun journal ouvert")
        return _result(NO_PROJECT)

    wstate = str(getattr(writer, "state", "") or "")
    error = False
    if lock_lost:
        error = True
        reasons.append("le verrou du journal a ete pris par une autre instance "
                       "de QGIS : deux instances peuvent ecrire ce journal, les "
                       "modifications faites ici ne sont plus protegees. Fermez "
                       "l'autre instance puis rouvrez le projet")
    for text in at_risk:
        error = True
        reasons.append(_internal_reason(text, True))
    if writer_error:
        error = True
        reasons.append(f"etat du thread d'ecriture inconnu ({writer_error}) : "
                       f"rien ne permet de verifier que les modifications "
                       f"sont ecrites")
    if writer is not None and wstate == "failed":
        error = True
        reasons.append("thread d'ecriture arrete : les editions restent dans "
                       "l'outbox et seront integrees au prochain demarrage")
    if lost:
        error = True
        reasons.append(f"{lost} evenement(s) n'ont pu etre ecrits nulle part "
                       f"(ni outbox ni journal)")
    if capture_failures:
        error = True
        reasons.append(f"{capture_failures} commit(s) dont la capture a echoue "
                       f"(etat d'avant conserve au registre)")
    if disabled_by_disk or disk in (DISK_UNREACHABLE, DISK_CRITICAL):
        error = True
        reasons.append("volume du journal inaccessible" if disk == DISK_UNREACHABLE
                       else "espace disque critique : capture coupee")
    if error:
        return _result(ERROR)

    if not tracker_active or pressure_paused:
        reasons.append("capture en pause (memoire)" if pressure_paused
                       else "capture desactivee")
        return _result(PAUSED)

    degraded = False
    if carry_fallback:
        degraded = True
        reasons.append(carry_fallback)
    if unverified_inserts:
        degraded = True
        reasons.append(_unverified_inserts_reason(list(unverified_inserts)))
    if unverified_updates:
        degraded = True
        reasons.append(_unverified_updates_reason(list(unverified_updates)))
    for text in harmless:
        degraded = True
        reasons.append(_internal_reason(text, False))
    if wstate == "degraded":
        degraded = True
        err = str(getattr(writer, "last_persistence_error", "") or "")
        reasons.append(f"journal momentanement non inscriptible ({err}) : "
                       f"les editions sont durables dans l'outbox")
    quarantined = int(getattr(writer, "quarantined_events", 0) or 0)
    if quarantined:
        degraded = True
        reasons.append(f"{quarantined} evenement(s) refuse(s) par le journal et "
                       f"conserve(s) en quarantaine (a analyser)")
    if writer is not None and getattr(writer, "outbox_ok", True) is False:
        degraded = True
        err = str(getattr(writer, "last_persistence_error", "") or "")
        reasons.append(f"stockage d'intention (outbox) non inscriptible ({err}) : "
                       f"les editions sont ecrites directement dans le journal, "
                       f"un plantage pendant un commit ne serait pas couvert")
    if degraded_events:
        # History of the session, not a current danger: said, not escalated.
        reasons.append(f"{degraded_events} evenement(s) de cette session sont "
                       f"passes par un chemin de secours durable")
    if disk == DISK_LOW:
        degraded = True
        reasons.append("espace disque faible")
    if prepare_failures:
        degraded = True
        reasons.append(f"{prepare_failures} commit(s) sans etat d'avant durable "
                       f"pendant l'ecriture")
    if uncapturable_layers:
        degraded = True
        reasons.append(f"{uncapturable_layers} couche(s) editee(s) en mode "
                       f"transactionnel automatique : non capturables")
    if degraded:
        return _result(DEGRADED)
    if queued > 0:
        reasons.append(f"{queued} evenement(s) en cours d'ecriture (deja durables)")
        return _result(PERSISTING)
    return _result(PROTECTED)
