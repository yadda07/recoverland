"""Journal health monitoring for RecoverLand (UX-A01, UX-A04, UX-H01).

Provides threshold evaluation, disk space checks, and user-facing
diagnostic messages. Pure logic module with no Qt dependency.
"""
import os
import shutil
from typing import NamedTuple, Optional

from qgis.PyQt.QtCore import QCoreApplication

from .logger import flog


def _tr(msg):
    return QCoreApplication.translate("HealthMonitor", msg)


class HealthLevel:
    HEALTHY = "healthy"
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class JournalHealthStatus(NamedTuple):
    level: str
    size_bytes: int
    event_count: int
    oldest_event: str
    newest_event: str
    message: str
    suggestion: str


class DiskSpaceStatus(NamedTuple):
    level: str
    free_bytes: int
    message: str
    should_disable_tracking: bool


# --- Size thresholds (bytes) ---
_SIZE_INFO = 50 * 1024 * 1024        # 50 MB
_SIZE_WARNING = 200 * 1024 * 1024    # 200 MB
_SIZE_CRITICAL = 500 * 1024 * 1024   # 500 MB

# --- Event count thresholds ---
_COUNT_INFO = 100_000
_COUNT_WARNING = 500_000
_COUNT_CRITICAL = 1_000_000

# --- Disk space thresholds (bytes) ---
_DISK_WARNING = 500 * 1024 * 1024    # 500 MB
_DISK_CRITICAL = 100 * 1024 * 1024   # 100 MB


def evaluate_journal_health(
    size_bytes: int,
    event_count: int,
    oldest_event: str,
    newest_event: str,
) -> JournalHealthStatus:
    """Evaluate journal health from size and event metrics.

    Returns a status with level, message and actionable suggestion.
    """
    size_level = _classify_size(size_bytes)
    count_level = _classify_count(event_count)
    level = _worst_level(size_level, count_level)

    message = _build_health_message(level, size_bytes, event_count)
    suggestion = _build_health_suggestion(level, size_bytes, event_count)

    return JournalHealthStatus(
        level=level,
        size_bytes=size_bytes,
        event_count=event_count,
        oldest_event=oldest_event,
        newest_event=newest_event,
        message=message,
        suggestion=suggestion,
    )


def check_disk_space(journal_path: str) -> DiskSpaceStatus:
    """Check free disk space on the volume hosting the journal."""
    if not journal_path:
        return DiskSpaceStatus(
            level=HealthLevel.HEALTHY,
            free_bytes=0,
            message="",
            should_disable_tracking=False,
        )
    try:
        usage = shutil.disk_usage(os.path.dirname(journal_path))
        free = usage.free
    except (OSError, ValueError) as e:
        flog(f"health_monitor: disk_usage failed: {e}", "WARNING")
        return DiskSpaceStatus(
            level=HealthLevel.WARNING,
            free_bytes=0,
            message=_tr("Impossible de verifier l'espace disque."),
            should_disable_tracking=False,
        )

    if free < _DISK_CRITICAL:
        return DiskSpaceStatus(
            level=HealthLevel.CRITICAL,
            free_bytes=free,
            message=_tr("Espace disque critique : {size} libre.").format(size=_format_size(free)),
            should_disable_tracking=True,
        )
    if free < _DISK_WARNING:
        return DiskSpaceStatus(
            level=HealthLevel.WARNING,
            free_bytes=free,
            message=_tr("Espace disque faible : {size} libre.").format(size=_format_size(free)),
            should_disable_tracking=False,
        )
    return DiskSpaceStatus(
        level=HealthLevel.HEALTHY,
        free_bytes=free,
        message="",
        should_disable_tracking=False,
    )


def format_integrity_message(issues: list, recovered: int) -> Optional[str]:
    """Build a user-facing message from integrity check results.

    Returns None if journal is healthy with no recoveries.
    """
    parts = []
    if recovered > 0:
        parts.append(
            _tr("{count} evenement(s) recupere(s) depuis la derniere session.").format(count=recovered)
        )
    for issue in issues:
        parts.append(_humanize_integrity_issue(issue))
    if not parts:
        return None
    return " ".join(parts)


def format_user_error(what: str, why: str, action: str) -> str:
    """Format a structured error message for UI display."""
    return f"{what} : {why}. {action}"


# --- Internal helpers ---

def _classify_size(size_bytes: int) -> str:
    if size_bytes >= _SIZE_CRITICAL:
        return HealthLevel.CRITICAL
    if size_bytes >= _SIZE_WARNING:
        return HealthLevel.WARNING
    if size_bytes >= _SIZE_INFO:
        return HealthLevel.INFO
    return HealthLevel.HEALTHY


def _classify_count(event_count: int) -> str:
    if event_count >= _COUNT_CRITICAL:
        return HealthLevel.CRITICAL
    if event_count >= _COUNT_WARNING:
        return HealthLevel.WARNING
    if event_count >= _COUNT_INFO:
        return HealthLevel.INFO
    return HealthLevel.HEALTHY


def _worst_level(a: str, b: str) -> str:
    order = {
        HealthLevel.HEALTHY: 0,
        HealthLevel.INFO: 1,
        HealthLevel.WARNING: 2,
        HealthLevel.CRITICAL: 3,
    }
    return a if order.get(a, 0) >= order.get(b, 0) else b


def _build_health_message(
    level: str, size_bytes: int, event_count: int,
) -> str:
    size_str = _format_size(size_bytes)
    count_str = f"{event_count:,}".replace(",", " ")
    if level == HealthLevel.CRITICAL:
        return _tr(
            "Journal volumineux : {size}, "
            "{count} evenement(s). Purge recommandee."
        ).format(size=size_str, count=count_str)
    if level == HealthLevel.WARNING:
        return _tr(
            "Journal en croissance : {size}, "
            "{count} evenement(s). "
            "Pensez a purger les anciens evenements."
        ).format(size=size_str, count=count_str)
    if level == HealthLevel.INFO:
        return _tr("Journal : {size}, {count} evenement(s).").format(size=size_str, count=count_str)
    return ""


def _build_health_suggestion(level: str, _size: int, _count: int) -> str:
    if level == HealthLevel.CRITICAL:
        return _tr(
            "Ouvrez la maintenance du journal pour purger les "
            "evenements anciens et compacter la base."
        )
    if level == HealthLevel.WARNING:
        return _tr(
            "Ouvrez la maintenance du journal pour configurer "
            "la politique de retention."
        )
    return ""


def _humanize_integrity_issue(issue: str) -> str:
    """Convert technical integrity messages to user-friendly text."""
    lower = issue.lower()
    if "integrity check failed" in lower:
        return _tr(
            "Le journal presente des anomalies. "
            "Les donnees recentes peuvent etre incompletes."
        )
    if "wal checkpoint failed" in lower:
        return _tr("Le journal a des ecritures en attente de consolidation.")
    if "schema version" in lower and "newer" in lower:
        return _tr(
            "Le journal a ete cree par une version plus recente du plugin. "
            "Certaines donnees pourraient ne pas etre lisibles."
        )
    if "no schema version" in lower:
        return _tr("Le journal ne contient pas d'information de version.")
    if "not found" in lower:
        return _tr("Le fichier journal est introuvable.")
    if "cannot open" in lower:
        return _tr("Impossible d'ouvrir le journal.")
    return _tr("Anomalie detectee : {issue}").format(issue=issue)


def _format_size(size_bytes: int) -> str:
    if size_bytes < 1024:
        return _tr("{size} o").format(size=size_bytes)
    if size_bytes < 1024 * 1024:
        return _tr("{size} Ko").format(size=f"{size_bytes / 1024:.1f}")
    if size_bytes < 1024 * 1024 * 1024:
        return _tr("{size} Mo").format(size=f"{size_bytes / (1024 * 1024):.1f}")
    return _tr("{size} Go").format(size=f"{size_bytes / (1024 * 1024 * 1024):.2f}")
