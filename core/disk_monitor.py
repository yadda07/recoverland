"""Disk space monitoring for RecoverLand (UX-A04).

Lightweight disk space checker. No Qt dependency.
"""
import os
import shutil
from typing import NamedTuple

from qgis.PyQt.QtCore import QCoreApplication

from .logger import flog
from .health_monitor import _format_size


def _tr(msg):
    return QCoreApplication.translate("DiskMonitor", msg)

_CHECK_INTERVAL_SEC = 300  # 5 minutes


class DiskStatus(NamedTuple):
    free_bytes: int
    total_bytes: int
    volume_path: str
    is_low: bool
    is_critical: bool


_THRESHOLD_LOW = 500 * 1024 * 1024       # 500 MB
_THRESHOLD_CRITICAL = 100 * 1024 * 1024   # 100 MB


def check_disk_for_path(path: str) -> DiskStatus:
    """Check disk space for the volume containing path."""
    if not path:
        return DiskStatus(0, 0, "", False, False)
    target = os.path.dirname(path) if not os.path.isdir(path) else path
    if not os.path.exists(target):
        target = _find_existing_parent(target)
    if not target:
        return DiskStatus(0, 0, "", False, False)
    try:
        usage = shutil.disk_usage(target)
        return DiskStatus(
            free_bytes=usage.free,
            total_bytes=usage.total,
            volume_path=_extract_volume(target),
            is_low=usage.free < _THRESHOLD_LOW,
            is_critical=usage.free < _THRESHOLD_CRITICAL,
        )
    except (OSError, ValueError) as e:
        flog(f"disk_monitor: check failed for {target}: {e}", "WARNING")
        return DiskStatus(0, 0, "", False, False)


def format_disk_message(status: DiskStatus) -> str:
    """Build user-facing message from disk status."""
    if status.is_critical:
        return _tr(
            "Espace disque critique sur {volume} : "
            "{free} libre. "
            "L'enregistrement a ete desactive pour eviter la perte de donnees."
        ).format(volume=status.volume_path, free=_format_size(status.free_bytes))
    if status.is_low:
        return _tr(
            "Espace disque faible sur {volume} : "
            "{free} libre."
        ).format(volume=status.volume_path, free=_format_size(status.free_bytes))
    return ""


def _find_existing_parent(path: str) -> str:
    current = path
    for _ in range(50):
        parent = os.path.dirname(current)
        if parent == current:
            break
        if os.path.exists(parent):
            return parent
        current = parent
    return ""


def _extract_volume(path: str) -> str:
    drive = os.path.splitdrive(os.path.abspath(path))[0]
    return drive if drive else "/"
