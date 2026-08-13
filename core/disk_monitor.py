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
    """Check disk space for the volume containing path.

    A volume that cannot be measured is reported critical, never healthy:
    an unplugged network share, a drive letter that disappeared or a
    device that is not ready all answer "0 byte free", and calling that
    healthy is what lets every following write fail in silence while the
    status bar shows a journal in good shape.
    """
    if not path:
        # No journal open yet: nothing to watch, and no volume to blame.
        return DiskStatus(0, 0, "", False, False)
    target = os.path.dirname(path) if not os.path.isdir(path) else path
    if not os.path.exists(target):
        target = _find_existing_parent(target)
    if not target:
        return _unreadable(path, "volume unreachable")
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
        return _unreadable(path, f"check failed for {target}: {e}")


def _unreadable(path: str, reason: str) -> DiskStatus:
    """Status for a volume we could not measure at all.

    total_bytes == 0 marks "no reading obtained" and tells
    format_disk_message to name the real problem rather than pretend the
    disk is full. An empty path means no journal is open yet, which is
    not an incident: it is reported without a log line.
    """
    if path:
        flog(f"disk_monitor: {reason}", "WARNING")
    return DiskStatus(
        free_bytes=0,
        total_bytes=0,
        volume_path=_extract_volume(path) if path else "",
        is_low=True,
        is_critical=True,
    )


def format_disk_message(status: DiskStatus) -> str:
    """Build user-facing message from disk status."""
    if status.is_critical and status.total_bytes <= 0:
        # No reading at all: naming a free size here would be a lie.
        return _tr(
            "Volume du journal inaccessible{volume}. "
            "L'enregistrement a ete desactive pour eviter la perte de donnees."
        ).format(
            volume=f" ({status.volume_path})" if status.volume_path else "",
        )
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
