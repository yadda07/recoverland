"""Human-friendly time formatting for RecoverLand (UX-E01).

Converts ISO timestamps to relative or short absolute representations.
Pure Python, no Qt dependency.
"""
from datetime import datetime, timezone, timedelta
from typing import Optional


def format_relative_time(iso_timestamp: str) -> str:
    """Format an ISO timestamp as relative time ("il y a 5 min").

    Falls back to short absolute format for timestamps older than 7 days.
    Returns the raw string if parsing fails.
    """
    dt = _parse_iso(iso_timestamp)
    if dt is None:
        return iso_timestamp[:19].replace("T", " ") if iso_timestamp else ""

    now = datetime.now(timezone.utc)
    delta = now - dt

    if delta.total_seconds() < 0:
        return format_short_absolute(iso_timestamp)

    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "a l'instant" if seconds < 10 else f"il y a {seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"il y a {minutes} min"
    hours = minutes // 60
    if hours < 24:
        return f"il y a {hours}h"

    days = delta.days
    if days == 1:
        return f"hier {dt.strftime('%H:%M')}"
    if days < 7:
        return f"il y a {days}j"

    return format_short_absolute(iso_timestamp)


def format_short_absolute(iso_timestamp: str) -> str:
    """Format as short absolute date: "15/06 14:32" or "15/06/2024 14:32"."""
    dt = _parse_iso(iso_timestamp)
    if dt is None:
        return iso_timestamp[:19].replace("T", " ") if iso_timestamp else ""

    now = datetime.now(timezone.utc)
    if dt.year == now.year:
        return dt.strftime("%d/%m %H:%M")
    return dt.strftime("%d/%m/%Y %H:%M")


def format_full_timestamp(iso_timestamp: str) -> str:
    """Format as full timestamp with timezone for tooltips."""
    dt = _parse_iso(iso_timestamp)
    if dt is None:
        return iso_timestamp or ""
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def compute_history_span(oldest: str, newest: str) -> str:
    """Compute a human-readable span between oldest and newest events."""
    dt_old = _parse_iso(oldest)
    dt_new = _parse_iso(newest)
    if dt_old is None or dt_new is None:
        return ""
    delta = dt_new - dt_old
    days = delta.days
    if days == 0:
        hours = int(delta.total_seconds() // 3600)
        if hours == 0:
            return "< 1 heure"
        return f"{hours} heure(s)"
    if days < 30:
        return f"{days} jour(s)"
    months = days // 30
    if months < 12:
        return f"{months} mois"
    years = days // 365
    remainder = (days % 365) // 30
    if remainder > 0:
        return f"{years} an(s) et {remainder} mois"
    return f"{years} an(s)"


def _parse_iso(timestamp: str) -> Optional[datetime]:
    """Parse ISO timestamp string to timezone-aware datetime."""
    if not timestamp or not isinstance(timestamp, str):
        return None
    cleaned = timestamp.strip()
    if not cleaned:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(cleaned, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None
