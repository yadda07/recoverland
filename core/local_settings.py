"""Local backend settings for RecoverLand (RLU-060).

Manages opt-in activation, user identity override, retention policy,
and per-project configuration. Settings persist in the SQLite journal
via the backend_settings table.
"""
import sqlite3
from datetime import datetime, timezone
from typing import Optional, Dict

from .logger import flog

_SETTING_KEYS = {
    "local_mode_active": "0",
    "user_name_override": "",
    "retention_days": "365",
    "max_events": "1000000",
    "capture_inserts": "1",
}


class LocalSettings:
    """Read/write settings from the backend_settings table."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._cache: Dict[str, str] = {}
        self._load_all()

    def _load_all(self) -> None:
        try:
            rows = self._conn.execute(
                "SELECT setting_key, setting_value FROM backend_settings"
            ).fetchall()
            self._cache = {r[0]: r[1] for r in rows}
        except sqlite3.Error:
            self._cache = {}

    def get(self, key: str) -> str:
        if key in self._cache:
            return self._cache[key]
        return _SETTING_KEYS.get(key, "")

    def set(self, key: str, value: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._conn:
                self._conn.execute(
                    """INSERT INTO backend_settings (setting_key, setting_value, updated_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(setting_key) DO UPDATE SET setting_value=?, updated_at=?""",
                    (key, value, now, value, now),
                )
            self._cache[key] = value
        except sqlite3.Error as e:
            flog(f"LocalSettings.set error: {e}", "ERROR")

    @property
    def is_local_active(self) -> bool:
        return self.get("local_mode_active") == "1"

    def activate_local_mode(self) -> None:
        self.set("local_mode_active", "1")
        flog("LocalSettings: local mode activated")

    def deactivate_local_mode(self) -> None:
        self.set("local_mode_active", "0")
        flog("LocalSettings: local mode deactivated")

    @property
    def user_name_override(self) -> Optional[str]:
        val = self.get("user_name_override")
        return val if val else None

    def set_user_name_override(self, name: str) -> None:
        self.set("user_name_override", name.strip())

    @property
    def retention_days(self) -> int:
        try:
            return int(self.get("retention_days"))
        except ValueError:
            return 365

    def set_retention_days(self, days: int) -> None:
        if days < 1:
            raise ValueError("Retention must be at least 1 day")
        self.set("retention_days", str(days))

    @property
    def max_events(self) -> int:
        try:
            return int(self.get("max_events"))
        except ValueError:
            return 1_000_000

    @property
    def capture_inserts(self) -> bool:
        return self.get("capture_inserts") == "1"

    def set_capture_inserts(self, enabled: str) -> None:
        self.set("capture_inserts", enabled)

    def to_dict(self) -> Dict[str, str]:
        result = dict(_SETTING_KEYS)
        result.update(self._cache)
        return result
