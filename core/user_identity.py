"""User identification for local audit mode (RLU-027).

Resolves the current user name from multiple sources, in priority order:
1. Plugin configuration (explicit override)
2. Environment variable RECOVERLAND_USER
3. OS login name
4. QGIS profile name
5. Fallback: 'unknown'
"""
import os
from typing import Optional

_ENVVAR_NAME = "RECOVERLAND_USER"
_FALLBACK_USER = "unknown"
_MAX_USER_LEN = 200
_cached_user: Optional[str] = None


def resolve_user_name(plugin_config_user: Optional[str] = None) -> str:
    """Resolve current user name. Never returns empty or None."""
    global _cached_user

    if plugin_config_user and plugin_config_user.strip():
        _cached_user = plugin_config_user.strip()
        return _cached_user

    if _cached_user is not None:
        return _cached_user

    _cached_user = _resolve_from_sources()
    return _cached_user


def _sanitize(name: str) -> str:
    """Strip control characters and limit length for safe storage."""
    clean = "".join(ch for ch in name if ch.isprintable())
    clean = clean.strip()[:_MAX_USER_LEN]
    return clean if clean else _FALLBACK_USER


def _resolve_from_sources() -> str:
    env_user = os.environ.get(_ENVVAR_NAME, "").strip()
    if env_user:
        return _sanitize(env_user)

    os_user = _get_os_login()
    if os_user:
        return _sanitize(os_user)

    profile_user = _get_qgis_profile_name()
    if profile_user:
        return _sanitize(profile_user)

    return _FALLBACK_USER


def _get_os_login() -> Optional[str]:
    try:
        login = os.getlogin()
        if login and login.strip():
            return login.strip()
    except OSError:
        pass

    for var in ("USERNAME", "USER", "LOGNAME"):
        val = os.environ.get(var, "").strip()
        if val:
            return val
    return None


def _get_qgis_profile_name() -> Optional[str]:
    try:
        from qgis.core import QgsApplication
        profile_folder = QgsApplication.qgisSettingsDirPath()
        if profile_folder:
            parts = profile_folder.replace("\\", "/").rstrip("/").split("/")
            if len(parts) >= 2 and parts[-2] == "profiles":
                name = parts[-1]
                if name and name != "default":
                    return name
    except Exception:
        pass
    return None


def invalidate_cache() -> None:
    global _cached_user
    _cached_user = None
