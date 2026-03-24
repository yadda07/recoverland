"""Backend router for RecoverLand (RLU-050, RLU-051, RLU-052).

Routes audit operations to the appropriate backend based on layer context.
One backend per layer per session. No silent fallback between backends.
"""
from typing import Dict, Optional

from .audit_backend import AuditBackend
from .sqlite_backend import SQLiteAuditBackend
from .pg_backend import PostgreSQLAuditBackend
from .support_policy import evaluate_layer_support, SupportLevel
from .identity import compute_datasource_fingerprint
from .logger import flog


class BackendMode:
    POSTGRES_LEGACY = "postgres_legacy"
    LOCAL_SQLITE = "local_sqlite"
    NONE = "none"


class BackendRouter:
    """Routes layers to the correct audit backend.

    Rules:
    - PostgreSQL layers with known audit schemas use the legacy backend.
    - All other editable layers use the local SQLite backend.
    - Unsupported layers get no backend (explicit refusal).
    - No double-journaling: one backend per layer.
    """

    def __init__(self):
        self._pg_backend: Optional[PostgreSQLAuditBackend] = None
        self._sqlite_backend: Optional[SQLiteAuditBackend] = None
        self._layer_modes: Dict[str, str] = {}
        self._local_active = False

    def set_pg_backend(self, backend: PostgreSQLAuditBackend) -> None:
        self._pg_backend = backend

    def set_sqlite_backend(self, backend: SQLiteAuditBackend) -> None:
        self._sqlite_backend = backend

    def activate_local_mode(self) -> None:
        self._local_active = True
        flog("BackendRouter: local mode activated")

    def deactivate_local_mode(self) -> None:
        self._local_active = False
        flog("BackendRouter: local mode deactivated")

    @property
    def is_local_active(self) -> bool:
        return self._local_active

    def resolve_backend(self, layer) -> Optional[AuditBackend]:
        """Determine which backend handles a given layer.

        Returns None if the layer is not supported.
        Never returns silently wrong backend.
        """
        layer_id = layer.id()

        cached_mode = self._layer_modes.get(layer_id)
        if cached_mode == BackendMode.POSTGRES_LEGACY:
            return self._pg_backend
        if cached_mode == BackendMode.LOCAL_SQLITE:
            return self._sqlite_backend
        if cached_mode == BackendMode.NONE:
            return None

        mode = self._determine_mode(layer)
        self._layer_modes[layer_id] = mode
        return self._backend_for_mode(mode)

    def resolve_mode(self, layer) -> str:
        """Return the backend mode string for a layer."""
        layer_id = layer.id()
        if layer_id in self._layer_modes:
            return self._layer_modes[layer_id]
        mode = self._determine_mode(layer)
        self._layer_modes[layer_id] = mode
        return mode

    def get_search_backend(self, layer) -> Optional[AuditBackend]:
        """Get the backend for searching a layer's audit history."""
        return self.resolve_backend(layer)

    def invalidate_layer(self, layer_id: str) -> None:
        """Remove cached backend assignment for a layer."""
        self._layer_modes.pop(layer_id, None)

    def clear_cache(self) -> None:
        self._layer_modes.clear()

    def _determine_mode(self, layer) -> str:
        policy = evaluate_layer_support(layer)
        if policy.support_level == SupportLevel.REFUSED:
            return BackendMode.NONE

        provider_name = layer.dataProvider().name()
        if provider_name == "postgres" and self._pg_backend is not None:
            if self._pg_backend.is_available():
                return BackendMode.POSTGRES_LEGACY

        if self._local_active and self._sqlite_backend is not None:
            if self._sqlite_backend.is_available():
                return BackendMode.LOCAL_SQLITE

        return BackendMode.NONE

    def _backend_for_mode(self, mode: str) -> Optional[AuditBackend]:
        if mode == BackendMode.POSTGRES_LEGACY:
            return self._pg_backend
        if mode == BackendMode.LOCAL_SQLITE:
            return self._sqlite_backend
        return None


def format_mode_display(mode: str) -> str:
    """Human-readable backend mode for UI display."""
    if mode == BackendMode.POSTGRES_LEGACY:
        return "PostgreSQL (server-side audit)"
    if mode == BackendMode.LOCAL_SQLITE:
        return "Local (SQLite audit)"
    return "Not audited"
