"""Audit field filtering policy for RecoverLand.

Defines which layer fields are considered 'audit metadata' (date_modif,
modif_par, updated_at, etc.) and should be excluded from change detection,
delta computation, and restore operations.

Centralised here so that edit_tracker, search_service, restore_service
and serialization all share the same source of truth.
"""
import re
import unicodedata
from typing import Any


_LAYER_AUDIT_FIELD_NAMES = frozenset([
    "audittimestamp",
    "audituser",
    "datemodif",
    "datemodification",
    "lasteditedat",
    "lasteditedby",
    "modifpar",
    "modifiepar",
    "updatedat",
    "updatedby",
    "username",
])

_AUDIT_PREFIXES = (
    "audittimestamp",
    "audituser",
    "datemodif",
    "datemodification",
    "modifpar",
    "modifiepar",
    "updatedat",
    "updatedby",
)


def is_layer_audit_field(field_name: Any) -> bool:
    """Return True if the field is an audit-metadata field to be ignored."""
    normalized = _normalize_field_name(field_name)
    if not normalized:
        return False
    if normalized in _LAYER_AUDIT_FIELD_NAMES:
        return True
    for prefix in _AUDIT_PREFIXES:
        if normalized.startswith(prefix):
            return True
    return False


def _normalize_field_name(field_name: Any) -> str:
    if not isinstance(field_name, str):
        return ""
    normalized = unicodedata.normalize("NFKD", field_name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", ascii_name.lower())
