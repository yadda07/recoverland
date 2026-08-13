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


# Closed list of row-timestamp / row-author columns, the ones a database
# trigger rewrites on its own. Excluding them is what keeps a rewind from
# fighting the trigger.
#
# Two names were removed from this list because they are business data,
# not audit noise:
#   - the shp2pgsql default primary key: it is the very column
#     identity.compute_feature_identity uses to find the entity again.
#     Filtering it meant the re-inserted feature got a brand new key and
#     lost its whole history in one restore.
#   - the account/holder column: on a lease, concession or account layer
#     it names the title holder.
# Removing a name only ever ADDS fields to future deltas and restores;
# events already written stay readable, since the filter is applied when
# reading them too.
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
])


def is_layer_audit_field(field_name: Any) -> bool:
    """Return True if the field is an audit-metadata field to be ignored.

    Exact match on the normalized name only. Prefix matching used to
    swallow every column merely STARTING with an audit token, whatever
    followed: a business column such as ``date_modification_parcelle``
    was classified as audit noise and silently dropped from both the
    delta and the restore. A closed list can be audited; a prefix net
    cannot, and no naming convention put a business column out of reach.
    """
    normalized = _normalize_field_name(field_name)
    if not normalized:
        return False
    return normalized in _LAYER_AUDIT_FIELD_NAMES


def _normalize_field_name(field_name: Any) -> str:
    if not isinstance(field_name, str):
        return ""
    normalized = unicodedata.normalize("NFKD", field_name)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", ascii_name.lower())
