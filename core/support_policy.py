"""Provider support policy for RecoverLand plugin (RLU-001, RLU-026).

Determines which QGIS layers are supported for audit capture and restoration.
Each provider gets a policy with support level, identity strength, and phase.
"""
from enum import Enum
from typing import NamedTuple


class IdentityStrength(Enum):
    STRONG = "strong"
    MEDIUM = "medium"
    WEAK = "weak"
    NONE = "none"


class SupportLevel(Enum):
    FULL = "full"
    PARTIAL = "partial"
    INFORMATIONAL = "informational"
    REFUSED = "refused"


class ProviderPolicy(NamedTuple):
    provider_name: str
    support_level: SupportLevel
    identity_strength: IdentityStrength
    capture: bool
    restore: bool
    phase: int
    reason: str


_PROVIDER_POLICIES = {
    "postgres": ProviderPolicy(
        "postgres", SupportLevel.FULL, IdentityStrength.STRONG,
        True, True, 0, "Legacy + local"
    ),
    "ogr": ProviderPolicy(
        "ogr", SupportLevel.FULL, IdentityStrength.MEDIUM,
        True, True, 1, "GeoPackage, Shapefile, GeoJSON, etc."
    ),
    "spatialite": ProviderPolicy(
        "spatialite", SupportLevel.FULL, IdentityStrength.STRONG,
        True, True, 1, "SQLite spatial"
    ),
    "memory": ProviderPolicy(
        "memory", SupportLevel.INFORMATIONAL, IdentityStrength.NONE,
        True, False, 0, "Non-persisted data"
    ),
    "virtual": ProviderPolicy(
        "virtual", SupportLevel.REFUSED, IdentityStrength.NONE,
        False, False, 0, "Derived layer, not a primary source"
    ),
    "delimitedtext": ProviderPolicy(
        "delimitedtext", SupportLevel.PARTIAL, IdentityStrength.WEAK,
        True, True, 2, "CSV/delimited text"
    ),
    "wfs": ProviderPolicy(
        "wfs", SupportLevel.PARTIAL, IdentityStrength.MEDIUM,
        True, True, 3, "WFS-T service"
    ),
    "mssql": ProviderPolicy(
        "mssql", SupportLevel.FULL, IdentityStrength.STRONG,
        True, True, 3, "MS SQL Server"
    ),
    "oracle": ProviderPolicy(
        "oracle", SupportLevel.FULL, IdentityStrength.STRONG,
        True, True, 3, "Oracle Spatial"
    ),
}

_DEFAULT_POLICY = ProviderPolicy(
    "unknown", SupportLevel.REFUSED, IdentityStrength.NONE,
    False, False, 99, "Unknown provider"
)

_EDIT_CAPABILITIES_MASK = 1 | 2 | 4 | 8


def get_provider_policy(provider_name: str) -> ProviderPolicy:
    return _PROVIDER_POLICIES.get(provider_name, _DEFAULT_POLICY)


def evaluate_layer_support(layer) -> ProviderPolicy:
    """Evaluate support level for a QgsVectorLayer."""
    if layer is None:
        return _DEFAULT_POLICY

    provider = layer.dataProvider()
    if provider is None:
        return _DEFAULT_POLICY

    provider_name = provider.name()
    policy = get_provider_policy(provider_name)

    if policy.support_level == SupportLevel.REFUSED:
        return policy

    if not _has_edit_capabilities(provider):
        return ProviderPolicy(
            provider_name, SupportLevel.REFUSED, IdentityStrength.NONE,
            False, False, policy.phase, "Layer is not editable"
        )

    if provider_name == "ogr":
        strength = refine_ogr_identity(layer.source())
        return policy._replace(identity_strength=strength)

    return policy


def _has_edit_capabilities(provider) -> bool:
    caps = provider.capabilities()
    return bool(caps & _EDIT_CAPABILITIES_MASK)


def refine_ogr_identity(source_uri: str) -> IdentityStrength:
    """Refine identity strength for OGR sub-formats."""
    lower = source_uri.lower()
    if '.gpkg' in lower:
        return IdentityStrength.STRONG
    if '.sqlite' in lower:
        return IdentityStrength.STRONG
    if '.dbf' in lower:
        return IdentityStrength.MEDIUM
    if '.db' in lower:
        return IdentityStrength.STRONG
    if '.shp' in lower:
        return IdentityStrength.MEDIUM
    if '.fgb' in lower:
        return IdentityStrength.STRONG
    if '.geojson' in lower or '.json' in lower:
        return IdentityStrength.MEDIUM
    if '.csv' in lower or '.xlsx' in lower or '.ods' in lower:
        return IdentityStrength.WEAK
    if '.kml' in lower or '.kmz' in lower:
        return IdentityStrength.WEAK
    return IdentityStrength.MEDIUM


def is_capture_supported(layer) -> bool:
    policy = evaluate_layer_support(layer)
    return policy.capture


def is_restore_supported(layer) -> bool:
    policy = evaluate_layer_support(layer)
    return policy.restore


def format_support_message(policy: ProviderPolicy) -> str:
    """Human-readable support status for UI display."""
    if policy.support_level == SupportLevel.FULL:
        return f"Fully supported ({policy.reason})"
    if policy.support_level == SupportLevel.PARTIAL:
        return f"Partially supported: {policy.reason}"
    if policy.support_level == SupportLevel.INFORMATIONAL:
        return f"Informational only: {policy.reason}"
    return f"Not supported: {policy.reason}"
