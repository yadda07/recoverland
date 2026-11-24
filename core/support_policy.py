# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Provider support policy for RecoverLand plugin (RLU-001, RLU-026).

Determines which QGIS layers are supported for audit capture and restoration.
Each provider gets a policy with support level, identity strength, and phase.
"""
import os
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
    # capture=False: EditSessionTracker refuses every layer whose identity
    # strength is NONE, so announcing capture=True made the public API
    # promise a protection the tracker never delivers.
    "memory": ProviderPolicy(
        "memory", SupportLevel.INFORMATIONAL, IdentityStrength.NONE,
        False, False, 0, "Non-persisted data, no stable feature identity"
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


def _edit_capabilities_mask():
    from ..compat import QgisCompat
    mask = QgisCompat.CAP_ADD_FEATURES
    mask |= QgisCompat.CAP_DELETE_FEATURES
    mask |= QgisCompat.CAP_CHANGE_ATTRIBUTE_VALUES
    mask |= QgisCompat.CAP_CHANGE_GEOMETRIES
    return mask


def get_provider_policy(provider_name: str) -> ProviderPolicy:
    return _PROVIDER_POLICIES.get(provider_name, _DEFAULT_POLICY)


def _is_truthy_custom_prop(val) -> bool:
    """Interpret QGIS customProperty values safely (XML stores 'false'/'0' as str)."""
    if val is None or val is False or val == 0:
        return False
    if isinstance(val, str):
        return val.strip().lower() not in ("", "0", "false", "no", "none")
    return bool(val)


def is_managed_overlay_layer(layer) -> bool:
    """Return True if *layer* is a RecoverLand Review/Lens managed overlay."""
    if layer is None:
        return False
    try:
        return (
            _is_truthy_custom_prop(layer.customProperty("_rl_snap_managed"))
            or _is_truthy_custom_prop(layer.customProperty("_rl_lens_managed"))
        )
    except (RuntimeError, AttributeError):
        return False


def is_ephemeral_layer(layer) -> bool:
    """Return True for non-persisted temporary, memory, virtual, or plugin overlay layers.

    Temporary scratch layers, Processing memory outputs, virtual layers, and
    RecoverLand's own Review/Lens overlays carry no durable on-disk identity
    and must never trigger user-facing error banners when edited.
    """
    if layer is None:
        return False
    if is_managed_overlay_layer(layer):
        return True
    try:
        if hasattr(layer, "isTemporary") and layer.isTemporary():
            return True
    except (RuntimeError, AttributeError):
        pass
    try:
        provider = layer.dataProvider()
        if provider is not None and provider.name() in ("memory", "virtual"):
            return True
    except (RuntimeError, AttributeError):
        pass
    return False


def evaluate_layer_support(layer) -> ProviderPolicy:
    """Evaluate support level for a QgsVectorLayer."""
    if layer is None:
        return _DEFAULT_POLICY

    try:
        if hasattr(layer, "isValid") and not layer.isValid():
            return _DEFAULT_POLICY
    except (RuntimeError, AttributeError):
        return _DEFAULT_POLICY

    try:
        provider = layer.dataProvider()
    except (RuntimeError, AttributeError):
        return _DEFAULT_POLICY
    if provider is None:
        return _DEFAULT_POLICY

    provider_name = provider.name()
    policy = get_provider_policy(provider_name)

    if policy.support_level == SupportLevel.REFUSED:
        return policy

    if policy.support_level == SupportLevel.INFORMATIONAL:
        return policy

    try:
        if hasattr(layer, "isReadOnly") and layer.isReadOnly():
            return ProviderPolicy(
                provider_name, SupportLevel.REFUSED, IdentityStrength.NONE,
                False, False, policy.phase, "Layer is read-only"
            )
    except (RuntimeError, AttributeError):
        pass

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
    return bool(caps & _edit_capabilities_mask())


_OGR_EXT_STRENGTH = {
    ".gpkg": IdentityStrength.STRONG,
    ".sqlite": IdentityStrength.STRONG,
    ".db": IdentityStrength.STRONG,
    ".gdb": IdentityStrength.STRONG,
    ".fgb": IdentityStrength.STRONG,
    ".shp": IdentityStrength.MEDIUM,
    ".dbf": IdentityStrength.MEDIUM,
    ".geojson": IdentityStrength.MEDIUM,
    ".json": IdentityStrength.MEDIUM,
    ".csv": IdentityStrength.WEAK,
    ".xlsx": IdentityStrength.WEAK,
    ".ods": IdentityStrength.WEAK,
    ".kml": IdentityStrength.WEAK,
    ".kmz": IdentityStrength.WEAK,
}


def refine_ogr_identity(source_uri: str) -> IdentityStrength:
    """Refine identity strength for OGR sub-formats using file extension."""
    if not source_uri:
        return IdentityStrength.MEDIUM
    head = source_uri.split("|", 1)[0].strip()
    if head.startswith("/vsizip/") or head.startswith("/vsigzip/"):
        head = head.rsplit("/", 1)[-1]
    _, ext = os.path.splitext(head.rstrip("/\\"))
    ext_lower = ext.lower()
    if ext_lower in _OGR_EXT_STRENGTH:
        return _OGR_EXT_STRENGTH[ext_lower]
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
