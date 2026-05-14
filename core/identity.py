"""Layer and entity identity for RecoverLand (RLU-011).

Computes stable fingerprints for datasources and features.
Datasource fingerprint = provider_type + normalized source URI.
Feature identity = best available primary key or FID.
"""
import json
import os
import re
from typing import Optional, Dict, Any

from .support_policy import IdentityStrength, refine_ogr_identity

# BL-RW-P2-13 (CR-1): portable fingerprints across machines.
#
# `RECOVERLAND_FINGERPRINT_MODE` env var controls how file-based sources
# (provider in {ogr, spatialite, delimitedtext}) are normalised inside
# `compute_datasource_fingerprint`:
#   - 'absolute' (default, legacy behaviour) -> os.path.abspath the path.
#   - 'relative'                              -> make the path relative
#     to `QgsProject.instance().homePath()`. When the project has no
#     home (unsaved project, no QGIS runtime, exception), the function
#     falls back to absolute mode and emits a WARNING log so the
#     degraded state is visible to operators.
# In both modes the path keeps `os.path.normcase` + forward slashes so
# the canonical form is identical to the historical layout, byte for
# byte, when mode is 'absolute'.
_FINGERPRINT_MODE_ENV = "RECOVERLAND_FINGERPRINT_MODE"
_FILE_PROVIDERS = ("ogr", "spatialite", "delimitedtext")


def _get_fingerprint_mode() -> str:
    raw = os.environ.get(_FINGERPRINT_MODE_ENV) or ""
    mode = raw.strip().lower()
    if mode in ("absolute", "relative"):
        return mode
    return "absolute"


def compute_datasource_fingerprint(layer) -> str:
    """Compute a deterministic fingerprint for a layer's data source.

    Format: 'provider::normalized_source'
    """
    provider = layer.dataProvider()
    provider_name = provider.name()
    raw_source = layer.source()
    normalized = _normalize_source_uri(provider_name, raw_source)
    return f"{provider_name}::{normalized}"


def datasource_fingerprints_match(stored: str, current: str) -> bool:
    """Return True when `stored` and `current` denote the same datasource.

    Strict equality first; cross-mode resolution (absolute vs relative
    against the current QGIS project home) as fallback so that audit
    records produced before BL-RW-P2-13 keep matching layers loaded in
    the new relative mode. Cross-mode only applies to file-based
    providers (`ogr` / `spatialite` / `delimitedtext`); DB fingerprints
    are compared strictly.
    """
    if not stored or not current:
        return stored == current
    if stored == current:
        return True
    return _cross_mode_match(stored, current)


def _cross_mode_match(stored: str, current: str) -> bool:
    """Resolve both fingerprints against the project home and compare."""
    if "::" not in stored or "::" not in current:
        return False
    stored_provider, _, stored_src = stored.partition("::")
    current_provider, _, current_src = current.partition("::")
    if stored_provider != current_provider:
        return False
    if stored_provider not in _FILE_PROVIDERS:
        return False
    home = _qgs_project_home_path()
    if not home:
        return False
    try:
        abs_home = os.path.abspath(home)
    except (OSError, ValueError):
        return False
    return _absolutize(stored_src, abs_home) == _absolutize(current_src, abs_home)


def _absolutize(source: str, abs_home: str) -> str:
    """Best-effort canonical absolute representation of a file source."""
    path = source.split("|")[0].strip().replace("\\", "/")
    if not os.path.isabs(path):
        path = os.path.join(abs_home, path)
    try:
        path = os.path.abspath(path)
        path = os.path.normcase(path)
    except (OSError, ValueError):
        pass
    path = path.replace("\\", "/")
    suffix = "|" + source.split("|", 1)[1] if "|" in source else ""
    return path + suffix


def _qgs_project_home_path():
    """Return QgsProject.instance().homePath() or '' if unavailable."""
    try:
        from qgis.core import QgsProject
        return QgsProject.instance().homePath() or ""
    except Exception:
        return ""


# DB source normalization profiles.
# Each profile lists (key, default) tuples in the EXACT order they must
# appear in the normalized fingerprint string. Order matters: the
# resulting string is the canonical key for the audit datasource and
# any reordering would change every fingerprint already stored.
_DB_NORMALIZATION_PROFILES = {
    "postgres": (
        ("host", ""),
        ("port", "5432"),
        ("dbname", ""),
        ("schema", "public"),
        ("table", ""),
    ),
    "mssql": (
        ("host", ""),
        ("port", "1433"),
        ("dbname", ""),
        ("schema", "dbo"),
        ("table", ""),
    ),
    "oracle": (
        ("host", ""),
        ("port", "1521"),
        ("dbname", ""),
        ("table", ""),
    ),
}


def _normalize_source_uri(provider_name: str, raw_source: str) -> str:
    """Normalize a source URI for deterministic fingerprinting."""
    profile = _DB_NORMALIZATION_PROFILES.get(provider_name)
    if profile is not None:
        return _normalize_db_source(raw_source, profile)
    if provider_name in ("ogr", "spatialite", "delimitedtext"):
        return _normalize_file_source(raw_source)
    return raw_source.strip()


def _normalize_db_source(raw: str, profile) -> str:
    """Extract stable parts from a DB URI according to a normalization profile.

    Same regex pipeline used historically for postgres / mssql / oracle:
      key='value'  -> single-quoted form
      key="value"  -> double-quoted form
      key=value    -> bare token form (whitespace-terminated)

    Output keeps the historical key=value space-separated layout so
    every fingerprint already stored stays valid byte-for-byte.
    """
    parts = {}
    for key, _default in profile:
        match = re.search(rf"{key}='([^']*)'", raw)
        if not match:
            match = re.search(rf'{key}="([^"]*)"', raw)
        if not match:
            match = re.search(rf"{key}=(\S+)", raw)
        if match:
            parts[key] = match.group(1)
    return " ".join(
        f"{key}={parts.get(key, default)}"
        for key, default in profile
    )


def _normalize_file_source(raw: str) -> str:
    """Normalize a file-based source URI.

    Honours `RECOVERLAND_FINGERPRINT_MODE`:
      - 'absolute' (default) -> historical behaviour: abspath + normcase.
      - 'relative'           -> path made relative to the current QGIS
        project home, with `os.path.normcase` still applied for case
        consistency. Falls back to absolute mode with a WARNING log
        when no project home is available.
    """
    path = raw.split("|")[0].strip()
    path = path.replace("\\", "/")
    mode = _get_fingerprint_mode()
    if mode == "relative":
        rel_path = _try_relative_to_project_home(path)
        if rel_path is not None:
            path = rel_path
        else:
            from .logger import flog
            flog(
                f"identity: RECOVERLAND_FINGERPRINT_MODE=relative but "
                f"QgsProject homePath() is empty/absent; falling back "
                f"to absolute path for source={raw!r}",
                "WARNING",
            )
            try:
                path = os.path.abspath(path)
            except (OSError, ValueError):
                pass
    else:
        try:
            path = os.path.abspath(path)
        except (OSError, ValueError):
            pass
    try:
        path = os.path.normcase(path)
    except (OSError, ValueError):
        pass
    path = path.replace("\\", "/")
    suffix = ""
    if "|" in raw:
        suffix = "|" + raw.split("|", 1)[1]
    return path + suffix


def _try_relative_to_project_home(path: str) -> Optional[str]:
    """Return `path` rewritten relative to QgsProject.homePath() or None.

    Returns None when the project has no home, when the QGIS runtime is
    not available, or when the path lives on a different drive / mount
    point so that no relative form exists.
    """
    home = _qgs_project_home_path()
    if not home:
        return None
    try:
        abs_path = os.path.abspath(path)
        abs_home = os.path.abspath(home)
        rel = os.path.relpath(abs_path, abs_home)
    except (OSError, ValueError):
        return None
    return rel.replace("\\", "/")


def compute_feature_identity(layer, feature) -> str:
    """Compute feature identity JSON for a given feature.

    Returns JSON string like: {"fid": 42} or {"fid": 42, "pk_field": "gid", "pk_value": 42}
    """
    identity: Dict[str, Any] = {"fid": feature.id()}

    pk_indices = layer.dataProvider().pkAttributeIndexes()
    if pk_indices:
        fields = layer.fields()
        for idx in pk_indices:
            if 0 <= idx < fields.count():
                field = fields.at(idx)
                try:
                    val = feature[field.name()]
                    if val is not None:
                        identity["pk_field"] = field.name()
                        identity["pk_value"] = _safe_pk_value(val)
                        break
                except (KeyError, IndexError) as exc:
                    # PK field declared by provider but not on this feature;
                    # fall through to the next candidate.
                    from .logger import flog
                    flog(f"identity: PK field {field.name()!r} not available on feature: {exc}", "DEBUG")

    return json.dumps(identity, ensure_ascii=False)


def _safe_pk_value(value: Any) -> Any:
    if isinstance(value, (int, float, str)):
        return value
    return str(value)


def compute_entity_fingerprint(identity_json: Optional[str]) -> Optional[str]:
    """Compute a stable, indexable fingerprint from feature_identity_json.

    Returns a canonical string like 'pk:field_name=value' or 'fid:123'.
    Returns None if identity cannot be determined.
    """
    if not identity_json or not isinstance(identity_json, str):
        return None
    try:
        identity = json.loads(identity_json)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(identity, dict):
        return None
    pk_field = identity.get("pk_field")
    pk_value = identity.get("pk_value")
    if pk_field and pk_value is not None:
        return f"pk:{pk_field}={pk_value}"
    fid = identity.get("fid")
    if fid is not None:
        return f"fid:{fid}"
    return None


def get_identity_strength_for_layer(layer) -> IdentityStrength:
    """Determine identity strength for a specific layer."""
    provider_name = layer.dataProvider().name()

    if provider_name in ("postgres", "spatialite", "mssql", "oracle"):
        return IdentityStrength.STRONG

    if provider_name == "ogr":
        return refine_ogr_identity(layer.source())

    if provider_name == "memory":
        return IdentityStrength.NONE

    if provider_name == "delimitedtext":
        return IdentityStrength.WEAK

    return IdentityStrength.MEDIUM


def compute_project_fingerprint() -> str:
    """Compute a fingerprint for the current QGIS project."""
    try:
        from qgis.core import QgsProject
        project = QgsProject.instance()
        path = project.absoluteFilePath()
        if path:
            normalized = os.path.abspath(path).replace("\\", "/")
            return f"project::{normalized}"
        return "project::unsaved"
    except Exception:
        return "project::unknown"


def extract_layer_name(layer) -> str:
    """Extract a human-readable layer name."""
    return layer.name() or "unnamed"
