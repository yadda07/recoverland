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


def compute_datasource_fingerprint(layer) -> str:
    """Compute a deterministic fingerprint for a layer's data source.

    Format: 'provider::normalized_source'
    """
    provider = layer.dataProvider()
    provider_name = provider.name()
    raw_source = layer.source()
    normalized = _normalize_source_uri(provider_name, raw_source)
    return f"{provider_name}::{normalized}"


def _normalize_source_uri(provider_name: str, raw_source: str) -> str:
    """Normalize a source URI for deterministic fingerprinting."""
    if provider_name == "postgres":
        return _normalize_pg_source(raw_source)
    if provider_name in ("ogr", "spatialite", "delimitedtext"):
        return _normalize_file_source(raw_source)
    return raw_source.strip()


def _normalize_pg_source(raw: str) -> str:
    """Extract stable parts from a PostgreSQL URI."""
    parts = {}
    for key in ("host", "port", "dbname", "schema", "table"):
        match = re.search(rf"""{key}='([^']*)'""", raw)
        if not match:
            match = re.search(rf'{key}="([^"]*)"', raw)
        if not match:
            match = re.search(rf"{key}=(\S+)", raw)
        if match:
            parts[key] = match.group(1)
    host = parts.get("host", "")
    port = parts.get("port", "5432")
    dbname = parts.get("dbname", "")
    schema = parts.get("schema", "public")
    table = parts.get("table", "")
    return f"host={host} port={port} dbname={dbname} schema={schema} table={table}"


def _normalize_file_source(raw: str) -> str:
    """Normalize a file-based source URI to absolute path."""
    path = raw.split("|")[0].strip()
    path = path.replace("\\", "/")
    try:
        path = os.path.abspath(path)
        path = os.path.normcase(path)
        path = path.replace("\\", "/")
    except (OSError, ValueError):
        pass
    suffix = ""
    if "|" in raw:
        suffix = "|" + raw.split("|", 1)[1]
    return path + suffix


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
                except (KeyError, IndexError):
                    pass

    return json.dumps(identity, ensure_ascii=False)


def _safe_pk_value(value: Any) -> Any:
    if isinstance(value, (int, float, str)):
        return value
    return str(value)


def get_identity_strength_for_layer(layer) -> IdentityStrength:
    """Determine identity strength for a specific layer."""
    provider_name = layer.dataProvider().name()

    if provider_name == "postgres":
        return IdentityStrength.STRONG

    if provider_name == "spatialite":
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
