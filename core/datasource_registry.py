"""Datasource registry for RecoverLand.

Stores the raw QGIS source URI for each audited datasource fingerprint.
Used at restore time to reconnect to a datasource when the layer is not
currently loaded in the project.

Security: stores authcfg references (not passwords). Passwords are never
persisted. The QGIS auth system resolves credentials at connection time.
"""
import sqlite3
import re
from datetime import datetime, timezone
from typing import Optional, NamedTuple

from .logger import flog


class DatasourceInfo(NamedTuple):
    fingerprint: str
    provider_type: str
    source_uri: str
    layer_name: str
    authcfg: str
    crs_authid: str
    geometry_type: str


def register_datasource(conn: sqlite3.Connection, layer) -> None:
    """Store or update the source URI for a layer in the registry.

    Extracts provider, URI, authcfg, CRS and geometry type from the
    live QGIS layer. Called at capture time (after first commit on a layer).
    """
    from .identity import compute_datasource_fingerprint
    from .geometry_utils import extract_geometry_type, extract_crs_authid

    fingerprint = compute_datasource_fingerprint(layer)
    provider_type = layer.dataProvider().name()
    source_uri = layer.source()
    layer_name = layer.name() or "unnamed"
    authcfg = _extract_authcfg(source_uri)
    crs = extract_crs_authid(layer) or ""
    geom_type = extract_geometry_type(layer)
    now = datetime.now(timezone.utc).isoformat()

    source_clean = _strip_password_from_uri(source_uri)

    try:
        with conn:
            conn.execute(
                """INSERT INTO datasource_registry
                   (datasource_fingerprint, provider_type, source_uri,
                    layer_name, authcfg, crs_authid, geometry_type, last_seen_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(datasource_fingerprint) DO UPDATE SET
                     source_uri = excluded.source_uri,
                     layer_name = excluded.layer_name,
                     authcfg = excluded.authcfg,
                     crs_authid = excluded.crs_authid,
                     geometry_type = excluded.geometry_type,
                     last_seen_at = excluded.last_seen_at""",
                (fingerprint, provider_type, source_clean,
                 layer_name, authcfg, crs, geom_type, now),
            )
    except sqlite3.Error as e:
        flog(f"datasource_registry: register error: {e}", "WARNING")


def lookup_datasource(conn: sqlite3.Connection,
                      fingerprint: str) -> Optional[DatasourceInfo]:
    """Look up stored datasource info by fingerprint. Returns None if not found."""
    try:
        row = conn.execute(
            """SELECT datasource_fingerprint, provider_type, source_uri,
                      layer_name, authcfg, crs_authid, geometry_type
               FROM datasource_registry
               WHERE datasource_fingerprint = ?""",
            (fingerprint,),
        ).fetchone()
        if row is None:
            return None
        return DatasourceInfo(
            fingerprint=row[0],
            provider_type=row[1],
            source_uri=row[2],
            layer_name=row[3] or "",
            authcfg=row[4] or "",
            crs_authid=row[5] or "",
            geometry_type=row[6] or "NoGeometry",
        )
    except sqlite3.Error as e:
        flog(f"datasource_registry: lookup error: {e}", "WARNING")
        return None


def create_layer_from_registry(info: DatasourceInfo):
    """Create a temporary QgsVectorLayer from stored datasource info.

    Returns the layer (caller must check isValid()) or None on error.
    Uses the QGIS auth system for credentials (authcfg).
    """
    from qgis.core import QgsVectorLayer, QgsDataSourceUri

    uri = info.source_uri

    if info.provider_type == "postgres" and info.authcfg:
        if "authcfg=" not in uri:
            parsed = QgsDataSourceUri(uri)
            parsed.setAuthConfigId(info.authcfg)
            uri = parsed.uri()

    display_name = f"{info.layer_name} (restore)"
    try:
        layer = QgsVectorLayer(uri, display_name, info.provider_type)
        if not layer.isValid():
            flog(f"datasource_registry: layer invalid for {info.fingerprint}: "
                 f"provider={info.provider_type}", "WARNING")
            return None
        flog(f"datasource_registry: created temp layer for {info.fingerprint}")
        return layer
    except Exception as e:
        flog(f"datasource_registry: cannot create layer: {e}", "ERROR")
        return None


def _extract_authcfg(source_uri: str) -> str:
    """Extract authcfg ID from a QGIS source URI, or return empty string."""
    match = re.search(r"authcfg=([A-Za-z0-9]+)", source_uri)
    return match.group(1) if match else ""


def _strip_password_from_uri(uri: str) -> str:
    """Remove password= from a source URI before storing.

    Passwords must NEVER be persisted. QGIS resolves them via authcfg.
    Single alternation regex tries quoted variants first to avoid the
    unquoted pattern swallowing the quotes.
    """
    return re.sub(
        r"""\bpassword='[^']*'|\bpassword="[^"]*"|\bpassword=[^'"\s]+""",
        "password=***",
        uri,
    )
