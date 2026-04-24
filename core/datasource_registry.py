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

_DB_PROVIDERS = frozenset({"postgres", "mssql", "oracle"})

_SETTINGS_PREFIX = {
    "postgres": "PostgreSQL/connections",
    "mssql": "MSSQL/connections",
    "oracle": "Oracle/connections",
}


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

    source_clean = _strip_password_from_uri(source_uri, provider_type)

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


def purge_orphan_datasources(conn: sqlite3.Connection) -> int:
    """Remove datasource_registry rows with no referencing events.

    Keeps the registry small after retention purges drop the last event
    of a datasource. Safe: live datasources are re-registered on every
    commit.
    """
    try:
        with conn:
            cursor = conn.execute(
                "DELETE FROM datasource_registry WHERE datasource_fingerprint "
                "NOT IN (SELECT DISTINCT datasource_fingerprint FROM audit_event)"
            )
            deleted = cursor.rowcount or 0
        if deleted:
            flog(f"datasource_registry: gc removed {deleted} orphan(s)")
        return deleted
    except sqlite3.Error as e:
        flog(f"datasource_registry: gc error: {e}", "WARNING")
        return 0


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
    For DB providers: resolves credentials via authcfg or QGIS saved connections.
    """
    from qgis.core import QgsVectorLayer

    if info.provider_type in _DB_PROVIDERS:
        uri = _enrich_db_uri(info)
    else:
        uri = info.source_uri

    if uri is None:
        flog(f"datasource_registry: no credentials for {info.fingerprint} "
             f"provider={info.provider_type}; load the layer manually", "WARNING")
        return None

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


def _enrich_db_uri(info: DatasourceInfo) -> Optional[str]:
    """Build a connectable URI for a DB-backed layer.

    Priority: (1) authcfg from registry, (2) QGIS saved connections, (3) None.
    Credentials are used transiently; never persisted.
    """
    from qgis.core import QgsDataSourceUri

    parsed = QgsDataSourceUri(info.source_uri)

    if info.authcfg:
        parsed.setAuthConfigId(info.authcfg)
        return parsed.uri()

    saved = _find_matching_saved_connection(info)
    if saved is None:
        return None

    if saved.get("authcfg"):
        parsed.setAuthConfigId(saved["authcfg"])
    elif saved.get("username"):
        parsed.setUsername(saved["username"])
        parsed.setPassword(saved.get("password", ""))
    else:
        return None

    return parsed.uri()


def _find_matching_saved_connection(info: DatasourceInfo) -> Optional[dict]:
    """Match stored URI against QGIS saved connections by host/port/db.

    Returns dict with 'authcfg' or 'username'+'password', or None.
    """
    prefix = _SETTINGS_PREFIX.get(info.provider_type)
    if not prefix:
        return None

    try:
        from qgis.core import QgsSettings, QgsDataSourceUri
    except ImportError:
        return None

    parsed = QgsDataSourceUri(info.source_uri)
    target_host = parsed.host().lower()
    target_port = parsed.port() or _default_port(info.provider_type)
    target_db = parsed.database().lower()

    if not target_host or not target_db:
        return None

    settings = QgsSettings()
    settings.beginGroup(prefix)
    names = settings.childGroups()
    settings.endGroup()

    for name in names:
        p = f"{prefix}/{name}"
        host = (settings.value(f"{p}/host", "") or "").lower()
        port = str(settings.value(f"{p}/port", target_port))
        db = (settings.value(f"{p}/database", "") or "").lower()

        if host != target_host or port != str(target_port) or db != target_db:
            continue

        authcfg = settings.value(f"{p}/authcfg", "")
        if authcfg:
            flog(f"datasource_registry: matched saved connection '{name}' via authcfg")
            return {"authcfg": authcfg}

        username = settings.value(f"{p}/username", "")
        if username:
            flog(f"datasource_registry: matched saved connection '{name}' via user")
            return {
                "username": username,
                "password": settings.value(f"{p}/password", "") or "",
            }

    return None


def _default_port(provider_type: str) -> str:
    return {"postgres": "5432", "mssql": "1433", "oracle": "1521"}.get(
        provider_type, "5432"
    )


def _extract_authcfg(source_uri: str) -> str:
    """Extract authcfg ID from a QGIS source URI, or return empty string."""
    match = re.search(r"authcfg=([A-Za-z0-9]+)", source_uri)
    return match.group(1) if match else ""


_PASSWORD_RE = re.compile(
    r"""\b(?:ssl)?passw(?:ord|d)?='[^']*'"""
    r'|\b(?:ssl)?passw(?:ord|d)?="[^"]*"'
    r"""|\b(?:ssl)?passw(?:ord|d)?=[^'"\s]+""",
    re.IGNORECASE,
)


def _strip_password_from_uri(uri: str, provider_type: str = "") -> str:
    """Remove password= variants from a source URI before storing.

    Passwords must NEVER be persisted. QGIS resolves them via authcfg.
    For DB providers, use QgsDataSourceUri.setPassword('') which is the
    canonical QGIS-side parser and handles all quoting corner cases.
    Falls back to a regex for non-DB URIs (files, delimited text, etc.).
    """
    if provider_type in _DB_PROVIDERS:
        try:
            from qgis.core import QgsDataSourceUri
            parsed = QgsDataSourceUri(uri)
            parsed.setPassword("")
            return parsed.uri()
        except Exception as exc:
            flog(f"datasource_registry: QgsDataSourceUri strip failed ({exc}); "
                 f"falling back to regex", "WARNING")
    return _PASSWORD_RE.sub("password=***", uri)
