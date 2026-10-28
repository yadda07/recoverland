# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Datasource registry for RecoverLand.

Stores the raw QGIS source URI for each audited datasource fingerprint.
Used at restore time to reconnect to a datasource when the layer is not
currently loaded in the project.

Security: stores authcfg references (not passwords). Passwords are never
persisted. The QGIS auth system resolves credentials at connection time.
"""
import os
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
    """Look up stored datasource info by fingerprint. Returns None if not found.

    Alias-aware: a journal migrated to v6 can hold the registry row of a
    source under its OBSOLETE fingerprint only. Looking up the canonical
    form strictly would answer "unknown source" and a restore on a layer
    the user has not loaded would fail with nothing to reconnect to.
    The canonical form is tried first; the aliases are only consulted
    when it is absent.
    """
    from .datasource_alias import expand_fingerprints

    try:
        candidates = expand_fingerprints(conn, fingerprint) or [fingerprint]
    except Exception as e:  # noqa: BLE001 - a lookup never blocks a restore
        flog(f"datasource_registry: alias expansion failed: {e}", "WARNING")
        candidates = [fingerprint]
    for candidate in candidates:
        info = _lookup_exact(conn, candidate)
        if info is not None:
            if _service_not_proven(candidate, info, candidates):
                continue
            if candidate != fingerprint:
                flog(f"datasource_registry: {fingerprint} resolved through "
                     f"alias {candidate}")
            return info
    return None


def _service_not_proven(fingerprint: str, info: DatasourceInfo,
                        scope=()) -> bool:
    """True when the row's URI may name another database than the events.

    Before v7 a DB fingerprint did not record the connection service, so
    every ``service='...'`` URI of a table shared one fingerprint and this
    row (upserted on it) kept the URI of the LAST service seen. Events
    captured through `test` would then be restored into the `prod` URI
    the row holds. A fingerprint that carries its service token (v7) is
    unambiguous; a service-less one whose URI leaves the database to a
    service (the v7 fingerprint of that URI carries the token, see
    `identity._normalize_db_source`) is not, and is refused, UNLESS the
    journal itself attached the two: the v7 fingerprint of the URI is in
    *scope*, the alias closure of the fingerprint asked (an alias the
    reconciliation wrote because this service is the only one known, or
    one the user wrote with `datasource_alias.attach_service_history`).
    """
    if info.provider_type not in _DB_PROVIDERS:
        return False
    from .identity import (
        SERVICE_TOKEN, compute_fingerprint_for_source, split_fingerprint,
    )

    _provider, body = split_fingerprint(fingerprint)
    if body.startswith(SERVICE_TOKEN):
        return False
    try:
        uri_fp = compute_fingerprint_for_source(
            info.provider_type, info.source_uri or "")
        _provider, uri_body = split_fingerprint(uri_fp)
    except Exception as exc:  # noqa: BLE001 - unreadable URI: nothing proven
        flog(f"datasource_registry: registry row of {fingerprint} refused: "
             f"its URI cannot be parsed ({exc})", "WARNING")
        return True
    if not uri_body.startswith(SERVICE_TOKEN):
        return False
    service = uri_body[len(SERVICE_TOKEN):].split(" host=", 1)[0]
    if uri_fp in (scope or ()):
        flog(f"datasource_registry: registry row of {fingerprint} used: its "
             f"URI goes through service {service!r}, to which the journal "
             f"attached this history")
        return False
    flog(f"datasource_registry: registry row of {fingerprint} refused: its "
         f"URI goes through service {service!r}, which this fingerprint did "
         f"not record (written before v7); the events may come from another "
         f"database served under the same table name. Load the layer in the "
         f"project to restore them.", "WARNING")
    return True


# ---------------------------------------------------------------------------
# Connection services this workstation can resolve
# ---------------------------------------------------------------------------

_ODBC_DSN_KEYS = (
    ("HKEY_CURRENT_USER", r"Software\ODBC\ODBC.INI\ODBC Data Sources"),
    ("HKEY_LOCAL_MACHINE", r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"),
    ("HKEY_LOCAL_MACHINE",
     r"SOFTWARE\WOW6432Node\ODBC\ODBC.INI\ODBC Data Sources"),
)


def configured_connection_services(provider_type: str):
    """``(services, sources)`` this workstation can resolve for a provider.

    *services* is the set of connection service names the provider's
    client library would find here: the sections of the libpq service
    files for ``postgres`` (``PGSERVICEFILE``, else the user's
    ``.pg_service.conf``, plus ``PGSYSCONFDIR/pg_service.conf``), the ODBC
    data sources for ``mssql``. None when this provider has no list that
    can be read (``oracle``, anything else): the caller must then not
    claim to know every service. *sources* names what was read, for the
    message shown to the user.

    Only files and registry keys that exist are read; one that cannot be
    read is skipped and logged. A caller checks that the service it is
    about to rely on is in the set: when it is not, the list is
    incomplete (a system file in a compiled-in directory, another
    workstation) and proves nothing.
    """
    if provider_type == "postgres":
        return _pg_services()
    if provider_type == "mssql":
        return _odbc_data_sources()
    return None, ()


def _pg_service_files():
    files = []
    explicit = os.environ.get("PGSERVICEFILE")
    if explicit:
        files.append(explicit)
    elif os.name == "nt":
        appdata = os.environ.get("APPDATA")
        if appdata:
            files.append(os.path.join(appdata, "postgresql",
                                      ".pg_service.conf"))
    else:
        files.append(os.path.join(os.path.expanduser("~"),
                                  ".pg_service.conf"))
    sysconf = os.environ.get("PGSYSCONFDIR")
    if sysconf:
        files.append(os.path.join(sysconf, "pg_service.conf"))
    return files


def _pg_services():
    services = set()
    sources = []
    for path in _pg_service_files():
        if not os.path.isfile(path):
            continue
        names = _ini_sections(path)
        if names is None:
            continue
        sources.append(path)
        services.update(names)
    return services, tuple(sources)


def _ini_sections(path: str):
    """Section names of an INI-style file, None when it cannot be read."""
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()
    except OSError as exc:
        flog(f"datasource_registry: service file {path} unreadable ({exc})",
             "WARNING")
        return None
    names = set()
    for line in lines:
        text = line.strip()
        if text.startswith("[") and text.endswith("]") and len(text) > 2:
            names.add(text[1:-1].strip())
    return names


def _odbc_data_sources():
    services = set()
    sources = []
    if os.name == "nt":
        try:
            import winreg
        except ImportError:
            return None, ()
        for hive_name, key_path in _ODBC_DSN_KEYS:
            hive = getattr(winreg, hive_name)
            try:
                with winreg.OpenKey(hive, key_path) as key:
                    index = 0
                    while True:
                        try:
                            name, _value, _kind = winreg.EnumValue(key, index)
                        except OSError:
                            break
                        services.add(name)
                        index += 1
                sources.append(f"{hive_name}\\{key_path}")
            except OSError:
                continue
        return services, tuple(sources)
    candidates = [os.environ.get("ODBCINI") or
                  os.path.join(os.path.expanduser("~"), ".odbc.ini"),
                  os.path.join(os.environ.get("ODBCSYSINI") or "/etc",
                               "odbc.ini")]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        names = _ini_sections(path)
        if names is None:
            continue
        names.discard("ODBC Data Sources")
        names.discard("ODBC")
        sources.append(path)
        services.update(names)
    return services, tuple(sources)


def _lookup_exact(conn: sqlite3.Connection,
                  fingerprint: str) -> Optional[DatasourceInfo]:
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
