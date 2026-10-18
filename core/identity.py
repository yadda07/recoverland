"""Layer and entity identity for RecoverLand (RLU-011).

Computes stable fingerprints for datasources and features.
Datasource fingerprint = provider_type + normalized source URI.
Feature identity = best available primary key or FID.

Canonical form (schema v6)
--------------------------
`compute_fingerprint_for_source(provider, source_uri)` is THE single
entry point that turns a raw QGIS source into a fingerprint. Both the
live capture path (`compute_datasource_fingerprint`) and the v6
reconciliation (`core.datasource_alias`) go through it, so the value
stored in the journal and the value the migration computes can never
diverge.

Three defects of the historical normalisation are fixed here:

  1. DB providers: the table was extracted with ``table="([^"]*)"``,
     which captures the SCHEMA of ``table="public"."routes"``. Every
     table of a schema then shared one identity. The parsing now goes
     through `QgsDataSourceUri`, the official QGIS parser (host, port,
     database, schema, table), with a regex fallback for the rare
     environments where `qgis.core` is not importable.
  2. File providers: everything after the first ``|`` was glued back
     verbatim, so a display filter (``|subset=...``) or an opening
     option (``|geometrytype=...``) forked the history of one file.
     Only the IDENTIFYING keys (``layername``, ``layerid``) are kept,
     sorted. For the ordinary ``|layername=x`` case the output is
     byte-for-byte what it has always been, so healthy journals need
     no alias at all.
  3. `os.path.abspath` was applied to sources that are not filesystem
     paths (``file:///...?type=csv``, ``dbname='...' table="..."``),
     making the fingerprint depend on the process working directory.
     Those forms are now left alone.

Fingerprints already written stay valid: nothing is rewritten, the v6
migration records an ALIAS from the obsolete form to the canonical one
(see `core.datasource_alias`).

Canonical form (v7)
-------------------
Two more merges and one more split are closed:

  4. DB providers reached through a connection SERVICE (pg_service.conf,
     an ODBC DSN): the profile kept host/port/dbname only, which a
     service URI leaves empty, so every service collapsed onto
     ``host= port=5432 dbname=``. Two databases then shared one history,
     one registry row and one rewind scope. The service name now leads
     the profile when the URI leaves the host or the database to the
     service::

         postgres::service=prod host= port=5432 dbname= schema=public table=routes

     (`SERVICE_TOKEN`, `fingerprint_service`, `with_service`). A URI that
     names its host AND its database keeps its fingerprint byte for byte,
     service or not: explicit parameters override the service's (libpq,
     ODBC), so the database is designated without it.

     The service-less form written before v7 cannot say which service its
     events came from. Backward-compatible matching, decided once by the
     reconciliation (`core.datasource_alias`): when the journal and this
     workstation know ONE service only, the old history is attached to it
     and the attachment is reported as an assumption; when they know
     several, or cannot list them, it is refused by name (the services in
     cause are listed) and `datasource_alias.attach_service_history` lets
     the user attach it to the service he names. Until then the old
     events stay readable under their own fingerprint, outside the rewind
     scope of the service layer, and `service_match_unproven` tells every
     writer that the lenient `datasource_fingerprints_match` (kept so that
     a layer tracked under a 6.x selection is still captured) proves
     nothing about the database.
  5. spatialite: the source is a connection string, kept verbatim, so a
     display filter (``sql=``), a key hint (``key=``) or a re-serialised
     URI forked the history of one table. It is now rebuilt from its
     identifying parts only (database, table, geometry column), with the
     QGIS serialiser, so an unfiltered source keeps its v6 value.

Feature identity (v7)
---------------------
A composite primary key used to be journaled as its FIRST column only,
and every restore path trusted that as a unique key. A composite key is
now journaled whole (``pk_fields`` / ``pk_values``) and deliberately
WITHOUT the single ``pk_field`` / ``pk_value`` pair: the key is a
conjunction, and a consumer that reads one pair looks one column up.
Consumer contract (see `compute_feature_identity`, `composite_key`,
`composite_key_filter`, `effective_identity`):

  * locate the row by EVERY column of `composite_key` (an AND), accept
    exactly one holder, refuse several by name;
  * a restore of a DELETE looks the key up BEFORE any duplicate scan that
    leaves the key columns out of its comparison: on a table whose other
    columns are shared, that scan matches another row and answers
    "already exists" for a row that is not there;
  * events journaled before v7 carry the first column only: pass them
    through `effective_identity`, which withdraws that pair on a
    composite-key layer (``pk_unproven``) instead of letting it designate
    every row sharing the first column;
  * each key value is journal text (``2026-01-15`` for a date, the v7
    date-time text), never the repr of a Qt object; a single-column key
    keeps its historical form, where a date was such a repr, and
    `effective_identity` decodes it for the lookup. Build the filter
    with `composite_key_filter(pairs, layer.fields())`: a date-time column
    is then compared as an instant, which a plain text literal is not on
    a provider that compiles it into SQL.
"""
import json
import os
import re
from typing import Optional, Dict, List, Any, Tuple

from .support_policy import (
    IdentityStrength, get_provider_policy, refine_ogr_identity,
)

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

# Keys of the `|key=value` suffix that take part in the IDENTITY of the
# datasource. Everything else (subset, geometrytype, encoding, ...) is a
# presentation option: it changes what the user sees, never which rows
# the layer writes to.
_IDENTIFYING_SUFFIX_KEYS = ("layerid", "layername")

# A source that is a plain filesystem path may be absolutised; a URI or a
# `key=value` connection string may not (see defect 3 above).
_KEY_VALUE_HEAD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\s*=")


def _get_fingerprint_mode() -> str:
    raw = os.environ.get(_FINGERPRINT_MODE_ENV) or ""
    mode = raw.strip().lower()
    if mode in ("absolute", "relative"):
        return mode
    return "absolute"


def compute_fingerprint_for_source(provider_name: str, source_uri: str) -> str:
    """Canonical fingerprint for a raw (provider, source) pair.

    Single source of truth: the capture path and the v6 migration both
    call this, so a fingerprint computed today and a fingerprint
    recomputed by the migration from `datasource_registry.source_uri`
    can never disagree.

    Format: 'provider::normalized_source'.
    """
    provider = (provider_name or "").strip()
    normalized = _normalize_source_uri(provider, source_uri or "")
    return f"{provider}::{normalized}"


def compute_datasource_fingerprint(layer) -> str:
    """Compute a deterministic fingerprint for a layer's data source.

    Format: 'provider::normalized_source'
    """
    provider = layer.dataProvider()
    provider_name = provider.name()
    raw_source = layer.source()
    return compute_fingerprint_for_source(provider_name, raw_source)


# The DB profiles EXACTLY as they were before the v6 canonicalisation.
# Frozen on purpose: legacy_fingerprint_for_source must keep reproducing what
# the plugin used to write even as _DB_NORMALIZATION_PROFILES evolves (v6 gave
# oracle a schema token, which the historical form never had).
_LEGACY_DB_PROFILES = {
    "postgres": (("host", ""), ("port", "5432"), ("dbname", ""),
                 ("schema", "public"), ("table", "")),
    "mssql": (("host", ""), ("port", "1433"), ("dbname", ""),
              ("schema", "dbo"), ("table", "")),
    "oracle": (("host", ""), ("port", "1521"), ("dbname", ""), ("table", "")),
}


def legacy_fingerprint_for_source(provider_name: str, source_uri: str) -> str:
    """The fingerprint this plugin produced BEFORE the v6 canonicalisation.

    Exists so that CAPTURE never depends on a row written in the journal.

    The tracked-layer selection lives in QgsSettings under whatever form was
    current when the user ticked the box. After v6 the canonical form differs,
    and a strict membership test stopped capturing on every DB and CSV layer --
    silently, which is the worst failure a recovery tool can have. Resolving
    that through the alias table would have made capture depend on a row that
    may not exist: no registry entry, an ambiguous DB source we refuse to
    guess, a read-only journal, or simply a migration that has not run yet.
    Matching on BOTH forms needs no journal at all.
    """
    profile = _LEGACY_DB_PROFILES.get(provider_name)
    if profile is not None:
        parts = {}
        for key, _default in profile:
            match = (re.search(rf"{key}='([^']*)'", source_uri)
                     or re.search(rf'{key}="([^"]*)"', source_uri)
                     or re.search(rf"{key}=(\S+)", source_uri))
            if match:
                parts[key] = match.group(1)
        normalized = " ".join(
            f"{key}={parts.get(key, default)}" for key, default in profile)
    elif provider_name in _FILE_PROVIDERS:
        path = source_uri.split("|")[0].strip().replace("\\", "/")
        try:
            path = os.path.normcase(os.path.abspath(path))
        except (OSError, ValueError):
            pass
        path = path.replace("\\", "/")
        tail = "|" + source_uri.split("|", 1)[1] if "|" in source_uri else ""
        normalized = path + tail
    else:
        normalized = source_uri.strip()
    return f"{provider_name}::{normalized}"


def v6_fingerprint_for_source(provider_name: str, source_uri: str) -> str:
    """The fingerprint the v6 canonical form produced (before v7).

    Same role as `legacy_fingerprint_for_source`, one generation later: a
    tracking selection saved by 6.x holds this form, and v7 changed it for
    two families (service-based DB sources gained a ``service=`` token,
    spatialite sources lost their filter tokens). Without it a tracked
    layer would stop matching its own selection and capture would go
    silent.
    """
    provider = (provider_name or "").strip()
    raw = source_uri or ""
    profile = _DB_NORMALIZATION_PROFILES.get(provider)
    if profile is not None:
        normalized = _normalize_db_source(raw, profile, with_service=False)
    elif provider in _FILE_PROVIDERS:
        normalized = _normalize_v6_file_source(raw)
    else:
        normalized = raw.strip()
    return f"{provider}::{normalized}"


def layer_fingerprint_forms(layer) -> Tuple[str, ...]:
    """Every fingerprint form under which this layer's history may be filed."""
    try:
        provider_name = layer.dataProvider().name()
        source = layer.source()
    except Exception:  # noqa: BLE001 - a broken layer is simply unmatchable
        return ()
    forms = [compute_fingerprint_for_source(provider_name, source)]
    for older in (v6_fingerprint_for_source(provider_name, source),
                  legacy_fingerprint_for_source(provider_name, source)):
        if older not in forms:
            forms.append(older)
    for bare in _single_layer_bare_forms(provider_name, forms[0]):
        if bare not in forms:
            forms.append(bare)
    return tuple(forms)


def layer_is_selected(layer, allowed) -> bool:
    """True when *layer* belongs to a persisted fingerprint selection.

    An empty selection means "every layer", which is the tracker's own
    convention. Membership is tested on both the canonical and the historical
    form, then through the path-mode equivalence, so a selection saved by any
    version of the plugin keeps designating the same layers.
    """
    if not allowed:
        return True
    for form in layer_fingerprint_forms(layer):
        if form in allowed:
            return True
        for stored in allowed:
            if datasource_fingerprints_match(stored, form):
                return True
    return False


def split_fingerprint(fingerprint: str) -> Tuple[str, str]:
    """Split 'provider::normalized' into ``(provider, normalized)``.

    Returns ``("", fingerprint)`` when the value carries no provider
    prefix, so callers can degrade instead of crashing on a hand-written
    or truncated fingerprint.
    """
    if not fingerprint:
        return "", ""
    provider, sep, normalized = fingerprint.partition("::")
    if not sep:
        return "", fingerprint
    return provider, normalized


def canonicalize_fingerprint(fingerprint: str) -> str:
    """Best-effort canonical form of an already-stored fingerprint.

    Drops the non-identifying `|key=value` tokens (``subset``,
    ``geometrytype``, ...) of a FILE fingerprint, which is exactly what
    the v6 canonical form removed. The PATH is left untouched on
    purpose: re-running the full normaliser would absolutise a
    fingerprint stored in relative mode (BL-RW-P2-13) against the
    process working directory instead of the project home.

    A DB fingerprint carries a lossy profile (the table token of a
    merged fingerprint holds the schema, not the table), so re-deriving
    it would be guessing: it is returned unchanged and only the
    registry-driven reconciliation may alias it.

    A spatialite fingerprint IS its connection string, so it is re-parsed
    and rebuilt from its identifying parts, exactly like the capture
    path does since v7: a v6 fingerprint written while a filter was set
    then names the same table as the unfiltered one. Pure parsing, no
    path resolution: nothing here depends on the working directory.

    Idempotent: canonicalize(canonicalize(x)) == canonicalize(x).
    """
    provider, normalized = split_fingerprint(fingerprint)
    if provider not in _FILE_PROVIDERS or not normalized:
        return fingerprint
    if provider == "spatialite" and not _is_filesystem_path(normalized):
        return provider + "::" + _normalize_spatialite_source(normalized)
    head, _sep, tail = normalized.partition("|")
    clean_head = head.strip()
    suffix = _identifying_suffix(tail)
    if provider == "ogr" and not suffix:
        suffix = _single_layer_suffix(clean_head)
    return provider + "::" + clean_head + suffix


def datasource_fingerprints_match(stored: str, current: str) -> bool:
    """Return True when `stored` and `current` denote the same datasource.

    Strict equality first; then the identity form (a pre-v6 fingerprint
    carrying a display filter denotes the same file as the canonical
    one); then cross-mode resolution (absolute vs relative against the
    current QGIS project home) so that audit records produced before
    BL-RW-P2-13 keep matching layers loaded in the new relative mode.
    Cross-mode only applies to file-based providers (`ogr` /
    `spatialite` / `delimitedtext`); DB fingerprints are compared
    strictly (their alias is resolved through the journal instead), with
    one exception: a v6 service-less fingerprint against the same table
    reached through a service (see `service_match_unproven`).
    """
    if not stored or not current:
        return stored == current
    if stored == current:
        return True
    if canonicalize_fingerprint(stored) == canonicalize_fingerprint(current):
        return True
    if service_match_unproven(stored, current):
        # A fingerprint written before v7 did not record the service: it
        # MAY name this table. Answering no would silently stop capturing
        # a tracked layer whose selection was saved by 6.x. A caller about
        # to WRITE on the strength of this match must ask
        # `service_match_unproven` and refuse.
        return True
    return _cross_mode_match(stored, current)


def service_match_unproven(stored: str, current: str) -> bool:
    """True when *stored* matches *current* only because it lacks a service.

    *stored* is a DB fingerprint written before v7 (no ``service=`` token)
    and *current* the same table reached through a connection service.
    The two MAY denote the same table, or the same table name in another
    database: the service the events came from was never recorded. Good
    enough to keep capturing a tracked layer, never enough to write the
    old events into this one.
    """
    stored_provider, stored_body = split_fingerprint(stored or "")
    current_provider, current_body = split_fingerprint(current or "")
    if (stored_provider != current_provider
            or stored_provider not in _DB_NORMALIZATION_PROFILES):
        return False
    if stored_body.startswith(SERVICE_TOKEN):
        return False
    if not current_body.startswith(SERVICE_TOKEN):
        return False
    # Every DB profile starts with `host=`: what follows the service token
    # is the v6 body, whatever characters the service name holds.
    index = current_body.find(" host=")
    return index > 0 and current_body[index + 1:] == stored_body


def fingerprint_service(fingerprint: str) -> str:
    """Connection service named by a v7 DB fingerprint, '' when none."""
    provider, body = split_fingerprint(fingerprint or "")
    if provider not in _DB_NORMALIZATION_PROFILES:
        return ""
    if not body.startswith(SERVICE_TOKEN):
        return ""
    index = body.find(" host=")
    if index <= len(SERVICE_TOKEN):
        return ""
    return body[len(SERVICE_TOKEN):index]


def with_service(fingerprint: str, service: str) -> str:
    """The v7 form of a service-less DB fingerprint reached through *service*.

    '' when *fingerprint* is not a service-less DB fingerprint (another
    provider, a service already recorded, a body that is not a profile) or
    when *service* is empty or holds whitespace: a fingerprint is only
    ever rebuilt with the exact layout `_normalize_db_source` writes.
    """
    provider, body = split_fingerprint(fingerprint or "")
    name = (service or "").strip()
    if (provider not in _DB_NORMALIZATION_PROFILES
            or not body.startswith("host=")
            or not name or any(ch.isspace() for ch in name)):
        return ""
    return f"{provider}::{SERVICE_TOKEN}{name} {body}"


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
    abs_home = ""
    if home:
        try:
            abs_home = os.path.abspath(home)
        except (OSError, ValueError):
            abs_home = ""
    if not abs_home and stored_provider != "spatialite":
        if not (stored_provider == "ogr"
                and _ogr_single_layer_bare_match(stored_src, current_src)):
            return False
    if (stored_provider == "ogr"
            and _ogr_single_layer_bare_match(stored_src, current_src, abs_home)):
        return True
    if not abs_home and stored_provider != "spatialite":
        return False
    return _absolutize(stored_src, abs_home) == _absolutize(current_src, abs_home)


def _absolutize(source: str, abs_home: str) -> str:
    """Best-effort canonical absolute representation of a file source."""
    head, _sep, tail = source.partition("|")
    path = head.strip().replace("\\", "/")
    if not _is_filesystem_path(head):
        if _KEY_VALUE_HEAD_RE.match(head.strip()):
            return _spatialite_comparison_key(source, abs_home)
        return source.strip()
    if not os.path.isabs(path):
        if not abs_home:
            return path + _identifying_suffix(tail)
        path = os.path.join(abs_home, path)
    try:
        path = os.path.abspath(path)
        path = os.path.normcase(path)
    except (OSError, ValueError):
        pass
    path = path.replace("\\", "/")
    suffix = _identifying_suffix(tail)
    if not suffix:
        suffix = _single_layer_suffix(path)
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
#
# `schema` is part of every profile, oracle included: an oracle table is
# named OWNER.TABLE exactly like a PostGIS one is named schema.table, and
# leaving the owner out merged the tables of two owners onto one identity.
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
        ("schema", ""),
        ("table", ""),
    ),
}

_DB_PROVIDERS = tuple(_DB_NORMALIZATION_PROFILES)


def _normalize_source_uri(provider_name: str, raw_source: str) -> str:
    """Normalize a source URI for deterministic fingerprinting."""
    profile = _DB_NORMALIZATION_PROFILES.get(provider_name)
    if profile is not None:
        return _normalize_db_source(raw_source, profile)
    if provider_name == "spatialite" and not _is_filesystem_path(
            raw_source.partition("|")[0]):
        return _normalize_spatialite_source(raw_source)
    if provider_name in _FILE_PROVIDERS:
        return _normalize_file_source(raw_source)
    return raw_source.strip()


# Token that names the connection SERVICE of a DB fingerprint. It LEADS the
# profile so the `table=` token stays the tail (datasource_alias reads it
# with rfind).
SERVICE_TOKEN = "service="


def _normalize_db_source(raw: str, profile, with_service: bool = True) -> str:
    """Extract stable parts from a DB URI according to a profile.

    The parts come from `QgsDataSourceUri`, the parser QGIS itself uses
    to build the URI. The historical regex pipeline read the table with
    ``table="([^"]*)"``, which on the form QGIS actually writes --
    ``table="public"."routes" (geom)`` -- captured ``public``, i.e. the
    SCHEMA: every table of a schema collapsed onto a single identity and
    their histories merged.

    Output keeps the historical ``key=value`` space-separated layout, so
    a fingerprint whose parts were already correct stays valid.

    A connection SERVICE (``service='prod'``) is where host and database
    live when the URI does not carry them: without it, two services
    reaching two databases produced the same ``host= port=5432 dbname=``
    fingerprint. It is prepended only when the URI leaves the host or the
    database to the service: explicit connection parameters override the
    service's (libpq, ODBC), so a URI that names both already designates
    its database and keeps its fingerprint byte for byte, like every
    host/dbname URI. ``with_service=False`` rebuilds the v6 form (see
    `v6_fingerprint_for_source`).
    """
    parts = _db_parts_from_qgis(raw)
    if parts is None:
        parts = _db_parts_from_regex(raw, profile)
    body = " ".join(
        f"{key}={parts.get(key) or default}"
        for key, default in profile
    )
    service = (parts.get("service") or "").strip()
    if (with_service and service
            and not (parts.get("host") and parts.get("dbname"))):
        return f"{SERVICE_TOKEN}{service} {body}"
    return body


def _db_parts_from_qgis(raw: str) -> Optional[Dict[str, str]]:
    """Parse a DB URI with QgsDataSourceUri. None when unavailable."""
    try:
        from qgis.core import QgsDataSourceUri
    except Exception:  # noqa: BLE001 - no QGIS runtime: caller falls back
        return None
    try:
        uri = QgsDataSourceUri(raw)
        parts = {
            "service": uri.service() or "",
            "host": uri.host() or "",
            "port": uri.port() or "",
            "dbname": uri.database() or "",
            "schema": uri.schema() or "",
            "table": uri.table() or "",
        }
    except Exception as exc:  # noqa: BLE001 - malformed URI: fall back
        from .logger import flog
        flog(f"identity: QgsDataSourceUri failed on a DB source ({exc}); "
             f"falling back to regex parsing", "WARNING")
        return None
    if not any(parts.values()):
        return None
    return parts


# `table=` in the three forms QGIS writes it:
#   table="schema"."name" / table='schema'.'name' / table=schema.name
_TABLE_RE = re.compile(
    r"""table=\s*(?:"(?P<sq>[^"]*)"|'(?P<ss>[^']*)'|(?P<bs>[^\s".'()]+))"""
    r"""(?:\s*\.\s*(?:"(?P<tq>[^"]*)"|'(?P<ts>[^']*)'|(?P<tb>[^\s".'()]+)))?"""
)


def _db_parts_from_regex(raw: str, profile) -> Dict[str, str]:
    """Fallback parser used when `qgis.core` cannot be imported."""
    parts: Dict[str, str] = {}
    keys = [key for key, _default in profile] + ["service"]
    for key in keys:
        if key in ("schema", "table"):
            continue
        match = re.search(rf"{key}='([^']*)'", raw)
        if not match:
            match = re.search(rf'{key}="([^"]*)"', raw)
        if not match:
            match = re.search(rf"{key}=(\S+)", raw)
        if match:
            parts[key] = match.group(1)
    match = _TABLE_RE.search(raw)
    if match:
        first = match.group("sq") or match.group("ss") or match.group("bs") or ""
        second = match.group("tq") or match.group("ts") or match.group("tb") or ""
        if second:
            parts["schema"] = first
            parts["table"] = second
        else:
            parts["table"] = first
    return parts


def find_fingerprint_collisions(layers) -> Dict[str, List[str]]:
    """``{fingerprint: [layer names]}`` for fingerprints shared by 2+ layers.

    A datasource fingerprint names one table. Since the v6 canonical
    form, two tables of one schema no longer collapse onto one value, so
    what this detector still catches is the real thing: two layers of the
    project pointing at the SAME table (the same file and layername, the
    same schema.table). Those two layers share one history by
    construction, and a rewind scoped on one of them would replay events
    captured through the other.

    The caller refuses the rewind instead of quietly operating on a
    merged identity. It costs one fingerprint computation per layer.
    """
    from .logger import flog

    by_fp: Dict[str, List[str]] = {}
    canonical_for_key: Dict[str, str] = {}
    for layer in layers or []:
        try:
            fp = compute_datasource_fingerprint(layer)
        except Exception as exc:  # noqa: BLE001 - never block on a bad layer
            flog(f"identity: fingerprint failed for a layer: {exc}", "WARNING")
            continue
        if not fp:
            continue
        key = _collision_group_key(fp)
        canonical_for_key.setdefault(key, fp)
        by_fp.setdefault(canonical_for_key[key], []).append(
            extract_layer_name(layer))
    return {fp: names for fp, names in by_fp.items() if len(names) > 1}


def _collision_group_key(fp: str) -> str:
    """Case-consistent key for collision detection across providers."""
    provider, body = split_fingerprint(fp)
    if provider == "spatialite" and not _is_filesystem_path(body):
        return f"spatialite::{_spatialite_comparison_key(body, '')}"
    return canonicalize_fingerprint(fp)


def _is_filesystem_path(raw: str) -> bool:
    """True when `raw` is a plain path abspath() may legitimately touch.

    False for URI forms (``file:///...``) and for connection strings
    (``dbname='...' table="..."``). Running `os.path.abspath` on those
    silently glued the process working directory in front of them, so the
    same file opened from two different shells produced two fingerprints
    and split the history of a fully supported provider (spatialite).
    """
    text = (raw or "").strip()
    if not text:
        return False
    if "://" in text:
        return False
    if _KEY_VALUE_HEAD_RE.match(text):
        return False
    return True


def _identifying_suffix(tail: str) -> str:
    """Keep only the identifying `|key=value` tokens, sorted.

    QGIS packs two very different things after the first ``|``: what the
    layer IS (``layername``, ``layerid``) and how it is currently shown
    (``subset``, ``geometrytype``, ``encoding``...). Keeping the second
    kind gave one file as many identities as the user had ways of
    opening it: posing a filter through `Couche > Filtrer` started a
    parallel history, and a rewind run filter-off never saw it.

    For the ordinary ``|layername=x`` source the returned value is
    ``|layername=x``, byte for byte what has always been stored.
    """
    if not tail:
        return ""
    kept = []
    for token in tail.split("|"):
        key, sep, value = token.partition("=")
        if not sep:
            continue
        name = key.strip().lower()
        if name in _IDENTIFYING_SUFFIX_KEYS:
            kept.append(f"{name}={value}")
    kept.sort()
    return "".join("|" + token for token in kept)


_SINGLE_LAYER_EXTS = (".gpkg", ".fgb")
_SINGLE_LAYER_CACHE: Dict[Tuple[str, int, int], str] = {}
_SINGLE_LAYER_CACHE_MAX = 512


def _resolve_candidate_path(path: str, abs_home: str = "") -> str:
    """Resolve *path* against *abs_home* (or project home) when relative."""
    cand = (path or "").strip().replace("\\", "/")
    if not cand:
        return ""
    if not os.path.isabs(cand):
        base = abs_home or _qgs_project_home_path()
        if base:
            cand = os.path.join(base, cand)
    try:
        return os.path.abspath(cand)
    except (OSError, ValueError):
        return cand


def _gpkg_single_table(cand_fs: str) -> str:
    """Single vector/attribute table name of a GeoPackage, or ''."""
    import sqlite3
    conn = None
    try:
        conn = sqlite3.connect(cand_fs, timeout=0.5)
        rows = conn.execute(
            "SELECT table_name FROM gpkg_contents "
            "WHERE data_type IN ('features', 'attributes') LIMIT 2"
        ).fetchall()
        if not rows:
            rows = conn.execute(
                "SELECT table_name FROM gpkg_geometry_columns LIMIT 2"
            ).fetchall()
        if len(rows) == 1 and rows[0][0]:
            return str(rows[0][0])
    except Exception:  # noqa: BLE001
        return ""
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 # nosec B110 - best-effort close on read-only probe
                pass
    return ""


def _fgb_single_layer(cand_fs: str, raw_path: str) -> str:
    """Layer name of a FlatGeobuf file."""
    try:
        from qgis.core import QgsProviderRegistry
        subs = QgsProviderRegistry.instance().querySublayers(cand_fs)
        if len(subs) == 1 and subs[0].name():
            return str(subs[0].name())
    except Exception:  # noqa: BLE001 # nosec B110 - fallback to basename when registry is unavailable
        pass
    return os.path.splitext(os.path.basename(raw_path))[0]


def _single_layer_suffix(path: str, abs_home: str = "") -> str:
    """``|layername=<t>`` when *path* is a single-layer .gpkg or .fgb, else ''."""
    clean = (path or "").strip()
    lower = clean.lower()
    if not lower.endswith(_SINGLE_LAYER_EXTS):
        return ""
    cand = _resolve_candidate_path(clean, abs_home)
    try:
        from .long_paths import when_needed
        cand_fs = when_needed(cand)
    except Exception:  # noqa: BLE001
        cand_fs = cand
    try:
        st = os.stat(cand_fs)
    except OSError:
        if lower.endswith(".fgb"):
            stem = os.path.splitext(os.path.basename(clean))[0]
            return f"|layername={stem}" if stem else ""
        return ""
    cache_key = (os.path.normcase(cand), int(st.st_mtime_ns), int(st.st_size))
    cached = _SINGLE_LAYER_CACHE.get(cache_key)
    if cached is not None:
        return cached
    if lower.endswith(".gpkg"):
        table = _gpkg_single_table(cand_fs)
    else:
        table = _fgb_single_layer(cand_fs, clean)
    suffix = f"|layername={table}" if table else ""
    if len(_SINGLE_LAYER_CACHE) >= _SINGLE_LAYER_CACHE_MAX:
        _SINGLE_LAYER_CACHE.clear()
    _SINGLE_LAYER_CACHE[cache_key] = suffix
    return suffix


def _single_layer_bare_forms(provider_name: str, canonical_fp: str) -> Tuple[str, ...]:
    """Bare ``ogr::<path>`` form when *canonical_fp* names a single-layer file."""
    if provider_name != "ogr" or "|layername=" not in canonical_fp:
        return ()
    _prov, norm = split_fingerprint(canonical_fp)
    head, _sep, tail = norm.partition("|")
    suffix = "|" + tail if tail else ""
    if suffix and _single_layer_suffix(head) == suffix:
        return (f"ogr::{head}",)
    return ()


def _ogr_single_layer_bare_match(stored_src: str, current_src: str,
                                 abs_home: str = "") -> bool:
    """True when one OGR source is bare and the other carries its single layername."""
    s_head, _, s_tail = stored_src.partition("|")
    c_head, _, c_tail = current_src.partition("|")
    s_suf = _identifying_suffix(s_tail)
    c_suf = _identifying_suffix(c_tail)
    if bool(s_suf) == bool(c_suf):
        return False
    s_path = _resolve_candidate_path(s_head, abs_home) if abs_home else s_head.strip()
    c_path = _resolve_candidate_path(c_head, abs_home) if abs_home else c_head.strip()
    try:
        s_norm = os.path.normcase(s_path).replace("\\", "/")
        c_norm = os.path.normcase(c_path).replace("\\", "/")
    except (OSError, ValueError):
        return False
    if s_norm != c_norm:
        return False
    named_suf = s_suf or c_suf
    resolved = _single_layer_suffix(s_path, abs_home)
    if resolved:
        return resolved == named_suf
    return s_norm.endswith(".fgb") and named_suf.startswith("|layername=")


def _normalize_v6_file_source(raw: str) -> str:
    """File source normalization as v6 produced it (without single-layer suffix)."""
    head, _sep, tail = raw.partition("|")
    suffix = _identifying_suffix(tail)
    if not _is_filesystem_path(head):
        return head.strip() + suffix
    path = head.strip().replace("\\", "/")
    if _get_fingerprint_mode() == "relative":
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
    return path.replace("\\", "/") + suffix


def _normalize_file_source(raw: str) -> str:
    """Normalize a file-based source URI."""
    base = _normalize_v6_file_source(raw)
    head, _sep, tail = base.partition("|")
    if not tail and _is_filesystem_path(head):
        return head + _single_layer_suffix(head)
    return base


# Fallback parser for a spatialite connection string (no QGIS runtime).
_SPATIALITE_DB_RE = re.compile(
    r"""dbname=\s*(?:'(?P<sq>(?:\\.|[^'\\])*)'|"(?P<dq>(?:\\.|[^"\\])*)"|(?P<bare>\S+))""")
_SPATIALITE_TABLE_RE = re.compile(
    r"""table=\s*(?:"(?P<dq>[^"]*)"|'(?P<sq>[^']*)'|(?P<bare>[^\s"'()]+))"""
    r"""(?:\s*\((?P<geom>[^)]*)\))?""")


def _normalize_spatialite_source(raw: str) -> str:
    """Identity of a spatialite connection string: database, table, geometry.

    A spatialite source is ``dbname='...' table="zones" (geom)`` plus
    whatever the layer accumulated: ``sql=<filter>`` when the user sets a
    filter, ``key=`` on a view, ``srid=`` / ``type=``, in the order
    `QgsDataSourceUri` happens to re-serialise them. Keeping the string
    verbatim gave one table one history per filter; with a tracking
    selection, the filtered layer matched nothing and was not captured
    at all.

    The canonical text is what the QGIS serialiser writes for the three
    identifying parts alone, so an unfiltered layer keeps the value v6
    stored byte for byte and needs no alias. The database path is kept
    as QGIS wrote it: resolving it would bring back the working-directory
    dependence that `_is_filesystem_path` exists to avoid.
    """
    text = (raw or "").strip()
    parts = _spatialite_parts_from_qgis(text)
    if parts is None:
        parts = _spatialite_parts_from_regex(text)
    if parts is None:
        # Nothing identifying could be read: keep the historical verbatim
        # form rather than inventing one.
        return text
    database, schema, table, geometry = parts
    rebuilt = _spatialite_uri_from_qgis(database, schema, table, geometry)
    if rebuilt is not None:
        return rebuilt
    body = f"dbname='{database}' table=\"{table}\""
    return f"{body} ({geometry})" if geometry else body


def _spatialite_comparison_key(raw: str, abs_home: str = "") -> str:
    """Case-folded spatialite key for equivalence and collision checks."""
    text = (raw or "").strip()
    parts = _spatialite_parts_from_qgis(text)
    if parts is None:
        parts = _spatialite_parts_from_regex(text)
    if parts is None:
        return text.lower()
    database, schema, table, geometry = parts
    db_path = database.strip().replace("\\", "/")
    if _is_filesystem_path(db_path):
        if not os.path.isabs(db_path) and abs_home:
            db_path = os.path.join(abs_home, db_path)
            try:
                db_path = os.path.abspath(db_path)
            except (OSError, ValueError):
                pass
        try:
            db_path = os.path.normcase(db_path).replace("\\", "/")
        except (OSError, ValueError):
            pass
    rebuilt = _spatialite_uri_from_qgis(db_path, schema, table, geometry)
    if rebuilt is not None:
        return rebuilt
    body = f"dbname='{db_path}' table=\"{table}\""
    return f"{body} ({geometry})" if geometry else body


def _spatialite_parts_from_qgis(text: str):
    try:
        from qgis.core import QgsDataSourceUri
    except Exception:  # noqa: BLE001 - no QGIS runtime: caller falls back
        return None
    try:
        uri = QgsDataSourceUri(text)
        database = uri.database() or ""
        table = uri.table() or ""
        if not database or not table:
            return None
        return database, uri.schema() or "", table, uri.geometryColumn() or ""
    except Exception as exc:  # noqa: BLE001 - malformed URI: fall back
        from .logger import flog
        flog(f"identity: QgsDataSourceUri failed on a spatialite source "
             f"({exc}); falling back to regex parsing", "WARNING")
        return None


def _spatialite_parts_from_regex(text: str):
    db_match = _SPATIALITE_DB_RE.search(text)
    table_match = _SPATIALITE_TABLE_RE.search(text)
    if db_match is None or table_match is None:
        return None
    database = (db_match.group("sq") or db_match.group("dq")
                or db_match.group("bare") or "")
    database = database.replace("\\'", "'").replace('\\"', '"')
    table = (table_match.group("dq") or table_match.group("sq")
             or table_match.group("bare") or "")
    if not database or not table:
        return None
    return database, "", table, (table_match.group("geom") or "").strip()


def _spatialite_uri_from_qgis(database, schema, table, geometry):
    try:
        from qgis.core import QgsDataSourceUri
    except Exception:  # noqa: BLE001 - no QGIS runtime: caller rebuilds
        return None
    try:
        uri = QgsDataSourceUri()
        uri.setDatabase(database)
        uri.setDataSource(schema, table, geometry)
        return uri.uri(False)
    except Exception:  # noqa: BLE001 - caller rebuilds by hand
        return None


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

    A composite primary key (``PRIMARY KEY (zone_id, parcel_no)``, which a
    partitioned PostGIS table requires, or a multi-column key chosen for
    a view) used to be journaled as its FIRST non-null column only:
    ``{"pk_field": "zone_id", "pk_value": 12}`` then named every row of
    zone 12, and every restore path trusted it as unique. Restoring an
    UPDATE wrote on whichever row of zone 12 came first, undoing an
    INSERT deleted an unrelated row, restoring a DELETE answered "already
    exists" because another row of zone 12 was there.

    A composite key is now journaled WHOLE, in the provider's column
    order, as ``{"fid": .., "pk_fields": [..], "pk_values": [..]}``, and
    on purpose without ``pk_field`` / ``pk_value``: a consumer that reads
    that pair looks ONE column up, which is exactly the defect. Locating
    the row is the consumer's job, through `composite_key` (every column,
    one holder, see its docstring for the DELETE restore): an identity
    read as FID-only by a consumer that ignores ``pk_fields`` falls back
    to its FID and content proofs, which on a table whose non-key columns
    are shared cannot tell two rows apart. When one column of the key is
    NULL or unreadable, the key is marked ``pk_incomplete`` and only the
    FID remains: an incomplete key designates nothing.

    Each value of a composite key is its JOURNAL TEXT (`_key_value_text`):
    ``2026-01-15`` for a date, the v7 date-time text, and not ``str()`` of
    the Qt object. ``str(QDate)`` is a Python repr
    (``PyQt6.QtCore.QDate(2026, 1, 15)``) that designates no row: the
    date-partitioned table this form exists for (``PRIMARY KEY (id,
    obs_date)``) had every restore refused as "not found" while the row
    was there, and the entity fingerprint changed with the PyQt binding
    (QGIS 3 against QGIS 4).

    A single-column key keeps the historical form byte for byte: the
    entity fingerprint of every event already journaled is built from it,
    and a new text would split the history of each such entity at the
    upgrade (rewind chains, entity streams). A legacy repr value is
    decoded for the lookups by `effective_identity` instead.
    """
    identity: Dict[str, Any] = {"fid": feature.id()}

    pk_indices = list(layer.dataProvider().pkAttributeIndexes() or [])
    if len(pk_indices) == 1:
        fields = layer.fields()
        idx = pk_indices[0]
        if 0 <= idx < fields.count():
            name, val = _read_pk_column(feature, fields.at(idx).name())
            if val is not None:
                identity["pk_field"] = name
                identity["pk_value"] = _safe_pk_value(val)
    elif len(pk_indices) > 1:
        _add_composite_key(identity, layer, feature, pk_indices)

    return json.dumps(identity, ensure_ascii=False)


def _read_pk_column(feature, name: str) -> Tuple[str, Any]:
    from .serialization import read_attribute
    try:
        return name, read_attribute(feature, name)
    except (KeyError, IndexError) as exc:
        # PK field declared by provider but not on this feature.
        from .logger import flog
        flog(f"identity: PK field {name!r} not available on feature: {exc}",
             "DEBUG")
        return name, None


def _add_composite_key(identity: Dict[str, Any], layer, feature,
                       pk_indices) -> None:
    fields = layer.fields()
    names: List[str] = []
    values: List[Any] = []
    complete = True
    for idx in pk_indices:
        if not 0 <= idx < fields.count():
            complete = False
            continue
        name, val = _read_pk_column(feature, fields.at(idx).name())
        names.append(name)
        text = (None if val is None or _is_null_variant(val)
                else _key_value_text(val))
        if text is None:
            complete = False
            continue
        values.append(text)
    identity["pk_fields"] = names
    if complete:
        identity["pk_values"] = values
    else:
        identity["pk_incomplete"] = True
        from .logger import flog
        flog(f"identity: composite key {names} incomplete on fid="
             f"{identity.get('fid')}; only the FID identifies this feature "
             f"(restores will require the post-state proof)", "WARNING")


def _is_null_variant(value: Any) -> bool:
    try:
        return bool(hasattr(value, "isNull") and value.isNull())
    except (TypeError, RuntimeError):
        return False


def _safe_pk_value(value: Any) -> Any:
    if isinstance(value, str):
        # A key read from the provider can hold a lone UTF-16 surrogate
        # (see serialization.scrub_surrogates): the identity JSON would then
        # be unwritable in SQLite and take its whole batch down with it.
        from .serialization import scrub_surrogates
        return scrub_surrogates(value)
    if isinstance(value, (int, float)):
        return value
    return str(value)


def _key_value_text(value: Any) -> Any:
    """Journal form of one column of a composite key; None if unusable.

    Scalars as they are (text scrubbed of lone surrogates), everything
    else through `serialization.serialize_value`: ``yyyy-MM-dd`` for a
    date, the v7 text for a date-time (milliseconds and time spec),
    ``HH:mm:ss`` for a time, ``b64:`` for bytes. Never ``str()`` of a Qt
    object, whose repr names the Python binding and no database value.
    """
    if isinstance(value, (bool, int, float)):
        return value
    from .serialization import serialize_value
    return serialize_value(value)


# ``str()`` of a Qt date / time value as a single-column key has always
# been journaled (and a v7 composite key was, before `_key_value_text`):
# ``PyQt6.QtCore.QDate(2026, 1, 15)``, ``PyQt5.QtCore.QDateTime(2026, 1,
# 15, 8, 0, 0, 0, PyQt5.QtCore.Qt.TimeSpec(1))``. PyQt writes only the
# fields that are set, and the time spec only when it is not local.
_QT_KEY_REPR_RE = re.compile(
    r"^PyQt[56]\.QtCore\.(QDate|QTime|QDateTime)\(([^()]*"
    r"(?:\([^()]*\))?)\)$")
_QT_TIMESPEC_RE = re.compile(
    r"^PyQt[56]\.QtCore\.Qt\.(?:TimeSpec\((\d+)\)|(?:TimeSpec\.)?"
    r"(LocalTime|UTC|OffsetFromUTC|TimeZone))$")
_TIMESPEC_NAMES = {"LocalTime": 0, "UTC": 1, "OffsetFromUTC": 2, "TimeZone": 3}

# Marker: a journaled key value that is a Qt repr the decoder cannot read.
_UNDECODABLE = object()


def _decode_key_repr(value: Any) -> Any:
    """The journal text a legacy Qt-repr key value stands for.

    *value* unchanged when it is no such repr; `_UNDECODABLE` when it is
    one that does not say its value (an offset or a named zone: PyQt
    writes the time spec, not the offset) or cannot be read.
    """
    if not isinstance(value, str) or not value.startswith("PyQt"):
        return value
    match = _QT_KEY_REPR_RE.match(value.strip())
    if match is None:
        return value
    kind = match.group(1)
    parts = [part.strip() for part in match.group(2).split(",")
             if part.strip()]
    spec = 0
    if kind == "QDateTime" and parts and not parts[-1].lstrip("-").isdigit():
        named = _QT_TIMESPEC_RE.match(parts.pop())
        if named is None:
            return _UNDECODABLE
        spec = (int(named.group(1)) if named.group(1) is not None
                else _TIMESPEC_NAMES[named.group(2)])
    try:
        numbers = [int(part) for part in parts]
        from qgis.PyQt.QtCore import QDate, QDateTime, QTime, QTimeZone
    except (ValueError, ImportError):
        return _UNDECODABLE
    from .serialization import serialize_value
    try:
        if kind == "QDate" and len(numbers) == 3:
            decoded = QDate(*numbers)
        elif kind == "QTime" and 2 <= len(numbers) <= 4:
            decoded = QTime(*numbers)
        elif kind == "QDateTime" and 5 <= len(numbers) <= 7 and spec in (0, 1):
            clock = QTime(*(numbers[3:] + [0] * (7 - len(numbers))))
            day = QDate(*numbers[:3])
            decoded = (QDateTime(day, clock) if spec == 0 else QDateTime(
                day, clock, QTimeZone(QTimeZone.Initialization.UTC)))
        else:
            return _UNDECODABLE
    except (TypeError, ValueError):
        return _UNDECODABLE
    text = serialize_value(decoded) if decoded.isValid() else None
    return _UNDECODABLE if text is None else text


def compute_entity_fingerprint(identity_json: Optional[str]) -> Optional[str]:
    """Compute a stable, indexable fingerprint from feature_identity_json.

    Returns a canonical string like 'pk:field_name=value' or 'fid:123'.
    Returns None if identity cannot be determined.

    A complete composite key gives ``pkc:zone_id=12|parcel_no=7``: one
    value per row, in the provider's column order, each value rendered as
    JSON so that a text value holding ``|`` or ``=`` cannot forge another
    key. The ``pkc:`` prefix keeps it disjoint from every single-column
    fingerprint ever written. An incomplete one falls back to the FID.
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
    composite = composite_key(identity)
    if composite:
        return "pkc:" + "|".join(
            f"{name}={json.dumps(value, ensure_ascii=False)}"
            for name, value in composite)
    fid = identity.get("fid")
    if fid is not None:
        return f"fid:{fid}"
    return None


def composite_key(identity: Dict[str, Any]) -> List[Tuple[str, Any]]:
    """``[(column, value), ...]`` of a COMPLETE composite key, else ``[]``.

    Consumers that locate a feature by its key must match EVERY pair (an
    AND over the columns) and accept exactly one row: zero is "absent",
    several is ambiguous and refused by name. That lookup comes FIRST in
    the restore of a DELETE: the duplicate scan that follows it compares
    the non-key columns only, so on a table without geometry whose other
    columns are shared (``(12,7,'libre')`` deleted, ``(12,8,'libre')``
    still there) it finds the neighbour and answers "already exists"
    while the deleted row is not back. `composite_key_filter` gives the
    expression for a `QgsFeatureRequest`.

    ``[]`` for a single-column identity, an incomplete key
    (``pk_incomplete``: a NULL key column designates nothing) and any
    malformed identity: the caller then has the FID and the post-state
    proof only.
    """
    if not isinstance(identity, dict) or identity.get("pk_incomplete"):
        return []
    names = identity.get("pk_fields")
    values = identity.get("pk_values")
    if (not isinstance(names, list) or not isinstance(values, list)
            or len(names) < 2 or len(names) != len(values)):
        return []
    if any(not name for name in names) or any(v is None for v in values):
        return []
    return list(zip(names, values))


def composite_key_filter(pairs: List[Tuple[str, Any]], fields=None) -> str:
    """QGIS expression matching the rows that hold EVERY pair of *pairs*.

    ``"zone_id" = 12 AND "parcel_no" = 7``, each column and value quoted by
    `QgsExpression` (a text value holding a quote cannot escape its
    literal). '' for an empty key, and for a key one of whose values
    cannot be decoded (`effective_identity`): the caller must not turn
    "no key" into "every row".

    With the layer's *fields*, a date-time column is compared as an
    INSTANT, ``"obs" = to_datetime('2026-01-15T08:00:00.000+01:00')``: a
    provider that compiles the plain text literal into SQL compares it as
    TEXT with what it stores, and a GeoPackage stores the UTC form, so
    the row was never found (measured on QGIS 4.2.1). The offset is always
    written: ``to_datetime`` reads a ``Z`` suffix as local time.
    """
    if not pairs:
        return ""
    from qgis.core import QgsExpression
    clauses = []
    for name, value in pairs:
        value = _decode_key_repr(value)
        if value is _UNDECODABLE:
            return ""
        clauses.append(f"{QgsExpression.quotedColumnRef(str(name))} = "
                       f"{_key_literal(value, _field_type_name(fields, name))}")
    return " AND ".join(clauses)


def _field_type_name(fields, name: Any) -> str:
    if fields is None:
        return ""
    try:
        idx = fields.indexOf(str(name))
        return fields.at(idx).typeName() if idx >= 0 else ""
    except (AttributeError, IndexError, TypeError):
        return ""


def _key_literal(value: Any, type_name: str) -> str:
    from qgis.core import QgsExpression
    from .serialization import _DATETIME_TYPE_NAMES, _normalize_type_name
    if (isinstance(value, str)
            and _normalize_type_name(type_name) in _DATETIME_TYPE_NAMES):
        from .serialization import deserialize_value
        decoded = deserialize_value(value, type_name, 7)
        if type(decoded).__name__ == "QDateTime" and decoded.isValid():
            offset = int(decoded.offsetFromUtc())
            sign = "-" if offset < 0 else "+"
            offset = abs(offset)
            text = (decoded.toString("yyyy-MM-ddTHH:mm:ss.zzz")
                    + f"{sign}{offset // 3600:02d}:{(offset % 3600) // 60:02d}")
            return f"to_datetime({QgsExpression.quotedValue(text)})"
    return QgsExpression.quotedValue(value)


def effective_identity(identity: Dict[str, Any], layer) -> Dict[str, Any]:
    """The identity a restore may TRUST on *layer*, legacy events included.

    Events captured before v7 on a layer whose key has several columns
    carry ``pk_field`` / ``pk_value`` = the FIRST column only. The restore
    paths trust that pair without reading the row, so it has to be
    withdrawn before they see it: the returned copy drops it and records
    why (``pk_unproven``), which sends the caller to the FID and the
    post-state proof exactly like a v7 composite identity. The same holds
    when the journaled column is no longer a key column of the layer.

    Returns the identity unchanged when the layer's key has one column
    and it is the journaled one, or when the layer cannot be inspected
    (the caller's own checks still apply). Never raises.

    A key value journaled as the repr of a Qt date / time
    (``PyQt6.QtCore.QDate(2026, 1, 15)``, see `compute_feature_identity`)
    is replaced by the journal text of that value (``2026-01-15``): looked
    up as it was, it matched no row, and the restore answered "not found"
    for a row that was there. A repr that does not say its value (an
    offset or a named zone) withdraws the key (``pk_unproven`` =
    ``key_value_not_decodable``): the target is then proven by its FID and
    its content, never looked up by a value nobody can read.
    """
    if not isinstance(identity, dict):
        return {}
    identity = _decoded_key_values(identity)
    pk_field = identity.get("pk_field")
    if not pk_field or identity.get("pk_value") is None:
        return identity
    try:
        provider = layer.dataProvider()
        pk_indices = list(provider.pkAttributeIndexes() or [])
        fields = layer.fields()
        pk_names = [fields.at(i).name() for i in pk_indices
                    if 0 <= i < fields.count()]
    except Exception as exc:  # noqa: BLE001 - uninspectable: caller decides
        from .logger import flog
        flog(f"identity: key columns of the target unreadable ({exc}); "
             f"journaled key kept as is", "DEBUG")
        return identity
    if len(pk_names) <= 1 and (not pk_names or pk_names[0] == pk_field):
        return identity
    reason = ("legacy_first_column_of_composite_key" if len(pk_names) > 1
              else "journaled_column_not_a_key_of_the_layer")
    downgraded = {k: v for k, v in identity.items()
                  if k not in ("pk_field", "pk_value")}
    downgraded["pk_unproven"] = reason
    downgraded["pk_unproven_column"] = pk_field
    from .logger import flog
    flog(f"identity: journaled key {pk_field}={identity.get('pk_value')!r} "
         f"not trusted on this layer (key columns={pk_names}): {reason}; "
         f"the target must be proven by its post-state", "WARNING")
    return downgraded


def _decoded_key_values(identity: Dict[str, Any]) -> Dict[str, Any]:
    """*identity* with its legacy Qt-repr key values decoded (a copy).

    See `effective_identity`. Returns *identity* itself when no key value
    is such a repr.
    """
    single = identity.get("pk_value")
    several = identity.get("pk_values")
    values = ([single] if identity.get("pk_field") and single is not None
              else list(several) if isinstance(several, list) else [])
    decoded = [_decode_key_repr(value) for value in values]
    if all(new is old for new, old in zip(decoded, values)):
        return identity
    out = dict(identity)
    column = (identity.get("pk_field") if "pk_field" in identity
              else identity.get("pk_fields"))
    from .logger import flog
    if any(value is _UNDECODABLE for value in decoded):
        for key in ("pk_field", "pk_value", "pk_values"):
            out.pop(key, None)
        out["pk_unproven"] = "key_value_not_decodable"
        out["pk_unproven_column"] = column
        flog(f"identity: journaled key {column}={values!r} is the repr of a "
             f"Qt value that does not say it (offset or zone): not looked "
             f"up; the target must be proven by its post-state", "WARNING")
        return out
    if "pk_field" in identity and single is not None:
        out["pk_value"] = decoded[0]
    else:
        out["pk_values"] = decoded
    flog(f"identity: journaled key {column}={values!r} decoded from its Qt "
         f"repr to {decoded!r} for the lookup", "DEBUG")
    return out


def get_identity_strength_for_layer(layer) -> IdentityStrength:
    """Identity strength of a layer, as the support matrix declares it.

    This is the gate `core.restore_service` consults before it writes
    into a layer: its three call sites all refuse on
    `IdentityStrength.NONE`. The function used to carry its OWN
    hard-coded provider list ending in a blanket `return MEDIUM`, so it
    drifted from `support_policy` on exactly the layers where the answer
    decides whether the plugin writes:

      * `virtual`: the matrix declares NONE / restore=False, this
        returned MEDIUM, so the restore gate never fired on a derived
        layer that has no primary source to write back to;
      * any provider ABSENT from the matrix (a third-party provider, a
        newer QGIS one): the default policy refuses it, this returned
        MEDIUM and let a restore write into it.

    Both directions now read the same matrix: what the tracker refuses
    to capture is exactly what the restore refuses to write. The OGR
    refinement stays, because that single provider spans formats from
    GeoPackage (STRONG) to KML (WEAK).

    A layer whose provider is gone answers NONE -- refuse -- rather than
    raising on `None.name()`.
    """
    provider = layer.dataProvider()
    provider_name = provider.name() if provider is not None else ""
    if provider_name == "ogr":
        return refine_ogr_identity(layer.source())
    return get_provider_policy(provider_name).identity_strength


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
