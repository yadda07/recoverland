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


def layer_fingerprint_forms(layer) -> Tuple[str, ...]:
    """Every fingerprint form under which this layer's history may be filed."""
    try:
        provider_name = layer.dataProvider().name()
        source = layer.source()
    except Exception:  # noqa: BLE001 - a broken layer is simply unmatchable
        return ()
    forms = [compute_fingerprint_for_source(provider_name, source)]
    legacy = legacy_fingerprint_for_source(provider_name, source)
    if legacy not in forms:
        forms.append(legacy)
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

    Idempotent: canonicalize(canonicalize(x)) == canonicalize(x).
    """
    provider, normalized = split_fingerprint(fingerprint)
    if provider not in _FILE_PROVIDERS or not normalized:
        return fingerprint
    head, _sep, tail = normalized.partition("|")
    return provider + "::" + head.strip() + _identifying_suffix(tail)


def datasource_fingerprints_match(stored: str, current: str) -> bool:
    """Return True when `stored` and `current` denote the same datasource.

    Strict equality first; then the identity form (a pre-v6 fingerprint
    carrying a display filter denotes the same file as the canonical
    one); then cross-mode resolution (absolute vs relative against the
    current QGIS project home) so that audit records produced before
    BL-RW-P2-13 keep matching layers loaded in the new relative mode.
    Cross-mode only applies to file-based providers (`ogr` /
    `spatialite` / `delimitedtext`); DB fingerprints are compared
    strictly (their alias is resolved through the journal instead).
    """
    if not stored or not current:
        return stored == current
    if stored == current:
        return True
    if canonicalize_fingerprint(stored) == canonicalize_fingerprint(current):
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
    head, _sep, tail = source.partition("|")
    path = head.strip().replace("\\", "/")
    if not _is_filesystem_path(head):
        return source.strip()
    if not os.path.isabs(path):
        path = os.path.join(abs_home, path)
    try:
        path = os.path.abspath(path)
        path = os.path.normcase(path)
    except (OSError, ValueError):
        pass
    path = path.replace("\\", "/")
    return path + _identifying_suffix(tail)


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
    if provider_name in _FILE_PROVIDERS:
        return _normalize_file_source(raw_source)
    return raw_source.strip()


def _normalize_db_source(raw: str, profile) -> str:
    """Extract stable parts from a DB URI according to a profile.

    The parts come from `QgsDataSourceUri`, the parser QGIS itself uses
    to build the URI. The historical regex pipeline read the table with
    ``table="([^"]*)"``, which on the form QGIS actually writes --
    ``table="public"."routes" (geom)`` -- captured ``public``, i.e. the
    SCHEMA: every table of a schema collapsed onto a single identity and
    their histories merged.

    Output keeps the historical ``key=value`` space-separated layout, so
    a fingerprint whose parts were already correct stays valid.
    """
    parts = _db_parts_from_qgis(raw)
    if parts is None:
        parts = _db_parts_from_regex(raw, profile)
    return " ".join(
        f"{key}={parts.get(key) or default}"
        for key, default in profile
    )


def _db_parts_from_qgis(raw: str) -> Optional[Dict[str, str]]:
    """Parse a DB URI with QgsDataSourceUri. None when unavailable."""
    try:
        from qgis.core import QgsDataSourceUri
    except Exception:  # noqa: BLE001 - no QGIS runtime: caller falls back
        return None
    try:
        uri = QgsDataSourceUri(raw)
        parts = {
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
    for key, _default in profile:
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
    for layer in layers or []:
        try:
            fp = compute_datasource_fingerprint(layer)
        except Exception as exc:  # noqa: BLE001 - never block on a bad layer
            flog(f"identity: fingerprint failed for a layer: {exc}", "WARNING")
            continue
        if not fp:
            continue
        by_fp.setdefault(fp, []).append(extract_layer_name(layer))
    return {fp: names for fp, names in by_fp.items() if len(names) > 1}


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


def _normalize_file_source(raw: str) -> str:
    """Normalize a file-based source URI.

    Honours `RECOVERLAND_FINGERPRINT_MODE`:
      - 'absolute' (default) -> historical behaviour: abspath + normcase.
      - 'relative'           -> path made relative to the current QGIS
        project home, with `os.path.normcase` still applied for case
        consistency. Falls back to absolute mode with a WARNING log
        when no project home is available.

    Sources that are not filesystem paths (delimitedtext URIs,
    spatialite connection strings) keep their text as-is: absolutising
    them made the fingerprint depend on the process working directory.
    """
    head, _sep, tail = raw.partition("|")
    suffix = _identifying_suffix(tail)
    if not _is_filesystem_path(head):
        return head.strip() + suffix
    path = head.strip().replace("\\", "/")
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
