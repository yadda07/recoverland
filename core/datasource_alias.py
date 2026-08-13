"""Datasource alias management (BLK-07) and v6 reconciliation.

A datasource_alias links an obsolete fingerprint to the canonical one,
so that events captured under the old fingerprint stay visible and
restorable when the way the identity is computed changes (schema v6), or
when a layer moves (path change, provider switch, renamed DB).

RECONCILE, NEVER REWRITE
------------------------
The v6 migration does not touch a single row of `audit_event`. For every
fingerprint of the journal that the current canonical rules would write
differently, it inserts ONE row `ancienne -> canonique` here, and every
read path expands the canonical fingerprint into the set of its aliases.
Deleting the alias rows restores the exact pre-migration behaviour --
which a column rewrite could never do.

Public API:
- add_alias(conn, alias_fp, target_fp, note) -> bool
- remove_alias(conn, alias_fp) -> int
- list_aliases(conn) -> list[tuple]
- resolve_fingerprints(conn, fp) -> list[str]
- expand_fingerprints(conn, fp) -> list[str]   (read paths)
- canonical_target_map(conn) -> {alias: final target}
- reconcile_legacy_fingerprints(conn, force=False) -> ReconciliationReport
- load_reconciliation(conn) -> ReconciliationReport | None
- get_ambiguous_datasources(conn) -> list[AmbiguousDatasource]
- describe_reconciliation(conn) -> str   (user-facing, French)

Design notes:
- Transitive resolution is bounded to avoid cycles (CHECK constraint already
  prevents self-alias, but a chain could still loop via multiple rows).
- resolve_fingerprints / expand_fingerprints are read-only, degrade to the
  asked fingerprint when the table does not exist (journals older than v4),
  and are safe for any thread.
- The reconciliation writes with plain `conn.execute`, never `with conn:`:
  it must live INSIDE the schema transaction of `initialize_schema` so an
  interrupted open leaves a journal that is either fully reconciled or not
  reconciled at all -- never half.
- It NEVER raises. A journal that cannot be reconciled is a journal that
  still opens, still reads, and retries at the next open.
"""
import json
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, NamedTuple, Optional, Tuple

from .logger import flog


_MAX_CHAIN_DEPTH = 8

# Bound on one reconciliation pass. A journal with more distinct
# fingerprints than this is scanned partially and reported as
# `truncated` -- NEVER as `ok`, or the pass would never be retried and
# the truncation would become permanent.
_MAX_SCAN = 5000

RECONCILE_SETTING_KEY = "datasource_reconciliation_v6"

_NOTE = "migration v6: empreinte canonique"

# Providers whose fingerprint cannot be re-derived from itself: their
# normalized form is a lossy profile. They get the strict ambiguity
# probe, because aliasing the wrong one attaches the history of a table
# to another table.
_DB_PROVIDERS = ("postgres", "mssql", "oracle")


class AmbiguousDatasource(NamedTuple):
    """A fingerprint the migration refused to alias, and why."""
    fingerprint: str
    provider_type: str
    layer_names: Tuple[str, ...]
    reason: str


class ReconciliationReport(NamedTuple):
    """Outcome of one reconciliation pass, persisted in backend_settings."""
    status: str
    scanned: int
    aliases_created: int
    already_canonical: int
    already_aliased: int
    ambiguous: Tuple[AmbiguousDatasource, ...]
    without_source_uri: Tuple[str, ...]
    completed_at: str = ""

    @property
    def is_degraded(self) -> bool:
        """True when part of the history is NOT attached to its source.

        Three ways that happens: the pass failed or was truncated, a
        source was too ambiguous to alias, or a fingerprint had no
        usable registry URI to recompute from. In all three the user
        keeps every event, but a rewind scoped on the current identity
        will not see the older ones -- which the interface must say
        BEFORE offering a rewind.
        """
        return (self.status != "ok"
                or bool(self.ambiguous)
                or bool(self.without_source_uri))


def add_alias(conn: sqlite3.Connection,
              alias_fingerprint: str,
              target_fingerprint: str,
              note: str = "") -> bool:
    """Register alias_fingerprint -> target_fingerprint.

    Returns False if the pair is invalid, would create a cycle, or already
    resolves to a different target. Logs the outcome.
    """
    if not alias_fingerprint or not target_fingerprint:
        flog("datasource_alias.add: empty fingerprint", "WARNING")
        return False
    if alias_fingerprint == target_fingerprint:
        flog("datasource_alias.add: refused self-alias", "WARNING")
        return False

    # Prevent cycles: the target, resolved, must not already lead back to alias.
    resolved_target = resolve_fingerprints(conn, target_fingerprint)
    if alias_fingerprint in resolved_target:
        flog(f"datasource_alias.add: refused cycle "
             f"{alias_fingerprint} <-> {target_fingerprint}", "WARNING")
        return False

    now = datetime.now(timezone.utc).isoformat()
    try:
        with conn:
            conn.execute(
                "INSERT OR REPLACE INTO datasource_alias "
                "(alias_fingerprint, target_fingerprint, created_at, note) "
                "VALUES (?, ?, ?, ?)",
                (alias_fingerprint, target_fingerprint, now, note or ""),
            )
        flog(f"datasource_alias: {alias_fingerprint} -> {target_fingerprint}")
        return True
    except sqlite3.Error as e:
        flog(f"datasource_alias.add: {e}", "ERROR")
        return False


def remove_alias(conn: sqlite3.Connection, alias_fingerprint: str) -> int:
    """Delete the alias row. Returns number of rows deleted (0 or 1)."""
    try:
        with conn:
            cursor = conn.execute(
                "DELETE FROM datasource_alias WHERE alias_fingerprint = ?",
                (alias_fingerprint,),
            )
            return cursor.rowcount or 0
    except sqlite3.Error as e:
        flog(f"datasource_alias.remove: {e}", "WARNING")
        return 0


def list_aliases(conn: sqlite3.Connection) -> List[Tuple[str, str, str, str]]:
    """Return all aliases as [(alias_fp, target_fp, created_at, note), ...]."""
    try:
        rows = conn.execute(
            "SELECT alias_fingerprint, target_fingerprint, created_at, "
            "COALESCE(note, '') FROM datasource_alias "
            "ORDER BY created_at DESC"
        ).fetchall()
        return [(r[0], r[1], r[2], r[3]) for r in rows]
    except sqlite3.Error as e:
        flog(f"datasource_alias.list: {e}", "WARNING")
        return []


def resolve_fingerprints(conn: sqlite3.Connection,
                         fingerprint: str) -> List[str]:
    """Return every fingerprint equivalent to the given one for queries.

    Includes the input, all aliases pointing at it (direct and transitive),
    and the alias target if the input itself is an alias. Order is stable:
    input first, then discovered fingerprints in insertion order.
    """
    if not fingerprint:
        return []

    seen: List[str] = [fingerprint]
    seen_set = {fingerprint}
    frontier = [fingerprint]

    for _ in range(_MAX_CHAIN_DEPTH):
        if not frontier:
            break
        try:
            placeholders = ",".join("?" for _ in frontier)
            params = tuple(frontier)
            # Rows where this fingerprint is the TARGET: pull the aliases.
            alias_rows = conn.execute(
                "SELECT alias_fingerprint FROM datasource_alias "  # nosec B608
                "WHERE target_fingerprint IN (" + placeholders + ")",
                params,
            ).fetchall()
            # Rows where this fingerprint is an ALIAS: pull the target (for
            # the caller-supplied fingerprint, there is at most one target).
            target_rows = conn.execute(
                "SELECT target_fingerprint FROM datasource_alias "  # nosec B608
                "WHERE alias_fingerprint IN (" + placeholders + ")",
                params,
            ).fetchall()
        except sqlite3.Error as e:
            # Journal older than v4 (no table), or a read error: degrade to
            # what we already know. A partial scope is a smaller rewind; an
            # exception here would make the journal unusable.
            flog(f"datasource_alias.resolve: {e}", "WARNING")
            return seen

        next_frontier: List[str] = []
        for row in alias_rows + target_rows:
            fp = row[0]
            if fp and fp not in seen_set:
                seen.append(fp)
                seen_set.add(fp)
                next_frontier.append(fp)
        frontier = next_frontier
    else:
        flog(f"datasource_alias.resolve: depth cap reached for {fingerprint}",
             "WARNING")

    return seen


def expand_fingerprints(conn: sqlite3.Connection,
                        fingerprint: str) -> List[str]:
    """Scope of a read: the asked fingerprint plus all its aliases.

    Every read path (rewind fetch, count, horizon, entity stream, search,
    registry lookup, Review rebuild) must go through this, or the user
    sees an amputated history without being told.

    Degrades to ``[fingerprint]`` when the alias table is missing.
    """
    if not fingerprint:
        return []
    return resolve_fingerprints(conn, fingerprint)


def canonical_target_map(conn: sqlite3.Connection) -> Dict[str, str]:
    """``{alias_fingerprint: final target}``, chains already resolved.

    Used to normalise the fingerprint carried by every event AT READ
    TIME. Without it the SQL expansion brings the old rows back but each
    keeps its own fingerprint, and `rewind_dedup._entity_key`, which
    prefixes the entity key with `event.datasource_fingerprint`, drops
    one entity into as many dedup buckets as it has historical
    identities. The collapse rules then never see the complete chain.
    """
    try:
        rows = conn.execute(
            "SELECT alias_fingerprint, target_fingerprint FROM datasource_alias"
        ).fetchall()
    except sqlite3.Error as e:
        flog(f"datasource_alias.map: {e}", "DEBUG")
        return {}
    direct = {r[0]: r[1] for r in rows if r[0] and r[1]}
    resolved: Dict[str, str] = {}
    for alias, target in direct.items():
        seen = {alias}
        final = target
        for _ in range(_MAX_CHAIN_DEPTH):
            if final in seen or final not in direct:
                break
            seen.add(final)
            final = direct[final]
        resolved[alias] = final
    return resolved


def canonicalize_event_fingerprints(conn: sqlite3.Connection, events: list) -> list:
    """Rewrite each event's `datasource_fingerprint` to its canonical target.

    The SQL expansion brings the old rows back, but each one still
    carries the fingerprint it was captured under. Downstream,
    `rewind_dedup._entity_key` prefixes the entity key with
    `event.datasource_fingerprint`: one entity would land in as many
    dedup buckets as it has historical identities, and the collapse
    rules (internal INSERT/DELETE cancellation, chain fusion) would
    never see the complete chain -- planning compensations on an entity
    that needs none.

    Nothing is written: the journal keeps the fingerprint of capture,
    only the in-memory view of a read is unified.
    """
    if not events:
        return events
    mapping = canonical_target_map(conn)
    if not mapping:
        return events
    unified = []
    n_rewritten = 0
    for event in events:
        target = mapping.get(event.datasource_fingerprint)
        if target and target != event.datasource_fingerprint:
            unified.append(event._replace(datasource_fingerprint=target))
            n_rewritten += 1
        else:
            unified.append(event)
    if n_rewritten:
        flog(f"datasource_alias: canonicalised the datasource fingerprint of "
             f"{n_rewritten}/{len(events)} event(s) read through an alias",
             "DEBUG")
    return unified


# ---------------------------------------------------------------------------
# v6 reconciliation
# ---------------------------------------------------------------------------


def reconcile_legacy_fingerprints(conn: sqlite3.Connection,
                                  force: bool = False) -> ReconciliationReport:
    """Attach every obsolete fingerprint of the journal to its canonical form.

    Runs at EVERY journal open (there is one journal per project, so it
    will be replayed hundreds of times) and is therefore idempotent: a
    pass already recorded as ``ok`` writes nothing at all -- not the
    alias rows, not the marker. A pass recorded as failed or truncated is
    retried, which is the whole point of keeping the marker distinct from
    the schema version number.

    Never raises: the caller is the journal open path.
    """
    try:
        existing = load_reconciliation(conn)
    except Exception as exc:  # noqa: BLE001 - marker unreadable: redo the pass
        flog(f"datasource_alias.reconcile: marker unreadable ({exc}); "
             f"running the pass again", "WARNING")
        existing = None

    if not force and existing is not None and existing.status == "ok":
        flog(f"datasource_alias.reconcile: status={existing.status} "
             f"already_done=1 aliases={existing.aliases_created} "
             f"scanned={existing.scanned}", "DEBUG")
        return existing

    try:
        report = _run_reconciliation(conn)
    except Exception as exc:  # noqa: BLE001 - never block the open
        flog(f"datasource_alias.reconcile: pass failed ({type(exc).__name__}: "
             f"{exc})", "ERROR")
        report = ReconciliationReport(
            status="failed", scanned=0, aliases_created=0,
            already_canonical=0, already_aliased=0, ambiguous=(),
            without_source_uri=(),
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

    try:
        _store_reconciliation(conn, report)
    except Exception as exc:  # noqa: BLE001
        # Disk full / read-only: the aliases already written stay valid and
        # the missing marker simply means the pass runs again next time.
        flog(f"datasource_alias.reconcile: cannot persist the report "
             f"({type(exc).__name__}: {exc}); the pass will be replayed",
             "WARNING")

    flog(
        f"datasource_alias.reconcile: status={report.status} "
        f"scanned={report.scanned} aliases_created={report.aliases_created} "
        f"already_canonical={report.already_canonical} "
        f"already_aliased={report.already_aliased} "
        f"ambiguous={len(report.ambiguous)} "
        f"without_source_uri={len(report.without_source_uri)} "
        f"degraded={report.is_degraded}",
        "WARNING" if report.is_degraded else "INFO",
    )
    return report


def _run_reconciliation(conn: sqlite3.Connection) -> ReconciliationReport:
    """One pass: scan, decide, write the alias rows. Writes no marker."""
    from .identity import compute_fingerprint_for_source, split_fingerprint

    fingerprints, truncated = _scan_fingerprints(conn)
    registry = _load_registry(conn)
    existing = {row[0]: row[1] for row in list_aliases(conn)}

    created = 0
    already_canonical = 0
    already_aliased = 0
    ambiguous: List[AmbiguousDatasource] = []
    without_source_uri: List[str] = []
    status = "ok"

    for fingerprint in fingerprints:
        provider, _normalized = split_fingerprint(fingerprint)
        entry = registry.get(fingerprint)
        if entry is None:
            without_source_uri.append(fingerprint)
            continue
        registry_provider, source_uri = entry
        if not source_uri:
            without_source_uri.append(fingerprint)
            continue
        if provider and registry_provider and registry_provider != provider:
            # The registry row does not describe THIS source (a fingerprint
            # prefixed `ogr::` cannot be described by a postgres row).
            # Recomputing from it would attach the history to another table.
            without_source_uri.append(fingerprint)
            continue
        try:
            target = compute_fingerprint_for_source(
                registry_provider or provider, source_uri)
        except Exception as exc:  # noqa: BLE001 - unusable URI, never guess
            flog(f"datasource_alias.reconcile: cannot recompute "
                 f"{fingerprint[:60]}...: {exc}", "WARNING")
            without_source_uri.append(fingerprint)
            continue

        if not target or target == fingerprint:
            already_canonical += 1
            continue
        if existing.get(fingerprint) == target:
            already_aliased += 1
            continue
        if fingerprint in existing:
            ambiguous.append(AmbiguousDatasource(
                fingerprint=fingerprint, provider_type=provider,
                layer_names=_layer_names(conn, fingerprint),
                reason="alias_deja_pose_vers_une_autre_cible"))
            continue
        if existing.get(target) == fingerprint:
            # Would close a cycle A -> B -> A. Cannot happen with canonical
            # targets (a canonical form is never itself an alias), so it
            # means the alias table was edited by hand: refuse and say so.
            ambiguous.append(AmbiguousDatasource(
                fingerprint=fingerprint, provider_type=provider,
                layer_names=_layer_names(conn, fingerprint),
                reason="cible_deja_aliasee_vers_cette_empreinte"))
            continue

        verdict = _ambiguity_verdict(conn, fingerprint, provider, target)
        if verdict is not None:
            ambiguous.append(verdict)
            continue

        try:
            _write_alias_row(conn, fingerprint, target, _NOTE)
        except Exception as exc:  # noqa: BLE001 - disk full, I/O, lock
            status = "failed"
            flog(f"datasource_alias.reconcile: alias write failed for "
                 f"{fingerprint[:60]}...: {type(exc).__name__}: {exc}; "
                 f"the pass will be replayed at the next open", "ERROR")
            break
        existing[fingerprint] = target
        created += 1
        flog(f"datasource_alias.reconcile: alias {fingerprint} -> {target}")

    if truncated and status == "ok":
        # Not 'ok': a status of ok is never retried, and the fingerprints
        # left out of this pass would stay detached forever.
        status = "truncated"
        flog(f"datasource_alias.reconcile: scan capped at {_MAX_SCAN} "
             f"fingerprints; the pass stays pending", "WARNING")

    return ReconciliationReport(
        status=status,
        scanned=len(fingerprints),
        aliases_created=created,
        already_canonical=already_canonical,
        already_aliased=already_aliased,
        ambiguous=tuple(ambiguous),
        without_source_uri=tuple(without_source_uri),
        completed_at=datetime.now(timezone.utc).isoformat(),
    )


def _write_alias_row(conn: sqlite3.Connection, alias_fingerprint: str,
                     target_fingerprint: str, note: str) -> None:
    """Insert ONE alias row inside the caller's transaction.

    Deliberately not `add_alias`: no `with conn:` here, or the commit
    would close the schema transaction of `initialize_schema` and a
    crash could leave a journal half migrated. Plain INSERT (never
    OR REPLACE) so an existing row is a programming error, not a silent
    rewrite of its `created_at`.
    """
    conn.execute(
        "INSERT INTO datasource_alias "
        "(alias_fingerprint, target_fingerprint, created_at, note) "
        "VALUES (?, ?, ?, ?)",
        (alias_fingerprint, target_fingerprint,
         datetime.now(timezone.utc).isoformat(), note or ""),
    )


def _scan_fingerprints(conn: sqlite3.Connection) -> Tuple[List[str], bool]:
    """Distinct fingerprints of the journal, bounded. (list, truncated)."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT datasource_fingerprint FROM audit_event "
            "WHERE datasource_fingerprint IS NOT NULL "
            "AND datasource_fingerprint <> '' "
            "ORDER BY datasource_fingerprint LIMIT ?",
            (_MAX_SCAN + 1,),
        ).fetchall()
    except sqlite3.Error as e:
        flog(f"datasource_alias.reconcile: cannot scan the journal: {e}",
             "WARNING")
        return [], False
    fingerprints = [r[0] for r in rows if r[0]]
    if len(fingerprints) > _MAX_SCAN:
        return fingerprints[:_MAX_SCAN], True
    return fingerprints, False


def _load_registry(conn: sqlite3.Connection) -> Dict[str, Tuple[str, str]]:
    """``{fingerprint: (provider_type, source_uri)}``. {} when absent."""
    try:
        rows = conn.execute(
            "SELECT datasource_fingerprint, provider_type, source_uri "
            "FROM datasource_registry LIMIT ?",
            (_MAX_SCAN + 1,),
        ).fetchall()
    except sqlite3.Error as e:
        # Journal older than the registry itself: nothing to recompute from.
        flog(f"datasource_alias.reconcile: no usable registry ({e})", "INFO")
        return {}
    return {r[0]: ((r[1] or ""), (r[2] or "")) for r in rows if r[0]}


def _ambiguity_verdict(conn: sqlite3.Connection, fingerprint: str,
                       provider: str,
                       target: str) -> Optional[AmbiguousDatasource]:
    """Refuse the alias when the evidence does not name ONE source.

    Only DB providers can be ambiguous: a file fingerprint carries its
    own path and layer name, so recomputing it can only ever describe
    the same table. A DB fingerprint written before v6 carried the
    SCHEMA in its table token, so several tables of a schema share it and
    `datasource_registry` (upsert on the fingerprint) kept only ONE of
    their URIs. Aliasing then decrees that the whole history of the
    schema belongs to that one table, and a rewind replays the history
    of `rivieres` onto `routes`.

    The probe asks `layer_id_snapshot` FIRST: a QGIS layer id is stable
    inside a project, while renaming a layer is the most ordinary action
    there is and used to be enough to classify a healthy source as
    ambiguous. The name is only consulted when no layer id was ever
    recorded.
    """
    if provider not in _DB_PROVIDERS:
        return None
    if _table_token(fingerprint) == _table_token(target):
        # The table token does not move: the alias cannot re-attribute the
        # history to another table, whatever else changed (host, port).
        return None

    layer_ids = _distinct(conn, "layer_id_snapshot", fingerprint, limit=5)
    names = _layer_names(conn, fingerprint)
    if len(layer_ids) > 1:
        return AmbiguousDatasource(
            fingerprint=fingerprint, provider_type=provider,
            layer_names=names, reason="plusieurs_couches_sous_une_empreinte")
    if len(layer_ids) == 1:
        return None
    # No layer id at all (journal written by a very old build). For a DB
    # provider we do not guess: one name may still cover several tables.
    if len(names) == 1:
        return None
    return AmbiguousDatasource(
        fingerprint=fingerprint, provider_type=provider, layer_names=names,
        reason=("aucune_identite_de_couche" if not names
                else "plusieurs_noms_de_couche"))


def _table_token(fingerprint: str) -> str:
    """The `table=` token of a normalized DB fingerprint ('' if none)."""
    if not fingerprint:
        return ""
    marker = " table="
    index = fingerprint.rfind(marker)
    if index < 0:
        return ""
    return fingerprint[index + len(marker):]


def _distinct(conn: sqlite3.Connection, column: str, fingerprint: str,
              limit: int = 10) -> Tuple[str, ...]:
    """Distinct non-empty values of `column` for one fingerprint."""
    if column not in ("layer_id_snapshot", "layer_name_snapshot"):
        raise ValueError(f"column not allowed: {column}")
    try:
        # B608: `column` comes from the two-value whitelist just above.
        rows = conn.execute(
            "SELECT DISTINCT " + column + " FROM audit_event "  # nosec B608
            "WHERE datasource_fingerprint = ? AND " + column + " IS NOT NULL "
            "AND " + column + " <> '' ORDER BY " + column + " LIMIT ?",
            (fingerprint, limit),
        ).fetchall()
    except sqlite3.Error as e:
        flog(f"datasource_alias.reconcile: probe on {column} failed: {e}",
             "WARNING")
        return ()
    return tuple(r[0] for r in rows if r[0])


def _layer_names(conn: sqlite3.Connection, fingerprint: str) -> Tuple[str, ...]:
    return _distinct(conn, "layer_name_snapshot", fingerprint, limit=10)


# ---------------------------------------------------------------------------
# Persistence of the report
# ---------------------------------------------------------------------------


def _store_reconciliation(conn: sqlite3.Connection,
                          report: ReconciliationReport) -> None:
    """Persist the report inside the caller's transaction (no commit)."""
    payload = json.dumps({
        "status": report.status,
        "scanned": report.scanned,
        "aliases_created": report.aliases_created,
        "already_canonical": report.already_canonical,
        "already_aliased": report.already_aliased,
        "ambiguous": [
            {"fingerprint": a.fingerprint, "provider_type": a.provider_type,
             "layer_names": list(a.layer_names), "reason": a.reason}
            for a in report.ambiguous
        ],
        "without_source_uri": list(report.without_source_uri),
        "completed_at": report.completed_at,
    }, ensure_ascii=False, sort_keys=True)
    conn.execute(
        "INSERT OR REPLACE INTO backend_settings "
        "(setting_key, setting_value, updated_at) VALUES (?, ?, ?)",
        (RECONCILE_SETTING_KEY, payload,
         datetime.now(timezone.utc).isoformat()),
    )


def load_reconciliation(
        conn: sqlite3.Connection) -> Optional[ReconciliationReport]:
    """Read back the last recorded reconciliation, or None."""
    try:
        row = conn.execute(
            "SELECT setting_value FROM backend_settings WHERE setting_key = ?",
            (RECONCILE_SETTING_KEY,),
        ).fetchone()
    except sqlite3.Error as e:
        flog(f"datasource_alias.load_reconciliation: {e}", "DEBUG")
        return None
    if row is None or not row[0]:
        return None
    try:
        data = json.loads(row[0])
    except (ValueError, TypeError) as e:
        flog(f"datasource_alias.load_reconciliation: unreadable marker: {e}",
             "WARNING")
        return None
    if not isinstance(data, dict):
        return None
    ambiguous = []
    for item in data.get("ambiguous") or []:
        if not isinstance(item, dict):
            continue
        ambiguous.append(AmbiguousDatasource(
            fingerprint=item.get("fingerprint") or "",
            provider_type=item.get("provider_type") or "",
            layer_names=tuple(item.get("layer_names") or ()),
            reason=item.get("reason") or "",
        ))
    return ReconciliationReport(
        status=data.get("status") or "unknown",
        scanned=int(data.get("scanned") or 0),
        aliases_created=int(data.get("aliases_created") or 0),
        already_canonical=int(data.get("already_canonical") or 0),
        already_aliased=int(data.get("already_aliased") or 0),
        ambiguous=tuple(ambiguous),
        without_source_uri=tuple(data.get("without_source_uri") or ()),
        completed_at=data.get("completed_at") or "",
    )


def get_ambiguous_datasources(
        conn: sqlite3.Connection) -> List[AmbiguousDatasource]:
    """Sources the migration refused to attach, for the interface."""
    report = load_reconciliation(conn)
    if report is None:
        return []
    return list(report.ambiguous)


def describe_reconciliation(conn: sqlite3.Connection) -> str:
    """User-facing French summary, '' when nothing has to be said."""
    report = load_reconciliation(conn)
    if report is None or not report.is_degraded:
        return ""
    parts = []
    if report.ambiguous:
        names = []
        for item in report.ambiguous[:3]:
            label = ", ".join(item.layer_names) or item.fingerprint[-40:]
            names.append(label)
        parts.append(
            f"{len(report.ambiguous)} source(s) de donnees n'ont pas pu etre "
            f"rattachees a leur historique ancien sans risque de l'attribuer "
            f"a la mauvaise table ({'; '.join(names)})."
        )
    if report.without_source_uri:
        parts.append(
            f"{len(report.without_source_uri)} source(s) de donnees n'ont "
            f"aucune adresse enregistree dans ce journal : leur historique "
            f"ancien reste consultable mais n'est pas rattache a la couche "
            f"actuelle."
        )
    if report.status != "ok":
        parts.append(
            f"Le rattachement des empreintes ne s'est pas termine "
            f"(etat : {report.status}) ; il sera repris a la prochaine "
            f"ouverture du projet."
        )
    parts.append(
        "Aucun evenement n'est perdu : un rewind lance sur ces couches "
        "peut ne pas couvrir la totalite de leur historique."
    )
    return " ".join(parts)
