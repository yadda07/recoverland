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
- service_history_candidates(conn) -> list[ServiceHistoryCandidate]
- attach_service_history(conn, fp, service) -> (bool, str)  (user decision)

Rules for a DB fingerprint (the only kind that can be ambiguous)
---------------------------------------------------------------
A NEW alias is created only when the events filed under the old
fingerprint are consistent with ONE table: one layer id (or one name),
one geometry family, the registered table's family, and field name lists
that nest (columns only added, or only dropped, over time: a join or a
column added keeps the alias). An alias already in place (written by a
6.x pass) is only WITHDRAWN on evidence that does not depend on the
table token, which every legacy PostGIS fingerprint changes: geometry
families that differ, or field lists that share nothing beyond the key.
A renamed column or a swapped join keeps it. The two thresholds differ on
purpose: withdrawing detaches a history the rewind already reaches, and
is only done on proof of two tables; a new alias refused costs an
automatic attachment, is reported, and can be made by hand.

A fingerprint written before v7 for a table reached through a connection
SERVICE did not record the service. It is attached to the v7 service
fingerprint when the journal and this workstation know ONE service for
that provider (the attachment is reported as an assumption); several
services, or a list that cannot be established, refuse it by name with
the services in cause, and `attach_service_history` attaches it to the
service the user names.

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
import re
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

# Revision of the canonical rules the recorded pass was run against. A pass
# recorded "ok" is never replayed, so when the rules change (v7: service
# token on DB sources, spatialite filter tokens dropped) the revision is
# bumped and a journal reconciled under an older revision is examined once
# more: otherwise its obsolete fingerprints would stay detached from the
# new canonical ones, and the rewind would miss them without a word.
# Revision 3: field lists that nest keep a new alias, a withdrawal needs
# proof independent of the table token, a service-less history is
# attached when one service only is known.
RECONCILE_REVISION = 3

_NOTE = "migration v6: empreinte canonique"
_NOTE_SERVICE = "migration v7: service unique"
_NOTE_MANUAL_SERVICE = "rattachement manuel : service "

_REASON_SERVICE = "service_de_connexion_non_enregistre"

# Providers whose fingerprint cannot be re-derived from itself: their
# normalized form is a lossy profile. They get the strict ambiguity
# probe, because aliasing the wrong one attaches the history of a table
# to another table.
_DB_PROVIDERS = ("postgres", "mssql", "oracle")


class AmbiguousDatasource(NamedTuple):
    """A fingerprint the migration refused to alias, and why.

    *detail* completes *reason* for the user (the services in cause for
    a connection service), '' when the reason says it all.
    """
    fingerprint: str
    provider_type: str
    layer_names: Tuple[str, ...]
    reason: str
    detail: str = ""


class AssumedAttachment(NamedTuple):
    """A history the migration attached on an assumption it must state.

    Today: a fingerprint written before v7 without its connection service,
    attached to the only service known (*detail* names it).
    """
    fingerprint: str
    target: str
    provider_type: str
    layer_names: Tuple[str, ...]
    detail: str


class ServiceHistoryCandidate(NamedTuple):
    """A service-less history the user may attach to a service he names."""
    fingerprint: str
    provider_type: str
    layer_names: Tuple[str, ...]
    known_services: Tuple[str, ...]
    detail: str


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
    revision: int = 1
    assumed: Tuple[AssumedAttachment, ...] = ()

    @property
    def needs_notice(self) -> bool:
        """True when the user must be told something: a degradation, or an
        attachment made on an assumption (`describe_reconciliation`)."""
        return self.is_degraded or bool(self.assumed)

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
    resolved = resolve_fingerprints(conn, fingerprint)
    seen = set(resolved)
    for eq in _implicit_equivalent_fingerprints(conn, resolved):
        if eq and eq not in seen:
            resolved.append(eq)
            seen.add(eq)
    return resolved


def _implicit_equivalent_fingerprints(
    conn: sqlite3.Connection, base_fps: List[str],
) -> List[str]:
    """Single-layer bare/named OGR twins and case-folded SpatiaLite twins."""
    from .identity import (
        canonicalize_fingerprint, split_fingerprint,
        _single_layer_bare_forms, _spatialite_comparison_key,
    )
    extra: List[str] = []
    spl_keys = set()
    for fp in base_fps:
        canon = canonicalize_fingerprint(fp)
        if canon and canon != fp:
            extra.append(canon)
        for bare in _single_layer_bare_forms("ogr", canon or fp):
            extra.append(bare)
        prov, body = split_fingerprint(fp)
        if prov == "spatialite" and body:
            spl_keys.add(_spatialite_comparison_key(body, ""))
    if spl_keys:
        try:
            rows = conn.execute(
                "SELECT DISTINCT datasource_fingerprint FROM audit_event "
                "WHERE datasource_fingerprint LIKE 'spatialite::%' LIMIT 500"
            ).fetchall()
            for (cand,) in rows:
                _p, cbody = split_fingerprint(cand or "")
                if cbody and _spatialite_comparison_key(cbody, "") in spl_keys:
                    extra.append(cand)
        except sqlite3.Error:
            pass
    return extra


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
    from .identity import canonicalize_fingerprint
    mapping = canonical_target_map(conn)
    unified = []
    n_rewritten = 0
    for event in events:
        raw_fp = event.datasource_fingerprint
        target = mapping.get(raw_fp) if mapping else None
        if not target and raw_fp and raw_fp.startswith("ogr::"):
            canon = canonicalize_fingerprint(raw_fp)
            if canon != raw_fp:
                target = mapping.get(canon, canon) if mapping else canon
        if target and target != raw_fp:
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

    if (not force and existing is not None and existing.status == "ok"
            and existing.revision >= RECONCILE_REVISION):
        flog(f"datasource_alias.reconcile: status={existing.status} "
             f"already_done=1 aliases={existing.aliases_created} "
             f"scanned={existing.scanned}", "DEBUG")
        return existing
    if existing is not None and existing.revision < RECONCILE_REVISION:
        flog(f"datasource_alias.reconcile: pass recorded under rules "
             f"revision {existing.revision}, current {RECONCILE_REVISION}: "
             f"examined again", "INFO")

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
            revision=RECONCILE_REVISION,
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
        f"assumed={len(report.assumed)} "
        f"degraded={report.is_degraded}",
        "WARNING" if report.needs_notice else "INFO",
    )
    return report


def _run_reconciliation(conn: sqlite3.Connection) -> ReconciliationReport:
    """One pass: scan, decide, write the alias rows. Writes no marker."""
    from .identity import compute_fingerprint_for_source, split_fingerprint

    fingerprints, truncated = _scan_fingerprints(conn)
    registry = _load_registry(conn)
    alias_rows = list_aliases(conn)
    existing = {row[0]: row[1] for row in alias_rows}
    notes = {row[0]: row[3] for row in alias_rows}

    created = 0
    already_canonical = 0
    already_aliased = 0
    ambiguous: List[AmbiguousDatasource] = []
    assumed: List[AssumedAttachment] = []
    without_source_uri: List[str] = []
    status = "ok"
    services = _ServiceKnowledge(conn)

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
        decision = _service_decision(conn, services, fingerprint, provider,
                                     target)
        if decision is not None:
            kind, detail = decision
            names = _layer_names(conn, fingerprint)
            if fingerprint in existing:
                # Attached already: by an earlier pass (still an
                # assumption, still said) or by the user (his decision).
                if existing[fingerprint] != target:
                    ambiguous.append(AmbiguousDatasource(
                        fingerprint=fingerprint, provider_type=provider,
                        layer_names=names,
                        reason="alias_deja_pose_vers_une_autre_cible"))
                    continue
                already_aliased += 1
                if notes.get(fingerprint) == _NOTE_SERVICE:
                    assumed.append(AssumedAttachment(
                        fingerprint, target, provider, names, detail))
                continue
            if kind == "refuse":
                ambiguous.append(AmbiguousDatasource(
                    fingerprint=fingerprint, provider_type=provider,
                    layer_names=names, reason=_REASON_SERVICE,
                    detail=detail))
                continue
            # One service settles the database, not the table: a pre-v6
            # fingerprint (schema in its table token) still goes through
            # the same one-table evidence as any other DB alias.
            verdict = _ambiguity_verdict(conn, fingerprint, provider, target)
            if verdict is not None:
                ambiguous.append(verdict)
                continue
            try:
                _write_alias_row(conn, fingerprint, target, _NOTE_SERVICE)
            except Exception as exc:  # noqa: BLE001 - disk full, I/O, lock
                status = "failed"
                flog(f"datasource_alias.reconcile: alias write failed for "
                     f"{fingerprint[:60]}...: {type(exc).__name__}: {exc}; "
                     f"the pass will be replayed at the next open", "ERROR")
                break
            existing[fingerprint] = target
            created += 1
            assumed.append(AssumedAttachment(
                fingerprint, target, provider, names, detail))
            flog(f"datasource_alias.reconcile: alias {fingerprint} -> "
                 f"{target} ASSUMED ({detail}); reported to the user",
                 "WARNING")
            continue
        if existing.get(fingerprint) == target:
            retracted = _retract_disproved_alias(
                conn, fingerprint, provider, target, notes.get(fingerprint))
            if retracted is not None:
                ambiguous.append(retracted)
                existing.pop(fingerprint, None)
                continue
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
        revision=RECONCILE_REVISION,
        assumed=tuple(assumed),
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

    One layer id is NOT one table, though: "Change Data Source" repoints
    a layer to another table of the same schema and keeps its id, so the
    legacy fingerprint (and the registry row, upserted on it) then covers
    the two tables, and the registry names the last one. What the events
    themselves captured settles it: one table has one geometry family and
    field name lists that nest (columns only added, or only dropped, over
    time). Two families under the fingerprint, a family the registered
    table does not have, or field lists that do not nest (``gid, nom``
    then ``gid, debit``), and the alias is refused. The field lists are
    ``layer.fields()`` of each capture, joined and virtual fields
    included: nothing in the journal tells them apart, so a join added is
    a column added (kept), a join swapped for another is refused here (it
    costs an automatic attachment, reported, re-attachable by hand; an
    alias already in place survives it, see `_two_tables_evidence`).
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
    # No layer id at all (journal written by a very old build). For a DB
    # provider we do not guess: one name may still cover several tables.
    if len(layer_ids) == 1 or len(names) == 1:
        return _content_verdict(conn, fingerprint, provider, names)
    return AmbiguousDatasource(
        fingerprint=fingerprint, provider_type=provider, layer_names=names,
        reason=("aucune_identite_de_couche" if not names
                else "plusieurs_noms_de_couche"))


def _content_verdict(conn: sqlite3.Connection, fingerprint: str,
                     provider: str,
                     names: Tuple[str, ...]) -> Optional[AmbiguousDatasource]:
    """Refuse when the events under *fingerprint* describe several tables."""
    def refuse(reason: str) -> AmbiguousDatasource:
        return AmbiguousDatasource(
            fingerprint=fingerprint, provider_type=provider,
            layer_names=names, reason=reason)

    field_sets = _field_name_sets(conn, fingerprint)
    if _first_non_nesting_pair(field_sets) is not None:
        return refuse("plusieurs_schemas_de_champs_sous_une_empreinte")
    families = _geometry_families(conn, fingerprint)
    if len(families) > 1:
        return refuse("plusieurs_types_de_geometrie_sous_une_empreinte")
    registered = _geometry_family(_registered_geometry_type(conn, fingerprint))
    if families and registered and registered not in families:
        return refuse("geometrie_differente_de_la_table_enregistree")
    return None


# Bound on the distinct field lists examined per fingerprint. A journal
# with more than this many schema variants under ONE fingerprint is not a
# single healthy table anyway; beyond the bound the lists are not read.
_MAX_FIELD_SCHEMAS = 50


def _field_name_sets(conn: sqlite3.Connection,
                     fingerprint: str) -> List[frozenset]:
    """Distinct field NAME sets of the events under *fingerprint*.

    Lengths, types and precisions are left out: a widened column is the
    same table. Unreadable schema texts are skipped.
    """
    sets = []
    for text in _distinct(conn, "field_schema_json", fingerprint,
                          limit=_MAX_FIELD_SCHEMAS):
        try:
            names = frozenset(str(field.get("name")) for field in json.loads(text)
                              if isinstance(field, dict))
        except (ValueError, TypeError, AttributeError):
            continue
        if names and names not in sets:
            sets.append(names)
    return sets


def _first_non_nesting_pair(field_sets: List[frozenset]):
    """Two field sets neither of which contains the other, else None."""
    for idx, first in enumerate(field_sets):
        for second in field_sets[idx + 1:]:
            if not (first <= second or second <= first):
                return first, second
    return None


def _geometry_families(conn: sqlite3.Connection, fingerprint: str) -> set:
    families = {_geometry_family(g) for g in
                _distinct(conn, "geometry_type", fingerprint, limit=10)}
    families.discard("")
    return families


def _key_columns(conn: sqlite3.Connection, fingerprint: str) -> set:
    """Key columns the events under *fingerprint* journaled, possibly none."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT json_extract(feature_identity_json, '$.pk_field'), "
            "json_extract(feature_identity_json, '$.pk_fields') "
            "FROM audit_event WHERE datasource_fingerprint = ? "
            "AND feature_identity_json IS NOT NULL LIMIT 20",
            (fingerprint,)).fetchall()
    except sqlite3.Error as e:
        flog(f"datasource_alias.reconcile: key column probe failed: {e}",
             "WARNING")
        return set()
    keys = set()
    for single, several in rows:
        if single:
            keys.add(str(single))
        if several:
            try:
                keys.update(str(name) for name in json.loads(several))
            except (ValueError, TypeError):
                continue
    return keys


def _retract_disproved_alias(conn: sqlite3.Connection, fingerprint: str,
                             provider: str, target: str,
                             note: Optional[str]) -> Optional[AmbiguousDatasource]:
    """Withdraw an alias an earlier pass guessed and the events now disprove.

    A journal reconciled by 6.x may already hold the alias of a layer
    repointed within a schema (see `_ambiguity_verdict`): its whole
    history is attached to the last table, and every rewind on that table
    replays the other table's events on its rows. Declining to create
    such an alias is not enough for those journals.

    Only an alias written by the migration itself (its note) is examined,
    never one a user added by hand, and only on evidence that two tables
    are involved, stronger than what refuses a NEW alias, and that does
    not rest on the table token (every legacy PostGIS fingerprint carried
    the schema there, so "the token changes" singles nothing out):
    geometry families that differ, or field lists that share nothing
    beyond the key (see `_two_tables_evidence`). A renamed column, a join
    swapped for another, columns added or dropped: the alias stays. The
    alias row is deleted inside the caller's transaction and the source
    is reported with the reason; the events stay readable under their own
    fingerprint, and `add_alias` re-attaches them if the user knows
    better.
    """
    if (provider not in _DB_PROVIDERS or note != _NOTE
            or _table_token(fingerprint) == _table_token(target)):
        return None
    reason = _two_tables_evidence(conn, fingerprint)
    if reason is None:
        return None
    try:
        conn.execute("DELETE FROM datasource_alias WHERE alias_fingerprint = ?",
                     (fingerprint,))
    except sqlite3.Error as exc:
        flog(f"datasource_alias.reconcile: cannot withdraw the disproved "
             f"alias {fingerprint} -> {target} ({exc}); it stays in place",
             "ERROR")
        return None
    flog(f"datasource_alias.reconcile: alias {fingerprint} -> {target} "
         f"withdrawn: {reason}; its events stay under their own "
         f"fingerprint", "WARNING")
    return AmbiguousDatasource(
        fingerprint=fingerprint, provider_type=provider,
        layer_names=_layer_names(conn, fingerprint),
        reason="alias_v6_retire_" + reason)


def _two_tables_evidence(conn: sqlite3.Connection,
                         fingerprint: str) -> Optional[str]:
    """Why the events under *fingerprint* come from two tables, or None.

    Proof, not suspicion: geometry families that differ (among the events,
    or against the registered table), or two field lists that share
    nothing beyond the key columns the events journaled (``gid, nom`` and
    ``gid, debit``). Lists that merely do not nest (a column renamed, a
    join swapped) are what a healthy table goes through, and are not
    evidence here.
    """
    families = _geometry_families(conn, fingerprint)
    if len(families) > 1:
        return "plusieurs_types_de_geometrie_sous_une_empreinte"
    registered = _geometry_family(_registered_geometry_type(conn, fingerprint))
    if families and registered and registered not in families:
        return "geometrie_differente_de_la_table_enregistree"
    field_sets = _field_name_sets(conn, fingerprint)
    if _first_non_nesting_pair(field_sets) is None:
        return None
    keys = _key_columns(conn, fingerprint)
    for idx, first in enumerate(field_sets):
        for second in field_sets[idx + 1:]:
            if first <= second or second <= first:
                continue
            if (first - keys and second - keys
                    and not (first & second) - keys):
                return "plusieurs_schemas_de_champs_sous_une_empreinte"
    return None


def _registered_geometry_type(conn: sqlite3.Connection,
                              fingerprint: str) -> str:
    try:
        row = conn.execute(
            "SELECT geometry_type FROM datasource_registry "
            "WHERE datasource_fingerprint = ?", (fingerprint,)).fetchone()
    except sqlite3.Error as e:
        flog(f"datasource_alias.reconcile: registry geometry probe failed: "
             f"{e}", "WARNING")
        return ""
    return (row[0] or "") if row else ""


def _geometry_family(geometry_type: str) -> str:
    """'MultiLineString' / 'LineStringZ' / 'Line' -> 'line'. '' if unknown.

    The registry and the events do not always spell the type the same
    way (WKB type name, geometry type name, single vs multi): only the
    FAMILY is compared, which is what tells two tables apart.
    """
    text = (geometry_type or "").strip().lower()
    for family, markers in (("point", ("point",)),
                            ("line", ("line", "curve")),
                            ("polygon", ("polygon", "surface")),
                            ("none", ("nogeometry", "null", "none"))):
        if any(marker in text for marker in markers):
            return family
    return text


class _ServiceKnowledge:
    """Connection services known, per DB provider: journal and workstation.

    The journal knows the services of every URI in its registry and of
    every v7 fingerprint it holds (events, aliases). The workstation knows
    the services its client library can resolve
    (`datasource_registry.configured_connection_services`). Both are read
    once per pass and per provider.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._journal: Dict[str, set] = {}
        self._configured: Dict[str, tuple] = {}

    def journal(self, provider: str) -> set:
        if provider not in self._journal:
            self._journal[provider] = _journal_services(self._conn, provider)
        return self._journal[provider]

    def configured(self, provider: str) -> tuple:
        if provider not in self._configured:
            from .datasource_registry import configured_connection_services
            try:
                self._configured[provider] = configured_connection_services(
                    provider)
            except Exception as exc:  # noqa: BLE001 - unreadable: not known
                flog(f"datasource_alias.reconcile: connection services of "
                     f"{provider} unreadable ({exc})", "WARNING")
                self._configured[provider] = (None, ())
        return self._configured[provider]


def _journal_services(conn: sqlite3.Connection, provider: str) -> set:
    """Services named anywhere in this journal for *provider*."""
    from .identity import fingerprint_service
    services = set()
    pattern = f"{provider}::service=%"
    for sql in ("SELECT DISTINCT datasource_fingerprint FROM audit_event "
                "WHERE datasource_fingerprint LIKE ? LIMIT 1000",
                "SELECT target_fingerprint FROM datasource_alias "
                "WHERE target_fingerprint LIKE ? LIMIT 1000",
                "SELECT datasource_fingerprint FROM datasource_registry "
                "WHERE datasource_fingerprint LIKE ? LIMIT 1000"):
        try:
            rows = conn.execute(sql, (pattern,)).fetchall()
        except sqlite3.Error as e:
            flog(f"datasource_alias.reconcile: service probe failed: {e}",
                 "WARNING")
            continue
        services.update(fingerprint_service(r[0]) for r in rows if r[0])
    try:
        rows = conn.execute(
            "SELECT source_uri FROM datasource_registry WHERE provider_type = ? "
            "AND source_uri IS NOT NULL LIMIT 1000", (provider,)).fetchall()
    except sqlite3.Error as e:
        flog(f"datasource_alias.reconcile: registry service probe failed: {e}",
             "WARNING")
        rows = []
    for (uri,) in rows:
        name = _uri_service(uri)
        if name:
            services.add(name)
    services.discard("")
    return services


def _uri_service(uri: str) -> str:
    try:
        from qgis.core import QgsDataSourceUri
        return (QgsDataSourceUri(uri or "").service() or "").strip()
    except Exception:  # noqa: BLE001 - no QGIS runtime: regex below
        match = (re.search(r"service='([^']*)'", uri or "")
                 or re.search(r"service=(\S+)", uri or ""))
        return match.group(1).strip() if match else ""


def _service_decision(conn: sqlite3.Connection, knowledge: "_ServiceKnowledge",
                      fingerprint: str, provider: str,
                      target: str) -> Optional[Tuple[str, str]]:
    """``("attach" | "refuse", detail)`` for a service-less DB fingerprint.

    Before v7 a DB fingerprint did not record the connection SERVICE: every
    ``service='...'`` URI of a table gave the same fingerprint, whatever
    database the service pointed at, and the registry row (upserted on it)
    keeps the URI of the LAST service seen. The events filed under it may
    come from `test` while the registry says `prod`.

    Refusing every such history cost the common setup (one service, ever)
    the rewind of its whole past. So: when the service of the registered
    URI is the ONLY service this journal and this workstation know for the
    provider, there is no other database the events can come from, and
    the history is attached (reported as an assumption). Several services
    known, or a workstation list that cannot be read or does not even
    contain the registered service (then it is incomplete and proves
    nothing), and it is refused, the services in cause named in *detail*.

    None when *fingerprint* is not a service-less DB fingerprint whose
    canonical form goes through a service.
    """
    if provider not in _DB_PROVIDERS:
        return None
    from .identity import SERVICE_TOKEN, fingerprint_service, split_fingerprint

    _target_provider, target_body = split_fingerprint(target)
    _provider, body = split_fingerprint(fingerprint)
    if (not target_body.startswith(SERVICE_TOKEN)
            or body.startswith(SERVICE_TOKEN)):
        return None
    service = fingerprint_service(target)
    configured, sources = knowledge.configured(provider)
    read = ", ".join(sources) or "aucun fichier de services trouve"
    if configured is None:
        return "refuse", (f"service '{service}' ; la liste des services de "
                          f"ce poste ne peut pas etre lue pour {provider}")
    if service not in configured:
        return "refuse", (f"service '{service}' absent des services lus sur "
                          f"ce poste ({read}) : la liste des services n'est "
                          f"pas etablie")
    known = set(configured) | knowledge.journal(provider) | {service}
    if known != {service}:
        return "refuse", (f"plusieurs services connus : "
                          f"{', '.join(sorted(known))}")
    return "attach", (f"service '{service}', le seul connu du journal et de "
                      f"ce poste ({read})")


def service_history_candidates(
        conn: sqlite3.Connection) -> List[ServiceHistoryCandidate]:
    """Service-less histories refused by the reconciliation, for the interface.

    Each comes with the services known for its provider, so the user can
    be asked which one the old events came from (`attach_service_history`).
    """
    report = load_reconciliation(conn)
    if report is None:
        return []
    knowledge = _ServiceKnowledge(conn)
    out = []
    for item in report.ambiguous:
        if item.reason != _REASON_SERVICE:
            continue
        configured, _sources = knowledge.configured(item.provider_type)
        known = set(configured or ()) | knowledge.journal(item.provider_type)
        out.append(ServiceHistoryCandidate(
            fingerprint=item.fingerprint, provider_type=item.provider_type,
            layer_names=item.layer_names,
            known_services=tuple(sorted(known)), detail=item.detail))
    return out


def attach_service_history(conn: sqlite3.Connection, fingerprint: str,
                           service: str) -> Tuple[bool, str]:
    """Attach a service-less history to the service the USER names.

    The target is the table the REGISTRY names for *fingerprint*, reached
    through that service: the v6 form of the registered URI
    (`identity.v6_fingerprint_for_source`) given the service token
    (`identity.with_service`). Never rebuilt from the stored body: a
    fingerprint written before v6 carried the SCHEMA in its table token
    (``schema=public table=public``), and the target it gave named a table
    that does not exist; the rewind scope of the service layer stayed
    empty while success was announced, and the source left the report for
    good. When the table token moves that way, the events must describe
    ONE table, the evidence the automatic pass requires
    (`_ambiguity_verdict`).

    Success is announced, and the source dropped from the stored report,
    only once the alias is written AND the rewind scope of the target
    (`expand_fingerprints`) is seen to hold every event filed under
    *fingerprint*; otherwise the alias is removed and the refusal
    returned. Refused, with the reason, when *fingerprint* is not a
    service-less DB fingerprint, when *service* is empty, when no
    registered URI of that provider names its table (or that URI does
    not go through a service), when the events describe several tables,
    or when the fingerprint is already attached elsewhere. Returns
    ``(ok, message)``.
    """
    from .identity import (
        SERVICE_TOKEN, compute_fingerprint_for_source, split_fingerprint,
        v6_fingerprint_for_source, with_service,
    )
    name = (service or "").strip()
    provider, _body = split_fingerprint(fingerprint or "")

    def refuse(reason: str) -> Tuple[bool, str]:
        message = f"rattachement refuse : {reason}"
        flog(f"datasource_alias.attach_service: {fingerprint!r} -> "
             f"{name!r}: {message}", "WARNING")
        return False, message

    if not with_service(fingerprint, name):
        return refuse(f"{fingerprint!r} n'est pas une empreinte de base de "
                      f"donnees sans service, ou {service!r} n'est pas un nom "
                      f"de service")
    entry = _load_registry(conn).get(fingerprint)
    registry_provider, uri = entry if entry is not None else ("", "")
    if not uri or (registry_provider and registry_provider != provider):
        return refuse("aucune adresse de source enregistree pour cette "
                      "empreinte : la table dont vient l'historique ne peut "
                      "pas etre etablie")
    try:
        canonical = compute_fingerprint_for_source(
            registry_provider or provider, uri)
        target = with_service(
            v6_fingerprint_for_source(registry_provider or provider, uri), name)
    except Exception as exc:  # noqa: BLE001 - unusable URI, never guess
        return refuse(f"adresse enregistree illisible ({exc})")
    if not target or not split_fingerprint(canonical)[1].startswith(
            SERVICE_TOKEN):
        return refuse("la source enregistree nomme son hote et sa base sans "
                      "passer par un service : il n'y a pas de service a "
                      "choisir")
    table = _table_token(target)
    current = {row[0]: row[1] for row in list_aliases(conn)}.get(fingerprint)
    if current == target:
        return True, f"deja rattache au service '{name}' (table {table})"
    if current is not None:
        return refuse(f"{fingerprint!r} est deja rattache a {current!r}")
    verdict = _ambiguity_verdict(conn, fingerprint, provider, target)
    if verdict is not None:
        return refuse(f"les evenements de cette empreinte ne decrivent pas "
                      f"une seule table ({verdict.reason}) : le service "
                      f"designe la base, pas la table")
    if not add_alias(conn, fingerprint, target,
                     _NOTE_MANUAL_SERVICE + name):
        return refuse("ecriture refusee par le journal (voir le journal "
                      "technique)")
    held, expected = _events_in_scope(conn, fingerprint, target)
    if held is None or held != expected:
        remove_alias(conn, fingerprint)
        return refuse(f"apres rattachement, la portee du rewind de {target!r} "
                      f"ne contient que {held} evenement(s) sur {expected} : "
                      f"rattachement retire")
    _forget_ambiguous(conn, fingerprint)
    flog(f"datasource_alias.attach_service: {fingerprint} -> {target} "
         f"(user decision, {held} event(s) now in the rewind scope)",
         "WARNING")
    return True, (f"historique rattache au service '{name}' (table {table}, "
                  f"{held} evenement(s))")


def _events_in_scope(conn: sqlite3.Connection, fingerprint: str,
                     target: str) -> Tuple[Optional[int], int]:
    """(events of *fingerprint* in the scope of *target*, events of it).

    None as the first item when the scope cannot be read.
    """
    try:
        expected = conn.execute(
            "SELECT COUNT(*) FROM audit_event WHERE datasource_fingerprint = ?",
            (fingerprint,)).fetchone()[0]
    except sqlite3.Error as exc:
        flog(f"datasource_alias.attach_service: event count failed: {exc}",
             "WARNING")
        return None, 0
    scope = expand_fingerprints(conn, target)
    if fingerprint not in scope:
        return 0, int(expected)
    return int(expected), int(expected)


def _forget_ambiguous(conn: sqlite3.Connection, fingerprint: str) -> None:
    """Drop *fingerprint* from the stored report once the user attached it."""
    report = load_reconciliation(conn)
    if report is None:
        return
    kept = tuple(a for a in report.ambiguous if a.fingerprint != fingerprint)
    if len(kept) == len(report.ambiguous):
        return
    try:
        with conn:
            _store_reconciliation(conn, report._replace(ambiguous=kept))
    except sqlite3.Error as exc:
        flog(f"datasource_alias.attach_service: report not updated ({exc}); "
             f"the source stays listed until the next pass", "WARNING")


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
    if column not in ("layer_id_snapshot", "layer_name_snapshot",
                      "field_schema_json", "geometry_type"):
        raise ValueError(f"column not allowed: {column}")
    try:
        # B608: `column` comes from the whitelist just above.
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
             "layer_names": list(a.layer_names), "reason": a.reason,
             "detail": a.detail}
            for a in report.ambiguous
        ],
        "assumed": [
            {"fingerprint": a.fingerprint, "target": a.target,
             "provider_type": a.provider_type,
             "layer_names": list(a.layer_names), "detail": a.detail}
            for a in report.assumed
        ],
        "without_source_uri": list(report.without_source_uri),
        "completed_at": report.completed_at,
        "revision": report.revision,
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
            detail=item.get("detail") or "",
        ))
    assumed = []
    for item in data.get("assumed") or []:
        if not isinstance(item, dict):
            continue
        assumed.append(AssumedAttachment(
            fingerprint=item.get("fingerprint") or "",
            target=item.get("target") or "",
            provider_type=item.get("provider_type") or "",
            layer_names=tuple(item.get("layer_names") or ()),
            detail=item.get("detail") or "",
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
        revision=_as_int(data.get("revision"), 1),
        assumed=tuple(assumed),
    )


def _as_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_ambiguous_datasources(
        conn: sqlite3.Connection) -> List[AmbiguousDatasource]:
    """Sources the migration refused to attach, for the interface."""
    report = load_reconciliation(conn)
    if report is None:
        return []
    return list(report.ambiguous)


def describe_reconciliation(conn: sqlite3.Connection) -> str:
    """User-facing French summary, '' when nothing has to be said.

    Something has to be said when part of the history is not attached
    (`ReconciliationReport.is_degraded`) AND when part of it was attached
    on an assumption (`ReconciliationReport.assumed`): the user must be
    able to tell the plugin it guessed wrong.
    """
    report = load_reconciliation(conn)
    if report is None or not report.needs_notice:
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
        services = [item for item in report.ambiguous
                    if item.reason == _REASON_SERVICE]
        if services:
            details = "; ".join(
                f"{', '.join(item.layer_names) or item.fingerprint[-40:]} : "
                f"{item.detail}" if item.detail
                else (", ".join(item.layer_names) or item.fingerprint[-40:])
                for item in services[:3])
            parts.append(
                f"{len(services)} d'entre elles passent par un service de "
                f"connexion que les versions precedentes n'enregistraient "
                f"pas : leur historique ancien peut venir d'une autre base "
                f"de donnees servie sous le meme nom de table ({details}). "
                f"Il peut etre rattache au service que vous designez."
            )
        n_retired = sum(1 for item in report.ambiguous
                        if item.reason.startswith("alias_v6_retire_"))
        if n_retired:
            parts.append(
                f"{n_retired} rattachement(s) pose(s) par une version "
                f"precedente ont ete retires : les evenements montrent deux "
                f"tables differentes sous la meme empreinte (couche repointee "
                f"vers une autre table), et un rewind aurait rejoue "
                f"l'historique de l'une sur les lignes de l'autre."
            )
    if report.assumed:
        details = "; ".join(
            f"{', '.join(item.layer_names) or item.fingerprint[-40:]} : "
            f"{item.detail}" for item in report.assumed[:3])
        parts.append(
            f"{len(report.assumed)} source(s) passant par un service de "
            f"connexion ont ete rattachees a leur historique d'avant v7 par "
            f"hypothese, parce qu'un seul service est connu ({details}). "
            f"Si cet historique venait d'une autre base, ne lancez pas de "
            f"rewind sur ces couches avant d'avoir retire ce rattachement."
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
    if report.is_degraded:
        parts.append(
            "Aucun evenement n'est perdu : un rewind lance sur ces couches "
            "peut ne pas couvrir la totalite de leur historique."
        )
    return " ".join(parts)
