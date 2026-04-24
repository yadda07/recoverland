"""Datasource alias management (BLK-07).

A datasource_alias links a new fingerprint to an existing one, so that
events captured under the old fingerprint are still visible / restorable
when a layer moves (path change, provider switch, renamed DB, etc.).

Public API:
- add_alias(conn, alias_fp, target_fp, note) -> bool
- remove_alias(conn, alias_fp) -> int
- list_aliases(conn) -> list[tuple]
- resolve_fingerprints(conn, fp) -> list[str]
  Returns [fp] plus every fingerprint that resolves to it (direct + transitive),
  capped to prevent pathological chains.

Design notes:
- Transitive resolution is bounded to avoid cycles (CHECK constraint already
  prevents self-alias, but a chain could still loop via multiple rows).
- resolve_fingerprints is read-only and safe for any thread.
"""
from datetime import datetime, timezone
from typing import List, Tuple
import sqlite3

from .logger import flog


_MAX_CHAIN_DEPTH = 8


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
