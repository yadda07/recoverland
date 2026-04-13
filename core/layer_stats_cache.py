"""Per-layer statistics cache for instant UI feedback.

Caches min/max dates and operation types per datasource fingerprint.
Single query populates the entire cache; incremental updates possible.
No QGIS dependency. Thread-safe for read after build.
"""
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from .logger import flog

_STATS_QUERY = """
    SELECT
        datasource_fingerprint,
        MIN(created_at) AS min_date,
        MAX(created_at) AS max_date,
        GROUP_CONCAT(DISTINCT operation_type) AS op_types,
        COUNT(*) AS event_count
    FROM audit_event
    GROUP BY datasource_fingerprint
"""

_GLOBAL_STATS_QUERY = """
    SELECT
        MIN(created_at) AS min_date,
        MAX(created_at) AS max_date,
        GROUP_CONCAT(DISTINCT operation_type) AS op_types,
        COUNT(*) AS event_count
    FROM audit_event
"""


@dataclass(frozen=True)
class LayerStats:
    """Cached statistics for a single datasource."""
    fingerprint: str
    min_date: str
    max_date: str
    operation_types: Set[str]
    event_count: int


@dataclass
class LayerStatsCache:
    """In-memory cache of per-layer statistics."""
    _by_fp: Dict[str, LayerStats] = field(default_factory=dict)
    _global_min: Optional[str] = None
    _global_max: Optional[str] = None
    _global_ops: Set[str] = field(default_factory=set)
    _global_count: int = 0

    def build(self, conn: sqlite3.Connection) -> None:
        """Populate the entire cache from two lightweight queries."""
        self._by_fp.clear()
        try:
            for row in conn.execute(_STATS_QUERY).fetchall():
                fp, min_d, max_d, ops_csv, count = row
                ops = set(ops_csv.split(",")) if ops_csv else set()
                self._by_fp[fp] = LayerStats(
                    fingerprint=fp,
                    min_date=min_d or "",
                    max_date=max_d or "",
                    operation_types=ops,
                    event_count=count or 0,
                )
            g_row = conn.execute(_GLOBAL_STATS_QUERY).fetchone()
            if g_row:
                self._global_min = g_row[0] or None
                self._global_max = g_row[1] or None
                self._global_ops = set(g_row[2].split(",")) if g_row[2] else set()
                self._global_count = g_row[3] or 0
            else:
                self._global_min = None
                self._global_max = None
                self._global_ops = set()
                self._global_count = 0
            flog(f"layer_stats_cache: built for {len(self._by_fp)} layers, {self._global_count} total events")
        except Exception as e:
            flog(f"layer_stats_cache: build error: {e}", "WARNING")

    def get(self, fingerprint: str) -> Optional[LayerStats]:
        """Return cached stats for a specific layer, or None."""
        return self._by_fp.get(fingerprint)

    def global_min_date(self) -> Optional[str]:
        return self._global_min

    def global_max_date(self) -> Optional[str]:
        return self._global_max

    def global_operation_types(self) -> Set[str]:
        return self._global_ops

    def global_event_count(self) -> int:
        return self._global_count

    def all_fingerprints(self) -> list:
        return list(self._by_fp.keys())

    def is_empty(self) -> bool:
        return len(self._by_fp) == 0
