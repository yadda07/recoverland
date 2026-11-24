# Youcef ADDA | GEODECI
# FR : Plugins QGIS, solutions SIG et administration PostgreSQL/PostGIS ; audit, automatisation et accompagnement.
# EN : QGIS plugins, GIS solutions and PostgreSQL/PostGIS administration; audits, automation and business support.
# AR : تطوير إضافات QGIS وحلول نظم المعلومات الجغرافية وإدارة PostgreSQL/PostGIS، والتدقيق والأتمتة ومرافقة المؤسسات.
# Site / Website / الموقع : https://geodeci.xyz/
# Contact professionnel / Business enquiries / للتواصل المهني : youcef.geodesien@gmail.com | contact@geodeci.xyz
# Détails / Details / التفاصيل : SIGNATURE.md
# ============================================================================
"""Per-layer statistics cache for instant UI feedback.

Caches min/max dates and operation types per datasource fingerprint.
Single query populates the entire cache; incremental updates possible.
No QGIS dependency. Thread-safe for read after build.
"""
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

from .datasource_alias import canonical_target_map
from .logger import flog

_STATS_QUERY = """
    SELECT
        datasource_fingerprint,
        MIN(created_at) AS min_date,
        MAX(created_at) AS max_date,
        GROUP_CONCAT(DISTINCT operation_type) AS op_types,
        COUNT(*) AS event_count
    FROM audit_event
    WHERE restored_from_event_id IS NULL
    GROUP BY datasource_fingerprint
"""

_GLOBAL_STATS_QUERY = """
    SELECT
        MIN(created_at) AS min_date,
        MAX(created_at) AS max_date,
        GROUP_CONCAT(DISTINCT operation_type) AS op_types,
        COUNT(*) AS event_count
    FROM audit_event
    WHERE restored_from_event_id IS NULL
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
    _aliases: Dict[str, str] = field(default_factory=dict)
    _global_min: Optional[str] = None
    _global_max: Optional[str] = None
    _global_ops: Set[str] = field(default_factory=set)
    _global_count: int = 0

    def build(self, conn: sqlite3.Connection) -> None:
        """Populate the entire cache from two lightweight queries."""
        self.invalidate()
        try:
            aliases = canonical_target_map(conn)
            self._aliases = dict(aliases)
            merged: Dict[str, list] = {}
            for row in conn.execute(_STATS_QUERY).fetchall():
                raw_fp, min_d, max_d, ops_csv, count = row
                fp = aliases.get(raw_fp, raw_fp)
                ops = set(ops_csv.split(",")) if ops_csv else set()
                cur = merged.get(fp)
                if cur is None:
                    merged[fp] = [min_d or "", max_d or "", ops, count or 0]
                else:
                    if min_d and (not cur[0] or min_d < cur[0]):
                        cur[0] = min_d
                    if max_d and (not cur[1] or max_d > cur[1]):
                        cur[1] = max_d
                    cur[2].update(ops)
                    cur[3] += count or 0
            for fp, (min_d, max_d, ops, count) in merged.items():
                self._by_fp[fp] = LayerStats(
                    fingerprint=fp,
                    min_date=min_d,
                    max_date=max_d,
                    operation_types=ops,
                    event_count=count,
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
        stats = self._by_fp.get(fingerprint)
        if stats is not None:
            return stats
        target = self._aliases.get(fingerprint)
        return self._by_fp.get(target) if target else None

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

    def invalidate(self) -> None:
        """Clear all cached data so next build() refreshes from DB (RW-20)."""
        self._by_fp.clear()
        self._aliases.clear()
        self._global_min = None
        self._global_max = None
        self._global_ops = set()
        self._global_count = 0
