"""Tests for LayerStatsCache."""
import sys
import os
import sqlite3
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.layer_stats_cache import LayerStatsCache


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.execute("""
        CREATE TABLE audit_event (
            event_id INTEGER PRIMARY KEY,
            datasource_fingerprint TEXT,
            operation_type TEXT,
            created_at TEXT
        )
    """)
    return c


def _insert(conn, fp, op, ts):
    conn.execute(
        "INSERT INTO audit_event (datasource_fingerprint, operation_type, created_at) VALUES (?,?,?)",
        (fp, op, ts),
    )


class TestLayerStatsCacheBuild:
    def test_empty_journal(self, conn):
        cache = LayerStatsCache()
        cache.build(conn)
        assert cache.is_empty()
        assert cache.global_min_date() is None
        assert cache.global_event_count() == 0

    def test_single_layer(self, conn):
        _insert(conn, "fp1", "UPDATE", "2026-04-01T10:00:00+00:00")
        _insert(conn, "fp1", "DELETE", "2026-04-02T12:00:00+00:00")
        cache = LayerStatsCache()
        cache.build(conn)
        assert not cache.is_empty()
        assert cache.global_event_count() == 2
        stats = cache.get("fp1")
        assert stats is not None
        assert stats.min_date == "2026-04-01T10:00:00+00:00"
        assert stats.max_date == "2026-04-02T12:00:00+00:00"
        assert stats.operation_types == {"UPDATE", "DELETE"}
        assert stats.event_count == 2

    def test_multiple_layers(self, conn):
        _insert(conn, "fp1", "UPDATE", "2026-04-01T10:00:00+00:00")
        _insert(conn, "fp2", "INSERT", "2026-04-03T08:00:00+00:00")
        _insert(conn, "fp2", "DELETE", "2026-04-04T09:00:00+00:00")
        cache = LayerStatsCache()
        cache.build(conn)
        assert cache.global_event_count() == 3
        assert cache.global_min_date() == "2026-04-01T10:00:00+00:00"
        assert cache.global_max_date() == "2026-04-04T09:00:00+00:00"
        assert cache.global_operation_types() == {"UPDATE", "INSERT", "DELETE"}

        s1 = cache.get("fp1")
        assert s1.operation_types == {"UPDATE"}
        assert s1.event_count == 1

        s2 = cache.get("fp2")
        assert s2.operation_types == {"INSERT", "DELETE"}
        assert s2.event_count == 2

    def test_unknown_fingerprint_returns_none(self, conn):
        _insert(conn, "fp1", "UPDATE", "2026-04-01T10:00:00+00:00")
        cache = LayerStatsCache()
        cache.build(conn)
        assert cache.get("unknown") is None

    def test_rebuild_replaces_old_data(self, conn):
        _insert(conn, "fp1", "UPDATE", "2026-04-01T10:00:00+00:00")
        cache = LayerStatsCache()
        cache.build(conn)
        assert cache.global_event_count() == 1

        _insert(conn, "fp1", "DELETE", "2026-04-05T10:00:00+00:00")
        cache.build(conn)
        assert cache.global_event_count() == 2
        assert cache.get("fp1").operation_types == {"UPDATE", "DELETE"}

    def test_all_fingerprints(self, conn):
        _insert(conn, "fp1", "UPDATE", "2026-04-01T10:00:00+00:00")
        _insert(conn, "fp2", "INSERT", "2026-04-02T10:00:00+00:00")
        cache = LayerStatsCache()
        cache.build(conn)
        fps = sorted(cache.all_fingerprints())
        assert fps == ["fp1", "fp2"]
