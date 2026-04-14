"""Brutal tests for event_stream_repository.py (BL-03).

Real SQLite in-memory DB, every query path, every edge case.
"""
import sqlite3

from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.restore_contracts import (
    CutoffType, RestoreCutoff, MAX_EVENTS_PER_RESTORE,
)
from recoverland.core.event_stream_repository import (
    fetch_entity_stream, fetch_events_after_cutoff,
    count_events_after_cutoff, fetch_events_by_ids,
    get_oldest_event_date,
)


def _db():
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _insert(conn, event_id_hint=None, ds_fp="ogr::test", entity_fp="pk:id=1",
            op="INSERT", created_at="2025-01-15T10:00:00Z"):
    conn.execute(
        "INSERT INTO audit_event "
        "(project_fingerprint, datasource_fingerprint, provider_type, "
        "feature_identity_json, operation_type, attributes_json, "
        "user_name, created_at, entity_fingerprint, event_schema_version) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("proj", ds_fp, "ogr",
         '{"fid": 1, "pk_field": "id", "pk_value": 1}',
         op, '{}', "user", created_at, entity_fp, 2),
    )
    conn.commit()


# ---- fetch_entity_stream ----

class TestFetchEntityStream:
    def test_empty_db(self):
        assert fetch_entity_stream(_db(), "ogr::test", "pk:id=1") == []

    def test_no_match_datasource(self):
        conn = _db()
        _insert(conn, ds_fp="ogr::other")
        assert fetch_entity_stream(conn, "ogr::test", "pk:id=1") == []

    def test_no_match_entity(self):
        conn = _db()
        _insert(conn, entity_fp="pk:id=99")
        assert fetch_entity_stream(conn, "ogr::test", "pk:id=1") == []

    def test_single_match(self):
        conn = _db()
        _insert(conn)
        events = fetch_entity_stream(conn, "ogr::test", "pk:id=1")
        assert len(events) == 1
        assert events[0].entity_fingerprint == "pk:id=1"

    def test_multiple_events_same_entity_ordered_asc(self):
        conn = _db()
        _insert(conn, created_at="2025-01-01T00:00:00Z")
        _insert(conn, created_at="2025-01-02T00:00:00Z")
        _insert(conn, created_at="2025-01-03T00:00:00Z")
        events = fetch_entity_stream(conn, "ogr::test", "pk:id=1")
        assert len(events) == 3
        ids = [e.event_id for e in events]
        assert ids == sorted(ids)

    def test_respects_limit(self):
        conn = _db()
        for i in range(10):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        events = fetch_entity_stream(conn, "ogr::test", "pk:id=1", limit=3)
        assert len(events) == 3

    def test_limit_zero_returns_empty(self):
        conn = _db()
        _insert(conn)
        assert fetch_entity_stream(conn, "ogr::test", "pk:id=1", limit=0) == []

    def test_does_not_mix_entities(self):
        conn = _db()
        _insert(conn, entity_fp="pk:id=1")
        _insert(conn, entity_fp="pk:id=2")
        events = fetch_entity_stream(conn, "ogr::test", "pk:id=1")
        assert len(events) == 1
        assert events[0].entity_fingerprint == "pk:id=1"


# ---- fetch_events_after_cutoff ----

class TestFetchEventsAfterCutoff:
    def test_empty_db(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        assert fetch_events_after_cutoff(_db(), "ogr::test", cutoff) == []

    def test_by_event_id_inclusive(self):
        conn = _db()
        for i in range(5):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 3, True)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff)
        assert len(events) == 3  # events 3, 4, 5
        assert all(e.event_id >= 3 for e in events)

    def test_by_event_id_exclusive(self):
        conn = _db()
        for i in range(5):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 3, False)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff)
        assert len(events) == 2  # events 4, 5
        assert all(e.event_id > 3 for e in events)

    def test_by_date_inclusive(self):
        conn = _db()
        _insert(conn, created_at="2025-01-01T00:00:00Z")
        _insert(conn, created_at="2025-01-02T00:00:00Z")
        _insert(conn, created_at="2025-01-03T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-01-02T00:00:00Z", True)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff)
        assert len(events) == 2

    def test_by_date_exclusive(self):
        conn = _db()
        _insert(conn, created_at="2025-01-01T00:00:00Z")
        _insert(conn, created_at="2025-01-02T00:00:00Z")
        _insert(conn, created_at="2025-01-03T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-01-02T00:00:00Z", False)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff)
        assert len(events) == 1

    def test_ordered_desc(self):
        conn = _db()
        for i in range(5):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff)
        ids = [e.event_id for e in events]
        assert ids == sorted(ids, reverse=True)

    def test_respects_limit(self):
        conn = _db()
        for i in range(10):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff, limit=3)
        assert len(events) == 3

    def test_no_match_datasource(self):
        conn = _db()
        _insert(conn, ds_fp="ogr::other")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        assert fetch_events_after_cutoff(conn, "ogr::test", cutoff) == []

    def test_cutoff_future_event_id_returns_empty(self):
        conn = _db()
        _insert(conn)
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 999999, True)
        assert fetch_events_after_cutoff(conn, "ogr::test", cutoff) == []

    def test_cutoff_future_date_returns_empty(self):
        conn = _db()
        _insert(conn, created_at="2025-01-01T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2099-01-01T00:00:00Z", True)
        assert fetch_events_after_cutoff(conn, "ogr::test", cutoff) == []


# ---- count_events_after_cutoff ----

class TestCountEventsAfterCutoff:
    def test_empty_db(self):
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        assert count_events_after_cutoff(_db(), "ogr::test", cutoff) == 0

    def test_counts_match_fetch(self):
        conn = _db()
        for i in range(7):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 3, True)
        count = count_events_after_cutoff(conn, "ogr::test", cutoff)
        events = fetch_events_after_cutoff(conn, "ogr::test", cutoff)
        assert count == len(events)

    def test_by_date_count(self):
        conn = _db()
        _insert(conn, created_at="2025-01-01T00:00:00Z")
        _insert(conn, created_at="2025-06-01T00:00:00Z")
        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-03-01T00:00:00Z", True)
        assert count_events_after_cutoff(conn, "ogr::test", cutoff) == 1

    def test_no_match_returns_zero(self):
        conn = _db()
        _insert(conn, ds_fp="ogr::other")
        cutoff = RestoreCutoff(CutoffType.BY_EVENT_ID, 1, True)
        assert count_events_after_cutoff(conn, "ogr::test", cutoff) == 0


# ---- fetch_events_by_ids ----

class TestFetchEventsByIds:
    def test_empty_list(self):
        assert fetch_events_by_ids(_db(), []) == []

    def test_single_id(self):
        conn = _db()
        _insert(conn)
        events = fetch_events_by_ids(conn, [1])
        assert len(events) == 1
        assert events[0].event_id == 1

    def test_multiple_ids(self):
        conn = _db()
        for i in range(5):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        events = fetch_events_by_ids(conn, [2, 4])
        assert len(events) == 2
        ids = {e.event_id for e in events}
        assert ids == {2, 4}

    def test_nonexistent_id_ignored(self):
        conn = _db()
        _insert(conn)
        events = fetch_events_by_ids(conn, [999])
        assert len(events) == 0

    def test_mix_existing_and_nonexistent(self):
        conn = _db()
        _insert(conn)
        events = fetch_events_by_ids(conn, [1, 999])
        assert len(events) == 1

    def test_ordered_desc(self):
        conn = _db()
        for i in range(5):
            _insert(conn, created_at=f"2025-01-{i+1:02d}T00:00:00Z")
        events = fetch_events_by_ids(conn, [1, 2, 3, 4, 5])
        ids = [e.event_id for e in events]
        assert ids == sorted(ids, reverse=True)

    def test_duplicate_ids_not_duplicated(self):
        conn = _db()
        _insert(conn)
        events = fetch_events_by_ids(conn, [1, 1, 1])
        assert len(events) == 1


# ---- get_oldest_event_date ----

class TestGetOldestEventDate:
    def test_empty_db(self):
        assert get_oldest_event_date(_db(), "ogr::test") is None

    def test_no_match_datasource(self):
        conn = _db()
        _insert(conn, ds_fp="ogr::other", created_at="2025-01-01T00:00:00Z")
        assert get_oldest_event_date(conn, "ogr::test") is None

    def test_single_event(self):
        conn = _db()
        _insert(conn, created_at="2025-06-15T12:00:00Z")
        assert get_oldest_event_date(conn, "ogr::test") == "2025-06-15T12:00:00Z"

    def test_returns_min_date(self):
        conn = _db()
        _insert(conn, created_at="2025-03-01T00:00:00Z")
        _insert(conn, created_at="2025-01-01T00:00:00Z")
        _insert(conn, created_at="2025-06-01T00:00:00Z")
        assert get_oldest_event_date(conn, "ogr::test") == "2025-01-01T00:00:00Z"

    def test_ignores_other_datasources(self):
        conn = _db()
        _insert(conn, ds_fp="ogr::test", created_at="2025-06-01T00:00:00Z")
        _insert(conn, ds_fp="ogr::other", created_at="2025-01-01T00:00:00Z")
        assert get_oldest_event_date(conn, "ogr::test") == "2025-06-01T00:00:00Z"


# ---- Data integrity ----

class TestDataIntegrity:
    def test_new_columns_populated_correctly(self):
        conn = _db()
        _insert(conn, entity_fp="pk:gid=42")
        events = fetch_events_by_ids(conn, [1])
        assert len(events) == 1
        assert events[0].entity_fingerprint == "pk:gid=42"
        assert events[0].event_schema_version == 2

    def test_null_new_columns_handled(self):
        conn = _db()
        conn.execute(
            "INSERT INTO audit_event "
            "(project_fingerprint, datasource_fingerprint, provider_type, "
            "operation_type, attributes_json, user_name, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("proj", "ogr::test", "ogr", "INSERT", "{}", "user", "2025-01-01"),
        )
        conn.commit()
        events = fetch_events_by_ids(conn, [1])
        assert len(events) == 1
        assert events[0].entity_fingerprint is None
        assert events[0].event_schema_version is None
        assert events[0].new_geometry_wkb is None
