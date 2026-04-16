"""Unit tests for core/rewind_dedup.py - event deduplication for temporal restore."""
from recoverland.core.audit_backend import AuditEvent
from recoverland.core.rewind_dedup import collapse_rewind_events


def _evt(event_id, op, entity_fp="pk:id=1", ds_fp="ogr::test"):
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="proj",
        datasource_fingerprint=ds_fp,
        layer_id_snapshot="l1",
        layer_name_snapshot="layer",
        provider_type="ogr",
        feature_identity_json='{"pk_field":"id","pk_value":1}',
        operation_type=op,
        attributes_json='{"all_attributes":{"id":1,"name":"a"}}',
        geometry_wkb=None,
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json='[{"name":"id","type":"int"}]',
        user_name="u",
        session_id="s",
        created_at="2025-06-01T10:00:00Z",
        restored_from_event_id=None,
        entity_fingerprint=entity_fp,
        event_schema_version=2,
    )


class TestEmptyAndSingle:
    def test_empty_returns_empty(self):
        assert collapse_rewind_events([]) == []

    def test_single_event_unchanged(self):
        e = _evt(1, "UPDATE")
        result = collapse_rewind_events([e])
        assert result == [e]


class TestMultipleUpdates:
    def test_many_updates_collapse_to_oldest(self):
        # DESC order: newest first
        events = [_evt(500, "UPDATE"), _evt(300, "UPDATE"), _evt(100, "UPDATE")]
        result = collapse_rewind_events(events)
        assert len(result) == 1
        assert result[0].event_id == 100

    def test_500_updates_single_entity(self):
        events = [_evt(i, "UPDATE") for i in range(500, 0, -1)]
        result = collapse_rewind_events(events)
        assert len(result) == 1
        assert result[0].event_id == 1


class TestInsertSequences:
    def test_insert_then_updates_keeps_insert(self):
        events = [_evt(50, "UPDATE"), _evt(20, "UPDATE"), _evt(10, "INSERT")]
        result = collapse_rewind_events(events)
        assert len(result) == 1
        assert result[0].event_id == 10
        assert result[0].operation_type == "INSERT"

    def test_insert_then_delete_skipped(self):
        events = [_evt(30, "DELETE"), _evt(10, "INSERT")]
        result = collapse_rewind_events(events)
        assert len(result) == 0

    def test_insert_update_delete_skipped(self):
        events = [_evt(30, "DELETE"), _evt(20, "UPDATE"), _evt(10, "INSERT")]
        result = collapse_rewind_events(events)
        assert len(result) == 0


class TestDeleteSequences:
    def test_single_delete_kept(self):
        events = [_evt(10, "DELETE")]
        result = collapse_rewind_events(events)
        assert len(result) == 1
        assert result[0].event_id == 10

    def test_delete_then_insert_keeps_delete(self):
        events = [_evt(20, "INSERT"), _evt(10, "DELETE")]
        result = collapse_rewind_events(events)
        assert len(result) == 1
        assert result[0].event_id == 10


class TestUpdateThenDelete:
    def test_updates_then_delete_keeps_both(self):
        events = [_evt(50, "DELETE"), _evt(30, "UPDATE"), _evt(10, "UPDATE")]
        result = collapse_rewind_events(events)
        assert len(result) == 2
        assert result[0].event_id == 50
        assert result[0].operation_type == "DELETE"
        assert result[1].event_id == 10
        assert result[1].operation_type == "UPDATE"


class TestMultipleEntities:
    def test_two_entities_independent(self):
        events = [
            _evt(100, "UPDATE", entity_fp="pk:id=1"),
            _evt(90, "UPDATE", entity_fp="pk:id=2"),
            _evt(50, "UPDATE", entity_fp="pk:id=1"),
            _evt(40, "UPDATE", entity_fp="pk:id=2"),
        ]
        result = collapse_rewind_events(events)
        assert len(result) == 2
        fps = {e.entity_fingerprint for e in result}
        assert fps == {"pk:id=1", "pk:id=2"}
        for e in result:
            if e.entity_fingerprint == "pk:id=1":
                assert e.event_id == 50
            else:
                assert e.event_id == 40

    def test_mixed_ops_multiple_entities(self):
        events = [
            _evt(100, "UPDATE", entity_fp="pk:id=1"),
            _evt(90, "DELETE", entity_fp="pk:id=2"),
            _evt(80, "UPDATE", entity_fp="pk:id=1"),
            _evt(70, "INSERT", entity_fp="pk:id=2"),
        ]
        result = collapse_rewind_events(events)
        # entity 1: UPDATE->UPDATE -> oldest UPDATE (80)
        # entity 2: INSERT->DELETE -> SKIP
        assert len(result) == 1
        assert result[0].event_id == 80


class TestDifferentDatasources:
    def test_same_entity_fp_different_ds_not_merged(self):
        events = [
            _evt(10, "UPDATE", entity_fp="pk:id=1", ds_fp="ds_A"),
            _evt(5, "UPDATE", entity_fp="pk:id=1", ds_fp="ds_B"),
        ]
        result = collapse_rewind_events(events)
        assert len(result) == 2


class TestReductionRatio:
    def test_8000_events_10_entities(self):
        events = []
        for entity_id in range(10):
            fp = f"pk:id={entity_id}"
            for eid in range(entity_id * 800 + 800, entity_id * 800, -1):
                events.append(_evt(eid, "UPDATE", entity_fp=fp))
        assert len(events) == 8000
        result = collapse_rewind_events(events)
        assert len(result) == 10
