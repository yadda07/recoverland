"""Regression tests for restore deduplication (anti-inflate).

Verifies:
- EditSessionTracker suppress/unsuppress prevents re-recording during restore
- Trace events are lightweight (reference-only, no data copy)
- Search/count queries exclude trace events by default
- get_journal_stats excludes trace events from totals
"""
import json
import sqlite3
import sys

from recoverland.core.audit_backend import AuditEvent
from recoverland.core.search_service import (
    count_events, summarize_scope, _build_where_clause,
)
from recoverland.core.retention import get_journal_stats
from recoverland.core.restore_service import restore_batch, undo_restore_batch
from recoverland.core.sqlite_schema import initialize_schema


def _make_event(event_id=None, op="DELETE", restored_from=None):
    return AuditEvent(
        event_id=event_id,
        project_fingerprint="proj",
        datasource_fingerprint="ogr::test",
        layer_id_snapshot="layer_1",
        layer_name_snapshot="test_layer",
        provider_type="ogr",
        feature_identity_json='{"fid": 1, "pk_field": "id", "pk_value": 1}',
        operation_type=op,
        attributes_json='{"all_attributes": {"id": 1, "name": "a"}}',
        geometry_wkb=b'\x01\x02\x03',
        geometry_type="Point",
        crs_authid="EPSG:4326",
        field_schema_json='[{"name": "id", "type": "int"}]',
        user_name="tester",
        session_id="sess1",
        created_at="2025-06-01T10:00:00Z",
        restored_from_event_id=restored_from,
        entity_fingerprint="pk:id=1",
        event_schema_version=2,
    )


def _make_db():
    conn = sqlite3.connect(":memory:")
    initialize_schema(conn)
    return conn


def _insert_event(conn, event):
    conn.execute(
        """INSERT INTO audit_event (
            project_fingerprint, datasource_fingerprint,
            layer_id_snapshot, layer_name_snapshot, provider_type,
            feature_identity_json, operation_type, attributes_json,
            geometry_wkb, geometry_type, crs_authid, field_schema_json,
            user_name, session_id, created_at, restored_from_event_id,
            entity_fingerprint, event_schema_version, new_geometry_wkb
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            event.project_fingerprint, event.datasource_fingerprint,
            event.layer_id_snapshot, event.layer_name_snapshot,
            event.provider_type, event.feature_identity_json,
            event.operation_type, event.attributes_json,
            event.geometry_wkb, event.geometry_type, event.crs_authid,
            event.field_schema_json, event.user_name, event.session_id,
            event.created_at, event.restored_from_event_id,
            event.entity_fingerprint, event.event_schema_version,
            event.new_geometry_wkb,
        ),
    )
    conn.commit()


# ---- EditSessionTracker suppress/unsuppress ----

class TestTrackerSuppression:
    def test_suppress_flag_exists(self):
        from recoverland.core.edit_tracker import EditSessionTracker
        t = EditSessionTracker(write_queue=None, journal_manager=None)
        assert t.is_suppressed is False
        t.suppress()
        assert t.is_suppressed is True
        t.unsuppress()
        assert t.is_suppressed is False

    def test_suppressed_tracker_ignores_signals(self):
        from recoverland.core.edit_tracker import EditSessionTracker
        t = EditSessionTracker(write_queue=None, journal_manager=None)
        t.activate()
        t.suppress()
        t._on_editing_started("layer_x")
        assert "layer_x" not in t._buffers

    def test_active_unsuppressed_creates_buffer(self):
        from recoverland.core.edit_tracker import EditSessionTracker
        t = EditSessionTracker(write_queue=None, journal_manager=None)
        t.activate()
        t._layer_fingerprints["layer_x"] = "fp"
        t._connected_layers["layer_x"] = object()
        t._on_editing_started("layer_x")
        assert "layer_x" in t._buffers


# ---- Trace events are lightweight ----

class TestTraceEventLightweight:
    def test_trace_has_ref_json_not_data(self):
        from recoverland.core.restore_service import build_restore_trace_event

        source = _make_event(event_id=42, op="DELETE")

        class _Layer:
            def id(self):
                return "l1"

            def name(self):
                return "test"

            def dataProvider(self):
                class _P:
                    def name(self):
                        return "ogr"
                return _P()

            def fields(self):
                class _F:
                    def __iter__(self):
                        return iter([])
                return _F()

            def source(self):
                return "/tmp/test.gpkg"

            def primaryKeyAttributes(self):
                return []

        trace = build_restore_trace_event(source, _Layer())
        assert trace is not None
        assert trace.restored_from_event_id == 42
        parsed = json.loads(trace.attributes_json)
        assert "_restore_ref" in parsed
        assert parsed["_restore_ref"] == 42
        assert trace.geometry_wkb is None
        assert trace.field_schema_json is None


# ---- Search excludes traces by default ----

class TestSearchExcludesTraces:
    def test_where_clause_excludes_traces(self):
        from recoverland.core.audit_backend import SearchCriteria
        criteria = SearchCriteria(
            datasource_fingerprint=None, layer_name=None,
            operation_type=None, user_name=None,
            start_date=None, end_date=None, page=1, page_size=100,
        )
        clause, params = _build_where_clause(criteria)
        assert "restored_from_event_id IS NULL" in clause

    def test_where_clause_includes_traces_when_asked(self):
        from recoverland.core.audit_backend import SearchCriteria
        criteria = SearchCriteria(
            datasource_fingerprint=None, layer_name=None,
            operation_type=None, user_name=None,
            start_date=None, end_date=None, page=1, page_size=100,
        )
        clause, params = _build_where_clause(criteria, include_traces=True)
        assert "restored_from_event_id IS NULL" not in clause

    def test_count_excludes_traces(self):
        from recoverland.core.audit_backend import SearchCriteria
        conn = _make_db()
        _insert_event(conn, _make_event(op="DELETE"))
        _insert_event(conn, _make_event(op="DELETE"))
        _insert_event(conn, _make_event(op="INSERT", restored_from=1))
        criteria = SearchCriteria(
            datasource_fingerprint=None, layer_name=None,
            operation_type=None, user_name=None,
            start_date=None, end_date=None, page=1, page_size=100,
        )
        total = count_events(conn, criteria)
        assert total == 2

    def test_summarize_excludes_traces(self):
        from recoverland.core.audit_backend import SearchCriteria
        conn = _make_db()
        _insert_event(conn, _make_event(op="DELETE"))
        _insert_event(conn, _make_event(op="UPDATE"))
        _insert_event(conn, _make_event(op="INSERT", restored_from=1))
        criteria = SearchCriteria(
            datasource_fingerprint=None, layer_name=None,
            operation_type=None, user_name=None,
            start_date=None, end_date=None, page=1, page_size=100,
        )
        summary = summarize_scope(conn, criteria)
        assert summary.total_count == 2
        assert summary.delete_count == 1
        assert summary.update_count == 1
        assert summary.insert_count == 0


# ---- get_journal_stats excludes traces ----

class TestJournalStatsExcludesTraces:
    def test_stats_total_excludes_traces(self):
        conn = _make_db()
        _insert_event(conn, _make_event(op="DELETE"))
        _insert_event(conn, _make_event(op="UPDATE"))
        _insert_event(conn, _make_event(op="INSERT", restored_from=1))
        stats = get_journal_stats(conn)
        assert stats["total_events"] == 2
        assert stats["trace_events"] == 1


def _ensure_qobject_stub():
    qtcore = sys.modules["qgis.PyQt.QtCore"]
    if hasattr(qtcore, "QObject"):
        return

    class _QObject:
        def __init__(self, parent=None):
            self._parent = parent

    qtcore.QObject = _QObject


class _EditableLayer:
    def isEditable(self):
        return True

    def isModified(self):
        return True

    def dataProvider(self):
        raise AssertionError("provider access must not happen on editable layer")


class _EditableCleanLayer:
    """Editable layer with no unsaved changes."""

    def __init__(self):
        self.committed = False

    def isEditable(self):
        return not self.committed

    def isModified(self):
        return False

    def commitChanges(self):
        self.committed = True

    def dataProvider(self):
        class _Stub:
            def pkAttributeIndexes(self):
                return []

            def getFeatures(self, _req=None):
                return iter([])

            def capabilities(self):
                return 0
        return _Stub()

    def fields(self):
        class _Fields:
            def count(self):
                return 0

            def indexOf(self, _name):
                return -1
        return _Fields()


class TestEditableLayerRestoreGuard:
    def test_restore_batch_refuses_editable_layer(self):
        report = restore_batch(_EditableLayer(), [_make_event(event_id=11, op="DELETE")])
        assert report.succeeded == []
        assert report.failed[11] == "Target layer has uncommitted edits; commit or rollback before restore"
        assert report.trace_events == ()

    def test_undo_restore_batch_refuses_editable_layer(self):
        report = undo_restore_batch(_EditableLayer(), [_make_event(event_id=12, op="INSERT")])
        assert report.succeeded == []
        assert report.failed[12] == "Target layer has uncommitted edits; commit or rollback before restore"
        assert report.trace_events == ()

    def test_restore_runner_refuses_editable_modified_layer(self):
        _ensure_qobject_stub()
        from recoverland.restore_runner import RestoreRunner

        runner = RestoreRunner(
            [_make_event(event_id=13, op="DELETE")],
            lambda _event: _EditableLayer(),
            write_queue=None,
            tracker=None,
        )
        runner._advance_group()
        assert runner._processed == 1
        assert runner._total_ok == 0
        assert runner._total_fail == 1
        assert runner._errors == [
            "Evt 13: Target layer has uncommitted edits; commit or rollback before restore"
        ]

    def test_restore_runner_auto_closes_clean_edit_session(self):
        _ensure_qobject_stub()
        from recoverland.restore_runner import RestoreRunner

        layer = _EditableCleanLayer()
        runner = RestoreRunner(
            [_make_event(event_id=14, op="DELETE")],
            lambda _event: layer,
            write_queue=None,
            tracker=None,
        )
        runner._advance_group()
        assert layer.committed is True


class TestTraceQueueFailureReporting:
    def test_restore_runner_records_trace_enqueue_failure(self):
        _ensure_qobject_stub()
        from recoverland.restore_runner import RestoreRunner

        class _Queue:
            def __init__(self):
                self.calls = []

            def enqueue(self, events):
                self.calls.append(events)
                return False

        queue = _Queue()
        runner = RestoreRunner([], lambda _event: None, write_queue=queue, tracker=None)
        runner._traces = [_make_event(event_id=21, op="INSERT", restored_from=1)]
        runner._finish()
        assert len(queue.calls) == 1
        assert len(queue.calls[0]) == 1
        assert any("Journal trace write failed; restore data changes succeeded" in err
                   for err in runner._errors)
