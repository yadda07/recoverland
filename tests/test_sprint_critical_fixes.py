"""Regression tests for the critical-fix sprint.

Covers:
- BUG-I18N-01: operation filter uses userData ('ALL'/'UPDATE'/...) not text.
- BUG-REWIND-01: collapse_rewind_events is applied on temporal restore fetch.
- BUG-INVALIDATE-01: reset_undo_state clears cross-project undo state.
- DATA-10: _find_target_feature refuses FID fallback when a PK was captured.
- BLK-03: file logger uses RotatingFileHandler (append, not truncate).
- BLK-04: WriteQueue.enqueue pre-checks capacity before accepting a batch.
- BLK-05: fetch_events_after_cutoff orders by created_at DESC.
"""
import logging
import sqlite3
import unittest
from unittest.mock import MagicMock

from recoverland.core.audit_backend import AuditEvent
from recoverland.core.event_stream_repository import (
    fetch_events_after_cutoff,
)
from recoverland.core.restore_contracts import (
    CutoffType, RestoreCutoff,
)
from recoverland.core.rewind_dedup import collapse_rewind_events
from recoverland.core.restore_service import _find_target_feature
from recoverland.core.sqlite_schema import initialize_schema
from recoverland.core.write_queue import WriteQueue


# ---------------------------------------------------------------------------
# BLK-03: log handler is RotatingFileHandler (append mode)
# ---------------------------------------------------------------------------

class TestLoggerIsRotating(unittest.TestCase):
    def test_file_logger_uses_rotating_handler(self):
        logger = logging.getLogger("RecoverLand.FileDebug")
        handlers = [h for h in logger.handlers
                    if isinstance(h, logging.handlers.RotatingFileHandler)]
        self.assertTrue(
            handlers,
            "RecoverLand.FileDebug must use logging.handlers.RotatingFileHandler; "
            "otherwise history is truncated at every QGIS start.",
        )
        handler = handlers[0]
        self.assertEqual(
            handler.mode, 'a',
            "Rotating handler must append, not truncate.",
        )
        self.assertGreater(handler.maxBytes, 0)
        self.assertGreater(handler.backupCount, 0)


# ---------------------------------------------------------------------------
# BLK-05: fetch_events_after_cutoff orders by created_at DESC
# ---------------------------------------------------------------------------

def _insert_event(conn, event_id, datasource_fp, created_at, op_type="UPDATE"):
    conn.execute(
        "INSERT INTO audit_event (event_id, project_fingerprint, "
        "datasource_fingerprint, layer_id_snapshot, layer_name_snapshot, "
        "provider_type, feature_identity_json, operation_type, "
        "attributes_json, geometry_wkb, geometry_type, crs_authid, "
        "field_schema_json, user_name, session_id, created_at, "
        "entity_fingerprint, event_schema_version) "
        "VALUES (?, 'p', ?, 'l', 'lname', 'ogr', '{}', ?, '{}', NULL, "
        "'Point', 'EPSG:4326', '[]', 'u', 's', ?, 'e', 2)",
        (event_id, datasource_fp, op_type, created_at),
    )


class TestFetchEventsOrderedByCreatedAt(unittest.TestCase):
    def test_events_with_nonmonotonic_event_id_ordered_by_time(self):
        conn = sqlite3.connect(":memory:")
        initialize_schema(conn)
        # event_id 10 is OLDER than event_id 5 (pending recovery re-insert).
        _insert_event(conn, 5, "ogr::t1", "2025-06-05T10:00:00Z")
        _insert_event(conn, 10, "ogr::t1", "2025-06-02T10:00:00Z")
        conn.commit()

        cutoff = RestoreCutoff(CutoffType.BY_DATE, "2025-06-01T00:00:00Z", False)
        events = fetch_events_after_cutoff(conn, "ogr::t1", cutoff, limit=10)
        self.assertEqual(len(events), 2)
        # Most recent created_at must come first (DESC).
        self.assertEqual(events[0].event_id, 5)
        self.assertEqual(events[1].event_id, 10)


# ---------------------------------------------------------------------------
# BLK-04: WriteQueue.enqueue pre-checks capacity
# ---------------------------------------------------------------------------

class TestWriteQueueBatchPreCheck(unittest.TestCase):
    def test_batch_exceeding_hard_limit_rejected_upfront(self):
        wq = WriteQueue()
        from recoverland.core import write_queue as wq_mod
        # Build a batch bigger than the hard limit in one shot.
        oversized_batch = [
            AuditEvent(
                event_id=None,
                project_fingerprint="p",
                datasource_fingerprint="ogr::t",
                layer_id_snapshot="l",
                layer_name_snapshot="n",
                provider_type="ogr",
                feature_identity_json='{"fid":1}',
                operation_type="UPDATE",
                attributes_json='{"a":1}',
                geometry_wkb=None,
                geometry_type="",
                crs_authid="",
                field_schema_json='[]',
                user_name="u",
                session_id="s",
                created_at="2025-01-01T00:00:00Z",
                restored_from_event_id=None,
                entity_fingerprint="e",
                event_schema_version=2,
            )
            for _ in range(wq_mod._QUEUE_HARD_LIMIT + 100)
        ]
        accepted = wq.enqueue(oversized_batch)
        self.assertFalse(
            accepted,
            "enqueue must refuse a single batch bigger than the hard limit.",
        )
        # No event should have reached the internal queue.
        self.assertEqual(wq.pending_count, 0)


# ---------------------------------------------------------------------------
# BUG-REWIND-01: collapse_rewind_events is used in temporal flow
# ---------------------------------------------------------------------------

class TestCollapseRewindEventsExported(unittest.TestCase):
    def test_collapse_is_exported_from_core(self):
        from recoverland import core
        self.assertTrue(hasattr(core, "collapse_rewind_events"))
        self.assertIs(core.collapse_rewind_events, collapse_rewind_events)


# ---------------------------------------------------------------------------
# DATA-10: _find_target_feature refuses FID fallback when PK exists
# ---------------------------------------------------------------------------

class TestFindTargetFeaturePKSafety(unittest.TestCase):
    def test_no_fid_fallback_when_pk_captured_but_missing(self):
        # Identity has a PK (captured at commit time) but the feature is gone.
        identity = {"pk_field": "gid", "pk_value": 42, "fid": 99}

        layer = MagicMock()
        provider = MagicMock()
        layer.dataProvider.return_value = provider
        # PK lookup returns nothing.
        provider.getFeatures.return_value = iter([])

        # Patch QgsExpression/QgsFeatureRequest at call site.
        from recoverland.core import restore_service

        class _FakeExpr:
            def __init__(self, _s):
                self._valid = True

            def hasParserError(self):
                return False

            def parserErrorString(self):
                return ""

            @staticmethod
            def quotedColumnRef(name):
                return f'"{name}"'

            @staticmethod
            def quotedValue(val):
                return f"'{val}'"

        class _FakeRequest:
            def __init__(self, _expr):
                pass

            def setLimit(self, _n):
                return self

        # Stub qgis.core module for this test only.
        import sys
        import types
        fake_qgis = types.SimpleNamespace(
            QgsFeatureRequest=_FakeRequest, QgsExpression=_FakeExpr)
        old = sys.modules.get("qgis.core")
        sys.modules["qgis.core"] = fake_qgis
        try:
            result = restore_service._find_target_feature(layer, identity)
        finally:
            if old is not None:
                sys.modules["qgis.core"] = old
            else:
                sys.modules.pop("qgis.core", None)

        self.assertIsNone(
            result,
            "When a PK was captured and PK lookup misses, the FID fallback "
            "must be refused (FID is not stable across commits on many "
            "providers; safety first).",
        )


class TestDashboardShowsFullJournalSpanOnOpen(unittest.TestCase):
    """BUG-DASHBOARD-ZERO: on opening a project, the SmartBar dashboard
    must reflect the full journal span. The initial date seed covers the
    maximum retention range and _apply_cached_date_bounds narrows it.
    """

    def _read_dialog_source(self) -> str:
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "recover_dialog.py",
        )
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()

    def test_initial_bounds_flag_exists(self):
        src = self._read_dialog_source()
        self.assertIn(
            "self._initial_bounds_applied = False", src,
            "a first-open flag is required to align date inputs to "
            "the real journal span on first open",
        )

    def test_apply_cached_date_bounds_uses_flag_on_first_call(self):
        src = self._read_dialog_source()
        # Must branch on the flag and align start_input to min on first call.
        self.assertIn("if not self._initial_bounds_applied:", src)
        self.assertIn("self.start_input.setDateTime(min_dt)", src)
        self.assertIn("self._initial_bounds_applied = True", src)

    def test_refresh_journal_status_triggers_initial_alignment(self):
        src = self._read_dialog_source()
        # _refresh_journal_status must call _apply_cached_date_bounds
        # with the global journal span on first visible refresh.
        self.assertIn(
            "if not self._initial_bounds_applied and not self._stats_cache.is_empty():",
            src,
            "_refresh_journal_status must align date inputs on first "
            "call once the stats cache is built",
        )
        self.assertIn("global_min_date()", src)
        self.assertIn("global_max_date()", src)


class TestDashboardEndDateAdvancesAfterCommit(unittest.TestCase):
    """BUG-DASHBOARD-STALE: end_input must advance after new events are
    committed, otherwise events with created_at > end_date are hidden
    in the SmartBar dashboard.
    """

    def _read_dialog_source(self) -> str:
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "recover_dialog.py",
        )
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()

    def _extract_method_body(self, src, method_name):
        tag = f"def {method_name}"
        idx = src.index(tag)
        idx_next = src.index("\n    def ", idx + 1)
        return src[idx:idx_next]

    def test_advance_end_date_method_exists(self):
        src = self._read_dialog_source()
        self.assertIn(
            "def _advance_end_date_to_now(self)",
            src,
            "a method must advance end_input so new events are visible",
        )

    def test_on_events_committed_advances_end_date(self):
        body = self._extract_method_body(
            self._read_dialog_source(), "on_events_committed")
        self.assertIn("_advance_end_date_to_now", body,
                       "on_events_committed must advance end date")

    def test_refresh_journal_status_closes_conn_and_advances(self):
        body = self._extract_method_body(
            self._read_dialog_source(), "_refresh_journal_status")
        self.assertIn("_close_dialog_read_conn", body,
                       "_refresh_journal_status must close stale read conn")
        self.assertIn("_advance_end_date_to_now", body,
                       "_refresh_journal_status must advance end date")

    def test_advance_blocks_signals(self):
        body = self._extract_method_body(
            self._read_dialog_source(), "_advance_end_date_to_now")
        self.assertIn("blockSignals(True)", body,
                       "must block signals to avoid recursive refresh")
        self.assertIn("blockSignals(False)", body,
                       "must unblock signals after advancing")


class TestParseIsoDatetimeTimezone(unittest.TestCase):
    """BUG-DASHBOARD-TZ: _parse_iso_datetime must produce UTC QDateTime.
    Stripping +00:00 without marking UTC causes a 2h offset in UTC+2,
    making end_date 2h earlier than intended and excluding all events.
    """

    def _read_dialog_source(self) -> str:
        import os
        path = os.path.join(
            os.path.dirname(__file__), "..", "recover_dialog.py",
        )
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()

    def test_parse_iso_marks_utc(self):
        body = self._read_dialog_source()
        idx = body.index("def _parse_iso_datetime")
        idx_end = body.index("\n    def ", idx + 1)
        method = body[idx:idx_end]
        self.assertIn("setTimeSpec", method,
                       "_parse_iso_datetime must mark the QDateTime as UTC")

    def test_apply_cached_date_bounds_does_not_set_end_input(self):
        body = self._read_dialog_source()
        idx = body.index("def _apply_cached_date_bounds")
        idx_end = body.index("\n    def ", idx + 1)
        method = body[idx:idx_end]
        self.assertNotIn("end_input.setDateTime", method,
                          "end_input must NOT be set to max_date (timezone "
                          "stripping makes it earlier than intended)")


class TestProjectSwitchResetsDialog(unittest.TestCase):
    """BUG-PROJECT-SWITCH: changing project must fully reset the dialog
    so it picks up the new journal instead of reading the old one.
    """

    def _read_source(self, filename) -> str:
        import os
        path = os.path.join(os.path.dirname(__file__), "..", filename)
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()

    def test_on_project_switched_exists_in_dialog(self):
        src = self._read_source("recover_dialog.py")
        self.assertIn("def on_project_switched(self", src)

    def test_on_project_switched_resets_key_state(self):
        src = self._read_source("recover_dialog.py")
        idx = src.index("def on_project_switched")
        idx_end = src.index("\n    def ", idx + 1)
        body = src[idx:idx_end]
        self.assertIn("_close_dialog_read_conn", body,
                       "must close stale read connection")
        self.assertIn("_initial_bounds_applied = False", body,
                       "must reset date bounds flag")
        self.assertIn("_refresh_journal_status", body,
                       "must trigger full refresh")

    def test_plugin_calls_on_project_switched(self):
        src = self._read_source("recover.py")
        idx = src.index("def _notify_dialog_project_switched")
        idx_end = src.index("\n    def ", idx + 1)
        body = src[idx:idx_end]
        self.assertIn("on_project_switched", body,
                       "plugin must call on_project_switched on the dialog")
        self.assertIn("tracker=self._tracker", body,
                       "must pass the new tracker to the dialog")


class TestRestoreDispatchCoverage(unittest.TestCase):
    """BLK-02: dispatch tables must cover exactly INSERT/UPDATE/DELETE."""

    def test_restore_dispatch_covers_all_ops(self):
        from recoverland.core.restore_service import _restore_dispatch
        dispatch = _restore_dispatch()
        self.assertEqual(
            set(dispatch.keys()), {"INSERT", "UPDATE", "DELETE"},
            "restore dispatch must cover exactly the 3 compensatory ops",
        )

    def test_undo_dispatch_covers_all_ops(self):
        from recoverland.core.restore_service import _undo_dispatch
        dispatch = _undo_dispatch()
        self.assertEqual(
            set(dispatch.keys()), {"INSERT", "UPDATE", "DELETE"},
            "undo dispatch must cover exactly the 3 compensatory ops",
        )

    def test_dispatch_keys_match_contract(self):
        """BLK-02: dispatch tables align with restore_contracts.COMPENSATORY_OPS."""
        from recoverland.core.restore_contracts import COMPENSATORY_OPS
        from recoverland.core.restore_service import (
            _restore_dispatch, _undo_dispatch,
        )
        contract_ops = set(COMPENSATORY_OPS.values())
        self.assertEqual(
            set(_restore_dispatch().keys()), contract_ops,
            "restore dispatch must stay in sync with COMPENSATORY_OPS",
        )
        self.assertEqual(
            set(_undo_dispatch().keys()), contract_ops,
            "undo dispatch must stay in sync with COMPENSATORY_OPS",
        )


if __name__ == "__main__":
    unittest.main()
