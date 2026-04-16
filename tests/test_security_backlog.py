"""Regression tests for the security backlog (SEC-01 through SEC-12).

Each test class maps to a backlog item. Tests verify the fix without
altering plugin functional behavior.
"""
import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.sql_safety import assert_safe_fragment
from recoverland.core.datasource_registry import _strip_password_from_uri
from recoverland.core.integrity import _validate_pending_event
from recoverland.core.search_service import (
    _build_where_clause, _MAX_PARAM_LEN,
)
from recoverland.core.audit_backend import SearchCriteria
from recoverland.i18n.compile_translations import (
    _safe_parse, _MAX_TS_FILE_SIZE, _parse_ts, compile_ts_to_qm,
)


# ---------------------------------------------------------------------------
# SEC-01: XML parsing safety
# ---------------------------------------------------------------------------

class TestSEC01_XmlParsing(unittest.TestCase):

    def test_safe_parse_normal_ts_file(self):
        """A normal .ts file should parse without error."""
        ts = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<TS version="2.1" language="fr">\n'
            '<context><name>Ctx</name>\n'
            '<message><source>Hello</source>'
            '<translation>Bonjour</translation></message>\n'
            '</context></TS>\n'
        )
        with tempfile.NamedTemporaryFile(
            suffix=".ts", mode="w", encoding="utf-8", delete=False
        ) as f:
            f.write(ts)
            path = f.name
        try:
            tree = _safe_parse(path)
            self.assertEqual(tree.getroot().tag, "TS")
        finally:
            os.unlink(path)

    def test_safe_parse_rejects_oversized_file(self):
        """Files exceeding _MAX_TS_FILE_SIZE are rejected."""
        with tempfile.NamedTemporaryFile(suffix=".ts", delete=False) as f:
            f.write(b"x" * (_MAX_TS_FILE_SIZE + 1))
            path = f.name
        try:
            with self.assertRaises(ValueError):
                _safe_parse(path)
        finally:
            os.unlink(path)

    def test_parse_ts_yields_translations(self):
        """_parse_ts correctly yields (context, source, translation)."""
        ts = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<TS version="2.1" language="fr">\n'
            '<context><name>Dialog</name>\n'
            '<message><source>OK</source>'
            '<translation>OK</translation></message>\n'
            '</context></TS>\n'
        )
        with tempfile.NamedTemporaryFile(
            suffix=".ts", mode="w", encoding="utf-8", delete=False
        ) as f:
            f.write(ts)
            path = f.name
        try:
            entries = list(_parse_ts(path))
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0], ("Dialog", "OK", "OK"))
        finally:
            os.unlink(path)

    def test_compile_ts_to_qm_produces_file(self):
        """End-to-end: compile a .ts to .qm without error."""
        ts = (
            '<?xml version="1.0" encoding="utf-8"?>\n'
            '<TS version="2.1" language="fr">\n'
            '<context><name>Ctx</name>\n'
            '<message><source>Hello</source>'
            '<translation>Bonjour</translation></message>\n'
            '</context></TS>\n'
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            ts_path = os.path.join(tmpdir, "test_fr.ts")
            qm_path = os.path.join(tmpdir, "test_fr.qm")
            with open(ts_path, "w", encoding="utf-8") as f:
                f.write(ts)
            count = compile_ts_to_qm(ts_path, qm_path)
            self.assertEqual(count, 1)
            self.assertTrue(os.path.exists(qm_path))


# ---------------------------------------------------------------------------
# SEC-04: SQL fragment safety
# ---------------------------------------------------------------------------

class TestSEC04_SqlSafety(unittest.TestCase):

    def test_safe_fragment_accepts_valid_clauses(self):
        assert_safe_fragment("")
        assert_safe_fragment("restored_from_event_id IS NULL")
        assert_safe_fragment("WHERE datasource_fingerprint = ? AND user_name = ?")
        assert_safe_fragment("event_id, project_fingerprint, datasource_fingerprint")
        assert_safe_fragment("?,?,?,?")

    def test_safe_fragment_rejects_semicolon(self):
        with self.assertRaises(ValueError):
            assert_safe_fragment("1; DROP TABLE audit_event")

    def test_safe_fragment_rejects_comment(self):
        with self.assertRaises(ValueError):
            assert_safe_fragment("1 -- comment")

    def test_safe_fragment_rejects_quotes(self):
        with self.assertRaises(ValueError):
            assert_safe_fragment("user_name = 'admin'")

    def test_safe_fragment_returns_input(self):
        clause = "WHERE event_id = ?"
        self.assertIs(assert_safe_fragment(clause), clause)


# ---------------------------------------------------------------------------
# SEC-07: Pending event validation
# ---------------------------------------------------------------------------

class TestSEC07_PendingEventValidation(unittest.TestCase):

    def _valid_event(self):
        return {
            "project_fingerprint": "p",
            "datasource_fingerprint": "d",
            "provider_type": "ogr",
            "operation_type": "DELETE",
            "attributes_json": "{}",
            "user_name": "alice",
            "created_at": "2025-01-01T00:00:00",
        }

    def test_valid_event_accepted(self):
        self.assertEqual(_validate_pending_event(self._valid_event()), "")

    def test_not_a_dict_rejected(self):
        self.assertIn("not a dict", _validate_pending_event("string"))
        self.assertIn("not a dict", _validate_pending_event(42))

    def test_missing_required_key_rejected(self):
        evt = self._valid_event()
        del evt["operation_type"]
        reason = _validate_pending_event(evt)
        self.assertIn("missing keys", reason)

    def test_unknown_key_rejected(self):
        evt = self._valid_event()
        evt["malicious_key"] = "payload"
        reason = _validate_pending_event(evt)
        self.assertIn("unknown keys", reason)

    def test_invalid_operation_type_rejected(self):
        evt = self._valid_event()
        evt["operation_type"] = "TRUNCATE"
        reason = _validate_pending_event(evt)
        self.assertIn("invalid operation_type", reason)

    def test_empty_created_at_rejected(self):
        evt = self._valid_event()
        evt["created_at"] = ""
        reason = _validate_pending_event(evt)
        self.assertIn("empty created_at", reason)

    def test_all_valid_operation_types(self):
        for op in ("INSERT", "UPDATE", "DELETE"):
            evt = self._valid_event()
            evt["operation_type"] = op
            self.assertEqual(_validate_pending_event(evt), "")


# ---------------------------------------------------------------------------
# SEC-09: Password stripping
# ---------------------------------------------------------------------------

class TestSEC09_PasswordStripping(unittest.TestCase):

    def test_single_quoted_password(self):
        uri = "host=localhost password='secret123' dbname=test"
        self.assertNotIn("secret123", _strip_password_from_uri(uri))
        self.assertIn("password=***", _strip_password_from_uri(uri))

    def test_double_quoted_password(self):
        uri = 'host=localhost password="secret123" dbname=test'
        self.assertNotIn("secret123", _strip_password_from_uri(uri))

    def test_unquoted_password(self):
        uri = "host=localhost password=secret123 dbname=test"
        self.assertNotIn("secret123", _strip_password_from_uri(uri))

    def test_sslpassword(self):
        uri = "host=localhost sslpassword='cert_pass' dbname=test"
        self.assertNotIn("cert_pass", _strip_password_from_uri(uri))

    def test_passwd_variant(self):
        uri = "host=localhost passwd=mypass dbname=test"
        self.assertNotIn("mypass", _strip_password_from_uri(uri))

    def test_case_insensitive(self):
        uri = "host=localhost PASSWORD=MySecret dbname=test"
        self.assertNotIn("MySecret", _strip_password_from_uri(uri))

    def test_no_password_unchanged(self):
        uri = "host=localhost dbname=test user=admin"
        self.assertEqual(_strip_password_from_uri(uri), uri)

    def test_multiple_passwords_all_stripped(self):
        uri = "password='a' sslpassword='b' passwd=c"
        result = _strip_password_from_uri(uri)
        self.assertNotIn("'a'", result)
        self.assertNotIn("'b'", result)
        self.assertNotIn("=c", result)


# ---------------------------------------------------------------------------
# SEC-11: SearchCriteria length validation
# ---------------------------------------------------------------------------

class TestSEC11_SearchCriteriaLength(unittest.TestCase):

    def test_normal_length_accepted(self):
        criteria = SearchCriteria(
            datasource_fingerprint="fp",
            layer_name="layer",
            operation_type="UPDATE",
            user_name="alice",
            start_date="2025-01-01",
            end_date="2025-12-31",
            page=1,
            page_size=100,
        )
        where, params = _build_where_clause(criteria)
        self.assertIn("WHERE", where)
        self.assertEqual(len(params), 6)

    def test_oversized_param_rejected(self):
        criteria = SearchCriteria(
            datasource_fingerprint="x" * (_MAX_PARAM_LEN + 1),
            layer_name=None,
            operation_type=None,
            user_name=None,
            start_date=None,
            end_date=None,
            page=1,
            page_size=100,
        )
        with self.assertRaises(ValueError):
            _build_where_clause(criteria)

    def test_exactly_max_length_accepted(self):
        criteria = SearchCriteria(
            datasource_fingerprint="x" * _MAX_PARAM_LEN,
            layer_name=None,
            operation_type=None,
            user_name=None,
            start_date=None,
            end_date=None,
            page=1,
            page_size=100,
        )
        where, params = _build_where_clause(criteria)
        self.assertEqual(len(params), 1)


# ---------------------------------------------------------------------------
# SEC-07 integration: pending event validation in recovery flow
# ---------------------------------------------------------------------------

class TestSEC07_IntegrationRecovery(unittest.TestCase):

    @staticmethod
    def _cleanup(path):
        for p in (path, path + "-wal", path + "-shm"):
            try:
                os.unlink(p)
            except OSError:
                pass

    def test_event_with_unknown_key_rejected_in_recovery(self):
        """Pending events with unknown keys are rejected during recovery."""
        from recoverland.core.sqlite_schema import initialize_schema
        from recoverland.core.integrity import (
            check_journal_integrity, _get_pending_path,
        )
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.close()
            pending_path = _get_pending_path(tmp_path)
            events = [{
                "project_fingerprint": "p",
                "datasource_fingerprint": "d",
                "provider_type": "ogr",
                "operation_type": "DELETE",
                "attributes_json": "{}",
                "user_name": "alice",
                "created_at": "2025-01-01T00:00:00",
                "INJECTED_KEY": "payload",
            }]
            with open(pending_path, "w") as f:
                json.dump(events, f)
            result = check_journal_integrity(tmp_path)
            self.assertEqual(result.recovered_events, 0)
            conn = sqlite3.connect(tmp_path)
            count = conn.execute("SELECT COUNT(*) FROM audit_event").fetchone()[0]
            conn.close()
            self.assertEqual(count, 0)
        finally:
            self._cleanup(tmp_path)

    def test_event_with_invalid_op_rejected_in_recovery(self):
        """Pending events with invalid operation_type are rejected."""
        from recoverland.core.sqlite_schema import initialize_schema
        from recoverland.core.integrity import (
            check_journal_integrity, _get_pending_path,
        )
        with tempfile.NamedTemporaryFile(suffix='.sqlite', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            conn = sqlite3.connect(tmp_path)
            initialize_schema(conn)
            conn.close()
            pending_path = _get_pending_path(tmp_path)
            events = [{
                "project_fingerprint": "p",
                "datasource_fingerprint": "d",
                "provider_type": "ogr",
                "operation_type": "TRUNCATE",
                "attributes_json": "{}",
                "user_name": "alice",
                "created_at": "2025-01-01T00:00:00",
            }]
            with open(pending_path, "w") as f:
                json.dump(events, f)
            result = check_journal_integrity(tmp_path)
            self.assertEqual(result.recovered_events, 0)
        finally:
            self._cleanup(tmp_path)


if __name__ == '__main__':
    unittest.main()
