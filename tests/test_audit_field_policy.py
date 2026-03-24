"""Tests for core.audit_field_policy - hard edge cases, toxic inputs."""
import unittest
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from recoverland.core.audit_field_policy import is_layer_audit_field


class TestExactMatches(unittest.TestCase):
    """Every known audit field must be detected."""

    MUST_MATCH = [
        "audit_timestamp", "audit_user", "date_modif",
        "date_modification", "last_edited_at", "last_edited_by",
        "modif_par", "modifie_par", "updated_at", "updated_by",
        "user_name",
    ]

    def test_all_known_audit_fields(self):
        for name in self.MUST_MATCH:
            with self.subTest(name=name):
                self.assertTrue(
                    is_layer_audit_field(name),
                    f"{name!r} should be detected as audit field",
                )


class TestPrefixMatches(unittest.TestCase):
    """Fields that START with an audit prefix must be caught."""

    def test_date_modification_suffixed(self):
        self.assertTrue(is_layer_audit_field("date_modification_auto"))
        self.assertTrue(is_layer_audit_field("date_modification_123"))

    def test_modif_par_suffixed(self):
        self.assertTrue(is_layer_audit_field("modif_par_systeme"))
        self.assertTrue(is_layer_audit_field("modif_par_batch"))

    def test_updated_at_suffixed(self):
        self.assertTrue(is_layer_audit_field("updated_at_utc"))

    def test_audit_timestamp_suffixed(self):
        self.assertTrue(is_layer_audit_field("audit_timestamp_ms"))


class TestCaseInsensitivity(unittest.TestCase):
    def test_upper(self):
        self.assertTrue(is_layer_audit_field("AUDIT_TIMESTAMP"))
        self.assertTrue(is_layer_audit_field("UPDATED_AT"))
        self.assertTrue(is_layer_audit_field("DATE_MODIF"))

    def test_mixed(self):
        self.assertTrue(is_layer_audit_field("Date_Modif"))
        self.assertTrue(is_layer_audit_field("Updated_By"))
        self.assertTrue(is_layer_audit_field("AuditTimestamp"))

    def test_camel_case(self):
        self.assertTrue(is_layer_audit_field("dateModif"))
        self.assertTrue(is_layer_audit_field("updatedAt"))


class TestUnicodeAccents(unittest.TestCase):
    def test_modifie_with_accent(self):
        self.assertTrue(is_layer_audit_field("modifie_par"))
        self.assertTrue(is_layer_audit_field("modifi\u00e9_par"))

    def test_accented_e_acute(self):
        self.assertTrue(is_layer_audit_field("modifi\u00e9par"))

    def test_combining_accent(self):
        self.assertTrue(is_layer_audit_field("modifie\u0301_par"))


class TestNonAuditFieldsMustBeRejected(unittest.TestCase):
    """Normal data fields must NEVER be flagged as audit."""

    MUST_REJECT = [
        "gid", "nom", "surface", "geometry", "the_geom", "wkb_geometry",
        "id", "fid", "code", "label", "description", "type",
        "longueur", "largeur", "hauteur", "statut", "commune",
        "date_creation", "created_at", "created_by",
        "update_count",
    ]

    def test_all_normal_fields(self):
        for name in self.MUST_REJECT:
            with self.subTest(name=name):
                self.assertFalse(
                    is_layer_audit_field(name),
                    f"{name!r} should NOT be detected as audit field",
                )

    def test_partial_substring_not_matched(self):
        self.assertFalse(is_layer_audit_field("update"))
        self.assertFalse(is_layer_audit_field("audit"))
        self.assertFalse(is_layer_audit_field("date"))
        self.assertFalse(is_layer_audit_field("modif"))


class TestToxicInputs(unittest.TestCase):
    """The function must never crash on garbage input."""

    def test_none(self):
        self.assertFalse(is_layer_audit_field(None))

    def test_empty_string(self):
        self.assertFalse(is_layer_audit_field(""))

    def test_integer(self):
        self.assertFalse(is_layer_audit_field(42))

    def test_float(self):
        self.assertFalse(is_layer_audit_field(3.14))

    def test_boolean(self):
        self.assertFalse(is_layer_audit_field(True))

    def test_list(self):
        self.assertFalse(is_layer_audit_field(["audit_timestamp"]))

    def test_dict(self):
        self.assertFalse(is_layer_audit_field({"name": "audit_timestamp"}))

    def test_bytes(self):
        self.assertFalse(is_layer_audit_field(b"audit_timestamp"))

    def test_null_byte_in_string_still_matches_after_strip(self):
        self.assertTrue(is_layer_audit_field("audit\x00timestamp"))

    def test_sql_injection_string(self):
        self.assertFalse(is_layer_audit_field("'; DROP TABLE audit_event; --"))

    def test_very_long_string(self):
        self.assertFalse(is_layer_audit_field("x" * 100_000))

    def test_whitespace_only(self):
        self.assertFalse(is_layer_audit_field("   "))
        self.assertFalse(is_layer_audit_field("\t\n"))

    def test_emoji_suffix_stripped_prefix_still_matches(self):
        self.assertTrue(is_layer_audit_field("date_modif_\U0001F600"))

    def test_pure_emoji_rejected(self):
        self.assertFalse(is_layer_audit_field("\U0001F600\U0001F601"))


class TestBoundaryBetweenAuditAndData(unittest.TestCase):
    """Fields that are close to audit names but should NOT match."""

    def test_created_at_is_data(self):
        self.assertFalse(is_layer_audit_field("created_at"))

    def test_created_by_is_data(self):
        self.assertFalse(is_layer_audit_field("created_by"))

    def test_date_creation_is_data(self):
        self.assertFalse(is_layer_audit_field("date_creation"))

    def test_user_id_is_data(self):
        self.assertFalse(is_layer_audit_field("user_id"))


if __name__ == '__main__':
    unittest.main()
