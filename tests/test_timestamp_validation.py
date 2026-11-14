"""Regression tests for untrusted client timestamp parsing."""
import unittest

from analyze_fixture import parse_ts


class TimestampOffsetValidation(unittest.TestCase):
    def test_rejects_invalid_offset_fields(self):
        for suffix in ("+00:60", "-00:99", "+24:00", "-99:99", "+12:75"):
            with self.subTest(offset=suffix):
                self.assertIsNone(parse_ts("2026-06-15T14:00:00" + suffix))

    def test_rejects_utc_boundary_overflows_without_crashing(self):
        self.assertIsNone(parse_ts("0001-01-01T00:00:00+01:00"))
        self.assertIsNone(parse_ts("9999-12-31T23:59:59-01:00"))

    def test_keeps_valid_offsets_unchanged(self):
        self.assertEqual(
            parse_ts("2026-06-15T16:00:00+02:00"),
            parse_ts("2026-06-15T14:00:00Z"),
        )
        self.assertEqual(
            parse_ts("2026-06-15T09:00:00-0500"),
            parse_ts("2026-06-15T14:00:00Z"),
        )
