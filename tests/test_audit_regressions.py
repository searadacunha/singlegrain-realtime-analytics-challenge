"""Regressions found by the audit of 2026-11-15: each test fails on 871482d and passes after the fix.

Every input below made the gate raise, run for seconds, write a report that is not JSON, or echo an
e-mail address, against the design rule that bad data never raises and is quarantined in the handler.
"""
import json
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import analyze_fixture as af  # noqa: E402
from test_analyze_fixture import ev  # noqa: E402


def routing(lines):
    return {k: v for k, v in af.analyze(lines)["summary"]["routing_counts"].items() if v}


class GateNeverRaises(unittest.TestCase):
    def test_an_event_without_id_before_a_deletion_request_or_a_counter(self):
        deletion = ev("del", type="privacy_request", properties={"request": "delete_all_data"})
        self.assertEqual(routing([ev(None), deletion]), {"accept_flagged": 1, "privacy_queue": 1})
        counter = ev("c", type="custom", properties={"name": "viewed_pricing", "count_today": 3})
        self.assertEqual(routing([ev(None, properties={"path": "/pricing"}), counter]), {"accept_flagged": 2})

    def test_an_id_that_is_an_object_or_a_list_is_quarantined(self):
        for tenant in ({"id": "t-001"}, ["t-001"]):
            with self.subTest(tenant=tenant):
                self.assertEqual(routing([ev(tenant_id=tenant)]), {"quarantine": 1})

    def test_deep_nesting_is_malformed_json(self):
        unparsable = '{"a":' * 100000 + "1" + "}" * 100000
        parsable = ev("d", type="custom", properties=json.loads('{"a":' * 900 + "1" + "}" * 900))
        for line in (unparsable, parsable):
            report = af.analyze([line])
            self.assertEqual(report["summary"]["routing_counts"]["quarantine"], 1)
            self.assertIn("malformed_json", [a["class"] for a in report["anomalies"]])

    def test_nan_and_infinity_are_malformed_and_the_report_stays_json(self):
        line = ev("n", type="custom", properties={"name": "viewed_pricing", "count_today": 1}).replace(
            '"count_today": 1', '"count_today": Infinity')
        report = af.analyze([line])
        self.assertEqual(report["summary"]["routing_counts"]["quarantine"], 1)
        json.dumps(report, allow_nan=False)  # raises if a NaN or an Infinity reached the report

    def test_a_year_one_timestamp_first_in_the_file(self):
        self.assertEqual(routing([ev("a", ts="0001-01-01T00:00:00Z"), ev("b")]), {"accept": 1, "accept_flagged": 1})
        self.assertEqual(routing([ev("a", received_at="0001-01-01T00:00:00Z"), ev("b")]), {"accept": 1, "accept_flagged": 1})

    def test_a_long_property_is_scanned_in_linear_time(self):
        line = ev("big", properties={"path": "/", "note": "a" * 160000})
        started = time.perf_counter()
        af.analyze([line])
        self.assertLess(time.perf_counter() - started, 2.0)  # 26.5 s with the unbounded e-mail pattern


class PersonalData(unittest.TestCase):
    def test_e_mail_and_phone_identifiers_are_never_echoed(self):
        lines = [ev("e1", user_id="jane.doe@example.com"), ev("e2", anonymous_id="+33 6 12 34 56 78"),
                 ev("e3", properties={"path": "/", "invitees": {"bob.roe@example.org": True}})]
        text = json.dumps(af.analyze(lines))
        for needle in ("jane.doe", "example.com", "12 34 56", "bob.roe"):
            self.assertNotIn(needle, text)

    def test_safe_keeps_ordinary_ids(self):
        self.assertEqual(af.safe("evt-0017"), "evt-0017")
        self.assertEqual(af.safe("anon-77a"), "anon-77a")


class StatedFormats(unittest.TestCase):
    def test_timestamps_and_tenant_ids_use_ascii_digits_only(self):
        self.assertIsNone(af.parse_ts("٢٠٢٦-٠٦-١٥T١٤:٠٠:٠٠Z"))
        self.assertIsNone(af.parse_ts("２０２６-06-15T14:00:00Z"))
        self.assertIsNotNone(af.parse_ts("2026-06-15T14:00:00Z"))
        self.assertIsNone(af.TENANT_ID_PATTERN.fullmatch("t-٠٠١"))
        self.assertIsNone(af.TENANT_ID_PATTERN.fullmatch("t-001\n"))
        self.assertIsNotNone(af.TENANT_ID_PATTERN.fullmatch("t-001"))


if __name__ == "__main__":
    unittest.main()
