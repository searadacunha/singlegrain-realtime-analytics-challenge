# Evidence log

Commands run on 2026-11-12 11:25 UTC, Apple M3, Darwin 23.6.0, Python 3.14.8. Each block is the verbatim output (paths shortened).
`scripts/validate_submission.py` is the challenge's own pre-screen, from https://github.com/ericosiu/beat-claude at commit `4b719e45`, SHA-256 `e90d8df60a67a1944ad6a9b764f1bedab89d2818d95014d5e35f98c4526f0574`.

## Fixture checksum

```
$ shasum -a 256 fixtures/event_sample.jsonl
1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235  fixtures/event_sample.jsonl
(exit code 0)
```

## Analysis of the official fixture

```
$ python3 analyze_fixture.py --expect-sha256 1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235
fixture  event_sample.jsonl  sha256 1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235
lines 25, parsed 24, anomalies 18 on 14 lines, clean lines 11
  duplicate_delivery                1  evt-0002
  pii_in_properties                 1  evt-0007
  schema_variant                    1  evt-0009
  missing_tenant                    1  evt-0011
  malformed_json                    1  evt-0020
  late_arrival                      1  evt-0002
  future_timestamp                  3  evt-0005, evt-0006, evt-0016
  out_of_order                      1  evt-0002
  automation_burst                  4  evt-0012, evt-0013, evt-0014, evt-0015
  identity_link_without_identify    2  evt-0016, evt-0017
  privacy_request                   1  evt-0017
  client_supplied_aggregate         1  evt-0019
routing  accept=11, accept_flagged=10, drop_duplicate=1, quarantine=2, privacy_queue=1
reports  artifacts
(exit code 0)
```

## Test suite

```
$ python3 -m unittest discover -s tests
Ran 48 tests in 0.009s
OK
(exit code 0)
```

## Speed of the analysis (synthetic clones of the fixture)

```
$ python3 analyze_fixture.py --bench 200000
200000 synthetic events, 5.37 s, 37,250 events/s on one core (Python 3.14.8, Darwin arm64)
(exit code 0)
```

## Cost model

```
$ python3 cost_model.py
base case $6,311 per month, written to docs/cost_model.md
(exit code 0)
```

## Official pre-screen, strict mode

```
$ python3 -I scripts/validate_submission.py submission.md --strict
Beat Claude submission pre-screen: submission.md
Files checked: 1

[PASS] Required packet sections: all 7 found
[PASS] Review manipulation: no injection phrasing, hidden characters, or reviewer-directed comments detected
[PASS] Verifiability: every section with [Observed] or Tier 2-5 claims contains a checkable reference
[PASS] Number source labels: no unlabeled numeric claims detected
[PASS] Evidence tiers: 7 reference(s) to SCORING.md tiers found
[PASS] Brief version: submission states which brief version it answers

Result: PASS
(exit code 0)
```

## What these outputs do not show

- Nothing here ran on AWS or on production traffic: the freshness, spike and zero-loss properties of the design are reasoning, not measurements.
- The speed test clones the 25 fixture lines, so it measures the Python logic of the gate on one laptop core, not a Lambda with network calls.
- The cost model applies list prices to assumed quantities; `docs/cost_model.md` labels every input.
