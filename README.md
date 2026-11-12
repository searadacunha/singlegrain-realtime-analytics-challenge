# Real-time analytics pipeline: Beat Claude, Engineer 004

An answer to Single Grain's [Engineer 004 challenge](https://github.com/ericosiu/beat-claude/blob/main/challenges/engineer-004/brief.md) (brief version 2026-07): the architecture of a real-time analytics pipeline for 50M events a day on AWS, and a runnable event-quality gate that works the official fixture.

**Read [`submission.md`](submission.md) first.** It is the written answer and the full submission packet.

## Run it

Python 3.10 or later, standard library only. No AWS account, no API key, nothing to install.

```sh
python3 analyze_fixture.py --expect-sha256 1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235
python3 -m unittest discover -s tests -v
python3 cost_model.py --print
```

`analyze_fixture.py` reads `fixtures/event_sample.jsonl`, writes [`artifacts/anomaly_report.json`](artifacts/anomaly_report.json) and [`artifacts/anomaly_report.md`](artifacts/anomaly_report.md), and exits with code 3 if the fixture is not the one the answer was written against. On a rotated fixture: `python3 analyze_fixture.py --fixture path/to/event_sample.jsonl --out-dir /tmp/out`.

## What it finds in the official fixture

18 anomalies on 14 of the 25 lines, 11 certain and 7 suspected. Every line gets exactly one routing decision: 11 accepted, 10 accepted with flags, 1 duplicate dropped, 2 quarantined, 1 sent to the privacy queue.

| Class | event_id |
|---|---|
| Duplicate delivery, late and out of order | evt-0002 |
| Client clock ahead of the server | evt-0005, evt-0006, evt-0016 |
| Personal data in properties | evt-0007 |
| Legacy schema | evt-0009 |
| Missing tenant | evt-0011 |
| Suspected automation | evt-0012, evt-0013, evt-0014, evt-0015 |
| User id without an identify event | evt-0016, evt-0017 |
| Deletion request | evt-0017 |
| Counter computed by the browser | evt-0019 |
| Truncated JSON | evt-0020 |

Each anomaly carries its `event_id`, class, reason, certainty, confidence, proposed treatment and false-positive risk. Every rule is generic: no event id is hard-coded, and the tests run the same rules on synthetic edge cases. Personal data is reported by field name only, and nothing read from an event is echoed unless it looks like an identifier.

## Layout

| Path | What |
|---|---|
| [`submission.md`](submission.md) | The written answer and the seven-part packet |
| [`analyze_fixture.py`](analyze_fixture.py) | Event-quality gate: validation, dedup, timestamps, schema variants, PII, identity, automation, deletion requests, routing |
| [`tests/`](tests/) | Unit tests, including a check that the committed reports and the figures quoted in `submission.md` match a fresh run |
| [`artifacts/`](artifacts/) | Generated reports and the [evidence log](artifacts/evidence_log.md) of this run |
| [`architecture/pipeline.mmd`](architecture/pipeline.mmd) | Architecture diagram (Mermaid) |
| [`cost_model.py`](cost_model.py), [`docs/cost_model.md`](docs/cost_model.md) | Sizing and monthly cost, generated from pinned AWS list prices |
| [`tools/fetch_aws_prices.py`](tools/fetch_aws_prices.py), [`docs/aws_prices_snapshot.json`](docs/aws_prices_snapshot.json) | Price snapshot from the public AWS Price List API, with publication dates |
| [`docs/design_notes.md`](docs/design_notes.md) | Mechanisms behind the answer: Lambda settings, deduplication, freshness budget, ClickHouse layout, deletion, migration edge, exports |
| [`docs/migration_plan.md`](docs/migration_plan.md) | Phased cutover, acceptance criteria, rollback triggers, MVP scope, SLOs |
| [`docs/references.md`](docs/references.md) | Sources of every benchmarked number |
| [`fixtures/`](fixtures/) | The official fixture, unchanged |

## What is measured and what is not

Measured here: the fixture analysis, the tests, the speed of the analysis on a laptop, and the arithmetic of the cost model. Not measured: anything about a deployed system. Service limits and prices come from vendor documentation, and the inputs labelled [Assumed] in the cost model (event size, collector throughput, ClickHouse size) need a load test before anyone relies on them. Nothing in this repository has run in production.

## Continuous integration

On every push, [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the tests on Python 3.10 and 3.13, regenerates the reports and the cost model and fails if they differ from the committed files, then runs the challenge's own pre-screen (`scripts/validate_submission.py --strict`, downloaded at a pinned commit and checksum) on `submission.md`.

## AI usage

Written with Claude Code; the disclosure is section 5 of [`submission.md`](submission.md).
