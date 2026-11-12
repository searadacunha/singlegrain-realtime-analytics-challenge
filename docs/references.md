# Sources

Sources of the [Benchmarked] numbers and of the service behaviour the design depends on. Each page was read on 2026-11-11 or 2026-11-12. No code was copied from any of them: every script in this repository was written for this challenge.

## Challenge

| Item | Version read |
|---|---|
| https://github.com/ericosiu/beat-claude: `README.md`, `SCORING.md`, `submissions/README.md`, `challenges/engineer-004/brief.md`, `scripts/validate_submission.py` | commit `4b719e4501c8ff98fd3e641c46d3bffe34b9aaa5` (2026-07-08), brief version 2026-07 |
| `challenges/engineer-004/fixtures/event_sample.jsonl` | SHA-256 `1aeb24b415009e89fcf8acb5a178410faf216dc17b16920d9849ecc8bbb24235`. The repository has no license file, so the fixture is fetched by `tools/fetch_fixture.py`, not redistributed |

## Prices

| Source | What |
|---|---|
| AWS Price List Bulk API, `https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<service>/current/us-east-1/index.json` | Every AWS price in `docs/cost_model.md`, with the offer version and publication date of each, in `docs/aws_prices_snapshot.json` |
| https://aws.amazon.com/kinesis/data-streams/pricing/ | PUT payload unit of 25 KB |
| https://aws.amazon.com/firehose/pricing/ | Ingestion billed in 5 KB increments |
| https://aws.amazon.com/elasticloadbalancing/pricing/ | LCU dimensions (25 new connections/s, 1 GB/h for container targets) |
| https://clickhouse.com/pricing | Scale plan on AWS us-east-1: $0.2985 per compute unit-hour (2 vCPU, 8 GiB), $25.30 per TB-month of compressed storage |

## Service behaviour

| Topic | Page |
|---|---|
| Kinesis shard limits (1,000 records/s or 1 MB/s write), retention up to 365 days, capacity-mode switches twice per 24 h | https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html |
| On-demand mode absorbs up to twice the previous 30-day peak, throttling for up to 15 minutes beyond it; warm throughput requires On-demand Advantage | https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html |
| `PutRecords` can return HTTP 200 with failed records; ordering not guaranteed | https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html |
| Duplicates from producer and consumer retries; embed a primary key | https://docs.aws.amazon.com/streams/latest/dev/kinesis-record-processor-duplicates.html |
| Iterator age, risk of loss past 50% of retention | https://docs.aws.amazon.com/streams/latest/dev/monitoring-with-cloudwatch.html |
| Lambda polls each shard once per second; batch size, parallelization factor | https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html |
| Default retries are infinite and a bad record can block a shard; S3 failure destination keeps the payload | https://docs.aws.amazon.com/lambda/latest/dg/kinesis-on-failure-destination.html |
| Partial batch responses: checkpoint at the lowest failed sequence number | https://docs.aws.amazon.com/lambda/latest/dg/services-kinesis-batchfailurereporting.html |
| Firehose buffering, dynamic partitioning (500 active partitions by default) | https://docs.aws.amazon.com/firehose/latest/dev/create-configure-backup.html |
| ALB weighted target groups (weights 0 to 999, requests distributed by weight) | https://docs.aws.amazon.com/elasticloadbalancing/latest/application/rule-action-types.html |
| ALB capacity unit reservation for an upcoming spike | https://docs.aws.amazon.com/elasticloadbalancing/latest/application/capacity-unit-reservation.html |
| API Gateway account throttle of 10,000 requests/s per Region | https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html |
| MSK Serverless limits | https://docs.aws.amazon.com/msk/latest/developerguide/limits.html |
| Valkey node memory (cache.r7g.xlarge, 26.32 GiB) | https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheNodes.SupportedTypes.html |
| Managed Flink autoscaling (scale-up after 15 minutes above 75% CPU) | https://docs.aws.amazon.com/managed-flink/latest/java/how-scaling-auto.html |
| Managed Flink default checkpoint interval of 60 s | https://docs.aws.amazon.com/managed-flink/latest/java/how-fault.html |
| Flink watermarks follow the highest timestamp seen; late records dropped by default | https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/ and https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/ (Flink 2.3) |
| Flink exactly-once needs a replayable source and a transactional or idempotent sink | https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/ |
| ClickHouse asynchronous inserts, flush within 1 s on ClickHouse Cloud, acknowledgement after flush | https://clickhouse.com/docs/optimize/asynchronous-inserts |
| ClickHouse insert deduplication and `insert_deduplication_token` | https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries |
| Materialized views are insert triggers | https://clickhouse.com/docs/materialized-view/incremental-materialized-view |
| ReplacingMergeTree deduplicates only at merge time | https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree |
| Low-cardinality partition keys | https://clickhouse.com/docs/best-practices/choosing-a-partitioning-key |
| Parameterized views, which cannot run without their parameters | https://clickhouse.com/docs/sql-reference/statements/create/view |
| Row policies; a user with no policy sees every row | https://clickhouse.com/docs/reference/statements/create/row-policy |
| Lightweight DELETE leaves bytes until a merge | https://clickhouse.com/docs/reference/statements/delete |
| ClickHouse Cloud compliance (SOC 2 Type II, ISO 27001) | https://clickhouse.com/docs/products/cloud/reference/security/compliance-overview |
| ClickHouse Cloud backups, retention 1 to 45 days | https://clickhouse.com/docs/products/cloud/features/backups/overview |
| Powertools for AWS Lambda (Python) batch utility, v3.40.0 | https://github.com/aws-powertools/powertools-lambda-python/blob/develop/docs/utilities/batch.md |
| Powertools idempotency utility: 2 write units per new call, so not used per event | https://github.com/aws-powertools/powertools-lambda-python/blob/develop/docs/utilities/idempotency.md |
| OpenTelemetry Python 1.45.2: traces and metrics stable, 2,000 attribute sets per metric by default | https://github.com/open-telemetry/opentelemetry-python |

## Law

| Rule | Source |
|---|---|
| GDPR: answer a data subject request within one month of receipt (Art. 12(3)) | https://gdpr-info.eu/art-12-gdpr/ |
| CCPA: respond within 45 calendar days | https://oag.ca.gov/privacy/ccpa |

These are the published deadlines, not legal advice.
