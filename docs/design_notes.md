# Design notes

The mechanisms behind `submission.md`, in more detail than four pages allow. Labels as in the submission: [Benchmarked] comes from `docs/references.md`, [Assumed] waits for a measurement, [Estimated] is computed from both.

## Gate: Lambda event source mapping

| Setting | Value | Why |
|---|---|---|
| Batch size, batching window | 500 records, 0 s | Freshness first; about 480 records per shard per second at the 10x peak [Estimated] |
| ParallelizationFactor | 2, raised to 4 if the iterator age grows at peak | More concurrency without resharding, order kept per partition key [Benchmarked] |
| Function response | `ReportBatchItemFailures`, returning only the first failed sequence number | Lambda checkpoints at the lowest failed sequence number and re-delivers what follows [Benchmarked], so handlers are idempotent |
| Bisect on error | on | Isolates a record that crashes the code |
| Maximum retry attempts | unbounded | Bad data never raises: it is quarantined inside the handler. A failing batch therefore means an outage or a bug, and it should wait, not be discarded |
| Maximum record age | 6 days (518,400 s) | Inside the 7-day stream retention [Benchmarked] |
| On-failure destination | S3, which keeps the payload [Benchmarked] | Last resort only. Any object there pages someone and must be replayed; reconciliation counts it as not stored |

Overload or a ClickHouse outage therefore costs freshness, not data: records wait in Kinesis, the iterator age pages a human after 60 s, and nothing is discarded before 6 days.

## Deduplication: claim after commit

1. **Read.** The gate drops events whose `(tenant_id, event_id)` key in Valkey says committed, and counts them as duplicates.
2. **Write.** It inserts the rest into ClickHouse with `async_insert=1`, `wait_for_async_insert=1` and `insert_deduplication_token` = hash of (shard id, first and last sequence number of the batch). ClickHouse drops a retried block with the same token inside its deduplication window, which counts recent inserts, not time [Benchmarked]. It is a second line of defence.
3. **Commit.** One Lua script per batch sets each key with `NX` and a 48 h expiry and, only for keys it creates, updates the visitor's segment counters. On an `identify` event the same script copies the visitor's counters to the user's key. A key that already existed means two copies were in flight together: that event id goes to a purge list.

What this gives:

- A failed insert leaves no key, so the retry re-inserts: no loss.
- Segment counters move once per event id **only while the 48 h dedup key exists**. The configured Kinesis retry/record-age window extends to 6 days, so a delayed redelivery can increment counters again. ClickHouse deduplication and nightly reconciliation do not protect the Valkey counter in that case. **Production blocker:** either retain event-id keys for longer than the full retry/rewind horizon (and resize Valkey in the cost model), or require a separately fenced, idempotent replay path for expired keys. Neither option is implemented or costed here.
- ClickHouse can hold a doubled row only if two copies were in flight together or the gate died between write and commit. The nightly reconciliation compares rows with distinct event ids per tenant-hour, applies the purge list, and rebuilds the rollups of the hours it touched. Exactly-once effects in ClickHouse are reached after reconciliation, not instantly.
- A Valkey failover can lose the last second of keys and counter updates. After a failover, segment state for the last 48 h is rebuilt from ClickHouse.
- Replays, from the raw archive or from the failure destination, run the gate in replay mode: they delete their ClickHouse window first, skip the dedup read, and rebuild segment state for that window afterwards. Tombstones always apply.

## Freshness budget

| Hop | Expected | Worst, no backlog | Label |
|---|---|---|---|
| Collector batch flush | 0.05 s | 0.1 s | [Assumed] |
| `PutRecords` round trip | 0.05 s | 0.1 s | [Assumed] |
| Kinesis to Lambda, one poll per shard per second | 0.5 s | 1.0 s | [Benchmarked] |
| Gate: one pipelined Valkey call and one insert per batch | 0.3 s | 0.5 s | [Assumed] |
| ClickHouse Cloud asynchronous insert flush, up to 1 s | 0.5 s | 1.0 s | [Benchmarked] |
| Dashboard query on minute rollups | 0.1 s | 0.3 s | [Assumed] |
| Dashboard refresh every 2 s | 1.0 s | 2.0 s | [Assumed] |
| **Total** | **2.5 s** | **5.0 s** | [Estimated] |

The worst case touches the target, which is why this is an **unmeasured** p95 SLO target, not a proven latency bound. The proposed canary sends an event every 10 s and polls the API until it appears. That only measures API visibility: dashboard refresh, polling interval, and the time before the request reaches the collector (TLS, the browser) are excluded. An end-to-end dashboard SLO needs a separate measurement.

## ClickHouse layout

- `events`: partitioned by `toYYYYMM(received_at)`, ordered by `(tenant_id, toStartOfHour(event_time), anonymous_id, event_id)`, TTL `received_at + retention_days`, where `retention_days` is written per row from the tenant's setting.
- `rollups_minute`: AggregatingMergeTree fed by a materialized view that is a plain `GROUP BY` on the inserted block. One block in, one block out is what deduplication with asynchronous inserts requires [Benchmarked].
- `identity_links`: one row per `(tenant_id, anonymous_id, user_id)` with `first_seen`.
- Tenant isolation: the API is the only client. Dashboard queries go through parameterized views that cannot run without a tenant parameter, which the API takes from the authenticated session. Row policies are not enough on their own: a user with no policy sees every row [Benchmarked]. Per-tenant quotas, keyed by tenant, are in the MVP against noisy neighbours.

## Deletion

- **Intake.** From the tenant console or API, authenticated. A `privacy_request` arriving in the SDK stream only triggers suppression, rate-limited per tenant and per source; beyond the limit nothing happens until the tenant confirms, and the tenant is alerted. This blocks using the public endpoint to hide visitors at will.
- **Scope.** Deletion may cover the verified subject's user id and visitor ids supported by authenticated, independent identity evidence inside the tenant. A public `privacy_request` asserting `(user_id, anonymous_id)` **cannot itself establish that link** or authorize a wider deletion. Shared-device anonymous records must remain undecided for human review, rather than being deleted by visitor id alone. `analyze_fixture.py` reports a **candidate** scope derived from all observed links, including self-asserted ones; it is a forensic fixture analyzer, **not** an authenticated deletion executor or an approved deletion manifest. Do not feed its candidate `subject_anonymous_ids`/`affected_events_in_file` directly to a deletion job. The shared-device case is tested; independent verification of links is not implemented by the fixture analyzer.
- **Cutoff.** The server receipt time of the request. On the fixture, evt-0006 has a later client time than the request but was received earlier [Observed].
- **Targets.** ClickHouse raw rows, then the rollup buckets that held them, rebuilt from raw; Valkey keys; the Parquet exports of affected hours, rewritten, plus a deletion feed that customers apply to their warehouses. The raw archive, quarantine and failure destination expire after 14 days; ClickHouse backups after 7 days.
- **During the migration.** The forwarder checks tombstones before replaying into the old ingest, and the request is also run through the old pipeline's own deletion process [Assumed: one exists, since the old system already faces GDPR and CCPA].
- **Tombstones.** `(tenant_id, subject ids, cutoff)` in DynamoDB, read by the gate, the forwarder, every replay and every backfill.
- **Proof.** Every week a test subject in the canary tenant is deleted, then searched for in every store. Each step writes an audit row.
- **Deadlines.** GDPR: one month (Art. 12(3)); CCPA: 45 days [Benchmarked]. Suppression within the hour, hot stores within 7 days, archives and backups within 14 days [Assumed].

## Edge and migration mechanics

- The SDK hostname points to the new ALB once, and the ALB first forwards all requests to today's ingest. The ramp then moves request-level weights between the old target group and the collector (weights from 0 to 999 per target group [Benchmarked]), so rollback is a weight change, not a DNS change [Assumed: today's ingest can be registered as an ALB target].
- Before a known peak: an LCU reservation on the ALB [Benchmarked] and shards doubled the day before. Nine collector tasks always run.
- The collector keeps the original request metadata (client IP, user agent, headers) in the Kinesis record, so the forwarder can replay the same request to the old ingest.
- **Legacy forwarder.** It reads Kinesis like any consumer, skips events that entered through the old ingest, deduplicates on `(tenant_id, event_id)`, checks tombstones, and re-sends the original request, rate-limited below the old ingest's safe rate. When the old system is slow the forwarder lags, alarmed per tenant, instead of overloading it; Kinesis keeps 7 days.
- **Contract tests.** Recorded SDK requests and today's responses (status codes, headers, CORS preflight, `sendBeacon` content types, payload variants such as evt-0009) are replayed against the collector in CI.

## Exports

Hourly Parquet per tenant, written from ClickHouse to S3 by tenant, date and hour. A late event re-issues its hour with a new manifest version, and customers' loads are idempotent on `event_id`. Snowflake and BigQuery pull from S3, so egress is compressed Parquet.
