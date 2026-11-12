# Migration plan

Goal: move 500+ tenants to the new pipeline without changing the SDK and without breaking any existing dashboard, export or integration, with a rollback at every step. Durations are [Assumed] for two dedicated senior engineers; thresholds are [Assumed] until the baseline of step 1 exists.

## Two independent switches

The plan never flips everything at once. Two switches move separately:

1. **Front door.** Which component receives SDK traffic: today's ingest or the new collector. The SDK hostname points to the new ALB once, with all requests forwarded to today's ingest; the ramp then moves request-level weights between the two target groups, so rollback is a weight change, not a DNS change [Assumed: today's ingest can be registered as an ALB target].
2. **Read path per tenant.** Which store a tenant's dashboard, API and exports read. Moved with a feature flag per tenant.

While the front door moves, a **legacy forwarder** (a Kinesis consumer) replays every event the collector accepted into today's ingest, so the old pipeline keeps receiving what it receives today and stays the system of record. It skips events that entered through the old ingest (each event carries its ingest path), deduplicates on `(tenant_id, event_id)`, checks deletion tombstones, re-sends the original request metadata, and is rate-limited below the old ingest's safe rate: when the old system is slow, the forwarder lags (alarmed per tenant) instead of overloading it. Mechanics: `docs/design_notes.md`.

## Phases

| Weeks | Phase | Done when | Rollback |
|---|---|---|---|
| 1 to 3 | **Baseline.** Count events per tenant-hour at today's ingest and at its dashboard store, to locate the 3% loss. Record real SDK requests and responses per SDK version and turn them into contract tests for the collector (status codes, headers, CORS preflight, `sendBeacon` content types, payload variants such as evt-0009). Send customers the notice their contracts require before ClickHouse Cloud becomes a sub-processor. | Baseline dashboards exist for every tenant; contract tests pass against the collector in staging; notice period running. | Nothing changed in production. |
| 3 to 5 | **Build in staging.** Collector, Kinesis, gate, Firehose archive, ClickHouse raw and rollups, reconciliation, canary tenant. Replay archived traffic at 10x the 30-day peak. | Load test: zero unaccounted events, iterator age back under 5 s within 10 minutes after the spike. | Not in production yet. |
| 4 to 7 | **Front door ramp.** 1%, 10%, 50%, 100% of SDK requests to the collector, one week per step, each including a weekend. Old pipeline fed by the forwarder. | 100% of requests through the collector. | ALB weight back to the old target group. The old ingest never stopped. |
| 7 to 9 | **Parallel run.** Both pipelines receive 100% of events. | 14 days with the acceptance criteria below, measured for every tenant at once. | Same as above. |
| 9 to 12 | **Per-tenant cutover.** Cohorts: internal tenants, then 10%, 50% and 100% of tenants, the 20 largest last. | Each cohort keeps parity for 7 days after its switch. | Flip the tenant's flag back: the old pipeline is still complete. |
| 13 to 22 | **Full system.** Segment builder and personalization API, Parquet exports to Snowflake and BigQuery, per-tenant retention, self-serve privacy API. Old exports keep running from the forwarder until each tenant's new export is validated. | Exports validated per tenant (row counts and checksums per hour). | Keep the old export for that tenant. |
| 23 to 26 | **Decommission**, 30 days after the last tenant moved. | Forwarder off, old pipeline archived. | Not reversible: requires sign-off. |

## What "data accuracy verified" means

Measured over 14 consecutive days that include a weekend peak, for every tenant-hour:

1. **Completeness.** Accepted by the collector = rows in ClickHouse + quarantined + dropped as duplicates + failure destination. Zero unaccounted events, exactly. This is the reconciliation job; it runs hourly and nightly.
2. **No overcount.** Rows equal distinct event ids in every tenant-hour, after reconciliation.
3. **Parity with the old system.** The new deduplicated count is at least the old count minus 0.1%, and every difference is explained by category (duplicate removed, quarantined, tagged automation, lost by the old system). Parity cannot prove zero loss, since the old system loses events; completeness (1) is the proof.
4. **Freshness.** p95 under 5 s and p99 under 15 s from the canary: a canary tenant sends an event every 10 s and polls the API until it appears.
5. **Dashboard parity.** For 20 sampled tenants, top pages and hourly counts match within 0.5% where both systems define them.

## Automatic rollback triggers

| Signal | Threshold | Action |
|---|---|---|
| Unaccounted events | any, in any tenant-hour | Freeze the ramp, page, investigate before the next step |
| Collector 5xx | above 0.1% for 5 minutes | ALB weight back to the old target group |
| Kinesis iterator age | above 60 s for 10 minutes | Page; above 1 day, freeze the ramp. Records wait up to 6 days before the failure destination, inside the 7-day retention |
| Forwarder lag | above 15 minutes for a tenant | Page: the old system of record is behind for that tenant |
| Failure destination | any object | Page, replay, investigate |
| Tenant parity gap | above 1% | Flip that tenant's read flag back |

## MVP in 3 months versus 6 months

| | MVP, end of month 3 | Full system, end of month 6 |
|---|---|---|
| Ingest | Collector, Kinesis, gate, archive, quarantine | Event packing if the cost model asks for it |
| Store | ClickHouse raw and minute rollups, per-tenant query quotas | Per-tenant retention |
| Product | Real-time dashboards for all tenants | Segment builder, personalization API, exports |
| Compliance | Suppression, tenant verification, deletion, tombstones, audit | Self-serve privacy API for tenants |
| Operations | Reconciliation, canary, alarms, runbooks | Decommission of the old pipeline |

Not in either: Flink, multi-region, dedicated clusters per tenant. Each needs a measured reason first.

## Team

- **Engineer A:** collector, Kinesis, gate, archive, replay tool, reconciliation, load tests.
- **Engineer B:** ClickHouse schema and rollups, query and segment API, privacy workflow, parity dashboards, per-tenant cutover.
- Both: on-call for the pipeline from the first production traffic (week 4), runbooks written before each ramp step.

## Service level objectives

| SLO | Target | Measured by |
|---|---|---|
| Freshness | p95 < 5 s, p99 < 15 s | Canary tenant, histogram of (visible in API) minus (received_at) |
| Completeness | 100% accounted per tenant-day | Reconciliation job |
| Ingest availability | 99.95% of requests answered 2xx per month | ALB metrics |
| Query latency | p95 < 1 s for dashboard API calls | API histogram |
| Deletion | Suppressed within 1 hour, deleted from hot stores within 7 days, gone from archives and backups within 14 days | Weekly test subject deleted in the canary tenant and searched for in every store |

All targets are [Assumed]: they are commitments to measure against, not results.
