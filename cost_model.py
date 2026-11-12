#!/usr/bin/env python3
"""Sizing and monthly cost model for the proposed pipeline. Standard library only.

Every input carries a source label. AWS prices are read from
docs/aws_prices_snapshot.json (AWS Price List Bulk API, see tools/fetch_aws_prices.py),
ClickHouse Cloud prices from docs/clickhouse_cloud_prices.json (public pricing page).
The Markdown is generated, so the prose cannot drift from the arithmetic:

    python3 cost_model.py            # rewrites docs/cost_model.md
    python3 cost_model.py --print    # prints it instead
"""
import argparse
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HOURS = 730                     # hours per month, the convention of AWS pricing examples
CEILING = 50_000                # budget ceiling of the brief, USD per month

# ---- Inputs. B = from the brief, A = assumed, K = benchmarked (vendor documentation) ----
BRIEF = {"events_per_day": 50_000_000, "spike_multiplier": 10}
ASSUMED = {
    "event_bytes": (1000, "average event on the wire; the fixture line is smaller because it carries no URL, user agent or context"),
    "shard_headroom": (2.0, "provisioned shards over the bare per-shard limit, for uneven partition keys"),
    "requests_per_collector_task": (1000, "requests per second one 1 vCPU / 2 GB collector task handles; to be load-tested"),
    "collector_headroom": (1.5, "collector capacity over the design peak, always on, so the spike does not wait for autoscaling"),
    "events_per_connection": (4, "events sent per new TLS connection"),
    "http_overhead_bytes": (800, "request headers plus response per event"),
    "response_bytes": (300, "bytes sent back per event (status and headers)"),
    "lambda_seconds": (0.2, "average duration of one gate invocation"),
    "lambda_gb": (1.0, "memory of the gate function"),
    "dedup_window_days": (2, "how long a (tenant_id, event_id) key is remembered"),
    "bytes_per_dedup_key": (120, "Valkey memory per key including overhead"),
    "segment_state_gb": (4, "per-visitor segment state held in Valkey"),
    "valkey_usable_fraction": (0.75, "share of node memory used before scaling"),
    "raw_retention_days": (14, "raw pre-gate archive, short on purpose so that erasure by expiry beats the one-month legal deadline"),
    "gzip_ratio": (5, "compression of raw JSON lines"),
    "curated_months": (13, "retention of the curated store"),
    "parquet_fraction": (0.15, "curated Parquet size as a share of raw JSON"),
    "clickhouse_bytes_per_row": (100, "compressed bytes per event row: the 1,000-byte event at the roughly 10x compression the vendor cites; to be measured on real data"),
    "clickhouse_replicas": (3, "replicas of the ClickHouse service, one per availability zone"),
    "clickhouse_units_per_replica": (4, "compute units per replica, each 2 vCPU and 8 GiB"),
    "log_gb": (50, "structured logs per month: per batch, never per event"),
    "custom_metrics": (200, "CloudWatch custom metrics, low cardinality: no tenant dimension"),
    "interface_endpoints": (6, "VPC interface endpoints in 3 zones: Kinesis, CloudWatch Logs, ECR api, ECR dkr, STS, ClickHouse PrivateLink"),
    "cross_az_hops": (3, "zone crossings per event: load balancer to collector, Lambda to Valkey, Lambda to ClickHouse"),
    "api_tasks": (4, "tasks of the query and privacy API"),
    "non_prod_fraction": (0.35, "staging and development as a share of production"),
    "flink_kpus": (5, "4 KPUs sized for the peak plus 1 orchestration KPU"),
    "redshift_base_rpu": (8, "Redshift Serverless base capacity kept warm for streaming ingestion"),
}
BENCHMARKED = {
    "shard_records_per_s": (1000, "https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html"),
    "shard_mb_per_s": (1.0, "https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html"),
    "put_unit_bytes": (25_000, "https://aws.amazon.com/kinesis/data-streams/pricing/"),
    "firehose_increment_bytes": (5000, "https://aws.amazon.com/firehose/pricing/"),
    "lcu_new_connections_per_s": (25, "https://aws.amazon.com/elasticloadbalancing/pricing/"),
    "lcu_gb_per_hour": (1.0, "https://aws.amazon.com/elasticloadbalancing/pricing/"),
    "lambda_polls_per_shard_per_s": (1, "https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html"),
    "valkey_node_gib": (26.32, "https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheNodes.SupportedTypes.html"),
}


def A(name):
    return ASSUMED[name][0]


def K(name):
    return BENCHMARKED[name][0]


def sizing(events_per_day, event_bytes, peak_multiplier):
    avg_eps = events_per_day / 86400
    peak_eps = avg_eps * peak_multiplier
    events_month = avg_eps * 3600 * HOURS
    peak_mb_s = peak_eps * event_bytes / 1e6
    bare_shards = max(peak_eps / K("shard_records_per_s"), peak_mb_s / K("shard_mb_per_s"))
    return {
        "events_per_day": events_per_day, "event_bytes": event_bytes, "peak_multiplier": peak_multiplier,
        "avg_eps": avg_eps, "peak_eps": peak_eps, "events_month": events_month,
        "avg_mb_s": avg_eps * event_bytes / 1e6, "peak_mb_s": peak_mb_s,
        "gb_day": events_per_day * event_bytes / 1e9, "gb_month": events_month * event_bytes / 1e9,
        "bare_shards": math.ceil(bare_shards), "shards": math.ceil(bare_shards * A("shard_headroom")),
        "collector_tasks": math.ceil(peak_eps * A("collector_headroom") / A("requests_per_collector_task")),
    }


def costs(s, price, ch):
    """Monthly cost lines: (component, how it is computed, USD)."""
    p = {k: v["usd"] for k, v in price.items()}
    task_hour = p["fargate_arm_vcpu_hour"] + 2 * p["fargate_arm_gb_hour"]
    lcu = max(s["avg_eps"] / A("events_per_connection") / K("lcu_new_connections_per_s"),
              s["avg_eps"] * (s["event_bytes"] + A("http_overhead_bytes")) * 3600 / 1e9 / K("lcu_gb_per_hour"))
    put_units = s["events_month"] * math.ceil(s["event_bytes"] / K("put_unit_bytes"))
    invocations = s["shards"] * K("lambda_polls_per_shard_per_s") * 3600 * HOURS
    billed_bytes = math.ceil(s["event_bytes"] / K("firehose_increment_bytes")) * K("firehose_increment_bytes")
    firehose_gb = s["events_month"] * billed_bytes / 1e9
    raw_gb = s["gb_day"] * A("raw_retention_days") / A("gzip_ratio")
    curated_gb = s["gb_month"] * A("curated_months") * A("parquet_fraction")
    valkey_gb = s["events_per_day"] * A("dedup_window_days") * A("bytes_per_dedup_key") / 1e9 + A("segment_state_gb")
    valkey_nodes = 2 * math.ceil(valkey_gb / (K("valkey_node_gib") * A("valkey_usable_fraction")))
    ch_tb = s["events_month"] * A("curated_months") * A("clickhouse_bytes_per_row") / 1e12
    ch_units = A("clickhouse_replicas") * A("clickhouse_units_per_replica")
    lines = [
        ("Collector on Fargate (ARM)", "%d tasks of 1 vCPU and 2 GB, always on" % s["collector_tasks"], s["collector_tasks"] * HOURS * task_hour),
        ("Application Load Balancer", "1 load balancer and %.1f LCU" % lcu, HOURS * p["alb_hour"] + lcu * HOURS * p["alb_lcu_hour"]),
        ("Data transfer out (responses)", "%.0f GB" % (s["events_month"] * A("response_bytes") / 1e9), s["events_month"] * A("response_bytes") / 1e9 * p["data_transfer_out_gb"]),
        ("Kinesis Data Streams, provisioned", "%d shards with 7-day retention, %.2f billion PUT units" % (s["shards"], put_units / 1e9),
         s["shards"] * HOURS * (p["kinesis_shard_hour"] + p["kinesis_extended_retention_shard_hour"]) + put_units * p["kinesis_put_payload_unit"]),
        ("Lambda gate (ARM)", "%.1f million invocations of %.1f s at %.0f GB" % (invocations / 1e6, A("lambda_seconds"), A("lambda_gb")),
         invocations * p["lambda_request_arm"] + invocations * A("lambda_seconds") * A("lambda_gb") * p["lambda_gb_second_arm"]),
        ("Firehose to the raw archive", "%.0f GB billed (records rounded up to 5 KB)" % firehose_gb, firehose_gb * p["firehose_gb_ingested_tier1"]),
        ("S3, raw archive and curated Parquet", "%.0f GB raw for %d days, %.0f GB curated at month %d" % (raw_gb, A("raw_retention_days"), curated_gb, A("curated_months")),
         (raw_gb + curated_gb) * p["s3_standard_gb_month"]),
        ("ElastiCache for Valkey", "%d r7g.xlarge nodes for %.0f GB of keys and segment state" % (valkey_nodes, valkey_gb),
         valkey_nodes * HOURS * p["elasticache_valkey_r7g_xlarge_hour"]),
        ("ClickHouse Cloud compute", "%d compute units around the clock" % ch_units, ch_units * HOURS * ch["compute_unit_hour_usd"]),
        ("ClickHouse Cloud storage", "%.2f TB compressed at month %d" % (ch_tb, A("curated_months")), ch_tb * ch["storage_tb_month_usd"]),
        ("DynamoDB control tables", "tombstones, tenant settings, replay checkpoints: one million writes", 1e6 * p["dynamodb_write_request_unit"]),
        ("CloudWatch logs and metrics", "%d GB of logs and %d custom metrics" % (A("log_gb"), A("custom_metrics")),
         A("log_gb") * p["cloudwatch_logs_gb"] + A("custom_metrics") * p["cloudwatch_custom_metric_month"]),
        ("VPC interface endpoints", "%d endpoints in 3 zones and %.0f GB" % (A("interface_endpoints"), s["gb_month"]),
         A("interface_endpoints") * 3 * HOURS * p["vpc_endpoint_hour"] + s["gb_month"] * p["vpc_endpoint_gb"]),
        ("Cross-zone data transfer", "%d crossings of %.0f GB, billed in both directions" % (A("cross_az_hops"), s["gb_month"]),
         A("cross_az_hops") * s["gb_month"] * 2 * p["data_transfer_inter_az_gb"]),
        ("Query and privacy API on Fargate", "%d tasks of 1 vCPU and 2 GB" % A("api_tasks"), A("api_tasks") * HOURS * task_hour),
    ]
    production = sum(usd for _, _, usd in lines)
    lines.append(("Staging and development", "%.0f%% of production" % (A("non_prod_fraction") * 100), production * A("non_prod_fraction")))
    return lines


def alternatives(s, price, ch):
    p = {k: v["usd"] for k, v in price.items()}
    chosen = dict((name, usd) for name, _, usd in costs(s, price, ch))
    http_api = min(s["events_month"], 3e8) * p["apigateway_http_request"] + max(s["events_month"] - 3e8, 0) * 0.9e-6
    on_demand = HOURS * p["kinesis_ondemand_stream_hour"] + s["gb_month"] * (p["kinesis_ondemand_gb_in"] + 2 * p["kinesis_ondemand_gb_out"])
    msk = 3 * HOURS * p["msk_m7g_large_broker_hour"] + s["gb_day"] * 7 * 3 * p["msk_storage_gb_month"]
    msk_serverless = HOURS * p["msk_serverless_cluster_hour"] + s["gb_month"] * p["msk_serverless_gb_in"]
    flink = A("flink_kpus") * HOURS * p["flink_kpu_hour"]
    redshift = A("redshift_base_rpu") * HOURS * p["redshift_serverless_rpu_hour"]
    clickhouse = chosen["ClickHouse Cloud compute"] + chosen["ClickHouse Cloud storage"]
    return [
        ("Front door", "ALB and Fargate collector", chosen["Collector on Fargate (ARM)"] + chosen["Application Load Balancer"],
         "API Gateway HTTP API, %.2f billion requests" % (s["events_month"] / 1e9), http_api),
        ("Buffer", "Kinesis provisioned, %d shards" % s["shards"], chosen["Kinesis Data Streams, provisioned"],
         "Kinesis on-demand, two consumers", on_demand),
        ("Buffer", "Kinesis provisioned, %d shards" % s["shards"], chosen["Kinesis Data Streams, provisioned"],
         "MSK, 3 kafka.m7g.large brokers, 7 days on 3 replicas", msk),
        ("Buffer", "Kinesis provisioned, %d shards" % s["shards"], chosen["Kinesis Data Streams, provisioned"],
         "MSK Serverless, cluster hours and data in only", msk_serverless),
        ("Processing", "Lambda gate", chosen["Lambda gate (ARM)"], "Managed Service for Apache Flink, %d KPUs" % A("flink_kpus"), flink),
        ("Analytical store", "ClickHouse Cloud", clickhouse,
         "Redshift Serverless, %d RPU kept warm, storage excluded" % A("redshift_base_rpu"), redshift),
    ]


def usd(value):
    return "$%s" % format(round(value), ",")


def render(price_file, ch, observed):
    price = price_file["prices"]
    base = sizing(BRIEF["events_per_day"], A("event_bytes"), BRIEF["spike_multiplier"])
    scenarios = [
        ("Base case", base),
        ("Fat events (2 KB)", sizing(BRIEF["events_per_day"], 2000, BRIEF["spike_multiplier"])),
        ("Spike on top of the daily peak (25x the average)", sizing(BRIEF["events_per_day"], A("event_bytes"), 25)),
        ("Volume grows 10x and stays there", sizing(BRIEF["events_per_day"] * 10, A("event_bytes"), BRIEF["spike_multiplier"])),
    ]
    def table(header, rows):
        return "\n".join(["| " + " | ".join(header) + " |", "|" + "---|" * len(header)] + ["| " + " | ".join(str(c) for c in r) + " |" for r in rows])

    lines = costs(base, price, ch)
    total = sum(v for _, _, v in lines)
    dates = sorted({v["publication_date"][:10] for v in price.values()})
    out = [
        "# Sizing and cost model",
        "Generated by `cost_model.py`. Do not edit by hand: change an input in the script and re-run it.",
        "Labels: [Observed] measured here, [Estimated] computed from labelled inputs, [Benchmarked] taken from a named public source, [Assumed] placeholder to be replaced by a measurement. Units: 1 GB = 10^9 bytes, 1 month = %d hours." % HOURS,
        "\n## Throughput and volume\n",
        table(["quantity", "value", "label and derivation"], [
            ("Events per day", format(BRIEF["events_per_day"], ","), "[Benchmarked] the brief"),
            ("Average rate", "%.1f events/s" % base["avg_eps"], "[Estimated] 50,000,000 / 86,400"),
            ("Design peak", "%.0f events/s" % base["peak_eps"], "[Estimated] average x %d, the spike multiplier of the brief" % BRIEF["spike_multiplier"]),
            ("Event size in the fixture", "%s bytes mean, %s to %s" % (observed["mean"], observed["min"], observed["max"]), "[Observed] `artifacts/anomaly_report.json`, 25 lines"),
            ("Event size on the wire", "%d bytes" % A("event_bytes"), "[Assumed] " + ASSUMED["event_bytes"][1]),
            ("Average bytes", "%.2f MB/s" % base["avg_mb_s"], "[Estimated] rate x size"),
            ("Peak bytes", "%.2f MB/s" % base["peak_mb_s"], "[Estimated] rate x size"),
            ("Raw volume", "%.0f GB/day, %.0f GB/month" % (base["gb_day"], base["gb_month"]), "[Estimated] %.2f billion events per month" % (base["events_month"] / 1e9)),
            ("Kinesis shards at the bare limit", base["bare_shards"], "[Estimated] max(peak events / %d, peak MB / %.0f): records per second bind before bytes" % (K("shard_records_per_s"), K("shard_mb_per_s"))),
            ("Kinesis shards provisioned", base["shards"], "[Estimated] bare limit x %.0f headroom [Assumed]" % A("shard_headroom")),
            ("Collector tasks", base["collector_tasks"], "[Estimated] peak x %.1f / %d requests per task [Assumed]" % (A("collector_headroom"), A("requests_per_collector_task"))),
        ]),
        "\nEvent size sensitivity [Estimated]:\n",
        table(["event size", "average MB/s", "peak MB/s", "GB/day", "GB/month", "shards provisioned"],
              [("%d bytes" % b, "%.2f" % x["avg_mb_s"], "%.2f" % x["peak_mb_s"], "%.0f" % x["gb_day"], "%.0f" % x["gb_month"], x["shards"])
               for b in (500, 1000, 2000) for x in [sizing(BRIEF["events_per_day"], b, BRIEF["spike_multiplier"])]]),
        "\n## Monthly cost, base case\n",
        "Region %s, %s. AWS prices [Benchmarked]: AWS Price List Bulk API, offer files published %s to %s, pinned in `docs/aws_prices_snapshot.json`. ClickHouse Cloud prices [Benchmarked]: %s, read %s, %s. Quantities [Estimated] from the inputs listed at the end.\n"
        % (price_file["region"], price_file["billing"], dates[0], dates[-1], ch["source"], ch["read_on"], ch["tier"]),
        table(["component", "quantity", "USD per month"], [(n, q, usd(v)) for n, q, v in lines] + [("**Total**", "", "**%s**" % usd(total))]),
        "\n[Estimated] The base case uses %.0f%% of the %s ceiling. This is a list-price model, not an AWS quote: no support plan, no taxes, no committed-use discount."
        % (total / CEILING * 100, usd(CEILING)),
        "\n## Scenarios\n",
        table(["scenario", "peak events/s", "shards", "collector tasks", "USD per month", "share of ceiling"],
              [(name, "%.0f" % s["peak_eps"], s["shards"], s["collector_tasks"], usd(sum(v for _, _, v in costs(s, price, ch))),
                "%.0f%%" % (sum(v for _, _, v in costs(s, price, ch)) / CEILING * 100)) for name, s in scenarios]),
        "\n[Assumed] ClickHouse compute is held constant in every scenario. For the last one that is optimistic: if compute had to grow in proportion to volume it would add %s per month and the total would pass the ceiling. The honest reading: a 10x spike for hours or days fits easily, and a permanent 10x growth needs a budget conversation and a load test before it needs a new architecture."
        % usd(9 * A("clickhouse_replicas") * A("clickhouse_units_per_replica") * HOURS * ch["compute_unit_hour_usd"] * (1 + A("non_prod_fraction"))),
        "\n## What was priced and not chosen\n",
        "Same inputs, same price files [Estimated]. Cost is rarely the deciding factor at this volume: the written answer gives the reasons.\n",
        table(["layer", "chosen", "USD per month", "alternative", "USD per month"], [(a, b, usd(c), d, usd(e)) for a, b, c, d, e in alternatives(base, price, ch)]),
        "\n## What could break the budget\n",
        "\n".join("- " + t for t in [
            "**The analytical store.** It is the largest line and the least certain: compute units and bytes per row are [Assumed] until a load test on real events. Unbounded dashboard queries or one dedicated service per tenant would multiply it.",
            "**Event size.** Session-replay style payloads of 10 KB or more multiply Kinesis shards, Firehose and S3. The 5 KB billing increment of Firehose already bills a 1 KB event as 5 KB [Benchmarked].",
            "**Per-tenant anything.** 500 tenants times a metric, a stream, a log group or a partition: a tenant dimension on %d CloudWatch metrics alone would cost %s per month [Estimated]. Per-tenant numbers belong in ClickHouse."
            % (A("custom_metrics"), usd(500 * A("custom_metrics") * 0.10)),
            "**Per-event logging.** One 1 KB log line per event is %.0f GB, about %s per month of CloudWatch ingestion [Estimated], and it would also copy personal data into logs."
            % (base["gb_month"], usd(base["gb_month"] * price["cloudwatch_logs_gb"]["usd"])),
            "**Egress.** Pushing every raw event to customer warehouses outside AWS costs %s per month at %s per GB [Estimated]; exports are compressed Parquet for that reason."
            % (usd(base["gb_month"] * price["data_transfer_out_gb"]["usd"]), "$%.2f" % price["data_transfer_out_gb"]["usd"]),
            "**Retention in the hot store.** Storage grows linearly with months kept; the model stops at %d months." % A("curated_months"),
            "**A European region.** Prices there are higher; re-run `tools/fetch_aws_prices.py --region eu-west-1` before promising a number.",
        ]),
        "\n## Inputs\n",
        table(["input", "value", "label", "note or source"],
              [(k, format(v, ","), "[Benchmarked]", "the brief") for k, v in BRIEF.items()]
              + [(k, v[0], "[Assumed]", v[1]) for k, v in ASSUMED.items()]
              + [(k, v[0], "[Benchmarked]", v[1]) for k, v in BENCHMARKED.items()]
              + [("clickhouse_compute_unit_hour_usd", ch["compute_unit_hour_usd"], "[Benchmarked]", ch["source"]),
                 ("clickhouse_storage_tb_month_usd", ch["storage_tb_month_usd"], "[Benchmarked]", ch["source"])]),
        "\nAWS prices used [Benchmarked]:\n",
        table(["key", "USD", "unit", "published", "description"],
              [(k, "%g" % v["usd"], v["unit"], v["publication_date"][:10], v["description"]) for k, v in price.items()]),
    ]
    return "\n".join(out) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--print", action="store_true", help="print the Markdown instead of writing docs/cost_model.md")
    args = parser.parse_args(argv)
    price_file = json.loads((ROOT / "docs" / "aws_prices_snapshot.json").read_text())
    ch = json.loads((ROOT / "docs" / "clickhouse_cloud_prices.json").read_text())
    observed = json.loads((ROOT / "artifacts" / "anomaly_report.json").read_text())["summary"]["bytes_per_line"]
    text = render(price_file, ch, observed)
    if args.print:
        sys.stdout.write(text)
    else:
        (ROOT / "docs" / "cost_model.md").write_text(text, encoding="utf-8")
        base = sizing(BRIEF["events_per_day"], A("event_bytes"), BRIEF["spike_multiplier"])
        print("base case %s per month, written to docs/cost_model.md" % usd(sum(v for _, _, v in costs(base, price_file["prices"], ch))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
