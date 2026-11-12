#!/usr/bin/env python3
"""Pin the AWS list prices used by cost_model.py.

Source: the public AWS Price List Bulk API (no account, no key):
https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/<ServiceCode>/current/<region>/index.json
Each price is stored with the offer file version and publication date, so a
reviewer can re-derive every number. On-demand list prices, no discounts.

    python3 tools/fetch_aws_prices.py                 # writes docs/aws_prices_snapshot.json
    python3 tools/fetch_aws_prices.py --cache /tmp/p  # reuse downloaded offer files
"""
import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASE = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/%s/current/%s/index.json"

# key: (service code, exact usagetype with {R} for the region prefix, regex the description must match)
SELECT = {
    "kinesis_shard_hour": ("AmazonKinesis", "Storage-ShardHour", ""),
    "kinesis_put_payload_unit": ("AmazonKinesis", "PutRequestPayloadUnits", ""),
    "kinesis_extended_retention_shard_hour": ("AmazonKinesis", "Extended-ShardHour", ""),
    "kinesis_ondemand_stream_hour": ("AmazonKinesis", "OnDemand-StreamHour", ""),
    "kinesis_ondemand_gb_in": ("AmazonKinesis", "OnDemand-BilledIncomingBytes", ""),
    "kinesis_ondemand_gb_out": ("AmazonKinesis", "OnDemand-BilledOutgoingBytes", ""),
    "firehose_gb_ingested_tier1": ("AmazonKinesisFirehose", "{R}BilledBytes", "Tier 1"),
    "lambda_request_arm": ("AWSLambda", "Request-ARM", ""),
    "lambda_gb_second_arm": ("AWSLambda", "Lambda-GB-Second-ARM", "Tier-1"),
    "fargate_arm_vcpu_hour": ("AmazonECS", "{R}Fargate-ARM-vCPU-Hours:perCPU", ""),
    "fargate_arm_gb_hour": ("AmazonECS", "{R}Fargate-ARM-GB-Hours", ""),
    "alb_hour": ("AWSELB", "LoadBalancerUsage", "Application LoadBalancer-hour"),
    "alb_lcu_hour": ("AWSELB", "LCUUsage", "Application load balancer capacity"),
    "s3_standard_gb_month": ("AmazonS3", "TimedStorage-ByteHrs", "first 50 TB"),
    "s3_put_request": ("AmazonS3", "Requests-Tier1", ""),
    "elasticache_valkey_r7g_xlarge_hour": ("AmazonElastiCache", "NodeUsage:cache.r7g.xlarge", "Valkey"),
    "dynamodb_write_request_unit": ("AmazonDynamoDB", "WriteRequestUnits", ""),
    "cloudwatch_logs_gb": ("AmazonCloudWatch", "{R}DataProcessing-Bytes", "Standard log class"),
    "cloudwatch_custom_metric_month": ("AmazonCloudWatch", "CW:MetricMonitorUsage", "first 10,000"),
    "vpc_endpoint_hour": ("AmazonVPC", "{R}VpcEndpoint-Hours", ""),
    "vpc_endpoint_gb": ("AmazonVPC", "{R}VpcEndpoint-Bytes", "upto 1 PB"),
    "data_transfer_out_gb": ("AWSDataTransfer", "DataTransfer-Out-Bytes", "first 10 TB"),
    "data_transfer_inter_az_gb": ("AWSDataTransfer", "DataTransfer-Regional-Bytes", ""),
    # alternatives that were priced and rejected
    "flink_kpu_hour": ("AmazonKinesisAnalytics", "{R}KPU-Hour-Java", ""),
    "msk_m7g_large_broker_hour": ("AmazonMSK", "{R}Kafka.m7g.large", ""),
    "msk_storage_gb_month": ("AmazonMSK", "{R}Kafka.Storage.GP2", ""),
    "msk_serverless_cluster_hour": ("AmazonMSK", "{R}KafkaServerless-ClusterHours", ""),
    "msk_serverless_gb_in": ("AmazonMSK", "{R}KafkaServerless-In-Bytes", ""),
    "redshift_serverless_rpu_hour": ("AmazonRedshift", "{R}Redshift:ServerlessUsage", ""),
    "apigateway_http_request": ("AmazonApiGateway", "{R}ApiGatewayHttpRequest", "first 300 million"),
}
REGION_PREFIX = {"us-east-1": "USE1-", "eu-west-1": "EU-", "eu-central-1": "EUC1-"}


def load(service, region, cache):
    url = BASE % (service, region)
    path = cache / ("%s.%s.json" % (service, region)) if cache else None
    if path and path.exists():
        return json.loads(path.read_text()), url
    with urllib.request.urlopen(url, timeout=120) as response:
        raw = response.read()
    if path:
        path.write_bytes(raw)
    return json.loads(raw), url


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--region", default="us-east-1", choices=sorted(REGION_PREFIX))
    parser.add_argument("--cache", type=Path, help="directory for downloaded offer files")
    parser.add_argument("--out", type=Path, default=ROOT / "docs" / "aws_prices_snapshot.json")
    args = parser.parse_args()
    if args.cache:
        args.cache.mkdir(parents=True, exist_ok=True)
    offers, prices, missing = {}, {}, []
    for key, (service, usagetype, wanted) in SELECT.items():
        if service not in offers:
            offers[service] = load(service, args.region, args.cache)
        offer, url = offers[service]
        usagetype = usagetype.replace("{R}", REGION_PREFIX[args.region])
        found = []
        for sku, product in offer["products"].items():
            if product.get("attributes", {}).get("usagetype") != usagetype:
                continue
            for term in offer["terms"]["OnDemand"].get(sku, {}).values():
                for dim in term["priceDimensions"].values():
                    if re.search(wanted, dim["description"]) and float(dim["pricePerUnit"]["USD"]) > 0:
                        found.append((float(dim["pricePerUnit"]["USD"]), dim["unit"], dim["description"].strip(), sku))
        if len({f[0] for f in found}) != 1:  # ambiguity or absence is an error, never a silent default
            missing.append("%s: %d distinct prices for %s" % (key, len({f[0] for f in found}), usagetype))
            continue
        usd, unit, description, sku = sorted(found)[0]
        prices[key] = {"usd": usd, "unit": unit, "description": description, "service": service, "usagetype": usagetype,
                       "sku": sku, "offer_version": offer["version"], "publication_date": offer["publicationDate"], "source": url}
    if missing:
        print("price selection failed:\n  " + "\n  ".join(missing), file=sys.stderr)
        return 1
    args.out.write_text(json.dumps({"region": args.region, "billing": "on-demand list prices, USD, no discount, no free tier",
                                    "prices": prices}, indent=2) + "\n")
    print("%d prices written to %s" % (len(prices), args.out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
