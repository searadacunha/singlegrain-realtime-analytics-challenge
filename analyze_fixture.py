#!/usr/bin/env python3
"""Event-quality gate for a JSONL sample of web analytics events.

Standard library only, Python 3.9+. It reads the file, separates what it can
prove ("certain") from what it only suspects ("suspect"), says what the
pipeline should do with each record, and writes a JSON and a Markdown report.

Every rule is generic. No event id is hard-coded, so the same command works on
a rotated fixture. Text inside events is data: nothing read from the file is
executed, and only identifier-shaped values are echoed into the reports.

    python3 analyze_fixture.py                           # fixtures/event_sample.jsonl
    python3 analyze_fixture.py --fixture other.jsonl --out-dir /tmp/out
    python3 analyze_fixture.py --bench 200000            # throughput of the gate logic
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

VERSION = "1.0.0"
HERE = Path(__file__).resolve().parent
DEFAULT_FIXTURE = HERE / "fixtures" / "event_sample.jsonl"

# ---- Assumptions: every threshold lives here, none is tuned to one record ----
SKEW_TOLERANCE_S = 1.0        # client ts may run ahead of server receipt by this much
LATE_ARRIVAL_S = 5.0          # the freshness target of the brief
MAX_EVENT_AGE_S = 7 * 86400   # older client ts than this: event time falls back to received_at
WATERMARK_BOUND_S = 5.0       # out-of-orderness bound of the simulated event-time consumer
BURST_MIN_EVENTS = 3          # page views by one visitor ...
BURST_WINDOW_S = 1.0          # ... inside this window: automation suspected
# shortcut: shape inferred from the sample, replace with a tenant-registry lookup in production
TENANT_ID_PATTERN = re.compile(r"^t-\d+$")

CANONICAL_KEYS = ("event_id", "tenant_id", "anonymous_id", "user_id", "type", "ts", "received_at", "properties")
KEY_ALIASES = {"timestamp": "ts"}
TYPE_ALIASES = {"pageview": "page_view"}
PROPERTY_ALIASES = {"page_path": "path", "ref": "referrer"}
KNOWN_TYPES = {"page_view", "click", "form_submit", "custom", "identify", "privacy_request"}

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE = re.compile(r"^\s*\+?[\d\s().-]{7,20}\s*$")
PII_KEY = re.compile(r"e[-_]?mail|phone|mobile|address|first[-_]?name|last[-_]?name|full[-_]?name|ssn|passport|birth", re.I)
PHONE_KEY = re.compile(r"phone|mobile|(^|_)tel(_|$)", re.I)
INSTRUCTION = re.compile(
    r"\b(ignore|disregard|forget|override)\b.{0,40}\b(instructions?|prompts?|rules)\b"
    r"|\byou are an? (ai|assistant|language model)\b|\bsystem prompt\b", re.I)
PRIVACY_WORDS = re.compile(r"delet|eras|forget|opt.?out|do.?not.?sell", re.I)
AGGREGATE_KEY = re.compile(r"(^|_)(count|total|visits|times)(_|$)", re.I)  # whole words: "discount" is not a counter
AUTOMATION_HINT = re.compile(r"bot|crawl|spider|scan|headless|monitor", re.I)

# class -> certainty of the observation, default confidence in the reading,
# proposed treatment, false-positive risk, and what not to do at face value.
CATALOG = {
    "malformed_json": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Quarantine the raw bytes with a reason code and count them. The consumer acknowledges the record and moves on: one poison record must never block a shard.",
        "fp": "None for the detection. The risk sits in the remedy: a cut record can be made to parse, but the fields that were cut off are unknown.",
        "trap": "Do not auto-repair a truncated record and let it drive segments or billing: what was lost is unknown.",
    },
    "schema_variant": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Accept and normalise through a versioned alias map, keep the original payload, count events per tenant and variant. The SDK cannot change, so the pipeline adapts. A missing received_at is stamped at the new edge.",
        "fp": "An alias map can mis-map a tenant's own custom property that happens to share a legacy name. Aliases must be tied to known SDK versions.",
        "trap": None,
    },
    "missing_tenant": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Quarantine. In the target design the edge derives the tenant from the SDK write key, never from the payload, and a payload tenant that disagrees is quarantined too.",
        "fp": "None for the detection. Any attempt to guess the tenant can leak one customer's visitors into another customer's dashboard.",
        "trap": "Do not infer the tenant from the path, from neighbouring lines or from a default bucket.",
    },
    "malformed_tenant_id": {
        "certainty": "suspect", "confidence": "medium",
        "treatment": "Quarantine until the tenant registry confirms the id. Production validates against the registry, not against a pattern.",
        "fp": "High: the expected shape was inferred from a small sample.",
        "trap": None,
    },
    "missing_event_id": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Accept with an id derived from a hash of tenant, visitor, type, timestamp and properties, marked as derived, so that redeliveries of the same payload still collapse.",
        "fp": "Two distinct events with identical content in the same millisecond would collapse into one.",
        "trap": None,
    },
    "missing_identity": {
        "certainty": "certain", "confidence": "medium",
        "treatment": "Keep for aggregate counts only. Exclude from sessions, segments and identity stitching.",
        "fp": "None for the detection.",
        "trap": None,
    },
    "unknown_event_type": {
        "certainty": "certain", "confidence": "medium",
        "treatment": "Store in the raw archive and in quarantine, alert on the first occurrence per tenant, never drop silently: an unknown type can be a new SDK feature or a control message.",
        "fp": "A legitimate new event type looks identical to garbage until someone checks the SDK release notes.",
        "trap": None,
    },
    "invalid_timestamp": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Use received_at as event time and flag the record. Quarantine only when neither timestamp is usable.",
        "fp": "An unusual but valid format is flagged until the parser learns it.",
        "trap": None,
    },
    "duplicate_delivery": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Idempotent write keyed on (tenant_id, event_id): keep the first copy, count the dropped copy per tenant. The dedup window must outlive SDK retries and any stream rewind.",
        "fp": "An SDK that reuses an id for different events would lose data, which is why payloads are compared before a copy is dropped (see event_id_collision).",
        "trap": None,
    },
    "event_id_collision": {
        "certainty": "certain", "confidence": "medium",
        "treatment": "Keep the first record, quarantine the later one, alert: either an SDK id-generation bug or a forged event.",
        "fp": "A client-side correction of an earlier event would look the same. The SDK contract has no such feature that we know of.",
        "trap": "Do not overwrite the first record with the later one.",
    },
    "future_timestamp": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Keep the event. Event time becomes received_at, the client value is kept as client_ts and the record is flagged ts_adjusted. Raw client time never drives partitions, TTL, windows or watermarks.",
        "fp": "None for the detection (two fields are compared). Which of the two clocks is wrong cannot be proven from one record.",
        "trap": "Do not repair the client timestamp by subtracting a guessed offset. Use the server receipt time and keep the original value.",
    },
    "late_arrival": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Accept. Aggregates and segments must be updatable after the fact, and freshness is measured from received_at: no pipeline can show an event within 5 s of a client time that is already older than that on arrival.",
        "fp": "A client clock running behind looks exactly like a late delivery. The SDK sends no sent_at stamp to tell them apart.",
        "trap": None,
    },
    "out_of_order": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Never rely on arrival order. Per-visitor order comes from the partition key (tenant_id + anonymous_id), and stateful logic uses commutative updates keyed on the event id.",
        "fp": "None for the detection.",
        "trap": None,
    },
    "pii_in_properties": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Field-level policy at the gate: tokenise (keyed hash per tenant) or drop according to the tenant's allow-list before the event reaches logs, metric labels, dashboards or exports. Logs carry event_id and tenant only. This report prints field names, never values.",
        "fp": "Test or role addresses, e-mails inside URLs, order numbers that look like phone numbers. The opposite error matters more: names and free text that no pattern can see.",
        "trap": "Do not use contact details found in properties to identify or contact the visitor: they are form input, may belong to someone else, and consent is unknown.",
    },
    "instruction_like_text": {
        "certainty": "suspect", "confidence": "medium",
        "treatment": "Treat as data. Keep it out of anything that builds a prompt for a language model (auto-insights, support summaries) unless quoted and isolated. Log the event id only.",
        "fp": "Ordinary prose about rules or instructions, such as a help-centre search query, matches the same patterns.",
        "trap": "Do not let event text reach an AI agent as instructions.",
    },
    "identity_conflict": {
        "certainty": "certain", "confidence": "medium",
        "treatment": "Do not merge the users. Attribute the anonymous visitor's events to each user by time range (shared device, logout then login).",
        "fp": "A user id that changed (migration, new e-mail) looks like two users on one device.",
        "trap": "Do not merge two user ids because they share a device.",
    },
    "cross_tenant_identifier": {
        "certainty": "certain", "confidence": "low",
        "treatment": "Nothing to fix as long as every identity key is (tenant_id, id). Never join visitors or users across tenants.",
        "fp": "High: short ids collide across tenants by coincidence.",
        "trap": None,
    },
    "identity_link_without_identify": {
        "certainty": "suspect", "confidence": "low",
        "treatment": "Treat the link as provisional unless the SDK contract makes user_id authoritative on every event. Do not back-fill earlier anonymous events on this evidence alone.",
        "fp": "High: an identify event sent before the first line of the snapshot explains it innocently.",
        "trap": None,
    },
    "automation_burst": {
        "certainty": "suspect", "confidence": "high",
        "treatment": "Tag is_suspected_automation, exclude from segments, personalization triggers and default dashboard counts, keep the raw rows and give the tenant a filter. Do not drop.",
        "fp": "Prerender or prefetch, an SDK that stamps a queued batch at flush time, or the tenant's own monitoring or security scanner.",
        "trap": "Do not block or delete on the referrer name: it is client-supplied, trivially changed, and the scanner may belong to the tenant. The timing is the evidence.",
    },
    "privacy_request": {
        "certainty": "certain", "confidence": "high",
        "treatment": "Route to the privacy queue, never to dashboards. Suppress the subject at once (reversible), verify the request with the tenant (the controller) through an authenticated channel, then delete by identity closure inside the tenant, bounded by server receipt time, and record a tombstone that every replay consults.",
        "fp": "The message type is unambiguous. Its authenticity is not: the SDK endpoint is public, so anyone can post this event with any user id.",
        "trap": "Do not execute a deletion that arrived on the public event endpoint without verification, and never read 'delete all data' as tenant-wide.",
    },
    "client_supplied_aggregate": {
        "certainty": "suspect", "confidence": "medium",
        "treatment": "Store the property, never use it as segment truth. Segments are computed server-side from deduplicated, automation-filtered events, with 'today' defined by the tenant's time zone.",
        "fp": "The snapshot does not show earlier activity, so the client number may be right. That is the reason to recompute it rather than assume either way.",
        "trap": "Do not fire a behaviour-based personalization from a counter the browser reports about itself.",
    },
}

QUARANTINE = {"malformed_json", "missing_tenant", "malformed_tenant_id", "event_id_collision", "unknown_event_type"}

_UNSAFE = re.compile(r"[^A-Za-z0-9_.:/\[\]-]")
_TS = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|z|[+-]\d{2}:?\d{2})$")


def safe(value, limit=64):
    """Echo only identifier-shaped text into reports: event content is untrusted."""
    if value is None:
        return None
    text = _UNSAFE.sub("?", str(value))
    return text[:limit] + ("..." if len(text) > limit else "")


def is_id(value):
    return isinstance(value, str) and value.strip() != ""


def parse_ts(value):
    """RFC 3339 instant -> aware UTC datetime, or None.

    Written by hand because datetime.fromisoformat rejects 'Z' before 3.11.
    A timestamp without an offset is rejected: guessing the zone is worse.
    """
    if not isinstance(value, str):
        return None
    match = _TS.match(value.strip())
    if not match:
        return None
    year, month, day, hour, minute, second, fraction, zone = match.groups()
    micro = int((fraction or "0").ljust(6, "0")[:6])
    try:
        instant = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), micro, tzinfo=timezone.utc)
    except ValueError:
        return None
    if zone not in ("Z", "z"):
        digits = zone[1:].replace(":", "")
        offset = timedelta(hours=int(digits[:2]), minutes=int(digits[2:]))
        instant = instant - offset if zone[0] == "+" else instant + offset
    return instant


def iso(instant):
    return instant.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (instant.microsecond // 1000) if instant else None


def duration(seconds):
    if seconds < 120:
        return "%.2f s" % seconds
    if seconds < 2 * 86400:
        return "%.1f min" % (seconds / 60)
    return "%.1f days" % (seconds / 86400)


def scalars(value, path="properties"):
    """Yield (path, key, scalar) for every leaf of a JSON value."""
    if isinstance(value, dict):
        for key, child in value.items():
            if isinstance(child, (dict, list)):
                yield from scalars(child, "%s.%s" % (path, key))
            else:
                yield "%s.%s" % (path, key), str(key), child
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from scalars(child, "%s[%d]" % (path, index))
    else:
        yield path, path.rsplit(".", 1)[-1], value


def pii_hits(properties):
    hits = []
    for path, key, value in scalars(properties):
        if isinstance(value, str) and EMAIL.search(value):
            detector = "email"
        elif isinstance(value, str) and PHONE.match(value) and sum(c.isdigit() for c in value) >= 7 and (
                value.lstrip().startswith("+") or PHONE_KEY.search(key)):
            detector = "phone"
        elif PII_KEY.search(key) and value not in (None, "", False):
            detector = "key_name"
        else:
            continue
        hits.append({"field": safe(path), "detector": detector})
    return hits


def normalise(obj, line_no, raw_len):
    """Map a parsed record onto the canonical shape and list every deviation."""
    variants = []
    original = obj
    obj = dict(obj)
    for alias, canon in KEY_ALIASES.items():
        if alias in obj and canon not in obj:
            obj[canon] = obj.pop(alias)
            variants.append("top-level key '%s' used for '%s'" % (alias, canon))
    if isinstance(obj.get("type"), str) and obj["type"] in TYPE_ALIASES:
        variants.append("type '%s' used for '%s'" % (obj["type"], TYPE_ALIASES[obj["type"]]))
        obj["type"] = TYPE_ALIASES[obj["type"]]
    properties = obj.get("properties")
    if isinstance(properties, dict):
        properties = dict(properties)
        for alias, canon in PROPERTY_ALIASES.items():
            if alias in properties and canon not in properties:
                properties[canon] = properties.pop(alias)
                variants.append("property '%s' used for '%s'" % (alias, canon))
    else:
        if properties is not None:
            variants.append("properties is not an object")
        properties = {}
    for key in CANONICAL_KEYS:
        if key not in obj:
            variants.append("key '%s' absent" % key)
    for key in obj:
        if key not in CANONICAL_KEYS:
            variants.append("unknown top-level key '%s'" % safe(key, 32))
    ts, received = parse_ts(obj.get("ts")), parse_ts(obj.get("received_at"))
    lag = (received - ts).total_seconds() if ts and received else None
    # Event time used downstream: client time, unless it is ahead of the server
    # receipt or older than the allowed lateness, in which case receipt wins.
    if lag is not None and (lag < 0 or lag > MAX_EVENT_AGE_S):
        event_time = received
    else:
        event_time = ts or received
    return {
        "line": line_no, "raw_len": raw_len, "variants": variants,
        "event_id": obj.get("event_id"), "tenant_id": obj.get("tenant_id"),
        "anonymous_id": obj.get("anonymous_id"), "user_id": obj.get("user_id"),
        "type": obj.get("type"), "properties": properties,
        "ts": ts, "received_at": received, "lag": lag, "event_time": event_time,
        "ts_present": obj.get("ts") is not None, "received_present": obj.get("received_at") is not None,
        "fingerprint": json.dumps({k: v for k, v in original.items() if k != "received_at"}, sort_keys=True),
    }


def simulate_watermark(events, clamp):
    """Which records would an event-time consumer call late?

    Single partition, file order, watermark = max event time seen minus the
    bound. clamp=False trusts the raw client ts; clamp=True uses event_time.
    """
    late, high = [], None
    for event in events:
        when = event["event_time"] if clamp else (event["ts"] or event["received_at"])
        if when is None:
            continue
        if high is not None and when < high - timedelta(seconds=WATERMARK_BOUND_S):
            late.append(event)
        elif high is None or when > high:
            high = when
    return late


def analyze(lines):
    """Analyse an iterable of JSONL lines (str or bytes) and return the report dict."""
    anomalies, events = [], []
    line_classes = defaultdict(list)
    line_ids = {}

    def flag(event, cls, reason, certainty=None, confidence=None, **evidence):
        spec = CATALOG[cls]
        anomalies.append({
            "event_id": safe(event.get("event_id")), "line": event["line"], "class": cls,
            "certainty": certainty or spec["certainty"], "confidence": confidence or spec["confidence"],
            "reason": reason, "proposed_treatment": spec["treatment"],
            "false_positive_risk": spec["fp"], "evidence": evidence,
        })
        line_classes[event["line"]].append(cls)

    def ids(records):
        return [safe(r["event_id"]) for r in records]

    # ---- pass 1: parse, normalise, per-record checks, dedup ----
    total_lines, sizes, seen = 0, [], {}
    for line_no, line in enumerate(lines, 1):
        if isinstance(line, bytes):
            try:
                line = line.decode("utf-8")
            except UnicodeDecodeError:
                total_lines += 1
                line_ids[line_no] = None
                flag({"line": line_no}, "malformed_json", "Line %d is not valid UTF-8." % line_no, bytes=len(line))
                continue
        line = line.rstrip("\r\n")
        if not line.strip():
            continue
        total_lines += 1
        size = len(line.encode("utf-8"))
        sizes.append(size)
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError("not an object")
        except ValueError as exc:
            found = re.search(r'"event_id"\s*:\s*"([^"]*)"', line)
            stub = {"line": line_no, "event_id": found.group(1) if found else None}
            line_ids[line_no] = safe(stub["event_id"])
            try:
                closes = isinstance(json.loads(line + "}"), dict)
            except ValueError:
                closes = False
            where = " at column %d" % exc.colno if hasattr(exc, "colno") else ""
            reason = "Line %d is not valid JSON (%s%s)." % (line_no, type(exc).__name__, where)
            if closes:
                reason += " Appending one closing brace makes it parse, which shows the record was cut, not what was lost."
            flag(stub, "malformed_json", reason, bytes=size, parses_if_brace_appended=closes)
            continue
        event = normalise(obj, line_no, size)
        events.append(event)
        line_ids[line_no] = safe(event["event_id"])

        if event["variants"]:
            flag(event, "schema_variant", "Payload shape differs from the canonical event: %s." % "; ".join(event["variants"]),
                 variants=event["variants"])
        if not is_id(event["event_id"]):
            flag(event, "missing_event_id", "event_id is absent, null or empty: the record cannot be deduplicated as sent.")
        if not is_id(event["tenant_id"]):
            flag(event, "missing_tenant", "tenant_id is null or absent: the record cannot be attributed to a customer.")
        elif not TENANT_ID_PATTERN.match(event["tenant_id"]):
            flag(event, "malformed_tenant_id", "tenant_id does not have the shape of the other tenant ids.")
        if not is_id(event["anonymous_id"]) and not is_id(event["user_id"]):
            flag(event, "missing_identity", "Neither anonymous_id nor user_id is set.")
        if not is_id(event["type"]):
            flag(event, "unknown_event_type", "type is absent or not a string.")
        elif event["type"] not in KNOWN_TYPES:
            flag(event, "unknown_event_type", "type '%s' is not a known event type." % safe(event["type"], 32))
        if event["ts"] is None:
            flag(event, "invalid_timestamp", "Client timestamp is absent or not an RFC 3339 instant with an offset; received_at is %s."
                 % ("usable" if event["received_at"] else "not usable either"))
        if event["received_present"] and event["received_at"] is None:
            flag(event, "invalid_timestamp", "received_at is present but is not an RFC 3339 instant with an offset.")

        hits = pii_hits(event["properties"])
        if hits:
            proven = any(h["detector"] != "key_name" for h in hits)
            flag(event, "pii_in_properties",
                 "Personal data in free-form properties: %s. Values are withheld from this report."
                 % ", ".join("%s (%s)" % (h["field"], h["detector"]) for h in hits),
                 certainty="certain" if proven else "suspect", confidence="high" if proven else "medium", fields=hits)
        risky = [safe(path) for path, _, value in scalars(event["properties"]) if isinstance(value, str) and INSTRUCTION.search(value)]
        if risky:
            flag(event, "instruction_like_text", "Text shaped like an instruction to an automated reader in %s. Content withheld."
                 % ", ".join(risky), fields=risky)

        if is_id(event["event_id"]):
            key = (event["tenant_id"], event["event_id"])
            first = seen.get(key)
            if first is None:
                seen[key] = event
            elif first["fingerprint"] == event["fingerprint"]:
                event["duplicate_of"] = first["line"]
                delta = None
                if event["received_at"] and first["received_at"]:
                    delta = round((event["received_at"] - first["received_at"]).total_seconds(), 3)
                flag(event, "duplicate_delivery",
                     "Same (tenant_id, event_id) as line %d with an identical payload%s: a redelivery, not a second event."
                     % (first["line"], "" if delta is None else ", received %.3f s after the first copy" % delta),
                     first_line=first["line"], received_delta_seconds=delta)
            else:
                flag(event, "event_id_collision", "Same (tenant_id, event_id) as line %d but a different payload." % first["line"],
                     first_line=first["line"])

    unique = [e for e in events if "duplicate_of" not in e]
    # Per-subject indexes keep the second pass linear in the number of records.
    by_anon, by_user = defaultdict(list), defaultdict(list)
    for event in events:
        if is_id(event["anonymous_id"]):
            by_anon[(event["tenant_id"], event["anonymous_id"])].append(event)
        if is_id(event["user_id"]):
            by_user[(event["tenant_id"], event["user_id"])].append(event)

    # ---- pass 2: timestamps, with the visitor's other records as context ----
    normal_lag = defaultdict(list)
    for event in events:
        if event["lag"] is not None and -SKEW_TOLERANCE_S <= event["lag"] <= LATE_ARRIVAL_S and is_id(event["anonymous_id"]):
            normal_lag[(event["tenant_id"], event["anonymous_id"])].append(event["lag"])
    for event in events:
        lag = event["lag"]
        if lag is None:
            continue
        if lag < -SKEW_TOLERANCE_S:
            skew, notes = -lag, []
            siblings = normal_lag.get((event["tenant_id"], event["anonymous_id"]), [])
            if siblings:
                low, high = min(siblings) * 1000, max(siblings) * 1000
                spread = "%.0f ms" % low if low == high else "%.0f to %.0f ms" % (low, high)
                notes.append("the same visitor's %d other record(s) show a normal lag of %s, so this is not a constant device clock offset"
                             % (len(siblings), spread))
            days = round(skew / 86400)
            residual = days * 86400 - skew
            if days >= 1 and 0 <= residual <= LATE_ARRIVAL_S:
                notes.append("it is %d days ahead to within a normal lag of %.0f ms, which points at a wrong date field rather than a drifting clock"
                             % (days, residual * 1000))
            if event["ts"].microsecond == event["received_at"].microsecond:
                notes.append("both stamps carry the same sub-second part (1 chance in 1,000 by coincidence), so one was probably derived from the other rather than measured")
            flag(event, "future_timestamp", "Client ts is %s after the server receipt time%s."
                 % (duration(skew), "".join("; " + n for n in notes)),
                 skew_seconds=round(skew, 3), client_ts=iso(event["ts"]), received_at=iso(event["received_at"]))
        elif lag > LATE_ARRIVAL_S:
            stale = lag > MAX_EVENT_AGE_S
            flag(event, "late_arrival", "Received %s after its client timestamp%s."
                 % (duration(lag), ", older than the allowed lateness, so event time falls back to received_at" if stale else ""),
                 lag_seconds=round(lag, 3), stale=stale)

    latest_time = latest_receipt = None
    for event in events:
        when = event["event_time"]
        if when is not None:
            if latest_time is not None and (latest_time - when).total_seconds() > WATERMARK_BOUND_S:
                behind = (latest_time - when).total_seconds()
                note = ""
                if event["received_at"] and latest_receipt and event["received_at"] < latest_receipt:
                    note = "; its received_at is also %.3f s earlier than the latest receipt already seen, so the file is not in receipt order" % (
                        latest_receipt - event["received_at"]).total_seconds()
                flag(event, "out_of_order", "Event time is %s behind the latest event time already seen in file order%s."
                     % (duration(behind), note), behind_seconds=round(behind, 3))
            elif latest_time is None or when > latest_time:
                latest_time = when
        if event["received_at"] and (latest_receipt is None or event["received_at"] > latest_receipt):
            latest_receipt = event["received_at"]

    # ---- pass 2: automation bursts (same visitor, page views packed in a window) ----
    visits = defaultdict(list)
    for event in unique:
        if event["type"] == "page_view" and event["event_time"] and is_id(event["anonymous_id"]):
            visits[(event["tenant_id"], event["anonymous_id"])].append(event)
    for group in visits.values():
        group.sort(key=lambda e: e["event_time"])
        start = 0
        while start < len(group):
            end = start
            while end + 1 < len(group) and (group[end + 1]["event_time"] - group[start]["event_time"]).total_seconds() <= BURST_WINDOW_S:
                end += 1
            burst = group[start:end + 1]
            if len(burst) < BURST_MIN_EVENTS:
                start += 1
                continue
            span = (burst[-1]["event_time"] - burst[0]["event_time"]).total_seconds()
            gaps = [round((b["event_time"] - a["event_time"]).total_seconds() * 1000) for a, b in zip(burst, burst[1:])]
            paths = {str(b["properties"].get("path")) for b in burst}
            hosts = set()
            for b in burst:
                found = re.match(r"https?://([^/?#]+)", str(b["properties"].get("referrer") or ""))
                if found:
                    hosts.add(safe(found.group(1)))
            hint = sorted(h for h in hosts if AUTOMATION_HINT.search(h))
            reason = "%d page views on %d distinct path(s) within %.0f ms by one visitor (gaps %s ms): not human navigation." % (
                len(burst), len(paths), span * 1000, ", ".join(str(g) for g in gaps[:10]))
            if hint:
                reason += " The referrer host %s is a weak supporting hint only, because the browser supplies it." % ", ".join(hint)
            for member in burst:
                flag(member, "automation_burst", reason, confidence="high" if span / (len(burst) - 1) < 0.2 else "medium",
                     burst_event_ids=ids(burst), span_ms=round(span * 1000), gaps_ms=gaps[:10],
                     distinct_paths=len(paths), referrer_hosts=sorted(hosts), referrer_hint=bool(hint))
            start = end + 1

    # ---- pass 2: identity ----
    links = defaultdict(dict)           # (tenant, anonymous_id) -> {user_id: link}
    anons_of = defaultdict(set)         # (tenant, user_id) -> anonymous ids
    first_tenant = {}
    for event in unique:
        tenant, anon, user = event["tenant_id"], event["anonymous_id"], event["user_id"]
        if is_id(tenant):
            for kind, value in (("anonymous_id", anon), ("user_id", user)):
                if is_id(value):
                    known = first_tenant.setdefault((kind, value), tenant)
                    if known != tenant and not event.get("cross_flagged"):
                        event["cross_flagged"] = True
                        flag(event, "cross_tenant_identifier", "%s %s also appears under tenant %s: the two are unrelated unless proven otherwise."
                             % (kind, safe(value), safe(known)), identifier_kind=kind, other_tenant=safe(known))
        if is_id(anon) and is_id(user):
            link = links[(tenant, anon)].setdefault(user, {"first": event, "explicit": False, "privacy_only": True})
            anons_of[(tenant, user)].add(anon)
            link["explicit"] = link["explicit"] or event["type"] == "identify"
            link["privacy_only"] = link["privacy_only"] and event["type"] == "privacy_request"
    identity = []
    for (tenant, anon), users in links.items():
        for position, (user, link) in enumerate(users.items()):
            first = link["first"]
            before = [e for e in by_anon[(tenant, anon)] if "duplicate_of" not in e
                      and not is_id(e["user_id"]) and e["line"] < first["line"]]
            identity.append({
                "tenant_id": safe(tenant), "anonymous_id": safe(anon), "user_id": safe(user),
                "linked_by_event": safe(first["event_id"]), "linked_at_line": first["line"],
                "explicit_identify": link["explicit"], "earlier_anonymous_events": ids(before),
            })
            if position > 0:
                flag(first, "identity_conflict", "anonymous_id %s is already linked to another user in this tenant (%s)."
                     % (safe(anon), ", ".join(safe(u) for u in list(users)[:position])), users=[safe(u) for u in users])
            if not link["explicit"]:
                flag(first, "identity_link_without_identify",
                     "user_id %s is attached to anonymous_id %s with no identify event for that pair in the file%s."
                     % (safe(user), safe(anon), ": the link is asserted only by the privacy request itself" if link["privacy_only"] else ""),
                     asserted_only_by_privacy_request=link["privacy_only"])

    def subject_of(tenant, anon, user):
        """A subject inside one tenant: the user, the visitor ids linked to that user, and the visitor itself.

        It never expands to another user. A visitor id also linked to another user is a shared device."""
        users = {user} if is_id(user) else set()
        anons = {anon} if is_id(anon) else set()
        for u in users:
            anons |= anons_of.get((tenant, u), set())
        shared = {a for a in anons if set(links.get((tenant, a), ())) - users}
        return anons, users, shared

    def records_of(tenant, anons, users, shared=frozenset()):
        """(the subject's records, ambiguous records), each in file order, one tenant only.

        A record carrying a user id belongs to the subject only if that user is the subject. An anonymous
        record from a shared device is ambiguous: a human decides, the pipeline does not."""
        theirs, ambiguous = {}, {}
        for anon in anons:
            for record in by_anon.get((tenant, anon), ()):
                if is_id(record["user_id"]):
                    if record["user_id"] in users:
                        theirs[record["line"]] = record
                else:
                    (ambiguous if anon in shared else theirs)[record["line"]] = record
        for user in users:
            for record in by_user.get((tenant, user), ()):
                theirs[record["line"]] = record
        return [theirs[k] for k in sorted(theirs)], [ambiguous[k] for k in sorted(ambiguous)]

    def unique_ids(records):
        return list(dict.fromkeys(ids(records)))

    # ---- pass 2: privacy requests ----
    privacy = []
    for event in unique:
        request = event["properties"].get("request")
        if event["type"] != "privacy_request" and not (isinstance(request, str) and PRIVACY_WORDS.search(request)):
            continue
        tenant = event["tenant_id"]
        anons, users, shared = subject_of(tenant, event["anonymous_id"], event["user_id"])
        theirs, ambiguous = records_of(tenant, anons, users, shared)
        affected = [e for e in theirs if e is not event]
        missed_by_user = [e for e in affected if e["user_id"] not in users]
        missed_by_time = [e for e in affected if e["ts"] and event["ts"] and e["ts"] > event["ts"]
                          and e["received_at"] and event["received_at"] and e["received_at"] <= event["received_at"]]
        reason = "A %s message (%s claimed) for user %s, visitor %s, tenant %s travels in the behavioural stream. %d other record(s) in this file belong to the subject%s." % (
            safe(request) or "privacy", safe(event["properties"].get("regulation")) or "no regulation",
            safe(event["user_id"]), safe(event["anonymous_id"]), safe(tenant), len(unique_ids(affected)),
            ": " + ", ".join(unique_ids(affected)) if affected else "")
        if missed_by_user:
            reason += " A delete keyed on user_id alone would miss %s (anonymous at the time)." % ", ".join(unique_ids(missed_by_user))
        if missed_by_time:
            reason += " A delete bounded by client time would miss %s, whose client ts is later than the request although it was received earlier." % ", ".join(unique_ids(missed_by_time))
        if ambiguous:
            reason += " %s: anonymous record(s) from a device shared with another user, left for a human to decide." % ", ".join(unique_ids(ambiguous))
        record = {
            "event_id": safe(event["event_id"]), "line": event["line"], "tenant_id": safe(tenant),
            "request": safe(request), "regulation_claimed": safe(event["properties"].get("regulation")),
            "subject_user_ids": sorted(safe(u) for u in users), "subject_anonymous_ids": sorted(safe(a) for a in anons),
            "affected_events_in_file": unique_ids(affected),
            "ambiguous_shared_device": unique_ids(ambiguous),
            "missed_by_user_id_only_delete": unique_ids(missed_by_user),
            "missed_by_client_time_bound": unique_ids(missed_by_time),
            "cutoff_received_at": iso(event["received_at"]),
        }
        privacy.append(record)
        flag(event, "privacy_request", reason, **{k: v for k, v in record.items() if k not in ("event_id", "line")})

    # ---- pass 2: counters the browser reports about itself ----
    for event in unique:
        for key, value in event["properties"].items():
            if not AGGREGATE_KEY.search(str(key)) or isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            evidence = {"property": safe(key), "claimed": value}
            reason = "Property %s=%s is a counter computed by the browser." % (safe(key), value)
            name = event["properties"].get("name")
            target = re.match(r"^viewed_([a-z0-9_]+)$", name) if isinstance(name, str) else None
            if target:
                path = "/" + target.group(1)
                anons, users, shared = subject_of(event["tenant_id"], event["anonymous_id"], event["user_id"])
                mine = [e for e in records_of(event["tenant_id"], anons, users, shared)[0] if e["line"] < event["line"]]
                views = [e for e in mine if "duplicate_of" not in e and e["type"] == "page_view" and e["properties"].get("path") == path]
                naive = [e for e in mine if e["properties"].get("path") == path]
                reason += " Before this record the file holds %d deduplicated page view(s) of %s for the same visitor (%s). Counting every line that mentions %s, duplicates included, gives %d (%s)." % (
                    len(views), path, ", ".join(ids(views)) or "none", path, len(naive), ", ".join(ids(naive)) or "none")
                evidence.update(path=path, server_side_page_views=len(views), naive_line_count=len(naive))
            flag(event, "client_supplied_aggregate", reason, **evidence)

    # ---- routing: every line is accounted for exactly once ----
    routing = {"accept": [], "accept_flagged": [], "drop_duplicate": [], "quarantine": [], "privacy_queue": []}
    for line_no in sorted(line_ids):
        classes = set(line_classes.get(line_no, ()))
        if classes & QUARANTINE:
            decision = "quarantine"
        elif "duplicate_delivery" in classes:
            decision = "drop_duplicate"
        elif "privacy_request" in classes:
            decision = "privacy_queue"
        elif classes:
            decision = "accept_flagged"
        else:
            decision = "accept"
        routing[decision].append({"line": line_no, "event_id": line_ids[line_no]})

    naive_late, clamped_late = simulate_watermark(events, clamp=False), simulate_watermark(events, clamp=True)
    by_class = Counter(a["class"] for a in anomalies)
    ids_by_class = defaultdict(dict)    # dict used as an ordered set
    for a in anomalies:
        ids_by_class[a["class"]][a["event_id"]] = None
    ids_by_class = {cls: list(found) for cls, found in ids_by_class.items()}
    lags_ms = sorted(e["lag"] * 1000 for e in events if e["lag"] is not None and -SKEW_TOLERANCE_S <= e["lag"] <= LATE_ARRIVAL_S)
    traps = [{"class": cls, "event_ids": ids_by_class[cls], "why": spec["trap"]}
             for cls, spec in CATALOG.items() if spec["trap"] and cls in ids_by_class]
    first_ts = min((e["event_time"] for e in events if e["event_time"]), default=None)
    return {
        "assumptions": {
            "skew_tolerance_s": SKEW_TOLERANCE_S, "late_arrival_s": LATE_ARRIVAL_S, "max_event_age_s": MAX_EVENT_AGE_S,
            "watermark_bound_s": WATERMARK_BOUND_S, "burst_min_events": BURST_MIN_EVENTS, "burst_window_s": BURST_WINDOW_S,
        },
        "summary": {
            "lines": total_lines, "parsed": len(events), "unique_events": len(unique),
            "lines_with_anomaly": len(line_classes), "clean_lines": total_lines - len(line_classes),
            "anomalies": len(anomalies),
            "by_certainty": dict(Counter(a["certainty"] for a in anomalies)),
            "by_class": dict(by_class), "event_ids_by_class": dict(ids_by_class),
            "routing_counts": {k: len(v) for k, v in routing.items()},
            "tenants": dict(Counter(safe(e["tenant_id"]) or "null" for e in events)),
            "bytes_per_line": {"min": min(sizes), "mean": round(statistics.mean(sizes), 1), "max": max(sizes)} if sizes else {},
            "normal_lag_ms": {"n": len(lags_ms), "min": round(lags_ms[0]), "median": round(statistics.median(lags_ms)),
                              "max": round(lags_ms[-1])} if lags_ms else {},
        },
        "anomalies": anomalies,
        "do_not_act_at_face_value": traps,
        "routing": routing,
        "identity": identity,
        "privacy_requests": privacy,
        "watermark_simulation": {
            "method": "single partition, file order, watermark = max event time seen minus %.0f s, no allowed lateness" % WATERMARK_BOUND_S,
            "parsed_records": len(events),
            "late_if_client_ts_trusted": ids(naive_late),
            "late_if_event_time_clamped_to_receipt": ids(clamped_late),
        },
        "sample_caveats": [
            "%d lines of seeded synthetic data: the shares of duplicates, automation or malformed records in this file are not estimates of production rates." % total_lines,
            "The file is a snapshot that starts at %s: counters, identity links and anything defined over 'today' cannot be checked against earlier activity." % (iso(first_ts) or "an unknown time"),
            "Request metadata (write key, IP, user agent, Origin) is absent, so tenant recovery and the stronger automation checks cannot run here.",
        ],
    }


def render_markdown(report):
    s, fx = report["summary"], report.get("fixture", {})

    def table(header, rows):
        out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
        out += ["| " + " | ".join(str(c).replace("|", "/") for c in row) + " |" for row in rows]
        return "\n".join(out)

    def idlist(values):
        return ", ".join(str(v) for v in values) or "none"

    parts = [
        "# Anomaly report",
        "Generated by `analyze_fixture.py` %s. Do not edit by hand: re-run the script." % report.get("tool", {}).get("version", ""),
        "- Fixture: `%s`, SHA-256 `%s`, %s bytes" % (fx.get("file"), fx.get("sha256"), fx.get("bytes")),
        "- Lines: %d, parsed as JSON: %d, unique (tenant_id, event_id): %d" % (s["lines"], s["parsed"], s["unique_events"]),
        "- Lines with at least one anomaly: %d, clean lines: %d" % (s["lines_with_anomaly"], s["clean_lines"]),
        "- Anomalies: %d (%s)" % (s["anomalies"], ", ".join("%d %s" % (n, c) for c, n in sorted(s["by_certainty"].items()))),
        "\n## What the gate does with each line\n\nEvery line is accounted for exactly once.\n",
        table(["decision", "lines", "event ids"], [(k, len(v), idlist(x["event_id"] for x in v)) for k, v in report["routing"].items()]),
        "\n## Anomalies by class\n",
        table(["class", "count", "event ids"], [(c, n, idlist(s["event_ids_by_class"][c])) for c, n in s["by_class"].items()]),
        "\n## Each anomaly\n\n`certainty` says whether the observation is a measured fact (certain) or an inference (suspect). `confidence` is how sure the reading and the treatment are.\n",
        table(["event_id", "line", "class", "certainty", "confidence", "reason"],
              [(a["event_id"], a["line"], a["class"], a["certainty"], a["confidence"], a["reason"]) for a in report["anomalies"]]),
        "\n## Proposed treatment and false-positive risk\n",
        table(["class", "proposed treatment", "false-positive risk"],
              [(c, CATALOG[c]["treatment"], CATALOG[c]["fp"]) for c in s["by_class"]]),
        "\n## Do not act on these at face value\n",
        "\n".join("- **%s** (%s): %s" % (t["class"], idlist(t["event_ids"]), t["why"]) for t in report["do_not_act_at_face_value"]) or "- none",
        "\n## Identity events\n",
        table(["tenant", "anonymous_id", "user_id", "linked by", "explicit identify", "earlier anonymous events"],
              [(i["tenant_id"], i["anonymous_id"], i["user_id"], i["linked_by_event"], "yes" if i["explicit_identify"] else "no",
                idlist(i["earlier_anonymous_events"])) for i in report["identity"]]),
        "\nLinks are scoped to one tenant. Earlier anonymous events are candidates for attribution at query time, not rewritten.",
        "\n## Privacy requests\n",
        "\n".join("- **%s** (line %d, tenant %s): request `%s`, regulation claimed `%s`. Subject ids: users %s, visitors %s. Records of the subject in this file: %s. Ambiguous, shared device: %s. Missed by a user_id-only delete: %s. Missed by a client-time bound: %s. Deletion cutoff (server receipt): %s."
                  % (p["event_id"], p["line"], p["tenant_id"], p["request"], p["regulation_claimed"], idlist(p["subject_user_ids"]),
                     idlist(p["subject_anonymous_ids"]), idlist(p["affected_events_in_file"]), idlist(p["ambiguous_shared_device"]),
                     idlist(p["missed_by_user_id_only_delete"]), idlist(p["missed_by_client_time_bound"]), p["cutoff_received_at"]) for p in report["privacy_requests"]) or "- none",
        "\n## Watermark simulation\n",
        "Method: %s." % report["watermark_simulation"]["method"],
        "- Trusting the raw client ts: %d of %d parsed records are classified late: %s" % (
            len(report["watermark_simulation"]["late_if_client_ts_trusted"]), report["watermark_simulation"]["parsed_records"],
            idlist(report["watermark_simulation"]["late_if_client_ts_trusted"])),
        "- With event time clamped to the server receipt: %d: %s" % (
            len(report["watermark_simulation"]["late_if_event_time_clamped_to_receipt"]),
            idlist(report["watermark_simulation"]["late_if_event_time_clamped_to_receipt"])),
        "\n## Measured on the sample\n",
        "- Bytes per line: min %(min)s, mean %(mean)s, max %(max)s" % s["bytes_per_line"] if s["bytes_per_line"] else "- No line to measure",
        "- Lag from client ts to server receipt on the %(n)s records with a plausible lag: min %(min)s ms, median %(median)s ms, max %(max)s ms"
        % s["normal_lag_ms"] if s["normal_lag_ms"] else "- No record with a plausible lag",
        "- Lines per tenant: %s" % ", ".join("%s: %d" % kv for kv in s["tenants"].items()),
        "\n## Limits of this sample\n",
        "\n".join("- " + c for c in report["sample_caveats"]),
    ]
    return "\n".join(parts) + "\n"


def bench(raw, count):
    """Throughput of analyze() on synthetic visitors cloned from the fixture.

    Clones keep the fixture's timestamps, so only the speed is meaningful here.
    """
    templates = []
    for line in raw.decode("utf-8", errors="replace").split("\n"):
        try:
            templates.append(json.loads(line))
        except ValueError:
            pass
    lines, replica = [], 0
    while len(lines) < count:
        for template in templates:
            clone = dict(template)
            for key in ("event_id", "anonymous_id", "user_id"):
                if clone.get(key):
                    clone[key] = "%s-%d" % (clone[key], replica)
            lines.append(json.dumps(clone, separators=(",", ":")))
        replica += 1
    lines = lines[:count]
    started = time.perf_counter()
    analyze(lines)
    elapsed = time.perf_counter() - started
    print("%d synthetic events, %.2f s, %s events/s on one core (Python %s, %s %s)" % (
        count, elapsed, format(round(count / elapsed), ","), platform.python_version(), platform.system(), platform.machine()))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE, help="JSONL file to analyse")
    parser.add_argument("--out-dir", type=Path, default=HERE / "artifacts", help="where the two reports are written")
    parser.add_argument("--expect-sha256", help="exit with code 3 if the fixture checksum differs")
    parser.add_argument("--bench", type=int, metavar="N", help="time the gate on N synthetic events and exit")
    args = parser.parse_args(argv)
    if not args.fixture.exists():
        print("fixture not found: %s. Run python3 tools/fetch_fixture.py first: the official file is not redistributed here."
              % args.fixture, file=sys.stderr)
        return 2
    raw = args.fixture.read_bytes()
    if args.bench:
        return bench(raw, args.bench)
    digest = hashlib.sha256(raw).hexdigest()
    report = {"tool": {"name": "analyze_fixture.py", "version": VERSION},
              "fixture": {"file": args.fixture.name, "sha256": digest, "bytes": len(raw)}}
    report.update(analyze(raw.split(b"\n")))
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "anomaly_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (args.out_dir / "anomaly_report.md").write_text(render_markdown(report), encoding="utf-8")
    s = report["summary"]
    print("fixture  %s  sha256 %s" % (args.fixture.name, digest))
    print("lines %d, parsed %d, anomalies %d on %d lines, clean lines %d" % (
        s["lines"], s["parsed"], s["anomalies"], s["lines_with_anomaly"], s["clean_lines"]))
    for cls, n in s["by_class"].items():
        print("  %-32s %2d  %s" % (cls, n, ", ".join(str(i) for i in s["event_ids_by_class"][cls])))
    print("routing  " + ", ".join("%s=%d" % kv for kv in s["routing_counts"].items()))
    print("reports  %s" % args.out_dir)
    if args.expect_sha256 and digest != args.expect_sha256.lower():
        print("WARNING: fixture checksum differs from the expected one: the written answer may describe another fixture.", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
