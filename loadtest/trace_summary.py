#!/usr/bin/env python3
"""Find the largest recent Flowforge trace in Jaeger (v3 API) and print its span tree.

A full workflow trace should contain spans from flowforge-api, flowforge-orchestrator and flowforge-worker.
"""
import collections
import datetime as dt
import json
import urllib.parse
import urllib.request

JAEGER = "http://localhost:16686"


def get(path, **params):
    url = JAEGER + path + ("?" + urllib.parse.urlencode(params) if params else "")
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def spans_of(payload):
    """Flatten OTLP JSON (resourceSpans -> scopeSpans -> spans), tagging each span with its service."""
    out = []
    for rs in payload.get("result", {}).get("resourceSpans", []):
        attrs = {a["key"]: a["value"].get("stringValue") for a in rs.get("resource", {}).get("attributes", [])}
        service = attrs.get("service.name", "?")
        for ss in rs.get("scopeSpans", []):
            for s in ss.get("spans", []):
                s["_service"] = service
                out.append(s)
    return out


now = dt.datetime.now(dt.timezone.utc)
fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
found = get("/api/v3/traces", **{
    "query.service_name": "flowforge-api",
    "query.start_time_min": (now - dt.timedelta(minutes=30)).strftime(fmt),
    "query.start_time_max": now.strftime(fmt),
    "query.search_depth": 50,
})
by_trace = collections.defaultdict(list)
for s in spans_of(found):
    by_trace[s["traceId"]].append(s)
if not by_trace:
    raise SystemExit("no traces found for flowforge-api in the last 30 minutes")

# Re-fetch the most complete candidate in full (search results can be partial).
def score(spans):
    return (len({s["_service"] for s in spans}), len(spans))

trace_id = max(by_trace, key=lambda t: score(by_trace[t]))
spans = spans_of(get(f"/api/v3/traces/{trace_id}"))

services = collections.Counter(s["_service"] for s in spans)
print(f"trace {trace_id}: {len(spans)} spans across {dict(services)}")

children = collections.defaultdict(list)
ids = {s["spanId"] for s in spans}
for s in spans:
    parent = s.get("parentSpanId") or ""
    children[parent if parent in ids else "ROOT"].append(s)
for kids in children.values():
    kids.sort(key=lambda s: int(s["startTimeUnixNano"]))

t0 = min(int(s["startTimeUnixNano"]) for s in spans)
lines = []


def walk(span, depth):
    start_ms = (int(span["startTimeUnixNano"]) - t0) / 1e6
    dur_ms = (int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"])) / 1e6
    lines.append(f"{'  ' * depth}{span['name']}  [{span['_service'].replace('flowforge-', '')}]"
                 f"  +{start_ms:.0f}ms ({dur_ms:.1f}ms)")
    for c in children.get(span["spanId"], []):
        walk(c, depth + 1)


for root in children["ROOT"]:
    walk(root, 0)
noise = ("security", "authorize", "secured")
shown = [l for l in lines if not any(n in l.lower() for n in noise)]
print(f"\nspan tree ({len(shown)} of {len(lines)} lines; Spring Security filter spans hidden):")
print("\n".join(shown[:60]))
if len(shown) > 60:
    print(f"... {len(shown) - 60} more")
print(f"\nopen: {JAEGER}/trace/{trace_id}")
