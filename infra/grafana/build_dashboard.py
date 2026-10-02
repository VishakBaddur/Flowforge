#!/usr/bin/env python3
"""Generates infra/grafana/dashboards/flowforge.json (dashboard as code). Re-run after editing."""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}
RATE = "$__rate_interval"
panels = []


def target(i, expr, legend):
    return {"refId": chr(65 + i), "expr": expr, "legendFormat": legend, "datasource": DS}


def stat(title, expr, x, y, unit="short", w=4, h=4, decimals=1, red_above=None):
    steps = [{"color": "green", "value": None}]
    if red_above is not None:
        steps.append({"color": "red", "value": red_above})
    panels.append({
        "type": "stat", "title": title, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(0, expr, "")],
        "fieldConfig": {"defaults": {"unit": unit, "decimals": decimals,
                                     "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"},
    })


def series(title, queries, x, y, unit="short", w=12, h=8, stack=False):
    panels.append({
        "type": "timeseries", "title": title, "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(i, e, l) for i, (e, l) in enumerate(queries)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "fillOpacity": 15, "lineWidth": 2, "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max"]},
                    "tooltip": {"mode": "multi"}},
    })


def quantiles(metric, by=""):
    group = f"le{', ' + by if by else ''}"
    label = f"{{{{{by}}}}} " if by else ""
    return [(f"histogram_quantile({q}, sum by ({group}) (rate({metric}_bucket[{RATE}])))", f"{label}p{int(q * 100)}")
            for q in (0.5, 0.95, 0.99)]


# Row 0: headline numbers
stat("Tasks succeeded / s", f'sum(rate(flowforge_task_events_total{{event="succeeded"}}[{RATE}]))', 0, 0, "ops")
stat("Workflows completed / s", f'sum(rate(flowforge_workflows_finished_total{{status="COMPLETED"}}[{RATE}]))', 4, 0, "ops")
stat("Active workflows", "sum(flowforge_workflows_active)", 8, 0, decimals=0)
stat("Worker tasks in flight", "sum(flowforge_worker_in_flight)", 12, 0, decimals=0)
stat("Dead-lettered (total)", 'sum(flowforge_task_events_total{event="dead_lettered"})', 16, 0, decimals=0, red_above=1)
stat("Redis degraded", "max(flowforge_worker_redis_degraded)", 20, 0, decimals=0, red_above=1)

# Throughput
series("Task events / s", [(f"sum by (event) (rate(flowforge_task_events_total[{RATE}]))", "{{event}}")], 0, 4, "ops")
series("Workflows finished / s", [(f"sum by (status) (rate(flowforge_workflows_finished_total[{RATE}]))", "{{status}}")],
       12, 4, "ops", stack=True)

# Latency
series("Task turnaround (queued → succeeded)", quantiles("flowforge_task_turnaround_seconds"), 0, 12, "s")
series("Dispatch latency (queued → started)", quantiles("flowforge_task_dispatch_latency_seconds"), 12, 12, "s")
series("Event-store append (Postgres txn per decision)", quantiles("flowforge_eventstore_append_seconds"), 0, 20, "s")
series("Worker execution p99 by task type",
       [(f"histogram_quantile(0.99, sum by (le, type) (rate(flowforge_worker_task_execution_seconds_bucket[{RATE}])))",
         "{{type}}")], 12, 20, "s")

# Kafka + reliability
series("Kafka consumer lag (max records behind)",
       [("max by (application) (kafka_consumer_fetch_manager_records_lag_max)", "{{application}}")], 0, 28)
series("Idempotency at work / s", [
    (f"sum by (kind) (rate(flowforge_inputs_ignored_total[{RATE}]))", "ignored: {{kind}}"),
    (f"sum(rate(flowforge_worker_cache_replays_total[{RATE}]))", "worker: cached result replayed"),
    (f"sum(rate(flowforge_worker_claims_skipped_total[{RATE}]))", "worker: claim held elsewhere"),
    (f"sum(rate(flowforge_concurrency_conflicts_total[{RATE}]))", "event store: concurrency conflict"),
], 12, 28, "ops")
series("Recovery: state rebuild time (max)", [("max(flowforge_recovery_duration_seconds_max)", "rebuild time")], 0, 36, "s")
series("Workflow duration", quantiles("flowforge_workflow_duration_seconds"), 12, 36, "s")

dashboard = {
    "uid": "flowforge", "title": "Flowforge", "tags": ["flowforge"], "schemaVersion": 39, "version": 1,
    "time": {"from": "now-15m", "to": "now"}, "refresh": "5s", "panels": panels,
}
out = pathlib.Path(__file__).parent / "dashboards" / "flowforge.json"
out.write_text(json.dumps(dashboard, indent=2))
print(f"wrote {out} ({len(panels)} panels)")
