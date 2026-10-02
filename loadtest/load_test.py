#!/usr/bin/env python3
"""End-to-end throughput test: API -> Kafka -> orchestrator -> Kafka -> workers -> Redis/Postgres.

Each workflow is a fan-out/fan-in DAG: start -> (WIDTH parallel tasks) -> end.
Throughput comes from the event store (ground truth); latency percentiles from Prometheus histograms
over exactly the test window. Every run is appended to loadtest/results.jsonl.

Example: python3 loadtest/load_test.py --workflows 1000 --width 20 --label baseline
"""
import argparse
import concurrent.futures
import json
import math
import subprocess
import time
import urllib.parse
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--workflows", type=int, default=1000)
p.add_argument("--width", type=int, default=20, help="parallel tasks between start and end")
p.add_argument("--type", default="noop")
p.add_argument("--label", required=True, help="e.g. baseline, batched")
p.add_argument("--concurrency", type=int, default=64, help="parallel HTTP submitters")
p.add_argument("--api", default="http://localhost:8080/api/v1")
p.add_argument("--prometheus", default="http://localhost:9090")
p.add_argument("--max-wait", type=float, default=600)
args = p.parse_args()

name = f"load-{args.label}-{int(time.time())}"
tasks_per_wf = args.width + 2


def sql(query):
    out = subprocess.run(["docker", "exec", "ff-postgres", "psql", "-U", "flowforge", "-d", "flowforge", "-tAc", query],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def post(url, body, headers):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", **headers})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def prom(query, at):
    url = f"{args.prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": f"{at:.3f}"})
    with urllib.request.urlopen(url, timeout=30) as resp:
        result = json.load(resp)["data"]["result"]
    if not result:
        return None
    if len(result) == 1:
        v = float(result[0]["value"][1])
        return None if math.isnan(v) else v
    return {r["metric"].get("application", "?"): float(r["value"][1]) for r in result}


definition = {"name": name, "tasks":
              [{"id": "start", "type": args.type}]
              + [{"id": f"t{i}", "type": args.type, "dependsOn": ["start"]} for i in range(args.width)]
              + [{"id": "end", "type": args.type, "dependsOn": [f"t{i}" for i in range(args.width)]}]}

token = post(f"{args.api}/auth/token", {"username": "alice", "password": "alice-pass"}, {})["accessToken"]
auth = {"Authorization": f"Bearer {token}"}

print(f"[{name}] submitting {args.workflows} workflows x {tasks_per_wf} tasks = {args.workflows * tasks_per_wf:,} tasks")
wall_start = time.time()
with concurrent.futures.ThreadPoolExecutor(args.concurrency) as pool:
    list(pool.map(lambda _: post(f"{args.api}/workflows", definition, auth), range(args.workflows)))
submit_s = time.time() - wall_start
print(f"  submitted in {submit_s:.1f}s ({args.workflows / submit_s:.0f} workflows/s)")

last_print = 0
while True:
    done = int(sql(f"SELECT count(*) FROM workflow_runs WHERE name = '{name}' AND status <> 'RUNNING'"))
    if done >= args.workflows:
        break
    if time.time() - wall_start > args.max_wait:
        print(f"  TIMED OUT with {done}/{args.workflows} finished")
        break
    if time.time() - last_print > 5:
        print(f"  {time.time() - wall_start:5.0f}s  finished {done}/{args.workflows}")
        last_print = time.time()
    time.sleep(1)

time.sleep(7)  # let Prometheus scrape the final counters
now = time.time()
w = int(now - wall_start) + 2

first, last = sql(f"SELECT extract(epoch FROM min(created_at)), extract(epoch FROM max(updated_at)) "
                  f"FROM workflow_runs WHERE name = '{name}'").split("|")
elapsed = float(last) - float(first)
succeeded = int(sql(f"SELECT count(*) FROM workflow_events e JOIN workflow_runs r USING (workflow_id) "
                    f"WHERE r.name = '{name}' AND e.event_type = 'TaskSucceeded'"))
statuses = sql(f"SELECT string_agg(status || '=' || c, ' ') FROM (SELECT status, count(*) c "
               f"FROM workflow_runs WHERE name = '{name}' GROUP BY status) s")
duplicates = int(sql(f"SELECT count(*) FROM (SELECT e.workflow_id, e.payload->>'taskId' FROM workflow_events e "
                     f"JOIN workflow_runs r USING (workflow_id) WHERE r.name = '{name}' AND e.event_type = 'TaskSucceeded' "
                     f"GROUP BY 1, 2 HAVING count(*) > 1) d"))
retries = int(sql(f"SELECT count(*) FROM workflow_events e JOIN workflow_runs r USING (workflow_id) "
                  f"WHERE r.name = '{name}' AND e.event_type = 'TaskFailed'"))


def q(metric, quantile):
    v = prom(f"histogram_quantile({quantile}, sum by (le) (increase({metric}_bucket[{w}s])))", now)
    return None if v is None else round(v * 1000, 1)


result = {
    "label": args.label, "run": name, "workflows": args.workflows, "tasks_per_workflow": tasks_per_wf,
    "task_type": args.type, "statuses": statuses, "tasks_succeeded": succeeded,
    "duplicate_successes": duplicates, "failed_attempts": retries,
    "elapsed_s": round(elapsed, 2),
    "tasks_per_s": round(succeeded / elapsed, 1),
    "workflows_per_s": round(args.workflows / elapsed, 1),
    "peak_tasks_per_s": prom(f'max_over_time(sum(rate(flowforge_task_events_total{{event="succeeded"}}[10s]))[{w}s:2s])', now),
    "peak_active_workflows": prom(f"max_over_time(sum(flowforge_workflows_active)[{w}s:1s])", now),
    "peak_worker_in_flight": prom(f"max_over_time(sum(flowforge_worker_in_flight)[{w}s:1s])", now),
    "peak_kafka_lag": prom(f"max_over_time(max by (application) (kafka_consumer_fetch_manager_records_lag_max)[{w}s:5s])", now),
    "turnaround_ms": {f"p{int(x * 100)}": q("flowforge_task_turnaround_seconds", x) for x in (0.5, 0.95, 0.99)},
    "dispatch_ms": {f"p{int(x * 100)}": q("flowforge_task_dispatch_latency_seconds", x) for x in (0.5, 0.95, 0.99)},
    "append_ms": {f"p{int(x * 100)}": q("flowforge_eventstore_append_seconds", x) for x in (0.5, 0.95, 0.99)},
    "workflow_ms": {f"p{int(x * 100)}": q("flowforge_workflow_duration_seconds", x) for x in (0.5, 0.95, 0.99)},
    "peak_process_cpu": prom(f"max_over_time(max by (application) (process_cpu_usage)[{w}s:5s])", now),
    "peak_system_cpu": prom(f"max_over_time(max(system_cpu_usage)[{w}s:5s])", now),
}
with open("loadtest/results.jsonl", "a") as f:
    f.write(json.dumps(result) + "\n")

r = result
print(f"""
================= {r['run']} =================
statuses                 : {r['statuses']}
tasks succeeded          : {r['tasks_succeeded']:,} / {args.workflows * tasks_per_wf:,}
duplicate successes      : {r['duplicate_successes']}   failed attempts (retries): {r['failed_attempts']}
elapsed (first start -> last finish) : {r['elapsed_s']} s
THROUGHPUT (average)     : {r['tasks_per_s']:,} tasks/s   ({r['workflows_per_s']} workflows/s)
throughput (peak 10s)    : {r['peak_tasks_per_s'] and round(r['peak_tasks_per_s'], 1)} tasks/s
peak active workflows    : {r['peak_active_workflows']}
peak worker in-flight    : {r['peak_worker_in_flight']}
peak Kafka lag (records) : {r['peak_kafka_lag']}
task turnaround   ms     : {r['turnaround_ms']}
dispatch latency  ms     : {r['dispatch_ms']}
Postgres append   ms     : {r['append_ms']}
workflow duration ms     : {r['workflow_ms']}
peak CPU (per JVM, 0-1)  : {r['peak_process_cpu']}
peak system CPU (0-1)    : {r['peak_system_cpu']}
(appended to loadtest/results.jsonl)""")
