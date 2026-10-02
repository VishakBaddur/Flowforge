#!/usr/bin/env python3
"""Kill an orchestrator in the middle of a run and measure failover.

Needs two orchestrators running (victim + survivor).
  --signal KILL  hard crash (kill -9): Kafka must detect the dead member via session timeout
  --signal TERM  graceful shutdown (what Kubernetes sends): the consumer leaves the group immediately
"""
import argparse
import datetime as dt
import os
import re
import signal
import subprocess
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--signal", choices=["KILL", "TERM"], default="KILL")
p.add_argument("--count", type=int, default=200)
p.add_argument("--kill-after", type=float, default=2.5)
p.add_argument("--victim-pattern", default="flowforge-orchestrator-0.1.0-SNAPSHOT.jar$")
p.add_argument("--survivor-log", default="logs/orchestrator-2.log")
args = p.parse_args()

name = f"failover-{args.signal.lower()}-{int(time.time())}"
RECOVERED = re.compile(r"^(\S+)\s.*Recovered (\d+) running workflows for partitions \[([^\]]*)\] in (\d+) ms")


def sql(query):
    out = subprocess.run(["docker", "exec", "ff-postgres", "psql", "-U", "flowforge", "-d", "flowforge", "-tAc", query],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def recovered_lines():
    with open(args.survivor_log) as f:
        return [m for m in (RECOVERED.match(line) for line in f) if m]


victim_pid = int(subprocess.check_output(["pgrep", "-f", args.victim_pattern]).split()[0])
subprocess.run([sys.executable, "loadtest/submit.py", "--file", "examples/chain.json",
                "--count", str(args.count), "--name", name], check=True)
time.sleep(args.kill_after)

seen_before = len(recovered_lines())
running = sql(f"SELECT count(*) FROM workflow_runs WHERE name = '{name}' AND status = 'RUNNING'")
t_kill = dt.datetime.now(dt.timezone.utc)
os.kill(victim_pid, signal.SIGKILL if args.signal == "KILL" else signal.SIGTERM)
print(f"SIG{args.signal} -> orchestrator pid {victim_pid} with {running} workflows running")

new = []
deadline = time.time() + 90
while time.time() < deadline:
    new = recovered_lines()[seen_before:]
    partitions = {int(x) for m in new for x in m.group(3).split(",") if x.strip()}
    if len(partitions) == 12:
        break
    time.sleep(0.05)
else:
    sys.exit("survivor never took over all 12 partitions")

t_recovered = max(dt.datetime.fromisoformat(m.group(1)) for m in new)
failover_ms = (t_recovered - t_kill).total_seconds() * 1000
replayed = sum(int(m.group(2)) for m in new)
rebuild_ms = max(int(m.group(4)) for m in new)

while sql(f"SELECT count(*) FROM workflow_runs WHERE name = '{name}' AND status = 'RUNNING'") != "0":
    if time.time() > deadline + 60:
        sys.exit("workflows did not finish")
    time.sleep(0.5)

events = f"workflow_events e JOIN workflow_runs r USING (workflow_id) WHERE r.name = '{name}'"
statuses = sql(f"SELECT string_agg(status || '=' || c, ' ') FROM (SELECT status, count(*) c FROM workflow_runs WHERE name = '{name}' GROUP BY status) s")
succeeded = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskSucceeded'")
duplicates = sql(f"SELECT count(*) FROM (SELECT e.workflow_id, e.payload->>'taskId' FROM {events} AND e.event_type = 'TaskSucceeded' GROUP BY 1, 2 HAVING count(*) > 1) d")
reruns = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskStarted' AND (e.payload->>'attempt')::int > 1")
stall = sql(f"SELECT round(max(extract(epoch FROM gap)) * 1000) FROM (SELECT e.created_at - lag(e.created_at) OVER (PARTITION BY e.workflow_id ORDER BY e.sequence) AS gap FROM {events}) g")

print(f"""
=== {name} ===
failover (signal -> survivor recovered)  : {failover_ms:,.0f} ms
state rebuild (replay + re-arm timers)    : {rebuild_ms} ms for {replayed} in-flight workflows
final statuses                            : {statuses}
tasks succeeded                           : {succeeded} (expected {args.count * 5})
tasks succeeded twice                     : {duplicates}
task attempts re-run after the crash      : {reruns}
longest stall seen by any workflow        : {stall} ms (normal gap ~1000 ms: each step sleeps 1s)""")
