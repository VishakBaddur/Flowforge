#!/usr/bin/env python3
"""kill -9 a worker in the middle of a run; verify every workflow still completes.

Needs two workers running. In-flight tasks on the dead worker must be retried (via lease expiry)
on the surviving worker. Re-runs are expected; lost workflows and duplicate successes are not.
"""
import argparse
import os
import signal
import subprocess
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--count", type=int, default=200)
p.add_argument("--kill-after", type=float, default=2.5)
p.add_argument("--victim-pattern", default="flowforge-worker-0.1.0-SNAPSHOT.jar$")
p.add_argument("--max-wait", type=float, default=90)
args = p.parse_args()

name = f"worker-kill-{int(time.time())}"


def sql(query):
    out = subprocess.run(["docker", "exec", "ff-postgres", "psql", "-U", "flowforge", "-d", "flowforge", "-tAc", query],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def running():
    return int(sql(f"SELECT count(*) FROM workflow_runs WHERE name = '{name}' AND status = 'RUNNING'"))


victim_pid = int(subprocess.check_output(["pgrep", "-f", args.victim_pattern]).split()[0])
subprocess.run([sys.executable, "loadtest/submit.py", "--file", "examples/chain.json",
                "--count", str(args.count), "--name", name], check=True)
time.sleep(args.kill_after)
print(f"SIGKILL -> worker pid {victim_pid} with {running()} workflows running")
t_kill = time.time()
os.kill(victim_pid, signal.SIGKILL)

while running() > 0 and time.time() - t_kill < args.max_wait:
    time.sleep(0.5)
drain_s = time.time() - t_kill

events = f"workflow_events e JOIN workflow_runs r USING (workflow_id) WHERE r.name = '{name}'"
stuck = running()
statuses = sql(f"SELECT string_agg(status || '=' || c, ' ') FROM (SELECT status, count(*) c FROM workflow_runs WHERE name = '{name}' GROUP BY status) s")
succeeded = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskSucceeded'")
duplicates = sql(f"SELECT count(*) FROM (SELECT e.workflow_id, e.payload->>'taskId' FROM {events} AND e.event_type = 'TaskSucceeded' GROUP BY 1, 2 HAVING count(*) > 1) d")
reruns = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskStarted' AND (e.payload->>'attempt')::int > 1")
lease_expired = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskFailed' AND e.payload->>'error' LIKE 'lease expired%'")
stall = sql(f"SELECT round(max(extract(epoch FROM gap)) * 1000) FROM (SELECT e.created_at - lag(e.created_at) OVER (PARTITION BY e.workflow_id ORDER BY e.sequence) AS gap FROM {events}) g")

print(f"""
=== {name} ===
all workflows finished within             : {drain_s:.1f} s of the kill{'' if stuck == 0 else f'  (TIMED OUT: {stuck} still RUNNING)'}
final statuses                            : {statuses}
tasks succeeded                           : {succeeded} (expected {args.count * 5})
tasks succeeded twice                     : {duplicates}
leases expired (dead worker detected)     : {lease_expired}
task attempts re-run (attempt > 1)        : {reruns}
longest stall seen by any workflow        : {stall} ms""")

if stuck:
    print("\nStuck workflows (last event each):")
    print(sql(f"""SELECT DISTINCT ON (e.workflow_id) e.workflow_id || '  ' || e.event_type || '  task=' ||
                  coalesce(e.payload->>'taskId', '-') || '  attempt=' || coalesce(e.payload->>'attempt', '-')
                  FROM {events} AND r.status = 'RUNNING' ORDER BY e.workflow_id, e.sequence DESC LIMIT 10"""))
