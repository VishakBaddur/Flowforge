#!/usr/bin/env python3
"""Freeze Redis in the middle of a run and check that workflows still complete.

  --mode pause  docker pause: Redis hangs (like a network partition); clients see timeouts
  --mode stop   docker stop:  Redis is gone; clients see connection refused
Redis is an optimization (idempotency cache + claims); losing it must not stop or lose work.
"""
import argparse
import subprocess
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--count", type=int, default=200)
p.add_argument("--mode", choices=["pause", "stop"], default="pause")
p.add_argument("--outage", type=float, default=10, help="seconds Redis stays down")
p.add_argument("--start-after", type=float, default=1.5)
p.add_argument("--max-wait", type=float, default=90)
args = p.parse_args()

name = f"redis-{args.mode}-{int(time.time())}"


def sql(query):
    out = subprocess.run(["docker", "exec", "ff-postgres", "psql", "-U", "flowforge", "-d", "flowforge", "-tAc", query],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def running():
    return int(sql(f"SELECT count(*) FROM workflow_runs WHERE name = '{name}' AND status = 'RUNNING'"))


def redis(action):
    subprocess.run(["docker", action, "ff-redis"], check=True, capture_output=True)


subprocess.run([sys.executable, "loadtest/submit.py", "--file", "examples/chain.json",
                "--count", str(args.count), "--name", name], check=True)
time.sleep(args.start_after)
redis(args.mode)
t_down = time.time()
print(f"Redis {args.mode}d with {running()} workflows running; restoring in {args.outage:.0f}s")
time.sleep(args.outage)
redis("unpause" if args.mode == "pause" else "start")
print(f"Redis restored after {time.time() - t_down:.1f}s")

while running() > 0 and time.time() - t_down < args.max_wait:
    time.sleep(0.5)
elapsed = time.time() - t_down

events = f"workflow_events e JOIN workflow_runs r USING (workflow_id) WHERE r.name = '{name}'"
stuck = running()
statuses = sql(f"SELECT string_agg(status || '=' || c, ' ') FROM (SELECT status, count(*) c FROM workflow_runs WHERE name = '{name}' GROUP BY status) s")
succeeded = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskSucceeded'")
duplicates = sql(f"SELECT count(*) FROM (SELECT e.workflow_id, e.payload->>'taskId' FROM {events} AND e.event_type = 'TaskSucceeded' GROUP BY 1, 2 HAVING count(*) > 1) d")
failed_attempts = sql(f"SELECT count(*) FROM {events} AND e.event_type = 'TaskFailed'")
stall = sql(f"SELECT round(max(extract(epoch FROM gap)) * 1000) FROM (SELECT e.created_at - lag(e.created_at) OVER (PARTITION BY e.workflow_id ORDER BY e.sequence) AS gap FROM {events}) g")

print(f"""
=== {name} ===
finished within (from outage start)       : {elapsed:.1f} s{'' if stuck == 0 else f'  (TIMED OUT: {stuck} still RUNNING)'}
final statuses                            : {statuses}
tasks succeeded                           : {succeeded} (expected {args.count * 5})
tasks succeeded twice                     : {duplicates}
failed task attempts                      : {failed_attempts}
longest stall seen by any workflow        : {stall} ms""")
if stuck:
    print("\nWhere the stuck workflows are (last event, grouped):")
    print(sql(f"""SELECT last.event_type || ' attempt=' || last.att || ': ' || count(*) FROM (
                    SELECT DISTINCT ON (e.workflow_id) e.event_type, e.payload->>'attempt' AS att
                    FROM {events} AND r.status = 'RUNNING' ORDER BY e.workflow_id, e.sequence DESC) last
                  GROUP BY last.event_type, last.att"""))
