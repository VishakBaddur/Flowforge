#!/usr/bin/env python3
"""Run the NYC 311 ETL pipeline as a Flowforge workflow.

  backfill     python3 etl/pipeline.py backfill --start 2026-09-01 --end 2026-09-30
  incremental  python3 etl/pipeline.py incremental [--lookback 2]
  fault demo   python3 etl/pipeline.py backfill --start 2026-09-01 --end 2026-09-07 --inject-fault 2026-09-04

DAG: plan -> for each day [extract -> validate -> transform -> load] -> finalize.
Finalize runs only after every day has loaded, and it is the only step that advances the watermark.
"""
import argparse
import collections
import datetime as dt
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = "http://localhost:8080/api/v1"
PIPELINE = "nyc311"
BACKOFF = {"initialDelay": "PT2S", "multiplier": 2.0, "maxDelay": "PT30S", "jitter": True}


def psql(sql):
    out = subprocess.run(["docker", "exec", "ff-postgres", "psql", "-U", "flowforge", "-d", "analytics", "-tAc", sql],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


def call(method, path, token=None, body=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(API + path, method=method, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def task(tid, ttype, inputs, deps, retries, timeout):
    return {"id": tid, "type": ttype, "input": {k: str(v) for k, v in inputs.items()}, "dependsOn": deps,
            "maxRetries": retries, "timeout": timeout, "backoff": BACKOFF}


def build(run_id, mode, days, fault_day):
    common = {"run_id": run_id, "pipeline": PIPELINE}
    window = {"start_date": days[0], "end_date": days[-1], "partitions": len(days)}
    tasks = [task("plan", "etl.plan", {**common, **window, "mode": mode}, [], 3, "PT1M")]
    loads = []
    for d in days:
        p = {**common, "partition_date": d}
        extract_in = dict(p, inject_fault="drop_created_date") if d == fault_day else p
        tasks += [
            task(f"extract-{d}", "etl.extract", extract_in, ["plan"], 5, "PT90S"),        # network: retry
            task(f"validate-{d}", "etl.validate", p, [f"extract-{d}"], 0, "PT2M"),       # bad data won't fix itself
            task(f"transform-{d}", "etl.transform", p, [f"validate-{d}"], 2, "PT2M"),
            task(f"load-{d}", "etl.load", p, [f"transform-{d}"], 3, "PT2M"),            # transient lock conflicts
        ]
        loads.append(f"load-{d}")
    tasks.append(task("finalize", "etl.finalize", {**common, **window}, loads, 3, "PT5M"))
    return {"name": f"etl-{PIPELINE}-{mode}-{days[0]}-to-{days[-1]}", "tasks": tasks}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--start", required=True)
    b.add_argument("--end", required=True)
    b.add_argument("--inject-fault", metavar="DATE", help="simulate an upstream schema change on this day")
    i = sub.add_parser("incremental")
    i.add_argument("--lookback", type=int, default=2, help="re-process this many complete days for late updates")
    i.add_argument("--lag", type=int, default=2, help="source publishes with a delay: stop this many days before today")
    args = ap.parse_args()

    if args.mode == "backfill":
        start, end = dt.date.fromisoformat(args.start), dt.date.fromisoformat(args.end)
        fault = args.inject_fault
    else:
        wm = psql(f"SELECT complete_through FROM etl.watermark WHERE pipeline = '{PIPELINE}'")
        if not wm:
            sys.exit("no watermark yet: run a backfill first")
        start = dt.date.fromisoformat(wm) - dt.timedelta(days=args.lookback - 1)
        end = dt.date.today() - dt.timedelta(days=args.lag)
        fault = None
        print(f"watermark is {wm}: re-processing {args.lookback} day(s) for late updates, "
              f"new days up to {end} (publication lag {args.lag} days)")
    if end < start:
        sys.exit(f"nothing to do: {start} is after {end}")
    days = [(start + dt.timedelta(n)).isoformat() for n in range((end - start).days + 1)]
    run_id = f"{args.mode}-{days[0]}-{days[-1]}-{int(time.time())}"
    definition = build(run_id, args.mode, days, fault)

    token = call("POST", "/auth/token", body={"username": "alice", "password": "alice-pass"})["accessToken"]
    t0 = time.time()
    wf = call("POST", "/workflows", token, definition)["workflowId"]
    print(f"run {run_id}: {len(days)} partition(s), {len(definition['tasks'])} tasks, workflow {wf}")

    last = 0
    while True:
        try:
            view = call("GET", f"/workflows/{wf}", token)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            time.sleep(1)
            continue
        if view["status"] != "RUNNING":
            break
        if time.time() - last > 5:
            counts = collections.Counter(t["status"] for t in view["tasks"])
            print(f"  {time.time() - t0:5.0f}s  " + "  ".join(f"{k.lower()}={v}" for k, v in sorted(counts.items())))
            last = time.time()
        time.sleep(1)

    elapsed = time.time() - t0
    kinds = collections.Counter(e["type"] for e in call("GET", f"/workflows/{wf}/events", token))
    print(f"\nworkflow {view['status']} in {elapsed:.1f}s  |  task retries: {kinds['TaskFailed']}  "
          f"dead-lettered: {kinds['TaskDeadLettered']}  skipped: {kinds['TaskSkipped']}")
    if view["status"] == "COMPLETED":
        print(psql(f"SELECT jsonb_pretty(report) FROM etl.run WHERE run_id = '{run_id}'"))
    else:
        psql(f"UPDATE etl.run SET status = 'FAILED', finished_at = now() WHERE run_id = '{run_id}' AND status = 'RUNNING'")
        for t in view["tasks"]:
            if t["status"] == "DEAD_LETTERED":
                print(f"  {t['id']}: {t['lastError']}")
        print("watermark (unchanged):",
              psql(f"SELECT complete_through FROM etl.watermark WHERE pipeline = '{PIPELINE}'") or "none")


if __name__ == "__main__":
    main()
