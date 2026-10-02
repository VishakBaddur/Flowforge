#!/usr/bin/env python3
"""Submit many workflows to the Flowforge API concurrently (standard library only).

Example: python3 loadtest/submit.py --file examples/chain.json --count 200 --name failover-a
Every workflow in a run gets the same --name, so results can be checked with SQL:
  SELECT status, count(*) FROM workflow_runs WHERE name = 'failover-a' GROUP BY status;
"""
import argparse
import concurrent.futures
import json
import time
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--api", default="http://localhost:8080/api/v1")
parser.add_argument("--user", default="alice")
parser.add_argument("--file", required=True)
parser.add_argument("--count", type=int, default=100)
parser.add_argument("--name", required=True, help="label stored as the workflow name")
parser.add_argument("--concurrency", type=int, default=32)
args = parser.parse_args()


def post(url, body, headers):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", **headers})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


token = post(f"{args.api}/auth/token", {"username": args.user, "password": f"{args.user}-pass"}, {})["accessToken"]
definition = json.load(open(args.file))
definition["name"] = args.name
auth = {"Authorization": f"Bearer {token}"}

start = time.time()
with concurrent.futures.ThreadPoolExecutor(args.concurrency) as pool:
    ids = list(pool.map(lambda _: post(f"{args.api}/workflows", definition, auth)["workflowId"], range(args.count)))
elapsed = time.time() - start
print(f"submitted {len(ids)} workflows named '{args.name}' in {elapsed:.2f}s ({len(ids) / elapsed:.0f}/s)")
