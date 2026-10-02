#!/usr/bin/env bash
# Re-runs every failure test against the current build:
#   1. orchestrator SIGTERM   2. orchestrator kill -9   3. worker kill -9   4. Redis frozen 10s
# Starts a second orchestrator and worker as hot standbys and restarts each victim between tests.
set -uo pipefail
cd "$(dirname "$0")/.."
ORCH=flowforge-orchestrator/target/flowforge-orchestrator-0.1.0-SNAPSHOT.jar
WORKER=flowforge-worker/target/flowforge-worker-0.1.0-SNAPSHOT.jar
SUMMARY='===|failover|rebuild|finished|statuses|succeeded|duplicates|twice|re-run|expired|failed|stall|Stuck|TIMED'

./scripts/dev-up.sh
nohup java -jar "$ORCH" --server.port=8083 > logs/orchestrator-2.log 2>&1 &
nohup java -jar "$WORKER" --server.port=8084 > logs/worker-2.log 2>&1 &
echo "standby orchestrator + worker started; waiting for rebalance"; sleep 15

echo; echo "########## 1/4 orchestrator SIGTERM ##########"
python3 loadtest/failover_test.py --signal TERM | grep -E "$SUMMARY"
nohup java -jar "$ORCH" > logs/orchestrator.log 2>&1 &
sleep 15

echo; echo "########## 2/4 orchestrator kill -9 ##########"
python3 loadtest/failover_test.py --signal KILL | grep -E "$SUMMARY"
nohup java -jar "$ORCH" > logs/orchestrator.log 2>&1 &
sleep 15

echo; echo "########## 3/4 worker kill -9 ##########"
python3 loadtest/worker_kill_test.py | grep -E "$SUMMARY|wf-"
nohup java -jar "$WORKER" > logs/worker.log 2>&1 &
sleep 12

echo; echo "########## 4/4 Redis frozen 10s ##########"
python3 loadtest/redis_outage_test.py --mode pause | grep -E "$SUMMARY"

echo; echo "batch fallbacks across the suite: $(cat logs/orchestrator*.log | grep -c 'not committed')"
